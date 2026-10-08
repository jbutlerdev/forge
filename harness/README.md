# @forge/harness

The forge **harness process**: the Node runtime that runs agent
conversations, turns, tasks, timers, and documents on
[`@earendil-works/pi-durable`](../vendor/pi-durable) (pinned at 1.0.4)
with the [`@forge/durable-pg`](../durable-pg) Postgres storage backend.
forge-api (Rust) drives this process over a unix socket; tenancy, the
HTTP surface, SSE fan-out, and the tool sandbox stay in forge-api —
tool calls flow **harness extension → `POST /tools/execute` → sandbox**
(the same enforcement point the standalone `extensions/forge-tools` pi
extension uses today).

Part of Herd phase **H2.0** (see `~/src/ranch-2/docs/PLAN-HERD.md` §H2.0).

## Layout

| File | Purpose |
| --- | --- |
| `src/main.ts` | Boot sequence (below), signal handling, process entry. |
| `src/forge-ext.ts` | The forge pi-durable extension: `bash`/`read`/`write`/`edit` tools that relay to forge's `/tools/execute[ /stream]`. |
| `src/ipc.ts` | Handler map (testable in-process) + the two socket servers. |
| `src/events.ts` | pi-durable commit publications → the event vocabulary. |
| `src/timers.ts` / `src/cron.ts` | Timer registry + minimal 5-field cron. |
| `src/docs.ts` | The harness document families (`forge.meta`, `forge.document`). |

## Install / build / run

The forge repo root is an **npm workspace** (`package.json` lists
`durable-pg` and `harness`). From `~/src/forge`:

```sh
npm install     # hoists deps to the root node_modules
cd harness
npm run build   # tsc → dist/
npm start       # node dist/main.js
npm test        # vitest (hits the scratch Postgres, see below)
```

`@forge/durable-pg` is a workspace member (sibling directory, symlinked
into `node_modules`), so the compiled `dist/main.js` imports its
TypeScript source at runtime — Node ≥ 22.18 strips types from files
outside `node_modules`; the symlink's target is `durable-pg/src/`, which
is outside the store.

### Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `FORGE_DATABASE_URL` | — (**required**) | The forge Postgres database, e.g. `postgres://postgres@localhost/forge`. durable-pg applies its migrations on open. |
| `FORGE_API_KEY` | — (**required**) | A real forge API key; sent as `Authorization: Bearer` on `/tools/execute[ /stream]` (forge-api also accepts `X-API-Key`). |
| `FORGE_API_URL` | `http://127.0.0.1:8080` | Where forge-api listens. |
| `FORGE_HARNESS_SOCKET` | `~/.local/state/forge/harness.sock` | RPC socket path. |
| `FORGE_HARNESS_EVENTS_SOCKET` | `~/.local/state/forge/harness-events.sock` | Event push socket path. |
| `FORGE_HARNESS_SCHEMA` | *(server default)* | Optional Postgres schema for the `durable_*` tables (pins `search_path`). |
| `FORGE_HARNESS_FAUX` | unset | `1` registers pi-ai's faux provider (tests/dry runs; use model `faux` / `faux-1`). |

**Model provider credentials come from the standard pi-ai environment
variables** (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `ANTHROPIC_OAUTH_TOKEN`,
`GOOGLE_API_KEY`, …) — `createModels()`'s built-in providers resolve them
themselves. Forge-side per-machine model-credential plumbing is a later
task; for now the harness inherits the environment of whatever runs it.

## Sockets

### RPC socket (`FORGE_HARNESS_SOCKET`)

Unix socket, **one JSON object per line**:

```
request:  {"id": <any>, "method": <string>, "params": <object>}
response: {"id": <same>, "result": <any>}   or
          {"id": <same>, "error": {"code": <string>, "message": <string>}}
```

Methods:

| Method | Params | Result |
| --- | --- | --- |
| `status` | `{}` | `{version, activeTasks, conversations, timers}` |
| `createConversation` | `{forgeSessionId, agent: {provider, modelId, systemPrompt?}, extraInstructions?, replaySafeTools?, toolsAllowlist?}` | `{conversationId}` |
| `submit` | `{conversationId, requestId, entryDraft}` where `entryDraft` is a pi-durable `SubmissionDraft` (`{type:"input", content}` or `{type:"write", entry}`) | `{submissionId}` — exactly-once: resubmitting the same `requestId` returns the original submission (pi-durable `submissionByRequest` dedup). |
| `steer` | `{taskId, text}` | `null` — submits the text into the task's conversation with `whenBusy: "steer"`. |
| `abort` | `{taskId, tree?}` (default `true`) | `{aborted: n}` — aborts the task and, with `tree`, every task it owns (the walk additionally reaches background descendants; pi-durable cascades the mark to ordinary owned work on its own). |
| `documentGet` | `{conversationId, name}` | the JSON value, or `null` |
| `documentPut` | `{conversationId, name, value}` | `null` |
| `timerSet` | `{conversationId, at?, cron?, prompt}` (exactly one of `at`/`cron`) | `{timerId}` |
| `timerClear` | `{conversationId, timerId}` | `{cleared: bool}` |
| `timerList` | `{conversationId?}` | `{timers: [{timerId, conversationId, at?, cron?, prompt, createdAt?, firedAt?}]}` |
| `compact` | `{conversationId, instructions?}` | `{taskId}` — forces a manual compaction now (reason `manual`, background: the summary lands at once when idle or at the next turn boundary; an in-flight turn is never interrupted) |
| `reset` | `{conversationId, handoffNote?}` | `null` — starts a new context segment from a handoff note (pi-durable `reset()`); older entries leave the model context but stay in `durable_entries` |
| `compactionStatus` | `{conversationId}` | `{compactions: [{taskId, reason, blocking, attempt}], lastCompaction: {entryId, reason, summaryChars} \| null, activeContextChars, activeEntryCount}` — the ACTIVE window (post-compaction/reset; entries before the head marker excluded) |

Typed error codes: `invalid_params`, `unknown_conversation`,
`unknown_task`, `unknown_method`, `bad_request`, `internal`.

IDs are plain numbers (pi-durable's erased numeric ID brands).

### Event socket (`FORGE_HARNESS_EVENTS_SOCKET`)

The harness **LISTENS**; forge-api connects (one client at a time — a
second connection displaces the first and is logged). Events are JSON
lines, pushed only while connected:

```
{"type":"hello","version":"0.1.0"}        on connect
{"type":"task_state","taskId","conversationId","status","outcomeStatus?"}
{"type":"turn_end","conversationId","entryId","summary"}
{"type":"document_changed","conversationId","name"}
{"type":"timer_fired","timerId","conversationId","prompt"}
```

- `task_state.status`: `started` (first `running`), then `done` /
  `failed` / `aborted` (pi-durable terminal outcomes: completed / failed /
  aborted-or-orphaned).
- `turn_end`: a generation-produced `pi.assistant` entry was committed
  (summary = first ≤200 chars of the assistant text).
- `document_changed`: only harness documents (`forge.*`).

**No replay on reconnect.** After (re)connecting, forge-api resynchronizes
through its own API (session transcripts / messages tables are the source
of truth; the messages-table projection sync is H2.1's job). Missing an
event is not an error.

## Boot / recovery semantics

1. `PgStorage.open` (migrations applied on open).
2. `Harness.open` — pi-durable's open path **reconciles unfinished work
   from a previous (dead) process**: `running` tasks are marked back to
   `pending` with their checkpoints and memos intact, before anything
   runs.
3. Per-conversation forge extensions are re-installed for every existing
   conversation (conversations store extension *names*, not code; the
   `replaySafeTools` flags **and the H2.5 `toolsAllowlist`** come from
   each conversation's `forge.meta` document).
4. `harness.resume()` — self-supervision: interrupted turns, tool calls,
   and queued submissions are scheduled again.

Also at boot (H2.4): the trigram/GIN companion index behind
`GET /sessions/:id/history?q=` is ensured in the harness schema
(`src/history-index.ts`, guarded DDL — created when `pg_trgm` is
available, skipped with a log line otherwise; the `ILIKE` query runs
without it), and the compaction-threshold watcher is armed (after every
committed `pi.assistant` entry the conversation's `config` document is
checked; above its threshold the built-in compaction task is enqueued
as a background task — see `src/compaction.ts`).

SIGTERM/SIGINT → close the socket servers, drop timers,
`harness.close(context)` (which closes storage and its pool), exit 0.
`Restart=on-failure` on the systemd unit makes crashes self-healing.

## How the forge machine runs it

`lab/ct/forge` ships a `forge-harness.service` user/system unit next to
`forge-api` (`Restart=always`, same secret plumbing: the host-local
`forge.env` EnvironmentFile supplies `FORGE_API_KEY`; `FORGE_DATABASE_URL`
and `FORGE_API_URL` are fixed). The flake bakes the harness into the
image the same way `forge-tools` is baked (a Nix derivation that runs
`npm install && npm run build` over the pinned forge source and ships
`dist/` + `node_modules/`), and
`ExecStart` runs `node <store-path>/dist/main.js`.

## Malleable layer (H2.5)

- **Documents as prompt sections**: the extension registers
  `document_plan` / `document_handoff` prompt sections that render the
  conversation's `plan` / `handoff` documents on every turn (absent
  document ⇒ the section renders nothing), and the registered stub
  `memory_beliefs` (empty; H4 plugs in the belief store). Edit them
  through the ordinary document surface (`documentPut` /
  `PUT /sessions/:id/documents/:name`) — the next turn picks up the
  new text.
- **Tool allowlist**: `createConversation` carries the agent's
  `tools_allowlist` (H1.1). When non-empty, the extension's
  `before_tool` hook BLOCKS any tool call whose name is not in the
  list, with a reason the model sees in the transcript (`Tool '<name>'
  is not in this agent's tool allowlist …`); the call never reaches
  `/tools/execute`. Empty/absent = allow all (non-breaking). The marked
  H3.5 policy-hook extension point (mule policy engine) sits next to
  the allowlist check — no mule calls today.

## Known limits

- **Timers are durable (H2.3)**: `timerSet`/`timerClear`/`timerList`
  persist in `harness_timers` (the harness schema, beside `durable_*`);
  on boot, live rows are re-armed and overdue ones fire exactly once
  through the atomic row claim. What is lost in a crash: nothing — the
  cost is a fire delay up to the next boot, and a timer that was in
  flight between the claim and the submit (the submission dedup
  backstops re-fire).
- **Messages projection:** the harness does not write forge's
  `messages` table; forge-api projects from the event stream / transcripts
  (H2.1).
- **Provider credentials** from environment only (see above).
- **Fork/steer surface:** `forkConversation` from the PLAN-HERD draft is
  intentionally not exposed yet (no forge-api caller).
