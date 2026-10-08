# Rust harness client (`crates/forge-harness-client`)

The thin Rust side of the H2.0 split: a unix-socket JSON-lines client
for this process's two sockets (`src/ipc.ts`), plus the
disabled-mode gate forge-api uses, and (from H2.1) the turn-routing
rules below.

## Turn routing (H2.1) — `FORGE_HARNESS_MESSAGES`

Env flag, default **off**. Read once at forge-api `AppState`
construction (operator config, not a live switch).

**Off** → zero behavior change anywhere: session creation never talks
to the harness and every `POST /messages` takes the legacy
`drive_turn` path.

**On + harness enabled** (`FORGE_HARNESS_SOCKET` present):

- **Session creation** (`POST /sessions`, `POST
  /agents/:id/conversations`): after the session row + working dir
  exist, forge-api calls `createConversation` with the session's
  `provider`/`model` (session override ?? profile, the same rule the
  legacy spawn uses) and the profile's `system_prompt`, and stamps the
  returned id in `sessions.durable_conversation_id` (migration 017).
  **Any harness failure at creation keeps the session legacy** (warn
  log, no stamp) — session creation never fails because of the
  harness. **Forks (`fork_from`) stay legacy** while the flag is on:
  the harness IPC has no fork semantics yet (H2.2 `forkConversation`),
  and a fork's copied `messages` would not exist inside a fresh
  durable conversation.
- **`POST /messages`** (and the `/agents/.../messages` alias): tenancy
  check unchanged. When the session's `durable_conversation_id` is
  non-NULL, the user row is written + published exactly as legacy,
  then `harness.submit(conversation, request_id, {type:"input",
  content})` with `request_id` freshly minted per attempt (the harness
  dedupes exactly-once per request id). The response keeps the legacy
  shape (202 + the user message row).
  - **Submit failure → 503** ("harness submit failed (turn was not
    started; the user message is already recorded — resubmitting mints
    a fresh request id)"). The user row IS persisted; a retry mints a
    new `request_id` and starts a new turn (the dedup key only covers
    same-attempt retries).
  - **No deltas this phase**: the harness event stream has no live
    text deltas yet (pi-durable LiveDoc wiring is follow-up), so the
    legacy per-request delta stream is unavailable on harness
    sessions. Clients get the **full assistant `message` bus event at
    turn end** instead; the HTTP response is complete when submit is
    accepted (it never waited on streaming anyway).
- **Assistant projection** (`harness.rs`, the H2.0 `turn_end` TODO):
  on `HarnessEvent::TurnEnd { conversation_id, entry_id }` the event
  consumer claims `(conversation, entry)` in `durable_projection`
  (migration 018), reads the entry's `record` out of the `durable_*`
  schema (the one the harness pins via `FORGE_HARNESS_SCHEMA`; default
  `public`), extracts the answer text (the `text` content blocks of
  the `pi.assistant` entry's `model` assistant messages — the same
  blocks `src/events.ts` `assistantSummary` reads), and writes one
  assistant row via `insert_and_publish_assistant` → bus `message`
  event → the existing SSE stream. Tool-only turns (no text blocks)
  project no row; failed/aborted tasks project nothing (`turn_ended`
  is the whole signal). After the row lands, the fire-and-forget
  `refresh_session_summary` runs, mirroring the legacy post-turn
  refresh.

## Environment (read by `HarnessClient::from_env`, called from
`forge-api`'s `AppState::new`)

| Variable | Default | Meaning |
| --- | --- | --- |
| `FORGE_HARNESS_SOCKET` | `~/.local/state/forge/harness.sock` | RPC socket. **Unset OR file absent at forge-api startup ⇒ disabled mode** (warn log; every harness call → `HarnessError::Unavailable`; the legacy `drive_turn` path stays the default with zero behavior change). |
| `FORGE_HARNESS_EVENTS_SOCKET` | `~/.local/state/forge/harness-events.sock` | Events socket. Used only when enabled. |
| `FORGE_HARNESS_SCHEMA` | `public` | Schema the `durable_*` tables live in (the harness pins `search_path` to it via `PgStorage.open({schema})` in `src/main.ts`); forge-api's read-side projection queries it the same way. |
| `FORGE_HARNESS_MESSAGES` | off | The H2.1 turn-routing flag (see above). |

The harness process's own test-only env: `FORGE_HARNESS_FAUX=1`
registers the scripted faux provider (`faux`/`faux-1`), and
`FORGE_HARNESS_FAUX_RESPONSES` (JSON array of strings) pre-queues its
answers, one per generation call (the faux queue is otherwise empty —
an unqueued call errors and the turn fails).

## Reconnect semantics

Both sockets run an internal redial loop: exponential backoff
250 ms → 5 s cap, **forever** (harness restarts are frequent by
design; systemd `Restart=always` self-heals it).

- **In-flight requests die on reconnect** — they fail with
  `HarnessError::Disconnected` and are never replayed. Callers retry.
  `submit` is the only non-idempotent-looking method and it is safe to
  resubmit with the same `request_id`: the harness dedupes exactly-once
  (`submissionByRequest`), so a mid-flight crash returns the original
  submission instead of a duplicate turn.
- **Requests made while disconnected** wait for a connection with a
  5 s bound, then fail `Disconnected`.
- **Connected requests** get a 30 s response bound, then `Timeout`.
- **Events stream**: capacity-64 mpsc; a consumer slower than 5 s gets
  its event dropped with a warning rather than backpressuring the
  harness's commit path.
- **No event replay on reconnect.** Each (re)connect begins with a
  client-side `ResyncRequired` marker (never on the wire); consumers
  re-derive state (forge-api re-queries `status` and re-learns in-flight
  marks from subsequent `task_state` events).

## RPC methods (1:1 with `src/ipc.ts`)

| Client method | Wire method | Notes |
| --- | --- | --- |
| `status()` | `status` | health + bookkeeping |
| `create_conversation(&CreateConversation)` | `createConversation` | returns the durable conversation id; the forge session id lands in the `forge.meta` document; from H2.5, `tools_allowlist` (the agent's H1.1 column) is carried in the meta document and enforced by the extension's `before_tool` hook; `extra_instructions` are appended to the prompt |
| `submit(conv, request_id, entry_draft)` | `submit` | exactly-once per `request_id` |
| `steer(task_id, text)` | `steer` | `whenBusy: "steer"` |
| `abort(task_id, tree)` | `abort` | `tree` defaults true on the harness side |
| `document_get` / `document_put` | `documentGet` / `documentPut` | H2.5: `GET`/`PUT /sessions/:id/documents/:name` |
| `timer_set` / `timer_clear` | `timerSet` / `timerClear` | timers are Postgres-backed (H2.3: `harness_timers` in the durable schema; exactly-once claim on fire) |
| `timer_list(conversation_id?)` | `timerList` | all live timers, optionally scoped to one conversation (H2.3) |
| `compact(conv, instructions?)` | `compact` | H2.4: forces a manual compaction now (reason `manual`, background task; the summary lands at once when idle or at the next turn boundary); returns the task id |
| `reset(conv, handoff_note?)` | `reset` | H2.4: starts a new context segment from a handoff note (pi-durable `reset()`); old entries stay in `durable_entries` |
| `compaction_status(conv)` | `compactionStatus` | H2.4: live compactions, the newest placed summary, and the ACTIVE (post-compaction/reset) context size |

The legacy path (409 when a turn is in flight) stays for legacy
sessions. `POST /sessions/:id/interrupt` forwards to `abort` for sessions with a
stamped `sessions.durable_conversation_id` (migration 017) and accepts
`?tree=false` to abort the task alone (default: whole tree).

## Event-name contract (harness event → forge bus/mark)

| harness event (`src/events.ts`) | forge action |
| --- | --- |
| `hello` | log only |
| `task_state { status: "started" }` | remember conversation→task; `agent_registry.begin_turn(session)` (keeps `GET /agents/:id/active` + idle-cleanup correct) |
| `task_state { status: "done" \| "failed" \| "aborted" }` | forget conversation→task; `agent_registry.end_turn(session)`; bus **`turn_ended`** (always, even on error — byte-compatible with what the legacy turn driver in `crates/forge-api/src/api/turn.rs` publishes) |
| `turn_end` | **assistant projection (H2.1)**: claim `(conversation, entry)` in `durable_projection` (migration 018), read the `pi.assistant` entry text from the `durable_*` schema, write one assistant row → bus **`message`** event (byte-compatible with the legacy turn driver's rows); empty text (tool-only turn) claims but writes nothing |
| `document_changed` (H2.5) | bus **`document_changed`** on this session's stream (the doc row in the harness schema is the source of truth) |
| `timer_fired` | log only (the fired turn surfaces as `task_state` / `turn_end`) |
| `subagent_spawned` (H2.2) | mint the child's session row under the parent (`parent_session_id`, id = the harness-pre-minted forge session UUID; idempotent `ON CONFLICT (id) DO NOTHING`) → bus **`subagent_started`** on the PARENT's stream |
| `task_state` terminal on a subagent conversation (H2.2) | when no live task remains in the child conversation, bus **`subagent_ended`** on the PARENT's stream (parent derived from `sessions.parent_session_id`) |
| `ResyncRequired` (client-side) | re-query `status`; keep learned marks; log |

The bus events carry the forge **session id** (UUID), not the harness
conversation id: the consumer resolves them through
`sessions.durable_conversation_id` so every existing SSE consumer
(ranch's forge worker matches `"message"` / `"turn_ended"` /
`"ranch_tool_request"` on the wire) sees unchanged payloads.
The `subagent_started` / `subagent_ended` markers are the exception:
they carry BOTH session ids and are filtered on the PARENT's
(stream = parent session id).

### H2.2 / H2.3 API surface (this harness build)

- **Subagents** (`spawn_subagent` tool in the forge extension):
  foreground (child owned by the calling tool task; the tool waits and
  returns the child's answer) or `detach: true` (child owned by a
  background anchor task of the parent; it survives the parent's turn
  completing and its abort). `GET /sessions?parent=<uuid>` lists
  children. `POST /sessions/:id/interrupt` (default whole tree) aborts
  bottom-up including the child.
- **Durable timers** (Postgres `harness_timers`, same schema as
  `durable_*`): `POST /sessions/:id/timers {at_ms?, cron?, prompt}`
  (201; 503 when the session is not harness-backed or the harness is
  disabled), `GET /sessions/:id/timers`,
  `DELETE /sessions/:id/timers/:timerId`. Firing submits
  `timer fired: <prompt>` as a system turn with the request id
  `timer-fired:<timerId>:<firedAt>` (submission dedup); the row claim
  (`UPDATE … WHERE fired_at IS NULL … RETURNING`) is the exactly-once
  gate and survives kill -9 (boot reload re-arms live rows and fires
  overdue ones once).
  Deviation from PLAN-HERD: the routes live under `/sessions/:id/*`
  instead of `/conversations/:id/*` for API consistency with the rest
  of the sessions surface.

### H2.4 / H2.5 API surface (this harness build)

- **Compaction** (harness-owned): after every committed `pi.assistant`
  entry the conversation's threshold (its `config` document, `{
  "compaction": { "maxContextChars", "divisor" } }`, default 300000 / 4
  — the legacy forge-api heuristic, moved here) is checked; above it
  the built-in compaction task is enqueued as a conversation-owned
  **background** task (`reason: "threshold"`). It never interrupts an
  in-flight turn; the summary lands at once when idle, otherwise at the
  next turn boundary. `POST /sessions/:id/compact` forwards to the
  harness (200 + `task_id`; no 409 — nothing races the running turn).
  `GET /sessions/:id/context` reports `source: "harness"` with the
  active window (`active_context_chars`, `active_entry_count`,
  `last_compaction`, live `compactions`).
- **Reset / handoff**: `POST /sessions/:id/reset {handoff_note?}` (new
  route; legacy sessions get a clear 400) → `harness.reset` — the
  model no longer sees older entries, but they stay in
  `durable_entries` (searchable).
- **History search**: `GET /sessions/:id/history?q=` — `ILIKE` over
  `durable_entries.record` in the durable schema (400 without `q=`;
  400 when the session is not harness-backed). The trigram/GIN
  companion index (when `pg_trgm` is available) is created by the
  harness at boot (`src/history-index.ts`, guarded DDL); the query is
  identical with or without it.
- **Documents**: `GET`/`PUT /sessions/:id/documents/:name` (404 when
  absent) → `documentGet`/`documentPut`. First users: `plan`,
  `handoff`, and `config` (the compaction threshold). The harness
  extension renders `plan`/`handoff` as prompt sections each turn
  (`document_plan` / `document_handoff`); `memory_beliefs` is the
  registered empty stub (H4 plugs in). Every put publishes
  `document_changed` on the session's SSE stream (and the in-process
  bus).
- **Tool allowlist**: an agent's `tools_allowlist` (H1.1) is read at
  session attach (when `sessions.agent_id` is set) and sent in
  `createConversation`; the extension's `before_tool` hook blocks any
  tool call whose name is not listed (empty list = allow all) with a
  reason the model sees in the transcript — the call never reaches
  `/tools/execute`. The marked H3.5 policy-hook extension point (mule)
  sits next to it in `src/forge-ext.ts`.
