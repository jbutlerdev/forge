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
| `create_conversation(&CreateConversation)` | `createConversation` | returns the durable conversation id; the forge session id lands in the `forge.meta` document |
| `submit(conv, request_id, entry_draft)` | `submit` | exactly-once per `request_id` |
| `steer(task_id, text)` | `steer` | `whenBusy: "steer"` |
| `abort(task_id, tree)` | `abort` | `tree` defaults true on the harness side |
| `document_get` / `document_put` | `documentGet` / `documentPut` | |
| `timer_set` / `timer_clear` | `timerSet` / `timerClear` | |

**There is no `compact` method on the wire.** forge-api's
`POST /sessions/:id/compact` therefore stays on the legacy path for
harness-backed sessions too (until the harness gains compaction, H2.4);
`POST /sessions/:id/interrupt` forwards to `abort` for sessions with a
stamped `sessions.durable_conversation_id` (migration 017) and accepts
`?tree=false` to abort the task alone (default: whole tree).

## Event-name contract (harness event → forge bus/mark)

| harness event (`src/events.ts`) | forge action |
| --- | --- |
| `hello` | log only |
| `task_state { status: "started" }` | remember conversation→task; `agent_registry.begin_turn(session)` (keeps `GET /agents/:id/active` + idle-cleanup correct) |
| `task_state { status: "done" \| "failed" \| "aborted" }` | forget conversation→task; `agent_registry.end_turn(session)`; bus **`turn_ended`** (always, even on error — byte-compatible with what the legacy turn driver in `crates/forge-api/src/api/turn.rs` publishes) |
| `turn_end` | **assistant projection (H2.1)**: claim `(conversation, entry)` in `durable_projection` (migration 018), read the `pi.assistant` entry text from the `durable_*` schema, write one assistant row → bus **`message`** event (byte-compatible with the legacy turn driver's rows); empty text (tool-only turn) claims but writes nothing |
| `document_changed` | log only |
| `timer_fired` | log only (the fired turn surfaces as `task_state` / `turn_end`) |
| `ResyncRequired` (client-side) | re-query `status`; keep learned marks (no task-listing IPC yet — H2.2); log |

The bus events carry the forge **session id** (UUID), not the harness
conversation id: the consumer resolves them through
`sessions.durable_conversation_id` so every existing SSE consumer
(ranch's forge worker matches `"message"` / `"turn_ended"` /
`"ranch_tool_request"` on the wire) sees unchanged payloads.
