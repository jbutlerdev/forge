# Rust harness client (`crates/forge-harness-client`)

The thin Rust side of the H2.0 split: a unix-socket JSON-lines client
for this process's two sockets (`src/ipc.ts`), plus the
[disabled-mode](#disabled-mode) gate forge-api uses. No sqlx, no
business logic — just the wire protocol and the reconnect loops.

## Environment (read by `HarnessClient::from_env`, called from
`forge-api`'s `AppState::new`)

| Variable | Default | Meaning |
| --- | --- | --- |
| `FORGE_HARNESS_SOCKET` | `~/.local/state/forge/harness.sock` | RPC socket. **Unset OR file absent at forge-api startup ⇒ disabled mode** (warn log; every harness call → `HarnessError::Unavailable`; the legacy `drive_turn` path stays the default with zero behavior change). |
| `FORGE_HARNESS_EVENTS_SOCKET` | `~/.local/state/forge/harness-events.sock` | Events socket. Used only when enabled. |

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
| `turn_end` | log only — messages-table projection of harness transcripts is H2.1 |
| `document_changed` | log only |
| `timer_fired` | log only (the fired turn surfaces as `task_state` / `turn_end`) |
| `ResyncRequired` (client-side) | re-query `status`; keep learned marks (no task-listing IPC yet — H2.2); log |

The bus events carry the forge **session id** (UUID), not the harness
conversation id: the consumer resolves them through
`sessions.durable_conversation_id` so every existing SSE consumer
(ranch's forge worker matches `"message"` / `"turn_ended"` /
`"ranch_tool_request"` on the wire) sees unchanged payloads.
