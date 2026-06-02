# Architecture

This document covers how Forge is put together: the message lifecycle, the split between harness and executor, the pi rpc event protocol, and the audit log schema.

## 1. The big picture

Forge is a single axum process that owns a PostgreSQL connection pool and a map of long-lived `pi` subprocesses (one per session). The flow when a client sends a message is:

```
  client                            forge-api                                 pi
    │                                  │                                       │
    │  POST /messages                   │                                       │
    │  {session_id, content}            │                                       │
    │ ────────────────────────────────▶ │                                       │
    │                                  │ 1. insert user row                    │
    │                                  │    (sequence = get_next_sequence(s))  │
    │                                  │                                       │
    │                                  │ 2. acquire PiAgent from registry      │
    │                                  │    (or spawn one)                     │
    │                                  │                                       │
    │                                  │ 3. drain pending events               │
    │                                  │    (from any straggler turn)          │
    │                                  │                                       │
    │                                  │ 4. write prompt to pi's stdin         │
    │                                  │ ──────────────────────────────────▶  │
    │                                  │      {"type":"prompt","message":...}  │
    │                                  │                                       │
    │                                  │ 5. spawn harness task:                │
    │                                  │    read line-buffered JSON events     │
    │                                  │ ◀──────────────────────────────────  │
    │                                  │      agent_start                      │
    │                                  │      turn_start                       │
    │                                  │      message_start                    │
    │                                  │      message_update...                │
    │                                  │                                       │
    │  202 Accepted                     │                                       │
    │ ◀──────────────────────────────── │                                       │
    │                                  │ 6. meanwhile pi calls tools:          │
    │                                  │                                       │
    │                                  │    ┌─────────────────────────────┐    │
    │                                  │    │ forge-tools extension       │    │
    │                                  │    │ registers bash/read/...     │    │
    │                                  │    │ on execute: POST /tools/... │    │
    │                                  │    └────────────┬────────────────┘    │
    │                                  │ ◀──────────── │                     │
    │                                  │   POST /tools/execute                 │
    │                                  │   {tool, input, tool_call_id}         │
    │                                  │                                       │
    │                                  │ 7. tool_executor.rs runs the tool,    │
    │                                  │    recorder.record_result()           │
    │                                  │ ──── insert result row ────▶ DB       │
    │                                  │                                       │
    │                                  │ 8. return ToolOutput to extension     │
    │                                  │ ──────────────────────────────────▶  │
    │                                  │                                       │
    │                                  │ 9. eventually:                        │
    │                                  │ ◀──────────────────────────────────  │
    │                                  │      turn_end                         │
    │                                  │      agent_end                        │
    │                                  │                                       │
    │                                  │ 10. harness returns                   │
    │                                  │                                       │
    │  GET /messages?session_id=…      │                                       │
    │ ────────────────────────────────▶ │                                       │
    │ 200 OK                            │                                       │
    │ {messages: [...]}                 │                                       │
    │ ◀──────────────────────────────── │                                       │
```

The client is expected to subscribe to `GET /sessions/{id}/events?since=<seq>` for live updates. The endpoint speaks Server-Sent Events: on connect we replay any rows with `sequence > since` (catch-up), then forward new rows in real time as the harness and tool executor write them. Clients that prefer polling can still use `GET /messages?session_id=…`; the two are equivalent. The CLI's `message ask` uses polling because it's stateless and doesn't want to manage SSE reconnects.

## 2. Module map

| Module | What it owns |
|---|---|
| `main.rs` | Build `AppState`, run migrations, start axum, spawn cleanup / metrics background tasks |
| `lib.rs` | Module declarations, public error type |
| `api/mod.rs` | HTTP handlers; **the harness event loop** that consumes pi's stdout |
| `api/auth.rs` | Register / login / API key middleware |
| `api/sse.rs` | `/tools/execute/stream` and the streaming bash path |
| `db/` | SQLx row types (`Message`, `Profile`, `Session`, `User`, `ApiKey`, …) |
| `pi_agent.rs` | The `PiAgent` struct: spawn pi with `--mode rpc`, write prompts to stdin, read events from stdout, expose `send_prompt` and `drain_pending_events` |
| `agent_registry.rs` | `Arc<Mutex<HashMap<Uuid, Arc<PiAgent>>>>`; the `get_or_spawn` method resolves `FORGE_TOOLS_EXTENSION` to an absolute path |
| `tool_executor.rs` | `ToolExecutor` — the four tool implementations, plus the per-call timing/recording |
| `recording.rs` | The `ToolRecorder` trait, the `DbToolRecorder` impl, and the row-flattening helper |
| `session_manager.rs` | `/forge/sessions/<id>/` lifecycle; cleanup of inactive sessions after 30 min |
| `sandbox.rs` | systemd-nspawn wrapper. **Currently inactive** — tools run on the host with `in_sandbox=false` |
| `observability.rs` | Request / tool-execution counters, exposed at `/metrics` and `/metrics/prometheus` |
| `logging.rs` | `tracing_subscriber` setup |

## 3. The ToolRecorder split

The most important design idea in this codebase: **the harness and the tool executor each write to `messages` independently, coordinated only through a trait.**

```
                       ┌─────────────────────────────┐
   pi stdout           │ api/mod.rs                  │  /tools/execute (HTTP
   (events)            │  (harness event loop)       │   from extension)
        │              │                             │          │
        ▼              │  PiEvent::ToolCallEnd       │          ▼
   PiEvent             │   ──▶ recorder.record_call( │   ToolExecutor::execute
                        │        ToolCallRecord {    │    ──▶ recorder.record_result(
                        │          session_id,        │         ToolResultRecord {
                        │          tool_call_id,      │           session_id,
                        │          tool_name,         │           tool_call_id,
                        │          tool_input,        │           tool_name,
                        │          content,            │           content,
                        │        })                   │           output, is_error,
                        │                             │           duration_ms,
                        │  PiEvent::ToolExecutionEnd  │         })
                        │   ──▶ (no DB write; just    │          │
                        │        log)                 │          │
                        └──────────────┬──────────────┘          │
                                       │                         │
                                       ▼                         ▼
                                  messages table
                              (linked by tool_call_id)
```

### Why split

- The **harness** is the only place that sees the model's intent (`toolcall_end` carries `{id, name, arguments}` from the assistant message). It owns the *call* row.
- The **executor** is the only place that knows what the tool actually did (exit code, structured output, duration, timeout). It owns the *result* row.
- The schema knowledge (column names, the `[tool_call:<name>]` content marker, the per-session sequence allocator) is encapsulated in `recording.rs`. Neither the harness nor the executor needs to know.
- Swapping the backend — separate `tool_invocations` table, event bus, observability sink — means writing one new `ToolRecorder` and passing it in `main.rs`. The harness and executor don't change.

### The trait

```rust
#[async_trait]
pub trait ToolRecorder: Send + Sync {
    async fn record_call(&self, record: ToolCallRecord) -> Result<(), sqlx::Error>;
    async fn record_result(&self, record: ToolResultRecord) -> Result<(), sqlx::Error>;
}

pub struct ToolCallRecord {
    pub session_id: Uuid,
    pub tool_call_id: String,
    pub tool_name: String,
    pub tool_input: serde_json::Value,
    pub content: String,    // "[tool_call:<name>]"
}

pub struct ToolResultRecord {
    pub session_id: Uuid,
    pub tool_call_id: String,
    pub tool_name: String,
    pub content: String,                 // flattened text for the content column
    pub output: serde_json::Value,       // structured payload for tool_output jsonb
    pub is_error: bool,
    pub duration_ms: Option<u64>,
}
```

### Concurrent writes and `get_next_sequence`

Both call sites acquire a per-session sequence number and insert the row in a single transaction:

```sql
BEGIN;
SELECT get_next_sequence($session_id);   -- takes pg_advisory_xact_lock(1, hashtext($session_id))
INSERT INTO messages (...);
COMMIT;
```

The advisory lock serializes concurrent allocations per session. **If you remove the lock, the unique constraint `(session_id, sequence)` will fire on concurrent writes from the harness and the executor.** See `migrations/004_get_next_sequence_locking.sql` for the full reasoning.

## 4. The pi rpc event protocol

Pi is launched with `--mode rpc` (line-delimited JSON over stdio). The harness writes prompts as one JSON object per line and reads events the same way.

### Prompt input

```json
{"type":"prompt","message":"<user text>"}
```

### Events the harness cares about

| Event | Field of interest | Harness action |
|---|---|---|
| `response` | `command: "prompt"`, `success` | log |
| `agent_start` | — | log |
| `turn_start` | — | set `seen_turn_start = true` |
| `message_start` | `messageId` | begin a new assistant text row |
| `message_update` (text start / delta / end) | text content | stream deltas to subscribers (text log) |
| `message_update` (thinking start / delta / end) | thinking content | stream as `[thinking]` to log |
| `message_update` (`ToolCallStart`) | `toolCall: {id, name, arguments}` | log |
| `message_update` (`ToolCallEnd`) | `toolCall: {id, name, arguments}` | **write call row** via `recorder.record_call(...)` |
| `message_update` (`ToolCallDelta`) | delta on the arguments | not persisted |
| `message_end` | `messageId`, text content | finalize the assistant text row |
| `turn_end` | — | log |
| `agent_end` | — | **only break the message loop if `seen_turn_start` was true** |
| `tool_execution_start` | `{toolCallId, toolName, args}` | log only (executor owns the result) |
| `tool_execution_end` | `{toolCallId, toolName, result, isError}` | log only |
| `extension_ui_request` | — | not handled |

The event types are defined in `pi_agent.rs` as `PiEvent` with `#[serde(rename_all = "camelCase")]`. Pi's protocol uses camelCase; Rust uses snake_case; the rename bridges them. The `MessageUpdate` variant also has `#[serde(rename_all = "camelCase")]` on its inner `TextStart` / `ToolCallStart` / `ToolCallEnd` shapes, with explicit `#[serde(rename = "contentIndex")]` on the few fields that aren't matched by the rename rule.

### Why the harness ignores `tool_execution_end`

In the original design, the harness parsed this event and wrote the result row. That works when there's only one writer (the harness, in sequence with the LLM). With the ToolRecorder split, the executor is the source of truth for results — it ran the tool, it knows the exit code and the duration, and it can write a properly-shaped `tool_output` jsonb. The harness just logs the event.

This is also why the `tool_call_id` round-trips end-to-end: pi gives the extension one id in its `execute(toolCallId, params, ...)` callback, and the extension passes that same id back in the `POST /tools/execute` body. The executor uses it as the join key.

## 5. The audit log

The `messages` table is the single source of truth. A reader can reconstruct the entire conversation by walking the rows in `sequence` order and joining call rows to result rows on `tool_call_id`.

### Reading a single call

```sql
SELECT
  c.sequence AS call_seq,
  c.tool_name,
  c.tool_input,
  c.tool_call_id,
  r.sequence AS result_seq,
  r.duration_ms,
  r.tool_output,
  r.content AS result_text,
  r.tool_output->>'success' AS success
FROM messages c
LEFT JOIN messages r
  ON r.session_id = c.session_id
  AND r.role = 'tool'
  AND r.tool_call_id = c.tool_call_id
WHERE c.session_id = $1
  AND c.role = 'assistant'
  AND c.tool_call_id IS NOT NULL
ORDER BY c.sequence;
```

### What the rows look like

Call row:

| seq | role | tool | tool_call_id | content | tool_input |
|---|---|---|---|---|---|
| 71 | assistant | bash | call_function_x9n…_1 | `[tool_call:bash]` | `{"command": "date", "timeout_ms": 30000}` |

Result row:

| seq | role | tool | tool_call_id | duration_ms | content | tool_output |
|---|---|---|---|---|---|---|
| 72 | tool | bash | call_function_x9n…_1 | 1 | `[bash exit=Some(0) duration=1ms]` | `{"stderr": null, "stdout": null, "success": true, "streamed": true, "exit_code": 0, "timed_out": false}` |

### Per-tool `tool_output` shapes

| Tool | Shape |
|---|---|
| `bash` (streaming) | `{success, stdout, stderr, exit_code, timed_out, streamed}` — `stdout`/`stderr` are NULL because the bytes go to the SSE consumer; `streamed: true` flags this |
| `bash` (non-streaming) | `{success, stdout, stderr, exit_code, timed_out, streamed: false}` — both `stdout` and `stderr` populated |
| `read` | `{success, output, error}` — `output` is the file contents; `error` is populated on failure |
| `write` | `{success, output, error}` |
| `edit` | `{success, output, error}` |

### Known data shape quirk

The DB `sequence` is the order rows were *written*, not the call/result pairing order. The executor writes the call row before running the tool and the result row after; for parallel tool calls, the call rows are written in sequence order as each tool's HTTP request arrives at forge, then the result rows interleave as each tool completes. Result rows often interleave with the *next* turn's call rows in `sequence` order. Always join on `tool_call_id`, not on adjacent sequences.

## 6. Streaming tool execution

`POST /tools/execute/stream` is a separate path from the normal `POST /tools/execute`. It's only used for `bash` (where the consumer wants to see output as it's produced). Other tools go through the normal path regardless.

The handler:

1. Resolves the session, working directory, and nix shell.
2. Builds a `ToolExecutor` with the session's recorder.
3. For non-bash tools, fires off the tool in a spawned task and emits a single `tool_start` / `tool_end` SSE pair.
4. For bash, calls `execute_bash_streaming(session_id, recorder, tool_call_id, …)` which:
   - Spawns the child process with stdout/stderr piped.
   - Streams each chunk to the SSE consumer as a `stdout` / `stderr` event.
   - On exit, hands `ToolResultRecord { …, output: {success, exit_code, duration_ms, timed_out, streamed: true, stdout: null, stderr: null}, … }` to the recorder.
   - Sends a final `tool_end` event with the exit code and duration, then a `done` event.

The CLI's `forge tools stream` (and the curl example in [`docs/API.md`](API.md)) show how to consume this.

## 7. Session lifecycle

1. Client calls `POST /sessions` with `{profile_id, title?}`. The API:
   - Inserts a row into `sessions` (id, profile_id, title, last_active, cell_host, cell_state).
   - Creates `/forge/sessions/<id>/` as the working directory.
   - Returns `{session, working_dir}`.
2. Client sends messages via `POST /messages`. The harness spawns a `PiAgent` for the session on first use and keeps it warm in `agent_registry`.
3. After 30 minutes of inactivity, the session's `last_active` is old enough that the cleanup task removes the working directory and shuts down the pi process. (This is the only place the in-memory state is lost; the messages table is the source of truth.)
4. A subsequent `POST /messages` on the same session id will respawn the pi process and the message just continues — the harness replays the existing message log back into pi's context as part of spawning it.

The `register_existing_session` helper in `session_manager.rs` (and `api::lookup_session_working_dir`) handles the case where the API restarts: the in-memory session map is empty but the working directory on disk is still there, so we re-seed the map from the DB row.

## 8. Failure modes and how the design absorbs them

| Failure | What happens |
|---|---|
| pi crashes mid-turn | The harness sees EOF on stdout, the per-event timeout fires, the request returns 500. The user row and any partial assistant rows are still in the DB. The next `POST /messages` for the same session will respawn pi. |
| Extension fails to load | Logged at session-startup. Tool calls return errors; the LLM gets the error and can adapt. |
| Tool call times out | The executor records `timed_out: true` in `tool_output` and `is_error: true`. |
| Concurrent writes to messages | `get_next_sequence()` advisory lock serializes per session. The UNIQUE constraint never fires in practice. |
| API restart mid-session | The session's working directory on disk is intact. The next `POST /messages` for that session respawns pi and replays history. |
| Database connection drop | The pool retries with exponential backoff (sqlx defaults). If it can't recover, the request returns 500. The user message may or may not have been written; clients should be idempotent. |
| Streaming bash chunks exceed SSE buffer | Axum's `Sse` keeps flushing; chunks are small (8KB reads). The consumer should keep up. |
