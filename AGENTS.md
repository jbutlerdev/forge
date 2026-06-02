# Forge — Agent Development Guide

This document is for AI coding agents (and humans) working on the Forge codebase. Read it before making changes; it covers the architecture, the contracts between modules, and the operational quirks that bit us during development.

---

## 1. Project overview

Forge is a Rust API server that hosts durable AI coding agents. The flow:

1. Client calls `POST /messages`.
2. The API server spawns (or reuses) a long-lived [`pi`](https://github.com/badlogic/pi-mono) subprocess for the session.
3. pi talks to the LLM. When the model wants a tool, the `forge-tools` extension forwards to the API's `POST /tools/execute` endpoint.
4. The API runs the tool, records the result to the audit log, and returns the result to the extension, which returns it to pi.
5. The harness (an async task in `api/mod.rs`) consumes the event stream from pi's stdout, persists each event to the audit log, and broadcasts progress.

The two halves — harness (records calls) and executor (records results) — write to the same `messages` table independently and are linked by `tool_call_id`. See §5.

---

## 2. CRITICAL: do not start the server yourself

The `forge-api` process **runs as a systemd service** (`forge-api.service`). Your tool execution environment may already have it running in the background. If you start a second copy manually:

- It will fail to bind port 8080.
- Even if you set a different port, your test client will hit a different process than the one writing the audit log.
- If you do manage to start it, two services racing on the same DB will corrupt the message sequence.

Always use `sudo systemctl {status,restart,stop,start} forge-api` and `sudo journalctl -u forge-api -f` for logs. Never invoke `/opt/forge/forge-api` directly.

```bash
sudo systemctl status forge-api        # is it running? what PID?
sudo systemctl restart forge-api       # after a code change
sudo journalctl -u forge-api -n 200    # last 200 log lines
sudo journalctl -u forge-api -f        # follow live
```

---

## 3. Tech stack

| Component | Technology |
|---|---|
| API server | Rust 1.75+ on `axum` 0.7, `tokio`, `sqlx` 0.8, `tracing` |
| Database | PostgreSQL 15+ |
| LLM agent | `pi` (Node.js), package `@earendil-works/pi-coding-agent` v0.74+ |
| Bridge extension | TypeScript at `extensions/forge-tools/`, built to `dist/index.js` |
| Reference CLI | Bash at `cli/forge` |

`pi` is run with `--mode rpc` for line-delimited JSON over stdio. The harness writes `{type:"prompt", message:"…"}` and reads back per-turn events (`agent_start`, `turn_start`, `message_start`, `message_update`, `message_end`, `turn_end`, `agent_end`, plus `toolcall_start/delta/end` and `tool_execution_start/end`).

---

## 4. Repository layout

```
forge/
├── crates/forge-api/
│   ├── src/
│   │   ├── main.rs            # Entry point: builds AppState, starts axum
│   │   ├── lib.rs             # Module declarations, error type
│   │   ├── api/
│   │   │   ├── mod.rs         # HTTP handlers + harness event loop
│   │   │   ├── auth.rs        # Register / login / API-key middleware
│   │   │   └── sse.rs         # /tools/execute/stream + streaming bash
│   │   ├── db/                # SQLx row types (Message, Profile, Session, ...)
│   │   ├── pi_agent.rs        # pi subprocess: spawn, stdin/stdout pump, event types
│   │   ├── agent_registry.rs  # Arc<Mutex<HashMap<Uuid, Arc<PiAgent>>>>  + FORGE_TOOLS_EXTENSION
│   │   ├── tool_executor.rs   # ToolExecutor::new(session_id, working_dir, in_sandbox, nix_shell, recorder)
│   │   │                      #   .execute(tool_call_id, tool_name, input) -> Result<ToolOutput, ToolError>
│   │   ├── recording.rs       # ToolRecorder trait + DbToolRecorder + flatten_tool_result
│   │   ├── session_manager.rs # /forge/sessions lifecycle
│   │   ├── sandbox.rs         # systemd-nspawn wrapper (inactive in this build)
│   │   ├── observability.rs   # Request / tool-execution counters
│   │   └── logging.rs         # tracing_subscriber wiring
│   └── migrations/            # Embedded via sqlx::migrate!("./migrations") at startup
├── extensions/forge-tools/
│   ├── src/index.ts           # TypeScript source
│   └── dist/index.js          # Built artifact loaded by the agent at runtime
├── cli/
│   ├── forge                  # Top-level dispatcher
│   └── forge.d/               # Per-subcommand sources (common.sh, profile.sh,
│                              #   session.sh, message.sh)
├── systemd/forge-api.service  # Example unit file (User=forge)
└── docs/                      # Architecture, operations, CLI, and API references
```

### Where the migrations actually live

`sqlx::migrate!` resolves its path relative to `CARGO_MANIFEST_DIR`, which is `crates/forge-api/`. **The migrations directory the binary reads is `crates/forge-api/migrations/`**, not the workspace root. We previously had stale copies at both locations; the working tree is now consolidated in `crates/forge-api/migrations/`.

`docs/OPERATIONS.md` covers the migration workflow in detail.

---

## 5. Architecture: the executor is the sole writer of tool rows

The most important architectural idea in this codebase: **the executor is the sole writer of tool-related rows in `messages`, and the harness is a passive reader of `pi`'s event stream.**

```
pi stdout (events)              /tools/execute  (HTTP from extension)
        │                                  │
        ▼                                  ▼
 api/mod.rs                        tool_executor.rs
  PiEvent::ToolCallEnd              ToolExecutor::execute
  → log only (no DB write)          → recorder.record_call(
                                         ToolCallRecord { ... }
                                       )   ← call row written here
                                      → run tool
                                      → recorder.record_result(
                                          ToolResultRecord { ... }
                                        )   ← result row written here
        │                                  │
        ▼                                  ▼
        ▼                              messages
        ▼                          (linked by tool_call_id,
        ▼                           no race: only the executor
                                     writes tool rows)
```

The harness still reads `pi`'s stdout — it's how the harness detects when a turn ends (`agent_end`) and how it forwards text deltas to the bus for live UI. But it no longer writes any rows. This eliminates a class of bugs where the harness could exit its event loop before all parallel `ToolCallEnd` events arrived, leaving some calls without a row. The executor is guaranteed to see every call (it has to run the tool anyway) and writes the call row before running.

The non-streaming `bash` path and the `read`/`write`/`edit` tools go through `ToolExecutor::execute` in `tool_executor.rs`, which writes the call row. The streaming `bash` path is in `execute_streaming_tool` / `execute_bash_streaming` in `api/sse.rs` and writes its own call row there (it doesn't go through `ToolExecutor::execute`).
```

### `ToolRecorder` (in `recording.rs`)

```rust
#[async_trait]
pub trait ToolRecorder: Send + Sync {
    async fn record_call(&self, record: ToolCallRecord) -> Result<Message, sqlx::Error>;
    async fn record_result(&self, record: ToolResultRecord) -> Result<Message, sqlx::Error>;
}
```

`DbToolRecorder` is the only implementation; it allocates a per-session sequence via `get_next_sequence(session_id)` and writes the row inside a single transaction. Constructed once in `main.rs` and shared as `Arc<dyn ToolRecorder>` through `AppState`. The `record_call` / `record_result` return the inserted `Message` so callers can publish it to the bus (see §6).

### Why the executor owns both the call and result rows

The executor is the **sole writer of all tool-related rows** in `messages`. The harness used to write a call row on `ToolCallEnd`, but that created a race: the harness could exit its event loop on `agent_end` before all parallel `ToolCallEnd` events arrived, leaving some calls with no row. The executor is guaranteed to see every call (it has to run the tool anyway) so it writes the call row before running, and the result row after. The audit log is now self-consistent: every `role='tool'` row has a matching `role='assistant'` row with the same `tool_call_id`, linkable for replay (see [`session_replay.rs`](crates/forge-api/src/session_replay.rs)).

The harness's job is reduced to: forward `pi`'s events to the bus (text deltas for live UI, `agent_end` for turn-end detection), and call the LLM with the next user prompt. It no longer writes any rows.

This split is enforced by:

- `ToolExecutor::execute` in [`tool_executor.rs`](crates/forge-api/src/tool_executor.rs) always calls `recorder.record_call` before running the tool (covers `read`/`write`/`edit` and the non-streaming `bash` path).
- `execute_streaming_tool` in [`api/sse.rs`](crates/forge-api/src/api/sse.rs) does the same for streaming `bash` (it doesn't go through `ToolExecutor::execute`).
- The harness's `ToolCallEnd` arm in [`api/mod.rs`](crates/forge-api/src/api/mod.rs) just logs the event; it does not call `recorder.record_call`.

The schema knowledge (column names, the `[tool_call:<name>]` content marker, the per-session sequence allocator) is encapsulated in `recording.rs` — neither the harness nor the executor needs to know about it. Swapping the backend (e.g. a separate `tool_invocations` table, or an event bus) means writing one new `ToolRecorder` and passing it in `main.rs`. The harness and executor don't change.

### `messages` table column reference

| Column | Type | Used for |
|---|---|---|
| `id` | uuid PK | default `gen_random_uuid()` |
| `session_id` | uuid FK | references `sessions.id` ON DELETE CASCADE |
| `sequence` | int | per-session monotonic via `get_next_sequence(session_id)`, **UNIQUE** with session_id |
| `role` | text | `user` / `assistant` / `tool` / `system` (CHECK constraint) |
| `content` | text | human-readable text. For tool-call rows: `[tool_call:<name>]`. For tool-result rows: the flattened text content of the result. |
| `tool_name` | text | for `role='assistant'` rows: the tool being called. For `role='tool'` rows: the tool that ran. |
| `tool_input` | jsonb | for `role='assistant'` rows: the model's arguments. NULL for `role='tool'`. |
| `tool_call_id` | text | pi's id for this call. **The join key between call row and result row.** |
| `tool_output` | jsonb | for `role='tool'` rows: `{output, error, success}` for non-bash tools; `{stdout, stderr, exit_code, success, timed_out, streamed}` for bash. NULL for call rows. |
| `duration_ms` | bigint | for `role='tool'` rows: wall-clock duration of the tool call. |
| `created_at` | timestamptz | default `now()` |

### `get_next_sequence()` is concurrency-safe

The function is `SELECT COALESCE(MAX(sequence), 0) + 1 FROM messages WHERE session_id = $1` wrapped in `pg_advisory_xact_lock(1, hashtext(session_uuid::text))`. The advisory lock serializes concurrent sequence allocations per session and is auto-released on COMMIT/ROLLBACK. The first-int namespace `1` keeps us from colliding with other code that uses advisory locks.

**Do not** rewrite this function without the lock. The original (unlocked) version was fine in the single-writer era but blew up with `duplicate key value violates unique constraint "messages_session_id_sequence_key"` the moment the harness and executor started writing concurrently.

---

## 6. The pi event protocol

pi is launched with `--mode rpc`. The harness writes prompts to stdin and reads per-turn events from stdout. Event shapes live in `pi_agent.rs::PiEvent` (use `#[serde(rename_all = "camelCase")]` on the enum and on the field types — pi sends camelCase).

| Pi event | What the harness does with it |
|---|---|
| `response` (response to a `prompt` command) | logged at INFO |
| `agent_start` / `agent_end` | turn boundary markers; `agent_end` ends the message loop only if `seen_turn_start` was true |
| `turn_start` | sets `seen_turn_start = true`, allows `agent_end` to terminate the loop |
| `message_start` | a new assistant message is beginning |
| `message_update` (TextStart/TextDelta/ThinkingStart/ThinkingDelta/ToolCallStart/ToolCallDelta/ToolCallEnd) | persisted to `messages` for tool calls; text deltas are streamed to subscribers |
| `message_end` | finalizes the assistant text row |
| `turn_end` | log only |
| `tool_execution_start` / `tool_execution_end` | the executor owns the result row; the harness only logs (no longer writes) |
| `extension_ui_request` | not currently handled |

### `toolcall_end` carries the call row

`assistantMessageEvent.toolCall = { id, name, arguments }`. The harness constructs:

```rust
ToolCallRecord {
    session_id,
    tool_call_id: id.clone(),
    tool_name: name.clone(),
    tool_input: serde_json::from_str(&arguments).unwrap_or(Value::String(arguments)),
    content: format!("[tool_call:{}]", name),
}
```

### `tool_execution_end` carries the result

`{ toolCallId, toolName, result: { content: [{type:"text", text:"…"}], isError }, isError }`. The harness no longer touches the result row — the executor (which actually ran the tool) is the source of truth and writes it via `recorder.record_result(...)`.

### Why we use `--mode rpc`, not `--mode json`

`--mode json` is a one-shot "print everything as JSON, then exit when stdin closes" mode. It is not designed for a long-lived per-session agent. `--mode rpc` is the proper line-delimited JSON RPC protocol that keeps pi running between prompts.

---

## 7. The forge-tools extension

`extensions/forge-tools/src/index.ts` registers four tools with pi (`bash`, `read`, `write`, `edit`). On each tool call, it POSTs to `${FORGE_API_URL}/tools/execute` (or `/stream` for bash) with the tool call id and the parsed arguments. The tool output comes back, and the extension returns it to pi in `AgentToolResult` shape.

The `pi` package's `registerTool` callback signature is:

```ts
execute(toolCallId: string, params: Static<TParams>, signal, onUpdate, ctx): Promise<AgentToolResult>
```

Note the **first** arg is the tool call id, not the params. If you swap the order, every tool call will be dispatched with a bogus id and the call/result linkage will break in subtle ways.

Build the extension with `cd extensions/forge-tools && npm run build`. The runtime loads `dist/index.js`, **not** the TypeScript source.

The path to the extension is resolved at startup:

1. If `FORGE_TOOLS_EXTENSION` is set, that absolute path is used.
2. Otherwise, fall back to a hard-coded path under `~/.nvm/versions/node/.../lib/node_modules/@earendil-works/pi-coding-agent/`.

If the extension isn't found, the harness logs an error at startup and tool calls will fail.

---

## 8. Timeouts

| Component | Default | Configurable via |
|---|---|---|
| Bash tool | 30s | `input.timeout_ms` in the tool call |
| pi initialization | 30s | `wait_for_event(pi_event::Session, 30s)` in `pi_agent.rs` |
| pi event read | 60s | per `read_line()` on pi's stdout |
| pi total runtime | 5 min | `wait_for_agent_end(...)` in `api/mod.rs` |
| Session inactivity cleanup | 30 min | `session_manager.rs` cleanup task |

If pi takes longer than 5 minutes total for a single turn, the harness returns a timeout error to the caller. The user prompt row is still in `messages`; the assistant will be missing or truncated. Bump the constant in `api/mod.rs` if you need longer.

---

## 9. Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | yes | — | Postgres connection string (sqlx) |
| `FORGE_API_URL` | no | `http://localhost:8080` | base URL the extension uses to call back |
| `FORGE_TOOLS_EXTENSION` | no | hard-coded fallback | absolute path to the built `forge-tools/dist/index.js` |
| `RUST_LOG` | no | `info` | `tracing` filter, e.g. `forge_api=debug,sqlx=warn` |
| `PATH` | yes (in env file) | — | must include the directory containing the `pi` binary |
| `ANTHROPIC_API_KEY` | yes* | — | only required by profiles that use Anthropic as the provider |
| `OPENAI_API_KEY` | yes* | — | only required by profiles that use OpenAI |

`*` Required depending on the profile's `provider`. With `provider=proxy-anthropic` the API key is set on the profile itself (e.g. a `minimax-anthropic/MiniMax-M3` model behind a proxy), so neither global env var is consulted.

---

## 10. API surface (quick reference)

| Method | Endpoint | Notes |
|---|---|---|
| `GET`   | `/health` | `200 OK` if the service is up |
| `GET`   | `/metrics` | JSON metrics |
| `GET`   | `/metrics/prometheus` | Prometheus text format |
| `POST`  | `/auth/register` | `{email, name, password}` |
| `POST`  | `/auth/login` | returns API key + user |
| `POST`  | `/profiles` | create |
| `GET`   | `/profiles` | list |
| `GET`   | `/profiles/get?id=<uuid>` | one |
| `PATCH` | `/profiles/update?id=<uuid>` | partial update |
| `DELETE`| `/profiles/delete?id=<uuid>` | |
| `POST`  | `/sessions` | create |
| `GET`   | `/sessions` | list |
| `GET`   | `/sessions/{id}` | one |
| `DELETE`| `/sessions/delete?id=<uuid>` | |
| `POST`  | `/messages` | `{session_id, content}` — async, spawns pi |
| `GET`   | `/messages?session_id=<uuid>` | full message list with tool_call_id / tool_input / tool_output |
| `POST`  | `/tools/execute` | one-shot tool execution |
| `POST`  | `/tools/execute/stream` | SSE stream of stdout/stderr/tool_end |

For the per-endpoint request/response shape see [`docs/API.md`](docs/API.md). For the CLI reference see [`docs/CLI.md`](docs/CLI.md).

---

## 11. Common development tasks

### Add a new migration

```bash
# 1. Create the file - the NNN_ prefix matters, the description is the filename after _
$EDITOR crates/forge-api/migrations/005_my_change.sql

# 2. Build and restart. sqlx::migrate! picks it up at compile time
cargo build --release -p forge-api
sudo systemctl restart forge-api

# 3. Verify it ran
sudo -u postgres psql forge -c "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"
```

If your migration rewrites a function or column, **always** use `IF NOT EXISTS` / `CREATE OR REPLACE` so re-running the migration (e.g. on a partially-applied state) doesn't fail.

### Add a new tool

1. Add a `pub async fn my_tool_execute(cwd: &Path, input: Value) -> Result<MyToolOutcome, ToolError>` in `tool_executor.rs`.
2. Add a new arm to the `match` in `ToolExecutor::execute()` that calls your function and then `record_outcome`.
3. Define a `MyToolOutcome` struct that produces a `ToolResultRecord` with structured `output` jsonb.
4. Register the tool in `extensions/forge-tools/src/index.ts` and rebuild the extension.
5. Add the tool name to the profile's allowed `tools` list in the DB if you gate tools per-profile (currently all tools are registered but the LLM can call whichever it wants).
6. End-to-end test with a fresh session, query the DB to verify the call and result rows line up.

### Add a new API endpoint

1. Add a handler in `api/mod.rs` (or `api/sse.rs` for streaming).
2. Add it to the `create_router()` builder in `lib.rs` or `main.rs`.
3. If it touches `AppState`, extend the `AppState` struct and its constructor.
4. Document it in `docs/API.md`.

### Bump the pi version

1. `npm install -g @earendil-works/pi-coding-agent@<new-version>`
2. Check the package's `dist/` for any breaking changes to the `registerTool` signature, `AgentToolResult` shape, or the rpc event types.
3. Update `extensions/forge-tools/src/index.ts` if needed, then `npm run build` in that directory.
4. Update `FORGE_TOOLS_EXTENSION` (or the fallback path in `agent_registry.rs`) if the install path changed.
5. Rebuild forge-api, restart, run a multi-tool test session, query the DB to verify call/result pairing still works.

---

## 12. Debugging checklist

When something goes wrong, work through these in order:

1. **Is the service running?** `sudo systemctl status forge-api`
2. **What do the logs say?** `sudo journalctl -u forge-api -n 200 --no-pager`
3. **Is the extension path resolvable?** Look for `forge-tools` log lines from the agent at session start; if it's missing, the tool calls will fail with no useful error.
4. **Did migrations apply?** `sudo -u postgres psql forge -c "SELECT * FROM _sqlx_migrations ORDER BY version;"` — every row should have `success = t`.
5. **Is the audit log intact?** Pick a session, query `messages` ordered by `sequence`, and check that (a) sequences are contiguous, (b) every `tool_call_id` on a `tool` row also appears on an `assistant` row.
6. **Are the tool calls being made?** Look for `[forge-tools] Tool call: bash/read/write/edit` lines in the journal.
7. **Are the tool calls returning?** Look for `Tool <name> completed: success=...` lines.

Common errors:

- `duplicate key value violates unique constraint "messages_session_id_sequence_key"` — the sequence allocation is racing. This used to mean `get_next_sequence()` lost its advisory lock; re-check the function body.
- `pi timed out waiting for response after 60s` — pi's stdout is stalled. Check that the agent's `pi` process is alive (`ps -ef | grep pi`) and that the extension is loaded.
- `Forge tools extension not found at <path>` — `FORGE_TOOLS_EXTENSION` is wrong or the extension wasn't built. `cd extensions/forge-tools && npm run build`.
- `Failed to connect to the database` — `DATABASE_URL` wrong or Postgres not running. `sudo systemctl status postgresql`.

---

## 13. Things that previously bit us

These are documented in `docs/AGENT-CONVERSATION-DEBUG.md` and various commits, but worth re-summarizing for the next agent:

- **pi was launched in `--mode json`** (one-shot print mode) instead of `--mode rpc` (the long-lived protocol). Fixed.
- **Stdin field name**: `PiInput::Prompt.text` vs `message`. The rpc protocol wants `message`. Fixed with `#[serde(rename = "message")]`.
- **Pre-send `wait_for_session` was blocking forever** because pi only emits the session event *after* a prompt, not before. Removed.
- **Stderr pipe was 64KB** and would deadlock pi when it logged a lot. Fixed by `Stdio::inherit()`.
- **`seen_turn_start` guard** is required in the harness event loop: a previous turn's `turn_end`/`agent_end` events might still be buffered and would otherwise terminate the new turn's loop. There's also `PiAgent::drain_pending_events()` called after acquiring the per-session lock to flush stragglers.
- **`tool_call_id` from the extension is sometimes an object, not a string.** Loosened the field to `serde_json::Value` and added a `tool_call_id_str()` helper that handles string, object-with-`id`, and null.
- **The bash streaming stdout is NULL** in the audit log. The streaming-bash path (`api/sse.rs::execute_bash_streaming`) emits stdout as SSE chunks to the consumer but never captures it into the result row. The `tool_output` for bash is `{exit_code, success, timed_out, streamed: true, stdout: null, stderr: null}`. The model has learned to work around this by redirecting to a file and `read`ing it back.
- **Migrations were in the wrong directory** (`migrations/` at the workspace root instead of `crates/forge-api/migrations/`). `sqlx::migrate!("./migrations")` resolves to `CARGO_MANIFEST_DIR`. Migrations are now consolidated in `crates/forge-api/migrations/`.
- **The CLI's `api_post` and friends had an unquoted `$auth_header` expansion** that turned `-H 'X-API-Key: sk_forge_…'` into three malformed args, sending the wrong request and getting a 422 with `Failed to deserialize the JSON body`. Fixed by switching to a bash array: `local -a auth_args=(-H "X-API-Key: $FORGE_API_KEY")` and `"${auth_args[@]}"`.
