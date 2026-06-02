# Changelog

## Unreleased

### Durable resume: replay prior tool calls to restore sandbox working tree

A new `crates/forge-api/src/resume.rs` module closes the last gap in the durable-resume story. Before, when a session was reactivated after a cleanup (or an API restart), the LLM context was rebuilt from the `messages` table via a transcript preamble but the sandbox was re-cloned from the profile's `git_url` / `working_dir` baseline — the prior `write` / `edit` / `bash` side effects were gone. The model had the conversation but not the files, and burned tokens re-deriving the state on the next turn.

The new `replay_tool_calls` function walks the `messages` table in `sequence ASC` order and re-executes every recorded `bash` / `write` / `edit` call against the fresh sandbox. `read` is skipped (read-only, nothing to restore). Tool calls with no matching result row are skipped (interrupted mid-execution in the prior session; the original sandbox may be in a half-applied state). All replays use a `ReplayNoopRecorder` that returns `sqlx::Error` from every `record_call` / `record_result` and logs at `error!` — the replay path must not write to the audit log, and any accidental write is loud in `journalctl` rather than silent corruption.

Wired into `AgentRegistry::get_or_create` between the sandbox creation and the pi spawn, so the working tree is fully restored before the fresh pi starts running. On a brand-new session the messages table is empty and this is a cheap no-op.

Deliberately does **not** introduce pi's `new_session` RPC + `parentSession` jsonl path. That triggers a bug in user-installed extensions that capture the pi ctx in a `session_start` event handler and reference it from a periodic timer (the captured ctx goes stale and the next tick throws an unhandled error that crashes the whole pi process). The transcript-preamble approach for LLM context + the replay path for filesystem state is simpler and avoids that whole class of issues.

5 new unit tests in `resume::tests` (replay id determinism / uniqueness / prefix, noop recorder rejects call and result). 46 total tests pass.

Verified live: created a session, model wrote `/tmp/replay-test-marker.txt` and `/tmp/replay-test-dir/inside.txt`, deleted both files, restarted forge-api to clear the in-memory agent registry, sent a new message. Replay fired before the new pi spawned; 7 tool calls considered, 7 executed, 0 failed, 0 diverged. The model correctly answered "Yes" when asked if the files were still there, and `cat` of both files showed the original content.

### Harness: bump per-read timeout during tool calls (integration fix)

The per-`read_line()` timeout in the harness's event loop was a flat 60 seconds. That was the right value for the "is pi stuck?" check while the model is generating text, but it was wrong while a tool call was in flight: pi emits `tool_execution_start` when a tool begins and `tool_execution_end` when it finishes, and is silent between those two events. A legitimately long tool (a `cargo test --release`, a `git clone` of a large repo, a long compile) would hit the 60s idle timeout and get killed mid-run, even though the tool was making normal progress.

Two new constants in `crates/forge-api/src/api/mod.rs`:

- `IDLE_READ_TIMEOUT_SECS = 300` (5 min) — the per-read timeout when no tool is in flight. The harness bails if pi is silent for 5 min while waiting for a model response.
- `TOOL_READ_TIMEOUT_SECS = 3600` (1 hr) — the per-read timeout while one or more tool calls are in flight. Long enough for any reasonable tool run; if the tool never finishes, the underlying `timeout_ms` on the tool call (or the executor's keep-alive) will catch it.

A `u32` counter (`in_flight_tools`) tracks parallel tool calls: increment on `tool_execution_start`, decrement (saturating) on `tool_execution_end`. The next read uses the appropriate timeout based on whether the counter is > 0. The model can issue parallel tool calls without confusing the counter.

The on-timeout log line now reports which timeout fired and the current in-flight count, so post-mortem is easier.

41 unit tests pass.

### Harness: remove the 5-minute total runtime cap (integration fix)

The harness's event loop in `create_message` had a hard 5-minute `MAX_RUNTIME_SECS` total cap. This was wrong: pi is designed for long agent runs that produce hundreds of tool calls across many turns, and the cap was cutting off legitimate long work in the middle of a turn.

Live case: session `1faa1686-040e-4c80-9baf-749c8b103c48` on 2026-06-02. The model made 123 tool calls in one turn (`keep going with whatever you were doing` → many reads, edits, and bash). The harness's loop hit the 5-minute cap at 09:57:33, exited, and the post-loop code published `turn_ended` to the matrix room — while the model was still making tool calls. From the user's perspective the agent "stopped responding" but the underlying pi process and the executor's HTTP path were still alive and processing tools for another 12 minutes. The cleanup task eventually killed the session 30 minutes later.

The 60-second per-`read_line()` timeout is the real "is pi stuck?" check — if pi goes 60s without emitting *any* event, something is genuinely wrong and we should bail. The total runtime cap is removed; the only remaining termination conditions are `agent_end` (the model is done), the 60s read timeout (pi is stuck), and a `loop_count < 10000` hard safety net.

41 unit tests pass. Verified live: a 6-second `sleep 6 && echo done` bash call completes cleanly with the call row, the 6002ms result row, and the assistant's follow-up text — the harness is now patient about long tool calls.

### Tool audit log: executor is the sole writer (race fix)

The harness used to write the `role='assistant'` call row when it saw the LLM's `ToolCallEnd` event, and the executor wrote the `role='tool'` result row when the tool finished. This created a race: the harness could exit its event loop on `agent_end` before all parallel `ToolCallEnd` events arrived, leaving some calls without a row (we saw this in the wild with two parallel `bash` calls where one call row was missing). The audit log was then incomplete, and durable-resume couldn't see those calls.

The fix is structural: **the executor is now the sole writer of both the call row and the result row**, for every tool path (non-streaming bash, streaming bash, read, write, edit). The harness reads `pi`'s event stream only to detect turn boundaries (`agent_end`) and forward text deltas to the bus for live UI; it no longer writes any rows.

- **Harness** (`api/mod.rs`) — the `ToolCallEnd` arm is reduced to a `tracing::debug!`. The `ToolExecutionEnd` arm stays the same (a `tracing::info!` only). `ToolCallRecord` is no longer imported.
- **Executor** (`tool_executor.rs`) — `ToolExecutor::execute` calls `recorder.record_call` unconditionally before running the tool. The old `ensure_call_row` method (which did a SELECT-then-conditional-INSERT) is gone. The `pool` field is removed; the executor no longer needs the DB connection.
- **Streaming bash** (`api/sse.rs::execute_streaming_tool`) — writes the call row before spawning the process. Used to rely on the harness's `ToolCallEnd` arm for the call row, which is exactly what the race was about. The `pool` parameter on `execute_streaming_tool` is removed.
- **Test helper** (`tool_executor.rs::tests::temp_executor`) — no longer constructs a lazy PgPool.

41 unit tests pass.

### Tool audit log: harness and executor split

The harness and the tool executor each write to `messages` independently, coordinated through a new `ToolRecorder` trait. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) §3 and [`docs/TOOL-AUDIT-LOG.md`](docs/TOOL-AUDIT-LOG.md) for the full design.

- **New** `crates/forge-api/src/recording.rs` — the `ToolRecorder` trait, the `DbToolRecorder` impl, and the `flatten_tool_result` helper. Single chokepoint for both the call row and the result row.
- **Harness** (`api/mod.rs`) — the `ToolCallEnd` arm now calls `state.recorder.record_call(ToolCallRecord { … })`. The `ToolExecutionEnd` arm is reduced to a `tracing::info!`. The old `persist_tool_call` and `persist_tool_result` functions are gone.
- **Executor** (`tool_executor.rs`) — the public entry point is now `ToolExecutor::new(session_id, working_dir, in_sandbox, nix_shell, recorder).execute(tool_call_id, tool_name, input)`. The executor owns the result row and the timing data.
- **Streaming bash** (`api/sse.rs::execute_bash_streaming`) — accepts `session_id` and `recorder` as leading parameters, and at the end of the run hands a `ToolResultRecord` to the recorder. The `tool_output` for streaming bash is `{success, stdout, stderr, exit_code, timed_out, streamed: true}` with `stdout`/`stderr` as `null` (the bytes went to the SSE consumer).
- **AppState** — carries `recorder: Arc<dyn ToolRecorder>`. Constructed once in `main.rs` and shared everywhere.
- **DB schema** — `migrations/003_tool_output.sql` adds `tool_output JSONB` and `duration_ms BIGINT` columns to `messages`. The `Message` row type picks up the matching optional fields.
- **Race fix** — `migrations/004_get_next_sequence_locking.sql` makes `get_next_sequence()` concurrency-safe with `pg_advisory_xact_lock(1, hashtext(session_uuid::text))`. The first-int namespace `1` keeps us out of other apps' advisory-lock space; the lock is auto-released on COMMIT/ROLLBACK. Without this, the split's two writers would have raced on the `(session_id, sequence)` UNIQUE constraint.

### CLI

- **New** `forge message ask <session_id> <text>` — send a message and stream the response by polling `GET /messages`. Renders user/assistant/tool rows with color coding; tool-call and tool-result rows show the `tool_call_id` (the join key), the `tool_input` jsonb, the `tool_output` jsonb, and the `duration_ms`. The poll loop runs for up to 5 minutes total; a turn is considered "done" when the API has been quiet for 5 seconds *after* the agent has emitted at least one text response. See [`docs/CLI.md`](docs/CLI.md).
- **New** `forge message send`, `forge message watch`, `forge message list`. The old top-level `forge messages` is kept as an alias for `forge message list`.
- **Fixed** — `cli/forge.d/common.sh` had a quoting bug: the four `api_*` functions built the curl auth header as a string with embedded single quotes (`-H 'X-API-Key: …'`) and then expanded it unquoted, which turned the header into three malformed args and caused `Failed to deserialize the JSON body into the target type` on the server. Fixed by switching to a bash array: `local -a auth_args=(-H "X-API-Key: $FORGE_API_KEY")` and `"${auth_args[@]}"`.

### Operations

- **Migrations are now consolidated in `crates/forge-api/migrations/`.** `sqlx::migrate!("./migrations")` resolves relative to `CARGO_MANIFEST_DIR` (the crate's own directory), not the workspace root. Stale copies in the workspace root have been removed; new migrations go in the crate dir.
- **Environment file at `/etc/forge/forge.env`** must include `PATH=` with the directory containing the `pi` binary. Without it, the harness spawn fails with "pi: not found".
- **Service unit** — the in-tree `systemd/forge-api.service` ships with `User=forge` / `Group=forge` and security hardening (`ProtectSystem=strict`, `PrivateTmp`, `NoNewPrivileges`, `ProtectHome`). On hosts that don't have a `forge` user, edit the file or run the simpler variant from `docs/OPERATIONS.md`.

### Documentation

- **New** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — the message lifecycle, the ToolRecorder split, the pi rpc event protocol, the audit log schema, the streaming tool path, the session lifecycle, and a failure-mode table.
- **New** [`docs/API.md`](docs/API.md) — per-endpoint request/response shape and curl examples.
- **New** [`docs/CLI.md`](docs/CLI.md) — the example bash client's command reference and output rendering.
- **New** [`docs/OPERATIONS.md`](docs/OPERATIONS.md) — systemd, database, migrations, log/metric endpoints, common failure modes, upgrade procedure.
- **New** [`docs/TOOL-AUDIT-LOG.md`](docs/TOOL-AUDIT-LOG.md) — the `messages` table as an audit log: row shapes, per-tool `tool_output` shapes, and SQL recipes for the most common queries.
- **Updated** `README.md` — current pi package name, real CLI commands, real API URL, the system service story, the migrations location.
- **Updated** `AGENTS.md` — the ToolRecorder architecture, the advisory-lock rationale, the migrations location gotcha, the bash-stdout-capture known limitation, the CLI quoting-bug history, and a debugging checklist.

### Known limitations carried forward

- The `bash` tool's stdout/stderr is not captured into the audit log on the streaming path. `tool_output.stdout` and `tool_output.stderr` are `null`; the model has learned to work around this by writing to a file and `read`ing it back. Fixing this is in the roadmap.
- The `sandbox.rs` subsystem exists but is inactive — tools run on the host, not in containers.

---

## Earlier work (pre-this-release)

See [`docs/AGENT-CONVERSATION-DEBUG.md`](docs/AGENT-CONVERSATION-DEBUG.md) for the 2026-05-30 debugging session that fixed the initial `pi` integration: switching from `--mode json` to `--mode rpc`, fixing the `message` vs `text` stdin field name, removing the pre-send `wait_for_session` block, loosening `tool_call_id` to `serde_json::Value`, fixing the 64KB stderr pipe deadlock, and adding the `seen_turn_start` guard plus `drain_pending_events` to handle buffered events from prior turns.
