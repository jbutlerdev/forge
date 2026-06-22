# Changelog

## Unreleased

### pi spawn: `--skills-dir` → `--no-skills --skill` (pi 0.79.x flag rename)

- **Fix:** `pi_agent.rs` passed `--skills-dir <path>`, a flag pi
  0.79.x renamed to `--skill <path>` (repeatable). pi rejected the
  old flag with `Error: Unknown option: --skills-dir` and exited
  immediately, so every session with a configured skills dir
  (the default — `AgentRegistry` resolves `<repo>/skills`
  automatically) failed to spawn a working pi. The native
  `/messages` handler returned 202 anyway (`PiAgent::spawn` forks
  the process and returns before detecting the crash), leaving the
  failure hidden behind an `#[ignore]`'d test. The OpenAI
  `/v1/chat/completions` path (synchronous) returned 500 "the
  agent process ended unexpectedly".
  - `pi_agent.rs` now passes `--no-skills --skill <path>`.
    `--no-skills` disables pi's global (`~/.pi/agent/skills/`) and
    project (`.pi/skills/`) auto-discovery so the skill set is
    deterministic across machines; `--skill <path>` is additive
    (loads even with `--no-skills`) and scans the directory
    recursively for skill packs. Verified: `--no-skills --skill
    <repo>/skills` loads only the bundled `search-cli` skill
    (count 1), matching the original intent.
  - The `None` branch (`--no-skills` alone, no skills dir) is
    unchanged.
- **pi RPC failures now surface promptly in the OpenAI path:**
  `PiEvent::Response` gained an `error` field, and `run_agent_turn`
  (`api/openai.rs`) returns `AgentError` on a `success: false`
  response instead of ignoring it and waiting for the 5-minute idle
  timeout. A no-key request now fails in ~3s with a 500 mentioning
  the missing API key, instead of hanging 5 minutes.
- **Tests un-ignored / added (the `--skills-dir` bug was hidden by
  an `#[ignore]`'d test):**
  - `tests/pi_spawn_tests.rs` (new): direct-spawn smoke tests that
    spawn pi via `PiAgent::spawn`, send a prompt, and assert pi
    emits a first event (not EOF). This is the regression net for
    flag renames and missing extensions — it catches the
    `--skills-dir` class of bug that `test_send_message` can't (the
    native handler returns 202 before detecting a spawn-time
    crash). Verified: reverting the flag makes the test fail.
  - `test_send_message` un-ignored: CI now installs `pi` and builds
    the extension, so it runs in CI and verifies the HTTP handler
    returns 202.
  - `test_openai_chat_completions_runs_agent_turn` (new, CI-runnable):
    verifies the OpenAI `/v1/chat/completions` plumbing end-to-end
    (auth → model resolution → session → pi spawn → turn loop →
    error handling) without a provider key — asserts a 500
    mentioning the missing API key. The happy-path test (200 +
    content) stays `#[ignore]`'d because it genuinely needs a paid
    provider key.
- **CI (`rust-test` job):** installs
  `@earendil-works/pi-coding-agent@0.79.10` via `npm`, builds the
  forge-tools extension, and sets `FORGE_TOOLS_EXTENSION` so the
  pi-spawning tests run against a real pi + extension.
- Docs updated (`skills/README.md`, `docs/SEARCH-TOOL.md`,
  `AGENTS.md` §5/§13) to reference `--no-skills --skill` and the
  new CI pin + bump workflow.

### OpenAI-compatible API

- Forge is now usable as an OpenAI drop-in. Two new endpoints,
  `POST /v1/chat/completions` and `GET /v1/models`, speak the
  OpenAI Chat Completions protocol so the `openai` SDK, LangChain,
  Continue, and any OpenAI-speaking chat UI can drive a forge agent
  without learning the native API. Point the client at
  `http://localhost:8080/v1` and use a forge profile name as the
  `model`.
  - Auth is `Authorization: Bearer <forge-api-key>` (the standard
    OpenAI header); the native `X-API-Key` is also accepted on the
    `/v1/*` surface. The same `sk_forge_…` key works on both.
  - `model: "<profile-name>"` is **stateless**: a fresh ephemeral
    session is created per request, the request's `messages` are
    replayed as context, the agent runs one turn, and the final
    assistant text is returned. `model: "forge:<session-id>"` is
    **stateful**: reuses an existing session and sends only the
    last user message.
  - `stream: true` emits standard `chat.completion.chunk` SSE
    events followed by `data: [DONE]`.
  - Agentic turns (internal `bash`/`read`/`write`/`edit` calls)
  run to completion before the response is returned; tool calls
  are forge-internal and not surfaced as OpenAI `tool_calls`.
  Every assistant text chunk is persisted to the audit log, so
  OpenAI-driven turns are indistinguishable from native
  `/messages`-driven turns in the `messages` table and the live
  SSE event stream.
  - `api/auth.rs` `extract_auth_user` is generalized to accept
  either `Authorization: Bearer` or `X-API-Key` and made
  `pub(crate)`; `auth_middleware` accepts either header's
  presence. Real key validation (hash + DB lookup) runs on the
  OpenAI endpoints, not just the middleware presence check.
- New module `crates/forge-api/src/api/openai.rs`; routes wired in
  `api::create_router`. 18 unit tests + 12 integration tests (the
  full happy-path test is `#[ignore]`'d because it needs the `pi`
  binary). Documented in `docs/API.md`, `README.md`, and the
  `AGENTS.md` §12 API surface table.

### Continuous integration + GitHub Releases

- **`.github/workflows/ci.yml`** — seven jobs on every push to
  `main` and on every pull request: `cargo fmt --check`,
  `cargo clippy --workspace --all-targets --locked -- -D
  warnings`, `cargo check`, `cargo test` against a Postgres
  15 service container, `shellcheck` over `cli/` and
  `scripts/`, `npm ci && npm run build` for the TypeScript
  extension, and a `cargo build --release` smoke test.
  Concurrency-cancels stale runs per ref. The `cargo test` job
  installs `pgcrypto` ahead of the suite because
  `001_initial_schema.sql` calls `CREATE EXTENSION IF NOT
  EXISTS pgcrypto` against the admin connection.
- **`.github/workflows/release.yml`** — builds the Rust
  binary (`cargo build --release --bin forge-api`) and the
  TypeScript extension on every `v*` tag push (and on manual
  `workflow_dispatch`); packages both as tarballs, emits
  `SHA256SUMS.txt`, and creates a GitHub Release via
  `softprops/action-gh-release@v2` with auto-generated
  release notes. Tag-push releases are published, manual
  dispatches are draft / prerelease.
- **Project-policy fixes the CI surfaces.**
  - `cargo fmt --all` was applied across the workspace
    (782 diff hunks); the file was last touched under an
    older rustfmt, so the first CI run would have failed.
  - `clippy::all -D warnings` now passes. The 50-odd lints
    it flagged were a mix of pre-existing drift from a
    newer rustc (`needless_borrows_for_generic_args`,
    `manual_div_ceil`, `irrefutable_let_patterns`), plus
    dead-code / public-API cleanup:
    - `events.rs::make_event` now returns `Event` instead
      of `Result<Event, Infallible>` (it never failed).
    - `agent_registry.rs` gains an `is_empty()` alongside
      the existing `len()`.
    - `sse.rs` and `bus.rs` get targeted `#[allow(...)]`
      for `too_many_arguments` and `large_enum_variant`
      (the function signature is the API of the streaming
      tool path; boxing the inner `Message` would touch
      every consumer).
    - The unused `SessionEntry` enum was deleted from
      `session_replay.rs`; it was a leftover from an
      earlier jsonl-shape experiment.
    - `ToolExecutor::new` gained a `sandbox: Option<Arc<…>>`
      argument; the in-test `temp_executor()` was updated
      to pass `None`.
  - `Cargo.lock` is now committed (it was gitignored — wrong
    for a binary crate, and the CI cache key needs a stable
    lockfile to be effective). Regenerated with 299 packages.
  - `test_helpers.rs::Drop` now uses
    `db_url.split('/').next_back()` instead of `Iterator::last`
    (which clippy 1.96 correctly flagged as needlessly walking
    the whole iterator on a `DoubleEndedIterator`).

### Auth: tighten the public-endpoint allowlist

`api/mod.rs::auth_middleware` no longer exempts `/profiles`,
`/sessions`, `/messages`, `/api-keys`, or their `:id` variants
from the `X-API-Key` check. The only paths that bypass auth
now are `/health`, `/metrics`, `/metrics/prometheus`,
`/auth/register`, `/auth/login`, `/auth/logout`,
`/tools/execute`, and `/tools/execute/stream` (the last two
are still called by the in-process `forge-tools` extension
without a key, and by tests). `test_create_profile_unauthorized`
now passes — it was previously failing because the test author
expected the auth check and the code didn't enforce it.

### Tests: fix the `simple-status` calls and ignore the pi-spawn one

- `e2e_tests.rs` and `integration_tests.rs` had three
  `app.get("/simple-status")` calls left over from an earlier
  endpoint that no longer exists; they now hit `/health`,
  which the API has had since the beginning. The test
  comments were also updated to match.
- `test_send_message` is `#[ignore]`'d: it exercises
  `POST /messages` end-to-end, which spawns a `pi`
  subprocess. The CI runner doesn't (and shouldn't) install
  the pi agent. Run the test on a host with `pi` on `PATH`
  via `cargo test -- --ignored` if you need to exercise the
  spawn path.

## Unreleased

### Durable resume: cap the replay budget and drop malformed rows

Two follow-ups to the durable-resume replay path that were
caught when resuming a 1166-message session in production.

1. **`replay_tool_calls` skips `bash` calls with original
   `timeout_ms > 30s`.** Long-running build / test commands
   (`cargo build --release`, `npm install`, long curls, etc.)
   don't create the kind of filesystem state the replay is
   trying to restore, and honoring the original 10-minute
   timeout in the replay path blocks `get_or_create` from
   returning and the user from getting a response. The
   threshold is `REPLAY_BASH_MAX_ORIGINAL_TIMEOUT_MS = 30_000`
   (constant, with a unit test pinning the value).
2. **`replay_tool_calls` has a 60s total wall-clock budget.**
   A session with hundreds of short `write` / `edit` / bash
   calls could still take minutes to replay end-to-end. After
   `REPLAY_TOTAL_BUDGET_SECS = 60` elapses we stop walking
   the messages and proceed to spawn pi; the model can
   re-derive any state we skipped on the next turn.
3. **`write_session_jsonl_with_max_seq` drops orphaned tool
   results.** Some sessions have `tool` rows whose
   `tool_call_id` doesn't appear on any assistant row in the
   audit log (e.g. an interrupted turn, or an earlier bug
   that let a result row exist without its call row). The
   model provider rejects the entire conversation with
   `invalid params, tool result's tool id ... not found` if
   the jsonl includes such a row, which is much worse than
   dropping one tool result. We collect the set of
   `tool_call_id`s seen on assistant rows in a first pass,
   then skip any `tool` row whose `tool_call_id` isn't in
   that set. The model can re-run the call on its next turn
   if it actually needs the result. The count of dropped
   rows is logged at INFO.
4. **Harness uses `TurnStart` (not `AgentStart`) as the gate
   for the per-turn event loop.** `agent_start` is emitted
   once at the beginning of the pi process's lifetime; on
   a durable-resume spawn the loaded session's
   `agent_start` / `agent_end` events get replayed before
   the user's new prompt is even sent. Gating on
   `agent_start` caused the harness to honor the loaded
   `agent_end` and exit the loop with "No response" before
   the model ever saw the new turn. `turn_start` is per-turn
   and only fires when the model starts processing the new
   prompt, which is the signal we actually want.

### Durable resume: spawn pi with `--session <jsonl>` for structured context restore

The previous `switch_session` RPC approach (write jsonl, send RPC, wait for Response event) was replaced with the simpler `pi --session <path>` CLI flag. pi opens the file as its active session at startup, so the fresh pi sees the full prior conversation as structured messages before it ever processes a prompt. The model can't be in a "post-replacement weird state" because there is no replacement — pi starts in the loaded session.

What changed:

1. **`pi_agent::spawn` accepts a `session_path: Option<PathBuf>` field on `PiConfig`.** When `Some`, the spawned `pi` gets `--session <path>` (loads that jsonl as the active session). When `None`, `pi` gets `--no-session` (in-memory ephemeral session). The two are mutually exclusive in pi's CLI; we pick one.

2. **Removed the entire `PiInput::SwitchSession` / `PiAgent::switch_session` / `SwitchSessionResult` machinery.** No more RPC round-trip to load the prior session — pi handles it natively at startup.

3. **`agent_registry::get_or_create` writes the jsonl BEFORE spawning pi**, then passes the path through `PiConfig`. If the messages table is empty (brand-new session), `session_path` is `None` and pi starts with an empty context. If the write fails, we log a warning and fall back to a fresh context.

4. **`write_session_jsonl_with_max_seq` excludes the just-inserted user prompt from the jsonl.** The harness inserts the user's prompt into the `messages` table before calling `get_or_create`; without the cap, the jsonl would contain the prompt twice (once from the loaded session, once from the stdin prompt). The cap is `MAX(sequence) - 1`.

Verified live: a session with a 4-message prior conversation was killed via `systemctl restart forge-api`, then resumed. The fresh pi loaded the jsonl via `--session`, the model saw the prior context, and correctly answered a follow-up question by recalling the user's stated preference. Brand-new sessions still spawn pi with `--no-session` (in-memory ephemeral context, no on-disk session file in `~/.pi/agent/sessions/`).

Side note: when pi is spawned with `--session <path>`, pi writes its subsequent activity (the new user prompt, the model's response) to the same file as it goes. This is harmless — the `messages` table is still the source of truth and is always re-read on resume, so pi's auto-save to the jsonl doesn't affect correctness. It's actually convenient for debugging: `cat /forge/sessions/<id>/.parent.jsonl` shows the full conversation in pi's native format.

### Durable resume: switch_session for structured context restore

Replaces the previous "prepend the transcript as one giant user message" preamble with pi's `switch_session` RPC, which loads a prior session jsonl as the new active session. Each prior turn ends up as a discrete structured message in pi's internal tree (UserMessage / AssistantMessage with text and toolCall blocks / ToolResultMessage), with `tool_input` and `tool_output` preserved as proper jsonb on the wire rather than flattened to plain text.

Two key changes in this commit:

1. **`--no-extensions` on the pi CLI** (`pi_agent.rs::spawn`). Disables pi's auto-discovery of user-installed extensions under `~/.pi/agent/extensions/`; explicit `-e` paths below still work. This is a stability and security boundary: a user extension that captures the pi ctx in a `session_start` handler and references it from a periodic timer (e.g. `setInterval`) goes stale after the `switch_session` RPC, and pi's `assertActive` throws an unhandled error that kills the whole pi process. Disabling auto-discovery makes forge's runtime deterministic across machines. The forge-tools extension is still loaded via the explicit `--extension <path>` flag.

2. **`switch_session` instead of `new_session`** for loading the prior context. The `new_session` RPC only records `parentSession` for lineage tracking; it does NOT load the parent's messages into the model context. We confirmed this empirically: pi returned success and the model still said "I don't have access to session history." `switch_session` is the real "load this session file as the new active session" verb.

Removed the preamble machinery entirely (`PiAgent::set_resume_preamble`, `PiAgent::take_resume_preamble`, `agent_registry::build_resume_preamble`, the `effective_content` formatting in `api/mod.rs`). The user's first prompt is now sent verbatim to pi, and pi sees the prior conversation as proper structured messages.

`SwitchSessionResult` is the outcome enum for the new RPC call (`Ok` / `Cancelled` from an extension veto / `Err` for any other failure). On `Cancelled` or `Err`, the harness surfaces a warning and the user gets a fresh context (the prior conversation is still in the `messages` table for the user's `forge message list` to surface).

41 unit tests pass. Verified live: a fresh session with 1 prior message in the jsonl successfully loaded via `switch_session` and the model responded to the new prompt in ~2.5s. A multi-turn session (7 prior messages) successfully recalled the user's name and work language from the prior turn after switch_session reload — the model sees the full prior conversation as structured messages and continues from where the prior turn left off.

### Durable resume: replay prior tool calls to restore sandbox working tree

A new `crates/forge-api/src/resume.rs` module closes the last gap in the durable-resume story. Before, when a session was reactivated after a cleanup (or an API restart), the LLM context was rebuilt from the `messages` table via a transcript preamble but the sandbox was re-cloned from the profile's `git_url` / `working_dir` baseline — the prior `write` / `edit` / `bash` side effects were gone. The model had the conversation but not the files, and burned tokens re-deriving the state on the next turn.

The new `replay_tool_calls` function walks the `messages` table in `sequence ASC` order and re-executes every recorded `bash` / `write` / `edit` call against the fresh sandbox. `read` is skipped (read-only, nothing to restore). Tool calls with no matching result row are skipped (interrupted mid-execution in the prior session; the original sandbox may be in a half-applied state). All replays use a `ReplayNoopRecorder` that returns `sqlx::Error` from every `record_call` / `record_result` and logs at `error!` — the replay path must not write to the audit log, and any accidental write is loud in `journalctl` rather than silent corruption.

Wired into `AgentRegistry::get_or_create` between the sandbox creation and the pi spawn, so the working tree is fully restored before the fresh pi starts running. On a brand-new session the messages table is empty and this is a cheap no-op.

5 new unit tests in `resume::tests` (replay id determinism / uniqueness / prefix, noop recorder rejects call and result). 41 total tests pass.

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
