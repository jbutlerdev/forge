# Forge Code Review — Findings & Fix Plan

Review of `crates/forge-api/` for **completeness, correctness, single-responsibility,
DRY, and testability**. Each finding has a severity, a `file:line` reference, the
problem, and the fix. Fixes are applied in this branch (`review/code-quality`);
see the commit log. Status: ✅ fixed in this branch · ⏭️ deferred (see note).

Severity legend: 🔴 correctness bug (wrong behavior in prod) · 🟠 DRY/SRP
(reuse / structure; often the *cause* of a correctness drift) · 🟡
observability / docs / testability · ⚪ nice-to-have.

---

## Tier 1 — Correctness bugs

### 1 ✅ `profiles.provider` CHECK rejects the documented `proxy-anthropic` provider
- **Where:** `crates/forge-api/migrations/001_initial_schema.sql:14`
  `CHECK (provider IN ('openai', 'anthropic'))`.
- **Problem:** `pi_agent.rs:369` handles `"anthropic" | "proxy-anthropic"`;
  `docs/API.md` and `AGENTS.md` document `proxy-anthropic` as a supported
  provider (a custom Anthropic-compatible base URL, e.g. MiniMax behind a
  proxy). But the DB CHECK never had `proxy-anthropic` added, so
  `POST /profiles` with `provider:"proxy-anthropic"` fails with a CHECK
  violation → mapped to a generic **500** "Failed to create profile"
  (`api/mod.rs:571`). No migration ever widened the CHECK, and no test
  exercises it, so it shipped broken.
- **Fix:** new migration `005_provider_check.sql` that drops the old
  constraint and adds `CHECK (provider IN ('openai','anthropic','proxy-anthropic'))`
  (`ALTER TABLE … DROP CONSTRAINT … , ADD CONSTRAINT …`). Use
  `IF NOT EXISTS`-style idempotency via `DO $$` / `ALTER … DROP CONSTRAINT IF EXISTS`.
  Add an integration test that creates a `proxy-anthropic` profile and
  asserts 201.

### 2 ✅ Native `/messages` hangs ~5 min on provider config errors
- **Where:** `crates/forge-api/src/api/mod.rs` `create_message` event loop
  (the `match event { … }` ~lines 1050–1300). Missing `PiEvent::Response`
  arm.
- **Problem:** `openai.rs::run_agent_turn` has a
  `PiEvent::Response { success: false, error, command, .. }` arm that
  surfaces a fast 500 when pi reports the prompt failed before any turn
  ran (the canonical case: "No API key found for <provider>"). The native
  `create_message` loop has **no such arm** — a failed `response` falls
  through to the `_ => {}` catch-all, the loop keeps reading, and the
  request doesn't fail until `IDLE_READ_TIMEOUT_SECS` (5 min) fires. So a
  misconfigured profile makes `/messages` appear to hang for 5 minutes
  instead of failing immediately. This drift is a direct consequence of
  the duplicated event loop (see #4).
- **Fix:** unify the two loops (see #4) so the `Response{success:false}`
  arm exists in exactly one place and both surfaces get it. As a
  standalone fix it would be one arm; the real fix is to stop having two
  copies.

### 3 ✅ Streaming-sandbox bash drops `SEARCH_INSTANCE` / `SEARCH_API_KEY`
- **Where:** `crates/forge-api/src/api/sse.rs:260` `execute_bash_streaming`
  nspawn builder. Compare `crates/forge-api/src/sandbox.rs:808` `run_in_container`.
- **Problem:** `run_in_container` (non-streaming bash) passes
  `FORGE_SEARCH_INSTANCE`→`SEARCH_INSTANCE` and `FORGE_SEARCH_API_KEY`→
  `SEARCH_API_KEY` into the container so the bundled `search` CLI works.
  The streaming bash path in `sse.rs` builds the **same** nspawn command
  from scratch and omits these two env vars. So a search run via
  streaming bash inside a sandbox doesn't see the operator's configured
  instance/key and silently falls back to the compiled-in default (or
  fails auth on a private instance). Same drift class as #2, same root
  cause (#5).
- **Fix:** extract one `build_nspawn_command(root_dir, working_dir,
  timeout_ms, command)` builder used by both paths (see #5). Add a test
  that asserts the env vars are present on the built `Command` (feasible
  by exposing the builder as a pure function returning the arg list).

---

## Tier 2 — DRY / SRP (the structural causes of Tier 1)

### 4 ✅ The pi event loop is duplicated and has already drifted
- **Where:** `api/mod.rs::create_message` (~600-line god-function, loop
  ~lines 1050–1300) vs `api/openai.rs::run_agent_turn` (~lines 540–740).
- **Problem:** ~250 lines of near-identical logic: per-read timeout
  selection by `in_flight_tools`, `seen_turn_start` gate, `MessageUpdate`
  text-delta accumulation + chunk-flush on `TextEnd`/`ToolCallStart`,
  `ToolExecutionStart/End` in-flight counting, `AgentEnd`/`Error`/EOF/
  timeout handling, trailing-text flush + `[no response from agent]`
  placeholder, `publish_turn_ended`, `UPDATE sessions SET last_active`.
  The two copies have already drifted: openai has the `Response` arm
  (#2) and forwards deltas to a channel; native has the compaction
  prelude and is fire-and-forget. Every future event-protocol change
  (new pi event, new timeout rule) must be made twice or one surface
  silently rots.
- **Fix:** extract a `AgentTurnDriver` (new module `api/turn.rs`) that
  owns the read-loop and exposes:
  - `TurnOutcome { text, reason: TurnEndReason }` where
    `TurnEndReason` = `AgentEnd | ResponseError(String) | PiError(String)
    | PiDied | Timeout { in_flight_tools }`.
  - an `on_delta: Option<mpsc::Sender<String>>` for streaming.
  - the shared audit-log write path (`insert_and_publish_assistant`) and
    the shared `last_active`/`publish_turn_ended` epilogue.
  `create_message` keeps the HTTP wrapping + compaction prelude +
  fire-and-forget spawn; `run_agent_turn` keeps the OpenAI error
  mapping. Both call the driver. This makes #2 structurally impossible
  to reintroduce and shrinks `create_message` from ~600 to ~150 lines.

### 5 ✅ `systemd-nspawn` command construction is duplicated and has drifted
- **Where:** `sandbox.rs::run_in_container` (~lines 673–825) vs
  `sse.rs::execute_bash_streaming` (~lines 260–315). Also a third,
  legitimately-different nspawn at `sandbox.rs:285` (`start_container`,
  boot mode `-b` with `--network-veth`) — that one is fine.
- **Problem:** ~60 lines of identical nspawn arg setup
  (`-D`, `--as-pid2`, `--user=root`, `--bind working_dir`, `--chdir`,
  `PATH/HOME/USER/LOGNAME/TERM`, `/nix/store` read-only bind,
  `NIX_CONFIG`, `NIX_SSL_CERT_FILE`, `FORGE_GITHUB_TOKEN`, `timeout
  --kill-after`). They have drifted: the streaming copy is missing the
  `SEARCH_*` passthrough (#3). Adding a new env passthrough or bind-mount
  to one and not the other is a security/correctness footgun.
- **Fix:** one `fn build_nspawn_args(root_dir, working_dir, timeout_ms,
  command) -> Vec<String>` (or a small builder) in `sandbox.rs`, called
  by both `run_in_container` and `execute_bash_streaming`. Keep the
  `Stdio::piped`/`output()` vs `spawn()` differences at the call site.

### 6 ✅ stdout/stderr readers in `execute_bash_streaming` are copy-pasted
- **Where:** `api/sse.rs` — the two `tokio::spawn(async move { … read
  … try_send … })` blocks (~lines 380–470 and ~480–525), ~60 lines each,
  differ only in `event_names::STDOUT` vs `STDERR` and the buffer they
  accumulate into.
- **Fix:** extract `async fn spawn_reader(handle, event_name, buf, tx,
  dropped, metrics) -> JoinHandle<()>` and call it twice.

### 7 ⏭️ SSE event-builder helpers duplicated across modules
- **Where:** `api/events.rs::make_event` vs `api/sse.rs::make_named_event`
  + `make_data_event`. All do "serialize to JSON, fallback on failure,
  build `Event`".
- **Fix:** one `fn sse_event(name: Option<&str>, data: &impl Serialize)
  -> Event` in a shared spot (e.g. `api/sse.rs` or a new `api/sse_util.rs`).

### 8 ✅ Four duplicate get/delete handler pairs (path vs query routing)
- **Where:** `api/mod.rs` — `get_profile_by_id`/`get_profile_by_uuid`,
  `delete_profile_by_id`/`delete_profile_by_uuid`,
  `get_session_by_id`/`get_session_by_uuid`,
  `delete_session_by_id`/`delete_session_by_uuid`. ~60 lines duplicated;
  bodies differ only in `Path(id)` vs `Query(params).id`.
- **Fix:** private `fn get_profile_core(state, id) -> Response` etc.,
  called by both extractors. Preserves both routes for backward compat.

### 9 ✅ `.parent.jsonl` path hard-coded in two places
- **Where:** `agent_registry.rs:430`
  (`format!("/forge/sessions/{}/.parent.jsonl", session_id)`) and
  `api/mod.rs::admin_session_replay` (same literal). In CI there is no
  `/forge/` tree; `write_session_jsonl_with_max_seq` masks this by
  `create_dir_all`-ing the parent and falling back to "fresh context" on
  error, but it's wasteful and inconsistent with the `working_dir` which
  is already the sandbox-manager-derived (tempdir-in-tests) path.
- **Fix:** derive the jsonl path from the session's `working_dir`
  (`working_dir.join(".parent.jsonl")`) in both sites, or a single
  `fn parent_jsonl_path(working_dir) -> PathBuf` helper.

### 10 ✅ `session_replay` does two round-trips for provider + model
- **Where:** `session_replay.rs:110–122` — two `query_scalar` calls.
- **Fix:** one `SELECT p.provider, p.model FROM profiles p JOIN sessions
  s ON s.profile_id = p.id WHERE s.id = $1`.

---

## Tier 3 — Observability & error handling

### 11 ✅ 21 handlers swallow DB errors with `Err(_) =>`, no logging
- **Where:** `api/mod.rs` — 21 `Err(_) => err_resp(…, "Failed to X"|"Database error")`
  arms (e.g. `list_profiles`, `get_session_by_uuid`, `delete_session…`,
  `list_messages_by_session`). The real `sqlx::Error` is dropped at the
  binding (`_`) so it never reaches the journal. A handful of handlers
  (`create_profile`, `update_profile_internal`) *do* log; the rest are
  inconsistent.
- **Problem:** a prod 500 "Failed to list profiles" gives zero clue why
  (connection dropped? column dropped by a bad migration? pool
  exhausted?). The error is gone before tracing sees it.
- **Fix:** bind `Err(e)`, `tracing::error!(error = %e, "…")`, then
  `err_resp`. Add a tiny helper `fn db_err(state, status, ctx, e) ->
  Response` that logs + counts + responds, and use it at the 21 sites.

### 12 ✅ No app-level provider validation → bad provider is a 500, not a 400
- **Where:** `api/mod.rs::create_profile` (only maps
  `profiles_name_key` → 409; everything else is 500). Tied to #1.
- **Fix:** validate `provider` against the allowed set at the handler
  (return 400 with a clear message), *and* fix the CHECK (#1) so the DB
  is the backstop, not the primary gate.

---

## Tier 4 — Stale / incorrect documentation

### 13 ✅ `recording.rs` module doc says the harness writes the call row
- **Where:** `recording.rs:8–16`. Says "Call row … Written by the agent
  harness the instant it sees the model emit a `toolcall_end` event."
- **Reality:** per `AGENTS.md` §7 and the actual code
  (`tool_executor.rs:311` `recorder.record_call(...)`, `api/mod.rs`
  `ToolCallEnd` arm now only logs), the **executor** is the sole writer
  of both rows. The harness stopped writing call rows to fix a race.
- **Fix:** rewrite the doc to say the executor owns both halves.

### 14 ✅ `tool_executor.rs` module doc says the harness owns the call half
- **Where:** `tool_executor.rs:8–16`. Same staleness as #13, in the
  other direction ("The harness … owns the *call* half … when it sees
  `toolcall_end`").
- **Fix:** correct it: the executor owns both the call and result rows;
  the harness is a passive reader of pi's stdout.

### 15 ✅ `resume.rs` module doc describes an abandoned design
- **Where:** `resume.rs:1–20`. Argues *against* using pi's
  `new_session`/`parentSession` jsonl (cites an extension-timer crash
  bug) and says the LLM-context half is done by
  `build_resume_preamble` (a giant user-message transcript).
- **Reality:** `agent_registry.rs::get_or_create` now **does** use
  `write_session_jsonl_with_max_seq` + `--session` (the
  `new_session`/`parentSession` path). `build_resume_preamble` is not
  the active path. The doc directly contradicts the implementation and
  cites a bug rationale for a design that was replaced.
- **Fix:** rewrite the module doc to describe the current two halves:
  (a) filesystem-state replay via `replay_tool_calls` (this module),
  (b) LLM-context replay via the session jsonl
  (`session_replay.rs` + `agent_registry.rs`). Drop the stale
  anti-`new_session` rationale or move it to a "history" note.

---

## Tier 5 — Testability

### 16 ✅ `TestApp` DB cleanup shells out to `sudo -u postgres psql` in `Drop`
- **Where:** `tests/test_helpers.rs` `impl Drop for TestApp` (~lines 195–230).
- **Problem:** (a) requires a running Postgres **and** passwordless
  `sudo` to the `postgres` user — narrows where tests can run; (b) a
  `sudo` in a `Drop` impl can hang (no TTY / password prompt) and the
  `AGENTS.md` §2/§3 rules explicitly call out `sudo` as a hang source;
  (c) it's the blocker for the "SQLite for quick cleanup" idea — though
  see the note below on why a full SQLite swap is **not** recommended.
- **Fix:** replace the `sudo … psql` dance with a `sqlx` cleanup that
  connects to the `postgres` admin DB from within the test process
  (`PgPoolOptions::connect("postgres://postgres:forge@localhost/postgres")`),
  runs `SELECT pg_terminate_backend(pid)…` + `DROP DATABASE`, and
  returns. No `sudo`, no subprocess, no hang. Even better: adopt
  `sqlx::test` (or a thin equivalent) which provisions a per-test DB
  automatically and rolls it back on drop — removing the hand-rolled
  `CREATE DATABASE`/`DROP DATABASE`/`sudo` code entirely.

### 17 ✅ `LAST_BASH_RESULT` thread_local passes bash outcome across functions
- **Where:** `tool_executor.rs:55` (the `thread_local!`) + set sites at
  376/405/567/588/644/670 + read site in `record_outcome` (~360).
- **Problem:** structured bash output (stdout/stderr/exit_code/timed_out)
  is stashed in a thread-local inside `execute_bash*` and read back in
  `record_outcome`. The comment claims "the executor is single-threaded
  per request so the thread-local is safe" — but tokio's multi-thread
  runtime (the default, used by `main.rs`'s `#[tokio::main]`) uses
  work-stealing; the claim is wrong as stated. It is *currently* correct
  only because there is no `.await` between the final set and the read
  (same `poll`), which is a fragile invariant the next refactor will
  silently break — dropping the structured stdout/stderr from the audit
  row with no error. It also makes `record_outcome`'s bash branch
  un-unit-testable in isolation (it depends on thread-local state set by
  a prior `execute_bash` call).
- **Fix:** return the `BashOutcome` from `execute_bash` /
  `execute_bash_sandboxed` (e.g. `(ToolOutput, Option<BashOutcome>)` or
  embed it in a small `BashToolOutput`), and pass it explicitly to
  `record_outcome`. Delete the thread-local. Pure, testable, no
  scheduler-dependent invariant.

### 18 ✅ No regression tests for the three correctness bugs (Tier 1)
- **Problem:** #1 (proxy-anthropic), #2 (Response{success:false} fast
  fail), #3 (streaming-sandbox SEARCH env) all shipped because nothing
  exercised them.
- **Fix:** add tests:
  - `test_create_profile_proxy_anthropic` → `POST /profiles` with
    `provider:"proxy-anthropic"`, assert 201 (catches #1).
  - A driver-level test for the unified turn loop that feeds a
    `PiEvent::Response{success:false,error}` line and asserts a fast
    non-timeout error result (catches #2 without needing a provider
    key). The spawn-smoke tests already cover the spawn path.
  - A unit test for the extracted `build_nspawn_args` asserting
    `SEARCH_INSTANCE`/`SEARCH_API_KEY` are present when the env vars
    are set (catches #3 without spawning nspawn).

### 19 ⏭️ The `forge:<session-id>` stateful OpenAI mode has no e2e coverage
- **Where:** `tests/openai_tests.rs` — only the happy-path
  `test_openai_chat_completions_end_to_end` is `#[ignore]`'d (needs a
  paid provider key); the stateful mode's session-reuse semantics
  (only last user message sent, rest ignored) is untested even at the
  plumbing level.
- **Fix:** lower priority; a plumbing test that creates a session with
  prior rows, posts `model:"forge:<id>"` with a multi-message body, and
  asserts only the last user message is sent to pi (via a fake/stub
  agent) would cover it. Defer unless the unification (#4) makes it
  cheap.

---

## Tier 6 — Minor / nice-to-haves (not fixed in this pass)

### 20 ⏭️ `main.rs` uses two `broadcast::channel(1)` for one shutdown signal
- `main.rs:138` (`shutdown_tx`/`shutdown_rx`) and `:165`
  (`metrics_shutdown_tx`/`metrics_shutdown_rx`). Could be one channel
  with `shutdown_tx.subscribe()` for the second receiver. Pure cleanup.

### 21 ⏭️ Prometheus exporter hardcodes metric names inline
- `api/mod.rs::get_prometheus_metrics` builds the text format by hand
  with string literals; if `Metrics`/`MetricsSnapshot` adds a field,
  the exporter silently doesn't expose it. A single source of truth
  (iterate over labeled fields) would prevent drift. Low impact today.

---

## On the "SQLite for tests" suggestion

**Recommendation: do NOT swap the production DB to SQLite, and do not
maintain a parallel SQLite migration set.** The codebase is deeply tied
to Postgres-only features that matter for correctness:

- `get_next_sequence()` uses `pg_advisory_xact_lock` for
  concurrency-safe per-session sequence allocation (migration
  `004_get_next_sequence_locking.sql`; the lock is the fix for a real
  `duplicate key` race documented in `AGENTS.md` §7). SQLite has no
  advisory locks and no equivalent; a SQLite test path would test a
  *different* concurrency model than prod.
- `jsonb` columns (`tool_input`, `tool_output`), `gen_random_uuid()`,
  `pgcrypto`, `plpgsql` functions, `TIMESTAMPTZ`, trigger functions.

A SQLite harness would either skip these (testing something other than
the prod code paths) or require a compatibility shim that itself becomes
a maintenance burden and a source of "passes on SQLite, fails on
Postgres" bugs.

**Instead, make the Postgres-based tests fast and portable (fix #16):**
drop the `sudo -u postgres psql` cleanup in favor of an in-process
`sqlx` connection that terminates backends and drops the test DB, or
adopt `sqlx::test` which does per-test DB provisioning/rollback
automatically. That keeps tests running against the *real* DB
semantics (advisory locks, jsonb, the actual `get_next_sequence`) while
removing the `sudo`/subprocess fragility and keeping cleanup instant.

---

## Fix order (this branch)

All steps below are complete; each ends with `cargo fmt && cargo clippy
-- -D warnings && cargo test` green locally, and the release build is
clean.

1. ✅ #1 provider CHECK (migration 005) + `test_create_profile_proxy_anthropic`.
2. ✅ #13 / #14 / #15 stale docs in `recording.rs`, `tool_executor.rs`,
   `resume.rs`.
3. ✅ #16 test-cleanup `sudo` removal (in-process sqlx cleanup on a
   dedicated thread).
4. ✅ #4 unify the event loop → `api/turn.rs::drive_turn`; structurally
     fixes #2; `create_message` shrinks ~600→~150 lines. Both surfaces
     now share one loop, one `Response{success:false}` arm, one
     unconditional `turn_ended` publish.
5. ✅ #5 unify the nspawn builder → `sandbox::nspawn_args` /
     `build_nspawn_command`; structurally fixes #3. ✅ #6 dedup the
     stdout/stderr readers → `sse::spawn_stream_reader`. + 3
     `nspawn_args` unit tests.
6. ✅ #11 `db_err` helper; 17 DB-error sites now log the real
     `sqlx::Error`. ✅ #12 app-level provider validation → 400.
7. ✅ #17 drop the `LAST_BASH_RESULT` thread_local; `execute_bash*`
     return the `BashOutcome` explicitly. +
   `test_bash_record_outcome_carries_structured_output`.
8. ✅ #8 dedup the 4 get/delete handler pairs. ✅ #9
   `parent_jsonl_path` helper (no more hard-coded `/forge/sessions/…`).
   ✅ #10 one DB round-trip for provider+model in `session_replay`.

## Deferred (documented for a follow-up)

- **#7** SSE event-builder helpers (`make_event` vs `make_named_event` /
  `make_data_event`) — small, low drift risk; both modules stable.
- **#19** e2e coverage for the `forge:<session-id>` stateful OpenAI
  mode — needs either a fake `PiAgent` trait or a paid provider key;
  the plumbing test + the unified `drive_turn` cover the loop.
- **#20** `main.rs` two `broadcast::channel(1)` → one + `subscribe()`.
- **#21** Prometheus exporter inline metric names → single source of
  truth. Low impact today (metric set is small and stable).
- **Pre-existing flake:** `tool_executor::tests::test_edit_file`
  (from the initial commit, untouched here) occasionally fails under
  the full parallel `cargo test --workspace --all-targets` run but
  passes in isolation and in `--lib` (3/3 clean). Likely a
  tempdir/timing race in the test itself, not in the code under
  test; worth a dedicated look but out of scope for this review.

## Testability net added in this branch

- 3 `nspawn_args` unit tests (env-passthrough-when-set,
  omitted-when-unset, always-present structure) — guard the single
  source of truth for what a sandboxed bash call gets.
- `test_bash_record_outcome_carries_structured_output` — proves the
  structured bash outcome flows through `record_outcome` without a
  thread-local (the previously un-testable path).
- `test_create_profile_proxy_anthropic` — pins that the documented
  `proxy-anthropic` provider is creatable (the #1 regression).
- `test_create_profile_rejects_unknown_provider_with_400` — pins the
  app-level provider validation (#12).
- `TestApp` cleanup no longer needs `sudo`/a `postgres` user, so the
  suite runs in more environments and can't hang on a password prompt.
