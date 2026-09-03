# Historical security/bug review (May 2026, against HEAD `75716a8`) — kept for reference only; open items should be tracked as issues.

# Forge Bug Report

Reviewed: `crates/forge-api` (Rust API + pi harness), `extensions/forge-tools` (TS), `web/` (PWA), `crates/forge-api/migrations`, `cli/`, `migrations/`. Focus: correctness & completeness of functionality. All line references are against the current working tree (HEAD `75716a8`).

---

## CRITICAL — Security

### 1. Authentication is presence-only on the entire native API surface; provider API keys are leaked; tool execution is fully unauthenticated
**Status:** ✅ FIXED — `auth_middleware` now runs the real hash + DB lookup (`extract_auth_user`) for every protected route (applied via `from_fn_with_state` in `build_app`); `Profile.api_key` and `Session.override_api_key` are masked in serialization (sentinel `sk-***redacted***`, kept on update); `/tools/execute*` removed from the middleware allowlist and now require a real API key or the process-scoped tool token forge-api hands its pi subprocesses.
`crates/forge-api/src/api/mod.rs:1792-1860` — the `auth_middleware` only checks that an `X-API-Key` / `Authorization: Bearer` header *exists* (any value passes; the comment at :1816 says real validation "runs inside each handler via `auth::extract_auth_user`"). But `extract_auth_user` is **only called in 3 modules** (`api/auth.rs`, `api/openai.rs`, `api/router.rs` — verified by grep). None of these call it:
- `create_profile` / `list_profiles` / `get_profile_by_uuid` / `delete_profile_by_uuid` / `update_profile_internal` (`mod.rs:751-1010`) — and `Profile` (`db/mod.rs:84-102`) serializes `api_key` raw, so **`GET /profiles` returns provider LLM API keys in plaintext to anyone with a bogus header** (the web UI stores any key in localStorage and never validates, `web/app.js:36`).
- `create_session` / `list_all_sessions` / `get_session_core` / `delete_session_core` (`mod.rs:1015-1160`)
- `create_message` / `list_messages_by_session` / `execute_tool` (`mod.rs:1204-1410`)
- `stream_tool_execution` (`api/sse.rs:1090-1160`)
- Admin endpoints: `self_update` (`mod.rs:250-360` — **arbitrary binary replacement**), `reset_sandbox`, `admin_session_replay`

Worse, `/tools/execute` and `/tools/execute/stream` are in the middleware's **allowlist** (`mod.rs:1799-1800`), so they require *no header at all* — anyone who can reach port 8080 can POST a tool call for any session UUID and run arbitrary commands. Since session UUIDs are enumerable via `GET /sessions` (fake key), this is unauthenticated RCE on the host for sandbox-less sessions.

### 2. `/admin/self-update` is reachable with a forged key
**Status:** ✅ FIXED — `/admin/self-update`, `/admin/sandbox-reset`, and `/admin/session-replay` all require `role == "admin"` (the middleware stashes the validated `AuthenticatedUser` in request extensions; the handlers check it). Non-admin keys get 403. The middleware also now requires a real key (not just presence) for these paths.
`api/mod.rs:250` — the endpoint writes the request body to `/opt/forge/forge-api.staging` and restarts the service. Given bug #1, any value in the `X-API-Key` header authorizes it. Even with proper auth, this endpoint should be restricted to admin keys.

---

## HIGH — Correctness

### 3. `get_or_create` has a TOCTOU race: two concurrent messages on a cold session spawn two pi processes
**Status:** ✅ FIXED — per-session spawn lock + double-checked locking in `AgentRegistry::get_or_create`; lock entries cleaned up on success/remove.
`crates/forge-api/src/agent_registry.rs:255-530` — the map is read (line 263-272), then the slow path runs (sandbox create, tool-call replay, jsonl write, `PiAgent::spawn`, lines 274-520), and only then is the entry inserted (line 525). Two concurrent `POST /messages` (or `/v1/chat/completions`) on a session with no live pi both see an empty map, both spawn pi, both insert (last wins), and both drive turns against their own pi. Result: two divergent conversation states, duplicate/misordered assistant rows, and an orphaned pi process that keeps its stdout buffer. There is no per-session spawn lock.

### 4. Streaming bash on the host-side path ignores `timeout_ms` — command runs forever
**Status:** ✅ FIXED — the whole child lifetime (spawn→wait) runs inside the outer `tokio::time::timeout`, and the host path (streaming + non-streaming) got the inner `timeout --kill-after=2 Ns` wrapper the nspawn path had.
`crates/forge-api/src/api/sse.rs:417-460` — the outer `tokio::time::timeout(timeout_duration, ...)` wraps `async { spawn_result }` (line 417), i.e. only the **spawn** call, which returns immediately. `child.wait().await` (line 454) has **no timeout**. The host-side path (when the session has no container: `sandboxed_root` is `None` at line 371-376) builds plain `bash -c <cmd>` with no inner `timeout` wrapper (unlike the nspawn path, which wraps `timeout --kill-after=2`). A `sleep 10000` with `timeout_ms: 5000` runs for the full 10k seconds, holding the tool lock and the SSE connection open.

### 5. `bash_record_content` panics on multi-byte UTF-8 at the 8 KiB boundary → result row never recorded
**Status:** ✅ FIXED — truncation now walks back to a char boundary instead of panicking.
`crates/forge-api/src/api/sse.rs:671` — `truncated.truncate(MAX_TOTAL)` where `MAX_TOTAL = 8192`. `String::truncate` panics if the byte index isn't a char boundary; any bash output with non-ASCII characters crossing byte 8192 panics inside the spawned recording task. The panic aborts the task → the audit-log `tool_result` row is never written for that call and the SSE stream just dies. Extremely easy to trigger (e.g. `printf '%*s' 8190 '' | tr ' ' 'é'`).

### 6. Byte-slicing panics (UTF-8 boundary) in several request paths
**Status:** ✅ FIXED — char-boundary-safe truncation in `tool_executor.rs` (command preview), `router.rs` (7 sites), and `embedding.rs` (reranker doc).
- `crates/forge-api/src/tool_executor.rs:515` — `&input.command[..100]` panics on a >100-byte bash command containing multi-byte chars → the whole `/tools/execute` request aborts (task panic), no result row.
- `crates/forge-api/src/api/router.rs:839` — `&snippet[..200]` (user message text) in `build_routing_prompt`.
- `crates/forge-api/src/api/router.rs:381, 946, 952, 1034` — `&text[..text.len().min(500)]` on LLM output inside `make_openai_call`/`make_anthropic_call` error paths.
- `crates/forge-api/src/embedding.rs:144` — `&document[..500]` in `rerank` (session summaries with multibyte chars).

All four panic on a non-boundary byte index; in `create_message`/router paths this is a 500/connection-reset, in `execute_bash` it's the tool call dying mid-flight.

### 7. `cp -r -p src dst` nests the source dir instead of copying contents
**Status:** ✅ FIXED — both `copy_directory` impls now use `cp -r -p <src>/. <dst>` (contents, not the dir).
`crates/forge-api/src/session_manager.rs:466-478` and `crates/forge-api/src/sandbox.rs:937-950` — both `copy_directory` implementations run `cp -r -p <src> <dst>` with `<dst>` **already existing** (created by `create_dir_all` just before, `session_manager.rs:92-94` / `sandbox.rs:354-360`). GNU `cp` copies *into* an existing destination directory, so the working tree ends up at `/forge/sessions/<id>/<basename-of-working_dir>/…` — one level deeper than the agent's cwd. The pi process and bash tools run in `/forge/sessions/<id>`, so the agent sees an empty dir with one subdirectory. This affects every profile that uses `working_dir` (no `git_url`).

### 8. Logout deletes the key before reading its user_id → audit log entry never written
**Status:** ✅ FIXED — single `DELETE … RETURNING user_id`; `logout` also accepts `Authorization: Bearer` now.
`crates/forge-api/src/api/auth.rs:342-353` — `DELETE FROM api_keys WHERE key_hash = $1` runs first; the subsequent `SELECT user_id FROM api_keys WHERE key_hash = $1` (line 350) always returns `None` because the row is already deleted. `audit::logout(...)` is therefore unreachable. (Also, `logout` only reads `X-API-Key`, not `Authorization: Bearer`, so Bearer-only clients can't log out.)

### 9. Session-summary refresh stops after 30 messages → semantic routing goes permanently stale
**Status:** ✅ FIXED — refresh threshold uses `COUNT(*)`; the LIMIT-30 window is only the summarizer transcript.
`crates/forge-api/src/api/router.rs:691-718` — `current_count = messages.len()` where the SELECT is `LIMIT 30` (line 691). Once a session exceeds 30 messages, `current_count` is capped at 30 while `session_embeddings.message_count` is also 30, so `current_count - last_count < 10` (line 718) is always true and the summary/embedding is never regenerated again. `refresh_session_summary` should count all messages (e.g. a `COUNT(*)`), not the truncated window length.

### 10. nix-shell wrap is *not* skipped in the sandbox path despite the warning claiming it is
**Status:** ✅ FIXED — the raw command is passed into the container (both streaming and non-streaming) when `nix_shell` is set; warning now fires only when the wrap is genuinely skipped.
`crates/forge-api/src/tool_executor.rs:539-558` — `wrap_command` is applied unconditionally at the top of `execute_bash` (line 540), and the wrapped `nix-shell -p … -c '…'` command is passed to `execute_bash_sandboxed` (line 557), *after* logging "nix-shell wrap is skipped in the container path" (lines 545-551). Same duplicated bug in `api/sse.rs:319-335` (streaming path passes the wrapped command to `build_nspawn_command` at line 338). Every bash call for a `nix_shell` profile in a sandboxed session executes inside the container, where `nix-shell` isn't guaranteed to exist and the read-only `/nix/store` makes `-p pkg` installs fail → "nix-shell: command not found" / store-write errors on every call.

### 11. Provider env vars are wrong for every non-Anthropic provider
**Status:** ✅ FIXED — `google`/`gemini` → `GOOGLE_API_KEY`/`GOOGLE_BASE_URL`; `openai` → `OPENAI_BASE_URL`; anthropic-family stays `ANTHROPIC_*`.
`crates/forge-api/src/pi_agent.rs:338-360`:
- The `_ =>` arm sets `ANTHROPIC_API_KEY` for providers `google`, `gemini`, `proxy`, `custom` — pi's google/gemini providers read `GOOGLE_API_KEY`, not `ANTHROPIC_API_KEY`.
- `base_url` is *always* exported as `ANTHROPIC_BASE_URL` (line 358), even for `provider = "openai"` — pi's openai provider reads `OPENAI_BASE_URL`, so a profile with `provider=openai, base_url=<custom>` silently ignores its base URL and calls api.openai.com.

(Impact is reduced only when `models.json` carries the provider config; the code makes no attempt to use `--api-key` or provider-correct env names.)

### 12. Ctrl-C doesn't shut down the HTTP server
**Status:** ✅ FIXED — `axum::serve(…).with_graceful_shutdown(…)` now drains and exits on ctrl-c/SIGTERM.
`crates/forge-api/src/main.rs:214-227` — the ctrl-c handler only sends on `shutdown_tx` (stopping the cleanup task); `axum::serve(...)` is never given `with_graceful_shutdown`, so on Ctrl-C the API keeps serving indefinitely and `main` never reaches the shutdown sends at lines 224-225.

### 13. SSE "lagged" events don't recover lost rows (server doesn't re-query, client doesn't reload)
**Status:** ✅ FIXED — the `Lagged` branch re-queries the DB (`sequence > last_seq`), re-emits the missed rows as `message` events, then re-anchors the high-water mark. Web client comment updated.
`crates/forge-api/src/api/events.rs:200-215` — the module docstring (lines 22-30) promises "the handler responds by re-querying the database for catch-up", but the implementation only emits a `lagged` event and continues forwarding from the *current* bus position — the missed rows are never replayed. The web client's handler is `console.warn` only (`web/app.js:753-759`, comment even says "just reload history to be safe" but no reload happens), and `state.lastSeq` is only advanced by delivered messages. Any rows dropped by a lagging consumer are permanently invisible until the connection closes and reconnects.

### 14. `system`-role rows replay as empty user messages
**Status:** ✅ FIXED — `forge_to_pi_message` returns `Option`; `system` rows are skipped entirely instead of becoming empty user rows.
`crates/forge-api/src/session_replay.rs:498-503` — `("system", _)` is documented as "Skip them on replay" but actually emits `{"role":"user","content":""}`. An empty user message enters the new pi's conversation tree (and Anthropic-style APIs can reject empty user content). This is known downstream — `api/openai.rs` works around it by mapping `system` → `user` rows (openai.rs:551-556) — but the replay path itself still emits the empty row.

### 15. Extension double-executes tools on any streaming failure
**Status:** ✅ FIXED — no fallback re-POST on mid-stream failure; a stream ending without `tool_end` now surfaces as a failure, not a silent success.
`extensions/forge-tools/src/index.ts:259-271` — on a non-OK response *or any network error mid-stream*, `executeToolStreaming` falls back to `executeToolNonStreaming`, which re-POSTs the same `tool_call_id`. If the first streamed attempt already ran the command (network died mid-way, or the harness's read-timeout killed pi while the tool was in flight), the command runs twice — duplicate side effects (git push, appends, installs) and **two `tool` rows with the same `tool_call_id`** in the audit log. The `parseSSEStream` also defaults `success = true` when the stream ends before a `tool_end` event (line 181), so an aborted stream surfaces to the model as a successful (partial) run.

### 16. `get_next_sequence`'s advisory lock doesn't protect the two-step call sites
**Status:** ✅ FIXED — all remaining sites (`insert_and_publish_assistant`, `dispatch_message`, `openai::insert_history_row`) are single-statement `INSERT … VALUES ($1, get_next_sequence($1), …)`.
The function (`migrations/004_get_next_sequence_locking.sql`) uses a **transaction-scoped** `pg_advisory_xact_lock`. `recording.rs:125,143` correctly call it *inside* the INSERT (one implicit transaction). But `api/mod.rs:231` + `:247`, `api/mod.rs:1209` + `:1224`, and `api/openai.rs:607` + `:615` first `SELECT get_next_sequence($1)` in an autocommitted query (lock released on commit), then INSERT the returned value in a separate statement. Two concurrent dispatches can harvest the same sequence → `duplicate key value violates unique constraint "messages_session_id_sequence_key"` — exactly the failure class migration 004 was written to fix. The fix is to use the `INSERT … VALUES ($1, get_next_sequence($1), …)` form everywhere, or wrap select+insert in one transaction.

---

## MEDIUM

### 17. Model switcher destroys untracked files — doc claims workspace is "preserved"
**Status:** ✅ FIXED — `update_session` sets a one-shot preserve flag; the next `get_or_create` uses `create_container_preserving` (no wipe, no baseline clone) and skips the tool-call replay.
`crates/forge-api/src/api/mod.rs:1107-1130` (update_session) claims the working dir is preserved and only the agent is torn down. But the next message runs `get_or_create` → `create_container` → `sandbox.rs:349-357` — for any profile with `git_url` or an existing `working_dir`, `will_populate` is true and the whole session dir is `remove_dir_all`'d and re-cloned/re-copied. Only tool calls recorded in `messages` are restored (via `resume::replay_tool_calls`); untracked files, user-side edits, and manual changes are silently deleted.

### 18. `destroy_container` fails (and leaks the rootfs) when the in-memory map is empty
**Status:** ✅ FIXED — falls back to the canonical on-disk rootfs path when the in-memory map has no entry.
`crates/forge-api/src/sandbox.rs:255-272` — `containers.remove(&session_id).ok_or(NotFound)?` aborts before deleting the on-disk rootfs, even though `get_container` (line 210-248) rehydrates the map from disk on restart. After an API restart, `delete_session_core` (`mod.rs:1145-1147`) and `cleanup_task` (`main.rs:88-91`) call `destroy_container`, get Err, ignore it (`let _ =`), and leave `/forge/sandbox/forge-<uuid>/` on disk forever.

### 19. Resume replay drops the nix_shell environment
**Status:** ✅ FIXED — the profile's `nix_shell` is now passed to the replay `ToolExecutor`.
`crates/forge-api/src/resume.rs:126` — `_nix_shell: Option<String>` is accepted but discarded; `ToolExecutor::new(..., None, ...)` is used. Replayed `bash` calls run without the `nix-shell` wrapper that the original calls had, so a profile whose tooling only exists inside its nix shell re-executes commands that now fail with "command not found" — replay silently diverges from the recorded state. (The caller does pass `profile.nix_shell.clone()` at `agent_registry.rs:360-362`; the parameter is just unused.)

### 20. `ReplayStats.diverged` is never incremented; skipped long bash calls are counted as `failed`
**Status:** ✅ FIXED — deliberate skips count as a new `skipped` counter; `diverged` increments on replay-vs-recorded outcome mismatch.
`crates/forge-api/src/resume.rs:89, 352` — `diverged` is defined, logged, and never set anywhere; the "skip long-running bash" branch does `stats.failed += 1` (line 175), conflating "deliberately skipped" with "failed".

### 21. Debug `eprintln!` left in production handler
**Status:** ✅ FIXED — removed.
`crates/forge-api/src/api/auth.rs:457` — `eprintln!("DEBUG: delete_api_key called with id: {}", key_id);` prints the raw UUID to stderr on every key deletion.

### 22. Migration 002 inserts an admin user with a placeholder argon2 hash
**Status:** ✅ FIXED — migration 002 now contains a real argon2id hash for `admin123`; `login` treats an unparseable stored hash as invalid credentials (no more 500).
`crates/forge-api/migrations/002_users_and_api_keys.sql:40-42` — `'$argon2id$v=19$m=19456,t=2,p=1$placeholder$placeholder'` is not a valid hash (`PasswordHash::new` fails), so any login attempt with `admin@forge.local` throws `AuthError::PasswordHash` → 500 instead of "invalid credentials", and the admin account can never be used (nor its password reset via the API).

### 23. `update_session`/`create_session` workflows clone the repo twice per new session
**Status:** ✅ FIXED — `create_session_dir` only creates the dir; the clone/copy happens once, in `create_container`, on the first message.
`create_session` → `SessionManager::create_session_dir` (`session_manager.rs:100-108`) clones; then the first message → `get_or_create` → `SandboxManager::create_container` (`sandbox.rs:349-364`) wipes and re-clones. For large repos this doubles first-message latency; for `working_dir` profiles it also turns the correct first clone into the nested-copy bug (#7) on the second pass.

### 24. Durable-resume jsonl uses profile provider/model, ignoring session overrides
**Status:** ✅ FIXED — query now `COALESCE(s.override_provider, p.provider)` / model.
`crates/forge-api/src/session_replay.rs:157-160` — `SELECT p.provider, p.model` — assistant entries in the `.parent.jsonl` carry the *profile's* provider/model even when the session has `override_provider`/`override_model` set (migration 006). Metadata only (pi uses CLI flags for the actual call), but the file misrepresents which model produced the messages.

### 25. Stale duplicate migration at repo root
**Status:** ✅ FIXED — deleted `migrations/001_initial_schema.sql`.
`migrations/001_initial_schema.sql` — a stale copy of the first migration (all later migrations exist only in `crates/forge-api/migrations/`). `sqlx::migrate!("./migrations")` resolves to `CARGO_MANIFEST_DIR` so the root copy is unused, but its continued presence invites exactly the wrong-directory confusion documented in `AGENTS.md` §6.

### 26. Service worker shell cache can never match asset requests
**Status:** ✅ FIXED — `sw.js` matches by leading-slash pathname (`SHELL_PATHNAMES`), so the cache-first branch works.
`web/sw.js:26-47` — `SHELL` entries are relative (`"./app.js"`), and the fetch handler tests `SHELL.includes(url.pathname)` (`"/app.js"` — no match) and `SHELL.includes("./" + url.pathname)` (`".//app.js"` — double slash, no match). The cache-first branch is dead code; static assets (`app.js`, `styles.css`, …) are always network-only, so the "offline relaunch" promise fails (index.html loads, scripts don't).

### 27. Metrics updates spawn a tokio task per request
**Status:** ✅ FIXED — synchronous updates under a `std::sync::Mutex`; no task spawn, snapshots are immediately consistent.
`crates/forge-api/src/observability.rs:63-85` — `inc_requests`/`inc_errors`/`inc_tool_execution` each `tokio::spawn` a task to update the per-key maps. Under load this spawns multiple tasks per request, and snapshots taken immediately after an increment can miss it (the unit tests sleep 10-50ms to make this pass).

### 28. Harness loop cap exits as "success"
**Status:** ✅ FIXED — loop cap now surfaces as `TurnEndReason::PiError` ("pi is wedged") instead of a clean `AgentEnd`.
`crates/forge-api/src/api/turn.rs:214-220` — if `MAX_LOOP_ITERATIONS` (10000) is hit, the loop breaks with `reason` still at its default `AgentEnd`, flushing the turn as a normal completion even though pi is still producing events (likely wedged).

---

## LOW / Notes

- `api/middleware.rs` (`AuthExt`) is dead code — no handler uses it.
- `SessionManager::pull_git_changes`/`get_git_status`/`get_ahead_behind` are dead code (never called).
- `voice.rs:60-70` — `transcribe` doc claims "we don't buffer the whole audio in memory", but `field.bytes().await` buffers each part fully (then copies to a `Vec`).
- `main.rs:36` default log filter is `forge_api=debug,tower_http=debug`, diverging from the documented `info` default (`AGENTS.md` §11).
- `sse.rs` `record_outcome` for a timed-out/bash error writes content `"[error] …"` and drops the partial stdout/stderr that was captured (only `tool_output` jsonb keeps them).
- `tool_executor.rs:539-557` — `execute_bash` passes the nix-wrapped command into the container and logs the "skipped" warning on *every* call even when it isn't skipped (see #10).
- `session_manager.rs:210-212` `clone_repository` doesn't pass `--branch` at clone time (checks out after, unlike `sandbox.rs`), so `git_ref` set to a commit SHA works differently across the two clone paths.

---

## Top 10 to fix first

1. #1/#2 — real API-key validation on every protected handler (or middleware-level hash+DB lookup); stop serializing `profiles.api_key`.
2. #4 — put `child.wait()` inside the timeout on the host-side streaming bash path.
3. #3 — per-session spawn mutex in `get_or_create`.
4. #5/#6 — replace byte slicing/truncation with char-boundary-safe truncation.
5. #16 — single-statement `get_next_sequence` + insert everywhere.
6. #7 — `cp -r -p <src>/. <dst>` (contents, not the dir itself).
7. #9 — uncapped message count for summary refresh.
8. #10/#11 — fix nix-shell/sandbox interaction and provider env vars.
9. #15 — don't re-execute on mid-stream failure (reuse the partial result or surface it as error).
10. #12 — wire `with_graceful_shutdown` so Ctrl-C/systemd stops actually stop the server.
---

## FIX STATUS — LOW / Notes (additive ledger; original text above untouched)

- `api/middleware.rs` (`AuthExt`) is dead code — **✅ fixed** (file deleted; `pub mod middleware` removed from `api/mod.rs`).
- `SessionManager::pull_git_changes`/`get_git_status`/`get_ahead_behind` are dead code — **✅ fixed** (removed, along with the now-unreferenced `clone_repository`, `copy_directory`, `GitStatus` struct, and `SessionError::Git` variant).
- `voice.rs:60-70` — **✅ fixed** (doc comment corrected to describe the actual per-part buffering instead of claiming a streaming proxy).
- `main.rs:36` default log filter — **✅ fixed** (default is now `info`).
- `sse.rs` `record_outcome` partial stdout/stderr on timed-out/bash errors — **✅ fixed** (streaming-bash failure rows keep partial output in `content`; `tool_output` jsonb already had it).
- `tool_executor.rs:539-557` nix-wrap "skipped" warning — **✅ fixed** (warning fires only in the sandbox path when `nix_shell` is set; raw command is passed into the container — see #10).
- `session_manager.rs:210-212` `clone_repository` / `--branch` at clone time — **✅ fixed** (the dead `session_manager` clone was removed entirely; only the sandbox manager's clone remains, which passes `--branch` to `git clone` — see #23).

## FIX STATUS — Second pass (auth phase)

- #1 — **✅ fixed**: real middleware auth everywhere; `/tools/execute*` no longer allowlisted. The `forge-tools` extension sends `X-API-Key` from `process.env.FORGE_API_KEY` (set by `pi_agent` spawn from `AgentRegistry::tool_auth_token` — the operator's `FORGE_API_KEY` or a random per-process token). `scripts/test-api.sh` now sends the operator key; `ContainerEnv` passes `FORGE_API_KEY` into sandboxed bash so the AGENT_GUARD's documented `curl … /admin/self-update` keeps working. `web/app.js` was already 401-aware (stale localStorage key → login screen), so no client change was needed beyond the masked-key round-trips, which the update handlers now treat as "keep existing".
- #2 — **✅ fixed**: admin-role check on `/admin/self-update`, `/admin/sandbox-reset`, `/admin/session-replay`.

## FIX STATUS — Validation pass (post-fix verification, additive ledger; original text above untouched)

Verified against the working tree (HEAD `75716a8` + uncommitted fixes): every fix inspected in
source, plus `cargo check` (clean, no warnings) and 175 passing tests / 1 ignored (lib 102,
integration 34, openai 19, web 14, e2e 6, pi-spawn 2).

- #1 — **✅ verified**: `auth_middleware` (`extract_auth_user`) applied via `from_fn_with_state`
  in `build_app`; `/tools/execute*` not in the allowlist (tool token path instead); `Profile.api_key`
  / `Session.override_api_key` masked via `serialize_redacted_secret`; redacted value on update =
  keep existing; `create_profile` rejects the sentinel. Every route in `create_router` (incl.
  `events::stream_session_events`, `/router/message`) is under the middleware; only the static/SPA
  fallback is public (intentional).
- #2 — **✅ verified**: admin-role check (403) on all three `/admin/*` handlers.
- #3 — **✅ verified**: `spawn_locks` per-session `Mutex` + double-checked locking in `get_or_create`;
  entries removed on success/remove paths. `test_concurrent_sessions` (e2e) passes.
- #4 — **✅ verified**: `child.wait()` is inside the outer `tokio::time::timeout`; both host and
  nspawn paths wrap the command in inner `timeout --kill-after=2`. `test_bash_timeout` passes.
- #5 — **✅ verified**: `bash_record_content` walks back with `is_char_boundary` before truncate.
- #6 — **⚠️ PARTIAL**: all listed sites are char-boundary-safe (`chars().take(N)`, safe
  `char_indices` parsing in `router.rs`), **but one unlisted site was missed** — see
  "New findings" below (`embedding.rs:221`).
- #7 — **✅ verified**: `cp -r -p <src>/. <dst>` in `sandbox.rs::copy_directory`; session_manager
  copy removed entirely.
- #8 — **✅ verified**: single `DELETE … RETURNING user_id` → `audit::logout` reachable; Bearer
  accepted via `extract_api_key_header`.
- #9 — **✅ verified**: `SELECT COUNT(*) FROM messages …` (uncapped) for refresh threshold.
- #10 — **✅ verified as documented**: raw (unwrapped) command handed to the container on both
  streaming and non-streaming paths when `nix_shell` is set; warning fires only when the wrap is
  genuinely skipped. ⚠️ Known remaining limitation — see "New findings" below (nix_shell is
  effectively inert for sandboxed sessions; comment says "TODO").
- #11 — **✅ verified**: provider-correct env names for key AND base URL (`openai` →
  `OPENAI_API_KEY`/`OPENAI_BASE_URL`; `google`/`gemini` → `GOOGLE_*`; default → `ANTHROPIC_*`).
  Both `pi_spawn_tests` pass against pi 0.83.0.
- #12 — **✅ verified**: `axum::serve(…).with_graceful_shutdown(ctrl_c + shutdown_tx)`.
- #13 — **✅ verified**: `Lagged` branch re-queries `sequence > last_seq`, re-emits rows as
  `message` events, re-anchors `last_seq`, emits `lagged {missed, recovered}`; `web/app.js`
  dedupes by sequence and logs the recovered count.
- #14 — **✅ verified**: `forge_to_pi_message` → `Option`; `("system", _) => None`; caller skips
  `None` (parentId chain jumps over skipped rows).
- #15 — **✅ verified**: no fallback re-POST in `executeToolStreaming`; end-of-stream without
  `tool_end` forces `success = false` + explanatory error. Confirmed in `src/index.ts` AND the
  rebuilt `dist/index.js` (deterministic `tsc`; all fix markers present; `git diff dist` = the
  fix only).
- #16 — **✅ verified**: `SELECT get_next_sequence` no longer exists anywhere outside INSERT
  VALUES; all sites (`mod.rs` insert_and_publish_assistant / dispatch_message,
  `openai.rs::insert_history_row`, `recording.rs` ×2) are single-statement.
- #17 — **✅ verified**: `update_session` → `preserve_working_dir_on_next_spawn`; `get_or_create`
  consumes flag, uses `create_container_preserving` (no wipe when dir has content) and skips
  tool-call replay.
- #18 — **✅ verified**: `destroy_container` falls back to `base_dir/forge-<uuid>` on map miss
  (still stops the container + removes the rootfs).
- #19 — **✅ verified**: replay `ToolExecutor::new(..., nix_shell.clone(), ...)`.
- #20 — **✅ verified**: `skipped` counter (2 sites), `diverged` incremented on outcome mismatch
  (2 sites), logged at the end.
- #21 — **✅ verified**: no `eprintln!` anywhere in `crates/forge-api/src`.
- #22 — **✅ verified cryptographically**: hash parses with the project's argon2 0.5, verifies
  `admin123`, rejects a wrong password (standalone check with the exact crate). ⚠️ Deploy gap —
  see "New findings" below.
- #23 — **✅ verified**: `create_session_dir` only creates the dir; clone/copy happens once in
  `create_container`.
- #24 — **✅ verified**: `COALESCE(s.override_provider, p.provider), COALESCE(s.override_model,
  p.model)` in the durable-resume query.
- #25 — **✅ verified**: `migrations/001_initial_schema.sql` deleted (git status `D`).
- #26 — **✅ verified**: `SHELL_PATHNAMES` set with leading-slash pathnames; `node --check` OK on
  both `app.js` and `sw.js`.
- #27 — **✅ verified**: synchronous `std::sync::Mutex` increments; no `tokio::spawn`; all three
  metrics tests pass.
- #28 — **✅ verified**: `terminated` flag distinguishes clean end from loop-cap exhaustion; cap →
  `TurnEndReason::PiError` "wedged".

LOW notes — all **✅ verified**: `middleware.rs` deleted (and `pub mod middleware` removed);
SessionManager `pull_git_changes`/`get_git_status`/`get_ahead_behind`/`clone_repository`/
`copy_directory`/`GitStatus`/`SessionError::Git` gone (compile-clean); `voice.rs` doc corrected;
`main.rs` default filter `info`; sse.rs failure rows keep partial stdout/stderr in `content`;
nix-wrap warning behavior (#10); sandbox-manager-only clone passes `--branch`.

## New findings (need small additional work)

1. **#6 incomplete — `embedding.rs:221` byte-slice panic (fix it).** `classify_answer`'s warn!
   branch still does `&answer[..answer.len().min(100)]`. This is the same panic class as #6
   (multi-byte UTF-8 at the 100-byte boundary → panic → aborts the turn task inside `rerank`),
   one function away from the fixed `document[..500]` site, and was missed by the pass. Fix:
   `answer.chars().take(100).collect::<String>()`.

2. **#22 fix never reaches already-deployed databases.** The migration uses
   `ON CONFLICT (email) DO NOTHING` and sqlx tracks applied migrations by version, so any DB that
   already applied migration 002 keeps the broken placeholder hash forever — fresh installs only.
   The in-file comment documents a manual `UPDATE`, but a safer idempotent path is a follow-up
   migration (008): `UPDATE users SET password_hash = '<real hash>' WHERE email =
   'admin@forge.local' AND password_hash LIKE '%placeholder%';` which is a no-op on fresh DBs and
   self-heals existing ones.

3. **#10 known limitation (no code change, but document it):** with the fix, `nix_shell` is
   effectively inert for sandboxed sessions — the raw command runs inside the container without
   the nix environment, so tooling that only exists in the nix shell fails "command not found"
   (the code comment itself says "nix_shell support in the sandbox rootfs is TODO"). Not a
   regression (before, every call failed), but operators relying on `nix_shell` for tooling should
   treat it as host-path/resume-only.

4. **Test-coverage gaps worth closing:**
   - #5: `bash_record_content_truncates` only exercises ASCII; add a multibyte case
     (`'é'.repeat(5000)`) to lock in the char-boundary walk (and cover finding #1's class).
   - #22: no test logs in as the seeded `admin@forge.local` / `admin123` on a fresh migration —
     a future hash break would silently 500 again.
   - #13: the `Lagged` DB re-query recovery branch has no unit test.

5. **Housekeeping:** `extensions/forge-tools/dist/index.js` was rebuilt (`npm run build`, plain
   `tsc`, deterministic — same sha on repeat build) to guarantee it matches the fixed `src`;
   the 67-line diff vs HEAD is exactly the #15 fix. Nothing stale remains.

## FIX STATUS — Third pass (the "New findings" above; additive ledger, original text untouched)

All five new-finding items are resolved. Verified with `cargo check --all-targets` (clean) and
181 passing tests / 1 ignored (lib 104, integration 36, openai 19, web 14, e2e 6, pi-spawn 2):

1. **#6 (embedding.rs warn! branch) — ✅ fixed.** `rerank`'s neither-yes-nor-no warn! now uses
   `answer.chars().take(100).collect::<String>()` instead of `&answer[..answer.len().min(100)]`
   (same panic class as the `document` truncation fixed earlier).
2. **#22 (placeholder hash on already-deployed DBs) — ✅ fixed.** New migration
   `crates/forge-api/migrations/008_fix_admin_placeholder_hash.sql`: idempotent
   `UPDATE users SET password_hash = '<real hash>' WHERE email = 'admin@forge.local' AND
   password_hash LIKE '%placeholder%'` — no-op on fresh DBs (002 already ships the good hash),
   self-heals existing ones, and never clobbers an operator-changed hash (only `%placeholder%`
   rows match). Verified against a scratch DB: `UPDATE 1`, placeholder cleared, good-hash row
   untouched.
3. **#10 (nix_shell inert for sandboxed sessions) — ✅ documented** in `docs/API.md` next to the
   profile schema: the wrap only applies on the host/resume path; sandboxed sessions run the raw
   command inside the container's rootfs (read-only `/nix/store` breaks `-p pkg` installs);
   operators should put persistent tooling in `sandbox/default.nix`. Code comment ("TODO")
   already accurate.
4. **Test coverage — ✅ closed:**
   - #5 multibyte: `bash_record_content_truncates_multibyte` (`'é'.repeat(5000)`) and
     `bash_record_content_truncates_mixed_ascii_multibyte` (ASCII below the 8 KiB boundary, `é`
     above it) assert the truncation walk lands on a char boundary and the marker survives
     (2 new lib tests; lib now 104).
   - #22: `test_seeded_admin_login` (integration) — `admin@forge.local`/`admin123` on a fresh
     migration returns 200 with role `admin`; wrong password returns 401, never 500.
   - #13: `test_sse_lag_recovery_requeries_missed_rows` (integration) — stalls an SSE consumer,
     floods 300 tool calls (~600 bus events, ~45 KiB each) past the capped channel + broadcast
     buffer to force `Lagged`, then drains and asserts a `lagged` event fired AND every sequence
     1..=700 arrived with no gaps (rows dropped from the live bus are only recoverable via the
     DB re-query). ~32s runtime.
5. **Housekeeping — ✅ N/A** (dist rebuilt in pass two; `008_*` migration added, nothing stale).

## FIX STATUS — Fourth pass (independent validation of the whole ledger + two closing fixes; additive ledger, original text untouched)

Independent re-validation of every fix against the working tree, plus the
full suite (`cargo check --all-targets` 0 warnings; release build clean;
104 lib + 105 bin + 37 integration + 19 openai + 14 web + 6 e2e + 2
pi-spawn passing, 1 ignored). Everything above re-verified in source.

1. **#22 CRITICAL follow-up — the in-place 002 edit broke deployed-DB startup (fixed here).**
   sqlx 0.8.6 `Migrator::run` compares the checksum of every already-applied
   migration against the current file (no escape hatch). Editing
   `002_users_and_api_keys.sql` in place (placeholder → real hash) meant every
   DB that applied the ORIGINAL 002 failed startup with `migration 2 was
   previously applied but has been modified` — migration 008 never ran, so the
   placeholder hash was permanent. Reproduced against a replica sim **and the
   live `forge` DB** (v2 ledger checksum `7f7e671c…` = original 002, admin row
   still placeholder → startup refused). **Fix: reverted 002 to its HEAD
   content** (byte-identical, checksum matches every deployed DB) so migration
   008 is the sole carrier of the real hash: fresh DBs get placeholder + 008
   heals in the same startup; deployed DBs pass checksum validation and 008
   heals them; operator-changed hashes are never clobbered (008 only matches
   `%placeholder%`).
2. **#22 regression test (new, integration):** `test_deployed_db_with_old_002_migrates_and_heals_admin`
   replays a pre-fix deployed DB (applies 001 + the byte-exact original
   placeholder 002 via `sqlx::raw_sql`, records sha384 checksums in
   `_sqlx_migrations`, runs the current `sqlx::migrate!` startup path) and
   asserts a clean upgrade, the placeholder gone, and the healed hash verifies
   `admin123`. Proven as a real net: temporarily re-editing 002 makes it fail
   with `VersionMismatch(2)` — exactly the startup failure a deployed DB would
   hit. Added `create_database`/`drop_test_db` pub helpers to `test_helpers.rs`
   (integration_tests only; `#[allow(dead_code)]` for the other binaries).
2b. **PWA not installable (fixed here).** The manifest shipped SVG-only icons
   (`sizes: "any"`) — Chromium's installability check requires raster `any`
   icons at 192x192 and 512x512 plus a maskable one, so no install prompt
   ever fired. Also `app.js` registered the service worker with a relative
   `"sw.js"`, so opening a deep link (`/chat/<id>`) registered it at
   `/chat/sw.js` (SPA fallback serves index.html there → wrong scope /
   MIME failure). Fixes: generated PNG icons (192/512/180 + 512 maskable)
   from the SVGs, manifest now lists them with explicit sizes + `maskable`;
   `app.js` registers `"/sw.js"` (absolute); `index.html` gains PNG
   apple-touch-icons; `sw.js` shell list + `SHELL_PATHNAMES` include the
   new PNGs (cache bumped to `forge-shell-v2`); `api/web.rs` embeds the
   PNGs via `include_bytes!` (binary — `include_str!` would fail) with
   `image/png` MIME + unit-test guards for the PNG magic bytes.
   Deployed + verified: all five assets served with correct MIME from the
   embedded handler. Remaining constraint (app-side can't fix): Chrome
   only prompts on a secure context — localhost or HTTPS; plain
   `http://<lan-ip>:8080` needs an HTTPS reverse proxy.

3. **#12 follow-up — SIGTERM graceful shutdown now actually wired (fixed here).**
   `with_graceful_shutdown` previously only awaited `tokio::signal::ctrl_c()`
   (SIGINT); the code comment claimed SIGTERM coverage that wasn't there — a
   systemd stop fell through to the default disposition and killed the process
   without draining (in-flight tool calls lose their SSE connection, pi
   subprocess orphaned). `main.rs` now has a `shutdown_signal()` helper
   (axum-docs shape: futures built before `tokio::select!`) completing on
   SIGINT or SIGTERM. New unit test `shutdown_signal_completes_on_sigterm`
   (bin target) polls the future once to register the handler, sends SIGTERM,
   and asserts the future resolves (deterministic — no sleep race).

Test tally after this pass: lib 104 (was 102), integration 36 (was 34) — +2 each; all other
suites unchanged and green.
