# Forge — Consolidated Improvement Plan

Working document (delete before publication). Consolidates the five sub-agent
deep-dive reports (`/tmp/forge-analysis/{core-api,tools-audit,pi-sessions,db-auth,pub-polish}.md`)
plus a first-hand review. Items are tagged with their source report.

## Status

- [x] P0-10 clippy warnings fixed (commits 89961e4, 394a21b) + rustfmt normalization
- [ ] Wave 1 (in progress, 5 parallel agents in worktrees /tmp/forge-w1-*):
  - tools: P0-1, P0-2, P1-25, P2-35 → branch wave1/tools
  - events: P0-6, P2-34, P2-36 → branch wave1/events
  - registry: P0-7, P0-8, P0-9, P0-11, P0-12, P1-21, P0-13(ext path) → branch wave1/registry
  - auth: P0-3, P0-4, P0-5, P0-12(auth), P1-28, P2-39 → branch wave1/auth
  - oss: P0-13, P0-14, P2-43, P2-44, P2-45, P2-46 → branch wave1/oss
- Gate: merge branches to main sequentially, run full clippy/test/fmt gate.

## P0 — Security (publication blockers)

### P0-1: Contain `read`/`write`/`edit` paths to `working_dir`
- **Files:** `tool_executor.rs` (execute_read ~744, execute_write ~788, execute_edit ~834), `resume.rs` (replay path).
- `Path::join` with an absolute `path` replaces the base; `..` chains are not
  normalized. Agent can read/write arbitrary host files
  (`/etc/forge/forge.env` holds `FORGE_API_KEY`). Add a
  `resolve_in_base(base, user) -> Result<PathBuf>` helper: reject absolute
  paths, reject `..` components, canonicalize + `starts_with(canonical(base))`.
  Reject (tool error, not silent) on escape. Unit tests with `../..`, absolute,
  and symlink-adjacent paths (tools-audit B1, G1; pi-sessions P1.3).

### P0-2: Stop streaming bash silently falling back to host execution
- **File:** `api/sse.rs` ~330: `get_container(...).ok().map(|c| c.root_dir)` swallows
  errors → command runs on the host when the sandbox is unavailable. The
  non-streaming path (`execute_bash_sandboxed`) fails with `ToolError` instead.
  Match on the result: error → SSE `error` event + 500-ish terminal, never host exec.

### P0-3: Remove the default `admin@forge.local` / `admin123` account
- **Files:** `migrations/` (new 009 or rewrite 008+002 pair).
- 002 inserts a placeholder admin; 008 "repairs" it to a real argon2id hash of
  `admin123` — every database ends up with a known-credential admin (db-auth H1).
  Ship a bootstrap flow instead: `FORGE_BOOTSTRAP_ADMIN_EMAIL/PASSWORD` env,
  applied once at startup (or `forge admin create` CLI), no default in migrations.

### P0-4: Rate-limit + bound the unauthenticated auth endpoints
- **File:** `api/auth.rs` register/login (224-320).
- In-process token bucket per (IP) with small capacity; cap password length
  (128 chars); reject `Json` bodies over 64 KiB on auth routes only (db-auth H3).

### P0-5: Harden API-key storage
- **File:** `api/auth.rs` `hash_api_key` (208), migration 009.
- SHA-256 of key is one-way but unsalted → offline-crackable from a DB dump;
  and `strip_prefix("sk_forge_")` means the same secret hashes identically with
  and without the prefix (aliasing). Switch to `HMAC-SHA256(key, server_secret)`
  with dual-read (accept SHA-256 rows until re-keyed), drop the dead SQL
  `hash_api_key()` function (db-auth M5).

## P0 — Correctness

### P0-6: SSE catch-up → subscribe window loses rows (silent data loss)
- **File:** `api/events.rs` 124-251.
- Catch-up query runs before `bus.subscribe()`; rows committed in that window are
  in neither the snapshot nor the new broadcast receiver, and no `Lagged` fires.
  Fix: subscribe first, run catch-up, fuse into one in-order stream deduped by
  sequence (the existing guard at ~251 supports it). Also fix B5 (two producers
  racing one mpsc — collapse to one) and B6 (lag payload `last_seq - since`
  math). Add tests (core-api B1/B2; tools-audit G5).

### P0-7: Wedged session after pi dies (registry hot path)
- **File:** `agent_registry.rs` `get_or_create` 324-341.
- Hot path returns a cached `PiAgent` without a liveness check; a dead pi (killed
  by the `Timeout` arm) makes every subsequent `POST /messages` fail with
  `PiError` until 30 min of *inactivity* elapses. Add `PiAgent::is_alive()`
  (`child.try_wait()`), fall through to the slow path when dead
  (core-api B3; pi-sessions test gap).

### P0-8: Idle cleanup can reap a session mid-turn
- **Files:** `main.rs` `cleanup_task`, `agent_registry.rs`, `api/turn.rs`.
- `last_active` only updates at dispatch and turn end; a turn longer than 30 min
  (legal: bash timeout up to 1 h) gets pi killed + sandbox destroyed. Skip
  sessions with an in-flight turn (registry can expose `has_active_turn`); bump
  `last_active` at dispatch (core-api B4).

### P0-9: `AgentRegistry::remove()` holds the global write lock while killing pi
- **File:** `agent_registry.rs` 695-707.
- `remove()` takes the `agents` write lock, then awaits the per-agent mutex held
  by a running turn (up to 1 h). One cleanup kills API-wide dispatch. Clone the
  `SharedPiAgent` under a short write lock, drop it, then kill (pi-sessions P1).

### P0-10: CI is red — 6 clippy warnings in `api/router.rs`
- `result_large_err` at 503 (`create_new_session` → `Result<Uuid, Response>`):
  define a small `SessionCreateError` enum. 5× `useless_borrows_in_formatting`
  (985, 1015, 1021, 1031, 1041). CI denies warnings; fix and verify
  `cargo clippy --all-targets -- -D warnings` is clean.

### P0-11: `spawn_locks` map grows without bound on failed `get_or_create`
- **File:** `agent_registry.rs` 660-685.
- `locks.remove(&session_id)` only runs on success; every bogus session id
  (attacker or retry loop) leaks an entry. Use a scope guard / cleanup on all
  exit paths (pi-sessions P2).

### P0-12: Raw DB errors leak to clients
- **Files:** `api/mod.rs` `dispatch_message` ~1257, `create_session` ~855,
  `api/auth.rs` `AuthError::Database` → 500 body.
- Use the existing `db_err` pattern (generic client message, log detail server-side).

### P0-13: OSS scrub — internal refs, personal paths, personal tooling
- **Files:** many, mostly docs/config.
  - `10.10.199.51` (Parakeet/Kokoro) → placeholder in AGENTS.md, docs/API.md,
    CHANGELOG.md; code default in `api/voice.rs` → disabled unless env set.
  - `10.10.199.29` + `jbutler` in `scripts/dd-cli-rotate.sh` — remove the whole
    DoorDash stack: `scripts/dd-cli-rotate.sh`,
    `systemd/dd-cli-rotate.{service,timer}`, `scripts/git-credential-forge`,
    `DD_CLI_ACCESS_TOKEN` block in `systemd/forge.env.example`, `dd_cli_access_token`
    in `sandbox.rs` `ContainerEnv`, `dd-cli` from `sandbox/default.nix`.
  - `/data/jbutler/git/jbutlerdev/...` → `/opt/forge/...` or `$(...)` in
    systemd units, docs; and `agent_registry.rs:265` — fail loudly (or resolve
    relative to the binary) instead of a personal path default.
  - `bitfrost.botnet` + `bifrost` key default in `embedding.rs` → disabled unless
    `FORGE_EMBEDDING_URL`/`FORGE_RERANKER_URL` set.
  - `search.butler.ooo` in `skills/search-cli/SKILL.md` + `docs/SEARCH-TOOL.md`
    → placeholder.
- (pub-polish 1a, 2a; db-auth embedding finding)

### P0-14: README/docs factual errors
- `README.md` layout lists `api/middleware.rs` and `web.rs` (neither exists) and
  a phantom root `migrations/` dir. `AGENTS.md` §7 says the harness "no longer
  writes any rows" — false (the turn driver writes assistant rows,
  mod.rs:222). Duplicate `NIX_CONFIG` paragraph (AGENTS.md:562 vs 576).
  (pub-polish 1c, 1d; core-api smell #10.)

## P1 — Multi-tenancy

### P1-15: Enforce per-user ownership of profiles/sessions
- **Files:** `api/mod.rs` handlers, `api/auth.rs`, maybe a `db` helper.
- `user_id` columns exist (002) but are never set on create and never filtered:
  any valid key sees everyone's profiles, sessions, messages, and can run tools
  in any session. Thread `auth.user_id` through create (stamp) and
  list/get/delete (owner-or-admin), add tests for owner/foreign/admin
  (db-auth H2).

## P1 — Structure / SRP

### P1-16: Split `api/mod.rs` (2091 lines)
- Handlers into `api/profiles.rs`, `api/sessions.rs`, `api/messages.rs`,
  `api/admin.rs`; keep router assembly + `AppState` in `mod.rs`. `update_session`
  (~210 lines) split into focused helpers (core-api smell #1, P1-5).

### P1-17: `main.rs` is a duplicate crate root
- Binary re-declares the module tree + `#![allow(dead_code)]` (main.rs:1).
  Make main a thin `fn main` over the lib (core-api smell #3).

### P1-18: Rename `api/router.rs` → `api/routing.rs`
- It is the LLM *message* router; `create_router()` is the HTTP router —
  a name collision that will confuse contributors (core-api smell #2).

### P1-19: `drive_turn` — injectable event source
- 350-line nested match; extract per-event handlers behind a `PiEventSource`
  trait so the loop is unit-testable (core-api P1-8, test gap).

### P1-20: Dead code sweep
- `observability.rs`: `create_observability_router` + handler fns are unused
  (mod.rs defines its own). `logging.rs` `request_log_middleware` is defined +
  re-exported but never layered into `build_app` — wire it in (it's the request
  log!) or delete. `session_manager.rs`: most of it is
  `#[allow(dead_code)]`/"reserved"; collapse or delete. `sandbox.rs`:
  `start_container`/`stop_container`/`execute_in_container`/`SandboxState` are
  dead alternate-design code. `pi_agent.rs`: `wait_for_session` (broken 30s cap,
  no callers — fix or delete), `PiInput::Abort` (never sent), `config` field
  (dead, holds api_key in memory), `AgentEntry.last_active` (never updated),
  `db/mod.rs` dead structs. (core-api §3; pi-sessions §3; first-hand review.)

### P1-21: `kill_on_drop(true)` on nspawn + pi commands
- Timeout currently "drops" the child but can't kill it; only the in-container
  `timeout` binary backstops. Also document (or mitigate) that operator secrets
  ride nspawn argv and are visible in `/proc/<pid>/cmdline`
  (pi-sessions P3).

### P1-22: Consolidate repeated `last_active` UPDATE
- Verbatim `UPDATE sessions SET last_active = NOW() ...` in 4+ places → one
  `touch_session` helper (core-api smell #6).

### P1-23: API surface inconsistency
- Two styles: `/profiles/get?id=` (query) AND `/profiles/:id` (path), same for
  sessions/delete. Pick one as canonical (keep both working for CLI compat, or
  update the CLI). Document in docs/API.md.

## P1 — API correctness

### P1-24: Idempotent tool-record writes
- Retry of `POST /tools/execute` writes duplicate call rows → the exact
  Anthropic-400 chain session_replay documents. Check existing
  `(session_id, tool_call_id, role)` inside the advisory-locked tx (no-op or
  upsert); add partial unique index (tools-audit B3, G2).

### P1-25: `read` tool cap
- Advertise a 2000-line/50 KB cap; `execute_read` materializes the whole file
  then slices. Enforce the cap at read time (tools-audit S5).

### P1-26: voice.rs
- `reqwest::Client` built per request (docstring says "shared") → `OnceLock`.
- Multipart parts buffered unbounded → cap (e.g. 25 MB → 413). Forward only
  known fields upstream (db-auth M8).

### P1-27: web.rs SPA fallback
- `GET /typos` → 200 index.html; gate the fallback on
  `Accept: text/html`-like checks (404 otherwise); add `Cache-Control:
  no-store` for index.html, `immutable` for hashed assets, basic CSP
  (db-auth M7).

### P1-28: `users.role` CHECK constraint
- `PATCH /users/:id` can set arbitrary text; add
  `CHECK (role IN ('user','admin'))` (db-auth M6).

## P2 — Hygiene

### P2-29: AGENT_GUARD
- 115-line prompt const in `agent_registry.rs`: move to a `data/` asset or
  profile field; strip the `sudo systemctl/journalctl` diagnostic advice that
  contradicts AGENTS.md §2 (it reproduces the hang it diagnoses)
  (pi-sessions P2).

### P2-30: bus.rs per-row INFO log → DEBUG; add lag/publish counters to metrics.

### P2-31: `logging.rs` one-line audit wrappers (15 fns) → direct builder calls
  (or keep if they read better; low priority).

### P2-32: Non-streaming bash: preserve partial output on timeout (B7).
### P2-33: Orphan call-row janitor: synthesize `[abandoned]` result rows for
  stale open calls (B4-tools).
### P2-34: Symmetric divergence detection in `resume.rs` (B8).
### P2-35: `400` on malformed streaming tool input, not 500 (B9); sse.rs:488
  dead tuple element, sse.rs:360 no-op fork (S6).

## P2 — Tests

### P2-36: `events.rs` has zero tests (ordering B5, lag B6, oneshot close).
### P2-37: `turn.rs` has no unit tests — gated on P1-19 (event source trait).
### P2-38: `agent_registry`: respawn-after-dead-pi (P0-7), spawn-lock leak
  (P0-11), `remove()` lock behavior (P0-9).
### P2-39: `auth.rs` has zero tests: header extraction edge cases, key-hash
  aliasing (P0-5), role/ownership checks (P1-15).
### P2-40: `voice.rs`/`embedding.rs` pure fns (url_from_env, HOP_BY_HOP,
  parse, cosine already done).
### P2-41: `DbToolRecorder` Postgres round-trip (sequence uniqueness under
  concurrency) — CI has a Postgres service; use it.
### P2-42: OpenAI stateful mode e2e (deferred REVIEW #19); extension SSE-parser
  test; `--session <jsonl>` spawn test (pi-sessions §5).

## P2 — OSS repo hygiene

### P2-43: Move `REVIEW.md` + `BUGS.md` to `docs/` (or delete; open items become
  issues). (pub-polish 1e)
### P2-44: Add `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `.github/ISSUE_TEMPLATE/`, `.github/PULL_REQUEST_TEMPLATE`, root
  `.env.example`, license badge in README.
### P2-45: CLI hardening: `set -euo pipefail`; prompt for password instead of
  argv; `curl --max-time`; `jq` guards; `--version` (pub-polish 2c).
### P2-46: Extension: pick one module system (drop `module.exports` line);
  comment on `process.stdout.write` isolation from the RPC channel
  (pub-polish 2d).
### P2-47: Dedup: AGENTS.md §5/§12 → pointers into docs/; endpoint table in one
  place. Makefile/justfile for build/test/lint. CI: web lint, ts lint, doc
  link-check, shellcheck cache (pub-polish 1d, 2e).

## Implementation waves (parallel sub-agents, disjoint file sets)

- **Wave 1** (parallel): P0-1/2/25 (tools), P0-10 (clippy), P0-6 (events),
  P0-7/8/9/11 (registry+main), P0-3/4/5/12-auth/28 (auth+migrations),
  P0-13/14 (OSS scrub, docs+config+scripts).
- **Wave 2** (after wave 1 builds green): P1-15 (multi-tenancy), P1-16 (split
  mod.rs), P1-17/18/20/22 (structural), P1-21 (kill_on_drop), P1-23 (API
  surface), P1-24/26/27 (correctness).
- **Wave 3**: P2 items + tests (P2-36…42) + remaining docs (P2-43…47).

Gate between waves: `cargo fmt --check`, `cargo clippy --all-targets -- -D
warnings`, `cargo test --workspace` (with Postgres).
