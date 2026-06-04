# Forge

[![CI](https://github.com/jbutlerdev/forge/actions/workflows/ci.yml/badge.svg)](https://github.com/jbutlerdev/forge/actions/workflows/ci.yml)
[![Release](https://github.com/jbutlerdev/forge/actions/workflows/release.yml/badge.svg)](https://github.com/jbutlerdev/forge/releases)

A platform for running durable AI coding agents backed by [pi](https://github.com/badlogic/pi-mono). The Rust API server owns a long-lived `pi` subprocess per session, persists every user / assistant / tool-call / tool-result row to PostgreSQL, exposes a REST API for clients, and ships a bash CLI as a reference client.

```
┌───────────────────────┐      POST /messages      ┌────────────────────────┐
│  Client (CLI, curl,   │ ───────────────────────▶ │  forge-api (Rust)      │
│  your own app)        │                         │  • spawns pi per       │
│                       │ ◀──── GET /sessions ──── │    session             │
│                       │      /{id}/events       │  • streams pi events   │
│                       │       (SSE, live push)  │  • runs tool executor  │
│                       │ ◀──── POST /tools/... ── │  • records audit log   │
└───────────────────────┘                         └──────────┬─────────────┘
                                                              │ stdin/stdout (--mode rpc)
                                                              ▼
                                                ┌────────────────────────┐
                                                │  pi (Node.js)          │
                                                │  + forge-tools ext     │
                                                │  • LLM loop            │
                                                │  • calls back to API   │
                                                │    for tool execution  │
                                                └────────────────────────┘
```

Forge is built around a simple durability claim: **the `messages` table is the source of truth for every conversation**. The `pi` subprocess is disposable. When a session is reactivated after the prior `pi` has been killed, the API rebuilds the working tree by replaying prior tool calls (`resume.rs`) and rebuilds the model's context by handing the fresh `pi` a session jsonl via `--session <path>` (`session_replay.rs` + `agent_registry.rs`). The user sees a conversation that picks up exactly where it left off.

## Features

- **Session persistence** — every user prompt, assistant reply, tool call, and tool result is written to PostgreSQL with a monotonic per-session `sequence`. Replay any session from the message table. `get_next_sequence()` is serialized with a transaction-scoped advisory lock so the harness and the executor can write concurrently without violating the `UNIQUE (session_id, sequence)` constraint.
- **Durable resume** — when a session's `pi` is killed (idle timeout, API restart, etc.) the next `POST /messages` rebuilds the working tree from the audit log and spawns a fresh `pi` with the prior conversation loaded as structured messages. No re-derivation, no "I don't have access to session history".
- **Long-lived `pi` processes** — one `pi` subprocess per session, kept warm for the life of the session so LLM context is preserved across turns.
- **Structured tool audit log** — call rows (`role='assistant'`, with `tool_input` jsonb) and result rows (`role='tool'`, with `tool_output` jsonb and `duration_ms`) are linked by `tool_call_id`. The **executor is the sole writer of tool rows** — the harness no longer races to write call rows, eliminating a class of dropped-row bugs from the previous harness-written design.
- **Streaming tool execution** — `POST /tools/execute/stream` returns Server-Sent Events with `stdout` / `stderr` / `tool_end` chunks as the command runs. `POST /tools/execute` returns a one-shot `ToolOutput` for non-streaming tools (`read`, `write`, `edit`, non-streaming `bash`).
- **Live agent events** — `GET /sessions/{id}/events?since=<seq>` is an SSE endpoint that streams new `messages` rows and `agent_end` signals as the agent works. Multiple consumers can subscribe independently via the in-process `MessageBus`; slow consumers are lagged but never miss a row (they re-query the DB for catch-up). Polling `GET /messages?session_id=…` is also supported and equivalent.
- **Multi-user, multi-tenant** — `users` + `api_keys` tables, argon2-hashed passwords, SHA-256-hashed API keys, per-key `last_used_at` and `expires_at`. Every `profiles` and `sessions` row is owned by a `user_id`.
- **Per-session isolation** — each session gets `/forge/sessions/<session_id>/` as its working directory. The optional nspawn sandbox runs `bash` inside a per-session Debian rootfs; see §Sandbox below.
- **Atomic self-update** — `POST /admin/self-update` accepts a new binary, stages it, and triggers a zero-downtime `systemctl restart` (the LLM uses this after `cargo build --release` to deploy its own changes).
- **Reference CLI** — `cli/forge` is a small bash client that exercises the API: auth, profiles, sessions, and a `message ask` command that streams the response by polling.
- **Observability** — structured logging via `tracing`, JSON metrics at `/metrics`, Prometheus exposition at `/metrics/prometheus`, plus integration and e2e test suites under `crates/forge-api/tests/`.

## Sandbox

The sandbox subsystem is wired in but the operator decides whether to enable it. `SandboxManager::init()` only creates the base directories (`/forge/sandbox/`, `/forge/sessions/`); it is intentionally allowed to fail at startup so the API still comes up on hosts without nspawn or Nix. Tool execution falls back to host-side Rust file ops and direct `tokio::process::Command` invocations when no per-session container is registered.

When the operator *has* bootstrapped the base rootfs (see `docs/ARCHITECTURE.md` §7), every `bash` tool call is wrapped in a per-call `systemd-nspawn` invocation that creates a namespace on the fly, runs the command in the session's Debian rootfs, and tears the namespace down on exit. Per-call overhead is ~50ms; the LLM can mutate the per-session rootfs freely (apt, pip, etc.) without affecting other sessions or the host. `/nix/store` is bind-mounted read-only so the LLM can use the operator-installed nixpkgs set but cannot mutate the host's Nix cache. The default user package set lives in `sandbox/default.nix` and is materialized by `sandbox/build.sh`.

The default package set includes a **pinned Rust toolchain** (rustc, cargo, rustfmt, clippy, rust-analyzer) so the LLM can `cargo build` / `cargo test` / `cargo run` inside a cloned repo on its first turn without first having to install a compiler. The toolchain comes from [`oxalica/rust-overlay`](https://github.com/oxalica/rust-overlay), is wired up in `flake.nix`, and is the same version the dev shell ships — so a fix that works in `nix develop` also works inside the sandbox. To rebuild the sandbox package set:

```bash
# Via the flake (includes the Rust toolchain)
nix build .#sandbox-deps
sudo -E ./sandbox/build.sh
```

`./sandbox/build.sh` will detect the flake and use it; on hosts without a working flake command it falls back to `nix-build sandbox/default.nix`, which builds the non-Rust portion of the set.

`POST /admin/sandbox-reset?session_id=<uuid>` wipes a session's per-session rootfs and removes the in-memory container entry, forcing the next bash call to re-`cp -a` from the base. This is the operator workflow for refreshing an existing long-running session against an updated base.

## Repository layout

```
forge/
├── Cargo.toml                          # Workspace root
├── rust-toolchain.toml                 # Stable + rustfmt/clippy/rust-analyzer
├── flake.nix / flake.lock              # Nix dev shell + .#sandbox-deps (Rust toolchain for the sandbox)
├── README.md                           # This file
├── AGENTS.md                           # Working guide for AI agents and humans
├── CHANGELOG.md                        # Release notes
│
├── crates/forge-api/                   # The Rust API server (axum + sqlx + tokio)
│   ├── Cargo.toml
│   ├── migrations/                     # Embedded by sqlx::migrate! at startup
│   │   ├── 001_initial_schema.sql      # profiles, sessions, messages, pgcrypto
│   │   ├── 002_users_and_api_keys.sql  # users, api_keys, user_id on profiles/sessions
│   │   ├── 003_tool_output.sql         # tool_output jsonb + duration_ms on messages
│   │   └── 004_get_next_sequence_locking.sql  # pg_advisory_xact_lock wrapper
│   ├── src/
│   │   ├── main.rs                     # Entry point: builds AppState, runs migrations, starts axum
│   │   ├── lib.rs                      # Public crate surface (modules, error type)
│   │   ├── api/
│   │   │   ├── mod.rs                  # HTTP handlers + the harness event loop
│   │   │   ├── auth.rs                 # register, login, API keys, user CRUD
│   │   │   ├── middleware.rs           # Auth middleware
│   │   │   ├── sse.rs                  # /tools/execute/stream + streaming bash
│   │   │   ├── events.rs               # /sessions/{id}/events SSE handler
│   │   │   └── events_integration.rs   # Tests for the events endpoint
│   │   ├── db/mod.rs                   # SQLx row types (User, ApiKey, Profile, Session, Message, …)
│   │   ├── pi_agent.rs                 # pi subprocess management (--mode rpc)
│   │   ├── agent_registry.rs           # Per-session PiAgent map + AGENT_GUARD system-prompt prefix
│   │   ├── tool_executor.rs            # bash / read / write / edit
│   │   ├── recording.rs                # ToolRecorder trait + DbToolRecorder
│   │   ├── session_manager.rs          # /forge/sessions lifecycle, 30-min idle cleanup
│   │   ├── session_replay.rs           # Build a pi session jsonl from the messages table
│   │   ├── resume.rs                   # Re-execute prior tool calls on resume (filesystem restore)
│   │   ├── sandbox.rs                  # systemd-nspawn wrapper
│   │   ├── observability.rs            # Metrics
│   │   ├── logging.rs                  # tracing_subscriber setup, audit log
│   │   └── bus.rs                      # In-process pub/sub for new message rows
│   └── tests/
│       ├── integration_tests.rs        # HTTP API tests (require a running DB)
│       ├── e2e_tests.rs                # End-to-end agent run
│       └── test_helpers.rs             # TestApp builder
│
├── extensions/forge-tools/             # pi TypeScript extension (registers tools)
│   ├── src/index.ts                    # Source
│   ├── dist/index.js                   # Built artifact loaded by the agent at runtime
│   └── package.json / tsconfig.json
│
├── cli/                                # Reference bash client
│   ├── forge                           # Top-level dispatcher
│   └── forge.d/                        # common.sh, profile.sh, session.sh, message.sh
│
├── sandbox/
│   ├── default.nix                     # Default user package set (nixpkgs buildEnv)
│   └── build.sh                        # nix-build + symlink into /forge/sandbox/base/
│
├── systemd/
│   ├── forge-api.service               # Example unit file
│   └── forge.env.example               # Example env file
│
├── scripts/
│   ├── setup.sh                        # Dev-box quick start
│   ├── install.sh                      # Production-ish install
│   ├── uninstall.sh                    # Uninstall (--purge also wipes data)
│   └── test-api.sh                     # Smoke-test the running API
│
├── migrations/                         # Stale symlink directory (see note below)
└── docs/                               # ARCHITECTURE.md, API.md, CLI.md, OPERATIONS.md, …
```

**Where the migrations actually live:** `sqlx::migrate!("./migrations")` resolves the path relative to `CARGO_MANIFEST_DIR`, which is `crates/forge-api/`. The migrations the binary reads are in `crates/forge-api/migrations/`. See `docs/OPERATIONS.md` for the migration workflow.

## Quick start

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Rust | 1.75+ (`rust-toolchain.toml` pins stable) | `rustup install stable` |
| PostgreSQL | 15+ | The role needs `CREATE` on the `forge` database and `CREATE EXTENSION` for `pgcrypto` |
| Node.js | 20+ | Only needed to build the `forge-tools` extension; the binary doesn't run Node itself |
| pi | latest | `npm install -g @earendil-works/pi-coding-agent` (the binary uses this at runtime) |

The path to the built extension is read from `FORGE_TOOLS_EXTENSION` if set, otherwise the harness falls back to a hard-coded search under `/root/.nvm/versions/node/.../lib/node_modules/@earendil-works/pi-coding-agent/`. If both fail, tool calls will return an error pointing at the missing extension.

### One-shot dev setup

```bash
git clone https://github.com/jbutlerdev/forge
cd forge
bash scripts/setup.sh
sudo systemctl status forge-api
```

`scripts/setup.sh` checks for Rust, Node, the `pi` CLI, and PostgreSQL; builds `forge-api`; creates the `forge` database; sets up `/forge/sessions`; and builds the `forge-tools` extension.

### Manual install

```bash
# 1. Database
sudo -u postgres psql <<'SQL'
CREATE DATABASE forge;
\c forge
CREATE EXTENSION IF NOT EXISTS pgcrypto;
SQL

# 2. Build the extension
( cd extensions/forge-tools && npm install && npm run build )

# 3. Build and install the binary
cargo build --release -p forge-api
sudo install -m 0755 target/release/forge-api /opt/forge/forge-api

# 4. Install the systemd unit
sudo mkdir -p /etc/forge
sudo cp systemd/forge-api.service /etc/systemd/system/forge-api.service
sudo cp systemd/forge.env.example /etc/forge/forge.env
sudo chmod 600 /etc/forge/forge.env
sudo systemctl daemon-reload
sudo systemctl enable --now forge-api
sudo systemctl status forge-api
sudo journalctl -u forge-api -f
```

Migrations are embedded in the binary via `sqlx::migrate!("./migrations")` and run automatically on startup. Adding a new migration means dropping a `NNN_description.sql` file into `crates/forge-api/migrations/` and rebuilding — see `docs/OPERATIONS.md` for the full workflow and the per-session advisory lock rationale.

### Nix dev shell

```bash
nix develop
createdb forge
sqlx migrate run
cargo run -p forge-api
```

The shell includes `rustc`, `cargo`, `rustfmt`, `clippy`, `rust-analyzer`, `postgresql_16`, `sqlx-cli`, `watchexec`, `curl`, and `jq`.

### Configure

`/etc/forge/forge.env`:

```ini
# --- Required ---
DATABASE_URL=postgres://postgres@localhost/forge

# --- Optional but recommended ---
FORGE_API_URL=http://localhost:8080
FORGE_TOOLS_EXTENSION=/opt/forge/extensions/forge-tools/dist/index.js
RUST_LOG=forge_api=debug,info
# PATH must include the directory containing the `pi` binary
PATH=/root/.nvm/versions/node/v20.18.1/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# --- Provider keys (only required by profiles that use the matching provider) ---
# With provider=proxy-anthropic the API key is stored on the profile itself;
# these globals are only consulted when provider=anthropic or provider=openai.
# ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...
```

> The example unit file at `systemd/forge-api.service` uses `User=forge` / `Group=forge` and includes systemd hardening (`NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`, `ProtectHome=true`). On a single-tenant dev box without a `forge` user, switch to `User=root` / `Group=root` and remove the hardening directives. `docs/OPERATIONS.md` has both the hardened and slimmed-down unit files.

### Run

```bash
sudo systemctl status forge-api          # is it running?
sudo systemctl restart forge-api         # after a code change
sudo journalctl -u forge-api -f          # follow logs
sudo journalctl -u forge-api -n 200      # last 200 lines
```

**Do not start the server yourself.** `forge-api` runs as a systemd service. Starting a second copy manually will fail to bind port 8080, and even on a different port your test client would hit a different process than the one writing the audit log.

## CLI quick start

```bash
export FORGE_API_URL=http://localhost:8080

# Register a user (prints an API key — export it)
forge register you@example.com "Your Name" password123
export FORGE_API_KEY=sk_forge_...

# Or, if you already have a key:
export FORGE_API_KEY=sk_forge_...

# Create a profile
forge profile create my-agent \
  --provider proxy-anthropic \
  --model claude-sonnet-4-20250514 \
  --working-dir /tmp/my-project

# Create a session
SESSION=$(forge session create <profile-id> --title "Demo" | jq -r .session.id)

# Send a question and stream the response (polls /messages, prints new rows)
forge message ask "$SESSION" "What is the capital of France?"

# Watch a session that's already running
forge message watch "$SESSION"

# List the full audit log for a session
forge messages "$SESSION"
```

The CLI is an example — see [`docs/CLI.md`](docs/CLI.md) for the full command reference. For richer clients, hit the REST API directly.

## API summary

All endpoints accept / return JSON. Auth is `X-API-Key: <key>` on every endpoint except `/health`, `/auth/register`, and `/auth/login`. Errors look like `{"error": "<message>"}` with the appropriate 4xx/5xx status.

| Method | Endpoint | Purpose |
|---|---|---|
| `GET`   | `/health` | Liveness probe |
| `GET`   | `/metrics` | JSON metrics (requests, errors, active sessions, per-tool execution counts) |
| `GET`   | `/metrics/prometheus` | Prometheus exposition format |
| `POST`  | `/auth/register` | Create a user |
| `POST`  | `/auth/login` | Exchange email/password for an API key |
| `POST`  | `/auth/logout` | Invalidate the calling API key |
| `GET` / `POST` / `DELETE` | `/api-keys[/:id]` | List / create / delete API keys for the calling user |
| `GET` / `PATCH` / `DELETE` | `/users[/:id]` | User CRUD (admin role required for cross-user access) |
| `POST`  | `/profiles` | Create a profile (LLM provider + model + tools) |
| `GET`   | `/profiles` | List profiles |
| `GET`   | `/profiles/get?id=<uuid>` / `/profiles/:id` | Get a profile |
| `PATCH` | `/profiles/update?id=<uuid>` | Update a profile |
| `DELETE`| `/profiles/delete?id=<uuid>` / `/profiles/:id` | Delete a profile |
| `POST`  | `/sessions` | Create a session |
| `GET`   | `/sessions` | List sessions |
| `GET`   | `/sessions/get?id=<uuid>` / `/sessions/:id` | Get a session |
| `DELETE`| `/sessions/delete?id=<uuid>` / `/sessions/:id` | Delete a session |
| `GET`   | `/sessions/:id/events?since=<seq>` | SSE stream of new message rows + turn-ended signals |
| `POST`  | `/messages` | Send a message; the API spawns / reuses `pi` in the background, returns 202 |
| `GET`   | `/messages?session_id=<uuid>` | List messages (poll for new rows) |
| `POST`  | `/tools/execute` | One-shot tool call (`read`, `write`, `edit`, non-streaming `bash`) |
| `POST`  | `/tools/execute/stream` | Streaming tool call (SSE for stdout / stderr) |
| `GET`   | `/sandbox/containers` | List active per-session containers |
| `POST` / `DELETE` | `/sandbox/sessions/:id` | Create / destroy a per-session container |
| `POST`  | `/admin/sandbox-reset?session_id=<uuid>` | Wipe a session's per-session rootfs so the next bash call re-`cp -a`s from base |
| `POST`  | `/admin/self-update` | Atomic self-update: stages a new binary and triggers a graceful restart (raw ELF body) |

For the per-endpoint request / response shape and curl examples, see [`docs/API.md`](docs/API.md).

## Architecture deep-dive

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for:

- The message lifecycle (harness + executor + `--mode rpc` event protocol)
- The `ToolRecorder` split between harness and executor (and why the executor is the sole writer of tool rows)
- The audit log schema, per-tool `tool_output` shapes, and SQL recipes ([`docs/TOOL-AUDIT-LOG.md`](docs/TOOL-AUDIT-LOG.md))
- The session lifecycle, including the durable-resume path (`session_replay.rs` + `resume.rs` + `agent_registry.rs`)
- Streaming tool execution and the `MessageBus`

## Operations

See [`docs/OPERATIONS.md`](docs/OPERATIONS.md) for the systemd unit, database provisioning, migrations, log / metric endpoints, common failure modes, the upgrade procedure, and backups.

See [`AGENTS.md`](AGENTS.md) for the debugging checklist (the steps to follow when something is wrong, in order) and a list of things that previously bit us.

## Development

```bash
# Run unit + integration tests
cargo test

# Run only the integration suite (needs DATABASE_URL pointing at a clean DB)
DATABASE_URL=postgres://postgres@localhost/forge_test cargo test -p forge-api --test integration_tests

# Lint and format
cargo fmt --all
cargo clippy --all-targets -- -D warnings

# Auto-reload on code change
watchexec -e rs -r cargo run -p forge-api

# Restart the service after a code change
sudo systemctl restart forge-api
sudo journalctl -u forge-api -f

# Run the API smoke test against a running service
bash scripts/test-api.sh
```

## Status

Forge is functional and the core flows work end-to-end:

- All four tools (`bash`, `read`, `write`, `edit`) execute correctly, with call and result rows persisted to the audit log and linked by `tool_call_id`.
- Multi-turn conversations with parallel tool calls work; the model can run several tools in one turn and get the results back.
- Durable resume works: a session killed by `systemctl restart forge-api` is rebuilt from the `messages` table on the next message, with the working tree and the model's context both restored. Verified on a 1166-message session.
- The `UNIQUE (session_id, sequence)` constraint is enforced, and `get_next_sequence()` is serialized with `pg_advisory_xact_lock(1, hashtext(session_uuid::text))` so the harness and executor can write concurrently.
- The CLI's `message ask` streams the response by polling `GET /messages`.

Known limitations:

- The `bash` tool's stdout / stderr are **not** captured into the result row when the streaming path is used (`tool_output.stdout` and `tool_output.stderr` are `NULL`). The exit code, success flag, and duration are recorded. Workaround: the model has learned to redirect output to a file and `read` it back. Fixing this is on the roadmap.
- The sandbox subsystem is wired in but the per-session nspawn container is only used when the operator has bootstrapped `/forge/sandbox/base/` and `get_container` returns `Ok`. On a host without nspawn or Nix, `bash` falls back to host execution in the session's working directory. `read` / `write` / `edit` are always host-side Rust file ops; they hit the bind-mounted working dir.
- The pi subprocess is launched with `--no-extensions` for stability (a user extension that captures the pi ctx in a `session_start` handler and references it from a timer can crash pi after a context switch). The `forge-tools` extension is still loaded via the explicit `--extension <path>` flag.

## License

Dual-licensed under MIT or Apache-2.0, at your option. See [`LICENSE-MIT`](LICENSE-MIT) and [`LICENSE-APACHE`](LICENSE-APACHE) for the full texts. The workspace license is also declared in [`Cargo.toml`](Cargo.toml) as `license.workspace = "MIT OR Apache-2.0"`.
