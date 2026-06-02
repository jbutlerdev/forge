# Forge

A platform for running durable AI coding agents backed by [pi](https://github.com/badlogic/pi-mono). The Rust API server owns persistent per-session pi processes, persists every user / assistant / tool-call / tool-result row to PostgreSQL, and exposes a REST API for clients. A bash CLI is included as a reference client.

```
┌───────────────────────┐      POST /messages      ┌────────────────────────┐
│  Client (CLI, curl,   │ ───────────────────────▶ │  forge-api (Rust)      │
│  your own app)        │                         │  • spawns pi per       │
│                       │ ◀──── GET /sessions ──── │    session             │
│                       │      /{id}/events       │  • streams pi events   │
│                       │       (SSE, live push)  │  • runs tools          │
│                       │                         │  • runs tool executor  │
│                       │ ◀──── POST /tools/... ── │  • records audit log   │
└───────────────────────┘                         └──────────┬─────────────┘
                                                              │ stdin/stdout
                                                              ▼
                                                ┌────────────────────────┐
                                                │  pi (Node.js)          │
                                                │  + forge-tools ext     │
                                                │  • LLM loop            │
                                                │  • calls back to API   │
                                                │    for tool execution  │
                                                └────────────────────────┘
```

## Features

- **Session persistence** — every user prompt, assistant reply, tool call, and tool result is written to PostgreSQL with a monotonic per-session `sequence`. Replay any session from the message table.
- **Long-lived pi processes** — one pi subprocess per session, kept warm for the life of the session so LLM context is preserved across turns (no re-reading the transcript on every message).
- **Structured tool audit log** — call rows (`role='assistant'`, with `tool_input` jsonb) and result rows (`role='tool'`, with `tool_output` jsonb and `duration_ms`) are linked by `tool_call_id` so you can reconstruct exactly what the model asked for and what actually happened.
- **Streaming tool execution** — `POST /tools/execute/stream` returns Server-Sent Events with `stdout` / `stderr` / `tool_end` chunks as the command runs. `POST /tools/execute` returns a one-shot `ToolOutput` for non-streaming tools (read, write, edit).
- **Live agent events** — `GET /sessions/{id}/events?since=<seq>` is an SSE endpoint that streams new `messages` rows and `agent_end` signals as the agent works. The matrix appservice and any other live client subscribes here. Polling `GET /messages?session_id=…` is also supported and is equivalent (the SSE endpoint is the live one).
- **Per-session isolation** — each session gets `/forge/sessions/<session_id>/` as its working directory. Tools execute inside that directory; nothing else on the host is reachable from a tool call.
- **Example CLI** — `cli/forge` is a small bash client that exercises the API: register, login, profiles, sessions, and a `message ask` command that streams the response by polling.
- **Observability** — structured logging via `tracing`, JSON metrics at `/metrics`, Prometheus exposition at `/metrics/prometheus`.

## Status

Forge is functional but still in flux. The pieces that work end-to-end:

- All four tools (`bash`, `read`, `write`, `edit`) execute correctly, with results persisted to the audit log linked to the call.
- Multi-turn conversations with parallel tool calls work; the model can run several tools in one turn and get the results back.
- Errors (e.g. `read` on a missing file) are captured in `tool_output.error` with `is_error=true`.
- The CLI's `message ask` streams the response by polling `GET /messages`.
- The DB enforces a UNIQUE `(session_id, sequence)` constraint, and the sequence allocation is serialized with a transaction-scoped advisory lock to allow the harness and the executor to write concurrently.

Known limitations:

- The `bash` tool's stdout/stderr is **not** captured into the result row when the streaming path is used (`tool_output.stdout` and `tool_output.stderr` are NULL). The exit code and duration are recorded. Workaround: in the meantime, the model has learned to redirect output to a file and `read` it back. Fixing this is on the roadmap.
- The sandbox subsystem (`sandbox.rs`) exists but `sandbox_manager.init()` is allowed to fail; tools currently run on the host, not in containers.

## Repository layout

```
forge/
├── crates/forge-api/          # The Rust API server
│   ├── src/
│   │   ├── main.rs            # Entry point, wires AppState, starts axum
│   │   ├── lib.rs             # Public crate surface (modules, error type)
│   │   ├── api/
│   │   │   ├── mod.rs         # HTTP handlers for profiles, sessions, messages,
│   │   │   │                  #   /tools/execute, and the harness event loop
│   │   │   ├── auth.rs        # Register / login / API key middleware
│   │   │   └── sse.rs         # /tools/execute/stream and streaming bash
│   │   ├── db/                # SQLx row types
│   │   ├── pi_agent.rs        # pi subprocess management (--mode rpc)
│   │   ├── agent_registry.rs  # Per-session PiAgent map
│   │   ├── tool_executor.rs   # bash / read / write / edit
│   │   ├── recording.rs       # ToolRecorder trait + DbToolRecorder
│   │   ├── session_manager.rs # /forge/sessions lifecycle
│   │   ├── sandbox.rs         # systemd-nspawn wrapper (not active)
│   │   ├── observability.rs   # Metrics
│   │   └── logging.rs
│   └── migrations/            # Embedded by sqlx::migrate! at startup
├── extensions/forge-tools/    # pi TypeScript extension (registers tools)
├── cli/                       # Reference bash client
└── systemd/forge-api.service  # Example unit file
```

## Quick start

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Rust | 1.75+ | `rustup install stable` |
| PostgreSQL | 15+ | Database owned by a role that can `CREATE EXTENSION pgcrypto` |
| Node.js | 20+ | Only needed for the `pi` agent binary |
| pi | latest | `npm install -g @earendil-works/pi-coding-agent` |

The full pi package path is read from `FORGE_TOOLS_EXTENSION` at startup if set; otherwise the build falls back to a hard-coded search path under `/root/.nvm/versions/node/.../lib/node_modules/@earendil-works/pi-coding-agent/...`.

### Build

```bash
# Database
sudo -u postgres createuser -s postgres
sudo -u postgres createdb forge

# Forge
cargo build --release -p forge-api
cp target/release/forge-api /opt/forge/forge-api

# Service
sudo mkdir -p /etc/forge
sudo cp systemd/forge-api.service /etc/systemd/system/
```

### Configure

`/etc/forge/forge.env`:

```ini
DATABASE_URL=postgres://postgres@localhost/forge
FORGE_API_URL=http://localhost:8080
RUST_LOG=forge_api=debug,info
# PATH must include the directory containing the `pi` binary
PATH=/root/.nvm/versions/node/v20.18.1/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
```

> The service runs as a systemd unit. The unit file at `systemd/forge-api.service` is an example — edit `User=` and security directives to match the host (the in-repo example uses `User=forge`; on the dev box we use `User=root` because there's no `forge` user).

### Run

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now forge-api
sudo systemctl status forge-api
sudo journalctl -u forge-api -f
```

Migrations are embedded in the binary via `sqlx::migrate!("./migrations")` and run automatically on startup. Adding a new migration means dropping a `NNN_description.sql` file into `crates/forge-api/migrations/` and rebuilding.

## CLI quick start

```bash
export FORGE_API_URL=http://localhost:8080
export FORGE_API_KEY=sk_forge_...

# Sign in (or just set an API key directly)
forge register you@example.com "Your Name" password123
# -> sets FORGE_API_KEY in your shell once you export what the response prints

# Create a profile
forge profile create my-agent \
  --provider proxy-anthropic \
  --model claude-sonnet-4-20250514 \
  --working-dir /tmp/my-project

# Create a session
SESSION=$(forge session create <profile-id> --title "Demo" \
  | sed 's/\x1b\[[0-9;]*m//g' | awk '/^{/,/^}/' | jq -r '.session.id')

# Send a question and stream the response
forge message ask "$SESSION" "What is the capital of France?"
```

The CLI is an example — see [`docs/CLI.md`](docs/CLI.md) for the full reference. For richer clients, hit the REST API directly.

## API summary

All endpoints accept / return JSON. Auth is `X-API-Key: <key>`.

| Method | Endpoint | Purpose |
|---|---|---|
| `GET`   | `/health` | Liveness probe |
| `GET`   | `/metrics`, `/metrics/prometheus` | Metrics |
| `POST`  | `/auth/register` | Create a user |
| `POST`  | `/auth/login` | Exchange email/password for an API key |
| `POST`  | `/profiles` | Create a profile (LLM provider + model + tools) |
| `GET`   | `/profiles` | List profiles |
| `GET`   | `/profiles/get?id=<uuid>` | Get a profile |
| `PATCH` | `/profiles/update?id=<uuid>` | Update a profile |
| `DELETE`| `/profiles/delete?id=<uuid>` | Delete a profile |
| `POST`  | `/sessions` | Create a session |
| `GET`   | `/sessions` | List sessions |
| `GET`   | `/sessions/{id}` | Get a session |
| `GET`   | `/sessions/{id}/events?since=<seq>` | SSE stream of new message rows + turn_ended signals |
| `DELETE`| `/sessions/delete?id=<uuid>` | Delete a session |
| `POST`  | `/messages` | Send a message; the API spawns/uses pi in the background |
| `GET`   | `/messages?session_id=<uuid>` | List messages (poll for new rows) |
| `POST`  | `/tools/execute` | One-shot tool call (read, write, edit, non-streaming bash) |
| `POST`  | `/tools/execute/stream` | Streaming tool call (SSE for stdout/stderr) |

For the per-endpoint request/response shape see [`docs/API.md`](docs/API.md).

## Architecture deep-dive

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the message lifecycle, the `ToolRecorder` split between harness and executor, the `pi --mode rpc` event protocol, and how the audit log is assembled.

## Operations

See [`docs/OPERATIONS.md`](docs/OPERATIONS.md) for migrations, the systemd unit, log/metric endpoints, common failure modes, and the upgrade procedure.

## Development

```bash
# Build
cargo build --release -p forge-api

# Run tests
cargo test

# Restart the service after a code change
sudo systemctl restart forge-api
sudo journalctl -u forge-api -f
```

## License

MIT
