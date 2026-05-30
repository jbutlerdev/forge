# Forge - Durable AI Coding Agents

Forge is a platform for running durable AI coding agents with session persistence, isolated tool execution, and Git integration.

## Features

- **Session Persistence**: Sessions persist across messages with full conversation history
- **Tool Isolation**: Tools execute in per-session isolated directories  
- **Persistent Agents**: LLM context preserved across messages via persistent pi processes
- **Git Integration**: Automatic repository cloning and pull on resume
- **Nix Shell Support**: Run commands in nix shells via profile configuration
- **REST API**: Full CRUD for profiles, sessions, and messages
- **Observability**: Structured logging, tracing spans, JSON metrics, and Prometheus format
- **CLI**: Bash-based CLI for easy interaction

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         forge-api (Rust)                        │
│   REST API │ PostgreSQL │ Tool Executor │ Session Manager       │
└─────────────────────────────────────────────────────────────────┘
                             │
                             │ Tool requests
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                         pi (Node.js)                             │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              forge-tools Extension                      │   │
│   │   - Registers tools (bash, read, write, edit)         │   │
│   │   - Forwards to /tools/execute API                    │   │
│   └─────────────────────────────────────────────────────────┘   │
│   LLM Calls (Anthropic, OpenAI, etc.)                          │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Session Directories                           │
│   /forge/sessions/{session_id}/                                 │
│   - Isolated working directory per session                      │
│   - Git repositories cloned per session                          │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.75+
- PostgreSQL 15+
- Node.js 18+ (for pi CLI and forge-tools extension)
- pi CLI: `npm install -g @mariozechner/pi-coding-agent`

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/forge.git
cd forge

# Install Rust dependencies
cargo build

# Setup PostgreSQL
sudo -u postgres createuser -s postgres
sudo -u postgres createdb forge

# Run migrations
sqlx migrate run

# Build forge-tools extension
cd extensions/forge-tools && npm install && npm run build && cd ../..

# Create session directory
sudo mkdir -p /forge/sessions
sudo chmod 777 /forge/sessions
```

### Configuration

Create a `.env` file:

```bash
DATABASE_URL=postgres://postgres@localhost/forge
FORGE_API_URL=http://localhost:8080/api/v1
ANTHROPIC_API_KEY=your-api-key-here
```

### Running

```bash
# Start the API server
cargo run -p forge-api
```

The API will be available at `http://localhost:8080/api/v1`

## Usage

### Create a Profile

```bash
forge profile create my-agent \
    --provider anthropic \
    --model claude-sonnet-4-20250514 \
    --working-dir /tmp/my-project
```

### Create a Session

```bash
forge session create <profile-id> --title "My Session"
```

### Send Messages

```bash
forge send <session-id> "Read the main.rs file and explain it"
```

### View Messages

```bash
forge messages <session-id>
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | API metrics (JSON) |
| GET | `/metrics/prometheus` | API metrics (Prometheus format) |
| POST | `/profiles` | Create profile |
| GET | `/profiles` | List profiles |
| GET | `/profiles/get?id=<uuid>` | Get profile |
| PATCH | `/profiles/update?id=<uuid>` | Update profile |
| DELETE | `/profiles/delete?id=<uuid>` | Delete profile |
| POST | `/sessions` | Create session |
| GET | `/sessions` | List sessions |
| GET | `/sessions/{id}` | Get session |
| GET | `/sessions/{id}/status` | Get session status |
| POST | `/sessions/{id}/resume` | Resume session |
| GET | `/sessions/{id}/git` | Get git status |
| POST | `/sessions/{id}/git/pull` | Pull latest changes |
| DELETE | `/sessions/delete?id=<uuid>` | Delete session |
| POST | `/messages` | Send message |
| GET | `/messages?session_id=<uuid>` | List messages |
| POST | `/tools/execute` | Execute tool |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | Required | PostgreSQL connection string |
| `FORGE_API_URL` | `http://localhost:8080/api/v1` | API base URL |
| `ANTHROPIC_API_KEY` | Required | Anthropic API key |
| `OPENAI_API_KEY` | Optional | OpenAI API key |

## Nix Shell Integration

Forge supports running commands within a nix shell:

```bash
forge profile create my-agent \
    --provider anthropic \
    --model claude-sonnet-4-20250514 \
    --nix-shell "hello curl git"
```

## Git Integration

Forge automatically clones git repositories when creating sessions:

```bash
forge profile create my-project \
    --provider anthropic \
    --model claude-sonnet-4-20250514 \
    --git-url "https://github.com/user/repo.git" \
    --git-ref main
```

## Development

```bash
# Run tests
cargo test

# Build release
cargo build --release

# Format code
cargo fmt

# Lint
cargo clippy
```

## Project Structure

```
forge/
├── crates/forge-api/src/
│   ├── main.rs           # Entry point
│   ├── api/              # HTTP handlers
│   ├── db/               # Database types
│   ├── tool_executor.rs  # Tool execution
│   ├── session_manager.rs # Session management
│   ├── agent_registry.rs # pi process management
│   ├── sandbox.rs        # Container management
│   └── observability.rs  # Metrics & tracing
├── extensions/forge-tools/
│   └── src/index.ts      # pi extension
├── cli/
│   └── forge             # CLI entry point
└── migrations/           # SQL migrations
```

## License

MIT
