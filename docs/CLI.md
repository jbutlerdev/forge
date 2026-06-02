# CLI reference

`cli/forge` is a small bash client that exercises the API. It is intended as a **reference implementation** and a quick way to drive the API from the shell — not as a full-featured client. Use `curl` or build your own client for anything serious.

The CLI is a dispatcher. The top-level script (`cli/forge`) routes to per-subcommand files in `cli/forge.d/`. Each subcommand follows the same pattern: dispatch on `$1`, call a `cmd_<name>_*` function.

## Setup

```bash
export FORGE_API_URL=http://localhost:8080
export FORGE_API_KEY=sk_forge_<...>
```

`FORGE_API_KEY` is read from the environment by every command that hits an authenticated endpoint. If it's missing, the API returns 401 and the CLI prints the body of the error response.

## Commands

```
forge <command> [subcommand] [args]

Commands:
  auth:
    register <email> <name> <password>            Create a user
    login    <email> <password>                   Get an API key

  profiles:
    profile create <name> [opts]                 Create a profile
    profile list                                  List profiles
    profile get <id>                              Get a profile
    profile update <id> [opts]                    Update a profile
    profile delete <id>                           Delete a profile

  sessions:
    session create <profile_id> [--title <t>]     Create a session
    session list                                  List sessions
    session get <id>                              Get a session
    session delete <id>                           Delete a session

  messages:
    message send <session_id> <text>              Send (fire-and-forget)
    message ask  <session_id> <text>              Send + stream the response
    message watch <session_id>                    Stream new messages
    message list <session_id>                     List all messages
    messages      <session_id>                    Alias for `message list`

  utilities:
    health                                        Liveness check
    status                                        API status summary
    metrics                                       JSON metrics
```

## Common patterns

### Create a profile, a session, and ask a question

```bash
# 1. register
forge register you@example.com "Your Name" password123
# prints: { "user": {...}, "api_key": "sk_forge_<...>" }
# -> export the key in your shell

export FORGE_API_KEY=sk_forge_<...>

# 2. create a profile
PROFILE_ID=$(forge profile create my-agent \
  --provider proxy-anthropic \
  --model claude-sonnet-4-20250514 \
  --working-dir /tmp/my-project \
  | sed 's/\x1b\[[0-9;]*m//g' \
  | awk '/^{/,/^}/' \
  | jq -r '.id')
# -> PROFILE_ID is a uuid

# 3. create a session
SESSION_ID=$(forge session create "$PROFILE_ID" --title "demo" \
  | sed 's/\x1b\[[0-9;]*m//g' \
  | awk '/^{/,/^}/' \
  | jq -r '.session.id')
# -> SESSION_ID is a uuid

# 4. ask a question and stream the response
forge message ask "$SESSION_ID" "What is the capital of France?"
```

The `sed | awk | jq` dance extracts the JSON object from a CLI invocation that has a colored status line ("Session created successfully") before the JSON.

### Inspect a session's audit log

```bash
forge message list "$SESSION_ID" \
  | head -100
```

The output is one row per message with role, sequence, and a content preview. For structured tool input / output / duration, query the database directly:

```bash
sudo -u postgres psql forge -c "
SELECT sequence, role, tool_name, tool_call_id, duration_ms,
       jsonb_pretty(tool_output) AS output
FROM messages
WHERE session_id = '$SESSION_ID'
ORDER BY sequence;
"
```

### Watch an already-active session

If a session is being driven by another process (e.g. your own client), you can tail the message log from a shell:

```bash
forge message watch "$SESSION_ID"
```

This polls `GET /messages?session_id=…` on a one-second loop and prints new rows as they appear. It exits when the API has been quiet for five seconds *after* at least one assistant text response has been seen — i.e. when a turn appears to be complete. Multi-step turns that run several tool calls in a row before the final text are handled correctly.

### Stream a tool call

```bash
# Bash with streaming SSE output
curl -N -X POST "$FORGE_API_URL/tools/execute/stream" \
  -H "X-API-Key: $FORGE_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"session_id\":\"$SESSION_ID\",\"tool\":\"bash\",\"input\":{\"command\":\"ls -la\",\"timeout_ms\":5000}}"
```

The CLI does not wrap this; use `curl -N` directly.

## Output rendering

`message ask`, `message watch`, and `message list` all use a single JSON-to-text renderer. Each row is colored by role:

| Role | Color | Example |
|---|---|---|
| `user` | cyan | `[user] 2026-06-01T17:30:42.697857Z` |
| `assistant` (text) | green | `[assistant] 2026-06-01T17:30:45.789324Z` |
| `assistant` (tool call) | magenta | `[assistant (bash)] ...` |
| `tool` (result) | yellow | `[tool (bash)] ...` |

Tool-call rows show the `call_id` and the model's arguments (as JSON). Tool-result rows show the `call_id`, the duration in ms, the structured `tool_output` jsonb, and the flattened `content` text. The `call_id` is the join key — visually match the magenta `[assistant (read)]` call row to the yellow `[tool (read)]` result row with the same id to see the full trace for that step.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `FORGE_API_URL` | `http://localhost:8080` | base URL for all API calls |
| `FORGE_API_KEY` | (none) | sent as `X-API-Key` on every authenticated request |

## Common gotchas

- **`forge` is a bash script, not a binary.** The dispatcher sources the per-subcommand files at runtime. If you copy only `cli/forge` to another box, copy `cli/forge.d/` too.
- **The output of `forge session create` and `forge profile create` has a colored status line *before* the JSON.** Pipe through `sed 's/\x1b\[[0-9;]*m//g' | awk '/^{/,/^}/' | jq` to extract the JSON for programmatic use.
- **`forge message ask` polls `GET /messages`; the API also has `GET /sessions/{id}/events` (SSE) for live push.** The CLI uses polling for simplicity (no need to manage SSE reconnects). Long-running clients that want lower latency should subscribe to the SSE endpoint. See [`docs/API.md`](API.md) §Streaming.
- **The CLI uses `jq` everywhere.** Install it (`apt install jq`).
