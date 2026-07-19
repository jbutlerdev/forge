# API reference

Forge is an HTTP/JSON API. The base URL is `http://localhost:8080` by default (`FORGE_API_URL` env var). All authenticated endpoints expect `X-API-Key: <key>`.

This document covers the request and response shapes. For the high-level architecture, see [`ARCHITECTURE.md`](ARCHITECTURE.md). For the example CLI client, see [`CLI.md`](CLI.md).

## Conventions

- **JSON** request and response bodies. `Content-Type: application/json`.
- **UUIDs** as path or query parameters, no braces.
- **Timestamps** are RFC 3339 (`2026-06-01T17:32:49.618510Z`).
- **Authentication** via `X-API-Key: <key>` on every endpoint except `/health`, `/auth/register`, and `/auth/login`.
- **Errors** look like `{"error": "<message>"}` with the appropriate 4xx/5xx status. Some errors add extra context fields.

## Health and observability

### `GET /health`

Liveness probe. Always `200 OK` with body `OK` if the service is up.

```bash
curl -s http://localhost:8080/health
# OK
```

### `GET /metrics`

JSON metrics. Includes request counts, error counts, active sessions, active agents, and per-tool execution counts.

```bash
curl -s http://localhost:8080/metrics | jq .
```

### `GET /metrics/prometheus`

Same metrics in Prometheus exposition format. Scrape this with Prometheus.

```bash
curl -s http://localhost:8080/metrics/prometheus
```

## Auth

### `POST /auth/register`

Create a user.

Request:
```json
{"email": "user@example.com", "name": "User Name", "password": "password123"}
```

Response (201):
```json
{
  "user": {"id": "<uuid>", "email": "...", "name": "...", "created_at": "..."},
  "api_key": "sk_forge_<...>"
}
```

Save the `api_key` immediately — it's not returned again. The CLI prints it once.

### `POST /auth/login`

Exchange email + password for a new API key.

Request:
```json
{"email": "user@example.com", "password": "password123"}
```

Response (200):
```json
{
  "user": {...},
  "api_key": "sk_forge_<...>",
  "expires_at": "2027-06-01T00:00:00Z"
}
```

### `POST /api-keys`

Mint a new API key for the authenticated user, with an optional `expires_in_days`.

Request:
```json
{"expires_in_days": 365}
```

### `GET /api-keys`

List the current user's API keys (id, prefix, last_used_at, expires_at — never the full key).

## Profiles

A profile bundles a model, provider, working directory, and (optionally) a git repo and nix shell. Sessions are created against a profile.

### `POST /profiles`

Request:
```json
{
  "name": "my-agent",
  "provider": "proxy-anthropic",
  "model": "claude-sonnet-4-20250514",
  "working_dir": "/tmp/my-project",
  "base_url": "https://proxy.example.com",
  "api_key": "<provider api key>",
  "git_url": "https://github.com/me/repo.git",
  "git_ref": "main",
  "nix_shell": "hello curl git",
  "system_prompt": "You are a helpful assistant.",
  "tools": ["bash", "read", "write", "edit"]
}
```

`provider` is one of `anthropic`, `openai`, `proxy-anthropic`. The proxy variants let you point at a custom OpenAI-compatible base URL.

Response (201): the created `Profile` object.

### `GET /profiles`

Query: `?limit=20&offset=0` (optional). Response: `{profiles: [...]}`.

### `GET /profiles/get?id=<uuid>`

Response: the `Profile` object.

### `PATCH /profiles/update?id=<uuid>`

Any subset of the create fields. Response: the updated `Profile`.

### `DELETE /profiles/delete?id=<uuid>`

Response (204) on success.

## Sessions

### `POST /sessions`

Request:
```json
{"profile_id": "<uuid>", "title": "My session"}
```

Response (201):
```json
{
  "session": {
    "id": "<uuid>",
    "profile_id": "<uuid>",
    "title": "My session",
    "cell_host": null,
    "cell_state": null,
    "created_at": "...",
    "last_active": "...",
    "ended_at": null,
    "user_id": null
  },
  "working_dir": "/forge/sessions/<uuid>"
}
```

### `GET /sessions`

List the user's sessions.

### `GET /sessions/{id}`

One session by id.

### `PATCH /sessions/{id}`

The model switcher. Change a session's provider / model / base_url /
api_key via per-session overrides, **without changing the profile**
— so the working dir, git repo, sandbox, tools, and system_prompt
stay as the profile configured them. Only the model + credentials
change. The prior conversation is replayed on the next message, so
history is preserved.

```bash
curl -X PATCH http://localhost:8080/sessions/$SID \
  -H "X-API-Key: $FORGE_API_KEY" -H "Content-Type: application/json" \
  -d '{"provider":"proxy","model":"llamacpp/qwen3.6-27b","base_url":null,"api_key":null}'
```

Each override field is a `serde_json::Value`: a string sets the
override; JSON `null` clears it (falls back to the profile / models.json);
omitting the key leaves the column alone. Returns `{ session, profile }`
where the session's `override_*` fields reflect the new state and
`profile` is the (unchanged) profile so the UI can compute the
effective model = override ?? profile.*. Title can be updated in the
same call (`"title":"new"`).

### `DELETE /sessions/delete?id=<uuid>`

Delete a session. Cascades to its messages.

## Streaming

### `GET /sessions/{id}/events?since=<seq>`

Server-Sent Events stream of new message rows for a session. Clients
that want live updates (e.g. the matrix appservice) open one SSE
connection per active session and read events as they're written to
the `messages` table.

On connect the server replays every row with `sequence > since`
before going live. The `since` parameter is optional and defaults
to 0 (full history). SSE event names:

| Event | Data | When |
|---|---|---|
| `message` | `{"message": <Message row>}` | The harness or tool executor wrote a new row |
| `turn_ended` | `{"session_id": "..."}` | The agent signaled `agent_end` for this turn |
| `heartbeat` | `{}` | Every 15 seconds, to keep the connection alive across proxies |

The stream also sends a `: keepalive` SSE comment every 15 seconds.

```bash
curl -N "http://localhost:8080/sessions/$SESSION_ID/events?since=0" \
  -H "X-API-Key: $FORGE_API_KEY"
```

If the consumer falls behind (the bus buffer fills up), the server
emits a `lagged` event with `{"missed": N}`. The client should
reconnect with its latest known `since`; the catch-up phase will
replay the missing rows.

## Messages

### `POST /messages`

Send a user message. The API spawns (or reuses) a `pi` process for the session, writes the prompt to its stdin, and returns immediately. The harness handles the response asynchronously, writing each event to the `messages` table as it arrives.

Request:
```json
{"session_id": "<uuid>", "content": "What is the capital of France?"}
```

Response (202):
```json
{
  "message": {
    "id": "<uuid>",
    "session_id": "<uuid>",
    "sequence": 1,
    "role": "user",
    "content": "What is the capital of France?",
    "tool_name": null,
    "tool_input": null,
    "tool_call_id": null,
    "tool_output": null,
    "duration_ms": null,
    "created_at": "..."
  }
}
```

`sequence` is the row's per-session position. The client polls `GET /messages` to see the agent's response and any tool calls.

### `GET /messages?session_id=<uuid>`

List all messages in a session, ordered by `sequence`.

Response:
```json
{
  "messages": [
    {
      "id": "...",
      "session_id": "...",
      "sequence": 1,
      "role": "user",
      "content": "What is the capital of France?",
      "tool_name": null,
      "tool_input": null,
      "tool_call_id": null,
      "tool_output": null,
      "duration_ms": null,
      "created_at": "..."
    },
    {
      "id": "...",
      "session_id": "...",
      "sequence": 2,
      "role": "assistant",
      "content": "The capital of France is Paris.",
      "tool_name": null,
      "tool_input": null,
      "tool_call_id": null,
      "tool_output": null,
      "duration_ms": null,
      "created_at": "..."
    }
  ]
}
```

Tool-call rows have `tool_call_id` and `tool_input` populated; tool-result rows have `tool_call_id`, `tool_output`, and `duration_ms` populated. See `docs/ARCHITECTURE.md` §5 for the join semantics.

## Tools

The tool endpoints are how the `forge-tools` extension runs tools on behalf of the LLM. You can also call them directly to run a tool without involving the LLM at all.

### `POST /tools/execute`

One-shot tool execution. Returns the full `ToolOutput` when the tool finishes (success or error).

Request:
```json
{
  "session_id": "<uuid>",
  "tool": "read",
  "input": {"path": "/etc/hostname", "limit": 1},
  "tool_call_id": "my-read-1"
}
```

`tool` is one of `read`, `write`, `edit`, `bash`. For `bash`, `input` is `{"command": "...", "timeout_ms": 30000}` (the timeout defaults to 30s).

`tool_call_id` is optional from a non-extension caller. pi passes one through; if you call this endpoint yourself you can omit it or provide a unique id.

Response (200):
```json
{
  "output": "dev",
  "error": null,
  "success": true
}
```

The successful result is also written to the audit log (`messages` table) by the executor, with the `tool_call_id` you provided. This means the audit log captures direct tool calls, not just ones the LLM initiated.

### `POST /tools/execute/stream`

Streaming tool execution via Server-Sent Events. Use this for `bash` when you want to see stdout/stderr as it's produced. The other tools work too but there's no benefit over the one-shot endpoint.

Request body: same as `/tools/execute`.

Response: `text/event-stream`. Each event has an `event:` name and a JSON `data:` payload.

| Event | Data |
|---|---|
| `tool_start` | `{"tool_call_id": "...", "tool": "...", "input": {...}}` |
| `stdout` | `{"tool_call_id": "...", "chunk": "<8KB of stdout>"}` (bash only) |
| `stderr` | `{"tool_call_id": "...", "chunk": "<8KB of stderr>"}` (bash only) |
| `tool_end` | `{"tool_call_id": "...", "success": true, "exit_code": 0, "duration_ms": 42}` |
| `error` | `{"tool_call_id": "...", "error": "<message>"}` |
| `done` | `{"done": true}` (always last) |

The result is also written to the audit log with the same `tool_call_id`. The streaming-bash path records `tool_output` with `stdout` and `stderr` set to `null` and `streamed: true`, because the bytes went to the SSE consumer rather than to a captured buffer.

#### curl example

```bash
curl -N -X POST http://localhost:8080/tools/execute/stream \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $FORGE_API_KEY" \
  -d '{"session_id":"'"$SESSION_ID"'","tool":"bash","input":{"command":"ls -la /etc | head -20","timeout_ms":5000}}'
```

The `-N` disables curl's output buffering so you see events as they arrive.

## OpenAI-compatible API

Forge exposes an OpenAI-compatible surface so any client that speaks
the OpenAI Chat Completions API — the `openai` SDK, LangChain,
Continue, chat UIs — can drive a forge agent without learning the
native API. Point your OpenAI client at forge:

```bash
export OPENAI_BASE_URL=http://localhost:8080/v1
export OPENAI_API_KEY=$FORGE_API_KEY   # the same sk_forge_... key
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8080/v1", api_key=FORGE_API_KEY)
resp = client.chat.completions.create(
    model="my-coding-profile",   # a forge profile name
    messages=[{"role": "user", "content": "Refactor utils.py"}],
)
print(resp.choices[0].message.content)
```

### Authentication

`Authorization: Bearer <forge-api-key>` (the standard OpenAI header).
The native `X-API-Key: <key>` header is also accepted on the `/v1/*`
endpoints, so a client that already has a forge key can use either
surface. Either way the key is validated with the real hash + DB
lookup, not just a presence check.

### Model → profile mapping

The OpenAI `model` field selects the forge backend. A forge **profile**
(provider + model + system prompt + tools + sandbox config) *is* a
"model" from the client's perspective:

- `model: "<profile-name>"` — **stateless**. A fresh ephemeral
  session is created for the request, the request's `messages` are
  replayed into the session as prior context, the agent runs one
  turn, and the final assistant text is returned. Matches OpenAI's
  stateless semantics: send the full conversation each request, get
  one response back.
- `model: "forge:<session-id>"` — **stateful**. Reuses an existing
  forge session (which holds its conversation state in pi). Only the
  last user message is sent; the rest of `messages` is ignored. Use
  this for long-running agentic sessions where the client doesn't
  want to re-send history every turn.

### `POST /v1/chat/completions`

Request body (OpenAI shape; fields forge uses are required, the rest
are accepted and ignored):

```json
{
  "model": "my-coding-profile",
  "messages": [
    {"role": "system", "content": "You are a helpful coding agent."},
    {"role": "user", "content": "Fix the failing tests."}
  ],
  "stream": false,
  "temperature": 0.7,
  "max_tokens": 4096
}
```

Non-streaming response (200):

```json
{
  "id": "chatcmpl-<uuid>",
  "object": "chat.completion",
  "created": 1718000000,
  "model": "my-coding-profile",
  "choices": [
    {
      "index": 0,
      "message": {"role": "assistant", "content": "<the agent's final answer>"},
      "finish_reason": "stop"
    }
  ],
  "usage": {"prompt_tokens": 12, "completion_tokens": 48, "total_tokens": 60}
}
```

Streaming (`"stream": true`) returns `text/event-stream` with
standard OpenAI `chat.completion.chunk` events — an opening chunk
carrying `delta.role = "assistant"`, one chunk per text delta with
`delta.content`, a final chunk with `finish_reason: "stop"`, then
`data: [DONE]`.

Errors use OpenAI's envelope: `{"error": {"message": "…", "type": "…", "code": "…"}}`.
Status codes: 401 (bad key), 400 (empty messages / last message not a
user message), 404 (unknown profile name / unknown `forge:<id>`
session), 504 (agent timeout), 500 (agent died / internal error).

#### Agentic turns

A forge agent is agentic: it can call tools (`bash`, `read`, `write`,
`edit`) across many internal rounds before producing its final answer.
From the OpenAI client's view the call is single-turn, but the backend
may run for minutes while the agent works — the HTTP request stays
open until the turn ends. Tool calls the agent makes internally are
forge-internal and are **not** returned as OpenAI `tool_calls`; the
client just receives the final text. Every assistant text chunk is
also persisted to the audit log, so OpenAI-driven turns are
indistinguishable from native `/messages`-driven turns in the
`messages` table and the live SSE event stream.

#### Limitations

- `tool_calls` / `tool`-role messages in the request history are not
  reconstructed into the session (forge's tools are internal; clients
  won't have forge tool_calls to send back). Their text content is
  preserved; the tool round-trips are dropped. Pure user/assistant
  text conversations round-trip fully.
- `usage` is a rough `chars / 4` estimate, not real token counts.
- Generation parameters (`temperature`, `max_tokens`, `top_p`, `n`,
  …) are accepted and ignored; the profile's model settings govern
  generation. `n` is always 1.

### `GET /v1/models`

List forge profiles as OpenAI models. Each profile becomes a model
whose `id` is the profile `name` (the value the client passes as
`model:`). The `forge:<session-id>` stateful form is not listed —
clients construct it themselves.

```bash
curl -s http://localhost:8080/v1/models \
  -H "Authorization: Bearer $FORGE_API_KEY" | jq .
```

Response (200):

```json
{
  "object": "list",
  "data": [
    {"id": "my-coding-profile", "object": "model", "created": 1718000000, "owned_by": "forge"}
  ]
}
```

### `POST /v1/audio/transcriptions` — STT (Parakeet)

OpenAI-compatible speech-to-text. Proxies multipart uploads to the
Parakeet STT backend (`PARAKEET_URL`, default
`http://10.10.199.51:5093`) so a browser that can't reach the
internal voice container can still dictate. Auth: `X-API-Key` or
`Authorization: Bearer` (same as the rest of `/v1/*`).

```bash
curl -s http://localhost:8080/v1/audio/transcriptions \
  -H "X-API-Key: $FORGE_API_KEY" \
  -F "file=@dictation.webm" \
  -F "model=parakeet" \
  -F "response_format=json"
```

Returns Parakeet's `{"text": "…"}` untouched (or `text`/`srt`/
`vtt`/`verbose_json` per `response_format`). 503 if STT is
disabled (`PARAKEET_URL=` empty); 502 if the backend is
unreachable; 400 if no `file` field.

### `POST /v1/audio/speech` — TTS (Kokoro)

OpenAI-compatible text-to-speech. Proxies JSON to the Kokoro TTS
backend (`KOKORO_URL`, default `http://10.10.199.51:8766`) and
returns audio bytes (`audio/ogg` by default). Same auth.

```bash
curl -s http://localhost:8080/v1/audio/speech \
  -H "X-API-Key: $FORGE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"kokoro","input":"Hello","voice":"af_heart","response_format":"ogg"}' \
  --output hello.ogg
```

503 if TTS is disabled; 502 if unreachable; 400 on empty/non-JSON
body. Voices: see `/v1/audio/voices`.

### `GET /v1/audio/voices` — voice availability + catalog

Always returns 200 (even when both backends are down) so clients
can degrade gracefully. Probes Parakeet `/health` and Kokoro `/`,
returns which are up, the Kokoro default voice, and a curated list
of the voices shipped in Kokoro's stock `voices.bin`.

```bash
curl -s http://localhost:8080/v1/audio/voices \
  -H "X-API-Key: $FORGE_API_KEY" | jq .
```

```json
{
  "stt": true,
  "tts": true,
  "default_voice": "af_heart",
  "voices": ["af_heart", "af_bella", "am_adam"]
}
```

## Static file serving (the web UI)

The API server serves the web UI on two paths, chosen at startup:

- **Disk** (dev / override): if `FORGE_WEB_DIR` is set, or the repo
  `web/` dir is reachable via `CARGO_MANIFEST_DIR` (i.e. a `cargo
  run`), the UI is served from `ServeDir` with `index.html` as the
  SPA fallback. Live edits reload without a rebuild.
- **Embedded** (deployed binary): otherwise — e.g. a deployed
  `/opt/forge/forge-api` with no `CARGO_MANIFEST_DIR` and no
  `FORGE_WEB_DIR` — the UI is served from assets compiled into the
  binary with `include_str!` (`api/web.rs`). The deployed binary is
  self-contained: **no external files, no env config**.

Both paths serve deep links as `index.html` with HTTP 200 (not 404)
so the client-side router can take over. `api::build_app(state,
web_dir)` branches on the disk dir; `None` → embedded SPA handler.

| Variable | Default | Description |
|---|---|---|
| `FORGE_WEB_DIR` | `<repo>/web` (dev) / embedded (deployed) | Absolute path to the web UI's static assets. If unset and the repo `web/` exists (via `CARGO_MANIFEST_DIR`), that's used; otherwise the compile-time-embedded UI is served. Set explicitly to override with custom assets. |
| `PARAKEET_URL` | `http://10.10.199.51:5093` | Parakeet STT base URL. Set to empty string to disable STT. |
| `KOKORO_URL` | `http://10.10.199.51:8766` | Kokoro TTS base URL. Set to empty string to disable TTS. |
```
