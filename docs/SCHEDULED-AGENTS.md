# Scheduled forge agents with a matrix room

A small system on top of forge + [matrix_appservice](https://github.com/mule-ai/matrix_appservice) that lets an operator provision long-lived agents which:

1. Run on a schedule via systemd timers.
2. Have a per-agent forge profile (model, system prompt, tools, working dir).
3. Carry a `heartbeat.md` that drives each scheduled run.
4. Optionally carry an `AGENTS.md` that supplements the system prompt.
5. Have a Matrix room for the human to talk to between scheduled runs.

The room is a "you can also DM the agent" channel. The scheduled run is the primary reason the agent exists.

---

## 1. Architecture

```
                ┌──────────────────────────────────────────────────────────────┐
                │                       operator's box                          │
                │                                                              │
                │   ┌─────────────────────┐       ┌─────────────────────────┐  │
                │   │  forge-agent-setup  │       │ /etc/forge/agents/<n>/  │  │
                │   │  (bash, one-shot)   │──────▶│   agent.yaml            │  │
                │   └──────────┬──────────┘       │   heartbeat.md (req)    │  │
                │              │                  │   AGENTS.md    (opt)    │  │
                │              │                  │   session.json (gen)    │  │
                │              ▼                  └─────────────────────────┘  │
                │   POST /profiles  (forge)                   │                 │
                │   POST /sessions  (forge)                   │ session_id     │
                │   POST /api/v1/agents  (pi-matrix)          │                 │
                │              │                              │                 │
                │              ▼                              │                 │
                │   ┌─────────────────────┐                   │                 │
                │   │   forge-api (Rust)  │◀────POST /messages┘                 │
                │   │                     │                                       │
                │   │  one pi subprocess  │                                       │
                │   │  per session        │                                       │
                │   └──────────┬──────────┘                                       │
                │              │ stdin/stdout                                     │
                │              ▼                                                 │
                │   ┌─────────────────────┐                                       │
                │   │  pi + forge-tools   │                                       │
                │   └─────────────────────┘                                       │
                │                                                              │
                │   ┌─────────────────────┐                                       │
                │   │ forge-agent@<n>.    │  systemd timer                       │
                │   │   timer + service   │  ────── fires ─────▶  service        │
                │   └──────────┬──────────┘                       │              │
                │              │                                  ▼              │
                │              │                  ┌─────────────────────────┐   │
                │              │                  │  /usr/local/bin/        │   │
                │              │                  │  forge-heartbeat <name> │   │
                │              │                  └────────────┬────────────┘   │
                │              │                               │                 │
                │              └─────────────── reads session.json              │
                │                                              │                 │
                │                                              ▼                 │
                │                                    POST /messages  ───────────▶│
                │                                                              │
                └──────────────────────────────────────────────────────────────┘
                                                                                │
                                                                                │
                ┌───────────────────────────────────────────────────────────────┘
                │
                │  HTTP
                ▼
        ┌────────────────────────┐
        │  matrix_appservice     │    one instance, runs on the matrix homeserver
        │  (pi-matrix, Go)       │
        │                        │
        │  POST /api/v1/agents ──┼─▶ mints forge profile + session (cached),
        │                        │   creates Matrix room, invites user,
        │                        │   binds session_id ↔ room_id
        │                        │
        │  GET /sessions/:id/    │   forge SSE consumer; same code path
        │  events (consumed)     │   renders heartbeat turns into the room
        └────────────────────────┘
```

Three moving parts:

- **forge** — already does everything agent-shaped. The only thing we add on this side is two bash scripts and two systemd unit templates.
- **matrix_appservice** — needs one new endpoint, `POST /api/v1/agents`, so the operator can drive room creation without sending a DM as the bot.
- **operator** — runs `forge-agent-setup` once per agent. The script does every wire call; the operator's only job is to point it at an agent dir and confirm the matrix user.

The matrix_appservice and forge keep their existing responsibilities. No internal coupling. No schema changes.

---

## 2. On-disk layout

```
/etc/forge/
├── forge.env                         # existing secrets/env for the forge-api service
├── agents.yaml                       # NEW: global defaults (named profile templates, matrix defaults)
└── agents/                           # NEW: one directory per agent
    └── <name>/
        ├── agent.yaml                # NEW: name, profile reference + overrides, schedule, matrix target
        ├── heartbeat.md              # NEW (required): instructions for the scheduled run
        ├── AGENTS.md                 # NEW (optional): behavior supplement (system-prompt override)
        └── session.json              # NEW (generated): {profile_id, session_id, room_id, matrix_to_url}
```

### `/etc/forge/agents.yaml` — global defaults

```yaml
# Named profile templates. Agents reference one of these by name in
# their agent.yaml; inline fields in agent.yaml override the template.
profiles:
  default:
    provider: anthropic
    model: claude-sonnet-4-20250514
    system_prompt: "You are a helpful coding assistant."
    tools: [bash, read, write, edit]

  coder:
    provider: anthropic
    model: claude-sonnet-4-20250514
    system_prompt: "You write code. Be terse. No prose. No summaries."
    tools: [bash, read, write, edit]

  reviewer:
    provider: anthropic
    model: claude-sonnet-4-20250514
    system_prompt: "You review code changes. Cite line numbers. Be specific."
    tools: [read, bash]

# Matrix homeserver and default invitee. Every agent inherits these
# unless overridden in its own agent.yaml.
matrix:
  homeserver: http://localhost:8008
  domain: example.com
  default_user: "@ops:example.com"
```

The "all agents use the same profile for now" case is just `profile: default` everywhere. When the operator wants differentiation, they add a new entry under `profiles:` and point the relevant agent at it. No code change.

### `/etc/forge/agents/<name>/agent.yaml`

```yaml
# Per-agent config. Validated and partially rewritten by
# forge-agent-setup on each run (profile.id and session.id are
# filled in by the script).

name: foo-bot

# Profile: reference a named template, with optional inline overrides.
profile:
  template: default
  # Any field from the profile template can be overridden inline.
  # working_dir: /data/projects/foo
  # system_prompt: "..."
  # tools: [bash, read]

# Schedule. systemd OnCalendar / OnUnitActiveSec form.
schedule:
  on_calendar: "*-*-* *:00/15"   # every 15 minutes
  persistent: true               # run missed runs after downtime
  # on_unit_active_sec: 30m       # alternative to on_calendar
  # randomized_delay_sec: 30      # spread the herd

# Matrix integration
matrix:
  enabled: true
  user: "@alice:example.com"     # who to invite; falls back to default_user
  room_name: "Foo Bot"           # optional; defaults to "Pi: <name>"
```

### `/etc/forge/agents/<name>/heartbeat.md` (required)

Free-form markdown. The systemd service composes a single prompt from this and (optionally) `AGENTS.md`, and posts it to the forge session. The agent reads it and acts.

Example:

```markdown
# Heartbeat: foo-bot

You are running on the foo-bot heartbeat. Each tick:

1. `git -C /data/projects/foo status --porcelain`
2. If there are uncommitted changes, run `cargo check` and report any errors.
3. If CI is green and there are no errors, leave a one-line status in your reply.
4. If anything is broken, list the failures and stop.

Do not run any other tools. Do not write to the repo. Read-only.
```

### `/etc/forge/agents/<name>/AGENTS.md` (optional)

Free-form markdown. If present, its contents are prepended to the heartbeat prompt. The intent is "this is who I am, in plain text, across runs" — a more durable version of the system prompt that lives next to the agent and is easy to edit.

Example:

```markdown
# foo-bot identity

I am foo-bot. I look after /data/projects/foo.

## Style
- Reply in English.
- Be terse. No filler.
- When reporting status, lead with the verdict ("clean", "1 error", "5 changes").

## Constraints
- Never push to remote.
- Never install packages.
- Never modify code outside /data/projects/foo.

## Escalation
- If something is broken, ping @alice in the matrix room.
```

The heartbeat service concatenates `AGENTS.md + "\n\n" + heartbeat.md` (both with their original content) and posts the result as a single user message.

### `/etc/forge/agents/<name>/session.json` (generated)

```json
{
  "profile_id": "7f3a8c12-...",
  "session_id": "8c12a1b3-...",
  "room_id": "!AbCdEf:example.com",
  "matrix_to_url": "https://matrix.to/#/!AbCdEf:example.com",
  "created_at": "2026-06-04T18:30:00Z"
}
```

The heartbeat service reads this on every tick to know which forge session to talk to. The setup script regenerates it on each run.

---

## 3. The setup script: `scripts/forge-agent-setup`

Single bash script, lives in the forge repo, installed to `/usr/local/bin/forge-agent-setup` by the existing `scripts/install.sh`.

```bash
sudo forge-agent-setup <agent-name>
# or
sudo forge-agent-setup /etc/forge/agents/foo-bot    # path form
```

Behavior:

1. **Validate.** Parse `agent.yaml`. Require `heartbeat.md`. Verify the agent name (lowercase, alnum + dash, ≤ 32 chars).
2. **Resolve profile.** Look up `profile.template` in `/etc/forge/agents.yaml`. Merge inline overrides on top. Result is the effective forge profile.
3. **Mint or reuse profile.** `POST /profiles` to forge with the effective profile. The script caches the resulting `profile_id` back into `agent.yaml` as `profile.id`; re-runs reuse it.
4. **Mint or reuse session.** `POST /sessions` to forge. Cache `session_id` back into `agent.yaml` as `session.id`.
5. **Mint or reuse matrix room.** `POST /api/v1/agents` to the matrix_appservice (the new endpoint described in §5). Body: `{profile_id, session_id, user, room_name, working_dir, source}`. The endpoint is responsible for creating the room and inviting the user; on re-runs it should be idempotent (reuse the existing `session_id → room_id` binding if present).
6. **Write `session.json`.** Contains `profile_id`, `session_id`, `room_id`, `matrix_to_url`, `created_at`.
7. **Render systemd units.** Substitute `<name>` and the schedule into the templates in `systemd/agents/`. Write to `/etc/systemd/system/forge-agent@<name>.{service,timer}`.
8. **Reload systemd.** `systemctl daemon-reload && systemctl enable --now forge-agent@<name>.timer`.
9. **Print summary.** Profile id, session id, room id, `matrix.to` URL, next scheduled run (`systemctl list-timers forge-agent@<name>.timer`).

Idempotency: the script may be re-run safely. Steps 3–6 check for existing `profile.id` / `session.id` / `room_id` in `agent.yaml` and `session.json` and skip the work if present. To force re-creation, the operator deletes the relevant field and re-runs.

Auth: the script picks up forge's API key from `/etc/forge/forge.env` and the matrix_appservice's API key from the same file (`MATRIX_AGENT_API_KEY` or, by default, the bridge's `as_token`).

### Failure modes

| Failure | What the script does |
|---|---|
| `heartbeat.md` missing | Refuses to run. Tells the operator to create one. |
| `agent.yaml` invalid | Refuses to run. Prints the parse error. |
| `/etc/forge/agents.yaml` missing | Refuses to run the first time. Prints instructions for creating it. On subsequent runs, reads it. |
| forge unreachable | Retries 3× with backoff, then aborts. No partial state. |
| matrix_appservice unreachable | Same. |
| systemd not present | Skips steps 7–8. Prints instructions for installing the units manually. |

The script never leaves the agent in a half-configured state. A failure at any step rolls back: profile/session are not created, or are created and then deleted, depending on which step failed.

---

## 4. The heartbeat service

Two files: a bash script the service runs, and the systemd units that schedule it.

### `/usr/local/bin/forge-heartbeat` (bash, ~50 lines)

```bash
#!/usr/bin/env bash
set -euo pipefail

NAME="${1:?usage: forge-heartbeat <agent-name>}"
AGENT_DIR="/etc/forge/agents/${NAME}"
SESSION_FILE="${AGENT_DIR}/session.json"

if [[ ! -f "$SESSION_FILE" ]]; then
    echo "forge-heartbeat: ${SESSION_FILE} missing; run forge-agent-setup ${NAME}" >&2
    exit 1
fi

# Load forge API URL + key from the same env file the forge-api service uses.
# shellcheck disable=SC1091
source /etc/forge/forge.env

SESSION_ID=$(jq -r .session_id "$SESSION_FILE")

# Compose the prompt: AGENTS.md (optional) + heartbeat.md (required).
PROMPT=""
if [[ -f "${AGENT_DIR}/AGENTS.md" ]]; then
    PROMPT+="$(cat "${AGENT_DIR}/AGENTS.md")"
    PROMPT+=$'\n\n---\n\n'
fi
PROMPT+="$(cat "${AGENT_DIR}/heartbeat.md")"

# POST to forge. fire-and-forget; the matrix bridge picks up the result
# via its existing SSE poller.
curl -fsS -X POST "${FORGE_API_URL}/messages" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${FORGE_API_KEY:-}" \
    -d "$(jq -n --arg sid "$SESSION_ID" --arg c "$PROMPT" \
        '{session_id: $sid, content: $c}')"
```

The service runs the script as `Type=oneshot`. The script returns immediately after the curl completes; it does not wait for the agent to finish. The agent's reply flows back through forge → SSE → matrix_appservice → matrix room, exactly the same as an interactive message.

### `systemd/agents/forge-agent@.service` (template)

```ini
[Unit]
Description=Forge agent heartbeat: %i
After=forge-api.service
Wants=forge-api.service

[Service]
Type=oneshot
User=forge
Group=forge
EnvironmentFile=/etc/forge/forge.env
ExecStart=/usr/local/bin/forge-heartbeat %i
WorkingDirectory=/etc/forge/agents/%i

# Heartbeats can run a long time. The agent may invoke cargo / npm / git
# on a big repo. Set generously; the default of 90s is far too low.
TimeoutStartSec=2h

# Don't preempt the interactive user.
Nice=10

# Log to journal. The script's stdout/stderr is already small.
StandardOutput=journal
StandardError=journal
SyslogIdentifier=forge-agent-%i
```

### `systemd/agents/forge-agent@.timer` (template, rendered per agent)

```ini
[Unit]
Description=Forge agent heartbeat timer: %i

[Timer]
# The setup script substitutes OnCalendar / Persistent from agent.yaml.
# Examples:
#   OnCalendar=*-*-* *:00/15      # every 15 minutes
#   OnCalendar=hourly             # top of every hour
#   OnCalendar=Mon..Fri 09:00:00  # weekdays at 9am
OnCalendar=__ON_CALENDAR__
Persistent=__PERSISTENT__
Unit=forge-agent@%i.service

[Install]
WantedBy=timers.target
```

`__ON_CALENDAR__` and `__PERSISTENT__` are placeholders the setup script replaces from `agent.yaml`. `RandomizedDelaySec` is added if the agent's `agent.yaml` has it.

### Example: enabling a timer

```bash
sudo forge-agent-setup foo-bot
sudo systemctl status forge-agent@foo-bot.timer
sudo systemctl list-timers forge-agent@foo-bot.timer
sudo journalctl -u forge-agent@foo-bot.service -n 50
```

---

## 5. The matrix_appservice endpoint: `POST /api/v1/agents`

This is the one piece that lives in the [matrix_appservice](https://github.com/mule-ai/matrix_appservice) repo. Everything else is in the forge repo.

### Purpose

The matrix_appservice already knows how to "DM the bot `/start <path>` and get a room" — that flow is implemented in `pkg/appservice/appservice.go::handleStartCommand` (and the room-bound sibling `handleStartInRoomCommand`). The operator's setup script needs the same behavior, but triggered by HTTP, not by a DM event.

We extract the body of `handleStartCommand` into a new exported method:

```go
// CreateAgent provisions a forge session and binds it to a Matrix
// room. Idempotent: if a session_id is provided and already maps to
// a room, that room is returned unchanged.
func (as *AppService) CreateAgent(ctx context.Context, req CreateAgentRequest) (*CreateAgentResponse, error)

type CreateAgentRequest struct {
    ProfileID  string  // forge profile id (operator passes the one setup-script minted)
    SessionID  string  // forge session id (operator passes the one setup-script minted)
    WorkingDir string  // for display in room name/topic
    UserID     string  // matrix user id to invite
    RoomName   string  // optional; defaults to "Pi: <basename(WorkingDir)>"
}

type CreateAgentResponse struct {
    SessionID    string
    RoomID       id.RoomID
    MatrixToURL  string
}
```

`handleStartCommand` and `handleStartInRoomCommand` are both rewritten to call `CreateAgent` after they have their `profile_id` / `session_id`. The DM-driven flow is unchanged; we just deduplicate the implementation.

### Route

```go
httpMux.HandleFunc("/api/v1/agents", as.handleCreateAgent)
```

Body matches `CreateAgentRequest`. Response matches `CreateAgentResponse` (JSON). Errors return `4xx`/`5xx` with a body of `{"error": "..."}`.

### Auth

The setup script sends `X-API-Key: <forge API key>` (the same key forge uses to authenticate itself with the appservice — or, if you'd rather, a new `api.admin_token` field in `config.yaml`). Recommendation: reuse forge's `X-API-Key` for now; it's an operator action, not a Matrix user action, so the matrix-issued `as_token` is the wrong secret. If you want belt-and-suspenders later, add `api.admin_token` and check both.

### Idempotency

- If `(profile_id, session_id, user_id)` is already in the persisted portal store, return the existing `room_id` and `matrix_to_url`. No new room is created.
- If the room exists but the user isn't in it, the endpoint re-sends the invite.
- If `session_id` is new but `profile_id` exists, the endpoint creates a new room and binds the new `session_id → room_id` mapping.

---

## 6. How a heartbeat run looks end-to-end

1. `forge-agent@foo-bot.timer` fires.
2. systemd starts `forge-agent@foo-bot.service`.
3. The service runs `forge-heartbeat foo-bot`.
4. `forge-heartbeat` reads `session.json` and `heartbeat.md` (and `AGENTS.md` if present), composes the prompt.
5. `forge-heartbeat` POSTs `/messages` to forge. The curl returns 202.
6. `forge-heartbeat` exits. systemd marks the service as `inactive (dead)` once the script exits (it's `Type=oneshot`).
7. Meanwhile, forge inserts a `role=user` row in the `messages` table, wakes the long-lived `pi` subprocess for the session, and starts the agent loop.
8. The agent reads the prompt, runs tools (or doesn't), produces a response.
9. Forge writes `role=assistant` and `role=tool` rows as the agent works.
10. The matrix_appservice's SSE poller (`pkg/forge/events.go`) sees the new rows and turns them into Matrix events.
11. The user in the room sees `🔧 Running bash...` notices, then the assistant's reply, then a typing indicator clearing.
12. After `OnCalendar` seconds, the timer fires again. Goto 1.

The user can also type into the room at any time. Those messages go through the same `POST /messages` path and interleave with the heartbeats. The session has a single, ordered, durable message log; the room is a renderer.

---

## 7. Trade-offs

### Why bash for the heartbeat service?

- The script is 50 lines and does one thing: read two files, post JSON.
- Adding a Rust binary is overkill. Adding a Go binary requires a separate build.
- bash + curl + jq is on every forge host already.
- The systemd unit is also bash-runnable; no new runtime dependency.

If the heartbeat ever needs to do something bash can't (signed payloads, structured retry, batched heartbeats across agents), promote it to a Rust binary in `crates/forge-heartbeat/`. Until then, bash.

### Why a separate profile per agent?

Two reasons.

1. **Cacheability.** The matrix_appservice already mints a profile per working directory. The setup script piggybacks on that. The `profile.id` is stable across re-runs, so the same agent always uses the same profile unless the operator explicitly changes it.
2. **Per-agent overrides.** "All use the same for now" is a *current* property, not a *design* property. The data model supports per-agent overrides from day one; the operator just doesn't use them yet.

### Why expose `POST /api/v1/agents` instead of letting the script use the matrix SDK directly?

The matrix_appservice owns the as_token. The setup script doesn't (and shouldn't) have it. Adding one HTTP endpoint on the appservice, with one new auth secret, is smaller than adding a new SDK consumer to the forge repo.

### Heartbeat visibility in the room — always, or filterable?

Two options.

- **A. Always visible.** Simplest. Every heartbeat turn is a `🔧 Running bash...` notice plus the assistant's reply. The user sees the agent is alive. The "checked, no changes" turn is a small price for the visibility.
- **B. Quiet mode.** The setup script tags heartbeat prompts with a marker (e.g. `[heartbeat]`). The matrix_appservice filters `[heartbeat]`-tagged *assistant* replies from the room by default, exposing them via a "history" command (`!heartbeats`). Heartbeat *user* messages still render, so the operator can see heartbeats are firing.

Recommendation: start with **A**. The schedule is yours to set per agent. A 15-minute heartbeat that says "all clear" every 15 minutes is not noisy. Add **B** if and when a particular agent's heartbeat is noisy enough to justify it. The design supports either; the matrix_appservice can grow a `quiet_heartbeats: bool` config flag without changing the forge side.

---

## 8. File changes

### In the forge repo

| Path | Status | Purpose |
|---|---|---|
| `scripts/forge-agent-setup` | NEW | The setup script (~200 lines bash) |
| `scripts/forge-heartbeat` | NEW | The heartbeat script (~50 lines bash) |
| `scripts/install.sh` | MODIFIED | Install the two scripts to `/usr/local/bin` |
| `systemd/agents/forge-agent@.service` | NEW | Heartbeat service template |
| `systemd/agents/forge-agent@.timer` | NEW | Heartbeat timer template (with `__ON_CALENDAR__` placeholder) |
| `examples/agents/foo-bot/agent.yaml` | NEW | Sample agent config |
| `examples/agents/foo-bot/heartbeat.md` | NEW | Sample heartbeat |
| `examples/agents/foo-bot/AGENTS.md` | NEW | Sample behavior supplement |
| `examples/agents.yaml` | NEW | Sample global defaults |
| `docs/SCHEDULED-AGENTS.md` | NEW | This document |
| `docs/AGENTS.md` | MODIFIED | Link to this document from the project-wide guide |
| `docs/OPERATIONS.md` | MODIFIED | Add "managing scheduled agents" subsection |
| `docs/README.md` | MODIFIED | Add this doc to the table |

### In the matrix_appservice repo

| Path | Status | Purpose |
|---|---|---|
| `pi-sessions/pkg/appservice/appservice.go` | MODIFIED | Extract `CreateAgent` from `handleStartCommand`; add `CreateAgentRequest`/`CreateAgentResponse` types |
| `pi-sessions/pkg/appservice/appservice.go` | MODIFIED | Rewrite `handleStartCommand` and `handleStartInRoomCommand` to call `CreateAgent` |
| `pi-sessions/pkg/appservice/appservice.go` | NEW handler | `handleCreateAgent` (HTTP handler) |
| `pi-sessions/cmd/pi-matrix/main.go` | MODIFIED | Register `POST /api/v1/agents` route on the existing `httpMux` |
| `pi-sessions/config.yaml.example` | MODIFIED | Document the new `forge.admin_api_key` (or note that `forge.api_key` is reused) |
| `pi-sessions/SPEC.md` | MODIFIED | Add a "Programmatic agent creation" section |
| `pi-sessions/AGENTS.md` | MODIFIED | Document the new endpoint in the API surface table |

No database schema changes in either repo. No new dependencies in either repo.

---

## 9. Open questions

These are the calls I need before writing code. Defaults are my recommendation.

1. **Heartbeat visibility: A (always) or B (quiet mode)?** Default: **A**.
2. **Auth on `POST /api/v1/agents`: forge's `X-API-Key`, or a new `api.admin_token`?** Default: **forge's `X-API-Key`**. The operator already has it; one less secret to mint.
3. **User resolution in `agent.yaml`: MXID, or display name + lookup?** Default: **MXID**. Display name lookup requires a permission grant on the homeserver.
4. **Where the matrix appservice change lives: matrix_appservice repo, or a vendored copy in forge?** Default: **matrix_appservice repo**. It's a single Go endpoint; vendoring buys nothing.
5. **Heartbeat binary: bash, or promote to a Rust binary in `crates/`?** Default: **bash**. Promote to Rust only if the script grows past ~100 lines.
6. **Should `forge-agent-setup` also create the `~/.ssh` config and any per-agent ssh keys?** Out of scope for v1. Agents inherit the forge user's environment.

---

## 10. End-to-end example

Operator wants a "build-watcher" agent that runs every 15 minutes, watches the `forge` repo, and reports CI status to `@alice`.

```bash
# 1. Create the agent dir
sudo mkdir -p /etc/forge/agents/build-watcher
sudo chown forge:forge /etc/forge/agents/build-watcher

# 2. Write the per-agent config
sudo tee /etc/forge/agents/build-watcher/agent.yaml > /dev/null <<'YAML'
name: build-watcher
profile:
  template: coder
  working_dir: /data/jbutler/git/jbutlerdev/forge
schedule:
  on_calendar: "*-*-* *:00/15"
  persistent: true
matrix:
  enabled: true
  user: "@alice:example.com"
  room_name: "forge build-watcher"
YAML

# 3. Write the heartbeat
sudo tee /etc/forge/agents/build-watcher/heartbeat.md > /dev/null <<'MD'
# Heartbeat: build-watcher

For each tick:

1. `git -C /data/jbutler/git/jbutlerdev/forge fetch --quiet`
2. `git -C /data/jbutler/git/jbutlerdev/forge status --porcelain --branch`
3. If the branch is behind, do nothing (Alice is on it).
4. If CI is red on the latest main commit, ping @alice in the room.
5. Otherwise, reply "all clear" and stop.

Do not run cargo. Do not write to the repo.
MD

# (optional) Write the behavior supplement
sudo tee /etc/forge/agents/build-watcher/AGENTS.md > /dev/null <<'MD'
# build-watcher identity

I am build-watcher. I watch the `forge` GitHub repo and report CI status.

Style: terse, one line per tick.
Escalation: if CI is red for more than 30 minutes, open a new ping.
MD
sudo chown -R forge:forge /etc/forge/agents/build-watcher

# 4. Run the setup
sudo -u forge forge-agent-setup build-watcher
# ✓ profile_id   7f3a8c12-...
# ✓ session_id   8c12a1b3-...
# ✓ room_id      !AbCdEf:example.com
# ✓ open in:     https://matrix.to/#/!AbCdEf:example.com
# ✓ timer enabled; next run: in 14m 32s

# 5. Watch it run
sudo journalctl -u forge-agent@build-watcher.service -f
systemctl list-timers forge-agent@build-watcher.timer
```

Alice clicks the `matrix.to` link, joins the room, and sees the agent's first heartbeat turn. She can also type into the room to ask follow-up questions; the agent replies in context.

When Alice wants to change the schedule, she edits `agent.yaml` and re-runs `forge-agent-setup`. The setup script rewrites the systemd unit and restarts the timer. The session, profile, and room are unchanged.
