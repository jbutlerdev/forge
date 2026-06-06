# Operations

Running Forge in production-ish: systemd service, database, migrations, log and metric endpoints, common failure modes, and the upgrade procedure.

## Service: `forge-api.service`

The example unit file is `systemd/forge-api.service`. It uses `User=forge` / `Group=forge`, which assumes a `forge` user exists on the host. On a single-tenant dev box, you can simplify it: the in-tree file is the right starting point, and the file we actually run on our dev box is `/etc/systemd/system/forge-api.service` with `User=root` (because there is no `forge` user on this host).

```ini
[Unit]
Description=Forge API Server
Documentation=https://github.com/jbutlerdev/forge
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/forge
EnvironmentFile=/etc/forge/forge.env
ExecStart=/opt/forge/forge-api
Restart=on-failure
RestartSec=5

StandardOutput=journal
StandardError=journal
SyslogIdentifier=forge-api

LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
```

### Install / enable / start

```bash
sudo cp target/release/forge-api /opt/forge/forge-api
sudo cp systemd/forge-api.service /etc/systemd/system/forge-api.service
sudo cp systemd/forge.env.example /etc/forge/forge.env   # edit it first
sudo systemctl daemon-reload
sudo systemctl enable --now forge-api
sudo systemctl status forge-api
```

### Common operations

```bash
sudo systemctl status forge-api          # is it running?
sudo systemctl restart forge-api         # after a code change
sudo systemctl stop forge-api            # before manual DB work
sudo journalctl -u forge-api -f          # follow logs
sudo journalctl -u forge-api -n 200      # last 200 lines
sudo journalctl -u forge-api --since "10 min ago"
```

The harness, executor, and per-tool logging all funnel through `tracing`. The default env in `/etc/forge/forge.env` is `RUST_LOG=forge_api=debug,info` — verbose enough to see every tool call and result without drowning in sqlx chatter. For deeper debugging, `RUST_LOG=forge_api=trace,sqlx=warn` traces the harness event loop.

### Service hardening

The in-tree unit file includes `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`, and `ProtectHome=true`. These require a host that supports the relevant namespaces (recent systemd on a normal distro). On minimal dev containers some of these may not be available — remove them and the service still works, just without the hardening. The in-repo example assumes you have a hardened host; the file we actually run is the slimmed-down version above.

## Database

PostgreSQL 15+. The connection string is in `/etc/forge/forge.env` as `DATABASE_URL`. The `forge` database is created out-of-band; the schema is created by `sqlx::migrate!` at startup.

### Provisioning

```bash
sudo -u postgres psql <<'SQL'
CREATE DATABASE forge;
\c forge
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- for gen_random_uuid()
SQL
```

If you're running as a non-superuser, the role needs `CREATE` on the `forge` database and `CREATE EXTENSION` permission for `pgcrypto`. The `postgres` superuser has both by default.

### Migrations

Migrations live in `crates/forge-api/migrations/`. The macro `sqlx::migrate!("./migrations")` in `main.rs` resolves this path relative to `CARGO_MANIFEST_DIR` (i.e. the crate's own directory), **not** the workspace root.

**A migration is a file `NNN_description.sql` in that directory.** The leading zero-padded number is the version. The convention is one logical change per file.

The `sqlx::migrate!` macro reads the directory at **compile time** and embeds the SQL into the binary. To add a migration:

```bash
$EDITOR crates/forge-api/migrations/005_my_change.sql
cargo build --release -p forge-api
sudo systemctl restart forge-api
```

On startup, the service compares the embedded migration set with the rows in `_sqlx_migrations` and applies any missing ones, each in its own transaction. The table records `(version, description, installed_on, success, checksum, execution_time)`.

Verify:

```bash
sudo -u postgres psql forge -c "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"
```

### Why a per-session advisory lock

`get_next_sequence(session_id)` was originally `SELECT COALESCE(MAX(sequence), 0) + 1 FROM messages WHERE session_id = $1`. Under concurrent writes — which is exactly what the harness and executor do in the ToolRecorder split — that race produced `duplicate key value violates unique constraint "messages_session_id_sequence_key"`.

The fix in `migrations/004_get_next_sequence_locking.sql` wraps the MAX query in `pg_advisory_xact_lock(1, hashtext(session_uuid::text))`. The lock is transaction-scoped (auto-released on COMMIT/ROLLBACK) and keyed by the session id, so concurrent allocations on different sessions don't block each other.

**Don't rewrite `get_next_sequence` to a simpler form without keeping the lock.** If you need a different locking strategy (e.g. a per-session counter row updated via `UPDATE … RETURNING`), do that and add a new migration. Don't drop the lock.

### Manual data inspection

```sql
-- Pair every assistant tool-call row with its tool result
SELECT
  c.sequence AS call_seq,
  c.tool_name,
  c.tool_call_id,
  c.tool_input,
  r.sequence AS result_seq,
  r.duration_ms,
  r.tool_output,
  r.tool_output->>'success' AS success
FROM messages c
LEFT JOIN messages r
  ON r.session_id = c.session_id
  AND r.role = 'tool'
  AND r.tool_call_id = c.tool_call_id
WHERE c.session_id = '<session-uuid>'
  AND c.role = 'assistant'
  AND c.tool_call_id IS NOT NULL
ORDER BY c.sequence;
```

```sql
-- Per-tool result stats over the last 24h
SELECT
  tool_name,
  COUNT(*) AS n,
  ROUND(AVG(duration_ms), 1) AS avg_ms,
  SUM(CASE WHEN tool_output->>'success' = 'false' THEN 1 ELSE 0 END) AS errors
FROM messages
WHERE role = 'tool'
  AND created_at > NOW() - INTERVAL '1 day'
GROUP BY tool_name
ORDER BY n DESC;
```

## Configuration reference

`/etc/forge/forge.env`:

```ini
DATABASE_URL=postgres://postgres@localhost/forge
FORGE_API_URL=http://localhost:8080
RUST_LOG=forge_api=debug,info
# PATH must include the directory containing the `pi` binary
PATH=/root/.nvm/versions/node/v20.18.1/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
```

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | yes | — | PostgreSQL connection string (sqlx) |
| `FORGE_API_URL` | no | `http://localhost:8080` | base URL the extension uses to call back |
| `FORGE_TOOLS_EXTENSION` | no | fallback path | absolute path to the built `forge-tools/dist/index.js` |
| `RUST_LOG` | no | `info` | tracing filter |
| `PATH` | yes (in env) | — | must include the directory containing the `pi` binary |
| `ANTHROPIC_API_KEY` | conditional | — | only required by profiles whose `provider` is `anthropic` |
| `OPENAI_API_KEY` | conditional | — | only required by profiles whose `provider` is `openai` |

With `provider=proxy-anthropic` the API key is stored on the profile, not in the global env.

## Log and metric endpoints

| URL | What |
|---|---|
| `GET /health` | `200 OK` body `OK` |
| `GET /metrics` | JSON metrics — see below |
| `GET /metrics/prometheus` | Prometheus text exposition |

`/metrics` (JSON shape):

```json
{
  "metrics": {
    "requests_total": 1042,
    "errors_total": 7,
    "active_sessions": 3,
    "active_agents": 2,
    "tools_executed": {
      "bash": 412,
      "read": 188,
      "write": 12,
      "edit": 5
    }
  }
}
```

## Common failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `forge-api.service: Main process exited, code=exited, status=2/INVALIDARGUMENT` | env file missing or unreadable | `ls -la /etc/forge/forge.env`; `sudo systemctl show forge-api -p EnvironmentFiles` |
| `Failed to connect to the database` | `DATABASE_URL` wrong or Postgres down | `sudo systemctl status postgresql`; verify `psql $DATABASE_URL` works as the service user |
| `pi timed out waiting for response after 60s` | pi is hung or crashed | `ps -ef \| grep pi`; check the journal for the most recent session; consider raising the 60s per-event timeout in `pi_agent.rs` |
| Tool calls return `[error] Forge tools extension not found at <path>` | `FORGE_TOOLS_EXTENSION` wrong or the extension wasn't built | `cd extensions/forge-tools && npm run build`; verify `dist/index.js` exists |
| `duplicate key value violates unique constraint "messages_session_id_sequence_key"` | the `get_next_sequence` advisory lock is missing | check the function body matches `migrations/004_get_next_sequence_locking.sql`; rebuild and restart |
| The CLI returns `Failed to deserialize the JSON body into the target type` | the `auth_header` quoting bug in `cli/forge.d/common.sh` (unquoted `$auth_header`) | verify the file uses the bash-array pattern: `local -a auth_args=(-H "X-API-Key: $FORGE_API_KEY")` and `"${auth_args[@]}"` |
| `/tools/execute/stream` SSE never produces a `tool_end` | the streaming bash crashed before the child could exit; check the journal for stack traces | the executor logs `Failed to persist streaming bash result` if the recorder failed; check for DB errors |
| `bash` returns exit code 0 but the agent never sees the output | the streaming-bash-stdout-capture bug: bytes go to the SSE consumer, not into the audit log | the model should `bash … > /tmp/out.txt && read /tmp/out.txt`; permanent fix is in the roadmap |

## Upgrade procedure

```bash
# 1. Pull the new code
cd /data/jbutler/git/jbutlerdev/forge
git pull

# 2. Build
cargo build --release -p forge-api

# 3. (If there are new pi version requirements) rebuild the extension
cd extensions/forge-tools
npm install
npm run build
cd ../..

# 4. (If there are new migrations) check the diff first
git log --stat HEAD~5 -- crates/forge-api/migrations/

# 5. Restart
sudo systemctl restart forge-api
sudo journalctl -u forge-api -f

# 6. Verify migrations applied
sudo -u postgres psql forge -c "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"

# 7. Smoke test
curl -s http://localhost:8080/health
# -> OK
```

If a migration fails partway through, the service logs the error and exits non-zero. The failed migration is recorded with `success = false` in `_sqlx_migrations`. Fix the migration, delete the failed row (`DELETE FROM _sqlx_migrations WHERE version = N`), and restart. The migration will re-run on startup.

## Managing scheduled agents

A scheduled agent is a long-lived forge session driven by a systemd timer that POSTs a heartbeat prompt on a fixed schedule. The full design is in [`SCHEDULED-AGENTS.md`](SCHEDULED-AGENTS.md); this section is the on-host operations cheatsheet.

### On-disk layout

```
/etc/forge/
├── forge.env                  # secrets/env (FORGE_API_URL, FORGE_API_KEY, MATRIX_*)
├── agents.yaml                # global profile templates + matrix defaults
└── agents/<name>/
    ├── agent.yaml             # name, profile reference + overrides, schedule
    ├── heartbeat.md           # the prompt posted on each tick
    ├── AGENTS.md              # (optional) identity / style supplement
    └── session.json           # generated: profile_id, session_id, room_id
```

The matching systemd unit lives at `/etc/systemd/system/forge-agent@<name>.{service,timer}`.

### Provisioning a new agent

```bash
# 1. Create the agent dir + config
sudo mkdir -p /etc/forge/agents/foo-bot
sudo cp examples/agents/foo-bot/* /etc/forge/agents/foo-bot/
sudo chown -R forge:forge /etc/forge/agents/foo-bot
$EDITOR /etc/forge/agents/foo-bot/agent.yaml   # pick schedule, matrix user, ...

# 2. Run the setup. Idempotent: re-runs reuse cached profile/session ids.
sudo forge-agent-setup foo-bot
# ✓ profile 7f3a8c12-...
# ✓ session 8c12a1b3-...
# ✓ room    !AbCdEf:example.com
# ✓ open in https://matrix.to/#/!AbCdEf:example.com
# ✓ timer enabled; next run: in 14m 32s
```

### Day-to-day operations

```bash
# What timers do I have?
systemctl list-timers 'forge-agent@*'

# Is this one scheduled? When does it next fire?
systemctl status forge-agent@foo-bot.timer
systemctl list-timers forge-agent@foo-bot.timer

# Why did the last tick fail?
sudo journalctl -u forge-agent@foo-bot.service -n 200 --no-pager

# Force a run right now (without waiting for the timer)
sudo systemctl start forge-agent@foo-bot.service
sudo journalctl -u forge-agent@foo-bot.service -f

# Pause the schedule
sudo systemctl disable --now forge-agent@foo-bot.timer

# Resume
sudo systemctl enable --now forge-agent@foo-bot.timer

# Tear down (keeps the agent dir on disk; you can re-run setup later)
sudo systemctl disable --now forge-agent@foo-bot.timer
sudo rm /etc/systemd/system/forge-agent@foo-bot.{service,timer}
sudo systemctl daemon-reload
```

The `forge` CLI wraps these:

```bash
forge agent list
forge agent status foo-bot
forge agent logs   foo-bot
forge agent setup  foo-bot
```

### Rotating a heartbeat prompt

Edit `/etc/forge/agents/<name>/heartbeat.md` (and `AGENTS.md` if relevant). The next timer tick picks up the new prompt; no restart required. The new content is read by `forge-heartbeat` on every run.

### Recreating a Matrix room

If the room gets nuked and the agent is still bound to a stale `room_id` in `session.json`, delete the `room_id` and `matrix_to_url` keys from `/etc/forge/agents/<name>/session.json` and re-run `forge-agent-setup <name>`. The setup script will mint a new room via `POST /api/v1/agents` and re-send the invite.

### Common failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `forge-heartbeat: /etc/forge/agents/<n>/session.json missing` | setup never ran, or session.json was deleted | `sudo forge-agent-setup <n>` |
| `forge-heartbeat: session_id missing from session.json` | the JSON is corrupt or hand-edited | re-run `sudo forge-agent-setup <n>` |
| Timer fires but service exits non-zero | check `journalctl -u forge-agent@<n>.service` — most often a missing `FORGE_API_KEY` in `/etc/forge/forge.env` or the API is down | `curl -fsS "${FORGE_API_URL}/health"` should return `OK` |
| `forge-agent-setup: yq is required` | the host doesn't have `yq` | `sudo bash scripts/install.sh` (installs yq) or `sudo curl -fsSL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/local/bin/yq && sudo chmod +x /usr/local/bin/yq` |
| `agent.yaml is missing required field: name` | typo / wrong file | the directory name and `name:` must match |
| `POST /api/v1/agents` returns 4xx | the matrix_appservice isn't running, or `MATRIX_AGENT_API_KEY` in `forge.env` doesn't match what the appservice expects | check the appservice journal; the script falls back to using `FORGE_API_KEY` if `MATRIX_AGENT_API_KEY` is unset |

## Backups

The only stateful component is PostgreSQL. The `/forge/sessions/<id>/` directories are on-disk state, but they can be regenerated from the message log (the agent can re-run its tools).

```bash
# Postgres
sudo -u postgres pg_dump forge > /var/backups/forge-$(date +%F).sql

# Sessions (optional)
tar -czf /var/backups/forge-sessions-$(date +%F).tar.gz -C /forge sessions
```

Restore:

```bash
sudo -u postgres psql forge < /var/backups/forge-2026-06-01.sql
```
