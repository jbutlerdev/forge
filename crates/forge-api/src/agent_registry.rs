//! Agent Registry - Manages pi Processes per Session
//!
//! Each session has its own pi process for context preservation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::db::{Profile, Session};
use crate::pi_agent::{PiAgent, PiConfig};
use crate::sandbox::SandboxManager;
use sqlx::PgPool;

pub struct SharedPiAgent {
    inner: Arc<Mutex<PiAgent>>,
}

impl SharedPiAgent {
    pub fn new(agent: PiAgent) -> Self {
        Self { inner: Arc::new(Mutex::new(agent)) }
    }
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, PiAgent> {
        self.inner.lock().await
    }
}

impl Clone for SharedPiAgent {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

pub struct AgentEntry {
    pub agent: SharedPiAgent,
    pub session_id: Uuid,
    pub last_active: std::time::Instant,
}

impl AgentEntry {
    pub fn new(agent: PiAgent, session_id: Uuid) -> Self {
        Self { agent: SharedPiAgent::new(agent), session_id, last_active: std::time::Instant::now() }
    }
}

pub struct AgentRegistry {
    agents: RwLock<HashMap<Uuid, AgentEntry>>,
    forge_api_url: String,
    forge_tools_extension: PathBuf,
    /// Per-session sandbox containers. Each session gets a
    /// fresh clone (if profile.git_url is set) or copy (if
    /// profile.working_dir exists) so the agent's edits don't
    /// touch the user's real checkout. The spawned pi runs
    /// with `current_dir` pointed at the sandbox path.
    sandbox: Arc<SandboxManager>,
}

impl AgentRegistry {
    pub fn new(forge_api_url: String, sandbox: Arc<SandboxManager>) -> Self {
        // Allow the extension path to be overridden via env so the same
        // binary works in dev and production. Default to the well-known dev
        // location.
        let extension_path = std::env::var("FORGE_TOOLS_EXTENSION")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from("/data/jbutler/git/jbutlerdev/forge/extensions/forge-tools/dist/index.js")
            });
        Self {
            agents: RwLock::new(HashMap::new()),
            forge_api_url,
            forge_tools_extension: extension_path,
            sandbox,
        }
    }

    pub async fn get_or_create(&self, pool: &PgPool, session_id: Uuid) -> Result<SharedPiAgent, AgentRegistryError> {
        // Check if exists. The hot path is "user is in the same
        // session, just keep using the same pi"; in that case
        // we have nothing to do.
        {
            let agents = self.agents.read().await;
            if let Some(entry) = agents.get(&session_id) {
                let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
                    .bind(session_id)
                    .execute(pool)
                    .await;
                return Ok(entry.agent.clone());
            }
        }

        // No live pi for this session. Spawn a fresh one, and
        // (this is the whole point of the durability story)
        // replay the prior conversation from the messages table
        // into it before we hand it back to the caller. See
        // `replay_prior_conversation` below.
        let _ = sqlx::query("UPDATE sessions SET ended_at = NULL, last_active = NOW() WHERE id = $1")
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
        let working_dir = match self.sandbox.create_container(session_id, &profile).await {
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
                format!("/forge/sessions/{}", session_id)
            }
        };

        let tools: Vec<String> = serde_json::from_str(&profile.tools)
            .unwrap_or_else(|_| vec!["bash".to_string(), "read".to_string(), "write".to_string(), "edit".to_string()]);

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
                "durable resume: replayed prior tool calls to restore sandbox working tree"
            );
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
        let jsonl_path = std::path::PathBuf::from(format!(
            "/forge/sessions/{}/.parent.jsonl",
            session_id
        ));
        // Exclude the just-inserted user message (current
        // max sequence) from the jsonl.
        let max_prior_sequence: Option<i32> = sqlx::query_scalar(
            "SELECT MAX(sequence) - 1 FROM messages WHERE session_id = $1",
        )
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
            provider: profile.provider.clone(),
            model: profile.model.clone(),
            base_url: profile.base_url.clone(),
            api_key: profile.api_key.clone(),
            system_prompt: profile.system_prompt.clone(),
            forge_tools_extension: self.forge_tools_extension.clone(),
            forge_api_url: self.forge_api_url.clone(),
            session_id,
            session_path,
        };

        let mut agent = PiAgent::spawn(config)
            .await
            .map_err(|e| AgentRegistryError::AgentSpawn(e.to_string()))?;

        tracing::info!("Spawned pi agent for session {} with PID {:?}", session_id, agent.id());

        let entry = AgentEntry::new(agent, session_id);
        let shared_agent = entry.agent.clone();

        let mut agents = self.agents.write().await;
        agents.insert(session_id, entry);

        Ok(shared_agent)
    }

    pub async fn contains(&self, session_id: Uuid) -> bool {
        let agents = self.agents.read().await;
        agents.contains_key(&session_id)
    }

    pub async fn remove(&self, session_id: Uuid) -> Result<(), AgentRegistryError> {
        let mut agents = self.agents.write().await;
        if let Some(entry) = agents.remove(&session_id) {
            entry.agent.lock().await.kill().await
                .map_err(|e| AgentRegistryError::AgentKill(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn len(&self) -> usize {
        let agents = self.agents.read().await;
        agents.len()
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

#[derive(Debug, thiserror::Error)]
pub enum AgentRegistryError {
    #[error("Database error: {0}")]
    Database(String),
    #[error("Failed to spawn agent: {0}")]
    AgentSpawn(String),
    #[error("Failed to kill agent: {0}")]
    AgentKill(String),
}
