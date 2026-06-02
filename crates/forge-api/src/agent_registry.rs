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
        // Check if exists
        {
            let agents = self.agents.read().await;
            if let Some(entry) = agents.get(&session_id) {
                // The pi subprocess is still alive. This is the
                // normal "resume" path: the cleanup task left the
                // process running and just marked the session as
                // ended_at, so the LLM's conversation memory is
                // intact and the next message continues the same
                // turn-of-thought the user left off in.
                //
                // Touch last_active so the cleanup task won't
                // immediately re-idle this session.
                let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
                    .bind(session_id)
                    .execute(pool)
                    .await;
                return Ok(entry.agent.clone());
            }
        }

        // No live pi for this session. Either the session has never
        // been activated, or the server restarted and lost the
        // in-memory registry, or some longer-term reaper killed
        // the pi after very extended idle. In all those cases we
        // spin up a fresh pi; if the session was previously marked
        // ended_at, clear that so the audit log reflects the
        // resumption.
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

        let config = PiConfig {
            working_dir,
            provider: profile.provider.clone(),
            model: profile.model.clone(),
            base_url: profile.base_url.clone(),
            api_key: profile.api_key.clone(),
            system_prompt: profile.system_prompt.clone(),
            forge_tools_extension: self.forge_tools_extension.clone(),
            forge_api_url: self.forge_api_url.clone(),
            session_id,
        };

        let agent = PiAgent::spawn(config)
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
