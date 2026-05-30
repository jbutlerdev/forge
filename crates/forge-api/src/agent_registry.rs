//! Agent Registry - Persistent pi Processes per Session
//!
//! Manages persistent pi agent processes for active sessions.
//! Each session maintains its own pi process for context preservation.
//!
//! ## Design
//!
//! - pi runs on the HOST (not in sandbox)
//! - Tools execute in the SANDBOX via forge-tools extension
//!
//! This decouples the LLM/harness from the tool execution environment.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::db::{Profile, Session};
use crate::pi_agent::{PiAgent, PiConfig};
use sqlx::PgPool;

/// Wrapper for PiAgent that can be shared
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
}

impl Clone for SharedPiAgent {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Entry for an active pi process
pub struct AgentEntry {
    /// The pi process wrapper
    pub agent: SharedPiAgent,
    /// Session this agent belongs to (reserved for future use)
    #[allow(dead_code)]
    pub session_id: Uuid,
    /// Profile used to configure this agent (reserved for future use)
    #[allow(dead_code)]
    pub profile_id: Uuid,
    /// Created at timestamp (reserved for future use)
    #[allow(dead_code)]
    pub created_at: std::time::Instant,
    /// Last activity timestamp
    pub last_active: std::time::Instant,
}

impl AgentEntry {
    pub fn new(agent: PiAgent, session_id: Uuid, profile_id: Uuid) -> Self {
        let now = std::time::Instant::now();
        Self {
            agent: SharedPiAgent::new(agent),
            session_id,
            profile_id,
            created_at: now,
            last_active: now,
        }
    }

    /// Update last activity timestamp (reserved for future use)
    #[allow(dead_code)]
    pub fn touch(&mut self) {
        self.last_active = std::time::Instant::now();
    }
}

/// Registry of active pi agents
pub struct AgentRegistry {
    /// Active agents by session ID
    agents: RwLock<HashMap<Uuid, AgentEntry>>,
    /// Forge API URL
    forge_api_url: String,
    /// Default forge-tools extension path
    forge_tools_extension: PathBuf,
}

impl AgentRegistry {
    /// Create a new agent registry
    pub fn new(forge_api_url: String) -> Self {
        Self {
            agents: RwLock::new(HashMap::new()),
            forge_api_url,
            forge_tools_extension: PathBuf::from("./extensions/forge-tools/dist/index.js"),
        }
    }

    /// Set the forge-tools extension path (reserved for future use)
    #[allow(dead_code)]
    pub fn with_extension(mut self, path: PathBuf) -> Self {
        self.forge_tools_extension = path;
        self
    }

    /// Get or create an agent for a session
    pub async fn get_or_create(
        &self,
        pool: &PgPool,
        session_id: Uuid,
    ) -> Result<SharedPiAgent, AgentRegistryError> {
        // Check if agent exists
        {
            let agents = self.agents.read().await;
            if let Some(entry) = agents.get(&session_id) {
                tracing::debug!("Found existing agent for session {}", session_id);
                return Ok(entry.agent.clone());
            }
        }

        // Agent doesn't exist, create it
        tracing::info!("Creating new agent for session {}", session_id);

        // Get session and profile from database
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

        // Get session working directory
        let working_dir = format!("/forge/sessions/{}", session_id);

        // Build pi config
        let tools: Vec<String> = serde_json::from_str(&profile.tools)
            .unwrap_or_else(|_| vec![
                "bash".to_string(),
                "read".to_string(),
                "write".to_string(),
                "edit".to_string(),
            ]);

        let config = PiConfig {
            working_dir,
            provider: profile.provider.clone(),
            model: profile.model.clone(),
            base_url: profile.base_url.clone(),
            api_key: profile.api_key.clone(),
            system_prompt: profile.system_prompt.clone(),
            tools,
            nix_shell: profile.nix_shell.clone(),
            forge_tools_extension: self.forge_tools_extension.clone(),
            forge_api_url: self.forge_api_url.clone(),
            session_id,
        };

        // Spawn pi process
        let agent = PiAgent::spawn(config)
            .await
            .map_err(|e| AgentRegistryError::AgentSpawn(e.to_string()))?;

        tracing::info!("Spawned pi agent for session {} with PID {:?}", session_id, agent.id());

        // Create entry
        let entry = AgentEntry::new(agent, session_id, profile.id);
        let shared_agent = entry.agent.clone();

        // Register the agent
        let mut agents = self.agents.write().await;
        agents.insert(session_id, entry);

        Ok(shared_agent)
    }

    /// Get an existing agent (does not create) - reserved for future use
    #[allow(dead_code)]
    pub async fn get(&self, session_id: Uuid) -> Option<SharedPiAgent> {
        let agents = self.agents.read().await;
        agents.get(&session_id).map(|entry| entry.agent.clone())
    }

    /// Check if agent exists
    pub async fn contains(&self, session_id: Uuid) -> bool {
        let agents = self.agents.read().await;
        agents.contains_key(&session_id)
    }

    /// Remove an agent (stops the process)
    pub async fn remove(&self, session_id: Uuid) -> Result<(), AgentRegistryError> {
        let mut agents = self.agents.write().await;
        
        if let Some(entry) = agents.remove(&session_id) {
            tracing::info!("Stopping pi agent for session {}", session_id);
            entry.agent.lock().await.kill().await
                .map_err(|e| AgentRegistryError::AgentKill(e.to_string()))?;
            tracing::info!("Removed agent for session {}", session_id);
        }

        Ok(())
    }

    /// List all active session IDs - reserved for future use
    #[allow(dead_code)]
    pub async fn active_sessions(&self) -> Vec<Uuid> {
        let agents = self.agents.read().await;
        agents.keys().cloned().collect()
    }

    /// Get the number of active agents
    pub async fn len(&self) -> usize {
        let agents = self.agents.read().await;
        agents.len()
    }

    /// Check if empty - reserved for future use
    #[allow(dead_code)]
    pub async fn is_empty(&self) -> bool {
        let agents = self.agents.read().await;
        agents.is_empty()
    }

    /// Clean up stale agents (timeout) - reserved for future use
    #[allow(dead_code)]
    pub async fn cleanup_stale(&self, timeout_secs: u64) -> usize {
        let mut agents = self.agents.write().await;
        let now = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);

        let stale: Vec<Uuid> = agents
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.last_active) > timeout)
            .map(|(id, _)| *id)
            .collect();

        let count = stale.len();
        
        for session_id in stale {
            if let Some(entry) = agents.remove(&session_id) {
                tracing::info!("Cleaning up stale agent for session {}", session_id);
                let _ = entry.agent.lock().await.kill().await;
            }
        }

        count
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new("http://localhost:8080/api/v1".to_string())
    }
}

/// Errors from agent registry operations
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum AgentRegistryError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Failed to spawn agent: {0}")]
    AgentSpawn(String),

    #[error("Failed to kill agent: {0}")]
    AgentKill(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
