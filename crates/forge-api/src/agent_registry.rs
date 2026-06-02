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
}

impl AgentRegistry {
    pub fn new(forge_api_url: String) -> Self {
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
        }
    }

    pub async fn get_or_create(&self, pool: &PgPool, session_id: Uuid) -> Result<SharedPiAgent, AgentRegistryError> {
        // Check if exists
        {
            let agents = self.agents.read().await;
            if let Some(entry) = agents.get(&session_id) {
                return Ok(entry.agent.clone());
            }
        }

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

        let working_dir = format!("/forge/sessions/{}", session_id);
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
        Self::new("http://localhost:8080".to_string())
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
