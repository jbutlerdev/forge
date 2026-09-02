//! Session Manager
//!
//! Manages active sessions and their isolated working directories.
//! Provides concurrency-safe access to session resources.
//!
//! ## Isolation Strategy
//!
//! Each session gets:
//! - Unique working directory: `/forge/sessions/:session_id/`
//! - Isolated tool execution context
//! - Persistent pi process (in Phase 2)
//!
//! ## Thread Safety
//!
//! Uses RwLock for concurrent read access and exclusive write access.
//! Sessions can be accessed concurrently, but modifications are serialized.

use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::Profile;

/// Base directory for all session data
const SESSIONS_BASE_DIR: &str = "/forge/sessions";

/// Manages session state and working directories
pub struct SessionManager {
    /// Base path for all sessions
    base_path: PathBuf,
    /// Active sessions with their working directories
    sessions: RwLock<HashMap<Uuid, SessionState>>,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new() -> Self {
        // Try to create base directory on startup
        let base_path = PathBuf::from(SESSIONS_BASE_DIR);
        if let Err(e) = std::fs::create_dir_all(&base_path) {
            tracing::warn!(
                "Failed to create sessions base directory {:?}: {}",
                base_path,
                e
            );
        }
        Self {
            base_path,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new session manager rooted at `base_path`. The
    /// `new()` constructor hardcodes `/forge/sessions`; this
    /// entry point exists for tests (and any future non-default
    /// deployment) that need a guaranteed per-process or
    /// per-test directory. CI runners also don't have write
    /// access to `/forge/`, so this is the only way the
    /// integration / e2e suites run there at all.
    pub fn with_base_path(base_path: PathBuf) -> Self {
        if let Err(e) = std::fs::create_dir_all(&base_path) {
            tracing::warn!(
                "Failed to create sessions base directory {:?}: {}",
                base_path,
                e
            );
        }
        Self {
            base_path,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Initialize the session manager (call this on startup)
    pub async fn init(&self) -> Result<(), SessionError> {
        // Ensure base directory exists
        tokio::fs::create_dir_all(&self.base_path)
            .await
            .map_err(|e| SessionError::Io(format!("Failed to create sessions directory: {}", e)))?;
        tracing::info!("Session manager initialized at {:?}", self.base_path);
        Ok(())
    }

    /// Create the working directory for a new session.
    ///
    /// This only creates the (empty) directory and registers the
    /// session in the in-memory map. The actual repo clone / working
    /// dir copy happens lazily in
    /// [`crate::sandbox::SandboxManager::create_container`] on the
    /// session's first message. Previously we cloned here AND
    /// re-cloned in `create_container` (which wipes and repopulates
    /// the same `/forge/sessions/<id>` dir) — twice per new session,
    /// doubling first-message latency for large repos and turning the
    /// correct first clone into the nested-copy bug on the second
    /// pass.
    pub async fn create_session_dir(
        &self,
        session_id: Uuid,
        profile: &Profile,
    ) -> Result<PathBuf, SessionError> {
        let session_dir = self.base_path.join(session_id.to_string());

        // Create session directory
        tokio::fs::create_dir_all(&session_dir)
            .await
            .map_err(|e| SessionError::Io(e.to_string()))?;

        // Register session
        let state = SessionState {
            working_dir: session_dir.clone(),
            profile_id: profile.id,
            active: true,
        };

        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id, state);

        tracing::info!(
            "Created session directory: {:?} for session {}",
            session_dir,
            session_id
        );

        Ok(session_dir)
    }

    /// Get the working directory for a session
    pub async fn get_session_dir(&self, session_id: Uuid) -> Result<PathBuf, SessionError> {
        let sessions = self.sessions.read().await;

        match sessions.get(&session_id) {
            Some(state) => Ok(state.working_dir.clone()),
            None => Err(SessionError::SessionNotFound(session_id)),
        }
    }

    /// Get session state (reserved for future use)
    #[allow(dead_code)]
    pub async fn get_session_state(&self, session_id: Uuid) -> Result<SessionState, SessionError> {
        let sessions = self.sessions.read().await;

        match sessions.get(&session_id) {
            Some(state) => Ok(state.clone()),
            None => Err(SessionError::SessionNotFound(session_id)),
        }
    }

    /// Check if session exists
    pub async fn session_exists(&self, session_id: Uuid) -> bool {
        let sessions = self.sessions.read().await;
        sessions.contains_key(&session_id)
    }

    /// Mark session as ended (reserved for future use)
    #[allow(dead_code)]
    pub async fn end_session(&self, session_id: Uuid) -> Result<(), SessionError> {
        let mut sessions = self.sessions.write().await;

        match sessions.get_mut(&session_id) {
            Some(state) => {
                state.active = false;
                tracing::info!("Session {} marked as ended", session_id);
                Ok(())
            }
            None => Err(SessionError::SessionNotFound(session_id)),
        }
    }

    /// Remove session (cleanup)
    pub async fn remove_session(&self, session_id: Uuid) -> Result<(), SessionError> {
        let mut sessions = self.sessions.write().await;

        match sessions.remove(&session_id) {
            Some(_state) => {
                // Note: We don't delete the directory here - let cleanup handle it
                tracing::info!("Removed session {} from manager", session_id);
                Ok(())
            }
            None => Err(SessionError::SessionNotFound(session_id)),
        }
    }

    /// Register a session that was already created (e.g. on a previous
    /// API run) so the in-memory cache is populated. Used by routes
    /// that look the session up from the database after a restart.
    pub async fn register_existing_session(
        &self,
        session_id: Uuid,
        profile_id: Uuid,
        working_dir: PathBuf,
    ) {
        let mut sessions = self.sessions.write().await;
        sessions.entry(session_id).or_insert_with(|| SessionState {
            working_dir,
            profile_id,
            active: true,
        });
    }

    /// List all active sessions (reserved for future use)
    #[allow(dead_code)]
    pub async fn list_active_sessions(&self) -> Vec<Uuid> {
        let sessions = self.sessions.read().await;
        sessions
            .iter()
            .filter(|(_, state)| state.active)
            .map(|(id, _)| *id)
            .collect()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// State for an active session
#[derive(Debug, Clone)]
pub struct SessionState {
    /// Session's working directory
    pub working_dir: PathBuf,
    /// Profile this session is based on (reserved for future use)
    #[allow(dead_code)]
    pub profile_id: Uuid,
    /// Whether the session is active (reserved for future use)
    #[allow(dead_code)]
    pub active: bool,
}

/// Errors from session management
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum SessionError {
    #[error("Session not found: {0}")]
    SessionNotFound(Uuid),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Session already exists: {0}")]
    SessionExists(Uuid),
}
