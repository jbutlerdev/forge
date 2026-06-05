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

    /// Create a working directory for a new session
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

        // Clone git repo if specified in profile
        if let Some(ref git_url) = profile.git_url {
            self.clone_repository(&session_dir, git_url, &profile.git_ref)
                .await?;
        } else {
            // If no git URL, copy the base working directory content if it exists
            let base_dir = PathBuf::from(&profile.working_dir);
            if base_dir.exists() {
                self.copy_directory(&base_dir, &session_dir).await?;
            }
        }

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

    /// Pull latest changes from git repository for a session
    ///
    /// This is called when resuming a session to ensure the agent has
    /// the latest code changes.
    pub async fn pull_git_changes(&self, session_id: Uuid) -> Result<(), SessionError> {
        let session_dir = self.get_session_dir(session_id).await?;

        // Check if this is a git repository
        let git_dir = session_dir.join(".git");
        if !git_dir.exists() {
            tracing::debug!(
                "Session {} is not a git repository, skipping pull",
                session_id
            );
            return Ok(());
        }

        // Check if there are any commits
        let output = tokio::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&session_dir)
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        if !output.status.success() {
            tracing::debug!("Session {} has no git history, skipping pull", session_id);
            return Ok(());
        }

        // Fetch and pull latest changes
        tracing::info!("Pulling latest changes for session {}", session_id);

        let output = tokio::process::Command::new("git")
            .args(["fetch", "--all"])
            .current_dir(&session_dir)
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("Git fetch failed for session {}: {}", session_id, stderr);
            // Continue anyway - fetch failures are not critical
        }

        let output = tokio::process::Command::new("git")
            .args(["pull", "--ff-only"])
            .current_dir(&session_dir)
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("Git pull failed for session {}: {}", session_id, stderr);
            // Fall back to merge or rebase
            let output = tokio::process::Command::new("git")
                .args(["pull"])
                .current_dir(&session_dir)
                .output()
                .await
                .map_err(|e| SessionError::Git(e.to_string()))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(SessionError::Git(format!("Git pull failed: {}", stderr)));
            }
        }

        tracing::info!("Successfully pulled changes for session {}", session_id);
        Ok(())
    }

    /// Get git status for a session
    ///
    /// Returns information about modified, staged, and untracked files.
    pub async fn get_git_status(&self, session_id: Uuid) -> Result<GitStatus, SessionError> {
        let session_dir = self.get_session_dir(session_id).await?;

        // Check if this is a git repository
        let git_dir = session_dir.join(".git");
        if !git_dir.exists() {
            return Err(SessionError::Git("Not a git repository".to_string()));
        }

        // Get porcelain status
        let output = tokio::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&session_dir)
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SessionError::Git(format!("Git status failed: {}", stderr)));
        }

        let status_output = String::from_utf8_lossy(&output.stdout);
        let mut modified = Vec::new();
        let mut staged = Vec::new();
        let mut untracked = Vec::new();

        for line in status_output.lines() {
            if line.len() < 3 {
                continue;
            }
            let index_status = line.chars().next().unwrap_or(' ');
            let worktree_status = line.chars().nth(1).unwrap_or(' ');
            let file = line[3..].to_string();

            // Staged changes (index)
            if index_status != ' ' && index_status != '?' {
                staged.push(file.clone());
            }
            // Unstaged changes (worktree)
            if worktree_status != ' ' && worktree_status != '?' {
                modified.push(file.clone());
            }
            // Untracked files
            if index_status == '?' && worktree_status == '?' {
                untracked.push(file);
            }
        }

        // Get current branch
        let output = tokio::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(&session_dir)
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        let branch = if output.status.success() {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        } else {
            "unknown".to_string()
        };

        // Get commits ahead/behind origin
        let (ahead, behind) = self.get_ahead_behind(&session_dir).await.unwrap_or((0, 0));

        Ok(GitStatus {
            branch,
            modified,
            staged,
            untracked,
            ahead,
            behind,
        })
    }

    /// Get number of commits ahead/behind origin
    async fn get_ahead_behind(&self, dir: &PathBuf) -> Result<(u32, u32), SessionError> {
        let output = tokio::process::Command::new("git")
            .args(["rev-list", "--left-right", "--count", "@{upstream}...HEAD"])
            .current_dir(dir)
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        if !output.status.success() {
            return Ok((0, 0)); // No upstream or not a tracking branch
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let parts: Vec<&str> = stdout.split_whitespace().collect();

        let ahead = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let behind = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        Ok((ahead, behind))
    }

    /// Clone a git repository into a directory
    async fn clone_repository(
        &self,
        target_dir: &PathBuf,
        git_url: &str,
        git_ref: &Option<String>,
    ) -> Result<(), SessionError> {
        // For github.com URLs, inject FORGE_GITHUB_TOKEN so
        // private repos the token has access to clone cleanly.
        // See `sandbox::inject_github_token` for the long
        // rationale. The streaming-bash path does the same
        // thing for `git push` via GITHUB_TOKEN in the
        // container env.
        let (effective_url, redacted_url) = crate::sandbox::inject_github_token(git_url);

        let output = tokio::process::Command::new("git")
            .args(["clone", "--depth", "1"])
            .arg(&effective_url)
            .arg(target_dir.to_str().unwrap())
            .output()
            .await
            .map_err(|e| SessionError::Git(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Some git failures include the full URL in
            // stderr; redact any token before surfacing to
            // the caller (which logs it / sends it to the
            // matrix room as a user-visible error).
            let stderr_safe = crate::sandbox::redact_token_in_message(&stderr);
            return Err(SessionError::Git(format!(
                "Clone failed for {}: {}",
                redacted_url, stderr_safe
            )));
        }

        // Checkout specific ref if provided
        if let Some(ref ref_name) = git_ref {
            let output = tokio::process::Command::new("git")
                .args(["checkout", ref_name])
                .current_dir(target_dir)
                .output()
                .await
                .map_err(|e| SessionError::Git(e.to_string()))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(SessionError::Git(format!("Checkout failed: {}", stderr)));
            }
        }

        tracing::info!("Cloned repository {} into {:?}", redacted_url, target_dir);
        Ok(())
    }

    /// Copy directory contents recursively
    async fn copy_directory(&self, src: &PathBuf, dst: &PathBuf) -> Result<(), SessionError> {
        let output = tokio::process::Command::new("cp")
            .args(["-r", "-p"])
            .arg(src)
            .arg(dst)
            .output()
            .await
            .map_err(|e| SessionError::Io(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SessionError::Io(format!("Copy failed: {}", stderr)));
        }

        Ok(())
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

    #[error("Git error: {0}")]
    Git(String),

    #[error("Session already exists: {0}")]
    SessionExists(Uuid),
}

/// Git status information for a session
#[derive(Debug, Clone, serde::Serialize)]
pub struct GitStatus {
    /// Current branch name
    pub branch: String,
    /// Modified files (unstaged)
    pub modified: Vec<String>,
    /// Staged files
    pub staged: Vec<String>,
    /// Untracked files
    pub untracked: Vec<String>,
    /// Commits ahead of upstream
    pub ahead: u32,
    /// Commits behind upstream
    pub behind: u32,
}
