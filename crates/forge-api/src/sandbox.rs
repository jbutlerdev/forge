//! Sandbox Manager - nspawn Container Lifecycle
//!
//! Manages systemd-nspawn containers for session isolation.
//! Each session gets its own container with isolated filesystem, network, and processes.

use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::Profile;

/// Base directory for container root filesystems
const SANDBOX_BASE_DIR: &str = "/forge/sandbox";
/// Base directory for session working directories (bind-mounted into containers)
const SESSION_BASE_DIR: &str = "/forge/sessions";

/// Sandbox state
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum SandboxState {
    /// Container is being created
    Creating,
    /// Container is running
    Running,
    /// Container is paused
    Paused,
    /// Container is stopped
    Stopped,
    /// Container failed to start
    Failed,
}

/// Sandbox container info
#[derive(Debug, Clone)]
pub struct SandboxContainer {
    /// Unique container name
    pub name: String,
    /// Session this container belongs to
    pub session_id: Uuid,
    /// Current state
    pub state: SandboxState,
    /// Working directory (on host, bind-mounted to container)
    pub working_dir: PathBuf,
    /// Container root (for cleanup)
    pub root_dir: PathBuf,
    /// PID of the running container (if running)
    pub pid: Option<u32>,
}

/// Sandbox manager for container lifecycle
pub struct SandboxManager {
    /// Base directory for sandboxes
    base_dir: PathBuf,
    /// Active containers
    containers: RwLock<std::collections::HashMap<Uuid, SandboxContainer>>,
}

impl SandboxManager {
    /// Create a new sandbox manager
    pub fn new() -> Self {
        Self {
            base_dir: PathBuf::from(SANDBOX_BASE_DIR),
            containers: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Initialize the sandbox directory structure
    pub async fn init(&self) -> std::result::Result<(), SandboxError> {
        // Create base directory
        tokio::fs::create_dir_all(&self.base_dir).await
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        
        tokio::fs::create_dir_all(SESSION_BASE_DIR).await
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        tracing::info!("Sandbox manager initialized at {:?}", self.base_dir);
        Ok(())
    }

    /// Create a new container for a session
    pub async fn create_container(
        &self,
        session_id: Uuid,
        profile: &Profile,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        let container_name = format!("forge-{}", session_id);
        let root_dir = self.base_dir.join(&container_name);
        let working_dir = PathBuf::from(SESSION_BASE_DIR).join(session_id.to_string());

        tracing::info!("Creating container {} for session {}", container_name, session_id);

        // Create session working directory
        tokio::fs::create_dir_all(&working_dir).await
            .map_err(|e| SandboxError::Io(format!("Failed to create working dir: {}", e)))?;

        // Clone git repo if specified
        if let Some(ref git_url) = profile.git_url {
            self.clone_repository(&working_dir, git_url, &profile.git_ref).await?;
        } else {
            // Copy base working directory if exists
            let base_dir = PathBuf::from(&profile.working_dir);
            if base_dir.exists() {
                self.copy_directory(&base_dir, &working_dir).await?;
            }
        }

        // Create container root (minimal Debian-like structure for now)
        self.create_container_root(&root_dir).await?;

        // Register container
        let container = SandboxContainer {
            name: container_name,
            session_id,
            state: SandboxState::Creating,
            working_dir,
            root_dir: root_dir.clone(),
            pid: None,
        };

        let mut containers = self.containers.write().await;
        containers.insert(session_id, container.clone());

        Ok(container)
    }

    /// Start a container (reserved for future use with full nspawn support)
    #[allow(dead_code)]
    pub async fn start_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        let mut containers = self.containers.write().await;
        let container = containers.get_mut(&session_id)
            .ok_or(SandboxError::NotFound(session_id))?;

        let root_dir = &container.root_dir;
        let working_dir = &container.working_dir;

        tracing::info!("Starting container {} with nspawn", container.name);

        // Start nspawn container
        // -b: boot (use init)
//        --bind=/forge/sessions/{}:/workspace
        let mut cmd = Command::new("systemd-nspawn");
        cmd
            .arg("-D").arg(root_dir)
            .arg("-M").arg(&container.name)
            .arg("--chdir").arg("/workspace")
            .arg("--bind").arg(format!("{}:/workspace", working_dir.display()))
            .arg("--private-users=pick")
            .arg("--network-veth")
            .arg("-b") // boot
            .arg("--console=pipe")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd.spawn()
            .map_err(|e| SandboxError::SpawnFailed(e.to_string()))?;

        let pid = child.id();
        container.pid = pid;
        container.state = SandboxState::Running;

        tracing::info!("Container {} started with PID {:?}", container.name, pid);

        Ok(container.clone())
    }

    /// Stop a container
    pub async fn stop_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<(), SandboxError> {
        let mut containers = self.containers.write().await;
        let container = containers.get_mut(&session_id)
            .ok_or(SandboxError::NotFound(session_id))?;

        tracing::info!("Stopping container {}", container.name);

        // Try graceful shutdown first
        let _ = Command::new("machinectl")
            .arg("terminate")
            .arg(&container.name)
            .output()
            .await;

        container.state = SandboxState::Stopped;
        container.pid = None;

        Ok(())
    }

    /// Get container state
    pub async fn get_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        let containers = self.containers.read().await;
        containers.get(&session_id)
            .cloned()
            .ok_or(SandboxError::NotFound(session_id))
    }

    /// Remove a container and its resources
    pub async fn destroy_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<(), SandboxError> {
        // Stop if running
        let _ = self.stop_container(session_id).await;

        let mut containers = self.containers.write().await;
        let container = containers.remove(&session_id)
            .ok_or(SandboxError::NotFound(session_id))?;

        // Remove container root
        if container.root_dir.exists() {
            tokio::fs::remove_dir_all(&container.root_dir).await
                .map_err(|e| SandboxError::Io(format!("Failed to remove root dir: {}", e)))?;
        }

        tracing::info!("Destroyed container {}", container.name);
        Ok(())
    }

    /// List all active containers
    pub async fn list_containers(&self) -> Vec<SandboxContainer> {
        let containers = self.containers.read().await;
        containers.values().cloned().collect()
    }

    /// Create a minimal container root filesystem
    async fn create_container_root(
        &self,
        root_dir: &PathBuf,
    ) -> std::result::Result<(), SandboxError> {
        // Create directory structure
        let dirs = [
            "bin", "etc", "etc/apt", "var", "var/lib", "var/lib/dpkg",
            "var/log", "tmp", "root", "home", "usr", "usr/bin", "usr/lib",
            "workspace", "proc", "sys", "dev", "run", "run/nspawn",
        ];

        for dir in &dirs {
            let path = root_dir.join(dir);
            tokio::fs::create_dir_all(&path).await
                .map_err(|e| SandboxError::Io(format!("Failed to create {}: {}", path.display(), e)))?;
        }

        // Create minimal /bin/sh
        let sh_path = root_dir.join("bin/sh");
        if !sh_path.exists() {
            // Try to copy from host or create placeholder
            if std::path::Path::new("/bin/sh").exists() {
                tokio::fs::copy("/bin/sh", &sh_path).await
                    .map_err(|e| SandboxError::Io(e.to_string()))?;
            }
        }

        // Create minimal /etc/passwd
        let passwd = root_dir.join("etc/passwd");
        tokio::fs::write(&passwd, "root:x:0:0:root:/root:/bin/sh\n").await
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        // Create minimal /etc/group
        let group = root_dir.join("etc/group");
        tokio::fs::write(&group, "root:x:0:\n").await
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        tracing::debug!("Created container root at {:?}", root_dir);
        Ok(())
    }

    /// Clone a git repository
    async fn clone_repository(
        &self,
        target_dir: &PathBuf,
        git_url: &str,
        git_ref: &Option<String>,
    ) -> std::result::Result<(), SandboxError> {
        let mut cmd = Command::new("git");
        cmd.arg("clone").arg("--depth").arg("1");

        if let Some(ref ref_name) = git_ref {
            cmd.arg("--branch").arg(ref_name);
        }

        cmd.arg(git_url).arg(target_dir);

        let output = cmd.output().await
            .map_err(|e| SandboxError::Git(format!("Clone failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SandboxError::Git(format!("Clone failed: {}", stderr)));
        }

        tracing::info!("Cloned repository {} into {:?}", git_url, target_dir);
        Ok(())
    }

    /// Copy directory contents
    async fn copy_directory(
        &self,
        src: &PathBuf,
        dst: &PathBuf,
    ) -> std::result::Result<(), SandboxError> {
        let output = Command::new("cp")
            .args(["-r", "-p"])
            .arg(src)
            .arg(dst)
            .output()
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SandboxError::Io(format!("Copy failed: {}", stderr)));
        }

        Ok(())
    }
}

impl Default for SandboxManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Sandbox errors
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum SandboxError {
    #[error("Container not found: {0}")]
    NotFound(Uuid),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Git error: {0}")]
    Git(String),

    #[error("Failed to spawn container: {0}")]
    SpawnFailed(String),

    #[error("Container is in invalid state: {0}")]
    InvalidState(String),
}

/// Execute a command in a running container (reserved for future use)
#[allow(dead_code)]
pub async fn execute_in_container(
    _container_name: &str,
    command: &[&str],
    working_dir: &PathBuf,
) -> std::result::Result<std::process::Output, SandboxError> {
    let mut cmd = Command::new("nsenter");
    cmd.arg("--target").arg("1"); // Enter container's PID 1 namespace
    cmd.arg("--mount");
    cmd.arg("--pid");
    cmd.arg("--");
    
    for arg in command {
        cmd.arg(arg);
    }

    cmd.current_dir(working_dir);

    let output = cmd.output().await
        .map_err(|e| SandboxError::Io(e.to_string()))?;

    Ok(output)
}
