//! Sandbox Manager - nspawn Container Lifecycle
//!
//! Each session gets its own real Debian bookworm rootfs
//! (bootstrapped from a shared `/forge/sandbox/base/`) under
//! `/forge/sandbox/forge-<uuid>/`, and its own working tree
//! under `/forge/sessions/<uuid>/`.
//!
//! The container is *not* started as a long-running machine.
//! Instead, every `bash` tool call is wrapped in a per-call
//! `systemd-nspawn` invocation that creates the namespace on
//! the fly, runs the command, and tears the namespace down
//! when the command exits. This is simpler than keeping a
//! long-running container alive (no init process, no
//! `machinectl` dance, no orphan cleanup) and the per-call
//! overhead (~50ms for the nspawn spawn) is negligible next
//! to typical bash work.
//!
//! ## What is and isn't isolated
//!
//! | Resource | Status | Notes |
//! |---|---|---|
//! | Filesystem (rootfs) | Isolated | per-session rootfs, `apt install` only affects this session |
//! | Filesystem (working dir) | Shared with host | bind-mounted at the same path so `/forge/sessions/<uuid>/foo.txt` resolves the same on host and in container |
//! | Process namespace | Isolated | commands run as PID 2 inside the container; the host `pgrep` doesn't see them |
//! | Mount namespace | Isolated | host mounts aren't visible; the bind-mount is explicit |
//! | Network namespace | **NOT isolated** | host network (`--network=host` semantics — no `--network-veth`); the LLM can reach the network from inside the container |
//! | User namespace | NOT isolated | runs as root both inside and outside |
//!
//! The "no network isolation" choice was deliberate: this is
//! a coding-agent sandbox, not a hostile-multitenancy sandbox.
//! Agents need to `git push`, hit package registries, talk to
//! model APIs, etc. The process + filesystem isolation is
//! what prevents `rm -rf /` from taking down the host, and
//! what gives each session a clean Debian it can mutate.
//!
//! ## First-time bootstrap
//!
//! The shared base rootfs at `/forge/sandbox/base/` is
//! debootstrapped on the first session creation. This is
//! a one-time 30-60s `debootstrap` call that downloads
//! Debian bookworm minbase. Every subsequent session is a
//! fast `cp -a` from base to its own rootfs.
//!
//! ## read/write/edit
//!
//! `read`/`write`/`edit` are host-side Rust file ops. They
//! hit the bind-mounted working dir via the same host path
//! the LLM used, so they see the same content the container
//! would. This is acceptable because the LLM is constrained
//! by `AGENT_GUARD` to only touch files under its working
//! dir. If we ever need stricter isolation for file ops,
//! we'd route them through `nspawn` too.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::Profile;

/// Base directory for container root filesystems
const SANDBOX_BASE_DIR: &str = "/forge/sandbox";
/// Shared base rootfs. Debootstrapped on first session creation,
/// then `cp -a`'d per session to make per-session rootfs trees.
/// Keeping one shared base saves ~150MB per session.
const SANDBOX_BASE_ROOTFS: &str = "/forge/sandbox/base";
/// Base directory for session working directories (bind-mounted into containers)
const SESSION_BASE_DIR: &str = "/forge/sessions";
/// Debian suite to bootstrap (bookworm = Debian 12)
const DEBOOTSTRAP_SUITE: &str = "bookworm";
/// Debian mirror
const DEBOOTSTRAP_MIRROR: &str = "http://deb.debian.org/debian";

/// Result of running one command inside a session's container.
/// Mirrors the structured outcome the bash tool records, so the
/// executor can forward it to `record_outcome` without an extra
/// round-trip through string parsing.
#[derive(Debug, Clone)]
pub struct ContainerRunOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

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

        // Create session working directory. The session manager
        // pre-creates this dir on session creation, so it may
        // already exist with a `.gitkeep` or similar placeholder.
        // We need a clean target for `git clone` / `cp -r` so
        // wipe and recreate the dir if we're about to populate
        // it from git_url or working_dir.
        let will_populate = profile.git_url.is_some()
            || PathBuf::from(&profile.working_dir).exists();
        if will_populate && working_dir.exists() {
            tracing::debug!(
                "clearing pre-existing session dir {:?} before populating",
                working_dir
            );
            tokio::fs::remove_dir_all(&working_dir).await
                .map_err(|e| SandboxError::Io(format!("Failed to clear working dir: {}", e)))?;
        }
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

    /// Create a per-session rootfs by copying from the shared
    /// base, bootstrapping the base on first call. Returns the
    /// rootfs path on success.
    ///
    /// Bootstrap strategy: the first time anyone calls this,
    /// `/forge/sandbox/base/` is created by running
    /// `debootstrap --arch=amd64 --variant=minbase bookworm`.
    /// That downloads a ~150MB Debian bookworm minbase. Every
    /// subsequent session is a fast `cp -a` of base into the
    /// per-session rootfs. Per-session rootfs is what gives
    /// each agent its own `apt install` state without
    /// affecting other sessions or the host.
    ///
    /// The base is created with mode 0755 so subsequent `cp -a`
    /// from a root-owned base works without tripping over
    /// "permission denied" for the bind-mount user.
    async fn create_container_root(
        &self,
        root_dir: &PathBuf,
    ) -> std::result::Result<(), SandboxError> {
        let base = PathBuf::from(SANDBOX_BASE_ROOTFS);

        if !base.exists() {
            tracing::info!(
                "bootstrapping shared Debian base at {:?} (first session ever; ~30-60s debootstrap)",
                base
            );
            tokio::fs::create_dir_all(&base).await
                .map_err(|e| SandboxError::Io(format!("Failed to create base dir: {}", e)))?;
            self.run_debootstrap(&base).await?;
            tracing::info!("debootstrap complete at {:?}", base);
        } else {
            tracing::debug!("reusing existing base at {:?}", base);
        }

        // Per-session rootfs is a clean copy of base. We don't
        // try to be clever with hardlinks / reflinks because
        // the per-session `apt install` will diverge the
        // trees and we'd end up with weird CoW behavior.
        // A plain `cp -a` is ~1s for 150MB on SSD, which is
        // fine for session creation.
        //
        // If rootfs already exists but is a *stub* (the old
        // pre-debootstrap code created per-session dirs
        // with just `bin/`, `etc/`, `home/`, `proc/`,
        // etc. and nothing in them), wipe and re-copy from
        // base. We detect a stub by checking for
        // `/etc/debian_version`, which debootstrap writes
        // into the rootfs and which the stub never had.
        // Real rootfs trees are left alone (preserves any
        // state the agent has built up, e.g. `apt install`
        // output).
        let is_real_rootfs = tokio::fs::try_exists(root_dir.join("etc/debian_version"))
            .await
            .unwrap_or(false);
        if root_dir.exists() && is_real_rootfs {
            tracing::debug!(
                "per-session rootfs {:?} is a real Debian tree; reusing",
                root_dir
            );
            return Ok(());
        }
        if root_dir.exists() {
            tracing::info!(
                "per-session rootfs {:?} exists but is a stub; wiping and re-copying from base",
                root_dir
            );
            tokio::fs::remove_dir_all(&root_dir).await
                .map_err(|e| SandboxError::Io(format!(
                    "Failed to wipe stub rootfs {:?}: {}",
                    root_dir, e
                )))?;
        }
        tracing::info!("copying base -> {:?} (per-session rootfs)", root_dir);
        let output = Command::new("cp")
            .arg("-a")
            .arg(format!("{}/.", base.display()))
            .arg(&root_dir)
            .output()
            .await
            .map_err(|e| SandboxError::Io(format!("cp -a base -> rootfs failed: {}", e)))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SandboxError::Io(format!(
                "cp -a base -> rootfs failed: {}",
                stderr
            )));
        }
        tracing::info!("rootfs ready at {:?}", root_dir);
        Ok(())
    }

    /// First-time-only: bootstrap a Debian bookworm minbase
    /// into the given target directory. Uses `debootstrap`,
    /// which the system package `debootstrap` provides.
    ///
    /// We don't poll the deb.debian.org repo for the latest
    /// bookworm packages on every session creation — once the
    /// base is bootstrapped, per-session `cp -a` reuses it.
    /// To refresh the base, the operator runs
    /// `rm -rf /forge/sandbox/base` and lets the next
    /// session re-bootstrap.
    async fn run_debootstrap(
        &self,
        target: &PathBuf,
    ) -> std::result::Result<(), SandboxError> {
        // `debootstrap` can take a while. The outer API call
        // creating this session would otherwise hang on the
        // bash tool's 30s default timeout — but
        // `create_container` is called from the agent registry
        // / session bootstrap path, not from a tool call, so
        // there's no tool-side timeout to bump. We still
        // wrap debootstrap in a 10-minute cap so a stuck
        // mirror doesn't hang session creation forever.
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(600),
            Command::new("debootstrap")
                .arg("--arch=amd64")
                .arg("--variant=minbase")
                .arg(DEBOOTSTRAP_SUITE)
                .arg(target)
                .arg(DEBOOTSTRAP_MIRROR)
                .output(),
        )
        .await
        .map_err(|_| {
            SandboxError::Io("debootstrap timed out after 600s".to_string())
        })?
        .map_err(|e| SandboxError::Io(format!("debootstrap spawn failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SandboxError::Io(format!(
                "debootstrap failed (exit {:?}): {}",
                output.status.code(),
                stderr
            )));
        }
        Ok(())
    }

    /// Run a single bash command in the session's container
    /// namespace, capturing stdout / stderr / exit code.
    ///
    /// Implementation: one-shot `systemd-nspawn` invocation
    /// with `--as-pid2` (skips the init process entirely),
    /// the session's working dir bind-mounted at the same
    /// path, and `--chdir` to the working dir. The command
    /// runs as `timeout --kill-after=2 Ns bash -c '<cmd>'` so
    /// a hung subprocess (e.g. a `cargo test` that's wedged
    /// in a syscall) can be reaped cleanly with SIGKILL.
    ///
    /// We do NOT pass `--network-veth` (network isolation
    /// is intentionally off; the agent needs network access
    /// to talk to the model API, package mirrors, etc.).
    /// Filesystem isolation comes from running inside the
    /// per-session rootfs, not from a network namespace.
    pub async fn run_in_container(
        self: &Arc<Self>,
        session_id: Uuid,
        command: &str,
        timeout_ms: u64,
    ) -> std::result::Result<ContainerRunOutput, SandboxError> {
        let container = {
            let containers = self.containers.read().await;
            containers.get(&session_id).cloned()
        };
        let container = container.ok_or(SandboxError::NotFound(session_id))?;

        let root_dir = &container.root_dir;
        let working_dir = &container.working_dir;

        // Convert ms -> ceil seconds for the inner `timeout`
        // command. Min 1s so a `timeout_ms=0` request still
        // has a real deadline. The outer `tokio::time::timeout`
        // is the hard cap.
        let timeout_secs = std::cmp::max(1, (timeout_ms + 999) / 1000);

        // nspawn itself has a `--time-bound=` option, but we
        // also want the inner `timeout --kill-after=2` so a
        // grandchild that ignores SIGTERM gets SIGKILLed
        // (matching the streaming-bash fix in `api/sse.rs`).
        // The outer tokio timeout is the hard wall-clock cap.
        let mut cmd = Command::new("systemd-nspawn");
        cmd.arg("-D").arg(root_dir)
            .arg("--as-pid2")
            .arg("--user=root")
            .arg("--bind").arg(format!("{}:{}", working_dir.display(), working_dir.display()))
            .arg("--chdir").arg(working_dir)
            // PATH inside the container is the minbase PATH.
            // `bash` is at /bin/bash via the usr-merge
            // symlink in /forge/sandbox/base/bin -> usr/bin.
            .arg("--setenv=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
            .arg("--setenv=HOME=/root")
            .arg("--setenv=USER=root")
            .arg("--setenv=LOGNAME=root")
            .arg("--setenv=TERM=xterm")
            .arg("--")
            .arg("timeout")
            .arg("--kill-after=2")
            .arg(format!("{}s", timeout_secs))
            .arg("bash")
            .arg("-c")
            .arg(command)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null());

        // Outer hard cap. Bump by 5s of grace so the inner
        // `timeout` can escalate SIGTERM -> SIGKILL cleanly
        // before the tokio watchdog fires.
        let outer = std::time::Duration::from_millis(timeout_ms + 5_000);

        let run = cmd.output();
        let timed_out;
        let output = match tokio::time::timeout(outer, run).await {
            Ok(Ok(o)) => {
                timed_out = false;
                o
            }
            Ok(Err(e)) => return Err(SandboxError::Io(format!("nspawn spawn failed: {}", e))),
            Err(_) => {
                // The nspawn process didn't exit within the
                // outer deadline. We can't actually kill it
                // from here (the `cmd.output()` future holds
                // the child); tokio's timeout drops the
                // future, but the OS child keeps running
                // until nspawn's internal cleanup decides to
                // exit. The `timeout --kill-after=2` inside
                // the container will SIGKILL the bash
                // process, after which nspawn itself exits.
                // Worst case: a few seconds of leak.
                timed_out = true;
                std::process::Output {
                    status: std::process::ExitStatus::default(),
                    stdout: Vec::new(),
                    stderr: format!(
                        "[forge-sandbox] nspawn watchdog fired after {}ms; \
                         the inner timeout should have killed the bash process by now",
                        timeout_ms + 5_000
                    ).into_bytes(),
                }
            }
        };

        Ok(ContainerRunOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code(),
            timed_out,
        })
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
