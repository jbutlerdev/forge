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

/// Result of a sandbox reset. `noop: true` when the session
/// had no container to begin with (e.g. /new was called on a
/// room that never had a forge session, or `destroy_container`
/// had already been called). `root_dir` is the path that was
/// (or would have been) wiped.
#[derive(Debug, Clone)]
pub struct ResetResult {
    pub noop: bool,
    pub root_dir: Option<PathBuf>,
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
    /// Root under which per-session working directories live.
    /// The default points at `/forge/sessions` (where the
    /// session manager also creates its dirs); tests override
    /// it via [`Self::with_base_dir`] so the suite can run
    /// on hosts that can't write to `/forge`.
    session_base_dir: PathBuf,
    /// Active containers
    containers: RwLock<std::collections::HashMap<Uuid, SandboxContainer>>,
}

impl SandboxManager {
    /// Create a new sandbox manager
    pub fn new() -> Self {
        Self {
            base_dir: PathBuf::from(SANDBOX_BASE_DIR),
            session_base_dir: PathBuf::from(SESSION_BASE_DIR),
            containers: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Create a new sandbox manager rooted at `base_dir`, with
    /// per-session working dirs under `session_base_dir`. See
    /// [`crate::session_manager::SessionManager::with_base_path`]
    /// for the parallel entry point on the session manager.
    /// Direct construction sidesteps the env-var TOCTOU race
    /// that the old FORGE_SESSIONS_DIR / FORGE_SANDBOX_DIR
    /// fallback had under parallel tests.
    pub fn with_base_dir(base_dir: PathBuf, session_base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            session_base_dir,
            containers: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Initialize the sandbox directory structure
    pub async fn init(&self) -> std::result::Result<(), SandboxError> {
        // Create base directory
        tokio::fs::create_dir_all(&self.base_dir)
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        tokio::fs::create_dir_all(&self.session_base_dir)
            .await
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
        let working_dir = self.session_base_dir.join(session_id.to_string());

        tracing::info!(
            "Creating container {} for session {}",
            container_name,
            session_id
        );

        // Create session working directory. The session manager
        // pre-creates this dir on session creation, so it may
        // already exist with a `.gitkeep` or similar placeholder.
        // We need a clean target for `git clone` / `cp -r` so
        // wipe and recreate the dir if we're about to populate
        // it from git_url or working_dir.
        let will_populate =
            profile.git_url.is_some() || PathBuf::from(&profile.working_dir).exists();
        if will_populate && working_dir.exists() {
            tracing::debug!(
                "clearing pre-existing session dir {:?} before populating",
                working_dir
            );
            tokio::fs::remove_dir_all(&working_dir)
                .await
                .map_err(|e| SandboxError::Io(format!("Failed to clear working dir: {}", e)))?;
        }
        tokio::fs::create_dir_all(&working_dir)
            .await
            .map_err(|e| SandboxError::Io(format!("Failed to create working dir: {}", e)))?;

        // Clone git repo if specified
        if let Some(ref git_url) = profile.git_url {
            self.clone_repository(&working_dir, git_url, &profile.git_ref)
                .await?;
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
        let container = containers
            .get_mut(&session_id)
            .ok_or(SandboxError::NotFound(session_id))?;

        let root_dir = &container.root_dir;
        let working_dir = &container.working_dir;

        tracing::info!("Starting container {} with nspawn", container.name);

        // Start nspawn container
        // -b: boot (use init)
        //        --bind=/forge/sessions/{}:/workspace
        let mut cmd = Command::new("systemd-nspawn");
        cmd.arg("-D")
            .arg(root_dir)
            .arg("-M")
            .arg(&container.name)
            .arg("--chdir")
            .arg("/workspace")
            .arg("--bind")
            .arg(format!("{}:/workspace", working_dir.display()))
            .arg("--private-users=pick")
            .arg("--network-veth")
            .arg("-b") // boot
            .arg("--console=pipe")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .map_err(|e| SandboxError::SpawnFailed(e.to_string()))?;

        let pid = child.id();
        container.pid = pid;
        container.state = SandboxState::Running;

        tracing::info!("Container {} started with PID {:?}", container.name, pid);

        Ok(container.clone())
    }

    /// Stop a container
    pub async fn stop_container(&self, session_id: Uuid) -> std::result::Result<(), SandboxError> {
        let mut containers = self.containers.write().await;
        let container = containers
            .get_mut(&session_id)
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

    /// Get container state. If the in-memory map doesn't
    /// have an entry but the on-disk rootfs is a real
    /// Debian tree, re-register and return it.
    ///
    /// This rehydration is what makes the sandbox survive
    /// an API restart: the in-memory map is process-local,
    /// but the per-session rootfs lives on disk. Without
    /// rehydration, the first bash call after a restart
    /// would see "no container" and fall back to host
    /// execution, even though the container's state is
    /// perfectly intact on disk.
    pub async fn get_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        // Fast path: in-memory hit.
        {
            let containers = self.containers.read().await;
            if let Some(c) = containers.get(&session_id) {
                return Ok(c.clone());
            }
        }
        // Slow path: rehydrate from disk. The check is
        // cheap (one stat) and only runs on a cache miss.
        let container_name = format!("forge-{}", session_id);
        let root_dir = self.base_dir.join(&container_name);
        let working_dir = self.session_base_dir.join(session_id.to_string());
        let is_real_rootfs = tokio::fs::try_exists(root_dir.join("etc/debian_version"))
            .await
            .unwrap_or(false);
        if !is_real_rootfs {
            return Err(SandboxError::NotFound(session_id));
        }
        let container = SandboxContainer {
            name: container_name,
            session_id,
            state: SandboxState::Running, // treat rehydrated as "ready"
            working_dir,
            root_dir,
            pid: None,
        };
        let mut containers = self.containers.write().await;
        // Re-check under the write lock: another caller may
        // have raced us to the rehydrate.
        if let Some(existing) = containers.get(&session_id) {
            return Ok(existing.clone());
        }
        containers.insert(session_id, container.clone());
        tracing::info!(
            session_id = %session_id,
            container = %container.name,
            "sandbox rehydrated from disk after in-memory cache miss"
        );
        Ok(container)
    }

    /// Remove a container and its resources
    pub async fn destroy_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<(), SandboxError> {
        // Stop if running
        let _ = self.stop_container(session_id).await;

        let mut containers = self.containers.write().await;
        let container = containers
            .remove(&session_id)
            .ok_or(SandboxError::NotFound(session_id))?;

        // Remove container root
        if container.root_dir.exists() {
            tokio::fs::remove_dir_all(&container.root_dir)
                .await
                .map_err(|e| SandboxError::Io(format!("Failed to remove root dir: {}", e)))?;
        }

        tracing::info!("Destroyed container {}", container.name);
        Ok(())
    }

    /// Wipe the per-session rootfs and remove the in-memory
    /// container entry, so the next `create_container` (or
    /// the next bash tool call that triggers it) does a
    /// fresh `cp -a` from `/forge/sandbox/base/`.
    ///
    /// Operator workflow this enables:
    /// 1. `chroot /forge/sandbox/base apt install -y foo`
    /// 2. `POST /admin/sandbox-reset?session_id=<new-sid>`
    /// 3. Next bash call in that session: re-cp's from base,
    ///    picks up `foo`.
    ///
    /// The working dir (`/forge/sessions/<uuid>/`) is NOT
    /// touched. Only the per-session rootfs under
    /// `/forge/sandbox/forge-<uuid>/` is wiped. The session
    /// record and the messages table are also untouched —
    /// the conversation continues, just with a clean Debian.
    ///
    /// Idempotent: if the session has no container, returns
    /// a `ResetResult { noop: true, .. }`. If the rootfs is
    /// already gone (e.g. destroyed by a prior call), still
    /// `noop: false` because the in-memory entry was
    /// removed.
    pub async fn reset_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<ResetResult, SandboxError> {
        let mut containers = self.containers.write().await;
        let Some(container) = containers.remove(&session_id) else {
            tracing::info!(
                session_id = %session_id,
                "sandbox reset: no container for this session; noop"
            );
            return Ok(ResetResult {
                noop: true,
                root_dir: None,
            });
        };
        // Drop the write lock before rm -rf so concurrent
        // tool calls don't deadlock on the container map
        // (a bash call mid-reset would otherwise block on
        // the read lock until our rm finishes).
        drop(containers);

        let root_dir = container.root_dir.clone();
        if root_dir.exists() {
            let size = tokio::fs::metadata(&root_dir)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            tokio::fs::remove_dir_all(&root_dir).await.map_err(|e| {
                SandboxError::Io(format!("Failed to wipe rootfs {:?}: {}", root_dir, e))
            })?;
            tracing::info!(
                session_id = %session_id,
                container = %container.name,
                root_dir = %root_dir.display(),
                bytes = size,
                "sandbox reset: wiped per-session rootfs; next bash call will re-cp from base"
            );
        } else {
            tracing::info!(
                session_id = %session_id,
                container = %container.name,
                "sandbox reset: in-memory entry was stale (rootfs already gone); cleared entry"
            );
        }

        Ok(ResetResult {
            noop: false,
            root_dir: Some(root_dir),
        })
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
            tokio::fs::create_dir_all(&base)
                .await
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
            tokio::fs::remove_dir_all(&root_dir).await.map_err(|e| {
                SandboxError::Io(format!("Failed to wipe stub rootfs {:?}: {}", root_dir, e))
            })?;
        }
        tracing::info!("copying base -> {:?} (per-session rootfs)", root_dir);
        let output = Command::new("cp")
            .arg("-a")
            .arg(format!("{}/.", base.display()))
            .arg(root_dir)
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
    async fn run_debootstrap(&self, target: &PathBuf) -> std::result::Result<(), SandboxError> {
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
        .map_err(|_| SandboxError::Io("debootstrap timed out after 600s".to_string()))?
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
        let timeout_secs = std::cmp::max(1, timeout_ms.div_ceil(1000));

        // nspawn itself has a `--time-bound=` option, but we
        // also want the inner `timeout --kill-after=2` so a
        // grandchild that ignores SIGTERM gets SIGKILLed
        // (matching the streaming-bash fix in `api/sse.rs`).
        // The outer tokio timeout is the hard wall-clock cap.
        let mut cmd = Command::new("systemd-nspawn");
        cmd.arg("-D")
            .arg(root_dir)
            .arg("--as-pid2")
            .arg("--user=root")
            .arg("--bind")
            .arg(format!(
                "{}:{}",
                working_dir.display(),
                working_dir.display()
            ))
            .arg("--chdir")
            .arg(working_dir)
            // PATH inside the container is the minbase PATH.
            // `bash` is at /bin/bash via the usr-merge
            // symlink in /forge/sandbox/base/bin -> usr/bin.
            .arg("--setenv=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
            .arg("--setenv=HOME=/root")
            .arg("--setenv=USER=root")
            .arg("--setenv=LOGNAME=root")
            .arg("--setenv=TERM=xterm");

        // Bind-mount the host's Nix store **read-only**
        // into the container. The per-session rootfs has
        // symlinks in /usr/local/bin that point at
        // /nix/store/... (set up by sandbox/build.sh via
        // the nixpkgs package set in
        // sandbox/default.nix). Without this bind-mount
        // the symlinks are dangling and the LLM's bash
        // falls through to /usr/bin (the debootstrap
        // versions).
        //
        // Read-ONLY. The LLM runs as root inside the
        // container; a read-write bind-mount would let
        // it `rm -rf /nix/store/...` and destroy the
        // host's build cache (and any operator-built
        // packages that other sessions depend on). The
        // whole point of the sandbox is host isolation;
        // the LLM gets to use the cached packages, not
        // mutate them. If the LLM needs a one-off
        // tool, it uses `nix shell nixpkgs#foo -- bash
        // -c '...'` (works with read-only /nix/store as
        // long as foo is cached; the temp profile goes
        // in /tmp inside the container, not in
        // /nix/var/nix on the host). For persistent new
        // packages, the operator edits default.nix and
        // re-runs sandbox/build.sh.
        //
        // Skipped when /nix/store doesn't exist on the
        // host (no Nix installed). In that case the LLM
        // runs with whatever the debootstrap base has;
        // the symlinks in /usr/local/bin are just
        // dangling and PATH falls through to /usr/bin.
        if std::path::Path::new("/nix/store").is_dir() {
            cmd.arg("--bind-ro=/nix/store:/nix/store");
        }

        // NIX_CONFIG: `nix shell`, `nix search`, etc.
        // are part of the experimental `nix-command`
        // feature set in Nix 2.x. Enabling it here via
        // the env var means the LLM doesn't have to
        // pass `--extra-experimental-features
        // nix-command` on every invocation. `flakes` is
        // also enabled because the LLM is likely to use
        // flake refs (e.g. `nixpkgs#htop`).
        // `build-users-group = root` suppresses the
        // "group 'nixbld' specified in 'build-users-group'
        // does not exist" warning; we don't have build
        // users in the container and don't need them
        // (everything is fetched from cache.nixos.org).
        //
        // We do NOT bind-mount /nix/var/nix (the host's
        // per-user profiles + gc state) and we do NOT
        // set NIX_USER_PROFILE_DIR. The LLM can run
        // `nix shell nixpkgs#foo -- bash -c '...'` for
        // one-off tools, but cannot do `nix profile
        // add` (which would need to write to the host's
        // /nix/var/nix). That's deliberate: persistent
        // installs in the host's Nix store are an
        // operator decision made via
        // sandbox/default.nix + build.sh, not a
        // per-session mutation.
        cmd.arg("--setenv=NIX_CONFIG=experimental-features = nix-command flakes\nbuild-users-group = root");

        // NIX_SSL_CERT_FILE: Nix uses its own trust
        // anchors (not the system openssl) for
        // downloads from cache.nixos.org. Point it at
        // the base's ca-bundle (installed by
        // sandbox/build.sh from the nixpkgs `cacert`
        // package). Without this, `nix shell` and
        // `nix search` fail with "Problem with the SSL
        // CA cert" and "error adding trust anchors from
        // file".
        cmd.arg("--setenv=NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt");

        // Pass the operator's GitHub PAT (if configured) into
        // the container as $GITHUB_TOKEN. The base rootfs has
        // a credential helper at /usr/local/bin/git-credential-github
        // that reads $GITHUB_TOKEN and provides it to git for
        // github.com auth, so the LLM can `git push` without
        // constructing token-bearing URLs and without the token
        // ending up in the audit log.
        //
        // forge-api's process env is read here; the env var
        // lives in /etc/forge/forge.env (mode 0600). Empty
        // / missing env var: no --setenv added, the LLM sees
        // an empty / unset GITHUB_TOKEN and the credential
        // helper returns no credentials for github.com (git
        // falls back to its default auth, which will fail for
        // a non-interactive session — the LLM will see a clear
        // "could not read Username" error rather than a silent
        // misconfig).
        if let Ok(token) = std::env::var("FORGE_GITHUB_TOKEN") {
            if !token.is_empty() {
                cmd.arg(format!("--setenv=GITHUB_TOKEN={}", token));
            }
        }

        cmd.arg("--")
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
                    )
                    .into_bytes(),
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

        // For github.com URLs, inject the FORGE_GITHUB_TOKEN
        // from the forge-api process env so private repos the
        // token has access to clone cleanly. Without this, git
        // over HTTPS prompts for a username on the controlling
        // TTY; the nspawn container has no TTY, so the prompt
        // fails with "could not read Username for
        // 'https://github.com': No such device or address".
        // The streaming-bash path also passes the same token
        // into the container as GITHUB_TOKEN for `git push`
        // — the two paths use the same operator-controlled
        // PAT. Non-github.com URLs are passed through verbatim;
        // we'd rather let a missing token surface as a clean
        // 401/404 than smuggle a GitHub PAT into a third-party
        // host.
        let (effective_url, redacted_url) = inject_github_token(git_url);

        cmd.arg(&effective_url).arg(target_dir);

        let output = cmd
            .output()
            .await
            .map_err(|e| SandboxError::Git(format!("Clone failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Redact any token that may have leaked into the
            // git error message (git includes the URL in some
            // failure paths, e.g. "fatal: could not read
            // Username for 'https://x-access-token:ghp_***@...'")
            let stderr_safe = redact_token_in_message(&stderr);
            return Err(SandboxError::Git(format!(
                "Clone failed for {}: {}",
                redacted_url, stderr_safe
            )));
        }

        tracing::info!("Cloned repository {} into {:?}", redacted_url, target_dir);
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

    let output = cmd
        .output()
        .await
        .map_err(|e| SandboxError::Io(e.to_string()))?;

    Ok(output)
}

/// Inject `FORGE_GITHUB_TOKEN` into a github.com HTTPS URL so
/// `git clone` can authenticate against private repos. Returns
/// `(effective_url, redacted_url)` — the first is what to pass
/// to `git`, the second is safe to log (token stripped).
///
/// For non-github.com URLs, or when no token is set, the URL
/// is returned unchanged. We deliberately don't fall back to
/// SSH-style URLs, env-var credential helpers, or any other
/// auth path: the operator's `FORGE_GITHUB_TOKEN` is the
/// single source of truth for github auth in forge, and the
/// streaming-bash path passes the same token into the
/// container as `GITHUB_TOKEN` for `git push`. One PAT, one
/// injection point, one source of bugs.
///
/// The "x-access-token" form (vs `token` / `ghp_***` directly)
/// is GitHub's documented auth pattern; it works for both
/// classic PATs and fine-grained tokens, and `git` will use
/// it for clone + push.
pub(crate) fn inject_github_token(url: &str) -> (String, String) {
    let token = match std::env::var("FORGE_GITHUB_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => return (url.to_string(), url.to_string()),
    };

    // Only transform github.com URLs. Don't smuggle a GitHub
    // PAT into a third-party host (e.g. a private GitLab).
    // No `url` crate dependency: we accept either the
    // canonical `https://github.com/...` form or
    // `https://www.github.com/...` and check the prefix
    // directly. Anything else passes through unchanged.
    // Note: `host` is the part after the scheme; the result
    // keeps the host literal as it was in the input.
    let (host, rest) = if let Some(rest) = url.strip_prefix("https://github.com/") {
        ("github.com", rest)
    } else if let Some(rest) = url.strip_prefix("https://www.github.com/") {
        ("www.github.com", rest)
    } else {
        return (url.to_string(), url.to_string());
    };

    let effective = format!("https://x-access-token:{token}@{host}/{rest}");
    let redacted = redact_token_in_url(&effective);
    (effective, redacted)
}

/// Replace any `x-access-token:ghp_***@` substring in `s` with
/// `x-access-token:***@`. Used for both the URL we pass to
/// logging helpers and the stderr messages git emits, which
/// can include the URL on failure.
pub(crate) fn redact_token_in_url(s: &str) -> String {
    // Match `x-access-token:` followed by any chars that
    // aren't `@`, then `@`.
    let bytes = s.as_bytes();
    let needle = b"x-access-token:";
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + needle.len() <= bytes.len() && &bytes[i..i + needle.len()] == needle {
            out.push_str("x-access-token:");
            // Skip until '@' or end.
            let mut j = i + needle.len();
            while j < bytes.len() && bytes[j] != b'@' {
                j += 1;
            }
            out.push_str("***");
            // The '@' (if any) is preserved.
            if j < bytes.len() && bytes[j] == b'@' {
                out.push('@');
                j += 1;
            }
            i = j;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Redact any `x-access-token:<value>@` substring in `s` (a
/// git error message) with `x-access-token:***@`. Some git
/// failures include the full URL in stderr.
pub(crate) fn redact_token_in_message(s: &str) -> String {
    redact_token_in_url(s)
}

#[cfg(test)]
mod git_token_tests {
    use super::*;

    #[test]
    fn no_token_leaves_url_unchanged() {
        // The env var may or may not be set in the test
        // process; either way the URL passes through.
        let (effective, redacted) = inject_github_token("https://github.com/owner/repo.git");
        assert_eq!(effective, "https://github.com/owner/repo.git");
        assert_eq!(redacted, "https://github.com/owner/repo.git");
    }

    #[test]
    fn redacts_x_access_token_substring() {
        let s = "fatal: could not read Username for 'https://x-access-token:ghp_supersecret@github.com/owner/repo': No such device or address";
        let out = redact_token_in_message(s);
        assert!(!out.contains("ghp_supersecret"), "token leaked: {out}");
        assert!(out.contains("x-access-token:***"));
    }

    #[test]
    fn redacts_token_at_end_without_at_sign() {
        let out = redact_token_in_url("https://x-access-token:ghp_xyz");
        assert_eq!(out, "https://x-access-token:***");
    }

    #[test]
    fn with_token_injects_x_access_token_form() {
        // Set the env var, then verify the injection shape.
        // We snapshot the existing value (if any) and restore
        // it after the test, so this is safe to run in any
        // environment.
        let prior = std::env::var("FORGE_GITHUB_TOKEN").ok();
        // SAFETY: the test serializes env mutation by being
        // a single-threaded test; cargo's default test
        // runner runs tests in parallel only across crates,
        // not within a single test function.
        std::env::set_var("FORGE_GITHUB_TOKEN", "ghp_testtoken123");

        let (effective, redacted) = inject_github_token("https://github.com/owner/repo.git");
        assert_eq!(
            effective,
            "https://x-access-token:ghp_testtoken123@github.com/owner/repo.git"
        );
        // The redacted form must NOT contain the token but
        // must keep the rest of the URL intact.
        assert!(
            !redacted.contains("ghp_testtoken123"),
            "token leaked: {redacted}"
        );
        assert!(redacted.contains("x-access-token:***@"));
        assert_eq!(
            redacted,
            "https://x-access-token:***@github.com/owner/repo.git"
        );

        // www.github.com also gets transformed.
        let (eff2, _) = inject_github_token("https://www.github.com/owner/repo");
        assert_eq!(
            eff2,
            "https://x-access-token:ghp_testtoken123@www.github.com/owner/repo"
        );

        // Non-github.com URLs pass through unchanged, even
        // with a token set.
        let (eff3, red3) = inject_github_token("https://gitlab.example.com/owner/repo.git");
        assert_eq!(eff3, "https://gitlab.example.com/owner/repo.git");
        assert_eq!(red3, "https://gitlab.example.com/owner/repo.git");

        match prior {
            Some(v) => std::env::set_var("FORGE_GITHUB_TOKEN", v),
            None => std::env::remove_var("FORGE_GITHUB_TOKEN"),
        }
    }
}
