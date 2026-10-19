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
use std::path::Path;

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

/// Determine the debootstrap `--arch` flag for the host architecture.
/// debootstrap wants the Debian arch name: `amd64` on x86_64 hosts,
/// `arm64` on aarch64 hosts (e.g. Apple Silicon / ALC machines). Can be
/// overridden with the FORGE_SANDBOX_ARCH env var for exotic setups.
fn debootstrap_arch() -> String {
    if let Ok(arch) = std::env::var("FORGE_SANDBOX_ARCH") {
        if !arch.is_empty() {
            return arch;
        }
    }
    match std::env::consts::ARCH {
        "x86_64" => "amd64".to_string(),
        "aarch64" => "arm64".to_string(),
        other => {
            tracing::warn!(
                host_arch = %other,
                "unknown host architecture; defaulting to amd64 for debootstrap"
            );
            "amd64".to_string()
        }
    }
}

/// Operator-controlled env vars that are passed through from the
/// forge-api process into every per-call sandbox container. Read at
/// nspawn-build time so a rotation in `/etc/forge/forge.env` + an API
/// restart takes effect on the next bash call. Empty / unset vars are
/// skipped (no `--setenv` added), so the LLM sees the binary's
/// compiled-in default instead.
///
/// This is a struct (not a tuple) so [`nspawn_args`] stays pure and
/// unit-testable: the caller reads the env once and passes the values
/// in, rather than the builder reading `std::env` itself (which would
/// race under parallel tests and hide the inputs from the test).
#[derive(Debug, Default, Clone)]
pub(crate) struct ContainerEnv {
    pub github_token: Option<String>,
    pub search_instance: Option<String>,
    pub search_api_key: Option<String>,
    /// The forge API credential, passed so the agent inside the
    /// container can use its documented deploy flow
    /// (`curl -H "X-API-Key: $FORGE_API_KEY" …/admin/self-update`).
    /// The auth middleware now requires a REAL key (admin role for
    /// `/admin/*`), so without this passthrough the guard's curl
    /// would expand `$FORGE_API_KEY` to empty and get a 401; with
    /// it, the operator's key (or the process tool token) arrives
    /// intact.
    pub forge_api_key: Option<String>,
    /// Herd H6.5: the agent's credential slots (secret values),
    /// resolved from the `secrets` table at tool-execution time
    /// (see `crate::credentials`). Values land in the container
    /// environment ONLY (nspawn `--setenv=` args) and in nothing
    /// else — never in `messages` rows, the approval cards, or the
    /// model context.
    pub extra_env: Vec<(String, String)>,
}

impl ContainerEnv {
    /// Read the passthrough env vars from the current process. Call
    /// this once per bash call (it's cheap — a few `getenv`s) and
    /// pass the result to [`nspawn_args`] / [`build_nspawn_command`].
    pub(crate) fn from_process_env() -> Self {
        fn nonempty(name: &str) -> Option<String> {
            std::env::var(name).ok().filter(|s| !s.is_empty())
        }
        Self {
            github_token: nonempty("FORGE_GITHUB_TOKEN"),
            search_instance: nonempty("FORGE_SEARCH_INSTANCE"),
            search_api_key: nonempty("FORGE_SEARCH_API_KEY"),
            forge_api_key: nonempty("FORGE_API_KEY"),
            extra_env: Vec::new(),
        }
    }

    /// Builder: attach the agent's resolved credential slots
    /// (H6.5) to this env. The caller resolves them via
    /// [`crate::credentials::resolve_credential_env`] and fails the
    /// call when any declared ref has no live value.
    pub(crate) fn with_credential_env(mut self, extra: Vec<(String, String)>) -> Self {
        self.extra_env = extra;
        self
    }
}

/// Build the argument vector for a per-call `systemd-nspawn` that
/// runs `timeout --kill-after=2 {timeout_secs}s bash -c {command}`
/// inside the session's rootfs at `working_dir`.
///
/// This is the **single source of truth** for what every sandboxed
/// bash call gets: the rootfs bind, the working-dir bind/chdir, the
/// container PATH/HOME/USER/LOGNAME/TERM, the read-only `/nix/store`
/// bind-mount, `NIX_CONFIG` / `NIX_SSL_CERT_FILE`, and the operator
/// env passthrough (`GITHUB_TOKEN`, `SEARCH_INSTANCE`, `SEARCH_API_KEY`).
/// The non-streaming (`run_in_container`) and streaming
/// (`api::sse::execute_bash_streaming`) bash paths both call this so
/// the two can't drift on which env vars the container receives — a
/// drift that previously cost the streaming path the `SEARCH_*` vars
/// (the bundled `search` CLI silently fell back to defaults / failed
/// auth on a private instance when run via streaming bash).
///
/// Pure: takes the env values as a struct (see [`ContainerEnv`]) so a
/// unit test can assert the passthrough args are present without
/// touching `std::env`. The `--bind-ro=/nix/store` arg is included
/// only when `/nix/store` exists on the host at call time.
/// Per-tree exec lock (2026-10-19).
///
/// `systemd-nspawn` (pinned 261.2) takes a `LOCK_EX | LOCK_NB` flock
/// on the `--directory` tree for the machine's whole lifetime — a
/// second nspawn on the same tree does NOT wait, it fails instantly
/// (measured 1–2 ms) with `Directory tree <dir> is currently busy.`.
/// pi emits multiple bash tool calls per assistant turn and the
/// harness dispatches them concurrently, so any turn with two
/// sandboxed calls deterministically failed one of them (the
/// dependabot dots ate this daily: `… is currently busy` as
/// exit=1/duration=1ms tool rows). The sandbox tree IS a single
/// shared filesystem, so the right semantic is to serialize: every
/// in-tree exec (nspawn or chroot) takes this per-tree FIFO lock
/// and waits instead of losing the flock race.
///
/// Entries live for the process lifetime (a few hundred bytes per
/// session tree); that's deliberate — removing entries under
/// concurrency would re-introduce the race in a subtler form.
pub(crate) fn tree_lock_for(root_dir: &std::path::Path) -> Arc<tokio::sync::Mutex<()>> {
    use std::collections::HashMap;
    use std::sync::LazyLock;
    static TREE_LOCKS: LazyLock<std::sync::Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> =
        LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
    let mut map = TREE_LOCKS.lock().unwrap_or_else(|p| p.into_inner());
    map.entry(root_dir.to_path_buf())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// True when nspawn stderr is the flock-loss (not the command's own
/// stderr). Matched on the stable substring of
/// `Directory tree %s is currently busy.`.
pub(crate) fn nspawn_busy_error(stderr: &str) -> bool {
    stderr.contains("is currently busy") && stderr.contains("Directory tree")
}

/// Acquire the per-tree lock, FIFO. Waits up to `TREE_LOCK_WAIT`
/// (a wedged sibling exec can't deadlock the session — past the
/// cap we run anyway and let nspawn's own busy error be the
/// backstop) and returns an OWNED guard so callers can stash it in
/// a local across awaits.
pub(crate) async fn acquire_tree_guard(
    root_dir: &std::path::Path,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    const TREE_LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(120);
    let m = tree_lock_for(root_dir);
    match tokio::time::timeout(TREE_LOCK_WAIT, m.lock_owned()).await {
        Ok(g) => Some(g),
        Err(_) => {
            tracing::warn!(
                tree = %root_dir.display(),
                "sandbox tree still busy after {}s lock wait; running anyway (nspawn flock is the backstop)",
                TREE_LOCK_WAIT.as_secs()
            );
            None
        }
    }
}

pub(crate) fn nspawn_args(
    root_dir: &Path,
    working_dir: &Path,
    timeout_secs: u64,
    command: &str,
    env: &ContainerEnv,
) -> Vec<String> {
    let mut args: Vec<String> = Vec::with_capacity(32);
    args.push("-D".to_string());
    args.push(root_dir.to_string_lossy().to_string());
    args.push("--as-pid2".to_string());
    // Clean agent stderr (2026-10-18): nspawn's own banner —
    // "Spawning container…/exited successfully.", the `--user=root`
    // deprecation note, the address-families future-default warning —
    // was landing in the agent's tool output and being misread as
    // command errors (recurred across sessions; agents wasted turns
    // on it). `--quiet` + the modern `--uid=0` + an explicit
    // restrict-address-families (pinning today's default set) leave
    // only genuine command stderr.
    args.push("--quiet".to_string());
    args.push("--uid=0".to_string());
    args.push("--restrict-address-families=AF_INET AF_INET6 AF_UNIX".to_string());
    args.push("--bind".to_string());
    args.push(format!(
        "{}:{}",
        working_dir.display(),
        working_dir.display()
    ));
    args.push("--chdir".to_string());
    args.push(working_dir.to_string_lossy().to_string());
    args.push(
        "--setenv=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
    );
    args.push("--setenv=HOME=/root".to_string());
    args.push("--setenv=USER=root".to_string());
    args.push("--setenv=LOGNAME=root".to_string());
    args.push("--setenv=TERM=xterm".to_string());
    // Never let git (or anything else) spawn an interactive pager on the
    // nspawn root TTY — a pager (less) blocks on input and hangs the whole
    // tool call (observed: `git log` stuck 7+ min on a fresh minbase where
    // less was absent but git's pager fallback still wedged on the PTY).
    args.push("--setenv=GIT_PAGER=cat".to_string());
    args.push("--setenv=PAGER=cat".to_string());
    if std::path::Path::new("/nix/store").is_dir() {
        args.push("--bind-ro=/nix/store:/nix/store".to_string());
    }
    args.push(
        "--setenv=NIX_CONFIG=experimental-features = nix-command flakes\nbuild-users-group = root"
            .to_string(),
    );
    args.push("--setenv=NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt".to_string());
    if let Some(token) = &env.github_token {
        args.push(format!("--setenv=GITHUB_TOKEN={}", token));
    }
    if let Some(instance) = &env.search_instance {
        args.push(format!("--setenv=SEARCH_INSTANCE={}", instance));
    }
    if let Some(api_key) = &env.search_api_key {
        args.push(format!("--setenv=SEARCH_API_KEY={}", api_key));
    }
    if let Some(api_key) = &env.forge_api_key {
        args.push(format!("--setenv=FORGE_API_KEY={}", api_key));
    }
    // H6.5 credential slots: the agent's env_refs, resolved by the
    // caller at exec time. Slot names are validated `A-Z0-9_` shapes
    // at declaration, so they are safe as env var names; the values
    // ride argv for the call's lifetime only (same accepted risk as
    // the GITHUB_TOKEN passthrough above — nspawn argv is visible in
    // /proc/<pid>/cmdline for the call).
    for (name, value) in &env.extra_env {
        args.push(format!("--setenv={name}={value}"));
    }
    args.push("--".to_string());
    args.push("timeout".to_string());
    args.push("--kill-after=2".to_string());
    args.push(format!("{}s", timeout_secs));
    args.push("bash".to_string());
    args.push("-c".to_string());
    args.push(command.to_string());
    args
}

/// Build a ready-to-spawn `systemd-nspawn` `Command` from
/// [`nspawn_args`], with stdin wired to `/dev/null`. The caller sets
/// `stdout` / `stderr` (piped for capture, or inherit) and calls
/// `spawn()` / `output()` as appropriate for its streaming vs.
/// one-shot needs.
pub(crate) fn build_nspawn_command(
    root_dir: &Path,
    working_dir: &Path,
    timeout_secs: u64,
    command: &str,
    env: &ContainerEnv,
) -> Command {
    let mut cmd = Command::new("systemd-nspawn");
    for arg in nspawn_args(root_dir, working_dir, timeout_secs, command, env) {
        cmd.arg(arg);
    }
    cmd.stdin(Stdio::null());
    // If the caller drops the output future (e.g. the read watchdog
    // timeout fires), kill nspawn instead of orphaning it. Accepted
    // limitation: nspawn's argv — including any `--setenv=<secret>`
    // passthrough — is visible to host users in /proc/<pid>/cmdline
    // for the call's lifetime; the in-container `timeout
    // --kill-after=2` remains the backstop if nspawn is killed early.
    cmd.kill_on_drop(true);
    cmd
}

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

/// Global serialization for chroot apt operations (rare, and apt's
/// own locking across sibling rootfs copies on shared media is
/// nastier than a process-wide queue).
fn pkg_install_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// `chroot <root_dir> <argv..>` with a wall-clock timeout.
async fn run_chroot(
    root_dir: &Path,
    argv: &[&str],
    timeout_secs: u64,
) -> std::io::Result<std::process::Output> {
    let mut cmd = Command::new("chroot");
    cmd.arg(root_dir).args(argv).stdout(Stdio::piped()).stderr(Stdio::piped());
    tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output())
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "chroot timed out"))?
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

    /// The working directory a session's pi process runs in:
    /// `<session_base_dir>/<session_id>`. In production this is
    /// `/forge/sessions/<id>`; in tests (`TestApp` points the
    /// manager at a tempdir) it's `<tmp>/sessions/<id>`. Used by
    /// `AgentRegistry::get_or_create`'s sandbox-fallback path so
    /// the fallback cwd matches the dir `create_session` already
    /// created (and exists in CI, where the hard-coded
    /// `/forge/sessions/<id>` does not).
    pub fn session_working_dir(&self, session_id: Uuid) -> PathBuf {
        self.session_base_dir.join(session_id.to_string())
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

    /// Create a new container for a session. See
    /// [`Self::create_container_inner`] with `preserve_existing = false`.
    pub async fn create_container(
        &self,
        session_id: Uuid,
        profile: &Profile,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        self.create_container_inner(session_id, profile, false)
            .await
    }

    /// Create a container but **keep** an already-populated working
    /// directory instead of wiping it back to the profile baseline.
    /// Used by the model-switcher path: `update_session` tears down
    /// only the in-memory pi agent and promises the workspace
    /// survives, so the next spawn must not `rm -rf` the tree (that
    /// would destroy the agent's untracked files and unrecorded
    /// edits, which the tool-call replay cannot restore). When the
    /// directory is empty or missing (e.g. a session that was
    /// created but never used), it is populated normally.
    pub async fn create_container_preserving(
        &self,
        session_id: Uuid,
        profile: &Profile,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        self.create_container_inner(session_id, profile, true).await
    }

    async fn create_container_inner(
        &self,
        session_id: Uuid,
        profile: &Profile,
        preserve_existing: bool,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        let container_name = format!("forge-{}", session_id);
        let root_dir = self.base_dir.join(&container_name);
        let working_dir = self.session_base_dir.join(session_id.to_string());

        tracing::info!(
            "Creating container {} for session {} (preserve_existing={})",
            container_name,
            session_id,
            preserve_existing
        );

        // Create session working directory. The session manager
        // pre-creates this dir on session creation, so it may
        // already exist with a `.gitkeep` or similar placeholder.
        // We need a clean target for `git clone` / `cp -r` so
        // wipe and recreate the dir if we're about to populate
        // it from git_url or working_dir — UNLESS the caller asked
        // to preserve an existing populated tree (model switch).
        let will_populate =
            profile.git_url.is_some() || PathBuf::from(&profile.working_dir).exists();
        // `dir_has_content` is false when the dir is missing, empty,
        // or unreadable — in all those cases (re)populating from the
        // baseline is correct even under `preserve_existing`.
        let dir_has_content = match tokio::fs::read_dir(&working_dir).await {
            Ok(mut rd) => rd.next_entry().await.ok().flatten().is_some(),
            Err(_) => false,
        };
        let should_wipe =
            will_populate && working_dir.exists() && !(preserve_existing && dir_has_content);
        if should_wipe {
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

        if !(preserve_existing && dir_has_content) {
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
        } else {
            tracing::info!(
                "preserving existing working tree at {:?} (model-switch spawn)",
                working_dir
            );
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

    /// Get the session's container, or bootstrap its per-session rootfs
    /// on demand if it isn't on disk yet. This is the self-healing path
    /// for the tool-execution endpoints: a session whose rootfs was never
    /// created (a brand-new session, or one lost to a disk reset / manual
    /// deletion / a failed first-time bootstrap) otherwise fails every
    /// `bash` call with "Container not found" forever. Bootstrapping is a
    /// fast `cp -a` from the shared base rootfs.
    ///
    /// Uses `create_container_preserving` so an already-populated working
    /// tree is never wiped back to the profile baseline — the common
    /// rehydration case is a *live* session that has files the agent made.
    pub async fn get_or_create_container(
        &self,
        session_id: Uuid,
        profile: &Profile,
    ) -> std::result::Result<SandboxContainer, SandboxError> {
        if let Ok(c) = self.get_container(session_id).await {
            return Ok(c);
        }
        tracing::warn!(
            session_id = %session_id,
            "sandbox rootfs missing; bootstrapping per-session rootfs on demand"
        );
        self.create_container_preserving(session_id, profile).await
    }

    /// Remove a container and its resources
    pub async fn destroy_container(
        &self,
        session_id: Uuid,
    ) -> std::result::Result<(), SandboxError> {
        // Stop if running
        let _ = self.stop_container(session_id).await;

        let mut containers = self.containers.write().await;
        let removed = containers.remove(&session_id);
        // Fall back to the canonical on-disk rootfs path when the
        // in-memory map has no entry (e.g. after an API restart the
        // map is empty but `get_container`-style rehydration never
        // ran). Previously this aborted with NotFound and leaked the
        // per-session rootfs on disk forever.
        let root_dir = match removed {
            Some(c) => c.root_dir,
            None => self.base_dir.join(format!("forge-{}", session_id)),
        };
        let name = format!("forge-{}", session_id);

        // Remove container root
        if root_dir.exists() {
            tokio::fs::remove_dir_all(&root_dir)
                .await
                .map_err(|e| SandboxError::Io(format!("Failed to remove root dir: {}", e)))?;
        }

        tracing::info!("Destroyed container {}", name);
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
    /// `debootstrap --arch=<host-arch> --variant=minbase bookworm`.
    /// The arch is detected from the host (`amd64` on x86_64, `arm64`
    /// on aarch64) or overridden via `FORGE_SANDBOX_ARCH`.
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
        // Hold the per-tree lock across the WHOLE existence-check +
        // wipe + `cp -a` (2026-10-19). Without it, two concurrent
        // creators for the same session (harness spawn + first tool
        // call, or a parallel tool-call turn) both saw "missing or
        // stub" and raced `cp -a` into the same destination — one
        // creator's in-progress copy made `etc/debian_version`
        // visible to the other's `remove_dir_all`, and the first
        // exec then hit a half-populated tree:
        // `execv(timeout) failed: No such file or directory` at
        // ~40 ms on the session's first bash (observed on fresh
        // sessions ddbc8cc9 + ed409c0c). Execs already take this
        // lock, so creation now completes-before-anyone-runs.
        let _tree = acquire_tree_guard(root_dir).await;
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
                .arg(format!("--arch={}", debootstrap_arch()))
                .arg("--variant=minbase")
                // minbase is bare-bones. Agents' first reflex is
                // `git clone` / `curl` / `gh api` — without these every
                // session hits "command not found", burns turns hunting
                // for tools, and (2026-10-18, mule-dependabot) honestly
                // reports zero findings it could not collect. Bake the
                // working set in:
                //   VCS/HTTP:   git, openssh-client, curl, wget + TLS
                //   GitHub:     gh (bookworm main ships it)
                //   plumbing:   jq, ripgrep, unzip/zip, procps, less
                //   self-serve: apt + ca-certificates so agents can
                //               install the long tail (nodejs, python3)
                //               themselves — verified egress permitting
                .arg("--include=git,ca-certificates,curl,wget,gh,jq,ripgrep,unzip,zip,openssh-client,procps,less,apt")
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
        self.run_in_container_env(session_id, command, timeout_ms, Vec::new())
            .await
    }

    /// [`run_in_container`] with the agent's H6.5 credential slots
    /// injected into the container environment (`extra_env`, already
    /// resolved by the caller via
    /// [`crate::credentials::resolve_credential_env`]).
    pub async fn run_in_container_env(
        self: &Arc<Self>,
        session_id: Uuid,
        command: &str,
        timeout_ms: u64,
        extra_env: Vec<(String, String)>,
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

        // Build the per-call nspawn command via the shared
        // `build_nspawn_command` helper so the streaming and
        // non-streaming bash paths can't drift on which env vars
        // / bind-mounts the container gets (see `nspawn_args` for
        // the full rationale, including the `SEARCH_*` passthrough
        // the streaming path previously missed).
        let env = ContainerEnv::from_process_env().with_credential_env(extra_env);

        // Outer hard cap. Bump by 5s of grace so the inner
        // `timeout` can escalate SIGTERM -> SIGKILL cleanly
        // before the tokio watchdog fires.
        let outer = std::time::Duration::from_millis(timeout_ms + 5_000);

        // One shot at the tree lock + one busy-retry (see
        // `tree_lock_for` for why the lock exists; the retry covers
        // contention from OUTSIDE this process — e.g. an nspawn
        // orphaned by a hot upgrade still finishing its inner
        // `timeout`). The second loss surfaces honestly; agents
        // have always seen the raw error in that case.
        let mut attempt = 0u8;
        let (timed_out, output) = loop {
            attempt += 1;
            let mut cmd = build_nspawn_command(root_dir, working_dir, timeout_secs, command, &env);
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            let _tree = acquire_tree_guard(root_dir).await;
            let run = cmd.output();
            let timed_out;
            let output = match tokio::time::timeout(outer, run).await {
                Ok(Ok(o)) => {
                    timed_out = false;
                    o
                }
                Ok(Err(e)) => {
                    return Err(SandboxError::Io(format!("nspawn spawn failed: {e}")))
                }
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
            drop(_tree);
            if attempt < 2 && !timed_out && nspawn_busy_error(&String::from_utf8_lossy(&output.stderr)) {
                tracing::warn!(
                    session_id = %session_id,
                    "nspawn lost the tree flock to a sibling exec; retrying once in 1.5s"
                );
                tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
                continue;
            }
            break (timed_out, output);
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

        // Auth via git's credential-helper protocol, not URL
        // injection. The host has /usr/local/bin/git-credential-github
        // (installed by scripts/install.sh; reads $FORGE_GITHUB_TOKEN
        // from the operator's env). Git calls it for github.com
        // URLs and the token never enters the clone URL — so it
        // doesn't end up in:
        //
        //   - the .git/config the LLM can read inside the sandbox
        //   - `ps`/`top`/process listing of the forge-api child
        //   - git's stderr on a 401 (git includes the URL in
        //     some failure paths: "fatal: could not read Username
        //     for 'https://x-access-token:ghp_…@…'")
        //   - any log line that captures the clone command
        //
        // The previous design injected the token into the URL
        // directly, which left it in all of the above. The
        // streaming-bash path on the API side does the same
        // thing for the container: it passes the token into
        // the nspawn container as `$GITHUB_TOKEN`, and the
        // in-container helper at
        // /forge/sandbox/base/usr/local/bin/git-credential-github
        // hands it to git on demand. Host and container
        // credential helpers are deliberately separate files
        // because the env var name differs (FORGE_GITHUB_TOKEN
        // vs GITHUB_TOKEN) and the install paths differ
        // (/usr/local/bin vs /forge/sandbox/base/usr/local/bin).
        //
        // We pass the helper via `GIT_CONFIG_*` env vars rather
        // than `git -c credential.helper=…` because `git clone -c`
        // *persists* the option to the new repo's local config
        // (the `-c` for `clone` is documented to set values in
        // the created repository, intended for things like
        // `git clone -c init.defaultBranch=main …`). The env-var
        // form is one-shot: git reads it for the duration of
        // the invocation, then discards it. The .git/config
        // stays clean — just `[remote "origin"] url = …` with
        // no `x-access-token` and no `credential.helper`.
        //
        // For non-github.com URLs we pass the clean URL with
        // no credential helper at all. We'd rather let a
        // missing token surface as a clean 401/404 than smuggle
        // a GitHub PAT into a third-party host's auth logs.
        let use_credential_helper = git_url.starts_with("https://github.com/")
            || git_url.starts_with("https://www.github.com/");
        if use_credential_helper {
            cmd.env("GIT_CONFIG_COUNT", "1");
            cmd.env("GIT_CONFIG_KEY_0", "credential.helper");
            cmd.env("GIT_CONFIG_VALUE_0", "/usr/local/bin/git-credential-github");
        }

        cmd.arg(git_url).arg(target_dir);

        let output = cmd
            .output()
            .await
            .map_err(|e| SandboxError::Git(format!("Clone failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SandboxError::Git(format!(
                "Clone failed for {}: {}",
                git_url, stderr
            )));
        }

        tracing::info!("Cloned repository {} into {:?}", git_url, target_dir);
        Ok(())
    }

    /// Copy directory **contents** into an existing destination.
    ///
    /// `cp -r -p <src> <dst>` with an existing `<dst>` copies the
    /// source *into* the destination, nesting it one level (the
    /// agent's cwd would end up with a single child directory instead
    /// of the repo contents). Appending `/.` makes GNU cp copy the
    /// contents — the behavior both call sites need. Hidden files are
    /// included (`.` matches everything).
    async fn copy_directory(
        &self,
        src: &Path,
        dst: &Path,
    ) -> std::result::Result<(), SandboxError> {
        let src_contents = format!("{}/.", src.display());
        let output = Command::new("cp")
            .args(["-r", "-p"])
            .arg(&src_contents)
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

    // ============================================
    // Persistent approved packages (sandbox governance, 2026-10-18)
    // ============================================
    //
    // The approved set lives in `sandbox_package_requests` (see
    // migration 028 + api/sandbox_pkgs.rs). Before every sandboxed
    // bash exec we reconcile the session rootfs against it: a marker
    // file `<rootfs>/etc/forge-packages.json` records what has been
    // installed, and anything approved-but-missing is
    // `chroot <rootfs> apt-get install`ed. That makes agent package
    // installs PERSISTENT across session death, resets, and base
    // refreshes — an approved `nodejs` shows up in every future
    // sandbox of that agent without anyone re-asking.

    /// Reconcile one session's rootfs with the agent's approved
    /// package set. Best-effort by contract: every failure path logs
    /// and returns — bash must never be blocked by governance
    /// bookkeeping. A global async lock serializes installs (they are
    /// rare; apt's own locking is coarser and would collide across
    /// sibling rootfs copies on shared base files).
    pub async fn ensure_packages(&self, db: Option<&sqlx::PgPool>, session_id: Uuid) {
        let Some(db) = db else { return };
        let agent_id: Option<Uuid> = match sqlx::query_scalar(
            "SELECT agent_id FROM sessions WHERE id = $1",
        )
        .bind(session_id)
        .fetch_optional(db)
        .await
        {
            Ok(Some(a)) => a,
            _ => return,
        };
        let Some(agent_id) = agent_id else { return };
        let root_dir = self.base_dir.join(format!("forge-{}", session_id));
        Self::reconcile_packages(db, &root_dir, agent_id).await;
    }

    /// Approve-time fast path: install into every existing rootfs of
    /// the agent right now (fire-and-forget from the decision
    /// handler). Future sandboxes get the package via ensure_packages.
    pub async fn apply_for_agent(&self, db: &sqlx::PgPool, agent_id: Uuid) {
        let sessions: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM sessions WHERE agent_id = $1",
        )
        .bind(agent_id)
        .fetch_all(db)
        .await
        .unwrap_or_default();
        for sid in sessions {
            let root_dir = self.base_dir.join(format!("forge-{}", sid));
            if root_dir.exists() {
                Self::reconcile_packages(db, &root_dir, agent_id).await;
            }
        }
    }

    /// Remove the agent's per-session rootfs trees (mobile/CLI
    /// "reinstall sandbox"). Working trees are untouched; the next
    /// bash re-copies a clean base and the reconciler re-installs the
    /// approved set. Returns how many trees were removed.
    pub async fn reset_for_agent(&self, db: &sqlx::PgPool, agent_id: Uuid) -> usize {
        let sessions: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM sessions WHERE agent_id = $1",
        )
        .bind(agent_id)
        .fetch_all(db)
        .await
        .unwrap_or_default();
        let mut n = 0;
        for sid in sessions {
            let root_dir = self.base_dir.join(format!("forge-{}", sid));
            // Drop the registry entry so the next exec rebuilds.
            self.containers.write().await.remove(&sid);
            if root_dir.exists() {
                match tokio::fs::remove_dir_all(&root_dir).await {
                    Ok(_) => n += 1,
                    Err(e) => tracing::warn!("sandbox reset: failed to remove {:?}: {}", root_dir, e),
                }
            }
        }
        n
    }

    /// Shared reconcile body (see ensure_packages / apply_for_agent).
    ///
    /// NEVER blocks the caller: anything missing is marked
    /// `installing` in the marker and installed by a spawned task.
    /// A bash exec that races an install simply lacks the package
    /// (the agent is told to expect this on the approval path) — the
    /// previous awaited design died with the client's HTTP timeout
    /// mid-`apt-get`, half-installing the batch (lab, 2026-10-18).
    async fn reconcile_packages(db: &sqlx::PgPool, root_dir: &Path, agent_id: Uuid) {
        let approved = match crate::db::approved_packages_for_agent(db, agent_id).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("pkg reconcile: db read failed: {}", e);
                return;
            }
        };
        if approved.is_empty() {
            return;
        }
        // The rootfs must exist already (exec path ensures it); if a
        // caller beats the ensure, skip — the next exec reconciles.
        if !root_dir.join("etc/debian_version").exists() {
            return;
        }
        let marker_path = root_dir.join("etc/forge-packages.json");
        let mut marker: serde_json::Map<String, serde_json::Value> =
            tokio::fs::read(&marker_path)
                .await
                .ok()
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                .and_then(|v| v.as_object().cloned())
                .unwrap_or_default();
        let now_iso = chrono::Utc::now().to_rfc3339();
        let mut missing: Vec<String> = Vec::new();
        for p in &approved {
            match marker.get(p) {
                Some(v) if v.get("ok").and_then(|b| b.as_bool()) == Some(true) => continue,
                // an in-flight install younger than 10 minutes is
                // left alone (its task is still running; older ones
                // mean the task died — retry).
                Some(v)
                    if v.get("installing").and_then(|b| b.as_bool()) == Some(true)
                        && v.get("at")
                            .and_then(|a| a.as_str())
                            .and_then(|a| chrono::DateTime::parse_from_rfc3339(a).ok())
                            .is_some_and(|t| t > chrono::Utc::now() - chrono::Duration::minutes(10)) =>
                {
                    continue
                }
                _ => missing.push(p.clone()),
            }
        }
        if missing.is_empty() {
            return;
        }
        // Claim the install BEFORE spawning: a concurrent exec
        // re-entering here must see `installing` and pass through.
        for p in &missing {
            marker.insert(
                p.clone(),
                serde_json::json!({"ok": false, "installing": true, "at": now_iso}),
            );
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&serde_json::Value::Object(marker.clone())) {
            let _ = tokio::fs::write(&marker_path, bytes).await;
        }
        let db = db.clone();
        let root_dir = root_dir.to_path_buf();
        let root_log = root_dir.clone();
        let marker_path_c = marker_path.clone();
        let missing_c = missing.clone();
        let agent_c = agent_id;
        tokio::spawn(async move {
            let _guard = pkg_install_lock().lock().await;
            // Same per-tree lock the bash paths hold: apt/dpkg
            // unpacking while the agent's nspawn is mid-`git status`
            // on the same tree is a real fs race, not just a lock
            // formality. Order: pkg_install_lock → tree lock
            // (nothing takes them in reverse — no deadlock).
            let _tree = acquire_tree_guard(&root_dir).await;
            // DNS inside the chroot (debootstrap does not persist one).
            let _ =
                tokio::fs::copy("/etc/resolv.conf", root_dir.join("etc/resolv.conf")).await;
            // Repair a half-installed dpkg from a previously killed
            // run, then refresh lists and install (each best-effort —
            // the outcome is judged per package below).
            let _ = run_chroot(&root_dir, &["dpkg", "--configure", "-a"], 120).await;
            let _ = run_chroot(
                &root_dir,
                &["apt-get", "-f", "install", "-y", "-qq"],
                120,
            )
            .await;
            let _ = run_chroot(&root_dir, &["apt-get", "update", "-qq"], 120).await;
            let mut argv: Vec<&str> =
                vec!["apt-get", "install", "-y", "-qq", "--no-install-recommends"];
            argv.extend(missing_c.iter().map(|s| s.as_str()));
            let result = run_chroot(&root_dir, &argv, 600).await;
            let now = chrono::Utc::now().to_rfc3339();
            let mut marker: serde_json::Map<String, serde_json::Value> =
                tokio::fs::read(&marker_path_c)
                    .await
                    .ok()
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                    .and_then(|v| v.as_object().cloned())
                    .unwrap_or_default();
            // Per-package verdict via dpkg, not apt's exit code (a
            // single flaky dep must not blacklist the batch).
            let mut newly_installed: Vec<String> = Vec::new();
            for p in &missing_c {
                let installed = run_chroot(&root_dir, &["dpkg", "-s", p], 20)
                    .await
                    .map(|o| o.status.success())
                    .unwrap_or(false);
                if installed {
                    marker.insert(
                        p.clone(),
                        serde_json::json!({"ok": true, "at": now}),
                    );
                    newly_installed.push(p.clone());
                } else {
                    marker.insert(
                        p.clone(),
                        serde_json::json!({"ok": false, "at": now, "error": "apt-get install did not land the package"}),
                    );
                }
            }
            match &result {
                Ok(out) if !out.status.success() => {
                    let err = String::from_utf8_lossy(&out.stderr);
                    let tail: String = err.chars().rev().take(400).collect::<String>();
                    tracing::warn!(
                        "sandbox pkg install: apt-get {:?} exit-failed in {:?}: {}",
                        missing_c,
                        root_dir,
                        tail.chars().rev().collect::<String>()
                    );
                }
                Err(e) => tracing::warn!(
                    "sandbox pkg install: chroot spawn failed in {:?}: {}",
                    root_dir,
                    e
                ),
                _ => {}
            }
            if let Ok(bytes) =
                serde_json::to_vec_pretty(&serde_json::Value::Object(marker))
            {
                let _ = tokio::fs::write(&marker_path_c, bytes).await;
            }
            if !newly_installed.is_empty() {
                let _ = crate::db::mark_pkgs_installed(&db, agent_c, &newly_installed).await;
                tracing::info!(
                    "sandbox pkg install: {:?} landed in {:?} (agent {agent_c})",
                    newly_installed,
                    root_dir
                );
            }
        });
        tracing::info!(
            "sandbox pkg reconcile: installing {:?} into {:?} (background)",
            missing, root_log
        );
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

    #[error("Container is in invalid state: {0}")]
    InvalidState(String),
}

#[cfg(test)]
#[cfg(test)]
mod tree_lock_tests {
    use super::*;

    #[test]
    fn busy_error_detected() {
        // The verbatim nspawn 261 line (format-expanded).
        assert!(nspawn_busy_error(
            "Directory tree /forge/sandbox/forge-ddbc8cc9-2c39-43c3-95d7-d3887ccc2cb5 is currently busy."
        ));
        // A command's own stderr that happens to say 'busy' is NOT
        // the flock loss (no retry loop on agent output).
        assert!(!nspawn_busy_error("nginx: [emerg] bind() is busy"));
        assert!(!nspawn_busy_error(""));
        assert!(!nspawn_busy_error("Disk image /x is currently busy."));
    }

    #[tokio::test]
    async fn tree_lock_serializes_same_tree_only() {
        // Same path: mutually exclusive, FIFO.
        let a = tree_lock_for(std::path::Path::new("/forge/sandbox/forge-A"));
        let b = tree_lock_for(std::path::Path::new("/forge/sandbox/forge-A"));
        assert!(Arc::ptr_eq(&a, &b), "same tree must share one mutex");
        let c = tree_lock_for(std::path::Path::new("/forge/sandbox/forge-B"));
        assert!(!Arc::ptr_eq(&a, &c), "different trees must not share");

        let order = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let mut handles = Vec::new();
        for i in 0..4u32 {
            let m = tree_lock_for(std::path::Path::new("/forge/sandbox/forge-A"));
            let o = order.clone();
            handles.push(tokio::spawn(async move {
                let _g = m.lock_owned().await;
                o.lock().unwrap().push(i);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // All four ran, none overlapped (would still produce a
        // permutation here; exclusivity is what lock_owned gives us
        // — assert the observable: every task got in exactly once).
        let mut got = order.lock().unwrap().clone();
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3]);

        // Different trees run genuinely concurrently: while A is
        // held, B's guard resolves ~instantly.
        let ga = tree_lock_for(std::path::Path::new("/forge/sandbox/forge-A")).lock_owned().await;
        let t = std::time::Instant::now();
        let _gb = acquire_tree_guard(std::path::Path::new("/forge/sandbox/forge-B")).await;
        assert!(t.elapsed() < std::time::Duration::from_millis(500));
        drop(ga);
    }
}

#[cfg(test)]
mod nspawn_args_tests {
    use super::*;
    use std::path::Path;

    /// Assert the shared nspawn arg vector carries the operator env
    /// passthrough (`GITHUB_TOKEN`, `SEARCH_INSTANCE`, `SEARCH_API_KEY`)
    /// when the values are present. Regression test for the drift that
    /// previously cost the streaming bash path the `SEARCH_*` vars: the
    /// streaming path built its own nspawn command and omitted them, so
    /// a `search` run via streaming bash in a sandbox didn't see the
    /// operator's configured instance/key. Both paths now call
    /// `nspawn_args`, so this test guards the single source of truth.
    #[test]
    fn nspawn_args_includes_env_passthrough_when_set() {
        let env = ContainerEnv {
            github_token: Some("ghp_testtoken".to_string()),
            search_instance: Some("https://search.example.com".to_string()),
            search_api_key: Some("sk-search-test".to_string()),
            forge_api_key: Some("sk_forge_testtoken".to_string()),
            ..ContainerEnv::default()
        };
        let args = nspawn_args(
            Path::new("/forge/sandbox/forge-abc"),
            Path::new("/forge/sessions/abc"),
            30,
            "echo hi",
            &env,
        );
        assert!(
            args.iter()
                .any(|a| a == "--setenv=GITHUB_TOKEN=ghp_testtoken"),
            "missing GITHUB_TOKEN passthrough: {args:?}"
        );
        assert!(
            args.iter()
                .any(|a| a == "--setenv=SEARCH_INSTANCE=https://search.example.com"),
            "missing SEARCH_INSTANCE passthrough: {args:?}"
        );
        assert!(
            args.iter()
                .any(|a| a == "--setenv=SEARCH_API_KEY=sk-search-test"),
            "missing SEARCH_API_KEY passthrough: {args:?}"
        );
        assert!(
            args.iter()
                .any(|a| a == "--setenv=FORGE_API_KEY=sk_forge_testtoken"),
            "missing FORGE_API_KEY passthrough: {args:?}"
        );
    }

    /// The structural args (rootfs, working-dir bind, container env,
    /// nix config, the `timeout --kill-after bash -c` tail) are always
    /// present regardless of the operator env. Guards against a future
    /// edit accidentally dropping one of the always-on args.
    #[test]
    fn nspawn_args_extra_env_passthrough() {
        // H6.5: the agent's resolved credential slots ride the SAME
        // argv passthrough as the operator env — and nowhere else
        // (the grep-audit invariant, `tests/herd_h65_tests.rs`).
        let env = ContainerEnv {
            extra_env: vec![("BILLING_COOKIE".into(), "sk-live-abc".into())],
            ..ContainerEnv::default()
        };
        let args = nspawn_args(
            Path::new("/forge/sandbox/forge-abc"),
            Path::new("/forge/sessions/abc"),
            30,
            "echo hi",
            &env,
        );
        assert!(
            args.iter()
                .any(|a| a == "--setenv=BILLING_COOKIE=sk-live-abc"),
            "missing credential-slot passthrough: {args:?}"
        );
    }

    /// When the operator env vars are unset, no passthrough args are
    /// added (the container sees the binary's compiled-in defaults).
    #[test]
    fn nspawn_args_omits_env_passthrough_when_unset() {
        let env = ContainerEnv::default();
        let args = nspawn_args(
            Path::new("/forge/sandbox/forge-abc"),
            Path::new("/forge/sessions/abc"),
            30,
            "echo hi",
            &env,
        );
        assert!(
            !args.iter().any(|a| a.starts_with("--setenv=GITHUB_TOKEN=")),
            "GITHUB_TOKEN should be absent when unset: {args:?}"
        );
        assert!(
            !args
                .iter()
                .any(|a| a.starts_with("--setenv=SEARCH_INSTANCE=")),
            "SEARCH_INSTANCE should be absent when unset: {args:?}"
        );
    }

    /// The structural args (rootfs, working-dir bind, container env,
    /// nix config, the `timeout --kill-after bash -c` tail) are always
    /// present regardless of the operator env. Guards against a future
    /// edit accidentally dropping one of the always-on args.
    #[test]
    fn nspawn_args_always_present_structure() {
        let args = nspawn_args(
            Path::new("/root"),
            Path::new("/work"),
            42,
            "ls",
            &ContainerEnv::default(),
        );
        // rootfs + working dir
        assert!(args.contains(&"-D".to_string()));
        assert!(args.contains(&"/root".to_string()));
        assert!(args.contains(&"--bind".to_string()));
        assert!(args.contains(&"/work:/work".to_string()));
        assert!(args.contains(&"--chdir".to_string()));
        assert!(args.contains(&"/work".to_string()));
        // container env
        assert!(args
            .iter()
            .any(|a| a.starts_with("--setenv=PATH=/usr/local/sbin")));
        assert!(args
            .iter()
            .any(|a| a.starts_with("--setenv=NIX_CONFIG=experimental-features")));
        assert!(args
            .iter()
            .any(|a| a.starts_with("--setenv=NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt")));
        // the timeout tail
        assert!(args.contains(&"--".to_string()));
        assert!(args.contains(&"timeout".to_string()));
        assert!(args.contains(&"--kill-after=2".to_string()));
        assert!(args.contains(&"42s".to_string()));
        assert!(args.contains(&"bash".to_string()));
        assert!(args.contains(&"-c".to_string()));
        assert!(args.contains(&"ls".to_string()));
    }
}
