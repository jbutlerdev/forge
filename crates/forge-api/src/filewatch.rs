//! Herd H5.2 — the inotify file-change worker (wake row: "file
//! change").
//!
//! **Config** (boot-only, read once into
//! [`crate::wake::WakeConfig`]): `FORGE_FILEWATCH="path1:agentA,
//! path2:agentB"` — each entry is `<path>:<agent-ref>` split on the
//! LAST colon (agent refs — UUIDs or agent names — contain no
//! colons; paths may). The agent ref prefers the forge agent ID
//! (UUID); an agent NAME is accepted via a name→id lookup at boot
//! (unknown or ambiguous names warn and drop that watch). Malformed
//! entries warn and are skipped. The whole worker starts only when at
//! least one watch survives parsing + resolution — otherwise it is a
//! no-op (zero cost).
//!
//! **Detection.** One worker thread, one inotify instance
//! (`inotify-sys` FFI — see the dep note in the module footer).
//! Directories are watched with `IN_CREATE|IN_MODIFY|IN_MOVED_TO`;
//! plain files with `IN_MODIFY`. Per-path 2 s debounce: each event
//! resets that path's quiet timer, so a burst coalesces into exactly
//! one fire. The thread polls the inotify fd with a 1 s heartbeat, so
//! [`FileWatchHandle::stop`] joins within ~1 s (clean shutdown; the
//! thread re-reads nothing else — there is no state to recover, the
//! only durable artifact is the timer row it arms).
//!
//! **Sink** (per fire, per path):
//! 1. `BusEvent::FileChanged { path, agent_id }` on the bus (live
//!    observation; the SSE handler ignores it — no session binding);
//! 2. a one-shot DURABLE TIMER due ~0 s on the watched agent's
//!    most-active session (the `GET /agents/:id/active` rule —
//!    `last_active DESC`), via the H2.3 timer machinery
//!    (`ensure_migrated` + `harness timerSet`). The fired timer
//!    re-prompts that conversation with `[file-watch] <path> changed`
//!    — the notification lands in the agent's conversation as a real
//!    turn. The H2.3 row + atomic claim give exactly-once semantics
//!    across a forge restart.
//!
//! **Degradation.** No active session for the agent → warn-log + drop
//! (the bus event still published). Harness disabled / session not
//! harness-backed / timer RPC failed → warn-log + drop. A watch path
//! that does not exist at boot → warn + skip that path (documented:
//! create the path and restart forge). inotify is Linux-only: on
//! other platforms the worker does not start (warn).
//!
//! **Dep note:** this uses `inotify-sys` (the FFI layer of the
//! `inotify` ecosystem) rather than the `inotify` crate's
//! `Inotify::read_events_blocking`: the high-level API does not
//! expose the fd for `poll(2)`, which the 1 s stop-heartbeat needs.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::api::AppState;

/// Per-path quiet window before a fire (the debounce).
const DEBOUNCE: Duration = Duration::from_secs(2);
/// The stop-heartbeat: poll(2) timeout, bounds the join time of
/// [`FileWatchHandle::stop`] to ~1 s.
const POLL_MS: i32 = 1_000;
/// One inotify read batch.
const READ_BUF: usize = 8 * 1024;

/// The relevant event bits (the wake-matrix row).
const RELEVANT: u32 = inotify_sys::IN_CREATE | inotify_sys::IN_MODIFY | inotify_sys::IN_MOVED_TO;

/// `FORGE_FILEWATCH` parser: `"path1:agentA,path2:agentB"` →
/// `[(path, agent_ref)]`. Split on the LAST `:` per entry; malformed
/// entries (no `:`, empty path or ref) warn and are dropped. Pure
/// (no I/O) so it is directly unit-testable.
pub fn parse_watches(raw: &str) -> Vec<(PathBuf, String)> {
    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            match entry.rsplit_once(':') {
                Some((path, agent)) if !path.trim().is_empty() && !agent.trim().is_empty() => {
                    Some((PathBuf::from(path.trim()), agent.trim().to_string()))
                }
                _ => {
                    tracing::warn!(
                        entry,
                        "filewatch: malformed FORGE_FILEWATCH entry (expected <path>:<agent-id|name>); entry skipped"
                    );
                    None
                }
            }
        })
        .collect()
}

/// One configured watch (resolved at boot).
struct Watch {
    path: PathBuf,
    agent_id: Uuid,
}

/// Handle to the running worker. Drop without calling
/// [`Self::stop`] leaks nothing (the thread dies with the process),
/// but `stop` is the clean path used at shutdown.
pub struct FileWatchHandle {
    stop: Arc<AtomicBool>,
    /// Set by the worker thread once every watch is armed (or the
    /// thread has exited). Callers gate their first trigger on this
    /// so an event cannot race ahead of `inotify_add_watch`.
    armed: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl FileWatchHandle {
    /// True once the worker has finished arming every watch (or the
    /// thread has exited).
    pub fn armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    /// Signal the worker to exit and join it. Bounded by the 1 s
    /// poll heartbeat: returns within ~1 s even mid-debounce.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Start the worker when `FORGE_FILEWATCH` (on the state's wake
/// config) yields at least one resolvable watch. Returns `None` when
/// unconfigured or nothing survives boot validation.
pub async fn spawn_filewatch(state: Arc<AppState>) -> Option<FileWatchHandle> {
    let raw = match &state.wake.filewatch_raw {
        Some(r) if !r.trim().is_empty() => r.clone(),
        _ => return None,
    };
    let specs = parse_watches(&raw);
    if specs.is_empty() {
        tracing::warn!(raw = %raw, "filewatch: no valid entries in FORGE_FILEWATCH; worker not started");
        return None;
    }

    let mut watches = Vec::new();
    for (path, ref_str) in specs {
        // Agent ref: UUID preferred, name accepted via lookup.
        let agent_id: Option<Uuid> = match Uuid::parse_str(&ref_str) {
            Ok(id) => Some(id),
            Err(_) => {
                let rows: Vec<Uuid> = match sqlx::query_scalar(
                    "SELECT id FROM agents WHERE name = $1",
                )
                .bind(&ref_str)
                .fetch_all(&state.db)
                .await
                {
                    Ok(rows) => rows,
                    Err(e) => {
                        tracing::warn!(agent = %ref_str, error = %e, "filewatch: agent name lookup failed; watch dropped");
                        continue;
                    }
                };
                match rows.len() {
                    1 => Some(rows[0]),
                    0 => {
                        tracing::warn!(agent = %ref_str, "filewatch: unknown agent name; watch dropped");
                        None
                    }
                    n => {
                        tracing::warn!(agent = %ref_str, matches = n, "filewatch: ambiguous agent name; watch dropped");
                        None
                    }
                }
            }
        };
        let Some(agent_id) = agent_id else { continue };
        if !path.exists() {
            tracing::warn!(path = %path.display(), "filewatch: path does not exist at boot; watch dropped (create the path and restart forge)");
            continue;
        }
        tracing::info!(
            path = %path.display(),
            agent_id = %agent_id,
            "filewatch: watch armed (debounce {:?})",
            DEBOUNCE
        );
        watches.push(Watch { path, agent_id });
    }

    if watches.is_empty() {
        tracing::warn!("filewatch: no watch survived boot validation; worker not started");
        return None;
    }

    let handle = tokio::runtime::Handle::current();
    let stop = Arc::new(AtomicBool::new(false));
    let armed = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();
    let armed_flag = armed.clone();
    let worker_state = state.clone();
    let watch_count = watches.len();
    let join = std::thread::Builder::new()
        .name("h52-filewatch".into())
        .spawn(move || {
            worker(stop_flag, armed_flag, watches, worker_state, handle);
        })
        .expect("filewatch worker thread spawn failed");
    tracing::info!(watches = watch_count, "H5.2 file-watch worker started");
    Some(FileWatchHandle {
        stop,
        armed,
        join: Some(join),
    })
}

/// The worker loop (runs on its own std thread). See module docs.
#[cfg(unix)]
fn worker(
    stop: Arc<AtomicBool>,
    armed: Arc<AtomicBool>,
    watches: Vec<Watch>,
    state: Arc<AppState>,
    handle: tokio::runtime::Handle,
) {
    let fd = unsafe { inotify_sys::inotify_init() };
    if fd < 0 {
        tracing::error!(
            error = %std::io::Error::last_os_error(),
            "filewatch: inotify_init failed; worker exiting"
        );
        return;
    }
    let _fd_guard = FdGuard(fd);

    // Arm the watches (wd → watch index).
    let mut active: Vec<(i32, usize)> = Vec::new();
    let mut last: Vec<Option<Instant>> = vec![None; watches.len()];
    for (i, w) in watches.iter().enumerate() {
        let mask = if w.path.is_dir() {
            RELEVANT
        } else {
            inotify_sys::IN_MODIFY
        };
        let cpath = match std::ffi::CString::new(w.path.to_string_lossy().into_owned()) {
            Ok(c) => c,
            Err(_) => {
                tracing::warn!(path = %w.path.display(), "filewatch: path is not a valid C string; watch skipped");
                continue;
            }
        };
        let wd = unsafe { inotify_sys::inotify_add_watch(fd, cpath.as_ptr(), mask) };
        if wd < 0 {
            tracing::warn!(
                path = %w.path.display(),
                error = %std::io::Error::last_os_error(),
                "filewatch: inotify_add_watch failed; watch skipped"
            );
            continue;
        }
        active.push((wd, i));
    }
    if active.is_empty() {
        tracing::warn!("filewatch: no watch could be armed; worker exiting");
        armed.store(true, Ordering::Relaxed);
        return;
    }
    armed.store(true, Ordering::Relaxed);

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut pfd, 1, POLL_MS) };
        if rc > 0 {
            let mut buf = [0u8; READ_BUF];
            let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len()) };
            if n > 0 {
                for (wd, mask) in parse_events(&buf[..n as usize]) {
                    if mask & RELEVANT == 0 {
                        continue;
                    }
                    for &(active_wd, i) in &active {
                        if active_wd == wd {
                            // Reset this path's quiet timer (burst
                            // coalescing: many events ⇒ one fire after
                            // 2 s of quiet).
                            last[i] = Some(Instant::now());
                        }
                    }
                }
            } else if n < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EINTR) {
                    tracing::warn!(error = %err, "filewatch: inotify read error (continuing)");
                }
            }
        } else if rc < 0 {
            // poll error (EINTR is the common one on shutdown signal):
            // the stop flag + heartbeat keep the loop correct.
            continue;
        }

        // Fire any debounced watch whose quiet window elapsed.
        for (i, w) in watches.iter().enumerate() {
            if let Some(fired_at) = last[i] {
                if fired_at.elapsed() >= DEBOUNCE {
                    last[i] = None;
                    fire(&state, &handle, &w.path, w.agent_id);
                }
            }
        }
    }
    tracing::info!("filewatch: worker stopped");
}

/// Non-unix platforms: inotify is Linux-only. Documented no-op.
#[cfg(not(unix))]
fn worker(
    _stop: Arc<AtomicBool>,
    _armed: Arc<AtomicBool>,
    _watches: Vec<Watch>,
    _state: Arc<AppState>,
    _handle: tokio::runtime::Handle,
) {
    tracing::warn!("filewatch: inotify is Linux-only; worker not running on this platform");
}

/// One debounced fire for one watch: bus marker + the one-shot timer
/// on the agent's active session.
fn fire(state: &AppState, handle: &tokio::runtime::Handle, path: &std::path::Path, agent_id: Uuid) {
    let path_str = path.to_string_lossy().into_owned();
    tracing::info!(path = %path_str, agent_id = %agent_id, "filewatch: change detected (debounced)");
    state.bus.publish_file_changed(path_str.clone(), agent_id);
    // The timer arm is async (db + harness IPC); block on it from the
    // worker thread (we are outside any runtime context here —
    // `Handle::block_on` is the sanctioned entry point).
    handle.block_on(arm_agent_timer(state, agent_id, &path_str));
}

/// Arm the `[file-watch] <path> changed` one-shot timer (due ~0 s) on
/// the agent's most-active session — the plan's "bus event → forge
/// timer task" sink. All failure modes warn-log and drop (see module
/// docs).
async fn arm_agent_timer(state: &AppState, agent_id: Uuid, path: &str) {
    // The `GET /agents/:id/active` rule: most-recent session first.
    let session_id: Option<Uuid> = match sqlx::query_scalar(
        "SELECT id FROM sessions WHERE agent_id = $1 ORDER BY last_active DESC LIMIT 1",
    )
    .bind(agent_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(agent_id = %agent_id, error = %e, "filewatch: session lookup failed; event dropped");
            return;
        }
    };
    let Some(session_id) = session_id else {
        tracing::warn!(
            agent_id = %agent_id,
            "filewatch: agent has no active session; event dropped (bus marker already published)"
        );
        return;
    };
    let conversation =
        match crate::harness_migration::ensure_migrated(state, session_id, None).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    session_id = %session_id,
                    error = %e,
                    "filewatch: session is not harness-backed; timer not armed"
                );
                return;
            }
        };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let prompt = format!("[file-watch] {path} changed");
    match state
        .harness
        .client()
        .timer_set(conversation, Some(now_ms), None, &prompt)
        .await
    {
        Ok(timer_id) => {
            tracing::info!(
                agent_id = %agent_id,
                session_id = %session_id,
                %timer_id,
                "filewatch: one-shot timer armed on the agent's active session"
            );
        }
        Err(e) => {
            tracing::warn!(
                session_id = %session_id,
                error = %e,
                "filewatch: timer arm failed (harness down?); event dropped"
            );
        }
    }
}

/// Close the inotify fd exactly once, on every exit path.
struct FdGuard(i32);
impl Drop for FdGuard {
    fn drop(&mut self) {
        unsafe { inotify_sys::close(self.0) };
    }
}

/// Parse a `read(2)` buffer of inotify events. The kernel delivers
/// back-to-back `inotify_event` records (16-byte header + a
/// NUL-padded name), each aligned to a 4-byte stride: `16 + (len + 3)
/// & !3`. A partial tail (should not happen for a full read) is
/// dropped, not truncated-misread.
fn parse_events(buf: &[u8]) -> Vec<(i32, u32)> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 16 <= buf.len() {
        let ev = unsafe {
            std::ptr::read_unaligned(buf[off..].as_ptr() as *const inotify_sys::inotify_event)
        };
        let name_len = ev.len as usize;
        if off + 16 + name_len > buf.len() {
            break; // partial tail: stop
        }
        out.push((ev.wd, ev.mask));
        off += 16 + ((name_len + 3) & !3);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_watches_happy_path() {
        let specs = parse_watches("/srv/a:agent-1,/srv/b/x:11111111-1111-4111-8111-111111111111 ,");
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].0, PathBuf::from("/srv/a"));
        assert_eq!(specs[0].1, "agent-1");
        assert_eq!(specs[1].0, PathBuf::from("/srv/b/x"));
        assert_eq!(specs[1].1, "11111111-1111-4111-8111-111111111111");
    }

    #[test]
    fn parse_watches_malformed_entries_dropped() {
        // No colon, empty agent, empty path — all dropped; the one
        // good entry survives.
        let specs = parse_watches("no-colon, :agent, : ,/ok/path:agent-2");
        assert_eq!(
            specs,
            vec![(PathBuf::from("/ok/path"), "agent-2".to_string())]
        );
    }

    #[test]
    fn parse_watches_split_on_last_colon() {
        // Agent refs never contain ':'; paths may (e.g. C-style
        // volumes). The LAST colon is the separator.
        let specs = parse_watches("/mnt/vol:1:agent-9");
        assert_eq!(
            specs,
            vec![(PathBuf::from("/mnt/vol:1"), "agent-9".to_string())]
        );
    }

    #[test]
    fn parse_events_rebuilds_a_two_event_buffer() {
        // Hand-build two inotify_event records exactly as the kernel
        // lays them out: 16-byte header + NUL-padded name, 4-byte
        // stride.
        use std::mem::size_of;
        let name1 = b"new.txt";
        let name2 = b"dir";
        let stride = |len: usize| 16 + (len + 3) & !3;
        let total = stride(name1.len()) + stride(name2.len());
        let mut buf = vec![0u8; total];
        // event 1: wd=7, mask=IN_MODIFY, cookie=0, len, name
        let h1 = size_of::<inotify_sys::inotify_event>(); // 16
        assert_eq!(h1, 16, "inotify_event header layout is stable on Linux");
        buf[..4].copy_from_slice(&7i32.to_ne_bytes());
        buf[4..8].copy_from_slice(&inotify_sys::IN_MODIFY.to_ne_bytes());
        buf[8..12].copy_from_slice(&0u32.to_ne_bytes());
        buf[12..16].copy_from_slice(&(name1.len() as u32).to_ne_bytes());
        buf[16..16 + name1.len()].copy_from_slice(name1);
        // event 2 at the stride offset
        let off2 = stride(name1.len());
        buf[off2..off2 + 4].copy_from_slice(&7i32.to_ne_bytes());
        buf[off2 + 4..off2 + 8].copy_from_slice(&inotify_sys::IN_CREATE.to_ne_bytes());
        buf[off2 + 8..off2 + 12].copy_from_slice(&0u32.to_ne_bytes());
        buf[off2 + 12..off2 + 16].copy_from_slice(&(name2.len() as u32).to_ne_bytes());
        buf[off2 + 16..off2 + 16 + name2.len()].copy_from_slice(name2);

        let events = parse_events(&buf);
        assert_eq!(
            events,
            vec![(7, inotify_sys::IN_MODIFY), (7, inotify_sys::IN_CREATE)]
        );
    }
}
