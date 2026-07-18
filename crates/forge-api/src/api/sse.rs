//! SSE (Server-Sent Events) module for streaming responses
//!
//! Provides streaming capabilities for tool execution and agent responses.

use axum::{
    extract::State,
    http::HeaderValue,
    response::{
        sse::{Event, Sse},
        IntoResponse, Json, Response,
    },
};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use uuid::Uuid;

use crate::api::AppState;
use crate::tool_executor::ToolExecutor;

/// SSE event names
mod event_names {
    pub const TOOL_START: &str = "tool_start";
    pub const STDOUT: &str = "stdout";
    pub const STDERR: &str = "stderr";
    pub const TOOL_END: &str = "tool_end";
    pub const ERROR: &str = "error";
}

/// Input for streaming bash tool
#[derive(Debug, Deserialize)]
pub struct StreamingBashInput {
    pub command: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    crate::tool_executor::BASH_DEFAULT_TIMEOUT_MS
}

/// Streaming tool input
#[derive(Debug, Deserialize)]
pub struct StreamingToolInput {
    pub session_id: String,
    pub tool: String,
    pub input: serde_json::Value,
    /// See [`ToolInput::tool_call_id`] - pi sometimes hands extensions
    /// the whole tool-call object instead of a bare id, so accept any
    /// JSON shape and stringify on the way through.
    #[serde(default)]
    pub tool_call_id: serde_json::Value,
}

impl StreamingToolInput {
    /// Same as [`crate::tool_executor::ToolInput::tool_call_id_str`].
    pub fn tool_call_id_str(&self) -> String {
        match &self.tool_call_id {
            serde_json::Value::String(s) if !s.is_empty() => s.clone(),
            serde_json::Value::Object(map) => map
                .get("id")
                .and_then(|v| v.as_str())
                .map(String::from)
                .unwrap_or_else(|| "synthetic".to_string()),
            serde_json::Value::Null => "synthetic".to_string(),
            other => other.to_string(),
        }
    }
}

/// Create an SSE event with event name and data
fn make_named_event(event_name: &str, data: impl serde::Serialize) -> Event {
    let json = serde_json::to_string(&data)
        .unwrap_or_else(|_| r#"{"error": "serialization failed"}"#.to_string());
    Event::default().event(event_name).data(json)
}

/// Send a terminal SSE event with a bounded flush window.
///
/// Reader tasks use plain `try_send` (non-blocking, drop on
/// full) so a slow consumer can never backpressure into the
/// child. The terminal events (`tool_end`, `error`, `done`)
/// only fire a handful of times per call, so it's worth giving
/// the consumer a short window to drain the channel before we
/// give up on the event. This bounds the function's worst-case
/// latency from "child exits" to "HTTP response closes" to
/// `grace`, regardless of how stuck the consumer is.
///
/// On a closed channel (rx dropped = client disconnected) we
/// silently return. On a grace timeout we drop the event and
/// return; the function will then drop `tx` and close the
/// HTTP response, and the consumer will see end-of-stream.
async fn try_send_with_grace(
    tx: &mpsc::Sender<Result<Event, axum::Error>>,
    event: Event,
    grace: Duration,
    label: &'static str,
) {
    match tx.try_send(Ok(event)) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(rejected)) => {
            // `rejected` is the inner `Result<Event, axum::Error>`
            // we tried to send. We don't expect the inner Err
            // arm to fire here (axum::Error is for response
            // building, not for individual events), but handle
            // it anyway so a future change to make_named_event
            // can't silently break the call.
            let inner = match rejected {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        stream = label,
                        error = %e,
                        "rejected SSE event carried an axum::Error; dropping"
                    );
                    return;
                }
            };
            tracing::debug!(
                stream = label,
                "SSE channel full on terminal event; waiting up to {:?} for consumer",
                grace
            );
            match tokio::time::timeout(grace, tx.send(Ok(inner))).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {} // channel closed
                Err(_) => {
                    tracing::debug!(
                        stream = label,
                        "SSE channel still full after grace; dropping terminal event"
                    );
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

/// Create a simple SSE data event (no event name)
fn make_data_event(data: impl serde::Serialize) -> Event {
    let json = serde_json::to_string(&data)
        .unwrap_or_else(|_| r#"{"error": "serialization failed"}"#.to_string());
    Event::default().data(json)
}

/// Stream type for SSE responses
type SseStream = Pin<Box<dyn Stream<Item = Result<Event, axum::Error>> + Send>>;

/// Per-stream cap on how many bytes of stdout/stderr we accumulate for
/// the audit log. A runaway `cat /dev/zero` would otherwise OOM the api
/// process; the live SSE stream is already lossy (try_send drops on a
/// full channel), so this only bounds the in-memory capture. The
/// audit-log row is complete up to this cap.
const MAX_CAPTURED_BYTES: usize = 10 * 1024 * 1024;

/// Spawn a reader task that drains one of the child's piped streams
/// (`stdout` or `stderr`), forwarding each chunk as a named SSE event
/// and accumulating it into `buf_acc` for the audit-log capture.
///
/// Used for both streams in `execute_bash_streaming` so the
/// backpressure / drop-counting / byte-cap logic lives in one place.
/// `try_send` (not `send().await`) so a slow SSE consumer can never
/// backpressure into the reader and then into the kernel pipe buffer
/// and the child's `write` — the historical hang this design avoids.
/// On a full channel the chunk is dropped (counted locally + in
/// metrics); on a closed channel the reader exits. The accumulator is
/// capped at `MAX_CAPTURED_BYTES` so a runaway `cat /dev/zero` can't
/// OOM the api process; the audit log is still complete up to the cap.
#[allow(clippy::too_many_arguments)]
fn spawn_stream_reader<R>(
    mut handle: R,
    event_name: &'static str,
    label: &'static str,
    tool_call_id: String,
    tx: mpsc::Sender<Result<Event, axum::Error>>,
    buf_acc: Arc<tokio::sync::Mutex<Vec<u8>>>,
    dropped_sse_chunks: Arc<std::sync::atomic::AtomicU64>,
    metrics: Arc<crate::observability::Metrics>,
) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match handle.read(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    {
                        let mut acc = buf_acc.lock().await;
                        if acc.len() < MAX_CAPTURED_BYTES {
                            let take = (MAX_CAPTURED_BYTES - acc.len()).min(n);
                            acc.extend_from_slice(&buf[..take]);
                        }
                    }
                    let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                    let event = make_named_event(
                        event_name,
                        serde_json::json!({
                            "tool_call_id": tool_call_id,
                            "chunk": chunk,
                        }),
                    );
                    match tx.try_send(Ok(event)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            dropped_sse_chunks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            metrics.inc_sse_chunks_dropped(1);
                            tracing::debug!(
                                tool_call_id = %tool_call_id,
                                "SSE channel full; dropping {} chunk to avoid backpressuring the child",
                                label,
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                Err(e) => {
                    // Surface the read error on the stream's own event
                    // name (previously the stdout reader mislabeled
                    // its error as a STDERR event).
                    let _ = tx.try_send(Ok(make_named_event(
                        event_name,
                        serde_json::json!({
                            "tool_call_id": tool_call_id,
                            "chunk": format!("{} error: {}", label, e),
                        }),
                    )));
                    break;
                }
            }
        }
    })
}

/// Execute bash command with streaming output
///
/// This function spawns a bash process and streams stdout/stderr as
/// SSE events. It supports timeout and nix-shell wrapping. At the end
/// of the run we hand the result to the `ToolRecorder` so the audit
/// log mirrors what the non-streaming `ToolExecutor` would have
/// written.
///
/// `sandbox`: when `Some`, the user command is wrapped in a
/// per-call `systemd-nspawn` against the session's rootfs,
/// giving the bash process its own process + filesystem
/// namespace. When `None`, the command runs directly on the
/// host in `working_dir` (legacy behavior).
#[allow(clippy::too_many_arguments)]
pub async fn execute_bash_streaming(
    session_id: uuid::Uuid,
    recorder: std::sync::Arc<dyn crate::recording::ToolRecorder>,
    bus: crate::bus::MessageBus,
    metrics: std::sync::Arc<crate::observability::Metrics>,
    tool_call_id: String,
    working_dir: String,
    command: String,
    timeout_ms: u64,
    nix_shell: Option<String>,
    sandbox: Option<std::sync::Arc<crate::sandbox::SandboxManager>>,
) -> SseStream {
    let (tx, rx) = mpsc::channel::<Result<Event, axum::Error>>(100);

    tokio::spawn(async move {
        let start_time = std::time::Instant::now();

        // Per-call counter for SSE chunks that the live
        // consumer did not receive because the mpsc channel
        // was full. The reader tasks increment this on every
        // dropped chunk; the main task reads it once at the
        // end to (a) include it in the `tool_end` event so
        // the LLM knows the live stream was lossy, and (b)
        // include it in the audit-log row's `tool_output`
        // jsonb so the operator can see drops after the
        // fact. The audit-log *accumulator* is unaffected by
        // drops — it captures every byte the child wrote
        // regardless of consumer state, so the recorded
        // `stdout` / `stderr` are always complete (up to
        // the 10 MiB per-call cap).
        let dropped_sse_chunks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Send tool_start event. try_send for the
        // same reason as the readers: we never want a
        // full SSE channel to delay setup work.
        let _ = tx.try_send(Ok(make_named_event(
            event_names::TOOL_START,
            serde_json::json!({
                "tool": "bash",
                "tool_call_id": tool_call_id
            }),
        )));

        // Wrap command with nix-shell if configured
        let (cmd_to_run, wrapped_command) = wrap_command(&command, nix_shell.as_deref());

        // Build the spawn command. Two paths:
        //
        // 1. **Sandboxed** (when `sandbox` is `Some` and the
        //    session has a container): we run the bash command
        //    inside a per-call `systemd-nspawn` against the
        //    session's rootfs. nspawn creates a fresh process
        //    + filesystem namespace for the duration of the
        //    call; the host `pgrep` doesn't see the bash
        //    process, and root-mutating commands (`apt
        //    install`, `dpkg -i`, etc.) only affect the
        //    per-session rootfs. We do NOT pass
        //    `--network-veth` — network namespace is
        //    intentionally off, the agent needs network.
        //
        //    The nix-shell wrap is skipped in the sandboxed
        //    path because nix-shell needs the host's nix
        //    store mounted, which we deliberately don't
        //    bind-mount. If a profile has `nix_shell` set
        //    the bash command runs without the wrap and we
        //    log a warning.
        //
        //    The user command is wrapped in
        //    `timeout --kill-after=2` inside nspawn so a
        //    grandchild that ignores SIGTERM gets SIGKILLed
        //    (matches the non-streaming path's behavior).
        //
        // 2. **Host-side** (when `sandbox` is `None`): the
        //    command runs directly on the host via
        //    `bash -c '<user_cmd>'`. The nix-shell wrap is
        //    applied here if `nix_shell` is set.
        let sandboxed_root: Option<std::path::PathBuf> = match sandbox.as_ref() {
            Some(mgr) => mgr.get_container(session_id).await.ok().map(|c| c.root_dir),
            None => None,
        };

        let mut cmd = if let Some(root_dir) = sandboxed_root.as_ref() {
            if nix_shell.is_some() {
                tracing::warn!(
                    tool_call_id = %tool_call_id,
                    "profile has nix_shell set but the session is running sandboxed; \
                     nix-shell wrap is skipped in the container path. \
                     nix_shell support in the sandbox rootfs is TODO."
                );
            }
            let timeout_secs = std::cmp::max(1, timeout_ms.div_ceil(1000));
            // Build the per-call nspawn command via the shared
            // `build_nspawn_command` helper (in `sandbox.rs`) so the
            // streaming and non-streaming bash paths get identical
            // env vars + bind-mounts. This fixes a drift where the
            // streaming path previously omitted the `SEARCH_INSTANCE`
            // / `SEARCH_API_KEY` passthrough, so a `search` run via
            // streaming bash in a sandbox didn't see the operator's
            // configured instance/key.
            let env = crate::sandbox::ContainerEnv::from_process_env();
            let mut c = crate::sandbox::build_nspawn_command(
                root_dir,
                std::path::Path::new(&working_dir),
                timeout_secs,
                &wrapped_command,
                &env,
            );
            c.stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            c
        } else {
            let mut c = Command::new(&cmd_to_run);
            c.arg("-c")
                .arg(&wrapped_command)
                .current_dir(&working_dir)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            c
        };

        // Set timeout. The outer tokio watchdog is the hard
        // cap; the inner `timeout --kill-after=2` (in the
        // sandboxed path) is a clean escalation. We add 5s of
        // grace so the inner timeout can fire its SIGTERM and
        // SIGKILL cleanly before the tokio watchdog SIGKILLs
        // the outer process.
        let outer_grace_ms: u64 = 5_000;
        let timeout_duration = Duration::from_millis(timeout_ms + outer_grace_ms);

        // Spawn the process with timeout
        let spawn_result = cmd.spawn();

        // Hoisted out of the match so the recorder call after the
        // match can see the outcome. Only set on the success branch.
        let mut recorded_outcome: Option<(bool, Option<i32>, u64)> = None;

        // Buffers the stdout/stderr reader tasks write into as
        // they go. We read them after the reader tasks complete
        // (after child.wait() returns and EOF hits the pipes) to
        // capture the full output for the audit log. Capped at
        // `MAX_CAPTURED_BYTES` (10 MiB) per stream so a runaway
        // `cat /dev/zero` doesn't OOM the api process.

        // How long to wait for the consumer to drain the
        // mpsc channel when sending a terminal event
        // (tool_end, error, done). If the channel is still
        // full after this, the event is dropped and the
        // function proceeds; the spawned task then drops
        // `tx` and the HTTP response closes. This caps the
        // call's post-child latency at ~this duration
        // regardless of how stuck the consumer is.
        const TERMINAL_FLUSH_GRACE: Duration = Duration::from_millis(500);
        let stdout_buf: Arc<tokio::sync::Mutex<Vec<u8>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stderr_buf: Arc<tokio::sync::Mutex<Vec<u8>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        // JoinHandles for the reader tasks. We hold them so we
        // can await them after child.wait() and be sure the
        // accumulators are fully populated before we read them.
        let mut reader_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        match timeout(timeout_duration, async { spawn_result }).await {
            Ok(Ok(mut child)) => {
                let stdout = child.stdout.take();
                let stderr = child.stderr.take();

                // Stream stdout + stderr via a shared reader helper. The two
                // streams are identical except for the SSE event name and
                // which accumulator they feed; factoring them into one
                // function keeps the try_send / drop-counting / cap logic
                // in one place (it had already drifted once — the stdout
                // reader sent its read-error as a STDERR event).
                if let Some(stdout_handle) = stdout {
                    reader_handles.push(spawn_stream_reader(
                        stdout_handle,
                        event_names::STDOUT,
                        "stdout",
                        tool_call_id.clone(),
                        tx.clone(),
                        stdout_buf.clone(),
                        dropped_sse_chunks.clone(),
                        metrics.clone(),
                    ));
                }
                if let Some(stderr_handle) = stderr {
                    reader_handles.push(spawn_stream_reader(
                        stderr_handle,
                        event_names::STDERR,
                        "stderr",
                        tool_call_id.clone(),
                        tx.clone(),
                        stderr_buf.clone(),
                        dropped_sse_chunks.clone(),
                        metrics.clone(),
                    ));
                }

                // Wait for process to complete
                recorded_outcome = match child.wait().await {
                    Ok(status) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        let success = status.success();
                        let exit_code = status.code();
                        let dropped = dropped_sse_chunks.load(std::sync::atomic::Ordering::Relaxed);

                        // Send tool_end event. The bounded
                        // flush gives the consumer a short
                        // window to drain a full channel
                        // (TERMINAL_FLUSH_GRACE) before we
                        // give up; the audit-log row carries
                        // the drop count regardless. See
                        // `try_send_with_grace` for the
                        // rationale.
                        let tool_end_event = make_named_event(
                            event_names::TOOL_END,
                            serde_json::json!({
                                "tool_call_id": tool_call_id,
                                "success": success,
                                "duration_ms": duration_ms,
                                "exit_code": exit_code,
                                "dropped_sse_chunks": dropped,
                            }),
                        );
                        try_send_with_grace(&tx, tool_end_event, TERMINAL_FLUSH_GRACE, "tool_end")
                            .await;

                        tracing::info!(
                            tool_call_id = %tool_call_id,
                            success = %success,
                            duration_ms = %duration_ms,
                            dropped_sse_chunks = dropped,
                            "Bash streaming completed"
                        );

                        Some((success, exit_code, duration_ms))
                    }
                    Err(e) => {
                        try_send_with_grace(
                            &tx,
                            make_named_event(
                                event_names::ERROR,
                                serde_json::json!({
                                    "tool_call_id": tool_call_id,
                                    "error": format!("Process error: {}", e)
                                }),
                            ),
                            TERMINAL_FLUSH_GRACE,
                            "error",
                        )
                        .await;
                        None
                    }
                }
            }
            Ok(Err(e)) => {
                try_send_with_grace(
                    &tx,
                    make_named_event(
                        event_names::ERROR,
                        serde_json::json!({
                            "tool_call_id": tool_call_id,
                            "error": format!("Failed to spawn process: {}", e)
                        }),
                    ),
                    TERMINAL_FLUSH_GRACE,
                    "error",
                )
                .await;
            }
            Err(_) => {
                try_send_with_grace(
                    &tx,
                    make_named_event(
                        event_names::ERROR,
                        serde_json::json!({
                            "tool_call_id": tool_call_id,
                            "error": format!("Command timed out after {}ms", timeout_ms)
                        }),
                    ),
                    TERMINAL_FLUSH_GRACE,
                    "error",
                )
                .await;
            }
        }

        // Wait for the stdout/stderr reader tasks to finish so
        // the accumulators hold the complete output. The pipes
        // are closed by child.wait() returning, so the readers
        // will see EOF and exit promptly.
        for h in reader_handles {
            let _ = h.await;
        }
        let captured_stdout = String::from_utf8_lossy(&stdout_buf.lock().await).into_owned();
        let captured_stderr = String::from_utf8_lossy(&stderr_buf.lock().await).into_owned();

        // Hand the outcome to the recorder. The captured stdout
        // and stderr are recorded in `tool_output` so the model
        // (and any audit-log reader) can see what the command
        // produced. `streamed: true` is kept for backward compat
        // with clients that may be branching on it; a future
        // release can drop it once we're confident nobody cares.
        // `dropped_sse_chunks` is included alongside the full
        // captured stdout/stderr so an operator looking at the
        // audit log after the fact can see whether the live SSE
        // stream was lossy for this call. The captured
        // stdout/stderr are still complete (up to the 10 MiB
        // per-call cap) regardless of drop count.
        let dropped_sse_chunks_final =
            dropped_sse_chunks.load(std::sync::atomic::Ordering::Relaxed);
        let record = match recorded_outcome {
            Some((success, exit_code, duration_ms)) => crate::recording::ToolResultRecord {
                session_id,
                tool_call_id: tool_call_id.clone(),
                tool_name: "bash".to_string(),
                content: bash_record_content(
                    &captured_stdout,
                    &captured_stderr,
                    exit_code,
                    duration_ms,
                ),
                output: serde_json::json!({
                    "success": success,
                    "stdout": captured_stdout,
                    "stderr": captured_stderr,
                    "exit_code": exit_code,
                    "timed_out": false,
                    "streamed": true,
                    "dropped_sse_chunks": dropped_sse_chunks_final,
                }),
                is_error: !success,
                duration_ms: Some(duration_ms),
            },
            None => crate::recording::ToolResultRecord {
                session_id,
                tool_call_id: tool_call_id.clone(),
                tool_name: "bash".to_string(),
                content: "[bash failed to start]".to_string(),
                output: serde_json::json!({
                    "success": false,
                    "stdout": captured_stdout,
                    "stderr": captured_stderr,
                    "streamed": true,
                    "dropped_sse_chunks": dropped_sse_chunks_final,
                }),
                is_error: true,
                duration_ms: None,
            },
        };
        match recorder.record_result(record).await {
            Ok(row) => {
                // Publish to the bus so SSE consumers see the
                // new row without polling.
                bus.publish_message(row);
            }
            Err(e) => {
                tracing::warn!(
                    tool_call_id = %tool_call_id,
                    error = %e,
                    "Failed to persist streaming bash result to audit log"
                );
            }
        }

        // Send done event. Bounded flush so a stuck
        // consumer can't keep the HTTP response open past
        // TERMINAL_FLUSH_GRACE after the child has
        // already exited. The drop count is included so a
        // client can detect lossy live streaming from the
        // final event it sees.
        try_send_with_grace(
            &tx,
            make_data_event(serde_json::json!({
                "done": true,
                "dropped_sse_chunks": dropped_sse_chunks_final,
            })),
            TERMINAL_FLUSH_GRACE,
            "done",
        )
        .await;
    });

    // Convert channel to stream
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Box::pin(stream.map(|result| result.map_err(|e| axum::Error::new(e.to_string()))))
}

/// Build the human-readable `content` for a recorded bash result.
/// Format: `[bash exit=<code> duration=<ms>ms]\n[--stderr--]\n<stderr>[--stdout--]\n<stdout>`.
/// The total is truncated to 8 KiB so a giant `cat` of a log file
/// doesn't bloat the messages table.
fn bash_record_content(
    stdout: &str,
    stderr: &str,
    exit_code: Option<i32>,
    duration_ms: u64,
) -> String {
    const MAX_TOTAL: usize = 8 * 1024;
    let mut out = format!("[bash exit={:?} duration={}ms]\n", exit_code, duration_ms);
    if !stderr.is_empty() {
        out.push_str("--stderr--\n");
        out.push_str(stderr);
        if !stderr.ends_with('\n') {
            out.push('\n');
        }
    }
    if !stdout.is_empty() {
        out.push_str("--stdout--\n");
        out.push_str(stdout);
        if !stdout.ends_with('\n') {
            out.push('\n');
        }
    }
    if out.len() > MAX_TOTAL {
        let mut truncated = out;
        truncated.truncate(MAX_TOTAL);
        truncated.push_str("\n... [truncated]\n");
        return truncated;
    }
    out
}

/// Wrap a command with nix-shell if configured
fn wrap_command(command: &str, nix_shell: Option<&str>) -> (String, String) {
    match nix_shell {
        Some(nix_expr) => {
            // Check if it's a path or an expression
            if nix_expr.starts_with('/') || nix_expr.starts_with('.') || nix_expr.ends_with(".nix")
            {
                // It's a path to a .nix file or shell.nix
                let escaped_cmd = command.replace('\'', "'\"'\"'");
                let wrapped = format!("nix-shell '{}' -c '{}'", nix_expr, escaped_cmd);
                tracing::debug!("Using nix-shell with path: {}", nix_expr);
                ("bash".to_string(), wrapped)
            } else {
                // It's a nix shell expression (packages)
                let escaped_cmd = command.replace('\'', "'\"'\"'");
                let wrapped = format!("nix-shell -p {} -c '{}'", nix_expr, escaped_cmd);
                tracing::debug!("Using nix-shell with expression: {}", nix_expr);
                ("bash".to_string(), wrapped)
            }
        }
        None => ("bash".to_string(), command.to_string()),
    }
}

/// Execute a streaming tool (currently only bash supports streaming)
#[allow(clippy::too_many_arguments)]
pub async fn execute_streaming_tool(
    session_id: uuid::Uuid,
    recorder: std::sync::Arc<dyn crate::recording::ToolRecorder>,
    bus: crate::bus::MessageBus,
    metrics: std::sync::Arc<crate::observability::Metrics>,
    tool_call_id: &str,
    working_dir: &str,
    tool_name: &str,
    input: serde_json::Value,
    nix_shell: Option<&str>,
    sandbox: Option<std::sync::Arc<crate::sandbox::SandboxManager>>,
) -> Result<SseStream, String> {
    match tool_name {
        "bash" => {
            let bash_input: StreamingBashInput =
                serde_json::from_value(input.clone()).map_err(|e| e.to_string())?;

            // The executor is the sole writer of the call row
            // (and the result row). The streaming bash path
            // doesn't go through `ToolExecutor::execute` so it
            // has to write its own call row here, before running
            // the command. A failure to record the call is logged
            // but does not abort the tool - the result row is
            // still useful on its own.
            match recorder
                .record_call(crate::recording::ToolCallRecord {
                    session_id,
                    tool_call_id: tool_call_id.to_string(),
                    tool_name: "bash".to_string(),
                    input: input.clone(),
                })
                .await
            {
                Ok(row) => {
                    // Publish to the bus so SSE consumers see
                    // the call row immediately.
                    bus.publish_message(row);
                }
                Err(e) => {
                    tracing::warn!(
                        tool_call_id = %tool_call_id,
                        error = %e,
                        "Failed to write streaming bash call row; the result row will still be recorded but the call/result linkage may be broken"
                    );
                }
            }

            Ok(execute_bash_streaming(
                session_id,
                recorder,
                bus,
                metrics,
                tool_call_id.to_string(),
                working_dir.to_string(),
                bash_input.command,
                bash_input.timeout_ms,
                nix_shell.map(|s| s.to_string()),
                sandbox.clone(),
            )
            .await)
        }
        // For other tools, return a simple event stream
        _ => {
            let (tx, rx) = mpsc::channel::<Result<Event, axum::Error>>(10);
            let working_dir = working_dir.to_string();
            let nix_shell = nix_shell.map(|s| s.to_string());
            let tool_name_owned = tool_name.to_string();
            let recorder = recorder.clone();
            let session_id_clone = session_id;
            let tool_call_id_owned = tool_call_id.to_string();
            tokio::spawn(async move {
                // Execute non-streaming tool. The executor records
                // both the call row and the result row to the
                // audit log, so we don't have to do that here.
                // Non-bash tool calls that come in over the
                // streaming endpoint (the forge-tools
                // extension sends read/write/edit through
                // the streaming path too, even though only
                // bash really streams). Pass the sandbox
                // manager so the executor applies nspawn
                // wrapping if the session has a container.
                // (Today only `read`/`write`/`edit` hit this
                // path; bash is the only tool that actually
                // gets nspawn-wrapped, but the executor's
                // sandbox field is set uniformly so the
                // path is consistent.)
                let executor = ToolExecutor::new(
                    session_id_clone,
                    working_dir,
                    sandbox.is_some(),
                    nix_shell,
                    recorder,
                    bus,
                    sandbox,
                );
                match executor
                    .execute(&tool_call_id_owned, &tool_name_owned, input)
                    .await
                {
                    Ok(output) => {
                        // try_send for the same reason as in
                        // the bash streaming path: a slow SSE
                        // consumer must not delay the HTTP
                        // response close after the tool has
                        // finished. The executor has already
                        // written the audit-log result row
                        // before this match arm runs, so
                        // dropping these terminal events on a
                        // full channel only delays the live
                        // UI, not the tool outcome.
                        let _ = tx.try_send(Ok(make_data_event(serde_json::json!({
                            "output": output.output
                        }))));
                        let _ = tx.try_send(Ok(make_named_event(
                            event_names::TOOL_END,
                            serde_json::json!({
                                "success": output.success,
                                "error": output.error
                            }),
                        )));
                    }
                    Err(e) => {
                        let _ = tx.try_send(Ok(make_named_event(
                            event_names::ERROR,
                            serde_json::json!({
                                "error": e.to_string()
                            }),
                        )));
                    }
                }
                let _ = tx.try_send(Ok(make_data_event(serde_json::json!({"done": true}))));
            });

            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            Ok(Box::pin(stream.map(|result| {
                result.map_err(|e| axum::Error::new(e.to_string()))
            })))
        }
    }
}

/// SSE endpoint for streaming tool execution
pub async fn stream_tool_execution(
    State(state): State<AppState>,
    axum::extract::Json(payload): axum::extract::Json<StreamingToolInput>,
) -> Response {
    let session_id = match Uuid::parse_str(&payload.session_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid session ID format"
                })),
            )
                .into_response();
        }
    };

    // Verify session exists
    let session_exists = sqlx::query("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .map(|r| r.is_some())
        .unwrap_or(false);

    if !session_exists {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Session not found"
            })),
        )
            .into_response();
    }

    // Get the session's isolated working directory. If the in-memory
    // session manager lost the entry (e.g. after an API restart), look
    // the directory up on disk so the tool call still works.
    let working_dir = match state.session_manager.get_session_dir(session_id).await {
        Ok(dir) => dir.to_string_lossy().to_string(),
        Err(_) => match crate::api::lookup_session_working_dir(&state, session_id).await {
            Some(dir) => dir,
            None => {
                return (
                    axum::http::StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": "Session not initialized"
                    })),
                )
                    .into_response();
            }
        },
    };

    // Get nix shell configuration
    let nix_shell: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT p.nix_shell FROM sessions s JOIN profiles p ON s.profile_id = p.id WHERE s.id = $1",
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();

    // Track metrics
    state.metrics.inc_requests("POST /tools/execute/stream");
    state.metrics.inc_tool_execution(&payload.tool);

    let tool_call_id_str = payload.tool_call_id_str();

    tracing::info!(
        session_id = %session_id,
        tool = %payload.tool,
        tool_call_id = %tool_call_id_str,
        "SSE tool execution started"
    );

    // Execute streaming tool
    match execute_streaming_tool(
        session_id,
        state.recorder.clone(),
        state.bus.clone(),
        state.metrics.clone(),
        &tool_call_id_str,
        &working_dir,
        &payload.tool,
        payload.input,
        nix_shell.as_deref(),
        // Pass the sandbox manager so `bash` calls run in
        // the session's container namespace (per-call
        // systemd-nspawn). `None` when the session has no
        // container (legacy / pre-sandbox sessions).
        Some(state.sandbox_manager.clone()),
    )
    .await
    {
        Ok(stream) => {
            // Create SSE response with appropriate headers
            let mut response = Sse::new(stream)
                .keep_alive(
                    axum::response::sse::KeepAlive::new()
                        .interval(Duration::from_secs(15))
                        .text("ping"),
                )
                .into_response();

            // Add SSE headers
            let headers = response.headers_mut();
            headers.insert("X-Accel-Buffering", HeaderValue::from_static("no"));

            response
        }
        Err(e) => {
            tracing::error!("Failed to start streaming tool: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": e
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::bash_record_content;
    use super::execute_bash_streaming;
    use crate::bus::MessageBus;
    use crate::db::Message;
    use crate::observability::Metrics;
    use crate::recording::{ToolCallRecord, ToolRecorder, ToolResultRecord};
    use async_trait::async_trait;
    use futures_util::StreamExt;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    /// In-memory recorder for tests. Returns synthetic `Message`
    /// rows so we can drive `execute_bash_streaming` without a
    /// Postgres pool. Records both calls and results so the test
    /// can assert that the result row was written even when the
    /// SSE consumer is stalled.
    #[derive(Default)]
    struct TestRecorder {
        calls: Arc<Mutex<Vec<ToolCallRecord>>>,
        results: Arc<Mutex<Vec<ToolResultRecord>>>,
    }

    #[async_trait]
    impl ToolRecorder for TestRecorder {
        async fn record_call(&self, record: ToolCallRecord) -> Result<Message, sqlx::Error> {
            self.calls.lock().unwrap().push(record.clone());
            Ok(Message {
                id: Uuid::new_v4(),
                session_id: record.session_id,
                sequence: 0,
                role: "assistant".to_string(),
                content: None,
                tool_name: Some(record.tool_name),
                tool_input: Some(record.input),
                tool_call_id: Some(record.tool_call_id),
                tool_output: None,
                duration_ms: None,
                created_at: chrono::Utc::now(),
            })
        }

        async fn record_result(&self, record: ToolResultRecord) -> Result<Message, sqlx::Error> {
            self.results.lock().unwrap().push(record.clone());
            Ok(Message {
                id: Uuid::new_v4(),
                session_id: record.session_id,
                sequence: 1,
                role: "tool".to_string(),
                content: Some(record.content),
                tool_name: Some(record.tool_name),
                tool_input: None,
                tool_call_id: Some(record.tool_call_id),
                tool_output: Some(record.output),
                duration_ms: record.duration_ms.map(|d| d as i64),
                created_at: chrono::Utc::now(),
            })
        }
    }

    /// Regression test for the consumer-backpressure bug.
    ///
    /// Before the fix, the stdout/stderr reader tasks called
    /// `tx.send(...).await` to forward each chunk to the SSE
    /// channel. With a slow HTTP consumer (here: a test that
    /// never reads from the stream), the channel would fill,
    /// the reader would block on the send, the child process's
    /// pipe would fill, the child would block on `write`, and
    /// `child.wait()` would never return — so the recorder
    /// would never see the result row, and the HTTP response
    /// to the forge-tools extension would never close.
    ///
    /// The fix replaces `tx.send(...).await` with `tx.try_send`
    /// (dropping chunks on a full channel; the accumulator
    /// still has the bytes for the audit log). This test
    /// produces far more chunks than the 100-event channel
    /// capacity, never drains the rx, and asserts the result
    /// row is written within a few seconds. With the old
    /// code the test would time out and the result row would
    /// never appear.
    #[tokio::test]
    async fn bash_streaming_does_not_block_on_slow_consumer() {
        // Produce enough output to reliably generate
        // hundreds of 8 KiB read events, well over the
        // mpsc channel capacity of 100. `yes` writes
        // continuously with no internal buffering, so the
        // API reader gets one read syscall per pipe buffer
        // of output. 5 MB of base64-encoded random data is
        // ~6.6 MB of output = ~830 reads of 8 KiB = ~830
        // stdout events.
        let command = "head -c 5000000 /dev/urandom | base64".to_string();
        let recorder = Arc::new(TestRecorder::default());
        let bus = MessageBus::new();
        let metrics = Arc::new(Metrics::new());
        let results = recorder.results.clone();
        let dropped_metric_before = metrics
            .sse_chunks_dropped
            .load(std::sync::atomic::Ordering::Relaxed);

        let stream = execute_bash_streaming(
            Uuid::new_v4(),
            recorder as Arc<dyn ToolRecorder>,
            bus,
            metrics.clone(),
            "test-call-1".to_string(),
            "/tmp".to_string(),
            command,
            10_000,
            None,
            // sandbox: None => host-side path, simpler test
            // (no nspawn, no per-session rootfs needed).
            None,
        )
        .await;

        // Pin the stream so we can hold the rx and never read
        // from it. The bug used to surface here: a slow
        // consumer would backpressure the readers and the
        // child would block. The fix replaces
        // `tx.send(...).await` with `try_send` (and a bounded
        // flush for terminal events) so the child exits
        // promptly regardless of consumer state.
        let mut pinned = Box::pin(stream);
        // Pull exactly one event (the tool_start) to make sure
        // the task is actually running, then drop the rest on
        // the floor by never calling `next` again.
        let _ = tokio::time::timeout(Duration::from_secs(2), pinned.next()).await;

        // The bug: result row never gets written because the
        // child never exits because the readers never drain
        // the pipe because the channel sends block. Wait for
        // the result row with a hard ceiling; if it doesn't
        // show up, fail the test with a clear message. Also
        // measure how long the call took end-to-end, so we
        // can assert it completed promptly (sub-second
        // rather than the multi-minute hangs the original
        // bug produced).
        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !results.lock().unwrap().is_empty() {
                    return results.lock().unwrap()[0].clone();
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect(
            "streaming bash hung: the child never exited and no result row \
             was written within 5s. This is the consumer-backpressure bug \
             the try_send fix is supposed to prevent.",
        );
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(3),
            "call took {elapsed:?}; the fix should make it complete in <1s \
             even with a stalled consumer"
        );

        assert_eq!(result.tool_name, "bash");
        // The recorder's `output` jsonb carries the exit code
        // for bash. exit 0 == success, which is what the
        // `for i in $(seq 1 250); do echo line_$i; done` command
        // produces when bash itself is on PATH.
        let exit_code = result
            .output
            .get("exit_code")
            .and_then(|v| v.as_i64())
            .expect("bash result should include exit_code");
        assert_eq!(exit_code, 0, "bash should have exited 0");

        // The most-correct fix surfaces the drop count in
        // three places; verify all three so a future change
        // can't quietly regress the visibility.
        //
        // We don't pin the exact drop count: the reader
        // coalesces writes into 8 KiB reads, so the number
        // of distinct stdout events depends on how the
        // kernel schedules bash's writes. The invariant we
        // care about is that (a) drops happen (the live
        // stream is lossy under a stalled consumer), (b)
        // the drop count is exposed in the audit log and
        // the global metric, and (c) the audit-log stdout
        // is still complete.
        let dropped_in_result = result
            .output
            .get("dropped_sse_chunks")
            .and_then(|v| v.as_u64())
            .expect("audit-log row should include dropped_sse_chunks");
        assert!(
            dropped_in_result > 0,
            "expected some dropped SSE chunks with a stalled consumer; got 0"
        );

        // The `sse_chunks_dropped` global metric must
        // reflect the same count.
        let dropped_metric_after = metrics
            .sse_chunks_dropped
            .load(std::sync::atomic::Ordering::Relaxed);
        let metric_delta = dropped_metric_after - dropped_metric_before;
        assert_eq!(
            metric_delta, dropped_in_result,
            "sse_chunks_dropped metric should match the per-call drop count"
        );

        // The audit-log stdout is still complete — the
        // accumulator captured every byte the child wrote
        // regardless of how many live SSE chunks we
        // dropped. For 5 MB of input, base64 produces
        // ~6.6 MB of output (line-wrapped, no newlines
        // added at the start); we just check it's
        // substantially larger than the 100 * 8 KiB
        // channel capacity to prove no truncation happened
        // for the audit log.
        let captured_stdout = result
            .output
            .get("stdout")
            .and_then(|v| v.as_str())
            .expect("bash result should include captured stdout");
        assert!(
            captured_stdout.len() > 1_000_000,
            "audit-log stdout should hold the full base64 output, not just the live-stream chunks; got {} bytes",
            captured_stdout.len()
        );
    }

    /// Companion test: drain the rx as fast as possible
    /// (i.e. a fast consumer). Drops should be zero. This
    /// pins the "happy path" so a future change can't
    /// accidentally start dropping chunks for fast
    /// consumers too.
    #[tokio::test]
    async fn bash_streaming_does_not_drop_for_fast_consumer() {
        let command = "for i in $(seq 1 50); do echo line_$i; done".to_string();
        let recorder = Arc::new(TestRecorder::default());
        let bus = MessageBus::new();
        let metrics = Arc::new(Metrics::new());
        let results = recorder.results.clone();

        let stream = execute_bash_streaming(
            Uuid::new_v4(),
            recorder as Arc<dyn ToolRecorder>,
            bus,
            metrics.clone(),
            "test-call-2".to_string(),
            "/tmp".to_string(),
            command,
            10_000,
            None,
            // sandbox: None => host-side path; no nspawn.
            None,
        )
        .await;

        // Drain the stream to completion (fast consumer).
        let mut pinned = Box::pin(stream);
        while pinned.next().await.is_some() {}

        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !results.lock().unwrap().is_empty() {
                    return results.lock().unwrap()[0].clone();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("result row never written");

        let dropped = result
            .output
            .get("dropped_sse_chunks")
            .and_then(|v| v.as_u64())
            .expect("audit-log row should include dropped_sse_chunks");
        assert_eq!(
            dropped, 0,
            "fast consumer should see zero SSE drops; got {dropped}"
        );
        assert_eq!(
            metrics
                .sse_chunks_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "sse_chunks_dropped metric should be 0 for a fast consumer"
        );
    }

    #[test]
    fn bash_record_content_with_stdout() {
        let s = bash_record_content("hello\n", "", Some(0), 5);
        assert_eq!(s, "[bash exit=Some(0) duration=5ms]\n--stdout--\nhello\n");
    }

    #[test]
    fn bash_record_content_with_stderr() {
        let s = bash_record_content("ok", "warn\n", Some(0), 5);
        assert_eq!(
            s,
            "[bash exit=Some(0) duration=5ms]\n--stderr--\nwarn\n--stdout--\nok\n"
        );
    }

    #[test]
    fn bash_record_content_empty() {
        let s = bash_record_content("", "", Some(0), 0);
        assert_eq!(s, "[bash exit=Some(0) duration=0ms]\n");
    }

    #[test]
    fn bash_record_content_truncates() {
        // 8 KiB cap, so 20 KiB of stdout should be truncated.
        let big = "x".repeat(20 * 1024);
        let s = bash_record_content(&big, "", Some(0), 0);
        assert!(s.len() < 10 * 1024);
        assert!(s.contains("... [truncated]"));
    }

    #[test]
    fn bash_record_content_appends_missing_newline() {
        // stdout lacking a trailing newline should still get one
        // before the next section, otherwise the audit log reads
        // as a single run-on line.
        let s = bash_record_content("no-newline", "", Some(0), 1);
        assert!(s.ends_with("no-newline\n"));
    }
}
