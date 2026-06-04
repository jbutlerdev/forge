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
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use tokio::process::Command;
use tokio::io::AsyncReadExt;
use serde::Deserialize;
use uuid::Uuid;

use crate::tool_executor::ToolExecutor;
use crate::api::AppState;

/// SSE event names
mod event_names {
    pub const TOOL_START: &str = "tool_start";
    pub const STDOUT: &str = "stdout";
    pub const STDERR: &str = "stderr";
    pub const TOOL_END: &str = "tool_end";
    pub const ERROR: &str = "error";
    pub const DONE: &str = "done";
}

/// Input for streaming bash tool
#[derive(Debug, Deserialize)]
pub struct StreamingBashInput {
    pub command: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30000
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
    let json = serde_json::to_string(&data).unwrap_or_else(|_| r#"{"error": "serialization failed"}"#.to_string());
    Event::default()
        .event(event_name)
        .data(json)
}

/// Create a simple SSE data event (no event name)
fn make_data_event(data: impl serde::Serialize) -> Event {
    let json = serde_json::to_string(&data).unwrap_or_else(|_| r#"{"error": "serialization failed"}"#.to_string());
    Event::default().data(json)
}

/// Stream type for SSE responses
type SseStream = Pin<Box<dyn Stream<Item = Result<Event, axum::Error>> + Send>>;

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
pub async fn execute_bash_streaming(
    session_id: uuid::Uuid,
    recorder: std::sync::Arc<dyn crate::recording::ToolRecorder>,
    bus: crate::bus::MessageBus,
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

        // Send tool_start event
        let _ = tx.send(Ok(make_named_event(event_names::TOOL_START, serde_json::json!({
            "tool": "bash",
            "tool_call_id": tool_call_id
        })))).await;

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
            let timeout_secs = std::cmp::max(1, (timeout_ms + 999) / 1000);
            let mut c = Command::new("systemd-nspawn");
            c.arg("-D").arg(root_dir)
                .arg("--as-pid2")
                .arg("--user=root")
                .arg("--bind").arg(format!("{}:{}", working_dir, working_dir))
                .arg("--chdir").arg(&working_dir)
                .arg("--setenv=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
                .arg("--setenv=HOME=/root")
                .arg("--setenv=USER=root")
                .arg("--setenv=LOGNAME=root")
                .arg("--setenv=TERM=xterm");

            // Same FORGE_GITHUB_TOKEN -> GITHUB_TOKEN passthrough
            // as the non-streaming path. See the long comment in
            // `SandboxManager::run_in_container` for the rationale
            // (operator-controlled PAT, base-rootfs credential
            // helper, LLM does plain `git push`).
            if let Ok(token) = std::env::var("FORGE_GITHUB_TOKEN") {
                if !token.is_empty() {
                    c.arg(format!("--setenv=GITHUB_TOKEN={}", token));
                }
            }

            c.arg("--")
                .arg("timeout")
                .arg("--kill-after=2")
                .arg(format!("{}s", timeout_secs))
                .arg("bash")
                .arg("-c")
                .arg(&wrapped_command)
                .stdout(std::process::Stdio::piped())
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
        // 10 MiB per stream so a runaway `cat /dev/zero` doesn't
        // OOM the api process.
        const MAX_CAPTURED_BYTES: usize = 10 * 1024 * 1024;
        let stdout_buf: Arc<tokio::sync::Mutex<Vec<u8>>> = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stderr_buf: Arc<tokio::sync::Mutex<Vec<u8>>> = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        // JoinHandles for the reader tasks. We hold them so we
        // can await them after child.wait() and be sure the
        // accumulators are fully populated before we read them.
        let mut reader_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        match timeout(timeout_duration, async { spawn_result }).await {
            Ok(Ok(mut child)) => {
                let stdout = child.stdout.take();
                let stderr = child.stderr.take();

                // Stream stdout
                if let Some(mut stdout_handle) = stdout {
                    let tool_call_id = tool_call_id.clone();
                    let tx = tx.clone();
                    let stdout_buf = stdout_buf.clone();
                    let handle = tokio::spawn(async move {
                        let mut buf = [0u8; 8192];
                        loop {
                            match stdout_handle.read(&mut buf).await {
                                Ok(0) => break, // EOF
                                Ok(n) => {
                                    {
                                        let mut acc = stdout_buf.lock().await;
                                        if acc.len() < MAX_CAPTURED_BYTES {
                                            let take = (MAX_CAPTURED_BYTES - acc.len()).min(n);
                                            acc.extend_from_slice(&buf[..take]);
                                        }
                                    }
                                    let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                                    let _ = tx.send(Ok(make_named_event(event_names::STDOUT, serde_json::json!({
                                        "tool_call_id": tool_call_id,
                                        "chunk": chunk
                                    })))).await;
                                }
                                Err(e) => {
                                    let _ = tx.send(Ok(make_named_event(event_names::STDERR, serde_json::json!({
                                        "tool_call_id": tool_call_id,
                                        "chunk": format!("stdout error: {}", e)
                                    })))).await;
                                    break;
                                }
                            }
                        }
                    });
                    reader_handles.push(handle);
                }

                // Stream stderr
                if let Some(mut stderr_handle) = stderr {
                    let tool_call_id = tool_call_id.clone();
                    let tx = tx.clone();
                    let stderr_buf = stderr_buf.clone();
                    let handle = tokio::spawn(async move {
                        let mut buf = [0u8; 8192];
                        loop {
                            match stderr_handle.read(&mut buf).await {
                                Ok(0) => break, // EOF
                                Ok(n) => {
                                    {
                                        let mut acc = stderr_buf.lock().await;
                                        if acc.len() < MAX_CAPTURED_BYTES {
                                            let take = (MAX_CAPTURED_BYTES - acc.len()).min(n);
                                            acc.extend_from_slice(&buf[..take]);
                                        }
                                    }
                                    let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                                    let _ = tx.send(Ok(make_named_event(event_names::STDERR, serde_json::json!({
                                        "tool_call_id": tool_call_id,
                                        "chunk": chunk
                                    })))).await;
                                }
                                Err(e) => {
                                    let _ = tx.send(Ok(make_named_event(event_names::STDERR, serde_json::json!({
                                        "tool_call_id": tool_call_id,
                                        "chunk": format!("stderr error: {}", e)
                                    })))).await;
                                    break;
                                }
                            }
                        }
                    });
                    reader_handles.push(handle);
                }

                // Wait for process to complete
                recorded_outcome = match child.wait().await {
                    Ok(status) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        let success = status.success();
                        let exit_code = status.code();

                        // Send tool_end event
                        let _ = tx.send(Ok(make_named_event(event_names::TOOL_END, serde_json::json!({
                            "tool_call_id": tool_call_id,
                            "success": success,
                            "duration_ms": duration_ms,
                            "exit_code": exit_code
                        })))).await;

                        tracing::info!(
                            tool_call_id = %tool_call_id,
                            success = %success,
                            duration_ms = %duration_ms,
                            "Bash streaming completed"
                        );

                        Some((success, exit_code, duration_ms))
                    }
                    Err(e) => {
                        let _ = tx.send(Ok(make_named_event(event_names::ERROR, serde_json::json!({
                            "tool_call_id": tool_call_id,
                            "error": format!("Process error: {}", e)
                        })))).await;
                        None
                    }
                }
            }
            Ok(Err(e)) => {
                let _ = tx.send(Ok(make_named_event(event_names::ERROR, serde_json::json!({
                    "tool_call_id": tool_call_id,
                    "error": format!("Failed to spawn process: {}", e)
                })))).await;
            }
            Err(_) => {
                let _ = tx.send(Ok(make_named_event(event_names::ERROR, serde_json::json!({
                    "tool_call_id": tool_call_id,
                    "error": format!("Command timed out after {}ms", timeout_ms)
                })))).await;
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
        let record = match recorded_outcome {
            Some((success, exit_code, duration_ms)) => crate::recording::ToolResultRecord {
                session_id,
                tool_call_id: tool_call_id.clone(),
                tool_name: "bash".to_string(),
                content: bash_record_content(&captured_stdout, &captured_stderr, exit_code, duration_ms),
                output: serde_json::json!({
                    "success": success,
                    "stdout": captured_stdout,
                    "stderr": captured_stderr,
                    "exit_code": exit_code,
                    "timed_out": false,
                    "streamed": true,
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

        // Send done event
        let _ = tx.send(Ok(make_data_event(serde_json::json!({"done": true})))).await;
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
            if nix_expr.starts_with('/') || nix_expr.starts_with('.') || nix_expr.ends_with(".nix") {
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
pub async fn execute_streaming_tool(
    session_id: uuid::Uuid,
    recorder: std::sync::Arc<dyn crate::recording::ToolRecorder>,
    bus: crate::bus::MessageBus,
    tool_call_id: &str,
    working_dir: &str,
    tool_name: &str,
    input: serde_json::Value,
    nix_shell: Option<&str>,
    sandbox: Option<std::sync::Arc<crate::sandbox::SandboxManager>>,
) -> Result<SseStream, String> {
    match tool_name {
        "bash" => {
            let bash_input: StreamingBashInput = serde_json::from_value(input.clone())
                .map_err(|e| e.to_string())?;

            // The executor is the sole writer of the call row
            // (and the result row). The streaming bash path
            // doesn't go through `ToolExecutor::execute` so it
            // has to write its own call row here, before running
            // the command. A failure to record the call is logged
            // but does not abort the tool - the result row is
            // still useful on its own.
            match recorder.record_call(crate::recording::ToolCallRecord {
                session_id,
                tool_call_id: tool_call_id.to_string(),
                tool_name: "bash".to_string(),
                input: input.clone(),
            }).await {
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
                tool_call_id.to_string(),
                working_dir.to_string(),
                bash_input.command,
                bash_input.timeout_ms,
                nix_shell.map(|s| s.to_string()),
                sandbox.clone(),
            ).await)
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
                match executor.execute(&tool_call_id_owned, &tool_name_owned, input).await {
                    Ok(output) => {
                        let _ = tx.send(Ok(make_data_event(serde_json::json!({
                            "output": output.output
                        })))).await;
                        let _ = tx.send(Ok(make_named_event(event_names::TOOL_END, serde_json::json!({
                            "success": output.success,
                            "error": output.error
                        })))).await;
                    }
                    Err(e) => {
                        let _ = tx.send(Ok(make_named_event(event_names::ERROR, serde_json::json!({
                            "error": e.to_string()
                        })))).await;
                    }
                }
                let _ = tx.send(Ok(make_data_event(serde_json::json!({"done": true})))).await;
            });
            
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            Ok(Box::pin(stream.map(|result| result.map_err(|e| axum::Error::new(e.to_string())))))
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
            return (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({
                "error": "Invalid session ID format"
            }))).into_response();
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
        return (axum::http::StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "Session not found"
        }))).into_response();
    }

    // Get the session's isolated working directory. If the in-memory
    // session manager lost the entry (e.g. after an API restart), look
    // the directory up on disk so the tool call still works.
    let working_dir = match state.session_manager.get_session_dir(session_id).await {
        Ok(dir) => dir.to_string_lossy().to_string(),
        Err(_) => match crate::api::lookup_session_working_dir(&state, session_id).await {
            Some(dir) => dir,
            None => {
                return (axum::http::StatusCode::NOT_FOUND, Json(serde_json::json!({
                    "error": "Session not initialized"
                }))).into_response();
            }
        },
    };
    
    // Get nix shell configuration
    let nix_shell: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT p.nix_shell FROM sessions s JOIN profiles p ON s.profile_id = p.id WHERE s.id = $1"
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
                .keep_alive(axum::response::sse::KeepAlive::new()
                    .interval(Duration::from_secs(15))
                    .text("ping"))
                .into_response();
            
            // Add SSE headers
            let headers = response.headers_mut();
            headers.insert("X-Accel-Buffering", HeaderValue::from_static("no"));
            
            response
        }
        Err(e) => {
            tracing::error!("Failed to start streaming tool: {}", e);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": e
            }))).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::bash_record_content;

    #[test]
    fn bash_record_content_with_stdout() {
        let s = bash_record_content("hello\n", "", Some(0), 5);
        assert_eq!(s, "[bash exit=Some(0) duration=5ms]\n--stdout--\nhello\n");
    }

    #[test]
    fn bash_record_content_with_stderr() {
        let s = bash_record_content("ok", "warn\n", Some(0), 5);
        assert_eq!(s, "[bash exit=Some(0) duration=5ms]\n--stderr--\nwarn\n--stdout--\nok\n");
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
