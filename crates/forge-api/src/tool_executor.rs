//! Tool Executor Module
//!
//! Executes tools (bash, read, write, edit) in the sandbox.
//! This is where actual tool work happens - NOT in pi.
//!
//! ## Audit log
//!
//! The executor is the single owner of the *result* half of the
//! tool-call audit log. When a tool finishes (or errors), the
//! executor hands a [`ToolResultRecord`] to its [`ToolRecorder`],
//! which persists it to the `messages` table. The harness (the
//! `pi_agent` event loop) owns the *call* half - it records the
//! model's intent to call a tool when it sees `toolcall_end`.
//! The two halves are linked by `tool_call_id`.
//!
//! Why split it this way? The executor has the most accurate view
//! of the outcome (exit code, byte counts, timing, stdout/stderr
//! split) and is the only place that knows when the tool
//! definitively finished. The harness has the most accurate view
//! of the intent (it sees the model emit the tool call) and is
//! the only place that knows the call happened *before* execution.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use uuid::Uuid;

use crate::bus::MessageBus;
use crate::recording::{ToolRecorder, ToolResultRecord};
use crate::sandbox::SandboxManager;

/// Structured outcome of a bash execution. Populated by `execute_bash`
/// and consumed by `record_outcome` so the `tool_output` jsonb column
/// can carry stdout, stderr, and exit_code separately instead of just
/// the flattened text the LLM sees.
///
/// We use a thread-local rather than a struct field because the
/// recording happens after `execute` returns, and we don't want to
/// thread the structured data through every tool's Result type just
/// to re-include it in the audit row. The executor is single-threaded
/// per request so the thread-local is safe.
#[derive(Debug, Clone, Default)]
struct BashOutcome {
    stdout: String,
    stderr: String,
    exit_code: Option<i32>,
    timed_out: bool,
}

thread_local! {
    static LAST_BASH_RESULT: std::cell::RefCell<Option<BashOutcome>> = const { std::cell::RefCell::new(None) };
}

/// Tool execution errors
#[derive(Error, Debug)]
pub enum ToolError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Tool execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Timeout: {0}")]
    Timeout(String),
}

/// Tool execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolOutput {
    pub success: bool,
    pub output: Option<String>,
    pub error: Option<String>,
}

/// Input for bash tool
#[derive(Debug, Deserialize)]
pub struct BashInput {
    pub command: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30000
}

/// Input for read tool
#[derive(Debug, Deserialize)]
pub struct ReadInput {
    pub path: String,
    #[serde(default = "default_offset")]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_offset() -> usize {
    1
}

fn default_limit() -> usize {
    100
}

/// Input for write tool
#[derive(Debug, Deserialize)]
pub struct WriteInput {
    pub path: String,
    pub content: String,
}

/// Input for edit tool
#[derive(Debug, Deserialize)]
pub struct EditInput {
    pub path: String,
    pub old_text: String,
    pub new_text: String,
}

/// Unified tool input
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ToolInput {
    pub session_id: String,
    pub tool: String,
    pub input: serde_json::Value,
    /// Identifier for the call as provided by the LLM. Different pi
    /// versions and tool-registration paths sometimes hand the
    /// `toolCallId` through to extensions as the whole tool-call object
    /// (`{"id": ..., "name": ..., "arguments": ...}`) instead of just
    /// the id string. Accept any JSON value and stringify it so the
    /// request never fails just because of shape.
    #[serde(rename = "tool_call_id", default)]
    pub tool_call_id: serde_json::Value,
}

impl ToolInput {
    /// Pull a usable string id out of the (loosely typed)
    /// `tool_call_id` field. Falls back to a synthetic id when the
    /// caller didn't supply one.
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

/// Tool executor that runs tools in the sandbox
pub struct ToolExecutor {
    /// Working directory for tool execution
    working_dir: String,
    /// Whether to execute in sandbox container
    in_sandbox: bool,
    /// Nix shell expression or path (if configured)
    nix_shell: Option<String>,
    /// Session the executor is bound to (for recording result rows)
    session_id: Uuid,
    /// Recorder for the audit log (call + result rows)
    recorder: Arc<dyn ToolRecorder>,
    /// In-process bus for live SSE delivery. The executor publishes
    /// every result row to it so SSE consumers see the new row
    /// without polling.
    bus: crate::bus::MessageBus,
    /// Sandbox manager for `run_in_container`. `Some` when the
    /// session has a container; `None` for the resume/replay
    /// path (where the working dir is already inside the
    /// sandbox) and for the legacy host-side execution path.
    sandbox: Option<Arc<SandboxManager>>,
}

impl ToolExecutor {
    /// Create a new tool executor bound to a session, with a
    /// `ToolRecorder` for persisting both call and result rows.
    ///
    /// `sandbox`: when `Some`, `bash` tool calls are wrapped in
    /// a per-call `systemd-nspawn` against the session's
    /// container rootfs. When `None`, bash runs directly on the
    /// host in `working_dir` (the legacy behavior, used by
    /// resume/replay and by sessions that have no container
    /// yet).
    pub fn new(
        session_id: Uuid,
        working_dir: String,
        in_sandbox: bool,
        nix_shell: Option<String>,
        recorder: Arc<dyn ToolRecorder>,
        bus: MessageBus,
        sandbox: Option<Arc<SandboxManager>>,
    ) -> Self {
        Self {
            working_dir,
            in_sandbox,
            nix_shell,
            session_id,
            recorder,
            bus,
            sandbox,
        }
    }

    /// Wrap a command with nix-shell if configured
    ///
    /// Returns (command_to_run, command_to_pass)
    /// If nix-shell is configured, returns ("bash", "nix-shell -c '...' -p pkg")
    /// Otherwise, returns ("bash", "command")
    fn wrap_command(&self, command: &str) -> (String, String) {
        match &self.nix_shell {
            Some(nix_expr) => {
                // Check if it's a path or an expression
                if nix_expr.starts_with('/') || nix_expr.starts_with('.') || nix_expr.ends_with(".nix") {
                    // It's a path to a .nix file or shell.nix
                    let wrapped = format!("nix-shell '{}' -c '{}'", nix_expr, command.replace("'", "'\"'\"'"));
                    tracing::debug!("Using nix-shell with path: {}", nix_expr);
                    ("bash".to_string(), wrapped)
                } else {
                    // It's a nix shell expression (packages)
                    let wrapped = format!("nix-shell -p {} -c '{}'", nix_expr, command.replace("'", "'\"'\"'"));
                    tracing::debug!("Using nix-shell with expression: {}", nix_expr);
                    ("bash".to_string(), wrapped)
                }
            }
            None => {
                ("bash".to_string(), command.to_string())
            }
        }
    }

    /// Execute a tool by name with the given input and persist the
    /// outcome to the audit log.
    ///
    /// `tool_call_id` is the id the agent runtime gave the call.
    /// It's used here to link the result row back to the call row
    /// that this method writes immediately before running the tool.
    /// The executor is the sole writer of both rows in the audit
    /// log; see `docs/ARCHITECTURE.md` §5 and `crates/forge-api/src/recording.rs`
    /// for the design.
    pub async fn execute(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        input: serde_json::Value,
    ) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!(
            "tool_execute",
            tool = %tool_name,
            tool_call_id = %tool_call_id,
            working_dir = %self.working_dir,
            in_sandbox = %self.in_sandbox
        );
        let _guard = span.enter();

        tracing::debug!(input = ?input, "Executing tool");

        // Record the call row. The executor is the sole writer of
        // tool-related rows in the audit log - the harness used to
        // write the call row when it saw the LLM's `ToolCallEnd`
        // event, but that created a race with the executor (the
        // harness could exit its event loop on `agent_end` before
        // all parallel `ToolCallEnd` events arrived, leaving some
        // calls without a row). The executor is guaranteed to see
        // every call (it has to run the tool anyway) so it writes
        // the call row here, before the tool runs. A failure to
        // record the call is logged but does not abort the tool -
        // the result row is still useful on its own, and the
        // `tool_call_id` on the result row gives the audit-log
        // reader enough to find the call from pi's stdout if it
        // ever needs to.
        match self.recorder.record_call(crate::recording::ToolCallRecord {
            session_id: self.session_id,
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
            input: input.clone(),
        }).await {
            Ok(row) => {
                // Publish to the bus so SSE consumers see the
                // call row as soon as it's written.
                self.bus.publish_message(row);
            }
            Err(e) => {
                tracing::warn!(
                    tool_call_id = %tool_call_id,
                    tool = %tool_name,
                    error = %e,
                    "Failed to write tool call row; the result row will still be recorded but the call/result linkage may be broken"
                );
            }
        }

        let start = Instant::now();
        let result = match tool_name {
            "bash" => self.execute_bash(input).await,
            "read" => self.execute_read(input).await,
            "write" => self.execute_write(input).await,
            "edit" => self.execute_edit(input).await,
            _ => {
                tracing::error!("Unknown tool: {}", tool_name);
                Err(ToolError::NotFound(tool_name.to_string()))
            }
        };
        let duration_ms = start.elapsed().as_millis() as u64;

        // Persist the outcome, regardless of success or error. A
        // recorded error is more useful than no record at all - the
        // caller can match the row by `tool_call_id` to find out what
        // the agent tried to do.
        self.record_outcome(tool_call_id, tool_name, &result, duration_ms)
            .await;

        result
    }

    /// Hand a tool outcome to the recorder. Never fails the call -
    /// audit-log errors are best-effort so a transient DB hiccup
    /// doesn't take down the tool path.
    async fn record_outcome(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        result: &Result<ToolOutput, ToolError>,
        duration_ms: u64,
    ) {
        let (content, output_value, is_error) = match result {
            Ok(out) => {
                let content = out.output.clone().unwrap_or_default();
                // For bash, swap the plain output blob for the
                // structured stdout/stderr/exit_code we stashed
                // before returning. The flattened string still ends
                // up in `content`; this is just about preserving
                // structure in `tool_output`.
                let output_value = if tool_name == "bash" {
                    LAST_BASH_RESULT.with(|cell| {
                        let outcome = cell.borrow_mut().take();
                        match outcome {
                            Some(o) => serde_json::json!({
                                "success": out.success,
                                "stdout": o.stdout,
                                "stderr": o.stderr,
                                "exit_code": o.exit_code,
                                "timed_out": o.timed_out,
                            }),
                            None => serde_json::json!({
                                "success": out.success,
                                "output": out.output,
                                "error": out.error,
                            }),
                        }
                    })
                } else {
                    serde_json::json!({
                        "success": out.success,
                        "output": out.output,
                        "error": out.error,
                    })
                };
                (content, output_value, !out.success)
            }
            Err(e) => {
                let msg = e.to_string();
                let output_value = if tool_name == "bash" {
                    LAST_BASH_RESULT.with(|cell| {
                        let outcome = cell.borrow_mut().take();
                        match outcome {
                            Some(o) => serde_json::json!({
                                "success": false,
                                "stdout": o.stdout,
                                "stderr": o.stderr,
                                "exit_code": o.exit_code,
                                "timed_out": o.timed_out,
                                "error": msg,
                            }),
                            None => serde_json::json!({
                                "success": false,
                                "error": msg,
                            }),
                        }
                    })
                } else {
                    serde_json::json!({
                        "success": false,
                        "error": msg,
                    })
                };
                (format!("[error] {}", msg), output_value, true)
            }
        };

        let record = ToolResultRecord {
            session_id: self.session_id,
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
            content,
            output: output_value,
            is_error,
            duration_ms: Some(duration_ms),
        };
        match self.recorder.record_result(record).await {
            Ok(row) => {
                // Publish to the bus so SSE consumers see the
                // new row without polling.
                self.bus.publish_message(row);
            }
            Err(db_err) => {
                tracing::warn!(
                    tool_call_id = %tool_call_id,
                    tool = %tool_name,
                    error = %db_err,
                    "Failed to persist tool result to audit log"
                );
            }
        }
    }

    /// Execute a bash command
    async fn execute_bash(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!(
            "bash_execute",
            cwd = %self.working_dir,
            has_nix = %self.nix_shell.is_some(),
            in_sandbox = %self.sandbox.is_some(),
        );
        let _guard = span.enter();

        let input: BashInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid bash input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;

        tracing::debug!(
            command = %input.command,
            timeout_ms = %input.timeout_ms,
            "Executing bash command"
        );

        // Sanitize command length for logging
        let cmd_preview = if input.command.len() > 100 {
            format!("{}... ({} chars)", &input.command[..100], input.command.len())
        } else {
            input.command.clone()
        };
        tracing::info!(command_preview = %cmd_preview, "Running bash command");

        // Determine the command wrapper (nix-shell if configured)
        let (cmd_to_run, wrapped_command) = self.wrap_command(&input.command);

        // Sandbox fast-path: if the session has a container,
        // run the command in the per-session rootfs via
        // `systemd-nspawn --as-pid2`. This gives the command
        // its own process + filesystem namespace; the host
        // process tree doesn't see it, and `apt install`
        // (and other root-mutating commands) only affect the
        // per-session rootfs. Network namespace is *not*
        // isolated — the agent needs network access.
        //
        // We do NOT apply the nix-shell wrap when running in
        // the container; nix-shell needs the host's nix
        // store mounted, which we deliberately don't
        // bind-mount. If a profile has `nix_shell` set, the
        // session's container will need a custom build
        // (TODO), so we log a warning and run without the
        // wrap.
        if let Some(sandbox) = &self.sandbox {
            if self.nix_shell.is_some() {
                tracing::warn!(
                    "profile has nix_shell set but the session is running sandboxed; \
                     nix-shell wrap is skipped in the container path. \
                     nix_shell support in the sandbox rootfs is TODO."
                );
            }
            return self.execute_bash_sandboxed(
                sandbox,
                &wrapped_command,
                input.timeout_ms,
            ).await;
        }

        // Host-side execution (legacy / resume path).
        let mut cmd = Command::new(&cmd_to_run);
        cmd.arg("-c")
            .arg(&wrapped_command)
            .current_dir(&self.working_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Set timeout
        let timeout = std::time::Duration::from_millis(input.timeout_ms);

        match tokio::time::timeout(timeout, cmd.output()).await {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();

                let stdout_empty = stdout.is_empty();
                let stdout_len = stdout.len();
                let stderr_len = stderr.len();
                let result_output = if stdout_empty { stderr.clone() } else { stdout.clone() };
                let error = if stderr.is_empty() || !stdout_empty { None } else { Some(stderr.clone()) };

                let success = output.status.success();
                let exit_code = output.status.code();
                tracing::info!(
                    success = %success,
                    exit_code = ?exit_code,
                    stdout_len = %stdout_len,
                    stderr_len = %stderr_len,
                    "Bash command completed"
                );

                // Stash the structured outcome on the side so
                // `record_outcome` can include stdout/stderr/exit_code
                // in the `tool_output` jsonb without having to
                // re-parse the flattened strings.
                LAST_BASH_RESULT.with(|cell| {
                    *cell.borrow_mut() = Some(BashOutcome {
                        stdout,
                        stderr,
                        exit_code,
                        timed_out: false,
                    });
                });

                Ok(ToolOutput {
                    success,
                    output: Some(result_output),
                    error,
                })
            }
            Ok(Err(e)) => {
                tracing::error!("Failed to execute bash: {}", e);
                Err(ToolError::ExecutionFailed(e.to_string()))
            }
            Err(_) => {
                tracing::warn!(timeout_ms = %input.timeout_ms, "Bash command timed out");
                LAST_BASH_RESULT.with(|cell| {
                    *cell.borrow_mut() = Some(BashOutcome {
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: None,
                        timed_out: true,
                    });
                });
                Err(ToolError::Timeout(format!(
                    "Command timed out after {}ms",
                    input.timeout_ms
                )))
            }
        }
    }

    /// Sandboxed bash path. Delegates to
    /// `SandboxManager::run_in_container`, which spawns
    /// `systemd-nspawn` with the session's rootfs and runs
    /// the command inside the namespace. See the comment on
    /// `run_in_container` for the timeout layering.
    async fn execute_bash_sandboxed(
        &self,
        sandbox: &Arc<SandboxManager>,
        command: &str,
        timeout_ms: u64,
    ) -> Result<ToolOutput, ToolError> {
        match sandbox.run_in_container(self.session_id, command, timeout_ms).await {
            Ok(out) => {
                let success = out.exit_code.map(|c| c == 0).unwrap_or(false) && !out.timed_out;
                let stdout_empty = out.stdout.is_empty();
                let result_output = if stdout_empty { out.stderr.clone() } else { out.stdout.clone() };
                let error = if out.stderr.is_empty() || !out.stdout.is_empty() {
                    None
                } else {
                    Some(out.stderr.clone())
                };
                let exit_code = out.exit_code;
                let stdout_len = out.stdout.len();
                let stderr_len = out.stderr.len();
                tracing::info!(
                    success = %success,
                    exit_code = ?exit_code,
                    timed_out = %out.timed_out,
                    stdout_len = %stdout_len,
                    stderr_len = %stderr_len,
                    "Sandboxed bash command completed"
                );

                LAST_BASH_RESULT.with(|cell| {
                    *cell.borrow_mut() = Some(BashOutcome {
                        stdout: out.stdout,
                        stderr: out.stderr,
                        exit_code,
                        timed_out: out.timed_out,
                    });
                });

                if out.timed_out {
                    return Err(ToolError::Timeout(format!(
                        "Command timed out after {}ms",
                        timeout_ms
                    )));
                }
                Ok(ToolOutput {
                    success,
                    output: Some(result_output),
                    error,
                })
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Sandboxed bash failed to run"
                );
                LAST_BASH_RESULT.with(|cell| {
                    *cell.borrow_mut() = Some(BashOutcome {
                        stdout: String::new(),
                        stderr: format!("[sandbox error] {}", e),
                        exit_code: None,
                        timed_out: false,
                    });
                });
                Err(ToolError::ExecutionFailed(e.to_string()))
            }
        }
    }

    /// Read a file
    async fn execute_read(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("read_execute", cwd = %self.working_dir);
        let _guard = span.enter();

        let input: ReadInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid read input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;

        let path = Path::new(&self.working_dir).join(&input.path);
        tracing::debug!(path = ?path, offset = %input.offset, limit = %input.limit, "Reading file");

        let mut file = tokio::fs::File::open(&path).await
            .map_err(|e| {
                tracing::error!("Failed to open file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to open file: {}", e))
            })?;

        let mut content = String::new();
        file.read_to_string(&mut content).await
            .map_err(|e| {
                tracing::error!("Failed to read file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to read file: {}", e))
            })?;

        // Apply offset and limit (lines are 1-indexed)
        let lines: Vec<&str> = content.lines().collect();
        let start = input.offset.saturating_sub(1).min(lines.len());
        let end = (start + input.limit).min(lines.len());
        let selected: String = lines[start..end].join("\n");

        tracing::info!(
            total_lines = %lines.len(),
            returned_lines = %(end - start),
            bytes = %selected.len(),
            "File read completed"
        );

        Ok(ToolOutput {
            success: true,
            output: Some(selected),
            error: None,
        })
    }

    /// Write content to a file
    async fn execute_write(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("write_execute", cwd = %self.working_dir);
        let _guard = span.enter();

        let input: WriteInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid write input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;

        let path = Path::new(&self.working_dir).join(&input.path);
        tracing::debug!(path = ?path, bytes = %input.content.len(), "Writing file");

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| {
                    tracing::error!("Failed to create directory {:?}: {}", parent, e);
                    ToolError::ExecutionFailed(format!("Failed to create directory: {}", e))
                })?;
        }

        let mut file = tokio::fs::File::create(&path).await
            .map_err(|e| {
                tracing::error!("Failed to create file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to create file: {}", e))
            })?;

        file.write_all(input.content.as_bytes()).await
            .map_err(|e| {
                tracing::error!("Failed to write to file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to write file: {}", e))
            })?;

        tracing::info!(path = ?path, bytes_written = %input.content.len(), "File write completed");

        Ok(ToolOutput {
            success: true,
            output: Some(format!("Successfully wrote {} bytes to {}", input.content.len(), input.path)),
            error: None,
        })
    }

    /// Edit a file with targeted replacement
    async fn execute_edit(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("edit_execute", cwd = %self.working_dir);
        let _guard = span.enter();

        let input: EditInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid edit input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;

        let path = Path::new(&self.working_dir).join(&input.path);
        tracing::debug!(
            path = ?path,
            old_text_len = %input.old_text.len(),
            new_text_len = %input.new_text.len(),
            "Editing file"
        );

        // Read the file
        let mut content = tokio::fs::read_to_string(&path).await
            .map_err(|e| {
                tracing::error!("Failed to read file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to read file: {}", e))
            })?;

        // Find and replace
        if !content.contains(&input.old_text) {
            tracing::warn!(
                path = ?path,
                old_text_len = %input.old_text.len(),
                "old_text not found in file"
            );
            return Err(ToolError::InvalidInput(
                "old_text not found in file. Make sure to match the exact text including whitespace.".to_string()
            ));
        }

        content = content.replacen(&input.old_text, &input.new_text, 1);

        // Write back
        tokio::fs::write(&path, &content).await
            .map_err(|e| {
                tracing::error!("Failed to write file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to write file: {}", e))
            })?;

        tracing::info!(path = ?path, "Edit applied successfully");

        Ok(ToolOutput {
            success: true,
            output: Some("Edit applied successfully".to_string()),
            error: None,
        })
    }
}

/// Execute a tool from an API request
#[allow(dead_code)]
pub async fn execute_tool(
    tool_executor: &ToolExecutor,
    request: &ToolInput,
) -> Result<ToolOutput, ToolError> {
    let tool_call_id = request.tool_call_id_str();
    tool_executor
        .execute(&tool_call_id, &request.tool, request.input.clone())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::MessageBus;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Build a `ToolExecutor` against a fresh tempdir with a
    /// no-op recorder. Used by the local unit tests below. They
    /// only exercise the executor's read/write/edit/bash paths
    /// and don't need to assert anything about the audit log.
    fn temp_executor() -> (ToolExecutor, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let session_id = uuid::Uuid::new_v4();
        struct NoopRecorder;
        #[async_trait::async_trait]
        impl crate::recording::ToolRecorder for NoopRecorder {
            async fn record_call(
                &self,
                _record: crate::recording::ToolCallRecord,
            ) -> Result<crate::db::Message, sqlx::Error> {
                // Tests that need a real recording can use a
                // dedicated helper; the smoke tests just want
                // to exercise the tool code paths.
                Err(sqlx::Error::ColumnNotFound(
                    "noop test recorder".to_string(),
                ))
            }
            async fn record_result(
                &self,
                _record: crate::recording::ToolResultRecord,
            ) -> Result<crate::db::Message, sqlx::Error> {
                Err(sqlx::Error::ColumnNotFound(
                    "noop test recorder".to_string(),
                ))
            }
        }
        let executor = ToolExecutor::new(
            session_id,
            temp_dir.path().to_string_lossy().to_string(),
            false,
            None,
            Arc::new(NoopRecorder),
            MessageBus::new(),
        );
        (executor, temp_dir)
    }

    #[tokio::test]
    async fn test_bash_simple_command() {
        let (executor, _temp) = temp_executor();

        let input = serde_json::json!({
            "command": "echo 'hello world'",
            "timeout_ms": 5000
        });

        let result = executor.execute("t1", "bash", input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.success);
        assert!(output.output.unwrap().contains("hello world"));
    }

    #[tokio::test]
    async fn test_bash_command_failure() {
        let (executor, _temp) = temp_executor();

        let input = serde_json::json!({
            "command": "exit 1",
            "timeout_ms": 5000
        });

        let result = executor.execute("t1", "bash", input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.success);
    }

    #[tokio::test]
    async fn test_bash_timeout() {
        let (executor, _temp) = temp_executor();

        let input = serde_json::json!({
            "command": "sleep 10",
            "timeout_ms": 100
        });

        let result = executor.execute("t1", "bash", input).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn test_unknown_tool() {
        let (executor, _temp) = temp_executor();

        let input = serde_json::json!({});

        let result = executor.execute("t1", "nonexistent_tool", input).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_and_read_file() {
        let (executor, temp) = temp_executor();

        // Write a file
        let write_input = serde_json::json!({
            "path": "test.txt",
            "content": "Hello, World!"
        });

        let write_result = executor.execute("t1", "write", write_input).await;
        assert!(write_result.is_ok());
        assert!(write_result.unwrap().success);

        // Read it back
        let read_input = serde_json::json!({
            "path": "test.txt"
        });

        let read_result = executor.execute("t1", "read", read_input).await;
        assert!(read_result.is_ok());
        assert!(read_result.unwrap().output.unwrap().contains("Hello, World!"));
    }

    #[tokio::test]
    async fn test_edit_file() {
        let (executor, temp) = temp_executor();

        // Create a file first
        let write_input = serde_json::json!({
            "path": "edit_test.txt",
            "content": "Hello, World!"
        });
        executor.execute("t1", "write", write_input).await.unwrap();

        // Edit it
        let edit_input = serde_json::json!({
            "path": "edit_test.txt",
            "old_text": "World",
            "new_text": "Rust"
        });

        let result = executor.execute("t1", "edit", edit_input).await;
        assert!(result.is_ok());

        // Verify the edit
        let read_input = serde_json::json!({"path": "edit_test.txt"});
        let read_result = executor.execute("t1", "read", read_input).await.unwrap();
        assert!(read_result.output.unwrap().contains("Hello, Rust!"));
    }

    #[tokio::test]
    async fn test_edit_file_not_found() {
        let (executor, _temp) = temp_executor();

        let edit_input = serde_json::json!({
            "path": "nonexistent.txt",
            "old_text": "something",
            "new_text": "replacement"
        });

        let result = executor.execute("t1", "edit", edit_input).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let (executor, _temp) = temp_executor();

        let read_input = serde_json::json!({
            "path": "nonexistent_file.txt"
        });

        let result = executor.execute("t1", "read", read_input).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_nested_directory() {
        let (executor, temp) = temp_executor();

        let write_input = serde_json::json!({
            "path": "nested/dir/test.txt",
            "content": "nested content"
        });

        let result = executor.execute("t1", "write", write_input).await;
        assert!(result.is_ok());

        // Verify file exists
        let file_path: PathBuf = temp.path().join("nested/dir/test.txt");
        assert!(file_path.exists());
    }
}
