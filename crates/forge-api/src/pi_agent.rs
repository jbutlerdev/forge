//! pi Agent Integration - Simplified
//! 
//! Spawns pi as a subprocess for session-based agent interactions.
//! Tools are executed via the forge-tools extension which calls back to the API.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Command, ChildStdin, ChildStdout};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Configuration for spawning pi
#[derive(Debug, Clone)]
pub struct PiConfig {
    pub working_dir: String,
    pub provider: String,
    pub model: String,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub system_prompt: String,
    pub forge_tools_extension: PathBuf,
    pub forge_api_url: String,
    pub session_id: Uuid,
    /// Optional path to a pi-format session jsonl file to
    /// load as the new active session at startup. When
    /// `Some`, pi is launched with `--session <path>`
    /// instead of `--no-session`. Used by the durable-resume
    /// path: the harness writes the prior conversation
    /// (from the `messages` table) to a jsonl file, then
    /// passes the path here so the fresh pi sees the full
    /// prior conversation as structured messages from the
    /// moment it starts running. When `None`, the fresh pi
    /// starts with an empty context (in-memory session).
    pub session_path: Option<PathBuf>,
}

/// pi RPC mode events.
///
/// pi emits events with camelCase field names (e.g. `assistantMessageEvent`,
/// `contentIndex`). The `rename_all = "camelCase"` on the enum translates
/// variant names; struct-like variants need an additional `rename_all` to
/// translate their fields, and field-level `#[serde(rename = ...)]` is used
/// for variants where not every field follows the camelCase convention.
///
/// In addition to the agent lifecycle events, RPC mode also emits:
/// - `response` events that correlate with commands we sent
/// - `extension_ui_request` events when an extension wants to talk to the host
/// These are captured here so the caller's event loop can ignore them
/// without producing `unknown variant` warnings on every line.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PiEvent {
    #[serde(rename = "session")]
    Session { version: i32, id: String },
    #[serde(rename = "agent_start")]
    AgentStart,
    #[serde(rename = "turn_start")]
    TurnStart,
    #[serde(rename = "message_start")]
    MessageStart { message: MessageMetadata },
    #[serde(rename = "message_update")]
    #[serde(rename_all = "camelCase")]
    MessageUpdate {
        assistant_message_event: Option<AssistantMessageEvent>,
        message: Option<MessageMetadata>,
    },
    #[serde(rename = "message_end")]
    MessageEnd { message: Option<MessageMetadata> },
    #[serde(rename = "turn_end")]
    TurnEnd,
    #[serde(rename = "agent_end")]
    AgentEnd,
    #[serde(rename = "error")]
    Error { message: String },
    // RPC mode command responses (e.g. `{"type":"response","command":"prompt","success":true}`)
    #[serde(rename = "response")]
    #[serde(rename_all = "camelCase")]
    Response {
        command: String,
        success: bool,
        #[serde(default)]
        id: Option<String>,
    },
    // RPC mode extension UI bridge events
    #[serde(rename = "extension_ui_request")]
    #[serde(rename_all = "camelCase")]
    ExtensionUiRequest {
        id: String,
        method: String,
    },
    // Tool execution lifecycle. The extension handles these, but pi
    // still streams them on stdout, so we need to accept them and
    // (in the case of `_end`) persist the result for an audit trail.
    #[serde(rename = "tool_execution_start")]
    #[serde(rename_all = "camelCase")]
    ToolExecutionStart {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        #[serde(default)]
        args: Option<serde_json::Value>,
    },
    #[serde(rename = "tool_execution_end")]
    #[serde(rename_all = "camelCase")]
    ToolExecutionEnd {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        result: serde_json::Value,
        #[serde(default)]
        is_error: bool,
    },
}

/// The tool call that the model produced, as carried on
/// `assistantMessageEvent.toolCall` in `toolcall_end` events. Used by
/// the message loop to persist the tool invocation so callers can see
/// what the agent asked for.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallPayload {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageMetadata {
    pub role: String,
    #[serde(default)]
    pub content: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AssistantMessageEvent {
    #[serde(rename = "text_start")]
    TextStart {
        #[serde(rename = "contentIndex")]
        content_index: usize,
    },
    #[serde(rename = "text_delta")]
    TextDelta { delta: String },
    #[serde(rename = "text_end")]
    TextEnd,
    #[serde(rename = "thinking_start")]
    ThinkingStart {
        #[serde(rename = "contentIndex")]
        content_index: usize,
    },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta { delta: String },
    #[serde(rename = "thinking_end")]
    ThinkingEnd,
    // Tool call streaming events - these appear when the model
    // produces a tool call instead of plain text. We capture the
    // completed call on `ToolCallEnd` and persist it as an assistant
    // message with the tool_name / tool_input columns populated.
    #[serde(rename = "toolcall_start")]
    ToolCallStart,
    #[serde(rename = "toolcall_delta")]
    ToolCallDelta { delta: String },
    #[serde(rename = "toolcall_end")]
    ToolCallEnd {
        #[serde(rename = "toolCall")]
        tool_call: ToolCallPayload,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum PiInput {
    #[serde(rename = "prompt")]
    Prompt {
        #[serde(rename = "message")]
        text: String,
    },
    #[serde(rename = "tool_result")]
    ToolResult { id: String, content: String, is_error: Option<bool> },
    #[serde(rename = "abort")]
    Abort,
}

/// pi Agent subprocess manager
pub struct PiAgent {
    child: tokio::process::Child,
    stdin: Arc<Mutex<ChildStdin>>,
    stdout_reader: Arc<Mutex<BufReader<ChildStdout>>>,
    config: PiConfig,
}

impl PiAgent {
    /// Spawn a new pi process
    pub async fn spawn(config: PiConfig) -> Result<Self, PiError> {
        let mut cmd = Command::new("pi");
        // Use `rpc` mode, not `json`. `json` mode is a one-shot `print` mode:
        // pi reads all of stdin, only acts on it when stdin is closed, and
        // then exits. That doesn't work for a long-lived per-session agent.
        // `rpc` mode keeps pi alive, reads JSON commands line-by-line from
        // stdin, and streams events back on stdout.
        cmd.arg("--mode").arg("rpc")
           .arg("--no-builtin-tools")
           // Disable pi's auto-discovery of user-installed
           // extensions (the files under
           // `~/.pi/agent/extensions/`). We only want the
           // forge-tools extension loaded. This is a
           // stability and security boundary: a user
           // extension that captures the pi ctx in a
           // `session_start` handler and references it from
           // a periodic timer (e.g. `setInterval`) goes
           // stale after the session-replacement logic in
           // the durable-resume path, and pi's
           // `assertActive` check throws an unhandled
           // error that kills the whole pi process.
           // Disabling auto-discovery makes forge's
           // runtime deterministic across machines.
           // Explicit `-e` paths below still work.
           .arg("--no-extensions")
           .arg("--extension").arg(&config.forge_tools_extension)
           .arg("--no-skills")
           .arg("--no-prompt-templates")
           .arg("--provider").arg(&config.provider)
           .arg("--model").arg(&config.model)
           .arg("--thinking").arg("medium");

        // Session handling: either load a prior session
        // file at startup (durable-resume path), or run
        // with no on-disk session at all (brand-new
        // session; pi uses an in-memory session that
        // doesn't get persisted to ~/.pi/agent/sessions/).
        // --no-session and --session are mutually exclusive
        // in pi's CLI; we pick one based on whether a
        // session_path was provided.
        if let Some(ref path) = config.session_path {
            cmd.arg("--session").arg(path.as_os_str());
            tracing::info!(
                session_id = %config.session_id,
                session_path = %path.display(),
                "spawning pi with --session (durable resume)"
            );
        } else {
            cmd.arg("--no-session");
            tracing::info!(
                session_id = %config.session_id,
                "spawning pi with --no-session (fresh context)"
            );
        }

        cmd.current_dir(&config.working_dir)
           .env("FORGE_API_URL", &config.forge_api_url)
           .env("FORGE_SESSION_ID", config.session_id.to_string())
           .stdout(Stdio::piped())
           // Send stderr to the inherited handle so it ends up in the
           // journal (when running under systemd) or the parent shell. We
           // deliberately avoid `Stdio::piped()` for stderr because if we
           // never drained it, pi would block once the 64KB pipe buffer
           // filled up and the agent would appear to hang.
           .stderr(Stdio::inherit())
           .stdin(Stdio::piped());

        // Set API key based on provider
        if let Some(ref key) = config.api_key {
            match config.provider.to_lowercase().as_str() {
                "openai" => { cmd.env("OPENAI_API_KEY", key); }
                "anthropic" | "proxy-anthropic" => { cmd.env("ANTHROPIC_API_KEY", key); }
                _ => { cmd.env("ANTHROPIC_API_KEY", key); }
            }
        }

        if let Some(ref base_url) = config.base_url {
            cmd.env("ANTHROPIC_BASE_URL", base_url);
        }

        tracing::info!("Spawning pi process for session {}", config.session_id);

        let mut child = cmd.spawn().map_err(|e| PiError::SpawnFailed(e.to_string()))?;

        let stdin = child.stdin.take()
            .ok_or_else(|| PiError::SpawnFailed("Failed to capture stdin".to_string()))?;
        let stdout = child.stdout.take()
            .ok_or_else(|| PiError::SpawnFailed("Failed to capture stdout".to_string()))?;

        let stdout_reader = BufReader::new(stdout);

        Ok(Self {
            child,
            stdin: Arc::new(Mutex::new(stdin)),
            stdout_reader: Arc::new(Mutex::new(stdout_reader)),
            config,
        })
    }

    /// Send a message to pi
    pub async fn send_message(&mut self, text: &str) -> Result<(), PiError> {
        let prompt = PiInput::Prompt { text: text.to_string() };
        self.send(&prompt).await
    }

    /// Send a tool result back to pi
    pub async fn send_tool_result(&mut self, id: &str, content: &str, error: Option<bool>) -> Result<(), PiError> {
        let result = PiInput::ToolResult { id: id.to_string(), content: content.to_string(), is_error: error };
        self.send(&result).await
    }

    /// Send a `switch_session` RPC command and wait for the
    /// matching `Response` event. Used by the durable-resume
    /// path: the harness writes the prior conversation to a
    /// session jsonl file (using the [`crate::session_replay`]
    /// writer), then asks the fresh pi to load it as the new
    /// active session. The model sees the prior conversation
    /// as a proper tree of structured messages (UserMessage /
    /// AssistantMessage / ToolResultMessage) — strictly better
    /// Wait for an RPC `{"type":"response","command":"<cmd>"}`
    /// envelope on pi's stdout, draining any other lines (such as
    /// `session` events) along the way. Returns the parsed
    /// envelope. Times out after `timeout`.
    ///
    async fn send(&mut self, input: &PiInput) -> Result<(), PiError> {
        let json = serde_json::to_string(input)
            .map_err(|e| PiError::Serialization(e.to_string()))?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(json.as_bytes()).await.map_err(|e| PiError::Io(e.to_string()))?;
        stdin.write_all(b"\n").await.map_err(|e| PiError::Io(e.to_string()))?;
        stdin.flush().await.map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    /// Read the next line from stdout
    pub async fn read_line(&mut self) -> Result<Option<String>, PiError> {
        let mut reader = self.stdout_reader.lock().await;
        let mut line = String::new();
        match tokio::time::timeout(std::time::Duration::from_secs(120), reader.read_line(&mut line)).await {
            Ok(Ok(0)) => Ok(None),
            Ok(Ok(_)) => Ok(Some(line)),
            Ok(Err(e)) => Err(PiError::Io(e.to_string())),
            Err(_) => Err(PiError::Timeout),
        }
    }

    /// Drain any pending events from the stdout buffer without blocking.
    ///
    /// When pi handles multiple prompts in sequence (RPC mode), the
    /// `turn_end` / `agent_end` events from the previous response can
    /// still be sitting in the read buffer. If the next call to
    /// [`PiAgent::read_line`] picks those up first, it will see an
    /// `agent_end` immediately and exit its loop before the new turn has
    /// even started. Callers should invoke this method after acquiring
    /// the agent lock and before sending the next prompt.
    pub async fn drain_pending_events(&mut self) {
        // Read lines with a tiny per-line timeout. We give up as soon as
        // one read returns nothing - that means the pipe is empty for
        // now and it's safe to send the next prompt.
        loop {
            match tokio::time::timeout(
                std::time::Duration::from_millis(50),
                self.read_line(),
            )
            .await
            {
                Ok(Ok(Some(_line))) => continue,
                _ => return,
            }
        }
    }

    /// Wait for initial session event.
    ///
    /// NOTE: pi does not emit the `session` event until it receives a prompt
    /// on stdin, so this is only useful when a prompt is already in flight or
    /// when callers have sent an initial probe message. Most callers should
    /// simply call [`PiAgent::send_message`] and then read events in a loop.
    pub async fn wait_for_session(&mut self) -> Result<(), PiError> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);

        while start.elapsed() < timeout {
            if let Ok(Some(line)) = self.read_line().await {
                if let Ok(event) = serde_json::from_str::<PiEvent>(&line) {
                    if matches!(event, PiEvent::Session { .. }) {
                        tracing::info!("Pi session ready");
                        return Ok(());
                    }
                }
            }
        }
        Err(PiError::Timeout)
    }

    /// Wait for process to exit
    pub async fn wait(&mut self) -> Result<(), PiError> {
        self.child.wait().await.map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    /// Kill the process
    pub async fn kill(&mut self) -> Result<(), PiError> {
        self.child.kill().await.map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }
}

/// Wait for an RPC `{"type":"response","command":"<cmd>"}`
/// envelope on pi's stdout, draining any other lines (such as
/// `session` events) along the way. Returns the parsed
/// envelope. Times out after `timeout`.
///
#[derive(Debug, thiserror::Error)]
pub enum PiError {
    #[error("Failed to spawn pi: {0}")]
    SpawnFailed(String),
    #[error("IO error: {0}")]
    Io(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Timeout waiting for pi response")]
    Timeout,
}
