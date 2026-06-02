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
    /// Load a different session file. The session file is a
    /// pi-format jsonl; the new active session is the loaded
    /// one, and the model sees its full conversation as
    /// context. Used by the durable-resume path: forge writes
    /// the prior conversation to a jsonl from the
    /// `messages` table, then asks the fresh pi to load it
    /// as the new active session. See
    /// [`PiAgent::switch_session`].
    #[serde(rename = "switch_session")]
    SwitchSession {
        /// Absolute path to the session jsonl to load.
        #[serde(rename = "sessionPath")]
        session_path: std::path::PathBuf,
        /// Optional request id for correlating with the
        /// matching `Response` event. When `Some`, the
        /// response will carry the same id. When `None`,
        /// the command is fire-and-forget for response
        /// correlation (pi still emits a Response, but
        /// without an id).
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
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
           .arg("--no-session")
           .arg("--no-builtin-tools")
           // Disable pi's auto-discovery of user-installed
           // extensions (the files under
           // `~/.pi/agent/extensions/`). We only want the
           // forge-tools extension loaded. This is a security
           // boundary as well as a stability one: a user
           // extension that captures the pi ctx in a
           // `session_start` handler and references it from a
           // periodic timer (e.g. `setInterval`) goes stale
           // after the `switch_session` RPC command that the
           // durable-resume path issues, and pi's
           // `assertActive` check throws an unhandled error
           // that kills the whole pi process. Disabling
           // auto-discovery makes forge's runtime
           // deterministic across machines. Explicit `-e`
           // paths below still work.
           .arg("--no-extensions")
           .arg("--extension").arg(&config.forge_tools_extension)
           .arg("--no-skills")
           .arg("--no-prompt-templates")
           .arg("--provider").arg(&config.provider)
           .arg("--model").arg(&config.model)
           .arg("--thinking").arg("medium")
           .current_dir(&config.working_dir)
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
    /// than the older "prepend the transcript as one giant
    /// user message" approach, which lost `tool_input` /
    /// `tool_output` structure and broke the model's view of
    /// long sessions.
    ///
    /// `request_id` should be unique per call so the matching
    /// `Response` event can be correlated unambiguously. pi
    /// will reject duplicate ids in flight, so callers that
    /// run multiple resumes in parallel need different ids
    /// (a uuid is the right shape).
    ///
    /// The 30-second total deadline is generous: pi loads the
    /// jsonl synchronously, then sends a `Session` event
    /// followed by a `Response` event for the command. The
    /// `Response` event carries `success: true` /
    /// `cancelled: true` / `success: false` — see
    /// `SwitchSessionResult` for the three outcomes.
    ///
    /// We use `switch_session` rather than the superficially
    /// similar `new_session` RPC because `new_session` only
    /// records the parent session file in the new session's
    /// header (for lineage tracking); it does not load the
    /// parent's messages into the model context. The prior
    /// commit's "durable resume via new_session" path was
    /// therefore broken: pi reported success but the model
    /// responded with "I don't have access to session
    /// history." `switch_session` is the real "load this
    /// session file as the new active session" verb.
    pub async fn switch_session(
        &mut self,
        session_path: &std::path::Path,
        request_id: &str,
    ) -> Result<SwitchSessionResult, PiError> {
        self.send(&PiInput::SwitchSession {
            session_path: session_path.to_path_buf(),
            id: Some(request_id.to_string()),
        })
        .await?;

        let start = std::time::Instant::now();
        let deadline = std::time::Duration::from_secs(30);
        while start.elapsed() < deadline {
            // Per-line read with a 5s timeout; we expect a
            // `Session` event first (the loaded session's id),
            // then the `Response` event for our request. Any
            // other events are part of session bookkeeping
            // and are consumed here so they don't pollute
            // the harness's event stream later.
            let line = match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.read_line(),
            )
            .await
            {
                Ok(Ok(Some(l))) => l,
                Ok(Ok(None)) => {
                    return Err(PiError::Io(
                        "pi process ended during switch_session".to_string(),
                    ));
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(PiError::Timeout);
                }
            };

            let event: PiEvent = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        line = ?line,
                        "switch_session: ignoring unparseable line from pi"
                    );
                    continue;
                }
            };

            match event {
                PiEvent::Session { version, id } => {
                    tracing::info!(
                        version,
                        loaded_session_id = %id,
                        "switch_session: pi reported loaded session"
                    );
                }
                PiEvent::Response { command, success, id: Some(resp_id) } if resp_id == request_id => {
                    if command != "switch_session" {
                        return Err(PiError::Io(format!(
                            "unexpected response for command {command} (wanted switch_session)"
                        )));
                    }
                    if !success {
                        // pi reports `success: false` when an
                        // extension cancelled via the
                        // `session_before_switch` event handler.
                        // We surface that distinctly from a
                        // hard failure (extension data parsing
                        // error, missing file, etc.) so the
                        // caller can decide whether to fall
                        // back to a fresh context.
                        let raw: serde_json::Value =
                            serde_json::from_str(&line).unwrap_or(serde_json::Value::Null);
                        let cancelled = raw
                            .get("data")
                            .and_then(|d| d.get("cancelled"))
                            .and_then(|c| c.as_bool())
                            .unwrap_or(false);
                        if cancelled {
                            tracing::warn!(
                                "switch_session: extension cancelled the session switch; continuing with a fresh context"
                            );
                            return Ok(SwitchSessionResult::Cancelled);
                        }
                        let error = raw
                            .get("error")
                            .and_then(|e| e.as_str())
                            .unwrap_or("(no error message)")
                            .to_string();
                        return Err(PiError::Io(format!(
                            "switch_session failed: {error}"
                        )));
                    }
                    return Ok(SwitchSessionResult::Ok);
                }
                // Stray events that aren't ours: a
                // `message_start` / `message_end` for the
                // session-init chatter, or a stray extension
                // event. Consume and continue looking for our
                // response.
                other => {
                    tracing::debug!(
                        event = ?other,
                        "switch_session: consuming pre-response event from pi"
                    );
                }
            }
        }

        Err(PiError::Timeout)
    }

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

/// Outcome of [`PiAgent::switch_session`]. The three states
/// map onto three durable-resume decisions:
///
/// - `Ok` — the fresh pi has loaded the session jsonl and
///   the model now has the full prior conversation as
///   structured messages. This is the happy path.
/// - `Cancelled` — an extension vetoed the session switch via
///   pi's `session_before_switch` event. The fresh pi is
///   alive but has no parent; the caller can choose to retry
///   with a different parent (e.g. a shorter jsonl, no
///   parent) or to send the user's prompt into a fresh empty
///   context.
/// - `Err(_)` — the command failed for some other reason
///   (jsonl file was missing or malformed, the agent's RPC
///   channel closed, etc.). The caller should fall back to a
///   fresh context; the messages table is still the source
///   of truth for the prior conversation.
#[derive(Debug)]
pub enum SwitchSessionResult {
    Ok,
    Cancelled,
}
