//! pi Agent Integration - Simplified
//!
//! Spawns pi as a subprocess for session-based agent interactions.
//! Tools are executed via the forge-tools extension which calls back to the API.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
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
    /// Optional path to a directory of pi skill packs (one
    /// subdirectory per skill, with a `SKILL.md` inside).
    /// When `Some`, pi is launched with `--skills-dir
    /// <path>` instead of `--no-skills`, so the agent can
    /// discover and load skills at runtime. The
    /// `skills/` directory at the forge repo root is the
    /// canonical home (the operator can override via
    /// `FORGE_SKILLS_DIR`); the default is shipped as
    /// part of the repo so the agent has stable,
    /// versioned skill content that doesn't depend on
    /// whichever machine happens to run forge-api.
    /// `None` keeps the legacy `--no-skills` behavior
    /// for tests / minimal builds.
    pub skills_dir: Option<PathBuf>,
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
///
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
    ExtensionUiRequest { id: String, method: String },
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
    ToolResult {
        id: String,
        content: String,
        is_error: Option<bool>,
    },
    #[serde(rename = "compact")]
    Compact {
        #[serde(rename = "customInstructions", skip_serializing_if = "Option::is_none")]
        custom_instructions: Option<String>,
    },
    #[serde(rename = "abort")]
    Abort,
}

/// pi Agent subprocess manager
pub struct PiAgent {
    child: tokio::process::Child,
    stdin: Arc<Mutex<ChildStdin>>,
    stdout_reader: Arc<Mutex<BufReader<ChildStdout>>>,
    #[allow(dead_code)]
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
        cmd.arg("--mode")
            .arg("rpc")
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
            .arg("--extension")
            .arg(&config.forge_tools_extension);

        // Skills: by default we keep pi's auto-discovery
        // off (the historical behavior), so the only
        // way a skill enters the agent's context is via
        // `--skills-dir <path>` (an explicit, repo-bundled
        // directory the operator controls). The
        // `skills/` tree at the repo root is the
        // canonical home; operators override with
        // `FORGE_SKILLS_DIR` (see `AgentRegistry::new`).
        //
        // We deliberately avoid pi's user-level
        // `~/.pi/agent/skills/` discovery: it depends on
        // which user the systemd unit runs as and which
        // machines have which `~/.pi/agent/skills/`
        // checked out, which would make the agent's
        // skill set non-deterministic across deploys.
        match &config.skills_dir {
            Some(dir) => {
                cmd.arg("--skills-dir").arg(dir.as_os_str());
                tracing::info!(
                    session_id = %config.session_id,
                    skills_dir = %dir.display(),
                    "enabling pi skills from explicit directory"
                );
            }
            None => {
                cmd.arg("--no-skills");
            }
        }

        cmd.arg("--no-prompt-templates")
            .arg("--provider")
            .arg(&config.provider)
            .arg("--model")
            .arg(&config.model)
            .arg("--thinking")
            .arg("medium");

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
                "openai" => {
                    cmd.env("OPENAI_API_KEY", key);
                }
                "anthropic" | "proxy-anthropic" => {
                    cmd.env("ANTHROPIC_API_KEY", key);
                }
                _ => {
                    cmd.env("ANTHROPIC_API_KEY", key);
                }
            }
        }

        if let Some(ref base_url) = config.base_url {
            cmd.env("ANTHROPIC_BASE_URL", base_url);
        }

        tracing::info!("Spawning pi process for session {}", config.session_id);

        let mut child = cmd
            .spawn()
            .map_err(|e| PiError::SpawnFailed(e.to_string()))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| PiError::SpawnFailed("Failed to capture stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
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
        let prompt = PiInput::Prompt {
            text: text.to_string(),
        };
        self.send(&prompt).await
    }

    /// Send a tool result back to pi
    pub async fn send_tool_result(
        &mut self,
        id: &str,
        content: &str,
        error: Option<bool>,
    ) -> Result<(), PiError> {
        let result = PiInput::ToolResult {
            id: id.to_string(),
            content: content.to_string(),
            is_error: error,
        };
        self.send(&result).await
    }

    /// Send a `compact` RPC command and wait for the
    /// matching `Response` event. Used by the
    /// long-context resume path: when the prior
    /// conversation's estimated token count exceeds the
    /// model's `contextWindow - reserveTokens` threshold
    /// (default 300k for M3), forge triggers an
    /// LLM-generated compaction BEFORE sending the
    /// user's first prompt. Pi handles the summary
    /// generation (using `keepRecentTokens` worth of
    /// recent messages plus a synthesized summary of the
    /// older turns) and appends a `CompactionEntry` to
    /// the session file. After this returns, the session
    /// file has summary + recent, and subsequent
    /// `--session` loads start from a manageable
    /// context.
    ///
    /// The `custom_instructions` is forwarded to pi's
    /// `/compact` command; the harness typically passes
    /// `None` (let the model use its default
    /// summarization) or a hint to focus the summary on
    /// recent work.
    ///
    /// The wait deadline is 10 minutes. A 700k-token
    /// summary call has been observed to take 60-100s
    /// on a slow network; we leave generous headroom for
    /// the model to think. (The previous 5-minute
    /// deadline with a 60-second per-line read timeout
    /// was too tight: the LLM call would be silent for
    /// 60-90s while it thought, hit the per-line
    /// timeout, and forge would abandon the compaction
    /// even though it was about to succeed.)
    pub async fn compact(
        &mut self,
        custom_instructions: Option<&str>,
    ) -> Result<serde_json::Value, PiError> {
        let cmd = PiInput::Compact {
            custom_instructions: custom_instructions.map(|s| s.to_string()),
        };
        self.send(&cmd).await?;
        // Wait for the matching response. 10-minute
        // wall-clock deadline; per-line timeout is the
        // remaining budget so a slow LLM call doesn't
        // false-alarm.
        let deadline = std::time::Duration::from_secs(600);
        self.wait_for_response_with_command("compact", deadline)
            .await
    }

    /// Wait for an RPC `{"type":"response","command":"<cmd>"}`
    /// envelope on pi's stdout, draining any other lines (such as
    /// `session` events, `compaction_start`/`compaction_end`
    /// events emitted by the long-context resume path) along
    /// the way. Returns the parsed envelope as a JSON value so
    /// callers can read `data` for the response payload (e.g.
    /// the compact result's `summary`, `firstKeptEntryId`,
    /// `tokensBefore`). Times out after `timeout`.
    ///
    async fn send(&mut self, input: &PiInput) -> Result<(), PiError> {
        let json =
            serde_json::to_string(input).map_err(|e| PiError::Serialization(e.to_string()))?;
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(json.as_bytes())
            .await
            .map_err(|e| PiError::Io(e.to_string()))?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|e| PiError::Io(e.to_string()))?;
        stdin
            .flush()
            .await
            .map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    /// Read the next line from stdout.
    ///
    /// No inner timeout. Callers wrap this with their own
    /// `tokio::time::timeout` — the event loop in
    /// [`crate::api`] uses `TOOL_READ_TIMEOUT_SECS` (1+ hr) when
    /// a tool is in flight, and `IDLE_READ_TIMEOUT_SECS` (5 min)
    /// otherwise. `drain_pending_events` uses a 50 ms timeout.
    ///
    /// The previous implementation hardcoded a 120 s inner
    /// timeout that returned [`PiError::Timeout`]. The event
    /// loop treated that as a hard error and killed the pi
    /// process, which meant a long-running tool call (anything
    /// the model legitimately asked `timeout_ms` for) would
    /// have its turn aborted after 120 s of pi-silence —
    /// before the tool's own outer grace could fire, before
    /// the inner `timeout --kill-after=2` could escalate
    /// SIGTERM -> SIGKILL, and before the bash tool's
    /// `timeout_ms` had any chance to elapse. EOF (`Ok(0)`)
    /// and IO errors are still returned directly so the caller
    /// can distinguish "pi died" from "pi is silent".
    pub async fn read_line(&mut self) -> Result<Option<String>, PiError> {
        let mut reader = self.stdout_reader.lock().await;
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(line)),
            Err(e) => Err(PiError::Io(e.to_string())),
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
            match tokio::time::timeout(std::time::Duration::from_millis(50), self.read_line()).await
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

    /// Drain stdout until an RPC `{"type":"response","command":<cmd>}`
    /// envelope arrives, returning the parsed envelope. Any other lines
    /// (`session` events, `compaction_start`/`compaction_end` lifecycle
    /// events, etc.) are read and discarded. The deadline is enforced as
    /// a wall-clock budget. The per-line timeout is the
    /// remaining budget, so an outer `timeout` of 5 minutes
    /// gives pi up to 5 minutes of total silence before we
    /// give up — long enough for a slow LLM summary call on
    /// a 700k-token context (which can be silent for 60-90s
    /// while the model thinks).
    pub async fn wait_for_response_with_command(
        &mut self,
        command: &str,
        timeout: std::time::Duration,
    ) -> Result<serde_json::Value, PiError> {
        let start = std::time::Instant::now();
        loop {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(PiError::Timeout);
            }
            // Per-line timeout is the remaining wall-clock
            // budget. We use a 5-second floor so a hung
            // stdout doesn't pin the loop forever waiting
            // for the outer check; the outer check happens
            // at the start of each iteration so even on the
            // worst case (each line just hits the per-line
            // floor) we make progress through the loop
            // roughly every 5s.
            let per_line = remaining.max(std::time::Duration::from_secs(5));
            let line = match tokio::time::timeout(per_line, self.read_line_unbounded()).await {
                Ok(Ok(Some(line))) => line,
                Ok(Ok(None)) => return Err(PiError::Io("pi stdout closed".to_string())),
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(PiError::Timeout),
            };
            // Try to parse as a generic JSON value; if it has
            // a matching `command` field, return it.
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                let cmd = v.get("command").and_then(|c| c.as_str()).unwrap_or("");
                if ty == "response" && cmd == command {
                    return Ok(v);
                }
                // For the compact path, the harness also
                // wants to know when the compaction
                // finished so it can log progress. Pi emits
                // a `compaction_end` event before the
                // `response`; we just drop it here.
            }
        }
    }

    /// Read the next line from stdout without the 120s
    /// timeout. Used by [`Self::wait_for_response_with_command`]
    /// which has its own outer deadline.
    async fn read_line_unbounded(&mut self) -> Result<Option<String>, PiError> {
        let mut reader = self.stdout_reader.lock().await;
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(line)),
            Err(e) => Err(PiError::Io(e.to_string())),
        }
    }

    /// Wait for process to exit
    pub async fn wait(&mut self) -> Result<(), PiError> {
        self.child
            .wait()
            .await
            .map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    /// Kill the process
    pub async fn kill(&mut self) -> Result<(), PiError> {
        self.child
            .kill()
            .await
            .map_err(|e| PiError::Io(e.to_string()))?;
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
