#![allow(dead_code)]

//! pi Agent Integration
//!
//! Spawns pi as a subprocess and manages the agent session.
//! Uses pi's JSON mode for event streaming and message passing.
//!
//! ## Architecture
//!
//! pi runs on the host as the agent harness. Tools are NOT executed
//! by pi - they are delegated to Forge's Tool API and executed in the
//! sandbox container via the forge-tools extension.
//!
//! ## Decoupled Design
//!
//! **pi runs on the host. Tools execute in the sandbox.**
//!
//! This provides resilience:
//! - Agent runs `rm -rf /` -> only sandbox destroyed, pi survives
//! - pi never directly accesses files
//! - Forge delegates tool execution to sandbox container

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Command, ChildStdin, ChildStdout};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::{Profile, Session};
use sqlx::PgPool;

/// Configuration for spawning pi
#[derive(Debug, Clone)]
pub struct PiConfig {
    /// Working directory path (in sandbox)
    pub working_dir: String,
    /// Provider name (openai, anthropic, etc.)
    pub provider: String,
    /// Model name (gpt-4o, claude-sonnet-4-20250514, etc.)
    pub model: String,
    /// Base URL for API (optional, uses provider default)
    pub base_url: Option<String>,
    /// API key (passed via environment to pi)
    pub api_key: Option<String>,
    /// System prompt
    pub system_prompt: String,
    /// Tool names to enable (bash, read, write, edit)
    /// NOTE: Tools are executed in sandbox, not by pi directly
    pub tools: Vec<String>,
    /// nix shell expression (optional, for sandbox)
    pub nix_shell: Option<String>,
    /// Path to forge-tools extension
    pub forge_tools_extension: PathBuf,
    /// Forge API URL
    pub forge_api_url: String,
    /// Session ID for tool delegation
    pub session_id: Uuid,
}

/// pi JSON mode event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PiEvent {
    #[serde(rename = "message_start")]
    MessageStart { id: String },

    #[serde(rename = "text_delta")]
    TextDelta { delta: String },

    #[serde(rename = "thinking_delta")]
    ThinkingDelta { delta: String },

    #[serde(rename = "tool_call")]
    ToolCall {
        id: String,
        name: String,
        input: serde_json::Value,
    },

    #[serde(rename = "tool_result")]
    ToolResult {
        id: String,
        #[serde(rename = "content")]
        output: String,
        #[serde(rename = "is_error")]
        error: Option<bool>,
    },

    #[serde(rename = "message_end")]
    MessageEnd { id: String },

    #[serde(rename = "error")]
    Error { message: String },

    #[serde(rename = "agent_start")]
    AgentStart,

    #[serde(rename = "agent_end")]
    AgentEnd,
}

/// Messages sent to pi's stdin
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PiInput {
    #[serde(rename = "prompt")]
    Prompt { text: String },

    #[serde(rename = "tool_result")]
    ToolResult {
        id: String,
        content: String,
        #[serde(rename = "is_error")]
        error: Option<bool>,
    },

    #[serde(rename = "abort")]
    Abort,
}

/// Agent events for external consumption
#[derive(Debug, Clone)]
pub enum AgentEvent {
    TextDelta(String),
    ThinkingDelta(String),
    ToolCall { id: String, name: String, input: serde_json::Value },
    ToolResult { id: String, output: String, error: Option<bool> },
    MessageEnd,
    Error(String),
}

/// pi Agent subprocess manager
pub struct PiAgent {
    child: tokio::process::Child,
    stdin: Arc<Mutex<ChildStdin>>,
    stdout_reader: Arc<Mutex<BufReader<ChildStdout>>>,
    config: PiConfig,
}

impl PiAgent {
    /// Spawn a new pi process with the given configuration
    pub async fn spawn(config: PiConfig) -> Result<Self, PiError> {
        // Build pi command
        let mut cmd = Command::new("pi");

        // JSON mode for programmatic access
        cmd.arg("--mode").arg("json");

        // Don't save sessions to files (we use PostgreSQL)
        cmd.arg("--no-session");

        // DISABLE BUILT-IN TOOLS - tools come from forge-tools extension
        cmd.arg("--no-builtin-tools");

        // Load forge-tools extension for tool delegation
        cmd.arg("--extension").arg(&config.forge_tools_extension);

        // Don't load skills/prompts (optional extensions)
        cmd.arg("--no-skills");
        cmd.arg("--no-prompt-templates");

        // Set working directory
        cmd.current_dir(&config.working_dir);

        // Model configuration
        cmd.arg("--provider").arg(&config.provider);
        cmd.arg("--model").arg(&config.model);

        // Forge API URL and Session ID for tool delegation
        cmd.env("FORGE_API_URL", &config.forge_api_url);
        cmd.env("FORGE_SESSION_ID", config.session_id.to_string());

        // API key via environment
        if let Some(ref key) = config.api_key {
            let key_clone = key.clone();
            match config.provider.to_lowercase().as_str() {
                "openai" => {
                    cmd.env("OPENAI_API_KEY", key_clone);
                }
                "anthropic" => {
                    cmd.env("ANTHROPIC_API_KEY", key_clone);
                }
                "google" | "gemini" => {
                    cmd.env("GOOGLE_API_KEY", key_clone);
                }
                _ => {}
            }
        }

        // Thinking level (could be configurable)
        cmd.arg("--thinking").arg("medium");

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.stdin(Stdio::piped());

        tracing::info!("Spawning pi process");

        // Spawn the process
        let mut child = cmd.spawn().map_err(|e| PiError::SpawnFailed(e.to_string()))?;

        let stdin = child.stdin.take()
            .ok_or_else(|| PiError::SpawnFailed("Failed to capture stdin".to_string()))?;

        let stdout = child.stdout.take()
            .ok_or_else(|| PiError::SpawnFailed("Failed to capture stdout".to_string()))?;

        // pi writes to stdout
        let stdout_reader = BufReader::new(stdout);

        // Send system prompt as first message
        let mut agent = Self {
            child,
            stdin: Arc::new(Mutex::new(stdin)),
            stdout_reader: Arc::new(Mutex::new(stdout_reader)),
            config,
        };

        // Set system prompt
        agent.set_system_prompt().await?;

        Ok(agent)
    }

    /// Set the system prompt
    async fn set_system_prompt(&mut self) -> Result<(), PiError> {
        let prompt = PiInput::Prompt {
            text: format!("/system\n{}", self.config.system_prompt)
        };
        self.send(&prompt).await?;
        Ok(())
    }

    /// Send a message to pi
    pub async fn send_message(&mut self, text: &str) -> Result<(), PiError> {
        let prompt = PiInput::Prompt { text: text.to_string() };
        self.send(&prompt).await
    }

    /// Send a tool result back to pi
    pub async fn send_tool_result(&mut self, id: &str, content: &str, error: Option<bool>) -> Result<(), PiError> {
        let result = PiInput::ToolResult {
            id: id.to_string(),
            content: content.to_string(),
            error,
        };
        self.send(&result).await
    }

    /// Send input to pi's stdin
    async fn send(&mut self, input: &PiInput) -> Result<(), PiError> {
        let json = serde_json::to_string(input)
            .map_err(|e| PiError::Serialization(e.to_string()))?;

        let mut stdin = self.stdin.lock().await;
        stdin.write_all(json.as_bytes()).await.map_err(|e| PiError::Io(e.to_string()))?;
        stdin.write_all(b"\n").await.map_err(|e| PiError::Io(e.to_string()))?;
        stdin.flush().await.map_err(|e| PiError::Io(e.to_string()))?;

        Ok(())
    }

    /// Read the next line from stdout (blocking read)
    pub async fn read_line(&mut self) -> Result<Option<String>, PiError> {
        let mut reader = self.stdout_reader.lock().await;
        let mut line = String::new();

        match tokio::time::timeout(
            std::time::Duration::from_secs(120),
            reader.read_line(&mut line)
        ).await {
            Ok(Ok(0)) => Ok(None), // EOF
            Ok(Ok(_)) => Ok(Some(line)),
            Ok(Err(e)) => Err(PiError::Io(e.to_string())),
            Err(_) => Err(PiError::Timeout),
        }
    }

    /// Wait for the process to exit
    pub async fn wait(&mut self) -> Result<(), PiError> {
        self.child.wait().await.map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    /// Kill the process
    pub async fn kill(&mut self) -> Result<(), PiError> {
        self.child.kill().await.map_err(|e| PiError::Io(e.to_string()))?;
        Ok(())
    }

    /// Get the PID of the child process
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }
}

/// Errors from pi agent operations
#[derive(Debug, thiserror::Error)]
pub enum PiError {
    #[error("Failed to spawn pi: {0}")]
    SpawnFailed(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("pi process failed: {0}")]
    PiProcessFailed(String),

    #[error("Timeout waiting for pi response")]
    Timeout,
}

/// Process a session using pi as the agent runtime
pub async fn process_with_pi(
    pool: &PgPool,
    session_id: Uuid,
    user_message: &str,
) -> Result<String, PiProcessError> {
    // Get Forge API URL from environment
    let forge_api_url = std::env::var("FORGE_API_URL")
        .unwrap_or_else(|_| "http://localhost:8080/api/v1".to_string());

    // Get forge-tools extension path
    let forge_tools_extension = std::env::var("FORGE_TOOLS_EXTENSION")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // Default to relative path from crate
            let crate_dir = std::env::var("CARGO_MANIFEST_DIR")
                .unwrap_or_else(|_| ".".to_string());
            PathBuf::from(crate_dir)
                .join("../../extensions/forge-tools/dist/index.js")
        });

    // Get session and profile
    let session: Session = sqlx::query_as("SELECT * FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_one(pool)
        .await
        .map_err(|e| PiProcessError::Database(e.to_string()))?;

    let profile: Profile = sqlx::query_as("SELECT * FROM profiles WHERE id = $1")
        .bind(session.profile_id)
        .fetch_one(pool)
        .await
        .map_err(|e| PiProcessError::Database(e.to_string()))?;

    // Build pi config
    let tools: Vec<String> = serde_json::from_str(&profile.tools)
        .unwrap_or_else(|_| vec!["bash".to_string(), "read".to_string(), "write".to_string(), "edit".to_string()]);

    let config = PiConfig {
        working_dir: profile.working_dir.clone(),
        provider: profile.provider.clone(),
        model: profile.model.clone(),
        base_url: profile.base_url.clone(),
        api_key: profile.api_key.clone()
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
            .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok()),
        system_prompt: profile.system_prompt.clone(),
        tools,
        nix_shell: profile.nix_shell.clone(),
        forge_tools_extension,
        forge_api_url,
        session_id,
    };

    // Spawn pi agent
    let mut agent = PiAgent::spawn(config)
        .await
        .map_err(|e| PiProcessError::AgentSpawn(e.to_string()))?;

    tracing::info!("Spawned pi process with PID: {:?}", agent.id());

    // Send user message
    agent.send_message(user_message).await
        .map_err(|e| PiProcessError::AgentCommunication(e.to_string()))?;

    // Process events from stdout
    // NOTE: With forge-tools extension, tool calls are handled by the extension
    // which forwards them to Forge's /tools/execute API. The extension then
    // sends the tool_result back to pi automatically.
    //
    // So we just need to handle text output and errors here.
    let mut final_text = String::new();

    while let Ok(Some(line)) = agent.read_line().await {
        // Parse the event
        if let Ok(event) = serde_json::from_str::<PiEvent>(&line) {
            match event {
                PiEvent::TextDelta { delta } => {
                    final_text.push_str(&delta);
                    print!("{}", delta);
                    std::io::stdout().flush().ok();
                }
                PiEvent::ThinkingDelta { delta } => {
                    tracing::debug!("[thinking] {}", delta);
                }
                PiEvent::ToolCall { id: _, name, input } => {
                    // Tool call received - this is forwarded to forge-tools extension
                    // The extension handles the API call and sends result back to pi
                    tracing::info!("Tool call (forwarded to forge-tools): {} with {:?}", name, input);
                    
                    // Send tool result back to pi (handled by extension)
                    // The forge-tools extension intercepts and handles this automatically
                }
                PiEvent::ToolResult { id, output: _, error } => {
                    tracing::info!("Tool result from extension: {} (error: {:?})", id, error);
                    // This comes from the forge-tools extension
                }
                PiEvent::MessageEnd { .. } => {
                    println!(); // newline after response
                    break;
                }
                PiEvent::Error { message } => {
                    eprintln!("pi error: {}", message);
                    return Err(PiProcessError::AgentCommandError(message));
                }
                PiEvent::AgentStart => {
                    tracing::debug!("Agent started");
                }
                PiEvent::AgentEnd => {
                    tracing::debug!("Agent ended");
                }
                PiEvent::MessageStart { id } => {
                    tracing::debug!("Message started: {}", id);
                }
            }
        }
    }

    // Update session
    sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
        .bind(session_id)
        .execute(pool)
        .await
        .map_err(|e| PiProcessError::Database(e.to_string()))?;

    // Store the assistant's response as a message
    let sequence: i32 = sqlx::query_scalar("SELECT get_next_sequence($1)")
        .bind(session_id)
        .fetch_one(pool)
        .await
        .map_err(|e| PiProcessError::Database(e.to_string()))?;

    sqlx::query(
        r#"
        INSERT INTO messages (session_id, sequence, role, content)
        VALUES ($1, $2, 'assistant', $3)
        "#,
    )
    .bind(session_id)
    .bind(sequence)
    .bind(&final_text)
    .execute(pool)
    .await
    .map_err(|e| PiProcessError::Database(e.to_string()))?;

    tracing::info!("Stored assistant response for session {}", session_id);

    Ok(final_text)
}

/// Errors from session processing
#[derive(Debug, thiserror::Error)]
pub enum PiProcessError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Failed to spawn agent: {0}")]
    AgentSpawn(String),

    #[error("Agent communication error: {0}")]
    AgentCommunication(String),

    #[error("Agent returned an error: {0}")]
    AgentCommandError(String),
}
