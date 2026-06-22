//! OpenAI-compatible API surface.
//!
//! Exposes `POST /v1/chat/completions` and `GET /v1/models` so any
//! client that speaks the OpenAI Chat Completions API — the `openai`
//! SDK, LangChain, Continue, any number of chat UIs — can drive a
//! forge agent without learning forge's native `/messages` +
//! `/sessions` + `/profiles` API.
//!
//! ## Authentication
//!
//! OpenAI clients authenticate with `Authorization: Bearer <key>`.
//! `auth::extract_auth_user` accepts either that or forge's native
//! `X-API-Key` header, so the same forge API key (`sk_forge_…`) works
//! here. Set it as the OpenAI API key in your client and point the
//! base URL at forge.
//!
//! ## Model → profile mapping
//!
//! The OpenAI `model` field selects the backend. In forge the backend
//! is a **profile** (provider + model + system prompt + tools + sandbox
//! config), so a profile *is* a "model" from the client's perspective:
//!
//! - `model: "<profile-name>"` — **stateless**. A fresh ephemeral
//!   session is created for the request, the request's `messages`
//!   array is replayed into the session as prior context, the agent
//!   runs one turn, and the final assistant text is returned. This
//!   matches OpenAI's stateless semantics: the client sends the full
//!   conversation each request and gets a single response back.
//!
//! - `model: "forge:<session-uuid>"` — **stateful**. Reuses an
//!   existing forge session (which already holds its conversation
//!   state in pi). Only the last user message in the request is sent;
//!   the rest of the `messages` array is ignored (the session has the
//!   history). Use this for long-running agentic sessions where the
//!   client doesn't want to re-send history every turn.
//!
//! ## Agentic turns
//!
//! A forge agent is agentic: it can call tools (`bash`, `read`,
//! `write`, `edit`) across many internal rounds before producing its
//! final answer. From the OpenAI client's point of view the call is
//! single-turn — the client sends a prompt and gets back one
//! assistant message — but the backend may run for minutes while the
//! agent works. The HTTP request stays open until the agent's turn
//! ends (`agent_end`). Tool calls the agent makes internally are
//! forge-internal and are *not* returned as OpenAI `tool_calls`; the
//! client just receives the final text. Every assistant text chunk the
//! model produces is also persisted to the audit log (via
//! `insert_and_publish_assistant`), so OpenAI-driven turns are
//! indistinguishable from native `/messages`-driven turns in the
//! `messages` table and the live SSE event stream.
//!
//! ## Streaming
//!
//! `stream: true` emits standard OpenAI `chat.completion.chunk` SSE
//! events as the model produces text deltas, followed by a final
//! `finish_reason: "stop"` chunk and `data: [DONE]`. The first chunk
//! carries `delta.role = "assistant"`; subsequent chunks carry
//! `delta.content`; the final chunk has an empty delta.
//!
//! ## Limitations
//!
//! - `tool_calls` / `tool`-role messages in the request history are
//!   not reconstructed into the session (forge's tools are internal;
//!   clients won't have forge tool_calls to send back). Their text
//!   content is preserved; the tool round-trips are dropped from the
//!   replayed context. Pure user/assistant text conversations — the
//!   common chat-UI case — round-trip fully.
//! - `usage` is reported as zeros. forge doesn't surface per-request
//!   token counts to the harness; pi tracks usage internally but
//!   doesn't expose it on the RPC event stream.
//! - Generation parameters (`temperature`, `max_tokens`, `top_p`,
//!   `n`, …) are accepted and ignored; the profile's model settings
//!   govern generation. `n` is always 1.

use axum::{
    extract::State,
    http::HeaderMap,
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Response, Sse,
    },
    Json,
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::agent_registry::SharedPiAgent;
use crate::api::auth::extract_auth_user;
use crate::api::{insert_and_publish_assistant, AppState};
use crate::bus::MessageBus;
use crate::db::Profile;
use crate::observability::Metrics;
use crate::pi_agent::{AssistantMessageEvent, PiEvent};

use super::{IDLE_READ_TIMEOUT_SECS, TOOL_READ_TIMEOUT_SECS};

/// Sentinel prefix on the OpenAI `model` field that selects the
/// stateful "reuse an existing session" mode. `model: "forge:<uuid>"`
/// reuses the session `<uuid>`; any other value is treated as a
/// profile name and a fresh ephemeral session is created.
const SESSION_MODEL_PREFIX: &str = "forge:";

// ============================================
// Request / response shapes (OpenAI Chat Completions)
// ============================================

/// `POST /v1/chat/completions` request body. Only the fields forge
/// actually uses are strictly required; the rest are accepted and
/// ignored so unmodified OpenAI clients don't get a 422 for sending
/// `temperature` etc.
#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    /// Emit SSE `chat.completion.chunk` events instead of a single
    /// JSON response. Defaults to false (OpenAI's default).
    #[serde(default)]
    pub stream: bool,
    // Accepted-and-ignored generation parameters. Explicitly listed
    // (rather than #[serde(deny_unknown_fields)] flipped off, which is
    // the default) so the intent is self-documenting and so a future
    // change that wires one of these up doesn't have to touch the
    // struct.
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub max_completion_tokens: Option<u32>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub n: Option<u32>,
    #[serde(default)]
    pub stop: Option<serde_json::Value>,
    #[serde(default)]
    pub user: Option<String>,
}

/// One message in the `messages` array. `content` is an
/// `Option<serde_json::Value>` because OpenAI allows either a plain
/// string (`"hello"`) or an array of content parts
/// (`[{"type":"text","text":"hello"}, …]`); `normalize_content`
/// extracts the text from either shape.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    /// String or array of content parts. May be `null` for
    /// assistant messages that carry only `tool_calls`.
    #[serde(default)]
    pub content: Option<serde_json::Value>,
    /// Assistant messages may carry tool calls. Accepted but not
    /// reconstructed into the session (see the module docs).
    #[serde(default)]
    pub tool_calls: Option<Vec<serde_json::Value>>,
    /// `tool`-role messages carry the id of the call they answer.
    /// Accepted but not reconstructed.
    #[serde(default)]
    pub tool_call_id: Option<String>,
    /// Optional name on `tool` / function messages. Ignored.
    #[serde(default)]
    pub name: Option<String>,
}

/// Non-streaming response (`object: "chat.completion"`).
#[derive(Debug, Serialize)]
struct ChatCompletionResponse {
    id: String,
    object: &'static str,
    created: i64,
    model: String,
    choices: Vec<Choice>,
    usage: Usage,
}

#[derive(Debug, Serialize)]
struct Choice {
    index: u32,
    message: ResponseMessage,
    finish_reason: String,
}

#[derive(Debug, Serialize)]
struct ResponseMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct Usage {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

/// One streaming chunk (`object: "chat.completion.chunk"`).
#[derive(Debug, Serialize)]
struct ChatCompletionChunk {
    id: String,
    object: &'static str,
    created: i64,
    model: String,
    choices: Vec<ChunkChoice>,
}

#[derive(Debug, Serialize)]
struct ChunkChoice {
    index: u32,
    delta: Delta,
    finish_reason: Option<String>,
}

#[derive(Debug, Serialize, Default)]
struct Delta {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

/// `GET /v1/models` response.
#[derive(Debug, Serialize)]
struct ModelsList {
    object: &'static str,
    data: Vec<ModelInfo>,
}

#[derive(Debug, Serialize)]
struct ModelInfo {
    id: String,
    object: &'static str,
    created: i64,
    owned_by: &'static str,
}

// ============================================
// Errors
// ============================================

/// Errors surfaced as HTTP responses from the chat-completions
/// handler. Mapped to OpenAI-ish status codes: 400 for bad client
/// input, 401 for auth, 404 for unknown model/session, 500 for
/// internal failures, 504 for agent timeouts.
#[derive(Debug, thiserror::Error)]
enum ChatError {
    #[error("missing or invalid API key")]
    Unauthorized,
    #[error("request must include at least one message")]
    EmptyMessages,
    #[error("the final message must be a user message")]
    LastMessageNotUser,
    #[error("model '{0}' not found (expected a forge profile name or 'forge:<session-id>')")]
    ModelNotFound(String),
    #[error("session '{0}' not found")]
    SessionNotFound(Uuid),
    #[error("failed to create session: {0}")]
    SessionCreate(String),
    #[error("failed to start agent: {0}")]
    AgentStart(String),
    #[error("the agent timed out producing a response")]
    AgentTimeout,
    #[error("the agent process ended unexpectedly")]
    AgentDied,
    #[error("agent error: {0}")]
    AgentError(String),
    #[error("database error: {0}")]
    Database(String),
}

impl ChatError {
    fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            ChatError::Unauthorized => StatusCode::UNAUTHORIZED,
            ChatError::EmptyMessages | ChatError::LastMessageNotUser => StatusCode::BAD_REQUEST,
            ChatError::ModelNotFound(_) | ChatError::SessionNotFound(_) => StatusCode::NOT_FOUND,
            ChatError::SessionCreate(_) | ChatError::AgentStart(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            ChatError::AgentTimeout => StatusCode::GATEWAY_TIMEOUT,
            ChatError::AgentDied | ChatError::AgentError(_) | ChatError::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl IntoResponse for ChatError {
    fn into_response(self) -> Response {
        // Match OpenAI's error envelope shape:
        // `{"error": {"message": "...", "type": "...", "code": ...}}`.
        let code = match self.status().as_u16() {
            401 => "invalid_api_key",
            404 => "model_not_found",
            400 => "invalid_request_error",
            504 => "timeout",
            _ => "internal_error",
        };
        let body = serde_json::json!({
            "error": {
                "message": self.to_string(),
                "type": if self.status().is_client_error() { "invalid_request_error" } else { "internal_error" },
                "code": code,
            }
        });
        (self.status(), Json(body)).into_response()
    }
}

type ChatResult<T> = Result<T, ChatError>;

// ============================================
// Content normalization
// ============================================

/// Extract a plain-text string from an OpenAI `content` field, which
/// may be a JSON string, an array of content parts
/// (`[{"type":"text","text":"…"}, {"type":"image_url", …}]`), or
/// `null`. Text parts are concatenated with no separator; non-text
/// parts (images, etc.) are skipped — forge agents are text-only
/// today. Returns an empty string for `null` / missing / unrecognized
/// shapes rather than erroring, so an assistant message that carries
/// only `tool_calls` (and thus `content: null`) round-trips as an
/// empty assistant turn.
fn normalize_content(content: &Option<serde_json::Value>) -> String {
    match content {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(parts)) => {
            let mut out = String::new();
            for part in parts {
                if let Some(obj) = part.as_object() {
                    if obj.get("type").and_then(|v| v.as_str()) == Some("text") {
                        if let Some(t) = obj.get("text").and_then(|v| v.as_str()) {
                            out.push_str(t);
                        }
                    }
                }
            }
            out
        }
        // Any other scalar shape (number, bool): coerce to string so
        // a weirdly-formed request doesn't 500.
        Some(other) => other.to_string(),
    }
}

// ============================================
// Session resolution
// ============================================

/// The resolved target for a chat-completion request: either a fresh
/// session to create (stateless, profile-name mode) or an existing
/// session to reuse (stateful, `forge:<id>` mode).
enum ResolvedTarget {
    /// Create a fresh session for this profile and replay the request
    /// history into it. `profile` is the looked-up profile; the
    /// session row is created by `chat_completions` after this
    /// returns. `prior_messages` is `messages[..len-1]` (everything
    /// except the prompt to send).
    ///
    /// `profile` is boxed to keep the enum small: `Profile` is ~400
    /// bytes (it carries the system prompt, tools JSON, etc.) while
    /// the `Existing` variant is ~40 bytes. Without `Box`, clippy's
    /// `large_enum_variant` flags the 10x size gap and every
    /// `ResolvedTarget` pays for the largest variant.
    Fresh {
        profile: Box<Profile>,
        prior_messages: Vec<ChatMessage>,
        prompt: String,
    },
    /// Reuse an existing session. Only `prompt` (the last user
    /// message's text) is sent; the rest of the request is ignored.
    Existing { session_id: Uuid, prompt: String },
}

/// Validate the request shape and resolve the `model` field to a
/// concrete target. Does NOT create the session yet (that requires a
/// DB write and is done in `chat_completions` so the stateless path
/// can surface session-creation failures cleanly).
async fn resolve_target(
    state: &AppState,
    req: &ChatCompletionRequest,
) -> ChatResult<ResolvedTarget> {
    if req.messages.is_empty() {
        return Err(ChatError::EmptyMessages);
    }
    let last = req.messages.last().unwrap();
    if last.role != "user" {
        return Err(ChatError::LastMessageNotUser);
    }
    let prompt = normalize_content(&last.content);
    let prior = &req.messages[..req.messages.len() - 1];

    if let Some(session_str) = req.model.strip_prefix(SESSION_MODEL_PREFIX) {
        // Stateful: reuse an existing session.
        let session_id = Uuid::parse_str(session_str)
            .map_err(|_| ChatError::ModelNotFound(req.model.clone()))?;
        let exists: Option<(Uuid,)> = sqlx::query_as("SELECT id FROM sessions WHERE id = $1")
            .bind(session_id)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| ChatError::Database(e.to_string()))?;
        if exists.is_none() {
            return Err(ChatError::SessionNotFound(session_id));
        }
        Ok(ResolvedTarget::Existing { session_id, prompt })
    } else {
        // Stateless: look up a profile by name.
        let profile: Profile = sqlx::query_as("SELECT * FROM profiles WHERE name = $1")
            .bind(&req.model)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| ChatError::Database(e.to_string()))?
            .ok_or_else(|| ChatError::ModelNotFound(req.model.clone()))?;
        Ok(ResolvedTarget::Fresh {
            profile: Box::new(profile),
            prior_messages: prior.to_vec(),
            prompt,
        })
    }
}

/// Create a fresh session for `profile` and replay `prior_messages`
/// into the `messages` table as the conversation context that
/// `get_or_create` will load into pi via the durable-resume jsonl.
/// Returns the new session id.
async fn create_session_with_history(
    state: &AppState,
    profile: &Profile,
    prior_messages: &[ChatMessage],
) -> ChatResult<Uuid> {
    let title = format!(
        "OpenAI /v1/chat/completions {}",
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
    );
    let session: crate::db::Session =
        sqlx::query_as("INSERT INTO sessions (profile_id, title) VALUES ($1, $2) RETURNING *")
            .bind(profile.id)
            .bind(&title)
            .fetch_one(&state.db)
            .await
            .map_err(|e| ChatError::SessionCreate(e.to_string()))?;

    // Materialize the session directory the same way the native
    // `POST /sessions` handler does, so the sandbox manager +
    // agent registry can find a working dir for the session when
    // they spawn pi. A failure here rolls back the session row so
    // we don't leave a half-created session behind.
    if let Err(e) = state
        .session_manager
        .create_session_dir(session.id, profile)
        .await
    {
        let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
            .bind(session.id)
            .execute(&state.db)
            .await;
        return Err(ChatError::SessionCreate(e.to_string()));
    }

    // Replay the prior messages as rows. Only text-bearing roles
    // are reconstructed (see the module docs for why tool_calls /
    // tool-role messages are skipped). `system` is mapped to
    // `user` so the content survives the jsonl replay
    // (`session_replay` renders `system` rows as empty user
    // messages; a `user` row with the same content keeps the
    // instructions in the model's context).
    for msg in prior_messages {
        let text = normalize_content(&msg.content);
        match msg.role.as_str() {
            "user" | "system" => {
                insert_history_row(state, session.id, "user", &text, None, None, None).await?;
            }
            // Skip assistant turns that had no text (e.g. a
            // tool_calls-only message): an empty assistant row
            // would just add noise to the replayed context. The
            // guard falls through to the `_` arm on empty text.
            "assistant" if !text.is_empty() => {
                insert_history_row(state, session.id, "assistant", &text, None, None, None).await?;
            }
            // `tool` and anything else: not reconstructed.
            _ => {}
        }
    }

    Ok(session.id)
}

/// Insert one prior-context row with the next per-session sequence.
/// Thin wrapper around `get_next_sequence` + the insert so the
/// reconstruction loop above stays readable.
async fn insert_history_row(
    state: &AppState,
    session_id: Uuid,
    role: &str,
    content: &str,
    tool_name: Option<&str>,
    tool_input: Option<serde_json::Value>,
    tool_call_id: Option<&str>,
) -> ChatResult<()> {
    let seq: i32 = sqlx::query_scalar("SELECT get_next_sequence($1)")
        .bind(session_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| ChatError::Database(e.to_string()))?;
    sqlx::query(
        r#"INSERT INTO messages (session_id, sequence, role, content, tool_name, tool_input, tool_call_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
    )
    .bind(session_id)
    .bind(seq)
    .bind(role)
    .bind(content)
    .bind(tool_name)
    .bind(tool_input)
    .bind(tool_call_id)
    .execute(&state.db)
    .await
    .map_err(|e| ChatError::Database(e.to_string()))?;
    Ok(())
}

/// Insert the user's prompt as the latest message row and return it.
/// `get_or_create`'s durable-resume path excludes this row from the
/// loaded jsonl (it picks `MAX(sequence) - 1` as the cutoff) and the
/// harness sends it to pi via the normal stdin `prompt` flow, so the
/// model sees the replayed history followed by the new prompt exactly
/// once each.
async fn insert_prompt_row(state: &AppState, session_id: Uuid, prompt: &str) -> ChatResult<()> {
    insert_history_row(state, session_id, "user", prompt, None, None, None).await
}

// ============================================
// Agent turn driver
// ============================================

/// The outcome of one agent turn: the full assistant text produced
/// during the turn (the concatenation of every text delta, across
/// all chunk flushes).
#[derive(Debug, Default, Clone)]
struct TurnOutcome {
    text: String,
}

/// Messages sent from the agent-turn driver to the streaming SSE
/// bridge. `Delta` carries one text chunk as it's produced; `End`
/// signals the turn finished (with the full text on success or an
/// error on failure). The non-streaming path ignores the channel
/// entirely and uses the `Result` return value.
#[derive(Debug)]
enum StreamMsg {
    Delta(String),
    End(Result<TurnOutcome, ChatError>),
}

/// Drive one agent turn to completion: acquire the per-session agent
/// lock, drain leftover events from any prior turn, send the user
/// prompt, and read pi's event stream until `agent_end` (or a
/// timeout / error). Assistant text deltas are both accumulated into
/// the returned `TurnOutcome.text` and flushed to the audit log as
/// separate assistant rows (via `insert_and_publish_assistant`), so
/// OpenAI-driven turns are durable and visible to the live SSE event
/// stream exactly like native `/messages`-driven turns.
///
/// When `delta_tx` is `Some`, each text delta is also forwarded as a
/// `StreamMsg::Delta` for the streaming SSE response. The channel is
/// closed (sender dropped) when this function returns; the streaming
/// bridge reads until close, then awaits the turn result.
///
/// This mirrors the harness loop in `create_message` but is
/// synchronous (the caller waits for the turn to finish) rather than
/// fire-and-forget. The two intentionally share the same event
/// shapes and audit-log write path; a future refactor could unify
/// them.
async fn run_agent_turn(
    pool: &sqlx::PgPool,
    bus: &MessageBus,
    metrics: &Metrics,
    session_id: Uuid,
    agent: SharedPiAgent,
    user_content: &str,
    delta_tx: Option<mpsc::Sender<StreamMsg>>,
) -> Result<TurnOutcome, ChatError> {
    let mut guard = agent.lock().await;

    // Flush any straggler events from a previous turn so we don't
    // mistake a stale `agent_end` for this turn's completion.
    guard.drain_pending_events().await;

    if let Err(e) = guard.send_message(user_content).await {
        tracing::error!(
            session_id = %session_id,
            error = %e,
            "openai: failed to send prompt to pi"
        );
        return Err(ChatError::AgentError(e.to_string()));
    }

    let mut full_response = String::new();
    // `chunk_buf` accumulates deltas between flush boundaries
    // (text_end / toolcall_start / end-of-turn); `full_response`
    // accumulates the whole turn without resetting, so it's the
    // text we return to the OpenAI client.
    let mut chunk_buf = String::new();
    let mut produced_any_text = false;
    let mut seen_turn_start = false;
    let mut in_flight_tools: u32 = 0;
    let mut loop_count = 0u32;

    while loop_count < 10000 {
        loop_count += 1;
        let read_timeout = if in_flight_tools > 0 {
            Duration::from_secs(TOOL_READ_TIMEOUT_SECS)
        } else {
            Duration::from_secs(IDLE_READ_TIMEOUT_SECS)
        };

        match tokio::time::timeout(read_timeout, guard.read_line()).await {
            Ok(Ok(Some(line))) => match serde_json::from_str::<PiEvent>(&line) {
                Ok(event) => match event {
                    PiEvent::Session { .. } => {}
                    PiEvent::TurnStart => {
                        seen_turn_start = true;
                    }
                    PiEvent::AgentStart => {}
                    PiEvent::MessageUpdate {
                        assistant_message_event: Some(evt),
                        ..
                    } if seen_turn_start => match evt {
                        AssistantMessageEvent::TextDelta { delta } => {
                            produced_any_text = true;
                            full_response.push_str(&delta);
                            chunk_buf.push_str(&delta);
                            if let Some(tx) = &delta_tx {
                                // Best-effort forward; a slow or
                                // disconnected SSE consumer must not
                                // backpressure the agent (the same
                                // lesson as the bash-streaming
                                // `try_send` fix in `api::sse`).
                                let _ = tx.try_send(StreamMsg::Delta(delta));
                            }
                        }
                        AssistantMessageEvent::TextEnd => {
                            let chunk = std::mem::take(&mut chunk_buf);
                            insert_and_publish_assistant(pool, bus, session_id, &chunk).await;
                        }
                        AssistantMessageEvent::ToolCallStart => {
                            let chunk = std::mem::take(&mut chunk_buf);
                            insert_and_publish_assistant(pool, bus, session_id, &chunk).await;
                        }
                        AssistantMessageEvent::ThinkingDelta { .. } => {}
                        AssistantMessageEvent::ToolCallEnd { .. } => {}
                        _ => {}
                    },
                    PiEvent::ToolExecutionStart { .. } if seen_turn_start => {
                        in_flight_tools = in_flight_tools.saturating_add(1);
                    }
                    PiEvent::ToolExecutionEnd { .. } if seen_turn_start => {
                        in_flight_tools = in_flight_tools.saturating_sub(1);
                    }
                    PiEvent::AgentEnd if seen_turn_start => {
                        break;
                    }
                    PiEvent::Error { message } => {
                        tracing::error!(
                            session_id = %session_id,
                            "openai: pi error: {}", message
                        );
                        // The partial text produced before the
                        // error is already in the audit log (the
                        // per-chunk flushes above wrote it). We
                        // surface the error to the client rather
                        // than a partial completion, matching how
                        // OpenAI returns an error object on
                        // mid-generation failures.
                        return Err(ChatError::AgentError(message));
                    }
                    // pi's RPC response envelope. A `success: false`
                    // response means the prompt itself failed before
                    // any turn ran — most commonly "No API key found
                    // for <provider>" when the profile has no key,
                    // but also covers provider-side auth / model
                    // errors. Without this arm the event loop would
                    // ignore the response, keep reading, hit the
                    // 5-minute idle timeout, and return a 504 —
                    // turning a fast config error into a 5-minute
                    // hang. Surface it as a 500 immediately so the
                    // client sees the real cause. (Successful
                    // responses fall through to the `_` arm; the
                    // turn events that follow drive the loop.)
                    PiEvent::Response {
                        success: false,
                        error,
                        command,
                        ..
                    } => {
                        let msg =
                            error.unwrap_or_else(|| format!("pi RPC command '{}' failed", command));
                        tracing::error!(
                            session_id = %session_id,
                            command = %command,
                            "openai: pi RPC response reported failure: {}",
                            msg
                        );
                        return Err(ChatError::AgentError(msg));
                    }
                    _ => {}
                },
                Err(e) => {
                    tracing::warn!(
                        session_id = %session_id,
                        "openai: failed to parse pi event: {} (line: {:?})",
                        e,
                        line
                    );
                }
            },
            Ok(Ok(None)) => {
                tracing::info!(session_id = %session_id, "openai: pi process ended");
                return Err(ChatError::AgentDied);
            }
            Ok(Err(e)) => {
                tracing::error!(session_id = %session_id, "openai: pi read error: {}", e);
                return Err(ChatError::AgentError(e.to_string()));
            }
            Err(_) => {
                let secs = if in_flight_tools > 0 {
                    TOOL_READ_TIMEOUT_SECS
                } else {
                    IDLE_READ_TIMEOUT_SECS
                };
                tracing::warn!(
                    session_id = %session_id,
                    in_flight_tools,
                    timeout_secs = secs,
                    "openai: pi timed out waiting for response; killing pi"
                );
                let _ = guard.kill().await;
                return Err(ChatError::AgentTimeout);
            }
        }
    }

    metrics.inc_requests("pi.responses");

    // Flush any trailing text after the last boundary, or the
    // historical "[no response from agent]" placeholder if the model
    // produced no text at all (keeps the audit log consistent with
    // the native harness).
    if !chunk_buf.is_empty() {
        let chunk = std::mem::take(&mut chunk_buf);
        insert_and_publish_assistant(pool, bus, session_id, &chunk).await;
    } else if !produced_any_text {
        insert_and_publish_assistant(pool, bus, session_id, "[no response from agent]").await;
    }

    bus.publish_turn_ended(session_id);

    let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
        .bind(session_id)
        .execute(pool)
        .await;

    Ok(TurnOutcome {
        text: std::mem::take(&mut full_response),
    })
}

// ============================================
// Handlers
// ============================================

/// `POST /v1/chat/completions` — OpenAI-compatible chat completions.
pub async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ChatCompletionRequest>,
) -> Response {
    // Real key validation (hash + DB lookup), not just the
    // middleware's presence check. A bad/expired key is a 401
    // with the OpenAI error envelope. The underlying
    // `AuthError` detail (e.g. "API key expired") is folded into
    // the message so the client can tell a bad key from an
    // expired one.
    if let Err(e) = extract_auth_user(&state.db, &headers).await {
        let body = serde_json::json!({
            "error": {
                "message": format!("missing or invalid API key: {}", e),
                "type": "invalid_request_error",
                "code": "invalid_api_key",
            }
        });
        return (axum::http::StatusCode::UNAUTHORIZED, Json(body)).into_response();
    }

    let target = match resolve_target(&state, &req).await {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };

    let (session_id, prompt) = match target {
        ResolvedTarget::Existing { session_id, prompt } => (session_id, prompt),
        ResolvedTarget::Fresh {
            profile,
            prior_messages,
            prompt,
        } => {
            let session_id =
                match create_session_with_history(&state, &profile, &prior_messages).await {
                    Ok(id) => id,
                    Err(e) => return e.into_response(),
                };
            (session_id, prompt)
        }
    };

    // Record the user's prompt as a row so it's in the audit log
    // and so `get_or_create`'s durable-resume cutoff excludes it
    // from the loaded jsonl (it'll be sent via stdin instead).
    if let Err(e) = insert_prompt_row(&state, session_id, &prompt).await {
        return e.into_response();
    }

    // Spawn (or reuse) the pi agent for this session. This is the
    // expensive step: on a fresh session it clones the sandbox and
    // boots pi; on an existing session it reuses the live process.
    let agent = match state
        .agent_registry
        .get_or_create(&state.db, session_id)
        .await
    {
        Ok(a) => a,
        Err(e) => return ChatError::AgentStart(e.to_string()).into_response(),
    };

    let completion_id = format!("chatcmpl-{}", Uuid::new_v4().simple());
    let created = chrono::Utc::now().timestamp();
    // The `model` we report back: the profile name for stateless,
    // the `forge:<id>` form for stateful. Either way it matches
    // what the client sent, so OpenAI clients that echo `model`
    // back to the user see a familiar string.
    let model_label = req.model.clone();

    state.metrics.inc_requests("POST /v1/chat/completions");

    if req.stream {
        streaming_response(
            &state,
            session_id,
            agent,
            prompt,
            completion_id,
            created,
            model_label,
        )
        .await
    } else {
        non_streaming_response(
            &state,
            session_id,
            agent,
            prompt,
            completion_id,
            created,
            model_label,
        )
        .await
    }
}

/// Non-streaming path: drive the turn to completion, then return one
/// `chat.completion` JSON object.
async fn non_streaming_response(
    state: &AppState,
    session_id: Uuid,
    agent: SharedPiAgent,
    prompt: String,
    completion_id: String,
    created: i64,
    model_label: String,
) -> Response {
    let outcome = run_agent_turn(
        &state.db,
        &state.bus,
        &state.metrics,
        session_id,
        agent,
        &prompt,
        None,
    )
    .await;

    match outcome {
        Ok(outcome) => {
            let prompt_tokens = estimate_tokens(&prompt);
            let completion_tokens = estimate_tokens(&outcome.text);
            let body = ChatCompletionResponse {
                id: completion_id,
                object: "chat.completion",
                created,
                model: model_label,
                choices: vec![Choice {
                    index: 0,
                    message: ResponseMessage {
                        role: "assistant",
                        content: outcome.text,
                    },
                    finish_reason: "stop".to_string(),
                }],
                usage: Usage {
                    prompt_tokens,
                    completion_tokens,
                    total_tokens: prompt_tokens + completion_tokens,
                },
            };
            Json(body).into_response()
        }
        Err(e) => {
            // A turn may have produced partial text before failing.
            // We don't have it here (run_agent_turn returns it only
            // on the Ok path's outcome; the Err path discards the
            // partial text to keep the error path simple). Surface
            // the error to the client; the partial text is already
            // in the audit log.
            e.into_response()
        }
    }
}

/// Streaming path: return an `Sse` stream that emits
/// `chat.completion.chunk` events as the agent produces text, then a
/// final `finish_reason: "stop"` chunk and `data: [DONE]`.
async fn streaming_response(
    state: &AppState,
    session_id: Uuid,
    agent: SharedPiAgent,
    prompt: String,
    completion_id: String,
    created: i64,
    model_label: String,
) -> Response {
    // Channel for text deltas + the terminal End signal. The agent
    // task is the sole producer; the SSE stream is the sole
    // consumer. When the agent task finishes it drops its sender,
    // closing the channel; the stream then emits the final chunk.
    let (tx, rx) = mpsc::channel::<StreamMsg>(64);

    let pool = state.db.clone();
    let bus = state.bus.clone();
    let metrics = state.metrics.clone();

    tokio::spawn(async move {
        let result = run_agent_turn(
            &pool,
            &bus,
            &metrics,
            session_id,
            agent,
            &prompt,
            Some(tx.clone()),
        )
        .await;
        // Signal completion. Best-effort: if the consumer already
        // disconnected (client closed the SSE connection), the send
        // fails and we just exit.
        let _ = tx.send(StreamMsg::End(result)).await;
    });

    let stream = build_chunk_stream(rx, completion_id.clone(), created, model_label.clone());

    let mut response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("hb"),
        )
        .into_response();
    // Disable proxy buffering (nginx `proxy_buffering on`) so
    // chunks reach the client as they're produced, and hint no
    // caching — same headers the session-events SSE sets.
    let headers = response.headers_mut();
    headers.insert(
        "X-Accel-Buffering",
        axum::http::HeaderValue::from_static("no"),
    );
    headers.insert(
        "Cache-Control",
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

/// Build the SSE `Stream<Item = Result<Event, Infallible>>` for the
/// streaming response. Emits the opening role chunk, one content
/// chunk per `Delta`, then the final stop chunk + `[DONE]`. If the
/// turn ended with an error, the error message is emitted as a final
/// content chunk (so the client sees *something* went wrong) and the
/// stream still closes with `[DONE]`.
fn build_chunk_stream(
    mut rx: mpsc::Receiver<StreamMsg>,
    completion_id: String,
    created: i64,
    model_label: String,
) -> Pin<Box<dyn Stream<Item = Result<Event, std::convert::Infallible>> + Send>> {
    use async_stream::stream;
    let s = stream! {
        // Opening chunk: role only, no content. Mirrors what the
        // real OpenAI API sends as the first chunk.
        let first = ChatCompletionChunk {
            id: completion_id.clone(),
            object: "chat.completion.chunk",
            created,
            model: model_label.clone(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: Delta { role: Some("assistant"), content: None },
                finish_reason: None,
            }],
        };
        yield Ok(make_data_event(&first));

        while let Some(msg) = rx.recv().await {
            match msg {
                StreamMsg::Delta(text) => {
                    let chunk = ChatCompletionChunk {
                        id: completion_id.clone(),
                        object: "chat.completion.chunk",
                        created,
                        model: model_label.clone(),
                        choices: vec![ChunkChoice {
                            index: 0,
                            delta: Delta { role: None, content: Some(text) },
                            finish_reason: None,
                        }],
                    };
                    yield Ok(make_data_event(&chunk));
                }
                StreamMsg::End(result) => {
                    match result {
                        Ok(_outcome) => {
                            let final_chunk = ChatCompletionChunk {
                                id: completion_id.clone(),
                                object: "chat.completion.chunk",
                                created,
                                model: model_label.clone(),
                                choices: vec![ChunkChoice {
                                    index: 0,
                                    delta: Delta::default(),
                                    finish_reason: Some("stop".to_string()),
                                }],
                            };
                            yield Ok(make_data_event(&final_chunk));
                        }
                        Err(e) => {
                            // Surface the error as a content chunk so
                            // a streaming client isn't left without
                            // any indication of what happened, then
                            // close with a stop + [DONE].
                            let err_chunk = ChatCompletionChunk {
                                id: completion_id.clone(),
                                object: "chat.completion.chunk",
                                created,
                                model: model_label.clone(),
                                choices: vec![ChunkChoice {
                                    index: 0,
                                    delta: Delta {
                                        role: None,
                                        content: Some(format!("[error: {}]", e)),
                                    },
                                    finish_reason: None,
                                }],
                            };
                            yield Ok(make_data_event(&err_chunk));
                            let final_chunk = ChatCompletionChunk {
                                id: completion_id.clone(),
                                object: "chat.completion.chunk",
                                created,
                                model: model_label.clone(),
                                choices: vec![ChunkChoice {
                                    index: 0,
                                    delta: Delta::default(),
                                    finish_reason: Some("stop".to_string()),
                                }],
                            };
                            yield Ok(make_data_event(&final_chunk));
                        }
                    }
                    break;
                }
            }
        }

        // OpenAI streaming terminates with `data: [DONE]`.
        yield Ok(Event::default().data("[DONE]"));
    };
    Box::pin(s)
}

/// `GET /v1/models` — list forge profiles as OpenAI models.
///
/// Each profile becomes a model whose `id` is the profile `name`
/// (the same value the client passes as `model:` on
/// `/v1/chat/completions`). The `forge:<session-id>` stateful form
/// is not listed — clients construct it themselves from a session id
/// they already have.
pub async fn list_models(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if extract_auth_user(&state.db, &headers).await.is_err() {
        return ChatError::Unauthorized.into_response();
    }
    state.metrics.inc_requests("GET /v1/models");
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles ORDER BY name ASC")
        .fetch_all(&state.db)
        .await
    {
        Ok(profiles) => {
            let data = profiles
                .into_iter()
                .map(|p| ModelInfo {
                    id: p.name,
                    object: "model",
                    created: p.created_at.timestamp(),
                    owned_by: "forge",
                })
                .collect();
            Json(ModelsList {
                object: "list",
                data,
            })
            .into_response()
        }
        Err(e) => ChatError::Database(e.to_string()).into_response(),
    }
}

// ============================================
// Helpers
// ============================================

/// Serialize an SSE `data:` event carrying one JSON value. The
/// OpenAI streaming protocol uses untyped `data:` events (no SSE
/// `event:` name), so this is `Event::data(json)`.
fn make_data_event(data: impl serde::Serialize) -> Event {
    let json = serde_json::to_string(&data)
        .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string());
    Event::default().data(json)
}

/// Rough token estimate (4 chars ≈ 1 token). Used only for the
/// `usage` field so token-counting clients don't see zeros; it's
/// not authoritative. forge doesn't track real per-request token
/// counts at the harness boundary.
fn estimate_tokens(text: &str) -> u64 {
    let chars = text.chars().count() as u64;
    chars.saturating_div(4)
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::auth::extract_api_key_header;

    #[test]
    fn normalize_string_content() {
        assert_eq!(
            normalize_content(&Some(serde_json::json!("hello world"))),
            "hello world"
        );
    }

    #[test]
    fn normalize_null_content() {
        assert_eq!(normalize_content(&None), "");
        assert_eq!(normalize_content(&Some(serde_json::Value::Null)), "");
    }

    #[test]
    fn normalize_array_content_concatenates_text_parts() {
        let content = Some(serde_json::json!([
            { "type": "text", "text": "hello " },
            { "type": "text", "text": "world" }
        ]));
        assert_eq!(normalize_content(&content), "hello world");
    }

    #[test]
    fn normalize_array_content_skips_non_text_parts() {
        // An image_url part should be skipped, leaving only the
        // text parts' text.
        let content = Some(serde_json::json!([
            { "type": "text", "text": "describe this" },
            { "type": "image_url", "image_url": { "url": "data:..." } }
        ]));
        assert_eq!(normalize_content(&content), "describe this");
    }

    #[test]
    fn normalize_scalar_content_is_stringified() {
        assert_eq!(normalize_content(&Some(serde_json::json!(42))), "42");
    }

    #[test]
    fn bearer_prefix_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "bearer sk_forge_abc".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("sk_forge_abc".to_string())
        );

        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "BEARER sk_forge_X".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("sk_forge_X".to_string())
        );
    }

    #[test]
    fn bearer_prefix_preserves_key_case() {
        // API keys are case-sensitive; the extractor must not
        // lowercase the token even though it matches the
        // `Bearer ` prefix case-insensitively.
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer Sk_FoRgE_AbC".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("Sk_FoRgE_AbC".to_string())
        );
    }

    #[test]
    fn bearer_with_no_token_falls_through_to_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer ".parse().unwrap());
        headers.insert("X-API-Key", "sk_forge_from_x".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("sk_forge_from_x".to_string())
        );
    }

    #[test]
    fn bare_authorization_without_bearer_is_ignored() {
        // A non-Bearer Authorization scheme (e.g. "Basic …")
        // should not be treated as an API key.
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        headers.insert("X-API-Key", "sk_forge_real".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("sk_forge_real".to_string())
        );
    }

    #[test]
    fn x_api_key_works_without_authorization() {
        let mut headers = HeaderMap::new();
        headers.insert("X-API-Key", "sk_forge_only".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("sk_forge_only".to_string())
        );
    }

    #[test]
    fn authorization_preferred_over_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer sk_forge_bearer".parse().unwrap());
        headers.insert("X-API-Key", "sk_forge_header".parse().unwrap());
        assert_eq!(
            extract_api_key_header(&headers),
            Some("sk_forge_bearer".to_string())
        );
    }

    #[test]
    fn missing_header_returns_none() {
        let headers = HeaderMap::new();
        assert_eq!(extract_api_key_header(&headers), None);
    }

    #[test]
    fn estimate_tokens_is_chars_div_four() {
        assert_eq!(estimate_tokens("abcd"), 1);
        assert_eq!(estimate_tokens("abcdefgh"), 2);
        assert_eq!(estimate_tokens("abc"), 0);
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn chat_error_status_codes() {
        assert_eq!(
            ChatError::Unauthorized.status(),
            axum::http::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ChatError::EmptyMessages.status(),
            axum::http::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ChatError::LastMessageNotUser.status(),
            axum::http::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ChatError::ModelNotFound("x".into()).status(),
            axum::http::StatusCode::NOT_FOUND
        );
        assert_eq!(
            ChatError::SessionNotFound(Uuid::new_v4()).status(),
            axum::http::StatusCode::NOT_FOUND
        );
        assert_eq!(
            ChatError::AgentTimeout.status(),
            axum::http::StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            ChatError::AgentDied.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn chat_error_envelope_is_openai_shaped() {
        let resp = ChatError::ModelNotFound("nope".into()).into_response();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v["error"]["message"],
            "model 'nope' not found (expected a forge profile name or 'forge:<session-id>')"
        );
        assert_eq!(v["error"]["code"], "model_not_found");
    }

    #[test]
    fn session_model_prefix_recognized() {
        assert_eq!(SESSION_MODEL_PREFIX, "forge:");
    }

    #[test]
    fn chunk_serialization_omits_empty_delta_fields() {
        // A content chunk must include `content` but not `role`.
        let chunk = ChatCompletionChunk {
            id: "chatcmpl-x".into(),
            object: "chat.completion.chunk",
            created: 0,
            model: "p".into(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: Delta {
                    role: None,
                    content: Some("hi".into()),
                },
                finish_reason: None,
            }],
        };
        let s = serde_json::to_string(&chunk).unwrap();
        assert!(s.contains("\"content\":\"hi\""));
        assert!(!s.contains("\"role\""));
        assert!(s.contains("\"finish_reason\":null"));

        // The final chunk has an empty delta (no role, no content)
        // and a finish_reason.
        let final_chunk = ChatCompletionChunk {
            id: "chatcmpl-x".into(),
            object: "chat.completion.chunk",
            created: 0,
            model: "p".into(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: Delta::default(),
                finish_reason: Some("stop".into()),
            }],
        };
        let s = serde_json::to_string(&final_chunk).unwrap();
        assert!(!s.contains("\"role\""));
        assert!(!s.contains("\"content\""));
        assert!(s.contains("\"finish_reason\":\"stop\""));
    }

    #[test]
    fn non_streaming_response_shape() {
        let body = ChatCompletionResponse {
            id: "chatcmpl-1".into(),
            object: "chat.completion",
            created: 123,
            model: "my-profile".into(),
            choices: vec![Choice {
                index: 0,
                message: ResponseMessage {
                    role: "assistant",
                    content: "hello".into(),
                },
                finish_reason: "stop".into(),
            }],
            usage: Usage {
                prompt_tokens: 2,
                completion_tokens: 1,
                total_tokens: 3,
            },
        };
        let s = serde_json::to_string(&body).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["object"], "chat.completion");
        assert_eq!(v["choices"][0]["message"]["role"], "assistant");
        assert_eq!(v["choices"][0]["message"]["content"], "hello");
        assert_eq!(v["choices"][0]["finish_reason"], "stop");
        assert_eq!(v["usage"]["total_tokens"], 3);
    }
}
