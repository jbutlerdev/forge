use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::Duration;
use uuid::Uuid;

use crate::db::{CreateProfile, Message, Profile, Session, UpdateProfile};
use crate::session_manager::SessionManager;
use crate::tool_executor::{ToolExecutor, ToolInput};
use crate::sandbox::SandboxManager;
use crate::agent_registry::AgentRegistry;
use crate::observability::Metrics;
use crate::pi_agent::{PiEvent, AssistantMessageEvent};
use crate::recording::{ToolRecorder, ToolCallRecord};
use crate::bus::MessageBus;

pub mod auth;
pub mod middleware;
pub mod sse;
pub mod events;
#[cfg(test)]
mod events_integration;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub session_manager: Arc<SessionManager>,
    pub sandbox_manager: Arc<SandboxManager>,
    pub agent_registry: Arc<AgentRegistry>,
    pub metrics: Arc<Metrics>,
    /// Records tool call intents (from the harness) and tool results
    /// (from the executor) to durable storage. Held as a trait object
    /// so we can swap the backend (e.g. a different DB or a separate
    /// audit-log table) without touching call sites.
    pub recorder: Arc<dyn ToolRecorder>,
    /// In-process pub/sub for new message rows. The harness and the
    /// tool executor publish to it; the SSE handler at
    /// `GET /sessions/:id/events` subscribes. See
    /// [`crate::bus::MessageBus`] for the design.
    pub bus: MessageBus,
}

impl AppState {
    pub fn new(
        db: PgPool,
        session_manager: Arc<SessionManager>,
        sandbox_manager: Arc<SandboxManager>,
        agent_registry: Arc<AgentRegistry>,
        metrics: Arc<Metrics>,
        recorder: Arc<dyn ToolRecorder>,
        bus: MessageBus,
    ) -> Self {
        Self { db, session_manager, sandbox_manager, agent_registry, metrics, recorder, bus }
    }
}

fn err_resp(state: &AppState, status: StatusCode, message: &str) -> Response {
    state.metrics.inc_errors(status.as_u16());
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// Look up a session's working directory directly from the database.
///
/// [`crate::session_manager::SessionManager`] keeps an in-memory map of
/// session id -> working directory. That map is populated when a
/// session is first created and lost whenever the API restarts. The
/// directory itself is durable (`/forge/sessions/<id>`), so for any
/// well-formed session we can recompute the path here and re-seed the
/// in-memory map so subsequent calls hit the cache.
pub async fn lookup_session_working_dir(state: &AppState, session_id: Uuid) -> Option<String> {
    // The session directory is always `/forge/sessions/<id>`; we don't
    // need the profile to recompute it. We do still verify the session
    // exists in the DB so a bogus id returns None.
    let exists: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    if exists.is_none() {
        return None;
    }

    let dir = std::path::PathBuf::from("/forge/sessions").join(session_id.to_string());
    if !dir.exists() {
        return None;
    }

    // Re-seed the in-memory map so future calls hit the fast path.
    if let Ok(profile_id) = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT profile_id FROM sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    {
        state
            .session_manager
            .register_existing_session(session_id, profile_id, dir.clone())
            .await;
    }

    Some(dir.to_string_lossy().to_string())
}

// ============================================
// Profile Routes
// ============================================

async fn create_profile(State(state): State<AppState>, Json(payload): Json<CreateProfile>) -> Response {
    let tools_json = payload.tools.as_ref().map(|t| serde_json::to_string(t).unwrap_or_default())
        .unwrap_or_else(|| r#"["bash", "read", "write", "edit"]"#.to_string());

    match sqlx::query_as::<_, Profile>(
        r#"INSERT INTO profiles (name, description, provider, model, base_url, api_key, working_dir, git_url, git_ref, nix_shell, system_prompt, tools)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING *"#
    ).bind(&payload.name).bind(&payload.description).bind(&payload.provider).bind(&payload.model)
     .bind(&payload.base_url).bind(&payload.api_key).bind(&payload.working_dir).bind(&payload.git_url)
     .bind(&payload.git_ref).bind(&payload.nix_shell).bind(payload.system_prompt.as_deref().unwrap_or("You are a helpful coding assistant."))
     .bind(&tools_json).fetch_one(&state.db).await {
        Ok(p) => { state.metrics.inc_requests("POST /profiles"); (StatusCode::CREATED, Json(serde_json::json!({ "profile": p }))).into_response() }
        Err(e) => { tracing::error!("Failed to create profile: {e}"); err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create profile") }
    }
}

#[derive(Deserialize)]
struct ListProfilesQuery { limit: Option<i64>, offset: Option<i64> }

async fn list_profiles(State(state): State<AppState>, Query(query): Query<ListProfilesQuery>) -> Response {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles ORDER BY created_at DESC LIMIT $1 OFFSET $2")
        .bind(query.limit.unwrap_or(50)).bind(query.offset.unwrap_or(0)).fetch_all(&state.db).await {
        Ok(p) => { state.metrics.inc_requests("GET /profiles"); Json(serde_json::json!({ "profiles": p })).into_response() }
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to list profiles"),
    }
}

async fn get_profile_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1").bind(id).fetch_optional(&state.db).await {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile"),
    }
}

async fn delete_profile_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query("DELETE FROM profiles WHERE id = $1").bind(id).execute(&state.db).await {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete profile"),
    }
}

// ============================================
// Session Routes
// ============================================

#[derive(Debug, Deserialize)]
struct CreateSessionRequest { profile_id: Uuid, title: Option<String> }

async fn create_session(State(state): State<AppState>, Json(payload): Json<CreateSessionRequest>) -> Response {
    let profile: Profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1").bind(payload.profile_id).fetch_optional(&state.db).await {
        Ok(Some(p)) => p,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
    };

    let title = payload.title.unwrap_or_else(|| format!("Session {}", chrono::Utc::now().format("%Y-%m-%d %H:%M")));

    let session: Session = match sqlx::query_as::<_, Session>(
        r#"INSERT INTO sessions (profile_id, title) VALUES ($1, $2) RETURNING *"#
    ).bind(payload.profile_id).bind(&title).fetch_one(&state.db).await {
        Ok(s) => s,
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create session"),
    };

    match state.session_manager.create_session_dir(session.id, &profile).await {
        Ok(working_dir) => {
            tracing::info!("Created session {} with directory: {:?}", session.id, working_dir);
            (StatusCode::CREATED, Json(serde_json::json!({ "session": session, "working_dir": working_dir.to_string_lossy() }))).into_response()
        }
        Err(e) => {
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1").bind(session.id).execute(&state.db).await;
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to create session: {}", e))
        }
    }
}

async fn list_all_sessions(State(state): State<AppState>) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 100").fetch_all(&state.db).await {
        Ok(s) => Json(serde_json::json!({ "sessions": s })).into_response(),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to list sessions"),
    }
}

async fn get_session_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1").bind(id).fetch_optional(&state.db).await {
        Ok(Some(s)) => Json(serde_json::json!({ "session": s })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get session"),
    }
}

async fn delete_session_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let _ = state.agent_registry.remove(id).await;
    let _ = state.session_manager.remove_session(id).await;
    let _ = state.sandbox_manager.destroy_container(id).await;
    match sqlx::query("DELETE FROM sessions WHERE id = $1").bind(id).execute(&state.db).await {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete session"),
    }
}

// ============================================
// Message Routes
// ============================================

#[derive(Debug, Deserialize)]
struct SessionQuery { session_id: Uuid }

async fn list_messages_by_session(State(state): State<AppState>, Query(params): Query<SessionQuery>) -> Response {
    match sqlx::query_as::<_, Message>("SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC").bind(params.session_id).fetch_all(&state.db).await {
        Ok(m) => Json(serde_json::json!({ "messages": m })).into_response(),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to list messages"),
    }
}

#[derive(Debug, Deserialize)]
struct CreateMessageRequest { session_id: Uuid, content: String }

/// Send a message - pi processes it with timeouts
async fn create_message(State(state): State<AppState>, Json(payload): Json<CreateMessageRequest>) -> Response {
    let session_id = payload.session_id;

    let session_exists = sqlx::query("SELECT id FROM sessions WHERE id = $1").bind(session_id).fetch_optional(&state.db).await.map(|r| r.is_some()).unwrap_or(false);
    if !session_exists { return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"); }

    let sequence: i32 = match sqlx::query_scalar("SELECT get_next_sequence($1)").bind(session_id).fetch_one(&state.db).await {
        Ok(s) => s,
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get sequence"),
    };

    let message: Message = match sqlx::query_as::<_, Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, 'user', $3) RETURNING *"#
    ).bind(session_id).bind(sequence).bind(&payload.content).fetch_one(&state.db).await {
        Ok(m) => m,
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create message"),
    };

    // Publish the user row to the bus so any SSE consumer attached
    // to this session sees it immediately.
    state.bus.publish_message(message.clone());

    let agent = match state.agent_registry.get_or_create(&state.db, session_id).await {
        Ok(a) => a,
        Err(e) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to create agent: {}", e)),
    };

    let pool = state.db.clone();
    let user_content = payload.content.clone();
    let metrics = state.metrics.clone();
    let bus = state.bus.clone();

    // Spawn background task with TIMEOUTS
    tokio::spawn(async move {
        let mut agent_guard = agent.lock().await;

        // Durable resume: on the first prompt for a fresh pi,
        // `agent_registry` stashed a synthetic first-user-message
        // preamble that contains the prior conversation as a
        // transcript. We consume it (single-use) and prepend it
        // to the user's prompt. After this the second and later
        // prompts are sent verbatim. The model sees the prior
        // context as part of the user's first turn and continues
        // from there. See
        // `agent_registry::build_resume_preamble` for the format.
        let effective_content = match agent_guard.take_resume_preamble() {
            Some(preamble) => {
                tracing::info!(
                    "Prepending durable-resume preamble ({} bytes) to first user prompt",
                    preamble.len()
                );
                format!("{}{}", preamble, user_content)
            }
            None => user_content,
        };

        // Drain any leftover events from a previous turn. With a long-lived
        // pi process, the `agent_end` / `turn_end` events from a prior
        // response are still in the read buffer; if we don't drain them
        // here we'll treat them as the new turn's completion and return
        // before the second prompt is ever processed.
        agent_guard.drain_pending_events().await;

        // Send the prompt first. pi only emits the `session` event after it
        // receives a message on stdin, so we can't reliably wait for it
        // before sending. The session/turn events will appear at the start of
        // the event stream and are handled below.
        if let Err(e) = agent_guard.send_message(&effective_content).await {
            tracing::error!("Failed to send message to pi: {}", e);
            return;
        }

        let mut final_text = String::new();
        let mut loop_count = 0;
        let start = std::time::Instant::now();
        const MAX_RUNTIME_SECS: u64 = 300; // 5 minute max runtime
        // We need to see an `agent_start` for *this* turn before we trust
        // the events that follow. Anything we read before then is leftover
        // from a prior turn that wasn't fully drained.
        let mut seen_turn_start = false;

        while loop_count < 10000 && start.elapsed().as_secs() < MAX_RUNTIME_SECS {
            loop_count += 1;

            // Read with 60s timeout - if no event, assume pi is stuck
            match tokio::time::timeout(Duration::from_secs(60), agent_guard.read_line()).await {
                Ok(Ok(Some(line))) => {
                    match serde_json::from_str::<PiEvent>(&line) {
                        Ok(event) => match event {
                            PiEvent::Session { .. } => {
                                tracing::info!("Pi session ready");
                            }
                            PiEvent::AgentStart => {
                                seen_turn_start = true;
                            }
                            PiEvent::MessageUpdate { assistant_message_event: Some(evt), .. } if seen_turn_start => {
                                match evt {
                                    AssistantMessageEvent::TextDelta { delta } => {
                                        final_text.push_str(&delta);
                                    }
                                    AssistantMessageEvent::ThinkingDelta { delta } => {
                                        tracing::debug!("[thinking] {}", delta);
                                    }
                                    AssistantMessageEvent::ToolCallEnd { tool_call } => {
                                        // The model decided to invoke a
                                        // tool. We record the *call*
                                        // half of the audit log here -
                                        // the executor will record the
                                        // *result* half when the tool
                                        // actually finishes. Both
                                        // share the same `tool_call_id`
                                        // so the two rows can be
                                        // linked.
                                        match state
                                            .recorder
                                            .record_call(ToolCallRecord {
                                                session_id,
                                                tool_call_id: tool_call.id.clone(),
                                                tool_name: tool_call.name.clone(),
                                                input: tool_call.arguments.clone(),
                                            })
                                            .await
                                        {
                                            Ok(row) => {
                                                // Publish to the bus
                                                // so SSE consumers see
                                                // the new row without
                                                // polling.
                                                bus.publish_message(row);
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    tool_call_id = %tool_call.id,
                                                    tool = %tool_call.name,
                                                    error = %e,
                                                    "Failed to persist tool call to audit log"
                                                );
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            PiEvent::ToolExecutionEnd { tool_call_id, tool_name, result, is_error } if seen_turn_start => {
                                // The tool finished. The executor is
                                // the single owner of the *result*
                                // half of the audit log - it already
                                // wrote (or will write) a `role='tool'`
                                // row via the same recorder. We just
                                // log the event so it's visible in
                                // the journal. This arm exists so we
                                // don't fall through to the catch-all
                                // and lose the timing information.
                                tracing::info!(
                                    tool_call_id = %tool_call_id,
                                    tool = %tool_name,
                                    is_error = %is_error,
                                    "Tool execution finished (recorded by executor)"
                                );
                            }
                            PiEvent::AgentEnd { .. } if seen_turn_start => {
                                // The agent has finished this turn. Stop
                                // reading; anything else still in the
                                // buffer will be drained on the next call.
                                tracing::info!("Agent ended");
                                // Don't publish turn_ended here — the
                                // final assistant text is written to the
                                // DB and published after the event loop
                                // exits (see below), so consumers would
                                // see typing_stop before the final
                                // message. We publish turn_ended right
                                // after the message is published, in
                                // the same post-loop code path.
                                break;
                            }
                            PiEvent::Error { message } => {
                                tracing::error!("pi error: {}", message);
                                break;
                            }
                            _ => {
                                // Leftover from a previous turn
                                // (turn_end, agent_end, response,
                                // extension_ui_request, message_start/end
                                // for the user echo, etc.) or a
                                // non-actionable event for the current
                                // turn. Ignore.
                            }
                        },
                        Err(e) => {
                            tracing::warn!("Failed to parse pi output: {} (line: {:?})", e, line);
                        }
                    }
                }
                Ok(Ok(None)) => { tracing::info!("pi process ended"); break; }
                Ok(Err(e)) => { tracing::error!("pi read error: {}", e); break; }
                Err(_) => {
                    tracing::warn!("pi timed out waiting for response after 60s");
                    let _ = agent_guard.kill().await;
                    break;
                }
            }
        }

        metrics.inc_requests("pi.responses");

        let content = if final_text.is_empty() { "No response from agent (timed out?)".to_string() } else { final_text };
        if let Ok(seq) = sqlx::query_scalar::<_, i32>("SELECT get_next_sequence($1)").bind(session_id).fetch_one(&pool).await {
            if let Ok(row) = sqlx::query_as::<_, Message>(
                r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, 'assistant', $3) RETURNING *"#,
            )
            .bind(session_id)
            .bind(seq)
            .bind(&content)
            .fetch_one(&pool)
            .await
            {
                // Publish the assistant's final text so SSE
                // consumers see it immediately, then announce
                // the turn is over (so consumers can clear the
                // typing indicator). Order matters: message
                // before turn_ended.
                bus.publish_message(row);
                bus.publish_turn_ended(session_id);
            }
        }

        let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1").bind(session_id).execute(&pool).await;
    });

    (StatusCode::ACCEPTED, Json(serde_json::json!({ "message": message }))).into_response()
}

// ============================================
// Tool Execution Routes
// ============================================

async fn execute_tool(State(state): State<AppState>, Json(payload): Json<ToolInput>) -> Response {
    state.metrics.inc_requests("POST /tools/execute");
    state.metrics.inc_tool_execution(&payload.tool);

    let session_id = match Uuid::parse_str(&payload.session_id) {
        Ok(id) => id,
        Err(_) => return err_resp(&state, StatusCode::BAD_REQUEST, "Invalid session ID format"),
    };

    // Prefer the in-memory cache populated when the session was first
    // created, but fall back to the canonical working dir on disk so
    // tool calls keep working after an API restart (which would have
    // wiped the in-memory map).
    let working_dir = match state.session_manager.get_session_dir(session_id).await {
        Ok(dir) => dir.to_string_lossy().to_string(),
        Err(_) => match lookup_session_working_dir(&state, session_id).await {
            Some(dir) => dir,
            None => return err_resp(&state, StatusCode::NOT_FOUND, "Session not initialized"),
        },
    };

    let nix_shell: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT p.nix_shell FROM sessions s JOIN profiles p ON s.profile_id = p.id WHERE s.id = $1"
    ).bind(session_id).fetch_one(&state.db).await.ok().flatten();

    let tool_call_id = payload.tool_call_id_str();

    let executor = ToolExecutor::new(
        session_id,
        working_dir,
        false,
        nix_shell,
        state.recorder.clone(),
        state.db.clone(),
        state.bus.clone(),
    );

    match executor.execute(&tool_call_id, &payload.tool, payload.input.clone()).await {
        Ok(output) => {
            tracing::info!("Tool {} completed: success={}", payload.tool, output.success);
            Json(serde_json::json!({ "success": output.success, "output": output.output, "error": output.error })).into_response()
        }
        Err(e) => {
            tracing::error!("Tool error: {}", e);
            Json(serde_json::json!({ "success": false, "output": serde_json::Value::Null, "error": e.to_string() })).into_response()
        }
    }
}

// ============================================
// Health Check
// ============================================

async fn health() -> &'static str { "OK" }

// ============================================
// Query-based Routes
// ============================================

#[derive(Debug, Deserialize)]
struct ProfileQuery { id: Uuid }
async fn get_profile_by_id(State(state): State<AppState>, Query(params): Query<ProfileQuery>) -> Response {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1").bind(params.id).fetch_optional(&state.db).await {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile"),
    }
}

#[derive(Debug, Deserialize)]
struct DeleteProfileQuery { id: Uuid }
async fn delete_profile_by_id(State(state): State<AppState>, Query(params): Query<DeleteProfileQuery>) -> Response {
    match sqlx::query("DELETE FROM profiles WHERE id = $1").bind(params.id).execute(&state.db).await {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete profile"),
    }
}

#[derive(Debug, Deserialize)]
struct UpdateProfileQuery { id: Uuid }
async fn update_profile_by_id(State(state): State<AppState>, Query(params): Query<UpdateProfileQuery>, Json(payload): Json<UpdateProfile>) -> Response {
    update_profile_internal(&state, params.id, payload).await
}

async fn update_profile_internal(state: &AppState, id: Uuid, payload: UpdateProfile) -> Response {
    let mut query = "UPDATE profiles SET updated_at = NOW()".to_string();
    let mut param_idx = 1;
    let mut params: Vec<String> = Vec::new();

    macro_rules! add_param { ($field:expr, $name:expr) => { if $field.is_some() { params.push(format!("{} = ${}", $name, param_idx)); param_idx += 1; } }; }
    add_param!(payload.name, "name");
    add_param!(payload.description, "description");
    add_param!(payload.provider, "provider");
    add_param!(payload.model, "model");
    add_param!(payload.base_url, "base_url");
    add_param!(payload.api_key, "api_key");
    add_param!(payload.working_dir, "working_dir");
    add_param!(payload.git_url, "git_url");
    add_param!(payload.git_ref, "git_ref");
    add_param!(payload.nix_shell, "nix_shell");
    add_param!(payload.system_prompt, "system_prompt");
    add_param!(payload.tools, "tools");

    if params.is_empty() { return err_resp(state, StatusCode::BAD_REQUEST, "No fields to update"); }

    query.push_str(", ");
    query.push_str(&params.join(", "));
    query.push_str(&format!(" WHERE id = ${} RETURNING *", param_idx));

    let mut db_query = sqlx::query_as::<_, Profile>(&query);
    if let Some(ref v) = payload.name { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.description { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.provider { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.model { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.base_url { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.api_key { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.working_dir { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.git_url { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.git_ref { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.nix_shell { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.system_prompt { db_query = db_query.bind(v); }
    if let Some(ref v) = payload.tools { db_query = db_query.bind(serde_json::to_string(v).unwrap_or_default()); }
    db_query = db_query.bind(id);

    match db_query.fetch_optional(&state.db).await {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to update profile"),
    }
}

#[derive(Debug, Deserialize)]
struct DeleteSessionQuery { id: Uuid }
async fn delete_session_by_id(State(state): State<AppState>, Query(params): Query<DeleteSessionQuery>) -> Response {
    let _ = state.agent_registry.remove(params.id).await;
    let _ = state.session_manager.remove_session(params.id).await;
    let _ = state.sandbox_manager.destroy_container(params.id).await;
    match sqlx::query("DELETE FROM sessions WHERE id = $1").bind(params.id).execute(&state.db).await {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete session"),
    }
}

#[derive(Debug, Deserialize)]
struct GetSessionQuery { id: Uuid }
async fn get_session_by_id(State(state): State<AppState>, Query(params): Query<GetSessionQuery>) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1").bind(params.id).fetch_optional(&state.db).await {
        Ok(Some(s)) => Json(serde_json::json!({ "session": s })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get session"),
    }
}

// ============================================
// Sandbox Routes
// ============================================

async fn list_sandbox_containers(State(state): State<AppState>) -> Response {
    let containers = state.sandbox_manager.list_containers().await;
    let list: Vec<_> = containers.into_iter().map(|c| {
        serde_json::json!({ "name": c.name, "session_id": c.session_id.to_string(), "state": format!("{:?}", c.state), "working_dir": c.working_dir.to_string_lossy(), "pid": c.pid })
    }).collect();
    Json(serde_json::json!({ "containers": list })).into_response()
}

#[derive(Debug, Deserialize)]
struct SessionPath { session_id: Uuid }

async fn create_sandbox_for_session(State(state): State<AppState>, Path(params): Path<SessionPath>) -> Response {
    let session = match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1").bind(params.session_id).fetch_optional(&state.db).await {
        Ok(Some(s)) => s,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
    };
    let profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1").bind(session.profile_id).fetch_optional(&state.db).await {
        Ok(Some(p)) => p,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
    };
    match state.sandbox_manager.create_container(params.session_id, &profile).await {
        Ok(container) => {
            tracing::info!("Created sandbox container {} for session {}", container.name, params.session_id);
            Json(serde_json::json!({ "container": { "name": container.name, "session_id": container.session_id.to_string(), "working_dir": container.working_dir.to_string_lossy(), "state": format!("{:?}", container.state) } })).into_response()
        }
        Err(e) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to create sandbox: {}", e)),
    }
}

async fn destroy_sandbox_for_session(State(state): State<AppState>, Path(params): Path<SessionPath>) -> Response {
    match state.sandbox_manager.destroy_container(params.session_id).await {
        Ok(()) => { tracing::info!("Destroyed sandbox for session {}", params.session_id); StatusCode::NO_CONTENT.into_response() }
        Err(e) => err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to destroy sandbox: {}", e)),
    }
}

// ============================================
// Metrics Handlers
// ============================================

async fn get_metrics(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    let error_rate = if snapshot.requests_total > 0 {
        snapshot.errors_total as f64 / snapshot.requests_total as f64
    } else { 0.0 };
    Json(serde_json::json!({
        "metrics": snapshot,
        "error_rate": format!("{:.2}%", error_rate * 100.0),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })).into_response()
}

async fn get_prometheus_metrics(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    let mut output = String::new();

    output.push_str("# HELP forge_requests_total Total number of HTTP requests\n");
    output.push_str("# TYPE forge_requests_total counter\n");
    output.push_str(&format!("forge_requests_total {}\n", snapshot.requests_total));
    
    output.push_str("# HELP forge_active_sessions Number of active sessions\n");
    output.push_str("# TYPE forge_active_sessions gauge\n");
    output.push_str(&format!("forge_active_sessions {}\n", snapshot.active_sessions));
    
    output.push_str("# HELP forge_active_agents Number of active pi agents\n");
    output.push_str("# TYPE forge_active_agents gauge\n");
    output.push_str(&format!("forge_active_agents {}\n", snapshot.active_agents));

    (StatusCode::OK, [
        (axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
    ], output).into_response()
}

// ============================================
// Authentication Middleware
// ============================================

use axum::middleware::Next;
use axum::http::Request;
use axum::body::Body;

async fn auth_middleware(request: Request<Body>, next: Next) -> Response {
    let path = request.uri().path();

    match path {
        "/health" | "/metrics" | "/metrics/prometheus"
        | "/auth/register" | "/auth/login" | "/auth/logout"
        | "/api-keys"
        | "/profiles" | "/profiles/get" | "/profiles/:id"
        | "/sessions" | "/sessions/get" | "/sessions/:id"
        | "/messages"
        | "/tools/execute" | "/tools/execute/stream" => { return next.run(request).await; }
        _ => {}
    }
    
    match request.headers().get("X-API-Key") {
        Some(_) => next.run(request).await,
        None => (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Missing X-API-Key header"}))).into_response(),
    }
}

// ============================================
// Routes Aggregation
// ============================================

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
        .route("/metrics/prometheus", get(get_prometheus_metrics))
        .route("/auth/register", post(auth::register))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/api-keys", get(auth::list_api_keys))
        .route("/api-keys", post(auth::create_api_key))
        .route("/api-keys/:id", get(auth::get_api_key))
        .route("/api-keys/:id", delete(auth::delete_api_key))
        .route("/users", get(auth::list_users))
        .route("/users/:id", get(auth::get_user))
        .route("/users/:id", patch(auth::update_user))
        .route("/users/:id", delete(auth::delete_user))
        .route("/profiles", post(create_profile))
        .route("/profiles", get(list_profiles))
        .route("/profiles/get", get(get_profile_by_id))
        .route("/profiles/delete", delete(delete_profile_by_id))
        .route("/profiles/update", patch(update_profile_by_id))
        .route("/profiles/:id", get(get_profile_by_uuid))
        .route("/profiles/:id", delete(delete_profile_by_uuid))
        .route("/sessions", post(create_session))
        .route("/sessions", get(list_all_sessions))
        .route("/sessions/get", get(get_session_by_id))
        .route("/sessions/delete", delete(delete_session_by_id))
        .route("/sessions/:id", get(get_session_by_uuid))
        .route("/sessions/:id", delete(delete_session_by_uuid))
        .route("/messages", get(list_messages_by_session))
        .route("/messages", post(create_message))
        .route("/tools/execute", post(execute_tool))
        .route("/tools/execute/stream", post(sse::stream_tool_execution))
        .route("/sessions/:id/events", get(events::stream_session_events))
        .route("/sandbox/containers", get(list_sandbox_containers))
        .route("/sandbox/sessions/:id", post(create_sandbox_for_session))
        .route("/sandbox/sessions/:id", delete(destroy_sandbox_for_session))
        .layer(axum::middleware::from_fn(auth_middleware))
}
