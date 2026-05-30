use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;
use std::sync::Arc;

use crate::db::{
    CreateProfile, Message, Profile, Session, UpdateProfile,
};
use crate::session_manager::SessionManager;
use crate::tool_executor::{ToolExecutor, ToolInput};
use crate::sandbox::SandboxManager;
use crate::agent_registry::AgentRegistry;
use crate::observability::Metrics;

pub mod auth;
pub mod middleware;
pub mod sse;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub session_manager: Arc<SessionManager>,
    pub sandbox_manager: Arc<SandboxManager>,
    pub agent_registry: Arc<AgentRegistry>,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    /// Create new AppState with metrics
    pub fn new(
        db: PgPool,
        session_manager: Arc<SessionManager>,
        sandbox_manager: Arc<SandboxManager>,
        agent_registry: Arc<AgentRegistry>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            db,
            session_manager,
            sandbox_manager,
            agent_registry,
            metrics,
        }
    }
}

// Helper to create error responses and track metrics
fn error_response(state: &AppState, status: StatusCode, message: &str, _endpoint: &str) -> Response {
    state.metrics.inc_errors(status.as_u16());
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

// Convenience wrapper that extracts endpoint from path
fn err_resp(state: &AppState, status: StatusCode, message: &str) -> Response {
    error_response(state, status, message, "unknown")
}

// ============================================
// Profile Routes
// ============================================

async fn create_profile(
    State(state): State<AppState>,
    Json(payload): Json<CreateProfile>,
) -> Response {
    let tools_json = payload
        .tools
        .as_ref()
        .map(|t| serde_json::to_string(t).unwrap_or_default())
        .unwrap_or_else(|| r#"["bash", "read", "write", "edit"]"#.to_string());

    let profile = sqlx::query_as::<_, Profile>(
        r#"
        INSERT INTO profiles (name, description, provider, model, base_url, api_key,
                              working_dir, git_url, git_ref, nix_shell, system_prompt, tools)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING *
        "#,
    )
    .bind(&payload.name)
    .bind(&payload.description)
    .bind(&payload.provider)
    .bind(&payload.model)
    .bind(&payload.base_url)
    .bind(&payload.api_key)
    .bind(&payload.working_dir)
    .bind(&payload.git_url)
    .bind(&payload.git_ref)
    .bind(&payload.nix_shell)
    .bind(payload.system_prompt.as_deref().unwrap_or("You are a helpful coding assistant."))
    .bind(&tools_json)
    .fetch_one(&state.db)
    .await;

    match profile {
        Ok(p) => {
            state.metrics.inc_requests("POST /profiles");
            (StatusCode::CREATED, Json(serde_json::json!({ "profile": p }))).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to create profile: {e}");
            state.metrics.inc_requests("POST /profiles");
            state.metrics.inc_errors(500);
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create profile")
        }
    }
}

#[derive(Deserialize)]
struct ListProfilesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_profiles(
    State(state): State<AppState>,
    Query(query): Query<ListProfilesQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(50);
    let offset = query.offset.unwrap_or(0);

    let profiles = sqlx::query_as::<_, Profile>(
        "SELECT * FROM profiles ORDER BY created_at DESC LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.db)
    .await;

    match profiles {
        Ok(p) => {
            state.metrics.inc_requests("GET /profiles");
            Json(serde_json::json!({ "profiles": p })).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list profiles: {e}");
            state.metrics.inc_requests("GET /profiles");
            state.metrics.inc_errors(500);
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to list profiles")
        }
    }
}

async fn get_profile_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    eprintln!("DEBUG: get_profile called with id: {:?}", id);
    let profile = sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await;

    match profile {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            tracing::error!("Failed to get profile: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile")
        }
    }
}

async fn update_profile_by_uuid(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateProfile>,
) -> Response {
    update_profile_internal(&state, id, payload).await
}

async fn delete_profile_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let result = sqlx::query("DELETE FROM profiles WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            tracing::error!("Failed to delete profile: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete profile")
        }
    }
}

// ============================================
// Session Routes
// ============================================

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    profile_id: Uuid,
    title: Option<String>,
}

async fn create_session(
    State(state): State<AppState>,
    Json(payload): Json<CreateSessionRequest>,
) -> Response {
    // Verify profile exists and get it
    let profile: Profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(payload.profile_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(p)) => p,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            tracing::error!("Failed to check profile: {e}");
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error");
        }
    };

    // Auto-generate title if not provided
    let title = payload.title.unwrap_or_else(|| {
        let now = chrono::Utc::now();
        format!("Session {}", now.format("%Y-%m-%d %H:%M"))
    });

    let session = sqlx::query_as::<_, Session>(
        r#"
        INSERT INTO sessions (profile_id, title)
        VALUES ($1, $2)
        RETURNING *
        "#,
    )
    .bind(payload.profile_id)
    .bind(&title)
    .fetch_one(&state.db)
    .await;

    let session = match session {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create session: {e}");
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create session");
        }
    };

    // Create isolated working directory for this session
    match state.session_manager.create_session_dir(session.id, &profile).await {
        Ok(working_dir) => {
            tracing::info!(
                "Created session {} with isolated working directory: {:?}",
                session.id,
                working_dir
            );
            (StatusCode::CREATED, Json(serde_json::json!({
                "session": session,
                "working_dir": working_dir.to_string_lossy()
            }))).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to create session directory: {e}");
            // Rollback the database entry
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to create session: {e}"))
        }
    }
}

async fn list_all_sessions(State(state): State<AppState>) -> Response {
    let sessions = sqlx::query_as::<_, Session>(
        "SELECT * FROM sessions ORDER BY created_at DESC LIMIT 100",
    )
    .fetch_all(&state.db)
    .await;

    match sessions {
        Ok(s) => Json(serde_json::json!({ "sessions": s })).into_response(),
        Err(e) => {
            tracing::error!("Failed to list sessions: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to list sessions")
        }
    }
}

async fn get_session_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let session = sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await;

    match session {
        Ok(Some(s)) => Json(serde_json::json!({ "session": s })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => {
            tracing::error!("Failed to get session: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get session")
        }
    }
}

async fn delete_session_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    delete_session_internal(&state, id).await
}

// ============================================
// Message Routes
// ============================================

#[derive(Debug, Deserialize)]
struct SessionQuery {
    session_id: Uuid,
}

async fn list_messages_by_session(State(state): State<AppState>, Query(params): Query<SessionQuery>) -> Response {
    let messages = sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
    )
    .bind(params.session_id)
    .fetch_all(&state.db)
    .await;

    match messages {
        Ok(m) => Json(serde_json::json!({ "messages": m })).into_response(),
        Err(e) => {
            tracing::error!("Failed to list messages: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to list messages")
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateMessageRequest {
    session_id: Uuid,
    content: String,
}

async fn create_message(
    State(state): State<AppState>,
    Json(payload): Json<CreateMessageRequest>,
) -> Response {
    let session_id = payload.session_id;
    // Get next sequence number
    let sequence: Result<i32, _> = sqlx::query_scalar("SELECT get_next_sequence($1)")
        .bind(session_id)
        .fetch_one(&state.db)
        .await;

    let sequence = match sequence {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to get sequence: {e}");
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get sequence");
        }
    };

    let message = sqlx::query_as::<_, Message>(
        r#"
        INSERT INTO messages (session_id, sequence, role, content)
        VALUES ($1, $2, 'user', $3)
        RETURNING *
        "#,
    )
    .bind(session_id)
    .bind(sequence)
    .bind(&payload.content)
    .fetch_one(&state.db)
    .await;

    let user_message = match message {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed to create message: {e}");
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create message");
        }
    };

    // Get or create persistent pi agent for this session
    let agent = match state.agent_registry.get_or_create(&state.db, session_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("Failed to get/create agent for session {}: {}", session_id, e);
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create agent");
        }
    };

    // Spawn background task to process with persistent pi
    let pool = state.db.clone();
    let user_content = payload.content.clone();
    
    tokio::spawn(async move {
        // Get the agent lock and send the message
        let mut agent_guard = agent.lock().await;
        
        // Send user message to pi
        if let Err(e) = agent_guard.send_message(&user_content).await {
            tracing::error!("Failed to send message to pi: {}", e);
            return;
        }

        // Process events from pi (with persistent agent)
        let mut final_text = String::new();
        let mut loop_count = 0;
        const MAX_LOOPS: usize = 1000; // Safety limit

        while loop_count < MAX_LOOPS {
            loop_count += 1;
            
            match agent_guard.read_line().await {
                Ok(Some(line)) => {
                    if let Ok(event) = serde_json::from_str::<crate::pi_agent::PiEvent>(&line) {
                        match event {
                            crate::pi_agent::PiEvent::TextDelta { delta } => {
                                final_text.push_str(&delta);
                                print!("{}", delta);
                            }
                            crate::pi_agent::PiEvent::ThinkingDelta { delta } => {
                                tracing::debug!("[thinking] {}", delta);
                            }
                            crate::pi_agent::PiEvent::ToolCall { id: _, name, input } => {
                                tracing::info!("Tool call (handled by forge-tools): {} with {:?}", name, input);
                            }
                            crate::pi_agent::PiEvent::ToolResult { id, output: _, error } => {
                                tracing::info!("Tool result: {} (error: {:?})", id, error);
                            }
                            crate::pi_agent::PiEvent::MessageEnd { id: _ } => {
                                println!();
                                break;
                            }
                            crate::pi_agent::PiEvent::Error { message } => {
                                eprintln!("pi error: {}", message);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!("pi process ended");
                    break;
                }
                Err(crate::pi_agent::PiError::Timeout) => {
                    tracing::warn!("pi timed out waiting for response");
                    break;
                }
                Err(e) => {
                    tracing::error!("pi read error: {}", e);
                    break;
                }
            }
        }

        // Store the response
        let sequence: Result<i32, _> = sqlx::query_scalar("SELECT get_next_sequence($1)")
            .bind(session_id)
            .fetch_one(&pool)
            .await;

        if let Ok(seq) = sequence {
            let _ = sqlx::query(
                r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, 'assistant', $3)"#
            )
            .bind(session_id)
            .bind(seq)
            .bind(&final_text)
            .execute(&pool)
            .await;
            tracing::info!("Stored agent response for session {}", session_id);
        }
        
        // Update session last_active
        let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
            .bind(session_id)
            .execute(&pool)
            .await;
    });

    (StatusCode::ACCEPTED, Json(serde_json::json!({ "message": user_message }))).into_response()
}

// ============================================
// Metrics Endpoints
// ============================================

/// Get JSON metrics
async fn get_metrics_handler(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    
    // Calculate error rate
    let error_rate = if snapshot.requests_total > 0 {
        snapshot.errors_total as f64 / snapshot.requests_total as f64
    } else {
        0.0
    };
    
    Json(serde_json::json!({
        "metrics": snapshot,
        "error_rate": format!("{:.2}%", error_rate * 100.0),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })).into_response()
}

/// Get Prometheus-format metrics
async fn get_prometheus_metrics_handler(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    
    let mut output = String::new();
    
    // Forge metrics
    output.push_str("# HELP forge_requests_total Total number of HTTP requests\n");
    output.push_str("# TYPE forge_requests_total counter\n");
    output.push_str(&format!("forge_requests_total {}\n", snapshot.requests_total));
    
    output.push_str("# HELP forge_errors_total Total number of HTTP errors\n");
    output.push_str("# TYPE forge_errors_total counter\n");
    output.push_str(&format!("forge_errors_total {}\n", snapshot.errors_total));
    
    output.push_str("# HELP forge_tool_executions_total Total number of tool executions\n");
    output.push_str("# TYPE forge_tool_executions_total counter\n");
    output.push_str(&format!("forge_tool_executions_total {}\n", snapshot.tool_executions_total));
    
    output.push_str("# HELP forge_active_sessions Number of active sessions\n");
    output.push_str("# TYPE forge_active_sessions gauge\n");
    output.push_str(&format!("forge_active_sessions {}\n", snapshot.active_sessions));
    
    output.push_str("# HELP forge_active_agents Number of active pi agents\n");
    output.push_str("# TYPE forge_active_agents gauge\n");
    output.push_str(&format!("forge_active_agents {}\n", snapshot.active_agents));
    
    // Requests by endpoint
    output.push_str("# HELP forge_requests_by_endpoint Requests by endpoint\n");
    output.push_str("# TYPE forge_requests_by_endpoint counter\n");
    for (endpoint, count) in &snapshot.requests_by_endpoint {
        let label = endpoint.replace('"', "\\\"").replace('\n', "\\n");
        output.push_str(&format!("forge_requests_by_endpoint{{endpoint=\"{}\"}} {}\n", label, count));
    }
    
    // Errors by status
    output.push_str("# HELP forge_errors_by_status Errors by HTTP status code\n");
    output.push_str("# TYPE forge_errors_by_status counter\n");
    for (status, count) in &snapshot.errors_by_status {
        output.push_str(&format!("forge_errors_by_status{{status=\"{}\"}} {}\n", status, count));
    }
    
    // Tool executions by type
    output.push_str("# HELP forge_tool_executions_by_type Tool executions by type\n");
    output.push_str("# TYPE forge_tool_executions_by_type counter\n");
    for (tool_type, count) in &snapshot.tool_executions_by_type {
        let label = tool_type.replace('"', "\\\"").replace('\n', "\\n");
        output.push_str(&format!("forge_tool_executions_by_type{{type=\"{}\"}} {}\n", label, count));
    }
    
    (StatusCode::OK, [
        (axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
    ], output).into_response()
}

// ============================================
// Tool Execution Routes
// ============================================

/// Execute a tool in the sandbox
/// 
/// This endpoint is called by the forge-tools pi extension to execute
/// tools (bash, read, write, edit) in the sandbox.
/// 
/// IMPORTANT: Each session has its own isolated working directory.
/// Tools execute in the session's directory, not the profile's base directory.
async fn execute_tool(
    State(state): State<AppState>,
    Json(payload): Json<ToolInput>,
) -> Response {
    state.metrics.inc_requests("POST /tools/execute");
    
    tracing::info!(
        "Tool execution request: {} for session {}",
        payload.tool,
        payload.session_id
    );
    
    // Track tool execution
    state.metrics.inc_tool_execution(&payload.tool);
    
    // Parse session ID
    let session_id = match Uuid::parse_str(&payload.session_id) {
        Ok(id) => id,
        Err(_) => {
            state.metrics.inc_errors(400);
            return err_resp(&state, StatusCode::BAD_REQUEST, "Invalid session ID format");
        }
    };
    
    // Get the session's isolated working directory from session manager
    let working_dir = match state.session_manager.get_session_dir(session_id).await {
        Ok(dir) => dir,
        Err(e) => {
            tracing::error!("Session not registered with manager: {}", e);
            state.metrics.inc_errors(404);
            return err_resp(
                &state,
                StatusCode::NOT_FOUND, 
                "Session not found or not initialized. Create session first."
            );
        }
    };
    
    // Verify session exists in database
    let session_exists = sqlx::query("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .map(|r| r.is_some())
        .unwrap_or(false);
    
    if !session_exists {
        state.metrics.inc_errors(404);
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found in database");
    }
    
    // Check if sandbox container is running for this session
    let in_sandbox = state.sandbox_manager.get_container(session_id).await.is_ok();
    
    if in_sandbox {
        tracing::debug!(
            "Executing tool {} in sandbox container for session {}",
            payload.tool,
            session_id
        );
    } else {
        tracing::debug!(
            "Executing tool {} in host context for session {} (no container)",
            payload.tool,
            session_id
        );
    }
    
    // Get the session's profile to access nix_shell configuration
    let nix_shell: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT p.nix_shell FROM sessions s JOIN profiles p ON s.profile_id = p.id WHERE s.id = $1"
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();

    
    if let Some(ref shell) = nix_shell {
        tracing::info!("Using nix shell configuration for session {}: {}", session_id, shell);
    }
    
    // Create tool executor with the session's isolated working directory and nix shell support
    let executor = ToolExecutor::new_with_nix(
        working_dir.to_string_lossy().to_string(),
        in_sandbox,
        nix_shell
    );
    
    // Execute the tool
    match executor.execute(&payload.tool, payload.input.clone()).await {
        Ok(output) => {
            tracing::info!(
                "Tool {} completed for session {}: success={}",
                payload.tool,
                session_id,
                output.success
            );
            Json(serde_json::json!({
                "success": output.success,
                "output": output.output,
                "error": output.error
            })).into_response()
        }
        Err(e) => {
            tracing::error!("Tool execution error for session {}: {}", session_id, e);
            state.metrics.inc_errors(500);
            Json(serde_json::json!({
                "success": false,
                "output": serde_json::Value::Null,
                "error": e.to_string()
            })).into_response()
        }
    }
}

// ============================================
// Health Check
// ============================================

async fn health() -> &'static str {
    "OK"
}

// ============================================
// Get Profile by ID Query
// ============================================

#[derive(Debug, Deserialize)]
struct ProfileQuery {
    id: Uuid,
}

async fn get_profile_by_id(State(state): State<AppState>, Query(params): Query<ProfileQuery>) -> Response {
    let profile = sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(params.id)
        .fetch_optional(&state.db)
        .await;

    match profile {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            tracing::error!("Failed to get profile: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile")
        }
    }
}

// ============================================
// Delete Profile by ID Query (workaround for path params issue)
// ============================================

#[derive(Debug, Deserialize)]
struct DeleteProfileQuery {
    id: Uuid,
}

async fn delete_profile_by_id(State(state): State<AppState>, Query(params): Query<DeleteProfileQuery>) -> Response {
    let result = sqlx::query("DELETE FROM profiles WHERE id = $1")
        .bind(params.id)
        .execute(&state.db)
        .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            tracing::error!("Failed to delete profile: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete profile")
        }
    }
}

// ============================================
// Update Profile by ID Query (workaround for path params issue)
// ============================================

#[derive(Debug, Deserialize)]
struct UpdateProfileQuery {
    id: Uuid,
}

async fn update_profile_by_id(
    State(state): State<AppState>,
    Query(params): Query<UpdateProfileQuery>,
    Json(payload): Json<UpdateProfile>,
) -> Response {
    update_profile_internal(&state, params.id, payload).await
}

// Shared update logic
async fn update_profile_internal(
    state: &AppState,
    id: Uuid,
    payload: UpdateProfile,
) -> Response {
    // Build dynamic update query
    let mut query = "UPDATE profiles SET updated_at = NOW()".to_string();
    let mut param_idx = 1;
    let mut params: Vec<String> = Vec::new();

    if payload.name.is_some() {
        params.push(format!("name = ${}", param_idx));
        param_idx += 1;
    }
    if payload.description.is_some() {
        params.push(format!("description = ${}", param_idx));
        param_idx += 1;
    }
    if payload.provider.is_some() {
        params.push(format!("provider = ${}", param_idx));
        param_idx += 1;
    }
    if payload.model.is_some() {
        params.push(format!("model = ${}", param_idx));
        param_idx += 1;
    }
    if payload.base_url.is_some() {
        params.push(format!("base_url = ${}", param_idx));
        param_idx += 1;
    }
    if payload.api_key.is_some() {
        params.push(format!("api_key = ${}", param_idx));
        param_idx += 1;
    }
    if payload.working_dir.is_some() {
        params.push(format!("working_dir = ${}", param_idx));
        param_idx += 1;
    }
    if payload.git_url.is_some() {
        params.push(format!("git_url = ${}", param_idx));
        param_idx += 1;
    }
    if payload.git_ref.is_some() {
        params.push(format!("git_ref = ${}", param_idx));
        param_idx += 1;
    }
    if payload.nix_shell.is_some() {
        params.push(format!("nix_shell = ${}", param_idx));
        param_idx += 1;
    }
    if payload.system_prompt.is_some() {
        params.push(format!("system_prompt = ${}", param_idx));
        param_idx += 1;
    }
    if payload.tools.is_some() {
        params.push(format!("tools = ${}", param_idx));
        param_idx += 1;
    }

    if params.is_empty() {
        return err_resp(state, StatusCode::BAD_REQUEST, "No fields to update");
    }

    query.push_str(", ");
    query.push_str(&params.join(", "));
    query.push_str(" WHERE id = $");
    query.push_str(&param_idx.to_string());
    query.push_str(" RETURNING *");

    let mut db_query = sqlx::query_as::<_, Profile>(&query);

    if let Some(ref v) = payload.name {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.description {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.provider {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.model {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.base_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.api_key {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.working_dir {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.git_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.git_ref {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.nix_shell {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.system_prompt {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.tools {
        db_query = db_query.bind(serde_json::to_string(v).unwrap_or_default());
    }

    db_query = db_query.bind(id);

    let profile = db_query.fetch_optional(&state.db).await;

    match profile {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            tracing::error!("Failed to update profile: {e}");
            err_resp(state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to update profile")
        }
    }
}

// ============================================
// Delete Session by ID Query (workaround for path params issue)
// ============================================

#[derive(Debug, Deserialize)]
struct DeleteSessionQuery {
    id: Uuid,
}

async fn delete_session_by_id(
    State(state): State<AppState>,
    Query(params): Query<DeleteSessionQuery>,
) -> Response {
    delete_session_internal(&state, params.id).await
}

// Shared session deletion logic
async fn delete_session_internal(state: &AppState, id: Uuid) -> Response {
    // Stop the pi agent for this session
    if let Err(e) = state.agent_registry.remove(id).await {
        tracing::warn!("Failed to stop pi agent for session {}: {}", id, e);
    }

    // Clean up session directory
    if let Err(e) = state.session_manager.remove_session(id).await {
        tracing::warn!("Failed to remove session directory: {}", e);
    }

    // Destroy sandbox container if exists
    if let Err(e) = state.sandbox_manager.destroy_container(id).await {
        tracing::warn!("Failed to destroy sandbox for session {}: {}", id, e);
    }

    let result = sqlx::query("DELETE FROM sessions WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => {
            tracing::error!("Failed to delete session: {e}");
            err_resp(state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete session")
        }
    }
}

// ============================================
// Get Session by ID Query (workaround for path params issue)
// ============================================

#[derive(Debug, Deserialize)]
struct GetSessionQuery {
    id: Uuid,
}

async fn get_session_by_id(State(state): State<AppState>, Query(params): Query<GetSessionQuery>) -> Response {
    let session = sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(params.id)
        .fetch_optional(&state.db)
        .await;

    match session {
        Ok(Some(s)) => Json(serde_json::json!({ "session": s })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => {
            tracing::error!("Failed to get session: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to get session")
        }
    }
}

// ============================================
// Session Status Route
/// Session status response type
#[derive(Debug, serde::Serialize)]
struct SessionStatusResponse {
    session_id: Uuid,
    active: bool,
    has_agent: bool,
}

// Handler for /simple-status
async fn simple_status() -> &'static str {
    "OK"
}

// Handler for /test/{id}
async fn test_handler(Path(id): Path<String>) -> String {
    format!("ID: {}", id)
}

// Handler for /sessions/{id}/status
async fn get_session_status_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
    // Return a mock response
    let status = SessionStatusResponse {
        session_id: id,
        active: true,
        has_agent: false,
    };
    Json(serde_json::json!({ "status": status })).into_response()
}

// Handler for /sessions/{id}/git
async fn get_git_status_handler(
    State(_state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
    eprintln!("DEBUG: get_git_status_handler called with id: {}", id);
    Json(serde_json::json!({
        "git_status": null,
        "error": "Git not available"
    })).into_response()
}

// Handler for /sessions/{id}
async fn get_session_by_uuid_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
    let session = sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| {
            tracing::error!("Database error: {}", e);
        })
        .ok();
    
    match session {
        Some(s) => Json(serde_json::json!({
            "session": s
        })).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "Session not found"
        }))).into_response(),
    }
}

/// Pull latest git changes for a session
async fn pull_git_changes(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
    let _span = tracing::info_span!("pull_git_changes", session_id = %id);
    
    // Verify session exists
    let session_exists = sqlx::query("SELECT id FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
        .map(|r| r.is_some())
        .unwrap_or(false);
    
    if !session_exists {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    
    // Pull changes
    match state.session_manager.pull_git_changes(id).await {
        Ok(()) => {
            tracing::info!("Git changes pulled successfully for session {}", id);
            Json(serde_json::json!({
                "success": true,
                "message": "Git changes pulled successfully"
            })).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to pull git changes for session {}: {}", id, e);
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Git pull failed: {}", e))
        }
    }
}

// ============================================
// Session Resume Route
// ============================================

/// Resume a session by replaying its message history to a new pi agent
async fn resume_session(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
    let _span = tracing::info_span!("resume_session", session_id = %id);
    
    tracing::info!("Resuming session {}", id);
    
    // Get session
    let _session = match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => {
            tracing::error!(error = %e, "Failed to fetch session");
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error");
        }
    };

    // Check if session already has an active agent
    if state.agent_registry.contains(id).await {
        tracing::info!("Session {} already has an active agent", id);
        return Json(serde_json::json!({
            "resumed": true,
            "message": "Session already has an active agent"
        })).into_response();
    }
    
    // Pull latest changes from git (if applicable)
    match state.session_manager.pull_git_changes(id).await {
        Ok(()) => {
            tracing::info!("Git changes pulled successfully for session {}", id);
        }
        Err(e) => {
            tracing::warn!("Failed to pull git changes for session {}: {}", id, e);
            // Continue anyway - git pull failures are not critical
        }
    }
    
    // Get messages to replay
    let messages = match sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC"
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "Failed to fetch messages");
            return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to fetch messages");
        }
    };

    tracing::info!("Found {} messages to replay for session {}", messages.len(), id);

    // Create new agent for this session
    let agent = match state.agent_registry.get_or_create(&state.db, id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "Failed to create agent for session {}", id);
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to create agent: {}", e)
            );
        }
    };

    // Replay messages to the new agent
    let mut agent_guard = agent.lock().await;
    for msg in &messages {
        if msg.role.as_str() == "user" {
            if let Some(content) = &msg.content {
                tracing::debug!("Replaying user message: {}", content.chars().take(50).collect::<String>());
                if let Err(e) = agent_guard.send_message(content).await {
                    tracing::warn!("Failed to replay message: {}", e);
                }
                // Small delay between messages
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        // Skip assistant/tool messages
    }

    // Update session last_active
    let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await;

    tracing::info!("Session {} resumed successfully with {} messages", id, messages.len());

    Json(serde_json::json!({
        "resumed": true,
        "message": format!("Session resumed with {} messages", messages.len()),
        "message_count": messages.len()
    })).into_response()
}

// ============================================
// Authentication Middleware
// ============================================

use axum::middleware::Next;
use axum::http::Request;
use axum::body::Body;

/// Middleware to require API key authentication for protected routes
/// 
/// Note: This middleware allows public routes through without authentication.
async fn auth_middleware(request: Request<Body>, next: Next) -> Response {
    let path = request.uri().path();
    
    eprintln!("DEBUG: auth_middleware called for path: {}", path);
    
    // Define public paths (exact matches only - no subpaths)
    match path {
        "/health" => {
            tracing::info!("Public path /health - allowing through");
            return next.run(request).await;
        }
        "/metrics" => {
            tracing::info!("Public path /metrics - allowing through");
            return next.run(request).await;
        }
        "/metrics/prometheus" => {
            tracing::info!("Public path /metrics/prometheus - allowing through");
            return next.run(request).await;
        }
        "/auth/register" => {
            tracing::info!("Public path /auth/register - allowing through");
            return next.run(request).await;
        }
        "/auth/login" => {
            tracing::info!("Public path /auth/login - allowing through");
            return next.run(request).await;
        }
        _ => {
            tracing::info!("Protected path - checking API key");
        }
    }
    
    // Extract API key from headers for protected routes
    match request.headers().get("X-API-Key") {
        Some(v) => match v.to_str() {
            Ok(_s) => {
                tracing::info!("API key present - allowing through");
            }
            Err(_) => {
                tracing::info!("Invalid API key format - returning 401");
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({"error": "Invalid API key format"})),
                ).into_response();
            }
        },
        None => {
            tracing::info!("No API key - returning 401");
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "Missing X-API-Key header"})),
            ).into_response();
        }
    };
    
    tracing::debug!("Auth middleware passed");
    
    // Continue to next middleware/handler
    next.run(request).await
}

// ============================================
// Routes Aggregation
// ============================================

/// Create the main API router with authentication middleware applied to protected routes
pub fn create_router() -> Router<AppState> {
    // Build router first, then apply layer at the end
    let mut router = Router::new()
        // Test routes
        .route("/simple-status", get(simple_status))
        // Public routes (no auth required)
        .route("/health", get(health))
        // Auth routes (internal auth handling)
        .route("/auth/register", post(auth::register))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/api-keys", get(auth::list_api_keys))
        .route("/api-keys", post(auth::create_api_key))
        .route("/api-keys/{id}", get(auth::get_api_key))
        .route("/api-keys/{id}", delete(auth::delete_api_key))
        .route("/users", get(auth::list_users))
        .route("/users/{id}", get(auth::get_user))
        .route("/users/{id}", patch(auth::update_user))
        .route("/users/{id}", delete(auth::delete_user))
        // Protected routes
        .route("/profiles", post(create_profile))
        .route("/profiles", get(list_profiles))
        .route("/profiles/get", get(get_profile_by_id))
        .route("/profiles/delete", delete(delete_profile_by_id))
        .route("/profiles/update", patch(update_profile_by_id))
        .route("/profiles/{id}", get(get_profile_by_uuid))
        .route("/profiles/{id}", patch(update_profile_by_uuid))
        .route("/profiles/{id}", delete(delete_profile_by_uuid))
        // Session base routes
        .route("/sessions", post(create_session))
        .route("/sessions", get(list_all_sessions))
        .route("/sessions/get", get(get_session_by_id))
        .route("/sessions/delete", delete(delete_session_by_id))
        // Session routes with path params
        .route("/sessions/{session_id}/status", get(get_session_status_handler))
        .route("/sessions/{session_id}/resume", post(resume_session))
        .route("/sessions/{session_id}/git", get(get_git_status_handler))
        .route("/sessions/{session_id}/git/pull", post(pull_git_changes))
        .route("/sessions/{session_id}", get(get_session_by_uuid_handler))
        .route("/sessions/{session_id}", delete(delete_session_by_uuid))
        // Messages
        .route("/messages", get(list_messages_by_session))
        .route("/messages", post(create_message))
        // Tools
        .route("/tools/execute", post(execute_tool))
        .route("/tools/execute/stream", post(sse::stream_tool_execution))
        // Sandbox
        .route("/sandbox/containers", get(list_sandbox_containers))
        .route("/sandbox/sessions/{session_id}", post(create_sandbox_for_session))
        .route("/sandbox/sessions/{session_id}", delete(destroy_sandbox_for_session));
    
    // Apply auth middleware to all routes and return
    router = router.layer(axum::middleware::from_fn(auth_middleware));
    router
}

async fn list_sandbox_containers(State(state): State<AppState>) -> Response {
    let containers = state.sandbox_manager.list_containers().await;
    let container_list: Vec<_> = containers.into_iter().map(|c| {
        serde_json::json!({
            "name": c.name,
            "session_id": c.session_id.to_string(),
            "state": format!("{:?}", c.state),
            "working_dir": c.working_dir.to_string_lossy(),
            "pid": c.pid,
        })
    }).collect();
    
    Json(serde_json::json!({
        "containers": container_list
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct SessionPath {
    session_id: Uuid,
}

async fn create_sandbox_for_session(
    State(state): State<AppState>,
    Path(params): Path<SessionPath>,
) -> Response {
    let session_id = params.session_id;
    
    // Get session and profile
    let session = match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await {
        Ok(Some(s)) => s,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Database error: {}", e)),
    };
    
    let profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(session.profile_id)
        .fetch_optional(&state.db)
        .await {
        Ok(Some(p)) => p,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Database error: {}", e)),
    };
    
    match state.sandbox_manager.create_container(session_id, &profile).await {
        Ok(container) => {
            tracing::info!("Created sandbox container {} for session {}", container.name, session_id);
            Json(serde_json::json!({
                "container": {
                    "name": container.name,
                    "session_id": container.session_id.to_string(),
                    "working_dir": container.working_dir.to_string_lossy(),
                    "state": format!("{:?}", container.state),
                }
            })).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to create sandbox: {}", e);
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to create sandbox: {}", e))
        }
    }
}

async fn destroy_sandbox_for_session(
    State(state): State<AppState>,
    Path(params): Path<SessionPath>,
) -> Response {
    let session_id = params.session_id;
    
    match state.sandbox_manager.destroy_container(session_id).await {
        Ok(()) => {
            tracing::info!("Destroyed sandbox for session {}", session_id);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            tracing::error!("Failed to destroy sandbox: {}", e);
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, &format!("Failed to destroy sandbox: {}", e))
        }
    }
}
