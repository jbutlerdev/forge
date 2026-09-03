//! API surface: `AppState`, shared helpers, the turn-driver glue
//! (`dispatch_message`, `execute_tool`), operator/observability
//! routes, auth middleware, and route assembly (`create_router`,
//! `build_app`). The per-resource handlers live in sibling modules:
//! [`profiles`], [`sessions`], [`messages`], [`admin`].

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::agent_registry::AgentRegistry;

use crate::bus::MessageBus;
use crate::db::{Message, Profile, Session};
use crate::observability::Metrics;
use crate::recording::ToolRecorder;
use crate::sandbox::SandboxManager;
use crate::session_manager::SessionManager;
use crate::tool_executor::{ToolExecutor, ToolInput};

/// Per-`read_line()` timeout when no tool call is in flight. If pi
/// goes this long without emitting any event, the harness assumes
/// something is wrong (LLM provider hung, pi wedged, network blip)
/// and bails. Long enough for slow LLM responses; short enough
/// that we surface real failures quickly.
pub(crate) const IDLE_READ_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Per-`read_line()` timeout while one or more tool calls are in
/// flight. Pi emits `tool_execution_start` when a tool begins and
/// `tool_execution_end` when it finishes; between those two events
/// pi is silent. A tool that legitimately takes longer than
/// `IDLE_READ_TIMEOUT_SECS` (e.g. a long compile, a large
/// `git clone`, a `cargo test --release`) would otherwise hit the
/// idle timeout.
///
/// **This must be at least `BASH_DEFAULT_TIMEOUT_MS` + the
/// outermost grace window (5 s on the sandbox + streaming
/// paths).** If it's less, the harness will kill pi a few
/// seconds before the bash tool's outer `tokio::time::timeout`
/// fires — the tool would have been killed by the harness
/// before it could clean up, and the `tool_output` row in
/// the audit log would record a `Container … terminated by
/// signal KILL` from the harness SIGKILL rather than from the
/// model's `timeout_ms`. Set to 2 h to give the 1 h bash
/// default (see [`crate::tool_executor::BASH_DEFAULT_TIMEOUT_MS`])
/// plenty of headroom and to accommodate a model that asks
/// `timeout_ms` for up to ~2 h.
pub(crate) const TOOL_READ_TIMEOUT_SECS: u64 = 7200; // 2 hours

pub mod admin;
pub mod auth;
pub mod events;
#[cfg(test)]
mod events_integration;
pub mod messages;
pub mod openai;
pub mod profiles;
pub mod router;
pub mod sessions;
pub mod sse;
pub mod turn;
pub mod voice;
pub mod web;

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
    /// Path to pi's `models.json`, used by `GET /v1/models/catalog`
    /// to populate the web UI's model-switcher dropdown. Defaults to
    /// `models_json_path()` (env `PI_MODELS_PATH` / `~/.pi/agent/models.json`);
    /// tests inject a temp file directly to avoid the process-global
    /// env-var race documented in `test_helpers`.
    pub models_path: std::path::PathBuf,
    /// Embedding + reranker config for the semantic message router.
    /// Resolved from env vars at startup (see `EmbeddingConfig::default`).
    /// If the endpoints are unreachable, the router degrades to the
    /// LLM-classification fallback.
    pub embedding_config: crate::embedding::EmbeddingConfig,
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
        Self::with_models_path(
            db,
            session_manager,
            sandbox_manager,
            agent_registry,
            metrics,
            recorder,
            bus,
            crate::api::openai::models_json_path(),
            crate::embedding::EmbeddingConfig::default(),
        )
    }

    /// Same as [`AppState::new`] but with an explicit `models.json`
    /// path. Used by tests to inject a temp file without the env-var
    /// race.
    #[allow(clippy::too_many_arguments)]
    pub fn with_models_path(
        db: PgPool,
        session_manager: Arc<SessionManager>,
        sandbox_manager: Arc<SandboxManager>,
        agent_registry: Arc<AgentRegistry>,
        metrics: Arc<Metrics>,
        recorder: Arc<dyn ToolRecorder>,
        bus: MessageBus,
        models_path: std::path::PathBuf,
        embedding_config: crate::embedding::EmbeddingConfig,
    ) -> Self {
        Self {
            db,
            session_manager,
            sandbox_manager,
            agent_registry,
            metrics,
            recorder,
            bus,
            models_path,
            embedding_config,
        }
    }
}

pub(crate) fn err_resp(state: &AppState, status: StatusCode, message: &str) -> Response {
    state.metrics.inc_errors(status.as_u16());
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// Same as [`err_resp`] but for a **database** error: log the real
/// `sqlx::Error` (with the `ctx` label) before returning the generic
/// client-facing message. The majority of API handlers previously
/// wrote `Err(e) => db_err(&state, INTERNAL_SERVER_ERROR, "Failed
/// to X",
/// to X"e
/// to X")`, binding the error to `_` so it never reached the journal
/// — a prod 500 "Failed to list profiles" gave zero clue why
/// (connection dropped? a bad migration? pool exhausted?). This
/// helper keeps the client-facing message generic (no leaking of DB
/// internals) but makes the real cause visible in the journal so the
/// 500 is debuggable.
pub(crate) fn db_err(state: &AppState, status: StatusCode, ctx: &str, e: sqlx::Error) -> Response {
    tracing::error!(error = %e, ctx = %ctx, "database error surfaced as HTTP response");
    err_resp(state, status, ctx)
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
    exists?;

    let dir = std::path::PathBuf::from("/forge/sessions").join(session_id.to_string());
    if !dir.exists() {
        return None;
    }

    // Re-seed the in-memory map so future calls hit the fast path.
    if let Ok(profile_id) =
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT profile_id FROM sessions WHERE id = $1")
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

/// Insert one assistant message row for the given chunk of
/// model text and publish it on the bus. Returns the inserted
/// row, or `None` if `content` is empty (the function is a
/// no-op in that case, so the harness can flush an empty
/// buffer — e.g. between `text_end` and the next `text_start` —
/// without writing an empty placeholder row).
///
/// Used by the harness event loop to flush text chunks as the
/// model produces them, once on `text_end` / `toolcall_start`
/// (chunk boundary) and once more after `agent_end` (catch any
/// trailing text). Each call produces one assistant row, so a
/// multi-tool turn yields one row per text chunk rather than
/// one big concatenated row at the end of the turn.
pub(crate) async fn insert_and_publish_assistant(
    pool: &PgPool,
    bus: &MessageBus,
    session_id: Uuid,
    content: &str,
) -> Option<Message> {
    if content.is_empty() {
        return None;
    }
    // Single-statement INSERT: `get_next_sequence` is called *inside*
    // the INSERT so the advisory-xact-lock transaction wraps both the
    // sequence allocation and the insert. Splitting them into a
    // `SELECT get_next_sequence` + separate INSERT autocommits each
    // (releasing the lock between), so two concurrent dispatches can
    // harvest the same sequence -> `duplicate key value violates
    // unique constraint "messages_session_id_sequence_key"`.
    let row = match sqlx::query_as::<_, Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'assistant', $2) RETURNING *"#,
    )
    .bind(session_id)
    .bind(content)
    .fetch_one(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                session_id = %session_id,
                error = %e,
                "failed to insert assistant message chunk"
            );
            return None;
        }
    };
    bus.publish_message(row.clone());
    Some(row)
}
// ============================================
// Message Routes
// ============================================

/// Core message-dispatch logic shared by `create_message` and the
/// router. Inserts the user row, publishes it to the bus, gets/creates
/// the pi agent, and spawns the background `drive_turn` task. Returns
/// the inserted `Message` on success.
pub(crate) async fn dispatch_message(
    state: &AppState,
    session_id: Uuid,
    content: &str,
) -> Result<Message, (StatusCode, String)> {
    // Single-statement INSERT so the sequence allocation and the
    // insert share one transaction (see `insert_and_publish_assistant`
    // for the race this avoids).
    let message: Message = match sqlx::query_as::<_, Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'user', $2) RETURNING *"#
    )
    .bind(session_id)
    .bind(content)
    .fetch_one(&state.db)
    .await
    {
        Ok(m) => m,
        Err(e) => {
            // Don't leak the raw driver error to the client; log it
            // server-side instead (same pattern as `db_err`).
            tracing::error!(
                session_id = %session_id,
                error = %e,
                "failed to insert user message"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create message".to_string(),
            ))
        }
    };

    state.bus.publish_message(message.clone());

    // Bump `sessions.last_active` at the moment the user row lands,
    // so the idle-cleanup 30-minute clock measures wall time from
    // the user's message, not from the previous turn's end. (The
    // get_or_create and end-of-turn bumps are kept: they only ever
    // move the timestamp *forward*, never shorten the window.)
    crate::db::touch_session(&state.db, &session_id).await;

    let agent = match state
        .agent_registry
        .get_or_create(&state.db, session_id)
        .await
    {
        Ok(a) => a,
        Err(e) => {
            // Log the real cause (which may include DB internals) but
            // return a generic message to the client.
            tracing::error!(
                session_id = %session_id,
                error = %e,
                "failed to get or create pi agent"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create agent".to_string(),
            ));
        }
    };

    let prior_context_tokens: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(LENGTH(content) + COALESCE(LENGTH(tool_input::text), 0) + COALESCE(LENGTH(tool_output::text), 0)), 0)::bigint / 4 FROM messages WHERE session_id = $1 AND sequence <= (SELECT MAX(sequence) - 1 FROM messages WHERE session_id = $1)"
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);
    let needs_compaction = prior_context_tokens > 296_000;
    if needs_compaction {
        tracing::info!(
            session_id = %session_id,
            prior_context_tokens,
            "long-context resume: prior conversation exceeds 300k tokens; sending pi `compact` RPC before user prompt"
        );
    }

    let pool = state.db.clone();
    let user_content = content.to_string();
    let metrics = state.metrics.clone();
    let bus = state.bus.clone();
    let registry = state.agent_registry.clone();
    let models_path = state.models_path.clone();
    let embedding_config = state.embedding_config.clone();

    tokio::spawn(async move {
        let outcome = crate::api::turn::drive_turn(
            &pool,
            &bus,
            &metrics,
            &registry,
            session_id,
            agent,
            &user_content,
            None,
            needs_compaction,
        )
        .await;
        use crate::api::turn::TurnEndReason;
        match outcome.reason {
            TurnEndReason::AgentEnd => {
                tracing::info!(
                    session_id = %session_id,
                    text_len = outcome.text.len(),
                    "turn completed"
                );
            }
            TurnEndReason::ResponseError(msg) => {
                tracing::error!(session_id = %session_id, "turn failed before it started: {}", msg);
            }
            TurnEndReason::PiError(msg) => {
                tracing::error!(session_id = %session_id, "turn ended with pi error: {}", msg);
            }
            TurnEndReason::PiDied => {
                tracing::error!(session_id = %session_id, "turn ended: pi process exited unexpectedly");
            }
            TurnEndReason::Timeout { in_flight_tools } => {
                tracing::warn!(
                    session_id = %session_id,
                    in_flight_tools,
                    "turn ended by read timeout (durable resume will rebuild on next message)"
                );
            }
        }

        // After the turn ends, refresh the session's summary + embedding
        // in the background so the semantic router has up-to-date context.
        // This is fire-and-forget — it never blocks the response and
        // silently skips if the router profile or embedding endpoint is
        // unavailable.
        let pool2 = pool.clone();
        let mp = models_path.clone();
        let ec = embedding_config.clone();
        tokio::spawn(async move {
            crate::api::router::refresh_session_summary(&pool2, &mp, &ec, session_id).await;
        });
    });

    Ok(message)
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
        "SELECT p.nix_shell FROM sessions s JOIN profiles p ON s.profile_id = p.id WHERE s.id = $1",
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();

    let tool_call_id = payload.tool_call_id_str();

    // Look up the session's container. If it has one, the
    // executor will wrap bash calls in a per-session
    // `systemd-nspawn` namespace. If not (e.g. an old session
    // predating the sandbox, or a session that failed to
    // bootstrap), bash falls back to running on the host in
    // `working_dir` — the legacy behavior.
    let sandbox = state
        .sandbox_manager
        .get_container(session_id)
        .await
        .ok()
        .map(|_| state.sandbox_manager.clone());

    let executor = ToolExecutor::new(
        session_id,
        working_dir,
        sandbox.is_some(),
        nix_shell,
        state.recorder.clone(),
        state.bus.clone(),
        sandbox,
    );

    match executor
        .execute(&tool_call_id, &payload.tool, payload.input.clone())
        .await
    {
        Ok(output) => {
            tracing::info!(
                "Tool {} completed: success={}",
                payload.tool,
                output.success
            );
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

async fn health() -> &'static str {
    "OK"
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
struct SessionPath {
    session_id: Uuid,
}

async fn create_sandbox_for_session(
    State(state): State<AppState>,
    Path(params): Path<SessionPath>,
) -> Response {
    let session = match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(params.session_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database error",
                e,
            )
        }
    };
    let profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(session.profile_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(p)) => p,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database error",
                e,
            )
        }
    };
    match state
        .sandbox_manager
        .create_container(params.session_id, &profile)
        .await
    {
        Ok(container) => {
            tracing::info!(
                "Created sandbox container {} for session {}",
                container.name,
                params.session_id
            );
            Json(serde_json::json!({ "container": { "name": container.name, "session_id": container.session_id.to_string(), "working_dir": container.working_dir.to_string_lossy(), "state": format!("{:?}", container.state) } })).into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to create sandbox: {}", e),
        ),
    }
}

async fn destroy_sandbox_for_session(
    State(state): State<AppState>,
    Path(params): Path<SessionPath>,
) -> Response {
    match state
        .sandbox_manager
        .destroy_container(params.session_id)
        .await
    {
        Ok(()) => {
            tracing::info!("Destroyed sandbox for session {}", params.session_id);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to destroy sandbox: {}", e),
        ),
    }
}

// ============================================
// Metrics Handlers
// ============================================

async fn get_metrics(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    let error_rate = if snapshot.requests_total > 0 {
        snapshot.errors_total as f64 / snapshot.requests_total as f64
    } else {
        0.0
    };
    Json(serde_json::json!({
        "metrics": snapshot,
        "error_rate": format!("{:.2}%", error_rate * 100.0),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
    .into_response()
}

async fn get_prometheus_metrics(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    let mut output = String::new();

    output.push_str("# HELP forge_requests_total Total number of HTTP requests\n");
    output.push_str("# TYPE forge_requests_total counter\n");
    output.push_str(&format!(
        "forge_requests_total {}\n",
        snapshot.requests_total
    ));

    output.push_str("# HELP forge_active_sessions Number of active sessions\n");
    output.push_str("# TYPE forge_active_sessions gauge\n");
    output.push_str(&format!(
        "forge_active_sessions {}\n",
        snapshot.active_sessions
    ));

    output.push_str("# HELP forge_active_agents Number of active pi agents\n");
    output.push_str("# TYPE forge_active_agents gauge\n");
    output.push_str(&format!("forge_active_agents {}\n", snapshot.active_agents));

    // SSE chunks dropped from the live stream because the
    // consumer fell behind. The audit-log row for each
    // affected call also carries a per-call
    // `dropped_sse_chunks` count in its `tool_output`
    // jsonb; this metric is the process-wide total. See
    // `api::sse::execute_bash_streaming` for the
    // rationale and the per-call exposure.
    output.push_str("# HELP forge_sse_chunks_dropped_total SSE chunks dropped because the live consumer fell behind\n");
    output.push_str("# TYPE forge_sse_chunks_dropped_total counter\n");
    output.push_str(&format!(
        "forge_sse_chunks_dropped_total {}\n",
        snapshot.sse_chunks_dropped
    ));

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
        .into_response()
}

// ============================================
// Authentication Middleware
// ============================================

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;

/// Build the 401 response for a failed/absent credential.
///
/// The OpenAI-compatible surface (`/v1/*`) must return OpenAI's
/// error envelope (`{"error": {"code": "invalid_api_key", ...}}`),
/// or unmodified OpenAI clients can't surface the failure. Every
/// other path gets forge's native `{"error": ...}` shape.
fn unauthorized_response(path: &str) -> Response {
    if path.starts_with("/v1/") {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": {
                    "message": "missing or invalid API key",
                    "type": "invalid_request_error",
                    "code": "invalid_api_key",
                }
            })),
        )
            .into_response();
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"error": "Missing or invalid API key"})),
    )
        .into_response()
}

/// Real per-request authentication for every route defined in
/// [`create_router`] (applied in [`build_app`] via
/// `from_fn_with_state` so the middleware can reach the DB and the
/// tool token). Unlike the old presence-only check — which let any
/// `X-API-Key` value (or no header at all on `/tools/execute*`)
/// through — this runs the actual hash + DB lookup
/// (`auth::extract_auth_user`) and only forwards requests carrying
/// a key that exists in `api_keys`, hasn't expired, and belongs to a
/// known user.
///
/// The validated caller is stashed in the request extensions as
/// [`crate::api::auth::AuthenticatedUser`]; the admin endpoints
/// (`/admin/self-update`, `/admin/sandbox-reset`,
/// `/admin/session-replay`) extract it and require `role ==
/// "admin"`.
///
/// Allowlist (no credential required):
///   * `/health`, `/metrics*` — health checks and observability
///     scrape endpoints, typically polled by load balancers /
///     Prometheus.
///   * `/auth/register`, `/auth/login` — you can't authenticate
///     before you have an account or a key.
///   * `/auth/logout` — self-revocation: presenting the key (or
///     nothing, for a stale UI) can only ever delete that key's row.
///
/// `/tools/execute` and `/tools/execute/stream` are **not** in the
/// allowlist anymore. They used to be ("the extension is
/// in-process"), which made them unauthenticated remote code
/// execution for anyone who could reach port 8080. The
/// `forge-tools` extension now authenticates with either a real
/// forge API key (CLI / tests / operators) or the process-scoped
/// tool token the API hands its own pi subprocesses
/// ([`crate::agent_registry::AgentRegistry::tool_auth_token`],
/// exported to pi as `FORGE_API_KEY`).
async fn auth_middleware(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();

    match path {
        "/health"
        | "/metrics"
        | "/metrics/prometheus"
        | "/auth/register"
        | "/auth/login"
        | "/auth/logout" => {
            return next.run(request).await;
        }
        _ => {}
    }

    // Both `X-API-Key` and `Authorization: Bearer <key>` are
    // accepted everywhere (the OpenAI-compatible surface can only
    // send the latter). A request with neither is anonymous → 401.
    let Some(key) = auth::extract_api_key_header(request.headers()) else {
        return unauthorized_response(path);
    };

    // Tool execution accepts either a real api key or the
    // process-scoped tool token forge-api passes to its own pi
    // subprocesses. Everything else requires a real, valid,
    // non-expired DB api key.
    let is_tool_exec = path == "/tools/execute" || path == "/tools/execute/stream";
    if is_tool_exec && key == state.agent_registry.tool_auth_token() {
        // Trusted in-process extension (or an operator who knows
        // the token). No DB row backs the token, so there is no
        // AuthenticatedUser to stash — the tool endpoints don't
        // need one.
    } else {
        match auth::extract_auth_user(&state.db, request.headers()).await {
            Ok(user) => {
                // Handlers that need the caller's identity (the
                // admin endpoints) extract it from the request
                // extensions.
                request.extensions_mut().insert(user);
            }
            Err(_) => return unauthorized_response(path),
        }
    }

    next.run(request).await
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
        .route("/profiles", post(profiles::create_profile))
        .route("/profiles", get(profiles::list_profiles))
        .route("/profiles/get", get(profiles::get_profile_by_id))
        .route("/profiles/delete", delete(profiles::delete_profile_by_id))
        .route("/profiles/update", patch(profiles::update_profile_by_id))
        .route("/profiles/:id", get(profiles::get_profile_by_uuid))
        .route("/profiles/:id", delete(profiles::delete_profile_by_uuid))
        .route("/sessions", post(sessions::create_session))
        .route("/sessions", get(sessions::list_all_sessions))
        .route("/sessions/get", get(sessions::get_session_by_id))
        .route("/sessions/delete", delete(sessions::delete_session_by_id))
        .route(
            "/sessions/:id",
            get(sessions::get_session_by_uuid).patch(sessions::update_session),
        )
        .route("/sessions/:id", delete(sessions::delete_session_by_uuid))
        .route("/messages", get(messages::list_messages_by_session))
        .route("/messages", post(messages::create_message))
        // Message router — universal entrypoint that classifies a
        // message via a routing LLM call and forwards it to the
        // right session (existing or new). See `api/router.rs`.
        .route("/router/message", post(router::route_message))
        .route("/tools/execute", post(execute_tool))
        .route("/tools/execute/stream", post(sse::stream_tool_execution))
        .route("/sessions/:id/events", get(events::stream_session_events))
        .route("/sandbox/containers", get(list_sandbox_containers))
        .route("/sandbox/sessions/:id", post(create_sandbox_for_session))
        .route("/sandbox/sessions/:id", delete(destroy_sandbox_for_session))
        .route(
            "/admin/self-update",
            post(admin::self_update)
                // The release binary is ~10 MiB; axum's default
                // 2 MiB request-body limit would reject it. The
                // `/admin/self-update` endpoint takes the new
                // binary as its raw request body, so disable the
                // limit on this route only. The auth middleware
                // still gates access.
                .layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/admin/sandbox-reset", post(admin::reset_sandbox))
        .route("/admin/session-replay", post(admin::admin_session_replay))
        // OpenAI-compatible surface. `/v1/chat/completions` and
        // `/v1/models` let any OpenAI client (the `openai` SDK,
        // LangChain, Continue, etc.) drive a forge agent without
        // learning forge's native API. Auth is `Authorization:
        // Bearer <forge-api-key>` (the standard OpenAI header);
        // `auth_middleware` accepts either header, and the handlers
        // run the real key validation via `auth::extract_auth_user`.
        // See `api/openai.rs` for the model->profile mapping and
        // the stateless/stateful session semantics.
        .route("/v1/chat/completions", post(openai::chat_completions))
        .route("/v1/models", get(openai::list_models))
        .route("/v1/models/catalog", get(openai::list_model_catalog))
        // OpenAI-compatible STT/TTS proxies. Forward to the voice
        // container (Parakeet :5093 / Kokoro :8766) so a browser
        // that can't reach the LAN still gets voice. See
        // `api/voice.rs`. `GET /v1/audio/voices` reports
        // availability (always 200); the POSTs return 503/502 on
        // disabled/unreachable backends.
        .route("/v1/audio/transcriptions", post(voice::transcribe))
        .route("/v1/audio/speech", post(voice::speech))
        .route("/v1/audio/voices", get(voice::voices))
    // NOTE: the auth middleware is NOT layered here. It needs
    // `AppState` (DB for real key validation + the tool token),
    // which doesn't exist until `build_app` calls `.with_state`.
    // `build_app` applies `from_fn_with_state` around this
    // router before adding the static/SPA fallback, so every
    // API route is authenticated and the web assets stay public.
}

/// Resolve the directory holding the web UI's static assets.
///
/// Order:
/// 1. `FORGE_WEB_DIR` env var (absolute path; operator override).
/// 2. `<repo root>/web` — derived from `CARGO_MANIFEST_DIR`
///    (`crates/forge-api` → `../../web`). This is where the web
///    UI lives in the repo, so a `cargo run` from the source tree
///    serves it with no config.
/// 3. `None` if neither exists — the API still works, it just
///    doesn't serve a UI (e.g. a slim prod deploy, or a test
///    build). `main.rs` skips the static fallback in that case.
///
/// Returns `None` (not an error) so the server can boot without a
/// web dir; the API surface is independent of the UI.
pub fn resolve_web_dir() -> Option<std::path::PathBuf> {
    if let Ok(dir) = std::env::var("FORGE_WEB_DIR") {
        let p = std::path::PathBuf::from(&dir);
        if p.is_dir() {
            return Some(p);
        }
        tracing::warn!("FORGE_WEB_DIR={dir:?} is not a directory; ignoring");
    }
    // CARGO_MANIFEST_DIR = .../forge/crates/forge-api
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = std::path::PathBuf::from(manifest).join("../../web");
        if p.is_dir() {
            return Some(p);
        }
    }
    None
}

/// Assemble the full axum app: the API router (all routes + auth
/// middleware), an optional static-file fallback for the web UI,
/// and the permissive CORS layer. `main.rs` and the test harness
/// both call this so the app assembly isn't duplicated.
///
/// `web_dir`: pass `Some(dir)` to serve the UI from **disk** (dev /
/// `FORGE_WEB_DIR` override — live edits, no rebuild); `None` to
/// serve the **compile-time-embedded** UI (`api::web::embedded_spa`)
/// so a deployed `/opt/forge/forge-api` binary is self-contained
/// with no external files or env config. When a disk dir is set,
/// any path not matched by an API route falls through to
/// `ServeDir`, with `index.html` as the SPA fallback so deep links
/// (`/chat/<id>`) resolve client-side.
pub fn build_app(state: AppState, web_dir: Option<std::path::PathBuf>) -> axum::Router {
    // Real, stateful auth middleware: every route defined in
    // `create_router` runs through hash + DB validation (or the
    // tool-execution token check) — not just the presence check the
    // old stateless middleware did. The router is built without
    // state, so the middleware is applied here where the state
    // exists (`from_fn_with_state`). The static/SPA fallback is
    // added AFTER the middleware so web assets stay publicly
    // served.
    let app = create_router()
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state);
    let app = if let Some(dir) = web_dir {
        let index = dir.join("index.html");
        // `.fallback` (not `.not_found_service`) so deep links get
        // `index.html` with HTTP 200 — `not_found_service` would
        // serve the same body but force a 404, which breaks SPA
        // reloads on a deep link (browsers/clients treat 404 as
        // "gone", and some routers refuse to render).
        let serve = tower_http::services::ServeDir::new(dir)
            .fallback(tower_http::services::ServeFile::new(index));
        app.fallback_service(serve)
    } else {
        // Deployed binary: serve the compile-time-embedded UI so
        // the host needs no `FORGE_WEB_DIR` / external `web/` dir.
        // `embedded_spa` serves any of the 7 known assets and falls
        // back to `index.html` (200) for deep links.
        app.fallback(web::embedded_spa)
    };
    app.layer(tower_http::cors::CorsLayer::permissive())
}
