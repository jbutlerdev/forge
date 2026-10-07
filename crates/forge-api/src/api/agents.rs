//! Agent entity + agent-scoped endpoints (Herd H1.1 / H1.2).
//!
//! CRUD mirrors [`super::profiles`]: same owner-or-admin tenancy gate
//! (`auth::can_access`, 404 not 403 so existence of another user's row
//! is not leaked), same restricted-key gate (migration 015 — a demo key
//! may *use* agents but not manage them), same 409-on-unique-constraint
//! create response and dynamic partial `PATCH`.
//!
//! Agent-scoped routes:
//! - `GET  /agents/:id/conversations?limit=&latest=1` — the agent's
//!   sessions, most-active first
//! - `POST /agents/:id/conversations` — new conversation bound to the
//!   agent (optionally forked from one of its own)
//! - `POST /agents/:id/conversations/:cid/messages` — thin alias of
//!   `POST /messages` with the tenancy check against the agent
//! - `GET  /agents/:id/tasks` — stub (`[]` until H2.3)
//! - `GET  /agents/:id/active` — busy flag + most-active conversation

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::{db_err, dispatch_message, err_resp, AppState};
use crate::api::auth::{can_access, AuthenticatedUser};
use crate::db::{Agent, CreateAgent, Profile, Session, UpdateAgent};

const ALLOWED_VISIBILITIES: &[&str] = &["private", "org"];
const ALLOWED_MEMORY_SCOPES: &[&str] = &["agent", "org"];

// ============================================
// Shared tenancy helper
// ============================================

/// Fetch an agent the caller may access. A missing agent or an agent
/// the caller cannot see is a 404 (not 403 — don't leak existence).
/// Returns `None` when the caller may proceed.
async fn agent_access_err(
    state: &AppState,
    user: &AuthenticatedUser,
    id: Uuid,
) -> Option<Response> {
    match sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM agents WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(owner)) if can_access(user, Some(owner)) => None,
        Ok(_) => Some(err_resp(state, StatusCode::NOT_FOUND, "Agent not found")),
        Err(e) => Some(db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get agent",
            e,
        )),
    }
}

/// Validate an optional `primary_profile_id`: the profile must exist
/// AND the caller must own it (or be admin) — an agent may not point
/// at another user's credentials. 404 on either failure.
async fn check_profile_accessible(
    state: &AppState,
    user: &AuthenticatedUser,
    id: Uuid,
) -> Option<Response> {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(p)) if can_access(user, p.user_id) => None,
        Ok(_) => Some(err_resp(state, StatusCode::NOT_FOUND, "Profile not found")),
        Err(e) => Some(db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get profile",
            e,
        )),
    }
}

// ============================================
// CRUD
// ============================================

pub(crate) async fn create_agent(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<CreateAgent>,
) -> Response {
    // Restricted (demo) keys: agent management is operator territory —
    // an agent pins a primary profile (and later, credentials +
    // memory scope). Demo keys may still *talk* to agents.
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: agent management is not available",
        );
    }
    let visibility = payload.visibility.as_deref().unwrap_or("private");
    if !ALLOWED_VISIBILITIES.contains(&visibility) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            &format!(
                "invalid visibility '{}'; expected one of: {}",
                visibility,
                ALLOWED_VISIBILITIES.join(", ")
            ),
        );
    }
    let memory_scope = payload.memory_scope.as_deref().unwrap_or("agent");
    if !ALLOWED_MEMORY_SCOPES.contains(&memory_scope) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            &format!(
                "invalid memory_scope '{}'; expected one of: {}",
                memory_scope,
                ALLOWED_MEMORY_SCOPES.join(", ")
            ),
        );
    }
    // The primary profile pins provider credentials: it must exist
    // and belong to the caller before the agent row lands.
    if let Some(pid) = payload.primary_profile_id {
        if let Some(resp) = check_profile_accessible(&state, &user, pid).await {
            return resp;
        }
    }
    let allowlist_json: serde_json::Value = payload
        .tools_allowlist
        .as_ref()
        .map(|t| serde_json::to_value(t).unwrap_or_else(|_| serde_json::json!([])))
        .unwrap_or_else(|| serde_json::json!([]));
    let creds_json: serde_json::Value = payload
        .credentials_scope
        .unwrap_or_else(|| serde_json::json!({}));

    match sqlx::query_as::<_, Agent>(
        r#"INSERT INTO agents (owner_id, name, avatar_url, home_machine, primary_profile_id, visibility, memory_scope, tools_allowlist, credentials_scope, extra_instructions)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING *"#,
    )
    .bind(user.user_id)
    .bind(&payload.name)
    .bind(&payload.avatar_url)
    .bind(&payload.home_machine)
    .bind(payload.primary_profile_id)
    .bind(visibility)
    .bind(memory_scope)
    .bind(&allowlist_json)
    .bind(&creds_json)
    .bind(&payload.extra_instructions)
    .fetch_one(&state.db)
    .await
    {
        Ok(a) => {
            state.metrics.inc_requests("POST /agents");
            (StatusCode::CREATED, Json(serde_json::json!({ "agent": a }))).into_response()
        }
        // UNIQUE (owner_id, name): 409 like `POST /profiles` does for
        // a duplicate profile name.
        Err(sqlx::Error::Database(db_err))
            if db_err.constraint() == Some("agents_owner_id_name_key") =>
        {
            tracing::info!(
                name = %payload.name,
                "POST /agents: agent name already exists; returning 409"
            );
            state.metrics.inc_requests("POST /agents");
            err_resp(
                &state,
                StatusCode::CONFLICT,
                &format!("agent name '{}' already exists", payload.name),
            )
        }
        Err(e) => {
            tracing::error!("Failed to create agent: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create agent")
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct ListAgentsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

pub(crate) async fn list_agents(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(query): Query<ListAgentsQuery>,
) -> Response {
    // Tenancy: admins see every agent; a regular user sees only their
    // own. (Unlike profiles/sessions, `agents.owner_id` is NOT NULL —
    // there are no legacy pre-tenancy rows.)
    let rows = if can_access(&user, None) {
        sqlx::query_as::<_, Agent>(
            "SELECT * FROM agents ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        )
        .bind(query.limit.unwrap_or(50))
        .bind(query.offset.unwrap_or(0))
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, Agent>(
            "SELECT * FROM agents WHERE owner_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
        )
        .bind(user.user_id)
        .bind(query.limit.unwrap_or(50))
        .bind(query.offset.unwrap_or(0))
        .fetch_all(&state.db)
        .await
    };
    match rows {
        Ok(a) => {
            state.metrics.inc_requests("GET /agents");
            Json(serde_json::json!({ "agents": a })).into_response()
        }
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list agents",
            e,
        ),
    }
}

pub(crate) async fn get_agent_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, id).await {
        return resp;
    }
    match sqlx::query_as::<_, Agent>("SELECT * FROM agents WHERE id = $1")
        .bind(id)
        .fetch_one(&state.db)
        .await
    {
        Ok(a) => Json(serde_json::json!({ "agent": a })).into_response(),
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get agent",
            e,
        ),
    }
}

pub(crate) async fn update_agent_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateAgent>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: agent management is not available",
        );
    }
    update_agent_internal(&state, &user, id, payload).await
}

async fn update_agent_internal(
    state: &AppState,
    user: &AuthenticatedUser,
    id: Uuid,
    payload: UpdateAgent,
) -> Response {
    if let Some(ref v) = payload.visibility {
        if !ALLOWED_VISIBILITIES.contains(&v.as_str()) {
            return err_resp(
                state,
                StatusCode::BAD_REQUEST,
                &format!(
                    "invalid visibility '{}'; expected one of: {}",
                    v,
                    ALLOWED_VISIBILITIES.join(", ")
                ),
            );
        }
    }
    if let Some(ref v) = payload.memory_scope {
        if !ALLOWED_MEMORY_SCOPES.contains(&v.as_str()) {
            return err_resp(
                state,
                StatusCode::BAD_REQUEST,
                &format!(
                    "invalid memory_scope '{}'; expected one of: {}",
                    v,
                    ALLOWED_MEMORY_SCOPES.join(", ")
                ),
            );
        }
    }
    if let Some(pid) = payload.primary_profile_id {
        if let Some(resp) = check_profile_accessible(state, user, pid).await {
            return resp;
        }
    }
    // Tenancy gate before any work.
    if let Some(resp) = agent_access_err(state, user, id).await {
        return resp;
    }

    // Partial update, same dynamic-SET-clause pattern as
    // `update_profile_internal`. Note (consistent with profiles): a
    // JSON `null` and an absent key are indistinguishable and both
    // mean "leave the column alone" — clearing a nullable column
    // (avatar_url, home_machine, extra_instructions,
    // primary_profile_id) is not supported in H1.
    let mut sets: Vec<String> = Vec::new();
    let mut params: Vec<String> = Vec::new();
    let mut param_idx = 1;

    macro_rules! add_param {
        ($field:expr, $name:expr) => {
            if $field.is_some() {
                sets.push(format!("{} = ${}", $name, param_idx));
                params.push(format!("${}", param_idx));
                param_idx += 1;
            }
        };
    }
    add_param!(payload.name, "name");
    add_param!(payload.avatar_url, "avatar_url");
    add_param!(payload.home_machine, "home_machine");
    add_param!(payload.primary_profile_id, "primary_profile_id");
    add_param!(payload.visibility, "visibility");
    add_param!(payload.memory_scope, "memory_scope");
    if payload.tools_allowlist.is_some() {
        sets.push(format!("tools_allowlist = ${}", param_idx));
        param_idx += 1;
    }
    if payload.credentials_scope.is_some() {
        sets.push(format!("credentials_scope = ${}", param_idx));
        param_idx += 1;
    }
    add_param!(payload.extra_instructions, "extra_instructions");

    if sets.is_empty() {
        return err_resp(state, StatusCode::BAD_REQUEST, "No fields to update");
    }

    let sql = format!(
        "UPDATE agents SET updated_at = NOW(), {} WHERE id = ${} RETURNING *",
        sets.join(", "),
        param_idx
    );
    let mut db_query = sqlx::query_as::<_, Agent>(&sql);
    if let Some(ref v) = payload.name {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.avatar_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.home_machine {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.primary_profile_id {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.visibility {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.memory_scope {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.tools_allowlist {
        db_query = db_query.bind(serde_json::to_value(v).unwrap_or_else(|_| serde_json::json!([])));
    }
    if let Some(ref v) = payload.credentials_scope {
        db_query = db_query.bind(serde_json::to_value(v).unwrap_or_else(|_| serde_json::json!({})));
    }
    if let Some(ref v) = payload.extra_instructions {
        db_query = db_query.bind(v);
    }
    db_query = db_query.bind(id);

    match db_query.fetch_optional(&state.db).await {
        Ok(Some(a)) => Json(serde_json::json!({ "agent": a })).into_response(),
        Ok(None) => err_resp(state, StatusCode::NOT_FOUND, "Agent not found"),
        Err(e) => db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update agent",
            e,
        ),
    }
}

pub(crate) async fn delete_agent_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: agent management is not available",
        );
    }
    if let Some(resp) = agent_access_err(&state, &user, id).await {
        return resp;
    }
    // sessions.agent_id is ON DELETE SET NULL: the agent's
    // conversations survive the agent row (they just become
    // unattached sessions), so no session-count guard like profiles
    // has.
    match sqlx::query("DELETE FROM agents WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Agent not found"),
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete agent",
            e,
        ),
    }
}

// ============================================
// Conversations
// ============================================

#[derive(Deserialize)]
pub(crate) struct ListConversationsQuery {
    limit: Option<i64>,
    /// `latest=1` (the query form of "just the most recent one")
    /// overrides `limit`.
    latest: Option<i64>,
}

/// `GET /agents/:id/conversations` — the agent's sessions,
/// most-active first. `last_active` is the sessions-table column that
/// tracks activity (bumped on message dispatch and turn activity).
pub(crate) async fn list_conversations(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Query(query): Query<ListConversationsQuery>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, id).await {
        return resp;
    }
    let limit = if query.latest.is_some() {
        1
    } else {
        query.limit.unwrap_or(50)
    };
    match sqlx::query_as::<_, Session>(
        "SELECT * FROM sessions WHERE agent_id = $1 ORDER BY last_active DESC LIMIT $2",
    )
    .bind(id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    {
        Ok(s) => {
            state.metrics.inc_requests("GET /agents/:id/conversations");
            Json(serde_json::json!({ "conversations": s })).into_response()
        }
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list conversations",
            e,
        ),
    }
}

#[derive(Deserialize)]
pub(crate) struct CreateConversationRequest {
    title: Option<String>,
    /// Fork point: must be an existing session of THIS agent (400
    /// otherwise). Its messages are copied into the new session with
    /// sequence numbers reset to start at 1 (pre-H2 fork semantics;
    /// post-H2 this becomes pi-durable's native conversation fork and
    /// this endpoint keeps its shape).
    fork_from: Option<Uuid>,
    /// Directory anchor: the agent works directly in this existing
    /// directory instead of a fresh per-session tree.
    cwd: Option<String>,
}

/// `POST /agents/:id/conversations` — create a session bound to the
/// agent. The session runs on the agent's `primary_profile_id`; when
/// that is unset we fall back to the agent owner's most recently
/// created profile (mirrors "the default profile" — `create_session`
/// itself has no default, it requires the caller to name one).
pub(crate) async fn create_conversation(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Json(payload): Json<CreateConversationRequest>,
) -> Response {
    // Restricted (demo) keys may talk to agents; `cwd` anchors run
    // host-side, which demo keys are barred from (same gate as
    // `POST /sessions`).
    if user.restricted && payload.cwd.is_some() {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: working_dir anchors are not available (sessions run in the sandbox)",
        );
    }
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    let agent = match sqlx::query_as::<_, Agent>("SELECT * FROM agents WHERE id = $1")
        .bind(agent_id)
        .fetch_one(&state.db)
        .await
    {
        Ok(a) => a,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get agent",
                e,
            )
        }
    };

    // Resolve the profile: the agent's primary, else the owner's most
    // recently created profile as the default.
    let profile: Profile = if let Some(pid) = agent.primary_profile_id {
        match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
            .bind(pid)
            .fetch_optional(&state.db)
            .await
        {
            Ok(Some(p)) => p,
            Ok(None) => {
                return err_resp(
                    &state,
                    StatusCode::NOT_FOUND,
                    "Agent's primary profile no longer exists",
                )
            }
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get profile",
                    e,
                )
            }
        }
    } else {
        match sqlx::query_as::<_, Profile>(
            "SELECT * FROM profiles WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(agent.owner_id)
        .fetch_optional(&state.db)
        .await
        {
            Ok(Some(p)) => p,
            Ok(None) => {
                return err_resp(
                    &state,
                    StatusCode::NOT_FOUND,
                    "Agent has no primary profile and the owner has no default profile",
                )
            }
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get profile",
                    e,
                )
            }
        }
    };

    // The conversation can only run on a profile the caller may use
    // (the agent's owner obviously can; this gates e.g. an admin who
    // is not… admins pass, so this is belt-and-braces for the
    // fallback-to-owner's-profile path under multi-tenant setups).
    if !can_access(&user, profile.user_id) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found");
    }

    let title = payload.title.unwrap_or_else(|| {
        format!(
            "{} · {}",
            agent.name,
            chrono::Utc::now().format("%Y-%m-%d %H:%M")
        )
    });

    // Validate the cwd anchor up front (before the INSERT), same
    // rules as `create_session`.
    let anchor = match &payload.cwd {
        Some(dir) => {
            let p = std::path::Path::new(dir);
            if !p.is_absolute() || !p.is_dir() {
                return err_resp(
                    &state,
                    StatusCode::BAD_REQUEST,
                    "cwd must be an existing absolute directory",
                );
            }
            Some(dir.clone())
        }
        None => None,
    };

    // Fork gate: `fork_from` must be an existing session of THIS
    // agent. 400 (not 404) on purpose — the caller named the agent
    // in the path, so "that conversation isn't yours to fork" is a
    // malformed request, and the status must not depend on whether
    // the session exists at all (no existence leak).
    let fork_source: Option<Uuid> = match payload.fork_from {
        Some(f) => {
            let src: Option<Option<Uuid>> =
                match sqlx::query_scalar("SELECT agent_id FROM sessions WHERE id = $1")
                    .bind(f)
                    .fetch_optional(&state.db)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        return db_err(
                            &state,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to check fork source",
                            e,
                        )
                    }
                };
            match src {
                Some(Some(aid)) if aid == agent_id => Some(f),
                _ => {
                    return err_resp(
                        &state,
                        StatusCode::BAD_REQUEST,
                        "fork_from must be an existing conversation of this agent",
                    )
                }
            }
        }
        None => None,
    };

    let session: Session =
        match sqlx::query_as::<_, Session>(
            r#"INSERT INTO sessions (profile_id, title, user_id, working_dir, agent_id) VALUES ($1, $2, $3, $4, $5) RETURNING *"#,
        )
        .bind(profile.id)
        .bind(&title)
        .bind(user.user_id)
        .bind(&anchor)
        .bind(agent_id)
        .fetch_one(&state.db)
        .await
        {
            Ok(s) => s,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to create conversation",
                    e,
                )
            }
        };

    if let Some(dir) = &anchor {
        tracing::info!(
            session_id = %session.id,
            agent_id = %agent_id,
            working_dir = %dir,
            "agent conversation anchored to existing directory"
        );
        return (
            StatusCode::CREATED,
            Json(serde_json::json!({ "session": session, "working_dir": dir })),
        )
            .into_response();
    }
    // Working directory: prefer the per-session tree; on hosts without
    // a writable /forge/sessions (unprivileged local deployments),
    // fall back to an anchor-style default — the profile's working_dir,
    // then the forge process user's home — instead of failing the whole
    // conversation (os error 13 on `create_dir_all` used to kill it).
    let working_dir = match state
        .session_manager
        .create_session_dir(session.id, &profile)
        .await
    {
        Ok(working_dir) => working_dir,
        Err(e) => match fallback_conversation_dir(&profile)
            .or_else(std::env::home_dir)
            .filter(|p| p.is_dir())
        {
            Some(dir) => {
                tracing::warn!(
                    session_id = %session.id,
                    error = %e,
                    working_dir = %dir.display(),
                    "sessions tree unavailable; anchored agent conversation to fallback working dir"
                );
                dir
            }
            None => {
                let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                    .bind(session.id)
                    .execute(&state.db)
                    .await;
                tracing::error!(
                    session_id = %session.id,
                    error = %e,
                    "failed to create conversation working dir; rolled back session row"
                );
                return err_resp(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to create conversation",
                );
            }
        },
    };

    // Pre-H2 fork semantics: copy the source's message rows
    // into the new session with sequence numbers reset to
    // start at 1 (the new session's sequence space is empty,
    // so 1..N is free and `get_next_sequence` continues
    // cleanly from N).
    if let Some(f) = fork_source {
        if let Err(e) = copy_messages_forked(&state.db, session.id, f).await {
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            tracing::error!(
                session_id = %session.id,
                fork_from = %f,
                error = %e,
                "failed to copy forked messages; rolled back conversation row"
            );
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to fork conversation",
            );
        }
    }
    tracing::info!(
        session_id = %session.id,
        agent_id = %agent_id,
        fork_from = ?fork_source,
        "created agent conversation"
    );
    (
        StatusCode::CREATED,
        Json(
            serde_json::json!({ "session": session, "working_dir": working_dir.to_string_lossy() }),
        ),
    )
        .into_response()
}

/// Fallback working directory for an agent conversation on a host
/// where the per-session tree cannot be created: the profile's
/// `working_dir`, when it is a valid existing absolute directory.
/// (The handler additionally falls back to the process user's home.)
fn fallback_conversation_dir(profile: &Profile) -> Option<std::path::PathBuf> {
    let dir = std::path::PathBuf::from(profile.working_dir.trim());
    if dir.is_absolute() && dir.is_dir() {
        Some(dir)
    } else {
        None
    }
}

/// Copy all message rows from `src` into `dst` with the sequence
/// numbers reset to start at 1, preserving order. One statement; the
/// copy is atomic with the later turn of the new session.
async fn copy_messages_forked(db: &PgPool, dst: Uuid, src: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO messages (session_id, sequence, role, content, tool_name, tool_input, tool_call_id, tool_output, duration_ms)
           SELECT $1, row_number() OVER (ORDER BY sequence ASC), role, content, tool_name, tool_input, tool_call_id, tool_output, duration_ms
           FROM messages WHERE session_id = $2"#,
    )
    .bind(dst)
    .bind(src)
    .execute(db)
    .await?;
    Ok(())
}

// ============================================
// Tasks (stub) + active status
// ============================================

/// `GET /agents/:id/tasks` — stub returning an empty list until the
/// durable tasks land (H2.3). H5's Activity View depends on this
/// route existing.
pub(crate) async fn agent_tasks(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, id).await {
        return resp;
    }
    Json(serde_json::json!({ "tasks": [] })).into_response()
}

/// `GET /agents/:id/active` → `{busy: bool, current_conversation?}`.
///
/// Approach: the in-flight-turn marks in `AgentRegistry`
/// (`begin_turn`/`end_turn`, see `agent_registry.rs`) are keyed by
/// **session id**, not agent id, so we map session → agent through
/// the `sessions.agent_id` column: fetch the agent's sessions and ask
/// the registry for each one. `busy` is true when any of the
/// agent's sessions has a turn in flight; `current_conversation` is
/// the agent's most-active session id (NULL when it has none).
pub(crate) async fn agent_active(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, id).await {
        return resp;
    }
    let session_ids: Vec<Uuid> = match sqlx::query_scalar(
        "SELECT id FROM sessions WHERE agent_id = $1 ORDER BY last_active DESC",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list conversations",
                e,
            )
        }
    };
    let busy = session_ids
        .iter()
        .any(|s| state.agent_registry.has_in_flight_turn(*s));
    Json(serde_json::json!({
        "busy": busy,
        "current_conversation": session_ids.first(),
    }))
    .into_response()
}

// ============================================
// Agent-scoped message dispatch (H1.2)
// ============================================

#[derive(Deserialize)]
pub(crate) struct AgentMessagePath {
    id: Uuid,
    cid: Uuid,
}

#[derive(Deserialize)]
pub(crate) struct CreateAgentMessageRequest {
    content: String,
}

/// `POST /agents/:id/conversations/:cid/messages` — thin alias of
/// `POST /messages` so mule's wakes (H3) and cross-host callers can
/// address *agents* rather than raw sessions. Tenancy is checked
/// against the AGENT owner (owner-or-admin) plus the session
/// belonging to that agent; the turn-driving logic is the exact
/// shared [`dispatch_message`] path.
pub(crate) async fn create_agent_message(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(params): Path<AgentMessagePath>,
    Json(payload): Json<CreateAgentMessageRequest>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, params.id).await {
        return resp;
    }
    // The conversation must exist AND belong to this agent (404, not
    // 403 — no existence leak for foreign/other-agent sessions).
    let session_agent: Option<Option<Uuid>> =
        match sqlx::query_scalar("SELECT agent_id FROM sessions WHERE id = $1")
            .bind(params.cid)
            .fetch_optional(&state.db)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get conversation",
                    e,
                )
            }
        };
    match session_agent {
        Some(Some(aid)) if aid == params.id => {}
        _ => return err_resp(&state, StatusCode::NOT_FOUND, "Conversation not found"),
    }

    match dispatch_message(&state, params.cid, &payload.content).await {
        Ok(message) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "message": message })),
        )
            .into_response(),
        Err((status, msg)) => err_resp(&state, status, &msg),
    }
}
