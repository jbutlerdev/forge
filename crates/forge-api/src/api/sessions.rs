//! Session handlers: `POST/GET /sessions`, `PATCH /sessions/:id`
//! (the model switcher), the path- and query-based fetch/delete
//! routes, and the helper logic for the model switcher.

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use uuid::Uuid;

use super::{db_err, err_resp, AppState};
use crate::api::auth::{can_access, AuthenticatedUser};
use crate::db::{Message, Profile, Session, UpdateSession};
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateSessionRequest {
    profile_id: Uuid,
    title: Option<String>,
    /// Optional directory anchor (migration 014): the agent works
    /// directly in this EXISTING directory instead of a fresh
    /// per-session tree. Must be absolute + exist.
    working_dir: Option<String>,
}

pub(crate) async fn create_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<CreateSessionRequest>,
) -> Response {
    let profile: Profile =
        match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
            .bind(payload.profile_id)
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

    // Restricted (demo) keys: no `working_dir` anchors. An anchored
    // session runs host-side (no container) in the given directory —
    // the "local access" path. Demo sessions run in the sandboxed
    // per-session tree only.
    if user.restricted && payload.working_dir.is_some() {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: working_dir anchors are not available (sessions run in the sandbox)",
        );
    }

    // Tenancy gate: the caller must own the profile (or be an admin)
    // before a session can be carved out of it. 404, not 403 — don't
    // leak that the profile exists.
    if !can_access(&user, profile.user_id) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found");
    }

    let title = payload
        .title
        .unwrap_or_else(|| format!("Session {}", chrono::Utc::now().format("%Y-%m-%d %H:%M")));

    // Validate the anchor up front (before the INSERT): it must be an
    // existing directory on the host.
    let anchor = match &payload.working_dir {
        Some(dir) => {
            let p = std::path::Path::new(dir);
            if !p.is_absolute() || !p.is_dir() {
                return err_resp(
                    &state,
                    StatusCode::BAD_REQUEST,
                    "working_dir must be an existing absolute directory",
                );
            }
            Some(dir.clone())
        }
        None => None,
    };

    let session: Session = match sqlx::query_as::<_, Session>(
        r#"INSERT INTO sessions (profile_id, title, user_id, working_dir) VALUES ($1, $2, $3, $4) RETURNING *"#,
    )
    .bind(payload.profile_id)
    .bind(&title)
    .bind(user.user_id)
    .bind(&anchor)
    .fetch_one(&state.db)
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create session",
                e,
            )
        }
    };

    if let Some(dir) = &anchor {
        tracing::info!(
            session_id = %session.id,
            working_dir = %dir,
            "session anchored to existing directory"
        );
        // Herd H2.1/H2.6: attach a durable harness conversation (no-op
        // with the kill switch off or a disabled harness; never fails
        // session creation — see `attach_harness_conversation`).
        let mut session = session;
        session.durable_conversation_id =
            crate::harness::attach_harness_conversation(&state, &session, &profile).await;
        return (
            StatusCode::CREATED,
            Json(serde_json::json!({ "session": session, "working_dir": dir })),
        )
            .into_response();
    }
    match state
        .session_manager
        .create_session_dir(session.id, &profile)
        .await
    {
        Ok(working_dir) => {
            tracing::info!(
                "Created session {} with directory: {:?}",
                session.id,
                working_dir
            );
            // Herd H2.1/H2.6: attach a durable harness conversation
            // (same semantics as the anchored branch above).
            let mut session = session;
            session.durable_conversation_id =
                crate::harness::attach_harness_conversation(&state, &session, &profile).await;
            (StatusCode::CREATED, Json(serde_json::json!({ "session": session, "working_dir": working_dir.to_string_lossy() }))).into_response()
        }
        Err(e) => {
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            tracing::error!(
                session_id = %session.id,
                error = %e,
                "failed to create session working dir; rolled back session row"
            );
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create session",
            )
        }
    }
}

pub(crate) async fn list_all_sessions(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    // Herd H2.2: `?parent=<uuid>` lists one session's subagents
    // (`sessions.parent_session_id`). The parent itself must be
    // accessible (404 otherwise — don't leak that a session exists),
    // and children inherit the parent's tenancy, so the owner filter
    // still applies.
    let parent = match params.get("parent") {
        Some(p) => match uuid::Uuid::parse_str(p) {
            Ok(u) => {
                let owner = match session_owner(&state.db, u).await {
                    Ok(o) => o,
                    Err(e) => return e.into_response(),
                };
                if !can_access(&user, owner) {
                    return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
                }
                Some(u)
            }
            Err(_) => return err_resp(&state, StatusCode::BAD_REQUEST, "parent must be a UUID"),
        },
        None => None,
    };

    // Tenancy: admins see every session; a regular user sees only the
    // sessions they own. Legacy rows (`user_id IS NULL`) are
    // admin-only.
    let rows = match (can_access(&user, None), parent) {
        (true, None) => {
            sqlx::query_as::<_, Session>("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 100")
                .fetch_all(&state.db)
                .await
        }
        (true, Some(p)) => {
            sqlx::query_as::<_, Session>(
                "SELECT * FROM sessions WHERE parent_session_id = $1 ORDER BY created_at DESC LIMIT 100",
            )
            .bind(p)
            .fetch_all(&state.db)
            .await
        }
        (false, None) => {
            sqlx::query_as::<_, Session>(
                "SELECT * FROM sessions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 100",
            )
            .bind(user.user_id)
            .fetch_all(&state.db)
            .await
        }
        (false, Some(p)) => {
            sqlx::query_as::<_, Session>(
                "SELECT * FROM sessions WHERE user_id = $1 AND parent_session_id = $2 ORDER BY created_at DESC LIMIT 100",
            )
            .bind(user.user_id)
            .bind(p)
            .fetch_all(&state.db)
            .await
        }
    };
    match rows {
        Ok(s) => Json(serde_json::json!({ "sessions": s })).into_response(),
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list sessions",
            e,
        ),
    }
}

pub(crate) async fn get_session_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    get_session_core(&state, &user, id).await
}

pub(crate) async fn delete_session_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    delete_session_core(&state, &user, id).await
}

/// **Sever** the session's agent: the H2.6 cutover deleted the
/// in-process pi subprocess, so there is no agent process to kill —
/// this is now a no-op that reports whether a harness turn is
/// currently in flight. The session row, the `messages` history, and
/// the working tree are untouched, and the next `POST /messages`
/// simply runs on the session's durable conversation as before.
///
/// Kept for operator muscle-memory and existing clients (it is also
/// what the public *Sever &amp; Resume* demo exercises).
/// Idempotent: severing a session with no in-flight turn still
/// reports success.
///
/// Tenancy gate is identical to the other session routes (404, not
/// 403, for sessions the caller cannot access).
pub(crate) async fn sever_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    let owner: Option<Option<Uuid>> =
        match sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to sever session",
                    e,
                )
            }
        };
    match owner {
        Some(o) if can_access(&user, o) => {}
        _ => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
    }

    let turn_in_flight = state.agent_registry.has_in_flight_turn(id);
    Json(serde_json::json!({
        "status": "severed",
        "session_id": id,
        "agent_was_running": turn_in_flight,
        "note": "post-cutover there is no in-process agent to kill; session, history, and working tree are untouched and the next message continues on the durable conversation",
    }))
    .into_response()
}

/// Translate an override field (`serde_json::Value`) into the
/// `Option<String>` sqlx binds: `null` -> `None` (clear the
/// override), `"x"` -> `Some("x")`, anything else -> error. The
/// caller handles the "field omitted" case (no SET clause) before
/// calling this, so we only get here for present values.
fn override_to_bind(v: &serde_json::Value) -> Result<Option<String>, &'static str> {
    match v {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Ok(Some(s.clone())),
        _ => Err("override must be a string or null"),
    }
}

/// Is this override value the redacted placeholder the UI echoes
/// back? A redacted `override_api_key` means "keep the stored
/// value", never "set the key to the placeholder".
fn is_redacted_override(v: &serde_json::Value) -> bool {
    matches!(v, serde_json::Value::String(s) if s == crate::db::REDACTED_SECRET)
}

/// Whether a `PATCH /sessions/:id` payload has anything to update:
/// a `title`, or any non-redacted override. A redacted `api_key`
/// (the UI echoing the masked placeholder back) is not a change.
fn is_noop_update(payload: &UpdateSession, api_key_redacted: bool) -> bool {
    let has_override = payload.provider.is_some()
        || payload.model.is_some()
        || payload.base_url.is_some()
        || (payload.api_key.is_some() && !api_key_redacted);
    !has_override && payload.title.is_none()
}

/// Build the dynamic `UPDATE sessions ... RETURNING *` query for a
/// model-switcher patch. Each override is `Option<serde_json::Value>`:
/// `null` -> clear the override (bind `None`), `"x"` -> set it,
/// anything else -> error. An omitted field (`None`) is left out of
/// the SET list. `last_active = NOW()` uses no parameter, so $1 is
/// the first override bind. The `api_key` column is only touched
/// when `api_key_redacted` is false.
/// Error from [`apply_session_update`]: a bad override value (400)
/// vs. a DB failure (500, logged via `db_err`).
enum SessionUpdateError {
    BadField(&'static str),
    Db(sqlx::Error),
}

/// Build and execute the dynamic `UPDATE sessions ... RETURNING *`
/// query for a model-switcher patch. Each override is
/// `Option<serde_json::Value>`: `null` -> clear the override
/// (bind `None`), `"x"` -> set it, anything else -> error. An
/// omitted field (`None`) is left out of the SET list.
/// `last_active = NOW()` uses no parameter, so $1 is the first
/// override bind. The `api_key` column is only touched when
/// `api_key_redacted` is false.
async fn apply_session_update(
    db: &PgPool,
    payload: &UpdateSession,
    api_key_redacted: bool,
    id: Uuid,
) -> Result<Session, SessionUpdateError> {
    let mut sets = vec!["last_active = NOW()".to_string()];
    let mut idx = 1;
    if payload.provider.is_some() {
        sets.push(format!("override_provider = ${idx}"));
        idx += 1;
    }
    if payload.model.is_some() {
        sets.push(format!("override_model = ${idx}"));
        idx += 1;
    }
    if payload.base_url.is_some() {
        sets.push(format!("override_base_url = ${idx}"));
        idx += 1;
    }
    if payload.api_key.is_some() && !api_key_redacted {
        sets.push(format!("override_api_key = ${idx}"));
        idx += 1;
    }
    if payload.title.is_some() {
        sets.push(format!("title = ${idx}"));
        idx += 1;
    }
    let where_idx = idx;
    let sql = format!(
        "UPDATE sessions SET {} WHERE id = ${where_idx} RETURNING *",
        sets.join(", ")
    );
    let mut q = sqlx::query_as::<_, Session>(&sql);
    // Bind overrides: Value::Null -> None (clear), Value::String -> Some.
    let mut bad: Option<&'static str> = None;
    if let Some(ref v) = payload.provider {
        match override_to_bind(v) {
            Ok(b) => q = q.bind(b),
            Err(m) => bad = Some(m),
        }
    }
    if bad.is_none() {
        if let Some(ref v) = payload.model {
            match override_to_bind(v) {
                Ok(b) => q = q.bind(b),
                Err(m) => bad = Some(m),
            }
        }
    }
    if bad.is_none() {
        if let Some(ref v) = payload.base_url {
            match override_to_bind(v) {
                Ok(b) => q = q.bind(b),
                Err(m) => bad = Some(m),
            }
        }
    }
    if bad.is_none() && !api_key_redacted {
        if let Some(ref v) = payload.api_key {
            match override_to_bind(v) {
                Ok(b) => q = q.bind(b),
                Err(m) => bad = Some(m),
            }
        }
    }
    if let Some(m) = bad {
        return Err(SessionUpdateError::BadField(m));
    }
    if let Some(ref v) = payload.title {
        q = q.bind(v);
    }
    q = q.bind(id);

    q.fetch_one(db).await.map_err(SessionUpdateError::Db)
}

/// `PATCH /sessions/:id` — the model switcher (Option A). Updates
/// the session's `title` and/or its per-session model overrides
/// (`override_provider` / `override_model` / `override_base_url` /
/// `override_api_key`).
///
/// Setting an override to `null` *clears* it (falls back to the
/// profile). Omitting the field leaves it alone. The request type
/// uses `Option<Option<String>>` to make that distinction.
///
/// Herd H2.6: overrides apply at the NEXT conversation creation —
/// the attach (new session) or the lazy migration (pre-cutover
/// session). A session that is ALREADY attached keeps the model of
/// its durable conversation (the harness fixes the agent model at
/// `createConversation`); clearing an override on an attached
/// session does not retro-switch its model. Documented limitation.
///
/// Returns `{ session, profile }` so the UI can update its header
/// (effective model = override ?? profile.model) without a second
/// round-trip.
pub(crate) async fn update_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateSession>,
) -> Response {
    // The UI echoes the masked override_api_key back unchanged on
    // save; a redacted api_key is a no-op (keep the stored value),
    // not an override to apply.
    let api_key_redacted = payload
        .api_key
        .as_ref()
        .map(is_redacted_override)
        .unwrap_or(false);
    if is_noop_update(&payload, api_key_redacted) {
        return err_resp(&state, StatusCode::BAD_REQUEST, "No fields to update");
    }

    // Fetch the current session so we can (a) 404 if it doesn't
    // exist, and (b) detect whether any override actually changed
    // (tearing down on a no-op switch is wasteful).
    let current: Option<Session> =
        match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get session",
                    e,
                )
            }
        };
    let current = match current {
        Some(s) => s,
        None => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
    };
    // Tenancy gate: the caller must own the session (or be an admin).
    if !can_access(&user, current.user_id) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }

    let session = match apply_session_update(&state.db, &payload, api_key_redacted, id).await {
        Ok(s) => s,
        Err(SessionUpdateError::BadField(m)) => {
            return err_resp(&state, StatusCode::BAD_REQUEST, m)
        }
        Err(SessionUpdateError::Db(e)) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update session",
                e,
            )
        }
    };

    // Herd H2.6: there is no in-memory agent to tear down. The
    // override is recorded for the next conversation creation
    // (attach / lazy migration); see the handler docs.

    // Return the session + its (unchanged) profile so the UI can
    // compute the effective model = override ?? profile.*.
    let profile: Option<Profile> =
        match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
            .bind(session.profile_id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get profile",
                    e,
                )
            }
        };

    state.metrics.inc_requests("PATCH /sessions/:id");
    Json(serde_json::json!({ "session": session, "profile": profile })).into_response()
}

/// Shared body of the path-based (`/sessions/:id`) and query-based
/// (`/sessions/get?id=`, `/sessions/delete?id=`) session fetchers /
/// deleters. Both routes exist for backward compatibility; the logic
/// is identical. Deleting a session also tears down its in-memory
/// agent entry and sandbox container.
async fn get_session_core(state: &AppState, user: &AuthenticatedUser, id: Uuid) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        // 404 (not 403) for rows the caller cannot see: don't leak
        // existence of other users' sessions.
        Ok(Some(s)) if can_access(user, s.user_id) => {
            // `agent_running` is advisory: true while a harness turn
            // for this session is in flight (learned from the harness
            // event stream).
            let agent_running = state.agent_registry.has_in_flight_turn(id);
            Json(serde_json::json!({ "session": s, "agent_running": agent_running }))
                .into_response()
        }
        Ok(_) => err_resp(state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get session",
            e,
        ),
    }
}

async fn delete_session_core(state: &AppState, user: &AuthenticatedUser, id: Uuid) -> Response {
    // Tenancy gate first so an inaccessible session 404s identically
    // to a missing one (and we don't tear down agent/sandbox state
    // that doesn't belong to the caller).
    let owner: Option<Option<Uuid>> =
        match sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                return db_err(
                    state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to delete session",
                    e,
                )
            }
        };
    let owner = match owner {
        Some(o) => o,
        None => return err_resp(state, StatusCode::NOT_FOUND, "Session not found"),
    };
    if !can_access(user, owner) {
        return err_resp(state, StatusCode::NOT_FOUND, "Session not found");
    }

    let _ = state.session_manager.remove_session(id).await;
    let _ = state.sandbox_manager.destroy_container(id).await;
    match sqlx::query("DELETE FROM sessions WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(state, StatusCode::NOT_FOUND, "Session not found"),
        Err(e) => db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete session",
            e,
        ),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteSessionQuery {
    id: Uuid,
}
/// Shared tenancy lookup: the session's `user_id`, or 404 / 500.
async fn session_owner(db: &PgPool, id: Uuid) -> Result<Option<Uuid>, (StatusCode, String)> {
    match sqlx::query_scalar::<_, Uuid>("SELECT user_id FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(db)
        .await
    {
        Ok(o) => Ok(o),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {e}"),
        )),
    }
}

/// `GET /sessions/{id}/context` — current context-window usage for
/// the session's agent.
///
/// A harness-backed session's context is its ACTIVE window
/// (post-compaction/reset — entries before the head marker are
/// excluded): `compactionStatus` is the source of truth. An
/// unmigrated session (no durable conversation yet — the write
/// paths migrate lazily, reads do not) falls back to a rough chars/4
/// estimate over the `messages` table.
pub(crate) async fn get_session_context(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }

    if let Some(conversation_id) = state.harness.conversation_for_session(&state.db, id).await {
        return harness_context(&state, id, conversation_id).await;
    }

    // Unmigrated session: rough estimate (chars/4) over the durable
    // messages.
    let estimated: i64 =
        match sqlx::query_scalar(
            "SELECT COALESCE(SUM(LENGTH(content) + COALESCE(LENGTH(tool_input::text), 0) + COALESCE(LENGTH(tool_output::text), 0)), 0)::bigint / 4 FROM messages WHERE session_id = $1",
        )
        .bind(id)
        .fetch_one(&state.db)
        .await
        {
            Ok(n) => n,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to estimate context",
                    e,
                )
            }
        };
    Json(serde_json::json!({
        "session_id": id,
        "source": "estimate",
        "tokens": estimated,
        "context_window": serde_json::Value::Null,
        "percent": serde_json::Value::Null,
    }))
    .into_response()
}

/// `POST /sessions/{id}/compact` — manually compact the session's
/// context now (instead of waiting for the auto threshold).
///
/// Herd H2.4/H2.6: the compaction runs as a background task on the
/// session's durable conversation (the harness `compact`); the
/// in-flight turn (if any) is never interrupted. An unmigrated
/// session is migrated on this first write before the compact is
/// submitted.
pub(crate) async fn compact_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }

    let conversation_id = match crate::harness_migration::ensure_migrated(&state, id, None).await {
        Ok(c) => c,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness unavailable: {e} (compaction was not submitted)"),
            )
        }
    };
    harness_compact(&state, id, conversation_id).await
}

/// `POST /sessions/:id/interrupt` — interrupt the session's in-flight
/// turn. The turn runs on the harness: aborting forwards to the
/// harness `abort` (the active task is learned from the harness event
/// stream). Idempotent: a session with no in-flight turn reports
/// `interrupted: false` and records nothing.
pub(crate) async fn interrupt_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }

    // `?tree=false` aborts the task alone; the default aborts the
    // whole ownership tree.
    let tree = params.get("tree").map(|v| v != "false").unwrap_or(true);

    // Herd H2.6: there is no in-process agent — an unmigrated session
    // (no durable conversation yet) has no in-flight turn anywhere.
    let conversation_id = match state.harness.conversation_for_session(&state.db, id).await {
        Some(c) => c,
        None => {
            return Json(serde_json::json!({
                "ok": true,
                "session_id": id,
                "interrupted": false,
                "note": "no in-flight harness task; nothing to interrupt",
            }))
            .into_response();
        }
    };
    harness_interrupt(&state, id, conversation_id, tree).await
}

/// Herd H2.0 part 2: interrupt a **harness-backed** session.
///
/// The active task is learned from the harness event stream (`task_state`
/// events, kept in [`crate::harness::HarnessState`]) — the harness IPC
/// has no "tasks for a conversation" lookup. No known in-flight task ⇒
/// no-op (same shape as the legacy path's "no live agent" reply). A
/// disabled harness (API restarted with the harness down, column
/// stamped from a previous run) ⇒ 503 HarnessUnavailable, no panic.
async fn harness_interrupt(
    state: &AppState,
    session_id: Uuid,
    conversation_id: i64,
    tree: bool,
) -> Response {
    // A disabled harness (socket unset/absent at startup, or the API
    // restarted with the harness down) cannot answer: HarnessUnavailable
    // as a 503, per the H2.0 contract — never a panic.
    if !state.harness.is_enabled() {
        return err_resp(
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot interrupt a harness-backed session",
        );
    }

    let task_id = match state.harness.active_task(conversation_id).await {
        Some(t) => t,
        None => {
            return Json(serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "interrupted": false,
                "note": "no in-flight harness task; nothing to interrupt",
            }))
            .into_response();
        }
    };

    match state.harness.client().abort(task_id, tree).await {
        Ok(n) => {
            state.harness.clear_active(conversation_id).await;
            state.agent_registry.end_turn(session_id);

            // Record a system row so the interrupt is visible in chat
            // history (same as the legacy path above).
            if n > 0 {
                if let Ok(row) = sqlx::query_as::<_, Message>(
                    r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'system', $2) RETURNING *"#,
                )
                .bind(session_id)
                .bind("⏹ Turn interrupted")
                .fetch_one(&state.db)
                .await
                {
                    state.bus.publish_message(row);
                }
            }

            Json(serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "interrupted": n > 0,
                "aborted": n,
            }))
            .into_response()
        }
        Err(forge_harness_client::HarnessError::Unavailable) => err_resp(
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot interrupt a harness-backed session",
        ),
        Err(e) => err_resp(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("harness abort failed: {e}"),
        ),
    }
}

// ============================================
// Herd H2.4 / H2.5: harness-backed session routes
// ============================================

/// Herd H2.4: the `GET /sessions/:id/context` reply for a harness-
/// backed session — the ACTIVE window from `compactionStatus`
/// (post-compaction/reset; entries before the head marker are
/// excluded). A disabled harness cannot answer: 503, no panic.
async fn harness_context(state: &AppState, session_id: Uuid, conversation_id: i64) -> Response {
    if !state.harness.is_enabled() {
        return err_resp(
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot read a harness-backed session's context",
        );
    }
    match state
        .harness
        .client()
        .compaction_status(conversation_id)
        .await
    {
        Ok(s) => Json(serde_json::json!({
            "session_id": session_id,
            "source": "harness",
            "active_context_chars": s.active_context_chars,
            "active_entry_count": s.active_entry_count,
            "last_compaction": s.last_compaction,
            "compactions": s.compactions,
        }))
        .into_response(),
        Err(forge_harness_client::HarnessError::Unavailable) => err_resp(
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot read a harness-backed session's context",
        ),
        Err(e) => err_resp(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("harness compactionStatus failed: {e}"),
        ),
    }
}

/// Herd H2.4: the `POST /sessions/:id/compact` forward for a
/// harness-backed session. The harness runs the compaction as a
/// background task; the reply carries the task id so clients can follow
/// it (the summary lands at once when idle or at the next turn
/// boundary). No in-flight 409 — the running turn is never interrupted.
async fn harness_compact(state: &AppState, session_id: Uuid, conversation_id: i64) -> Response {
    if !state.harness.is_enabled() {
        return err_resp(
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot compact a harness-backed session",
        );
    }
    match state.harness.client().compact(conversation_id, None).await {
        Ok(task_id) => Json(serde_json::json!({
            "ok": true,
            "session_id": session_id,
            "task_id": task_id,
        }))
        .into_response(),
        Err(forge_harness_client::HarnessError::Unavailable) => err_resp(
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot compact a harness-backed session",
        ),
        Err(e) => err_resp(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("harness compact failed: {e}"),
        ),
    }
}

/// The durable entry's text: every `text` block of every message in
/// the entry's `model` array, joined with newlines (any role — user,
/// assistant, tool result, compaction summary alike). The history
/// search (Herd H2.4) matches over this.
fn entry_text(record: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(record) else {
        return String::new();
    };
    let mut texts: Vec<&str> = Vec::new();
    if let Some(messages) = value.get("model").and_then(|m| m.as_array()) {
        for message in messages {
            if let Some(blocks) = message.get("content").and_then(|c| c.as_array()) {
                for block in blocks {
                    if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                        if let Some(t) = block.get("text").and_then(|t| t.as_str()) {
                            if !t.is_empty() {
                                texts.push(t);
                            }
                        }
                    }
                }
            }
        }
    }
    texts.join("\n")
}

/// Herd H2.4: `GET /sessions/:id/history?q=` — full-text search over a
/// harness-backed session's **durable** entries (`durable_entries`
/// in the `FORGE_HARNESS_SCHEMA` schema), NOT the projected
/// `messages` table. This is the read path that stays valid across
/// compaction and reset: those operations move entries out of the
/// model's ACTIVE window but never delete them, so the full history
/// remains searchable. Portable `ILIKE`; the trigram/GIN companion
/// index (when `pg_trgm` is available) accelerates the leading-wildcard
/// match — the query is the same with or without it.
///
/// Harness-only: a legacy (unstamped) session has no durable entries,
/// so this returns 400 rather than silently searching `messages`
pub(crate) async fn search_session_history(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    let conversation_id =
        match state.harness.conversation_for_session(&state.db, id).await {
            Some(c) => c,
            None => return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                "session is not harness-backed; /history searches the harness's durable entries",
            ),
        };
    let q = params.get("q").map(|s| s.trim()).filter(|s| !s.is_empty());
    let Some(q) = q else {
        return err_resp(&state, StatusCode::BAD_REQUEST, "missing ?q= search term");
    };
    let schema = state.harness.durable_schema();
    let like = format!("%{q}%");
    // The schema identifier is validated at read time (is_sql_identifier)
    // and the search term is a bind param, so this interpolation is safe.
    let sql = format!(
        r#"SELECT id, record FROM "{schema}".durable_entries WHERE conversation_id = $1 AND record ILIKE $2 ORDER BY id ASC LIMIT 200"#
    );
    let rows: Result<Vec<(i64, String)>, sqlx::Error> = sqlx::query_as(&sql)
        .bind(conversation_id)
        .bind(&like)
        .fetch_all(&state.db)
        .await;
    let matches = match rows {
        Ok(rows) => rows
            .into_iter()
            .map(|(entry_id, record)| {
                let text = entry_text(&record);
                serde_json::json!({
                    "entry_id": entry_id,
                    "text": text,
                })
            })
            .collect::<Vec<_>>(),
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to search history",
                e,
            )
        }
    };
    Json(serde_json::json!({
        "session_id": id,
        "query": q,
        "matches": matches,
    }))
    .into_response()
}

/// Herd H2.4: `POST /sessions/:id/reset` — start a fresh context
/// segment on a harness-backed session from an optional handoff note
/// (pi-durable `reset()`). The model no longer sees the older entries,
/// but they stay in `durable_entries` (the `/history?q=` read path
/// searches them). The reset is admitted as a write submission: placed
/// at once when idle, otherwise at the next turn boundary.
#[derive(Debug, Deserialize)]
pub(crate) struct ResetSessionRequest {
    handoff_note: Option<String>,
}

pub(crate) async fn reset_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Json(body): Json<ResetSessionRequest>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    // Herd H2.6: reset is a write — an unmigrated session migrates on
    // this first touch.
    let conversation_id = match crate::harness_migration::ensure_migrated(&state, id, None).await {
        Ok(c) => c,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness unavailable: {e} (reset was not admitted)"),
            )
        }
    };
    if !state.harness.is_enabled() {
        return err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot reset a harness-backed session",
        );
    }
    let note = body.handoff_note.filter(|s| !s.trim().is_empty());
    match state
        .harness
        .client()
        .reset(conversation_id, note.as_deref())
        .await
    {
        Ok(()) => Json(serde_json::json!({
            "ok": true,
            "session_id": id,
            "conversation_id": conversation_id,
            "note": note,
            "state": "admitted (lands at once when idle, else at the next turn boundary)",
        }))
        .into_response(),
        Err(forge_harness_client::HarnessError::Unavailable) => err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot reset a harness-backed session",
        ),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("harness reset failed: {e}"),
        ),
    }
}

/// Herd H2.5: `GET /sessions/:id/documents/{name}` — read one
/// conversation document by family name. The harness schema's document
/// row is the source of truth. Absent document ⇒ 404.
pub(crate) async fn get_session_document(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((id, name)): Path<(Uuid, String)>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    let conversation_id = match state.harness.conversation_for_session(&state.db, id).await {
        Some(c) => c,
        None => {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                "session is not harness-backed; /documents is a harness operation",
            )
        }
    };
    if !state.harness.is_enabled() {
        return err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot read a harness-backed session's documents",
        );
    }
    match state
        .harness
        .client()
        .document_get(conversation_id, &name)
        .await
    {
        Ok(Some(value)) => Json(serde_json::json!({
            "session_id": id,
            "name": name,
            "value": value,
        }))
        .into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "document not found"),
        Err(forge_harness_client::HarnessError::Unavailable) => err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot read a harness-backed session's documents",
        ),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("harness documentGet failed: {e}"),
        ),
    }
}

/// Herd H2.5: `PUT /sessions/:id/documents/{name}` — create or replace
/// one conversation document. The harness emits a `document_changed`
/// event on the commit, which the event consumer republishes on this
/// session's SSE stream.
#[derive(Debug, Deserialize)]
pub(crate) struct PutSessionDocumentRequest {
    value: serde_json::Value,
}

pub(crate) async fn put_session_document(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((id, name)): Path<(Uuid, String)>,
    Json(body): Json<PutSessionDocumentRequest>,
) -> Response {
    let owner = match session_owner(&state.db, id).await {
        Ok(o) => o,
        Err(e) => return e.into_response(),
    };
    if !can_access(&user, owner) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    // Herd H2.6: document put is a write — an unmigrated session
    // migrates on this first touch.
    let conversation_id = match crate::harness_migration::ensure_migrated(&state, id, None).await {
        Ok(c) => c,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness unavailable: {e} (document was not written)"),
            )
        }
    };
    if !state.harness.is_enabled() {
        return err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot write a harness-backed session's documents",
        );
    }
    match state
        .harness
        .client()
        .document_put(conversation_id, &name, &body.value)
        .await
    {
        Ok(()) => Json(serde_json::json!({
            "ok": true,
            "session_id": id,
            "name": name,
        }))
        .into_response(),
        Err(forge_harness_client::HarnessError::Unavailable) => err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled mode); cannot write a harness-backed session's documents",
        ),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("harness documentPut failed: {e}"),
        ),
    }
}

/// **Deprecated** query-based alias of the canonical path route
/// `DELETE /sessions/{id}`. Kept for CLI / web-UI compatibility; see
/// the "Deprecated routes" note in `docs/API.md`.
pub(crate) async fn delete_session_by_id(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<DeleteSessionQuery>,
) -> Response {
    delete_session_core(&state, &user, params.id).await
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetSessionQuery {
    id: Uuid,
}
/// **Deprecated** query-based alias of the canonical path route
/// `GET /sessions/{id}`. Kept for CLI / web-UI compatibility; see the
/// "Deprecated routes" note in `docs/API.md`.
pub(crate) async fn get_session_by_id(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<GetSessionQuery>,
) -> Response {
    get_session_core(&state, &user, params.id).await
}

// ============================================
// Herd H2.3: durable timers on a session
// ============================================

/// The timer gate's failures: 404 (tenancy / unknown session) vs
/// 503 (harness disabled or the session is not harness-backed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerGateError {
    NotFound,
    HarnessUnavailable,
}

impl TimerGateError {
    fn into_response(self, state: &AppState) -> Response {
        match self {
            Self::NotFound => err_resp(state, StatusCode::NOT_FOUND, "Session not found"),
            Self::HarnessUnavailable => err_resp(
                state,
                StatusCode::SERVICE_UNAVAILABLE,
                "harness unavailable, or session is not harness-backed; timers are unavailable",
            ),
        }
        .into_response()
    }
}

/// Shared gate for the timer routes: tenancy (404 when the caller
/// can't access the session) + harness-backing. `write: true` (the
/// timer-SET route) lazily migrates an unmigrated session first —
/// timers are a write operation (H2.6: first write touch migrates);
/// the read routes (list/clear) keep the stamp-only gate so they
/// never pay for a migration.
///
/// Returns the durable conversation id on success.
async fn timer_gate(
    state: &AppState,
    user: &AuthenticatedUser,
    session_id: Uuid,
    write: bool,
) -> Result<i64, TimerGateError> {
    let owner = match session_owner(&state.db, session_id).await {
        Ok(o) => o,
        Err(_) => return Err(TimerGateError::NotFound),
    };
    if !can_access(user, owner) {
        return Err(TimerGateError::NotFound);
    }
    if !state.harness.is_enabled() {
        return Err(TimerGateError::HarnessUnavailable);
    }
    if !state.harness_messages {
        return Err(TimerGateError::HarnessUnavailable);
    }
    if let Some(conversation_id) = state
        .harness
        .conversation_for_session(&state.db, session_id)
        .await
    {
        return Ok(conversation_id);
    }
    if write {
        return crate::harness_migration::ensure_migrated(state, session_id, None)
            .await
            .map_err(|e| {
                tracing::warn!(session_id = %session_id, error = %e, "timer gate: migration failed");
                TimerGateError::HarnessUnavailable
            });
    }
    Err(TimerGateError::HarnessUnavailable)
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateTimerRequest {
    /// Absolute fire time, epoch ms. Exactly one of `at` / `cron`.
    at_ms: Option<u64>,
    /// 5-field cron expression (UTC) for recurring timers.
    cron: Option<String>,
    /// The prompt submitted as a turn when the timer fires.
    prompt: String,
}

/// `POST /sessions/:id/timers` — schedule a durable timer on the
/// session's harness conversation. 201 + the timer id on success.
///
/// Durability (H2.3): the timer row lives in Postgres (`harness_timers`
/// in the harness schema), not in the harness process's memory. A
/// kill -9 during the timer leaves the row un-claimed; the next boot
/// reloads it and fires it exactly once through the atomic claim.
pub(crate) async fn create_session_timer(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Json(body): Json<CreateTimerRequest>,
) -> Response {
    let conversation_id = match timer_gate(&state, &user, id, true).await {
        Ok(c) => c,
        Err(e) => return e.into_response(&state),
    };
    let at = body.at_ms;
    let cron = body
        .cron
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let prompt = body.prompt.trim();
    if prompt.is_empty() {
        return err_resp(&state, StatusCode::BAD_REQUEST, "prompt must not be empty");
    }
    match (at, cron.as_ref()) {
        (Some(_), Some(_)) => {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                "exactly one of at_ms or cron must be set",
            );
        }
        (None, None) => {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                "exactly one of at_ms or cron must be set",
            );
        }
        _ => {}
    }
    if let Some(at) = at {
        if at
            <= std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                "at_ms must be in the future",
            );
        }
    }
    match state
        .harness
        .client()
        .timer_set(conversation_id, at, cron, prompt)
        .await
    {
        Ok(timer_id) => {
            tracing::info!(
                session_id = %id,
                conversation_id,
                %timer_id,
                "durable timer scheduled (H2.3)"
            );
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "ok": true,
                    "session_id": id,
                    "timer_id": timer_id,
                    "at_ms": at,
                    "cron": cron,
                    "prompt": prompt,
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(session_id = %id, error = %e, "harness timerSet failed");
            err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness timerSet failed: {e}"),
            )
        }
    }
}

// ============================================
// Herd H5.2: the `schedule_reminder` agent tool's door
// ============================================

/// `POST /sessions/:id/reminders` — the Herd H5.2 `schedule_reminder`
/// agent tool's door (wake-matrix row: user "check back in N days").
/// Internally the H2.3 durable-timer machinery: a timer on this
/// session's harness conversation whose prompt is
/// `[reminder] <message>` — when it fires, the harness re-prompts the
/// SAME conversation with `timer fired: [reminder] <message>` (the
/// H2.3 fire shape), so the agent recognizes it as its own reminder.
/// Exactly one of `in_minutes` / `cron` must be set.
#[derive(Debug, Deserialize)]
pub(crate) struct CreateReminderRequest {
    /// The reminder text (re-prompted into the conversation, prefixed
    /// `[reminder] `).
    message: String,
    /// Fire in N minutes (one-shot). Exactly one of `in_minutes` /
    /// `cron`.
    in_minutes: Option<u64>,
    /// 5-field cron expression (UTC) for a recurring reminder. Exactly
    /// one of `in_minutes` / `cron`.
    cron: Option<String>,
}

pub(crate) async fn create_session_reminder(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
    Json(body): Json<CreateReminderRequest>,
) -> Response {
    // Validate the payload first so input errors are 400 even when
    // the harness is down (a 503 would mask the bad request).
    let message = body.message.trim();
    if message.is_empty() {
        return err_resp(&state, StatusCode::BAD_REQUEST, "message must not be empty");
    }
    let cron = body
        .cron
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match (body.in_minutes, cron.as_ref()) {
        (Some(_), Some(_)) | (None, None) => {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                "exactly one of in_minutes or cron must be set",
            );
        }
        (Some(0), None) => {
            return err_resp(&state, StatusCode::BAD_REQUEST, "in_minutes must be > 0");
        }
        _ => {}
    }
    let conversation_id = match timer_gate(&state, &user, id, true).await {
        Ok(c) => c,
        Err(e) => return e.into_response(&state),
    };
    // The prompt the H2.3 fire path re-prompts with:
    // `timer fired: [reminder] <message>`.
    let prompt = format!("[reminder] {message}");
    let (at_ms, when) = match (body.in_minutes, cron.as_ref()) {
        (Some(minutes), _) => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let at = now_ms.saturating_add(minutes.saturating_mul(60_000));
            let iso = chrono::DateTime::from_timestamp_millis(at as i64)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "(unrepresentable fire time)".into());
            (Some(at), iso)
        }
        (None, Some(c)) => (None, c.to_string()),
        _ => unreachable!(),
    };
    match state
        .harness
        .client()
        .timer_set(conversation_id, at_ms, cron, &prompt)
        .await
    {
        Ok(timer_id) => {
            tracing::info!(
                session_id = %id,
                conversation_id,
                %timer_id,
                "schedule_reminder: durable timer scheduled (H5.2)"
            );
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "scheduled": true,
                    "session_id": id,
                    "timer_id": timer_id,
                    "when": when,
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(session_id = %id, error = %e, "harness timerSet failed (reminder)");
            err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness timerSet failed: {e}"),
            )
        }
    }
}

/// `GET /sessions/:id/timers` — the session's live durable timers
/// (un-fired one-shots + still-recurring cron rows; a fired one-shot
/// no longer lists).
pub(crate) async fn list_session_timers(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    let conversation_id = match timer_gate(&state, &user, id, false).await {
        Ok(c) => c,
        Err(e) => return e.into_response(&state),
    };
    match state
        .harness
        .client()
        .timer_list(Some(conversation_id))
        .await
    {
        Ok(timers) => Json(serde_json::json!({
            "session_id": id,
            "timers": timers,
        }))
        .into_response(),
        Err(e) => {
            tracing::warn!(session_id = %id, error = %e, "harness timerList failed");
            err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness timerList failed: {e}"),
            )
        }
    }
}

/// `DELETE /sessions/:id/timers/:timer_id` — clear a live timer.
/// 200 + `cleared: true` when it existed; 404 when the timer is
/// unknown (already fired-and-consumed one-shots are gone from the
/// live set, so clearing them reports 404 too).
pub(crate) async fn delete_session_timer(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((id, timer_id)): Path<(Uuid, String)>,
) -> Response {
    let conversation_id = match timer_gate(&state, &user, id, false).await {
        Ok(c) => c,
        Err(e) => return e.into_response(&state),
    };
    match state
        .harness
        .client()
        .timer_clear(conversation_id, &timer_id)
        .await
    {
        Ok(true) => {
            tracing::info!(session_id = %id, %timer_id, "durable timer cleared (H2.3)");
            Json(serde_json::json!({
                "ok": true,
                "session_id": id,
                "timer_id": timer_id,
                "cleared": true,
            }))
            .into_response()
        }
        Ok(false) => err_resp(
            &state,
            StatusCode::NOT_FOUND,
            "no such live timer on this session",
        ),
        Err(e) => {
            tracing::warn!(session_id = %id, error = %e, "harness timerClear failed");
            err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness timerClear failed: {e}"),
            )
        }
    }
}
