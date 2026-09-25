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
) -> Response {
    // Tenancy: admins see every session; a regular user sees only the
    // sessions they own. Legacy rows (`user_id IS NULL`) are
    // admin-only.
    let rows = if can_access(&user, None) {
        sqlx::query_as::<_, Session>("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 100")
            .fetch_all(&state.db)
            .await
    } else {
        sqlx::query_as::<_, Session>(
            "SELECT * FROM sessions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 100",
        )
        .bind(user.user_id)
        .fetch_all(&state.db)
        .await
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

/// **Sever** the session's agent: kill the in-memory pi subprocess and
/// drop the registry entry while leaving the session row, the
/// `messages` history, and the working tree untouched. The next
/// `POST /messages` respawns a fresh pi via the durable-resume path
/// (tool-call replay + `--session` jsonl), so the conversation picks up
/// exactly where it left off.
///
/// This is the operator "kill a stuck agent without losing history"
/// operation; it is also what the public *Sever &amp; Resume* demo
/// exercises. Idempotent: severing a session with no live agent is a
/// no-op that still reports success.
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

    let had_agent = state.agent_registry.contains(id).await;
    match state.agent_registry.remove(id).await {
        Ok(()) => Json(serde_json::json!({
            "status": "severed",
            "session_id": id,
            "agent_was_running": had_agent,
            "note": "agent process killed; session + working tree preserved; the next message triggers durable resume",
        }))
        .into_response(),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to sever agent: {e}"),
        ),
    }
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

/// Does the requested override `Value` differ from the session's
/// current column value? Used to skip a wasteful agent teardown on
/// a no-op update (e.g. re-sending the same model).
///   `None` (field omitted)             -> no change
///   `Null` vs `Some(_)` / `None`       -> differs iff current is set
///   `String(s)` vs `None`/`Some(other)` -> differs unless equal
fn override_differs(requested: Option<&serde_json::Value>, current: Option<&str>) -> bool {
    let Some(v) = requested else {
        return false;
    };
    match v {
        serde_json::Value::Null => current.is_some(),
        serde_json::Value::String(s) => current != Some(s.as_str()),
        // Non-string/non-null is a 400 (caught earlier); treat as
        // "differs" so we don't accidentally skip a needed teardown,
        // though the 400 short-circuits before this matters.
        _ => true,
    }
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

/// Compare the requested overrides against the session's current
/// column values; true when any override actually changed, so the
/// caller knows whether to tear down the in-memory agent.
fn overrides_changed(payload: &UpdateSession, current: &Session, api_key_redacted: bool) -> bool {
    override_differs(
        payload.provider.as_ref(),
        current.override_provider.as_deref(),
    ) || override_differs(payload.model.as_ref(), current.override_model.as_deref())
        || override_differs(
            payload.base_url.as_ref(),
            current.override_base_url.as_deref(),
        )
        || (!api_key_redacted
            && override_differs(
                payload.api_key.as_ref(),
                current.override_api_key.as_deref(),
            ))
}

/// Tear down the in-memory agent for a model switch: evict it from
/// the registry, mark the next spawn to keep the working tree, and
/// drop the session_manager entry (but NOT the sandbox — the working
/// dir is unchanged, so the existing container + replayed tool calls
/// stay valid).
async fn teardown_agent_for_model_switch(state: &AppState, id: Uuid, session: &Session) {
    tracing::info!(
        session_id = %id,
        new_provider = ?session.override_provider,
        new_model = ?session.override_model,
        "model switch: tearing down in-memory agent for override change (workspace preserved)"
    );
    // Removing the agent from the registry makes the next message
    // spawn a fresh pi that reads the new overrides.
    let _ = state.agent_registry.remove(id).await;
    // Tell the next `get_or_create` to KEEP the existing working
    // tree: without this, its `create_container` call would wipe
    // the dir back to the profile baseline and delete the
    // agent's untracked files / unrecorded edits, contradicting
    // the "workspace is preserved" contract of the model
    // switcher. The flag is consumed by that one spawn.
    state
        .agent_registry
        .preserve_working_dir_on_next_spawn(id)
        .await;
    // Also drop the session_manager entry so get_or_create's
    // working-dir resolution runs fresh — but NOT the sandbox
    // dir itself (destroy_container would wipe the working
    // tree we want to keep). remove_session just evicts the
    // in-memory map entry; the dir on disk is reused.
    let _ = state.session_manager.remove_session(id).await;
}

/// `PATCH /sessions/:id` — the model switcher (Option A). Updates
/// the session's `title` and/or its per-session model overrides
/// (`override_provider` / `override_model` / `override_base_url` /
/// `override_api_key`). When an override is set, the next message
/// spawns pi with the override instead of the profile's value —
/// so you change *just the brain* (provider + model + credentials)
/// while the workspace (working dir / git repo / tools /
/// system_prompt) stays as the profile configured it. The prior
/// conversation is replayed from the `messages` table, so history
/// is preserved.
///
/// Setting an override to `null` *clears* it (falls back to the
/// profile). Omitting the field leaves it alone. The request type
/// uses `Option<Option<String>>` to make that distinction.
///
/// The handler tears down the in-memory agent on any override
/// change so `get_or_create` doesn't short-circuit on the cached
/// (old-model) pi. We do NOT tear down the sandbox — the working
/// dir is unchanged (that's the whole point of Option A), so the
/// existing sandbox + replayed tool calls stay valid.
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

    // Only tear down the agent when an override really changed
    // value, so the next spawn will use the new model; a no-op
    // update skips the wasteful teardown.
    if overrides_changed(&payload, &current, api_key_redacted) {
        teardown_agent_for_model_switch(&state, id, &session).await;
    }

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
            // `agent_running` is advisory: true when a live pi subprocess
            // is registered for this session *at this instant*. After a
            // sever (or idle reap) it is false until the next message
            // respawns the agent via the durable-resume path.
            let agent_running = state.agent_registry.contains(id).await;
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

    let _ = state.agent_registry.remove(id).await;
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
async fn session_owner(
    db: &PgPool,
    id: Uuid,
) -> Result<Option<Uuid>, (StatusCode, String)> {
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

/// `GET /sessions/{id}/context` — current context-window usage for the
/// session's agent.
///
/// Prefers a live `get_session_stats` RPC to the session's running pi
/// process (accurate: reflects compaction state; pi does the bookkeeping
/// locally, no LLM call). Falls back to a rough chars/4 estimate over
/// the `messages` table when no agent is live or the RPC fails — the
/// same heuristic the long-context resume prelude uses.
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

    // Live stats from the running pi process (only when one is already
    // registered — `peek` never spawns; a cold session falls back to
    // the estimate below). Bounded so a wedged pi can't hold the
    // session's agent lock for long.
    if let Some(agent) = state.agent_registry.peek(id).await {
        if let Ok(mut pi) =
            tokio::time::timeout(std::time::Duration::from_secs(5), agent.lock()).await
        {
            if let Ok(Ok(stats)) =
                tokio::time::timeout(std::time::Duration::from_secs(30), pi.get_session_stats())
                    .await
            {
                if let Some(cu) = stats.pointer("/data/contextUsage") {
                    return Json(
                        serde_json::json!({
                            "session_id": id,
                            "source": "live",
                            "tokens": cu.get("tokens"),
                            "context_window": cu.get("contextWindow"),
                            "percent": cu.get("percent"),
                        }),
                    )
                    .into_response();
                }
            }
        }
    }

    // Fallback: rough estimate (chars/4) over the durable messages.
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
    Json(
        serde_json::json!({
            "session_id": id,
            "source": "estimate",
            "tokens": estimated,
            "context_window": serde_json::Value::Null,
            "percent": serde_json::Value::Null,
        }),
    )
    .into_response()
}

/// `POST /sessions/{id}/compact` — manually compact the session's pi
/// context now (instead of waiting for the auto threshold or the
/// long-context resume prelude). Records a `system` row in the message
/// history so chat clients can see the compaction.
///
/// 409 when a turn is in flight (compacting mid-turn would race the
/// running agent on pi's stdin/stdout).
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
    if state.agent_registry.has_in_flight_turn(id) {
        return err_resp(
            &state,
            StatusCode::CONFLICT,
            "agent is mid-turn; wait for it to finish before compacting",
        );
    }
    let agent = match state.agent_registry.get_or_create(&state.db, id).await {
        Ok(a) => a,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to get agent: {e}"),
            )
        }
    };
    let mut pi = agent.lock().await;
    match pi.compact(None).await {
        Ok(resp) => {
            let data = resp.get("data").cloned().unwrap_or(serde_json::Value::Null);
            let tokens_before = data.get("tokensBefore").cloned().unwrap_or(serde_json::Value::Null);
            let after = data
                .get("estimatedTokensAfter")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            // Record a system row so the compaction is visible in chat
            // history (and durable across agent respawns).
            let note = format!(
                "Context compacted ({} → {} est. tokens)",
                tokens_before.as_i64().map(|n| n.to_string()).unwrap_or_else(|| "?".into()),
                after.as_i64().map(|n| n.to_string()).unwrap_or_else(|| "?".into()),
            );
            if let Ok(row) = sqlx::query_as::<_, Message>(
                r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'system', $2) RETURNING *"#,
            )
            .bind(id)
            .bind(&note)
            .fetch_one(&state.db)
            .await
            {
                state.bus.publish_message(row);
            }
            Json(
                serde_json::json!({
                    "ok": true,
                    "session_id": id,
                    "tokens_before": tokens_before,
                    "estimated_tokens_after": after,
                }),
            )
            .into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::BAD_GATEWAY,
            &format!("compaction failed: {e}"),
        ),
    }
}

/// `POST /sessions/:id/interrupt` — interrupt the session's in-flight
/// turn. Immediate and non-destructive: the pi process, session file,
/// and conversation all survive. `drive_turn` holds the per-session
/// agent lock for the whole turn, so the abort goes straight to the
/// shared stdin pipe; the running event loop consumes pi's terminal
/// events (and the `response` line) and releases the lock. Idempotent:
/// a session with no live agent or no in-flight turn reports
/// `interrupted: false` and records nothing.
pub(crate) async fn interrupt_session(
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

    let agent = match state.agent_registry.peek(id).await {
        Some(a) => a,
        None => {
            return Json(serde_json::json!({
                "ok": true,
                "session_id": id,
                "interrupted": false,
                "note": "no live agent; nothing to interrupt",
            }))
            .into_response();
        }
    };

    let had_turn = state.agent_registry.has_in_flight_turn(id);
    if let Err(e) = agent.interrupt().await {
        return err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to interrupt agent: {e}"),
        );
    }

    // Record a system row so the interrupt is visible in chat history
    // (and durable across agent respawns). Only when a turn actually
    // was in flight — an idle interrupt is a no-op, not an event.
    if had_turn {
        if let Ok(row) = sqlx::query_as::<_, Message>(
            r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'system', $2) RETURNING *"#,
        )
        .bind(id)
        .bind("⏹ Turn interrupted")
        .fetch_one(&state.db)
        .await
        {
            state.bus.publish_message(row);
        }
    }

    Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "interrupted": had_turn,
    }))
    .into_response()
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
