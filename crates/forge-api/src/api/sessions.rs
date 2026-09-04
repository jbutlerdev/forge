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
use crate::db::{Profile, Session, UpdateSession};
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateSessionRequest {
    profile_id: Uuid,
    title: Option<String>,
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

    // Tenancy gate: the caller must own the profile (or be an admin)
    // before a session can be carved out of it. 404, not 403 — don't
    // leak that the profile exists.
    if !can_access(&user, profile.user_id) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found");
    }

    let title = payload
        .title
        .unwrap_or_else(|| format!("Session {}", chrono::Utc::now().format("%Y-%m-%d %H:%M")));

    let session: Session = match sqlx::query_as::<_, Session>(
        r#"INSERT INTO sessions (profile_id, title, user_id) VALUES ($1, $2, $3) RETURNING *"#,
    )
    .bind(payload.profile_id)
    .bind(&title)
    .bind(user.user_id)
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
            Json(serde_json::json!({ "session": s })).into_response()
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
