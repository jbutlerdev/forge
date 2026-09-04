//! Message handlers: `GET /messages?session_id=…` and
//! `POST /messages` (which delegates to `crate::api::dispatch_message`).

use axum::{
    extract::{Extension, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use uuid::Uuid;

use super::{db_err, dispatch_message, err_resp, AppState};
use crate::api::auth::{can_access, AuthenticatedUser};
use crate::db::Message;

#[derive(Debug, Deserialize)]
pub(crate) struct SessionQuery {
    session_id: Uuid,
}

/// Fetch the session's owner, enforcing the tenancy gate: a
/// missing session or a session the caller can't access is a 404
/// (not 403 — don't leak existence). Returns `None` when the
/// caller may proceed (row exists AND access allowed).
pub(crate) async fn session_access_err(
    state: &AppState,
    user: &AuthenticatedUser,
    session_id: Uuid,
) -> Option<Response> {
    match sqlx::query_scalar::<_, Option<Uuid>>("SELECT user_id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(owner)) if can_access(user, owner) => None,
        Ok(_) => Some(err_resp(state, StatusCode::NOT_FOUND, "Session not found")),
        Err(e) => Some(db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get session",
            e,
        )),
    }
}

pub(crate) async fn list_messages_by_session(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<SessionQuery>,
) -> Response {
    if let Some(resp) = session_access_err(&state, &user, params.session_id).await {
        return resp;
    }
    match sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
    )
    .bind(params.session_id)
    .fetch_all(&state.db)
    .await
    {
        Ok(m) => Json(serde_json::json!({ "messages": m })).into_response(),
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list messages",
            e,
        ),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateMessageRequest {
    session_id: Uuid,
    content: String,
}

/// Send a message - pi processes it with timeouts
pub(crate) async fn create_message(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<CreateMessageRequest>,
) -> Response {
    let session_id = payload.session_id;

    // The session must exist AND the caller must own it (or be an
    // admin) before we touch the audit log or spawn the agent.
    if let Some(resp) = session_access_err(&state, &user, session_id).await {
        return resp;
    }

    match dispatch_message(&state, session_id, &payload.content).await {
        Ok(message) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "message": message })),
        )
            .into_response(),
        Err((status, msg)) => err_resp(&state, status, &msg),
    }
}
