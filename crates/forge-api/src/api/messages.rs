//! Message handlers: `GET /messages?session_id=…` and
//! `POST /messages` (which delegates to `crate::api::dispatch_message`).

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use uuid::Uuid;

use super::{db_err, dispatch_message, err_resp, AppState};
use crate::db::Message;

#[derive(Debug, Deserialize)]
pub(crate) struct SessionQuery {
    session_id: Uuid,
}

pub(crate) async fn list_messages_by_session(
    State(state): State<AppState>,
    Query(params): Query<SessionQuery>,
) -> Response {
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
    Json(payload): Json<CreateMessageRequest>,
) -> Response {
    let session_id = payload.session_id;

    let session_exists = sqlx::query("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .map(|r| r.is_some())
        .unwrap_or(false);
    if !session_exists {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
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
