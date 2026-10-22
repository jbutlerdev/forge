//! Message handlers: `GET /messages?session_id=…` and
//! `POST /messages` (which delegates to `crate::api::dispatch_message`).

use axum::{
    extract::{Extension, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use uuid::Uuid;

use super::{db_err, dispatch_message, err_resp, insert_and_publish_assistant, AppState, DispatchOutcome};
use crate::api::auth::{can_access, AuthenticatedUser};
use crate::bus::MessageBus;
use crate::db::Message;
use sqlx::PgPool;

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

/// Harness-side assistant projection: `POST /messages/assistant`
/// writes ONE `role='assistant'` row (same write path as the harness
/// event consumer's turn projection, `insert_and_publish_assistant`)
/// WITHOUT driving a turn. Used for the compaction fallback surfacing:
/// when the harness's bounded summarizer fails and truncation compaction
/// kicks in, it writes a `⚠ `-prefixed row here so humans/phones get
/// notified (mobile treats ⚠ rows as error notifications).
#[derive(Debug, Deserialize)]
pub(crate) struct CreateAssistantMessageRequest {
    session_id: Uuid,
    content: String,
}

pub(crate) async fn create_assistant_message(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<CreateAssistantMessageRequest>,
) -> Response {
    let session_id = payload.session_id;
    if let Some(resp) = session_access_err(&state, &user, session_id).await {
        return resp;
    }
    match insert_and_publish_assistant(&state.db, &state.bus, session_id, &payload.content).await {
        Some(message) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "message": message })),
        )
            .into_response(),
        None => err_resp(&state, StatusCode::BAD_REQUEST, "Empty assistant content"),
    }
}

/// Collapsible-checkin rule (migration 034). A "check-in" is one
/// scheduled wake turn: the user row carrying mule's `[mule wake …]`
/// prompt plus everything the agent writes after it, up to (but not
/// including) the next wake prompt. The mobile UI folds check-ins
/// away by default; this marks the CURRENT check-in's rows pinned so
/// it renders expanded. Two triggers:
///
/// * the harness `pin_checkin` tool (agent says: critical content),
/// * an `@notify:` row posted mid-check-in (notify_user already told
///   the user this matters — the fold must not then hide it).
///
/// `sequence >=` the newest wake row in the session; if the session
/// never had a wake the statement updates nothing (COALESCE 0 with an
/// explicit guard keeps that a no-op rather than pinning everything).
pub(crate) async fn pin_current_checkin(db: &PgPool, bus: &MessageBus, session_id: Uuid) -> u64 {
    // The newest wake row defines the current check-in; no wake ever
    // means no check-in to expand.
    let wake_seq: Option<i32> = sqlx::query_scalar(
        "SELECT MAX(sequence) FROM messages WHERE session_id = $1 AND role = 'user' AND content LIKE '[mule wake%'",
    )
    .bind(session_id)
    .fetch_optional(db)
    .await
    .unwrap_or(None)
    .flatten();
    let Some(wake) = wake_seq else { return 0 };

    // Idempotent: an already-expanded group (any pinned row OR the
    // marker from a previous call) is left alone — repeated calls and
    // notify-after-pin must not stack markers.
    let already: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE session_id = $1 AND sequence >= $2 AND (pinned OR (role = 'system' AND content = '[checkin-pinned]')))",
    )
    .bind(session_id)
    .bind(wake)
    .fetch_one(db)
    .await
    .unwrap_or(false);
    if already {
        return 0;
    }

    let n = match sqlx::query("UPDATE messages SET pinned = TRUE WHERE session_id = $1 AND sequence >= $2")
        .bind(session_id)
        .bind(wake)
        .execute(db)
        .await
    {
        Ok(r) => r.rows_affected(),
        Err(e) => {
            tracing::warn!("pin_current_checkin failed for {session_id}: {e}");
            return 0;
        }
    };

    // The UPDATE mutates existing rows, which live views never learn
    // about (the SSE stream delivers fresh sequences only). A fresh
    // system row solves both planes: delivered live (clients hide it
    // and expand the group on receipt) and durable on reload, next to
    // the pinned column. It also terminates the group scan for pure
    // marker-driven clients.
    if n > 0 {
        if let Ok(row) = sqlx::query_as::<_, Message>(
            "INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'system', $2) RETURNING *",
        )
        .bind(session_id)
        .bind("[checkin-pinned]")
        .fetch_one(db)
        .await
        {
            bus.publish_message(row);
        }
    }
    n
}

#[derive(Debug, Deserialize)]
pub(crate) struct PinCheckinRequest {
    session_id: Uuid,
}

/// Harness `pin_checkin` tool endpoint: expand the current scheduled
/// check-in in the mobile UI. Idempotent; outside a check-in it is a
/// successful no-op (`pinned: 0`).
pub(crate) async fn pin_checkin(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<PinCheckinRequest>,
) -> Response {
    let session_id = payload.session_id;
    if let Some(resp) = session_access_err(&state, &user, session_id).await {
        return resp;
    }
    let pinned = pin_current_checkin(&state.db, &state.bus, session_id).await;
    Json(serde_json::json!({ "pinned": pinned })).into_response()
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

    // A notification mid-check-in means the agent surfaced something
    // user-facing during a scheduled turn — expand the whole check-in
    // so the fold never hides what the agent deemed worth pinging.
    if payload.content.trim_start().starts_with("@notify:") {
        pin_current_checkin(&state.db, &state.bus, session_id).await;
    }

    match dispatch_message(&state, session_id, &payload.content).await {
        Ok(DispatchOutcome::Dispatched(message)) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "message": message })),
        )
            .into_response(),
        // F-00x: duplicate wake fire dropped (identical prompt
        // already queued) — a success, marked for the caller.
        Ok(DispatchOutcome::Coalesced { wake_id }) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "coalesced": true,
                "wake_id": wake_id,
            })),
        )
            .into_response(),
        Err((status, msg)) => err_resp(&state, status, &msg),
    }
}
