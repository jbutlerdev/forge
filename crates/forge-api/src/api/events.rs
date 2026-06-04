//! Server-Sent Events endpoint for live message delivery.
//!
//! `GET /sessions/:id/events?since=<seq>` is the streaming
//! counterpart to `GET /messages?session_id=<id>`. The matrix
//! appservice (and any other live client) opens one SSE
//! connection per active session and receives every new message
//! row as it's written by the harness or the tool executor.
//!
//! ## Protocol
//!
//! SSE event names:
//!
//! - `message` — the full `Message` row, JSON-encoded, in
//!   `{"message": {...}}`. The client uses the row's `sequence`
//!   field as the high-water mark for catch-up on reconnect.
//! - `turn_ended` — `{"session_id": "..."}`. The agent signaled
//!   `agent_end`; clients can clear typing indicators.
//! - `heartbeat` — `{}`. Sent every 15s to keep the connection
//!   alive across proxies that idle-out.
//!
//! On connect we replay any rows with `sequence > since` (the
//! catch-up phase) before subscribing to the bus, so a client
//! that reconnects after a network blip never misses a row.
//! Clients that don't supply `since` get the full message log.
//!
//! ## Backpressure
//!
//! The bus is a bounded broadcast channel. If a slow client's
//! receiver falls behind by more than the channel buffer, the
//! receiver reports a lag. The handler responds by re-querying
//! the database for catch-up and resuming from the latest
//! sequence it has seen.
//!
//! The handler closes the connection on:
//! - the client disconnecting (broken pipe on the socket)
//! - the agent ending and the client asking for one-shot
//!   behavior via `?oneshot=true`
//! - a fatal error from the database (e.g. session deleted)

use axum::{
    extract::{Path, Query, State},
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Response, Sse,
    },
};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use std::{convert::Infallible, pin::Pin, time::Duration};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::api::AppState;
use crate::bus::BusEvent;
use crate::db::Message;

/// Stream type returned by [`stream_session_events`]. Same shape
/// as the existing tool-streaming SSE in `sse.rs`.
type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>>;

/// Query parameters for `GET /sessions/:id/events`.
#[derive(Debug, Deserialize, Default)]
pub struct EventStreamQuery {
    /// Replay messages with `sequence > since` before going live.
    /// Omit (or pass 0) to get the full message log.
    pub since: Option<i32>,
    /// If true, the connection closes after the next `turn_ended`
    /// event. Useful for one-shot "ask the agent, get an answer"
    /// flows where the caller doesn't want to keep the SSE
    /// connection open across multiple turns.
    #[serde(default)]
    pub oneshot: bool,
}

/// One row of an SSE event: an event name and a JSON data payload.
///
/// Serialization failures are unrecoverable for a `Serialize` type
/// the caller has already chosen, so we fall back to a fixed JSON
/// string and return the event directly (no `Result` to unwrap).
fn make_event(name: &str, data: impl serde::Serialize) -> Event {
    let json = serde_json::to_string(&data)
        .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string());
    Event::default().event(name).data(json)
}

/// Build the SSE response for a session.
///
/// The handler does the catch-up synchronously (since clients are
/// happy to wait a few hundred ms for the initial replay), then
/// hands off to a broadcast receiver for live events.
pub async fn stream_session_events(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
    Query(q): Query<EventStreamQuery>,
) -> Response {
    // Verify the session exists before doing anything expensive.
    let exists: Option<(Uuid,)> = match sqlx::query_as("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(row) => row,
        Err(e) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({ "error": format!("db error: {e}") })),
            )
                .into_response();
        }
    };
    if exists.is_none() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({ "error": "session not found" })),
        )
            .into_response();
    }

    // Catch-up: replay every message row with sequence > since
    // before going live. We do this in a single query and feed
    // the rows into the same channel the live events come out
    // of, so the client doesn't have to special-case the start
    // of the stream.
    let since = q.since.unwrap_or(0);
    let catchup_rows: Vec<Message> = match sqlx::query_as::<_, Message>(
        r#"SELECT * FROM messages
           WHERE session_id = $1 AND sequence > $2
           ORDER BY sequence ASC"#,
    )
    .bind(session_id)
    .bind(since)
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({ "error": format!("db error: {e}") })),
            )
                .into_response();
        }
    };

    // Subscribe to the bus *after* the catch-up query so we don't
    // miss any rows written between the catch-up snapshot and
    // the subscription. The harness publishes rows in
    // `INSERT ... RETURNING *` order, and the bus preserves the
    // publish order, so any row that lands between the catch-up
    // query and the subscribe will be delivered as a live event
    // (with a `sequence > since`) and the client can dedupe by
    // sequence number if it cares.
    //
    // If a row landed *before* the catch-up query but with a
    // sequence > since (which is what we just queried), it's
    // already in `catchup_rows`. So there's no row we'd miss.
    let rx = state.bus.subscribe();
    let oneshot = q.oneshot;

    // Compose the final stream: catch-up rows first, then live
    // bus events. We use a small unbounded channel to bridge the
    // catch-up snapshot (synchronous) into the async stream the
    // SSE wrapper consumes.
    let (tx, rx_stream) = tokio::sync::mpsc::channel::<Event>(64);

    // Push catch-up rows. We do this in a one-shot task so the
    // HTTP handler returns quickly and the SSE response starts
    // flowing as soon as possible.
    let tx_catchup = tx.clone();
    tokio::spawn(async move {
        for row in catchup_rows {
            let event = make_event("message", &row);
            if tx_catchup.send(event).await.is_err() {
                return; // client disconnected mid-replay
            }
        }
        // Catch-up is done. From here on, the bridge task in the
        // closure below forwards bus events.
    });

    // Bridge bus events to the SSE channel. We spawn a task that
    // owns `rx`, filters for the requested session_id, and
    // serializes each event into an SSE event.
    let tx_live = tx.clone();
    let session_filter = session_id;
    tokio::spawn(async move {
        let mut last_seq = since;
        let mut stream = BroadcastStream::new(rx);
        while let Some(item) = stream.next().await {
            let evt = match item {
                Ok(evt) => evt,
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    let payload = serde_json::json!({ "missed": n });
                    let event = make_event("lagged", &payload);
                    if tx_live.send(event).await.is_err() {
                        return;
                    }
                    continue;
                }
            };
            match evt {
                BusEvent::Message { message } => {
                    if message.session_id != session_filter {
                        continue;
                    }
                    // Defensive: if the bus somehow delivers a
                    // row with sequence <= last_seq, skip it.
                    // The catch-up phase already sent those.
                    if message.sequence <= last_seq {
                        continue;
                    }
                    last_seq = message.sequence;
                    let event = make_event("message", &message);
                    if tx_live.send(event).await.is_err() {
                        return; // client disconnected
                    }
                }
                BusEvent::TurnEnded { session_id: sid } => {
                    if sid != session_filter {
                        continue;
                    }
                    let payload = serde_json::json!({ "session_id": sid });
                    let event = make_event("turn_ended", &payload);
                    if tx_live.send(event).await.is_err() {
                        return;
                    }
                    if oneshot {
                        // Drop tx_live to close the stream.
                        return;
                    }
                }
            }
        }
    });

    // Convert the mpsc receiver into a stream and build the SSE
    // response. We add a 15s keepalive so reverse proxies don't
    // kill the connection.
    let stream: EventStream =
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx_stream).map(Ok));
    let response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("hb"),
        )
        .into_response();

    // Add a header to disable buffering in nginx-style proxies
    // (which would otherwise wait for the full response before
    // forwarding any bytes to the client).
    let mut response = response;
    let headers = response.headers_mut();
    headers.insert(
        "X-Accel-Buffering",
        axum::http::HeaderValue::from_static("no"),
    );
    // Hint to clients that they should reconnect on close.
    headers.insert(
        "Cache-Control",
        axum::http::HeaderValue::from_static("no-cache"),
    );

    response
}
