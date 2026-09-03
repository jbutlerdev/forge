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
//! - `message` — the full `Message` row, JSON-encoded as the
//!   event data. The client uses the row's `sequence` field as
//!   the high-water mark for catch-up on reconnect.
//! - `turn_ended` — `{"session_id": "..."}`. The agent signaled
//!   `agent_end`; clients can clear typing indicators.
//! - `lagged` — `{"missed": n, "recovered": m}`. The in-process
//!   bus dropped `n` events for this (slow) connection and the
//!   handler re-queried the DB, backfilling `m` rows as
//!   `message` events immediately before this one.
//! - `heartbeat` — `{}`. Sent every 15s to keep the connection
//!   alive across proxies that idle-out.
//!
//! On connect the handler subscribes to the bus *first*, then
//! runs the catch-up query for rows with `sequence > since`,
//! then delivers catch-up rows followed by live bus events from
//! a single producer task, deduplicating by `sequence`.
//! Subscribing before the query closes the window in which a row
//! committed between the two would land in neither the catch-up
//! snapshot nor the (new) broadcast receiver. Clients that don't
//! supply `since` get the full message log.
//!
//! ## Backpressure
//!
//! The bus is a bounded broadcast channel. If a slow client's
//! receiver falls behind by more than the channel buffer, the
//! receiver reports a lag. The handler responds by re-querying
//! the database for rows with `sequence > last delivered` and
//! emitting them (plus a `lagged` event reporting how many rows
//! the re-query recovered), then resumes from the live stream.
//!
//! The handler closes the connection on:
//! - the client disconnecting (broken pipe on the socket)
//! - the agent ending and the client asking for one-shot
//!   behavior via `?oneshot=true`
//! - a fatal error from the database (e.g. session deleted)

use std::{convert::Infallible, pin::Pin, sync::Arc, time::Duration};

use axum::{
    extract::{Path, Query, State},
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Response, Sse,
    },
};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::api::AppState;
use crate::bus::BusEvent;
use crate::db::Message;

/// A single SSE item with publicly readable fields so tests can
/// inspect name + data without axum's private `Event` internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEvent {
    pub name: String,
    pub data: String,
}

impl From<StreamEvent> for Event {
    fn from(se: StreamEvent) -> Self {
        Event::default().event(&se.name).data(se.data)
    }
}

type FusedStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, Infallible>> + Send>>;

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

/// Provides the DB rows a fused stream needs: the initial
/// catch-up snapshot and lag-recovery backfills. Abstracted so
/// the stream logic is testable without a database.
#[async_trait::async_trait]
pub trait CatchUpStore: Send + Sync {
    /// Every row for `session_id` with `sequence > after_seq`,
    /// ascending.
    async fn rows_after(
        &self,
        session_id: Uuid,
        after_seq: i32,
    ) -> Result<Vec<Message>, sqlx::Error>;
}

/// [`CatchUpStore`] backed by the app's Postgres pool.
pub struct DbCatchUpStore {
    pool: PgPool,
}

#[async_trait::async_trait]
impl CatchUpStore for DbCatchUpStore {
    async fn rows_after(
        &self,
        session_id: Uuid,
        after_seq: i32,
    ) -> Result<Vec<Message>, sqlx::Error> {
        sqlx::query_as::<_, Message>(
            "SELECT * FROM messages WHERE session_id = $1 AND sequence > $2 ORDER BY sequence ASC",
        )
        .bind(session_id)
        .bind(after_seq)
        .fetch_all(&self.pool)
        .await
    }
}

/// Serialize `v` to a JSON string; fall back to an error object
/// if serialization fails (should never happen for known types).
fn serialize(v: &impl serde::Serialize) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string())
}

/// Build the fused event stream for a session: catch-up rows
/// first, then live bus events, from a *single* producer task so
/// the client can never see a live event overtake a catch-up row.
///
/// `catchup_rows` must be every row with `sequence > since`,
/// ascending. The bus receiver is subscribed *before* the
/// catch-up query in the handler, so any row committed in that
/// window is buffered on the bus; it's delivered here exactly
/// once, after the catch-up rows, because every bus event with
/// `sequence <= last delivered` is skipped.
///
/// On broadcast lag the task re-queries [`store`] for rows after
/// the last delivered sequence, emits them, then a `lagged` event
/// reporting how many rows the re-query recovered.
///
/// The producer task exits when the client disconnects (the mpsc
/// send fails) or, with `oneshot`, after delivering this
/// session's `turn_ended`.
pub fn build_event_stream(
    catchup_rows: Vec<Message>,
    rx: broadcast::Receiver<BusEvent>,
    session_id: Uuid,
    since: i32,
    oneshot: bool,
    store: Arc<dyn CatchUpStore>,
) -> FusedStream {
    let (tx, rx_stream) = tokio::sync::mpsc::channel::<StreamEvent>(64);

    tokio::spawn(async move {
        let mut last_seq = since;

        // 1. Catch-up snapshot: every row with sequence > since,
        //    in order. `last_seq` tracks the highest sequence the
        //    client has been given, across both phases.
        for row in catchup_rows {
            last_seq = row.sequence;
            let item = StreamEvent {
                name: "message".into(),
                data: serialize(&row),
            };
            if tx.send(item).await.is_err() {
                return; // client disconnected mid-replay
            }
        }

        // 2. Live bus events, deduped against everything already
        //    delivered (catch-up rows and earlier live events).
        let mut stream = BroadcastStream::new(rx);
        while let Some(item) = stream.next().await {
            match item {
                Ok(evt) => match evt {
                    BusEvent::Message { message } => {
                        if message.session_id != session_id {
                            continue;
                        }
                        // Dedupe: the catch-up phase (or a lag
                        // backfill) already sent anything up to
                        // `last_seq`.
                        if message.sequence <= last_seq {
                            continue;
                        }
                        last_seq = message.sequence;
                        let item = StreamEvent {
                            name: "message".into(),
                            data: serialize(&message),
                        };
                        if tx.send(item).await.is_err() {
                            return; // client disconnected
                        }
                    }
                    BusEvent::TurnEnded { session_id: sid } => {
                        if sid != session_id {
                            continue;
                        }
                        let item = StreamEvent {
                            name: "turn_ended".into(),
                            data: serialize(&serde_json::json!({ "session_id": sid })),
                        };
                        if tx.send(item).await.is_err() {
                            return;
                        }
                        if oneshot {
                            return; // drop tx to close the stream
                        }
                    }
                },
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    // This receiver fell behind the bounded bus
                    // buffer by `n` events. The DB is the source
                    // of truth: re-query every row after the last
                    // delivered sequence and backfill it.
                    let recovered = match store.rows_after(session_id, last_seq).await {
                        Ok(rows) => rows,
                        Err(e) => {
                            tracing::error!(
                                session_id = %session_id,
                                error = %e,
                                "SSE lag recovery: DB re-query failed; closing stream"
                            );
                            return;
                        }
                    };
                    let recovered_count = recovered.len();
                    for row in recovered {
                        last_seq = row.sequence;
                        let item = StreamEvent {
                            name: "message".into(),
                            data: serialize(&row),
                        };
                        if tx.send(item).await.is_err() {
                            return;
                        }
                    }
                    // Tell the client the stream was lossy and how
                    // many rows the re-query actually recovered.
                    let item = StreamEvent {
                        name: "lagged".into(),
                        data: serialize(&serde_json::json!({
                            "missed": n,
                            "recovered": recovered_count,
                        })),
                    };
                    if tx.send(item).await.is_err() {
                        return;
                    }
                    continue;
                }
            }
        }
    });

    Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx_stream).map(Ok))
}

/// Build the SSE response for a session.
///
/// Subscribe to the bus *first*, then run the catch-up query, so
/// rows committed in between are buffered on the bus and picked
/// up by the fused stream (deduped by sequence) instead of being
/// lost on this connection.
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

    // Subscribe BEFORE the catch-up query: a broadcast receiver
    // starts at the sender's current position, so anything
    // published after this point is buffered for us even if it
    // was committed before our catch-up snapshot.
    let rx = state.bus.subscribe();

    // Catch-up: replay every message row with sequence > since.
    // We do this synchronously (clients are happy to wait a few
    // hundred ms for the initial replay) and hand the rows to the
    // fused stream below.
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

    let oneshot = q.oneshot;
    let fused = build_event_stream(
        catchup_rows,
        rx,
        session_id,
        since,
        oneshot,
        Arc::new(DbCatchUpStore {
            pool: state.db.clone(),
        }),
    );

    // Convert the testable StreamEvent to axum's wire Event.
    let stream: Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> =
        Box::pin(fused.map(|item| item.map(Into::into)));

    let response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("hb"),
        )
        .into_response();

    // Disable buffering in nginx-style proxies (which would
    // otherwise wait for the full response before forwarding
    // any bytes to the client), and hint clients to reconnect
    // on close.
    let mut response = response;
    let headers = response.headers_mut();
    headers.insert(
        "X-Accel-Buffering",
        axum::http::HeaderValue::from_static("no"),
    );
    headers.insert(
        "Cache-Control",
        axum::http::HeaderValue::from_static("no-cache"),
    );

    response
}
