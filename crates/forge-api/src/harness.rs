//! Harness-backed session support (Herd H2.0, part 2).
//!
//! The Node harness (`~/src/forge/harness/`) owns durable conversations
//! and turns on pi-durable; forge-api drives it over the unix-socket IPC
//! client in `forge-harness-client`. This module is forge-api's side of
//! that:
//!
//! * [`HarnessState`] — the per-process harness handle (client + the
//!   conversation→active-task table learned from the event stream).
//! * [`spawn_event_consumer`] — the harness-event consumer task: it
//!   maps harness events onto **the same bus events / in-flight marks
//!   the legacy turn driver produces** so existing SSE consumers
//!   (ranch's forge worker, the web UI) see byte-identical behavior.
//!
//! ## Event-name contract (harness event → forge action)
//!
//! | harness event (`harness/src/events.ts`) | forge action |
//! | --- | --- |
//! | `hello` | log only (the client already emitted `ResyncRequired`) |
//! | `task_state { status: "started" }` | remember conversation→task; `registry.begin_turn(session)` (keeps `GET /agents/:id/active` + idle-cleanup correct) |
//! | `task_state { status: "done" \| "failed" \| "aborted" }` | forget conversation→task; `registry.end_turn(session)`; bus `turn_ended` (always, even on error — same as `turn.rs`) |
//! | `turn_end` | log only for now — messages-table projection from harness transcripts is H2.1's job |
//! | `document_changed` | log only |
//! | `timer_fired` | log only (the fired turn surfaces as `task_state` / `turn_end`) |
//! | `ResyncRequired` (client-side marker) | re-query harness `status`, keep learned marks (no task-listing IPC yet — see H2.2), log |
//!
//! ## Disabled mode
//!
//! When `FORGE_HARNESS_SOCKET` is unset or the socket is absent at
//! startup, [`HarnessState::from_env`] yields a disabled state: no
//! sockets dialed, no consumer spawned, every harness-backed call
//! fails with `HarnessError::Unavailable`, and the legacy `drive_turn`
//! path remains the default with zero behavior change.
use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use forge_harness_client::{HarnessClient, HarnessEvent, TaskState as HarnessTaskState};

use crate::agent_registry::AgentRegistry;
use crate::api::AppState;
use crate::bus::MessageBus;

/// forge-api's harness handle: the IPC/event client plus the
/// conversation→active-task table (learned from `task_state` events).
///
/// The table is in-memory: it is the interrupt fast-path, and it
/// re-derives (best-effort) from events after a resync. It is NOT
/// persisted — the harness's Postgres is the source of truth.
#[derive(Clone)]
pub struct HarnessState {
    client: Arc<HarnessClient>,
    active_tasks: std::sync::Arc<tokio::sync::RwLock<HashMap<i64, i64>>>,
}

impl HarnessState {
    /// Production constructor: enabled only when
    /// `FORGE_HARNESS_SOCKET` is set **and** the socket file exists
    /// (see `HarnessClient::from_env`).
    pub fn from_env() -> Self {
        let client = HarnessClient::from_env();
        if client.is_enabled() {
            tracing::info!("harness mode enabled; wiring the event consumer");
        } else {
            tracing::warn!(
                "harness mode disabled: FORGE_HARNESS_SOCKET unset or socket absent; legacy drive_turn path is the default"
            );
        }
        Self {
            client: Arc::new(client),
            active_tasks: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Disabled state (tests, or when no harness is running).
    pub fn disabled() -> Self {
        Self {
            client: Arc::new(HarnessClient::disabled()),
            active_tasks: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub fn client(&self) -> &HarnessClient {
        &self.client
    }

    pub fn is_enabled(&self) -> bool {
        self.client.is_enabled()
    }

    /// The active harness task for a durable conversation, if one is
    /// known from the event stream (used by interrupt forwarding).
    pub async fn active_task(&self, conversation_id: i64) -> Option<i64> {
        self.active_tasks
            .read()
            .await
            .get(&conversation_id)
            .copied()
    }

    /// Forget the active task for a conversation.
    pub async fn clear_active(&self, conversation_id: i64) {
        self.active_tasks.write().await.remove(&conversation_id);
    }

    /// Remember the active task for a conversation.
    async fn set_active(&self, conversation_id: i64, task_id: i64) {
        self.active_tasks
            .write()
            .await
            .insert(conversation_id, task_id);
    }

    /// The durable conversation id stamped on a session, if any
    /// (migration 017). `None` = legacy session.
    pub async fn conversation_for_session(&self, db: &PgPool, session_id: Uuid) -> Option<i64> {
        sqlx::query_scalar("SELECT durable_conversation_id FROM sessions WHERE id = $1")
            .bind(session_id)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
    }
}

/// Map a durable conversation id to its forge session (migration 017).
async fn session_for_conversation(pool: &PgPool, conversation_id: i64) -> Option<Uuid> {
    sqlx::query_scalar("SELECT id FROM sessions WHERE durable_conversation_id = $1")
        .bind(conversation_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

/// Spawn the harness-event consumer (enabled mode only).
///
/// Returns `None` when the client is disabled or the events receiver
/// was already taken. The consumer runs until the process exits; it
/// must not outlive its `state` clone.
pub fn spawn_event_consumer(state: Arc<AppState>) -> Option<JoinHandle<()>> {
    let rx = state.harness.client().take_event_rx()?;
    Some(tokio::spawn(consume_events(state, rx)))
}

async fn consume_events(state: Arc<AppState>, mut rx: mpsc::Receiver<HarnessEvent>) {
    tracing::info!("harness event consumer started");
    while let Some(event) = rx.recv().await {
        handle_event(&state, event).await;
    }
    tracing::info!("harness event consumer exiting (stream closed)");
}

/// Map one harness event onto the legacy turn driver's side effects
/// (in-flight marks + bus events) — see the module-level contract
/// table.
#[allow(dead_code)]
pub(crate) async fn handle_event(state: &AppState, event: HarnessEvent) {
    use HarnessEvent::*;
    let bus: &MessageBus = &state.bus;
    let registry: &AgentRegistry = &state.agent_registry;

    match event {
        Hello { version } => {
            tracing::info!("harness hello: version {version}");
        }
        ResyncRequired => {
            // No replay on reconnect: re-derive what we can. The
            // harness IPC has no task-listing method yet (H2.2), so
            // learned marks are kept and re-learned from subsequent
            // task_state events; `status` is logged for operators.
            match state.harness.client().status().await {
                Ok(s) => tracing::info!(
                    "harness resync: version {} active_tasks {} conversations {} timers {}",
                    s.version,
                    s.active_tasks,
                    s.conversations,
                    s.timers
                ),
                Err(e) => tracing::warn!("harness resync: status failed: {e}"),
            }
        }
        TaskState {
            task_id,
            conversation_id,
            status,
            outcome_status: _,
        } => {
            let session = session_for_conversation(&state.db, conversation_id).await;
            match status {
                HarnessTaskState::Started => {
                    state.harness.set_active(conversation_id, task_id).await;
                    if let Some(sid) = session {
                        registry.begin_turn(sid);
                        tracing::info!(
                            session_id = %sid,
                            task_id,
                            "harness task started (in-flight mark set)"
                        );
                    }
                }
                HarnessTaskState::Done | HarnessTaskState::Failed | HarnessTaskState::Aborted => {
                    state.harness.clear_active(conversation_id).await;
                    if let Some(sid) = session {
                        registry.end_turn(sid);
                        bus.publish_turn_ended(sid);
                        tracing::info!(
                            session_id = %sid,
                            task_id,
                            ?status,
                            "harness task terminal (in-flight mark cleared, turn_ended published)"
                        );
                    }
                }
            }
        }
        TurnEnd {
            conversation_id,
            entry_id,
            summary,
        } => {
            // The messages-table projection of harness transcripts is
            // H2.1; until then this is a log line (the turn_ended bus
            // event above is what SSE consumers key off).
            tracing::debug!(
                conversation_id,
                entry_id,
                summary_len = summary.len(),
                "harness turn_end (messages projection is H2.1)"
            );
        }
        DocumentChanged {
            conversation_id,
            name,
        } => {
            tracing::debug!(conversation_id, %name, "harness document changed");
        }
        TimerFired {
            timer_id,
            conversation_id,
            prompt,
        } => {
            tracing::info!(
                timer_id,
                conversation_id,
                prompt_len = prompt.len(),
                "harness timer fired"
            );
        }
    }
}
