//! Harness-backed session support (Herd H2.0, part 2).
//!
//! The Node harness (`~/src/forge/harness/`) owns durable conversations
//! and turns on pi-durable; forge-api drives it over the unix-socket IPC
//! client in `forge-harness-client`. This module is forge-api's side of
//! that:
//!
//! * [`HarnessState`] — the per-process harness handle (client + the
//!   conversation→active-task table learned from the event stream +
//!   the `durable_*` schema address).
//! * [`attach_harness_conversation`] — the H2.1 session-creation
//!   attach: when the `FORGE_HARNESS_MESSAGES` flag is on and the
//!   harness is enabled, a freshly created session gets a durable
//!   conversation and its id is stamped in
//!   `sessions.durable_conversation_id`. Any harness failure keeps
//!   the session legacy (never fails creation).
//! * [`spawn_event_consumer`] — the harness-event consumer task: it
//!   maps harness events onto **the same bus events / in-flight marks
//!   the legacy turn driver produces** so existing SSE consumers
//!   (ranch's forge worker, the web UI) see byte-identical behavior.
//!   On `TurnEnd` (H2.1) it projects the durable `pi.assistant`
//!   entry onto the `messages` table via
//!   [`crate::api::insert_and_publish_assistant`] — deduplicated
//!   through the `durable_projection` table (migration 018).
//!
//! ## Event-name contract (harness event → forge action)
//!
//! | harness event (`harness/src/events.ts`) | forge action |
//! | --- | --- |
//! | `hello` | log only (the client already emitted `ResyncRequired`) |
//! | `task_state { status: "started" }` | remember conversation→task; `registry.begin_turn(session)` (keeps `GET /agents/:id/active` + idle-cleanup correct) |
//! | `task_state { status: "done" \| "failed" \| "aborted" }` | forget conversation→task; `registry.end_turn(session)`; bus `turn_ended` (always, even on error — same as `turn.rs`) |
//! | `turn_end` | **assistant projection (H2.1)**: claim the entry in `durable_projection`, read the `pi.assistant` entry's answer text out of the `durable_*` schema, write one assistant row via `insert_and_publish_assistant` (bus `message` event), then the fire-and-forget summary refresh. Failed/aborted turns project nothing — `turn_ended` above is the whole signal. |
//! | `subagent_spawned` (H2.2) | mint the child's session row (id = the harness-pre-minted forge session UUID, `parent_session_id` = the parent session, `durable_conversation_id` stamped; idempotent through `ON CONFLICT (id) DO NOTHING`), then bus `subagent_started` on the PARENT's stream |
//! | `task_state` terminal on a subagent conversation (H2.2) | when no live task remains in the child conversation, bus `subagent_ended` (parent derived from `sessions.parent_session_id`) on the PARENT's stream |
//! | `document_changed` (H2.5) | bus `document_changed` on this session's stream (the doc row is the source of truth) |
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

use forge_harness_client::{
    CreateConversation, HarnessClient, HarnessEvent, Limits, TaskState as HarnessTaskState,
};

use crate::agent_registry::AgentRegistry;
use crate::api::{insert_and_publish_assistant, AppState};
use crate::bus::MessageBus;
use crate::db::{Profile, Session};

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
    /// Which Postgres schema the `durable_*` tables live in (read-only
    /// side: the assistant projection queries them directly). Same
    /// value the harness process pins via `FORGE_HARNESS_SCHEMA` in
    /// `harness/src/main.ts` (default `public`).
    durable_schema: String,
}

/// The `durable_*` schema from the environment: the same
/// `FORGE_HARNESS_SCHEMA` env var the harness process reads
/// (`harness/src/main.ts` `readConfig` → `PgStorage.open({schema})`
/// pins `search_path` to it). Invalid identifiers fall back to
/// `public` rather than being interpolated into a query.
pub fn durable_schema_from_env() -> String {
    std::env::var("FORGE_HARNESS_SCHEMA")
        .ok()
        .filter(|s| !s.is_empty() && is_sql_identifier(s))
        .unwrap_or_else(|| "public".to_string())
}

fn is_sql_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.chars()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphanumeric() || c == '_' && i != 0)
}

/// The H2.1 turn-routing flag: `FORGE_HARNESS_MESSAGES=1`. Read once
/// per process at [`AppState`] construction (never per request — the
/// env is operator config, not a live switch): on, NEW sessions are
/// attached to durable harness conversations at creation, and
/// `POST /messages` on stamped sessions routes through
/// `harness.submit` instead of the legacy `drive_turn`. Default off:
/// zero behavior change anywhere.
pub fn harness_messages_enabled() -> bool {
    std::env::var("FORGE_HARNESS_MESSAGES").is_ok_and(|v| v == "1")
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
            durable_schema: durable_schema_from_env(),
        }
    }

    /// Disabled state (tests, or when no harness is running).
    pub fn disabled() -> Self {
        Self {
            client: Arc::new(HarnessClient::disabled()),
            active_tasks: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            durable_schema: "public".to_string(),
        }
    }

    /// Enabled client dialing explicit socket paths (tests: the
    /// integration test dials the real child harness in a tempdir
    /// with short limits and a scratch `durable_*` schema). Skips the
    /// socket-existence gate like [`HarnessClient::from_paths_with`].
    pub fn from_paths_with(
        rpc: &std::path::Path,
        events: &std::path::Path,
        limits: Limits,
        durable_schema: impl Into<String>,
    ) -> Self {
        Self {
            client: Arc::new(HarnessClient::from_paths_with(rpc, events, limits)),
            active_tasks: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            durable_schema: durable_schema.into(),
        }
    }

    pub fn client(&self) -> &HarnessClient {
        &self.client
    }

    pub fn is_enabled(&self) -> bool {
        self.client.is_enabled()
    }

    /// The schema the assistant-projection queries address the
    /// `durable_*` tables in.
    pub fn durable_schema(&self) -> &str {
        &self.durable_schema
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
/// (in-flight marks + bus events) and, on `TurnEnd`, the H2.1
/// assistant projection — see the module-level contract table.
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
                    // Herd H2.2: a subagent's conversation just lost a
                    // task; when none remain, notify the PARENT's stream.
                    let status_str = match status {
                        HarnessTaskState::Done => "done",
                        HarnessTaskState::Failed => "failed",
                        _ => "aborted",
                    };
                    publish_subagent_ended_if_settled(state, conversation_id, status_str).await;
                }
            }
        }
        TurnEnd {
            conversation_id,
            entry_id,
            summary: _,
        } => {
            // H2.1: project the committed `pi.assistant` entry onto the
            // `messages` table (one assistant row + bus `message`
            // event). Deduplicated through `durable_projection`
            // (migration 018); empty text (tool-only turns) projects
            // no row.
            project_turn_end(state, conversation_id, entry_id).await;
        }
        DocumentChanged {
            conversation_id,
            name,
        } => {
            // Herd H2.5: a conversation document (plan/handoff/config/…)
            // changed; notify this session's SSE stream. The document
            // row in the harness schema is the source of truth, so a
            // missed event is recoverable (a client re-GETs the doc).
            match session_for_conversation(&state.db, conversation_id).await {
                Some(sid) => bus.publish_document_changed(sid, name),
                None => tracing::debug!(
                    conversation_id,
                    %name,
                    "harness document changed for a conversation with no session row"
                ),
            }
        }
        SubagentSpawned {
            parent_conversation_id,
            child_conversation_id,
            child_forge_session_id,
            task,
            detached,
        } => {
            handle_subagent_spawned(
                state,
                parent_conversation_id,
                child_conversation_id,
                child_forge_session_id,
                task,
                detached,
            )
            .await;
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

// ============================================
// H2.2: subagents (spawn_subagent exposure)
// ============================================

/// Handle the harness's `subagent_spawned` event: mint the child's
/// session row under the parent and notify the parent's SSE stream.
///
/// Idempotent: the harness re-fires the event when the replayed
/// `spawn_subagent` tool call re-runs after a harness restart, and the
/// child's `forge.meta` carries the same pre-minted UUID. `ON
/// CONFLICT (id) DO NOTHING` keeps exactly one row; the stamps
/// (`durable_conversation_id` / `parent_session_id`) are set
/// unconditionally afterwards, so a row minted by an earlier attempt
/// still converges to the same state.
async fn handle_subagent_spawned(
    state: &AppState,
    parent_conversation_id: i64,
    child_conversation_id: i64,
    child_forge_session_id: String,
    task: String,
    detached: bool,
) {
    let parent_sid = match session_for_conversation(&state.db, parent_conversation_id).await {
        Some(s) => s,
        None => {
            tracing::warn!(
                parent_conversation_id,
                child_conversation_id,
                "subagent_spawned: parent conversation has no session row; child stays unlinked"
            );
            return;
        }
    };
    let child_sid = match uuid::Uuid::parse_str(&child_forge_session_id) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!(
                child_forge_session_id = %child_forge_session_id,
                error = %e,
                "subagent_spawned: pre-minted session id is not a UUID; child row not minted"
            );
            return;
        }
    };

    // The child inherits the parent's tenancy, profile, and working
    // dir (the harness child conversation was created in the parent's
    // cwd).
    let parent_row: Option<(Uuid, Option<Uuid>, Option<String>)> =
        match sqlx::query_as("SELECT profile_id, user_id, working_dir FROM sessions WHERE id = $1")
            .bind(parent_sid)
            .fetch_optional(&state.db)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    parent_session_id = %parent_sid,
                    error = %e,
                    "subagent_spawned: failed to read the parent session row"
                );
                return;
            }
        };
    let (profile_id, user_id, working_dir) = match parent_row {
        Some(r) => r,
        None => {
            tracing::error!(
                parent_session_id = %parent_sid,
                "subagent_spawned: parent session row vanished; child row not minted"
            );
            return;
        }
    };

    let title = subagent_title(&task);
    let inserted = sqlx::query(
        r"INSERT INTO sessions
               (id, profile_id, title, user_id, working_dir, durable_conversation_id, parent_session_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO NOTHING",
    )
    .bind(child_sid)
    .bind(profile_id)
    .bind(&title)
    .bind(user_id)
    .bind(&working_dir)
    .bind(child_conversation_id)
    .bind(parent_sid)
    .execute(&state.db)
    .await;
    match inserted {
        Ok(r) if r.rows_affected() > 0 => {
            tracing::info!(
                parent_session_id = %parent_sid,
                child_session_id = %child_sid,
                child_conversation_id,
                detached,
                "subagent session row minted (H2.2)"
            );
        }
        Ok(_) => {
            tracing::debug!(
                child_session_id = %child_sid,
                "subagent session row already exists (replayed spawn event); converging stamps"
            );
        }
        Err(e) => {
            tracing::error!(
                child_session_id = %child_sid,
                error = %e,
                "subagent session row insert failed"
            );
            return;
        }
    }
    // Convergence for the pre-existing-row path: stamp the durable
    // conversation + parent link unconditionally (same values every
    // replay, so the UPDATE is a no-op when already set).
    if let Err(e) = sqlx::query(
        r"UPDATE sessions
              SET durable_conversation_id = $1,
                  parent_session_id = $2
            WHERE id = $3",
    )
    .bind(child_conversation_id)
    .bind(parent_sid)
    .bind(child_sid)
    .execute(&state.db)
    .await
    {
        tracing::error!(
            child_session_id = %child_sid,
            error = %e,
            "subagent session stamp update failed"
        );
        return;
    }
    state
        .bus
        .publish_subagent_started(parent_sid, child_sid, task, detached);
}

/// The child session's title: `Subagent: <task>` truncated to 80
/// chars (titles are user-visible list rows).
fn subagent_title(task: &str) -> String {
    const LIMIT: usize = 80;
    if task.len() <= LIMIT {
        format!("Subagent: {task}")
    } else {
        let mut end = LIMIT;
        while !task.is_char_boundary(end) {
            end -= 1;
        }
        format!("Subagent: {}…", &task[..end])
    }
}

/// Herd H2.2: when a subagent's durable conversation just lost a task
/// and no live task remains in it, publish `subagent_ended` on the
/// PARENT session's stream.
///
/// The "no live task remains" check (rather than "this task was the
/// only one") is what makes this exactly-once-ish: nested tool tasks
/// of the child's turn go terminal BEFORE the child's own turn task,
/// and the conversation only settles when the last one does. The
/// check runs against the `durable_*` schema after the terminal
/// commit, so the row state is stable.
async fn publish_subagent_ended_if_settled(state: &AppState, conversation_id: i64, status: &str) {
    let link: Option<(Uuid, Option<Uuid>)> = match sqlx::query_as(
        "SELECT id, parent_session_id FROM sessions WHERE durable_conversation_id = $1",
    )
    .bind(conversation_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(l) => l,
        Err(e) => {
            tracing::debug!(conversation_id, error = %e, "subagent link lookup failed; skipping subagent_ended");
            return;
        }
    };
    let Some((child_sid, parent_sid)) = link else {
        return; // not a subagent conversation (or row absent)
    };
    let Some(parent_sid) = parent_sid else { return };
    let schema = state.harness.durable_schema();
    let live: Option<i64> = match sqlx::query_scalar(&format!(
        r"SELECT 1 FROM {schema}.durable_tasks WHERE conversation_id = $1 AND status <> 'terminal' LIMIT 1"
    ))
    .bind(conversation_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                conversation_id,
                schema = %schema,
                error = %e,
                "subagent_ended: live-task check failed; not publishing"
            );
            return;
        }
    };
    if live.is_some() {
        return; // the child's turn (or one of its tool tasks) is still running
    }
    tracing::info!(
        child_session_id = %child_sid,
        parent_session_id = %parent_sid,
        conversation_id,
        %status,
        "subagent settled"
    );
    state
        .bus
        .publish_subagent_ended(parent_sid, child_sid, status.to_string());
}

// ============================================
// H2.1: session creation attach
// ============================================

/// Attach a durable harness conversation to a freshly created session
/// (the H2.1 cutover point). Called from `POST /sessions` and
/// `POST /agents/:id/conversations` after the session row + working
/// dir exist.
///
/// Rules:
/// * **Flag off** (`FORGE_HARNESS_MESSAGES` unset) → `None`; the
///   session is legacy with zero harness contact.
/// * **Harness disabled** (no socket at startup) → `None`, same.
/// * **Any harness/DB failure** → `None` + a warn/error log: session
///   creation must never fail because of the harness. An orphaned
///   harness conversation (created but unstamped) is harmless — it
///   just carries a `forge.meta` document.
///
/// Model resolution mirrors the legacy spawn path
/// (`agent_registry.rs`): session override, then the profile's value.
/// `systemPrompt` is the profile's `system_prompt` (the session's
/// working dir / tools still come from the profile + session row, the
/// way the forge tool extension finds them through
/// `POST /tools/execute`).
pub async fn attach_harness_conversation(
    state: &AppState,
    session: &Session,
    profile: &Profile,
) -> Option<i64> {
    if !state.harness_messages {
        return None;
    }
    if !state.harness.is_enabled() {
        tracing::debug!(
            session_id = %session.id,
            "harness mode disabled; new session stays legacy"
        );
        return None;
    }
    let provider = session
        .override_provider
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| profile.provider.clone());
    let model_id = session
        .override_model
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| profile.model.clone());
    let system_prompt = if profile.system_prompt.trim().is_empty() {
        None
    } else {
        Some(profile.system_prompt.clone())
    };

    // Herd H2.5: when the session belongs to an agent, its
    // `tools_allowlist` (H1.1) is enforced by the harness's `before_tool`
    // hook, and its `extra_instructions` are appended to the prompt. A
    // lookup failure keeps the session attachable (allow-all), never
    // fails creation.
    let (tools_allowlist, extra_instructions) = match session.agent_id {
        Some(agent_id) => read_agent_tooling(&state.db, agent_id)
            .await
            .unwrap_or_else(|| (Vec::new(), None)),
        None => (Vec::new(), None),
    };

    let params = CreateConversation {
        forge_session_id: session.id.to_string(),
        provider: provider.clone(),
        model_id: model_id.clone(),
        system_prompt: system_prompt.clone(),
        extra_instructions: extra_instructions.clone(),
        tools_allowlist: tools_allowlist.clone(),
        ..Default::default()
    };
    let conversation_id = match state.harness.client().create_conversation(&params).await {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(
                session_id = %session.id,
                provider = %provider,
                model = %model_id,
                error = %e,
                "harness createConversation failed; session falls back to the legacy turn path"
            );
            return None;
        }
    };
    match sqlx::query("UPDATE sessions SET durable_conversation_id = $1 WHERE id = $2")
        .bind(conversation_id)
        .bind(session.id)
        .execute(&state.db)
        .await
    {
        Ok(_) => {
            tracing::info!(
                session_id = %session.id,
                conversation_id,
                provider = %provider,
                model = %model_id,
                "session attached to a durable harness conversation (H2.1)"
            );
            Some(conversation_id)
        }
        Err(e) => {
            tracing::error!(
                session_id = %session.id,
                conversation_id,
                error = %e,
                "failed to stamp durable_conversation_id; session stays legacy (harness conversation is orphaned)"
            );
            None
        }
    }
}

/// Read an agent's harness tooling: its `tools_allowlist` (JSON array
/// of tool names; non-string members are dropped) and its
/// `extra_instructions`. `None` when the agent row is absent or the
/// lookup fails — the caller then attaches with allow-all.
async fn read_agent_tooling(db: &PgPool, agent_id: Uuid) -> Option<(Vec<String>, Option<String>)> {
    let row: Option<(serde_json::Value, Option<String>)> =
        sqlx::query_as("SELECT tools_allowlist, extra_instructions FROM agents WHERE id = $1")
            .bind(agent_id)
            .fetch_optional(db)
            .await
            .ok()?;
    let (allowlist, extra) = row?;
    let tools = allowlist
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();
    Some((tools, extra))
}

// ============================================
// H2.1: assistant projection (turn_end → messages)
// ============================================

/// Project one committed `pi.assistant` entry onto the `messages`
/// table: one assistant row + a bus `message` event, exactly the way
/// the legacy turn driver writes chunks
/// ([`crate::api::insert_and_publish_assistant`]).
///
/// Dedup: `durable_projection` (migration 018) claims
/// (conversation, entry) once, so a re-processed event can never
/// write a second row. The harness has no event replay, so this is
/// belt-and-suspenders — it exists so the claim (not the row's
/// existence) is the single source of truth, even if a consumer
/// restart re-sees an entry mid-commit-batch.
///
/// Empty-text entries (tool-only turns) claim but write nothing.
async fn project_turn_end(state: &AppState, conversation_id: i64, entry_id: i64) {
    let session = match session_for_conversation(&state.db, conversation_id).await {
        Some(s) => s,
        None => {
            tracing::warn!(
                conversation_id,
                entry_id,
                "turn_end for a conversation with no matching session; skipping projection"
            );
            return;
        }
    };

    let claimed = sqlx::query(
        r"INSERT INTO durable_projection (conversation_id, entry_id, session_id)
           VALUES ($1, $2, $3)
           ON CONFLICT (conversation_id, entry_id) DO NOTHING",
    )
    .bind(conversation_id)
    .bind(entry_id)
    .bind(session)
    .execute(&state.db)
    .await
    .is_ok_and(|r| r.rows_affected() > 0);
    if !claimed {
        tracing::debug!(
            conversation_id,
            entry_id,
            "turn_end already projected (durable_projection claim); skipping"
        );
        return;
    }

    let schema = state.harness.durable_schema().to_string();
    let record: Result<String, sqlx::Error> = sqlx::query_scalar(&format!(
        r#"SELECT record FROM "{schema}".durable_entries WHERE id = $1 AND conversation_id = $2"#
    ))
    .bind(entry_id)
    .bind(conversation_id)
    .fetch_optional(&state.db)
    .await
    .and_then(|r| r.ok_or(sqlx::Error::RowNotFound));
    let text = match record {
        Ok(record) => extract_assistant_text(&record),
        Err(e) => {
            tracing::error!(
                conversation_id,
                entry_id,
                schema = %schema,
                error = %e,
                "turn_end: failed to read the durable assistant entry"
            );
            return;
        }
    };
    if text.is_empty() {
        tracing::debug!(
            conversation_id,
            entry_id,
            "turn_end entry has no text content (tool-only turn); no assistant row"
        );
        return;
    }

    match insert_and_publish_assistant(&state.db, &state.bus, session, &text).await {
        Some(row) => tracing::info!(
            session_id = %session,
            conversation_id,
            entry_id,
            sequence = row.sequence,
            text_len = text.len(),
            "harness assistant turn projected to messages"
        ),
        None => tracing::error!(
            session_id = %session,
            conversation_id,
            entry_id,
            "harness assistant turn projection failed to insert"
        ),
    }

    // Mirror the legacy post-turn refresh so the semantic router's
    // session summary stays current (fire-and-forget, same as the
    // legacy dispatch path).
    let pool = state.db.clone();
    let models_path = state.models_path.clone();
    let embedding_config = state.embedding_config.clone();
    tokio::spawn(async move {
        crate::api::routing::refresh_session_summary(
            &pool,
            &models_path,
            &embedding_config,
            session,
        )
        .await;
    });
}

/// Extract the answer text from a durable `pi.assistant` entry record
/// (JSON text; shape per `vendor/pi-durable/packages/durable/src/
/// entries.ts`: `model` = `[AssistantMessage]`, each with a `content`
/// array of content blocks). Concatenates every `text` block of
/// every assistant message in the entry with newlines — the same
/// blocks the harness's `turn_end` summary is built from
/// (`harness/src/events.ts` `assistantSummary`).
pub(crate) fn extract_assistant_text(record: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(record) else {
        tracing::warn!("durable entry record is not JSON; projecting empty text");
        return String::new();
    };
    let mut texts: Vec<&str> = Vec::new();
    if let Some(messages) = value.get("model").and_then(|m| m.as_array()) {
        for message in messages {
            if message.get("role").and_then(|r| r.as_str()) != Some("assistant") {
                continue;
            }
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

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_assistant_text_collects_text_blocks() {
        let record = serde_json::json!({
            "id": 7,
            "kind": "pi.assistant",
            "conversationId": 3,
            "model": [
                {
                    "role": "assistant",
                    "content": [
                        { "type": "text", "text": "Hello " },
                        { "type": "thinking", "thinking": "hmm" },
                        { "type": "text", "text": "world" }
                    ]
                },
                { "role": "toolResult", "content": [{ "type": "text", "text": "should not leak" }] }
            ]
        })
        .to_string();
        assert_eq!(extract_assistant_text(&record), "Hello \nworld");
    }

    #[test]
    fn extract_assistant_text_handles_garbage() {
        assert_eq!(extract_assistant_text("not json"), "");
        assert_eq!(extract_assistant_text("{}"), "");
        assert_eq!(
            extract_assistant_text(r#"{"model": [ {"role": "assistant", "content": []} ]}"#),
            ""
        );
        // A toolCall block carries no text: tool-only turns project
        // nothing.
        assert_eq!(
            extract_assistant_text(
                r#"{"model": [ {"role": "assistant", "content": [{"type": "toolCall", "name": "bash"} ]} ]}"#
            ),
            ""
        );
    }

    #[test]
    fn durable_schema_identifier_validation() {
        assert!(is_sql_identifier("public"));
        assert!(is_sql_identifier("harness_test_abc123"));
        assert!(!is_sql_identifier(""));
        assert!(!is_sql_identifier("bad name"));
        assert!(!is_sql_identifier("x; DROP"));
        assert!(!is_sql_identifier(&"a".repeat(64)));
    }
}
