//! Herd H5.1: proactive research — read-only BY CONSTRUCTION.
//!
//! A research task is a durable harness conversation whose per-task
//! tool registry is a FILTERED surface: `{read, webfetch, search,
//! note}`. The write-class tools (`bash`/`write`/`edit`), the
//! subagent tool, and the memory tools are not OFFERED at all — the
//! harness receives the tool list per task (the extension instance is
//! per conversation), so the model never sees a write tool it could
//! call. The H2.5 `tools_allowlist` hook is defense in depth, not the
//! mechanism: a blocking hook still means the tool was offered.
//!
//! Lifecycle (the `agent_research` table, migration 025):
//!
//!   pending   → `POST /agents/:id/research` accepted, task not started
//!   running   → a `task_state started` event was seen (event consumer)
//!   done      → the task settled; the `research_report` document
//!               landed and the suggestion card was pushed
//!   adopted   → the card answered Use
//!   discarded → the card answered Discard
//!
//! (Ask-more re-injects the follow-up prompt into the research
//! conversation and sends the row back to `running`.)
//!
//! Card flow: when the task settles, the event consumer
//! (`crate::harness::handle_event`) calls
//! [`complete_research_if_settled`], which lands the report document,
//! flips the row to `done`, and pushes a `research_report` ranch-tool
//! card on the agent's most-active conversation — the SAME queue and
//! SSE door as the H4.4 `memory_review` cards. ranchd's forge worker
//! renders it (Use / Discard / Ask more) and answers via
//! `POST /ranch-tools/:id/result`, which resolves to
//! [`apply_research_report`] here.
//!
//! Security: owner-gated like the rest of the agent surface (404, not
//! 403, so a foreign agent's existence is not leaked); restricted
//! (demo) keys may use it, like agent message dispatch.

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::{agents::agent_access_err, db_err, err_resp, AppState};
use crate::api::auth::{can_access, AuthenticatedUser};
use crate::db::{Profile, Session};

/// The `agent_research` row (migration 025).
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AgentResearch {
    pub id: Uuid,
    pub agent_id: Uuid,
    /// The research session's row (`sessions.id`); its
    /// `durable_conversation_id` links to the harness conversation.
    pub conversation_id: Uuid,
    /// The durable task id: learned from the first `task_state
    /// started` event, or backfilled from the terminal task at
    /// settlement when that event raced the session stamp (see
    /// `mark_research_running`).
    pub task_id: Option<i64>,
    pub question: String,
    pub scope: Option<String>,
    /// `pending` | `running` | `done` | `adopted` | `discarded`.
    pub state: String,
    /// The resolution: the card answer (`use`/`discard`/`ask_more: …`)
    /// or the terminal status for a failed task (`failed`/`aborted`).
    pub resolution: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
}

// ============================================
// POST /agents/:id/research
// ============================================

#[derive(Deserialize)]
pub struct CreateResearchRequest {
    pub question: String,
    #[serde(default)]
    pub scope: Option<String>,
}

/// `POST /agents/:id/research` — start a proactive research task for
/// the agent. Creates the research session + a `spawnResearch`
/// harness task (filtered registry, prompt submitted exactly-once)
/// and the `agent_research` lifecycle row. Response:
/// `{research: {id, conversation_id, task_id (null until started),
/// question, scope, state}}`.
pub async fn agent_research(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Json(payload): Json<CreateResearchRequest>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    let question = payload.question.trim();
    if question.is_empty() || question.len() > 4000 {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "question must be 1..4000 chars",
        );
    }
    let scope = payload
        .scope
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    if scope.as_ref().is_some_and(|s| s.len() > 1000) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "scope must be ≤ 1000 chars",
        );
    }

    // The agent row (owner + primary profile).
    let agent_row: Option<(Uuid, Option<Uuid>)> =
        match sqlx::query_as("SELECT owner_id, primary_profile_id FROM agents WHERE id = $1")
            .bind(agent_id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get agent",
                    e,
                )
            }
        };
    let Some((owner_id, primary_profile_id)) = agent_row else {
        return err_resp(&state, StatusCode::NOT_FOUND, "Agent not found");
    };

    // Profile resolution mirrors `POST /agents/:id/conversations`:
    // the agent's primary, else the owner's most recently created.
    let profile: Profile = match primary_profile_id {
        Some(pid) => match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
            .bind(pid)
            .fetch_optional(&state.db)
            .await
        {
            Ok(Some(p)) => p,
            Ok(None) => {
                return err_resp(
                    &state,
                    StatusCode::NOT_FOUND,
                    "Agent's primary profile no longer exists",
                )
            }
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get profile",
                    e,
                )
            }
        },
        None => match sqlx::query_as::<_, Profile>(
            "SELECT * FROM profiles WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(owner_id)
        .fetch_optional(&state.db)
        .await
        {
            Ok(Some(p)) => p,
            Ok(None) => {
                return err_resp(
                    &state,
                    StatusCode::NOT_FOUND,
                    "Agent has no primary profile and the owner has no default profile",
                )
            }
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get profile",
                    e,
                )
            }
        },
    };
    if !can_access(&user, profile.user_id) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found");
    }

    if !state.harness.is_enabled() || !state.harness_messages {
        return err_resp(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            "harness unavailable (disabled); research requires a durable task",
        );
    }

    // The research session (tenancy follows the AGENT — the card and
    // the report surface under the agent's owner regardless of which
    // caller (owner or admin) started the task).
    let title = truncate_chars(&format!("Research: {question}"), 80);
    let session = match sqlx::query_as::<_, Session>(
        "INSERT INTO sessions (profile_id, title, user_id, agent_id) VALUES ($1, $2, $3, $4) RETURNING *",
    )
    .bind(profile.id)
    .bind(&title)
    .bind(owner_id)
    .bind(agent_id)
    .fetch_one(&state.db)
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create research conversation",
                e,
            )
        }
    };

    // The harness task: filtered registry + exactly-once prompt.
    let params = serde_json::json!({
        "forgeSessionId": session.id,
        "question": question,
        "provider": profile.provider,
        "modelId": profile.model,
        "scope": scope,
    });
    let conversation_id = match state.harness.client().spawn_research(&params).await {
        Ok(cid) => cid,
        Err(e) => {
            // No conversation was admitted (or the socket is down): roll
            // back the session row so no dead session lingers.
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            tracing::warn!(
                agent_id = %agent_id,
                session_id = %session.id,
                error = %e,
                "research spawn failed; session row rolled back"
            );
            return err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("harness spawn_research failed: {e}"),
            );
        }
    };
    if let Err(e) = sqlx::query("UPDATE sessions SET durable_conversation_id = $1 WHERE id = $2")
        .bind(conversation_id)
        .bind(session.id)
        .execute(&state.db)
        .await
    {
        let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
            .bind(session.id)
            .execute(&state.db)
            .await;
        tracing::error!(
            session_id = %session.id,
            conversation_id,
            error = %e,
            "research: failed to stamp durable_conversation_id; rolled back session row"
        );
        return err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create research conversation",
        );
    }

    let research = match sqlx::query_as::<_, AgentResearch>(
        "INSERT INTO agent_research (agent_id, conversation_id, question, scope, state) VALUES ($1, $2, $3, $4, 'pending') RETURNING *",
    )
    .bind(agent_id)
    .bind(session.id)
    .bind(question)
    .bind(&scope)
    .fetch_one(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            tracing::error!(agent_id = %agent_id, error = %e, "research: agent_research insert failed; rolled back session row");
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create research task",
                e,
            );
        }
    };

    state.metrics.inc_requests("POST /agents/:id/research");
    tracing::info!(
        agent_id = %agent_id,
        research_id = %research.id,
        session_id = %session.id,
        conversation_id,
        "research task started (pending)"
    );

    // Race-closure: the task may have already settled INSIDE the
    // `spawn_research` RPC (fast turn + the prompt submitted inside
    // the call) — in that case the terminal event was handled before
    // this row existed and settled nothing. Re-run the settle check:
    // it is idempotent (state-guarded) and a no-op while a task is
    // still live in the conversation.
    let schema = state.harness.durable_schema();
    let outcome: Option<String> = sqlx::query_scalar(&format!(
        r"SELECT record::jsonb->'state'->'outcome'->>'status' FROM {schema}.durable_tasks WHERE conversation_id = $1 AND status = 'terminal' ORDER BY id ASC LIMIT 1"
    ))
    .bind(conversation_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();
    if let Some(outcome) = outcome {
        let status = match outcome.as_str() {
            "completed" => "done",
            "failed" => "failed",
            _ => "aborted",
        };
        complete_research_if_settled(&state, conversation_id, status).await;
    }

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "research": research })),
    )
        .into_response()
}

// ============================================
// GET /agents/:id/research?open=1
// ============================================

#[derive(Deserialize)]
pub struct ListResearchQuery {
    /// `open=1` (or any non-zero) filters to unresolved tasks:
    /// state NOT IN ('adopted', 'discarded').
    pub open: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /agents/:id/research` — the agent's research tasks, newest
/// first. `?open=1` returns only the unresolved ones (H5.3's Activity
/// View polls this).
pub async fn list_agent_research(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Query(query): Query<ListResearchQuery>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let rows: Result<Vec<AgentResearch>, sqlx::Error> = if query.open.unwrap_or(0) != 0 {
        sqlx::query_as::<_, AgentResearch>(
            "SELECT * FROM agent_research WHERE agent_id = $1 AND state NOT IN ('adopted', 'discarded') ORDER BY created_at DESC LIMIT $2",
        )
        .bind(agent_id)
        .bind(limit)
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, AgentResearch>(
            "SELECT * FROM agent_research WHERE agent_id = $1 ORDER BY created_at DESC LIMIT $2",
        )
        .bind(agent_id)
        .bind(limit)
        .fetch_all(&state.db)
        .await
    };
    match rows {
        Ok(research) => {
            state.metrics.inc_requests("GET /agents/:id/research");
            Json(serde_json::json!({ "research": research })).into_response()
        }
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list research tasks",
            e,
        ),
    }
}

/// Trim a string to `max` chars (on a char boundary), ellipsis-marked.
fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let cut: String = s.chars().take(max).collect();
        format!("{cut}…")
    }
}

// ============================================
// Completion path (event consumer → done + card)
// ============================================

/// Herd H5.1: mark a research row `running` (+ its task id) when the
/// harness reports the first `task_state started` on the row's
/// conversation. No-op when there is no pending row (ordinary turns
/// and the follow-up turns of a running task keep their state).
///
/// The `started` event can arrive BEFORE the session's
/// `durable_conversation_id` stamp is committed (`spawnResearch`
/// admits the prompt inside the RPC, so the turn task starts before
/// the POST response) — in that window `session_for_conversation`
/// finds nothing and this mark is simply missed. That is not a
/// problem: [`complete_research_if_settled`] backfills the task id
/// from the terminal task, and [`resync_unsettled_research`] re-derives
/// settled-but-unsettled rows after any events-socket reconnect.
pub(crate) async fn mark_research_running(state: &AppState, session_id: Uuid, task_id: i64) {
    let r = sqlx::query(
        "UPDATE agent_research SET state = 'running', task_id = $2 WHERE conversation_id = $1 AND state = 'pending'",
    )
    .bind(session_id)
    .bind(task_id)
    .execute(&state.db)
    .await;
    match &r {
        Ok(row) if row.rows_affected() > 0 => {
            tracing::info!(
                session_id = %session_id,
                task_id,
                "research task running"
            );
        }
        Err(e) => {
            tracing::warn!(session_id = %session_id, error = %e, "research: mark running failed")
        }
        _ => {}
    }
}

/// Herd H5.1: when a research conversation just lost a task and no
/// live task remains in it, settle the pending/running `agent_research`
/// row: land the `research_report` document, flip the row to `done`,
/// and push the suggestion card. Mirrors
/// `crate::harness::publish_subagent_ended_if_settled` (the "no live
/// task remains" check makes it exactly-once-ish: nested tool tasks
/// go terminal BEFORE the turn task; the conversation settles when
/// the last one does).
///
/// On `done`: the report is the newest `pi.assistant` entry of the
/// conversation (the research instructions make the model's FINAL
/// message the report), falling back to the `research_notes`
/// document, then to a placeholder. On `failed`/`aborted`: no report
/// and no card — the row records the terminal status as
/// `resolution` and stays visible via `GET /agents/:id/research`.
pub(crate) async fn complete_research_if_settled(
    state: &AppState,
    conversation_id: i64,
    status: &str,
) {
    let row: Option<AgentResearch> = match sqlx::query_as::<_, AgentResearch>(
        r#"SELECT * FROM agent_research r
                 WHERE r.conversation_id =
                       (SELECT id FROM sessions WHERE durable_conversation_id = $1)
                   AND r.state IN ('pending', 'running')
                 ORDER BY r.created_at DESC LIMIT 1"#,
    )
    .bind(conversation_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(conversation_id, error = %e, "research: row lookup failed");
            return;
        }
    };
    let Some(research) = row else {
        return; // not a research conversation (or already settled)
    };

    // No live task remains? (Same check as the subagent_ended path.)
    let schema = state.harness.durable_schema();
    let live: Option<i64> =
        match sqlx::query_scalar(&format!(
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
                    "research: live-task check failed; not settling"
                );
                return;
            }
        };
    if live.is_some() {
        return; // the turn (or one of its tool tasks) is still running
    }

    // On `done`, compute the summary + land the report document.
    let summary: Option<String> = if status == "done" {
        let report = read_research_report_text(state, conversation_id).await;
        let summary = truncate_chars(&report, 300);
        // Land the report document on the research conversation (the
        // H2.5 document surface every client can GET).
        let doc = serde_json::json!({
            "question": research.question,
            "scope": research.scope,
            "summary": summary,
            "report": report,
        });
        if let Err(e) = state
            .harness
            .client()
            .document_put(conversation_id, "research_report", &doc)
            .await
        {
            tracing::warn!(
                conversation_id,
                research_id = %research.id,
                error = %e,
                "research: research_report document write failed (state still flips)"
            );
        }
        Some(summary)
    } else {
        None
    };

    // The state flip is the COMMIT point (idempotent through the
    // WHERE state guard; a replayed terminal event settles nothing).
    // The task id backfill covers the `started` event that raced the
    // session stamp (see `mark_research_running`): the first terminal
    // task of the conversation IS the turn task the mark would have
    // learned.
    let terminal_task_id: Option<i64> =
        match sqlx::query_scalar(&format!(
            r"SELECT id FROM {schema}.durable_tasks WHERE conversation_id = $1 AND status = 'terminal' ORDER BY id ASC LIMIT 1"
        ))
        .bind(conversation_id)
        .fetch_optional(&state.db)
        .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(conversation_id, error = %e, "research: terminal task lookup failed");
                None
            }
        };
    let resolution = if status == "done" {
        None
    } else {
        Some(status.to_string())
    };
    let r = sqlx::query(
        "UPDATE agent_research SET state = 'done', resolution = $2, task_id = COALESCE(task_id, $3) WHERE id = $1 AND state IN ('pending', 'running')",
    )
    .bind(research.id)
    .bind(&resolution)
    .bind(terminal_task_id)
    .execute(&state.db)
    .await;
    match r {
        Ok(row) if row.rows_affected() > 0 => {
            if let Some(summary) = summary {
                tracing::info!(
                    research_id = %research.id,
                    conversation_id,
                    "research task done (report landed)"
                );
                push_research_report_card(state, &research, &summary).await;
            } else {
                tracing::warn!(
                    research_id = %research.id,
                    conversation_id,
                    %status,
                    "research task ended {status}; no card"
                );
            }
        }
        Ok(_) => tracing::debug!(
            research_id = %research.id,
            "research row already settled (replayed terminal event)"
        ),
        Err(e) => tracing::error!(
            research_id = %research.id,
            error = %e,
            "research: state flip failed"
        ),
    }
}

/// Herd H5.1 (resync): settle research rows whose task has ended
/// while the events socket was down or the `started`/terminal events
/// raced the session stamp. Mirrors `resync_unprojected` (H2.6):
/// after any reconnect, re-derive from Postgres what the lossy event
/// stream may have missed. A row is settled when its conversation has
/// NO live task and AT LEAST one terminal task; the settle path is
/// idempotent (state-guarded), so a row already answered or settled
/// is untouched.
pub(crate) async fn resync_unsettled_research(state: &AppState) {
    let rows: Vec<(Uuid, i64)> = match sqlx::query_as(
        "SELECT ar.id, s.durable_conversation_id FROM agent_research ar JOIN sessions s ON s.id = ar.conversation_id WHERE ar.state IN ('pending', 'running') AND s.durable_conversation_id IS NOT NULL",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "research resync: unsettled-row scan failed");
            return;
        }
    };
    let schema = state.harness.durable_schema();
    for (_research_id, conversation_id) in rows {
        let live: Option<i64> = sqlx::query_scalar(&format!(
            r"SELECT 1 FROM {schema}.durable_tasks WHERE conversation_id = $1 AND status <> 'terminal' LIMIT 1"
        ))
        .bind(conversation_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
        let terminal: Option<i64> = sqlx::query_scalar(&format!(
            r"SELECT 1 FROM {schema}.durable_tasks WHERE conversation_id = $1 AND status = 'terminal' LIMIT 1"
        ))
        .bind(conversation_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
        if live.is_none() && terminal.is_some() {
            tracing::info!(
                conversation_id,
                "research resync: settling a task that ended while events were down"
            );
            complete_research_if_settled(state, conversation_id, "done").await;
        }
    }
}

/// The report text: the newest `pi.assistant` entry of the research
/// conversation, else the `research_notes` document entries, else a
/// placeholder.
async fn read_research_report_text(state: &AppState, conversation_id: i64) -> String {
    let schema = state.harness.durable_schema();
    let record: Option<String> = sqlx::query_scalar(&format!(
        r#"SELECT record FROM "{schema}".durable_entries
             WHERE conversation_id = $1 AND record::jsonb ->> 'kind' = 'pi.assistant'
             ORDER BY id DESC LIMIT 1"#
    ))
    .bind(conversation_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();
    if let Some(record) = record {
        let text = crate::harness::extract_assistant_text(&record);
        if !text.trim().is_empty() {
            return text;
        }
    }
    // Fallback: the notes the task recorded with the `note` tool.
    if let Ok(Some(notes)) = state
        .harness
        .client()
        .document_get(conversation_id, "research_notes")
        .await
    {
        if let Some(entries) = notes.get("entries").and_then(|e| e.as_array()) {
            let joined: String = entries
                .iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            if !joined.trim().is_empty() {
                return joined;
            }
        }
    }
    "(the research task ended without a report)".to_string()
}

/// Push the `research_report` suggestion card on the agent's
/// most-active conversation — the same queue and SSE door as the H4.4
/// `memory_review` cards, so ranchd's existing forge worker renders
/// it through the existing AgentAsk machinery. The oneshot is
/// dropped at once (cards are not relayed tool calls — the answer
/// arrives via `POST /ranch-tools/:id/result` within the queue TTL
/// (~60 s); a slower human keeps the row at `done`, still answerable
/// through `GET /agents/:id/research?open=1`).
async fn push_research_report_card(state: &AppState, research: &AgentResearch, summary: &str) {
    let conversation: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM sessions WHERE agent_id = $1 ORDER BY last_active DESC LIMIT 1",
    )
    .bind(research.agent_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();
    let Some(session_id) = conversation else {
        tracing::warn!(
            agent_id = %research.agent_id,
            research_id = %research.id,
            "research card: agent has no conversation to push on — the row stays 'done' (visible via ?open=1)"
        );
        return;
    };

    let payload = serde_json::json!({
        "kind": "research",
        "agent": research.agent_id,
        "research_id": research.id,
        "conversation_id": research.conversation_id,
        "report_ref": "research_report",
        "summary": summary,
        "question": research.question,
    });
    let (tx, _rx) = tokio::sync::oneshot::channel::<super::ranch_tools::RanchToolResult>();
    let id = state.ranch_tools.insert_meta(
        session_id,
        tx,
        Some("research_report".into()),
        payload.clone(),
    );
    state.bus.publish_ranch_tool_request(
        session_id,
        serde_json::json!({
            "id": id,
            "session_id": session_id,
            "tool": "research_report",
            "input": payload,
        }),
    );
    tracing::info!(
        agent_id = %research.agent_id,
        research_id = %research.id,
        session_id = %session_id,
        id = %id,
        "research_report card: pending (ranch approval round-trip)"
    );
}

// ============================================
// Card answers (the /ranch-tools/:id/result door)
// ============================================

/// Parse a ranchd answer to a `research_report` card into an action.
/// Pure, so the card-answer contract is unit-testable:
///
/// - `use` → the finding is adopted;
/// - `discard` → it is discarded;
/// - `ask_more` (with the user's follow-up text) → the text is
///   RE-INJECTED into the research conversation as a new user prompt
///   (the same submit path H2.1 uses for `POST /messages`; the
///   research conversation's filtered registry keeps the follow-up
///   read-only too) and the task runs again — the next completion
///   re-issues the card.
///
/// `None` = no transition (a failed relay or the queue TTL expired —
/// the row stays `done`, still visible via `?open=1`).
pub(crate) fn research_report_action(
    success: bool,
    output: &serde_json::Value,
) -> Option<(&'static str, Option<String>)> {
    if !success {
        return None;
    }
    let action = output.get("action")?.as_str()?;
    match action {
        "use" => Some(("use", None)),
        "discard" => Some(("discard", None)),
        "ask_more" => {
            let text = output
                .get("followup")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if text.is_empty() {
                return None;
            }
            Some(("ask_more", Some(text)))
        }
        _ => None,
    }
}

/// Apply a `research_report` card answer (called from
/// `ranch_tools::ranch_tool_result` when the pending entry's kind is
/// `research_report`). `payload` is the card payload published at
/// push time (`{research_id, agent, conversation_id, ...}`); `output`
/// is the ranchd result payload.
pub(crate) async fn apply_research_report(
    state: &AppState,
    user: &AuthenticatedUser,
    payload: &serde_json::Value,
    output: &serde_json::Value,
    success: bool,
) {
    let Some(action) = research_report_action(success, output) else {
        tracing::info!(
            "research_report apply: no action (relay failed or expired); row stays done"
        );
        return;
    };
    let research_id = match payload
        .get("research_id")
        .and_then(|v| Uuid::parse_str(v.as_str().unwrap_or("")).ok())
    {
        Some(id) => id,
        None => {
            tracing::warn!("research_report apply: malformed payload (no research_id)");
            return;
        }
    };
    let row: Option<AgentResearch> = match sqlx::query_as::<_, AgentResearch>(
        "SELECT * FROM agent_research WHERE id = $1",
    )
    .bind(research_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(research_id = %research_id, error = %e, "research_report apply: row lookup failed");
            return;
        }
    };
    let Some(research) = row else {
        tracing::warn!(research_id = %research_id, "research_report apply: unknown research row (deleted?)");
        return;
    };
    // Tenancy: the answerer must be able to see the agent's OWNER
    // (owner-or-admin) — the card itself was pushed on a session the
    // answerer already owns, but the result door is open to any API
    // key, so re-check against the agent row.
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
        .bind(research.agent_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    if !can_access(user, owner) {
        tracing::warn!(
            research_id = %research_id,
            "research_report apply: caller may not access the agent; ignoring"
        );
        return;
    }

    match action {
        ("use", _) | ("discard", _) => {
            let next_state = if action.0 == "use" {
                "adopted"
            } else {
                "discarded"
            };
            match sqlx::query(
                "UPDATE agent_research SET state = $2, resolved_at = NOW(), resolution = $3 WHERE id = $1 AND state NOT IN ('adopted', 'discarded')",
            )
            .bind(research_id)
            .bind(next_state)
            .bind(action.0)
            .execute(&state.db)
            .await
            {
                Ok(r) if r.rows_affected() > 0 => {
                    state.bus.publish_research_resolved(
                        research.conversation_id,
                        research.agent_id,
                        research_id,
                        next_state,
                        action.0,
                    );
                    tracing::info!(
                        research_id = %research_id,
                        agent_id = %research.agent_id,
                        state = next_state,
                        "research resolved"
                    );
                }
                Ok(_) => {
                    tracing::debug!(research_id = %research_id, "research already resolved (duplicate answer)");
                }
                Err(e) => tracing::error!(
                    research_id = %research_id,
                    error = %e,
                    "research_report apply: state transition failed"
                ),
            }
        }
        ("ask_more", Some(text)) => {
            // Re-inject the follow-up into the research conversation
            // and send the row back to `running` (the completion path
            // re-issues the card when the follow-up turn settles).
            let submitted = reinject_research_followup(state, &research, &text).await;
            if !submitted {
                // The follow-up was NOT admitted: the card was consumed
                // but no task runs — the row STAYS `done` (still open,
                // answerable via `GET /agents/:id/research?open=1`).
                tracing::warn!(
                    research_id = %research_id,
                    "research ask-more: follow-up was NOT submitted (harness unavailable); row stays done"
                );
                return;
            }
            match sqlx::query(
                "UPDATE agent_research SET state = 'running', resolution = $2 WHERE id = $1",
            )
            .bind(research_id)
            .bind(format!("ask_more: {text}"))
            .execute(&state.db)
            .await
            {
                Ok(r) if r.rows_affected() > 0 => {
                    tracing::info!(
                        research_id = %research_id,
                        "research ask-more re-injected (running again)"
                    );
                }
                Ok(_) => {
                    tracing::debug!(
                        research_id = %research_id,
                        "research ask-more: row already settled (duplicate answer)"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        research_id = %research_id,
                        error = %e,
                        "research ask-more: state transition failed"
                    );
                }
            }
        }
        _ => {}
    }
}

/// Re-inject an Ask-more follow-up into the research conversation:
/// persist the user row (audit), publish it on the bus, and submit it
/// to the harness — exactly the H2.1 write path, minus the lazy
/// migration (the research session was stamped at spawn).
async fn reinject_research_followup(
    state: &AppState,
    research: &AgentResearch,
    text: &str,
) -> bool {
    use crate::db::Message;

    let message: Message = match sqlx::query_as::<_, Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'user', $2) RETURNING *"#,
    )
    .bind(research.conversation_id)
    .bind(text)
    .fetch_one(&state.db)
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(
                research_id = %research.id,
                session_id = %research.conversation_id,
                error = %e,
                "research ask-more: user row insert failed"
            );
            return false;
        }
    };
    state.bus.publish_message(message.clone());
    crate::db::touch_session(&state.db, &research.conversation_id).await;

    let conversation_id: Option<i64> = match sqlx::query_scalar(
        "SELECT durable_conversation_id FROM sessions WHERE id = $1",
    )
    .bind(research.conversation_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(research_id = %research.id, error = %e, "research ask-more: conversation lookup failed");
            return false;
        }
    };
    let Some(conversation_id) = conversation_id else {
        tracing::error!(research_id = %research.id, "research ask-more: session lost its durable conversation");
        return false;
    };
    let request_id = Uuid::new_v4().to_string();
    let draft = serde_json::json!({ "type": "input", "content": text });
    match state
        .harness
        .client()
        .submit(conversation_id, &request_id, &draft)
        .await
    {
        Ok(_) => true,
        Err(e) => {
            tracing::error!(
                research_id = %research.id,
                conversation_id,
                error = %e,
                "research ask-more: harness submit failed"
            );
            false
        }
    }
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn research_report_action_parses_card_answers() {
        assert_eq!(
            research_report_action(true, &json!({ "action": "use" })),
            Some(("use", None))
        );
        assert_eq!(
            research_report_action(true, &json!({ "action": "discard" })),
            Some(("discard", None))
        );
        assert_eq!(
            research_report_action(
                true,
                &json!({ "action": "ask_more", "followup": "dig deeper into X" })
            ),
            Some(("ask_more", Some("dig deeper into X".to_string())))
        );
        // Blank follow-up is not an ask_more.
        assert_eq!(
            research_report_action(true, &json!({ "action": "ask_more", "followup": "   " })),
            None
        );
        // A failed relay is not an answer.
        assert_eq!(
            research_report_action(false, &json!({ "action": "use" })),
            None
        );
        assert_eq!(research_report_action(true, &json!({})), None);
        assert_eq!(research_report_action(true, &json!(null)), None);
        assert_eq!(
            research_report_action(true, &json!({ "action": "nonsense" })),
            None
        );
    }

    #[test]
    fn truncate_chars_respects_char_boundaries() {
        assert_eq!(truncate_chars("hello", 10), "hello");
        assert_eq!(truncate_chars("héllo", 3), "hél…");
        assert!(!truncate_chars("abcdef", 0).is_empty());
    }
}
