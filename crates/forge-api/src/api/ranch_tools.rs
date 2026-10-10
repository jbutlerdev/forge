//! Ranch tool relay (the "forge bridge", ranch PLAN F3a/F3b).
//!
//! Forge agents running on a remote host can't reach ranchd's loopback
//! control API, so the `ranch_*` tools (`ranch_status`, `ranch_read`,
//! `ranch_send`, `ranch_spawn`, `ranch_close`) are forwarded here:
//!
//! 1. `POST /tools/execute` with a `ranch_*` tool name intercepts the
//!    call BEFORE the sandbox executor, allocates a pending request,
//!    publishes a `ranch_tool_request` event on the session's SSE
//!    stream, and long-polls (bounded) for the result. The agent's
//!    turn stays synchronous, like any other tool call.
//! 2. ranchd's forge worker (already consuming that SSE stream for
//!    watched panes) executes the tool against its agent-tools registry
//!    and POSTs the result to `POST /ranch-tools/{id}/result`,
//!    authenticated with the forge API key it already holds.
//! 3. `POST /sessions/{id}/notify` (F3b) persists a `role:"system"`
//!    row + publishes it on the bus, so a spawn callback reaches an
//!    idle forge agent's harness without polling.
//!
//! The pending map is in-memory (like the sandbox manager): a forge
//! restart orphans at most the in-flight requests, and the long-poll
//! timeout turns them into tool errors the agent can retry.
//!
//! Herd H3.5: `POST /sessions/{id}/policy-ask` rides the SAME queue —
//! the harness's `before_tool` policy hook gets an `ask` verdict from
//! mule, publishes a `ranch_tool_request` with tool `policy_ask`, and
//! long-polls for ranchd's decision (`allow` / `deny` / `expired`).

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::api::auth::AuthenticatedUser;
use crate::api::{err_resp, AppState};

/// How long `/tools/execute` waits for ranchd to answer. Bounded so a
/// dead/absent ranch worker surfaces as a tool error, not a hung turn.
pub(crate) const RANCH_TOOL_TIMEOUT: Duration = Duration::from_secs(60);

/// One in-flight `ranch_*` tool call.
struct PendingRanchTool {
    #[allow(dead_code)] // kept for diagnostics when reaping expired rows
    session_id: Uuid,
    /// expire-before timestamp (unix ms); stale entries are reaped on insert
    expires_at: i64,
    /// The request kind (`None` for the plain `ranch_*` relay and
    /// `policy_ask`; `Some("memory_review")` for the H4.4 approval
    /// cards) — the result handler applies the kind's side effect.
    kind: Option<String>,
    /// The kind's context (memory_review: the belief + agent ids),
    /// carried so the result handler can apply the belief transition
    /// without re-deriving it.
    payload: serde_json::Value,
    tx: oneshot::Sender<RanchToolResult>,
}

/// The tool result ranchd POSTs back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RanchToolResult {
    pub success: bool,
    #[serde(default)]
    pub output: serde_json::Value,
    #[serde(default)]
    pub error: Option<String>,
}

/// The shared pending-request registry. `AppState` holds an `Arc` of
/// this; the execute handler inserts, the result handler resolves.
#[derive(Default)]
pub struct RanchToolQueue {
    pending: Mutex<HashMap<String, PendingRanchTool>>,
}

impl RanchToolQueue {
    pub fn new() -> Self {
        Self::default()
    }

    fn insert(&self, session_id: Uuid, tx: oneshot::Sender<RanchToolResult>) -> String {
        self.insert_meta(session_id, tx, None, serde_json::Value::Null)
    }

    /// [`insert`] with a request kind + context payload. The H4.4
    /// memory_review cards use this so that `POST /ranch-tools/{id}/result`
    /// knows HOW to apply the answer (belief transition + audit) on
    /// top of the plain oneshot resolution.
    pub fn insert_meta(
        &self,
        session_id: Uuid,
        tx: oneshot::Sender<RanchToolResult>,
        kind: Option<String>,
        payload: serde_json::Value,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(i64::MAX);
        let mut map = self.pending.lock().unwrap();
        // reap expired entries (their senders are gone; dropping the
        // entry closes the oneshot and the long-poll wakes with an error)
        map.retain(|_, p| p.expires_at > now_ms);
        map.insert(
            id.clone(),
            PendingRanchTool {
                session_id,
                expires_at: now_ms + RANCH_TOOL_TIMEOUT.as_millis() as i64,
                kind,
                payload,
                tx,
            },
        );
        id
    }

    /// Resolve a pending request with its result. Returns the entry's
    /// `(kind, payload)` when an unexpired entry existed — the caller
    /// applies the kind's side effect (H4.4 belief transition) on top
    /// of the oneshot delivery, which is best-effort (a dropped
    /// receiver — the caller already timed out — still counts as a
    /// delivered answer; the side effect is what matters). `None`
    /// when the id is unknown or expired.
    fn resolve(
        &self,
        id: &str,
        result: RanchToolResult,
    ) -> Option<(Option<String>, serde_json::Value)> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(i64::MAX);
        let mut map = self.pending.lock().unwrap();
        // reap expired entries; then the requested one (its oneshot
        // may already be dropped — send failure is not an error)
        map.retain(|_, p| p.expires_at > now_ms);
        let entry = map.remove(id)?;
        entry.tx.send(result).ok();
        Some((entry.kind, entry.payload))
    }
}

/// True when this tool call must be relayed to ranchd instead of run
/// in the sandbox executor.
pub fn is_ranch_tool(tool: &str) -> bool {
    tool.starts_with("ranch_")
}

/// Interception point called from `execute_tool` before the executor.
/// Returns `None` when the tool is not a ranch tool (caller proceeds);
/// `Some(response)` is the final answer for the call.
pub async fn relay_ranch_tool(
    state: &AppState,
    session_id: Uuid,
    tool: &str,
    input: serde_json::Value,
) -> Option<Response> {
    if !is_ranch_tool(tool) {
        return None;
    }
    let (tx, rx) = oneshot::channel();
    let id = state.ranch_tools.insert(session_id, tx);

    // publish the request on the session's SSE stream; ranchd's forge
    // worker filters on this session and executes the tool
    let payload = json!({
        "id": id,
        "session_id": session_id,
        "tool": tool,
        "input": input,
    });
    state.bus.publish_ranch_tool_request(session_id, payload);

    tracing::info!(session_id = %session_id, tool = %tool, id = %id, "ranch tool relay: pending");

    match tokio::time::timeout(RANCH_TOOL_TIMEOUT, rx).await {
        Ok(Ok(result)) => {
            tracing::info!(id = %id, tool = %tool, success = %result.success, "ranch tool relay: resolved");
            Some(
                Json(json!({
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                }))
                .into_response(),
            )
        }
        Ok(Err(_)) | Err(_) => {
            // sender dropped (result raced a restart) or the wait timed
            // out — surface as a failed tool call, never a hung turn
            tracing::warn!(id = %id, tool = %tool, "ranch tool relay: no result (timeout or worker gone)");
            Some(
                (
                    axum::http::StatusCode::GATEWAY_TIMEOUT,
                    Json(json!({
                        "success": false,
                        "output": serde_json::Value::Null,
                        "error": "ranch tool relay timed out (no ranch worker answered)",
                    })),
                )
                    .into_response(),
            )
        }
    }
}

/// Body of `POST /sessions/:id/policy-ask` (H3.5 part 2). Sent by the
/// harness `before_tool` policy hook when mule's verdict is `ask`.
#[derive(Debug, Deserialize)]
pub struct PolicyAskBody {
    /// The winning mule rule's id ("" when the rule churned away).
    #[serde(default)]
    pub rule_id: String,
    /// The rule's transcript-ready reason.
    #[serde(default)]
    pub reason: String,
    /// The tool name the agent wanted to call (mule tool surface:
    /// bash | read | write | edit | spawn_subagent | …).
    pub tool: String,
    /// The tool's arguments, verbatim (the mule action hash is keyed
    /// on exactly this object).
    #[serde(default)]
    pub input: serde_json::Value,
}

/// `POST /sessions/:id/policy-ask` — the forge side of the H3.5
/// `ask` round-trip. Inserts a PENDING ROW into the SAME queue the
/// `ranch_*` relay uses and publishes a `ranch_tool_request` event
/// with tool name `policy_ask` (the existing event shape — rule_id
/// and reason ride along in the input payload). ranchd's forge worker
/// turns it into the ranch approval round-trip (an `AgentAskRequest`
/// on every client via the loopback control API) and POSTs the
/// decision back to `POST /ranch-tools/{id}/result`, which resolves
/// the oneshot.
///
/// The long-poll is bounded by `RANCH_TOOL_TIMEOUT`, like the
/// `ranch_*` relay: no hung turns. Response: `{"decision":
/// "allow"|"deny"|"expired"}` — allow when ranchd reported
/// success, deny on an explicit failure, expired when the oneshot
/// fired with no result (TTL or a forge restart).
pub(crate) async fn session_policy_ask(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(session_id): Path<Uuid>,
    Json(body): Json<PolicyAskBody>,
) -> Response {
    // Tenancy gate, the `session_notify` shape: the caller must be
    // the session's owner or an admin. In practice the harness calls
    // with the operator's FORGE_API_KEY (admin-class) and the session
    // row was minted by the same forge instance, so a foreign key on
    // someone else's session gets the same 404 as notify.
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    let Some(owner) = owner else {
        return err_resp(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "Session not found",
        );
    };
    if !crate::api::auth::can_access(&user, Some(owner)) {
        return err_resp(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "Session not found",
        );
    }
    if body.tool.is_empty() {
        return err_resp(
            &state,
            axum::http::StatusCode::BAD_REQUEST,
            "tool is required",
        );
    }

    let (tx, rx) = oneshot::channel();
    let id = state.ranch_tools.insert(session_id, tx);

    // The existing event shape: tool name `policy_ask`, with the
    // decision context in the input payload. ranchd filters on
    // tool == "policy_ask" and drives the approval round-trip.
    let payload = json!({
        "id": id,
        "session_id": session_id,
        "tool": "policy_ask",
        "input": {
            "rule_id": body.rule_id,
            "reason": body.reason,
            "tool": body.tool,
            "input": body.input,
        },
    });
    state.bus.publish_ranch_tool_request(session_id, payload);

    tracing::info!(
        session_id = %session_id,
        tool = %body.tool,
        rule_id = %body.rule_id,
        id = %id,
        "policy ask: pending (ranch approval round-trip)"
    );

    match tokio::time::timeout(RANCH_TOOL_TIMEOUT, rx).await {
        Ok(Ok(result)) => {
            let decision = if result.success { "allow" } else { "deny" };
            tracing::info!(
                id = %id,
                tool = %body.tool,
                rule_id = %body.rule_id,
                decision,
                "policy ask: resolved"
            );
            (
                axum::http::StatusCode::OK,
                Json(json!({ "decision": decision })),
            )
                .into_response()
        }
        Ok(Err(_)) | Err(_) => {
            tracing::warn!(
                id = %id,
                tool = %body.tool,
                rule_id = %body.rule_id,
                "policy ask: no decision (timeout or queue loss)"
            );
            (
                axum::http::StatusCode::OK,
                Json(json!({ "decision": "expired" })),
            )
                .into_response()
        }
    }
}

/// Body of `POST /ranch-tools/:id/result` from ranchd.
#[derive(Debug, Deserialize)]
pub struct RanchToolResultBody {
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub output: serde_json::Value,
    #[serde(default)]
    pub error: Option<String>,
}

/// `POST /ranch-tools/:id/result` — ranchd answers a pending tool call.
/// Authenticated like any forge API route (the key ranchd already
/// holds); the pending row's session tenancy was checked at request
/// time by the `/tools/execute` gate, so here we only need the id.
///
/// H4.4: when the pending entry's kind is `memory_review`, the answer
/// ALSO drives the belief transition (keep → active, forget →
/// forgotten, edit → new content + active, version bump + audit) —
/// see [`crate::api::memory::apply_memory_review`]. The oneshot
/// resolution to the original caller happens exactly as before.
pub(crate) async fn ranch_tool_result(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(tool_id): Path<String>,
    Json(body): Json<RanchToolResultBody>,
) -> Response {
    let Some((kind, payload)) = state.ranch_tools.resolve(
        &tool_id,
        RanchToolResult {
            success: body.success,
            output: body.output.clone(),
            error: body.error.clone(),
        },
    ) else {
        return err_resp(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "unknown or expired ranch tool request",
        );
    };
    if kind.as_deref() == Some("memory_review") {
        crate::api::memory::apply_memory_review(
            &state,
            &user,
            &payload,
            &body.output,
            body.success,
        );
    }
    // Herd H5.1: the research_report card — Use/Discard resolve the
    // agent_research row (bus `research_resolved`); Ask-more re-injects
    // the follow-up into the research conversation.
    if kind.as_deref() == Some("research_report") {
        crate::api::research::apply_research_report(
            &state,
            &user,
            &payload,
            &body.output,
            body.success,
        )
        .await;
    }
    (axum::http::StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

/// Body of `POST /sessions/:id/notify` (F3b).
#[derive(Debug, Deserialize)]
pub struct NotifyBody {
    /// System-role text (e.g. `✓ sub-agent "refactor" finished`).
    pub text: String,
}

/// `POST /sessions/:id/notify` — persist a `role:"system"` row and
/// publish it on the bus. This is how a ranch spawn callback reaches
/// an idle forge agent's harness: the harness sees the system row on
/// its message stream and can react without polling.
pub(crate) async fn session_notify(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(session_id): Path<Uuid>,
    Json(body): Json<NotifyBody>,
) -> Response {
    // tenancy: the notifier must be able to see the session
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    let Some(owner) = owner else {
        // no existence leak (same as the tools gate)
        return err_resp(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "Session not found",
        );
    };
    if !crate::api::auth::can_access(&user, Some(owner)) {
        return err_resp(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "Session not found",
        );
    }
    if body.text.is_empty() || body.text.len() > 8 * 1024 {
        return err_resp(
            &state,
            axum::http::StatusCode::BAD_REQUEST,
            "text must be 1..8192 chars",
        );
    }

    // system rows don't drive turns; plain INSERT + bus publish
    // (sequence allocated inside the INSERT, same race-avoidance as
    // `insert_and_publish_assistant`)
    let row = sqlx::query_as::<_, crate::db::Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, get_next_sequence($1), 'system', $2) RETURNING *"#,
    )
    .bind(session_id)
    .bind(&body.text)
    .fetch_one(&state.db)
    .await;
    match row {
        Ok(message) => {
            state.bus.publish_message(message);
            (
                axum::http::StatusCode::ACCEPTED,
                Json(json!({ "ok": true })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(session_id = %session_id, error = %e, "session notify: insert failed");
            err_resp(
                &state,
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to persist notification",
            )
        }
    }
}

// ---- tests ----

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranch_tool_names_intercept() {
        assert!(is_ranch_tool("ranch_status"));
        assert!(is_ranch_tool("ranch_spawn"));
        assert!(!is_ranch_tool("bash"));
        assert!(!is_ranch_tool("read_file"));
        assert!(!is_ranch_tool("ranch")); // bare prefix, not a tool we ship
    }

    #[test]
    fn policy_ask_decision_mapping() {
        // The queue mechanics the policy-ask handler relies on: insert
        // → the bus payload's id resolves via the result endpoint's
        // queue row → the oneshot fires with the relayed decision.
        let q = RanchToolQueue::new();

        // allow: success result → decision "allow"
        let (tx, mut rx) = oneshot::channel();
        let id = q.insert(Uuid::new_v4(), tx);
        assert!(q
            .resolve(
                &id,
                RanchToolResult {
                    success: true,
                    output: json!({ "decision_context": "rule-x" }),
                    error: None
                }
            )
            .is_some());
        let result = rx.try_recv().unwrap();
        assert!(result.success, "an approval maps to success=true");

        // deny: failure result → decision "deny"
        let (tx, mut rx) = oneshot::channel();
        let id = q.insert(Uuid::new_v4(), tx);
        assert!(q
            .resolve(
                &id,
                RanchToolResult {
                    success: false,
                    output: json!(null),
                    error: Some("user denied".into())
                }
            )
            .is_some());
        let result = rx.try_recv().unwrap();
        assert!(!result.success, "a denial maps to success=false");

        // expired: the id is unknown/removed → resolve None, the
        // handler's oneshot never resolves → "expired"
        assert!(q
            .resolve(
                "no-such-id",
                RanchToolResult {
                    success: true,
                    output: json!(null),
                    error: None
                }
            )
            .is_none());
        // and a dropped sender (restarted forge) fires the oneshot
        // with RecvError — the handler's `Ok(Err(_))` arm → "expired"
        let (tx, mut rx) = oneshot::channel();
        q.insert(Uuid::new_v4(), tx);
        drop(q);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn queue_insert_and_resolve() {
        let q = RanchToolQueue::new();
        let (tx, mut rx) = oneshot::channel();
        let id = q.insert(Uuid::new_v4(), tx);
        let meta = q.resolve(
            &id,
            RanchToolResult {
                success: true,
                output: json!("ok"),
                error: None,
            },
        );
        assert!(meta.is_some());
        assert!(meta.unwrap().0.is_none()); // plain insert has no kind
        assert!(rx.try_recv().is_ok());
        // second resolve of the same id fails (removed)
        assert!(q
            .resolve(
                &id,
                RanchToolResult {
                    success: false,
                    output: json!(null),
                    error: None
                }
            )
            .is_none());
    }

    #[test]
    fn queue_insert_meta_carries_kind_and_payload() {
        let q = RanchToolQueue::new();
        let (tx, mut rx) = oneshot::channel();
        let id = q.insert_meta(
            Uuid::new_v4(),
            tx,
            Some("memory_review".into()),
            json!({ "belief_id": "b1" }),
        );
        let meta = q.resolve(
            &id,
            RanchToolResult {
                success: true,
                output: json!({ "action": "keep" }),
                error: None,
            },
        );
        assert!(rx.try_recv().is_ok());
        let (kind, payload) = meta.unwrap();
        assert_eq!(kind.as_deref(), Some("memory_review"));
        assert_eq!(payload["belief_id"], "b1");
    }
}
