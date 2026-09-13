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
const RANCH_TOOL_TIMEOUT: Duration = Duration::from_secs(60);

/// One in-flight `ranch_*` tool call.
struct PendingRanchTool {
    #[allow(dead_code)] // kept for diagnostics when reaping expired rows
    session_id: Uuid,
    /// expire-before timestamp (unix ms); stale entries are reaped on insert
    expires_at: i64,
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
                tx,
            },
        );
        id
    }

    fn resolve(&self, id: &str, result: RanchToolResult) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(i64::MAX);
        match self.pending.lock().unwrap().remove(id) {
            Some(p) if p.expires_at > now_ms => p.tx.send(result).is_ok(),
            _ => false,
        }
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
pub(crate) async fn ranch_tool_result(
    State(state): State<AppState>,
    Path(tool_id): Path<String>,
    Json(body): Json<RanchToolResultBody>,
) -> Response {
    let resolved = state.ranch_tools.resolve(
        &tool_id,
        RanchToolResult {
            success: body.success,
            output: body.output,
            error: body.error,
        },
    );
    if resolved {
        (axum::http::StatusCode::OK, Json(json!({ "ok": true }))).into_response()
    } else {
        err_resp(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "unknown or expired ranch tool request",
        )
    }
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
    fn queue_insert_and_resolve() {
        let q = RanchToolQueue::new();
        let (tx, mut rx) = oneshot::channel();
        let id = q.insert(Uuid::new_v4(), tx);
        assert!(q.resolve(
            &id,
            RanchToolResult {
                success: true,
                output: json!("ok"),
                error: None
            }
        ));
        assert!(rx.try_recv().is_ok());
        // second resolve of the same id fails (removed)
        assert!(!q.resolve(
            &id,
            RanchToolResult {
                success: false,
                output: json!(null),
                error: None
            }
        ));
    }
}
