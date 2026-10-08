//! Herd H4: agent memory read routes + the `memory_remember` tool
//! endpoint.
//!
//! Read routes (H4.3):
//! - `GET /agents/:id/memory/search?q=&k=&scope=` — embed the query
//!   (2560-dim Qwen3 via `crate::embedding`) and cosine-retrieve the
//!   agent's episodes + active beliefs (org tier when `scope=org` and
//!   `memory_acl` grants read). Provenance (`source` /
//!   `source_episodes`) rides on every hit.
//! - `GET /agents/:id/memory/beliefs?status=&limit=` — belief rows
//!   (newest update first), optionally filtered by status.
//! - `GET /agents/:id/memory/beliefs/:bid` — one belief + its
//!   `belief_audit` chain.
//!
//! Write route (H4.2, the `memory_remember` tool's target; H4.4's
//! reflection proposals land in H4.4 as their own endpoint):
//! - `POST /agents/:id/memory/beliefs` — insert a `pending` belief
//!   (`kind` defaults to `preference`), best-effort embedded. This is
//!   user-instructed memory: it records but never auto-activates.
//!
//! Trigger-queue routes (H4.5, the mule forwarder lane's pull + ACK):
//! - `GET /agents/:id/memory/triggers/pending?limit=50` — unconsumed
//!   `memory_trigger_queue` rows, oldest first;
//! - `POST /agents/:id/memory/triggers/:tid/consumed` — idempotent ACK.
//!
//! Tenancy is the owner-or-admin agent gate (`agent_access_err`, same
//! 404-not-403 contract as every other agent route). When pgvector was
//! not installable at migration time the memory tables are absent and
//! every route returns 501 (the skip-when-no-pgvector test contract).

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use super::agents::agent_access_err;
use super::{err_resp, AppState};
use crate::api::auth::AuthenticatedUser;
use axum::extract::Extension;

/// 501 when the memory tables are absent (no pgvector at migration
/// time). `None` ⇒ proceed.
async fn memory_unavailable(state: &AppState) -> Option<Response> {
    if crate::memory::vector_available(&state.db).await {
        None
    } else {
        Some(err_resp(
            state,
            StatusCode::NOT_IMPLEMENTED,
            "memory unavailable: pgvector is not installed on this database",
        ))
    }
}

fn memory_caller(user: &AuthenticatedUser) -> crate::memory::Caller {
    crate::memory::Caller {
        user_id: user.user_id,
        is_admin: user.role == "admin",
    }
}

// ============================================
// GET /agents/:id/memory/search
// ============================================

#[derive(Deserialize)]
pub(crate) struct MemorySearchQuery {
    q: String,
    k: Option<i64>,
    /// `agent` (default) or `org` — `org` widens the belief search to
    /// the caller's read-granted memory orgs (H4.3).
    scope: Option<String>,
}

pub(crate) async fn memory_search(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Query(query): Query<MemorySearchQuery>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    let include_org = query.scope.as_deref() == Some("org");
    let k = query.k.unwrap_or(12).clamp(1, 50);

    // Embed the query; a missing/unreachable embedding endpoint is a
    // 503 with a clear reason (the prompt-section consumer degrades to
    // the confidence-only pass instead of calling this).
    let qvec = match crate::embedding::embed(&state.embedding_config, &query.q).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("memory search: embedding failed: {e}");
            return err_resp(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                "embedding unavailable: the memory embedding endpoint did not answer",
            );
        }
    };

    match crate::memory::search(
        &state.db,
        &memory_caller(&user),
        agent_id,
        &qvec,
        k,
        include_org,
    )
    .await
    {
        Ok(r) => {
            state.metrics.inc_requests("GET /agents/:id/memory/search");
            Json(serde_json::json!({
                "query": query.q,
                "scope": if include_org { "org" } else { "agent" },
                "episodes": r.episodes,
                "beliefs": r.beliefs,
            }))
            .into_response()
        }
        Err(e) => {
            tracing::error!("memory search failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "memory search failed",
            )
        }
    }
}

// ============================================
// GET /agents/:id/memory/beliefs
// ============================================

#[derive(Deserialize)]
pub(crate) struct MemoryBeliefsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

pub(crate) async fn list_beliefs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Query(query): Query<MemoryBeliefsQuery>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    if let Some(ref s) = query.status {
        if !["pending", "active", "forgotten", "superseded"].contains(&s.as_str()) {
            return err_resp(&state, StatusCode::BAD_REQUEST, "invalid status filter");
        }
    }
    match crate::memory::list(
        &state.db,
        &memory_caller(&user),
        agent_id,
        query.status.as_deref(),
        query.limit.unwrap_or(50),
    )
    .await
    {
        Ok(beliefs) => {
            state.metrics.inc_requests("GET /agents/:id/memory/beliefs");
            Json(serde_json::json!({ "beliefs": beliefs })).into_response()
        }
        Err(e) => {
            tracing::error!("list beliefs failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list beliefs",
            )
        }
    }
}

// ============================================
// GET /agents/:id/memory/beliefs/:bid
// ============================================

pub(crate) async fn get_belief(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((agent_id, belief_id)): Path<(Uuid, Uuid)>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    let caller = memory_caller(&user);
    match (
        crate::memory::get(&state.db, &caller, agent_id, belief_id).await,
        crate::memory::audit(&state.db, &caller, agent_id, belief_id).await,
    ) {
        (Ok(belief), Ok(audit)) => {
            state
                .metrics
                .inc_requests("GET /agents/:id/memory/beliefs/:bid");
            Json(serde_json::json!({ "belief": belief, "audit": audit })).into_response()
        }
        _ => {
            // Either lookup failing means the belief isn't visible to
            // this caller — 404, never 403 (no existence leaks).
            err_resp(&state, StatusCode::NOT_FOUND, "Belief not found")
        }
    }
}

// ============================================
// POST /agents/:id/memory/beliefs (memory_remember)
// ============================================

#[derive(Deserialize)]
pub(crate) struct MemoryRememberBody {
    content: String,
    /// 'preference' (default) | 'fact' | 'procedure' | 'constraint'.
    kind: Option<String>,
}

pub(crate) async fn remember(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Json(body): Json<MemoryRememberBody>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: memory writes are not available",
        );
    }
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    if body.content.trim().is_empty() {
        return err_resp(&state, StatusCode::BAD_REQUEST, "content is required");
    }
    let kind = body.kind.as_deref().unwrap_or("preference");
    if !["preference", "fact", "procedure", "constraint"].contains(&kind) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "invalid kind; expected preference|fact|procedure|constraint",
        );
    }

    // Best-effort embedding: the belief is stored even when the
    // endpoint is down (it then just stays out of cosine retrieval
    // until re-embedded by the H4.4 reflection pass).
    let embedding = match crate::embedding::embed(&state.embedding_config, &body.content).await {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!("memory_remember: embedding failed (storing unembedded): {e}");
            None
        }
    };

    match crate::memory::upsert_pending(
        &state.db,
        &memory_caller(&user),
        agent_id,
        "agent",
        kind,
        &body.content,
        0.5,
        Vec::new(),
        embedding,
        "memory_remember",
    )
    .await
    {
        Ok(b) => {
            state
                .metrics
                .inc_requests("POST /agents/:id/memory/beliefs");
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "status": "recorded",
                    "note": "pending your review",
                    "belief_id": b.id,
                    "belief": b,
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("memory_remember failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to record belief",
            )
        }
    }
}

// ============================================
// POST /agents/:id/memory/beliefs/proposals (H4.4 reflection)
// ============================================

/// One belief proposed by the reflection workflow.
#[derive(Deserialize)]
pub(crate) struct ProposedBelief {
    content: String,
    /// 'preference' (default) | 'fact' | 'procedure' | 'constraint'.
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    confidence: Option<f32>,
    /// Why the agent learned this (shown on the approval card).
    #[serde(default)]
    rationale: Option<String>,
    /// Provenance: the episode ids this belief was distilled from.
    #[serde(default)]
    source_episodes: Vec<String>,
    /// H4.5 watch spec (stored verbatim; honored by a later phase).
    #[serde(default)]
    watch: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub(crate) struct ProposalsBody {
    /// The proposing identity: defaults to `reflection` (the mule
    /// workflow); a workflow may stamp its own actor id into the audit
    /// trail.
    #[serde(default)]
    actor: Option<String>,
    beliefs: Vec<ProposedBelief>,
}

/// Cosine similarity above which two beliefs are treated as the same
/// thing (PLAN-HERD H4.4 step 3).
const NEAR_DUPLICATE_THRESHOLD: f32 = 0.92;

/// `POST /agents/:id/memory/beliefs/proposals` — the H4.4 reflection
/// loop's submission door. For each proposed belief: reject when a
/// nearly-identical pending/active belief already exists (normalized
/// string equality first; cosine > `NEAR_DUPLICATE_THRESHOLD` when
/// embeddings are available — string-normalize is the primary gate, a
/// down embedding endpoint degrades to string-only, documented),
/// otherwise insert it `pending` with a `proposed` audit row carrying
/// the full proposal, and push the approval card (see
/// [`push_memory_review_card`]). `memory_remember` (the user-instructed
/// path) does NOT get cards: the user asked for the memory directly.
pub(crate) async fn propose_beliefs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Json(body): Json<ProposalsBody>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: memory writes are not available",
        );
    }
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    let caller = memory_caller(&user);
    let actor = body
        .actor
        .as_deref()
        .filter(|a| !a.trim().is_empty())
        .unwrap_or("reflection");

    // Existing pending/active beliefs + embeddings: the near-duplicate
    // gate's candidate set (fetched once for the whole batch).
    let existing = match crate::memory::duplicate_scan(&state.db, &caller, agent_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("belief proposals: duplicate scan failed: {e}");
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to scan beliefs",
            );
        }
    };

    // Batch-local dedupe: two identical items in ONE request must not
    // both land.
    let mut accepted_norms: Vec<(Uuid, String)> = Vec::new();

    let mut results: Vec<serde_json::Value> = Vec::with_capacity(body.beliefs.len());
    for p in &body.beliefs {
        if p.content.trim().is_empty() {
            results.push(json!({ "accepted": false, "rejected_reason": "content is required" }));
            continue;
        }
        let kind = p.kind.as_deref().unwrap_or("preference");
        if !["preference", "fact", "procedure", "constraint"].contains(&kind) {
            results.push(json!({
                "accepted": false,
                "rejected_reason": "invalid kind; expected preference|fact|procedure|constraint"
            }));
            continue;
        }
        let confidence = p.confidence.unwrap_or(0.5).clamp(0.0, 1.0);

        // source_episodes: per-item rejection on a malformed uuid
        // (the whole request is NOT aborted by one bad element).
        let episodes: Vec<Uuid> = match p
            .source_episodes
            .iter()
            .map(|s| Uuid::parse_str(s))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(e) => {
                results.push(json!({
                    "accepted": false,
                    "rejected_reason": format!("invalid source_episodes: {e}")
                }));
                continue;
            }
        };

        // Best-effort embedding: a down endpoint degrades the cosine
        // pass to string-normalize-only (documented above).
        let embedding = match crate::embedding::embed(&state.embedding_config, &p.content).await {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!("belief proposal: embedding failed (string gate only): {e}");
                None
            }
        };

        // Near-duplicate gate: existing agent beliefs, then the batch
        // itself (string equality — batch items that share the
        // embedding endpoint already have identical vectors).
        let dup = crate::memory::find_near_duplicate(
            &p.content,
            embedding.as_deref(),
            NEAR_DUPLICATE_THRESHOLD,
            &existing,
        );
        let dup = dup.or_else(|| {
            crate::memory::find_near_duplicate(
                &p.content,
                None,
                0.0,
                &accepted_norms
                    .iter()
                    .map(|(id, n)| (*id, n.clone(), None))
                    .collect::<Vec<_>>(),
            )
        });
        if let Some((id, sim)) = dup {
            let reason = if sim >= 1.0 {
                format!("near-duplicate of belief {id} (string match)")
            } else {
                format!("near-duplicate of belief {id} (cosine {sim:.2})")
            };
            results.push(json!({ "accepted": false, "rejected_reason": reason }));
            continue;
        }

        match crate::memory::upsert_proposal(
            &state.db,
            &caller,
            agent_id,
            "agent",
            kind,
            &p.content,
            confidence,
            p.rationale.as_deref(),
            &episodes,
            p.watch.as_ref(),
            embedding,
            actor,
        )
        .await
        {
            Ok(b) => {
                // The approval card (documented degradation: no active
                // conversation ⇒ no push, the belief stays pending and
                // is still visible via GET /memory/beliefs?status=pending).
                let card = push_memory_review_card(&state, agent_id, &b).await;
                accepted_norms.push((b.id, crate::memory::normalize_belief_text(&b.content)));
                results.push(json!({
                    "accepted": true,
                    "belief_id": b.id,
                    "card_pushed": card,
                }));
            }
            Err(e) => {
                tracing::error!("belief proposal failed: {e}");
                results.push(json!({ "accepted": false, "rejected_reason": "storage error" }));
            }
        }
    }

    state
        .metrics
        .inc_requests("POST /agents/:id/memory/beliefs/proposals");
    Json(json!({ "results": results })).into_response()
}

// ============================================
// POST /agents/:id/memory/beliefs/:bid/keep | /forget
// ============================================

/// The direct (REST) Keep/Forget doors — the same transitions the
/// approval card drives, for users who answer through the API instead
/// of the card. Actor is the approver user (`user:<id>`).
async fn belief_lifecycle(
    state: &AppState,
    user: &AuthenticatedUser,
    agent_id: Uuid,
    belief_id: Uuid,
    target: &str, // "active" (keep) | "forgotten" (forget)
    metric: &str,
) -> Response {
    if user.restricted {
        return err_resp(
            state,
            StatusCode::FORBIDDEN,
            "Restricted key: memory writes are not available",
        );
    }
    if let Some(resp) = agent_access_err(state, user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(state).await {
        return resp;
    }
    let caller = memory_caller(user);
    match crate::memory::set_status(
        &state.db,
        &caller,
        agent_id,
        belief_id,
        target,
        None,
        &format!("user:{}", user.user_id),
    )
    .await
    {
        Ok(b) => {
            state.metrics.inc_requests(metric);
            Json(json!({ "belief": b })).into_response()
        }
        Err(crate::memory::MemoryError::BeliefNotFound) => {
            err_resp(state, StatusCode::NOT_FOUND, "Belief not found")
        }
        Err(e) => {
            tracing::error!("belief lifecycle failed: {e}");
            err_resp(
                state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update belief",
            )
        }
    }
}

pub(crate) async fn keep_belief(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((agent_id, belief_id)): Path<(Uuid, Uuid)>,
) -> Response {
    belief_lifecycle(
        &state,
        &user,
        agent_id,
        belief_id,
        "active",
        "POST /agents/:id/memory/beliefs/:bid/keep",
    )
    .await
}

pub(crate) async fn forget_belief(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((agent_id, belief_id)): Path<(Uuid, Uuid)>,
) -> Response {
    belief_lifecycle(
        &state,
        &user,
        agent_id,
        belief_id,
        "forgotten",
        "POST /agents/:id/memory/beliefs/:bid/forget",
    )
    .await
}

// ============================================
// H4.5 memory trigger queue (mule forwarder lane's pull + ACK)
// ============================================

/// `GET /agents/:id/memory/triggers/pending?limit=50` — the agent's
/// unconsumed memory triggers, oldest first. The mule forwarder lane
/// (H4.5, `internal/wake/memory_trigger.go`) polls this every 30 s
/// for each configured agent and ACKs via
/// `POST …/triggers/:tid/consumed`.
#[derive(Deserialize)]
pub(crate) struct TriggersPendingQuery {
    limit: Option<i64>,
}

pub(crate) async fn pending_triggers(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(agent_id): Path<Uuid>,
    Query(query): Query<TriggersPendingQuery>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    match crate::memory::pending_triggers(
        &state.db,
        &memory_caller(&user),
        agent_id,
        query.limit.unwrap_or(50),
    )
    .await
    {
        Ok(triggers) => {
            state
                .metrics
                .inc_requests("GET /agents/:id/memory/triggers/pending");
            Json(json!({ "triggers": triggers })).into_response()
        }
        Err(e) => {
            tracing::error!("pending triggers failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list memory triggers",
            )
        }
    }
}

/// `POST /agents/:id/memory/triggers/:tid/consumed` — the mule lane's
/// ACK after it fired the watch's wake. Idempotent: an already-
/// consumed trigger returns 200 with the row unchanged (a redelivered
/// ACK is a no-op; the lane's at-least-once retry is safe).
pub(crate) async fn trigger_consumed(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((agent_id, trigger_id)): Path<(Uuid, Uuid)>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, agent_id).await {
        return resp;
    }
    if let Some(resp) = memory_unavailable(&state).await {
        return resp;
    }
    match crate::memory::mark_trigger_consumed(
        &state.db,
        &memory_caller(&user),
        agent_id,
        trigger_id,
    )
    .await
    {
        Ok(t) => {
            state
                .metrics
                .inc_requests("POST /agents/:id/memory/triggers/:tid/consumed");
            Json(json!({ "trigger": t })).into_response()
        }
        Err(crate::memory::MemoryError::TriggerNotFound) => {
            err_resp(&state, StatusCode::NOT_FOUND, "Trigger not found")
        }
        Err(e) => {
            tracing::error!("trigger consumed failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to mark trigger consumed",
            )
        }
    }
}

// ============================================
// H4.4 approval cards (memory_review on the ranch queue)
// ============================================

/// Push the approval card for a freshly-proposed pending belief: find
/// the agent's most-active conversation (the `GET /agents/:id/active`
/// rule — `last_active DESC`), queue a `memory_review` ranch-tool
/// request on it, and publish the `ranch_tool_request` event on that
/// conversation's SSE stream — the same door `policy_ask` (H3.5) uses,
/// so ranchd's existing forge worker picks it up and the card reaches
/// every client through the existing AgentAsk machinery.
///
/// Returns `true` when the card was pushed. `false` (with a warn log)
/// when the agent has no conversation: the belief simply stays
/// pending — visible via `GET …/memory/beliefs?status=pending` and
/// answerable via the keep/forget routes. v1 does NOT re-push on the
/// agent's next conversation attach.
async fn push_memory_review_card(
    state: &AppState,
    agent_id: Uuid,
    belief: &crate::memory::Belief,
) -> bool {
    let conversation: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM sessions WHERE agent_id = $1 ORDER BY last_active DESC LIMIT 1",
    )
    .bind(agent_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();
    let Some(session_id) = conversation else {
        tracing::warn!(
            agent_id = %agent_id,
            belief_id = %belief.id,
            "memory_review card: agent has no active conversation — belief stays pending (no re-push in v1)"
        );
        return false;
    };

    let payload = json!({
        "kind": "memory",
        "agent": agent_id,
        "belief_id": belief.id,
        "belief_kind": belief.kind,
        "content": belief.content,
        "rationale": belief.rationale,
        "source_episodes": belief.source_episodes,
    });
    let (tx, _rx) = tokio::sync::oneshot::channel::<crate::api::ranch_tools::RanchToolResult>();
    let id = state.ranch_tools.insert_meta(
        session_id,
        tx,
        Some("memory_review".into()),
        payload.clone(),
    );
    state.bus.publish_ranch_tool_request(
        session_id,
        json!({
            "id": id,
            "session_id": session_id,
            "tool": "memory_review",
            "input": payload,
        }),
    );
    tracing::info!(
        agent_id = %agent_id,
        belief_id = %belief.id,
        session_id = %session_id,
        id = %id,
        "memory_review card: pending (ranch approval round-trip)"
    );
    true
}

/// Parse a ranchd answer to a `memory_review` card into a belief
/// action. Pure, so the card-answer contract is unit-testable:
/// `keep` | `forget` | `edit` (with the user's free text as the new
/// content). `None` = no transition (a failed relay, a deadline
/// expiry — the belief stays pending).
pub(crate) fn memory_review_action(
    success: bool,
    output: &serde_json::Value,
) -> Option<(&'static str, Option<String>)> {
    if !success {
        return None;
    }
    let action = output.get("action").and_then(|a| a.as_str()).unwrap_or("");
    match action {
        "keep" => Some(("keep", None)),
        "forget" => Some(("forget", None)),
        // The user's free-text answer IS the new belief content
        // (ranch-2 offers `free_text` next to the Keep/Forget choices).
        "edit" => output
            .get("content")
            .and_then(|c| c.as_str())
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .map(|c| ("edit", Some(c))),
        _ => None,
    }
}

/// Apply a card answer (from `POST /ranch-tools/:id/result`) to the
/// belief its payload names. Runs AFTER the oneshot resolution, so a
/// storage failure here never wedges the tool-result endpoint — it is
/// logged and the response is still `ok` (ranchd has nothing left to
/// retry with; the belief remains in its last state).
pub(crate) fn apply_memory_review(
    state: &AppState,
    user: &AuthenticatedUser,
    payload: &serde_json::Value,
    output: &serde_json::Value,
    success: bool,
) {
    let Some(action) = memory_review_action(success, output) else {
        return; // no answer in time / relay failed: belief stays pending
    };
    let Ok(agent_id) = Uuid::parse_str(payload.get("agent").and_then(|v| v.as_str()).unwrap_or(""))
    else {
        tracing::warn!("memory_review apply: malformed payload (no agent)");
        return;
    };
    let Ok(belief_id) = Uuid::parse_str(
        payload
            .get("belief_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    ) else {
        tracing::warn!("memory_review apply: malformed payload (no belief_id)");
        return;
    };

    let state = state.clone();
    let caller = memory_caller(user);
    let content = action.1.clone();
    tokio::spawn(async move {
        let outcome: Result<crate::memory::Belief, crate::memory::MemoryError> = match action.0 {
            "keep" | "forget" => {
                crate::memory::set_status(
                    &state.db,
                    &caller,
                    agent_id,
                    belief_id,
                    if action.0 == "keep" {
                        "active"
                    } else {
                        "forgotten"
                    },
                    None,
                    "user-card",
                )
                .await
            }
            // Edit: new content + active + re-embed (best effort —
            // a failed embed nulls the vector, better than a stale
            // one ranking the wrong text).
            _ => {
                let new_content = content.unwrap();
                let embedding =
                    match crate::embedding::embed(&state.embedding_config, &new_content).await {
                        Ok(v) => Some(v),
                        Err(e) => {
                            tracing::warn!("memory_review edit: re-embed failed: {e}");
                            None
                        }
                    };
                crate::memory::edit_content(
                    &state.db,
                    &caller,
                    agent_id,
                    belief_id,
                    &new_content,
                    embedding,
                    "user-card",
                )
                .await
            }
        };
        match outcome {
            Ok(b) => tracing::info!(
                belief_id = %belief_id,
                action = %action.0,
                version = b.version,
                status = %b.status,
                "memory_review apply: belief updated"
            ),
            Err(e) => tracing::warn!(
                belief_id = %belief_id,
                action = %action.0,
                error = %e,
                "memory_review apply: belief transition failed (belief unchanged)"
            ),
        }
    });
}

// ---- tests ----

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_review_action_parses_card_answers() {
        // keep
        assert_eq!(
            memory_review_action(true, &json!({ "action": "keep" })),
            Some(("keep", None))
        );
        // forget
        assert_eq!(
            memory_review_action(true, &json!({ "action": "forget" })),
            Some(("forget", None))
        );
        // edit: the free text becomes the new content
        assert_eq!(
            memory_review_action(
                true,
                &json!({ "action": "edit", "content": "prefer concise replies" })
            ),
            Some(("edit", Some("prefer concise replies".into())))
        );
        // edit with blank text → no transition
        assert_eq!(
            memory_review_action(true, &json!({ "action": "edit", "content": "   " })),
            None
        );
        // deadline / failed relay → no transition
        assert_eq!(
            memory_review_action(false, &json!({ "action": "keep" })),
            None
        );
        assert_eq!(memory_review_action(true, &json!({})), None);
        assert_eq!(memory_review_action(true, &json!(null)), None);
        assert_eq!(
            memory_review_action(true, &json!({ "action": "nonsense" })),
            None
        );
    }
}
