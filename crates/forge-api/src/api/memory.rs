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
