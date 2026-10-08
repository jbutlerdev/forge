//! Herd H4: agent memory store — episodes, beliefs, org ACL, cross-agent
//! signals, and the belief audit trail.
//!
//! Tables come from migration `022_memory.sql` and only exist when
//! pgvector (>= 0.5, the `vector` extension) was installable at
//! migration time. [`vector_available`] probes that; the API layer turns
//! a negative probe into 501 and the tests follow the
//! skip-when-no-pgvector contract (`tests/memory_tests.rs`).
//!
//! Tenancy: every function takes the caller (owner-or-admin of the
//! agent, matching `api/agents.rs` — `can_access`) and enforces it
//! against `agents.owner_id`. Org-tier memory is shared through
//! [`memory_acl`]: two agents sharing an `org` label are in the same
//! memory org; `access = 'read'` includes the org tier in that
//! agent's reads, `'write'` lets it contribute org-scope beliefs.
//!
//! Embeddings: 2560-dim Qwen3-Embedding-4B vectors (see
//! [`crate::embedding::EMBEDDING_DIM`]). Stored as pgvector columns;
//! ranking uses pgvector's `<=>` (cosine distance) for retrieval and
//! [`crate::embedding::cosine_similarity`] for the pure in-process
//! ranking helpers (kept pure so they are testable without a database).

use crate::embedding::cosine_similarity;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Type};
use thiserror::Error;
use uuid::Uuid;

// ============================================
// Errors / identity
// ============================================

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("database error: {0}")]
    Db(#[source] sqlx::Error),
    #[error("agent not found")]
    AgentNotFound,
    #[error("forbidden: caller cannot access this agent")]
    Forbidden,
    #[error("belief not found")]
    BeliefNotFound,
    #[error("signal not found")]
    SignalNotFound,
    #[error("memory unavailable: pgvector not installed")]
    Unavailable,
}

/// The caller's tenancy identity: the user id + whether it is an admin
/// (mirrors `api::auth::AuthenticatedUser` without dragging the API
/// layer into the store).
#[derive(Debug, Clone, Copy)]
pub struct Caller {
    pub user_id: Uuid,
    pub is_admin: bool,
}

// ============================================
// Row types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, Type)]
pub struct Episode {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub conversation_id: Option<Uuid>,
    pub task_ref: Option<String>,
    pub summary: String,
    pub feedback: Option<serde_json::Value>,
    pub source: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// An episode as returned by [`search`]: the row plus its cosine
/// similarity to the query vector (`1 - <=>`, higher = closer).
#[derive(Debug, Clone, Serialize)]
pub struct EpisodeHit {
    #[serde(flatten)]
    pub episode: Episode,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, Type)]
pub struct Belief {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub scope: String,
    pub kind: String,
    pub content: String,
    pub confidence: f32,
    pub source_episodes: Vec<Uuid>,
    pub watch: Option<serde_json::Value>,
    pub status: String,
    pub superseded_by: Option<Uuid>,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// A belief as returned by [`search`]: the row plus its cosine
/// similarity to the query vector and its `scope` (callers render the
/// provenance: `source_episodes` + writer agent).
#[derive(Debug, Clone, Serialize)]
pub struct BeliefHit {
    #[serde(flatten)]
    pub belief: Belief,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct MemoryAcl {
    pub agent_id: Uuid,
    pub org: String,
    pub access: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AgentSignal {
    pub id: Uuid,
    pub from_agent: Uuid,
    pub to_agent: Option<Uuid>,
    pub kind: String,
    pub payload: serde_json::Value,
    pub consumed_by: Vec<Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct BeliefAudit {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub belief_id: Uuid,
    pub actor: String,
    pub change: String,
    pub detail: Option<serde_json::Value>,
    pub at: chrono::DateTime<chrono::Utc>,
}

/// The result of [`search`].
#[derive(Debug, Clone, Serialize, Default)]
pub struct MemorySearchResult {
    pub episodes: Vec<EpisodeHit>,
    pub beliefs: Vec<BeliefHit>,
}

/// pgvector text-literal for a vector column (`[0.1,0.2,…]`). sqlx
/// has no built-in pgvector type, so embeddings cross the wire as
/// their text form.
fn vector_literal(v: &[f32]) -> String {
    let inner = v
        .iter()
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{inner}]")
}

// ============================================
// Redaction (H4.2 episode capture; reusable by H4.4/H5)
// ============================================

/// Secret shapes masked by [`redact`]: `(pattern, replacement)` —
/// the replacement may use `$1`-style capture references. Compiled
/// once; the list is the single source of truth for which shapes are
/// considered secrets.
fn redaction_rules() -> &'static [(Regex, &'static str)] {
    static RULES: std::sync::OnceLock<[(Regex, &'static str); 5]> = std::sync::OnceLock::new();
    RULES.get_or_init(|| {
        [
            // `Authorization: …` headers: the scheme word (Bearer,
            // Basic, …) plus the value token.
            (
                Regex::new(r"(?i)\bauthorization\s*:\s*(?:[a-z]+\s+)?[\w.~+/=-]+").expect("static regex"),
                "Authorization: ***",
            ),
            // Standalone `Bearer` tokens.
            (
                Regex::new(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+").expect("static regex"),
                "Bearer ***",
            ),
            // `sk_` / `sk-` style API keys (OpenAI/Anthropic shapes).
            (
                Regex::new(r"sk[-_][A-Za-z0-9_-]{8,}").expect("static regex"),
                "sk_***",
            ),
            // `password=` / `token:` / `api_key=…` style assignments
            // (value = the next non-space run; case-insensitive).
            (
                Regex::new(
                    r"(?i)\b(password|passwd|secret|token|access[-_]?key|api[-_]?key)\b(\s*[:=]\s*)\S+",
                )
                .expect("static regex"),
                "$1$2***",
            ),
            // URL-embedded credentials: `https://user:pass@host`.
            (
                Regex::new(r"(?i)\b([a-z][a-z0-9+.-]*://)\S+:\S+@").expect("static regex"),
                "$1***@",
            ),
        ]
    })
}

/// Mask the known secret shapes out of captured text. Applied to
/// every piece of text that leaves the turn slice (episode summaries,
/// feedback sentences, commands, files) before it reaches a prompt,
/// an embedding, or the `episodes` table. Idempotent (a masked
/// value cannot re-match a rule).
pub fn redact(text: &str) -> String {
    let mut out = text.to_string();
    for (re, rep) in redaction_rules() {
        out = re.replace_all(&out, *rep).into_owned();
    }
    out
}

// ============================================
// Probes + tenancy
// ============================================

/// Is the `vector` extension installed on this database? (Migration 022
/// skips the memory tables when it was not available, so a `false` here
/// means every memory table is absent.)
pub async fn vector_available(db: &PgPool) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM pg_available_extensions WHERE name = 'vector' AND installed_version IS NOT NULL",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .is_some()
}

/// Fetch the agent the caller may access (owner or admin — the same
/// gate as `api::agents::agent_access_err`). `AgentNotFound` and
/// `Forbidden` are deliberately indistinguishable to the caller's
/// clients: both are "404 Agent not found" at the API layer.
pub async fn fetch_agent(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
) -> Result<Uuid, MemoryError> {
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
        .bind(agent_id)
        .fetch_optional(db)
        .await
        .map_err(MemoryError::Db)?;
    match owner {
        Some(owner_id) if caller.is_admin || owner_id == caller.user_id => Ok(agent_id),
        Some(_) => Err(MemoryError::Forbidden),
        None => Err(MemoryError::AgentNotFound),
    }
}

/// The org labels + access levels granted to an agent (its membership
/// in the shared memory tier).
pub async fn acl_list(db: &PgPool, agent_id: Uuid) -> Result<Vec<MemoryAcl>, MemoryError> {
    sqlx::query_as::<_, MemoryAcl>(
        "SELECT agent_id, org, access FROM memory_acl WHERE agent_id = $1 ORDER BY org",
    )
    .bind(agent_id)
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)
}

/// Can `reader` (grants) see the org-tier memory of an agent in orgs
/// `writer_orgs`? Pure: the API/store layer loads both sides via
/// [`acl_list`]; this is the matching rule itself.
pub fn org_readable(reader_grants: &[(String, String)], writer_orgs: &[String]) -> bool {
    reader_grants
        .iter()
        .any(|(org, access)| access == "read" && writer_orgs.iter().any(|w| w == org))
}

/// Can `writer` (grants) contribute to the org tier of any org in
/// `writer_orgs`? (The H4.4 reflection path checks this before an
/// org-scope belief write.)
pub fn org_writable(writer_grants: &[(String, String)], writer_orgs: &[String]) -> bool {
    writer_grants
        .iter()
        .any(|(org, access)| access == "write" && writer_orgs.iter().any(|w| w == org))
}

// ============================================
// Episodes
// ============================================

/// Insert one episode. `embedding` is the 2560-dim query vector for the
/// episode `summary` (None when the embedding endpoint was unavailable
/// — the row is still ranked by the B-tree time index and excluded from
/// cosine retrieval). `source` carries the provenance
/// (`{conversation_id, seq_range}`).
#[allow(clippy::too_many_arguments)]
pub async fn insert_episode(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    conversation_id: Option<Uuid>,
    task_ref: Option<&str>,
    summary: &str,
    feedback: Option<serde_json::Value>,
    embedding: Option<Vec<f32>>,
    source: serde_json::Value,
) -> Result<Episode, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let e = sqlx::query_as::<_, Episode>(
        r#"INSERT INTO episodes (agent_id, conversation_id, task_ref, summary, feedback, embedding, source)
           VALUES ($1, $2, $3, $4, $5, $6::vector, $7) RETURNING *"#,
    )
    .bind(agent_id)
    .bind(conversation_id)
    .bind(task_ref)
    .bind(summary)
    .bind(feedback)
    .bind(embedding.as_ref().map(|v| vector_literal(v)))
    .bind(&source)
    .fetch_one(db)
    .await
    .map_err(MemoryError::Db)?;
    Ok(e)
}

/// Cosine retrieval over the agent's episodes and active beliefs.
///
/// * `include_org`: also search the org tier — active org-scope beliefs
///   written by agents in the caller's read-granted orgs (via
///   [`memory_acl`]) are included; the agent's own org beliefs are
///   always included.
/// * `k` per collection (episodes and beliefs are ranked separately and
///   each truncated to `k`).
pub async fn search(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    query_vec: &[f32],
    k: i64,
    include_org: bool,
) -> Result<MemorySearchResult, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let q: String = vector_literal(query_vec);
    let k = k.clamp(1, 50);

    // Org-tier visibility set: the agent's own id plus every agent whose
    // orgs intersect a read-granted org of `agent_id`.
    let mut agent_ids = vec![agent_id];
    if include_org {
        let grants = acl_list(db, agent_id).await?;
        if !grants.is_empty() {
            let rows: Vec<Uuid> = sqlx::query_scalar(
                r#"SELECT DISTINCT m.agent_id
                   FROM memory_acl m
                   WHERE m.org IN (SELECT org FROM memory_acl WHERE agent_id = $1 AND access = 'read')
                     AND m.agent_id <> $1"#,
            )
            .bind(agent_id)
            .fetch_all(db)
            .await
            .map_err(MemoryError::Db)?;
            agent_ids.extend(rows);
        }
    }

    let episodes = sqlx::query_as::<_, (Episode, f32)>(
        r#"SELECT e.*, (1 - (e.embedding <=> $3::vector)) AS score
           FROM episodes e
           WHERE e.agent_id = $1 AND e.embedding IS NOT NULL
           ORDER BY e.embedding <=> $3::vector
           LIMIT $2"#,
    )
    .bind(agent_id)
    .bind(k)
    .bind(&q)
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)?;

    // The `agent_ids` array already encodes the org ACL (own agent +
    // read-granted-org writers); org-scope beliefs are included only
    // when `include_org` admitted the writer set at all — so when it is
    // off, restrict to the agent's own rows.
    let writer_ids: Vec<Uuid> = if include_org {
        agent_ids
    } else {
        vec![agent_id]
    };
    let beliefs = sqlx::query_as::<_, (Belief, f32)>(
        r#"SELECT b.*, (1 - (b.embedding <=> $3::vector)) AS score
           FROM beliefs b
           WHERE b.agent_id = ANY($1) AND b.status = 'active' AND b.embedding IS NOT NULL
           ORDER BY b.embedding <=> $3::vector
           LIMIT $2"#,
    )
    .bind(&writer_ids)
    .bind(k)
    .bind(&q)
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)?;

    Ok(MemorySearchResult {
        episodes: episodes
            .into_iter()
            .map(|(e, s)| EpisodeHit {
                episode: e,
                score: s,
            })
            .collect(),
        beliefs: beliefs
            .into_iter()
            .map(|(b, s)| BeliefHit {
                belief: b,
                score: s,
            })
            .collect(),
    })
}

// ============================================
// Beliefs
// ============================================

/// List beliefs of an agent, optionally filtered by status.
pub async fn list(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<Belief>, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let limit = limit.clamp(1, 200);
    match status {
        Some(s) => Ok(sqlx::query_as::<_, Belief>(
            "SELECT * FROM beliefs WHERE agent_id = $1 AND status = $2 ORDER BY updated_at DESC LIMIT $3",
        )
        .bind(agent_id)
        .bind(s)
        .bind(limit)
        .fetch_all(db)
        .await
        .map_err(MemoryError::Db)?),
        None => Ok(sqlx::query_as::<_, Belief>(
            "SELECT * FROM beliefs WHERE agent_id = $1 ORDER BY updated_at DESC LIMIT $2",
        )
        .bind(agent_id)
        .bind(limit)
        .fetch_all(db)
        .await
        .map_err(MemoryError::Db)?),
    }
}

/// The top-`limit` ACTIVE beliefs by confidence (no embedding needed —
/// the prompt-section confidence pass, H4.3).
pub async fn top_confidence(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    limit: i64,
) -> Result<Vec<Belief>, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    sqlx::query_as::<_, Belief>(
        "SELECT * FROM beliefs WHERE agent_id = $1 AND status = 'active'
         ORDER BY confidence DESC, updated_at DESC LIMIT $2",
    )
    .bind(agent_id)
    .bind(limit.clamp(1, 100))
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)
}

/// Fetch one belief belonging to the agent.
pub async fn get(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    belief_id: Uuid,
) -> Result<Belief, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    match sqlx::query_as::<_, Belief>("SELECT * FROM beliefs WHERE id = $1 AND agent_id = $2")
        .bind(belief_id)
        .bind(agent_id)
        .fetch_optional(db)
        .await
        .map_err(MemoryError::Db)?
    {
        Some(b) => Ok(b),
        None => Err(MemoryError::BeliefNotFound),
    }
}

/// The belief's audit trail (version chain, most recent first).
pub async fn audit(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    belief_id: Uuid,
) -> Result<Vec<BeliefAudit>, MemoryError> {
    get(db, caller, agent_id, belief_id).await?;
    sqlx::query_as::<_, BeliefAudit>(
        "SELECT * FROM belief_audit WHERE belief_id = $1 ORDER BY at DESC, id DESC",
    )
    .bind(belief_id)
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)
}

/// Insert a `pending` belief (version 1) — the H4.2
/// `memory_remember` path and the H4.4 proposal path. Writes the
/// initial audit row (`actor`, who inserted it).
#[allow(clippy::too_many_arguments)]
pub async fn upsert_pending(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    scope: &str,
    kind: &str,
    content: &str,
    confidence: f32,
    source_episodes: Vec<Uuid>,
    embedding: Option<Vec<f32>>,
    actor: &str,
) -> Result<Belief, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let b = sqlx::query_as::<_, Belief>(
        r#"INSERT INTO beliefs (agent_id, scope, kind, content, confidence, source_episodes, status, embedding, version)
           VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7::vector, 1) RETURNING *"#,
    )
    .bind(agent_id)
    .bind(scope)
    .bind(kind)
    .bind(content)
    .bind(confidence)
    .bind(&source_episodes)
    .bind(embedding.as_ref().map(|v| vector_literal(v)))
    .fetch_one(db)
    .await
    .map_err(MemoryError::Db)?;
    sqlx::query(
        r#"INSERT INTO belief_audit (agent_id, belief_id, actor, change, detail)
           VALUES ($1, $2, $3, 'created', $4)"#,
    )
    .bind(agent_id)
    .bind(b.id)
    .bind(actor)
    .bind(serde_json::json!({ "kind": kind, "content": content }))
    .execute(db)
    .await
    .map_err(MemoryError::Db)?;
    Ok(b)
}

/// Transition a belief's status (`pending` → `active` / `forgotten`,
/// `active` → `superseded` with `superseded_by`), bumping `version` and
/// appending an audit row.
pub async fn set_status(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    belief_id: Uuid,
    status: &str,
    superseded_by: Option<Uuid>,
    actor: &str,
) -> Result<Belief, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let b = sqlx::query_as::<_, Belief>(
        r#"UPDATE beliefs
           SET status = $3, superseded_by = $4, version = version + 1, updated_at = NOW()
           WHERE id = $1 AND agent_id = $2
           RETURNING *"#,
    )
    .bind(belief_id)
    .bind(agent_id)
    .bind(status)
    .bind(superseded_by)
    .fetch_optional(db)
    .await
    .map_err(MemoryError::Db)?
    .ok_or(MemoryError::BeliefNotFound)?;
    sqlx::query(
        r#"INSERT INTO belief_audit (agent_id, belief_id, actor, change, detail)
           VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind(agent_id)
    .bind(belief_id)
    .bind(actor)
    .bind(status)
    .bind(serde_json::json!({ "version": b.version }))
    .execute(db)
    .await
    .map_err(MemoryError::Db)?;
    Ok(b)
}

// ============================================
// Memory ACL
// ============================================

/// Grant (upsert) an org access level for an agent.
pub async fn acl_grant(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    org: &str,
    access: &str,
) -> Result<MemoryAcl, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    if !["read", "write"].contains(&access) {
        return Err(MemoryError::Db(sqlx::Error::Protocol(
            "invalid access level".into(),
        )));
    }
    sqlx::query_as::<_, MemoryAcl>(
        r#"INSERT INTO memory_acl (agent_id, org, access) VALUES ($1, $2, $3)
           ON CONFLICT (agent_id, org) DO UPDATE SET access = EXCLUDED.access
           RETURNING *"#,
    )
    .bind(agent_id)
    .bind(org)
    .bind(access)
    .fetch_one(db)
    .await
    .map_err(MemoryError::Db)
}

// ============================================
// Agent signals (cross-agent bus, H4.6)
// ============================================

/// Post a signal. `to_agent` None = org broadcast (delivered to every
/// agent in the sender's read-granted orgs — the receiver-side
/// [`unread`] handles that).
#[allow(clippy::too_many_arguments)]
pub async fn insert_signal(
    db: &PgPool,
    caller: &Caller,
    from_agent: Uuid,
    to_agent: Option<Uuid>,
    kind: &str,
    payload: serde_json::Value,
    embedding: Option<Vec<f32>>,
) -> Result<AgentSignal, MemoryError> {
    fetch_agent(db, caller, from_agent).await?;
    if let Some(target) = to_agent {
        fetch_agent(db, caller, target).await?;
    }
    let s = sqlx::query_as::<_, AgentSignal>(
        r#"INSERT INTO agent_signals (from_agent, to_agent, kind, payload, embedding)
           VALUES ($1, $2, $3, $4, $5::vector) RETURNING *"#,
    )
    .bind(from_agent)
    .bind(to_agent)
    .bind(kind)
    .bind(&payload)
    .bind(embedding.as_ref().map(|v| vector_literal(v)))
    .fetch_one(db)
    .await
    .map_err(MemoryError::Db)?;
    Ok(s)
}

/// The agent's unread signals: direct (`to_agent = agent`) or org
/// broadcast (`to_agent IS NULL` from an agent sharing a read-granted
/// org), not yet in `consumed_by`. `kind` filters on the signal kind.
pub async fn unread(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    kind: Option<&str>,
    limit: i64,
) -> Result<Vec<AgentSignal>, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let limit = limit.clamp(1, 200);
    let direct = sqlx::query_as::<_, AgentSignal>(
        r#"SELECT * FROM agent_signals
           WHERE to_agent = $1 AND ($2::text IS NULL OR kind = $2)
             AND $1 = ANY(consumed_by) IS FALSE
           ORDER BY created_at LIMIT $3"#,
    )
    .bind(agent_id)
    .bind(kind)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)?;
    // Org broadcasts: from agents in the caller's read-granted orgs.
    let broadcasts = sqlx::query_as::<_, AgentSignal>(
        r#"SELECT * FROM agent_signals s
           WHERE s.to_agent IS NULL AND ($2::text IS NULL OR s.kind = $2)
             AND $1 = ANY(s.consumed_by) IS FALSE
             AND s.from_agent IN (
                   SELECT m.agent_id FROM memory_acl m
                   WHERE m.org IN (
                       SELECT org FROM memory_acl WHERE agent_id = $1 AND access = 'read')
                     AND m.agent_id <> $1)
           ORDER BY s.created_at LIMIT $3"#,
    )
    .bind(agent_id)
    .bind(kind)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(MemoryError::Db)?;
    let mut out = direct;
    out.extend(broadcasts);
    out.sort_by_key(|s| s.created_at);
    Ok(out)
}

/// Mark a signal consumed by the agent (idempotent — the agent id is
/// not appended twice).
pub async fn mark_consumed(
    db: &PgPool,
    caller: &Caller,
    agent_id: Uuid,
    signal_id: Uuid,
) -> Result<AgentSignal, MemoryError> {
    fetch_agent(db, caller, agent_id).await?;
    let s = sqlx::query_as::<_, AgentSignal>(
        r#"UPDATE agent_signals
           SET consumed_by = consumed_by || $2
           WHERE id = $1 AND $2 = ANY(consumed_by) IS FALSE
           RETURNING *"#,
    )
    .bind(signal_id)
    .bind(agent_id)
    .fetch_optional(db)
    .await
    .map_err(MemoryError::Db)?;
    // A second call leaves the row untouched; fetch it either way.
    match s {
        Some(s) => Ok(s),
        None => sqlx::query_as::<_, AgentSignal>("SELECT * FROM agent_signals WHERE id = $1")
            .bind(signal_id)
            .fetch_optional(db)
            .await
            .map_err(MemoryError::Db)?
            .ok_or(MemoryError::SignalNotFound),
    }
}

// ============================================
// Pure ranking helper (H4.3 prompt-section pass)
// ============================================

/// Rank `(id, embedding?)` candidates by cosine similarity to `query`.
/// Candidates without an embedding sort last with score 0.0. Ties break
/// on the candidate index (stable, input order). Pure: no I/O, so the
/// H4.3 ranking is testable without a database or the vector extension.
pub fn rank_by_cosine(
    query: &[f32],
    candidates: &[(Uuid, Option<&[f32]>)],
    k: usize,
) -> Vec<(Uuid, f32)> {
    let mut scored: Vec<(Uuid, f32, usize)> = candidates
        .iter()
        .enumerate()
        .map(|(i, (id, emb))| {
            let score = emb.map_or(0.0, |e| cosine_similarity(query, e));
            (*id, score, i)
        })
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.2.cmp(&b.2)));
    scored
        .into_iter()
        .take(k)
        .map(|(id, s, _)| (id, s))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_masks_api_keys() {
        assert_eq!(
            redact("use the key sk-abcDEF123456789 for the call"),
            "use the key sk_*** for the call"
        );
        assert_eq!(
            redact("ANTHROPIC=sk_abcdefghijklmnopqr"),
            "ANTHROPIC=sk_***"
        );
        // Too short to be a key: untouched.
        assert_eq!(redact("sk-short"), "sk-short");
        // Idempotent.
        let once = redact("Bearer abc.def-ghi_123");
        assert_eq!(redact(&once), once);
    }

    #[test]
    fn redact_masks_bearer_and_authorization() {
        assert_eq!(
            redact("curl -H 'Authorization: Bearer eyJhbGci.eyJub2Rl' https://x"),
            "curl -H 'Authorization: ***' https://x"
        );
        assert_eq!(redact("Authorization:bearer abc123"), "Authorization: ***");
        assert_eq!(redact("-H 'Bearer abc123'"), "-H 'Bearer ***'");
    }

    #[test]
    fn redact_masks_assignment_secrets() {
        assert_eq!(
            redact("export password=hunter2 now"),
            "export password=*** now"
        );
        assert_eq!(redact("token: s3cr3t-value"), "token: ***");
        assert_eq!(redact("api_key = sk-live-abcdef123456"), "api_key = ***");
        assert_eq!(redact("Access-Key: KK99"), "Access-Key: ***");
        // A word without an assignment is left alone.
        assert_eq!(
            redact("the token rotation worked"),
            "the token rotation worked"
        );
    }

    #[test]
    fn redact_masks_url_credentials() {
        assert_eq!(
            redact("git clone https://bot:ghp_x9y8z7w6v5u4@host/repo"),
            "git clone https://***@host/repo"
        );
    }

    fn grants() -> Vec<(String, String)> {
        vec![
            ("alpha".into(), "read".into()),
            ("beta".into(), "write".into()),
        ]
    }

    #[test]
    fn org_readable_requires_matching_org_and_read() {
        let g = grants();
        assert!(org_readable(&g, &["alpha".into()]));
        assert!(org_readable(&g, &["alpha".into(), "gamma".into()]));
        assert!(!org_readable(&g, &["beta".into()])); // write grant ≠ read
        assert!(!org_readable(&g, &["gamma".into()])); // unknown org
        assert!(!org_readable(&g, &[]));
        assert!(!org_readable(&[], &["alpha".into()]));
    }

    #[test]
    fn org_writable_requires_matching_org_and_write() {
        let g = grants();
        assert!(org_writable(&g, &["beta".into()]));
        assert!(!org_writable(&g, &["alpha".into()]));
        assert!(!org_writable(&g, &[]));
    }

    #[test]
    fn rank_by_cosine_orders_by_similarity_and_respects_k() {
        let query = vec![1.0f32, 0.0, 0.0];
        let a = vec![1.0f32, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0];
        let c = vec![0.5f32, 0.5, 0.0];
        let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let ranked = rank_by_cosine(
            &query,
            &[(ids[0], Some(&b)), (ids[1], Some(&a)), (ids[2], Some(&c))],
            2,
        );
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].0, ids[1]);
        assert!((ranked[0].1 - 1.0).abs() < 1e-6);
        assert_eq!(ranked[1].0, ids[2]);
        assert!((ranked[1].1 - 2.0f32.sqrt() / 2.0).abs() < 1e-6);
    }

    #[test]
    fn rank_by_cosine_puts_unembedded_last() {
        let query = vec![1.0f32, 0.0];
        let a = vec![1.0f32, 0.0];
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        let ranked = rank_by_cosine(&query, &[(ids[0], None), (ids[1], Some(&a))], 5);
        assert_eq!(ranked[0].0, ids[1]);
        assert_eq!(ranked[1].0, ids[0]);
        assert_eq!(ranked[1].1, 0.0);
    }
}
