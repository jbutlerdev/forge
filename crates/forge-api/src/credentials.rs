//! Herd H6.5: the agent credentials scope — named credential slots.
//!
//! `agents.credentials_scope` is a JSON object of the shape
//! `{env_refs: ["SLOT_NAME", …]}`: the NAMES of the secret slots the
//! agent's sandboxed bash calls get as environment variables. The
//! values live in the `secrets` table (migration 027) and are
//! resolved HERE, at tool-execution time, and never serialized into:
//!
//!   - `messages` rows (tool call/result rows carry the slot NAMES at
//!     most — the bash tool's input is a plain command),
//!   - the `agents` row itself (the JSONB holds names only),
//!   - the ranch approval cards (the `web_login` card carries the URL
//!     + slot name, never a credential),
//!   - any API response (`GET /secrets` lists names + expiry only).
//!
//! Resolution is fail-closed: if ANY declared ref has no live value
//! (missing, or past `valid_until`), the bash call is refused with an
//! error naming the slot(s) — never the values. Running the sandbox
//! with a silently-empty credential env would just teach the agent to
//! type the password into a command, which lands in the transcript.
//!
//! Ownership: a session's slots resolve against the session's USER
//! (`sessions.user_id`) — the person who owns the conversation, who
//! signed in to get the cookie, and who manages the secrets.

use sqlx::PgPool;
use uuid::Uuid;

/// One (slot name, live value) pair, ready for the nspawn `--setenv=`
/// passthrough.
pub type ResolvedEnv = Vec<(String, String)>;

/// Why a session's sandboxed bash call can't get its credential env.
///
/// * `Unavailable` — declared slots with no live value. The names are
///   safe to surface to the model (names are not secrets); the values
///   never leave this module except into the sandbox argv.
/// * `Db` — the resolution query itself failed; the caller fails the
///   call with a generic message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    Unavailable(Vec<String>),
    Db(String),
}

impl CredentialError {
    /// The unavailable slot names (`Unavailable` arm only).
    pub fn unavailable_names(&self) -> &[String] {
        match self {
            Self::Unavailable(names) => names,
            Self::Db(_) => &[],
        }
    }
}

/// Fetch the session's `(user_id, agent_id)` pair. `None` when the
/// session row is gone (the executor's own tenancy check already
/// handles that; a resolution here is simply "no agent").
async fn session_owner_and_agent(
    db: &PgPool,
    session_id: Uuid,
) -> Result<Option<(Uuid, Option<Uuid>)>, String> {
    sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT user_id, agent_id FROM sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("credential resolution: session lookup failed: {e}"))
}

/// The declared slot names: the agent's `credentials_scope.env_refs`
/// (JSON string array; anything else is treated as no refs).
async fn env_refs(db: &PgPool, agent_id: Uuid) -> Result<Vec<String>, String> {
    let scope: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT credentials_scope FROM agents WHERE id = $1")
            .bind(agent_id)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("credential resolution: agent lookup failed: {e}"))?;
    let Some(scope) = scope else {
        return Ok(Vec::new());
    };
    let arr = scope.get("env_refs").and_then(|v| v.as_array());
    Ok(arr
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_string))
        .filter(|s| !s.is_empty())
        .collect())
}

/// Resolve one slot for an owner. `Ok(None)` = no live value (either
/// never set, or expired).
pub async fn resolve_secret(
    db: &PgPool,
    owner_id: Uuid,
    name: &str,
) -> Result<Option<String>, String> {
    let row: Option<(String, Option<chrono::DateTime<chrono::Utc>>)> =
        sqlx::query_as("SELECT value, valid_until FROM secrets WHERE owner_id = $1 AND name = $2")
            .bind(owner_id)
            .bind(name)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("credential resolution: secret lookup failed: {e}"))?;
    match row {
        None => Ok(None),
        Some((value, valid_until)) => match valid_until {
            Some(until) if until <= chrono::Utc::now() => Ok(None), // expired
            _ => Ok(Some(value)),
        },
    }
}

/// The full resolution for one session's sandboxed bash call:
/// every declared ref → its live value. `Ok(env)` is the passthrough
/// (empty when the agent declares no refs — the common case);
/// `Err(CredentialError::Unavailable)` lists the names that have no
/// live value — caller fails the call with a message built from that
/// list ONLY.
pub async fn resolve_credential_env(
    db: &PgPool,
    session_id: Uuid,
) -> Result<ResolvedEnv, CredentialError> {
    let (owner_id, agent_id) = match session_owner_and_agent(db, session_id).await {
        Ok(Some(p)) => p,
        Ok(None) => return Ok(Vec::new()),
        Err(e) => return Err(CredentialError::Db(e)),
    };
    let agent_id = match agent_id {
        Some(a) => a,
        None => return Ok(Vec::new()),
    };
    let refs = match env_refs(db, agent_id).await {
        Ok(r) => r,
        Err(e) => return Err(CredentialError::Db(e)),
    };
    if refs.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(refs.len());
    let mut unavailable = Vec::new();
    for name in refs {
        match resolve_secret(db, owner_id, &name).await {
            Ok(Some(value)) => out.push((name, value)),
            Ok(None) => unavailable.push(name),
            Err(e) => return Err(CredentialError::Db(e)),
        }
    }
    if unavailable.is_empty() {
        Ok(out)
    } else {
        Err(CredentialError::Unavailable(unavailable))
    }
}

// ============================================
// Secret store primitives (used by api/secrets.rs,
// api/weblogin.rs, and the unit tests)
// ============================================

/// Upsert an owner's slot. `valid_until` = None for no expiry.
pub async fn set_secret(
    db: &PgPool,
    owner_id: Uuid,
    name: &str,
    value: &str,
    valid_until: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), String> {
    sqlx::query(
        r#"INSERT INTO secrets (owner_id, name, value, valid_until)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (owner_id, name)
           DO UPDATE SET value = EXCLUDED.value,
                         valid_until = EXCLUDED.valid_until,
                         updated_at = NOW()"#,
    )
    .bind(owner_id)
    .bind(name)
    .bind(value)
    .bind(valid_until)
    .execute(db)
    .await
    .map_err(|e| format!("failed to store secret slot '{name}': {e}"))?;
    Ok(())
}

/// Delete an owner's slot. `true` when a row was removed.
pub async fn delete_secret(db: &PgPool, owner_id: Uuid, name: &str) -> Result<bool, String> {
    let r = sqlx::query("DELETE FROM secrets WHERE owner_id = $1 AND name = $2")
        .bind(owner_id)
        .bind(name)
        .execute(db)
        .await
        .map_err(|e| format!("failed to delete secret slot '{name}': {e}"))?;
    Ok(r.rows_affected() > 0)
}

/// One slot's metadata — NO value (the API's list shape).
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct SecretMeta {
    pub name: String,
    pub valid_until: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub async fn list_secret_meta(db: &PgPool, owner_id: Uuid) -> Result<Vec<SecretMeta>, String> {
    sqlx::query_as::<_, SecretMeta>(
        "SELECT name, valid_until, updated_at FROM secrets \
         WHERE owner_id = $1 ORDER BY name",
    )
    .bind(owner_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("failed to list secret slots: {e}"))
}
