//! Bitwarden (Vaultwarden) Secrets Manager bridge (2026-10-18).
//!
//! Why: agents need real credentials (GITHUB_TOKEN, API keys) without
//! the user copy-pasting vault values into forge and without forge
//! ever *storing* them. The H6.5 credential slots become the seam: a
//! slot's stored value may be a **reference** — `bw://<sm-key>` —
//! which is resolved here, at sandboxed-bash exec time, against the
//! owner's self-hosted Vaultwarden via the Secrets Manager
//! (`/align`-compatible) API. Values flow ONLY into the container
//! env (`--setenv=`), exactly like native slot values — never into
//! messages, model context, or any API response.
//!
//! Flow per resolution:
//!   1. load `bitwarden_config` (one row per owner, migration 029);
//!   2. mint/cache the SM access token (`POST /connect/token`,
//!      client_credentials, scope `api.secrets`) — cached in-process
//!      with a 60 s safety margin on `expires_in`;
//!   3. `GET /api/secrets/{key}` → `{"key":…, "value":…}`.
//!
//! Failure semantics mirror "slot unset": resolution errors degrade
//! to `Ok(None)` at the credentials layer (the agent gets the
//! *declared-but-unavailable* message, not a 500), with the real
//! cause logged server-side.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;

use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

/// Slot-value scheme prefix marking a vault reference.
pub const BW_SCHEME: &str = "bw://";

pub fn is_bw_ref(value: &str) -> bool {
    value.trim_start().starts_with(BW_SCHEME)
}

/// Extract the SM secret key from a `bw://` reference.
pub fn bw_key(value: &str) -> Option<&str> {
    let k = value.trim()[BW_SCHEME.len()..].trim();
    if k.is_empty() || k.len() > 200 {
        None
    } else {
        Some(k)
    }
}

/// True when the URL is acceptable as a vault endpoint (https always;
/// http only for explicit localhost/LAN — the lab's own vault).
pub fn valid_server_url(u: &str) -> bool {
    let u = u.trim().trim_end_matches('/');
    if let Some(rest) = u.strip_prefix("https://") {
        return !rest.is_empty() && !rest.contains(' ');
    }
    if let Some(rest) = u.strip_prefix("http://") {
        let host = rest.split('/').next().unwrap_or("");
        let host_only = host.split(':').next().unwrap_or("");
        return host_only == "localhost"
            || host_only == "127.0.0.1"
            || host_only.starts_with("10.")
            || host_only.starts_with("192.168.")
            || host_only.starts_with("172.");
    }
    false
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ConfigRow {
    server_url: String,
    org_id: String,
    client_id: String,
    client_secret: String,
    enabled: bool,
}

async fn load_config(db: &PgPool, owner_id: Uuid) -> Result<Option<ConfigRow>, sqlx::Error> {
    sqlx::query_as::<_, ConfigRow>(
        "SELECT server_url, org_id, client_id, client_secret, enabled \
         FROM bitwarden_config WHERE owner_id = $1",
    )
    .bind(owner_id)
    .fetch_optional(db)
    .await
}

/// True when the owner has a usable vault connection (used by the
/// secrets API to label slots as vault-backed).
pub async fn configured(db: &PgPool, owner_id: Uuid) -> bool {
    matches!(load_config(db, owner_id).await, Ok(Some(c)) if c.enabled)
}

pub async fn upsert_config(
    db: &PgPool,
    owner_id: Uuid,
    server_url: &str,
    org_id: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO bitwarden_config (owner_id, server_url, org_id, client_id, client_secret) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (owner_id) DO UPDATE SET \
           server_url = EXCLUDED.server_url, org_id = EXCLUDED.org_id, \
           client_id = EXCLUDED.client_id, client_secret = EXCLUDED.client_secret, \
           enabled = true, updated_at = NOW()",
    )
    .bind(owner_id)
    .bind(server_url.trim().trim_end_matches('/'))
    .bind(org_id.trim())
    .bind(client_id.trim())
    .bind(client_secret.trim())
    .execute(db)
    .await?;
    Ok(())
}

pub async fn delete_config(db: &PgPool, owner_id: Uuid) -> Result<bool, sqlx::Error> {
    let r = sqlx::query("DELETE FROM bitwarden_config WHERE owner_id = $1")
        .bind(owner_id)
        .execute(db)
        .await?;
    Ok(r.rows_affected() > 0)
}

/// Metadata for the client (values NEVER included).
pub async fn config_meta(db: &PgPool, owner_id: Uuid) -> Result<Option<serde_json::Value>, sqlx::Error> {
    Ok(load_config(db, owner_id).await?.map(|c| {
        serde_json::json!({
            "server_url": c.server_url,
            "org_id": c.org_id,
            "client_id": mask(&c.client_id),
            "enabled": c.enabled,
        })
    }))
}

fn mask(s: &str) -> String {
    let head: String = s.chars().take(6).collect();
    format!("{head}…")
}

// ============================================
// SM token cache + HTTP
// ============================================

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

fn token_cache() -> &'static tokio::sync::Mutex<HashMap<Uuid, CachedToken>> {
    static CACHE: OnceLock<tokio::sync::Mutex<HashMap<Uuid, CachedToken>>> = OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

fn client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_default()
        })
        .clone()
}

async fn sm_access_token(db: &PgPool, owner_id: Uuid, cfg: &ConfigRow) -> Result<String, String> {
    {
        let cache = token_cache().lock().await;
        if let Some(c) = cache.get(&owner_id) {
            if c.expires_at > Instant::now() {
                return Ok(c.token.clone());
            }
        }
    }
    let resp = client()
        .post(format!("{}/connect/token", cfg.server_url))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
            ("scope", "api.secrets"),
        ])
        .send()
        .await
        .map_err(|e| format!("bitwarden token request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "bitwarden token HTTP {status}: {}",
            body.chars().take(200).collect::<String>()
        ));
    }
    let tr: TokenResponse = resp
        .json()
        .await
        .map_err(|e| format!("bitwarden token decode: {e}"))?;
    let ttl = tr.expires_in.unwrap_or(600).saturating_sub(60).max(30);
    token_cache().lock().await.insert(
        owner_id,
        CachedToken {
            token: tr.access_token.clone(),
            expires_at: Instant::now() + std::time::Duration::from_secs(ttl),
        },
    );
    Ok(tr.access_token)
}

/// Resolve one `bw://` slot value to its vault value. `Ok(None)` =
/// key not found in the vault (behaves like an unset slot); `Err` =
/// config/network problems (logged by the caller, still degrades to
/// "unavailable" at the credentials layer).
pub async fn resolve_bw_value(
    db: &PgPool,
    owner_id: Uuid,
    reference: &str,
) -> Result<Option<String>, String> {
    let key = bw_key(reference).ok_or("bw:// reference has an empty key")?;
    let cfg = load_config(db, owner_id)
        .await
        .map_err(|e| format!("bitwarden config read: {e}"))?
        .filter(|c| c.enabled)
        .ok_or("bitwarden not configured for this owner")?;
    let token = sm_access_token(db, owner_id, &cfg).await?;
    let url = format!("{}/api/secrets/{}", cfg.server_url, key);
    let resp = client()
        .get(&url)
        .query(&[("organizationId", cfg.org_id.as_str())])
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| format!("bitwarden fetch {key}: {e}"))?;
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        // token invalidated server-side (rotation): drop cache once.
        token_cache().lock().await.remove(&owner_id);
        return Err("bitwarden token rejected (cache cleared; retry)".into());
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "bitwarden fetch {key}: HTTP {status} {}",
            body.chars().take(160).collect::<String>()
        ));
    }
    let v: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("bitwarden decode {key}: {e}"))?;
    Ok(v.get("value").and_then(|x| x.as_str()).map(|s| s.to_string()))
}

/// List the vault's SM secret KEY names (never values) — the picker
/// feed for `PUT /secrets/<slot>` (set a slot to `bw://<key>`).
pub async fn list_bw_keys(db: &PgPool, owner_id: Uuid) -> Result<Vec<String>, String> {
    let cfg = load_config(db, owner_id)
        .await
        .map_err(|e| format!("bitwarden config read: {e}"))?
        .filter(|c| c.enabled)
        .ok_or("bitwarden not configured for this owner")?;
    let token = sm_access_token(db, owner_id, &cfg).await?;
    let resp = client()
        .get(format!("{}/api/secrets", cfg.server_url))
        .query(&[
            ("organizationId", cfg.org_id.as_str()),
            ("skip", "0"),
            ("take", "1000"),
        ])
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| format!("bitwarden list: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "bitwarden list: HTTP {status} {}",
            body.chars().take(160).collect::<String>()
        ));
    }
    let v: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("bitwarden list decode: {e}"))?;
    Ok(v.get("data")
        .and_then(|d| d.get("data"))
        .and_then(|d| d.as_array())
        .or_else(|| v.get("data").and_then(|d| d.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|item| item.get("key").and_then(|k| k.as_str()).map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default())
}

// ============================================
// tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refs_parse() {
        assert!(is_bw_ref("bw://github-token"));
        assert!(is_bw_ref("  bw://space in/key"));
        assert!(!is_bw_ref("plain-value"));
        assert_eq!(bw_key("bw://github-token").unwrap(), "github-token");
        assert_eq!(bw_key("bw://"), None);
        assert_eq!(bw_key("bw://   "), None);
    }

    #[test]
    fn server_url_policy() {
        assert!(valid_server_url("https://vault.butler.ooo"));
        assert!(valid_server_url("https://vault.butler.ooo/"));
        assert!(valid_server_url("http://192.168.67.5:8080"));
        assert!(valid_server_url("http://localhost:8037"));
        assert!(!valid_server_url("http://evil.example.com")); // public http
        assert!(!valid_server_url("ftp://nope"));
        assert!(!valid_server_url("https:// spaced"));
    }

    #[test]
    fn client_id_masked() {
        assert_eq!(mask("serviceAccount:abcdef123"), "servic…");
    }
}
