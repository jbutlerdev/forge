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
    /// 'sm' (Secrets Manager /align) | 'bw-cli' (Vaultwarden via the
    /// official CLI — vaultwarden ships no SM endpoints).
    mode: String,
    /// bw-cli mode: the vault account email (the master password is
    /// FORGE_BW_MASTER_PASSWORD env, never the DB).
    email: String,
}

async fn load_config(db: &PgPool, owner_id: Uuid) -> Result<Option<ConfigRow>, sqlx::Error> {
    sqlx::query_as::<_, ConfigRow>(
        "SELECT server_url, org_id, client_id, client_secret, enabled, mode, email \
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
    mode: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO bitwarden_config \
         (owner_id, server_url, org_id, client_id, client_secret, mode, email) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (owner_id) DO UPDATE SET \
           server_url = EXCLUDED.server_url, org_id = EXCLUDED.org_id, \
           client_id = EXCLUDED.client_id, client_secret = EXCLUDED.client_secret, \
           mode = EXCLUDED.mode, email = EXCLUDED.email, \
           enabled = true, updated_at = NOW()",
    )
    .bind(owner_id)
    .bind(server_url.trim().trim_end_matches('/'))
    .bind(org_id.trim())
    .bind(client_id.trim())
    .bind(client_secret.trim())
    .bind(mode)
    .bind(email.trim())
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
            "mode": c.mode,
            "org_id": c.org_id,
            "client_id": mask(&c.client_id),
            "email": c.email,
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
    if cfg.mode == "bw-cli" {
        return resolve_via_bw_cli(&cfg, key).await;
    }
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
    if cfg.mode == "bw-cli" {
        return list_via_bw_cli(&cfg).await;
    }
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
// bw-CLI mode (Vaultwarden — no SM endpoints upstream)
// ============================================
//
// Runs the official `bw` CLI (shipped on the forge-api unit's PATH by
// the flake) against the owner's Vaultwarden. The process keeps one
// unlocked session in-process; a locked/expired state triggers
// login+unlock with FORGE_BW_MASTER_PASSWORD (unit env — the DB never
// sees it). Reference grammar (after `bw://`):
//
//   notes/<item-name>                  item notes (SecureNote body)
//   item/<name>/login/username|password|totp
//   item/<name>/fields/<field-name>    a custom field's value
//   item/<name>/notes                  same as notes/<name>
//
// `bw get item <name> --response` backs all of them, so selection is
// JSON navigation in Rust (no jq dependency).

/// bw prints informational chatter ("Could not find dir…") to stdout
/// *before* the `--response` JSON; parse from the first brace.
fn json_tail(raw: &str) -> Option<&str> {
    raw.find('{').map(|i| &raw[i..])
}

/// `bw status` shape-drifts between CLI generations (bare
/// {"status":…} vs {"success":true,"data":{…,"status":…}}); read
/// either.
fn bw_status_of(raw: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json_tail(raw)?).ok()?;
    for path in [
        vec!["status"],
        vec!["data", "status"],
        // bw 2026.8: {"success":true,"data":{"object":"template",
        //            "template":{"serverUrl":…,"status":"unauthenticated"}}}
        vec!["data", "template", "status"],
    ] {
        let mut cur = Some(&v);
        for k in path {
            cur = cur.and_then(|c| c.get(k));
        }
        if let Some(s) = cur.and_then(|c| c.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}

#[derive(Deserialize)]
struct BwUnlockResp {
    data: BwUnlockData,
}

#[derive(Deserialize)]
struct BwUnlockData {
    // pre-2026.8 CLI: {"data":{"key":…}}; 2026.8: {"data":{"object":
    // "message","raw":…,"message":"To unlock your vault…"}} — the
    // session key rides `raw`.
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    raw: Option<String>,
}

impl BwUnlockData {
    fn session_key(&self) -> Option<&str> {
        self.key.as_deref().or(self.raw.as_deref())
    }
}

fn bw_session() -> &'static tokio::sync::RwLock<Option<String>> {
    static SESSION: OnceLock<tokio::sync::RwLock<Option<String>>> = OnceLock::new();
    SESSION.get_or_init(|| tokio::sync::RwLock::new(None))
}

async fn bw_run(args: &[&str], session: Option<&str>) -> Result<(bool, String, String), String> {
    let bin = std::env::var("FORGE_BW_BIN").unwrap_or_else(|_| "bw".to_string());
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.args(args)
        .env("BW_NOT_IN_PROGRESS", "true")
        .env("NODE_OPTIONS", "--no-warnings")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(s) = session {
        cmd.env("BW_SESSION", s);
    }
    let out = tokio::time::timeout(
        std::time::Duration::from_secs(45),
        cmd.output(),
    )
    .await
    .map_err(|_| "bw timed out after 45s".to_string())?
    .map_err(|e| format!("spawn {bin}: {e} (is bitwarden-cli on the unit PATH?)"))?;
    Ok((
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    ))
}

/// Ensure an unlocked session, logging in / unlocking as needed.
async fn bw_ensure_session(cfg: &ConfigRow) -> Result<String, String> {
    {
        let s = bw_session().read().await;
        if let Some(s) = s.as_ref() {
            return Ok(s.clone());
        }
    }
    // ONE write guard for check + establish — tokio RwLock read
    // while self-held-write deadlocks (2026-10-18: the first bw call
    // hung the streaming bash path for exactly that reason).
    let mut guard = bw_session().write().await;
    if let Some(s) = guard.as_ref() {
        return Ok(s.clone());
    }
    // tell bw which server to talk to
    let _ = bw_run(&["config", "server", &cfg.server_url], None).await;
    let (_ok, out, err) = bw_run(&["status", "--response"], None).await?;
    let status = bw_status_of(&out)
        .ok_or_else(|| format!("bw status decode: no status in output ({err})"))?;
    let master = std::env::var("FORGE_BW_MASTER_PASSWORD")
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or("FORGE_BW_MASTER_PASSWORD not set on the forge-api unit")?;
    if status == "unauthenticated" {
        if cfg.email.is_empty() {
            return Err("bw-cli mode needs the vault account email".into());
        }
        let (ok, out, err) =
            bw_run(&["login", &cfg.email, &master, "--response"], None).await?;
        if !bw_resp_ok(ok, &out) {
            return Err(format!("bw login failed: {}", first_line(&err).or_else(|| first_line(&out)).unwrap_or_default()));
        }
    }
    let (ok, out, err) = bw_run(&["unlock", &master, "--response"], None).await?;
    if !bw_resp_ok(ok, &out) {
        return Err(format!(
            "bw unlock failed: {}",
            first_line(&err).or_else(|| first_line(&out)).unwrap_or_default()
        ));
    }
    let resp: BwUnlockResp = serde_json::from_str(json_tail(&out).unwrap_or("{}"))
        .map_err(|e| format!("bw unlock decode: {e}"))?;
    let key = resp
        .data
        .session_key()
        .ok_or("bw unlock response carried no session key (key/raw)")?
        .to_string();
    tracing::info!("bitwarden bw-cli session established (mode bw-cli)");
    *guard = Some(key.clone());
    Ok(key)
}

fn first_line(s: &str) -> Option<String> {
    s.lines().map(str::trim).find(|l| !l.is_empty()).map(|l| l.chars().take(160).collect())
}

/// `--response` mode exits 0 even on failures (CLI behavior), so the
/// success flag in the JSON is the real verdict.
fn bw_resp_ok(ok: bool, out: &str) -> bool {
    ok && !json_tail(out).is_some_and(|j| j.contains("\"success\":false"))
}

/// Run a bw command with the session; on "locked" refresh once.
async fn bw_with_retry(cfg: &ConfigRow, args: &[&str]) -> Result<(bool, String, String), String> {
    let session = bw_ensure_session(cfg).await?;
    let (ok, out, err) = bw_run(args, Some(&session)).await?;
    if !ok && err.to_lowercase().contains("locked") {
        tracing::info!("bitwarden bw-cli session locked — re-unlocking");
        *bw_session().write().await = None;
        let session = bw_ensure_session(cfg).await?;
        return bw_run(args, Some(&session)).await;
    }
    Ok((ok, out, err))
}

/// Parse a bw-cli reference tail into (item-name, selector).
pub fn parse_bw_cli_ref(key: &str) -> Option<(&str, BwSelector<'_>)> {
    let key = key.trim();
    if let Some(name) = key.strip_prefix("notes/") {
        let name = name.trim();
        return (!name.is_empty()).then_some((name, BwSelector::Notes));
    }
    // Item names may themselves contain slashes ("matrix-wrap/api-
    // token"), so the SELECTOR is matched right-anchored.
    let rest = key.strip_prefix("item/")?;
    if let Some((name, f)) = rest.rsplit_once("/login/") {
        if name.is_empty() {
            return None;
        }
        let selector = match f {
            "username" => BwSelector::LoginUsername,
            "password" => BwSelector::LoginPassword,
            "totp" => BwSelector::LoginTotp,
            _ => return None,
        };
        return Some((name, selector));
    }
    if let Some((name, f)) = rest.rsplit_once("/fields/") {
        if name.is_empty() || f.is_empty() {
            return None;
        }
        return Some((name, BwSelector::Field(f)));
    }
    if let Some(name) = rest.strip_suffix("/notes") {
        if name.is_empty() {
            return None;
        }
        return Some((name, BwSelector::Notes));
    }
    None
}

#[derive(Debug, PartialEq)]
pub enum BwSelector<'a> {
    Notes,
    LoginUsername,
    LoginPassword,
    LoginTotp,
    Field(&'a str),
}

fn select_item_json(raw: &str, selector: &BwSelector<'_>) -> Option<String> {
    // `bw get item NAME --response` → {"success":true,"data":{...}};
    // tolerate the bare item object too.
    let v: serde_json::Value = serde_json::from_str(json_tail(raw)?).ok()?;
    let item = v.get("data").cloned().unwrap_or(v);
    let pick = |s: &str| item.get(s).and_then(|x| x.as_str()).map(|s| s.to_string());
    match selector {
        BwSelector::Notes => pick("notes"),
        BwSelector::LoginUsername => item.get("login")?.get("username")?.as_str().map(String::from),
        BwSelector::LoginPassword => item.get("login")?.get("password")?.as_str().map(String::from),
        BwSelector::LoginTotp => item.get("login")?.get("totp")?.as_str().map(String::from),
        BwSelector::Field(name) => item
            .get("fields")?
            .as_array()?
            .iter()
            .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(name))
            .and_then(|f| f.get("value"))
            .and_then(|x| x.as_str())
            .map(String::from),
    }
}

async fn resolve_via_bw_cli(cfg: &ConfigRow, key: &str) -> Result<Option<String>, String> {
    let (name, selector) = parse_bw_cli_ref(key).ok_or("unrecognized bw:// reference (expected notes/<name>, item/<name>/login/<field> or item/<name>/fields/<field>)")?;
    let (ok, out, err) = bw_with_retry(cfg, &["get", "item", name, "--response"]).await?;
    if !bw_resp_ok(ok, &out) {
        let msg = first_line(&err).or_else(|| first_line(&out)).unwrap_or_default();
        // "Row was not found" / "must be unique" are vault-level misses.
        if msg.to_lowercase().contains("not found") {
            return Ok(None);
        }
        return Err(format!("bw get item {name}: {msg}"));
    }
    select_item_json(&out, &selector).map(Some).ok_or_else(|| {
        format!("vault item '{name}' has no {selector:?} value")
    })
}

async fn list_via_bw_cli(cfg: &ConfigRow) -> Result<Vec<String>, String> {
    let (ok, out, err) = bw_with_retry(cfg, &["list", "items", "--response"]).await?;
    if !bw_resp_ok(ok, &out) {
        let msg = first_line(&err).or_else(|| first_line(&out)).unwrap_or_default();
        return Err(format!("bw list items: {msg}"));
    }
    let v: serde_json::Value = serde_json::from_str(json_tail(&out).unwrap_or("{}"))
        .map_err(|e| format!("bw list decode: {e}"))?;
    Ok(v.get("data")
        .and_then(|d| d.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|i| i.get("name").and_then(|n| n.as_str()).map(String::from))
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

    #[test]
    fn bw_cli_refs_parse() {
        assert_eq!(
            parse_bw_cli_ref("notes/matrix-wrap/api-token"),
            Some(("matrix-wrap/api-token", BwSelector::Notes))
        );
        assert_eq!(
            parse_bw_cli_ref("item/github/mule/login/password"),
            Some(("github/mule", BwSelector::LoginPassword))
        );
        assert_eq!(
            parse_bw_cli_ref("item/forge/fields/api_key"),
            Some(("forge", BwSelector::Field("api_key")))
        );
        assert_eq!(
            parse_bw_cli_ref("item/x/notes"),
            Some(("x", BwSelector::Notes))
        );
        assert_eq!(parse_bw_cli_ref("item/x/login/bogus"), None);
        assert_eq!(parse_bw_cli_ref("item//login/password"), None);
        assert_eq!(parse_bw_cli_ref("weird"), None);
    }

    #[test]
    fn bw_status_shape_drift() {
        assert_eq!(
            bw_status_of(r#"{"status":"unlocked"}"#).as_deref(),
            Some("unlocked")
        );
        assert_eq!(
            bw_status_of(r#"Could not find dir…
{"success":true,"data":{"object":"template","status":"unauthenticated"}}"#)
                .as_deref(),
            Some("unauthenticated")
        );
        assert_eq!(
            bw_status_of(
                r#"{"success":true,"data":{"object":"template","template":{"serverUrl":"https://x","lastSync":null,"status":"unauthenticated"}}}"#
            )
            .as_deref(),
            Some("unauthenticated")
        );
        assert_eq!(bw_status_of("no json here"), None);
    }

    #[test]
    fn json_tail_skips_chatter() {
        let raw = "Could not find dir, \"/root/.config/Bitwarden CLI\"; creating it instead.\n{\"success\":true,\"data\":{\"status\":\"unlocked\"}}\n";
        assert_eq!(json_tail(raw).unwrap().starts_with('{'), true);
        let v: serde_json::Value = serde_json::from_str(json_tail(raw).unwrap()).unwrap();
        assert_eq!(v["data"]["status"].as_str(), Some("unlocked"));
    }

    #[test]
    fn bw_cli_json_selection() {
        let raw = r#"{"success":true,"data":{"name":"gh","notes":"secure note","login":{"username":"u","password":"s3cr3t"},"fields":[{"name":"api_key","value":"k1"}]}}"#;
        assert_eq!(select_item_json(raw, &BwSelector::LoginPassword).as_deref(), Some("s3cr3t"));
        assert_eq!(select_item_json(raw, &BwSelector::LoginUsername).as_deref(), Some("u"));
        assert_eq!(select_item_json(raw, &BwSelector::Notes).as_deref(), Some("secure note"));
        assert_eq!(select_item_json(raw, &BwSelector::Field("api_key")).as_deref(), Some("k1"));
        assert_eq!(select_item_json(raw, &BwSelector::Field("nope")), None);
        // bare item object (no envelope) still works
        let bare = r#"{"name":"x","login":{"password":"p"}}"#;
        assert_eq!(select_item_json(bare, &BwSelector::LoginPassword).as_deref(), Some("p"));
    }
}
