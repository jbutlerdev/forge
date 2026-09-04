//! Authentication module
//!
//! Provides user registration, login, and API key management.

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::api::AppState;
use crate::db::{
    ApiKey, ApiKeyCreated, ApiKeyResponse, CreateApiKey, CreateUser, LoginRequest, LoginResponse,
    UpdateUser, User, UserResponse,
};
use crate::logging::audit;

// ============================================
// Rate limiting (H3)
// ============================================
//
// `/auth/register` and `/auth/login` are unauthenticated and each
// attempt runs an argon2id hash (or a DB lookup + argon2id verify),
// so they are trivially usable for CPU/memory DoS and unlimited brute
// force. These endpoints share an in-process per-client token bucket:
// burst of 5, refilling at 1 token/second. No new dependencies — a
// small static map guarded by a Mutex.
//
// Client identification: the first entry of `X-Forwarded-For` when
// present (proxy-fronted deployments), then `X-Real-IP`, otherwise a
// bucket keyed by the `Host` header (this binary is served without
// `ConnectInfo`, so the handler cannot see the TCP peer address;
// keying by `Host` gives each locally-run server instance its own
// bucket in dev/test, and collapses to one shared bucket for the
// common single-port direct deployment — fail-closed). The distinct
// key count is capped so a client cannot enumerate identifiers to
// escape the limiter; once the cap is hit, new identifiers fall into
// the shared `shared` bucket.

/// Burst capacity for the auth endpoints' per-client token bucket.
const AUTH_RATE_BURST: f64 = 5.0;
/// Steady-state refill rate: 1 token per second.
const AUTH_RATE_REFILL_PER_SEC: f64 = 1.0;
/// Bound on the number of distinct rate-limit keys held, so
// untrusted identifiers cannot grow the map without limit.
const AUTH_RATE_MAX_DISTINCT_KEYS: usize = 256;
/// Sentinel key used when the number of distinct identifiers exceeds
/// `AUTH_RATE_MAX_DISTINCT_KEYS`.
const AUTH_RATE_SHARED_KEY: &str = "shared";

/// A token bucket: `tokens` refills continuously at
/// `AUTH_RATE_REFILL_PER_SEC` up to `AUTH_RATE_BURST`.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new() -> Self {
        Self {
            tokens: AUTH_RATE_BURST,
            last_refill: Instant::now(),
        }
    }

    /// Consume one token, refilling for the elapsed time first.
    /// `now` is injected so the logic is unit-testable without
    /// sleeping.
    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * AUTH_RATE_REFILL_PER_SEC).min(AUTH_RATE_BURST);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

type RateLimitMap = Mutex<HashMap<String, TokenBucket>>;

fn auth_rate_limiters() -> &'static RateLimitMap {
    static BUCKETS: OnceLock<RateLimitMap> = OnceLock::new();
    BUCKETS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Parse a client address out of proxy headers: first entry of
/// `X-Forwarded-For`, else `X-Real-IP`.
fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next().map(str::trim))
        .and_then(|s| s.parse::<IpAddr>().ok())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<IpAddr>().ok())
        })
}

/// Choose the rate-limit key for a request: the proxy-supplied client
/// IP when one is present, else a bucket keyed by the `Host` header
/// (see module note above).
fn auth_rate_limit_key(headers: &HeaderMap) -> String {
    if let Some(ip) = forwarded_client_ip(headers) {
        return ip.to_string();
    }
    match headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
    {
        Some(host) if !host.is_empty() => format!("host:{host}"),
        _ => "host:unknown".to_string(),
    }
}

/// Try to consume one rate-limit token for this client. Returns
/// `false` when the request should be rejected with 429.
fn allow_auth_request(headers: &HeaderMap) -> bool {
    let key = auth_rate_limit_key(headers);
    let mut map = auth_rate_limiters()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // Cap the number of distinct keys: an identifier never seen
    // before (and the map already at capacity) falls into the shared
    // bucket instead of minting a fresh one.
    let effective = if !map.contains_key(&key) && map.len() >= AUTH_RATE_MAX_DISTINCT_KEYS {
        AUTH_RATE_SHARED_KEY.to_string()
    } else {
        key
    };
    let bucket = map.entry(effective).or_insert_with(TokenBucket::new);
    bucket.try_take(Instant::now())
}

/// The 429 response returned when an auth request is rate-limited.
fn rate_limited_response() -> Response {
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        axum::http::header::RETRY_AFTER,
        "1".parse().expect("static header value"),
    );
    (
        StatusCode::TOO_MANY_REQUESTS,
        hdrs,
        Json(serde_json::json!({ "error": "Too many requests, please try again later" })),
    )
        .into_response()
}

// ============================================
// Password length cap (H3)
// ============================================

/// Maximum password length accepted by `/auth/register` and
/// `/auth/login`. Bounds the per-request argon2 work and JSON body
/// size; longer payloads are rejected with 400 before any hashing.
const MAX_PASSWORD_CHARS: usize = 128;

pub(crate) fn password_within_limit(password: &str) -> bool {
    password.chars().count() <= MAX_PASSWORD_CHARS
}

// ============================================
// Auth errors
// ============================================

/// Auth error types
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("User not found")]
    UserNotFound,
    #[error("Email already exists")]
    EmailExists,
    #[error("Invalid API key")]
    InvalidApiKey,
    #[error("API key expired")]
    ApiKeyExpired,
    #[error("Password hash error: {0}")]
    PasswordHash(String),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        // L9: never echo the raw sqlx error string to the client; log
        // the real error server-side and return a generic body instead.
        if let AuthError::Database(e) = &self {
            tracing::error!(error = %e, "auth request failed with database error");
        }

        let status = match &self {
            AuthError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthError::UserNotFound => StatusCode::NOT_FOUND,
            AuthError::EmailExists => StatusCode::CONFLICT,
            AuthError::InvalidApiKey => StatusCode::UNAUTHORIZED,
            AuthError::ApiKeyExpired => StatusCode::UNAUTHORIZED,
            AuthError::PasswordHash(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // L9: never echo the raw sqlx error string to the client — it
        // leaks driver/schema detail. Log the real error server-side
        // and return a generic body instead.
        let body = match &self {
            AuthError::Database(_) => "internal server error".to_string(),
            other => other.to_string(),
        };

        (status, Json(serde_json::json!({ "error": body }))).into_response()
    }
}

// ============================================
// Auth Context (extracted from request)
// ============================================

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: Uuid,
    pub role: String,
}

/// Tenancy gate: true when `user` may access a resource row owned by
/// `owner_id`. Admins can access anything; a non-admin may access the
/// row only when `owner_id == Some(user.user_id)`. Pre-tenancy rows
/// (where `owner_id` is `None`) are therefore admin-only — this keeps
/// existing single-operator data working under the admin key (which
/// is what the CLI / web UI use).
pub fn can_access(user: &AuthenticatedUser, owner_id: Option<Uuid>) -> bool {
    user.role == "admin" || owner_id == Some(user.user_id)
}

/// Extract authenticated user from request headers
/// Extract the raw API key string from a request's headers.
///
/// Forge's native API uses the `X-API-Key` header. The
/// OpenAI-compatible surface (`/v1/*`) uses the standard
/// `Authorization: Bearer <key>` header, because that's what
/// every OpenAI client sends and there's no way to reconfigure
/// `openai` / LangChain / etc. to send `X-API-Key` instead.
/// Accepting both headers in one place means the same key works
/// against either surface and the same validation path runs for
/// both, so the OpenAI endpoints get the real hash + DB lookup
/// (not just the presence check the middleware does).
///
/// `Authorization` wins over `X-API-Key` when both are present
/// (unlikely in practice); either is sufficient on its own.
pub(crate) fn extract_api_key_header(headers: &HeaderMap) -> Option<String> {
    if let Some(auth) = headers.get("Authorization").and_then(|v| v.to_str().ok()) {
        // OpenAI sends `Authorization: Bearer sk_forge_...`. Match
        // the `Bearer ` prefix case-insensitively, then slice the
        // *original* `auth` string by byte length so the returned
        // token keeps its original case — API keys are
        // case-sensitive and lowercasing the key here would break
        // the hash. ASCII lowercasing is length-preserving, so
        // stripping `"Bearer ".len()` bytes off the original is
        // the suffix after the prefix regardless of the prefix's
        // case. A bare `Bearer` with no token falls through to the
        // X-API-Key check below.
        let (prefix, _) = auth.split_at(auth.len().min("Bearer ".len()));
        if prefix.eq_ignore_ascii_case("Bearer ") {
            let token = auth["Bearer ".len()..].trim().to_string();
            if !token.is_empty() {
                return Some(token);
            }
        }
    }
    headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Validate the API key carried in a request's headers and return
/// the authenticated user. Accepts either `Authorization: Bearer
/// <key>` (OpenAI convention) or `X-API-Key: <key>` (forge native).
/// `pub(crate)` so the OpenAI-compatible handlers in `api::openai`
/// can run the same real validation the auth/user-management
/// handlers do, rather than relying on the middleware's
/// presence-only check.
pub(crate) async fn extract_auth_user(
    pool: &PgPool,
    headers: &HeaderMap,
) -> Result<AuthenticatedUser, AuthError> {
    let api_key = extract_api_key_header(headers).ok_or(AuthError::InvalidApiKey)?;

    // Hash the provided key. Dual-read: legacy SHA-256 always matches;
    // the hmac form matches when FORGE_KEY_HMAC_SECRET is set.
    let key_hashes = api_key_hash_candidates(&api_key);

    // Look up the API key in database
    let api_key_record: ApiKey = sqlx::query_as(
        r#"
        SELECT * FROM api_keys 
        WHERE key_hash = ANY($1) 
        LIMIT 1
        "#,
    )
    .bind(&key_hashes)
    .fetch_optional(pool)
    .await
    .map_err(AuthError::Database)?
    .ok_or(AuthError::InvalidApiKey)?;

    // Check expiration
    if let Some(expires_at) = api_key_record.expires_at {
        if expires_at < chrono::Utc::now() {
            return Err(AuthError::ApiKeyExpired);
        }
    }

    // Update last_used_at
    let _ = sqlx::query("UPDATE api_keys SET last_used_at = NOW() WHERE id = $1")
        .bind(api_key_record.id)
        .execute(pool)
        .await;

    // Get the user
    let user: User = sqlx::query_as("SELECT * FROM users WHERE id = $1")
        .bind(api_key_record.user_id)
        .fetch_one(pool)
        .await
        .map_err(AuthError::Database)?;

    Ok(AuthenticatedUser {
        user_id: user.id,
        role: user.role,
    })
}

// ============================================
// Password Hashing
// ============================================

// ============================================
// API Key Hashing (M5)
// ============================================
//
// Two storage forms coexist:
//
// * legacy: `hex(SHA-256(normalized))` — what every existing row
//   holds. Kept for dual-read: verification always tries it.
// * hmac: `hmac:<hex(HMAC-SHA256(secret, normalized))>` where the
//   secret comes from `FORGE_KEY_HMAC_SECRET`. New keys are stored in
//   this form when the secret is set; when it is unset we log a WARN
//   and keep storing legacy hashes so the system keeps working.
//
// `normalized` strips the `sk_forge_` prefix exactly once, so
// minted keys hash their secret material (without the prefix) rather
// than the full string; presentation of a bare key and a prefixed key
// still both verify (dual-read compat), but newly-stored hmac hashes
// are computed from the canonical normalized form.

/// Prefix minted onto every new API key.
pub const API_KEY_PREFIX: &str = "sk_forge_";

/// Normalize an API key by stripping the `sk_forge_` prefix exactly
/// once. `sk_forge_ABC` -> `ABC`; `ABC` -> `ABC`; `sk_forge_sk_forge_ABC`
/// -> `sk_forge_ABC` (a single strip, not a loop).
fn normalize_api_key(api_key: &str) -> &str {
    api_key.strip_prefix(API_KEY_PREFIX).unwrap_or(api_key)
}

/// Legacy storage form: `hex(SHA-256(normalized bytes))`.
fn legacy_api_key_hash(normalized: &str) -> String {
    hex::encode(Sha256::digest(normalized.as_bytes()))
}

/// HMAC-SHA256 (manual implementation — RFC 2104; avoids a new
/// dependency). Returns the hex digest.
fn hmac_sha256(key: &[u8], message: &[u8]) -> String {
    const BLOCK: usize = 64;
    let mut k: Vec<u8> = if key.len() > BLOCK {
        Sha256::digest(key).to_vec()
    } else {
        key.to_vec()
    };
    k.resize(BLOCK, 0u8);
    let mut ipad = Vec::with_capacity(BLOCK);
    let mut opad = Vec::with_capacity(BLOCK);
    for b in &k {
        ipad.push(b ^ 0x36);
        opad.push(b ^ 0x5c);
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(inner_digest);
    hex::encode(outer.finalize())
}

/// HMAC storage form: `hmac:<hex(HMAC-SHA256(secret, normalized))>`.
/// The `hmac:` prefix makes it unambiguous against the 64-hex-char
/// legacy form.
fn hmac_api_key_hash(normalized: &str, secret: &str) -> String {
    format!(
        "hmac:{}",
        hmac_sha256(secret.as_bytes(), normalized.as_bytes())
    )
}

/// The secret for HMAC key hashing, from `FORGE_KEY_HMAC_SECRET`
/// (read once per process). `None` when unset/empty.
fn key_hmac_secret() -> Option<&'static str> {
    static SECRET: OnceLock<Option<String>> = OnceLock::new();
    SECRET
        .get_or_init(|| {
            std::env::var("FORGE_KEY_HMAC_SECRET")
                .ok()
                .filter(|s| !s.is_empty())
        })
        .as_deref()
}

static WARNED_NO_HMAC_SECRET: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Storage form used for a newly minted key: hmac when the secret is
/// configured, legacy otherwise (with a one-time WARN).
fn new_api_key_hash_stored(api_key: &str) -> String {
    let normalized = normalize_api_key(api_key);
    match key_hmac_secret() {
        Some(secret) => hmac_api_key_hash(normalized, secret),
        None => {
            use std::sync::atomic::Ordering;
            if WARNED_NO_HMAC_SECRET.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "FORGE_KEY_HMAC_SECRET is not set; new API keys are stored as legacy \
                     SHA-256 hashes. Set it to enable HMAC key hashing."
                );
            }
            legacy_api_key_hash(normalized)
        }
    }
}

/// All storage forms that could match a presented key — legacy
/// SHA-256 always, plus the hmac form when the secret is configured.
/// Verification looks up `key_hash = ANY(candidates)`, which is the
/// dual-read: legacy rows keep working after the operator enables the
/// HMAC secret, without any re-keying migration.
fn api_key_hash_candidates(api_key: &str) -> Vec<String> {
    api_key_hash_candidates_nominal(normalize_api_key(api_key), key_hmac_secret())
}

/// Pure core of `api_key_hash_candidates` with no env access, so the
/// dual-read behavior is unit-testable without touching process state.
fn api_key_hash_candidates_nominal(normalized: &str, hmac_secret: Option<&str>) -> Vec<String> {
    let mut candidates = vec![legacy_api_key_hash(normalized)];
    if let Some(secret) = hmac_secret {
        candidates.push(hmac_api_key_hash(normalized, secret));
    }
    candidates
}

fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| AuthError::PasswordHash(e.to_string()))
}

fn verify_password(password: &str, hash: &str) -> Result<bool, AuthError> {
    let parsed_hash =
        PasswordHash::new(hash).map_err(|e| AuthError::PasswordHash(e.to_string()))?;

    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

// ============================================
// API Key Generation
// ============================================

fn generate_api_key() -> String {
    let mut key_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut key_bytes);
    let key_hex = hex::encode(key_bytes);
    format!("{}{}", API_KEY_PREFIX, key_hex)
}

fn get_key_prefix(api_key: &str) -> String {
    api_key.chars().take(12).collect()
}

// ============================================
// Admin bootstrap
// ============================================

/// Startup hook: create the operator's admin account, if configured and
/// not yet present.
///
/// Migration 009 removed the historically-seeded `admin@forge.local`
/// account (a fixed-credential backdoor). An admin is now created only
/// when the operator sets BOTH `FORGE_ADMIN_EMAIL` and
/// `FORGE_ADMIN_PASSWORD`. The account is created with the same
/// argon2id hash as `register`. Skipped (with a log line) when the
/// env vars are unset, when either is empty, or when any `role='admin'`
/// user already exists.
pub async fn bootstrap_admin(db: &PgPool) {
    let email = std::env::var("FORGE_ADMIN_EMAIL")
        .ok()
        .filter(|s| !s.is_empty());
    let password = std::env::var("FORGE_ADMIN_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());

    let (Some(email), Some(password)) = (email, password) else {
        tracing::info!("admin bootstrap: skipped (FORGE_ADMIN_EMAIL / FORGE_ADMIN_PASSWORD not set); no default admin is created");
        return;
    };

    bootstrap_admin_with(db, &email, &password).await;
}

/// Create the admin account, if not yet present. Exposed separately
/// from `bootstrap_admin` (which reads the environment) so tests can
/// drive it with fixed credentials without touching process env vars.
/// No-op (with a log line) when any `role='admin'` user already
/// exists.
pub async fn bootstrap_admin_with(db: &PgPool, email: &str, password: &str) {
    let has_admin =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE role = 'admin')")
            .fetch_one(db)
            .await
            .unwrap_or(false);
    if has_admin {
        tracing::info!("admin bootstrap: skipped (an admin user already exists)");
        return;
    }

    let password_hash = match hash_password(password) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "admin bootstrap: failed to hash password");
            return;
        }
    };

    match sqlx::query(
        "INSERT INTO users (email, name, password_hash, role) VALUES ($1, $2, $3, 'admin')",
    )
    .bind(email)
    .bind("Forge Admin")
    .bind(&password_hash)
    .execute(db)
    .await
    {
        Ok(_) => tracing::info!(admin_email = %email, "admin bootstrap: created admin account"),
        Err(e) => {
            // Usually means the email was created out-of-band between the
            // check above and this insert; not fatal.
            tracing::warn!(admin_email = %email, error = %e, "admin bootstrap: insert failed");
        }
    }
}

// ============================================
// Auth Routes
// ============================================

/// Register a new user
pub async fn register(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateUser>,
) -> Result<Response, AuthError> {
    // Rate limit (H3): unauthenticated argon2 work is a CPU/memory DoS
    // vector; reject with 429 before doing any hashing or DB work.
    if !allow_auth_request(&headers) {
        return Ok(rate_limited_response());
    }

    // Reject over-long passwords (H3) before any hashing. Counted in
    // chars, not bytes, so multi-byte passwords aren't over-limited.
    if !password_within_limit(&payload.password) {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("Password must be at most {MAX_PASSWORD_CHARS} characters") })),
        )
            .into_response());
    }

    // Validate password strength (minimum 8 characters)
    if payload.password.len() < 8 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Password must be at least 8 characters" })),
        )
            .into_response());
    }

    // Validate email format (basic check)
    if !payload.email.contains('@') {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid email format" })),
        )
            .into_response());
    }

    // Hash the password
    let password_hash = hash_password(&payload.password)?;

    // Create user
    let user: User = sqlx::query_as(
        r#"
        INSERT INTO users (email, name, password_hash, role)
        VALUES ($1, $2, $3, 'user')
        RETURNING *
        "#,
    )
    .bind(&payload.email)
    .bind(&payload.name)
    .bind(&password_hash)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e {
            if db_err.constraint() == Some("users_email_key") {
                return AuthError::EmailExists;
            }
        }
        AuthError::Database(e)
    })?;

    audit::user_register(user.id, &user.email);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "user": UserResponse::from(user) })),
    )
        .into_response())
}

/// Login and get API key
pub async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<LoginRequest>,
) -> Result<Response, AuthError> {
    // Rate limit (H3): same rationale as register.
    if !allow_auth_request(&headers) {
        return Ok(rate_limited_response());
    }

    // Reject over-long passwords (H3).
    if !password_within_limit(&payload.password) {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("Password must be at most {MAX_PASSWORD_CHARS} characters") })),
        )
            .into_response());
    }

    // Find user by email
    let user: User = sqlx::query_as("SELECT * FROM users WHERE email = $1")
        .bind(&payload.email)
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::InvalidCredentials)?;

    // Verify password. A broken stored hash (e.g. the placeholder
    // argon2 string in older migration 002, which `PasswordHash::new`
    // cannot parse) must read as "invalid credentials", not 500 —
    // otherwise the account can neither log in nor have its password
    // reset via the API.
    let valid = verify_password(&payload.password, &user.password_hash).unwrap_or(false);
    if !valid {
        return Err(AuthError::InvalidCredentials);
    }

    // Generate API key
    let api_key = generate_api_key();
    let key_hash = new_api_key_hash_stored(&api_key);
    let key_prefix = get_key_prefix(&api_key);

    // Store API key
    let _api_key_record: ApiKey = sqlx::query_as(
        r#"
        INSERT INTO api_keys (user_id, name, key_hash, key_prefix)
        VALUES ($1, 'Default API Key', $2, $3)
        RETURNING *
        "#,
    )
    .bind(user.id)
    .bind(&key_hash)
    .bind(&key_prefix)
    .fetch_one(&state.db)
    .await
    .map_err(AuthError::Database)?;

    audit::login(user.id, "unknown");

    Ok(Json(LoginResponse {
        user: UserResponse::from(user),
        api_key,
    })
    .into_response())
}

/// Logout (revoke the current API key)
pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    // Accept both headers (same as every other auth path), so
    // Bearer-only clients can log out too.
    let api_key = extract_api_key_header(&headers).ok_or(AuthError::InvalidApiKey)?;

    // Dual-read: accept both the legacy SHA-256 hash and the hmac form.
    let key_hashes = api_key_hash_candidates(&api_key);
    let key_prefix = get_key_prefix(&api_key);

    // Delete the API key and read its user_id in ONE statement via
    // RETURNING. The previous code deleted first, then ran a second
    // query for `user_id` — which always returned None because the
    // row was already gone, so `audit::logout` was never reached and
    // the audit log never recorded the logout.
    let user_id: Option<Uuid> =
        sqlx::query_scalar("DELETE FROM api_keys WHERE key_hash = ANY($1) RETURNING user_id")
            .bind(&key_hashes)
            .fetch_optional(&state.db)
            .await
            .map_err(AuthError::Database)?
            .flatten();

    if let Some(uid) = user_id {
        audit::logout(uid, "unknown", &key_prefix);
    } else {
        tracing::debug!(
            key_prefix = %key_prefix,
            "logout: no API key matched (already revoked or invalid); no audit entry"
        );
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ============================================
// API Key Management Routes
// ============================================

/// List user's API keys
pub async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    let keys: Vec<ApiKey> =
        sqlx::query_as("SELECT * FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC")
            .bind(auth.user_id)
            .fetch_all(&state.db)
            .await
            .map_err(AuthError::Database)?;

    let response: Vec<ApiKeyResponse> = keys.into_iter().map(ApiKeyResponse::from).collect();

    Ok(Json(serde_json::json!({ "api_keys": response })).into_response())
}

/// Create a new API key
pub async fn create_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateApiKey>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    let api_key = generate_api_key();
    let key_hash = new_api_key_hash_stored(&api_key);
    let key_prefix = get_key_prefix(&api_key);

    // Calculate expiration if provided
    let expires_at = payload
        .expires_in_days
        .map(|days| chrono::Utc::now() + chrono::Duration::days(days as i64));

    let key_record: ApiKey = sqlx::query_as(
        r#"
        INSERT INTO api_keys (user_id, name, key_hash, key_prefix, expires_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
    )
    .bind(auth.user_id)
    .bind(&payload.name)
    .bind(&key_hash)
    .bind(&key_prefix)
    .bind(expires_at)
    .fetch_one(&state.db)
    .await
    .map_err(AuthError::Database)?;

    audit::api_key_create(auth.user_id, &key_prefix);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!(ApiKeyCreated {
            api_key,
            api_key_response: ApiKeyResponse::from(key_record),
        })),
    )
        .into_response())
}

/// Get API key details
pub async fn get_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(key_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    let key: ApiKey = sqlx::query_as("SELECT * FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(key_id)
        .bind(auth.user_id)
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::InvalidApiKey)?;

    Ok(Json(serde_json::json!({ "api_key": ApiKeyResponse::from(key) })).into_response())
}

/// Revoke an API key
pub async fn delete_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(key_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    let result = sqlx::query("DELETE FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(key_id)
        .bind(auth.user_id)
        .execute(&state.db)
        .await
        .map_err(AuthError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AuthError::InvalidApiKey);
    }

    audit::api_key_revoke(auth.user_id, key_id);

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ============================================
// User Management Routes (Admin only)
// ============================================

/// List all users (admin only)
pub async fn list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    if auth.role != "admin" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Admin access required" })),
        )
            .into_response());
    }

    let users: Vec<User> = sqlx::query_as("SELECT * FROM users ORDER BY created_at DESC")
        .fetch_all(&state.db)
        .await
        .map_err(AuthError::Database)?;

    let response: Vec<UserResponse> = users.into_iter().map(UserResponse::from).collect();

    Ok(Json(serde_json::json!({ "users": response })).into_response())
}

/// Get user by ID
pub async fn get_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(user_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    // Users can only view their own profile, admins can view anyone
    if auth.role != "admin" && auth.user_id != user_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Access denied" })),
        )
            .into_response());
    }

    let user: User = sqlx::query_as("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::UserNotFound)?;

    Ok(Json(serde_json::json!({ "user": UserResponse::from(user) })).into_response())
}

/// Update user
pub async fn update_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(user_id): axum::extract::Path<Uuid>,
    Json(payload): Json<UpdateUser>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    // Users can only update their own profile, admins can update anyone
    if auth.role != "admin" && auth.user_id != user_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Access denied" })),
        )
            .into_response());
    }

    // Non-admins cannot change role
    if payload.role.is_some() && auth.role != "admin" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Only admins can change user role" })),
        )
            .into_response());
    }

    // Build dynamic update query
    let mut updates = Vec::new();
    let mut param_idx = 1;

    if payload.email.is_some() {
        updates.push(format!("email = ${}", param_idx));
        param_idx += 1;
    }
    if payload.name.is_some() {
        updates.push(format!("name = ${}", param_idx));
        param_idx += 1;
    }
    if payload.role.is_some() && auth.role == "admin" {
        updates.push(format!("role = ${}", param_idx));
        param_idx += 1;
    }

    if updates.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "No fields to update" })),
        )
            .into_response());
    }

    let query = format!(
        "UPDATE users SET updated_at = NOW(), {} WHERE id = ${} RETURNING *",
        updates.join(", "),
        param_idx
    );

    let mut db_query = sqlx::query_as::<_, User>(&query);

    if let Some(ref v) = payload.email {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.name {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.role {
        db_query = db_query.bind(v);
    }

    db_query = db_query.bind(user_id);

    let user = db_query
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::UserNotFound)?;

    Ok(Json(serde_json::json!({ "user": UserResponse::from(user) })).into_response())
}

/// Delete user (admin only)
pub async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(user_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;

    if auth.role != "admin" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Admin access required" })),
        )
            .into_response());
    }

    // Cannot delete yourself
    if auth.user_id == user_id {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Cannot delete your own account" })),
        )
            .into_response());
    }

    let result = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&state.db)
        .await
        .map_err(AuthError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AuthError::UserNotFound);
    }

    audit::user_delete(auth.user_id, user_id);

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ============================================
// Router
// ============================================

pub fn create_auth_router() -> Router<AppState> {
    Router::new()
        // Auth routes (no authentication required)
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        // Auth routes (authentication required)
        .route("/auth/logout", post(logout))
        // API Key routes (authentication required)
        .route("/api-keys", get(list_api_keys))
        .route("/api-keys", post(create_api_key))
        .route("/api-keys/:id", get(get_api_key))
        .route("/api-keys/:id", delete(delete_api_key))
        // User routes (authentication required)
        .route("/users", get(list_users))
        .route("/users/:id", get(get_user))
        .route("/users/:id", patch(update_user))
        .route("/users/:id", delete(delete_user))
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn token_bucket_allows_burst_then_rejects() {
        let mut b = TokenBucket::new();
        let t0 = Instant::now();
        for _ in 0..AUTH_RATE_BURST as u32 {
            assert!(b.try_take(t0));
        }
        assert!(
            !b.try_take(t0),
            "6th request in same instant must be rejected"
        );
        assert!(!b.try_take(t0));
    }

    #[test]
    fn token_bucket_refills_at_one_per_second() {
        let mut b = TokenBucket::new();
        let t0 = Instant::now();
        for _ in 0..AUTH_RATE_BURST as u32 {
            b.try_take(t0);
        }
        assert!(!b.try_take(t0));
        // One second later: exactly one token refilled.
        assert!(b.try_take(t0 + Duration::from_secs(1)));
        assert!(!b.try_take(t0 + Duration::from_secs(1)));
        // Three more seconds: exactly three tokens refilled.
        for _ in 0..3 {
            assert!(b.try_take(t0 + Duration::from_secs(4)));
        }
        assert!(!b.try_take(t0 + Duration::from_secs(4)));
    }

    #[test]
    fn token_bucket_caps_at_burst_capacity() {
        let mut b = TokenBucket::new();
        let t0 = Instant::now();
        // A very long idle period refills to capacity, not beyond.
        let t_later = t0 + Duration::from_secs(3600);
        for _ in 0..AUTH_RATE_BURST as u32 {
            assert!(b.try_take(t_later));
        }
        assert!(!b.try_take(t_later), "bucket must cap at burst capacity");
    }

    #[test]
    fn password_length_limit_is_char_based() {
        assert!(password_within_limit(&"a".repeat(MAX_PASSWORD_CHARS)));
        assert!(!password_within_limit(&"a".repeat(MAX_PASSWORD_CHARS + 1)));
        // Multi-byte chars count as chars, not bytes (128 × é = 256 bytes).
        let multi: String = "é".repeat(MAX_PASSWORD_CHARS);
        assert!(password_within_limit(&multi));
        assert!(!password_within_limit(&"é".repeat(MAX_PASSWORD_CHARS + 1)));
        assert!(password_within_limit("short"));
    }

    #[test]
    fn normalize_api_key_strips_prefix_exactly_once() {
        assert_eq!(normalize_api_key("sk_forge_abc"), "abc");
        assert_eq!(normalize_api_key("abc"), "abc");
        // Strip exactly once: the inner prefix survives normalization.
        assert_eq!(normalize_api_key("sk_forge_sk_forge_abc"), "sk_forge_abc");
    }

    #[test]
    fn hmac_sha256_known_vector() {
        // RFC 4231 §4.4-style test value for HMAC-SHA256 with key "key".
        assert_eq!(
            hmac_sha256(b"key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn hmac_hash_format_and_prefix_disambiguation() {
        let h = hmac_api_key_hash("abc", "s3cret");
        assert!(
            h.starts_with("hmac:"),
            "hmac form must carry the hmac: prefix"
        );
        assert_eq!(h, format!("hmac:{}", hmac_sha256(b"s3cret", b"abc")));
        // Distinct from the legacy form, and distinct across secrets.
        assert_ne!(h, legacy_api_key_hash("abc"));
        assert_ne!(h, hmac_api_key_hash("abc", "other"));
    }

    #[test]
    fn key_hash_dual_read_candidates() {
        // Without the secret, only the legacy candidate is produced —
        // pre-existing keys keep verifying exactly as before.
        // (Input is the already-normalized key body.)
        let legacy_only = api_key_hash_candidates_nominal("abc", None);
        assert_eq!(legacy_only, vec![legacy_api_key_hash("abc")]);

        // With the secret, verification tries both forms, so keys minted
        // under either scheme verify (dual-read).
        let both = api_key_hash_candidates_nominal("abc", Some("s3cret"));
        assert_eq!(both.len(), 2);
        assert_eq!(both[0], legacy_api_key_hash("abc"));
        assert_eq!(both[1], hmac_api_key_hash("abc", "s3cret"));
    }

    #[test]
    fn forwarded_client_ip_parsing() {
        use axum::http::header::HeaderName;
        fn hdrs(pairs: &[(&str, &str)]) -> HeaderMap {
            let mut h = HeaderMap::new();
            for (k, v) in pairs {
                h.insert(
                    HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    v.parse().unwrap(),
                );
            }
            h
        }

        assert_eq!(
            forwarded_client_ip(&hdrs(&[("x-forwarded-for", "1.2.3.4, 5.6.7.8")])),
            Some(IpAddr::from([1, 2, 3, 4])),
            "first entry of a chained XFF wins"
        );
        assert_eq!(
            forwarded_client_ip(&hdrs(&[("x-forwarded-for", "::1")])),
            Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1])),
        );
        assert_eq!(
            forwarded_client_ip(&hdrs(&[("x-real-ip", "9.9.9.9")])),
            Some(IpAddr::from([9, 9, 9, 9])),
            "X-Real-IP is the fallback when XFF is absent"
        );
        assert_eq!(
            forwarded_client_ip(&hdrs(&[("x-forwarded-for", "not-an-ip")])),
            None
        );
        assert_eq!(forwarded_client_ip(&hdrs(&[])), None);
    }
}
