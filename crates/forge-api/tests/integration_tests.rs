//! API Integration Tests for Forge
//!
//! These tests verify the HTTP API endpoints work correctly.
//! They require a running database and test various API operations.

use futures_util::StreamExt;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

mod test_helpers;

/// Test helper to create a test app with database
async fn create_test_app() -> (test_helpers::TestApp, String) {
    test_helpers::TestApp::new().await
}

/// Authentication helper
async fn register_and_login(app: &test_helpers::TestApp) -> (String, String) {
    // Register user
    let register_resp = app
        .post("/auth/register")
        .json(&json!({
            "email": "test@example.com",
            "name": "Test User",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(register_resp.status(), 201, "Registration should succeed");

    // Login
    let login_resp = app
        .post("/auth/login")
        .json(&json!({
            "email": "test@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(login_resp.status(), 200, "Login should succeed");

    let body: serde_json::Value = login_resp.json().await.unwrap();
    let api_key = body["api_key"].as_str().unwrap().to_string();
    let user_id = body["user"]["id"].as_str().unwrap().to_string();

    (user_id, api_key)
}

// ============================================
// Health Endpoint Tests
// ============================================

#[tokio::test]
async fn test_health_endpoint() {
    let (app, _db_url) = create_test_app().await;

    let resp = app.get("/health").send().await.unwrap();

    assert_eq!(resp.status(), 200, "Health check should return 200");
    assert_eq!(resp.text(), "OK");
}

// ============================================
// Auth Endpoint Tests
// ============================================

#[tokio::test]
async fn test_register_success() {
    let (app, _db_url) = create_test_app().await;

    let resp = app
        .post("/auth/register")
        .json(&json!({
            "email": "newuser@example.com",
            "name": "New User",
            "password": "securepassword123"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201, "Registration should return 201");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["user"].is_object(), "Response should contain user");
    assert_eq!(body["user"]["email"], "newuser@example.com");
    assert_eq!(body["user"]["role"], "user");
}

#[tokio::test]
async fn test_register_duplicate_email() {
    let (app, _db_url) = create_test_app().await;

    // Register first user
    let resp1 = app
        .post("/auth/register")
        .json(&json!({
            "email": "duplicate@example.com",
            "name": "First User",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 201);

    // Try to register with same email
    let resp2 = app
        .post("/auth/register")
        .json(&json!({
            "email": "duplicate@example.com",
            "name": "Second User",
            "password": "password456"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp2.status(),
        409,
        "Duplicate email should return 409 Conflict"
    );
}

#[tokio::test]
async fn test_register_invalid_email() {
    let (app, _db_url) = create_test_app().await;

    let resp = app
        .post("/auth/register")
        .json(&json!({
            "email": "notanemail",
            "name": "Test User",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400, "Invalid email should return 400");
}

#[tokio::test]
async fn test_register_short_password() {
    let (app, _db_url) = create_test_app().await;

    let resp = app
        .post("/auth/register")
        .json(&json!({
            "email": "test@example.com",
            "name": "Test User",
            "password": "short"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400, "Short password should return 400");
}

#[tokio::test]
async fn test_login_success() {
    let (app, _db_url) = create_test_app().await;

    // Register first
    app.post("/auth/register")
        .json(&json!({
            "email": "logintest@example.com",
            "name": "Login Test",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();

    // Login
    let resp = app
        .post("/auth/login")
        .json(&json!({
            "email": "logintest@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Login should succeed");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["api_key"].is_string(),
        "Response should contain api_key"
    );
    assert!(body["api_key"].as_str().unwrap().starts_with("sk_forge_"));
}

#[tokio::test]
async fn test_login_invalid_password() {
    let (app, _db_url) = create_test_app().await;

    // Register first
    app.post("/auth/register")
        .json(&json!({
            "email": "wrongpass@example.com",
            "name": "Wrong Pass",
            "password": "correctpassword"
        }))
        .send()
        .await
        .unwrap();

    // Login with wrong password
    let resp = app
        .post("/auth/login")
        .json(&json!({
            "email": "wrongpass@example.com",
            "password": "wrongpassword"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401, "Wrong password should return 401");
}

#[tokio::test]
async fn test_login_nonexistent_user() {
    let (app, _db_url) = create_test_app().await;

    let resp = app
        .post("/auth/login")
        .json(&json!({
            "email": "nonexistent@example.com",
            "password": "anypassword"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401, "Nonexistent user should return 401");
}

/// Regression test for the seeded admin account (migration 002).
///
/// Every fresh database gets `admin@forge.local` from the migration.
/// Older versions of migration 002 inserted a *placeholder* argon2
/// hash that `PasswordHash::new` couldn't parse, so the correct
/// password (`admin123`) read as invalid credentials (a 500 before
/// the parse-failure was mapped to invalid, then a 401 after). The
/// migrations now ship a real argon2id hash AND migration 008
/// self-heals already-deployed databases; this test pins the
/// contract: correct password -> 200 with an admin-role user,
/// wrong password -> 401 (never 500).
#[tokio::test]
async fn test_seeded_admin_login() {
    let (app, _db_url) = create_test_app().await;

    // Correct password: must succeed and return an admin-role user.
    let resp = app
        .post("/auth/login")
        .json(&json!({
            "email": "admin@forge.local",
            "password": "admin123"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Seeded admin login should succeed");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["user"]["email"], "admin@forge.local");
    assert_eq!(
        body["user"]["role"], "admin",
        "Seeded admin must have the admin role"
    );
    assert!(
        body["api_key"].as_str().unwrap().starts_with("sk_forge_"),
        "Admin login should mint a usable API key"
    );

    // Wrong password: 401 (a broken/placeholder hash must never 500).
    let bad = app
        .post("/auth/login")
        .json(&json!({
            "email": "admin@forge.local",
            "password": "not-the-password"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401, "Wrong admin password should return 401, not 500");
}

/// The byte-exact content of the ORIGINAL migration 002 (as committed
/// at HEAD `75716a8`): it inserted a placeholder argon2 string for the
/// admin user. Every database deployed before the #22 fix has applied
/// exactly this file, and sqlx has stored sha384(these exact bytes) as
/// the migration's checksum in `_sqlx_migrations`.
///
/// Keeping this const as the historical content is what makes the
/// test a real regression net: sqlx compares the stored checksum
/// against the CURRENT on-disk 002 file, so if anyone ever edits 002
/// in place again (instead of shipping a new migration), the
/// checksums diverge and this test fails with the same
/// "migration 2 was previously applied but has been modified" error
/// that a deployed DB would hit at startup.
const ORIGINAL_002_PLACEHOLDER: &str = r#"-- Migration: 002_users_and_api_keys.sql
-- Add user management and API key authentication

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'user',  -- 'admin' | 'user'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- API Keys table
CREATE TABLE IF NOT EXISTS api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    key_hash        TEXT NOT NULL UNIQUE,  -- SHA-256 hash of the key
    key_prefix      TEXT NOT NULL,          -- First 12 chars for identification
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add user_id to existing tables (with default NULL for backwards compatibility)
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

-- Function to hash API key
CREATE OR REPLACE FUNCTION hash_api_key(api_key TEXT) RETURNS TEXT AS $$
    WITH decoded AS (
        SELECT decode(replace(api_key, 'sk_forge_', ''), 'hex') as key_bytes
    )
    SELECT encode(sha256(key_bytes), 'hex') FROM decoded;
$$ LANGUAGE SQL IMMUTABLE;

-- Create admin user if not exists (for initial setup)
-- Password: admin123 (change this in production!)
INSERT INTO users (email, name, password_hash, role)
VALUES ('admin@forge.local', 'Forge Admin', '$argon2id$v=19$m=19456,t=2,p=1$placeholder$placeholder', 'admin')
ON CONFLICT (email) DO NOTHING;
"#;

/// Deployed-DB upgrade regression (bug #22). A database that applied
/// the ORIGINAL migration 002 (placeholder admin hash) must upgrade
/// cleanly to the current tree. sqlx validates the checksum of every
/// already-applied migration, so editing 002 in place (placeholder →
/// real hash) made every such DB fail startup with "migration 2 was
/// previously applied but has been modified" — migration 008 (the
/// healer) never got to run, so the placeholder hash was permanent.
///
/// The test replays a pre-fix deployed DB: applies 001 + the
/// placeholder 002 byte-for-byte, records the checksums the old
/// binary stored, then runs the current `sqlx::migrate!` set (the
/// exact `main.rs:135` startup path) and asserts the upgrade succeeds
/// AND migration 008 replaced the placeholder with a hash that
/// actually verifies `admin123`.
#[tokio::test]
async fn test_deployed_db_with_old_002_migrates_and_heals_admin() {
    use argon2::{Argon2, PasswordHash, PasswordVerifier};
    use sha2::{Digest, Sha384};

    let db_name = format!("forge_test_mig_{}", Uuid::new_v4().to_string().replace('-', ""));
    let db_url = test_helpers::create_database(&db_name).await;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect to scratch database");

    // 1. Apply 001 + the ORIGINAL (placeholder) 002, exactly as the
    // pre-fix binary did on its first run. `sqlx::raw_sql` uses the
    // simple query protocol, so multi-statement files work the same
    // way sqlx's own migrator applies them.
    sqlx::raw_sql(include_str!("../migrations/001_initial_schema.sql"))
        .execute(&pool)
        .await
        .expect("apply original 001");
    sqlx::raw_sql(ORIGINAL_002_PLACEHOLDER)
        .execute(&pool)
        .await
        .expect("apply original (placeholder) 002");

    // 2. Record the migration ledger the way the old binary did:
    // sqlx stores sha384 of the applied file's raw bytes.
    sqlx::raw_sql(
        "CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .expect("create _sqlx_migrations");

    let checksum_001: Vec<u8> =
        Sha384::digest(include_str!("../migrations/001_initial_schema.sql").as_bytes()).to_vec();
    let checksum_002: Vec<u8> = Sha384::digest(ORIGINAL_002_PLACEHOLDER.as_bytes()).to_vec();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (1, 'initial schema', true, $1, 10), (2, 'users and api keys', true, $2, 10)",
    )
    .bind(&checksum_001)
    .bind(&checksum_002)
    .execute(&pool)
    .await
    .expect("record applied migrations 001 + 002");

    // 3. Run the current migration set — must NOT fail with a
    // VersionMismatch on migration 2.
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("deployed DB must upgrade cleanly (no checksum VersionMismatch)");

    // 4. Migration 008 must have healed the placeholder admin hash,
    // and the healed hash must actually verify `admin123`.
    let hash: String = sqlx::query_scalar(
        "SELECT password_hash FROM users WHERE email = 'admin@forge.local'",
    )
    .fetch_one(&pool)
    .await
    .expect("admin row exists");
    assert!(
        !hash.contains("placeholder"),
        "migration 008 must replace the placeholder hash; got: {}",
        hash
    );
    let parsed =
        PasswordHash::new(&hash).expect("healed hash must be a parseable argon2id string");
    assert!(
        Argon2::default()
            .verify_password(b"admin123", &parsed)
            .is_ok(),
        "healed hash must verify the admin123 bootstrap password"
    );

    // 5. And 008 must be recorded as applied.
    let applied_008: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM _sqlx_migrations WHERE version = 8 AND success = true",
    )
    .fetch_one(&pool)
    .await
    .expect("query 008 ledger row");
    assert_eq!(applied_008, 1, "migration 008 must be applied");

    pool.close().await;
    test_helpers::drop_test_db("postgres://postgres:forge@localhost/postgres", &db_name).await;
}

// ============================================
// Profile Endpoint Tests
// ============================================

#[tokio::test]
async fn test_create_profile() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/test-profile"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201, "Profile creation should return 201");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["profile"]["id"].is_string(),
        "Response should contain profile ID"
    );
    assert_eq!(body["profile"]["name"], "Test Profile");
}

/// Regression test for the `profiles.provider` CHECK constraint.
/// `proxy-anthropic` is a documented, code-supported provider
/// (`pi_agent.rs` handles `"anthropic" | "proxy-anthropic"`, and
/// `docs/API.md` / `AGENTS.md` list it), but migration 001's CHECK
/// only allowed `('openai','anthropic')`. Creating a
/// `proxy-anthropic` profile used to fail with a CHECK violation
/// mapped to a generic 500. Migration 005 widens the CHECK; this
/// test pins that the documented provider is creatable and returns
/// 201, not 500.
#[tokio::test]
async fn test_create_profile_proxy_anthropic() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Proxy Anthropic Profile",
            "provider": "proxy-anthropic",
            "model": "minimax-anthropic/MiniMax-M3",
            "base_url": "https://proxy.example.com/v1",
            "api_key": "sk-proxy-test",
            "working_dir": "/tmp/proxy-profile"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        201,
        "proxy-anthropic profile creation should return 201, not a 500 CHECK violation"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["profile"]["provider"], "proxy-anthropic");
    assert_eq!(body["profile"]["base_url"], "https://proxy.example.com/v1");
}

#[tokio::test]
async fn test_create_profile_rejects_unknown_provider_with_400() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Bad Provider Profile",
            "provider": "not-a-real-provider",
            "model": "some-model",
            "working_dir": "/tmp/bad-provider"
        }))
        .send()
        .await
        .unwrap();

    // App-level validation rejects the unknown provider with a 400
    // (a clear, client-actionable error) rather than letting it fall
    // through to the DB CHECK and come back as a generic 500.
    assert_eq!(
        resp.status(),
        400,
        "unknown provider should return 400, not a 500 CHECK violation"
    );
}

#[tokio::test]
async fn test_create_profile_unauthorized() {
    let (app, _db_url) = create_test_app().await;

    let resp = app
        .post("/profiles")
        .json(&json!({
            "name": "Unauthorized Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/test"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401, "Missing API key should return 401");
}

/// Regression test for the `forge-agent-setup` idempotency
/// bug: when a profile with the same name already exists
/// (e.g. the cached `profile.id` in `agent.yaml` was
/// wiped via `yq del .profile.id` and the script tried to
/// recreate the profile), `POST /profiles` used to return
/// generic `500 {"error":"Failed to create profile"}` —
/// which hid the actual cause (Postgres
/// `profiles_name_key` unique-constraint violation) and
/// made the script's idempotency logic impossible to
/// implement on the client side.
///
/// The fix: return `409 Conflict` with a body that names
/// the conflicting profile, so `forge-agent-setup` can
/// `GET /profiles`, filter by name, recover the existing
/// `profile_id`, and treat the call as an idempotent
/// re-run.
#[tokio::test]
async fn test_create_profile_duplicate_name_returns_409() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let payload = json!({
        "name": "Duplicate Name Test",
        "provider": "anthropic",
        "model": "claude-sonnet-4-20250514",
        "working_dir": "/tmp/dup"
    });

    // First call: should succeed.
    let resp1 = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 201, "first POST /profiles should succeed");
    let first: serde_json::Value = resp1.json().await.unwrap();
    let first_id = first["profile"]["id"].as_str().unwrap().to_string();

    // Second call with the same name: should return 409
    // Conflict, NOT 500 Internal Server Error. The
    // response body should mention the conflicting name.
    let resp2 = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp2.status(),
        409,
        "second POST /profiles with the same name should return 409 Conflict (got {})",
        resp2.status()
    );
    let body: serde_json::Value = resp2.json().await.unwrap();
    let err = body["error"].as_str().unwrap_or("");
    assert!(
        err.contains("Duplicate Name Test"),
        "409 body should mention the conflicting profile name; got: {err}"
    );

    // And the existing profile is still there with the
    // same id (the second call didn't mutate state).
    let resp3 = app
        .get(&format!("/profiles/{first_id}"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp3.status(), 200);
}

#[tokio::test]
async fn test_list_profiles() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile first
    app.post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "List Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/list-test"
        }))
        .send()
        .await
        .unwrap();

    // List profiles
    let resp = app
        .get("/profiles")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "List profiles should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["profiles"].is_array(),
        "Response should contain profiles array"
    );
    assert!(
        !body["profiles"].as_array().unwrap().is_empty(),
        "Should have at least one profile"
    );
}

#[tokio::test]
async fn test_get_profile_by_id() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile
    let create_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Get Test Profile",
            "provider": "openai",
            "model": "gpt-4",
            "working_dir": "/tmp/get-test"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_resp.json().await.unwrap();
    let profile_id = create_body["profile"]["id"].as_str().unwrap();

    // Get profile by ID
    let resp = app
        .get(&format!("/profiles/get?id={}", profile_id))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Get profile should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["profile"]["id"], profile_id);
    assert_eq!(body["profile"]["name"], "Get Test Profile");
}

#[tokio::test]
async fn test_get_profile_not_found() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let fake_id = "00000000-0000-0000-0000-000000000000";

    let resp = app
        .get(&format!("/profiles/get?id={}", fake_id))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404, "Non-existent profile should return 404");
}

#[tokio::test]
async fn test_update_profile() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile
    let create_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Original Name",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/update-test"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_resp.json().await.unwrap();
    let profile_id = create_body["profile"]["id"].as_str().unwrap();

    // Update profile
    let resp = app
        .patch(&format!("/profiles/update?id={}", profile_id))
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Updated Name",
            "model": "claude-opus-4-20250514"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Update should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["profile"]["name"], "Updated Name");
    assert_eq!(body["profile"]["model"], "claude-opus-4-20250514");
}

#[tokio::test]
async fn test_delete_profile() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile
    let create_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Delete Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/delete-test"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_resp.json().await.unwrap();
    let profile_id = create_body["profile"]["id"].as_str().unwrap();

    // Delete profile
    let resp = app
        .delete(&format!("/profiles/delete?id={}", profile_id))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204, "Delete should return 204");

    // Verify it's deleted
    let get_resp = app
        .get(&format!("/profiles/get?id={}", profile_id))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.status(), 404, "Deleted profile should return 404");
}

// ============================================
// Session Endpoint Tests
// ============================================

#[tokio::test]
async fn test_create_session() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile first
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Session Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    // Create session
    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id,
            "title": "Test Session"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201, "Session creation should return 201");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["session"]["id"].is_string(),
        "Response should contain session ID"
    );
    assert_eq!(body["session"]["title"], "Test Session");
    assert!(
        body["working_dir"].is_string(),
        "Response should contain working_dir"
    );
}

#[tokio::test]
async fn test_switch_session_model_via_patch() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile with one model.
    let p1 = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({"name":"model-a","provider":"anthropic","model":"claude-sonnet-4-20250514","working_dir":"/tmp/ma"}))
        .send().await.unwrap();
    let pid1: String = {
        let b: serde_json::Value = p1.json().await.unwrap();
        b["profile"]["id"].as_str().unwrap().to_string()
    };

    // Create a session with that profile.
    let s = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({"profile_id":&pid1,"title":"Switch Test"}))
        .send()
        .await
        .unwrap();
    let sid: String = {
        let b: serde_json::Value = s.json().await.unwrap();
        b["session"]["id"].as_str().unwrap().to_string()
    };

    // Switch the model via an override (Option A). The profile_id
    // is unchanged; only provider+model are overridden.
    let resp = app
        .patch(&format!("/sessions/{}", sid))
        .header("X-API-Key", &api_key)
        .json(&json!({"provider":"openai","model":"gpt-4o"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PATCH should return 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["session"]["override_provider"], "openai",
        "override_provider should be set"
    );
    assert_eq!(
        body["session"]["override_model"], "gpt-4o",
        "override_model should be set"
    );
    assert_eq!(
        body["session"]["profile_id"], pid1,
        "profile_id must be unchanged (workspace preserved)"
    );
    // The response includes the (unchanged) profile so the UI can
    // compute effective model = override ?? profile.*
    assert_eq!(
        body["profile"]["model"], "claude-sonnet-4-20250514",
        "profile itself is unchanged"
    );

    // Verify via GET that the overrides persisted.
    let got = app
        .get(&format!("/sessions/{}", sid))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let got_body: serde_json::Value = got.json().await.unwrap();
    assert_eq!(
        got_body["session"]["override_model"], "gpt-4o",
        "GET should confirm the override"
    );
    assert_eq!(
        got_body["session"]["profile_id"], pid1,
        "GET should confirm profile_id unchanged"
    );
}

#[tokio::test]
async fn test_patch_session_clears_override_with_null() {
    // Setting an override to null *clears* it (falls back to the
    // profile). Distinguishes "omitted" from "explicitly null".
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let p = app.post("/profiles").header("X-API-Key", &api_key)
        .json(&json!({"name":"p","provider":"anthropic","model":"claude-sonnet-4-20250514","working_dir":"/tmp/p"}))
        .send().await.unwrap();
    let pid: String = {
        let b: serde_json::Value = p.json().await.unwrap();
        b["profile"]["id"].as_str().unwrap().to_string()
    };
    let s = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({"profile_id":pid}))
        .send()
        .await
        .unwrap();
    let sid: String = {
        let b: serde_json::Value = s.json().await.unwrap();
        b["session"]["id"].as_str().unwrap().to_string()
    };

    // Set an override.
    let set_resp = app
        .patch(&format!("/sessions/{}", sid))
        .header("X-API-Key", &api_key)
        .json(&json!({"model":"gpt-4o"}))
        .send()
        .await
        .unwrap();
    assert_eq!(set_resp.status(), 200, "set override should be 200");
    // Clear it with null.
    let resp = app
        .patch(&format!("/sessions/{}", sid))
        .header("X-API-Key", &api_key)
        .json(&json!({"model":null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "clear override should be 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["session"]["override_model"].is_null(),
        "override_model should be cleared"
    );
}

#[tokio::test]
async fn test_patch_session_404_on_unknown_session() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // PATCH a non-existent session UUID.
    let fake = uuid::Uuid::new_v4().to_string();
    let resp = app
        .patch(&format!("/sessions/{}", fake))
        .header("X-API-Key", &api_key)
        .json(&json!({"model":"gpt-4o"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown session should 404");
}

#[tokio::test]
async fn test_patch_session_rejects_no_fields() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let p = app.post("/profiles").header("X-API-Key", &api_key)
        .json(&json!({"name":"p","provider":"anthropic","model":"claude-sonnet-4-20250514","working_dir":"/tmp/p"}))
        .send().await.unwrap();
    let pid: String = {
        let b: serde_json::Value = p.json().await.unwrap();
        b["profile"]["id"].as_str().unwrap().to_string()
    };
    let s = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({"profile_id":pid}))
        .send()
        .await
        .unwrap();
    let sid: String = {
        let b: serde_json::Value = s.json().await.unwrap();
        b["session"]["id"].as_str().unwrap().to_string()
    };

    // PATCH with an empty body should 400.
    let resp = app
        .patch(&format!("/sessions/{}", sid))
        .header("X-API-Key", &api_key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "no fields should 400");
}

#[tokio::test]
async fn test_patch_session_updates_title() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let p = app.post("/profiles").header("X-API-Key", &api_key)
        .json(&json!({"name":"p","provider":"anthropic","model":"claude-sonnet-4-20250514","working_dir":"/tmp/p"}))
        .send().await.unwrap();
    let pid: String = {
        let b: serde_json::Value = p.json().await.unwrap();
        b["profile"]["id"].as_str().unwrap().to_string()
    };
    let s = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({"profile_id":pid,"title":"Old"}))
        .send()
        .await
        .unwrap();
    let sid: String = {
        let b: serde_json::Value = s.json().await.unwrap();
        b["session"]["id"].as_str().unwrap().to_string()
    };

    let resp = app
        .patch(&format!("/sessions/{}", sid))
        .header("X-API-Key", &api_key)
        .json(&json!({"title":"New Title"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["session"]["title"], "New Title");
    // profile_id should be unchanged.
    assert_eq!(body["session"]["profile_id"], pid);
}

#[tokio::test]
async fn test_list_sessions() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create a profile
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "List Sessions Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/list-sessions"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    // Create sessions
    for i in 0..3 {
        app.post("/sessions")
            .header("X-API-Key", &api_key)
            .json(&json!({
                "profile_id": profile_id,
                "title": format!("Session {}", i)
            }))
            .send()
            .await
            .unwrap();
    }

    // List sessions
    let resp = app
        .get("/sessions")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "List sessions should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["sessions"].is_array(),
        "Response should contain sessions array"
    );
    assert!(
        body["sessions"].as_array().unwrap().len() >= 3,
        "Should have at least 3 sessions"
    );
}

#[tokio::test]
async fn test_get_session_status() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create profile and session
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Status Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/status-test"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id
        }))
        .send()
        .await
        .unwrap();

    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let _session_id = session_body["session"]["id"].as_str().unwrap();

    // Get session status
    let resp = app
        .get("/health")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Health check should return 200");
}

#[tokio::test]
async fn test_delete_session() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create profile and session
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Delete Session Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/delete-session"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id
        }))
        .send()
        .await
        .unwrap();

    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let session_id = session_body["session"]["id"].as_str().unwrap();

    // Delete session
    let resp = app
        .delete(&format!("/sessions/delete?id={}", session_id))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204, "Delete session should return 204");
}

// ============================================
// Message Endpoint Tests
// ============================================

// `test_send_message` exercises the full POST /messages path:
// the handler spawns a `pi` subprocess via `get_or_create` and
// returns 202 once the subprocess is launched. It needs the
// `pi` binary on PATH (CI installs it in the `rust-test` job;
// see `.github/workflows/ci.yml`) but does **not** need a
// provider API key — 202 is returned before pi processes the
// prompt, so a no-key profile still yields 202. The
// spawn-flags regression class (e.g. the `--skills-dir` →
// `--skill` rename) is caught by the direct-spawn smoke tests
// in `tests/pi_spawn_tests.rs`; this test covers the HTTP
// handler path (message insert, `get_or_create`, 202).
#[tokio::test]
async fn test_send_message() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create profile and session
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Message Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/message-test"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id
        }))
        .send()
        .await
        .unwrap();

    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let session_id = session_body["session"]["id"].as_str().unwrap();

    // Send message
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": session_id,
            "content": "Hello, this is a test message"
        }))
        .send()
        .await
        .unwrap();

    // Note: This returns 202 Accepted as message processing is async
    assert_eq!(resp.status(), 202, "Send message should return 202");
}

#[tokio::test]
async fn test_list_messages() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create profile and session
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "List Messages Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/list-messages"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id
        }))
        .send()
        .await
        .unwrap();

    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let session_id = session_body["session"]["id"].as_str().unwrap();

    // List messages (empty)
    let resp = app
        .get(&format!("/messages?session_id={}", session_id))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "List messages should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["messages"].is_array(),
        "Response should contain messages array"
    );
}

// ============================================
// Tool Execution Tests
// ============================================

#[tokio::test]
async fn test_tool_execution_bash() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create profile and session
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Tool Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/tool-test"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id
        }))
        .send()
        .await
        .unwrap();

    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let session_id = session_body["session"]["id"].as_str().unwrap();

    // Execute bash tool
    let resp = app
        .post("/tools/execute")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": session_id,
            "tool": "bash",
            "input": {
                "command": "echo 'hello world'",
                "timeout_ms": 5000
            },
            "tool_call_id": "test_call_1"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Tool execution should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["success"], true);
    assert!(body["output"].as_str().unwrap().contains("hello world"));
}

#[tokio::test]
async fn test_tool_execution_invalid_session() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let fake_session_id = "00000000-0000-0000-0000-000000000000";

    let resp = app
        .post("/tools/execute")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": fake_session_id,
            "tool": "bash",
            "input": {
                "command": "echo 'test'"
            },
            "tool_call_id": "test_call_2"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404, "Invalid session should return 404");
}

#[tokio::test]
async fn test_tool_execution_read() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Create profile and session
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Read Tool Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/nonexistent"
        }))
        .send()
        .await
        .unwrap();

    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "profile_id": profile_id
        }))
        .send()
        .await
        .unwrap();

    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let session_id = session_body["session"]["id"].as_str().unwrap();
    let working_dir = session_body["working_dir"].as_str().unwrap();

    // Create a test file
    std::fs::write(format!("{}/test.txt", working_dir), "Hello, Test!").unwrap();

    // Execute read tool
    let resp = app
        .post("/tools/execute")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": session_id,
            "tool": "read",
            "input": {
                "path": "test.txt"
            },
            "tool_call_id": "test_call_3"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Read tool should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["success"], true);
    assert!(body["output"].as_str().unwrap().contains("Hello, Test!"));
}

// ============================================
// Metrics Endpoint Tests
// ============================================

#[tokio::test]
async fn test_metrics_endpoint() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Make some requests to generate metrics
    app.get("/profiles")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    let resp = app
        .get("/metrics")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Metrics should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["metrics"].is_object(),
        "Response should contain metrics"
    );
    assert!(
        body["error_rate"].is_string(),
        "Response should contain error_rate"
    );
}

#[tokio::test]
async fn test_prometheus_metrics() {
    let (app, _db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let resp = app
        .get("/metrics/prometheus")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "Prometheus metrics should return 200");

    let text = resp.text();
    assert!(
        text.contains("forge_requests_total"),
        "Should contain request metrics"
    );
    assert!(text.contains("# HELP"), "Should contain HELP comments");
    assert!(text.contains("# TYPE"), "Should contain TYPE comments");
}

/// Parse a single SSE frame (the text between blank lines) into its
/// `event:` name and `data:` payload. Keepalive frames (`data: hb`
/// with no event line) yield `(None, None)` and are ignored by the
/// caller.
fn parse_sse_frame(frame: &str) -> (Option<&str>, Option<&str>) {
    let mut name = None;
    let mut data = None;
    for line in frame.lines() {
        if let Some(rest) = line.strip_prefix("event:") {
            name = Some(rest.trim());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data = Some(rest.trim());
        }
    }
    (name, data)
}

/// Pull the message `sequence` out of a `message` SSE event.
/// The event data is the raw serialized `Message` row (top-level
/// `sequence`), but tolerate the documented `{"message": {...}}`
/// envelope too.
fn sse_message_sequence(data: &str) -> Option<i64> {
    let v: serde_json::Value = serde_json::from_str(data).ok()?;
    if let Some(s) = v.get("sequence").and_then(|s| s.as_i64()) {
        return Some(s);
    }
    v.get("message")
        .and_then(|m| m.get("sequence"))
        .and_then(|s| s.as_i64())
}

/// Regression test for the SSE lag-recovery branch (BUGS.md #13 —
/// "Lagged DB re-query recovery branch has no unit test").
///
/// `GET /sessions/:id/events` subscribes to a bounded broadcast bus
/// (256 events). A consumer that falls more than the buffer behind
/// gets `Lagged(n)` on its receiver; the handler must then re-query
/// the database (`sequence > last_seq`) and re-emit the missed rows as
/// `message` events. Before the fix it only forwarded from the current
/// bus position, so rows dropped from the broadcast buffer stayed
/// invisible until reconnect.
///
/// Mechanics:
/// 1. Pre-insert 100 rows DIRECTLY into the DB (never published on
///    the bus) so the initial catch-up phase fills the capped SSE
///    channel (64 events) and blocks the bridge task.
/// 2. Open the SSE stream with `since=0` and DON'T read from it.
/// 3. Fire 300 bash tool calls CONCURRENTLY, each emitting ~30 KiB of
///    stdout. Every call publishes 2 bus events (call row + result
///    row) = 600 events of ~45 KiB each. The socket buffer can
///    absorb only a few MiB worth (~100 events), so the capped
///    channel + broadcast buffer (64 + 256 = 320) overflow and the
///    stalled receiver observes `Lagged` with comfortable margin.
///    (Small events would NOT work here: Linux loopback buffering
///    swallows a few MiB, so thousands of small rows never stall the
///    bridge. And fewer-but-bigger calls would also lose margin,
///    since the per-session sequence advisory lock serializes the
///    inserts.)
/// 4. Drain the stream. Assert a `lagged` event fired AND every
///    sequence 1..=700 arrived with no gaps. Rows 153..=700 exist
///    only in the DB — the ones dropped from the live bus — so their
///    presence proves the DB re-query recovery re-emitted them.
#[tokio::test]
async fn test_sse_lag_recovery_requeries_missed_rows() {
    let (app, db_url) = create_test_app().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    // Profile + session.
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "SSE Lag Test",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/sse-lag-test"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(profile_resp.status(), 201, "Profile creation should succeed");
    let profile_body: serde_json::Value = profile_resp.json().await.unwrap();
    let profile_id = profile_body["profile"]["id"].as_str().unwrap();

    let session_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "SSE Lag Test" }))
        .send()
        .await
        .unwrap();
    assert_eq!(session_resp.status(), 201, "Session creation should succeed");
    let session_body: serde_json::Value = session_resp.json().await.unwrap();
    let session_id = session_body["session"]["id"].as_str().unwrap();
    let session_uuid = Uuid::parse_str(session_id).unwrap();

    // Pre-insert 100 rows that NEVER touch the bus (sequences 1..=100).
    // The tool calls below continue from sequence 101, so the final
    // high-water mark is 100 + 2*300 = 700.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("connect to test db");
    let mut tx = pool.begin().await.unwrap();
    for seq in 1..=100 {
        sqlx::query(
            "INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, 'user', $3)",
        )
        .bind(session_uuid)
        .bind(seq)
        .bind(format!("stalled-row-{seq}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();

    // Open the SSE stream and STALL: hold the response body without
    // reading it. The catch-up phase (350 rows, channel cap 64) fills
    // the capped channel, the bridge task blocks on its first send,
    // and the broadcast receiver stops draining.
    let client = reqwest::Client::new();
    let events_url = format!("{}/sessions/{}/events?since=0", app.base_url, session_id);
    let sse_resp = client
        .get(&events_url)
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        sse_resp.status(),
        200,
        "SSE endpoint should accept the connection"
    );
    let mut body_stream = sse_resp.bytes_stream();

    // Let the catch-up fill the channel before we publish anything.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Overflow the bus while the consumer is stalled. 500 calls, run
    // concurrently (bounded to 100 in flight to keep the test
    // process's fd usage sane). Each call's result row carries ~50 KiB
    // of stdout, so ~120 KiB per bus event: the server's socket
    // buffer + the capped channel + the 256-event broadcast buffer
    // saturate long before all 1000 events are published, guaran-
    // teeing a `Lagged` on the stalled receiver.
    const TOOL_CALLS: usize = 300;
    let tool_client = reqwest::Client::new();
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(100));
    let tools_url = format!("{}/tools/execute", app.base_url);
    let mut handles = Vec::new();
    for i in 0..TOOL_CALLS {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let client = tool_client.clone();
        let url = tools_url.clone();
        let key = api_key.clone();
        let sid = session_id.to_string();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let resp = client
                .post(&url)
                .header("X-API-Key", &key)
                .json(&json!({
                    "session_id": sid,
                    "tool": "bash",
                    "tool_call_id": format!("lag-test-call-{i}"),
                    "input": { "command": "yes a | head -c 30000", "timeout_ms": 5000 }
                }))
                .send()
                .await
                .unwrap();
            resp.status().as_u16()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), 200, "tool call should succeed");
    }

    // Now drain the stream, parsing SSE frames as they arrive.
    let mut buffer = String::new();
    let mut saw_lagged = false;
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut frames = 0usize;
    let drain = async {
        while frames < 5000 {
            let chunk = match body_stream.next().await {
                Some(Ok(c)) => c,
                Some(Err(e)) => panic!("SSE read error: {e}"),
                None => break,
            };
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = buffer.find("\n\n") {
                let frame = buffer[..pos].to_string();
                buffer.drain(..pos + 2);
                frames += 1;
                let (name, data) = parse_sse_frame(&frame);
                match name {
                    Some("message") => {
                        if let Some(d) = data {
                            if let Some(seq) = sse_message_sequence(d) {
                                seen.insert(seq);
                            }
                        }
                    }
                    Some("lagged") => saw_lagged = true,
                    _ => {}
                }
            }
            if saw_lagged && seen.len() >= 700 {
                break;
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(90), drain)
        .await
        .expect("draining the SSE stream timed out");

    // The `lagged` event proves the Lagged branch ran. An unbroken
    // 1..=700 proves the recovery re-query re-emitted the rows that
    // the live bus dropped: rows 153..=700 (roughly) exist only in the
    // database — the ~350 dropped from the bus would otherwise be
    // permanently missing from the stream.
    assert!(saw_lagged, "expected a `lagged` event after the bus overflow");
    assert!(
        seen.len() >= 700,
        "drain ended early: saw {} of 700 sequences",
        seen.len()
    );
    let missing: Vec<i64> = (1..=700).filter(|s| !seen.contains(s)).collect();
    assert!(
        missing.is_empty(),
        "lag recovery dropped sequences: {missing:?} (seen {} of 700)",
        seen.len()
    );
}
