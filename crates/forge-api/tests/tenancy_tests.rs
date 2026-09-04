//! Multi-tenancy (P1-15) integration tests.
//!
//! Verifies the owner-or-admin ownership gate on profiles, sessions,
//! messages, and the tool-execution endpoints:
//!
//! - creates stamp the caller's `user_id`
//! - lists return only the caller's rows (admins see all)
//! - get/patch/delete on another user's row → 404 (no existence leak)
//! - the tool-execution tenancy gate rejects a foreign user key with 404
//! - the OpenAI surface's model-resolution gate rejects a foreign profile
//!   / session with 404 *before* any pi spawn
//! - legacy `user_id IS NULL` rows are admin-only
//!
//! All negative cases short-circuit at the ownership gate, so none of
//! them require the `pi` binary on PATH and they run in CI.

mod test_helpers;

use serde_json::json;
use test_helpers::TestApp;
use uuid::Uuid;

// ============================================
// Helpers
// ============================================

const PASSWORD: &str = "password123";

/// Register a new user and log in, returning `(user_id, api_key)`.
async fn register_user(app: &TestApp, email: &str, name: &str) -> (Uuid, String) {
    let resp = app
        .post("/auth/register")
        .json(&json!({ "email": email, "name": name, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "register {} → 201", email);
    let user_id: Uuid = resp.json::<serde_json::Value>().await.unwrap()["user"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let resp = app
        .post("/auth/login")
        .json(&json!({ "email": email, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "login {} → 200", email);
    let api_key = resp.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string();

    (user_id, api_key)
}

/// Promote an existing user to admin directly in the DB. (The API has
/// no register-as-admin path and no bootstrap env var is set in
/// tests, so this is the way to get an admin key.)
async fn promote_to_admin(db_url: &str, user_id: Uuid) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .expect("connect to test db");
    sqlx::query("UPDATE users SET role = 'admin' WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("promote user to admin");
    pool.close().await;
}

/// Create a forge profile owned by `api_key`; returns the profile id.
async fn create_profile(app: &TestApp, api_key: &str, name: &str) -> Uuid {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": name,
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/tenancy-test-profile"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "profile creation should succeed");
    let id = resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    Uuid::parse_str(&id).unwrap()
}

/// Create a session from `profile_id` as `api_key`; returns the session id.
async fn create_session(app: &TestApp, api_key: &str, profile_id: Uuid) -> Uuid {
    let resp = app
        .post("/sessions")
        .header("X-API-Key", api_key)
        .json(&json!({ "profile_id": profile_id.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "session creation should succeed: {}",
        resp.text()
    );
    let id = resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    Uuid::parse_str(&id).unwrap()
}

/// Insert a legacy pre-tenancy row directly in the DB (`user_id IS NULL`).
async fn insert_legacy_profile(db_url: &str, name: &str) -> Uuid {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .expect("connect to test db");
    let id: Uuid = sqlx::query_scalar(
        r#"INSERT INTO profiles
           (name, description, provider, model, base_url, api_key, working_dir,
            git_url, git_ref, nix_shell, system_prompt, tools, user_id)
           VALUES ($1, NULL, 'anthropic', 'claude-sonnet-4-20250514', NULL, NULL,
                   '/tmp/legacy', NULL, NULL, NULL, 'You are a helpful coding assistant.',
                   '["bash", "read", "write", "edit"]', NULL)
           RETURNING id"#,
    )
    .bind(name)
    .fetch_one(&pool)
    .await
    .expect("insert legacy profile");
    pool.close().await;
    id
}

/// Collect the profile names visible on `GET /profiles` for a key.
async fn list_profile_names(app: &TestApp, api_key: &str) -> Vec<String> {
    let resp = app
        .get("/profiles")
        .header("X-API-Key", api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let names = resp.json::<serde_json::Value>().await.unwrap()["profiles"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    names
        .iter()
        .map(|p| p["name"].as_str().unwrap_or("").to_string())
        .collect()
}

// ============================================
// Profiles
// ============================================

/// 1. `POST /profiles` stamps the caller's user_id.
#[tokio::test]
async fn profile_create_stamps_user_id() {
    let (app, db_url) = TestApp::new().await;
    let (user_a, key_a) = register_user(&app, "alice@tenancy.test", "Alice").await;

    let profile_id = create_profile(&app, &key_a, "alice-profile").await;

    let owner: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM profiles WHERE id = $1")
        .bind(profile_id)
        .fetch_one(&sqlx::PgPool::connect(&db_url).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(user_a),
        "created profile must be owned by the caller"
    );
}

/// 2. User B cannot get/list/patch/delete A's profile; 404, not 403.
#[tokio::test]
async fn profile_cross_user_isolation() {
    let (app, _db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@iso.test", "Alice").await;
    let (_ub, key_b) = register_user(&app, "bob@iso.test", "Bob").await;
    let profile_id = create_profile(&app, &key_a, "alice-secret-profile").await;

    // B cannot get it (path- and query-based routes alike).
    for path in [
        format!("/profiles/{}", profile_id),
        format!("/profiles/get?id={}", profile_id),
    ] {
        let r = app
            .get(&path)
            .header("X-API-Key", &key_b)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404, "B's GET {path} → 404");
    }

    // B cannot patch it.
    let r = app
        .patch(&format!("/profiles/update?id={}", profile_id))
        .header("X-API-Key", &key_b)
        .json(&json!({ "description": "pwned" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "B's PATCH → 404");

    // B cannot delete it.
    for path in [
        format!("/profiles/{}", profile_id),
        format!("/profiles/delete?id={}", profile_id),
    ] {
        let r = app
            .delete(&path)
            .header("X-API-Key", &key_b)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404, "B's DELETE {path} → 404");
    }

    // B's list does not contain A's profile.
    assert!(
        !list_profile_names(&app, &key_b)
            .await
            .iter()
            .any(|n| n == "alice-secret-profile"),
        "B's list must not include A's profile"
    );

    // A's row is untouched and still visible to A.
    let r = app
        .get(&format!("/profiles/{}", profile_id))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["profile"]["description"], serde_json::Value::Null);
}

/// 3. The owner can get / patch / delete their own profile.
#[tokio::test]
async fn profile_owner_can_access() {
    let (app, _db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@own.test", "Alice").await;
    let profile_id = create_profile(&app, &key_a, "alice-own").await;

    // get (both routes)
    for path in [
        format!("/profiles/{}", profile_id),
        format!("/profiles/get?id={}", profile_id),
    ] {
        let r = app
            .get(&path)
            .header("X-API-Key", &key_a)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "owner GET {path} → 200");
    }

    // patch
    let r = app
        .patch(&format!("/profiles/update?id={}", profile_id))
        .header("X-API-Key", &key_a)
        .json(&json!({ "description": "mine" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "owner PATCH → 200");

    // delete
    let r = app
        .delete(&format!("/profiles/{}", profile_id))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        204,
        "owner DELETE (path) → 204 (got {})",
        r.status()
    );
    // The query-based delete confirms the row is actually gone.
    let r = app
        .delete(&format!("/profiles/delete?id={}", profile_id))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "second DELETE → 404 (row gone)");

    // Gone now.
    let r = app
        .get(&format!("/profiles/{}", profile_id))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

/// 4. An admin can get / patch / delete another user's profile and
/// sees it in the admin's list.
#[tokio::test]
async fn profile_admin_can_access_foreign_profile() {
    let (app, db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@admin.test", "Alice").await;
    let (admin_id, key_admin) = register_user(&app, "root@admin.test", "Root").await;
    promote_to_admin(&db_url, admin_id).await;
    let profile_id = create_profile(&app, &key_a, "alice-admin-target").await;

    // Admin sees it in the list.
    assert!(
        list_profile_names(&app, &key_admin)
            .await
            .iter()
            .any(|n| n == "alice-admin-target"),
        "admin's list must include the foreign profile"
    );

    // Admin can get + patch.
    let r = app
        .get(&format!("/profiles/{}", profile_id))
        .header("X-API-Key", &key_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "admin GET → 200");
    let r = app
        .patch(&format!("/profiles/update?id={}", profile_id))
        .header("X-API-Key", &key_admin)
        .json(&json!({ "description": "admin-edited" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "admin PATCH → 200");

    // Admin can delete.
    let r = app
        .delete(&format!("/profiles/{}", profile_id))
        .header("X-API-Key", &key_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204, "admin DELETE → 204");
}

/// Legacy `user_id IS NULL` profile: admin-only.
#[tokio::test]
async fn profile_legacy_null_owner_is_admin_only() {
    let (app, db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@legacy.test", "Alice").await;
    let (admin_id, key_admin) = register_user(&app, "root@legacy.test", "Root").await;
    promote_to_admin(&db_url, admin_id).await;

    let legacy_id = insert_legacy_profile(&db_url, "legacy-profile").await;

    // Regular user: invisible and inaccessible.
    assert!(
        !list_profile_names(&app, &key_a)
            .await
            .iter()
            .any(|n| n == "legacy-profile"),
        "user must not see legacy (NULL-owner) profiles"
    );
    let r = app
        .get(&format!("/profiles/{}", legacy_id))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "user GET on legacy profile → 404");

    // Admin: visible and accessible.
    assert!(
        list_profile_names(&app, &key_admin)
            .await
            .iter()
            .any(|n| n == "legacy-profile"),
        "admin must see legacy profiles"
    );
    let r = app
        .get(&format!("/profiles/{}", legacy_id))
        .header("X-API-Key", &key_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "admin GET on legacy profile → 200");
}

// ============================================
// Sessions
// ============================================

/// 5. Session matrix: B cannot create from A's profile; B cannot
/// get/list/patch/delete A's session; B's own session works; admin
/// can access A's session.
#[tokio::test]
async fn session_cross_user_isolation() {
    let (app, db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@sess.test", "Alice").await;
    let (_ub, key_b) = register_user(&app, "bob@sess.test", "Bob").await;
    let (admin_id, key_admin) = register_user(&app, "root@sess.test", "Root").await;
    promote_to_admin(&db_url, admin_id).await;

    let profile_a = create_profile(&app, &key_a, "alice-sess-profile").await;
    let session_a = create_session(&app, &key_a, profile_a).await;

    // B cannot create a session from A's profile.
    let r = app
        .post("/sessions")
        .header("X-API-Key", &key_b)
        .json(&json!({ "profile_id": profile_a.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "B creating session from A's profile → 404");

    // B cannot get A's session.
    for path in [
        format!("/sessions/{}", session_a),
        format!("/sessions/get?id={}", session_a),
    ] {
        let r = app
            .get(&path)
            .header("X-API-Key", &key_b)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404, "B's GET {path} → 404");
    }

    // B's list does not contain A's session.
    let r = app
        .get("/sessions")
        .header("X-API-Key", &key_b)
        .send()
        .await
        .unwrap();
    let ids: Vec<Uuid> = r.json::<serde_json::Value>().await.unwrap()["sessions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|s| s["id"].as_str().unwrap().parse().unwrap())
        .collect();
    assert!(
        !ids.contains(&session_a),
        "B's list must not include A's session"
    );

    // B's own session works end-to-end.
    let profile_b = create_profile(&app, &key_b, "bob-sess-profile").await;
    let session_b = create_session(&app, &key_b, profile_b).await;
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
        .bind(session_b)
        .fetch_one(&sqlx::PgPool::connect(&db_url).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(_ub),
        "created session must be owned by the caller"
    );

    // B cannot patch or delete A's session.
    let r = app
        .patch(&format!("/sessions/{}", session_a))
        .header("X-API-Key", &key_b)
        .json(&json!({ "title": "pwned" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "B's PATCH on A's session → 404");
    for path in [
        format!("/sessions/{}", session_a),
        format!("/sessions/delete?id={}", session_a),
    ] {
        let r = app
            .delete(&path)
            .header("X-API-Key", &key_b)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404, "B's DELETE {path} → 404");
    }

    // Admin can get + delete A's session.
    let r = app
        .get(&format!("/sessions/{}", session_a))
        .header("X-API-Key", &key_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "admin GET on A's session → 200");
    let r = app
        .delete(&format!("/sessions/{}", session_a))
        .header("X-API-Key", &key_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204, "admin DELETE on A's session → 204");

    // B's session is untouched.
    let r = app
        .get(&format!("/sessions/{}", session_b))
        .header("X-API-Key", &key_b)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

// ============================================
// Messages
// ============================================

/// 6. B cannot list or post to A's session; A and admin can list.
#[tokio::test]
async fn messages_cross_user_isolation() {
    let (app, _db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@msg.test", "Alice").await;
    let (_ub, key_b) = register_user(&app, "bob@msg.test", "Bob").await;
    let (admin_id, key_admin) = register_user(&app, "root@msg.test", "Root").await;
    let db_url = _db_url.clone();
    let pool = sqlx::PgPool::connect(&db_url).await.unwrap();
    sqlx::query("UPDATE users SET role = 'admin' WHERE email = 'root@msg.test'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let _ = admin_id;

    let profile_a = create_profile(&app, &key_a, "alice-msg-profile").await;
    let session_a = create_session(&app, &key_a, profile_a).await;

    // B cannot list messages.
    let r = app
        .get(&format!("/messages?session_id={}", session_a))
        .header("X-API-Key", &key_b)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "B's GET /messages on A's session → 404");

    // B cannot post a message.
    let r = app
        .post("/messages")
        .header("X-API-Key", &key_b)
        .json(&json!({ "session_id": session_a.to_string(), "content": "hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "B's POST /messages on A's session → 404");

    // A can list their own (empty) message history.
    let r = app
        .get(&format!("/messages?session_id={}", session_a))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "A's GET /messages → 200");
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(body["messages"].as_array().unwrap().is_empty());

    // Admin can list A's messages.
    let r = app
        .get(&format!("/messages?session_id={}", session_a))
        .header("X-API-Key", &key_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        200,
        "admin's GET /messages on A's session → 200"
    );
}

// ============================================
// Tool execution
// ============================================

/// 7. `POST /tools/execute` with a user key against a foreign session
/// → 404 before any tool runs. The owner passes the gate (the bash
/// itself may fail without a sandbox, but must not be a 404/401).
#[tokio::test]
async fn tool_execute_foreign_session_rejected() {
    let (app, _db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@tool.test", "Alice").await;
    let (_ub, key_b) = register_user(&app, "bob@tool.test", "Bob").await;

    let profile_a = create_profile(&app, &key_a, "alice-tool-profile").await;
    let session_a = create_session(&app, &key_a, profile_a).await;

    let body = json!({
        "session_id": session_a.to_string(),
        "tool": "bash",
        "input": { "command": "echo pwned" }
    });

    // B is rejected with 404.
    let r = app
        .post("/tools/execute")
        .header("X-API-Key", &key_b)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        404,
        "B's POST /tools/execute on A's session → 404 (got {})",
        r.status()
    );

    // A (the owner) passes the gate: 200, and the tool actually ran
    // (host-side, since the test session has no nspawn container).
    let r = app
        .post("/tools/execute")
        .header("X-API-Key", &key_a)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        200,
        "A's POST /tools/execute on own session → 200: {}",
        r.text()
    );
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["success"], true);
    assert_eq!(v["output"], "pwned\n");
}

// ============================================
// OpenAI surface
// ============================================

/// 8. OpenAI model resolution gate: B cannot resolve A's profile name
/// (stateless) or A's session id (stateful) → 404, before any pi spawn.
#[tokio::test]
async fn openai_respects_ownership() {
    let (app, _db_url) = TestApp::new().await;
    let (_ua, key_a) = register_user(&app, "alice@openai.test", "Alice").await;
    let (_ub, key_b) = register_user(&app, "bob@openai.test", "Bob").await;

    let profile_a = create_profile(&app, &key_a, "alice-openai-profile").await;
    let session_a = create_session(&app, &key_a, profile_a).await;

    // Stateless: B uses A's profile name → 404 model_not_found.
    let r = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", key_b))
        .json(&json!({
            "model": "alice-openai-profile",
            "messages": [{ "role": "user", "content": "hi" }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        404,
        "B's stateless completion on A's profile → 404 (got {})",
        r.status()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "model_not_found");

    // Stateful: B reuses A's session → 404.
    let r = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", key_b))
        .json(&json!({
            "model": format!("forge:{}", session_a),
            "messages": [{ "role": "user", "content": "hi" }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        404,
        "B's stateful completion on A's session → 404 (got {})",
        r.status()
    );

    // B's model list does not include A's profile.
    let r = app
        .get("/v1/models")
        .header("Authorization", &format!("Bearer {}", key_b))
        .send()
        .await
        .unwrap();
    let ids: Vec<String> = r.json::<serde_json::Value>().await.unwrap()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !ids.iter().any(|i| i == "alice-openai-profile"),
        "B's /v1/models must not list A's profile"
    );
}
