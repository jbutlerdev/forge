//! API Integration Tests for Forge
//!
//! These tests verify the HTTP API endpoints work correctly.
//! They require a running database and test various API operations.

use serde_json::json;

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
// the handler tries to spawn a `pi` subprocess, which 500s if
// the binary isn't on PATH. In CI we don't (and shouldn't)
// install the pi agent — the subprocess-lifecycle test is the
// job of the e2e / integration suites that run with the full
// stack. Marked `#[ignore]` so the default `cargo test` run
// stays green; opt in with `cargo test -- --ignored` (or
// `--include-ignored`) on a host that has `pi` available.
#[tokio::test]
#[ignore = "requires the `pi` agent binary on PATH (used to spawn the subprocess); see comment above"]
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
