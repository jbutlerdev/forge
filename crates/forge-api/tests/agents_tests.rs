//! Agent entity integration tests (Herd H1.1 / H1.2).
//!
//! Covers the `agents` CRUD surface (mirroring the profiles tenancy
//! tests in `tenancy_tests.rs`), the agent-scoped conversation
//! endpoints, the restricted-key gate (migration 015), and the
//! `/agents/:id/tasks` + `/agents/:id/active` status endpoints.
//!
//! Note on `POST /agents/:id/conversations/:cid/messages`: only the
//! tenancy rejections are exercised here (404 for a foreign agent /
//! a conversation not owned by the agent). The positive path goes
//! through `dispatch_message`, which spawns a real `pi --mode rpc`
//! subprocess — the same reason `tenancy_tests.rs` keeps its
//! message-dispatch cases on the rejection side.

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

/// Promote an existing user to admin directly in the DB.
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
            "working_dir": "/tmp/agents-test-profile"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "profile creation should succeed: {}",
        resp.text()
    );
    Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

/// Create an agent owned by `api_key` with the given primary profile;
/// returns the agent id.
async fn create_agent(app: &TestApp, api_key: &str, name: &str, profile_id: Uuid) -> Uuid {
    let resp = app
        .post("/agents")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": name,
            "primary_profile_id": profile_id.to_string()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "agent creation should succeed: {}",
        resp.text()
    );
    Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

/// Mint a restricted (demo) API key for the user behind `api_key`;
/// the key's owner must be admin (the API enforces that).
async fn create_restricted_key(app: &TestApp, admin_key: &str) -> String {
    let resp = app
        .post("/api-keys")
        .header("X-API-Key", admin_key)
        .json(&json!({ "name": "demo-key", "restricted": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "restricted key mint should succeed: {}",
        resp.text()
    );
    resp.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string()
}

// ============================================
// CRUD
// ============================================

#[tokio::test]
async fn agent_crud_round_trip() {
    let (app, _db_url) = TestApp::new().await;
    let (_, key) = register_user(&app, "crud@example.com", "Crud").await;
    let profile = create_profile(&app, &key, "crud-profile").await;
    let agent_id = create_agent(&app, &key, "Iris", profile).await;

    // Create: defaults applied.
    let resp = app
        .get(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "get agent → 200: {}", resp.text());
    let agent = resp.json::<serde_json::Value>().await.unwrap()["agent"].clone();
    assert_eq!(agent["name"], "Iris");
    assert_eq!(agent["visibility"], "private");
    assert_eq!(agent["memory_scope"], "agent");
    assert_eq!(agent["tools_allowlist"], json!([]));
    assert_eq!(agent["credentials_scope"], json!({}));
    assert_eq!(
        Uuid::parse_str(agent["primary_profile_id"].as_str().unwrap()).unwrap(),
        profile
    );

    // List: the owner sees their agent.
    let resp = app
        .get("/agents")
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "list agents → 200: {}", resp.text());
    let names: Vec<String> = resp.json::<serde_json::Value>().await.unwrap()["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"Iris".to_string()),
        "list should contain Iris: {names:?}"
    );

    // Patch: partial update.
    let resp = app
        .patch(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &key)
        .json(&json!({
            "name": "Iris v2",
            "visibility": "org",
            "extra_instructions": "be kind",
            "tools_allowlist": ["read", "write"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "patch agent → 200: {}", resp.text());
    let agent = resp.json::<serde_json::Value>().await.unwrap()["agent"].clone();
    assert_eq!(agent["name"], "Iris v2");
    assert_eq!(agent["visibility"], "org");
    assert_eq!(agent["extra_instructions"], "be kind");
    assert_eq!(agent["tools_allowlist"], json!(["read", "write"]));

    // Patch with no fields → 400.
    let resp = app
        .patch(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "empty patch → 400: {}", resp.text());

    // Invalid visibility → 400.
    let resp = app
        .patch(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &key)
        .json(&json!({ "visibility": "public" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "bad visibility → 400: {}", resp.text());

    // Duplicate name (same owner) → 409.
    let resp = app
        .post("/agents")
        .header("X-API-Key", &key)
        .json(&json!({ "name": "Iris v2" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409, "duplicate name → 409: {}", resp.text());

    // Delete.
    let resp = app
        .delete(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "delete agent → 204: {}", resp.text());
    let resp = app
        .get(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "get deleted agent → 404: {}",
        resp.text()
    );
}

#[tokio::test]
async fn agent_points_at_foreign_profile_rejected() {
    let (app, _db_url) = TestApp::new().await;
    let (_, owner_key) = register_user(&app, "ownp@example.com", "OwnP").await;
    let profile = create_profile(&app, &owner_key, "ownp-profile").await;
    let (_, other_key) = register_user(&app, "othp@example.com", "OthP").await;

    // Agent pointing at someone else's profile → 404 (no existence
    // leak; the profile pins provider credentials).
    let resp = app
        .post("/agents")
        .header("X-API-Key", &other_key)
        .json(&json!({ "name": "Sneak", "primary_profile_id": profile.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "foreign profile → 404: {}", resp.text());

    // Unknown profile → 404 as well.
    let resp = app
        .post("/agents")
        .header("X-API-Key", &other_key)
        .json(&json!({ "name": "Sneak", "primary_profile_id": "00000000-0000-0000-0000-000000000000" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown profile → 404: {}", resp.text());
}

// ============================================
// Tenancy
// ============================================

#[tokio::test]
async fn agent_tenancy_owner_or_admin() {
    let (app, db_url) = TestApp::new().await;
    let (owner_id, owner_key) = register_user(&app, "teno@example.com", "Teno").await;
    let (_, other_key) = register_user(&app, "tenb@example.com", "TenB").await;
    let profile = create_profile(&app, &owner_key, "ten-profile").await;
    let agent_id = create_agent(&app, &owner_key, "Trixie", profile).await;

    // A second user cannot see, patch, or delete another user's agent
    // (404, not 403 — no existence leak), and does not see it in their
    // list.
    for (method, path) in [
        ("GET", &format!("/agents/{}", agent_id)),
        ("DELETE", &format!("/agents/{}", agent_id)),
    ] {
        let rb = match method {
            "GET" => app.get(path),
            _ => app.delete(path),
        };
        let resp = rb.header("X-API-Key", &other_key).send().await.unwrap();
        assert_eq!(
            resp.status(),
            404,
            "{method} foreign agent → 404: {}",
            resp.text()
        );
    }
    let resp = app
        .patch(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &other_key)
        .json(&json!({ "name": "Hacked" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "patch foreign agent → 404: {}",
        resp.text()
    );

    let resp = app
        .get("/agents")
        .header("X-API-Key", &other_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let names: Vec<String> = resp.json::<serde_json::Value>().await.unwrap()["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !names.contains(&"Trixie".to_string()),
        "foreign agent must not leak into list: {names:?}"
    );

    // Agent-scoped routes are gated the same way.
    for path in [
        format!("/agents/{}/conversations", agent_id),
        format!("/agents/{}/tasks", agent_id),
        format!("/agents/{}/active", agent_id),
    ] {
        let resp = app
            .get(&path)
            .header("X-API-Key", &other_key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "GET {path} as foreign user → 404: {}",
            resp.text()
        );
    }

    // Admin can see, patch, and delete.
    promote_to_admin(&db_url, owner_id).await;
    let resp = app
        .get(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &owner_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "admin get → 200: {}", resp.text());
    let resp = app
        .patch(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &owner_key)
        .json(&json!({ "home_machine": "mini" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "admin patch → 200: {}", resp.text());
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["agent"]["home_machine"],
        "mini"
    );
    // Admin sees both users' rows in the list…
    let (_, user_b) = register_user(&app, "tenlist@example.com", "TenList").await;
    let profile_b = create_profile(&app, &user_b, "ten-profile-b").await;
    let agent_b = create_agent(&app, &user_b, "Bee", profile_b).await;
    let resp = app
        .get("/agents")
        .header("X-API-Key", &owner_key)
        .send()
        .await
        .unwrap();
    let names: Vec<String> = resp.json::<serde_json::Value>().await.unwrap()["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"Bee".to_string()) && names.contains(&"Trixie".to_string()),
        "admin list sees all: {names:?}"
    );
    // …and can delete a foreign agent.
    let resp = app
        .delete(&format!("/agents/{}", agent_b))
        .header("X-API-Key", &owner_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "admin delete foreign agent → 204: {}",
        resp.text()
    );
}

#[tokio::test]
async fn restricted_key_cannot_manage_agents() {
    let (app, db_url) = TestApp::new().await;
    let (owner_id, owner_key) = register_user(&app, "restr@example.com", "Restr").await;
    promote_to_admin(&db_url, owner_id).await;
    let profile = create_profile(&app, &owner_key, "restr-profile").await;
    let agent_id = create_agent(&app, &owner_key, "Rexy", profile).await;
    let restricted = create_restricted_key(&app, &owner_key).await;

    // Create / patch / delete are all barred for a restricted key,
    // mirroring the profile CRUD gate (migration 015).
    let resp = app
        .post("/agents")
        .header("X-API-Key", &restricted)
        .json(&json!({ "name": "Mug" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "restricted create → 403: {}",
        resp.text()
    );
    let resp = app
        .patch(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &restricted)
        .json(&json!({ "name": "Mug" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "restricted patch → 403: {}",
        resp.text()
    );
    let resp = app
        .delete(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &restricted)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "restricted delete → 403: {}",
        resp.text()
    );

    // But the restricted key CAN still talk to the agent (reads +
    // starting a conversation are part of the demo experience).
    let resp = app
        .get(&format!("/agents/{}", agent_id))
        .header("X-API-Key", &restricted)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "restricted get → 200: {}", resp.text());
    let resp = app
        .post(&format!("/agents/{}/conversations", agent_id))
        .header("X-API-Key", &restricted)
        .json(&json!({ "title": "demo chat" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "restricted new conversation → 201: {}",
        resp.text()
    );
}

// ============================================
// Conversations
// ============================================

#[tokio::test]
async fn create_conversation_binds_agent_and_profile() {
    let (app, _db_url) = TestApp::new().await;
    let (_, key) = register_user(&app, "conv@example.com", "Conv").await;
    let profile = create_profile(&app, &key, "conv-profile").await;
    let agent_id = create_agent(&app, &key, "Nova", profile).await;

    let resp = app
        .post(&format!("/agents/{}/conversations", agent_id))
        .header("X-API-Key", &key)
        .json(&json!({ "title": "first chat" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create conversation → 201: {}",
        resp.text()
    );
    let body = resp.json::<serde_json::Value>().await.unwrap();
    let session = body["session"].clone();
    let sid = Uuid::parse_str(session["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        Uuid::parse_str(session["agent_id"].as_str().unwrap()).unwrap(),
        agent_id,
        "session.agent_id must point at the agent"
    );
    assert_eq!(
        Uuid::parse_str(session["profile_id"].as_str().unwrap()).unwrap(),
        profile,
        "session must run on the agent's primary profile"
    );
    // The response reports the conversation's working directory; it
    // must exist on disk (per-session tree when /forge/sessions is
    // writable, otherwise the profile-working_dir / home fallback).
    let working_dir = std::path::PathBuf::from(
        body["working_dir"]
            .as_str()
            .expect("working_dir in response"),
    );
    assert!(
        working_dir.is_absolute() && working_dir.is_dir(),
        "working_dir must be an existing absolute directory: {working_dir:?}"
    );

    // It shows up in the agent's conversation list (most-active
    // first) and in the owner's session list with agent_id set.
    let resp = app
        .get(&format!("/agents/{}/conversations?latest=1", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "list conversations → 200: {}",
        resp.text()
    );
    let convs = resp.json::<serde_json::Value>().await.unwrap()["conversations"].clone();
    assert_eq!(convs.as_array().unwrap().len(), 1);
    assert_eq!(convs[0]["id"], session["id"]);

    let resp = app
        .get("/sessions")
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    let sids: Vec<String> = resp.json::<serde_json::Value>().await.unwrap()["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap().to_string())
        .collect();
    assert!(sids.contains(&sid.to_string()));
}

/// Insert message rows directly in the DB (the positive message-
/// dispatch path requires a live pi subprocess, so the transcript is
/// seeded by hand).
async fn insert_messages(db_url: &str, session_id: Uuid, rows: &[(i32, &str, &str)]) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .expect("connect to test db");
    for (seq, role, content) in rows {
        sqlx::query(
            "INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, $3, $4)",
        )
        .bind(session_id)
        .bind(*seq)
        .bind(role)
        .bind(content)
        .execute(&pool)
        .await
        .unwrap();
    }
    pool.close().await;
}

async fn fetch_messages(app: &TestApp, key: &str, session_id: Uuid) -> serde_json::Value {
    let resp = app
        .get(&format!("/messages?session_id={}", session_id))
        .header("X-API-Key", key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "list messages → 200: {}", resp.text());
    resp.json::<serde_json::Value>().await.unwrap()["messages"].clone()
}

#[tokio::test]
async fn fork_from_copies_messages_with_sequence_reset() {
    let (app, db_url) = TestApp::new().await;
    let (_, key) = register_user(&app, "fork@example.com", "Fork").await;
    let profile = create_profile(&app, &key, "fork-profile").await;
    let agent_id = create_agent(&app, &key, "Foxy", profile).await;

    // Source conversation with a transcript.
    let resp = app
        .post(&format!("/agents/{}/conversations", agent_id))
        .header("X-API-Key", &key)
        .json(&json!({ "title": "source" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "source conversation → 201: {}",
        resp.text()
    );
    let src_id: Uuid = Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    // Deliberately non-contiguous sequence numbers: a proper reset
    // renumbers 1..N regardless of the source's gaps.
    insert_messages(
        &db_url,
        src_id,
        &[
            (3, "user", "hello"),
            (5, "assistant", "hi"),
            (9, "user", "bye"),
        ],
    )
    .await;

    // Fork it.
    let resp = app
        .post(&format!("/agents/{}/conversations", agent_id))
        .header("X-API-Key", &key)
        .json(&json!({ "title": "branch", "fork_from": src_id.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "fork → 201: {}", resp.text());
    let forked: Uuid = Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_ne!(forked, src_id);

    // The fork's transcript: same rows, same order, sequences 1..3.
    let msgs = fetch_messages(&app, &key, forked).await;
    let arr = msgs.as_array().unwrap();
    assert_eq!(arr.len(), 3, "fork copied all 3 rows: {msgs:?}");
    let got: Vec<(i32, &str, &str)> = arr
        .iter()
        .map(|m| {
            (
                m["sequence"].as_i64().unwrap() as i32,
                m["role"].as_str().unwrap(),
                m["content"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(got[0].0, 1, "sequence reset to start at 1");
    assert_eq!(got[1].0, 2);
    assert_eq!(got[2].0, 3);
    assert_eq!((got[0].2, got[1].2, got[2].2), ("hello", "hi", "bye"));
    assert_eq!(
        (got[0].1, got[1].1, got[2].1),
        ("user", "assistant", "user")
    );

    // The source transcript is untouched.
    let src_msgs = fetch_messages(&app, &key, src_id).await;
    assert_eq!(src_msgs.as_array().unwrap().len(), 3);

    // Both conversations list under the agent, most-active first
    // (the fork was created later).
    let resp = app
        .get(&format!("/agents/{}/conversations", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    let convs = resp.json::<serde_json::Value>().await.unwrap()["conversations"].clone();
    assert_eq!(convs.as_array().unwrap().len(), 2);
    assert_eq!(
        convs[0]["id"],
        forked.to_string(),
        "most-active (fork) first"
    );
}

#[tokio::test]
async fn fork_from_across_agents_rejected() {
    let (app, db_url) = TestApp::new().await;
    let (_, key) = register_user(&app, "xfork@example.com", "XFork").await;
    let profile_a = create_profile(&app, &key, "xa-profile").await;
    let profile_b = create_profile(&app, &key, "xb-profile").await;
    let agent_a = create_agent(&app, &key, "Ada", profile_a).await;
    let agent_b = create_agent(&app, &key, "Bob", profile_b).await;

    let resp = app
        .post(&format!("/agents/{}/conversations", agent_a))
        .header("X-API-Key", &key)
        .json(&json!({ "title": "a-chat" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create conversation → 201: {}",
        resp.text()
    );
    let a_conv: Uuid = Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    insert_messages(&db_url, a_conv, &[(1, "user", "secret")]).await;

    // Forking Ada's conversation from Bob → 400 (cross-agent forks
    // would leak Ada's transcript into Bob's identity).
    let resp = app
        .post(&format!("/agents/{}/conversations", agent_b))
        .header("X-API-Key", &key)
        .json(&json!({ "title": "b-branch", "fork_from": a_conv.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "cross-agent fork → 400: {}",
        resp.text()
    );

    // Forking a nonexistent session → 400 as well (the status must
    // not depend on whether the session exists).
    let resp = app
        .post(&format!("/agents/{}/conversations", agent_b))
        .header("X-API-Key", &key)
        .json(&json!({ "fork_from": "00000000-0000-0000-0000-000000000000" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "missing fork source → 400: {}",
        resp.text()
    );

    // And Bob really has no conversations.
    let resp = app
        .get(&format!("/agents/{}/conversations", agent_b))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["conversations"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

// ============================================
// Tasks + active status
// ============================================

#[tokio::test]
async fn agent_tasks_empty_for_agent_without_conversations() {
    let (app, db_url) = TestApp::new().await;
    let (_, key) = register_user(&app, "task@example.com", "Task").await;
    let profile = create_profile(&app, &key, "task-profile").await;
    let agent_id = create_agent(&app, &key, "Tess", profile).await;

    // The test DBs carry no durable-pg migrations: create the
    // (empty) task table the route reads, so the join has a target.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(&db_url)
        .expect("lazy test pool");
    sqlx::query(
        "CREATE TABLE durable_tasks (
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            abort_requested BOOLEAN NOT NULL,
            background BOOLEAN NOT NULL,
            record TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create durable_tasks");

    let resp = app
        .get(&format!("/agents/{}/tasks", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "tasks → 200: {}", resp.text());
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v,
        json!({ "tasks": [] }),
        "an agent with no stamped sessions has no tasks"
    );
    pool.close().await;
}

#[tokio::test]
async fn agent_active_reports_most_active_conversation() {
    let (app, db_url) = TestApp::new().await;
    let (_, key) = register_user(&app, "active@example.com", "Active").await;
    let profile = create_profile(&app, &key, "active-profile").await;
    let agent_id = create_agent(&app, &key, "Viv", profile).await;

    // No conversations yet: busy=false, no current conversation.
    let resp = app
        .get(&format!("/agents/{}/active", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "active → 200: {}", resp.text());
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["busy"], false);
    assert!(v["current_conversation"].is_null());

    // Two conversations; touch the second one so it is most-active.
    let mut convs = Vec::new();
    for t in ["older", "newer"] {
        let resp = app
            .post(&format!("/agents/{}/conversations", agent_id))
            .header("X-API-Key", &key)
            .json(&json!({ "title": t }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            201,
            "create conversation → 201: {}",
            resp.text()
        );
        let sid: Uuid = Uuid::parse_str(
            resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&db_url)
            .await
            .unwrap();
        sqlx::query("UPDATE sessions SET last_active = NOW() + $1::interval WHERE id = $2")
            .bind(if t == "newer" {
                "5 minutes"
            } else {
                "0 seconds"
            })
            .bind(sid)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        convs.push(sid);
    }

    let resp = app
        .get(&format!("/agents/{}/active", agent_id))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["busy"], false, "no live turn driver in tests → not busy");
    assert_eq!(
        Uuid::parse_str(v["current_conversation"].as_str().unwrap()).unwrap(),
        convs[1],
        "current_conversation = most-active session"
    );
}

// ============================================
// Agent-scoped message dispatch (H1.2)
// ============================================

#[tokio::test]
async fn agent_message_endpoint_tenancy_gates() {
    let (app, _db_url) = TestApp::new().await;
    let (_, owner_key) = register_user(&app, "msgo@example.com", "MsgO").await;
    let (_, other_key) = register_user(&app, "msgr@example.com", "MsgR").await;
    let profile = create_profile(&app, &owner_key, "msg-profile").await;
    let agent_id = create_agent(&app, &owner_key, "Meg", profile).await;

    let resp = app
        .post(&format!("/agents/{}/conversations", agent_id))
        .header("X-API-Key", &owner_key)
        .json(&json!({ "title": "chat" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create conversation → 201: {}",
        resp.text()
    );
    let conv_id: Uuid = Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    // Foreign user's key → 404 (the agent gate).
    let resp = app
        .post(&format!(
            "/agents/{}/conversations/{}/messages",
            agent_id, conv_id
        ))
        .header("X-API-Key", &other_key)
        .json(&json!({ "content": "ping" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "foreign agent → 404: {}", resp.text());

    // The owner posting to a session that does NOT belong to this
    // agent (a plain session on the same profile) → 404.
    let resp = app
        .post("/sessions")
        .header("X-API-Key", &owner_key)
        .json(&json!({ "profile_id": profile.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "plain session → 201: {}", resp.text());
    let plain: Uuid = Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let resp = app
        .post(&format!(
            "/agents/{}/conversations/{}/messages",
            agent_id, plain
        ))
        .header("X-API-Key", &owner_key)
        .json(&json!({ "content": "ping" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "conversation not of this agent → 404: {}",
        resp.text()
    );

    // Bogus conversation id → 404 as well.
    let resp = app
        .post(&format!(
            "/agents/{}/conversations/{}/messages",
            agent_id, "00000000-0000-0000-0000-000000000000"
        ))
        .header("X-API-Key", &owner_key)
        .json(&json!({ "content": "ping" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "missing conversation → 404: {}",
        resp.text()
    );

    // Note: the positive path (owner + agent conversation) runs the
    // shared `dispatch_message` turn driver, which spawns a real
    // `pi --mode rpc` subprocess; that is covered by the existing
    // message/turn test suites, not here.
}
