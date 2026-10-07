//! Herd H2.1 — the turn, re-stated: unit coverage of the routing
//! rules WITHOUT a live harness.
//!
//! * Flag off → zero behavior change: session creation never touches
//!   the durable column, even with an enabled (unreachable) harness
//!   handle.
//! * Harness failure at creation → the session still lands (201) and
//!   stays legacy (`durable_conversation_id` NULL) — creation never
//!   fails because of the harness.
//! * Stamped session + flag on → `POST /messages` routes to
//!   `harness.submit` (not the legacy `drive_turn`): with a disabled
//!   client that surfaces as 503 "harness submit failed", and the
//!   user row is already persisted.
//! * Forks (`fork_from`) stay legacy when the flag is on (no harness
//!   fork semantics yet — H2.2).
//!
//! The live end-to-end turn (real child harness, faux provider,
//! assistant projection onto `messages`) is
//! `tests/harness_turn_tests.rs`.

use serde_json::json;
use sqlx::postgres::PgPoolOptions;

mod test_helpers;

use forge_harness_client::Limits;

/// Enabled harness client pointing at sockets that never exist:
/// `create_conversation` fails fast with `Disconnected` after the
/// (short) disconnect-wait bound. Exercises every "harness error →
/// legacy fallback" path without a process.
fn enabled_unreachable_harness() -> forge_api::harness::HarnessState {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Leak the tempdir for the test's lifetime: the redial loops hold
    // the paths, and the sockets never materialize anyway.
    let tmp = std::mem::ManuallyDrop::new(tmp);
    forge_api::harness::HarnessState::from_paths_with(
        tmp.path().join("rpc.sock").as_path(),
        tmp.path().join("events.sock").as_path(),
        Limits {
            min_backoff: std::time::Duration::from_millis(5),
            max_backoff: std::time::Duration::from_millis(20),
            disconnect_wait: std::time::Duration::from_millis(150),
            response_timeout: std::time::Duration::from_millis(500),
            event_send_timeout: std::time::Duration::from_millis(500),
        },
        "public",
    )
}

fn auth_client_ip() -> String {
    use rand::Rng;
    let b: [u8; 3] = rand::thread_rng().gen();
    format!("10.{}.{}.{}", b[0], b[1], b[2])
}

async fn register_and_login(app: &test_helpers::TestApp) -> String {
    let register_resp = app
        .post("/auth/register")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({
            "email": "h21-unit@example.com",
            "name": "H21 Unit User",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(register_resp.status(), 201);
    let login_resp = app
        .post("/auth/login")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({
            "email": "h21-unit@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(login_resp.status(), 200);
    let body: serde_json::Value = login_resp.json().await.unwrap();
    body["api_key"].as_str().unwrap().to_string()
}

async fn create_profile(app: &test_helpers::TestApp, api_key: &str) -> String {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": "H21 Unit Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, 201, "profile creation: {body}");
    body["profile"]["id"].as_str().unwrap().to_string()
}

async fn pool(db_url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .unwrap()
}

async fn durable_conversation_id(db_url: &str, session_id: &str) -> Option<i64> {
    let p = pool(db_url).await;
    let v: Option<i64> =
        sqlx::query_scalar("SELECT durable_conversation_id FROM sessions WHERE id = $1")
            .bind(uuid::Uuid::parse_str(session_id).unwrap())
            .fetch_one(&p)
            .await
            .unwrap();
    p.close().await;
    v
}

/// Flag OFF + enabled (unreachable) harness: new sessions are
/// legacy — the durable column is never touched and no harness RPC is
/// attempted (so this does not even pay the disconnect-wait).
#[tokio::test]
async fn flag_off_new_session_stays_legacy() {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(enabled_unreachable_harness(), false).await;
    let api_key = register_and_login(&app).await;
    let profile_id = create_profile(&app, &api_key).await;

    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "flag off" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    let session_id = resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    assert_eq!(
        durable_conversation_id(&db_url, &session_id).await,
        None,
        "flag off: session must stay legacy"
    );
}

/// Flag ON + harness fails at creation (unreachable socket): the
/// session still lands (201) and stays legacy — creation never fails
/// because of the harness.
#[tokio::test]
async fn harness_failure_at_creation_falls_back_to_legacy() {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(enabled_unreachable_harness(), true).await;
    let api_key = register_and_login(&app).await;
    let profile_id = create_profile(&app, &api_key).await;

    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "fallback" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "session creation must survive a harness failure: {}",
        resp.text()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let session_id = body["session"]["id"].as_str().unwrap().to_string();
    assert!(
        body["session"]["durable_conversation_id"].is_null(),
        "response session must be unstamped: {body}"
    );

    assert_eq!(durable_conversation_id(&db_url, &session_id).await, None);
}

/// Stamped session + flag ON: `POST /messages` routes to
/// `harness.submit` — with a disabled client that is 503
/// "harness submit failed" (NOT the legacy path, which would go on to
/// spawn pi / 500 "Failed to create agent"). The user row IS
/// persisted, and the response error says a resubmit mints a fresh
/// request id.
#[tokio::test]
async fn stamped_session_message_dispatch_goes_to_harness() {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(forge_api::harness::HarnessState::disabled(), true)
            .await;
    let api_key = register_and_login(&app).await;
    let profile_id = create_profile(&app, &api_key).await;

    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "stamped" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    let session_id = resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Stamp the session as the H2.1 cutover would (the harness RPC
    // itself is exercised by harness_turn_tests.rs).
    let p = pool(&db_url).await;
    sqlx::query("UPDATE sessions SET durable_conversation_id = $1 WHERE id = $2")
        .bind(999_999i64)
        .bind(uuid::Uuid::parse_str(&session_id).unwrap())
        .execute(&p)
        .await
        .unwrap();

    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": "hello" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "harness submit failure must surface as 503, got: {}",
        resp.text()
    );
    let text = resp.text();
    assert!(text.contains("harness submit failed"), "{text}");
    assert!(
        text.contains("resubmitting mints a fresh request id"),
        "{text}"
    );

    // The user row was persisted before the submit attempt.
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages WHERE session_id = $1 AND role = 'user' AND content = 'hello'",
    )
    .bind(uuid::Uuid::parse_str(&session_id).unwrap())
    .fetch_one(&p)
    .await
    .unwrap();
    p.close().await;
    assert_eq!(rows, 1, "user row must be persisted despite the 503");
}

/// Forks stay legacy when the flag is ON: the harness IPC has no fork
/// semantics yet (H2.2), and a forked session must not carry a stamp
/// (its copied messages would not exist in a fresh durable
/// conversation).
#[tokio::test]
async fn forked_agent_conversation_stays_legacy() {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(enabled_unreachable_harness(), true).await;
    let api_key = register_and_login(&app).await;
    let profile_id = create_profile(&app, &api_key).await;

    let agent_resp = app
        .post("/agents")
        .header("X-API-Key", &api_key)
        .json(&json!({ "name": "H21 Unit Agent", "primary_profile_id": profile_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(agent_resp.status(), 201, "{}", agent_resp.text());
    let agent_id = agent_resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Source conversation (harness unreachable → legacy fallback).
    let src_resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "title": "source" }))
        .send()
        .await
        .unwrap();
    assert_eq!(src_resp.status(), 201, "{}", src_resp.text());
    let source_id = src_resp
        .json::<serde_json::Value>()
        .await
        .unwrap()
        .get("session")
        .and_then(|s| s.get("id"))
        .and_then(|i| i.as_str())
        .unwrap()
        .to_string();

    // Give the source a message row to copy (direct SQL — driving a
    // real turn on an unreachable harness is out of scope here).
    let p = pool(&db_url).await;
    sqlx::query(
        r#"INSERT INTO messages (session_id, sequence, role, content)
           VALUES ($1, get_next_sequence($1), 'user', 'seed')"#,
    )
    .bind(uuid::Uuid::parse_str(&source_id).unwrap())
    .execute(&p)
    .await
    .unwrap();

    // The fork: 201, stamp stays NULL even with the flag on.
    let fork_resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "title": "fork", "fork_from": source_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(fork_resp.status(), 201, "{}", fork_resp.text());
    let body: serde_json::Value = fork_resp.json().await.unwrap();
    let fork_id = body["session"]["id"].as_str().unwrap().to_string();
    assert!(
        body["session"]["durable_conversation_id"].is_null(),
        "fork must stay legacy: {body}"
    );
    assert_eq!(durable_conversation_id(&db_url, &fork_id).await, None);

    // And the fork actually copied the source's rows.
    let copied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1")
        .bind(uuid::Uuid::parse_str(&fork_id).unwrap())
        .fetch_one(&p)
        .await
        .unwrap();
    p.close().await;
    assert_eq!(copied, 1, "fork must copy the source's message rows");
}
