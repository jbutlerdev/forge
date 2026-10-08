//! Herd H2.6 — post-cutover turn routing, unit coverage WITHOUT a
//! live harness.
//!
//! * Kill switch OFF (`FORGE_HARNESS_MESSAGES=0`) → session creation
//!   never attaches (session stays unstamped, zero harness contact)
//!   AND every write is a 503 "harness disabled" — there is no
//!   legacy fallback left.
//! * Harness failure at creation (unreachable socket) → the session
//!   still lands (201) and stays UNSTAMPED (creation never fails
//!   because of the harness); its first write then attempts the lazy
//!   migration, which fails 503 on the unreachable harness, and the
//!   claim is released so the next write retries.
//! * Stamped session → `POST /messages` routes to `harness.submit`:
//!   with a disabled client that surfaces as 503 "harness submit
//!   failed", and the user row is already persisted.
//! * Forks (`fork_from`) stay unstamped at creation (no harness fork
//!   semantics yet); their first write lazy-migrates like any other
//!   session.
//!
//! The live end-to-end turn (real child harness, faux provider,
//! assistant projection onto `messages`) is
//! `tests/harness_turn_tests.rs`; the live lazy-migration tests are
//! `tests/harness_migration_tests.rs`.

use serde_json::json;
use sqlx::postgres::PgPoolOptions;

mod test_helpers;

use forge_harness_client::Limits;

/// Enabled harness client pointing at sockets that never exist:
/// `create_conversation` fails fast with `Disconnected` after the
/// (short) disconnect-wait bound. Exercises every "harness error"
/// path without a process.
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

/// Kill switch OFF + enabled (unreachable) harness: new sessions
/// stay unstamped (the durable column is never touched, no harness
/// RPC is attempted — no disconnect-wait paid), and the first write
/// is a hard 503 "harness disabled": the cutover left no legacy path.
#[tokio::test]
async fn kill_switch_off_writes_are_unavailable() {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(enabled_unreachable_harness(), false).await;
    let api_key = register_and_login(&app).await;
    let profile_id = create_profile(&app, &api_key).await;

    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "switch off" }))
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
        "kill switch off: session must stay unstamped"
    );

    // The first write is refused outright (no migration, no legacy
    // fallback).
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
        "kill switch off: writes must be 503, got: {}",
        resp.text()
    );
    assert!(
        resp.text().contains("FORGE_HARNESS_MESSAGES=0"),
        "{}",
        resp.text()
    );
}

/// Switch ON + harness fails at creation (unreachable socket): the
/// session still lands (201) and stays UNSTAMPED — creation never
/// fails because of the harness. The first write then attempts the
/// lazy migration, which fails 503 on the unreachable harness: the
/// user row is persisted, the claim is released, and the session
/// stays unstamped for the next write to retry.
#[tokio::test]
async fn harness_failure_at_creation_keeps_session_unstamped() {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(enabled_unreachable_harness(), true).await;
    let api_key = register_and_login(&app).await;
    let profile_id = create_profile(&app, &api_key).await;

    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "unmigrated" }))
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

    // First write: the lazy migration's createConversation fails on
    // the unreachable harness → 503, user row persisted, no stamp,
    // claim released.
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
        "migration on an unreachable harness must be 503, got: {}",
        resp.text()
    );
    assert!(
        resp.text().contains("harness unavailable"),
        "{}",
        resp.text()
    );

    let p = pool(&db_url).await;
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages WHERE session_id = $1 AND role = 'user' AND content = 'hello'",
    )
    .bind(uuid::Uuid::parse_str(&session_id).unwrap())
    .fetch_one(&p)
    .await
    .unwrap();
    let migrating: bool =
        sqlx::query_scalar("SELECT harness_migrating FROM sessions WHERE id = $1")
            .bind(uuid::Uuid::parse_str(&session_id).unwrap())
            .fetch_one(&p)
            .await
            .unwrap();
    p.close().await;
    assert_eq!(rows, 1, "user row must be persisted despite the 503");
    assert!(!migrating, "the failed migration must release its claim");
    assert_eq!(durable_conversation_id(&db_url, &session_id).await, None);
}

/// Stamped session: `POST /messages` routes to `harness.submit` —
/// with a disabled client that is 503 "harness submit failed". The
/// user row IS persisted, and the response error says a resubmit
/// mints a fresh request id.
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

    // Stamp the session as the cutover attach would (the harness RPC
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

/// Forks stay UNSTAMPED at creation (the harness IPC has no fork
/// semantics yet): the copied `messages` rows do not exist in a
/// durable conversation. The fork's first write lazy-migrates it
/// like any other session — with an unreachable harness that's a
/// 503, and the fork stays unstamped for the next write to retry.
#[tokio::test]
async fn forked_conversation_migrates_on_first_touch() {
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

    // Source conversation (harness unreachable → unstamped).
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

    // The fork: 201, stamp stays NULL even with the switch on.
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
        "fork must stay unstamped at creation: {body}"
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

    // The fork's first write attempts the lazy migration and fails
    // 503 on the unreachable harness (no stamp, claim released).
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": fork_id, "content": "after fork" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "fork first-touch migration must be 503 on an unreachable harness, got: {}",
        resp.text()
    );
    assert_eq!(durable_conversation_id(&db_url, &fork_id).await, None);
}
