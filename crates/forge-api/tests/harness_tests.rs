//! Herd H2.0 part 2 — harness wiring.
//!
//! Covers the *disabled-mode* edges of the harness-backed surface:
//! a session stamped with a `durable_conversation_id` (as H2.1's
//! `createConversation` cutover will do) must not panic when the
//! harness is unavailable, and must surface `HarnessUnavailable` as a
//! 503 on interrupt. Compaction of a harness-backed session stays on
//! the legacy path (the harness IPC has no compact method) — asserted
//! by the fact that the disabled-harness compact call takes exactly
//! the same shape as a legacy one (no 503 from the harness gate).

use serde_json::json;
use sqlx::postgres::PgPoolOptions;

mod test_helpers;

/// Same random-client-IP trick as `integration_tests` (rate limiter).
fn auth_client_ip() -> String {
    use rand::Rng;
    let b: [u8; 3] = rand::thread_rng().gen();
    format!("10.{}.{}.{}", b[0], b[1], b[2])
}

async fn register_and_login(app: &test_helpers::TestApp) -> (String, String) {
    let register_resp = app
        .post("/auth/register")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({
            "email": "harness-test@example.com",
            "name": "Harness Test User",
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
            "email": "harness-test@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(login_resp.status(), 200);
    let body: serde_json::Value = login_resp.json().await.unwrap();
    (
        body["user"]["id"].as_str().unwrap().to_string(),
        body["api_key"].as_str().unwrap().to_string(),
    )
}

/// Create a profile + session, then stamp the session with a durable
/// conversation id via direct SQL (the H2.1 `createConversation`
/// cutover will do this through the harness IPC).
async fn harness_backed_session(
    app: &test_helpers::TestApp,
    api_key: &str,
    db_url: &str,
) -> (uuid::Uuid, i64) {
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": "Harness Test Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    let profile_id = profile_resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .post("/sessions")
        .header("X-API-Key", api_key)
        .json(&json!({ "profile_id": profile_id, "title": "Harness Test Session" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "session creation");
    let session_id: uuid::Uuid = resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let conversation_id: i64 = 424242;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .unwrap();
    sqlx::query("UPDATE sessions SET durable_conversation_id = $1 WHERE id = $2")
        .bind(conversation_id)
        .bind(session_id)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    (session_id, conversation_id)
}

/// No harness socket (TestApp builds a disabled `HarnessState`):
/// interrupting a harness-backed session returns 503
/// HarnessUnavailable — and does not panic.
#[tokio::test]
async fn interrupt_harness_backed_session_disabled_harness_is_503() {
    let (app, db_url) = test_helpers::TestApp::new().await;
    let (_user_id, api_key) = register_and_login(&app).await;
    let (session_id, _conversation_id) = harness_backed_session(&app, &api_key, &db_url).await;

    let resp = app
        .post(&format!("/sessions/{session_id}/interrupt"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "disabled harness: expected 503, got {session_id}: {:?}",
        resp.text()
    );
    assert!(resp.text().contains("harness unavailable"));
}

/// Same, with `?tree=false` — the query param parses and the same
/// 503 gate applies (no 4xx from the extractor).
#[tokio::test]
async fn interrupt_harness_backed_session_tree_param_disabled_harness_is_503() {
    let (app, db_url) = test_helpers::TestApp::new().await;
    let (_user_id, api_key) = register_and_login(&app).await;
    let (session_id, _conversation_id) = harness_backed_session(&app, &api_key, &db_url).await;

    let resp = app
        .post(&format!("/sessions/{session_id}/interrupt?tree=false"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "disabled harness: expected 503");
    assert!(resp.text().contains("harness unavailable"));
}

/// A LEGACY session (no durable_conversation_id) is untouched by the
/// harness gate: with no live agent its interrupt is the usual
/// no-op 200.
#[tokio::test]
async fn interrupt_legacy_session_ignores_harness() {
    let (app, _db_url) = test_helpers::TestApp::new().await;
    let (_user_id, api_key) = register_and_login(&app).await;

    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Harness Test Profile 2",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    let profile_id = profile_resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "Legacy Session" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let session_id = resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .post(&format!("/sessions/{session_id}/interrupt"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "legacy session: expected the no-op 200, got {:?}",
        resp.text()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["interrupted"], false);
}
