//! F-00x — duplicate wake-prompt coalescing (backlog guard).
//!
//! Mule hourly wakes POST `[mule wake <uuid>] …` prompts into a
//! sticky agent conversation. When turns are slow, every fire used to
//! enqueue an identical submission (production: a 14-row
//! `durable_submissions` backlog the agent then commented on). These
//! tests drive `POST /messages` against a STAMPED session (durable
//! conversation id set) whose `durable_submissions` rows are seeded
//! directly, with the enabled-but-unreachable harness client:
//!
//! * duplicate wake (same wake id, pending `queued`/`placed`
//!   submission) → 200 `{"coalesced": true}`, NO user row, no submit;
//! * settled rows (`done`/`unanswered`) with the same wake id do NOT
//!   suppress a later fire (it reaches `harness.submit` → 503 with
//!   the unreachable client, user row persisted);
//! * a different wake id dispatches normally;
//! * pending submissions cap at `MAX_PENDING_SUBMISSIONS` (8): a
//!   fresh wake or an ordinary message is 429 while full, and a
//!   duplicate wake still coalesces (duplicate wins over cap).
//!
//! The pure decision function + wake-marker parsing are unit-tested
//! in `src/wake.rs` (`decide_wake_gate`, `parse_wake_id`).

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

mod test_helpers;

use forge_harness_client::Limits;

/// The scratch schema the seeded `durable_submissions` table lives
/// in (and the schema the injected harness client claims the durable
/// tables live in — same value, so the guard's query finds them).
const WAKE_TEST_SCHEMA: &str = "forge_wake_f00x";

/// Enabled harness client pointing at sockets that never exist:
/// dispatches that PASS the coalescing gate fail at `harness.submit`
/// with 503 "harness submit failed" — which is exactly how these
/// tests observe "the fire was not dropped by the guard".
fn enabled_unreachable_harness() -> forge_api::harness::HarnessState {
    let tmp = tempfile::tempdir().expect("tempdir");
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
        WAKE_TEST_SCHEMA,
    )
}

fn auth_client_ip() -> String {
    use rand::Rng;
    let b: [u8; 3] = rand::thread_rng().gen();
    format!("10.{}.{}.{}", b[0], b[1], b[2])
}

async fn register_and_login(app: &test_helpers::TestApp, email: &str) -> String {
    let register_resp = app
        .post("/auth/register")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({
            "email": email,
            "name": "Wake Coalesce User",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(register_resp.status(), 201, "{}", register_resp.text());
    let login_resp = app
        .post("/auth/login")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({ "email": email, "password": "password123" }))
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
            "name": "F00x Profile",
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn create_session(app: &test_helpers::TestApp, api_key: &str, profile_id: &str) -> String {
    let resp = app
        .post("/sessions")
        .header("X-API-Key", api_key)
        .json(&json!({ "profile_id": profile_id, "title": "f00x sticky" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn pool(db_url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .unwrap()
}

/// Create the scratch schema + the (minimal) `durable_submissions`
/// table the guard queries, mirroring durable-pg's 001 columns.
async fn seed_durable_schema(db_url: &str) {
    let p = pool(db_url).await;
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {WAKE_TEST_SCHEMA}"))
        .execute(&p)
        .await
        .unwrap();
    sqlx::query(&format!(
        r#"CREATE TABLE IF NOT EXISTS {WAKE_TEST_SCHEMA}.durable_submissions (
               id BIGINT PRIMARY KEY,
               conversation_id BIGINT NOT NULL,
               request_id TEXT,
               status TEXT NOT NULL CHECK (status IN ('queued','placed','done','unanswered')),
               record TEXT NOT NULL
           )"#
    ))
    .execute(&p)
    .await
    .unwrap();
    p.close().await;
}

/// Stamp the session onto a durable conversation (what the harness
/// migration does in production).
async fn stamp_session(db_url: &str, session_id: &str, conversation_id: i64) {
    let p = pool(db_url).await;
    sqlx::query("UPDATE sessions SET durable_conversation_id = $2 WHERE id = $1")
        .bind(Uuid::parse_str(session_id).unwrap())
        .bind(conversation_id)
        .execute(&p)
        .await
        .unwrap();
    p.close().await;
}

async fn insert_submission(
    db_url: &str,
    conversation_id: i64,
    request_id: Option<&str>,
    status: &str,
) {
    let p = pool(db_url).await;
    let id: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM {WAKE_TEST_SCHEMA}.durable_submissions"
    ))
    .fetch_one(&p)
    .await
    .unwrap();
    sqlx::query(&format!(
        r#"INSERT INTO {WAKE_TEST_SCHEMA}.durable_submissions
               (id, conversation_id, request_id, status, record)
           VALUES ($1, $2, $3, $4, '{{}}')"#
    ))
    .bind(id)
    .bind(conversation_id)
    .bind(request_id)
    .bind(status)
    .execute(&p)
    .await
    .unwrap();
    p.close().await;
}

async fn delete_submissions(db_url: &str, conversation_id: i64) {
    let p = pool(db_url).await;
    sqlx::query(&format!(
        "DELETE FROM {WAKE_TEST_SCHEMA}.durable_submissions WHERE conversation_id = $1"
    ))
    .bind(conversation_id)
    .execute(&p)
    .await
    .unwrap();
    p.close().await;
}

async fn user_row_count(db_url: &str, session_id: &str, content: &str) -> i64 {
    let p = pool(db_url).await;
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages WHERE session_id = $1 AND role = 'user' AND content = $2",
    )
    .bind(Uuid::parse_str(session_id).unwrap())
    .bind(content)
    .fetch_one(&p)
    .await
    .unwrap();
    p.close().await;
    n
}

/// The `[mule wake <uuid>] …` shape seen in production (F-00x).
fn wake_content(wake_id: Uuid) -> String {
    format!("[mule wake {wake_id}] Hourly herd audit. Review the agent roster and report drift.")
}

/// Boilerplate: app + authed user + stamped session on `conversation_id`.
async fn setup(
    email: &str,
    conversation_id: i64,
) -> (test_helpers::TestApp, String, String, String) {
    let (app, db_url) =
        test_helpers::TestApp::with_harness(enabled_unreachable_harness(), true).await;
    seed_durable_schema(&db_url).await;
    let api_key = register_and_login(&app, email).await;
    let profile_id = create_profile(&app, &api_key).await;
    let session_id = create_session(&app, &api_key, &profile_id).await;
    stamp_session(&db_url, &session_id, conversation_id).await;
    (app, db_url, api_key, session_id)
}

/// A wake fire whose wake id is already queued (`queued` AND
/// `placed` — "queued but not yet answered") is DROPPED: 200 with
/// `{"coalesced": true, "wake_id": …}`, and NO user row lands.
#[tokio::test]
async fn duplicate_wake_fire_is_coalesced() {
    let wake = Uuid::new_v4();
    let (app, db_url, api_key, session_id) = setup("wake-dup@example.com", 77001).await;

    // Pending duplicate: the request-id shape `dispatch_message`
    // mints for wake fires (`<wake-id>:<nonce>`).
    insert_submission(
        &db_url,
        77001,
        Some(&format!("{wake}:{}", Uuid::new_v4().simple())),
        "queued",
    )
    .await;

    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(wake) }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "coalesced fire must be a success: {}",
        resp.text()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["coalesced"], json!(true), "{body}");
    assert_eq!(body["wake_id"], json!(wake.to_string()), "{body}");
    assert_eq!(
        user_row_count(&db_url, &session_id, &wake_content(wake)).await,
        0,
        "a coalesced fire must not record a user row"
    );

    // `placed` (transcript-added, run-owned) is still "not yet
    // answered": same drop.
    insert_submission(
        &db_url,
        77001,
        Some(&format!("{wake}:{}", Uuid::new_v4().simple())),
        "placed",
    )
    .await;
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(wake) }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["coalesced"],
        json!(true)
    );
}

/// Settled submissions (`done` / `unanswered` — the 14-row
/// production state) must NOT suppress a later fire of the same
/// wake: the new fire passes the gate and reaches `harness.submit`
/// (503 with the unreachable client, user row persisted). A
/// different wake id likewise dispatches while another is pending.
#[tokio::test]
async fn settled_wake_rows_do_not_suppress_and_distinct_wakes_dispatch() {
    let wake = Uuid::new_v4();
    let (app, db_url, api_key, session_id) = setup("wake-settled@example.com", 77002).await;

    insert_submission(&db_url, 77002, Some(&format!("{wake}:n1")), "done").await;
    insert_submission(&db_url, 77002, Some(&format!("{wake}:n2")), "unanswered").await;

    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(wake) }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "settled rows must not coalesce the fire: {}",
        resp.text()
    );
    let body = resp.text();
    assert!(body.contains("harness submit failed"), "{body}");
    assert_eq!(
        user_row_count(&db_url, &session_id, &wake_content(wake)).await,
        1
    );

    // Distinct wake id while the first is genuinely pending →
    // dispatches (not coalesced).
    insert_submission(&db_url, 77002, Some(&format!("{wake}:pending")), "queued").await;
    let other = Uuid::new_v4();
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(other) }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "a distinct wake must not be coalesced: {}",
        resp.text()
    );
    assert_eq!(
        user_row_count(&db_url, &session_id, &wake_content(other)).await,
        1
    );
}

/// The pending queue caps at `MAX_PENDING_SUBMISSIONS` (8): with a
/// full queue a FRESH wake and an ordinary message are rejected
/// (429, clear error, no user row) so the backlog cannot grow — but
/// a DUPLICATE wake still coalesces (dropping an identical prompt
/// adds nothing to the queue, so the duplicate rule wins over the
/// cap), and room below the cap dispatches normally.
#[tokio::test]
async fn pending_cap_rejects_when_full_duplicate_still_coalesces() {
    let wake = Uuid::new_v4();
    let (app, db_url, api_key, session_id) = setup("wake-cap@example.com", 77003).await;

    let cap = forge_api::wake::MAX_PENDING_SUBMISSIONS;
    for i in 0..cap {
        insert_submission(&db_url, 77003, Some(&format!("unrelated-{i}")), "queued").await;
    }
    // One row without a request_id still counts toward the cap.
    // (cap rows already inserted; the cap check is len() based.)

    // Fresh wake while full → 429, no user row.
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(wake) }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        429,
        "full queue must reject a fresh wake: {}",
        resp.text()
    );
    let body = resp.text();
    assert!(body.contains("pending harness submissions"), "{body}");
    assert!(body.contains(&format!("cap {cap}")), "{body}");
    assert_eq!(
        user_row_count(&db_url, &session_id, &wake_content(wake)).await,
        0
    );

    // Ordinary message while full → 429 too (queue must not grow).
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": "just a ping" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        429,
        "full queue must reject ordinary traffic: {}",
        resp.text()
    );

    // Duplicate wake while full → coalesced (200), NOT rejected.
    insert_submission(&db_url, 77003, Some(&format!("{wake}:dup")), "queued").await;
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(wake) }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "duplicate wins over cap: {}",
        resp.text()
    );
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["coalesced"],
        json!(true)
    );

    // Drain the queue → the same fresh wake dispatches (reaches
    // submit → 503, user row persisted).
    delete_submissions(&db_url, 77003).await;
    let other = Uuid::new_v4();
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_id, "content": wake_content(other) }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "empty queue must dispatch: {}",
        resp.text()
    );
    assert_eq!(
        user_row_count(&db_url, &session_id, &wake_content(other)).await,
        1
    );
}

/// An UNSTAMPED session (no durable conversation) can have no
/// backlog: the guard is skipped entirely — even wake-shaped content
/// flows to the normal lazy-migration path (503 "harness unavailable"
/// on the unreachable client), never a coalesce marker.
#[tokio::test]
async fn unstamped_session_skips_the_guard() {
    let (app, _db_url, api_key, profile_id) = {
        let (app, db_url) =
            test_helpers::TestApp::with_harness(enabled_unreachable_harness(), true).await;
        seed_durable_schema(&db_url).await;
        let api_key = register_and_login(&app, "wake-unstamped@example.com").await;
        let profile_id = create_profile(&app, &api_key).await;
        (app, db_url, api_key, profile_id)
    };
    let wake = Uuid::new_v4();
    let resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": create_session(&app, &api_key, &profile_id).await,
            "content": wake_content(wake)
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "{}", resp.text());
    let body = resp.text();
    assert!(body.contains("harness unavailable"), "{body}");
    assert!(!body.contains("coalesced"), "{body}");
}
