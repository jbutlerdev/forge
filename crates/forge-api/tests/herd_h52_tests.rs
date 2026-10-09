//! Herd H5.2 — wake-condition wiring (API-level acceptance).
//!
//! Covers the forge-side wake rows:
//!
//! 1. **turn-end → mule event wake**: the forwarder fires
//!    `POST /api/v1/wakes/fire` with the exact body on
//!    `BusEvent::TurnEnded` for agent sessions (and only for them,
//!    and only inside the optional filter); a 500 from mule is
//!    warn-only and the forwarder keeps running.
//! 2. **agent-signal push wake** (optional): after a successful
//!    `insert_signal`, the configured fake mule captures the
//!    `agent.signal` event fire (skip-when-no-pgvector contract —
//!    the signal table lives in the pgvector-gated migration 022).
//! 3. **`schedule_reminder` endpoint**: tenancy/validation gates
//!    (404s, 400s, 503 on a disabled harness) AND the live-harness
//!    acceptance: a reminder lands as a durable `harness_timers`
//!    row with the `[reminder]` prompt and the right fire time.
//! 4. **file-watch worker**: an inotify fire publishes
//!    `BusEvent::FileChanged` (agent name→id resolution at boot;
//!    clean stop).
//!
//! The mule is an in-process axum fake capturing
//! `POST /api/v1/wakes/fire` (auth header + body).

mod test_helpers;

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::response::IntoResponse;
use serde_json::json;
use test_helpers::TestApp;
use uuid::Uuid;

// ============================================
// Fakes + helpers
// ============================================

/// In-process fake mule: captures `POST /api/v1/wakes/fire`
/// (auth + body) and answers with a per-request-overridable status.
struct FakeMule {
    url: String,
    hits: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    status: Arc<AtomicU16>,
    _server: tokio::task::JoinHandle<()>,
}

impl FakeMule {
    /// Wait (≤ `timeout`) for at least `want` captured hits.
    async fn wait_hits(&self, want: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.hits.lock().unwrap().len() >= want {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn start_fake_mule() -> FakeMule {
    let hits: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let status = Arc::new(AtomicU16::new(200));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake mule");
    let addr = listener.local_addr().expect("fake mule addr");
    let hits_c = hits.clone();
    let status_c = status.clone();
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/api/v1/wakes/fire",
            axum::routing::post(
                move |auth: axum::http::HeaderMap, body: axum::extract::Json<serde_json::Value>| {
                    let hits = hits_c.clone();
                    let status = status_c.clone();
                    async move {
                        let auth = auth
                            .get("authorization")
                            .map(|v| v.to_str().unwrap_or("").to_string())
                            .unwrap_or_default();
                        let st = status.load(Ordering::Relaxed);
                        hits.lock().unwrap().push((auth, body.0));
                        (
                            axum::http::StatusCode::from_u16(st)
                                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
                            axum::Json(json!({ "ok": st == 200 })),
                        )
                            .into_response()
                    }
                },
            ),
        );
        axum::serve(listener, app).await.ok();
    });
    FakeMule {
        url: format!("http://{addr}"),
        hits,
        status,
        _server: server,
    }
}

fn auth_client_ip() -> String {
    use rand::Rng;
    let b: [u8; 3] = rand::thread_rng().gen();
    format!("10.{}.{}.{}", b[0], b[1], b[2])
}

async fn register_and_login(app: &TestApp, email: &str, name: &str) -> String {
    let register_resp = app
        .post("/auth/register")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({ "email": email, "name": name, "password": "password123" }))
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
    assert_eq!(login_resp.status(), 200, "{}", login_resp.text());
    login_resp.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn create_profile(app: &TestApp, key: &str, name: &str) -> String {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", key)
        .json(&json!({
            "name": name,
            "provider": "faux",
            "model": "faux-1",
            "working_dir": "/tmp/session-test",
            "system_prompt": "You are a concise assistant."
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

async fn create_agent(app: &TestApp, key: &str, name: &str, profile_id: &str) -> String {
    let resp = app
        .post("/agents")
        .header("X-API-Key", key)
        .json(&json!({ "name": name, "primary_profile_id": profile_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// One agent conversation (session with `agent_id` stamped).
async fn create_agent_conversation(app: &TestApp, key: &str, agent_id: &str) -> String {
    let resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

// ============================================
// 1. turn-end → mule event wake
// ============================================

#[tokio::test]
async fn turnend_forwarder_fires_event_wake() {
    let mule = start_fake_mule().await;
    // One app (one DB): filter = agent B only, so A's turns must NOT
    // fire while B's must.
    let (app, _db_url) = TestApp::with_wake_config(forge_api::wake::WakeConfig {
        turnend_mule_base: Some(mule.url.clone()),
        turnend_mule_key: Some("sk_mule_test".into()),
        ..Default::default()
    })
    .await;
    let key = register_and_login(&app, "h52-te@example.com", "H52 TurnEnd").await;
    let profile = create_profile(&app, &key, "H52 TE Profile").await;
    let agent_a = create_agent(&app, &key, "h52 te agent a", &profile).await;
    let session_a = create_agent_conversation(&app, &key, &agent_a).await;

    let forwarder = forge_api::wake::spawn_turnend_forwarder(app.app_state.clone());
    assert!(forwarder.is_some(), "forwarder must start when configured");
    // Yield so the forwarder task subscribes to the bus BEFORE the
    // first publish (tokio::spawn does not run the task inline; a
    // publish with no live receiver is lost by broadcast semantics).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Raw session (no agent_id) → no fire by design.
    let raw_resp = app
        .post("/sessions")
        .header("X-API-Key", &key)
        .json(&json!({ "profile_id": profile }))
        .send()
        .await
        .unwrap();
    assert_eq!(raw_resp.status(), 201, "{}", raw_resp.text());
    let raw_sid = raw_resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    app.app_state
        .bus
        .publish_turn_ended(raw_sid.parse().unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        mule.hits.lock().unwrap().is_empty(),
        "raw session (no agent_id) must NOT fire a mule wake"
    );

    // Agent turn ends → exactly one fire, exact body.
    let sid_a = session_a.parse::<Uuid>().unwrap();
    app.app_state.bus.publish_turn_ended(sid_a);
    assert!(
        mule.wait_hits(1, Duration::from_secs(10)).await,
        "turn-end must fire the mule event wake"
    );
    let hits = mule.hits.lock().unwrap().clone();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "Bearer sk_mule_test");
    let body = &hits[0].1;
    assert_eq!(body["kind"], "event");
    assert_eq!(body["source"], "agent.turn_ended");
    assert_eq!(body["payload"]["agent_id"], agent_a);
    assert_eq!(body["payload"]["session_id"], session_a);
    assert!(
        body["payload"]["ts"].is_string(),
        "payload.ts must be set: {body}"
    );
}

#[tokio::test]
async fn turnend_forwarder_filtered_agent_does_not_fire() {
    let mule = start_fake_mule().await;
    let excluded = Uuid::new_v4(); // a filter that matches NO agent
    let (app, _db_url) = TestApp::with_wake_config(forge_api::wake::WakeConfig {
        turnend_mule_base: Some(mule.url.clone()),
        turnend_mule_key: Some("sk_mule_test".into()),
        turnend_mule_agents: vec![excluded],
        ..Default::default()
    })
    .await;
    let key = register_and_login(&app, "h52-tef@example.com", "H52 TE Filtered").await;
    let profile = create_profile(&app, &key, "H52 TEF Profile").await;
    let agent = create_agent(&app, &key, "h52 tef agent", &profile).await;
    let session = create_agent_conversation(&app, &key, &agent).await;

    let forwarder = forge_api::wake::spawn_turnend_forwarder(app.app_state.clone());
    assert!(forwarder.is_some());
    tokio::time::sleep(Duration::from_millis(100)).await; // forwarder subscribes

    app.app_state
        .bus
        .publish_turn_ended(session.parse().unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        mule.hits.lock().unwrap().is_empty(),
        "filtered-out agent must NOT fire a mule wake"
    );
}

#[tokio::test]
async fn turnend_forwarder_mule_500_is_warn_only() {
    let mule = start_fake_mule().await;
    mule.status.store(500, Ordering::Relaxed);
    let wake = forge_api::wake::WakeConfig {
        turnend_mule_base: Some(mule.url.clone()),
        turnend_mule_key: Some("sk_mule_test".into()),
        ..Default::default()
    };
    let (app, _db_url) = TestApp::with_wake_config(wake).await;
    let key = register_and_login(&app, "h52-te5@example.com", "H52 TE 500").await;
    let profile = create_profile(&app, &key, "H52 TE5 Profile").await;
    let agent = create_agent(&app, &key, "h52 te5 agent", &profile).await;
    let session = create_agent_conversation(&app, &key, &agent).await;
    let sid = session.parse::<Uuid>().unwrap();

    let forwarder = forge_api::wake::spawn_turnend_forwarder(app.app_state.clone());
    assert!(forwarder.is_some());
    tokio::time::sleep(Duration::from_millis(100)).await; // forwarder subscribes

    // 500: the attempt is made (captured) and the forwarder survives.
    app.app_state.bus.publish_turn_ended(sid);
    assert!(
        mule.wait_hits(1, Duration::from_secs(10)).await,
        "the fire attempt must reach mule even when it 500s"
    );
    mule.status.store(200, Ordering::Relaxed);
    app.app_state.bus.publish_turn_ended(sid);
    assert!(
        mule.wait_hits(2, Duration::from_secs(10)).await,
        "forwarder must keep firing after a mule 500 (warn only)"
    );
}

// ============================================
// 2. agent-signal push wake (optional leg of H4.6)
// ============================================

#[tokio::test]
async fn signal_push_wake_fires_event_when_configured() {
    // Skip-when-no-pgvector contract: the signal table lives in the
    // pgvector-gated migration 022.
    let (probe, _probe_db) = TestApp::new().await;
    if !forge_api::memory::vector_available(&probe.app_state.db).await {
        eprintln!("SKIP H5.2 signal-push test: pgvector not installed on the scratch Postgres");
        return;
    }
    let mule = start_fake_mule().await;
    let (app, _db_url) = TestApp::with_wake_config(forge_api::wake::WakeConfig {
        signal_wake_mule_base: Some(mule.url.clone()),
        signal_wake_mule_key: Some("sk_mule_sig".into()),
        ..Default::default()
    })
    .await;
    let key = register_and_login(&app, "h52-sig@example.com", "H52 Signal").await;
    let profile = create_profile(&app, &key, "H52 SIG Profile").await;
    let from_agent = create_agent(&app, &key, "h52 sig from", &profile).await;
    let to_agent = create_agent(&app, &key, "h52 sig to", &profile).await;

    let resp = app
        .post(&format!("/agents/{from_agent}/memory/signals"))
        .header("X-API-Key", &key)
        .json(&json!({
            "kind": "insight",
            "to": to_agent,
            "payload": { "note": "PO template changed" },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    let signal_id = resp.json::<serde_json::Value>().await.unwrap()["signal_id"]
        .as_str()
        .unwrap()
        .to_string();

    assert!(
        mule.wait_hits(1, Duration::from_secs(10)).await,
        "configured signal push must fire the mule event wake"
    );
    let hits = mule.hits.lock().unwrap().clone();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "Bearer sk_mule_sig");
    let body = &hits[0].1;
    assert_eq!(body["kind"], "event");
    assert_eq!(body["source"], "agent.signal");
    assert_eq!(body["payload"]["kind"], "insight");
    assert_eq!(body["payload"]["from_agent"], from_agent);
    assert_eq!(body["payload"]["to_agent"], to_agent);
    assert_eq!(body["payload"]["payload_ref"], signal_id);
}

#[tokio::test]
async fn signal_push_wake_off_by_default() {
    // No push config: a signal records fine and NOTHING leaves the
    // process (the fake mule must see zero requests). This test needs
    // the signal table, so it follows the same skip contract.
    let (probe, _probe_db) = TestApp::new().await;
    if !forge_api::memory::vector_available(&probe.app_state.db).await {
        eprintln!("SKIP H5.2 signal-push-off test: pgvector not installed on the scratch Postgres");
        return;
    }
    let mule = start_fake_mule().await;
    let _ = &mule; // captured server, asserted empty at the end
    let (app, _db_url) = TestApp::new().await;
    let key = register_and_login(&app, "h52-sigoff@example.com", "H52 Signal Off").await;
    let profile = create_profile(&app, &key, "H52 SIGOFF Profile").await;
    let from_agent = create_agent(&app, &key, "h52 sigoff from", &profile).await;
    let to_agent = create_agent(&app, &key, "h52 sigoff to", &profile).await;

    let resp = app
        .post(&format!("/agents/{from_agent}/memory/signals"))
        .header("X-API-Key", &key)
        .json(&json!({
            "kind": "request",
            "to": to_agent,
            "payload": { "note": "help needed" },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        mule.hits.lock().unwrap().is_empty(),
        "pull-only deployment: no mule request may leave the process"
    );
}

// ============================================
// 3. schedule_reminder endpoint (gates + live timer row)
// ============================================

/// Reaps the harness child process on EVERY exit path — including
/// panics mid-test (the ChildReaper pattern from research_tests.rs).
struct ChildReaper(std::process::Child);
impl Drop for ChildReaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn reminder_validation_and_tenancy() {
    // Harness DISABLED (default TestApp): the gates run before any
    // harness contact; a valid request 503s on the harness check.
    let (app, _db_url) = TestApp::new().await;
    let key_a = register_and_login(&app, "h52-rma@example.com", "H52 RM A").await;
    let key_b = register_and_login(&app, "h52-rmb@example.com", "H52 RM B").await;
    let profile = create_profile(&app, &key_a, "H52 RM Profile").await;
    let agent = create_agent(&app, &key_a, "h52 rm agent", &profile).await;
    let session = create_agent_conversation(&app, &key_a, &agent).await;

    // Unknown session → 404 (no existence leak).
    let resp = app
        .post(&format!("/sessions/{}/reminders", Uuid::new_v4()))
        .header("X-API-Key", &key_a)
        .json(&json!({ "message": "hi", "in_minutes": 5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "{}", resp.text());

    // Foreign user's session → 404 (owner-gated).
    let resp = app
        .post(&format!("/sessions/{session}/reminders"))
        .header("X-API-Key", &key_b)
        .json(&json!({ "message": "hi", "in_minutes": 5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "{}", resp.text());

    // Input gates (all on the owner's session):
    //   empty message → 400; both in_minutes+cron → 400; neither → 400;
    //   in_minutes=0 → 400.
    for (label, body) in [
        (
            "empty message",
            json!({ "message": "   ", "in_minutes": 5 }),
        ),
        (
            "both set",
            json!({ "message": "hi", "in_minutes": 5, "cron": "0 6 * * *" }),
        ),
        ("neither set", json!({ "message": "hi" })),
        ("zero minutes", json!({ "message": "hi", "in_minutes": 0 })),
    ] {
        let resp = app
            .post(&format!("/sessions/{session}/reminders"))
            .header("X-API-Key", &key_a)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "{label}: {}", resp.text());
    }

    // Valid input, disabled harness → 503 (no timer anywhere).
    let resp = app
        .post(&format!("/sessions/{session}/reminders"))
        .header("X-API-Key", &key_a)
        .json(&json!({ "message": "check the deploy", "in_minutes": 5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "disabled harness must 503: {}",
        resp.text()
    );
}

/// Live-harness acceptance: the reminder lands as a DURABLE
/// `harness_timers` row with the `[reminder]` prompt and the right
/// fire time (the fired timer re-prompts the same conversation with
/// `timer fired: [reminder] …` — the H2.3 shape).
#[tokio::test]
async fn reminder_creates_durable_timer_row() {
    let schema = format!("forge_h52_{}", Uuid::new_v4().simple());
    let socket_dir = tempfile::tempdir().expect("socket tempdir");
    let rpc_sock = socket_dir.path().join("harness.sock");
    let events_sock = socket_dir.path().join("harness-events.sock");

    let harness = forge_api::harness::HarnessState::from_paths_with(
        &rpc_sock,
        &events_sock,
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    let (app, db_url) = TestApp::with_harness(harness, true).await;
    let api_key = register_and_login(&app, "h52-rm-integ@example.com", "H52 RM Integ").await;
    let profile = create_profile(&app, &api_key, "H52 RM Integ Profile").await;
    let agent = create_agent(&app, &api_key, "h52 rm integ agent", &profile).await;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .unwrap();

    // Spawn the real harness child (faux provider; no turns needed —
    // the reminder is a pure timerSet RPC). It must be up BEFORE the
    // agent conversation is created: the H2.1 cutover stamps
    // `sessions.durable_conversation_id` at creation time via a
    // harness createConversation RPC, and creation never fails on a
    // harness hiccup (the session just stays unstamped).
    let repo_root = std::path::Path::new(
        std::env::var("CARGO_MANIFEST_DIR")
            .as_deref()
            .expect("CARGO_MANIFEST_DIR"),
    )
    .parent()
    .and_then(|p| p.parent())
    .expect("repo root")
    .to_path_buf();
    let child = std::process::Command::new(repo_root.join("node_modules/.bin/tsx"))
        .arg("harness/src/main.ts")
        .current_dir(&repo_root)
        .env("FORGE_DATABASE_URL", &db_url)
        .env("FORGE_API_URL", &app.base_url)
        .env("FORGE_API_KEY", &api_key)
        .env("FORGE_HARNESS_SOCKET", &rpc_sock)
        .env("FORGE_HARNESS_EVENTS_SOCKET", &events_sock)
        .env("FORGE_HARNESS_SCHEMA", &schema)
        .env("FORGE_HARNESS_FAUX", "1")
        .spawn()
        .expect("spawn the harness child");
    let child = ChildReaper(child); // reaped on panic too
    eprintln!(
        "h52-integ: harness child pid {:?} (schema {schema})",
        child.0.id()
    );

    let boot_deadline = Instant::now() + Duration::from_secs(120);
    while !rpc_sock.exists() {
        assert!(
            Instant::now() < boot_deadline,
            "harness child did not open its RPC socket in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Harness up: create the agent conversation so the H2.1 cutover
    // stamps `durable_conversation_id`.
    let session = create_agent_conversation(&app, &api_key, &agent).await;

    // The agent conversation got a durable conversation at creation
    // (H2.6 cutover on): the timer row must carry that id.
    let conversation_id: i64 =
        sqlx::query_scalar("SELECT durable_conversation_id FROM sessions WHERE id = $1")
            .bind(session.parse::<Uuid>().unwrap())
            .fetch_one(&pool)
            .await
            .expect("durable conversation stamp");

    // ---- one-shot reminder (in_minutes) ----
    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let resp = app
        .post(&format!("/sessions/{session}/reminders"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "message": "check the deploy", "in_minutes": 30 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["scheduled"], true, "{body}");
    let timer_id = body["timer_id"].as_str().unwrap().to_string();
    assert!(body["when"].is_string(), "when must be ISO: {body}");

    let row: (String, i64, String, chrono::DateTime<chrono::Utc>, Option<String>) =
        sqlx::query_as(
            &format!(
                "SELECT timer_id, conversation_id, prompt, at, cron FROM {schema}.harness_timers WHERE timer_id = $1"
            ),
        )
        .bind(&timer_id)
        .fetch_one(&pool)
        .await
        .expect("the durable timer row must exist");
    assert_eq!(row.0, timer_id);
    assert_eq!(
        row.1, conversation_id,
        "the timer must live on this session's durable conversation"
    );
    assert_eq!(
        row.2, "[reminder] check the deploy",
        "prompt must be the [reminder]-prefixed message (fire re-prompts `timer fired: [reminder] …`)"
    );
    let fire_at_ms = row.3.timestamp_millis();
    let expected_min = before_ms + 30 * 60_000 - 2 * 60_000; // 2-min clock slack
    let expected_max = before_ms + 30 * 60_000 + 60_000;
    assert!(
        fire_at_ms >= expected_min && fire_at_ms <= expected_max,
        "fire time {fire_at_ms}ms not ≈ now+30min (window [{expected_min}, {expected_max}])"
    );
    assert!(row.4.is_none(), "one-shot reminder must have cron NULL");

    // ---- recurring reminder (cron) ----
    let resp = app
        .post(&format!("/sessions/{session}/reminders"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "message": "morning report", "cron": "30 6 * * *" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["when"], "30 6 * * *", "{body}");
    let cron_timer_id = body["timer_id"].as_str().unwrap().to_string();
    let row: (String, String, Option<String>) = sqlx::query_as(&format!(
        "SELECT timer_id, prompt, cron FROM {schema}.harness_timers WHERE timer_id = $1"
    ))
    .bind(&cron_timer_id)
    .fetch_one(&pool)
    .await
    .expect("the cron reminder row must exist");
    assert_eq!(row.1, "[reminder] morning report");
    assert_eq!(row.2.as_deref(), Some("30 6 * * *"));

    pool.close().await;
}

// ============================================
// 4. file-watch worker (inotify fire → bus marker)
// ============================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filewatch_publishes_bus_event_on_change() {
    let (app, _db_url) = TestApp::new().await;
    let key = register_and_login(&app, "h52-fw@example.com", "H52 FW").await;
    let profile = create_profile(&app, &key, "H52 FW Profile").await;
    let _agent_id = create_agent(&app, &key, "h52 fw agent", &profile).await;

    // Watch directory + agent NAME (exercises the name->id lookup at
    // boot). The agent row lives in `app`'s DB; the worker runs on a
    // wake-patched clone of the SAME state (same pool, same agents).
    let tmp = tempfile::tempdir().expect("watch tempdir");
    let watch_dir = tmp.path().join("inbox");
    std::fs::create_dir_all(&watch_dir).unwrap();

    let wake_state = (*app.app_state)
        .clone()
        .with_wake_config(forge_api::wake::WakeConfig {
            filewatch_raw: Some(format!("{}:h52 fw agent", watch_dir.display())),
            ..Default::default()
        });
    let mut bus_rx = wake_state.bus.subscribe();
    let handle = forge_api::filewatch::spawn_filewatch(std::sync::Arc::new(wake_state))
        .await
        .expect("worker must start (path exists, name resolvable)");
    // Wait for the worker to arm its inotify watch — an event that
    // lands before inotify_add_watch is never delivered.
    let deadline_arm = Instant::now() + Duration::from_secs(5);
    while !handle.armed() {
        assert!(
            Instant::now() < deadline_arm,
            "worker must arm its watch within 5 s"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // Trigger: create + write inside the watched directory.
    let file = watch_dir.join("new.txt");
    std::fs::write(&file, "hello").unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut saw = false;
    while !saw && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, bus_rx.recv()).await {
            Ok(Ok(ev)) => {
                if let forge_api::bus::BusEvent::FileChanged { path, agent_id } = ev {
                    assert_eq!(path, watch_dir.to_string_lossy());
                    assert_eq!(agent_id, Uuid::parse_str(&_agent_id).unwrap());
                    saw = true;
                }
            }
            Ok(Err(e)) => panic!("bus receiver error: {e}"),
            Err(_) => break,
        }
    }
    assert!(
        saw,
        "FileChanged bus event expected within 10 s (debounce 2 s)"
    );

    // Clean stop: the 1 s poll heartbeat bounds the join.
    handle.stop();
}

#[tokio::test]
async fn filewatch_boot_validation_drops_unresolvable() {
    let (app, _db_url) = TestApp::new().await;

    // Unknown agent name -> no watch survives -> worker not started.
    let wake_state = (*app.app_state)
        .clone()
        .with_wake_config(forge_api::wake::WakeConfig {
            filewatch_raw: Some("/tmp:h52-no-such-agent".into()),
            ..Default::default()
        });
    let handle = forge_api::filewatch::spawn_filewatch(std::sync::Arc::new(wake_state)).await;
    assert!(
        handle.is_none(),
        "unresolvable watch must not start a worker"
    );

    // Malformed entries (no separator) -> nothing survives either.
    let wake_state = (*app.app_state)
        .clone()
        .with_wake_config(forge_api::wake::WakeConfig {
            filewatch_raw: Some("no-colon-entry".into()),
            ..Default::default()
        });
    let handle = forge_api::filewatch::spawn_filewatch(std::sync::Arc::new(wake_state)).await;
    assert!(handle.is_none(), "malformed spec must not start a worker");
}
