//! Herd H2.1 — the turn, re-stated: the live end-to-end acceptance.
//!
//! Spawns the **real Node harness** as a child process
//! (`tsx harness/src/main.ts`) with the scripted faux provider
//! (`FORGE_HARNESS_FAUX=1`, answers pre-queued via
//! `FORGE_HARNESS_FAUX_RESPONSES`), a scratch Postgres schema for the
//! `durable_*` tables, and sockets in a tempdir. forge-api runs with
//! `FORGE_HARNESS_MESSAGES` ON (flag injected via
//! `TestApp::with_harness`) and its event consumer wired to the child.
//!
//! Then the H1 demo is re-run against harness sessions —
//! PLAN-HERD's shadow-parity seed:
//!
//! 1. `POST /agents` + `POST /agents/:id/conversations` → 201 and the
//!    session is stamped (`durable_conversation_id` set).
//! 2. `POST /agents/:id/conversations/:cid/messages` → 202 + the user
//!    row (same shape as legacy).
//! 3. Bounded wait → an assistant row lands in `messages` (projected
//!    from the durable `pi.assistant` entry), a `message` bus event
//!    fired, `turn_ended` fired, and `GET /agents/:id/active` returns
//!    to idle.
//! 4. `POST /sessions` + `POST /messages` — the same assertions on the
//!    raw-session path.
//! 5. Direct SQL: the durable entries exist in the scratch schema and
//!    the projection bookkeeping has exactly one claim per entry.
//!
//! The faux answers are plain text (no tool calls), so no
//! `/tools/execute` round trip happens — the fake model cannot emit
//! tool calls without a queued one, and none are queued.

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

mod test_helpers;

use forge_api::bus::BusEvent;

const TURN_WAIT: Duration = Duration::from_secs(90);
const HARNESS_BOOT_WAIT: Duration = Duration::from_secs(120);

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
            "email": "h21-integ@example.com",
            "name": "H21 Integ User",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(register_resp.status(), 201, "{}", register_resp.text());
    let login_resp = app
        .post("/auth/login")
        .header("X-Forwarded-For", &auth_client_ip())
        .json(&json!({
            "email": "h21-integ@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(login_resp.status(), 200, "{}", login_resp.text());
    let body: serde_json::Value = login_resp.json().await.unwrap();
    body["api_key"].as_str().unwrap().to_string()
}

/// Wait until `assistant_text` shows up as an assistant row for the
/// session; returns the row's content (also the projection source).
async fn wait_for_assistant_row(pool: &sqlx::PgPool, session_id: &str) -> String {
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    loop {
        match sqlx::query_scalar::<_, String>(
            "SELECT content FROM messages WHERE session_id = $1 AND role = 'assistant' AND content <> '' ORDER BY sequence DESC LIMIT 1",
        )
        .bind(uuid::Uuid::parse_str(session_id).unwrap())
        .fetch_optional(pool)
        .await
        {
            Ok(Some(content)) => return content,
            Ok(None) => {}
            Err(e) => panic!("assistant row poll failed: {e}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the assistant row on session {session_id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn harness_turn_end_to_end() {
    // Scratch schema for the durable_* tables (same database as the
    // forge test DB; the harness pins search_path to it, the test
    // process never touches its own public schema for these).
    let schema = format!("forge_h21_{}", uuid::Uuid::new_v4().simple());
    let socket_dir = tempfile::tempdir().expect("socket tempdir");
    let rpc_sock = socket_dir.path().join("harness.sock");
    let events_sock = socket_dir.path().join("harness-events.sock");

    // forge-api under test: harness enabled (dials the child's
    // sockets), H2.1 flag ON.
    let harness = forge_api::harness::HarnessState::from_paths_with(
        &rpc_sock,
        &events_sock,
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    let (app, db_url) = test_helpers::TestApp::with_harness(harness, true).await;
    // The H2.1 event consumer (assistant projection + in-flight marks
    // + turn_ended) — lib.rs does this at boot; tests wire it here.
    let consumer_handle = forge_api::harness::spawn_event_consumer(app.app_state.clone());
    assert!(consumer_handle.is_some(), "event consumer must spawn");

    let api_key = register_and_login(&app).await;

    // Faux profile: provider/model resolved by attach_harness_conversation
    // (override ?? profile) and sent to the harness as the agent's model.
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "H21 Faux Profile",
            "provider": "faux",
            "model": "faux-1",
            "working_dir": "/tmp/session-test",
            "system_prompt": "You are a concise assistant."
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(profile_resp.status(), 201, "{}", profile_resp.text());
    let profile_id = profile_resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Scratch schema exists before the harness opens storage.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .unwrap();

    // Spawn the real harness child process.
    let repo_root = std::path::Path::new(
        std::env::var("CARGO_MANIFEST_DIR")
            .as_deref()
            .expect("CARGO_MANIFEST_DIR"),
    )
    .parent()
    .and_then(|p| p.parent())
    .expect("repo root")
    .to_path_buf();
    let mut child = std::process::Command::new(repo_root.join("node_modules/.bin/tsx"))
        .arg("harness/src/main.ts")
        .current_dir(&repo_root)
        .env("FORGE_DATABASE_URL", &db_url)
        .env("FORGE_API_URL", &app.base_url)
        .env("FORGE_API_KEY", &api_key)
        .env("FORGE_HARNESS_SOCKET", &rpc_sock)
        .env("FORGE_HARNESS_EVENTS_SOCKET", &events_sock)
        .env("FORGE_HARNESS_SCHEMA", &schema)
        .env("FORGE_HARNESS_FAUX", "1")
        .env(
            "FORGE_HARNESS_FAUX_RESPONSES",
            r#"["Paris.", "Iris reports in: the shadow-parity seed holds."]"#,
        )
        .spawn()
        .expect("spawn the harness child");
    eprintln!(
        "h21-integ: harness child pid {:?} (schema {schema})",
        child.id()
    );

    // Wait for the RPC socket to appear (tsx boot + PgStorage
    // migrations + Harness.open).
    let deadline = tokio::time::Instant::now() + HARNESS_BOOT_WAIT;
    while !rpc_sock.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "harness child did not open its RPC socket in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Give the events socket + the redial loops a beat.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ---- 1. The agent path (H1 demo, shadow-parity seed) ----
    let agent_resp = app
        .post("/agents")
        .header("X-API-Key", &api_key)
        .json(&json!({ "name": "H21 Integ Agent", "primary_profile_id": profile_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(agent_resp.status(), 201, "{}", agent_resp.text());
    let agent_id = agent_resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let conv_resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "title": "H21 shadow parity" }))
        .send()
        .await
        .unwrap();
    assert_eq!(conv_resp.status(), 201, "{}", conv_resp.text());
    let conv_body: serde_json::Value = conv_resp.json().await.unwrap();
    let session_a: uuid::Uuid = conv_body["session"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let conversation_a: i64 = conv_body["session"]["durable_conversation_id"]
        .as_i64()
        .unwrap_or_else(|| panic!("new agent conversation must be stamped: {conv_body}"));
    eprintln!("h21-integ: agent conversation {session_a} stamped durable {conversation_a}");

    // Bus listener: the projected assistant `message` + `turn_ended`.
    let mut bus_rx = app.app_state.bus.subscribe();

    let msg_resp = app
        .post(&format!(
            "/agents/{agent_id}/conversations/{session_a}/messages"
        ))
        .header("X-API-Key", &api_key)
        .json(&json!({ "content": "What is the capital of France?" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        msg_resp.status(),
        202,
        "harness submit accepted must keep the legacy 202 shape: {}",
        msg_resp.text()
    );
    let msg_body: serde_json::Value = msg_resp.json().await.unwrap();
    assert_eq!(msg_body["message"]["role"], "user", "{msg_body}");
    assert_eq!(
        msg_body["message"]["content"], "What is the capital of France?",
        "{msg_body}"
    );

    // Bounded wait for turn_ended on the bus.
    let mut saw_turn_ended = false;
    let mut saw_assistant_message = false;
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    while !saw_turn_ended {
        let ev = match tokio::time::timeout_at(deadline, bus_rx.recv()).await {
            Ok(Ok(ev)) => ev,
            Ok(Err(_)) => panic!("bus receiver lagged or closed"),
            Err(_) => panic!("timed out waiting for turn_ended on {session_a}"),
        };
        match ev {
            BusEvent::TurnEnded { session_id } if session_id == session_a => {
                saw_turn_ended = true;
            }
            BusEvent::Message { message }
                if message.session_id == session_a && message.role == "assistant" =>
            {
                saw_assistant_message = true;
                eprintln!(
                    "h21-integ: projected assistant bus message (seq {}): {:?}",
                    message.sequence, message.content
                );
            }
            _ => {}
        }
    }
    assert!(
        saw_assistant_message,
        "the projected assistant `message` bus event must have fired before turn_ended"
    );

    let assistant_text = wait_for_assistant_row(&pool, &session_a.to_string()).await;
    eprintln!("h21-integ: assistant row landed: {assistant_text:?}");
    assert_eq!(
        assistant_text, "Paris.",
        "the projected assistant row must carry the faux answer verbatim"
    );

    // Back to idle.
    let active_resp = app
        .get(&format!("/agents/{agent_id}/active"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(active_resp.status(), 200, "{}", active_resp.text());
    let active_body: serde_json::Value = active_resp.json().await.unwrap();
    assert_eq!(
        active_body["busy"], false,
        "the agent must be idle after the turn: {active_body}"
    );
    assert_eq!(
        active_body["current_conversation"],
        session_a.to_string(),
        "{active_body}"
    );

    // ---- 2. The raw-session path (POST /sessions + POST /messages) ----
    let sess_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "H21 raw session" }))
        .send()
        .await
        .unwrap();
    assert_eq!(sess_resp.status(), 201, "{}", sess_resp.text());
    let sess_body: serde_json::Value = sess_resp.json().await.unwrap();
    let session_b: uuid::Uuid = sess_body["session"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let conversation_b: i64 = sess_body["session"]["durable_conversation_id"]
        .as_i64()
        .unwrap_or_else(|| panic!("new session must be stamped: {sess_body}"));
    eprintln!("h21-integ: session {session_b} stamped durable {conversation_b}");

    let msg2_resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({ "session_id": session_b.to_string(), "content": "Check in." }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        msg2_resp.status(),
        202,
        "second turn must keep the 202 shape: {}",
        msg2_resp.text()
    );

    let assistant_text_b = wait_for_assistant_row(&pool, &session_b.to_string()).await;
    eprintln!("h21-integ: second assistant row landed: {assistant_text_b:?}");
    assert_eq!(
        assistant_text_b,
        "Iris reports in: the shadow-parity seed holds."
    );

    // ---- 3. Direct SQL: durable side + projection bookkeeping ----
    // `durable_entries` has no indexed `kind` column — the entry kind
    // lives in the JSON `record` payload (see durable-pg 001).
    let entry_kinds: Vec<String> = sqlx::query_scalar(
        &format!("SELECT DISTINCT record::jsonb ->> 'kind' FROM {schema}.durable_entries WHERE conversation_id = $1"),
    )
    .bind(conversation_a)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        entry_kinds.contains(&"pi.assistant".to_string()),
        "conversation {conversation_a} must hold a pi.assistant entry, got {entry_kinds:?}"
    );

    let projection_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM durable_projection")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        projection_rows, 2,
        "one projection claim per projected entry (2 turns), got {projection_rows}"
    );
    let claims: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT conversation_id, entry_id FROM durable_projection ORDER BY entry_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        claims.iter().any(|(c, _)| *c == conversation_a)
            && claims.iter().any(|(c, _)| *c == conversation_b),
        "both conversations must be claimed: {claims:?}"
    );

    // Transcript of the test (for the build log / report).
    eprintln!("h21-integ: transcript session A:");
    for row in sqlx::query_as::<_, (String, String)>(
        "SELECT role, content FROM messages WHERE session_id = $1 ORDER BY sequence",
    )
    .bind(session_a)
    .fetch_all(&pool)
    .await
    .unwrap()
    {
        let (role, content) = row;
        eprintln!("h21-integ:   {role}: {content}");
    }
    eprintln!("h21-integ: transcript session B:");
    for row in sqlx::query_as::<_, (String, String)>(
        "SELECT role, content FROM messages WHERE session_id = $1 ORDER BY sequence",
    )
    .bind(session_b)
    .fetch_all(&pool)
    .await
    .unwrap()
    {
        let (role, content) = row;
        eprintln!("h21-integ:   {role}: {content}");
    }

    pool.close().await;
    let _ = child.kill();
    let _ = child.wait();
}
