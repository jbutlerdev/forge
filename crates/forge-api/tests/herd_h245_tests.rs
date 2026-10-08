//! Herd H2.4 / H2.5 — the API surface over the live harness.
//!
//! Spawns the **real Node harness** child process (scripted faux
//! provider, scratch `durable_*` schema, tempdir sockets) exactly like
//! `harness_turn_tests.rs`, then exercises the H2.4/H2.5 routes:
//!
//! H2.4 (compaction / reset / history):
//! 1. `GET /sessions/:id/context` reports `source: "harness"` with the
//!    active window size.
//! 2. `POST /sessions/:id/compact` forwards to the harness and returns
//!    the background task id.
//! 3. `POST /sessions/:id/reset` admits the reset (pi.reset head entry
//!    lands in `durable_entries`), and the OLD segment stays readable
//!    through `GET /sessions/:id/history?q=` (ILIKE over
//!    `durable_entries`).
//! 4. The turn AFTER the reset runs on the new segment (assistant row
//!    lands from the handoff note forward).
//!
//! H2.5 (documents / allowlist):
//! 5. `PUT`/`GET /sessions/:id/documents/:name` round-trips; an absent
//!    document is 404; the `document_changed` event reaches BOTH the
//!    in-process bus (event consumer) and the SSE stream.
//! 6. An agent with a non-empty `tools_allowlist` has its conversation
//!    stamped with the allowlist: a `bash` tool call is BLOCKED by the
//!    harness `before_tool` hook (the reason lands in the durable
//!    transcript; the call never reaches `/tools/execute`), and the
//!    turn still completes with the next faux answer.
//!
//! The faux provider is a single global queue shared by every
//! conversation, so all turns in this test run strictly sequentially
//! (each awaited to completion before the next starts) and the queued
//! answers are ordered to that exact call sequence.

use futures_util::StreamExt;
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
            "email": "h245-integ@example.com",
            "name": "H245 Integ User",
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
            "email": "h245-integ@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(login_resp.status(), 200, "{}", login_resp.text());
    let body: serde_json::Value = login_resp.json().await.unwrap();
    body["api_key"].as_str().unwrap().to_string()
}

/// Wait until an assistant row with `content = expected` exists for
/// the session (the projected durable answer).
async fn wait_for_assistant_row(pool: &sqlx::PgPool, session_id: uuid::Uuid, expected: &str) {
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    loop {
        match sqlx::query_scalar::<_, String>(
            "SELECT content FROM messages WHERE session_id = $1 AND role = 'assistant' AND content = $2 LIMIT 1",
        )
        .bind(session_id)
        .bind(expected)
        .fetch_optional(pool)
        .await
        {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(e) => panic!("assistant row poll failed: {e}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for assistant row {expected:?} on session {session_id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until a `durable_entries` row for the conversation matches the
/// LIKE pattern (the harness-side durable state).
async fn wait_for_durable_entry(
    pool: &sqlx::PgPool,
    schema: &str,
    conversation_id: i64,
    like: &str,
    what: &str,
) -> String {
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    loop {
        let sql = format!(
            "SELECT record FROM {schema}.durable_entries WHERE conversation_id = $1 AND record ILIKE $2 LIMIT 1"
        );
        match sqlx::query_scalar::<_, String>(&sql)
            .bind(conversation_id)
            .bind(like)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(record)) => return record,
            Ok(None) => {}
            Err(e) => panic!("durable entry poll failed: {e}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what} in durable_entries"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn herd_h245_routes_end_to_end() {
    // ---- scaffolding (identical to harness_turn_tests.rs) ----
    let schema = format!("forge_h245_{}", uuid::Uuid::new_v4().simple());
    let socket_dir = tempfile::tempdir().expect("socket tempdir");
    let rpc_sock = socket_dir.path().join("harness.sock");
    let events_sock = socket_dir.path().join("harness-events.sock");

    let harness = forge_api::harness::HarnessState::from_paths_with(
        &rpc_sock,
        &events_sock,
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    let (app, db_url) = test_helpers::TestApp::with_harness(harness, true).await;
    let consumer_handle = forge_api::harness::spawn_event_consumer(app.app_state.clone());
    assert!(consumer_handle.is_some(), "event consumer must spawn");

    let api_key = register_and_login(&app).await;

    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "H245 Faux Profile",
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

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .unwrap();

    // The global faux queue, in exact call order:
    //   1. session A turn 1 — the needle answer (later compacted out
    //      of the active window; stays in durable_entries)
    //   2. session A turn 2 — after the reset, from the handoff note
    //   3. session B turn 1 first generation — a bash tool call
    //      (BLOCKED by the allowlist hook; never reaches /tools/execute)
    //   4. session B turn 1 final generation — the plain answer
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
        r#"["herd-needle-x7f3 is the answer", "after-reset-answer-marker", { "toolCall": { "name": "bash", "input": { "command": "rm -rf /" } } }, "read-only final answer"]"#,
    )
    .spawn()
    .expect("spawn the harness child");
    eprintln!(
        "h245-integ: harness child pid {:?} (schema {schema})",
        child.id()
    );

    let deadline = tokio::time::Instant::now() + HARNESS_BOOT_WAIT;
    while !rpc_sock.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "harness child did not open its RPC socket in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ---- session A: the raw-session path ----
    let sess_resp = app
        .post("/sessions")
        .header("X-API-Key", &api_key)
        .json(&json!({ "profile_id": profile_id, "title": "H245 session" }))
        .send()
        .await
        .unwrap();
    assert_eq!(sess_resp.status(), 201, "{}", sess_resp.text());
    let sess_body: serde_json::Value = sess_resp.json().await.unwrap();
    let session_a: uuid::Uuid = sess_body["session"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let conversation_a: i64 = sess_body["session"]["durable_conversation_id"]
        .as_i64()
        .unwrap_or_else(|| panic!("new session must be stamped: {sess_body}"));
    eprintln!("h245-integ: session {session_a} stamped durable {conversation_a}");

    // Turn 1.
    let msg_resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": session_a.to_string(),
            "content": "what is the needle?"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(msg_resp.status(), 202, "{}", msg_resp.text());
    wait_for_assistant_row(&pool, session_a, "herd-needle-x7f3 is the answer").await;

    // (1) context: harness source, active window sized.
    let ctx_resp = app
        .get(&format!("/sessions/{session_a}/context"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(ctx_resp.status(), 200, "{}", ctx_resp.text());
    let ctx: serde_json::Value = ctx_resp.json().await.unwrap();
    assert_eq!(ctx["source"], "harness", "{ctx}");
    assert!(
        ctx["active_context_chars"].as_u64().unwrap_or(0) > 0,
        "active window must be non-empty: {ctx}"
    );
    eprintln!("h245-integ: context -> {ctx}");

    // (2) compact: forward to the harness; task id comes back.
    let compact_resp = app
        .post(&format!("/sessions/{session_a}/compact"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(compact_resp.status(), 200, "{}", compact_resp.text());
    let compact_body: serde_json::Value = compact_resp.json().await.unwrap();
    assert_eq!(compact_body["ok"], true, "{compact_body}");
    let task_id = compact_body["task_id"]
        .as_i64()
        .unwrap_or_else(|| panic!("compact must return the harness task id: {compact_body}"));
    eprintln!("h245-integ: compact accepted, harness task {task_id}");

    // ---- documents (H2.5) ----
    // Absent document is a 404.
    let miss_resp = app
        .get(&format!("/sessions/{session_a}/documents/plan"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        miss_resp.status(),
        404,
        "absent document must 404: {}",
        miss_resp.text()
    );

    // Bus listener for the document_changed publish (event consumer).
    let mut bus_rx = app.app_state.bus.subscribe();
    // SSE listener: open the stream BEFORE the PUT so the event is
    // delivered live, not by catch-up. A raw reqwest call (the
    // TestApp wrapper reads the whole body, which would block on an
    // endless stream).
    let sse_resp = reqwest::Client::new()
        .get(format!(
            "{}/sessions/{session_a}/events?since=0",
            app.base_url
        ))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .expect("sse connect");
    assert_eq!(sse_resp.status(), 200, "sse must open");
    let mut sse_stream = sse_resp.bytes_stream();

    let put_resp = app
        .put(&format!("/sessions/{session_a}/documents/plan"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "value": "plan-doc-v1: ship the herd" }))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), 200, "{}", put_resp.text());
    let put_body: serde_json::Value = put_resp.json().await.unwrap();
    assert_eq!(put_body["ok"], true, "{put_body}");
    assert_eq!(put_body["name"], "plan", "{put_body}");

    // (5) bus: the event consumer republished the harness event.
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    let mut saw_doc_bus = false;
    while !saw_doc_bus {
        let ev = match tokio::time::timeout_at(deadline, bus_rx.recv()).await {
            Ok(Ok(ev)) => ev,
            Ok(Err(_)) => panic!("bus receiver lagged or closed"),
            Err(_) => panic!("timed out waiting for the bus document_changed event"),
        };
        if let BusEvent::DocumentChanged { session_id, name } = ev {
            if session_id == session_a && name == "plan" {
                saw_doc_bus = true;
                eprintln!("h245-integ: bus document_changed observed");
            }
        }
    }

    // GET round-trips the value.
    let get_resp = app
        .get(&format!("/sessions/{session_a}/documents/plan"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.status(), 200, "{}", get_resp.text());
    let get_body: serde_json::Value = get_resp.json().await.unwrap();
    assert_eq!(
        get_body["value"], "plan-doc-v1: ship the herd",
        "{get_body}"
    );
    assert_eq!(get_body["name"], "plan", "{get_body}");

    // (5) SSE: the stream delivered the document_changed event.
    let mut sse_buf: Vec<u8> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut saw_sse_doc = false;
    while !saw_sse_doc {
        match tokio::time::timeout_at(deadline, sse_stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                sse_buf.extend_from_slice(&chunk);
                let text = String::from_utf8_lossy(&sse_buf).into_owned();
                if text.contains("event: document_changed") && text.contains("\"plan\"") {
                    saw_sse_doc = true;
                    let start = text.find("event: document_changed").unwrap_or(0);
                    let end = text[start..]
                        .char_indices()
                        .map(|(i, _)| i)
                        .find(|i| *i >= 300)
                        .unwrap_or_else(|| text.len() - start);
                    eprintln!(
                        "h245-integ: SSE document_changed observed: {}",
                        &text[start..start + end]
                    );
                }
            }
            Ok(Some(Err(e))) => panic!("sse stream error: {e}"),
            Ok(None) => panic!("sse stream closed before document_changed"),
            Err(_) => panic!("timed out waiting for the SSE document_changed event"),
        }
    }

    // ---- reset + history (H2.4) ----
    // (3) reset: admit a new segment from a handoff note.
    let reset_resp = app
        .post(&format!("/sessions/{session_a}/reset"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "handoff_note": "handoff-marker-7f3: keep going" }))
        .send()
        .await
        .unwrap();
    assert_eq!(reset_resp.status(), 200, "{}", reset_resp.text());
    let reset_body: serde_json::Value = reset_resp.json().await.unwrap();
    assert_eq!(reset_body["ok"], true, "{reset_body}");
    let reset_entry = wait_for_durable_entry(
        &pool,
        &schema,
        conversation_a,
        "%pi.reset%",
        "the pi.reset head entry",
    )
    .await;
    assert!(
        reset_entry.contains("handoff-marker-7f3"),
        "the reset entry must carry the handoff note: {reset_entry}"
    );

    // (3) history: the OLD segment (pre-reset) is still searchable in
    // durable_entries, even though it is out of the model's active
    // window.
    let hist_resp = app
        .get(&format!("/sessions/{session_a}/history?q=herd-needle-x7f3"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(hist_resp.status(), 200, "{}", hist_resp.text());
    let hist_body: serde_json::Value = hist_resp.json().await.unwrap();
    let matches = hist_body["matches"]
        .as_array()
        .unwrap_or_else(|| panic!("history must carry a matches array: {hist_body}"));
    let found = matches
        .iter()
        .any(|m| m["text"].as_str() == Some("herd-needle-x7f3 is the answer"));
    assert!(
        found,
        "the pre-reset answer must still be searchable via /history?q=: {hist_body}"
    );
    eprintln!("h245-integ: history?q= found {} match(es)", matches.len());

    // A missing q= is a 400, not an empty scan.
    let noq_resp = app
        .get(&format!("/sessions/{session_a}/history"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(noq_resp.status(), 400, "{}", noq_resp.text());

    // (4) the next turn runs on the NEW segment (from the handoff note).
    let msg2_resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": session_a.to_string(),
            "content": "continue"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(msg2_resp.status(), 202, "{}", msg2_resp.text());
    wait_for_assistant_row(&pool, session_a, "after-reset-answer-marker").await;
    eprintln!("h245-integ: post-reset turn completed on the new segment");

    // context still reports the harness source after the reset.
    let ctx2_resp = app
        .get(&format!("/sessions/{session_a}/context"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(ctx2_resp.status(), 200, "{}", ctx2_resp.text());
    let ctx2: serde_json::Value = ctx2_resp.json().await.unwrap();
    assert_eq!(ctx2["source"], "harness", "{ctx2}");

    // ---- session B: the allowlist agent (H2.5) ----
    // An agent whose tools_allowlist is ["read"]: bash must be blocked
    // by the harness before_tool hook (the call never reaches
    // /tools/execute).
    let agent_resp = app
        .post("/agents")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "H245 Allowlist Agent",
            "primary_profile_id": profile_id,
            "tools_allowlist": ["read"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(agent_resp.status(), 201, "{}", agent_resp.text());
    let agent_body: serde_json::Value = agent_resp.json().await.unwrap();
    let agent_id = agent_body["agent"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        agent_body["agent"]["tools_allowlist"],
        json!(["read"]),
        "the agent row must persist the allowlist: {agent_body}"
    );

    let conv_resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "title": "H245 allowlist conversation" }))
        .send()
        .await
        .unwrap();
    assert_eq!(conv_resp.status(), 201, "{}", conv_resp.text());
    let conv_body: serde_json::Value = conv_resp.json().await.unwrap();
    let session_b: uuid::Uuid = conv_body["session"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let conversation_b: i64 = conv_body["session"]["durable_conversation_id"]
        .as_i64()
        .unwrap_or_else(|| panic!("agent conversation must be stamped: {conv_body}"));
    // The session must carry the agent link (the allowlist lookup key).
    let agent_link: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT agent_id FROM sessions WHERE id = $1")
            .bind(session_b)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        agent_link,
        Some(agent_id.parse().unwrap()),
        "the conversation session must be linked to the agent"
    );
    eprintln!("h245-integ: allowlist conversation {session_b} stamped durable {conversation_b}");

    let msg3_resp = app
        .post(&format!(
            "/agents/{agent_id}/conversations/{session_b}/messages"
        ))
        .header("X-API-Key", &api_key)
        .json(&json!({ "content": "run the cleanup command" }))
        .send()
        .await
        .unwrap();
    assert_eq!(msg3_resp.status(), 202, "{}", msg3_resp.text());

    // The blocked tool round: the reason lands in the durable
    // transcript (a pi.tool-result entry the model reads).
    let blocked_entry = wait_for_durable_entry(
        &pool,
        &schema,
        conversation_b,
        "%not in this agent's tool allowlist%",
        "the allowlist block reason",
    )
    .await;
    assert!(
        blocked_entry.contains("bash"),
        "the block reason must name the blocked tool: {blocked_entry}"
    );
    eprintln!("h245-integ: bash blocked by the allowlist hook");

    // And the turn still completes with the final answer.
    wait_for_assistant_row(&pool, session_b, "read-only final answer").await;
    eprintln!("h245-integ: allowlist turn completed with the final answer");

    // ---- transcript of the test (for the build log / report) ----
    for sid in [session_a, session_b] {
        eprintln!("h245-integ: transcript {sid}:");
        for row in sqlx::query_as::<_, (String, String)>(
            "SELECT role, content FROM messages WHERE session_id = $1 ORDER BY sequence",
        )
        .bind(sid)
        .fetch_all(&pool)
        .await
        .unwrap()
        {
            let (role, content) = row;
            eprintln!("h245-integ:   {role}: {content}");
        }
    }

    pool.close().await;
    let _ = child.kill();
    let _ = child.wait();
}
