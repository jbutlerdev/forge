//! Herd H5.1 — proactive research: the live end-to-end acceptance.
//!
//! Spawns the **real Node harness** as a child process (scripted faux
//! provider), drives `POST /agents/:id/research`, and asserts the
//! full lifecycle:
//!
//! 1. the task runs (event consumer marks it `running` with its task
//!    id, settles it to `done`);
//! 2. the tool registry pi received for the research conversation has
//!    NO write/bash/edit entries and exactly the research surface
//!    (`extensionTools` RPC against the live registry);
//! 3. the `research_report` document lands on the conversation;
//! 4. the `research_report` card is pending on the bus;
//! 5. answering `use` via `POST /ranch-tools/:id/result` flips the
//!    row to `adopted` and publishes `research_resolved`; the open
//!    listing then excludes it.
//!
//! Plus the tenancy gates (foreign-agent 404s, bad-input 400,
//! disabled-harness 503). The `agent_research` table is plain DDL
//! (no vector column), so these run WITHOUT pgvector — the skip-when
//! contract of `memory_tests.rs` does not apply.

use forge_api::bus::BusEvent;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::process;
use std::time::Duration;

mod test_helpers;

use test_helpers::TestApp;

const TURN_WAIT: Duration = Duration::from_secs(90);
const HARNESS_BOOT_WAIT: Duration = Duration::from_secs(120);

/// Reaps the harness child process on EVERY exit path — including
/// panics mid-test. The child holds stdout open; an unreaped child
/// would make any pipeline waiting on it hang forever.
struct ChildReaper(process::Child);
impl Drop for ChildReaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The agent_research state machine is monotonic in
/// pending → running → done → adopted/discarded: a wait for an earlier
/// state also accepts a later one. (The live process's faux turn
/// settles in milliseconds, so the transient `running` state is
/// routinely missed between two 100 ms polls.)
fn state_rank(state: &str) -> u8 {
    match state {
        "pending" => 0,
        "running" => 1,
        "done" => 2,
        _ => 3, // adopted | discarded — past `done`
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

/// Poll `agent_research.state` until it reaches `want` (or passes it);
/// returns the row at that point.
async fn wait_state(
    pool: &sqlx::PgPool,
    research_id: &str,
    want: &str,
    timeout: Duration,
) -> (String, Option<String>) {
    let want_rank = state_rank(want);
    // UUID-typed bind: `id = $1` with a text bind is `uuid = text` and
    // fails every poll (the previous silent-timeout bug).
    let rid = uuid::Uuid::parse_str(research_id).expect("research_id is a uuid");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT state, resolution FROM agent_research WHERE id = $1")
                .bind(rid)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
        if let Some((state, resolution)) = row {
            if state_rank(&state) >= want_rank {
                return (state, resolution);
            }
        }
        let timed_out = tokio::time::Instant::now() >= deadline;
        if timed_out {
            // Debug dump before the assertion fires.
            match sqlx::query_as::<_, (Option<i64>, Option<i64>, String, Option<String>)>(
                "SELECT s.durable_conversation_id, ar.task_id, ar.state, ar.resolution FROM agent_research ar JOIN sessions s ON s.id = ar.conversation_id WHERE ar.id = $1",
            )
            .bind(rid)
            .fetch_optional(pool)
            .await
            {
                Ok(cur) => eprintln!("h51-integ: debug row {cur:?}"),
                Err(e) => eprintln!("h51-integ: debug row query failed {e}"),
            }
            match sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM messages m JOIN agent_research ar ON ar.conversation_id = m.session_id WHERE ar.id = $1",
            )
            .bind(rid)
            .fetch_optional(pool)
            .await
            {
                Ok(n) => eprintln!("h51-integ: debug message rows {:?}", n),
                Err(e) => eprintln!("h51-integ: debug message count failed {e}"),
            }
        }
        assert!(
            !timed_out,
            "timed out waiting for agent_research {research_id} to reach {want}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn research_task_end_to_end() {
    // Scratch schema for the durable_* tables (same database as the
    // forge test DB; the harness pins search_path to it).
    let schema = format!("forge_h51_{}", uuid::Uuid::new_v4().simple());
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
    let consumer_handle = forge_api::harness::spawn_event_consumer(app.app_state.clone());
    assert!(consumer_handle.is_some(), "event consumer must spawn");

    let api_key = register_and_login(&app, "h51-integ@example.com", "H51 Integ User").await;

    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "H51 Faux Profile",
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

    // Spawn the real harness child process: the faux provider's single
    // queued response is the research REPORT (the task's final message).
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
        .env(
            "FORGE_HARNESS_FAUX_RESPONSES",
            r#"["REPORT: SearXNG is a privacy-focused metasearch engine; findings: (1) it aggregates multiple search engines without tracking users. Sources: https://searxng.org"]"#,
        )
        .spawn()
        .expect("spawn the harness child");
    let child = ChildReaper(child); // reaped on panic too
    eprintln!(
        "h51-integ: harness child pid {:?} (schema {schema})",
        child.0.id()
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

    // ---- the agent ----
    let agent_resp = app
        .post("/agents")
        .header("X-API-Key", &api_key)
        .json(&json!({ "name": "H51 Research Agent", "primary_profile_id": profile_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(agent_resp.status(), 201, "{}", agent_resp.text());
    let agent_id = agent_resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Bus listener BEFORE the POST: the card lands when the task
    // settles (seconds later), and the projected assistant message on
    // the research session's stream is part of the acceptance.
    let mut bus_rx = app.app_state.bus.subscribe();

    // ---- 1. start the research task ----
    let research_resp = app
        .post(&format!("/agents/{agent_id}/research"))
        .header("X-API-Key", &api_key)
        .json(&json!({
            "question": "What is SearXNG and what makes it different?",
            "scope": "quick fact-check"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        research_resp.status(),
        202,
        "POST /agents/:id/research must accept: {}",
        research_resp.text()
    );
    let research_body: serde_json::Value = research_resp.json().await.unwrap();
    let research_id = research_body["research"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let research_session: uuid::Uuid = research_body["research"]["conversation_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        research_body["research"]["state"], "pending",
        "{research_body}"
    );
    eprintln!("h51-integ: research task {research_id} on session {research_session}");

    // ---- 2. open listing includes it while unresolved ----
    let open_resp = app
        .get(&format!("/agents/{agent_id}/research?open=1"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(open_resp.status(), 200, "{}", open_resp.text());
    let open_body: serde_json::Value = open_resp.json().await.unwrap();
    let open_ids: Vec<&str> = open_body["research"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert!(open_ids.contains(&research_id.as_str()), "{open_body}");

    // ---- 3. the task runs: pending → running (task id learned) → done.
    // Monotonic wait: the faux turn settles in milliseconds, so the
    // transient `running` row is routinely missed between two polls;
    // the `task_id` stamp is the proof the `started` event was seen.
    wait_state(&pool, &research_id, "running", TURN_WAIT).await;
    eprintln!("h51-integ: research task reached running-or-later");
    let (state, resolution) = wait_state(&pool, &research_id, "done", TURN_WAIT).await;
    assert_eq!(state, "done");
    assert!(
        resolution.is_none(),
        "a done task has no resolution yet: {resolution:?}"
    );
    let task_id: Option<i64> =
        sqlx::query_scalar("SELECT task_id FROM agent_research WHERE id = $1")
            .bind(uuid::Uuid::parse_str(&research_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        task_id.is_some(),
        "the task id must be learned (at start, or backfilled at settlement)"
    );

    // ---- 4. registry absence: what pi actually received for this task ----
    let durable_conversation_id: i64 =
        sqlx::query_scalar("SELECT durable_conversation_id FROM sessions WHERE id = $1")
            .bind(research_session)
            .fetch_one(&pool)
            .await
            .unwrap();
    let tools = app
        .app_state
        .harness
        .client()
        .extension_tools(durable_conversation_id)
        .await
        .expect("extensionTools RPC must answer");
    let mut tools_sorted = tools.clone();
    tools_sorted.sort();
    assert_eq!(
        tools_sorted,
        vec!["note", "read", "search", "webfetch"],
        "the research registry must be EXACTLY the read-only surface: {tools:?}"
    );
    for denied in [
        "bash",
        "write",
        "edit",
        "spawn_subagent",
        "memory_remember",
        "agent_signal",
    ] {
        assert!(
            !tools.contains(&denied.to_string()),
            "{denied} must be ABSENT from the research registry: {tools:?}"
        );
    }

    // ---- 5. the report document landed ----
    let doc_resp = app
        .get(&format!(
            "/sessions/{research_session}/documents/research_report"
        ))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        doc_resp.status(),
        200,
        "research_report document must exist: {}",
        doc_resp.text()
    );
    let doc_body: serde_json::Value = doc_resp.json().await.unwrap();
    eprintln!("h51-integ: research_report document: {doc_body}");
    assert!(
        doc_body.to_string().contains("metasearch engine"),
        "the document must carry the faux report: {doc_body}"
    );

    // ---- 6. the card is pending: capture it from the bus ----
    let mut card_id: Option<String> = None;
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    while card_id.is_none() {
        let ev = match tokio::time::timeout_at(deadline, bus_rx.recv()).await {
            Ok(Ok(ev)) => ev,
            Ok(Err(_)) => panic!("bus receiver lagged or closed"),
            Err(_) => panic!("timed out waiting for the research_report card"),
        };
        if let BusEvent::RanchToolRequest { payload, .. } = ev {
            if payload["tool"] == "research_report" {
                card_id = payload["id"].as_str().map(String::from);
            }
        }
    }
    let card_id = card_id.unwrap();
    eprintln!("h51-integ: research_report card {card_id} pending");

    // ---- 7. answer Use via the result door ----
    let result_resp = app
        .post(&format!("/ranch-tools/{card_id}/result"))
        .header("X-API-Key", &api_key)
        .json(&json!({ "success": true, "output": { "action": "use" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        result_resp.status(),
        200,
        "result door must accept: {}",
        result_resp.text()
    );

    let (state, resolution) = wait_state(&pool, &research_id, "adopted", TURN_WAIT).await;
    assert_eq!(state, "adopted");
    assert_eq!(resolution, Some("use".to_string()));

    // research_resolved published on the bus.
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    let mut saw_resolved = false;
    while !saw_resolved {
        let ev = match tokio::time::timeout_at(deadline, bus_rx.recv()).await {
            Ok(Ok(ev)) => ev,
            Ok(Err(_)) => panic!("bus receiver lagged or closed"),
            Err(_) => panic!("timed out waiting for research_resolved"),
        };
        if let BusEvent::ResearchResolved {
            research_id: rid,
            state,
            resolution,
            ..
        } = ev
        {
            if rid == uuid::Uuid::parse_str(&research_id).unwrap() {
                saw_resolved = true;
                assert_eq!(state, "adopted");
                assert_eq!(resolution, "use");
            }
        }
    }

    // ---- 8. adopted ⇒ no longer open ----
    let open_resp = app
        .get(&format!("/agents/{agent_id}/research?open=1"))
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let open_body: serde_json::Value = open_resp.json().await.unwrap();
    let open_ids: Vec<&str> = open_body["research"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert!(!open_ids.contains(&research_id.as_str()), "{open_body}");

    pool.close().await;
}

#[tokio::test]
async fn research_tenancy_and_validation() {
    // Harness DISABLED (no sockets): the tenancy gates run before any
    // harness contact, and a valid request 503s on the harness check.
    let (app, _db_url) = TestApp::new().await;

    let key_a = register_and_login(&app, "h51-a@example.com", "H51 A").await;
    let key_b = register_and_login(&app, "h51-b@example.com", "H51 B").await;

    // User A's profile + agent.
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &key_a)
        .json(&json!({
            "name": "H51 Profile A",
            "provider": "openai",
            "model": "gpt-4o-mini",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(profile_resp.status(), 201, "{}", profile_resp.text());
    let profile_id = profile_resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let agent_resp = app
        .post("/agents")
        .header("X-API-Key", &key_a)
        .json(&json!({ "name": "H51 Tenancy Agent", "primary_profile_id": profile_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(agent_resp.status(), 201, "{}", agent_resp.text());
    let agent_id = agent_resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Foreign agent → 404 (no existence leak), both verbs.
    let ghost_agent = uuid::Uuid::new_v4();
    let resp = app
        .get(&format!("/agents/{ghost_agent}/research"))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "{}", resp.text());

    let resp = app
        .post(&format!("/agents/{ghost_agent}/research"))
        .header("X-API-Key", &key_a)
        .json(&json!({ "question": "anything" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "{}", resp.text());

    // User B on user A's agent → 404 (owner-gated, not 403).
    let resp = app
        .get(&format!("/agents/{agent_id}/research"))
        .header("X-API-Key", &key_b)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "{}", resp.text());
    let resp = app
        .post(&format!("/agents/{agent_id}/research"))
        .header("X-API-Key", &key_b)
        .json(&json!({ "question": "anything" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "{}", resp.text());

    // Owner, empty question → 400.
    let resp = app
        .post(&format!("/agents/{agent_id}/research"))
        .header("X-API-Key", &key_a)
        .json(&json!({ "question": "   " }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "{}", resp.text());

    // Owner, valid input but harness disabled → 503 (no task, no row).
    let resp = app
        .post(&format!("/agents/{agent_id}/research"))
        .header("X-API-Key", &key_a)
        .json(&json!({ "question": "What is up?" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "disabled harness must 503 before any row lands: {}",
        resp.text()
    );

    // The owner's listing is empty (nothing landed).
    let resp = app
        .get(&format!("/agents/{agent_id}/research?open=1"))
        .header("X-API-Key", &key_a)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["research"].as_array().unwrap().is_empty(), "{body}");
}
