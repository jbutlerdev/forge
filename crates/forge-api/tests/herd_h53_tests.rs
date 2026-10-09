//! Herd H5.3 (part 1): the agent pause kill switch + the real agent
//! task listing.
//!
//! Covers:
//! - `POST /agents/:id/pause` / `POST /agents/:id/resume` — state,
//!   idempotency, tenancy (owner vs other → 404), admin access;
//! - `dispatch_message` 409 "agent paused" BEFORE the user row lands
//!   and before any harness interaction (the default `TestApp` runs
//!   with the harness DISABLED, where an unpaused write would be a
//!   503 — the 409 therefore proves the pause gate precedes the
//!   harness path);
//! - the agent-talk alias (`POST /agents/:id/conversations/:cid/messages`)
//!   409s the same way;
//! - `GET /agents/:id/tasks` — the durable-task join through
//!   `sessions.durable_conversation_id`, state filtering, and
//!   tenancy. The durable rows are seeded directly in the test
//!   schema (deterministic; the harness itself is not involved).
//!
//! The timer-fire no-op is tested on the HARNESS side (the admission
//! seam is `harness/src/timers.ts` `fire()` — see
//! `harness/test/agent-pause.test.ts`); there is no Rust admission
//! guard for timer prompts to unit-test.

mod test_helpers;

use serde_json::json;
use test_helpers::TestApp;
use uuid::Uuid;

const PASSWORD: &str = "password123";

// ============================================
// Helpers
// ============================================

async fn register_user(app: &TestApp, email: &str, name: &str) -> String {
    let resp = app
        .post("/auth/register")
        .json(&json!({ "email": email, "name": name, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "register {email} → 201");
    let resp = app
        .post("/auth/login")
        .json(&json!({ "email": email, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "login {email} → 200");
    resp.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn create_profile(app: &TestApp, api_key: &str, name: &str) -> Uuid {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": name,
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/herd-h53-profile"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "profile creation: {}", resp.text());
    Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

async fn create_agent(app: &TestApp, api_key: &str, name: &str, profile_id: Uuid) -> Uuid {
    let resp = app
        .post("/agents")
        .header("X-API-Key", api_key)
        .json(&json!({ "name": name, "primary_profile_id": profile_id.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "agent creation: {}", resp.text());
    Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

async fn create_agent_conversation(app: &TestApp, api_key: &str, agent_id: Uuid) -> Uuid {
    let resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", api_key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "agent conversation creation: {}",
        resp.text()
    );
    Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

fn pool_for(db_url: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(db_url)
        .expect("lazy test pool")
}

// ============================================
// Pause / resume
// ============================================

#[tokio::test]
async fn pause_resume_state_and_idempotency() {
    let (app, _db_url) = TestApp::new().await;
    let key = register_user(&app, "pause1@example.com", "Pause One").await;
    let profile = create_profile(&app, &key, "pause1-profile").await;
    let agent_id = create_agent(&app, &key, "Comet", profile).await;

    // Unpaused by default.
    let resp = app
        .get(&format!("/agents/{agent_id}"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["agent"]["paused"],
        false
    );

    // Pause → 200 {id, paused: true}; repeat → idempotent 200.
    for i in 0..2 {
        let resp = app
            .post(&format!("/agents/{agent_id}/pause"))
            .header("X-API-Key", &key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "pause #{i} → 200: {}", resp.text());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["paused"], true);
        assert_eq!(
            Uuid::parse_str(body["id"].as_str().unwrap()).unwrap(),
            agent_id
        );
    }
    let resp = app
        .get(&format!("/agents/{agent_id}"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["agent"]["paused"],
        true
    );

    // Resume → 200 {id, paused: false}; repeat → idempotent 200.
    for i in 0..2 {
        let resp = app
            .post(&format!("/agents/{agent_id}/resume"))
            .header("X-API-Key", &key)
            .json(&json!({ "reason": "done investigating" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "resume #{i} → 200: {}", resp.text());
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap()["paused"],
            false
        );
    }

    // Pause again WITH a reason body (logged, not stored).
    let resp = app
        .post(&format!("/agents/{agent_id}/pause"))
        .header("X-API-Key", &key)
        .json(&json!({ "reason": "runaway" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "pause with reason: {}", resp.text());
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["paused"],
        true
    );
}

#[tokio::test]
async fn pause_resume_tenancy_and_unknown_agent() {
    let (app, _db_url) = TestApp::new().await;
    let owner_key = register_user(&app, "pause2a@example.com", "Pause Two A").await;
    let other_key = register_user(&app, "pause2b@example.com", "Pause Two B").await;
    let profile = create_profile(&app, &owner_key, "pause2-profile").await;
    let agent_id = create_agent(&app, &owner_key, "Vega", profile).await;

    // A second user cannot pause or resume a foreign agent (404, not
    // 403 — no existence leak), and the flag does not move.
    for path in [
        &format!("/agents/{agent_id}/pause"),
        &format!("/agents/{agent_id}/resume"),
    ] {
        let resp = app
            .post(path)
            .header("X-API-Key", &other_key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404, "foreign {path} → 404: {}", resp.text());
    }
    let resp = app
        .get(&format!("/agents/{agent_id}"))
        .header("X-API-Key", &owner_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["agent"]["paused"],
        false
    );

    // Unknown agent id → 404 for the owner too.
    let missing = Uuid::new_v4();
    let resp = app
        .post(&format!("/agents/{missing}/pause"))
        .header("X-API-Key", &owner_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown agent → 404: {}", resp.text());
}

// ============================================
// Turn admission: 409 before the user row lands
// ============================================

#[tokio::test]
async fn dispatch_message_rejects_paused_agent_before_any_harness_contact() {
    let (app, db_url) = TestApp::new().await;
    let key = register_user(&app, "pause3@example.com", "Pause Three").await;
    let profile = create_profile(&app, &key, "pause3-profile").await;
    let agent_id = create_agent(&app, &key, "Lyra", profile).await;
    let session_id = create_agent_conversation(&app, &key, agent_id).await;
    let pool = pool_for(&db_url);

    // Unpaused: on the DEFAULT test app (harness disabled) the write
    // reaches the harness path and 503s (the user row lands first —
    // H2.6 audit semantics). Remember that row count: the paused write
    // below must not add to it.
    let resp = app
        .post("/messages")
        .header("X-API-Key", &key)
        .json(&json!({ "session_id": session_id.to_string(), "content": "hello" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "unpaused write on a disabled-harness app → 503 (harness path): {}",
        resp.text()
    );
    let rows_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1 AND role = 'user'")
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .expect("count user rows");
    assert_eq!(rows_before, 1, "the unpaused write persisted its audit row");

    // Pause → the SAME write 409s BEFORE the user row lands and before
    // any harness interaction (the 503 above is unreachable now).
    let resp = app
        .post(&format!("/agents/{agent_id}/pause"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = app
        .post("/messages")
        .header("X-API-Key", &key)
        .json(&json!({ "session_id": session_id.to_string(), "content": "still there?" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409, "paused agent → 409: {}", resp.text());
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["error"],
        "agent paused"
    );

    // No partial state: no NEW user row for the session.
    let n: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1 AND role = 'user'")
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .expect("count user rows");
    assert_eq!(
        n, rows_before,
        "the rejected turn must not leave a user row"
    );

    // The agent-talk alias rejects the same way (same gate).
    let resp = app
        .post(&format!(
            "/agents/{agent_id}/conversations/{session_id}/messages"
        ))
        .header("X-API-Key", &key)
        .json(&json!({ "content": "talk while paused" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        409,
        "agent-talk alias → 409: {}",
        resp.text()
    );
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["error"],
        "agent paused"
    );

    // Resume → back to the harness-path 503 (admission open again).
    let resp = app
        .post(&format!("/agents/{agent_id}/resume"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = app
        .post("/messages")
        .header("X-API-Key", &key)
        .json(&json!({ "session_id": session_id.to_string(), "content": "hello again" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "resumed agent → 503 again (harness disabled in tests): {}",
        resp.text()
    );

    // A session NOT bound to an agent is never paused.
    let resp = app
        .post("/sessions")
        .header("X-API-Key", &key)
        .json(&json!({ "profile_id": profile.to_string() }))
        .send()
        .await
        .unwrap();
    let bare_session: Uuid = Uuid::parse_str(
        resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let resp = app
        .post("/messages")
        .header("X-API-Key", &key)
        .json(&json!({ "session_id": bare_session.to_string(), "content": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "agent-less session takes the harness path (503), not the pause gate: {}",
        resp.text()
    );
    pool.close().await;
}

// ============================================
// GET /agents/:id/tasks — the durable-task join
// ============================================

/// The minimal `durable_tasks` shape (durable-pg
/// `migrations/001_initial.sql`); the test DBs carry no durable-pg
/// migrations, so the table is created directly in the test schema.
const DURABLE_TASKS_DDL: &str = "CREATE TABLE durable_tasks (
    id BIGINT PRIMARY KEY,
    conversation_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
    abort_requested BOOLEAN NOT NULL,
    background BOOLEAN NOT NULL,
    record TEXT NOT NULL
)";

/// Stamp a session as the agent's durable conversation N.
async fn stamp_session(
    pool: &sqlx::PgPool,
    session_id: Uuid,
    agent_id: Uuid,
    conversation_id: i64,
) {
    sqlx::query("UPDATE sessions SET agent_id = $2, durable_conversation_id = $3 WHERE id = $1")
        .bind(session_id)
        .bind(agent_id)
        .bind(conversation_id)
        .execute(pool)
        .await
        .expect("stamp session");
}

async fn seed_task(
    pool: &sqlx::PgPool,
    id: i64,
    conversation_id: i64,
    status: &str,
    kind: &str,
    owner: Option<i64>,
) {
    let record = match owner {
        Some(parent) => format!(
            r#"{{"id":"task_{id}","conversationId":{conversation_id},"kind":"{kind}","version":1,"input":{{}},"owner":"task_{parent}","background":false,"state":{{"status":"{status}"}}}}"#
        ),
        None => format!(
            r#"{{"id":"task_{id}","conversationId":{conversation_id},"kind":"{kind}","version":1,"input":{{}},"background":false,"state":{{"status":"{status}"}}}}"#
        ),
    };
    sqlx::query(
        "INSERT INTO durable_tasks (id, conversation_id, kind, status, abort_requested, background, record)
         VALUES ($1, $2, $3, $4, FALSE, FALSE, $5)",
    )
    .bind(id)
    .bind(conversation_id)
    .bind(kind)
    .bind(status)
    .bind(record)
    .execute(pool)
    .await
    .expect("seed task");
}

#[tokio::test]
async fn agent_tasks_join_state_filter_and_tenancy() {
    let (app, db_url) = TestApp::new().await;
    let key = register_user(&app, "tasks1@example.com", "Tasks One").await;
    let other_key = register_user(&app, "tasks2@example.com", "Tasks Two").await;
    let profile = create_profile(&app, &key, "tasks-profile").await;
    let other_profile = create_profile(&app, &other_key, "tasks-profile-2").await;
    let agent_id = create_agent(&app, &key, "Rigel", profile).await;
    let agent2_id = create_agent(&app, &other_key, "Suhail", other_profile).await;
    let pool = pool_for(&db_url);
    sqlx::query(DURABLE_TASKS_DDL)
        .execute(&pool)
        .await
        .expect("create durable_tasks");

    // Two of THIS agent's conversations + one of another agent's.
    let c1 = create_agent_conversation(&app, &key, agent_id).await;
    let c2 = create_agent_conversation(&app, &key, agent_id).await;
    let c3 = create_agent_conversation(&app, &other_key, agent2_id).await;
    stamp_session(&pool, c1, agent_id, 101).await;
    stamp_session(&pool, c2, agent_id, 102).await;
    stamp_session(&pool, c3, agent2_id, 103).await;

    // Seed: pending + running + waiting + terminal on 101; running on
    // 102 (a child task with an owner); a pending task on the OTHER
    // agent's conversation 103.
    seed_task(&pool, 1, 101, "pending", "tool", None).await;
    seed_task(&pool, 2, 101, "running", "tool", None).await;
    seed_task(&pool, 3, 101, "waiting", "tool", Some(2)).await;
    seed_task(&pool, 4, 101, "terminal", "research", None).await;
    seed_task(&pool, 5, 102, "running", "tool", None).await;
    seed_task(&pool, 6, 103, "pending", "tool", None).await;

    // Default: all NON-terminal tasks of the agent, id DESC; the
    // other agent's task and the terminal task are excluded.
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "tasks → 200: {}", resp.text());
    let tasks = resp.json::<serde_json::Value>().await.unwrap()["tasks"]
        .as_array()
        .cloned()
        .unwrap();
    let ids: Vec<i64> = tasks
        .iter()
        .map(|t| t["task_id"].as_i64().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![5, 3, 2, 1],
        "default = non-terminal, newest first: {ids:?}"
    );
    let t3 = &tasks[1];
    assert_eq!(t3["state"], "waiting");
    assert_eq!(t3["kind"], "tool");
    assert_eq!(
        t3["parent_task_id"], "task_2",
        "child task exposes its owner"
    );
    assert_eq!(tasks[0]["parent_task_id"], serde_json::Value::Null);

    // Explicit state list (the Activity View's poll).
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks?state=pending,running"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tasks = resp.json::<serde_json::Value>().await.unwrap()["tasks"]
        .as_array()
        .cloned()
        .unwrap();
    let ids: Vec<i64> = tasks
        .iter()
        .map(|t| t["task_id"].as_i64().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![5, 2, 1],
        "pending+running across both conversations, newest first: {ids:?}"
    );

    // Terminal must be requested explicitly.
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks?state=terminal"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tasks = resp.json::<serde_json::Value>().await.unwrap()["tasks"]
        .as_array()
        .cloned()
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["task_id"], 4);
    assert_eq!(tasks[0]["kind"], "research");

    // Unknown state → 400 (the list is a closed set).
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks?state=done"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "unknown state → 400: {}", resp.text());

    // limit caps the list.
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks?limit=2"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tasks = resp.json::<serde_json::Value>().await.unwrap()["tasks"]
        .as_array()
        .cloned()
        .unwrap();
    assert_eq!(tasks.len(), 2);

    // Tenancy: the other user's listing of THIS agent → 404; and their
    // own listing sees only their own conversation's tasks.
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks"))
        .header("X-API-Key", &other_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "foreign agent tasks → 404: {}",
        resp.text()
    );
    let resp = app
        .get(&format!("/agents/{agent2_id}/tasks?state=pending"))
        .header("X-API-Key", &other_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tasks = resp.json::<serde_json::Value>().await.unwrap()["tasks"]
        .as_array()
        .cloned()
        .unwrap();
    assert_eq!(tasks.len(), 1, "only agent2's own tasks: {tasks:?}");
    assert_eq!(tasks[0]["task_id"], 6);

    // A paused agent's task listing stays OPEN (reads are not gated).
    let resp = app
        .post(&format!("/agents/{agent_id}/pause"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = app
        .get(&format!("/agents/{agent_id}/tasks"))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "paused agent's task listing stays open: {}",
        resp.text()
    );

    pool.close().await;
}
