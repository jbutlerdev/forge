//! Herd H2.6 (Migrate & cutover) — lazy-migration tests.
//!
//! The cutover deleted the legacy turn driver; an unstamped session
//! (`sessions.durable_conversation_id IS NULL`) is migrated onto a
//! durable harness conversation **on its first write**
//! (`crate::harness_migration::ensure_migrated`):
//!
//! 1. claim (atomic `UPDATE … WHERE … harness_migrating` — migration
//!    021) so concurrent writes run the migration exactly once;
//! 2. `createConversation` with the same parameters the H2.1
//!    session-creation attach uses;
//! 3. the `messages` transcript imported as pi-durable entries in ONE
//!    `importEntries` commit (the caller's just-inserted prompt row
//!    excluded by the sequence cap);
//! 4. the session stamped, claim cleared.
//!
//! These tests cover the pure entry mapping (no DB), the live
//! lazy-migration against the **real** Node harness child (faux
//! provider), and the claim's exactly-once guarantee under two
//! concurrent writes.

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

mod test_helpers;

use forge_api::bus::BusEvent;
use forge_api::db::Message;
use forge_api::harness_migration::messages_to_entries;
use forge_api::recording::ToolRecorder;

const TURN_WAIT: Duration = Duration::from_secs(90);
const HARNESS_BOOT_WAIT: Duration = Duration::from_secs(120);

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
            "name": "H26 Migrate User",
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
            "email": email,
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(login_resp.status(), 200, "{}", login_resp.text());
    let body: serde_json::Value = login_resp.json().await.unwrap();
    body["api_key"].as_str().unwrap().to_string()
}

/// Create the scratch schema + spawn the real harness child (faux
/// provider, `n` pre-queued answers). Registers `email` as the test
/// user and returns (child, pool, api_key, email).
async fn spawn_harness(
    app: &test_helpers::TestApp,
    db_url: &str,
    schema: &str,
    socket_dir: &std::path::Path,
    faux_responses: &str,
    email: &str,
) -> (std::process::Child, sqlx::PgPool, String, String) {
    let rpc_sock = socket_dir.join("harness.sock");
    let events_sock = socket_dir.join("harness-events.sock");

    let api_key = register_and_login(app, email).await;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(db_url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .unwrap();

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
        .env("FORGE_DATABASE_URL", db_url)
        .env("FORGE_API_URL", &app.base_url)
        .env("FORGE_API_KEY", &api_key)
        .env("FORGE_HARNESS_SOCKET", &rpc_sock)
        .env("FORGE_HARNESS_EVENTS_SOCKET", &events_sock)
        .env("FORGE_HARNESS_SCHEMA", schema)
        .env("FORGE_HARNESS_FAUX", "1")
        .env("FORGE_HARNESS_FAUX_RESPONSES", faux_responses)
        .spawn()
        .expect("spawn the harness child");
    eprintln!(
        "h26-migrate: harness child pid {:?} (schema {schema})",
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
    (child, pool, api_key, email.to_string())
}

/// Wait until `sessions.durable_conversation_id` is stamped.
async fn wait_for_stamp(pool: &sqlx::PgPool, session_id: uuid::Uuid) -> i64 {
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    loop {
        match sqlx::query_scalar::<_, i64>(
            "SELECT durable_conversation_id FROM sessions WHERE id = $1",
        )
        .bind(session_id)
        .fetch_optional(pool)
        .await
        {
            Ok(Some(id)) => return id,
            Ok(None) => {}
            Err(e) => panic!("stamp poll failed: {e}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the session's durable_conversation_id stamp"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait for a `turn_ended` bus event on the session.
async fn wait_for_turn_ended(
    bus_rx: &mut tokio::sync::broadcast::Receiver<BusEvent>,
    session_id: uuid::Uuid,
) {
    let deadline = tokio::time::Instant::now() + TURN_WAIT;
    loop {
        let ev = match tokio::time::timeout_at(deadline, bus_rx.recv()).await {
            Ok(Ok(ev)) => ev,
            Ok(Err(_)) => panic!("bus receiver lagged or closed"),
            Err(_) => panic!("timed out waiting for turn_ended on {session_id}"),
        };
        if matches!(ev, BusEvent::TurnEnded { session_id: s } if s == session_id) {
            return;
        }
    }
}

fn entry_count_sql(schema: &str) -> String {
    format!("SELECT COUNT(*) FROM \"{schema}\".durable_entries WHERE conversation_id = $1")
}

// ============================================================
// Pure entry-mapping tests (no DB, no harness)
// ============================================================

fn msg(
    sequence: i32,
    role: &str,
    content: &str,
    tool_call_id: Option<&str>,
    tool_name: Option<&str>,
    tool_input: Option<serde_json::Value>,
    tool_output: Option<serde_json::Value>,
) -> Message {
    Message {
        id: uuid::Uuid::new_v4(),
        session_id: uuid::Uuid::new_v4(),
        sequence,
        role: role.to_string(),
        content: Some(content.to_string()),
        tool_name: tool_name.map(str::to_string),
        tool_input,
        tool_call_id: tool_call_id.map(str::to_string),
        tool_output,
        duration_ms: None,
        created_at: chrono::DateTime::from_timestamp(sequence as i64, 0).expect("valid timestamp"),
    }
}

/// The row → entry mapping: user → `pi.user`; assistant text →
/// `pi.assistant` (text block); assistant tool call + tool result →
/// `pi.assistant` (toolCall block) immediately followed by
/// `pi.tool-result`; system + placeholder rows dropped.
#[test]
fn entry_mapping_roles_and_ordering() {
    let messages = vec![
        msg(1, "user", "run ls", None, None, None, None),
        msg(
            2,
            "assistant",
            "",
            Some("call_1"),
            Some("bash"),
            Some(json!({ "command": "ls" })),
            None,
        ),
        msg(
            3,
            "tool",
            "file.txt",
            Some("call_1"),
            Some("bash"),
            None,
            Some(json!({ "success": true, "exit_code": 0 })),
        ),
        msg(4, "assistant", "All done.", None, None, None, None),
        msg(5, "system", "session metadata", None, None, None, None),
        msg(
            6,
            "assistant",
            "[no response from agent]",
            None,
            None,
            None,
            None,
        ),
    ];

    let entries = messages_to_entries(&messages, "anthropic", "test-model");
    let kinds: Vec<&str> = entries
        .iter()
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        vec!["pi.user", "pi.assistant", "pi.tool-result", "pi.assistant"]
    );

    assert_eq!(entries[0]["model"][0]["content"], "run ls");
    let call = &entries[1]["model"][0]["content"][0];
    assert_eq!(call["type"], "toolCall");
    assert_eq!(call["id"], "call_1");
    assert_eq!(call["name"], "bash");
    assert_eq!(call["arguments"], json!({ "command": "ls" }));
    assert_eq!(entries[1]["model"][0]["stopReason"], "toolUse");
    let result = &entries[2]["model"][0];
    assert_eq!(result["role"], "toolResult");
    assert_eq!(result["toolCallId"], "call_1");
    assert_eq!(result["content"][0]["text"], "file.txt");
    assert_eq!(result["isError"], false);
    assert_eq!(entries[3]["model"][0]["content"][0]["text"], "All done.");
}

/// A tool result with `success: false` maps to `isError: true`; a
/// duplicate result for the same call keeps only the LAST row.
#[test]
fn entry_mapping_error_flag_and_duplicate_results() {
    let messages = vec![
        msg(
            1,
            "assistant",
            "",
            Some("call_e"),
            Some("bash"),
            Some(json!({})),
            None,
        ),
        msg(
            2,
            "tool",
            "first result",
            Some("call_e"),
            Some("bash"),
            None,
            Some(json!({ "success": true })),
        ),
        msg(
            3,
            "tool",
            "second (retry) result",
            Some("call_e"),
            Some("bash"),
            None,
            Some(json!({ "success": false })),
        ),
    ];

    let entries = messages_to_entries(&messages, "anthropic", "test-model");
    let results: Vec<&serde_json::Value> = entries
        .iter()
        .filter(|e| e["kind"] == "pi.tool-result")
        .collect();
    assert_eq!(results.len(), 1, "only the LAST result row is imported");
    assert_eq!(
        results[0]["model"][0]["content"][0]["text"],
        "second (retry) result"
    );
    assert_eq!(results[0]["model"][0]["isError"], true);
}

/// An orphaned tool result (call id with no matching assistant
/// toolCall row) is dropped, not imported.
#[test]
fn entry_mapping_orphaned_result_is_dropped() {
    let messages = vec![
        msg(1, "user", "hi", None, None, None, None),
        msg(
            2,
            "tool",
            "orphaned result",
            Some("call_nope"),
            Some("bash"),
            None,
            None,
        ),
    ];
    let entries = messages_to_entries(&messages, "anthropic", "test-model");
    assert_eq!(
        entries.len(),
        1,
        "only the user row survives, got {entries:?}"
    );
}

// ============================================================
// Live lazy-migration tests (real harness child, faux provider)
// ============================================================

/// Seed `content` directly as a `messages` row (simulating a
/// pre-cutover transcript written by the deleted legacy turn driver).
/// Returns the row's sequence.
async fn seed_message(
    pool: &sqlx::PgPool,
    session_id: uuid::Uuid,
    role: &str,
    content: &str,
) -> i32 {
    sqlx::query_scalar::<_, i32>(
        "INSERT INTO messages (session_id, sequence, role, content)
         VALUES ($1, get_next_sequence($1), $2, $3)
         RETURNING sequence",
    )
    .bind(session_id)
    .bind(role)
    .bind(content)
    .fetch_one(pool)
    .await
    .expect("seed legacy row")
}

/// Insert a pre-cutover session: a plain `sessions` row owned by the
/// test user, `durable_conversation_id IS NULL` — exactly what a
/// session that existed before the H2.6 cutover looks like (new
/// sessions are stamped at creation; only these migrate lazily).
async fn seed_legacy_session(
    pool: &sqlx::PgPool,
    profile_id: &str,
    user_email: &str,
    title: &str,
) -> uuid::Uuid {
    let user_id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
        .bind(user_email)
        .fetch_one(pool)
        .await
        .expect("test user exists");
    let profile_id: uuid::Uuid = profile_id.parse().expect("profile id");
    let session_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sessions (id, profile_id, title, user_id)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(session_id)
    .bind(profile_id)
    .bind(title)
    .bind(user_id)
    .execute(pool)
    .await
    .expect("seed legacy session row");
    session_id
}

/// An unstamped session with a pre-cutover transcript migrates on its
/// FIRST write: the transcript is imported, the new prompt is
/// submitted (not re-imported), the session is stamped, and the turn
/// completes on the harness with the imported context.
#[tokio::test]
async fn unmigrated_session_migrates_on_first_write() {
    let schema = format!("forge_h26_{}", uuid::Uuid::new_v4().simple());
    let socket_dir = tempfile::tempdir().expect("socket tempdir");
    let harness = forge_api::harness::HarnessState::from_paths_with(
        &socket_dir.path().join("harness.sock"),
        &socket_dir.path().join("harness-events.sock"),
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    let (app, db_url) = test_helpers::TestApp::with_harness(harness, true).await;
    let consumer_handle = forge_api::harness::spawn_event_consumer(app.app_state.clone());
    assert!(consumer_handle.is_some(), "event consumer must spawn");

    let mut bus_rx = app.app_state.bus.subscribe();
    let email = format!("h26-migrate-{}@example.com", uuid::Uuid::new_v4());
    let (mut child, pool, api_key, email) = spawn_harness(
        &app,
        &db_url,
        &schema,
        socket_dir.path(),
        r#"["Paris."]"#,
        &email,
    )
    .await;

    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "H26 Faux Profile",
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

    // A pre-cutover session: plain row, no durable stamp.
    let session_id =
        seed_legacy_session(&pool, &profile_id, &email, "H26 pre-cutover session").await;

    // The pre-cutover transcript (5 rows: user, assistant text, user,
    // assistant tool call, tool result).
    let recorder = std::sync::Arc::new(forge_api::recording::DbToolRecorder::new(pool.clone()));
    seed_message(&pool, session_id, "user", "seed prompt one").await;
    seed_message(&pool, session_id, "assistant", "seed answer one").await;
    seed_message(&pool, session_id, "user", "seed prompt two").await;
    let call = recorder
        .record_call(forge_api::recording::ToolCallRecord {
            session_id,
            tool_call_id: "call_seed".into(),
            tool_name: "bash".into(),
            input: json!({ "command": "echo seed" }),
        })
        .await
        .expect("record seed tool call");
    let result = recorder
        .record_result(forge_api::recording::ToolResultRecord {
            session_id,
            tool_call_id: "call_seed".into(),
            tool_name: "bash".into(),
            content: "seed".into(),
            output: json!({ "success": true, "exit_code": 0 }),
            is_error: false,
            duration_ms: None,
        })
        .await
        .expect("record seed tool result");
    let _ = (call, result);

    // First write: the migration + the prompt submit, in one request.
    let msg_resp = app
        .post("/messages")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "session_id": session_id.to_string(),
            "content": "What is the capital of France?"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(msg_resp.status(), 202, "{}", msg_resp.text());

    let conversation_id = wait_for_stamp(&pool, session_id).await;
    eprintln!("h26-migrate: session {session_id} stamped durable {conversation_id}");

    // The claim is cleared.
    let claim: (bool,) = sqlx::query_as("SELECT harness_migrating FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        !claim.0,
        "the migration claim must be cleared after the stamp"
    );

    // The imported transcript: the 5 legacy rows are in the durable
    // entries, each exactly once. (The prompt's own pi.user entry
    // commits atomically with the 202 `submit`, so a raw total count
    // is racy — count by content instead.)
    let legacy_count: i64 = sqlx::query_scalar(
        &format!(
            r#"SELECT COUNT(*) FROM "{schema}".durable_entries
                WHERE conversation_id = $1 AND (
                    ((record::jsonb)->>'kind' = 'pi.user'
                      AND (record::jsonb)->'model'->0->>'content' IN ('seed prompt one', 'seed prompt two'))
                    OR ((record::jsonb)->>'kind' = 'pi.assistant'
                      AND ((record::jsonb)->'model'->0->'content'->0->>'text' = 'seed answer one'
                           OR (record::jsonb)->'model'->0->'content'->0->>'id' = 'call_seed'))
                    OR ((record::jsonb)->>'kind' = 'pi.tool-result'
                      AND (record::jsonb)->'model'->0->>'toolCallId' = 'call_seed')
                )"#
        ),
    )
    .bind(conversation_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        legacy_count, 5,
        "all 5 legacy rows must be imported exactly once"
    );

    // The imported user entries carry the legacy prompts VERBATIM —
    // and the just-sent prompt appears exactly once (committed by the
    // `submit` atomically with the 202, NOT imported by the
    // migration: the sequence cap excluded it). Order: the import
    // commit lands before the submit's.
    let user_entries: Vec<String> = sqlx::query_scalar(&format!(
        r#"SELECT (record::jsonb)->'model'->0->>'content' FROM "{schema}".durable_entries
                WHERE conversation_id = $1 AND (record::jsonb)->>'kind' = 'pi.user' ORDER BY id"#
    ))
    .bind(conversation_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        user_entries,
        vec![
            "seed prompt one".to_string(),
            "seed prompt two".to_string(),
            "What is the capital of France?".to_string(),
        ],
        "the import must be the pre-write transcript; the new prompt rides on the submit exactly once"
    );

    // The turn completes on the harness with the imported context.
    wait_for_turn_ended(&mut bus_rx, session_id).await;
    let final_count: i64 = sqlx::query_scalar(&entry_count_sql(&schema))
        .bind(conversation_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let entry_dump: Vec<String> = sqlx::query_scalar(&format!(
        r#"SELECT record::text FROM "{schema}".durable_entries
                WHERE conversation_id = $1 ORDER BY id"#
    ))
    .bind(conversation_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        final_count,
        8,
        "5 imported + the prompt's pi.user + the harness's pi.system context entry + the answer's pi.assistant (entries: {entry_dump:?})"
    );
    let assistant: String = sqlx::query_scalar(
        "SELECT content FROM messages WHERE session_id = $1 AND role = 'assistant'
          AND content <> '' AND content <> 'seed answer one'
          ORDER BY sequence DESC LIMIT 1",
    )
    .bind(session_id)
    .fetch_one(&pool)
    .await
    .expect("the projected assistant answer");
    assert_eq!(assistant, "Paris.");

    child.kill().ok();
    drop(consumer_handle);
}

/// Two CONCURRENT writes to an unstamped session run the migration
/// exactly once: both requests land (202), the session gets exactly
/// one durable conversation, and the transcript is imported exactly
/// once.
#[tokio::test]
async fn concurrent_writes_migrate_exactly_once() {
    let schema = format!("forge_h26c_{}", uuid::Uuid::new_v4().simple());
    let socket_dir = tempfile::tempdir().expect("socket tempdir");
    let harness = forge_api::harness::HarnessState::from_paths_with(
        &socket_dir.path().join("harness.sock"),
        &socket_dir.path().join("harness-events.sock"),
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    let (app, db_url) = test_helpers::TestApp::with_harness(harness, true).await;
    let consumer_handle = forge_api::harness::spawn_event_consumer(app.app_state.clone());
    assert!(consumer_handle.is_some(), "event consumer must spawn");

    let mut bus_rx = app.app_state.bus.subscribe();
    let email = format!("h26-migratec-{}@example.com", uuid::Uuid::new_v4());
    let (mut child, pool, api_key, email) = spawn_harness(
        &app,
        &db_url,
        &schema,
        socket_dir.path(),
        r#"["answer A.", "answer B."]"#,
        &email,
    )
    .await;

    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "H26c Faux Profile",
            "provider": "faux",
            "model": "faux-1",
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

    let session_id =
        seed_legacy_session(&pool, &profile_id, &email, "H26 concurrent session").await;
    seed_message(&pool, session_id, "user", "concurrent seed prompt").await;

    // Two writes racing each other — the claim's atomic UPDATE runs
    // the migration exactly once; the loser polls the stamp.
    let app_a = &app;
    let app_b = &app;
    let key_a = api_key.clone();
    let key_b = api_key.clone();
    let sid_a = session_id.to_string();
    let sid_b = session_id.to_string();
    let (res_a, res_b) = tokio::join!(
        async move {
            app_a
                .post("/messages")
                .header("X-API-Key", &key_a)
                .json(&json!({ "session_id": sid_a, "content": "prompt A" }))
                .send()
                .await
                .unwrap()
        },
        async move {
            app_b
                .post("/messages")
                .header("X-API-Key", &key_b)
                .json(&json!({ "session_id": sid_b, "content": "prompt B" }))
                .send()
                .await
                .unwrap()
        }
    );
    assert_eq!(res_a.status(), 202, "{}", res_a.text());
    assert_eq!(res_b.status(), 202, "{}", res_b.text());

    let conversation_id = wait_for_stamp(&pool, session_id).await;

    // Exactly one durable conversation owns this session.
    let sessions_with_stamp: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sessions WHERE id = $1 AND durable_conversation_id IS NOT NULL",
    )
    .bind(session_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sessions_with_stamp, 1);

    // The seed prompt appears in the durable entries exactly ONCE.
    let seed_count: i64 = sqlx::query_scalar(&format!(
        r#"SELECT COUNT(*) FROM "{schema}".durable_entries
                WHERE conversation_id = $1 AND (record::jsonb)->>'kind' = 'pi.user'
                  AND (record::jsonb)->'model'->0->>'content' = 'concurrent seed prompt'"#
    ))
    .bind(conversation_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(seed_count, 1, "the transcript import must run exactly once");

    // Both turns complete; both answers are projected; the user
    // prompts each appear in the durable transcript exactly once.
    wait_for_turn_ended(&mut bus_rx, session_id).await;
    wait_for_turn_ended(&mut bus_rx, session_id).await;
    let prompt_counts: i64 = sqlx::query_scalar(&format!(
        r#"SELECT COUNT(*) FROM "{schema}".durable_entries
                WHERE conversation_id = $1 AND (record::jsonb)->>'kind' = 'pi.user'
                  AND (record::jsonb)->'model'->0->>'content' IN ('prompt A', 'prompt B')"#
    ))
    .bind(conversation_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let all_user_entries: Vec<String> = sqlx::query_scalar(&format!(
        r#"SELECT (record::jsonb)->'model'->0->>'content' FROM "{schema}".durable_entries
                WHERE conversation_id = $1 AND (record::jsonb)->>'kind' = 'pi.user' ORDER BY id"#
    ))
    .bind(conversation_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let submissions_dump: Vec<String> = sqlx::query_scalar(&format!(
        r#"SELECT record FROM "{schema}".durable_submissions
                WHERE conversation_id = $1 ORDER BY id"#
    ))
    .bind(conversation_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    eprintln!(
        "h26-migrate: pi.user entries: {all_user_entries:?} | submissions: {submissions_dump:?}"
    );
    assert_eq!(
        prompt_counts, 2,
        "one pi.user entry per submitted prompt (entries: {all_user_entries:?})"
    );

    child.kill().ok();
    drop(consumer_handle);
}
