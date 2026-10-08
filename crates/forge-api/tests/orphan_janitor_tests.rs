//! P2-33: orphaned tool-call janitor tests (post-H2.6).
//!
//! When a session dies mid-tool (harness crash, API restart, user
//! disconnect between the executor's call-row write and result-row
//! write), the audit log has a `role='assistant'` tool-call row with
//! no matching `role='tool'` result row. Imported into a durable
//! conversation, that produces a `toolCall` block with no
//! `toolResult`, which the model API rejects with "tool_use without a
//! matching tool_result".
//!
//! `abandon_orphan_calls` (Herd H2.6: moved from the now-deleted
//! `session_replay.rs` into `harness_migration.rs`) heals those
//! orphans in-place with synthetic "abandoned" result rows; the
//! lazy-migration import runs it first, and
//! `messages_to_entries` guarantees the imported context keeps every
//! `toolCall` immediately adjacent to a `toolResult`.
//!
//! These tests verify, against a real Postgres:
//! - an orphan call row is detected and a matching tool row is
//!   inserted (correct content / tool_output / linkage);
//! - the janitor is idempotent (second run heals nothing);
//! - sessions without orphans are untouched;
//! - the entry mapping (`messages_to_entries`) emits a tool-result
//!   entry for every tool-call entry, including the janitor's
//!   synthetic rows allocated AFTER the import cap.

use forge_api::harness_migration::{abandon_orphan_calls, messages_to_entries};
use forge_api::recording::{DbToolRecorder, ToolCallRecord, ToolRecorder};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

mod test_helpers;

const ADMIN_URL: &str = "postgres://postgres:forge@localhost/postgres";

/// Create a fresh test database, run migrations, and seed one
/// profile + session. Returns (pool, session_id, db_url).
async fn setup() -> (PgPool, Uuid, String) {
    let db_name = format!("forge_test_{}", Uuid::new_v4().simple());
    let db_url = format!("postgres://postgres:forge@localhost/{db_name}");

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .expect("connect admin db");
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin)
        .await
        .expect("create test db");
    admin.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("connect test db");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    let profile_id: Uuid = sqlx::query_scalar(
        "INSERT INTO profiles (name, provider, model, working_dir, system_prompt, tools)
         VALUES ($1, 'anthropic', 'test-model', '/tmp/orphan-test', '', '[]')
         RETURNING id",
    )
    .bind(format!("orphan-test-{}", Uuid::new_v4().simple()))
    .fetch_one(&pool)
    .await
    .expect("seed profile");

    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO sessions (profile_id, title) VALUES ($1, 'orphan test') RETURNING id",
    )
    .bind(profile_id)
    .fetch_one(&pool)
    .await
    .expect("seed session");

    (pool, session_id, db_url)
}

/// Drop the per-test database (shared helper from test_helpers).
async fn teardown(pool: &PgPool, db_url: &str) {
    let db_name = db_url
        .rsplit('/')
        .next()
        .and_then(|s| s.split('?').next())
        .unwrap_or("forge_test")
        .to_string();
    pool.close().await;
    test_helpers::drop_test_db(ADMIN_URL, &db_name).await;
}

/// Insert a user prompt row directly (the harness owns user rows;
/// the recorder only handles tool rows).
async fn insert_user(pool: &PgPool, session_id: Uuid, content: &str) -> i32 {
    sqlx::query_scalar::<_, i32>(
        "INSERT INTO messages (session_id, sequence, role, content)
         VALUES ($1, get_next_sequence($1), 'user', $2)
         RETURNING sequence",
    )
    .bind(session_id)
    .bind(content)
    .fetch_one(pool)
    .await
    .expect("insert user row")
}

fn call_record(session_id: Uuid, id: &str, tool: &str) -> ToolCallRecord {
    ToolCallRecord {
        session_id,
        tool_call_id: id.to_string(),
        tool_name: tool.to_string(),
        input: serde_json::json!({"command": "echo hi"}),
    }
}

/// A session with one user prompt and one orphaned tool call (the
/// session "died" between the call-row write and the result-row
/// write).
async fn seed_orphan(pool: &PgPool, session_id: Uuid, call_id: &str) -> i32 {
    insert_user(pool, session_id, "run something").await;
    let recorder = DbToolRecorder::new(pool.clone());
    let call = recorder
        .record_call(call_record(session_id, call_id, "bash"))
        .await
        .expect("record call");
    call.sequence
}

/// An orphan call row must produce exactly one matching tool row with
/// the synthetic abandoned content.
#[tokio::test]
async fn orphan_call_is_healed_with_matching_tool_row() {
    let (pool, session_id, db_url) = setup().await;

    seed_orphan(&pool, session_id, "call_orphan_1").await;

    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1")
        .bind(session_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    let healed = abandon_orphan_calls(&pool, session_id)
        .await
        .expect("janitor runs");

    assert_eq!(healed.len(), 1, "exactly one orphan call should be healed");
    let row = &healed[0];
    assert_eq!(row.role, "tool");
    assert_eq!(row.tool_call_id.as_deref(), Some("call_orphan_1"));
    assert_eq!(row.tool_name.as_deref(), Some("bash"));
    assert_eq!(
        row.content.as_deref(),
        Some("[abandoned: no result recorded]")
    );
    let out = row.tool_output.as_ref().expect("tool_output jsonb");
    assert_eq!(out.get("success"), Some(&serde_json::json!(false)));
    assert!(
        out.get("error").is_some(),
        "tool_output should carry the error marker, got {out}"
    );

    // The healed row is durable in the DB (not just returned).
    let db_row: (i32, String) = sqlx::query_as(
        "SELECT sequence, content FROM messages
          WHERE session_id = $1 AND role = 'tool' AND tool_call_id = 'call_orphan_1'",
    )
    .bind(session_id)
    .fetch_one(&pool)
    .await
    .expect("healed tool row in DB");
    assert_eq!(db_row.1, "[abandoned: no result recorded]");

    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1")
        .bind(session_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(after, before + 1, "exactly one row added");

    teardown(&pool, &db_url).await;
}

/// Running the janitor twice heals the orphan only once; the second
/// run finds nothing (the idempotent `record_result` ON CONFLICT
/// would also protect us, but the NOT EXISTS query should simply
/// find no orphans).
#[tokio::test]
async fn janitor_is_idempotent() {
    let (pool, session_id, db_url) = setup().await;

    seed_orphan(&pool, session_id, "call_idem").await;

    let first = abandon_orphan_calls(&pool, session_id)
        .await
        .expect("first run");
    assert_eq!(first.len(), 1);

    let second = abandon_orphan_calls(&pool, session_id)
        .await
        .expect("second run");
    assert!(
        second.is_empty(),
        "second run should find no orphans, got {second:?}"
    );

    let tool_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages
          WHERE session_id = $1 AND role = 'tool' AND tool_call_id = 'call_idem'",
    )
    .bind(session_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tool_rows, 1, "still exactly one result row for the call");

    teardown(&pool, &db_url).await;
}

/// A session with no tool calls at all is untouched by the janitor.
#[tokio::test]
async fn session_without_orphans_is_untouched() {
    let (pool, session_id, db_url) = setup().await;

    insert_user(&pool, session_id, "hello").await;
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1")
        .bind(session_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    let healed = abandon_orphan_calls(&pool, session_id)
        .await
        .expect("janitor runs");
    assert!(healed.is_empty(), "nothing to heal");

    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = $1")
        .bind(session_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(before, after, "no rows added");

    teardown(&pool, &db_url).await;
}

/// A call row that DOES have a result row is not orphaned and must
/// not get a second synthetic row.
#[tokio::test]
async fn matched_call_is_not_orphaned() {
    let (pool, session_id, db_url) = setup().await;

    insert_user(&pool, session_id, "run something").await;
    let recorder = DbToolRecorder::new(pool.clone());
    recorder
        .record_call(call_record(session_id, "call_matched", "bash"))
        .await
        .expect("record call");
    recorder
        .record_result(forge_api::recording::ToolResultRecord {
            session_id,
            tool_call_id: "call_matched".to_string(),
            tool_name: "bash".to_string(),
            content: "hi".to_string(),
            output: serde_json::json!({"exit_code": 0, "success": true}),
            is_error: false,
            duration_ms: Some(5),
        })
        .await
        .expect("record result");

    let healed = abandon_orphan_calls(&pool, session_id)
        .await
        .expect("janitor runs");
    assert!(
        healed.is_empty(),
        "a call with a result is not an orphan, got {healed:?}"
    );

    teardown(&pool, &db_url).await;
}

/// The actual bug being fixed, in the entry world: the imported
/// entries must contain a `pi.tool-result` for every tool-call entry.
/// Before the janitor, an orphan call would import a `toolCall`
/// block with no matching result, which the model API rejects.
#[tokio::test]
async fn entries_have_a_result_for_every_call() {
    let (pool, session_id, db_url) = setup().await;

    seed_orphan(&pool, session_id, "call_entries").await;
    abandon_orphan_calls(&pool, session_id)
        .await
        .expect("janitor runs");

    let messages: Vec<forge_api::db::Message> =
        sqlx::query_as("SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC")
            .bind(session_id)
            .fetch_all(&pool)
            .await
            .expect("fetch rows");

    let entries = messages_to_entries(&messages, "anthropic", "test-model");
    let kinds: Vec<&str> = entries
        .iter()
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    // user, call, result (the call is immediately followed by its
    // result — the ordering guarantee).
    assert_eq!(
        kinds,
        vec!["pi.user", "pi.assistant", "pi.tool-result"],
        "each toolCall entry must be followed by its tool-result entry; got {kinds:?}"
    );
    let call = &entries[1];
    let result = &entries[2];
    let call_id = call["model"][0]["content"][0]["id"].as_str().unwrap();
    assert_eq!(result["model"][0]["toolCallId"].as_str(), Some(call_id));
    let result_text = result["model"][0]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(result_text, "[abandoned: no result recorded]");

    teardown(&pool, &db_url).await;
}

/// Capped import (the `dispatch_message` path): the transcript fetch
/// is capped at `sequence < caller-row` to exclude the just-arrived
/// prompt, but the janitor's synthetic rows are allocated *after*
/// the cap and must still be included for a call inside the cap —
/// otherwise the import would ship an orphan.
#[tokio::test]
async fn capped_entries_still_include_healed_result() {
    let (pool, session_id, db_url) = setup().await;

    // user prompt, orphan call, then the user's new prompt (which the
    // cap will exclude, exactly like the dispatch path does).
    insert_user(&pool, session_id, "first prompt").await;
    let call_seq = seed_orphan(&pool, session_id, "call_capped").await;
    let new_prompt_seq = insert_user(&pool, session_id, "second prompt").await;
    assert!(call_seq < new_prompt_seq);

    // Heal first (the import runs the janitor before the fetch).
    let healed = abandon_orphan_calls(&pool, session_id)
        .await
        .expect("janitor runs");
    assert_eq!(healed.len(), 1);
    assert!(
        healed[0].sequence > new_prompt_seq,
        "the synthetic row is allocated after the cap; the import must append it explicitly"
    );

    // Cap at the new user prompt; append the synthetic rows
    // (exactly `import_legacy_messages` does).
    let mut messages: Vec<forge_api::db::Message> = sqlx::query_as(
        "SELECT * FROM messages WHERE session_id = $1 AND sequence < $2 ORDER BY sequence ASC",
    )
    .bind(session_id)
    .bind(new_prompt_seq)
    .fetch_all(&pool)
    .await
    .expect("capped fetch");
    messages.extend(healed);

    let entries = messages_to_entries(&messages, "anthropic", "test-model");

    let user_texts: Vec<String> = entries
        .iter()
        .filter(|e| e["kind"] == "pi.user")
        .map(|e| e["model"][0]["content"].as_str().unwrap_or("").to_string())
        .collect();
    let calls: Vec<&str> = entries
        .iter()
        .filter(|e| e["kind"] == "pi.assistant")
        .filter_map(|e| {
            e["model"][0]["content"][0]
                .get("id")
                .and_then(|v| v.as_str())
        })
        .collect();
    let results: Vec<&str> = entries
        .iter()
        .filter(|e| e["kind"] == "pi.tool-result")
        .map(|e| e["model"][0]["toolCallId"].as_str().unwrap_or(""))
        .collect();

    assert!(
        calls.contains(&"call_capped"),
        "the call inside the cap must be in the import, got {calls:?}"
    );
    assert!(
        results.contains(&"call_capped"),
        "the healed synthetic result must survive the cap, got {results:?}"
    );
    assert!(
        !user_texts.iter().any(|t| t == "second prompt"),
        "the just-arrived user prompt must stay excluded by the cap, got {user_texts:?}"
    );

    teardown(&pool, &db_url).await;
}
