//! P2-41: Postgres round-trip tests for `DbToolRecorder`.
//!
//! Verifies against a real database that:
//! - concurrent `record_call` invocations for the same session
//!   produce unique, contiguous sequence numbers;
//! - a retried `record_call` / `record_result` with the same
//!   `(session_id, tool_call_id, role)` does not double-write and
//!   returns the existing row.

mod test_helpers;

use forge_api::recording::{DbToolRecorder, ToolCallRecord, ToolRecorder, ToolResultRecord};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

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
         VALUES ($1, 'anthropic', 'test-model', '/tmp/recorder-test', '', '[]')
         RETURNING id",
    )
    .bind(format!("recorder-test-{}", Uuid::new_v4().simple()))
    .fetch_one(&pool)
    .await
    .expect("seed profile");

    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO sessions (profile_id, title) VALUES ($1, 'recorder test') RETURNING id",
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

fn call_record(session_id: Uuid, id: &str) -> ToolCallRecord {
    ToolCallRecord {
        session_id,
        tool_call_id: id.to_string(),
        tool_name: "bash".to_string(),
        input: serde_json::json!({"command": "echo hi"}),
    }
}

fn result_record(session_id: Uuid, id: &str, content: &str) -> ToolResultRecord {
    ToolResultRecord {
        session_id,
        tool_call_id: id.to_string(),
        tool_name: "bash".to_string(),
        content: content.to_string(),
        output: serde_json::json!({"exit_code": 0, "success": true}),
        is_error: false,
        duration_ms: Some(42),
    }
}

/// Concurrent calls with distinct ids must produce unique and
/// contiguous sequences (advisory lock in `get_next_sequence`).
#[tokio::test]
async fn concurrent_call_records_get_unique_contiguous_sequences() {
    let (pool, session_id, db_url) = setup().await;

    const N: usize = 10;
    let handles: Vec<_> = (0..N)
        .map(|i| {
            let recorder = DbToolRecorder::new(pool.clone());
            let r = call_record(session_id, &format!("call-{i}"));
            tokio::spawn(async move { recorder.record_call(r).await })
        })
        .collect();
    let results = futures_util::future::join_all(handles).await;
    for r in &results {
        assert!(r.as_ref().is_ok(), "record_call should succeed: {:?}", r);
    }

    let sequences: Vec<i32> = sqlx::query_scalar(
        "SELECT sequence FROM messages
         WHERE session_id = $1 AND role = 'assistant' ORDER BY sequence",
    )
    .bind(session_id)
    .fetch_all(&pool)
    .await
    .expect("fetch sequences");
    assert_eq!(sequences.len(), N, "one row per call");
    for w in sequences.windows(2) {
        assert!(
            w[1] > w[0],
            "sequences must be strictly increasing: {sequences:?}"
        );
    }
    assert_eq!(
        sequences[0], 1,
        "first row in a fresh session must be sequence 1: {sequences:?}"
    );
    assert_eq!(
        sequences.last().copied(),
        Some(N as i32),
        "sequences must be contiguous (no gaps): {sequences:?}"
    );

    teardown(&pool, &db_url).await;
}

/// Retrying a call with the same (session_id, tool_call_id) must not
/// insert a second row and must return the existing row.
#[tokio::test]
async fn duplicate_call_record_is_idempotent() {
    let (pool, session_id, db_url) = setup().await;
    let recorder = DbToolRecorder::new(pool.clone());

    let r1 = recorder
        .record_call(call_record(session_id, "call-dup"))
        .await
        .expect("first record_call");
    let r2 = recorder
        .record_call(call_record(session_id, "call-dup"))
        .await
        .expect("retry record_call");

    assert_eq!(
        r1.id, r2.id,
        "retry must return the existing row, not insert a new one"
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages
         WHERE session_id = $1 AND tool_call_id = $2 AND role = 'assistant'",
    )
    .bind(session_id)
    .bind("call-dup")
    .fetch_one(&pool)
    .await
    .expect("count rows");
    assert_eq!(count, 1, "duplicate call must not be recorded twice");

    teardown(&pool, &db_url).await;
}

/// The same guarantee holds for result rows, and the existing row's
/// content is preserved.
#[tokio::test]
async fn duplicate_result_record_is_idempotent() {
    let (pool, session_id, db_url) = setup().await;
    let recorder = DbToolRecorder::new(pool.clone());

    let r1 = recorder
        .record_result(result_record(session_id, "call-dup", "first result"))
        .await
        .expect("first record_result");
    let r2 = recorder
        .record_result(result_record(session_id, "call-dup", "retried result"))
        .await
        .expect("retry record_result");

    assert_eq!(r1.id, r2.id, "retry must return the existing row");

    let content: Option<String> = sqlx::query_scalar(
        "SELECT content FROM messages
         WHERE session_id = $1 AND tool_call_id = $2 AND role = 'tool'",
    )
    .bind(session_id)
    .bind("call-dup")
    .fetch_one(&pool)
    .await
    .expect("fetch content");
    assert_eq!(
        content.as_deref(),
        Some("first result"),
        "existing result row content must be preserved, not overwritten"
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages
         WHERE session_id = $1 AND tool_call_id = $2 AND role = 'tool'",
    )
    .bind(session_id)
    .bind("call-dup")
    .fetch_one(&pool)
    .await
    .expect("count rows");
    assert_eq!(count, 1, "duplicate result must not be recorded twice");

    teardown(&pool, &db_url).await;
}

/// Two concurrent retries of the *same* call must still yield a
/// single row (the partial unique index rejects the loser).
#[tokio::test]
async fn concurrent_duplicate_call_records_yield_one_row() {
    let (pool, session_id, db_url) = setup().await;
    let recorder = DbToolRecorder::new(pool.clone());

    let (r1, r2) = tokio::join!(
        recorder.record_call(call_record(session_id, "call-race")),
        recorder.record_call(call_record(session_id, "call-race"))
    );
    assert!(r1.is_ok(), "first concurrent call: {:?}", r1);
    assert!(r2.is_ok(), "second concurrent call: {:?}", r2);
    assert_eq!(
        r1.unwrap().id,
        r2.unwrap().id,
        "both concurrent retries must return the same row"
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages
         WHERE session_id = $1 AND tool_call_id = $2 AND role = 'assistant'",
    )
    .bind(session_id)
    .bind("call-race")
    .fetch_one(&pool)
    .await
    .expect("count rows");
    assert_eq!(count, 1, "race must produce exactly one row");

    teardown(&pool, &db_url).await;
}
