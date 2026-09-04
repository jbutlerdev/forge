//! Unit tests for the agent-turn driver (`api::turn::drive_turn_with`).
//!
//! Instead of spawning a live `pi` process, these tests drive the turn
//! loop with a scripted fake [`PiEventSource`]. Each test gets a fresh
//! Postgres database (migrations applied, one profile + session row) so
//! we can assert on the assistant rows the loop flushes to the audit
//! log.
//!
//! Covers: normal turn, `Response { success: false }` fast-fail,
//! `error` event, EOF (pi died), stale pre-turn events, the
//! `[no response from agent]` placeholder, and the toolcall-start
//! chunk-flush boundary.
//!
//! NOT covered: the read-timeout branch (it would require a
//! multi-minute stall to trigger; covered e2e by the spawn tests).

mod test_helpers;

use std::collections::VecDeque;

use forge_api::api::turn::{drive_turn_with, PiEventSource, TurnEndReason};
use forge_api::bus::MessageBus;
use forge_api::observability::Metrics;
use forge_api::pi_agent::PiError;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::mpsc;
use uuid::Uuid;

// ============================================
// Fake event source
// ============================================

/// One line of scripted pi stdout.
enum ScriptLine {
    /// A JSONL line to hand back from `read_line`.
    Line(String),
    /// EOF — pi died.
    Eof,
    /// An IO/transport error from `read_line`.
    Err(String),
}

/// A scripted fake [`PiEventSource`].
///
/// `lines` is consumed one per `read_line` call; when exhausted the
/// behavior is whatever the last element was (tests push an explicit
/// `Eof` when they want a clean EOF, or leave the queue empty to
/// simulate immediate death). Method counters let tests assert what
/// the driver did (and *didn't* do) — e.g. that a fast-failed turn
/// never read the rest of the script.
struct ScriptedSource {
    lines: VecDeque<ScriptLine>,
    sent: Vec<String>,
    drained: u32,
    compacted: u32,
    killed: u32,
}

impl ScriptedSource {
    fn new(lines: Vec<ScriptLine>) -> Self {
        Self {
            lines: lines.into(),
            sent: Vec::new(),
            drained: 0,
            compacted: 0,
            killed: 0,
        }
    }

    fn from_jsonl(lines: &[&str]) -> Self {
        Self::new(
            lines
                .iter()
                .map(|l| ScriptLine::Line(l.to_string()))
                .collect(),
        )
    }

    fn remaining(&self) -> usize {
        self.lines.len()
    }
}

#[async_trait::async_trait]
impl PiEventSource for ScriptedSource {
    async fn read_line(&mut self) -> Result<Option<String>, PiError> {
        match self.lines.pop_front() {
            Some(ScriptLine::Line(l)) => Ok(Some(l)),
            Some(ScriptLine::Eof) => Ok(None),
            Some(ScriptLine::Err(msg)) => Err(PiError::Io(msg)),
            None => Ok(None),
        }
    }

    async fn drain_pending_events(&mut self) {
        self.drained += 1;
    }

    async fn send_message(&mut self, text: &str) -> Result<(), PiError> {
        self.sent.push(text.to_string());
        Ok(())
    }

    async fn compact(
        &mut self,
        _custom_instructions: Option<&str>,
    ) -> Result<serde_json::Value, PiError> {
        self.compacted += 1;
        Ok(serde_json::json!({ "data": { "tokensBefore": 123 } }))
    }

    async fn kill(&mut self) -> Result<(), PiError> {
        self.killed += 1;
        Ok(())
    }
}

// ============================================
// Test database setup
// ============================================

/// Per-test database: fresh Postgres DB with migrations, one profile,
/// one session. Returns the pool + session id + db name (for teardown).
async fn setup_db() -> (PgPool, Uuid, String) {
    let db_name = format!("forge_turn_test_{}", Uuid::new_v4().simple());
    let admin_url = "postgres://postgres:forge@localhost/postgres";
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
        .expect("connect to admin db");
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin)
        .await
        .expect("create test database");
    admin.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!("postgres://postgres:forge@localhost/{db_name}"))
        .await
        .expect("connect to test db");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    let profile: Uuid = sqlx::query_scalar(
        "INSERT INTO profiles (name, provider, model, working_dir)
         VALUES ($1, 'anthropic', 'claude-sonnet-4-20250514', '/tmp')
         RETURNING id",
    )
    .bind(format!("turn-test-{db_name}"))
    .fetch_one(&pool)
    .await
    .expect("insert profile");
    let session: Uuid = sqlx::query_scalar(
        "INSERT INTO sessions (profile_id, title) VALUES ($1, 'turn test') RETURNING id",
    )
    .bind(profile)
    .fetch_one(&pool)
    .await
    .expect("insert session");

    (pool, session, db_name)
}

async fn teardown(pool: &PgPool, db_name: &str) {
    pool.close().await;
    test_helpers::drop_test_db("postgres://postgres:forge@localhost/postgres", db_name).await;
}

/// Run the turn driver against a scripted source with no compaction
/// prelude and no delta channel.
async fn run_turn(
    pool: &PgPool,
    session: Uuid,
    source: &mut ScriptedSource,
) -> forge_api::api::turn::TurnOutcome {
    let bus = MessageBus::new();
    let metrics = Metrics::new();
    drive_turn_with(
        pool,
        &bus,
        &metrics,
        session,
        source,
        "hello from test",
        None,
        false,
    )
    .await
}

/// The assistant rows flushed to the audit log, in sequence order.
async fn assistant_rows(pool: &PgPool, session: Uuid) -> Vec<(i32, String)> {
    sqlx::query_as::<_, (i32, String)>(
        "SELECT sequence, content FROM messages
         WHERE session_id = $1 AND role = 'assistant'
         ORDER BY sequence ASC",
    )
    .bind(session)
    .fetch_all(pool)
    .await
    .expect("query assistant rows")
}

// ============================================
// Tests
// ============================================

/// 1. Normal turn: `turn_start`, two text deltas, `agent_end` →
/// `AgentEnd` with concatenated text and one assistant row.
#[tokio::test]
async fn test_normal_turn_concatenates_text_and_flushes_one_row() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::from_jsonl(&[
        r#"{"type":"turn_start"}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Hello "}}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"World"}}"#,
        r#"{"type":"agent_end"}"#,
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(
        matches!(outcome.reason, TurnEndReason::AgentEnd),
        "expected AgentEnd, got {:?}",
        outcome.reason
    );
    assert_eq!(outcome.text, "Hello World");
    assert_eq!(source.sent, vec!["hello from test".to_string()]);
    assert!(source.drained >= 1, "driver must drain pre-turn stragglers");
    assert_eq!(source.killed, 0, "no kill on clean completion");

    let rows = assistant_rows(&pool, session).await;
    assert_eq!(
        rows,
        vec![(1, "Hello World".to_string())],
        "one flushed row"
    );

    teardown(&pool, &db).await;
}

/// 1b. Streaming deltas are forwarded best-effort to `delta_tx`.
#[tokio::test]
async fn test_normal_turn_forwards_deltas_to_tx() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::from_jsonl(&[
        r#"{"type":"turn_start"}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"a"}}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"b"}}"#,
        r#"{"type":"agent_end"}"#,
    ]);

    let bus = MessageBus::new();
    let metrics = Metrics::new();
    let (tx, mut rx) = mpsc::channel::<String>(8);
    let outcome = drive_turn_with(
        &pool,
        &bus,
        &metrics,
        session,
        &mut source,
        "hi",
        Some(tx),
        false,
    )
    .await;

    assert!(matches!(outcome.reason, TurnEndReason::AgentEnd));
    let d1 = rx.try_recv().expect("first delta");
    let d2 = rx.try_recv().expect("second delta");
    assert_eq!((d1, d2), ("a".to_string(), "b".to_string()));

    teardown(&pool, &db).await;
}

/// 2. `Response { success: false }` fast-fail: the loop stops
/// immediately; the remaining scripted lines are never read.
#[tokio::test]
async fn test_response_error_fast_fails_without_reading_rest() {
    let (pool, session, db) = setup_db().await;
    // The failure response, followed by lines that must NOT be read.
    let mut source = ScriptedSource::from_jsonl(&[
        r#"{"type":"response","command":"prompt","success":false,"error":"No API key found for anthropic"}"#,
        r#"{"type":"turn_start"}"#,
        r#"{"type":"agent_end"}"#,
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(
        matches!(
            outcome.reason,
            TurnEndReason::ResponseError(ref e) if e == "No API key found for anthropic"
        ),
        "expected ResponseError, got {:?}",
        outcome.reason
    );
    assert_eq!(outcome.text, "");
    assert_eq!(
        source.remaining(),
        2,
        "lines after the failure must be unread"
    );

    // No assistant rows at all — the turn never ran, and the
    // `[no response from agent]` placeholder is only written on a
    // clean `AgentEnd`.
    let rows = assistant_rows(&pool, session).await;
    assert!(rows.is_empty(), "no assistant rows on RPC failure");

    teardown(&pool, &db).await;
}

/// 3. pi `error` event → `TurnEndReason::PiError` with the message.
#[tokio::test]
async fn test_pi_error_event() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::from_jsonl(&[
        r#"{"type":"turn_start"}"#,
        r#"{"type":"error","message":"model exploded"}"#,
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(
        matches!(
            outcome.reason,
            TurnEndReason::PiError(ref e) if e == "model exploded"
        ),
        "expected PiError, got {:?}",
        outcome.reason
    );

    let rows = assistant_rows(&pool, session).await;
    assert!(rows.is_empty(), "error turns flush no assistant rows");

    teardown(&pool, &db).await;
}

/// 4. EOF (`read_line` → `Ok(None)`) → `TurnEndReason::PiDied`.
#[tokio::test]
async fn test_eof_means_pi_died() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::new(vec![
        ScriptLine::Line(r#"{"type":"turn_start"}"#.to_string()),
        ScriptLine::Eof,
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(
        matches!(outcome.reason, TurnEndReason::PiDied),
        "expected PiDied, got {:?}",
        outcome.reason
    );
    // The trailing text flush only fires when there's text; none was
    // produced, and the placeholder is AgentEnd-only → no rows.
    let rows = assistant_rows(&pool, session).await;
    assert!(rows.is_empty());

    teardown(&pool, &db).await;
}

/// 4b. `read_line` transport error → `TurnEndReason::PiError`.
#[tokio::test]
async fn test_read_line_error() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::new(vec![
        ScriptLine::Line(r#"{"type":"turn_start"}"#.to_string()),
        ScriptLine::Err("connection reset".to_string()),
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(
        matches!(
            outcome.reason,
            TurnEndReason::PiError(ref e) if e.contains("connection reset")
        ),
        "expected PiError from read failure, got {:?}",
        outcome.reason
    );

    teardown(&pool, &db).await;
}

/// 5. Stale pre-turn events ignored: an `agent_end` BEFORE
/// `turn_start` (a replayed prior turn) is not honored; the loop
/// completes on the real `agent_end` after `turn_start`.
#[tokio::test]
async fn test_stale_agent_end_before_turn_start_is_ignored() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::from_jsonl(&[
        // Leftover from a prior turn / replayed session: must NOT end
        // this turn.
        r#"{"type":"agent_end"}"#,
        r#"{"type":"turn_start"}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"real"}}"#,
        r#"{"type":"agent_end"}"#,
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(
        matches!(outcome.reason, TurnEndReason::AgentEnd),
        "expected AgentEnd, got {:?}",
        outcome.reason
    );
    assert_eq!(outcome.text, "real");

    let rows = assistant_rows(&pool, session).await;
    assert_eq!(rows, vec![(1, "real".to_string())]);

    teardown(&pool, &db).await;
}

/// 6. No-response placeholder: `turn_start` + `agent_end` with zero
/// text → an assistant row with content `[no response from agent]`.
#[tokio::test]
async fn test_no_response_placeholder_row() {
    let (pool, session, db) = setup_db().await;
    let mut source =
        ScriptedSource::from_jsonl(&[r#"{"type":"turn_start"}"#, r#"{"type":"agent_end"}"#]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(matches!(outcome.reason, TurnEndReason::AgentEnd));
    assert_eq!(outcome.text, "");

    let rows = assistant_rows(&pool, session).await;
    assert_eq!(rows, vec![(1, "[no response from agent]".to_string())]);

    teardown(&pool, &db).await;
}

/// 7. Chunk-flush boundary: `text_delta("pre")`, `toolcall_start`,
/// `text_delta("post")`, `agent_end` → TWO distinct assistant rows
/// ("pre" at seq 1, "post" at seq 2), in sequence order.
#[tokio::test]
async fn test_toolcall_start_flushes_chunk_boundary() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::from_jsonl(&[
        r#"{"type":"turn_start"}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"pre"}}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"toolcall_start"}}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"post"}}"#,
        r#"{"type":"agent_end"}"#,
    ]);

    let outcome = run_turn(&pool, session, &mut source).await;

    assert!(matches!(outcome.reason, TurnEndReason::AgentEnd));
    // full_text accumulates across chunk flushes.
    assert_eq!(outcome.text, "prepost");

    let rows = assistant_rows(&pool, session).await;
    assert_eq!(
        rows,
        vec![(1, "pre".to_string()), (2, "post".to_string())],
        "toolcall_start must flush 'pre'; the trailing flush writes 'post'"
    );

    teardown(&pool, &db).await;
}

// 8. In-flight tool counting / read-timeout branch:
// NOT unit-testable without a multi-minute stall (the per-read
// timeouts are 5 min idle / 2h in-tool). Covered e2e instead;
// this file deliberately skips it.

/// 9. Compaction prelude: `compact_first = true` runs the compact RPC
/// and drains afterwards, then the normal loop.
#[tokio::test]
async fn test_compact_first_runs_compact_rpc() {
    let (pool, session, db) = setup_db().await;
    let mut source = ScriptedSource::from_jsonl(&[
        r#"{"type":"turn_start"}"#,
        r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"ok"}}"#,
        r#"{"type":"agent_end"}"#,
    ]);

    let bus = MessageBus::new();
    let metrics = Metrics::new();
    let outcome = drive_turn_with(
        &pool,
        &bus,
        &metrics,
        session,
        &mut source,
        "hi",
        None,
        true,
    )
    .await;

    assert!(matches!(outcome.reason, TurnEndReason::AgentEnd));
    assert_eq!(source.compacted, 1, "compact RPC must run exactly once");
    assert!(source.drained >= 2, "drain before prompt AND after compact");

    teardown(&pool, &db).await;
}
