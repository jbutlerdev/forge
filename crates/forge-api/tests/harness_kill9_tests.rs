//! Herd H2.6 (Migrate & cutover) — the scaled-up kill -9 acceptance.
//!
//! PLAN-HERD H2.6 gate: "a kill -9 of *both* forge-api and the harness
//! mid-turn recovers to a consistent state (the H0.2 kill-9 test,
//! scaled up)."
//!
//! Scenario:
//!
//! 1. A legacy session (unstamped, with a pre-cutover transcript)
//!    gets its first write: the lazy migration imports the
//!    transcript, the prompt is submitted, and the harness child
//!    starts streaming the answer (faux provider paced to
//!    ~15 s/turn via `FORGE_HARNESS_FAUX_TOKENS_PER_SEC=5`).
//! 2. Mid-stream: the harness child is **kill -9**'d and the
//!    forge-api process is dropped (its axum server + event consumer
//!    die with it). Everything durable is in Postgres: the migrated
//!    conversation, the submitted prompt, and the in-flight task's
//!    checkpoint.
//! 3. A SECOND forge-api process boots against the SAME database, and
//!    a SECOND harness child boots against the SAME schema. Its
//!    `harness.resume()` (main.ts step 7) self-supervises the
//!    unfinished turn; the provider call is retried (the faux
//!    provider's re-queued answer) and the turn COMPLETES.
//! 4. Consistency assertions: the prompt was submitted exactly once
//!    (one durable `pi.user` entry, one `messages` user row), the
//!    imported transcript was written exactly once, and exactly one
//!    assistant answer row lands for the prompt.
//!
//! `TestApp::with_existing_db` is the shared-database half: the DB
//! name is intentionally NOT `forge_test_*` so the first app's
//! `Drop` does not drop it out from under the second.

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

mod test_helpers;

const TURN_WAIT: Duration = Duration::from_secs(120);
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
            "name": "H26 Kill9 User",
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

/// SIGKILL a process AND its descendants. `tsx` (the runtime that
/// launches the harness child in these tests) re-execs `node` with
/// its loader flags as a CHILD process, so killing only the tsx
/// parent would leave the real harness running — and holding its
/// Postgres connection. Two `pkill -P` passes cover the tsx→node
/// depth.
fn kill_process_tree(root: u32) {
    for _ in 0..2 {
        let _ = std::process::Command::new("pkill")
            .args(["-9", "-P", &root.to_string()])
            .status();
    }
    std::thread::sleep(Duration::from_millis(200));
}

/// Spawn a harness child with the faux provider. `tps` paces the
/// token stream; `responses` is the queued answer(s).
fn spawn_harness_child(
    app_base_url: &str,
    db_url: &str,
    schema: &str,
    socket_dir: &std::path::Path,
    api_key: &str,
    tps: Option<u32>,
    responses: &str,
) -> std::process::Child {
    let repo_root = std::path::Path::new(
        std::env::var("CARGO_MANIFEST_DIR")
            .as_deref()
            .expect("CARGO_MANIFEST_DIR"),
    )
    .parent()
    .and_then(|p| p.parent())
    .expect("repo root")
    .to_path_buf();
    let mut cmd = std::process::Command::new(repo_root.join("node_modules/.bin/tsx"));
    cmd.arg("harness/src/main.ts")
        .current_dir(&repo_root)
        .env("FORGE_DATABASE_URL", db_url)
        .env("FORGE_API_URL", app_base_url)
        .env("FORGE_API_KEY", api_key)
        .env("FORGE_HARNESS_SOCKET", socket_dir.join("harness.sock"))
        .env(
            "FORGE_HARNESS_EVENTS_SOCKET",
            socket_dir.join("harness-events.sock"),
        )
        .env("FORGE_HARNESS_SCHEMA", schema)
        .env("FORGE_HARNESS_FAUX", "1")
        .env("FORGE_HARNESS_FAUX_RESPONSES", responses);
    if let Some(t) = tps {
        cmd.env("FORGE_HARNESS_FAUX_TOKENS_PER_SEC", t.to_string());
    }
    cmd.spawn().expect("spawn the harness child")
}

async fn wait_for_harness_boot(socket_dir: &std::path::Path, label: &str) {
    let rpc_sock = socket_dir.join("harness.sock");
    let deadline = tokio::time::Instant::now() + HARNESS_BOOT_WAIT;
    while !rpc_sock.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{label}: harness child did not open its RPC socket in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

async fn wait_until(pool: &sqlx::PgPool, sql: &str, deadline: Duration, what: &str) -> i64 {
    let deadline_instant = tokio::time::Instant::now() + deadline;
    let mut last_err: Option<String> = None;
    loop {
        match sqlx::query_scalar::<_, i64>(sql).fetch_optional(pool).await {
            Ok(Some(n)) if n > 0 => return n,
            Ok(_) => {}
            Err(e) => last_err = Some(e.to_string()),
        }
        assert!(
            tokio::time::Instant::now() < deadline_instant,
            "timed out waiting: {what} (last query error: {:?})",
            last_err
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// The H2.6 gate: kill -9 the harness mid-turn on an API process,
/// then recover the turn on a fresh API + harness pair over the same
/// database. No duplicate submissions; no lost transcript.
#[tokio::test]
async fn dual_kill9_recover_mid_turn() {
    let db_name = format!("forge_kill9_{}", uuid::Uuid::new_v4().simple());
    let db_url = test_helpers::create_database(&db_name).await;
    let schema = format!("forge_kill9s_{}", uuid::Uuid::new_v4().simple());
    let socket_dir_1 = tempfile::tempdir().expect("socket tempdir 1");
    let socket_dir_2 = tempfile::tempdir().expect("socket dir 2");

    let pool_probe = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("probe pool");
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool_probe)
        .await
        .expect("scratch schema");

    // ---- process 1: forge-api + harness child ----
    let harness_1 = forge_api::harness::HarnessState::from_paths_with(
        &socket_dir_1.path().join("harness.sock"),
        &socket_dir_1.path().join("harness-events.sock"),
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    // `with_existing_db`: no CREATE/DROP DATABASE around this test
    // (the second process must inherit the first one's data).
    let (app_1, _) = test_helpers::TestApp::with_existing_db(&db_url, harness_1, true).await;
    let consumer_1 = forge_api::harness::spawn_event_consumer(app_1.app_state.clone());
    assert!(consumer_1.is_some(), "event consumer must spawn");

    let user_email = format!("h26-kill9-{}@example.com", uuid::Uuid::new_v4());
    let api_key = register_and_login(&app_1, &user_email).await;

    let profile_resp = app_1
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Kill9 Faux Profile",
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

    // A pre-cutover session: plain row, no durable stamp (a session
    // created before H2.6 cutover — the only kind that migrates
    // lazily; new sessions are stamped at creation).
    let user_id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
        .bind(&user_email)
        .fetch_one(&pool_probe)
        .await
        .expect("test user exists");
    let profile_id_uuid: uuid::Uuid = profile_id.parse().expect("profile id");
    let session_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sessions (id, profile_id, title, user_id)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(session_id)
    .bind(profile_id_uuid)
    .bind("Kill9 session")
    .bind(user_id)
    .execute(&pool_probe)
    .await
    .expect("seed legacy session row");

    // Pre-cutover transcript (the lazy migration must import it).
    sqlx::query(
        "INSERT INTO messages (session_id, sequence, role, content)
         VALUES ($1, get_next_sequence($1), 'user', 'legacy seed prompt')",
    )
    .bind(session_id)
    .execute(&pool_probe)
    .await
    .expect("seed legacy user row");
    sqlx::query(
        "INSERT INTO messages (session_id, sequence, role, content)
         VALUES ($1, get_next_sequence($1), 'assistant', 'legacy seed answer')",
    )
    .bind(session_id)
    .execute(&pool_probe)
    .await
    .expect("seed legacy assistant row");

    // Harness child #1: faux provider paced at 5 tokens/s — the ~60
    // token answer streams for ~12 s, plenty of time to be killed
    // mid-stream.
    let mut child_1 = spawn_harness_child(
        &app_1.base_url,
        &db_url,
        &schema,
        socket_dir_1.path(),
        &api_key,
        Some(5),
        r#"["The capital of France is Paris, the city on the Seine that has served as the political, cultural, and economic heart of France for well over a thousand years and remains so today."]"#,
    );
    wait_for_harness_boot(socket_dir_1.path(), "process 1").await;

    // First write: migrate + submit + start the turn.
    let msg_resp = app_1
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
    eprintln!("kill9: POST /messages -> 202: {}", msg_resp.text());

    // Wait for the migration stamp AND the prompt's durable entry
    // (the submit commit landed — the in-flight task is checkpointed).
    let stamp_sql = format!(
        "SELECT COUNT(*) FROM sessions WHERE id = '{}' AND durable_conversation_id IS NOT NULL",
        session_id
    );
    wait_until(&pool_probe, &stamp_sql, TURN_WAIT, "migration stamp").await;
    let conversation_id: i64 =
        sqlx::query_scalar("SELECT durable_conversation_id FROM sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(&pool_probe)
            .await
            .expect("stamped conversation");

    let prompt_entry_sql = format!(
        r#"SELECT COUNT(*) FROM "{schema}".durable_entries
            WHERE conversation_id = {conversation_id} AND (record::jsonb)->>'kind' = 'pi.user'
              AND (record::jsonb)->'model'->0->>'content' = 'What is the capital of France?'"#
    );
    wait_until(
        &pool_probe,
        &prompt_entry_sql,
        TURN_WAIT,
        "the prompt's durable entry",
    )
    .await;

    // Let the turn actually start streaming: the answer is ~60 tokens
    // at 5 tps (~12 s), so a 1.5 s beat guarantees we kill mid-stream,
    // not before the first generation call.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let task_dump: Vec<String> = sqlx::query_scalar(
        &format!(
            r#"SELECT record::text FROM "{schema}".durable_tasks WHERE conversation_id = {conversation_id} ORDER BY id"#
        ),
    )
    .fetch_all(&pool_probe)
    .await
    .unwrap();
    let sub_dump: Vec<String> = sqlx::query_scalar(
        &format!(
            r#"SELECT record::text FROM "{schema}".durable_submissions WHERE conversation_id = {conversation_id} ORDER BY id"#
        ),
    )
    .fetch_all(&pool_probe)
    .await
    .unwrap();
    eprintln!("kill9: tasks at kill time: {task_dump:?}");
    eprintln!("kill9: submissions at kill time: {sub_dump:?}");

    // Mid-stream now (the answer is ~12 s at 5 tps; we are a few
    // seconds in). Kill -9 the harness: no flush, no cleanup — the
    // whole process tree (tsx + its re-execed node child).
    eprintln!("kill9: killing harness child {} mid-turn", child_1.id());
    kill_process_tree(child_1.id());
    let _ = child_1.kill();
    let _ = child_1.wait();

    // ---- process 1 dies: drop the axum server + consumer + pool ----
    drop(consumer_1);
    drop(app_1);
    pool_probe.close().await;

    // ---- process 2: a fresh forge-api + harness child, same DB ----
    let harness_2 = forge_api::harness::HarnessState::from_paths_with(
        &socket_dir_2.path().join("harness.sock"),
        &socket_dir_2.path().join("harness-events.sock"),
        forge_harness_client::Limits::default(),
        schema.clone(),
    );
    let (app_2, _) = test_helpers::TestApp::with_existing_db(&db_url, harness_2, true).await;
    let consumer_2 = forge_api::harness::spawn_event_consumer(app_2.app_state.clone());
    assert!(consumer_2.is_some(), "event consumer must spawn");

    let pool_2 = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("process-2 pool");

    // Harness child #2 reboots on the same schema; its `resume()`
    // self-supervises the unfinished turn and retries the provider
    // call — the re-queued answer (fast: high TPS) completes it.
    let mut child_2 = spawn_harness_child(
        &app_2.base_url,
        &db_url,
        &schema,
        socket_dir_2.path(),
        &api_key,
        Some(200),
        r#"["Capital: Paris."]"#,
    );
    wait_for_harness_boot(socket_dir_2.path(), "process 2").await;

    // The resumed turn must COMPLETE: app_2's event consumer
    // projects the durable `pi.assistant` entry onto `messages`.
    let answer_sql = format!(
        "SELECT COUNT(*) FROM messages WHERE session_id = '{}' AND role = 'assistant' AND content = 'Capital: Paris.'",
        session_id
    );
    wait_until(
        &pool_2,
        &answer_sql,
        TURN_WAIT,
        "the resumed turn to complete",
    )
    .await;
    eprintln!("kill9: the resumed turn completed on process 2");

    // ---- consistency assertions ----
    // The prompt was submitted exactly once.
    let user_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages
          WHERE session_id = $1 AND role = 'user' AND content = 'What is the capital of France?'",
    )
    .bind(session_id)
    .fetch_one(&pool_2)
    .await
    .unwrap();
    assert_eq!(user_rows, 1, "the prompt row must exist exactly once");

    let prompt_entries: i64 = sqlx::query_scalar(&prompt_entry_sql)
        .fetch_one(&pool_2)
        .await
        .unwrap();
    assert_eq!(
        prompt_entries, 1,
        "the prompt's durable entry must exist exactly once (no duplicate submissions)"
    );

    // The imported transcript was written exactly once.
    let legacy_entries: i64 = sqlx::query_scalar(&format!(
        r#"SELECT COUNT(*) FROM "{schema}".durable_entries
                WHERE conversation_id = $1 AND (record::jsonb)->>'kind' = 'pi.user'
                  AND (record::jsonb)->'model'->0->>'content' = 'legacy seed prompt'"#
    ))
    .bind(conversation_id)
    .fetch_one(&pool_2)
    .await
    .unwrap();
    assert_eq!(legacy_entries, 1, "the import must run exactly once");

    // Exactly one assistant answer for the prompt (the resumed one;
    // the killed stream never committed a partial entry).
    let answer_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages
          WHERE session_id = $1 AND role = 'assistant' AND content = 'Capital: Paris.'",
    )
    .bind(session_id)
    .fetch_one(&pool_2)
    .await
    .unwrap();
    assert_eq!(answer_rows, 1, "exactly one assistant answer row");

    kill_process_tree(child_2.id());
    let _ = child_2.kill();
    let _ = child_2.wait();
    drop(consumer_2);
    drop(app_2);
    pool_2.close().await;
    test_helpers::drop_test_db("postgres://postgres:forge@localhost/postgres", &db_name).await;
}
