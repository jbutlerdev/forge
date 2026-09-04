//! P2-38: AgentRegistry behavioral tests.
//!
//! Covers the three Wave-1 registry fixes:
//!
//! - **Respawn after dead pi** (P0-7): `get_or_create` must not hand
//!   back a cached agent whose pi process is dead; it must drop the
//!   stale entry and respawn via the slow path.
//! - **Spawn-lock leak** (P0-11): a `get_or_create` that fails
//!   (bogus session, failed pi spawn) must not leave a per-session
//!   spawn-lock entry behind — a retry loop hitting bad ids used to
//!   grow the map unboundedly.
//! - **Remove lock behavior** (P0-9): `remove()` must take the map
//!   write lock only long enough to clone the agent and drop the
//!   entry, then kill pi with the map lock released (a regression
//!   held the map lock across the agent-lock await and stalled every
//!   other session's dispatch for up to an hour).
//!
//! The spawn-lock and remove-noop tests run without a pi binary.
//! The tests that need to observe a live pi process (respawn,
//! remove-while-locked) are `#[ignore]`d and require `pi` on
//! `PATH` (plus the usual profile env, e.g. `ANTHROPIC_API_KEY`):
//!
//! ```sh
//! cargo test -p forge-api --test registry_tests -- --ignored
//! ```

mod test_helpers;

use forge_api::agent_registry::{AgentRegistry, AgentRegistryError};
use forge_api::sandbox::SandboxManager;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const ADMIN_URL: &str = "postgres://postgres:forge@localhost/postgres";

/// Create a fresh test database and run migrations. Returns (pool,
/// db_url).
async fn setup_db() -> (PgPool, String) {
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

    (pool, db_url)
}

/// Drop the per-test database (shared helper from test_helpers).
fn teardown(pool: PgPool, db_url: &str) {
    let db_name = db_url
        .rsplit('/')
        .next()
        .and_then(|s| s.split('?').next())
        .unwrap_or("forge_test")
        .to_string();
    let admin_url = ADMIN_URL.to_string();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("cleanup runtime");
        rt.block_on(async move {
            test_helpers::drop_test_db(&admin_url, &db_name).await;
            let _ = sqlx::PgPool::close(&pool).await;
        })
    })
    .join()
    .ok();
}

/// Seed a profile + session usable by `get_or_create`. The profile
/// has no `git_url` and a non-existent `working_dir`, so sandbox
/// creation takes the bare-session-dir fallback (no external
/// binaries involved). Returns the session id.
async fn seed_session(pool: &PgPool) -> Uuid {
    let profile_id: Uuid = sqlx::query_scalar(
        "INSERT INTO profiles (name, provider, model, working_dir, system_prompt, tools)
         VALUES ($1, 'anthropic', 'test-model', '/nonexistent-forge-registry-test', '', '[]')
         RETURNING id",
    )
    .bind(format!("registry-test-{}", Uuid::new_v4().simple()))
    .fetch_one(pool)
    .await
    .expect("seed profile");

    sqlx::query_scalar(
        "INSERT INTO sessions (profile_id, title) VALUES ($1, 'registry test') RETURNING id",
    )
    .bind(profile_id)
    .fetch_one(pool)
    .await
    .expect("seed session")
}

/// Build a registry whose sandbox managers point at a fresh
/// per-test tempdir (never the production `/forge/` tree).
fn make_registry() -> Arc<AgentRegistry> {
    let tmp = TempDir::new().expect("create tempdir");
    let sessions_dir = tmp.path().join("sessions");
    let sandbox_dir = tmp.path().join("sandbox");
    std::fs::create_dir_all(&sessions_dir).expect("mkdir sessions");
    std::fs::create_dir_all(&sandbox_dir).expect("mkdir sandbox");
    let sandbox = Arc::new(SandboxManager::with_base_dir(sandbox_dir, sessions_dir));
    Arc::new(AgentRegistry::new(
        "http://localhost:8080/api/v1".to_string(),
        sandbox,
    ))
}

/// Guard that hides every binary from `PATH` for the duration of a
/// test, so `PiAgent::spawn` fails with "No such file or directory"
/// instead of starting a real pi. Restores `PATH` on drop.
///
/// Only safe in this test binary: no other test here spawns
/// subprocesses (sqlx talks TCP; the sandbox path is the
/// bare-dir fallback).
struct NarrowPathGuard;
impl NarrowPathGuard {
    fn new() -> Self {
        std::env::set_var("PATH", "/nonexistent-forge-registry-test");
        Self
    }
}
impl Drop for NarrowPathGuard {
    fn drop(&mut self) {
        std::env::set_var("PATH", "/usr/local/bin:/usr/bin:/bin");
    }
}

/// `get_or_create` returns `Result<SharedPiAgent, _>` but
/// `SharedPiAgent` is not `Debug`, so `Result::expect` isn't usable
/// directly. Unwrap with a readable panic message instead.
fn agent_or_panic(
    res: std::result::Result<forge_api::agent_registry::SharedPiAgent, AgentRegistryError>,
    what: &str,
) -> forge_api::agent_registry::SharedPiAgent {
    match res {
        Ok(a) => a,
        Err(e) => panic!("{what}: {e:?}"),
    }
}

/// P0-11: a `get_or_create` that fails on a bogus session id must
/// release its per-session spawn lock on the error path. The old
/// code only removed the lock entry on success, so a retry loop
/// against bad ids grew the map unboundedly.
#[tokio::test]
async fn failed_get_or_create_bogus_session_releases_spawn_lock() {
    let (pool, db_url) = setup_db().await;
    let registry = make_registry();

    assert_eq!(registry.spawn_lock_count(), 0);

    let bogus = Uuid::new_v4();
    // `SharedPiAgent` is not `Debug`, so `Result::expect_err` (which
    // requires `T: Debug`) is not usable; match instead.
    let err = match registry.get_or_create(&pool, bogus).await {
        Ok(_) => panic!("bogus session id must fail"),
        Err(e) => e,
    };
    assert!(
        matches!(err, AgentRegistryError::Database(_)),
        "expected a database error for a missing session, got {err:?}"
    );
    assert_eq!(
        registry.spawn_lock_count(),
        0,
        "spawn lock entry must be removed after a failed get_or_create"
    );

    // Simulate the retry loop the P0-11 fix targets: several
    // sequential failures against distinct bad ids. Each must fail
    // promptly (no deadlock on a leaked lock) and leave no entry.
    for i in 0..5 {
        let id = Uuid::new_v4();
        let res = tokio::time::timeout(Duration::from_secs(30), registry.get_or_create(&pool, id))
            .await
            .unwrap_or_else(|_| panic!("iteration {i} deadlocked"));
        assert!(res.is_err(), "iteration {i} must fail");
        assert_eq!(
            registry.spawn_lock_count(),
            0,
            "iteration {i} leaked a spawn lock entry"
        );
    }

    teardown(pool, &db_url);
}

/// P0-11, deeper variant: a session that passes the DB checks but
/// whose pi spawn itself fails (no pi on PATH) must also release
/// the spawn lock. This exercises the `SpawnLockDtor` on the
/// `AgentSpawn` error path — far down the slow path (sandbox prep,
/// tool-call replay, jsonl write, then the failing spawn).
#[tokio::test]
async fn failed_pi_spawn_releases_spawn_lock() {
    let (pool, db_url) = setup_db().await;
    let registry = make_registry();
    let session_id = seed_session(&pool).await;

    let _narrow = NarrowPathGuard::new();
    assert_eq!(registry.spawn_lock_count(), 0);

    let res = tokio::time::timeout(
        Duration::from_secs(120),
        registry.get_or_create(&pool, session_id),
    )
    .await
    .unwrap_or_else(|e| panic!("get_or_create must not hang: {e}"));
    let err = match res {
        Ok(_) => panic!("pi spawn must fail with an empty PATH"),
        Err(e) => e,
    };
    assert!(
        matches!(err, AgentRegistryError::AgentSpawn(_)),
        "expected an AgentSpawn error, got {err:?}"
    );

    assert_eq!(
        registry.spawn_lock_count(),
        0,
        "spawn lock entry must be removed after a failed pi spawn"
    );
    assert!(
        !registry.contains(session_id).await,
        "no agent entry may survive a failed spawn"
    );

    // And the next attempt proceeds instead of deadlocking behind
    // a leaked lock.
    let err2 = tokio::time::timeout(
        Duration::from_secs(120),
        registry.get_or_create(&pool, session_id),
    )
    .await
    .unwrap_or_else(|e| panic!("second attempt must not hang: {e}"));
    let err2 = match err2 {
        Ok(_) => panic!("second spawn attempt must fail too"),
        Err(e) => e,
    };
    assert!(matches!(err2, AgentRegistryError::AgentSpawn(_)));

    teardown(pool, &db_url);
}

/// P0-9, no-pi variant: `remove()` on a session with no cached
/// agent is a clean no-op (no error, no panic), and the
/// in-flight-turn bookkeeping survives it — `end_turn` after
/// `remove` still works.
#[tokio::test]
async fn remove_missing_session_is_clean_noop() {
    let registry = make_registry();
    let sid = Uuid::new_v4();

    registry.begin_turn(sid);
    assert!(registry.has_in_flight_turn(sid));

    registry
        .remove(sid)
        .await
        .expect("remove on a session with no agent is a clean no-op");
    assert!(!registry.contains(sid).await);
    assert!(registry.is_empty().await);

    // The in-flight set is independent of the agent map; end_turn
    // after remove must not panic.
    registry.end_turn(sid);
    assert!(!registry.has_in_flight_turn(sid));
}

/// P0-7: a cached agent whose pi process has died must not be
/// handed back by `get_or_create`; the entry is dropped and a fresh
/// pi is spawned (durable replay).
///
/// Requires a live pi binary on PATH (and a working model profile,
/// e.g. `ANTHROPIC_API_KEY` set). Marked `#[ignore]` so the default
/// suite runs without pi.
#[tokio::test]
#[ignore = "requires a live pi binary on PATH; run with `cargo test -p forge-api --test registry_tests -- --ignored`"]
async fn respawn_after_dead_pi_returns_a_fresh_agent() {
    let (pool, db_url) = setup_db().await;
    let registry = make_registry();
    let session_id = seed_session(&pool).await;

    let agent_a = agent_or_panic(
        registry.get_or_create(&pool, session_id).await,
        "first spawn",
    );
    let pid_a = { agent_a.lock().await.id() };

    // Kill pi out from under the registry (simulates pi crashing
    // or being reaped by a timed-out turn).
    agent_a.lock().await.kill().await.expect("kill pi");
    assert!(!agent_a.is_alive(), "killed agent must report dead");

    let agent_b = agent_or_panic(
        registry.get_or_create(&pool, session_id).await,
        "get_or_create must respawn, not return the dead agent",
    );
    let pid_b = { agent_b.lock().await.id() };

    assert!(
        pid_a != pid_b,
        "expected a fresh pi process, but got the same pid {pid_a:?} (== {pid_b:?})"
    );
    assert!(agent_b.is_alive(), "the respawned agent must be alive");

    // Cleanup: kill the fresh pi and clear the registry.
    let _ = agent_b.lock().await.kill().await;
    let _ = registry.remove(session_id).await;

    teardown(pool, &db_url);
}

/// P0-9, live-pi variant: while the per-agent turn lock is held by
/// an in-flight turn, `remove()` must kill the agent with the
/// global map write lock already released. A regression (holding
/// the map lock across the agent-lock await) would stall
/// `contains()` — a plain map read — for the whole duration of the
/// held agent lock.
///
/// Requires a live pi binary on PATH. Marked `#[ignore]`.
#[tokio::test]
#[ignore = "requires a live pi binary on PATH; run with `cargo test -p forge-api --test registry_tests -- --ignored`"]
async fn remove_releases_map_lock_before_waiting_on_agent_lock() {
    let (pool, db_url) = setup_db().await;
    let registry = make_registry();
    let session_id = seed_session(&pool).await;

    let agent = agent_or_panic(registry.get_or_create(&pool, session_id).await, "spawn");

    // Hold the per-agent lock for 1s to simulate an in-flight turn
    // that `remove()`'s kill must wait for.
    let hold = {
        let a = agent.clone();
        tokio::spawn(async move {
            let _g = a.lock().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        })
    };
    // Give the hold task time to actually grab the lock.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start the remove; it will clone the agent, drop the map entry,
    // release the map write lock, and *then* wait on the agent lock
    // for the kill.
    let remove_handle = tokio::spawn({
        let registry = Arc::clone(&registry);
        async move { registry.remove(session_id).await }
    });

    // The map read lock must be free immediately: this assertion
    // fires if the map write lock is still held (the P0-9 bug).
    let t0 = Instant::now();
    let contains = registry.contains(Uuid::new_v4()).await;
    let elapsed = t0.elapsed();
    assert!(!contains);
    assert!(
        elapsed < Duration::from_millis(500),
        "contains() blocked on the map lock for {elapsed:?} — remove() must release the map write lock before waiting on the agent lock"
    );

    let _ = tokio::time::timeout(Duration::from_secs(10), remove_handle)
        .await
        .expect("remove must finish")
        .expect("remove must succeed");
    assert!(
        !registry.contains(session_id).await,
        "the agent entry must be gone after remove"
    );
    let _ = hold.await;

    teardown(pool, &db_url);
}
