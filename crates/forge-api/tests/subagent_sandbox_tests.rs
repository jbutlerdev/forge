//! Migration 032: subagent sandbox inheritance + the explicit
//! `anchored` column.
//!
//! - `sandbox_key_for_session` walks subagent children (and only
//!   children — `sandbox_inherit`-true rows with a parent) up to the
//!   nearest non-inheriting ancestor; cycles converge instead of
//!   spinning.
//! - The sandbox-vs-host decision reads `sessions.anchored`, NOT
//!   `working_dir IS NOT NULL` (the inference that let fresh
//!   conversations execute bash on the host, 2026-10-20). These tests
//!   pin the column semantics through the API.

use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

mod test_helpers;
use test_helpers::TestApp;

const PASSWORD: &str = "password123";

async fn register_user(app: &TestApp, email: &str) -> String {
    let body = json!({ "email": email, "name": email, "password": PASSWORD });
    test_helpers::auth_post(app, "/auth/register", &body, "register", 201).await;
    let login = json!({ "email": email, "password": PASSWORD });
    let v = test_helpers::auth_post(app, "/auth/login", &login, "login", 200).await;
    v["api_key"].as_str().unwrap().to_string()
}

async fn make_profile(app: &TestApp, api_key: &str) -> Uuid {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": format!("p-{}", Uuid::new_v4()),
            "provider": "openai",
            "model": "gpt-4o",
            "api_key": "sk-test",
            "working_dir": "/tmp"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "profile create: {}", resp.text());
    let v: serde_json::Value = resp.json().await.unwrap();
    v["profile"]["id"].as_str().unwrap().parse().unwrap()
}

async fn create_session(app: &TestApp, api_key: &str, body: serde_json::Value) -> Uuid {
    let resp = app
        .post("/sessions")
        .header("X-API-Key", api_key)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "session create: {}", resp.text());
    let v: serde_json::Value = resp.json().await.unwrap();
    v["session"]["id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn plain_session_is_not_anchored_and_not_inheriting() {
    let (app, _db) = TestApp::new().await;
    let key = register_user(&app, "plain@migrate.test").await;
    let profile = make_profile(&app, &key).await;
    let sid = create_session(&app, &key, json!({"profile_id": profile, "title": "chain"})).await;

    let row =
        sqlx::query("SELECT anchored, sandbox_inherit, working_dir FROM sessions WHERE id = $1")
            .bind(sid)
            .fetch_one(&app.app_state.db)
            .await
            .unwrap();
    let anchored: bool = row.get("anchored");
    let inherit: bool = row.get("sandbox_inherit");
    let wd: Option<String> = row.get("working_dir");
    assert!(!anchored, "plain conversation must stay sandboxed");
    assert!(!inherit);
    assert!(
        wd.is_none(),
        "per-session tree rows must keep the column NULL"
    );

    // Resolver is a no-op on a plain session.
    let k = forge_api::api::sandbox_key_for_session(&app.app_state.db, sid).await;
    assert_eq!(k, sid);
}

#[tokio::test]
async fn anchored_session_sets_the_column() {
    let (app, _db) = TestApp::new().await;
    let key = register_user(&app, "anchor@migrate.test").await;
    let profile = make_profile(&app, &key).await;
    let dir = std::env::temp_dir().join(format!("anchor-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();

    let sid = create_session(
        &app,
        &key,
        json!({"profile_id": profile, "title": "anchored", "working_dir": dir.to_str().unwrap()}),
    )
    .await;

    let anchored: bool = sqlx::query_scalar("SELECT anchored FROM sessions WHERE id = $1")
        .bind(sid)
        .fetch_one(&app.app_state.db)
        .await
        .unwrap();
    assert!(anchored, "explicit cwd anchor must set anchored=TRUE");
    // Anchored rows are top-level: no inheritance walk.
    let k = forge_api::api::sandbox_key_for_session(&app.app_state.db, sid).await;
    assert_eq!(k, sid);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn resolver_walks_the_inherit_chain_and_survives_cycles() {
    let (app, _db) = TestApp::new().await;
    let key = register_user(&app, "chain@migrate.test").await;
    let profile = make_profile(&app, &key).await;
    let parent = create_session(
        &app,
        &key,
        json!({"profile_id": profile, "title": "parent"}),
    )
    .await;
    let user: Uuid = sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
        .bind(parent)
        .fetch_one(&app.app_state.db)
        .await
        .unwrap();
    let db = app.app_state.db.clone();

    let mk_child = |id: Uuid, p: Option<Uuid>| {
        let db = db.clone();
        async move {
            sqlx::query(
                "INSERT INTO sessions (id, profile_id, user_id, parent_session_id, sandbox_inherit, working_dir)
                 VALUES ($1, $2, $3, $4, TRUE, '/forge/sessions/parent')",
            )
            .bind(id)
            .bind(profile)
            .bind(user)
            .bind(p)
            .execute(&db)
            .await
            .unwrap();
        }
    };
    let child = Uuid::new_v4();
    let grand = Uuid::new_v4();
    mk_child(child, Some(parent)).await;
    mk_child(grand, Some(child)).await;

    assert_eq!(
        forge_api::api::sandbox_key_for_session(&db, child).await,
        parent
    );
    assert_eq!(
        forge_api::api::sandbox_key_for_session(&db, grand).await,
        parent
    );

    // Cycle: a <-> b both inheriting — must terminate on a visited id.
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    // FK blocks dangling refs, so create then link (mirrors how a
    // pathological cycle could only arise via multi-row races).
    for id in [a, b] {
        sqlx::query(
            "INSERT INTO sessions (id, profile_id, user_id, sandbox_inherit)
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(id)
        .bind(profile)
        .bind(user)
        .execute(&db)
        .await
        .unwrap();
    }
    for (id, p) in [(a, b), (b, a)] {
        sqlx::query("UPDATE sessions SET parent_session_id = $2 WHERE id = $1")
            .bind(id)
            .bind(p)
            .execute(&db)
            .await
            .unwrap();
    }
    let k = forge_api::api::sandbox_key_for_session(&db, a).await;
    assert!(k == a || k == b, "cycle converges to a visited id");
}
