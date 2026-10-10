//! Herd H6.5 integration tests: the credentials scope + the
//! `web_login` broker.
//!
//! Covers:
//!   - the `secrets` table seam (`GET/PUT/DELETE /secrets`): names +
//!     expiry on the wire, values NEVER; owner tenancy; restricted-
//!     name 400.
//!   - `credentials::resolve_credential_env`: present / absent /
//!     expired-ref cases (the sandbox env-ref resolution seam).
//!   - the `web_login` approval flow end to end: the ranch card
//!     (`RanchToolQueue` kind `web_login` + `ranch_tool_request` bus
//!     event) → approval → one-time token → handoff page → token/
//!     cookie exchange → the value lands in the secret store.
//!   - **the grep audit**: the cookie value is absent from every
//!     `messages` row of the session, from the approval card payload,
//!     and from the tool-facing response bodies. The value reaches
//!     ONLY the sandbox argv (`--setenv=`) at exec time.
//!   - the deny path: "Cancel" → `status: denied`, no token, no
//!     secret.

mod test_helpers;

use forge_api::credentials::{resolve_credential_env, CredentialError};
use serde_json::json;
use test_helpers::TestApp;
use uuid::Uuid;

// ============================================
// Helpers
// ============================================

const PASSWORD: &str = "password123";

/// Register a user, log in, return `(user_id, api_key)`.
async fn register_user(app: &TestApp, email: &str, name: &str) -> (Uuid, String) {
    let resp = app
        .post("/auth/register")
        .json(&json!({ "email": email, "name": name, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "register {email} → 201");
    let user_id: Uuid = resp.json::<serde_json::Value>().await.unwrap()["user"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let resp = app
        .post("/auth/login")
        .json(&json!({ "email": email, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "login {email} → 200");
    let api_key = resp.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string();
    (user_id, api_key)
}

/// Create a profile owned by `api_key`.
async fn create_profile(app: &TestApp, api_key: &str, name: &str) -> Uuid {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": name,
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "api_key": "sk-test-profile",
            "working_dir": "/tmp/h65-profile"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create profile: {}", resp.text());
    let id: Uuid = resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    id
}

/// Create an agent with a `credentials_scope` env_refs declaration
/// and one conversation (session) bound to it. Returns
/// `(agent_id, session_id)`.
async fn create_agent_with_session(
    app: &TestApp,
    api_key: &str,
    name: &str,
    profile_id: Uuid,
    env_refs: &[&str],
) -> (Uuid, Uuid) {
    let resp = app
        .post("/agents")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": name,
            "primary_profile_id": profile_id,
            "credentials_scope": { "env_refs": env_refs },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create agent: {}", resp.text());
    let agent_id: Uuid = resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let resp = app
        .post(&format!("/agents/{agent_id}/conversations"))
        .header("X-API-Key", api_key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create conversation: {}", resp.text());
    let session_id: Uuid = resp.json::<serde_json::Value>().await.unwrap()["session"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    (agent_id, session_id)
}

/// A direct pool for `forge_api::credentials` calls + message-row
/// audits.
async fn pool(db_url: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(db_url)
        .await
        .expect("connect to test db")
}

/// The grep-audit primitive: every `messages` row's content / tool
/// input / tool output must NOT contain `needle`.
async fn assert_secret_absent_from_rows(pool: &sqlx::PgPool, session_id: Uuid, needle: &str) {
    let rows: Vec<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT content, tool_input::text, tool_output::text FROM messages WHERE session_id = $1",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await
    .expect("read messages rows");
    for (content, tool_input, tool_output) in &rows {
        for field in [
            content.as_deref(),
            tool_input.as_deref(),
            tool_output.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            assert!(
                !field.contains(needle),
                "GREP AUDIT FAILED: the credential material '{needle}' \
                 appears in a messages row field (len {})",
                field.len()
            );
        }
    }
}

// ============================================
// The secrets table seam
// ============================================

#[tokio::test]
async fn secrets_crud_never_serializes_values() {
    let (app, _db_url) = TestApp::new().await;
    let secret_value = "sk-super-secret-h65-value";

    let (_uid, key) = register_user(&app, "secrets@example.com", "S").await;
    // foreign user for the tenancy leg
    let (_uid2, other_key) = register_user(&app, "other@example.com", "O").await;

    // set
    let resp = app
        .put("/secrets/BILLING_COOKIE")
        .header("X-API-Key", &key)
        .json(&json!({ "value": secret_value }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT: {}", resp.text());

    // list: the NAME is there, the VALUE is not (grep)
    let body = app
        .get("/secrets")
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    let text = body.text();
    assert!(
        text.contains("BILLING_COOKIE"),
        "list shows the name: {text}"
    );
    assert!(
        !text.contains(secret_value),
        "list must NOT serialize the value: {text}"
    );

    // tenancy: the other user does not see the slot
    let body = app
        .get("/secrets")
        .header("X-API-Key", &other_key)
        .send()
        .await
        .unwrap();
    assert!(
        !body.text().contains("BILLING_COOKIE"),
        "foreign user must not see the slot"
    );

    // invalid slot name → 400
    let resp = app
        .put("/secrets/bad-name")
        .header("X-API-Key", &key)
        .json(&json!({ "value": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "bad name: {}", resp.text());

    // empty value → 400
    let resp = app
        .put("/secrets/EMPTY_SLOT")
        .header("X-API-Key", &key)
        .json(&json!({ "value": "" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "empty value: {}", resp.text());

    // delete
    let resp = app
        .delete("/secrets/BILLING_COOKIE")
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "DELETE: {}", resp.text());
    let body = app
        .get("/secrets")
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert!(!body.text().contains("BILLING_COOKIE"), "gone after delete");
}

#[tokio::test]
async fn credential_env_resolution_present_absent_expired() {
    let (app, db_url) = TestApp::new().await;
    let key = register_user(&app, "envs@example.com", "E").await.1;
    let profile = create_profile(&app, &key, "h65-env-profile").await;
    let (_agent, session) =
        create_agent_with_session(&app, &key, "env-agent", profile, &["H65_SLOT"]).await;
    let pool = pool(&db_url).await;

    // ABSENT: declared but never set → Unavailable (names only, no
    // value material anywhere in the error).
    let err = resolve_credential_env(&pool, session)
        .await
        .expect_err("absent slot must fail closed");
    match err {
        CredentialError::Unavailable(names) => {
            assert_eq!(names, vec!["H65_SLOT".to_string()]);
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }

    // PRESENT
    let value = "the-live-value-99";
    let resp = app
        .put("/secrets/H65_SLOT")
        .header("X-API-Key", &key)
        .json(&json!({ "value": value }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let env = resolve_credential_env(&pool, session).await.unwrap();
    assert_eq!(env, vec![("H65_SLOT".to_string(), value.to_string())]);

    // EXPIRED: valid_until in the past → back to Unavailable.
    let resp = app
        .put("/secrets/H65_SLOT")
        .header("X-API-Key", &key)
        .json(&json!({ "value": value, "valid_until": "2000-01-01T00:00:00Z" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let err = resolve_credential_env(&pool, session)
        .await
        .expect_err("expired slot must fail closed");
    match err {
        CredentialError::Unavailable(names) => {
            assert_eq!(names, vec!["H65_SLOT".to_string()]);
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }

    // No refs declared → empty env (the common case, cheap path).
    let profile2 = create_profile(&app, &key, "h65-env-profile-2").await;
    let (_agent2, session2) =
        create_agent_with_session(&app, &key, "env-agent-2", profile2, &[]).await;
    let env = resolve_credential_env(&pool, session2).await.unwrap();
    assert!(env.is_empty());

    pool.close().await;
}

// ============================================
// The web_login flow + grep audit
// ============================================

/// Drive the card lane from the TEST side: wait for the bus's
/// `RanchToolRequest` for the session, return the captured card
/// payload (the exact JSON ranchd's forge worker sees).
async fn capture_card(
    app: &TestApp,
    session_id: Uuid,
    timeout: std::time::Duration,
) -> serde_json::Value {
    let mut rx = app.app_state.bus.subscribe();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline - tokio::time::Instant::now();
        if remaining.is_zero() {
            panic!("no RanchToolRequest card published within {timeout:?}");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(forge_api::bus::BusEvent::RanchToolRequest {
                session_id: sid,
                payload,
            })) if sid == session_id => {
                return payload;
            }
            Ok(Ok(_)) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                eprintln!("test bus lagged by {n}; continuing");
                continue;
            }
            Ok(Err(e)) => panic!("bus recv error: {e}"),
            Err(_) => panic!("deadline"),
        }
    }
}

/// Drive the long-poll POST from a worker thread with its own
/// runtime (the handler holds the request open for up to 60 s; the
/// test answers the card from the test task while it waits).
/// Returns `(status, body)`.
fn spawn_web_login_post(
    base_url: &str,
    api_key: &str,
    session_id: Uuid,
    url: &str,
    slot: &str,
) -> std::thread::JoinHandle<(u16, String)> {
    let base = base_url.to_string();
    let key = api_key.to_string();
    let url = url.to_string();
    let slot = slot.to_string();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("thread runtime");
        rt.block_on(async move {
            let resp = reqwest::Client::new()
                .post(format!("{base}/sessions/{session_id}/web-login"))
                .header("X-API-Key", key)
                .json(&json!({ "url": url, "slot": slot }))
                .send()
                .await
                .expect("web-login post");
            (
                resp.status().as_u16(),
                resp.text().await.unwrap_or_default(),
            )
        })
    })
}

/// Answer a pending ranch-tool card the way ranchd's forge worker
/// does: `POST /ranch-tools/{id}/result`.
async fn answer_card(app: &TestApp, api_key: &str, id: &str, success: bool) {
    let resp = app
        .post(&format!("/ranch-tools/{id}/result"))
        .header("X-API-Key", api_key)
        .json(&json!({ "success": success }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "card answer: {}", resp.text());
}

/// Pull the one-time token out of the approved response's
/// `handoff_url` query string.
fn token_from_handoff_url(url: &str) -> String {
    let q = url
        .rsplit_once('?')
        .map(|(_, q)| q)
        .unwrap_or("")
        .split('&')
        .find(|p| p.starts_with("token="))
        .unwrap_or_else(|| panic!("no token= in handoff url: {url}"));
    q["token=".len()..].to_string()
}

#[tokio::test]
async fn web_login_approval_flow_and_grep_audit() {
    let cookie = "H65_THE_SECRET_COOKIE_XK9"; // the grep-audit needle
    let target_url = "https://billing.example.com/login";

    let (app, db_url) = TestApp::new().await;
    let key = register_user(&app, "weblogin@example.com", "W").await.1;
    let profile = create_profile(&app, &key, "h65-web-profile").await;
    let (_agent, session) =
        create_agent_with_session(&app, &key, "billing", profile, &["BILLING_COOKIE"]).await;
    let pool = pool(&db_url).await;

    // 1 — the harness tool's call: POST /sessions/:id/web-login.
    let post_thread =
        spawn_web_login_post(&app.base_url, &key, session, target_url, "BILLING_COOKIE");

    // 2 — the approval card appears on the bus (what ranchd's forge
    //    worker turns into the "Sign me in / Cancel" AgentAsk).
    let card = capture_card(&app, session, std::time::Duration::from_secs(10)).await;
    assert_eq!(card["tool"], "web_login", "card tool kind: {card}");
    assert_eq!(
        card["input"]["url"], target_url,
        "card shows the url: {card}"
    );
    assert_eq!(
        card["input"]["slot"], "BILLING_COOKIE",
        "card names the slot: {card}"
    );
    assert!(
        !card.to_string().contains(cookie),
        "the card payload must carry no credential material: {card}"
    );

    // 3 — the user taps "Sign me in" (ranchd POSTs the decision).
    answer_card(&app, &key, card["id"].as_str().unwrap(), true).await;

    // 4 — the tool response: approved + handoff url with the
    //    one-time token (and NEVER the cookie — the cookie doesn't
    //    exist yet, but assert anyway).
    let (status, body_text) = post_thread.join().unwrap();
    assert_eq!(status, 200, "web-login: {body_text}");
    let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();
    assert_eq!(body["status"], "approved", "{body}");
    assert!(!body_text.contains(cookie), "response leaks: {body_text}");
    let handoff_url = body["handoff_url"].as_str().unwrap().to_string();
    let token = token_from_handoff_url(&handoff_url);
    assert!(
        handoff_url.contains("/auth/browser-handoff"),
        "{handoff_url}"
    );
    assert!(
        handoff_url.contains("next=https%3A%2F%2Fbilling.example.com%2Flogin"),
        "next= must be query-encoded: {handoff_url}"
    );

    // 5 — the handoff page (NO api key — the token is the
    //    credential): 200 + renders the target.
    let next_q = url_query(&handoff_url);
    let page = app
        .get(&format!("/auth/browser-handoff?{next_q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200, "handoff page: {}", page.text());
    assert!(
        page.text().contains("billing.example.com"),
        "page shows target"
    );
    assert!(!page.text().contains(cookie));

    // 6 — the user pastes the cookie: token → secret exchange.
    let resp = app
        .post("/auth/browser-handoff")
        .json(&json!({ "token": token, "value": cookie }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "handoff post: {}", resp.text());

    // single-use: the token is consumed.
    let resp = app
        .post("/auth/browser-handoff")
        .json(&json!({ "token": token, "value": "again" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "token reuse must 404: {}", resp.text());

    // unknown token → 404.
    let resp = app
        .post("/auth/browser-handoff")
        .json(&json!({ "token": "nope", "value": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // 7 — `web_login_done` probe: signed in.
    let resp = app
        .get(&format!(
            "/sessions/{session}/web-login?slot=BILLING_COOKIE"
        ))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["status"],
        "signed_in"
    );

    // 8 — the secret store seam now resolves the slot for the
    //    agent's sandbox env (value reaches ONLY the nspawn argv).
    let env = resolve_credential_env(&pool, session).await.unwrap();
    assert_eq!(
        env,
        vec![("BILLING_COOKIE".to_string(), cookie.to_string())]
    );

    // 9 — THE GREP AUDIT: the cookie is absent from every messages
    //    row of the session (content / tool input / tool output).
    assert_secret_absent_from_rows(&pool, session, cookie).await;

    pool.close().await;
}

#[tokio::test]
async fn web_login_denied_no_secret_lands() {
    let cookie = "H65_DENIED_COOKIE_QQ1";
    let (app, db_url) = TestApp::new().await;
    let key = register_user(&app, "deny@example.com", "D").await.1;
    let profile = create_profile(&app, &key, "h65-deny-profile").await;
    let (_agent, session) =
        create_agent_with_session(&app, &key, "deny-agent", profile, &["BILLING_COOKIE"]).await;
    let pool = pool(&db_url).await;

    let post_thread = spawn_web_login_post(
        &app.base_url,
        &key,
        session,
        "https://x.io/login",
        "BILLING_COOKIE",
    );

    let card = capture_card(&app, session, std::time::Duration::from_secs(10)).await;
    // the user taps "Cancel"
    answer_card(&app, &key, card["id"].as_str().unwrap(), false).await;

    let (_status, body_text) = post_thread.join().unwrap();
    let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();
    assert_eq!(body["status"], "denied", "{body}");
    assert!(
        body.get("handoff_url").is_none(),
        "no token on deny: {body}"
    );

    // nothing was stored; the probe says none.
    let resp = app
        .get(&format!(
            "/sessions/{session}/web-login?slot=BILLING_COOKIE"
        ))
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["status"],
        "none"
    );

    let err = resolve_credential_env(&pool, session)
        .await
        .expect_err("no secret may exist after a denial");
    assert!(matches!(err, CredentialError::Unavailable(_)));

    assert_secret_absent_from_rows(&pool, session, cookie).await;
    pool.close().await;
}

/// The query string of a `scheme://host/path?QUERY` url.
fn url_query(url: &str) -> &str {
    url.rsplit_once('?').map(|(_, q)| q).unwrap_or("")
}

#[tokio::test]
async fn web_login_input_validation() {
    let (app, _db_url) = TestApp::new().await;
    let key = register_user(&app, "val@example.com", "V").await.1;
    let profile = create_profile(&app, &key, "h65-val-profile").await;
    let (_agent, session) = create_agent_with_session(&app, &key, "val-agent", profile, &[]).await;

    // relative URL → 400
    let resp = app
        .post(&format!("/sessions/{session}/web-login"))
        .header("X-API-Key", &key)
        .json(&json!({ "url": "/login", "slot": "S" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "relative url: {}", resp.text());

    // bad slot name → 400
    let resp = app
        .post(&format!("/sessions/{session}/web-login"))
        .header("X-API-Key", &key)
        .json(&json!({ "url": "https://x.io", "slot": "lower" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "bad slot: {}", resp.text());

    // foreign session (another user's) → 404, no existence leak
    let _other = register_user(&app, "val2@example.com", "V2").await;
    let key2 = register_user(&app, "val3@example.com", "V3").await.1;
    let resp = app
        .post(&format!("/sessions/{session}/web-login"))
        .header("X-API-Key", &key2)
        .json(&json!({ "url": "https://x.io", "slot": "S" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "foreign session: {}", resp.text());
}
