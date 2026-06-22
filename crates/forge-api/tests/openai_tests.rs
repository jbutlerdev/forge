//! Integration tests for the OpenAI-compatible API surface
//! (`POST /v1/chat/completions`, `GET /v1/models`).
//!
//! These tests exercise the request-validation, auth, and
//! model-resolution paths — all of which run *before* the agent
//! subprocess is spawned, so they don't need the `pi` binary on
//! PATH and run in CI. The full happy-path (agent runs a turn and
//! returns text) needs `pi` and is in `test_openai_chat_completions_end_to_end`,
//! marked `#[ignore]` for the same reason the native
//! `test_send_message` is.

mod test_helpers;

use serde_json::json;

async fn create_test_app() -> (test_helpers::TestApp, String) {
    test_helpers::TestApp::new().await
}

/// Register + log in, returning the forge API key (`sk_forge_…`).
async fn register_and_login(app: &test_helpers::TestApp) -> String {
    let resp = app
        .post("/auth/register")
        .json(&json!({
            "email": "openai@example.com",
            "name": "OpenAI Test",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = app
        .post("/auth/login")
        .json(&json!({
            "email": "openai@example.com",
            "password": "password123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    body["api_key"].as_str().unwrap().to_string()
}

/// Create a forge profile with the given name and return its id.
/// Uses a non-existent `working_dir` so the session-dir creation
/// doesn't try to copy anything (matches the pattern in the
/// existing integration suite).
async fn create_profile(app: &test_helpers::TestApp, api_key: &str, name: &str) -> String {
    let resp = app
        .post("/profiles")
        .header("X-API-Key", api_key)
        .json(&json!({
            "name": name,
            "provider": "anthropic",
            "model": "claude-sonnet-4-20250514",
            "working_dir": "/tmp/openai-test-profile"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "profile creation should succeed");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["profile"]["id"].as_str().unwrap().to_string()
}

// ============================================
// GET /v1/models
// ============================================

#[tokio::test]
async fn test_openai_models_requires_auth() {
    let (app, _db_url) = create_test_app().await;

    // No auth header at all → the middleware rejects it before
    // the handler runs.
    let resp = app.get("/v1/models").send().await.unwrap();
    assert_eq!(resp.status(), 401, "no auth → 401");
}

#[tokio::test]
async fn test_openai_models_requires_valid_bearer() {
    let (app, _db_url) = create_test_app().await;

    // A Bearer header with a bogus key passes the middleware's
    // presence check but fails the handler's real validation
    // (`extract_auth_user`) → 401 with the OpenAI error envelope.
    let resp = app
        .get("/v1/models")
        .header("Authorization", "Bearer sk_forge_bogus_not_a_real_key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "invalid bearer → 401");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn test_openai_models_lists_profiles_with_bearer() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;
    create_profile(&app, &api_key, "coder-agent").await;
    create_profile(&app, &api_key, "reviewer-agent").await;

    // The OpenAI convention: `Authorization: Bearer <forge-key>`.
    let resp = app
        .get("/v1/models")
        .header("Authorization", &format!("Bearer {}", api_key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "valid bearer → 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "list");
    let ids: Vec<String> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.contains(&"coder-agent".to_string()),
        "profiles should appear as models: {:?}",
        ids
    );
    assert!(
        ids.contains(&"reviewer-agent".to_string()),
        "profiles should appear as models: {:?}",
        ids
    );
    // Each entry should be OpenAI-model-shaped.
    for model in body["data"].as_array().unwrap() {
        assert_eq!(model["object"], "model");
        assert_eq!(model["owned_by"], "forge");
        assert!(
            model["created"].is_i64(),
            "created should be a unix timestamp"
        );
    }
}

#[tokio::test]
async fn test_openai_models_also_accepts_x_api_key() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;
    create_profile(&app, &api_key, "x-key-agent").await;

    // The native forge header should work on the OpenAI surface
    // too, so a client that already has a forge key can use
    // either surface without reconfiguring.
    let resp = app
        .get("/v1/models")
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "X-API-Key should also be accepted");
}

// ============================================
// POST /v1/chat/completions — validation (no pi needed)
// ============================================

#[tokio::test]
async fn test_openai_chat_completions_requires_auth() {
    let (app, _db_url) = create_test_app().await;

    let resp = app
        .post("/v1/chat/completions")
        .json(&json!({
            "model": "anything",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "no auth → 401");
}

#[tokio::test]
async fn test_openai_chat_completions_empty_messages_is_400() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    // A valid bearer gets past auth; the empty `messages` array
    // is a client error → 400 with the OpenAI envelope.
    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "coder-agent",
            "messages": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "empty messages → 400");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_request_error");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("at least one message"),
        "message should explain the empty-messages error: {}",
        body["error"]["message"]
    );
}

#[tokio::test]
async fn test_openai_chat_completions_last_message_must_be_user() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    // Ending on an assistant message is not a valid chat prompt
    // for a forge agent → 400.
    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "coder-agent",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "last message not user → 400");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("user message"),
        "message should explain the last-message error: {}",
        body["error"]["message"]
    );
}

#[tokio::test]
async fn test_openai_chat_completions_unknown_profile_is_404() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    // `model` is a profile name that doesn't exist → 404, before
    // any session is created or pi is spawned.
    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "no-such-profile",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown profile → 404");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn test_openai_chat_completions_malformed_session_model_is_404() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    // `forge:not-a-uuid` is the stateful form but with a
    // non-parseable session id → ModelNotFound (404), not a 500.
    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "forge:not-a-uuid",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "malformed forge: session model → 404");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn test_openai_chat_completions_nonexistent_session_is_404() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    // `forge:<valid-uuid-not-in-db>` parses but the session row
    // doesn't exist → SessionNotFound (404).
    let bogus_session = uuid::Uuid::new_v4().to_string();
    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": format!("forge:{}", bogus_session),
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "nonexistent session → 404");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn test_openai_chat_completions_accepts_unknown_fields() {
    // OpenAI clients send a lot of fields forge ignores
    // (`temperature`, `top_p`, `n`, `presence_penalty`, …). The
    // request must not 422 for them. We drive this to the
    // model-not-found error (a 404, past deserialization) to
    // prove the body parsed successfully with the extra fields.
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "no-such-profile",
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 0.7,
            "top_p": 0.9,
            "n": 1,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "stream": false,
            "user": "test-user"
        }))
        .send()
        .await
        .unwrap();
    // 404 (model not found) means the body deserialized fine and
    // we reached model resolution; a 400/422 would mean we
    // rejected the extra fields.
    assert_eq!(
        resp.status(),
        404,
        "extra fields should be accepted, not 422"
    );
}

#[tokio::test]
async fn test_openai_chat_completions_x_api_key_accepted() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;

    // The native X-API-Key header should authenticate the OpenAI
    // endpoint too. We drive to the empty-messages 400 to prove
    // auth passed (a 401 would mean it didn't).
    let resp = app
        .post("/v1/chat/completions")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "model": "coder-agent",
            "messages": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "X-API-Key should authenticate; 400 means auth passed and we hit validation"
    );
}

// ============================================
// Agent-turn plumbing — needs `pi` on PATH, no provider key
// ============================================

/// Verify the OpenAI path runs the agent turn end-to-end (auth →
/// model resolution → session creation → pi spawn → turn loop →
/// error handling) without a provider key. With no key, pi emits
/// a `response` envelope with `success: false` ("No API key found …")
/// and `run_agent_turn` surfaces it as a 500 immediately (see the
/// `PiEvent::Response` arm in `api/openai.rs`) instead of hanging
/// for the idle timeout. A 500 here proves the request made it
/// all the way into the turn loop; a 400/401/404 would mean it
/// failed earlier (validation / auth / model resolution). Needs
/// `pi` on PATH + the built extension (CI installs both); does
/// **not** need a paid provider key.
#[tokio::test]
async fn test_openai_chat_completions_runs_agent_turn() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;
    create_profile(&app, &api_key, "plumbing-agent").await;

    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "plumbing-agent",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();

    // 500 = the turn ran and pi reported a failure (no key). Not
    // 400/401/404 (those would mean the request never reached pi).
    assert_eq!(
        resp.status(),
        500,
        "no-key request should reach the agent turn and fail with 500, not fail earlier"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"].is_object(),
        "should be an OpenAI error envelope"
    );
    assert_eq!(body["error"]["code"], "internal_error");
    // The message should mention the API key — proving pi actually
    // ran and reported the failure (vs. a forge-internal error).
    let msg = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        msg.to_lowercase().contains("api key"),
        "error should mention the missing API key (proves the turn ran); got: {}",
        msg
    );
}

// ============================================
// Full happy path — needs `pi` + a paid provider key
// ============================================

/// End-to-end happy path: a valid profile + a real user message
/// drives the agent for one turn and returns an OpenAI-shaped
/// `chat.completion` with the model's text. This genuinely needs
/// a configured provider API key (the profile's `api_key` or the
/// matching `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` env var) to get
/// a 200 — without one the turn fails with the 500 plumbed above.
/// CI doesn't have a paid key (and shouldn't), so this stays
/// `#[ignore]`'d; run it locally with `cargo test -- --ignored`
/// against a real provider. The plumbing test above covers the
/// CI-runnable portion.
#[tokio::test]
#[ignore = "requires the `pi` agent binary + a configured provider API key; run with --ignored"]
async fn test_openai_chat_completions_end_to_end() {
    let (app, _db_url) = create_test_app().await;
    let api_key = register_and_login(&app).await;
    create_profile(&app, &api_key, "e2e-agent").await;

    let resp = app
        .post("/v1/chat/completions")
        .header("Authorization", &format!("Bearer {}", api_key))
        .json(&json!({
            "model": "e2e-agent",
            "messages": [{"role": "user", "content": "Say hello in one word."}]
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["model"], "e2e-agent");
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert!(
        body["choices"][0]["message"]["content"].is_string(),
        "content should be a string"
    );
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert!(body["usage"]["total_tokens"].is_i64());
}
