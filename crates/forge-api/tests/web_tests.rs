//! Integration tests for the web UI surface: the OpenAI-compatible
//! STT/TTS proxies (`/v1/audio/*`) and the static-file fallback that
//! serves the SPA.
//!
//! The voice *backends* (Parakeet :5093, Kokoro :8766) live on the
//! lab LAN and aren't reachable from CI, so these tests pin the
//! *contract* the web UI depends on rather than a live round-trip:
//! - `/v1/audio/voices` is always 200 with a catalog shape
//!   (`{stt, tts, default_voice, voices}`), so the UI can degrade
//!   gracefully when a backend is down.
//! - The POST endpoints are auth-gated and reject malformed input
//!   before touching the network (400 on empty/non-JSON body, 401
//!   with no key).
//! - `GET /` serves `index.html` when a web dir is configured (the
//!   SPA fallback), and an unmatched API path under a web dir
//!   falls back to `index.html` too (so deep links work).

mod test_helpers;

use serde_json::json;
use test_helpers::TestApp;

async fn create_test_app() -> (TestApp, String) {
    TestApp::new().await
}

async fn register_and_login(app: &TestApp) -> (String, String) {
    app.post("/auth/register")
        .json(&json!({"email": "voice@example.com", "name": "Voice Tester", "password": "password123"}))
        .send().await.unwrap();
    let resp = app
        .post("/auth/login")
        .json(&json!({"email": "voice@example.com", "password": "password123"}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    (
        body["user"]["id"].as_str().unwrap().to_string(),
        body["api_key"].as_str().unwrap().to_string(),
    )
}

// ============================================================
// /v1/audio/voices — availability + catalog (always 200)
// ============================================================

#[tokio::test]
async fn test_voices_requires_auth() {
    let (app, _db) = create_test_app().await;
    // No API key -> 401 (auth_middleware gates /v1/*).
    let resp = app.get("/v1/audio/voices").send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_voices_returns_catalog_shape() {
    let (app, _db) = create_test_app().await;
    let (_uid, key) = register_and_login(&app).await;

    let resp = app
        .get("/v1/audio/voices")
        .header("X-API-Key", &key)
        .send()
        .await
        .unwrap();
    // Always 200 — even when both backends are unreachable (CI),
    // the UI relies on this to decide whether to show mic/speaker.
    assert_eq!(
        resp.status(),
        200,
        "voices must be 200 even when backends are down"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body.get("stt").is_some() && body["stt"].is_boolean(),
        "missing stt boolean: {body}"
    );
    assert!(
        body.get("tts").is_some() && body["tts"].is_boolean(),
        "missing tts boolean: {body}"
    );
    assert!(
        body.get("default_voice").is_some() && body["default_voice"].is_string(),
        "missing default_voice string: {body}"
    );
    let voices = body["voices"]
        .as_array()
        .expect("voices should be an array");
    assert!(!voices.is_empty(), "voice catalog should be non-empty");
    // af_heart is Kokoro's flagship default; the curated list
    // always includes it so the UI has a sane default even before
    // probing the live backend.
    assert!(
        voices.iter().any(|v| v == "af_heart"),
        "catalog should include af_heart: {voices:?}"
    );
}

// ============================================================
// /v1/audio/speech — TTS proxy (auth + input validation)
// ============================================================

#[tokio::test]
async fn test_speech_requires_auth() {
    let (app, _db) = create_test_app().await;
    let resp = app
        .post("/v1/audio/speech")
        .json(&json!({"model": "kokoro", "input": "hi", "voice": "af_heart"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_speech_rejects_empty_body_with_400() {
    // Empty body is rejected before any upstream call, so this is
    // deterministic regardless of whether Kokoro is reachable.
    let (app, _db) = create_test_app().await;
    let (_uid, key) = register_and_login(&app).await;
    let resp = app
        .post("/v1/audio/speech")
        .header("X-API-Key", &key)
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "empty body should be 400");
}

#[tokio::test]
async fn test_speech_rejects_non_json_body_with_400() {
    let (app, _db) = create_test_app().await;
    let (_uid, key) = register_and_login(&app).await;
    let resp = app
        .post("/v1/audio/speech")
        .header("X-API-Key", &key)
        .header("Content-Type", "application/json")
        .body("not-json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "non-JSON body should be 400");
}

// ============================================================
// /v1/audio/transcriptions — STT proxy (auth + file presence)
// ============================================================

#[tokio::test]
async fn test_transcribe_requires_auth() {
    let (app, _db) = create_test_app().await;
    // A multipart body with no key -> 401.
    let form = "--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.webm\"\r\n\r\nbytes\r\n--b--\r\n";
    let resp = app
        .post("/v1/audio/transcriptions")
        .header("Content-Type", "multipart/form-data; boundary=b")
        .body(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_transcribe_rejects_no_file_with_400() {
    // A multipart body with no `file` field is rejected before
    // any upstream call. Deterministic in CI.
    let (app, _db) = create_test_app().await;
    let (_uid, key) = register_and_login(&app).await;
    let form = "--b\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nparakeet\r\n--b--\r\n";
    let resp = app
        .post("/v1/audio/transcriptions")
        .header("X-API-Key", &key)
        .header("Content-Type", "multipart/form-data; boundary=b")
        .body(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "missing file field should be 400");
}

// ============================================================
// Static file serving (the SPA fallback)
// ============================================================

/// Resolve the repo's `web/` dir the same way `main.rs` does, so
/// these tests exercise the real static-serving path. Falls back
/// to a throwaway stub dir if the repo web/ isn't present
/// (shouldn't happen in CI, but keeps the test self-contained).
fn web_dir_for_test() -> std::path::PathBuf {
    if let Some(dir) = forge_api::api::resolve_web_dir() {
        return dir;
    }
    let tmp = std::env::temp_dir().join("forge_web_test_stub");
    std::fs::create_dir_all(&tmp).ok();
    std::fs::write(
        tmp.join("index.html"),
        "<!doctype html><html><body>stub Forge</body></html>",
    )
    .ok();
    std::fs::write(tmp.join("styles.css"), "body{}").ok();
    tmp
}

#[tokio::test]
async fn test_get_root_serves_index_html() {
    // `GET /` (no API route matches) falls through to ServeDir
    // and returns the SPA index.html. Asserts the wiring the web
    // UI's "open the app" flow depends on.
    let (app, _db) = TestApp::with_web_dir(web_dir_for_test()).await;
    let resp = app.get("/").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text();
    assert!(body.contains("<html"), "root should serve HTML: {body}");
    assert!(
        body.contains("Forge"),
        "root should serve the Forge index: {body}"
    );
}

#[tokio::test]
async fn test_spa_deep_link_falls_back_to_index() {
    // A deep link like /chat/<uuid> isn't an API route and isn't
    // a real file; ServeDir's not_found_service serves index.html
    // so the client-side router can take over.
    let (app, _db) = TestApp::with_web_dir(web_dir_for_test()).await;
    let resp = app
        .get("/chat/00000000-0000-0000-0000-000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text();
    assert!(
        body.contains("<html"),
        "deep link should fall back to index.html: {body}"
    );
}

#[tokio::test]
async fn test_static_asset_served() {
    // A real file (styles.css) is served with 200 from ServeDir.
    let (app, _db) = TestApp::with_web_dir(web_dir_for_test()).await;
    let resp = app.get("/styles.css").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text();
    assert!(!body.is_empty(), "styles.css should be served: {body}");
}

// ============================================================
// Embedded UI (the deployed-binary path: no web dir resolved)
//
// A deployed `/opt/forge/forge-api` has no CARGO_MANIFEST_DIR and
// the host env file doesn't set FORGE_WEB_DIR, so `resolve_web_dir`
// returns None and `build_app(state, None)` falls back to the
// compile-time-embedded assets (`api::web::embedded_spa`). These
// tests pin that path — the one the forge host actually uses.
// ============================================================

#[tokio::test]
async fn test_embedded_root_serves_index_html() {
    // `new()` = build_app(state, None) -> embedded SPA. This is
    // the deployed-binary case: the UI must be served with zero
    // external files / env config.
    let (app, _db) = TestApp::new().await;
    let resp = app.get("/").send().await.unwrap();
    assert_eq!(resp.status(), 200, "embedded root should be 200");
    let body = resp.text();
    assert!(
        body.contains("<!doctype html>"),
        "embedded root should serve HTML: {body}"
    );
    assert!(
        body.contains("Forge"),
        "embedded root should serve the Forge index: {body}"
    );
}

#[tokio::test]
async fn test_embedded_deep_link_falls_back_to_index() {
    let (app, _db) = TestApp::new().await;
    let resp = app
        .get("/chat/00000000-0000-0000-0000-000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "embedded deep link should fall back to index.html (200)"
    );
    let body = resp.text();
    assert!(
        body.contains("<!doctype html>"),
        "deep link should fall back to index.html: {body}"
    );
}

#[tokio::test]
async fn test_embedded_asset_served_with_correct_content_type() {
    let (app, _db) = TestApp::new().await;
    let resp = app.get("/styles.css").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers.get("content-type").unwrap(),
        "text/css; charset=utf-8",
        "embedded CSS should have the right Content-Type"
    );
    assert!(
        resp.text().contains("--bg"),
        "embedded styles.css should be the real file"
    );
}

#[tokio::test]
async fn test_embedded_app_js_served() {
    // The SPA's logic lives in app.js; make sure it's reachable on
    // the embedded path with a JS content-type (the manifest
    // references it, and the service worker caches it).
    let (app, _db) = TestApp::new().await;
    let resp = app.get("/app.js").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers.get("content-type").unwrap();
    assert!(
        ct.to_str().unwrap().contains("javascript"),
        "app.js should be JS: {ct:?}"
    );
    assert!(
        resp.text().contains("forge.apiKey"),
        "embedded app.js should be the real file"
    );
}
