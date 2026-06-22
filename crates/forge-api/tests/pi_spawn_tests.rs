//! pi subprocess spawn smoke tests.
//!
//! These tests spawn a real `pi` process (via `PiAgent::spawn`,
//! the same path the harness uses) and verify pi starts up and
//! emits its first RPC event. They need the `pi` binary on PATH
//! and the built `forge-tools` extension at
//! `extensions/forge-tools/dist/index.js`, but they do **not**
//! need a provider API key — the first event (pi's `response`
//! envelope reporting "No API key found …") arrives before any
//! LLM call, so the test proves the spawn flags are correct
//! without making a paid request.
//!
//! This is the test that catches the `--skills-dir` → `--skill`
//! regression class: if `pi_agent.rs` passes a flag pi doesn't
//! recognize (or points at a missing extension), pi exits
//! immediately, `read_line` returns `None` (EOF), and the test
//! fails. The native `/messages` handler returns 202 *before*
//! detecting a spawn-time crash (the harness runs in a background
//! task), so `test_send_message` can't catch this class of bug —
//! this direct-spawn test can.
//!
//! CI installs `pi` and builds the extension in the `rust-test`
//! job (see `.github/workflows/ci.yml`). Locally, `pi` must be on
//! PATH and `extensions/forge-tools` must be built
//! (`cd extensions/forge-tools && npm run build`) — both are
//! documented prerequisites.

use forge_api::pi_agent::{PiAgent, PiConfig};
use std::path::PathBuf;
use std::time::Duration;

/// Resolve a path to a file/dir in the repo root from the test
/// crate's working directory. `cargo test` runs each test binary
/// with `cwd = <crate-dir>` (`crates/forge-api/`), so the repo
/// root is two levels up.
fn repo_root() -> PathBuf {
    std::env::current_dir()
        .expect("cwd")
        .join("../..")
        .canonicalize()
        .expect("repo root should canonicalize")
}

/// Build a minimal `PiConfig` pointing at the repo-bundled
/// extension + skills, with no provider key. pi will spawn, load
/// the extension, and — when prompted — emit a `response` event
/// reporting the missing key. That first event is all the test
/// needs; it never makes an LLM call.
fn minimal_config() -> PiConfig {
    let repo = repo_root();
    let ext = repo.join("extensions/forge-tools/dist/index.js");
    let skills = repo.join("skills");
    assert!(
        ext.exists(),
        "forge-tools extension not built: {}. Run `cd extensions/forge-tools && npm run build`.",
        ext.display()
    );
    assert!(
        skills.is_dir(),
        "skills directory missing: {}",
        skills.display()
    );
    PiConfig {
        working_dir: std::env::temp_dir().to_string_lossy().to_string(),
        provider: "anthropic".into(),
        model: "claude-sonnet-4-20250514".into(),
        base_url: None,
        api_key: None,
        system_prompt: "You are a test agent.".into(),
        forge_tools_extension: ext,
        forge_api_url: "http://localhost:8080".into(),
        session_id: uuid::Uuid::new_v4(),
        session_path: None,
        skills_dir: Some(skills),
    }
}

/// `PiAgent::spawn` must start pi and pi must emit at least one
/// event on stdout after a prompt. EOF on the first read means pi
/// crashed at startup — the signature of a bad CLI flag (e.g. the
/// `--skills-dir` regression, which pi 0.79.x rejects with
/// "Unknown option") or a missing extension.
#[tokio::test]
async fn pi_spawn_emits_first_event() {
    let mut agent = PiAgent::spawn(minimal_config())
        .await
        .expect("PiAgent::spawn should start the pi process");

    // pi only emits events after it receives a prompt on stdin.
    agent
        .send_message("hi")
        .await
        .expect("send_message should write the prompt to pi's stdin");

    // The first event arrives quickly (pi loads the extension,
    // parses the prompt, then emits its RPC response). 30s is
    // generous for a cold node start; the real arrival is
    // sub-second on a warm machine.
    let line = tokio::time::timeout(Duration::from_secs(30), agent.read_line())
        .await
        .expect("timed out waiting for pi's first event (30s)")
        .expect("read_line should not error");

    assert!(
        line.is_some(),
        "pi emitted EOF instead of an event — it crashed at startup. \
         This is the signature of a bad CLI flag (e.g. --skills-dir, \
         which pi 0.79.x renamed to --skill) or a missing extension. \
         Check pi_agent.rs's spawn args."
    );

    // The first line must be valid JSON (a pi event). A non-JSON
    // line would mean pi wrote a banner / error to stdout instead
    // of the RPC event stream.
    let raw = line.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|e| {
        panic!("pi's first stdout line was not valid JSON: {e}\nline: {raw:?}")
    });
    assert!(
        parsed.get("type").is_some(),
        "pi event should have a `type` field; got: {raw}"
    );

    let _ = agent.kill().await;
}

/// When `skills_dir` is `None`, pi is launched with just
/// `--no-skills` and must still spawn + emit an event. Pins the
/// no-skills branch so a future change to the skills-flag logic
/// can't break the minimal path.
#[tokio::test]
async fn pi_spawn_emits_first_event_no_skills() {
    let mut cfg = minimal_config();
    cfg.skills_dir = None;
    let mut agent = PiAgent::spawn(cfg)
        .await
        .expect("PiAgent::spawn should start the pi process");
    agent.send_message("hi").await.expect("send prompt");
    let line = tokio::time::timeout(Duration::from_secs(30), agent.read_line())
        .await
        .expect("timed out waiting for pi's first event (30s)")
        .expect("read_line should not error");
    assert!(
        line.is_some(),
        "pi emitted EOF with no skills dir — it crashed at startup"
    );
    let _ = agent.kill().await;
}
