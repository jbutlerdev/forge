//! Herd H6.5: the `web_login` tool + browser-handoff broker.
//!
//! The full flow (see `docs/design/agent-builder.md` §7 in ranch-2):
//!
//! 1. The agent's harness calls `web_login(url, slot)` — relayed as
//!    `POST /sessions/:id/web-login`. The session tenancy is checked,
//!    the URL and slot validated, and a PENDING ROW is inserted into
//!    the SAME `RanchToolQueue` the H3.5 policy lane uses, with kind
//!    `"web_login"`. A `ranch_tool_request` event publishes the card
//!    (`{url, slot}` — no credential material, ever) on the session's
//!    stream; ranchd's forge worker turns it into an `AgentAsk`
//!    ("Sign in to <url>?" / choices `Sign me in` | `Cancel`).
//! 2. The long-poll is bounded by `RANCH_TOOL_TIMEOUT` (60 s), like
//!    the policy-ask lane. Approval (`POST /ranch-tools/:id/result`,
//!    `success: true`) → the broker mints a ONE-TIME token, records a
//!    pending handoff (30 min TTL), and opens the handoff page in the
//!    USER'S BROWSER (`xdg-open`; on a headless box the URL is
//!    returned so the model can relay it to the user).
//! 3. The handoff page (`GET /auth/browser-handoff?token=…`) is served
//!    by forge with NO API key — the one-time token IS the credential
//!    (same posture as mule's job-id voice sockets). The user signs in
//!    to the target site in that browser, then pastes the resulting
//!    session cookie into the page's form.
//! 4. `POST /auth/browser-handoff` exchanges the token for the slot:
//!    the cookie lands in the `secrets` table under `(owner, slot)`
//!    and the token is consumed (one use, 404 on reuse/unknown).
//! 5. The agent's tool returns "signed in" / "still waiting" via
//!    `GET /sessions/:id/web-login?slot=` — it NEVER returns or
//!    receives the cookie value, so no credential material reaches
//!    the model context, a `messages` row, or an approval card.
//!
//! What lands in the sandbox: `resolve_credential_env` (credentials.rs)
//! resolves the agent's `credentials_scope.env_refs` at exec time, so
//! after step 4 the agent's bash calls see `$<slot>`.

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::api::auth::AuthenticatedUser;
use crate::api::ranch_tools::RANCH_TOOL_TIMEOUT;
use crate::api::{err_resp, AppState};

/// One in-flight sign-in handoff. The token is the only credential:
/// it is not an API key, it expires, and it is single-use.
struct PendingHandoff {
    owner_id: Uuid,
    slot: String,
    url: String,
    expires_at: Instant,
}

/// The one-time-token store. In-memory (like the ranch queue): a forge
/// restart orphans at most in-flight sign-ins, and the bounded
/// long-poll turns them into tool errors the agent can retry.
#[derive(Default)]
pub struct WebLoginStore {
    pending: Mutex<HashMap<String, PendingHandoff>>,
}

impl WebLoginStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn insert(&self, token: &str, handoff: PendingHandoff) {
        let now = Instant::now();
        let mut map = self.pending.lock().unwrap();
        map.retain(|_, h| h.expires_at > now);
        map.insert(token.to_string(), handoff);
    }

    /// Consume the token; returns the pending handoff when the token
    /// is live (removed, so a second use is a 404).
    fn take(&self, token: &str) -> Option<PendingHandoff> {
        let now = Instant::now();
        let mut map = self.pending.lock().unwrap();
        map.retain(|_, h| h.expires_at > now);
        map.remove(token)
    }

    /// A live (unexpired) handoff exists for this owner+slot.
    fn has_live_for_slot(&self, owner_id: Uuid, slot: &str) -> bool {
        let now = Instant::now();
        self.pending
            .lock()
            .unwrap()
            .values()
            .any(|h| h.expires_at > now && h.owner_id == owner_id && h.slot == slot)
    }

    /// A live (unexpired) handoff exists for this token.
    pub(crate) fn has_live_for_token(&self, token: &str) -> bool {
        let now = Instant::now();
        self.pending
            .lock()
            .unwrap()
            .get(token)
            .is_some_and(|h| h.expires_at > now)
    }
}

/// The handoff page base: `FORGE_PUBLIC_URL` when set (the URL the
/// user's browser can actually reach — e.g. through a tunnel on a
/// remote box), else the loopback default.
fn public_base() -> String {
    std::env::var("FORGE_PUBLIC_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

/// Minimal query-string percent-encoding (the `next=` param). No new
/// dependency: unreserved chars pass, everything else is `%XX`.
fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The handoff page template (see `browser_handoff_page`). `__NEXT__`
/// is replaced twice (link text + href), `__TOKEN__` once. The JS
/// POSTs `{token, value}` to `/auth/browser-handoff` — the value goes
/// straight to the secret store and is never shown to the agent.
const HANDOFF_PAGE_TEMPLATE: &str = r#"<!doctype html><html><head><meta charset="utf-8"><title>Sign-in handoff</title></head>
<body style="font-family:system-ui;background:#101418;color:#e5e5e5;max-width:560px;margin:40px auto">
<h2>Agent sign-in handoff</h2>
<p>Step 1 — sign in to <a href="__NEXT__" style="color:#7fd4ff">__NEXT__</a> in this
browser (you may need to open it in a new tab). The agent will NOT see your
password or any fields on that page.</p>
<p>Step 2 — copy the session cookie for that site (DevTools →
Application → Cookies, the long value) and paste it below. It goes
straight to this machine's secret store and is <b>never</b> shown to
the agent.</p>
<form onsubmit="return post(event)">
<input type="password" id="v" autocomplete="off"
 style="width:100%;box-sizing:border-box;padding:8px" required>
<p id="st" style="color:#9aa0a6">Paste the cookie value and press Submit.</p>
</form>
<script>
async function post(e){e.preventDefault();
 const v=document.getElementById('v').value;const st=document.getElementById('st');
 const r=await fetch('/auth/browser-handoff',{method:'POST',
  headers:{'Content-Type':'application/json'},
  body:JSON.stringify({token:'__TOKEN__',value:v})});
 const j=await r.json();
 st.textContent=r.ok?'Done — the agent is now signed in. You can close this tab.':'Error: '+(j.error||r.status);
 st.style.color=r.ok?'#7fd4ff':'#f59e0b';}
</script></body></html>"#;

/// Body of `POST /sessions/:id/web-login` (from the harness tool).
#[derive(Deserialize)]
pub struct WebLoginBody {
    pub url: String,
    pub slot: String,
}

/// The harness-side `web_login` tool's relay point. See the module
/// docs for the flow; the card payload is `{url, slot}` ONLY.
pub(crate) async fn session_web_login(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(session_id): Path<Uuid>,
    Json(body): Json<WebLoginBody>,
) -> Response {
    // Tenancy: the session must exist and belong to the caller (or
    // admin) — the `policy_ask` shape.
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    let Some(owner) = owner else {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    };
    if !crate::api::auth::can_access(&user, Some(owner)) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    if !body.url.starts_with("http://") && !body.url.starts_with("https://") {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "url must be an absolute http(s) URL",
        );
    }
    if !crate::api::secrets::valid_slot_name(&body.slot) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "slot must match ^[A-Z][A-Z0-9_]*$ (max 64 chars)",
        );
    }

    let (tx, rx) = oneshot::channel();
    let id = state.ranch_tools.insert_meta(
        session_id,
        tx,
        Some("web_login".into()),
        json!({ "url": body.url, "slot": body.slot }),
    );

    // The card the ranch lane renders — `{url, slot}` only. The
    // grep-audit test (tests/herd_h65_tests.rs) asserts no credential
    // material ever appears in this payload or in any message row.
    let payload = json!({
        "id": id,
        "session_id": session_id,
        "tool": "web_login",
        "input": { "url": body.url, "slot": body.slot },
    });
    state
        .bus
        .publish_ranch_tool_request(session_id, payload.clone());

    tracing::info!(
        session_id = %session_id,
        slot = %body.slot,
        url = %body.url,
        id = %id,
        "web login: pending (ranch approval round-trip)"
    );

    match tokio::time::timeout(RANCH_TOOL_TIMEOUT, rx).await {
        Ok(Ok(result)) if result.success => {
            // Approval → mint the one-time token + open the user's
            // browser at the handoff page (best-effort; the URL is
            // always returned so the model can relay it on a headless
            // box where xdg-open is absent).
            let token = Uuid::new_v4().simple().to_string();
            let handoff_url = format!(
                "{}/auth/browser-handoff?token={}&next={}",
                public_base(),
                token,
                url_encode(&body.url)
            );
            state.web_logins.insert(
                &token,
                PendingHandoff {
                    owner_id: owner,
                    slot: body.slot.clone(),
                    url: body.url.clone(),
                    expires_at: Instant::now() + Duration::from_secs(30 * 60),
                },
            );
            if let Ok(exe) = which_xdg_open() {
                let _ = std::process::Command::new(exe)
                    .arg(&handoff_url)
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn();
            }
            tracing::info!(
                session_id = %session_id,
                slot = %body.slot,
                "web login approved; handoff page issued"
            );
            (
                StatusCode::OK,
                Json(json!({
                    "status": "approved",
                    "handoff_url": handoff_url,
                    "slot": body.slot,
                })),
            )
                .into_response()
        }
        Ok(Ok(_)) => {
            // The user tapped "Cancel" (ranchd POSTs success:false).
            tracing::info!(session_id = %session_id, "web login cancelled by user");
            (StatusCode::OK, Json(json!({ "status": "denied" }))).into_response()
        }
        Ok(Err(_)) | Err(_) => {
            tracing::warn!(session_id = %session_id, "web login: no decision (timeout or queue loss)");
            (StatusCode::OK, Json(json!({ "status": "expired" }))).into_response()
        }
    }
}

fn which_xdg_open() -> Result<std::path::PathBuf, ()> {
    let path = match std::env::var_os("PATH") {
        Some(p) if !p.is_empty() => p,
        _ => return Err(()),
    };
    for dir in std::env::split_paths(&path) {
        let cand = dir.join("xdg-open");
        if cand.is_file() {
            return Ok(cand);
        }
    }
    Err(())
}

#[derive(Deserialize)]
pub(crate) struct HandoffQuery {
    token: String,
    next: Option<String>,
}

/// The handoff page. Token is the credential — no API key, no auth
/// header. Served for ANY host header; operators keep forge loopback
/// or tunnel-gated (documented). Unknown/expired token → 404.
pub(crate) async fn browser_handoff_page(
    State(state): State<AppState>,
    Query(q): Query<HandoffQuery>,
) -> Response {
    let Some(next) = q.next.filter(|s| !s.is_empty()) else {
        return err_resp(&state, StatusCode::NOT_FOUND, "handoff expired");
    };
    if !state.web_logins.has_live_for_token(&q.token) {
        return err_resp(&state, StatusCode::NOT_FOUND, "handoff expired");
    }
    let token = url_encode(&q.token);
    // The page's JS contains literal braces, so the two dynamic bits
    // are placeholder-replaced rather than `format!`-interpolated.
    let page = HANDOFF_PAGE_TEMPLATE
        .replace("__NEXT__", &next)
        .replace("__TOKEN__", &token);
    (StatusCode::OK, page).into_response()
}

#[derive(Deserialize)]
pub struct HandoffPostBody {
    pub token: String,
    pub value: String,
}

/// The token → secret exchange. Single-use; the value never logs.
pub(crate) async fn browser_handoff_post(
    State(state): State<AppState>,
    Json(body): Json<HandoffPostBody>,
) -> Response {
    let Some(handoff) = state.web_logins.take(&body.token) else {
        return err_resp(
            &state,
            StatusCode::NOT_FOUND,
            "handoff expired or already completed",
        );
    };
    if body.value.trim().is_empty() {
        return err_resp(&state, StatusCode::BAD_REQUEST, "value must be non-empty");
    }
    match crate::credentials::set_secret(
        &state.db,
        handoff.owner_id,
        &handoff.slot,
        body.value.trim(),
        None,
    )
    .await
    {
        Ok(()) => {
            tracing::info!(
                slot = %handoff.slot,
                url = %handoff.url,
                "web login handoff completed: credential stored"
            );
            Json(serde_json::json!({ "ok": true })).into_response()
        }
        Err(e) => {
            tracing::error!(slot = %handoff.slot, "web login handoff: store failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to store the credential",
            )
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct StatusQuery {
    slot: String,
}

/// The harness-side `web_login_done(slot)` probe: has the handoff
/// happened? `pending` (approved, waiting for the cookie),
/// `signed_in` (a live value for the slot), or `none`.
pub(crate) async fn web_login_status(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(session_id): Path<Uuid>,
    Query(q): Query<StatusQuery>,
) -> Response {
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    let Some(owner) = owner else {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    };
    if !crate::api::auth::can_access(&user, Some(owner)) {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    if state.web_logins.has_live_for_slot(owner, &q.slot) {
        return Json(serde_json::json!({ "status": "pending" })).into_response();
    }
    match crate::credentials::resolve_secret(&state.db, owner, &q.slot).await {
        Ok(Some(_)) => Json(serde_json::json!({ "status": "signed_in" })).into_response(),
        Ok(None) => Json(serde_json::json!({ "status": "none" })).into_response(),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to check sign-in status",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encode_roundtrip_shape() {
        assert_eq!(
            url_encode("https://x.io/login"),
            "https%3A%2F%2Fx.io%2Flogin"
        );
        assert_eq!(url_encode("abc-_.~123"), "abc-_.~123");
    }

    #[tokio::test]
    async fn handoff_token_is_single_use_and_slot_scoped() {
        let store = WebLoginStore::new();
        let owner = Uuid::new_v4();
        store.insert(
            "t1",
            PendingHandoff {
                owner_id: owner,
                slot: "SLOT_A".into(),
                url: "https://x.io".into(),
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );
        assert!(store.has_live_for_slot(owner, "SLOT_A"));
        assert!(!store.has_live_for_slot(owner, "SLOT_B"));
        assert!(store.has_live_for_token("t1"));
        assert!(store.take("t1").is_some());
        // single-use: the second take is None
        assert!(store.take("t1").is_none());
        assert!(!store.has_live_for_slot(owner, "SLOT_A"));
    }
}
