//! Sandbox package governance API (2026-10-18).
//!
//! Agents ask for tools via `request_sandbox_packages` (harness tool →
//! these endpoints); each request is a pending row (one per package).
//! The herd's designated approver agent — `FORGE_SANDBOX_APPROVER`
//! (agent name or uuid; the lab runs the "executive assistant") — is
//! notified in its own conversation and may `approve` / `reject` /
//! `escalate` (escalate = `@notify:` the user, who then decides in the
//! mobile Sandbox section, which speaks the same endpoints via the
//! ranch daemon's Sandbox* frames).
//!
//! Approval is *persistent*: [`SandboxManager::ensure_packages`]
//! reconciles every sandboxed bash exec against the approved set, so
//! installs survive session death and sandbox resets.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use super::agents::agent_access_err;
use super::{dispatch_message, err_resp, AppState};
use crate::api::auth::AuthenticatedUser;

/// Names Debian-ish: lowercase start, then lowercase/digits and the
/// packaging punctuation. Deliberately strict — the string is later
/// interpolated into an `apt-get install` argv inside a chroot, so no
/// slashes, spaces, globs, or `=` version pins.
fn valid_pkg(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes.len() > 64 {
        return false;
    }
    let first = bytes[0];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return false;
    }
    s.chars().all(|c| {
        c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '+' | '.' | '_' | '-')
    })
}

/// Packages that would break the sandbox's isolation guarantees or
/// the host (privilege tooling, container/sandbox escape primitives,
/// kernel/system plumbing). The reconciler refuses them at REQUEST
/// time so they never reach an approval card.
const DENYLIST: &[&str] = &[
    "openssh-server", "sudo", "su-exec", "docker.io", "docker-cli", "podman", "runc", "systemd",
    "systemd-sysv", "udev", "netcat", "netcat-openbsd", "netcat-traditional", "nmap", "hping3",
    "tcpdump", "strace", "ltrace", "debootstrap", "fakechroot", "proot", "bubblewrap", "uidmap",
    "gidmap", "apt", "apt-utils", "gnupg", "gnupg2", "passwd", "adduser", "login", "util-linux",
    "e2fsprogs", "kmod", "iptables", "nftables", "apparmor", "selinux-utils", "cron", "anacron",
];

fn validate_pkg(name: &str) -> Option<String> {
    let n = name.trim().to_ascii_lowercase();
    if !valid_pkg(&n) {
        return Some("invalid package name (lowercase, digits, +._- only, 2-64 chars)".into());
    }
    if DENYLIST.contains(&n.as_str()) {
        return Some("package is not allowed in sandboxes (isolation policy)".into());
    }
    None
}

/// Resolve `FORGE_SANDBOX_APPROVER` (agent name or uuid) to an agent
/// row. `None` when unset/unresolvable — requests then queue for the
/// user alone (mobile Sandbox section), with no EA notification.
async fn resolve_approver(state: &AppState) -> Option<serde_json::Value> {
    let cfg = std::env::var("FORGE_SANDBOX_APPROVER").ok()?;
    let cfg = cfg.trim().to_string();
    if cfg.is_empty() {
        return None;
    }
    let rows: Vec<(Uuid, String)> = if cfg.len() == 36 && Uuid::parse_str(&cfg).is_ok() {
        sqlx::query_as("SELECT id, name FROM agents WHERE id = $1 AND paused = false")
            .bind(Uuid::parse_str(&cfg).ok()?)
            .fetch_all(&state.db)
            .await
            .ok()?
    } else {
        sqlx::query_as(
            "SELECT id, name FROM agents WHERE lower(trim(name)) = lower($1) AND paused = false \
             ORDER BY created_at LIMIT 1",
        )
        .bind(&cfg)
        .fetch_all(&state.db)
        .await
        .ok()?
    };
    rows.first().map(|(id, name)| json!({"id": id, "name": name}))
}

async fn latest_conversation(state: &AppState, agent_id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM sessions WHERE agent_id = $1 ORDER BY last_active DESC NULLS LAST LIMIT 1",
    )
    .bind(agent_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten()
}

/// True when `user` IS the configured approver agent.
async fn is_approver(state: &AppState, user: &AuthenticatedUser) -> bool {
    let (Some(aid), Some(appr)) = (user.agent_id, resolve_approver(state).await) else {
        return false;
    };
    appraisal_match(&appr, aid)
}

fn appraisal_match(appraisal: &serde_json::Value, agent_id: Uuid) -> bool {
    appraisal.get("id").and_then(|v| v.as_str()) == Some(agent_id.to_string().as_str())
}

// ============================================
// POST /agents/:id/sandbox/requests
// ============================================

#[derive(Deserialize)]
pub(crate) struct SandboxRequestPath {
    id: Uuid,
}

#[derive(Deserialize)]
pub(crate) struct RequestPackagesBody {
    packages: Vec<String>,
    #[serde(default)]
    reason: Option<String>,
}

pub(crate) async fn request_packages(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(params): Path<SandboxRequestPath>,
    Json(payload): Json<RequestPackagesBody>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, params.id).await {
        return resp;
    }
    if payload.packages.is_empty() || payload.packages.len() > 10 {
        return err_resp(&state, StatusCode::BAD_REQUEST, "packages must hold 1-10 names");
    }
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(params.id)
            .fetch_optional(&state.db)
            .await
            .unwrap_or(None);
    let Some(owner_id) = owner else {
        return err_resp(&state, StatusCode::NOT_FOUND, "Agent not found");
    };
    // Agent-keyed credential => requested_by 'agent' (the caller's own
    // id recorded); user/daemon key => 'user'.
    let (requested_by, requested_by_agent) = match user.agent_id {
        Some(aid) => ("agent", Some(aid)),
        None => ("user", None),
    };
    let mut requested: Vec<serde_json::Value> = Vec::new();
    let mut skipped: Vec<serde_json::Value> = Vec::new();
    for raw in &payload.packages {
        let name = raw.trim().to_ascii_lowercase();
        if let Some(why) = validate_pkg(&name) {
            skipped.push(json!({"package": raw, "why": why}));
            continue;
        }
        match crate::db::pkg_request_exists_similar(
            &state.db,
            Some(params.id),
            &name,
            &["pending", "approved", "installed"],
        )
        .await
        {
            Ok(true) => {
                skipped.push(json!({"package": name, "why": "already pending or approved"}));
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                return err_resp(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("dedupe check failed: {e}"),
                )
            }
        }
        match crate::db::insert_pkg_request(
            &state.db,
            owner_id,
            Some(params.id),
            &name,
            payload.reason.as_deref(),
            requested_by,
            requested_by_agent,
        )
        .await
        {
            Ok(row) => requested.push(json!({
                "id": row.id, "package": row.package_name, "status": row.status,
            })),
            Err(e) => {
                return err_resp(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("insert failed: {e}"),
                )
            }
        }
    }

    // Notify the approver agent (best-effort — a missing/misconfigured
    // EA must never fail the request itself; the mobile queue still
    // shows it).
    if !requested.is_empty() {
        if let Some(appr) = resolve_approver(&state).await {
            if let Some(cid) = latest_conversation(&state, parse_appr_id(&appr)).await {
                let agent_name: String =
                    sqlx::query_scalar("SELECT name FROM agents WHERE id = $1")
                        .bind(params.id)
                        .fetch_optional(&state.db)
                        .await
                        .unwrap_or(None)
                        .unwrap_or_else(|| params.id.to_string());
                let pkgs: Vec<String> = requested
                    .iter()
                    .map(|r| r.get("package").and_then(|v| v.as_str()).unwrap_or("?").to_string())
                    .collect();
                let ids: Vec<String> = requested
                    .iter()
                    .map(|r| r.get("id").and_then(|v| v.as_str()).unwrap_or("?").to_string())
                    .collect();
                let content = format!(
                    "[sandbox-approval] Agent \"{}\" requests package(s): {} \u{2014} reason: {}. \
                     Request ids: {}. Decide with approve_sandbox_request / \
                     reject_sandbox_request (pass the id + package), or \
                     escalate_sandbox_request to put it in front of the user.",
                    agent_name,
                    pkgs.join(", "),
                    payload.reason.as_deref().unwrap_or("(none given)"),
                    ids.join(", "),
                );
                if let Err((status, msg)) = dispatch_message(&state, cid, &content).await {
                    tracing::warn!(
                        "sandbox-approval EA notify failed: {:?} {}",
                        status,
                        msg
                    );
                }
            }
        }
    }
    Json(json!({ "requested": requested, "skipped": skipped })).into_response()
}

fn parse_appr_id(appr: &serde_json::Value) -> Uuid {
    appr.get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_default()
}

// ============================================
// GET /agents/:id/sandbox/packages
// ============================================

pub(crate) async fn list_sandbox_packages(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(params): Path<SandboxRequestPath>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, params.id).await {
        return resp;
    }
    let rows = match crate::db::list_pkg_requests(&state.db, params.id).await {
        Ok(r) => r,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("list failed: {e}"),
            )
        }
    };
    let bucket = |s: &str| -> Vec<serde_json::Value> {
        rows.iter()
            .filter(|r| r.status == s)
            .map(|r| {
                json!({
                    "id": r.id, "package": r.package_name, "reason": r.reason,
                    "requested_by": r.requested_by, "created_at": r.created_at,
                    "decided_by": r.decided_by, "install_error": r.install_error,
                })
            })
            .collect()
    };
    let base_ready = std::path::Path::new("/forge/sandbox/base/etc/debian_version").exists();
    Json(json!({
        "base_ready": base_ready,
        "approver": resolve_approver(&state).await,
        "installed": bucket("installed"),
        "approved": bucket("approved"),
        "pending": bucket("pending"),
        "rejected": bucket("rejected"),
    }))
    .into_response()
}

// ============================================
// POST /sandbox/requests/:id/decision | /escalate
// ============================================

#[derive(Deserialize)]
pub(crate) struct DecidePath {
    id: Uuid,
}

#[derive(Deserialize)]
pub(crate) struct DecisionBody {
    decision: String, // "approve" | "reject"
    #[serde(default)]
    reason: Option<String>,
}

/// Shared load+auth for decision/escalate: returns (row, is_user).
/// Agent callers must be the configured approver; user/daemon keys
/// always decide (the mobile Sandbox section).
async fn authorize_decision(
    state: &AppState,
    user: &AuthenticatedUser,
    id: Uuid,
) -> Result<(crate::db::SandboxPackageRequest, bool), Response> {
    let row = crate::db::get_pkg_request(&state.db, id)
        .await
        .ok()
        .flatten()
        .ok_or_else(|| err_resp(state, StatusCode::NOT_FOUND, "Request not found"))?;
    // Tenancy: agent-scoped rows go through the agent gate; owner-wide
    // rows (agent_id NULL) require the owning user key.
    if let Some(aid) = row.agent_id {
        if let Some(resp) = agent_access_err(state, user, aid).await {
            return Err(resp);
        }
    } else if row.owner_id != user.user_id {
        return Err(err_resp(state, StatusCode::NOT_FOUND, "Request not found"));
    }
    let is_user = user.agent_id.is_none();
    if !is_user && !is_approver(state, user).await {
        return Err(err_resp(
            state,
            StatusCode::FORBIDDEN,
            "only the configured sandbox approver agent (or the user) may decide",
        ));
    }
    Ok((row, is_user))
}

pub(crate) async fn decide_request(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(params): Path<DecidePath>,
    Json(payload): Json<DecisionBody>,
) -> Response {
    let (row, is_user) = match authorize_decision(&state, &user, params.id).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if row.status != "pending" {
        return err_resp(
            &state,
            StatusCode::CONFLICT,
            &format!("request already {}", row.status),
        );
    }
    let decided_by = if is_user {
        "user".to_string()
    } else {
        format!("ea:{}", user.agent_id.map(|a| a.to_string()).unwrap_or_default())
    };
    match payload.decision.as_str() {
        "approve" => {
            if let Err(e) =
                crate::db::decide_pkg_request(&state.db, params.id, "approved", &decided_by, None)
                    .await
            {
                return err_resp(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("decision failed: {e}"),
                );
            }
            // Best-effort immediate install into the agent's live
            // sandboxes (future spawns reconcile anyway).
            if let Some(aid) = row.agent_id {
                let sm = state.sandbox_manager.clone();
                let db = state.db.clone();
                tokio::spawn(async move { sm.apply_for_agent(&db, aid).await });
            }
            Json(json!({ "ok": true, "status": "approved", "package": row.package_name }))
                .into_response()
        }
        "reject" => {
            let note = payload.reason.as_deref().unwrap_or("rejected");
            if let Err(e) =
                crate::db::decide_pkg_request(&state.db, params.id, "rejected", &decided_by, Some(note))
                    .await
            {
                return err_resp(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("decision failed: {e}"),
                );
            }
            Json(json!({ "ok": true, "status": "rejected", "package": row.package_name }))
                .into_response()
        }
        other => err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            &format!("unknown decision {other:?} (approve|reject)"),
        ),
    }
}

/// EA-only: push the decision up to the human. Posts an `@notify:`
/// row into the EA's own conversation (the notify engine turns it
/// into a phone notification) pointing at the mobile Sandbox section,
/// where the user taps approve/reject.
pub(crate) async fn escalate_request(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(params): Path<DecidePath>,
) -> Response {
    let (row, _is_user) = match authorize_decision(&state, &user, params.id).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let Some(ea_id) = user.agent_id else {
        return err_resp(&state, StatusCode::BAD_REQUEST, "escalate is agent-only");
    };
    let Some(cid) = latest_conversation(&state, ea_id).await else {
        return err_resp(&state, StatusCode::NOT_FOUND, "approver has no conversation to notify");
    };
    let agent_name = match row.agent_id {
        Some(aid) => sqlx::query_scalar::<_, String>("SELECT name FROM agents WHERE id = $1")
            .bind(aid)
            .fetch_optional(&state.db)
            .await
            .unwrap_or(None)
            .unwrap_or_else(|| row.agent_id.map(|a| a.to_string()).unwrap_or_default()),
        None => "(all agents)".to_string(),
    };
    let content = format!(
        "@notify: sandbox approval needs YOU: package `{}` for agent {} \u{2014} reason: {}. \
         Approve or reject in the agent editor's Sandbox section (request {}).",
        row.package_name,
        agent_name,
        row.reason.as_deref().unwrap_or("(none given)"),
        params.id,
    );
    match dispatch_message(&state, cid, &content).await {
        Ok(_) => Json(json!({ "ok": true, "escalated": params.id })).into_response(),
        Err((status, msg)) => err_resp(&state, status, &msg),
    }
}

// ============================================
// POST /agents/:id/sandbox/reset
// ============================================

/// Wipe the agent's per-session rootfs trees (next bash re-copies a
/// clean base + the reconciler re-installs the approved set). The
/// sessions' working trees (/forge/sessions/<id>) are KEPT — a reset
/// reinstalls the system, not the agent's files.
pub(crate) async fn reset_sandbox(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(params): Path<SandboxRequestPath>,
) -> Response {
    if let Some(resp) = agent_access_err(&state, &user, params.id).await {
        return resp;
    }
    // Agents may reset their OWN sandbox; anyone who passes the agent
    // gate (user) resets anything; approver-agent likewise.
    if let Some(aid) = user.agent_id {
        if aid != params.id && !is_approver(&state, &user).await {
            return err_resp(
                &state,
                StatusCode::FORBIDDEN,
                "an agent may only reset its own sandbox",
            );
        }
    }
    let n = state.sandbox_manager.reset_for_agent(&state.db, params.id).await;
    Json(json!({ "ok": true, "rootfs_removed": n })).into_response()
}

// ============================================
// tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkg_names_validate_tightly() {
        assert!(valid_pkg("nodejs"));
        assert!(valid_pkg("python3.11"));
        assert!(valid_pkg("libssl-dev"));
        assert!(valid_pkg("g++"));
        assert!(valid_pkg("fonts-liberation"));
        assert!(!valid_pkg("a")); // too short
        assert!(!valid_pkg("Git")); // uppercase caught via validate lowercasing — raw form rejected
        assert!(!valid_pkg("/bin/sh")); // path
        assert!(!valid_pkg("foo;rm")); // separator
        assert!(!valid_pkg("foo bar")); // space
        assert!(!valid_pkg("nodejs=18")); // version pin
        assert!(!valid_pkg("-lead")); // leading dash
    }

    #[test]
    fn denylist_blocks_escape_tooling() {
        assert!(validate_pkg("sudo").is_some());
        assert!(validate_pkg("openssh-server").is_some());
        assert!(validate_pkg("proot").is_some());
        assert!(validate_pkg("apt").is_some());
        assert!(validate_pkg("debootstrap").is_some());
        assert!(validate_pkg("nodejs").is_none());
        assert!(validate_pkg("gh").is_none());
        // uppercase smuggle attempt still lands on the denylist
        assert!(validate_pkg("SUDO").is_some());
    }
}
