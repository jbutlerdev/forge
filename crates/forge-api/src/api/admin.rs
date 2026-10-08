//! Admin operator endpoints: `/admin/self-update`,
//! `/admin/sandbox-reset`, `/admin/session-replay`.
//!
//! All require `role == "admin"` (validated by the auth middleware,
//! checked again in each handler).

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    Extension,
};
use serde::Deserialize;
use uuid::Uuid;

use super::{err_resp, AppState};
use crate::api::auth::AuthenticatedUser;

// Admin Routes
// ============================================

/// Atomic self-update endpoint. Accepts a raw binary in the
/// request body, writes it to a staging path, and schedules
/// a graceful restart. Returns 202 immediately before the
/// API exits.
///
/// Deploy flow (called by the LLM after `cargo build --release`):
///   1. API writes the new binary to `/opt/forge/forge-api.staging`
///   2. API spawns a `setsid` helper that sleeps 0.5s then runs
///      `systemctl restart forge-api`
///   3. API returns 202
///   4. Helper wakes, systemd stops the API (SIGTERM)
///   5. `ExecStopPost=` runs `mv -f staging final` (atomic)
///   6. `Restart=always` starts the new binary
///   7. New API is up with the new binary
///
/// The `setsid` helper detaches from the API's process group
/// so it survives the API's SIGTERM. The unit's
/// `KillMode=process` is required to keep the helper alive —
/// the default `KillMode=control-group` would kill it along
/// with the API before it could issue the restart.
///
/// Auth: requires a valid **admin** API key (the operator's
/// `$FORGE_API_KEY` from `/etc/forge/forge.env` is the key of the
/// seeded `admin@forge.local` user). A mere "any valid user key"
/// used to authorize this endpoint — combined with the old
/// presence-only middleware, that meant any header value at all
/// could replace the running binary. Now the middleware validates
/// the key AND the handler checks `role == "admin"`.
pub(crate) async fn self_update(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    body: Bytes,
) -> Response {
    if user.role != "admin" {
        return err_resp(&state, StatusCode::FORBIDDEN, "Admin access required");
    }
    if body.is_empty() {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "empty body; expected the new binary in the request body",
        );
    }
    // Sanity check: the first 4 bytes of an ELF binary are
    // `0x7F 'E' 'L' 'F'`. Catches "I sent the wrong file"
    // before we replace the running binary. Doesn't validate
    // the architecture, but rejecting arbitrary garbage
    // (a build log, a tarball, the path string from a typo)
    // is enough to prevent accidentally clobbering
    // forge-api with junk.
    if body.len() < 4 || &body[..4] != b"\x7fELF" {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "body is not an ELF binary; refusing to overwrite /opt/forge/forge-api",
        );
    }
    let staging = "/opt/forge/forge-api.staging";
    if let Err(e) = tokio::fs::write(staging, &body).await {
        return err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to write staging binary: {e}"),
        );
    }
    // Make the staging binary executable. `tokio::fs::set_permissions`
    // isn't stable across all platforms; the permissions came
    // from the umask, so just chmod 0755 explicitly.
    if let Ok(meta) = std::fs::metadata(staging) {
        let mut perms = meta.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        let _ = std::fs::set_permissions(staging, perms);
    }

    // Spawn a detached helper that schedules the restart.
    // `setsid` creates a new session so the helper is not in
    // the API's process group; when the API gets SIGTERM, the
    // helper survives (assuming `KillMode=process` in the
    // unit). The 0.5s sleep gives the API time to return
    // 202 to the client before the restart tears the
    // connection down.
    //
    // We swallow the helper's stderr (the API's journal is
    // already noisy); any restart failure is observable via
    // `systemctl status forge-api` and `journalctl -u
    // forge-api` after the deploy.
    let helper = std::process::Command::new("setsid")
        .arg("bash")
        .arg("-c")
        .arg("sleep 0.5; systemctl restart forge-api >/dev/null 2>&1 || true")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    match helper {
        Ok(_) => {
            tracing::info!(
                bytes = body.len(),
                staging,
                "self-update scheduled: wrote staging binary and spawned restart helper"
            );
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "status": "deploy scheduled",
                    "staging": staging,
                    "bytes": body.len(),
                })),
            )
                .into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!(
                "failed to spawn restart helper: {e}; staging binary is at {staging}, \
                 run `sudo cp {staging} /opt/forge/forge-api && \
                 sudo systemctl restart forge-api` manually"
            ),
        ),
    }
}

/// Reset (wipe + re-copy on next use) the per-session sandbox
/// rootfs. The session itself, its working dir, and its
/// messages table are untouched — only the per-session
/// container rootfs at `/forge/sandbox/forge-<uuid>/` is
/// removed. The next `bash` tool call will see no rootfs
/// and do a fresh `cp -a` from `/forge/sandbox/base/`,
/// picking up any changes the operator made to the base
/// (`chroot /forge/sandbox/base apt install -y foo`,
/// edits to `/etc/`, etc.).
///
/// Operator workflow:
///
/// 1. Update the base: `chroot /forge/sandbox/base apt install -y foo`
/// 2. `POST /admin/sandbox-reset?session_id=<uuid>` (no body)
/// 3. Next bash call in the session: ~0.5s of `cp -a` and the
///    new `foo` is available.
///
/// This is the endpoint the matrix appservice's `/new`
/// command hits so that the freshly-minted session starts
/// from a base the operator can mutate out-of-band. Without
/// this, the new session's rootfs would be cp'd at session
/// creation time, locking in whatever the base looked like
/// at that moment — a race that mattered for the `apt
/// install` use case above.
///
/// Query params:
///   - `session_id` (UUID, required)
///
/// Idempotent. Returns 200 with `noop: true` if the session
/// has no container (e.g. the session was deleted or never
/// bootstrapped). Returns 200 with `noop: false,
/// root_dir: ...` if a rootfs was wiped.
#[derive(Debug, Deserialize)]
pub(crate) struct SandboxResetQuery {
    session_id: Uuid,
}

pub(crate) async fn reset_sandbox(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<SandboxResetQuery>,
) -> Response {
    if user.role != "admin" {
        return err_resp(&state, StatusCode::FORBIDDEN, "Admin access required");
    }
    match state
        .sandbox_manager
        .reset_container(params.session_id)
        .await
    {
        Ok(result) => {
            tracing::info!(
                session_id = %params.session_id,
                noop = %result.noop,
                root_dir = ?result.root_dir,
                "sandbox reset endpoint: completed"
            );
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "ok",
                    "session_id": params.session_id.to_string(),
                    "noop": result.noop,
                    "root_dir": result.root_dir.as_ref().map(|p| p.display().to_string()),
                    "note": if result.noop {
                        "session had no container; nothing to wipe"
                    } else {
                        "per-session rootfs wiped; next bash call will re-cp from /forge/sandbox/base"
                    },
                })),
            )
                .into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("sandbox reset failed: {e}"),
        ),
    }
}

/// Migrate a session onto the harness NOW instead of waiting for its
/// next write (the H2.6 lazy-migration trigger, on demand).
///
/// Post-cutover there is no `.parent.jsonl` to backfill — the legacy
/// jsonl replay was deleted. This endpoint runs exactly what the
/// first write would run: claim + `createConversation` + one bulk
/// `importEntries` + stamp (`crate::harness_migration::ensure_migrated`),
/// then reports the durable conversation's entry count so the
/// operator can sanity-check the import against the `messages`
/// table.
///
/// - Already-stamped session → `already_harness_backed: true` and the
///   conversation's entry count (a no-op — re-importing is not
///   supported: the `messages` table keeps growing after the import,
///   and the durable conversation is the live source of truth).
/// - Unstamped session → migrated now; `entries` reflects the
///   import.
///
/// Idempotent in the useful sense: re-running on a migrated session
/// is a cheap read. Auth: requires an **admin** API key, like the
/// other operator endpoints.
pub(crate) async fn admin_session_replay(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<SessionReplayQuery>,
) -> Response {
    if user.role != "admin" {
        return err_resp(&state, StatusCode::FORBIDDEN, "Admin access required");
    }
    let session_id = params.session_id;

    // Existence check first (404 for a missing session, not a
    // migration error), then the stamp (NULL = unmigrated).
    let exists: bool = match sqlx::query_scalar::<_, i32>("SELECT 1 FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(v) => v.is_some(),
        Err(e) => {
            return super::db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to look up session",
                e,
            )
        }
    };
    if !exists {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }
    let already =
        sqlx::query_scalar::<_, i64>("SELECT durable_conversation_id FROM sessions WHERE id = $1")
            .bind(session_id)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten()
            .is_some();

    let conversation_id =
        match crate::harness_migration::ensure_migrated(&state, session_id, None).await {
            Ok(id) => id,
            Err(e) => {
                return err_resp(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &format!("session migration failed: {e}"),
                )
            }
        };

    // The durable entry count (the imported transcript entries plus
    // anything the conversation has produced since; `forge.meta` /
    // document rows are separate doc rows, not entries).
    let schema = state.harness.durable_schema().to_string();
    let entries: i64 = match sqlx::query_scalar(&format!(
        r#"SELECT COUNT(*) FROM "{schema}".durable_entries WHERE conversation_id = $1"#
    ))
    .bind(conversation_id)
    .fetch_one(&state.db)
    .await
    {
        Ok(n) => n,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to count durable entries: {e}"),
            )
        }
    };

    tracing::info!(
        session_id = %session_id,
        conversation_id,
        entries,
        already_harness_backed = already,
        "admin/session-replay: session is (now) harness-backed"
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "session_id": session_id.to_string(),
            "conversation_id": conversation_id,
            "already_harness_backed": already,
            "entries": entries,
            "note": if already {
                "session was already on the harness; entry count reported, nothing re-imported"
            } else {
                "session migrated now (createConversation + bulk importEntries + stamp); the next write continues on the durable conversation"
            },
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub(crate) struct SessionReplayQuery {
    session_id: Uuid,
}
