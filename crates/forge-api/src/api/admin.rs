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

/// Rewrite the `.parent.jsonl` for a session by re-running
/// `write_session_jsonl_with_max_seq` against the current
/// state of the `messages` table. This is a one-off
/// operator endpoint for backfilling a session whose
/// `.parent.jsonl` was written by an older binary that had
/// a bug in the jsonl layout (e.g. the parallel-tool-call
/// reordering bug fixed when the 999 errors started
/// appearing in June 2026 — see
/// `crates/forge-api/src/session_replay.rs` for the bug
/// description and the fix).
///
/// Behavior:
///
/// 1. The session's in-memory agent entry is evicted from
///    the `agent_registry`, so the next prompt will spawn a
///    fresh pi instead of reusing the stuck one. This is
///    critical: if the stuck pi is still running, the
///    rewritten `.parent.jsonl` would be overwritten as soon
///    as that pi processes its next event. The eviction
///    ensures the next prompt's durable-resume path picks up
///    the new file.
/// 2. `write_session_jsonl_with_max_seq` is called with
///    `max_sequence = None`, which writes the full history
///    including any user prompts the user has already
///    queued. The durable-resume path would normally exclude
///    the just-inserted user message; for an operator
///    backfill, we want to rewrite the entire history so the
///    next prompt's durable-resume sees a clean file even if
///    the user has sent several prompts while the session
///    was stuck.
///
/// The endpoint is idempotent. Re-running it is safe.
///
/// Auth: requires `X-API-Key` like the other protected
/// endpoints. The operator passes `$FORGE_API_KEY` in the
/// curl headers.
pub(crate) async fn admin_session_replay(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<SessionReplayQuery>,
) -> Response {
    if user.role != "admin" {
        return err_resp(&state, StatusCode::FORBIDDEN, "Admin access required");
    }
    let session_id = params.session_id;

    // Look up the session and its profile's working_dir.
    // The `cwd` field on the .parent.jsonl header is just
    // metadata (pi doesn't strictly require it), so if the
    // session or profile is missing we fall back to the
    // session directory. The session_replay module fetches
    // provider/model internally from the joined profile.
    let working_dir: Option<String> = sqlx::query_scalar(
        "SELECT p.working_dir FROM sessions s \
         JOIN profiles p ON s.profile_id = p.id \
         WHERE s.id = $1",
    )
    .bind(session_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten()
    .flatten();
    let working_dir = working_dir.unwrap_or_else(|| format!("/forge/sessions/{session_id}"));

    // Evict the in-memory agent entry (if any) so the next
    // prompt spawns a fresh pi that loads the rewritten
    // `.parent.jsonl`. Without this, the stuck pi would
    // overwrite the file on its next event and the
    // backfill would be lost. `remove()` also kills the
    // stuck pi subprocess, which is the desired behavior
    // for a "backfill a stuck session" operation.
    let evicted = match state.agent_registry.remove(session_id).await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                session_id = %session_id,
                error = %e,
                "admin/session-replay: failed to kill in-memory agent entry; continuing with jsonl rewrite anyway"
            );
            false
        }
    };
    tracing::info!(
        session_id = %session_id,
        evicted = evicted,
        "admin/session-replay: evicted in-memory agent entry so the next prompt will spawn a fresh pi"
    );

    // Rewrite the .parent.jsonl. Use max_sequence = None so
    // the entire history is written (operator backfill, not
    // the normal durable-resume path that excludes the
    // just-inserted user prompt).
    let jsonl_path = crate::session_replay::parent_jsonl_path(&working_dir);
    let written = match crate::session_replay::write_session_jsonl_with_max_seq(
        &state.db,
        session_id,
        &working_dir,
        &jsonl_path,
        None,
    )
    .await
    {
        Ok(n) => n,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to rewrite .parent.jsonl: {e}"),
            );
        }
    };

    tracing::info!(
        session_id = %session_id,
        entries_written = written,
        jsonl_path = %jsonl_path.display(),
        "admin/session-replay: rewrote .parent.jsonl with the current binary's session_replay code"
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "session_id": session_id.to_string(),
            "jsonl_path": jsonl_path.display().to_string(),
            "entries_written": written,
            "evicted_in_memory_agent": evicted,
            "note": "the next prompt on this session will spawn a fresh pi that loads the rewritten .parent.jsonl",
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub(crate) struct SessionReplayQuery {
    session_id: Uuid,
}
