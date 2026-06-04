use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::Duration;
use uuid::Uuid;

use crate::agent_registry::AgentRegistry;
use crate::bus::MessageBus;
use crate::db::{CreateProfile, Message, Profile, Session, UpdateProfile};
use crate::observability::Metrics;
use crate::pi_agent::{AssistantMessageEvent, PiEvent};
use crate::recording::ToolRecorder;
use crate::sandbox::SandboxManager;
use crate::session_manager::SessionManager;
use crate::tool_executor::{ToolExecutor, ToolInput};

/// Per-`read_line()` timeout when no tool call is in flight. If pi
/// goes this long without emitting any event, the harness assumes
/// something is wrong (LLM provider hung, pi wedged, network blip)
/// and bails. Long enough for slow LLM responses; short enough
/// that we surface real failures quickly.
const IDLE_READ_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Per-`read_line()` timeout while one or more tool calls are in
/// flight. Pi emits `tool_execution_start` when a tool begins and
/// `tool_execution_end` when it finishes; between those two events
/// pi is silent. A tool that legitimately takes longer than
/// `IDLE_READ_TIMEOUT_SECS` (e.g. a long compile, a large
/// `git clone`, a `cargo test --release`) would otherwise hit the
/// idle timeout.
///
/// **This must be at least `BASH_DEFAULT_TIMEOUT_MS` + the
/// outermost grace window (5 s on the sandbox + streaming
/// paths).** If it's less, the harness will kill pi a few
/// seconds before the bash tool's outer `tokio::time::timeout`
/// fires — the tool would have been killed by the harness
/// before it could clean up, and the `tool_output` row in
/// the audit log would record a `Container … terminated by
/// signal KILL` from the harness SIGKILL rather than from the
/// model's `timeout_ms`. Set to 2 h to give the 1 h bash
/// default (see [`crate::tool_executor::BASH_DEFAULT_TIMEOUT_MS`])
/// plenty of headroom and to accommodate a model that asks
/// `timeout_ms` for up to ~2 h.
const TOOL_READ_TIMEOUT_SECS: u64 = 7200; // 2 hours

pub mod auth;
pub mod events;
#[cfg(test)]
mod events_integration;
pub mod middleware;
pub mod sse;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub session_manager: Arc<SessionManager>,
    pub sandbox_manager: Arc<SandboxManager>,
    pub agent_registry: Arc<AgentRegistry>,
    pub metrics: Arc<Metrics>,
    /// Records tool call intents (from the harness) and tool results
    /// (from the executor) to durable storage. Held as a trait object
    /// so we can swap the backend (e.g. a different DB or a separate
    /// audit-log table) without touching call sites.
    pub recorder: Arc<dyn ToolRecorder>,
    /// In-process pub/sub for new message rows. The harness and the
    /// tool executor publish to it; the SSE handler at
    /// `GET /sessions/:id/events` subscribes. See
    /// [`crate::bus::MessageBus`] for the design.
    pub bus: MessageBus,
}

impl AppState {
    pub fn new(
        db: PgPool,
        session_manager: Arc<SessionManager>,
        sandbox_manager: Arc<SandboxManager>,
        agent_registry: Arc<AgentRegistry>,
        metrics: Arc<Metrics>,
        recorder: Arc<dyn ToolRecorder>,
        bus: MessageBus,
    ) -> Self {
        Self {
            db,
            session_manager,
            sandbox_manager,
            agent_registry,
            metrics,
            recorder,
            bus,
        }
    }
}

fn err_resp(state: &AppState, status: StatusCode, message: &str) -> Response {
    state.metrics.inc_errors(status.as_u16());
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// Look up a session's working directory directly from the database.
///
/// [`crate::session_manager::SessionManager`] keeps an in-memory map of
/// session id -> working directory. That map is populated when a
/// session is first created and lost whenever the API restarts. The
/// directory itself is durable (`/forge/sessions/<id>`), so for any
/// well-formed session we can recompute the path here and re-seed the
/// in-memory map so subsequent calls hit the cache.
pub async fn lookup_session_working_dir(state: &AppState, session_id: Uuid) -> Option<String> {
    // The session directory is always `/forge/sessions/<id>`; we don't
    // need the profile to recompute it. We do still verify the session
    // exists in the DB so a bogus id returns None.
    let exists: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    exists?;

    let dir = std::path::PathBuf::from("/forge/sessions").join(session_id.to_string());
    if !dir.exists() {
        return None;
    }

    // Re-seed the in-memory map so future calls hit the fast path.
    if let Ok(profile_id) =
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT profile_id FROM sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(&state.db)
            .await
    {
        state
            .session_manager
            .register_existing_session(session_id, profile_id, dir.clone())
            .await;
    }

    Some(dir.to_string_lossy().to_string())
}

/// Insert one assistant message row for the given chunk of
/// model text and publish it on the bus. Returns the inserted
/// row, or `None` if `content` is empty (the function is a
/// no-op in that case, so the harness can flush an empty
/// buffer — e.g. between `text_end` and the next `text_start` —
/// without writing an empty placeholder row).
///
/// Used by the harness event loop to flush text chunks as the
/// model produces them, once on `text_end` / `toolcall_start`
/// (chunk boundary) and once more after `agent_end` (catch any
/// trailing text). Each call produces one assistant row, so a
/// multi-tool turn yields one row per text chunk rather than
/// one big concatenated row at the end of the turn.
async fn insert_and_publish_assistant(
    pool: &PgPool,
    bus: &MessageBus,
    session_id: Uuid,
    content: &str,
) -> Option<Message> {
    if content.is_empty() {
        return None;
    }
    let seq = match sqlx::query_scalar::<_, i32>("SELECT get_next_sequence($1)")
        .bind(session_id)
        .fetch_one(pool)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                session_id = %session_id,
                error = %e,
                "failed to allocate sequence for assistant message chunk"
            );
            return None;
        }
    };
    let row = match sqlx::query_as::<_, Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, 'assistant', $3) RETURNING *"#,
    )
    .bind(session_id)
    .bind(seq)
    .bind(content)
    .fetch_one(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                session_id = %session_id,
                error = %e,
                "failed to insert assistant message chunk"
            );
            return None;
        }
    };
    bus.publish_message(row.clone());
    Some(row)
}

// ============================================
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
/// Auth: requires `X-API-Key` like the other protected
/// endpoints. The LLM passes `$FORGE_API_KEY` in the curl
/// headers; the env is populated from `/etc/forge/forge.env`
/// when forge-api spawns `pi`, and `pi`'s env (and therefore
/// the bash tool's env) inherits it.
async fn self_update(State(state): State<AppState>, body: Bytes) -> Response {
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
struct SandboxResetQuery {
    session_id: Uuid,
}

async fn reset_sandbox(
    State(state): State<AppState>,
    Query(params): Query<SandboxResetQuery>,
) -> Response {
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

// ============================================
// Profile Routes
// ============================================

async fn create_profile(
    State(state): State<AppState>,
    Json(payload): Json<CreateProfile>,
) -> Response {
    let tools_json = payload
        .tools
        .as_ref()
        .map(|t| serde_json::to_string(t).unwrap_or_default())
        .unwrap_or_else(|| r#"["bash", "read", "write", "edit"]"#.to_string());

    match sqlx::query_as::<_, Profile>(
        r#"INSERT INTO profiles (name, description, provider, model, base_url, api_key, working_dir, git_url, git_ref, nix_shell, system_prompt, tools)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING *"#
    ).bind(&payload.name).bind(&payload.description).bind(&payload.provider).bind(&payload.model)
     .bind(&payload.base_url).bind(&payload.api_key).bind(&payload.working_dir).bind(&payload.git_url)
     .bind(&payload.git_ref).bind(&payload.nix_shell).bind(payload.system_prompt.as_deref().unwrap_or("You are a helpful coding assistant."))
     .bind(&tools_json).fetch_one(&state.db).await {
        Ok(p) => { state.metrics.inc_requests("POST /profiles"); (StatusCode::CREATED, Json(serde_json::json!({ "profile": p }))).into_response() }
        Err(e) => { tracing::error!("Failed to create profile: {e}"); err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create profile") }
    }
}

#[derive(Deserialize)]
struct ListProfilesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_profiles(
    State(state): State<AppState>,
    Query(query): Query<ListProfilesQuery>,
) -> Response {
    match sqlx::query_as::<_, Profile>(
        "SELECT * FROM profiles ORDER BY created_at DESC LIMIT $1 OFFSET $2",
    )
    .bind(query.limit.unwrap_or(50))
    .bind(query.offset.unwrap_or(0))
    .fetch_all(&state.db)
    .await
    {
        Ok(p) => {
            state.metrics.inc_requests("GET /profiles");
            Json(serde_json::json!({ "profiles": p })).into_response()
        }
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list profiles",
        ),
    }
}

async fn get_profile_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get profile",
        ),
    }
}

async fn delete_profile_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query("DELETE FROM profiles WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete profile",
        ),
    }
}

// ============================================
// Session Routes
// ============================================

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    profile_id: Uuid,
    title: Option<String>,
}

async fn create_session(
    State(state): State<AppState>,
    Json(payload): Json<CreateSessionRequest>,
) -> Response {
    let profile: Profile =
        match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
            .bind(payload.profile_id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(Some(p)) => p,
            Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
            Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
        };

    let title = payload
        .title
        .unwrap_or_else(|| format!("Session {}", chrono::Utc::now().format("%Y-%m-%d %H:%M")));

    let session: Session = match sqlx::query_as::<_, Session>(
        r#"INSERT INTO sessions (profile_id, title) VALUES ($1, $2) RETURNING *"#,
    )
    .bind(payload.profile_id)
    .bind(&title)
    .fetch_one(&state.db)
    .await
    {
        Ok(s) => s,
        Err(_) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create session",
            )
        }
    };

    match state
        .session_manager
        .create_session_dir(session.id, &profile)
        .await
    {
        Ok(working_dir) => {
            tracing::info!(
                "Created session {} with directory: {:?}",
                session.id,
                working_dir
            );
            (StatusCode::CREATED, Json(serde_json::json!({ "session": session, "working_dir": working_dir.to_string_lossy() }))).into_response()
        }
        Err(e) => {
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to create session: {}", e),
            )
        }
    }
}

async fn list_all_sessions(State(state): State<AppState>) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 100")
        .fetch_all(&state.db)
        .await
    {
        Ok(s) => Json(serde_json::json!({ "sessions": s })).into_response(),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list sessions",
        ),
    }
}

async fn get_session_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(s)) => Json(serde_json::json!({ "session": s })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get session",
        ),
    }
}

async fn delete_session_by_uuid(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let _ = state.agent_registry.remove(id).await;
    let _ = state.session_manager.remove_session(id).await;
    let _ = state.sandbox_manager.destroy_container(id).await;
    match sqlx::query("DELETE FROM sessions WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete session",
        ),
    }
}

// ============================================
// Message Routes
// ============================================

#[derive(Debug, Deserialize)]
struct SessionQuery {
    session_id: Uuid,
}

async fn list_messages_by_session(
    State(state): State<AppState>,
    Query(params): Query<SessionQuery>,
) -> Response {
    match sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
    )
    .bind(params.session_id)
    .fetch_all(&state.db)
    .await
    {
        Ok(m) => Json(serde_json::json!({ "messages": m })).into_response(),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list messages",
        ),
    }
}

#[derive(Debug, Deserialize)]
struct CreateMessageRequest {
    session_id: Uuid,
    content: String,
}

/// Send a message - pi processes it with timeouts
async fn create_message(
    State(state): State<AppState>,
    Json(payload): Json<CreateMessageRequest>,
) -> Response {
    let session_id = payload.session_id;

    let session_exists = sqlx::query("SELECT id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(&state.db)
        .await
        .map(|r| r.is_some())
        .unwrap_or(false);
    if !session_exists {
        return err_resp(&state, StatusCode::NOT_FOUND, "Session not found");
    }

    let sequence: i32 = match sqlx::query_scalar("SELECT get_next_sequence($1)")
        .bind(session_id)
        .fetch_one(&state.db)
        .await
    {
        Ok(s) => s,
        Err(_) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get sequence",
            )
        }
    };

    let message: Message = match sqlx::query_as::<_, Message>(
        r#"INSERT INTO messages (session_id, sequence, role, content) VALUES ($1, $2, 'user', $3) RETURNING *"#
    ).bind(session_id).bind(sequence).bind(&payload.content).fetch_one(&state.db).await {
        Ok(m) => m,
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create message"),
    };

    // Publish the user row to the bus so any SSE consumer attached
    // to this session sees it immediately.
    state.bus.publish_message(message.clone());

    let agent = match state
        .agent_registry
        .get_or_create(&state.db, session_id)
        .await
    {
        Ok(a) => a,
        Err(e) => {
            return err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to create agent: {}", e),
            )
        }
    };

    // Long-context resume: if the prior conversation in the
    // messages table exceeds the model's compaction
    // threshold, ask pi to compact it BEFORE the user's
    // first prompt lands. M3 is configured with
    // contextWindow=300k and reserveTokens=4k, so the
    // threshold is 296k. Sessions above this choke the
    // model on the first turn (the prior pi's auto-compact
    // never had a chance to fire because the model was
    // already erroring). We pre-empt that by sending
    // pi's `compact` RPC command now; pi runs an
    // LLM-generated summary (better than our heuristic
    // because the model itself decides what to keep) and
    // appends a CompactionEntry to the session file.
    //
    // The check is a rough char-based estimate from the
    // messages table; the actual model-side check happens
    // inside pi's `prepareCompaction` so the threshold
    // here just decides *whether* to fire the RPC, not
    // what pi does with it.
    let prior_context_tokens: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(LENGTH(content) + COALESCE(LENGTH(tool_input::text), 0) + COALESCE(LENGTH(tool_output::text), 0)), 0)::bigint / 4 FROM messages WHERE session_id = $1 AND sequence <= (SELECT MAX(sequence) - 1 FROM messages WHERE session_id = $1)"
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);
    let needs_compaction = prior_context_tokens > 296_000;
    if needs_compaction {
        tracing::info!(
            session_id = %session_id,
            prior_context_tokens,
            "long-context resume: prior conversation exceeds 300k tokens; sending pi `compact` RPC before user prompt"
        );
    }

    let pool = state.db.clone();
    let user_content = payload.content.clone();
    let metrics = state.metrics.clone();
    let bus = state.bus.clone();

    // Spawn background task with TIMEOUTS
    tokio::spawn(async move {
        let mut agent_guard = agent.lock().await;

        // Durable resume is handled entirely by
        // `agent_registry` at pi-spawn time: the prior
        // conversation was already loaded into the fresh
        // pi's session tree via the `new_session` RPC with
        // `parentSession` (see
        // `agent_registry::build_session_jsonl_and_load`).
        // The user's first prompt is sent verbatim — the
        // model already has the full prior conversation as
        // structured messages and continues from there. No
        // preamble formatting needed at this layer.

        // Drain any leftover events from a previous turn. With a long-lived
        // pi process, the `agent_end` / `turn_end` events from a prior
        // response are still in the read buffer; if we don't drain them
        // here we'll treat them as the new turn's completion and return
        // before the second prompt is ever processed.
        agent_guard.drain_pending_events().await;

        // Long-context compaction (see the get_or_create
        // call site for the rationale). If the prior
        // conversation is over 300k tokens, send a
        // `compact` RPC and wait for it to finish before
        // sending the user's prompt. This is the
        // "load + send a message + trigger compaction"
        // flow the user wants: pi loads the full history,
        // we then ask it to compact, then we send the
        // user's message in the now-smaller context.
        if needs_compaction {
            let start = std::time::Instant::now();
            match agent_guard.compact(None).await {
                Ok(resp) => {
                    let tokens_before = resp
                        .get("data")
                        .and_then(|d| d.get("tokensBefore"))
                        .and_then(|t| t.as_i64())
                        .unwrap_or(-1);
                    tracing::info!(
                        session_id = %session_id,
                        tokens_before,
                        duration_ms = start.elapsed().as_millis() as i64,
                        "long-context resume: pi compaction complete"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        session_id = %session_id,
                        error = %e,
                        duration_ms = start.elapsed().as_millis() as i64,
                        "long-context resume: pi compaction failed or timed out; proceeding with user prompt anyway (model may still respond if the context is within its limit)"
                    );
                    // Fall through and send the prompt
                    // anyway. pi is still alive; if the
                    // compact call timed out we may have
                    // partial state, but the model might
                    // still respond. The alternative is to
                    // fail the request, which leaves the
                    // user without a way forward.
                }
            }
            // Drain again after the compaction: pi
            // emits `compaction_start` /
            // `compaction_end` events that we don't
            // want leaking into the prompt's event loop.
            agent_guard.drain_pending_events().await;
        }

        // Send the prompt first. pi only emits the `session` event after it
        // receives a message on stdin, so we can't reliably wait for it
        // before sending. The session/turn events will appear at the start of
        // the event stream and are handled below.
        if let Err(e) = agent_guard.send_message(&user_content).await {
            tracing::error!("Failed to send message to pi: {}", e);
            return;
        }

        let mut final_text = String::new();
        // Tracks whether the model emitted at least one text
        // delta during this turn. Used at end of turn to decide
        // whether to write the "[no response from agent]"
        // placeholder: if the model produced zero text, every
        // chunk flush was a no-op and the audit log has no
        // assistant row, so we write a placeholder for clients
        // that key off "did the agent finish something?". Once
        // the model emits even one text delta this stays true
        // for the rest of the turn, the placeholder is skipped,
        // and any text the model emits lands in its own row
        // via the per-chunk flushes in the event loop.
        let mut produced_any_text = false;
        let mut loop_count = 0;
        // The harness is patient about *total* runtime - pi is designed
        // for long agent runs that can produce hundreds of tool calls
        // across many turns, and the prior 5-minute total cap was
        // cutting off legitimate long work (we saw 123 tool calls in
        // one turn exceed it). The per-read timeout below is the
        // real "is pi stuck?" check; if pi goes that long without
        // emitting *any* event, something is wrong and we should
        // bail. The timeout is bumped up while one or more tool
        // calls are in flight (see `IDLE_READ_TIMEOUT_SECS` /
        // `TOOL_READ_TIMEOUT_SECS` at the top of the module), so a
        // legitimately long tool doesn't get killed mid-run.
        // The `loop_count < 10000` cap is a hard safety net against
        // an infinite loop.
        // See: session 1faa1686-... on 2026-06-02, which hit the
        // 5-minute cap mid-turn and the user's matrix room saw
        // `turn_ended` while the model was still making tool calls.
        // We need to see a `turn_start` for *this* turn before we
        // trust the events that follow. Anything we read before then
        // is either leftover from a prior turn (long-lived pi) or
        // a `agent_start`/`agent_end` from the session that was
        // loaded via `--session` (durable resume). Using
        // `turn_start` instead of `agent_start` as the gate is
        // what makes the durable-resume path work: pi replays
        // the loaded session's events immediately, and those
        // include an `agent_end` we must NOT honor.
        let mut seen_turn_start = false;
        // Number of tool calls currently in flight (incremented on
        // `tool_execution_start`, decremented on `tool_execution_end`,
        // clamped at 0). Drives the per-read timeout: when > 0, pi
        // is expected to be silent until the tools finish, so the
        // timeout is bumped to `TOOL_READ_TIMEOUT_SECS`. A `u32` is
        // more than enough - the model would have to make >4 billion
        // parallel tool calls to overflow, and we'd have bigger
        // problems than a wrong timeout by then.
        let mut in_flight_tools: u32 = 0;

        while loop_count < 10000 {
            loop_count += 1;

            // Pick the per-read timeout based on whether any tool
            // calls are currently in flight. When tools are running,
            // pi is silent between `tool_execution_start` and
            // `tool_execution_end`; an idle-style timeout would kill
            // a legitimately long tool call (a compile, a clone, a
            // long-running test). When no tools are in flight, the
            // shorter idle timeout applies - we expect events to
            // keep flowing (text deltas, model thinking, etc.) and
            // want to bail quickly if pi goes quiet.
            let read_timeout = if in_flight_tools > 0 {
                Duration::from_secs(TOOL_READ_TIMEOUT_SECS)
            } else {
                Duration::from_secs(IDLE_READ_TIMEOUT_SECS)
            };

            match tokio::time::timeout(read_timeout, agent_guard.read_line()).await {
                Ok(Ok(Some(line))) => {
                    match serde_json::from_str::<PiEvent>(&line) {
                        Ok(event) => match event {
                            PiEvent::Session { .. } => {
                                tracing::info!("Pi session ready");
                            }
                            // We use `turn_start` (not
                            // `agent_start`) as the gate for
                            // when to start honoring `agent_end`.
                            // `agent_start` is emitted once at
                            // the beginning of the pi process's
                            // lifetime; on a durable-resume spawn
                            // the loaded session's `agent_start`
                            // and `agent_end` events get replayed
                            // before the harness has even sent the
                            // user's new prompt. If we gated on
                            // `agent_start`, the loaded
                            // `agent_end` would terminate the
                            // loop before the model ever sees the
                            // new turn. `turn_start` is per-turn,
                            // so it's only emitted when the model
                            // actually starts processing the
                            // user's prompt, which is exactly the
                            // signal we want.
                            PiEvent::TurnStart => {
                                seen_turn_start = true;
                                tracing::info!("Turn started");
                            }
                            // `agent_start` arrives once per pi
                            // process lifetime. We log it but
                            // don't use it as the gate.
                            PiEvent::AgentStart => {
                                tracing::info!("Agent started");
                            }
                            PiEvent::MessageUpdate {
                                assistant_message_event: Some(evt),
                                ..
                            } if seen_turn_start => {
                                match evt {
                                    AssistantMessageEvent::TextDelta { delta } => {
                                        // Accumulate deltas into the
                                        // current chunk. The chunk is
                                        // flushed on `text_end`,
                                        // `toolcall_start`, and at end
                                        // of turn, so SSE consumers see
                                        // each text chunk as its own
                                        // row rather than one big
                                        // concatenated blob.
                                        produced_any_text = true;
                                        final_text.push_str(&delta);
                                    }
                                    AssistantMessageEvent::TextEnd => {
                                        // Pi finished emitting a text
                                        // content block. The accumulated
                                        // deltas in `final_text` form
                                        // one logical chunk of the
                                        // model's response; flush it
                                        // to the DB and publish on the
                                        // bus so SSE consumers see it
                                        // as its own row. The buffer
                                        // is reset via `mem::take` so
                                        // the next text block (or the
                                        // next round of deltas) starts
                                        // fresh.
                                        let chunk = std::mem::take(&mut final_text);
                                        insert_and_publish_assistant(
                                            &pool, &bus, session_id, &chunk,
                                        )
                                        .await;
                                    }
                                    AssistantMessageEvent::ToolCallStart => {
                                        // The model is moving from
                                        // text to a tool call. Any
                                        // text produced since the
                                        // last flush (or since the
                                        // turn started) is logically
                                        // complete — flush it as its
                                        // own row before the tool
                                        // call lands. This is the
                                        // primary streaming boundary:
                                        // every "Let me check the
                                        // file..." preamble the
                                        // model emits before a tool
                                        // call becomes a separate,
                                        // visible assistant row.
                                        //
                                        // Some pi versions emit
                                        // `text_end` immediately
                                        // before `toolcall_start`;
                                        // in that case `text_end`
                                        // already flushed and the
                                        // buffer is empty, so this
                                        // is a no-op. We flush on
                                        // both for robustness.
                                        let chunk = std::mem::take(&mut final_text);
                                        insert_and_publish_assistant(
                                            &pool, &bus, session_id, &chunk,
                                        )
                                        .await;
                                    }
                                    AssistantMessageEvent::ThinkingDelta { delta } => {
                                        tracing::debug!("[thinking] {}", delta);
                                    }
                                    AssistantMessageEvent::ToolCallEnd { tool_call } => {
                                        // The model decided to invoke a
                                        // tool. The executor is the
                                        // sole writer of the call row
                                        // (and the result row) - the
                                        // harness used to write a call
                                        // row here, but that created
                                        // a race with the executor
                                        // (the harness could exit its
                                        // event loop on `agent_end`
                                        // before all parallel
                                        // `ToolCallEnd` events
                                        // arrived, leaving some calls
                                        // without a row). The
                                        // executor is guaranteed to
                                        // see every call (it has to
                                        // run the tool anyway) and
                                        // writes the call row before
                                        // running, so the audit log
                                        // is self-consistent.
                                        // See `ToolExecutor::execute`
                                        // and `execute_bash_streaming`
                                        // for the write sites.
                                        tracing::debug!(
                                            tool_call_id = %tool_call.id,
                                            tool = %tool_call.name,
                                            "Tool call dispatched (executor will record the call + result rows)"
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            PiEvent::ToolExecutionStart {
                                tool_call_id,
                                tool_name,
                                ..
                            } if seen_turn_start => {
                                // Pi is about to invoke a tool. From
                                // now until the matching
                                // `tool_execution_end`, pi will be
                                // silent on stdout (the tool runs in
                                // the extension's process; pi just
                                // forwards the call and the result).
                                // Bump the in-flight counter so the
                                // next read uses the longer
                                // `TOOL_READ_TIMEOUT_SECS` instead of
                                // the idle one. The model can issue
                                // parallel tool calls, so we count
                                // them rather than toggle a bool.
                                in_flight_tools = in_flight_tools.saturating_add(1);
                                tracing::debug!(
                                    tool_call_id = %tool_call_id,
                                    tool = %tool_name,
                                    in_flight = in_flight_tools,
                                    "Tool execution started; per-read timeout extended"
                                );
                            }
                            PiEvent::ToolExecutionEnd {
                                tool_call_id,
                                tool_name,
                                result: _,
                                is_error,
                            } if seen_turn_start => {
                                // The tool finished. The executor is
                                // the single owner of the *result*
                                // half of the audit log - it already
                                // wrote (or will write) a `role='tool'`
                                // row via the same recorder. We just
                                // log the event so it's visible in
                                // the journal. This arm exists so we
                                // don't fall through to the catch-all
                                // and lose the timing information.
                                // Decrement the in-flight counter so
                                // the next read drops back to the
                                // idle timeout. `saturating_sub` is
                                // defensive against a stray
                                // `tool_execution_end` with no
                                // matching start (e.g. from a prior
                                // turn's leftovers); we just stay at
                                // 0 instead of underflowing.
                                in_flight_tools = in_flight_tools.saturating_sub(1);
                                tracing::info!(
                                    tool_call_id = %tool_call_id,
                                    tool = %tool_name,
                                    is_error = %is_error,
                                    in_flight = in_flight_tools,
                                    "Tool execution finished (recorded by executor)"
                                );
                            }
                            PiEvent::AgentEnd if seen_turn_start => {
                                // The agent has finished this turn. Stop
                                // reading; anything else still in the
                                // buffer will be drained on the next call.
                                tracing::info!("Agent ended");
                                // Don't publish turn_ended here — the
                                // final assistant text is written to the
                                // DB and published after the event loop
                                // exits (see below), so consumers would
                                // see typing_stop before the final
                                // message. We publish turn_ended right
                                // after the message is published, in
                                // the same post-loop code path.
                                break;
                            }
                            PiEvent::Error { message } => {
                                tracing::error!("pi error: {}", message);
                                break;
                            }
                            _ => {
                                // Leftover from a previous turn
                                // (turn_end, agent_end, response,
                                // extension_ui_request, message_start/end
                                // for the user echo, etc.) or a
                                // non-actionable event for the current
                                // turn. Ignore.
                            }
                        },
                        Err(e) => {
                            tracing::warn!("Failed to parse pi output: {} (line: {:?})", e, line);
                        }
                    }
                }
                Ok(Ok(None)) => {
                    tracing::info!("pi process ended");
                    break;
                }
                Ok(Err(e)) => {
                    tracing::error!("pi read error: {}", e);
                    break;
                }
                Err(_) => {
                    // The per-read timeout fired. The actual value
                    // depends on whether tools are in flight (5min
                    // idle, 1hr with tools) - log which one we
                    // hit, kill the stuck pi, and bail. The
                    // durable-resume path will rebuild from the
                    // messages table on the next user message.
                    let _which = if in_flight_tools > 0 { "tool" } else { "idle" };
                    let secs = if in_flight_tools > 0 {
                        TOOL_READ_TIMEOUT_SECS
                    } else {
                        IDLE_READ_TIMEOUT_SECS
                    };
                    tracing::warn!(
                        in_flight_tools,
                        timeout_secs = secs,
                        "pi timed out waiting for response; killing pi (durable resume will rebuild on next message)"
                    );
                    let _ = agent_guard.kill().await;
                    break;
                }
            }
        }

        metrics.inc_requests("pi.responses");

        // Flush any trailing text the model emitted after the
        // last chunk boundary (the common case: a final
        // explanation after the last tool call). If the buffer
        // is empty but `produced_any_text` is true, every chunk
        // was already flushed at its boundary and there's
        // nothing left to do. If the model produced no text at
        // all during the turn, write the historical
        // "[no response from agent]" placeholder so consumers
        // that key off "an assistant row landed" still see a
        // turn end.
        if !final_text.is_empty() {
            let chunk = std::mem::take(&mut final_text);
            insert_and_publish_assistant(&pool, &bus, session_id, &chunk).await;
        } else if !produced_any_text {
            insert_and_publish_assistant(&pool, &bus, session_id, "[no response from agent]").await;
        }

        // Always announce the turn is over, even if every
        // chunk flush above errored. SSE consumers use this to
        // clear typing indicators; if the agent crashed or
        // timed out, the consumer still wants to know the
        // turn is no longer in flight. (The previous code
        // only published `turn_ended` if the final-row insert
        // succeeded, which left consumers stuck in
        // "agent is typing..." when the DB write failed.)
        bus.publish_turn_ended(session_id);

        let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
            .bind(session_id)
            .execute(&pool)
            .await;
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "message": message })),
    )
        .into_response()
}

// ============================================
// Tool Execution Routes
// ============================================

async fn execute_tool(State(state): State<AppState>, Json(payload): Json<ToolInput>) -> Response {
    state.metrics.inc_requests("POST /tools/execute");
    state.metrics.inc_tool_execution(&payload.tool);

    let session_id = match Uuid::parse_str(&payload.session_id) {
        Ok(id) => id,
        Err(_) => return err_resp(&state, StatusCode::BAD_REQUEST, "Invalid session ID format"),
    };

    // Prefer the in-memory cache populated when the session was first
    // created, but fall back to the canonical working dir on disk so
    // tool calls keep working after an API restart (which would have
    // wiped the in-memory map).
    let working_dir = match state.session_manager.get_session_dir(session_id).await {
        Ok(dir) => dir.to_string_lossy().to_string(),
        Err(_) => match lookup_session_working_dir(&state, session_id).await {
            Some(dir) => dir,
            None => return err_resp(&state, StatusCode::NOT_FOUND, "Session not initialized"),
        },
    };

    let nix_shell: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT p.nix_shell FROM sessions s JOIN profiles p ON s.profile_id = p.id WHERE s.id = $1",
    )
    .bind(session_id)
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();

    let tool_call_id = payload.tool_call_id_str();

    // Look up the session's container. If it has one, the
    // executor will wrap bash calls in a per-session
    // `systemd-nspawn` namespace. If not (e.g. an old session
    // predating the sandbox, or a session that failed to
    // bootstrap), bash falls back to running on the host in
    // `working_dir` — the legacy behavior.
    let sandbox = state
        .sandbox_manager
        .get_container(session_id)
        .await
        .ok()
        .map(|_| state.sandbox_manager.clone());

    let executor = ToolExecutor::new(
        session_id,
        working_dir,
        sandbox.is_some(),
        nix_shell,
        state.recorder.clone(),
        state.bus.clone(),
        sandbox,
    );

    match executor
        .execute(&tool_call_id, &payload.tool, payload.input.clone())
        .await
    {
        Ok(output) => {
            tracing::info!(
                "Tool {} completed: success={}",
                payload.tool,
                output.success
            );
            Json(serde_json::json!({ "success": output.success, "output": output.output, "error": output.error })).into_response()
        }
        Err(e) => {
            tracing::error!("Tool error: {}", e);
            Json(serde_json::json!({ "success": false, "output": serde_json::Value::Null, "error": e.to_string() })).into_response()
        }
    }
}

// ============================================
// Health Check
// ============================================

async fn health() -> &'static str {
    "OK"
}

// ============================================
// Query-based Routes
// ============================================

#[derive(Debug, Deserialize)]
struct ProfileQuery {
    id: Uuid,
}
async fn get_profile_by_id(
    State(state): State<AppState>,
    Query(params): Query<ProfileQuery>,
) -> Response {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(params.id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get profile",
        ),
    }
}

#[derive(Debug, Deserialize)]
struct DeleteProfileQuery {
    id: Uuid,
}
async fn delete_profile_by_id(
    State(state): State<AppState>,
    Query(params): Query<DeleteProfileQuery>,
) -> Response {
    match sqlx::query("DELETE FROM profiles WHERE id = $1")
        .bind(params.id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete profile",
        ),
    }
}

#[derive(Debug, Deserialize)]
struct UpdateProfileQuery {
    id: Uuid,
}
async fn update_profile_by_id(
    State(state): State<AppState>,
    Query(params): Query<UpdateProfileQuery>,
    Json(payload): Json<UpdateProfile>,
) -> Response {
    update_profile_internal(&state, params.id, payload).await
}

async fn update_profile_internal(state: &AppState, id: Uuid, payload: UpdateProfile) -> Response {
    let mut query = "UPDATE profiles SET updated_at = NOW()".to_string();
    let mut param_idx = 1;
    let mut params: Vec<String> = Vec::new();

    macro_rules! add_param {
        ($field:expr, $name:expr) => {
            if $field.is_some() {
                params.push(format!("{} = ${}", $name, param_idx));
                param_idx += 1;
            }
        };
    }
    add_param!(payload.name, "name");
    add_param!(payload.description, "description");
    add_param!(payload.provider, "provider");
    add_param!(payload.model, "model");
    add_param!(payload.base_url, "base_url");
    add_param!(payload.api_key, "api_key");
    add_param!(payload.working_dir, "working_dir");
    add_param!(payload.git_url, "git_url");
    add_param!(payload.git_ref, "git_ref");
    add_param!(payload.nix_shell, "nix_shell");
    add_param!(payload.system_prompt, "system_prompt");
    add_param!(payload.tools, "tools");

    if params.is_empty() {
        return err_resp(state, StatusCode::BAD_REQUEST, "No fields to update");
    }

    query.push_str(", ");
    query.push_str(&params.join(", "));
    query.push_str(&format!(" WHERE id = ${} RETURNING *", param_idx));

    let mut db_query = sqlx::query_as::<_, Profile>(&query);
    if let Some(ref v) = payload.name {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.description {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.provider {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.model {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.base_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.api_key {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.working_dir {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.git_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.git_ref {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.nix_shell {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.system_prompt {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.tools {
        db_query = db_query.bind(serde_json::to_string(v).unwrap_or_default());
    }
    db_query = db_query.bind(id);

    match db_query.fetch_optional(&state.db).await {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => err_resp(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update profile",
        ),
    }
}

#[derive(Debug, Deserialize)]
struct DeleteSessionQuery {
    id: Uuid,
}
async fn delete_session_by_id(
    State(state): State<AppState>,
    Query(params): Query<DeleteSessionQuery>,
) -> Response {
    let _ = state.agent_registry.remove(params.id).await;
    let _ = state.session_manager.remove_session(params.id).await;
    let _ = state.sandbox_manager.destroy_container(params.id).await;
    match sqlx::query("DELETE FROM sessions WHERE id = $1")
        .bind(params.id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete session",
        ),
    }
}

#[derive(Debug, Deserialize)]
struct GetSessionQuery {
    id: Uuid,
}
async fn get_session_by_id(
    State(state): State<AppState>,
    Query(params): Query<GetSessionQuery>,
) -> Response {
    match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(params.id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(s)) => Json(serde_json::json!({ "session": s })).into_response(),
        Ok(None) => err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get session",
        ),
    }
}

// ============================================
// Sandbox Routes
// ============================================

async fn list_sandbox_containers(State(state): State<AppState>) -> Response {
    let containers = state.sandbox_manager.list_containers().await;
    let list: Vec<_> = containers.into_iter().map(|c| {
        serde_json::json!({ "name": c.name, "session_id": c.session_id.to_string(), "state": format!("{:?}", c.state), "working_dir": c.working_dir.to_string_lossy(), "pid": c.pid })
    }).collect();
    Json(serde_json::json!({ "containers": list })).into_response()
}

#[derive(Debug, Deserialize)]
struct SessionPath {
    session_id: Uuid,
}

async fn create_sandbox_for_session(
    State(state): State<AppState>,
    Path(params): Path<SessionPath>,
) -> Response {
    let session = match sqlx::query_as::<_, Session>("SELECT * FROM sessions WHERE id = $1")
        .bind(params.session_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Session not found"),
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
    };
    let profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(session.profile_id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(p)) => p,
        Ok(None) => return err_resp(&state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(_) => return err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
    };
    match state
        .sandbox_manager
        .create_container(params.session_id, &profile)
        .await
    {
        Ok(container) => {
            tracing::info!(
                "Created sandbox container {} for session {}",
                container.name,
                params.session_id
            );
            Json(serde_json::json!({ "container": { "name": container.name, "session_id": container.session_id.to_string(), "working_dir": container.working_dir.to_string_lossy(), "state": format!("{:?}", container.state) } })).into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to create sandbox: {}", e),
        ),
    }
}

async fn destroy_sandbox_for_session(
    State(state): State<AppState>,
    Path(params): Path<SessionPath>,
) -> Response {
    match state
        .sandbox_manager
        .destroy_container(params.session_id)
        .await
    {
        Ok(()) => {
            tracing::info!("Destroyed sandbox for session {}", params.session_id);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to destroy sandbox: {}", e),
        ),
    }
}

// ============================================
// Metrics Handlers
// ============================================

async fn get_metrics(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    let error_rate = if snapshot.requests_total > 0 {
        snapshot.errors_total as f64 / snapshot.requests_total as f64
    } else {
        0.0
    };
    Json(serde_json::json!({
        "metrics": snapshot,
        "error_rate": format!("{:.2}%", error_rate * 100.0),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
    .into_response()
}

async fn get_prometheus_metrics(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    let mut output = String::new();

    output.push_str("# HELP forge_requests_total Total number of HTTP requests\n");
    output.push_str("# TYPE forge_requests_total counter\n");
    output.push_str(&format!(
        "forge_requests_total {}\n",
        snapshot.requests_total
    ));

    output.push_str("# HELP forge_active_sessions Number of active sessions\n");
    output.push_str("# TYPE forge_active_sessions gauge\n");
    output.push_str(&format!(
        "forge_active_sessions {}\n",
        snapshot.active_sessions
    ));

    output.push_str("# HELP forge_active_agents Number of active pi agents\n");
    output.push_str("# TYPE forge_active_agents gauge\n");
    output.push_str(&format!("forge_active_agents {}\n", snapshot.active_agents));

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
        .into_response()
}

// ============================================
// Authentication Middleware
// ============================================

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;

async fn auth_middleware(request: Request<Body>, next: Next) -> Response {
    let path = request.uri().path();

    // Paths that intentionally bypass X-API-Key auth:
    //
    //   * `/health` and `/metrics*` — health checks and
    //     observability scrape endpoints, typically polled by
    //     load balancers / Prometheus.
    //   * `/auth/register` and `/auth/login` — you can't
    //     authenticate before you have an account or a key.
    //   * `/auth/logout` — clearing a session doesn't strictly
    //     need a key (the caller is throwing it away), and
    //     logouts are commonly invoked from a stale UI that no
    //     longer has the key in memory.
    //   * `/tools/execute` and `/tools/execute/stream` — these
    //     are called by the in-process `forge-tools` extension
    //     (and by tests) without an API key, since the request
    //     comes from the same trust boundary as the API itself.
    //
    // Everything else (`/profiles`, `/sessions`, `/messages`,
    // `/api-keys`, ...) requires an X-API-Key. Note: this list
    // matches exact paths, so the parameterized axum routes
    // (`/sessions/:id`, `/profiles/:id`) are NOT in the
    // allowlist — the middleware falls through to the header
    // check for those.
    match path {
        "/health"
        | "/metrics"
        | "/metrics/prometheus"
        | "/auth/register"
        | "/auth/login"
        | "/auth/logout"
        | "/tools/execute"
        | "/tools/execute/stream" => {
            return next.run(request).await;
        }
        _ => {}
    }

    match request.headers().get("X-API-Key") {
        Some(_) => next.run(request).await,
        None => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Missing X-API-Key header"})),
        )
            .into_response(),
    }
}

// ============================================
// Routes Aggregation
// ============================================

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
        .route("/metrics/prometheus", get(get_prometheus_metrics))
        .route("/auth/register", post(auth::register))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/api-keys", get(auth::list_api_keys))
        .route("/api-keys", post(auth::create_api_key))
        .route("/api-keys/:id", get(auth::get_api_key))
        .route("/api-keys/:id", delete(auth::delete_api_key))
        .route("/users", get(auth::list_users))
        .route("/users/:id", get(auth::get_user))
        .route("/users/:id", patch(auth::update_user))
        .route("/users/:id", delete(auth::delete_user))
        .route("/profiles", post(create_profile))
        .route("/profiles", get(list_profiles))
        .route("/profiles/get", get(get_profile_by_id))
        .route("/profiles/delete", delete(delete_profile_by_id))
        .route("/profiles/update", patch(update_profile_by_id))
        .route("/profiles/:id", get(get_profile_by_uuid))
        .route("/profiles/:id", delete(delete_profile_by_uuid))
        .route("/sessions", post(create_session))
        .route("/sessions", get(list_all_sessions))
        .route("/sessions/get", get(get_session_by_id))
        .route("/sessions/delete", delete(delete_session_by_id))
        .route("/sessions/:id", get(get_session_by_uuid))
        .route("/sessions/:id", delete(delete_session_by_uuid))
        .route("/messages", get(list_messages_by_session))
        .route("/messages", post(create_message))
        .route("/tools/execute", post(execute_tool))
        .route("/tools/execute/stream", post(sse::stream_tool_execution))
        .route("/sessions/:id/events", get(events::stream_session_events))
        .route("/sandbox/containers", get(list_sandbox_containers))
        .route("/sandbox/sessions/:id", post(create_sandbox_for_session))
        .route("/sandbox/sessions/:id", delete(destroy_sandbox_for_session))
        .route(
            "/admin/self-update",
            post(self_update)
                // The release binary is ~10 MiB; axum's default
                // 2 MiB request-body limit would reject it. The
                // `/admin/self-update` endpoint takes the new
                // binary as its raw request body, so disable the
                // limit on this route only. The auth middleware
                // still gates access.
                .layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/admin/sandbox-reset", post(reset_sandbox))
        .layer(axum::middleware::from_fn(auth_middleware))
}
