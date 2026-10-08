//! Herd H2.6: lazy migration of legacy sessions onto the harness.
//!
//! The cutover deleted the legacy turn driver (`drive_turn`, the `pi`
//! subprocess, durable resume / `session_replay`). There is no longer
//! a second turn path: every session must own a durable harness
//! conversation before it can be written to. Sessions that predate
//! cutover (`sessions.durable_conversation_id IS NULL`) are migrated
//! **lazily, on their first write operation** (POST /messages,
//! compact, timer set, reset, document put) by
//! [`ensure_migrated`]:
//!
//! 1. **Claim** the migration (migration 021): an atomic
//!    `UPDATE … WHERE durable_conversation_id IS NULL AND
//!    (harness_migrating = FALSE OR claim is >10 min old)` so two
//!    concurrent writes to the same session run the migration
//!    exactly once. Losers poll the stamp instead.
//! 2. **Create** the durable conversation with the same parameters
//!    the H2.1 session-creation attach uses
//!    ([`crate::harness::conversation_params`]).
//! 3. **Import** the session's `messages` transcript as pi-durable
//!    entries in one `importEntries` commit
//!    ([`messages_to_entries`] — the entry mapping moved here from
//!    the now-deleted `session_replay.rs`, which replayed the same
//!    rows into a pi session jsonl).
//! 4. **Stamp** `sessions.durable_conversation_id` and clear the
//!    claim.
//!
//! On any failure the claim is released (stamped-or-stale sessions
//! never re-run the import; the session simply stays unmigrated and
//! the next write retries — including after the 10-minute
//! staleness window re-arms a claim left behind by a crash).
//!
//! The `messages` table stays the flat audit projection the whole
//! time: the harness event consumer keeps writing it
//! (`crate::harness::project_turn_end`), and the import only ever
//! READS from it.

use sqlx::PgPool;
use uuid::Uuid;

use crate::api::AppState;
use crate::db::{Message, Profile, Session};
use crate::recording::{DbToolRecorder, ToolRecorder, ToolResultRecord};

/// How long a loser of the migration claim polls the session for a
/// stamp before giving up with `InProgress` (503). 120 × 250 ms ≈
/// 30 s — plenty for even a large transcript import.
const WAIT_POLL_MS: u64 = 250;
const WAIT_POLLS: u32 = 120;

/// Failure classification for the API's response mapping.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// `FORGE_HARNESS_MESSAGES=0` — the cutover kill switch is off:
    /// no migration and no legacy fallback, so the write cannot
    /// happen at all.
    #[error("harness disabled (FORGE_HARNESS_MESSAGES=0); writes are unavailable")]
    HarnessDisabled,
    /// The harness socket is disabled (the API booted without
    /// `FORGE_HARNESS_SOCKET`, or the socket was absent at startup).
    #[error("harness unavailable (disabled mode): {0}")]
    Unavailable(String),
    /// Another caller is running the migration and it has not
    /// stamped the session yet (the claim is not yet stale).
    #[error("session migration in progress; try again shortly")]
    InProgress,
    /// The migration was attempted but failed (createConversation,
    /// importEntries, or the stamp). The claim was released; the
    /// next write retries.
    #[error("migration failed: {0}")]
    Failed(String),
}

/// The session's durable conversation id, stamped or not.
pub async fn stamped_conversation(db: &PgPool, session_id: Uuid) -> Option<i64> {
    sqlx::query_scalar("SELECT durable_conversation_id FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
}

/// Migrate the session if needed and return its durable conversation
/// id. `exclude_sequence` (the caller's own just-inserted row, when
/// the call came from `dispatch_message`) is excluded from the
/// import: it is about to be submitted through the harness as a
/// normal input, and importing it too would make the model see the
/// prompt twice. Rows are imported with `sequence < exclude_sequence`
/// (atomic `get_next_sequence` allocation guarantees that is
/// exactly the transcript as it stood before the caller's row —
/// *when the claim and the caller's row landed in one transaction*,
/// which `dispatch_message` does; see `claim_and_insert_user_message`.
/// Other write paths pass `None` and import everything).
///
/// Idempotent and concurrency-safe: stamped sessions return their
/// stamp without touching the harness; concurrent claim losers poll
/// until the winner stamps.
pub async fn ensure_migrated(
    state: &AppState,
    session_id: Uuid,
    exclude_sequence: Option<i32>,
) -> Result<i64, MigrationError> {
    if !state.harness_messages {
        return Err(MigrationError::HarnessDisabled);
    }
    if !state.harness.is_enabled() {
        return Err(MigrationError::Unavailable(
            "harness socket disabled at startup (FORGE_HARNESS_SOCKET unset or absent)".into(),
        ));
    }

    if let Some(id) = stamped_conversation(&state.db, session_id).await {
        return Ok(id);
    }

    if claim_migration(&state.db, session_id).await? {
        run_migration_claimed(state, session_id, exclude_sequence).await
    } else {
        wait_for_stamp(state, session_id).await
    }
}

/// The atomic migration claim (migration 021): `true` when this
/// caller owns the migration (it must then call
/// [`run_migration_claimed`]), `false` when the session is already
/// stamped or another caller is mid-migration (call
/// [`wait_for_stamp`]).
///
/// `dispatch_message` runs this UPDATE in the SAME transaction as the
/// user row's insert (see `claim_and_insert_user_message`): that is
/// what makes the import's sequence cap race-free under concurrent
/// writes — a claim loser's row can never be allocated below the
/// winner's row, so the winner's `sequence < cap` fetch can never
/// import a prompt that its own dispatch will also submit.
pub async fn claim_migration(db: &PgPool, session_id: Uuid) -> Result<bool, MigrationError> {
    let claimed = sqlx::query(
        r#"UPDATE sessions
              SET harness_migrating = TRUE, harness_migration_at = NOW()
            WHERE id = $1
              AND durable_conversation_id IS NULL
              AND (harness_migrating = FALSE
                   OR harness_migration_at < NOW() - INTERVAL '10 minutes')"#,
    )
    .bind(session_id)
    .execute(db)
    .await
    .map_err(|e| MigrationError::Failed(e.to_string()))?;
    Ok(claimed.rows_affected() > 0)
}

/// Run a migration whose claim this caller already owns (either via
/// [`claim_migration`] or the in-transaction claim in
/// `dispatch_message`). Creates the durable conversation, imports the
/// transcript, and stamps the session; on failure the claim is
/// released so the next write retries.
pub async fn run_migration_claimed(
    state: &AppState,
    session_id: Uuid,
    exclude_sequence: Option<i32>,
) -> Result<i64, MigrationError> {
    match run_migration(state, session_id, exclude_sequence).await {
        Ok(id) => Ok(id),
        Err(e) => {
            release_claim(&state.db, session_id).await;
            Err(e)
        }
    }
}

/// Poll the session for a stamp until the winner of the claim
/// finishes (or the wait budget runs out → [`MigrationError::InProgress`]).
pub async fn wait_for_stamp(state: &AppState, session_id: Uuid) -> Result<i64, MigrationError> {
    for _ in 0..WAIT_POLLS {
        tokio::time::sleep(std::time::Duration::from_millis(WAIT_POLL_MS)).await;
        if let Some(id) = stamped_conversation(&state.db, session_id).await {
            return Ok(id);
        }
    }
    tracing::warn!(
        session_id = %session_id,
        "migration claim wait timed out; the session is still unmigrated"
    );
    Err(MigrationError::InProgress)
}

/// Clear a claim we will not fulfill (migration failed). A crash
/// leaves the claim set; the 10-minute staleness window re-arms it.
async fn release_claim(db: &PgPool, session_id: Uuid) {
    if let Err(e) = sqlx::query("UPDATE sessions SET harness_migrating = FALSE WHERE id = $1")
        .bind(session_id)
        .execute(db)
        .await
    {
        tracing::error!(session_id = %session_id, error = %e, "failed to release the migration claim (the 10-minute staleness window re-arms it)");
    }
}

/// Run the claimed migration: create the durable conversation, import
/// the transcript, stamp the session.
async fn run_migration(
    state: &AppState,
    session_id: Uuid,
    exclude_sequence: Option<i32>,
) -> Result<i64, MigrationError> {
    let started = std::time::Instant::now();

    let session: Session = sqlx::query_as("SELECT * FROM sessions WHERE id = $1")
        .bind(session_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| MigrationError::Failed(e.to_string()))?;
    let profile: Profile = sqlx::query_as("SELECT * FROM profiles WHERE id = $1")
        .bind(session.profile_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| MigrationError::Failed(e.to_string()))?;

    // 1. Create the durable conversation (same parameters as the
    // H2.1 attach: model resolution + agent tooling).
    let params = crate::harness::conversation_params(&state.db, &session, &profile).await;
    let conversation_id = state
        .harness
        .client()
        .create_conversation(&params)
        .await
        .map_err(|e| MigrationError::Unavailable(e.to_string()))?;

    // 2. Import the transcript.
    let imported = import_legacy_messages(state, session_id, conversation_id, exclude_sequence)
        .await
        .map_err(|e| MigrationError::Failed(e.to_string()))?;

    // 3. Stamp + clear the claim in one UPDATE.
    sqlx::query(
        "UPDATE sessions SET durable_conversation_id = $1, harness_migrating = FALSE WHERE id = $2",
    )
    .bind(conversation_id)
    .bind(session_id)
    .execute(&state.db)
    .await
    .map_err(|e| MigrationError::Failed(e.to_string()))?;

    tracing::info!(
        session_id = %session_id,
        conversation_id,
        imported_rows = imported,
        elapsed_ms = started.elapsed().as_millis(),
        provider = %params.provider,
        model = %params.model_id,
        "legacy session migrated to a durable harness conversation (H2.6)"
    );
    Ok(conversation_id)
}

/// Import a legacy session's `messages` rows into `conversation_id` as
/// pi-durable entries, in one `importEntries` commit. Returns the
/// number of entries written (0 for a session with no importable rows
/// — the call is skipped entirely, not an error).
///
/// Orphaned tool calls (a session that died mid-tool before the
/// cutover) are healed first: [`abandon_orphan_calls`] inserts
/// synthetic "abandoned" result rows so every `toolCall` block in the
/// import has a matching `toolResult` — the same rule the deleted
/// jsonl replay enforced, because pi-durable would otherwise reject
/// the imported context the same way Anthropic did.
///
/// `exclude_sequence` caps the import to `sequence < exclude_sequence`
/// (see [`ensure_migrated`]); the synthetic janitor rows are appended
/// explicitly because their sequences are allocated after the cap.
async fn import_legacy_messages(
    state: &AppState,
    session_id: Uuid,
    conversation_id: i64,
    exclude_sequence: Option<i32>,
) -> Result<usize, sqlx::Error> {
    let healed = abandon_orphan_calls(&state.db, session_id).await?;

    let messages: Vec<Message> = match exclude_sequence {
        Some(cap) => {
            let mut msgs = sqlx::query_as::<_, Message>(
                "SELECT * FROM messages WHERE session_id = $1 AND sequence < $2 ORDER BY sequence ASC",
            )
            .bind(session_id)
            .bind(cap)
            .fetch_all(&state.db)
            .await?;
            msgs.extend(healed);
            msgs
        }
        None => {
            // The janitor already ran: its rows are in this SELECT.
            sqlx::query_as::<_, Message>(
                "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
            )
            .bind(session_id)
            .fetch_all(&state.db)
            .await?
        }
    };

    // Provider + model for the synthesized assistant entry metadata
    // (same resolution the jsonl replay used: session override, then
    // profile).
    let (provider, model): (String, String) =
        sqlx::query_as(
            "SELECT COALESCE(s.override_provider, p.provider), COALESCE(s.override_model, p.model) FROM profiles p JOIN sessions s ON s.profile_id = p.id WHERE s.id = $1",
        )
        .bind(session_id)
        .fetch_one(&state.db)
        .await?;

    let entries = messages_to_entries(&messages, &provider, &model);
    if entries.is_empty() {
        tracing::info!(
            session_id = %session_id,
            "migration: session has no importable rows; importing zero entries"
        );
        return Ok(0);
    }
    // One bulk commit on the harness (not one RPC per entry): the
    // import must be durably in the conversation before the caller's
    // prompt is submitted, and the sequential IPC socket gives that
    // ordering once this call's response has been read.
    state
        .harness
        .client()
        .import_entries(conversation_id, &entries)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("harness importEntries failed: {e}")))?;
    Ok(entries.len())
}

/// Convert forge `messages` rows into pi-durable entry drafts
/// (pi-durable `EntryDraft`: `{ kind, model: [message], data? }`),
/// applying the ordering / dedup / orphan-drop / placeholder-drop
/// rules the legacy jsonl replay (`session_replay.rs`, deleted in
/// H2.6) applied so a rebuilt context is always valid.
///
/// Mapping (row → entry):
///
/// * `role='user'` → `pi.user` (the prompt as plain text).
/// * `role='assistant'`, no `tool_call_id` → `pi.assistant` with a
///   single `text` content block. (Thinking blocks are not persisted
///   in `messages`; the model can re-think.)
/// * `role='assistant'`, `tool_call_id IS NOT NULL` → `pi.assistant`
///   with a single `toolCall` content block (`arguments` = the
///   `tool_input` jsonb).
/// * `role='tool'` → `pi.tool-result` (a single `text` block from the
///   recorded human-readable `content`; `isError` from the
///   `tool_output.success` field; structured `tool_output` under
///   `details`).
/// * `role='system'` and forge-side placeholder rows → dropped
///   (session metadata / "no response" markers, not conversation).
/// * Unhandled roles → dropped with a warn log (the messages table
///   only ever carries user/assistant/tool/system).
pub fn messages_to_entries(
    messages: &[Message],
    provider: &str,
    model: &str,
) -> Vec<serde_json::Value> {
    let mut last_result_seq: std::collections::HashMap<String, i32> =
        std::collections::HashMap::new();
    let mut known_call_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut result_by_call_id: std::collections::HashMap<String, &Message> =
        std::collections::HashMap::new();
    for msg in messages.iter() {
        match (msg.role.as_str(), &msg.tool_call_id) {
            ("assistant", Some(tcid)) => {
                known_call_ids.insert(tcid.clone());
            }
            ("tool", Some(tcid)) => {
                last_result_seq.insert(tcid.clone(), msg.sequence);
                result_by_call_id.insert(tcid.clone(), msg);
            }
            _ => {}
        }
    }

    let mut emitted_result_seqs: std::collections::HashSet<i32> = std::collections::HashSet::new();
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(messages.len());

    for msg in messages {
        match (msg.role.as_str(), &msg.tool_call_id) {
            // Tool call: emit the call, then its (last) result
            // immediately after — regardless of when the result row
            // actually landed. This is what keeps the imported
            // context a valid call/result-adjacent sequence (the rule
            // that kept the jsonl replay from Anthropic's 999).
            ("assistant", Some(tcid)) => {
                if let Some(e) = forge_to_entry(msg, provider, model) {
                    out.push(e);
                }
                if let Some(result) = result_by_call_id.get(tcid) {
                    emitted_result_seqs.insert(result.sequence);
                    if let Some(e) = forge_to_entry(result, provider, model) {
                        out.push(e);
                    }
                } else {
                    tracing::warn!(
                        sequence = msg.sequence,
                        tool_call_id = %tcid,
                        "migration: tool call has no matching result row; importing it without a result (abandon_orphan_calls should have healed this)"
                    );
                }
            }
            // Tool result: skip if already emitted with its call. Drop
            // orphans (no matching call) and duplicates (not the last
            // result for the call id).
            ("tool", Some(tcid)) => {
                if !known_call_ids.contains(tcid) {
                    tracing::warn!(
                        tool_call_id = %tcid,
                        sequence = msg.sequence,
                        "migration: dropping orphaned tool result whose call id has no matching assistant toolCall"
                    );
                    continue;
                }
                if last_result_seq.get(tcid) != Some(&msg.sequence) {
                    tracing::warn!(
                        tool_call_id = %tcid,
                        sequence = msg.sequence,
                        "migration: dropping duplicate tool result; keeping only the last one for this call id"
                    );
                    continue;
                }
                if emitted_result_seqs.contains(&msg.sequence) {
                    continue;
                }
                if let Some(e) = forge_to_entry(msg, provider, model) {
                    out.push(e);
                }
            }
            // Everything else: emit in sequence order (placeholders +
            // system rows map to `None` inside `forge_to_entry`).
            _ => {
                if let Some(e) = forge_to_entry(msg, provider, model) {
                    out.push(e);
                }
            }
        }
    }
    out
}

/// One `messages` row → one pi-durable entry draft, or `None` for
/// rows that must not enter the conversation.
fn forge_to_entry(msg: &Message, provider: &str, model: &str) -> Option<serde_json::Value> {
    match (msg.role.as_str(), &msg.tool_call_id) {
        ("user", None) => Some(serde_json::json!({
            "kind": "pi.user",
            "model": [{
                "role": "user",
                "content": msg.content.clone().unwrap_or_default(),
                "timestamp": msg.created_at.timestamp_millis(),
            }],
        })),

        ("assistant", None) => {
            let text = msg.content.clone().unwrap_or_default();
            // Forge-side placeholder rows: not real LLM output.
            if text == "No response from agent (timed out?)" || text == "[no response from agent]" {
                tracing::info!(
                    sequence = msg.sequence,
                    "migration: skipping forge-side placeholder row"
                );
                return None;
            }
            Some(serde_json::json!({
                "kind": "pi.assistant",
                "model": [{
                    "role": "assistant",
                    "content": [{ "type": "text", "text": text }],
                    "api": "anthropic-messages",
                    "provider": provider,
                    "model": model,
                    "usage": empty_usage(),
                    "stopReason": "stop",
                    "timestamp": msg.created_at.timestamp_millis(),
                }],
            }))
        }

        ("assistant", Some(tool_call_id)) => {
            let tool_name = msg.tool_name.clone().unwrap_or_default();
            let arguments = msg.tool_input.clone().unwrap_or(serde_json::Value::Null);
            Some(serde_json::json!({
                "kind": "pi.assistant",
                "model": [{
                    "role": "assistant",
                    "content": [{
                        "type": "toolCall",
                        "id": tool_call_id,
                        "name": tool_name,
                        "arguments": arguments,
                    }],
                    "api": "anthropic-messages",
                    "provider": provider,
                    "model": model,
                    "usage": empty_usage(),
                    "stopReason": "toolUse",
                    "timestamp": msg.created_at.timestamp_millis(),
                }],
            }))
        }

        ("tool", tool_call_id) => {
            let text = msg.content.clone().unwrap_or_default();
            let mut message = serde_json::json!({
                "role": "toolResult",
                "toolCallId": tool_call_id.clone().unwrap_or_default(),
                "toolName": msg.tool_name.clone().unwrap_or_default(),
                "content": [{ "type": "text", "text": text }],
                "isError": extract_is_error(msg),
                "timestamp": msg.created_at.timestamp_millis(),
            });
            if let Some(out) = &msg.tool_output {
                message
                    .as_object_mut()
                    .unwrap()
                    .insert("details".to_string(), out.clone());
            }
            Some(serde_json::json!({
                "kind": "pi.tool-result",
                "model": [message],
                "data": { "diagnostics": [] },
            }))
        }

        // Session-level metadata, not part of the LLM's conversation.
        ("system", _) => None,

        // The messages table only carries user/assistant/tool/system.
        (role, _) => {
            tracing::warn!(
                sequence = msg.sequence,
                role,
                "migration: dropping row with an unhandled role"
            );
            None
        }
    }
}

/// Heal orphaned tool-call rows in `session_id`'s audit log: for
/// every `role = 'assistant'` row that has a `tool_call_id` but no
/// matching `role = 'tool'` result row, insert a synthetic
/// "abandoned" result row so that the imported context is a valid
/// call/result-adjacent sequence (moved verbatim from the
/// now-deleted `session_replay.rs`; the janitor still heals live
/// sessions in place).
///
/// Rows are written through the standard [`DbToolRecorder`] path, so
/// the sequence allocation reuses the advisory-locked
/// `get_next_sequence` allocator and the insert is idempotent on
/// `(session_id, tool_call_id, role)` — calling this on a session
/// whose orphans were already healed returns an empty vec.
///
/// Returns the synthetic rows that were inserted (empty when there
/// was nothing to heal).
pub async fn abandon_orphan_calls(
    pool: &PgPool,
    session_id: Uuid,
) -> Result<Vec<Message>, sqlx::Error> {
    let orphans: Vec<(String, Option<String>)> = sqlx::query_as(
        r#"SELECT m.tool_call_id, m.tool_name
            FROM messages m
           WHERE m.session_id = $1
             AND m.role = 'assistant'
             AND m.tool_call_id IS NOT NULL
             AND NOT EXISTS (
                   SELECT 1 FROM messages r
                    WHERE r.session_id = m.session_id
                      AND r.role = 'tool'
                      AND r.tool_call_id = m.tool_call_id
             )"#,
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;

    if orphans.is_empty() {
        return Ok(Vec::new());
    }

    let recorder = DbToolRecorder::new(pool.clone());
    let mut healed = Vec::with_capacity(orphans.len());
    for (tool_call_id, tool_name) in &orphans {
        healed.push(
            recorder
                .record_result(ToolResultRecord {
                    session_id,
                    tool_call_id: tool_call_id.clone(),
                    tool_name: tool_name.clone().unwrap_or_else(|| "unknown".to_string()),
                    content: "[abandoned: no result recorded]".to_string(),
                    output: serde_json::json!({
                        "error": "no result recorded (orphaned tool call)",
                        "success": false
                    }),
                    is_error: true,
                    duration_ms: None,
                })
                .await?,
        );
    }

    tracing::warn!(
        session_id = %session_id,
        "session {session_id}: healed {} orphaned tool calls with synthetic abandoned rows",
        healed.len()
    );
    Ok(healed)
}

/// `is_error` for a tool row. The recorder stores a `success`
/// boolean in the `tool_output` jsonb; absent that, fall back to an
/// explicit `is_error` field (older rows) and finally to "not
/// error".
fn extract_is_error(msg: &Message) -> bool {
    if let Some(out) = &msg.tool_output {
        if let Some(success) = out.get("success").and_then(|v| v.as_bool()) {
            return !success;
        }
        if let Some(is_error) = out.get("is_error").and_then(|v| v.as_bool()) {
            return is_error;
        }
    }
    false
}

fn empty_usage() -> serde_json::Value {
    serde_json::json!({
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 0,
        "cost": {
            "input": 0.0,
            "output": 0.0,
            "cacheRead": 0.0,
            "cacheWrite": 0.0,
            "total": 0.0,
        },
    })
}
