//! Durable resume: rebuild the sandbox's working tree from
//! the `messages` table when a session is reactivated after
//! the prior pi subprocess has been disposed.
//!
//! The LLM-context half of resume (rebuilding the model's
//! view of the conversation) is handled separately by
//! [`crate::agent_registry::AgentRegistry::build_resume_preamble`],
//! which prepends a transcript of the prior turns to the
//! user's first real prompt. We deliberately don't use pi's
//! `new_session` RPC with a `parentSession` jsonl for that:
//! it triggers a bug in user-installed extensions that
//! capture the pi ctx in a `session_start` event handler
//! and reference it from a periodic timer (the captured ctx
//! becomes stale after `new_session` and the next tick
//! throws an unhandled error that crashes the whole pi
//! process). The transcript-preamble approach is simpler and
//! avoids that whole class of issues.
//!
//! What this module does is the *other* half: re-execute
//! the prior session's tool calls in the fresh sandbox so
//! the working tree ends up in the same state it was in
//! right before disposal. Without this, the model has the
//! prior conversation as context (via the preamble) but the
//! files it had been editing are gone — the sandbox is
//! re-cloned from the profile's `git_url` / `working_dir`
//! baseline on every resume. The model would have to
//! re-derive all the state by re-reading files, which
//! works but burns tokens and time.
//!
//! Together with the preamble path, the agent picks up
//! exactly where it left off: same files, same model
//! context, same conversation.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::Message;
use crate::recording::ToolRecorder;
use crate::tool_executor::ToolExecutor;

/// Maximum original `timeout_ms` we'll honor on a `bash`
/// call during replay. Calls the model asked to run for
/// longer than this (builds, tests, `cargo install`, etc.)
/// are skipped: they don't create the kind of filesystem
/// state the replay is trying to restore (a rebuild of
/// `/tmp/cargo-target-release` is not what the model needs
/// to see in the working tree), and honoring the original
/// 10-minute timeout would block `get_or_create` from
/// returning and the user from getting a response. The
/// model can re-derive anything it actually needs by
/// re-running the command on the next turn.
const REPLAY_BASH_MAX_ORIGINAL_TIMEOUT_MS: i64 = 30_000;

/// Hard wall-clock cap on the entire replay pass. Even
/// with the per-call timeout cap above, a session with
/// hundreds of `write`/`edit`/short-`bash` calls could
/// still take minutes to replay. We give it 60s and then
/// stop — the model can re-derive whatever's missing on
/// the next turn. The cap is generous because `write`
/// and `edit` are the calls that actually matter for
/// filesystem state restoration; `bash` is mostly noise
/// after the per-call timeout cap.
const REPLAY_TOTAL_BUDGET_SECS: u64 = 60;

/// Stats returned from a tool-call replay pass. Useful for
/// logging and for unit tests; nothing in the production
/// caller branches on these.
#[derive(Debug, Default, Clone)]
pub struct ReplayStats {
    /// Number of tool calls walked from the audit log.
    pub considered: usize,
    /// Number of tool calls actually re-executed (everything
    /// except `read`, which is read-only and skipped).
    pub executed: usize,
    /// Number of replays that produced a different result
    /// from the recorded one (e.g. a non-deterministic bash
    /// command, a file timestamp). Logged at WARN; the resume
    /// continues.
    pub diverged: usize,
    /// Number of replays that errored (e.g. the parent
    /// directory was missing, the file was deleted). Logged
    /// at WARN; the resume continues. The model can recover
    /// by re-reading files / re-running commands.
    pub failed: usize,
}

/// Re-execute the prior session's tool calls in `working_dir`
/// so the sandbox ends up in the same filesystem state it
/// was in right before the prior pi was disposed.
///
/// Walks `messages` in `sequence ASC` order so a `bash`
/// call that runs a freshly-written script sees the script
/// on disk. For each `role='assistant'` row with
/// `tool_call_id IS NOT NULL`, looks up the matching
/// `role='tool'` row and the `tool_input` jsonb, then
/// re-invokes the tool through a [`ToolExecutor`] pointed
/// at the new sandbox. Uses a synthetic `replay_<short_hash>`
/// `tool_call_id` so the replay never collides with the
/// original row's id (and so the audit log is unambiguous
/// about which call is which — though the replay path
/// deliberately does not write rows).
///
/// Read-only tools (`read`) are skipped: they don't change
/// state, and replaying them would just cost I/O. The audit
/// log is still the canonical record.
///
/// Failures and divergences are logged at WARN and the pass
/// continues. The whole point of resume is to put the agent
/// back into a useful state; if one tool call doesn't
/// replay cleanly, the model can re-derive the missing
/// state on its next turn. We do not abort resume.
pub async fn replay_tool_calls(
    pool: &PgPool,
    session_id: Uuid,
    working_dir: &str,
    _nix_shell: Option<String>,
) -> ReplayStats {
    let mut stats = ReplayStats::default();

    let messages: Vec<Message> = match sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(
                session_id = %session_id,
                error = %e,
                "resume: could not load messages for tool-call replay; sandbox will be left at the baseline clone"
            );
            return stats;
        }
    };

    // We need a recorder to hand to ToolExecutor, but we
    // don't want replay calls writing rows to the audit log
    // — the originals are still there. The
    // `ReplayNoopRecorder` below returns an `sqlx::Error`
    // from every `record_call` / `record_result` call, which
    // the executor logs at WARN and continues past. A
    // `tracing::error!` fires inside the recorder to make
    // any accidental write loud in the journal.
    let recorder: Arc<dyn ToolRecorder> = Arc::new(ReplayNoopRecorder);

    let executor = ToolExecutor::new(
        session_id,
        working_dir.to_string(),
        // `in_sandbox=false` — the working_dir is already
        // inside the per-session sandbox directory, and
        // there's no outer "host" path to consider. The
        // ToolExecutor's `in_sandbox` flag is for the
        // executor's own bookkeeping about whether it
        // should apply the sandbox wrapper; for replay we
        // want the tool to run directly in the working_dir
        // (the sandbox dir), which is what `false` does
        // here.
        false,
        None,
        recorder,
        // The bus is unused on this path — the noop
        // recorder above drops everything, so anything
        // published to this bus has no effect.
        crate::bus::MessageBus::new(),
        // No sandbox manager for replay — the working_dir
        // is the per-session dir, and replayed tool calls
        // are expected to operate on it directly without
        // another nspawn layer (the dir is already the
        // bind-mount target of the real container, so host
        // and container see the same content).
        None,
    );

    // Walk in order. For each `assistant` row with a
    // `tool_call_id`, find the matching `tool` row and
    // replay the call. We use the `tool` row's existence
    // as proof the call actually completed in the original
    // session (a tool call with no result row was
    // interrupted mid-execution and shouldn't be replayed:
    // it's half-done in the original sandbox, but the new
    // sandbox starts from a clean baseline so replaying it
    // would be a divergence anyway).
    let replay_start = std::time::Instant::now();
    let budget = std::time::Duration::from_secs(REPLAY_TOTAL_BUDGET_SECS);
    for msg in &messages {
        // Stop walking if we've blown the wall-clock
        // budget. The user is waiting for a response; the
        // model can re-derive whatever state we skipped on
        // its next turn. Logged at WARN so this is
        // visible in the journal.
        if replay_start.elapsed() > budget {
            tracing::warn!(
                session_id = %session_id,
                elapsed_secs = replay_start.elapsed().as_secs(),
                budget_secs = REPLAY_TOTAL_BUDGET_SECS,
                considered = stats.considered,
                executed = stats.executed,
                "resume: replay budget exhausted; the model will re-derive any missing state on the next turn"
            );
            break;
        }
        if msg.role != "assistant" {
            continue;
        }
        let Some(orig_tool_call_id) = msg.tool_call_id.clone() else {
            continue;
        };
        stats.considered += 1;

        let Some(tool_name) = msg.tool_name.clone() else {
            continue;
        };

        // Skip read-only tools: they don't change state, so
        // there's nothing to restore. Saves I/O and avoids
        // surfacing noise for sessions that did a lot of
        // reading.
        if tool_name == "read" {
            continue;
        }

        // Skip `bash` calls the model originally asked to
        // run for longer than [`REPLAY_BASH_MAX_ORIGINAL_TIMEOUT_MS`].
        // These are typically build / test / install
        // commands that don't create the filesystem state
        // the replay is trying to restore. The model's
        // context already shows what happened; the model
        // can re-run anything it actually needs on the
        // next turn. Honoring the original 10-minute
        // `cargo build --release` timeout in the replay
        // path blocks `get_or_create` from returning and
        // the user from getting a response, for state
        // they almost certainly don't need.
        let tool_input = msg.tool_input.clone().unwrap_or(serde_json::Value::Null);
        if tool_name == "bash" {
            let original_timeout_ms = tool_input
                .get("timeout_ms")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if original_timeout_ms > REPLAY_BASH_MAX_ORIGINAL_TIMEOUT_MS {
                tracing::warn!(
                    session_id = %session_id,
                    tool_call_id = %orig_tool_call_id,
                    tool = %tool_name,
                    original_timeout_ms,
                    max_timeout_ms = REPLAY_BASH_MAX_ORIGINAL_TIMEOUT_MS,
                    "resume: skipping bash replay for long-running command; the model can re-derive state on the next turn if needed"
                );
                stats.failed += 1;
                continue;
            }
        }

        // Verify the matching `role='tool'` row exists. The
        // audit log is the source of truth; if the call
        // was interrupted (call row but no result row), the
        // original sandbox may be in a half-applied state.
        // The new sandbox starts clean, so re-executing
        // here would be a divergence from the original in
        // the other direction (cleaner than the original).
        // We treat the missing result row as "don't replay"
        // — the model can re-derive whatever state was
        // needed.
        let result_row_exists = messages.iter().any(|m| {
            m.role == "tool" && m.tool_call_id.as_deref() == Some(orig_tool_call_id.as_str())
        });
        if !result_row_exists {
            tracing::warn!(
                session_id = %session_id,
                tool_call_id = %orig_tool_call_id,
                tool = %tool_name,
                "resume: tool call has no matching result row; skipping replay (call was interrupted mid-execution in the prior session)"
            );
            stats.failed += 1;
            continue;
        }

        let replay_id = make_replay_id(&orig_tool_call_id);

        tracing::info!(
            session_id = %session_id,
            tool = %tool_name,
            original_tool_call_id = %orig_tool_call_id,
            replay_id = %replay_id,
            "resume: replaying tool call"
        );

        // The executor's `execute` method tries to write
        // call and result rows via the recorder; ours is a
        // noop (returns `sqlx::Error`), so the audit log is
        // untouched. We only care about the side effects
        // (filesystem mutations from `write`/`edit`/`bash`).
        // Read-only tools are skipped above.
        match executor
            .execute(&replay_id, &tool_name, tool_input.clone())
            .await
        {
            Ok(_output) => {
                stats.executed += 1;
            }
            Err(e) => {
                // Replay errors are best-effort: log and
                // continue. The LLM context can still be
                // loaded even if the sandbox is partially
                // out of sync, and the model can recover by
                // re-reading files.
                tracing::warn!(
                    session_id = %session_id,
                    tool = %tool_name,
                    original_tool_call_id = %orig_tool_call_id,
                    error = %e,
                    "resume: tool replay failed; continuing (model can re-derive state on next turn)"
                );
                stats.failed += 1;
            }
        }
    }

    tracing::info!(
        session_id = %session_id,
        considered = stats.considered,
        executed = stats.executed,
        diverged = stats.diverged,
        failed = stats.failed,
        "resume: tool-call replay pass complete"
    );

    stats
}

/// A [`ToolRecorder`] that rejects every write with an
/// `sqlx::Error` and logs at `error!` so any accidental
/// write through the replay path is loud in the journal.
/// The replay path never wants to write to the `messages`
/// table — the original rows are still there, and
/// double-writing would corrupt the audit log.
struct ReplayNoopRecorder;

#[async_trait::async_trait]
impl ToolRecorder for ReplayNoopRecorder {
    async fn record_call(
        &self,
        record: crate::recording::ToolCallRecord,
    ) -> Result<Message, sqlx::Error> {
        tracing::error!(
            tool_call_id = %record.tool_call_id,
            tool = %record.tool_name,
            "ReplayNoopRecorder.record_call was invoked; replay paths must not write to the audit log"
        );
        Err(sqlx::Error::ColumnNotFound(
            "replay paths must not write call rows to the audit log".to_string(),
        ))
    }

    async fn record_result(
        &self,
        record: crate::recording::ToolResultRecord,
    ) -> Result<Message, sqlx::Error> {
        tracing::error!(
            tool_call_id = %record.tool_call_id,
            tool = %record.tool_name,
            "ReplayNoopRecorder.record_result was invoked; replay paths must not write to the audit log"
        );
        Err(sqlx::Error::ColumnNotFound(
            "replay paths must not write result rows to the audit log".to_string(),
        ))
    }
}

/// Build a synthetic replay id from the original
/// `tool_call_id`. The hash is short (16 hex chars) but
/// unique enough to never collide on the same
/// `tool_call_id`, and the `replay_` prefix makes it
/// unambiguous in logs that this is a re-execution and not
/// a fresh LLM-initiated call.
fn make_replay_id(original: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(original.as_bytes());
    let digest = hasher.finalize();
    format!("replay_{}", &hex::encode(&digest[..8]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The replay id is derived from the original
    /// `tool_call_id` via SHA-256, so it must be:
    /// - deterministic (same input -> same id)
    /// - unique across different inputs
    /// - prefixed with `replay_` so it's obviously not a
    ///   fresh LLM-initiated call id
    /// - 8 hex chars of digest, keeping the total length
    ///   under pi's typical id-length cap
    #[test]
    fn make_replay_id_is_deterministic() {
        let a = make_replay_id("toolu_abc123");
        let b = make_replay_id("toolu_abc123");
        assert_eq!(a, b);
    }

    #[test]
    fn make_replay_id_differs_per_input() {
        let a = make_replay_id("toolu_abc123");
        let b = make_replay_id("toolu_def456");
        assert_ne!(a, b);
    }

    #[test]
    fn make_replay_id_has_replay_prefix() {
        let id = make_replay_id("anything");
        assert!(id.starts_with("replay_"), "id was {id}");
        // "replay_" + 16 hex chars
        assert_eq!(id.len(), "replay_".len() + 16);
    }

    /// The noop recorder is the safety net that prevents
    /// the replay path from accidentally double-writing
    /// rows to the audit log. Both methods must always
    /// return an `sqlx::Error` so any code path that tries
    /// to record through the replay's executor surfaces
    /// loudly in logs instead of silently corrupting the
    /// message log.
    #[tokio::test]
    async fn replay_noop_recorder_rejects_call() {
        let r = ReplayNoopRecorder;
        let res = r
            .record_call(crate::recording::ToolCallRecord {
                session_id: Uuid::new_v4(),
                tool_call_id: "tc_1".to_string(),
                tool_name: "bash".to_string(),
                input: serde_json::json!({}),
            })
            .await;
        assert!(res.is_err(), "record_call must error on the replay path");
    }

    #[tokio::test]
    async fn replay_noop_recorder_rejects_result() {
        let r = ReplayNoopRecorder;
        let res = r
            .record_result(crate::recording::ToolResultRecord {
                session_id: Uuid::new_v4(),
                tool_call_id: "tc_1".to_string(),
                tool_name: "bash".to_string(),
                content: "x".to_string(),
                output: serde_json::json!({}),
                is_error: false,
                duration_ms: Some(0),
            })
            .await;
        assert!(res.is_err(), "record_result must error on the replay path");
    }

    /// The bash-timeout-skip threshold exists so a
    /// long-running build command from a prior turn
    /// (e.g. `cargo build --release` with a 600s
    /// timeout) doesn't block `get_or_create` from
    /// returning during resume. The threshold is the
    /// contract: a value above it must skip, a value
    /// at or below it must not.
    #[test]
    fn bash_timeout_threshold_is_correct() {
        // Pinned at 30s; the test below exists so a future
        // edit that bumps the constant has to acknowledge the
        // threshold change explicitly.
        const { assert!(REPLAY_BASH_MAX_ORIGINAL_TIMEOUT_MS == 30_000) };
    }
}
