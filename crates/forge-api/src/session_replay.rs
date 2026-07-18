//! Build a pi session jsonl file from the messages table so a
//! fresh `pi` subprocess can resume a conversation that was last
//! touched by a now-dead pi process.
//!
//! Forge's durability story is: the pi subprocess is disposable.
//! The `messages` table is the canonical record of what the LLM
//! saw and produced. When the user comes back to a session that
//! hasn't been touched in a while, the in-memory agent is gone,
//! the sandbox has been destroyed, and the only thing left is
//! the audit log. We rebuild the LLM's context by replaying the
//! prior turns as a pi session jsonl file and asking the fresh
//! pi to load it via the `new_session` RPC command with
//! `parentSession` pointing at the file.
//!
//! See `docs/operations/durability.md` for the high-level
//! picture and `node_modules/@earendil-works/pi-coding-agent/
//! docs/session-format.md` for the jsonl shape we emit.

use std::path::{Path, PathBuf};

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::Message;

/// Build a pi session jsonl file under `dest_path` from the
/// `messages` rows for `session_id`. The file is suitable for
/// loading via the `new_session` RPC command with
/// `parentSession` pointing at it.
///
/// Returns the number of `message` entries written (excluding
/// the header row). The caller is responsible for putting a
/// header row at the top of the file; this function emits the
/// messages in the same order pi expects: chronological by
/// `sequence`.
///
/// ## Mapping forge roles to pi message shapes
///
/// - `role='user'`, `tool_call_id IS NULL` -> a `UserMessage`
///   carrying the user prompt as plain text.
///
/// - `role='assistant'`, `tool_call_id IS NULL` -> an
///   `AssistantMessage` whose content is a single `text` block
///   from `content`. (We don't currently persist thinking
///   blocks; the replay won't have them, but the model can
///   re-think as needed.)
///
/// - `role='assistant'`, `tool_call_id IS NOT NULL` -> an
///   `AssistantMessage` whose content is a single `toolCall`
///   block. The `arguments` come from the `tool_input` jsonb
///   column.
///
/// - `role='tool'` -> a `ToolResultMessage` with the
///   `tool_call_id` matching the assistant's call row. The
///   `content` is a single `text` block from the recorded
///   `content` (the human-readable flattened form), with
///   `isError` derived from the `success` field of
///   `tool_output`.
///
/// The exact set of fields pi expects on each message is
/// documented in the session-format reference; for
/// `AssistantMessage` we synthesize `api`, `provider`, `model`,
/// `usage`, and `stopReason` from the session's profile +
/// sensible stubs. These aren't user-visible in any meaningful
/// way -- they're just what the jsonl schema requires.
/// Where the durable-resume jsonl for a session lives: inside the
/// session's working directory. Both `agent_registry::get_or_create`
/// (which writes it before spawning pi) and `admin_session_replay`
/// (which rewrites it on operator request) call this so the path is
/// derived from the session's actual `working_dir` rather than a
/// hard-coded `/forge/sessions/<id>/.parent.jsonl` literal. The
/// hard-coded form didn't exist in CI (no `/forge/` tree) and diverged
/// from the working_dir when a profile had a `git_url` (the working_dir
/// is the sandbox clone, not `/forge/sessions/<id>`); deriving it keeps
/// the jsonl next to the rest of the session's files.
pub(crate) fn parent_jsonl_path(working_dir: &str) -> PathBuf {
    PathBuf::from(working_dir).join(".parent.jsonl")
}

pub async fn write_session_jsonl(
    pool: &PgPool,
    session_id: Uuid,
    working_dir: &str,
    dest_path: &Path,
) -> Result<usize, sqlx::Error> {
    write_session_jsonl_with_max_seq(pool, session_id, working_dir, dest_path, None).await
}

/// Same as [`write_session_jsonl`] but caps the messages
/// written to those with `sequence <= max_sequence`. Used
/// by the durable-resume path: the harness inserts the
/// user's just-arrived prompt into the `messages` table
/// BEFORE calling into the registry, so by the time the
/// jsonl is written the latest row is the new user
/// prompt. We exclude it from the jsonl (caller passes
/// `Some(user_message_sequence - 1)`) and send the user
/// prompt through pi's normal stdin `prompt` flow. The
/// jsonl is the "prior conversation" — the messages that
/// existed before the user's just-arrived prompt — not
/// the just-arrived prompt itself. Without this cap, the
/// model would see the user prompt twice: once from the
/// loaded jsonl, once from the stdin prompt.
pub async fn write_session_jsonl_with_max_seq(
    pool: &PgPool,
    session_id: Uuid,
    working_dir: &str,
    dest_path: &Path,
    max_sequence: Option<i32>,
) -> Result<usize, sqlx::Error> {
    let messages: Vec<Message> = match max_sequence {
        Some(max) => sqlx::query_as::<_, Message>(
            "SELECT * FROM messages WHERE session_id = $1 AND sequence <= $2 ORDER BY sequence ASC",
        )
        .bind(session_id)
        .bind(max)
        .fetch_all(pool)
        .await?,
        None => {
            sqlx::query_as::<_, Message>(
                "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
            )
            .bind(session_id)
            .fetch_all(pool)
            .await?
        }
    };

    // Look up the profile's provider + model in one round-trip so we
    // can fill them in on the synthesized assistant messages. The
    // profile id lives on the session row.
    let (provider, model): (String, String) =
        sqlx::query_as("SELECT p.provider, p.model FROM profiles p JOIN sessions s ON s.profile_id = p.id WHERE s.id = $1")
            .bind(session_id)
            .fetch_one(pool)
            .await?;

    // First pass: compute the LAST (highest `sequence`) `tool`
    // row for each `tool_call_id`. We only want to emit the
    // final result for a given call. If the messages table
    // somehow ends up with multiple `tool` rows for the same
    // call id (e.g. a forge bug that double-writes a result
    // row, or a future fix that introduces one), the earlier
    // rows are dropped on the floor here. The messages table
    // is still the source of truth — we never rewrite it —
    // we just don't ship the duplicates to the new pi.
    let last_result_seq = compute_last_result_seq(&messages);

    // Second pass: emit the jsonl in the right order. This is
    // where the tool-call/tool-result reordering happens (see
    // the doc on `order_messages_for_jsonl` below).
    let ordered = order_messages_for_jsonl(&messages, &last_result_seq);

    // Serialize to jsonl.
    let mut buf = String::new();
    let header = json!({
        "type": "session",
        "version": 3,
        "id": session_id,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "cwd": working_dir,
    });
    buf.push_str(&header.to_string());
    buf.push('\n');
    let mut prev_id: Option<String> = None;
    let mut written = 0usize;
    for msg in &ordered {
        let entry_id = format!("{:08x}", msg.sequence);
        let timestamp = msg.created_at.to_rfc3339();
        let pi_message = forge_to_pi_message(msg, &provider, &model);
        let entry = json!({
            "type": "message",
            "id": entry_id,
            "parentId": prev_id,
            "timestamp": timestamp,
            "message": pi_message,
        });
        buf.push_str(&entry.to_string());
        buf.push('\n');
        prev_id = Some(entry_id);
        written += 1;
    }

    if let Some(parent) = dest_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    tokio::fs::write(dest_path, buf.as_bytes()).await?;

    Ok(written)
}

/// For each `tool_call_id`, return the highest `sequence` of
/// any `tool` row in `messages`. Used to dedup: if the
/// messages table has multiple `tool` rows for the same call
/// id, we only ship the last one to the new pi.
fn compute_last_result_seq(messages: &[Message]) -> std::collections::HashMap<String, i32> {
    let mut last: std::collections::HashMap<String, i32> = std::collections::HashMap::new();
    for msg in messages {
        if msg.role == "tool" {
            if let Some(tcid) = &msg.tool_call_id {
                last.insert(tcid.clone(), msg.sequence);
            }
        }
    }
    last
}

/// Walk the messages in the order they should appear in the
/// pi session jsonl. This reorders tool calls and their
/// results so that each call is immediately followed by its
/// result, and deduplicates tool results: if multiple `tool`
/// rows exist for the same `tool_call_id`, only the last
/// (highest sequence) is kept.
///
/// ## Why reorder
///
/// pi's `transformMessages` (in
/// `node_modules/@earendil-works/pi-ai/dist/providers/
/// transform-messages.js`, the `insertSyntheticToolResults`
/// pass) injects a synthetic
/// `{role: "toolResult", content: "No result provided",
/// isError: true}` placeholder for any tool call whose
/// result hasn't been seen yet when the NEXT assistant
/// message is processed. The synthetic is harmless on its
/// own, but if the real result for the call ALSO appears
/// later in the walk (e.g. parallel tool calls where the
/// results come back out of order: call A, call B, result B,
/// result A), the wire payload ends up with two
/// `tool_result` blocks for the same `tool_use_id`.
/// Anthropic rejects this with a 400, and
/// Bifrost/getbifrost.ai translates that into the cryptic
/// "999 (1000)" 500 we were seeing on the stuck session
/// `814981fc-…`. By emitting each call and its result as
/// adjacent entries, we ensure pi's transformMessages never
/// sees an orphan call and never injects the synthetic
/// placeholder, so the wire payload has exactly one
/// `tool_result` per `tool_use_id`.
///
/// ## Implementation note: two-pass
///
/// A naive "defer calls until their result arrives" approach
/// doesn't work. If result B arrives before result A, the
/// naive approach emits `[call B, result B, call A, result A]`
/// — and that's *still* a 999 trigger, because pi's
/// `transformMessages` resets `existingToolResultIds` to
/// `new Set()` every time it sees a new assistant message
/// with tool calls. So when it processes `call A` (which
/// arrives after `result B`), it forgets that `result B` was
/// seen, and `call B` becomes orphaned in the synthetic pass.
///
/// The fix is a two-pass walk:
///
/// 1. First pass: collect every `tool_call_id` → last result
///    mapping, and drop orphan results / duplicates here.
/// 2. Second pass: walk in sequence order. For each tool
///    call, emit the call followed by its result (if any).
///    For each tool result, skip it (it's already been
///    emitted with its call). For everything else, emit in
///    sequence order.
///
/// This guarantees the jsonl walk order is `[..., call A,
/// result A, call B, result B, ...]` regardless of when the
/// results arrived in the messages table. pi's
/// `transformMessages` sees each call immediately followed by
/// its result, so the synthetic injection never fires.
///
/// ## Drops
///
/// - Orphan tool results: a `tool` row whose `toolCallId`
///   has no matching `assistant` toolCall. pi rejects the
///   whole conversation with "invalid params, tool result's
///   tool id ... not found" — we'd rather drop one result
///   and let the model re-run the call than fail the resume.
///   Observed on session 1faa1686-… where a prior turn was
///   interrupted between the call and the result.
/// - Forge-side placeholder rows: assistant rows whose
///   `content` is the harness's "no response from agent"
///   marker. They're not real LLM output; replaying them
///   trains the new model to imitate the marker text.
/// - Duplicate tool results: a `tool` row for a `toolCallId`
///   that has a later `tool` row (i.e. it's not the last
///   one). Defensive: the messages table doesn't currently
///   have duplicates, but the safety net is cheap.
fn order_messages_for_jsonl<'a>(
    messages: &'a [Message],
    last_result_seq: &std::collections::HashMap<String, i32>,
) -> Vec<&'a Message> {
    // Set of `tool_call_id`s that have a matching assistant
    // toolCall row. Used to detect orphan tool results.
    let mut known_call_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Map from `tool_call_id` to the LAST (highest sequence)
    // tool result. We emit each call with this result, so we
    // need O(1) lookup at emission time.
    let mut result_by_call_id: std::collections::HashMap<String, &'a Message> =
        std::collections::HashMap::new();
    for msg in messages {
        if msg.role == "assistant" {
            if let Some(tcid) = &msg.tool_call_id {
                known_call_ids.insert(tcid.clone());
            }
        } else if msg.role == "tool" {
            if let Some(tcid) = &msg.tool_call_id {
                // Keep the last result for each call_id. This
                // also handles dedup: if there are multiple
                // `tool` rows for the same call_id, only the
                // last one is kept here.
                result_by_call_id.insert(tcid.clone(), msg);
            }
        }
    }

    // Track which tool results we've already emitted (via
    // their call row), so we can skip the standalone `tool`
    // row in the second pass without double-emitting.
    let mut emitted_result_seqs: std::collections::HashSet<i32> = std::collections::HashSet::new();

    let mut out: Vec<&'a Message> = Vec::with_capacity(messages.len());

    for msg in messages {
        match (msg.role.as_str(), &msg.tool_call_id) {
            // Tool call: emit the call, then its result (if
            // any) immediately after. This is the reordering.
            ("assistant", Some(tcid)) => {
                // Edge case: if the call has a toolCallId but
                // no known result, emit the call anyway — pi
                // will synthesize a "No result provided"
                // placeholder, which is the correct behavior
                // for an interrupted tool.
                out.push(msg);
                if let Some(result) = result_by_call_id.get(tcid) {
                    emitted_result_seqs.insert(result.sequence);
                    out.push(result);
                } else {
                    tracing::warn!(
                        sequence = msg.sequence,
                        tool_call_id = %tcid,
                        "durable resume: emitting tool call with no matching result row; pi's transformMessages will inject a synthetic 'No result provided' placeholder, which is the desired behavior for an interrupted call"
                    );
                }
            }

            // Tool result: skip if it was already emitted
            // with its call (the common case). Drop if it's
            // an orphan (no matching call) or a duplicate
            // (not the last result for this call_id).
            ("tool", Some(tcid)) => {
                if !known_call_ids.contains(tcid) {
                    tracing::warn!(
                        tool_call_id = %tcid,
                        sequence = msg.sequence,
                        "durable resume: dropping orphaned tool result whose call id has no matching assistant toolCall; the model can re-derive the result on the next turn if needed"
                    );
                    continue;
                }
                if last_result_seq.get(tcid) != Some(&msg.sequence) {
                    tracing::warn!(
                        tool_call_id = %tcid,
                        sequence = msg.sequence,
                        "durable resume: dropping duplicate tool result; keeping only the last one for this call id (the duplicate would have caused pi's transformMessages to insert a synthetic placeholder, producing two tool_result blocks for the same tool_use_id, which Anthropic/Bifrost rejects)"
                    );
                    continue;
                }
                // Common case: this result was already emitted
                // with its call. Skip the standalone row.
                if emitted_result_seqs.contains(&msg.sequence) {
                    continue;
                }
                // Unreachable in practice: if we got here, the
                // result has a matching call (orphan check
                // above) and is the last result (dedup check
                // above), but wasn't emitted with its call.
                // That can only happen if the call row was
                // dropped for some reason (e.g. a placeholder
                // row that we filtered out). Emit the result
                // on its own — pi will accept it as a
                // free-standing tool result.
                out.push(msg);
            }

            // Assistant text (no tool call): drop forge-side
            // placeholder rows, emit everything else in
            // sequence order.
            ("assistant", None) => {
                if let Some(content) = &msg.content {
                    if content == "No response from agent (timed out?)"
                        || content == "[no response from agent]"
                    {
                        tracing::info!(
                            sequence = msg.sequence,
                            content = %content,
                            "durable resume: skipping forge-side placeholder row; the prior turn had no real model output, replaying this would just train the new model to imitate the placeholder"
                        );
                        continue;
                    }
                }
                out.push(msg);
            }

            // User, system, anything else: emit in sequence
            // order.
            _ => {
                out.push(msg);
            }
        }
    }

    out
}

/// Convert one forge `messages` row to a pi AgentMessage object.
fn forge_to_pi_message(msg: &Message, provider: &str, model: &str) -> serde_json::Value {
    match (msg.role.as_str(), &msg.tool_call_id) {
        // Plain user prompt.
        ("user", None) => json!({
            "role": "user",
            "content": msg.content.clone().unwrap_or_default(),
            "timestamp": msg.created_at.timestamp_millis(),
        }),

        // Assistant text response (no tool call).
        ("assistant", None) => {
            let text = msg.content.clone().unwrap_or_default();
            json!({
                "role": "assistant",
                "content": [
                    { "type": "text", "text": text }
                ],
                "api": "anthropic-messages",
                "provider": provider,
                "model": model,
                "usage": empty_usage(),
                "stopReason": "stop",
                "timestamp": msg.created_at.timestamp_millis(),
            })
        }

        // Assistant tool call. The `tool_input` jsonb is the
        // arguments object pi expects to see on a toolCall
        // content block.
        ("assistant", Some(_tool_call_id)) => {
            let tool_name = msg.tool_name.clone().unwrap_or_default();
            let arguments = msg.tool_input.clone().unwrap_or(serde_json::Value::Null);
            json!({
                "role": "assistant",
                "content": [
                    {
                        "type": "toolCall",
                        "id": _tool_call_id,
                        "name": tool_name,
                        "arguments": arguments,
                    }
                ],
                "api": "anthropic-messages",
                "provider": provider,
                "model": model,
                "usage": empty_usage(),
                "stopReason": "toolUse",
                "timestamp": msg.created_at.timestamp_millis(),
            })
        }

        // Tool result for a prior call.
        ("tool", tool_call_id) => {
            // The recorded `content` is the human-readable
            // flattened form (e.g. "[bash exit=...]\n--stdout--
            // \n..."). pi's tool result wants a content block;
            // we surface it as a single text block. For bash,
            // the model already has access to this string from
            // the prior turn -- replaying it as a text block
            // gives the new pi the same view of the world.
            let text = msg.content.clone().unwrap_or_default();
            let is_error = extract_is_error(msg);
            let mut result = json!({
                "role": "toolResult",
                "toolCallId": tool_call_id.clone().unwrap_or_default(),
                "toolName": msg.tool_name.clone().unwrap_or_default(),
                "content": [
                    { "type": "text", "text": text }
                ],
                "isError": is_error,
                "timestamp": msg.created_at.timestamp_millis(),
            });
            // Surface the structured tool output under
            // `details` so extensions that care (e.g. for
            // re-rendering) can find it. The default
            // forge-tools extension ignores it.
            if let Some(out) = &msg.tool_output {
                result
                    .as_object_mut()
                    .unwrap()
                    .insert("details".to_string(), out.clone());
            }
            result
        }

        // System messages are session-level metadata, not part
        // of the LLM's conversation. Skip them on replay.
        ("system", _) => json!({
            "role": "user",
            "content": "",
            "timestamp": msg.created_at.timestamp_millis(),
        }),

        // Anything else: emit as a noop user message so the
        // turn is preserved in the tree even if the model can't
        // do anything useful with it. This shouldn't happen in
        // practice but we don't want a bad row to break resume.
        (role, _) => json!({
            "role": "user",
            "content": format!("[forge replay: unhandled role={} content={:?}]", role, msg.content),
            "timestamp": msg.created_at.timestamp_millis(),
        }),
    }
}

/// `is_error` for a tool row. The recorder stores a `success`
/// boolean in the `tool_output` jsonb; absent that, we fall
/// back to `is_error` if the row carried that field directly
/// (older rows) and finally to "not error" as a safe default.
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
    json!({
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn msg(
        role: &str,
        content: Option<&str>,
        tool_call_id: Option<&str>,
        tool_name: Option<&str>,
        tool_input: Option<serde_json::Value>,
        tool_output: Option<serde_json::Value>,
    ) -> Message {
        Message {
            id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            sequence: 1,
            role: role.to_string(),
            content: content.map(|s| s.to_string()),
            tool_name: tool_name.map(|s| s.to_string()),
            tool_input,
            tool_call_id: tool_call_id.map(|s| s.to_string()),
            tool_output,
            duration_ms: None,
            created_at: Utc::now(),
        }
    }

    fn msg_with_seq(
        seq: i32,
        role: &str,
        content: Option<&str>,
        tool_call_id: Option<&str>,
        tool_name: Option<&str>,
        tool_input: Option<serde_json::Value>,
        tool_output: Option<serde_json::Value>,
    ) -> Message {
        let mut m = msg(
            role,
            content,
            tool_call_id,
            tool_name,
            tool_input,
            tool_output,
        );
        m.sequence = seq;
        m
    }

    #[test]
    fn user_message_maps_to_user_pi_message() {
        let m = msg("user", Some("hi"), None, None, None, None);
        let j = forge_to_pi_message(&m, "anthropic", "claude");
        assert_eq!(j["role"], "user");
        assert_eq!(j["content"], "hi");
    }

    #[test]
    fn assistant_text_maps_to_text_block() {
        let m = msg("assistant", Some("hello"), None, None, None, None);
        let j = forge_to_pi_message(&m, "anthropic", "claude");
        assert_eq!(j["role"], "assistant");
        assert_eq!(j["content"][0]["type"], "text");
        assert_eq!(j["content"][0]["text"], "hello");
        assert_eq!(j["stopReason"], "stop");
    }

    #[test]
    fn assistant_tool_call_maps_to_tool_call_block() {
        let m = msg(
            "assistant",
            Some("[tool_call:bash]"),
            Some("call_1"),
            Some("bash"),
            Some(serde_json::json!({"command": "ls"})),
            None,
        );
        let j = forge_to_pi_message(&m, "anthropic", "claude");
        assert_eq!(j["role"], "assistant");
        assert_eq!(j["content"][0]["type"], "toolCall");
        assert_eq!(j["content"][0]["id"], "call_1");
        assert_eq!(j["content"][0]["name"], "bash");
        assert_eq!(j["content"][0]["arguments"]["command"], "ls");
        assert_eq!(j["stopReason"], "toolUse");
    }

    #[test]
    fn tool_result_is_error_comes_from_success_field() {
        let m = msg(
            "tool",
            Some("oops"),
            Some("call_1"),
            Some("bash"),
            None,
            Some(serde_json::json!({"success": false, "stdout": "", "stderr": "oops"})),
        );
        let j = forge_to_pi_message(&m, "anthropic", "claude");
        assert_eq!(j["role"], "toolResult");
        assert_eq!(j["toolCallId"], "call_1");
        assert_eq!(j["toolName"], "bash");
        assert_eq!(j["isError"], true);
    }

    #[test]
    fn tool_result_success_field_round_trip() {
        let m = msg(
            "tool",
            Some("ok"),
            Some("call_2"),
            Some("read"),
            None,
            Some(serde_json::json!({"success": true, "output": "ok"})),
        );
        let j = forge_to_pi_message(&m, "anthropic", "claude");
        assert_eq!(j["isError"], false);
    }

    #[test]
    fn tool_result_defaults_to_not_error_when_field_missing() {
        let m = msg("tool", Some("?"), Some("call_3"), Some("bash"), None, None);
        let j = forge_to_pi_message(&m, "anthropic", "claude");
        assert_eq!(j["isError"], false);
    }

    /// The bug that triggered the 999 in session
    /// `814981fc-…`: two parallel tool calls where the
    /// results came back out of order (call A, call B,
    /// result B, result A). pi's `transformMessages`
    /// sees an orphan call A between call A and call B
    /// and injects a synthetic `tool_result` for it. The
    /// real result A then appears later, producing two
    /// `tool_result` blocks for the same `tool_use_id` in
    /// the wire payload, which Anthropic/Bifrost rejects.
    ///
    /// Our fix: reorder the jsonl so each call is
    /// immediately followed by its result. Then pi's
    /// transformMessages never sees an orphan call and
    /// never injects the synthetic.
    #[test]
    fn out_of_order_parallel_tool_results_are_reordered() {
        // The exact pattern from session 814981fc-…,
        // messages table seq 5..9 (one user prompt, two
        // parallel bash calls, then results in B-then-A
        // order).
        let messages = vec![
            msg_with_seq(5, "user", Some("run two things"), None, None, None, None),
            msg_with_seq(
                6,
                "assistant",
                Some("[tool_call:bash]"),
                Some("call_A"),
                Some("bash"),
                Some(serde_json::json!({"command": "A"})),
                None,
            ),
            msg_with_seq(
                7,
                "assistant",
                Some("[tool_call:bash]"),
                Some("call_B"),
                Some("bash"),
                Some(serde_json::json!({"command": "B"})),
                None,
            ),
            msg_with_seq(
                8,
                "tool",
                Some("B's stdout"),
                Some("call_B"),
                Some("bash"),
                None,
                Some(serde_json::json!({"success": true})),
            ),
            msg_with_seq(
                9,
                "tool",
                Some("A's stdout"),
                Some("call_A"),
                Some("bash"),
                None,
                Some(serde_json::json!({"success": true})),
            ),
        ];

        let last_result_seq = compute_last_result_seq(&messages);
        let ordered = order_messages_for_jsonl(&messages, &last_result_seq);

        // Expected order: user, call_A, result_A, call_B, result_B.
        let seqs: Vec<i32> = ordered.iter().map(|m| m.sequence).collect();
        assert_eq!(
            seqs,
            vec![5, 6, 9, 7, 8],
            "each call must be immediately followed by its result, even when the results came back out of order"
        );

        // And the result for call_A must be the real one
        // (seq 9), not a synthetic placeholder.
        let result_a = ordered
            .iter()
            .find(|m| m.role == "tool" && m.tool_call_id.as_deref() == Some("call_A"))
            .unwrap();
        assert_eq!(result_a.sequence, 9);
        assert_eq!(result_a.content.as_deref(), Some("A's stdout"));
    }

    /// Defensive dedup: if the messages table somehow has
    /// multiple `tool` rows for the same `tool_call_id`,
    /// only the last one (highest sequence) is shipped to
    /// pi. The earlier rows would otherwise duplicate the
    /// real result in the wire payload and trigger the
    /// same 999 bug.
    #[test]
    fn duplicate_tool_results_are_deduped_to_last() {
        let messages = vec![
            msg_with_seq(1, "user", Some("go"), None, None, None, None),
            msg_with_seq(
                2,
                "assistant",
                Some("[tool_call:bash]"),
                Some("call_1"),
                Some("bash"),
                Some(serde_json::json!({"command": "ls"})),
                None,
            ),
            // Two results for the same call id. The
            // messages table shouldn't have these (forge
            // writes exactly one per call), but we want
            // the safety net.
            msg_with_seq(
                3,
                "tool",
                Some("first (stale) result"),
                Some("call_1"),
                Some("bash"),
                None,
                Some(serde_json::json!({"success": true})),
            ),
            msg_with_seq(
                4,
                "tool",
                Some("last (authoritative) result"),
                Some("call_1"),
                Some("bash"),
                None,
                Some(serde_json::json!({"success": true})),
            ),
        ];

        let last_result_seq = compute_last_result_seq(&messages);
        let ordered = order_messages_for_jsonl(&messages, &last_result_seq);

        // Expected: user, call_1, last_result. The
        // "first (stale) result" is dropped.
        let seqs: Vec<i32> = ordered.iter().map(|m| m.sequence).collect();
        assert_eq!(seqs, vec![1, 2, 4]);
        let result = ordered
            .iter()
            .find(|m| m.role == "tool" && m.tool_call_id.as_deref() == Some("call_1"))
            .unwrap();
        assert_eq!(result.sequence, 4);
        assert_eq!(
            result.content.as_deref(),
            Some("last (authoritative) result")
        );
    }

    /// Orphan tool results (no matching assistant
    /// toolCall) are dropped. The model can re-derive the
    /// missing result by re-running the call on its next
    /// turn. Without this, pi rejects the whole
    /// conversation.
    #[test]
    fn orphan_tool_results_are_dropped() {
        let messages = vec![
            msg_with_seq(1, "user", Some("go"), None, None, None, None),
            // Tool result with no matching call.
            msg_with_seq(
                2,
                "tool",
                Some("orphan result"),
                Some("call_orphan"),
                Some("bash"),
                None,
                Some(serde_json::json!({"success": true})),
            ),
        ];
        let last_result_seq = compute_last_result_seq(&messages);
        let ordered = order_messages_for_jsonl(&messages, &last_result_seq);
        let seqs: Vec<i32> = ordered.iter().map(|m| m.sequence).collect();
        assert_eq!(seqs, vec![1], "orphan tool result is dropped");
    }

    /// Forge-side placeholder rows are dropped. They're
    /// not real LLM output; replaying them trains the new
    /// model to imitate the marker text.
    #[test]
    fn forge_placeholder_rows_are_dropped() {
        let messages = vec![
            msg_with_seq(1, "user", Some("go"), None, None, None, None),
            msg_with_seq(
                2,
                "assistant",
                Some("No response from agent (timed out?)"),
                None,
                None,
                None,
                None,
            ),
            msg_with_seq(3, "assistant", Some("real answer"), None, None, None, None),
        ];
        let last_result_seq = compute_last_result_seq(&messages);
        let ordered = order_messages_for_jsonl(&messages, &last_result_seq);
        let seqs: Vec<i32> = ordered.iter().map(|m| m.sequence).collect();
        assert_eq!(
            seqs,
            vec![1, 3],
            "forge-side placeholder row is dropped; real answer is kept"
        );
    }

    /// A tool call with no matching result row (e.g. the
    /// tool was interrupted) is still emitted — pi's
    /// transformMessages will inject a synthetic
    /// "No result provided" placeholder, which is the
    /// correct behavior.
    #[test]
    fn unmatched_tool_calls_are_emitted() {
        let messages = vec![
            msg_with_seq(1, "user", Some("go"), None, None, None, None),
            msg_with_seq(
                2,
                "assistant",
                Some("[tool_call:bash]"),
                Some("call_unmatched"),
                Some("bash"),
                Some(serde_json::json!({"command": "ls"})),
                None,
            ),
        ];
        let last_result_seq = compute_last_result_seq(&messages);
        let ordered = order_messages_for_jsonl(&messages, &last_result_seq);
        let seqs: Vec<i32> = ordered.iter().map(|m| m.sequence).collect();
        assert_eq!(seqs, vec![1, 2], "unmatched tool call is still emitted");
    }
}
