// Copyright (C) 2026 mule-ai
// SPDX-License-Identifier: AGPL-3.0-or-later

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

use std::path::Path;

use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::Message;

/// One entry in a pi session jsonl file. Mirrors the
/// `SessionEntryBase` shape: a tree node with `id`, `parentId`,
/// `timestamp`, plus a type discriminator.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum SessionEntry<'a> {
    #[serde(rename = "session")]
    Header {
        version: u32,
        id: Uuid,
        timestamp: String,
        cwd: &'a str,
    },
    #[serde(rename = "message")]
    Message {
        id: String,
        #[serde(rename = "parentId")]
        parent_id: Option<String>,
        timestamp: String,
        message: serde_json::Value,
    },
}

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
pub async fn write_session_jsonl(
    pool: &PgPool,
    session_id: Uuid,
    working_dir: &str,
    dest_path: &Path,
) -> Result<usize, sqlx::Error> {
    let messages: Vec<Message> = sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE session_id = $1 ORDER BY sequence ASC",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;

    // Build a profile lookup so we can fill in `provider` and
    // `model` on assistant messages. The profile id lives on
    // the session row.
    let provider: String = sqlx::query_scalar(
        "SELECT p.provider FROM profiles p JOIN sessions s ON s.profile_id = p.id WHERE s.id = $1",
    )
    .bind(session_id)
    .fetch_one(pool)
    .await?;
    let model: String = sqlx::query_scalar(
        "SELECT p.model FROM profiles p JOIN sessions s ON s.profile_id = p.id WHERE s.id = $1",
    )
    .bind(session_id)
    .fetch_one(pool)
    .await?;

    let mut buf = String::new();

    // Header. The `id` here is the new pi session id we'll get
    // back from `new_session`; we use the forge session id as
    // a stable identifier for now.
    let header = json!({
        "type": "session",
        "version": 3,
        "id": session_id,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "cwd": working_dir,
    });
    buf.push_str(&header.to_string());
    buf.push('\n');

    // Walk the messages in order, threading parent_id through
    // the previous entry's id. The 8-char hex id is what pi
    // uses internally; we can synthesize a deterministic one
    // from the sequence number so the tree is easy to debug
    // (seq 1 -> "00000001", seq 2 -> "00000002", etc.).
    let mut prev_id: Option<String> = None;
    let mut written = 0usize;
    for msg in &messages {
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
                result.as_object_mut().unwrap().insert("details".to_string(), out.clone());
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
}
