//! Herd H4.2: episode capture at turn end.
//!
//! Trigger: [`crate::harness::project_turn_end`] — where the committed
//! `pi.assistant` entry lands as the session's assistant row — spawns
//! [`capture_turn`] fire-and-forget, next to
//! `api::routing::refresh_session_summary`. The turn never blocks or
//! fails: every error inside [`capture_turn_inner`] is a
//! `tracing::warn!` + skip.
//!
//! No-op (documented, per PLAN H4.2) when the session has no agent
//! (`sessions.agent_id IS NULL`) — episodes are agent memory.
//!
//! What is captured:
//!
//! 1. **Slice**: the turn's durable entries — from the most recent
//!    `pi.user` entry at or before the turn-end entry (the submitted
//!    prompt) to the turn-end `pi.assistant` entry. Provenance =
//!    `source: {conversation_id: <session id>, seq_range: [first,
//!    last]}` where the seqs are `durable_entries.id` (monotonic per
//!    conversation — the same id `turn_end` events carry).
//! 2. **Deterministic pass** ([`extract_turn_facts`], pure,
//!    unit-tested): commands run (bash toolCall `command` args,
//!    truncated to 200 chars each, capped at 10), files touched
//!    (write/edit toolCall paths, deduped, capped at 10), explicit
//!    user feedback (prompt sentences matching the negation/override
//!    patterns, capped at 5), and the implicit "user re-sent/edited
//!    the prompt after the assistant answered" flag.
//! 3. **Summary**: cheap-model second pass via the `message-router`
//!    profile (the same in-process LLM path
//!    `api::routing::refresh_session_summary` uses — no subprocess);
//!    when the profile is absent or the endpoint fails, the
//!    deterministic one-liner
//!    ("Ran N commands, touched M files, feedback: …").
//! 4. **Redaction** ([`crate::memory::redact`]) on everything that
//!    is captured (summary, feedback, commands, files).
//! 5. **Embedding** of summary + explicit feedback via
//!    [`crate::embedding::embed`]; on endpoint failure the row is
//!    still inserted with a NULL embedding (H4.1 contract: ranked
//!    by the B-tree time index only, invisible to cosine retrieval).
//! 6. **Exactly-once**: before inserting, `episodes` is checked for
//!    an existing row of the same agent + session whose
//!    `source.seq_range` overlaps this slice — a redelivered
//!    `TurnEnd` (or a `ResyncRequired` rescan) is a no-op. The check
//!    is deliberately simple (no constraint on the JSONB column):
//!    capture is fire-and-forget and a concurrent race between two
//!    captures of the *same* entry can only produce a duplicate
//!    window if both pass the check in the same instant, which the
//!    upstream `durable_projection` claim in `project_turn_end`
//!    already serializes.
//! 7. **Watch scan** (H4.5): right after the successful insert, the
//!    agent's ACTIVE beliefs with a non-null `watch` are scanned by
//!    `memory::scan_watch_triggers` — a `watch.match` substring hit on
//!    the episode summary / explicit feedback past the
//!    `watch.cooldown_hours` cooldown queues a `memory_trigger_queue`
//!    row + stamps `beliefs.last_triggered_at` in one txn. The mule
//!    forwarder lane polls that queue. A scan error is warn+skip:
//!    it never fails the turn.

use std::path::PathBuf;

use regex::Regex;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::api::routing::{
    make_llm_call, read_provider_config, ProviderConfig, ROUTER_PROFILE_NAME,
};
use crate::db::Profile;
use crate::embedding;
use crate::memory;

// ============================================
// Constants
// ============================================

/// Cap on captured commands per episode (each truncated to
/// [`MAX_TEXT`] chars).
pub const MAX_COMMANDS: usize = 10;
/// Cap on captured files per episode.
pub const MAX_FILES: usize = 10;
/// Cap on captured explicit-feedback sentences per episode.
pub const MAX_FEEDBACK: usize = 5;
/// Truncation length for any single captured text (chars).
pub const MAX_TEXT: usize = 200;

/// ToolCall names counted as "commands run".
const COMMAND_TOOLS: &[&str] = &["bash", "shell"];
/// ToolCall names counted as "files touched" (the argument carries
/// the path under `path` or `file_path`).
const FILE_TOOLS: &[&str] = &[
    "write",
    "edit",
    "apply_patch",
    "create_file",
    "write_file",
    "multi_edit",
];

fn truncate_chars(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// Negation / override patterns marking explicit user feedback in a
/// prompt sentence (PLAN H4.2: "no/wrong/actually I wanted…").
fn feedback_matcher() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\b(no|wrong|actually|instead)\b|i wanted").expect("static regex")
    })
}

// ============================================
// Pure deterministic extraction
// ============================================

/// The structured fields extracted from one turn's entry slice.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnFacts {
    /// Bash/shell commands run during the turn (≤ [`MAX_COMMANDS`],
    /// each ≤ [`MAX_TEXT`] chars, deduped, turn order).
    pub commands: Vec<String>,
    /// Files written/edited during the turn (≤ [`MAX_FILES`],
    /// deduped, turn order).
    pub files: Vec<String>,
    /// Sentences of the user prompt carrying explicit feedback
    /// (≤ [`MAX_FEEDBACK`], each ≤ [`MAX_TEXT`] chars).
    pub feedback_explicit: Vec<String>,
    /// Implicit feedback: the user re-sent/edited a previous prompt
    /// after the assistant had answered (see [`looks_like_resend`]).
    pub implicit_feedback: bool,
}

/// Extract the deterministic episode fields from a turn slice.
///
/// `user_prompt` is the submitted prompt (the turn's `pi.user`
/// entry); `prev_user_prompt` is the previous user prompt in the
/// same conversation (for the implicit-feedback flag); `records` are
/// the parsed `pi.assistant` entry records from AFTER the user entry
/// through the turn-end entry (their `toolCall` content blocks are
/// the source of commands/files).
pub fn extract_turn_facts(
    user_prompt: &str,
    prev_user_prompt: Option<&str>,
    records: &[Value],
) -> TurnFacts {
    let mut facts = TurnFacts {
        feedback_explicit: explicit_feedback(user_prompt),
        implicit_feedback: matches!(
            prev_user_prompt,
            Some(prev) if looks_like_resend(prev, user_prompt)
        ),
        ..Default::default()
    };
    for record in records {
        let Some(messages) = record.get("model").and_then(|m| m.as_array()) else {
            continue;
        };
        for message in messages {
            if message.get("role").and_then(|r| r.as_str()) != Some("assistant") {
                continue;
            }
            let Some(blocks) = message.get("content").and_then(|c| c.as_array()) else {
                continue;
            };
            for block in blocks {
                if block.get("type").and_then(|t| t.as_str()) != Some("toolCall") {
                    continue;
                }
                let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
                // pi-durable entry drafts carry the arguments under
                // `arguments` (migration shape); accept `input` too.
                let input = block.get("arguments").or_else(|| block.get("input"));
                if COMMAND_TOOLS.contains(&name) {
                    if let Some(cmd) = input
                        .and_then(|i| i.get("command"))
                        .and_then(|c| c.as_str())
                    {
                        let cmd = truncate_chars(cmd, MAX_TEXT);
                        if !cmd.is_empty() && !facts.commands.contains(&cmd) {
                            facts.commands.push(cmd);
                        }
                        if facts.commands.len() >= MAX_COMMANDS {
                            return early_merge(facts);
                        }
                    }
                }
                if FILE_TOOLS.contains(&name) {
                    let path = input
                        .and_then(|i| i.get("path").or_else(|| i.get("file_path")))
                        .and_then(|p| p.as_str());
                    if let Some(path) = path {
                        let path = truncate_chars(path, MAX_TEXT);
                        if !path.is_empty() && !facts.files.contains(&path) {
                            facts.files.push(path);
                        }
                        if facts.files.len() >= MAX_FILES {
                            return early_merge(facts);
                        }
                    }
                }
            }
        }
    }
    early_merge(facts)
}

/// No behavior — exists to give the early-exit paths a single merge
/// point that keeps any future post-processing in one place.
fn early_merge(mut facts: TurnFacts) -> TurnFacts {
    facts.commands.truncate(MAX_COMMANDS);
    facts.files.truncate(MAX_FILES);
    facts.feedback_explicit.truncate(MAX_FEEDBACK);
    facts
}

/// The explicit-feedback sentences of a user prompt: sentences
/// (newline- or `.!?`-delimited) containing a negation/override
/// pattern, trimmed, truncated to [`MAX_TEXT`], deduped, capped at
/// [`MAX_FEEDBACK`].
pub fn explicit_feedback(user_prompt: &str) -> Vec<String> {
    let re = feedback_matcher();
    let mut out: Vec<String> = Vec::new();
    for raw in user_prompt.split(['.', '!', '?', '\n']) {
        let sentence = raw.trim();
        if sentence.is_empty() {
            continue;
        }
        if re.is_match(sentence) {
            let s = truncate_chars(sentence, MAX_TEXT);
            if !out.contains(&s) {
                out.push(s);
            }
        }
        if out.len() >= MAX_FEEDBACK {
            break;
        }
    }
    out
}

/// v1 heuristic for "the user edited/re-sent after the assistant
/// answered": compare the submitted prompt to the previous user
/// prompt in the conversation. Normalized (case-folded,
/// whitespace-stripped), it is a resend/revision when the first 48
/// normalized chars of either string prefix the other — i.e. one is
/// a leading edit of the other.
pub fn looks_like_resend(prev: &str, cur: &str) -> bool {
    let norm = |s: &str| -> String {
        s.chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(|c| c.to_lowercase())
            .collect()
    };
    let (p, c) = (norm(prev), norm(cur));
    const PREFIX: usize = 48;
    if p.is_empty() || c.is_empty() {
        return false;
    }
    let p_head: String = p.chars().take(PREFIX).collect();
    let c_head: String = c.chars().take(PREFIX).collect();
    p.starts_with(&c_head) || c.starts_with(&p_head)
}

/// The deterministic one-line episode summary (used when the
/// cheap-model pass is unavailable):
/// "Ran N commands, touched M files, feedback: <first explicit or
/// none>".
pub fn deterministic_summary(facts: &TurnFacts) -> String {
    let feedback = facts
        .feedback_explicit
        .first()
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    format!(
        "Ran {} commands, touched {} files, feedback: {}",
        facts.commands.len(),
        facts.files.len(),
        feedback
    )
}

// ============================================
// Durable-entry helpers
// ============================================

/// The user prompt text of a `pi.user` entry record: `model[0]` is a
/// `[UserMessage]` whose `content` is either a plain string (the
/// migration shape) or an array of text blocks (the pi-durable
/// native shape).
pub fn user_text(record: &Value) -> String {
    let Some(messages) = record.get("model").and_then(|m| m.as_array()) else {
        return String::new();
    };
    for message in messages {
        if message.get("role").and_then(|r| r.as_str()) != Some("user") {
            continue;
        }
        match message.get("content") {
            Some(Value::String(s)) => return s.clone(),
            Some(Value::Array(blocks)) => {
                let mut texts: Vec<String> = Vec::new();
                for block in blocks {
                    if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                        if let Some(t) = block.get("text").and_then(|t| t.as_str()) {
                            texts.push(t.to_string());
                        }
                    }
                }
                if !texts.is_empty() {
                    return texts.join("\n");
                }
            }
            _ => {}
        }
    }
    String::new()
}

/// The `stopReason` of the first assistant message in a `pi.assistant`
/// entry record (`None` when absent/unparseable).
pub fn stop_reason(record: &Value) -> Option<String> {
    let messages = record.get("model")?.as_array()?;
    for message in messages {
        if message.get("role").and_then(|r| r.as_str()) == Some("assistant") {
            return message
                .get("stopReason")
                .and_then(|s| s.as_str())
                .map(str::to_string);
        }
    }
    None
}

// ============================================
// Capture driver
// ============================================

/// Capture one completed turn as an episode for the session's agent.
///
/// Fire-and-forget entry point: never panics, never returns — all
/// failures are logged inside. Callers `tokio::spawn` this.
pub async fn capture_turn(
    db: &PgPool,
    durable_schema: &str,
    models_path: &PathBuf,
    embedding_config: &embedding::EmbeddingConfig,
    conversation_id: i64,
    entry_id: i64,
) {
    if let Err(e) = capture_turn_inner(
        db,
        durable_schema,
        models_path,
        embedding_config,
        conversation_id,
        entry_id,
    )
    .await
    {
        tracing::warn!(
            conversation_id,
            entry_id,
            error = %e,
            "episode capture failed (turn unaffected)"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn capture_turn_inner(
    db: &PgPool,
    durable_schema: &str,
    models_path: &PathBuf,
    embedding_config: &embedding::EmbeddingConfig,
    conversation_id: i64,
    entry_id: i64,
) -> Result<(), String> {
    // 1. Read the turn-end entry. Only `pi.assistant` entries with a
    //    terminal stopReason end a turn: `toolUse` means the turn is
    //    still in flight (the final entry will fire its own
    //    TurnEnd).
    let record = sqlx::query_scalar::<_, String>(&format!(
        r#"SELECT record FROM "{durable_schema}".durable_entries WHERE id = $1 AND conversation_id = $2"#
    ))
    .bind(entry_id)
    .bind(conversation_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("read turn-end entry: {e}"))?
    .ok_or_else(|| "turn-end entry vanished".to_string())?;
    let record: Value =
        serde_json::from_str(&record).map_err(|e| format!("turn-end entry not JSON: {e}"))?;
    if record.get("kind").and_then(|k| k.as_str()) != Some("pi.assistant") {
        tracing::debug!(entry_id, "turn-end entry is not pi.assistant; no episode");
        return Ok(());
    }
    if stop_reason(&record) == Some("toolUse".to_string()) {
        // Non-terminal segment: the turn keeps going, capture at the
        // terminal entry.
        tracing::debug!(
            entry_id,
            "turn-end entry is a toolUse segment; no episode yet"
        );
        return Ok(());
    }

    // 2. Session + agent identity. Sessions without an agent are a
    //    documented no-op (episodes are agent memory).
    let session_row: Option<(Uuid, Option<Uuid>)> =
        sqlx::query_as("SELECT id, agent_id FROM sessions WHERE durable_conversation_id = $1")
            .bind(conversation_id)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("session lookup: {e}"))?;
    let (session_id, agent_id) = match session_row {
        Some((session_id, Some(agent_id))) => (session_id, agent_id),
        Some((session_id, None)) => {
            tracing::debug!(
                session_id = %session_id,
                "session has no agent; episode capture is a no-op"
            );
            return Ok(());
        }
        None => {
            tracing::debug!(
                conversation_id,
                "turn-end conversation has no session; no episode"
            );
            return Ok(());
        }
    };

    // 3. Slice start: the most recent `pi.user` entry at or before the
    //    turn-end entry (the submitted prompt).
    let start: Option<(i64, String)> = sqlx::query_as(&format!(
        r#"SELECT id, record FROM "{durable_schema}".durable_entries
           WHERE conversation_id = $1 AND id <= $2 AND record::jsonb->>'kind' = 'pi.user'
           ORDER BY id DESC LIMIT 1"#
    ))
    .bind(conversation_id)
    .bind(entry_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("slice-start lookup: {e}"))?;
    let (slice_start, prompt_record) =
        start.ok_or_else(|| "no pi.user entry at or before the turn-end entry".to_string())?;
    let prompt_record: Value =
        serde_json::from_str(&prompt_record).map_err(|e| format!("user entry not JSON: {e}"))?;
    let user_prompt = user_text(&prompt_record);
    if user_prompt.trim().is_empty() {
        tracing::warn!(
            slice_start,
            "turn slice has an empty user prompt; skipping episode"
        );
        return Ok(());
    }

    // 4. Exactly-once: an episode for the same agent + session whose
    //    source.seq_range overlaps this slice already exists → skip
    //    (redelivered TurnEnd / rescan).
    let dup: Option<i64> = sqlx::query_scalar(
        r#"SELECT 1 FROM episodes
           WHERE agent_id = $1 AND conversation_id = $2
             AND source ? 'seq_range'
             AND (source->'seq_range'->>0)::bigint <= $3
             AND (source->'seq_range'->>1)::bigint >= $4
           LIMIT 1"#,
    )
    .bind(agent_id)
    .bind(session_id)
    .bind(slice_start)
    .bind(entry_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("exactly-once check: {e}"))?;
    if dup.is_some() {
        tracing::debug!(
            session_id = %session_id,
            slice = %slice_start,
            entry_id,
            "episode for this seq range already captured; skipping (exactly-once)"
        );
        return Ok(());
    }

    // 5. Read the slice's entries + the previous user prompt (for the
    //    implicit-feedback flag).
    let slice_rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        r#"SELECT id, record FROM "{durable_schema}".durable_entries
           WHERE conversation_id = $1 AND id BETWEEN $2 AND $3
           ORDER BY id ASC"#
    ))
    .bind(conversation_id)
    .bind(slice_start)
    .bind(entry_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("slice read: {e}"))?;
    let prev_prompt: Option<String> = sqlx::query_scalar::<_, String>(&format!(
        r#"SELECT record FROM "{durable_schema}".durable_entries
           WHERE conversation_id = $1 AND id < $2 AND record::jsonb->>'kind' = 'pi.user'
           ORDER BY id DESC LIMIT 1"#
    ))
    .bind(conversation_id)
    .bind(slice_start)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .and_then(|rec| serde_json::from_str::<Value>(&rec).ok())
    .map(|rec| user_text(&rec))
    .filter(|s| !s.trim().is_empty());

    let records: Vec<Value> = slice_rows
        .iter()
        .filter(|(id, _)| *id > slice_start)
        .map(|(_, raw)| serde_json::from_str::<Value>(raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("slice records not JSON: {e}"))?
        .into_iter()
        .filter(|v| v.get("kind").and_then(|k| k.as_str()) == Some("pi.assistant"))
        .collect();

    // 6. Deterministic extraction, then redaction of every captured
    //    text before it reaches a prompt / embedding / the row.
    let mut facts = extract_turn_facts(&user_prompt, prev_prompt.as_deref(), &records);
    facts.commands = facts
        .commands
        .into_iter()
        .map(|c| memory::redact(&c))
        .collect();
    facts.files = facts
        .files
        .into_iter()
        .map(|f| memory::redact(&f))
        .collect();
    facts.feedback_explicit = facts
        .feedback_explicit
        .into_iter()
        .map(|s| memory::redact(&s))
        .collect();

    // 7. Summary: cheap-model second pass, deterministic fallback.
    let summary = cheap_model_summary(db, models_path, &user_prompt, &facts)
        .await
        .map(|s| memory::redact(&s))
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| memory::redact(&deterministic_summary(&facts)));

    // 8. Feedback column (None when there is no feedback at all).
    let feedback = if !facts.feedback_explicit.is_empty() || facts.implicit_feedback {
        Some(json!({
            "explicit": facts.feedback_explicit,
            "implicit_reprompt": facts.implicit_feedback,
        }))
    } else {
        None
    };

    // 9. Embed summary (+ explicit feedback when present).
    let embed_text = if facts.feedback_explicit.is_empty() {
        summary.clone()
    } else {
        format!(
            "{}\nUser feedback: {}",
            summary,
            facts.feedback_explicit.join("; ")
        )
    };
    let embedding = match embedding::embed(embedding_config, &embed_text).await {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(
                session_id = %session_id,
                error = %e,
                "episode embedding failed; storing without embedding"
            );
            None
        }
    };

    // 10. Insert as the agent's owner (server-side capture: the
    //     caller identity is the agent owner — `insert_episode`'s
    //     tenancy gate then always passes).
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
        .bind(agent_id)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("agent owner lookup: {e}"))?;
    let Some(owner) = owner else {
        return Err("agent has no owner".to_string());
    };
    let caller = memory::Caller {
        user_id: owner,
        is_admin: false,
    };
    let source = json!({
        "conversation_id": session_id.to_string(),
        "seq_range": [slice_start, entry_id],
    });
    match memory::insert_episode(
        db,
        &caller,
        agent_id,
        Some(session_id),
        None,
        &summary,
        feedback,
        embedding,
        source.clone(),
    )
    .await
    {
        Ok(episode) => {
            tracing::info!(
                session_id = %session_id,
                agent_id = %agent_id,
                episode_id = %episode.id,
                slice = %slice_start,
                entry_id,
                commands = facts.commands.len(),
                files = facts.files.len(),
                "episode captured"
            );
            // H4.5 watch scan: an active watch-bearing belief whose
            // predicate matches this episode (and whose cooldown has
            // elapsed) queues a memory trigger the mule forwarder
            // lane will poll and fire. Never fails the turn — the
            // episode is already stored; a trigger miss is a warn.
            if let Err(e) = memory::scan_watch_triggers(db, &caller, agent_id, &episode).await {
                tracing::warn!(
                    agent_id = %agent_id,
                    episode_id = %episode.id,
                    error = %e,
                    "memory watch scan failed (episode stored, no trigger queued)"
                );
            }
            Ok(())
        }
        Err(e) => Err(format!("episode insert: {e}")),
    }
}

// ============================================
// Cheap-model summary pass
// ============================================

/// The H4.2 cheap-model second pass: one small-model LLM call
/// (the `message-router` profile — the same in-process provider
/// resolution as `api::routing::refresh_session_summary`) producing
/// a one-sentence episode summary. `None` when the profile is absent
/// or the endpoint fails → the caller falls back to
/// [`deterministic_summary`].
///
/// Documented follow-up: a dedicated cheap-model profile
/// (`episode-summarizer`) so the router's model choice does not
/// steer episode wording; v1 deliberately reuses the router profile
/// to avoid a new surface (and never spawns a subprocess — the
/// turn-end context does not own a pi harness).
pub(crate) async fn cheap_model_summary(
    db: &PgPool,
    models_path: &PathBuf,
    user_prompt: &str,
    facts: &TurnFacts,
) -> Option<String> {
    let profile: Profile =
        sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE name = $1 LIMIT 1")
            .bind(ROUTER_PROFILE_NAME)
            .fetch_optional(db)
            .await
            .ok()?
            .filter(|p| !p.model.trim().is_empty())?;

    let provider_config = {
        let base_url = profile.base_url.as_deref().filter(|s| !s.is_empty());
        let api_key = profile.api_key.as_deref().filter(|s| !s.is_empty());
        match base_url {
            Some(url) => ProviderConfig {
                base_url: url.to_string(),
                api_key: api_key.unwrap_or("").to_string(),
                api_format: "openai-completions".to_string(),
            },
            None => read_provider_config(models_path, &profile.provider)?,
        }
    };

    // Keep the prompt small: the first 3 commands, all files,
    // all captured feedback sentences.
    let description = format!(
        "User prompt: {}\n\nCommands run ({} total): {}\nFiles touched: {}\nExplicit user feedback: {}",
        memory::redact(&truncate_chars(user_prompt, 500)),
        facts.commands.len(),
        facts.commands.iter().take(3).cloned().collect::<Vec<_>>().join(" | "),
        facts.files.join(", "),
        if facts.feedback_explicit.is_empty() {
            "none".to_string()
        } else {
            facts.feedback_explicit.join(" | ")
        },
    );

    let out = make_llm_call(
        &provider_config,
        &profile.model,
        "You summarize agent activity for a persistent memory store. Reply with EXACTLY one sentence describing what this agent turn accomplished for the user. No preamble.",
        &description,
    )
    .await
    .ok()?
    .trim()
    .to_string();
    (!out.is_empty()).then_some(out)
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assistant_record(content: Value) -> Value {
        json!({
            "id": 1,
            "kind": "pi.assistant",
            "conversationId": 42,
            "model": [{
                "role": "assistant",
                "content": content,
                "stopReason": "toolUse",
            }]
        })
    }

    fn bash_call(cmd: &str) -> Value {
        json!({ "type": "toolCall", "name": "bash", "arguments": { "command": cmd } })
    }

    fn write_call(path: &str) -> Value {
        json!({ "type": "toolCall", "name": "write", "arguments": { "path": path } })
    }

    #[test]
    fn extract_turn_facts_pulls_commands_files_and_feedback() {
        let prompt = "Fix the build. No, run the full test suite, not just compile. Actually I wanted the linting fixed too!";
        let records = vec![
            assistant_record(json!([
                { "type": "text", "text": "on it" },
                bash_call("cargo test --features auth"),
                write_call("src/auth.rs"),
            ])),
            assistant_record(json!([bash_call("curl -s https://example.com/health"),])),
        ];
        let facts = extract_turn_facts(prompt, None, &records);
        assert_eq!(
            facts.commands,
            vec![
                "cargo test --features auth",
                "curl -s https://example.com/health"
            ]
        );
        assert_eq!(facts.files, vec!["src/auth.rs"]);
        assert_eq!(facts.feedback_explicit.len(), 2);
        assert!(facts.feedback_explicit[0].contains("run the full test suite"));
        assert!(!facts.implicit_feedback);
    }

    #[test]
    fn extract_turn_facts_caps_and_truncates() {
        let long = format!("echo {}", "x".repeat(300));
        let records = (0..15)
            .map(|i| assistant_record(json!([bash_call(&format!("cmd-{i} {long}"))])))
            .collect::<Vec<_>>();
        let facts = extract_turn_facts("do stuff", None, &records);
        assert_eq!(facts.commands.len(), MAX_COMMANDS, "command cap");
        assert_eq!(
            facts.commands[0].chars().count(),
            MAX_TEXT,
            "200-char truncation"
        );
        assert!(facts.commands[0].starts_with("cmd-0 "));

        let files = (0..12)
            .map(|i| write_call(&format!("/f/{i}.rs")))
            .collect::<Vec<_>>();
        let facts = extract_turn_facts("x", None, &[assistant_record(json!(files))]);
        assert_eq!(facts.files.len(), MAX_FILES, "file cap");
        assert_eq!(facts.files[0], "/f/0.rs");
    }

    #[test]
    fn extract_turn_facts_dedupes_and_ignores_non_tool_blocks() {
        let records = vec![assistant_record(json!([
            { "type": "text", "text": "thinking out loud" },
            bash_call("cargo build"),
            bash_call("cargo build"),
            { "type": "thinking", "thinking": "a bash echo here" },
            json!({ "type": "toolCall", "name": "read", "arguments": { "path": "/x" } }),
        ]))];
        let facts = extract_turn_facts("", None, &records);
        assert_eq!(
            facts.commands,
            vec!["cargo build"],
            "dedup; read is not a write/edit"
        );
        assert!(facts.files.is_empty());
        assert!(facts.feedback_explicit.is_empty());
    }

    #[test]
    fn explicit_feedback_matches_negation_patterns() {
        assert_eq!(
            explicit_feedback("Actually I wanted tabs, not spaces. Ship it."),
            vec!["Actually I wanted tabs, not spaces"]
        );
        assert!(explicit_feedback("just do the thing").is_empty());
        // "no" must not fire inside words (\b guard): "know", "known".
        assert!(explicit_feedback("known constraints apply").is_empty());
    }

    #[test]
    fn looks_like_resend_detects_edits() {
        assert!(looks_like_resend(
            "Refactor the billing module to use the new client",
            "Refactor the billing module to use the new client, and also"
        ));
        assert!(looks_like_resend(
            "   Refactor   the billing module  ",
            "refactor the billing module to use"
        ));
        assert!(!looks_like_resend(
            "fix the login bug",
            "write a poem about birds"
        ));
        assert!(!looks_like_resend("", "anything"));
        assert!(!looks_like_resend("anything", ""));
    }

    #[test]
    fn deterministic_summary_shape() {
        let facts = TurnFacts {
            commands: vec!["a".into(), "b".into()],
            files: vec!["x.rs".into()],
            feedback_explicit: vec!["no, use bash".into()],
            implicit_feedback: false,
        };
        assert_eq!(
            deterministic_summary(&facts),
            "Ran 2 commands, touched 1 files, feedback: no, use bash"
        );
        let empty = TurnFacts::default();
        assert_eq!(
            deterministic_summary(&empty),
            "Ran 0 commands, touched 0 files, feedback: none"
        );
    }

    #[test]
    fn user_text_handles_string_and_block_content() {
        assert_eq!(
            user_text(
                &json!({ "kind": "pi.user", "model": [{ "role": "user", "content": "hi there" }] })
            ),
            "hi there"
        );
        assert_eq!(
            user_text(
                &json!({ "kind": "pi.user", "model": [{ "role": "user", "content": [{ "type": "text", "text": "a" }, { "type": "text", "text": "b" }] }] })
            ),
            "a\nb"
        );
        assert_eq!(user_text(&json!({ "kind": "pi.user", "model": [] })), "");
        assert_eq!(user_text(&json!({})), "");
    }

    #[test]
    fn stop_reason_reads_first_assistant_message() {
        let record = json!({
            "kind": "pi.assistant",
            "model": [
                { "role": "user", "stopReason": "stop" },
                { "role": "assistant", "stopReason": "toolUse" }
            ]
        });
        assert_eq!(stop_reason(&record), Some("toolUse".to_string()));
        assert_eq!(stop_reason(&json!({})), None);
    }
}
