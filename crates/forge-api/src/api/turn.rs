//! Shared agent-turn driver.
//!
//! Both surfaces that drive a pi agent turn — the native
//! `POST /messages` handler (`api::mod::create_message`) and the
//! OpenAI-compatible `POST /v1/chat/completions` handler
//! (`api::openai::run_agent_turn`) — run the same event loop over
//! pi's stdout: read one line at a time, match the `PiEvent`, flush
//! text chunks to the audit log at their boundaries, count in-flight
//! tool calls to pick the right per-read timeout, and stop when the
//! turn ends (or pi errors / dies / times out).
//!
//! This module is that loop, factored out so it exists in exactly one
//! place. Previously the two handlers each carried their own ~250-line
//! copy and the copies had **drifted**: the OpenAI copy handled
//! `PiEvent::Response { success: false }` (surfacing a fast error when
//! pi rejected the prompt — e.g. "No API key found"), but the native
//! copy did not, so a misconfigured profile made `/messages` hang for
//! the 5-minute idle timeout instead of failing immediately. Keeping
//! one copy makes that class of drift structurally impossible.
//!
//! ## What the driver owns vs. what the caller owns
//!
//! The driver owns the mechanics of consuming one turn:
//! - acquiring the per-session agent lock,
//! - draining straggler events from a prior turn,
//! - sending the user prompt,
//! - the read loop + all event matching,
//! - flushing the trailing text chunk + the `[no response from agent]`
//!   placeholder (only on a clean `AgentEnd` with no text),
//! - publishing `turn_ended` on the bus (always — even on error, so SSE
//!   consumers don't get stuck in "agent is typing…"),
//! - bumping `sessions.last_active`,
//! - counting `pi.responses` in metrics.
//!
//! The caller owns everything that is surface-specific: the HTTP
//! wrapping, the compaction prelude (native `/messages`), the
//! fire-and-forget spawn vs. synchronous await, the streaming delta
//! sink, and the mapping of [`TurnEndReason`] to the caller's error
//! type (e.g. `ChatError` for the OpenAI surface).
//!
//! ## Streaming deltas
//!
//! When `delta_tx` is `Some`, each `TextDelta` is forwarded
//! best-effort (`try_send`) so a slow SSE consumer can never
//! backpressure the agent — the same lesson as the bash-streaming
//! `try_send` fix in `api::sse`. A full/closed channel drops the delta
//! (the full text is still accumulated into the returned
//! [`TurnOutcome::text`] and flushed to the audit log).

use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::agent_registry::SharedPiAgent;
use crate::api::{insert_and_publish_assistant, IDLE_READ_TIMEOUT_SECS, TOOL_READ_TIMEOUT_SECS};
use crate::bus::MessageBus;
use crate::observability::Metrics;
use crate::pi_agent::{AssistantMessageEvent, PiEvent};

/// Hard safety net against an infinite read loop. The harness is
/// deliberately patient about *total* runtime (a long agent run can
/// make hundreds of tool calls across many turns); the per-read
/// timeout is the real "is pi stuck?" check. This cap just prevents a
/// wedged pi from spinning forever if the per-read timeout somehow
/// never fires.
const MAX_LOOP_ITERATIONS: u32 = 10000;

/// Why the turn ended. The driver returns this in [`TurnOutcome`] so
/// each caller can map it to its own error type (the native
/// `/messages` handler only logs it; the OpenAI handler maps it to
/// `ChatError`).
#[derive(Debug)]
pub enum TurnEndReason {
    /// The model signaled `agent_end` — the normal, successful
    /// completion of the turn.
    AgentEnd,
    /// pi's RPC response reported `success: false` before any turn
    /// ran (e.g. "No API key found for <provider>"). Surfacing this
    /// fast, instead of waiting for the idle read timeout, is the
    /// bug the unified driver exists to fix.
    ResponseError(String),
    /// pi emitted an `error` event.
    PiError(String),
    /// pi's stdout closed (the process exited) before `agent_end`.
    PiDied,
    /// No event arrived within the per-read timeout. `in_flight_tools`
    /// records whether any tool calls were running when the timeout
    /// fired (so the caller can log which timeout — idle vs. tool —
    /// was hit). The driver kills pi before returning this.
    Timeout { in_flight_tools: u32 },
}

/// The outcome of one agent turn.
///
/// `text` is the full assistant text produced during the turn (every
/// `TextDelta`, across all chunk flushes, without resetting). The
/// native `/messages` handler discards it (it already published each
/// chunk to the bus); the OpenAI handler returns it to the client.
#[derive(Debug)]
pub struct TurnOutcome {
    pub text: String,
    pub reason: TurnEndReason,
}

impl TurnOutcome {
    /// `true` if the turn ended normally (`AgentEnd`).
    pub fn is_ok(&self) -> bool {
        matches!(self.reason, TurnEndReason::AgentEnd)
    }
}

/// Drive one agent turn to completion.
///
/// Acquires the per-session agent lock, drains leftover events from
/// any prior turn, sends `user_content` to pi, and reads pi's event
/// stream until the turn ends (or pi errors / dies / times out).
/// Assistant text deltas are accumulated into [`TurnOutcome::text`]
/// and flushed to the audit log as separate assistant rows (via
/// [`insert_and_publish_assistant`]) at their chunk boundaries
/// (`text_end` / `toolcall_start` / end-of-turn), so the turn is
/// durable and visible to the live SSE event stream exactly like a
/// native `/messages`-driven turn.
///
/// When `delta_tx` is `Some`, each `TextDelta` is also forwarded
/// best-effort for a streaming response.
///
/// The caller must have already resolved the agent (e.g. via
/// `agent_registry.get_or_create`) and decided whether to run a
/// compaction prelude (native `/messages` does; the OpenAI surface
/// does not). The driver does not know about compaction.
#[allow(clippy::too_many_arguments)]
pub async fn drive_turn(
    pool: &PgPool,
    bus: &MessageBus,
    metrics: &Metrics,
    session_id: Uuid,
    agent: SharedPiAgent,
    user_content: &str,
    delta_tx: Option<mpsc::Sender<String>>,
    compact_first: bool,
) -> TurnOutcome {
    let mut guard = agent.lock().await;

    // Flush any straggler events from a previous turn so we don't
    // mistake a stale `agent_end` for this turn's completion. With a
    // long-lived pi process, the `agent_end` / `turn_end` events from
    // a prior response are still in the read buffer.
    guard.drain_pending_events().await;

    // Optional long-context compaction (native `/messages` only). If
    // the prior conversation in the messages table exceeds the
    // model's compaction threshold, ask pi to compact it BEFORE the
    // user's prompt lands, so the first turn doesn't choke on an
    // over-long context. The caller has already decided whether
    // compaction is needed (a rough char-based estimate from the
    // messages table); this just runs the `compact` RPC and drains
    // the `compaction_start`/`compaction_end` events it emits. A
    // compaction failure is logged and we proceed with the prompt
    // anyway — pi is still alive and the model may still respond.
    if compact_first {
        let start = std::time::Instant::now();
        match guard.compact(None).await {
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
                    "long-context resume: pi compaction failed or timed out; proceeding with user prompt anyway"
                );
            }
        }
        // Drain the compaction events so they don't leak into the
        // prompt's event loop.
        guard.drain_pending_events().await;
    }

    // Send the prompt first. pi only emits the `session` event after
    // it receives a message on stdin, so we can't reliably wait for it
    // before sending. The session/turn events will appear at the start
    // of the event stream and are handled below.
    if let Err(e) = guard.send_message(user_content).await {
        tracing::error!(
            session_id = %session_id,
            error = %e,
            "failed to send prompt to pi"
        );
        // No turn ran; still announce turn_ended so SSE consumers
        // don't sit in "agent is typing…" forever.
        bus.publish_turn_ended(session_id);
        let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
            .bind(session_id)
            .execute(pool)
            .await;
        return TurnOutcome {
            text: String::new(),
            reason: TurnEndReason::PiError(e.to_string()),
        };
    }

    // `chunk_buf` accumulates deltas between flush boundaries
    // (text_end / toolcall_start / end-of-turn) and is reset on each
    // flush. `full_text` accumulates the whole turn without resetting
    // — it's the text returned to the caller (the OpenAI client).
    let mut full_text = String::new();
    let mut chunk_buf = String::new();
    let mut produced_any_text = false;
    // We need to see a `turn_start` for *this* turn before we trust
    // the events that follow. Anything read before then is leftover
    // from a prior turn (long-lived pi) or a replayed session
    // (durable resume). Using `turn_start` (not `agent_start`) as the
    // gate is what makes the durable-resume path work: pi replays the
    // loaded session's events immediately, and those include an
    // `agent_end` we must NOT honor.
    let mut seen_turn_start = false;
    // Number of tool calls currently in flight. Drives the per-read
    // timeout: when > 0, pi is silent between `tool_execution_start`
    // and `tool_execution_end`, so the timeout is bumped to
    // `TOOL_READ_TIMEOUT_SECS`. A `u32` is more than enough.
    let mut in_flight_tools: u32 = 0;
    let mut loop_count = 0u32;
    let mut reason = TurnEndReason::AgentEnd;

    while loop_count < MAX_LOOP_ITERATIONS {
        loop_count += 1;

        let read_timeout = if in_flight_tools > 0 {
            Duration::from_secs(TOOL_READ_TIMEOUT_SECS)
        } else {
            Duration::from_secs(IDLE_READ_TIMEOUT_SECS)
        };

        match tokio::time::timeout(read_timeout, guard.read_line()).await {
            Ok(Ok(Some(line))) => match serde_json::from_str::<PiEvent>(&line) {
                Ok(event) => match event {
                    PiEvent::Session { .. } => {
                        tracing::info!(session_id = %session_id, "Pi session ready");
                    }
                    PiEvent::TurnStart => {
                        seen_turn_start = true;
                        tracing::info!(session_id = %session_id, "Turn started");
                    }
                    PiEvent::AgentStart => {
                        tracing::info!(session_id = %session_id, "Agent started");
                    }
                    PiEvent::MessageUpdate {
                        assistant_message_event: Some(evt),
                        ..
                    } if seen_turn_start => match evt {
                        AssistantMessageEvent::TextDelta { delta } => {
                            produced_any_text = true;
                            full_text.push_str(&delta);
                            chunk_buf.push_str(&delta);
                            if let Some(tx) = &delta_tx {
                                // Best-effort forward; a slow or
                                // disconnected SSE consumer must not
                                // backpressure the agent.
                                let _ = tx.try_send(delta.clone());
                            }
                        }
                        AssistantMessageEvent::TextEnd => {
                            let chunk = std::mem::take(&mut chunk_buf);
                            insert_and_publish_assistant(pool, bus, session_id, &chunk).await;
                        }
                        AssistantMessageEvent::ToolCallStart => {
                            // The model is moving from text to a tool
                            // call; any text produced since the last
                            // flush is logically complete — flush it
                            // as its own row before the tool call
                            // lands. (Some pi versions emit `text_end`
                            // immediately before `toolcall_start`; in
                            // that case the buffer is already empty
                            // and this is a no-op. We flush on both
                            // for robustness.)
                            let chunk = std::mem::take(&mut chunk_buf);
                            insert_and_publish_assistant(pool, bus, session_id, &chunk).await;
                        }
                        AssistantMessageEvent::ThinkingDelta { delta } => {
                            tracing::debug!(session_id = %session_id, "[thinking] {}", delta);
                        }
                        AssistantMessageEvent::ToolCallEnd { tool_call } => {
                            // The executor is the sole writer of the
                            // call + result rows (see `AGENTS.md` §7);
                            // the harness only logs here.
                            tracing::debug!(
                                session_id = %session_id,
                                tool_call_id = %tool_call.id,
                                tool = %tool_call.name,
                                "Tool call dispatched (executor will record the call + result rows)"
                            );
                        }
                        _ => {}
                    },
                    PiEvent::ToolExecutionStart {
                        tool_call_id,
                        tool_name,
                        ..
                    } if seen_turn_start => {
                        in_flight_tools = in_flight_tools.saturating_add(1);
                        tracing::debug!(
                            session_id = %session_id,
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
                        in_flight_tools = in_flight_tools.saturating_sub(1);
                        tracing::info!(
                            session_id = %session_id,
                            tool_call_id = %tool_call_id,
                            tool = %tool_name,
                            is_error = %is_error,
                            in_flight = in_flight_tools,
                            "Tool execution finished (recorded by executor)"
                        );
                    }
                    PiEvent::AgentEnd if seen_turn_start => {
                        tracing::info!(session_id = %session_id, "Agent ended");
                        reason = TurnEndReason::AgentEnd;
                        break;
                    }
                    PiEvent::Error { message } => {
                        tracing::error!(session_id = %session_id, "pi error: {}", message);
                        reason = TurnEndReason::PiError(message);
                        break;
                    }
                    // pi's RPC response envelope. A `success: false`
                    // response means the prompt itself failed before
                    // any turn ran — most commonly "No API key found
                    // for <provider>". Without this arm the loop would
                    // ignore the response, keep reading, and hit the
                    // idle timeout — turning a fast config error into
                    // a 5-minute hang. Surface it immediately. (This
                    // is the bug the unified driver fixes: the native
                    // `/messages` loop previously lacked this arm.)
                    PiEvent::Response {
                        success: false,
                        error,
                        command,
                        ..
                    } => {
                        let msg =
                            error.unwrap_or_else(|| format!("pi RPC command '{}' failed", command));
                        tracing::error!(
                            session_id = %session_id,
                            command = %command,
                            "pi RPC response reported failure: {}",
                            msg
                        );
                        reason = TurnEndReason::ResponseError(msg);
                        break;
                    }
                    // Successful responses, `message_start`/`message_end`,
                    // `turn_end`, `extension_ui_request`, and any
                    // pre-`turn_start` event fall through here.
                    _ => {}
                },
                Err(e) => {
                    tracing::warn!(
                        session_id = %session_id,
                        "Failed to parse pi event: {} (line: {:?})",
                        e,
                        line
                    );
                }
            },
            Ok(Ok(None)) => {
                tracing::info!(session_id = %session_id, "pi process ended");
                reason = TurnEndReason::PiDied;
                break;
            }
            Ok(Err(e)) => {
                tracing::error!(session_id = %session_id, "pi read error: {}", e);
                reason = TurnEndReason::PiError(e.to_string());
                break;
            }
            Err(_) => {
                // The per-read timeout fired. Log which one we hit,
                // kill the stuck pi, and bail. The durable-resume
                // path rebuilds from the messages table on the next
                // user message.
                let secs = if in_flight_tools > 0 {
                    TOOL_READ_TIMEOUT_SECS
                } else {
                    IDLE_READ_TIMEOUT_SECS
                };
                tracing::warn!(
                    session_id = %session_id,
                    in_flight_tools,
                    timeout_secs = secs,
                    "pi timed out waiting for response; killing pi (durable resume will rebuild on next message)"
                );
                let _ = guard.kill().await;
                reason = TurnEndReason::Timeout { in_flight_tools };
                break;
            }
        }
    }

    metrics.inc_requests("pi.responses");

    // Flush any trailing text the model emitted after the last chunk
    // boundary (the common case: a final explanation after the last
    // tool call). Only on a clean `AgentEnd` with no text at all do we
    // write the historical "[no response from agent]" placeholder, so
    // consumers that key off "an assistant row landed" still see a row
    // when the model genuinely produced nothing. On error reasons we
    // skip the placeholder — the error itself is the signal, and a
    // fake "no response" row would be misleading in the audit log.
    // Trailing partial text is still flushed regardless of reason
    // (it's real model output).
    if !chunk_buf.is_empty() {
        let chunk = std::mem::take(&mut chunk_buf);
        insert_and_publish_assistant(pool, bus, session_id, &chunk).await;
    } else if !produced_any_text && matches!(reason, TurnEndReason::AgentEnd) {
        insert_and_publish_assistant(pool, bus, session_id, "[no response from agent]").await;
    }

    // Always announce the turn is over, even on error. SSE consumers
    // use this to clear typing indicators; if the agent crashed or
    // timed out, the consumer still wants to know the turn is no
    // longer in flight.
    bus.publish_turn_ended(session_id);

    let _ = sqlx::query("UPDATE sessions SET last_active = NOW() WHERE id = $1")
        .bind(session_id)
        .execute(pool)
        .await;

    TurnOutcome {
        text: std::mem::take(&mut full_text),
        reason,
    }
}

// The driver's read loop is exercised end-to-end by the
// spawn-smoke tests (`tests/pi_spawn_tests.rs`) and the OpenAI
// plumbing test (`tests/openai_tests.rs::
// test_openai_chat_completions_runs_agent_turn`), which drives a
// real `pi` turn with no provider key and asserts the error is
// surfaced (proving the `Response { success: false }` fast-fail
// arm — the bug this unified loop fixes — works for both
// surfaces, since both now call `drive_turn`).
//
// A pure unit test of the loop itself would require a fake
// `PiAgent` event source; `PiAgent` owns a real
// `tokio::process::Child` and its stdout is not injectable today.
// Introducing a trait + fake would let us test the loop's event
// classification in isolation, but it's a larger refactor than
// this pass justifies — the end-to-end tests above already guard
// the fast-fail path.
