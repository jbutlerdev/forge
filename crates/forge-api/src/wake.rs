//! Herd H5.2 — wake-condition forwarding.
//!
//! The two MULE-side wake legs forge can drive (PLAN-HERD §H5.2 wake
//! matrix), both OFF unless configured and both failing soft (a dead
//! mule must never affect a turn):
//!
//! **Turn-end → mule event wake.** The bus already publishes
//! [`crate::bus::BusEvent::TurnEnded`] on every terminal task state.
//! When `FORGE_TURNEND_MULE_BASE` (+ `_KEY`) are set, the forwarder
//! spawned by [`spawn_turnend_forwarder`] resolves the session's
//! `agent_id` (016) and, for agents passing the optional
//! `FORGE_TURNEND_MULE_AGENTS` filter, fires
//! `POST {base}/api/v1/wakes/fire`
//! `{kind:"event", source:"agent.turn_ended",
//! payload:{agent_id, session_id, ts}}` — closing the
//! "agent turn ended → mule event wake" row. (Mule's event lane
//! matches `spec.type` against the fire request's `source` field, so
//! the event TYPE rides `source`; mule wakes are created with
//! `spec {type:"agent.turn_ended"}`.)
//!
//! **Agent-signal push wake** (optional, `FORGE_SIGNAL_WAKE_MULE_BASE`
//! (+ `_KEY`)): after a successful `insert_signal` (H4.6),
//! [`push_signal_wake`] fires
//! `{kind:"event", source:"agent.signal",
//! payload:{kind, from_agent, to_agent, payload_ref}}`. The PULL
//! path (the recipient's `memory:signals` prompt section) remains
//! the PRIMARY delivery — this push is for "wake the recipient
//! NOW" deployments.
//!
//! **Config** is read ONCE at boot into [`WakeConfig`] (held on
//! [`crate::api::AppState`]). Every knob is optional; a misconfig
//! (base without key, non-http(s) base, an invalid UUID in the
//! filter) degrades to a warn log and the affected surface stays off
//! — it never fails boot.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::api::AppState;
use crate::bus::BusEvent;

/// The mule fire endpoint's fixed timeout (PLAN: "10 s timeout").
const MULE_FIRE_TIMEOUT: Duration = Duration::from_secs(10);

// ============================================================
// F-00x: inbound wake-prompt coalescing (backlog guard)
// ============================================================
//
// Mule hourly wakes POST `[mule wake <uuid>] …` prompts into a
// sticky agent conversation. When the agent's turns are slow
// (compaction failures, long audits), every wake fire enqueues
// another submission behind the previous ones: production grew a
// 14-row `durable_submissions` backlog of IDENTICAL prompts, and the
// agent burned turns commenting on its own backlog ("Many wake
// prompts have been batch-queued together…").
//
// The rule enforced at dispatch (see `api::dispatch_message`):
//
// * A wake prompt whose wake id is ALREADY queued-but-not-yet-
//   answered for that conversation (a pending submission, statuses
//   `queued`/`placed`) is DROPPED — the fire succeeds with a
//   `{"coalesced": true}` marker, no user row, no submit.
// * Pending submissions per conversation are capped at
//   [`MAX_PENDING_SUBMISSIONS`]: when full, a duplicate wake is
//   still dropped (duplicate rule wins), anything else is rejected
//   with 429 so the queue cannot grow unbounded.
//
// Wake-id matching rides the harness's conversation-scoped
// `request_id` dedup key: wake dispatches mint
// `request_id = "<wake-id>:<nonce>"` (the nonce keeps each fire
// distinct for the harness's exact-once admission; the prefix makes
// the wake id queryable from `durable_submissions` without parsing
// durable record payloads).

/// Cap on queued-but-not-yet-answered submissions per session
/// (F-00x backlog guard). Wake fires and ordinary messages alike
/// are refused with 429 once the conversation has this many pending
/// submissions — the queue must not grow unbounded.
pub const MAX_PENDING_SUBMISSIONS: usize = 8;

/// The `[mule wake <uuid>]` marker that identifies a mule wake
/// prompt (the prefix mule's wake legs put on the message body).
const WAKE_PREFIX: &str = "[mule wake ";

/// Extract the wake id from a wake-driven message body: content of
/// the form `[mule wake <uuid>] …` yields the uuid; anything else
/// (plain messages, malformed markers, a non-UUID token) is `None`
/// — treated as an ordinary message, never coalesced.
pub fn parse_wake_id(content: &str) -> Option<Uuid> {
    let rest = content.trim_start().strip_prefix(WAKE_PREFIX)?;
    let token = rest.split(']').next()?;
    // Reject anything past the closing bracket that would make the
    // marker malformed (e.g. no `]` at all: `splitn` returns the
    // whole remainder). `token` must be the bracketed id only.
    Uuid::parse_str(token.trim()).ok()
}

/// The harness `request_id` for a wake dispatch: the wake id as a
/// queryable prefix plus a per-attempt nonce, so each fire is a
/// distinct exactly-once admission while the wake id stays
/// recoverable from `durable_submissions.request_id`.
pub fn wake_request_id(wake_id: Uuid) -> String {
    format!("{wake_id}:{}", Uuid::new_v4().simple())
}

/// True when a pending submission's `request_id` belongs to `wake_id`
/// (either the bare wake id or the `"<wake-id>:<nonce>"` form minted
/// by [`wake_request_id`]).
pub fn request_id_is_wake(request_id: &str, wake_id: &Uuid) -> bool {
    let prefix = format!("{wake_id}:");
    request_id == wake_id.to_string() || request_id.starts_with(&prefix)
}

/// Busy-skip TTL (2026-10-26, 30-min check-in cadence): an agent whose
/// previous trigger is STILL in flight must not be woken again — the
/// fire adds backlog, not a check-in. But a task wedged in `running`
/// by a crashed harness must not starve the cadence forever either:
/// once the conversation has been silent (no message row) for this
/// long, the "work" is presumed zombie and the wake proceeds.
pub const WAKE_BUSY_TTL_MINUTES: i64 = 45;

/// Pure busy-gate decision for a wake fire. `active_tasks` counts the
/// conversation's non-terminal durable tasks, `pending_submissions`
/// the queued-but-unanswered harness submissions, `last_message_at`
/// the conversation's newest message row.
///
/// Skip (true) only when ALL hold: it IS a wake (`wake_id` present —
/// human messages are never skipped), the agent is demonstrably still
/// working (active task or queued backlog), AND the conversation is
/// alive (last message inside the TTL — a stale "running" task is a
/// zombie, not work). No message rows yet counts as alive (a task
/// exists; the transcript simply has not started).
pub fn decide_wake_busy(
    wake_id: Option<Uuid>,
    active_tasks: i64,
    pending_submissions: usize,
    last_message_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    if wake_id.is_none() {
        return false;
    }
    if active_tasks <= 0 && pending_submissions == 0 {
        return false;
    }
    match last_message_at {
        Some(t) => now - t < chrono::Duration::minutes(WAKE_BUSY_TTL_MINUTES),
        None => true,
    }
}

/// The coalescing gate's verdict for one inbound dispatch attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeGate {
    /// No pending duplicate, queue below cap: dispatch normally.
    Dispatch,
    /// A submission for the SAME wake id is already queued (not yet
    /// answered): drop the new fire.
    Coalesce {
        /// The wake id extracted from the inbound prompt.
        wake_id: Uuid,
    },
    /// Queue is full (`pending >= cap`) and the fire is not a wake
    /// duplicate: reject it (429) so the backlog cannot grow.
    RejectFull {
        /// Pending submission count observed.
        pending: usize,
        /// The configured cap.
        cap: usize,
    },
}

/// Pure coalescing decision (unit-testable). `pending_request_ids`
/// are the `request_id`s of the conversation's queued-but-not-yet-
/// answered submissions (`status IN ('queued','placed')`); settled
/// rows (`done`/`unanswered`) must be excluded by the caller — an
/// answered wake must not suppress a later fire.
///
/// Duplicate wins over cap: an identical wake fire is always dropped
/// (it adds nothing to the backlog), everything else is rejected when
/// the queue is full.
pub fn decide_wake_gate(
    wake_id: Option<Uuid>,
    pending_request_ids: &[String],
    cap: usize,
) -> WakeGate {
    if let Some(wake_id) = wake_id {
        if pending_request_ids
            .iter()
            .any(|rid| request_id_is_wake(rid, &wake_id))
        {
            return WakeGate::Coalesce { wake_id };
        }
    }
    if pending_request_ids.len() >= cap {
        return WakeGate::RejectFull {
            pending: pending_request_ids.len(),
            cap,
        };
    }
    WakeGate::Dispatch
}

/// Herd H5.2 wake-condition config (see module docs). All fields
/// optional; the zero value (every field `None`) means every surface
/// is off.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WakeConfig {
    /// Mule base URL for the turn-end forwarder
    /// (`FORGE_TURNEND_MULE_BASE`).
    pub turnend_mule_base: Option<String>,
    /// Mule API key (`FORGE_TURNEND_MULE_KEY`, `sk_mule_…`).
    pub turnend_mule_key: Option<String>,
    /// Optional allow-filter of agent ids
    /// (`FORGE_TURNEND_MULE_AGENTS`, comma-separated UUIDs). Empty =
    /// every agent with an `agent_id` on the session.
    pub turnend_mule_agents: Vec<Uuid>,
    /// Mule base URL for the signal push wake
    /// (`FORGE_SIGNAL_WAKE_MULE_BASE`).
    pub signal_wake_mule_base: Option<String>,
    /// Mule API key (`FORGE_SIGNAL_WAKE_MULE_KEY`).
    pub signal_wake_mule_key: Option<String>,
    /// Raw `FORGE_FILEWATCH` value (`"path1:agentA,path2:agentB"`);
    /// parsed + resolved by `crate::filewatch::spawn_filewatch` at
    /// boot.
    pub filewatch_raw: Option<String>,
}

impl WakeConfig {
    /// Production constructor: read the env once, validating each
    /// surface independently (misconfigs warn + disable that surface
    /// only).
    pub fn from_env() -> Self {
        Self::from_pairs(&std::env::vars().collect::<Vec<_>>())
    }

    /// Testable core: build the config from an arbitrary
    /// `(key, value)` slice (the same validation [`Self::from_env`]
    /// applies, minus the process-global env read).
    pub fn from_pairs(pairs: &[(String, String)]) -> Self {
        Self {
            turnend_mule_base: valid_url(Self::get(pairs, "FORGE_TURNEND_MULE_BASE"), "turn-end"),
            turnend_mule_key: Self::get(pairs, "FORGE_TURNEND_MULE_KEY"),
            turnend_mule_agents: parse_agent_filter(Self::get(pairs, "FORGE_TURNEND_MULE_AGENTS")),
            signal_wake_mule_base: valid_url(
                Self::get(pairs, "FORGE_SIGNAL_WAKE_MULE_BASE"),
                "signal push",
            ),
            signal_wake_mule_key: Self::get(pairs, "FORGE_SIGNAL_WAKE_MULE_KEY"),
            filewatch_raw: Self::get(pairs, "FORGE_FILEWATCH")
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
        }
    }

    fn get(pairs: &[(String, String)], key: &str) -> Option<String> {
        pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }
}

/// A base URL is usable only when it is a non-empty http(s) URL;
/// otherwise warn + off (never fail boot).
fn valid_url(raw: Option<String>, what: &str) -> Option<String> {
    let raw = raw?.trim().trim_end_matches('/').to_string();
    if raw.is_empty() {
        return None;
    }
    if raw.starts_with("http://") || raw.starts_with("https://") {
        Some(raw)
    } else {
        tracing::warn!(
            raw = %raw,
            "H5.2 wake: {what} mule base is not an http(s) URL; surface stays off"
        );
        None
    }
}

/// Parse the comma-separated agent-id filter: invalid entries warn and
/// are dropped; all-invalid ⇒ empty filter (all agents) with a warn
/// per bad entry.
fn parse_agent_filter(raw: Option<String>) -> Vec<Uuid> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        match Uuid::parse_str(token) {
            Ok(id) => out.push(id),
            Err(_) => {
                tracing::warn!(
                    token,
                    "H5.2 wake: FORGE_TURNEND_MULE_AGENTS entry is not a UUID; entry dropped"
                );
            }
        }
    }
    out
}

/// POST `{base}/api/v1/wakes/fire` with the given body (Bearer
/// `key`), 10 s timeout. `Ok(())` on 2xx; `Err(String)` on any
/// failure (network, timeout, non-2xx) — callers warn-log and move
/// on; a fire failure NEVER affects the triggering turn.
pub async fn fire_mule_wake(base: &str, key: &str, body: &Value) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(MULE_FIRE_TIMEOUT)
        .build()
        .map_err(|e| format!("client build failed: {e}"))?;
    let url = format!("{}/api/v1/wakes/fire", base);
    let resp = client
        .post(&url)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {key}"))
        .json(body)
        .send()
        .await
        .map_err(|e| format!("mule fire request failed: {e}"))?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(format!("mule fire returned HTTP {status}"))
    }
}

/// Fire-and-forget mule wake: spawn the POST; warn on failure. The
/// `what` label is the caller's wake row (for the log line). Never
/// panics, never blocks the caller beyond a `tokio::spawn`.
pub fn spawn_mule_wake(base: String, key: String, body: Value, what: &'static str) {
    tokio::spawn(async move {
        match fire_mule_wake(&base, &key, &body).await {
            Ok(()) => tracing::info!(what, "H5.2 wake: mule event fired"),
            Err(e) => {
                tracing::warn!(what, error = %e, "H5.2 wake: mule event fire failed (local flow unaffected)")
            }
        }
    });
}

/// The turn-end → mule event-wake forwarder (wake row: "agent turn
/// ended"). Subscribes to the bus; on every
/// [`BusEvent::TurnEnded`] it resolves the session's `agent_id` and,
/// when set (and inside the optional filter), fires the mule event
/// wake. Returns `None` (no worker) when the surface is unconfigured.
pub fn spawn_turnend_forwarder(state: Arc<AppState>) -> Option<JoinHandle<()>> {
    let base = state.wake.turnend_mule_base.clone()?;
    let key = state.wake.turnend_mule_key.clone()?;
    if key.is_empty() {
        tracing::warn!(
            "H5.2 wake: FORGE_TURNEND_MULE_BASE set without a key; turn-end forwarder not started"
        );
        return None;
    }
    let filter = state.wake.turnend_mule_agents.clone();
    tracing::info!(
        base = %base,
        filter = filter.len(),
        "H5.2 wake: turn-end → mule event-wake forwarder started (filter 0 = all agents)"
    );
    // Subscribe BEFORE spawning the task: a broadcast receiver
    // created inside the spawned task would not exist until the
    // task first runs, and a publish in that gap is lost (the
    // forwarder must not miss a turn-end that lands while the
    // runtime is busy).
    let rx = state.bus.subscribe();
    Some(tokio::spawn(async move {
        let mut rx = rx;
        loop {
            match rx.recv().await {
                Ok(BusEvent::TurnEnded { session_id }) => {
                    let agent: Option<Uuid> =
                        sqlx::query_scalar("SELECT agent_id FROM sessions WHERE id = $1")
                            .bind(session_id)
                            .fetch_optional(&state.db)
                            .await
                            .ok()
                            .flatten();
                    match agent {
                        Some(agent_id) if filter.is_empty() || filter.contains(&agent_id) => {
                            let body = json!({
                                "kind": "event",
                                // Mule's event lane matches spec.type
                                // against the fire request's `source`
                                // field (the event TYPE).
                                "source": "agent.turn_ended",
                                "payload": {
                                    "agent_id": agent_id.to_string(),
                                    "session_id": session_id.to_string(),
                                    "ts": chrono::Utc::now().to_rfc3339(),
                                },
                            });
                            spawn_mule_wake(base.clone(), key.clone(), body, "turn-end wake");
                        }
                        // No agent on the session (raw conversation) or
                        // filtered out: no mule call, by design.
                        _ => {}
                    }
                }
                Ok(_) => { /* other events: not our wake row */ }
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!(
                        missed = n,
                        "H5.2 wake: turn-end forwarder lagged the bus (missed turn ends are acceptable)"
                    );
                }
                Err(RecvError::Closed) => break,
            }
        }
        tracing::info!("H5.2 wake: turn-end forwarder exiting");
    }))
}

/// The agent-signal push wake (wake row: "agent signal", the OPTIONAL
/// leg of H4.6's delivery). Called by the signal POST handler after a
/// successful `insert_signal`; a no-op when unconfigured. `payload_ref`
/// is the signal row id (the payload itself stays in the pull path).
pub fn push_signal_wake(
    state: &AppState,
    kind: &str,
    from_agent: Uuid,
    to_agent: Option<Uuid>,
    payload_ref: Uuid,
) {
    let base = match &state.wake.signal_wake_mule_base {
        Some(b) => b.clone(),
        None => return, // unconfigured: pull-only deployment
    };
    let key = match &state.wake.signal_wake_mule_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => {
            tracing::warn!(
                "H5.2 wake: FORGE_SIGNAL_WAKE_MULE_BASE set without a key; signal push skipped"
            );
            return;
        }
    };
    let body = json!({
        "kind": "event",
        // Mule's event lane matches spec.type against `source` (the
        // event TYPE).
        "source": "agent.signal",
        "payload": {
            "kind": kind,
            "from_agent": from_agent.to_string(),
            "to_agent": to_agent.map(|u| u.to_string()),
            "payload_ref": payload_ref.to_string(),
        },
    });
    spawn_mule_wake(base, key, body, "signal push wake");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(kv: &[(&str, &str)]) -> Vec<(String, String)> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn default_is_everything_off() {
        assert_eq!(WakeConfig::from_pairs(&[]), WakeConfig::default());
    }

    #[test]
    fn turnend_surface_parses() {
        let kv = pairs(&[
            ("FORGE_TURNEND_MULE_BASE", "https://mule.example/"),
            ("FORGE_TURNEND_MULE_KEY", "sk_mule_abc"),
            (
                "FORGE_TURNEND_MULE_AGENTS",
                "11111111-1111-4111-8111-111111111111,",
            ),
        ]);
        let cfg = WakeConfig::from_pairs(&kv);
        assert_eq!(
            cfg.turnend_mule_base.as_deref(),
            Some("https://mule.example")
        );
        assert_eq!(cfg.turnend_mule_key.as_deref(), Some("sk_mule_abc"));
        assert_eq!(
            cfg.turnend_mule_agents,
            vec![Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap()]
        );
    }

    #[test]
    fn misconfigs_degrade_without_cross_disabling() {
        let kv = pairs(&[
            // turn-end: base set, key missing ⇒ the forwarder is not
            // started (checked in spawn_turnend_forwarder), but the
            // base parse must not be poisoned by the signal-surface
            // misconfig below.
            ("FORGE_TURNEND_MULE_BASE", "https://mule.example"),
            // signal: base not http(s) → off + warn
            ("FORGE_SIGNAL_WAKE_MULE_BASE", "ftp://nope"),
            ("FORGE_SIGNAL_WAKE_MULE_KEY", "sk_mule_x"),
        ]);
        let cfg = WakeConfig::from_pairs(&kv);
        assert_eq!(
            cfg.turnend_mule_base.as_deref(),
            Some("https://mule.example")
        );
        assert_eq!(cfg.turnend_mule_key, None);
        assert_eq!(cfg.signal_wake_mule_base, None);
        assert_eq!(cfg.signal_wake_mule_key.as_deref(), Some("sk_mule_x"));
    }

    // ---- F-00x wake coalescing: pure-function coverage ----

    const W1: &str = "11111111-1111-4111-8111-111111111111";
    const W2: &str = "22222222-2222-4222-8222-222222222222";

    // ---- busy-skip gate (30-min check-in cadence) ----

    fn wake() -> Option<Uuid> {
        Uuid::parse_str(W1).ok()
    }

    #[test]
    fn busy_skip_only_for_wakes() {
        let now = chrono::Utc::now();
        // Human message while the agent is deep in work: never skipped.
        assert!(!decide_wake_busy(None, 3, 5, Some(now), now));
    }

    #[test]
    fn busy_skip_requires_actual_work_in_flight() {
        let now = chrono::Utc::now();
        // Idle: no tasks, empty queue → dispatch.
        assert!(!decide_wake_busy(wake(), 0, 0, Some(now), now));
        // Running generation → skip.
        assert!(decide_wake_busy(wake(), 1, 0, Some(now), now));
        // No task but queued backlog → skip (next tick catches up).
        assert!(decide_wake_busy(wake(), 0, 2, Some(now), now));
    }

    #[test]
    fn busy_skip_zombie_ttl_escape() {
        let now = chrono::Utc::now();
        let stale = now - chrono::Duration::minutes(WAKE_BUSY_TTL_MINUTES + 15);
        // "Running" task but the conversation has been silent past the
        // TTL: zombie — the cadence must not starve.
        assert!(!decide_wake_busy(wake(), 1, 0, Some(stale), now));
        // Just inside the TTL (crashed harness lookalike): still skip.
        let fresh = now - chrono::Duration::minutes(WAKE_BUSY_TTL_MINUTES - 1);
        assert!(decide_wake_busy(wake(), 1, 0, Some(fresh), now));
        // Working with zero message rows yet: believe the task table.
        assert!(decide_wake_busy(wake(), 1, 0, None, now));
    }

    #[test]
    fn parse_wake_id_accepts_the_wake_marker() {
        let uuid = Uuid::parse_str(W1).unwrap();
        assert_eq!(
            parse_wake_id(&format!("[mule wake {W1}] Hourly herd audit. ...")),
            Some(uuid)
        );
        // Leading whitespace is tolerated; the marker is the
        // discriminator, not its column.
        assert_eq!(parse_wake_id(&format!("  [mule wake {W1}]")), Some(uuid));
    }

    #[test]
    fn parse_wake_id_rejects_non_wakes_and_malformed_markers() {
        assert_eq!(parse_wake_id("hello agent"), None);
        assert_eq!(parse_wake_id("@notify: sandbox approval"), None);
        // marker present but the id is not a UUID → ordinary message
        assert_eq!(parse_wake_id("[mule wake not-a-uuid] audit"), None);
        // no closing bracket → malformed → ordinary message
        assert_eq!(parse_wake_id(&format!("[mule wake {W1} audit")), None);
        // the marker must LEAD the body, not appear inside it
        assert_eq!(
            parse_wake_id(&format!("reminder: [mule wake {W1}] audit")),
            None
        );
    }

    #[test]
    fn wake_request_id_roundtrips_through_the_matcher() {
        let uuid = Uuid::parse_str(W1).unwrap();
        let rid = wake_request_id(uuid);
        assert!(rid.starts_with(&format!("{W1}:")));
        assert!(request_id_is_wake(&rid, &uuid));
        // bare wake id also matches (defensive: a caller that
        // submitted without a nonce)
        assert!(request_id_is_wake(W1, &uuid));
        // a different wake's request id must NOT match
        let other = Uuid::parse_str(W2).unwrap();
        assert!(!request_id_is_wake(&wake_request_id(uuid), &other));
        // nonces make each fire a distinct request id
        assert_ne!(wake_request_id(uuid), wake_request_id(uuid));
    }

    #[test]
    fn gate_coalesces_same_wake_id_pending() {
        let uuid = Uuid::parse_str(W1).unwrap();
        let pending = vec![wake_request_id(uuid), "unrelated-request".to_string()];
        assert_eq!(
            decide_wake_gate(Some(uuid), &pending, MAX_PENDING_SUBMISSIONS),
            WakeGate::Coalesce { wake_id: uuid }
        );
        // Different wake id, queue not full → dispatch.
        let other = Uuid::parse_str(W2).unwrap();
        assert_eq!(
            decide_wake_gate(Some(other), &pending, MAX_PENDING_SUBMISSIONS),
            WakeGate::Dispatch
        );
    }

    #[test]
    fn gate_caps_the_backlog_and_duplicate_wins_over_cap() {
        let uuid = Uuid::parse_str(W1).unwrap();
        let other = Uuid::parse_str(W2).unwrap();
        let full: Vec<String> = (0..MAX_PENDING_SUBMISSIONS)
            .map(|i| format!("req-{i}"))
            .collect();
        // Full queue + fresh wake → rejected (must not grow).
        assert_eq!(
            decide_wake_gate(Some(other), &full, MAX_PENDING_SUBMISSIONS),
            WakeGate::RejectFull {
                pending: MAX_PENDING_SUBMISSIONS,
                cap: MAX_PENDING_SUBMISSIONS,
            }
        );
        // Full queue + non-wake message → rejected too.
        assert_eq!(
            decide_wake_gate(None, &full, MAX_PENDING_SUBMISSIONS),
            WakeGate::RejectFull {
                pending: MAX_PENDING_SUBMISSIONS,
                cap: MAX_PENDING_SUBMISSIONS,
            }
        );
        // Full queue + DUPLICATE wake → the duplicate drops (it adds
        // nothing to the backlog), no rejection.
        let mut full_with_dup = full.clone();
        full_with_dup[3] = wake_request_id(uuid);
        assert_eq!(
            decide_wake_gate(Some(uuid), &full_with_dup, MAX_PENDING_SUBMISSIONS),
            WakeGate::Coalesce { wake_id: uuid }
        );
        // Just below the cap → dispatch.
        assert_eq!(
            decide_wake_gate(
                None,
                &full[..MAX_PENDING_SUBMISSIONS - 1],
                MAX_PENDING_SUBMISSIONS
            ),
            WakeGate::Dispatch
        );
    }

    #[test]
    fn agent_filter_drops_invalid_entries() {
        let kv = pairs(&[(
            "FORGE_TURNEND_MULE_AGENTS",
            "not-a-uuid,22222222-2222-4222-8222-222222222222",
        )]);
        let cfg = WakeConfig::from_pairs(&kv);
        assert_eq!(
            cfg.turnend_mule_agents,
            vec![Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap()]
        );
    }
}
