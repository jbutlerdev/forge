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
