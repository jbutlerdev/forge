//! Post-H2.6 registry bookkeeping.
//!
//! The cutover deleted the per-session `pi` subprocess machinery
//! (spawn, replay, kill): turns now run on the Node harness
//! (`harness/`) via pi-durable, so there is no in-process agent to
//! registry, respawn, or reap. What survives is the two pieces of
//! process-local bookkeeping the rest of the API still reads:
//!
//! * **in-flight turn marks** — set/cleared by the harness event
//!   consumer (`crate::harness::handle_event` on `task_state`
//!   started/terminal). Used by `GET /agents/:id/active` and by
//!   `GET /sessions/:id` (`agent_running`) as an advisory signal.
//! * **the tool auth token** — the credential the harness-side
//!   `forge-tools` extension uses to call back into
//!   `POST /tools/execute` (accepted by the auth middleware; the
//!   tenancy gate in `execute_tool` still applies).
use std::collections::HashSet;

use uuid::Uuid;

pub struct AgentRegistry {
    /// Credential used to authenticate `/tools/execute*` calls from
    /// the `forge-tools` extension (now the harness's in-process
    /// extension rather than a pi subprocess's). This is the
    /// operator's `FORGE_API_KEY` when the API runs with one
    /// (dev/prod env file), otherwise a random per-process token.
    /// The extension sends it back as `X-API-Key`, and
    /// `auth_middleware` accepts it on the tool endpoints (in
    /// addition to real DB api keys).
    tool_auth_token: String,
    /// Sessions with a turn currently in flight on the harness.
    /// Set by the harness event consumer on `task_state: started`
    /// and cleared on the terminal state — the same marks the legacy
    /// turn driver maintained. `std::sync::RwLock`: held only across
    /// plain set operations, never an await.
    in_flight_turns: std::sync::RwLock<HashSet<Uuid>>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        // The tool token: prefer the operator's key (rotation of
        // `FORGE_API_KEY` in `/etc/forge/forge.env` rotates the
        // credential the extension uses too); fall back to a
        // random per-process token for dev / test runs that have
        // no env key. In the fallback case the token never enters
        // the DB — it is only ever compared against the header the
        // extension sends back.
        let tool_auth_token = std::env::var("FORGE_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("sk_internal_{}", Uuid::new_v4().simple()));
        Self {
            tool_auth_token,
            in_flight_turns: std::sync::RwLock::new(HashSet::new()),
        }
    }

    /// The credential the `forge-tools` extension uses to
    /// authenticate `/tools/execute*` calls. See the
    /// `tool_auth_token` field doc.
    pub fn tool_auth_token(&self) -> &str {
        &self.tool_auth_token
    }

    /// Mark `session_id` as having a harness turn in flight.
    pub fn begin_turn(&self, session_id: Uuid) {
        self.in_flight_turns
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id);
    }

    /// Clear the in-flight mark for `session_id`.
    pub fn end_turn(&self, session_id: Uuid) {
        self.in_flight_turns
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&session_id);
    }

    /// True if a harness turn is currently in flight for
    /// `session_id`.
    pub fn has_in_flight_turn(&self, session_id: Uuid) -> bool {
        self.in_flight_turns
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&session_id)
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}
