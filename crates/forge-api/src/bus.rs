//! In-process pub/sub for new message rows.
//!
//! forge clients that want live updates (rather than polling
//! `GET /messages`) connect to `GET /sessions/:id/events`, an SSE
//! endpoint. That handler subscribes to a `MessageBus` and forwards
//! every new row to the wire as it lands in the database.
//!
//! ## Why a broadcast channel?
//!
//! Every active SSE consumer holds one `broadcast::Receiver`; the
//! writer side (the harness and the tool executor) holds the
//! matching `Sender`. A single forge process can have many
//! consumers per session (e.g. multiple appservice instances, or a
//! user with two browser tabs), so per-session `mpsc` would force
//! us to fan out. With `broadcast`, every consumer is independent
//! and a slow client doesn't slow down the others.
//!
//! ## Backpressure
//!
//! `broadcast::Sender::send` returns `Err(RecvError::Lagged(n))` if
//! the channel's bounded buffer is full and the slowest receiver
//! has missed `n` messages. We size the buffer to 256 (about 1MB
//! of message rows in the worst case), which gives the SSE handler
//! ~256 messages of slack before the slow-consumer's receiver
//! reports a lag. The SSE handler treats a lag as "re-query the DB
//! for catch-up and re-anchor the high-water mark", so the client
//! can never miss a row.
//!
//! ## Per-session filtering
//!
//! Every event carries its `session_id`. The handler filters out
//! events for other sessions rather than maintaining a separate
//! bus per session. This keeps the type signature flat and avoids
//! having to remember to clean up per-session senders when a
//! session is deleted.

use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::db::Message;

/// What the bus carries. Two variants for now: a new message row
/// and a "turn ended" signal (the agent emitted `agent_end`).
/// The turn-end signal isn't persisted to the database; it's a
/// pure event for SSE consumers to know the agent is no longer
/// working on this turn.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum BusEvent {
    /// A new row in the `messages` table.
    #[serde(rename = "message")]
    Message { message: Message },

    /// The agent signaled `agent_end` for this session. SSE
    /// consumers can use this to clear typing indicators.
    #[serde(rename = "turn_ended")]
    TurnEnded { session_id: Uuid },
}

/// Bounded broadcast bus. New rows are `try_send`'d — if the
/// channel is full or has no subscribers, the call is a no-op and
/// the message is still in the database (the next polling client
/// will see it).
#[derive(Clone)]
pub struct MessageBus {
    tx: broadcast::Sender<BusEvent>,
}

impl MessageBus {
    /// Construct a new bus with a buffer of 256 events. Sized to
    /// keep the worst-case lag small while letting a slow consumer
    /// miss a few messages without the buffer ever filling.
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(256);
        Self { tx }
    }

    /// Subscribe to the bus. Returns a `broadcast::Receiver` that
    /// yields every event from this point forward.
    pub fn subscribe(&self) -> broadcast::Receiver<BusEvent> {
        self.tx.subscribe()
    }

    /// Publish a new message row. Returns the number of receivers
    /// that got it. Errors are silently dropped: the database is
    /// the source of truth and a polling client can catch up.
    pub fn publish_message(&self, message: Message) {
        tracing::info!(
            session_id = %message.session_id,
            sequence = message.sequence,
            role = %message.role,
            "bus: publish_message"
        );
        // The `try_send` ignores the result; we just want to
        // attempt the publish without panicking on a closed
        // channel or a lagged receiver.
        let _ = self.tx.send(BusEvent::Message { message });
    }

    /// Publish a turn-ended signal. Same semantics as
    /// `publish_message`.
    pub fn publish_turn_ended(&self, session_id: Uuid) {
        tracing::info!(session_id = %session_id, "bus: publish_turn_ended");
        let _ = self.tx.send(BusEvent::TurnEnded { session_id });
    }

    /// Number of active subscribers. Mostly for tests and the
    /// `/metrics` endpoint.
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn test_message() -> Message {
        Message {
            id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            sequence: 1,
            role: "user".to_string(),
            content: Some("hi".to_string()),
            tool_name: None,
            tool_input: None,
            tool_call_id: None,
            tool_output: None,
            duration_ms: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn subscribe_and_receive() {
        let bus = MessageBus::new();
        let mut rx = bus.subscribe();
        let msg = test_message();
        bus.publish_message(msg.clone());
        let event = rx.try_recv().expect("expected event");
        match event {
            BusEvent::Message { message } => assert_eq!(message.id, msg.id),
            _ => panic!("expected Message event"),
        }
    }

    #[test]
    fn publish_with_no_subscribers_is_a_noop() {
        let bus = MessageBus::new();
        // Should not panic, should not block.
        bus.publish_message(test_message());
        bus.publish_turn_ended(Uuid::new_v4());
        assert_eq!(bus.receiver_count(), 0);
    }

    #[test]
    fn multiple_subscribers_each_get_every_event() {
        let bus = MessageBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        bus.publish_message(test_message());
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn turn_ended_event() {
        let bus = MessageBus::new();
        let mut rx = bus.subscribe();
        let sid = Uuid::new_v4();
        bus.publish_turn_ended(sid);
        match rx.try_recv().expect("expected event") {
            BusEvent::TurnEnded { session_id } => assert_eq!(session_id, sid),
            _ => panic!("expected TurnEnded event"),
        }
    }
}
