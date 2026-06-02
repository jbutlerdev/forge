//! Integration tests for the live event stream. These run
//! without a real database — we just exercise the bus
//! directly, which is the data structure that backs the
//! `GET /sessions/:id/events` SSE handler.
//!
//! For a true end-to-end test (SSE wire format, DB catch-up),
//! the integration test suite in `tests/integration_tests.rs`
//! is the right place. Those require a running Postgres, so
//! they live outside the `cargo test` default run.

#[cfg(test)]
mod tests {
    use crate::bus::{BusEvent, MessageBus};
    use crate::db::Message;
    use chrono::Utc;
    use uuid::Uuid;

    fn test_message(role: &str) -> Message {
        let content = Some(format!("hi from {}", role));
        Message {
            id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            sequence: 1,
            role: role.to_string(),
            content,
            tool_name: None,
            tool_input: None,
            tool_call_id: None,
            tool_output: None,
            duration_ms: None,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn bus_subscribe_and_receive_message() {
        let bus = MessageBus::new();
        let mut rx = bus.subscribe();
        let msg = test_message("assistant");
        bus.publish_message(msg.clone());
        let event = rx.recv().await.expect("expected event");
        match event {
            BusEvent::Message { message } => {
                assert_eq!(message.id, msg.id);
                assert_eq!(message.role, "assistant");
            }
            _ => panic!("expected Message event, got {event:?}"),
        }
    }

    #[tokio::test]
    async fn bus_subscribe_and_receive_turn_ended() {
        let bus = MessageBus::new();
        let mut rx = bus.subscribe();
        let sid = Uuid::new_v4();
        bus.publish_turn_ended(sid);
        let event = rx.recv().await.expect("expected event");
        match event {
            BusEvent::TurnEnded { session_id } => assert_eq!(session_id, sid),
            _ => panic!("expected TurnEnded event, got {event:?}"),
        }
    }

    #[tokio::test]
    async fn bus_serializes_messages_in_publish_order() {
        let bus = MessageBus::new();
        let mut rx = bus.subscribe();
        // Publish 5 messages in order; verify they arrive in
        // the same order. The broadcast channel is FIFO
        // for a single receiver.
        for i in 1..=5 {
            let mut m = test_message("assistant");
            m.sequence = i;
            bus.publish_message(m);
        }
        for i in 1..=5 {
            let event = rx.recv().await.expect("expected event");
            match event {
                BusEvent::Message { message } => {
                    assert_eq!(message.sequence, i, "messages out of order");
                }
                _ => panic!("expected Message event, got {event:?}"),
            }
        }
    }

    #[tokio::test]
    async fn bus_supports_multiple_subscribers() {
        let bus = MessageBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        let msg = test_message("user");
        bus.publish_message(msg.clone());
        // Both receivers get a copy.
        let e1 = rx1.recv().await.expect("rx1");
        let e2 = rx2.recv().await.expect("rx2");
        assert!(matches!(e1, BusEvent::Message { .. }));
        assert!(matches!(e2, BusEvent::Message { .. }));
    }
}
