//! Integration tests for the fused event stream built by
//! `api::events::build_event_stream`.
//!
//! These tests exercise the catch-up + live bus merge, sequence
//! dedup, oneshot teardown, and lag recovery using a real
//! `MessageBus` and a fake [`CatchUpStore`]. No Postgres required.

use std::{convert::Infallible, pin::Pin, sync::Arc, time::Duration};

use chrono::Utc;
use futures_util::{Stream, StreamExt};
use tokio::time::timeout;
use uuid::Uuid;

use forge_api::api::events::{build_event_stream, CatchUpStore, StreamEvent};
use forge_api::bus::MessageBus;
use forge_api::db::Message;

/// Stream shape returned by [`build_event_stream`].
type TestStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, Infallible>> + Send>>;

/// A message row for the given session/sequence with optional content.
fn msg(session_id: Uuid, sequence: i32, content: &str) -> Message {
    Message {
        id: Uuid::new_v4(),
        session_id,
        sequence,
        role: "assistant".to_string(),
        content: Some(content.to_string()),
        tool_name: None,
        tool_input: None,
        tool_call_id: None,
        tool_output: None,
        duration_ms: None,
        created_at: Utc::now(),
    }
}

/// Fake "database" for lag-recovery re-queries: returns every row
/// with `sequence > after_seq` up to `max_seq`, tagged `backfill N`
/// so tests can tell recovered rows apart from catch-up/live rows.
struct FakeStore {
    max_seq: i32,
}

#[async_trait::async_trait]
impl CatchUpStore for FakeStore {
    async fn rows_after(
        &self,
        session_id: Uuid,
        after_seq: i32,
    ) -> Result<Vec<Message>, sqlx::Error> {
        Ok((after_seq + 1..=self.max_seq)
            .map(|s| msg(session_id, s, &format!("backfill {s}")))
            .collect())
    }
}

/// Read the next stream event, failing the test on timeout or early
/// stream end so a stuck stream never hangs the suite.
async fn next_event(stream: &mut TestStream) -> StreamEvent {
    timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for stream event")
        .expect("stream ended unexpectedly")
        .expect("stream error")
}

/// Read the next event, returning `None` if the stream has closed.
async fn next_event_or_eof(stream: &mut TestStream) -> Option<StreamEvent> {
    match timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(Ok(event))) => Some(event),
        Ok(Some(Err(e))) => panic!("stream error: {e:?}"),
        Ok(None) => None,
        Err(_) => panic!("timed out waiting for stream event"),
    }
}

/// Parse a `message` event's JSON data back into a `Message` row.
fn parse_message(event: &StreamEvent) -> Message {
    assert_eq!(event.name, "message", "unexpected event: {}", event.name);
    serde_json::from_str(&event.data).expect("message payload")
}

/// Task 3a: an event published to the bus at the moment the handler
/// subscribes (i.e. before catch-up completes) is delivered exactly
/// once, in sequence order. Under the old order (query first,
/// subscribe second) it would sit in neither the catch-up snapshot
/// nor the new broadcast receiver and be silently lost.
#[tokio::test]
async fn event_published_at_subscribe_is_delivered_once_in_order() {
    let bus = MessageBus::new();
    let sid = Uuid::new_v4();

    // Subscribe FIRST (as the handler now does), then "commit" row 6
    // while the catch-up snapshot (rows 1..=5) is being assembled.
    let rx = bus.subscribe();
    let catchup: Vec<Message> = (1..=5).map(|s| msg(sid, s, "row")).collect();
    bus.publish_message(msg(sid, 6, "row 6"));

    let store: Arc<dyn CatchUpStore> = Arc::new(FakeStore { max_seq: 6 });
    let mut stream: TestStream = build_event_stream(catchup, rx, sid, 0, false, store);

    let mut sequences = Vec::new();
    for _ in 0..6 {
        sequences.push(parse_message(&next_event(&mut stream).await).sequence);
    }
    assert_eq!(
        sequences,
        vec![1, 2, 3, 4, 5, 6],
        "rows must arrive in sequence order, exactly once each"
    );
}

/// Task 3b: bus events published while the catch-up window is still
/// being served must not overtake catch-up rows. The client sees
/// contiguous, strictly-increasing sequences with no duplicates.
#[tokio::test]
async fn bus_events_during_catchup_stay_in_sequence_order() {
    let bus = MessageBus::new();
    let sid = Uuid::new_v4();
    let rx = bus.subscribe();
    let catchup: Vec<Message> = (1..=200).map(|s| msg(sid, s, "row")).collect();

    let store: Arc<dyn CatchUpStore> = Arc::new(FakeStore { max_seq: 206 });
    let mut stream: TestStream = build_event_stream(catchup, rx, sid, 0, false, store);

    // Interleave: read a few catch-up rows, publish live rows while
    // the catch-up window is still being served, then publish more
    // after reading a few more.
    for _ in 0..3 {
        next_event(&mut stream).await;
    }
    for s in 201..=205 {
        bus.publish_message(msg(sid, s, "live"));
    }
    for _ in 0..3 {
        next_event(&mut stream).await;
    }
    bus.publish_message(msg(sid, 206, "live"));

    // Drain the remaining 200 rows (206 total minus the 6 read above).
    let mut sequences: Vec<i32> = (1..=6).collect();
    for _ in 0..200 {
        sequences.push(parse_message(&next_event(&mut stream).await).sequence);
    }
    assert_eq!(sequences, (1..=206).collect::<Vec<i32>>());
}

/// Task 3c: `oneshot=true` closes the stream right after this
/// session's next `turn_ended`. A `turn_ended` for a different
/// session must not close it.
#[tokio::test]
async fn oneshot_closes_stream_after_turn_ended() {
    let bus = MessageBus::new();
    let sid = Uuid::new_v4();
    let rx = bus.subscribe();
    let catchup = vec![msg(sid, 1, "row 1")];

    let store: Arc<dyn CatchUpStore> = Arc::new(FakeStore { max_seq: 2 });
    let mut stream: TestStream = build_event_stream(catchup, rx, sid, 0, true, store);

    // Another session's turn_ended must be ignored (the stream stays
    // open past it because our own turn_ended is published first).
    bus.publish_turn_ended(Uuid::new_v4());
    bus.publish_message(msg(sid, 2, "row 2"));
    bus.publish_turn_ended(sid);

    assert_eq!(parse_message(&next_event(&mut stream).await).sequence, 1);
    assert_eq!(parse_message(&next_event(&mut stream).await).sequence, 2);

    let event = next_event(&mut stream).await;
    assert_eq!(event.name, "turn_ended");
    let payload: serde_json::Value = serde_json::from_str(&event.data).expect("payload");
    assert_eq!(payload["session_id"], sid.to_string());

    // Oneshot: the producer exits after our turn_ended, so the
    // stream must end.
    assert!(
        next_event_or_eof(&mut stream).await.is_none(),
        "stream should have closed after turn_ended with oneshot=true"
    );
}

/// Task 3d: with >256 events published to a slow consumer the
/// broadcast receiver reports `Lagged`; the fused stream must
/// re-query the (fake) DB, backfill the missing rows, and emit a
/// `lagged` event whose `recovered` field is the re-query's actual
/// row count (B6), not rows-since-connect.
#[tokio::test]
async fn lag_recovery_backfills_rows_and_reports_recovered_count() {
    let bus = MessageBus::new();
    let sid = Uuid::new_v4();
    let since = 100;
    let rx = bus.subscribe();

    // Catch-up snapshot at connect time: rows 101..=200.
    let catchup: Vec<Message> = (101..=201).map(|s| msg(sid, s, "row")).collect();
    // The "database" now holds rows up to 500.
    let store: Arc<dyn CatchUpStore> = Arc::new(FakeStore { max_seq: 500 });
    let mut stream: TestStream = build_event_stream(catchup, rx, sid, since, false, store);

    // Slow consumer: publish 300 live events (201..=500) into the
    // 256-slot bus buffer BEFORE reading anything, so the receiver
    // falls behind and reports a lag.
    for s in 201..=501 {
        bus.publish_message(msg(sid, s, "live"));
    }

    let mut sequences: Vec<i32> = Vec::new();
    let mut backfills = 0usize;
    let lagged_payload: serde_json::Value = loop {
        let event = next_event(&mut stream).await;
        match event.name.as_str() {
            "message" => {
                let m: Message = serde_json::from_str(&event.data).expect("payload");
                if m.content
                    .as_deref()
                    .is_some_and(|c| c.starts_with("backfill"))
                {
                    backfills += 1;
                }
                sequences.push(m.sequence);
            }
            "lagged" => {
                break serde_json::from_str::<serde_json::Value>(&event.data).expect("payload");
            }
            other => panic!("unexpected event name: {other}"),
        }
    };

    let payload = &lagged_payload;
    assert!(
        payload["missed"].as_u64().unwrap() > 0,
        "expected a missed count"
    );
    // B6: 'recovered' must equal the number of rows the re-query
    // actually returned. The fake store tags those rows
    // `backfill N`, so the observed backfill count is the ground
    // truth. (The buggy `last_seq - since` formula would report
    // 500 - 100 = 400, which double-counts the catch-up snapshot.)
    let recovered = payload["recovered"].as_u64().expect("recovered present");
    assert_eq!(
        recovered as usize, backfills,
        "recovered must equal the re-query's row count"
    );
    // The split between live-delivered and backfilled rows depends
    // on timing, but after the catch-up snapshot the producer's
    // high-water mark is at 200, so the re-query can return at most
    // 201..=500 (300 rows) and never the buggy 400.
    assert!(
        (recovered as usize) <= 300,
        "recovered ({recovered}) exceeds the re-query's max row count; old last_seq - since bug?"
    );
    // The union of catch-up (101..=200) + live + backfill must be
    // exactly the contiguous range 101..=500 with no duplicates or
    // gaps, regardless of which delivery path each row took.
    assert_eq!(
        sequences,
        (101..=500).collect::<Vec<i32>>(),
        "delivered rows must be contiguous 101..=500 with no dupes"
    );
}

/// Out-of-order bus delivery: two concurrent recorders can commit in
/// an order different from their allocated sequences (the advisory
/// lock serializes allocation, not the commit -> publish gap). The
/// fused stream must park the out-of-order row and deliver the full
/// contiguous sequence exactly once each, in order — the old
/// `sequence <= last_seq -> drop` rule lost the lower row forever
/// (the lag backfill re-queries `sequence > last_seq`, never
/// including it), which made
/// `test_sse_stream_delivers_catchup_and_live_rows_without_gaps`
/// time out whenever the race fired.
#[tokio::test]
async fn out_of_order_bus_events_are_parked_and_delivered_in_order() {
    let bus = MessageBus::new();
    let sid = Uuid::new_v4();
    let rx = bus.subscribe();

    let store: Arc<dyn CatchUpStore> = Arc::new(FakeStore { max_seq: 4 });
    let mut stream: TestStream = build_event_stream(vec![], rx, sid, 0, false, store);

    // Publish deliberately out of sequence order.
    bus.publish_message(msg(sid, 3, "row 3"));
    bus.publish_message(msg(sid, 1, "row 1"));
    bus.publish_message(msg(sid, 4, "row 4"));
    bus.publish_message(msg(sid, 2, "row 2"));

    let mut sequences = Vec::new();
    for _ in 0..4 {
        sequences.push(parse_message(&next_event(&mut stream).await).sequence);
    }
    assert_eq!(
        sequences,
        vec![1, 2, 3, 4],
        "out-of-order rows must be parked and delivered in sequence order, exactly once"
    );
}
