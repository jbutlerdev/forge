//! Thin client for the forge **Node harness** process (Herd H2.0).
//!
//! The harness (`~/src/forge/harness/`, TypeScript) runs
//! `@earendil-works/pi-durable` over the `durable-pg` Postgres backend and
//! owns durable conversations, tasks, checkpoints, and timers. forge-api
//! (Rust) drives it over two unix sockets — both spoken as **one JSON
//! object per line**:
//!
//! * **RPC socket** (`FORGE_HARNESS_SOCKET`, default
//!   `~/.local/state/forge/harness.sock`): forge-api connects and sends
//!   requests,
//!   `{"id", "method", "params"}`; the harness replies
//!   `{"id", "result"}` or `{"id", "error": {"code", "message"}}`.
//!   Methods (1:1 with `harness/src/ipc.ts`): `status`,
//!   `createConversation`, `submit`, `importEntries`, `steer`,
//!   `abort`, `documentGet`, `documentPut`, `timerSet`, `timerClear`,
//!   `timerList`, `compact`, `reset`, `compactionStatus`.
//! * **Events socket** (`FORGE_HARNESS_EVENTS_SOCKET`, default
//!   `~/.local/state/forge/harness-events.sock`): the harness
//!   **listens**; forge-api connects (one client; a second connection
//!   displaces the first). The harness pushes JSON-line events
//!   (`hello`, `task_state`, `turn_end`, `document_changed`,
//!   `timer_fired`). **No replay on reconnect** — the
//!   [`HarnessEvent::ResyncRequired`] marker tells the consumer to
//!   re-derive state from Postgres instead.
//!
//! ## Reconnect semantics (both sockets)
//!
//! The harness restarts frequently **by design** (pi-durable is a
//! long-lived-but-crashable process; systemd `Restart=always` self-heals
//! it), so both the [`IpcClient`] and [`EventStream`] run internal
//! redial loops: exponential backoff 250 ms → 5 s cap, forever.
//!
//! * **In-flight requests** (already written to the socket) die when the
//!   connection drops and fail with [`HarnessError::Disconnected`].
//!   Callers retry; [`IpcClient::submit`] is safe to resubmit with the
//!   same `request_id` — the harness dedupes exactly-once
//!   (`submissionByRequest`), so a retry after a mid-flight crash
//!   returns the original submission instead of a duplicate turn.
//! * **Requests made while disconnected** wait for the connection with a
//!   bound (`Limits::disconnect_wait`, default 5 s) and then fail with
//!   [`HarnessError::Disconnected`].
//! * **Event stream overflow** is bounded backpressure: the redial task
//!   waits up to `Limits::event_send_timeout` for the consumer to drain
//!   before dropping the event with a warning. This bound is what keeps
//!   a slow consumer from ever blocking the harness's commit path (the
//!   events socket is drained as each event is accepted for delivery, so
//!   a stalled consumer can back up at most one window of socket reads).
//!
//! ## Disabled mode
//!
//! [`HarnessClient::from_env`] / [`HarnessClient::from_paths`] return a
//! **disabled** client when `FORGE_HARNESS_SOCKET` is unset or the RPC
//! socket file does not exist at startup. A disabled client spawns no
//! tasks and makes every call fail fast with
//! [`HarnessError::Unavailable`] — forge-api then keeps the legacy
//! `drive_turn` path as the default with zero behavior change. The
//! underlying [`IpcClient::connect`] / [`EventStream::connect`] do **not**
//! gate on socket existence (they redial forever); the existence check
//! is the harness-client boundary's job so tests can exercise the redial
//! loop against sockets that appear later.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};

/// Typed failure for a harness RPC / stream operation.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum HarnessError {
    /// The harness client is in **disabled** mode (socket unset/absent at
    /// startup) — no connection was ever attempted.
    #[error("harness unavailable (disabled mode)")]
    Unavailable,
    /// The socket is down: the request died on a reconnect (in-flight
    /// requests are never replayed — the caller retries, and
    /// [`IpcClient::submit`] dedupes by `request_id`) or the
    /// disconnect-wait bound elapsed before a connection was made.
    #[error("harness disconnected (redialing)")]
    Disconnected,
    /// A line on the wire was not the expected JSON shape.
    #[error("harness protocol error: {0}")]
    Protocol(String),
    /// The harness answered with a typed RPC error
    /// (`{"id", "error": {"code", "message"}}`); `code` is the wire
    /// contract from `harness/src/ipc.ts` (`invalid_params`,
    /// `unknown_conversation`, `unknown_task`, `unknown_method`,
    /// `bad_request`, `internal`).
    #[error("harness rpc error [{code}]: {message}")]
    Rpc { code: String, message: String },
    /// A connected harness never answered within
    /// [`Limits::response_timeout`].
    #[error("harness request timed out")]
    Timeout,
}

/// Tuning knobs for the redial / wait loops. Production defaults via
/// [`Default`]; tests use short values so the suite stays fast.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Initial reconnect backoff (both sockets).
    pub min_backoff: Duration,
    /// Cap on the reconnect backoff (both sockets).
    pub max_backoff: Duration,
    /// How long a request made **while disconnected** waits for the next
    /// connection before failing with [`HarnessError::Disconnected`].
    pub disconnect_wait: Duration,
    /// How long a **connected** request waits for its response before
    /// failing with [`HarnessError::Timeout`].
    pub response_timeout: Duration,
    /// How long the events redial task waits for the consumer to drain a
    /// full 64-event buffer before dropping the event with a warning.
    pub event_send_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(5),
            disconnect_wait: Duration::from_secs(5),
            response_timeout: Duration::from_secs(30),
            event_send_timeout: Duration::from_secs(5),
        }
    }
}

/* ------------------------------------------------------------------ */
/* JSON-lines codec (shared by both sockets)                           */
/* ------------------------------------------------------------------ */

/// Encode one RPC request line: `{"id", "method", "params"}`.
/// `id` is a plain non-negative integer echoed back by the harness.
pub fn encode_request(id: u64, method: &str, params: &serde_json::Value) -> String {
    serde_json::json!({ "id": id, "method": method, "params": params }).to_string()
}

/// One decoded RPC response: the matched request `id` plus its result.
/// A `result` and an `error` are mutually exclusive on the wire; both
/// present (or both absent) is a protocol violation.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedResponse {
    pub id: u64,
    pub result: Result<serde_json::Value, HarnessError>,
}

/// Decode one response line: `{"id", "result"}` or
/// `{"id", "error": {"code", "message"}}`.
///
/// Returns `Ok(None)` for well-formed lines whose `id` is not a number
/// (the harness's error reply for malformed request lines uses
/// `"id": null` — it matches no client-side request and is dropped).
pub fn parse_response(line: &str) -> Result<Option<ParsedResponse>, HarnessError> {
    let v: serde_json::Value = serde_json::from_str(line)
        .map_err(|e| HarnessError::Protocol(format!("response is not JSON: {e}")))?;
    let obj = v
        .as_object()
        .ok_or_else(|| HarnessError::Protocol("response is not a JSON object".into()))?;

    let id = match obj.get("id").and_then(|v| v.as_u64()) {
        Some(id) => id,
        None => return Ok(None), // "id": null — reply to a line we never sent
    };

    let has_result = obj.contains_key("result");
    let has_error = obj.contains_key("error");
    match (has_result, has_error) {
        (true, false) => {
            let result = obj
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok(Some(ParsedResponse {
                id,
                result: Ok(result),
            }))
        }
        (false, true) => {
            let err = obj
                .get("error")
                .and_then(|e| e.as_object())
                .ok_or_else(|| {
                    HarnessError::Protocol(format!(
                        "error field is not an object: {}",
                        obj.get("error").unwrap()
                    ))
                })?;
            let code = err
                .get("code")
                .and_then(|c| c.as_str())
                .unwrap_or("internal")
                .to_string();
            let message = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown harness error")
                .to_string();
            Ok(Some(ParsedResponse {
                id,
                result: Err(HarnessError::Rpc { code, message }),
            }))
        }
        _ => Err(HarnessError::Protocol(
            "response has both (or neither) of result/error".into(),
        )),
    }
}

/// Terminal status of a harness task (mapped from pi-durable outcomes in
/// `harness/src/events.ts`: completed→done, failed→failed,
/// aborted-or-orphaned→aborted).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// First transition into `running`.
    Started,
    /// pi-durable outcome `completed`.
    Done,
    /// pi-durable outcome `failed`.
    Failed,
    /// pi-durable outcome `aborted` (or orphaned).
    Aborted,
}

/// Events pushed on the events socket (see `harness/src/events.ts` for
/// the wire shapes this enum mirrors 1:1), plus the client-side
/// [`HarnessEvent::ResyncRequired`] marker (never on the wire).
#[derive(Debug, Clone, PartialEq)]
pub enum HarnessEvent {
    /// On (re)connect the harness announces itself.
    Hello { version: String },
    /// A task moved to `started` (first `running`) or to a terminal
    /// status.
    TaskState {
        task_id: i64,
        conversation_id: i64,
        status: TaskState,
        /// Raw pi-durable outcome status for terminal tasks.
        outcome_status: Option<String>,
    },
    /// A generation-produced `pi.assistant` entry was committed: the end
    /// of a turn (or of a turn's current answer segment).
    TurnEnd {
        conversation_id: i64,
        entry_id: i64,
        /// First ≤200 chars of the assistant text; empty for tool-only.
        summary: String,
    },
    /// A harness document (`forge.*`) changed.
    DocumentChanged { conversation_id: i64, name: String },
    /// A harness timer fired and submitted its prompt.
    TimerFired {
        timer_id: String,
        conversation_id: i64,
        prompt: String,
    },
    /// Herd H2.2: the `spawn_subagent` tool created (or re-found, on a
    /// replayed call) a child conversation. `child_forge_session_id` is
    /// pre-minted by the harness and must be reused as the child's
    /// session row id by the consumer.
    SubagentSpawned {
        parent_conversation_id: i64,
        child_conversation_id: i64,
        child_forge_session_id: String,
        /// The `task` argument of the spawn tool call.
        task: String,
        /// Detached subagents are owned by a background anchor task.
        detached: bool,
    },
    /// Client-side marker (**never on the wire**): emitted by
    /// [`EventStream`] on every (re)connect, because the harness does
    /// not replay events. Consumers re-derive state from their own
    /// source of truth (see `harness/README.md`, "No replay on
    /// reconnect").
    ResyncRequired,
}

/* ------------------------------------------------------------------ */
/* Events-socket codec                                                 */
/* ------------------------------------------------------------------ */

/// Decode one events-socket line into a [`HarnessEvent`].
///
/// The wire shapes are camelCase and mirror `harness/src/events.ts`
/// exactly; [`HarnessEvent::ResyncRequired`] is client-side only and
/// is never produced here.
///
/// Returns `Ok(None)` for well-formed lines with an unrecognized
/// `type` (forward compatibility: a newer harness may add event
/// types this client should ignore, not choke on).
pub fn parse_event(line: &str) -> Result<Option<HarnessEvent>, HarnessError> {
    let v: serde_json::Value = serde_json::from_str(line)
        .map_err(|e| HarnessError::Protocol(format!("event is not JSON: {e}")))?;
    let obj = v
        .as_object()
        .ok_or_else(|| HarnessError::Protocol("event is not a JSON object".into()))?;

    let obj_id = |field: &str| -> Result<i64, HarnessError> {
        obj.get(field).and_then(|v| v.as_i64()).ok_or_else(|| {
            HarnessError::Protocol(format!(
                "event field {} is not an integer: {}",
                field,
                obj.get(field).unwrap_or(&serde_json::Value::Null)
            ))
        })
    };

    match obj
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| HarnessError::Protocol("event missing string type".into()))?
    {
        "hello" => Ok(Some(HarnessEvent::Hello {
            version: obj
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })),
        "task_state" => {
            let status = match obj
                .get("status")
                .and_then(|s| s.as_str())
                .ok_or_else(|| HarnessError::Protocol("task_state missing status".into()))?
            {
                "started" => TaskState::Started,
                "done" => TaskState::Done,
                "failed" => TaskState::Failed,
                "aborted" => TaskState::Aborted,
                other => {
                    return Err(HarnessError::Protocol(format!(
                        "task_state has unknown status: {other}"
                    )))
                }
            };
            Ok(Some(HarnessEvent::TaskState {
                task_id: obj_id("taskId")?,
                conversation_id: obj_id("conversationId")?,
                status,
                outcome_status: obj
                    .get("outcomeStatus")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            }))
        }
        "turn_end" => Ok(Some(HarnessEvent::TurnEnd {
            conversation_id: obj_id("conversationId")?,
            entry_id: obj_id("entryId")?,
            summary: obj
                .get("summary")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string(),
        })),
        "document_changed" => Ok(Some(HarnessEvent::DocumentChanged {
            conversation_id: obj_id("conversationId")?,
            name: obj
                .get("name")
                .and_then(|s| s.as_str())
                .ok_or_else(|| HarnessError::Protocol("document_changed missing name".into()))?
                .to_string(),
        })),
        "timer_fired" => Ok(Some(HarnessEvent::TimerFired {
            timer_id: obj
                .get("timerId")
                .and_then(|s| s.as_str())
                .ok_or_else(|| HarnessError::Protocol("timer_fired missing timerId".into()))?
                .to_string(),
            conversation_id: obj_id("conversationId")?,
            prompt: obj
                .get("prompt")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string(),
        })),
        "subagent_spawned" => Ok(Some(HarnessEvent::SubagentSpawned {
            parent_conversation_id: obj_id("parentConversationId")?,
            child_conversation_id: obj_id("childConversationId")?,
            child_forge_session_id: obj
                .get("childForgeSessionId")
                .and_then(|s| s.as_str())
                .ok_or_else(|| {
                    HarnessError::Protocol("subagent_spawned missing childForgeSessionId".into())
                })?
                .to_string(),
            task: obj
                .get("task")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string(),
            detached: obj
                .get("detached")
                .and_then(|d| d.as_bool())
                .unwrap_or(false),
        })),
        _ => {
            // Forward compatibility: ignore unknown event types.
            Ok(None)
        }
    }
}

/* ------------------------------------------------------------------ */
/* Shared plumbing                                                     */
/* ------------------------------------------------------------------ */

/// Refuse lines longer than this: a malformed peer (or a protocol
/// desync) must not make us buffer unbounded input.
const MAX_LINE: usize = 1024 * 1024;

/// Read one `\n`-terminated line off a unix stream.
///
/// `buf` is a **rolling buffer of unconsumed bytes** from earlier
/// reads (a single socket read can carry several lines): it is NOT
/// cleared here and keeps its trailing remainder between calls.
///
/// `Ok(None)` is EOF (with no pending bytes). A non-empty final line
/// without a trailing newline is still returned (the harness always
/// terminates lines, so this is only reached when it dies mid-line).
async fn read_line(stream: &mut UnixStream, buf: &mut Vec<u8>) -> std::io::Result<Option<String>> {
    loop {
        // A whole line may already be buffered from a prior read.
        if let Some(idx) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buf.drain(..=idx).collect();
            return Ok(Some(String::from_utf8_lossy(&line[..idx]).into_owned()));
        }
        if buf.len() > MAX_LINE {
            return Err(std::io::Error::other("harness line too long"));
        }
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            let rest = std::mem::take(buf);
            return Ok(Some(String::from_utf8_lossy(&rest).into_owned()));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/* ------------------------------------------------------------------ */
/* RPC client                                                          */
/* ------------------------------------------------------------------ */

/// One request queued for the connection task.
struct PendingCmd {
    id: u64,
    bytes: Vec<u8>,
    done: oneshot::Sender<Result<ParsedResponse, HarnessError>>,
}

/// Async JSON-lines client for the harness **RPC socket**
/// (`harness/src/ipc.ts`).
///
/// One background task owns the connection for the client's lifetime:
/// it redials forever (backoff 250 ms → 5 s), answers requests, and
/// fails every in-flight request with [`HarnessError::Disconnected`]
/// when the connection drops. Requests made while down are bounded by
/// [`Limits::disconnect_wait`]; see the module docs for the full
/// reconnect / dedup semantics.
///
/// Dropping the client lets the connection task exit on its own (its
/// request channel closes).
#[derive(Debug)]
pub struct IpcClient {
    path: PathBuf,
    limits: Limits,
    next_id: AtomicU64,
    cmd_tx: mpsc::Sender<PendingCmd>,
    connected: Arc<AtomicBool>,
    _task: tokio::task::JoinHandle<()>,
}

impl IpcClient {
    /// Start redialing `path` immediately. This does **not** check that
    /// the socket exists — a socket that appears later is picked up by
    /// the redial loop (that is what [`HarnessClient::from_env`] gates
    /// on for the disabled-mode decision).
    pub fn new(path: impl Into<PathBuf>, limits: Limits) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let connected = Arc::new(AtomicBool::new(false));
        let path: PathBuf = path.into();
        let task = tokio::spawn(ipc_task(cmd_rx, path.clone(), limits, connected.clone()));
        Self {
            path,
            limits,
            next_id: AtomicU64::new(0),
            cmd_tx,
            connected,
            _task: task,
        }
    }

    /// True if a connection was established at this instant.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// The socket path this client dials.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Issue one RPC. `method` is one of the wire methods documented
    /// on the module; `params` is the JSON object body.
    ///
    /// * In-flight request when the socket dies →
    ///   [`HarnessError::Disconnected`] (callers retry; `submit` is
    ///   safe to resubmit with the same `request_id`).
    /// * Made while disconnected → waits up to
    ///   [`Limits::disconnect_wait`], then [`HarnessError::Disconnected`].
    /// * Made while connected → waits up to [`Limits::response_timeout`],
    ///   then [`HarnessError::Timeout`].
    pub async fn call(
        &self,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let mut bytes = encode_request(id, method, params).into_bytes();
        bytes.push(b'\n');
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .try_send(PendingCmd {
                id,
                bytes,
                done: tx,
            })
            .is_err()
        {
            return Err(HarnessError::Disconnected);
        }
        let bound = if self.is_connected() {
            self.limits.response_timeout
        } else {
            self.limits.disconnect_wait
        };
        match tokio::time::timeout(bound, rx).await {
            Ok(Ok(Ok(resp))) => resp.result,
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(HarnessError::Disconnected),
            Err(_) => {
                // Our own bound elapsed: distinguish "never connected"
                // (Disconnected) from "connected but silent" (Timeout).
                if self.is_connected() {
                    Err(HarnessError::Timeout)
                } else {
                    Err(HarnessError::Disconnected)
                }
            }
        }
    }
}

async fn ipc_task(
    mut cmd_rx: mpsc::Receiver<PendingCmd>,
    path: PathBuf,
    limits: Limits,
    connected: Arc<AtomicBool>,
) {
    use std::collections::HashMap;

    let mut backoff = limits.min_backoff;
    loop {
        if cmd_rx.is_closed() {
            return; // client dropped
        }
        match UnixStream::connect(&path).await {
            Ok(raw) => {
                backoff = limits.min_backoff;
                connected.store(true, Ordering::Relaxed);
                tracing::info!("harness rpc: connected to {}", path.display());

                // Two OS handles over the same socket: the reader task
                // owns one, this task owns the other.
                let std_stream = raw.into_std().expect("harness rpc: into_std");
                let reader_std = std_stream.try_clone().expect("harness rpc: try_clone");
                let reader =
                    UnixStream::from_std(reader_std).expect("harness rpc: from_std(reader)");
                let mut writer =
                    UnixStream::from_std(std_stream).expect("harness rpc: from_std(writer)");
                let (line_tx, mut line_rx) = mpsc::channel::<String>(64);
                let reader_task = tokio::spawn(async move {
                    let mut stream = reader;
                    let mut buf = Vec::new();
                    loop {
                        match read_line(&mut stream, &mut buf).await {
                            Ok(Some(line)) => {
                                if line_tx.send(line).await.is_err() {
                                    return;
                                }
                            }
                            _ => return,
                        }
                    }
                });

                // In-flight requests awaiting a response, by id. Failed
                // en masse with Disconnected when the connection drops.
                let mut pending: HashMap<
                    u64,
                    oneshot::Sender<Result<ParsedResponse, HarnessError>>,
                > = HashMap::new();

                // Flush requests that queued up while we were down.
                while let Ok(cmd) = cmd_rx.try_recv() {
                    if let Err(e) = writer.write_all(&cmd.bytes).await {
                        tracing::warn!("harness rpc: flush write failed: {e}");
                        break;
                    }
                    pending.insert(cmd.id, cmd.done);
                }

                // Connected: serve requests and responses until either
                // side goes away.
                loop {
                    tokio::select! {
                        maybe_cmd = cmd_rx.recv() => {
                            match maybe_cmd {
                                None => break, // client dropped
                                Some(cmd) => {
                                    if let Err(e) =
                                        writer.write_all(&cmd.bytes).await
                                    {
                                        tracing::warn!(
                                            "harness rpc: write failed: {e}; reconnecting"
                                        );
                                        break;
                                    }
                                    pending.insert(cmd.id, cmd.done);
                                }
                            }
                        }
                        maybe_line = line_rx.recv() => {
                            match maybe_line {
                                None => break, // reader task died
                                Some(line) => match parse_response(&line) {
                                    Ok(Some(resp)) => {
                                        if let Some(tx) = pending.remove(&resp.id) {
                                            let _ = tx.send(Ok(resp));
                                        } else {
                                            tracing::debug!(
                                                "harness rpc: response for unknown id {}",
                                                resp.id
                                            );
                                        }
                                    }
                                    Ok(None) => {
                                        // "id": null — the harness's
                                        // reply to a malformed line we
                                        // never sent; matches nothing.
                                    }
                                    Err(e) => tracing::warn!(
                                        "harness rpc: bad response line: {e} (line: {line:?})"
                                    ),
                                },
                            }
                        }
                    }
                }

                reader_task.abort();
                for (_, tx) in pending.drain() {
                    let _ = tx.send(Err(HarnessError::Disconnected));
                }
                tracing::info!("harness rpc: connection lost; redialing with backoff");
            }
            Err(e) => {
                tracing::debug!("harness rpc: connect failed: {e}; retrying in {backoff:?}");
            }
        }
        connected.store(false, Ordering::Relaxed);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(limits.max_backoff);
    }
}

/* ------------------------------------------------------------------ */
/* Events stream                                                       */
/* ------------------------------------------------------------------ */

/// The harness **events socket** (harness → forge-api JSON lines).
///
/// The harness *listens*; this client connects and redials forever
/// (backoff 250 ms → 5 s — harness restarts are frequent **by
/// design**). Parsed events are delivered over an `mpsc` channel of
/// capacity 64; a consumer slower than [`Limits::event_send_timeout`]
/// gets its event dropped with a warning rather than backpressuring
/// the harness's commit path.
///
/// There is no replay on reconnect: each (re)connect begins with a
/// client-side [`HarnessEvent::ResyncRequired`] marker, after which the
/// harness pushes its `hello`.
pub struct EventStream {
    rx: mpsc::Receiver<HarnessEvent>,
    /// Kept only for observability; dropping the whole [`EventStream`]
    /// closes the mpsc sender and the redial task exits on its own.
    _task: tokio::task::JoinHandle<()>,
}

impl EventStream {
    /// Start connecting to `path` on a background task. The first
    /// delivered event (once a connection lands) is always
    /// [`HarnessEvent::ResyncRequired`].
    ///
    /// Delivery stops when this `EventStream` is dropped.
    pub fn connect(path: impl Into<PathBuf>, limits: Limits) -> Self {
        let path: PathBuf = path.into();
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(events_task(path, limits, tx));
        Self { rx, _task: task }
    }

    /// The delivery channel (one [`HarnessEvent`] per line from the
    /// harness, plus the client-side [`HarnessEvent::ResyncRequired`]
    /// marker on every (re)connect).
    pub fn receiver(&mut self) -> &mut mpsc::Receiver<HarnessEvent> {
        &mut self.rx
    }

    /// Consume the delivery channel.
    pub fn into_receiver(self) -> mpsc::Receiver<HarnessEvent> {
        self.rx
    }
}

async fn events_task(path: PathBuf, limits: Limits, tx: mpsc::Sender<HarnessEvent>) {
    let mut backoff = limits.min_backoff;
    loop {
        if tx.is_closed() {
            return; // consumer dropped the whole stream
        }
        match UnixStream::connect(&path).await {
            Ok(mut stream) => {
                backoff = limits.min_backoff;
                tracing::info!("harness events: connected to {}", path.display());
                // No replay on reconnect: tell the consumer to
                // re-derive state before the first wire event.
                if tx.send(HarnessEvent::ResyncRequired).await.is_err() {
                    return;
                }
                let mut buf = Vec::new();
                'inner: loop {
                    tokio::select! {
                        _ = tx.closed() => return,
                        res = read_line(&mut stream, &mut buf) => {
                            match res {
                                Ok(Some(line)) => {
                                    match parse_event(&line) {
                                        Ok(Some(event)) => {
                                            match tokio::time::timeout(
                                                limits.event_send_timeout,
                                                tx.send(event),
                                            )
                                            .await
                                            {
                                                Ok(Ok(())) => {}
                                                Ok(Err(_)) => return,
                                                Err(_) => tracing::warn!(
                                                    "harness events: consumer slow after {:?}; dropped event (line: {line:?}) — consumer re-derives state via ResyncRequired/status",
                                                    limits.event_send_timeout
                                                ),
                                            }
                                        }
                                        Ok(None) => {
                                            // Unknown event type:
                                            // forward-compatible ignore.
                                        }
                                        Err(e) => tracing::warn!(
                                            "harness events: bad event line: {e} (line: {line:?})"
                                        ),
                                    }
                                }
                                _ => break 'inner, // EOF / read error: redial
                            }
                        }
                    }
                }
                tracing::info!("harness events: connection lost; redialing with backoff");
            }
            Err(e) => {
                tracing::debug!("harness events: connect failed: {e}; retrying in {backoff:?}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(limits.max_backoff);
    }
}

/* ------------------------------------------------------------------ */
/* Facade                                                              */
/* ------------------------------------------------------------------ */

/// One RPC call's `status` result.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessStatus {
    pub version: String,
    pub active_tasks: u64,
    pub conversations: u64,
    pub timers: u64,
}

/// Parameters for `createConversation` (camel-cased on the wire, in
/// 1:1 with `harness/src/ipc.ts`).
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateConversation {
    /// The forge session id this harness conversation represents.
    pub forge_session_id: String,
    /// Model provider (e.g. `openai`, `anthropic`, `faux`).
    pub provider: String,
    /// Model id within the provider.
    pub model_id: String,
    /// Optional agent system prompt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Extra instructions appended after the system prompt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_instructions: Option<String>,
    /// Tool names safe to replay after a checkpoint.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub replay_safe_tools: Vec<String>,
    /// Herd H2.5: the agent's `tools_allowlist` (H1.1) — the harness's
    /// `before_tool` hook blocks any tool call whose name is not in
    /// this list; an empty list allows all tools.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools_allowlist: Vec<String>,
}

impl CreateConversation {
    fn wire_params(&self) -> serde_json::Value {
        serde_json::json!({
            "forgeSessionId": self.forge_session_id,
            "agent": {
                "provider": self.provider,
                "modelId": self.model_id,
                "systemPrompt": self.system_prompt,
            },
            "extraInstructions": self.extra_instructions,
            "replaySafeTools": self.replay_safe_tools,
            "toolsAllowlist": self.tools_allowlist,
        })
    }
}

/// One live (in-flight) compaction task, from the conversation's
/// `pi.live` document (wire shape of `compactionStatus.compactions[]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveCompaction {
    pub task_id: i64,
    pub reason: String,
    pub blocking: bool,
    pub attempt: u64,
}

/// The newest placed `pi.compaction` entry (any segment).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCompaction {
    pub entry_id: i64,
    pub reason: String,
    pub summary_chars: u64,
}

/// `compactionStatus` result (H2.4): live compactions, the newest
/// placed summary, and the ACTIVE context size — the post-compaction/
/// post-reset window (entries before the head marker are excluded).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionStatus {
    pub compactions: Vec<LiveCompaction>,
    pub last_compaction: Option<LastCompaction>,
    pub active_context_chars: u64,
    pub active_entry_count: u64,
}

/// The forge-side handle to one harness process: the RPC client plus
/// the (single) events-stream receiver, or neither in **disabled mode**.
///
/// Disabled mode is entered when `FORGE_HARNESS_SOCKET` is unset or the
/// RPC socket file does not exist at construction (see
/// [`Self::from_env`]): no sockets are dialed and every method fails
/// fast with [`HarnessError::Unavailable`]. forge-api keeps the legacy
/// turn path as the default in that case — zero behavior change.
#[derive(Debug)]
pub struct HarnessClient {
    ipc: Option<Arc<IpcClient>>,
    event_rx: std::sync::Mutex<Option<mpsc::Receiver<HarnessEvent>>>,
}

impl HarnessClient {
    /// A disabled client: no sockets, every call
    /// [`HarnessError::Unavailable`].
    pub fn disabled() -> Self {
        Self {
            ipc: None,
            event_rx: std::sync::Mutex::new(None),
        }
    }

    /// Construct from `FORGE_HARNESS_SOCKET` /
    /// [`FORGE_HARNESS_EVENTS_SOCKET`](default) with production
    /// [`Limits`]. Unset or absent RPC socket ⇒ disabled mode.
    pub fn from_env() -> Self {
        Self::from_env_with(Limits::default())
    }

    /// Same as [`Self::from_env`] with explicit limits (tests).
    pub fn from_env_with(limits: Limits) -> Self {
        let rpc = match std::env::var("FORGE_HARNESS_SOCKET") {
            Ok(p) => PathBuf::from(p),
            Err(_) => {
                tracing::warn!(
                    "harness: FORGE_HARNESS_SOCKET unset; harness mode disabled (legacy turn path is the default)"
                );
                return Self::disabled();
            }
        };
        if !rpc.exists() {
            tracing::warn!(
                socket = %rpc.display(),
                "harness: RPC socket absent; harness mode disabled (legacy turn path is the default)"
            );
            return Self::disabled();
        }
        let events = std::env::var("FORGE_HARNESS_EVENTS_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_events_socket());
        Self::from_paths_with(&rpc, &events, limits)
    }

    /// Enabled client dialing explicit socket paths (skips the
    /// existence gate — the redial loop picks the socket up once it
    /// exists; tests use this to exercise the redial against sockets
    /// that appear later).
    pub fn from_paths_with(rpc: &Path, events: &Path, limits: Limits) -> Self {
        tracing::info!(
            rpc = %rpc.display(),
            events = %events.display(),
            "harness: enabled; connecting to the Node harness"
        );
        Self {
            ipc: Some(Arc::new(IpcClient::new(rpc.to_path_buf(), limits))),
            event_rx: std::sync::Mutex::new(Some(
                EventStream::connect(events.to_path_buf(), limits).into_receiver(),
            )),
        }
    }

    /// Enabled client with default limits.
    pub fn from_paths(rpc: &Path, events: &Path) -> Self {
        Self::from_paths_with(rpc, events, Limits::default())
    }

    /// Whether the harness mode is enabled (socket present at startup).
    pub fn is_enabled(&self) -> bool {
        self.ipc.is_some()
    }

    /// Take the events receiver exactly once (the event consumer is a
    /// single task at forge-api boot). `None` when disabled or already
    /// taken.
    pub fn take_event_rx(&self) -> Option<mpsc::Receiver<HarnessEvent>> {
        self.event_rx.lock().ok()?.take()
    }

    fn ipc(&self) -> Result<&IpcClient, HarnessError> {
        self.ipc.as_deref().ok_or(HarnessError::Unavailable)
    }

    /// `status` — health + bookkeeping.
    pub async fn status(&self) -> Result<HarnessStatus, HarnessError> {
        let v = self.ipc()?.call("status", &serde_json::json!({})).await?;
        serde_json::from_value(v).map_err(|e| HarnessError::Protocol(e.to_string()))
    }

    /// `createConversation` — create a harness conversation carrying
    /// the forge session id in its `forge.meta` document. Returns the
    /// durable conversation id.
    pub async fn create_conversation(
        &self,
        params: &CreateConversation,
    ) -> Result<i64, HarnessError> {
        let v = self
            .ipc()?
            .call("createConversation", &params.wire_params())
            .await?;
        v.get("conversationId")
            .and_then(|c| c.as_i64())
            .ok_or_else(|| {
                HarnessError::Protocol(format!(
                    "createConversation result missing conversationId: {v}"
                ))
            })
    }

    /// `submit` — durably admit one entry. **Exactly-once per
    /// `request_id`** (the harness dedupes via pi-durable
    /// `submissionByRequest`): resubmitting after a
    /// [`HarnessError::Disconnected`] is safe and returns the original
    /// submission id. `entry_draft` is a pi-durable `SubmissionDraft`
    /// (`{"type":"input","content":...}` or `{"type":"write","entry":...}`).
    pub async fn submit(
        &self,
        conversation_id: i64,
        request_id: &str,
        entry_draft: &serde_json::Value,
    ) -> Result<i64, HarnessError> {
        let v = self
            .ipc()?
            .call(
                "submit",
                &serde_json::json!({
                    "conversationId": conversation_id,
                    "requestId": request_id,
                    "entryDraft": entry_draft,
                }),
            )
            .await?;
        v.get("submissionId")
            .and_then(|c| c.as_i64())
            .ok_or_else(|| {
                HarnessError::Protocol(format!("submit result missing submissionId: {v}"))
            })
    }

    /// `importEntries` (Herd H2.6 lazy migration).
    ///
    /// Appends a batch of pi-durable entry drafts to an existing
    /// conversation in ONE commit. An entry draft is an `EntryDraft`
    /// object (a `kind` string plus `model`/`data`/`edits`/`head`;
    /// see `vendor/pi-durable/packages/durable/src/types.ts`), and the
    /// session assigns the ids.
    ///
    /// Returns the number of entries appended. Used by
    /// `harness_migration` to import a legacy `messages` transcript
    /// into a freshly created durable conversation (one round trip,
    /// not one per row).
    pub async fn import_entries(
        &self,
        conversation_id: i64,
        entries: &[serde_json::Value],
    ) -> Result<usize, HarnessError> {
        let v = self
            .ipc()?
            .call(
                "importEntries",
                &serde_json::json!({
                    "conversationId": conversation_id,
                    "entries": entries,
                }),
            )
            .await?;
        v.get("imported")
            .and_then(|n| n.as_u64())
            .map(|n| n as usize)
            .ok_or_else(|| {
                HarnessError::Protocol(format!("importEntries result missing imported: {v}"))
            })
    }

    /// `steer` — submit `text` into the task's live conversation
    /// (`whenBusy: "steer"`).
    pub async fn steer(&self, task_id: i64, text: &str) -> Result<(), HarnessError> {
        self.ipc()?
            .call(
                "steer",
                &serde_json::json!({ "taskId": task_id, "text": text }),
            )
            .await?;
        Ok(())
    }

    /// `abort` — abort the task (and, with `tree`, every task it owns).
    /// Returns the number of tasks aborted.
    pub async fn abort(&self, task_id: i64, tree: bool) -> Result<u64, HarnessError> {
        let v = self
            .ipc()?
            .call(
                "abort",
                &serde_json::json!({ "taskId": task_id, "tree": tree }),
            )
            .await?;
        v.get("aborted")
            .and_then(|a| a.as_u64())
            .ok_or_else(|| HarnessError::Protocol(format!("abort result missing aborted: {v}")))
    }

    /// `documentGet` — read a named conversation document; `Ok(None)`
    /// when absent.
    pub async fn document_get(
        &self,
        conversation_id: i64,
        name: &str,
    ) -> Result<Option<serde_json::Value>, HarnessError> {
        let v = self
            .ipc()?
            .call(
                "documentGet",
                &serde_json::json!({
                    "conversationId": conversation_id,
                    "name": name,
                }),
            )
            .await?;
        Ok(if v.is_null() { None } else { Some(v) })
    }

    /// `documentPut` — create or replace a named conversation document.
    pub async fn document_put(
        &self,
        conversation_id: i64,
        name: &str,
        value: &serde_json::Value,
    ) -> Result<(), HarnessError> {
        self.ipc()?
            .call(
                "documentPut",
                &serde_json::json!({
                    "conversationId": conversation_id,
                    "name": name,
                    "value": value,
                }),
            )
            .await?;
        Ok(())
    }

    /// `timerSet` — schedule a prompt on a conversation. Exactly one of
    /// `at` (absolute epoch ms) or `cron` (5-field expression).
    /// Returns the timer id.
    pub async fn timer_set(
        &self,
        conversation_id: i64,
        at: Option<u64>,
        cron: Option<&str>,
        prompt: &str,
    ) -> Result<String, HarnessError> {
        let mut params = serde_json::json!({ "conversationId": conversation_id, "prompt": prompt });
        if let Some(at) = at {
            params["at"] = serde_json::json!(at);
        }
        if let Some(cron) = cron {
            params["cron"] = serde_json::json!(cron);
        }
        let v = self.ipc()?.call("timerSet", &params).await?;
        v.get("timerId")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| HarnessError::Protocol(format!("timerSet result missing timerId: {v}")))
    }

    /// `timerClear` — clear a timer; `Ok(true)` if it existed.
    pub async fn timer_clear(
        &self,
        conversation_id: i64,
        timer_id: &str,
    ) -> Result<bool, HarnessError> {
        let v = self
            .ipc()?
            .call(
                "timerClear",
                &serde_json::json!({
                    "conversationId": conversation_id,
                    "timerId": timer_id,
                }),
            )
            .await?;
        v.get("cleared").and_then(|c| c.as_bool()).ok_or_else(|| {
            HarnessError::Protocol(format!("timerClear result missing cleared: {v}"))
        })
    }

    /// `compact` (H2.4) — force a manual compaction now. The harness
    /// creates its built-in compaction task (reason `manual`): the
    /// summary is made in the background, the conversation keeps
    /// working, and the summary lands at once when idle or at the next
    /// turn boundary. Returns the task id.
    pub async fn compact(
        &self,
        conversation_id: i64,
        instructions: Option<&str>,
    ) -> Result<i64, HarnessError> {
        let mut params = serde_json::json!({ "conversationId": conversation_id });
        if let Some(i) = instructions {
            params["instructions"] = serde_json::json!(i);
        }
        let v = self.ipc()?.call("compact", &params).await?;
        v.get("taskId")
            .and_then(|t| t.as_i64())
            .ok_or_else(|| HarnessError::Protocol(format!("compact result missing taskId: {v}")))
    }

    /// `reset` (H2.4) — start a new context segment from a handoff
    /// note: the model no longer sees older entries, but they stay in
    /// storage (searchable in the harness's `durable_entries`).
    pub async fn reset(
        &self,
        conversation_id: i64,
        handoff_note: Option<&str>,
    ) -> Result<(), HarnessError> {
        let mut params = serde_json::json!({ "conversationId": conversation_id });
        if let Some(n) = handoff_note {
            params["handoffNote"] = serde_json::json!(n);
        }
        self.ipc()?.call("reset", &params).await?;
        Ok(())
    }

    /// `compactionStatus` (H2.4) — live compactions, the newest placed
    /// summary, and the active (post-compaction/reset) context size.
    pub async fn compaction_status(
        &self,
        conversation_id: i64,
    ) -> Result<CompactionStatus, HarnessError> {
        let v = self
            .ipc()?
            .call(
                "compactionStatus",
                &serde_json::json!({ "conversationId": conversation_id }),
            )
            .await?;
        serde_json::from_value(v).map_err(|e| HarnessError::Protocol(e.to_string()))
    }

    /// `timerList` — all live timers, optionally scoped to one
    /// conversation. A timer is "live" while it is not deleted and
    /// either un-fired (one-shots) or still recurring (cron); a
    /// fired one-shot no longer lists.
    pub async fn timer_list(
        &self,
        conversation_id: Option<i64>,
    ) -> Result<Vec<TimerInfo>, HarnessError> {
        let mut params = serde_json::json!({});
        if let Some(cid) = conversation_id {
            params["conversationId"] = serde_json::json!(cid);
        }
        let v = self.ipc()?.call("timerList", &params).await?;
        let arr = v.get("timers").and_then(|t| t.as_array()).ok_or_else(|| {
            HarnessError::Protocol(format!("timerList result missing timers: {v}"))
        })?;
        arr.iter()
            .map(|row| {
                Ok(TimerInfo {
                    timer_id: row
                        .get("timerId")
                        .and_then(|s| s.as_str())
                        .ok_or_else(|| {
                            HarnessError::Protocol(format!("timer row missing timerId: {row}"))
                        })?
                        .to_string(),
                    conversation_id: row
                        .get("conversationId")
                        .and_then(|n| n.as_i64())
                        .ok_or_else(|| {
                            HarnessError::Protocol(format!(
                                "timer row missing conversationId: {row}"
                            ))
                        })?,
                    at_ms: row.get("at").and_then(|n| n.as_u64()),
                    cron: row
                        .get("cron")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    prompt: row
                        .get("prompt")
                        .and_then(|s| s.as_str())
                        .ok_or_else(|| {
                            HarnessError::Protocol(format!("timer row missing prompt: {row}"))
                        })?
                        .to_string(),
                    created_at_ms: row.get("createdAt").and_then(|n| n.as_u64()),
                    fired_at_ms: row.get("firedAt").and_then(|n| n.as_u64()),
                })
            })
            .collect()
    }
}

/// One live harness timer (Herd H2.3). Wire shape mirrors
/// `harness/src/timer-store.ts` `TimerRow` (camelCase, epoch ms).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerInfo {
    pub timer_id: String,
    pub conversation_id: i64,
    /// Next scheduled fire, epoch ms.
    pub at_ms: Option<u64>,
    /// 5-field cron expression (UTC) for recurring timers.
    pub cron: Option<String>,
    pub prompt: String,
    pub created_at_ms: Option<u64>,
    /// Set once fired (a one-shot stays set; a cron records the last fire).
    pub fired_at_ms: Option<u64>,
}

/* ------------------------------------------------------------------ */
/* Default socket paths                                                */
/* ------------------------------------------------------------------ */

/// `~/.local/state/forge` (the harness's default state dir, per
/// `harness/README.md`); `$HOME`-relative with a cwd fallback when
/// `HOME` is unset.
fn default_state_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(|h| Path::new(&h).join(".local/state/forge"))
        .unwrap_or_else(|| PathBuf::from(".local/state/forge"))
}

/// Default RPC socket path (`FORGE_HARNESS_SOCKET`).
pub fn default_rpc_socket() -> PathBuf {
    default_state_dir().join("harness.sock")
}

/// Default events socket path (`FORGE_HARNESS_EVENTS_SOCKET`).
pub fn default_events_socket() -> PathBuf {
    default_state_dir().join("harness-events.sock")
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_limits() -> Limits {
        Limits {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            disconnect_wait: Duration::from_millis(300),
            response_timeout: Duration::from_millis(2_000),
            event_send_timeout: Duration::from_millis(500),
        }
    }

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    // --- codec round-trips -------------------------------------------

    #[test]
    fn encode_request_shape() {
        let params = serde_json::json!({ "conversationId": 7, "requestId": "abc" });
        let line = encode_request(42, "submit", &params);
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["id"], 42);
        assert_eq!(v["method"], "submit");
        assert_eq!(v["params"]["conversationId"], 7);
        assert_eq!(v["params"]["requestId"], "abc");
    }

    #[test]
    fn parse_response_result() {
        let resp = parse_response(r#"{"id": 7, "result": {"submissionId": 3}}"#).unwrap();
        let resp = resp.expect("expected a response");
        assert_eq!(resp.id, 7);
        match resp.result {
            Ok(v) => assert_eq!(v["submissionId"], 3),
            Err(e) => panic!("expected result, got error: {e}"),
        }
    }

    #[test]
    fn parse_response_null_result() {
        let resp = parse_response(r#"{"id": 1, "result": null}"#).unwrap();
        let resp = resp.unwrap();
        assert!(matches!(resp.result, Ok(serde_json::Value::Null)));
    }

    #[test]
    fn parse_response_error() {
        let resp = parse_response(
            r#"{"id": 2, "error": {"code": "unknown_conversation", "message": "nope"}}"#,
        )
        .unwrap();
        let resp = resp.unwrap();
        assert_eq!(
            resp.result,
            Err(HarnessError::Rpc {
                code: "unknown_conversation".into(),
                message: "nope".into(),
            })
        );
    }

    #[test]
    fn parse_response_null_id_is_ignored() {
        // The harness's reply to a malformed line: {"id": null, "error": ...}
        let resp =
            parse_response(r#"{"id": null, "error": {"code": "bad_request", "message": "x"}}"#)
                .unwrap();
        assert!(resp.is_none());
    }

    #[test]
    fn parse_response_both_fields_is_protocol_error() {
        let err =
            parse_response(r#"{"id": 1, "result": {}, "error": {"code": "c", "message": "m"}}"#)
                .unwrap_err();
        assert!(matches!(err, HarnessError::Protocol(_)));
    }

    #[test]
    fn parse_event_all_variants() {
        let e = parse_event(r#"{"type":"hello","version":"0.1.0"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            e,
            HarnessEvent::Hello {
                version: "0.1.0".into()
            }
        );

        let e = parse_event(
            r#"{"type":"task_state","taskId":5,"conversationId":9,"status":"done","outcomeStatus":"completed"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            e,
            HarnessEvent::TaskState {
                task_id: 5,
                conversation_id: 9,
                status: super::TaskState::Done,
                outcome_status: Some("completed".into()),
            }
        );

        let e = parse_event(
            r#"{"type":"task_state","taskId":1,"conversationId":2,"status":"started"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            e,
            HarnessEvent::TaskState {
                task_id: 1,
                conversation_id: 2,
                status: super::TaskState::Started,
                outcome_status: None,
            }
        );

        let e = parse_event(r#"{"type":"turn_end","conversationId":3,"entryId":4,"summary":"hi"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            e,
            HarnessEvent::TurnEnd {
                conversation_id: 3,
                entry_id: 4,
                summary: "hi".into(),
            }
        );

        let e =
            parse_event(r#"{"type":"document_changed","conversationId":5,"name":"forge.meta"}"#)
                .unwrap()
                .unwrap();
        assert_eq!(
            e,
            HarnessEvent::DocumentChanged {
                conversation_id: 5,
                name: "forge.meta".into(),
            }
        );

        let e = parse_event(
            r#"{"type":"timer_fired","timerId":"t-1","conversationId":6,"prompt":"do it"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            e,
            HarnessEvent::TimerFired {
                timer_id: "t-1".into(),
                conversation_id: 6,
                prompt: "do it".into(),
            }
        );

        // Forward compatibility: unknown types are ignored, not errors.
        assert!(parse_event(r#"{"type":"some_future_event","x":1}"#)
            .unwrap()
            .is_none());
    }

    #[test]
    fn parse_event_bad_lines() {
        assert!(matches!(
            parse_event("not json").unwrap_err(),
            HarnessError::Protocol(_)
        ));
        assert!(matches!(
            parse_event(
                r#"{"type":"task_state","taskId":"nope","conversationId":1,"status":"started"}"#
            )
            .unwrap_err(),
            HarnessError::Protocol(_)
        ));
        assert!(matches!(
            parse_event(r#"{"type":"task_state","taskId":1,"conversationId":2,"status":"bogus"}"#)
                .unwrap_err(),
            HarnessError::Protocol(_)
        ));
    }

    #[test]
    fn create_conversation_wire_shape_includes_allowlist() {
        let cc = CreateConversation {
            forge_session_id: "s-1".into(),
            provider: "p".into(),
            model_id: "m".into(),
            system_prompt: Some("you are".into()),
            extra_instructions: None,
            replay_safe_tools: vec!["read".into()],
            tools_allowlist: vec!["bash".into(), "read".into()],
        };
        let params = cc.wire_params();
        assert_eq!(params["forgeSessionId"], "s-1");
        assert_eq!(params["agent"]["provider"], "p");
        assert_eq!(params["agent"]["modelId"], "m");
        assert_eq!(params["agent"]["systemPrompt"], "you are");
        assert_eq!(params["replaySafeTools"][0], "read");
        assert_eq!(params["toolsAllowlist"][0], "bash");
        assert_eq!(params["toolsAllowlist"][1], "read");
        // Absent optionals stay off the wire.
        let cc2 = CreateConversation {
            forge_session_id: "s-2".into(),
            provider: "p".into(),
            model_id: "m".into(),
            ..Default::default()
        };
        let p2 = cc2.wire_params();
        // Nulls stay on the wire (the harness treats null as absent);
        // the empty allowlist serializes as an empty array.
        assert!(p2["extraInstructions"].is_null());
        assert!(p2["toolsAllowlist"].as_array().map(|a| a.is_empty()) == Some(true));
    }

    #[test]
    fn compaction_status_wire_shape() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"compactions":[{"taskId":9,"reason":"threshold","blocking":false,"attempt":1}],"lastCompaction":{"entryId":14,"reason":"manual","summaryChars":1234},"activeContextChars":5678,"activeEntryCount":12}"#,
        )
        .unwrap();
        let status: CompactionStatus = serde_json::from_value(v).unwrap();
        assert_eq!(status.compactions.len(), 1);
        assert_eq!(status.compactions[0].task_id, 9);
        assert_eq!(status.compactions[0].reason, "threshold");
        assert!(!status.compactions[0].blocking);
        assert_eq!(status.compactions[0].attempt, 1);
        let last = status.last_compaction.unwrap();
        assert_eq!(last.entry_id, 14);
        assert_eq!(last.reason, "manual");
        assert_eq!(last.summary_chars, 1234);
        assert_eq!(status.active_context_chars, 5678);
        assert_eq!(status.active_entry_count, 12);
        // Absent lastCompaction decodes to None.
        let v2: serde_json::Value =
            serde_json::from_str(r#"{"compactions":[],"lastCompaction":null,"activeContextChars":1,"activeEntryCount":1}"#)
                .unwrap();
        let s2: CompactionStatus = serde_json::from_value(v2).unwrap();
        assert!(s2.last_compaction.is_none());
        assert!(s2.compactions.is_empty());
    }

    // --- disabled mode ------------------------------------------------

    #[test]
    fn disabled_client_fails_fast_on_every_method() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = HarnessClient::disabled();
        assert!(!client.is_enabled());
        assert!(client.take_event_rx().is_none());
        rt.block_on(async {
            assert_eq!(client.status().await, Err(HarnessError::Unavailable));
            let cc = CreateConversation {
                forge_session_id: "s".into(),
                provider: "p".into(),
                model_id: "m".into(),
                ..Default::default()
            };
            assert_eq!(
                client.create_conversation(&cc).await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client
                    .submit(1, "r", &serde_json::json!({ "type": "input" }))
                    .await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(client.steer(1, "x").await, Err(HarnessError::Unavailable));
            assert_eq!(client.abort(1, true).await, Err(HarnessError::Unavailable));
            assert_eq!(
                client.document_get(1, "n").await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client.document_put(1, "n", &serde_json::json!(null)).await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client.timer_set(1, None, Some("0 0 * * *"), "p").await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client.timer_clear(1, "t").await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client.compact(1, None).await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client.compact(1, Some("focus")).await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(client.reset(1, None).await, Err(HarnessError::Unavailable));
            assert_eq!(
                client.reset(1, Some("note")).await,
                Err(HarnessError::Unavailable)
            );
            assert_eq!(
                client.compaction_status(1).await,
                Err(HarnessError::Unavailable)
            );
        });
    }

    #[test]
    fn from_env_absent_socket_is_disabled() {
        // Point FORGE_HARNESS_SOCKET at a path that cannot exist.
        std::env::set_var("FORGE_HARNESS_SOCKET", "/definitely/not/a/socket/path");
        let client = HarnessClient::from_env();
        std::env::remove_var("FORGE_HARNESS_SOCKET");
        assert!(!client.is_enabled());
        assert!(client.take_event_rx().is_none());
    }

    // --- RPC: round-trip, disconnect, redial -------------------------

    /// A fake RPC server: accepts every connection, answers the first
    /// line of each with a canned result (or `error`), then stays open.
    async fn start_fake_rpc(path: &Path, reply: &str) -> tokio::task::JoinHandle<()> {
        let path = path.to_path_buf();
        let reply = reply.to_string();
        tokio::spawn(async move {
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            while let Ok((mut sock, _)) = listener.accept().await {
                let reply = reply.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    if read_line(&mut sock, &mut buf)
                        .await
                        .is_ok_and(|l| l.is_some())
                    {
                        let _ = sock.write_all(format!("{reply}\n").as_bytes()).await;
                    }
                });
            }
        })
    }

    #[tokio::test]
    async fn rpc_roundtrip_result_and_error() {
        let dir = temp_dir();
        let path = dir.path().join("harness.sock");
        let server = start_fake_rpc(
            &path,
            r#"{"id": 1, "result": {"version": "0.1.0", "activeTasks": 0, "conversations": 0, "timers": 0}}"#,
        )
        .await;

        let client = IpcClient::new(path.clone(), test_limits());
        // Wait for the connection to land.
        for _ in 0..100 {
            if client.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let v = client.call("status", &serde_json::json!({})).await.unwrap();
        let status: HarnessStatus = serde_json::from_value(v).unwrap();
        assert_eq!(status.version, "0.1.0");

        // Unknown-method style error reply: use the same server shape but
        // a different canned reply via a second socket.
        let err_path = dir.path().join("err.sock");
        let server2 = start_fake_rpc(
            &err_path,
            r#"{"id": 1, "error": {"code": "unknown_task", "message": "task 9 does not exist"}}"#,
        )
        .await;
        let client2 = IpcClient::new(err_path.clone(), test_limits());
        for _ in 0..100 {
            if client2.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let err = client2
            .call("abort", &serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            HarnessError::Rpc {
                code: "unknown_task".into(),
                message: "task 9 does not exist".into(),
            }
        );
        drop(server2);
        drop(server);
    }

    #[tokio::test]
    async fn rpc_in_flight_request_dies_when_server_dies() {
        let dir = temp_dir();
        let path = dir.path().join("harness.sock");

        // Server that accepts, reads one line, then drops the socket and
        // deletes the file: simulates the harness dying mid-request.
        let listener_task = tokio::spawn({
            let path = path.clone();
            async move {
                let listener = tokio::net::UnixListener::bind(&path).unwrap();
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let _ = read_line(&mut sock, &mut buf).await;
                // No reply; kill the connection and the socket.
                drop(sock);
                drop(listener);
                let _ = std::fs::remove_file(&path);
            }
        });

        let client = IpcClient::new(path.clone(), test_limits());
        for _ in 0..100 {
            if client.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // In-flight: dies with the connection, before the response bound.
        let err = client
            .call("status", &serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err, HarnessError::Disconnected);
        assert!(!client.is_connected());

        // While down: bounded wait, then Disconnected.
        let start = std::time::Instant::now();
        let err = client
            .call("status", &serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err, HarnessError::Disconnected);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= test_limits().disconnect_wait,
            "expected to wait for disconnect_wait, waited {elapsed:?}"
        );
        listener_task.await.unwrap();
    }

    #[tokio::test]
    async fn rpc_redials_when_server_comes_back() {
        let dir = temp_dir();
        let path = dir.path().join("harness.sock");

        // Phase 1: a server that answers one call then dies + unlinks.
        let first = tokio::spawn({
            let path = path.clone();
            async move {
                let listener = tokio::net::UnixListener::bind(&path).unwrap();
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let _ = read_line(&mut sock, &mut buf).await;
                let _ = sock
                    .write_all(
                        br#"{"id": 1, "result": {"version": "a", "activeTasks": 0, "conversations": 0, "timers": 0}}
"#,
                    )
                    .await;
                drop(sock);
                drop(listener);
                let _ = std::fs::remove_file(&path);
            }
        });

        let client = IpcClient::new(path.clone(), test_limits());
        for _ in 0..100 {
            if client.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(client.is_connected());
        assert!(client.call("status", &serde_json::json!({})).await.is_ok());

        // Let phase 1 die, then restart at the same path.
        first.await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await; // let the redial loop notice
        let second = start_fake_rpc(&path, r#"{"id": 2, "result": {"version": "b", "activeTasks": 1, "conversations": 1, "timers": 1}}"#).await;
        for _ in 0..100 {
            if client.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let v = client.call("status", &serde_json::json!({})).await.unwrap();
        let status: HarnessStatus = serde_json::from_value(v).unwrap();
        assert_eq!(status.version, "b");
        drop(second);
    }

    // --- events: delivery, resync on (re)connect, redial -------------

    #[tokio::test]
    async fn events_stream_delivers_and_resyncs_on_reconnect() {
        let dir = temp_dir();
        let path = dir.path().join("harness-events.sock");

        // Phase 1: accept, push hello + a task_state + a timer_fired,
        // then close.
        let phase1 = tokio::spawn({
            let path = path.clone();
            async move {
                let listener = tokio::net::UnixListener::bind(&path).unwrap();
                let (mut sock, _) = listener.accept().await.unwrap();
                let payload = format!(
                    "{}\n{}\n{}\n",
                    r#"{"type":"hello","version":"0.1.0"}"#,
                    r#"{"type":"task_state","taskId":5,"conversationId":9,"status":"started"}"#,
                    r#"{"type":"timer_fired","timerId":"t-1","conversationId":9,"prompt":"go"}"#
                );
                sock.write_all(payload.as_bytes()).await.unwrap();
                drop(sock);
                drop(listener);
                let _ = std::fs::remove_file(&path);
            }
        });

        let stream = EventStream::connect(path.clone(), test_limits());
        let mut rx = stream.into_receiver();

        // (Re)connect 1: ResyncRequired marker, then the wire events.
        assert_eq!(rx.recv().await.unwrap(), HarnessEvent::ResyncRequired);
        assert_eq!(
            rx.recv().await.unwrap(),
            HarnessEvent::Hello {
                version: "0.1.0".into()
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            HarnessEvent::TaskState {
                task_id: 5,
                conversation_id: 9,
                status: super::TaskState::Started,
                outcome_status: None,
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            HarnessEvent::TimerFired {
                timer_id: "t-1".into(),
                conversation_id: 9,
                prompt: "go".into(),
            }
        );

        // Phase 2: restart the server at the same path; the client must
        // redial and emit a SECOND ResyncRequired.
        phase1.await.unwrap();
        let phase2 = start_fake_events(&path).await;
        assert_eq!(rx.recv().await.unwrap(), HarnessEvent::ResyncRequired);
        assert_eq!(
            rx.recv().await.unwrap(),
            HarnessEvent::Hello {
                version: "0.2.0".into()
            }
        );
        drop(phase2);
    }

    /// A fake events server: on connect, pushes one `hello` and stays
    /// open.
    async fn start_fake_events(path: &Path) -> tokio::task::JoinHandle<()> {
        let path = path.to_path_buf();
        tokio::spawn(async move {
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            while let Ok((mut sock, _)) = listener.accept().await {
                let _ = sock
                    .write_all(format!("{}\n", r#"{"type":"hello","version":"0.2.0"}"#).as_bytes())
                    .await;
                // Hold the socket open until the client
                // disconnects (EOF on read) or the task is
                // dropped with the test runtime.
                let mut buf = [0u8; 64];
                let _ = sock.read(&mut buf).await;
            }
        })
    }

    #[tokio::test]
    async fn events_stream_unknown_types_are_ignored() {
        let dir = temp_dir();
        let path = dir.path().join("harness-events.sock");
        let phase1 = tokio::spawn({
            let path = path.clone();
            async move {
                let listener = tokio::net::UnixListener::bind(&path).unwrap();
                let (mut sock, _) = listener.accept().await.unwrap();
                let payload = format!(
                    "{}\n{}\n{}\n",
                    r#"{"type":"hello","version":"0.1.0"}"#,
                    r#"{"type":"some_future_event","data":1}"#,
                    r#"{"type":"turn_end","conversationId":3,"entryId":4,"summary":"done"}"#
                );
                sock.write_all(payload.as_bytes()).await.unwrap();
                drop(sock);
            }
        });

        let mut rx = EventStream::connect(path, test_limits()).into_receiver();
        assert_eq!(rx.recv().await.unwrap(), HarnessEvent::ResyncRequired);
        assert_eq!(
            rx.recv().await.unwrap(),
            HarnessEvent::Hello {
                version: "0.1.0".into()
            }
        );
        // The unknown event is skipped; the real one still arrives.
        assert_eq!(
            rx.recv().await.unwrap(),
            HarnessEvent::TurnEnd {
                conversation_id: 3,
                entry_id: 4,
                summary: "done".into(),
            }
        );
        phase1.await.unwrap();
    }
}
