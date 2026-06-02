//! Tool recording - the single chokepoint that persists tool-related
//! events to the `messages` table.
//!
//! ## Design
//!
//! Both halves of the audit trail go through the same trait so the
//! schema, sequence-numbering, and jsonb-vs-text decisions live in one
//! place:
//!
//! - **Call row** (`role = 'assistant'`, with `tool_name`,
//!   `tool_input`, `tool_call_id`). Written by the agent harness the
//!   instant it sees the model emit a `toolcall_end` event. This is the
//!   "intent" record - the executor may never get to run, and that's
//!   still useful audit data.
//!
//! - **Result row** (`role = 'tool'`, with `tool_name`, `tool_call_id`,
//!   `content` (flattened for human readers), `tool_output` jsonb
//!   (full structured result), `duration_ms`). Written by the tool
//!   executor after the tool returns or errors. This is the "outcome"
//!   record and lives closest to the code that actually ran the
//!   command, so it has the most accurate information (exit code,
//!   byte counts, timing, stdout/stderr split).
//!
//! The two rows share a `tool_call_id`, so consumers of the message
//! log can reconstruct the full trace by joining on it.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::Message;

/// Record describing the model's intent to invoke a tool.
#[derive(Debug, Clone)]
pub struct ToolCallRecord {
    pub session_id: Uuid,
    pub tool_call_id: String,
    pub tool_name: String,
    /// The model's arguments for the tool, as emitted by the agent
    /// runtime. Typically the raw `arguments` field of the
    /// `toolCall` payload.
    pub input: serde_json::Value,
}

/// Record describing the outcome of a tool execution.
#[derive(Debug, Clone)]
pub struct ToolResultRecord {
    pub session_id: Uuid,
    pub tool_call_id: String,
    pub tool_name: String,
    /// Human-readable, flattened summary. Goes into the `content`
    /// column so SQL clients can read it without a jsonb viewer.
    pub content: String,
    /// Full structured output. Goes into the `tool_output` jsonb
    /// column. For bash, this is `{stdout, stderr, exit_code}`; for
    /// read, `{lines, bytes}`; for write/edit, the diff or the
    /// bytes-written count. We keep both the human text and the
    /// structured blob so the two audiences don't fight over schema.
    pub output: serde_json::Value,
    pub is_error: bool,
    pub duration_ms: Option<u64>,
}

/// Persists tool-related events to durable storage.
///
/// Implemented today by [`DbToolRecorder`], which writes rows to the
/// `messages` table. Future implementations could target a different
/// store (e.g. a separate `tool_invocations` table, an external
/// observability backend) without touching the call sites in the
/// harness or the executor.
#[async_trait]
pub trait ToolRecorder: Send + Sync {
    /// Record that the model decided to invoke a tool. The matching
    /// [`ToolRecorder::record_result`] is called when the executor
    /// finishes (or errors). If the executor never runs, the call row
    /// stands on its own as a record of attempted intent.
    ///
    /// Returns the row that was inserted. Callers publish the row to
    /// the [`crate::bus::MessageBus`] so SSE consumers see the new
    /// row in real time.
    async fn record_call(&self, record: ToolCallRecord) -> Result<Message, sqlx::Error>;

    /// Record the outcome of a tool execution. The `tool_call_id`
    /// should match a previous [`ToolRecorder::record_call`] so the
    /// two rows can be linked.
    ///
    /// Returns the row that was inserted. Callers publish the row to
    /// the bus.
    async fn record_result(&self, record: ToolResultRecord) -> Result<Message, sqlx::Error>;
}

/// Postgres-backed implementation that writes to the `messages` table.
pub struct DbToolRecorder {
    pool: PgPool,
}

impl DbToolRecorder {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ToolRecorder for DbToolRecorder {
    async fn record_call(&self, record: ToolCallRecord) -> Result<Message, sqlx::Error> {
        let args_json =
            serde_json::to_string(&record.input).unwrap_or_else(|_| "null".to_string());
        // We use a `[tool_call:<name>]` marker in `content` so the
        // model's text-delta rows (also `role = 'assistant'`) can be
        // told apart from intent records at a glance.
        //
        // RETURNING * so the caller can publish the row to the
        // [`crate::bus::MessageBus`].
        sqlx::query_as::<_, Message>(
            r#"INSERT INTO messages
                 (session_id, sequence, role, content, tool_name, tool_input, tool_call_id)
               VALUES ($1, get_next_sequence($1), 'assistant', $2, $3, $4::jsonb, $5)
               RETURNING *"#,
        )
        .bind(record.session_id)
        .bind(format!("[tool_call:{}]", record.tool_name))
        .bind(&record.tool_name)
        .bind(&args_json)
        .bind(&record.tool_call_id)
        .fetch_one(&self.pool)
        .await
    }

    async fn record_result(&self, record: ToolResultRecord) -> Result<Message, sqlx::Error> {
        let output_json =
            serde_json::to_string(&record.output).unwrap_or_else(|_| "null".to_string());
        sqlx::query_as::<_, Message>(
            r#"INSERT INTO messages
                 (session_id, sequence, role, content, tool_name, tool_call_id, tool_output, duration_ms)
               VALUES ($1, get_next_sequence($1), 'tool', $2, $3, $4, $5::jsonb, $6)
               RETURNING *"#,
        )
        .bind(record.session_id)
        .bind(&record.content)
        .bind(&record.tool_name)
        .bind(&record.tool_call_id)
        .bind(&output_json)
        .bind(record.duration_ms.map(|d| d as i64))
        .fetch_one(&self.pool)
        .await
    }
}

/// Pull a flat string out of an `AgentToolResult`-shaped JSON value.
///
/// pi hands results back as `{content: [{type, text}, ...], is_error}`.
/// The `content` column on `messages` is plain `TEXT`, so we flatten
/// the array of text blocks. If the input isn't in the expected shape
/// (or `content` is missing/empty), we fall back to the raw JSON so we
/// never silently drop information.
pub fn flatten_tool_result(value: &serde_json::Value) -> String {
    let Some(arr) = value.get("content").and_then(|v| v.as_array()) else {
        return value.to_string();
    };
    let mut out = String::new();
    for (i, item) in arr.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
            out.push_str(text);
        } else {
            out.push_str(&item.to_string());
        }
    }
    if out.is_empty() {
        value.to_string()
    } else {
        out
    }
}
