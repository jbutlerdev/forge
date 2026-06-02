# Tool audit log

The `messages` table is the single source of truth for what the model did and what happened. This document explains how to read it, the per-tool `tool_output` shapes, and the SQL recipes you'll reach for most often.

## Schema reminder

| Column | Type | Used for |
|---|---|---|
| `id` | uuid PK | row identity |
| `session_id` | uuid FK | which conversation |
| `sequence` | int | per-session monotonic via `get_next_sequence(session_id)`, **UNIQUE** with `session_id` |
| `role` | text | `user` / `assistant` / `tool` / `system` |
| `content` | text | human-readable text (or `[tool_call:<name>]` for assistant tool-call rows) |
| `tool_name` | text | the tool's name, for `assistant` and `tool` rows |
| `tool_input` | jsonb | the model's arguments, for `assistant` rows with `tool_call_id` |
| `tool_call_id` | text | pi's id for this call — the **join key** between call and result rows |
| `tool_output` | jsonb | structured result, for `tool` rows |
| `duration_ms` | bigint | wall-clock duration, for `tool` rows |
| `created_at` | timestamptz | default `now()` |

## Row shapes

### User row

```sql
INSERT INTO messages (session_id, sequence, role, content)
VALUES ($session_id, get_next_sequence($session_id), 'user', $content);
```

`content` is the prompt as the user typed it. No `tool_*` fields are populated.

### Assistant text row

```sql
INSERT INTO messages (session_id, sequence, role, content)
VALUES ($session_id, get_next_sequence($session_id), 'assistant', $text);
```

`content` is the assistant's final reply for this turn (after all tool results have been folded in).

### Assistant tool-call row (the *call*)

The harness writes this when it sees `PiEvent::ToolCallEnd`:

```sql
INSERT INTO messages
  (session_id, sequence, role, content, tool_name, tool_input, tool_call_id)
VALUES
  ($session_id, get_next_sequence($session_id), 'assistant',
   '[tool_call:' || $tool_name || ']', $tool_name, $tool_input, $tool_call_id);
```

`tool_input` is the model's arguments as a parsed JSON object (e.g. `{"command": "date", "timeout_ms": 30000}`). `content` is just the marker `[tool_call:<name>]` so a reader can tell at a glance that this row represents an intent to call a tool.

### Tool result row (the *result*)

The executor writes this when the tool finishes:

```sql
INSERT INTO messages
  (session_id, sequence, role, content, tool_name, tool_call_id, tool_output, duration_ms)
VALUES
  ($session_id, get_next_sequence($session_id), 'tool',
   $content, $tool_name, $tool_call_id, $tool_output, $duration_ms);
```

`content` is the flattened text payload (what a human would see if they only had the `content` column). `tool_output` is the structured jsonb — what a programmatic consumer would use. The shape depends on the tool.

## Per-tool `tool_output` shapes

### `bash` (streaming path)

```json
{
  "success": true,
  "stdout": null,
  "stderr": null,
  "exit_code": 0,
  "timed_out": false,
  "streamed": true
}
```

`stdout` and `stderr` are **always null** for streaming bash. The bytes were emitted to the SSE consumer as `stdout` / `stderr` events, not captured. The `streamed: true` flag tells the reader that the bytes went somewhere other than the audit log. To see them, check the SSE consumer's own log.

If the model writes to a file and `read`s it back, the file contents end up in the `read` row's `tool_output.output`, not here.

### `bash` (non-streaming path)

```json
{
  "success": true,
  "stdout": "Wed Jun  1 17:32:46 UTC 2026\n",
  "stderr": "",
  "exit_code": 0,
  "timed_out": false,
  "streamed": false
}
```

Same shape, but `stdout` and `stderr` are populated. (`streamed: false` is the marker that this is the captured form.)

### `read`

Success:
```json
{
  "success": true,
  "output": "Wed Jun  1 17:32:46 UTC 2026\n",
  "error": null
}
```

Failure (file missing, permission denied, etc.):
```json
{
  "success": false,
  "output": "",
  "error": "Tool execution failed: Failed to open file: No such file or directory (os error 2)"
}
```

### `write`

Success:
```json
{
  "success": true,
  "output": "Successfully wrote 18 bytes to /tmp/example.txt",
  "error": null
}
```

Failure:
```json
{
  "success": false,
  "output": "",
  "error": "Tool execution failed: <reason>"
}
```

### `edit`

Success:
```json
{
  "success": true,
  "output": "Edit applied successfully",
  "error": null
}
```

Failure (`old_text` not found, multiple matches, etc.):
```json
{
  "success": false,
  "output": "",
  "error": "Tool execution failed: old_text not found in <path>"
}
```

## SQL recipes

### Reconstruct one session as a timeline

```sql
SELECT
  sequence,
  role,
  COALESCE(tool_name, '') AS tool,
  COALESCE(LEFT(tool_call_id, 24), '') AS call_id,
  COALESCE(duration_ms::text, '') AS dur_ms,
  CASE
    WHEN role = 'tool' THEN LEFT(content, 80)
    WHEN role = 'assistant' AND tool_call_id IS NOT NULL THEN '[tool_call:' || tool_name || ']'
    ELSE LEFT(content, 80)
  END AS preview
FROM messages
WHERE session_id = '<session-uuid>'
ORDER BY sequence;
```

### Pair call → result by `tool_call_id`

```sql
SELECT
  c.sequence AS call_seq,
  c.tool_name,
  c.tool_call_id,
  c.tool_input,
  r.sequence AS result_seq,
  r.duration_ms,
  r.tool_output,
  r.content AS result_text
FROM messages c
LEFT JOIN messages r
  ON r.session_id = c.session_id
  AND r.role = 'tool'
  AND r.tool_call_id = c.tool_call_id
WHERE c.session_id = '<session-uuid>'
  AND c.role = 'assistant'
  AND c.tool_call_id IS NOT NULL
ORDER BY c.sequence;
```

A `NULL` `result_seq` means the call's result never made it to the audit log — either the executor crashed, the recorder's transaction was rolled back (e.g. the unique-constraint race that the advisory lock prevents), or the extension failed to invoke `/tools/execute` for this id. Treat missing result rows as a bug.

### Find failed tool calls

```sql
SELECT
  c.sequence,
  c.tool_name,
  c.tool_input,
  r.tool_output->>'error' AS error,
  r.duration_ms
FROM messages c
JOIN messages r
  ON r.session_id = c.session_id
  AND r.role = 'tool'
  AND r.tool_call_id = c.tool_call_id
WHERE c.session_id = '<session-uuid>'
  AND c.role = 'assistant'
  AND c.tool_call_id IS NOT NULL
  AND r.tool_output->>'success' = 'false'
ORDER BY c.sequence;
```

### Aggregate per-tool stats over many sessions

```sql
SELECT
  tool_name,
  COUNT(*) AS calls,
  ROUND(AVG(duration_ms), 1) AS avg_ms,
  MIN(duration_ms) AS min_ms,
  MAX(duration_ms) AS max_ms,
  SUM(CASE WHEN tool_output->>'success' = 'false' THEN 1 ELSE 0 END) AS failures
FROM messages
WHERE role = 'tool'
  AND created_at > NOW() - INTERVAL '1 day'
GROUP BY tool_name
ORDER BY calls DESC;
```

### Pairing integrity check (every call has a result, no orphan results)

```sql
WITH calls AS (
  SELECT tool_call_id FROM messages
  WHERE session_id = '<session-uuid>' AND role = 'assistant' AND tool_call_id IS NOT NULL
),
results AS (
  SELECT tool_call_id FROM messages
  WHERE session_id = '<session-uuid>' AND role = 'tool'
)
SELECT
  (SELECT COUNT(*) FROM calls) AS calls_total,
  (SELECT COUNT(*) FROM results) AS results_total,
  (SELECT COUNT(*) FROM calls c WHERE NOT EXISTS (SELECT 1 FROM results r WHERE r.tool_call_id = c.tool_call_id)) AS orphaned_calls,
  (SELECT COUNT(*) FROM results r WHERE NOT EXISTS (SELECT 1 FROM calls c WHERE c.tool_call_id = r.tool_call_id)) AS orphan_results;
```

You want `orphaned_calls = 0` and `orphan_results = 0`. Non-zero values indicate a real bug; the advisory-lock fix on `get_next_sequence` is the most common cause of historical orphan rows.

### Sequence integrity (no gaps, no duplicates)

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT sequence) AS distinct_sequences,
  MIN(sequence) AS min_seq,
  MAX(sequence) AS max_seq,
  (MAX(sequence) - MIN(sequence) + 1) AS expected_rows
FROM messages
WHERE session_id = '<session-uuid>';
```

`total_rows = distinct_sequences = expected_rows` means sequences are contiguous. The `sequence` column is the **write order**, not the call/result pairing order — for pairing, always join on `tool_call_id`.

## Known data shape quirks

- **`sequence` is write order, not call/result order.** The executor writes the call row before running the tool and the result row after; for parallel tool calls, the call rows are written in sequence order as each tool's HTTP request arrives at forge, then the result rows interleave as each tool completes. A typical interleaving looks like `call_A, call_B, result_A, call_C, result_B, result_C` — adjacent sequences may not be a pair.
- **Streaming bash has `stdout: null, stderr: null`.** The bytes went to the SSE consumer. To capture them, the model writes to a file and `read`s it back; the file contents then show up in the `read` row's `tool_output.output`.
- **`tool_call_id` from the extension can be a string, an object, or null** in pi's output. The Rust code normalizes this in `ToolInput::tool_call_id_str()` (and the streaming variant) before persisting. The DB always stores it as a string.
- **Bash timeouts are not always recorded.** A timed-out bash call has `timed_out: true` in `tool_output` *and* `is_error: true` in the `content` text — but the `duration_ms` is the time the executor spent, which may be slightly less than the configured `timeout_ms` because the timeout fires and the executor returns immediately.
