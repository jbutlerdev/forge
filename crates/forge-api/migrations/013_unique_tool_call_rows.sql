-- 013_unique_tool_call_rows.sql
-- P1-24: enforce at most one row per (session_id, tool_call_id, role).
--
-- Backs the idempotent ON CONFLICT writes in DbToolRecorder
-- (record_call / record_result): a retried POST /tools/execute
-- returns the existing row instead of double-recording. The index
-- is partial because plain user/assistant text rows carry
-- tool_call_id = NULL and must stay unlimited.
--
-- Dedup any pre-existing duplicates first (keep the lowest sequence
-- per group) so the index can be created on a DB that already
-- recorded stray retries.

DELETE FROM messages AS a
USING messages AS b
WHERE a.session_id = b.session_id
  AND a.tool_call_id = b.tool_call_id
  AND a.role = b.role
  AND a.tool_call_id IS NOT NULL
  AND a.sequence > b.sequence;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_messages_tool_call
    ON messages (session_id, tool_call_id, role)
    WHERE tool_call_id IS NOT NULL;
