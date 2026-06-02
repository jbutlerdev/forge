-- 002_tool_output.sql
-- Add structured tool output + timing columns to the messages table.
--
-- The original schema captured tool calls (`tool_name`, `tool_input`,
-- `tool_call_id`) and the human-readable result text (`content`), but
-- there was nowhere to store the structured output that the executor
-- actually saw (stdout/stderr split, exit code, typed result blob).
-- The recording refactor split that responsibility: the harness
-- records the call intent, the executor records the outcome. The
-- outcome now needs somewhere to live beyond the flattened `content`
-- string.

ALTER TABLE messages ADD COLUMN IF NOT EXISTS tool_output JSONB;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS duration_ms BIGINT;
