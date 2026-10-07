-- Herd H2.2: subagent sessions.
--
-- A session whose agent was started by another session's
-- `spawn_subagent` tool carries the parent session's id in
-- `parent_session_id`. The parent link is what
-- `GET /sessions?parent=<uuid>` filters on, and what the
-- `subagent_ended` SSE event (published on the PARENT's stream)
-- is derived from: the harness event consumer looks up
-- `sessions.parent_session_id` for a terminal task on a
-- subagent's durable conversation.
--
-- The row itself is minted by the harness event consumer when it
-- sees the harness's `subagent_spawned` event: the harness pre-mints
-- the child's forge session UUID (it lives in the child's
-- `forge.meta` document) and the consumer reuses it as the row id,
-- so the two sides can never disagree.
ALTER TABLE sessions
    ADD COLUMN parent_session_id UUID
    REFERENCES sessions (id) ON DELETE CASCADE;

CREATE INDEX idx_sessions_parent ON sessions (parent_session_id);
