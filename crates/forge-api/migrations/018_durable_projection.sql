-- Herd H2.1: bookkeeping for the messages-table projection of
-- harness transcripts.
--
-- When a durable `pi.assistant` entry is committed in the harness
-- (Postgres), forge-api's event consumer projects it onto the flat
-- `messages` audit table (one assistant row + bus `message` event).
-- This table records which (conversation, entry) pairs have already
-- been projected, so a re-delivered or re-processed `turn_end` event
-- can never write a second assistant row for the same durable entry.
CREATE TABLE durable_projection (
    conversation_id BIGINT NOT NULL,
    entry_id        BIGINT NOT NULL,
    session_id      UUID   NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (conversation_id, entry_id)
);
