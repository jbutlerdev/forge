-- Herd H2.0 (part 2): session -> durable-conversation mapping.
--
-- A harness-backed session carries the pi-durable conversation id of
-- the harness conversation that owns its turns (created via the
-- harness IPC `createConversation`, which stores the forge session id
-- in the conversation's `forge.meta` document). Nullable: legacy
-- sessions stay NULL and keep the drive_turn/pi-subprocess path.
--
-- H2.0 part 2 wires only the READER side (interrupt forwarding, the
-- harness-event consumer); every existing session stays NULL until the
-- turn cutover (H2.1) stamps this column via `createConversation`.
-- Durable ids are pi-durable's erased non-negative integer brands, so
-- BIGINT (no cross-DB FK — the durable_* tables belong to the
-- harness's schema).
ALTER TABLE sessions ADD COLUMN durable_conversation_id BIGINT;
CREATE INDEX idx_sessions_durable_conversation_id ON sessions(durable_conversation_id);
