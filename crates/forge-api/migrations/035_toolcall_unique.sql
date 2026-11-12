-- 035: unique index required by DbToolRecorder's ON CONFLICT upsert.
--
-- recording.rs upserts tool-call/tool-result rows with
--   ON CONFLICT (session_id, tool_call_id, role) WHERE tool_call_id IS NOT NULL
-- but no matching partial unique index existed on messages, so EVERY
-- streaming bash audit write failed Postgres 42P10 ("there is no unique
-- or exclusion constraint matching the ON CONFLICT specification").
-- Agents saw calls die with no/partial output — the 2026-11-12 drover
-- "silent deaths" cluster. The index was created live on 2026-11-12;
-- this migration makes fresh installs conformant.

CREATE UNIQUE INDEX IF NOT EXISTS messages_session_toolcall_role_uq
    ON messages (session_id, tool_call_id, role)
    WHERE tool_call_id IS NOT NULL;
