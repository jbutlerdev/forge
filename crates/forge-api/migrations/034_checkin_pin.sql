-- 034: Collapsible check-ins (mobile).
--
-- Scheduled wake turns ("check-ins") render as collapsed groups on the
-- phone so the conversation stays readable. `pinned` marks rows that
-- must render expanded: set on the WHOLE current check-in by the
-- harness `pin_checkin` tool (the agent declares the check-in carries
-- critical content) and automatically when the agent posts an
-- `@notify:` row mid-check-in (notify_user implies user-facing
-- importance). Default false; pre-existing rows read as unpinned.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS pinned BOOLEAN NOT NULL DEFAULT FALSE;

-- Partial index: renderers/queries ask "does this check-in contain any
-- pinned row"; only pinned rows are ever indexed.
CREATE INDEX IF NOT EXISTS messages_session_pin_idx
    ON messages (session_id, sequence) WHERE pinned;
