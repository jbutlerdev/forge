-- Herd H2.6: lazy-migration claim (post-cutover).
--
-- After the cutover there is no legacy turn path: every session
-- must own a durable harness conversation before it can be written
-- to. Sessions that predate cutover (durable_conversation_id IS
-- NULL) are migrated on their first write operation (POST
-- /messages, compact, timer set) by
-- `harness_migration::ensure_migrated`.
--
-- Two concurrent touches of the same unmigrated session must not
-- both run the migration (createConversation + importEntries).
-- `harness_migrating` + `harness_migration_at` form a simple
-- atomic claim:
--
--   UPDATE sessions
--      SET harness_migrating = TRUE, harness_migration_at = NOW()
--    WHERE id = $1
--      AND durable_conversation_id IS NULL
--      AND (harness_migrating = FALSE
--           OR harness_migration_at < NOW() - INTERVAL '10 minutes');
--
-- Only one concurrent UPDATE can claim; the losers poll
-- `durable_conversation_id` until the winner stamps it. The
-- staleness window (10 minutes) lets a claim left behind by a
-- crashed forge-api (stuck at harness_migrating = TRUE with no
-- stamp) be taken over on the next touch instead of wedging the
-- session forever.

ALTER TABLE sessions ADD COLUMN harness_migrating BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sessions ADD COLUMN harness_migration_at TIMESTAMPTZ;
