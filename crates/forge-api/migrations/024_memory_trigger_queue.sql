-- Herd H4.5: memory trigger queue + belief trigger cooldown stamp.
--
-- A belief's `watch` predicate (022_memory.sql) wakes the agent when a
-- matching episode lands. The H4.2 capture task scans the agent's
-- active watch-bearing beliefs after each episode insert and, on a
-- hit past cooldown, writes one row here. The mule host's forwarder
-- lane (H4.5, `internal/wake/memory_trigger.go`) polls the
-- unconsumed rows of each configured agent every 30 s
-- (`GET /agents/:id/memory/triggers/pending`) and ACKs by id
-- (`POST …/triggers/:tid/consumed`) after firing the wake named by
-- the watch's `wake_id`.
--
-- 022 bundled the WHOLE memory schema (including `beliefs`) behind
-- the pgvector extension, so on a pgvector-less database `beliefs`
-- does not exist. The queue table itself has NO vector column (v1
-- predicates are text — `watch.match` is a substring, not a cosine),
-- so it is created UNCONDITIONALLY below. Only two things depend on
-- the 022 tables — the `belief_id` foreign key and the
-- `beliefs.last_triggered_at` cooldown stamp — and those are skipped
-- with a NOTICE when `beliefs` is absent (same guarded-ADD pattern as
-- 023_belief_rationale.sql). Without the 022 tables the memory
-- feature is 501-unavailable anyway, so the queue without the
-- constraint is a harmless no-op sink.
CREATE TABLE IF NOT EXISTS memory_trigger_queue (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id      UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    belief_id     UUID,
    episode_id    UUID,
    match_score   REAL,
    payload       JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS memory_trigger_queue_open
    ON memory_trigger_queue (agent_id, created_at)
    WHERE consumed_at IS NULL;

DO $mq$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'beliefs') THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'memory_trigger_queue_belief_id_fkey'
              AND conrelid = 'memory_trigger_queue'::regclass
        ) THEN
            ALTER TABLE memory_trigger_queue
                ADD CONSTRAINT memory_trigger_queue_belief_id_fkey
                FOREIGN KEY (belief_id) REFERENCES beliefs(id) ON DELETE SET NULL;
        END IF;
        -- H4.5 cooldown stamp: the last time this belief's watch
        -- actually queued a trigger (default 24 h cooldown applies
        -- until the first fire).
        ALTER TABLE beliefs ADD COLUMN IF NOT EXISTS last_triggered_at TIMESTAMPTZ;
    ELSE
        RAISE NOTICE 'beliefs table absent (no pgvector at 022 time); skipping memory_trigger_queue belief FK + beliefs.last_triggered_at';
    END IF;
END
$mq$;
