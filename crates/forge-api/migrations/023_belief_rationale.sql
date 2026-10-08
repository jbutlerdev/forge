-- Herd H4.4: belief rationale.
--
-- `beliefs` (022_memory.sql) has no column for the proposer's
-- stated reason. The reflection proposals endpoint and the approval
-- card both carry it, and it must survive on the row (GET
-- /memory/beliefs responses + the card payload), so it gets its own
-- column. Guarded exactly like 022: when pgvector was not installed
-- at migration time the `beliefs` table does not exist and this is a
-- no-op (the memory feature is unavailable and returns 501 anyway).
DO $rat$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'beliefs') THEN
        ALTER TABLE beliefs ADD COLUMN IF NOT EXISTS rationale TEXT;
    ELSE
        RAISE NOTICE 'beliefs table absent (no pgvector at 022 time); skipping H4.4 rationale column';
    END IF;
END
$rat$;
