-- Fix race in get_next_sequence(). The previous implementation
-- computed `MAX(sequence) + 1` without any locking, so two concurrent
-- transactions (e.g. the harness writing a tool-call row while the
-- executor writes the matching tool-result row) could both see the
-- same MAX and both try to INSERT at the same sequence, blowing up
-- the UNIQUE (session_id, sequence) constraint.
--
-- The fix: take a transaction-scoped advisory lock keyed on the
-- session id. We use the two-int form of pg_advisory_xact_lock so the
-- first int is a stable "namespace" for forge (1) and the second is a
-- hash of the session uuid. The lock is auto-released on COMMIT or
-- ROLLBACK, and is invisible to other applications that might use
-- advisory locks with a different namespace.
CREATE OR REPLACE FUNCTION get_next_sequence(session_uuid UUID)
RETURNS INTEGER AS $$
DECLARE
    max_seq INTEGER;
BEGIN
    PERFORM pg_advisory_xact_lock(1, hashtext(session_uuid::text));
    SELECT COALESCE(MAX(sequence), 0) + 1 INTO max_seq
    FROM messages
    WHERE session_id = session_uuid;
    RETURN max_seq;
END;
$$ LANGUAGE plpgsql;
