-- Migration 033: refine the 032 backfill.
--
-- 032 flagged every non-convention working_dir as anchored — but
-- WORKSPACE bindings (agents.workspace_dir, migration 031) are
-- sandboxed rows that legitimately persist a non-convention path,
-- and the 003286d6 blanket write left empty-string columns that
-- tripped the same rule. An over-flagged herd agent would run its
-- bash HOST-SIDE (unsandboxed) — the exact escape 032 exists to
-- close. A real anchor must differ from BOTH the per-session tree
-- convention AND the agent's declared workspace_dir.

UPDATE sessions SET anchored = FALSE
 WHERE anchored = TRUE
   AND agent_id IS NOT NULL
   AND working_dir IS NOT NULL
   AND working_dir = (SELECT workspace_dir FROM agents WHERE id = sessions.agent_id);

-- Non-anchored rows that are not live workspace bindings go back to
-- convention-based resolution: empty strings, whitespace, and the
-- per-session-tree copies the blanket write persisted.
UPDATE sessions SET working_dir = NULL
 WHERE anchored = FALSE
   AND (btrim(working_dir) = ''
        OR working_dir = '/forge/sessions/' || id::text);
