-- 032: subagent sandbox inheritance + explicit anchored flag (2026-10-20)
--
-- (a) anchored: the sandbox-vs-host decision for bash was INFERRED
--     from `sessions.working_dir IS NOT NULL` (the anchor convention:
--     an explicit cwd anchor runs host-side). Migration 031's
--     workspace binding started persisting the resolved working dir
--     on EVERY conversation, so fresh conversations were misclassified
--     as anchors and their bash executed on the forge VM host — a
--     sandbox escape. The decision now reads an explicit `anchored`
--     column (written only by the real cwd-anchor paths), and rows
--     polluted by the inference are restored to convention-based
--     resolution.
--
-- (b) sandbox_inherit: subagent children must execute in the EXACT
--     same sandbox as their parent (same rootfs tree, same bind, same
--     credential env). Tool calls arrive keyed by the CHILD session
--     id; the resolver walks this flag up the parent chain and uses
--     the nearest non-inheriting ancestor's container. Children also
--     now inherit agent_id (credential env_refs + audit visibility).

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS anchored BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS sandbox_inherit BOOLEAN NOT NULL DEFAULT FALSE;

-- (a) backfill: real anchors are dirs the operator passed explicitly
--     — they differ from BOTH the per-session tree convention AND the
--     agent's declared workspace_dir (workspace bindings are
--     sandboxed, never anchored; conflating them would unsandbox a
--     herd agent).
UPDATE sessions SET anchored = TRUE
 WHERE working_dir IS NOT NULL
   AND working_dir <> '/forge/sessions/' || id::text
   AND (agent_id IS NULL
        OR working_dir <> COALESCE(
              (SELECT workspace_dir FROM agents WHERE id = sessions.agent_id), ''));
-- Everything else that is not a live workspace binding goes back to
-- convention-based resolution (NULL): per-session-tree copies AND the
-- empty-string rows the blanket write left behind.
UPDATE sessions SET working_dir = NULL
 WHERE anchored = FALSE
   AND (working_dir IS NULL
        OR btrim(working_dir) = ''
        OR working_dir = '/forge/sessions/' || id::text);

CREATE INDEX IF NOT EXISTS sessions_parent_idx ON sessions (parent_session_id)
    WHERE parent_session_id IS NOT NULL;
