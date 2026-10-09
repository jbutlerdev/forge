-- Herd H5.1: proactive research — read-only-by-construction research
-- tasks + their user-facing suggestion cards.
--
-- One row per research task. The research work itself runs in the
-- Node harness as a durable conversation (`spawnResearch` RPC) whose
-- tool registry literally lacks the write-class tools; this table is
-- the FORGE-side lifecycle record:
--
--   pending    → the POST was accepted, the harness task not started yet
--   running    → a task_state `started` event was seen for the conversation
--   done       → the research task settled; the report document landed
--                and the suggestion card is (or was) pending
--   adopted    → the user answered the card with Use
--   discarded  → the user answered the card with Discard
--
-- (An Ask-more answer sends the task back to `running` with the
-- follow-up prompt as `resolution`; the next completion re-issues the
-- card.) `open=1` filtering = state NOT IN ('adopted','discarded').
--
-- `conversation_id` is the research session's row (its
-- `durable_conversation_id` links to the harness conversation; the
-- `agent_research.conversation_id` FK makes the H5.3
-- `GET /agents/:id/research?open=1` query one statement). `task_id`
-- is the durable task id learned from the first `task_state started`
-- event (NULL until the turn begins — the submission is admitted
-- before the task row is schedulable).
--
-- Plain DDL (no vector column): available with or without pgvector.

CREATE TABLE agent_research (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id       UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    task_id        BIGINT,
    question       TEXT NOT NULL,
    scope          TEXT,
    state          TEXT NOT NULL CHECK (state IN ('pending', 'running', 'done', 'adopted', 'discarded')),
    resolution     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ
);

-- The H5.3 activity view's open-research query.
CREATE INDEX agent_research_agent_state_idx
    ON agent_research (agent_id, state);
