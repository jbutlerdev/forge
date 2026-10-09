-- Herd H5.3: the agent pause kill switch.
--
-- `paused` gates NEW work only: while true, `dispatch_message` (and the
-- agent-talk alias + the OpenAI-completions session path) reject turns
-- with 409 "agent paused" BEFORE the user row lands, and timer fires on
-- the agent's conversations are no-ops (the harness checks this column
-- at the fire seam — `harness/src/timers.ts` — and the claimed timer
-- row simply loses that tick). In-flight turns finish; read surfaces
-- (search, memory, tasks, research) stay open.
--
-- Set via `POST /agents/:id/pause` / `POST /agents/:id/resume`
-- (owner-gated like the rest of the agent surface). The partial index
-- backs a cheap "list paused agents" scan.

ALTER TABLE agents ADD COLUMN IF NOT EXISTS paused BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS agents_paused_idx ON agents (owner_id) WHERE paused;
