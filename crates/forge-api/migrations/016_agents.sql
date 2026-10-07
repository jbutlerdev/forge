-- Herd H1.1: the Agent entity.
--
-- An agent ("dot") is a first-class row in forge, distinct from sessions
-- (conversations) and profiles (model/tool config). One agent has many
-- conversations; sessions created through an agent carry `agent_id`.

CREATE TABLE agents (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id           UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name               TEXT NOT NULL,
    avatar_url         TEXT,
    home_machine       TEXT,                 -- ranch machine id the agent "lives on" (nullable)
    primary_profile_id UUID REFERENCES profiles(id) ON DELETE SET NULL,
    visibility         TEXT NOT NULL DEFAULT 'private' CHECK (visibility IN ('private','org')),
    memory_scope       TEXT NOT NULL DEFAULT 'agent' CHECK (memory_scope IN ('agent','org')),
    tools_allowlist    JSONB NOT NULL DEFAULT '[]',   -- empty = profile's tools
    credentials_scope  JSONB NOT NULL DEFAULT '{}',   -- specialist-dot creds (H6.4)
    extra_instructions TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_id, name)
);
ALTER TABLE sessions ADD COLUMN agent_id UUID REFERENCES agents(id) ON DELETE SET NULL;
CREATE INDEX idx_sessions_agent_id ON sessions(agent_id);
