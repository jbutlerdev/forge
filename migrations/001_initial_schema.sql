-- Forge Initial Schema
-- Profiles, Sessions, and Messages

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Profiles: Configuration for agent environments
CREATE TABLE profiles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    
    -- Model configuration
    provider    TEXT NOT NULL CHECK (provider IN ('openai', 'anthropic')),
    model       TEXT NOT NULL,
    base_url    TEXT,          -- Override default API base URL (optional)
    api_key     TEXT,          -- Stored as-is for now, consider encryption later
    
    -- Sandbox configuration
    working_dir TEXT NOT NULL,
    git_url     TEXT,          -- Repository to clone (optional)
    git_ref     TEXT,          -- Branch/tag/commit (optional)
    nix_shell   TEXT,          -- Nix shell expression or path (optional)
    
    -- Agent behavior
    system_prompt TEXT NOT NULL DEFAULT 'You are a helpful coding assistant.',
    tools       TEXT NOT NULL DEFAULT '["bash", "read", "write", "edit"]',
    
    -- Timestamps
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sessions: Running or completed agent sessions
CREATE TABLE sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id  UUID NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    title       TEXT,          -- Auto-generated or user-set
    
    -- Cell state (nspawn container)
    cell_host   TEXT,          -- Host where cell is running (null if cold)
    cell_state  JSONB,         -- Serialized cell state (working dir, git state, etc.)
    last_active TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Metadata
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at    TIMESTAMPTZ
);

CREATE INDEX idx_sessions_profile_id ON sessions(profile_id);
CREATE INDEX idx_sessions_last_active ON sessions(last_active);

-- Messages: Event log for sessions
CREATE TABLE messages (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id  UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    
    -- Ordering
    sequence    INTEGER NOT NULL,
    
    -- Content
    role        TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'tool', 'system')),
    content     TEXT,
    
    -- Tool call metadata
    tool_name   TEXT,           -- e.g., 'bash', 'read'
    tool_input  JSONB,          -- Arguments passed to tool
    tool_call_id TEXT,          -- ID matching provider tool_call format
    
    -- Timestamps
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure sequence is unique per session
    UNIQUE(session_id, sequence)
);

CREATE INDEX idx_messages_session_id ON messages(session_id);
CREATE INDEX idx_messages_session_seq ON messages(session_id, sequence);

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_profiles_updated_at
    BEFORE UPDATE ON profiles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Get next sequence number for a session
CREATE OR REPLACE FUNCTION get_next_sequence(session_uuid UUID)
RETURNS INTEGER AS $$
DECLARE
    max_seq INTEGER;
BEGIN
    SELECT COALESCE(MAX(sequence), 0) INTO max_seq
    FROM messages
    WHERE session_id = session_uuid;
    RETURN max_seq + 1;
END;
$$ LANGUAGE plpgsql;
