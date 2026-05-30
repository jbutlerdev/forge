-- Migration: 002_users_and_api_keys.sql
-- Add user management and API key authentication

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'user',  -- 'admin' | 'user'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- API Keys table
CREATE TABLE IF NOT EXISTS api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    key_hash        TEXT NOT NULL UNIQUE,  -- SHA-256 hash of the key
    key_prefix      TEXT NOT NULL,          -- First 12 chars for identification
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add user_id to existing tables (with default NULL for backwards compatibility)
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

-- Function to hash API key
CREATE OR REPLACE FUNCTION hash_api_key(api_key TEXT) RETURNS TEXT AS $$
    WITH decoded AS (
        SELECT decode(replace(api_key, 'sk_forge_', ''), 'hex') as key_bytes
    )
    SELECT encode(sha256(key_bytes), 'hex') FROM decoded;
$$ LANGUAGE SQL IMMUTABLE;

-- Create admin user if not exists (for initial setup)
-- Password: admin123 (change this in production!)
INSERT INTO users (email, name, password_hash, role)
VALUES ('admin@forge.local', 'Forge Admin', '$argon2id$v=19$m=19456,t=2,p=1$placeholder$placeholder', 'admin')
ON CONFLICT (email) DO NOTHING;
