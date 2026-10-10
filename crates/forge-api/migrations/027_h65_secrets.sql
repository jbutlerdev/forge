-- Herd H6.5: named credential slots (the secret-store seam) + the
-- agent's declared memory org label.
--
-- `secrets` is forge's minimal honest secret store: named (owner,
-- name) values resolved by the sandbox at tool-execution time when an
-- agent's `credentials_scope` declares `{env_refs: [name, …]}`. The
-- value NEVER appears in: the `agents.credentials_scope` JSONB (that
-- holds the ref NAMES only), the /secrets API (names + expiry, never
-- values), `messages` rows, or the ranch approval cards. It enters a
-- sandboxed bash call only as an nspawn `--setenv=` argument for the
-- call's lifetime.
--
-- `valid_until` = NULL means no expiry; a set-and-past value is
-- "expired" (the sandbox call fails naming the slot, never leaking
-- the value).
CREATE TABLE secrets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    value       TEXT NOT NULL,
    valid_until TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_id, name)
);
CREATE INDEX idx_secrets_owner ON secrets(owner_id);

-- H6.5 wizard: the agent's declared memory org label (free-form, the
-- same label space as `memory_acl.org`). Storing it on the agent row
-- gives the provisioning wizard (`ranch agents new`, mobile "＋ Agent")
-- a real column to write the org id the user was prompted for; the
-- ACL grant itself remains `memory_acl` operator/reflection plumbing.
ALTER TABLE agents ADD COLUMN org_id TEXT;
