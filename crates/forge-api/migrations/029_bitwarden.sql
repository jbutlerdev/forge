-- 029: Bitwarden (Vaultwarden) Secrets Manager bridge (2026-10-18).
--
-- Owner-level connection config for pulling secret VALUES from the
-- user's self-hosted Vaultwarden at agent-bash exec time. Credential
-- slots (H6.5 `secrets` table) may store references instead of
-- values: value = `bw://<sm-secret-key>` resolves through this config
-- (see api/../bitwarden.rs). One config per owner (the lab vault).
--
-- At-rest posture matches H6.5 `secrets` (documented, accepted risk
-- for a self-hosted single-operator deployment): values are never
-- returned by any API — only names/metadata and a masked client_id.

CREATE TABLE IF NOT EXISTS bitwarden_config (
    owner_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    server_url text NOT NULL,               -- e.g. https://vault.example.tld
    org_id text NOT NULL,                   -- SM organization uuid
    client_id text NOT NULL,                -- SM service-account client id
    client_secret text NOT NULL,            -- SM service-account secret
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
