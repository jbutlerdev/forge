-- 030: bw-CLI mode for the Bitwarden bridge (2026-10-18).
--
-- Vaultwarden (the lab's vault.butler.ooo) does NOT implement the
-- Secrets Manager /align endpoints (verified against 1.37.4: no
-- /sm/* routes in the binary). The bridge therefore supports two
-- modes:
--   mode='sm'      — real Bitwarden (or any align-compatible server)
--                    via the SM service-account flow (migration 029).
--   mode='bw-cli'  — Vaultwarden via the official `bw` CLI shipped on
--                    the forge-api unit's PATH; the process logs in
--                    with email + FORGE_BW_MASTER_PASSWORD (unit env,
--                    never the DB) and resolves `bw://notes/<name>` /
--                    `bw://item/<name>/<path>` references.
--
-- The master password deliberately has NO column here — it rides the
-- systemd unit environment only (same posture as the lab bitwarden
-- skill's env file).

ALTER TABLE bitwarden_config
    ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'sm'
        CHECK (mode IN ('sm','bw-cli')),
    ADD COLUMN IF NOT EXISTS email text NOT NULL DEFAULT '';
