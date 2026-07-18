-- Widen the `profiles.provider` CHECK constraint to include
-- `proxy-anthropic`, which is a documented, code-supported provider
-- (pi_agent.rs handles `"anthropic" | "proxy-anthropic"`; docs/API.md
-- and AGENTS.md list it). The original CHECK in migration 001 only
-- allowed `('openai', 'anthropic')`, so `POST /profiles` with
-- `provider:"proxy-anthropic"` failed with a CHECK violation mapped
-- to a generic 500 "Failed to create profile" -- the documented
-- provider was uncreatable. No migration ever widened it.
--
-- We DROP the old constraint and ADD a new one with the full allowed
-- set. `DROP CONSTRAINT IF EXISTS` makes this idempotent against a
-- partially-applied state. The constraint name is the one Postgres
-- auto-generated for the original CHECK (`profiles_provider_check`),
-- so `IF EXISTS` matches it on databases that ran migration 001.
--
-- The allowed set is the union of what the code handles in
-- `pi_agent.rs` (`openai`, `anthropic`, `proxy-anthropic`, plus a
-- `_ =>` default that sets `ANTHROPIC_API_KEY` for any other string)
-- and the providers operators have actually configured in prod
-- (`proxy`, `google`, `gemini`, `custom`). The code's `_ =>` default
-- means an unknown provider still boots; the CHECK is a backstop that
-- rejects obvious garbage, not the primary gate (the API handler
-- validates `provider` and returns 400 before it reaches the DB).
ALTER TABLE profiles DROP CONSTRAINT IF EXISTS profiles_provider_check;

ALTER TABLE profiles
    ADD CONSTRAINT profiles_provider_check
    CHECK (provider IN ('openai', 'anthropic', 'proxy-anthropic',
                        'proxy', 'google', 'gemini', 'custom'));
