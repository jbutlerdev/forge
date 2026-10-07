-- Add `faux` to the allowed `profiles.provider` set.
--
-- `faux` is the pi-ai scripted test provider that the Node harness
-- registers when `FORGE_HARNESS_FAUX=1` (see `harness/src/main.ts`
-- `buildModels`). The Herd H2.1 integration test creates a forge
-- profile whose provider/model point at it (`faux` / `faux-1`); the
-- old CHECK constraint rejected the profile before it could reach
-- the harness. No credentials, no network — the provider is
-- in-process in the harness and only ever enabled by an explicit env
-- flag, so widening the backstop here has no production surface.
ALTER TABLE profiles DROP CONSTRAINT IF EXISTS profiles_provider_check;

ALTER TABLE profiles
    ADD CONSTRAINT profiles_provider_check
    CHECK (provider IN ('openai', 'anthropic', 'proxy-anthropic',
                        'proxy', 'google', 'gemini', 'custom', 'faux'));
