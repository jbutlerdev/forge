-- Per-session model overrides for the "model switcher" (Option A).
--
-- A profile carries the full agent config (provider + model + api_key
-- + base_url + working_dir + git_url + tools + system_prompt). The
-- model switcher lets a user change *just the brain* (provider +
-- model + credentials) mid-conversation while keeping the session's
-- existing working dir / git repo / sandbox / tools. Without these
-- columns, switching models meant switching profiles, which re-
-- derives the working dir from the new profile's git_url/working_dir
-- -- landing the agent in a different repo mid-conversation.
--
-- These four nullable columns hold the override. When non-NULL,
-- `agent_registry::get_or_create` prefers them over the profile's
-- values for provider/model/base_url/api_key (and only those). The
-- profile's working_dir, git_url, git_ref, nix_shell, system_prompt,
-- and tools remain in effect -- the workspace doesn't move.
--
-- All NULL-able so "no override" is the default; a session created
-- the normal way (POST /sessions with a profile_id) has no overrides
-- and behaves exactly as before.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS override_provider text,
    ADD COLUMN IF NOT EXISTS override_model   text,
    ADD COLUMN IF NOT EXISTS override_base_url text,
    ADD COLUMN IF NOT EXISTS override_api_key  text;
