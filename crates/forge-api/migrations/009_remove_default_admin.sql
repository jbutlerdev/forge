-- Migration: 009_remove_default_admin.sql
--
-- Remove the default admin account shipped by migration 002
-- (admin@forge.local, password admin123). Every database --
-- fresh or deployed -- ended up with a valid admin at known
-- credentials, which is a backdoor for an open-source project.
--
-- Admin accounts are now created at startup instead: when
-- FORGE_ADMIN_EMAIL + FORGE_ADMIN_PASSWORD are both set and no
-- role='admin' user exists, auth::bootstrap_admin creates one
-- with a real argon2id hash. Operators who configured their own
-- admin out-of-band are untouched -- this migration only deletes
-- the seeded admin@forge.local row.
--
-- api_keys rows are removed explicitly (the FK is ON DELETE
-- CASCADE on users(id), but being explicit makes the intent
-- auditable and is safe if the FK is ever changed).

DELETE FROM api_keys
WHERE user_id IN (SELECT id FROM users WHERE email = 'admin@forge.local');

DELETE FROM users WHERE email = 'admin@forge.local';
