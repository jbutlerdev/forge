-- Migration: 008_fix_admin_placeholder_hash.sql
--
-- Self-heal the admin account hash on databases that applied the
-- ORIGINAL 002_users_and_api_keys.sql (which inserted a placeholder
-- argon2 string: '$argon2id$v=19$m=19456,t=2,p=1$placeholder$
-- placeholder'). That string is not parseable by `PasswordHash::new`,
-- so every login attempt with admin@forge.local returned a 500 and
-- the account could never be used (nor its password reset via the API).
--
-- 002 was later fixed in place to insert a REAL hash for admin123,
-- but sqlx tracks applied migrations by version: any DB that already
-- applied the old 002 keeps the broken placeholder hash forever.
-- Fresh installs skip 002 (already applied with the good hash) and
-- this UPDATE matches nothing (no-op).
--
-- The hash below is the same one 002 now ships with: a real argon2id
-- hash (m=19456, t=2, p=1, v=19, fixed salt) for the password
-- `admin123`. Deliberately target ONLY the placeholder rows so we
-- never clobber a hash the operator changed out-of-band.

UPDATE users
SET password_hash = '$argon2id$v=19$m=19456,t=2,p=1$Rm9yZ2UtYm9vdHN0cmFwLTAx$1ywFNSUTJ12UHTGRD38ZGcDgPraEVrMU8JAm0bwyCdk'
WHERE email = 'admin@forge.local'
  AND password_hash LIKE '%placeholder%';
