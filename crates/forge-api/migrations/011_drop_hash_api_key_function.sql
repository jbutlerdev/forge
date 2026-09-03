-- 011_drop_hash_api_key_function.sql
--
-- Remove the legacy SQL `hash_api_key()` function created by migration 002.
--
-- It hashed a DIFFERENT representation than the Rust code does: the SQL
-- function decodes the `sk_forge_`-stripped key as hex bytes and hashes
-- those bytes, while the Rust `hash_api_key` in api/auth.rs hashes the
-- ASCII string of the stripped key. Two different "hashes" in the repo
-- was a footgun: any code (or operator) calling the SQL function got a
-- value that never matches the `api_keys.key_hash` column.
--
-- Key hashing now lives exclusively in Rust (api/auth.rs), which also
-- added HMAC-with-server-secret support. There is no remaining caller
-- of this SQL function; it is dropped so the divergence can never be
-- relied on again.

DROP FUNCTION IF EXISTS hash_api_key(text);
