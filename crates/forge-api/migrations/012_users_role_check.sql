-- 012_users_role_check.sql
--
-- Constrain `users.role` to the two values the application actually
-- understands. `role` was added in migration 002 as plain TEXT with a
-- DEFAULT 'user' and no CHECK constraint, so anything could be written
-- (e.g. via `PATCH /users/:id` by an admin). The codebase compares the
-- role against the exact literals 'user' and 'admin' in several
-- places, so a misspelled value like 'Admin' or 'ADMIN' silently
-- stripped the account's admin rights with no error anywhere.
--
-- The constraint makes invalid values a hard database error at
-- insert/update time instead of a silent authorization hole.
--
-- NOTE: on a database that somehow already contains out-of-range role
-- values this migration will fail. Repair those rows first
-- (e.g. `UPDATE users SET role = 'user' WHERE role NOT IN ('user','admin')`)
-- and re-run.

ALTER TABLE users ADD CONSTRAINT users_role_check CHECK (role IN ('user', 'admin'));
