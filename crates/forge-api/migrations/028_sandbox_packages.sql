-- 028: sandbox package governance (2026-10-18).
--
-- Agents get a `request_sandbox_packages` tool; requests land HERE as
-- pending rows. The herd's designated approver agent (default: the
-- "executive assistant", configured via FORGE_SANDBOX_APPROVER) is
-- notified in its conversation and may approve, reject, or escalate to
-- the user; the mobile agent editor shows the same queue for direct
-- user control. APPROVED rows are the persistent install set:
-- SandboxManager re-`apt-get install`s any package missing from a
-- session rootfs's `/etc/forge-packages.json` marker before every
-- sandboxed bash exec — so installs survive session death, sandbox
-- resets, and base refreshes.
--
-- Requests are per PACKAGE (one row each) so decisions can differ
-- within a batch. `package_name` is strict-validated in Rust before
-- insert (regex + isolation denylist); the marker file is written only
-- after a successful install.

CREATE TABLE IF NOT EXISTS sandbox_package_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- NULL = owner-wide (applies to every sandbox this owner's agents
    -- get; provisioned by the user from mobile/CLI only).
    agent_id uuid REFERENCES agents(id) ON DELETE CASCADE,
    package_name text NOT NULL,
    reason text,
    requested_by text NOT NULL DEFAULT 'agent',   -- 'agent' | 'user'
    requested_by_agent uuid REFERENCES agents(id) ON DELETE SET NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','approved','rejected','installed')),
    decided_by text,                              -- 'ea:<uuid>' | 'user' | 'auto'
    decided_at timestamptz,
    install_error text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_pkg_agent
    ON sandbox_package_requests(agent_id, status);
