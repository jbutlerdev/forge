-- 031: herd steward support (2026-10-19)
--
-- The steward is an always-learning herd agent that audits the herd
-- itself (ranch / forge / mule): agent runs, system logs, wakes,
-- sandbox governance. Two gaps blocked it:
--
-- 1. Per-agent persistent workspace. Every conversation gets a fresh
--    /forge/sessions/<uuid> working dir, so an hourly wake would
--    re-clone its repos every run. `agents.workspace_dir` pins a
--    stable directory instead: create_conversation binds it (the
--    nspawn bind maps host path 1:1, so checkouts survive across
--    wakes while each wake still gets a clean conversation context).
--    Empty = today's per-session tree (default for every existing
--    agent).
--
-- 2. Log access. Sandboxes are mount-namespace-isolated and cannot
--    read any journald (ranchd lives in the ranch-mini VM, mule in
--    the mule VM — not even the same filesystem). The shipper
--    (mac-host launchd/terminal job running `container machine run
--    -n <vm> journalctl -o json --since …` — the same root path all
--    deploys use) POSTs rows here; the steward reads them via
--    GET /logs. Only whitelisted sources are accepted (the ingest
--    handler is the trust boundary: source in a fixed set, msg
--    capped, retention pruned).

CREATE TABLE IF NOT EXISTS system_logs (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT        NOT NULL,   -- 'ranchd' | 'forge-api' | 'forge-harness' | 'mule'
    host        TEXT        NOT NULL DEFAULT 'mini',
    ts          TIMESTAMPTZ NOT NULL,
    level       TEXT,                   -- syslog priority name, best-effort (PRIORITY=)
    unit        TEXT,
    pid         INTEGER,
    msg         TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS system_logs_ts_idx      ON system_logs (ts DESC);
CREATE INDEX IF NOT EXISTS system_logs_source_ts   ON system_logs (source, ts DESC);

-- Newest-first read window is a seq scan over recent rows otherwise;
-- the retention sweep deletes by age hourly.
DELETE FROM system_logs WHERE ts < NOW() - INTERVAL '14 days';

ALTER TABLE agents ADD COLUMN IF NOT EXISTS workspace_dir TEXT;
