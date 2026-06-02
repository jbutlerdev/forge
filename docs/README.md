# Documentation

| File | What's in it |
|---|---|
| [`README.md`](../README.md) | Project overview, quick start, top-level API summary, repo layout |
| [`AGENTS.md`](../AGENTS.md) | Working guide for AI agents and humans: architecture, contracts, ops quirks, debugging checklist |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Deep-dive: message lifecycle, the ToolRecorder split, pi rpc event protocol, audit log schema, streaming tool path, session lifecycle, failure modes |
| [`API.md`](API.md) | REST API reference: per-endpoint request/response shape, curl examples, error formats |
| [`CLI.md`](CLI.md) | The `cli/forge` reference client: command reference, common patterns, output rendering, gotchas |
| [`OPERATIONS.md`](OPERATIONS.md) | systemd service, database setup, migrations, log/metric endpoints, common failure modes, upgrade procedure, backups |
| [`TOOL-AUDIT-LOG.md`](TOOL-AUDIT-LOG.md) | The `messages` table as an audit log: row shapes, per-tool `tool_output` shapes, SQL recipes for the most common queries |
| [`AGENT-CONVERSATION-DEBUG.md`](AGENT-CONVERSATION-DEBUG.md) | Historical: the 2026-05-30 debugging session that fixed the initial `pi` integration (rpc mode, event field renames, stderr pipe, turn guard) |
| [`../CHANGELOG.md`](../CHANGELOG.md) | What changed in each release |

## Suggested reading order

If you're new to the codebase:

1. `README.md` — what Forge is and how to start it
2. `ARCHITECTURE.md` — how the pieces fit together
3. `TOOL-AUDIT-LOG.md` — the data model you'll be querying most often
4. `OPERATIONS.md` — running it for real (migrations, systemd, failures)
5. `API.md` / `CLI.md` — for actually using it

If you're picking up a bug report:

1. `AGENTS.md` §12 (debugging checklist)
2. `OPERATIONS.md` (common failure modes)
3. `TOOL-AUDIT-LOG.md` (the SQL recipes)
4. `ARCHITECTURE.md` §3 (the ToolRecorder split — common source of "where does this row come from?" questions)
