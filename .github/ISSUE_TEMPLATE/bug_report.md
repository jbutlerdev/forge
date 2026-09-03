---
name: Bug report
about: Something is broken or behaves differently than documented
title: ''
labels: bug
---

## What happened

Describe the bug and the actual vs. expected behavior.

## How to reproduce

Minimal steps:

1.
2.
3.

```bash
# exact commands, if relevant
```

## Environment

- Forge version (commit or release tag):
- OS / host:
- Postgres version:
- `pi` version (`pi --version`):
- Sandbox enabled? (per-session nspawn) yes/no

## Logs

Relevant `forge-api` log lines (redact API keys / tokens):

```text
```

## Audit log

If the issue involves tool calls / message rows, the relevant
`messages` rows (redact secrets) help a lot:

```sql
SELECT sequence, role, tool_name, tool_call_id, duration_ms, created_at
FROM messages WHERE session_id = '<uuid>' ORDER BY sequence;
```
