# durable-pg

Postgres `Storage` backend for `@earendil-works/pi-durable` — the durable core for the Herd
initiative (forge `docs/PLAN-HERD.md`, Phase H0.2).

The vendor ships Memory, JSONL, and SQLite backends; Herd needs one shared Postgres store so the
Node harness (sole writer of the `durable_*` tables) and forge-api (reader) see the same durable
state. This package is a faithful port of the vendor's reference SQLite backend
(`packages/durable/src/storage/sqlite/storage.ts` at pin `ae92585d`): same schema shape, same
ID/sequence semantics, same write-time checks — only the driver differs.

## Usage

```ts
import { PgStorage } from "./src/storage-pg.ts";

// Owns its pool; applies pending migrations on open.
const storage = await PgStorage.open({
  connectionString: "postgres://postgres:forge@127.0.0.1:5432/postgres",
  schema: "durable", // optional; namespaces the durable_* tables
});

// Or wrap an existing pool (close() then does not close it).
const storage2 = await PgStorage.open({ pool: existingPool, schema: "durable" });
```

`schema` pins every connection the storage acquires to `search_path = <schema>, public` via a
wrapper around `pool.connect` (a `SET` in the pool's `connect` event races the first query; the
connection `options` startup parameter works only for pools PgStorage creates itself).

## Schema

`migrations/001_initial.sql`, mirrored from the SQLite reference with Postgres types:

- `durable_schema` — applied-migration version (created by `PgStorage.open`).
- `durable_metadata` — single row; `next_id` (record-ID allocator, TEXT: safe past 2^53) and
  `next_seq` (commit sequence). Read + advanced under `SELECT … FOR UPDATE` inside the commit
  transaction.
- `durable_record_ids` — global ID ownership; one numeric namespace across conversations, entries,
  tasks, submissions, documents (pi-durable semantics; exclusivity is enforced in `checkGlobalIds`).
- `durable_conversations` / `durable_entries` / `durable_tasks` / `durable_submissions` /
  `durable_documents` / `durable_document_revisions` — record JSON kept as JSON **text** (not
  `jsonb`: Postgres rejects JSON escapes for lone surrogates, which conformance requires storing
  losslessly). Indexed columns exist for scans/queries only; the harness owns payload shapes.
- `durable_submissions(conversation_id, request_id)` — the `submissionByRequest` dedup key;
  admission is decided by the harness inside the commit transaction, so the index is not unique.

Boundary (per `docs/HERD-DURABLE-DECISION.md`): the Node harness is the **sole writer** of these
tables; forge-api reads them directly and applies effects via IPC.

## Tests

```sh
# Scratch Postgres on 127.0.0.1:5432 (user postgres, password forge), or set DURABLE_PG_URL.
npm test
```

- **Conformance** — the vendor's own suite (`registerStorageConformance` from
  `@earendil-works/pi-durable/testing`), 23 cases, each over a fresh `durable_test_*` schema.
  Note: the installed npm dist ships 20 cases; the vendored source has 23 (the dist copy predates
  the scan `order` field). The suite registers from the installed package and asserts the vendor
  case count.
- **Kill-9 ×2** —
  1. SIGKILL a child mid-commit storm; reopen and assert the surviving log is a prefix of whole
     5-entry batches (no partial commit), no ID gaps, and both allocators exactly consistent with
     the survivors.
  2. SIGKILL a child after `Harness.open`; reopen the vendor Harness over PgStorage and verify the
     root conversation is readable and the store usable.

## Benchmark

`npm run bench` seeds the vendor's representative dataset (1,870 records at the timing scale) and
times the vendor's read/write benchmarks. Postgres vs the reference SQLite backend (same machine,
scratch stores):

| benchmark | postgres (ms) | sqlite (ms) |
|---|---:|---:|
| exact entry lookup | 0.06 | 0.01 |
| entry page scan (100) | 0.62 | 0.13 |
| filtered task scan (50) | 0.11 | 0.03 |
| exact document address | 0.10 | 0.01 |
| document replay tail (1024) | 2.08 | 1.58 |
| fork-depth head lookup | 1.15 | 0.04 |
| commit one entry | 0.92 | 0.05 |
| commit 100-entry batch | 32.31 | 0.68 |
| commit mixed batch | 4.67 | 0.26 |

Reads are single-digit-millisecond at Herd's scale; writes are network-round-trip bound (each
commit is one transaction over a TCP socket vs SQLite's in-process fsync). Both are acceptable for
the harness's commit cadence; revisit with statement pipelining or a Unix-socket DSN if commit
latency ever shows up in traces (see kill criteria in `docs/HERD-DURABLE-DECISION.md`).
