# Architecture

This document covers how Forge is put together after the Herd H2.6
cutover: the message lifecycle on the durable harness, the split
between the harness and the tool executor, the harness IPC protocol,
lazy migration of legacy sessions, and the audit log schema.

## Tech stack

| Component | Technology |
|---|---|
| API server | Rust (edition 2024) on `axum`, `tokio`, `sqlx`, `tracing` |
| Database | PostgreSQL 15+ |
| Durable harness | Node.js process running pi-durable's `Harness` over `durable-pg` (Postgres), pinned in `vendor/pi-durable/` |
| LLM provider | whatever the profile's provider/model resolves to (the harness's provider registry) |
| Tool bridge | the harness's agent tooling calls back into forge-api's `POST /tools/execute` |
| Reference CLI | Bash at `cli/forge` |

There is no `pi` subprocess anymore. The legacy turn driver
(`pi --mode rpc` per session, `drive_turn`, resume, replay) was
deleted in H2.6; every conversation lives in the durable harness.

## 1. The big picture

Forge is two cooperating processes sharing one Postgres database:

- **forge-api** (Rust, axum): authentication, the `sessions` /
  `messages` / `profiles` tables, the tool executor
  (`POST /tools/execute`), SSE, timers API, and the **event
  consumer** that projects harness output back onto `messages`.
- **the harness** (Node, `harness/src/main.ts`): pi-durable's
  `Harness` over `durable-pg` in a scratch schema. It owns the
  canonical transcript (durable entries), the task scheduler (one
  `pi.generation` task per turn), submissions (exactly-once input),
  timers, documents, and subagent spawning. It reaches back into
  forge-api over HTTP for tool execution.

They talk over two unix sockets (JSON-lines RPC and one-way events,
both in `FORGE_HARNESS_SOCKET`'s directory):

```
  client                       forge-api                          harness (Node)
    │                              │                                  │
    │  POST /messages              │                                  │
    │  {session_id, content}       │                                  │
    │ ────────────────────────────▶│                                  │
    │                              │ 1. ONE transaction:              │
    │                              │    migration-claim UPDATE        │
    │                              │    + user row INSERT             │
    │                              │    (sequence = get_next_sequence)│
    │                              │                                  │
    │                              │ 2. ensure_migrated:              │
    │                              │    stamped? fast path            │
    │                              │    unstamped? lazy migration     │
    │                              │      createConversation          │
    │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▶│      importEntries (legacy rows) │
    │                              │      stamp sessions              │
    │                              │                                  │
    │                              │ 3. submit (conversation, prompt) │
    │                              │ ───────────────────────────────▶│
    │                              │                                  │ 4. admission commit:
    │  202 Accepted                │                                  │    pi.user entry
    │ ◀────────────────────────────│◀────────────────── (submission)  │    + pi.generation task
    │                              │                                  │
    │                              │                                  │ 5. the turn runs:
    │                              │                                  │    provider calls,
    │                              │  POST /tools/execute             │    tool rounds via
    │                              │ ◀───────────────────────────────│    forge tools
    │                              │  (tool_executor runs the tool,   │
    │                              │   records the result row)       │
    │                              │ ───────────────────────────────▶│
    │                              │                                  │ 6. answer commit:
    │                              │                                  │    pi.assistant entry
    │                              │                                  │    + task terminal
    │                              │                                  │
    │                              │  events socket:                  │
    │                              │◀──────── task_state done ───────│
    │                              │◀──────── turn_end ───────────────│
    │                              │ 7. event consumer:               │
    │                              │    registry.end_turn + bus       │
    │                              │    project_turn_end:             │
    │                              │    assistant row in messages     │
    │                              │    (dedup: durable_projection)   │
    │                              │                                  │
    │  GET /sessions/{id}/events  │                                  │
    │ ───────────────────────────▶│ SSE (bus events)                 │
    │                              │                                  │
```

The client is expected to subscribe to `GET /sessions/{id}/events`
(SSE) for live updates: `message` events (user + projected assistant
rows) and `turn_ended` / `document_changed` / `subagent_ended` marks.
Polling `GET /messages?session_id=…` remains equivalent for
stateless clients (the CLI's `message ask` polls).

## 2. Module map

| Module | What it owns |
|---|---|
| `main.rs` | Build `AppState`, run migrations, start axum, spawn cleanup / metrics background tasks |
| `lib.rs` | Module declarations, public error type |
| `api/mod.rs` | HTTP handlers; **`dispatch_message`** — claim + user-row insert in one transaction, ensure-migrated, `submit`, 202 |
| `api/auth.rs` | Register / login / API key middleware |
| `api/sse.rs` | `/tools/execute/stream` and the streaming bash path |
| `api/messages.rs` | `POST /messages` → `dispatch_message`; `GET /messages` |
| `api/sessions.rs` | session CRUD, reset/interrupt/compact, timers, documents — all writes ensure-migrated first |
| `api/openai.rs` | OpenAI-compatible surface; `run_agent_turn` = ensure-migrated + submit + wait-for-projected-text |
| `api/admin.rs` | admin endpoints incl. session replay (now: ensure-migrated + durable entry count) |
| `db/` | SQLx row types (`Message`, `Profile`, `Session`, `User`, `ApiKey`, …) |
| `harness.rs` | `HarnessState` (client + kill switch), `attach_harness_conversation`, `conversation_params`, **`handle_event` / `spawn_event_consumer`** — the projection + in-flight registry, `resync_unprojected` |
| `harness_migration.rs` | lazy migration: the atomic claim (migration 021), `ensure_migrated` / `run_migration_claimed`, `import_legacy_messages` (sequence cap + orphan healing + placeholder skipping), `wait_for_stamp` |
| `agent_registry.rs` | slimmed to: the `tool_auth_token` (tool auth), `begin_turn` / `end_turn` / `has_in_flight_turn` marks |
| `tool_executor.rs` | `ToolExecutor` — the tool implementations, plus the per-call timing/recording |
| `recording.rs` | The `ToolRecorder` trait, the `DbToolRecorder` impl, and the row-flattening helper |
| `session_manager.rs` | `/forge/sessions/<id>/` lifecycle; cleanup of inactive sessions after 30 min |
| `sandbox.rs` | systemd-nspawn wrapper. **Currently inactive** — tools run on the host with `in_sandbox=false` |
| `observability.rs` | Request / tool-execution counters, exposed at `/metrics` and `/metrics/prometheus` |
| `logging.rs` | `tracing_subscriber` setup |
| `harness/src/main.ts` (Node) | boots pi-durable's `Harness` on `durable-pg` (scratch schema, `FORGE_HARNESS_SCHEMA`), opens the RPC + events sockets, installs forge extensions per conversation, then `resume()` — self-supervision of every unfinished task |
| `harness/src/ipc.ts` (Node) | the RPC surface: `status`, `createConversation`, `importEntries`, `submit`, `steer`, `abort`, `documentGet/Put`, `timerSet/List/Clear`, `compact`, `reset`, `compactionStatus` |
| `crates/forge-harness-client` (Rust) | the JSON-lines client over both sockets, with redial loops (250 ms → 5 s backoff, forever) |

## 3. The ToolRecorder split

The design idea from the legacy architecture survives unchanged:
**the tool executor is the only place that knows what a tool actually
did.** The harness (via the agent's tool calls) POSTs to
`/tools/execute`; the executor runs the tool and owns the *result*
row (exit code, structured output, duration, timeout). The assistant
*call* rows and the text rows come from the harness's committed
entries through the event consumer's projection (not from RPC event
parsing anymore).

### Why split

- The **harness** commits the canonical transcript (tool-call
  blocks in `pi.assistant` entries, tool results in `pi.tool-result`
  entries). It is the source of truth for what the model said.
- The **executor** is the source of truth for what the tool did. It
  runs the tool and writes the flat `messages` result row used by
  the audit-log readers and the SSE stream.
- The schema knowledge (column names, the `[tool_call:<name>]`
  content marker, the per-session sequence allocator) is encapsulated
  in `recording.rs`.

### The trait

```rust
#[async_trait]
pub trait ToolRecorder: Send + Sync {
    async fn record_call(&self, record: ToolCallRecord) -> Result<(), sqlx::Error>;
    async fn record_result(&self, record: ToolResultRecord) -> Result<(), sqlx::Error>;
}
```

(`ToolCallRecord` / `ToolResultRecord` shapes are unchanged from the
legacy docs — see `recording.rs`.)

### Concurrent writes and `get_next_sequence`

Every `messages` insert (user row, projected assistant row, tool
result row) acquires a per-session sequence number inside its
transaction:

```sql
BEGIN;
SELECT get_next_sequence($session_id);   -- pg_advisory_xact_lock(1, hashtext($session_id))
INSERT INTO messages (...);
COMMIT;
```

The advisory lock serializes concurrent allocations per session.
**If you remove the lock, the unique constraint `(session_id,
sequence)` will fire on concurrent writers.** The migration import
does NOT write `messages` rows (it writes durable entries), so the
only `messages` writers are: user dispatch, assistant projection, and
tool results.

## 4. The harness IPC protocol

Both sockets speak JSON-lines. The RPC socket is request/response
(`{id, method, params}` → `{id, ok, result|error}`); the events
socket is one-way (harness → client) and every (re)connect begins
with a client-side `ResyncRequired` marker — **the event stream has
no replay**.

### RPC methods (harness → see `ipc.ts`)

| Method | Purpose |
|---|---|
| `status` | version, active task count, conversation/timer counts |
| `createConversation` | mint a conversation with model + agent-tool params; returns the id |
| `importEntries` | append a batch of `EntryDraft`s (user/assistant/tool-result) in ONE commit — used by lazy migration |
| `submit` | exactly-once input (`requestId` dedupes per conversation); admits the entry and starts/resumes the generation task |
| `steer` / `abort` | steer a running task / abort it (optionally its tree) |
| `documentGet` / `documentPut` | conversation documents (`forge.*` kinds; e.g. plan/handoff) |
| `timerSet` / `timerList` / `timerClear` | the harness's native timers (`POST /sessions/:id/timers`) |
| `compact` / `reset` / `compactionStatus` | context compaction + conversation reset |

### Events (harness → client)

| Event | forge-api action |
|---|---|
| `task_state { status: started }` | remember conversation→task; `registry.begin_turn(session)` |
| `task_state { status: done/failed/aborted }` | forget it; `registry.end_turn(session)`; bus `turn_ended`; subagent-settled check for child conversations |
| `turn_end { conversationId, entryId }` | **`project_turn_end`** — project the committed `pi.assistant` entry onto `messages` (deduped by `durable_projection`) and publish the bus `message` |
| `document_changed` | bus `document_changed` on the session's SSE stream |
| `timer_fired` | log (the fired turn surfaces as task_state / turn_end) |
| `subagent_spawned` | bus `subagent_spawned` on the parent's stream |

### Projection and resync

The event consumer is the only thing that writes assistant rows.
Because events are lossy on reconnect, `ResyncRequired` triggers
`resync_unprojected`: a scan of the durable schema for `pi.assistant`
entries with no `durable_projection` claim, each projected
idempotently. This is what makes a harness kill -9 (or any
consumer/harness restart pair) lose no assistant rows: whatever
`turn_end` fired while the consumer was down is recovered on
reconnect.

## 5. The audit log

The `messages` table is the flat, auditable projection. It is
written by three paths — user dispatch, assistant projection, and
tool results — and is what `GET /messages`, SSE catch-up, and all the
audit-log SQL in [`TOOL-AUDIT-LOG.md`](TOOL-AUDIT-LOG.md) read. The
canonical transcript (everything, including thinking blocks, tool
round-trips, and compactions) is the durable entries in the harness
schema; `messages` is deliberately a lossy-but-stable projection of
it.

The row shapes, the call/result join on `tool_call_id`, the
per-tool `tool_output` shapes, and the known "sequence is write
order, not call order" quirk are unchanged from the legacy docs.

## 6. Session lifecycle

1. `POST /sessions` inserts the row and (when the harness is up and
   the switch is on) calls `attach_harness_conversation` —
   `createConversation` + stamp (`sessions.durable_conversation_id`).
   **Creation never fails because of the harness**: on any harness
   error the session is created UNSTAMPED.
2. First write on an **unstamped** session (a pre-cutover legacy
   session) triggers the **lazy migration** (below); new sessions
   never take that path.
3. `POST /messages` → `dispatch_message` (see §1) → 202.
4. After 30 minutes of inactivity the session manager removes the
   working directory; the durable conversation is unaffected.

### Lazy migration (H2.6)

Pre-cutover sessions have a `messages` transcript but no durable
conversation. Migration is lazy (on first write touch) and
concurrency-safe:

- **Atomic claim** (migration 021): `UPDATE sessions SET
  harness_migrating = TRUE, harness_migration_at = NOW() WHERE
  durable_conversation_id IS NULL AND (harness_migrating = FALSE OR
  harness_migration_at < NOW() - INTERVAL '10 minutes')`.
  `dispatch_message` runs the claim and the user-row INSERT in ONE
  transaction, so no claim-loser's row can be allocated below the
  winner's — the import's `sequence < winner_row` cap provably
  imports exactly the pre-write transcript.
- The winner: `createConversation` → `importEntries`
  (`messages_to_entries`: user/assistant rows → entries; tool rows
  fold into toolCall blocks + `pi.tool-result`; orphaned calls
  healed; forge-side placeholder rows skipped; the caller's own new
  row excluded by the sequence cap) → stamp. Any failure releases
  the claim; the next write retries.
- Losers poll (250 ms × 120 ≈ 30 s) for the stamp, then 503
  `InProgress`.

### Kill switch

`FORGE_HARNESS_MESSAGES` is a kill switch, not a rollout flag: it
defaults ON. `=0` refuses every write with 503 "harness disabled
(FORGE_HARNESS_MESSAGES=0)" **before the user row lands** — no
claim, no insert, no migration.

## 7. Streaming tool execution

Unchanged: `POST /tools/execute/stream` for bash (the consumer wants
chunks as produced); everything else through `POST /tools/execute`.
The caller is the harness's agent tooling instead of the legacy
extension. See the CLI's `forge tools stream` and the curl example in
[`docs/API.md`](API.md).

## 8. Failure modes and how the design absorbs them

| Failure | What happens |
|---|---|
| Harness process dies mid-turn (kill -9) | pi-durable's open path reconciles `running` → `pending`; `resume()` self-supervises and re-runs the task from its checkpoint. The submission is exactly-once: no duplicate prompt. The event consumer's `ResyncRequired` rescan (`resync_unprojected`) projects any assistant entry whose `turn_end` fired while it was disconnected. |
| forge-api restarts (harness keeps running) | The event consumer redials the events socket; `ResyncRequired` → status log + projection rescan. Learned in-flight marks re-learn from subsequent `task_state` events. |
| Both restart | Covered by the two rows above, composed: durable state is Postgres; the harness reboots on the same schema; the consumer rescans on reconnect. The `dual_kill9_recover_mid_turn` integration test exercises the full combination. |
| Concurrent first writes on a legacy session | Atomic migration claim; losers poll the stamp. The claim self-expires after 10 minutes if the winner crashed mid-migration. |
| Harness unreachable while switch is ON | The user row still lands (202 becomes 503 after the row: "turn was not started; the user message is already recorded"); the session stays unstamped so the next write retries the migration. Session creation is never failed by the harness. |
| Kill switch OFF | Every write is 503 before the row lands; nothing migrates; flipping back on resumes normal operation. |
| Tool call times out | The executor records `timed_out: true` in `tool_output` and `is_error: true` (unchanged). |
| Concurrent writes to messages | `get_next_sequence()` advisory lock serializes per session (unchanged). |
| Database connection drop | Pool retries (sqlx defaults); requests return 500; clients should be idempotent. |

## 9. Memory (H4)

Agent memory: episodic + semantic (beliefs) + org-shared + cross-agent
signals. Schema in migration `022_memory.sql`; store in
`crates/forge-api/src/memory.rs`; read routes in
`crates/forge-api/src/api/memory.rs`; the `memory_remember` tool +
`memory_beliefs` prompt section in the harness (`harness/src/forge-ext.ts`).

### Schema (pgvector, 2560-dim)

All four tables carry `embedding vector(2560)` — **not** the 1024 the
Herd plan sketch assumed: Qwen3-Embedding-4B (the Bifrost endpoint in
`src/embedding.rs`) returns 2560-dim vectors (`EMBEDDING_DIM`). HNSW
cosine indexes (`vector_cosine_ops`) back retrieval.

- `episodes` — one row per reflected turn (H4.2 writes it at turn-end):
  `agent_id`, `conversation_id`, `task_ref`, `summary`, `feedback`,
  `source` (JSON provenance: `{conversation_id, seq_range}`).
- `beliefs` — semantic memory. `scope` is `agent` (private) or `org`
  (shared tier, gated by `memory_acl`); `kind` is
  `preference|fact|procedure|constraint`; `status` is
  `pending|active|forgotten|superseded` (writes land `pending`;
  activation is a reviewed transition, H4.4); `version` bumps on every
  status change; `source_episodes` carries provenance.
- `memory_acl` — `(agent_id, org, access in read|write)`; two agents
  sharing an `org` label are in the same memory org; `read` includes
  the org tier in that agent's reads, `write` lets it contribute
  org-scope beliefs.
- `agent_signals` — cross-agent bus (H4.6): `to_agent` NULL = org
  broadcast; `consumed_by` array makes delivery at-least-once-ish per
  reader.
- `belief_audit` — who/what changed a belief and when
  (`actor`, `change`, `detail`, `at`), version chain per belief.

### Skip-when-no-pgvector

Migration 022 is a PL/pgSQL block: when the `vector` extension cannot
be created (pgvector not installed on that Postgres) it RAISEs a
NOTICE and skips the whole table set, so every other migration and all
other test binaries keep working on a pgvector-less database. The API
routes probe `memory::vector_available` and return **501** when the
tables are absent; `tests/memory_tests.rs` follows the same contract
and skips (with a clear message) when pgvector is unavailable — the
pure-Rust halves of the store (org ACL matching, cosine ranking) are
unit-tested in `src/memory.rs` and always run.

### Read API (H4.3)

All owner-or-admin tenancy-gated (404-not-403, like every agent route):

- `GET /agents/:id/memory/search?q=&k=12&scope=agent|org` — embeds
  `q` (2560-dim; **503** when the embedding endpoint is down), then
  pgvector cosine over the agent's episodes and its ACTIVE beliefs
  (org tier added when `scope=org` and `memory_acl` grants read).
  Every hit carries provenance (`source` / `source_episodes`) + score.
- `GET /agents/:id/memory/beliefs?status=&limit=` — belief rows.
- `GET /agents/:id/memory/beliefs/:bid` — one belief + its
  `belief_audit` chain.

### `memory_remember` tool (H4.2)

Offered by the harness extension **only for agent sessions** (the
`policyAgentId` field in `forge.meta`, the H3.5 field). The tool
relays `POST /agents/:id/memory/beliefs` (owner-scoped by the Bearer
key) and returns `recorded: pending your review`. The server inserts
`beliefs.status = 'pending'` (kind defaults to `preference`,
confidence 0.5), best-effort embedded — an embedding failure stores
the belief unembedded rather than failing the call — and writes the
first `belief_audit` row.

### `memory_beliefs` prompt section (H4.3)

Registered in the pi-durable prompt-section registry (H2.5; alongside
`document_plan` / `document_handoff`, rendered per request by
pi-durable's section-diff). Key is `memory_beliefs` — section keys
match `[a-z][a-z0-9_-]*` in pi-durable, so the plan's
`memory:beliefs` label maps to this. Per turn-start:

1. **confidence pass** — top-15 active beliefs by confidence
   (`GET …/memory/beliefs?status=active&limit=15`; no embedding).
   If this fails or returns nothing the section renders **nothing**
   (omitted ⇒ stable prompt).
2. **retrieval pass** — top-5 beliefs retrieved against the *current
   user message* (read from the conversation context via the
   harness handle) — `GET …/memory/search?q=<message>&k=5`. When the
   embedding endpoint is down this pass 503s and the section
   **degrades to the confidence-only pass** (logged, never fails the
   turn).

Rendered as a compact "What you know about this user" block (retrieved
first, then the confidence-only remainder, deduped). The text is a
pure function of (beliefs, query text), so pi-durable's
section-diff keeps it out of the prompt when unchanged.
