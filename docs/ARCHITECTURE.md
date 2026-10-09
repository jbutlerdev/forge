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
  broadcast; `kind` ∈ `handoff|insight|request|watch`; `consumed_by`
  array records which agents have seen the signal (implicit
  consumption, H4.6 — see below).
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

### Episode capture at turn end (H4.2)

Trigger: the harness event consumer's turn-end projection
(`crates/forge-api/src/harness.rs` `project_turn_end`) spawns
`memory_capture::capture_turn` fire-and-forget, next to
`refresh_session_summary` — the same contract: any error is a
`tracing::warn!` inside the task; the turn never blocks or fails.
Sessions without an agent (`sessions.agent_id IS NULL`) are a
documented **no-op** (episodes are agent memory).

The capture (`crates/forge-api/src/memory_capture.rs`):

1. **Slice** — the turn's durable entries: from the most recent
   `pi.user` entry at or before the turn-end entry (the submitted
   prompt) to the turn-end `pi.assistant` entry. Provenance is
   `source: {conversation_id: <session id>, seq_range: [first, last]}`
   using `durable_entries.id` (monotonic per conversation). Only a
   *terminal* entry captures (`stopReason != "toolUse"` — a
   toolUse segment is still in flight; its terminal entry fires its
   own TurnEnd).
2. **Deterministic pass** (pure, unit-tested —
   `extract_turn_facts`): commands run (bash/shell toolCall
   `command` args, ≤ 10, each ≤ 200 chars), files touched
   (write/edit/apply-patch-style toolCall paths, ≤ 10, deduped),
   explicit user feedback (prompt sentences matching the
   negation/override patterns `no|wrong|actually|instead|I wanted`,
   ≤ 5 sentences), and an implicit-feedback flag (the submitted
   prompt is a re-send/leading edit of the previous user prompt —
   v1 heuristic, `looks_like_resend`).
3. **Summary** — cheap-model second pass: one LLM call through the
   `message-router` profile (the same in-process provider resolution
   as the session-summary refresh; never a subprocess — the
   turn-end context does not own a pi harness). When the profile is
   absent or the endpoint fails, the deterministic one-liner
   `Ran N commands, touched M files, feedback: <explicit or none>`.
   **Documented follow-up:** a dedicated cheap-model profile
   (`episode-summarizer`) so the router's model choice does not steer
   episode wording; v1 reuses the router profile to avoid a new
   surface.
4. **Redaction** (`memory::redact`) — masks `sk_…`/`sk-…` API keys,
   `Authorization: …` / `Bearer …` tokens, `password=`/`token=`/
   `secret=`/`api_key=`-style assignments, and URL-embedded
   credentials, applied to *every* captured text (summary, feedback
   sentences, commands, files) before it reaches a prompt, an
   embedding, or the row. Idempotent; reusable by H4.4/H5.
5. **Embed** — summary + explicit feedback through
   `embedding::embed`; on endpoint failure the row is still inserted
   with a NULL embedding (B-tree time-ranked only, invisible to
   cosine retrieval — the H4.1 contract).
6. **Exactly-once** — before inserting, `episodes` is checked for an
   existing row of the same agent + session whose
   `source.seq_range` *overlaps* this slice (JSONB containment +
   numeric range overlap — no schema constraint; deliberately
   simple). A redelivered `TurnEnd` (or a `ResyncRequired` rescan)
   is a no-op; the upstream `durable_projection` claim in
   `project_turn_end` already serializes the common duplicate case.

`tests/memory_tests.rs::episode_capture_produces_one_episode_exactly_once`
exercises the full path against the scratch Postgres (seeded
`durable_entries`, one episode, provenance, 2560-dim embedding,
redacted feedback, second-capture no-op, agent-less no-op) — under
the skip-when-no-pgvector contract like the rest of the file.

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

### Memory triggers (H4.5)

A belief can carry a `watch` JSONB predicate —
`{match: "text", cooldown_hours: 24, wake_id: "<mule wake UUID>"}`.
When the H4.2 capture task inserts an episode, it runs
`memory::scan_watch_triggers` in the same task: the agent's **active**
beliefs with a non-null `watch` are checked, and a hit past the
cooldown inserts one `memory_trigger_queue` row **and** stamps
`beliefs.last_triggered_at = NOW()` in a single transaction.

**v1 predicate is text only.** `watch.match` is a case-insensitive
substring match against the episode summary + the explicit user-feedback
sentences (`match_score = 1.0` on any hit). No embedding cosine in v1 —
the `match_score` column and payload field carry the hook for the
embedding-predicate follow-up. Watch authoring: the H4.4a
`POST …/memory/beliefs/proposals` endpoint accepts `watch?` per belief
and stores it verbatim (one call creates the belief WITH its watch);
`memory_remember` does not (user-instructed memory is inert).

**Delivery is HTTP pull, not `LISTEN/NOTIFY` (documented deviation).**
The Herd plan sketch had mule poll a forge-written DB table on a
shared cluster; in the lab forge and mule are separate containers with
separate Postgres instances, so the v1 surface is:

- `GET /agents/:id/memory/triggers/pending?limit=50` — unconsumed rows,
  oldest first (owner-gated like every memory route; 501 when the
  memory tables are absent);
- `POST /agents/:id/memory/triggers/:tid/consumed` — the mule lane's
  ACK; **idempotent** (a redelivered ACK is a 200 no-op).

The queue row's `payload` carries the forwarder's full input:
`{belief_id, belief_content, watch_match, watch (verbatim — including
`wake_id` and `cooldown_hours`), episode_id, episode_summary
(truncated to 300 chars), match_score}`.

The mule host's forwarder lane (`internal/wake/memory_trigger.go`)
polls each configured agent's queue every 30 s (config:
`FORGE_TRIGGER_BASE` / `FORGE_TRIGGER_KEY` / `FORGE_TRIGGER_AGENTS`),
resolves `watch.wake_id` against its own `wakes` table, fires it
in-process via the scheduler's `FireWake` (NOT the HTTP fire ingress),
and ACKs. Rows are **at-least-once**, not exactly-once: a fire that
submitted but whose ACK failed (or a mule crash between fire and ACK)
re-polls and re-fires — `FireWake` is fire-and-forget per H3.3, so the
retry cost is a duplicate message to the target, bounded by the wake's
`rate_limit_per_hour` guardrail. A wake that is rate-limited IS
consumed (the guardrail deliberately dropped it; re-polling every 30 s
would just re-trigger the limiter). Rows without a `wake_id` (or whose
wake id is unknown to this mule) are consumed with a warn log — a
misconfigured belief must not wedge the queue. Rows older than 6 h are
skipped the same way (stale).

`memory_trigger_queue` (migration 024) has no vector column, so it is
created **unconditionally** — only the `belief_id` FK and
`beliefs.last_triggered_at` ride the guarded block, since `beliefs`
itself is absent on pgvector-less databases (022 bundled the whole
memory schema behind the extension).

### Cross-agent signals (H4.6)

Two surfaces, one table: the `agent_signal` tool (sender) and the
`memory_signals` prompt section (recipient pull).

**`agent_signal` tool** — offered by the harness extension for agent
sessions, like `memory_remember` (PLAN-HERD §H4.6: "on every forge
agent"). `agent_signal(to?, kind, payload)`: `kind` ∈
`handoff|insight|request|watch`; `payload` is a JSON object; `to`
absent/null = **org broadcast**. The tool relays
`POST /agents/:id/memory/signals` (owner-gated, 404-not-403 tenancy;
restricted keys 403; `to` must be an agent UUID the key may access).
Server-side, the payload's string fields pass through `memory::redact`
(a cross-agent payload may carry secrets from the sender's context),
the redacted JSON is embedded best-effort (a down endpoint stores the
signal unembedded, matching the `memory_remember` contract), and
`agent_signals` gets one row. Response: `{recorded: true, signal_id,
kind, to}`.

**Delivery is pull, not push (documented deviation).** The plan's
`NOTIFY` push leg is the H5 wake row (kind `agent_signal`) and is not
sent in v1 — nothing listens on the channel yet, and the acceptance
only requires the recipient's *next turn* to see the signal. The
recipient's `memory_signals` prompt section fetches on every turn.

**`memory_signals` prompt section + implicit consumption** —
registered in the pi-durable section registry next to `memory_beliefs`
(key `memory_signals`; section keys match `[a-z][a-z0-9_-]*`, so the
plan's `memory:signals` label maps to this). Per turn it calls
`GET /agents/:id/memory/signals/unread?limit=10`, which does
render+consume **atomically in one statement**: the agent's unread
signals (direct + org broadcast, `FOR UPDATE`), marked
`consumed_by += agent`, returned. **Rendered ⇒ consumed**: a signal is
shown to a given agent at most once, and two concurrent turns of the
same agent cannot render the same signal (the statement locks the
rows). **A turn whose fetch fails (404/501/503/transport) consumes
nothing** — the section renders nothing, the turn proceeds, and the
signals stay unread for the next successful turn. The section is
omitted when empty (stable-prompt rule, same as `memory_beliefs`).
Rendered as a compact block:
`Signals for you (cross-agent; each shown once — act on them):` then
one line per signal: `- [insight] from <from_agent>: <payload summary,
≤ 200 chars>` (the summary prefers a single-string
`note`/`summary`/`text`/`message` payload field, else the JSON).

**Org-broadcast visibility.** A `to_agent IS NULL` signal is visible
to every agent that shares a **read-granted** memory org with the
sender (the existing `memory_acl` plumbing: `org_readable` / the
broadcast subquery in `memory::unread_and_consume` — the same rule the
H4.3 org-tier belief search uses). An agent never sees its OWN
broadcast. True multi-org ACL semantics ride on `memory_acl` grants;
the "visible to all agents of the same owner" v1 simplification was
NOT taken — the org-ACL path was already implemented and cheaper.

**Delegator-scoped episode access (H4.6)** — `GET
/agents/:id/memory/search` gains `scope=episodes`, which requires both
`task_ref` (400 "task_ref required for episode-scope access" without
it) and `caller_session` (a session UUID; missing ⇒ 404 — the rule
cannot be proven, and the 404 leaks nothing). The access rule: walk
`sessions.parent_session_id` from `caller_session` (itself included,
≤ 5 parent links; missing row or cycle ends the walk) and grant access
when ANY session in the chain is a session of agent `:id`'s **owner**
resolved to that agent — `sessions.agent_id = :id` AND
`sessions.user_id = agents.owner_id`. This is exactly the H2.2
subagent shape (the child row is minted WITHOUT `agent_id`; its
parent's `agent_id` is the delegator) and the mule-spawned-conversation
shape (a session of the agent itself). `user_id IS NULL` (owner
unverifiable) grants nothing; everyone else gets 404 (no existence
leak, matching the tenancy style). The result is the agent's
episodes filtered to `task_ref` equality, ranked by cosine against the
embedded query — **summaries + `source` provenance only, never raw
text**: the `episodes` table stores no transcript (H4.2 writes the
deterministic summary + redacted feedback + `source.seq_range`), so
the door structurally cannot leak tool inputs, command text, or
conversation entries — dots' "keep only the information needed for
the task" rule (PLAN-HERD §6.3: a subagent gets the delegator's
task-relevant experience, not the delegator's conversations).

## 10. Proactive research (H5.1)

A research task is a durable harness conversation whose tool surface is
**read-only BY CONSTRUCTION**: the model never sees a write tool it
could call. The forge-side record is the `agent_research` table
(migration 025); the lifecycle and card flow live in
`crates/forge-api/src/api/research.rs`.

### Structural enforcement (the filtered registry)

pi-durable extensions are per-CONVERSATION: each conversation's agent
config names its extension instances, and the registry snapshot
resolves those names per process. `spawnResearch` (`harness/src/ipc.ts`)
installs a dedicated `forge-ext-*` instance pinned to
`tools: ["read"]` + `research: true` + `subagent: false`. That flag
adds ONLY `webfetch`, `search`, and `note`; the write-class tools
(`bash`/`write`/`edit`), `spawn_subagent`, and the memory tools
(`memory_remember`/`agent_signal` — which ride on `policyAgentId`,
absent here) are not in the instance at all. The registry the model
receives is therefore exactly `{read, webfetch, search, note}` — a
blocking allowlist hook would still mean the tool was OFFERED; here it
is not. Filtering is per TASK: an ordinary conversation in the same
process keeps its full surface.

Boot re-install (`reinstallConversationExtensions`) rebuilds research
instances from the conversation's `forge.meta` document (`research:
true` + the pinned `tools` subset), so a restarted harness keeps the
filtered registry. `extensionTools` RPC (telemetry) returns the live
tool list for a conversation — the acceptance tests assert the
absence through it, not through the harness's own claim.

### The webfetch / search / note tools

`webfetch` and `search` (`harness/src/webfetch.ts` + `forge-ext.ts`)
are in-process GET-only tools:

- **Method locked to GET** — `httpGet` never sends anything else.
- **No credential forwarding** — no `Authorization`/`Cookie` headers
  ever attached (the forge API key stays inside the harness); URLs with
  embedded `user:password@` are refused.
- **SSRF guard** (`assertFetchable`) — only `http:`/`https:` schemes;
  `localhost`, IP literals, and names whose DNS resolution includes
  ANY loopback / private / link-local / multicast / reserved address
  are refused (covers the cloud metadata endpoint
  `169.254.169.254`); a name resolving to one public AND one private
  address is refused too.
- **Redirects not followed** — a 3xx is reported with its `Location`
  instead of chased, so a public redirector cannot launder a fetch to
  a private target.
- **Bounded** — 30 s timeout, 32 KB body cap (truncation flagged in
  the tool output).

`search` is the SearXNG HTTP API over the same guarded GET path
(`FORGE_SEARCH_INSTANCE`, optional `FORGE_SEARCH_API_KEY`) — the
sandbox SearXNG CLI is a `bash` tool, which research tasks do not
have. `note` appends to the conversation's `research_notes` document
(the H2.5 document surface, readable by every client via
`GET /sessions/:id/documents`); it is NOT replay-safe, so a
crash-rerun does not double-append. The task's standing instructions
(`RESEARCH_TASK_INSTRUCTIONS` in `ipc.ts`) make the model's FINAL
message the report.

### The `agent_research` state machine

```
pending   → POST /agents/:id/research accepted (task submitted in-RPC)
running   → a task_state started event was seen
            (task_id learned — or backfilled at settlement)
done      → the task settled; the `research_report` document landed
            and the suggestion card was pushed
adopted   → the card answered Use      (leaves the open listing)
discarded → the card answered Discard  (leaves the open listing)
```

`open=1` = `state NOT IN ('adopted', 'discarded')`. A `failed`/
`aborted` terminal outcome settles the row to `done` with `resolution`
= the status and NO card (nothing to adopt).

**The race the settle path exists for.** `spawnResearch` admits the
prompt inside the RPC, so both the `started` and the terminal event
can be handled by the event consumer BEFORE the session's
`durable_conversation_id` stamp is committed and before the
`agent_research` row exists. The lossy events socket (no replay)
compounds this. Three re-derivations close it:

1. `complete_research_if_settled` **backfills `task_id`** from the
   first terminal task of the conversation when it settles (the
   "learn the task id at start" contract holds even when the
   `started` event raced the stamp).
2. The POST handler re-runs the settle check after the row insert
   (idempotent, state-guarded; a no-op while a task is still live).
3. `resync_unsettled_research` — on every events-socket reconnect,
   alongside `resync_unprojected` (H2.6): unsettled rows whose
   conversation has no live task and at least one terminal task are
   settled as `done`.

### The suggestion card and its timing

When a research task settles `done`, forge lands the
`research_report` document on the research conversation (report text =
the newest `pi.assistant` entry, falling back to the `research_notes`
document, then a placeholder) and pushes a `research_report`
ranch-tool card on the agent's most-active conversation — the SAME
`RanchToolQueue` and SSE door as the H4.4 `memory_review` cards.
ranchd's forge worker renders **Use / Discard / Ask more** through the
existing AgentAsk machinery (ranch-2 `crates/ranch/src/forge.rs`, the
`research_report` lane): ~2 s `AgentAskStatus` polls under a ~55 s
deadline — just under forge's 60 s `RANCH_TOOL_TIMEOUT`, so a slow
human surfaces as a failed relay (the row stays `done`, still open via
`?open=1`) rather than as our socket timing out under forge's.

The answer rides `POST /ranch-tools/:id/result` and applies through
`apply_research_report`:

- **Use** → row `adopted`, `research_resolved` on the bus (and on the
  research conversation's SSE stream).
- **Discard** → row `discarded`, same event.
- **Ask more** → the free text is RE-INJECTED into the research
  conversation as a new user prompt: the user row is persisted (audit),
  published on the bus, and submitted to the harness through the same
  `submit` RPC as H2.1 — the follow-up turn runs under the SAME
  filtered registry (the conversation's agent config is pinned, so a
  follow-up is read-only too). The row goes back to `running` with
  `resolution = "ask_more: …"`; when the follow-up turn settles the
  card is re-issued. If the follow-up submit FAILS (harness down), the
  row stays `done` — the card was consumed but no task runs.
