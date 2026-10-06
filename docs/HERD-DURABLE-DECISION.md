# Herd Durable-Runtime Decision

**Status: decided.** Decided by the user on **2026-10-06**; this document
is the decision record and needs no further sign-off
(`PLAN-HERD.md` §12 Q1 is answered — "use pi-durable upstream directly —
no Rust reimplementation"). The binding plan it implements is
`~/src/ranch-2/docs/PLAN-HERD.md` — specifically **H0.2** (the Postgres
backend), **H0.3** (this record), and **H2** (the refactor, whose
architecture this document locks).

## Decision

Forge's agent-turn runtime becomes **a Node process running
pi-durable upstream directly** —
`@earendil-works/pi-durable` (from
`github.com/earendil-works/pi`, `packages/durable`) — with a new
**`durable-pg` Postgres `Storage` backend** (H0.2). Rust does **not**
reimplement the harness. The Rust `forge-api` crate becomes the
API/tenancy/SSE/memory/sandbox layer that drives the Node harness over a
unix-socket IPC and reads the `durable_*` tables directly for queries
and SSE.

## Context: what this replaces

Forge today drives agent turns with a hand-rolled pipeline inside
`crates/forge-api`:

- **`api/turn.rs` — `drive_turn`** (L191–238): the turn event loop.
  Holds a per-session mutex, consumes pi-rpc events, writes
  `messages` rows, manages in-flight state. Everything "what happens
  while a turn runs" is bespoke code here.
- **`resume.rs`** (544 lines): filesystem replay of a conversation from
  pi's session JSONL after a restart.
- **`session_replay.rs`** (996 lines): context replay — reconstructing
  the model's context window from stored history.
- **`agent_registry.rs`** (~923 lines of pi-subprocess management):
  spawning `pi --mode rpc` per agent, keepalive, idle-clock, reaping.
  Plus `session_manager.rs`'s 30-minute cleanup and the
  `lib.rs` `cleanup_task` that runs it.

That is a lot of semantics — turn lifecycle, resume, context
reconstruction, process babysitting — maintained by hand, every one of
which pi-durable already implements natively with checkpoint-based
durability. pi-durable's storage interface was designed so a backend
can be swapped (bundled sqlite; our Postgres); the conformance suite
is the porting spec. Adopting it *deletes* complexity rather than
adding a new abstraction on top of the old driver.

## Alternatives considered

1. **Rust reimplementation of the harness.** Port pi-durable's semantic
   model (durable tasks, checkpoints, ownership trees, background
   tasks, hooks, documents, compaction) into Rust. Rejected: it is a
   large, ongoing translation of a moving upstream design; every
   upstream improvement would have to be re-ported; and it keeps the
   dual-writer problem (two implementations agreeing on semantics).
   The only argument for it was single-language operations, which the
   forge image already mixes (pi itself is Node).
2. **Keep the legacy driver.** Continue fixing resume/replay/babysit
   in Rust. Rejected: the Herd features (H2.2 subagents, H2.3 durable
   timers, H2.4 compaction, H2.5 hooks/documents) would each have to
   be hand-built in Rust, and the kill-9-consistency guarantees
   (H2.2 acceptance) would have to be proven per-feature instead of
   inheriting pi-durable's tested semantics.
3. **Adopt pi-durable directly, Node runtime + Postgres backend.**
   Chosen. We write exactly one new load-bearing artifact —
   `durable-pg` (H0.2) — plus thin wiring (IPC server, Rust client);
   all harness semantics are upstream's.

## Boundary (binding for H2)

- **The Node harness process is the only writer of the `durable_*`
  tables.** forge-api (Rust) *reads* them directly for queries and SSE
  (history search `GET /sessions/:id/history?q=`, `GET
  /sessions?parent=…`, the `durable_entries` projection) — but all
  **effects** — new turns, tool results, aborts, forks, timer fires —
  go through the harness via its IPC (the H2.0 unix-socket JSON-RPC:
  `submit`, `steer`, `abort`, `createConversation`, `forkConversation`,
  `documentGet/put`, `timerSet/clear`, plus the harness→forge-api
  event push channel).
- **Tenancy, agents, memory, and profiles stay in Rust-owned
  tables.** `users`/API keys, the H1 `agents` registry, H4 memory +
  pgvector, and profile definitions never move into `durable_*`.
  Tool calls still flow harness extension → forge
  `POST /tools/execute` → sandbox, so the single tenancy/allowlist
  enforcement point survives the refactor.

Rationale: Postgres gives us the multi-writer safety that the
bundled sqlite backend can't — Rust and Node read/write the same DB
directly — but concentrating *writes* in the harness means
pi-durable's checkpoint/transaction invariants are enforced in exactly
one place, with Rust as a pure observer for reads.

## Versioning (binding for H2)

- **pi-durable is pinned.** It is vendored under
  `vendor/pi-durable/` (gitignored clone of
  `github.com/earendil-works/pi`, `packages/durable` only) with the
  commit + package version recorded in `vendor/pi-durable/.pin` —
  vendored per H0.1. The pinned version is `@earendil-works/pi-durable`
  `1.0.4`. *(Note: the vendor clone is being created concurrently by
  H0.1; if `.pin` does not yet exist, treat this section as the record
  of the intent and update it once the pin lands.)* H0.5 freezes the
  pin across all images.
- **The `durable_*` schema is owned by `durable-pg` migrations.**
  Schema changes ride the same deploy as the image rebuild
  (H0.4-style reconcile) — never an ad-hoc `ALTER` on the live
  machine. `durable-pg` is a permanent, committed package
  (`~/src/forge/durable-pg/`), not a throwaway spike.

## Kill criteria (what reopens this decision)

Any of the following reopens the H0.3 decision formally
(`PLAN-HERD.md` risk register, "Harness runtime" row):

1. **The pi-durable conformance suite cannot pass on Postgres.**
   H0.2 runs `packages/durable/src/testing/storage-conformance.ts`
   against `storage-pg.ts`, plus a kill-9 recovery test. If the
   Postgres backend can't satisfy pi-durable's storage contract
   (or only with workarounds that defeat checkpoint atomicity), the
   backend premise fails.
2. **Per-turn IPC overhead makes the voice-loop latency targets
   unreachable.** H6.4's budget is TTS first-chunk < 400 ms and STT
   partials < 800 ms. The RPC hop (forge-api → harness → pi-durable
   → pi) must fit inside that budget; measure it in H2.1/H2.6
   before cutover.
3. **Upstream pi-durable churn breaks us faster than we can pin.**
   If upstream API/semantic churn forces rework of
   `durable-pg`/the harness more often than the pin discipline
   (H0.1/H0.5) absorbs, we re-evaluate — including the rejected
   Rust-reimplementation alternative.

## Consequences for H2

- **New: `~/src/forge/harness/`** — Node harness runtime
  (TypeScript, committed, baked into the forge image). Boots one
  Harness on `storage-pg`, registers forge's pi-durable extensions,
  serves the unix-socket JSON-RPC IPC + event push channel. Runs
  under `forge-harness.service` (`Restart=always`) and self-supervises
  via pi-durable `resume()` on boot.
- **New: `crates/forge-harness-client/`** — thin, sqlx-free Rust
  client: typed wrappers over the IPC socket + reconnect loop;
  `AppState` gains `harness: HarnessClient`.
- **Stays in Rust (unchanged):** tenancy/auth (`api/auth.rs`), the
  HTTP surface, SSE fan-out (`api/sse.rs`, `bus.rs`), memory tables +
  pgvector (H4), sandbox + executor (`sandbox.rs`,
  `tool_executor.rs`), tenancy/allowlist/policy enforcement at
  `/tools/execute`.
- **Deleted (the payoff):** `resume.rs` (544), `session_replay.rs`
  (996 — its reading logic is reused once by the H2.6 lazy
  `messages` → `durable_entries` importer, then removed), the
  pi-subprocess management in `agent_registry.rs` (~900 of its 923
  lines shrink to a small in-flight-turn tracker behind H1.2's
  `GET /agents/:id/active`), `session_manager.rs`'s 30-minute cleanup
  + `lib.rs` `cleanup_task`, and the hand-rolled turn driver
  (`api/turn.rs` `drive_turn*`). pi-durable owns turns, resume,
  compaction, timers, and documents; forge-api loses its harness
  event loop and process babysitting entirely.
- **Cutover (H2.6)** is not a big bang: harness deploys alongside,
  new conversations only, a week of shadow parity, then legacy
  `drive_turn` deletion + lazy migration of existing sessions.

## Cross-references

- `~/src/ranch-2/docs/PLAN-HERD.md` — H0.1 (vendor + pin + notes),
  H0.2 (`durable-pg` + conformance), **H0.3** (this record),
  H0.4/H0.5 (infra + pins), H2.0–H2.6 (implementation), H6.4
  (latency budget), §12 Q1 (the answer this records), risk register
  ("Harness runtime" row — kill-criteria owner).
- `~/src/forge/docs/HERD-PI-DURABLE-NOTES.md` — H0.1 concept notes
  (per-concept type/field mapping the `durable_*` schema and the Rust
  client mirror verbatim).
- `~/src/forge/durable-pg/README.md` (once H0.2 lands) — timing
  numbers vs the sqlite backend and Postgres-awkward semantics.
