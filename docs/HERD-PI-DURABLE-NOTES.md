# pi-durable digest (H0.1)

Source of truth: `~/src/forge/vendor/pi-durable/packages/durable/` —
`@earendil-works/pi-durable` v1.0.4, commit `ae92585d` (pin recorded in
`vendor/pi-durable/.pin`). All symbol names below are verified verbatim
against that tree (files: `docs/spec.md`, `src/types.ts`,
`src/harness/types.ts`, `src/session/`, `src/tasks.ts`, `src/documents.ts`,
`src/testing/`). The Postgres backend (H0.2) must implement the `Storage`
interface exactly as written here; `durable-pg` runs
`createStorageConformance()` against it to prove conformance.

## 1. Core IDs and sequences (`src/types.ts`, `src/ids.ts`)

- `Id<Kind, Type>` — erased nominal **number** with a compile-time brand.
  Concrete brands: `ConversationId = Id<"conversation">`,
  `EntryId = Id<"entry">`, `TaskId<Result> = Id<"task", Result>`,
  `SubmissionId = Id<"submission">`, `DocumentId = Id<"document">`.
  **All record kinds share ONE global numeric ID namespace** — the
  conformance suite proves that a task ID cannot collide with an entry ID
  (rejects `ID ${id} already belongs to entry`).
- `Seq` — `number & brand "sequence"`; strictly increasing, gaps
  permitted; assigned by the backend to one atomic commit.
- `ROOT_CONVERSATION_ID = 1` — reserved; `mintId()` returns 2 first.
- `idFromNumber<I>()` / `seqFromNumber()` — apply brands at trusted
  decode boundaries.

## 2. ConversationRecord (immutable)

```ts
type ConversationRecord = {
  readonly id: ConversationId;
  readonly parent?: { readonly conversationId: ConversationId; readonly at: EntryId };
  readonly owner?:  { readonly conversationId: ConversationId; readonly taskId: TaskId };
};
```

- `parent` = fork source + inclusive parent entry through which history
  is inherited. Forks cap ancestry: an entry is visible through a child
  only if it is visible in the parent *and* `id <= parent.at`
  (transitively; spec §2). Storage must answer visibility queries
  (`entry(conversationId, id)`, `scanEntries`, `findLatestHeadMarker`)
  against this cap chain.
- `owner` = creator edge (a task that created the conversation).
  `ConversationOwnership` (creation input):
  `{ kind: "ownerless" } | { kind: "task"; taskId: TaskId }`.
  Owner filters are **indexed** in storage:
  `ConversationQuery = { ownerConversationId?, ownerTaskId?, order? }`.
- Conversations are immutable once committed: a second commit of
  `{ type: "conversation", value: { id: rootId } }` rejects with
  `ID 1 already belongs to conversation`.

## 3. EntryRecord + head-marker semantics

```ts
type EntryRecord = {
  readonly id: EntryId;
  readonly conversationId: ConversationId;
  readonly kind: string;               // app discriminator, e.g. "pi.user"
  readonly model?: readonly Message[]; // pi-ai messages contributed to context
  readonly data?: JsonValue;           // app payload
  readonly head?: EntryId;             // context reset marker
  readonly edits?: readonly ContextEdit[];
  readonly byTaskId?: TaskId;          // stamped when a task appended it
};
type ContextEdit =
  | { target: EntryId; action: "omit"; messages?: never }
  | { target: EntryId; action: "replace"; messages: readonly Message[] };
```

Draft input is `EntryDraft` (same minus `id|conversationId|byTaskId`;
`head` may be the literal `"self"` = the assigned entry's own ID).

**Head-marker semantics** (spec §2.1/§10, exercised hard in the suite):
the active model context of a conversation is the range from the newest
visible entry carrying `head` (the *marker*) through the tail, **lower
bound = the marker's `head` value**, not the marker's own ID. Storage API:

- `findLatestHeadMarker(conversationId, atOrBeforeEntryId?, context)` →
  newest visible marker at/below the inclusive cutoff, or `undefined`.
- `scanEntries(query, limit, cursor?, context)` with
  `EntryQuery = { conversationId, minEntryId?, maxEntryId?, order? }`
  (bounds inclusive, default `order: "descending"`); pages the *visible*
  fork-aware history applying every ancestor cap.
- `entry(id, context)` / `entry(conversationId, id, context)` →
  `{ entry: EntryRecord; commitSeq: Seq } | undefined` — the global
  lookup plus the commit sequence (needed for historical document
  reads). Unknown conversation ID **rejects** ("Unknown conversation"),
  unknown entry returns `undefined`.

Context read recipe (spec §10): marker =
`findLatestHeadMarker(cv, at?)`; entries =
`scanEntries({ conversationId, minEntryId: marker?.head, maxEntryId: at })`.

Built-in entry kinds (`src/entries.ts`, `defineEntry(kind)` returns
`Entry<D>` with an `is()` guard): `pi.user`, `pi.assistant`,
`pi.system`, `pi.tool-result` (data `{ diagnostics: ToolDiagnostic[] }`),
`pi.reset` (always `head: "self"`),
`pi.compaction` (data `{ reason: CompactionReason }`, `head` = first
kept entry).

## 4. TaskRecord / TaskState / TaskOwnership / TaskOptions

```ts
type TaskRecord<I, S, R> = TaskRecordBase<I, R> &
  | { state: Extract<TaskState<S,R>, {status: "pending"|"running"|"waiting"}>;
      memos?: Readonly<Record<string, JsonValue>> }
  | { state: Extract<TaskState<S,R>, {status: "completing"|"terminal"}>;
      memos?: never };

type TaskRecordBase<I, R> = {
  readonly id: TaskId<R>;
  readonly conversationId: ConversationId;
  readonly kind: string;              // registered task definition name
  readonly version: number;           // definition version (migration)
  readonly input: I;                  // original input, kept live & terminal
  readonly owner?: TaskId;            // absent = conversation-owned
  readonly background: boolean;       // see below
  readonly abortRequested: boolean;
  readonly startedAt?: number;        // ms, first change to "running"
  readonly endedAt?: number;          // ms, change to "terminal"
};
```

Task records are **complete replacements** — every `commit` of a task
stamps the whole record (suite case "replaces complete task records").
A task row is a mutable current-value row, unlike immutable
conversations/entries.

```ts
type TaskState<S, R> =
  | { status: "pending";    checkpoint: S }
  | { status: "running";    checkpoint: S }
  | { status: "waiting";    checkpoint: S; on: readonly TaskId[];
      policy: JoinPolicy }                     // JoinPolicy = "failFast"|"allSettled"
  | { status: "completing"; outcome: TaskOutcome<R> }  // no checkpoint!
  | { status: "terminal";   outcome: TaskOutcome<R> };
```

Note: `completing`/`terminal` carry **no checkpoint** — storage must be
able to filter scans by `status`, `kind`, `conversationId`,
`abortRequested`, and `background`
(`TaskQuery = { conversationId?, kind?, status?, abortRequested?,
background?, order? }`).

```ts
type TaskOutcome<R> =
  | { status: "completed"; result: R }
  | { status: "failed";    error: TaskOutcomeError; result?: R }
  | { status: "aborted";   reason?: string;       result?: R }
  | { status: "orphaned";  reason: string }        // definition/migration gone
  | { status: "faulted";   error: TaskOutcomeError };
type TaskOutcomeError = { readonly message: string; readonly detail?: JsonValue };

type TaskOwnership = { readonly kind: "conversation" }
                    | { readonly kind: "task"; readonly taskId: TaskId };
type TaskOptions = {
  readonly ownership: TaskOwnership;   // required
  readonly conversationId?: ConversationId;
  /** conversation-owned only: excluded from ordinary idle waits,
      conversation aborts, and cascades. */
  readonly background?: boolean;
};
```

`RunningTask<I,S,R>` = record narrowed to `status: "running"`.
`TaskDefinition` (`src/tasks.ts`, `defineTask()`) fields:
`name`, `version`, `initial(input): S`,
`phases: { [P in S["phase"]]: PhaseHandler<...> }`,
`abort(task, runtime, context)`, `migrate?(input, checkpoint,
fromVersion)`, `hooks?: H`. `S extends { phase: string }` — the
checkpoint always names its phase.

**Checkpoints**: a task's durable resume point is its `checkpoint: S`,
stored inside the task record and replaced whole on each
`runtime.commit()`. `PhaseHandler<I,P,S,R,H>` receives
`RunningTask<I,P,R>` (phase-narrowed) plus `TaskRuntime` and must commit
progress (`runtime.commit()`) — returning without durable progress
faults the task. `NextTaskState` = running | waiting | terminal.

`TaskRuntime` (per-invocation operations, `src/types.ts`):
`taskId`, `conversationId`, `signal` (AbortSignal), `registry`
(RegistrySnapshot), `agent(context)`, `settings`, `models`,
`env(context)`, `hooks: HookRunner<H>`, `commit(change, context)`,
`memo(name, candidate?, context)` (first-writer-wins, shared with hooks),
`getTask`, `waitForTask` → `SettledTask`, `outcomes(ids)`,
`conversation(id)` → `ConversationHandle`, `entry`, `context(cv, opts?)`,
`now()`, `report(error)`, `sleep(until, context)` — plus
`DocumentObserver`/`DocumentReader`.

`HookRunner<H>` has one method:
`each<K extends keyof H>(name: K, invoke: (handler: NonNullable<H[K]>) =>
void | Promise<void>): Promise<void>` — dispatches to every matching
handler in registry order; a plain throw is reported and the next handler
runs; a signalled error propagates.

## 5. SubmissionRecord + exactly-once dedup

```ts
type SubmissionRecordBase = { id: SubmissionId; conversationId: ConversationId;
                              requestId?: string };   // host dedup key, conversation-scoped
type SubmissionRecord =
  | (base & { type: "input" } &
      ({ status: "queued" }
       | { status: "placed";   entry: EntryId }
       | { status: "done";     entry: EntryId; answer: EntryId }
       | { status: "unanswered"; entry?: EntryId; reason: string; detail?: JsonValue }))
  | (base & { type: "write" } &
      ({ status: "queued" }
       | { status: "done";     entry: EntryId }
       | { status: "unanswered"; reason: string; detail?: JsonValue }));
```

Lifecycle is enforced by Session (`applySubmissionChange`): only a queued
record can be placed (input → `placed`, write → `done`); only a placed
*input* can be answered (`done` + `answer`); a settled record never
changes. Submissions are complete-replacement rows like tasks.

**Dedup**: `storage.submissionByRequest(conversationId, requestId,
context)` and `tx.submissionByRequest(...)` must index
`(conversation_id, request_id)` → submission; the conformance suite
proves the same `requestId` in different conversations are distinct, and
that a status change moves the record between `status` scans
(`SubmissionQuery = { conversationId?, status?, order? }`). In
Postgres: `UNIQUE(conversation_id, request_id)` where `request_id IS
NOT NULL`.

## 6. Documents: DocumentRecord, DocumentSemantics, points

```ts
type DocumentRecord = {
  readonly id: DocumentId;       // incarnation ID, never reused
  readonly kind: string;
  readonly key?: string;         // family member; absent = singleton
  readonly createdAt: Seq;       // stamped by storage
  readonly retiredAt?: Seq;      // absent while current
} & (
  | { scope: { kind: "session" }; history?: never; fork?: never }
  | ({ scope: { kind: "conversation"; conversationId: ConversationId } } &
      ( { history: "latest";     fork: "current" | "initial" }
      | { history: "rewindable"; fork: "asOf"    | "current" | "initial" } ))
  | { scope: { kind: "task"; taskId: TaskId }; history?: never; fork?: never });
```

`DocumentSemantics` (definition-side): the same union minus the concrete
conversation ID — `{ scope: "session" } | LatestConversationSemantics |
RewindableConversationSemantics | { scope: "task" }`. Only conversation
documents have `history`/`fork`; they are product semantics (fork copy
behavior), not optimizations.

- **Incarnations**: create → content → optional retire, one logical
  address = `{ kind, scope, key? }`. At any seq exactly one incarnation
  is current at an address ("already has a current incarnation"
  rejection). `document.retire` + new create at one address in a batch:
  the new one is current at that seq.
- **Points**: `DocumentPoint = Seq | "current"`;
  `findDocument(address, at, context)` = membership resolution;
  `document(id, at, context)` = materialize one specific incarnation
  (never follows a replacement) →
  `StoredDocument = { record; version; value: JsonObject;
  deltasSinceBase }`. Half-open lifetime: `[createdAt, retiredAt)`;
  outside it a rewindable numeric lookup returns `undefined`; a
  current-only ("latest") document *rejects* historical numeric reads
  ("does not retain historical content").
- **Content**: `DocumentContent = { version, kind: "base", value } |
  { version, kind: "delta", ops: Op[] }` (chord deltas). A version
  transition must be a **base** ("version transition requires a base").
  `deltasSinceBase` counts deltas replayed after the selected base.
  Base/delta layout is backend-private.
- **Copies**: `document.copy { record, source: { id, at } }` reads
  committed pre-batch source state; persists one independent complete
  child base at the source's stored version; source kind/key/history/fork
  must match; batch must not create/change/retire the selected source
  (→ `StorageRejected`, error `name === "StorageRejected"`).
  `forkConversation` uses `prepareForkDocumentCopies()`
  (`src/session/forks.ts`): rewindable `fork: "asOf"` docs copy at the
  fork entry's `commitSeq`; `fork: "current"` docs copy at "current".
- **Definition side** (`src/documents.ts`): `DocDefinition<T>` /
  `DocFamilyDefinition<T, I>` = `{ kind, version, initial(),
  migrate?(value, fromVersion), checkpointWhen?(value, ops,
  CheckpointInfo) }` + semantics; `CheckpointInfo = { deltasSinceBase }`
  — `checkpointWhen` returning true stores the change as a full base
  (doc checkpoints). Family `initial(seed)` runs only for absent members.
  `addressId(address)` = stable JSON string
  `JSON.stringify([kind, scope.kind, ownerId, key ?? null])`.
- `scanDocuments(query, limit, cursor?, context)` with
  `DocumentQuery = { scope, at: DocumentPoint, kind? }` — ascending
  incarnation IDs, only incarnations alive at the point in that exact
  scope.

## 7. Storage interface (exact, `src/types.ts`)

```ts
interface Storage {
  commit(writes: readonly StorageWrite[], context: Context): Promise<Seq>;
  mintId<I extends Id<string>>(): Promise<I>;
  conversation(id, context): Promise<ConversationRecord | undefined>;
  scanConversations(query, limit, cursor, context): Promise<Page<ConversationRecord, Cursor>>;
  entry(id, context): Promise<{ entry: EntryRecord; commitSeq: Seq } | undefined>;
  entry(conversationId, id, context): Promise<{ entry: EntryRecord; commitSeq: Seq } | undefined>;
  findLatestHeadMarker(conversationId, atOrBeforeEntryId, context):
    Promise<(EntryRecord & { head: EntryId }) | undefined>;
  scanEntries(query, limit, cursor, context): Promise<Page<EntryRecord, Cursor>>;
  task(id, context): Promise<TaskRecord<JsonValue, JsonValue, JsonValue> | undefined>;
  scanTasks(query, limit, cursor, context): Promise<Page<...TaskRecord, Cursor>>;
  submission(id, context): Promise<SubmissionRecord | undefined>;
  scanSubmissions(query, limit, cursor, context): Promise<Page<SubmissionRecord, Cursor>>;
  submissionByRequest(conversationId, requestId, context): Promise<SubmissionRecord | undefined>;
  findDocument(address: DocumentAddress, at: DocumentPoint, context):
    Promise<DocumentRecord | undefined>;
  document(id, at: DocumentPoint, context): Promise<StoredDocument | undefined>;
  scanDocuments(query, limit, cursor, context): Promise<Page<DocumentRecord, Cursor>>;
  close(context): Promise<void>;
}
```

`StorageWrite` union (the `commit` payload):
`{ type: "conversation" | "entry" | "task" | "submission", value: <record> }`
| `{ type: "document.create", record: DocumentCreate,
content: base-only DocumentContent }`
| `{ type: "document.copy", record: DocumentCreate,
source: DocumentCopySource }`
| `{ type: "document.change", id: DocumentId, content: DocumentContent }`
| `{ type: "document.retire", id: DocumentId }`.

Contract (spec §10, §4, §12):

- `commit` is the only write path; one batch = one durable transaction,
  returns the assigned `Seq`. After resolution, later reads through this
  Storage observe the batch. `Session` serializes commits on its
  mutation line — storage adds no caller-facing mutex (but multi-writer
  Postgres safety still applies across processes).
- Global ID ownership: IDs are never reused across kinds; committing a
  record whose `id` belongs to another kind rejects
  (`ID n already belongs to <kind>`). `mintId` rejects with "ID space is
  exhausted" past `Number.MAX_SAFE_INTEGER`.
- `close()` after which **every** operation rejects ("closed").
- `StorageRejected` (error `name === "StorageRejected"`) = batch
  rejected before any durable effect; Session rolls it back normally.
  Use it only for deterministic `document.copy` failures with guaranteed
  rollback; unknown failures after admission are fatal.
- Detachment: backends must hand back deep copies (chord `copyJson`
  semantics, `omitUndefinedProperties: true`) — the suite mutates inputs
  *and* returned records and re-reads. Prototype-polluting keys
  (`__proto__`, `constructor`, `toString`) must survive as own
  properties without changing object prototypes.
- Cursors: backend-owned JSON objects, round-trip only; a cursor carries
  its scan's order — continuing in another order rejects ("cursor").
  Entry cursors must remain valid across intervening commits (resume
  below the last returned item).

## 8. Session/transaction layer (what storage serves)

`src/session/transaction.ts`: `class Transaction implements Tx` — holds
the Session mutation line; `#write`/`#read` staging; read-after-write
table reads throw `ReadAfterWrite` (document drafts stay usable).
`TransactionScope = { conversationId?, taskId? }` — default target of
`tx.createTask()` and the `byTaskId` stamp for appended entries.
`LoadedDocument = { addressId, record, storedVersion, valueVersion,
deltasSinceBase, tracker }` — per-incarnation tracker cache;
`adopt(seq)` publishes `DocumentCommitChange[]`
(`{ type: "document", record, conversationId, version?, value: JsonObject | null,
ops }` or `{ type: "document.copy", record, conversationId, source }`).
`settleSuccess()` assembles `StorageWrite[]`; owner validation
(`#validateOwners`) cross-checks tasks/conversations before commit.
`src/session/observation.ts` — non-creating watch acquisition
(`WatchHandle<T>`: `value`, `start(listener)`, `stop()`, `closed`);
watch end reasons `stopped | cancelled | session_closed | retired |
listener_error`.

`Tx` surface (inside a `Session.commit`): `conversation/entry/task`
reads, `scanConversations/scanEntries/scanTasks`,
`latestHeadMarker(conversationId)`,
`submissionByRequest`, `createConversation({ ownership })`,
`forkConversation(parentId, at, { ownership })`,
`appendEntry(cvId, EntryDraft)` (or typed with `Entry<D>` token),
`createTask(task, input, options)`, `createSubmission(create)`,
`settleSubmission(id, Settlement)`, `placeSubmission(id, entry)`,
typed `doc(token, ...)` (creating) and `retireDoc(token, ...)`.

Built-in harness kinds a durable store will actually see:
conversations carry `pi.agent` (AgentState), `pi.inbox` (InboxState),
`pi.live` (LiveState), `pi.usage` (UsageState), `pi.provider` documents
(latest/current conversation docs); built-in tasks:
`pi.generation` (run), `pi.tool`, `pi.compaction`
(result `CompactionResult = { entryId?, submissionId? }`). Compaction:
`CompactionPolicy = { enabled, reserveTokens, keepRecentTokens,
backgroundTokens }`; `CompactionReason = "manual" | "threshold" |
"overflow"`; a summary is a `pi.compaction` entry whose `head` is the
first kept entry, delivered through a write submission.
`ProgressPolicy = { partialIntervalMs, outputIntervalMs }` governs how
often run progress commits land — i.e., how much entry traffic a
conversational workload produces per turn.

## 9. Storage conformance suite (`src/testing/`)

- Entry point: `createStorageConformance(options:
  StorageConformanceOptions): readonly StorageConformanceCase[]` from
  `storage-conformance.ts`, or the runner-agnostic
  `registerStorageConformance(runner, name, withStorage)` from
  `runner.ts` (binds `describe`/`it`/`expect` — Vitest/Jest compatible).
- `StorageConformanceOptions = { assertions:
  StorageConformanceAssertions; withStorage:
  StorageConformanceProvider }` where `withStorage: (use:
  (storage: Storage) => Promise<void>) => Promise<void>` — **must create
  a fresh, empty backend and call `use` exactly once per case**, then
  clean up. `assertions` is a minimal shim (`ok`, `strictEqual`,
  `deepEqual`, `partialDeepEqual`, `greaterThan`, `rejects(promise,
  messageIncludes)`) so no test runner is required; `assertions.ts`
  adapts a Vitest/Jest `expect`. `StorageConformanceCase = { name,
  run() }` — 20 cases.
- What it tests (each case name = a contract):
  1. ID-1 root reservation + immutable conversation re-commit rejection.
  2. Atomic mixed-batch commits + full rollback (table writes AND
     secondary indexes) when one write rejects; seq strictly increases
     after rollback.
  3. Detachment of all returned records and stored values (input
     mutation, read mutation).
  4. Prototype-like JSON keys stored losslessly, no prototype pollution.
  5. Out-of-ID-order entry commits indexed correctly; head markers found.
  6. Entry cursors survive newer commits mid-page.
  7. Opaque cursor round-trip (JSON-serializable) for conversation scans.
  8. Indexed owner-edge filters (`ownerConversationId`, `ownerTaskId`,
     conjunctive) with paging.
  9. Deep fork ancestry caps: newest-first and oldest-first scans through
     every ancestor cap, `minEntryId`/`maxEntryId` inclusive bounds,
     cursor order mismatch rejection, historical vs current head
     markers, visibility exclusions on both sides of a fork,
     `commitSeq` accuracy, "Unknown conversation" rejection.
  10. All four table scans in both orders with order-carrying cursors.
  11. Task complete-replacement + filtered/paged task scans
      (status/kind/abortRequested/background).
  12. Task owner edges, `waiting`/`completing` status scans.
  13. `submissionByRequest` per-conversation indexing, replacement,
      status-scan movement.
  14. Write submissions: no input-only lifecycle states.
  15. Rewindable document reconstruction across bases/deltas
      (`document(id, seq)` half-open lifetime, `deltasSinceBase`),
      retire+recreate address handoff.
  16. Long delta tails across a root-replacement op (`["r", value]`).
  17. `document.copy` independence (source later changes/retired),
      `StorageRejected` on copy-while-retiring-same-batch and on
      kind/fork mismatch.
  18. Version transition must be a base; historical numeric reads of
      latest-only docs reject.
  19. Logical-address index independence (session/conversation/task
      singleton vs family, prototype-like keys) + exact-scope
      `scanDocuments` + kind filter.
  20. Document lifecycle failure atomicity; create+retire in one batch =
      empty lifetime; record-table rollback when a document command
      fails; lossless surrogate-pair indexed strings; one global ID
      namespace + "ID space is exhausted"; post-`close` rejection.
- Quirks for the Postgres backend: every read returns detached
  deep-copies; cursors encode the scan order and reject on mismatch;
  entry scans are fork-cap aware (the hard part — visibility is a
  walk up the `ConversationRecord.parent` chain with `at` caps);
  document content (bases/deltas) is backend-private, only the
  incarnation + lifetime + materialized point values are contractual;
  `StorageRejected` must mean "nothing durably happened".

## 10. Proposed `durable_*` table sketch

One single-row sequence/ID allocator; every ID comes from it.

```sql
-- one global monotonic counter (IDs AND commit seqs draw from it,
-- seqs are just the counter value at commit end)
CREATE TABLE durable_seq (last_value BIGINT NOT NULL DEFAULT 0);
-- next: UPDATE durable_seq SET last_value = last_value + 1 RETURNING last_value;

-- immutable; PK id (global namespace)
CREATE TABLE durable_conversations (
  id                 BIGINT PRIMARY KEY,
  owner_conversation BIGINT,          -- nullable
  owner_task         BIGINT,          -- nullable
  parent_conversation BIGINT,         -- nullable (fork source)
  parent_at          BIGINT           -- fork point entry id (with parent)
);
CREATE INDEX ON durable_conversations (owner_conversation);
CREATE INDEX ON durable_conversations (owner_task);

-- immutable; PK id; no updates ever
CREATE TABLE durable_entries (
  id               BIGINT PRIMARY KEY,
  conversation_id  BIGINT NOT NULL,
  kind             TEXT NOT NULL,
  model            JSONB,             -- pi-ai Message[] | null
  data             JSONB,
  head             BIGINT,            -- context marker (rare)
  edits            JSONB,             -- ContextEdit[] | null
  by_task_id       BIGINT,
  commit_seq       BIGINT NOT NULL    -- seq of the commit that wrote it
);
CREATE INDEX ON durable_entries (conversation_id, id DESC);  -- scans
-- ancestor-cap visibility: resolved in SQL via a recursive CTE over
-- durable_conversations.parent_* (+ cap `id <= parent_at` per hop);
-- consider a materialized path/ancestor table (conversation_id,
-- ancestor_conversation_id, ancestor_at) maintained in commit() for
-- O(1) cap lookup at read time.
CREATE INDEX ON durable_entries (id) WHERE head IS NOT NULL;  -- head markers

-- mutable current-value row; whole-record replace per commit
CREATE TABLE durable_tasks (
  id              BIGINT PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  kind             TEXT NOT NULL,
  version          INT  NOT NULL,
  input            JSONB NOT NULL,
  owner            BIGINT,            -- task id | null
  background       BOOLEAN NOT NULL,
  abort_requested  BOOLEAN NOT NULL,
  started_at       BIGINT,
  ended_at         BIGINT,
  state_status     TEXT NOT NULL,     -- pending|running|waiting|completing|terminal
  checkpoint       JSONB,             -- null when completing/terminal
  wait_on          JSONB,             -- TaskId[] when waiting
  wait_policy      TEXT,              -- failFast|allSettled when waiting
  outcome          JSONB,             -- TaskOutcome when completing/terminal
  memos            JSONB             -- null when terminal
);
CREATE INDEX ON durable_tasks (state_status);
CREATE INDEX ON durable_tasks (kind);
CREATE INDEX ON durable_tasks (conversation_id, id);

-- mutable current-value row
CREATE TABLE durable_submissions (
  id              BIGINT PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  request_id       TEXT,
  type             TEXT NOT NULL,     -- input|write
  status           TEXT NOT NULL,     -- queued|placed|done|unanswered
  entry_id         BIGINT,
  answer_entry_id  BIGINT,
  reason           TEXT,
  detail           JSONB
);
CREATE UNIQUE INDEX ON durable_submissions (conversation_id, request_id)
  WHERE request_id IS NOT NULL;       -- exactly-once dedup
CREATE INDEX ON durable_submissions (conversation_id, id);
CREATE INDEX ON durable_submissions (status);

-- document incarnations (one row per incarnation)
CREATE TABLE durable_documents (
  id            BIGINT PRIMARY KEY,
  kind          TEXT NOT NULL,
  scope_kind    TEXT NOT NULL,        -- session|conversation|task
  scope_owner   BIGINT,               -- conversation_id or task_id
  key           TEXT,                 -- null = singleton
  history       TEXT,                 -- latest|rewindable (conversation only)
  fork          TEXT,                 -- current|initial|asOf (conversation only)
  created_at    BIGINT NOT NULL,      -- seq
  retired_at    BIGINT                -- seq | null
);
CREATE UNIQUE INDEX ON durable_documents (kind, scope_kind, scope_owner, key)
  WHERE retired_at IS NULL;           -- one current incarnation per address
CREATE INDEX ON durable_documents (scope_kind, scope_owner);

-- document content: one row per commit touching an incarnation;
-- backend-private layout (chord bases + deltas)
CREATE TABLE durable_document_revisions (
  id          BIGINT PRIMARY KEY,     -- global id (kept in namespace)
  document_id BIGINT NOT NULL,        -- incarnation
  seq         BIGINT NOT NULL,        -- commit seq
  version     INT NOT NULL,
  kind        TEXT NOT NULL,          -- 'base' | 'delta'
  value       JSONB,                  -- bases
  ops         JSONB,                  -- deltas: Op[]
  UNIQUE (document_id, seq)
);
CREATE INDEX ON durable_document_revisions (document_id, seq);
```

`commit()` = one Postgres transaction: allocate seq
(`durable_seq`), apply table writes (conversations/entries INSERT-only;
tasks/submissions UPSERT), apply document commands (create + base row,
change + base/delta row, copy → read pre-batch source then INSERT a base
row, retire → set `retired_at`), validate uniqueness/ownership in the
same transaction, reject as `StorageRejected` (error `name =
"StorageRejected"`) before any effect on failure. Multi-writer safety
across processes (forge-api + harness) comes from Postgres row locks on
`durable_seq` + the per-address unique current-incarnation index.
