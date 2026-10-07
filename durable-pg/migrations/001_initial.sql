-- durable-pg: Postgres Storage backend for @earendil-works/pi-durable.
--
-- Mirrors the reference SQLite schema (vendor/pi-durable
-- packages/durable/src/storage/sqlite/migrations.ts) 1:1, with Postgres
-- types. Record payloads are stored as JSON text (the harness owns their shape — the
-- indexed columns exist for scans/queries only). The `durable_` prefix
-- namespaces the tables inside the shared `forge` database.
--
-- (durable_schema itself is created by PgStorage.applyMigrations before
-- migration 001 runs; it tracks the applied version.)

-- ID model (pi-durable semantics): ONE global numeric namespace across
-- conversations/entries/tasks/submissions/documents — enforced by
-- durable_record_ids + durable_metadata.next_id. Sequences (commits) are
-- a separate counter, durable_metadata.next_seq, bumped once per commit.

-- Single-row allocator: next_id = next candidate record ID (mintId);
-- next_seq = next commit sequence (commit). Read + advance inside the
-- commit transaction (SELECT ... FOR UPDATE), single row → no gaps
-- beyond aborted commits, and mintId's exactness comes from the same row.
CREATE TABLE durable_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_id TEXT NOT NULL,          -- TEXT: safe past 2^53 (cf. sqlite schema note)
    next_seq BIGINT NOT NULL
);
INSERT INTO durable_metadata (singleton, next_id, next_seq) VALUES (1, '2', 1);

-- Global ID ownership: one row per claimed record ID + its type. The
-- concurrency rules (conversation/entry/document ids are exclusive;
-- task/submission may share an id across types but not within) are
-- enforced in storage code inside the commit transaction; the PK gives
-- the race-free anchor.
CREATE TABLE durable_record_ids (
    id BIGINT PRIMARY KEY,
    record_type TEXT NOT NULL CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document'))
);

CREATE TABLE durable_conversations (
    id BIGINT PRIMARY KEY,
    owner_conversation_id BIGINT,
    owner_task_id BIGINT,
    record TEXT NOT NULL
);
CREATE INDEX durable_conversations_by_owner_conversation ON durable_conversations (owner_conversation_id, id);
CREATE INDEX durable_conversations_by_owner_task ON durable_conversations (owner_task_id, id);

CREATE TABLE durable_entries (
    id BIGINT PRIMARY KEY,
    conversation_id BIGINT NOT NULL,
    head BIGINT,
    commit_seq BIGINT NOT NULL,
    record TEXT NOT NULL
);
CREATE INDEX durable_entries_by_conversation ON durable_entries (conversation_id, id DESC);
CREATE INDEX durable_entry_heads_by_conversation ON durable_entries (conversation_id, id DESC) WHERE head IS NOT NULL;

CREATE TABLE durable_tasks (
    id BIGINT PRIMARY KEY,
    conversation_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
    abort_requested BOOLEAN NOT NULL,
    background BOOLEAN NOT NULL,
    record TEXT NOT NULL
);
CREATE INDEX durable_tasks_by_status ON durable_tasks (status, id);
CREATE INDEX durable_tasks_by_conversation ON durable_tasks (conversation_id, id);
CREATE INDEX durable_tasks_by_kind ON durable_tasks (kind, id);
CREATE INDEX durable_tasks_by_abort_requested ON durable_tasks (abort_requested, id);
CREATE INDEX durable_tasks_by_background ON durable_tasks (background, id);

CREATE TABLE durable_submissions (
    id BIGINT PRIMARY KEY,
    conversation_id BIGINT NOT NULL,
    request_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
    record TEXT NOT NULL
);
-- The exactly-once dedup key (submissionByRequest): conversation-scoped
-- request ids. Plain (non-unique) index mirrors sqlite; dedup admission
-- is decided by the harness inside the commit transaction.
CREATE INDEX durable_submissions_by_request ON durable_submissions (conversation_id, request_id);
CREATE INDEX durable_submissions_by_conversation ON durable_submissions (conversation_id, id);
CREATE INDEX durable_submissions_by_status ON durable_submissions (status, id);

CREATE TABLE durable_documents (
    id BIGINT PRIMARY KEY,
    kind TEXT NOT NULL,
    family INTEGER NOT NULL CHECK (family IN (0, 1)),
    key_value TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
    owner_id BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    retired_at BIGINT,
    record TEXT NOT NULL
);
CREATE INDEX durable_documents_by_address
    ON durable_documents (kind, scope_kind, owner_id, family, key_value, created_at DESC, retired_at);
CREATE INDEX durable_documents_by_scope ON durable_documents (scope_kind, owner_id, id);
CREATE INDEX durable_documents_by_scope_kind ON durable_documents (scope_kind, owner_id, kind, id);

CREATE TABLE durable_document_revisions (
    document_id BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    PRIMARY KEY (document_id, seq)
);
CREATE INDEX durable_document_revisions_by_kind ON durable_document_revisions (document_id, kind, seq DESC);
