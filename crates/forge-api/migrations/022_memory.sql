-- Herd H4.1: agent memory — episodes, beliefs, org ACL, cross-agent
-- signals, and the belief audit trail (introduced here with the rest of
-- the memory schema; H4.4's reflection loop writes its proposals/audit
-- against it).
--
-- Requires pgvector >= 0.5 (the `vector` extension) for the embedding
-- columns and HNSW cosine indexes. The dimension is 2560 (Qwen3-Embedding-4B
-- via `crates/forge-api/src/embedding.rs` — `EMBEDDING_DIM`), NOT the
-- 1024 the Herd plan sketch assumed.
--
-- When the extension cannot be created (pgvector not installed on this
-- Postgres), the whole migration is a NOTICE no-op: the memory feature
-- is unavailable (API routes return 501; tests follow the
-- skip-when-no-pgvector contract in `tests/memory_tests.rs`). Real
-- deployments install pgvector (e.g. `sudo pacman -S pgvector`); on
-- those databases this migration creates everything below.

DO $mem$
DECLARE
    v_vector_available BOOLEAN;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        v_vector_available := TRUE;
    ELSIF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
        CREATE EXTENSION vector;
        v_vector_available := TRUE;
    ELSE
        RAISE NOTICE 'pgvector (vector) extension not installed; skipping H4 memory tables — install pgvector to enable agent memory';
        RETURN;
    END IF;

    -- Episodic memory: one row per reflected conversation turn (H4.2
    -- writes these at turn-end; `source` carries the entry seq-range
    -- provenance back into the conversation).
    CREATE TABLE episodes (
        id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        agent_id       UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        conversation_id UUID REFERENCES sessions(id) ON DELETE SET NULL,
        task_ref       TEXT,
        summary        TEXT NOT NULL,
        feedback       JSONB,
        embedding      vector(2560),
        source         JSONB NOT NULL,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX episodes_agent_time ON episodes(agent_id, created_at DESC);
    -- Vector index: HNSW where the pgvector build allows it. pgvector >= 0.8
    -- caps HNSW (and IVFFlat) at 2000 dimensions; our embedding is 2560-dim, so
    -- on those builds this falls back to NO index — retrieval is a filtered
    -- sequential `<=>` scan (every search carries an agent_id filter first),
    -- which is fine at personal-herd scale. Documented per the H4.1 decision
    -- that the embedding dim is set by the model, not the index.
    DO $$
    BEGIN
        BEGIN
            CREATE INDEX episodes_embedding ON episodes USING hnsw (embedding vector_cosine_ops);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'no vector index on episodes (pgvector dim limit); retrieval = filtered sequential scan';
        END;
    END $$;

    -- Semantic memory: beliefs (preferences/facts/procedures/constraints).
    -- status: pending (awaiting human review) → active → forgotten |
    -- superseded (by `superseded_by`). Writes land in `pending` first —
    -- memory is only ever activated through review (H4.4).
    CREATE TABLE beliefs (
        id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        agent_id         UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        scope            TEXT NOT NULL DEFAULT 'agent' CHECK (scope IN ('agent','org')),
        kind             TEXT NOT NULL,
        content          TEXT NOT NULL,
        embedding        vector(2560),
        confidence       REAL NOT NULL DEFAULT 0.5,
        source_episodes  UUID[] NOT NULL DEFAULT '{}',
        watch            JSONB,
        status           TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','active','forgotten','superseded')),
        superseded_by    UUID REFERENCES beliefs(id),
        version          INT NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX beliefs_agent_active ON beliefs(agent_id) WHERE status = 'active';
    DO $$
    BEGIN
        BEGIN
            CREATE INDEX beliefs_embedding ON beliefs USING hnsw (embedding vector_cosine_ops);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'no vector index on beliefs (pgvector dim limit); retrieval = filtered sequential scan';
        END;
    END $$;

    -- Org/shared memory access. `org` is a free-form label: two agents
    -- sharing a label are in the same memory org; a 'read' grant lets
    -- that agent's memory scope include the org tier, a 'write' grant
    -- lets it contribute org-scope beliefs (H4.4 reflection writes only
    -- via a write grant).
    CREATE TABLE memory_acl (
        agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        org      TEXT NOT NULL,
        access   TEXT NOT NULL CHECK (access IN ('read','write')),
        PRIMARY KEY (agent_id, org)
    );

    -- Cross-agent signal bus (H4.6). to_agent NULL = org broadcast:
    -- deliverable to every agent in the sender's orgs with a read
    -- grant.
    CREATE TABLE agent_signals (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        from_agent UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        to_agent   UUID REFERENCES agents(id) ON DELETE CASCADE,
        kind   TEXT NOT NULL CHECK (kind IN ('handoff','insight','request','watch')),
        payload JSONB NOT NULL,
        embedding vector(2560),
        consumed_by UUID[] NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX agent_signals_to ON agent_signals(to_agent, created_at DESC);
    CREATE INDEX agent_signals_from ON agent_signals(from_agent, created_at DESC);
    DO $$
    BEGIN
        BEGIN
            CREATE INDEX agent_signals_embedding ON agent_signals USING hnsw (embedding vector_cosine_ops);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'no vector index on agent_signals (pgvector dim limit); retrieval = filtered sequential scan';
        END;
    END $$;

    -- Who/what proposed or changed each belief, with the version chain
    -- (H4.4: "belief changes are reviewable forever").
    CREATE TABLE belief_audit (
        id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        agent_id  UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        belief_id UUID NOT NULL REFERENCES beliefs(id) ON DELETE CASCADE,
        actor     TEXT NOT NULL,
        change    TEXT NOT NULL,
        detail    JSONB,
        at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX belief_audit_agent ON belief_audit(agent_id, at DESC);
    CREATE INDEX belief_audit_belief ON belief_audit(belief_id, at DESC);
END
$mem$;
