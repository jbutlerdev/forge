-- Session embeddings for semantic message routing.
--
-- Each session gets an LLM-generated 1-2 sentence summary (capturing
-- the conversation's topic, not just the last message), embedded via
-- Qwen3-Embedding-4B (2560-dim). pgvector is NOT installed on this
-- host, so the vector is stored as REAL[] and cosine similarity is
-- computed in-app (Rust). Session counts are small (dozens to low
-- hundreds), so in-app cosine over 2560-dim vectors is cheap.
--
-- The semantic router (POST /router/message) uses this table for
-- two-stage retrieval: embed the incoming message, cosine-retrieve
-- the top-K sessions, rerank with Qwen3-Reranker-4B, and route to
-- the best match (or start a new conversation if none score "yes").
--
-- `summary` is the text that was embedded — kept so we can (a) show
-- it in the UI later, (b) regenerate the embedding if we switch
-- models, and (c) pass it to the reranker without a join.
-- `message_count` is the number of messages in the session when the
-- summary was last regenerated, so we know when to refresh it (e.g.
-- every 10 messages, or when the topic shifts).

CREATE TABLE IF NOT EXISTS session_embeddings (
    session_id  UUID        NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    embedding   REAL[]      NOT NULL,
    summary     TEXT        NOT NULL,
    message_count INTEGER   NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id)
);

-- Index for fast lookup by session_id (the router loads all rows and
-- computes cosine in-app; this index is for the upsert/refresh path).
CREATE INDEX IF NOT EXISTS idx_session_embeddings_session_id
    ON session_embeddings(session_id);
