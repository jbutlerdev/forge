//! Herd H4.1/H4.3: memory schema, store, and read API.
//!
//! Skip-when-no-pgvector contract: the memory tables (migration 022)
//! exist only when the `vector` extension was installable at
//! migration time. Every test in this file (a) skips cleanly when the
//! scratch Postgres is unreachable, and (b) probes
//! `forge_api::memory::vector_available` and skips with a clear
//! message when pgvector is not installed. The pure-Rust halves of the
//! store (org ACL matching, cosine ranking) live in
//! `src/memory.rs`'s unit tests and always run.

mod test_helpers;

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::io::{Read, Write};
use test_helpers::TestApp;
use uuid::Uuid;

const PASSWORD: &str = "password123";

/// The H4 fake embedding endpoint: answers every request with a FIXED
/// 2560-dim vector `[1, 0, 0, …]` regardless of the input text. Enough
/// to exercise "embed + rank + shape response" — the ranking-order
/// property itself is covered by the pure `rank_by_cosine` unit tests.
fn fixed_embedding() -> serde_json::Value {
    let mut v = vec![0f32; forge_api::embedding::EMBEDDING_DIM];
    v[0] = 1.0;
    json!({ "data": [{ "embedding": v }] })
}

/// One shared fake-embeddings listener for the whole test binary
/// (booted lazily from a blocking thread; request bodies are ignored,
/// every connection gets a fixed-embedding 200).
struct FakeEmbeddingsServer {
    url: String,
}

impl FakeEmbeddingsServer {
    fn start() -> &'static std::sync::Arc<Self> {
        static ONCE: std::sync::OnceLock<std::sync::Arc<FakeEmbeddingsServer>> =
            std::sync::OnceLock::new();
        ONCE.get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::channel::<String>();
            std::thread::spawn(move || {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake embeddings port");
                let url = format!("http://{}", listener.local_addr().unwrap());
                tx.send(url).unwrap();
                let body = serde_json::to_vec(&fixed_embedding()).unwrap();
                loop {
                    let (mut stream, _) = match listener.accept() {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
                    // Read only the request HEADERS — the body is
                    // irrelevant (every request gets the same fixed
                    // embedding), and reading it to EOF would block on
                    // the keep-alive socket until the read timeout.
                    let mut buf = [0u8; 4096];
                    let mut acc: Vec<u8> = Vec::new();
                    while !acc.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => {
                                acc.extend_from_slice(&buf[..n]);
                                if acc.len() > 65536 {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let _ = stream
                        .set_write_timeout(Some(std::time::Duration::from_secs(10)));
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(&body);
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
            });
            let url = rx.recv().unwrap();
            std::sync::Arc::new(Self { url })
        })
    }

    /// The `EmbeddingConfig` pointing an app at this fake endpoint.
    fn config(&self) -> forge_api::embedding::EmbeddingConfig {
        forge_api::embedding::EmbeddingConfig {
            embedding_url: self.url.clone(),
            embedding_model: "embeddings/qwen3-embedding-4b".into(),
            embedding_api_key: String::new(),
            reranker_url: String::new(),
            reranker_model: "embeddings/qwen3-reranker-4b".into(),
            reranker_api_key: String::new(),
        }
    }
}

/// Scratch Postgres up? (`TestApp` panics on connect failure, so this
/// probe must run BEFORE `TestApp::with_embedding_config`.)
async fn pg_up() -> bool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://postgres:forge@localhost/postgres")
        .await
        .is_ok()
}

/// (b) vector extension installed in this test database. Skips the
/// test (with a clear eprintln) when pgvector is not installed —
/// the skip-when-no-pgvector contract.
async fn require_vector(app: &TestApp) -> Option<sqlx::PgPool> {
    let pool = app.app_state.db.clone();
    if !forge_api::memory::vector_available(&pool).await {
        eprintln!(
            "SKIP memory test: pgvector (vector extension) is not installed on the scratch Postgres — install it (e.g. `sudo pacman -S pgvector`) and re-run"
        );
        None
    } else {
        Some(pool)
    }
}

/// Register a user; returns (user_id, api_key).
async fn register_user(app: &TestApp, email: &str, name: &str) -> (Uuid, String) {
    let resp = app
        .post("/auth/register")
        .json(&json!({ "email": email, "name": name, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "register {} → 201", email);
    let user_id: Uuid = resp.json::<serde_json::Value>().await.unwrap()["user"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let resp = app
        .post("/auth/login")
        .json(&json!({ "email": email, "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "login {} → 200", email);
    let api_key = resp.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string();
    (user_id, api_key)
}

/// Create an agent owned by `api_key`; returns the agent id.
async fn create_agent(app: &TestApp, api_key: &str, name: &str) -> Uuid {
    let resp = app
        .post("/agents")
        .header("X-API-Key", api_key)
        .json(&json!({ "name": name }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create agent {name} → 201");
    resp.json::<serde_json::Value>().await.unwrap()["agent"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn memory_remember_inserts_pending_belief_and_lists_it() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "memowner@example.com", "Mem Owner").await;
    let agent_id = create_agent(&app, &api_key, "mem-bot").await;

    // memory_remember: the H4.2 tool endpoint.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "content": "user prefers brief replies", "kind": "preference" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "remember → 201");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "recorded");
    assert_eq!(body["note"], "pending your review");
    let belief_id: Uuid = body["belief"]["id"].as_str().unwrap().parse().unwrap();

    // It lands pending, with an audit row for the creation.
    let resp = app
        .get(format!("/agents/{agent_id}/memory/beliefs?status=pending").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let arr: serde_json::Value = resp.json().await.unwrap();
    let beliefs = arr["beliefs"].as_array().unwrap();
    assert_eq!(beliefs.len(), 1);
    assert_eq!(beliefs[0]["content"], "user prefers brief replies");
    assert_eq!(beliefs[0]["kind"], "preference");
    assert_eq!(beliefs[0]["status"], "pending");
    assert_eq!(beliefs[0]["version"], 1);

    // Single-belief route carries the audit trail.
    let resp = app
        .get(format!("/agents/{agent_id}/memory/beliefs/{belief_id}").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let one: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(one["belief"]["id"], belief_id.to_string());
    let audit = one["audit"].as_array().unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0]["actor"], "memory_remember");
    assert_eq!(audit[0]["change"], "created");

    // Default kind is 'preference'.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "content": "the deploy host is 10.0.0.4" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let b2: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(b2["belief"]["kind"], "preference");

    // Store-level status transition: pending → active, version bump +
    // audit row (the H4.4 Keep/Forget path).
    let caller = forge_api::memory::Caller {
        user_id: owner_id,
        is_admin: false,
    };
    let b =
        forge_api::memory::set_status(&pool, &caller, agent_id, belief_id, "active", None, "test")
            .await
            .unwrap();
    assert_eq!(b.status, "active");
    assert_eq!(b.version, 2);
    let audit = forge_api::memory::audit(&pool, &caller, agent_id, belief_id)
        .await
        .unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[0].change, "active");
    assert_eq!(audit[0].actor, "test");
}

#[tokio::test]
async fn memory_search_ranks_beliefs_and_episodes_with_provenance() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "memsearch@example.com", "Mem Search").await;
    let agent_id = create_agent(&app, &api_key, "search-bot").await;
    let caller = forge_api::memory::Caller {
        user_id: owner_id,
        is_admin: false,
    };

    // A belief (activated) + an episode, both carrying the fixed fake
    // embedding → cosine 1.0 against the (same fixed-vector) query.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "content": "always sign off commits with Co-authored-by: Herd" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let bid: Uuid = resp.json::<serde_json::Value>().await.unwrap()["belief"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    forge_api::memory::set_status(&pool, &caller, agent_id, bid, "active", None, "test")
        .await
        .unwrap();

    let source = json!({ "conversation_id": null, "seq_range": [3, 9] });
    forge_api::memory::insert_episode(
        &pool,
        &caller,
        agent_id,
        None,
        None,
        "did X, outcome Y, user feedback Z",
        None,
        Some(vec![1.0; forge_api::embedding::EMBEDDING_DIM]),
        source.clone(),
    )
    .await
    .unwrap();

    // The search: embed "sign-off" (fake endpoint → same fixed
    // vector), cosine over the agent's active beliefs + episodes,
    // provenance out.
    let resp = app
        .get(format!("/agents/{agent_id}/memory/search?q=sign-off&k=5").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "search → 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    let beliefs = body["beliefs"].as_array().unwrap();
    assert_eq!(beliefs.len(), 1);
    assert_eq!(
        beliefs[0]["content"],
        "always sign off commits with Co-authored-by: Herd"
    );
    assert!(
        beliefs[0]["score"].as_f64().unwrap() > 0.99,
        "score: {body}"
    );
    assert!(
        beliefs[0]["source_episodes"].is_array(),
        "provenance: {body}"
    );
    let episodes = body["episodes"].as_array().unwrap();
    assert_eq!(episodes.len(), 1);
    assert_eq!(episodes[0]["summary"], "did X, outcome Y, user feedback Z");
    assert_eq!(
        episodes[0]["source"], source,
        "episode provenance rides through"
    );

    // Tenancy: another user sees a 404 on every memory route (no
    // existence leak).
    let (_other_id, other_key) = register_user(&app, "memother@example.com", "Mem Other").await;
    for path in [
        format!("/agents/{agent_id}/memory/search?q=x"),
        format!("/agents/{agent_id}/memory/beliefs"),
        format!("/agents/{agent_id}/memory/beliefs/{bid}"),
    ] {
        let resp = app
            .get(path.as_str())
            .header("X-API-Key", &other_key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404, "foreign caller: {path}");
    }
}

#[tokio::test]
async fn memory_search_degrades_503_when_embedding_endpoint_is_down() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    // Deterministic empty embedding config (NOT env-dependent): the
    // query cannot be embedded, so the route reports 503 with a clear
    // reason (the prompt-section consumer is the one that degrades to
    // the confidence-only pass; the API surface does not).
    let empty_cfg = forge_api::embedding::EmbeddingConfig {
        embedding_url: String::new(),
        embedding_model: "embeddings/qwen3-embedding-4b".into(),
        embedding_api_key: String::new(),
        reranker_url: String::new(),
        reranker_model: "embeddings/qwen3-reranker-4b".into(),
        reranker_api_key: String::new(),
    };
    let (app, _db_url) = TestApp::with_embedding_config(empty_cfg).await;
    let has_vector = require_vector(&app).await.is_some();

    let (_owner_id, api_key) = register_user(&app, "memdegrade@example.com", "Mem Degrade").await;
    let agent_id = create_agent(&app, &api_key, "degrade-bot").await;
    let resp = app
        .get(format!("/agents/{agent_id}/memory/search?q=x").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    if has_vector {
        // Tables exist; the query simply cannot be embedded.
        assert_eq!(
            resp.status(),
            503,
            "search without embedding endpoint → 503"
        );
        assert!(body["error"].to_string().contains("embedding"));
    } else {
        // No pgvector: the memory surface reports 501 before the
        // embedding attempt ever matters.
        assert_eq!(resp.status(), 501, "search without pgvector → 501");
        assert!(body["error"].to_string().contains("pgvector"));
    }

    // H4.6 route wiring + query parsing (runs in BOTH environments —
    // this file's other H4.6 tests skip on pgvector-less hosts): the
    // episode-scope door parses its new params (a bad `caller_session`
    // UUID is a 400 at the router, not a handler 500), and the
    // `agent_signal` post door reaches the memory gate.
    let bad_uuid = app
        .get(format!(
            "/agents/{agent_id}/memory/search?q=x&scope=episodes&task_ref=t&caller_session=not-a-uuid"
        )
        .as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        bad_uuid.status(),
        400,
        "bad caller_session UUID → 400 at the router"
    );
    let episodes_scope = app
        .get(
            format!(
                "/agents/{agent_id}/memory/search?q=x&scope=episodes&task_ref=t&caller_session={}",
                Uuid::new_v4()
            )
            .as_str(),
        )
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    if has_vector {
        // Tables exist; task_ref + caller_session parsed, then the
        // Tables exist; task_ref + caller_session parsed, then the
        // embedding attempt fails first (this test's app has no
        // embedding endpoint) — the access-rule 404 comes after embed.
        assert_eq!(
            episodes_scope.status(),
            503,
            "episode-scope parses its params, then embed fails → 503"
        );
        let body: serde_json::Value = episodes_scope.json().await.unwrap();
        assert!(body["error"].to_string().contains("embedding"));
    } else {
        assert_eq!(
            episodes_scope.status(),
            501,
            "episode-scope without pgvector → 501"
        );
    }
    let signal_post = app
        .post(format!("/agents/{agent_id}/memory/signals").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "kind": "insight", "payload": { "note": "hi" } }))
        .send()
        .await
        .unwrap();
    if has_vector {
        assert_eq!(
            signal_post.status(),
            201,
            "agent_signal post reaches the store"
        );
        let sig: serde_json::Value = signal_post.json().await.unwrap();
        assert_eq!(sig["recorded"], true);
        assert!(sig["to"].is_null());
    } else {
        assert_eq!(
            signal_post.status(),
            501,
            "agent_signal without pgvector → 501"
        );
    }
    // The unread pull door is routed + owner-gated too.
    let unread = app
        .get(format!("/agents/{agent_id}/memory/signals/unread?limit=10").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    if has_vector {
        assert_eq!(unread.status(), 200);
        let arr: serde_json::Value = unread.json().await.unwrap();
        assert!(arr["signals"].is_array());
    } else {
        assert_eq!(
            unread.status(),
            501,
            "signals/unread without pgvector → 501"
        );
    }
}

#[tokio::test]
async fn signals_unread_and_consumption_with_org_broadcast() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "memsig@example.com", "Mem Sig").await;
    let agent_a = create_agent(&app, &api_key, "sig-a").await;
    let agent_b = create_agent(&app, &api_key, "sig-b").await;
    let caller = forge_api::memory::Caller {
        user_id: owner_id,
        is_admin: false,
    };

    // Same-org pair: both in org "team" (read grants).
    forge_api::memory::acl_grant(&pool, &caller, agent_a, "team", "read")
        .await
        .unwrap();
    forge_api::memory::acl_grant(&pool, &caller, agent_b, "team", "read")
        .await
        .unwrap();
    let acl = forge_api::memory::acl_list(&pool, agent_a).await.unwrap();
    assert_eq!(acl.len(), 1);
    assert_eq!(acl[0].org, "team");
    assert_eq!(acl[0].access, "read");
    assert!(forge_api::memory::org_readable(
        &[("team".to_string(), "read".to_string())],
        &["team".to_string()]
    ));

    // Direct signal a → b: unread for b, empty for a (the sender).
    let s1 = forge_api::memory::insert_signal(
        &pool,
        &caller,
        agent_a,
        Some(agent_b),
        "handoff",
        json!({ "note": "pick this up" }),
        None,
    )
    .await
    .unwrap();
    let unread = forge_api::memory::unread(&pool, &caller, agent_b, None, 50)
        .await
        .unwrap();
    assert_eq!(unread.len(), 1);
    assert_eq!(unread[0].id, s1.id);
    let unread_a = forge_api::memory::unread(&pool, &caller, agent_a, None, 50)
        .await
        .unwrap();
    assert_eq!(unread_a.len(), 0);

    // Kind filter hides non-matching signals.
    let filtered = forge_api::memory::unread(&pool, &caller, agent_b, Some("insight"), 50)
        .await
        .unwrap();
    assert_eq!(filtered.len(), 0);

    // Consumption: once consumed, no longer unread; idempotent.
    forge_api::memory::mark_consumed(&pool, &caller, agent_b, s1.id)
        .await
        .unwrap();
    let after = forge_api::memory::unread(&pool, &caller, agent_b, None, 50)
        .await
        .unwrap();
    assert_eq!(after.len(), 0);
    let again = forge_api::memory::mark_consumed(&pool, &caller, agent_b, s1.id)
        .await
        .unwrap();
    assert_eq!(again.consumed_by, vec![agent_b], "appended exactly once");

    // Org broadcast (to_agent NULL) from a is delivered to b (shared
    // read org).
    let s2 = forge_api::memory::insert_signal(
        &pool,
        &caller,
        agent_a,
        None,
        "insight",
        json!({ "note": "the user likes tabs" }),
        None,
    )
    .await
    .unwrap();
    let unread_b = forge_api::memory::unread(&pool, &caller, agent_b, None, 50)
        .await
        .unwrap();
    assert_eq!(unread_b.len(), 1);
    assert_eq!(unread_b[0].id, s2.id);
}

// ============================================
// H4.2: episode capture at turn end
// ============================================

/// Seed a minimal `durable_entries` table (the durable-pg 001 shape —
/// the scratch test DBs only carry the forge-api migrations) with a
/// two-turn conversation on conversation 42:
///
/// ```text
///   1 pi.user        "Fix the login bug, it returns 500s"
///   2 pi.assistant   (stop) "Fixed the login path."
///   3 pi.user        "Wrong. Instead rotate the tokens with key
///                    sk-abcDEF123456."
///   4 pi.assistant   (toolUse) bash + write toolCalls
///   5 pi.tool-result
///   6 pi.assistant   (toolUse) bash curl with a Bearer secret
///   7 pi.tool-result
///   8 pi.assistant   (stop) "Rotated the tokens."
/// ```
///
/// Capture at entry 8 must slice 3→8, extract the two commands + one
/// file + two explicit-feedback sentences (the second carrying the
/// sk- key, which must be redacted), and insert exactly one episode
/// whose source.seq_range is [3, 8].
async fn seed_capture_conversation(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"CREATE TABLE durable_entries (
               id BIGINT PRIMARY KEY,
               conversation_id BIGINT NOT NULL,
               head BIGINT,
               commit_seq BIGINT NOT NULL,
               record TEXT NOT NULL
           )"#,
    )
    .execute(pool)
    .await
    .expect("seed durable_entries table");

    let entries: &[&str] = &[
        r#"{"kind":"pi.user","model":[{"role":"user","content":"Fix the login bug, it returns 500s"}]}"#,
        r#"{"kind":"pi.assistant","model":[{"role":"assistant","content":[{"type":"text","text":"Fixed the login path."}],"stopReason":"stop"}]}"#,
        r#"{"kind":"pi.user","model":[{"role":"user","content":"Wrong. Instead rotate the tokens with key sk-abcDEF123456."}]}"#,
        r#"{"kind":"pi.assistant","model":[{"role":"assistant","content":[{"type":"toolCall","name":"bash","arguments":{"command":"cargo test --features auth"}},{"type":"toolCall","name":"write","arguments":{"path":"src/auth.rs"}}],"stopReason":"toolUse"}]}"#,
        r#"{"kind":"pi.tool-result","model":[{"role":"toolResult","content":[{"type":"text","text":"ok"}]}]}"#,
        r#"{"kind":"pi.assistant","model":[{"role":"assistant","content":[{"type":"toolCall","name":"bash","arguments":{"command":"curl -H 'Authorization: Bearer sk-live-zzz98765' https://api.test/token"}}],"stopReason":"toolUse"}]}"#,
        r#"{"kind":"pi.tool-result","model":[{"role":"toolResult","content":[{"type":"text","text":"ok"}]}]}"#,
        r#"{"kind":"pi.assistant","model":[{"role":"assistant","content":[{"type":"text","text":"Rotated the tokens."}],"stopReason":"stop"}]}"#,
    ];
    for (i, record) in entries.iter().enumerate() {
        let id = (i as i64) + 1;
        sqlx::query(
            "INSERT INTO durable_entries (id, conversation_id, commit_seq, record) VALUES ($1, 42, $1, $2)",
        )
        .bind(id)
        .bind(*record)
        .execute(pool)
        .await
        .expect("seed entry");
    }
}

#[tokio::test]
async fn episode_capture_produces_one_episode_exactly_once() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (_owner_id, api_key) = register_user(&app, "memcap@example.com", "Mem Cap").await;
    let agent_id = create_agent(&app, &api_key, "cap-bot").await;

    // Session row bound to the agent + durable conversation 42.
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Capture Faux Profile",
            "provider": "faux",
            "model": "faux-1",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(profile_resp.status(), 201, "{}", profile_resp.text());
    let profile_id: Uuid = profile_resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sessions (id, profile_id, agent_id, durable_conversation_id)
         VALUES ($1, $2, $3, 42)",
    )
    .bind(session_id)
    .bind(profile_id)
    .bind(agent_id)
    .execute(&pool)
    .await
    .expect("seed agent session row");

    seed_capture_conversation(&pool).await;

    let models_path = app.models_path.clone(); // does not exist → deterministic
    forge_api::memory_capture::capture_turn(&pool, "public", &models_path, &server.config(), 42, 8)
        .await;

    // Exactly one episode, with agent identity + provenance.
    let row: (Uuid, Uuid, String, Option<String>, i32, String) = sqlx::query_as(
        r#"SELECT agent_id, conversation_id, summary,
                      feedback::text, vector_dims(embedding), source::text
               FROM episodes"#,
    )
    .fetch_one(&pool)
    .await
    .expect("exactly one episode");
    assert_eq!(row.0, agent_id, "agent_id set");
    assert_eq!(row.1, session_id, "conversation_id = the forge session");
    assert_eq!(
        row.2, "Ran 2 commands, touched 1 files, feedback: Wrong",
        "deterministic summary (no router profile in the test DB)"
    );
    let source: serde_json::Value = serde_json::from_str(&row.5).unwrap();
    assert_eq!(
        source["seq_range"],
        json!([3, 8]),
        "seq_range points at the slice"
    );
    assert_eq!(source["conversation_id"], session_id.to_string());
    let feedback: serde_json::Value =
        serde_json::from_str(row.3.as_deref().expect("feedback present")).unwrap();
    assert_eq!(feedback["explicit"].as_array().unwrap().len(), 2);
    assert!(
        feedback["explicit"][1].to_string().contains("sk_***"),
        "sk- key redacted in captured feedback: {feedback}"
    );
    assert_eq!(feedback["implicit_reprompt"], json!(false));
    assert_eq!(
        row.4,
        forge_api::embedding::EMBEDDING_DIM as i32,
        "embedding dim"
    );

    // Exactly-once: redelivered TurnEnd for the same entry → no second row.
    forge_api::memory_capture::capture_turn(&pool, "public", &models_path, &server.config(), 42, 8)
        .await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episodes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "second capture of the same seq range is a no-op");

    // Sessions without an agent: capture is a documented no-op.
    let session2 = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sessions (id, profile_id, durable_conversation_id)
         VALUES ($1, $2, 43)",
    )
    .bind(session2)
    .bind(profile_id)
    .execute(&pool)
    .await
    .expect("seed agent-less session row");
    for (id, record) in [
        (
            9,
            r#"{"kind":"pi.user","model":[{"role":"user","content":"hello"}]}"#,
        ),
        (
            10,
            r#"{"kind":"pi.assistant","model":[{"role":"assistant","content":[{"type":"text","text":"hi"}],"stopReason":"stop"}]}"#,
        ),
    ] {
        sqlx::query(
            "INSERT INTO durable_entries (id, conversation_id, commit_seq, record)
             VALUES ($1, 43, $1, $2)",
        )
        .bind(id)
        .bind(record)
        .execute(&pool)
        .await
        .expect("seed agent-less conversation entry");
    }
    forge_api::memory_capture::capture_turn(
        &pool,
        "public",
        &models_path,
        &server.config(),
        43,
        10,
    )
    .await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episodes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "agent-less session captures nothing");

    // A toolUse-segment TurnEnd must not capture (the turn is in
    // flight; the terminal entry carries the capture).
    forge_api::memory_capture::capture_turn(&pool, "public", &models_path, &server.config(), 42, 4)
        .await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episodes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "toolUse segment captures nothing");
}

// ============================================
// Herd H4.4: belief proposals + memory_review
// approval cards
// ============================================

/// Post a single-belief proposal; returns the per-item result object.
async fn post_proposal(
    app: &TestApp,
    api_key: &str,
    agent_id: Uuid,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs/proposals").as_str())
        .header("X-API-Key", api_key)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "proposals → 200: {resp:?}");
    let v: serde_json::Value = resp.json().await.unwrap();
    v["results"][0].clone()
}

/// Simulate the card push: insert a `memory_review` pending entry on
/// the app's ranch-tool queue (the real push path needs a live SSE
/// consumer; this tests the queue → result → belief transition half).
fn queue_memory_review(app: &TestApp, payload: serde_json::Value) -> String {
    let (tx, _rx) = tokio::sync::oneshot::channel::<forge_api::api::ranch_tools::RanchToolResult>();
    app.app_state
        .ranch_tools
        .insert_meta(Uuid::new_v4(), tx, Some("memory_review".into()), payload)
}

/// Poll the single-belief route until the belief reaches `status`
/// (the card-answer transition runs in a spawned task — bounded wait
/// with a clear failure).
async fn wait_belief_status(
    app: &TestApp,
    api_key: &str,
    agent_id: Uuid,
    belief_id: Uuid,
    status: &str,
) -> serde_json::Value {
    for _ in 0..50 {
        let resp = app
            .get(format!("/agents/{agent_id}/memory/beliefs/{belief_id}").as_str())
            .header("X-API-Key", api_key)
            .send()
            .await
            .unwrap();
        if resp.status() == 200 {
            let one: serde_json::Value = resp.json().await.unwrap();
            if one["belief"]["status"] == status {
                return one;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("belief {belief_id} never reached status {status}");
}

#[tokio::test]
async fn memory_proposals_accept_dedupe_and_audit() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(_pool) = require_vector(&app).await else {
        return;
    };

    let (_owner_id, api_key) = register_user(&app, "propowner@example.com", "Prop Owner").await;
    let agent_id = create_agent(&app, &api_key, "prop-bot").await;

    // Batch: one fresh, one string-duplicate of it (different case +
    // spacing — the string-normalize gate), one distinct fact.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs/proposals").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({
            "actor": "nightly-reflection",
            "beliefs": [
                { "content": "user prefers brief replies",
                  "confidence": 0.8,
                  "rationale": "they asked for shorter output twice",
                  "source_episodes": [] },
                { "content": "  USER   PREFERENCES brief replies ",
                  "rationale": "same thing, different words", },
                { "content": "the deploy host is 10.0.0.4",
                  "kind": "fact" }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    let results = v["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["accepted"], true);
    let first_id: Uuid = results[0]["belief_id"].as_str().unwrap().parse().unwrap();
    // No conversation exists for this agent → the card is NOT pushed
    // (documented degradation; the belief stays pending and visible).
    assert_eq!(results[0]["card_pushed"], false);
    assert_eq!(results[1]["accepted"], false);
    assert!(
        results[1]["rejected_reason"]
            .as_str()
            .unwrap()
            .contains("near-duplicate of belief {first_id}"),
        "string-normalize dedupe: {}",
        results[1]["rejected_reason"]
    );
    assert_eq!(results[2]["accepted"], true);
    assert_eq!(results[2]["card_pushed"], false);

    // Both land pending; the duplicate did NOT.
    let resp = app
        .get(format!("/agents/{agent_id}/memory/beliefs?status=pending").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let arr: serde_json::Value = resp.json().await.unwrap();
    let beliefs = arr["beliefs"].as_array().unwrap();
    assert_eq!(beliefs.len(), 2, "duplicate must not be stored");

    // The response carries rationale + source_episodes.
    let one: serde_json::Value = app
        .get(format!("/agents/{agent_id}/memory/beliefs/{first_id}").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(one["belief"]["content"], "user prefers brief replies");
    assert_eq!(
        one["belief"]["rationale"],
        "they asked for shorter output twice"
    );
    assert_eq!(one["belief"]["confidence"], 0.8);
    assert_eq!(one["belief"]["status"], "pending");
    let audit = one["audit"].as_array().unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0]["actor"], "nightly-reflection");
    assert_eq!(audit[0]["change"], "proposed");
    // The audit detail carries the full proposal.
    assert_eq!(audit[0]["detail"]["content"], "user prefers brief replies");
    assert_eq!(
        audit[0]["detail"]["rationale"],
        "they asked for shorter output twice"
    );
    assert_eq!(audit[0]["detail"]["confidence"], 0.8);

    // Malformed source_episodes reject the item, not the request.
    let bad = post_proposal(
        &app,
        &api_key,
        agent_id,
        json!({ "beliefs": [ { "content": "distinct content", "source_episodes": ["nope"] } ] }),
    )
    .await;
    assert_eq!(bad["accepted"], false);
    assert!(bad["rejected_reason"]
        .as_str()
        .unwrap()
        .contains("invalid source_episodes"));
    // Bad kind rejects too.
    let badkind = post_proposal(
        &app,
        &api_key,
        agent_id,
        json!({ "beliefs": [ { "content": "x", "kind": "nonsense" } ] }),
    )
    .await;
    assert_eq!(badkind["accepted"], false);
}

#[tokio::test]
async fn memory_card_answers_drive_belief_transitions() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(_pool) = require_vector(&app).await else {
        return;
    };

    let (_owner_id, api_key) = register_user(&app, "cardowner@example.com", "Card Owner").await;
    let agent_id = create_agent(&app, &api_key, "card-bot").await;

    // Three pending beliefs, each with a queued memory_review card.
    let mut ids: Vec<Uuid> = Vec::new();
    for content in [
        "user prefers brief replies",
        "deploys go on Fridays",
        "watch the flaky test suite",
    ] {
        let item = post_proposal(
            &app,
            &api_key,
            agent_id,
            json!({ "beliefs": [ { "content": content, "rationale": "test" } ] }),
        )
        .await;
        assert_eq!(item["accepted"], true);
        let bid: Uuid = item["belief_id"].as_str().unwrap().parse().unwrap();
        let id = queue_memory_review(
            &app,
            json!({ "kind": "memory", "agent": agent_id, "belief_id": bid }),
        );
        ids.push(bid);
        // the result POST with the card's queue id (ranchd's door)
        let action = match ids.len() {
            1 => json!({ "action": "keep" }),
            2 => json!({ "action": "forget" }),
            _ => json!({ "action": "edit", "content": "watch the flaky test suite nightly" }),
        };
        let resp = app
            .post(format!("/ranch-tools/{id}/result").as_str())
            .header("X-API-Key", &api_key)
            .json(&json!({ "success": true, "output": action }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "card answer → 200");
    }

    let one = wait_belief_status(&app, &api_key, agent_id, ids[0], "active").await;
    assert_eq!(one["belief"]["version"], 2);
    let audit = one["audit"].as_array().unwrap();
    assert!(audit
        .iter()
        .any(|a| a["change"] == "active" && a["actor"] == "user-card"));

    let two = wait_belief_status(&app, &api_key, agent_id, ids[1], "forgotten").await;
    assert_eq!(two["belief"]["version"], 2);
    let audit = two["audit"].as_array().unwrap();
    assert!(audit
        .iter()
        .any(|a| a["change"] == "forgotten" && a["actor"] == "user-card"));

    // Edit: new content + active + version bump, audit 'edited'.
    let three = wait_belief_status(&app, &api_key, agent_id, ids[2], "active").await;
    assert_eq!(
        three["belief"]["content"],
        "watch the flaky test suite nightly"
    );
    assert_eq!(three["belief"]["version"], 2);
    let audit = three["audit"].as_array().unwrap();
    assert!(audit
        .iter()
        .any(|a| a["change"] == "edited" && a["actor"] == "user-card"));

    // A failed relay (success=false) applies NO transition.
    let bid4: Uuid = post_proposal(
        &app,
        &api_key,
        agent_id,
        json!({ "beliefs": [ { "content": "user likes long answers" } ] }),
    )
    .await["belief_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let id4 = queue_memory_review(
        &app,
        json!({ "kind": "memory", "agent": agent_id, "belief_id": bid4 }),
    );
    app.post(format!("/ranch-tools/{id4}/result").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "success": false, "output": json!({}), "error": "no answer in time" }))
        .send()
        .await
        .unwrap();
    // settled (a short beat past the spawn scheduling window)
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let resp = app
        .get(format!("/agents/{agent_id}/memory/beliefs/{bid4}").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let one: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(one["belief"]["status"], "pending");
    assert_eq!(one["belief"]["version"], 1);
}

#[tokio::test]
async fn memory_keep_forget_rest_endpoints() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(_pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "keepowner@example.com", "Keep Owner").await;
    let agent_id = create_agent(&app, &api_key, "keep-bot").await;

    let bid1: Uuid = post_proposal(
        &app,
        &api_key,
        agent_id,
        json!({ "beliefs": [ { "content": "user prefers brief replies" } ] }),
    )
    .await["belief_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let bid2: Uuid = post_proposal(
        &app,
        &api_key,
        agent_id,
        json!({ "beliefs": [ { "content": "deploys go on Fridays" } ] }),
    )
    .await["belief_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // Keep → active, version bump, audit actor = the approver user.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs/{bid1}/keep").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let one: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(one["belief"]["status"], "active");
    assert_eq!(one["belief"]["version"], 2);

    let resp = app
        .get(format!("/agents/{agent_id}/memory/beliefs/{bid1}").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let one: serde_json::Value = resp.json().await.unwrap();
    let audit = one["audit"].as_array().unwrap();
    assert!(audit
        .iter()
        .any(|a| a["change"] == "active" && a["actor"] == format!("user:{owner_id}")));

    // Forget → forgotten.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs/{bid2}/forget").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let one: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(one["belief"]["status"], "forgotten");

    // Unknown belief → 404.
    let phantom = Uuid::new_v4();
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs/{phantom}/keep").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ------------------------------------------------------------------
// H4.5: watch scan at episode insert → memory_trigger_queue →
// pending/consumed API flow.
// ------------------------------------------------------------------

/// H4.5 acceptance (contract-level): a belief whose `watch` carries
/// `match: "invoice deadline"` queues a trigger when an episode
/// containing that text lands; the 24 h default cooldown suppresses a
/// second episode; the pending/consumed API round-trips for the owner
/// and 404s for everyone else. Skips under the
/// skip-when-no-pgvector contract (like the rest of this file).
#[tokio::test]
async fn memory_watch_triggers_queue_cooldown_and_consume() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "memtrigger@example.com", "Mem Trigger").await;
    let agent_id = create_agent(&app, &api_key, "trigger-bot").await;
    let caller = forge_api::memory::Caller {
        user_id: owner_id,
        is_admin: false,
    };

    // The belief lands through the H4.4a proposals door, which passes
    // the `watch` object through verbatim — the watch-authoring path
    // (one call creates the belief WITH its watch).
    let resp = app
        .post(format!("/agents/{agent_id}/memory/beliefs/proposals").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({
            "beliefs": [{
                "content": "nudge when an invoice deadline is mentioned",
                "kind": "constraint",
                "rationale": "H4.5 acceptance scenario",
                "watch": {
                    "match": "invoice deadline",
                    "cooldown_hours": 24,
                    "wake_id": "wake-1"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "proposals → 200");
    let results = resp.json::<serde_json::Value>().await.unwrap()["results"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(results[0]["accepted"], true, "proposal accepted");
    let belief_id: Uuid = results[0]["belief_id"].as_str().unwrap().parse().unwrap();

    // The watch rides on the row verbatim…
    let belief = forge_api::memory::get(&pool, &caller, agent_id, belief_id)
        .await
        .unwrap();
    assert_eq!(belief.watch.as_ref().unwrap()["match"], "invoice deadline");
    // …and only ACTIVE beliefs are scanned: activate it.
    forge_api::memory::set_status(&pool, &caller, agent_id, belief_id, "active", None, "test")
        .await
        .unwrap();

    // Episode 1 mentions the match text → exactly one queued trigger.
    let ep1 = forge_api::memory::insert_episode(
        &pool,
        &caller,
        agent_id,
        None,
        None,
        "Reviewed accounts: the Invoice Deadline moved to Friday.",
        Some(json!({
            "explicit": ["no — send it before the deadline"],
            "implicit_reprompt": false,
        })),
        None,
        json!({ "conversation_id": null, "seq_range": [1, 2] }),
    )
    .await
    .unwrap();
    let queued = forge_api::memory::scan_watch_triggers(&pool, &caller, agent_id, &ep1)
        .await
        .unwrap();
    assert_eq!(
        queued.len(),
        1,
        "a matching active belief queues one trigger"
    );

    // Cooldown stamp set on the belief.
    let belief = forge_api::memory::get(&pool, &caller, agent_id, belief_id)
        .await
        .unwrap();
    assert!(
        belief.last_triggered_at.is_some(),
        "last_triggered_at stamped"
    );

    // The queue row carries the mule forwarder's full input: belief +
    // watch (verbatim, incl. wake_id) + episode summary + score.
    let rows = forge_api::memory::pending_triggers(&pool, &caller, agent_id, 50)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, queued[0]);
    assert!(rows[0].consumed_at.is_none());
    assert_eq!(rows[0].payload["belief_id"], belief_id.to_string());
    assert_eq!(rows[0].payload["watch"]["wake_id"], "wake-1");
    assert_eq!(rows[0].payload["match_score"], 1.0);
    let summary = rows[0].payload["episode_summary"].as_str().unwrap();
    assert!(
        summary.contains("invoice deadline"),
        "summary in payload: {summary}"
    );

    // Episode 2 within the 24 h cooldown → NO second row.
    let ep2 = forge_api::memory::insert_episode(
        &pool,
        &caller,
        agent_id,
        None,
        None,
        "Follow-up: the invoice deadline slipped again this time.",
        None,
        None,
        json!({ "conversation_id": null, "seq_range": [3, 4] }),
    )
    .await
    .unwrap();
    let queued2 = forge_api::memory::scan_watch_triggers(&pool, &caller, agent_id, &ep2)
        .await
        .unwrap();
    assert!(queued2.is_empty(), "cooldown suppresses the second trigger");
    let rows = forge_api::memory::pending_triggers(&pool, &caller, agent_id, 50)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "queue still holds exactly one row");

    // API flow: pending → consumed → pending empty → re-consumed no-op.
    let resp = app
        .get(format!("/agents/{agent_id}/memory/triggers/pending?limit=50").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "pending → 200");
    let arr: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(arr["triggers"].as_array().unwrap().len(), 1);

    // A different user cannot see this agent's queue (404, not 403).
    let (_other_id, other_key) = register_user(&app, "memtriggerother@example.com", "Other").await;
    let resp = app
        .get(format!("/agents/{agent_id}/memory/triggers/pending").as_str())
        .header("X-API-Key", &other_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "other user is gated out (404)");

    let tid = queued[0];
    let resp = app
        .post(format!("/agents/{agent_id}/memory/triggers/{tid}/consumed").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "consume → 200");
    let one: serde_json::Value = resp.json().await.unwrap();
    assert!(one["trigger"]["consumed_at"].is_string(), "consumed_at set");

    // The queue is empty now.
    let rows = forge_api::memory::pending_triggers(&pool, &caller, agent_id, 50)
        .await
        .unwrap();
    assert!(rows.is_empty(), "consumed row drops out of pending");

    // Re-ACK is an idempotent 200 no-op.
    let resp = app
        .post(format!("/agents/{agent_id}/memory/triggers/{tid}/consumed").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "re-consume → 200 no-op");

    // Unknown trigger id → 404.
    let phantom = Uuid::new_v4();
    let resp = app
        .post(format!("/agents/{agent_id}/memory/triggers/{phantom}/consumed").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown trigger → 404");
}

// ------------------------------------------------------------------
// H4.6: cross-agent signals — `agent_signal` tool door, implicit
// consumption, and delegator-scoped episode access.
// ------------------------------------------------------------------

/// H4.6 acceptance (contract-level): the `agent_signal` tool posts a
/// signal (direct + org broadcast, redacted payload); the recipient's
/// `GET …/memory/signals/unread` renders AND implicitly consumes (a
/// second fetch is empty); the sender does not see its own broadcast;
/// validation + tenancy gates hold. Skips under the
/// skip-when-no-pgvector contract (like the rest of this file).
#[tokio::test]
async fn agent_signal_posts_and_unread_implicitly_consumes() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "memsig2@example.com", "Mem Sig2").await;
    let agent_a = create_agent(&app, &api_key, "sig2-a").await;
    let agent_b = create_agent(&app, &api_key, "sig2-b").await;
    let caller = forge_api::memory::Caller {
        user_id: owner_id,
        is_admin: false,
    };

    // Same memory org (read grants) → the org broadcast reaches b.
    forge_api::memory::acl_grant(&pool, &caller, agent_a, "sig-team", "read")
        .await
        .unwrap();
    forge_api::memory::acl_grant(&pool, &caller, agent_b, "sig-team", "read")
        .await
        .unwrap();

    // Org broadcast (no `to`): recorded, to is null. The payload
    // carries a Bearer secret that must come back redacted.
    let resp = app
        .post(format!("/agents/{agent_a}/memory/signals").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({
            "kind": "insight",
            "payload": { "note": "PO template changed", "secret": "Bearer abcDEF123xyz" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "broadcast signal → 201");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["recorded"], true);
    assert!(body["signal_id"].is_string());
    assert_eq!(body["kind"], "insight");
    assert!(body["to"].is_null(), "absent to ⇒ broadcast");

    // Direct signal a → b with the fake-embedded payload.
    let resp = app
        .post(format!("/agents/{agent_a}/memory/signals").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "kind": "handoff", "to": agent_b.to_string(), "payload": { "note": "pick this up" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "direct signal → 201");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["to"], agent_b.to_string());

    // b's unread: BOTH signals (direct + shared-org broadcast),
    // payload redacted, provenance (from + created_at) rides out.
    let resp = app
        .get(format!("/agents/{agent_b}/memory/signals/unread?limit=10").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "unread → 200");
    let arr: serde_json::Value = resp.json().await.unwrap();
    let signals = arr["signals"].as_array().unwrap();
    assert_eq!(signals.len(), 2, "direct + org broadcast: {arr}");
    let froms: Vec<&str> = signals
        .iter()
        .map(|s| s["from_agent"].as_str().unwrap())
        .collect();
    assert!(froms.iter().all(|f| *f == agent_a.to_string()));
    for s in signals {
        assert!(
            ["insight", "handoff"].contains(&s["kind"].as_str().unwrap()),
            "kind rides out: {s}"
        );
        assert!(s["created_at"].is_string(), "created_at rides out");
        // The secret must have been masked before storage.
        let payload = &s["payload"];
        assert_eq!(
            payload["secret"], "Bearer ***",
            "payload redacted: {payload}"
        );
    }

    // Implicit consumption: the fetch just now marked both consumed —
    // the second fetch is empty.
    let resp = app
        .get(format!("/agents/{agent_b}/memory/signals/unread").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let arr: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        arr["signals"].as_array().unwrap().len(),
        0,
        "second fetch is empty (rendered ⇒ consumed)"
    );

    // The sender does not see its OWN broadcast (no self-delivery).
    let resp = app
        .get(format!("/agents/{agent_a}/memory/signals/unread").as_str())
        .header("X-API-Key", &api_key)
        .send()
        .await
        .unwrap();
    let arr: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        arr["signals"].as_array().unwrap().len(),
        0,
        "no self-broadcast"
    );

    // Validation: bad kind / non-object payload / bad to → 400.
    for body in [
        json!({ "kind": "nonsense", "payload": { "a": 1 } }),
        json!({ "kind": "insight", "payload": "not an object" }),
        json!({ "kind": "insight", "to": "not-a-uuid", "payload": {} }),
    ] {
        let resp = app
            .post(format!("/agents/{agent_a}/memory/signals").as_str())
            .header("X-API-Key", &api_key)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "validation: {body}");
    }

    // Unknown target agent → 404 (no existence leak).
    let resp = app
        .post(format!("/agents/{agent_a}/memory/signals").as_str())
        .header("X-API-Key", &api_key)
        .json(&json!({ "kind": "insight", "to": Uuid::new_v4().to_string(), "payload": {} }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown target → 404");

    // Tenancy: another user is 404-gated out of both routes.
    let (_other_id, other_key) = register_user(&app, "memsig2other@example.com", "Other2").await;
    let resp = app
        .post(format!("/agents/{agent_a}/memory/signals").as_str())
        .header("X-API-Key", &other_key)
        .json(&json!({ "kind": "insight", "payload": {} }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "foreign poster → 404");
    let resp = app
        .get(format!("/agents/{agent_b}/memory/signals/unread").as_str())
        .header("X-API-Key", &other_key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "foreign reader → 404");
}

/// One owner-keyed GET against the app's router (the episode-scope
/// search-door test matrix); the response is an owned concrete type, so
/// no closure-capture lifetime games.
async fn api_get(app: &TestApp, api_key: &str, q: &str) -> test_helpers::Response {
    app.get(q)
        .header("X-API-Key", api_key)
        .send()
        .await
        .unwrap()
}

/// H4.6 acceptance: a subagent (child session, `agent_id` NULL, parent
/// = a delegator session of agent A owned by the owner) may search A's
/// episodes FILTERED to the delegator task (`task_ref`) — summaries +
/// provenance only, nothing transcript-shaped; other task_refs return
/// an empty list (access granted, filter applied); an unrelated
/// session, a session owned by another user, or a missing
/// `task_ref`/`caller_session` → 404/400. Skips under the
/// skip-when-no-pgvector contract.
#[tokio::test]
async fn episode_scope_access_delegator_chain_only() {
    if !pg_up().await {
        eprintln!("SKIP memory test: scratch Postgres is unreachable");
        return;
    }
    let server = FakeEmbeddingsServer::start();
    let (app, _db_url) = TestApp::with_embedding_config(server.config()).await;
    let Some(pool) = require_vector(&app).await else {
        return;
    };

    let (owner_id, api_key) = register_user(&app, "memdeleg@example.com", "Mem Deleg").await;
    let agent_a = create_agent(&app, &api_key, "deleg-bot").await;
    let caller = forge_api::memory::Caller {
        user_id: owner_id,
        is_admin: false,
    };

    // Episodes on two tasks (fixed fake embedding → cosine 1.0).
    let emb = Some(vec![1.0; forge_api::embedding::EMBEDDING_DIM]);
    forge_api::memory::insert_episode(
        &pool,
        &caller,
        agent_a,
        None,
        Some("task-X"),
        "rotated the tokens, verified the endpoint",
        None,
        emb.clone(),
        json!({ "conversation_id": null, "seq_range": [1, 4] }),
    )
    .await
    .unwrap();
    forge_api::memory::insert_episode(
        &pool,
        &caller,
        agent_a,
        None,
        Some("task-Y"),
        "rearranged the deploy pipeline",
        None,
        emb.clone(),
        json!({ "conversation_id": null, "seq_range": [5, 9] }),
    )
    .await
    .unwrap();

    // Sessions: the delegator session (agent A, owner) + its subagent
    // child (agent_id NULL — the H2.2 child-row shape) + a session
    // resolved to agent A whose `user_id` is NULL (owner unverifiable
    // ⇒ no access).
    let profile_resp = app
        .post("/profiles")
        .header("X-API-Key", &api_key)
        .json(&json!({
            "name": "Deleg Faux Profile",
            "provider": "faux",
            "model": "faux-1",
            "working_dir": "/tmp/session-test"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(profile_resp.status(), 201);
    let profile_id: Uuid = profile_resp.json::<serde_json::Value>().await.unwrap()["profile"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let parent_sid = Uuid::new_v4();
    sqlx::query("INSERT INTO sessions (id, profile_id, agent_id, user_id) VALUES ($1, $2, $3, $4)")
        .bind(parent_sid)
        .bind(profile_id)
        .bind(agent_a)
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("seed delegator session");
    let child_sid = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sessions (id, profile_id, user_id, parent_session_id) VALUES ($1, $2, $3, $4)",
    )
    .bind(child_sid)
    .bind(profile_id)
    .bind(owner_id)
    .bind(parent_sid)
    .execute(&pool)
    .await
    .expect("seed subagent child session");
    let foreign_sid = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sessions (id, profile_id, agent_id, user_id) VALUES ($1, $2, $3, NULL)",
    )
    .bind(foreign_sid)
    .bind(profile_id)
    .bind(agent_a)
    .execute(&pool)
    .await
    .expect("seed owner-unverifiable session");

    // The subagent caller gets agent A's task-X episodes (summaries +
    // provenance only).
    let q = format!(
        "/agents/{agent_a}/memory/search?q=rotate&scope=episodes&task_ref=task-X&caller_session={child_sid}"
    );
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 200, "delegator chain → 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["scope"], "episodes");
    assert_eq!(body["task_ref"], "task-X");
    let episodes = body["episodes"].as_array().unwrap();
    assert_eq!(episodes.len(), 1, "task-X only: {body}");
    assert_eq!(
        episodes[0]["summary"],
        "rotated the tokens, verified the endpoint"
    );
    assert_eq!(episodes[0]["task_ref"], "task-X");
    assert!(
        episodes[0]["source"]["seq_range"].is_array(),
        "provenance rides out"
    );
    assert!(episodes[0]["score"].as_f64().unwrap() > 0.99);
    // Summaries-only contract: the serialized response must not carry
    // any transcript-shaped field (tool input, commands, raw entries).
    let serialized = body.to_string();
    for leak in [
        "tool_input",
        "command",
        "toolCall",
        "pi.assistant",
        "pi.user",
    ] {
        assert!(
            !serialized.contains(leak),
            "no transcript leak ({leak}): {serialized}"
        );
    }

    // The filter is task equality: a task-Y query is granted but
    // returns ONLY task-Y rows (the X episode above does not leak in
    // the other direction either).
    let q = format!(
        "/agents/{agent_a}/memory/search?q=rotate&scope=episodes&task_ref=task-Y&caller_session={child_sid}"
    );
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 200, "task-Y access granted");
    let body: serde_json::Value = resp.json().await.unwrap();
    let episodes = body["episodes"].as_array().unwrap();
    assert_eq!(episodes.len(), 1, "task-Y only: {body}");
    assert_eq!(episodes[0]["summary"], "rearranged the deploy pipeline");

    // An UNRELATED task_ref is an empty list (not an error).
    let q = format!(
        "/agents/{agent_a}/memory/search?q=rotate&scope=episodes&task_ref=task-NONE&caller_session={child_sid}"
    );
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["episodes"].as_array().unwrap().is_empty());

    // No task_ref → 400 (the episode door requires it).
    let q = format!(
        "/agents/{agent_a}/memory/search?q=rotate&scope=episodes&caller_session={child_sid}"
    );
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 400, "no task_ref → 400");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].to_string().contains("task_ref required"));

    // No caller_session → 404 (the rule cannot be proven; no leak).
    let q = format!("/agents/{agent_a}/memory/search?q=rotate&scope=episodes&task_ref=task-X");
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 404, "no caller_session → 404");

    // A session of another user resolved to agent A → 404.
    let q = format!(
        "/agents/{agent_a}/memory/search?q=rotate&scope=episodes&task_ref=task-X&caller_session={foreign_sid}"
    );
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 404, "owner-unverifiable session → 404");

    // A nonexistent caller session → 404 (the chain walk finds nothing).
    let q = format!(
        "/agents/{agent_a}/memory/search?q=rotate&scope=episodes&task_ref=task-X&caller_session={}",
        Uuid::new_v4()
    );
    let resp = api_get(&app, &api_key, &q).await;
    assert_eq!(resp.status(), 404, "unknown session → 404");
}
