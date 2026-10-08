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
