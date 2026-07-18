//! Test helpers for integration and E2E tests
//!
//! Provides a TestApp wrapper for making HTTP requests to the API
//! with automatic test database setup and teardown.

use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use forge_api::agent_registry::AgentRegistry;
use forge_api::api::{build_app, AppState};
use forge_api::observability::Metrics;
use forge_api::sandbox::SandboxManager;
use forge_api::session_manager::SessionManager;

/// Test application wrapper
pub struct TestApp {
    /// Base URL for HTTP requests
    pub base_url: String,
    /// Database connection string (to drop DB after test)
    pub db_url: String,
    /// Handle to shutdown the server
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Keeps the per-test session/sandbox tempdir alive for
    /// the lifetime of the `TestApp`. Without this the
    /// `TempDir` would drop at the end of `new()` and the
    /// session/sandbox managers would be left pointing at a
    /// deleted path. The dir is cleaned up on drop.
    _tmp_root: Option<TempDir>,
}

impl TestApp {
    /// Create a new test application with a fresh database (API only,
    /// no static file serving).
    pub async fn new() -> (Self, String) {
        Self::build(None).await
    }

    /// Create a test application that also serves the web UI from
    /// `web_dir` (the SPA fallback). Used by the web/static-serving
    /// tests in `tests/web_tests.rs`.
    #[allow(dead_code)]
    pub async fn with_web_dir(web_dir: std::path::PathBuf) -> (Self, String) {
        Self::build(Some(web_dir)).await
    }

    async fn build(web_dir: Option<std::path::PathBuf>) -> (Self, String) {
        // Generate unique database name
        let db_name = format!(
            "forge_test_{}",
            uuid::Uuid::new_v4().to_string().replace("-", "")
        );
        let db_url = format!("postgres://postgres:forge@localhost/{}", db_name);

        // First, create the database
        let admin_url = "postgres://postgres:forge@localhost/postgres";
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(admin_url)
            .await
            .expect("Failed to connect to postgres");

        sqlx::query(&format!("CREATE DATABASE {}", db_name))
            .execute(&pool)
            .await
            .expect("Failed to create test database");

        pool.close().await;

        // Connect to the test database
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .expect("Failed to connect to test database");

        // Run migrations
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        // Create shared components
        //
        // We point the session + sandbox managers at a fresh
        // per-test tempdir so we don't need (and can't
        // accidentally clobber) the production `/forge/sessions`
        // and `/forge/sandbox` paths. CI runners also don't
        // have write access to `/forge/`, so this is the only
        // way the suite runs there at all.
        //
        // We pass the path directly to the manager constructors
        // (rather than via FORGE_SESSIONS_DIR / FORGE_SANDBOX_DIR
        // env vars) because env-var injection has a TOCTOU race
        // when tests run in parallel: set_var from test A can be
        // observed by test B's manager construction in between
        // B's set_var and B's manager::new(). Direct
        // construction sidesteps the race entirely.
        let tmp_root = TempDir::new().expect("create tempdir");
        let sessions_dir = tmp_root.path().join("sessions");
        let sandbox_dir = tmp_root.path().join("sandbox");
        std::fs::create_dir_all(&sessions_dir).expect("mkdir sessions");
        std::fs::create_dir_all(&sandbox_dir).expect("mkdir sandbox");

        let session_manager = Arc::new(SessionManager::with_base_path(sessions_dir.clone()));
        let sandbox_manager = Arc::new(SandboxManager::with_base_dir(
            sandbox_dir.clone(),
            sessions_dir.clone(),
        ));
        let agent_registry = Arc::new(AgentRegistry::new(
            "http://localhost:8080/api/v1".to_string(),
            sandbox_manager.clone(),
        ));
        let metrics = Arc::new(Metrics::new());

        // Initialize session manager
        if let Err(e) = session_manager.init().await {
            eprintln!("Session manager init warning: {}", e);
        }

        // Initialize sandbox manager
        if let Err(e) = sandbox_manager.init().await {
            eprintln!("Sandbox manager init warning: {}", e);
        }

        // Create app state
        let recorder: Arc<dyn forge_api::recording::ToolRecorder> =
            Arc::new(forge_api::recording::DbToolRecorder::new(pool.clone()));
        let bus = forge_api::bus::MessageBus::new();
        let state = AppState::new(
            pool,
            session_manager,
            sandbox_manager,
            agent_registry,
            metrics,
            recorder,
            bus,
        );

        // Create router. API-only when `web_dir` is None; with a
        // ServeDir SPA fallback when set (mirrors `main.rs`).
        let app = build_app(state, web_dir);

        // Bind to a random port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port");

        let addr = listener.local_addr().expect("Failed to get local address");
        let base_url = format!("http://{}", addr);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Spawn server
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });

        // Wait a bit for server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let test_app = Self {
            base_url,
            db_url: db_url.clone(),
            shutdown_tx: Some(shutdown_tx),
            _tmp_root: Some(tmp_root),
        };

        (test_app, db_url)
    }

    /// Make a GET request
    #[allow(dead_code)]
    pub fn get(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::GET, path)
    }

    /// Make a POST request
    #[allow(dead_code)]
    pub fn post(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::POST, path)
    }

    /// Make a PATCH request
    #[allow(dead_code)]
    pub fn patch(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::PATCH, path)
    }

    /// Make a DELETE request
    #[allow(dead_code)]
    pub fn delete(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::DELETE, path)
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        // Signal the axum server to shut down.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Drop the per-test database. Previously this shelled out to
        // `sudo -u postgres psql -f <tmpfile>`, which (a) required
        // passwordless sudo to the postgres user, (b) can hang on a
        // TTY-less password prompt (the `AGENTS.md` §2/§3 `sudo` footgun),
        // and (c) narrowed where the suite can run. We now do the
        // cleanup in-process with sqlx against the same admin URL
        // `TestApp::new` already uses to create the DB
        // (`postgres://postgres:forge@localhost/postgres`) -- same
        // trust boundary, no subprocess, no sudo.
        //
        // `Drop` is synchronous and runs inside the test's tokio
        // runtime, so we can't `.await` here (a nested
        // `Runtime::block_on` panics). Run the async cleanup on a
        // dedicated thread with its own one-shot runtime and block on
        // that thread's completion instead.
        let db_name = self
            .db_url
            .split('/')
            .next_back()
            .unwrap_or("forge_test")
            .split('?')
            .next()
            .unwrap_or("forge_test")
            .to_string();

        if !db_name.starts_with("forge_test_") {
            return;
        }

        let admin_url = "postgres://postgres:forge@localhost/postgres".to_string();
        let db_name_for_thread = db_name.clone();
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("TestApp drop: failed to build cleanup runtime: {e}");
                    return;
                }
            };
            rt.block_on(async move {
                drop_test_db(&admin_url, &db_name_for_thread).await;
            });
        })
        .join()
        .ok();
    }
}

/// Terminate any lingering connections to the per-test database and
/// drop it. Best-effort: a failure here just leaks a `forge_test_*`
/// database; it does not affect test correctness. We must terminate
/// other backends first because `DROP DATABASE` fails if anything
/// (including our own just-closed pool) still holds a connection.
async fn drop_test_db(admin_url: &str, db_name: &str) {
    use sqlx::postgres::PgPoolOptions;
    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("TestApp drop: failed to connect to admin DB for cleanup of {db_name}: {e}");
            return;
        }
    };
    // Parameterize the datname filter; the DROP uses an identifier so
    // we quote it (db_name is a uuid-derived string we generated, not
    // user input, so injection isn't a concern, but quoting keeps
    // `psql`-style identifiers happy).
    let _ = sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(db_name)
    .execute(&pool)
    .await;
    let drop_sql = format!("DROP DATABASE IF EXISTS \"{}\"", db_name);
    if let Err(e) = sqlx::query(&drop_sql).execute(&pool).await {
        eprintln!("TestApp drop: failed to drop {db_name}: {e} (leaking test DB)");
    }
    let _ = pool.close().await;
}

/// Request builder for test HTTP requests
pub struct RequestBuilder<'a> {
    app: &'a TestApp,
    method: http::Method,
    path: String,
    headers: Vec<(http::header::HeaderName, String)>,
    body: Option<String>,
}

impl<'a> RequestBuilder<'a> {
    fn new(app: &'a TestApp, method: http::Method, path: &str) -> Self {
        Self {
            app,
            method,
            path: path.to_string(),
            headers: Vec::new(),
            body: None,
        }
    }

    /// Add a header to the request
    pub fn header(mut self, name: &str, value: &str) -> Self {
        let header_name = name.parse().expect("Invalid header name");
        self.headers.push((header_name, value.to_string()));
        self
    }

    /// Set JSON body
    pub fn json<T: serde::Serialize>(mut self, value: &T) -> Self {
        let json = serde_json::to_string(value).expect("Failed to serialize JSON");
        self.body = Some(json);
        self.headers.push((
            http::header::CONTENT_TYPE.clone(),
            "application/json".to_string(),
        ));
        self
    }

    /// Set a raw (string) body. The caller is responsible for
    /// adding a matching `Content-Type` header via `.header(...)`.
    /// Used by the voice-proxy tests to send multipart and
    /// non-JSON bodies that `.json()` can't express.
    #[allow(dead_code)]
    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Send the request
    pub async fn send(self) -> Result<Response, reqwest::Error> {
        let url = format!("{}{}", self.app.base_url, self.path);

        let mut request = reqwest::Client::new().request(self.method.clone(), &url);

        for (name, value) in self.headers {
            request = request.header(name, value);
        }

        if let Some(body) = self.body {
            request = request.body(body);
        }

        let response = request.send().await?;
        Ok(Response {
            status_code: response.status().as_u16(),
            headers: response.headers().clone(),
            body: response.text().await.ok(),
        })
    }
}

/// Response wrapper for tests
#[derive(Debug)]
pub struct Response {
    pub status_code: u16,
    #[allow(dead_code)]
    pub headers: http::HeaderMap,
    pub body: Option<String>,
}

impl Response {
    /// Get the status code
    pub fn status(&self) -> u16 {
        self.status_code
    }

    /// Get the response body as JSON
    pub async fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        match &self.body {
            Some(b) => serde_json::from_str(b),
            None => serde_json::from_str("null"),
        }
    }

    /// Get the response body as text
    #[allow(dead_code)]
    pub fn text(&self) -> String {
        self.body.clone().unwrap_or_default()
    }
}
