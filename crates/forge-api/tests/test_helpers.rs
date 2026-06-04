//! Test helpers for integration and E2E tests
//!
//! Provides a TestApp wrapper for making HTTP requests to the API
//! with automatic test database setup and teardown.

use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::cors::CorsLayer;

use forge_api::agent_registry::AgentRegistry;
use forge_api::api::{create_router, AppState};
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
    /// Create a new test application with a fresh database
    pub async fn new() -> (Self, String) {
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

        // Create router
        let app = create_router()
            .with_state(state)
            .layer(CorsLayer::permissive());

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
    pub fn get(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::GET, path)
    }

    /// Make a POST request
    pub fn post(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::POST, path)
    }

    /// Make a PATCH request
    pub fn patch(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::PATCH, path)
    }

    /// Make a DELETE request
    pub fn delete(&self, path: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, http::Method::DELETE, path)
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Drop the database synchronously using psql to ensure it completes
        let db_name = self
            .db_url
            .split('/')
            .next_back()
            .unwrap_or("forge_test")
            .split('?')
            .next()
            .unwrap_or("forge_test")
            .to_string();

        if db_name.starts_with("forge_test_") {
            // Write cleanup SQL to temp file
            let sql = format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid();\nDROP DATABASE IF EXISTS \"{}\";",
                db_name, db_name
            );
            let tmpfile = format!("/tmp/cleanup_{}.sql", db_name);
            if std::fs::write(&tmpfile, sql).is_ok() {
                let _ = std::process::Command::new("sudo")
                    .args(["-u", "postgres", "psql", "-f", &tmpfile])
                    .output();
                let _ = std::fs::remove_file(tmpfile);
            }
        }
    }
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
