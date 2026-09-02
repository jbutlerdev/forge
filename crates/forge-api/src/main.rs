// The binary in `main.rs` re-declares every module of the
// crate (it pre-dates the lib split). The router wires up a
// subset of the public functions each module exposes; the rest
// are reachable as `crate::xxx::...` for the lib's integration
// tests but unused from the binary. Without this allow the
// binary would fail clippy with `dead_code` on every unused
// `pub fn` even though the symbol is used elsewhere.
#![allow(dead_code)]

mod agent_registry;
mod api;
mod bus;
mod db;
mod embedding;
mod logging;
mod observability;
mod pi_agent;
mod recording;
mod resume;
mod sandbox;
mod session_manager;
mod session_replay;
mod tool_executor;

use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::agent_registry::AgentRegistry;
use crate::bus::MessageBus;
use crate::observability::Metrics;
use crate::recording::DbToolRecorder;
use crate::sandbox::SandboxManager;
use crate::session_manager::SessionManager;

const SESSION_TIMEOUT_SECS: i64 = 30 * 60;

/// Wait for a shutdown signal: ctrl-c (SIGINT) or SIGTERM (systemd
/// unit stops / `kill -TERM`). `tokio::signal::ctrl_c()` alone only
/// listens for SIGINT — without an explicit SIGTERM handler, systemd
/// stops fell through to the default disposition and killed the
/// process without draining, orphaning in-flight requests (a tool
/// call mid-execution loses its SSE connection; its pi subprocess
/// stays wedged until the next message or the read timeout).
///
/// The two signal futures are built *before* the `tokio::select!`
/// (per the axum docs' recommended shape) so each listener is
/// registered exactly once instead of being re-created on every
/// select iteration.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

async fn metrics_task(
    metrics: Arc<Metrics>,
    agent_registry: Arc<AgentRegistry>,
    db: sqlx::PgPool,
    mut shutdown: broadcast::Receiver<()>,
) {
    tracing::info!("Metrics task started");
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Ok(count) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sessions WHERE ended_at IS NULL").fetch_one(&db).await {
                    metrics.set_active_sessions(count as u64);
                }
                metrics.set_active_agents(agent_registry.len().await as u64);
            }
            _ = shutdown.recv() => break,
        }
    }
}

async fn cleanup_task(
    session_manager: Arc<SessionManager>,
    agent_registry: Arc<AgentRegistry>,
    sandbox_manager: Arc<SandboxManager>,
    db: sqlx::PgPool,
    mut shutdown: broadcast::Receiver<()>,
) {
    tracing::info!("Cleanup task started");
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(SESSION_TIMEOUT_SECS);
                if let Ok(stale_sessions) = sqlx::query_as::<_, (uuid::Uuid,)>(
                    "SELECT id FROM sessions WHERE ended_at IS NULL AND last_active < $1"
                ).bind(cutoff).fetch_all(&db).await {
                    for (session_id,) in stale_sessions {
                        // The pi subprocess is disposable. The audit
                        // log in the database is the source of
                        // truth for conversation history. When the
                        // user comes back, we re-clone the sandbox
                        // from scratch and replay the prior messages
                        // into a fresh pi via pi's `new_session`
                        // RPC command (with `parentSession`
                        // pointing at a session jsonl we build from
                        // the messages table).
                        //
                        // So: kill the pi, destroy the sandbox,
                        // forget the in-memory agent-registry entry.
                        // The next message for this session id will
                        // see an empty registry, spawn a fresh pi in
                        // a fresh sandbox, replay the prior
                        // conversation from the messages table, and
                        // resume.
                        tracing::info!(
                            session_id = %session_id,
                            "Cleaning up idle session: killing pi and destroying sandbox (durable resume will rebuild from messages table on next message)"
                        );
                        let _ = agent_registry.remove(session_id).await;
                        let _ = session_manager.remove_session(session_id).await;
                        let _ = sandbox_manager.destroy_container(session_id).await;
                        let _ = sqlx::query("UPDATE sessions SET ended_at = NOW() WHERE id = $1")
                            .bind(session_id)
                            .execute(&db)
                            .await;
                    }
                }
            }
            _ = shutdown.recv() => break,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let forge_api_url =
        std::env::var("FORGE_API_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    tracing::info!("Connected to database");

    sqlx::migrate!("./migrations").run(&pool).await?;

    let sandbox_manager = Arc::new(SandboxManager::new());
    if let Err(e) = sandbox_manager.init().await {
        tracing::warn!("Sandbox initialization failed: {}", e);
    }

    let agent_registry = Arc::new(AgentRegistry::new(forge_api_url, sandbox_manager.clone()));

    let session_manager = Arc::new(SessionManager::new());
    if let Err(e) = session_manager.init().await {
        tracing::warn!("Session manager initialization failed: {}", e);
    }

    let metrics = Arc::new(Metrics::new());

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let cleanup_session_manager = session_manager.clone();
    let cleanup_agent_registry = agent_registry.clone();
    let cleanup_sandbox_manager = sandbox_manager.clone();
    let cleanup_pool = pool.clone();

    tokio::spawn(async move {
        cleanup_task(
            cleanup_session_manager,
            cleanup_agent_registry,
            cleanup_sandbox_manager,
            cleanup_pool,
            shutdown_rx,
        )
        .await;
    });

    let metrics_pool = pool.clone();
    let metrics_agents = agent_registry.clone();
    let metrics_metrics = metrics.clone();
    let (metrics_shutdown_tx, metrics_shutdown_rx) = broadcast::channel(1);

    tokio::spawn(async move {
        metrics_task(
            metrics_metrics,
            metrics_agents,
            metrics_pool,
            metrics_shutdown_rx,
        )
        .await;
    });

    let recorder = Arc::new(DbToolRecorder::new(pool.clone()));
    let bus = MessageBus::new();

    let state = api::AppState::new(
        pool,
        session_manager,
        sandbox_manager,
        agent_registry,
        metrics.clone(),
        recorder,
        bus,
    );

    // Assemble the full app: API router + web UI static fallback
    // (if a web dir is resolved) + permissive CORS. Shared with
    // the test harness via `api::build_app` so the assembly isn't
    // duplicated. If no web dir is found, the API is served alone.
    let web_dir = api::resolve_web_dir();
    if let Some(ref d) = web_dir {
        tracing::info!("serving web UI from {:?}", d);
    } else {
        tracing::info!("no web dir found; serving API only (set FORGE_WEB_DIR to enable the UI)");
    }
    let app = api::build_app(state, web_dir);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("Starting server on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let shutdown_server = shutdown_tx.clone();

    // `with_graceful_shutdown` is what actually stops the HTTP
    // server: without it, ctrl-c (or systemd's SIGTERM) only woke
    // the signal handler above, the cleanup task exited, but
    // `axum::serve` kept serving requests forever and `main` never
    // reached the shutdown sends below. The future completes on
    // ctrl-c AND SIGTERM (systemd `kill -TERM` for unit stops);
    // `shutdown_signal` registers both handlers so a systemd stop
    // drains in-flight requests instead of killing them outright.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            tracing::info!("Received shutdown signal; draining HTTP connections");
            let _ = shutdown_server.send(());
        })
        .await?;

    let _ = shutdown_tx.send(());
    let _ = metrics_shutdown_tx.send(());

    tracing::info!("Server shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// The graceful-shutdown future must complete on SIGTERM (the
    /// signal systemd sends on unit stops), not just on ctrl-c.
    /// Previously `with_graceful_shutdown` only awaited
    /// `tokio::signal::ctrl_c()` (SIGINT); a SIGTERM fell through to
    /// the default disposition and killed the process without
    /// draining in-flight requests.
    ///
    /// We poll the future once first so tokio registers the SIGTERM
    /// handler; sending the signal before that would hit the default
    /// disposition and kill the whole test process.
    #[tokio::test]
    async fn shutdown_signal_completes_on_sigterm() {
        let mut signal = std::pin::pin!(crate::shutdown_signal());
        // First poll: creates the `Signal` instances / registers the
        // OS handlers (returns Pending).
        let _ = futures::poll!(&mut signal);

        let pid = std::process::id();
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();
        assert!(status.is_ok(), "failed to send SIGTERM to self");

        tokio::time::timeout(Duration::from_secs(5), &mut signal)
            .await
            .expect("shutdown_signal must complete when SIGTERM arrives");
    }
}
