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
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::agent_registry::AgentRegistry;
use crate::bus::MessageBus;
use crate::observability::Metrics;
use crate::recording::DbToolRecorder;
use crate::sandbox::SandboxManager;
use crate::session_manager::SessionManager;

const SESSION_TIMEOUT_SECS: i64 = 30 * 60;

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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "forge_api=debug,tower_http=debug".into()),
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

    let app = api::create_router()
        .with_state(state)
        .layer(CorsLayer::permissive());

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("Starting server on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let shutdown_server = shutdown_tx.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal");
        let _ = shutdown_server.send(());
    });

    axum::serve(listener, app).await?;

    let _ = shutdown_tx.send(());
    let _ = metrics_shutdown_tx.send(());

    tracing::info!("Server shutdown complete");
    Ok(())
}
