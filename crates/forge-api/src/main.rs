mod db;
mod api;
mod pi_agent;
mod session_manager;
mod tool_executor;
mod sandbox;
mod agent_registry;
mod observability;
mod logging;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use sqlx::postgres::PgPoolOptions;

use crate::session_manager::SessionManager;
use crate::sandbox::SandboxManager;
use crate::agent_registry::AgentRegistry;
use crate::observability::Metrics;

/// Session timeout in seconds (30 minutes of inactivity)
const SESSION_TIMEOUT_SECS: i64 = 30 * 60;

/// Background task for updating metrics periodically
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
                // Update active sessions count from database
                match sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM sessions WHERE ended_at IS NULL"
                )
                .fetch_one(&db)
                .await
                {
                    Ok(count) => {
                        metrics.set_active_sessions(count as u64);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to count active sessions: {}", e);
                    }
                }
                
                // Update active agents count
                let agent_count = agent_registry.len().await as u64;
                metrics.set_active_agents(agent_count);
            }
            _ = shutdown.recv() => {
                tracing::info!("Metrics task received shutdown signal");
                break;
            }
        }
    }
}

/// Background task for cleaning up stale sessions
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
                // Find stale sessions (inactive for more than SESSION_TIMEOUT_SECS)
                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(SESSION_TIMEOUT_SECS);
                
                match sqlx::query_as::<_, (uuid::Uuid,)>(
                    "SELECT id FROM sessions WHERE ended_at IS NULL AND last_active < $1"
                )
                .bind(cutoff)
                .fetch_all(&db)
                .await
                {
                    Ok(stale_sessions) => {
                        let count = stale_sessions.len();
                        for (session_id,) in stale_sessions {
                            tracing::info!("Cleaning up stale session {}", session_id);
                            
                            // Stop pi agent
                            if let Err(e) = agent_registry.remove(session_id).await {
                                tracing::warn!("Failed to stop pi agent for {}: {}", session_id, e);
                            }
                            
                            // Remove session directory
                            if let Err(e) = session_manager.remove_session(session_id).await {
                                tracing::warn!("Failed to remove session directory for {}: {}", session_id, e);
                            }
                            
                            // Destroy sandbox if exists
                            if let Err(e) = sandbox_manager.destroy_container(session_id).await {
                                tracing::warn!("Failed to destroy sandbox for {}: {}", session_id, e);
                            }
                            
                            // Mark session as ended
                            let _ = sqlx::query("UPDATE sessions SET ended_at = NOW() WHERE id = $1")
                                .bind(session_id)
                                .execute(&db)
                                .await;
                        }
                        
                        if count > 0 {
                            tracing::info!("Cleaned up {} stale sessions", count);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to query stale sessions: {}", e);
                    }
                }
            }
            _ = shutdown.recv() => {
                tracing::info!("Cleanup task received shutdown signal");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "forge_api=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Get database URL
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    // Get Forge API URL (for agent registry)
    let forge_api_url = std::env::var("FORGE_API_URL")
        .unwrap_or_else(|_| "http://localhost:8080/api/v1".to_string());

    // Create database pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    tracing::info!("Connected to database");

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Initialize sandbox manager
    let sandbox_manager = Arc::new(SandboxManager::new());
    if let Err(e) = sandbox_manager.init().await {
        tracing::warn!("Sandbox initialization failed (nspawn may not be available): {}", e);
    }

    // Initialize agent registry for persistent pi processes
    let agent_registry = Arc::new(AgentRegistry::new(forge_api_url));

    // Initialize session manager
    let session_manager = Arc::new(SessionManager::new());
    if let Err(e) = session_manager.init().await {
        tracing::warn!("Session manager initialization failed: {}", e);
    }
    
    // Initialize metrics
    let metrics = Arc::new(Metrics::new());
    tracing::info!("Observability metrics initialized");

    // Create shutdown signal for background tasks
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    
    // Spawn cleanup task
    let cleanup_session_manager = session_manager.clone();
    let cleanup_agent_registry = agent_registry.clone();
    let cleanup_sandbox_manager = sandbox_manager.clone();
    let cleanup_pool = pool.clone();
    
    let cleanup_handle = tokio::spawn(async move {
        cleanup_task(
            cleanup_session_manager,
            cleanup_agent_registry,
            cleanup_sandbox_manager,
            cleanup_pool,
            shutdown_rx,
        ).await;
    });
    
    // Spawn metrics update task
    let metrics_pool = pool.clone();
    let metrics_agents = agent_registry.clone();
    let metrics_metrics = metrics.clone();
    let (metrics_shutdown_tx, metrics_shutdown_rx) = broadcast::channel(1);
    
    let metrics_handle = tokio::spawn(async move {
        metrics_task(
            metrics_metrics,
            metrics_agents,
            metrics_pool,
            metrics_shutdown_rx,
        ).await;
    });

    // Build app state
    let state = api::AppState::new(
        pool,
        session_manager.clone(),
        sandbox_manager.clone(),
        agent_registry.clone(),
        metrics.clone(),
    );

    // Build observability router
    let obs_router = observability::create_observability_router(metrics.clone());
    
    // Build router with all routes
    let app = api::create_router()
        .with_state(state)
        .merge(obs_router)
        .layer(CorsLayer::permissive());

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("Starting server on {addr}");
    tracing::info!("pi CLI must be installed: npm install -g @mariozechner/pi-coding-agent");
    tracing::info!("Build forge-tools extension: cd extensions/forge-tools && npm install && npm run build");
    tracing::info!("nspawn sandbox available: systemd-nspawn and machinectl");
    tracing::info!("Agent registry initialized - pi processes will persist per session");
    tracing::info!("Session cleanup running - sessions inactive for {} seconds will be cleaned up", SESSION_TIMEOUT_SECS);
    tracing::info!("Metrics available at /api/v1/metrics");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    
    // Clone shutdown_tx for the server task
    let shutdown_server = shutdown_tx.clone();
    
    // Handle shutdown gracefully
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal");
        let _ = shutdown_server.send(());
    });
    
    // Run the server
    axum::serve(listener, app).await?;

    // Signal all background tasks to shutdown
    let _ = shutdown_tx.send(());
    let _ = metrics_shutdown_tx.send(());
    
    // Wait for background tasks to finish
    let _ = cleanup_handle.await;
    let _ = metrics_handle.await;

    tracing::info!("Server shutdown complete");
    Ok(())
}
