//! Observability Module
//!
//! Provides structured logging, metrics, and request tracing for Forge API.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};

/// Request metrics
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Total requests received
    pub requests_total: Arc<AtomicU64>,
    /// Requests by endpoint
    pub requests_by_endpoint: Arc<Mutex<std::collections::HashMap<String, Arc<AtomicU64>>>>,
    /// Total errors (4xx + 5xx)
    pub errors_total: Arc<AtomicU64>,
    /// Errors by status code
    pub errors_by_status: Arc<Mutex<std::collections::HashMap<u16, Arc<AtomicU64>>>>,
    /// Total tool executions
    pub tool_executions_total: Arc<AtomicU64>,
    /// Tool executions by type
    pub tool_executions_by_type: Arc<Mutex<std::collections::HashMap<String, Arc<AtomicU64>>>>,
    /// Active sessions
    pub active_sessions: Arc<AtomicU64>,
    /// Active agents
    pub active_agents: Arc<AtomicU64>,
    /// SSE chunks dropped because the live consumer fell
    /// behind. The audit-log accumulator always has the
    /// full output regardless; this counter exists so
    /// operators can see in `/metrics` when the live UI
    /// is being lossy.
    pub sse_chunks_dropped: Arc<AtomicU64>,
    /// Total events published on the `MessageBus`.
    pub bus_published: Arc<AtomicU64>,
    /// Total events dropped because an SSE consumer's
    /// broadcast receiver lagged the bounded bus buffer
    /// (the consumer recovers from the DB, so this is a
    /// lossiness indicator, not a data-loss count).
    pub bus_lagged_drops: Arc<AtomicU64>,
}

impl Metrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment request counter. Synchronous: the per-endpoint map
    /// is a plain `std::sync::Mutex` (held only for the map entry /
    /// atomic bump, microseconds). Previously each increment spawned
    /// a tokio task, which under load spawned multiple tasks per
    /// request and made snapshots taken immediately after an
    /// increment race the task — the tests had to sleep 10-50ms to
    /// make the count visible.
    pub fn inc_requests(&self, endpoint: &str) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        let mut map = self.requests_by_endpoint.lock().unwrap();
        let counter = map
            .entry(endpoint.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment error counter. Synchronous; see [`Self::inc_requests`].
    pub fn inc_errors(&self, status: u16) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        let mut map = self.errors_by_status.lock().unwrap();
        let counter = map
            .entry(status)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment tool execution counter. Synchronous; see [`Self::inc_requests`].
    pub fn inc_tool_execution(&self, tool_type: &str) {
        self.tool_executions_total.fetch_add(1, Ordering::Relaxed);
        let mut map = self.tool_executions_by_type.lock().unwrap();
        let counter = map
            .entry(tool_type.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Set active sessions count
    pub fn set_active_sessions(&self, count: u64) {
        self.active_sessions.store(count, Ordering::Relaxed);
    }

    /// Set active agents count
    pub fn set_active_agents(&self, count: u64) {
        self.active_agents.store(count, Ordering::Relaxed);
    }

    /// Increment the SSE-chunks-dropped counter. Called from
    /// the bash-streaming reader tasks when a slow consumer
    /// causes the mpsc channel to fill up and a chunk can't
    /// be forwarded to the live SSE stream. Cheap (relaxed
    /// atomic add).
    pub fn inc_sse_chunks_dropped(&self, n: u64) {
        self.sse_chunks_dropped.fetch_add(n, Ordering::Relaxed);
    }

    /// Increment the bus-published counter by one.
    pub fn inc_bus_published(&self) {
        self.bus_published.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the bus-lagged-drops counter by `n` (one
    /// lagged receiver missed `n` events).
    pub fn inc_bus_lagged_drops(&self, n: u64) {
        self.bus_lagged_drops.fetch_add(n, Ordering::Relaxed);
    }

    /// Get all metrics as a snapshot
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let mut requests_by_endpoint = std::collections::HashMap::new();
        for (endpoint, counter) in self.requests_by_endpoint.lock().unwrap().iter() {
            requests_by_endpoint.insert(endpoint.clone(), counter.load(Ordering::Relaxed));
        }

        let mut errors_by_status = std::collections::HashMap::new();
        for (status, counter) in self.errors_by_status.lock().unwrap().iter() {
            errors_by_status.insert(*status, counter.load(Ordering::Relaxed));
        }

        let mut tool_executions_by_type = std::collections::HashMap::new();
        for (tool_type, counter) in self.tool_executions_by_type.lock().unwrap().iter() {
            tool_executions_by_type.insert(tool_type.clone(), counter.load(Ordering::Relaxed));
        }

        MetricsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            requests_by_endpoint,
            errors_total: self.errors_total.load(Ordering::Relaxed),
            errors_by_status,
            tool_executions_total: self.tool_executions_total.load(Ordering::Relaxed),
            tool_executions_by_type,
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            active_agents: self.active_agents.load(Ordering::Relaxed),
            sse_chunks_dropped: self.sse_chunks_dropped.load(Ordering::Relaxed),
            bus_published: self.bus_published.load(Ordering::Relaxed),
            bus_lagged_drops: self.bus_lagged_drops.load(Ordering::Relaxed),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests_total: Arc::new(AtomicU64::new(0)),
            requests_by_endpoint: Arc::new(Mutex::new(std::collections::HashMap::new())),
            errors_total: Arc::new(AtomicU64::new(0)),
            errors_by_status: Arc::new(Mutex::new(std::collections::HashMap::new())),
            tool_executions_total: Arc::new(AtomicU64::new(0)),
            tool_executions_by_type: Arc::new(Mutex::new(std::collections::HashMap::new())),
            active_sessions: Arc::new(AtomicU64::new(0)),
            active_agents: Arc::new(AtomicU64::new(0)),
            sse_chunks_dropped: Arc::new(AtomicU64::new(0)),
            bus_published: Arc::new(AtomicU64::new(0)),
            bus_lagged_drops: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Process-wide `Metrics` handle used by the message bus.
///
/// `MessageBus` is constructed in `lib.rs` *before* the
/// `Metrics` instance is threaded into the handlers, and
/// `AppState` (which holds both) lives in `api/mod.rs`, so
/// the bus can't take a direct reference. Instead the bus
/// looks the counter up here, and [`create_observability_router`]
/// registers the app's `Arc<Metrics>` on startup. Tests that
/// construct a `MessageBus` without an app see no-op
/// increments (the bus also keeps its own local counters).
static GLOBAL_METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();

/// Register the application's `Metrics` instance. Called once
/// from [`create_observability_router`]; later calls are no-ops.
pub fn init_global_metrics(metrics: Arc<Metrics>) {
    let _ = GLOBAL_METRICS.set(metrics);
}

/// Increment the global `bus_published` counter (no-op if the
/// global hasn't been registered yet, e.g. in unit tests).
pub fn inc_bus_published() {
    if let Some(m) = GLOBAL_METRICS.get() {
        m.inc_bus_published();
    }
}

/// Increment the global `bus_lagged_drops` counter by `n`
/// (no-op if the global hasn't been registered yet).
pub fn inc_bus_lagged_drops(n: u64) {
    if let Some(m) = GLOBAL_METRICS.get() {
        m.inc_bus_lagged_drops(n);
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub requests_total: u64,
    pub requests_by_endpoint: std::collections::HashMap<String, u64>,
    pub errors_total: u64,
    pub errors_by_status: std::collections::HashMap<u16, u64>,
    pub tool_executions_total: u64,
    pub tool_executions_by_type: std::collections::HashMap<String, u64>,
    pub active_sessions: u64,
    pub active_agents: u64,
    pub sse_chunks_dropped: u64,
    pub bus_published: u64,
    pub bus_lagged_drops: u64,
}

// ============================================
// Metrics Endpoint
// ============================================

#[derive(Clone)]
pub struct ObservabilityState {
    pub metrics: Arc<Metrics>,
}

async fn get_metrics(State(state): State<ObservabilityState>) -> Response {
    let snapshot = state.metrics.snapshot().await;

    let error_rate = if snapshot.requests_total > 0 {
        snapshot.errors_total as f64 / snapshot.requests_total as f64
    } else {
        0.0
    };

    Json(serde_json::json!({
        "metrics": snapshot,
        "error_rate": format!("{:.2}%", error_rate * 100.0),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
    .into_response()
}

/// Prometheus metric names — the single source of truth for the
/// names emitted by [`get_prometheus_metrics`]. Dashboards/alerts
/// that reference these strings and the exporter can't drift apart
/// silently if both use the constants.
pub mod metric_names {
    pub const REQUESTS_TOTAL: &str = "forge_requests_total";
    pub const ERRORS_TOTAL: &str = "forge_errors_total";
    pub const TOOL_EXECUTIONS_TOTAL: &str = "forge_tool_executions_total";
    pub const ACTIVE_SESSIONS: &str = "forge_active_sessions";
    pub const ACTIVE_AGENTS: &str = "forge_active_agents";
    pub const SSE_CHUNKS_DROPPED_TOTAL: &str = "forge_sse_chunks_dropped_total";
    pub const BUS_PUBLISHED_TOTAL: &str = "forge_bus_published_total";
    pub const BUS_LAGGED_DROPS_TOTAL: &str = "forge_bus_lagged_drops_total";
    pub const REQUESTS_BY_ENDPOINT: &str = "forge_requests_by_endpoint";
    pub const ERRORS_BY_STATUS: &str = "forge_errors_by_status";
    pub const TOOL_EXECUTIONS_BY_TYPE: &str = "forge_tool_executions_by_type";
}

/// Emit one unlabelled `# HELP` / `# TYPE` / value triple.
fn push_metric(out: &mut String, ty: &str, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} {ty}\n{name} {value}\n"
    ));
}

/// Prometheus metrics endpoint
pub async fn get_prometheus_metrics(State(state): State<ObservabilityState>) -> Response {
    let snapshot = state.metrics.snapshot().await;

    let mut output = String::new();

    push_metric(
        &mut output,
        "counter",
        metric_names::REQUESTS_TOTAL,
        "Total number of HTTP requests",
        snapshot.requests_total,
    );
    push_metric(
        &mut output,
        "counter",
        metric_names::ERRORS_TOTAL,
        "Total number of HTTP errors",
        snapshot.errors_total,
    );
    push_metric(
        &mut output,
        "counter",
        metric_names::TOOL_EXECUTIONS_TOTAL,
        "Total number of tool executions",
        snapshot.tool_executions_total,
    );
    push_metric(
        &mut output,
        "gauge",
        metric_names::ACTIVE_SESSIONS,
        "Number of active sessions",
        snapshot.active_sessions,
    );
    push_metric(
        &mut output,
        "gauge",
        metric_names::ACTIVE_AGENTS,
        "Number of active pi agents",
        snapshot.active_agents,
    );
    push_metric(
        &mut output,
        "counter",
        metric_names::SSE_CHUNKS_DROPPED_TOTAL,
        "SSE chunks dropped because the consumer fell behind",
        snapshot.sse_chunks_dropped,
    );
    push_metric(
        &mut output,
        "counter",
        metric_names::BUS_PUBLISHED_TOTAL,
        "Total events published on the message bus",
        snapshot.bus_published,
    );
    push_metric(
        &mut output,
        "counter",
        metric_names::BUS_LAGGED_DROPS_TOTAL,
        "Events dropped because an SSE consumer lagged the bus buffer",
        snapshot.bus_lagged_drops,
    );

    for (endpoint, count) in &snapshot.requests_by_endpoint {
        let label = endpoint.replace('"', "\\\"").replace('\n', "\\n");
        output.push_str(&format!(
            "{}{{endpoint=\"{}\"}} {}\n",
            metric_names::REQUESTS_BY_ENDPOINT,
            label,
            count
        ));
    }

    for (status, count) in &snapshot.errors_by_status {
        output.push_str(&format!(
            "{}{{status=\"{}\"}} {}\n",
            metric_names::ERRORS_BY_STATUS,
            status,
            count
        ));
    }

    for (tool_type, count) in &snapshot.tool_executions_by_type {
        let label = tool_type.replace('"', "\\\"").replace('\n', "\\n");
        output.push_str(&format!(
            "{}{{type=\"{}\"}} {}\n",
            metric_names::TOOL_EXECUTIONS_BY_TYPE,
            label,
            count
        ));
    }

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
        .into_response()
}

/// Create the observability router
pub fn create_observability_router(metrics: Arc<Metrics>) -> Router {
    // Make the app's Metrics reachable from the message bus
    // (see `GLOBAL_METRICS`).
    init_global_metrics(metrics.clone());
    Router::new()
        .route("/metrics", get(get_metrics))
        .route("/metrics/prometheus", get(get_prometheus_metrics))
        .with_state(ObservabilityState { metrics })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_new() {
        let metrics = Metrics::new();

        assert_eq!(
            metrics
                .requests_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics
                .errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics
                .tool_executions_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_metrics_increment_requests() {
        let metrics = Metrics::new();

        metrics.inc_requests("GET /health");
        metrics.inc_requests("POST /messages");

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.requests_total, 2);
    }

    #[tokio::test]
    async fn test_metrics_increment_errors() {
        let metrics = Metrics::new();

        metrics.inc_errors(400);
        metrics.inc_errors(500);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.errors_total, 2);
        assert_eq!(snapshot.errors_by_status.get(&400), Some(&1));
        assert_eq!(snapshot.errors_by_status.get(&500), Some(&1));
    }

    #[tokio::test]
    async fn test_metrics_increment_tool_execution() {
        let metrics = Metrics::new();

        metrics.inc_tool_execution("bash");
        metrics.inc_tool_execution("read");
        metrics.inc_tool_execution("bash");

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.tool_executions_total, 3);
        assert_eq!(snapshot.tool_executions_by_type.get("bash"), Some(&2));
        assert_eq!(snapshot.tool_executions_by_type.get("read"), Some(&1));
    }

    #[tokio::test]
    async fn test_metrics_set_active_sessions() {
        let metrics = Metrics::new();

        metrics.set_active_sessions(5);
        assert_eq!(
            metrics
                .active_sessions
                .load(std::sync::atomic::Ordering::Relaxed),
            5
        );

        metrics.set_active_sessions(10);
        assert_eq!(
            metrics
                .active_sessions
                .load(std::sync::atomic::Ordering::Relaxed),
            10
        );
    }

    #[tokio::test]
    async fn test_metrics_snapshot() {
        let metrics = Metrics::new();

        metrics.inc_requests("GET /test");
        metrics.inc_errors(404);
        metrics.inc_tool_execution("bash");
        metrics.set_active_sessions(2);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let snapshot = metrics.snapshot().await;

        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.tool_executions_total, 1);
        assert_eq!(snapshot.active_sessions, 2);
    }

    #[tokio::test]
    async fn test_metrics_bus_counters() {
        let metrics = Metrics::new();

        metrics.inc_bus_published();
        metrics.inc_bus_published();
        metrics.inc_bus_lagged_drops(3);

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.bus_published, 2);
        assert_eq!(snapshot.bus_lagged_drops, 3);
    }
}
