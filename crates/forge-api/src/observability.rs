//! Observability Module
//!
//! Provides structured logging, metrics, and request tracing for Forge API.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use tokio::sync::RwLock;

/// Request metrics
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Total requests received
    pub requests_total: Arc<AtomicU64>,
    /// Requests by endpoint
    pub requests_by_endpoint: Arc<RwLock<std::collections::HashMap<String, Arc<AtomicU64>>>>,
    /// Total errors (4xx + 5xx)
    pub errors_total: Arc<AtomicU64>,
    /// Errors by status code
    pub errors_by_status: Arc<RwLock<std::collections::HashMap<u16, Arc<AtomicU64>>>>,
    /// Total tool executions
    pub tool_executions_total: Arc<AtomicU64>,
    /// Tool executions by type
    pub tool_executions_by_type: Arc<RwLock<std::collections::HashMap<String, Arc<AtomicU64>>>>,
    /// Active sessions
    pub active_sessions: Arc<AtomicU64>,
    /// Active agents
    pub active_agents: Arc<AtomicU64>,
}

impl Metrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment request counter
    pub fn inc_requests(&self, endpoint: &str) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        
        let endpoint_metrics = self.requests_by_endpoint.clone();
        let endpoint_owned = endpoint.to_string();
        tokio::spawn(async move {
            let mut map = endpoint_metrics.write().await;
            let counter = map.entry(endpoint_owned).or_insert_with(|| Arc::new(AtomicU64::new(0)));
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Increment error counter
    pub fn inc_errors(&self, status: u16) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        
        let status_metrics = self.errors_by_status.clone();
        tokio::spawn(async move {
            let mut map = status_metrics.write().await;
            let counter = map.entry(status).or_insert_with(|| Arc::new(AtomicU64::new(0)));
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Increment tool execution counter
    pub fn inc_tool_execution(&self, tool_type: &str) {
        self.tool_executions_total.fetch_add(1, Ordering::Relaxed);
        
        let tool_metrics = self.tool_executions_by_type.clone();
        let tool_type_owned = tool_type.to_string();
        tokio::spawn(async move {
            let mut map = tool_metrics.write().await;
            let counter = map.entry(tool_type_owned).or_insert_with(|| Arc::new(AtomicU64::new(0)));
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Set active sessions count
    pub fn set_active_sessions(&self, count: u64) {
        self.active_sessions.store(count, Ordering::Relaxed);
    }

    /// Set active agents count
    pub fn set_active_agents(&self, count: u64) {
        self.active_agents.store(count, Ordering::Relaxed);
    }

    /// Get all metrics as a snapshot
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let mut requests_by_endpoint = std::collections::HashMap::new();
        for (endpoint, counter) in self.requests_by_endpoint.read().await.iter() {
            requests_by_endpoint.insert(endpoint.clone(), counter.load(Ordering::Relaxed));
        }

        let mut errors_by_status = std::collections::HashMap::new();
        for (status, counter) in self.errors_by_status.read().await.iter() {
            errors_by_status.insert(*status, counter.load(Ordering::Relaxed));
        }

        let mut tool_executions_by_type = std::collections::HashMap::new();
        for (tool_type, counter) in self.tool_executions_by_type.read().await.iter() {
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
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests_total: Arc::new(AtomicU64::new(0)),
            requests_by_endpoint: Arc::new(RwLock::new(std::collections::HashMap::new())),
            errors_total: Arc::new(AtomicU64::new(0)),
            errors_by_status: Arc::new(RwLock::new(std::collections::HashMap::new())),
            tool_executions_total: Arc::new(AtomicU64::new(0)),
            tool_executions_by_type: Arc::new(RwLock::new(std::collections::HashMap::new())),
            active_sessions: Arc::new(AtomicU64::new(0)),
            active_agents: Arc::new(AtomicU64::new(0)),
        }
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
    })).into_response()
}

/// Prometheus metrics endpoint
pub async fn get_prometheus_metrics(State(state): State<ObservabilityState>) -> Response {
    let snapshot = state.metrics.snapshot().await;
    
    let mut output = String::new();
    
    output.push_str("# HELP forge_requests_total Total number of HTTP requests\n");
    output.push_str("# TYPE forge_requests_total counter\n");
    output.push_str(&format!("forge_requests_total {}\n", snapshot.requests_total));
    
    output.push_str("# HELP forge_errors_total Total number of HTTP errors\n");
    output.push_str("# TYPE forge_errors_total counter\n");
    output.push_str(&format!("forge_errors_total {}\n", snapshot.errors_total));
    
    output.push_str("# HELP forge_tool_executions_total Total number of tool executions\n");
    output.push_str("# TYPE forge_tool_executions_total counter\n");
    output.push_str(&format!("forge_tool_executions_total {}\n", snapshot.tool_executions_total));
    
    output.push_str("# HELP forge_active_sessions Number of active sessions\n");
    output.push_str("# TYPE forge_active_sessions gauge\n");
    output.push_str(&format!("forge_active_sessions {}\n", snapshot.active_sessions));
    
    output.push_str("# HELP forge_active_agents Number of active pi agents\n");
    output.push_str("# TYPE forge_active_agents gauge\n");
    output.push_str(&format!("forge_active_agents {}\n", snapshot.active_agents));
    
    for (endpoint, count) in &snapshot.requests_by_endpoint {
        let label = endpoint.replace('"', "\\\"").replace('\n', "\\n");
        output.push_str(&format!("forge_requests_by_endpoint{{endpoint=\"{}\"}} {}\n", label, count));
    }
    
    for (status, count) in &snapshot.errors_by_status {
        output.push_str(&format!("forge_errors_by_status{{status=\"{}\"}} {}\n", status, count));
    }
    
    for (tool_type, count) in &snapshot.tool_executions_by_type {
        let label = tool_type.replace('"', "\\\"").replace('\n', "\\n");
        output.push_str(&format!("forge_tool_executions_by_type{{type=\"{}\"}} {}\n", label, count));
    }
    
    (StatusCode::OK, [
        (axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
    ], output).into_response()
}

/// Create the observability router
pub fn create_observability_router(metrics: Arc<Metrics>) -> Router {
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
        
        assert_eq!(metrics.requests_total.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(metrics.errors_total.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(metrics.tool_executions_total.load(std::sync::atomic::Ordering::Relaxed), 0);
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
        assert_eq!(metrics.active_sessions.load(std::sync::atomic::Ordering::Relaxed), 5);
        
        metrics.set_active_sessions(10);
        assert_eq!(metrics.active_sessions.load(std::sync::atomic::Ordering::Relaxed), 10);
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
}
