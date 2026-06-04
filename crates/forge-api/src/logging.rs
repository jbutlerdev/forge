//! Structured Logging Module
//!
//! Provides request tracing, audit logging, and structured log formatting.

use axum::{extract::Request, http::HeaderMap, middleware::Next, response::Response};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;

/// Request ID header name
const REQUEST_ID_HEADER: &str = "X-Request-ID";

/// Slow request threshold in milliseconds
const SLOW_REQUEST_THRESHOLD_MS: u64 = 1000;

/// Log context for structured logging
#[derive(Debug, Clone, Serialize)]
pub struct LogContext {
    /// Unique request identifier
    pub request_id: Uuid,
    /// Authenticated user ID (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    /// Session ID (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    /// Profile ID (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<Uuid>,
    /// Action being performed
    pub action: String,
    /// HTTP method
    pub method: String,
    /// Request path
    pub path: String,
    /// Query string (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Client IP address
    pub client_ip: String,
    /// User agent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Response status code
    pub status_code: u16,
    /// Request duration in milliseconds
    pub duration_ms: u64,
    /// Timestamp when request started
    pub timestamp: DateTime<Utc>,
    /// Additional context fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, String>>,
}

impl LogContext {
    /// Create a new log context for a request
    pub fn new(request: &Request) -> Self {
        let request_id = get_request_id(request.headers());
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(|s| s.to_string());
        let user_agent = request
            .headers()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Extract client IP
        let client_ip = extract_client_ip(request.headers());

        // Extract action from path
        let action = extract_action_from_path(&path);

        Self {
            request_id,
            user_id: None,
            session_id: None,
            profile_id: None,
            action,
            method,
            path,
            query,
            client_ip,
            user_agent,
            status_code: 0,
            duration_ms: 0,
            timestamp: Utc::now(),
            extra: None,
        }
    }

    /// Set user ID
    pub fn with_user_id(mut self, user_id: Uuid) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Set session ID
    pub fn with_session_id(mut self, session_id: Uuid) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Set profile ID
    pub fn with_profile_id(mut self, profile_id: Uuid) -> Self {
        self.profile_id = Some(profile_id);
        self
    }

    /// Add extra context field
    pub fn with_extra(mut self, key: &str, value: &str) -> Self {
        self.extra
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Set response status code
    pub fn with_status(mut self, status: axum::http::StatusCode) -> Self {
        self.status_code = status.as_u16();
        self
    }

    /// Set duration
    pub fn with_duration(mut self, duration: std::time::Duration) -> Self {
        self.duration_ms = duration.as_millis() as u64;
        self
    }
}

/// Extract client IP from headers
fn extract_client_ip(headers: &HeaderMap) -> String {
    // Check X-Forwarded-For first (for proxied requests)
    if let Some(forwarded) = headers.get("x-forwarded-for") {
        if let Ok(forwarded_str) = forwarded.to_str() {
            // Take the first IP in the chain
            return forwarded_str
                .split(',')
                .next()
                .unwrap_or("unknown")
                .trim()
                .to_string();
        }
    }

    // Check X-Real-IP
    if let Some(real_ip) = headers.get("x-real-ip") {
        if let Ok(ip) = real_ip.to_str() {
            return ip.trim().to_string();
        }
    }

    // Check CF-Connecting-IP (Cloudflare)
    if let Some(cf_ip) = headers.get("cf-connecting-ip") {
        if let Ok(ip) = cf_ip.to_str() {
            return ip.trim().to_string();
        }
    }

    "unknown".to_string()
}

/// Extract or generate request ID from headers
pub fn get_request_id(headers: &HeaderMap) -> Uuid {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::new_v4)
}

/// Extract action from request path
fn extract_action_from_path(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return "root".to_string();
    }

    // Common patterns: /resource/verb, /resource/{id}/verb
    match segments.len() {
        1 => format!("{}.list", segments[0]),
        2 => {
            if segments[1].contains('-') {
                // Likely an ID
                format!("{}.get", segments[0])
            } else {
                format!("{}.{}", segments[0], segments[1])
            }
        }
        _ => {
            let resource = segments[0];
            let verb = segments.last().unwrap_or(&segments[1]);
            format!("{}.{}", resource, verb)
        }
    }
}

/// Audit event types
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    /// Event type
    pub event: String,
    /// Request ID
    pub request_id: Uuid,
    /// User ID (if authenticated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    /// Client IP
    pub client_ip: String,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Additional event-specific data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<HashMap<String, String>>,
}

impl AuditEvent {
    /// Create a new audit event
    pub fn new(event: &str) -> Self {
        Self {
            event: event.to_string(),
            request_id: Uuid::new_v4(),
            user_id: None,
            client_ip: "unknown".to_string(),
            timestamp: Utc::now(),
            data: None,
        }
    }

    /// Set user ID
    pub fn with_user(mut self, user_id: Uuid) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Set client IP
    pub fn with_ip(mut self, ip: &str) -> Self {
        self.client_ip = ip.to_string();
        self
    }

    /// Add data field
    pub fn with_data(mut self, key: &str, value: &str) -> Self {
        self.data
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Log the audit event
    pub fn log(&self) {
        match self.event.as_str() {
            "auth.login" | "auth.logout" | "apikey.create" | "apikey.revoke" | "session.create"
            | "session.delete" | "user.register" | "user.delete" => {
                tracing::info!(
                    request_id = %self.request_id,
                    user_id = ?self.user_id,
                    ip_address = %self.client_ip,
                    event = %self.event,
                    data = ?self.data,
                    "Audit event: {}",
                    self.event
                );
            }
            "auth.failed" | "rate.limited" => {
                tracing::warn!(
                    request_id = %self.request_id,
                    ip_address = %self.client_ip,
                    event = %self.event,
                    data = ?self.data,
                    "Audit event: {}",
                    self.event
                );
            }
            _ => {
                tracing::debug!(
                    request_id = %self.request_id,
                    event = %self.event,
                    data = ?self.data,
                    "Audit event: {}",
                    self.event
                );
            }
        }
    }
}

/// Log an audit event
pub fn log_audit(event: AuditEvent) {
    event.log()
}

/// Request logging middleware
pub async fn request_log_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let mut ctx = LogContext::new(&request);

    // Create tracing span
    let span = tracing::info_span!(
        "request",
        request_id = %ctx.request_id,
        method = %ctx.method,
        path = %ctx.path,
        action = %ctx.action
    );

    let response = {
        let _guard = span.enter();
        next.run(request).await
    };

    let duration = start.elapsed();
    let status = response.status();

    // Update context with response info
    ctx = ctx.with_status(status).with_duration(duration);

    // Log the request
    let level = if status.is_server_error() {
        tracing::Level::ERROR
    } else if status.is_client_error() {
        tracing::Level::WARN
    } else {
        tracing::Level::INFO
    };

    let log_message = format!(
        "HTTP {} {} {} ({}ms)",
        ctx.method, ctx.path, ctx.status_code, ctx.duration_ms
    );

    match level {
        tracing::Level::ERROR => {
            tracing::error!(
                request_id = %ctx.request_id,
                user_id = ?ctx.user_id,
                client_ip = %ctx.client_ip,
                method = %ctx.method,
                path = %ctx.path,
                status = %ctx.status_code,
                duration_ms = %ctx.duration_ms,
                action = %ctx.action,
                log_message
            );
        }
        tracing::Level::WARN => {
            tracing::warn!(
                request_id = %ctx.request_id,
                user_id = ?ctx.user_id,
                client_ip = %ctx.client_ip,
                method = %ctx.method,
                path = %ctx.path,
                status = %ctx.status_code,
                duration_ms = %ctx.duration_ms,
                action = %ctx.action,
                log_message
            );
        }
        _ => {
            tracing::info!(
                request_id = %ctx.request_id,
                user_id = ?ctx.user_id,
                client_ip = %ctx.client_ip,
                method = %ctx.method,
                path = %ctx.path,
                status = %ctx.status_code,
                duration_ms = %ctx.duration_ms,
                action = %ctx.action,
                log_message
            );
        }
    }

    // Check for slow requests
    if ctx.duration_ms > SLOW_REQUEST_THRESHOLD_MS {
        tracing::warn!(
            request_id = %ctx.request_id,
            duration_ms = %ctx.duration_ms,
            threshold_ms = %SLOW_REQUEST_THRESHOLD_MS,
            "Slow request detected (>{})",
            SLOW_REQUEST_THRESHOLD_MS
        );
    }

    // Add request ID to response headers
    let mut response = response;
    response.headers_mut().insert(
        REQUEST_ID_HEADER.parse::<axum::http::HeaderName>().unwrap(),
        ctx.request_id.to_string().parse().unwrap(),
    );

    response
}

/// Create audit event helper functions
pub mod audit {
    use super::*;

    /// Log a login event
    pub fn login(user_id: Uuid, ip_address: &str) {
        AuditEvent::new("auth.login")
            .with_user(user_id)
            .with_ip(ip_address)
            .log();
    }

    /// Log a logout event
    pub fn logout(user_id: Uuid, ip_address: &str, key_prefix: &str) {
        AuditEvent::new("auth.logout")
            .with_user(user_id)
            .with_ip(ip_address)
            .with_data("key_prefix", key_prefix)
            .log();
    }

    /// Log a failed login event
    pub fn login_failed(email: &str, ip_address: &str, reason: &str) {
        AuditEvent::new("auth.failed")
            .with_ip(ip_address)
            .with_data("email", email)
            .with_data("reason", reason)
            .log();
    }

    /// Log an API key creation event
    pub fn api_key_create(user_id: Uuid, key_prefix: &str) {
        AuditEvent::new("apikey.create")
            .with_user(user_id)
            .with_ip("unknown")
            .with_data("key_prefix", key_prefix)
            .log();
    }

    /// Log an API key revocation event
    pub fn api_key_revoke(user_id: Uuid, key_id: Uuid) {
        AuditEvent::new("apikey.revoke")
            .with_user(user_id)
            .with_ip("unknown")
            .with_data("key_id", &key_id.to_string())
            .log();
    }

    /// Log a session creation event
    pub fn session_create(user_id: Uuid, session_id: Uuid, profile_id: Uuid) {
        AuditEvent::new("session.create")
            .with_user(user_id)
            .with_ip("unknown")
            .with_data("session_id", &session_id.to_string())
            .with_data("profile_id", &profile_id.to_string())
            .log();
    }

    /// Log a session deletion event
    pub fn session_delete(user_id: Uuid, session_id: Uuid) {
        AuditEvent::new("session.delete")
            .with_user(user_id)
            .with_ip("unknown")
            .with_data("session_id", &session_id.to_string())
            .log();
    }

    /// Log a user registration event
    pub fn user_register(user_id: Uuid, email: &str) {
        AuditEvent::new("user.register")
            .with_user(user_id)
            .with_ip("unknown")
            .with_data("email", email)
            .log();
    }

    /// Log a user deletion event
    pub fn user_delete(admin_id: Uuid, user_id: Uuid) {
        AuditEvent::new("user.delete")
            .with_user(admin_id)
            .with_ip("unknown")
            .with_data("deleted_user_id", &user_id.to_string())
            .log();
    }

    /// Log a rate limit event
    pub fn rate_limited(user_id: Option<Uuid>, ip_address: &str) {
        let mut event = AuditEvent::new("rate.limited").with_ip(ip_address);
        if let Some(uid) = user_id {
            event = event.with_user(uid);
        }
        event.log();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_new() {
        let event = AuditEvent::new("test.event");

        assert_eq!(event.event, "test.event");
        assert!(event.user_id.is_none());
        assert_eq!(event.client_ip, "unknown");
    }

    #[test]
    fn test_audit_event_with_user() {
        let user_id = Uuid::new_v4();
        let event = AuditEvent::new("test.event").with_user(user_id);

        assert_eq!(event.user_id, Some(user_id));
    }

    #[test]
    fn test_audit_event_with_ip() {
        let event = AuditEvent::new("test.event").with_ip("192.168.1.1");

        assert_eq!(event.client_ip, "192.168.1.1");
    }

    #[test]
    fn test_audit_event_with_data() {
        let event = AuditEvent::new("test.event")
            .with_data("key1", "value1")
            .with_data("key2", "value2");

        assert!(event.data.is_some());
        let data = event.data.unwrap();
        assert_eq!(data.get("key1"), Some(&"value1".to_string()));
        assert_eq!(data.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_audit_event_chaining() {
        let user_id = Uuid::new_v4();
        let event = AuditEvent::new("auth.login")
            .with_user(user_id)
            .with_ip("10.0.0.1")
            .with_data("session_id", "abc123");

        assert_eq!(event.event, "auth.login");
        assert_eq!(event.user_id, Some(user_id));
        assert_eq!(event.client_ip, "10.0.0.1");
        assert!(event.data.is_some());
    }

    #[test]
    fn test_audit_event_log_does_not_panic() {
        let event = AuditEvent::new("test.event");
        event.log();
    }

    #[test]
    fn test_audit_helper_functions() {
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let profile_id = Uuid::new_v4();
        let key_id = Uuid::new_v4();

        // These should not panic
        audit::login(user_id, "127.0.0.1");
        audit::logout(user_id, "127.0.0.1", "sk_forge_abc");
        audit::login_failed("test@example.com", "127.0.0.1", "invalid_password");
        audit::api_key_create(user_id, "sk_forge_abc123");
        audit::api_key_revoke(user_id, key_id);
        audit::session_create(user_id, session_id, profile_id);
        audit::session_delete(user_id, session_id);
        audit::user_register(user_id, "new@example.com");
        audit::user_delete(user_id, Uuid::new_v4());
        audit::rate_limited(Some(user_id), "127.0.0.1");
        audit::rate_limited(None, "127.0.0.1");
    }
}
