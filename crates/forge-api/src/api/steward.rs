//! Steward support (Herd, 2026-10-19): system-log ingest/query and a
//! cross-session tool-call audit surface.
//!
//! The steward is a herd agent that audits the herd — ranch, forge,
//! and mule. Its sandbox is mount-namespace-isolated and cannot read
//! any journald (ranchd runs in the ranch-mini VM, mule in the mule
//! VM), so a privileged shipper on the mac host (the same
//! `container machine run` path all deploys use) tails each unit's
//! journal and POSTs rows here. The steward then reads:
//!
//!   POST /logs/ingest  — shipper only (batch; source allowlist;
//!                        optional FORGE_LOG_INGEST_TOKEN gate)
//!   GET  /logs         — filter by source/since/level/match/limit
//!   GET  /audit/tools  — recent tool-call rows across sessions
//!                        (failures, slow calls) — the signal that
//!                        surfaced the nspawn flock bug
//!
//! Trust: all three require an API key (the sandbox already carries
//! FORGE_API_KEY); /logs* additionally rejects restricted (demo)
//! keys. Ingest is further gated by FORGE_LOG_INGEST_TOKEN when that
//! env var is set — the shipper must then send a matching
//! X-Log-Token. Sources are a fixed allowlist so the table can never
//! become a general-purpose log sink for arbitrary agents.

use crate::api::auth::AuthenticatedUser;
use crate::api::{db_err, err_resp, AppState};
use axum::{
    extract::{Extension, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

/// The only log sources the shipper may write. Extending this is a
/// code change on purpose (trust boundary).
pub const LOG_SOURCES: &[&str] = &["ranchd", "forge-api", "forge-harness", "mule"];

/// Rows older than this are swept on ingest.
const RETENTION_DAYS: i64 = 14;
const MAX_BATCH: usize = 2000;
const MAX_MSG_CHARS: usize = 4000;
const DEFAULT_LOG_LIMIT: i64 = 200;
const MAX_LOG_LIMIT: i64 = 1000;

#[derive(Deserialize)]
pub struct LogEntryIn {
    pub source: String,
    #[serde(default)]
    pub host: Option<String>,
    /// RFC3339 (journalctl `__REALTIME_TIMESTAMP` / `__TIMESTAMP_REALTIME_8INT`)
    pub ts: String,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub pid: Option<i32>,
    pub msg: String,
}

#[derive(Deserialize)]
pub struct IngestRequest {
    pub entries: Vec<LogEntryIn>,
}

/// POST /logs/ingest — batch insert with validation. Rejects the
/// whole batch on any malformed entry (the shipper should fix its
/// tail, not half-ingest). Sweeps expired rows on the way.
pub(crate) async fn ingest_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<IngestRequest>,
) -> Response {
    // Optional second factor beyond the API key (the shipper keeps
    // both in /etc/forge-logship.env). Unset env = key-only, matching
    // the rest of the fleet's single-factor stance.
    if let Ok(expected) = std::env::var("FORGE_LOG_INGEST_TOKEN") {
        if !expected.is_empty() {
            let got = headers
                .get("x-log-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if got != expected {
                return err_resp(&state, StatusCode::UNAUTHORIZED, "invalid log ingest token");
            }
        }
    }

    if payload.entries.is_empty() {
        return (StatusCode::OK, Json(json!({ "ok": true, "inserted": 0 }))).into_response();
    }
    if payload.entries.len() > MAX_BATCH {
        return err_resp(
            &state,
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("batch exceeds {MAX_BATCH} entries"),
        );
    }
    let mut rows = Vec::with_capacity(payload.entries.len());
    for (i, e) in payload.entries.iter().enumerate() {
        if !LOG_SOURCES.contains(&e.source.as_str()) {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                &format!(
                    "entry {i}: source '{}' not in allowlist {:?}",
                    e.source, LOG_SOURCES
                ),
            );
        }
        let ts = match DateTime::parse_from_rfc3339(&e.ts) {
            Ok(t) => t.with_timezone(&Utc),
            Err(_) => {
                return err_resp(
                    &state,
                    StatusCode::BAD_REQUEST,
                    &format!("entry {i}: ts '{}' is not RFC3339", e.ts),
                )
            }
        };
        // Clock-skew guard: the future is not a log.
        if ts > Utc::now() + chrono::Duration::minutes(10) {
            return err_resp(
                &state,
                StatusCode::BAD_REQUEST,
                &format!("entry {i}: ts is in the future"),
            );
        }
        let msg = if e.msg.chars().count() > MAX_MSG_CHARS {
            let cut: String = e.msg.chars().take(MAX_MSG_CHARS).collect();
            format!("{cut}…[truncated]")
        } else {
            e.msg.clone()
        };
        rows.push((
            e.source.clone(),
            e.host.clone().unwrap_or_else(|| "mini".into()),
            ts,
            e.level.clone(),
            e.unit.clone(),
            e.pid,
            msg,
        ));
    }

    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "ingest begin failed",
                e,
            )
        }
    };
    let mut inserted = 0i64;
    for (source, host, ts, level, unit, pid, msg) in rows {
        let r = sqlx::query(
            "INSERT INTO system_logs (source, host, ts, level, unit, pid, msg) VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(&source)
        .bind(&host)
        .bind(ts)
        .bind(&level)
        .bind(&unit)
        .bind(pid)
        .bind(&msg)
        .execute(&mut *tx)
        .await;
        match r {
            Ok(_) => inserted += 1,
            Err(e) => {
                return db_err(
                    &state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "log insert failed",
                    e,
                )
            }
        }
    }
    // Retention sweep (cheap: ts index; runs on every ingest batch,
    // which is hourly per source at fleet cadence).
    if let Err(e) =
        sqlx::query("DELETE FROM system_logs WHERE ts < NOW() - ($1 || ' days')::INTERVAL")
            .bind(RETENTION_DAYS.to_string())
            .execute(&mut *tx)
            .await
    {
        tracing::warn!(error = %e, "system_logs retention sweep failed");
    }
    if let Err(e) = tx.commit().await {
        return db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "ingest commit failed",
            e,
        );
    }
    state.metrics.inc_requests("POST /logs/ingest");
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "inserted": inserted })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct LogsQuery {
    pub source: Option<String>,
    /// RFC3339 or relative ("6h", "30m", "2d"); default last 6h.
    pub since: Option<String>,
    pub level: Option<String>,
    /// Case-insensitive substring over msg.
    pub r#match: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Serialize)]
pub struct LogRow {
    pub source: String,
    pub host: String,
    pub ts: DateTime<Utc>,
    pub level: Option<String>,
    pub unit: Option<String>,
    pub pid: Option<i32>,
    pub msg: String,
}

/// Parse a `since` value: RFC3339, or relative like "6h" "30m" "2d"
/// "45s" (single unit suffix, integer magnitude).
pub(crate) fn parse_since(raw: Option<&str>) -> Result<DateTime<Utc>, String> {
    let default = Utc::now() - chrono::Duration::hours(6);
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(default);
    };
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    let (num, mult): (&str, Box<dyn Fn(i64) -> chrono::Duration>) =
        match s.chars().last().unwrap_or(' ') {
            's' => (&s[..s.len() - 1], Box::new(chrono::Duration::seconds)),
            'm' => (&s[..s.len() - 1], Box::new(chrono::Duration::minutes)),
            'h' => (&s[..s.len() - 1], Box::new(chrono::Duration::hours)),
            'd' => (&s[..s.len() - 1], Box::new(chrono::Duration::days)),
            _ => return Err(format!("since '{s}': use RFC3339 or Ns/Nm/Nh/Nd")),
        };
    let n: i64 = num
        .trim()
        .parse()
        .map_err(|_| format!("since '{s}': use RFC3339 or Ns/Nm/Nh/Nd"))?;
    if !((0..=30 * 24 * 60).contains(&n)) {
        return Err("since out of range".into());
    }
    Ok(Utc::now() - mult(n))
}
/// GET /logs — newest-first, allowlisted sources only.
pub(crate) async fn list_logs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(q): Query<LogsQuery>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: logs are not available",
        );
    }
    let since = match parse_since(q.since.as_deref()) {
        Ok(t) => t,
        Err(e) => return err_resp(&state, StatusCode::BAD_REQUEST, &e),
    };
    let limit = q.limit.unwrap_or(DEFAULT_LOG_LIMIT).clamp(1, MAX_LOG_LIMIT);
    let source = q.source.clone().unwrap_or_default();
    if !source.is_empty() && !LOG_SOURCES.contains(&source.as_str()) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            &format!("source must be one of {:?}", LOG_SOURCES),
        );
    }
    if q.r#match.as_deref().unwrap_or("").len() > 200 {
        return err_resp(&state, StatusCode::BAD_REQUEST, "match too long");
    }

    // Build the full SQL first, then bind in the same order the
    // placeholders were emitted.
    let lvl = q.level.as_deref().map(str::trim).filter(|l| !l.is_empty());
    let m = q
        .r#match
        .as_deref()
        .map(str::trim)
        .filter(|mm| !mm.is_empty());
    let mut sql = String::from(
        "SELECT source, host, ts, level, unit, pid, msg FROM system_logs WHERE ts >= $1",
    );
    let mut idx = 2;
    if !source.is_empty() {
        sql.push_str(&format!(" AND source = ${idx}"));
        idx += 1;
    }
    if lvl.is_some() {
        sql.push_str(&format!(" AND lower(level) = ${idx}"));
        idx += 1;
    }
    if m.is_some() {
        sql.push_str(&format!(" AND msg ILIKE ${idx}"));
        idx += 1;
    }
    sql.push_str(&format!(" ORDER BY ts DESC LIMIT ${idx}"));

    let mut query = sqlx::query_as::<
        _,
        (
            String,
            String,
            DateTime<Utc>,
            Option<String>,
            Option<String>,
            Option<i32>,
            String,
        ),
    >(&sql)
    .bind(since);
    if !source.is_empty() {
        query = query.bind(&source);
    }
    if let Some(lvl) = lvl {
        query = query.bind(lvl.to_lowercase());
    }
    if let Some(m) = m {
        // Escape LIKE wildcards so "40%" is a literal match.
        let escaped = m.replace('%', "\\%").replace('_', "\\_");
        query = query.bind(format!("%{escaped}%"));
    }
    query = query.bind(limit);

    let rows = match query.fetch_all(&state.db).await {
        Ok(r) => r,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "log query failed",
                e,
            )
        }
    };
    let logs: Vec<LogRow> = rows
        .into_iter()
        .map(|(source, host, ts, level, unit, pid, msg)| LogRow {
            source,
            host,
            ts,
            level,
            unit,
            pid,
            msg,
        })
        .collect();
    state.metrics.inc_requests("GET /logs");
    (
        StatusCode::OK,
        Json(json!({ "logs": logs, "count": logs.len() })),
    )
        .into_response()
}

// ============================================
// Tool-call audit surface
// ============================================

#[derive(Deserialize)]
pub struct AuditToolsQuery {
    /// RFC3339 or relative; default last 24h.
    pub since: Option<String>,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    /// "1"/"true" → only rows whose tool result says success=false.
    pub failed: Option<String>,
    /// Minimum duration_ms (slow-call finder).
    pub min_duration_ms: Option<i64>,
    pub limit: Option<i64>,
}

fn truthy(v: &Option<String>) -> bool {
    matches!(
        v.as_deref().map(str::to_lowercase).as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// GET /audit/tools — recent tool-result rows across ALL sessions of
/// the owner's tenant, with the owning agent/session attached. This
/// is the surface that turns "[bash exit=1 duration=1ms]" rows into
/// a fleet-wide signal (the nspawn flock investigation was raw psql;
/// the steward runs it on cron).
pub(crate) async fn audit_tools(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(q): Query<AuditToolsQuery>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: audit is not available",
        );
    }
    // Default window 24h (audit cadence), NOT the 6h log default.
    let since = match parse_since(q.since.as_deref()) {
        Ok(t) => t,
        Err(e) => return err_resp(&state, StatusCode::BAD_REQUEST, &e),
    };
    let since = if q.since.is_some() {
        since
    } else {
        Utc::now() - chrono::Duration::hours(24)
    };
    let limit = q.limit.unwrap_or(200).clamp(1, MAX_LOG_LIMIT);
    let agent_id: Option<Uuid> = match q
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(a) => match Uuid::parse_str(a) {
            Ok(u) => Some(u),
            Err(_) => return err_resp(&state, StatusCode::BAD_REQUEST, "agent_id is not a uuid"),
        },
        None => None,
    };
    let session_id: Option<Uuid> = match q
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(s) => match Uuid::parse_str(s) {
            Ok(u) => Some(u),
            Err(_) => return err_resp(&state, StatusCode::BAD_REQUEST, "session_id is not a uuid"),
        },
        None => None,
    };

    let mut sql = String::from(
        r#"SELECT m.session_id, s.agent_id, a.name, m.tool_name, m.created_at,
                  m.duration_ms, m.tool_output, LEFT(m.tool_input::text, 200)
           FROM messages m
           JOIN sessions s ON s.id = m.session_id
           LEFT JOIN agents a ON a.id = s.agent_id
           WHERE m.role = 'tool' AND m.created_at >= $1"#,
    );
    let mut idx = 2;
    if agent_id.is_some() {
        sql.push_str(&format!(" AND s.agent_id = ${idx}"));
        idx += 1;
    }
    if session_id.is_some() {
        sql.push_str(&format!(" AND m.session_id = ${idx}"));
        idx += 1;
    }
    if truthy(&q.failed) {
        // The executor records {success, stdout, stderr, ...}; treat
        // a missing/invalid JSON as "not failed" (only deny explicitly
        // failed rows), same shape the TUI failure marker uses.
        sql.push_str(" AND tool_output::jsonb->>'success' = 'false'");
    }
    if let Some(ms) = q.min_duration_ms {
        sql.push_str(&format!(" AND m.duration_ms >= ${idx}"));
        idx += 1;
        let _ = ms;
    }
    let _ = idx;
    sql.push_str(&format!(" ORDER BY m.created_at DESC LIMIT ${idx}"));

    let mut query = sqlx::query_as::<
        _,
        (
            Uuid,
            Option<Uuid>,
            Option<String>,
            Option<String>,
            DateTime<Utc>,
            Option<i64>,
            Option<serde_json::Value>,
            Option<String>,
        ),
    >(&sql)
    .bind(since);
    if let Some(a) = agent_id {
        query = query.bind(a);
    }
    if let Some(s) = session_id {
        query = query.bind(s);
    }
    if let Some(ms) = q.min_duration_ms {
        query = query.bind(ms);
    }
    query = query.bind(limit);

    let rows = match query.fetch_all(&state.db).await {
        Ok(r) => r,
        Err(e) => {
            return db_err(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "audit query failed",
                e,
            )
        }
    };
    let calls: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(session, agent, agent_name, tool, ts, dur, out, input)| {
            let brief = out.as_ref().and_then(|o| {
                o.get("stderr")
                    .or_else(|| o.get("error"))
                    .or_else(|| o.get("stdout"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.chars().take(300).collect::<String>())
            });
            json!({
                "session_id": session,
                "agent_id": agent,
                "agent_name": agent_name,
                "tool": tool,
                "ts": ts,
                "duration_ms": dur,
                "success": out.as_ref().and_then(|o| o.get("success")).and_then(|v| v.as_bool()),
                "output_brief": brief,
                "input_brief": input,
            })
        })
        .collect();
    state.metrics.inc_requests("GET /audit/tools");
    (
        StatusCode::OK,
        Json(json!({ "tool_calls": calls, "count": calls.len(), "since": since })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_since_accepts_relative_and_rfc3339() {
        let t = parse_since(Some("6h")).unwrap();
        assert!(t < Utc::now() && t > Utc::now() - chrono::Duration::hours(7));
        let t = parse_since(Some("90m")).unwrap();
        assert!(t > Utc::now() - chrono::Duration::hours(2));
        let t = parse_since(Some("2026-10-19T00:00:00Z")).unwrap();
        assert_eq!(t.to_rfc3339(), "2026-10-19T00:00:00+00:00");
        let t = parse_since(None).unwrap(); // default 6h
        assert!(t < Utc::now());
        assert!(parse_since(Some("bogus")).is_err());
        assert!(parse_since(Some("-5h")).is_err());
        assert!(parse_since(Some("5x")).is_err());
    }

    #[test]
    fn source_allowlist_is_closed() {
        assert!(LOG_SOURCES.contains(&"ranchd"));
        assert!(LOG_SOURCES.contains(&"forge-api"));
        assert!(LOG_SOURCES.contains(&"forge-harness"));
        assert!(LOG_SOURCES.contains(&"mule"));
        assert!(!LOG_SOURCES.contains(&"sshd"));
        assert!(!LOG_SOURCES.contains(&"postgres"));
    }

    #[test]
    fn truthy_parses_gate_words() {
        assert!(truthy(&Some("1".into())));
        assert!(truthy(&Some("TRUE".into())));
        assert!(!truthy(&Some("0".into())));
        assert!(!truthy(&None));
    }
}
