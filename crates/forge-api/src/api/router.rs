//! Message router — a "universal entrypoint" that classifies an
//! incoming message and forwards it to the right conversation.
//!
//! The router is its own forge profile (`name = "message-router"`).
//! It specifies the provider + model used for the **classification
//! LLM call** — a single, fast completion that decides whether the
//! message belongs to an existing session or should start a new one
//! in one of the other profiles. The router profile does not run pi
//! and does not need a working directory or tools; it only makes
//! one HTTP call to the LLM and then dispatches the message via the
//! normal [`crate::api::dispatch_message`] path.
//!
//! ## Endpoint
//!
//! `POST /router/message` — `{ content: String }` →
//! `{ session_id, profile_id, routed_to: "existing"|"new", reason }`.
//!
//! The message is dispatched fire-and-forget (the agent turn runs in
//! a background task, same as `POST /messages`). The client opens
//! the SSE stream for the returned `session_id` to see the response.
//!
//! ## Provider config resolution
//!
//! The LLM call needs a base URL + API key + API format. These come
//! from (in order of precedence):
//! 1. The router profile's own `base_url` / `api_key` (if set).
//! 2. pi's `models.json` (the `providers.<name>` entry, which has
//!    `baseUrl`, `apiKey`, and `api` — either `openai-completions`
//!    or `anthropic-messages`).
//!
//! If neither source has a base URL, the router returns 400.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

use crate::api::auth::extract_auth_user;
use crate::api::AppState;
use crate::db::{Profile, Session};

/// The conventional name of the router profile. The endpoint looks
/// up the profile by this name.
pub const ROUTER_PROFILE_NAME: &str = "message-router";

// ============================================
// Request / response types
// ============================================

#[derive(Debug, Deserialize)]
pub struct RouterRequest {
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct RouterResponse {
    pub session_id: Uuid,
    pub profile_id: Uuid,
    pub profile_name: String,
    /// "existing" if routed to an existing session, "new" if a fresh
    /// session was created.
    pub routed_to: &'static str,
    pub reason: String,
}

// ============================================
// Provider config (full, with secrets — never serialized to clients)
// ============================================

/// Full provider config from `models.json`, including secrets.
/// Used internally to make the routing LLM call. Unlike the catalog
/// endpoint (which strips `apiKey`/`baseUrl`), this needs them.
#[derive(Debug, Clone)]
struct ProviderConfig {
    base_url: String,
    api_key: String,
    /// The API format: `"openai-completions"` or `"anthropic-messages"`.
    api_format: String,
}

/// Read the full provider config from `models.json` for a given
/// provider name. Returns `None` if the provider isn't listed or
/// the file is missing/unreadable.
fn read_provider_config(models_path: &PathBuf, provider: &str) -> Option<ProviderConfig> {
    let contents = std::fs::read_to_string(models_path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&contents).ok()?;
    let cfg = v.get("providers")?.get(provider)?;
    let base_url = cfg.get("baseUrl")?.as_str()?.to_string();
    let api_key = cfg
        .get("apiKey")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let api_format = cfg
        .get("api")
        .and_then(|x| x.as_str())
        .unwrap_or("openai-completions")
        .to_string();
    Some(ProviderConfig {
        base_url,
        api_key,
        api_format,
    })
}

// ============================================
// Routing decision
// ============================================

#[derive(Debug, Deserialize)]
struct RoutingDecision {
    /// `"session"` or `"profile"`.
    target_type: String,
    /// The UUID of the target session or profile.
    target_id: Uuid,
    /// A brief human-readable explanation of the routing choice.
    #[serde(default)]
    reason: String,
}

// ============================================
// Session summary (for the routing prompt)
// ============================================

/// A lightweight summary of a session for the routing prompt.
/// Fetched with a single joined query to avoid N+1.
#[derive(Debug, sqlx::FromRow)]
struct SessionSummary {
    id: Uuid,
    title: Option<String>,
    profile_name: String,
    last_message: Option<String>,
}

// ============================================
// Handler
// ============================================

/// `POST /router/message` — classify and forward.
pub async fn route_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<RouterRequest>,
) -> Response {
    if extract_auth_user(&state.db, &headers).await.is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Unauthorized"})),
        )
            .into_response();
    }
    state.metrics.inc_requests("POST /router/message");

    if payload.content.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "content is required"})),
        )
            .into_response();
    }

    // 1. Find the router profile by name.
    let router_profile = match sqlx::query_as::<_, Profile>(
        "SELECT * FROM profiles WHERE name = $1 LIMIT 1",
    )
    .bind(ROUTER_PROFILE_NAME)
    .fetch_optional(&state.db)
    .await
    {
        Ok(Some(p)) => p,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!(
                        "Router profile '{}' not found. Create a profile named '{}' to use the universal entrypoint.",
                        ROUTER_PROFILE_NAME, ROUTER_PROFILE_NAME
                    )
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("router: failed to look up router profile: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Database error"})),
            )
                .into_response()
        }
    };

    // 2. Resolve the provider config for the LLM call.
    // Prefer the router profile's own base_url/api_key; fall back
    // to models.json.
    let provider_config = {
        let base_url = router_profile.base_url.as_deref().filter(|s| !s.is_empty());
        let api_key = router_profile.api_key.as_deref().filter(|s| !s.is_empty());
        match base_url {
            Some(url) => ProviderConfig {
                base_url: url.to_string(),
                api_key: api_key.unwrap_or("").to_string(),
                // If the profile overrides the base URL, assume
                // OpenAI format (the most common). The `api` field
                // is only in models.json.
                api_format: "openai-completions".to_string(),
            },
            None => match read_provider_config(&state.models_path, &router_profile.provider) {
                Some(cfg) => cfg,
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": format!(
                                "No base URL for provider '{}'. Set base_url on the router profile or add it to models.json.",
                                router_profile.provider
                            )
                        })),
                    )
                        .into_response()
                }
            },
        }
    };

    // 3. Fetch profiles (excluding the router) and recent sessions.
    let profiles: Vec<Profile> = match sqlx::query_as::<_, Profile>(
        "SELECT * FROM profiles WHERE name != $1 ORDER BY created_at DESC LIMIT 15",
    )
    .bind(ROUTER_PROFILE_NAME)
    .fetch_all(&state.db)
    .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("router: failed to fetch profiles: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Database error"})),
            )
                .into_response();
        }
    };

    if profiles.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "No non-router profiles exist. Create at least one profile for the router to target."
            })),
        )
            .into_response();
    }

    // Fetch recent sessions with their profile names and last user
    // message (for context). Limited to 20 to keep the prompt small.
    let sessions: Vec<SessionSummary> = match sqlx::query_as::<_, SessionSummary>(
        r#"SELECT s.id, s.title, p.name AS profile_name,
              (SELECT content FROM messages m
               WHERE m.session_id = s.id AND m.role = 'user'
               ORDER BY m.sequence DESC LIMIT 1) AS last_message
           FROM sessions s
           JOIN profiles p ON s.profile_id = p.id
           WHERE p.name != $1
           ORDER BY s.last_active DESC
           LIMIT 20"#,
    )
    .bind(ROUTER_PROFILE_NAME)
    .fetch_all(&state.db)
    .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("router: failed to fetch sessions: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Database error"})),
            )
                .into_response();
        }
    };

    // 4. Build the routing prompt and make the LLM call.
    let prompt = build_routing_prompt(&profiles, &sessions, &payload.content);
    let response_text = match make_llm_call(
        &provider_config,
        &router_profile.model,
        &prompt.system,
        &prompt.user,
    )
    .await
    {
        Ok(text) => text,
        Err(e) => {
            tracing::error!("router: LLM call failed: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": format!("Routing LLM call failed: {}", e)})),
            )
                .into_response();
        }
    };

    // 5. Parse the routing decision.
    let decision = match parse_routing_decision(&response_text) {
        Some(d) => d,
        None => {
            tracing::warn!(
                "router: could not parse routing decision from LLM response: {}",
                &response_text[..response_text.len().min(500)]
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Could not parse routing decision from LLM. Try again.",
                    "raw_response": response_text,
                })),
            )
                .into_response();
        }
    };

    // 6. Execute the routing decision: create or reuse a session.
    let (session_id, profile_id, profile_name, routed_to) = match decision.target_type.as_str() {
        "session" => {
            // Verify the session exists and get its profile.
            let s: Option<(Uuid, Uuid)> =
                sqlx::query_as("SELECT id, profile_id FROM sessions WHERE id = $1")
                    .bind(decision.target_id)
                    .fetch_optional(&state.db)
                    .await
                    .ok()
                    .flatten();
            match s {
                Some((sid, pid)) => {
                    let pname =
                        sqlx::query_scalar::<_, String>("SELECT name FROM profiles WHERE id = $1")
                            .bind(pid)
                            .fetch_one(&state.db)
                            .await
                            .unwrap_or_else(|_| "unknown".to_string());
                    (sid, pid, pname, "existing")
                }
                None => {
                    // The LLM picked a session that doesn't
                    // exist. Fall back to creating a new session
                    // with the first profile.
                    tracing::warn!(
                        "router: LLM picked non-existent session {}, falling back to new session",
                        decision.target_id
                    );
                    match create_new_session(&state, &profiles[0].id, &profiles[0].name).await {
                        Ok(sid) => (sid, profiles[0].id, profiles[0].name.clone(), "new"),
                        Err(resp) => return resp,
                    }
                }
            }
        }
        "profile" => {
            // Verify the profile exists and create a new session.
            let p = profiles.iter().find(|p| p.id == decision.target_id);
            match p {
                Some(profile) => {
                    match create_new_session(&state, &profile.id, &profile.name).await {
                        Ok(sid) => (sid, profile.id, profile.name.clone(), "new"),
                        Err(resp) => return resp,
                    }
                }
                None => {
                    // The LLM picked a profile that doesn't
                    // exist. Fall back to the first profile.
                    tracing::warn!(
                        "router: LLM picked non-existent profile {}, falling back to first profile",
                        decision.target_id
                    );
                    match create_new_session(&state, &profiles[0].id, &profiles[0].name).await {
                        Ok(sid) => (sid, profiles[0].id, profiles[0].name.clone(), "new"),
                        Err(resp) => return resp,
                    }
                }
            }
        }
        other => {
            tracing::warn!("router: LLM returned unknown target_type '{}'", other);
            // Fall back to creating a new session.
            match create_new_session(&state, &profiles[0].id, &profiles[0].name).await {
                Ok(sid) => (sid, profiles[0].id, profiles[0].name.clone(), "new"),
                Err(resp) => return resp,
            }
        }
    };

    // 7. Dispatch the message to the target session.
    if let Err((status, msg)) =
        crate::api::dispatch_message(&state, session_id, &payload.content).await
    {
        return (status, Json(serde_json::json!({"error": msg}))).into_response();
    }

    // 8. Return the routing result. The client opens SSE on the
    // returned session_id to see the agent's response.
    Json(RouterResponse {
        session_id,
        profile_id,
        profile_name,
        routed_to,
        reason: decision.reason,
    })
    .into_response()
}

/// Create a new session for the given profile, including the session
/// directory. Returns the session UUID on success, or an error
/// response on failure.
async fn create_new_session(
    state: &AppState,
    profile_id: &Uuid,
    profile_name: &str,
) -> Result<Uuid, Response> {
    let title = format!(
        "{} · {}",
        profile_name,
        chrono::Utc::now().format("%Y-%m-%d %H:%M")
    );
    let session: Session = match sqlx::query_as::<_, Session>(
        r#"INSERT INTO sessions (profile_id, title) VALUES ($1, $2) RETURNING *"#,
    )
    .bind(profile_id)
    .bind(&title)
    .fetch_one(&state.db)
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to create session: {}", e)})),
            )
                .into_response())
        }
    };

    // Create the session directory (needed for the agent's working
    // dir). If this fails, clean up the session row and error out.
    let profile = match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(profile_id)
        .fetch_one(&state.db)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
                .bind(session.id)
                .execute(&state.db)
                .await;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to load profile: {}", e)})),
            )
                .into_response());
        }
    };

    if let Err(e) = state
        .session_manager
        .create_session_dir(session.id, &profile)
        .await
    {
        let _ = sqlx::query("DELETE FROM sessions WHERE id = $1")
            .bind(session.id)
            .execute(&state.db)
            .await;
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                serde_json::json!({"error": format!("Failed to create session directory: {}", e)}),
            ),
        )
            .into_response());
    }

    Ok(session.id)
}

// ============================================
// Routing prompt
// ============================================

struct RoutingPrompt {
    system: String,
    user: String,
}

fn build_routing_prompt(
    profiles: &[Profile],
    sessions: &[SessionSummary],
    content: &str,
) -> RoutingPrompt {
    let system = "You are a message router. Your job is to decide where a user's message should go: either an existing conversation (if it's a follow-up) or a new conversation using one of the available profiles (if it starts a new topic).

Respond with ONLY a JSON object — no markdown, no code fences, no explanation outside the JSON:
{\"target_type\": \"session\" | \"profile\", \"target_id\": \"<uuid>\", \"reason\": \"<brief explanation>\"}

Rules:
- If the message is clearly a follow-up to an existing conversation, set target_type to \"session\" and use that conversation's UUID.
- If the message starts a new topic, set target_type to \"profile\" and pick the most appropriate profile UUID.
- The target_id MUST be one of the UUIDs listed below. Do not make up UUIDs.
- Keep the reason under 20 words.";

    let mut user = String::new();
    user.push_str("Available profiles (for new conversations):\n");
    for p in profiles {
        user.push_str(&format!(
            "- {} : {} — {}/{}\n",
            p.id, p.name, p.provider, p.model
        ));
        if let Some(desc) = &p.description {
            user.push_str(&format!("    Description: {}\n", desc));
        }
    }

    if !sessions.is_empty() {
        user.push_str("\nExisting conversations (most recent first):\n");
        for s in sessions {
            let title = s.title.as_deref().unwrap_or("Untitled");
            let snippet = s.last_message.as_deref().unwrap_or("(no messages yet)");
            // Truncate the snippet to keep the prompt small.
            let snippet = if snippet.len() > 200 {
                &snippet[..200]
            } else {
                snippet
            };
            user.push_str(&format!(
                "- {} : \"{}\" (profile: {}, last message: \"{}\")\n",
                s.id, title, s.profile_name, snippet
            ));
        }
    } else {
        user.push_str("\n(No existing conversations.)\n");
    }

    user.push_str(&format!("\nUser's message:\n{}", content));
    user.push_str("\n\nRespond with the JSON routing decision now.");

    RoutingPrompt {
        system: system.to_string(),
        user,
    }
}

// ============================================
// LLM call
// ============================================

/// Make a single LLM completion call and return the text response.
/// Supports both OpenAI-compatible and Anthropic API formats.
async fn make_llm_call(
    config: &ProviderConfig,
    model: &str,
    system: &str,
    user: &str,
) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("HTTP client build failed: {}", e))?;

    match config.api_format.as_str() {
        "anthropic-messages" => {
            make_anthropic_call(
                &client,
                &config.base_url,
                &config.api_key,
                model,
                system,
                user,
            )
            .await
        }
        _ => {
            // Default: OpenAI-compatible chat completions.
            make_openai_call(
                &client,
                &config.base_url,
                &config.api_key,
                model,
                system,
                user,
            )
            .await
        }
    }
}

/// OpenAI-compatible chat completions call.
/// POST {base_url}/chat/completions
async fn make_openai_call(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    system: &str,
    user: &str,
) -> Result<String, String> {
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 1024,
    });

    let mut req = client.post(&url).json(&body);
    if !api_key.is_empty() {
        req = req.bearer_auth(api_key);
    }

    tracing::info!("router LLM call: POST {} model={}", url, model);

    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Read body failed: {}", e))?;
    tracing::debug!(
        "router LLM response: status={} body_len={} body_preview={}",
        status,
        text.len(),
        &text[..text.len().min(500)]
    );
    if !status.is_success() {
        return Err(format!(
            "LLM returned {}: {}",
            status,
            &text[..text.len().min(500)]
        ));
    }

    let v: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("Invalid JSON response: {}", e))?;
    // Extract the first choice's message content.
    let content = v
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .map(|s| s.to_string());
    match content {
        Some(ref c) if !c.is_empty() => Ok(c.clone()),
        Some(_) => {
            // Content is empty — reasoning models (Qwen) sometimes
            // put the answer in a `reasoning` field. Try that.
            tracing::warn!("router: LLM returned empty content, trying reasoning field");
            let reasoning = v
                .get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("reasoning"))
                .and_then(|r| r.as_str());
            match reasoning {
                Some(r) if !r.is_empty() => Ok(r.to_string()),
                _ => Err(format!(
                    "Empty content and no reasoning in response: {}",
                    &text[..text.len().min(200)]
                )),
            }
        }
        None => Err(format!(
            "No content in response: {}",
            &text[..text.len().min(200)]
        )),
    }
}

/// Anthropic messages API call.
/// POST {base_url}/messages
async fn make_anthropic_call(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    system: &str,
    user: &str,
) -> Result<String, String> {
    let url = format!("{}/messages", base_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": model,
        "max_tokens": 1024,
        "system": system,
        "messages": [
            {"role": "user", "content": user},
        ],
    });

    let mut req = client
        .post(&url)
        .json(&body)
        .header("anthropic-version", "2023-06-01");
    if !api_key.is_empty() {
        req = req.header("x-api-key", api_key);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Read body failed: {}", e))?;
    if !status.is_success() {
        return Err(format!(
            "LLM returned {}: {}",
            status,
            &text[..text.len().min(500)]
        ));
    }

    let v: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("Invalid JSON response: {}", e))?;
    // Anthropic returns content as an array of blocks.
    v.get("content")
        .and_then(|c| c.as_array())
        .and_then(|blocks| {
            blocks
                .iter()
                .find(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .and_then(|b| b.get("text"))
                .and_then(|t| t.as_str())
        })
        .map(|s| s.to_string())
        .ok_or_else(|| {
            format!(
                "No text in Anthropic response: {}",
                &text[..text.len().min(200)]
            )
        })
}

// ============================================
// Routing decision parsing
// ============================================

/// Parse the routing decision from the LLM's text response.
/// Tolerant of markdown code fences and leading/trailing whitespace.
fn parse_routing_decision(text: &str) -> Option<RoutingDecision> {
    let trimmed = text.trim();

    // Direct parse (best case — the LLM returned pure JSON).
    if let Ok(d) = serde_json::from_str::<RoutingDecision>(trimmed) {
        return Some(d);
    }

    // Strip markdown code fences: ```json\n...\n``` or ```\n...\n```.
    let stripped = strip_code_fences(trimmed);
    if let Ok(d) = serde_json::from_str::<RoutingDecision>(&stripped) {
        return Some(d);
    }

    // Extract the first {...} block (in case the LLM added text
    // before/after the JSON).
    if let Some(json_str) = extract_json_object(trimmed) {
        if let Ok(d) = serde_json::from_str::<RoutingDecision>(&json_str) {
            return Some(d);
        }
    }

    None
}

/// Remove ```...``` fences from a string.
fn strip_code_fences(s: &str) -> String {
    let s = s.trim();
    if !s.starts_with("```") {
        return s.to_string();
    }
    // Remove the opening fence.
    let after_open = &s[3..];
    // Skip the optional language tag (e.g. "json") and the newline
    // that follows it. Everything up to the first newline is the
    // language tag (or empty if the content starts on the same line).
    let after_open = match after_open.find('\n') {
        Some(idx) => &after_open[idx + 1..],
        None => after_open,
    };
    // Remove the closing fence.
    let without_close = after_open.trim_end_matches("```").trim();
    without_close.to_string()
}

/// Extract the first balanced `{...}` object from a string.
fn extract_json_object(s: &str) -> Option<String> {
    let start = s.find('{')?;
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    for (i, c) in s[start..].char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match c {
            '\\' if in_string => escape = true,
            '"' => in_string = !in_string,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(s[start..start + i + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pure_json() {
        let d = parse_routing_decision(
            r#"{"target_type":"session","target_id":"550e8400-e29b-41d4-a716-446655440000","reason":"follow-up"}"#,
        )
        .unwrap();
        assert_eq!(d.target_type, "session");
        assert_eq!(
            d.target_id,
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
        );
        assert_eq!(d.reason, "follow-up");
    }

    #[test]
    fn parse_with_code_fences() {
        let d = parse_routing_decision(
            "```json\n{\"target_type\":\"profile\",\"target_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"reason\":\"new topic\"}\n```",
        )
        .unwrap();
        assert_eq!(d.target_type, "profile");
        assert_eq!(d.reason, "new topic");
    }

    #[test]
    fn parse_with_surrounding_text() {
        let d = parse_routing_decision(
            "Here is the decision:\n{\"target_type\":\"session\",\"target_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"reason\":\"yes\"}\nDone.",
        )
        .unwrap();
        assert_eq!(d.target_type, "session");
    }

    #[test]
    fn parse_missing_reason_defaults_to_empty() {
        let d = parse_routing_decision(
            r#"{"target_type":"session","target_id":"550e8400-e29b-41d4-a716-446655440000"}"#,
        )
        .unwrap();
        assert_eq!(d.reason, "");
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(parse_routing_decision("not json at all").is_none());
        assert!(parse_routing_decision("{}").is_none()); // missing fields
    }

    #[test]
    fn strip_fences_plain() {
        assert_eq!(strip_code_fences("```\nhello\n```"), "hello");
    }

    #[test]
    fn strip_fences_json_tag() {
        assert_eq!(strip_code_fences("```json\n{\"a\":1}\n```"), "{\"a\":1}");
    }

    #[test]
    fn strip_fences_no_fence() {
        assert_eq!(strip_code_fences("just text"), "just text");
    }

    #[test]
    fn extract_json_simple() {
        assert_eq!(
            extract_json_object(r#"prefix {"a":1} suffix"#),
            Some(r#"{"a":1}"#.to_string())
        );
    }

    #[test]
    fn extract_json_nested() {
        assert_eq!(
            extract_json_object(r#"before {"a":{"b":2},"c":3} after"#),
            Some(r#"{"a":{"b":2},"c":3}"#.to_string())
        );
    }

    #[test]
    fn extract_json_with_brace_in_string() {
        assert_eq!(
            extract_json_object(r#"{"a":"} is a brace"}"#),
            Some(r#"{"a":"} is a brace"}"#.to_string())
        );
    }

    #[test]
    fn build_prompt_includes_profiles_and_sessions() {
        let p = Profile {
            id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            name: "coder".to_string(),
            description: Some("codes stuff".to_string()),
            provider: "proxy".to_string(),
            model: "qwen".to_string(),
            base_url: None,
            api_key: None,
            working_dir: "/tmp".to_string(),
            git_url: None,
            git_ref: None,
            nix_shell: None,
            system_prompt: "".to_string(),
            tools: "[]".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            user_id: None,
        };
        let s = SessionSummary {
            id: Uuid::parse_str("660e8400-e29b-41d4-a716-446655440000").unwrap(),
            title: Some("test chat".to_string()),
            profile_name: "coder".to_string(),
            last_message: Some("hello world".to_string()),
        };
        let prompt = build_routing_prompt(&[p], &[s], "write a function");
        assert!(prompt.user.contains("coder"));
        assert!(prompt.user.contains("test chat"));
        assert!(prompt.user.contains("write a function"));
        assert!(prompt.system.contains("target_type"));
    }
}
