use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// ============================================
// User Types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub role: String, // 'admin' | 'user'
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUser {
    pub email: String,
    pub name: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateUser {
    pub email: Option<String>,
    pub name: Option<String>,
    pub role: Option<String>, // Only admins can update this
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<User> for UserResponse {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            email: user.email,
            name: user.name,
            role: user.role,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }
    }
}

// ============================================
// API Key Types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ApiKey {
    pub id: Uuid,
    pub user_id: Uuid,
    pub name: String,
    #[serde(skip_serializing)]
    pub key_hash: String,
    pub key_prefix: String, // First 12 chars for identification
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateApiKey {
    pub name: String,
    pub expires_in_days: Option<i32>, // None = no expiration
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub name: String,
    pub key_prefix: String,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyCreated {
    pub api_key: String, // Only returned on creation!
    pub api_key_response: ApiKeyResponse,
}

impl From<ApiKey> for ApiKeyResponse {
    fn from(key: ApiKey) -> Self {
        Self {
            id: key.id,
            user_id: key.user_id,
            name: key.name,
            key_prefix: key.key_prefix,
            last_used_at: key.last_used_at,
            expires_at: key.expires_at,
            created_at: key.created_at,
        }
    }
}

// ============================================
// Profile Types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Profile {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub provider: String,
    pub model: String,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub working_dir: String,
    pub git_url: Option<String>,
    pub git_ref: Option<String>,
    pub nix_shell: Option<String>,
    pub system_prompt: String,
    pub tools: String, // JSON array
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub user_id: Option<Uuid>, // Added in migration 002
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProfile {
    pub name: String,
    pub description: Option<String>,
    pub provider: String,
    pub model: String,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub working_dir: String,
    pub git_url: Option<String>,
    pub git_ref: Option<String>,
    pub nix_shell: Option<String>,
    pub system_prompt: Option<String>,
    pub tools: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProfile {
    pub name: Option<String>,
    pub description: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub working_dir: Option<String>,
    pub git_url: Option<String>,
    pub git_ref: Option<String>,
    pub nix_shell: Option<String>,
    pub system_prompt: Option<String>,
    pub tools: Option<Vec<String>>,
}

// ============================================
// Session Types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Session {
    pub id: Uuid,
    pub profile_id: Uuid,
    pub title: Option<String>,
    pub cell_host: Option<String>,
    pub cell_state: Option<serde_json::Value>,
    pub last_active: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub user_id: Option<Uuid>, // Added in migration 002
    // Per-session model overrides (migration 006). When non-NULL,
    // `agent_registry::get_or_create` prefers these over the
    // profile's values. The "model switcher" sets them; a normal
    // session has all four NULL and behaves as before.
    #[serde(default)]
    pub override_provider: Option<String>,
    #[serde(default)]
    pub override_model: Option<String>,
    #[serde(default)]
    pub override_base_url: Option<String>,
    #[serde(default)]
    pub override_api_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct CreateSession {
    pub title: Option<String>,
}

/// Partial update for a session. The model switcher (Option A):
/// change `provider` / `model` / `base_url` / `api_key` to override
/// the profile's values for this session, without changing the
/// profile (and thus without moving the working dir / git repo).
///
/// To distinguish "omitted" (leave the column alone) from
/// "explicitly null" (clear the override), each override field uses
/// a custom deserializer: an absent key becomes `None` (omit), a
/// JSON `null` becomes `Some(Value::Null)` (clear the override),
/// and a JSON string becomes `Some(Value::String(...))` (set it).
/// Without the custom deserializer, serde collapses both absent
/// and null into `None`, making it impossible to clear an override.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct UpdateSession {
    pub title: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present_value")]
    pub provider: Option<serde_json::Value>,
    #[serde(default, deserialize_with = "deserialize_present_value")]
    pub model: Option<serde_json::Value>,
    #[serde(default, deserialize_with = "deserialize_present_value")]
    pub base_url: Option<serde_json::Value>,
    #[serde(default, deserialize_with = "deserialize_present_value")]
    pub api_key: Option<serde_json::Value>,
}

/// Deserialize a field so that an absent key and an explicit `null`
/// are distinguishable: absent -> `None` (the `#[serde(default)]`
/// supplies it), `null` -> `Some(Value::Null)`, any other value ->
/// `Some(value)`. This is the standard serde pattern for
/// "nullable + omittable".
fn deserialize_present_value<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Use `Option<Value>` but override the null handling: serde's
    // default `Option` deserializer maps null to `None`, but we want
    // `Some(Null)`. Deserialize as a raw `Value` and wrap it.
    let v = serde_json::Value::deserialize(deserializer)?;
    Ok(Some(v))
}

// ============================================
// Message Types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Message {
    pub id: Uuid,
    pub session_id: Uuid,
    pub sequence: i32,
    pub role: String,
    pub content: Option<String>,
    pub tool_name: Option<String>,
    pub tool_input: Option<serde_json::Value>,
    pub tool_call_id: Option<String>,
    /// Structured tool result captured by the executor. Added in
    /// migration 002; populated only for `role = 'tool'` rows.
    #[serde(default)]
    pub tool_output: Option<serde_json::Value>,
    /// Wall-clock duration of the tool execution. Added in
    /// migration 002; populated only for `role = 'tool'` rows.
    #[serde(default)]
    pub duration_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct CreateMessage {
    pub content: String,
}

// ============================================
// Auth Types
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub user: UserResponse,
    pub api_key: String,
}
