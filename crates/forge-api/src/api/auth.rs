//! Authentication module
//! 
//! Provides user registration, login, and API key management.

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::{
    ApiKey, ApiKeyCreated, ApiKeyResponse, CreateApiKey, CreateUser, LoginRequest, LoginResponse,
    User, UserResponse, UpdateUser,
};
use crate::api::AppState;
use crate::logging::audit;

/// Auth error types
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("User not found")]
    UserNotFound,
    #[error("Email already exists")]
    EmailExists,
    #[error("Invalid API key")]
    InvalidApiKey,
    #[error("API key expired")]
    ApiKeyExpired,
    #[error("Password hash error: {0}")]
    PasswordHash(String),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match &self {
            AuthError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthError::UserNotFound => StatusCode::NOT_FOUND,
            AuthError::EmailExists => StatusCode::CONFLICT,
            AuthError::InvalidApiKey => StatusCode::UNAUTHORIZED,
            AuthError::ApiKeyExpired => StatusCode::UNAUTHORIZED,
            AuthError::PasswordHash(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, Json(serde_json::json!({ "error": self.to_string() }))).into_response()
    }
}

// ============================================
// Auth Context (extracted from request)
// ============================================

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: Uuid,
    pub role: String,
}

/// Extract authenticated user from request headers
async fn extract_auth_user(
    pool: &PgPool,
    headers: &HeaderMap,
) -> Result<AuthenticatedUser, AuthError> {
    let api_key = headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or(AuthError::InvalidApiKey)?;

    // Hash the provided key
    let key_hash = hash_api_key(api_key);

    // Look up the API key in database
    let api_key_record: ApiKey = sqlx::query_as(
        r#"
        SELECT * FROM api_keys 
        WHERE key_hash = $1 
        LIMIT 1
        "#,
    )
    .bind(&key_hash)
    .fetch_optional(pool)
    .await
    .map_err(AuthError::Database)?
    .ok_or(AuthError::InvalidApiKey)?;

    // Check expiration
    if let Some(expires_at) = api_key_record.expires_at {
        if expires_at < chrono::Utc::now() {
            return Err(AuthError::ApiKeyExpired);
        }
    }

    // Update last_used_at
    let _ = sqlx::query(
        "UPDATE api_keys SET last_used_at = NOW() WHERE id = $1",
    )
    .bind(api_key_record.id)
    .execute(pool)
    .await;

    // Get the user
    let user: User = sqlx::query_as("SELECT * FROM users WHERE id = $1")
        .bind(api_key_record.user_id)
        .fetch_one(pool)
        .await
        .map_err(AuthError::Database)?;

    Ok(AuthenticatedUser {
        user_id: user.id,
        role: user.role,
    })
}

// ============================================
// Password Hashing
// ============================================

fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| AuthError::PasswordHash(e.to_string()))
}

fn verify_password(password: &str, hash: &str) -> Result<bool, AuthError> {
    let parsed_hash = PasswordHash::new(hash)
        .map_err(|e| AuthError::PasswordHash(e.to_string()))?;

    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

// ============================================
// API Key Generation
// ============================================

fn generate_api_key() -> String {
    let mut key_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut key_bytes);
    let key_hex = hex::encode(key_bytes);
    format!("sk_forge_{}", key_hex)
}

fn hash_api_key(api_key: &str) -> String {
    let key_content = api_key.strip_prefix("sk_forge_").unwrap_or(api_key);
    let mut hasher = Sha256::new();
    hasher.update(key_content.as_bytes());
    hex::encode(hasher.finalize())
}

fn get_key_prefix(api_key: &str) -> String {
    api_key.chars().take(12).collect()
}

// ============================================
// Auth Routes
// ============================================

/// Register a new user
pub async fn register(
    State(state): State<AppState>,
    Json(payload): Json<CreateUser>,
) -> Result<Response, AuthError> {
    // Validate password strength (minimum 8 characters)
    if payload.password.len() < 8 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Password must be at least 8 characters" })),
        )
            .into_response());
    }

    // Validate email format (basic check)
    if !payload.email.contains('@') {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid email format" })),
        )
            .into_response());
    }

    // Hash the password
    let password_hash = hash_password(&payload.password)?;

    // Create user
    let user: User = sqlx::query_as(
        r#"
        INSERT INTO users (email, name, password_hash, role)
        VALUES ($1, $2, $3, 'user')
        RETURNING *
        "#,
    )
    .bind(&payload.email)
    .bind(&payload.name)
    .bind(&password_hash)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e {
            if db_err.constraint() == Some("users_email_key") {
                return AuthError::EmailExists;
            }
        }
        AuthError::Database(e)
    })?;

    audit::user_register(user.id, &user.email);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "user": UserResponse::from(user) })),
    )
        .into_response())
}

/// Login and get API key
pub async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Response, AuthError> {
    // Find user by email
    let user: User = sqlx::query_as("SELECT * FROM users WHERE email = $1")
        .bind(&payload.email)
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::InvalidCredentials)?;

    // Verify password
    let valid = verify_password(&payload.password, &user.password_hash)?;
    if !valid {
        return Err(AuthError::InvalidCredentials);
    }

    // Generate API key
    let api_key = generate_api_key();
    let key_hash = hash_api_key(&api_key);
    let key_prefix = get_key_prefix(&api_key);

    // Store API key
    let _api_key_record: ApiKey = sqlx::query_as(
        r#"
        INSERT INTO api_keys (user_id, name, key_hash, key_prefix)
        VALUES ($1, 'Default API Key', $2, $3)
        RETURNING *
        "#,
    )
    .bind(user.id)
    .bind(&key_hash)
    .bind(&key_prefix)
    .fetch_one(&state.db)
    .await
    .map_err(AuthError::Database)?;

    audit::login(user.id, "unknown");

    Ok(Json(LoginResponse {
        user: UserResponse::from(user),
        api_key,
    })
    .into_response())
}

/// Logout (revoke the current API key)
pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    let api_key = headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or(AuthError::InvalidApiKey)?;

    let key_hash = hash_api_key(api_key);
    let key_prefix = get_key_prefix(api_key);

    // Delete the API key
    sqlx::query("DELETE FROM api_keys WHERE key_hash = $1")
        .bind(&key_hash)
        .execute(&state.db)
        .await
        .map_err(AuthError::Database)?;

    // Get user ID for audit log
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM api_keys WHERE key_hash = $1"
    )
    .bind(&key_hash)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();
    
    if let Some(uid) = user_id {
        audit::logout(uid, "unknown", &key_prefix);
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ============================================
// API Key Management Routes
// ============================================

/// List user's API keys
pub async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    let keys: Vec<ApiKey> = sqlx::query_as(
        "SELECT * FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC",
    )
    .bind(auth.user_id)
    .fetch_all(&state.db)
    .await
    .map_err(AuthError::Database)?;

    let response: Vec<ApiKeyResponse> = keys.into_iter().map(ApiKeyResponse::from).collect();

    Ok(Json(serde_json::json!({ "api_keys": response })).into_response())
}

/// Create a new API key
pub async fn create_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateApiKey>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    let api_key = generate_api_key();
    let key_hash = hash_api_key(&api_key);
    let key_prefix = get_key_prefix(&api_key);

    // Calculate expiration if provided
    let expires_at = payload.expires_in_days.map(|days| {
        chrono::Utc::now() + chrono::Duration::days(days as i64)
    });

    let key_record: ApiKey = sqlx::query_as(
        r#"
        INSERT INTO api_keys (user_id, name, key_hash, key_prefix, expires_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
    )
    .bind(auth.user_id)
    .bind(&payload.name)
    .bind(&key_hash)
    .bind(&key_prefix)
    .bind(expires_at)
    .fetch_one(&state.db)
    .await
    .map_err(AuthError::Database)?;

    audit::api_key_create(auth.user_id, &key_prefix);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!(ApiKeyCreated {
            api_key,
            api_key_response: ApiKeyResponse::from(key_record),
        })),
    )
        .into_response())
}

/// Get API key details
pub async fn get_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(key_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    let key: ApiKey = sqlx::query_as("SELECT * FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(key_id)
        .bind(auth.user_id)
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::InvalidApiKey)?;

    Ok(Json(serde_json::json!({ "api_key": ApiKeyResponse::from(key) })).into_response())
}

/// Revoke an API key
pub async fn delete_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(key_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    eprintln!("DEBUG: delete_api_key called with id: {}", key_id);
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    let result = sqlx::query("DELETE FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(key_id)
        .bind(auth.user_id)
        .execute(&state.db)
        .await
        .map_err(AuthError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AuthError::InvalidApiKey);
    }

    audit::api_key_revoke(auth.user_id, key_id);

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ============================================
// User Management Routes (Admin only)
// ============================================

/// List all users (admin only)
pub async fn list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    if auth.role != "admin" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Admin access required" })),
        )
            .into_response());
    }

    let users: Vec<User> =
        sqlx::query_as("SELECT * FROM users ORDER BY created_at DESC")
            .fetch_all(&state.db)
            .await
            .map_err(AuthError::Database)?;

    let response: Vec<UserResponse> = users.into_iter().map(UserResponse::from).collect();

    Ok(Json(serde_json::json!({ "users": response })).into_response())
}

/// Get user by ID
pub async fn get_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(user_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    // Users can only view their own profile, admins can view anyone
    if auth.role != "admin" && auth.user_id != user_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Access denied" })),
        )
            .into_response());
    }

    let user: User = sqlx::query_as("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::UserNotFound)?;

    Ok(Json(serde_json::json!({ "user": UserResponse::from(user) })).into_response())
}

/// Update user
pub async fn update_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(user_id): axum::extract::Path<Uuid>,
    Json(payload): Json<UpdateUser>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    // Users can only update their own profile, admins can update anyone
    if auth.role != "admin" && auth.user_id != user_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Access denied" })),
        )
            .into_response());
    }

    // Non-admins cannot change role
    if payload.role.is_some() && auth.role != "admin" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Only admins can change user role" })),
        )
            .into_response());
    }

    // Build dynamic update query
    let mut updates = Vec::new();
    let mut param_idx = 1;

    if payload.email.is_some() {
        updates.push(format!("email = ${}", param_idx));
        param_idx += 1;
    }
    if payload.name.is_some() {
        updates.push(format!("name = ${}", param_idx));
        param_idx += 1;
    }
    if payload.role.is_some() && auth.role == "admin" {
        updates.push(format!("role = ${}", param_idx));
        param_idx += 1;
    }

    if updates.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "No fields to update" })),
        )
            .into_response());
    }

    let query = format!(
        "UPDATE users SET updated_at = NOW(), {} WHERE id = ${} RETURNING *",
        updates.join(", "),
        param_idx
    );

    let mut db_query = sqlx::query_as::<_, User>(&query);

    if let Some(ref v) = payload.email {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.name {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.role {
        db_query = db_query.bind(v);
    }

    db_query = db_query.bind(user_id);

    let user = db_query
        .fetch_optional(&state.db)
        .await
        .map_err(AuthError::Database)?
        .ok_or(AuthError::UserNotFound)?;

    Ok(Json(serde_json::json!({ "user": UserResponse::from(user) })).into_response())
}

/// Delete user (admin only)
pub async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(user_id): axum::extract::Path<Uuid>,
) -> Result<Response, AuthError> {
    let auth = extract_auth_user(&state.db, &headers).await?;
    
    if auth.role != "admin" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Admin access required" })),
        )
            .into_response());
    }

    // Cannot delete yourself
    if auth.user_id == user_id {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Cannot delete your own account" })),
        )
            .into_response());
    }

    let result = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&state.db)
        .await
        .map_err(AuthError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AuthError::UserNotFound);
    }

    audit::user_delete(auth.user_id, user_id);

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ============================================
// Router
// ============================================

pub fn create_auth_router() -> Router<AppState> {
    Router::new()
        // Auth routes (no authentication required)
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        // Auth routes (authentication required)
        .route("/auth/logout", post(logout))
        // API Key routes (authentication required)
        .route("/api-keys", get(list_api_keys))
        .route("/api-keys", post(create_api_key))
        .route("/api-keys/:id", get(get_api_key))
        .route("/api-keys/:id", delete(delete_api_key))
        // User routes (authentication required)
        .route("/users", get(list_users))
        .route("/users/:id", get(get_user))
        .route("/users/:id", patch(update_user))
        .route("/users/:id", delete(delete_user))
}
