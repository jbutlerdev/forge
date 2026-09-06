//! Profile CRUD handlers: `POST/GET /profiles`, the path-based
//! `/profiles/:id` and query-based `/profiles/get|update|delete`
//! routes, plus the shared provider-validation gate.

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;
use uuid::Uuid;

use super::{db_err, err_resp, AppState};
use crate::api::auth::{can_access, AuthenticatedUser};
use crate::db::{CreateProfile, Profile, UpdateProfile};

/// Providers forge knows how to wire an API key for (see
/// `pi_agent.rs`). Kept in sync with the `profiles.provider` CHECK
/// constraint (migration 005) so the handler can reject an unknown
/// provider with a 400 *before* it reaches the DB (the CHECK is the
/// backstop, not the primary gate). Add new providers here AND in
/// the migration when introducing one.
const ALLOWED_PROVIDERS: &[&str] = &[
    "openai",
    "anthropic",
    "proxy-anthropic",
    "proxy",
    "google",
    "gemini",
    "custom",
];

/// Return a 400 if `provider` is not in [`ALLOWED_PROVIDERS`].
/// Centralized so `create_profile` and `update_profile` share one
/// gate (and one list).
fn validate_provider(state: &AppState, provider: &str) -> Option<Response> {
    if ALLOWED_PROVIDERS.contains(&provider) {
        None
    } else {
        Some(err_resp(
            state,
            StatusCode::BAD_REQUEST,
            &format!(
                "invalid provider '{}'; expected one of: {}",
                provider,
                ALLOWED_PROVIDERS.join(", ")
            ),
        ))
    }
}

pub(crate) async fn create_profile(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<CreateProfile>,
) -> Response {
    // The redacted sentinel is only ever echoed back by the UI's
    // edit form, never intended for a create (the create form
    // starts empty). Storing it would mint a profile whose provider
    // key is a placeholder.
    if payload.api_key.as_deref() == Some(crate::db::REDACTED_SECRET) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "api_key is the redacted placeholder; provide a real key",
        );
    }
    if let Some(resp) = validate_provider(&state, &payload.provider) {
        return resp;
    }
    let tools_json = payload
        .tools
        .as_ref()
        .map(|t| serde_json::to_string(t).unwrap_or_default())
        .unwrap_or_else(|| r#"["bash", "read", "write", "edit"]"#.to_string());

    match sqlx::query_as::<_, Profile>(
        r#"INSERT INTO profiles (name, description, provider, model, base_url, api_key, working_dir, git_url, git_ref, nix_shell, system_prompt, tools, user_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING *"#
    ).bind(&payload.name).bind(&payload.description).bind(&payload.provider).bind(&payload.model)
     .bind(&payload.base_url).bind(&payload.api_key).bind(&payload.working_dir).bind(&payload.git_url)
     .bind(&payload.git_ref).bind(&payload.nix_shell).bind(payload.system_prompt.as_deref().unwrap_or("You are a helpful coding assistant."))
     .bind(&tools_json).bind(user.user_id).fetch_one(&state.db).await {
        Ok(p) => {
            state.metrics.inc_requests("POST /profiles");
            (StatusCode::CREATED, Json(serde_json::json!({ "profile": p }))).into_response()
        }
        // Postgres unique-constraint violation: `profiles.name`
        // is `UNIQUE NOT NULL`. Return 409 Conflict (not 500) so
        // clients can distinguish "name already taken" from
        // other failures. The body includes the conflicting
        // name so `forge-agent-setup` can use it to look up
        // the existing profile and treat the call as a
        // successful idempotent re-run. Hit on the first run
        // of the script for an agent whose name was previously
        // provisioned (e.g. operator manually created the
        // profile, or the cached `profile.id` in agent.yaml
        // was wiped via `yq del .profile.id`).
        Err(sqlx::Error::Database(db_err)) if db_err.constraint() == Some("profiles_name_key") => {
            tracing::info!(
                name = %payload.name,
                "POST /profiles: profile name already exists; returning 409"
            );
            state.metrics.inc_requests("POST /profiles");
            err_resp(
                &state,
                StatusCode::CONFLICT,
                &format!("profile name '{}' already exists", payload.name),
            )
        }
        Err(e) => {
            tracing::error!("Failed to create profile: {e}");
            err_resp(&state, StatusCode::INTERNAL_SERVER_ERROR, "Failed to create profile")
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct ListProfilesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

pub(crate) async fn list_profiles(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(query): Query<ListProfilesQuery>,
) -> Response {
    // Tenancy: admins see every profile; a regular user sees only the
    // rows they own. Pre-tenancy rows (`user_id IS NULL`) are
    // admin-only, so they are invisible to regular users either way.
    let rows = if can_access(&user, None) {
        sqlx::query_as::<_, Profile>(
            "SELECT * FROM profiles ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        )
        .bind(query.limit.unwrap_or(50))
        .bind(query.offset.unwrap_or(0))
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, Profile>(
            "SELECT * FROM profiles WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
        )
        .bind(user.user_id)
        .bind(query.limit.unwrap_or(50))
        .bind(query.offset.unwrap_or(0))
        .fetch_all(&state.db)
        .await
    };
    match rows {
        Ok(p) => {
            state.metrics.inc_requests("GET /profiles");
            Json(serde_json::json!({ "profiles": p })).into_response()
        }
        Err(e) => db_err(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list profiles",
            e,
        ),
    }
}

pub(crate) async fn get_profile_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    get_profile_core(&state, &user, id).await
}

pub(crate) async fn delete_profile_by_uuid(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(id): Path<Uuid>,
) -> Response {
    delete_profile_core(&state, &user, id).await
}

/// Shared body of the path-based (`/profiles/:id`) and query-based
/// (`/profiles/get?id=`) profile fetchers. Both routes exist for
/// backward compatibility; the logic is identical.
async fn get_profile_core(state: &AppState, user: &AuthenticatedUser, id: Uuid) -> Response {
    match sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        // 404 (not 403) for rows the caller cannot see: don't leak
        // existence of other users' profiles.
        Ok(Some(p)) if can_access(user, p.user_id) => {
            Json(serde_json::json!({ "profile": p })).into_response()
        }
        Ok(_) => err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get profile",
            e,
        ),
    }
}

/// Shared body of the path-based and query-based profile deleters.
async fn delete_profile_core(state: &AppState, user: &AuthenticatedUser, id: Uuid) -> Response {
    // Fetch first so an inaccessible (or missing) profile 404s
    // identically instead of leaking existence via a successful no-op
    // delete.
    let owner: Option<Option<Uuid>> =
        match sqlx::query_scalar("SELECT user_id FROM profiles WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                return db_err(
                    state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to delete profile",
                    e,
                )
            }
        };
    let owner = match owner {
        Some(o) => o,
        None => return err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
    };
    if !can_access(user, owner) {
        return err_resp(state, StatusCode::NOT_FOUND, "Profile not found");
    }

    // Guard: refuse to delete a profile that still has sessions.
    // sessions.profile_id has an ON DELETE CASCADE FK, so a bare
    // `DELETE FROM profiles` silently destroys every session + its
    // message history for that profile.  Return 409 instead; the
    // operator must delete the sessions first (or use an explicit
    // future `force` flag).
    let session_count: i64 = match sqlx::query_scalar(
        "SELECT COUNT(*) FROM sessions WHERE profile_id = $1",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return db_err(
                state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to check sessions",
                e,
            )
        }
    };
    if session_count > 0 {
        return err_resp(
            state,
            StatusCode::CONFLICT,
            &format!(
                "Profile has {} session(s). Delete the sessions first; \
                 deleting the profile would destroy their history.",
                session_count
            ),
        );
    }

    match sqlx::query("DELETE FROM profiles WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete profile",
            e,
        ),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProfileQuery {
    id: Uuid,
}
/// **Deprecated** query-based alias of the canonical path route
/// `GET /profiles/{id}`. Kept for CLI / web-UI compatibility; see
/// the "Deprecated routes" note in `docs/API.md`.
pub(crate) async fn get_profile_by_id(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<ProfileQuery>,
) -> Response {
    get_profile_core(&state, &user, params.id).await
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteProfileQuery {
    id: Uuid,
}
/// **Deprecated** query-based alias of the canonical path route
/// `DELETE /profiles/{id}`. Kept for CLI / web-UI compatibility;
/// see the "Deprecated routes" note in `docs/API.md`.
pub(crate) async fn delete_profile_by_id(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<DeleteProfileQuery>,
) -> Response {
    delete_profile_core(&state, &user, params.id).await
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateProfileQuery {
    id: Uuid,
}
/// **Deprecated** query-based update route (`PATCH /profiles/update?id=`).
/// No path-based equivalent exists yet; this remains the only update
/// form. See the "Deprecated routes" note in `docs/API.md`.
pub(crate) async fn update_profile_by_id(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(params): Query<UpdateProfileQuery>,
    Json(payload): Json<UpdateProfile>,
) -> Response {
    update_profile_internal(&state, &user, params.id, payload).await
}

async fn update_profile_internal(
    state: &AppState,
    user: &AuthenticatedUser,
    id: Uuid,
    payload: UpdateProfile,
) -> Response {
    if let Some(ref provider) = payload.provider {
        if let Some(resp) = validate_provider(state, provider) {
            return resp;
        }
    }
    // Tenancy gate before any work: the profile must exist AND the
    // caller must be its owner (or an admin).
    let owner: Option<Option<Uuid>> =
        match sqlx::query_scalar("SELECT user_id FROM profiles WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.db)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                return db_err(
                    state,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to update profile",
                    e,
                )
            }
        };
    let owner = match owner {
        Some(o) => o,
        None => return err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
    };
    if !can_access(user, owner) {
        return err_resp(state, StatusCode::NOT_FOUND, "Profile not found");
    }
    // The UI echoes the masked api_key back unchanged when you save
    // a form without touching the key field; treat that as "keep
    // the stored key" instead of persisting the sentinel. Only a
    // genuinely new (non-sentinel) value updates the column.
    let api_key_redacted = payload.api_key.as_deref() == Some(crate::db::REDACTED_SECRET);
    let mut query = "UPDATE profiles SET updated_at = NOW()".to_string();
    let mut param_idx = 1;
    let mut params: Vec<String> = Vec::new();

    macro_rules! add_param {
        ($field:expr, $name:expr) => {
            if $field.is_some() {
                params.push(format!("{} = ${}", $name, param_idx));
                param_idx += 1;
            }
        };
    }
    add_param!(payload.name, "name");
    add_param!(payload.description, "description");
    add_param!(payload.provider, "provider");
    add_param!(payload.model, "model");
    add_param!(payload.base_url, "base_url");
    if !api_key_redacted {
        add_param!(payload.api_key, "api_key");
    }
    add_param!(payload.working_dir, "working_dir");
    add_param!(payload.git_url, "git_url");
    add_param!(payload.git_ref, "git_ref");
    add_param!(payload.nix_shell, "nix_shell");
    add_param!(payload.system_prompt, "system_prompt");
    add_param!(payload.tools, "tools");

    if params.is_empty() {
        return err_resp(state, StatusCode::BAD_REQUEST, "No fields to update");
    }

    query.push_str(", ");
    query.push_str(&params.join(", "));
    query.push_str(&format!(" WHERE id = ${} RETURNING *", param_idx));

    let mut db_query = sqlx::query_as::<_, Profile>(&query);
    if let Some(ref v) = payload.name {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.description {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.provider {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.model {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.base_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.api_key {
        // Keeps the SET-clause/bind ordering in sync: when the
        // sentinel arrived, api_key was left out of the SET list.
        if !api_key_redacted {
            db_query = db_query.bind(v);
        }
    }
    if let Some(ref v) = payload.working_dir {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.git_url {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.git_ref {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.nix_shell {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.system_prompt {
        db_query = db_query.bind(v);
    }
    if let Some(ref v) = payload.tools {
        db_query = db_query.bind(serde_json::to_string(v).unwrap_or_default());
    }
    db_query = db_query.bind(id);

    match db_query.fetch_optional(&state.db).await {
        Ok(Some(p)) => Json(serde_json::json!({ "profile": p })).into_response(),
        Ok(None) => err_resp(state, StatusCode::NOT_FOUND, "Profile not found"),
        Err(e) => db_err(
            state,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update profile",
            e,
        ),
    }
}
