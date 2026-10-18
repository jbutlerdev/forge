//! Bitwarden connection API (2026-10-18).
//!
//! Owner-scoped CRUD for the vault connection + the read-only feeds
//! the UI needs. Strict-owner tenancy (like `/secrets`): agents (any
//! `X-Agent-Id`) are denied — configuring WHERE secrets come from is
//! a human action; agents only ever benefit indirectly at exec time.
//! No endpoint here ever returns a secret VALUE: `GET /bitwarden`
//! returns masked metadata, `GET /bitwarden/secrets` returns key
//! NAMES only.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;

use super::auth::AuthenticatedUser;
use super::{err_resp, AppState};
use crate::bitwarden;

fn deny_agents(user: &AuthenticatedUser) -> bool {
    user.agent_id.is_some()
}

#[derive(Deserialize)]
pub(crate) struct BitwardenConfigBody {
    server_url: String,
    org_id: String,
    client_id: String,
    client_secret: String,
}

pub(crate) async fn put_config(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(payload): Json<BitwardenConfigBody>,
) -> Response {
    if deny_agents(&user) {
        return err_resp(&state, StatusCode::FORBIDDEN, "only the user configures the vault");
    }
    if !bitwarden::valid_server_url(&payload.server_url) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "server_url must be https (or http for localhost/LAN)",
        );
    }
    if payload.org_id.trim().is_empty()
        || payload.client_id.trim().is_empty()
        || payload.client_secret.trim().is_empty()
    {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "org_id, client_id and client_secret are all required",
        );
    }
    match bitwarden::upsert_config(
        &state.db,
        user.user_id,
        &payload.server_url,
        &payload.org_id,
        &payload.client_id,
        &payload.client_secret,
    )
    .await
    {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to store vault config: {e}"),
        ),
    }
}

pub(crate) async fn get_config(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Response {
    if deny_agents(&user) {
        return err_resp(&state, StatusCode::FORBIDDEN, "only the user reads the vault config");
    }
    match bitwarden::config_meta(&state.db, user.user_id).await {
        Ok(meta) => Json(json!({ "configured": meta.is_some(), "config": meta })).into_response(),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("vault config read failed: {e}"),
        ),
    }
}

pub(crate) async fn delete_config(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Response {
    if deny_agents(&user) {
        return err_resp(&state, StatusCode::FORBIDDEN, "only the user removes the vault config");
    }
    match bitwarden::delete_config(&state.db, user.user_id).await {
        Ok(removed) => Json(json!({ "ok": true, "removed": removed })).into_response(),
        Err(e) => err_resp(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("vault config delete failed: {e}"),
        ),
    }
}

/// Mint a token and count the org's secrets — the config screen's
/// "test connection" button. Reports counts, never values.
pub(crate) async fn test_connection(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Response {
    if deny_agents(&user) {
        return err_resp(&state, StatusCode::FORBIDDEN, "only the user may test the vault");
    }
    match bitwarden::list_bw_keys(&state.db, user.user_id).await {
        Ok(keys) => Json(json!({ "ok": true, "secrets": keys.len() })).into_response(),
        Err(e) => Json(json!({ "ok": false, "error": e })).into_response(),
    }
}

/// SM secret KEY names (picker feed for binding slots to
/// `bw://<key>`); values never leave the vault.
pub(crate) async fn list_secrets(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Response {
    if deny_agents(&user) {
        return err_resp(&state, StatusCode::FORBIDDEN, "only the user browses the vault");
    }
    match bitwarden::list_bw_keys(&state.db, user.user_id).await {
        Ok(keys) => Json(json!({ "keys": keys })).into_response(),
        Err(e) => err_resp(&state, StatusCode::BAD_GATEWAY, &e),
    }
}
