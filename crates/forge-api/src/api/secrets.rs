//! Herd H6.5: the named credential slots API (the secret-store seam).
//!
//! `GET  /secrets`              — the caller's slot NAMES + expiry. The
//!                                values are never serialized, anywhere.
//! `PUT  /secrets/:name`        — upsert `{value, valid_until?}`.
//! `DELETE /secrets/:name`      — drop a slot.
//!
//! Tenancy is strict-owner (the slots are the user's own credentials;
//! admins manage their own, not other users', matching the
//! /api-keys posture). Restricted (demo) keys are barred — a demo key
//! must not be able to read, set, or erase credential slots.
//!
//! Slot names are validated `^[A-Z][A-Z0-9_]*$` (≤ 64) because they
//! become sandbox env var names (`--setenv=` args) and appear in
//! agent `credentials_scope.env_refs`.

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;

use super::{err_resp, AppState};
use crate::api::auth::AuthenticatedUser;
use crate::credentials::{delete_secret, list_secret_meta, set_secret};

/// Validate a slot name: `A-Z` start, `A-Z0-9_` after, ≤ 64.
pub fn valid_slot_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().enumerate().all(|(i, b)| {
            if i == 0 {
                b.is_ascii_uppercase()
            } else {
                b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'
            }
        })
}

/// `GET /secrets` → `{secrets: [{name, valid_until, updated_at}]}`.
pub(crate) async fn list_secrets(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Response {
    match list_secret_meta(&state.db, user.user_id).await {
        Ok(meta) => Json(serde_json::json!({ "secrets": meta })).into_response(),
        Err(e) => {
            tracing::error!("GET /secrets failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list secrets",
            )
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct UpsertSecretBody {
    pub value: String,
    /// RFC 3339; absent/`null` = no expiry.
    #[serde(default)]
    pub valid_until: Option<chrono::DateTime<chrono::Utc>>,
}

/// `PUT /secrets/:name` → `{ok: true}`.
pub(crate) async fn upsert_secret(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(name): Path<String>,
    Json(body): Json<UpsertSecretBody>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: secret management is not available",
        );
    }
    if !valid_slot_name(&name) {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "slot name must match ^[A-Z][A-Z0-9_]*$ (max 64 chars)",
        );
    }
    if body.value.is_empty() {
        return err_resp(
            &state,
            StatusCode::BAD_REQUEST,
            "value must be non-empty (use DELETE to drop a slot)",
        );
    }
    match set_secret(
        &state.db,
        user.user_id,
        &name,
        &body.value,
        body.valid_until,
    )
    .await
    {
        Ok(()) => {
            // The value is logged NOWHERE.
            state.metrics.inc_requests("PUT /secrets/:name");
            Json(serde_json::json!({ "ok": true })).into_response()
        }
        Err(e) => {
            tracing::error!("failed to store secret slot '{name}': {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store secret",
            )
        }
    }
}

/// `DELETE /secrets/:name` → `{ok: true}` (204-style; `ok` is false-
/// shape-free: unknown slots are still a clean 200, the audit line is
/// the point, not an existence oracle).
pub(crate) async fn remove_secret(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(name): Path<String>,
) -> Response {
    if user.restricted {
        return err_resp(
            &state,
            StatusCode::FORBIDDEN,
            "Restricted key: secret management is not available",
        );
    }
    match delete_secret(&state.db, user.user_id, &name).await {
        Ok(_) => {
            state.metrics.inc_requests("DELETE /secrets/:name");
            Json(serde_json::json!({ "ok": true })).into_response()
        }
        Err(e) => {
            tracing::error!("DELETE /secrets/{name} failed: {e}");
            err_resp(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to delete secret",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::valid_slot_name;

    #[test]
    fn slot_name_validation() {
        assert!(valid_slot_name("GITHUB_TOKEN"));
        assert!(valid_slot_name("A"));
        assert!(valid_slot_name("BILLING_COOKIE_V2"));
        assert!(!valid_slot_name(""));
        assert!(!valid_slot_name("lowercase"));
        assert!(!valid_slot_name("1STARTS"));
        assert!(!valid_slot_name("HAS-DASH"));
        assert!(!valid_slot_name("has space"));
        assert!(!valid_slot_name(&"A".repeat(65)));
    }

    /// The value-free list shape: serialize a meta row and assert a
    /// value field can't even exist on the wire type.
    #[test]
    fn list_shape_never_carries_values() {
        use crate::credentials::SecretMeta;
        let meta = SecretMeta {
            name: "SLOT_A".into(),
            valid_until: None,
            updated_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(!json.contains("value"));
        assert!(json.contains("\"name\":\"SLOT_A\""));
    }
}
