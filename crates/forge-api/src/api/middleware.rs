//! Authentication middleware helpers
//!
//! This module provides utilities for authentication.

use crate::api::auth::AuthenticatedUser;

/// Extension trait for extracting authenticated user from request extensions
pub trait AuthExt {
    fn user(&self) -> Option<AuthenticatedUser>;
}
