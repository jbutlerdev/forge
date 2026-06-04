// `forge-api` is a library + binary. The binary in `main.rs`
// declares its own `mod xxx;` for every module below (it
// pre-dates the library) so the binary never goes through this
// public surface. That means many of the `pub` items below are
// not "used" from the binary's point of view and trigger
// `dead_code` lints. The lib is still the public API for
// integration / e2e tests (`use forge_api::...` in
// `tests/*.rs`) and for any external consumer that might link
// against it, so we keep the visibility but silence the lints
// here.
#![allow(dead_code)]

pub mod agent_registry;
pub mod api;
pub mod bus;
pub mod db;
pub mod logging;
pub mod observability;
pub mod pi_agent;
pub mod recording;
pub mod resume;
pub mod sandbox;
pub mod session_manager;
pub mod session_replay;
pub mod tool_executor;

pub use agent_registry::{AgentRegistry, AgentRegistryError, SharedPiAgent};
pub use api::auth::{AuthError, AuthenticatedUser};
pub use bus::{BusEvent, MessageBus};
pub use db::{
    ApiKey, ApiKeyCreated, ApiKeyResponse, CreateApiKey, CreateMessage, CreateProfile,
    CreateSession, CreateUser, LoginRequest, LoginResponse, Message, Profile, Session,
    UpdateProfile, UpdateUser, User, UserResponse,
};
pub use logging::audit as audit_log;
pub use logging::{log_audit, request_log_middleware, AuditEvent, LogContext};
pub use observability::{Metrics, MetricsSnapshot, ObservabilityState};
pub use pi_agent::{PiAgent, PiConfig, PiError, PiEvent};
pub use sandbox::{SandboxContainer, SandboxError, SandboxManager, SandboxState};
pub use session_manager::{SessionError, SessionManager, SessionState};
pub use tool_executor::{ToolError, ToolExecutor, ToolInput, ToolOutput};
