//! Host-side backend for the chat sidebar's `#[server]` functions.
//!
//! The sidebar's server functions are defined in this crate (so the whole UI
//! is reusable), but session management is host-specific: hosts implement
//! [`ChatBackend`] over their chat service and register it once at startup
//! with [`set_chat_backend`]. Implementations resolve the current user
//! themselves (e.g. via `dioxus::fullstack::extract()` in the request task) —
//! the trait deliberately carries no user parameter.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;

use crate::sidebar::{ChatContext, ChatSessionMeta, ChatSessionSummary};

/// Errors are plain strings — they surface as `ServerFnError` messages.
pub type BackendResult<T> = Result<T, String>;

#[async_trait]
pub trait ChatBackend: Send + Sync + 'static {
    /// Whether chat is enabled for the current user + the model picker list.
    async fn context(&self) -> BackendResult<ChatContext>;
    async fn list_sessions(&self) -> BackendResult<Vec<ChatSessionSummary>>;
    async fn start_session(
        &self,
        provider: Option<String>,
        model: Option<String>,
        message: String,
        page_context: Option<String>,
    ) -> BackendResult<String>;
    async fn send_message(
        &self,
        session_id: String,
        message: String,
        page_context: Option<String>,
    ) -> BackendResult<()>;
    /// decision: "approve" | "approve_all" | "deny"
    async fn approve(
        &self,
        session_id: String,
        approval_id: String,
        decision: String,
        reason: Option<String>,
    ) -> BackendResult<()>;
    async fn pause(&self, session_id: String) -> BackendResult<()>;
    async fn cancel(&self, session_id: String) -> BackendResult<()>;
    async fn extend_budget(&self, session_id: String) -> BackendResult<()>;
    async fn set_auto_approve(&self, session_id: String, value: bool) -> BackendResult<()>;
    async fn session_meta(&self, session_id: String) -> BackendResult<ChatSessionMeta>;
}

static BACKEND: OnceLock<Arc<dyn ChatBackend>> = OnceLock::new();

/// Register the host's chat backend. Call once at startup, after the chat
/// service is initialized. Without a registered backend the sidebar reports
/// chat as disabled and renders nothing.
pub fn set_chat_backend(backend: Arc<dyn ChatBackend>) {
    let _ = BACKEND.set(backend);
}

pub(crate) fn backend() -> Option<&'static Arc<dyn ChatBackend>> {
    BACKEND.get()
}
