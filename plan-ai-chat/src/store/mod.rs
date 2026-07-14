//! Abstract session/message/token store for chat sessions.
//!
//! Domain crates either use [`pg::PgChatStore`] directly (with their table
//! names via [`pg::PgTables`]) or implement [`ChatStore`] themselves and add
//! domain-specific extension traits on top (the healer does both).

#[cfg(feature = "postgres")]
pub mod migrations;
#[cfg(feature = "postgres")]
pub mod pg;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::model::{ChatMessage, ChatSession};

/// Type-erased store shared across a chat subsystem.
pub type DynChatStore = Arc<dyn ChatStore>;

/// Parameters for creating a session (keeps the trait method signature sane).
#[derive(Debug, Clone, Default)]
pub struct NewSession<'a> {
    pub scope_id: Uuid,
    pub subject: &'a str,
    pub created_by: &'a str,
    pub initial_context: &'a serde_json::Value,
    pub state_data: &'a serde_json::Value,
    pub provider: Option<&'a str>,
    pub model: Option<&'a str>,
    pub label: Option<&'a str>,
}

#[async_trait]
pub trait ChatStore: Send + Sync + 'static {
    // -- Session lifecycle ------------------------------------------------

    async fn create_session(&self, new: NewSession<'_>) -> Result<Uuid>;

    async fn set_label(&self, session_id: Uuid, label: &str) -> Result<()>;

    /// Transition a session to a new state. `terminal` controls whether
    /// `completed_at` is stamped. Implementations MUST also persist a
    /// `state_change` chat message with the reason (if present in `state_data`).
    async fn transition_state(
        &self,
        session_id: Uuid,
        new_state: &str,
        terminal: bool,
        state_data: &serde_json::Value,
    ) -> Result<()>;

    /// Transition to `failed` with an error message.
    /// Implementations MUST also persist a `state_change` chat message.
    async fn fail_session(
        &self,
        session_id: Uuid,
        error_message: &str,
        state_data: &serde_json::Value,
    ) -> Result<()>;

    async fn get_session(&self, session_id: Uuid) -> Result<Option<ChatSession>>;
    async fn list_sessions(&self, scope_id: Uuid) -> Result<Vec<ChatSession>>;

    /// Sessions eligible for auto-resume: state not in `non_resumable`, and
    /// `created_by` not in `exclude_created_by`. Ordered oldest-first.
    async fn find_resumable(
        &self,
        non_resumable: &[&str],
        exclude_created_by: &[&str],
    ) -> Result<Vec<ChatSession>>;

    async fn update_provider_model(
        &self,
        session_id: Uuid,
        provider: &str,
        model: &str,
    ) -> Result<()>;

    /// Whether any session for `subject` is in a state NOT listed in
    /// `inactive_states`.
    async fn has_running_session(&self, subject: &str, inactive_states: &[&str]) -> Result<bool>;

    // -- Messages ---------------------------------------------------------

    async fn append_message(
        &self,
        session_id: Uuid,
        role: &str,
        content: &str,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()>;

    async fn get_messages(&self, session_id: Uuid) -> Result<Vec<ChatMessage>>;

    async fn get_messages_after(
        &self,
        session_id: Uuid,
        after: DateTime<Utc>,
    ) -> Result<Vec<ChatMessage>>;

    // -- Token usage tracking ----------------------------------------------

    /// Append a token usage event and update the session's running total.
    /// Returns the new total tokens_used for budget checking.
    async fn append_token_event(
        &self,
        session_id: Uuid,
        provider: &str,
        model: &str,
        input_tokens: u32,
        output_tokens: u32,
    ) -> Result<u64>;

    /// Get the current token usage for a session (denormalized total).
    async fn get_token_usage(&self, session_id: Uuid) -> Result<u64>;

    /// Set the token budget for a session.
    async fn set_token_budget(&self, session_id: Uuid, budget: u64) -> Result<()>;

    /// Get the token budget for a session.
    async fn get_token_budget(&self, session_id: Uuid) -> Result<u64>;
}
