//! Postgres-backed implementation of [`ChatStore`] over the unified
//! `chat_sessions`/`chat_messages`/`chat_token_events` tables.
//!
//! One store instance serves one domain, selected by the `session_type`
//! discriminator ('chat', 'healer', ...): `create_session` stamps it and the
//! cross-session queries (`list_sessions`, `find_resumable`,
//! `has_running_session`) filter by it. By-id lookups don't filter — session
//! ids are unique across types.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{ChatStore, NewSession};
use crate::model::{ChatMessage, ChatSession};

const SESSION_SELECT: &str = "SELECT id, scope_id, subject, state, state_data, created_by, \
            created_at, updated_at, completed_at, error_message, \
            initial_context, provider, model, label \
     FROM chat_sessions";

/// [`ChatStore`] backed by a Postgres connection pool, scoped to one
/// `session_type`.
#[derive(Clone)]
pub struct PgChatStore {
    pool: PgPool,
    session_type: &'static str,
}

impl PgChatStore {
    pub fn new(pool: PgPool, session_type: &'static str) -> Self {
        Self { pool, session_type }
    }

    /// Return the underlying pool (escape hatch for callers that still need it).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The `session_type` this store instance reads and writes.
    pub fn session_type(&self) -> &'static str {
        self.session_type
    }

    /// Persist a `state_change` message to the session chat log.
    async fn append_state_change(
        &self,
        session_id: Uuid,
        state: &str,
        state_data: &serde_json::Value,
    ) {
        let reason = state_data
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let content = serde_json::json!({
            "state": state,
            "reason": reason,
        })
        .to_string();
        let _ = sqlx::query(
            "INSERT INTO chat_messages (session_id, role, content, metadata) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(session_id)
        .bind("state_change")
        .bind(&content)
        .bind(Some(state_data))
        .execute(&self.pool)
        .await;
    }
}

// ── sqlx row types ─────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct SessionRow {
    id: Uuid,
    scope_id: Uuid,
    subject: String,
    state: String,
    state_data: serde_json::Value,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    error_message: Option<String>,
    initial_context: serde_json::Value,
    provider: Option<String>,
    model: Option<String>,
    label: Option<String>,
}

impl From<SessionRow> for ChatSession {
    fn from(r: SessionRow) -> Self {
        Self {
            id: r.id,
            scope_id: r.scope_id,
            subject: r.subject,
            state: r.state,
            state_data: r.state_data,
            created_by: r.created_by,
            created_at: r.created_at,
            updated_at: r.updated_at,
            completed_at: r.completed_at,
            error_message: r.error_message,
            initial_context: r.initial_context,
            provider: r.provider,
            model: r.model,
            label: r.label,
        }
    }
}

#[derive(sqlx::FromRow)]
struct MessageRow {
    id: Uuid,
    session_id: Uuid,
    role: String,
    content: String,
    metadata: Option<serde_json::Value>,
    created_at: DateTime<Utc>,
}

impl From<MessageRow> for ChatMessage {
    fn from(r: MessageRow) -> Self {
        Self {
            id: r.id,
            session_id: r.session_id,
            role: r.role,
            content: r.content,
            metadata: r.metadata,
            created_at: r.created_at,
        }
    }
}

// ── Trait implementation ───────────────────────────────────────────────

#[async_trait]
impl ChatStore for PgChatStore {
    async fn create_session(&self, new: NewSession<'_>) -> Result<Uuid> {
        let id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO chat_sessions (scope_id, subject, state, state_data, created_by, \
                                        initial_context, provider, model, label, session_type) \
             VALUES ($1, $2, 'created', $3, $4, $5, $6, $7, $8, $9) \
             RETURNING id",
        )
        .bind(new.scope_id)
        .bind(new.subject)
        .bind(new.state_data)
        .bind(new.created_by)
        .bind(new.initial_context)
        .bind(new.provider)
        .bind(new.model)
        .bind(new.label)
        .bind(self.session_type)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    async fn set_label(&self, session_id: Uuid, label: &str) -> Result<()> {
        sqlx::query("UPDATE chat_sessions SET label = $1 WHERE id = $2")
            .bind(label)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn transition_state(
        &self,
        session_id: Uuid,
        new_state: &str,
        terminal: bool,
        state_data: &serde_json::Value,
    ) -> Result<()> {
        // Terminal: stamp completed_at (keeping the first completion time).
        // Non-terminal: clear it — sessions can resume out of "completed"
        // (interactive idle-parking), and a stale timestamp would linger.
        if terminal {
            sqlx::query(
                "UPDATE chat_sessions \
                 SET state = $1, state_data = $2, updated_at = now(), \
                     completed_at = COALESCE(completed_at, now()) \
                 WHERE id = $3",
            )
            .bind(new_state)
            .bind(state_data)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                "UPDATE chat_sessions \
                 SET state = $1, state_data = $2, updated_at = now(), completed_at = NULL \
                 WHERE id = $3",
            )
            .bind(new_state)
            .bind(state_data)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        }

        self.append_state_change(session_id, new_state, state_data)
            .await;
        Ok(())
    }

    async fn fail_session(
        &self,
        session_id: Uuid,
        error_message: &str,
        state_data: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE chat_sessions \
             SET state = 'failed', state_data = $1, error_message = $2, \
                 updated_at = now(), completed_at = now() \
             WHERE id = $3",
        )
        .bind(state_data)
        .bind(error_message)
        .bind(session_id)
        .execute(&self.pool)
        .await?;

        let data = serde_json::json!({"reason": error_message});
        self.append_state_change(session_id, "failed", &data).await;
        Ok(())
    }

    async fn get_session(&self, session_id: Uuid) -> Result<Option<ChatSession>> {
        let row =
            sqlx::query_as::<_, SessionRow>(&format!("{SESSION_SELECT} WHERE id = $1"))
                .bind(session_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(Into::into))
    }

    async fn list_sessions(&self, scope_id: Uuid) -> Result<Vec<ChatSession>> {
        let rows = sqlx::query_as::<_, SessionRow>(&format!(
            "{SESSION_SELECT} WHERE scope_id = $1 AND session_type = $2 \
             ORDER BY created_at DESC LIMIT 100"
        ))
        .bind(scope_id)
        .bind(self.session_type)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn find_resumable(
        &self,
        non_resumable: &[&str],
        exclude_created_by: &[&str],
    ) -> Result<Vec<ChatSession>> {
        let states: Vec<String> = non_resumable.iter().map(|s| s.to_string()).collect();
        let creators: Vec<String> = exclude_created_by.iter().map(|s| s.to_string()).collect();
        let rows = sqlx::query_as::<_, SessionRow>(&format!(
            "{SESSION_SELECT} WHERE state != ALL($1) AND created_by != ALL($2) \
             AND session_type = $3 ORDER BY created_at ASC"
        ))
        .bind(&states)
        .bind(&creators)
        .bind(self.session_type)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn update_provider_model(
        &self,
        session_id: Uuid,
        provider: &str,
        model: &str,
    ) -> Result<()> {
        sqlx::query("UPDATE chat_sessions SET provider = $1, model = $2 WHERE id = $3")
            .bind(provider)
            .bind(model)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn has_running_session(&self, subject: &str, inactive_states: &[&str]) -> Result<bool> {
        let states: Vec<String> = inactive_states.iter().map(|s| s.to_string()).collect();
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM chat_sessions \
             WHERE subject = $1 AND state != ALL($2) AND session_type = $3)",
        )
        .bind(subject)
        .bind(&states)
        .bind(self.session_type)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false))
    }

    // -- Messages ---------------------------------------------------------

    async fn append_message(
        &self,
        session_id: Uuid,
        role: &str,
        content: &str,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO chat_messages (session_id, role, content, metadata) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(session_id)
        .bind(role)
        .bind(content)
        .bind(metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_messages(&self, session_id: Uuid) -> Result<Vec<ChatMessage>> {
        let rows = sqlx::query_as::<_, MessageRow>(
            "SELECT id, session_id, role, content, metadata, created_at \
             FROM chat_messages WHERE session_id = $1 ORDER BY created_at ASC",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn get_messages_after(
        &self,
        session_id: Uuid,
        after: DateTime<Utc>,
    ) -> Result<Vec<ChatMessage>> {
        let rows = sqlx::query_as::<_, MessageRow>(
            "SELECT id, session_id, role, content, metadata, created_at \
             FROM chat_messages WHERE session_id = $1 AND created_at > $2 \
             ORDER BY created_at ASC",
        )
        .bind(session_id)
        .bind(after)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    // -- Token usage tracking ------------------------------------------------

    async fn append_token_event(
        &self,
        session_id: Uuid,
        provider: &str,
        model: &str,
        input_tokens: u32,
        output_tokens: u32,
    ) -> Result<u64> {
        let total = (input_tokens + output_tokens) as i64;
        sqlx::query(
            "INSERT INTO chat_token_events (session_id, provider, model, input_tokens, output_tokens) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(session_id)
        .bind(provider)
        .bind(model)
        .bind(input_tokens as i32)
        .bind(output_tokens as i32)
        .execute(&self.pool)
        .await?;

        let new_total: i64 = sqlx::query_scalar(
            "UPDATE chat_sessions SET tokens_used = tokens_used + $1 WHERE id = $2 \
             RETURNING tokens_used",
        )
        .bind(total)
        .bind(session_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(new_total as u64)
    }

    async fn get_token_usage(&self, session_id: Uuid) -> Result<u64> {
        let used: i64 = sqlx::query_scalar("SELECT tokens_used FROM chat_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(used as u64)
    }

    async fn set_token_budget(&self, session_id: Uuid, budget: u64) -> Result<()> {
        sqlx::query("UPDATE chat_sessions SET token_budget = $1 WHERE id = $2")
            .bind(budget as i64)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_token_budget(&self, session_id: Uuid) -> Result<u64> {
        let budget: i64 = sqlx::query_scalar("SELECT token_budget FROM chat_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(budget as u64)
    }
}
