//! Postgres-backed implementation of [`ChatStore`] with configurable table
//! and column names, so existing domain tables (e.g. the healer's
//! `healer_sessions`/`healer_messages`/`healer_token_events`) are reused
//! without migration.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{ChatStore, NewSession};
use crate::model::{ChatMessage, ChatSession};

/// Table/column names for a chat store. All values are static configuration
/// baked in at construction — never user input (SQL is built via `format!`).
#[derive(Debug, Clone)]
pub struct PgTables {
    pub sessions: &'static str,
    pub messages: &'static str,
    pub token_events: &'static str,
    /// Column on the sessions table holding [`ChatSession::scope_id`].
    pub scope_col: &'static str,
    /// Column on the sessions table holding [`ChatSession::subject`].
    pub subject_col: &'static str,
    /// Column on the sessions table holding [`ChatSession::initial_context`].
    pub initial_context_col: &'static str,
}

impl PgTables {
    /// Generic chat tables (used by new consumers; see the chatbot migration).
    pub fn chat() -> Self {
        Self {
            sessions: "chat_sessions",
            messages: "chat_messages",
            token_events: "chat_token_events",
            scope_col: "scope_id",
            subject_col: "subject",
            initial_context_col: "initial_context",
        }
    }

    /// The healer's original tables (zero-migration adoption).
    pub fn healer() -> Self {
        Self {
            sessions: "healer_sessions",
            messages: "healer_messages",
            token_events: "healer_token_events",
            scope_col: "cluster_id",
            subject_col: "instance_id",
            initial_context_col: "initial_issues",
        }
    }
}

/// [`ChatStore`] backed by a Postgres connection pool.
#[derive(Clone)]
pub struct PgChatStore {
    pool: PgPool,
    t: PgTables,
}

impl PgChatStore {
    pub fn new(pool: PgPool, tables: PgTables) -> Self {
        Self { pool, t: tables }
    }

    /// Return the underlying pool (escape hatch for callers that still need it).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn tables(&self) -> &PgTables {
        &self.t
    }

    fn session_select(&self) -> String {
        format!(
            "SELECT id, {scope} AS scope_id, {subject} AS subject, state, state_data, created_by, \
                    created_at, updated_at, completed_at, error_message, \
                    {ctx} AS initial_context, provider, model, label \
             FROM {sessions}",
            scope = self.t.scope_col,
            subject = self.t.subject_col,
            ctx = self.t.initial_context_col,
            sessions = self.t.sessions,
        )
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
        let _ = sqlx::query(&format!(
            "INSERT INTO {} (session_id, role, content, metadata) VALUES ($1, $2, $3, $4)",
            self.t.messages
        ))
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
        let id = sqlx::query_scalar::<_, Uuid>(&format!(
            "INSERT INTO {sessions} ({scope}, {subject}, state, state_data, created_by, {ctx}, provider, model, label) \
             VALUES ($1, $2, 'created', $3, $4, $5, $6, $7, $8) \
             RETURNING id",
            sessions = self.t.sessions,
            scope = self.t.scope_col,
            subject = self.t.subject_col,
            ctx = self.t.initial_context_col,
        ))
        .bind(new.scope_id)
        .bind(new.subject)
        .bind(new.state_data)
        .bind(new.created_by)
        .bind(new.initial_context)
        .bind(new.provider)
        .bind(new.model)
        .bind(new.label)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    async fn set_label(&self, session_id: Uuid, label: &str) -> Result<()> {
        sqlx::query(&format!(
            "UPDATE {} SET label = $1 WHERE id = $2",
            self.t.sessions
        ))
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
            sqlx::query(&format!(
                "UPDATE {} \
                 SET state = $1, state_data = $2, updated_at = now(), \
                     completed_at = COALESCE(completed_at, now()) \
                 WHERE id = $3",
                self.t.sessions
            ))
            .bind(new_state)
            .bind(state_data)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(&format!(
                "UPDATE {} \
                 SET state = $1, state_data = $2, updated_at = now(), completed_at = NULL \
                 WHERE id = $3",
                self.t.sessions
            ))
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
        sqlx::query(&format!(
            "UPDATE {} \
             SET state = 'failed', state_data = $1, error_message = $2, \
                 updated_at = now(), completed_at = now() \
             WHERE id = $3",
            self.t.sessions
        ))
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
            sqlx::query_as::<_, SessionRow>(&format!("{} WHERE id = $1", self.session_select()))
                .bind(session_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(Into::into))
    }

    async fn list_sessions(&self, scope_id: Uuid) -> Result<Vec<ChatSession>> {
        let rows = sqlx::query_as::<_, SessionRow>(&format!(
            "{} WHERE {} = $1 ORDER BY created_at DESC LIMIT 100",
            self.session_select(),
            self.t.scope_col
        ))
        .bind(scope_id)
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
            "{} WHERE state != ALL($1) AND created_by != ALL($2) ORDER BY created_at ASC",
            self.session_select()
        ))
        .bind(&states)
        .bind(&creators)
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
        sqlx::query(&format!(
            "UPDATE {} SET provider = $1, model = $2 WHERE id = $3",
            self.t.sessions
        ))
        .bind(provider)
        .bind(model)
        .bind(session_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn has_running_session(&self, subject: &str, inactive_states: &[&str]) -> Result<bool> {
        let states: Vec<String> = inactive_states.iter().map(|s| s.to_string()).collect();
        Ok(sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE {} = $1 AND state != ALL($2))",
            self.t.sessions, self.t.subject_col
        ))
        .bind(subject)
        .bind(&states)
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
        sqlx::query(&format!(
            "INSERT INTO {} (session_id, role, content, metadata) VALUES ($1, $2, $3, $4)",
            self.t.messages
        ))
        .bind(session_id)
        .bind(role)
        .bind(content)
        .bind(metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_messages(&self, session_id: Uuid) -> Result<Vec<ChatMessage>> {
        let rows = sqlx::query_as::<_, MessageRow>(&format!(
            "SELECT id, session_id, role, content, metadata, created_at \
             FROM {} WHERE session_id = $1 ORDER BY created_at ASC",
            self.t.messages
        ))
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
        let rows = sqlx::query_as::<_, MessageRow>(&format!(
            "SELECT id, session_id, role, content, metadata, created_at \
             FROM {} WHERE session_id = $1 AND created_at > $2 ORDER BY created_at ASC",
            self.t.messages
        ))
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
        sqlx::query(&format!(
            "INSERT INTO {} (session_id, provider, model, input_tokens, output_tokens) \
             VALUES ($1, $2, $3, $4, $5)",
            self.t.token_events
        ))
        .bind(session_id)
        .bind(provider)
        .bind(model)
        .bind(input_tokens as i32)
        .bind(output_tokens as i32)
        .execute(&self.pool)
        .await?;

        let new_total: i64 = sqlx::query_scalar(&format!(
            "UPDATE {} SET tokens_used = tokens_used + $1 WHERE id = $2 RETURNING tokens_used",
            self.t.sessions
        ))
        .bind(total)
        .bind(session_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(new_total as u64)
    }

    async fn get_token_usage(&self, session_id: Uuid) -> Result<u64> {
        let used: i64 = sqlx::query_scalar(&format!(
            "SELECT tokens_used FROM {} WHERE id = $1",
            self.t.sessions
        ))
        .bind(session_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(used as u64)
    }

    async fn set_token_budget(&self, session_id: Uuid, budget: u64) -> Result<()> {
        sqlx::query(&format!(
            "UPDATE {} SET token_budget = $1 WHERE id = $2",
            self.t.sessions
        ))
        .bind(budget as i64)
        .bind(session_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_token_budget(&self, session_id: Uuid) -> Result<u64> {
        let budget: i64 = sqlx::query_scalar(&format!(
            "SELECT token_budget FROM {} WHERE id = $1",
            self.t.sessions
        ))
        .bind(session_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(budget as u64)
    }
}
