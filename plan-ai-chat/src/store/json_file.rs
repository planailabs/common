//! JSON file-backed implementation of [`ChatStore`].
//!
//! Each session is a single JSON file containing the session metadata and
//! messages. Zero external dependencies beyond serde_json — the local-mode /
//! embedded counterpart of the Postgres store. Domain crates wrap this store
//! to add their own data: unknown top-level keys in a session file are
//! preserved verbatim in [`SessionFile::extra`] (e.g. the healer's staff
//! pings live under `pings`).
//!
//! Unlike [`super::pg::PgChatStore`] there is no `session_type`
//! discriminator: a file store is scoped by its directory, so each domain
//! opens its own.
//!
//! Layout:
//! ```text
//! {dir}/
//!   {session_uuid}.json   # SessionFile { session, messages, ..extra }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ChatStore, NewSession};
use crate::model::{ChatMessage, ChatSession};

/// On-disk format: one file per session. Domain extensions ride in `extra`
/// (flattened), so a wrapper store can persist extra collections in the same
/// file without this crate knowing their shape.
#[derive(Serialize, Deserialize)]
pub struct SessionFile {
    pub session: ChatSession,
    pub messages: Vec<ChatMessage>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// [`ChatStore`] backed by JSON files in a directory.
pub struct JsonFileChatStore {
    dir: PathBuf,
    /// Serialize writes to avoid torn reads during concurrent appends.
    lock: Mutex<()>,
}

impl JsonFileChatStore {
    /// Open (or create) the store directory.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create chat store dir: {}", dir.display()))?;
        Ok(Self {
            dir,
            lock: Mutex::new(()),
        })
    }

    fn session_path(&self, id: Uuid) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    /// Read one session file (None if it doesn't exist).
    pub fn read_file(&self, id: Uuid) -> Result<Option<SessionFile>> {
        let path = self.session_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let data =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let sf: SessionFile = serde_json::from_slice(&data)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(Some(sf))
    }

    fn write_file(&self, id: Uuid, sf: &SessionFile) -> Result<()> {
        let path = self.session_path(id);
        let data = serde_json::to_vec_pretty(sf)?;
        atomic_write(&path, &data)
    }

    /// Read-modify-write one session file under the store lock.
    pub fn mutate<F>(&self, id: Uuid, f: F) -> Result<()>
    where
        F: FnOnce(&mut SessionFile),
    {
        let _guard = self.lock.lock().unwrap();
        let mut sf = self
            .read_file(id)?
            .ok_or_else(|| anyhow::anyhow!("session {id} not found"))?;
        f(&mut sf);
        self.write_file(id, &sf)
    }

    /// Read all session files in the directory (unparseable files are
    /// skipped with a warning).
    pub fn all_sessions(&self) -> Result<Vec<SessionFile>> {
        let mut out = Vec::new();
        let entries = std::fs::read_dir(&self.dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "json") {
                match std::fs::read(&path) {
                    Ok(data) => {
                        if let Ok(sf) = serde_json::from_slice::<SessionFile>(&data) {
                            out.push(sf);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed to read {}: {e}", path.display());
                    }
                }
            }
        }
        Ok(out)
    }

    fn append_state_change(sf: &mut SessionFile, state: &str, state_data: &serde_json::Value) {
        let reason = state_data
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        sf.messages.push(ChatMessage {
            id: Uuid::new_v4(),
            session_id: sf.session.id,
            role: "state_change".to_string(),
            content: serde_json::json!({ "state": state, "reason": reason }).to_string(),
            metadata: Some(state_data.clone()),
            created_at: Utc::now(),
        });
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, data).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename {} → {}", tmp.display(), path.display()))?;
    Ok(())
}

// ── Trait implementation ───────────────────────────────────────────────

#[async_trait]
impl ChatStore for JsonFileChatStore {
    async fn create_session(&self, new: NewSession<'_>) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let sf = SessionFile {
            session: ChatSession {
                id,
                scope_id: new.scope_id,
                subject: new.subject.to_string(),
                state: "created".to_string(),
                state_data: new.state_data.clone(),
                created_by: new.created_by.to_string(),
                created_at: now,
                updated_at: now,
                completed_at: None,
                error_message: None,
                initial_context: new.initial_context.clone(),
                provider: new.provider.map(String::from),
                model: new.model.map(String::from),
                label: new.label.map(String::from),
            },
            messages: Vec::new(),
            extra: serde_json::Map::new(),
        };
        let _guard = self.lock.lock().unwrap();
        self.write_file(id, &sf)?;
        Ok(id)
    }

    async fn set_label(&self, session_id: Uuid, label: &str) -> Result<()> {
        let label = label.to_string();
        self.mutate(session_id, |sf| {
            sf.session.label = Some(label);
            sf.session.updated_at = Utc::now();
        })
    }

    async fn transition_state(
        &self,
        session_id: Uuid,
        new_state: &str,
        terminal: bool,
        state_data: &serde_json::Value,
    ) -> Result<()> {
        let new_state = new_state.to_string();
        let state_data = state_data.clone();
        self.mutate(session_id, |sf| {
            sf.session.state = new_state.clone();
            sf.session.state_data = state_data.clone();
            sf.session.updated_at = Utc::now();
            // Same semantics as the Postgres store: terminal keeps the first
            // completion time, non-terminal clears it (resume out of
            // "completed" via interactive idle-parking).
            if terminal {
                sf.session.completed_at = sf.session.completed_at.or_else(|| Some(Utc::now()));
            } else {
                sf.session.completed_at = None;
            }
            Self::append_state_change(sf, &new_state, &state_data);
        })
    }

    async fn fail_session(
        &self,
        session_id: Uuid,
        error_message: &str,
        state_data: &serde_json::Value,
    ) -> Result<()> {
        let error_message = error_message.to_string();
        let state_data = state_data.clone();
        self.mutate(session_id, |sf| {
            sf.session.state = "failed".to_string();
            sf.session.state_data = state_data.clone();
            sf.session.error_message = Some(error_message.clone());
            sf.session.updated_at = Utc::now();
            sf.session.completed_at = Some(Utc::now());
            let data = serde_json::json!({"reason": error_message});
            Self::append_state_change(sf, "failed", &data);
        })
    }

    async fn get_session(&self, session_id: Uuid) -> Result<Option<ChatSession>> {
        Ok(self.read_file(session_id)?.map(|sf| sf.session))
    }

    async fn list_sessions(&self, scope_id: Uuid) -> Result<Vec<ChatSession>> {
        let mut sessions: Vec<ChatSession> = self
            .all_sessions()?
            .into_iter()
            .filter(|sf| sf.session.scope_id == scope_id)
            .map(|sf| sf.session)
            .collect();
        sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        sessions.truncate(100);
        Ok(sessions)
    }

    async fn find_resumable(
        &self,
        non_resumable: &[&str],
        exclude_created_by: &[&str],
    ) -> Result<Vec<ChatSession>> {
        let mut sessions: Vec<ChatSession> = self
            .all_sessions()?
            .into_iter()
            .filter(|sf| {
                !exclude_created_by.contains(&sf.session.created_by.as_str())
                    && !non_resumable.contains(&sf.session.state.as_str())
            })
            .map(|sf| sf.session)
            .collect();
        sessions.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(sessions)
    }

    async fn update_provider_model(
        &self,
        session_id: Uuid,
        provider: &str,
        model: &str,
    ) -> Result<()> {
        let provider = provider.to_string();
        let model = model.to_string();
        self.mutate(session_id, |sf| {
            sf.session.provider = Some(provider);
            sf.session.model = Some(model);
            sf.session.updated_at = Utc::now();
        })
    }

    async fn has_running_session(&self, subject: &str, inactive_states: &[&str]) -> Result<bool> {
        Ok(self.all_sessions()?.iter().any(|sf| {
            sf.session.subject == subject && !inactive_states.contains(&sf.session.state.as_str())
        }))
    }

    // -- Messages ---------------------------------------------------------

    async fn append_message(
        &self,
        session_id: Uuid,
        role: &str,
        content: &str,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        let msg = ChatMessage {
            id: Uuid::new_v4(),
            session_id,
            role: role.to_string(),
            content: content.to_string(),
            metadata: metadata.cloned(),
            created_at: Utc::now(),
        };
        self.mutate(session_id, |sf| {
            sf.messages.push(msg);
        })
    }

    async fn get_messages(&self, session_id: Uuid) -> Result<Vec<ChatMessage>> {
        Ok(self
            .read_file(session_id)?
            .map(|sf| sf.messages)
            .unwrap_or_default())
    }

    async fn get_messages_after(
        &self,
        session_id: Uuid,
        after: DateTime<Utc>,
    ) -> Result<Vec<ChatMessage>> {
        Ok(self
            .read_file(session_id)?
            .map(|sf| {
                sf.messages
                    .into_iter()
                    .filter(|m| m.created_at > after)
                    .collect()
            })
            .unwrap_or_default())
    }

    // -- Token usage tracking ----------------------------------------------
    // Stored in the session's state_data blob (no separate event log in
    // local mode).

    async fn append_token_event(
        &self,
        session_id: Uuid,
        _provider: &str,
        _model: &str,
        input_tokens: u32,
        output_tokens: u32,
    ) -> Result<u64> {
        let total = (input_tokens + output_tokens) as u64;
        self.mutate(session_id, |sf| {
            let used = sf
                .session
                .state_data
                .get("tokens_used")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + total;
            sf.session.state_data["tokens_used"] = serde_json::json!(used);
        })?;
        self.get_token_usage(session_id).await
    }

    async fn get_token_usage(&self, session_id: Uuid) -> Result<u64> {
        Ok(self
            .read_file(session_id)?
            .and_then(|sf| sf.session.state_data.get("tokens_used")?.as_u64())
            .unwrap_or(0))
    }

    async fn set_token_budget(&self, session_id: Uuid, budget: u64) -> Result<()> {
        self.mutate(session_id, |sf| {
            sf.session.state_data["token_budget"] = serde_json::json!(budget);
        })
    }

    async fn get_token_budget(&self, session_id: Uuid) -> Result<u64> {
        Ok(self
            .read_file(session_id)?
            .and_then(|sf| sf.session.state_data.get("token_budget")?.as_u64())
            .unwrap_or(0))
    }
}
