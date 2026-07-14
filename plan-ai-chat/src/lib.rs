//! plan-ai-chat — generic LLM agent-chat infrastructure.
//!
//! Extracted from the mac-mgmt healer: persisted sessions/messages with
//! pinning, a generic state machine over domain state strings, SSE-ready
//! event broadcasting, a swiftide agent loop with single-shot and interactive
//! multi-turn modes, tiered tool-call validation (static checks → validator
//! LLM guard → direct human approval), LLM provider resolution, and token
//! spend tracking with budgets.
//!
//! Domain crates (healer-like or chat-like systems) provide: a store (or
//! [`store::pg::PgChatStore`] with their table names), a [`StateModel`], the
//! tool set, and the system prompt — then drive sessions through
//! [`SessionManager`].

pub mod connector;
pub mod model;
pub mod session_loop;
pub mod spend;
pub mod state;
pub mod store;
pub mod tools;
pub mod validation;

#[cfg(feature = "api-mcp")]
pub mod bridge;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use connector::{ConnectorConfig, LlmHandle, LlmProvider, OpenAiSource, TokenEventContext};
pub use model::{
    ChatEvent, ChatMessage, ChatSession, RunningTool, RunningToolValidation, emit_state_change,
};
pub use session_loop::{InitialPrompt, SessionHandles, SessionSpec};
pub use state::{CoreState, CoreStateModel, StateModel};
pub use store::{ChatStore, DynChatStore, NewSession};
pub use validation::approval::{
    ApprovalBroker, ApprovalDecision, ApprovalPolicy, GuardRole, PendingApproval, TimeoutAction,
};
pub use validation::{ApprovalHook, ToolRisk, ValidatedTool, ValidationConfig};

/// How many events the per-session broadcast channel buffers.
const EVENTS_CHANNEL_CAPACITY: usize = 4096;
/// How many queued user messages a session accepts while busy.
const USER_MSG_CHANNEL_CAPACITY: usize = 64;

/// Manages running agent sessions: control handles, the agent tasks, and
/// their lifecycle. Clone-friendly (inner Arc).
#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    store: DynChatStore,
    state_model: Arc<dyn StateModel>,
    running: DashMap<Uuid, RunningEntry>,
    shutting_down: AtomicBool,
}

struct RunningEntry {
    handles: SessionHandles,
    user_tx: mpsc::Sender<String>,
    /// Taken by `run_detached` when the agent task starts.
    user_rx: std::sync::Mutex<Option<mpsc::Receiver<String>>>,
}

impl SessionManager {
    pub fn new(store: DynChatStore, state_model: Arc<dyn StateModel>) -> Self {
        Self {
            inner: Arc::new(ManagerInner {
                store,
                state_model,
                running: DashMap::new(),
                shutting_down: AtomicBool::new(false),
            }),
        }
    }

    pub fn store(&self) -> &DynChatStore {
        &self.inner.store
    }

    pub fn state_model(&self) -> &Arc<dyn StateModel> {
        &self.inner.state_model
    }

    /// Register a session's control handles before building its
    /// [`SessionSpec`] (tools need the event/notify handles). Must be
    /// followed by [`Self::run_detached`] or [`Self::unregister`].
    pub fn register(&self, session_id: Uuid) -> SessionHandles {
        let cancel = CancellationToken::new();
        let pause_notify = Arc::new(tokio::sync::Notify::new());
        let approval_notify = Arc::new(tokio::sync::Notify::new());
        let budget_notify = Arc::new(tokio::sync::Notify::new());
        let (events_tx, _) = broadcast::channel::<ChatEvent>(EVENTS_CHANNEL_CAPACITY);
        let running_tools = Arc::new(std::sync::Mutex::new(Vec::new()));
        let approval_broker = Arc::new(ApprovalBroker::new(
            session_id,
            events_tx.clone(),
            pause_notify.clone(),
        ));
        let (user_tx, user_rx) = mpsc::channel(USER_MSG_CHANNEL_CAPACITY);

        let handles = SessionHandles {
            cancel,
            pause_notify,
            approval_notify,
            budget_notify,
            events_tx,
            running_tools,
            approval_broker,
        };
        self.inner.running.insert(
            session_id,
            RunningEntry {
                handles: handles.clone(),
                user_tx,
                user_rx: std::sync::Mutex::new(Some(user_rx)),
            },
        );
        handles
    }

    /// Remove a registered session that never started (spec build failed).
    pub fn unregister(&self, session_id: Uuid) {
        self.inner.running.remove(&session_id);
    }

    /// Spawn the agent task for a registered session. Cleans up the running
    /// entry, fails the session on error, and broadcasts the final `Done`
    /// event when the task ends.
    pub fn run_detached(&self, spec: SessionSpec) {
        let session_id = spec.session_id;
        let Some(entry) = self.inner.running.get(&session_id) else {
            tracing::error!(%session_id, "run_detached called without register");
            return;
        };
        let handles = entry.handles.clone();
        let user_rx = entry
            .user_rx
            .lock()
            .unwrap()
            .take()
            .expect("run_detached called twice for the same session");
        drop(entry);

        let manager = self.clone();
        let events_tx = handles.events_tx.clone();
        tokio::spawn(async move {
            let shutdown = shutdown_signal(manager.clone());
            let result = session_loop::run_session(
                manager.inner.store.clone(),
                manager.inner.state_model.clone(),
                shutdown,
                spec,
                handles,
                user_rx,
            )
            .await;

            // Clean up
            manager.inner.running.remove(&session_id);

            if let Err(e) = &result {
                tracing::error!(%session_id, err = %e, "agent session failed");
                let _ = manager
                    .inner
                    .store
                    .fail_session(session_id, &e.to_string(), &json!({}))
                    .await;
            }

            // Broadcast done event
            let final_state = manager
                .inner
                .store
                .get_session(session_id)
                .await
                .ok()
                .flatten()
                .map(|s| s.state)
                .unwrap_or_else(|| "unknown".to_string());
            let _ = events_tx.send(ChatEvent::Done { state: final_state });
        });
    }

    /// Whether a session currently has a running agent task.
    pub fn is_running(&self, session_id: Uuid) -> bool {
        self.inner.running.contains_key(&session_id)
    }

    /// Deliver a user message to a running session. Queued if the agent is
    /// mid-turn; delivered at the next idle point.
    pub fn send_user_message(&self, session_id: Uuid, text: String) -> Result<()> {
        let entry = self
            .inner
            .running
            .get(&session_id)
            .ok_or_else(|| anyhow::anyhow!("session is not running"))?;
        entry
            .user_tx
            .try_send(text)
            .map_err(|e| anyhow::anyhow!("failed to queue user message: {e}"))
    }

    /// Request a running session to pause immediately.
    /// The agent future is dropped, then the session transitions to Paused.
    pub fn pause_session(&self, session_id: Uuid) -> Result<()> {
        if let Some(entry) = self.inner.running.get(&session_id) {
            entry.handles.pause_notify.notify_one();
            Ok(())
        } else {
            Err(anyhow::anyhow!("session is not running"))
        }
    }

    /// Cancel a running session. If the session is not running, marks it
    /// cancelled in the store directly.
    pub async fn cancel_session(&self, session_id: Uuid) -> Result<()> {
        if let Some(entry) = self.inner.running.get(&session_id) {
            entry.handles.cancel.cancel();
            Ok(())
        } else {
            self.inner
                .store
                .transition_state(session_id, "cancelled", true, &json!({}))
                .await
        }
    }

    /// Subscribe to live events for a running session.
    pub fn subscribe(&self, session_id: Uuid) -> Option<broadcast::Receiver<ChatEvent>> {
        self.inner
            .running
            .get(&session_id)
            .map(|entry| entry.handles.events_tx.subscribe())
    }

    /// Get the current running tools snapshot for a session.
    pub fn running_tools(&self, session_id: Uuid) -> Vec<RunningTool> {
        self.inner
            .running
            .get(&session_id)
            .map(|entry| entry.handles.running_tools.lock().unwrap().clone())
            .unwrap_or_default()
    }

    // -- Direct approvals ---------------------------------------------------

    /// Pending human approvals for a running session.
    pub fn pending_approvals(&self, session_id: Uuid) -> Vec<PendingApproval> {
        self.inner
            .running
            .get(&session_id)
            .map(|entry| entry.handles.approval_broker.pending())
            .unwrap_or_default()
    }

    /// Resolve a pending approval with a human decision.
    pub fn resolve_approval(
        &self,
        session_id: Uuid,
        approval_id: Uuid,
        decision: ApprovalDecision,
        by: Option<&str>,
    ) -> Result<()> {
        let entry = self
            .inner
            .running
            .get(&session_id)
            .ok_or_else(|| anyhow::anyhow!("session is not running"))?;
        entry
            .handles
            .approval_broker
            .resolve(approval_id, decision, by)
    }

    /// Whether the standing "approve all for this session" grant is active.
    /// None if the session is not running.
    pub fn approve_all(&self, session_id: Uuid) -> Option<bool> {
        self.inner
            .running
            .get(&session_id)
            .map(|entry| entry.handles.approval_broker.approve_all())
    }

    /// Set (or clear) the standing "approve all for this session" grant.
    pub fn set_approve_all(&self, session_id: Uuid, value: bool) -> Result<()> {
        let entry = self
            .inner
            .running
            .get(&session_id)
            .ok_or_else(|| anyhow::anyhow!("session is not running"))?;
        entry.handles.approval_broker.set_approve_all(value);
        Ok(())
    }

    // -- Shutdown -------------------------------------------------------------

    /// Graceful shutdown: signal all sessions to stop at the next safe point,
    /// wait for them to checkpoint, with a timeout. Returns the number of
    /// sessions that were running.
    pub async fn graceful_shutdown(&self, timeout: std::time::Duration) -> usize {
        self.inner.shutting_down.store(true, Ordering::SeqCst);

        let deadline = tokio::time::Instant::now() + timeout;
        let initial_count = self.inner.running.len();

        // Wait for all sessions to drain
        loop {
            if self.inner.running.is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    remaining = self.inner.running.len(),
                    "shutdown timed out, some sessions may not be cleanly checkpointed"
                );
                // Force-cancel remaining sessions
                for entry in self.inner.running.iter() {
                    entry.handles.cancel.cancel();
                }
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        initial_count
    }

    /// Check if the system is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::SeqCst)
    }
}

/// Resolves when the manager signals a shutdown.
async fn shutdown_signal(manager: SessionManager) {
    loop {
        if manager.is_shutting_down() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
