//! Generic session lifecycle classification over domain-specific state strings.
//!
//! Sessions persist their state as a raw string (DB rows stay untouched when a
//! domain adopts this crate). A [`StateModel`] maps those strings onto the
//! generic [`CoreState`] lifecycle so the session loop and stores can reason
//! about terminal/active/resumable without knowing the domain vocabulary.

/// Generic lifecycle classification every domain state maps onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreState {
    Created,
    Initializing,
    /// Agent task is (or should be) actively running. Domains may have several
    /// running phases (healer: diagnosing/remediating/verifying).
    Running,
    AwaitingApproval,
    /// Interrupted, will be auto-resumed (server restart, proxy expiry).
    AwaitingRetry,
    Paused,
    Completed,
    Failed,
    Cancelled,
    /// Escalated to a human; terminal.
    NeedsAttention,
}

impl CoreState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::NeedsAttention
        )
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Initializing | Self::Running)
    }
}

/// Domain state vocabulary. Implemented once per domain (healer, chatbot, ...).
pub trait StateModel: Send + Sync + 'static {
    /// Classify a raw state string. Unknown strings should map to a sensible
    /// default (typically `Failed`).
    fn core(&self, state: &str) -> CoreState;

    fn is_terminal(&self, state: &str) -> bool {
        self.core(state).is_terminal()
    }

    fn is_active(&self, state: &str) -> bool {
        self.core(state).is_active()
    }

    /// States the agent may transition to via the `set_phase` tool
    /// (phase name → canonical state string). None = not allowed.
    fn agent_allowed(&self, name: &str) -> Option<String>;

    /// State entered when the agent loop (re)starts running.
    fn initial_running_state(&self) -> &str;

    /// State strings that make a session NON-resumable, exactly as used in
    /// store queries. Kept as an explicit list (not derived from `core`) so
    /// existing SQL semantics can be replicated verbatim.
    fn non_resumable_states(&self) -> &[&str];

    /// State entered when the domain gates a phase behind human approval.
    fn approval_state(&self) -> &str {
        "awaiting_approval"
    }
}

/// Default state model for plain chat sessions (no domain phases).
///
/// Vocabulary: created, initializing, running, awaiting_approval,
/// awaiting_retry, paused, completed, failed, cancelled, needs_attention.
#[derive(Debug, Clone, Copy, Default)]
pub struct CoreStateModel;

impl StateModel for CoreStateModel {
    fn core(&self, state: &str) -> CoreState {
        match state {
            "created" => CoreState::Created,
            "initializing" => CoreState::Initializing,
            "running" => CoreState::Running,
            "awaiting_approval" => CoreState::AwaitingApproval,
            "awaiting_retry" => CoreState::AwaitingRetry,
            "paused" => CoreState::Paused,
            "completed" => CoreState::Completed,
            "failed" => CoreState::Failed,
            "cancelled" => CoreState::Cancelled,
            "needs_attention" => CoreState::NeedsAttention,
            _ => CoreState::Failed,
        }
    }

    fn agent_allowed(&self, _name: &str) -> Option<String> {
        // Plain chat sessions have no agent-drivable phases.
        None
    }

    fn initial_running_state(&self) -> &str {
        "running"
    }

    fn non_resumable_states(&self) -> &[&str] {
        &[
            "completed",
            "failed",
            "cancelled",
            "paused",
            "needs_attention",
        ]
    }
}
