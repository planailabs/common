//! Direct human approval for risky tool calls.
//!
//! Complements the validator-LLM guard: when enabled, a tool call at or above
//! the configured risk threshold suspends mid-flight, broadcasts a
//! [`ChatEvent::ApprovalRequest`], and waits for a human decision delivered
//! via [`ApprovalBroker::resolve`]. A standing "approve all for this session"
//! grant skips further prompts.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::{broadcast, oneshot};
use uuid::Uuid;

use crate::model::ChatEvent;
use crate::validation::ToolRisk;

/// Policy controlling when and how tool calls are gated on human approval.
#[derive(Debug, Clone)]
pub struct ApprovalPolicy {
    /// Tool calls at/above this risk require human approval.
    pub gate_threshold: ToolRisk,
    /// Role of the validator-LLM guard when direct approval is enabled.
    pub guard_role: GuardRole,
    /// How long to wait for a human decision before `on_timeout` applies.
    pub timeout: Duration,
    pub on_timeout: TimeoutAction,
    /// Whether "approve all for session" is offered/accepted.
    pub allow_approve_all: bool,
}

impl Default for ApprovalPolicy {
    fn default() -> Self {
        Self {
            gate_threshold: ToolRisk::Mutating,
            guard_role: GuardRole::Advises,
            timeout: Duration::from_secs(15 * 60),
            on_timeout: TimeoutAction::Deny,
            allow_approve_all: true,
        }
    }
}

/// What the guard verdict means when direct approval is enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardRole {
    /// Guard approval auto-approves the call; only guard-rejected or
    /// unguarded calls escalate to a human.
    Decides,
    /// Guard verdict is shown to the human; the human always decides.
    Advises,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutAction {
    /// Return a tool failure to the agent.
    Deny,
    /// Fire the session pause notify (session parks, request stays pending
    /// in the persisted log; treated as expired on resume).
    PauseSession,
}

/// Human decision for a pending approval.
#[derive(Debug, Clone)]
pub enum ApprovalDecision {
    Approve,
    ApproveAllForSession,
    Deny { reason: Option<String> },
}

impl ApprovalDecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approve => "approved",
            Self::ApproveAllForSession => "approved_all",
            Self::Deny { .. } => "denied",
        }
    }
}

/// A tool call waiting for a human decision.
#[derive(Debug, Clone)]
pub struct PendingApproval {
    pub id: Uuid,
    pub session_id: Uuid,
    pub tool_name: String,
    pub args: String,
    pub reason: String,
    pub risk: ToolRisk,
    /// Guard verdict reasoning, if the validator LLM ran.
    pub guard_reasoning: Option<String>,
    pub requested_at: DateTime<Utc>,
}

/// Outcome of [`ApprovalBroker::request`].
#[derive(Debug)]
pub enum ApprovalOutcome {
    Approved,
    Denied { reason: Option<String> },
    TimedOut,
}

struct PendingEntry {
    info: PendingApproval,
    tx: oneshot::Sender<ApprovalDecision>,
}

/// Per-session approval broker. Lives alongside the session's other control
/// handles; UI/API resolve pending requests through it.
pub struct ApprovalBroker {
    session_id: Uuid,
    approve_all: AtomicBool,
    pending: DashMap<Uuid, PendingEntry>,
    events_tx: broadcast::Sender<ChatEvent>,
    /// Fired for `TimeoutAction::PauseSession`.
    pause_notify: Arc<tokio::sync::Notify>,
}

impl ApprovalBroker {
    pub fn new(
        session_id: Uuid,
        events_tx: broadcast::Sender<ChatEvent>,
        pause_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            session_id,
            approve_all: AtomicBool::new(false),
            pending: DashMap::new(),
            events_tx,
            pause_notify,
        }
    }

    pub fn approve_all(&self) -> bool {
        self.approve_all.load(Ordering::SeqCst)
    }

    pub fn set_approve_all(&self, v: bool) {
        self.approve_all.store(v, Ordering::SeqCst);
    }

    /// Snapshot of currently pending approvals.
    pub fn pending(&self) -> Vec<PendingApproval> {
        self.pending.iter().map(|e| e.info.clone()).collect()
    }

    /// Resolve a pending approval with a human decision.
    pub fn resolve(
        &self,
        approval_id: Uuid,
        decision: ApprovalDecision,
        by: Option<&str>,
    ) -> anyhow::Result<()> {
        let Some((_, entry)) = self.pending.remove(&approval_id) else {
            anyhow::bail!("no pending approval {approval_id}");
        };
        if matches!(decision, ApprovalDecision::ApproveAllForSession) {
            self.set_approve_all(true);
        }
        let _ = self.events_tx.send(ChatEvent::ApprovalResolved {
            approval_id,
            decision: decision.as_str().to_string(),
            by: by.map(String::from),
        });
        // Receiver may have been dropped (session cancelled mid-wait) — fine.
        let _ = entry.tx.send(decision);
        Ok(())
    }

    /// Register a pending approval, broadcast the request event, and wait for
    /// a decision (with timeout). Called from `ValidatedTool::invoke`; the
    /// suspended future lives inside `agent.query()` so session cancel/pause
    /// still preempt it (the guard below cleans up the pending entry).
    pub async fn request(&self, info: PendingApproval, policy: &ApprovalPolicy) -> ApprovalOutcome {
        let approval_id = info.id;
        let (tx, rx) = oneshot::channel();
        let _ = self.events_tx.send(ChatEvent::ApprovalRequest {
            approval_id,
            tool_name: info.tool_name.clone(),
            args: info.args.clone(),
            reason: info.reason.clone(),
            risk: crate::validation::risk_to_str(info.risk).to_string(),
            requested_at: info.requested_at,
        });
        self.pending.insert(approval_id, PendingEntry { info, tx });

        // Ensure the pending entry is GC'd even if this future is dropped
        // (session cancelled/paused while waiting).
        let guard = PendingGuard {
            broker: self,
            approval_id,
        };

        let outcome = match tokio::time::timeout(policy.timeout, rx).await {
            Ok(Ok(ApprovalDecision::Approve | ApprovalDecision::ApproveAllForSession)) => {
                ApprovalOutcome::Approved
            }
            Ok(Ok(ApprovalDecision::Deny { reason })) => ApprovalOutcome::Denied { reason },
            // Sender dropped without a decision (broker cleared) — treat as denied.
            Ok(Err(_)) => ApprovalOutcome::Denied { reason: None },
            Err(_) => {
                let _ = self.events_tx.send(ChatEvent::ApprovalResolved {
                    approval_id,
                    decision: "timeout".to_string(),
                    by: None,
                });
                if policy.on_timeout == TimeoutAction::PauseSession {
                    self.pause_notify.notify_one();
                }
                ApprovalOutcome::TimedOut
            }
        };
        drop(guard);
        outcome
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
}

struct PendingGuard<'a> {
    broker: &'a ApprovalBroker,
    approval_id: Uuid,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.broker.pending.remove(&self.approval_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> Arc<ApprovalBroker> {
        let (tx, _rx) = broadcast::channel(16);
        Arc::new(ApprovalBroker::new(
            Uuid::new_v4(),
            tx,
            Arc::new(tokio::sync::Notify::new()),
        ))
    }

    fn info(broker: &ApprovalBroker) -> PendingApproval {
        PendingApproval {
            id: Uuid::new_v4(),
            session_id: broker.session_id(),
            tool_name: "write_file".into(),
            args: "{}".into(),
            reason: "test".into(),
            risk: ToolRisk::Destructive,
            guard_reasoning: None,
            requested_at: Utc::now(),
        }
    }

    fn policy(timeout: Duration) -> ApprovalPolicy {
        ApprovalPolicy {
            timeout,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn approve_resolves_request() {
        let b = broker();
        let i = info(&b);
        let id = i.id;
        let b2 = b.clone();
        let task = tokio::spawn(async move { b2.request(i, &policy(Duration::from_secs(5))).await });
        // Wait until pending appears, then resolve.
        for _ in 0..100 {
            if !b.pending().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b.resolve(id, ApprovalDecision::Approve, Some("admin")).unwrap();
        assert!(matches!(task.await.unwrap(), ApprovalOutcome::Approved));
        assert!(b.pending().is_empty());
        assert!(!b.approve_all());
    }

    #[tokio::test]
    async fn approve_all_sets_standing_grant() {
        let b = broker();
        let i = info(&b);
        let id = i.id;
        let b2 = b.clone();
        let task = tokio::spawn(async move { b2.request(i, &policy(Duration::from_secs(5))).await });
        for _ in 0..100 {
            if !b.pending().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b.resolve(id, ApprovalDecision::ApproveAllForSession, None)
            .unwrap();
        assert!(matches!(task.await.unwrap(), ApprovalOutcome::Approved));
        assert!(b.approve_all());
    }

    #[tokio::test]
    async fn deny_carries_reason() {
        let b = broker();
        let i = info(&b);
        let id = i.id;
        let b2 = b.clone();
        let task = tokio::spawn(async move { b2.request(i, &policy(Duration::from_secs(5))).await });
        for _ in 0..100 {
            if !b.pending().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b.resolve(
            id,
            ApprovalDecision::Deny {
                reason: Some("nope".into()),
            },
            None,
        )
        .unwrap();
        match task.await.unwrap() {
            ApprovalOutcome::Denied { reason } => assert_eq!(reason.as_deref(), Some("nope")),
            other => panic!("expected denied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_denies() {
        let b = broker();
        let i = info(&b);
        let outcome = b.request(i, &policy(Duration::from_millis(20))).await;
        assert!(matches!(outcome, ApprovalOutcome::TimedOut));
        assert!(b.pending().is_empty());
    }

    #[tokio::test]
    async fn dropped_request_cleans_pending() {
        let b = broker();
        let i = info(&b);
        let b2 = b.clone();
        let task =
            tokio::spawn(async move { b2.request(i, &policy(Duration::from_secs(60))).await });
        for _ in 0..100 {
            if !b.pending().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        task.abort();
        let _ = task.await;
        // Drop guard must have removed the pending entry.
        for _ in 0..100 {
            if b.pending().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(b.pending().is_empty());
        assert!(b.resolve(Uuid::new_v4(), ApprovalDecision::Approve, None).is_err());
    }
}
