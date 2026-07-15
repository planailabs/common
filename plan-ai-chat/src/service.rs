//! Reusable interactive chat service over an api-mcp registry.
//!
//! Extracted from mac-mgmt's fleet chatbot: multi-turn sessions persisted in
//! the unified chat tables, tools bridged from the host's api-mcp registry
//! (dispatched with the chat user's Principal, so the agent can only do what
//! the user can do), risk classing + validator-LLM guard, and per-call human
//! approval for mutating/destructive calls.
//!
//! Hosts provide a [`ChatServiceConfig`], a [`SystemPromptBuilder`] (the
//! domain voice of the agent) and their registry; everything else — session
//! lifecycle, launch/resume, approvals, budgets — is generic.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use uuid::Uuid;

use crate::bridge::api_mcp::{ToolFilter, registry_tools};
use crate::session_loop::{InitialPrompt, SessionHandles, SessionSpec};
use crate::store::pg::PgChatStore;
use crate::tools::{ChatToolContext, NameSessionTool, PinConfig, PinTool, SetPhaseTool};
use crate::{
    ApprovalDecision, ApprovalPolicy, ChatSession, ConnectorConfig, CoreState, DynChatStore,
    GuardRole, SessionManager, StateModel, TimeoutAction, ToolRisk, ValidatedTool,
    ValidationConfig,
};
use plan_ai_api_mcp::{Principal, Registry};

/// Identity of the chat user, resolved fresh from the web session for every
/// spawn/resume (permissions may change between turns).
#[derive(Clone)]
pub struct ChatUserCtx {
    pub email: String,
    pub is_admin: bool,
    pub principal: Principal,
}

/// Service knobs (the host maps its own config section onto this).
#[derive(Debug, Clone)]
pub struct ChatServiceConfig {
    /// Per-session token budget. 0 = unlimited.
    pub token_budget: u64,
    /// Minimum tool risk that requires human approval:
    /// "mutating", "destructive", or "never" (disable the gate).
    pub risk_threshold: String,
    /// Offer/accept "approve all for this session".
    pub allow_approve_all: bool,
    /// Max concurrently running agent sessions per user.
    pub max_active_sessions_per_user: u32,
    /// Minutes an interactive session idles before parking.
    pub idle_park_minutes: u64,
    /// Tool-name globs to include (empty = all registry tools).
    pub tools_include: Vec<String>,
    /// Tool-name globs to exclude.
    pub tools_exclude: Vec<String>,
    /// Validator LLM for the guard layer.
    pub validator_provider: Option<String>,
    pub validator_model: Option<String>,
}

impl Default for ChatServiceConfig {
    fn default() -> Self {
        Self {
            token_budget: 500_000,
            risk_threshold: "mutating".to_string(),
            allow_approve_all: true,
            max_active_sessions_per_user: 3,
            idle_park_minutes: 30,
            tools_include: Vec::new(),
            tools_exclude: Vec::new(),
            validator_provider: None,
            validator_model: None,
        }
    }
}

/// Builds the domain-specific system prompt. See [`tool_index`] for the
/// standard tool-group listing hosts usually embed.
#[async_trait]
pub trait SystemPromptBuilder: Send + Sync + 'static {
    async fn build(&self, user: &ChatUserCtx, resumed: bool, state: &ChatState) -> String;
}

/// One line per api-mcp resource group — full specs are already in the tool
/// list the agent sees.
pub fn tool_index(registry: &Registry<sqlx::PgPool>) -> String {
    let mut by_resource: std::collections::BTreeMap<&str, Vec<String>> = Default::default();
    for ep in registry.endpoints() {
        by_resource.entry(ep.resource).or_default().push(ep.tool_name());
    }
    by_resource
        .iter()
        .map(|(res, tools)| format!("- {res}: {}", tools.join(", ")))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Shared chatbot state. Clone-friendly (inner Arc).
#[derive(Clone)]
pub struct ChatState {
    inner: Arc<ChatStateInner>,
}

struct ChatStateInner {
    manager: SessionManager,
    registry: Arc<Registry<sqlx::PgPool>>,
    pool: sqlx::PgPool,
    connector: ConnectorConfig,
    cfg: ChatServiceConfig,
    prompt: Arc<dyn SystemPromptBuilder>,
}

/// Stable per-user scope id: chat sessions are grouped by owner.
fn scope_for(email: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, email.as_bytes())
}

/// Agent-drivable phases the chat agent tracks via `set_phase`.
pub const CHAT_PHASES: &[&str] = &["planning", "executing", "executed"];

/// Chat session state vocabulary: lifecycle states plus the working phases
/// created → planning → executing → executed. ("running" stays mapped for
/// rows created before phases existed.)
#[derive(Debug, Clone, Copy, Default)]
pub struct ChatStateModel;

impl StateModel for ChatStateModel {
    fn core(&self, state: &str) -> CoreState {
        match state {
            "created" => CoreState::Created,
            "initializing" => CoreState::Initializing,
            "planning" | "executing" | "executed" | "running" => CoreState::Running,
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

    fn agent_allowed(&self, name: &str) -> Option<String> {
        CHAT_PHASES.contains(&name).then(|| name.to_string())
    }

    fn initial_running_state(&self) -> &str {
        "planning"
    }

    fn non_resumable_states(&self) -> &[&str] {
        &["completed", "failed", "cancelled", "needs_attention"]
    }
}

impl ChatState {
    /// Build the session manager over the unified chat tables and park any
    /// sessions interrupted by the previous shutdown. The chat-table
    /// migrations (`crate::store::migrations`) must have run already.
    pub async fn new(
        pool: sqlx::PgPool,
        registry: Arc<Registry<sqlx::PgPool>>,
        connector: ConnectorConfig,
        cfg: ChatServiceConfig,
        prompt: Arc<dyn SystemPromptBuilder>,
    ) -> Result<Self> {
        let store: DynChatStore = Arc::new(PgChatStore::new(pool.clone(), "chat"));
        let manager = SessionManager::new(store, Arc::new(ChatStateModel));

        // Sweep sessions interrupted by a previous shutdown: no auto-respawn,
        // they resume lazily on the next user message.
        let model = ChatStateModel;
        if let Ok(interrupted) = manager
            .store()
            .find_resumable(model.non_resumable_states(), &[])
            .await
        {
            for sess in interrupted {
                // Already paused (e.g. by a previous restart sweep): re-pausing
                // would append a duplicate state_change event on every restart.
                if sess.state == "paused" {
                    continue;
                }
                let data = serde_json::json!({"reason": "server_restart"});
                let _ = manager
                    .store()
                    .transition_state(sess.id, "paused", false, &data)
                    .await;
            }
        }

        Ok(Self {
            inner: Arc::new(ChatStateInner {
                manager,
                registry,
                pool,
                connector,
                cfg,
                prompt,
            }),
        })
    }

    pub fn manager(&self) -> &SessionManager {
        &self.inner.manager
    }

    pub fn store(&self) -> &DynChatStore {
        self.inner.manager.store()
    }

    pub fn registry(&self) -> &Arc<Registry<sqlx::PgPool>> {
        &self.inner.registry
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.inner.pool
    }

    pub fn config(&self) -> &ChatServiceConfig {
        &self.inner.cfg
    }

    /// Sessions owned by this user (newest first).
    pub async fn list_sessions(&self, email: &str) -> Result<Vec<ChatSession>> {
        self.store().list_sessions(scope_for(email)).await
    }

    /// Fetch a session, enforcing ownership (admins may access any).
    pub async fn get_session_checked(
        &self,
        user: &ChatUserCtx,
        session_id: Uuid,
    ) -> Result<ChatSession> {
        let sess = self
            .store()
            .get_session(session_id)
            .await?
            .context("session not found")?;
        if !user.is_admin && sess.subject != user.email {
            anyhow::bail!("access denied");
        }
        Ok(sess)
    }

    /// Start a new chat session with the user's first message.
    pub async fn start_session(
        &self,
        user: &ChatUserCtx,
        provider: Option<String>,
        model: Option<String>,
        first_message: String,
        page_context: Option<String>,
    ) -> Result<Uuid> {
        // Per-user cap on concurrently running agents.
        let running = self
            .list_sessions(&user.email)
            .await?
            .iter()
            .filter(|s| self.inner.manager.is_running(s.id))
            .count() as u32;
        if running >= self.inner.cfg.max_active_sessions_per_user {
            anyhow::bail!("too many active chat sessions ({running}); close or pause one first");
        }

        let state_data = serde_json::json!({
            "owner": user.email,
            "page_context": page_context,
        });
        let session_id = self
            .store()
            .create_session(crate::NewSession {
                scope_id: scope_for(&user.email),
                subject: &user.email,
                created_by: &user.email,
                initial_context: &serde_json::json!({}),
                state_data: &state_data,
                provider: provider.as_deref(),
                model: model.as_deref(),
                label: None,
            })
            .await?;

        if self.inner.cfg.token_budget > 0 {
            self.store()
                .set_token_budget(session_id, self.inner.cfg.token_budget)
                .await
                .ok();
        }

        self.launch(
            user.clone(),
            session_id,
            provider,
            model,
            InitialPrompt::User(prefix_context(page_context.as_deref(), &first_message)),
            false,
            "planning".to_string(),
        )?;
        Ok(session_id)
    }

    /// Deliver a user message: queued into the running session, or the
    /// session is respawned with the message (lazy resume).
    pub async fn send_message(
        &self,
        user: &ChatUserCtx,
        session_id: Uuid,
        text: String,
        page_context: Option<String>,
    ) -> Result<()> {
        let sess = self.get_session_checked(user, session_id).await?;
        let msg = prefix_context(page_context.as_deref(), &text);
        // Fast path: queue into a running agent.
        if self
            .inner
            .manager
            .send_user_message(session_id, msg.clone())
            .is_ok()
        {
            return Ok(());
        }
        // Not running: resumable (paused/awaiting_retry/idle-parked "completed")?
        let model = ChatStateModel;
        let resumable = !model.is_terminal(&sess.state) || sess.state == "completed";
        if !resumable {
            anyhow::bail!("session is in state '{}' and cannot be resumed", sess.state);
        }
        // Budget-exhausted sessions need an explicit extend first.
        if sess.state == "paused"
            && sess.state_data.get("reason").and_then(|v| v.as_str())
                == Some("token_budget_exceeded")
        {
            let budget = self.store().get_token_budget(session_id).await.unwrap_or(0);
            let used = self.store().get_token_usage(session_id).await.unwrap_or(0);
            if budget > 0 && used >= budget {
                anyhow::bail!(
                    "session hit its token budget — extend the budget before continuing"
                );
            }
        }
        // Resume into the phase the session was in, if it was working;
        // parked/paused sessions restart their thinking at "planning".
        let resume_state = if model.is_active(&sess.state) {
            sess.state.clone()
        } else {
            "planning".to_string()
        };
        if self
            .launch(
                user.clone(),
                session_id,
                sess.provider.clone(),
                sess.model.clone(),
                InitialPrompt::User(msg.clone()),
                true,
                resume_state,
            )
            .is_err()
        {
            // Lost a race against a concurrent resume — queue instead.
            return self.inner.manager.send_user_message(session_id, msg);
        }
        Ok(())
    }

    /// Resolve a pending per-call approval.
    pub async fn resolve_approval(
        &self,
        user: &ChatUserCtx,
        session_id: Uuid,
        approval_id: Uuid,
        decision: ApprovalDecision,
    ) -> Result<()> {
        self.get_session_checked(user, session_id).await?;
        // Persist the decision for the audit trail before delivering it.
        let meta = serde_json::json!({
            "approval_id": approval_id,
            "decision": decision.as_str(),
            "by": user.email,
        });
        self.store()
            .append_message(
                session_id,
                "approval_decision",
                &format!("{} by {}", decision.as_str(), user.email),
                Some(&meta),
            )
            .await
            .ok();
        self.inner
            .manager
            .resolve_approval(session_id, approval_id, decision, Some(&user.email))
    }

    /// Session-level standing approval grant ("auto-approve"). Runtime-only:
    /// applies to the currently running agent, like ApproveAllForSession.
    pub async fn set_auto_approve(
        &self,
        user: &ChatUserCtx,
        session_id: Uuid,
        value: bool,
    ) -> Result<()> {
        self.get_session_checked(user, session_id).await?;
        self.inner.manager.set_approve_all(session_id, value)
    }

    /// Current auto-approve state (false when the session is not running).
    pub fn auto_approve(&self, session_id: Uuid) -> bool {
        self.inner.manager.approve_all(session_id).unwrap_or(false)
    }

    pub async fn pause(&self, user: &ChatUserCtx, session_id: Uuid) -> Result<()> {
        self.get_session_checked(user, session_id).await?;
        self.inner.manager.pause_session(session_id)
    }

    pub async fn cancel(&self, user: &ChatUserCtx, session_id: Uuid) -> Result<()> {
        self.get_session_checked(user, session_id).await?;
        self.inner.manager.cancel_session(session_id).await
    }

    /// Extend the session's token budget to 1M tokens.
    pub async fn extend_budget(&self, user: &ChatUserCtx, session_id: Uuid) -> Result<()> {
        self.get_session_checked(user, session_id).await?;
        self.store().set_token_budget(session_id, 1_000_000).await
    }

    pub async fn graceful_shutdown(&self, timeout: std::time::Duration) -> usize {
        self.inner.manager.graceful_shutdown(timeout).await
    }

    /// Build the spec (LLM, tools, prompt) in a spawned task and hand the
    /// session to the generic manager.
    fn launch(
        &self,
        user: ChatUserCtx,
        session_id: Uuid,
        provider: Option<String>,
        model: Option<String>,
        initial: InitialPrompt,
        resumed: bool,
        start_state: String,
    ) -> Result<()> {
        let handles = self.inner.manager.register(session_id)?;
        let state = self.clone();
        tokio::spawn(async move {
            match build_session_spec(
                &state, &user, session_id, provider, model, &handles, initial, resumed,
                start_state,
            )
            .await
            {
                Ok(spec) => state.inner.manager.run_detached(spec),
                Err(e) => {
                    tracing::error!(%session_id, err = %e, "chat session failed to start");
                    state.inner.manager.unregister(session_id);
                    let _ = state
                        .inner
                        .manager
                        .store()
                        .fail_session(session_id, &e.to_string(), &serde_json::json!({}))
                        .await;
                    let _ = handles.events_tx.send(crate::ChatEvent::Done {
                        state: "failed".to_string(),
                    });
                }
            }
        });
        Ok(())
    }
}

/// Prepend the UI's current-page context to a user turn.
fn prefix_context(page_context: Option<&str>, text: &str) -> String {
    match page_context {
        Some(ctx) if !ctx.is_empty() => format!("[context: user is viewing {ctx}]\n\n{text}"),
        _ => text.to_string(),
    }
}

fn approval_policy(cfg: &ChatServiceConfig) -> Option<ApprovalPolicy> {
    let threshold = match cfg.risk_threshold.as_str() {
        "never" => return None,
        "destructive" => ToolRisk::Destructive,
        _ => ToolRisk::Mutating,
    };
    Some(ApprovalPolicy {
        gate_threshold: threshold,
        guard_role: GuardRole::Advises,
        timeout: std::time::Duration::from_secs(15 * 60),
        on_timeout: TimeoutAction::Deny,
        allow_approve_all: cfg.allow_approve_all,
    })
}

#[allow(clippy::too_many_arguments)]
async fn build_session_spec(
    state: &ChatState,
    user: &ChatUserCtx,
    session_id: Uuid,
    provider: Option<String>,
    model: Option<String>,
    handles: &SessionHandles,
    initial: InitialPrompt,
    resumed: bool,
    start_state: String,
) -> Result<SessionSpec> {
    let inner = &state.inner;
    let store = inner.manager.store().clone();

    let mut connector = inner.connector.clone();
    connector.validator_provider = inner.cfg.validator_provider.clone();
    connector.validator_model = inner.cfg.validator_model.clone();

    let token_ctx = crate::TokenEventContext {
        store: store.clone(),
        session_id,
        budget_notify: handles.budget_notify.clone(),
    };
    let llm = crate::connector::resolve_llm(
        &connector,
        provider.as_deref(),
        model.as_deref(),
        Some(token_ctx),
    )
    .await
    .context("failed to resolve LLM")?;

    // Validation config: static checks + optional guard LLM + human approval.
    let validation_history = crate::validation::ToolCallHistory::default();
    let validator_token_ctx = connector
        .validator_provider
        .as_ref()
        .map(|_| crate::TokenEventContext {
            store: store.clone(),
            session_id,
            budget_notify: handles.budget_notify.clone(),
        });
    let validator_llm =
        crate::validation::build_validator_llm(&connector, validator_token_ctx).await;
    let approval = approval_policy(&inner.cfg).map(|policy| crate::ApprovalHook {
        policy,
        broker: handles.approval_broker.clone(),
        store: store.clone(),
        session_id,
    });
    let validation_config = ValidationConfig {
        validator_llm,
        enabled: true,
        running_tools: handles.running_tools.clone(),
        events_tx: handles.events_tx.clone(),
        approval,
    };

    // Tools: every api-mcp registry endpoint the filter allows, dispatched
    // with THIS user's principal — server-side authorization applies per call.
    let filter = ToolFilter {
        include: inner.cfg.tools_include.clone(),
        exclude: inner.cfg.tools_exclude.clone(),
    };
    let principal = Arc::new(user.principal.clone());
    let mut tools = registry_tools(
        inner.registry.clone(),
        inner.pool.clone(),
        principal,
        &filter,
        16 * 1024,
    );

    // Session-local tools: pinning + naming.
    let tool_ctx = ChatToolContext {
        store: store.clone(),
        session_id,
        events_tx: handles.events_tx.clone(),
        approval_notify: handles.approval_notify.clone(),
    };
    tools.push(PinTool::new_with_risk(tool_ctx.clone(), PinConfig::chat()));
    tools.push(NameSessionTool::new_with_risk(tool_ctx.clone()));
    // Phase tracking. Session-local risk: phase changes are bookkeeping and
    // must not trip the guard or the approval gate.
    let (set_phase, _) = SetPhaseTool::new_with_risk(
        tool_ctx,
        Arc::new(ChatStateModel),
        /* auto_approve */ true,
        /* approval_gated_phase */ None,
        CHAT_PHASES,
        "Track your progress. Phases: planning (deciding what to do),          executing (running tools / making changes), executed (the current          request is done). Call this when you move between phases.",
    );
    tools.push((set_phase, ToolRisk::SessionLocal));

    let wrapped = ValidatedTool::wrap_all(tools, validation_config, validation_history.clone());

    let system_prompt = inner.prompt.build(user, resumed, state).await;

    Ok(SessionSpec {
        session_id,
        system_prompt,
        initial_prompt: initial,
        tools: wrapped,
        llm,
        start_state,
        interactive: true,
        idle_timeout: Some(std::time::Duration::from_secs(
            inner.cfg.idle_park_minutes.max(1) * 60,
        )),
        deadline: None,
        deadline_reason: String::new(),
        loop_limit: 50,
        validation_history,
    })
}
