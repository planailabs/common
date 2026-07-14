//! Generic session tools: pinning, phase transitions, session naming.
//!
//! Wire formats (message role `pin`, metadata `pin_<slot>`, state_change
//! contents) are identical to the healer's originals.

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use swiftide::chat_completion::{Tool, ToolCall, ToolOutput, ToolSpec, errors::ToolError};
use swiftide::traits::AgentContext;
use uuid::Uuid;

use crate::model::{ChatEvent, emit_state_change};
use crate::state::StateModel;
use crate::store::DynChatStore;
use crate::validation::ToolRisk;

/// Minimal shared context for the generic session tools.
#[derive(Clone)]
pub struct ChatToolContext {
    pub store: DynChatStore,
    pub session_id: Uuid,
    pub events_tx: tokio::sync::broadcast::Sender<ChatEvent>,
    /// Notify handle to signal AwaitingApproval to the session loop.
    pub approval_notify: Arc<tokio::sync::Notify>,
}

// ── Pin tool ───────────────────────────────────────────────────────────

/// A named pin slot offered to the agent.
#[derive(Debug, Clone)]
pub struct PinSlot {
    pub name: &'static str,
    pub description: &'static str,
}

/// Configuration of the pin tool's slots.
#[derive(Debug, Clone)]
pub struct PinConfig {
    pub slots: Vec<PinSlot>,
    /// Accept slot names outside `slots` (free-form pinning).
    pub allow_arbitrary: bool,
}

impl PinConfig {
    /// The healer's original slots.
    pub fn healer() -> Self {
        Self {
            slots: vec![
                PinSlot {
                    name: "diagnosis",
                    description: "Pin once you identify the root cause. Include affected_services.",
                },
                PinSlot {
                    name: "remediation",
                    description: "Pin your remediation plan before applying fixes.",
                },
                PinSlot {
                    name: "final_report",
                    description: "Pin at the end summarizing what was done, what worked, and any remaining issues.",
                },
            ],
            allow_arbitrary: false,
        }
    }

    /// Generic chat slots.
    pub fn chat() -> Self {
        Self {
            slots: vec![
                PinSlot {
                    name: "notes",
                    description: "Pin durable findings or context worth keeping visible.",
                },
                PinSlot {
                    name: "plan",
                    description: "Pin your working plan before making changes.",
                },
                PinSlot {
                    name: "summary",
                    description: "Pin a summary of what was accomplished.",
                },
            ],
            allow_arbitrary: true,
        }
    }

    fn slot_names(&self) -> Vec<&'static str> {
        self.slots.iter().map(|s| s.name).collect()
    }

    fn description(&self) -> String {
        let mut d = String::from("Pin important information to the session. Available slots:\n");
        for s in &self.slots {
            d.push_str(&format!("- \"{}\": {}\n", s.name, s.description));
        }
        d.push_str("All pins are displayed to the user and persisted across restarts.");
        d
    }
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct PinParams {
    /// Which slot to pin to
    slot: String,
    /// The content to pin
    summary: String,
    /// List of affected services/entities (optional)
    #[serde(default)]
    affected_services: Vec<String>,
}

#[derive(Clone)]
pub struct PinTool {
    ctx: ChatToolContext,
    cfg: PinConfig,
    spec: ToolSpec,
}

impl PinTool {
    pub fn new_with_risk(ctx: ChatToolContext, cfg: PinConfig) -> (Box<dyn Tool>, ToolRisk) {
        let schema = schemars::schema_for!(PinParams);
        let spec = ToolSpec::builder()
            .name("pin")
            .description(&cfg.description())
            .parameters_schema(
                serde_json::from_value::<schemars::Schema>(serde_json::to_value(&schema).unwrap())
                    .unwrap(),
            )
            .build()
            .unwrap();
        (Box::new(Self { ctx, cfg, spec }), ToolRisk::SessionLocal)
    }
}

#[async_trait]
impl Tool for PinTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("pin")
    }

    fn tool_spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    async fn invoke(
        &self,
        _agent_context: &dyn AgentContext,
        tool_call: &ToolCall,
    ) -> Result<ToolOutput, ToolError> {
        let args = tool_call
            .args()
            .ok_or_else(|| ToolError::MissingArguments("no arguments".into()))?;
        let params: PinParams = serde_json::from_str(args)
            .map_err(|e| ToolError::MissingArguments(e.to_string().into()))?;

        let known = self.cfg.slot_names().contains(&params.slot.as_str());
        if !known && !self.cfg.allow_arbitrary {
            return Ok(ToolOutput::Text(format!(
                "Invalid slot '{}'. Valid slots: {}",
                params.slot,
                self.cfg.slot_names().join(", ")
            )));
        }
        let slot = &params.slot;

        let key = format!("pin_{slot}");
        let pin_data = serde_json::json!({
            "summary": params.summary,
            "affected_services": params.affected_services,
        });
        let data = serde_json::json!({ key: pin_data });
        let pin_content = serde_json::to_string(&serde_json::json!({
            "slot": slot,
            "summary": params.summary,
            "affected_services": params.affected_services,
        }))
        .unwrap_or_default();
        self.ctx
            .store
            .append_message(self.ctx.session_id, "pin", &pin_content, Some(&data))
            .await
            .ok();
        // Broadcast so SSE clients update pins live
        let _ = self.ctx.events_tx.send(ChatEvent::Message {
            role: "pin".to_string(),
            content: pin_content,
            metadata: Some(data),
            created_at: chrono::Utc::now(),
        });
        Ok(ToolOutput::Text(format!(
            "Pinned to '{slot}': {}",
            params.summary
        )))
    }
}

// ── set_phase tool ─────────────────────────────────────────────────────

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct SetPhaseParams {
    /// Phase to transition to
    phase: String,
    /// Brief explanation of why you are transitioning to this phase
    reason: String,
}

/// Agent-drivable phase transitions, following a domain [`StateModel`].
///
/// When `auto_approve` is false and the agent requests `approval_gated_phase`,
/// the transition is intercepted: the session moves to the model's approval
/// state and the session loop is signalled via `approval_notify`.
#[derive(Clone)]
pub struct SetPhaseTool {
    ctx: ChatToolContext,
    model: Arc<dyn StateModel>,
    auto_approve: bool,
    /// Phase name that requires approval when `auto_approve` is false
    /// (healer: "remediating"). None = no gated phase.
    approval_gated_phase: Option<String>,
    valid_phases: String,
    spec: ToolSpec,
}

impl SetPhaseTool {
    pub fn new_with_risk(
        ctx: ChatToolContext,
        model: Arc<dyn StateModel>,
        auto_approve: bool,
        approval_gated_phase: Option<String>,
        valid_phases: &[&str],
        description: &str,
    ) -> (Box<dyn Tool>, ToolRisk) {
        let schema = schemars::schema_for!(SetPhaseParams);
        let spec = ToolSpec::builder()
            .name("set_phase")
            .description(description)
            .parameters_schema(
                serde_json::from_value::<schemars::Schema>(serde_json::to_value(&schema).unwrap())
                    .unwrap(),
            )
            .build()
            .unwrap();
        (
            Box::new(Self {
                ctx,
                model,
                auto_approve,
                approval_gated_phase,
                valid_phases: valid_phases.join(", "),
                spec,
            }),
            ToolRisk::Mutating,
        )
    }
}

#[async_trait]
impl Tool for SetPhaseTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("set_phase")
    }

    fn tool_spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    async fn invoke(
        &self,
        _agent_context: &dyn AgentContext,
        tool_call: &ToolCall,
    ) -> Result<ToolOutput, ToolError> {
        let args = tool_call
            .args()
            .ok_or_else(|| ToolError::MissingArguments("no arguments".into()))?;
        let params: SetPhaseParams = serde_json::from_str(args)
            .map_err(|e| ToolError::MissingArguments(e.to_string().into()))?;

        let Some(new_state) = self.model.agent_allowed(&params.phase) else {
            return Ok(ToolOutput::Text(format!(
                "Invalid phase '{}'. Valid: {}",
                params.phase, self.valid_phases
            )));
        };

        // When approval is required, intercept the gated transition.
        if !self.auto_approve && Some(&params.phase) == self.approval_gated_phase.as_ref() {
            let data = serde_json::json!({ "reason": params.reason });
            let approval_state = self.model.approval_state().to_string();
            match self
                .ctx
                .store
                .transition_state(self.ctx.session_id, &approval_state, false, &data)
                .await
            {
                Ok(()) => {
                    emit_state_change(&self.ctx.events_tx, &approval_state, &data);
                    // Signal the session loop to stop the agent immediately.
                    self.ctx.approval_notify.notify_one();
                    return Ok(ToolOutput::Text(
                        "Session paused — awaiting approval before continuing.".to_string(),
                    ));
                }
                Err(e) => {
                    return Ok(ToolOutput::Text(format!("Error requesting approval: {e}")));
                }
            }
        }

        let data = serde_json::json!({ "reason": params.reason });
        let terminal = self.model.is_terminal(&new_state);
        match self
            .ctx
            .store
            .transition_state(self.ctx.session_id, &new_state, terminal, &data)
            .await
        {
            Ok(()) => {
                emit_state_change(&self.ctx.events_tx, &new_state, &data);
                Ok(ToolOutput::Text(format!("Phase set to: {}", params.phase)))
            }
            Err(e) => Ok(ToolOutput::Text(format!("Error setting phase: {e}"))),
        }
    }
}

// ── name_session tool ──────────────────────────────────────────────────

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct NameSessionParams {
    /// Short descriptive name for this session (max ~120 chars)
    name: String,
}

#[derive(Clone)]
pub struct NameSessionTool {
    ctx: ChatToolContext,
}

impl NameSessionTool {
    pub fn new_with_risk(ctx: ChatToolContext) -> (Box<dyn Tool>, ToolRisk) {
        (Box::new(Self { ctx }), ToolRisk::SessionLocal)
    }
}

#[async_trait]
impl Tool for NameSessionTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("name_session")
    }

    fn tool_spec(&self) -> ToolSpec {
        let schema = schemars::schema_for!(NameSessionParams);
        ToolSpec::builder()
            .name("name_session")
            .description(
                "Give this session a short, descriptive name summarizing what it is about. \
                 Call this early — once you understand the topic.",
            )
            .parameters_schema(
                serde_json::from_value::<schemars::Schema>(serde_json::to_value(&schema).unwrap())
                    .unwrap(),
            )
            .build()
            .unwrap()
    }

    async fn invoke(
        &self,
        _agent_context: &dyn AgentContext,
        tool_call: &ToolCall,
    ) -> Result<ToolOutput, ToolError> {
        let args = tool_call
            .args()
            .ok_or_else(|| ToolError::MissingArguments("no arguments".into()))?;
        let params: NameSessionParams = serde_json::from_str(args)
            .map_err(|e| ToolError::MissingArguments(e.to_string().into()))?;
        let label = params.name.chars().take(120).collect::<String>();
        match self.ctx.store.set_label(self.ctx.session_id, &label).await {
            Ok(()) => Ok(ToolOutput::Text(format!("Session named: {label}"))),
            Err(e) => Ok(ToolOutput::Text(format!("Error naming session: {e}"))),
        }
    }
}

// ── RenamedTool (Anthropic-safe MCP tool names) ────────────────────────

/// Wrapper that sanitizes tool names for Anthropic compatibility.
/// Replaces colons with hyphens (e.g. "Context7:query-docs" → "context7-query-docs").
#[derive(Clone)]
pub struct RenamedTool {
    inner: Box<dyn Tool>,
    name: String,
    spec: ToolSpec,
}

impl RenamedTool {
    pub fn wrap(tool: Box<dyn Tool>) -> Box<dyn Tool> {
        let orig_name = tool.name().to_string();
        let sanitized = orig_name.replace(':', "-").replace(' ', "_").to_lowercase();
        let mut spec = tool.tool_spec();
        spec.name = sanitized.clone();
        Box::new(Self {
            inner: tool,
            name: sanitized,
            spec,
        })
    }
}

#[async_trait]
impl Tool for RenamedTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn tool_spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    async fn invoke(
        &self,
        agent_context: &dyn AgentContext,
        tool_call: &ToolCall,
    ) -> Result<ToolOutput, ToolError> {
        self.inner.invoke(agent_context, tool_call).await
    }
}

// ── Context7 MCP connector ─────────────────────────────────────────────

/// Connect to the Context7 documentation MCP server via streamable HTTP.
#[cfg(feature = "mcp")]
pub async fn connect_context7(
    api_key: &str,
) -> anyhow::Result<swiftide::agents::tools::mcp::McpToolbox> {
    use anyhow::Context as _;
    let url = format!("https://mcp.context7.com/mcp?api_key={api_key}");
    let transport =
        rmcp::transport::StreamableHttpClientTransport::<reqwest::Client>::from_uri(url);
    let mut toolbox = swiftide::agents::tools::mcp::McpToolbox::try_from_transport(transport)
        .await
        .context("Context7 MCP handshake failed")?;
    toolbox.with_name("Context7");
    Ok(toolbox)
}
