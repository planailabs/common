//! The agent session loop: builds a swiftide agent with persistence hooks and
//! races it against control signals. Supports single-shot (healer-style) and
//! interactive multi-turn (chatbot-style) sessions.

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use swiftide::chat_completion::Tool;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::connector::{LlmHandle, LlmProvider};
use crate::model::{ChatEvent, RunningTool, emit_state_change};
use crate::store::DynChatStore;
use crate::validation::ToolCallHistory;

/// Initial prompt for a session run.
pub enum InitialPrompt {
    /// Fresh session: persisted as a `user` message and broadcast before the
    /// agent starts.
    User(String),
    /// Resumed session: sent to the agent without persisting.
    Resume(String),
}

/// Everything needed to run one agent session. The domain builds this after
/// registering the session with the [`crate::SessionManager`] (tools need the
/// session's event/notify handles).
pub struct SessionSpec {
    pub session_id: Uuid,
    pub system_prompt: String,
    pub initial_prompt: InitialPrompt,
    /// Fully wrapped tools (e.g. via `ValidatedTool::wrap_all`).
    pub tools: Vec<Box<dyn Tool>>,
    pub llm: LlmHandle,
    /// State to transition to when the loop starts (e.g. "diagnosing", "running").
    pub start_state: String,
    /// Multi-turn: after the agent completes a turn, wait for the next user
    /// message instead of finishing the session.
    pub interactive: bool,
    /// Interactive only: how long to idle-wait for the next user message
    /// before completing the session. None = wait indefinitely.
    pub idle_timeout: Option<std::time::Duration>,
    /// Hard wall-clock deadline (e.g. proxy token expiry minus lead time).
    /// The session parks in `awaiting_retry` with `deadline_reason`.
    pub deadline: Option<DateTime<Utc>>,
    /// Reason recorded when the deadline fires (healer: "proxy_token_expiring").
    pub deadline_reason: String,
    /// Max agent loop iterations per query (healer: 50).
    pub loop_limit: usize,
    /// Shared history the ValidatedTool wrappers record into; the after_tool
    /// hook reads it to attach validation metadata to tool results.
    pub validation_history: ToolCallHistory,
}

/// Shared control handles for a running session (created by
/// `SessionManager::register`).
#[derive(Clone)]
pub struct SessionHandles {
    pub cancel: CancellationToken,
    pub pause_notify: Arc<tokio::sync::Notify>,
    pub approval_notify: Arc<tokio::sync::Notify>,
    pub budget_notify: Arc<tokio::sync::Notify>,
    pub events_tx: broadcast::Sender<ChatEvent>,
    pub running_tools: Arc<std::sync::Mutex<Vec<RunningTool>>>,
    pub approval_broker: Arc<crate::validation::approval::ApprovalBroker>,
}

pub(crate) enum StopReason {
    Completed,
    AgentError(anyhow::Error),
    Cancelled,
    Paused,
    AwaitingApproval,
    Shutdown,
    DeadlineExpired,
    BudgetExceeded { used: u64, limit: u64 },
}

/// Extract role/content from ChatMessage. Returns None for ToolOutput
/// (handled by before_tool/after_tool hooks to avoid duplicates).
fn extract_role_content(msg: &swiftide::chat_completion::ChatMessage) -> Option<(String, String)> {
    match msg {
        swiftide::chat_completion::ChatMessage::System(s) => Some(("system".to_string(), s.clone())),
        swiftide::chat_completion::ChatMessage::User(s) => Some(("user".to_string(), s.clone())),
        swiftide::chat_completion::ChatMessage::Assistant(s, tool_calls) => {
            let mut content = s.clone().unwrap_or_default();
            // Include tool call names in the assistant message for visibility
            if let Some(calls) = tool_calls {
                if !calls.is_empty() && content.is_empty() {
                    content = calls
                        .iter()
                        .map(|tc| format!("`{}`", tc.name()))
                        .collect::<Vec<_>>()
                        .join(", ");
                }
            }
            Some(("assistant".to_string(), content))
        }
        // Skip ToolOutput — persisted by after_tool hook
        swiftide::chat_completion::ChatMessage::ToolOutput(_, _) => None,
        swiftide::chat_completion::ChatMessage::Summary(s) => {
            Some(("summary".to_string(), s.clone()))
        }
        swiftide::chat_completion::ChatMessage::UserWithParts(parts) => {
            let content = parts
                .iter()
                .filter_map(|p| {
                    if let swiftide::chat_completion::ChatMessageContentPart::Text { text } = p {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            Some(("user".to_string(), content))
        }
        swiftide::chat_completion::ChatMessage::Reasoning(_) => None,
    }
}

/// Wait until the deadline (minus nothing — caller applies any lead time).
/// Pends forever when no deadline is set.
async fn deadline_signal(deadline: Option<DateTime<Utc>>) {
    let Some(deadline) = deadline else {
        return std::future::pending().await;
    };
    let now = Utc::now();
    if now >= deadline {
        return;
    }
    let dur = (deadline - now).to_std().unwrap_or_default();
    tokio::time::sleep(dur).await;
}

/// Run the agent session. Drives the LLM agent until a terminal condition;
/// persists all state transitions. Called by `SessionManager::run_detached`.
pub(crate) async fn run_session(
    store: DynChatStore,
    state_model: Arc<dyn crate::state::StateModel>,
    shutdown: impl Future<Output = ()>,
    spec: SessionSpec,
    handles: SessionHandles,
    mut user_rx: mpsc::Receiver<String>,
) -> Result<()> {
    let session_id = spec.session_id;
    let events_tx = handles.events_tx.clone();
    let running_tools = handles.running_tools.clone();

    // Persist the resolved provider/model so we can resume with the same LLM
    // later and display it in the UI.
    store
        .update_provider_model(
            session_id,
            &spec.llm.resolved_provider,
            &spec.llm.resolved_model,
        )
        .await
        .ok();

    // 1. Transition to the start state.
    {
        let data = json!({});
        let terminal = state_model.is_terminal(&spec.start_state);
        store
            .transition_state(session_id, &spec.start_state, terminal, &data)
            .await
            .ok();
        emit_state_change(&events_tx, &spec.start_state, &data);
    }

    // 2. Determine + persist the initial prompt.
    let initial_prompt = match spec.initial_prompt {
        InitialPrompt::Resume(msg) => msg,
        InitialPrompt::User(msg) => {
            store
                .append_message(session_id, "user", &msg, None)
                .await
                .ok();
            let _ = events_tx.send(ChatEvent::Message {
                role: "user".to_string(),
                content: msg.clone(),
                metadata: None,
                created_at: Utc::now(),
            });
            msg
        }
    };

    // 3. Build the agent.
    let mut agent = {
        use swiftide::agents;

        let mut builder = agents::Agent::builder();

        match &spec.llm.provider {
            LlmProvider::Ollama(o) => {
                builder.llm(o);
            }
            LlmProvider::Anthropic(a) => {
                builder.llm(a);
            }
            LlmProvider::OpenRouter(o) => {
                builder.llm(o);
            }
            LlmProvider::OpenAICompat(o) => {
                builder.llm(o);
            }
        }

        for tool in spec.tools {
            builder.add_tool(tool);
        }

        let store_msg = store.clone();
        let events_tx_msg = events_tx.clone();
        let events_tx_before_tool = events_tx.clone();
        let running_tools_before = running_tools.clone();
        let store_after_tool = store.clone();
        let events_tx_after_tool = events_tx.clone();
        let running_tools_after = running_tools.clone();
        let validation_history_after = spec.validation_history.clone();
        let events_tx_stream = events_tx.clone();

        builder
            .system_prompt(spec.system_prompt)
            .on_new_message(move |_agent, msg| {
                let store = store_msg.clone();
                let events_tx = events_tx_msg.clone();
                let extracted = extract_role_content(msg);
                Box::pin(async move {
                    let Some((role, content)) = extracted else {
                        return Ok(()); // ToolOutput handled by after_tool
                    };
                    if content.is_empty() {
                        return Ok(());
                    }
                    // Persist message
                    store
                        .append_message(session_id, &role, &content, None)
                        .await
                        .ok();

                    // Broadcast to SSE subscribers
                    let _ = events_tx.send(ChatEvent::Message {
                        role,
                        content,
                        metadata: None,
                        created_at: Utc::now(),
                    });
                    Ok(())
                })
            })
            .on_stream(move |_agent, response| {
                let events_tx = events_tx_stream.clone();
                let delta = response
                    .delta
                    .as_ref()
                    .and_then(|d| d.message_chunk.clone());
                Box::pin(async move {
                    if let Some(delta) = delta {
                        if !delta.is_empty() {
                            let _ = events_tx.send(ChatEvent::StreamDelta { delta });
                        }
                    }
                    Ok(())
                })
            })
            .before_tool(move |_agent, tool_call| {
                let events_tx = events_tx_before_tool.clone();
                let running_tools = running_tools_before.clone();
                let name = tool_call.name().to_string();
                let args = tool_call.args().map(String::from);
                Box::pin(async move {
                    // Add to in-memory running tools and broadcast snapshot
                    let tool = RunningTool {
                        name: name.clone(),
                        args: args.clone(),
                        started_at: Utc::now(),
                        validation: None,
                    };
                    let snapshot = {
                        let mut tools = running_tools.lock().unwrap();
                        tools.push(tool);
                        tools.clone()
                    };
                    let _ = events_tx.send(ChatEvent::RunningTools { tools: snapshot });
                    Ok(())
                })
            })
            .after_tool(move |_agent, tool_call, result| {
                let store = store_after_tool.clone();
                let events_tx = events_tx_after_tool.clone();
                let running_tools = running_tools_after.clone();
                let val_history = validation_history_after.clone();
                let name = tool_call.name().to_string();
                let args = tool_call.args().map(String::from);
                let (status, output) = match result {
                    Ok(out) => ("ok", out.to_string()),
                    Err(e) => ("error", e.to_string()),
                };
                Box::pin(async move {
                    // Remove from running tools and broadcast snapshot
                    let snapshot = {
                        let mut tools = running_tools.lock().unwrap();
                        tools.retain(|t| t.name != name);
                        tools.clone()
                    };
                    let _ = events_tx.send(ChatEvent::RunningTools { tools: snapshot });

                    // Look up validation result from history (pushed by ValidatedTool)
                    let val_meta = val_history.recent(1).first().and_then(|r| {
                        if r.tool_name == name {
                            r.validation.as_ref().map(|v| {
                                serde_json::json!({
                                    "status": if v.approved { "approved" } else { "rejected" },
                                    "reasoning": v.reasoning,
                                    "risk": v.risk,
                                })
                            })
                        } else {
                            None
                        }
                    });

                    // Persist and broadcast the result
                    let content = format!("{name}: {output}");
                    let mut metadata = serde_json::json!({
                        "tool_name": name,
                        "tool_args": args.as_deref().unwrap_or("{}"),
                        "status": status,
                    });
                    if let Some(val) = val_meta {
                        metadata
                            .as_object_mut()
                            .unwrap()
                            .insert("validation".to_string(), val);
                    }
                    store
                        .append_message(session_id, "tool_result", &content, Some(&metadata))
                        .await
                        .ok();
                    let _ = events_tx.send(ChatEvent::Message {
                        role: "tool_result".to_string(),
                        content,
                        metadata: Some(metadata),
                        created_at: Utc::now(),
                    });
                    Ok(())
                })
            })
            .limit(spec.loop_limit);

        builder.build().context("failed to build agent")?
    };

    // 4. Race agent execution against control signals. When a signal fires,
    // the agent future is dropped — immediately stopping all LLM calls and
    // tool execution.
    tokio::pin!(shutdown);
    let mut prompt = initial_prompt;

    let final_reason = loop {
        let reason = tokio::select! {
            result = agent.query(prompt.clone()) => {
                match result {
                    Ok(()) => StopReason::Completed,
                    Err(e) => StopReason::AgentError(e.into()),
                }
            }
            _ = handles.cancel.cancelled() => StopReason::Cancelled,
            _ = handles.pause_notify.notified() => StopReason::Paused,
            _ = handles.approval_notify.notified() => StopReason::AwaitingApproval,
            _ = &mut shutdown => StopReason::Shutdown,
            _ = deadline_signal(spec.deadline) => StopReason::DeadlineExpired,
            _ = handles.budget_notify.notified() => {
                let used = store.get_token_usage(session_id).await.unwrap_or(0);
                let limit = store.get_token_budget(session_id).await.unwrap_or(0);
                StopReason::BudgetExceeded { used, limit }
            }
        };

        // Interactive sessions idle after a completed turn, waiting for the
        // next user message (which may already be queued in the channel).
        if spec.interactive && matches!(reason, StopReason::Completed) {
            let _ = events_tx.send(ChatEvent::Idle);
            let idle = async {
                match spec.idle_timeout {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            };
            let next = tokio::select! {
                msg = user_rx.recv() => match msg {
                    Some(m) => Some(m),
                    None => None, // channel closed — finish the session
                },
                _ = idle => None,
                _ = handles.cancel.cancelled() => break StopReason::Cancelled,
                _ = handles.pause_notify.notified() => break StopReason::Paused,
                _ = &mut shutdown => break StopReason::Shutdown,
                _ = deadline_signal(spec.deadline) => break StopReason::DeadlineExpired,
            };
            match next {
                Some(msg) => {
                    store
                        .append_message(session_id, "user", &msg, None)
                        .await
                        .ok();
                    let _ = events_tx.send(ChatEvent::Message {
                        role: "user".to_string(),
                        content: msg.clone(),
                        metadata: None,
                        created_at: Utc::now(),
                    });
                    prompt = msg;
                    continue;
                }
                None => break StopReason::Completed,
            }
        }

        break reason;
    };

    match final_reason {
        StopReason::Completed => {
            let data = json!({"reason": "agent finished"});
            store
                .transition_state(session_id, "completed", true, &data)
                .await?;
            emit_state_change(&events_tx, "completed", &data);
        }
        StopReason::AgentError(e) => {
            return Err(e.context("agent query failed"));
        }
        StopReason::Cancelled => {
            let data = json!({"reason": "cancelled"});
            store
                .transition_state(session_id, "cancelled", true, &data)
                .await
                .ok();
            emit_state_change(&events_tx, "cancelled", &data);
        }
        StopReason::Paused => {
            let data = json!({"reason": "manual_pause"});
            store
                .transition_state(session_id, "paused", false, &data)
                .await
                .ok();
            emit_state_change(&events_tx, "paused", &data);
        }
        StopReason::AwaitingApproval => {
            // State already transitioned by the set_phase tool — nothing to do.
        }
        StopReason::Shutdown => {
            let data = json!({"reason": "server_shutdown"});
            store
                .transition_state(session_id, "awaiting_retry", false, &data)
                .await
                .ok();
            emit_state_change(&events_tx, "awaiting_retry", &data);
        }
        StopReason::DeadlineExpired => {
            let data = json!({"reason": spec.deadline_reason});
            store
                .transition_state(session_id, "awaiting_retry", false, &data)
                .await
                .ok();
            emit_state_change(&events_tx, "awaiting_retry", &data);
        }
        StopReason::BudgetExceeded { used, limit } => {
            let data = json!({
                "reason": "token_budget_exceeded",
                "tokens_used": used,
                "budget_limit": limit,
            });
            store
                .transition_state(session_id, "paused", false, &data)
                .await
                .ok();
            emit_state_change(&events_tx, "paused", &data);
        }
    }

    Ok(())
}
