//! Server-side conversion from live `plan_ai_chat` session state into the
//! SSE wire types. Feature `server` only — not wasm-safe.

use crate::wire::{ChatPin, ChatRunningTool, ChatStreamEvent, ChatToolValidation};

pub fn running_tools_to_wire(tools: &[plan_ai_chat::RunningTool]) -> Vec<ChatRunningTool> {
    tools
        .iter()
        .map(|t| ChatRunningTool {
            name: t.name.clone(),
            args: t.args.clone(),
            started_at: t.started_at.to_rfc3339(),
            validation: t.validation.as_ref().map(|v| ChatToolValidation {
                status: v.status.clone(),
                reasoning: v.reasoning.clone(),
                risk: v.risk.clone(),
            }),
        })
        .collect()
}

/// Convert a live session event into its SSE wire shape. The bool is
/// "session finished" (terminates the stream).
pub fn chat_event_to_stream(event: &plan_ai_chat::ChatEvent) -> (ChatStreamEvent, bool) {
    use plan_ai_chat::ChatEvent;
    let empty = ChatStreamEvent::default();
    match event {
        ChatEvent::Message {
            role,
            content,
            metadata,
            ..
        } => (
            ChatStreamEvent {
                kind: "message".to_string(),
                role: Some(role.clone()),
                content: Some(content.clone()),
                metadata: metadata.clone(),
                ..empty
            },
            false,
        ),
        ChatEvent::RunningTools { tools } => (
            ChatStreamEvent {
                kind: "running_tools".to_string(),
                running_tools: Some(running_tools_to_wire(tools)),
                ..empty
            },
            false,
        ),
        ChatEvent::State { state, state_data } => {
            let reason = state_data
                .get("reason")
                .and_then(|v| v.as_str())
                .map(String::from);
            (
                ChatStreamEvent {
                    kind: "state".to_string(),
                    state: Some(state.clone()),
                    state_reason: reason,
                    ..empty
                },
                false,
            )
        }
        ChatEvent::Status { message } => (
            ChatStreamEvent {
                kind: "status".to_string(),
                status_message: Some(message.clone()),
                ..empty
            },
            false,
        ),
        ChatEvent::Done { state } => (
            ChatStreamEvent {
                kind: "done".to_string(),
                state: Some(state.clone()),
                ..empty
            },
            true,
        ),
        ChatEvent::ApprovalRequest {
            approval_id,
            tool_name,
            args,
            reason,
            risk,
            requested_at,
        } => (
            ChatStreamEvent {
                kind: "approval_request".to_string(),
                metadata: Some(serde_json::json!({
                    "approval_id": approval_id,
                    "tool_name": tool_name,
                    "tool_args": args,
                    "reason": reason,
                    "risk": risk,
                    "requested_at": requested_at,
                })),
                ..empty
            },
            false,
        ),
        ChatEvent::ApprovalResolved {
            approval_id,
            decision,
            by,
        } => (
            ChatStreamEvent {
                kind: "approval_resolved".to_string(),
                metadata: Some(serde_json::json!({
                    "approval_id": approval_id,
                    "decision": decision,
                    "by": by,
                })),
                ..empty
            },
            false,
        ),
        ChatEvent::Idle => (
            ChatStreamEvent {
                kind: "idle".to_string(),
                ..empty
            },
            false,
        ),
        ChatEvent::StreamDelta { delta } => (
            ChatStreamEvent {
                kind: "stream_delta".to_string(),
                content: Some(delta.clone()),
                ..empty
            },
            false,
        ),
    }
}

/// Wire event for a pending tool-call approval (snapshot replay on stream
/// connect; live requests arrive as `ChatEvent::ApprovalRequest`).
pub fn approval_request_event(p: &plan_ai_chat::PendingApproval) -> ChatStreamEvent {
    ChatStreamEvent {
        kind: "approval_request".to_string(),
        metadata: Some(serde_json::json!({
            "approval_id": p.id,
            "tool_name": p.tool_name,
            "tool_args": p.args,
            "reason": p.reason,
            "risk": plan_ai_chat::validation::risk_to_str(p.risk),
            "guard_reasoning": p.guard_reasoning,
            "requested_at": p.requested_at,
        })),
        ..ChatStreamEvent::default()
    }
}

/// Latest pin per slot from the persisted transcript, ordered by the
/// domain's `PinConfig` slots; pins in slots outside the config (free-form
/// pinning) trail in name order rather than being dropped.
pub fn extract_pins_from_messages(
    messages: &[plan_ai_chat::ChatMessage],
    pin_config: &plan_ai_chat::tools::PinConfig,
) -> Vec<ChatPin> {
    let mut pins = std::collections::HashMap::<String, ChatPin>::new();
    for msg in messages {
        if msg.role == "pin" {
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&msg.content) {
                if let Some(slot) = data.get("slot").and_then(|v| v.as_str()) {
                    pins.insert(
                        slot.to_string(),
                        ChatPin {
                            slot: slot.to_string(),
                            summary: data
                                .get("summary")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            affected_services: data
                                .get("affected_services")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect()
                                })
                                .unwrap_or_default(),
                        },
                    );
                }
            }
        }
    }
    let mut result = Vec::new();
    for slot in &pin_config.slots {
        if let Some(pin) = pins.remove(slot.name) {
            result.push(pin);
        }
    }
    let mut rest: Vec<ChatPin> = pins.into_values().collect();
    rest.sort_by(|a, b| a.slot.cmp(&b.slot));
    result.extend(rest);
    result
}
