//! Core chat data model: sessions, messages, events, running tools.
//!
//! Extracted from mac-mgmt-healer. Wire formats (serde shapes, message roles,
//! event tags) are byte-compatible with the healer's originals so existing
//! SSE consumers and persisted rows keep working.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A chat session. Domain crates map their own vocabulary onto the generic
/// fields: for the healer `scope_id` is the cluster id, `subject` is the
/// instance id and `initial_context` is the initial service issues snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSession {
    pub id: Uuid,
    /// Coarse grouping key (healer: cluster_id; chatbot: owner org/user key).
    /// The aliases parse pre-unification healer JSON session files.
    #[serde(alias = "cluster_id")]
    pub scope_id: Uuid,
    /// Fine target key (healer: instance_id; chatbot: free-form/empty).
    #[serde(alias = "instance_id")]
    pub subject: String,
    /// Raw domain state string (e.g. "diagnosing"). Interpreted via [`crate::StateModel`].
    pub state: String,
    pub state_data: serde_json::Value,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    /// Domain snapshot captured at session creation.
    #[serde(alias = "initial_issues")]
    pub initial_context: serde_json::Value,
    /// LLM provider used for this session (e.g. "ollama", "anthropic").
    pub provider: Option<String>,
    /// LLM model used for this session.
    pub model: Option<String>,
    /// Human-readable label (set by the LLM or the creator).
    pub label: Option<String>,
}

/// A persisted chat message. Roles in use: `user`, `assistant`, `system`,
/// `tool_result`, `state_change`, `pin`, `summary`, `approval_request`,
/// `approval_decision`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: Uuid,
    pub session_id: Uuid,
    pub role: String,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Event emitted during a running session, consumed by SSE streams.
///
/// The first five variants are wire-identical to the healer's `HealerEvent`.
/// The remaining variants are additive; consumers must tolerate unknown tags.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatEvent {
    Message {
        role: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
        created_at: DateTime<Utc>,
    },
    /// Snapshot of currently executing tools. Sent on every tool start/end.
    RunningTools {
        tools: Vec<RunningTool>,
    },
    /// Ephemeral status message (not persisted). Shown in UI but cleared on reload.
    Status {
        message: String,
    },
    State {
        state: String,
        state_data: serde_json::Value,
    },
    Done {
        state: String,
    },
    /// A tool call is suspended awaiting human approval.
    ApprovalRequest {
        approval_id: Uuid,
        tool_name: String,
        args: String,
        reason: String,
        risk: String,
        requested_at: DateTime<Utc>,
    },
    /// A pending approval was resolved (approved/approved_all/denied/timeout).
    ApprovalResolved {
        approval_id: Uuid,
        decision: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        by: Option<String>,
    },
    /// Interactive session finished its turn and is waiting for user input.
    Idle,
    /// Streaming completion chunk (best-effort, broadcast only).
    StreamDelta {
        delta: String,
    },
}

/// A tool currently being executed by the agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningTool {
    pub name: String,
    pub args: Option<String>,
    pub started_at: DateTime<Utc>,
    /// Validation state: None before validation, Some after verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<RunningToolValidation>,
}

/// Validation state attached to a running tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningToolValidation {
    /// "validating", "approved", "skipped", "awaiting_approval"
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// "read_only", "session_local", "mutating", "destructive"
    pub risk: String,
}

/// Send a `state_change` message event followed by a `State` event over the
/// SSE broadcast. Call this after `ChatStore::transition_state` (which handles
/// the DB-side message).
pub fn emit_state_change(
    events_tx: &tokio::sync::broadcast::Sender<ChatEvent>,
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
    let _ = events_tx.send(ChatEvent::Message {
        role: "state_change".to_string(),
        content,
        metadata: Some(state_data.clone()),
        created_at: chrono::Utc::now(),
    });
    let _ = events_tx.send(ChatEvent::State {
        state: state.to_string(),
        state_data: state_data.clone(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wire compatibility with the healer's original HealerEvent JSON shapes.
    #[test]
    fn event_serde_matches_healer_wire_format() {
        let ev = ChatEvent::Message {
            role: "assistant".into(),
            content: "hi".into(),
            metadata: None,
            created_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "message");
        assert_eq!(json["role"], "assistant");
        assert!(json.get("metadata").is_none());

        let ev = ChatEvent::State {
            state: "diagnosing".into(),
            state_data: serde_json::json!({"reason": "x"}),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "state");
        assert_eq!(json["state"], "diagnosing");

        let ev = ChatEvent::Done {
            state: "completed".into(),
        };
        assert_eq!(serde_json::to_value(&ev).unwrap()["type"], "done");

        // Round-trip a healer-era payload into ChatEvent.
        let healer_json = serde_json::json!({
            "type": "running_tools",
            "tools": [{
                "name": "read_file",
                "args": "{}",
                "started_at": "2026-01-01T00:00:00Z",
                "validation": {"status": "approved", "reasoning": "ok", "risk": "mutating"}
            }]
        });
        let ev: ChatEvent = serde_json::from_value(healer_json).unwrap();
        match ev {
            ChatEvent::RunningTools { tools } => {
                assert_eq!(tools[0].validation.as_ref().unwrap().status, "approved");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn message_serde_roundtrip() {
        let msg = ChatMessage {
            id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            role: "pin".into(),
            content: r#"{"slot":"diagnosis","summary":"s","affected_services":[]}"#.into(),
            metadata: Some(serde_json::json!({"pin_diagnosis": {"summary": "s"}})),
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ChatMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.role, "pin");
        assert_eq!(back.content, msg.content);
    }
}
