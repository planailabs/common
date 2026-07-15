//! SSE wire types shared between server streaming endpoints and the WASM
//! client renderer. These serde shapes are the protocol: change them
//! append-only (new optional fields), never rename existing fields.

use serde::{Deserialize, Serialize};

/// Event streamed from server to client during an agent-chat session.
/// This is the canonical wire type — used directly by both the server
/// streaming functions and the WASM client renderer.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatStreamEvent {
    /// Event kind: "session_created", "message", "running_tools", "pins",
    /// "staff_pings", "state", "done", "error"
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_tools: Option<Vec<ChatRunningTool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pins: Option<Vec<ChatPin>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staff_pings: Option<Vec<ChatStaffPing>>,
    /// Ephemeral status message (e.g. "Waiting for daemon reconnect...")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// Human-readable reason for the current state (e.g. "manual_pause",
    /// "token_budget_exceeded"). Sent alongside "state" and "done" events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatRunningTool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
    pub started_at: String,
    /// Validation status for this tool call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<ChatToolValidation>,
}

/// Validation verdict for an agent tool call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatToolValidation {
    /// "approved", "rejected", "skipped", "validating", "error"
    pub status: String,
    /// Validator reasoning (both for approvals and rejections).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// Risk level: "read_only", "session_local", "mutating", "destructive"
    pub risk: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatPin {
    pub slot: String,
    pub summary: String,
    #[serde(default)]
    pub affected_services: Vec<String>,
}

/// A staff-attention ping surfaced in a session stream (used by domains
/// whose agents can escalate to humans, e.g. the healer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatStaffPing {
    pub id: String,
    pub category: String,
    pub message: String,
    pub resolved: bool,
    pub created_at: String,
}
