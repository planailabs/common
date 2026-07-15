//! Dioxus transcript rendering for agent-chat sessions: messages, tool
//! results, state changes, and the sanitizing markdown-to-HTML helper.
//!
//! The `t!` keys used here (healer-*/chat-*) live in the host application's
//! locale files; the host provides the dioxus-i18n context.

use dioxus::prelude::*;
use dioxus_i18n::t;
use plan_ai_design::{Badge, BadgeVariant};
use serde::{Deserialize, Serialize};

/// One model offered in a session-launch model picker.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    pub name: String,
    pub model: String,
    pub provider: String,
}

/// One persisted chat message as rendered in the transcript.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMsg {
    pub role: String,
    pub content: String,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

pub fn reason_display(reason: &str) -> String {
    match reason {
        "manual_pause" => t!("healer-paused-by-user"),
        "token_budget_exceeded" => t!("healer-token-budget"),
        "proxy_token_expiring" => t!("healer-proxy-expiring"),
        "server_shutdown" => t!("healer-server-shutdown"),
        other => other.to_string(),
    }
}

pub fn state_badge(st: &str) -> (BadgeVariant, String) {
    match st {
        "starting" | "loading" | "created" | "initializing" => {
            (BadgeVariant::Info, t!("healer-state-initializing"))
        }
        "diagnosing" => (BadgeVariant::Warn, t!("healer-state-diagnosing")),
        "running" => (BadgeVariant::Info, t!("chat-state-running")),
        "planning" => (BadgeVariant::Info, t!("chat-state-planning")),
        "executing" => (BadgeVariant::Warn, t!("chat-state-executing")),
        "executed" => (BadgeVariant::Success, t!("chat-state-executed")),
        "remediating" => (BadgeVariant::Warn, t!("healer-state-remediating")),
        "verifying" => (BadgeVariant::Accent, t!("healer-state-verifying")),
        "completed" | "done" => (BadgeVariant::Success, t!("healer-state-done")),
        "failed" => (BadgeVariant::Danger, t!("healer-state-failed")),
        "cancelled" => (BadgeVariant::Neutral, t!("healer-state-cancelled")),
        "paused" => (BadgeVariant::Warn, t!("healer-state-paused")),
        "awaiting_approval" => (BadgeVariant::Warn, t!("healer-state-awaiting-approval")),
        "awaiting_retry" => (BadgeVariant::Info, t!("healer-state-awaiting-retry")),
        "needs_human_attention" => (BadgeVariant::Danger, t!("healer-state-needs-human")),
        _ => (BadgeVariant::Neutral, t!("healer-state-unknown")),
    }
}

/// Derive a left-border color class from a state name, matching `state_badge` hues.
fn state_border(st: &str) -> &'static str {
    match st {
        "starting" | "loading" | "created" | "initializing" | "awaiting_retry" => "border-info",
        "diagnosing" | "paused" | "remediating" => "border-warn",
        "verifying" => "border-accent",
        "completed" | "done" => "border-success",
        "failed" | "needs_human_attention" => "border-danger",
        "cancelled" | _ => "border-line",
    }
}

/// Render a state_change message in the same style as agent/system messages.
fn render_state_change(msg: &ChatMsg) -> Element {
    let (state, reason) = if let Ok(data) = serde_json::from_str::<serde_json::Value>(&msg.content)
    {
        (
            data.get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            data.get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        )
    } else {
        ("unknown".to_string(), String::new())
    };

    let (badge_variant, label) = state_badge(&state);
    let border = state_border(&state);
    let reason_text = reason_display(&reason);

    rsx! {
        div { class: "p-3 rounded bg-surface-2 border-l-4 {border}",
            div { class: "flex items-center gap-1.5 mb-1",
                span { class: "w-5 h-5 flex items-center justify-center rounded-full bg-surface-3 text-xs font-bold text-fg", "S" }
                span { class: "text-xs font-semibold text-fg-muted uppercase tracking-wider", {t!("healer-event-state-change")} }
            }
            div { class: "flex items-center gap-2",
                Badge { variant: badge_variant, "{label}" }
                if !reason.is_empty() {
                    span { class: "text-sm text-fg", "{reason_text}" }
                }
            }
        }
    }
}

/// Render a chat message. Tool calls/results get special UI.
/// Assistant/system messages are rendered as markdown via dangerous_inner_html.
pub fn render_message(msg: &ChatMsg) -> Element {
    if msg.role == "tool_result" {
        return render_tool_result(msg);
    }

    if msg.role == "state_change" {
        return render_state_change(msg);
    }

    let (bg, icon, label) = match msg.role.as_str() {
        "system" => (
            "bg-surface-2 border-l-4 border-line",
            "S",
            t!("healer-event-system"),
        ),
        "assistant" => (
            "bg-info-soft border-l-4 border-info",
            "A",
            t!("healer-event-agent"),
        ),
        "user" => (
            "bg-success-soft border-l-4 border-success",
            "U",
            t!("healer-event-user"),
        ),
        "summary" => (
            "bg-accent-soft border-l-4 border-accent",
            "S",
            t!("healer-event-summary"),
        ),
        _ => (
            "bg-surface-2 border-l-4 border-line-soft",
            "-",
            t!("healer-event-other"),
        ),
    };

    let html = simple_md_to_html(&msg.content);

    rsx! {
        div { class: "p-3 rounded {bg}",
            div { class: "flex items-center gap-1.5 mb-1",
                span { class: "w-5 h-5 flex items-center justify-center rounded-full bg-surface-2 text-xs font-bold text-fg", "{icon}" }
                span { class: "text-xs font-semibold text-fg-muted uppercase tracking-wider", "{label}" }
            }
            div {
                class: "text-sm text-fg-strong prose prose-sm dark:prose-invert max-w-none",
                dangerous_inner_html: "{html}",
            }
        }
    }
}

/// Largest index <= `max` that is a char boundary (safe multibyte truncation).
fn floor_char_boundary(s: &str, max: usize) -> usize {
    let mut i = max.min(s.len());
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

pub fn render_tool_result(msg: &ChatMsg) -> Element {
    let (tool_name, result) = msg
        .content
        .split_once(": ")
        .unwrap_or(("tool", &msg.content));
    let is_error = result.starts_with("Error:");
    let is_rejected =
        result.starts_with("[Validation rejected]") || result.starts_with("[Validator rejected]");
    let truncated = result.len() > 500;
    let preview = if truncated {
        &result[..floor_char_boundary(result, 500)]
    } else {
        result
    };
    let tool_badge = if is_rejected {
        "badge badge-danger font-mono"
    } else if is_error {
        "badge badge-danger font-mono"
    } else {
        "badge badge-neutral font-mono"
    };
    let display = if truncated {
        format!("{preview}\n... (output truncated)")
    } else {
        preview.to_string()
    };
    let args = msg
        .metadata
        .as_ref()
        .and_then(|m| m.get("tool_args"))
        .and_then(|v| v.as_str())
        .filter(|a| *a != "{}")
        .unwrap_or("");
    let args_short = if args.len() > 120 {
        format!("{}...", &args[..floor_char_boundary(args, 120)])
    } else {
        args.to_string()
    };
    let has_args = !args.is_empty();

    // Extract validation metadata
    let _validation_status = msg
        .metadata
        .as_ref()
        .and_then(|m| m.get("validation"))
        .and_then(|v| v.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let validation_reasoning = msg
        .metadata
        .as_ref()
        .and_then(|m| m.get("validation"))
        .and_then(|v| v.get("reasoning"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let validation_risk = msg
        .metadata
        .as_ref()
        .and_then(|m| m.get("validation"))
        .and_then(|v| v.get("risk"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let risk_badge = match validation_risk {
        "mutating" => Some(("M", "bg-warn text-warn-strong")),
        "destructive" => Some(("D", "bg-danger text-danger-strong")),
        _ => None,
    };

    rsx! {
        div { class: "p-2 rounded bg-surface-2 border border-line-soft",
            details { class: "group",
                summary { class: "flex items-center gap-2 cursor-pointer select-none",
                    span { class: "{tool_badge}", "{tool_name}" }
                    if let Some((label, cls)) = risk_badge {
                        span { class: "text-[10px] font-bold px-1 rounded {cls}", "{label}" }
                    }
                    if has_args {
                        span { class: "text-xs text-fg-muted truncate max-w-md", "{args_short}" }
                    }
                    if is_rejected {
                        span { class: "text-xs font-semibold text-danger", "REJECTED" }
                    } else if is_error {
                        span { class: "text-xs text-danger", {t!("healer-event-error")} }
                    }
                }
                if has_args {
                    pre { class: "mt-2 p-2 text-xs font-mono bg-surface-3 text-fg-muted rounded overflow-x-auto max-h-32 overflow-y-auto whitespace-pre-wrap",
                        "{args}"
                    }
                }
                if !validation_reasoning.is_empty() {
                    div { class: "mt-1 px-2 py-1 text-xs rounded bg-surface-3 text-fg-muted border-l-2 border-accent",
                        span { class: "font-semibold", "Validation: " }
                        "{validation_reasoning}"
                    }
                }
                pre { class: "log-output mt-1 max-h-64 min-h-0",
                    "{display}"
                }
            }
        }
    }
}

/// Render markdown to HTML using pulldown_cmark.
///
/// The result is rendered via `dangerous_inner_html`, so we strip raw HTML
/// events from the parser stream (otherwise `<script>` and friends embedded
/// in the markdown would survive) and rewrite link/image URLs whose scheme
/// isn't on the allowlist (`http`, `https`, `mailto`) to `#`, blocking
/// `javascript:` / `data:` / `vbscript:` payloads.
pub fn simple_md_to_html(md: &str) -> String {
    use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, html};
    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let events = Parser::new_ext(md, options).filter_map(|event| match event {
        Event::Html(_) | Event::InlineHtml(_) => None,
        Event::Start(Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }) => Some(Event::Start(Tag::Link {
            link_type,
            dest_url: if is_safe_uri(&dest_url) {
                dest_url
            } else {
                CowStr::Borrowed("#")
            },
            title,
            id,
        })),
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => Some(Event::Start(Tag::Image {
            link_type,
            dest_url: if is_safe_uri(&dest_url) {
                dest_url
            } else {
                CowStr::Borrowed("#")
            },
            title,
            id,
        })),
        e => Some(e),
    });
    let mut output = String::with_capacity(md.len() * 2);
    html::push_html(&mut output, events);
    output
}

fn is_safe_uri(uri: &str) -> bool {
    let trimmed = uri.trim_start();
    if trimmed.is_empty() {
        return true;
    }
    if trimmed.starts_with('/') || trimmed.starts_with('#') || trimmed.starts_with('?') {
        return true;
    }
    match trimmed.find(':') {
        Some(end) => {
            let scheme = trimmed[..end].to_ascii_lowercase();
            matches!(scheme.as_str(), "http" | "https" | "mailto")
        }
        None => true,
    }
}
