//! Reusable UI + SSE wire layer for `plan-ai-chat` agent sessions.
//!
//! - [`wire`]: the SSE stream-event types shared between server endpoints and
//!   the WASM client renderer (serde shapes are the protocol — append-only).
//! - [`render`]: Dioxus transcript rendering (messages, tool results, state
//!   changes) and the sanitizing markdown-to-HTML helper.
//! - [`convert`] (feature `server`): conversion from live
//!   `plan_ai_chat::ChatEvent`s and store rows into wire types.
//!
//! Everything outside `convert` is wasm-safe; hosts provide the dioxus-i18n
//! context (the `t!` keys live in the host's locale files).

pub mod render;
pub mod sidebar;
pub mod wire;

#[cfg(feature = "server")]
pub mod backend;
#[cfg(feature = "server")]
pub mod convert;
