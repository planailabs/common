//! Unified LLM model configuration shared by every chat domain.
//!
//! Hosts configure ONE list of model entries (TOML: `[[models]]`); each entry
//! can serve as a regular chat model, a validator/guard model, or both, and
//! can be restricted to specific chat types via `restrict` (session_type
//! strings like "healer" or "chat" — absent means available to all types):
//!
//! ```toml
//! [[models]]
//! name = "Claude Haiku 4.5"
//! provider = "anthropic"
//! model = "claude-haiku-4-5-20251001"
//! validator = true          # offered in validator pickers
//! restrict = "healer"       # string or list; omit for all chat types
//! input_cost_per_mtok = 1.0 # optional; enables spend estimation
//! output_cost_per_mtok = 5.0
//! ```

use serde::{Deserialize, Deserializer, Serialize};

/// A configured LLM model entry (model pickers, validator pickers, spend
/// dashboard pricing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmModelEntry {
    /// Human-readable display name shown in the dropdown.
    pub name: String,
    /// Model identifier passed to the provider (e.g. "gemma4", "claude-sonnet-4-6").
    pub model: String,
    /// Provider: "ollama", "anthropic", "openrouter", or the name of an
    /// OpenAI-compatible source.
    pub provider: String,
    /// Per-model token budget override. If set, overrides the global
    /// `token_budget` when this model is selected. 0 = unlimited.
    #[serde(default)]
    pub token_budget: Option<u64>,
    /// USD per 1M input tokens. When set (together with
    /// `output_cost_per_mtok`), the AI spend dashboard shows estimated
    /// dollar spend for this model; token counts are shown either way.
    #[serde(default)]
    pub input_cost_per_mtok: Option<f64>,
    /// USD per 1M output tokens. See `input_cost_per_mtok`.
    #[serde(default)]
    pub output_cost_per_mtok: Option<f64>,
    /// Offered in validator/guard pickers.
    #[serde(default)]
    pub validator: bool,
    /// Validator-only: excluded from regular model pickers.
    #[serde(default)]
    pub validator_only: bool,
    /// Chat types (session_type strings) this entry is limited to.
    /// Accepts a single string or a list in config; empty = all types.
    #[serde(default, deserialize_with = "one_or_many")]
    pub restrict: Vec<String>,
}

/// Accept `restrict = "healer"` as well as `restrict = ["healer", "chat"]`.
fn one_or_many<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match OneOrMany::deserialize(d)? {
        OneOrMany::One(s) => vec![s],
        OneOrMany::Many(v) => v,
    })
}

impl LlmModelEntry {
    /// Entry with just the identity fields; everything else defaulted
    /// (no budget, no prices, regular model, all chat types).
    pub fn basic(name: &str, model: &str, provider: &str) -> Self {
        Self {
            name: name.into(),
            model: model.into(),
            provider: provider.into(),
            token_budget: None,
            input_cost_per_mtok: None,
            output_cost_per_mtok: None,
            validator: false,
            validator_only: false,
            restrict: Vec::new(),
        }
    }

    /// `basic()` plus a per-model token budget.
    pub fn with_budget(name: &str, model: &str, provider: &str, budget: u64) -> Self {
        Self {
            token_budget: Some(budget),
            ..Self::basic(name, model, provider)
        }
    }

    /// Return the display name with an auto-appended provider suffix
    /// (e.g. "Gemma 4" becomes "Gemma 4 (Ollama)") unless it already
    /// contains the provider name (case-insensitive).
    pub fn display_name(&self) -> String {
        let lower = self.name.to_lowercase();
        let provider_lower = self.provider.to_lowercase();
        if lower.contains(&provider_lower) {
            self.name.clone()
        } else {
            let suffix = match self.provider.as_str() {
                "ollama" => "Ollama",
                "anthropic" => "Anthropic",
                "openrouter" => "OpenRouter",
                "openai_compat" => "plan.ai Hosted",
                other => other,
            };
            format!("{} ({})", self.name, suffix)
        }
    }

    fn allows(&self, chat_type: &str) -> bool {
        self.restrict.is_empty() || self.restrict.iter().any(|t| t == chat_type)
    }
}

/// The full configured model list with type-aware accessors.
#[derive(Debug, Clone, Default)]
pub struct ModelCatalog {
    entries: Vec<LlmModelEntry>,
}

impl ModelCatalog {
    pub fn new(entries: Vec<LlmModelEntry>) -> Self {
        Self { entries }
    }

    /// Every configured entry (spend dashboards, pricing).
    pub fn entries(&self) -> &[LlmModelEntry] {
        &self.entries
    }

    /// Regular-model picker entries for a chat type.
    pub fn models_for(&self, chat_type: &str) -> Vec<&LlmModelEntry> {
        self.entries
            .iter()
            .filter(|e| !e.validator_only && e.allows(chat_type))
            .collect()
    }

    /// Validator picker entries for a chat type.
    pub fn validators_for(&self, chat_type: &str) -> Vec<&LlmModelEntry> {
        self.entries
            .iter()
            .filter(|e| e.validator && e.allows(chat_type))
            .collect()
    }

    pub fn find(&self, provider: &str, model: &str) -> Option<&LlmModelEntry> {
        self.entries
            .iter()
            .find(|e| e.provider == provider && e.model == model)
    }

    /// (input, output) USD per 1M tokens, when both prices are configured.
    pub fn price_of(&self, provider: &str, model: &str) -> Option<(f64, f64)> {
        self.find(provider, model)
            .and_then(|e| Some((e.input_cost_per_mtok?, e.output_cost_per_mtok?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restrict_accepts_string_and_list() {
        let one: LlmModelEntry =
            toml::from_str("name = \"A\"\nmodel = \"a\"\nprovider = \"ollama\"\nrestrict = \"healer\"")
                .unwrap();
        assert_eq!(one.restrict, vec!["healer"]);
        let many: LlmModelEntry = toml::from_str(
            "name = \"A\"\nmodel = \"a\"\nprovider = \"ollama\"\nrestrict = [\"healer\", \"chat\"]",
        )
        .unwrap();
        assert_eq!(many.restrict, vec!["healer", "chat"]);
    }

    #[test]
    fn catalog_filters_by_type_and_role() {
        let catalog = ModelCatalog::new(vec![
            LlmModelEntry {
                name: "All".into(),
                model: "all".into(),
                provider: "ollama".into(),
                token_budget: None,
                input_cost_per_mtok: None,
                output_cost_per_mtok: None,
                validator: false,
                validator_only: false,
                restrict: vec![],
            },
            LlmModelEntry {
                name: "HealerOnly".into(),
                model: "h".into(),
                provider: "ollama".into(),
                token_budget: None,
                input_cost_per_mtok: None,
                output_cost_per_mtok: None,
                validator: true,
                validator_only: false,
                restrict: vec!["healer".into()],
            },
            LlmModelEntry {
                name: "Guard".into(),
                model: "g".into(),
                provider: "ollama".into(),
                token_budget: None,
                input_cost_per_mtok: None,
                output_cost_per_mtok: None,
                validator: true,
                validator_only: true,
                restrict: vec![],
            },
        ]);
        let chat_models: Vec<&str> = catalog
            .models_for("chat")
            .iter()
            .map(|e| e.model.as_str())
            .collect();
        assert_eq!(chat_models, vec!["all"]);
        let healer_models: Vec<&str> = catalog
            .models_for("healer")
            .iter()
            .map(|e| e.model.as_str())
            .collect();
        assert_eq!(healer_models, vec!["all", "h"]);
        let healer_validators: Vec<&str> = catalog
            .validators_for("healer")
            .iter()
            .map(|e| e.model.as_str())
            .collect();
        assert_eq!(healer_validators, vec!["h", "g"]);
    }
}
