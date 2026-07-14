//! Model pricing and spend math. Token events themselves are recorded through
//! [`crate::store::ChatStore::append_token_event`]; this module turns token
//! counts into dollar estimates.

/// Pricing for one provider/model pair, in dollars per million tokens.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelPricing {
    pub provider: String,
    pub model: String,
    pub input_cost_per_mtok: Option<f64>,
    pub output_cost_per_mtok: Option<f64>,
}

impl ModelPricing {
    /// Estimated dollar cost for a token count pair. None if no pricing set.
    pub fn cost(&self, input_tokens: u64, output_tokens: u64) -> Option<f64> {
        match (self.input_cost_per_mtok, self.output_cost_per_mtok) {
            (None, None) => None,
            (i, o) => Some(
                i.unwrap_or(0.0) * input_tokens as f64 / 1_000_000.0
                    + o.unwrap_or(0.0) * output_tokens as f64 / 1_000_000.0,
            ),
        }
    }
}

/// Lookup table of model pricing entries.
#[derive(Debug, Clone, Default)]
pub struct PricingTable(pub Vec<ModelPricing>);

impl PricingTable {
    pub fn find(&self, provider: &str, model: &str) -> Option<&ModelPricing> {
        self.0
            .iter()
            .find(|p| p.provider == provider && p.model == model)
    }

    /// Estimated dollar cost for a usage row; None if the model is unpriced.
    pub fn cost(
        &self,
        provider: &str,
        model: &str,
        input_tokens: u64,
        output_tokens: u64,
    ) -> Option<f64> {
        self.find(provider, model)?.cost(input_tokens, output_tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_math() {
        let p = ModelPricing {
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            input_cost_per_mtok: Some(3.0),
            output_cost_per_mtok: Some(15.0),
        };
        let c = p.cost(1_000_000, 1_000_000).unwrap();
        assert!((c - 18.0).abs() < 1e-9);
        assert_eq!(
            ModelPricing {
                provider: "ollama".into(),
                model: "gemma4".into(),
                input_cost_per_mtok: None,
                output_cost_per_mtok: None,
            }
            .cost(1, 1),
            None
        );
    }

    #[test]
    fn table_lookup() {
        let t = PricingTable(vec![ModelPricing {
            provider: "openrouter".into(),
            model: "x".into(),
            input_cost_per_mtok: Some(1.0),
            output_cost_per_mtok: None,
        }]);
        assert!(t.cost("openrouter", "x", 2_000_000, 0).unwrap() - 2.0 < 1e-9);
        assert!(t.cost("openrouter", "y", 1, 1).is_none());
    }
}
