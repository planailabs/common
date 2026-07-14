//! Expose plan-ai-api-mcp Registry endpoints as swiftide agent tools.
//!
//! Each [`plan_ai_api_mcp::ErasedEndpoint`] becomes one dynamic tool whose
//! spec is built from the endpoint's description and input schema, dispatched
//! in-process via [`plan_ai_api_mcp::Registry::call_by_name`] with a fixed
//! [`Principal`] — the chat user's. Authorization happens inside each endpoint
//! handler, so the agent can only do what that user can do.

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use plan_ai_api_mcp::{Principal, Registry};
use swiftide::chat_completion::{Tool, ToolCall, ToolOutput, ToolSpec, errors::ToolError};
use swiftide::traits::AgentContext;

use crate::validation::ToolRisk;

/// Include/exclude glob filter over tool names. Empty include = include all.
/// Globs support a trailing/leading/inner `*` via simple pattern matching.
#[derive(Debug, Clone, Default)]
pub struct ToolFilter {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl ToolFilter {
    pub fn allows(&self, name: &str) -> bool {
        if !self.include.is_empty() && !self.include.iter().any(|p| glob_match(p, name)) {
            return false;
        }
        !self.exclude.iter().any(|p| glob_match(p, name))
    }
}

/// Minimal glob: `*` matches any (possibly empty) substring.
fn glob_match(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !s.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            return s[pos..].ends_with(part);
        } else {
            match s[pos..].find(part) {
                Some(idx) => pos += idx + part.len(),
                None => return false,
            }
        }
    }
    // Pattern ends with '*' (last part empty) — prefix/middles matched.
    true
}

/// Build one swiftide tool per registry endpoint (after filtering),
/// dispatching as `principal`. Pair each with its mapped [`ToolRisk`] so the
/// caller can wrap them in `ValidatedTool` / approval gating.
pub fn registry_tools<S: Clone + Send + Sync + 'static>(
    registry: Arc<Registry<S>>,
    state: S,
    principal: Arc<Principal>,
    filter: &ToolFilter,
    max_output_bytes: usize,
) -> Vec<(Box<dyn Tool>, ToolRisk)> {
    let mut out: Vec<(Box<dyn Tool>, ToolRisk)> = Vec::new();
    for ep in registry.endpoints() {
        let name = ep.tool_name();
        if !filter.allows(&name) {
            continue;
        }
        let risk: ToolRisk = ep.risk().into();

        let mut schema_value =
            inline_defs(&serde_json::Value::Object(ep.input_schema.clone().into_iter().collect()));
        sanitize_for_strict(&mut schema_value);
        let parameters_schema: Option<schemars::Schema> =
            serde_json::from_value(schema_value).ok();

        let mut spec_builder = ToolSpec::builder();
        spec_builder.name(&name).description(&ep.description);
        if let Some(schema) = parameters_schema {
            spec_builder.parameters_schema(schema);
        }
        let Ok(spec) = spec_builder.build() else {
            tracing::warn!(tool = %name, "failed to build tool spec, skipping endpoint");
            continue;
        };

        out.push((
            Box::new(RegistryTool {
                registry: registry.clone(),
                state: state.clone(),
                principal: principal.clone(),
                name,
                spec,
                max_output_bytes,
            }),
            risk,
        ));
    }
    out
}

struct RegistryTool<S> {
    registry: Arc<Registry<S>>,
    state: S,
    principal: Arc<Principal>,
    name: String,
    spec: ToolSpec,
    max_output_bytes: usize,
}

impl<S> Clone for RegistryTool<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            state: self.state.clone(),
            principal: self.principal.clone(),
            name: self.name.clone(),
            spec: self.spec.clone(),
            max_output_bytes: self.max_output_bytes,
        }
    }
}

#[async_trait]
impl<S: Clone + Send + Sync + 'static> Tool for RegistryTool<S> {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn tool_spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    async fn invoke(
        &self,
        _agent_context: &dyn AgentContext,
        tool_call: &ToolCall,
    ) -> Result<ToolOutput, ToolError> {
        let args: serde_json::Value = match tool_call.args() {
            Some(s) if !s.trim().is_empty() => match serde_json::from_str(s) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(ToolOutput::Fail(format!(
                        "[{}] invalid JSON arguments: {e}",
                        self.name
                    )));
                }
            },
            _ => serde_json::json!({}),
        };

        match self
            .registry
            .call_by_name(self.state.clone(), self.principal.clone(), &self.name, args)
            .await
        {
            Ok(v) => {
                let text = serde_json::to_string_pretty(&v).unwrap_or_default();
                Ok(ToolOutput::Text(cap_output(text, self.max_output_bytes)))
            }
            // ApiError (403/404/400/...) → Fail so the agent sees it and adapts.
            Err(e) => Ok(ToolOutput::Fail(format!("[{}] {e}", self.name))),
        }
    }
}

fn cap_output(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let keep = max.min(text.len());
    // Find a char boundary at or below `keep`.
    let mut cut = keep;
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{}... [truncated: {} of {} bytes shown]",
        &text[..cut],
        cut,
        text.len()
    )
}

/// OpenAI strict tool schemas (enforced by swiftide's OpenAI adapter) reject
/// `oneOf` and array-valued `type` (e.g. schemars' `["string","null"]` for
/// `Option<T>`, or enum variants). Rewrite both into the equivalent — and
/// accepted — `anyOf` form.
fn sanitize_for_strict(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(m) => {
            if let Some(one_of) = m.remove("oneOf") {
                match m.get_mut("anyOf") {
                    Some(serde_json::Value::Array(existing)) => {
                        if let serde_json::Value::Array(add) = one_of {
                            existing.extend(add);
                        }
                    }
                    _ => {
                        m.insert("anyOf".to_string(), one_of);
                    }
                }
            }
            if let Some(serde_json::Value::Array(types)) = m.get("type").cloned() {
                m.remove("type");
                let variants: Vec<serde_json::Value> = types
                    .into_iter()
                    .map(|t| serde_json::json!({ "type": t }))
                    .collect();
                match m.get_mut("anyOf") {
                    Some(serde_json::Value::Array(existing)) => existing.extend(variants),
                    _ => {
                        m.insert(
                            "anyOf".to_string(),
                            serde_json::Value::Array(variants),
                        );
                    }
                }
            }
            // OpenAI requires array schemas to declare `items`.
            if m.get("type").and_then(|t| t.as_str()) == Some("array")
                && !m.contains_key("items")
            {
                m.insert("items".to_string(), serde_json::json!({}));
            }
            for (_, val) in m.iter_mut() {
                sanitize_for_strict(val);
            }
        }
        serde_json::Value::Array(a) => {
            for x in a {
                sanitize_for_strict(x);
            }
        }
        _ => {}
    }
}

/// Recursively inline `#/$defs/*` references so weaker models (and providers
/// that reject `$ref`) get flat schemas.
fn inline_defs(schema: &serde_json::Value) -> serde_json::Value {
    let defs = schema
        .get("$defs")
        .and_then(|d| d.as_object())
        .cloned()
        .unwrap_or_default();
    let mut out = schema.clone();
    if let Some(m) = out.as_object_mut() {
        m.remove("$defs");
    }
    // Depth-bounded to break ref cycles (recursive types stay as $ref).
    substitute_refs(&mut out, &defs, 8);
    out
}

fn substitute_refs(
    v: &mut serde_json::Value,
    defs: &serde_json::Map<String, serde_json::Value>,
    depth: usize,
) {
    if depth == 0 {
        return;
    }
    match v {
        serde_json::Value::Object(m) => {
            if let Some(serde_json::Value::String(r)) = m.get("$ref") {
                if let Some(name) = r.strip_prefix("#/$defs/") {
                    if let Some(def) = defs.get(name) {
                        let mut inlined = def.clone();
                        substitute_refs(&mut inlined, defs, depth - 1);
                        // Preserve sibling keys (e.g. description) over the def's.
                        if let (Some(target), Some(src)) =
                            (inlined.as_object_mut(), Some(&*m))
                        {
                            for (k, val) in src {
                                if k != "$ref" {
                                    target.insert(k.clone(), val.clone());
                                }
                            }
                        }
                        *v = inlined;
                        return;
                    }
                }
            }
            for (_, val) in m.iter_mut() {
                substitute_refs(val, defs, depth);
            }
        }
        serde_json::Value::Array(a) => {
            for x in a {
                substitute_refs(x, defs, depth);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matching() {
        assert!(glob_match("cluster_*", "cluster_list"));
        assert!(glob_match("*_delete", "cluster_delete"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("cluster_list", "cluster_list"));
        assert!(!glob_match("cluster_*", "org_list"));
        assert!(glob_match("*cluster*", "mcp_cluster_list"));
        assert!(!glob_match("*cluster*", "org_list"));
    }

    #[test]
    fn filter_semantics() {
        let f = ToolFilter::default();
        assert!(f.allows("anything"));

        let f = ToolFilter {
            include: vec!["cluster_*".into()],
            exclude: vec!["*_delete".into()],
        };
        assert!(f.allows("cluster_list"));
        assert!(!f.allows("cluster_delete"));
        assert!(!f.allows("org_list"));
    }

    #[test]
    fn inline_defs_flattens_refs() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "role": { "$ref": "#/$defs/Role", "description": "the role" }
            },
            "$defs": {
                "Role": { "type": "string", "enum": ["admin", "user"] }
            }
        });
        let flat = inline_defs(&schema);
        assert!(flat.get("$defs").is_none());
        let role = &flat["properties"]["role"];
        assert_eq!(role["type"], "string");
        assert_eq!(role["enum"][0], "admin");
        // Sibling description preserved over the def.
        assert_eq!(role["description"], "the role");
    }

    #[test]
    fn inline_defs_survives_cycles() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "node": { "$ref": "#/$defs/Node" } },
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": { "child": { "$ref": "#/$defs/Node" } }
                }
            }
        });
        // Must terminate; deep refs stay as $ref at the depth bound.
        let flat = inline_defs(&schema);
        assert!(flat["properties"]["node"]["type"] == "object");
    }

    #[test]
    fn sanitize_rewrites_oneof_and_type_unions() {
        // serde enum → oneOf; Option<String> → type: ["string","null"]
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": {
                "context": {
                    "oneOf": [
                        { "type": "object", "properties": { "kind": { "type": "string" } } },
                        { "type": "string" }
                    ]
                },
                "label": { "type": ["string", "null"] }
            }
        });
        sanitize_for_strict(&mut schema);
        let ctx = &schema["properties"]["context"];
        assert!(ctx.get("oneOf").is_none());
        assert_eq!(ctx["anyOf"].as_array().unwrap().len(), 2);
        let label = &schema["properties"]["label"];
        assert!(label.get("type").is_none());
        assert_eq!(label["anyOf"][0]["type"], "string");
        assert_eq!(label["anyOf"][1]["type"], "null");
    }

    #[test]
    fn sanitize_adds_missing_array_items() {
        // schemars for serde_json::Value emits bare {"type":"array"} variants.
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": {
                "source_config": {
                    "anyOf": [
                        { "type": "object" },
                        { "type": "array" },
                        { "type": ["array", "null"] }
                    ]
                }
            }
        });
        sanitize_for_strict(&mut schema);
        let variants = schema["properties"]["source_config"]["anyOf"]
            .as_array()
            .unwrap();
        assert_eq!(variants[1]["items"], serde_json::json!({}));
        // The type-union variant was rewritten into nested anyOf; its array
        // member must also carry items.
        let nested = variants[2]["anyOf"].as_array().unwrap();
        assert_eq!(nested[0]["type"], "array");
        assert_eq!(nested[0]["items"], serde_json::json!({}));
    }

    #[test]
    fn cap_output_truncates_on_char_boundary() {
        let s = "aä".repeat(100);
        let capped = cap_output(s.clone(), 15);
        assert!(capped.contains("[truncated"));
        // Should not panic and be valid UTF-8 (guaranteed by String type).
        assert!(cap_output("short".into(), 100) == "short");
    }
}
