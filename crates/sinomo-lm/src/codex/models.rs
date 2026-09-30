use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// Provider capabilities from the Codex model catalog. Unknown fields and future
/// reasoning/tier names are accepted so discovery does not require a new binary.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct CodexModelInfo {
    pub slug: String,
    pub display_name: String,
    pub visibility: String,
    pub priority: i64,
    pub minimal_client_version: Option<String>,
    pub context_window: Option<usize>,
    pub max_context_window: Option<usize>,
    pub default_reasoning_level: Option<String>,
    pub multi_agent_reasoning_effort: Option<String>,
    pub supported_reasoning_levels: Vec<CodexReasoningLevel>,
    pub service_tiers: Vec<CodexServiceTier>,
    pub default_service_tier: Option<String>,
    pub support_verbosity: bool,
    pub default_verbosity: Option<String>,
    pub use_responses_lite: bool,
    pub supports_parallel_tool_calls: bool,
    pub supports_reasoning_summary_parameter: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CodexReasoningLevel {
    pub effort: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CodexServiceTier {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
}

/// Protocol defaults, not evidence that a signed-in account can access a model.
/// Derived from codex-rs/models-manager/models.json at 2e5fea64e.
pub fn codex_builtin_model_catalog() -> &'static [CodexModelInfo] {
    static MODELS: OnceLock<Vec<CodexModelInfo>> = OnceLock::new();
    MODELS.get_or_init(|| {
        #[derive(Deserialize)]
        struct Catalog {
            models: Vec<CodexModelInfo>,
        }
        serde_json::from_str::<Catalog>(include_str!("models.json"))
            .expect("bundled Codex model metadata is valid")
            .models
    })
}

pub(super) fn builtin_model_info(model: &str) -> Option<&'static CodexModelInfo> {
    codex_builtin_model_catalog()
        .iter()
        .find(|info| info.slug.eq_ignore_ascii_case(model))
}

pub(super) fn extract_model_catalog(value: &serde_json::Value) -> Vec<CodexModelInfo> {
    let Some(entries) = value
        .get("models")
        .or_else(|| value.get("data"))
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array())
    else {
        return Vec::new();
    };
    let mut models: Vec<CodexModelInfo> = entries
        .iter()
        .filter_map(|entry| {
            let (priority, slug) = super::codex_model_list_entry(entry)?;
            let mut value =
                serde_json::to_value(builtin_model_info(&slug).cloned().unwrap_or_default())
                    .ok()?;
            if let Some(object) = entry.as_object() {
                value.as_object_mut()?.extend(object.clone());
            }
            value["slug"] = serde_json::json!(slug);
            value["priority"] = serde_json::json!(priority);
            serde_json::from_value(value).ok()
        })
        .collect();
    models.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.slug.cmp(&b.slug))
    });
    let mut seen = std::collections::BTreeSet::new();
    models.retain(|info| seen.insert(info.slug.clone()));
    models
}
