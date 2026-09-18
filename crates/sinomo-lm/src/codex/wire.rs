use super::*;

pub(super) const RESPONSES_LITE_HEADER: &str = "x-openai-internal-codex-responses-lite";

pub(super) fn tool_spec(tool: &CodexToolDescriptor) -> serde_json::Value {
    if let Some(custom) = &tool.custom {
        serde_json::json!({
            "type": "custom", "name": tool.name, "description": tool.description,
            "format": custom.format,
        })
    } else {
        serde_json::json!({
            "type": "function", "name": tool.name, "description": tool.description,
            "parameters": tool.parameters, "strict": false,
        })
    }
}

pub(super) fn custom_tool<'a>(
    name: &str,
    tools: &'a [LanguageModelToolSpec],
) -> Option<&'a crate::LanguageModelCustomToolSpec> {
    tools
        .iter()
        .find(|tool| tool.name == name)?
        .provider_specs
        .iter()
        .find(|spec| spec.provider.eq_ignore_ascii_case(CODEX_PROVIDER_ID))?
        .custom
        .as_ref()
}

pub(super) fn custom_input(
    input: &serde_json::Value,
    custom: &crate::LanguageModelCustomToolSpec,
) -> String {
    let value = custom
        .raw_input_argument
        .as_deref()
        .and_then(|key| input.get(key))
        .unwrap_or(input);
    codex_tool_value_text(value)
}

/// Convert the ordinary Responses envelope to the catalog-selected wire shape.
/// Sinomo continues to own tools, prompts, permissions and session orchestration.
pub(super) fn apply_model_profile(
    payload: &mut serde_json::Value,
    info: Option<&CodexModelInfo>,
) -> Result<(), LanguageModelError> {
    let Some(info) = info else { return Ok(()) };
    for key in ["reasoning", "text"] {
        if payload
            .get(key)
            .is_some_and(|value| !value.is_object() && !value.is_null())
        {
            return Err(codex_provider_invocation_error(
                &info.slug,
                format!("Codex `{key}` option must be an object"),
            ));
        }
    }
    if !payload["input"].is_array()
        || !payload["instructions"].is_string()
        || payload.get("tools").is_some_and(|value| !value.is_array())
    {
        return Err(codex_provider_invocation_error(
            &info.slug,
            "Codex input/tools must be arrays and instructions must be text".to_string(),
        ));
    }
    if payload.pointer("/reasoning/effort").is_none()
        && let Some(effort) = &info.default_reasoning_level
    {
        payload["reasoning"]["effort"] = serde_json::json!(effort);
    }
    // Ultra is an orchestration selection in Codex, not a backend effort.
    // Mirror ModelInfo::resolve_reasoning_effort; Sinomo owns delegation.
    if payload
        .pointer("/reasoning/effort")
        .and_then(serde_json::Value::as_str)
        == Some("ultra")
        && info
            .supported_reasoning_levels
            .iter()
            .any(|level| level.effort == "ultra")
    {
        let effort = info
            .multi_agent_reasoning_effort
            .as_deref()
            .filter(|effort| {
                *effort != "ultra"
                    && info
                        .supported_reasoning_levels
                        .iter()
                        .any(|level| level.effort == *effort)
            })
            .or_else(|| {
                info.supported_reasoning_levels
                    .iter()
                    .find(|level| level.effort == "max")
                    .map(|level| level.effort.as_str())
            })
            .or_else(|| {
                info.supported_reasoning_levels
                    .iter()
                    .rev()
                    .find(|level| level.effort != "ultra")
                    .map(|level| level.effort.as_str())
            })
            .unwrap_or("medium");
        payload["reasoning"]["effort"] = serde_json::json!(effort);
    }
    if info.support_verbosity
        && payload.pointer("/text/verbosity").is_none()
        && let Some(verbosity) = &info.default_verbosity
    {
        payload["text"]["verbosity"] = serde_json::json!(verbosity);
    }
    if let Some(reasoning) = payload
        .get_mut("reasoning")
        .and_then(serde_json::Value::as_object_mut)
        && (reasoning.get("summary").and_then(serde_json::Value::as_str) == Some("none")
            || !info.supports_reasoning_summary_parameter)
    {
        reasoning.remove("summary");
    }
    if payload.get("service_tier").is_none()
        && let Some(tier) = &info.default_service_tier
    {
        payload["service_tier"] = serde_json::json!(tier);
    }
    match payload
        .get("service_tier")
        .and_then(serde_json::Value::as_str)
    {
        Some("fast") => payload["service_tier"] = serde_json::json!("priority"),
        Some("default") => {
            payload.as_object_mut().unwrap().remove("service_tier");
        }
        _ => {}
    }
    if !info.supports_parallel_tool_calls {
        payload["parallel_tool_calls"] = serde_json::json!(false);
    }
    if !info.use_responses_lite {
        return Ok(());
    }

    let object = payload
        .as_object_mut()
        .expect("Responses request is an object");
    let tools = object
        .remove("tools")
        .unwrap_or_else(|| serde_json::json!([]));
    let tools = if tools.as_array().is_none_or(Vec::is_empty) {
        serde_json::json!([])
    } else {
        serde_json::json!([{"type": "namespace", "name": "functions", "description": "", "tools": tools}])
    };
    let instructions = object.remove("instructions").unwrap_or_default();
    let input = object
        .get_mut("input")
        .and_then(serde_json::Value::as_array_mut)
        .expect("Codex input is an array");
    // Tool names retain their Sinomo identity inside the default namespace.
    for item in input.iter_mut() {
        if item.get("type").is_none() && matches!(item["role"].as_str(), Some("user" | "assistant"))
        {
            item["type"] = serde_json::json!("message");
            if let Some(text) = item["content"].as_str() {
                item["content"] = serde_json::json!([{
                    "type": if item["role"] == "assistant" { "output_text" } else { "input_text" },
                    "text": text,
                }]);
            }
        }
        if matches!(
            item["type"].as_str(),
            Some("function_call" | "custom_tool_call")
        ) && item.get("namespace").is_none()
        {
            item["namespace"] = serde_json::json!("functions");
        }
    }
    let mut prefix = vec![serde_json::json!({
        "id": prompt_item_id("at", &tools.to_string()),
        "type": "additional_tools", "role": "developer", "tools": tools,
    })];
    if let Some(instructions) = instructions.as_str().filter(|text| !text.is_empty()) {
        prefix.push(serde_json::json!({
            "id": prompt_item_id("msg", instructions),
            "type": "message", "role": "developer",
            "content": [{"type": "input_text", "text": instructions}],
        }));
    }
    input.splice(0..0, prefix);
    payload["reasoning"]["context"] = serde_json::json!("all_turns");
    payload["parallel_tool_calls"] = serde_json::json!(false);
    if payload["tool_choice"].is_object() {
        payload["tool_choice"]["namespace"] = serde_json::json!("functions");
    }
    Ok(())
}

fn prompt_item_id(prefix: &str, text: &str) -> String {
    use std::fmt::Write;
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    let mut id = format!("{prefix}_");
    for byte in &digest.as_ref()[..16] {
        write!(&mut id, "{byte:02x}").unwrap();
    }
    id
}

pub(super) fn validate_model_options(
    payload: &serde_json::Value,
    info: Option<&CodexModelInfo>,
) -> Result<(), LanguageModelError> {
    let Some(info) = info else { return Ok(()) };
    if let Some(effort) = payload
        .pointer("/reasoning/effort")
        .and_then(serde_json::Value::as_str)
        && !info.supported_reasoning_levels.is_empty()
        && !info
            .supported_reasoning_levels
            .iter()
            .any(|level| level.effort == effort)
    {
        return Err(codex_provider_invocation_error(
            &info.slug,
            format!(
                "reasoning effort `{effort}` is not supported by {}",
                info.slug
            ),
        ));
    }
    if let Some(tier) = payload
        .get("service_tier")
        .and_then(serde_json::Value::as_str)
        && tier != "flex"
        && !info.service_tiers.iter().any(|entry| entry.id == tier)
    {
        return Err(codex_provider_invocation_error(
            &info.slug,
            format!("service tier `{tier}` is not supported by {}", info.slug),
        ));
    }
    Ok(())
}
