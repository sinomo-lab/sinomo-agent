use crate::{
    CODEX_DEFAULT_MODEL, CODEX_PROVIDER_ID, DeepSeekProvider, LM_PROVIDER_CONFIG_KEY,
    LanguageModelMessage, LanguageModelRequest, LanguageModelRequestItem,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelToolChoice, LanguageModelToolSpec,
    ModelConfig, ModelPaths, OpenAiProvider, PROVIDER_DEFAULT_MODEL,
    collect_language_model_tool_calls, complete_configured_model_request_with_account,
    model_config_from_cluster_kv_snapshot, model_config_store, normalize_provider_account_name,
    provider_account_config,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};

const ECHO_TOOL_NAME: &str = "echo";
const SMOKE_TEXT: &str = "sinomo-native-tool-smoke";

pub fn register_lm_test_cases(builder: SinomoTestRegistryBuilder) -> SinomoTestRegistryBuilder {
    builder
        .register(deepseek_native_tools())
        .register(openai_native_tools())
        .register(codex_native_tools())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn deepseek_native_tools(ctx) {
        id: "lm.deepseek.native-tools",
        summary: "DeepSeek Chat Completions native tool-call transcript smoke",
        tags: ["lm", "deepseek", "live", "native-tools"],
        requirements: [
            SinomoTestRequirement::ClusterRuntime,
            SinomoTestRequirement::LiveNetwork,
            SinomoTestRequirement::provider_account(DeepSeekProvider::PROVIDER_ID),
        ],
        run: {
            live_native_tool_smoke(ctx, DeepSeekProvider::PROVIDER_ID)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn codex_native_tools(ctx) {
        id: "lm.codex.native-tools",
        summary: "Codex ChatGPT Responses native tool-call transcript smoke",
        tags: ["lm", "codex", "gpt", "live", "native-tools"],
        requirements: [
            SinomoTestRequirement::ClusterRuntime,
            SinomoTestRequirement::LiveNetwork,
        ],
        run: {
            live_native_tool_smoke(ctx, CODEX_PROVIDER_ID)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn openai_native_tools(ctx) {
        id: "lm.openai.native-tools",
        summary: "OpenAI Responses native tool-call transcript smoke",
        tags: ["lm", "openai", "live", "native-tools"],
        requirements: [
            SinomoTestRequirement::ClusterRuntime,
            SinomoTestRequirement::LiveNetwork,
            SinomoTestRequirement::provider_account(OpenAiProvider::PROVIDER_ID),
        ],
        run: {
            live_native_tool_smoke(ctx, OpenAiProvider::PROVIDER_ID)
        }
    }
}

fn live_native_tool_smoke(ctx: &mut SinomoTestContext, provider: &str) -> SinomoTestResult {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let account_name = normalize_provider_account_name(ctx.parameter("account"));
    let (config, config_source) = model_config_for_test(ctx, &data_dir)?;
    let account = if provider == CODEX_PROVIDER_ID {
        provider_account_config(&config, provider, Some(&account_name)).cloned()
    } else {
        Some(
            provider_account_config(&config, provider, Some(&account_name))
                .cloned()
                .ok_or_else(|| {
                    SinomoTestError::failed(format!(
                        "provider account `{provider}:{account_name}` is not configured"
                    ))
                })?,
        )
    };
    let model = ctx
        .parameter("model")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            account
                .as_ref()
                .and_then(|account| account.default_model.clone())
        })
        .unwrap_or_else(|| native_tool_smoke_default_model(provider));
    let tool = echo_tool_spec();
    let first_request = native_tool_call_request(provider, &model, tool.clone());

    ctx.record("provider", provider);
    if account.is_some() {
        ctx.record("account", &account_name);
    }
    ctx.record("model", &model);
    ctx.record("config_source", &config_source);

    let first_response = complete_configured_model_request_with_account(
        &data_dir,
        provider,
        &model,
        account.as_ref(),
        None,
        &first_request,
    )
    .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let tool_output =
        collect_language_model_tool_calls(provider, &model, &first_response, &[], &[tool.clone()]);
    let call = tool_output.tool_calls.first().cloned().ok_or_else(|| {
        SinomoTestError::failed(format!(
            "model did not return a native tool call; response text: {}",
            first_response.content
        ))
    })?;
    if call.name != ECHO_TOOL_NAME {
        return Err(SinomoTestError::failed(format!(
            "expected tool `{ECHO_TOOL_NAME}`, got `{}`",
            call.name
        )));
    }
    let call_id = call.call_id.clone().ok_or_else(|| {
        SinomoTestError::failed("native tool call did not include provider call id")
    })?;
    let echoed_text = call
        .input
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(SMOKE_TEXT)
        .to_string();
    let second_request =
        native_tool_result_request(provider, &model, tool, &call_id, &call, &echoed_text);
    let final_response = complete_configured_model_request_with_account(
        &data_dir,
        provider,
        &model,
        account.as_ref(),
        None,
        &second_request,
    )
    .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let final_text = final_response.content.trim().to_string();
    if final_text.is_empty() || !final_text.contains(SMOKE_TEXT) {
        return Err(SinomoTestError::failed(format!(
            "final response did not confirm echoed text `{SMOKE_TEXT}`: {final_text}"
        )));
    }

    Ok(
        SinomoTestOutcome::passed("native tool protocol smoke passed").with_diagnostics(
            serde_json::json!({
                "tool_call_id": call_id,
                "tool_input": call.input,
                "tool_output": { "text": echoed_text },
                "final_text": final_text,
                "first_usage": first_response.usage,
                "final_usage": final_response.usage,
            }),
        ),
    )
}

fn model_config_for_test(
    ctx: &SinomoTestContext,
    data_dir: &SinomoDataDir,
) -> Result<(ModelConfig, String), SinomoTestError> {
    if let Some(snapshot) = ctx.cluster_snapshot() {
        let config = model_config_from_cluster_kv_snapshot(snapshot)
            .map_err(|source| SinomoTestError::failed(source.to_string()))?
            .unwrap_or_default();
        let source = ctx
            .cluster_config_source()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "cluster-kv".to_string());
        return Ok((config, source));
    }
    if ctx.require_cluster() {
        return Err(SinomoTestError::ClusterRequired {
            detail: "no cluster KV snapshot was provided to the test context".to_string(),
        });
    }
    let config = model_config_store()
        .and_then(|store| {
            store
                .get_typed(data_dir, None, LM_PROVIDER_CONFIG_KEY)
                .map_err(|source| crate::LanguageModelError::ConfigStore {
                    path: LM_PROVIDER_CONFIG_KEY.to_string(),
                    detail: source.to_string(),
                })
        })
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    Ok((
        config,
        ModelPaths::new(data_dir).config.display().to_string(),
    ))
}

fn native_tool_smoke_default_model(provider: &str) -> String {
    match provider {
        CODEX_PROVIDER_ID => CODEX_DEFAULT_MODEL.to_string(),
        _ => PROVIDER_DEFAULT_MODEL.to_string(),
    }
}

fn echo_tool_spec() -> LanguageModelToolSpec {
    LanguageModelToolSpec {
        name: ECHO_TOOL_NAME.to_string(),
        description: "Return the provided text unchanged.".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "text": { "type": "string" }
            },
            "required": ["text"],
            "additionalProperties": false
        }),
        output_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "text": { "type": "string" }
            },
            "required": ["text"],
            "additionalProperties": false
        }),
        supports_parallel: false,
        provider_specs: Vec::new(),
    }
}

fn native_tool_call_request(
    provider: &str,
    model: &str,
    tool: LanguageModelToolSpec,
) -> LanguageModelRequest {
    LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content:
                    "You are running a Sinomo live smoke test. Use the requested tool exactly once."
                        .to_string(),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: format!(
                    "Call the `{ECHO_TOOL_NAME}` tool with text `{SMOKE_TEXT}`. Do not answer directly."
                ),
            },
        ],
        items: Vec::new(),
        tools: vec![tool],
        tool_choice: Some(LanguageModelToolChoice::Tool {
            name: ECHO_TOOL_NAME.to_string(),
        }),
        parallel_tool_calls: Some(false),
        temperature: Some(0.0),
        max_output_tokens: Some(256),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::json!({
            "sinomo_test": "native_tools_first_call",
        }),
    }
}

fn native_tool_result_request(
    provider: &str,
    model: &str,
    tool: LanguageModelToolSpec,
    call_id: &str,
    call: &crate::LanguageModelToolCall,
    echoed_text: &str,
) -> LanguageModelRequest {
    LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![LanguageModelMessage {
            role: LanguageModelRole::System,
            content: "You are running a Sinomo live smoke test. After the tool result, answer with a short confirmation that includes the echoed text.".to_string(),
        }],
        items: vec![
            LanguageModelRequestItem::message(
                LanguageModelRole::User,
                format!("Call `{ECHO_TOOL_NAME}` with `{SMOKE_TEXT}`, then confirm the result."),
            ),
            LanguageModelRequestItem::tool_call(call_id, call.name.clone(), call.input.clone()),
            LanguageModelRequestItem::tool_call_output(
                call_id,
                call.name.clone(),
                serde_json::json!({ "text": echoed_text }),
            ),
        ],
        tools: vec![tool],
        tool_choice: Some(LanguageModelToolChoice::None),
        parallel_tool_calls: Some(false),
        temperature: Some(0.0),
        max_output_tokens: Some(256),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::json!({
            "sinomo_test": "native_tools_result_replay",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lm_test_registry_exports_native_tool_smokes() {
        let registry = register_lm_test_cases(SinomoTestRegistryBuilder::new())
            .try_build()
            .expect("registry");
        let ids = registry
            .metadata()
            .into_iter()
            .map(|case| case.id)
            .collect::<Vec<_>>();

        assert!(ids.contains(&"lm.deepseek.native-tools".to_string()));
        assert!(ids.contains(&"lm.codex.native-tools".to_string()));
        assert!(ids.contains(&"lm.openai.native-tools".to_string()));
    }

    #[test]
    fn native_tool_result_request_preserves_call_id_in_items() {
        let call = crate::LanguageModelToolCall {
            call_id: Some("call_echo".to_string()),
            name: ECHO_TOOL_NAME.to_string(),
            input: serde_json::json!({ "text": SMOKE_TEXT }),
            summary: None,
        };

        let request = native_tool_result_request(
            DeepSeekProvider::PROVIDER_ID,
            "deepseek-v4-pro",
            echo_tool_spec(),
            "call_echo",
            &call,
            SMOKE_TEXT,
        );

        assert!(request.items.iter().any(|item| matches!(
            item,
            LanguageModelRequestItem::ToolCall { call_id, name, .. }
                if call_id == "call_echo" && name == ECHO_TOOL_NAME
        )));
        assert!(request.items.iter().any(|item| matches!(
            item,
            LanguageModelRequestItem::ToolCallOutput { call_id, name, .. }
                if call_id == "call_echo" && name == ECHO_TOOL_NAME
        )));
    }
}
