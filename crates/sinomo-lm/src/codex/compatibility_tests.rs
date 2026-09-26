use super::*;
use crate::ProviderReasoningEffort;

fn request(model: &str) -> CodexModelRequest {
    CodexModelRequest {
        model: model.to_string(),
        messages: vec![
            CodexMessage::system("Use Sinomo tools."),
            CodexMessage::user("hello"),
        ],
        tools: Vec::new(),
        temperature: None,
        max_output_tokens: None,
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::Value::Null,
        options: LanguageModelProviderOptions::default(),
        tool_choice: None,
        parallel_tool_calls: Some(true),
    }
}

#[test]
fn new_models_use_responses_lite() {
    let payload = openai_responses_codex_payload(&request("gpt-6-astra"));
    assert!(payload.get("instructions").is_none());
    assert!(payload.get("tools").is_none());
    assert_eq!(payload["input"][0]["type"], "additional_tools");
    assert_eq!(payload["input"][1]["role"], "developer");
    assert_eq!(
        payload["input"][2],
        serde_json::json!({"type":"message", "role":"user", "content":[{"type":"input_text", "text":"hello"}]})
    );
    assert_eq!(payload["reasoning"]["context"], "all_turns");
    assert_eq!(payload["parallel_tool_calls"], false);
}

#[test]
fn codex_max_effort_is_not_downgraded() {
    let mut request = request("gpt-5.6-sol");
    request.options.reasoning_effort = Some(ProviderReasoningEffort::Max);
    assert_eq!(
        openai_responses_codex_payload(&request)["reasoning"]["effort"],
        "max"
    );
}

#[test]
fn freeform_calls_reach_the_tool_runtime() {
    let item = serde_json::json!({
        "type": "custom_tool_call", "call_id": "call_patch", "namespace": "functions",
        "name": "apply_patch", "input": "*** Begin Patch\n*** End Patch"
    });
    let response = parse_openai_responses_codex_response(&serde_json::json!({"output": [item]}));
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].name, "apply_patch");
    assert_eq!(
        response.tool_calls[0].input,
        "*** Begin Patch\n*** End Patch"
    );
    assert_eq!(response.continuation_items, vec![item]);
}

#[test]
fn codex_context_limits_follow_its_catalog() {
    assert_eq!(
        crate::inferred_model_context_window_tokens("codex", "gpt-6-astra"),
        Some(272_000)
    );
    assert_eq!(
        crate::inferred_model_max_context_window_tokens("codex", "gpt-5.6-sol"),
        Some(872_000)
    );
}

fn patch_tool() -> LanguageModelToolSpec {
    LanguageModelToolSpec {
        name: "apply_patch".to_string(),
        description: "Apply a patch".to_string(),
        input_schema: serde_json::json!({"type":"object", "properties":{"patch":{"type":"string"}}}),
        output_schema: serde_json::Value::Null,
        supports_parallel: false,
        provider_specs: vec![crate::LanguageModelProviderToolSpec {
            provider: "codex".to_string(),
            custom: Some(crate::LanguageModelCustomToolSpec {
                format: crate::LanguageModelCustomToolFormat::Grammar {
                    syntax: "lark".to_string(),
                    definition: "start: /.+/".to_string(),
                },
                raw_input_argument: Some("patch".to_string()),
            }),
        }],
    }
}

#[test]
fn catalog_exposes_models_efforts_and_tiers_without_claiming_account_access() {
    assert_eq!(
        crate::provider_known_model_ids("codex"),
        vec![
            "gpt-6-astra",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5"
        ]
    );
    let sol = builtin_model_info("gpt-5.6-sol").unwrap();
    assert!(sol.service_tiers.iter().any(|tier| tier.id == "ultrafast"));
    let luna = builtin_model_info("gpt-5.6-luna").unwrap();
    assert!(
        !luna
            .supported_reasoning_levels
            .iter()
            .any(|level| level.effort == "ultra")
    );
    // A server's new fields/names must not break catalog discovery.
    let remote = models::extract_model_catalog(&serde_json::json!({"models": [
        {"slug":"future-model", "supported_reasoning_levels":[{"effort":"future-effort"}], "priority":2},
        {"slug":"gpt-6-astra", "priority":1, "default_reasoning_level":"high"},
        {"slug":"hidden-model", "priority":0, "visibility":"hide"}
    ]}));
    assert_eq!(remote.len(), 2);
    assert_eq!(remote[0].default_reasoning_level.as_deref(), Some("high"));
    assert!(remote[0].use_responses_lite);
    assert_eq!(
        remote[1].supported_reasoning_levels[0].effort,
        "future-effort"
    );
}

#[test]
fn reasoning_and_tier_options_are_preserved_and_checked_against_the_model() {
    for (model, effort, tier, valid) in [
        (
            "gpt-6-astra",
            ProviderReasoningEffort::Ultra,
            "priority",
            true,
        ),
        (
            "gpt-5.6-sol",
            ProviderReasoningEffort::Max,
            "ultrafast",
            true,
        ),
        (
            "gpt-5.6-terra",
            ProviderReasoningEffort::Ultra,
            "fast",
            true,
        ),
        (
            "gpt-5.6-luna",
            ProviderReasoningEffort::Ultra,
            "priority",
            false,
        ),
        (
            "gpt-6-astra",
            ProviderReasoningEffort::Max,
            "ultrafast",
            false,
        ),
    ] {
        let mut request = request(model);
        request.options.reasoning_effort = Some(effort);
        request
            .options
            .vendor_options
            .insert("service_tier".to_string(), tier.to_string());
        let payload = openai_responses_codex_payload(&request);
        let resolved_effort = if effort == ProviderReasoningEffort::Ultra && model != "gpt-5.6-luna"
        {
            "max"
        } else {
            effort.as_config_value()
        };
        assert_eq!(payload["reasoning"]["effort"], resolved_effort);
        assert_eq!(
            wire::validate_model_options(&payload, builtin_model_info(model)).is_ok(),
            valid
        );
        assert_eq!(
            payload["service_tier"],
            if tier == "fast" { "priority" } else { tier }
        );
    }
    let mut request = request("gpt-6-astra");
    request
        .options
        .vendor_options
        .insert("service_tier".to_string(), "default".to_string());
    assert!(
        openai_responses_codex_payload(&request)
            .get("service_tier")
            .is_none()
    );
    assert_eq!(
        "ultra".parse::<ProviderReasoningEffort>().unwrap(),
        ProviderReasoningEffort::Ultra
    );
    assert_eq!(
        serde_json::to_string(&ProviderReasoningEffort::Ultra).unwrap(),
        "\"ultra\""
    );
}

#[test]
fn custom_tools_round_trip_in_both_responses_formats() {
    let tools = vec![patch_tool()];
    for model in [
        "gpt-6-astra",
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "gpt-5.5",
    ] {
        let mut request = request(model);
        request.tools = codex_tool_descriptors(Vec::new(), &tools);
        request.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: "apply_patch".to_string(),
        });
        request.messages.extend(codex_messages_from_request_items(
            &[
                LanguageModelRequestItem::tool_call(
                    "call_patch",
                    "apply_patch",
                    serde_json::json!({"patch":"raw patch"}),
                ),
                LanguageModelRequestItem::tool_call_output(
                    "call_patch",
                    "apply_patch",
                    serde_json::json!({"ok":true}),
                ),
            ],
            &tools,
        ));
        let payload = openai_responses_codex_payload(&request);
        let lite = model != "gpt-5.5";
        let tool = if lite {
            &payload["input"][0]["tools"][0]["tools"][0]
        } else {
            &payload["tools"][0]
        };
        assert_eq!(tool["type"], "custom");
        assert_eq!(tool["format"]["syntax"], "lark");
        let input = payload["input"].as_array().unwrap();
        let call = &input[input.len() - 2];
        assert_eq!(call["type"], "custom_tool_call");
        assert_eq!(call["input"], "raw patch");
        assert_eq!(input.last().unwrap()["type"], "custom_tool_call_output");
        assert_eq!(payload["tool_choice"]["type"], "custom");
        if lite {
            assert_eq!(call["namespace"], "functions");
            assert_eq!(
                payload,
                openai_responses_codex_payload(&request),
                "stable prompt item ids on retry"
            );
        } else {
            assert!(call.get("namespace").is_none());
            assert_eq!(payload["instructions"], "Use Sinomo tools.");
        }
    }
}

#[test]
fn foreign_namespaces_do_not_dispatch_to_an_unqualified_local_tool() {
    let call = parse_function_call_item(&serde_json::json!({
        "type":"function_call", "call_id":"call_1", "namespace":"other", "name":"apply_patch", "arguments":"{}"
    })).unwrap();
    assert_eq!(call.name, "other.apply_patch");
}

#[test]
fn ultra_uses_the_catalog_override_and_falls_back_to_a_supported_effort() {
    let mut request = request("gpt-6-astra");
    request.options.reasoning_effort = Some(ProviderReasoningEffort::Ultra);
    let mut info = builtin_model_info(&request.model).unwrap().clone();
    info.multi_agent_reasoning_effort = Some("high".to_string());
    assert_eq!(
        openai_responses_codex_payload_with_model(&request, Some(&info)).unwrap()["reasoning"]["effort"],
        "high"
    );
    info.multi_agent_reasoning_effort = Some("unsupported".to_string());
    assert_eq!(
        openai_responses_codex_payload_with_model(&request, Some(&info)).unwrap()["reasoning"]["effort"],
        "max"
    );
    info.supported_reasoning_levels
        .retain(|level| level.effort != "max");
    assert_eq!(
        openai_responses_codex_payload_with_model(&request, Some(&info)).unwrap()["reasoning"]["effort"],
        "xhigh"
    );
}

#[test]
fn malformed_vendor_overrides_return_errors_instead_of_panicking() {
    for key in ["reasoning", "text", "input", "tools", "instructions"] {
        let mut request = request("gpt-6-astra");
        request
            .options
            .vendor_options
            .insert(key.to_string(), "42".to_string());
        assert!(
            openai_responses_codex_payload_with_model(&request, builtin_model_info(&request.model))
                .is_err(),
            "{key}"
        );
    }
}

#[test]
fn model_discovery_and_lite_custom_tool_continuation_work_over_http() {
    use super::tests::{accept_expected, read_http_request_parts, test_server, transport_client};
    use std::io::Write;
    let (base_url, server) = test_server(|listener| {
        let mut stream = accept_expected(&listener, "model discovery");
        let (headers, _) = read_http_request_parts(&mut stream);
        assert!(headers.starts_with("GET /models?client_version=0.153.0 "));
        let catalog = serde_json::json!({"models":[{"slug":"gpt-6-astra", "visibility":"list", "priority":1}]}).to_string();
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{catalog}", catalog.len()).unwrap();
        for turn in 0..2 {
            let mut stream = accept_expected(&listener, "Responses Lite turn");
            let (headers, body) = read_http_request_parts(&mut stream);
            assert!(headers.starts_with("POST /responses "));
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("x-openai-internal-codex-responses-lite: true")
            );
            let payload: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                payload["input"][0]["tools"][0]["tools"][0]["type"],
                "custom"
            );
            assert_eq!(payload["reasoning"]["effort"], "max");
            assert_eq!(payload["service_tier"], "priority");
            let item = if turn == 0 {
                serde_json::json!({"type":"custom_tool_call", "call_id":"call_patch", "namespace":"functions", "name":"apply_patch", "input":"raw patch"})
            } else {
                let input = payload["input"].as_array().unwrap();
                assert_eq!(input[input.len() - 2]["input"], "raw patch");
                assert_eq!(input.last().unwrap()["type"], "custom_tool_call_output");
                serde_json::json!({"type":"message", "role":"assistant", "content":[{"type":"output_text", "text":"done"}]})
            };
            let body = format!(
                "data: {}\n\ndata: {}\n\n",
                serde_json::json!({"type":"response.output_item.done", "item":item}),
                serde_json::json!({"type":"response.completed", "response":{"status":"completed", "output":[]}})
            );
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });
    let dir = sinomo_testdir::TestDir::new("codex-lite-compatibility");
    let client = transport_client(&base_url, Duration::from_secs(5), &dir);
    assert_eq!(client.list_models().unwrap(), vec!["gpt-6-astra"]);
    let provider = CodexProvider::new(
        CodexConfig {
            options: LanguageModelProviderOptions {
                reasoning_effort: Some(ProviderReasoningEffort::Ultra),
                vendor_options: BTreeMap::from([(
                    "service_tier".to_string(),
                    "priority".to_string(),
                )]),
                ..Default::default()
            },
            ..Default::default()
        },
        client,
        CodexToolRegistry::new(),
        DefaultCodexPromptBuilder::default(),
        DefaultCodexContextManager::default(),
    );
    let mut request = LanguageModelRequest {
        provider: "codex".to_string(),
        model: "gpt-6-astra".to_string(),
        messages: vec![LanguageModelMessage {
            role: LanguageModelRole::User,
            content: "patch a file".to_string(),
        }],
        tools: vec![patch_tool()],
        items: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: Some(true),
        temperature: None,
        max_output_tokens: None,
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::Value::Null,
    };
    let mut events = Vec::new();
    let response = provider
        .complete_streaming_events(&request, &mut |event| events.push(event))
        .unwrap();
    assert_eq!(response.tool_calls.len(), 1);
    assert!(events.iter().any(|event| matches!(event, LanguageModelStreamEvent::ToolCallCompleted { name, .. } if name == "apply_patch")));
    let normalized = crate::collect_language_model_tool_calls(
        "codex",
        "gpt-6-astra",
        &response,
        &events,
        &request.tools,
    );
    assert_eq!(
        normalized.tool_calls[0].input,
        serde_json::json!({"patch":"raw patch"})
    );
    request.items = vec![
        LanguageModelRequestItem::message(LanguageModelRole::User, "patch a file"),
        LanguageModelRequestItem::tool_call(
            "call_patch",
            "apply_patch",
            normalized.tool_calls[0].input.clone(),
        ),
        LanguageModelRequestItem::tool_call_output(
            "call_patch",
            "apply_patch",
            serde_json::json!({"ok":true}),
        ),
    ];
    assert_eq!(provider.complete(&request).unwrap().content, "done");
    server.join().unwrap();
}

#[test]
fn qualified_custom_tools_survive_both_responses_envelopes() {
    let mut tool = patch_tool();
    tool.name = "workspace.apply_patch".into();
    let descriptors = codex_tool_descriptors(Vec::new(), std::slice::from_ref(&tool));
    let item = LanguageModelRequestItem::ToolCall {
        call_id: "patch_1".into(),
        name: tool.name.clone(),
        input: serde_json::json!({"patch":"*** Begin Patch\n*** End Patch"}),
    };
    for model in ["gpt-5.5", "gpt-6-astra"] {
        let mut req = request(model);
        req.tools = descriptors.clone();
        req.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: tool.name.clone(),
        });
        req.messages.extend(codex_messages_from_request_items(
            std::slice::from_ref(&item),
            std::slice::from_ref(&tool),
        ));
        let payload = openai_responses_codex_payload(&req);
        let tools = if model == "gpt-6-astra" {
            &payload["input"][0]["tools"]
        } else {
            &payload["tools"]
        };
        assert_eq!(tools[0]["type"], "namespace");
        assert_eq!(tools[0]["name"], "workspace");
        assert_eq!(tools[0]["tools"][0]["type"], "custom");
        assert_eq!(tools[0]["tools"][0]["name"], "apply_patch");
        assert_eq!(
            payload["tool_choice"],
            serde_json::json!({"type":"custom", "namespace":"workspace", "name":"apply_patch"})
        );
        let call = payload["input"].as_array().unwrap().last().unwrap();
        assert_eq!(call["namespace"], "workspace");
        assert_eq!(call["name"], "apply_patch");
        assert_eq!(parse_function_call_item(call).unwrap().name, tool.name);
        let output = LanguageModelRequestItem::ToolCallOutput {
            call_id: "patch_1".into(),
            name: tool.name.clone(),
            output: serde_json::json!("done"),
        };
        let messages = codex_messages_from_request_items(&[output], std::slice::from_ref(&tool));
        let output = messages[0].raw_item.as_ref().unwrap();
        assert_eq!(output["name"], "apply_patch");
        assert!(output.get("namespace").is_none());
    }
}

#[test]
fn lite_mixes_default_and_explicit_namespaces_without_nesting() {
    let tools = vec![
        serde_json::json!({"type":"function", "name":"read_file", "parameters":{}, "strict":false}),
        serde_json::json!({"type":"namespace", "name":"workspace", "description":"", "tools":[{"type":"custom", "name":"patch", "format":{"type":"text"}}]}),
        serde_json::json!({"type":"function", "name":"tool_search", "parameters":{}, "strict":false}),
    ];
    let mut payload = serde_json::json!({"instructions":"Use tools", "input":[], "tools":tools});
    wire::apply_model_profile(&mut payload, builtin_model_info("gpt-6-astra")).unwrap();
    let tools = payload["input"][0]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0]["name"], "functions");
    assert_eq!(tools[0]["tools"].as_array().unwrap().len(), 2);
    assert_eq!(tools[1]["name"], "workspace");
    assert_eq!(tools[1]["tools"][0]["type"], "custom");
}
