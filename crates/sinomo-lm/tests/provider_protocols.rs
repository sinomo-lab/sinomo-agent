use serde_json::{Value, json};
use sinomo_lm::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

fn server(body: Value) -> (String, thread::JoinHandle<Vec<(String, Value)>>) {
    sequence_server(vec![(200, "application/json", body.to_string())])
}

fn sequence_server(
    responses: Vec<(u16, &'static str, String)>,
) -> (String, thread::JoinHandle<Vec<(String, Value)>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let worker = thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, content_type, body) in responses {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "provider did not connect");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut data = Vec::new();
            let mut buffer = [0; 4096];
            let (headers, end, length) = loop {
                let n = stream.read(&mut buffer).unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buffer[..n]);
                if let Some(end) = data.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8(data[..end].to_vec()).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (headers, end + 4, length);
                }
            };
            while data.len() < end + length {
                let n = stream.read(&mut buffer).unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buffer[..n]);
            }
            let request = if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&data[end..end + length]).unwrap()
            };
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            requests.push((headers, request));
        }
        requests
    });
    (url, worker)
}

fn request(provider: &str, model: &str) -> LanguageModelRequest {
    LanguageModelRequest {
        provider: provider.into(),
        model: model.into(),
        messages: vec![LanguageModelMessage {
            role: LanguageModelRole::User,
            content: "Read a file".into(),
        }],
        items: Vec::new(),
        tools: vec![LanguageModelToolSpec {
            name: "read_file".into(),
            description: "Read a file".into(),
            input_schema: json!({"type":"object", "properties":{"path":{"type":"string"}}, "required":["path"]}),
            output_schema: Value::Null,
            supports_parallel: false,
            provider_specs: Vec::new(),
        }],
        tool_choice: Some(LanguageModelToolChoice::Auto),
        parallel_tool_calls: Some(false),
        temperature: None,
        max_output_tokens: Some(1024),
        response_format: LanguageModelResponseFormat::Text,
        metadata: Value::Null,
    }
}

#[test]
fn anthropic_exposes_native_tool_requests_and_results() {
    let (url, server) = server(
        json!({"content":[{"type":"tool_use", "id":"call_1", "name":"read_file", "input":{"path":"a.txt"}}]}),
    );
    let provider = AnthropicProvider::new(AnthropicConfig {
        api_base_url: url,
        api_key: Some("test-key".into()),
        ..Default::default()
    });
    let result = provider
        .complete(&request("anthropic", "claude-sonnet-5"))
        .unwrap();
    let (_, body) = server.join().unwrap().remove(0);
    assert_eq!(body["tools"][0]["name"], "read_file");
    assert_eq!(result.tool_calls.len(), 1);
    assert_eq!(result.tool_calls[0].input["path"], "a.txt");
}

#[test]
fn gemini_exposes_native_function_requests_and_results() {
    let (url, server) = server(
        json!({"candidates":[{"content":{"role":"model", "parts":[{"functionCall":{"id":"call_1", "name":"read_file", "args":{"path":"a.txt"}}, "thoughtSignature":"opaque-signature"}]}}]}),
    );
    let provider = GeminiProvider::new(GeminiConfig {
        api_base_url: url,
        api_key: Some("test-key".into()),
        ..Default::default()
    });
    let result = provider
        .complete(&request("gemini", "gemini-3.8-flash"))
        .unwrap();
    let (_, body) = server.join().unwrap().remove(0);
    assert_eq!(
        body["tools"][0]["functionDeclarations"][0]["name"],
        "read_file"
    );
    assert_eq!(result.tool_calls.len(), 1);
}

#[test]
fn nvidia_exposes_native_tool_requests_and_results() {
    let (url, server) = server(
        json!({"choices":[{"message":{"role":"assistant", "content":null, "tool_calls":[{"id":"call_1", "type":"function", "function":{"name":"read_file", "arguments":"{\"path\":\"a.txt\"}"}}]}}]}),
    );
    let provider = NvidiaProvider::new(NvidiaConfig {
        api_base_url: url,
        api_key: Some("test-key".into()),
        ..Default::default()
    });
    let result = provider
        .complete(&request("nvidia", NVIDIA_DEFAULT_MODEL))
        .unwrap();
    let (_, body) = server.join().unwrap().remove(0);
    assert_eq!(body["tools"][0]["function"]["name"], "read_file");
    assert_eq!(result.tool_calls.len(), 1);
}

fn invoke(
    base: &str,
    request: &LanguageModelRequest,
    options: LanguageModelProviderOptions,
    streaming: bool,
) -> (LanguageModelResponse, Vec<LanguageModelStreamEvent>) {
    invoke_result(base, request, options, streaming).unwrap()
}

fn invoke_result(
    base: &str,
    request: &LanguageModelRequest,
    options: LanguageModelProviderOptions,
    streaming: bool,
) -> Result<(LanguageModelResponse, Vec<LanguageModelStreamEvent>), LanguageModelError> {
    let dir = sinomo_testdir::TestDir::new("provider-wire-test");
    let provider: Box<dyn LanguageModelProvider> = match request.provider.as_str() {
        "anthropic" => Box::new(AnthropicProvider::new(AnthropicConfig {
            api_base_url: base.into(),
            api_key: Some("test-key".into()),
            options,
            ..Default::default()
        })),
        "gemini" => Box::new(GeminiProvider::new(GeminiConfig {
            api_base_url: base.into(),
            api_key: Some("test-key".into()),
            options,
            ..Default::default()
        })),
        "nvidia" => Box::new(NvidiaProvider::new(NvidiaConfig {
            api_base_url: base.into(),
            api_key: Some("test-key".into()),
            options,
            ..Default::default()
        })),
        "deepseek" => Box::new(DeepSeekProvider::new(DeepSeekConfig {
            api_base_url: base.into(),
            api_key: Some("test-key".into()),
            options,
            ..Default::default()
        })),
        "openai" => Box::new(OpenAiProvider::new(OpenAiConfig {
            api_base_url: base.into(),
            api_key: Some("test-key".into()),
            options,
            ..Default::default()
        })),
        "claude" => {
            let provider = ClaudeProvider::new(ClaudeConfig {
                api_base_url: base.into(),
                credentials_path: Some(dir.join("claude.json")),
                options,
                ..Default::default()
            });
            provider
                .save_oauth_tokens(ClaudeOAuthTokens {
                    access_token: "fixture-token".into(),
                    refresh_token: String::new(),
                    expires_at_ms: None,
                    scopes: Vec::new(),
                })
                .unwrap();
            Box::new(provider)
        }
        "codex" => {
            let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
                api_base_url: base.into(),
                credentials_path: Some(dir.join("codex.json")),
                ..Default::default()
            });
            client
                .save_chatgpt_tokens(CodexChatGptTokens {
                    id_token: String::new(),
                    access_token: "fixture-token".into(),
                    refresh_token: String::new(),
                    account_id: None,
                    chatgpt_account_is_fedramp: false,
                })
                .unwrap();
            Box::new(CodexProvider::new(
                CodexConfig {
                    options,
                    ..Default::default()
                },
                client,
                CodexToolRegistry::new(),
                DefaultCodexPromptBuilder::default(),
                DefaultCodexContextManager::default(),
            ))
        }
        "github-copilot" => {
            let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                api_base_url: base.into(),
                token_store_path: Some(dir.join("copilot.json")),
                options,
                ..Default::default()
            });
            std::fs::write(
                dir.join("copilot.json"),
                json!({"github_token":"ghu_test","source":"fixture","created_at_unix":0})
                    .to_string(),
            )
            .unwrap();
            Box::new(provider)
        }
        "ollama" | "lmstudio" => {
            let runtime = if request.provider == "ollama" {
                LocalResponsesRuntime::Ollama
            } else {
                LocalResponsesRuntime::LmStudio
            };
            let mut config = LocalResponsesConfig::new(runtime);
            config.api_base_url = base.into();
            config.options = options;
            Box::new(LocalResponsesProvider::new(config))
        }
        name => panic!("unknown test provider {name}"),
    };
    let mut events = Vec::new();
    let result = if streaming {
        provider.complete_streaming_events(request, &mut |event| events.push(event))
    } else {
        provider.complete(request)
    }?;
    Ok((result, events))
}

fn response_fixture(provider: &str, model: &str) -> Value {
    match provider {
        "anthropic" => {
            json!({"content":[{"type":"thinking","thinking":"consider","signature":"signed-state"},{"type":"tool_use","id":"call_1","name":"read_file","input":{"path":"a.txt"}}]})
        }
        "gemini" => {
            json!({"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"id":"call_1","name":"read_file","args":{"path":"a.txt"}},"thoughtSignature":"signed-state"}]}}]})
        }
        "openai" | "ollama" | "lmstudio" | "codex" => {
            json!({"output":[{"type":"reasoning","id":"rs_1","encrypted_content":"signed-state","summary":[]},{"type":"function_call","call_id":"call_1","name":"read_file","arguments":"{\"path\":\"a.txt\"}"}]})
        }
        "github-copilot" if model.starts_with("gpt-") => response_fixture("openai", model),
        _ => {
            json!({"choices":[{"message":{"role":"assistant","content":null,"reasoning_content":"signed-state","tool_calls":[{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"a.txt\"}"}}]}}]})
        }
    }
}

#[test]
fn provider_turns_replay_signed_state_once_and_fence_model_changes() {
    for (provider, model) in [
        ("anthropic", "claude-sonnet-5"),
        ("gemini", "gemini-3.8-flash"),
        ("deepseek", "deepseek-flash"),
        ("nvidia", NVIDIA_DEFAULT_MODEL),
        ("openai", "gpt-6-astra"),
        ("codex", "gpt-6-astra"),
        ("github-copilot", "gpt-6-astra"),
        ("github-copilot", "claude-sonnet-5"),
        ("ollama", "gpt-oss:20b"),
        ("lmstudio", "gpt-oss-20b"),
    ] {
        let fixture = response_fixture(provider, model);
        let (url, worker) = server(fixture.clone());
        let mut request = request(provider, model);
        let (response, _) = invoke(&url, &request, Default::default(), false);
        worker.join().unwrap();
        assert_eq!(response.tool_calls.len(), 1, "{provider}");
        assert!(!response.continuation_items().is_empty(), "{provider}");
        request.items = vec![
            LanguageModelRequestItem::message(LanguageModelRole::User, "Read a file"),
            LanguageModelRequestItem::message(LanguageModelRole::Assistant, "fallback assistant"),
        ];
        request.items.extend(response.continuation_items());
        request.items.push(LanguageModelRequestItem::tool_call(
            "call_1",
            "read_file",
            json!({"path":"a.txt"}),
        ));
        request
            .items
            .push(LanguageModelRequestItem::tool_call_output(
                "call_1",
                "read_file",
                json!({"text":"file data"}),
            ));
        // Signed state must survive persistence, not just a client's in-memory cache.
        request = serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        let (url, worker) = server(fixture.clone());
        invoke(&url, &request, Default::default(), false);
        let (_, body) = worker.join().unwrap().remove(0);
        let serialized = body.to_string();
        assert_eq!(
            serialized.matches("signed-state").count(),
            1,
            "{provider}: {body}"
        );
        assert!(
            !serialized.contains("fallback assistant"),
            "{provider}: {body}"
        );
        let calls = if provider == "gemini" {
            serialized.matches("\"functionCall\"").count()
        } else if provider == "anthropic" {
            serialized.matches("\"tool_use\"").count()
        } else if body.get("input").is_some() {
            serialized.matches("\"function_call\"").count()
        } else {
            serialized.matches("\"tool_calls\"").count()
        };
        assert_eq!(calls, 1, "{provider}: duplicate replay {body}");
        request.model = format!("{model}-different");
        let (url, worker) = server(fixture);
        invoke(&url, &request, Default::default(), false);
        let (_, body) = worker.join().unwrap().remove(0);
        assert!(
            !body.to_string().contains("signed-state"),
            "{provider}: wrong-model state leaked"
        );
        assert!(body.to_string().contains("fallback assistant"));
    }
}

#[test]
fn api_model_discovery_uses_account_endpoint_and_keeps_custom_default() {
    for provider in ["openai", "anthropic", "gemini", "deepseek", "nvidia"] {
        let body = if provider == "gemini" {
            json!({"models":[{"name":"models/new-model","supportedGenerationMethods":["generateContent"]},{"name":"models/embed-only","supportedGenerationMethods":["embedContent"]}]})
        } else {
            json!({"data":[{"id":"new-model"},{"id":"new-model"}]})
        };
        let (url, worker) = server(body);
        let account = ProviderAccountConfig {
            provider: provider.into(),
            account: "fixture".into(),
            default_model: Some("custom-default".into()),
            api_base_url: Some(format!("{url}/v1")),
            api_key: Some("account-key".into()),
            enabled: true,
            options: Default::default(),
            model_options: Vec::new(),
        };
        let dir = sinomo_testdir::TestDir::new("scoped-provider-discovery");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(dir.path().to_path_buf());
        let models = provider_available_model_ids_for_account_in_data_dir(
            &data_dir,
            provider,
            Some(&account),
        )
        .unwrap();
        assert_eq!(models, vec!["custom-default", "new-model"], "{provider}");
        let (headers, _) = worker.join().unwrap().remove(0);
        assert!(
            headers.starts_with("GET /v1/models "),
            "{provider}: {headers}"
        );
        assert!(headers.contains("account-key"));
    }
}

#[test]
fn claude_oauth_catalog_keeps_authentication_across_pages() {
    let (url, worker) = sequence_server(vec![
        (
            200,
            "application/json",
            json!({"data":[{"id":"claude-sonnet-5"}],"has_more":true,"last_id":"claude-sonnet-5"})
                .to_string(),
        ),
        (
            200,
            "application/json",
            json!({"data":[{"id":"claude-opus-5"}],"has_more":false}).to_string(),
        ),
    ]);
    let dir = sinomo_testdir::TestDir::new("claude-catalog");
    let provider = ClaudeProvider::new(ClaudeConfig {
        api_base_url: format!("{url}/v1"),
        credentials_path: Some(dir.join("claude.json")),
        ..Default::default()
    });
    provider
        .save_oauth_tokens(ClaudeOAuthTokens {
            access_token: "fixture-oauth".into(),
            refresh_token: String::new(),
            expires_at_ms: None,
            scopes: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        provider.list_models().unwrap(),
        vec!["claude-opus-5", "claude-sonnet-5"]
    );
    let requests = worker.join().unwrap();
    assert!(
        requests[1]
            .0
            .starts_with("GET /v1/models?after_id=claude-sonnet-5 ")
    );
    assert!(requests.iter().all(|(headers, _)| {
        headers
            .to_ascii_lowercase()
            .contains("authorization: bearer fixture-oauth")
    }));
}

#[test]
fn model_catalogs_follow_cursors_and_reject_repeated_cursors() {
    let (url, worker) = sequence_server(vec![
        (
            200,
            "application/json",
            json!({"data":[{"id":"a"}],"has_more":true,"last_id":"a"}).to_string(),
        ),
        (
            200,
            "application/json",
            json!({"data":[{"id":"b"}],"has_more":false}).to_string(),
        ),
    ]);
    let provider = AnthropicProvider::new(AnthropicConfig {
        api_base_url: url,
        api_key: Some("test".into()),
        ..Default::default()
    });
    assert_eq!(provider.list_models().unwrap(), vec!["a", "b"]);
    let requests = worker.join().unwrap();
    assert!(requests[1].0.starts_with("GET /v1/models?after_id=a "));
    let repeated=json!({"models":[{"name":"models/a","supportedGenerationMethods":["generateContent"]}],"nextPageToken":"again"}).to_string();
    let (url, worker) = sequence_server(vec![
        (200, "application/json", repeated.clone()),
        (200, "application/json", repeated),
    ]);
    let provider = GeminiProvider::new(GeminiConfig {
        api_base_url: url,
        api_key: Some("test".into()),
        ..Default::default()
    });
    assert!(
        provider
            .list_models()
            .unwrap_err()
            .to_string()
            .contains("repeated")
    );
    let requests = worker.join().unwrap();
    assert!(requests[1].0.starts_with("GET /models?pageToken=again "));
}

fn stream_fixture(provider: &str, model: &str) -> Vec<Value> {
    match provider {
        "anthropic" | "claude" => vec![
            json!({"type":"message_start","message":{"usage":{"input_tokens":7}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hidden"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-state"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call_1","name":"read_file","input":{}}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"a.txt\"}"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}),
            json!({"type":"message_stop"}),
        ],
        "gemini" => vec![
            json!({"candidates":[{"content":{"role":"model","parts":[{"thought":true,"text":"hidden"},{"functionCall":{"id":"call_1","name":"read_file","args":{"path":"a.txt"}},"thoughtSignature":"signed-state"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":3,"thoughtsTokenCount":9}}),
        ],
        "openai" | "ollama" | "lmstudio" | "codex" => {
            let fixture = response_fixture("openai", model);
            let mut events = fixture["output"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| json!({"type":"response.output_item.done","item":item}))
                .collect::<Vec<_>>();
            events.push(json!({"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":7,"output_tokens":3}}}));
            events
        }
        "github-copilot" if model.starts_with("gpt-") => stream_fixture("openai", model),
        _ => vec![
            json!({"choices":[{"delta":{"reasoning_content":"signed-state"}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"a.txt\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}),
        ],
    }
}

#[test]
fn streaming_decodes_tool_fragments_and_keeps_signed_continuation() {
    for (provider, model) in [
        ("anthropic", "claude-sonnet-5"),
        ("claude", "claude-sonnet-5"),
        ("gemini", "gemini-3.8-flash"),
        ("deepseek", "deepseek-flash"),
        ("nvidia", NVIDIA_DEFAULT_MODEL),
        ("openai", "gpt-6-astra"),
        ("codex", "gpt-6-astra"),
        ("github-copilot", "gpt-6-astra"),
        ("github-copilot", "claude-sonnet-5"),
        ("ollama", "gpt-oss:20b"),
        ("lmstudio", "gpt-oss-20b"),
    ] {
        let body = stream_fixture(provider, model)
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>();
        let (url, worker) = sequence_server(vec![(200, "text/event-stream", body)]);
        let (response, events) = invoke(&url, &request(provider, model), Default::default(), true);
        worker.join().unwrap();
        assert_eq!(response.tool_calls.len(), 1, "{provider}");
        assert_eq!(
            response.tool_calls[0].input,
            json!({"path":"a.txt"}),
            "{provider}"
        );
        assert!(
            response.content.is_empty(),
            "{provider}: reasoning leaked into visible content"
        );
        assert!(
            serde_json::to_string(&response.continuation_items())
                .unwrap()
                .contains("signed-state"),
            "{provider}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, LanguageModelStreamEvent::ToolCallCompleted { .. }))
                .count(),
            1,
            "{provider}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, LanguageModelStreamEvent::Finished { .. }))
                .count(),
            1,
            "{provider}"
        );
        assert_eq!(
            response.usage.unwrap().output_tokens,
            Some(if provider == "gemini" { 12 } else { 3 })
        );
    }
}

#[test]
fn truncated_streams_do_not_complete_a_turn() {
    let mut accepted = Vec::new();
    for (provider, model, event) in [
        (
            "anthropic",
            "claude-sonnet-5",
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}),
        ),
        (
            "gemini",
            "gemini-3.8-flash",
            json!({"candidates":[{"content":{"role":"model","parts":[{"text":"partial"}]}}]}),
        ),
        (
            "deepseek",
            "deepseek-flash",
            json!({"choices":[{"delta":{"content":"partial"}}]}),
        ),
        (
            "openai",
            "gpt-6-astra",
            json!({"type":"response.output_text.delta","delta":"partial"}),
        ),
    ] {
        let (url, worker) = sequence_server(vec![(
            200,
            "text/event-stream",
            format!("data: {event}\n\n"),
        )]);
        if invoke_result(&url, &request(provider, model), Default::default(), true).is_ok() {
            accepted.push(provider);
        }
        worker.join().unwrap();
    }
    assert!(
        accepted.is_empty(),
        "providers accepted a truncated stream: {accepted:?}"
    );
}

#[test]
fn current_model_options_reach_the_provider_wire() {
    for (provider, model, effort, key, expected) in [
        (
            "openai",
            "gpt-6-astra",
            ProviderReasoningEffort::Max,
            "/reasoning/effort",
            json!("max"),
        ),
        (
            "anthropic",
            "claude-sonnet-5",
            ProviderReasoningEffort::XHigh,
            "/output_config/effort",
            json!("xhigh"),
        ),
        (
            "gemini",
            "gemini-3.8-flash",
            ProviderReasoningEffort::Low,
            "/generationConfig/thinkingConfig/thinkingLevel",
            json!("low"),
        ),
        (
            "deepseek",
            "deepseek-flash",
            ProviderReasoningEffort::Max,
            "/reasoning_effort",
            json!("max"),
        ),
        (
            "github-copilot",
            "claude-sonnet-5",
            ProviderReasoningEffort::High,
            "/reasoning_effort",
            json!("high"),
        ),
    ] {
        let (url, worker) = server(response_fixture(provider, model));
        let mut req = request(provider, model);
        req.temperature = Some(0.2);
        let options = LanguageModelProviderOptions {
            reasoning_effort: Some(effort),
            ..Default::default()
        };
        invoke(&url, &req, options, false);
        let (_, body) = worker.join().unwrap().remove(0);
        assert_eq!(body.pointer(key), Some(&expected), "{provider}");
        if matches!(provider, "openai" | "anthropic" | "deepseek") {
            assert!(body.get("temperature").is_none(), "{provider}");
        }
    }
    let (url, worker) = server(response_fixture("nvidia", NVIDIA_DEFAULT_MODEL));
    let mut options = LanguageModelProviderOptions::default();
    options.vendor_options.insert(
        "chat_template_kwargs.enable_thinking".into(),
        "false".into(),
    );
    invoke(
        &url,
        &request("nvidia", NVIDIA_DEFAULT_MODEL),
        options,
        false,
    );
    let (_, body) = worker.join().unwrap().remove(0);
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
}
