use super::*;

#[test]
fn agent_config_workspace_root_populates_local_workspace_ref() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-workspace");

    let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);

    assert_eq!(config.workspace_root.as_deref(), Some(root.path()));
    let workspace = config
        .run_context
        .workspace
        .as_ref()
        .expect("workspace ref");
    assert_eq!(workspace.local_root.as_deref(), Some(root.path()));
    assert_eq!(workspace.write_policy.as_deref(), Some("local"));
}

#[test]
fn tool_evidence_records_support_generic_typed_payloads() {
    let evidence = AgentToolEvidence::availability_only(true, "available")
        .with_record(
            AGENT_CODING_EVIDENCE_KIND,
            &AgentCodingToolEvidence {
                changed_files: vec!["hello.txt".to_string()],
                ..AgentCodingToolEvidence::default()
            },
        )
        .with_record_value("quota", serde_json::json!({ "remaining": 3 }))
        .with_record_value("quota", serde_json::json!({ "remaining": 2 }));

    assert_eq!(evidence.records.len(), 2);
    assert_eq!(
        coding_evidence(&evidence).changed_files,
        vec!["hello.txt".to_string()]
    );
    assert_eq!(
        evidence
            .record_value("quota")
            .and_then(|record| record.get("remaining"))
            .and_then(serde_json::Value::as_i64),
        Some(2)
    );
}

#[test]
fn agent_executes_tool_calls_and_continues_until_final_answer() {
    let task =
        AgentTaskRef::new("coding", "inspect workspace").with_owner_role(AgentRole::Deliberation);
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I will inspect the file.",
            "tool_calls": [{
                "tool": "read_file",
                "input": {"path": "Cargo.toml", "timeout_ms": 123},
                "async": false,
                "summary": "Inspect workspace metadata."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "The workspace evidence is available.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let config = AgentConfig::new("fake", "fake")
        .with_task_ref(task.clone())
        .with_placement(AgentPlacementMetadata::local_current_process());
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

    let run = agent.run(&agent_input("inspect the repo")).expect("run");

    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    assert_eq!(
        run.final_decision.text,
        "The workspace evidence is available."
    );
    assert_eq!(run.model_turns.len(), 2);
    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
    let orchestration = run.tool_results[0]
        .evidence
        .metadata
        .get("tool_orchestration")
        .expect("tool orchestration metadata");
    assert_eq!(
        orchestration
            .get("surface_id")
            .and_then(serde_json::Value::as_str),
        Some("main-default")
    );
    assert_eq!(
        orchestration
            .get("registry_generation")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert!(
        orchestration
            .get("visible_toolsets")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|toolsets| {
                toolsets
                    .iter()
                    .any(|toolset| toolset.as_str() == Some("interactive"))
            })
    );
    assert_eq!(
        orchestration
            .get("executor_available")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        orchestration
            .get("requested_timeout_ms")
            .and_then(serde_json::Value::as_u64),
        Some(123)
    );
    assert_eq!(
        orchestration
            .get("run_context")
            .and_then(|context| context.get("task"))
            .and_then(|task| task.get("task_id"))
            .and_then(serde_json::Value::as_str),
        Some(task.task_id.as_str())
    );
    assert!(
        agent.model.requests.lock().expect("requests")[1]
            .messages
            .iter()
            .any(|message| message.content.contains("tool_results"))
    );
}

#[test]
fn active_tool_turn_keeps_provider_continuation_for_the_next_request() {
    struct NativeStateModel(SequencedModel);
    impl InteractionModel for NativeStateModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            let mut response = self.0.complete(request)?;
            if response.content == "tool turn" {
                response.content.clear();
                response.tool_calls = vec![sinomo_lm::LanguageModelToolCall {
                    call_id: Some("signed-call".into()),
                    name: "echo".into(),
                    input: serde_json::json!({"text":"hello"}),
                    summary: None,
                }];
                response.items = vec![sinomo_lm::LanguageModelResponseItem::ProviderContinuation {
                    continuation: sinomo_lm::LanguageModelProviderContinuation {
                        provider: response.provider.clone(),
                        model: response.model.clone(),
                        items: vec![serde_json::json!({"signature":"opaque-signature"})],
                        tool_call_ids: vec!["signed-call".into()],
                    },
                }];
            }
            Ok(response)
        }
    }
    let final_answer=serde_json::json!({"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}).to_string();
    let model = NativeStateModel(SequencedModel::new(["tool turn".to_string(), final_answer]));
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));
    let run = agent.run(&agent_input("use a tool")).unwrap();
    let requests = agent.model.0.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].items.iter().any(|item|matches!(item,LanguageModelRequestItem::ProviderContinuation {continuation} if continuation.items[0]["signature"]=="opaque-signature")));
    assert!(run.items.iter().any(|item|matches!(item,AgentItem::AssistantText {native_items,..} if native_items.iter().any(|item|matches!(item,LanguageModelRequestItem::ProviderContinuation {..})))));
}

#[test]
fn agent_records_provider_stream_reasoning_tool_usage_and_finish_events() {
    let first_response = r#"{
        "response_mode": "defer",
        "text": "I will inspect the file.",
        "tool_calls": [{
            "tool": "read_file",
            "input": {"path": "Cargo.toml"},
            "async": false,
            "summary": "Inspect workspace metadata."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    }"#;
    let final_response = r#"{
        "response_mode": "answer",
        "text": "Done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#;
    let model = EventfulModel::new([
        (
            first_response,
            vec![
                LanguageModelStreamEvent::Started {
                    provider: "fake".to_string(),
                    model: "fake".to_string(),
                },
                LanguageModelStreamEvent::ReasoningDelta {
                    delta: "checking ".to_string(),
                },
                LanguageModelStreamEvent::ReasoningSummary {
                    summary: "checking workspace".to_string(),
                },
                LanguageModelStreamEvent::ToolCallStarted {
                    call_id: Some("call_1".to_string()),
                    name: "read_file".to_string(),
                },
                LanguageModelStreamEvent::ToolCallDelta {
                    call_id: Some("call_1".to_string()),
                    name: Some("read_file".to_string()),
                    arguments_delta: "{\"path\":\"Cargo.toml\"}".to_string(),
                },
                LanguageModelStreamEvent::ToolCallCompleted {
                    call_id: Some("call_1".to_string()),
                    name: "read_file".to_string(),
                    input: serde_json::json!({"path": "Cargo.toml"}),
                },
                LanguageModelStreamEvent::Usage {
                    usage: sinomo_lm::LanguageModelUsage {
                        input_tokens: Some(5),
                        output_tokens: Some(7),
                    },
                },
                LanguageModelStreamEvent::Finished {
                    finish_reason: Some("tool_calls".to_string()),
                },
            ],
        ),
        (
            final_response,
            vec![LanguageModelStreamEvent::Finished {
                finish_reason: Some("stop".to_string()),
            }],
        ),
    ]);
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

    let run = agent.run(&agent_input("inspect")).expect("run");

    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::AssistantReasoningDelta { delta, .. }
                if delta == "checking "
        )
    }));
    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::AssistantReasoningSummary { summary, .. }
                if summary == "checking workspace"
        )
    }));
    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::ToolCallDelta { arguments_delta, .. }
                if arguments_delta.contains("Cargo.toml")
        )
    }));
    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::Usage { usage: Some(usage), .. }
                if usage.input_tokens == Some(5)
        )
    }));
    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::Finished {
                finish_reason: Some(reason),
                ..
            } if reason == "tool_calls"
        )
    }));
    let requests = agent.model.requests.lock().expect("requests");
    assert_eq!(
        run.model_metrics.final_decision_source,
        Some(AgentModelDecisionSource::StrictJsonDecision)
    );
    assert_eq!(run.model_metrics.model_turn_count, 2);
    assert_eq!(run.model_metrics.native_tool_call_turn_count, 1);
    assert_eq!(run.model_metrics.native_tool_call_count, 1);
    assert_eq!(
        run.model_turns[0].decision_source,
        AgentModelDecisionSource::NativeToolCalls
    );
    assert_eq!(
        run.model_turns[1].decision_source,
        AgentModelDecisionSource::StrictJsonDecision
    );
    let second_items = &requests[1].items;
    assert!(second_items.iter().any(|item| {
        matches!(
            item,
            LanguageModelRequestItem::ToolCall {
                call_id,
                name,
                input,
            } if call_id == "call_1" && name == "read_file" && input["path"] == "Cargo.toml"
        )
    }));
    assert!(second_items.iter().any(|item| {
        matches!(
            item,
            LanguageModelRequestItem::ToolCallOutput {
                call_id,
                name,
                output,
            } if call_id == "call_1"
                && name == "read_file"
                && output["native_call_id"] == "call_1"
                && output["status"] == serde_json::json!(AgentToolStatus::Succeeded)
        )
    }));
}

#[test]
fn agent_consumes_native_stream_tool_calls_without_json_decision_text() {
    let final_response = r#"{
        "response_mode": "answer",
        "text": "The native tool call completed.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#;
    let model = EventfulModel::new([
        (
            "",
            vec![
                LanguageModelStreamEvent::Started {
                    provider: "fake".to_string(),
                    model: "fake".to_string(),
                },
                LanguageModelStreamEvent::ToolCallCompleted {
                    call_id: Some("call_native_1".to_string()),
                    name: "read_file".to_string(),
                    input: serde_json::json!({"path": "Cargo.toml"}),
                },
                LanguageModelStreamEvent::Finished {
                    finish_reason: Some("tool_calls".to_string()),
                },
            ],
        ),
        (
            final_response,
            vec![LanguageModelStreamEvent::Finished {
                finish_reason: Some("stop".to_string()),
            }],
        ),
    ]);
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

    let run = agent.run(&agent_input("inspect natively")).expect("run");

    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].call.tool, "read_file");
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
    assert_eq!(run.final_decision.text, "The native tool call completed.");
    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::ToolCallCompleted {
                call_id: Some(call_id),
                call,
                ..
            } if call_id == "call_native_1" && call.tool == "read_file"
        )
    }));
}

#[test]
fn file_trace_store_records_run_evidence_as_jsonl() {
    let trace_root = sinomo_testdir::TestDir::new("sinomo-agent-trace");
    let trace_path = trace_root.join("trace.jsonl");
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I will inspect the file.",
            "tool_calls": [{
                "tool": "read_file",
                "input": {"path": "Cargo.toml"},
                "async": false,
                "summary": "Inspect workspace metadata."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool))
        .with_trace_store(FileAgentTraceStore::new(&trace_path));

    agent.run(&agent_input("inspect the repo")).expect("run");

    let records: Vec<AgentTraceRecord> = fs::read_to_string(&trace_path)
        .expect("trace")
        .lines()
        .map(|line| serde_json::from_str(line).expect("trace record"))
        .collect();

    assert!(
        records
            .iter()
            .any(|record| { matches!(record.record, AgentTraceRecordKind::RunStarted { .. }) })
    );
    assert!(
        records
            .iter()
            .any(|record| { matches!(record.record, AgentTraceRecordKind::ContextReport { .. }) })
    );
    assert!(
        records
            .iter()
            .any(|record| { matches!(record.record, AgentTraceRecordKind::ModelEvent { .. }) })
    );
    assert!(
        records
            .iter()
            .any(|record| { matches!(record.record, AgentTraceRecordKind::ToolOutcome { .. }) })
    );
    assert!(records.iter().any(|record| {
        matches!(
            record.record,
            AgentTraceRecordKind::Item {
                item: AgentItem::ToolResult { .. }
            }
        )
    }));
    assert!(
        records
            .iter()
            .any(|record| { matches!(record.record, AgentTraceRecordKind::RunCompleted { .. }) })
    );
}

#[test]
fn file_trace_store_can_write_to_agent_data_dir_layout() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-trace-layout");
    let data_dir = SinomoDataDir::new(&data_root);
    let session_id = SessionId::from_legacy_named("sess_agent_trace").expect("session");
    let store = FileAgentTraceStore::for_data_dir_session(&data_dir, &session_id);

    store
        .append(&AgentTraceRecord::new(
            session_id.clone(),
            AgentTraceRecordKind::RunStarted {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                role: AgentRole::Deliberation,
                surface_id: "test-surface".to_string(),
                max_steps: Some(1),
                run_context: AgentRunContext::default(),
            },
        ))
        .expect("append trace");

    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    assert_eq!(
        store.path(),
        paths.trace_path_for_session(&session_id).as_path()
    );
    assert!(
        fs::read_to_string(paths.trace_path_for_session(&session_id))
            .expect("trace")
            .contains("\"run_started\"")
    );
}

#[test]
fn unknown_tool_is_returned_as_model_visible_failure() {
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I will call a tool.",
            "tool_calls": [{
                "tool": "missing_tool",
                "input": {},
                "async": false,
                "summary": "Try a missing tool."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "The requested tool is not available.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake");

    let run = agent.run(&agent_input("use a tool")).expect("run");

    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Failed);
    assert!(run.tool_results[0].summary.contains("unknown tool"));
    assert!(
        agent.model.requests.lock().expect("requests")[1]
            .messages
            .iter()
            .any(|message| message.content.contains("unknown_tool"))
    );
}

#[test]
fn tool_registry_cancellation_prevents_custom_tool_execution() {
    let executed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let registry = AgentToolRegistry::new().with_tool(CancellationProbeTool {
        executed: executed.clone(),
    });
    let input = agent_input("cancel custom tool");
    let call = AgentToolCall {
        call_id: None,
        tool: "cancel_probe".to_string(),
        input: serde_json::json!({}),
        r#async: false,
        summary: "cancel probe".to_string(),
    };
    let cancellation_token = AgentCancellationToken::new();
    cancellation_token.cancel();

    let outcome =
        registry.execute_with_cancellation(&input.session, 0, 0, &call, cancellation_token);

    assert_eq!(outcome.status, AgentToolStatus::Cancelled);
    assert!(!executed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        outcome
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("tool_cancelled")
    );
    assert_eq!(
        outcome.evidence.availability.reason.as_str(),
        "turn cancellation requested before tool execution"
    );
}

#[test]
fn tool_plan_tracks_generation_and_scopes_model_visible_descriptors() {
    let runtime_descriptors = vec![
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "read a fake file",
            false,
        )),
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ConfigGet,
            "config_get",
            "read config",
            false,
        )),
    ];

    let plan = AgentToolPlanner::plan(
        runtime_descriptors,
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["config"],
        42,
    );
    let visible_tools: Vec<_> = plan
        .model_visible_descriptors()
        .into_iter()
        .map(|descriptor| descriptor.spec.name)
        .collect();

    assert_eq!(plan.registry_generation, 42);
    assert_eq!(plan.visible_toolsets, vec!["config"]);
    assert!(plan.is_executable("read_file"));
    assert!(!plan.is_model_visible("read_file"));
    assert!(plan.is_model_visible("config_get"));
    assert_eq!(visible_tools, vec!["config_get"]);
}

#[test]
fn progressive_tool_disclosure_searches_describes_and_calls_hidden_tools() {
    let registry = AgentToolRegistry::new().with_tool(EchoTool);
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let input = agent_input("use disclosure");
    let policy = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let run_context = AgentRunContext::default();
    let orchestrator = AgentToolOrchestrator::new(&policy, None, &run_context);

    assert!(!plan.is_model_visible("read_file"));
    assert!(plan.is_model_visible("tool_search"));

    let search = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "tool_search".to_string(),
            input: serde_json::json!({ "query": "read" }),
            r#async: false,
            summary: "find read tool".to_string(),
        },
    );
    assert_eq!(search.status, AgentToolStatus::Succeeded);
    assert!(
        search
            .output
            .get("matches")
            .and_then(serde_json::Value::as_array)
            .expect("matches")
            .iter()
            .any(
                |entry| entry.get("name").and_then(serde_json::Value::as_str) == Some("read_file")
                    && entry
                        .get("model_visible")
                        .and_then(serde_json::Value::as_bool)
                        == Some(false)
            )
    );

    let describe = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "tool_describe".to_string(),
            input: serde_json::json!({ "tool": "read_file" }),
            r#async: false,
            summary: "describe read tool".to_string(),
        },
    );
    assert_eq!(describe.status, AgentToolStatus::Succeeded);
    assert_eq!(
        describe
            .output
            .pointer("/descriptor/spec/name")
            .and_then(serde_json::Value::as_str),
        Some("read_file")
    );

    let called = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        2,
        &AgentToolCall {
            call_id: None,
            tool: "tool_call".to_string(),
            input: serde_json::json!({
                "tool": "read_file",
                "input": { "path": "Cargo.toml" },
                "summary": "read through bridge"
            }),
            r#async: false,
            summary: "call hidden read tool".to_string(),
        },
    );
    assert_eq!(called.status, AgentToolStatus::Succeeded);
    assert_eq!(
        called
            .output
            .get("content")
            .and_then(serde_json::Value::as_str),
        Some("workspace evidence")
    );
    assert_eq!(
        called
            .output
            .pointer("/_tool_call/target_tool")
            .and_then(serde_json::Value::as_str),
        Some("read_file")
    );
    assert!(called.evidence.record_value("tool_disclosure").is_some());
}

#[test]
fn render_tool_results_bounds_aggregate_model_visible_output() {
    let late_tail = "LATE_TOOL_RESULT_CONTENT_SHOULD_NOT_BE_MODEL_VISIBLE";
    let outcomes = (0..10)
        .map(|index| {
            let marker = if index == 9 {
                late_tail
            } else {
                "visible-result"
            };
            let content = format!("{marker}\n{}", "r".repeat(12 * 1024));
            AgentToolOutcome {
                call_id: TaskId::generate(),
                step_index: 4,
                call_index: index,
                call: AgentToolCall {
                    call_id: None,
                    tool: "read_file".to_string(),
                    input: serde_json::json!({ "path": format!("file-{index}.txt") }),
                    r#async: false,
                    summary: "read file".to_string(),
                },
                status: AgentToolStatus::Succeeded,
                summary: format!("read file-{index}.txt"),
                output: serde_json::json!({
                    "path": format!("file-{index}.txt"),
                    "bytes": content.len(),
                    "content_bytes": content.len(),
                    "truncated": false,
                    "content": content,
                    "total_lines": 2,
                    "line_range": {
                        "requested_start_line": null,
                        "requested_end_line": null,
                        "returned_start_line": 1,
                        "returned_end_line": 2,
                    },
                }),
                event_id: None,
                evidence: AgentToolEvidence::availability_only(true, "allowed"),
            }
        })
        .collect::<Vec<_>>();
    assert!(
        outcomes[9]
            .output
            .get("content")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.contains(late_tail))
    );

    let rendered = render_tool_results(4, &outcomes).expect("render");

    assert!(rendered.contains("\"projection\": \"model_visible_aggregate_bounded\""));
    assert!(rendered.contains("\"output_omitted\""));
    assert!(rendered.contains("read file-9.txt"));
    assert!(!rendered.contains(late_tail), "{rendered}");
}

#[test]
fn render_tool_results_surfaces_failed_command_stderr() {
    let outcome = AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index: 4,
        call_index: 0,
        call: AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "make check" }),
            r#async: false,
            summary: "run focused check".to_string(),
        },
        status: AgentToolStatus::Failed,
        summary: "make check failed with exit status Some(2)".to_string(),
        output: serde_json::json!({
            "program": "bash",
            "args": ["-lc", "make check"],
            "cwd": ".",
            "status": 2,
            "success": false,
            "timed_out": false,
            "timeout_ms": null,
            "stdout": "running checks\n",
            "stderr": "checker reported the actual failure\n",
            "stdout_truncated": false,
            "stderr_truncated": false,
            "changed_files": [],
        }),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(true, "allowed"),
    };

    let rendered = render_tool_results(4, &[outcome]).expect("render");

    assert!(rendered.contains("\"command_output_diagnostics\""));
    assert!(rendered.contains("\"kind\": \"command_failed\""));
    assert!(rendered.contains("\"validation_status\": \"failed_patch_relevant\""));
    assert!(rendered.contains("\"failure_kind\": \"patch_relevant\""));
    assert!(rendered.contains("checker reported the actual failure"));
}

#[test]
fn render_tool_results_omits_command_artifact_uris_from_model_output() {
    let stdout_ref = AgentToolArtifactRef::new("stdout", "/tmp/sinomo-agent/evidence/stdout.txt")
        .with_label("complete stdout")
        .with_media_type("text/plain; charset=utf-8");
    let stderr_ref = AgentToolArtifactRef::new("stderr", "/tmp/sinomo-agent/evidence/stderr.txt")
        .with_label("complete stderr")
        .with_media_type("text/plain; charset=utf-8");
    let command_output = AgentCommandOutput {
        exit_code: Some(1),
        stdout: "truncated stdout".to_string(),
        stderr: "truncated stderr".to_string(),
        stdout_ref: Some(stdout_ref.clone()),
        stderr_ref: Some(stderr_ref.clone()),
        aggregated_output: "stdout:\ntruncated stdout\nstderr:\ntruncated stderr".to_string(),
        duration_ms: 42,
        timed_out: false,
        truncated: true,
        stdout_truncated: true,
        stderr_truncated: true,
    };
    let outcome = AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index: 5,
        call_index: 0,
        call: AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "make check" }),
            r#async: false,
            summary: "run focused check".to_string(),
        },
        status: AgentToolStatus::Failed,
        summary: "make check failed".to_string(),
        output: serde_json::json!({
            "program": "bash",
            "args": ["-lc", "make check"],
            "cwd": ".",
            "status": 1,
            "success": false,
            "timed_out": false,
            "duration_ms": 42,
            "timeout_ms": 1000,
            "stdout": "truncated stdout",
            "stderr": "truncated stderr",
            "stdout_ref": stdout_ref,
            "stderr_ref": stderr_ref,
            "aggregated_output": "stdout:\ntruncated stdout\nstderr:\ntruncated stderr",
            "command_output": command_output,
            "stdout_truncated": true,
            "stderr_truncated": true,
            "changed_files": [],
        }),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(true, "allowed")
            .with_artifact(AgentToolArtifactRef::new(
                "stdout",
                "/tmp/sinomo-agent/evidence/stdout.txt",
            ))
            .with_artifact(AgentToolArtifactRef::new(
                "stderr",
                "/tmp/sinomo-agent/evidence/stderr.txt",
            )),
    };

    let rendered = render_tool_results(5, &[outcome]).expect("render");

    assert!(
        !rendered.contains("/tmp/sinomo-agent/evidence"),
        "{rendered}"
    );
    assert!(rendered.contains("\"stdout_ref_available\": true"));
    assert!(rendered.contains("\"stderr_ref_available\": true"));
    assert!(rendered.contains("\"duration_ms\": 42"));
    assert!(rendered.contains("\"timeout_ms\": 1000"));
    assert!(rendered.contains("\"uri_omitted\": true"));
    assert!(rendered.contains("\"aggregated_output_excerpt\""));
}

#[test]
fn native_custom_tool_text_is_wrapped_with_configured_argument() {
    let descriptor = AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::ApplyPatch,
        "apply_patch",
        "apply a patch",
        false,
    ))
    .with_provider_specs([AgentToolProviderSpec::custom_grammar(
        "openai",
        "lark",
        "start: /.+/",
    )
    .with_raw_input_argument("patch")]);
    let raw_patch = "*** Begin Patch\n*** End Patch\n";
    let call = LanguageModelToolCall {
        call_id: Some("call_patch".to_string()),
        name: "apply_patch".to_string(),
        input: serde_json::json!(raw_patch),
        summary: None,
    };

    let response = LanguageModelResponse {
        provider: "openai".to_string(),
        model: "gpt-5.2-codex".to_string(),
        content: String::new(),
        items: Vec::new(),
        tool_calls: vec![call],
        usage: None,
    };
    let parsed = parse_agent_model_decision(
        &response,
        &[],
        &[descriptor],
        &AgentOutputFormat::JsonDecision,
    )
    .expect("decision");

    let decision = parsed.decision;
    assert_eq!(parsed.source, AgentModelDecisionSource::NativeToolCalls);
    assert_eq!(decision.tool_calls.len(), 1);
    assert_eq!(decision.tool_calls[0].tool, "apply_patch");
    assert_eq!(decision.tool_calls[0].input["patch"], raw_patch);
}

#[test]
fn tool_plan_readiness_reports_visible_tools_without_executors() {
    let plan = AgentToolPlanner::plan(
        std::iter::empty::<AgentToolDescriptor>(),
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        default_coding_agent_tools(),
        true,
        default_visible_toolsets_for_role(&AgentRole::Deliberation),
        0,
    );

    let issues = plan.readiness_issues();

    assert!(issues.iter().any(|issue| {
        issue.kind == crate::tools::AgentToolPlanReadinessIssueKind::VisibleToolMissingExecutor
            && issue.tool == "read_file"
            && issue.source == crate::tools::AgentToolPlanSource::Default
    }));
    assert!(issues.iter().any(|issue| issue.tool == "run_command"));
    assert!(
        !issues
            .iter()
            .any(|issue| issue.tool == "tool_search" || issue.tool == "skill_search"),
        "disclosure bridge tools are runtime-executable"
    );
}

#[test]
fn tool_plan_readiness_accepts_local_workspace_visible_executors() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-readiness-local-tools");
    let registry = AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root).enabling_git_tools(),
    );
    crate::readiness::register_readiness_recall_tools(&registry);
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        default_coding_agent_tools(),
        true,
        default_visible_toolsets_for_role(&AgentRole::Deliberation),
        registry.generation(),
    );

    assert_eq!(plan.readiness_issues(), Vec::new());
    assert!(plan.is_model_visible("changed_files"));
    assert!(plan.is_executable("changed_files"));
    assert!(plan.is_model_visible("git_diff"));
    assert!(plan.is_executable("git_diff"));
    for recall_tool in [
        "session_history_search",
        "memory_search",
        "memory_read",
        "memory_write",
    ] {
        assert!(plan.is_model_visible(recall_tool));
        assert!(plan.is_executable(recall_tool));
    }
}

#[test]
fn default_agent_tools_match_initial_architecture_surface() {
    let coding_tool_names = default_coding_agent_tools()
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    assert!(!coding_tool_names.contains(&"changed_files".to_string()));
    assert!(!coding_tool_names.contains(&"git_diff".to_string()));
    assert!(!coding_tool_names.contains(&"apply_patch".to_string()));
    assert!(!coding_tool_names.contains(&"replace_exact".to_string()));
    assert!(!coding_tool_names.contains(&"replace_range".to_string()));
    assert!(coding_tool_names.contains(&"edit_file".to_string()));
    assert!(coding_tool_names.contains(&"write_file".to_string()));

    let main_tool_names = default_main_agent_tools()
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    for tool in &coding_tool_names {
        assert!(
            main_tool_names.contains(tool),
            "main defaults are missing coding tool {tool}"
        );
    }
    for system_tool in [
        "config_search",
        "config_get",
        "config_set",
        "cluster_status",
    ] {
        assert!(
            main_tool_names.contains(&system_tool.to_string()),
            "main defaults are missing Sinomo system tool {system_tool}"
        );
    }

    let background_tool_names = default_background_agent_tools()
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    for mutation_tool in ["edit_file", "write_file", "run_command", "tool_call"] {
        assert!(
            !background_tool_names.contains(&mutation_tool.to_string()),
            "background defaults unexpectedly gained {mutation_tool}"
        );
    }

    for (role, tools) in [
        ("main", default_main_agent_tools()),
        ("background", default_background_agent_tools()),
        ("coding", default_coding_agent_tools()),
        (
            "custom",
            AgentDefinition::custom("custom-defaults", AgentRole::Background, "Custom agent.")
                .tools,
        ),
    ] {
        let names = tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>();
        for expected in [
            "session_history_search",
            "memory_search",
            "memory_read",
            "memory_write",
        ] {
            assert!(
                names.contains(&expected),
                "{role} defaults are missing {expected}"
            );
        }
    }

    let descriptors = default_agent_tool_descriptors();
    let names: Vec<_> = descriptors
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect();

    for expected in [
        "session_history_search",
        "memory_search",
        "memory_read",
        "memory_write",
        "read_file",
        "list_files",
        "search",
        "edit_file",
        "write_file",
        "apply_patch",
        "changed_files",
        "git_diff",
        "run_command",
        "request_user_input",
        "skill_update",
    ] {
        assert!(names.contains(&expected), "{expected}");
    }

    assert!(!names.contains(&"replace_exact"));
    assert!(!names.contains(&"replace_range"));

    let apply_patch = descriptors
        .iter()
        .find(|descriptor| descriptor.spec.name == "apply_patch")
        .expect("apply_patch descriptor");
    assert!(
        apply_patch
            .toolsets
            .contains(&"provider_specific".to_string())
    );
    assert!(!apply_patch.toolsets.contains(&"core".to_string()));

    for expected_hidden in ["changed_files", "git_diff"] {
        let descriptor = descriptors
            .iter()
            .find(|descriptor| descriptor.spec.name == expected_hidden)
            .expect(expected_hidden);
        assert_eq!(
            descriptor
                .toolsets
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["version_control"]
        );
    }
}

#[test]
fn tool_surface_filters_model_visible_tool_schemas() {
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_tool_registry(AgentToolRegistry::descriptor_only(
        default_agent_tool_descriptors(),
    ))
    .with_tool_surface(
        AgentToolSurface::default_for_role(AgentRole::Deliberation)
            .with_denied_capabilities([AgentToolCapability::ExecuteProcess]),
    );

    let context = agent
        .assemble_context(&agent_input("inspect"), &[], 0)
        .expect("context");
    let visible_tools: Vec<_> = context
        .available_tool_specs
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect();

    assert!(!visible_tools.contains(&"run_command"));
    assert!(
        context
            .report
            .omitted
            .iter()
            .any(|omission| omission.label == "run_command"
                && omission.reason.contains("unavailable capability"))
    );
    assert!(
        context
            .report
            .included
            .iter()
            .any(|source| source.kind == "agent_tool_surface")
    );
}

#[test]
fn openai_workspace_agent_surfaces_expose_apply_patch() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-openai-apply-patch-surface");
    let openai_config = AgentConfig::new("openai", "gpt-5.2-codex")
        .with_agent_definition(AgentDefinition::coding())
        .with_workspace_root(&root);
    let openai_agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "openai",
        "gpt-5.2-codex",
    )
    .with_config(openai_config)
    .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root),
    ));

    let openai_context = openai_agent
        .assemble_context(&agent_input("inspect"), &[], 0)
        .expect("openai context");
    let openai_tools: Vec<_> = openai_context
        .available_tool_specs
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect();
    assert!(openai_tools.contains(&"edit_file"));
    assert!(openai_tools.contains(&"write_file"));
    assert!(openai_tools.contains(&"apply_patch"));
    let apply_patch = openai_context
        .available_tool_specs
        .iter()
        .find(|descriptor| descriptor.spec.name == "apply_patch")
        .expect("openai apply_patch descriptor");
    for provider in ["openai", "codex"] {
        assert!(apply_patch.provider_specs.iter().any(|spec| {
            spec.provider == provider
                && spec.custom.as_ref().is_some_and(|custom| {
                    matches!(&custom.format, AgentToolCustomFormat::Grammar { .. })
                })
        }));
    }

    let fake_config = AgentConfig::new("fake", "fake")
        .with_agent_definition(AgentDefinition::coding())
        .with_workspace_root(&root);
    let fake_agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(fake_config)
    .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root),
    ));
    let fake_context = fake_agent
        .assemble_context(&agent_input("inspect"), &[], 0)
        .expect("fake context");
    let fake_tools: Vec<_> = fake_context
        .available_tool_specs
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect();
    assert!(fake_tools.contains(&"edit_file"));
    assert!(fake_tools.contains(&"write_file"));
    assert!(!fake_tools.contains(&"apply_patch"));

    let openai_main = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "openai",
        "gpt-5.2-codex",
    )
    .with_config(
        AgentConfig::new("openai", "gpt-5.2-codex")
            .with_agent_definition(AgentDefinition::main())
            .with_workspace_root(&root),
    )
    .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root),
    ));
    let openai_main_context = openai_main
        .assemble_context(&agent_input("inspect"), &[], 0)
        .expect("openai main context");
    let openai_main_tools = openai_main_context
        .available_tool_specs
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect::<Vec<_>>();
    for coding_tool in ["edit_file", "write_file", "apply_patch"] {
        assert!(
            openai_main_tools.contains(&coding_tool),
            "OpenAI main tool surface is missing {coding_tool}"
        );
    }
}

#[test]
fn tool_surface_blocks_denied_tool_before_registry_execution() {
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I will run a command.",
            "tool_calls": [{
                "tool": "run_command",
                "input": {"command": "echo should-not-run"},
                "async": false,
                "summary": "Try a denied command."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "The command was hidden by the tool surface.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake").with_tool_surface(
        AgentToolSurface::default_for_role(AgentRole::Deliberation)
            .with_denied_tools(["run_command"]),
    );

    let run = agent.run(&agent_input("run a command")).expect("run");

    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Blocked);
    assert!(!run.tool_results[0].evidence.availability.allowed);
    assert_eq!(
        run.tool_results[0]
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("tool_unavailable")
    );
    assert!(
        run.items
            .iter()
            .any(|item| matches!(item, AgentItem::ToolResult { evidence, .. } if !evidence.availability.allowed))
    );
}

#[test]
fn tool_surface_and_evidence_use_availability_names_with_legacy_aliases() {
    let surface =
        AgentToolSurface::new("main-default", AgentRole::Main).with_denied_tools(["run_command"]);
    let rendered_surface = serde_json::to_value(&surface).expect("surface json");
    assert_eq!(rendered_surface["surface_id"], "main-default");
    assert!(rendered_surface.get("policy_id").is_none());

    let legacy_surface: AgentToolSurface = serde_json::from_value(serde_json::json!({
        "policy_id": "legacy-main",
        "role": "main",
        "tools": {
            "denied_tools": ["run_command"]
        }
    }))
    .expect("legacy surface json");
    assert_eq!(legacy_surface.surface_id, "legacy-main");
    assert_eq!(
        legacy_surface.tools.denied_tools,
        vec!["run_command".to_string()]
    );

    let evidence = AgentToolEvidence::availability_only(false, "hidden by surface");
    let rendered_evidence = serde_json::to_value(&evidence).expect("evidence json");
    assert_eq!(rendered_evidence["availability"]["allowed"], false);
    assert!(rendered_evidence.get("policy").is_none());

    let legacy_evidence: AgentToolEvidence = serde_json::from_value(serde_json::json!({
        "policy": {
            "allowed": false,
            "reason": "legacy hidden tool"
        }
    }))
    .expect("legacy evidence json");
    assert!(!legacy_evidence.availability.allowed);
    assert_eq!(legacy_evidence.availability.reason, "legacy hidden tool");
}

#[test]
fn local_workspace_registry_reads_repo_files() {
    let registry = AgentToolRegistry::local_workspace(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    let input = agent_input("read");
    let call = AgentToolCall {
        call_id: None,
        tool: "read_file".to_string(),
        input: serde_json::json!({
            "path": "Cargo.toml",
            "max_bytes": 512
        }),
        r#async: false,
        summary: "read manifest".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert!(
        outcome
            .output
            .get("content")
            .and_then(serde_json::Value::as_str)
            .expect("content")
            .contains("name = \"sinomo-agent\"")
    );
}

#[test]
fn local_workspace_registry_lists_files_with_bounds_and_hidden_filter() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-list-files");
    fs::create_dir_all(root.join("src")).expect("workspace");
    fs::write(root.join("README.md"), "hello").expect("readme");
    fs::write(root.join(".secret"), "hidden").expect("hidden");
    fs::write(root.join("src").join("lib.rs"), "pub fn sample() {}").expect("source");
    let registry = AgentToolRegistry::local_workspace(&root);
    let input = agent_input("list files");

    let shallow = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "list_files".to_string(),
            input: serde_json::json!({ "path": "." }),
            r#async: false,
            summary: "list root".to_string(),
        },
    );
    assert_eq!(shallow.status, AgentToolStatus::Succeeded);
    let shallow_entries = shallow
        .output
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .expect("entries");
    assert!(shallow_entries.iter().any(|entry| {
        entry.get("path").and_then(serde_json::Value::as_str) == Some("README.md")
    }));
    assert!(
        shallow_entries
            .iter()
            .any(|entry| entry.get("path").and_then(serde_json::Value::as_str) == Some("src"))
    );
    assert!(
        !shallow_entries
            .iter()
            .any(|entry| entry.get("path").and_then(serde_json::Value::as_str) == Some(".secret"))
    );
    assert!(!shallow_entries.iter().any(|entry| {
        entry.get("path").and_then(serde_json::Value::as_str) == Some("src/lib.rs")
    }));

    let recursive = registry.execute(
        &input.session,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "list_files".to_string(),
            input: serde_json::json!({
                "path": ".",
                "recursive": true,
                "include_hidden": true,
                "max_entries": 2
            }),
            r#async: false,
            summary: "list recursively".to_string(),
        },
    );
    assert_eq!(recursive.status, AgentToolStatus::Succeeded);
    assert_eq!(
        recursive
            .output
            .get("truncated")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        recursive
            .output
            .get("count")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );
}

#[test]
fn local_workspace_read_file_supports_one_based_line_ranges() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-read-line-range");
    fs::write(root.join("sample.txt"), "one\ntwo\nthree\nfour\n").expect("seed file");
    let registry = AgentToolRegistry::local_workspace(&root);
    let input = agent_input("read range");
    let call = AgentToolCall {
        call_id: None,
        tool: "read_file".to_string(),
        input: serde_json::json!({
            "path": "sample.txt",
            "start_line": 2,
            "end_line": 3
        }),
        r#async: false,
        summary: "read lines".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        outcome
            .output
            .get("content")
            .and_then(serde_json::Value::as_str),
        Some("two\nthree\n")
    );
    assert_eq!(
        outcome
            .output
            .pointer("/line_range/returned_start_line")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );
    assert_eq!(
        outcome
            .output
            .pointer("/line_range/returned_end_line")
            .and_then(serde_json::Value::as_u64),
        Some(3)
    );
    assert_eq!(
        outcome
            .output
            .get("total_lines")
            .and_then(serde_json::Value::as_u64),
        Some(4)
    );
}

#[test]
fn local_workspace_registry_preserves_truncated_read_evidence() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-test");
    let data_dir = SinomoDataDir::new(&data_root);
    let input = agent_input("read");
    let mut config = LocalWorkspaceAgentConfig::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
        .with_data_dir_evidence(&data_dir, &input.session.session_id);
    config.max_read_bytes = 16;
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let call = AgentToolCall {
        call_id: None,
        tool: "read_file".to_string(),
        input: serde_json::json!({
            "path": "Cargo.toml",
            "max_bytes": 16
        }),
        r#async: false,
        summary: "read manifest".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert!(outcome.evidence.output.truncated);
    let evidence_ref = outcome.evidence.full_output_ref().expect("full output ref");
    assert!(evidence_ref.contains("events/agent/evidence"));
    assert!(
        fs::read_to_string(evidence_ref)
            .expect("evidence")
            .contains("name = \"sinomo-agent\"")
    );
}

#[test]
fn local_workspace_registry_can_mirror_evidence_to_sifs_mount() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-sifs-evidence");
    let mount_dir = root.join("sifs-mount");
    fs::create_dir_all(&root).expect("workspace");
    fs::write(root.join("large.txt"), "alpha\nbeta\ngamma\n").expect("seed file");
    let input = agent_input("read with sifs evidence");
    let mut config = LocalWorkspaceAgentConfig::new(&root).with_sifs_evidence_mount(
        &mount_dir,
        format!(
            "/events/agent/evidence/{}",
            input.session.session_id.as_str()
        ),
    );
    config.max_read_bytes = 5;
    let registry = AgentToolRegistry::local_workspace_with_config(config);

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "read_file".to_string(),
            input: serde_json::json!({
                "path": "large.txt",
                "max_bytes": 5
            }),
            r#async: false,
            summary: "read file".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    let evidence_ref = outcome.evidence.full_output_ref().expect("full output ref");
    assert!(evidence_ref.starts_with("sifs://events/agent/evidence/"));
    assert!(evidence_ref.contains(input.session.session_id.as_str()));
    let mirrored: Vec<_> = fs::read_dir(&mount_dir)
        .expect("sifs mirror dir")
        .map(|entry| entry.expect("mirror entry").path())
        .collect();
    assert_eq!(mirrored.len(), 1);
    assert_eq!(
        fs::read_to_string(&mirrored[0]).expect("sifs mirror"),
        "alpha\nbeta\ngamma\n"
    );
}

#[test]
fn local_workspace_registry_records_output_evidence_for_readonly_tools() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-readonly-evidence");
    fs::write(root.join("a.txt"), "needle\n").expect("seed a");
    fs::write(root.join("b.txt"), "other\n").expect("seed b");
    let mut config = LocalWorkspaceAgentConfig::new(&root);
    config.max_search_matches = 10;
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("readonly evidence");

    let search = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "search".to_string(),
            input: serde_json::json!({
                "query": "needle",
                "path": ".",
                "max_matches": 10
            }),
            r#async: false,
            summary: "search files".to_string(),
        },
    );

    assert_eq!(search.status, AgentToolStatus::Succeeded);
    assert!(search.evidence.output.model_visible_bytes > 0);
    assert!(!search.evidence.output.truncated);
}

#[test]
fn local_workspace_registry_reports_changed_files_and_git_diff() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-git-tools");
    let evidence_dir = root.join("evidence");
    fs::create_dir_all(&root).expect("workspace");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    run_git_for_test(&root, ["init"]);
    run_git_for_test(&root, ["config", "user.email", "sinomo@example.invalid"]);
    run_git_for_test(&root, ["config", "user.name", "Sinomo Test"]);
    run_git_for_test(&root, ["add", "hello.txt"]);
    run_git_for_test(&root, ["commit", "-m", "seed"]);
    fs::write(root.join("hello.txt"), "new\n").expect("modify file");
    fs::write(root.join("new.txt"), "untracked\n").expect("untracked file");
    let registry = with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root)
            .using_local_process_commands()
            .with_evidence_dir(&evidence_dir),
    ));
    let input = agent_input("inspect git state");

    let changed = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "changed_files".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "List changes.".to_string(),
        },
    );
    assert_eq!(changed.status, AgentToolStatus::Succeeded);
    let paths = changed
        .output
        .get("paths")
        .and_then(serde_json::Value::as_array)
        .expect("paths");
    assert!(paths.iter().any(|path| path == "hello.txt"));
    assert!(paths.iter().any(|path| path == "new.txt"));

    let diff = registry.execute(
        &input.session,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "git_diff".to_string(),
            input: serde_json::json!({ "max_bytes": 4096 }),
            r#async: false,
            summary: "Read diff.".to_string(),
        },
    );
    assert_eq!(diff.status, AgentToolStatus::Succeeded);
    assert!(
        diff.output
            .get("diff")
            .and_then(serde_json::Value::as_str)
            .expect("diff")
            .contains("-old")
    );
    let diff_ref = diff
        .output
        .get("full_diff_ref")
        .and_then(serde_json::Value::as_str)
        .expect("full diff ref");
    assert!(Path::new(diff_ref).exists());
    let coding = coding_evidence(&diff.evidence);
    assert_eq!(coding.changed_files, vec!["hello.txt".to_string()]);
    assert!(
        coding
            .diff_ref
            .as_deref()
            .is_some_and(|path| Path::new(path).exists())
    );
}

#[test]
fn local_workspace_registry_blocks_shell_commands_by_default() {
    let registry = with_remote_management_for_test(AgentToolRegistry::local_workspace(
        std::env::current_dir().expect("current dir"),
    ));
    let input = agent_input("command");
    let call = AgentToolCall {
        call_id: None,
        tool: "run_command".to_string(),
        input: serde_json::json!({ "command": "echo hi" }),
        r#async: false,
        summary: "run command".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Blocked);
    assert_eq!(
        outcome.output.get("error").and_then(|value| value.as_str()),
        Some("command_execution_disabled")
    );
}

#[cfg(not(windows))]
#[test]
fn local_workspace_run_command_uses_simple_sandbox_by_default() {
    if !sinomo_runtime::sandbox_simple::SandboxSimpleNativeCommandRunner::landlock_supported() {
        return;
    }
    let root = sinomo_testdir::TestDir::new("sinomo-agent-command-simple-sandbox");
    let registry = AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root)
            .without_agent_home_sifs_path()
            .allowing_shell_commands(true),
    );
    let input = agent_input("simple sandbox command");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "printf ok > inside.txt" }),
            r#async: false,
            summary: "run sandboxed command".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("inside.txt")).expect("inside file"),
        "ok"
    );
    assert_eq!(
        outcome
            .evidence
            .metadata
            .pointer("/sandbox/runtime")
            .and_then(serde_json::Value::as_str),
        Some("simple_sandbox")
    );
    assert_eq!(
        outcome
            .evidence
            .metadata
            .pointer("/sandbox/checked")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[cfg(not(windows))]
#[test]
fn local_workspace_run_command_honors_timeout_and_cwd() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-command-timeout");
    let subdir = root.join("subdir");
    fs::create_dir_all(&subdir).expect("workspace");
    let registry = with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root)
            .using_local_process_commands()
            .allowing_shell_commands(true),
    ));
    let input = agent_input("command timeout");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({
                "command": "pwd; sleep 1",
                "cwd": "subdir",
                "timeout_ms": 40
            }),
            r#async: false,
            summary: "run timed command".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert_eq!(
        outcome
            .output
            .get("timed_out")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        outcome
            .output
            .get("cwd")
            .and_then(serde_json::Value::as_str),
        Some("subdir")
    );
    assert!(outcome.summary.contains("timed out after 40 ms"));
}

#[cfg(not(windows))]
#[test]
fn local_workspace_run_command_uses_pipefail_when_bash_is_available() {
    if !Path::new("/bin/bash").exists() && !Path::new("/usr/bin/bash").exists() {
        return;
    }
    let root = sinomo_testdir::TestDir::new("sinomo-agent-command-pipefail");
    let registry = with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root)
            .using_local_process_commands()
            .allowing_shell_commands(true),
    ));
    let input = agent_input("command pipefail");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "false | true" }),
            r#async: false,
            summary: "run pipeline".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert_eq!(
        outcome
            .output
            .get("success")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        outcome
            .output
            .get("cwd")
            .and_then(serde_json::Value::as_str),
        Some(".")
    );
}

#[cfg(not(windows))]
#[test]
fn local_workspace_run_command_records_structured_output_and_stream_refs() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-command-structured");
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-command-data");
    let data_dir = SinomoDataDir::new(&data_root);
    let input = agent_input("structured command output");
    let mut config = LocalWorkspaceAgentConfig::new(&root)
        .using_local_process_commands()
        .allowing_shell_commands(true)
        .with_data_dir_evidence(&data_dir, &input.session.session_id);
    config.max_command_output_bytes = 4;
    let registry =
        with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({
                "command": "printf abcdef; printf uvwxyz >&2; exit 2"
            }),
            r#async: false,
            summary: "run structured command".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    let command_output = outcome
        .output
        .get("command_output")
        .expect("command output");
    assert_eq!(
        command_output
            .get("exit_code")
            .and_then(serde_json::Value::as_i64),
        Some(2)
    );
    assert_eq!(
        command_output
            .get("stdout")
            .and_then(serde_json::Value::as_str),
        Some("abcd")
    );
    assert_eq!(
        command_output
            .get("stderr")
            .and_then(serde_json::Value::as_str),
        Some("uvwx")
    );
    assert_eq!(
        command_output
            .get("truncated")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(
        command_output
            .get("duration_ms")
            .and_then(serde_json::Value::as_u64)
            .is_some()
    );
    assert!(
        command_output
            .get("aggregated_output")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("stderr:")
    );
    let stdout_ref = command_output
        .get("stdout_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str)
        .expect("stdout ref");
    let stderr_ref = command_output
        .get("stderr_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str)
        .expect("stderr ref");
    assert!(
        fs::read_to_string(stdout_ref)
            .expect("stdout ref")
            .contains("abcdef")
    );
    assert!(
        fs::read_to_string(stderr_ref)
            .expect("stderr ref")
            .contains("uvwxyz")
    );
    assert!(
        outcome
            .evidence
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "stdout")
    );
    assert!(
        outcome
            .evidence
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "stderr")
    );
}

#[test]
fn local_workspace_registry_blocks_apply_patch_by_default() {
    let registry =
        AgentToolRegistry::local_workspace(std::env::current_dir().expect("current dir"));
    let input = agent_input("patch");
    let call = AgentToolCall {
        call_id: None,
        tool: "apply_patch".to_string(),
        input: serde_json::json!({ "patch": "--- a/file\n+++ b/file\n" }),
        r#async: false,
        summary: "patch".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Blocked);
    assert_eq!(
        outcome.output.get("error").and_then(|value| value.as_str()),
        Some("file_writes_disabled")
    );
    assert!(!outcome.evidence.availability.allowed);
}

#[test]
fn local_workspace_registry_applies_edit_file_when_file_writes_enabled() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-edit-file");
    let evidence_dir = root.join("evidence");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root)
        .allowing_file_writes(true)
        .with_evidence_dir(&evidence_dir);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("edit file");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "edit_file".to_string(),
            input: serde_json::json!({
                "path": "hello.txt",
                "old_text": "old\n",
                "new_text": "new\n"
            }),
            r#async: false,
            summary: "edit file".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("edited file"),
        "new\n"
    );
    assert_eq!(
        outcome
            .output
            .get("format")
            .and_then(serde_json::Value::as_str),
        Some("edit_file")
    );
    assert_eq!(
        coding_evidence(&outcome.evidence).changed_files,
        vec!["hello.txt".to_string()]
    );
    assert!(
        coding_evidence(&outcome.evidence)
            .diff_ref
            .as_deref()
            .is_some_and(|path| Path::new(path).exists())
    );
    assert_eq!(
        outcome
            .evidence
            .metadata
            .pointer("/sandbox/runtime")
            .and_then(serde_json::Value::as_str),
        Some("simple_sandbox")
    );
    assert_eq!(
        outcome
            .evidence
            .metadata
            .pointer("/sandbox/checked")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[test]
fn local_workspace_registry_write_file_requires_overwrite_for_existing_file() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-write-file-overwrite");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("write file");

    let blocked = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "write_file".to_string(),
            input: serde_json::json!({
                "path": "hello.txt",
                "content": "new\n"
            }),
            r#async: false,
            summary: "write file without overwrite".to_string(),
        },
    );

    assert_eq!(blocked.status, AgentToolStatus::Failed);
    assert_eq!(
        blocked
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("target_exists")
    );
    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("unchanged file"),
        "old\n"
    );

    let overwritten = registry.execute(
        &input.session,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "write_file".to_string(),
            input: serde_json::json!({
                "path": "hello.txt",
                "content": "new\n",
                "overwrite": true
            }),
            r#async: false,
            summary: "overwrite file".to_string(),
        },
    );

    assert_eq!(overwritten.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("overwritten file"),
        "new\n"
    );
    assert_eq!(
        overwritten
            .output
            .get("format")
            .and_then(serde_json::Value::as_str),
        Some("write_file")
    );
    assert_eq!(
        coding_evidence(&overwritten.evidence).changed_files,
        vec!["hello.txt".to_string()]
    );
}

#[test]
fn local_workspace_registry_rejects_unified_diff_patches() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-patch");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("patch");
    let patch = "--- a/hello.txt\n+++ b/hello.txt\n@@ -1 +1 @@\n-old\n+new\n";
    let call = AgentToolCall {
        call_id: None,
        tool: "apply_patch".to_string(),
        input: serde_json::json!({ "patch": patch }),
        r#async: false,
        summary: "patch file".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("file"),
        "old\n"
    );
    assert_eq!(
        outcome
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("freeform_patch_parse_failed")
    );
}

#[test]
fn local_workspace_registry_applies_freeform_patch_when_file_writes_enabled() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-freeform-patch");
    fs::write(root.join("old.txt"), "old\n").expect("seed old file");
    fs::write(root.join("multi.txt"), "line1\nline2\nline3\nline4\n").expect("seed multi file");
    fs::write(root.join("move.txt"), "before\n").expect("seed move file");
    fs::write(root.join("delete.txt"), "delete me\n").expect("seed delete file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("freeform patch");
    let patch = r#"*** Begin Patch
*** Add File: nested/added.txt
+hello
+world
*** Update File: old.txt
@@
-old
+new
*** Update File: multi.txt
@@
-line2
+changed2
@@
-line4
+changed4
*** Update File: move.txt
*** Move to: moved/move.txt
@@
-before
+after
*** Delete File: delete.txt
*** End Patch"#;

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "apply freeform patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("nested/added.txt")).expect("added file"),
        "hello\nworld\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("old.txt")).expect("old file"),
        "new\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("multi.txt")).expect("multi file"),
        "line1\nchanged2\nline3\nchanged4\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("moved/move.txt")).expect("moved file"),
        "after\n"
    );
    assert!(!root.join("move.txt").exists());
    assert!(!root.join("delete.txt").exists());
    assert_eq!(
        coding_evidence(&outcome.evidence).changed_files,
        vec![
            "delete.txt".to_string(),
            "move.txt".to_string(),
            "moved/move.txt".to_string(),
            "multi.txt".to_string(),
            "nested/added.txt".to_string(),
            "old.txt".to_string(),
        ]
    );
    assert_eq!(
        outcome
            .output
            .get("format")
            .and_then(serde_json::Value::as_str),
        Some("freeform")
    );
    assert!(
        outcome
            .summary
            .starts_with("Success. Updated the following files:")
    );
    assert!(outcome.summary.contains("\nA nested/added.txt"));
    assert!(outcome.summary.contains("\nM old.txt"));
    assert!(outcome.summary.contains("\nD delete.txt"));
    assert!(
        outcome
            .output
            .get("diff")
            .and_then(|diff| diff.get("files"))
            .and_then(serde_json::Value::as_array)
            .is_some_and(|files| !files.is_empty())
    );
}

#[test]
fn local_workspace_registry_accepts_raw_string_apply_patch_input() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-raw-string-patch");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("raw string patch");
    let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::Value::String(patch.to_string()),
            r#async: false,
            summary: "apply raw string patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("patched file"),
        "new\n"
    );
}

#[test]
fn local_workspace_apply_patch_records_diff_artifact_metadata() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-patch-diff-evidence");
    let evidence_dir = root.join("evidence");
    fs::create_dir_all(&evidence_dir).expect("workspace");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root)
        .allowing_file_writes(true)
        .with_evidence_dir(&evidence_dir);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("diff evidence");
    let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "patch with diff evidence".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert!(outcome.evidence.artifacts.iter().any(|artifact| {
        artifact.kind == "diff"
            && fs::read_to_string(&artifact.uri)
                .expect("diff artifact")
                .contains("+new")
    }));
    let patch_diff = outcome
        .evidence
        .record_value("patch_diff")
        .expect("patch diff record");
    assert_eq!(patch_diff["files"][0]["path"], "hello.txt");
    assert_eq!(patch_diff["files"][0]["status"], "M");
    assert!(
        coding_evidence(&outcome.evidence)
            .diff_ref
            .as_ref()
            .is_some_and(|path| path.ends_with(".diff"))
    );
}

#[test]
fn local_workspace_apply_patch_preflights_all_changes_before_mutating() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-preflight-patch");
    fs::write(root.join("target.txt"), "actual\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("preflight patch");
    let patch = r#"*** Begin Patch
*** Add File: added.txt
+created
*** Update File: target.txt
@@
-expected
+new
*** End Patch"#;

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "preflight patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert!(!root.join("added.txt").exists());
    assert_eq!(
        fs::read_to_string(root.join("target.txt")).expect("target"),
        "actual\n"
    );
    assert!(
        outcome
            .summary
            .starts_with("apply_patch verification failed:")
    );
    assert!(outcome.summary.contains("Closest candidate starts at line"));
}

#[test]
fn local_workspace_apply_patch_preserves_crlf_and_missing_final_newline() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-line-ending-patch");
    fs::write(root.join("crlf.txt"), "old\r\nsecond\r\n").expect("seed crlf");
    fs::write(root.join("nonewline.txt"), "old").expect("seed no newline");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("line endings");
    let patch = r#"*** Begin Patch
*** Update File: crlf.txt
@@
-old
+new
*** Update File: nonewline.txt
@@
-old
+new
*** End Patch"#;

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "line ending patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("crlf.txt")).expect("crlf"),
        "new\r\nsecond\r\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("nonewline.txt")).expect("no newline"),
        "new"
    );
}

#[cfg(unix)]
#[test]
fn local_workspace_apply_patch_preserves_existing_file_mode() {
    use std::os::unix::fs::PermissionsExt;

    let root = sinomo_testdir::TestDir::new("sinomo-agent-mode-patch");
    let script = root.join("script.sh");
    fs::write(&script, "old\n").expect("seed script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("mode patch");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({
                "changes": [{
                    "action": "replace_file",
                    "path": "script.sh",
                    "content": "new\n"
                }]
            }),
            r#async: false,
            summary: "replace script".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::metadata(&script)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
}

#[cfg(unix)]
#[test]
fn local_workspace_apply_patch_rejects_symlink_path_components() {
    use std::os::unix::fs::symlink;

    let root = sinomo_testdir::TestDir::new("sinomo-agent-symlink-patch");
    fs::create_dir_all(root.join("real")).expect("workspace");
    symlink(root.join("real"), root.join("link")).expect("symlink");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("symlink patch");
    let patch = r#"*** Begin Patch
*** Add File: link/new.txt
+created
*** End Patch"#;

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "reject symlink".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert!(!root.join("real/new.txt").exists());
    assert!(outcome.summary.contains("symlink component"));
}

#[test]
fn local_workspace_apply_patch_rejects_malformed_update_without_mutating() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-bad-patch");
    fs::write(root.join("bad.txt"), "old\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("bad patch");
    let patch = r#"*** Begin Patch
*** Update File: bad.txt
@@
old
+new
*** End Patch"#;

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "reject bad patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert_eq!(
        fs::read_to_string(root.join("bad.txt")).expect("bad file"),
        "old\n"
    );
    assert_eq!(
        outcome
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("freeform_patch_parse_failed")
    );
}

#[test]
fn local_workspace_registry_applies_structured_patch_changes() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-structured-patch");
    fs::write(root.join("replace.txt"), "old\n").expect("seed replace file");
    fs::write(root.join("move.txt"), "move me\n").expect("seed move file");
    fs::write(root.join("delete.txt"), "delete me\n").expect("seed delete file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("structured patch");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({
                "changes": [
                    {
                        "action": "add_file",
                        "path": "added.txt",
                        "content": "created\n"
                    },
                    {
                        "action": "update_file",
                        "path": "replace.txt",
                        "content": "new\n"
                    },
                    {
                        "action": "move_file",
                        "path": "move.txt",
                        "new_path": "renamed/move.txt"
                    },
                    {
                        "action": "delete_file",
                        "path": "delete.txt"
                    }
                ]
            }),
            r#async: false,
            summary: "apply structured patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("added.txt")).expect("added file"),
        "created\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("replace.txt")).expect("replace file"),
        "new\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("renamed/move.txt")).expect("moved file"),
        "move me\n"
    );
    assert!(!root.join("move.txt").exists());
    assert!(!root.join("delete.txt").exists());
    assert_eq!(
        outcome
            .output
            .get("format")
            .and_then(serde_json::Value::as_str),
        Some("structured")
    );
}

#[test]
fn local_workspace_apply_patch_supports_structured_exact_and_range_edits() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-structured-small-edits");
    fs::write(root.join("exact.txt"), "fn value() {\n    old_call();\n}\n")
        .expect("seed exact file");
    fs::write(root.join("range.txt"), "one\ntwo\nthree\nfour\n").expect("seed range file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("structured small edits");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({
                "changes": [
                    {
                        "action": "replace_exact",
                        "path": "exact.txt",
                        "old": "old_call();",
                        "new": "new_call();"
                    },
                    {
                        "action": "replace_range",
                        "path": "range.txt",
                        "start_line": 2,
                        "end_line": 3,
                        "content": "TWO\nTHREE\n"
                    }
                ]
            }),
            r#async: false,
            summary: "apply structured small edits".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("exact.txt")).expect("exact file"),
        "fn value() {\n    new_call();\n}\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("range.txt")).expect("range file"),
        "one\nTWO\nTHREE\nfour\n"
    );
    let operations = outcome
        .output
        .get("operations")
        .and_then(serde_json::Value::as_array)
        .expect("operations");
    assert!(
        operations
            .iter()
            .any(|operation| operation["action"] == "replace_exact")
    );
    assert!(
        operations
            .iter()
            .any(|operation| operation["action"] == "replace_range")
    );
}

#[test]
fn local_workspace_apply_patch_rejects_ambiguous_structured_exact_edit_without_mutating() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-structured-ambiguous-edit");
    fs::write(root.join("ambiguous.txt"), "same\nsame\n").expect("seed ambiguous file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("ambiguous structured edit");

    let outcome = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({
                "changes": [
                    {
                        "action": "replace_exact",
                        "path": "ambiguous.txt",
                        "old": "same",
                        "new": "changed"
                    }
                ]
            }),
            r#async: false,
            summary: "reject ambiguous structured edit".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Failed);
    assert_eq!(
        fs::read_to_string(root.join("ambiguous.txt")).expect("ambiguous file"),
        "same\nsame\n"
    );
    assert!(
        outcome
            .summary
            .contains("replace_exact expected one match for `ambiguous.txt`, found 2")
    );
}

#[test]
fn tool_search_loads_hidden_tools_for_the_next_model_step() {
    let model = SequencedModel::new([
        r#"{"response_mode":"defer","text":"Find a reader","tool_calls":[{"tool":"tool_search","input":{"query":"read_file","limit":1},"async":false,"summary":"discover"}],"next_state":"waiting"}"#,
        r#"{"response_mode":"defer","text":"Read","tool_calls":[{"tool":"read_file","input":{"path":"Cargo.toml"},"async":false,"summary":"read"}],"next_state":"waiting"}"#,
        r#"{"response_mode":"answer","text":"Done","tool_calls":[],"next_state":"listening"}"#,
    ]);
    let mut config = AgentConfig::new("fake", "fake");
    config.visible_toolsets = vec!["discovery".into()];
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));
    let run = agent.run(&agent_input("find and read")).unwrap();
    assert_eq!(run.tool_results.len(), 2);
    assert!(
        run.tool_results
            .iter()
            .all(|outcome| outcome.status == AgentToolStatus::Succeeded)
    );
    assert_eq!(run.tool_results[0].output["count"], 1);
    let requests = agent.model.requests.lock().unwrap();
    assert!(
        !requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == "read_file")
    );
    assert!(
        requests[1]
            .tools
            .iter()
            .any(|tool| tool.name == "read_file")
    );
}

#[test]
fn tool_discovery_uses_current_registry_and_keeps_surface_denials() {
    let model = SequencedModel::new(std::iter::empty::<String>());
    let mut config = AgentConfig::new("fake", "fake");
    config.visible_toolsets = vec!["discovery".into()];
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));
    let input = agent_input("discover");
    let receipt = serde_json::json!({"results":[{"status":"succeeded","call":{"tool":"tool_search"},"output":{"matches":[{"name":"read_file"},{"name":"unregistered_tool"}]}}]}).to_string();
    let user = AgentTranscriptItem::user(0, receipt.clone());
    assert!(
        !agent
            .tool_plan_with_discovery(&input, &[user])
            .is_model_visible("read_file")
    );
    let transcript = vec![AgentTranscriptItem::tool(0, TaskId::generate(), receipt)];
    let plan = agent.tool_plan_with_discovery(&input, &transcript);
    assert!(plan.is_model_visible("read_file"));
    assert!(!plan.is_model_visible("unregistered_tool"));
    let surface =
        AgentToolSurface::default_for_role(AgentRole::Main).with_denied_tools(["read_file"]);
    let context = AgentRunContext::default();
    let outcome = AgentToolOrchestrator::new(&surface, None, &context).execute(
        &agent.tools,
        &input.session,
        &plan,
        1,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "read_file".into(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "read".into(),
        },
    );
    assert_eq!(outcome.status, AgentToolStatus::Blocked);
    let mut unloaded = agent.tool_plan(&input);
    unloaded.discovered_tools = plan.discovered_tools;
    unloaded
        .entries
        .retain(|entry| entry.descriptor.spec.name != "read_file");
    assert!(!unloaded.is_model_visible("read_file"));
}

#[test]
fn str_replace_editor_uses_guarded_workspace_transactions() {
    let root = sinomo_testdir::TestDir::new("deepseek-editor");
    let registry = AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true),
    );
    let input = agent_input("edit");
    let run = |args| {
        registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "str_replace_editor".into(),
                input: args,
                r#async: false,
                summary: "edit".into(),
            },
        )
    };
    let create = serde_json::json!({"command":"create", "path":root.join("file.txt"), "file_text":"one\ntwo\n", "insert_line":null});
    assert_eq!(run(create.clone()).status, AgentToolStatus::Succeeded);
    assert_ne!(run(create).status, AgentToolStatus::Succeeded);
    assert_eq!(run(serde_json::json!({"command":"str_replace","path":"file.txt","old_str":"two","new_str":"second"})).status, AgentToolStatus::Succeeded);
    assert_eq!(run(serde_json::json!({"command":"insert","path":"file.txt","insert_line":1,"new_str":"middle"})).status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("file.txt")).unwrap(),
        "one\nmiddle\nsecond\n"
    );
    let viewed = run(
        serde_json::json!({"command":"view","path":"file.txt","view_range":[2,-1],"file_text":null}),
    );
    assert_eq!(viewed.status, AgentToolStatus::Succeeded);
    assert!(
        viewed.output["content"]
            .as_str()
            .unwrap()
            .starts_with("     2\tmiddle")
    );
    assert_ne!(run(serde_json::json!({"command":"insert","path":"file.txt","insert_line":999,"new_str":"bad"})).status, AgentToolStatus::Succeeded);
    fs::write(root.join("repeat.txt"), "same\nsame\n").unwrap();
    assert_ne!(run(serde_json::json!({"command":"str_replace","path":"repeat.txt","old_str":"same","new_str":"bad"})).status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("repeat.txt")).unwrap(),
        "same\nsame\n"
    );
    assert_ne!(run(serde_json::json!({"command":"str_replace","path":"file.txt","old_str":"one","new_str":null})).status, AgentToolStatus::Succeeded);
    assert_eq!(
        run(serde_json::json!({"command":"str_replace","path":"file.txt","old_str":"one"})).status,
        AgentToolStatus::Succeeded
    );
    fs::write(root.join("empty.txt"), "").unwrap();
    assert_eq!(run(serde_json::json!({"command":"insert","path":"empty.txt","insert_line":0,"new_str":"first"})).status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("empty.txt")).unwrap(),
        "first\n"
    );
}

#[test]
fn str_replace_editor_keeps_write_and_path_boundaries() {
    let root = sinomo_testdir::TestDir::new("deepseek-editor-boundaries");
    let outside = sinomo_testdir::TestDir::new("deepseek-editor-outside");
    fs::write(outside.join("secret"), "private").unwrap();
    fs::write(root.join("file"), "original").unwrap();
    let input = agent_input("view");
    for writable in [false, true] {
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(writable),
        );
        let run = |args| {
            registry.execute(
                &input.session,
                0,
                0,
                &AgentToolCall {
                    call_id: None,
                    tool: "str_replace_editor".into(),
                    input: args,
                    r#async: false,
                    summary: "edit".into(),
                },
            )
        };
        assert_eq!(
            run(serde_json::json!({"command":"view","path":outside.join("secret")})).status,
            AgentToolStatus::Blocked
        );
        assert_ne!(
            run(
                serde_json::json!({"command":"create","path":outside.join("new"),"file_text":"bad"})
            )
            .status,
            AgentToolStatus::Succeeded
        );
        if !writable {
            assert_eq!(run(serde_json::json!({"command":"insert","path":"file","insert_line":0,"new_str":"bad"})).status, AgentToolStatus::Blocked);
            assert_eq!(
                run(serde_json::json!({"command":"view","path":"file"})).status,
                AgentToolStatus::Succeeded
            );
        }
    }
    assert_eq!(fs::read_to_string(root.join("file")).unwrap(), "original");
    assert!(!outside.join("new").exists());
}

#[test]
fn str_replace_editor_is_exposed_for_deepseek_and_lists_two_directory_levels() {
    let root = sinomo_testdir::TestDir::new("deepseek-editor-directory");
    fs::create_dir_all(root.join("a/b")).unwrap();
    fs::write(root.join("a/b/too-deep"), "hidden").unwrap();
    fs::write(root.join(".hidden"), "hidden").unwrap();
    let registry =
        AgentToolRegistry::local_workspace_with_config(LocalWorkspaceAgentConfig::new(&root));
    let input = agent_input("inspect");
    let result = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "str_replace_editor".into(),
            input: serde_json::json!({"command":"view","path":"."}),
            r#async: false,
            summary: "view".into(),
        },
    );
    assert_eq!(result.status, AgentToolStatus::Succeeded);
    assert_eq!(result.output["entries"], serde_json::json!(["a", "a/b"]));
    for provider in ["deepseek", "openai"] {
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            provider,
            "provider-default",
        )
        .with_config(AgentConfig::new(provider, "provider-default"))
        .with_tool_registry(registry.clone());
        assert_eq!(
            agent
                .tool_plan(&input)
                .is_model_visible("str_replace_editor"),
            provider == "deepseek"
        );
    }
}
