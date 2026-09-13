use super::*;

#[test]
fn agent_config_defaults_to_model_derived_context_bounds() {
    let config = AgentConfig::new("fake", "fake");

    assert_eq!(config.definition.name, "main-agent");
    assert_eq!(config.definition.role, AgentRole::Main);
    assert_eq!(config.max_steps, None);
    assert_eq!(config.max_tool_calls_per_step, None);
    assert_eq!(config.max_model_repair_attempts, None);
    assert_eq!(config.temperature, None);
    assert_eq!(config.definition.model_policy.temperature, None);
    assert_eq!(config.max_output_tokens, Some(1600));
    assert_eq!(
        config.instruction_file_max_bytes,
        Some(DEFAULT_WORKSPACE_INSTRUCTION_FILE_BYTES)
    );
    assert!(config.expand_prompt_references);
    assert_eq!(
        config.prompt_reference_max_bytes,
        Some(DEFAULT_PROMPT_REFERENCE_FILE_BYTES)
    );
    assert_eq!(
        config.prompt_reference_max_entries,
        Some(DEFAULT_PROMPT_REFERENCE_DIRECTORY_ENTRIES)
    );
    assert_eq!(
        config.context_attachment_max_bytes,
        Some(DEFAULT_CONTEXT_ATTACHMENT_PAYLOAD_BYTES)
    );
    assert_eq!(
        config.async_update_max_bytes,
        Some(DEFAULT_ASYNC_UPDATE_BYTES)
    );
    assert_eq!(
        config.context_budget.max_context_bytes,
        Some((DEFAULT_UNKNOWN_MODEL_CONTEXT_WINDOW_TOKENS - 1600) * 4)
    );
    assert!(config.context_budget.compaction_threshold_bytes().is_some());
}

#[test]
fn unknown_model_context_budget_fully_reserves_requested_output() {
    let budget =
        AgentContextBudget::for_loaded_model("unknown-provider", "unknown-model", Some(30_000));

    assert_eq!(
        budget.max_context_bytes,
        Some((DEFAULT_UNKNOWN_MODEL_CONTEXT_WINDOW_TOKENS - 30_000) * 4)
    );
}

#[test]
fn agent_normalizes_legacy_unbounded_context_config_before_execution() {
    let mut config = AgentConfig::new("fake", "fake");
    config.context_budget = AgentContextBudget::default();

    let agent =
        Agent::new(SequencedModel::new(Vec::<String>::new()), "fake", "fake").with_config(config);

    assert!(agent.config.context_budget.max_context_bytes.is_some());
    assert!(
        agent
            .config
            .context_budget
            .compaction_threshold_bytes()
            .is_some()
    );
}

#[test]
fn agent_run_context_reaches_model_metadata_and_events() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-run-context");
    let task = AgentTaskRef::new("coding", "fix run context propagation")
        .with_owner_role(AgentRole::Deliberation);
    let placement = AgentPlacementMetadata::local_current_process()
        .with_origin_node("node.origin")
        .with_sandbox_class("local-process");
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Context recorded.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let config = AgentConfig::new("fake", "fake")
        .with_workspace_root(&root)
        .with_task_ref(task.clone())
        .with_placement(placement);
    let agent = Agent::new(model, "fake", "fake").with_config(config);
    let input = agent_input("record context");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Main);

    let outcome = agent
        .handle_op(&mut session, AgentOp::start_turn(input))
        .expect("op");
    let run = outcome.run.as_ref().expect("run");

    assert_eq!(
        run.run_context.task.as_ref().map(|task| &task.task_id),
        Some(&task.task_id)
    );
    assert!(matches!(
        &outcome.events[0].payload,
        AgentEventPayload::TurnStarted {
            role: AgentRole::Main,
            surface_id,
            run_context,
            ..
        } if surface_id == "main-default"
            && run_context.task.as_ref().map(|task| &task.task_id) == Some(&task.task_id)
    ));
    assert!(matches!(
        &outcome.events[1].payload,
        AgentEventPayload::TurnCompleted {
            run_context,
            emitted_event_ids,
            ..
        } if run_context.task.as_ref().map(|task| &task.task_id) == Some(&task.task_id)
            && !emitted_event_ids.is_empty()
    ));

    let request = agent.model.requests.lock().expect("requests")[0].clone();
    assert_eq!(request.metadata["run_context"]["task"]["kind"], "coding");
    assert_eq!(
        request.metadata["run_context"]["placement"]["origin_node"],
        "node.origin"
    );
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("\"run_context\""));
    assert!(request_text.contains("fix run context propagation"));
    assert!(request_text.contains("local-process"));
    assert_eq!(
        run.events[0].payload["run_context"]["task"]["kind"],
        "coding"
    );
}

#[test]
fn custom_agent_definition_controls_context_role_prompt_and_default_tools() {
    let mut input = agent_input("draft docs");
    input.tool_specs = Vec::new();
    let definition = AgentDefinition::custom(
        "docs-agent",
        AgentRole::Background,
        "Draft operator docs from visible evidence.",
    )
    .with_tool(AgentToolSpec::new(
        AgentToolKind::ReadFile,
        "draft_doc",
        "Draft a documentation note from current evidence.",
        false,
    ));
    let config = AgentConfig::new("fake", "fake").with_agent_definition(definition);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");
    let request = agent.request(&input, &[], 0).expect("request");
    let visible_tools: Vec<_> = context
        .available_tool_specs
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect();

    assert_eq!(context.role, AgentRole::Background);
    assert!(context.system_instructions.contains("Draft operator docs"));
    assert!(!context.system_instructions.contains("Return only JSON"));
    assert!(
        !context
            .system_instructions
            .contains("Available agent tools")
    );
    assert!(!visible_tools.contains(&"draft_doc"));
    assert!(!visible_tools.contains(&"read_file"));
    assert!(
        context
            .report
            .omitted
            .iter()
            .any(|omission| omission.kind == "tool_readiness"
                && omission.label == "draft_doc"
                && omission.reason.contains("no runtime executor"))
    );
    assert_eq!(
        request.response_format,
        LanguageModelResponseFormat::JsonObject
    );
    assert!(
        request.messages[0]
            .content
            .contains("Draft operator docs from visible evidence")
    );
    assert!(request.messages[0].content.contains("Response contract:"));
    assert!(
        request.messages[0]
            .content
            .contains("Return only JSON shaped as")
    );
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("\"runtime\""));
    assert!(request_text.contains("\"provider\": \"fake\""));
    assert!(request_text.contains("\"agent_output_format\": \"json_decision\""));
    assert_eq!(
        request
            .metadata
            .get("agent_name")
            .and_then(serde_json::Value::as_str),
        Some("docs-agent")
    );
    assert_eq!(
        request
            .metadata
            .get("agent_role")
            .and_then(serde_json::Value::as_str),
        Some("background")
    );
}

#[test]
fn context_budget_can_be_tightened_from_loaded_model_metadata() {
    let large_context = AgentContextBudget::for_loaded_model("openai", "custom-1m", Some(1_200));

    let max_context_bytes = large_context.max_context_bytes.expect("context bytes");
    assert!(max_context_bytes > 500 * 1024);
    assert_eq!(
        large_context.compaction_threshold_ratio,
        Some(DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO)
    );
    assert_eq!(
        large_context.compaction_threshold_bytes(),
        Some(
            (max_context_bytes as f32 * DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO).round()
                as usize
        )
    );

    let custom = AgentContextBudget {
        max_context_bytes: Some(10_000),
        compaction_threshold_ratio: Some(0.5),
        retained_tail_items: Some(8),
    };
    assert_eq!(custom.compaction_threshold_bytes(), Some(5_000));

    let unknown = AgentContextBudget::for_loaded_model("unknown", "unknown", Some(1_600));
    assert_eq!(
        unknown.max_context_bytes,
        Some((DEFAULT_UNKNOWN_MODEL_CONTEXT_WINDOW_TOKENS - 1_600) * 4)
    );
}

#[test]
fn agent_records_context_items_model_events_and_tool_evidence() {
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
            "text": "The workspace evidence is available.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

    let run = agent.run(&agent_input("inspect the repo")).expect("run");

    assert_eq!(run.context_reports.len(), 2);
    assert_eq!(run.model_turns[0].events.len(), 5);
    assert!(run.model_events.iter().any(|event| {
        matches!(
            event,
            AgentModelEvent::ToolCallCompleted { call, .. }
                if call.tool == "read_file"
        )
    }));
    assert!(
        run.items
            .iter()
            .any(|item| matches!(item, AgentItem::ContextInjection { .. }))
    );
    assert!(run.items.iter().any(
        |item| matches!(item, AgentItem::ToolResult { evidence, .. } if evidence.availability.allowed)
    ));
    assert!(run.tool_results[0].evidence.availability.allowed);
    assert!(
        agent.model.requests.lock().expect("requests")[0].metadata["context_report_id"]
            .as_str()
            .is_some()
    );
}

#[test]
fn agent_emits_context_assembled_to_model_progress_sink() {
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let agent = Agent::new(model, "fake", "fake");
    let input = agent_input("show context");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Main);
    let events = Arc::new(Mutex::new(Vec::<AgentModelEvent>::new()));
    let sink = {
        let events = Arc::clone(&events);
        AgentModelProgressSink::new(move |event| {
            events.lock().expect("model progress lock").push(event);
        })
    };

    let run = agent
        .start_turn_with_cancellation_and_progress(
            &mut session,
            &input,
            AgentCancellationToken::new(),
            AgentToolProgressSink::default(),
            sink,
        )
        .expect("run");

    let progress_events = events.lock().expect("model progress lock");
    let context_events = progress_events
        .iter()
        .filter_map(|event| match event {
            AgentModelEvent::ContextAssembled { step_index, report } => {
                Some((*step_index, report.report_id.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        context_events,
        vec![(0, run.context_reports[0].report_id.clone())]
    );
}

#[test]
fn file_trace_store_records_context_frames_and_deltas() {
    let trace_root = sinomo_testdir::TestDir::new("sinomo-agent-context-frame-trace");
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
    let frames = records
        .iter()
        .filter_map(|record| match &record.record {
            AgentTraceRecordKind::ContextFrame { frame, delta } => Some((frame, delta)),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[1].1.from_frame_id.as_deref(),
        Some(frames[0].0.frame_id.as_str())
    );
    assert!(frames[1].1.cache_reuse_prefix_bytes > 0);
    assert!(
        frames[1]
            .1
            .kept
            .iter()
            .any(|block| block.kind == ContextBlockKind::SystemInstructions)
    );
    assert!(
        frames[1]
            .0
            .blocks
            .iter()
            .any(|block| block.kind == ContextBlockKind::ToolSurface)
    );
    assert!(records.iter().any(|record| {
        matches!(
            &record.record,
            AgentTraceRecordKind::ContextReport {
                report
            } if report.context_frame_id.is_some()
        )
    }));
}

#[test]
fn transient_system_context_stays_out_of_two_turn_persisted_session() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-transient-turn-context");
    let data_dir = SinomoDataDir::new(&data_root);
    let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
    let model = SequencedModel::new([
        r#"{
            "response_mode": "answer",
            "text": "First channel reply.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "Second channel reply.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake").with_session_store(store.clone());
    let mut first_input = agent_input("");
    first_input.transient_system_context =
        Some("CHANNEL_CONTROL_TURN_ONE: reply only when useful".to_string());
    let mut session = AgentSession::new(first_input.session.session_id.clone(), AgentRole::Main);

    let first_visible = AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "User: first visible channel message".to_string(),
        event_id: None,
    };
    session.transcript.push(first_visible.clone());
    session.raw_transcript.push(first_visible);
    agent
        .start_turn(&mut session, &first_input)
        .expect("first transient channel turn");

    let second_visible = AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 1,
        content: "User: second visible channel message".to_string(),
        event_id: None,
    };
    session.transcript.push(second_visible.clone());
    session.raw_transcript.push(second_visible);
    let mut second_input = agent_input("");
    second_input.transient_system_context =
        Some("CHANNEL_CONTROL_TURN_TWO: stay concise".to_string());
    agent
        .start_turn(&mut session, &second_input)
        .expect("second transient channel turn");

    let loaded = store
        .load_session(&first_input.session.session_id)
        .expect("load persisted session")
        .expect("persisted session");
    for transcript in [&loaded.transcript, &loaded.raw_transcript] {
        let user_messages = transcript
            .items
            .iter()
            .filter_map(|item| match item {
                AgentItem::UserMessage { content, .. } => Some(content.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            user_messages,
            vec![
                "User: first visible channel message",
                "User: second visible channel message"
            ],
            "each visible channel message must have exactly one durable representation"
        );
        assert!(
            transcript.items.iter().all(|item| {
                !serde_json::to_string(item)
                    .expect("serialize transcript item")
                    .contains("CHANNEL_CONTROL_TURN_")
            }),
            "transient channel control context must not accumulate in persisted prompts"
        );
    }

    let requests = agent.model.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    let system_message = |request: &LanguageModelRequest| {
        request
            .messages
            .iter()
            .find(|message| message.role == LanguageModelRole::System)
            .map(|message| message.content.clone())
            .expect("system message")
    };
    assert!(system_message(&requests[0]).contains("CHANNEL_CONTROL_TURN_ONE"));
    assert!(system_message(&requests[1]).contains("CHANNEL_CONTROL_TURN_TWO"));
    assert!(!system_message(&requests[1]).contains("CHANNEL_CONTROL_TURN_ONE"));
}

#[test]
fn file_session_store_records_items_and_projections_append_only() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session-projection");
    let data_dir = SinomoDataDir::new(&data_root);
    let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let agent = Agent::new(model, "fake", "fake")
        .with_session_store(store.clone())
        .with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        });
    let input = agent_input("persist projection");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

    agent.start_turn(&mut session, &input).expect("run");
    agent
        .compact(&mut session, "persist compacted projection")
        .expect("compact");

    let loaded = store
        .load_session(&input.session.session_id)
        .expect("load session")
        .expect("saved session");
    assert!(loaded.projection.is_some());
    assert!(
        loaded.raw_transcript.items.len() > loaded.transcript.items.len(),
        "raw transcript should retain pre-compaction history"
    );

    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    let events =
        fs::read_to_string(paths.session_events_path_for_session(&input.session.session_id))
            .expect("session events");
    let items = fs::read_to_string(paths.session_items_path_for_session(&input.session.session_id))
        .expect("session items");
    let projections =
        fs::read_to_string(paths.session_projections_path_for_session(&input.session.session_id))
            .expect("session projections");
    assert!(events.contains("\"items_appended\""));
    assert!(events.contains("\"projection_saved\""));
    assert!(items.contains("\"compaction_summary\""));
    assert!(projections.contains("\"working_state\""));
}

#[test]
fn explicit_skill_context_loads_standard_instructions_and_resource_names() {
    let markdown = "---\nname: explicit-shortcut\ndescription: Explicitly selected workflow.\n---\n\n# Explicit shortcut\nFollow the explicit workflow.\n";
    let resource = crate::AgentSkillPackageFile {
        relative_path: "references/checklist.md".to_string(),
        bytes: b"# Checklist\nVerify the result.\n".to_vec(),
    };
    let write = crate::SkillCatalogWrite::from_agent_skill_package(
        None,
        "1.0.0",
        crate::SkillCatalogSource::user("user.test"),
        crate::AgentSkillPackage {
            markdown: markdown.to_string(),
            files: vec![resource.clone()],
        },
    )
    .expect("standard package");
    let entry = write.entry;
    let key = crate::skill_catalog_entry_text_key(&entry);
    let mut catalog = crate::SkillCatalog::default();
    catalog.upsert_entry(entry.clone()).expect("catalog entry");
    catalog
        .set_activation(crate::SkillActivationRecord::new(
            crate::SkillActivationScope::Cluster.as_str(),
            &entry,
            "test",
            "cluster default",
        ))
        .expect("activation");
    let config = AgentConfig::new("fake", "fake")
        .with_agent_definition(AgentDefinition::main())
        .with_skill_catalog(catalog)
        .with_skill_catalog_texts([(key.clone(), markdown.to_string())])
        .with_skill_catalog_resources([(key, vec![resource])]);
    let agent = Agent::new(SequencedModel::new(["{}"]), "fake", "fake").with_config(config);

    let context = agent
        .load_explicit_skill_context("explicit-shortcut")
        .expect("explicit skill context");

    assert_eq!(context.name, "explicit-shortcut");
    assert_eq!(context.description, "Explicitly selected workflow.");
    assert!(
        context
            .instructions
            .contains("Follow the explicit workflow.")
    );
    assert_eq!(context.resources, vec!["references/checklist.md"]);
    assert!(
        agent
            .load_explicit_skill_context("missing-shortcut")
            .expect_err("unknown skill")
            .to_string()
            .contains("not available")
    );
}

#[test]
fn module_tool_projection_uses_active_loader_state() {
    let module_id = TaskId::generate();
    let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
        module_id.clone(),
        "0.1.0",
        "module-test",
        "sinomo-module-test",
        "module-test-v1",
        sinomo_runtime::core::TrustRing::R1TrustedRuntime,
        "module.test",
    )
    .with_surfaces(
        sinomo_runtime::module::ModuleSurfaces::default().with_object_rpc_route(
            sinomo_runtime::module::ModuleObjectRpcRoute::new(
                "/module/test/status",
                "module-test",
                "status",
                sinomo_runtime::module::ModuleRouteAuth::LocalOrClusterMember,
            ),
        ),
    );
    let projection = AgentModuleToolProjection::new()
        .with_registration(registration)
        .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
            module_id: module_id.clone(),
            active_artifact_id: TaskId::generate(),
            previous_artifact_id: None,
            shadow_artifact_id: None,
            candidate_artifact_id: None,
            hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
        });
    let plan = AgentToolPlanner::plan(
        std::iter::empty::<AgentToolDescriptor>(),
        &projection,
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        0,
    );

    let projected = plan
        .entries
        .iter()
        .find(|entry| entry.descriptor.spec.name == "module_rpc_module_test_status")
        .expect("module route descriptor");
    assert_eq!(projected.source, crate::tools::AgentToolPlanSource::Module);
    assert!(!projected.executable);
    assert!(!plan.is_model_visible("module_rpc_module_test_status"));

    let registry = AgentToolRegistry::new();
    let input = agent_input("find module route");
    let policy = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let run_context = AgentRunContext::default();
    let orchestrator = AgentToolOrchestrator::new(&policy, None, &run_context);
    let search = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "tool_search".to_string(),
            input: serde_json::json!({ "query": "status" }),
            r#async: false,
            summary: "search modules".to_string(),
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
                |entry| entry.get("name").and_then(serde_json::Value::as_str)
                    == Some("module_rpc_module_test_status")
            )
    );
}

#[test]
fn module_tool_projection_exposes_network_cluster_health_tool() {
    let registration = sinomo_runtime::network_module::network_module_registration();
    let module_id = registration.manifest.module_id.clone();
    let projection = AgentModuleToolProjection::new()
        .with_registration(registration)
        .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
            module_id,
            active_artifact_id: TaskId::generate(),
            previous_artifact_id: None,
            shadow_artifact_id: None,
            candidate_artifact_id: None,
            hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
        });
    let plan = AgentToolPlanner::plan(
        std::iter::empty::<AgentToolDescriptor>(),
        &projection,
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["object_rpc"],
        0,
    );

    let projected = plan
        .entries
        .iter()
        .find(|entry| entry.descriptor.spec.name == "module_rpc_cluster_health_cluster_health_read")
        .expect("network cluster health module route descriptor");
    assert_eq!(projected.source, crate::tools::AgentToolPlanSource::Module);
    assert!(!projected.executable);
    assert!(plan.is_model_visible("module_rpc_cluster_health_cluster_health_read"));
    assert!(
        projected
            .descriptor
            .spec
            .description
            .contains(sinomo_runtime::network::CLUSTER_HEALTH_ROUTE)
    );
    assert!(
        projected
            .descriptor
            .toolsets
            .contains(&sinomo_runtime::network::SINOMO_NETWORK_MODULE_OWNER.to_string())
    );
    assert!(
        projected
            .descriptor
            .capabilities
            .contains(&AgentToolCapability::CallModule)
    );
}

#[test]
fn module_tool_projection_exposes_native_dylib_declared_tools() {
    let module_id = sinomo_runtime::module::ModuleId::from_legacy_named("module.dynamic-native")
        .expect("module id");
    let state = sinomo_runtime::module::NativeDylibModuleLoaderState {
        module_id: module_id.clone(),
        artifact_id: Some(
            sinomo_runtime::module::ArtifactId::from_legacy_named("artifact.dynamic-native.test")
                .expect("artifact id"),
        ),
        content_hash: "blake3:abc".to_string(),
        replaces_content_hash: None,
        cache_key: "module/dynamic-native".to_string(),
        library_name: "libdynamic_native_module.so".to_string(),
        platform_triple: "x86_64-linux-unix".to_string(),
        native_abi_version: sinomo_runtime::module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
        interface_version: "sinomo.dynamic-native.v1".to_string(),
        activated_at: "2026-06-11T00:00:00Z".to_string(),
        source: "test".to_string(),
    };
    let descriptor = sinomo_runtime::module_api::NativeModuleDescriptor {
        module_id: module_id.as_str().to_string(),
        module_version: "0.1.0".to_string(),
        owner: "dynamic-native".to_string(),
        native_abi_version: sinomo_runtime::module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: sinomo_runtime::module_api::NativeModuleBackend::NativeDylib,
        interface_version: "sinomo.dynamic-native.v1".to_string(),
        platform_triple: "x86_64-linux-unix".to_string(),
        supported_operations: vec![
            "dynamic.native.inspect".to_string(),
            "dynamic.native.apply".to_string(),
        ],
        capabilities: vec![
            "dynamic.native.inspect".to_string(),
            "dynamic.native.apply".to_string(),
        ],
        surfaces: sinomo_runtime::module_api::NativeModuleSurfaces {
            object_rpc_routes: vec![
                sinomo_runtime::module_api::NativeModuleObjectRpcRoute {
                    route: "/dynamic-native/inspect".to_string(),
                    category: "dynamic.native".to_string(),
                    operation: "dynamic.native.inspect".to_string(),
                    auth: "admin".to_string(),
                    request_body_mode: sinomo_runtime::module_api::NativeModuleBodyMode::None,
                    response_body_mode: sinomo_runtime::module_api::NativeModuleBodyMode::None,
                    required_local_permissions: Vec::new(),
                    exact_member_identity: false,
                    concurrency_limit: None,
                    timeout_ms: None,
                },
                sinomo_runtime::module_api::NativeModuleObjectRpcRoute {
                    route: "/dynamic-native/apply".to_string(),
                    category: "dynamic.native".to_string(),
                    operation: "dynamic.native.apply".to_string(),
                    auth: "admin".to_string(),
                    request_body_mode: sinomo_runtime::module_api::NativeModuleBodyMode::None,
                    response_body_mode: sinomo_runtime::module_api::NativeModuleBodyMode::None,
                    required_local_permissions: Vec::new(),
                    exact_member_identity: false,
                    concurrency_limit: None,
                    timeout_ms: None,
                },
            ],
            capability_exports: vec![
                "dynamic.native.inspect".to_string(),
                "dynamic.native.apply".to_string(),
            ],
            ..Default::default()
        },
        component: Default::default(),
    };
    let registration =
        sinomo_runtime::module::native_dylib_module_registration_from_state_and_descriptor(
            &state,
            &descriptor,
        )
        .expect("native dylib registration");
    let module_id = registration.manifest.module_id.clone();
    let projection = AgentModuleToolProjection::new()
        .with_registration(registration)
        .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
            module_id,
            active_artifact_id: TaskId::generate(),
            previous_artifact_id: None,
            shadow_artifact_id: None,
            candidate_artifact_id: None,
            hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
        });
    let plan = AgentToolPlanner::plan(
        std::iter::empty::<AgentToolDescriptor>(),
        &projection,
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["object_rpc"],
        0,
    );

    let system_info = plan
        .entries
        .iter()
        .find(|entry| {
            entry.descriptor.spec.name == "module_rpc_dynamic_native_dynamic_native_inspect"
        })
        .expect("native dylib inspect tool");
    let shell_run = plan
        .entries
        .iter()
        .find(|entry| {
            entry.descriptor.spec.name == "module_rpc_dynamic_native_dynamic_native_apply"
        })
        .expect("native dylib apply tool");

    assert_eq!(
        system_info.source,
        crate::tools::AgentToolPlanSource::Module
    );
    assert!(plan.is_model_visible(&system_info.descriptor.spec.name));
    assert!(plan.is_model_visible(&shell_run.descriptor.spec.name));
    assert!(
        system_info
            .descriptor
            .spec
            .description
            .contains("/dynamic-native/inspect")
    );
    assert!(
        system_info
            .descriptor
            .toolsets
            .contains(&"dynamic-native".to_string())
    );
}

#[test]
fn render_tool_results_keeps_large_evidence_out_of_model_context() {
    let outcome = AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index: 3,
        call_index: 0,
        call: AgentToolCall {
            call_id: None,
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": "*** Begin Patch\n*** End Patch" }),
            r#async: false,
            summary: "patch".to_string(),
        },
        status: AgentToolStatus::Succeeded,
        summary: "Success. Updated the following files:\nM hello.txt".to_string(),
        output: serde_json::json!({
            "changed_files": ["hello.txt"],
            "diff": {
                "files": [{
                    "status": "M",
                    "path": "hello.txt",
                    "added_lines": 1,
                    "removed_lines": 1,
                    "diff": "large diff body that should not return to the model"
                }],
                "unified": "large unified diff body that should not return to the model"
            }
        }),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(true, "allowed")
            .with_artifact(AgentToolArtifactRef::new(
                "diff",
                "/tmp/sinomo-agent/evidence/apply-patch.diff",
            ))
            .with_record(
                AGENT_CODING_EVIDENCE_KIND,
                &AgentCodingToolEvidence {
                    patch_ref: Some("/tmp/sinomo-agent/evidence/apply-patch.patch".to_string()),
                    diff_ref: Some("/tmp/sinomo-agent/evidence/apply-patch.diff".to_string()),
                    diff_summary: Some("M hello.txt".to_string()),
                    changed_files: vec!["hello.txt".to_string()],
                    verification_state: AgentVerificationState::NotRun,
                },
            )
            .with_record_value(
                "patch_diff",
                serde_json::json!({
                    "files": [{
                        "status": "M",
                        "path": "hello.txt",
                        "added_lines": 1,
                        "removed_lines": 1,
                        "diff": "large patch evidence diff that should not return to the model"
                    }]
                }),
            )
            .with_metadata(serde_json::json!({
                "workspace_root": "/tmp/sinomo-agent/workspace"
            })),
    };

    let rendered = render_tool_results(3, &[outcome]).expect("render");

    assert!(rendered.contains("\"hello.txt\""));
    assert!(rendered.contains("\"content_omitted\": true"));
    assert!(rendered.contains("\"uri_omitted\": true"));
    assert!(rendered.contains("\"metadata_omitted\""));
    assert!(
        !rendered.contains("/tmp/sinomo-agent/evidence"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("/tmp/sinomo-agent/workspace"),
        "{rendered}"
    );
    assert!(!rendered.contains("large diff body"));
    assert!(!rendered.contains("large unified diff body"));
    assert!(!rendered.contains("large patch evidence diff"));
}

#[test]
fn render_tool_results_bounds_read_file_content_in_model_context() {
    let tail = "READ_FILE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let content = format!(
        "{}{tail}{}",
        "r".repeat(MODEL_VISIBLE_READ_FILE_CONTENT_BYTES + 512),
        "R".repeat(1024)
    );
    let outcome = AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index: 4,
        call_index: 0,
        call: AgentToolCall {
            call_id: None,
            tool: "read_file".to_string(),
            input: serde_json::json!({ "path": "large.txt" }),
            r#async: false,
            summary: "read large file".to_string(),
        },
        status: AgentToolStatus::Succeeded,
        summary: "read large.txt".to_string(),
        output: serde_json::json!({
            "path": "large.txt",
            "bytes": content.len(),
            "content_bytes": content.len(),
            "truncated": false,
            "content": content,
            "total_lines": 1,
            "line_range": {
                "requested_start_line": null,
                "requested_end_line": null,
                "returned_start_line": 1,
                "returned_end_line": 1,
            },
        }),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(true, "allowed"),
    };
    assert!(
        outcome
            .output
            .get("content")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.contains(tail))
    );

    let rendered = render_tool_results(4, &[outcome]).expect("render");

    assert!(rendered.contains("\"content_model_visible_truncated\": true"));
    assert!(rendered.contains("[truncated]"));
    assert!(!rendered.contains(tail), "{rendered}");
}

#[test]
fn prior_tool_call_projection_bounds_large_input() {
    let tail = "PATCH_INPUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let patch = format!(
        "{}{tail}{}",
        "p".repeat(MODEL_VISIBLE_TOOL_INPUT_STRING_BYTES + 512),
        "P".repeat(1024)
    );
    let call = AgentToolCall {
        call_id: Some("call_large_patch".to_string()),
        tool: "apply_patch".to_string(),
        input: serde_json::json!({
            "patch": patch,
            "path": "hello.txt"
        }),
        r#async: false,
        summary: "apply large patch".to_string(),
    };
    assert!(
        call.input
            .get("patch")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.contains(tail))
    );

    let transcript_item = project_agent_item_to_transcript_item(AgentItem::ToolCall {
        item_id: TaskId::generate(),
        step_index: 4,
        call_index: 0,
        call: call.clone(),
    })
    .expect("projected")
    .expect("projection succeeds");

    assert!(transcript_item.content.contains("\"path\": \"hello.txt\""));
    assert!(
        transcript_item
            .content
            .contains("\"patch_model_visible_truncated\": true")
    );
    assert!(transcript_item.content.contains("\"patch_original_bytes\""));
    assert!(
        !transcript_item.content.contains(tail),
        "{}",
        transcript_item.content
    );
    assert_eq!(transcript_item.native_items.len(), 1);
    match &transcript_item.native_items[0] {
        LanguageModelRequestItem::ToolCall { input, .. } => {
            assert_eq!(
                input.get("path").and_then(serde_json::Value::as_str),
                Some("hello.txt")
            );
            assert_eq!(
                input
                    .get("patch_model_visible_truncated")
                    .and_then(serde_json::Value::as_bool),
                Some(true)
            );
            assert!(
                !input
                    .get("patch")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .contains(tail)
            );
        }
        item => panic!("expected native tool call item, got {item:?}"),
    }
}

#[test]
fn assistant_decision_projection_bounds_large_tool_call_input() {
    let tail = "QUESTION_INPUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let question = format!(
        "{}{tail}{}",
        "q".repeat(MODEL_VISIBLE_TOOL_INPUT_STRING_BYTES + 512),
        "Q".repeat(1024)
    );
    let content = serde_json::json!({
        "response_mode": "defer",
        "text": "I will ask for input.",
        "tool_calls": [{
            "tool": "request_user_input",
            "input": {
                "question": question,
                "choices": ["continue"]
            },
            "async": false,
            "summary": "Ask for user input."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    assert!(content.contains(tail));

    let projected = model_visible_assistant_content(&content);

    assert!(projected.contains("\"question_model_visible_truncated\": true"));
    assert!(projected.contains("\"question_original_bytes\""));
    assert!(projected.contains("\"choices\""));
    assert!(!projected.contains(tail), "{projected}");
}

#[test]
fn render_tool_results_bounds_tool_call_input_in_model_context() {
    let tail = "TOOL_RESULT_CALL_INPUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let question = format!(
        "{}{tail}{}",
        "q".repeat(MODEL_VISIBLE_TOOL_INPUT_STRING_BYTES + 512),
        "Q".repeat(1024)
    );
    let outcome = AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index: 4,
        call_index: 0,
        call: AgentToolCall {
            call_id: Some("call_large_question".to_string()),
            tool: "request_user_input".to_string(),
            input: serde_json::json!({
                "question": question,
                "choices": ["continue"]
            }),
            r#async: false,
            summary: "ask for user input".to_string(),
        },
        status: AgentToolStatus::Blocked,
        summary: "request_user_input unavailable".to_string(),
        output: serde_json::json!({
            "error": "missing_user_input_bridge",
        }),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(false, "missing interactive runtime bridge"),
    };
    assert!(
        outcome
            .call
            .input
            .get("question")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.contains(tail))
    );

    let rendered = render_tool_results(4, &[outcome]).expect("render");

    assert!(rendered.contains("\"question_model_visible_truncated\": true"));
    assert!(rendered.contains("\"question_original_bytes\""));
    assert!(!rendered.contains(tail), "{rendered}");
}

#[test]
fn render_tool_results_bounds_raw_command_output_in_model_context() {
    let stdout = format!(
        "{}STDOUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE",
        "o".repeat(MODEL_VISIBLE_COMMAND_STDOUT_BYTES + 512)
    );
    let stderr = format!(
        "{}STDERR_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE",
        "e".repeat(MODEL_VISIBLE_COMMAND_STDERR_BYTES + 512)
    );
    let aggregated_output = format!(
        "{}AGGREGATED_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE",
        "a".repeat(MODEL_VISIBLE_COMMAND_AGGREGATED_BYTES + 512)
    );
    let command_output = AgentCommandOutput {
        exit_code: Some(1),
        stdout: stdout.clone(),
        stderr: stderr.clone(),
        stdout_ref: None,
        stderr_ref: None,
        aggregated_output: aggregated_output.clone(),
        duration_ms: 42,
        timed_out: false,
        truncated: false,
        stdout_truncated: false,
        stderr_truncated: false,
    };
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
        summary: "make check failed with exit status Some(1)".to_string(),
        output: serde_json::json!({
            "program": "bash",
            "args": ["-lc", "make check"],
            "cwd": ".",
            "status": 1,
            "success": false,
            "timed_out": false,
            "duration_ms": 42,
            "timeout_ms": null,
            "stdout": stdout,
            "stderr": stderr,
            "aggregated_output": aggregated_output,
            "command_output": command_output,
            "stdout_truncated": false,
            "stderr_truncated": false,
            "changed_files": [],
        }),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(true, "allowed"),
    };

    let rendered = render_tool_results(4, &[outcome]).expect("render");

    assert!(rendered.contains("\"command_output_diagnostics\""));
    assert!(rendered.contains("\"stdout_model_visible_truncated\": true"));
    assert!(rendered.contains("\"stderr_model_visible_truncated\": true"));
    assert!(rendered.contains("\"aggregated_output_model_visible_truncated\": true"));
    assert!(rendered.contains("[truncated]"));
    assert!(
        !rendered.contains("STDOUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("STDERR_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("AGGREGATED_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE"),
        "{rendered}"
    );
}

#[test]
fn context_assembler_expands_prompt_file_and_directory_references() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-prompt-ref");
    fs::create_dir_all(root.join("docs")).expect("workspace");
    fs::write(root.join("note.txt"), "important context\n").expect("note");
    fs::write(root.join("docs").join("a.md"), "a").expect("doc");
    fs::write(root.join("docs").join("b.md"), "b").expect("doc");
    let input = agent_input("inspect @note.txt and @docs");
    let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert_eq!(context.prompt_references.len(), 2);
    assert!(context.prompt_references.iter().any(|reference| {
        reference.kind == PromptReferenceKind::File
            && reference.path == "note.txt"
            && reference
                .content
                .as_deref()
                .expect("content")
                .contains("important context")
    }));
    assert!(context.prompt_references.iter().any(|reference| {
        reference.kind == PromptReferenceKind::Directory
            && reference.path == "docs"
            && reference
                .entries
                .iter()
                .any(|entry| entry.path == "docs/a.md")
    }));
    assert!(
        context
            .report
            .included
            .iter()
            .any(|source| source.kind == "prompt_reference_file")
    );

    let request = agent.request(&input, &[], 0).expect("request");
    assert!(
        request
            .messages
            .iter()
            .any(|message| message.content.contains("\"prompt_references\"")
                && message.content.contains("important context"))
    );
}

#[test]
fn context_assembler_bounds_prompt_reference_defaults() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-prompt-ref-bounded");
    fs::create_dir_all(root.join("many")).expect("workspace");
    let tail = "PROMPT_REFERENCE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let note = format!(
        "{}{tail}{}",
        "n".repeat(DEFAULT_PROMPT_REFERENCE_FILE_BYTES + 512),
        "N".repeat(1024)
    );
    fs::write(root.join("large.txt"), note).expect("large prompt ref");
    for index in 0..(DEFAULT_PROMPT_REFERENCE_DIRECTORY_ENTRIES + 5) {
        fs::write(root.join("many").join(format!("{index:03}.txt")), "x").expect("entry");
    }
    let input = agent_input("inspect @large.txt and @many");
    let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");
    let file_reference = context
        .prompt_references
        .iter()
        .find(|reference| reference.path == "large.txt")
        .expect("large file reference");
    assert_eq!(file_reference.bytes, DEFAULT_PROMPT_REFERENCE_FILE_BYTES);
    assert!(file_reference.truncated);
    assert!(
        !file_reference
            .content
            .as_deref()
            .unwrap_or_default()
            .contains(tail)
    );
    let directory_reference = context
        .prompt_references
        .iter()
        .find(|reference| reference.path == "many")
        .expect("many directory reference");
    assert_eq!(
        directory_reference.entries.len(),
        DEFAULT_PROMPT_REFERENCE_DIRECTORY_ENTRIES
    );
    assert!(directory_reference.truncated);

    let request = agent.request(&input, &[], 0).expect("request");
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("\"truncated\": true"));
    assert!(!request_text.contains(tail), "{request_text}");
}

#[test]
fn context_assembler_can_disable_prompt_reference_expansion() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-no-prompt-ref");
    fs::write(root.join("note.txt"), "important context\n").expect("note");
    let input = agent_input("inspect @note.txt and @missing.txt");
    let config = AgentConfig::new("fake", "fake")
        .with_workspace_root(&root)
        .with_prompt_reference_expansion(false);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert!(context.prompt_references.is_empty());
    assert!(
        !context
            .report
            .included
            .iter()
            .any(|source| source.kind.starts_with("prompt_reference"))
    );
    assert!(
        !context
            .report
            .omitted
            .iter()
            .any(|omission| omission.kind == "prompt_reference")
    );
}

#[test]
fn context_assembler_reports_unresolved_prompt_references() {
    let input = agent_input("inspect @missing.txt");
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    );

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert!(context.prompt_references.is_empty());
    assert!(context.report.omitted.iter().any(|omission| {
        omission.kind == "prompt_reference"
            && omission.label == "missing.txt"
            && omission.reason.contains("workspace root is not configured")
    }));
}

#[test]
fn context_budget_omits_empty_optional_projection_lanes() {
    let input = agent_input("plain request without optional context");
    let config =
        AgentConfig::new("fake", "fake").with_workspace_instruction_files(Vec::<PathBuf>::new());
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert!(context.loaded_instruction_files.is_empty());
    assert!(context.prompt_references.is_empty());
    assert!(context.context_attachments.is_empty());
    assert!(context.async_update.is_none());
    assert_eq!(context.budget_estimate.instruction_file_bytes, 0);
    assert_eq!(context.budget_estimate.prompt_reference_bytes, 0);
    assert_eq!(context.budget_estimate.context_attachment_bytes, 0);
    assert_eq!(context.budget_estimate.async_update_bytes, 0);
    assert!(!context.budget_estimate.lanes.iter().any(|lane| {
        matches!(
            lane.kind.as_str(),
            "prompt_references" | "context_attachments" | "async_update"
        )
    }));
    assert!(!context.report.included.iter().any(|source| {
        matches!(
            source.kind.as_str(),
            "prompt_references" | "context_attachments" | "async_update"
        )
    }));
}

#[test]
fn context_assembler_loads_workspace_instruction_files() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-instructions");
    fs::write(
        root.join("AGENTS.md"),
        "# Repo Instructions\n\nPrefer deterministic context.\n",
    )
    .expect("instructions");
    let input = agent_input("inspect");
    let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert_eq!(context.loaded_instruction_files.len(), 1);
    assert_eq!(context.loaded_instruction_files[0].path, "AGENTS.md");
    assert_eq!(context.loaded_instruction_files[0].original_bytes, None);
    assert!(!context.loaded_instruction_files[0].truncated);
    assert!(
        context.loaded_instruction_files[0]
            .content
            .contains("Prefer deterministic context")
    );
    assert!(
        context
            .report
            .included
            .iter()
            .any(|source| source.kind == "instruction_file" && source.label == "AGENTS.md")
    );

    let request = agent.request(&input, &[], 0).expect("request");
    assert!(request.messages.iter().any(|message| {
        message.content.contains("\"loaded_instruction_files\"")
            && message.content.contains("Prefer deterministic context")
    }));
}

#[test]
fn context_assembler_builds_cache_friendly_context_frame() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-context-frame");
    fs::write(
        root.join("AGENTS.md"),
        "# Repo Instructions\n\nKeep stable context first.\n",
    )
    .expect("instructions");
    let input = agent_input("inspect");
    let transcript = vec![AgentTranscriptItem::user(0, "Earlier user request.")];
    let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config);

    let context = agent
        .assemble_context(&input, &transcript, 0)
        .expect("context");
    let block_kinds = context
        .context_frame
        .blocks
        .iter()
        .map(|block| block.kind.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        context.report.context_frame_id.as_deref(),
        Some(context.context_frame.frame_id.as_str())
    );
    assert!(context.context_frame.stable_prefix_bytes > 0);
    assert!(block_kinds.starts_with(&[
        ContextBlockKind::SystemInstructions,
        ContextBlockKind::ToolSchemas,
        ContextBlockKind::ToolSurface,
        ContextBlockKind::InstructionFile,
    ]));
    assert!(
        block_kinds
            .iter()
            .position(|kind| *kind == ContextBlockKind::TranscriptItem)
            .expect("transcript block")
            < block_kinds
                .iter()
                .position(|kind| *kind == ContextBlockKind::WorkspaceFacts)
                .expect("workspace facts block")
    );

    let repeated = agent
        .assemble_context(&input, &transcript, 0)
        .expect("repeated context");
    assert_eq!(
        context.context_frame.frame_id,
        repeated.context_frame.frame_id
    );

    let request = agent.request(&input, &transcript, 0).expect("request");
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        request_text
            .find("\"stable_prefix\"")
            .expect("stable prefix")
            < request_text
                .find("\"volatile_tail\"")
                .expect("volatile tail")
    );
}

#[test]
fn context_assembler_uses_canonical_evidence_once_with_typed_access() {
    let summary = "Exact indexed evidence must be model visible once";
    let mut bundle = evidence_bundle(summary);
    let continuation = sinomo_memory::MemoryQueryContinuation {
        router_manifest_id: TaskId::generate(),
        query_fingerprint: TaskId::generate(),
        window_start_partition_id: None,
        window_start_exclusive: false,
        ranked_evidence: false,
        next_partition_id: TaskId::generate(),
        next_generation_id: TaskId::generate(),
        resume_after_partition: false,
        next_anchor: 2,
        authenticator: "fixture-authenticator".to_string(),
    };
    bundle.coverage = EvidenceCoverage::BudgetTruncated {
        reason: "fixture has another partition".to_string(),
    };
    bundle.continuation = Some(continuation.clone());
    bundle.trace.terms = vec!["exact".to_string(), "indexed".to_string()];
    let provider = RecordingMemoryRetriever::new(bundle);
    let calls = provider.calls.clone();
    let task = AgentTaskRef::new("coding", "wire indexed memory evidence");
    let workspace = WorkspaceRef::sifs("workspace-alpha", "/workspaces/alpha");
    let config = AgentConfig::new("fake", "fake")
        .with_task_ref(task.clone())
        .with_workspace_ref(workspace.clone());
    let input = agent_input("inspect indexed memory");
    let transcript = vec![AgentTranscriptItem::user(0, "Earlier request")];
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config)
    .with_memory_retriever(provider);

    let context = agent
        .assemble_context(&input, &transcript, 0)
        .expect("context");

    let retained_bundle = context
        .memory_evidence_bundle
        .as_ref()
        .expect("retained evidence bundle");
    assert_eq!(retained_bundle.continuation, Some(continuation));
    assert_eq!(
        retained_bundle.trace.terms,
        vec!["exact".to_string(), "indexed".to_string()]
    );
    assert!(context.report.omitted.iter().any(|omission| {
        omission.kind == "memory_coverage" && omission.reason.contains("budget truncated")
    }));
    let memory_blocks = context
        .context_frame
        .blocks
        .iter()
        .filter(|block| block.kind == ContextBlockKind::RetrievedMemory)
        .collect::<Vec<_>>();
    assert_eq!(memory_blocks.len(), 1);
    assert_eq!(memory_blocks[0].payload["content"], summary);
    assert!(memory_blocks[0].payload.get("score").is_none());
    assert!(memory_blocks[0].payload.get("reason").is_none());
    assert!(memory_blocks[0].payload.get("trace").is_none());
    let memory_position = context
        .context_frame
        .blocks
        .iter()
        .position(|block| block.kind == ContextBlockKind::RetrievedMemory)
        .expect("memory block");
    let transcript_position = context
        .context_frame
        .blocks
        .iter()
        .position(|block| block.kind == ContextBlockKind::TranscriptItem)
        .expect("transcript block");
    assert!(memory_position < transcript_position);
    assert_eq!(
        context.budget_estimate.retrieved_memory_bytes,
        memory_blocks
            .iter()
            .map(|block| block.content_bytes)
            .sum::<usize>()
    );

    let calls = calls.lock().expect("memory calls");
    assert_eq!(calls.len(), 1);
    let (access, query) = &calls[0];
    assert_eq!(access.subject_agent_id.as_deref(), Some("main-agent"));
    assert!(access.allowed_scopes.contains(&MemoryScope::Cluster));
    assert!(access.allowed_scopes.contains(&MemoryScope::Agent {
        agent_id: "main-agent".to_string(),
    }));
    assert!(access.allowed_scopes.contains(&MemoryScope::Task {
        task_id: task.task_id.to_string(),
    }));
    assert!(access.allowed_scopes.contains(&MemoryScope::Workspace {
        workspace_id: workspace.id,
    }));
    assert_eq!(query.task_id.as_deref(), Some(task.task_id.as_str()));
    assert_eq!(query.workspace_id.as_deref(), Some("workspace-alpha"));
    drop(calls);

    let request = agent.request(&input, &transcript, 0).expect("request");
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("\"memory_evidence\""));
    assert!(!request_text.contains("\"retrieved_memories\": ["));
    assert_eq!(request_text.matches(summary).count(), 1);
}

#[test]
fn context_assembler_keeps_evidence_block_stable_when_trace_changes() {
    let bundle = evidence_bundle("Stable versioned evidence");
    let provider = RecordingMemoryRetriever::new(bundle);
    let mutable_result = provider.result.clone();
    let input = agent_input("inspect stable evidence");
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_memory_retriever(provider);

    let first = agent.assemble_context(&input, &[], 0).expect("first");
    {
        let mut bundle = mutable_result.lock().expect("memory result");
        bundle.trace.terms = vec!["volatile".to_string(), "trace".to_string()];
        bundle.trace.routed_partitions = 99;
        bundle.evidence[0].score = 999;
        bundle.evidence[0].reason = "volatile reranker explanation".to_string();
        bundle.files[0].source.observed_at = time::OffsetDateTime::now_utc();
    }
    let second = agent.assemble_context(&input, &[], 0).expect("second");
    let first_block = first
        .context_frame
        .blocks
        .iter()
        .find(|block| block.kind == ContextBlockKind::RetrievedMemory)
        .expect("first memory block");
    let second_block = second
        .context_frame
        .blocks
        .iter()
        .find(|block| block.kind == ContextBlockKind::RetrievedMemory)
        .expect("second memory block");

    assert_eq!(first_block.block_id, second_block.block_id);
    assert_eq!(first_block.payload, second_block.payload);
    assert_ne!(
        first
            .memory_evidence_bundle
            .as_ref()
            .expect("first bundle")
            .trace,
        second
            .memory_evidence_bundle
            .as_ref()
            .expect("second bundle")
            .trace
    );
}

#[test]
fn context_assembler_defensively_enforces_actual_evidence_bytes() {
    let input = agent_input("inspect bounded evidence");
    let base_config = AgentConfig::new("fake", "fake");
    let baseline = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(base_config.clone())
    .assemble_context(&input, &[], 0)
    .expect("baseline context");
    let byte_budget = 256usize;
    let mut config = base_config;
    config.context_budget.max_context_bytes = Some(
        baseline
            .budget_estimate
            .total_bytes
            .saturating_add(byte_budget),
    );
    let oversized = format!("MUST_NOT_REACH_MODEL {}", "x".repeat(4096));
    let bundle = evidence_bundle(&oversized);
    let provider = RecordingMemoryRetriever::new(bundle);
    let calls = provider.calls.clone();
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_config(config)
    .with_memory_retriever(provider);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert_eq!(calls.lock().expect("calls")[0].1.max_bytes, byte_budget);
    assert_eq!(context.budget_estimate.retrieved_memory_bytes, 0);
    assert!(
        !context
            .context_frame
            .blocks
            .iter()
            .any(|block| block.kind == ContextBlockKind::RetrievedMemory)
    );
    assert!(context.report.omitted.iter().any(|omission| {
        omission.kind == "retrieved_memory"
            && omission
                .reason
                .contains("agent context packing omitted evidence")
    }));
    let rendered = render_turn_context_message(&input, &context).expect("render context");
    assert!(!rendered.contains("MUST_NOT_REACH_MODEL"));
}

#[test]
fn agent_session_runtime_injects_default_indexed_memory_retriever() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-memory");
    let data_dir = SinomoDataDir::new(root.to_path_buf());
    let storage = AgentRuntimeStorage::from_data_dir(&data_dir);
    let session_id = SessionId::generate();
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    );
    let agent = storage.apply_to_agent(agent, &session_id);

    assert!(storage.memory_retriever().is_some());
    assert!(agent.memory_retriever.is_some());
}

#[test]
fn context_assembler_bounds_large_instruction_files_by_default() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-large-instructions");
    let tail = "INSTRUCTION_FILE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let content = format!(
        "# Repo Instructions\n\n{}{tail}\n",
        "x".repeat(DEFAULT_WORKSPACE_INSTRUCTION_FILE_BYTES + 512)
    );
    fs::write(root.join("AGENTS.md"), &content).expect("instructions");
    let input = agent_input("inspect");
    let config = AgentConfig::new("local", "custom-1m").with_workspace_root(&root);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "local",
        "custom-1m",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert_eq!(context.loaded_instruction_files.len(), 1);
    assert_eq!(
        context.loaded_instruction_files[0].bytes,
        DEFAULT_WORKSPACE_INSTRUCTION_FILE_BYTES
    );
    assert_eq!(
        context.loaded_instruction_files[0].original_bytes,
        Some(content.len())
    );
    assert!(context.loaded_instruction_files[0].truncated);
    assert!(!context.loaded_instruction_files[0].content.contains(tail));
    let request = agent.request(&input, &[], 0).expect("request");
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("\"truncated\": true"));
    assert!(!request_text.contains(tail), "{request_text}");
    assert!(context.budget_estimate.max_context_bytes.is_some());
}

#[test]
fn context_assembler_can_opt_out_of_instruction_file_truncation() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-unbounded-instructions");
    let content = format!("# Repo Instructions\n\n{}\n", "x".repeat(128 * 1024));
    fs::write(root.join("AGENTS.md"), &content).expect("instructions");
    let input = agent_input("inspect");
    let config = AgentConfig::new("local", "custom-1m")
        .with_workspace_root(&root)
        .with_instruction_file_max_bytes(None);
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "local",
        "custom-1m",
    )
    .with_config(config);

    let context = agent.assemble_context(&input, &[], 0).expect("context");

    assert_eq!(context.loaded_instruction_files.len(), 1);
    assert_eq!(context.loaded_instruction_files[0].bytes, content.len());
    assert_eq!(context.loaded_instruction_files[0].original_bytes, None);
    assert!(!context.loaded_instruction_files[0].truncated);
    assert_eq!(context.loaded_instruction_files[0].content, content);
    assert!(context.budget_estimate.max_context_bytes.is_some());
}

#[test]
fn local_workspace_apply_patch_honors_context_and_eof_anchors() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-anchored-patch");
    fs::write(
        root.join("anchors.txt"),
        "section a\nvalue\nsection b\nvalue\nfooter\n",
    )
    .expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("anchored patch");
    let patch = r#"*** Begin Patch
*** Update File: anchors.txt
@@ section b
-value
+changed
*** Update File: anchors.txt
@@
-footer
+tail
*** End of File
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
            summary: "apply anchored patch".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("anchors.txt")).expect("anchors file"),
        "section a\nvalue\nsection b\nchanged\ntail\n"
    );
}

#[test]
fn local_workspace_apply_patch_inserts_addition_only_hunk_after_context() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-context-add-patch");
    fs::write(root.join("insert.txt"), "before\nanchor\nafter\n").expect("seed file");
    let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
    let registry = AgentToolRegistry::local_workspace_with_config(config);
    let input = agent_input("context insertion");
    let patch = r#"*** Begin Patch
*** Update File: insert.txt
@@ anchor
+inserted
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
            summary: "insert after context".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(root.join("insert.txt")).expect("insert file"),
        "before\nanchor\ninserted\nafter\n"
    );
}

#[test]
fn turn_context_renders_context_attachments_for_model() {
    let mut input = agent_input("continue the attached job");
    input.context_attachments.push(AgentContextAttachment {
        kind: "sinomo.evolution.mutation".to_string(),
        summary: "candidate verification context".to_string(),
        payload_json: serde_json::json!({
            "verification_results": [
                {
                    "command": "cargo test -p sinomo-agent",
                    "status": "passed",
                    "summary": "baseline passed"
                }
            ]
        }),
        payload_original_bytes: None,
        payload_model_visible_truncated: false,
    });
    let agent = Agent::new(
        SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]),
        "fake",
        "fake",
    );

    let context = agent.assemble_context(&input, &[], 0).expect("context");
    let request = agent.request(&input, &[], 0).expect("request");

    assert_eq!(context.context_attachments.len(), 1);
    assert!(
        context
            .report
            .included
            .iter()
            .any(|source| source.kind == "context_attachments")
    );
    assert!(context.budget_estimate.context_attachment_bytes > 0);
    assert!(request.messages.iter().any(|message| {
        message.content.contains("context_attachments")
            && message.content.contains("cargo test -p sinomo-agent")
    }));
}

#[test]
fn turn_context_bounds_large_context_attachments_for_model() {
    let mut input = agent_input("continue the attached job");
    let tail = "CONTEXT_ATTACHMENT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let payload = format!(
        "{}{tail}{}",
        "a".repeat(DEFAULT_CONTEXT_ATTACHMENT_PAYLOAD_BYTES + 512),
        "A".repeat(1024)
    );
    input.context_attachments.push(AgentContextAttachment {
        kind: "sinomo.evolution.large".to_string(),
        summary: "large evolution evidence context".to_string(),
        payload_json: serde_json::json!({
            "log": payload,
            "status": "passed"
        }),
        payload_original_bytes: None,
        payload_model_visible_truncated: false,
    });
    assert!(
        input.context_attachments[0]
            .payload_json
            .get("log")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.contains(tail))
    );
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    );

    let context = agent.assemble_context(&input, &[], 0).expect("context");
    let request = agent.request(&input, &[], 0).expect("request");
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(context.context_attachments.len(), 1);
    let attachment = &context.context_attachments[0];
    assert!(attachment.payload_model_visible_truncated);
    assert!(attachment.payload_original_bytes.is_some());
    assert_eq!(
        attachment
            .payload_json
            .get("model_visible_truncated")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(
        attachment
            .payload_json
            .get("payload_json_excerpt")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.contains(tail))
    );
    assert!(
        serde_json::to_vec(&attachment.payload_json)
            .expect("attachment payload projection serializes")
            .len()
            <= DEFAULT_CONTEXT_ATTACHMENT_PAYLOAD_BYTES
    );
    assert!(request_text.contains("payload_model_visible_truncated"));
    assert!(request_text.contains("payload_json_excerpt"));
    assert!(!request_text.contains(tail), "{request_text}");
}

#[test]
fn turn_context_bounds_large_async_update_for_model() {
    let mut input = agent_input("continue after the async task update");
    let tail = "ASYNC_UPDATE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let summary = format!(
        "{}{tail}{}",
        "s".repeat(DEFAULT_ASYNC_UPDATE_BYTES + 512),
        "S".repeat(1024)
    );
    let result_ref = format!(
        "{}{tail}{}",
        "r".repeat(DEFAULT_ASYNC_UPDATE_BYTES + 512),
        "R".repeat(1024)
    );
    input.async_update = Some(AsyncTaskUpdate {
        kind: "coding.validation".to_string(),
        task_id: TaskId::generate(),
        status: crate::AsyncTaskStatus::Progress,
        summary,
        needs_user_attention: true,
        result_ref: Some(result_ref),
        next_action_hint: AsyncTaskNextAction::TellUser,
    });
    assert!(input.async_update.as_ref().is_some_and(|update| {
        update.summary.contains(tail)
            && update
                .result_ref
                .as_deref()
                .is_some_and(|result_ref| result_ref.contains(tail))
    }));
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    );

    let context = agent.assemble_context(&input, &[], 0).expect("context");
    let request = agent.request(&input, &[], 0).expect("request");
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let async_update = context.async_update.as_ref().expect("async update");
    assert_eq!(
        async_update
            .get("model_visible_truncated")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(
        async_update
            .get("summary_excerpt")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.contains(tail))
    );
    assert!(
        async_update
            .get("result_ref_excerpt")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.contains(tail))
    );
    assert!(
        serde_json::to_vec(async_update)
            .expect("async update projection serializes")
            .len()
            <= DEFAULT_ASYNC_UPDATE_BYTES
    );
    assert!(context.budget_estimate.async_update_bytes > 0);
    assert!(
        context
            .report
            .included
            .iter()
            .any(|source| source.kind == "async_update"
                && source.bytes == context.budget_estimate.async_update_bytes)
    );
    assert!(request_text.contains("async_update_original_bytes"));
    assert!(request_text.contains("summary_excerpt"));
    assert!(!request_text.contains(tail), "{request_text}");
}

#[test]
fn session_projection_is_rendered_as_current_work_budget_lane() {
    let model = SequencedModel::new([
        "Checkpoint summary for projection context.",
        r#"{
        "response_mode": "answer",
        "text": "Projection-aware answer.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
        retained_tail_count: Some(1),
    });
    let input = agent_input("continue after compaction");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "remember this active objective".to_string(),
        event_id: None,
    });
    session.transcript.push(AgentItem::AssistantText {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "I will keep working on it.".to_string(),
    });
    agent
        .compact(&mut session, "test projection context")
        .expect("compact");

    let run = agent.start_turn(&mut session, &input).expect("run");

    let requests = agent.model.requests.lock().expect("requests");
    let request_text = requests[1]
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("\"session_projection\""));
    assert!(request_text.contains("\"current_work\""));
    assert!(request_text.contains("\"lanes\""));
    assert!(request_text.contains("\"recent_tail\""));
    assert!(
        run.context_reports[0]
            .budget_estimate
            .lanes
            .iter()
            .any(|lane| lane.kind == "current_work" && lane.bytes > 0)
    );
}

#[test]
fn agent_session_continuation_uses_history_and_consumes_queued_context() {
    let model = SequencedModel::new([
        r#"{
            "response_mode": "answer",
            "text": "First answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "Second answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake");
    let input = agent_input("first");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

    agent.start_turn(&mut session, &input).expect("first turn");
    agent
        .steer(&mut session, "prefer concise reasoning")
        .expect("steer");
    agent
        .follow_up(&mut session, "also mention tests")
        .expect("follow up");
    let second_input = agent_input("second");
    let run = agent
        .continue_turn(&mut session, &second_input)
        .expect("second turn");

    assert_eq!(run.final_decision.text, "Second answer.");
    assert!(session.steering_queue.is_empty());
    assert!(session.follow_up_queue.is_empty());
    let requests = agent.model.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    let second_request_text = requests[1]
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(second_request_text.contains("First answer."));
    assert!(second_request_text.contains("agent_queued_turn_context"));
    assert!(second_request_text.contains("prefer concise reasoning"));
    assert!(second_request_text.contains("also mention tests"));
}
