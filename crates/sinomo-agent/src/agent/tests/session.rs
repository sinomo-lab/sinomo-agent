use super::*;

#[test]
fn reaping_a_failed_turn_does_not_reject_its_continuation() {
    let input = agent_input("first model request fails");
    let runtime = AgentSessionRuntime::new(
        Agent::new(SequencedModel::new(Vec::<String>::new()), "fake", "fake"),
        AgentSession::new(input.session.session_id.clone(), AgentRole::Main),
    );
    runtime
        .submit_op(AgentOp::start_turn(input.clone()))
        .expect("start");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !runtime
        .state
        .lock()
        .expect("state")
        .worker
        .as_ref()
        .expect("worker")
        .is_finished()
    {
        assert!(std::time::Instant::now() < deadline, "worker must finish");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    runtime.agent.model.responses.lock().expect("responses").push_back(
        r#"{"response_mode":"answer","text":"Recovered.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#.into(),
    );
    let next = AgentOp::continue_turn(input);
    let next_id = next.op_id.clone();
    assert!(
        runtime
            .submit_op(next)
            .expect("old failure cannot reject new input")
            .accepted
    );
    runtime.join_active_turn().expect("recovered turn");
    assert_eq!(
        runtime
            .turn_result(&next_id)
            .unwrap()
            .unwrap()
            .run
            .unwrap()
            .final_decision
            .text,
        "Recovered."
    );
}

#[test]
fn new_session_prefers_agent_home_server_and_migration_does_not_move_sandbox() {
    let definition = AgentDefinition::custom("remote-agent", AgentRole::Main, "Help.")
        .with_home_server_node_id("home-server");
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    session.place_new_session(&definition, "request-server");
    session.resource_bindings = vec![
        AgentSessionResourceBinding {
            resource_id: "home".to_string(),
            kind: AgentSessionResourceKind::AgentHome,
            location: AgentSessionResourceLocation::Sifs {
                path: "/sinomo/agents/remote-agent/home".to_string(),
            },
            availability: AgentSessionResourceAvailability::Available,
        },
        AgentSessionResourceBinding {
            resource_id: "local-workspace".to_string(),
            kind: AgentSessionResourceKind::Workspace,
            location: AgentSessionResourceLocation::SessionServerLocal {
                server_id: "home-server".to_string(),
                path: PathBuf::from("/srv/workspace"),
            },
            availability: AgentSessionResourceAvailability::Available,
        },
    ];
    session
        .attach_sandbox(AgentSessionSandboxAttachment {
            sandbox_id: "sandbox-1".to_string(),
            server_id: "sandbox-server".to_string(),
            attachment_kind: AgentSessionSandboxAttachmentKind::Automatic,
        })
        .expect("attach sandbox");

    let report = session.migrate_to_server("target-server");

    let placement = session.execution_placement.as_ref().expect("placement");
    assert_eq!(placement.active_server_id, "target-server");
    assert_eq!(placement.generation, 1);
    assert_eq!(
        session
            .sandbox_attachment
            .as_ref()
            .expect("sandbox")
            .server_id,
        "sandbox-server",
        "session migration must not implicitly move the sandbox attachment"
    );
    assert_eq!(report.unavailable_resource_ids, ["local-workspace"]);
    assert_eq!(
        session.resource_bindings[0].availability,
        AgentSessionResourceAvailability::Available,
        "SIFS remains accessible after migration"
    );
    assert_eq!(
        session.resource_bindings[1].availability,
        AgentSessionResourceAvailability::UnavailableAfterMigration
    );

    let replacement = AgentSessionSandboxAttachment {
        sandbox_id: "sandbox-2".to_string(),
        server_id: "target-server".to_string(),
        attachment_kind: AgentSessionSandboxAttachmentKind::Explicit,
    };
    assert!(matches!(
        session.attach_sandbox(replacement),
        Err(AgentSessionSandboxAttachmentError::AlreadyAttached { .. })
    ));

    let mut other_session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    other_session
        .attach_sandbox(
            session
                .sandbox_attachment
                .clone()
                .expect("shared attachment"),
        )
        .expect("a sandbox resource may be shared by another session");
}

#[test]
fn agent_config_definition_swap_clears_package_runtime_limits_without_losing_overrides() {
    let mut capped_definition =
        AgentDefinition::custom("capped-agent", AgentRole::Deliberation, "Use one step.");
    capped_definition.runtime_limits.max_steps = Some(1);
    capped_definition.runtime_limits.max_tool_calls_per_step = Some(2);
    capped_definition.runtime_limits.max_model_repair_attempts = Some(3);

    let capped_config =
        AgentConfig::new("fake", "fake").with_agent_definition(capped_definition.clone());
    assert_eq!(capped_config.max_steps, Some(1));
    assert_eq!(capped_config.max_tool_calls_per_step, Some(2));
    assert_eq!(capped_config.max_model_repair_attempts, Some(3));

    let uncapped_config = capped_config.with_agent_definition(AgentDefinition::coding());
    assert_eq!(uncapped_config.max_steps, None);
    assert_eq!(uncapped_config.max_tool_calls_per_step, None);
    assert_eq!(uncapped_config.max_model_repair_attempts, None);

    let override_config = AgentConfig::new("fake", "fake")
        .with_agent_definition(capped_definition)
        .with_max_steps(1)
        .with_max_tool_calls_per_step(2)
        .with_max_model_repair_attempts(3)
        .with_agent_definition(AgentDefinition::coding());
    assert_eq!(override_config.max_steps, Some(1));
    assert_eq!(override_config.max_tool_calls_per_step, Some(2));
    assert_eq!(override_config.max_model_repair_attempts, Some(3));
}

#[test]
fn agent_config_runtime_limit_overrides_survive_json_round_trip() {
    let mut capped_definition =
        AgentDefinition::custom("capped-agent", AgentRole::Deliberation, "Use one step.");
    capped_definition.runtime_limits.max_steps = Some(1);
    capped_definition.runtime_limits.max_tool_calls_per_step = Some(2);
    capped_definition.runtime_limits.max_model_repair_attempts = Some(3);

    let package_config =
        AgentConfig::new("fake", "fake").with_agent_definition(capped_definition.clone());
    let package_value = serde_json::to_value(&package_config).expect("serialize package caps");
    assert!(package_value.get("max_steps_explicit_override").is_none());
    assert!(
        package_value
            .get("max_tool_calls_per_step_explicit_override")
            .is_none()
    );
    assert!(
        package_value
            .get("max_model_repair_attempts_explicit_override")
            .is_none()
    );

    let package_round_trip: AgentConfig =
        serde_json::from_value(package_value).expect("deserialize package caps");
    let uncapped_package = package_round_trip.with_agent_definition(AgentDefinition::coding());
    assert_eq!(uncapped_package.max_steps, None);
    assert_eq!(uncapped_package.max_tool_calls_per_step, None);
    assert_eq!(uncapped_package.max_model_repair_attempts, None);

    let override_config = AgentConfig::new("fake", "fake")
        .with_agent_definition(capped_definition)
        .with_max_steps(1)
        .with_max_tool_calls_per_step(2)
        .with_max_model_repair_attempts(3);
    let override_value = serde_json::to_value(&override_config).expect("serialize explicit caps");
    assert_eq!(
        override_value
            .get("max_steps_explicit_override")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        override_value
            .get("max_tool_calls_per_step_explicit_override")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        override_value
            .get("max_model_repair_attempts_explicit_override")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );

    let override_round_trip: AgentConfig =
        serde_json::from_value(override_value).expect("deserialize explicit caps");
    let uncapped_override = override_round_trip.with_agent_definition(AgentDefinition::coding());
    assert_eq!(uncapped_override.max_steps, Some(1));
    assert_eq!(uncapped_override.max_tool_calls_per_step, Some(2));
    assert_eq!(uncapped_override.max_model_repair_attempts, Some(3));
}

#[test]
fn agent_config_preserves_definition_tool_runtime() {
    let definition = AgentDefinition::custom("worker-agent", AgentRole::Main, "Be helpful.")
        .with_sandbox_runtime_node_id("worker-node");
    let config = AgentConfig::new("fake", "fake").with_agent_definition(definition);

    let value = serde_json::to_value(&config).expect("serialize config");
    assert_eq!(
        value
            .pointer("/definition/tool_runtime/sandbox_node_id")
            .and_then(serde_json::Value::as_str),
        Some("worker-node")
    );

    let round_trip: AgentConfig = serde_json::from_value(value).expect("deserialize config");
    assert_eq!(
        round_trip
            .definition
            .tool_runtime
            .sandbox_node_id
            .as_deref(),
        Some("worker-node")
    );
}

#[test]
fn package_tool_allow_and_deny_lists_drive_the_runtime_surface() {
    let mut definition =
        AgentDefinition::custom("restricted-agent", AgentRole::Deliberation, "Stay bounded.");
    definition.tool_surface.allow_tools = vec!["memory_read".to_string()];
    definition.tool_surface.deny_tools = vec!["run_command".to_string()];
    definition.compaction.retained_tail_items = Some(7);
    let config = AgentConfig::new("fake", "fake").with_agent_definition(definition.clone());
    let agent =
        Agent::new(SequencedModel::new(Vec::<String>::new()), "fake", "fake").with_config(config);
    let call = |tool: &str| AgentToolCall {
        call_id: None,
        tool: tool.to_string(),
        input: serde_json::json!({}),
        r#async: false,
        summary: String::new(),
    };

    assert_eq!(
        agent.tool_surface,
        AgentToolSurface::for_definition(&definition)
    );
    assert_eq!(agent.compactor.retained_tail_count, Some(7));
    assert!(
        agent
            .tool_surface
            .evaluate_tool_call(None, &call("memory_read"))
            .allowed
    );
    assert!(
        !agent
            .tool_surface
            .evaluate_tool_call(None, &call("run_command"))
            .allowed
    );
    assert!(
        !agent
            .tool_surface
            .evaluate_tool_call(None, &call("memory_write"))
            .allowed
    );
}

#[test]
fn file_session_store_persists_and_reloads_session_snapshot() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session");
    let data_dir = SinomoDataDir::new(&data_root);
    let session_id = SessionId::from_legacy_named("sess_agent_store").expect("session");
    let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
    let mut session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "hello".to_string(),
        event_id: None,
    });

    store.save_session(&session).expect("save session");
    let loaded = store
        .load_session(&session_id)
        .expect("load session")
        .expect("saved session");
    assert_eq!(loaded, session);

    session
        .steering_queue
        .push(AgentSteeringMessage::new("prefer concise answers"));
    store.save_session(&session).expect("replace session");
    let loaded = store
        .load_session(&session_id)
        .expect("load replaced session")
        .expect("saved session");
    assert_eq!(loaded.steering_queue, session.steering_queue);

    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    let events = fs::read_to_string(paths.session_events_path_for_session(&session_id))
        .expect("session events");
    assert!(events.contains("\"snapshot_saved\""));
    assert!(
        fs::read_to_string(paths.session_snapshot_path_for_session(&session_id))
            .expect("snapshot")
            .contains("\"schema_version\"")
    );
}

#[test]
fn agent_session_store_records_turns_and_queue_updates() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session-run");
    let data_dir = SinomoDataDir::new(&data_root);
    let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let agent = Agent::new(model, "fake", "fake").with_session_store(store.clone());
    let input = agent_input("persist this turn");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

    let run = agent.start_turn(&mut session, &input).expect("run");
    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    agent
        .steer(&mut session, "carry this into the next turn")
        .expect("steer");

    let loaded = store
        .load_session(&input.session.session_id)
        .expect("load session")
        .expect("saved session");
    assert_eq!(loaded.transcript.items, session.transcript.items);
    assert_eq!(loaded.steering_queue, session.steering_queue);

    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    let events =
        fs::read_to_string(paths.session_events_path_for_session(&input.session.session_id))
            .expect("session events");
    assert!(events.contains("\"turn_started\""));
    assert!(events.contains("\"turn_completed\""));
    assert!(events.contains("\"steering_queued\""));
    assert!(events.contains("\"snapshot_saved\""));
}

#[test]
fn agent_tool_pair_integrity_pairs_by_runtime_call_slot() {
    let paired_call_item = TaskId::generate();
    let paired_result_call_id = TaskId::generate();
    let orphan_call_item = TaskId::generate();
    let orphan_result_call_id = TaskId::generate();
    let items = vec![
        AgentItem::ToolCall {
            item_id: paired_call_item,
            step_index: 0,
            call_index: 0,
            call: AgentToolCall {
                call_id: None,
                tool: "read_file".to_string(),
                input: serde_json::json!({"path": "src/lib.rs"}),
                r#async: false,
                summary: "read source".to_string(),
            },
        },
        AgentItem::ToolResult {
            item_id: TaskId::generate(),
            step_index: 0,
            call_index: 0,
            call_id: paired_result_call_id,
            native_call_id: None,
            tool_name: Some("read_file".to_string()),
            status: AgentToolStatus::Succeeded,
            summary: "read source".to_string(),
            output: serde_json::json!({"content": "ok"}),
            evidence: AgentToolEvidence::default(),
        },
        AgentItem::ToolCall {
            item_id: orphan_call_item,
            step_index: 1,
            call_index: 0,
            call: AgentToolCall {
                call_id: None,
                tool: "search".to_string(),
                input: serde_json::json!({"query": "needle"}),
                r#async: false,
                summary: "search source".to_string(),
            },
        },
        AgentItem::ToolResult {
            item_id: TaskId::generate(),
            step_index: 2,
            call_index: 0,
            call_id: orphan_result_call_id,
            native_call_id: None,
            tool_name: Some("read_file".to_string()),
            status: AgentToolStatus::Succeeded,
            summary: "late result".to_string(),
            output: serde_json::json!({"content": "late"}),
            evidence: AgentToolEvidence::default(),
        },
    ];

    let metrics = AgentToolPairIntegrity::from_items(&items);

    assert_eq!(metrics.tool_calls, 2);
    assert_eq!(metrics.tool_results, 2);
    assert_eq!(metrics.orphan_tool_calls, 1);
    assert_eq!(metrics.orphan_tool_results, 1);
    assert!(!metrics.is_balanced());
}

#[test]
fn tool_registry_supports_runtime_registration_replacement_and_removal() {
    let registry = AgentToolRegistry::new();
    let input = agent_input("read");
    let call = AgentToolCall {
        call_id: None,
        tool: "read_file".to_string(),
        input: serde_json::json!({"path": "Cargo.toml"}),
        r#async: false,
        summary: "read".to_string(),
    };

    assert!(registry.is_empty());
    assert_eq!(registry.generation(), 0);
    assert!(registry.register(EchoTool).is_none());
    assert_eq!(registry.generation(), 1);
    assert!(registry.contains("read_file"));
    assert_eq!(registry.len(), 1);
    let first = registry.execute(&input.session, 0, 0, &call);
    assert_eq!(
        first
            .output
            .get("content")
            .and_then(serde_json::Value::as_str),
        Some("workspace evidence")
    );

    let replaced = registry
        .register(ReplacementReadTool)
        .expect("replaced descriptor");
    assert_eq!(registry.generation(), 2);
    assert_eq!(replaced.spec.name, "read_file");
    assert_eq!(registry.len(), 1);
    let second = registry.execute(&input.session, 0, 0, &call);
    assert_eq!(
        second
            .output
            .get("content")
            .and_then(serde_json::Value::as_str),
        Some("replacement evidence")
    );

    let removed = registry.remove("read_file").expect("removed descriptor");
    assert_eq!(registry.generation(), 3);
    assert_eq!(removed.spec.name, "read_file");
    assert!(!registry.contains("read_file"));
    let missing = registry.execute(&input.session, 0, 0, &call);
    assert_eq!(missing.status, AgentToolStatus::Failed);
    assert_eq!(
        missing
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("unknown_tool")
    );
}

#[test]
fn tool_planner_composes_runtime_and_session_descriptors() {
    let agent = Agent::new(
        SequencedModel::new(std::iter::empty::<String>()),
        "fake",
        "fake",
    )
    .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));
    let mut input = agent_input("inspect with composed tools");
    input.tool_specs = vec![AgentToolSpec::new(
        AgentToolKind::SearchFiles,
        "search",
        "session-visible search tool",
        false,
    )];

    let context = agent.assemble_context(&input, &[], 0).expect("context");
    let visible_tools: Vec<_> = context
        .available_tool_specs
        .iter()
        .map(|descriptor| descriptor.spec.name.as_str())
        .collect();

    assert!(visible_tools.contains(&"read_file"));
    assert!(!visible_tools.contains(&"search"));
    assert!(
        context
            .report
            .omitted
            .iter()
            .any(|omission| omission.kind == "tool_readiness"
                && omission.label == "search"
                && omission.reason.contains("no runtime executor"))
    );
    assert_eq!(
        visible_tools
            .iter()
            .filter(|tool| **tool == "read_file")
            .count(),
        1
    );
}

#[test]
fn planned_tool_without_runtime_executor_returns_missing_executor() {
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I will search files.",
            "tool_calls": [{
                "tool": "search",
                "input": {"query": "needle", "path": "."},
                "async": false,
                "summary": "Search the workspace."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "The tool is visible but has no executor.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake");

    let run = agent.run(&agent_input("list files")).expect("run");

    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    assert!(
        !run.model_turns[0]
            .request
            .tools
            .iter()
            .any(|tool| tool.name == "search"),
        "unexecutable visible defaults should not be advertised to the model"
    );
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Blocked);
    assert_eq!(
        run.tool_results[0]
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("missing_executor")
    );
}

#[test]
fn hidden_runtime_tool_is_blocked_if_model_guesses_it() {
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I will try a hidden tool.",
            "tool_calls": [{
                "tool": "read_file",
                "input": {"path": "Cargo.toml"},
                "async": false,
                "summary": "Read the workspace."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "The tool is outside the current projection.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let config = AgentConfig::new("fake", "fake").with_visible_toolsets(["config"]);
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

    let run = agent.run(&agent_input("try hidden tool")).expect("run");
    let orchestration = run.tool_results[0]
        .evidence
        .metadata
        .get("tool_orchestration")
        .expect("tool orchestration metadata");

    assert_eq!(run.tool_results[0].status, AgentToolStatus::Blocked);
    assert_eq!(
        run.tool_results[0]
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("tool_not_visible")
    );
    assert_eq!(
        orchestration
            .get("descriptor_visible")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        orchestration
            .get("registry_generation")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert_eq!(
        orchestration
            .get("visible_toolsets")
            .and_then(serde_json::Value::as_array)
            .and_then(|toolsets| toolsets.first())
            .and_then(serde_json::Value::as_str),
        Some("config")
    );
    assert_eq!(
        orchestration
            .get("executor_available")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[cfg(unix)]
#[test]
fn version_control_tools_cancel_active_git_subprocesses() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-git-tool-cancel");
    fs::create_dir_all(&root).expect("workspace");
    let fake_git = root.join("slow-git");
    fs::write(&fake_git, "#!/bin/sh\nsleep 5\n").expect("fake git");
    let mut permissions = fs::metadata(&fake_git).expect("metadata").permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    fs::set_permissions(&fake_git, permissions).expect("chmod fake git");
    let fake_git_program = fake_git.display().to_string();
    let registry = with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(&root)
            .using_local_process_commands()
            .with_git_program(fake_git_program.clone())
            .enabling_git_tools(),
    ));
    let input = agent_input("cancel git status");
    let cancellation_token = AgentCancellationToken::new();
    let token_for_sink = cancellation_token.clone();
    let progress_sink = crate::AgentToolProgressSink::new(move |event| {
        if matches!(event, crate::AgentToolProgressEvent::ProcessStarted { .. }) {
            token_for_sink.cancel();
        }
    });

    let outcome = registry.execute_with_cancellation_and_progress(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "changed_files".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "cancel changed files".to_string(),
        },
        cancellation_token,
        progress_sink,
    );

    assert_eq!(outcome.status, AgentToolStatus::Cancelled);
    assert_eq!(
        outcome
            .output
            .get("cancelled")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        outcome
            .output
            .get("program")
            .and_then(serde_json::Value::as_str),
        Some(fake_git_program.as_str())
    );
}

#[test]
fn agent_op_surface_runs_turn_and_emits_lifecycle_events() {
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Done through op.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let agent = Agent::new(model, "fake", "fake");
    let input = agent_input("hello through op");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

    let outcome = agent
        .handle_op(&mut session, AgentOp::start_turn(input))
        .expect("op");

    assert_eq!(outcome.session_id, session.session_id);
    assert_eq!(outcome.events.len(), 2);
    assert!(matches!(
        &outcome.events[0].payload,
        AgentEventPayload::TurnStarted {
            provider,
            model,
            continuation: false,
            ..
        } if provider == "fake" && model == "fake"
    ));
    assert!(matches!(
        &outcome.events[1].payload,
        AgentEventPayload::TurnCompleted {
            stop_reason: AgentStopReason::FinalDecision,
            model_turn_count: 1,
            tool_result_count: 0,
            ..
        }
    ));
    assert_eq!(
        outcome.run.expect("run").final_decision.text,
        "Done through op."
    );
    assert!(!session.transcript.items.is_empty());
}

#[test]
fn agent_op_surface_wraps_session_control_methods() {
    let model = SequencedModel::new(Vec::<String>::new());
    let agent = Agent::new(model, "fake", "fake");
    let session_id = SessionId::from_legacy_named("sess_agent_ops").expect("session");
    let mut session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "remember this before compacting".to_string(),
        event_id: None,
    });

    let steering = agent
        .handle_op(&mut session, AgentOp::steer("prefer precise output"))
        .expect("steer");
    assert!(matches!(
        &steering.events[0].payload,
        AgentEventPayload::SteeringQueued { steering }
            if steering.message == "prefer precise output"
    ));
    assert_eq!(session.steering_queue.len(), 1);

    let follow_up = agent
        .handle_op(&mut session, AgentOp::follow_up("also mention tests"))
        .expect("follow up");
    assert!(matches!(
        &follow_up.events[0].payload,
        AgentEventPayload::FollowUpQueued { follow_up }
            if follow_up.message == "also mention tests"
    ));
    assert_eq!(session.follow_up_queue.len(), 1);

    let compaction = agent
        .handle_op(&mut session, AgentOp::compact("op compaction"))
        .expect("compact");
    assert!(matches!(
        &compaction.events[0].payload,
        AgentEventPayload::Compacted { summary, item_count }
            if summary.reason == "op compaction"
                && *item_count == session.transcript.items.len()
    ));

    let snapshot = agent
        .handle_op(&mut session, AgentOp::snapshot())
        .expect("snapshot");
    assert!(matches!(
        &snapshot.events[0].payload,
        AgentEventPayload::Snapshot {
            item_count,
            steering_count: 1,
            follow_up_count: 1,
            aborted: false,
        } if *item_count == session.transcript.items.len()
    ));

    let abort = agent
        .handle_op(&mut session, AgentOp::abort("op abort"))
        .expect("abort");
    assert!(matches!(
        &abort.events[0].payload,
        AgentEventPayload::Aborted { abort } if abort.reason == "op abort"
    ));
    assert!(session.is_aborted());
}

#[test]
fn agent_op_removes_queued_inputs_by_id() {
    let model = SequencedModel::new(Vec::<String>::new());
    let agent = Agent::new(model, "fake", "fake");
    let session_id = SessionId::from_legacy_named("sess_agent_remove_queued").expect("session");
    let mut session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);

    let removable = agent
        .steer(&mut session, "prefer AV1, skip the HEVC pass")
        .expect("steer");
    let kept = agent.steer(&mut session, "keep this one").expect("steer");
    let follow_up = agent
        .follow_up(&mut session, "share to #renders when done")
        .expect("follow up");
    assert_eq!(session.steering_queue.len(), 2);
    assert_eq!(session.follow_up_queue.len(), 1);

    let removed = agent
        .handle_op(
            &mut session,
            AgentOp::remove_queued_input(removable.steering_id.clone()),
        )
        .expect("remove steering");
    assert!(matches!(
        &removed.events[0].payload,
        AgentEventPayload::SteeringRemoved { steering }
            if steering.steering_id == removable.steering_id
                && steering.message == "prefer AV1, skip the HEVC pass"
    ));
    assert_eq!(session.steering_queue.len(), 1);
    assert_eq!(
        session.steering_queue[0].steering_id, kept.steering_id,
        "only the addressed steering message is removed"
    );

    let removed_follow_up = agent
        .handle_op(
            &mut session,
            AgentOp::remove_queued_input(follow_up.follow_up_id.clone()),
        )
        .expect("remove follow-up");
    assert!(matches!(
        &removed_follow_up.events[0].payload,
        AgentEventPayload::FollowUpRemoved { follow_up: removed }
            if removed.follow_up_id == follow_up.follow_up_id
    ));
    assert!(session.follow_up_queue.is_empty());

    // A miss (already consumed or bogus id) is an error, not a silent no-op.
    let missing = agent.handle_op(
        &mut session,
        AgentOp::remove_queued_input(TaskId::generate()),
    );
    assert!(matches!(missing, Err(AgentError::Runtime { .. })));
    assert_eq!(
        session.steering_queue.len(),
        1,
        "a miss leaves the queues untouched"
    );
}

#[test]
fn agent_session_control_plane_orders_ops_and_tracks_task_status() {
    let task = AgentTaskRef::new("coding", "control-plane coding task")
        .with_owner_role(AgentRole::Deliberation);
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Controlled turn done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
    let agent = Agent::new(model, "fake", "fake").with_config(config);
    let input = agent_input("run under control plane");
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation)
        .with_agent_definition_key("deliberation-agent");
    let mut control = AgentSessionControlPlane::new(session);

    let result = control
        .submit_op(&agent, AgentOp::start_turn(input))
        .expect("submit op");

    assert_eq!(result.sequence, 0);
    assert!(result.accepted);
    assert!(
        result
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.run.as_ref())
            .is_some()
    );
    assert!(control.active_turn().is_none());
    assert_eq!(control.snapshot().next_sequence, 1);
    assert!(matches!(
        &result.events[0].payload,
        AgentEventPayload::OperationQueued {
            sequence: 0,
            action,
            ..
        } if action == "start_turn"
    ));
    assert!(result.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::TaskStatusChanged { snapshot }
                if snapshot.task.task_id == task.task_id
                    && snapshot.agent_definition_key.as_deref()
                        == Some("deliberation-agent")
                    && snapshot.status == AgentTaskLifecycleStatus::Running
                    && snapshot.active_op_id.is_some()
        )
    }));
    assert!(result.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::TurnCompleted {
                stop_reason: AgentStopReason::FinalDecision,
                ..
            }
        )
    }));
    let snapshot = control.snapshot();
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].task.task_id, task.task_id);
    assert_eq!(
        snapshot.tasks[0].agent_definition_key.as_deref(),
        Some("deliberation-agent")
    );
    assert_eq!(
        snapshot.tasks[0].status,
        AgentTaskLifecycleStatus::Completed
    );
    assert!(!snapshot.tasks[0].event_ids.is_empty());
    assert_eq!(control.drain_events().len(), result.events.len());
}

#[test]
fn agent_session_control_plane_persists_progress_and_terminal_result() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-control-store");
    let data_dir = SinomoDataDir::new(&data_root);
    let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
    let task = AgentTaskRef::new("coding", "persisted control-plane task")
        .with_owner_role(AgentRole::Deliberation);
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Persisted turn done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
    let agent = Agent::new(model, "fake", "fake").with_config(config);
    let input = agent_input("persist control progress");
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation)
        .with_agent_definition_key("deliberation-agent");
    let mut control = AgentSessionControlPlane::new(session);
    let cursor = AgentSessionControlEventCursor {
        offset: 0,
        last_event_id: None,
    };

    let result = control
        .submit_op(&agent, AgentOp::start_turn(input.clone()))
        .expect("submit op");
    result.persist_to_store(&store).expect("persist control");

    let progress = control.progress_since(cursor);
    assert_eq!(progress.events.len(), result.events.len());
    assert!(progress.task_updates.iter().any(|snapshot| {
        snapshot.task.task_id == task.task_id
            && snapshot.status == AgentTaskLifecycleStatus::Completed
    }));
    assert_eq!(progress.terminal_results.len(), 1);
    assert_eq!(
        control
            .terminal_result(&task.task_id)
            .expect("terminal result")
            .status,
        AgentTaskLifecycleStatus::Completed
    );
    assert_eq!(
        result.terminal_results()[0].status,
        AgentTaskLifecycleStatus::Completed
    );

    let loaded_snapshot = store
        .load_control_snapshot(&input.session.session_id)
        .expect("load control snapshot")
        .expect("control snapshot");
    assert_eq!(loaded_snapshot.next_sequence, 1);
    assert_eq!(loaded_snapshot.tasks[0].task.task_id, task.task_id);
    assert_eq!(
        loaded_snapshot.tasks[0].agent_definition_key.as_deref(),
        Some("deliberation-agent")
    );
    assert_eq!(
        loaded_snapshot.tasks[0].status,
        AgentTaskLifecycleStatus::Completed
    );
    let session_events = fs::read_to_string(
        AgentStoragePaths::from_data_dir(&data_dir)
            .session_events_path_for_session(&input.session.session_id),
    )
    .expect("session events");
    assert!(session_events.contains("\"control_event_published\""));
    assert!(session_events.contains("\"task_status_changed\""));
    assert!(session_events.contains("\"control_snapshot_saved\""));
}

#[test]
fn agent_session_progress_cursor_uses_last_event_id_and_preserves_empty_cursor() {
    let session_id = SessionId::from_legacy_named("sess_progress_cursor").expect("session");
    let task = AgentTaskRef::new("coding", "progress cursor task")
        .with_owner_role(AgentRole::Deliberation);
    let running = AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Running);
    let completed = AgentTaskSnapshot::new(task, AgentTaskLifecycleStatus::Completed);
    let first = AgentEvent::new(
        TaskId::from_legacy_named("op_progress_cursor_first").expect("first op"),
        session_id.clone(),
        AgentEventPayload::TaskStatusChanged { snapshot: running },
    );
    let second = AgentEvent::new(
        TaskId::from_legacy_named("op_progress_cursor_second").expect("second op"),
        session_id,
        AgentEventPayload::TaskStatusChanged {
            snapshot: completed,
        },
    );
    let events = vec![first.clone(), second.clone()];

    let realigned = agent_session_control_event_batch_from_events(
        AgentSessionControlEventCursor {
            offset: 99,
            last_event_id: Some(first.event_id.clone()),
        },
        &events,
    );
    assert_eq!(realigned.events.len(), 1);
    assert_eq!(realigned.events[0].event_id, second.event_id);
    assert_eq!(realigned.next_cursor.offset, 2);
    assert_eq!(
        realigned.next_cursor.last_event_id.as_ref(),
        Some(&second.event_id)
    );

    let empty =
        agent_session_control_event_batch_from_events(realigned.next_cursor.clone(), &events);
    assert!(empty.events.is_empty());
    assert!(empty.task_updates.is_empty());
    assert!(empty.terminal_results.is_empty());
    assert_eq!(empty.next_cursor.offset, 2);
    assert_eq!(
        empty.next_cursor.last_event_id.as_ref(),
        Some(&second.event_id)
    );
}

#[test]
fn agent_session_control_snapshot_restores_task_status_for_resume() {
    let task = AgentTaskRef::new("coding", "resume task").with_owner_role(AgentRole::Deliberation);
    let session_id = SessionId::from_legacy_named("sess_resume_control").expect("session");
    let snapshot = AgentSessionControlSnapshot {
        session_id: session_id.clone(),
        next_sequence: 7,
        active_turn: None,
        tasks: vec![
            AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Completed)
                .with_agent_definition_key("deliberation-agent"),
        ],
        buffered_event_count: 3,
    };
    let session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);

    let control =
        AgentSessionControlPlane::from_snapshot(session, snapshot).expect("restore snapshot");

    assert_eq!(control.snapshot().next_sequence, 7);
    assert_eq!(control.tasks().len(), 1);
    assert_eq!(
        control
            .task_snapshot(&task.task_id)
            .and_then(|snapshot| snapshot.agent_definition_key.as_deref()),
        Some("deliberation-agent")
    );
    assert_eq!(
        control
            .task_snapshot(&task.task_id)
            .map(|snapshot| snapshot.status.clone()),
        Some(AgentTaskLifecycleStatus::Completed)
    );
    assert_eq!(control.events_since(0), &[]);
    assert_eq!(control.terminal_task_snapshots().count(), 1);

    let wrong_session = AgentSession::new(
        SessionId::from_legacy_named("sess_wrong_control").expect("session"),
        AgentRole::Deliberation,
    );
    let wrong_snapshot = control.snapshot();
    let error = AgentSessionControlPlane::from_snapshot(wrong_session, wrong_snapshot)
        .expect_err("session mismatch");
    assert!(matches!(
        error,
        AgentSessionControlRestoreError::SessionMismatch { .. }
    ));
}

#[test]
fn agent_task_snapshot_owner_defaults_for_legacy_control_records() {
    let owned = AgentTaskSnapshot::new(
        AgentTaskRef::new("coding", "owned task"),
        AgentTaskLifecycleStatus::Completed,
    )
    .with_agent_definition_key("coding-agent");
    let mut legacy = serde_json::to_value(&owned).expect("serialize task snapshot");
    legacy
        .as_object_mut()
        .expect("task snapshot object")
        .remove("agent_definition_key");

    let restored: AgentTaskSnapshot =
        serde_json::from_value(legacy).expect("deserialize legacy task snapshot");

    assert_eq!(owned.agent_definition_key.as_deref(), Some("coding-agent"));
    assert_eq!(restored.agent_definition_key, None);
}

#[test]
fn agent_task_snapshot_owner_is_immutable_across_status_updates() {
    let session_id = SessionId::from_legacy_named("sess_immutable_task_owner").expect("session");
    let session =
        AgentSession::new(session_id, AgentRole::Main).with_agent_definition_key("main-agent");
    let mut control = AgentSessionControlPlane::new(session);
    let owned_task = AgentTaskRef::new("maintenance", "owned task");
    let legacy_task = AgentTaskRef::new("maintenance", "legacy unowned task");
    let op_id = TaskId::generate();

    control.set_task_status(
        &op_id,
        owned_task.clone(),
        AgentTaskLifecycleStatus::Running,
        None,
        Some(op_id.clone()),
        None,
        Vec::new(),
    );
    control.tasks.push(AgentTaskSnapshot::new(
        legacy_task.clone(),
        AgentTaskLifecycleStatus::Running,
    ));
    control.session_mut().agent_definition_key = Some("coding-agent".to_string());
    control.set_task_status(
        &op_id,
        owned_task.clone(),
        AgentTaskLifecycleStatus::Completed,
        None,
        None,
        None,
        Vec::new(),
    );
    control.set_task_status(
        &op_id,
        legacy_task.clone(),
        AgentTaskLifecycleStatus::Completed,
        None,
        None,
        None,
        Vec::new(),
    );

    assert_eq!(
        control
            .task_snapshot(&owned_task.task_id)
            .and_then(|snapshot| snapshot.agent_definition_key.as_deref()),
        Some("main-agent")
    );
    assert_eq!(
        control
            .task_snapshot(&legacy_task.task_id)
            .and_then(|snapshot| snapshot.agent_definition_key.as_deref()),
        None
    );
}

#[test]
fn agent_session_control_plane_rejects_cancels_and_times_out_active_turns() {
    let model = SequencedModel::new(Vec::<String>::new());
    let task =
        AgentTaskRef::new("coding", "active turn guard").with_owner_role(AgentRole::Deliberation);
    let config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
    let agent = Agent::new(model, "fake", "fake").with_config(config);
    let input = agent_input("should be rejected");
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
    let mut control = AgentSessionControlPlane::new(session);
    control.tasks.push(AgentTaskSnapshot::new(
        task.clone(),
        AgentTaskLifecycleStatus::Running,
    ));
    control.active_turn = Some(AgentActiveTurn::new(
        TaskId::generate(),
        99,
        Some(1_000),
        Some(task.task_id.clone()),
    ));

    let rejected = control
        .submit_op(&agent, AgentOp::start_turn(input))
        .expect("rejected op");
    assert!(!rejected.accepted);
    assert!(control.active_turn().is_some());
    assert!(rejected.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::OperationRejected { reason, .. }
                if reason.contains("active turn")
        )
    }));

    let cancelled = control
        .submit_op(&agent, AgentOp::cancel_active_turn("user cancelled"))
        .expect("cancel active");
    assert!(cancelled.accepted);
    assert!(control.active_turn().is_none());
    assert!(cancelled.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::CancellationRequested { reason, active_op_id }
                if reason == "user cancelled" && active_op_id.is_some()
        )
    }));
    assert_eq!(
        control.snapshot().tasks[0].status,
        AgentTaskLifecycleStatus::Cancelled
    );

    let timeout_task =
        AgentTaskRef::new("coding", "timeout guard").with_owner_role(AgentRole::Deliberation);
    control.tasks.clear();
    control.tasks.push(AgentTaskSnapshot::new(
        timeout_task.clone(),
        AgentTaskLifecycleStatus::Running,
    ));
    let mut active_turn = AgentActiveTurn::new(
        TaskId::generate(),
        7,
        Some(1),
        Some(timeout_task.task_id.clone()),
    );
    active_turn.started_at -= time::Duration::milliseconds(10);
    control.active_turn = Some(active_turn);

    let timed_out = control
        .cleanup_timed_out_turn("active turn exceeded timeout")
        .expect("timed out");

    assert!(control.active_turn().is_none());
    assert!(timed_out.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::TurnTimedOut { timeout_ms: 1, reason, .. }
                if reason == "active turn exceeded timeout"
        )
    }));
    assert_eq!(
        control.snapshot().tasks[0].status,
        AgentTaskLifecycleStatus::TimedOut
    );
}

#[test]
fn agent_session_runtime_with_data_dir_storage_persists_completed_session_and_trace() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-storage");
    let data_dir = SinomoDataDir::new(&data_root);
    let input = agent_input("persist this completed runtime turn");
    let session_id = input.session.session_id.clone();
    let task = AgentTaskRef::new(
        "background_maintenance",
        "persist a provider-neutral activity projection",
    )
    .with_owner_role(AgentRole::Background);
    let agent = Agent::new(
        SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Persisted runtime answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]),
        "fake",
        "fake",
    )
    .with_config(
        AgentConfig::new("fake", "fake")
            .with_agent_definition(AgentDefinition::background())
            .with_task_ref(task.clone()),
    );
    let runtime = AgentSessionRuntime::with_data_dir_storage(
        agent,
        AgentSession::new(session_id.clone(), AgentRole::Background)
            .with_agent_definition_key("background-agent"),
        &data_dir,
    );

    let op = AgentOp::start_turn(input);
    let op_id = op.op_id.clone();
    let submitted = runtime.submit_op(op).expect("submit persisted turn");
    assert!(submitted.accepted);
    runtime.join_active_turn().expect("join persisted turn");
    assert_eq!(
        runtime
            .turn_result(&op_id)
            .expect("turn result")
            .and_then(|outcome| outcome.run)
            .expect("completed run")
            .final_decision
            .text,
        "Persisted runtime answer."
    );
    let completed_session = runtime.session_snapshot().expect("completed session");
    assert!(!completed_session.transcript.items.is_empty());
    drop(runtime);

    let storage = AgentRuntimeStorage::from_data_dir(&data_dir);
    let loaded = storage
        .load_session(&session_id)
        .expect("reload runtime session")
        .expect("persisted runtime session");
    assert_eq!(loaded, completed_session);

    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    assert!(
        paths
            .session_snapshot_path_for_session(&session_id)
            .is_file()
    );
    let trace_records = fs::read_to_string(paths.trace_path_for_session(&session_id))
        .expect("persisted runtime trace")
        .lines()
        .map(|line| serde_json::from_str::<AgentTraceRecord>(line).expect("trace record"))
        .collect::<Vec<_>>();
    assert!(
        trace_records
            .iter()
            .all(|record| record.session_id == session_id)
    );
    assert!(
        trace_records
            .iter()
            .any(|record| { matches!(record.record, AgentTraceRecordKind::RunStarted { .. }) })
    );
    assert!(trace_records.iter().any(|record| {
        matches!(
            &record.record,
            AgentTraceRecordKind::RunCompleted { final_decision, .. }
                if final_decision.text == "Persisted runtime answer."
        )
    }));

    let activity_path = paths.task_activity_path_for_agent("background-agent", &task.task_id);
    let activity = fs::read_to_string(activity_path)
        .expect("normal runtime persisted sanitized task activity");
    let activity_json: serde_json::Value =
        serde_json::from_str(&activity).expect("typed task activity JSON");
    assert_eq!(
        activity_json["activity"]["owner_agent_key"],
        "background-agent"
    );
    assert_eq!(
        activity_json["activity"]["activity_kind"],
        "background_maintenance"
    );
    assert_eq!(activity_json["activity"]["status"], "completed");
    let stable_activity = activity_json["activity"]
        .as_object()
        .expect("stable task activity object");
    for private_field in [
        "provider",
        "model",
        "provider_session_id",
        "active_op_id",
        "result_ref",
        "event_ids",
    ] {
        assert!(
            !stable_activity.contains_key(private_field),
            "stable activity projection leaked {private_field}: {activity}"
        );
    }
}

#[test]
fn runtime_control_history_failure_prevents_turn_start_and_surfaces_non_turn_failure() {
    let input = agent_input("do not start without durable control history");
    let session_id = input.session.session_id.clone();
    let task = AgentTaskRef::new("maintenance", "durability-gated task")
        .with_owner_role(AgentRole::Background);
    let agent = Agent::new(PanickingModel, "fake", "fake")
        .with_config(AgentConfig::new("fake", "fake").with_task_ref(task))
        .with_session_store(FailingControlHistoryStore::always());
    let runtime = AgentSessionRuntime::new(
        agent,
        AgentSession::new(session_id, AgentRole::Background)
            .with_agent_definition_key("background-agent"),
    );

    let start_error = runtime
        .submit_op(AgentOp::start_turn(input))
        .expect_err("turn start must fail when its running state is not durable");
    assert!(matches!(start_error, AgentError::SessionStore { .. }));
    assert!(runtime.active_turn().expect("active turn").is_none());
    runtime
        .join_active_turn()
        .expect("failed submission did not leave a worker");
    assert!(
        runtime
            .progress_since(AgentSessionControlEventCursor {
                offset: 0,
                last_event_id: None,
            })
            .expect("control events")
            .events
            .is_empty(),
        "uncommitted start events must not remain published in memory"
    );

    let snapshot_error = runtime
        .submit_op(AgentOp::snapshot())
        .expect_err("non-turn control persistence failure must be surfaced");
    assert!(matches!(snapshot_error, AgentError::SessionStore { .. }));
}

#[test]
fn runtime_surfaces_terminal_control_history_failure_from_join() {
    let input = agent_input("finish only after terminal control history is durable");
    let session_id = input.session.session_id.clone();
    let task = AgentTaskRef::new("maintenance", "terminal durability task")
        .with_owner_role(AgentRole::Background);
    let agent = Agent::new(
        SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Terminal state prepared.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]),
        "fake",
        "fake",
    )
    .with_config(AgentConfig::new("fake", "fake").with_task_ref(task))
    .with_session_store(FailingControlHistoryStore::terminal_task());
    let runtime = AgentSessionRuntime::new(
        agent,
        AgentSession::new(session_id, AgentRole::Background)
            .with_agent_definition_key("background-agent"),
    );

    assert!(
        runtime
            .submit_op(AgentOp::start_turn(input))
            .expect("running control state persists")
            .accepted
    );
    let error = runtime
        .join_active_turn()
        .expect_err("terminal control persistence failure must reach the caller");
    assert!(matches!(error, AgentError::SessionStore { .. }));
    assert!(runtime.active_turn().expect("active turn").is_none());
    assert!(
        runtime
            .runtime_events_since(AgentRuntimeEventCursor {
                offset: 0,
                last_event_id: None,
            })
            .expect("runtime events")
            .events
            .iter()
            .any(|event| matches!(
                &event.payload,
                AgentRuntimeEventPayload::WorkerFailed { detail, .. }
                    if detail.contains("persist agent control history failed")
            )),
        "polling clients must also observe the persistence failure"
    );
}

#[test]
fn runtime_merge_advances_revision_for_inputs_queued_during_a_turn() {
    let session_id = SessionId::generate();
    let mut base = AgentSession::new(session_id, AgentRole::Main);
    base.state_revision = 4;
    let mut live = base.clone();
    live.steering_queue
        .push(AgentSteeringMessage::new("arrived during turn"));
    live.state_revision = 5;
    let mut completed = base;
    completed.transcript.push(AgentItem::AssistantText {
        native_items: Vec::new(),
        item_id: TaskId::generate(),
        step_index: 0,
        content: "completed turn".to_string(),
    });
    completed.raw_transcript = completed.transcript.clone();
    completed.state_revision = 5;
    let mut state = AgentSessionRuntimeState {
        control: AgentSessionControlPlane::new(live),
        in_flight_input_ids: BTreeSet::new(),
        runtime_events: Vec::new(),
        completed_turns: Vec::new(),
        turn_worker_running: false,
        worker: None,
    };

    state.replace_session_preserving_live_inputs(completed, 4, &BTreeSet::new());

    assert_eq!(state.control.session.state_revision, 6);
    assert_eq!(state.control.session.steering_queue.len(), 1);
    assert_eq!(
        state.control.session.steering_queue[0].message,
        "arrived during turn"
    );
    assert_eq!(state.control.session.transcript.items.len(), 1);
}

#[test]
fn runtime_persists_inputs_queued_during_a_completed_turn() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-mid-turn-input");
    let data_dir = SinomoDataDir::new(&data_root);
    let input = agent_input("block while steering arrives");
    let session_id = input.session.session_id.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let agent = Agent::new(
        BlockingModel {
            started: started_tx,
            release: Mutex::new(release_rx),
        },
        "fake",
        "fake",
    );
    let mut initial_session = AgentSession::new(session_id.clone(), AgentRole::Main);
    initial_session
        .steering_queue
        .push(AgentSteeringMessage::new("consume this in the active turn"));
    initial_session.advance_state_revision();
    let runtime = AgentSessionRuntime::with_data_dir_storage(agent, initial_session, &data_dir);

    assert!(
        runtime
            .submit_op(AgentOp::start_turn(input))
            .expect("submit turn")
            .accepted
    );
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("model started");
    assert!(
        runtime
            .submit_op(AgentOp::steer("preserve this steering"))
            .expect("steer during turn")
            .accepted
    );
    let during_turn = AgentRuntimeStorage::from_data_dir(&data_dir)
        .load_session(&session_id)
        .expect("load in-flight session")
        .expect("in-flight session persisted by steering");
    assert_eq!(
        during_turn.steering_queue.len(),
        2,
        "the consumed baseline remains durable until turn commit"
    );
    release_tx.send(()).expect("release model");
    runtime.join_active_turn().expect("join turn");

    let merged = runtime.session_snapshot().expect("merged session");
    assert_eq!(merged.state_revision, 3);
    assert_eq!(merged.steering_queue.len(), 1);
    assert_eq!(merged.steering_queue[0].message, "preserve this steering");
    let persisted = AgentRuntimeStorage::from_data_dir(&data_dir)
        .load_session(&session_id)
        .expect("load persisted session")
        .expect("persisted session");
    assert_eq!(persisted, merged);
}

#[test]
fn runtime_rejects_a_new_turn_until_a_cancelled_worker_exits() {
    let first_input = agent_input("block until the cancelled worker is released");
    let session_id = first_input.session.session_id.clone();
    let second_input = agent_input("do not start while the first worker is exiting");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let agent = Agent::new(
        BlockingModel {
            started: started_tx,
            release: Mutex::new(release_rx),
        },
        "fake",
        "fake",
    );
    let runtime = AgentSessionRuntime::new(agent, AgentSession::new(session_id, AgentRole::Main));

    assert!(
        runtime
            .submit_op(AgentOp::start_turn(first_input))
            .expect("submit first turn")
            .accepted
    );
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("first worker started");
    assert!(
        runtime
            .interrupt_active_turn("cancel without releasing the model")
            .expect("request cancellation")
            .accepted
    );
    let join_runtime = runtime.clone();
    let joiner = thread::spawn(move || join_runtime.join_active_turn());
    for _ in 0..100 {
        let joiner_owns_handle = runtime
            .state
            .lock()
            .expect("runtime state")
            .worker
            .is_none();
        if joiner_owns_handle {
            break;
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    {
        let state = runtime.state.lock().expect("runtime state");
        assert!(
            state.worker.is_none(),
            "joiner should own the worker handle"
        );
        assert!(
            state.turn_worker_running,
            "worker liveness must outlast handle ownership"
        );
    }

    let rejected = runtime
        .submit_op(AgentOp::start_turn(second_input))
        .expect("reject overlapping turn");
    assert!(!rejected.accepted);
    assert!(rejected.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::OperationRejected { reason, .. }
                if reason == "session already has an active turn worker"
        )
    }));

    release_tx.send(()).expect("release first worker");
    joiner
        .join()
        .expect("join joiner thread")
        .expect("join first worker");
}

#[test]
fn runtime_worker_panic_is_terminalized_without_fencing_the_session() {
    let input = agent_input("panic in the model worker");
    let session_id = input.session.session_id.clone();
    let mut session = AgentSession::new(session_id.clone(), AgentRole::Main);
    session
        .steering_queue
        .push(AgentSteeringMessage::new("retain after worker panic"));
    session.advance_state_revision();
    let runtime = AgentSessionRuntime::new(Agent::new(PanickingModel, "fake", "fake"), session);

    assert!(
        runtime
            .submit_op(AgentOp::start_turn(input))
            .expect("submit panicking turn")
            .accepted
    );
    runtime
        .join_active_turn()
        .expect_err("worker failure must also reach the joining caller");

    let snapshot = runtime.session_snapshot().expect("session snapshot");
    assert_eq!(snapshot.steering_queue.len(), 1);
    assert_eq!(
        snapshot.steering_queue[0].message,
        "retain after worker panic"
    );
    assert!(runtime.active_turn().expect("active turn").is_none());
    assert!(
        runtime
            .runtime_events_since(AgentRuntimeEventCursor {
                offset: 0,
                last_event_id: None,
            })
            .expect("runtime events")
            .events
            .iter()
            .any(|event| matches!(
                &event.payload,
                AgentRuntimeEventPayload::WorkerFailed { detail, .. }
                    if detail == "agent runtime failed: agent session runtime worker panicked"
            ))
    );

    runtime
        .replace_session(AgentSession::new(session_id, AgentRole::Main))
        .expect("worker panic must not fence session replacement");
}

#[test]
fn data_dir_runtime_routes_traces_after_session_replacement() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-trace-routing");
    let data_dir = SinomoDataDir::new(&data_root);
    let first_session_id = SessionId::generate();
    let second_input = agent_input("trace only the replacement session");
    let second_session_id = second_input.session.session_id.clone();
    let agent = Agent::new(
        SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Replacement session answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]),
        "fake",
        "fake",
    );
    let runtime = AgentSessionRuntime::with_data_dir_storage(
        agent,
        AgentSession::new(first_session_id.clone(), AgentRole::Main),
        &data_dir,
    );
    runtime
        .replace_session(AgentSession::new(
            second_session_id.clone(),
            AgentRole::Main,
        ))
        .expect("replace runtime session");

    let op = AgentOp::start_turn(second_input);
    assert!(runtime.submit_op(op).expect("submit turn").accepted);
    runtime.join_active_turn().expect("join replacement turn");

    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    let second_trace = fs::read_to_string(paths.trace_path_for_session(&second_session_id))
        .expect("replacement session trace");
    assert!(!second_trace.trim().is_empty());
    assert!(second_trace.lines().all(|line| {
        serde_json::from_str::<AgentTraceRecord>(line)
            .expect("trace record")
            .session_id
            == second_session_id
    }));
    let first_trace_path = paths.trace_path_for_session(&first_session_id);
    assert!(
        !first_trace_path.exists()
            || fs::read_to_string(&first_trace_path)
                .expect("initial trace")
                .trim()
                .is_empty(),
        "replacement turn must not cross-file traces into the initial session"
    );
}

#[test]
fn agent_session_runtime_replace_session_swaps_transcript() {
    let first_session_id =
        SessionId::from_legacy_named("sess_runtime_replace_first").expect("session");
    let second_session_id =
        SessionId::from_legacy_named("sess_runtime_replace_second").expect("session");
    let agent = Agent::new(SequencedModel::new(Vec::<String>::new()), "fake", "fake");
    let runtime =
        AgentSessionRuntime::new(agent, AgentSession::new(first_session_id, AgentRole::Main));
    let mut replacement = AgentSession::new(second_session_id.clone(), AgentRole::Main);
    replacement.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "remember this".to_string(),
        event_id: None,
    });
    replacement.raw_transcript = replacement.transcript.clone();

    runtime
        .replace_session(replacement.clone())
        .expect("replace session");

    let snapshot = runtime.session_snapshot().expect("snapshot");
    assert_eq!(snapshot.session_id, second_session_id);
    assert_eq!(snapshot.transcript, replacement.transcript);
    assert_eq!(snapshot.raw_transcript, replacement.raw_transcript);
    let events = runtime
        .runtime_events_since(AgentRuntimeEventCursor {
            offset: 0,
            last_event_id: None,
        })
        .expect("runtime events");
    assert!(events.events.is_empty());
}

#[cfg(not(windows))]
#[test]
fn agent_session_runtime_interrupts_active_command_turn() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-cancel");
    let task = AgentTaskRef::new("coding", "runtime cancellable command")
        .with_owner_role(AgentRole::Deliberation);
    let marker = root.join("started.txt");
    let command_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will run a long command.",
        "tool_calls": [{
            "tool": "run_command",
            "input": {"command": "printf started > started.txt; sleep 5"},
            "async": false,
            "summary": "Run a long command."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let model = SequencedModel::new([command_turn]);
    let agent_config = AgentConfig::new("fake", "fake")
        .with_task_ref(task.clone())
        .with_max_steps(4);
    let workspace_config = LocalWorkspaceAgentConfig::new(&root)
        .using_local_process_commands()
        .allowing_shell_commands(true);
    let agent = Agent::new(model, "fake", "fake")
        .with_config(agent_config)
        .with_tool_registry(with_remote_management_for_test(
            AgentToolRegistry::local_workspace_with_config(workspace_config),
        ));
    let input = agent_input("run a cancellable command");
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
    let runtime = AgentSessionRuntime::new(agent, session);
    let cursor = runtime.event_cursor().expect("cursor");
    let runtime_cursor = runtime.runtime_event_cursor().expect("runtime cursor");

    let queued = runtime
        .submit_op(AgentOp::start_turn(input))
        .expect("submit turn");
    assert!(queued.accepted);
    assert!(queued.outcome.is_none());
    assert!(runtime.active_turn().expect("active turn").is_some());
    for _ in 0..100 {
        if marker.exists() {
            break;
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(marker.exists());

    let cancelled = runtime
        .interrupt_active_turn("user stopped command")
        .expect("cancel turn");
    assert!(cancelled.accepted);
    runtime.join_active_turn().expect("join runtime worker");

    let progress = runtime.progress_since(cursor).expect("progress");
    let runtime_events = runtime
        .runtime_events_since(runtime_cursor)
        .expect("runtime events");
    assert!(runtime_events.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentRuntimeEventPayload::WorkerStarted { action, .. }
                if action == "start_turn"
        )
    }));
    assert!(runtime_events.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentRuntimeEventPayload::WorkerCompleted {
                stop_reason: AgentStopReason::Cancelled,
                ..
            }
        )
    }));
    assert!(progress.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::TurnCompleted {
                stop_reason: AgentStopReason::Cancelled,
                ..
            }
        )
    }));
    assert!(progress.task_updates.iter().any(|snapshot| {
        snapshot.task.task_id == task.task_id
            && snapshot.status == AgentTaskLifecycleStatus::Cancelled
    }));
    assert_eq!(
        runtime
            .terminal_result(&task.task_id)
            .expect("terminal result lookup")
            .expect("terminal result")
            .status,
        AgentTaskLifecycleStatus::Cancelled
    );
    let session = runtime.session_snapshot().expect("session snapshot");
    assert!(session.raw_transcript.items.iter().any(|item| {
        matches!(
            item,
            AgentItem::ToolResult {
                status: AgentToolStatus::Cancelled,
                ..
            }
        )
    }));
}

#[test]
fn agent_session_runtime_interrupts_active_model_turn() {
    let task = AgentTaskRef::new("coding", "runtime cancellable model")
        .with_owner_role(AgentRole::Deliberation);
    let model = CancellationAwareModel::default();
    let started = model.started();
    let agent_config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
    let agent = Agent::new(model, "fake", "fake").with_config(agent_config);
    let input = agent_input("run a cancellable model turn");
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
    let runtime = AgentSessionRuntime::new(agent, session);
    let cursor = runtime.event_cursor().expect("cursor");
    let runtime_cursor = runtime.runtime_event_cursor().expect("runtime cursor");

    let queued = runtime
        .submit_op(AgentOp::start_turn(input))
        .expect("submit turn");
    assert!(queued.accepted);
    for _ in 0..100 {
        if started.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(started.load(std::sync::atomic::Ordering::SeqCst));

    let cancelled = runtime
        .interrupt_active_turn("user stopped model")
        .expect("cancel turn");
    assert!(cancelled.accepted);
    runtime.join_active_turn().expect("join runtime worker");

    let progress = runtime.progress_since(cursor).expect("progress");
    let runtime_events = runtime
        .runtime_events_since(runtime_cursor)
        .expect("runtime events");
    assert!(runtime_events.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentRuntimeEventPayload::WorkerCompleted {
                stop_reason: AgentStopReason::Cancelled,
                ..
            }
        )
    }));
    assert!(!runtime_events.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentRuntimeEventPayload::WorkerFailed { .. }
        )
    }));
    assert!(progress.events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEventPayload::TurnCompleted {
                stop_reason: AgentStopReason::Cancelled,
                ..
            }
        )
    }));
    assert_eq!(
        runtime
            .terminal_result(&task.task_id)
            .expect("terminal result lookup")
            .expect("terminal result")
            .status,
        AgentTaskLifecycleStatus::Cancelled
    );
}
