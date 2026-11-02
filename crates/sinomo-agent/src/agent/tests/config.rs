use super::*;

#[test]
fn native_tools_mode_executes_native_call_and_accepts_plain_answer() {
    let model = EventfulModel::new([
        (
            "",
            vec![LanguageModelStreamEvent::ToolCallCompleted {
                call_id: Some("realtime_call_1".into()),
                name: "read_file".into(),
                input: serde_json::json!({"path":"Cargo.toml"}),
            }],
        ),
        ("Done.", Vec::new()),
    ]);
    let definition = AgentDefinition::main().with_output_format(AgentOutputFormat::NativeTools);
    let wire: sinomo_runtime::api::app::agent::AgentDefinition =
        serde_json::from_value(serde_json::to_value(&definition).unwrap()).unwrap();
    let definition: AgentDefinition =
        serde_json::from_value(serde_json::to_value(&wire).unwrap()).unwrap();
    assert_eq!(definition.output_format, AgentOutputFormat::NativeTools);
    let config = AgentConfig::new("fake", "fake").with_agent_definition(definition);
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));
    let run = agent.run(&agent_input("read the file")).unwrap();
    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
    assert_eq!(
        run.tool_results[0].call.call_id.as_deref(),
        Some("realtime_call_1")
    );
    assert_eq!(run.final_decision.text, "Done.");
    assert_eq!(run.model_metrics.native_tool_call_count, 1);
    assert_eq!(run.model_metrics.repair_attempt_count, 0);
}

#[test]
fn text_output_agent_definition_returns_plain_final_answer() {
    let mut input = agent_input("status");
    input.tool_specs = Vec::new();
    let definition =
        AgentDefinition::custom("status-agent", AgentRole::Main, "Answer status plainly.")
            .with_output_format(AgentOutputFormat::Text);
    let config = AgentConfig::new("fake", "fake").with_agent_definition(definition);
    let agent =
        Agent::new(SequencedModel::new(["Plain status."]), "fake", "fake").with_config(config);

    let run = agent.run(&input).expect("run");
    let request = agent.model.requests.lock().expect("requests")[0].clone();

    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    assert_eq!(
        run.final_decision.response_mode,
        crate::AgentLoopResponseMode::Answer
    );
    assert_eq!(run.final_decision.text, "Plain status.");
    assert_eq!(
        run.model_metrics.final_decision_source,
        Some(AgentModelDecisionSource::TextFallback)
    );
    assert_eq!(run.model_metrics.text_fallback_turn_count, 1);
    assert!(run.tool_results.is_empty());
    assert_eq!(request.response_format, LanguageModelResponseFormat::Text);
    assert_eq!(request.tool_choice, None);
    assert!(
        !request.messages[0]
            .content
            .contains("Available agent tools")
    );
    assert!(
        request.messages[0]
            .content
            .contains("Respond directly in plain text")
    );
}

#[test]
fn agent_storage_paths_use_sinomo_data_dir_layout() {
    let data_root = sinomo_testdir::TestDir::new("sinomo-agent-storage");
    let data_dir = SinomoDataDir::new(&data_root);
    let session_id = SessionId::from_legacy_named("sess_agent_storage").expect("session");
    let paths = AgentStoragePaths::from_data_dir(&data_dir);

    assert_eq!(paths.root, data_dir.root().join("events").join("agent"));
    assert_eq!(paths.sessions_dir, paths.root.join("sessions"));
    assert_eq!(paths.traces_dir, paths.root.join("traces"));
    assert_eq!(paths.evidence_dir, paths.root.join("evidence"));
    assert_eq!(
        paths.session_dir_for_session(&session_id),
        paths.sessions_dir.join(session_id.as_str())
    );
    assert_eq!(
        paths.session_snapshot_path_for_session(&session_id),
        paths
            .sessions_dir
            .join(session_id.as_str())
            .join("snapshot.json")
    );
    assert_eq!(
        paths.session_events_path_for_session(&session_id),
        paths
            .sessions_dir
            .join(session_id.as_str())
            .join("events.jsonl")
    );
    assert_eq!(
        paths.session_items_path_for_session(&session_id),
        paths
            .sessions_dir
            .join(session_id.as_str())
            .join("items.jsonl")
    );
    assert_eq!(
        paths.session_projections_path_for_session(&session_id),
        paths
            .sessions_dir
            .join(session_id.as_str())
            .join("projections.jsonl")
    );
    assert_eq!(
        paths.trace_path_for_session(&session_id),
        paths
            .traces_dir
            .join(format!("{}.jsonl", session_id.as_str()))
    );
    assert_eq!(
        paths.evidence_dir_for_session(&session_id),
        paths.evidence_dir.join(session_id.as_str())
    );
    assert_eq!(
        paths.sifs_evidence_path_for_session(&session_id, "apply-patch.diff"),
        format!(
            "/events/agent/evidence/{}/apply-patch.diff",
            session_id.as_str()
        )
    );

    let artifact = AgentToolArtifactRef::sifs(
        "diff",
        paths.sifs_evidence_path_for_session(&session_id, "diff.patch"),
    );
    assert!(artifact.is_sifs());
    assert_eq!(
        artifact.uri,
        format!(
            "sifs://events/agent/evidence/{}/diff.patch",
            session_id.as_str()
        )
    );
}

#[test]
fn agent_storage_paths_are_registered_in_storage_contract() {
    let mut layout = sinomo_runtime::storage::sinomo_data_dir_layout().expect("layout");
    crate::register_storage_paths(&mut layout).expect("register agent storage");

    let owner = layout
        .owner_for(
            PathBuf::from("events")
                .join("agent")
                .join("evidence")
                .join("session")
                .join("read-file.txt"),
        )
        .expect("owner");
    match owner {
        sinomo_runtime::storage::StoragePathOwner::Escape(escape) => {
            assert_eq!(escape.module, "sinomo-agent");
            assert_eq!(escape.ring, sinomo_runtime::storage::StorageRing::R2);
        }
        sinomo_runtime::storage::StoragePathOwner::File(_) => {
            panic!("agent storage should be an escape path")
        }
    }

    let memory_owner = layout
        .owner_for("memory/records/segments/0000000000000001.sobjlog")
        .expect("memory owner");
    match memory_owner {
        sinomo_runtime::storage::StoragePathOwner::Escape(escape) => {
            assert_eq!(escape.module, "sinomo-memory");
            assert_eq!(escape.ring, sinomo_runtime::storage::StorageRing::R2);
        }
        sinomo_runtime::storage::StoragePathOwner::File(_) => {
            panic!("memory log segment should be covered by the memory segment subtree")
        }
    }
}

#[test]
fn agent_stops_at_configured_step_limit() {
    let model = SequencedModel::new([r#"{
        "response_mode": "defer",
        "text": "I need one more tool.",
        "tool_calls": [{
            "tool": "read_file",
            "input": {"path": "Cargo.toml"},
            "async": false,
            "summary": "Inspect workspace metadata."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    }"#]);
    let config = AgentConfig::new("fake", "fake").with_max_steps(1);
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

    let run = agent.run(&agent_input("inspect once")).expect("run");

    assert_eq!(run.stop_reason, AgentStopReason::MaxStepsReached);
    assert_eq!(run.model_turns.len(), 1);
    assert_eq!(run.tool_results.len(), 1);
}

#[test]
fn descriptor_only_registry_blocks_execution_without_losing_contract() {
    let registry = AgentToolRegistry::descriptor_only(default_agent_tool_descriptors());
    let input = agent_input("read");
    let call = AgentToolCall {
        call_id: None,
        tool: "read_file".to_string(),
        input: serde_json::json!({"path": "Cargo.toml"}),
        r#async: false,
        summary: "read".to_string(),
    };

    let outcome = registry.execute(&input.session, 0, 0, &call);

    assert_eq!(outcome.status, AgentToolStatus::Blocked);
    assert_eq!(
        outcome.output.get("error").and_then(|value| value.as_str()),
        Some("missing_executor")
    );
}

#[test]
fn progressive_skill_disclosure_searches_and_describes_package_and_module_skills() {
    let module_id = sinomo_runtime::module::ModuleId::from_legacy_named("module.skills.rust")
        .expect("module id");
    let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
        module_id,
        "0.2.0",
        "module-skills",
        "module-skills",
        "module-skills-v1",
        sinomo_runtime::core::TrustRing::R2TrustedService,
        "module-skills",
    )
    .with_surfaces(
        sinomo_runtime::module::ModuleSurfaces::default()
            .with_declarative_resource(
                sinomo_runtime::module::ModuleDeclarativeResourceExport::skill(
                    "rust-debug",
                    "skills/rust-debug/SKILL.md",
                )
                .with_description("Debug Rust test failures. Use for failing Rust tests.")
                .with_target_agent("coding-agent")
                .with_activation_tag("debug")
                .with_embedded_text(
                    "---\nname: rust-debug\ndescription: Debug Rust test failures. Use for failing Rust tests.\n---\n\n# Rust debugging\nInspect the failing assertion first.",
                ),
            )
            .with_declarative_resource(
                sinomo_runtime::module::ModuleDeclarativeResourceExport::skill(
                    "other-agent",
                    "skills/other-agent/SKILL.md",
                )
                .with_description("A skill carrying unrelated legacy target metadata.")
                .with_target_agent("background-agent")
                .with_embedded_text(
                    "---\nname: other-agent\ndescription: A skill carrying unrelated legacy target metadata.\n---\n\n# Other agent\nRemain independently discoverable.",
                ),
            ),
    );
    let module_projection = AgentModuleToolProjection {
        registrations: vec![registration],
        loader_states: Vec::new(),
    };
    let skill_plan = AgentSkillPlan::from_package_and_modules(
        ["package-only", "rust-debug"],
        &module_projection,
        ["coding-agent"],
    );
    let registry = AgentToolRegistry::new();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let input = agent_input("use skill disclosure");
    let policy = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let run_context = AgentRunContext::default();
    let orchestrator =
        AgentToolOrchestrator::new(&policy, None, &run_context).with_skill_plan(skill_plan);

    assert!(plan.is_model_visible("skill_search"));
    assert!(plan.is_model_visible("skill_describe"));
    assert!(plan.is_model_visible("skill_activate"));

    let search = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "skill_search".to_string(),
            input: serde_json::json!({ "query": "" }),
            r#async: false,
            summary: "find rust skill".to_string(),
        },
    );
    assert_eq!(search.status, AgentToolStatus::Succeeded);
    assert!(search.evidence.record_value("skill_usage").is_some());
    let matches = search
        .output
        .get("matches")
        .and_then(serde_json::Value::as_array)
        .expect("matches");
    assert!(
        matches.iter().any(|entry| {
            entry.get("id").and_then(serde_json::Value::as_str) == Some("rust-debug")
                && entry.get("source").and_then(serde_json::Value::as_str) == Some("module")
                && entry
                    .get("package_declared")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && entry
                    .get("has_embedded_text")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && entry
                    .pointer("/activation/mode")
                    .and_then(serde_json::Value::as_str)
                    == Some("dedicated_tool")
                && entry
                    .pointer("/activation/available")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
        }),
        "{matches:?}"
    );
    assert!(
        matches.iter().any(|entry| {
            entry.get("id").and_then(serde_json::Value::as_str) == Some("other-agent")
        }),
        "module skills must remain discoverable independently of legacy target metadata: {matches:?}"
    );

    let describe = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "skill_describe".to_string(),
            input: serde_json::json!({ "skill": "rust-debug" }),
            r#async: false,
            summary: "describe rust skill".to_string(),
        },
    );
    assert_eq!(describe.status, AgentToolStatus::Succeeded);
    assert_eq!(
        describe
            .output
            .pointer("/descriptor/has_skill_content")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        describe
            .output
            .pointer("/activation/mode")
            .and_then(serde_json::Value::as_str),
        Some("dedicated_tool")
    );
    assert_eq!(
        describe
            .output
            .pointer("/activation/available")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(describe.evidence.record_value("skill_disclosure").is_some());
    assert!(describe.evidence.record_value("skill_usage").is_some());

    let activate = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        2,
        &AgentToolCall {
            call_id: None,
            tool: "skill_activate".to_string(),
            input: serde_json::json!({
                "skill": "rust-debug",
                "reason": "load debugging guidance"
            }),
            r#async: false,
            summary: "activate rust skill".to_string(),
        },
    );
    assert_eq!(activate.status, AgentToolStatus::Succeeded);
    assert_eq!(
        activate
            .output
            .pointer("/skill_content/name")
            .and_then(serde_json::Value::as_str),
        Some("rust-debug")
    );
    assert_eq!(
        activate
            .output
            .pointer("/activation/mode")
            .and_then(serde_json::Value::as_str),
        Some("dedicated_tool")
    );
    assert_eq!(
        activate
            .output
            .pointer("/activation/available")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(activate.evidence.record_value("skill_disclosure").is_some());
    assert!(activate.evidence.record_value("skill_usage").is_some());

    let unreadable_search = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        3,
        &AgentToolCall {
            call_id: None,
            tool: "skill_search".to_string(),
            input: serde_json::json!({ "query": "package-only" }),
            r#async: false,
            summary: "search unreadable package reference".to_string(),
        },
    );
    assert_eq!(
        unreadable_search
            .output
            .get("count")
            .and_then(serde_json::Value::as_u64),
        Some(0)
    );
    let unreadable_describe = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        4,
        &AgentToolCall {
            call_id: None,
            tool: "skill_describe".to_string(),
            input: serde_json::json!({ "skill": "package-only" }),
            r#async: false,
            summary: "describe unreadable package reference".to_string(),
        },
    );
    assert_eq!(unreadable_describe.status, AgentToolStatus::Failed);
    assert_eq!(
        unreadable_describe
            .output
            .get("error")
            .and_then(serde_json::Value::as_str),
        Some("invalid_agent_skill")
    );
}

#[test]
fn skill_disclosure_includes_active_catalog_skills() {
    let text = "---\nname: catalog-debug\ndescription: Catalog-backed debug workflow.\n---\n\n# Catalog skill\nUse the SIFS catalog version.";
    let entry = crate::SkillCatalogEntry::new(
        "catalog-debug",
        "1.0.0",
        crate::SkillCatalogSource::user("user.test"),
        text,
    )
    .with_description("Catalog-backed debug workflow.")
    .with_activation_tags(vec!["debug".to_string()]);
    let mut catalog = crate::SkillCatalog::default();
    catalog.upsert_entry(entry.clone()).expect("catalog entry");
    catalog
        .set_activation(crate::SkillActivationRecord::new(
            crate::SkillActivationScope::Cluster.as_str(),
            &entry,
            "test",
            "catalog default",
        ))
        .expect("activation");
    let catalog_entries =
        catalog.active_entries_for_scopes([crate::SkillActivationScope::Cluster.as_str()]);
    let skill_plan = AgentSkillPlan::from_package_modules_and_catalog(
        std::iter::empty::<String>(),
        &AgentModuleToolProjection::default(),
        ["coding-agent"],
        catalog_entries
            .into_iter()
            .map(|entry| entry.to_agent_descriptor(Some(text.to_string()))),
    );

    let registry = AgentToolRegistry::new();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let input = agent_input("use catalog skill");
    let policy = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let run_context = AgentRunContext::default();
    let orchestrator =
        AgentToolOrchestrator::new(&policy, None, &run_context).with_skill_plan(skill_plan);
    let search = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "skill_search".to_string(),
            input: serde_json::json!({ "query": "catalog" }),
            r#async: false,
            summary: "find catalog skill".to_string(),
        },
    );
    assert_eq!(search.status, AgentToolStatus::Succeeded);
    let matches = search
        .output
        .get("matches")
        .and_then(serde_json::Value::as_array)
        .expect("matches");
    assert!(
        matches.iter().any(|value| {
            value.get("id").and_then(serde_json::Value::as_str) == Some("catalog-debug")
                && value.get("source").and_then(serde_json::Value::as_str) == Some("catalog")
                && value
                    .get("catalog_version")
                    .and_then(serde_json::Value::as_str)
                    == Some("1.0.0")
                && value
                    .get("catalog_content_hash")
                    .and_then(serde_json::Value::as_str)
                    == Some(entry.content_hash.as_str())
        }),
        "{matches:?}"
    );
}

#[test]
fn agent_skill_plan_includes_cluster_active_catalog_skills() {
    let text = "---\nname: catalog-shortcut\ndescription: Catalog-backed shortcut workflow.\n---\n\n# Catalog shortcut\nUse the catalog shortcut.";
    let entry = crate::SkillCatalogEntry::new(
        "catalog-shortcut",
        "1.0.0",
        crate::SkillCatalogSource::user("user.test"),
        text,
    )
    .with_description("Catalog-backed shortcut workflow.");
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
        .with_skill_catalog_texts([(
            crate::skill_catalog_entry_text_key(&entry),
            text.to_string(),
        )]);
    let agent = Agent::new(SequencedModel::new(["{}"]), "fake", "fake").with_config(config);

    let skill_plan = agent.skill_plan();
    let skill = skill_plan.get("catalog-shortcut").expect("catalog skill");
    assert_eq!(skill.source, crate::tools::AgentSkillSource::Catalog);
    assert_eq!(skill.catalog_version.as_deref(), Some("1.0.0"));
    assert_eq!(
        skill.catalog_content_hash.as_deref(),
        Some(entry.content_hash.as_str())
    );
    assert_eq!(skill.embedded_text.as_deref(), Some(text));
}

#[test]
fn agent_can_answer_task_that_requires_catalog_skill_text() {
    let text = "---\nname: verification-howto\ndescription: Contains the hidden verification phrase. Use when asked for verification.\n---\n\n# Howto\nWhen asked for the verification phrase, answer `violet-ridge`.\n";
    let entry = crate::SkillCatalogWrite::from_markdown_skill(
        None,
        "1.0.0",
        crate::SkillCatalogSource::user("user.test"),
        text,
    )
    .expect("standard skill")
    .entry;
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
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "I need to activate the howto skill.",
            "tool_calls": [{
                "tool": "skill_activate",
                "input": {"skill": "verification-howto"},
                "async": false,
                "summary": "Activate the howto skill."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "violet-ridge",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let config = AgentConfig::new("fake", "fake")
        .with_agent_definition(AgentDefinition::main())
        .with_skill_catalog(catalog)
        .with_skill_catalog_texts([(
            crate::skill_catalog_entry_text_key(&entry),
            text.to_string(),
        )]);
    let agent = Agent::new(model, "fake", "fake").with_config(config);
    let probe_input = agent_input("What is the verification phrase?");
    let context = agent
        .assemble_context(&probe_input, &[], 0)
        .expect("context");
    assert_eq!(
        context.available_skills,
        vec![AvailableAgentSkill {
            name: "verification-howto".to_string(),
            description:
                "Contains the hidden verification phrase. Use when asked for verification."
                    .to_string(),
        }]
    );
    let probe_plan = agent.tool_plan(&probe_input);
    assert!(
        probe_plan.is_executable("skill_activate"),
        "skill_activate plan entry: {:?}",
        probe_plan.entry("skill_activate")
    );

    let run = agent.run(&probe_input).expect("run");

    assert_eq!(run.final_decision.text, "violet-ridge");
    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].call.tool, "skill_activate");
    assert_eq!(
        run.tool_results[0].status,
        AgentToolStatus::Succeeded,
        "skill_activate output: {}",
        run.tool_results[0].output
    );
    assert!(
        run.tool_results[0].output["skill_content"]["instructions"]
            .as_str()
            .is_some_and(|value| value.contains("violet-ridge"))
    );
    assert!(
        run.tool_results[0]
            .evidence
            .record_value("skill_usage")
            .is_some()
    );
}

#[test]
fn skill_activation_loads_bundled_resources_on_demand() {
    let markdown = "---\nname: scripted-skill\ndescription: Run a bundled script. Use for scripted verification.\n---\n\n# Scripted skill\nRead `scripts/check.sh` when needed.\n";
    let resource = crate::AgentSkillPackageFile {
        relative_path: "scripts/check.sh".to_string(),
        bytes: b"#!/bin/sh\nprintf verified\n".to_vec(),
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
    let skill_plan = agent.skill_plan();
    let registry = AgentToolRegistry::new();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::default(),
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let input = agent_input("read the scripted skill resource");
    let policy = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let run_context = AgentRunContext::default();
    let orchestrator =
        AgentToolOrchestrator::new(&policy, None, &run_context).with_skill_plan(skill_plan);

    let outcome = orchestrator.execute(
        &registry,
        &input.session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "skill_activate".to_string(),
            input: serde_json::json!({
                "skill": "scripted-skill",
                "resource": "scripts/check.sh",
            }),
            r#async: false,
            summary: "read bundled script".to_string(),
        },
    );

    assert_eq!(outcome.status, AgentToolStatus::Succeeded);
    assert_eq!(
        outcome
            .output
            .pointer("/resource_content/content")
            .and_then(serde_json::Value::as_str),
        Some("#!/bin/sh\nprintf verified\n")
    );
    assert_eq!(
        outcome
            .evidence
            .record_value("skill_usage")
            .and_then(|value| value.get("operation"))
            .and_then(serde_json::Value::as_str),
        Some("resource_read")
    );
}

#[test]
fn agent_can_persist_learned_shortcut_as_skill_update() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-update-task");
    let data_dir = SinomoDataDir::new(&root);
    let _native = sinomo_runtime::vfs::native_test_support::NativeMountedOwner::start_in_data_dir(
        "sinomo-agent-skill-update-task",
        data_dir.clone(),
    )
    .expect("native SIFS owner");
    let store = crate::SkillCatalogSifsStore::open(Some(data_dir.root())).expect("store");
    let original_text = "---\nname: shortcut-runner\ndescription: Run the long path.\n---\n\n# Shortcut Runner\nUse the long path.\n";
    let original_write = crate::SkillCatalogWrite::from_markdown_skill(
        Some("shortcut-runner".to_string()),
        "1.0.0",
        crate::SkillCatalogSource::user("test"),
        original_text,
    )
    .expect("original write")
    .activate_for_scope(
        crate::SkillActivationScope::Cluster.as_str(),
        "test",
        "seed",
    );
    let original_entry = original_write.entry.clone();
    store
        .write_skill_version(original_write)
        .expect("write original");
    let model = SequencedModel::new([
        r#"{
            "response_mode": "defer",
            "text": "The task revealed a shortcut; I will update the skill.",
            "tool_calls": [{
                "tool": "skill_update",
                "input": {
                    "skill": "shortcut-runner",
                    "version": "1.1.0",
                    "source_id": "job-shortcut-task",
                    "reason": "task revealed the fast path",
                    "markdown": "---\nname: shortcut-runner\ndescription: Run the fast path.\n---\n\n# Shortcut Runner\nUse shortcut command `fast-run` before the long path.\n"
                },
                "async": false,
                "summary": "Persist the learned shortcut."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        }"#,
        r#"{
            "response_mode": "answer",
            "text": "Recorded the shortcut for future runs.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let agent = Agent::new(model, "fake", "fake").with_tool_registry(crate::skill_registry(
        crate::SkillAgentToolConfig::new(data_dir.clone()),
    ));

    let run = agent
        .run(&agent_input("The shortcut is fast-run; update the skill."))
        .expect("run");

    assert_eq!(
        run.final_decision.text,
        "Recorded the shortcut for future runs."
    );
    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].call.tool, "skill_update");
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
    assert!(
        run.tool_results[0]
            .evidence
            .record_value("skill_evolution")
            .is_some()
    );
    let catalog = store.load_catalog().expect("catalog");
    let active = catalog
        .active_entry_for_scope(
            crate::SkillActivationScope::Cluster.as_str(),
            "shortcut-runner",
        )
        .expect("active shortcut");
    assert_eq!(active.version, "1.1.0");
    assert_eq!(active.source.kind, crate::SkillCatalogSourceKind::Evolution);
    assert_eq!(
        active.base_hash.as_deref(),
        Some(original_entry.content_hash.as_str())
    );
    let updated_text = store
        .read_skill_text(active)
        .expect("read updated")
        .expect("updated text");
    assert!(updated_text.contains("fast-run"));
}

#[test]
fn agent_records_patch_snapshot_items_for_edit_file() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-loop-patch");
    let evidence_dir = root.join("evidence");
    fs::create_dir_all(&root).expect("workspace");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let tool_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will edit the file.",
        "tool_calls": [{
            "tool": "edit_file",
            "input": {
                "path": "hello.txt",
                "old_text": "old\n",
                "new_text": "new\n"
            },
            "async": false,
            "summary": "Edit hello.txt."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let model = SequencedModel::new([
        tool_turn,
        r#"{
            "response_mode": "answer",
            "text": "Patched.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#
        .to_string(),
    ]);
    let config = LocalWorkspaceAgentConfig::new(&root)
        .allowing_file_writes(true)
        .with_evidence_dir(&evidence_dir);
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::local_workspace_with_config(config));

    let run = agent.run(&agent_input("patch file")).expect("run");

    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("file"),
        "new\n"
    );
    let patch_snapshot = run
        .items
        .iter()
        .find_map(|item| {
            if let AgentItem::PatchSnapshot {
                status,
                changed_files,
                patch_ref,
                ..
            } = item
            {
                Some((status, changed_files, patch_ref))
            } else {
                None
            }
        })
        .expect("patch snapshot");
    assert_eq!(patch_snapshot.0, &AgentToolStatus::Succeeded);
    assert_eq!(patch_snapshot.1, &vec!["hello.txt".to_string()]);
    assert!(patch_snapshot.2.is_none());
}

#[test]
fn agent_run_collects_candidate_evidence_bundle() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-candidate-evidence");
    let evidence_dir = root.join("evidence");
    fs::create_dir_all(&root).expect("workspace");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let tool_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will edit the file.",
        "tool_calls": [
            {
                "tool": "edit_file",
                "input": {
                    "path": "hello.txt",
                    "old_text": "old\n",
                    "new_text": "new\n"
                },
                "async": false,
                "summary": "Edit hello.txt."
            }
        ],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let model = SequencedModel::new([
        tool_turn,
        r#"{
            "response_mode": "answer",
            "text": "Candidate is ready.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#
        .to_string(),
    ]);
    let config = LocalWorkspaceAgentConfig::new(&root)
        .allowing_file_writes(true)
        .with_evidence_dir(&evidence_dir);
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(AgentToolRegistry::local_workspace_with_config(config));
    let mut input = agent_input("build a candidate");
    input.context_attachments.push(AgentContextAttachment {
        kind: "sinomo.evolution.mutation".to_string(),
        summary: "candidate mutation context".to_string(),
        payload_json: serde_json::json!({
            "objective": "build a candidate"
        }),
        payload_original_bytes: None,
        payload_model_visible_truncated: false,
    });

    let run = agent.run(&input).expect("run");

    let bundle = run
        .candidate_evidence_bundle
        .expect("candidate evidence bundle");
    assert_eq!(bundle.final_response, "Candidate is ready.");
    assert!(bundle.evidence.iter().any(|evidence| {
        matches!(
            evidence,
            AgentCandidateEvidence::Patch { changed_files, .. }
                if changed_files == &vec!["hello.txt".to_string()]
        )
    }));
}
