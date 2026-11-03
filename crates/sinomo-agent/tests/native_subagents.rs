//! Exercise the independently built dylib through activation, discovery and real sessions.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
use sinomo_agent::*;
use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
use sinomo_runtime::{
    events::SessionId, local::SinomoDataDir, module::*, module_api::agent::*, node::NodeId,
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Model(Arc<Mutex<Vec<LanguageModelRequest>>>);
impl InteractionModel for Model {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.0.lock().unwrap().push(request.clone());
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: serde_json::to_string(&AgentLoopDecision {
                response_mode: AgentLoopResponseMode::Answer,
                text: "finished work".into(),
                tool_calls: vec![],
                background_handoff: None,
                next_state: AgentInteractionState::Listening,
            })
            .unwrap(),
            items: vec![],
            tool_calls: vec![],
            usage: None,
        })
    }
}
fn input(id: SessionId) -> AgentLoopInput {
    let session = InteractionSession::new(
        id,
        NodeId::generate(),
        InteractionSurface::new("native-test", InteractionSurfaceKind::CommandLine),
    );
    AgentLoopInput {
        latest_event: user_interaction_input_event(&session, "parent instructions"),
        session,
        transient_system_context: None,
        async_update: None,
        context_attachments: vec![],
        tool_specs: vec![],
    }
}
fn execute(
    registry: &AgentToolRegistry,
    input: &AgentLoopInput,
    value: serde_json::Value,
) -> serde_json::Value {
    let result = registry.execute(
        &input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "subagents".into(),
            input: value,
            r#async: false,
            summary: "test".into(),
        },
    );
    assert_eq!(
        result.status,
        AgentToolStatus::Succeeded,
        "{}",
        result.summary
    );
    result.output
}
fn wait(manager: &AgentSubagents, parent: &SessionId, id: &TaskId) {
    let result = manager
        .wait(parent, id, 5000, &AgentCancellationToken::new())
        .unwrap();
    assert_eq!(result.status, AgentSubagentStatus::Completed, "{result:?}");
}

#[test]
fn native_module_load_reload_catalog_and_durable_reusable_sessions() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    assert!(
        std::process::Command::new("cargo")
            .args(["build", "-p", "sinomo-subagents"])
            .current_dir(workspace)
            .status()
            .unwrap()
            .success()
    );
    let library = workspace.join("target/debug").join(format!(
        "{}sinomo_subagents{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    let requirement = NativeAgentSessionTools::requirement();
    let mut descriptor = None;
    for _ in 0..2 {
        let provider =
            unsafe { ActiveNativeComponent::load_verified(&library, &requirement) }.unwrap();
        descriptor = Some(provider.descriptor().clone());
        let definition = provider
            .invoke_sobj::<_, SessionToolDefinition>(SESSION_TOOLS_DESCRIBE, &(), None)
            .unwrap();
        let ActiveNativeComponentResponse::Value(definition) = definition else {
            panic!("definition failed");
        };
        assert_eq!(definition.name, "subagents");
        assert!(definition.skill.contains("24 hours"));
        let command = provider
            .invoke_sobj::<_, SessionCommand>(
                SESSION_TOOLS_TRANSLATE,
                &serde_json::json!({"action":"list"}),
                None,
            )
            .unwrap();
        assert!(matches!(
            command,
            ActiveNativeComponentResponse::Value(SessionCommand::List { archived: false })
        ));
        assert!(provider.unload().shutdown_ok);
    }
    let descriptor = descriptor.unwrap();
    let root = sinomo_testdir::TestDir::new("native-subagent-sessions");
    let bytes = std::fs::read(&library).unwrap();
    let content_hash = native_module_content_hash(&bytes);
    let library_name = library.file_name().unwrap().to_str().unwrap().to_string();
    let cache_key = "module/agent/subagents";
    let cache = native_module_cache_path(root.path(), cache_key, &content_hash, &library_name);
    std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
    std::fs::write(&cache, bytes).unwrap();
    write_native_dylib_module_loader_state(
        root.path(),
        &NativeDylibModuleLoaderState {
            module_id: ModuleId::from_legacy_named(&descriptor.module_id).unwrap(),
            artifact_id: None,
            content_hash,
            replaces_content_hash: None,
            cache_key: cache_key.into(),
            library_name,
            platform_triple: descriptor.platform_triple,
            native_abi_version: descriptor.native_abi_version,
            interface_version: descriptor.interface_version,
            activated_at: module_loader_applied_at_now(),
            source: "test-fixture".into(),
        },
    )
    .unwrap();
    let registrations = active_native_dylib_module_registrations(root.path());
    let mut catalog = ComponentCatalog::builder();
    for registration in registrations.unwrap() {
        catalog = catalog.register_module(registration).unwrap();
    }
    catalog.build().unwrap();

    let data_dir = SinomoDataDir::new(root.path());
    let requests = Arc::new(Mutex::new(vec![]));
    let parent_input = input(SessionId::generate());
    let parent_session =
        AgentSession::new(parent_input.session.session_id.clone(), AgentRole::Main);
    let agent = Agent::new(Model(requests.clone()), "test", "test");
    let registry = agent.tool_registry();
    let runtime =
        AgentSessionRuntime::with_data_dir_storage(agent, parent_session.clone(), &data_dir);
    runtime
        .submit_op(AgentOp::start_turn(parent_input.clone()))
        .unwrap();
    runtime.join_active_turn().unwrap();
    let descriptor = registry
        .descriptors()
        .into_iter()
        .find(|tool| tool.spec.name == "subagents")
        .unwrap();
    assert_eq!(descriptor.spec.kind, AgentToolKind::ModuleService);
    assert!(
        descriptor
            .capabilities
            .contains(&AgentToolCapability::CallModule)
    );
    assert_eq!(descriptor.toolsets, ["core", "module", "delegation"]);
    let spawned = execute(
        &registry,
        &parent_input,
        serde_json::json!({"action":"spawn","task":"specific delegated work","prompt":"SPECIALIZED CHILD PROMPT"}),
    );
    let child: AgentSubagentSnapshot = serde_json::from_value(spawned["subagent"].clone()).unwrap();
    let manager = runtime.subagents();
    wait(&manager, &parent_input.session.session_id, &child.id);
    assert_eq!(child.idle_timeout_secs, 86400);
    assert!(requests.lock().unwrap().iter().any(|r| {
        r.messages
            .iter()
            .any(|m| m.content.contains("SPECIALIZED CHILD PROMPT"))
    }));
    assert!(runtime.load_explicit_skill_context("subagents").is_ok());
    let reattached = AgentSessionRuntime::with_data_dir_storage(
        Agent::new(Model(requests.clone()), "test", "test"),
        runtime.session_snapshot().unwrap(),
        &data_dir,
    );
    assert!(
        Arc::ptr_eq(&manager, &reattached.subagents()),
        "reattach must reuse the live registry"
    );
    drop(runtime);
    let runtime = reattached;
    let before = manager
        .session_snapshot(&parent_input.session.session_id, &child.id)
        .unwrap();
    execute(
        &registry,
        &parent_input,
        serde_json::json!({"action":"message","id":child.id,"message":"second task"}),
    );
    wait(&manager, &parent_input.session.session_id, &child.id);
    assert!(
        manager
            .session_snapshot(&parent_input.session.session_id, &child.id)
            .unwrap()
            .transcript
            .items
            .len()
            > before.transcript.items.len()
    );
    execute(
        &registry,
        &parent_input,
        serde_json::json!({"action":"discard","id":child.id}),
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while !manager
        .inspect(&parent_input.session.session_id, &child.id)
        .unwrap()
        .archived
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        execute(
            &registry,
            &parent_input,
            serde_json::json!({"action":"list"})
        )["subagents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        execute(
            &registry,
            &parent_input,
            serde_json::json!({"action":"list","archived":true})
        )["subagents"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let store = FileAgentSessionStore::for_data_dir(&data_dir);
    let archived = store.load_session(&child.session_id).unwrap().unwrap();
    assert!(archived.subagent.unwrap().archived);
    assert!(!archived.transcript.is_empty());
    let released_registry = Arc::downgrade(&manager);
    drop(registry);
    drop(manager);
    drop(runtime);
    let release_deadline = Instant::now() + Duration::from_secs(2);
    while released_registry.upgrade().is_some() && Instant::now() < release_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        released_registry.upgrade().is_none(),
        "archival must release runtime ownership before cold recovery"
    );
    let restore_agent = Agent::new(Model(requests), "test", "test");
    let restored_registry = restore_agent.tool_registry();
    let restored =
        AgentSessionRuntime::with_data_dir_storage(restore_agent, parent_session, &data_dir);
    restored
        .submit_op(AgentOp::start_turn(parent_input.clone()))
        .unwrap();
    restored.join_active_turn().unwrap();
    assert_eq!(
        restored
            .subagents()
            .search(&parent_input.session.session_id, true)
            .len(),
        1
    );
    assert!(
        restored
            .subagents()
            .list(&parent_input.session.session_id)
            .is_empty()
    );
    remove_native_dylib_module_loader_state(
        root.path(),
        &ModuleId::from_legacy_named("module.subagents").unwrap(),
    )
    .unwrap();
    let unavailable = NativeAgentSessionTools::resolve(&data_dir);
    assert!(unavailable.is_err());
    let stale_call = restored_registry.execute(
        &parent_input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "subagents".into(),
            input: serde_json::json!({"action":"list"}),
            r#async: false,
            summary: "withdrawn provider".into(),
        },
    );
    assert_eq!(stale_call.status, AgentToolStatus::Failed);
    let agent = Agent::new(Model(Arc::new(Mutex::new(vec![]))), "test", "test");
    let no_module_tools = agent.tool_registry();
    let plain = AgentSessionRuntime::with_data_dir_storage(
        agent,
        AgentSession::new(SessionId::generate(), AgentRole::Main),
        &data_dir,
    );
    assert!(!no_module_tools.contains("subagents"));
    let unavailable_call = no_module_tools.execute(
        &parent_input.session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "subagents".into(),
            input: serde_json::json!({"action":"spawn","task":"must not run"}),
            r#async: false,
            summary: "no module".into(),
        },
    );
    assert_ne!(unavailable_call.status, AgentToolStatus::Succeeded);
    assert!(
        plain
            .subagents()
            .list(&plain.session_snapshot().unwrap().session_id)
            .is_empty()
    );
    for tools in [
        default_main_agent_tools(),
        default_coding_agent_tools(),
        default_background_agent_tools(),
    ] {
        assert!(tools.iter().all(|tool| tool.name != "subagents"));
    }
    // Old saved kinds remain readable as metadata; they supply no built-in
    // category or executor when the module is absent.
    let legacy_kind: AgentToolKind = serde_json::from_str("\"subagents\"").unwrap();
    assert_eq!(legacy_kind, AgentToolKind::ModuleService);
    assert_eq!(
        serde_json::to_value(&legacy_kind).unwrap(),
        "module_service"
    );
    let legacy = AgentToolDescriptor::new(AgentToolSpec::new(
        legacy_kind,
        "subagents",
        "old saved specification",
        false,
    ));
    assert_eq!(legacy.toolsets, ["module"]);

    plain
        .submit_op(AgentOp::start_turn(input(
            plain.session_snapshot().unwrap().session_id,
        )))
        .unwrap();
    plain.join_active_turn().unwrap();
}
