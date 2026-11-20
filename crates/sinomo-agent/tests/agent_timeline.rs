#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#[path = "fixtures/native_module.rs"]
mod native_module;

use sinomo_agent::session_access::AgentSessionAccess;
use sinomo_agent::*;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::{
    local::SinomoDataDir,
    module::sessions::{SessionAccess, SessionHostCallbacks},
    module::*,
    module_api::{panels::*, sessions::*, *},
};
use std::{path::Path, sync::Arc};
fn install(root: &Path, library: &Path) -> NativeModuleDescriptor {
    let bytes = std::fs::read(library).unwrap();
    let hash = native_module_content_hash(&bytes);
    let module = unsafe { NativeModule::load_verified(library) }.unwrap();
    let descriptor = module.descriptor().clone();
    assert!(module.unload().shutdown_ok);
    let name = library.file_name().unwrap().to_str().unwrap().to_owned();
    let key = format!("test/{}", descriptor.module_id);
    let path = native_module_cache_path(root, &key, &hash, &name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
    write_native_dylib_module_loader_state(
        root,
        &NativeDylibModuleLoaderState {
            module_id: ModuleId::from_legacy_named(&descriptor.module_id).unwrap(),
            artifact_id: None,
            content_hash: hash,
            replaces_content_hash: None,
            cache_key: key,
            library_name: name,
            platform_triple: descriptor.platform_triple.clone(),
            native_abi_version: descriptor.native_abi_version,
            interface_version: descriptor.interface_version.clone(),
            activated_at: module_loader_applied_at_now(),
            source: "test".into(),
        },
    )
    .unwrap();
    descriptor
}
fn render_request(conversation_id: String) -> SessionPanelRequest {
    let color = |r, g, b| UiColor {
        red: r,
        green: g,
        blue: b,
        alpha: 255,
    };
    SessionPanelRequest {
        conversation_id,
        contribution_id: "module.agent-timeline.conversation".into(),
        expected_generation: None,
        module_id: "module.agent-timeline".into(),
        render: PanelRenderRequest {
            viewport: UiViewport {
                width: 960.,
                height: 340.,
                scale_factor: 1.,
            },
            theme: UiThemeSnapshot {
                mode: UiThemeMode::Dark,
                accent: color(90, 170, 240),
                foreground: color(240, 240, 240),
                muted_foreground: color(150, 160, 170),
                background: color(20, 24, 31),
                surface: color(40, 45, 55),
                danger: color(230, 70, 80),
            },
            event: None,
            state: serde_json::Value::Null,
        },
    }
}
fn record_tool(store: &FileAgentSessionStore, id: &SessionId, name: &str) {
    let call = TaskId::generate();
    let op = AgentItemOrigin::for_op(TaskId::generate(), []);
    store
        .append_items(
            id,
            &op,
            &[
                AgentItem::ToolCall {
                    item_id: TaskId::generate(),
                    tool_call_id: call.clone(),
                    step_index: 0,
                    call_index: 0,
                    call: AgentToolCall {
                        call_id: None,
                        tool: name.into(),
                        input: serde_json::json!({"command":"check"}),
                        r#async: false,
                        summary: "check".into(),
                    },
                },
                AgentItem::ToolResult {
                    item_id: TaskId::generate(),
                    step_index: 0,
                    call_index: 0,
                    call_id: call,
                    native_call_id: None,
                    tool_name: Some(name.into()),
                    status: AgentToolStatus::Succeeded,
                    summary: "done".into(),
                    output: serde_json::json!({"ok":true}),
                    evidence: AgentToolEvidence::default(),
                },
            ],
        )
        .unwrap();
}
#[test]
fn native_timeline_reads_channel_sessions_and_optional_related_sessions() {
    let root = sinomo_testdir::TestDir::new("agent-timeline");
    let data_dir = SinomoDataDir::new(root.path());
    let descriptor = install(root.path(), &native_module::build("sinomo-agent-timeline"));
    let mut channel = AgentChannel::new_channel(SessionId::generate(), "Team");
    channel.ensure_participant("planner", "Planner");
    channel.ensure_participant("builder", "Builder");
    let sessions = FileAgentSessionStore::for_data_dir(&data_dir);
    for participant in &channel.participants {
        sessions
            .save_session(&AgentSession::new(
                participant.agent_session_id.clone(),
                AgentRole::Main,
            ))
            .unwrap();
        record_tool(&sessions, &participant.agent_session_id, "read_file");
    }
    channel.append_agent_message(
        &channel.participants[0].clone(),
        "Please review the result",
        AgentChannelSource::app(),
    );
    FileAgentChannelStore::for_data_dir(&data_dir)
        .save_channel(&channel)
        .unwrap();
    let request = render_request(channel.channel_id.to_string());
    let response = sinomo_agent::session_panels::render_panel(&data_dir, request.clone()).unwrap();
    validate_response(&response).unwrap();
    assert_eq!(response.frame.semantics.len(), 3);
    let access = Arc::new(
        AgentSessionAccess::for_conversation(&data_dir, &channel.channel_id.to_string()).unwrap(),
    );
    let foreign = SessionId::generate();
    sessions
        .save_session(&AgentSession::new(foreign.clone(), AgentRole::Main))
        .unwrap();
    assert!(
        access
            .request(SessionRequest::Inspect {
                session_id: foreign.to_string()
            })
            .is_err()
    );
    assert!(
        access
            .request(SessionRequest::Command {
                command: SessionCommand::Cancel {
                    id: channel.participants[0].agent_session_id.to_string()
                }
            })
            .is_err()
    );
    assert!(
        access
            .request(SessionRequest::Activity {
                session_id: channel.participants[0].agent_session_id.to_string(),
                after_sequence: None,
                tail: true,
                limit: usize::MAX
            })
            .is_err()
    );
    let requirement = ServiceRequirement::new(PANEL_INTERFACE, "1.0.0")
        .from_service("agent-timeline-panel")
        .with_operation(PANEL_RENDER);
    for _ in 0..2 {
        let native = unsafe { ActiveNativeComponent::resolve(root.path(), &requirement) }.unwrap();
        let missing = native
            .invoke_sobj::<_, PanelRenderResponse>(PANEL_RENDER, &request.render, None)
            .unwrap();
        assert!(matches!(missing, ActiveNativeComponentResponse::Error(_)));
        let valid = native
            .invoke_sobj::<_, PanelRenderResponse>(
                PANEL_RENDER,
                &request.render,
                Some(Arc::new(SessionHostCallbacks {
                    sessions: access.clone(),
                })),
            )
            .unwrap();
        assert!(matches!(valid, ActiveNativeComponentResponse::Value(_)));
        assert!(native.unload().shutdown_ok);
    }
    install(root.path(), &native_module::build("sinomo-subagents"));
    // A waiting module invocation must not prevent another session from
    // entering the same provider to deliver the event it is waiting for.
    struct WaitAccess {
        started: std::sync::mpsc::Sender<()>,
        done: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl SessionAccess for WaitAccess {
        fn request(&self, _: SessionRequest) -> Result<serde_json::Value, String> {
            self.started.send(()).unwrap();
            self.done
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(3))
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"ready":true}))
        }
    }
    struct ReleaseAccess(std::sync::mpsc::Sender<()>);
    impl SessionAccess for ReleaseAccess {
        fn request(&self, _: SessionRequest) -> Result<serde_json::Value, String> {
            self.0.send(()).unwrap();
            Ok(serde_json::json!({"ready":true}))
        }
    }
    let tools = NativeAgentSessionTools::resolve_shared(&data_dir).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiting = tools.clone();
    let worker = std::thread::spawn(move || {
        waiting.execute(
            serde_json::json!({"action":"wait","id":"child"}),
            Arc::new(WaitAccess {
                started: started_tx,
                done: std::sync::Mutex::new(done_rx),
            }),
        )
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let began = std::time::Instant::now();
    tools
        .execute(
            serde_json::json!({"action":"list"}),
            Arc::new(ReleaseAccess(done_tx)),
        )
        .unwrap();
    assert!(
        began.elapsed() < std::time::Duration::from_secs(1),
        "concurrent session calls must not wait for a waiting invocation"
    );
    worker.join().unwrap().unwrap();

    struct ReviewTool;
    impl AgentTool for ReviewTool {
        fn descriptor(&self) -> AgentToolDescriptor {
            AgentToolDescriptor::new(AgentToolSpec::new(
                AgentToolKind::ReadFile,
                "review_changes",
                "Review fixture",
                false,
            ))
        }
        fn execute(&self, _: AgentToolRequest<'_>) -> AgentToolResult {
            std::thread::sleep(std::time::Duration::from_millis(150));
            AgentToolResult::succeeded("Reviewed", serde_json::json!({"ok":true}))
        }
    }
    let answer = r#"{"response_mode":"answer","text":"Ready","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#;
    let tool_call = r#"{"response_mode":"tool_calls","text":"Reviewing","tool_calls":[{"tool":"review_changes","input":{},"async":false,"summary":"Review"}],"background_handoff":null,"next_state":"waiting"}"#;
    let model = sinomo_lm::ScriptedProvider::new(
        sinomo_lm::ModelScript::new()
            .reply(sinomo_lm::ScriptedReply::new().text(answer))
            .reply(sinomo_lm::ScriptedReply::new().text(tool_call))
            .reply(sinomo_lm::ScriptedReply::new().text(answer)),
    );
    let agent = Agent::new(model, "fixture", "fixture")
        .with_tool_registry(AgentToolRegistry::new().with_tool(ReviewTool));
    let registry = agent.tool_registry();
    let parent_id = channel.participants[0].agent_session_id.clone();
    let runtime = AgentSessionRuntime::with_data_dir_storage(
        agent,
        sessions.load_session(&parent_id).unwrap().unwrap(),
        &data_dir,
    );
    let interaction = InteractionSession::new(
        parent_id.clone(),
        sinomo_runtime::node::NodeId::generate(),
        InteractionSurface::new("timeline-test", InteractionSurfaceKind::CommandLine),
    );
    runtime
        .submit_op(AgentOp::start_turn(AgentLoopInput {
            latest_event: user_interaction_input_event(&interaction, "Delegate review"),
            session: interaction.clone(),
            transient_system_context: None,
            async_update: None,
            context_attachments: vec![],
            tool_specs: vec![],
        }))
        .unwrap();
    runtime.join_active_turn().unwrap();
    let result = registry.execute(
        &interaction,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "subagents".into(),
            input: serde_json::json!({"action":"spawn","name":"Reviewer","task":"Review changes"}),
            r#async: false,
            summary: "delegate".into(),
        },
    );
    assert_eq!(
        result.status,
        AgentToolStatus::Succeeded,
        "{}",
        result.summary
    );
    let child: ManagedSessionSnapshot =
        serde_json::from_value(result.output["subagent"].clone()).unwrap();
    assert_eq!(
        runtime
            .sessions()
            .wait(&parent_id, &child.id, 5000, &AgentCancellationToken::new())
            .unwrap()
            .status,
        ManagedSessionStatus::Completed
    );
    assert_eq!(
        sessions
            .load_session(&child.session_id)
            .unwrap()
            .unwrap()
            .relationship
            .unwrap()
            .parent_session_id,
        parent_id.to_string()
    );
    assert!(sessions.load_item_records(&child.session_id).unwrap().iter().any(|record|matches!(&record.item,AgentItem::ToolResult{tool_name:Some(name),status:AgentToolStatus::Succeeded,..} if name=="review_changes")));
    let mut legacy = sessions.load_session(&child.session_id).unwrap().unwrap();
    legacy.relationship = None;
    sessions.save_session(&legacy).unwrap();
    let mut response =
        sinomo_agent::session_panels::render_panel(&data_dir, request.clone()).unwrap();
    if !response.frame.semantics.iter().any(|node| {
        node.name
            .as_deref()
            .is_some_and(|name| name.contains("Reviewer: review_changes"))
    }) {
        let mut scrolled = request.clone();
        scrolled.render.state = response.state.clone();
        scrolled.render.event = Some(UiInputEvent::Key {
            key: "ArrowDown".into(),
            pressed: true,
            repeat: false,
            modifiers: UiModifiers::default(),
        });
        response = sinomo_agent::session_panels::render_panel(&data_dir, scrolled).unwrap();
    }
    assert!(response.frame.semantics.iter().any(|node| {
        node.name
            .as_deref()
            .is_some_and(|name| name.contains("Reviewer: review_changes"))
    }));
    assert!(response.frame.semantics.len() >= 4);
    assert!(
        response.frame.semantics.iter().any(|node| node
            .name
            .as_deref()
            .unwrap_or("")
            .contains("Reviewer"))
    );
    runtime.sessions().dispose(&parent_id, &child.id).unwrap();
    let registrations = active_native_dylib_module_registrations(root.path()).unwrap();
    let mut catalog = ComponentCatalog::builder();
    for registration in registrations {
        catalog = catalog.register_module(registration).unwrap();
    }
    let catalog = catalog.build().unwrap();
    let snapshots = ModuleRegistrySnapshotStore::new(catalog.modules().clone()).unwrap();
    let ui = ui_extension_catalog_response(
        &snapshots.active_snapshot().unwrap(),
        &UiExtensionCatalogRequest {
            extension_point_id: Some(CONVERSATION_PANELS.into()),
        },
        root.path(),
    )
    .unwrap();
    assert_eq!(ui.entries.len(), 1);
    let mut stale = request.clone();
    stale.expected_generation = Some(ui.registry_generation + 1);
    assert!(sinomo_agent::session_panels::render_panel(&data_dir, stale).is_err());
    assert_eq!(
        ui.entries[0].contribution.contribution_id,
        "module.agent-timeline.conversation"
    );
    // Resident lifecycle state fences direct native bindings even before
    // the persisted loader record is removed by hot unload.
    let snapshot = snapshots.active_snapshot().unwrap();
    let extensions = Arc::new(compile_default_extension_registry(&snapshot).unwrap());
    let host = ModuleRuntimeHost::new(snapshot.clone(), extensions, 1).unwrap();
    let _context =
        ComponentContext::new(Arc::new(data_dir.clone())).with_module_runtime(host.clone());
    struct PendingWait(std::sync::mpsc::Sender<()>);
    impl SessionAccess for PendingWait {
        fn request(&self, request: SessionRequest) -> Result<serde_json::Value, String> {
            let SessionRequest::Command {
                command: SessionCommand::Wait { timeout_ms, .. },
            } = request
            else {
                return Err("expected wait".into());
            };
            let _ = self.0.send(());
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms));
            Ok(serde_json::json!({"status":"running","state":{"status":"running"}}))
        }
    }
    let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
    let tools = tools.clone();
    let cancelled = std::thread::spawn(move || {
        tools.execute(
            serde_json::json!({"action":"wait","id":"child","timeout_ms":60000}),
            Arc::new(PendingWait(waiting_tx)),
        )
    });
    waiting_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let subagent_id = ModuleId::from_legacy_named("module.subagents").unwrap();
    assert_eq!(
        host.native_operations.cancel_module(subagent_id.as_str()),
        1
    );
    let cancelled_at = std::time::Instant::now();
    assert!(cancelled.join().unwrap().is_err());
    assert!(cancelled_at.elapsed() < std::time::Duration::from_secs(1));
    assert_eq!(
        host.native_operations
            .active_for_module(subagent_id.as_str()),
        0
    );
    let held = unsafe { ActiveNativeComponent::resolve_shared(root.path(), &requirement) }.unwrap();
    let timeline_id = ModuleId::from_legacy_named("module.agent-timeline").unwrap();
    assert_eq!(
        host.invocations.begin_drain(timeline_id.as_str()).unwrap(),
        1
    );
    assert!(sinomo_agent::session_panels::render_panel(&data_dir, request.clone()).is_err());
    assert!(
        held.invoke_sobj::<_, PanelRenderResponse>(PANEL_RENDER, &request.render, None)
            .is_err(),
        "retained bindings cannot admit calls after drain begins"
    );
    drop(held);
    assert_eq!(
        host.invocations
            .status(timeline_id.as_str())
            .unwrap()
            .active_invocations,
        0
    );
    host.invocations
        .complete_unload(timeline_id.as_str())
        .unwrap();
    assert!(
        descriptor
            .component
            .requirements
            .iter()
            .any(|r| r.interface_id == SESSION_INTERFACE)
    );
    let artifact = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/change-scope/agent-timeline/frame.sobj");
    std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
    std::fs::write(artifact, encode_sobj(&response.frame).unwrap()).unwrap();
    remove_native_dylib_module_loader_state(
        root.path(),
        &ModuleId::from_legacy_named("module.agent-timeline").unwrap(),
    )
    .unwrap();
    assert!(sinomo_agent::session_panels::render_panel(&data_dir, request).is_err());
}

#[test]
fn scoped_activity_pages_mark_previews_without_mutating_the_journal() {
    let root = sinomo_testdir::TestDir::new("session-page-previews");
    let data = SinomoDataDir::new(root.path());
    let store = FileAgentSessionStore::for_data_dir(&data);
    let id = SessionId::generate();
    store
        .save_session(&AgentSession::new(id.clone(), AgentRole::Main))
        .unwrap();
    record_tool(&store, &id, "small");
    store.append_items(&id,&AgentItemOrigin::for_op(TaskId::generate(),[]),&[AgentItem::ToolCall{item_id:TaskId::generate(),tool_call_id:TaskId::generate(),step_index:0,call_index:0,call:AgentToolCall{call_id:None,tool:"large".into(),input:serde_json::json!({"entries":(0..100).collect::<Vec<_>>(),"text":"x".repeat(5000)}),r#async:false,summary:"large input".into()}}]).unwrap();
    let access = AgentSessionAccess::new(
        Arc::new(store.clone()),
        [(id.to_string(), "Agent".into())].into_iter().collect(),
    );
    let latest: SessionActivityPage = serde_json::from_value(
        access
            .request(SessionRequest::Activity {
                session_id: id.to_string(),
                after_sequence: None,
                tail: true,
                limit: 1,
            })
            .unwrap(),
    )
    .unwrap();
    assert!(latest.has_more);
    assert_eq!(latest.records.len(), 1);
    let item = &latest.records[0].item;
    assert_eq!(item["projection_truncated"], true);
    assert!(item["call"]["input"]["entries"].as_array().unwrap().len() < 100);
    let raw = store.load_item_records(&id).unwrap();
    let AgentItem::ToolCall { call, .. } = &raw.last().unwrap().item else {
        panic!("raw call")
    };
    assert_eq!(call.input["entries"].as_array().unwrap().len(), 100);
    let first: SessionActivityPage = serde_json::from_value(
        access
            .request(SessionRequest::Activity {
                session_id: id.to_string(),
                after_sequence: None,
                tail: false,
                limit: 1,
            })
            .unwrap(),
    )
    .unwrap();
    let next: SessionActivityPage = serde_json::from_value(
        access
            .request(SessionRequest::Activity {
                session_id: id.to_string(),
                after_sequence: first.next_sequence,
                tail: false,
                limit: 1,
            })
            .unwrap(),
    )
    .unwrap();
    assert!(next.records[0].sequence > first.records[0].sequence);
}

#[test]
fn session_view_routes_are_local_and_bounded() {
    let routes = sinomo_runtime::service::Service::route_categories(
        &sinomo_agent::session_panels::SessionModuleService,
    );
    assert_eq!(routes.len(), 2);
    assert!(
        routes
            .iter()
            .all(|route| route.auth_scope.as_deref() == Some("local-command")
                && route.timeout_ms == Some(5000)
                && route.concurrency_limit == Some(4))
    );
}
