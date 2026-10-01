use sinomo_agent::*;
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelStreamEvent,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::node::NodeId;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Behavior {
    Block,
    Probe,
    Fail,
    Panic,
    Long,
    Nested,
}

struct Model {
    spawn_from_model: bool,
    behavior: Behavior,
    started: mpsc::Sender<SessionId>,
    finished: mpsc::Sender<bool>,
    release: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
    leaf_started: Arc<AtomicBool>,
}

fn response(
    request: &LanguageModelRequest,
    text: String,
    calls: Vec<AgentToolCall>,
) -> LanguageModelResponse {
    LanguageModelResponse {
        provider: request.provider.clone(),
        model: request.model.clone(),
        content: serde_json::to_string(&AgentLoopDecision {
            response_mode: AgentLoopResponseMode::Answer,
            text,
            tool_calls: calls,
            background_handoff: None,
            next_state: AgentInteractionState::Listening,
        })
        .unwrap(),
        items: vec![],
        tool_calls: vec![],
        usage: None,
    }
}

fn call(tool: &str, input: serde_json::Value) -> AgentToolCall {
    AgentToolCall {
        call_id: None,
        tool: tool.into(),
        input,
        r#async: false,
        summary: "test operation".into(),
    }
}

impl InteractionModel for Model {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming_events_with_cancellation(
            request,
            &AgentCancellationToken::new(),
            &mut |_| {},
        )
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        token: &AgentCancellationToken,
        _: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests.lock().unwrap().push(request.clone());
        let step = request.metadata["step_index"].as_u64().unwrap();
        if request
            .metadata
            .pointer("/run_context/task/kind")
            .and_then(serde_json::Value::as_str)
            != Some("subagent")
        {
            if request
                .messages
                .iter()
                .any(|message| message.content.contains("hold-parent"))
            {
                self.started
                    .send(serde_json::from_value(request.metadata["session_id"].clone()).unwrap())
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(5);
                while !token.is_cancelled() {
                    assert!(Instant::now() < deadline, "test did not cancel parent");
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
            let calls = if self.spawn_from_model && step == 0 {
                vec![
                    call(
                        "subagents",
                        serde_json::json!({"action":"spawn", "name":"one", "task":"first task"}),
                    ),
                    call(
                        "subagents",
                        serde_json::json!({"action":"spawn", "name":"two", "task":"second task"}),
                    ),
                ]
            } else {
                vec![]
            };
            return Ok(response(request, "parent answer".into(), calls));
        }
        let nested_branch = matches!(self.behavior, Behavior::Nested)
            && request.metadata["run_context"]["task"]["summary"] == "specific task";
        if step == 0 {
            self.started
                .send(serde_json::from_value(request.metadata["session_id"].clone()).unwrap())
                .unwrap();
            if nested_branch {
                return Ok(response(
                    request,
                    "delegating".into(),
                    vec![call(
                        "subagents",
                        serde_json::json!({"action":"spawn", "task":"leaf task"}),
                    )],
                ));
            }
            if matches!(self.behavior, Behavior::Nested) {
                self.leaf_started.store(true, Ordering::SeqCst);
            }
            if matches!(
                self.behavior,
                Behavior::Block | Behavior::Probe | Behavior::Nested
            ) {
                let deadline = Instant::now() + Duration::from_secs(5);
                while !self.release.load(Ordering::SeqCst) && !token.is_cancelled() {
                    assert!(Instant::now() < deadline, "test did not release child");
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
            self.finished.send(token.is_cancelled()).unwrap();
            match self.behavior {
                Behavior::Fail => {
                    return Err(LanguageModelError::ProviderInvocationFailed {
                        provider: request.provider.clone(),
                        model: request.model.clone(),
                        detail: "child provider failure".into(),
                    });
                }
                Behavior::Panic => panic!("child provider panic"),
                Behavior::Probe => {
                    return Ok(response(
                        request,
                        "checking inherited permission".into(),
                        vec![call("probe", serde_json::json!({}))],
                    ));
                }
                Behavior::Long => return Ok(response(request, "界".repeat(30_000), vec![])),
                Behavior::Block | Behavior::Nested => {}
            }
        }
        if nested_branch {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !self.leaf_started.load(Ordering::SeqCst) {
                assert!(Instant::now() < deadline, "grandchild did not start");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        let text = if matches!(self.behavior, Behavior::Probe) {
            request.messages.last().unwrap().content.clone()
        } else {
            "child answer".into()
        };
        Ok(response(request, text, vec![]))
    }
}

struct Harness {
    runtime: AgentSessionRuntime<Model>,
    registry: AgentToolRegistry,
    input: AgentLoopInput,
    started: mpsc::Receiver<SessionId>,
    finished: mpsc::Receiver<bool>,
    release: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
}

fn harness(
    behavior: Behavior,
    spawn_from_model: bool,
    registry: AgentToolRegistry,
    surface: AgentToolSurface,
) -> Harness {
    let interaction = InteractionSession::new(
        SessionId::generate(),
        NodeId::generate(),
        InteractionSurface::new("subagents-test", InteractionSurfaceKind::CommandLine),
    );
    let input = AgentLoopInput {
        latest_event: user_interaction_input_event(&interaction, "parent task"),
        session: interaction,
        transient_system_context: None,
        async_update: None,
        context_attachments: vec![],
        tool_specs: vec![],
    };
    let (started_tx, started) = mpsc::channel();
    let (finished_tx, finished) = mpsc::channel();
    let release = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(Mutex::new(vec![]));
    let model = Model {
        behavior,
        spawn_from_model,
        started: started_tx,
        finished: finished_tx,
        release: release.clone(),
        requests: requests.clone(),
        leaf_started: Arc::new(AtomicBool::new(false)),
    };
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Main)
        .with_agent_definition_key("main-agent");
    session.workspace_binding = Some(WorkspaceRef::local_path("/workspace"));
    session.execution_placement = Some(AgentSessionExecutionPlacement {
        active_server_id: "server".into(),
        generation: 2,
    });
    session.sandbox_attachment = Some(AgentSessionSandboxAttachment {
        sandbox_id: "sandbox".into(),
        server_id: "server".into(),
        attachment_kind: AgentSessionSandboxAttachmentKind::Explicit,
    });
    session.resource_bindings.push(AgentSessionResourceBinding {
        resource_id: "home".into(),
        kind: AgentSessionResourceKind::AgentHome,
        location: AgentSessionResourceLocation::Sifs {
            path: "/sinomo/agents/main-agent/home".into(),
        },
        availability: AgentSessionResourceAvailability::Available,
    });
    let config = AgentConfig::new("test", "test")
        .with_temperature(Some(0.2))
        .with_max_output_tokens(Some(1234))
        .with_max_steps(3);
    let runtime = AgentSessionRuntime::new(
        Agent::new(model, "test", "test")
            .with_config(config)
            .with_tool_surface(surface)
            .with_tool_registry(registry.clone()),
        session,
    );
    runtime
        .submit_op(AgentOp::start_turn(input.clone()))
        .unwrap();
    runtime.join_active_turn().unwrap();
    Harness {
        runtime,
        registry,
        input,
        started,
        finished,
        release,
        requests,
    }
}

fn setup(behavior: Behavior) -> Harness {
    harness(
        behavior,
        false,
        AgentToolRegistry::new(),
        AgentToolSurface::default_for_role(AgentRole::Main),
    )
}

fn spawn(h: &Harness) -> AgentSubagentSnapshot {
    h.runtime
        .subagents()
        .spawn(
            &h.input.session.session_id,
            "worker",
            "specific task",
            &AgentCancellationToken::new(),
        )
        .unwrap()
}

fn wait(h: &Harness, id: &TaskId) -> AgentSubagentSnapshot {
    let result = h
        .runtime
        .subagents()
        .wait(
            &h.input.session.session_id,
            id,
            3000,
            &AgentCancellationToken::new(),
        )
        .unwrap();
    assert_ne!(
        result.status,
        AgentSubagentStatus::Running,
        "child did not finish"
    );
    result
}

#[test]
fn model_spawns_parallel_children_through_the_registered_tool() {
    let h = harness(
        Behavior::Block,
        true,
        AgentToolRegistry::new(),
        AgentToolSurface::default_for_role(AgentRole::Main),
    );
    let first = h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let second = h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_ne!(first, second);
    let manager = h.runtime.subagents();
    let children = manager.list(&h.input.session.session_id);
    assert_eq!(children.len(), 2);
    let parent = h.runtime.session_snapshot().unwrap();
    for child in &children {
        let session = manager
            .session_snapshot(&parent.session_id, &child.id)
            .unwrap();
        assert_eq!(session.workspace_binding, parent.workspace_binding);
        assert_eq!(session.sandbox_attachment, parent.sandbox_attachment);
        assert_eq!(session.execution_placement, parent.execution_placement);
        assert_eq!(session.resource_bindings, parent.resource_bindings);
        assert_eq!(session.agent_definition_key, parent.agent_definition_key);
        assert!(session.transcript.items.is_empty());
    }
    h.release.store(true, Ordering::SeqCst);
    for child in &children {
        let result = wait(&h, &child.id);
        assert_eq!(result.status, AgentSubagentStatus::Completed);
        assert_eq!(result.result.as_deref(), Some("child answer"));
    }
    let requests = h.requests.lock().unwrap();
    let child_request = requests
        .iter()
        .find(|r| {
            r.metadata
                .pointer("/run_context/task/kind")
                .and_then(serde_json::Value::as_str)
                == Some("subagent")
        })
        .unwrap();
    assert_eq!(child_request.temperature, Some(0.2));
    assert_eq!(child_request.max_output_tokens, Some(1234));
    assert!(
        child_request
            .tools
            .iter()
            .any(|tool| tool.name == "subagents")
    );
    assert!(!parent.transcript.items.iter().any(
        |item| matches!(item, AgentItem::AssistantText { content, .. } if content == "child answer")
    ));
}

struct Probe(Arc<AtomicUsize>);
impl AgentTool for Probe {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::McpCall,
            "probe",
            "permission probe",
            false,
        ))
        .requiring_local_permission("external-integrations")
    }
    fn execute(&self, _: AgentToolRequest<'_>) -> AgentToolResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        AgentToolResult::succeeded("probe executed", serde_json::json!({"inherited": true}))
    }
}

#[test]
fn children_inherit_tool_authority_and_observe_revocation_at_execution() {
    for allowed in [true, false] {
        let permission = Arc::new(AtomicBool::new(true));
        let checker = permission.clone();
        let effects = Arc::new(AtomicUsize::new(0));
        let registry = AgentToolRegistry::new()
            .with_tool(Probe(effects.clone()))
            .with_local_permission_checker(move |_| Ok(checker.load(Ordering::SeqCst)));
        let h = harness(
            Behavior::Probe,
            false,
            registry,
            AgentToolSurface::default_for_role(AgentRole::Main),
        );
        let child = spawn(&h);
        h.started.recv_timeout(Duration::from_secs(2)).unwrap();
        permission.store(allowed, Ordering::SeqCst);
        h.release.store(true, Ordering::SeqCst);
        let result = wait(&h, &child.id);
        assert_eq!(effects.load(Ordering::SeqCst), usize::from(allowed));
        assert!(
            result
                .result
                .unwrap()
                .contains(if allowed { "succeeded" } else { "blocked" })
        );
    }
}

#[test]
fn children_keep_parent_tool_surface_denials() {
    let effects = Arc::new(AtomicUsize::new(0));
    let registry = AgentToolRegistry::new()
        .with_tool(Probe(effects.clone()))
        .with_local_permission_checker(|_| Ok(true));
    let h = harness(
        Behavior::Probe,
        false,
        registry,
        AgentToolSurface::default_for_role(AgentRole::Main).with_denied_tools(["probe"]),
    );
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.release.store(true, Ordering::SeqCst);
    assert!(wait(&h, &child.id).result.unwrap().contains("blocked"));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}

#[test]
fn child_resources_survive_parent_registry_reconfiguration() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let registry = AgentToolRegistry::new();
    registry.retain_resource(resource);
    let h = harness(
        Behavior::Block,
        false,
        registry,
        AgentToolSurface::default_for_role(AgentRole::Main),
    );
    let child = spawn(&h);
    h.registry.clear();
    assert!(weak.upgrade().is_some());
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.release.store(true, Ordering::SeqCst);
    wait(&h, &child.id);
    assert!(weak.upgrade().is_none());
}

#[test]
fn child_handles_are_owned_and_disposable() {
    let h = setup(Behavior::Block);
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let manager = h.runtime.subagents();
    let foreign = SessionId::generate();
    assert!(manager.inspect(&foreign, &child.id).is_err());
    assert!(manager.cancel(&foreign, &child.id).is_err());
    assert!(manager.dispose(&foreign, &child.id).is_err());
    let quick = manager
        .wait(
            &h.input.session.session_id,
            &child.id,
            0,
            &AgentCancellationToken::new(),
        )
        .unwrap();
    assert_eq!(quick.status, AgentSubagentStatus::Running);
    manager
        .dispose(&h.input.session.session_id, &child.id)
        .unwrap();
    assert!(h.finished.recv_timeout(Duration::from_secs(2)).unwrap());
    assert!(manager.list(&h.input.session.session_id).is_empty());
    assert!(
        manager
            .inspect(&h.input.session.session_id, &child.id)
            .is_err()
    );
}

#[test]
fn cancellation_is_one_way_and_does_not_cancel_siblings() {
    let h = setup(Behavior::Block);
    let parent_token = AgentCancellationToken::new();
    let manager = h.runtime.subagents();
    let first = manager
        .spawn(&h.input.session.session_id, "one", "task", &parent_token)
        .unwrap();
    let second = manager
        .spawn(&h.input.session.session_id, "two", "task", &parent_token)
        .unwrap();
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    manager
        .cancel(&h.input.session.session_id, &first.id)
        .unwrap();
    assert_eq!(wait(&h, &first.id).status, AgentSubagentStatus::Cancelled);
    assert!(!parent_token.is_cancelled());
    assert_eq!(
        manager
            .inspect(&h.input.session.session_id, &second.id)
            .unwrap()
            .status,
        AgentSubagentStatus::Running
    );
    parent_token.cancel();
    assert_eq!(wait(&h, &second.id).status, AgentSubagentStatus::Cancelled);
}

#[test]
fn last_parent_runtime_release_cancels_and_reclaims_children() {
    let h = setup(Behavior::Block);
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let manager = h.runtime.subagents();
    let clone = h.runtime.clone();
    drop(h.runtime);
    assert_eq!(
        manager
            .inspect(&h.input.session.session_id, &child.id)
            .unwrap()
            .status,
        AgentSubagentStatus::Running
    );
    drop(clone);
    assert!(h.finished.recv_timeout(Duration::from_secs(2)).unwrap());
    assert!(manager.list(&h.input.session.session_id).is_empty());
    assert!(
        manager
            .spawn(
                &h.input.session.session_id,
                "late",
                "task",
                &AgentCancellationToken::new()
            )
            .is_err()
    );
}

#[test]
fn child_failure_and_panic_are_reported_without_failing_parent() {
    for behavior in [Behavior::Fail, Behavior::Panic] {
        let h = setup(behavior);
        let child = spawn(&h);
        let result = wait(&h, &child.id);
        assert_eq!(result.status, AgentSubagentStatus::Failed);
        assert!(result.error.is_some());
        assert!(h.runtime.active_turn().unwrap().is_none());
    }
}

#[test]
fn tool_rejects_permission_overrides_and_bounds_utf8_results() {
    let h = setup(Behavior::Long);
    let rejected = h.registry.execute(
        &h.input.session,
        0,
        0,
        &call(
            "subagents",
            serde_json::json!({"action":"spawn", "task":"task", "permissions":["all"]}),
        ),
    );
    assert_eq!(rejected.status, AgentToolStatus::Failed);
    assert!(
        h.runtime
            .subagents()
            .list(&h.input.session.session_id)
            .is_empty()
    );
    let child = spawn(&h);
    let result = wait(&h, &child.id);
    assert!(result.result_truncated);
    assert!(result.result.unwrap().len() <= 64 * 1024);
}

#[test]
fn aborting_parent_cancels_children_between_turns() {
    let h = setup(Behavior::Block);
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let result = h
        .runtime
        .submit_op(AgentOp::abort("stop the task"))
        .unwrap();
    assert!(result.accepted);
    assert_eq!(wait(&h, &child.id).status, AgentSubagentStatus::Cancelled);
    assert!(
        h.runtime
            .subagents()
            .spawn(
                &h.input.session.session_id,
                "late",
                "task",
                &AgentCancellationToken::new()
            )
            .is_err()
    );
}

#[test]
fn children_can_spawn_descendants_and_reclaim_them_on_completion() {
    let h = setup(Behavior::Nested);
    let branch = spawn(&h);
    let first = h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let leaf = h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(first, branch.session_id);
    assert_ne!(leaf, branch.session_id);
    assert_eq!(wait(&h, &branch.id).status, AgentSubagentStatus::Completed);
    assert!(h.finished.recv_timeout(Duration::from_secs(2)).unwrap());
    let requests = h.requests.lock().unwrap();
    let leaf_request = requests
        .iter()
        .find(|r| r.metadata["session_id"] == serde_json::json!(leaf))
        .unwrap();
    assert_eq!(
        leaf_request.metadata["run_context"]["task"]["parent_task_id"],
        serde_json::json!(branch.id)
    );
}

#[test]
fn parent_turn_deadline_reaches_children_after_parent_answer() {
    let h = harness(
        Behavior::Block,
        true,
        AgentToolRegistry::new(),
        AgentToolSurface::default_for_role(AgentRole::Main),
    );
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let manager = h.runtime.subagents();
    let original = manager.list(&h.input.session.session_id);
    h.runtime
        .submit_op(AgentOp::start_turn(h.input.clone()).with_timeout_ms(100))
        .unwrap();
    h.runtime.join_active_turn().unwrap();
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let current = manager.list(&h.input.session.session_id);
    assert_eq!(current.len(), 4);
    for child in current
        .iter()
        .filter(|child| !original.iter().any(|old| old.id == child.id))
    {
        assert_eq!(wait(&h, &child.id).status, AgentSubagentStatus::Cancelled);
    }
    for child in original {
        assert_eq!(
            manager
                .inspect(&h.input.session.session_id, &child.id)
                .unwrap()
                .status,
            AgentSubagentStatus::Running
        );
    }
}

#[test]
fn replacing_parent_session_releases_old_children_and_rebinds_tool() {
    let mut h = setup(Behavior::Block);
    let old = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut replacement = h.runtime.session_snapshot().unwrap();
    replacement.session_id = SessionId::generate();
    h.runtime.replace_session(replacement.clone()).unwrap();
    assert!(h.finished.recv_timeout(Duration::from_secs(2)).unwrap());
    assert!(
        h.runtime
            .subagents()
            .inspect(&old.parent_session_id, &old.id)
            .is_err()
    );
    h.input.session.session_id = replacement.session_id;
    h.input.latest_event = user_interaction_input_event(&h.input.session, "new parent task");
    h.runtime
        .submit_op(AgentOp::start_turn(h.input.clone()))
        .unwrap();
    h.runtime.join_active_turn().unwrap();
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.release.store(true, Ordering::SeqCst);
    assert_eq!(wait(&h, &child.id).status, AgentSubagentStatus::Completed);
}

#[test]
fn registered_tool_supports_inspect_wait_cancel_and_dispose() {
    let h = setup(Behavior::Block);
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    for action in ["list", "inspect", "wait", "cancel", "dispose"] {
        let input = if action == "list" {
            serde_json::json!({"action": action})
        } else if action == "wait" {
            serde_json::json!({"action":action, "id":child.id, "timeout_ms":0})
        } else {
            serde_json::json!({"action":action, "id":child.id})
        };
        let result = h
            .registry
            .execute(&h.input.session, 0, 0, &call("subagents", input));
        assert_eq!(
            result.status,
            AgentToolStatus::Succeeded,
            "{action}: {}",
            result.summary
        );
        if action == "cancel" {
            assert_eq!(result.output["subagent"]["cancel_requested"], true);
        }
    }
    assert!(h.finished.recv_timeout(Duration::from_secs(2)).unwrap());
    assert!(
        h.runtime
            .subagents()
            .list(&h.input.session.session_id)
            .is_empty()
    );
}

#[test]
fn interrupting_parent_cancels_children_from_previous_turns() {
    let mut h = setup(Behavior::Block);
    let child = spawn(&h);
    h.started.recv_timeout(Duration::from_secs(2)).unwrap();
    h.input.latest_event = user_interaction_input_event(&h.input.session, "hold-parent");
    h.runtime
        .submit_op(AgentOp::start_turn(h.input.clone()))
        .unwrap();
    assert_eq!(
        h.started.recv_timeout(Duration::from_secs(2)).unwrap(),
        h.input.session.session_id
    );
    assert!(
        h.runtime
            .interrupt_active_turn("cancel task")
            .unwrap()
            .accepted
    );
    assert_eq!(wait(&h, &child.id).status, AgentSubagentStatus::Cancelled);
    h.runtime.join_active_turn().unwrap();
}
