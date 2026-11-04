//! The chat host worker acts on events: commands, wakes from its sessions'
//! work, and stops. These tests wait on chat events, never on an interval.
use super::*;
use sinomo_agent::{
    Agent, AgentConfig, AgentLoopInput, AgentOp, AgentRole, AgentSession, AgentSessionRuntime,
    AgentTool, AgentToolDescriptor, AgentToolKind, AgentToolRegistry, AgentToolRequest,
    AgentToolResult, AgentToolSpec, InteractionSession, InteractionSurface, InteractionSurfaceKind,
};

const ANSWER: &str = r#"{"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#;
const ASYNC_TOOL_CALL: &str = r#"{"response_mode":"tool_calls","text":"","tool_calls":[{"tool":"read_file","input":{},"async":true,"summary":"slow work"}],"background_handoff":null,"next_state":"waiting"}"#;

/// Requests one `async` tool call, then answers.
struct AsyncToolModel(Mutex<VecDeque<&'static str>>);

impl sinomo_agent::InteractionModel for AsyncToolModel {
    fn complete(
        &self,
        _: &sinomo_lm::LanguageModelRequest,
    ) -> Result<sinomo_lm::LanguageModelResponse, sinomo_lm::LanguageModelError> {
        let content = self.0.lock().unwrap().pop_front().unwrap_or(ANSWER);
        Ok(sinomo_lm::LanguageModelResponse {
            provider: "fixture".into(),
            model: "fixture".into(),
            content: content.into(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

/// Runs in the background until released.
struct GatedTool(Mutex<Receiver<()>>);

impl AgentTool for GatedTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "slow work",
            false,
        ))
    }

    fn execute(&self, _: AgentToolRequest<'_>) -> AgentToolResult {
        let _ = self.0.lock().unwrap().recv_timeout(Duration::from_secs(30));
        AgentToolResult::succeeded("slow work done", serde_json::json!({ "done": true }))
    }
}

/// Start a detached call of `session_id` on another runtime, as when the host
/// rebuilt the session's runtime while the call ran. Sending on the returned
/// channel lets the call complete.
fn start_detached_call(
    session_id: &SessionId,
) -> (AgentSessionRuntime<AsyncToolModel>, Sender<()>) {
    let (release, gate) = channel();
    let model = AsyncToolModel(Mutex::new(VecDeque::from([ASYNC_TOOL_CALL, ANSWER])));
    let agent = Agent::new(model, "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(5))
        .with_tool_registry(AgentToolRegistry::new().with_tool(GatedTool(Mutex::new(gate))));
    let starter = AgentSessionRuntime::new(
        agent,
        AgentSession::new(session_id.clone(), AgentRole::Main),
    );
    let session = InteractionSession::new(
        session_id.clone(),
        NodeId::generate(),
        InteractionSurface::new("chat-host-wake", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = sinomo_agent::user_interaction_input_event(&session, "Do the slow work.");
    starter
        .submit_op(AgentOp::start_turn(AgentLoopInput {
            session,
            latest_event,
            transient_system_context: None,
            async_update: None,
            context_attachments: Vec::new(),
            tool_specs: Vec::new(),
        }))
        .unwrap();
    starter.join_active_turn().unwrap();
    (starter, release)
}

/// Block on the view's event log until an event after `seen` matches.
fn await_event(
    events: &ChatEventLog,
    seen: &mut u64,
    what: &str,
    matches: impl Fn(&wire::ChatEvent) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = events.poll(&wire::ChatSessionEventsRequest {
            client_id: "view".into(),
            after_sequence: *seen,
            max_events: wire::CHAT_EVENT_PAGE_MAX,
            wait_ms: 1_000,
        });
        for event in response.events {
            *seen = event.sequence;
            if matches(&event.event) {
                return;
            }
        }
        assert!(Instant::now() < deadline, "no {what}");
    }
}

#[test]
fn background_work_wakes_the_host_to_continue_and_to_finish_draining() {
    let root = sinomo_testdir::TestDir::new("chat-host-wake");
    let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
    let _native = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let registry = AgentRuntimeControlRegistry::new();
    let chat_id = SessionId::generate();
    let events = ChatEventLog::new();
    let fanout = ChatFanout::default();
    fanout.subscribe("view", events.clone());
    let (command_tx, command_rx) = channel();
    let wake = HostWake::new(command_tx);
    let alive = Arc::new(AtomicBool::new(true));
    let context = ChatHostContext {
        service: super::tests::service_with_environment(&root, &runtime, registry.clone()),
        lifecycle: Arc::default(),
        completion: HostCompletion::default(),
        model: None,
        chat_id: chat_id.to_string(),
        history: Vec::new(),
        data_dir: data_dir.clone(),
        runtime_control: registry.clone(),
        storage: local::AgentChatSifsStorage::new(
            data_dir.clone(),
            "127.0.0.1:1",
            runtime.handle(),
        ),
        fanout,
        ready: Arc::default(),
        alive: alive.clone(),
        activity: HostActivity::with_wake(wake.clone()),
        wake: wake.clone(),
        host_readiness: Arc::default(),
    };
    let (exited, worker_exit) = channel();
    let worker = std::thread::spawn(move || {
        chat_host_worker(context, command_rx, |opening| {
            let mut chat = local::AgentChat::with_runtime_control_registry(
                opening.data_dir.clone(),
                false,
                opening.runtime_control.clone(),
            )
            .expect("chat runtime");
            chat.resume_session(opening.chat_id, opening.history)
                .expect("open chat");
            // Without credentials the continuation's model turn fails fast.
            chat.set_model(
                sinomo_lm::CODEX_PROVIDER_ID,
                "gpt-5.2-codex",
                sinomo_lm::DEFAULT_PROVIDER_ACCOUNT,
            )
            .expect("switch to codex");
            Some(chat)
        });
        let _ = exited.send(());
    });
    let mut seen = 0;
    await_event(&events, &mut seen, "ready host", |event| {
        matches!(event, wire::ChatEvent::Ready { .. })
    });

    // A detached call of the chat's session completes while the host idles
    // with no deadline before its warm-idle grace: the completion wakes it.
    let (_first, release) = start_detached_call(&chat_id);
    release.send(()).unwrap();
    await_event(&events, &mut seen, "continuation", |event| {
        matches!(event, wire::ChatEvent::TurnStarted { .. })
    });
    await_event(&events, &mut seen, "continuation end", |event| {
        matches!(event, wire::ChatEvent::TurnDone { .. })
    });

    // A stopped host keeps its runtime registered while live work remains,
    // and the end of that work wakes it to exit.
    let (_second, release) = start_detached_call(&chat_id);
    alive.store(false, Ordering::Release);
    wake.wake();
    assert!(
        matches!(registry.has_live_work(&chat_id), Ok(Some(true))),
        "the draining host retains its live runtime"
    );
    assert!(
        worker_exit.try_recv().is_err(),
        "the host waits for its work"
    );
    release.send(()).unwrap();
    worker_exit
        .recv_timeout(Duration::from_secs(30))
        .expect("the finished work woke the draining host");
    worker.join().unwrap();
    assert!(
        matches!(registry.has_live_work(&chat_id), Ok(None)),
        "the exited host released its runtime"
    );
}
