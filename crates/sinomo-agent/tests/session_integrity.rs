#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
use sinomo_agent::*;
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelStreamEvent,
};
use sinomo_runtime::{events::SessionId, local::SinomoDataDir, node::NodeId};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

const OLD_CONSTRAINT: &str = "preserve-earlier-user-constraint-73921";
const TOOL_RECEIPT: &str = "completed-tool-receipt-81423";

fn input(session_id: SessionId) -> AgentLoopInput {
    let session = InteractionSession::new(
        session_id,
        NodeId::generate(),
        InteractionSurface::new("session-integrity", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = user_interaction_input_event(&session, "Continue the work.");
    AgentLoopInput {
        session,
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    }
}

fn response(content: impl Into<String>) -> LanguageModelResponse {
    LanguageModelResponse {
        provider: "fixture".into(),
        model: "fixture".into(),
        content: content.into(),
        items: Vec::new(),
        tool_calls: Vec::new(),
        usage: None,
    }
}

fn answer() -> LanguageModelResponse {
    response(
        r#"{"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#,
    )
}

fn provider_failure() -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: "fixture".into(),
        model: "fixture".into(),
        detail: "injected provider failure after tool execution".into(),
    }
}

#[derive(Clone, Default)]
struct RecordingModel(Arc<Mutex<Vec<LanguageModelRequest>>>);

impl InteractionModel for RecordingModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.0.lock().unwrap().push(request.clone());
        if request.metadata["loop"] == "agent_compaction" {
            let source = serde_json::to_string(request).unwrap();
            Ok(response(if source.contains(OLD_CONSTRAINT) {
                OLD_CONSTRAINT
            } else {
                "No earlier constraint in source."
            }))
        } else {
            Ok(answer())
        }
    }
}

#[test]
fn automatic_compaction_summarizes_prior_turns_and_survives_resume() {
    let root = sinomo_testdir::TestDir::new("agent-compaction-integrity");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    for index in 0..5 {
        session.transcript.push(AgentItem::UserMessage {
            item_id: AgentOp::snapshot().op_id,
            step_index: 0,
            content: format!(
                "{} {}",
                if index == 0 {
                    OLD_CONSTRAINT
                } else {
                    "old exchange"
                },
                "x".repeat(4000)
            ),
            event_id: None,
        });
    }
    let model = RecordingModel::default();
    let agent = Agent::new(model.clone(), "fixture", "fixture")
        .with_config(
            AgentConfig::new("fixture", "fixture")
                .with_agent_definition(AgentDefinition::custom(
                    "integrity-agent",
                    AgentRole::Main,
                    "Help.",
                ))
                .with_context_budget(AgentContextBudget {
                    max_context_bytes: Some(16_384),
                    compaction_threshold_ratio: Some(0.8),
                    retained_tail_items: None,
                }),
        )
        .with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        })
        .with_session_store(store.clone());
    let turn_input = input(session.session_id.clone());
    let run = agent.start_turn(&mut session, &turn_input).unwrap();
    assert!(
        !run.compactions.is_empty(),
        "fixture must trigger automatic compaction"
    );
    let requests = model.0.lock().unwrap();
    let compaction = requests
        .iter()
        .find(|r| r.metadata["loop"] == "agent_compaction")
        .unwrap();
    assert!(
        serde_json::to_string(compaction)
            .unwrap()
            .contains(OLD_CONSTRAINT),
        "history removed from context must be supplied to the summarizer"
    );
    let continuation = requests
        .iter()
        .find(|r| r.metadata["loop"] != "agent_compaction")
        .unwrap();
    assert!(
        serde_json::to_string(continuation)
            .unwrap()
            .contains(OLD_CONSTRAINT)
    );
    drop(requests);

    let mut resumed = store.load_session(&session.session_id).unwrap().unwrap();
    assert_eq!(resumed, session);
    let resume_model = RecordingModel::default();
    let resumed_agent = Agent::new(resume_model.clone(), "fixture", "fixture");
    let resume_input = input(resumed.session_id.clone());
    resumed_agent
        .continue_turn(&mut resumed, &resume_input)
        .unwrap();
    assert!(
        serde_json::to_string(&resume_model.0.lock().unwrap()[0])
            .unwrap()
            .contains(OLD_CONSTRAINT)
    );
}

struct ReceiptTool(Arc<AtomicUsize>);
impl AgentTool for ReceiptTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "record a completed operation",
            false,
        ))
    }
    fn execute(&self, _: AgentToolRequest<'_>) -> AgentToolResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        AgentToolResult::succeeded(
            "operation completed",
            serde_json::json!({"receipt": TOOL_RECEIPT}),
        )
    }
}

struct ToolThenFailure {
    calls: AtomicUsize,
    blocked: Option<(mpsc::Sender<()>, Mutex<mpsc::Receiver<()>>)>,
}
impl InteractionModel for ToolThenFailure {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(response(
                r#"{"response_mode":"tool_calls","text":"","tool_calls":[{"tool":"read_file","input":{},"async":false,"summary":"read"}],"background_handoff":null,"next_state":"waiting"}"#,
            ))
        } else {
            if let Some((started, release)) = &self.blocked {
                started.send(()).unwrap();
                release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
            Err(provider_failure())
        }
    }
}

fn failed_turn_agent(
    store: FileAgentSessionStore,
    effects: Arc<AtomicUsize>,
    model: ToolThenFailure,
) -> Agent<ToolThenFailure> {
    Agent::new(model, "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(3))
        .with_tool_registry(AgentToolRegistry::new().with_tool(ReceiptTool(effects)))
        .with_session_store(store)
}

fn assert_resumable_receipt(session: &AgentSession, store: &FileAgentSessionStore) {
    assert!(
        session.transcript.items.iter().any(|item| matches!(item,
        AgentItem::ToolResult { output, .. } if output["receipt"] == TOOL_RECEIPT)),
        "the failed turn must retain its successful tool result"
    );
    assert_eq!(
        store.load_session(&session.session_id).unwrap().as_ref(),
        Some(session)
    );
    assert!(
        session
            .transcript
            .items
            .iter()
            .any(|item| matches!(item, AgentItem::Error { detail, .. }
        if detail.contains("injected provider failure")))
    );
    let model = RecordingModel::default();
    let agent = Agent::new(model.clone(), "fixture", "fixture");
    let mut resumed = session.clone();
    agent
        .continue_turn(&mut resumed, &input(session.session_id.clone()))
        .unwrap();
    assert!(
        serde_json::to_string(&model.0.lock().unwrap()[0])
            .unwrap()
            .contains(TOOL_RECEIPT),
        "resumed model must see the completed operation before deciding what to do next"
    );
}

#[test]
fn synchronous_provider_failure_preserves_completed_tool_results() {
    let root = sinomo_testdir::TestDir::new("agent-sync-failure-integrity");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let effects = Arc::new(AtomicUsize::new(0));
    let agent = failed_turn_agent(
        store.clone(),
        effects.clone(),
        ToolThenFailure {
            calls: AtomicUsize::new(0),
            blocked: None,
        },
    );
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    assert!(agent.start_turn(&mut session, &turn_input).is_err());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_resumable_receipt(&session, &store);
}

#[test]
fn worker_provider_failure_reaches_join_and_preserves_live_inputs() {
    let root = sinomo_testdir::TestDir::new("agent-worker-failure-integrity");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let effects = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let agent = failed_turn_agent(
        store.clone(),
        effects.clone(),
        ToolThenFailure {
            calls: AtomicUsize::new(0),
            blocked: Some((started_tx, Mutex::new(release_rx))),
        },
    );
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    session
        .steering_queue
        .push(AgentSteeringMessage::new("captured by this turn"));
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent, session);
    let cursor = runtime.runtime_event_cursor().unwrap();
    let op = AgentOp::start_turn(turn_input);
    let op_id = op.op_id.clone();
    runtime.submit_op(op).unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    runtime
        .submit_op(AgentOp::steer("new live steering"))
        .unwrap();
    runtime
        .submit_op(AgentOp::follow_up("new live follow-up"))
        .unwrap();
    release_tx.send(()).unwrap();
    let joined = runtime.join_active_turn();
    assert!(
        matches!(joined, Err(AgentError::Model(_))),
        "provider failure must reach join: {joined:?}"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(runtime.active_turn().unwrap().is_none());
    assert!(runtime.turn_result(&op_id).unwrap().is_none());
    let events = runtime.runtime_events_since(cursor).unwrap().events;
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, AgentRuntimeEventPayload::WorkerFailed { .. }))
    );
    assert!(!events.iter().any(|e| matches!(
        e.payload,
        AgentRuntimeEventPayload::WorkerCompleted { .. }
            | AgentRuntimeEventPayload::TurnFinalDecision { .. }
    )));
    let session = runtime.session_snapshot().unwrap();
    assert_eq!(session.steering_queue.len(), 1);
    assert_eq!(session.steering_queue[0].message, "new live steering");
    assert_eq!(session.follow_up_queue[0].message, "new live follow-up");
    assert_resumable_receipt(&session, &store);
}

#[derive(Clone, Copy, Debug)]
enum FailurePoint {
    Record,
    Snapshot,
    ControlHistory,
}
struct FailingStore(FailurePoint);
impl AgentSessionStore for FailingStore {
    fn load_session(&self, _: &SessionId) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        Ok(None)
    }
    fn save_session(&self, _: &AgentSession) -> Result<(), AgentSessionStoreError> {
        if matches!(self.0, FailurePoint::Snapshot) {
            Err(store_failure())
        } else {
            Ok(())
        }
    }
    fn append_items(&self, _: &SessionId, _: &[AgentItem]) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }
    fn append_projection(
        &self,
        _: &SessionId,
        _: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }
    fn append_record(
        &self,
        record: &AgentSessionStoreRecord,
    ) -> Result<(), AgentSessionStoreError> {
        if matches!(self.0, FailurePoint::Record)
            || (matches!(self.0, FailurePoint::ControlHistory)
                && matches!(
                    record.record,
                    AgentSessionStoreRecordKind::ControlEventPublished { .. }
                        | AgentSessionStoreRecordKind::ControlSnapshotSaved { .. }
                ))
        {
            Err(store_failure())
        } else {
            Ok(())
        }
    }
}
fn store_failure() -> AgentSessionStoreError {
    AgentSessionStoreError::Io {
        path: "injected".into(),
        detail: "injected control persistence failure".into(),
    }
}

fn queued_session() -> (AgentSession, Vec<AgentOp>) {
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let steering = AgentSteeringMessage::new("existing steering");
    let follow_up = AgentFollowUpMessage::new("existing follow-up");
    let ops = vec![
        AgentOp::steer("rejected steering"),
        AgentOp::follow_up("rejected follow-up"),
        AgentOp::remove_queued_input(steering.steering_id.clone()),
        AgentOp::remove_queued_input(follow_up.follow_up_id.clone()),
        AgentOp::abort("rejected abort"),
        AgentOp::compact("rejected compaction"),
    ];
    session.steering_queue.push(steering);
    session.follow_up_queue.push(follow_up);
    (session, ops)
}

#[test]
fn rejected_control_operations_leave_runtime_state_unchanged() {
    for point in [
        FailurePoint::Record,
        FailurePoint::Snapshot,
        FailurePoint::ControlHistory,
    ] {
        let (original, ops) = queued_session();
        for op in ops {
            let agent = Agent::new(RecordingModel::default(), "fixture", "fixture")
                .with_session_store(FailingStore(point));
            let runtime = AgentSessionRuntime::new(agent, original.clone());
            let before = runtime.control_snapshot().unwrap();
            let cursor = runtime.runtime_event_cursor().unwrap();
            assert!(
                runtime.submit_op(op.clone()).is_err(),
                "must fail at {point:?}: {op:?}"
            );
            assert_eq!(
                runtime.session_snapshot().unwrap(),
                original,
                "failed {point:?}: {op:?}"
            );
            assert_eq!(runtime.control_snapshot().unwrap(), before);
            assert!(
                runtime
                    .runtime_events_since(cursor)
                    .unwrap()
                    .events
                    .is_empty()
            );
        }
    }
}

#[test]
fn rejected_control_operations_leave_synchronous_session_unchanged() {
    for point in [FailurePoint::Record, FailurePoint::Snapshot] {
        let (original, ops) = queued_session();
        for op in ops {
            let agent = Agent::new(RecordingModel::default(), "fixture", "fixture")
                .with_session_store(FailingStore(point));
            let mut session = original.clone();
            assert!(agent.handle_op(&mut session, op.clone()).is_err());
            assert_eq!(session, original, "failed {point:?}: {op:?}");

            let mut control = AgentSessionControlPlane::new(original.clone());
            let before = control.snapshot();
            assert!(control.submit_op(&agent, op.clone()).is_err());
            assert_eq!(control.session(), &original);
            assert_eq!(control.snapshot(), before);
        }
    }
}

struct GatedModel {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    saw_cancellation: Arc<AtomicBool>,
}

impl InteractionModel for GatedModel {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        Ok(answer())
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        _: &LanguageModelRequest,
        cancellation: &AgentCancellationToken,
        _: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        self.saw_cancellation
            .store(cancellation.is_cancelled(), Ordering::SeqCst);
        Ok(answer())
    }
}

struct SwitchableControlStore(Arc<AtomicBool>);
impl AgentSessionStore for SwitchableControlStore {
    fn load_session(&self, _: &SessionId) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        Ok(None)
    }
    fn save_session(&self, _: &AgentSession) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }
    fn append_items(&self, _: &SessionId, _: &[AgentItem]) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }
    fn append_projection(
        &self,
        _: &SessionId,
        _: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }
    fn append_record(&self, _: &AgentSessionStoreRecord) -> Result<(), AgentSessionStoreError> {
        if self.0.load(Ordering::SeqCst) {
            Err(store_failure())
        } else {
            Ok(())
        }
    }
}

#[test]
fn rejected_interrupt_does_not_signal_the_live_worker() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let reject = Arc::new(AtomicBool::new(false));
    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let model = GatedModel {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        saw_cancellation: saw_cancellation.clone(),
    };
    let agent = Agent::new(model, "fixture", "fixture")
        .with_session_store(SwitchableControlStore(reject.clone()));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent, session);
    runtime.submit_op(AgentOp::start_turn(turn_input)).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let before = runtime.control_snapshot().unwrap();
    reject.store(true, Ordering::SeqCst);
    assert!(
        runtime
            .interrupt_active_turn("rejected cancellation")
            .is_err()
    );
    assert_eq!(runtime.control_snapshot().unwrap(), before);
    reject.store(false, Ordering::SeqCst);
    release_tx.send(()).unwrap();
    runtime.join_active_turn().unwrap();
    assert!(!saw_cancellation.load(Ordering::SeqCst));
}
