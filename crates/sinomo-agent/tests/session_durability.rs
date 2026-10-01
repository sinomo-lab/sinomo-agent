#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
//! Durability and correctness of persisted agent sessions: crash-safe logs,
//! recovery of unsaved turn items, control-edit commit order, cancellation
//! finality, and compaction tails that keep tool calls paired.

use sinomo_agent::*;
use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
use sinomo_runtime::{events::SessionId, local::SinomoDataDir, node::NodeId};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

fn input(session_id: SessionId) -> AgentLoopInput {
    let session = InteractionSession::new(
        session_id,
        NodeId::generate(),
        InteractionSurface::new("session-durability", InteractionSurfaceKind::CommandLine),
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

fn response(content: &str) -> LanguageModelResponse {
    LanguageModelResponse {
        provider: "fixture".into(),
        model: "fixture".into(),
        content: content.into(),
        items: Vec::new(),
        tool_calls: Vec::new(),
        usage: None,
    }
}

const ANSWER: &str = r#"{"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#;
const READ_TOOL_CALL: &str = r#"{"response_mode":"tool_calls","text":"","tool_calls":[{"tool":"read_file","input":{},"async":false,"summary":"read"}],"background_handoff":null,"next_state":"waiting"}"#;

fn user_message(content: &str) -> AgentItem {
    AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: content.to_string(),
        event_id: None,
    }
}

fn tool_call(step_index: usize, call_index: usize) -> AgentItem {
    AgentItem::ToolCall {
        item_id: TaskId::generate(),
        step_index,
        call_index,
        call: AgentToolCall {
            call_id: Some(format!("native-{step_index}-{call_index}")),
            tool: "read_file".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "read".to_string(),
        },
    }
}

fn tool_result(step_index: usize, call_index: usize) -> AgentItem {
    AgentItem::ToolResult {
        item_id: TaskId::generate(),
        step_index,
        call_index,
        call_id: TaskId::generate(),
        native_call_id: Some(format!("native-{step_index}-{call_index}")),
        tool_name: Some("read_file".to_string()),
        status: AgentToolStatus::Succeeded,
        summary: "read".to_string(),
        output: serde_json::json!({}),
        evidence: AgentToolEvidence::availability_only(true, "fixture"),
    }
}

fn is_pair(item: &AgentItem, kind: &str, step: usize, call: usize) -> bool {
    match (kind, item) {
        (
            "call",
            AgentItem::ToolCall {
                step_index,
                call_index,
                ..
            },
        )
        | (
            "result",
            AgentItem::ToolResult {
                step_index,
                call_index,
                ..
            },
        ) => *step_index == step && *call_index == call,
        _ => false,
    }
}

#[test]
fn torn_log_tail_is_ignored_and_the_next_append_starts_a_new_line() {
    let root = sinomo_testdir::TestDir::new("agent-durability-torn-tail");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let session_id = SessionId::generate();
    let steering = |message: &str| {
        AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::SteeringQueued {
                steering: AgentSteeringMessage::new(message),
            },
        )
    };
    store.append_record(&steering("first")).unwrap();
    let events_path = store.paths().session_events_path_for_session(&session_id);
    {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&events_path)
            .unwrap();
        // A crash in the middle of an append leaves a fragment, no newline.
        file.write_all(br#"{"record_id":"01J"#).unwrap();
    }

    // A fresh store (a restarted process) must read past the fragment and
    // keep its next record on a line of its own.
    let restarted = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    restarted.load_control_events(&session_id).unwrap();
    restarted.append_record(&steering("after restart")).unwrap();
    let contents = std::fs::read_to_string(&events_path).unwrap();
    let last = contents.lines().last().unwrap();
    let record: AgentSessionStoreRecord = serde_json::from_str(last).unwrap();
    assert!(matches!(
        record.record,
        AgentSessionStoreRecordKind::SteeringQueued { steering } if steering.message == "after restart"
    ));
    restarted.load_control_snapshot(&session_id).unwrap();
}

#[test]
fn snapshot_replacement_leaves_no_partial_or_temporary_files() {
    let root = sinomo_testdir::TestDir::new("agent-durability-atomic-snapshot");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    store.save_session(&session).unwrap();
    session.transcript.push(user_message("second save"));
    store.save_session(&session).unwrap();

    assert_eq!(
        store.load_session(&session.session_id).unwrap().as_ref(),
        Some(&session)
    );
    let snapshot_dir = store
        .paths()
        .session_snapshot_path_for_session(&session.session_id)
        .parent()
        .unwrap()
        .to_path_buf();
    let leftovers = std::fs::read_dir(snapshot_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".sinomo-vfs-"))
        .collect::<Vec<_>>();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn unsaved_turn_items_are_recovered_with_interrupted_calls_closed() {
    let root = sinomo_testdir::TestDir::new("agent-durability-recover");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    session
        .transcript
        .push(user_message("saved before the turn"));
    store.save_session(&session).unwrap();

    // The interrupted turn journaled a finished tool pair and a call whose
    // result never arrived, then the process stopped before saving.
    let journaled = vec![
        user_message("the interrupted turn"),
        tool_call(1, 0),
        tool_result(1, 0),
        tool_call(2, 0),
    ];
    store.append_items(&session.session_id, &journaled).unwrap();

    let recovered = store.load_session(&session.session_id).unwrap().unwrap();
    let items = &recovered.transcript.items;
    assert_eq!(&items[1..5], &journaled[..]);
    assert!(
        items.iter().any(|item| matches!(
            item,
            AgentItem::ToolResult {
                step_index: 2,
                call_index: 0,
                status: AgentToolStatus::Failed,
                ..
            }
        )),
        "the interrupted call must be closed with a failed result: {items:?}"
    );
    assert!(
        AgentToolPairIntegrity::from_items(items).is_balanced(),
        "recovered transcript must keep tool calls paired"
    );
    assert!(matches!(items.last(), Some(AgentItem::Error { .. })));

    // Once saved, the recovered items are known and nothing is added again.
    store.save_session(&recovered).unwrap();
    let reloaded = store.load_session(&session.session_id).unwrap().unwrap();
    assert_eq!(reloaded, recovered);
}

#[test]
fn completed_turns_recover_nothing() {
    let root = sinomo_testdir::TestDir::new("agent-durability-no-recovery");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let agent = Agent::new(Sequenced::new([ANSWER]), "fixture", "fixture")
        .with_session_store(store.clone());
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    agent.start_turn(&mut session, &turn_input).unwrap();
    assert_eq!(
        store.load_session(&session.session_id).unwrap().as_ref(),
        Some(&session)
    );
}

struct Sequenced(Mutex<Vec<&'static str>>);

impl Sequenced {
    fn new<const N: usize>(responses: [&'static str; N]) -> Self {
        let mut responses = responses.to_vec();
        responses.reverse();
        Self(Mutex::new(responses))
    }
}

impl InteractionModel for Sequenced {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let next = self.0.lock().unwrap().pop().unwrap_or(ANSWER);
        Ok(response(next))
    }
}

struct JournalProbeTool {
    items_path: std::path::PathBuf,
    saw_own_call: Arc<AtomicBool>,
}

impl AgentTool for JournalProbeTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "inspect the session item journal",
            false,
        ))
    }

    fn execute(&self, _: AgentToolRequest<'_>) -> AgentToolResult {
        let journal = std::fs::read_to_string(&self.items_path).unwrap_or_default();
        self.saw_own_call.store(
            journal.contains(r#""kind":"tool_call""#) && journal.contains(r#""tool":"read_file""#),
            Ordering::SeqCst,
        );
        AgentToolResult::succeeded("probed", serde_json::json!({}))
    }
}

#[test]
fn turn_items_are_journaled_before_the_turn_finishes() {
    let root = sinomo_testdir::TestDir::new("agent-durability-journal");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let saw_own_call = Arc::new(AtomicBool::new(false));
    let agent = Agent::new(
        Sequenced::new([READ_TOOL_CALL, ANSWER]),
        "fixture",
        "fixture",
    )
    .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(3))
    .with_tool_registry(
        AgentToolRegistry::new().with_tool(JournalProbeTool {
            items_path: store
                .paths()
                .session_items_path_for_session(&session.session_id),
            saw_own_call: Arc::clone(&saw_own_call),
        }),
    )
    .with_session_store(store.clone());
    let mut session = session;
    let turn_input = input(session.session_id.clone());
    agent.start_turn(&mut session, &turn_input).unwrap();

    assert!(
        saw_own_call.load(Ordering::SeqCst),
        "the tool call must be durable before the tool runs"
    );
    let journal = std::fs::read_to_string(
        store
            .paths()
            .session_items_path_for_session(&session.session_id),
    )
    .unwrap();
    let ids = journal
        .lines()
        .map(|line| {
            serde_json::from_str::<AgentSessionItemRecord>(line)
                .unwrap()
                .item_id
        })
        .collect::<Vec<_>>();
    let unique = ids.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ids.len(),
        unique.len(),
        "each item is journaled exactly once"
    );
}

/// Delegates to a real store but fails to append the records `fails` selects.
struct FailingRecords {
    inner: FileAgentSessionStore,
    fails: fn(&AgentSessionStoreRecordKind) -> bool,
}

impl AgentSessionStore for FailingRecords {
    fn load_session(&self, id: &SessionId) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        self.inner.load_session(id)
    }
    fn save_session(&self, session: &AgentSession) -> Result<(), AgentSessionStoreError> {
        self.inner.save_session(session)
    }
    fn append_items(
        &self,
        id: &SessionId,
        items: &[AgentItem],
    ) -> Result<(), AgentSessionStoreError> {
        self.inner.append_items(id, items)
    }
    fn append_projection(
        &self,
        id: &SessionId,
        projection: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError> {
        self.inner.append_projection(id, projection)
    }
    fn append_record(
        &self,
        record: &AgentSessionStoreRecord,
    ) -> Result<(), AgentSessionStoreError> {
        if (self.fails)(&record.record) {
            return Err(AgentSessionStoreError::Io {
                path: "injected".into(),
                detail: "injected record failure".into(),
            });
        }
        self.inner.append_record(record)
    }
}

#[test]
fn rejected_runtime_control_edit_is_not_resumed_after_restart() {
    let root = sinomo_testdir::TestDir::new("agent-durability-control-commit");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    store.save_session(&session).unwrap();
    // Control history is persisted before the snapshot commit.
    let agent =
        Agent::new(Sequenced::new([]), "fixture", "fixture").with_session_store(FailingRecords {
            inner: store.clone(),
            fails: |record| {
                matches!(
                    record,
                    AgentSessionStoreRecordKind::ControlSnapshotSaved { .. }
                )
            },
        });
    let runtime = AgentSessionRuntime::new(agent, session.clone());

    assert!(
        runtime
            .submit_op(AgentOp::steer("rejected steering"))
            .is_err()
    );
    assert!(runtime.submit_op(AgentOp::abort("rejected abort")).is_err());

    let resumed = store.load_session(&session.session_id).unwrap().unwrap();
    assert!(resumed.steering_queue.is_empty(), "{resumed:?}");
    assert!(resumed.abort.is_none(), "{resumed:?}");
    assert_eq!(runtime.session_snapshot().unwrap(), session);
}

struct BlockThenAnswer {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl InteractionModel for BlockThenAnswer {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.started.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        Ok(response(ANSWER))
    }
}

#[test]
fn accepted_cancellation_is_not_overwritten_by_the_workers_late_error() {
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let task = AgentTaskRef::new("chat", "cancel me");
    let root = sinomo_testdir::TestDir::new("agent-durability-cancel-final");
    // The worker winds down with an error after the cancel was accepted: it
    // cannot record the turn's completion.
    let agent = Agent::new(
        BlockThenAnswer {
            started: started_tx,
            release: Mutex::new(release_rx),
        },
        "fixture",
        "fixture",
    )
    .with_config(AgentConfig::new("fixture", "fixture").with_task_ref(task.clone()))
    .with_session_store(FailingRecords {
        inner: FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root)),
        fails: |record| matches!(record, AgentSessionStoreRecordKind::TurnCompleted { .. }),
    });
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent, session);
    let cursor = runtime.runtime_event_cursor().unwrap();
    runtime.submit_op(AgentOp::start_turn(turn_input)).unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let cancelled = runtime.interrupt_active_turn("user stopped it").unwrap();
    assert!(cancelled.accepted);
    release_tx.send(()).unwrap();
    assert!(
        runtime.join_active_turn().is_err(),
        "the fixture must make the worker fail after the cancel"
    );

    let status = runtime
        .control_snapshot()
        .unwrap()
        .tasks
        .into_iter()
        .find(|snapshot| snapshot.task.task_id == task.task_id)
        .map(|snapshot| snapshot.status);
    assert_eq!(status, Some(AgentTaskLifecycleStatus::Cancelled));
    let events = runtime.runtime_events_since(cursor).unwrap().events;
    assert!(
        !events.iter().any(|event| matches!(&event.payload,
            AgentRuntimeEventPayload::ControlEvent { event }
                if matches!(&event.payload, AgentEventPayload::OperationRejected { reason, .. }
                    if reason.contains("turn execution failed")))),
        "a cancelled turn's late error must not be reported as a failure"
    );
    assert!(events.iter().any(|event| matches!(
        event.payload,
        AgentRuntimeEventPayload::WorkerCompleted {
            stop_reason: AgentStopReason::Cancelled,
            ..
        }
    )));
}

#[test]
fn retained_tails_never_split_a_tool_call_from_its_result() {
    let transcript = AgentTranscript {
        items: vec![
            user_message("request"),
            tool_call(1, 0),
            tool_call(1, 1),
            tool_result(1, 0),
            tool_result(1, 1),
            user_message("latest"),
        ],
    };

    let tail = transcript.retained_tail(2);

    assert_eq!(tail.len(), 5);
    assert!(is_pair(&tail[0], "call", 1, 0));
    assert!(AgentToolPairIntegrity::from_items(&tail).is_balanced());
    assert_eq!(transcript.retained_tail(1).len(), 1);
}

#[test]
fn manual_compaction_summarizes_the_projection_and_drops_old_items() {
    let agent = Agent::new(Sequenced::new([]), "fixture", "fixture");
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    for index in 0..30 {
        session
            .transcript
            .push(user_message(&format!("message {index}")));
    }

    let first = agent.compact(&mut session, "manual").unwrap();
    assert!(
        session.transcript.items.len() < 30,
        "manual compaction must drop old items, kept {}",
        session.transcript.items.len()
    );
    assert_eq!(first.projection.source_item_count, 30);

    for index in 0..5 {
        session
            .transcript
            .push(user_message(&format!("later {index}")));
    }
    let second = agent.compact(&mut session, "manual again").unwrap();
    assert!(
        second.projection.source_item_count < 30,
        "the next compaction must summarize the projection, not the whole raw history ({})",
        second.projection.source_item_count
    );
    // The raw history still holds everything the first compaction removed.
    assert!(session.raw_transcript.items.len() >= 30);
}
