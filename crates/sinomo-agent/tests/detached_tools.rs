#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
//! `async` tool calls run detached when the host can continue the session:
//! the turn moves on, and the result arrives later as an async update that
//! names the call that started it.

use sinomo_agent::*;
use sinomo_lm::{ModelScript, ScriptedExpectation, ScriptedProvider, ScriptedReply};
use sinomo_runtime::{events::SessionId, node::NodeId};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

const ANSWER: &str = r#"{"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#;
const ASYNC_TOOL_CALL: &str = r#"{"response_mode":"tool_calls","text":"","tool_calls":[{"tool":"read_file","input":{},"async":true,"summary":"slow work"}],"background_handoff":null,"next_state":"waiting"}"#;

fn input(session_id: SessionId) -> AgentLoopInput {
    let session = InteractionSession::new(
        session_id,
        NodeId::generate(),
        InteractionSurface::new("detached-tools", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = user_interaction_input_event(&session, "Do the slow work.");
    AgentLoopInput {
        session,
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    }
}

/// Requests the async tool and answers once the call is under way.
fn async_tool_then_answer() -> ModelScript {
    ModelScript::new()
        .reply(ScriptedReply::new().text(ASYNC_TOOL_CALL))
        .reply(ScriptedReply::new().text(ANSWER))
}

/// Blocks until released, then reports success.
struct GatedTool {
    release: Mutex<mpsc::Receiver<()>>,
}

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
        let _ = self
            .release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
        AgentToolResult::succeeded("slow work done", serde_json::json!({ "done": true }))
    }
}

fn agent(model: ScriptedProvider, release: mpsc::Receiver<()>) -> Agent<ScriptedProvider> {
    Agent::new(model, "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(5))
        .with_tool_registry(AgentToolRegistry::new().with_tool(GatedTool {
            release: Mutex::new(release),
        }))
}

fn recorded_call_id(session: &AgentSession) -> TaskId {
    session
        .raw_transcript
        .items
        .iter()
        .find_map(|item| match item {
            AgentItem::ToolCall { tool_call_id, .. } => Some(tool_call_id.clone()),
            _ => None,
        })
        .expect("the tool call is recorded")
}

#[test]
fn async_call_detaches_and_its_completion_continues_the_session() {
    let (release_tx, release_rx) = mpsc::channel();
    let model = ScriptedProvider::new(
        async_tool_then_answer()
            .expect(ScriptedExpectation::contains("Background work finished."))
            .reply(ScriptedReply::new().text(ANSWER)),
    );
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent(model.clone(), release_rx), session);

    runtime
        .submit_op(AgentOp::start_turn(turn_input.clone()))
        .unwrap();
    runtime.join_active_turn().unwrap();

    // The turn finished while the tool still runs.
    let session = runtime.session_snapshot().unwrap();
    let call_id = recorded_call_id(&session);
    assert!(
        session
            .raw_transcript
            .items
            .iter()
            .any(|item| matches!(item,
        AgentItem::ToolResult { call_id: result_call, output, .. }
            if *result_call == call_id && output["detached"] == true))
    );
    assert!(!runtime.has_async_updates(), "the tool has not finished");
    assert!(
        runtime.has_running_work().unwrap(),
        "running detached work keeps the session's host"
    );
    assert_eq!(model.request_count(), 2);

    release_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !runtime.has_async_updates() {
        assert!(
            Instant::now() < deadline,
            "the detached call never completed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let updates = runtime.take_async_updates();
    assert_eq!(updates.len(), 1);
    let update = updates[0].clone();
    assert_eq!(update.tool_call_id.as_ref(), Some(&call_id));
    assert_eq!(update.status, AsyncTaskStatus::Completed);
    assert!(update.summary.contains("slow work done"));
    assert!(
        runtime.take_async_updates().is_empty(),
        "each update is delivered once"
    );
    assert!(!runtime.has_running_work().unwrap());

    // The host continues the session with the update.
    let mut continuation = turn_input;
    continuation.transient_system_context = Some("Background work finished.".to_string());
    continuation.async_update = Some(update);
    runtime
        .submit_op(AgentOp::continue_turn(continuation))
        .unwrap();
    runtime.join_active_turn().unwrap();
    assert_eq!(model.request_count(), 3, "the continuation asked the model");
    let session = runtime.session_snapshot().unwrap();
    assert!(
        session.raw_transcript.items.iter().any(|item| matches!(item,
            AgentItem::AsyncTaskUpdate { update, .. } if update.tool_call_id.as_ref() == Some(&call_id))),
        "the continuation records the update with the call's id"
    );
    model.assert_finished();
}

#[test]
fn async_call_runs_inline_without_a_host_to_continue_the_session() {
    let (release_tx, release_rx) = mpsc::channel();
    release_tx.send(()).unwrap();
    let model = ScriptedProvider::new(async_tool_then_answer());
    let agent = agent(model.clone(), release_rx);
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());

    agent.start_turn(&mut session, &turn_input).unwrap();

    assert!(
        session
            .raw_transcript
            .items
            .iter()
            .any(|item| matches!(item,
        AgentItem::ToolResult { output, .. } if output["done"] == true))
    );
    model.assert_finished();
}

fn wait_for_update(runtime: &AgentSessionRuntime<ScriptedProvider>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !runtime.has_async_updates() {
        assert!(
            Instant::now() < deadline,
            "the detached call never completed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Runs a turn whose `async` call completes, without continuing with it.
fn completed_detached_call(
    store: &FileAgentSessionStore,
) -> (AgentSession, AgentLoopInput, TaskId) {
    let (release_tx, release_rx) = mpsc::channel();
    release_tx.send(()).unwrap();
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(
        agent(ScriptedProvider::new(async_tool_then_answer()), release_rx)
            .with_session_store(store.clone()),
        session,
    );
    runtime
        .submit_op(AgentOp::start_turn(turn_input.clone()))
        .unwrap();
    runtime.join_active_turn().unwrap();
    wait_for_update(&runtime);
    let session = runtime.session_snapshot().unwrap();
    let task_id = runtime.take_async_updates()[0].task_id.clone();
    (session, turn_input, task_id)
}

/// Attaches a new runtime, as after a restart, whose model follows `script`.
fn reattached(
    store: &FileAgentSessionStore,
    session_id: &SessionId,
    script: ModelScript,
) -> AgentSessionRuntime<ScriptedProvider> {
    let (_release_tx, release_rx) = mpsc::channel();
    let session = store
        .load_session(session_id)
        .unwrap()
        .expect("saved session");
    let runtime = AgentSessionRuntime::new(
        agent(ScriptedProvider::new(script), release_rx).with_session_store(store.clone()),
        session,
    );
    runtime.recover_interrupted_turn().unwrap();
    runtime
}

#[test]
fn a_completion_waiting_to_be_continued_survives_a_restart() {
    let root = sinomo_testdir::TestDir::new("agent-detached-restart-completion");
    let store =
        FileAgentSessionStore::for_data_dir(&sinomo_runtime::local::SinomoDataDir::new(&root));
    let (session, turn_input, task_id) = completed_detached_call(&store);

    // The process stops before the session continued with the completion.
    let runtime = reattached(&store, &session.session_id, async_tool_then_answer());
    let updates = runtime.take_async_updates();
    assert_eq!(updates.len(), 1, "the completion is waiting again");
    assert_eq!(updates[0].task_id, task_id);
    assert_eq!(updates[0].status, AsyncTaskStatus::Completed);

    // Continuing with it marks it delivered, so it is not continued twice.
    let mut continuation = turn_input;
    continuation.transient_system_context = Some("Background work finished.".to_string());
    continuation.async_update = Some(updates[0].clone());
    runtime
        .submit_op(AgentOp::continue_turn(continuation))
        .unwrap();
    runtime.join_active_turn().unwrap();
    // (The continuation's model asks for another async call; only the
    // delivered one must be gone.)
    assert!(
        store
            .load_detached_calls(&session.session_id)
            .unwrap()
            .iter()
            .all(|call| call.task_id != task_id),
        "the completion is delivered"
    );
    let runtime = reattached(&store, &session.session_id, ModelScript::new());
    assert!(
        runtime
            .take_async_updates()
            .iter()
            .all(|update| update.task_id != task_id),
        "a delivered completion is not continued again"
    );
}

#[test]
fn a_call_lost_with_its_process_completes_as_interrupted() {
    let root = sinomo_testdir::TestDir::new("agent-detached-restart-interrupted");
    let store =
        FileAgentSessionStore::for_data_dir(&sinomo_runtime::local::SinomoDataDir::new(&root));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    store.save_session(&session).unwrap();
    let task_id = TaskId::generate();
    let tool_call_id = TaskId::generate();
    // An earlier process started the call and stopped before it finished.
    store
        .append_record(&AgentSessionStoreRecord::new(
            session.session_id.clone(),
            AgentSessionStoreRecordKind::DetachedCallStarted {
                task_id: task_id.clone(),
                tool_call_id: tool_call_id.clone(),
                tool: "exec_command".to_string(),
            },
        ))
        .unwrap();

    let runtime = reattached(&store, &session.session_id, ModelScript::new());
    assert!(
        runtime.has_running_work().unwrap(),
        "a completion waiting to be continued keeps the host"
    );
    let updates = runtime.take_async_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].status, AsyncTaskStatus::Failed);
    assert_eq!(updates[0].tool_call_id.as_ref(), Some(&tool_call_id));
    assert!(updates[0].summary.contains("interrupted"));
    assert!(
        store.load_detached_calls(&session.session_id).unwrap()[0]
            .completion
            .is_some(),
        "the interruption is recorded, so a later attach does not decide again"
    );
}

#[test]
fn a_paused_session_holds_its_completions_until_resumed() {
    let root = sinomo_testdir::TestDir::new("agent-detached-paused");
    let store =
        FileAgentSessionStore::for_data_dir(&sinomo_runtime::local::SinomoDataDir::new(&root));
    let (session, _, _) = completed_detached_call(&store);
    let runtime = reattached(&store, &session.session_id, ModelScript::new());
    runtime.submit_op(AgentOp::pause("user paused")).unwrap();
    assert!(!runtime.has_async_updates());
    assert!(runtime.take_async_updates().is_empty());
    runtime.submit_op(AgentOp::unpause()).unwrap();
    assert_eq!(runtime.take_async_updates().len(), 1);
}
