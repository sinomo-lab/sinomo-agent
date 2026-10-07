#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
//! `async` tool calls run detached when the host can continue the session:
//! the turn moves on, and the result arrives later as an async update that
//! names the call that started it.

use sinomo_agent::*;
use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
use sinomo_runtime::{events::SessionId, node::NodeId};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
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

/// Requests the async tool first and answers afterwards.
#[derive(Clone, Default)]
struct AsyncToolThenAnswer(Arc<AtomicUsize>);

impl InteractionModel for AsyncToolThenAnswer {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let content = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            ASYNC_TOOL_CALL
        } else {
            ANSWER
        };
        Ok(LanguageModelResponse {
            provider: "fixture".into(),
            model: "fixture".into(),
            content: content.into(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
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

fn agent(model: AsyncToolThenAnswer, release: mpsc::Receiver<()>) -> Agent<AsyncToolThenAnswer> {
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
    let model = AsyncToolThenAnswer::default();
    let requests = Arc::clone(&model.0);
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent(model, release_rx), session);

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
    assert_eq!(requests.load(Ordering::SeqCst), 2);

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
    assert_eq!(
        requests.load(Ordering::SeqCst),
        3,
        "the continuation asked the model"
    );
    let session = runtime.session_snapshot().unwrap();
    assert!(
        session.raw_transcript.items.iter().any(|item| matches!(item,
            AgentItem::AsyncTaskUpdate { update, .. } if update.tool_call_id.as_ref() == Some(&call_id))),
        "the continuation records the update with the call's id"
    );
}

#[test]
fn async_call_runs_inline_without_a_host_to_continue_the_session() {
    let (release_tx, release_rx) = mpsc::channel();
    release_tx.send(()).unwrap();
    let agent = agent(AsyncToolThenAnswer::default(), release_rx);
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
}
