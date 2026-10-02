#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
//! Cancellation stops all in-flight work; pause only stops further model
//! requests while running tools keep going.

use sinomo_agent::*;
use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
use sinomo_runtime::{events::SessionId, node::NodeId};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

const ANSWER: &str = r#"{"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#;
const TOOL_CALL: &str = r#"{"response_mode":"tool_calls","text":"","tool_calls":[{"tool":"read_file","input":{},"async":false,"summary":"work"}],"background_handoff":null,"next_state":"waiting"}"#;

fn input(session_id: SessionId) -> AgentLoopInput {
    let session = InteractionSession::new(
        session_id,
        NodeId::generate(),
        InteractionSurface::new("cancel-and-pause", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = user_interaction_input_event(&session, "Do the work.");
    AgentLoopInput {
        session,
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    }
}

/// Calls the tool on its first request and answers afterwards.
#[derive(Clone, Default)]
struct ToolThenAnswer(Arc<AtomicUsize>);

impl InteractionModel for ToolThenAnswer {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let content = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            TOOL_CALL
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

/// Blocks until released, ignoring cancellation, then reports success.
struct GatedTool {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    finished: Arc<AtomicBool>,
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
        let _ = self.started.send(());
        let _ = self
            .release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
        self.finished.store(true, Ordering::SeqCst);
        AgentToolResult::succeeded("slow work finished", serde_json::json!({ "done": true }))
    }
}

/// Runs until its cancellation token fires.
struct CooperativeTool {
    started: mpsc::Sender<()>,
}

impl AgentTool for CooperativeTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "work until cancelled",
            false,
        ))
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        let _ = self.started.send(());
        while !request.cancellation_token.is_cancelled() {
            std::thread::sleep(Duration::from_millis(5));
        }
        AgentToolResult::cancelled("stopped", serde_json::json!({ "stopped": true }))
    }
}

fn tool_results(session: &AgentSession) -> Vec<(AgentToolStatus, serde_json::Value)> {
    session
        .transcript
        .items
        .iter()
        .filter_map(|item| match item {
            AgentItem::ToolResult { status, output, .. } => Some((status.clone(), output.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn pause_lets_the_running_tool_finish_and_starts_no_new_model_request() {
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let model = ToolThenAnswer::default();
    let requests = Arc::clone(&model.0);
    let agent = Agent::new(model, "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(5))
        .with_tool_registry(AgentToolRegistry::new().with_tool(GatedTool {
            started: started_tx,
            release: Mutex::new(release_rx),
            finished: Arc::clone(&finished),
        }));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent, session);
    let op = AgentOp::start_turn(turn_input.clone());
    let op_id = op.op_id.clone();
    runtime.submit_op(op).unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let paused = runtime.submit_op(AgentOp::pause("user paused")).unwrap();
    assert!(paused.accepted);
    release_tx.send(()).unwrap();
    runtime.join_active_turn().unwrap();

    assert!(
        finished.load(Ordering::SeqCst),
        "the running tool must finish"
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "no model request may start after the pause"
    );
    let outcome = runtime.turn_result(&op_id).unwrap().unwrap();
    assert_eq!(outcome.run.unwrap().stop_reason, AgentStopReason::Paused);
    let session = runtime.session_snapshot().unwrap();
    assert!(session.is_paused());
    assert_eq!(
        tool_results(&session),
        vec![(
            AgentToolStatus::Succeeded,
            serde_json::json!({ "done": true })
        )],
        "the tool's result is kept"
    );

    // A paused session runs no new turns until it is unpaused.
    let rejected = runtime
        .submit_op(AgentOp::start_turn(turn_input.clone()))
        .unwrap();
    assert!(!rejected.accepted);
    assert!(rejected.events.iter().any(|event| matches!(&event.payload,
        AgentEventPayload::OperationRejected { reason, .. } if reason.contains("paused"))));

    runtime.submit_op(AgentOp::unpause()).unwrap();
    runtime
        .submit_op(AgentOp::continue_turn(turn_input))
        .unwrap();
    runtime.join_active_turn().unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 2, "the continuation ran");
    assert!(!runtime.session_snapshot().unwrap().is_paused());
}

#[test]
fn abort_cancels_the_running_turn_and_its_tool() {
    let (started_tx, started_rx) = mpsc::channel();
    let agent = Agent::new(ToolThenAnswer::default(), "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(5))
        .with_tool_registry(AgentToolRegistry::new().with_tool(CooperativeTool {
            started: started_tx,
        }));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let turn_input = input(session.session_id.clone());
    let runtime = AgentSessionRuntime::new(agent, session);
    runtime.submit_op(AgentOp::start_turn(turn_input)).unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let started = Instant::now();
    runtime
        .submit_op(AgentOp::abort("user stopped everything"))
        .unwrap();
    runtime.join_active_turn().unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));

    let session = runtime.session_snapshot().unwrap();
    assert!(session.is_aborted());
    assert_eq!(
        tool_results(&session)
            .first()
            .map(|(status, _)| status.clone()),
        Some(AgentToolStatus::Cancelled)
    );
    assert!(runtime.active_turn().unwrap().is_none());
}

#[test]
fn a_tool_that_ignores_cancellation_is_abandoned_promptly() {
    let (started_tx, started_rx) = mpsc::channel();
    let (_release_tx, release_rx) = mpsc::channel();
    let registry = AgentToolRegistry::new().with_tool(GatedTool {
        started: started_tx,
        release: Mutex::new(release_rx),
        finished: Arc::new(AtomicBool::new(false)),
    });
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::generate(),
        InteractionSurface::new("cancel-and-pause", InteractionSurfaceKind::CommandLine),
    );
    let token = AgentCancellationToken::new();
    let canceller = token.clone();
    std::thread::spawn(move || {
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        canceller.cancel();
    });
    let started = Instant::now();
    let outcome = registry.execute_with_cancellation(
        &session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "read_file".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "slow".to_string(),
        },
        token,
    );

    assert!(
        started.elapsed() < Duration::from_secs(6),
        "a cancelled call must not wait for the tool ({:?})",
        started.elapsed()
    );
    assert_eq!(outcome.status, AgentToolStatus::Cancelled);
    assert_eq!(outcome.output["error"], "tool_cancellation_unconfirmed");
}

#[test]
fn interrupt_is_pause_and_cancel_stops_the_turn() {
    // Interrupt lets the running tool finish and leaves the session paused.
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let agent = Agent::new(ToolThenAnswer::default(), "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(5))
        .with_tool_registry(AgentToolRegistry::new().with_tool(GatedTool {
            started: started_tx,
            release: Mutex::new(release_rx),
            finished: Arc::clone(&finished),
        }));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let runtime = AgentSessionRuntime::new(agent, session.clone());
    runtime
        .submit_op(AgentOp::start_turn(input(session.session_id.clone())))
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(runtime.interrupt_active_turn("esc").unwrap().accepted);
    release_tx.send(()).unwrap();
    runtime.join_active_turn().unwrap();
    assert!(finished.load(Ordering::SeqCst));
    assert!(runtime.session_snapshot().unwrap().is_paused());
    assert!(runtime.control_snapshot().unwrap().paused);

    // Cancel stops a running tool and does not pause.
    let (started_tx, started_rx) = mpsc::channel();
    let agent = Agent::new(ToolThenAnswer::default(), "fixture", "fixture")
        .with_config(AgentConfig::new("fixture", "fixture").with_max_steps(5))
        .with_tool_registry(AgentToolRegistry::new().with_tool(CooperativeTool {
            started: started_tx,
        }));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let runtime = AgentSessionRuntime::new(agent, session.clone());
    runtime
        .submit_op(AgentOp::start_turn(input(session.session_id.clone())))
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(runtime.cancel_active_turn("stop now").unwrap().accepted);
    runtime.join_active_turn().unwrap();
    let session = runtime.session_snapshot().unwrap();
    assert!(!session.is_paused());
    assert_eq!(
        tool_results(&session)
            .first()
            .map(|(status, _)| status.clone()),
        Some(AgentToolStatus::Cancelled)
    );
}
