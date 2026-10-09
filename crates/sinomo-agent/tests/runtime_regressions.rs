#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
use sinomo_agent::*;
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelStreamEvent,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::node::NodeId;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

fn input() -> AgentLoopInput {
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::generate(),
        InteractionSurface::new("review", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = user_interaction_input_event(&session, "test turn");
    AgentLoopInput {
        session,
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: vec![],
        tool_specs: vec![],
    }
}

struct Model {
    fail: bool,
}
impl InteractionModel for Model {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if self.fail {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: "review".into(),
                model: "review".into(),
                detail: "injected provider failure".into(),
            });
        }
        Ok(LanguageModelResponse { provider: "review".into(), model: "review".into(), content: r#"{"response_mode":"answer","text":"done","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#.into(), items: vec![], tool_calls: vec![], usage: None })
    }
}

struct SaveFailure;
impl AgentSessionStore for SaveFailure {
    fn load_session(&self, _: &SessionId) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        Ok(None)
    }
    fn save_session(&self, _: &AgentSession) -> Result<(), AgentSessionStoreError> {
        Err(AgentSessionStoreError::Io {
            path: "snapshot".into(),
            detail: "injected snapshot failure".into(),
        })
    }
    fn append_items(
        &self,
        _: &SessionId,
        _: &AgentItemOrigin,
        _: &[AgentItem],
    ) -> Result<(), AgentSessionStoreError> {
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
        Ok(())
    }
}

#[test]
fn failed_completed_snapshot_must_fail_join() {
    let input = input();
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Main);
    let runtime = AgentSessionRuntime::new(
        Agent::new(Model { fail: false }, "review", "review").with_session_store(SaveFailure),
        session,
    );
    let task = AgentOp::start_turn(input);
    let task_id = task.op_id.clone();
    let cursor = runtime.runtime_event_cursor().unwrap();
    runtime.submit_op(task).unwrap();
    let joined = runtime.join_active_turn();
    assert!(
        joined.is_err(),
        "snapshot save failure must reach the caller; got {joined:?}"
    );
    assert!(runtime.turn_result(&task_id).unwrap().is_none());
    let events = runtime.runtime_events_since(cursor).unwrap().events;
    assert!(events.iter().any(|event| matches!(&event.payload,
        AgentRuntimeEventPayload::WorkerFailed { detail, .. } if detail.contains("snapshot failure"))));
    assert!(!events.iter().any(|event| matches!(
        event.payload,
        AgentRuntimeEventPayload::WorkerCompleted { .. }
            | AgentRuntimeEventPayload::TurnFinalDecision { .. }
    )));
}

#[test]
fn model_failure_must_release_synchronous_active_turn() {
    let input = input();
    let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Main);
    let mut control = AgentSessionControlPlane::new(session);
    let agent = Agent::new(Model { fail: true }, "review", "review");
    assert!(
        control
            .submit_op(&agent, AgentOp::start_turn(input.clone()))
            .is_err()
    );
    assert!(control.active_turn().is_none());
    let recovered = Agent::new(Model { fail: false }, "review", "review");
    let retry = control
        .submit_op(&recovered, AgentOp::start_turn(input))
        .expect("retry after provider recovery");
    assert!(retry.accepted);
    assert_eq!(
        retry.outcome.unwrap().run.unwrap().stop_reason,
        AgentStopReason::FinalDecision
    );
}

struct CooperativeModel {
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}
impl InteractionModel for CooperativeModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        Model { fail: false }.complete(request)
    }
    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        token: &AgentCancellationToken,
        _: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.started.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !token.is_cancelled()
            && !self.release.load(Ordering::SeqCst)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(2));
        }
        self.cancelled.store(token.is_cancelled(), Ordering::SeqCst);
        self.complete(request)
    }
}

#[test]
fn runtime_turn_timeout_must_cancel_cooperative_model() {
    let input = input();
    let started = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let model = CooperativeModel {
        started: started.clone(),
        cancelled: cancelled.clone(),
        release: release.clone(),
    };
    let runtime = AgentSessionRuntime::new(
        Agent::new(model, "review", "review"),
        AgentSession::new(input.session.session_id.clone(), AgentRole::Main),
    );
    runtime
        .submit_op(AgentOp::start_turn(input.clone()).with_timeout_ms(100))
        .unwrap();
    let wait_started = Instant::now();
    runtime.join_active_turn().unwrap();
    assert!(
        wait_started.elapsed() < Duration::from_secs(2),
        "cooperative turn must stop at its deadline"
    );
    assert!(
        cancelled.load(Ordering::SeqCst),
        "model observed its timeout token"
    );
    assert!(runtime.active_turn().unwrap().is_none());
    assert!(
        runtime
            .progress_since(AgentSessionControlEventCursor {
                offset: 0,
                last_event_id: None
            })
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, AgentEventPayload::TurnTimedOut { .. }))
    );
    // A finished turn's deadline must not poison the next turn's token.
    release.store(true, Ordering::SeqCst);
    let next = AgentOp::start_turn(input);
    let next_id = next.op_id.clone();
    runtime.submit_op(next).unwrap();
    runtime.join_active_turn().unwrap();
    assert_eq!(
        runtime
            .turn_result(&next_id)
            .unwrap()
            .unwrap()
            .run
            .unwrap()
            .stop_reason,
        AgentStopReason::FinalDecision
    );
}

#[test]
fn write_tool_accepts_a_valid_long_filename() {
    let root = sinomo_testdir::TestDir::new("agent-review-long-name");
    let name = "x".repeat(230);
    std::fs::write(root.join(&name), b"before").expect("host filesystem supports this filename");
    let registry = AgentToolRegistry::local_workspace_with_config(
        LocalWorkspaceAgentConfig::new(root.path()).allowing_file_writes(true),
    );
    let result = registry.execute(
        &input().session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "write_file".into(),
            input: serde_json::json!({"path":name,"content":"after","overwrite":true}),
            r#async: false,
            summary: "write valid long filename".into(),
        },
    );
    assert_eq!(
        result.status,
        AgentToolStatus::Succeeded,
        "valid filename failed: {}",
        result.output
    );
}

#[test]
fn synchronous_turn_timeout_releases_the_active_turn() {
    let input = input();
    let cancelled = Arc::new(AtomicBool::new(false));
    let task = AgentTaskRef::new("test", "deadline task");
    let agent = Agent::new(
        CooperativeModel {
            started: Arc::new(AtomicBool::new(false)),
            cancelled: cancelled.clone(),
            release: Arc::new(AtomicBool::new(false)),
        },
        "review",
        "review",
    )
    .with_config(AgentConfig::new("review", "review").with_task_ref(task.clone()));
    let mut control = AgentSessionControlPlane::new(AgentSession::new(
        input.session.session_id.clone(),
        AgentRole::Main,
    ));
    let result = control
        .submit_op(&agent, AgentOp::start_turn(input).with_timeout_ms(100))
        .unwrap();
    assert!(cancelled.load(Ordering::SeqCst));
    assert!(control.active_turn().is_none());
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event.payload, AgentEventPayload::TurnTimedOut { .. }))
    );
    assert_eq!(
        control.terminal_result(&task.task_id).unwrap().status,
        AgentTaskLifecycleStatus::TimedOut
    );
}

#[test]
fn zero_deadline_prevents_model_execution() {
    let input = input();
    let agent = Agent::new(Model { fail: true }, "review", "review");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Main);
    let result = agent
        .handle_op(&mut session, AgentOp::start_turn(input).with_timeout_ms(0))
        .unwrap();
    assert_eq!(result.run.unwrap().stop_reason, AgentStopReason::Cancelled);
}
