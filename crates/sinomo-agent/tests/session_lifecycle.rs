#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
//! Session lifecycle: a turn left running by an earlier process is reconciled
//! as an interrupted (paused) turn, and live runtimes report whether they
//! still have work in flight.

use sinomo_agent::*;
use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
use sinomo_runtime::{events::SessionId, local::SinomoDataDir, node::NodeId};

const ANSWER: &str = r#"{"response_mode":"answer","text":"Done.","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#;

struct Answers;

impl InteractionModel for Answers {
    fn complete(
        &self,
        _: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        Ok(LanguageModelResponse {
            provider: "fixture".into(),
            model: "fixture".into(),
            content: ANSWER.into(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

fn input(session_id: SessionId) -> AgentLoopInput {
    let session = InteractionSession::new(
        session_id,
        NodeId::generate(),
        InteractionSurface::new("session-lifecycle", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = user_interaction_input_event(&session, "Carry on.");
    AgentLoopInput {
        session,
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    }
}

/// Persist the control state an earlier process left behind when it stopped
/// in the middle of a turn.
fn persist_running_turn(
    store: &FileAgentSessionStore,
    session_id: &SessionId,
    host_instance: Option<String>,
) -> AgentTaskRef {
    let task = AgentTaskRef::new("chat", "the interrupted turn");
    let op_id = TaskId::generate();
    let mut active_turn = AgentActiveTurn::new(op_id.clone(), 7, None, Some(task.task_id.clone()));
    active_turn.host_instance = host_instance;
    let mut running = AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Running);
    running.active_op_id = Some(op_id);
    store
        .append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::ControlSnapshotSaved {
                snapshot: AgentSessionControlSnapshot {
                    session_id: session_id.clone(),
                    next_sequence: 8,
                    active_turn: Some(active_turn),
                    tasks: vec![running],
                    buffered_event_count: 0,
                    paused: false,
                },
            },
        ))
        .unwrap();
    task
}

fn restarted_runtime(
    root: &sinomo_testdir::TestDir,
    session_id: &SessionId,
) -> AgentSessionRuntime<Answers> {
    let data_dir = SinomoDataDir::new(root);
    let session = FileAgentSessionStore::for_data_dir(&data_dir)
        .load_session(session_id)
        .unwrap()
        .unwrap();
    AgentSessionRuntime::with_data_dir_storage(
        Agent::new(Answers, "fixture", "fixture"),
        session,
        &data_dir,
    )
}

#[test]
fn a_turn_left_running_by_an_earlier_process_is_paused_on_restart() {
    let root = sinomo_testdir::TestDir::new("agent-lifecycle-restart");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    store.save_session(&session).unwrap();
    let task = persist_running_turn(
        &store,
        &session.session_id,
        Some("an-earlier-process".into()),
    );

    let runtime = restarted_runtime(&root, &session.session_id);
    let recovered = runtime
        .recover_interrupted_turn()
        .unwrap()
        .expect("the stale turn is reconciled");
    assert!(recovered.snapshot.paused);
    assert!(recovered.snapshot.active_turn.is_none());
    assert!(runtime.session_snapshot().unwrap().is_paused());
    let control = runtime.control_snapshot().unwrap();
    assert!(
        control.next_sequence > 8,
        "sequences continue after restart"
    );
    let reconciled = control
        .tasks
        .iter()
        .find(|snapshot| snapshot.task.task_id == task.task_id)
        .expect("the interrupted task is carried over");
    assert_eq!(reconciled.status, AgentTaskLifecycleStatus::Paused);
    assert!(
        runtime
            .progress_since(AgentSessionControlEventCursor {
                offset: 0,
                last_event_id: None,
            })
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, AgentEventPayload::TurnInterrupted { .. }))
    );

    // The reconciled state is durable: inspection after another restart sees
    // a paused session with no running turn, and recovery is idempotent.
    let persisted = store
        .load_control_snapshot(&session.session_id)
        .unwrap()
        .unwrap();
    assert!(persisted.paused);
    assert!(persisted.active_turn.is_none());
    assert!(
        persisted
            .tasks
            .iter()
            .all(|snapshot| snapshot.status != AgentTaskLifecycleStatus::Running)
    );
    assert!(
        store
            .load_session(&session.session_id)
            .unwrap()
            .unwrap()
            .is_paused()
    );
    let again = restarted_runtime(&root, &session.session_id);
    assert!(again.recover_interrupted_turn().unwrap().is_none());

    // The next message resumes the session.
    runtime.submit_op(AgentOp::unpause()).unwrap();
    let result = runtime
        .submit_op(AgentOp::start_turn(input(session.session_id.clone())))
        .unwrap();
    assert!(result.accepted);
    runtime.join_active_turn().unwrap();
    assert!(!runtime.session_snapshot().unwrap().is_paused());
    assert!(!runtime.has_running_work().unwrap());
}

#[test]
fn a_legacy_running_turn_without_a_host_is_treated_as_interrupted() {
    let root = sinomo_testdir::TestDir::new("agent-lifecycle-legacy");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    store.save_session(&session).unwrap();
    persist_running_turn(&store, &session.session_id, None);

    let runtime = restarted_runtime(&root, &session.session_id);
    assert!(runtime.recover_interrupted_turn().unwrap().is_some());
    assert!(runtime.session_snapshot().unwrap().is_paused());
}

#[test]
fn a_turn_hosted_by_this_process_is_left_alone() {
    let root = sinomo_testdir::TestDir::new("agent-lifecycle-live-here");
    let store = FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(&root));
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    store.save_session(&session).unwrap();
    persist_running_turn(
        &store,
        &session.session_id,
        Some(agent_runtime_host_instance().to_string()),
    );

    let runtime = restarted_runtime(&root, &session.session_id);
    assert!(runtime.recover_interrupted_turn().unwrap().is_none());
    assert!(!runtime.session_snapshot().unwrap().is_paused());
}

#[test]
fn a_finished_session_has_nothing_to_recover() {
    let root = sinomo_testdir::TestDir::new("agent-lifecycle-finished");
    let data_dir = SinomoDataDir::new(&root);
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let runtime = AgentSessionRuntime::with_data_dir_storage(
        Agent::new(Answers, "fixture", "fixture"),
        session.clone(),
        &data_dir,
    );
    runtime
        .submit_op(AgentOp::start_turn(input(session.session_id.clone())))
        .unwrap();
    runtime.join_active_turn().unwrap();

    let restarted = restarted_runtime(&root, &session.session_id);
    assert!(restarted.recover_interrupted_turn().unwrap().is_none());
    assert!(!restarted.session_snapshot().unwrap().is_paused());
}

#[test]
fn purging_a_session_removes_its_local_data_only() {
    let root = sinomo_testdir::TestDir::new("agent-lifecycle-purge");
    let data_dir = SinomoDataDir::new(&root);
    let store = FileAgentSessionStore::for_data_dir(&data_dir);
    let paths = AgentStoragePaths::from_data_dir(&data_dir);
    let deleted = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let kept = AgentSession::new(SessionId::generate(), AgentRole::Main);
    for session in [&deleted, &kept] {
        let runtime = AgentSessionRuntime::with_data_dir_storage(
            Agent::new(Answers, "fixture", "fixture"),
            session.clone(),
            &data_dir,
        );
        runtime
            .submit_op(AgentOp::start_turn(input(session.session_id.clone())))
            .unwrap();
        runtime.join_active_turn().unwrap();
    }
    let evidence = paths.evidence_dir_for_session(&deleted.session_id);
    std::fs::create_dir_all(evidence.join("nested")).unwrap();
    std::fs::write(evidence.join("nested").join("proof.json"), b"{}").unwrap();

    assert!(store.purge_session(&deleted.session_id).unwrap());
    assert!(store.load_session(&deleted.session_id).unwrap().is_none());
    assert!(!paths.session_dir_for_session(&deleted.session_id).exists());
    assert!(!evidence.exists());
    assert!(store.load_session(&kept.session_id).unwrap().is_some());
    assert!(!store.purge_session(&deleted.session_id).unwrap());
}

#[test]
fn only_the_deleted_sessions_own_sandbox_is_released() {
    let root = sinomo_testdir::TestDir::new("agent-lifecycle-sandbox");
    let data_dir = SinomoDataDir::new(&root);
    let store = FileAgentSessionStore::for_data_dir(&data_dir);
    let runtime_root = root.join("sandbox").join("runtime");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    let mut owner = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let sandbox_id = local_workspace::agent_sandbox_id_for_session(&owner.session_id);
    owner
        .attach_sandbox(AgentSessionSandboxAttachment {
            sandbox_id: sandbox_id.to_string(),
            server_id: "local-daemon".into(),
            attachment_kind: AgentSessionSandboxAttachmentKind::Automatic,
        })
        .unwrap();
    store.save_session(&owner).unwrap();
    let config = local_workspace::LocalWorkspaceAgentConfig::new(&workspace)
        .with_sandbox_runtime_root(&runtime_root)
        .with_sandbox_instance_id(sandbox_id.clone());
    let sandboxes = sinomo_runtime::sandbox::SandboxLifecycleClient::in_process(data_dir.clone());
    local_workspace::provision_default_agent_session_sandbox(&sandboxes, &config, Vec::new())
        .unwrap();

    // Another session explicitly shares the sandbox, so it is still in use.
    let mut sharer = AgentSession::new(SessionId::generate(), AgentRole::Main);
    sharer
        .attach_sandbox(AgentSessionSandboxAttachment {
            sandbox_id: sandbox_id.to_string(),
            server_id: "local-daemon".into(),
            attachment_kind: AgentSessionSandboxAttachmentKind::Explicit,
        })
        .unwrap();
    store.save_session(&sharer).unwrap();
    store
        .save_session(&AgentSession::new(SessionId::generate(), AgentRole::Main))
        .unwrap();
    assert_eq!(
        store
            .sessions_attached_to_sandbox(sandbox_id.as_str(), &owner.session_id)
            .unwrap(),
        vec![sharer.session_id.clone()]
    );

    assert!(
        local_workspace::release_default_agent_session_sandbox(&sandboxes, None, &owner.session_id)
            .unwrap()
    );
    assert!(
        !local_workspace::release_default_agent_session_sandbox(
            &sandboxes,
            None,
            &owner.session_id
        )
        .unwrap(),
        "releasing an already released sandbox is a no-op"
    );
}

#[test]
fn a_released_runtime_does_not_remove_its_replacements_route() {
    let registry = AgentRuntimeControlRegistry::new();
    let session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let released =
        AgentSessionRuntime::new(Agent::new(Answers, "fixture", "fixture"), session.clone());
    let replacement =
        AgentSessionRuntime::new(Agent::new(Answers, "fixture", "fixture"), session.clone());
    registry.register_runtime(released.clone()).unwrap();
    registry.register_runtime(replacement.clone()).unwrap();

    // The released host exits after its replacement registered.
    assert!(!registry.unregister_runtime(&released).unwrap());
    assert!(registry.contains_session(&session.session_id).unwrap());
    assert!(registry.unregister_runtime(&replacement).unwrap());
    assert!(!registry.contains_session(&session.session_id).unwrap());
}
