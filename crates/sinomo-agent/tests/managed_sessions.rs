use sinomo_agent::*;
use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
use sinomo_runtime::{
    events::SessionId, local::SinomoDataDir, module_api::sessions::*, node::NodeId,
};
use std::sync::Arc;

struct Model;
impl InteractionModel for Model {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: serde_json::to_string(&AgentLoopDecision {
                response_mode: AgentLoopResponseMode::Answer,
                text: "candidate complete".into(),
                tool_calls: vec![],
                background_handoff: None,
                next_state: AgentInteractionState::Listening,
            })
            .unwrap(),
            items: vec![],
            tool_calls: vec![],
            usage: None,
        })
    }
}
#[test]
fn module_root_uses_common_lifecycle_without_a_subagent_provider() {
    let temp = sinomo_testdir::TestDir::new("managed-module-root");
    let data = SinomoDataDir::new(temp.path());
    let store = Arc::new(FileAgentSessionStore::for_data_dir(&data));
    let host = AgentSessionHost::new(store.clone());
    let agent = Agent::new(Model, "fake", "fake")
        .with_session_store(FileAgentSessionStore::for_data_dir(&data));
    let interaction = InteractionSession::new(
        SessionId::generate(),
        NodeId::generate(),
        InteractionSurface::new("module-test", InteractionSurfaceKind::CommandLine),
    );
    let input = AgentLoopInput {
        latest_event: user_interaction_input_event(&interaction, "implement candidate"),
        session: interaction,
        transient_system_context: None,
        async_update: None,
        context_attachments: vec![],
        tool_specs: vec![],
    };
    let root = host
        .start(
            "module.test",
            agent,
            input,
            AgentCancellationToken::new(),
            86400,
            false,
        )
        .unwrap();
    assert!(root.parent_session_id.is_none());
    let id = root.session_id.to_string();
    assert!(
        host.owned_command("module.other", SessionCommand::Cancel { id: id.clone() })
            .is_err()
    );
    let wait = || {
        host.owned_command(
            "module.test",
            SessionCommand::Wait {
                id: id.clone(),
                timeout_ms: 5000,
            },
        )
        .unwrap()
    };
    assert_eq!(wait()["status"], "completed");
    let outcome = host
        .owned_command(
            "module.test",
            SessionCommand::Result {
                id: id.clone(),
                operation_id: root.operation_id.as_ref().unwrap().to_string(),
            },
        )
        .unwrap();
    assert_eq!(outcome["session_id"], id);
    assert_eq!(
        outcome["run"]["final_decision"]["text"],
        "candidate complete"
    );
    host.owned_command(
        "module.test",
        SessionCommand::Message {
            id: id.clone(),
            message: "next task".into(),
        },
    )
    .unwrap();
    assert_eq!(wait()["status"], "completed");
    assert!(
        !store
            .load_session(&root.session_id)
            .unwrap()
            .unwrap()
            .transcript
            .is_empty()
    );
    host.owned_command("module.test", SessionCommand::Archive { id })
        .unwrap();
    host.shutdown(std::time::Instant::now() + std::time::Duration::from_secs(5))
        .unwrap();
    assert!(
        store
            .load_session(&root.session_id)
            .unwrap()
            .unwrap()
            .managed
            .unwrap()
            .archived
    );
}

#[test]
fn paused_admission_has_a_durable_operation_and_controls_do_not_run_it_early() {
    use sinomo_runtime::module::sessions::SessionAccess;
    let temp = sinomo_testdir::TestDir::new("paused-module-admission");
    let data = SinomoDataDir::new(temp.path());
    let store = Arc::new(FileAgentSessionStore::for_data_dir(&data));
    let host = AgentSessionHost::new(store.clone());
    let make = || {
        let interaction = InteractionSession::new(
            SessionId::generate(),
            NodeId::generate(),
            InteractionSurface::new("paused-test", InteractionSurfaceKind::CommandLine),
        );
        let input = AgentLoopInput {
            latest_event: user_interaction_input_event(&interaction, "work"),
            session: interaction,
            transient_system_context: None,
            async_update: None,
            context_attachments: vec![],
            tool_specs: vec![],
        };
        host.start(
            "module.test",
            Agent::new(Model, "fake", "fake"),
            input,
            AgentCancellationToken::new(),
            86400,
            true,
        )
        .unwrap()
    };
    let root = make();
    let execution = SessionExecutionRef {
        session_id: root.session_id.to_string(),
        operation_id: root.operation_id.as_ref().unwrap().to_string(),
    };
    let access = sinomo_agent::session_access::AgentSessionAccess::new(
        store.clone(),
        host.owned_roots("module.test").unwrap(),
    );
    let state: SessionExecutionState = serde_json::from_value(
        access
            .request(SessionRequest::Execution {
                execution: execution.clone(),
            })
            .unwrap(),
    )
    .unwrap();
    assert_eq!(state.phase, SessionExecutionPhase::Paused);
    assert!(
        store
            .load_control_events(&root.session_id)
            .unwrap()
            .is_empty()
    );
    host.owned_command(
        "module.test",
        SessionCommand::Resume {
            id: execution.session_id.clone(),
        },
    )
    .unwrap();
    assert_eq!(
        host.owned_command(
            "module.test",
            SessionCommand::Wait {
                id: execution.session_id.clone(),
                timeout_ms: 5000
            }
        )
        .unwrap()["status"],
        "completed"
    );
    let state: SessionExecutionState = serde_json::from_value(
        access
            .request(SessionRequest::Execution {
                execution: execution.clone(),
            })
            .unwrap(),
    )
    .unwrap();
    assert_eq!(state.phase, SessionExecutionPhase::Completed);
    assert!(state.started_at.is_some());
    assert!(state.finished_at.is_some());
    let activity: SessionActivityPage = serde_json::from_value(
        access
            .request(SessionRequest::Activity {
                session_id: execution.session_id.clone(),
                after_sequence: None,
                tail: false,
                limit: 256,
            })
            .unwrap(),
    )
    .unwrap();
    assert!(
        activity
            .records
            .iter()
            .any(|item| item.operation_id.as_deref() == Some(&execution.operation_id))
    );
    let pending = make();
    let id = pending.session_id.to_string();
    host.owned_command("module.test", SessionCommand::Cancel { id: id.clone() })
        .unwrap();
    assert_eq!(
        host.owned_command(
            "module.test",
            SessionCommand::Wait {
                id,
                timeout_ms: 100
            }
        )
        .unwrap()["status"],
        "cancelled"
    );
    assert!(
        store
            .load_control_events(&pending.session_id)
            .unwrap()
            .iter()
            .all(|event| !matches!(event.payload, AgentEventPayload::TurnStarted { .. }))
    );
    host.shutdown(std::time::Instant::now() + std::time::Duration::from_secs(5))
        .unwrap();
}

#[test]
fn restart_records_interruption_without_replay_and_expired_history_is_archived() {
    use sinomo_runtime::module::sessions::SessionAccess;
    let temp = sinomo_testdir::TestDir::new("managed-session-recovery");
    let data = SinomoDataDir::new(temp.path());
    let store = Arc::new(FileAgentSessionStore::for_data_dir(&data));
    let host = AgentSessionHost::new(store.clone());
    let interaction = InteractionSession::new(
        SessionId::generate(),
        NodeId::generate(),
        InteractionSurface::new("recovery-test", InteractionSurfaceKind::CommandLine),
    );
    let input = AgentLoopInput {
        latest_event: user_interaction_input_event(&interaction, "work"),
        session: interaction,
        transient_system_context: None,
        async_update: None,
        context_attachments: vec![],
        tool_specs: vec![],
    };
    let root = host
        .start(
            "module.test",
            Agent::new(Model, "fake", "fake"),
            input,
            AgentCancellationToken::new(),
            86400,
            true,
        )
        .unwrap();
    let mut interrupted = store.load_session(&root.session_id).unwrap().unwrap();
    host.shutdown(std::time::Instant::now() + std::time::Duration::from_secs(5))
        .unwrap();
    // Restore the durable pre-execution snapshot to model abrupt process loss.
    interrupted.state_revision += 100;
    store.save_session(&interrupted).unwrap();
    let recovered = AgentSessionHost::new(store.clone());
    recovered.recover_interrupted().unwrap();
    let reference = SessionExecutionRef {
        session_id: root.session_id.to_string(),
        operation_id: root.operation_id.unwrap().to_string(),
    };
    let access = sinomo_agent::session_access::AgentSessionAccess::new(
        store.clone(),
        recovered.owned_roots("module.test").unwrap(),
    );
    let state: SessionExecutionState = serde_json::from_value(
        access
            .request(SessionRequest::Execution {
                execution: reference.clone(),
            })
            .unwrap(),
    )
    .unwrap();
    assert_eq!(state.phase, SessionExecutionPhase::Failed);
    assert!(state.error.unwrap().contains("not replayed"));
    assert!(
        store
            .load_control_events(&root.session_id)
            .unwrap()
            .iter()
            .all(|e| !matches!(e.payload, AgentEventPayload::TurnStarted { .. }))
    );
    let mut expired = store.load_session(&root.session_id).unwrap().unwrap();
    expired.managed.as_mut().unwrap().last_activity_unix -= 86401;
    expired.state_revision += 1;
    store.save_session(&expired).unwrap();
    let access = sinomo_agent::session_access::AgentSessionAccess::new(
        store.clone(),
        recovered.owned_roots("module.test").unwrap(),
    );
    assert_eq!(
        access
            .request(SessionRequest::List {
                include_archived: false,
                include_related: true
            })
            .unwrap(),
        serde_json::json!([])
    );
    let archived = access
        .request(SessionRequest::Inspect {
            session_id: reference.session_id.clone(),
        })
        .unwrap();
    assert_eq!(archived["managed"]["archived"], true);
    recovered
        .owned_command(
            "module.test",
            SessionCommand::Archive {
                id: reference.session_id,
            },
        )
        .unwrap();
    assert!(
        store
            .load_session(&root.session_id)
            .unwrap()
            .unwrap()
            .managed
            .unwrap()
            .archived
    );
}
