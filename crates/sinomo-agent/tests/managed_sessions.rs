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
        .start("module.test", agent, input, AgentCancellationToken::new())
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
        .owned_command("module.test", SessionCommand::Result { id: id.clone() })
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
