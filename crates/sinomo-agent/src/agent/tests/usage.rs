use super::*;
use sinomo_runtime::api::app::usage::*;
pub(super) struct Probe {
    pub data: SinomoDataDir,
    _registration: sinomo_runtime::api::observe::recording::Registration,
    rx: std::sync::mpsc::Receiver<sinomo_runtime::api::observe::recording::Submission>,
}
impl Probe {
    pub fn new(data: SinomoDataDir) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(128);
        let registration = sinomo_runtime::api::observe::recording::register(&data, tx);
        Self {
            data,
            _registration: registration,
            rx,
        }
    }
    pub fn records(&self) -> Vec<UsageRecord> {
        self.rx
            .try_iter()
            .filter_map(|event| match event {
                sinomo_runtime::api::observe::recording::Submission::Event(event) => {
                    Some(serde_json::from_value(event.payload).unwrap())
                }
                _ => None,
            })
            .collect()
    }
    pub fn model<M: InteractionModel>(&self, model: M) -> Recorded<M> {
        Recorded {
            data: self.data.clone(),
            model,
        }
    }
}
pub(super) struct Recorded<M> {
    data: SinomoDataDir,
    model: M,
}
impl<M: InteractionModel> InteractionModel for Recorded<M> {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let observation = sinomo_lm::usage::RequestObservation::new(
            &self.data,
            &request.provider,
            &request.model,
            None,
            request,
        );
        let result = self.model.complete(request);
        observation.finish(&result, false, None, None);
        result
    }
}
#[test]
fn usage_turn_repair_compaction_and_tool_each_emit_one_attributed_observe_event() {
    let root = sinomo_testdir::TestDir::new("usage-agent-integration");
    let probe = Probe::new(SinomoDataDir::new(&root));
    let model=probe.model(SequencedModel::new([
        "not json",
        r#"{"response_mode":"defer","text":"Inspecting","tool_calls":[{"tool":"read_file","input":{"path":"Cargo.toml"},"async":false,"summary":"Read file"}],"background_handoff":null,"next_state":"waiting"}"#,
        r#"{"response_mode":"answer","text":"Done","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#,
        "A compact summary of the completed file inspection.",
    ]));
    let tools = AgentToolRegistry::new().with_tool(EchoTool);
    tools.set_usage_data_dir(probe.data.clone());
    let agent = Agent::new(model, "fake", "fake")
        .with_tool_registry(tools)
        .with_compactor(AgentCompactor {
            retained_tail_count: Some(0),
        });
    let mut input = agent_input("inspect");
    let conversation = SessionId::generate();
    input.session.conversation_id = Some(conversation.clone());
    input.session.participant_id = Some(TaskId::generate());
    let cause = input.latest_event.event_id.clone();
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let operation = AgentOp::start_turn(input);
    let op_id = operation.op_id.clone();
    let run = agent
        .handle_op(&mut session, operation)
        .unwrap()
        .run
        .unwrap();
    assert_eq!(run.tool_results.len(), 1);
    let compaction = AgentOp::compact("reduce context");
    let compact_id = compaction.op_id.clone();
    agent.handle_op(&mut session, compaction).unwrap();
    let records = probe.records();
    let models = records
        .iter()
        .filter(|r| matches!(r.event, UsageEvent::ModelRequest { .. }))
        .collect::<Vec<_>>();
    assert_eq!(models.len(), 4);
    assert_eq!(
        models
            .iter()
            .filter(|r| r.attribution.purpose == "decision_repair")
            .count(),
        1
    );
    let compact = models
        .iter()
        .find(|r| r.attribution.purpose == "compaction")
        .unwrap();
    assert_eq!(
        compact.attribution.operation.as_deref(),
        Some(compact_id.as_str())
    );
    for record in models
        .iter()
        .filter(|r| r.attribution.purpose != "compaction")
    {
        assert_eq!(
            record.attribution.session.as_deref(),
            Some(session.session_id.as_str())
        );
        assert_eq!(
            record.attribution.chat.as_deref(),
            Some(conversation.as_str())
        );
        assert_eq!(record.attribution.turn.as_deref(), Some(op_id.as_str()));
        assert_eq!(record.attribution.caused_by, vec![cause.to_string()]);
    }
    let tools = records
        .iter()
        .filter(|r| matches!(r.event, UsageEvent::ToolCall { .. }))
        .collect::<Vec<_>>();
    assert_eq!(tools.len(), 1);
    assert_eq!(
        tools[0].attribution.tool_call.as_deref(),
        Some(run.tool_results[0].call_id.as_str())
    );
    assert_eq!(
        tools[0].attribution.operation.as_deref(),
        Some(op_id.as_str())
    );
}
