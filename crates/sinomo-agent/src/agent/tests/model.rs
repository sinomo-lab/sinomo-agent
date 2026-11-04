use super::*;

#[test]
fn agent_records_streaming_text_deltas_when_model_supports_them() {
    let response = r#"{
        "response_mode": "answer",
        "text": "Done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#;
    let agent = Agent::new(
        StreamingModel::new(response, ["chunk-one", "chunk-two"]),
        "fake",
        "fake",
    );

    let run = agent.run(&agent_input("stream")).expect("run");

    let deltas: Vec<_> = run
        .model_events
        .iter()
        .filter_map(|event| match event {
            AgentModelEvent::AssistantTextDelta { delta, .. } => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(deltas, vec!["chunk-one", "chunk-two"]);
    assert!(
        !deltas
            .iter()
            .any(|delta| delta.contains("\"response_mode\""))
    );
}

#[test]
fn agent_repairs_malformed_model_response_once() {
    let model = SequencedUsageModel::new([
        (
            "not json",
            Some(sinomo_lm::LanguageModelUsage {
                input_tokens: Some(10),
                output_tokens: Some(1),
                ..Default::default()
            }),
        ),
        (
            r#"{
                "response_mode": "answer",
                "text": "Recovered.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#,
            Some(sinomo_lm::LanguageModelUsage {
                input_tokens: Some(20),
                output_tokens: Some(2),
                ..Default::default()
            }),
        ),
    ]);
    let agent = Agent::new(model, "fake", "fake");

    let run = agent.run(&agent_input("recover")).expect("run");

    assert_eq!(run.final_decision.text, "Recovered.");
    assert_eq!(
        run.model_metrics.final_decision_source,
        Some(AgentModelDecisionSource::StrictJsonDecision)
    );
    assert_eq!(run.model_metrics.model_turn_count, 1);
    assert_eq!(run.model_metrics.model_response_count, 2);
    assert_eq!(run.model_metrics.model_response_parse_failure_count, 1);
    assert_eq!(run.model_metrics.strict_json_decision_turn_count, 1);
    assert_eq!(run.model_metrics.repair_attempt_count, 1);
    assert_eq!(run.model_metrics.reported_usage.response_count, 2);
    assert_eq!(run.model_metrics.reported_usage.input_tokens, 30);
    assert_eq!(run.model_metrics.reported_usage.output_tokens, 3);
    assert_eq!(run.model_metrics.repair_reported_usage.response_count, 1);
    assert_eq!(run.model_metrics.repair_reported_usage.input_tokens, 20);
    assert_eq!(run.model_metrics.repair_reported_usage.output_tokens, 2);
    let requests = agent.model.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    let repair_message_bytes = requests[1]
        .messages
        .last()
        .expect("repair message")
        .content
        .len();
    assert_eq!(
        run.model_metrics.repair_request_model_visible_bytes,
        repair_message_bytes
    );
    assert!(
        requests[1]
            .messages
            .last()
            .expect("repair message")
            .content
            .contains("agent_model_response_repair")
    );
    assert!(run.items.iter().any(
        |item| matches!(item, AgentItem::Error { detail, .. } if detail.contains("invalid JSON"))
    ));
    assert!(
        run.model_events
            .iter()
            .any(|event| matches!(event, AgentModelEvent::Error { detail, .. } if detail.contains("invalid JSON")))
    );
}

#[test]
fn agent_returns_error_after_model_repair_budget_is_exhausted() {
    let model = SequencedModel::new(["not json", "still not json"]);
    let config = AgentConfig::new("fake", "fake").with_max_model_repair_attempts(1);
    let agent = Agent::new(model, "fake", "fake").with_config(config);

    let error = agent.run(&agent_input("recover")).expect_err("parse error");

    match error {
        AgentError::ModelResponse { detail } => {
            assert!(detail.contains("attempt 2 of 2"));
            assert!(detail.contains("invalid JSON"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(agent.model.requests.lock().expect("requests").len(), 2);
}
