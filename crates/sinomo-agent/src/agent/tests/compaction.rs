use super::*;

#[test]
fn explicit_long_context_and_compaction_limits_follow_provider_options() {
    let budget = AgentContextBudget::for_loaded_model_with_limits(
        "codex",
        "gpt-5.6-sol",
        None,
        Some(1_000_000),
        Some(900_000),
    );

    // Codex's catalog caps this model at 872k, independently of the API provider.
    assert_eq!(budget.max_context_bytes, Some(872_000 * 4));
    assert_eq!(budget.compaction_threshold_bytes(), Some(872_000 * 4));

    let cost_aware = AgentContextBudget::for_loaded_model("codex", "gpt-5.6-sol", None);
    assert_eq!(
        cost_aware.max_context_bytes,
        Some(sinomo_lm::OPENAI_LONG_CONTEXT_PRICE_THRESHOLD_TOKENS * 4)
    );
}

#[test]
fn explicit_compaction_preserves_raw_history_and_records_working_state() {
    let model = SequencedModel::new(Vec::<String>::new());
    let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
        retained_tail_count: Some(1),
    });
    let session_id = SessionId::from_legacy_named("sess_agent_projection").expect("session");
    let mut session = AgentSession::new(session_id, AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "old request".to_string(),
        event_id: None,
    });
    session.transcript.push(AgentItem::AssistantText {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "old answer".to_string(),
    });
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 1,
        content: "current work request".to_string(),
        event_id: None,
    });

    let summary = agent
        .compact(&mut session, "trim old context")
        .expect("compact");

    assert_eq!(summary.projection.source_item_count, 3);
    assert_eq!(summary.projection.retained_item_ids.len(), 1);
    assert_eq!(
        summary
            .projection
            .working_state
            .current_user_request
            .as_deref(),
        Some("current work request")
    );
    assert_eq!(session.projection, Some(summary.projection.clone()));
    assert_eq!(session.raw_transcript.items.len(), 4);
    assert!(session.raw_transcript.items.iter().any(|item| {
        matches!(item, AgentItem::UserMessage { content, .. } if content == "old request")
    }));
    assert_eq!(session.transcript.items.len(), 2);
    assert!(
        session
            .transcript
            .items
            .first()
            .is_some_and(|item| matches!(item, AgentItem::CompactionSummary { .. }))
    );
}

#[test]
fn compaction_working_state_uses_latest_coding_validation_and_blockers() {
    let changed_files = vec!["src/lib.rs".to_string()];
    let items = vec![
        AgentItem::CodingRunState {
            item_id: TaskId::generate(),
            step_index: 1,
            state: CodingRunState {
                changed_files: changed_files.clone(),
                patch_status: CodingPatchStatus::Applied,
                validation_status: CodingValidationStatus::FailedEnvironment,
                blocked_reason: Some(CodingBlockedReason::Environment),
                open_risks: vec!["cargo was denied by the old sandbox policy".to_string()],
                ..CodingRunState::default()
            },
        },
        AgentItem::CodingRunState {
            item_id: TaskId::generate(),
            step_index: 2,
            state: CodingRunState {
                changed_files,
                patch_status: CodingPatchStatus::Applied,
                validation_status: CodingValidationStatus::Passed,
                outstanding_evidence: vec!["no patch has been produced yet".to_string()],
                stop_recommendation: CodingStopDecision::FinishWithPatch,
                ..CodingRunState::default()
            },
        },
    ];

    let state = working_state_from_items(&items, &[], &[], None);

    assert_eq!(state.verification_state, AgentVerificationState::Passed);
    assert!(state.blockers.is_empty());
    assert!(state.open_risks.is_empty());
    assert!(state.next_action.is_none());
}

#[test]
fn codex_style_compaction_uses_model_checkpoint_summary() {
    let model = SequencedModel::new(["## Current Progress\n- Summary from compaction model."]);
    let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
        retained_tail_count: Some(1),
    });
    let session_id = SessionId::from_legacy_named("sess_agent_model_compaction").expect("session");
    let mut session = AgentSession::new(session_id, AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "old objective with critical id abc-123".to_string(),
        event_id: None,
    });
    session.transcript.push(AgentItem::AssistantText {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "working notes".to_string(),
    });

    let summary = agent
        .compact(&mut session, "checkpoint old context")
        .expect("compact");

    assert_eq!(
        summary.summary,
        "## Current Progress\n- Summary from compaction model."
    );
    assert_eq!(
        summary.strategy_id,
        crate::DEFAULT_AGENT_COMPACTION_STRATEGY_ID
    );
    assert_eq!(summary.trigger, AgentCompactionTrigger::Manual);
    assert_eq!(
        summary.summary_model,
        Some(AgentCompactionModelRef {
            provider: "fake".to_string(),
            model: "fake".to_string(),
        })
    );
    let requests = agent.model.requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].response_format,
        LanguageModelResponseFormat::Text
    );
    assert_eq!(
        requests[0]
            .metadata
            .get("loop")
            .and_then(serde_json::Value::as_str),
        Some("agent_compaction")
    );
    let request_text = requests[0]
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(request_text.contains("CONTEXT CHECKPOINT COMPACTION"));
    assert!(request_text.contains("abc-123"));
}

#[test]
fn compaction_engine_can_be_replaced_online() {
    struct ReplacementEngine;

    impl<M: InteractionModel> AgentCompactionEngine<M> for ReplacementEngine {
        fn compact(
            &self,
            _model: &M,
            request: AgentCompactionRequest<'_>,
        ) -> Result<AgentCompactionSummary, AgentError> {
            Ok(request
                .projection_builder
                .compact_retaining_tail_count_with_options(
                    request.source_transcript,
                    request.reason,
                    request.token_estimate_before,
                    request.retained_tail_count,
                    request.previous_projection,
                    request.steering,
                    request.follow_ups,
                    AgentCompactionBuildOptions {
                        strategy_id: "test.replacement.compaction".to_string(),
                        trigger: request.trigger,
                        summary_override: Some("replacement engine summary".to_string()),
                        summary_model: None,
                        fallback_reason: None,
                    },
                ))
        }
    }

    let model = SequencedModel::new(Vec::<String>::new());
    let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
        retained_tail_count: Some(1),
    });
    agent
        .replace_compaction_engine(ReplacementEngine)
        .expect("replace engine");
    let session_id =
        SessionId::from_legacy_named("sess_agent_replace_compaction").expect("session");
    let mut session = AgentSession::new(session_id, AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "replace this compaction implementation".to_string(),
        event_id: None,
    });

    let summary = agent.compact(&mut session, "online swap").expect("compact");

    assert_eq!(summary.strategy_id, "test.replacement.compaction");
    assert_eq!(summary.summary, "replacement engine summary");
    assert!(agent.model.requests.lock().expect("requests").is_empty());
}

#[test]
fn compaction_carries_prior_projection_working_state_forward() {
    let compactor = AgentCompactor {
        retained_tail_count: Some(1),
    };
    let mut first_transcript = AgentTranscript::new();
    first_transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "long running objective".to_string(),
        event_id: None,
    });
    let first = compactor.compact_retaining_tail_count_with_session_state(
        &first_transcript,
        "first compaction",
        100,
        1,
        None,
        &[],
        &[],
    );
    let mut second_transcript = AgentTranscript::new();
    second_transcript.push(AgentItem::AssistantText {
        item_id: TaskId::generate(),
        step_index: 1,
        content: "still working".to_string(),
    });

    let second = compactor.compact_retaining_tail_count_with_session_state(
        &second_transcript,
        "second compaction",
        100,
        1,
        Some(&first.projection),
        &[],
        &[],
    );

    assert_eq!(
        second.projection.previous_projection_id,
        Some(first.projection.projection_id.clone())
    );
    assert_eq!(
        second.projection.working_state.active_objective.as_deref(),
        Some("long running objective")
    );
    assert_eq!(
        second
            .projection
            .working_state
            .current_user_request
            .as_deref(),
        Some("long running objective")
    );
    assert!(
        second
            .projection
            .working_state
            .decisions
            .contains(&"still working".to_string())
    );
}

#[test]
fn automatic_compaction_prunes_model_context_before_sampling() {
    let old_context = format!("OLD_CONTEXT_MARKER:{}", "x".repeat(8 * 1024));
    let model = SequencedModel::new([
        "Checkpoint summary for old context.",
        r#"{
            "response_mode": "answer",
            "text": "First compacted answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
        "Checkpoint summary for follow-up context.",
        r#"{
            "response_mode": "answer",
            "text": "Second compacted answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let config = AgentConfig::new("fake", "fake").with_context_budget(AgentContextBudget {
        max_context_bytes: Some(4 * 1024),
        compaction_threshold_ratio: Some(0.5),
        retained_tail_items: None,
    });
    let agent = Agent::new(model, "fake", "fake")
        .with_config(config)
        .with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        });
    let input = agent_input("new request");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: old_context,
        event_id: None,
    });
    session.transcript.push(AgentItem::AssistantText {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "OLD_ASSISTANT_MARKER".to_string(),
    });

    let first_run = agent.start_turn(&mut session, &input).expect("first run");
    let second_input = agent_input("follow up");
    let second_run = agent
        .continue_turn(&mut session, &second_input)
        .expect("second run");

    assert_eq!(first_run.compactions.len(), 1);
    assert_eq!(second_run.final_decision.text, "Second compacted answer.");
    let requests = agent.model.requests.lock().expect("requests");
    assert_eq!(requests.len(), 4);
    let compaction_requests = requests
        .iter()
        .filter(|request| {
            request
                .metadata
                .get("loop")
                .and_then(serde_json::Value::as_str)
                == Some("agent_compaction")
        })
        .collect::<Vec<_>>();
    assert_eq!(compaction_requests.len(), 2);
    assert_eq!(
        compaction_requests[0].response_format,
        LanguageModelResponseFormat::Text
    );
    let compaction_source = serde_json::to_string(compaction_requests[0]).expect("source");
    assert!(compaction_source.contains("OLD_CONTEXT_MARKER"));
    assert!(compaction_source.contains("OLD_ASSISTANT_MARKER"));
    let agent_requests = requests
        .iter()
        .filter(|request| {
            request
                .metadata
                .get("loop")
                .and_then(serde_json::Value::as_str)
                == Some("agent")
        })
        .collect::<Vec<_>>();
    assert_eq!(agent_requests.len(), 2);
    for request in agent_requests {
        let request_text = request
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        // Working-state summaries may preserve earlier facts and
        // identifiers; the unabridged old payload must leave context.
        assert!(!request_text.contains(&"x".repeat(8 * 1024)));
        assert!(request_text.contains("prior_compaction_summary"));
    }
}

#[test]
fn agent_session_api_supports_steer_compact_and_abort() {
    let model = SequencedModel::new([r#"{
        "response_mode": "answer",
        "text": "Done.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    }"#]);
    let agent = Agent::new(model, "fake", "fake");
    let input = agent_input("hello");
    let mut session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

    let run = agent.start_turn(&mut session, &input).expect("run");
    assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
    assert!(!session.transcript.items.is_empty());

    let steering = agent
        .steer(&mut session, "prefer a concise answer")
        .expect("steer");
    assert_eq!(session.steering_queue[0], steering);

    let compaction = agent
        .compact(&mut session, "test compaction")
        .expect("compact");
    assert_eq!(compaction.reason, "test compaction");
    assert!(
        session
            .transcript
            .items
            .iter()
            .any(|item| matches!(item, AgentItem::CompactionSummary { .. }))
    );

    let abort = agent
        .abort(&mut session, "no longer needed")
        .expect("abort");
    assert_eq!(session.abort, Some(abort));
    let aborted_run = agent.continue_turn(&mut session, &input).expect("aborted");
    assert_eq!(aborted_run.stop_reason, AgentStopReason::Aborted);
}
