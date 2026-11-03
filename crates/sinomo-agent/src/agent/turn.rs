//! Model/tool turn execution and progress retained across execution failures.

use super::{
    Agent, AgentCancellationToken, AgentCompactionSummary, AgentCompactionTrigger, AgentError,
    AgentFollowUpMessage, AgentItem, AgentLoopDecision, AgentLoopInput, AgentModelEvent,
    AgentModelProgressSink, AgentModelTurn, AgentRun, AgentRunModelMetrics, AgentSessionProjection,
    AgentSteeringMessage, AgentStopReason, AgentToolOrchestrator, AgentToolProgressSink,
    AgentToolStatus, AgentTraceRecordKind, AgentTranscript, AgentTranscriptItem,
    AsyncTaskNextAction, CodingRunState, ContextFrame, InteractionModel, TaskId, agent_event,
    apply_context_parent, build_candidate_evidence_bundle, cancelled_tool_outcome,
    caused_by_latest_event, coding_final_decision_from_state, coding_state_from_items,
    coding_state_should_stop_loop, compacted_transcript_view, compaction_target_transcript_bytes,
    latest_compacted_projection_items, model_repair_parse_error_detail,
    model_visible_assistant_content, native_tool_call_items_from_decision,
    native_tool_output_items_from_outcomes, parse_agent_model_decision,
    patch_snapshot_item_from_tool_outcome, project_agent_item_to_transcript_item,
    project_session_transcript, render_initial_agent_input, render_queued_turn_context,
    render_tool_results, repair_request_for_model_response,
    retained_transcript_tail_for_compaction, transient_system_context,
    verification_item_from_tool_outcome,
};

/// Durable items produced by a turn, including those produced before an error.
/// Keeping this outside the fallible loop prevents an early return from
/// discarding receipts for tools that have already executed.
#[derive(Default)]
pub(super) struct AgentTurnProgress {
    pub(super) items: Vec<AgentItem>,
    pub(super) compactions: Vec<AgentCompactionSummary>,
    pub(super) step_index: usize,
}

impl<M: InteractionModel> Agent<M> {
    pub fn run(&self, input: &AgentLoopInput) -> Result<AgentRun, AgentError> {
        self.run_with_session_context(input, &AgentTranscript::new(), None, &[], &[])
    }

    pub(super) fn run_with_session_context(
        &self,
        input: &AgentLoopInput,
        source_transcript: &AgentTranscript,
        session_projection: Option<AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
    ) -> Result<AgentRun, AgentError> {
        self.run_with_session_context_and_cancellation(
            input,
            source_transcript,
            session_projection,
            steering,
            follow_ups,
            AgentCancellationToken::new(),
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
            &mut AgentTurnProgress::default(),
        )
    }

    pub(super) fn run_with_session_context_and_cancellation(
        &self,
        input: &AgentLoopInput,
        source_transcript: &AgentTranscript,
        mut session_projection: Option<AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
        progress: &mut AgentTurnProgress,
    ) -> Result<AgentRun, AgentError> {
        let _usage_scope = sinomo_lm::usage::enter_attribution(self.usage_attribution(input));
        if let Some(subagents) = self.subagents.get() {
            subagents.set_input(input);
        }
        if let Some(update) = &input.async_update
            && !update.needs_user_attention
            && update.next_action_hint == AsyncTaskNextAction::ContinueSilently
        {
            let item = AgentItem::AsyncTaskUpdate {
                item_id: TaskId::generate(),
                update: update.clone(),
            };
            let event = caused_by_latest_event(
                agent_event(
                    &input.session,
                    "agent.loop.completed",
                    0.48,
                    serde_json::json!({
                        "stop_reason": AgentStopReason::SilentAsyncUpdate,
                        "silent_async_update": input.async_update,
                        "run_context": &self.config.run_context,
                    }),
                ),
                input,
            );
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item { item: item.clone() },
            )?;
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Event {
                    event: event.clone(),
                },
            )?;
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::RunCompleted {
                    stop_reason: AgentStopReason::SilentAsyncUpdate,
                    final_decision: AgentLoopDecision::silent(),
                },
            )?;
            return Ok(AgentRun {
                final_decision: AgentLoopDecision::silent(),
                stop_reason: AgentStopReason::SilentAsyncUpdate,
                model_metrics: AgentRunModelMetrics::default(),
                run_context: self.config.run_context.clone(),
                transcript: Vec::new(),
                model_turns: Vec::new(),
                tool_results: Vec::new(),
                items: vec![item],
                context_reports: Vec::new(),
                model_events: Vec::new(),
                compactions: Vec::new(),
                events: vec![event],
                coding_state: None,
                candidate_evidence_bundle: None,
            });
        }

        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::RunStarted {
                provider: self.config.provider.clone(),
                model: self.config.model.clone(),
                role: self.config.definition.role.clone(),
                surface_id: self.tool_surface.surface_id.clone(),
                max_steps: self.config.max_steps,
                run_context: self.config.run_context.clone(),
            },
        )?;
        let mut transcript = project_session_transcript(source_transcript, None)?;
        let mut queued_agent_item = None;
        let queued_turn_context = render_queued_turn_context(steering, follow_ups)
            .map(|content| AgentTranscriptItem::user(0, content));
        if let Some(item) = queued_turn_context {
            let queued_item = AgentItem::UserMessage {
                item_id: TaskId::generate(),
                step_index: 0,
                content: item.content.clone(),
                event_id: None,
            };
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item {
                    item: queued_item.clone(),
                },
            )?;
            queued_agent_item = Some(queued_item);
            transcript.push(item);
        }
        let items = &mut progress.items;
        items.extend(queued_agent_item);
        // A continuation records the update it continues with, so the
        // session's activity shows when detached work completed.
        if let Some(update) = &input.async_update {
            let update_item = AgentItem::AsyncTaskUpdate {
                item_id: TaskId::generate(),
                update: update.clone(),
            };
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item {
                    item: update_item.clone(),
                },
            )?;
            items.push(update_item);
        }
        if transient_system_context(input).is_none() {
            let initial_input = render_initial_agent_input(input)?;
            transcript.push(AgentTranscriptItem::user(0, initial_input.clone()));
            let initial_item = AgentItem::UserMessage {
                item_id: input
                    .latest_event
                    .payload
                    .get("agent_item_id")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| value.parse::<TaskId>().ok())
                    .unwrap_or_else(TaskId::generate),
                step_index: 0,
                content: initial_input,
                event_id: Some(input.latest_event.event_id.clone()),
            };
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item {
                    item: initial_item.clone(),
                },
            )?;
            items.push(initial_item);
        }
        let mut model_turns = Vec::new();
        let mut tool_results = Vec::new();
        let mut context_reports = Vec::new();
        let mut model_events = Vec::new();
        let compactions = &mut progress.compactions;
        let start_event = caused_by_latest_event(
            agent_event(
                &input.session,
                "agent.loop.started",
                0.54,
                serde_json::json!({
                    "provider": self.config.provider,
                    "model": self.config.model,
                    "role": self.config.definition.role,
                    "agent_tool_surface_id": self.tool_surface.surface_id.clone(),
                    "max_steps": self.config.max_steps,
                    "max_tool_calls_per_step": self.config.max_tool_calls_per_step,
                    "run_context": &self.config.run_context,
                }),
            ),
            input,
        );
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::Event {
                event: start_event.clone(),
            },
        )?;
        let mut events = vec![start_event];
        let mut final_decision = AgentLoopDecision::silent();
        let mut stop_reason = AgentStopReason::MaxStepsReached;
        let mut step_index = 0usize;
        let mut latest_coding_state: Option<CodingRunState> = None;
        let mut repair_request_model_visible_bytes = 0usize;
        let mut previous_context_frame: Option<ContextFrame> = None;

        'turn_loop: loop {
            progress.step_index = step_index;
            if cancellation_token.is_cancelled() {
                stop_reason = AgentStopReason::Cancelled;
                let event = AgentModelEvent::Aborted {
                    reason: "turn cancellation requested".to_string(),
                };
                model_progress_sink.emit(event.clone());
                model_events.push(event);
                break;
            }
            // Pause only takes effect here, at a model-request boundary: the
            // previous step's tool calls have finished and been recorded.
            if cancellation_token.is_paused() {
                stop_reason = AgentStopReason::Paused;
                let event = AgentModelEvent::Paused {
                    reason: "session paused before the next model request".to_string(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::ModelEvent {
                        event: event.clone(),
                    },
                )?;
                model_progress_sink.emit(event.clone());
                model_events.push(event);
                break;
            }
            if self
                .config
                .max_steps
                .is_some_and(|max_steps| step_index >= max_steps)
            {
                break;
            }
            let mut turn_context = self.assemble_context_with_projection(
                input,
                &transcript,
                session_projection.as_ref(),
                step_index,
            )?;
            if self
                .config
                .context_budget
                .compaction_threshold_bytes()
                .is_some_and(|threshold_bytes| {
                    turn_context.budget_estimate.total_bytes > threshold_bytes
                })
            {
                let target_transcript_bytes =
                    compaction_target_transcript_bytes(&turn_context.budget_estimate);
                let retained_transcript = retained_transcript_tail_for_compaction(
                    &transcript,
                    self.compactor.retained_tail_count,
                    target_transcript_bytes,
                );
                if retained_transcript.len() < transcript.len() {
                    let mut typed_transcript = source_transcript.clone();
                    typed_transcript.items.extend(items.iter().cloned());
                    typed_transcript.items =
                        latest_compacted_projection_items(&typed_transcript.items);
                    // The model-visible tail is counted in view items; the
                    // stored projection retains the typed items behind them.
                    let retained_tail_count = self
                        .compactor
                        .retained_tail_count
                        .unwrap_or_else(|| {
                            super::typed_tail_count_for_view_items(
                                &typed_transcript.items,
                                retained_transcript.len(),
                            )
                        })
                        .min(typed_transcript.items.len());
                    let compaction = self.compact_source_transcript(
                        &input.session.session_id,
                        &typed_transcript,
                        "context budget threshold reached".to_string(),
                        turn_context.budget_estimate.total_bytes,
                        retained_tail_count,
                        session_projection.as_ref(),
                        steering,
                        follow_ups,
                        AgentCompactionTrigger::PreSamplingBudget,
                        &cancellation_token,
                    )?;
                    let compaction_item = AgentItem::CompactionSummary {
                        item_id: TaskId::generate(),
                        summary: compaction.clone(),
                    };
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Compaction {
                            summary: compaction.clone(),
                        },
                    )?;
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Item {
                            item: compaction_item.clone(),
                        },
                    )?;
                    items.push(compaction_item);
                    let compaction_event = caused_by_latest_event(
                        agent_event(
                            &input.session,
                            "agent.compaction.completed",
                            0.57,
                            serde_json::json!({
                                "step_index": step_index,
                                "compaction": compaction,
                            }),
                        ),
                        input,
                    );
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Event {
                            event: compaction_event.clone(),
                        },
                    )?;
                    events.push(compaction_event);
                    session_projection = Some(compaction.projection.clone());
                    transcript = compacted_transcript_view(&compaction, retained_transcript)?;
                    turn_context = self.assemble_context_with_projection(
                        input,
                        &transcript,
                        session_projection.as_ref(),
                        step_index,
                    )?;
                    compactions.push(compaction);
                }
            }
            apply_context_parent(&mut turn_context, previous_context_frame.as_ref());
            context_reports.push(turn_context.report.clone());
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::ContextReport {
                    report: turn_context.report.clone(),
                },
            )?;
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::ContextFrame {
                    frame: turn_context.context_frame.clone(),
                    delta: turn_context.context_delta.clone(),
                },
            )?;
            model_progress_sink.emit(AgentModelEvent::ContextAssembled {
                step_index,
                report: turn_context.report.clone(),
            });
            let context_item = AgentItem::ContextInjection {
                item_id: TaskId::generate(),
                report: turn_context.report.clone(),
            };
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item {
                    item: context_item.clone(),
                },
            )?;
            items.push(context_item);
            previous_context_frame = Some(turn_context.context_frame.clone());
            let context_event = caused_by_latest_event(
                agent_event(
                    &input.session,
                    "agent.context.assembled",
                    0.53,
                    serde_json::json!({
                        "step_index": step_index,
                        "report": turn_context.report.clone(),
                        "run_context": &turn_context.run_context,
                    }),
                ),
                input,
            );
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Event {
                    event: context_event.clone(),
                },
            )?;
            events.push(context_event);
            let mut request = self.request_from_context(input, &turn_context, step_index)?;
            let mut repair_attempt = 0;
            let (request, response, decision, decision_source, step_model_events) = loop {
                let sampled_request = request.clone();
                let mut language_model_stream_events = Vec::new();
                let response = match self.model.complete_streaming_events_with_cancellation(
                    &sampled_request,
                    &cancellation_token,
                    &mut |event| {
                        if let Some(model_event) = AgentModelEvent::from_stream_event(
                            step_index,
                            &sampled_request.provider,
                            &sampled_request.model,
                            &event,
                        ) {
                            model_progress_sink.emit(model_event);
                        }
                        language_model_stream_events.push(event);
                    },
                ) {
                    Ok(response) => response,
                    Err(error) if cancellation_token.is_cancelled() => {
                        stop_reason = AgentStopReason::Cancelled;
                        let event = AgentModelEvent::Aborted {
                            reason: format!(
                                "turn cancellation requested during model response: {error}"
                            ),
                        };
                        self.trace(
                            &input.session.session_id,
                            AgentTraceRecordKind::ModelEvent {
                                event: event.clone(),
                            },
                        )?;
                        model_progress_sink.emit(event.clone());
                        model_events.push(event);
                        break 'turn_loop;
                    }
                    Err(error) => return Err(AgentError::Model(error)),
                };
                let assistant_item = AgentItem::AssistantText {
                    item_id: TaskId::generate(),
                    step_index,
                    content: response.content.clone(),
                    native_items: response.continuation_items(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: assistant_item.clone(),
                    },
                )?;
                items.push(assistant_item);
                match parse_agent_model_decision(
                    &response,
                    &language_model_stream_events,
                    &turn_context.available_tool_specs,
                    &self.config.definition.output_format,
                ) {
                    Ok(parsed_decision) => {
                        let step_model_events = AgentModelEvent::from_model_response(
                            step_index,
                            &response,
                            &parsed_decision.decision,
                            language_model_stream_events,
                        );
                        break (
                            sampled_request,
                            response,
                            parsed_decision.decision,
                            parsed_decision.source,
                            step_model_events,
                        );
                    }
                    Err(error) => {
                        let detail = model_repair_parse_error_detail(
                            repair_attempt,
                            self.config.max_model_repair_attempts,
                            &error,
                        );
                        let step_model_events = AgentModelEvent::from_unparsed_model_response(
                            step_index,
                            &response,
                            language_model_stream_events,
                            detail.clone(),
                        );
                        for event in &step_model_events {
                            self.trace(
                                &input.session.session_id,
                                AgentTraceRecordKind::ModelEvent {
                                    event: event.clone(),
                                },
                            )?;
                        }
                        model_events.extend(step_model_events);
                        let error_item = AgentItem::Error {
                            item_id: TaskId::generate(),
                            step_index,
                            detail: detail.clone(),
                        };
                        self.trace(
                            &input.session.session_id,
                            AgentTraceRecordKind::Item {
                                item: error_item.clone(),
                            },
                        )?;
                        items.push(error_item);
                        if self
                            .config
                            .max_model_repair_attempts
                            .is_some_and(|max_attempts| repair_attempt >= max_attempts)
                        {
                            return Err(AgentError::ModelResponse { detail });
                        }
                        repair_attempt += 1;
                        // A repair request is another model request.
                        if cancellation_token.is_paused() {
                            stop_reason = AgentStopReason::Paused;
                            let event = AgentModelEvent::Paused {
                                reason: "session paused before a repair request".to_string(),
                            };
                            model_progress_sink.emit(event.clone());
                            model_events.push(event);
                            break 'turn_loop;
                        }
                        request = repair_request_for_model_response(
                            &sampled_request,
                            &response.content,
                            &detail,
                            repair_attempt,
                        )?;
                        repair_request_model_visible_bytes = repair_request_model_visible_bytes
                            .saturating_add(
                                request
                                    .messages
                                    .last()
                                    .map(|message| message.content.len())
                                    .unwrap_or_default(),
                            );
                    }
                }
            };
            if cancellation_token.is_cancelled() {
                stop_reason = AgentStopReason::Cancelled;
                let event = AgentModelEvent::Aborted {
                    reason: "turn cancellation requested after model response".to_string(),
                };
                model_progress_sink.emit(event.clone());
                model_events.push(event);
                break;
            }
            transcript.push(
                AgentTranscriptItem::assistant(
                    step_index,
                    model_visible_assistant_content(&response.content),
                )
                .with_native_items({
                    let mut native_items = response.continuation_items();
                    native_items.extend(native_tool_call_items_from_decision(&decision));
                    native_items
                }),
            );
            for event in &step_model_events {
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::ModelEvent {
                        event: event.clone(),
                    },
                )?;
            }
            model_events.extend(step_model_events.iter().cloned());
            if response.usage.is_some() {
                let usage_item = AgentItem::ModelUsage {
                    item_id: TaskId::generate(),
                    step_index,
                    provider: response.provider.clone(),
                    model: response.model.clone(),
                    usage: response.usage.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: usage_item.clone(),
                    },
                )?;
                items.push(usage_item);
            }
            let model_event = caused_by_latest_event(
                agent_event(
                    &input.session,
                    "agent.loop.model.completed",
                    0.58,
                    serde_json::json!({
                        "step_index": step_index,
                        "response_mode": decision.response_mode,
                        "decision_source": &decision_source,
                        "tool_call_count": decision.tool_calls.len(),
                        "next_state": decision.next_state,
                    }),
                ),
                input,
            );
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Event {
                    event: model_event.clone(),
                },
            )?;
            events.push(model_event);
            model_turns.push(AgentModelTurn {
                step_index,
                request,
                response,
                decision: decision.clone(),
                decision_source,
                events: step_model_events,
            });
            final_decision = decision.clone();

            if decision.tool_calls.is_empty() {
                stop_reason = AgentStopReason::FinalDecision;
                break;
            }

            let mut step_results = Vec::new();
            let tool_plan = self.tool_plan_with_discovery(input, &transcript);
            let tool_orchestrator = AgentToolOrchestrator::new(
                &self.tool_surface,
                self.config.max_tool_calls_per_step,
                &self.config.run_context,
            )
            .with_skill_plan(self.skill_plan());
            for (call_index, call) in decision.tool_calls.iter().enumerate() {
                let tool_call_id = TaskId::generate();
                let tool_call_item = AgentItem::ToolCall {
                    item_id: TaskId::generate(),
                    tool_call_id: tool_call_id.clone(),
                    step_index,
                    call_index,
                    call: call.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: tool_call_item.clone(),
                    },
                )?;
                items.push(tool_call_item);
                let call_progress =
                    progress_sink.for_tool_call(tool_call_id.clone(), step_index, call_index, call);
                let tool_started = std::time::Instant::now();
                let detached = (!cancellation_token.is_cancelled())
                    .then(|| {
                        tool_orchestrator.detach(
                            &self.tools,
                            &input.session,
                            &tool_plan,
                            step_index,
                            call_index,
                            call,
                            call_progress.clone(),
                        )
                    })
                    .flatten();
                let was_detached = detached.is_some();
                let cancelled_before_execution = cancellation_token.is_cancelled();
                let mut outcome = if cancelled_before_execution {
                    cancelled_tool_outcome(
                        step_index,
                        call_index,
                        call,
                        "turn cancellation requested before tool execution",
                    )
                } else if let Some(detached) = detached {
                    detached
                } else {
                    tool_orchestrator.execute_with_cancellation_and_progress(
                        &self.tools,
                        &input.session,
                        &tool_plan,
                        step_index,
                        call_index,
                        call,
                        cancellation_token.clone(),
                        call_progress,
                    )
                };
                // Every outcome, including ones decided before execution,
                // pairs with the recorded call.
                outcome.call_id = tool_call_id;
                if cancelled_before_execution {
                    self.tools
                        .record_usage(&input.session, &outcome.call_id, &outcome, 0, false);
                }
                let event = caused_by_latest_event(
                    agent_event(
                        &input.session,
                        if was_detached {
                            "agent.tool.call.detached"
                        } else {
                            "agent.tool.call.completed"
                        },
                        0.62,
                        serde_json::json!({
                            "call_id": outcome.call_id,
                            "duration_ms": tool_started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                            "detached": was_detached,
                            "runtime": "agent",
                            "node": input.session.node_id,
                            "step_index": outcome.step_index,
                            "call_index": outcome.call_index,
                            "tool": outcome.call.tool,
                            "status": outcome.status,
                            "summary": outcome.summary,
                        }),
                    ),
                    input,
                );
                outcome.event_id = Some(event.event_id.clone());
                let tool_result_item = AgentItem::ToolResult {
                    item_id: TaskId::generate(),
                    step_index: outcome.step_index,
                    call_index: outcome.call_index,
                    call_id: outcome.call_id.clone(),
                    native_call_id: outcome.call.call_id.clone(),
                    tool_name: Some(outcome.call.tool.clone()),
                    status: outcome.status.clone(),
                    summary: outcome.summary.clone(),
                    output: outcome.output.clone(),
                    evidence: outcome.evidence.clone(),
                };
                items.push(tool_result_item.clone());
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::ToolOutcome {
                        outcome: outcome.clone(),
                    },
                )?;
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Event {
                        event: event.clone(),
                    },
                )?;
                events.push(event);
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: tool_result_item.clone(),
                    },
                )?;

                if let Some(patch_snapshot_item) = patch_snapshot_item_from_tool_outcome(&outcome) {
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Item {
                            item: patch_snapshot_item.clone(),
                        },
                    )?;
                    items.push(patch_snapshot_item);
                }
                if let Some(verification_item) = verification_item_from_tool_outcome(&outcome) {
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Item {
                            item: verification_item.clone(),
                        },
                    )?;
                    items.push(verification_item);
                }
                step_results.push(outcome.clone());
                if outcome.status == AgentToolStatus::Cancelled {
                    stop_reason = AgentStopReason::Cancelled;
                }
                tool_results.push(outcome);
            }
            let tool_message_id = step_results
                .first()
                .map(|result| result.call_id.clone())
                .unwrap_or_else(TaskId::generate);
            transcript.push(
                AgentTranscriptItem::tool(
                    step_index,
                    tool_message_id,
                    render_tool_results(step_index, &step_results)?,
                )
                .with_native_items(native_tool_output_items_from_outcomes(&step_results)?),
            );
            let coding_state = coding_state_from_items(items);
            if !coding_state.is_empty() {
                let coding_state_item = AgentItem::CodingRunState {
                    item_id: TaskId::generate(),
                    step_index,
                    state: coding_state.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: coding_state_item.clone(),
                    },
                )?;
                if let Some(transcript_item) =
                    project_agent_item_to_transcript_item(coding_state_item.clone()).transpose()?
                {
                    transcript.push(transcript_item);
                }
                items.push(coding_state_item);
                let coding_state_event = caused_by_latest_event(
                    agent_event(
                        &input.session,
                        "agent.coding.state.updated",
                        0.59,
                        serde_json::json!({
                            "step_index": step_index,
                            "state": coding_state.clone(),
                        }),
                    ),
                    input,
                );
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Event {
                        event: coding_state_event.clone(),
                    },
                )?;
                events.push(coding_state_event);
                if coding_state_should_stop_loop(&coding_state) {
                    final_decision = coding_final_decision_from_state(&coding_state);
                    stop_reason = AgentStopReason::CodingStopRecommended;
                    latest_coding_state = Some(coding_state);
                    break;
                }
                latest_coding_state = Some(coding_state);
            }
            if stop_reason == AgentStopReason::Cancelled {
                break;
            }
            step_index = step_index.saturating_add(1);
        }

        let model_metrics = AgentRunModelMetrics::from_turns(
            self.config.definition.output_format.clone(),
            &model_turns,
            &model_events,
            repair_request_model_visible_bytes,
        );
        let completed_event = caused_by_latest_event(
            agent_event(
                &input.session,
                "agent.loop.completed",
                0.56,
                serde_json::json!({
                    "stop_reason": stop_reason,
                    "model_turn_count": model_turns.len(),
                    "tool_result_count": tool_results.len(),
                    "model_metrics": &model_metrics,
                    "final_response_mode": final_decision.response_mode,
                    "run_context": &self.config.run_context,
                    "coding_state": &latest_coding_state,
                }),
            ),
            input,
        );
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::Event {
                event: completed_event.clone(),
            },
        )?;
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::RunCompleted {
                stop_reason: stop_reason.clone(),
                final_decision: final_decision.clone(),
            },
        )?;
        events.push(completed_event);

        let candidate_evidence_bundle =
            build_candidate_evidence_bundle(&input.context_attachments, &final_decision, items);

        Ok(AgentRun {
            final_decision,
            stop_reason,
            model_metrics,
            run_context: self.config.run_context.clone(),
            transcript,
            model_turns,
            tool_results,
            items: std::mem::take(items),
            context_reports,
            model_events,
            compactions: std::mem::take(compactions),
            events,
            coding_state: latest_coding_state,
            candidate_evidence_bundle,
        })
    }
}
