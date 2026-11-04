//! Evolution's projection over the host's authoritative execution journal.
use crate::*;
use sinomo_module_api::sessions::*;

pub(crate) fn execution_state(
    sessions: SessionServices<'_>,
    reference: &SessionExecutionRef,
) -> Result<SessionExecutionState, String> {
    let sessions = sessions.ok_or("host session service is unavailable")?;
    // Follow continuations through bounded execution metadata, not full transcripts.
    let state: SessionExecutionState =
        serde_json::from_value(sessions.request(&SessionRequest::Execution {
            execution: reference.clone(),
        })?)
        .map_err(|e| e.to_string())?;
    if let Some(operation_id) = &state.current_operation_id
        && operation_id != &reference.operation_id
    {
        return serde_json::from_value(sessions.request(&SessionRequest::Execution {
            execution: SessionExecutionRef {
                session_id: reference.session_id.clone(),
                operation_id: operation_id.clone(),
            },
        })?)
        .map_err(|e| e.to_string());
    }
    Ok(state)
}

pub(crate) fn apply_execution_state(
    run: &mut EvolutionAgentRunRecord,
    state: SessionExecutionState,
) -> Result<(), String> {
    if state.agent_id.as_deref() != Some(run.agent_id.as_str()) {
        return Err("execution agent identity does not match the workflow assignment".into());
    }
    run.execution = Some(state.execution);
    run.status = match state.phase {
        SessionExecutionPhase::Queued => EvolutionAgentRunStatus::Queued,
        SessionExecutionPhase::Running => EvolutionAgentRunStatus::Running,
        SessionExecutionPhase::Paused => EvolutionAgentRunStatus::Waiting,
        SessionExecutionPhase::Completed => EvolutionAgentRunStatus::Complete,
        SessionExecutionPhase::Failed => EvolutionAgentRunStatus::Failed,
        SessionExecutionPhase::Cancelled => EvolutionAgentRunStatus::Cancelled,
    };
    if let Some(started) = state.started_at {
        run.started_at = started;
    }
    run.finished_at = state.finished_at;
    run.execution_detail = state.error.or_else(|| {
        (state.cancel_requested && run.status == EvolutionAgentRunStatus::Running)
            .then(|| "Cancellation requested; execution is stopping".into())
    });
    Ok(())
}

pub(crate) fn refresh_execution_states(
    graph: &mut EvolutionGoalGraph,
    sessions: SessionServices<'_>,
) -> Vec<String> {
    let mut errors = vec![];
    for run in &mut graph.agent_runs {
        if run.execution.is_none()
            && !matches!(
                run.status,
                EvolutionAgentRunStatus::Queued | EvolutionAgentRunStatus::Cancelled
            )
        {
            run.status = EvolutionAgentRunStatus::Unavailable;
            run.execution_detail = Some("No runtime execution reference is available".into());
        }
        if let Some(reference) = &run.execution {
            let result = execution_state(sessions, reference)
                .and_then(|state| apply_execution_state(run, state));
            if let Err(error) = result {
                run.status = EvolutionAgentRunStatus::Unavailable;
                run.execution_detail = Some(error.clone());
                errors.push(format!("{}: {error}", run.agent_run_id));
            }
        }
    }
    for task in &mut graph.tasks {
        if let Some(run) = task
            .assigned_agent_run_id
            .as_ref()
            .and_then(|id| graph.agent_runs.iter().find(|run| &run.agent_run_id == id))
        {
            task.status = match run.status {
                EvolutionAgentRunStatus::Queued if graph.status == EvolutionGoalStatus::Paused => {
                    EvolutionTaskStatus::AwaitingDecision
                }
                EvolutionAgentRunStatus::Queued => EvolutionTaskStatus::Ready,
                EvolutionAgentRunStatus::Running => EvolutionTaskStatus::Running,
                EvolutionAgentRunStatus::Waiting | EvolutionAgentRunStatus::Unavailable => {
                    EvolutionTaskStatus::AwaitingDecision
                }
                EvolutionAgentRunStatus::Complete => EvolutionTaskStatus::Complete,
                EvolutionAgentRunStatus::Failed => EvolutionTaskStatus::Failed,
                EvolutionAgentRunStatus::Cancelled => EvolutionTaskStatus::Cancelled,
            };
        }
    }
    errors
}

pub(crate) fn control_executions(
    graph: &EvolutionGoalGraph,
    sessions: SessionServices<'_>,
    action: &sinomo_runtime::api::app::evolution::EvolutionGoalControlAction,
) -> Vec<String> {
    use sinomo_runtime::api::app::evolution::EvolutionGoalControlAction as Action;
    let mut seen = std::collections::BTreeSet::new();
    let mut errors = vec![];
    for reference in graph
        .agent_runs
        .iter()
        .filter_map(|run| run.execution.as_ref())
    {
        if !seen.insert(reference.session_id.clone()) {
            continue;
        }
        let result = (|| {
            let state = execution_state(sessions, reference)?;
            if matches!(action, Action::Pause | Action::Resume)
                && matches!(
                    state.phase,
                    SessionExecutionPhase::Completed
                        | SessionExecutionPhase::Failed
                        | SessionExecutionPhase::Cancelled
                )
            {
                return Ok(());
            }
            let id = reference.session_id.clone();
            let command = match action {
                Action::Pause => SessionCommand::Pause { id },
                Action::Resume => SessionCommand::Resume { id },
                Action::Cancel | Action::Complete => SessionCommand::Archive { id },
            };
            sessions
                .ok_or("host session service is unavailable")?
                .request(&SessionRequest::Command { command })?;
            Ok::<_, String>(())
        })();
        if let Err(error) = result {
            errors.push(format!("{}: {error}", reference.session_id));
        }
    }
    errors
}

pub(crate) fn admit_candidate(
    paths: &EvolutionPaths,
    packet: &MutationPacket,
    reference: &SessionExecutionRef,
    sessions: SessionServices<'_>,
) -> Result<(), String> {
    use sinomo_runtime::api::app::evolution::EvolutionGoalControlAction as Action;
    let _guard = crate::persistence::workflow_lock().map_err(|e| e.to_string())?;
    let goal = crate::persistence::goal_id_for_candidate(&packet.job_id, &packet.objective);
    let graph =
        crate::persistence::read_evolution_goal_graph(paths, &goal).map_err(|e| e.to_string())?;
    if packet.objective.starts_with("Campaign ") {
        for other in graph
            .agent_runs
            .iter()
            .filter_map(|r| r.execution.as_ref())
            .filter(|r| r.session_id != reference.session_id)
        {
            let state = execution_state(sessions, other)?;
            if matches!(
                state.phase,
                SessionExecutionPhase::Queued
                    | SessionExecutionPhase::Running
                    | SessionExecutionPhase::Paused
            ) {
                return Err("another campaign worker is still active".into());
            }
        }
    }
    let id = reference.session_id.clone();
    let command = match graph.control_action {
        Some(Action::Cancel | Action::Complete) => SessionCommand::Cancel { id },
        Some(Action::Pause) => return Ok(()),
        _ if matches!(
            graph.status,
            EvolutionGoalStatus::Paused | EvolutionGoalStatus::AwaitingDecision
        ) =>
        {
            return Ok(());
        }
        _ => SessionCommand::Resume { id },
    };
    sessions
        .ok_or("host session service unavailable")?
        .request(&SessionRequest::Command { command })?;
    Ok(())
}

/// One campaign resource slot. The mutex is held only while reserving/releasing
/// the slot; it is never held during execution or by goal-control requests.
pub(crate) struct CampaignExecutionLease {
    key: (std::path::PathBuf, String),
}
fn campaign_slots()
-> &'static std::sync::Mutex<std::collections::BTreeSet<(std::path::PathBuf, String)>> {
    static SLOTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeSet<(std::path::PathBuf, String)>>,
    > = std::sync::OnceLock::new();
    SLOTS.get_or_init(Default::default)
}
impl CampaignExecutionLease {
    pub(crate) fn acquire(paths: &EvolutionPaths, campaign: &str) -> Result<Self, String> {
        let key = (paths.data_dir.clone(), campaign.to_string());
        if !campaign_slots()
            .lock()
            .map_err(|_| "campaign admission lock poisoned")?
            .insert(key.clone())
        {
            return Err("campaign already has an active iteration".into());
        }
        Ok(Self { key })
    }
}
impl Drop for CampaignExecutionLease {
    fn drop(&mut self) {
        if let Ok(mut slots) = campaign_slots().lock() {
            slots.remove(&self.key);
        }
    }
}
