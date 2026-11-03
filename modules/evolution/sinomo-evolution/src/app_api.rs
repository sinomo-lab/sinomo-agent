//! Daemon-owned application projection over evolution persistence and commands.

use sinomo_runtime::api::app::evolution as contract;
use sinomo_runtime::local::SinomoDataDir;

use crate::persistence::{
    EvolutionPaths, StoredEvolutionJob, list_evolution_goal_graphs, list_job_records,
    read_campaign, read_evolution_goal_graph, write_campaign_bundle, write_evolution_goal_graph,
};
use crate::{
    CliError, EvolutionAgentRole, EvolutionAgentRunRecord, EvolutionAgentRunStatus,
    EvolutionArtifact, EvolutionArtifactRole, EvolutionCampaignStatus, EvolutionEventProvenance,
    EvolutionEventRouting, EvolutionGoalGraph, EvolutionGoalStatus, EvolutionGraphEvent,
    EvolutionJobId, EvolutionJobStatus, EvolutionMetricDefinition, EvolutionMetricDirection,
    EvolutionMetricObservation, EvolutionMetricValue, EvolutionResourceRecord, EvolutionRevision,
    EvolutionRevisionStatus, EvolutionTaskRecord, EvolutionTaskStatus, RevisionContentIdentity,
    run_evolve_command_captured,
};

pub fn app_evolution_dashboard(data_dir: &SinomoDataDir) -> contract::EvolutionDashboard {
    let paths = EvolutionPaths::new(data_dir);
    let source = paths.root.display().to_string();
    let mut records = match list_job_records(&paths) {
        Ok(records) => records,
        Err(error) => {
            return contract::EvolutionDashboard {
                source,
                goals: Vec::new(),
                jobs: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };
    records.sort_by_key(|record| std::cmp::Reverse(record.bundle.created_at));
    let (goals, graph_error) = match list_evolution_goal_graphs(&paths) {
        Ok(graphs) => (graphs.into_iter().map(summarize_graph).collect(), None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    };
    contract::EvolutionDashboard {
        source,
        goals,
        jobs: records.into_iter().map(summarize_record).collect(),
        error: graph_error,
    }
}

pub fn run_app_evolution_job(
    request: contract::EvolutionStartRequest,
    sessions: crate::SessionServices<'_>,
) -> Result<(), CliError> {
    let mut args = vec![
        "propose".to_string(),
        request.objective,
        "--target-ring".to_string(),
        request.target_ring,
    ];
    for path in request.intended_paths {
        args.push("--allow".to_string());
        args.push(path);
    }
    run_evolve_command_captured(&args, sessions).map(|_| ())
}

fn summarize_graph(graph: EvolutionGoalGraph) -> contract::EvolutionGoalSummary {
    let edges = graph
        .edges()
        .into_iter()
        .map(|edge| contract::EvolutionRevisionEdgeSummary {
            parent_id: edge.parent_revision_id,
            child_id: edge.child_revision_id,
        })
        .collect();
    let heads = graph.head_revision_ids();
    let recent_events = graph
        .events
        .iter()
        .rev()
        .take(20)
        .rev()
        .map(|event| contract::EvolutionEventSummary {
            id: event.event_id.clone(),
            kind: event.kind.clone(),
            occurred_at: event.occurred_at.clone(),
            revision_id: event.revision_id.clone(),
            task_id: event.task_id.clone(),
            agent_run_id: event.agent_run_id.clone(),
            actor: event.provenance.actor.clone(),
            evidence_refs: event.evidence_refs.clone(),
        })
        .collect();
    contract::EvolutionGoalSummary {
        id: graph.goal_id,
        objective: graph.objective,
        status: enum_label(&graph.status),
        evolution_agent_id: graph.evolution_agent_id,
        created_at: graph.created_at,
        updated_at: graph.updated_at,
        metric_definitions: graph
            .metric_definitions
            .into_iter()
            .map(|metric| contract::EvolutionMetricDefinitionSummary {
                id: metric.metric_id,
                label: metric.label,
                direction: enum_label(&metric.direction),
                unit: metric.unit,
                target: metric.target.as_ref().map(metric_value_label),
                required: metric.required,
            })
            .collect(),
        revisions: graph
            .revisions
            .into_iter()
            .map(|revision| {
                let content_kind = match &revision.content {
                    RevisionContentIdentity::GitCommit { .. } => "git-commit",
                    RevisionContentIdentity::DirectorySnapshot { .. } => "directory-snapshot",
                    RevisionContentIdentity::InlineContent { .. } => "inline-content",
                    RevisionContentIdentity::Composite { .. } => "composite",
                }
                .to_string();
                contract::EvolutionRevisionSummary {
                    id: revision.revision_id,
                    parent_ids: revision.parent_revision_ids,
                    status: enum_label(&revision.status),
                    summary: revision.summary,
                    content_kind,
                    content_identity: revision.content.locator(),
                    content_hash: revision.content.content_hash(),
                    created_at: revision.created_at,
                    job_id: revision.job_id,
                    artifacts: revision
                        .artifacts
                        .into_iter()
                        .map(|artifact| contract::EvolutionArtifactSummary {
                            id: artifact.artifact_id,
                            kind: artifact.kind,
                            uri: artifact.uri,
                            content_hash: artifact.content_hash,
                            role: enum_label(&artifact.role),
                        })
                        .collect(),
                    metrics: revision
                        .metrics
                        .into_iter()
                        .map(|metric| contract::EvolutionMetricObservationSummary {
                            metric_id: metric.metric_id,
                            value: metric_value_label(&metric.value),
                            sample_count: metric.sample_count,
                            outcome: metric.outcome,
                            evaluator: metric.evaluator,
                            artifact_id: metric.artifact_id,
                            measured_at: metric.measured_at,
                        })
                        .collect(),
                }
            })
            .collect(),
        edges,
        head_revision_ids: heads,
        tasks: graph
            .tasks
            .into_iter()
            .map(|task| contract::EvolutionTaskSummary {
                id: task.task_id,
                title: task.title,
                status: enum_label(&task.status),
                revision_id: task.revision_id,
                parent_task_id: task.parent_task_id,
                dependency_task_ids: task.dependency_task_ids,
                subgoals: task.subgoals,
                assigned_agent_run_id: task.assigned_agent_run_id,
            })
            .collect(),
        agent_runs: graph
            .agent_runs
            .into_iter()
            .map(|run| contract::EvolutionAgentRunSummary {
                id: run.agent_run_id,
                agent_id: run.agent_id,
                role: enum_label(&run.role),
                status: enum_label(&run.status),
                parent_agent_run_id: run.parent_agent_run_id,
                task_id: run.task_id,
                session_ref: run.session_ref,
                summary: run.summary,
            })
            .collect(),
        resources: graph
            .resources
            .into_iter()
            .map(|resource| contract::EvolutionResourceSummary {
                id: resource.record_id,
                revision_id: resource.revision_id,
                task_id: resource.task_id,
                agent_run_id: resource.agent_run_id,
                resource: resource.resource,
                quantity: resource.quantity,
                unit: resource.unit,
                monetary_microunits: resource.monetary_microunits,
                currency: resource.currency,
                confidence: resource.confidence,
                evidence_ref: resource.evidence_ref,
            })
            .collect(),
        recent_events,
        event_count: graph.events.len(),
    }
}

fn enum_label(value: &impl std::fmt::Debug) -> String {
    humanize_key(&format!("{value:?}"))
}

fn metric_value_label(value: &EvolutionMetricValue) -> String {
    match value {
        EvolutionMetricValue::Boolean(value) => value.to_string(),
        EvolutionMetricValue::Integer(value) => value.to_string(),
        EvolutionMetricValue::Decimal(value) | EvolutionMetricValue::Text(value) => value.clone(),
    }
}

pub fn run_app_evolution_decision(
    request: contract::EvolutionDecisionRequest,
) -> Result<(), CliError> {
    run_evolve_command_captured(
        &[
            request.action.command().to_string(),
            "--job".to_string(),
            request.job_id,
        ],
        None,
    )
    .map(|_| ())
}

pub fn run_app_create_goal(
    data_dir: &SinomoDataDir,
    request: contract::EvolutionGoalCreateRequest,
) -> Result<contract::EvolutionGoalCreateResponse, CliError> {
    if request.objective.trim().is_empty() {
        return Err(parse_error("evolution goal objective must be non-empty"));
    }
    if request.metrics.is_empty() {
        return Err(parse_error(
            "evolution goal must declare at least one metric or guardrail",
        ));
    }
    let paths = EvolutionPaths::new(data_dir);
    paths.ensure()?;
    let goal_id = format!("goal_{}", EvolutionJobId::generate());
    let now = current_time();
    let mut graph = EvolutionGoalGraph::new(
        goal_id.clone(),
        request.objective,
        request
            .evolution_agent_id
            .unwrap_or_else(|| crate::EVOLUTION_AGENT_ID.to_string()),
        now.clone(),
    );
    for metric in request.metrics {
        let direction = parse_metric_direction(&metric.direction)?;
        let target = metric.target.map(metric_input_value);
        if matches!(
            direction,
            EvolutionMetricDirection::Target | EvolutionMetricDirection::Constraint
        ) && target.is_none()
        {
            return Err(parse_error(format!(
                "metric `{}` requires a target value",
                metric.id
            )));
        }
        graph.upsert_metric_definition(EvolutionMetricDefinition {
            metric_id: required_text(metric.id, "metric id")?,
            label: required_text(metric.label, "metric label")?,
            direction,
            unit: required_text(metric.unit, "metric unit")?,
            target,
            required: metric.required,
        });
    }
    let governor_run_id = format!("governor_{goal_id}");
    graph.upsert_agent_run(EvolutionAgentRunRecord {
        agent_run_id: governor_run_id.clone(),
        agent_id: graph.evolution_agent_id.clone(),
        role: EvolutionAgentRole::Governor,
        status: EvolutionAgentRunStatus::Running,
        parent_agent_run_id: None,
        task_id: None,
        session_ref: None,
        summary: "govern the goal, tasks, revision DAG, evidence, and resources".to_string(),
        started_at: now.clone(),
        finished_at: None,
    });
    graph.append_event(EvolutionGraphEvent {
        event_id: format!("event_{}", EvolutionJobId::generate()),
        kind: crate::EVOLUTION_GOAL_CREATED_EVENT_KIND.to_string(),
        occurred_at: now.clone(),
        goal_id: goal_id.clone(),
        revision_id: None,
        task_id: None,
        agent_run_id: None,
        provenance: app_event_provenance(&graph.evolution_agent_id),
        evidence_refs: Vec::new(),
        caused_by_event_ids: Vec::new(),
        routing: app_event_routing(),
    });
    graph.append_event(EvolutionGraphEvent {
        event_id: format!("event_{}", EvolutionJobId::generate()),
        kind: crate::EVOLUTION_AGENT_UPDATED_EVENT_KIND.to_string(),
        occurred_at: now.clone(),
        goal_id: goal_id.clone(),
        revision_id: None,
        task_id: None,
        agent_run_id: Some(governor_run_id),
        provenance: app_event_provenance(&graph.evolution_agent_id),
        evidence_refs: Vec::new(),
        caused_by_event_ids: Vec::new(),
        routing: app_event_routing(),
    });
    write_evolution_goal_graph(&paths, &graph)?;
    Ok(contract::EvolutionGoalCreateResponse {
        accepted: true,
        goal_id,
    })
}

pub fn run_app_record_revision(
    data_dir: &SinomoDataDir,
    request: contract::EvolutionRevisionRecordRequest,
) -> Result<contract::EvolutionRevisionRecordResponse, CliError> {
    let paths = EvolutionPaths::new(data_dir);
    paths.ensure()?;
    let mut graph = read_evolution_goal_graph(&paths, &request.goal_id)?;
    let revision_id = request
        .revision_id
        .filter(|revision_id| !revision_id.trim().is_empty())
        .unwrap_or_else(|| format!("revision_{}", EvolutionJobId::generate()));
    let content = revision_content_from_input(request.content)?;
    let artifacts = request
        .artifacts
        .into_iter()
        .map(|artifact| artifact_from_input(&revision_id, artifact))
        .collect::<Result<Vec<_>, _>>()?;
    let artifact_ids = artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let now = current_time();
    let metrics = request
        .metrics
        .into_iter()
        .map(|metric| {
            if !graph
                .metric_definitions
                .iter()
                .any(|definition| definition.metric_id == metric.metric_id)
            {
                return Err(parse_error(format!(
                    "metric `{}` is not declared by goal {}",
                    metric.metric_id, graph.goal_id
                )));
            }
            if !artifact_ids.contains(metric.artifact_id.as_str()) {
                return Err(parse_error(format!(
                    "metric `{}` refers to artifact `{}` outside this revision",
                    metric.metric_id, metric.artifact_id
                )));
            }
            Ok(EvolutionMetricObservation {
                metric_id: metric.metric_id,
                value: metric_input_value(metric.value),
                sample_count: metric.sample_count,
                outcome: metric.outcome,
                evaluator: required_text(metric.evaluator, "metric evaluator")?,
                artifact_id: metric.artifact_id,
                measured_at: now.clone(),
            })
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    if let RevisionContentIdentity::Composite {
        patch_artifact_id, ..
    } = &content
        && !artifact_ids.contains(patch_artifact_id.as_str())
    {
        return Err(parse_error(format!(
            "composite revision patch artifact `{patch_artifact_id}` is not attached"
        )));
    }
    let status = parse_revision_status(&request.status)?;
    graph.status = match status {
        EvolutionRevisionStatus::AwaitingDecision => EvolutionGoalStatus::AwaitingDecision,
        EvolutionRevisionStatus::Selected => EvolutionGoalStatus::Running,
        EvolutionRevisionStatus::Failed => EvolutionGoalStatus::Failed,
        _ => EvolutionGoalStatus::Running,
    };
    graph.updated_at = now.clone();
    graph
        .upsert_revision(EvolutionRevision {
            revision_id: revision_id.clone(),
            parent_revision_ids: request.parent_revision_ids,
            content,
            artifacts,
            metrics,
            status,
            summary: required_text(request.summary, "revision summary")?,
            created_at: now.clone(),
            job_id: None,
            proposed_by_agent_run_id: None,
        })
        .map_err(|error| parse_error(error.to_string()))?;
    graph.append_event(EvolutionGraphEvent {
        event_id: format!("event_{}", EvolutionJobId::generate()),
        kind: crate::EVOLUTION_REVISION_PROPOSED_EVENT_KIND.to_string(),
        occurred_at: now,
        goal_id: graph.goal_id.clone(),
        revision_id: Some(revision_id.clone()),
        task_id: None,
        agent_run_id: None,
        provenance: app_event_provenance(&graph.evolution_agent_id),
        evidence_refs: graph
            .revisions
            .iter()
            .find(|revision| revision.revision_id == revision_id)
            .into_iter()
            .flat_map(|revision| {
                revision
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.uri.clone())
            })
            .collect(),
        caused_by_event_ids: Vec::new(),
        routing: app_event_routing(),
    });
    write_evolution_goal_graph(&paths, &graph)?;
    Ok(contract::EvolutionRevisionRecordResponse {
        accepted: true,
        revision_id,
    })
}

pub fn run_app_control_goal(
    data_dir: &SinomoDataDir,
    request: contract::EvolutionGoalControlRequest,
) -> Result<contract::EvolutionMutationResponse, CliError> {
    let paths = EvolutionPaths::new(data_dir);
    paths.ensure()?;
    if let Ok(mut campaign) = read_campaign(&paths, &request.goal_id) {
        campaign.status = match request.action {
            contract::EvolutionGoalControlAction::Pause => EvolutionCampaignStatus::PausedHuman,
            contract::EvolutionGoalControlAction::Resume => EvolutionCampaignStatus::Scheduled,
            contract::EvolutionGoalControlAction::Complete => EvolutionCampaignStatus::Complete,
            contract::EvolutionGoalControlAction::Cancel => EvolutionCampaignStatus::Cancelled,
        };
        campaign.updated_at = current_time();
        write_campaign_bundle(&paths, &campaign)?;
    }
    let mut graph = read_evolution_goal_graph(&paths, &request.goal_id)?;
    graph.status = match request.action {
        contract::EvolutionGoalControlAction::Pause => EvolutionGoalStatus::Paused,
        contract::EvolutionGoalControlAction::Resume => EvolutionGoalStatus::Running,
        contract::EvolutionGoalControlAction::Complete => EvolutionGoalStatus::Complete,
        contract::EvolutionGoalControlAction::Cancel => EvolutionGoalStatus::Cancelled,
    };
    let now = current_time();
    for task in &mut graph.tasks {
        task.status = match request.action {
            contract::EvolutionGoalControlAction::Pause
                if matches!(
                    task.status,
                    EvolutionTaskStatus::Ready | EvolutionTaskStatus::Running
                ) =>
            {
                EvolutionTaskStatus::AwaitingDecision
            }
            contract::EvolutionGoalControlAction::Resume
                if task.status == EvolutionTaskStatus::AwaitingDecision =>
            {
                EvolutionTaskStatus::Ready
            }
            contract::EvolutionGoalControlAction::Complete
                if !matches!(
                    task.status,
                    EvolutionTaskStatus::Failed | EvolutionTaskStatus::Cancelled
                ) =>
            {
                EvolutionTaskStatus::Complete
            }
            contract::EvolutionGoalControlAction::Cancel
                if !matches!(
                    task.status,
                    EvolutionTaskStatus::Complete | EvolutionTaskStatus::Failed
                ) =>
            {
                EvolutionTaskStatus::Cancelled
            }
            _ => task.status.clone(),
        };
        task.updated_at = now.clone();
    }
    for run in &mut graph.agent_runs {
        run.status = match request.action {
            contract::EvolutionGoalControlAction::Pause
                if run.status == EvolutionAgentRunStatus::Running =>
            {
                EvolutionAgentRunStatus::Waiting
            }
            contract::EvolutionGoalControlAction::Resume
                if run.status == EvolutionAgentRunStatus::Waiting =>
            {
                EvolutionAgentRunStatus::Running
            }
            contract::EvolutionGoalControlAction::Complete
                if !matches!(
                    run.status,
                    EvolutionAgentRunStatus::Failed | EvolutionAgentRunStatus::Cancelled
                ) =>
            {
                EvolutionAgentRunStatus::Complete
            }
            contract::EvolutionGoalControlAction::Cancel
                if !matches!(
                    run.status,
                    EvolutionAgentRunStatus::Complete | EvolutionAgentRunStatus::Failed
                ) =>
            {
                EvolutionAgentRunStatus::Cancelled
            }
            _ => run.status.clone(),
        };
        if matches!(
            run.status,
            EvolutionAgentRunStatus::Complete
                | EvolutionAgentRunStatus::Failed
                | EvolutionAgentRunStatus::Cancelled
        ) {
            run.finished_at.get_or_insert_with(|| now.clone());
        }
    }
    graph.updated_at = now.clone();
    graph.append_event(EvolutionGraphEvent {
        event_id: format!("event_{}", EvolutionJobId::generate()),
        kind: format!(
            "evolution.goal.{}",
            match request.action {
                contract::EvolutionGoalControlAction::Pause => "paused",
                contract::EvolutionGoalControlAction::Resume => "resumed",
                contract::EvolutionGoalControlAction::Complete => "completed",
                contract::EvolutionGoalControlAction::Cancel => "cancelled",
            }
        ),
        occurred_at: now,
        goal_id: graph.goal_id.clone(),
        revision_id: None,
        task_id: None,
        agent_run_id: None,
        provenance: app_event_provenance(&graph.evolution_agent_id),
        evidence_refs: (!request.note.trim().is_empty())
            .then_some(format!("operator-note:{}", request.note))
            .into_iter()
            .collect(),
        caused_by_event_ids: Vec::new(),
        routing: app_event_routing(),
    });
    write_evolution_goal_graph(&paths, &graph)?;
    Ok(contract::EvolutionMutationResponse { accepted: true })
}

pub fn run_app_update_workflow(
    data_dir: &SinomoDataDir,
    request: contract::EvolutionWorkflowUpdateRequest,
) -> Result<contract::EvolutionMutationResponse, CliError> {
    let paths = EvolutionPaths::new(data_dir);
    paths.ensure()?;
    let mut graph = read_evolution_goal_graph(&paths, &request.goal_id)?;
    let now = current_time();
    let mut events = Vec::new();
    for run in request.agent_runs {
        let run_id = required_text(run.id, "agent run id")?;
        graph.upsert_agent_run(EvolutionAgentRunRecord {
            agent_run_id: run_id.clone(),
            agent_id: required_text(run.agent_id, "agent id")?,
            role: parse_agent_role(&run.role)?,
            status: parse_agent_status(&run.status)?,
            parent_agent_run_id: run.parent_agent_run_id,
            task_id: run.task_id,
            session_ref: run.session_ref,
            summary: required_text(run.summary, "agent run summary")?,
            started_at: if run.started_at.trim().is_empty() {
                now.clone()
            } else {
                run.started_at
            },
            finished_at: run.finished_at,
        });
        events.push((
            crate::EVOLUTION_AGENT_UPDATED_EVENT_KIND,
            None,
            None,
            Some(run_id),
            Vec::new(),
        ));
    }
    for task in request.tasks {
        let task_id = required_text(task.id, "task id")?;
        graph.upsert_task(EvolutionTaskRecord {
            task_id: task_id.clone(),
            title: required_text(task.title, "task title")?,
            status: parse_task_status(&task.status)?,
            revision_id: task.revision_id.clone(),
            parent_task_id: task.parent_task_id,
            dependency_task_ids: task.dependency_task_ids,
            subgoals: task.subgoals,
            assigned_agent_run_id: task.assigned_agent_run_id,
            updated_at: now.clone(),
        });
        events.push((
            crate::EVOLUTION_TASK_UPDATED_EVENT_KIND,
            task.revision_id,
            Some(task_id),
            None,
            Vec::new(),
        ));
    }
    for resource in request.resources {
        let record_id = required_text(resource.id, "resource record id")?;
        let evidence_ref = required_text(resource.evidence_ref, "resource evidence ref")?;
        graph.upsert_resource(EvolutionResourceRecord {
            record_id,
            revision_id: resource.revision_id.clone(),
            task_id: resource.task_id.clone(),
            agent_run_id: resource.agent_run_id.clone(),
            resource: required_text(resource.resource, "resource name")?,
            quantity: resource.quantity,
            unit: required_text(resource.unit, "resource unit")?,
            monetary_microunits: resource.monetary_microunits,
            currency: resource.currency,
            confidence: required_text(resource.confidence, "resource confidence")?,
            evidence_ref: evidence_ref.clone(),
            recorded_at: now.clone(),
        });
        events.push((
            crate::EVOLUTION_RESOURCE_RECORDED_EVENT_KIND,
            resource.revision_id,
            resource.task_id,
            resource.agent_run_id,
            vec![evidence_ref],
        ));
    }
    if graph.status == EvolutionGoalStatus::Planning {
        graph.status = EvolutionGoalStatus::Running;
    }
    graph.updated_at = now.clone();
    for (kind, revision_id, task_id, agent_run_id, evidence_refs) in events {
        graph.append_event(EvolutionGraphEvent {
            event_id: format!("event_{}", EvolutionJobId::generate()),
            kind: kind.to_string(),
            occurred_at: now.clone(),
            goal_id: graph.goal_id.clone(),
            revision_id,
            task_id,
            agent_run_id,
            provenance: app_event_provenance(&graph.evolution_agent_id),
            evidence_refs,
            caused_by_event_ids: Vec::new(),
            routing: app_event_routing(),
        });
    }
    write_evolution_goal_graph(&paths, &graph)?;
    Ok(contract::EvolutionMutationResponse { accepted: true })
}

fn revision_content_from_input(
    content: contract::EvolutionRevisionContentInput,
) -> Result<RevisionContentIdentity, CliError> {
    Ok(match content {
        contract::EvolutionRevisionContentInput::GitCommit { repository, commit } => {
            RevisionContentIdentity::GitCommit {
                repository: required_text(repository, "git repository")?,
                commit: required_text(commit, "git commit")?,
            }
        }
        contract::EvolutionRevisionContentInput::DirectorySnapshot {
            path,
            digest,
            manifest_ref,
        } => RevisionContentIdentity::DirectorySnapshot {
            path: required_text(path, "snapshot path")?,
            digest: required_text(digest, "snapshot digest")?,
            manifest_ref,
        },
        contract::EvolutionRevisionContentInput::InlineContent {
            media_type,
            content,
            digest,
        } => {
            let actual = format!("blake3:{}", blake3::hash(content.as_bytes()).to_hex());
            if digest.as_deref().is_some_and(|digest| digest != actual) {
                return Err(parse_error(
                    "inline content digest does not match its bytes",
                ));
            }
            RevisionContentIdentity::InlineContent {
                media_type: required_text(media_type, "inline media type")?,
                digest: actual,
                content,
            }
        }
        contract::EvolutionRevisionContentInput::Composite {
            base_revision_id,
            patch_artifact_id,
            digest,
        } => RevisionContentIdentity::Composite {
            base_revision_id: required_text(base_revision_id, "base revision id")?,
            patch_artifact_id: required_text(patch_artifact_id, "patch artifact id")?,
            digest: required_text(digest, "composite digest")?,
        },
    })
}

fn artifact_from_input(
    revision_id: &str,
    artifact: contract::EvolutionArtifactInput,
) -> Result<EvolutionArtifact, CliError> {
    let kind = required_text(artifact.kind, "artifact kind")?;
    Ok(EvolutionArtifact {
        artifact_id: artifact
            .id
            .unwrap_or_else(|| format!("artifact_{}_{}", revision_id, EvolutionJobId::generate())),
        kind,
        uri: required_text(artifact.uri, "artifact URI")?,
        content_hash: required_text(artifact.content_hash, "artifact content hash")?,
        media_type: artifact.media_type,
        schema: artifact.schema,
        role: parse_artifact_role(&artifact.role)?,
    })
}

fn parse_metric_direction(value: &str) -> Result<EvolutionMetricDirection, CliError> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "maximize" => Ok(EvolutionMetricDirection::Maximize),
        "minimize" => Ok(EvolutionMetricDirection::Minimize),
        "target" => Ok(EvolutionMetricDirection::Target),
        "constraint" => Ok(EvolutionMetricDirection::Constraint),
        "observe" => Ok(EvolutionMetricDirection::Observe),
        _ => Err(parse_error(format!("unknown metric direction `{value}`"))),
    }
}

fn parse_artifact_role(value: &str) -> Result<EvolutionArtifactRole, CliError> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "candidate-content" => Ok(EvolutionArtifactRole::CandidateContent),
        "evaluation-input" => Ok(EvolutionArtifactRole::EvaluationInput),
        "evaluation-output" => Ok(EvolutionArtifactRole::EvaluationOutput),
        "provenance" => Ok(EvolutionArtifactRole::Provenance),
        "agent-trace" => Ok(EvolutionArtifactRole::AgentTrace),
        "resource-evidence" => Ok(EvolutionArtifactRole::ResourceEvidence),
        "other" => Ok(EvolutionArtifactRole::Other),
        _ => Err(parse_error(format!("unknown artifact role `{value}`"))),
    }
}

fn parse_revision_status(value: &str) -> Result<EvolutionRevisionStatus, CliError> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "baseline" => Ok(EvolutionRevisionStatus::Baseline),
        "proposed" => Ok(EvolutionRevisionStatus::Proposed),
        "evaluating" => Ok(EvolutionRevisionStatus::Evaluating),
        "awaiting-decision" => Ok(EvolutionRevisionStatus::AwaitingDecision),
        "selected" => Ok(EvolutionRevisionStatus::Selected),
        "rejected" => Ok(EvolutionRevisionStatus::Rejected),
        "failed" => Ok(EvolutionRevisionStatus::Failed),
        "rolled-back" => Ok(EvolutionRevisionStatus::RolledBack),
        "superseded" => Ok(EvolutionRevisionStatus::Superseded),
        _ => Err(parse_error(format!("unknown revision status `{value}`"))),
    }
}

fn parse_task_status(value: &str) -> Result<EvolutionTaskStatus, CliError> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "planned" => Ok(EvolutionTaskStatus::Planned),
        "ready" => Ok(EvolutionTaskStatus::Ready),
        "running" => Ok(EvolutionTaskStatus::Running),
        "awaiting-evidence" => Ok(EvolutionTaskStatus::AwaitingEvidence),
        "awaiting-decision" => Ok(EvolutionTaskStatus::AwaitingDecision),
        "complete" => Ok(EvolutionTaskStatus::Complete),
        "failed" => Ok(EvolutionTaskStatus::Failed),
        "cancelled" => Ok(EvolutionTaskStatus::Cancelled),
        _ => Err(parse_error(format!("unknown task status `{value}`"))),
    }
}

fn parse_agent_role(value: &str) -> Result<EvolutionAgentRole, CliError> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "governor" => Ok(EvolutionAgentRole::Governor),
        "proposer" => Ok(EvolutionAgentRole::Proposer),
        "implementer" => Ok(EvolutionAgentRole::Implementer),
        "evaluator" => Ok(EvolutionAgentRole::Evaluator),
        "specialist" => Ok(EvolutionAgentRole::Specialist),
        _ => Err(parse_error(format!(
            "unknown evolution agent role `{value}`"
        ))),
    }
}

fn parse_agent_status(value: &str) -> Result<EvolutionAgentRunStatus, CliError> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "queued" => Ok(EvolutionAgentRunStatus::Queued),
        "running" => Ok(EvolutionAgentRunStatus::Running),
        "waiting" => Ok(EvolutionAgentRunStatus::Waiting),
        "complete" => Ok(EvolutionAgentRunStatus::Complete),
        "failed" => Ok(EvolutionAgentRunStatus::Failed),
        "cancelled" => Ok(EvolutionAgentRunStatus::Cancelled),
        _ => Err(parse_error(format!(
            "unknown evolution agent status `{value}`"
        ))),
    }
}

fn metric_input_value(value: contract::EvolutionMetricValueInput) -> EvolutionMetricValue {
    match value {
        contract::EvolutionMetricValueInput::Boolean(value) => EvolutionMetricValue::Boolean(value),
        contract::EvolutionMetricValueInput::Integer(value) => EvolutionMetricValue::Integer(value),
        contract::EvolutionMetricValueInput::Decimal(value) => EvolutionMetricValue::Decimal(value),
        contract::EvolutionMetricValueInput::Text(value) => EvolutionMetricValue::Text(value),
    }
}

fn required_text(value: String, field: &str) -> Result<String, CliError> {
    if value.trim().is_empty() {
        Err(parse_error(format!("{field} must be non-empty")))
    } else {
        Ok(value)
    }
}

fn parse_error(detail: impl Into<String>) -> CliError {
    CliError::Parse {
        detail: detail.into(),
    }
}

fn current_time() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string())
}

fn app_event_provenance(actor: &str) -> EvolutionEventProvenance {
    EvolutionEventProvenance {
        source: "sinomo.evolution.app".to_string(),
        actor: actor.to_string(),
        node_id: None,
    }
}

fn app_event_routing() -> EvolutionEventRouting {
    EvolutionEventRouting {
        scope: "evolution-goal".to_string(),
        retention: "durable".to_string(),
    }
}

fn summarize_record(record: StoredEvolutionJob) -> contract::EvolutionJobSummary {
    let StoredEvolutionJob {
        job,
        candidate_workspace,
        bundle,
        verification_outcome,
        changed_files,
        decision,
    } = record;
    let status_key = status_key(&job.status).to_string();
    let id = job.job_id.to_string();
    contract::EvolutionJobSummary {
        short_id: short_id(&id, 12),
        id,
        objective: job.objective,
        status_label: humanize_key(&status_key),
        status_key,
        phase: phase_for_status(&job.status),
        target_ring: format!("{:?}", job.target_ring),
        target_module: job.target_module_id.map(|id| id.to_string()),
        created_at: bundle
            .created_at
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| bundle.created_at.to_string()),
        workspace_root: job.edit_scope.workspace_root,
        candidate_workspace: candidate_workspace.path.display().to_string(),
        candidate_branch: candidate_workspace.branch_name,
        source_revision: candidate_workspace.source_revision,
        verification_outcome: verification_outcome
            .as_ref()
            .map(|outcome| humanize_key(&format!("{outcome:?}"))),
        required_gate_count: job.required_gates.len(),
        verification_command_count: job.verification.commands.len(),
        module_replay_count: job.verification.module_replay_reports.len(),
        changed_files,
        allowed_paths: job.scope.allowed_paths,
        forbidden_paths: job.scope.forbidden_paths,
        recovery_strategy: humanize_key(&format!("{:?}", job.recovery.strategy)),
        recovery_note: job.recovery.note,
        rollback_policy: format!("{:?}", job.rollback_policy),
        bundle_path: bundle.bundle_path,
        mutation_packet_path: bundle.mutation_packet_path,
        diff_path: bundle.diff_path,
        scorecard_path: bundle.scorecard_path,
        candidate_summary_path: bundle.candidate_summary_path,
        decision_path: bundle.decision_path,
        artifact_ref_count: bundle.artifact_refs.len(),
        command_log_count: bundle.command_logs.len(),
        decision: decision.map(|decision| contract::EvolutionDecisionSummary {
            outcome: humanize_key(&format!("{:?}", decision.outcome)),
            decided_at: decision
                .decided_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| decision.decided_at.to_string()),
            decided_by: decision.decided_by,
            note: decision.note,
            preflight_passed: decision
                .approval_preflight
                .map(|preflight| preflight.passed),
        }),
    }
}

fn status_key(status: &EvolutionJobStatus) -> &'static str {
    match status {
        EvolutionJobStatus::Created => "created",
        EvolutionJobStatus::PacketWritten => "packet-written",
        EvolutionJobStatus::MutationStarted => "mutation-started",
        EvolutionJobStatus::MutationFinished => "mutation-finished",
        EvolutionJobStatus::Verified => "verified",
        EvolutionJobStatus::AwaitingApproval => "awaiting-approval",
        EvolutionJobStatus::Approved => "approved",
        EvolutionJobStatus::Revised => "revised",
        EvolutionJobStatus::Restored => "restored",
        EvolutionJobStatus::Failed => "failed",
        EvolutionJobStatus::Observed => "observed",
        EvolutionJobStatus::Scoped => "scoped",
        EvolutionJobStatus::Proposed => "proposed",
        EvolutionJobStatus::Implemented => "implemented",
        EvolutionJobStatus::Built => "built",
        EvolutionJobStatus::Evaluated => "evaluated",
        EvolutionJobStatus::PolicyApproved => "policy-approved",
        EvolutionJobStatus::Staged => "staged",
        EvolutionJobStatus::Shadowed => "shadowed",
        EvolutionJobStatus::Canary => "canary",
        EvolutionJobStatus::Promoted => "promoted",
        EvolutionJobStatus::Rejected => "rejected",
        EvolutionJobStatus::BuildFailed => "build-failed",
        EvolutionJobStatus::EvalFailed => "eval-failed",
        EvolutionJobStatus::Denied => "denied",
        EvolutionJobStatus::RolledBack => "rolled-back",
        EvolutionJobStatus::Reverted => "reverted",
    }
}

fn phase_for_status(status: &EvolutionJobStatus) -> contract::EvolutionPhase {
    match status {
        EvolutionJobStatus::AwaitingApproval => contract::EvolutionPhase::AwaitingDecision,
        EvolutionJobStatus::Approved
        | EvolutionJobStatus::PolicyApproved
        | EvolutionJobStatus::Staged
        | EvolutionJobStatus::Shadowed
        | EvolutionJobStatus::Canary
        | EvolutionJobStatus::Promoted => contract::EvolutionPhase::Passed,
        EvolutionJobStatus::Failed
        | EvolutionJobStatus::BuildFailed
        | EvolutionJobStatus::EvalFailed
        | EvolutionJobStatus::Denied
        | EvolutionJobStatus::Rejected => contract::EvolutionPhase::Failed,
        EvolutionJobStatus::Restored
        | EvolutionJobStatus::RolledBack
        | EvolutionJobStatus::Reverted => contract::EvolutionPhase::Recovered,
        _ => contract::EvolutionPhase::Active,
    }
}

fn humanize_key(value: &str) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch == '-' || ch == '_' {
            out.push(' ');
        } else if ch.is_uppercase() {
            if index > 0 {
                out.push(' ');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn short_id(id: &str, max: usize) -> String {
    if id.chars().count() <= max {
        id.to_string()
    } else {
        id.chars().take(max).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_goal_accepts_inline_revision_artifacts_metrics_and_control() {
        let root = sinomo_testdir::TestDir::new("evolution-generic-app-api");
        let data_dir = SinomoDataDir::new(root.path());
        let goal = run_app_create_goal(
            &data_dir,
            contract::EvolutionGoalCreateRequest {
                objective: "Improve a generated answer".to_string(),
                evolution_agent_id: None,
                metrics: vec![contract::EvolutionMetricDefinitionInput {
                    id: "quality".to_string(),
                    label: "Quality".to_string(),
                    direction: "maximize".to_string(),
                    unit: "points".to_string(),
                    target: None,
                    required: true,
                }],
            },
        )
        .expect("create goal");
        let revision = run_app_record_revision(
            &data_dir,
            contract::EvolutionRevisionRecordRequest {
                goal_id: goal.goal_id.clone(),
                revision_id: Some("revision-baseline".to_string()),
                parent_revision_ids: Vec::new(),
                content: contract::EvolutionRevisionContentInput::InlineContent {
                    media_type: "text/plain".to_string(),
                    content: "candidate answer".to_string(),
                    digest: None,
                },
                artifacts: vec![contract::EvolutionArtifactInput {
                    id: Some("artifact-eval".to_string()),
                    kind: "evaluation-results".to_string(),
                    uri: "memory://evaluation-results".to_string(),
                    content_hash: format!("blake3:{}", "2".repeat(64)),
                    media_type: "application/json".to_string(),
                    schema: "example.eval/v1".to_string(),
                    role: "evaluation-output".to_string(),
                }],
                metrics: vec![contract::EvolutionMetricObservationInput {
                    metric_id: "quality".to_string(),
                    value: contract::EvolutionMetricValueInput::Integer(82),
                    sample_count: Some(10),
                    outcome: Some("passed".to_string()),
                    evaluator: "example-evaluator".to_string(),
                    artifact_id: "artifact-eval".to_string(),
                }],
                status: "baseline".to_string(),
                summary: "initial answer".to_string(),
            },
        )
        .expect("record revision");
        assert_eq!(revision.revision_id, "revision-baseline");

        run_app_update_workflow(
            &data_dir,
            contract::EvolutionWorkflowUpdateRequest {
                goal_id: goal.goal_id.clone(),
                tasks: vec![contract::EvolutionTaskInput {
                    id: "task-improve".to_string(),
                    title: "Improve the answer".to_string(),
                    status: "running".to_string(),
                    revision_id: Some("revision-baseline".to_string()),
                    parent_task_id: None,
                    dependency_task_ids: Vec::new(),
                    subgoals: vec!["raise quality".to_string()],
                    assigned_agent_run_id: Some("run-implementer".to_string()),
                }],
                agent_runs: vec![contract::EvolutionAgentRunInput {
                    id: "run-implementer".to_string(),
                    agent_id: crate::EVOLUTION_IMPLEMENTER_AGENT_ID.to_string(),
                    role: "implementer".to_string(),
                    status: "running".to_string(),
                    parent_agent_run_id: Some(format!("governor_{}", goal.goal_id)),
                    task_id: Some("task-improve".to_string()),
                    session_ref: Some("agent://implementer".to_string()),
                    summary: "implement the next revision".to_string(),
                    started_at: String::new(),
                    finished_at: None,
                }],
                resources: vec![contract::EvolutionResourceInput {
                    id: "cost-1".to_string(),
                    revision_id: Some("revision-baseline".to_string()),
                    task_id: Some("task-improve".to_string()),
                    agent_run_id: Some("run-implementer".to_string()),
                    resource: "model-tokens".to_string(),
                    quantity: 1200,
                    unit: "tokens".to_string(),
                    monetary_microunits: Some(35_000),
                    currency: Some("USD".to_string()),
                    confidence: "provider-reported".to_string(),
                    evidence_ref: "usage://run-implementer".to_string(),
                }],
            },
        )
        .expect("update workflow");

        run_app_control_goal(
            &data_dir,
            contract::EvolutionGoalControlRequest {
                goal_id: goal.goal_id.clone(),
                action: contract::EvolutionGoalControlAction::Pause,
                note: "review metrics".to_string(),
            },
        )
        .expect("pause goal");
        let dashboard = app_evolution_dashboard(&data_dir);
        assert!(dashboard.error.is_none());
        assert_eq!(dashboard.goals.len(), 1);
        assert_eq!(dashboard.goals[0].status, "paused");
        assert_eq!(dashboard.goals[0].revisions[0].metrics[0].value, "82");
        assert_eq!(
            dashboard.goals[0].revisions[0].content_kind,
            "inline-content"
        );
        assert_eq!(dashboard.goals[0].tasks.len(), 1);
        assert_eq!(dashboard.goals[0].agent_runs.len(), 2);
        assert_eq!(
            dashboard.goals[0].resources[0].monetary_microunits,
            Some(35_000)
        );
    }
}
