//! Generic, evaluator-driven evolution goal and revision graph.
//!
//! The graph is deliberately independent from git and from Sinomo rollout. A
//! revision proves its immutable content identity, owns the artifacts that were
//! evaluated, and records its metric observations. Jobs, agents, tasks, and
//! resource records are orchestration metadata around that immutable core.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const EVOLUTION_GRAPH_SCHEMA_VERSION: u32 = 1;
pub const EVOLUTION_GOAL_CREATED_EVENT_KIND: &str = "evolution.goal.created";
pub const EVOLUTION_REVISION_PROPOSED_EVENT_KIND: &str = "evolution.revision.proposed";
pub const EVOLUTION_REVISION_EVALUATED_EVENT_KIND: &str = "evolution.revision.evaluated";
pub const EVOLUTION_REVISION_AWAITING_DECISION_EVENT_KIND: &str =
    "evolution.revision.awaiting-decision";
pub const EVOLUTION_REVISION_SELECTED_EVENT_KIND: &str = "evolution.revision.selected";
pub const EVOLUTION_REVISION_FAILED_EVENT_KIND: &str = "evolution.revision.failed";
pub const EVOLUTION_REVISION_RECOVERED_EVENT_KIND: &str = "evolution.revision.recovered";
pub const EVOLUTION_TASK_UPDATED_EVENT_KIND: &str = "evolution.task.updated";
pub const EVOLUTION_AGENT_UPDATED_EVENT_KIND: &str = "evolution.agent.updated";
pub const EVOLUTION_RESOURCE_RECORDED_EVENT_KIND: &str = "evolution.resource.recorded";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionGoalGraph {
    pub schema_version: u32,
    pub goal_id: String,
    pub objective: String,
    pub status: EvolutionGoalStatus,
    pub evolution_agent_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metric_definitions: Vec<EvolutionMetricDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revisions: Vec<EvolutionRevision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<EvolutionTaskRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_runs: Vec<EvolutionAgentRunRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<EvolutionResourceRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EvolutionGraphEvent>,
    pub created_at: String,
    pub updated_at: String,
}

impl EvolutionGoalGraph {
    pub fn new(
        goal_id: impl Into<String>,
        objective: impl Into<String>,
        evolution_agent_id: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Self {
        let created_at = created_at.into();
        Self {
            schema_version: EVOLUTION_GRAPH_SCHEMA_VERSION,
            goal_id: goal_id.into(),
            objective: objective.into(),
            status: EvolutionGoalStatus::Planning,
            evolution_agent_id: evolution_agent_id.into(),
            metric_definitions: Vec::new(),
            revisions: Vec::new(),
            tasks: Vec::new(),
            agent_runs: Vec::new(),
            resources: Vec::new(),
            events: Vec::new(),
            created_at: created_at.clone(),
            updated_at: created_at,
        }
    }

    pub fn upsert_metric_definition(&mut self, definition: EvolutionMetricDefinition) {
        if let Some(existing) = self
            .metric_definitions
            .iter_mut()
            .find(|existing| existing.metric_id == definition.metric_id)
        {
            *existing = definition;
        } else {
            self.metric_definitions.push(definition);
            self.metric_definitions
                .sort_by(|left, right| left.metric_id.cmp(&right.metric_id));
        }
    }

    pub fn upsert_revision(
        &mut self,
        revision: EvolutionRevision,
    ) -> Result<(), EvolutionGraphError> {
        revision.content.validate()?;
        if revision
            .parent_revision_ids
            .iter()
            .any(|parent| parent == &revision.revision_id)
        {
            return Err(EvolutionGraphError::SelfParent {
                revision_id: revision.revision_id,
            });
        }
        let known = self
            .revisions
            .iter()
            .map(|revision| revision.revision_id.as_str())
            .collect::<BTreeSet<_>>();
        for parent in &revision.parent_revision_ids {
            if !known.contains(parent.as_str()) {
                return Err(EvolutionGraphError::MissingParent {
                    revision_id: revision.revision_id.clone(),
                    parent_revision_id: parent.clone(),
                });
            }
        }
        let mut next = self.clone();
        if let Some(existing) = next
            .revisions
            .iter_mut()
            .find(|existing| existing.revision_id == revision.revision_id)
        {
            if existing.parent_revision_ids != revision.parent_revision_ids
                || existing.content != revision.content
            {
                return Err(EvolutionGraphError::ImmutableRevisionConflict {
                    revision_id: revision.revision_id,
                });
            }
            let created_at = existing.created_at.clone();
            *existing = revision;
            existing.created_at = created_at;
        } else {
            next.revisions.push(revision);
        }
        next.validate()?;
        *self = next;
        Ok(())
    }

    pub fn upsert_task(&mut self, task: EvolutionTaskRecord) {
        if let Some(existing) = self
            .tasks
            .iter_mut()
            .find(|existing| existing.task_id == task.task_id)
        {
            *existing = task;
        } else {
            self.tasks.push(task);
        }
    }

    pub fn upsert_agent_run(&mut self, run: EvolutionAgentRunRecord) {
        if let Some(existing) = self
            .agent_runs
            .iter_mut()
            .find(|existing| existing.agent_run_id == run.agent_run_id)
        {
            *existing = run;
        } else {
            self.agent_runs.push(run);
        }
    }

    pub fn upsert_resource(&mut self, resource: EvolutionResourceRecord) {
        if let Some(existing) = self
            .resources
            .iter_mut()
            .find(|existing| existing.record_id == resource.record_id)
        {
            *existing = resource;
        } else {
            self.resources.push(resource);
        }
    }

    pub fn append_event(&mut self, event: EvolutionGraphEvent) {
        if !self
            .events
            .iter()
            .any(|existing| existing.event_id == event.event_id)
        {
            self.events.push(event);
        }
    }

    pub fn head_revision_ids(&self) -> Vec<String> {
        let parents = self
            .revisions
            .iter()
            .flat_map(|revision| revision.parent_revision_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        self.revisions
            .iter()
            .filter(|revision| !parents.contains(&revision.revision_id))
            .map(|revision| revision.revision_id.clone())
            .collect()
    }

    pub fn edges(&self) -> Vec<EvolutionRevisionEdge> {
        self.revisions
            .iter()
            .flat_map(|revision| {
                revision
                    .parent_revision_ids
                    .iter()
                    .map(|parent| EvolutionRevisionEdge {
                        parent_revision_id: parent.clone(),
                        child_revision_id: revision.revision_id.clone(),
                    })
            })
            .collect()
    }

    pub fn validate(&self) -> Result<(), EvolutionGraphError> {
        if self.schema_version != EVOLUTION_GRAPH_SCHEMA_VERSION {
            return Err(EvolutionGraphError::UnsupportedSchema {
                schema_version: self.schema_version,
            });
        }
        if self.goal_id.trim().is_empty() || self.objective.trim().is_empty() {
            return Err(EvolutionGraphError::InvalidGoal);
        }
        let metric_ids = self
            .metric_definitions
            .iter()
            .map(|metric| metric.metric_id.as_str())
            .collect::<BTreeSet<_>>();
        if metric_ids.len() != self.metric_definitions.len()
            || self.metric_definitions.iter().any(|metric| {
                metric.metric_id.trim().is_empty()
                    || metric.label.trim().is_empty()
                    || metric.unit.trim().is_empty()
                    || metric
                        .target
                        .as_ref()
                        .is_some_and(|target| !valid_metric_value(target))
            })
        {
            return Err(EvolutionGraphError::InvalidMetricDefinition);
        }
        let mut revisions = BTreeMap::new();
        for revision in &self.revisions {
            revision.content.validate()?;
            let artifact_ids = revision
                .artifacts
                .iter()
                .map(|artifact| artifact.artifact_id.as_str())
                .collect::<BTreeSet<_>>();
            if artifact_ids.len() != revision.artifacts.len()
                || revision.artifacts.iter().any(|artifact| {
                    artifact.artifact_id.trim().is_empty()
                        || artifact.kind.trim().is_empty()
                        || artifact.uri.trim().is_empty()
                        || !valid_blake3_digest(&artifact.content_hash)
                })
            {
                return Err(EvolutionGraphError::InvalidArtifact {
                    revision_id: revision.revision_id.clone(),
                });
            }
            if let RevisionContentIdentity::Composite {
                patch_artifact_id, ..
            } = &revision.content
                && !artifact_ids.contains(patch_artifact_id.as_str())
            {
                return Err(EvolutionGraphError::MissingContentArtifact {
                    revision_id: revision.revision_id.clone(),
                    artifact_id: patch_artifact_id.clone(),
                });
            }
            for observation in &revision.metrics {
                if !valid_metric_value(&observation.value) {
                    return Err(EvolutionGraphError::InvalidMetricValue {
                        revision_id: revision.revision_id.clone(),
                        metric_id: observation.metric_id.clone(),
                    });
                }
                if !metric_ids.contains(observation.metric_id.as_str()) {
                    return Err(EvolutionGraphError::UnknownMetric {
                        revision_id: revision.revision_id.clone(),
                        metric_id: observation.metric_id.clone(),
                    });
                }
                if !artifact_ids.contains(observation.artifact_id.as_str()) {
                    return Err(EvolutionGraphError::MissingMetricArtifact {
                        revision_id: revision.revision_id.clone(),
                        metric_id: observation.metric_id.clone(),
                        artifact_id: observation.artifact_id.clone(),
                    });
                }
            }
            if revisions
                .insert(revision.revision_id.as_str(), revision)
                .is_some()
            {
                return Err(EvolutionGraphError::DuplicateRevision {
                    revision_id: revision.revision_id.clone(),
                });
            }
        }
        for revision in &self.revisions {
            for parent in &revision.parent_revision_ids {
                if parent == &revision.revision_id {
                    return Err(EvolutionGraphError::SelfParent {
                        revision_id: revision.revision_id.clone(),
                    });
                }
                if !revisions.contains_key(parent.as_str()) {
                    return Err(EvolutionGraphError::MissingParent {
                        revision_id: revision.revision_id.clone(),
                        parent_revision_id: parent.clone(),
                    });
                }
            }
        }

        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for revision_id in revisions.keys().copied() {
            visit_revision(revision_id, &revisions, &mut visiting, &mut visited)?;
        }
        let tasks = unique_records(
            self.tasks.iter().map(|task| task.task_id.as_str()),
            self.tasks.len(),
        )
        .ok_or(EvolutionGraphError::InvalidTaskGraph)?;
        let agents = unique_records(
            self.agent_runs.iter().map(|run| run.agent_run_id.as_str()),
            self.agent_runs.len(),
        )
        .ok_or(EvolutionGraphError::InvalidAgentGraph)?;
        for task in &self.tasks {
            if task
                .revision_id
                .as_deref()
                .is_some_and(|revision_id| !revisions.contains_key(revision_id))
                || task
                    .parent_task_id
                    .as_deref()
                    .is_some_and(|parent| !tasks.contains(parent))
                || task
                    .dependency_task_ids
                    .iter()
                    .any(|dependency| !tasks.contains(dependency.as_str()))
                || task
                    .assigned_agent_run_id
                    .as_deref()
                    .is_some_and(|agent| !agents.contains(agent))
            {
                return Err(EvolutionGraphError::InvalidTaskGraph);
            }
        }
        let task_records = self
            .tasks
            .iter()
            .map(|task| (task.task_id.as_str(), task))
            .collect::<BTreeMap<_, _>>();
        validate_reference_dag(
            task_records.keys().copied(),
            |task_id| {
                task_records.get(task_id).into_iter().flat_map(|task| {
                    task.parent_task_id
                        .iter()
                        .chain(task.dependency_task_ids.iter())
                        .map(String::as_str)
                })
            },
            EvolutionGraphError::InvalidTaskGraph,
        )?;
        for run in &self.agent_runs {
            if run
                .parent_agent_run_id
                .as_deref()
                .is_some_and(|parent| !agents.contains(parent))
                || run
                    .task_id
                    .as_deref()
                    .is_some_and(|task| !tasks.contains(task))
            {
                return Err(EvolutionGraphError::InvalidAgentGraph);
            }
        }
        let agent_records = self
            .agent_runs
            .iter()
            .map(|run| (run.agent_run_id.as_str(), run))
            .collect::<BTreeMap<_, _>>();
        validate_reference_dag(
            agent_records.keys().copied(),
            |agent_id| {
                agent_records
                    .get(agent_id)
                    .and_then(|run| run.parent_agent_run_id.as_deref())
                    .into_iter()
            },
            EvolutionGraphError::InvalidAgentGraph,
        )?;
        let resource_ids = unique_records(
            self.resources
                .iter()
                .map(|resource| resource.record_id.as_str()),
            self.resources.len(),
        )
        .ok_or(EvolutionGraphError::InvalidResourceRecords)?;
        if resource_ids.len() != self.resources.len()
            || self.resources.iter().any(|resource| {
                resource.resource.trim().is_empty()
                    || resource.unit.trim().is_empty()
                    || resource.evidence_ref.trim().is_empty()
                    || resource
                        .revision_id
                        .as_deref()
                        .is_some_and(|revision| !revisions.contains_key(revision))
                    || resource
                        .task_id
                        .as_deref()
                        .is_some_and(|task| !tasks.contains(task))
                    || resource
                        .agent_run_id
                        .as_deref()
                        .is_some_and(|agent| !agents.contains(agent))
            })
        {
            return Err(EvolutionGraphError::InvalidResourceRecords);
        }
        Ok(())
    }
}

fn validate_reference_dag<'a, I, F, P>(
    ids: I,
    parents: F,
    error: EvolutionGraphError,
) -> Result<(), EvolutionGraphError>
where
    I: IntoIterator<Item = &'a str>,
    F: Fn(&'a str) -> P,
    P: IntoIterator<Item = &'a str>,
{
    fn visit<'a, F, P>(
        id: &'a str,
        parents: &F,
        visiting: &mut BTreeSet<&'a str>,
        visited: &mut BTreeSet<&'a str>,
    ) -> bool
    where
        F: Fn(&'a str) -> P,
        P: IntoIterator<Item = &'a str>,
    {
        if visited.contains(id) {
            return true;
        }
        if !visiting.insert(id) {
            return false;
        }
        for parent in parents(id) {
            if !visit(parent, parents, visiting, visited) {
                return false;
            }
        }
        visiting.remove(id);
        visited.insert(id);
        true
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for id in ids {
        if !visit(id, &parents, &mut visiting, &mut visited) {
            return Err(error);
        }
    }
    Ok(())
}

fn unique_records<'a>(
    values: impl Iterator<Item = &'a str>,
    expected: usize,
) -> Option<BTreeSet<&'a str>> {
    let values = values.collect::<BTreeSet<_>>();
    (values.len() == expected && values.iter().all(|value| !value.trim().is_empty()))
        .then_some(values)
}

fn visit_revision<'a>(
    revision_id: &'a str,
    revisions: &BTreeMap<&'a str, &'a EvolutionRevision>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> Result<(), EvolutionGraphError> {
    if visited.contains(revision_id) {
        return Ok(());
    }
    if !visiting.insert(revision_id) {
        return Err(EvolutionGraphError::Cycle {
            revision_id: revision_id.to_string(),
        });
    }
    if let Some(revision) = revisions.get(revision_id) {
        for parent in &revision.parent_revision_ids {
            visit_revision(parent, revisions, visiting, visited)?;
        }
    }
    visiting.remove(revision_id);
    visited.insert(revision_id);
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionGoalStatus {
    Planning,
    Running,
    AwaitingDecision,
    Paused,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRevision {
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_revision_ids: Vec<String>,
    pub content: RevisionContentIdentity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<EvolutionArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<EvolutionMetricObservation>,
    pub status: EvolutionRevisionStatus,
    pub summary: String,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_by_agent_run_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RevisionContentIdentity {
    GitCommit {
        repository: String,
        commit: String,
    },
    DirectorySnapshot {
        path: String,
        digest: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        manifest_ref: Option<String>,
    },
    InlineContent {
        media_type: String,
        digest: String,
        content: String,
    },
    Composite {
        base_revision_id: String,
        patch_artifact_id: String,
        digest: String,
    },
}

impl RevisionContentIdentity {
    pub fn content_hash(&self) -> String {
        match self {
            Self::GitCommit { commit, .. } => format!("git:{commit}"),
            Self::DirectorySnapshot { digest, .. }
            | Self::InlineContent { digest, .. }
            | Self::Composite { digest, .. } => digest.clone(),
        }
    }

    pub fn locator(&self) -> String {
        match self {
            Self::GitCommit { repository, commit } => format!("{repository}@{commit}"),
            Self::DirectorySnapshot { path, .. } => path.clone(),
            Self::InlineContent { media_type, .. } => format!("inline:{media_type}"),
            Self::Composite {
                base_revision_id,
                patch_artifact_id,
                ..
            } => format!("{base_revision_id}+{patch_artifact_id}"),
        }
    }

    pub fn validate(&self) -> Result<(), EvolutionGraphError> {
        let invalid = match self {
            Self::GitCommit { repository, commit } => {
                repository.trim().is_empty() || commit.trim().is_empty()
            }
            Self::DirectorySnapshot { path, digest, .. } => {
                path.trim().is_empty() || !valid_blake3_digest(digest)
            }
            Self::InlineContent {
                media_type,
                digest,
                content,
            } => {
                media_type.trim().is_empty()
                    || !valid_blake3_digest(digest)
                    || *digest != format!("blake3:{}", blake3::hash(content.as_bytes()).to_hex())
            }
            Self::Composite {
                base_revision_id,
                patch_artifact_id,
                digest,
            } => {
                base_revision_id.trim().is_empty()
                    || patch_artifact_id.trim().is_empty()
                    || !valid_blake3_digest(digest)
            }
        };
        if invalid {
            Err(EvolutionGraphError::InvalidContentIdentity)
        } else {
            Ok(())
        }
    }
}

fn valid_blake3_digest(value: &str) -> bool {
    value.strip_prefix("blake3:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

fn valid_metric_value(value: &EvolutionMetricValue) -> bool {
    match value {
        EvolutionMetricValue::Decimal(value) => {
            value.parse::<f64>().is_ok_and(|value| value.is_finite())
        }
        EvolutionMetricValue::Boolean(_)
        | EvolutionMetricValue::Integer(_)
        | EvolutionMetricValue::Text(_) => true,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionArtifact {
    pub artifact_id: String,
    pub kind: String,
    pub uri: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub media_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    pub role: EvolutionArtifactRole,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionArtifactRole {
    CandidateContent,
    EvaluationInput,
    EvaluationOutput,
    Provenance,
    AgentTrace,
    ResourceEvidence,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMetricDefinition {
    pub metric_id: String,
    pub label: String,
    pub direction: EvolutionMetricDirection,
    pub unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<EvolutionMetricValue>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionMetricDirection {
    Maximize,
    Minimize,
    Target,
    Constraint,
    Observe,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMetricObservation {
    pub metric_id: String,
    pub value: EvolutionMetricValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    pub evaluator: String,
    pub artifact_id: String,
    pub measured_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
pub enum EvolutionMetricValue {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionRevisionStatus {
    Baseline,
    Proposed,
    Evaluating,
    AwaitingDecision,
    Selected,
    Rejected,
    Failed,
    RolledBack,
    Superseded,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRevisionEdge {
    pub parent_revision_id: String,
    pub child_revision_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionTaskRecord {
    pub task_id: String,
    pub title: String,
    pub status: EvolutionTaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_task_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subgoals: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned_agent_run_id: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionTaskStatus {
    Planned,
    Ready,
    Running,
    AwaitingEvidence,
    AwaitingDecision,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionAgentRunRecord {
    pub agent_run_id: String,
    pub agent_id: String,
    pub role: EvolutionAgentRole,
    pub status: EvolutionAgentRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_ref: Option<String>,
    pub summary: String,
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionAgentRole {
    Governor,
    Proposer,
    Implementer,
    Evaluator,
    Specialist,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionAgentRunStatus {
    Queued,
    Running,
    Waiting,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionResourceRecord {
    pub record_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    pub resource: String,
    pub quantity: u64,
    pub unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monetary_microunits: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    pub confidence: String,
    pub evidence_ref: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionGraphEvent {
    pub event_id: String,
    pub kind: String,
    pub occurred_at: String,
    pub goal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    pub provenance: EvolutionEventProvenance,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caused_by_event_ids: Vec<String>,
    pub routing: EvolutionEventRouting,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionEventProvenance {
    pub source: String,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionEventRouting {
    pub scope: String,
    pub retention: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EvolutionGraphError {
    #[error("evolution goal id and objective must be non-empty")]
    InvalidGoal,
    #[error("unsupported evolution graph schema version {schema_version}")]
    UnsupportedSchema { schema_version: u32 },
    #[error("revision {revision_id} appears more than once")]
    DuplicateRevision { revision_id: String },
    #[error("revision {revision_id} refers to missing parent {parent_revision_id}")]
    MissingParent {
        revision_id: String,
        parent_revision_id: String,
    },
    #[error("revision {revision_id} cannot be its own parent")]
    SelfParent { revision_id: String },
    #[error("revision {revision_id} already exists with a different parent or content identity")]
    ImmutableRevisionConflict { revision_id: String },
    #[error("revision graph contains a cycle at {revision_id}")]
    Cycle { revision_id: String },
    #[error("revision content identity is incomplete")]
    InvalidContentIdentity,
    #[error("evolution graph contains an invalid or duplicate metric definition")]
    InvalidMetricDefinition,
    #[error("revision {revision_id} contains an invalid or duplicate artifact")]
    InvalidArtifact { revision_id: String },
    #[error("revision {revision_id} content refers to missing artifact {artifact_id}")]
    MissingContentArtifact {
        revision_id: String,
        artifact_id: String,
    },
    #[error("revision {revision_id} observes undeclared metric {metric_id}")]
    UnknownMetric {
        revision_id: String,
        metric_id: String,
    },
    #[error("revision {revision_id} metric {metric_id} refers to missing artifact {artifact_id}")]
    MissingMetricArtifact {
        revision_id: String,
        metric_id: String,
        artifact_id: String,
    },
    #[error("revision {revision_id} metric {metric_id} has an invalid value")]
    InvalidMetricValue {
        revision_id: String,
        metric_id: String,
    },
    #[error("evolution task graph contains a missing, duplicate, or invalid reference")]
    InvalidTaskGraph,
    #[error("evolution agent graph contains a missing, duplicate, or invalid reference")]
    InvalidAgentGraph,
    #[error("evolution resource records contain a missing, duplicate, or invalid reference")]
    InvalidResourceRecords,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn revision(id: &str, parents: &[&str]) -> EvolutionRevision {
        EvolutionRevision {
            revision_id: id.to_string(),
            parent_revision_ids: parents.iter().map(|parent| (*parent).to_string()).collect(),
            content: RevisionContentIdentity::GitCommit {
                repository: "/repo".to_string(),
                commit: id.to_string(),
            },
            artifacts: Vec::new(),
            metrics: Vec::new(),
            status: EvolutionRevisionStatus::Proposed,
            summary: id.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            job_id: None,
            proposed_by_agent_run_id: None,
        }
    }

    #[test]
    fn revision_graph_supports_branches_and_merge_heads() {
        let mut graph = EvolutionGoalGraph::new(
            "goal-1",
            "improve a metric",
            "evolution-agent",
            "2026-01-01T00:00:00Z",
        );
        graph.upsert_revision(revision("base", &[])).expect("base");
        graph
            .upsert_revision(revision("left", &["base"]))
            .expect("left");
        graph
            .upsert_revision(revision("right", &["base"]))
            .expect("right");
        assert_eq!(graph.head_revision_ids(), ["left", "right"]);

        graph
            .upsert_revision(revision("merge", &["left", "right"]))
            .expect("merge");
        assert_eq!(graph.head_revision_ids(), ["merge"]);
        assert_eq!(graph.edges().len(), 4);
    }

    #[test]
    fn revision_graph_rejects_missing_parents() {
        let mut graph = EvolutionGoalGraph::new(
            "goal-1",
            "improve a metric",
            "evolution-agent",
            "2026-01-01T00:00:00Z",
        );
        assert!(matches!(
            graph.upsert_revision(revision("candidate", &["missing"])),
            Err(EvolutionGraphError::MissingParent { .. })
        ));
    }

    #[test]
    fn workflow_validation_rejects_task_cycles() {
        let mut graph = EvolutionGoalGraph::new(
            "goal-1",
            "improve a metric",
            "evolution-agent",
            "2026-01-01T00:00:00Z",
        );
        for (task_id, dependency) in [("task-a", "task-b"), ("task-b", "task-a")] {
            graph.upsert_task(EvolutionTaskRecord {
                task_id: task_id.to_string(),
                title: task_id.to_string(),
                status: EvolutionTaskStatus::Ready,
                revision_id: None,
                parent_task_id: None,
                dependency_task_ids: vec![dependency.to_string()],
                subgoals: Vec::new(),
                assigned_agent_run_id: None,
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            });
        }
        assert_eq!(graph.validate(), Err(EvolutionGraphError::InvalidTaskGraph));
    }

    #[test]
    fn inline_identity_verifies_the_exact_raw_contents() {
        let content = "frozen contents";
        let valid = RevisionContentIdentity::InlineContent {
            media_type: "text/plain".to_string(),
            digest: format!("blake3:{}", blake3::hash(content.as_bytes()).to_hex()),
            content: content.to_string(),
        };
        valid.validate().expect("matching digest");
        let invalid = RevisionContentIdentity::InlineContent {
            media_type: "text/plain".to_string(),
            digest: format!("blake3:{}", "0".repeat(64)),
            content: content.to_string(),
        };
        assert_eq!(
            invalid.validate(),
            Err(EvolutionGraphError::InvalidContentIdentity)
        );
    }
}
