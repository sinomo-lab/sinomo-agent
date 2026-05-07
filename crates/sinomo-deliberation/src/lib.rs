//! Deliberation job, cancellation, and summary skeleton.

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelAttribution, LanguageModelMessage, LanguageModelProvider, LanguageModelRequest,
    LanguageModelResponseFormat, LanguageModelRole,
};
use sinomo_runtime::core::{
    Confidence, ContextPacket, DeliberationJob, DfsObjectId, EventEnvelope, EventId, EventKind,
    JobCancellation, JobStatus, LatencyClass, NodeId, PrivacyClass, RoutingMetadata, RoutingScope,
    Salience, SessionId, Source, TaskId, WorkspaceId,
};
use std::collections::VecDeque;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DeliberationJobRecord {
    pub job: DeliberationJob,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub objective: String,
    pub context_packet: ContextPacket,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_snapshot: Option<WorkspaceSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<JobCancellation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<DeliberationSummary>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl DeliberationJobRecord {
    pub fn new(
        trigger_event_id: EventId,
        session_id: Option<SessionId>,
        objective: impl Into<String>,
        context_packet: ContextPacket,
        workspace_snapshot: Option<WorkspaceSnapshot>,
    ) -> Self {
        Self {
            job: DeliberationJob {
                job_id: TaskId::generate(),
                trigger_event_id,
                status: JobStatus::Queued,
            },
            session_id,
            objective: objective.into(),
            context_packet,
            workspace_snapshot,
            cancellation: None,
            summary: None,
            created_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WorkspaceSnapshot {
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub object_ids: Vec<DfsObjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeliberationSummary {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supporting_events: Vec<EventId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memory_promotions: Vec<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<LanguageModelAttribution>,
}

#[derive(Clone, Debug, Default)]
pub struct DeliberationQueue {
    queued: VecDeque<DeliberationJobRecord>,
    completed: Vec<DeliberationJobRecord>,
}

impl DeliberationQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&mut self, job: DeliberationJobRecord) -> TaskId {
        let job_id = job.job.job_id.clone();
        self.queued.push_back(job);
        job_id
    }

    pub fn next_job(&mut self) -> Option<DeliberationJobRecord> {
        self.queued.pop_front().map(|mut job| {
            job.job.status = JobStatus::Running;
            job
        })
    }

    pub fn cancel(
        &mut self,
        job_id: &TaskId,
        reason: impl Into<String>,
        superseded_by_event_id: Option<EventId>,
    ) -> Result<JobCancellation, DeliberationError> {
        let position = self
            .queued
            .iter()
            .position(|job| &job.job.job_id == job_id)
            .ok_or_else(|| DeliberationError::UnknownJob {
                job_id: job_id.clone(),
            })?;
        let mut job = self.queued.remove(position).expect("position exists");
        job.job.status = JobStatus::Cancelled;
        let cancellation = JobCancellation {
            job_id: job_id.clone(),
            reason: reason.into(),
            superseded_by_event_id,
        };
        job.cancellation = Some(cancellation.clone());
        self.completed.push(job);
        Ok(cancellation)
    }

    pub fn complete(
        &mut self,
        mut job: DeliberationJobRecord,
        summary: DeliberationSummary,
    ) -> DeliberationJobRecord {
        job.job.status = JobStatus::Completed;
        job.summary = Some(summary);
        self.completed.push(job.clone());
        job
    }

    pub fn queued_len(&self) -> usize {
        self.queued.len()
    }

    pub fn completed(&self) -> &[DeliberationJobRecord] {
        &self.completed
    }
}

#[derive(Clone, Debug)]
pub struct DeliberationWorker {
    node_id: NodeId,
}

impl DeliberationWorker {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn summarize(&self, mut job: DeliberationJobRecord) -> DeliberationJobRecord {
        job.job.status = JobStatus::Completed;
        job.summary = Some(DeliberationSummary {
            summary: format!(
                "Deliberation reviewed {} salient event(s) for: {}",
                job.context_packet.salient_events.len(),
                job.objective
            ),
            supporting_events: job.context_packet.salient_events.clone(),
            memory_promotions: job.context_packet.salient_events.clone(),
            model: None,
        });
        job
    }

    pub fn summarize_with_model<P: LanguageModelProvider + ?Sized>(
        &self,
        mut job: DeliberationJobRecord,
        model_provider: &P,
        provider_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<DeliberationJobRecord, DeliberationError> {
        let request = self.model_request(&job, provider_id, model);
        let response = model_provider.complete(&request)?;
        let mut summary = parse_deliberation_summary(&response.content)?;
        summary.model = Some(LanguageModelAttribution {
            provider: response.provider,
            model: response.model,
        });
        job.job.status = JobStatus::Completed;
        job.summary = Some(summary);
        Ok(job)
    }

    pub fn model_request(
        &self,
        job: &DeliberationJobRecord,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> LanguageModelRequest {
        let context = serde_json::json!({
            "job_id": job.job.job_id,
            "trigger_event_id": job.job.trigger_event_id,
            "session_id": job.session_id,
            "objective": job.objective,
            "context_packet": job.context_packet,
            "workspace_snapshot": job.workspace_snapshot,
            "created_at": job.created_at,
        });
        LanguageModelRequest {
            provider: provider.into(),
            model: model.into(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: DELIBERATION_SYSTEM_PROMPT.to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: serde_json::to_string_pretty(&context)
                        .expect("deliberation context serializes"),
                },
            ],
            temperature: Some(0.2),
            max_output_tokens: Some(800),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({
                "loop": "deliberative",
                "node_id": self.node_id,
                "job_id": job.job.job_id,
            }),
        }
    }

    pub fn summary_event(
        &self,
        job: &DeliberationJobRecord,
    ) -> Result<EventEnvelope, DeliberationError> {
        let summary = job
            .summary
            .as_ref()
            .ok_or_else(|| DeliberationError::MissingSummary {
                job_id: job.job.job_id.clone(),
            })?;
        let mut event = EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("deliberation.worker").expect("valid source"),
            EventKind::new("deliberation.summary.completed").expect("valid kind"),
            Salience::new(0.78).expect("valid salience"),
            Confidence::new(0.86).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "job_id": job.job.job_id,
            "trigger_event_id": job.job.trigger_event_id,
            "status": job.job.status,
            "summary": summary,
            "workspace_snapshot": job.workspace_snapshot
        }));
        if let Some(session_id) = job.session_id.clone() {
            event = event.with_session(session_id);
        }
        event.links.caused_by.push(job.job.trigger_event_id.clone());
        Ok(event)
    }
}

const DELIBERATION_SYSTEM_PROMPT: &str = r#"You are Sinomo's slow deliberative loop.
You receive compact event and memory identifiers, not raw continuous media.
Produce one JSON object only:
{
  "summary": "short operator-facing result",
  "supporting_events": ["evt_..."],
  "memory_promotions": ["evt_..."]
}
Use only event ids present in context_packet.salient_events.
Treat the output as a revision, continuation, or plan; do not claim realtime control."#;

fn parse_deliberation_summary(content: &str) -> Result<DeliberationSummary, DeliberationError> {
    let content = strip_json_fence(content.trim());
    #[derive(Deserialize)]
    struct ModelSummary {
        summary: String,
        #[serde(default)]
        supporting_events: Vec<EventId>,
        #[serde(default)]
        memory_promotions: Vec<EventId>,
    }

    let summary: ModelSummary =
        serde_json::from_str(content).map_err(|source| DeliberationError::ModelResponseParse {
            detail: source.to_string(),
            content: content.to_string(),
        })?;

    Ok(DeliberationSummary {
        summary: summary.summary,
        supporting_events: summary.supporting_events,
        memory_promotions: summary.memory_promotions,
        model: None,
    })
}

fn strip_json_fence(content: &str) -> &str {
    content
        .strip_prefix("```json")
        .or_else(|| content.strip_prefix("```"))
        .and_then(|stripped| stripped.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(content)
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DeliberationError {
    #[error("unknown deliberation job: {job_id}")]
    UnknownJob { job_id: TaskId },
    #[error("deliberation job {job_id} has no summary")]
    MissingSummary { job_id: TaskId },
    #[error(transparent)]
    ModelProvider(#[from] sinomo_lm::LanguageModelError),
    #[error("failed to parse model response as deliberation summary: {detail}; content={content}")]
    ModelResponseParse { detail: String, content: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::ContextPacket;

    #[test]
    fn queue_runs_and_completes_job_with_summary() {
        let trigger = EventId::from_legacy_named("evt_trigger").expect("event");
        let mut queue = DeliberationQueue::new();
        let job = DeliberationJobRecord::new(
            trigger.clone(),
            Some(SessionId::from_legacy_named("sess_delib").expect("session")),
            "inspect alert",
            ContextPacket {
                salient_events: vec![trigger],
                memory_records: Vec::new(),
            },
            Some(WorkspaceSnapshot {
                workspace_id: WorkspaceId::from_legacy_named("workspace_evolution_trigger")
                    .expect("workspace"),
                object_ids: vec![
                    DfsObjectId::from_legacy_named("obj_trigger_candidate").expect("object"),
                ],
                source_event_id: None,
            }),
        );
        queue.enqueue(job);
        let running = queue.next_job().expect("running job");
        assert_eq!(running.job.status, JobStatus::Running);

        let completed =
            DeliberationWorker::new(NodeId::from_legacy_named("node_server").expect("node"))
                .summarize(running);
        let completed = queue.complete(
            completed,
            DeliberationSummary {
                summary: "done".to_string(),
                supporting_events: Vec::new(),
                memory_promotions: Vec::new(),
                model: None,
            },
        );
        assert_eq!(completed.job.status, JobStatus::Completed);
        assert_eq!(queue.completed().len(), 1);
    }

    #[test]
    fn queued_job_can_be_cancelled() {
        let trigger = EventId::from_legacy_named("evt_trigger").expect("event");
        let mut queue = DeliberationQueue::new();
        let job = DeliberationJobRecord::new(
            trigger,
            None,
            "obsolete work",
            ContextPacket::default(),
            None,
        );
        let job_id = queue.enqueue(job);
        let cancellation = queue
            .cancel(
                &job_id,
                "superseded",
                Some(EventId::from_legacy_named("evt_new").expect("event")),
            )
            .expect("cancel");
        assert_eq!(cancellation.reason, "superseded");
        assert_eq!(queue.queued_len(), 0);
    }

    #[test]
    fn worker_builds_structured_model_request() {
        let trigger = EventId::from_legacy_named("evt_trigger").expect("event");
        let worker =
            DeliberationWorker::new(NodeId::from_legacy_named("node_server").expect("node"));
        let job = DeliberationJobRecord::new(
            trigger.clone(),
            Some(SessionId::from_legacy_named("sess_delib").expect("session")),
            "inspect alert",
            ContextPacket {
                salient_events: vec![trigger],
                memory_records: Vec::new(),
            },
            None,
        );

        let request = worker.model_request(&job, "command", "test-model");

        assert_eq!(request.provider, "command");
        assert_eq!(request.model, "test-model");
        assert_eq!(
            request.response_format,
            LanguageModelResponseFormat::JsonObject
        );
        assert_eq!(request.messages.len(), 2);
        assert!(request.messages[1].content.contains("inspect alert"));
    }

    #[test]
    fn worker_can_complete_job_with_model_summary() {
        let trigger = EventId::from_legacy_named("evt_trigger").expect("event");
        let worker =
            DeliberationWorker::new(NodeId::from_legacy_named("node_server").expect("node"));
        let job = DeliberationJobRecord::new(
            trigger.clone(),
            Some(SessionId::from_legacy_named("sess_delib").expect("session")),
            "inspect alert",
            ContextPacket {
                salient_events: vec![trigger.clone()],
                memory_records: Vec::new(),
            },
            None,
        );
        let client = FixtureLanguageModelClient {
            content: serde_json::json!({
                "summary": "The alert is worth a slower follow-up.",
                "supporting_events": [trigger],
                "memory_promotions": [trigger]
            })
            .to_string(),
        };

        let completed = worker
            .summarize_with_model(job, &client, "fixture", "deliberator")
            .expect("model summary");

        let summary = completed.summary.expect("summary");
        assert_eq!(completed.job.status, JobStatus::Completed);
        assert_eq!(summary.summary, "The alert is worth a slower follow-up.");
        assert_eq!(
            summary.model,
            Some(LanguageModelAttribution {
                provider: "fixture".to_string(),
                model: "deliberator".to_string()
            })
        );
    }

    struct FixtureLanguageModelClient {
        content: String,
    }

    impl LanguageModelProvider for FixtureLanguageModelClient {
        fn info(&self) -> sinomo_lm::LanguageModelProviderInfo {
            sinomo_lm::LanguageModelProviderInfo {
                provider: "fixture".to_string(),
                default_model: "deliberator".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::InProcess,
                supports_json_object: true,
                supports_tool_permissions: false,
            }
        }

        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<sinomo_lm::LanguageModelResponse, sinomo_lm::LanguageModelError> {
            Ok(sinomo_lm::LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: self.content.clone(),
                usage: None,
            })
        }
    }
}
