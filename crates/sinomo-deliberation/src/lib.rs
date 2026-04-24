//! Deliberation job, cancellation, and summary skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
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
        });
        job
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

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DeliberationError {
    #[error("unknown deliberation job: {job_id}")]
    UnknownJob { job_id: TaskId },
    #[error("deliberation job {job_id} has no summary")]
    MissingSummary { job_id: TaskId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::ContextPacket;

    #[test]
    fn queue_runs_and_completes_job_with_summary() {
        let trigger = EventId::new("evt_trigger").expect("event");
        let mut queue = DeliberationQueue::new();
        let job = DeliberationJobRecord::new(
            trigger.clone(),
            Some(SessionId::new("sess_delib").expect("session")),
            "inspect alert",
            ContextPacket {
                salient_events: vec![trigger],
                memory_records: Vec::new(),
            },
            Some(WorkspaceSnapshot {
                workspace_id: WorkspaceId::new("workspace_evolution_trigger").expect("workspace"),
                object_ids: vec![DfsObjectId::new("obj_trigger_candidate").expect("object")],
                source_event_id: None,
            }),
        );
        queue.enqueue(job);
        let running = queue.next_job().expect("running job");
        assert_eq!(running.job.status, JobStatus::Running);

        let completed =
            DeliberationWorker::new(NodeId::new("node_server").expect("node")).summarize(running);
        let completed = queue.complete(
            completed,
            DeliberationSummary {
                summary: "done".to_string(),
                supporting_events: Vec::new(),
                memory_promotions: Vec::new(),
            },
        );
        assert_eq!(completed.job.status, JobStatus::Completed);
        assert_eq!(queue.completed().len(), 1);
    }

    #[test]
    fn queued_job_can_be_cancelled() {
        let trigger = EventId::new("evt_trigger").expect("event");
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
                Some(EventId::new("evt_new").expect("event")),
            )
            .expect("cancel");
        assert_eq!(cancellation.reason, "superseded");
        assert_eq!(queue.queued_len(), 0);
    }
}
