//! Speech output and duplex control skeleton.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    Confidence, DeviceId, EventEnvelope, EventId, EventKind, LatencyClass, NodeId, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, SessionId, Source, TaskId,
};
use sinomo_runtime::device::{DevicePermission, DeviceProfile};
use std::collections::BTreeMap;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SpeechOutputRequest {
    pub request_id: TaskId,
    pub origin_node_id: NodeId,
    pub target_node_id: NodeId,
    pub target_device_id: DeviceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub text: String,
    pub allow_barge_in: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SpeechOutputRecord {
    pub request: SpeechOutputRequest,
    pub status: VoiceOutputStatus,
    pub reason: String,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum VoiceOutputStatus {
    Speaking,
    Completed,
    Interrupted,
    Blocked,
}

#[derive(Clone, Debug)]
pub struct VoiceOutputService {
    node_id: NodeId,
    speakers: Vec<DeviceProfile>,
    active_by_session: BTreeMap<SessionId, SpeechOutputRequest>,
}

impl VoiceOutputService {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            speakers: Vec::new(),
            active_by_session: BTreeMap::new(),
        }
    }

    pub fn with_speaker(mut self, speaker: DeviceProfile) -> Self {
        self.speakers.push(speaker);
        self
    }

    pub fn start(&mut self, request: SpeechOutputRequest) -> SpeechOutputRecord {
        if let Some(reason) = self.block_reason(&request) {
            return self.record(request, VoiceOutputStatus::Blocked, reason);
        }

        if let Some(session_id) = &request.session_id {
            self.active_by_session
                .insert(session_id.clone(), request.clone());
        }
        self.record(
            request,
            VoiceOutputStatus::Speaking,
            "speech output started",
        )
    }

    pub fn complete(&mut self, request: SpeechOutputRequest) -> SpeechOutputRecord {
        if let Some(session_id) = &request.session_id {
            self.active_by_session.remove(session_id);
        }
        self.record(
            request,
            VoiceOutputStatus::Completed,
            "speech output completed",
        )
    }

    pub fn speak(&mut self, request: SpeechOutputRequest) -> SpeechOutputRecord {
        let started = self.start(request.clone());
        if started.status == VoiceOutputStatus::Speaking {
            self.complete(request)
        } else {
            started
        }
    }

    pub fn barge_in(
        &mut self,
        session_id: &SessionId,
        reason: impl Into<String>,
    ) -> Option<SpeechOutputRecord> {
        let request = self.active_by_session.remove(session_id)?;
        Some(self.record(request, VoiceOutputStatus::Interrupted, reason.into()))
    }

    pub fn output_event(&self, record: &SpeechOutputRecord) -> EventEnvelope {
        let kind = match record.status {
            VoiceOutputStatus::Speaking => "voice.output.started",
            VoiceOutputStatus::Completed => "voice.output.completed",
            VoiceOutputStatus::Interrupted => "voice.output.interrupted",
            VoiceOutputStatus::Blocked => "voice.output.blocked",
        };
        let mut event = EventEnvelope::new(
            EventId::generate(),
            record.updated_at,
            self.node_id.clone(),
            Source::new("voice.output").expect("valid source"),
            EventKind::new(kind).expect("valid event kind"),
            Salience::new(if record.status == VoiceOutputStatus::Interrupted {
                0.78
            } else {
                0.5
            })
            .expect("valid salience"),
            Confidence::new(0.94).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(record.request.target_node_id.clone())
        .with_target_device(record.request.target_device_id.clone())
        .with_payload(serde_json::json!({
            "record": record,
            "output_state": record.status,
            "allow_barge_in": record.request.allow_barge_in,
            "reason": record.reason,
        }));
        if let Some(session_id) = &record.request.session_id {
            event = event.with_session(session_id.clone());
        }
        event
    }

    fn block_reason(&self, request: &SpeechOutputRequest) -> Option<String> {
        if request.target_node_id != self.node_id {
            return Some("speech output target is not local to this node".to_string());
        }
        let speaker = self
            .speakers
            .iter()
            .find(|speaker| speaker.device_id == request.target_device_id)?;
        if !speaker.belongs_to(&self.node_id)
            || !speaker.permissions.contains(&DevicePermission::EmitOutput)
        {
            return Some("target speaker does not grant EmitOutput locally".to_string());
        }
        None
    }

    fn record(
        &self,
        request: SpeechOutputRequest,
        status: VoiceOutputStatus,
        reason: impl Into<String>,
    ) -> SpeechOutputRecord {
        SpeechOutputRecord {
            request,
            status,
            reason: reason.into(),
            updated_at: OffsetDateTime::now_utc(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::device::DeviceKind;

    #[test]
    fn speech_output_completes_on_local_speaker() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let speaker = DeviceProfile::new(node.clone(), "speaker", DeviceKind::Speaker)
            .with_permission(DevicePermission::EmitOutput);
        let request = SpeechOutputRequest {
            request_id: TaskId::from_legacy_named("task_voice").expect("task"),
            origin_node_id: node.clone(),
            target_node_id: node.clone(),
            target_device_id: speaker.device_id.clone(),
            session_id: Some(SessionId::from_legacy_named("sess_voice").expect("session")),
            text: "I see the alert.".to_string(),
            allow_barge_in: true,
        };

        let mut service = VoiceOutputService::new(node).with_speaker(speaker);
        let record = service.speak(request);

        assert_eq!(record.status, VoiceOutputStatus::Completed);
        assert_eq!(
            service.output_event(&record).kind.as_str(),
            "voice.output.completed"
        );
    }

    #[test]
    fn barge_in_interrupts_active_session_output() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let session = SessionId::from_legacy_named("sess_voice").expect("session");
        let speaker = DeviceProfile::new(
            node.clone(),
            "speaker",
            sinomo_runtime::device::DeviceKind::Speaker,
        )
        .with_permission(DevicePermission::EmitOutput);
        let request = SpeechOutputRequest {
            request_id: TaskId::from_legacy_named("task_voice").expect("task"),
            origin_node_id: node.clone(),
            target_node_id: node.clone(),
            target_device_id: speaker.device_id.clone(),
            session_id: Some(session.clone()),
            text: "Long response".to_string(),
            allow_barge_in: true,
        };

        let mut service = VoiceOutputService::new(node).with_speaker(speaker);
        assert_eq!(service.start(request).status, VoiceOutputStatus::Speaking);
        let interrupted = service
            .barge_in(&session, "user started speaking")
            .expect("active");

        assert_eq!(interrupted.status, VoiceOutputStatus::Interrupted);
    }
}
