//! Session membership and synchronization metadata skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{DeviceId, EventId, NodeId, SessionId, SyncPolicy, TaskId};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionParticipant {
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_devices: Vec<DeviceId>,
    pub role: ParticipantRole,
    #[serde(with = "time::serde::rfc3339")]
    pub joined_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ParticipantRole {
    Origin,
    ActionTarget,
    MemoryHome,
    Observer,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionSyncState {
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub participants: Vec<SessionParticipant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<EventId>,
    pub sync_policy: SyncPolicy,
}

impl SessionSyncState {
    pub fn new(session_id: SessionId, sync_policy: SyncPolicy) -> Self {
        Self {
            session_id,
            participants: Vec::new(),
            active_task_id: None,
            last_event_id: None,
            sync_policy,
        }
    }

    pub fn add_participant(&mut self, participant: SessionParticipant) {
        if let Some(existing) = self
            .participants
            .iter_mut()
            .find(|existing| existing.node_id == participant.node_id)
        {
            *existing = participant;
        } else {
            self.participants.push(participant);
        }
    }

    pub fn participant_nodes(&self) -> BTreeSet<NodeId> {
        self.participants
            .iter()
            .map(|participant| participant.node_id.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct SyncRegistry {
    sessions: BTreeMap<SessionId, SessionSyncState>,
}

impl SyncRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_session(&mut self, state: SessionSyncState) {
        self.sessions.insert(state.session_id.clone(), state);
    }

    pub fn session(&self, session_id: &SessionId) -> Option<&SessionSyncState> {
        self.sessions.get(session_id)
    }

    pub fn add_participant(
        &mut self,
        session_id: SessionId,
        participant: SessionParticipant,
    ) -> Result<(), SyncError> {
        let state =
            self.sessions
                .get_mut(&session_id)
                .ok_or_else(|| SyncError::UnknownSession {
                    session_id: session_id.clone(),
                })?;
        state.add_participant(participant);
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SyncError {
    #[error("unknown session: {session_id}")]
    UnknownSession { session_id: SessionId },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_membership_merges_by_node() {
        let session_id = SessionId::new("sess_sync").expect("session");
        let node_id = NodeId::new("node_desktop").expect("node");
        let mut state = SessionSyncState::new(session_id, SyncPolicy::Session);
        state.add_participant(SessionParticipant {
            node_id: node_id.clone(),
            active_devices: Vec::new(),
            role: ParticipantRole::Observer,
            joined_at: OffsetDateTime::now_utc(),
        });
        state.add_participant(SessionParticipant {
            node_id,
            active_devices: Vec::new(),
            role: ParticipantRole::ActionTarget,
            joined_at: OffsetDateTime::now_utc(),
        });

        assert_eq!(state.participants.len(), 1);
        assert_eq!(state.participants[0].role, ParticipantRole::ActionTarget);
    }
}
