use crate::{
    CapabilityName, DeviceId, DfsObjectId, DfsVersion, EntityId, EventId, NodeId, SessionId,
    TaskId, TrustDomain, WorkspaceId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeCapability {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl NodeCapability {
    pub fn new(name: CapabilityName) -> Self {
        Self {
            name,
            description: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeviceCapability {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl DeviceCapability {
    pub fn new(name: CapabilityName) -> Self {
        Self {
            name,
            description: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModelCapability {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionState {
    pub session_id: SessionId,
    #[serde(default)]
    pub participant_nodes: Vec<NodeId>,
    #[serde(default)]
    pub active_devices: Vec<DeviceId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TaskState {
    pub task_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub status: TaskStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskStatus {
    Open,
    Waiting,
    Running,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionRequest {
    pub request_id: TaskId,
    pub origin_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_device_id: Option<DeviceId>,
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionResult {
    pub request_id: TaskId,
    pub status: ActionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<EventId>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ActionStatus {
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRecord {
    pub entity_id: EntityId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
    pub summary: String,
    pub visibility: TrustDomain,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TriggerDecision {
    pub trigger_event_id: EventId,
    pub action: TriggerAction,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerAction {
    StaySilent,
    Acknowledge,
    Interrupt,
    Alert,
    Escalate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeliberationJob {
    pub job_id: TaskId,
    pub trigger_event_id: EventId,
    pub status: JobStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum JobStatus {
    Queued,
    Running,
    Superseded,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SyncPolicy {
    LocalOnly,
    Session,
    TrustedNetwork,
    DurableGlobal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectPolicy {
    LocalOwned,
    ReplicatedConfirmed,
    ReplicatedEventual,
    AppendOnly,
    EphemeralLocal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteRequest {
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub target_version: DfsVersion,
    pub requesting_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteReceipt {
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub committed_version: DfsVersion,
    pub owner_node_id: NodeId,
    pub event_id: EventId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct HotStateSnapshot {
    #[serde(default)]
    pub recent_events: Vec<EventId>,
    #[serde(default)]
    pub active_tasks: Vec<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ContextPacket {
    #[serde(default)]
    pub salient_events: Vec<EventId>,
    #[serde(default)]
    pub memory_records: Vec<EntityId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct JobCancellation {
    pub job_id: TaskId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by_event_id: Option<EventId>,
}
