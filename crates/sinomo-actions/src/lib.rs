//! Sandbox-aware action execution skeleton.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    ActionRequest, Confidence, EventEnvelope, EventId, EventKind, LatencyClass, NodeId,
    PrivacyClass, RoutingMetadata, RoutingScope, Salience, SessionId, Source, TaskId,
};
use sinomo_runtime::device::{DevicePermission, DeviceProfile};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionSandboxPolicy {
    pub local_node_id: NodeId,
    #[serde(default)]
    pub allowed_action_kinds: Vec<String>,
    pub kill_switch_engaged: bool,
}

impl ActionSandboxPolicy {
    pub fn local(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            allowed_action_kinds: Vec::new(),
            kill_switch_engaged: false,
        }
    }

    pub fn allow_kind(mut self, kind: impl Into<String>) -> Self {
        self.allowed_action_kinds.push(kind.into());
        self
    }

    pub fn with_kill_switch(mut self, engaged: bool) -> Self {
        self.kill_switch_engaged = engaged;
        self
    }

    fn allows_kind(&self, kind: &str) -> bool {
        self.allowed_action_kinds
            .iter()
            .any(|allowed| allowed == kind)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionExecutionRecord {
    pub request: ActionRequest,
    pub status: ActionExecutionStatus,
    pub decision: ActionPolicyDecision,
    pub executing_node_id: NodeId,
    pub reason: String,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ActionExecutionStatus {
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ActionPolicyDecision {
    Allowed,
    BlockedLocalAuthority,
    BlockedDevicePermission,
    BlockedActionKind,
    BlockedKillSwitch,
}

#[derive(Clone, Debug)]
pub struct ActionExecutor {
    policy: ActionSandboxPolicy,
    devices: Vec<DeviceProfile>,
}

impl ActionExecutor {
    pub fn new(policy: ActionSandboxPolicy) -> Self {
        Self {
            policy,
            devices: Vec::new(),
        }
    }

    pub fn with_device(mut self, device: DeviceProfile) -> Self {
        self.devices.push(device);
        self
    }

    pub fn execute(&self, request: ActionRequest) -> ActionExecutionRecord {
        let started_at = OffsetDateTime::now_utc();
        let (status, decision, reason) = self.enforce(&request);
        ActionExecutionRecord {
            request,
            status,
            decision,
            executing_node_id: self.policy.local_node_id.clone(),
            reason,
            started_at,
            completed_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn execution_event(
        &self,
        record: &ActionExecutionRecord,
        session_id: Option<SessionId>,
    ) -> EventEnvelope {
        let blocked = record.status == ActionExecutionStatus::Blocked;
        let kind = if blocked {
            "action.sandbox.blocked"
        } else {
            "action.execution.completed"
        };
        let mut event = EventEnvelope::new(
            EventId::generate(),
            record.completed_at,
            self.policy.local_node_id.clone(),
            Source::new("actions.executor").expect("valid source"),
            EventKind::new(kind).expect("valid event kind"),
            Salience::new(if blocked { 0.88 } else { 0.55 }).expect("valid salience"),
            Confidence::new(0.95).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "record": record,
            "requesting_node_id": record.request.origin_node_id,
            "executing_node_id": record.executing_node_id,
            "target_node_id": record.request.target_node_id,
            "target_device_id": record.request.target_device_id,
            "policy_decision": record.decision,
            "reason": record.reason,
        }));

        if let Some(session_id) = session_id {
            event = event.with_session(session_id);
        }
        if let Some(target_node_id) = &record.request.target_node_id {
            event = event.with_target_node(target_node_id.clone());
        }
        if let Some(target_device_id) = &record.request.target_device_id {
            event = event.with_target_device(target_device_id.clone());
        }
        event
    }

    pub fn cancellation_event(
        &self,
        request_id: TaskId,
        reason: impl Into<String>,
        session_id: Option<SessionId>,
    ) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.policy.local_node_id.clone(),
            Source::new("actions.executor").expect("valid source"),
            EventKind::new("action.execution.cancelled").expect("valid event kind"),
            Salience::new(0.74).expect("valid salience"),
            Confidence::new(0.94).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "request_id": request_id,
            "reason": reason.into(),
        }));
        if let Some(session_id) = session_id {
            event = event.with_session(session_id);
        }
        event
    }

    fn enforce(
        &self,
        request: &ActionRequest,
    ) -> (ActionExecutionStatus, ActionPolicyDecision, String) {
        if self.policy.kill_switch_engaged {
            return (
                ActionExecutionStatus::Blocked,
                ActionPolicyDecision::BlockedKillSwitch,
                "action kill switch is engaged".to_string(),
            );
        }

        if request.target_node_id.as_ref() != Some(&self.policy.local_node_id) {
            return (
                ActionExecutionStatus::Blocked,
                ActionPolicyDecision::BlockedLocalAuthority,
                "target node is not owned by this executor".to_string(),
            );
        }

        if !self.policy.allows_kind(&request.kind) {
            return (
                ActionExecutionStatus::Blocked,
                ActionPolicyDecision::BlockedActionKind,
                format!("action kind {} is not allowed", request.kind),
            );
        }

        let Some(target_device_id) = &request.target_device_id else {
            return (
                ActionExecutionStatus::Blocked,
                ActionPolicyDecision::BlockedDevicePermission,
                "action has no target device".to_string(),
            );
        };
        let Some(device) = self
            .devices
            .iter()
            .find(|device| &device.device_id == target_device_id)
        else {
            return (
                ActionExecutionStatus::Blocked,
                ActionPolicyDecision::BlockedDevicePermission,
                "target device is not registered with this executor".to_string(),
            );
        };

        if !device.belongs_to(&self.policy.local_node_id)
            || !device
                .permissions
                .contains(&DevicePermission::ExecuteAction)
        {
            return (
                ActionExecutionStatus::Blocked,
                ActionPolicyDecision::BlockedDevicePermission,
                "target device does not grant ExecuteAction locally".to_string(),
            );
        }

        (
            ActionExecutionStatus::Succeeded,
            ActionPolicyDecision::Allowed,
            "action executed by local sandbox skeleton".to_string(),
        )
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ActionError {
    #[error("action request {request_id} is not known")]
    UnknownRequest { request_id: TaskId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{DeviceCapability, DeviceId};
    use sinomo_runtime::device::DeviceKind;

    #[test]
    fn local_action_executes_when_kind_and_device_are_allowed() {
        let node = NodeId::new("node_desktop").expect("node");
        let device = DeviceProfile::new(node.clone(), "screen", DeviceKind::Screen)
            .with_capability(DeviceCapability::new(
                sinomo_runtime::core::CapabilityName::new("action.desktop.inspect").expect("cap"),
            ))
            .with_permission(DevicePermission::ExecuteAction);
        let request = ActionRequest {
            request_id: TaskId::new("task_action").expect("task"),
            origin_node_id: node.clone(),
            target_node_id: Some(node.clone()),
            target_device_id: Some(device.device_id.clone()),
            kind: "ui.inspect.alert".to_string(),
            payload: serde_json::json!({ "alert": true }),
        };

        let executor =
            ActionExecutor::new(ActionSandboxPolicy::local(node).allow_kind("ui.inspect.alert"))
                .with_device(device);
        let record = executor.execute(request);

        assert_eq!(record.status, ActionExecutionStatus::Succeeded);
        assert_eq!(record.decision, ActionPolicyDecision::Allowed);
    }

    #[test]
    fn remote_target_is_blocked_by_local_authority() {
        let local = NodeId::new("node_desktop").expect("node");
        let remote = NodeId::new("node_mobile").expect("node");
        let request = ActionRequest {
            request_id: TaskId::new("task_remote").expect("task"),
            origin_node_id: remote.clone(),
            target_node_id: Some(remote),
            target_device_id: Some(DeviceId::new("device_remote").expect("device")),
            kind: "ui.inspect.alert".to_string(),
            payload: serde_json::Value::Null,
        };

        let executor =
            ActionExecutor::new(ActionSandboxPolicy::local(local).allow_kind("ui.inspect.alert"));
        let record = executor.execute(request);
        let event = executor.execution_event(&record, None);

        assert_eq!(record.status, ActionExecutionStatus::Blocked);
        assert_eq!(record.decision, ActionPolicyDecision::BlockedLocalAuthority);
        assert_eq!(event.kind.as_str(), "action.sandbox.blocked");
    }
}
