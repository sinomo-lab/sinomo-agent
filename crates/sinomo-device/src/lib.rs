//! Device profile, permission, and capability metadata.

mod native;

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::{CapabilityName, DeviceCapability};

pub use native::*;

pub type DeviceId = sinomo_core::Id;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeviceProfile {
    pub device_id: DeviceId,
    pub node_id: NodeId,
    pub display_name: String,
    pub kind: DeviceKind,
    #[serde(default)]
    pub capabilities: Vec<DeviceCapability>,
    #[serde(default)]
    pub permissions: Vec<DevicePermission>,
    pub state: DeviceState,
}

impl DeviceProfile {
    pub fn new(node_id: NodeId, display_name: impl Into<String>, kind: DeviceKind) -> Self {
        Self {
            device_id: DeviceId::generate(),
            node_id,
            display_name: display_name.into(),
            kind,
            capabilities: Vec::new(),
            permissions: Vec::new(),
            state: DeviceState::available(),
        }
    }

    pub fn with_device_id(mut self, device_id: DeviceId) -> Self {
        self.device_id = device_id;
        self
    }

    pub fn mock_microphone(node_id: NodeId) -> Self {
        Self::new(node_id, "mock microphone", DeviceKind::Microphone)
    }

    pub fn mock_screen(node_id: NodeId) -> Self {
        Self::new(node_id, "mock screen", DeviceKind::Screen)
    }

    pub fn mobile_location(node_id: NodeId) -> Self {
        Self::new(node_id, "mobile location", DeviceKind::Location)
    }

    pub fn mobile_touchscreen(node_id: NodeId) -> Self {
        Self::new(node_id, "mobile touchscreen", DeviceKind::Touchscreen)
    }

    pub fn with_capability(mut self, capability: DeviceCapability) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn with_permission(mut self, permission: DevicePermission) -> Self {
        self.permissions.push(permission);
        self
    }

    pub fn belongs_to(&self, node_id: &NodeId) -> bool {
        &self.node_id == node_id
    }

    pub fn has_capability(&self, name: &CapabilityName) -> bool {
        self.capabilities
            .iter()
            .any(|capability| &capability.name == name)
    }

    pub fn source(&self, source: impl Into<String>) -> DeviceSource {
        DeviceSource {
            node_id: self.node_id.clone(),
            device_id: self.device_id.clone(),
            source: source.into(),
        }
    }

    pub fn action_target(&self, target: impl Into<String>) -> ActionTarget {
        ActionTarget {
            node_id: self.node_id.clone(),
            device_id: self.device_id.clone(),
            target: target.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DeviceKind {
    Microphone,
    Camera,
    Screen,
    Keyboard,
    Mouse,
    Filesystem,
    Gpu,
    Speaker,
    NotificationSurface,
    Location,
    Touchscreen,
    MotionSensor,
    Battery,
    Haptic,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DevicePermission {
    Observe,
    CaptureRaw,
    EmitOutput,
    ExecuteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeviceState {
    pub availability: DeviceAvailability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl DeviceState {
    pub fn available() -> Self {
        Self {
            availability: DeviceAvailability::Available,
            message: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DeviceAvailability {
    Available,
    Busy,
    PermissionDenied,
    Offline,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeviceSource {
    pub node_id: NodeId,
    pub device_id: DeviceId,
    pub source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionTarget {
    pub node_id: NodeId,
    pub device_id: DeviceId,
    pub target: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{CapabilityName, DeviceCapability};

    #[test]
    fn device_profile_round_trips_json() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node id");
        let capability_name = CapabilityName::new("input.audio.vad").expect("capability");
        let device = DeviceProfile::mock_microphone(node_id.clone())
            .with_capability(DeviceCapability::new(capability_name.clone()))
            .with_permission(DevicePermission::Observe);

        let json = serde_json::to_string(&device).expect("serialize device");
        let parsed: DeviceProfile = serde_json::from_str(&json).expect("deserialize device");

        assert!(parsed.belongs_to(&node_id));
        assert!(parsed.has_capability(&capability_name));
    }

    #[test]
    fn device_source_carries_provenance() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node id");
        let device = DeviceProfile::mock_screen(node_id.clone());
        let source = device.source("screen.main");

        assert_eq!(source.node_id, node_id);
        assert_eq!(source.device_id, device.device_id);
    }

    #[test]
    fn mobile_device_kinds_round_trip_json() {
        for kind in [
            DeviceKind::Location,
            DeviceKind::Touchscreen,
            DeviceKind::MotionSensor,
            DeviceKind::Battery,
            DeviceKind::Haptic,
        ] {
            let json = serde_json::to_string(&kind).expect("serialize mobile kind");
            let parsed: DeviceKind = serde_json::from_str(&json).expect("deserialize mobile kind");
            assert_eq!(parsed, kind);
        }
    }
}
