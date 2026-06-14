//! Node identity, capability, and health metadata.

mod daemon;
mod identity;

pub use daemon::*;
pub use identity::*;

pub type NodeId = sinomo_core::Id;

use serde::{Deserialize, Serialize};
use sinomo_core::{CapabilityName, ModelCapability, NodeCapability, ScalarError, TrustDomain};

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    for relative_path in ["identity/main-ed25519.sobj", "identity/shadow-ed25519.sobj"] {
        layout.register_sinomo_object(
            "sinomo-node",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new("NodeIdentity", STORAGE_CONTRACT_OBJECT_VERSION)
                .field("public_key", "String")
                .field("private_key_pkcs8", "String")
                .field("created_at", "String"),
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeProfile {
    pub node_id: NodeId,
    pub display_name: String,
    pub role: NodeRole,
    pub trust_domain: TrustDomain,
    #[serde(default)]
    pub capabilities: Vec<NodeCapability>,
    #[serde(default)]
    pub hosted_services: Vec<HostedService>,
    #[serde(default)]
    pub hosted_models: Vec<HostedModel>,
    pub resource_envelope: NodeResourceEnvelope,
    pub health: NodeHealth,
}

impl NodeProfile {
    pub fn local(display_name: impl Into<String>) -> Result<Self, ScalarError> {
        Ok(Self {
            node_id: NodeId::generate(),
            display_name: display_name.into(),
            role: NodeRole::Workstation,
            trust_domain: TrustDomain::new("trust.local")?,
            capabilities: Vec::new(),
            hosted_services: Vec::new(),
            hosted_models: Vec::new(),
            resource_envelope: NodeResourceEnvelope::default(),
            health: NodeHealth::available(),
        })
    }

    pub fn with_capability(mut self, capability: NodeCapability) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn with_service(mut self, service: HostedService) -> Self {
        self.hosted_services.push(service);
        self
    }

    pub fn with_model(mut self, model: HostedModel) -> Self {
        self.hosted_models.push(model);
        self
    }

    pub fn has_capability(&self, name: &CapabilityName) -> bool {
        self.capabilities
            .iter()
            .any(|capability| &capability.name == name)
    }

    pub fn set_health(&mut self, health: NodeHealth) {
        self.health = health;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NodeRole {
    Workstation,
    Mobile,
    Server,
    Sensor,
    Test,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostedService {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

impl HostedService {
    pub fn new(name: CapabilityName) -> Self {
        Self {
            name,
            endpoint: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostedModel {
    pub capability: ModelCapability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeResourceEnvelope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_threads: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_count: Option<u16>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeHealth {
    pub availability: NodeAvailability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl NodeHealth {
    pub fn available() -> Self {
        Self {
            availability: NodeAvailability::Available,
            message: None,
        }
    }

    pub fn degraded(message: impl Into<String>) -> Self {
        Self {
            availability: NodeAvailability::Degraded,
            message: Some(message.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NodeAvailability {
    Available,
    Degraded,
    Offline,
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::NodeCapability;

    #[test]
    fn node_profile_round_trips_json() {
        let capability_name = CapabilityName::new("storage.memory.durable").expect("capability");
        let profile = NodeProfile::local("desktop")
            .expect("profile")
            .with_capability(NodeCapability::new(capability_name.clone()));

        let json = serde_json::to_string(&profile).expect("serialize profile");
        let parsed: NodeProfile = serde_json::from_str(&json).expect("deserialize profile");

        assert_eq!(parsed.display_name, "desktop");
        assert!(parsed.has_capability(&capability_name));
    }

    #[test]
    fn health_can_be_updated() {
        let mut profile = NodeProfile::local("desktop").expect("profile");
        profile.set_health(NodeHealth::degraded("high load"));

        assert_eq!(profile.health.availability, NodeAvailability::Degraded);
    }

    #[test]
    fn node_identity_persists_main_and_shadow_keys() {
        let root = sinomo_testdir::TestDir::new("sinomo-node-identity");
        let main = load_or_create_main_node_identity(&root).expect("main identity");
        let shadow = load_or_create_shadow_node_identity(&root).expect("shadow identity");
        let main_again = load_or_create_main_node_identity(&root).expect("main identity reload");

        assert!(!main.public_key.trim().is_empty());
        assert!(!shadow.public_key.trim().is_empty());
        assert_eq!(main.public_key, main_again.public_key);
        assert_ne!(main.public_key, shadow.public_key);
        main.key_pair().expect("main key pair");
        shadow.key_pair().expect("shadow key pair");
    }

    #[test]
    fn daemon_node_helpers_are_stable() {
        let main_node = NodeId::from_legacy_named("node_test_main").expect("node");
        let shadow_node = shadow_daemon_node_id(&main_node);

        assert_eq!(shadow_node, shadow_daemon_node_id(&main_node));
        assert_eq!(shadow_node.as_str().len(), 26);
        assert!(
            main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter)
                .iter()
                .any(|capability| capability.as_str() == "cluster.kv.vote")
        );
        assert!(
            shadow_daemon_capabilities()
                .iter()
                .any(|capability| capability.as_str() == "daemon.shadow.supervise")
        );
        let loopback = daemon_loopback_endpoint_id(&main_node);
        assert_eq!(loopback, daemon_loopback_endpoint_id(&main_node));
        assert_eq!(loopback.as_str().len(), 26);
        let lan = daemon_lan_endpoint_id(&main_node, "192.168.1.5:21353");
        assert_eq!(lan, daemon_lan_endpoint_id(&main_node, "192.168.1.5:21353"));
        assert_eq!(lan.as_str().len(), 26);
    }
}
