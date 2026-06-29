//! Node identity, capability, and health metadata.

mod daemon;
mod identity;
mod membership;

pub use daemon::*;
pub use identity::*;
pub use membership::*;

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
    use sinomo_core::{
        ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterMemberRecord, ClusterMemberState, ClusterNodeAvailability,
        ClusterNodeStatusRecord, ClusterTransportKind, Id, NodeCapability,
    };

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

    #[test]
    fn cluster_endpoint_stable_eq_treats_mdns_and_observed_as_same_source() {
        let node_id = Id::from_legacy_named("node_endpoint_stable").expect("node");
        let endpoint_id = Id::from_legacy_named("endpoint_stable").expect("endpoint");
        let left = ClusterEndpointRecord {
            endpoint_id: endpoint_id.clone(),
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.1:21353".to_string(),
            source: ClusterEndpointSource::Mdns,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: Some(5),
        };
        let mut right = left.clone();
        right.source = ClusterEndpointSource::Observed;

        assert!(cluster_endpoint_stable_eq(&left, &right));

        right.address = "127.0.0.1:21354".to_string();
        assert!(!cluster_endpoint_stable_eq(&left, &right));
    }

    #[test]
    fn cluster_state_stable_eq_ignores_endpoint_order() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let cluster_id = Id::from_legacy_named("cluster_state_stable").expect("cluster");
        let node_id = Id::from_legacy_named("node_state_stable").expect("node");
        let mut left = ClusterBootstrapState::initialized_locally(
            cluster_id, "home", node_id, "desktop", "pk", timestamp,
        );
        let first = ClusterEndpointRecord::loopback(left.local_node_id.clone(), "127.0.0.1:21353");
        let second = ClusterEndpointRecord::loopback(left.local_node_id.clone(), "127.0.0.1:21354");
        left.members[0].endpoints = vec![first.clone(), second.clone()];
        let mut right = left.clone();
        right.members[0].endpoints = vec![second, first];

        assert!(cluster_bootstrap_state_stable_eq(&left, &right));
    }

    #[test]
    fn cluster_identity_record_stable_eq_ignores_revision_and_updated_at() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let cluster_id = Id::from_legacy_named("cluster_identity_stable").expect("cluster");
        let node_id = Id::from_legacy_named("node_identity_stable").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id, "home", node_id, "desktop", "pk", timestamp,
        );
        let left = ClusterIdentityRecord::from(&state);
        let mut right = left.clone();
        right.kv_revision += 1;
        right.updated_at = right.updated_at + time::Duration::minutes(1);

        assert!(cluster_identity_record_stable_eq(&left, &right));

        right.cluster_name = "other".to_string();
        assert!(!cluster_identity_record_stable_eq(&left, &right));
    }

    #[test]
    fn cluster_capability_policy_stable_eq_ignores_updated_at() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let cluster_id = Id::from_legacy_named("cluster_policy_stable").expect("cluster");
        let node_id = Id::from_legacy_named("node_policy_stable").expect("node");
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id,
            node_id,
            "desktop",
            "pk-policy",
            timestamp,
        );
        member.capabilities = vec![CapabilityName::new("daemon.main.run").expect("capability")];
        member.allowed_capabilities = member.capabilities.clone();
        let left = ClusterCapabilityPolicyRecord::from(&member);
        let mut right = left.clone();
        right.updated_at = right.updated_at + time::Duration::minutes(1);

        assert!(cluster_capability_policy_stable_eq(&left, &right));

        right
            .allowed_capabilities
            .push(CapabilityName::new("cluster.kv.vote").expect("capability"));
        assert!(!cluster_capability_policy_stable_eq(&left, &right));
    }

    #[test]
    fn cluster_node_key_helpers_are_stable() {
        let node_id = Id::from_legacy_named("node_policy_key").expect("node");
        let invite_id = Id::from_legacy_named("invite_policy_key").expect("invite");
        let observed_at = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let shadow_status = ClusterNodeStatusRecord {
            cluster_id: Id::from_legacy_named("cluster_status_key").expect("cluster"),
            node_id: node_id.clone(),
            process: DAEMON_SHADOW_CLUSTER_ROLE.to_string(),
            availability: ClusterNodeAvailability::Online,
            daemon_version: "0.1.0-test".to_string(),
            commit: Some("abcdef0".to_string()),
            target_os: Some("linux".to_string()),
            target_arch: Some("x86_64".to_string()),
            hostname: None,
            pid: Some(42),
            address: Some("127.0.0.1:21354".to_string()),
            bind_address: Some("0.0.0.0:21354".to_string()),
            executable: Some("/tmp/sinomo-shadow".to_string()),
            rollout: None,
            daemon_started_at: Some("2026-05-02T12:00:00Z".to_string()),
            observed_at,
        };

        assert_eq!(cluster_identity_key(), "/cluster/config/identity");
        assert_eq!(
            cluster_capability_policy_key(&node_id),
            format!("/cluster/policy/capabilities/{node_id}")
        );
        assert_eq!(
            cluster_member_key(&node_id),
            format!("/cluster/members/{node_id}")
        );
        assert_eq!(
            cluster_invitation_key(&invite_id),
            format!("/cluster/invitations/{invite_id}")
        );
        assert_eq!(
            cluster_invitation_state_key(&invite_id),
            format!("/cluster/invitations/{invite_id}/state")
        );
        assert_eq!(
            cluster_node_status_key(&node_id),
            format!("/cluster/node-status/{node_id}")
        );
        assert_eq!(
            cluster_node_status_key_for_process(&node_id, DAEMON_MAIN_CLUSTER_ROLE),
            cluster_node_status_key(&node_id)
        );
        assert_eq!(
            cluster_node_status_key_for_record(&shadow_status),
            format!("/cluster/node-status/{node_id}/daemon-shadow")
        );
        assert!(cluster_node_status_key_matches_record(
            &cluster_node_status_key_for_record(&shadow_status),
            &shadow_status
        ));
    }

    #[test]
    fn cluster_node_status_stable_eq_ignores_volatile_observation_fields() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let left = ClusterNodeStatusRecord {
            cluster_id: Id::from_legacy_named("cluster_status_stable").expect("cluster"),
            node_id: Id::from_legacy_named("node_status_stable").expect("node"),
            process: DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            availability: ClusterNodeAvailability::Online,
            daemon_version: "0.1.0-test".to_string(),
            commit: Some("abcdef0".to_string()),
            target_os: Some("linux".to_string()),
            target_arch: Some("x86_64".to_string()),
            hostname: Some("node-a".to_string()),
            pid: Some(100),
            address: Some("127.0.0.1:21353".to_string()),
            bind_address: Some("0.0.0.0:21353".to_string()),
            executable: Some("/tmp/sinomo".to_string()),
            rollout: None,
            daemon_started_at: Some("2026-05-02T12:00:00Z".to_string()),
            observed_at: timestamp,
        };
        let mut right = left.clone();
        right.pid = Some(200);
        right.observed_at = right.observed_at + time::Duration::minutes(1);

        assert!(cluster_node_status_stable_eq(&left, &right));

        right.hostname = Some("node-b".to_string());
        assert!(!cluster_node_status_stable_eq(&left, &right));

        right = left.clone();
        right.address = Some("127.0.0.1:21354".to_string());
        assert!(!cluster_node_status_stable_eq(&left, &right));
    }

    #[test]
    fn cluster_invitation_state_stable_eq_ignores_updated_at() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let left = ClusterInvitationStateRecord {
            cluster_id: Id::from_legacy_named("cluster_invitation_stable").expect("cluster"),
            invite_id: Id::from_legacy_named("invite_stable").expect("invite"),
            state: "active".to_string(),
            issuer_node_id: Id::from_legacy_named("node_invitation_issuer").expect("node"),
            expires_at: timestamp + time::Duration::hours(1),
            updated_at: timestamp,
        };
        let mut right = left.clone();
        right.updated_at = right.updated_at + time::Duration::minutes(1);

        assert!(cluster_invitation_state_stable_eq(&left, &right));

        right.state = "revoked".to_string();
        assert!(!cluster_invitation_state_stable_eq(&left, &right));
    }

    #[test]
    fn upsert_member_record_replaces_by_node_id() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let cluster_id = Id::from_legacy_named("cluster_member_upsert").expect("cluster");
        let node_id = Id::from_legacy_named("node_member_upsert").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "home",
            node_id.clone(),
            "desktop",
            "pk",
            timestamp,
        );
        let mut replacement = ClusterMemberRecord::pending_learner(
            state.cluster_id.clone(),
            node_id,
            "renamed",
            "pk-renamed",
            timestamp,
        );
        replacement.endpoints.push(ClusterEndpointRecord::loopback(
            replacement.node_id.clone(),
            "127.0.0.1:21355",
        ));

        upsert_member_record(&mut state, replacement);

        assert_eq!(state.members.len(), 1);
        assert_eq!(state.members[0].display_name, "renamed");
        assert_eq!(state.members[0].endpoints.len(), 1);
    }

    #[test]
    fn cluster_member_process_helpers_treat_main_and_shadow_as_local() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let cluster_id = Id::from_legacy_named("cluster_node_process_helpers").expect("cluster");
        let local = Id::from_legacy_named("node_process_local").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "home",
            local.clone(),
            "desktop",
            "pk-local",
            timestamp,
        );
        state.members[0].capabilities = main_daemon_capabilities(&state.members[0].role);

        let mut shadow = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            shadow_daemon_node_id(&local),
            "desktop shadow",
            "pk-shadow",
            timestamp,
        );
        shadow.capabilities = shadow_daemon_capabilities();
        state.members.push(shadow.clone());

        assert_eq!(
            cluster_member_expected_daemon_process(&state.members[0]),
            Some(DAEMON_MAIN_CLUSTER_ROLE)
        );
        assert_eq!(
            cluster_member_expected_daemon_process(&shadow),
            Some(DAEMON_SHADOW_CLUSTER_ROLE)
        );
        assert!(cluster_member_is_legacy_shadow_daemon(&shadow));
        assert_eq!(cluster_local_process_members(&state).len(), 2);
        assert!(!cluster_has_remote_peers(&state));

        let remote = Id::from_legacy_named("node_process_remote").expect("remote");
        state.members.push(ClusterMemberRecord::local_voter(
            cluster_id,
            remote.clone(),
            "remote",
            "pk-remote",
            timestamp,
        ));
        assert!(cluster_has_remote_peers(&state));

        let remote_member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == remote)
            .expect("remote member");
        remote_member.state = ClusterMemberState::Removed;
        assert!(!cluster_has_remote_peers(&state));
    }
}
