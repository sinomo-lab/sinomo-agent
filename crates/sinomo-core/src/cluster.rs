use crate::{CapabilityName, Id, TrustDomain, domain::RolloutPlan};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct ClusterEpoch(pub u64);

impl ClusterEpoch {
    pub fn initial() -> Self {
        Self(1)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterIdentity {
    pub cluster_id: Id,
    pub cluster_name: String,
    pub cluster_epoch: ClusterEpoch,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterBootstrapState {
    pub cluster_id: Id,
    pub cluster_name: String,
    pub local_node_id: Id,
    pub local_node_display_name: String,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<ClusterMemberRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutDesiredRecord {
    pub cluster_id: Id,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub source_bundle: Option<Vec<u8>>,
    #[serde(default)]
    pub force_source_overwrite: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_repo: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterModuleRolloutStatusRecord {
    pub cluster_id: Id,
    pub node_id: Id,
    pub rollout_plan_id: Id,
    pub module_id: Id,
    pub artifact_id: Id,
    pub stage: String,
    pub current_weight_bps: u16,
    pub active_artifact_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<Id>,
    pub hot_swap_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry_last_consumed_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_record_id: Option<Id>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterModuleRolloutDesiredRecord {
    pub cluster_id: Id,
    pub state: String,
    pub rollout_plan: RolloutPlan,
    pub requested_by_node_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_material_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterModuleRolloutReconciliationRecord {
    pub cluster_id: Id,
    pub node_id: Id,
    pub rollout_plan_id: Id,
    pub module_id: Id,
    pub artifact_id: Id,
    pub state: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_material_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_runtime_stage: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

pub fn cluster_rollout_desired_key() -> String {
    "/cluster/rollout/desired".to_string()
}

pub fn cluster_module_rollout_node_prefix() -> String {
    "/cluster/module-rollouts/nodes/".to_string()
}

pub fn cluster_module_rollout_node_key(node_id: &Id, module_id: &Id) -> String {
    format!(
        "{}{node_id}/{module_id}",
        cluster_module_rollout_node_prefix()
    )
}

pub fn cluster_module_rollout_desired_prefix() -> String {
    "/cluster/module-rollouts/desired/".to_string()
}

pub fn cluster_module_rollout_desired_key(module_id: &Id, rollout_plan_id: &Id) -> String {
    format!(
        "{}{module_id}/{rollout_plan_id}",
        cluster_module_rollout_desired_prefix()
    )
}

pub fn cluster_module_rollout_reconcile_prefix() -> String {
    "/cluster/module-rollouts/reconcile/".to_string()
}

pub fn cluster_module_rollout_reconcile_key(
    node_id: &Id,
    module_id: &Id,
    rollout_plan_id: &Id,
) -> String {
    format!(
        "{}{node_id}/{module_id}/{rollout_plan_id}",
        cluster_module_rollout_reconcile_prefix()
    )
}

impl ClusterBootstrapState {
    pub fn initialized_locally(
        cluster_id: Id,
        cluster_name: impl Into<String>,
        local_node_id: Id,
        local_node_display_name: impl Into<String>,
        local_node_public_key: impl Into<String>,
        timestamp: OffsetDateTime,
    ) -> Self {
        let cluster_name = cluster_name.into();
        let local_node_display_name = local_node_display_name.into();
        let local_node_public_key = local_node_public_key.into();
        Self {
            cluster_id: cluster_id.clone(),
            cluster_name: cluster_name.clone(),
            local_node_id: local_node_id.clone(),
            local_node_display_name: local_node_display_name.clone(),
            term: 1,
            kv_revision: 1,
            members: vec![ClusterMemberRecord::local_voter(
                cluster_id,
                local_node_id,
                local_node_display_name,
                local_node_public_key,
                timestamp,
            )],
            initialized_at: timestamp,
            updated_at: timestamp,
        }
    }

    pub fn pending_join(
        cluster_id: Id,
        cluster_name: impl Into<String>,
        local_node_id: Id,
        local_node_display_name: impl Into<String>,
        local_node_public_key: impl Into<String>,
        timestamp: OffsetDateTime,
    ) -> Self {
        let cluster_name = cluster_name.into();
        let local_node_display_name = local_node_display_name.into();
        let local_node_public_key = local_node_public_key.into();
        Self {
            cluster_id: cluster_id.clone(),
            cluster_name: cluster_name.clone(),
            local_node_id: local_node_id.clone(),
            local_node_display_name: local_node_display_name.clone(),
            term: 0,
            kv_revision: 0,
            members: vec![ClusterMemberRecord::pending_learner(
                cluster_id,
                local_node_id,
                local_node_display_name,
                local_node_public_key,
                timestamp,
            )],
            initialized_at: timestamp,
            updated_at: timestamp,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterJoinInvite {
    pub invite_id: Id,
    pub cluster_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(default)]
    pub cluster_public_key: String,
    #[serde(default)]
    pub invitation_secret: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub communication_channels: Vec<ClusterInvitationChannel>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub introducers: Vec<ClusterEndpointRecord>,
    #[serde(default)]
    pub token_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub trust_domain: TrustDomain,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_member_roles: Vec<ClusterMemberRole>,
    pub issuer_node_id: Id,
    #[serde(default)]
    pub issuer_public_key: String,
    #[serde(default)]
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterInvitationChannel {
    pub endpoint: ClusterEndpointRecord,
    pub node_public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterMemberRecord {
    pub cluster_id: Id,
    pub node_id: Id,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "ClusterNodeRole::is_default")]
    pub node_role: ClusterNodeRole,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub trust_domain: TrustDomain,
    pub node_public_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<ClusterEndpointRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub joined_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterNodeStatusRecord {
    pub cluster_id: Id,
    pub node_id: Id,
    pub process: String,
    pub availability: ClusterNodeAvailability,
    pub daemon_version: String,
    /// Short git commit the daemon binary was built from. Lets nodes be told apart
    /// when the package version (e.g. 0.1.0) is not bumped per build. Absent for
    /// nodes running older builds that predate this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<ClusterNodeRolloutStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daemon_started_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterNodeAvailability {
    Online,
    Offline,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterNodeRolloutStatus {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_current: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_total: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_log: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_log: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
}

impl ClusterMemberRecord {
    pub fn local_voter(
        cluster_id: Id,
        node_id: Id,
        display_name: impl Into<String>,
        node_public_key: impl Into<String>,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self::new(
            cluster_id,
            node_id,
            display_name,
            node_public_key,
            ClusterMemberRole::Voter,
            ClusterMemberState::Voter,
            timestamp,
        )
    }

    pub fn pending_learner(
        cluster_id: Id,
        node_id: Id,
        display_name: impl Into<String>,
        node_public_key: impl Into<String>,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self::new(
            cluster_id,
            node_id,
            display_name,
            node_public_key,
            ClusterMemberRole::Learner,
            ClusterMemberState::Pending,
            timestamp,
        )
    }

    pub fn local_non_voting(
        cluster_id: Id,
        node_id: Id,
        display_name: impl Into<String>,
        node_public_key: impl Into<String>,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self::new(
            cluster_id,
            node_id,
            display_name,
            node_public_key,
            ClusterMemberRole::NonVoting,
            ClusterMemberState::NonVoting,
            timestamp,
        )
    }

    pub fn pending_non_voting(
        cluster_id: Id,
        node_id: Id,
        display_name: impl Into<String>,
        node_public_key: impl Into<String>,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self::new(
            cluster_id,
            node_id,
            display_name,
            node_public_key,
            ClusterMemberRole::NonVoting,
            ClusterMemberState::Pending,
            timestamp,
        )
    }

    fn new(
        cluster_id: Id,
        node_id: Id,
        display_name: impl Into<String>,
        node_public_key: impl Into<String>,
        role: ClusterMemberRole,
        state: ClusterMemberState,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self {
            cluster_id,
            node_id,
            display_name: display_name.into(),
            node_role: ClusterNodeRole::default(),
            role,
            state,
            trust_domain: TrustDomain::new("trust.local").expect("static trust domain"),
            node_public_key: node_public_key.into(),
            certificate_id: None,
            capabilities: Vec::new(),
            allowed_capabilities: Vec::new(),
            endpoints: Vec::new(),
            joined_at: timestamp,
            updated_at: timestamp,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterNodeRole {
    #[default]
    Workstation,
    Mobile,
    Server,
    Sensor,
    Test,
}

impl ClusterNodeRole {
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Workstation)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterMemberRole {
    Voter,
    Learner,
    NonVoting,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterMemberState {
    Pending,
    Active,
    Learner,
    Voter,
    NonVoting,
    Suspect,
    Offline,
    Removed,
}

impl ClusterMemberState {
    pub fn is_authorized(&self) -> bool {
        !matches!(self, Self::Pending | Self::Removed)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterEndpointRecord {
    pub endpoint_id: Id,
    pub node_id: Id,
    pub transport: ClusterTransportKind,
    pub address: String,
    pub source: ClusterEndpointSource,
    pub privacy: ClusterEndpointPrivacy,
    pub priority: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed_latency_ms: Option<u32>,
}

impl ClusterEndpointRecord {
    pub fn loopback(node_id: Id, address: impl Into<String>) -> Self {
        Self {
            endpoint_id: Id::generate(),
            node_id,
            transport: ClusterTransportKind::Loopback,
            address: address.into(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::LocalOnly,
            priority: 0,
            last_observed_latency_ms: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterTransportKind {
    InProcess,
    Loopback,
    LanTcp,
    LanQuic,
    Iroh,
    WebRtc,
    TurnRelay,
    HttpsRelay,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterEndpointSource {
    Static,
    Mdns,
    Ice,
    Rendezvous,
    KvMembership,
    Observed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterEndpointPrivacy {
    LocalOnly,
    Lan,
    InternetDirect,
    Relayed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterCertificate {
    pub certificate_id: Id,
    pub cluster_id: Id,
    pub node_id: Id,
    pub issuer_node_id: Id,
    pub public_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_scope: Vec<CapabilityName>,
    #[serde(with = "time::serde::rfc3339")]
    pub issued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub signature: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_state_serializes_single_node_cluster() {
        let timestamp = OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_home").expect("cluster"),
            "home",
            Id::from_legacy_named("node_desktop").expect("node"),
            "desktop",
            "pk_desktop",
            timestamp,
        );

        let json = serde_json::to_value(&state).expect("serialize");

        assert_eq!(json["cluster_id"].as_str().expect("cluster id").len(), 26);
        assert!(
            !json["cluster_id"]
                .as_str()
                .expect("cluster id")
                .contains('_')
        );
        assert!(json.get("bootstrap").is_none());
        assert!(json["members"][0].get("node_role").is_none());
        assert_eq!(json["members"][0]["role"], "voter");
        assert_eq!(json["members"][0]["state"], "voter");
    }

    #[test]
    fn join_invite_round_trips_with_introducer_endpoint() {
        let timestamp = OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let invite = ClusterJoinInvite {
            invite_id: Id::from_legacy_named("invite_phone").expect("invite"),
            cluster_id: Id::from_legacy_named("cluster_home").expect("cluster"),
            cluster_name: Some("home".to_string()),
            cluster_public_key: "pk_desktop".to_string(),
            invitation_secret: "secret_1".to_string(),
            communication_channels: vec![ClusterInvitationChannel {
                endpoint: ClusterEndpointRecord::loopback(
                    Id::from_legacy_named("node_desktop").expect("node"),
                    "127.0.0.1:21353",
                ),
                node_public_key: "pk_desktop".to_string(),
            }],
            introducers: vec![ClusterEndpointRecord::loopback(
                Id::from_legacy_named("node_desktop").expect("node"),
                "127.0.0.1:21353",
            )],
            token_id: "token_1".to_string(),
            expires_at: timestamp,
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: Id::from_legacy_named("node_desktop").expect("node"),
            issuer_public_key: "pk_desktop".to_string(),
            signature: "sig".to_string(),
        };

        let json = serde_json::to_string(&invite).expect("serialize");
        let parsed: ClusterJoinInvite = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.cluster_id, invite.cluster_id);
        assert_eq!(
            parsed.introducers[0].transport,
            ClusterTransportKind::Loopback
        );
    }
}
