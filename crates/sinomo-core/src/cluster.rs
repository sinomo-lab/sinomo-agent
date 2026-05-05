use crate::{
    CapabilityName, ClusterCertificateId, ClusterEndpointId, ClusterId, ClusterInviteId, NodeId,
    TrustDomain,
};
use serde::{Deserialize, Serialize};
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
    pub cluster_id: ClusterId,
    pub cluster_name: String,
    pub cluster_epoch: ClusterEpoch,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterBootstrapState {
    pub cluster_id: ClusterId,
    pub cluster_name: String,
    pub local_node_id: NodeId,
    pub local_node_display_name: String,
    pub bootstrap: ClusterBootstrap,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<ClusterMemberRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl ClusterBootstrapState {
    pub fn initialized_locally(
        cluster_id: ClusterId,
        cluster_name: impl Into<String>,
        local_node_id: NodeId,
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
            bootstrap: ClusterBootstrap::InitializedLocally,
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
        cluster_id: ClusterId,
        cluster_name: impl Into<String>,
        local_node_id: NodeId,
        local_node_display_name: impl Into<String>,
        local_node_public_key: impl Into<String>,
        invite: ClusterJoinInvite,
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
            bootstrap: ClusterBootstrap::Joined { invite },
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
#[serde(rename_all = "kebab-case")]
pub enum ClusterBootstrap {
    InitializedLocally,
    Joined { invite: ClusterJoinInvite },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterJoinInvite {
    pub invite_id: ClusterInviteId,
    pub cluster_id: ClusterId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub introducers: Vec<ClusterEndpointRecord>,
    pub token_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub trust_domain: TrustDomain,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_member_roles: Vec<ClusterMemberRole>,
    #[serde(default = "legacy_unsigned_invite_issuer_node_id")]
    pub issuer_node_id: NodeId,
    #[serde(default)]
    pub issuer_public_key: String,
    #[serde(default)]
    pub signature: String,
}

fn legacy_unsigned_invite_issuer_node_id() -> NodeId {
    NodeId::new("node_legacy_unsigned_invite").expect("static legacy node id")
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterMemberRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub display_name: String,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub trust_domain: TrustDomain,
    pub node_public_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_id: Option<ClusterCertificateId>,
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
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub process: String,
    pub availability: ClusterNodeAvailability,
    pub daemon_version: String,
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
        cluster_id: ClusterId,
        node_id: NodeId,
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
            ClusterMemberState::Active,
            timestamp,
        )
    }

    pub fn pending_learner(
        cluster_id: ClusterId,
        node_id: NodeId,
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
        cluster_id: ClusterId,
        node_id: NodeId,
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
            ClusterMemberState::Active,
            timestamp,
        )
    }

    pub fn pending_non_voting(
        cluster_id: ClusterId,
        node_id: NodeId,
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
        cluster_id: ClusterId,
        node_id: NodeId,
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
    pub endpoint_id: ClusterEndpointId,
    pub node_id: NodeId,
    pub transport: ClusterTransportKind,
    pub address: String,
    pub source: ClusterEndpointSource,
    pub privacy: ClusterEndpointPrivacy,
    pub priority: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed_latency_ms: Option<u32>,
}

impl ClusterEndpointRecord {
    pub fn loopback(node_id: NodeId, address: impl Into<String>) -> Self {
        Self {
            endpoint_id: ClusterEndpointId::generate(),
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
    pub certificate_id: ClusterCertificateId,
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub issuer_node_id: NodeId,
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
    fn bootstrap_state_serializes_single_node_cluster() {
        let timestamp = OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::new("cluster_home").expect("cluster"),
            "home",
            NodeId::new("node_desktop").expect("node"),
            "desktop",
            "pk_desktop",
            timestamp,
        );

        let json = serde_json::to_value(&state).expect("serialize");

        assert_eq!(json["cluster_id"], "cluster_home");
        assert_eq!(json["bootstrap"], "initialized-locally");
        assert_eq!(json["members"][0]["role"], "voter");
        assert_eq!(json["members"][0]["state"], "active");
    }

    #[test]
    fn join_invite_round_trips_with_introducer_endpoint() {
        let timestamp = OffsetDateTime::parse(
            "2026-05-02T12:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("timestamp");
        let invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::new("invite_phone").expect("invite"),
            cluster_id: ClusterId::new("cluster_home").expect("cluster"),
            cluster_name: Some("home".to_string()),
            introducers: vec![ClusterEndpointRecord::loopback(
                NodeId::new("node_desktop").expect("node"),
                "127.0.0.1:21353",
            )],
            token_id: "token_1".to_string(),
            expires_at: timestamp,
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: NodeId::new("node_desktop").expect("node"),
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
