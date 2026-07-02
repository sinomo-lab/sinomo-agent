use crate::maintenance::CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS;
use crate::prelude::*;
use sinomo_node::cluster_endpoint_list_stable_eq;

pub const CLUSTER_NETWORK_INFO_TTL_SECONDS: i64 = 3600;

/// Whether a network-domain record should be republished because it is close
/// enough to expiry that a stable record still needs a TTL refresh.
pub fn cluster_network_record_should_refresh(
    expires_at: OffsetDateTime,
    now: OffsetDateTime,
) -> bool {
    expires_at <= now + time::Duration::seconds(CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkInfoRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub environment_id: String,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub observed_addresses: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub endpoints: Vec<ClusterEndpointRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

pub fn build_cluster_network_info_record(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    observed_at: OffsetDateTime,
    ttl_seconds: i64,
) -> ClusterNetworkInfoRecord {
    let endpoints = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.endpoints.clone())
        .unwrap_or_default();
    let observed_addresses = cluster_network_observed_addresses(&endpoints);
    ClusterNetworkInfoRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: node_id.clone(),
        environment_id: cluster_network_environment_id(&observed_addresses),
        observed_addresses,
        endpoints,
        observed_at,
        expires_at: observed_at + time::Duration::seconds(ttl_seconds),
    }
}

fn cluster_network_observed_addresses(endpoints: &[ClusterEndpointRecord]) -> Vec<String> {
    let mut observed_addresses = endpoints
        .iter()
        .filter(|endpoint| {
            matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::LanQuic
                    | ClusterTransportKind::Iroh
                    | ClusterTransportKind::WebRtc
                    | ClusterTransportKind::TurnRelay
                    | ClusterTransportKind::HttpsRelay
            )
        })
        .map(|endpoint| endpoint.address.clone())
        .collect::<Vec<_>>();
    observed_addresses.sort();
    observed_addresses.dedup();
    observed_addresses
}

fn cluster_network_environment_id(values: &[String]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for value in values {
        for byte in value.as_bytes().iter().chain(std::iter::once(&0)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("env_{hash:016x}")
}

pub fn cluster_network_info_publish_record_matches(
    known: &ClusterNetworkInfoRecord,
    info: &ClusterNetworkInfoRecord,
    now: OffsetDateTime,
) -> bool {
    cluster_network_info_stable_eq(known, info)
        && !cluster_network_record_should_refresh(known.expires_at, now)
}

pub fn cluster_network_info_stable_eq(
    left: &ClusterNetworkInfoRecord,
    right: &ClusterNetworkInfoRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.environment_id == right.environment_id
        && left.observed_addresses == right.observed_addresses
        && cluster_endpoint_list_stable_eq(&left.endpoints, &right.endpoints)
}
