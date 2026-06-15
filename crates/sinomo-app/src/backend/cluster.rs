//! Cluster topology snapshot used by the graphical Cluster tab.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
pub use sinomo_cli::app_api::{AppClusterInvite, AppClusterStatus};
use sinomo_runtime::core::{ClusterEndpointPrivacy, ClusterNodeAvailability};
use sinomo_runtime::network::{
    self, ClusterConnectivityRecord, ClusterConnectivityState, ClusterHealthRow, ClusterPaths,
};
use time::OffsetDateTime;

use crate::backend::{Backend, Connection};

const CLUSTER_SNAPSHOT_RPC_TIMEOUT: Duration = Duration::from_millis(1200);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub cluster_id: Option<String>,
    pub local_node_id: Option<String>,
    pub source: String,
    pub observed_at: String,
    pub nodes: Vec<ClusterNodeInfo>,
    pub links: Vec<ClusterLinkInfo>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    pub node_id: String,
    pub display_name: String,
    pub process: String,
    pub node_role: String,
    pub member_role: String,
    pub member_state: String,
    pub availability: String,
    pub daemon_version: String,
    pub commit: Option<String>,
    pub platform: Option<String>,
    pub pid: Option<u32>,
    pub address: Option<String>,
    pub bind_address: Option<String>,
    pub rollout: Option<String>,
    pub observed_at: String,
    pub public_reachable: bool,
    pub endpoints: Vec<ClusterEndpointInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterEndpointInfo {
    pub endpoint_id: String,
    pub transport: String,
    pub address: String,
    pub source: String,
    pub privacy: String,
    pub priority: u16,
    pub latency_ms: Option<u32>,
    pub public_ip: bool,
    pub reachable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterLinkInfo {
    pub source_node_id: String,
    pub target_node_id: String,
    pub endpoint_id: String,
    pub transport: String,
    pub address: String,
    pub privacy: String,
    pub state: String,
    pub latency_ms: Option<u64>,
    pub consecutive_failures: u16,
    pub public_ip: bool,
}

impl Backend {
    pub fn app_cluster_status(&self) -> AppClusterStatus {
        sinomo_cli::app_api::cluster_status(self.data_dir.as_ref())
    }

    pub fn create_cluster_invite(&self) -> Result<AppClusterInvite, String> {
        sinomo_cli::app_api::create_cluster_invite(self.data_dir.as_ref())
            .map_err(|error| error.to_string())
    }

    pub fn join_cluster(&self, invite: &str) -> Result<AppClusterStatus, String> {
        sinomo_cli::app_api::join_cluster(
            self.data_dir.as_ref(),
            invite,
            self.node.display_name.as_str(),
        )
        .map_err(|error| error.to_string())
    }

    pub fn leave_cluster(&self) -> Result<AppClusterStatus, String> {
        sinomo_cli::app_api::leave_cluster(self.data_dir.as_ref())
            .map_err(|error| error.to_string())
    }

    pub fn cluster_snapshot(&self) -> ClusterSnapshot {
        let observed_at = OffsetDateTime::now_utc();
        let paths = ClusterPaths::new(self.data_dir.as_ref());
        let mut warnings = Vec::new();

        let state = match network::read_cluster_state(&paths) {
            Ok(state) => Some(state),
            Err(error) => {
                warnings.push(format!("cluster state unavailable: {error}"));
                None
            }
        };

        let rows = match self.connection.daemon_address() {
            Some(address) => match fetch_health_rows(address) {
                Ok(rows) => {
                    warnings.retain(|warning| !warning.starts_with("cluster state unavailable:"));
                    rows
                }
                Err(error) => {
                    warnings.push(format!("daemon health unavailable: {error}"));
                    state
                        .as_ref()
                        .map(|state| rows_from_state(state, observed_at))
                        .unwrap_or_default()
                }
            },
            None => state
                .as_ref()
                .map(|state| rows_from_state(state, observed_at))
                .unwrap_or_default(),
        };

        let connectivity = match network::read_cluster_connectivity_graph_from_kv(&paths) {
            Ok(records) => records,
            Err(error) => {
                warnings.push(format!("connectivity graph unavailable: {error}"));
                Vec::new()
            }
        };

        snapshot_from_parts(
            state.as_ref(),
            &rows,
            &connectivity,
            connection_source(&self.connection),
            observed_at,
            warnings,
        )
    }
}

fn fetch_health_rows(address: &str) -> Result<Vec<ClusterHealthRow>, String> {
    let request = network::cluster_health_rpc_request().map_err(|error| error.to_string())?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        address,
        &request,
        CLUSTER_SNAPSHOT_RPC_TIMEOUT,
    )
    .map_err(|error| error.to_string())?;
    network::decode_cluster_health_rpc_response(&response).map_err(|error| error.to_string())
}

fn rows_from_state(
    state: &sinomo_runtime::core::ClusterBootstrapState,
    observed_at: OffsetDateTime,
) -> Vec<ClusterHealthRow> {
    state
        .members
        .iter()
        .filter(|member| !matches!(member.state, sinomo_runtime::core::ClusterMemberState::Removed))
        .map(|member| {
            let status = network::unknown_cluster_node_status(state, member, observed_at);
            ClusterHealthRow {
                member: member.clone(),
                status,
                availability: ClusterNodeAvailability::Unknown,
            }
        })
        .collect()
}

fn snapshot_from_parts(
    state: Option<&sinomo_runtime::core::ClusterBootstrapState>,
    rows: &[ClusterHealthRow],
    connectivity: &[ClusterConnectivityRecord],
    source: String,
    observed_at: OffsetDateTime,
    mut warnings: Vec<String>,
) -> ClusterSnapshot {
    let mut endpoint_lookup = BTreeMap::new();
    for row in rows {
        for endpoint in &row.member.endpoints {
            endpoint_lookup.insert(
                (
                    endpoint.node_id.to_string(),
                    endpoint.endpoint_id.to_string(),
                    endpoint.address.clone(),
                ),
                endpoint.clone(),
            );
        }
    }

    let reachable_endpoint_ids = reachable_endpoint_ids(connectivity);
    let links = connectivity
        .iter()
        .map(|record| {
            let endpoint = endpoint_lookup
                .get(&(
                    record.target_node_id.to_string(),
                    record.endpoint_id.to_string(),
                    record.address.clone(),
                ))
                .or_else(|| {
                    endpoint_lookup
                        .iter()
                        .find(|((node_id, _, address), _)| {
                            node_id == record.target_node_id.as_str()
                                && address == &record.address
                        })
                        .map(|(_, endpoint)| endpoint)
                });
            ClusterLinkInfo {
                source_node_id: record.source_node_id.to_string(),
                target_node_id: record.target_node_id.to_string(),
                endpoint_id: record.endpoint_id.to_string(),
                transport: format_enum(&record.transport),
                address: record.address.clone(),
                privacy: endpoint
                    .map(|endpoint| format_enum(&endpoint.privacy))
                    .unwrap_or_else(|| "unknown".to_string()),
                state: format_enum(&record.state),
                latency_ms: record.latency_ms,
                consecutive_failures: record.consecutive_failures,
                public_ip: endpoint
                    .map(endpoint_has_public_ip)
                    .unwrap_or_else(|| address_has_public_ip(&record.address)),
            }
        })
        .collect::<Vec<_>>();

    let nodes = rows
        .iter()
        .map(|row| {
            let node_id = row.member.node_id.to_string();
            let endpoints = row
                .member
                .endpoints
                .iter()
                .map(|endpoint| {
                    let public_ip = endpoint_has_public_ip(endpoint);
                    let reachable = reachable_endpoint_ids.contains(&(
                        endpoint.node_id.to_string(),
                        endpoint.endpoint_id.to_string(),
                    )) || (public_ip && row.availability == ClusterNodeAvailability::Online);
                    ClusterEndpointInfo {
                        endpoint_id: endpoint.endpoint_id.to_string(),
                        transport: format_enum(&endpoint.transport),
                        address: endpoint.address.clone(),
                        source: format_enum(&endpoint.source),
                        privacy: format_enum(&endpoint.privacy),
                        priority: endpoint.priority,
                        latency_ms: endpoint.last_observed_latency_ms,
                        public_ip,
                        reachable,
                    }
                })
                .collect::<Vec<_>>();
            let public_reachable = endpoints
                .iter()
                .any(|endpoint| endpoint.public_ip && endpoint.reachable);
            ClusterNodeInfo {
                node_id,
                display_name: row.member.display_name.clone(),
                process: row.status.process.clone(),
                node_role: format_enum(&row.member.node_role),
                member_role: format_enum(&row.member.role),
                member_state: format_enum(&row.member.state),
                availability: format_enum(&row.availability),
                daemon_version: row.status.daemon_version.clone(),
                commit: row.status.commit.clone(),
                platform: match (&row.status.target_os, &row.status.target_arch) {
                    (Some(os), Some(arch)) => Some(format!("{os}/{arch}")),
                    (Some(os), None) => Some(os.clone()),
                    (None, Some(arch)) => Some(arch.clone()),
                    _ => None,
                },
                pid: row.status.pid,
                address: row.status.address.clone(),
                bind_address: row.status.bind_address.clone(),
                rollout: row
                    .status
                    .rollout
                    .as_ref()
                    .map(|rollout| rollout.status.clone()),
                observed_at: row.status.observed_at.to_string(),
                public_reachable,
                endpoints,
            }
        })
        .collect::<Vec<_>>();

    if rows.is_empty() && state.is_none() {
        warnings.push("no cluster metadata was found for this data directory".to_string());
    }

    ClusterSnapshot {
        cluster_id: state
            .map(|state| state.cluster_id.to_string())
            .or_else(|| rows.first().map(|row| row.member.cluster_id.to_string())),
        local_node_id: state.map(|state| state.local_node_id.to_string()),
        source,
        observed_at: observed_at.to_string(),
        nodes,
        links,
        warnings,
    }
}

fn reachable_endpoint_ids(
    records: &[ClusterConnectivityRecord],
) -> BTreeSet<(String, String)> {
    records
        .iter()
        .filter(|record| {
            matches!(
                record.state,
                ClusterConnectivityState::Healthy | ClusterConnectivityState::Degraded
            )
        })
        .map(|record| {
            (
                record.target_node_id.to_string(),
                record.endpoint_id.to_string(),
            )
        })
        .collect()
}

fn connection_source(connection: &Connection) -> String {
    match connection {
        Connection::Daemon(status) => format!("daemon {}", status.address),
        Connection::InProcessDaemon(status) => format!("in-process daemon {}", status.address),
        Connection::LocalFallback => "local files".to_string(),
    }
}

fn format_enum<T: std::fmt::Debug>(value: &T) -> String {
    let raw = format!("{value:?}");
    let mut out = String::new();
    for (index, ch) in raw.chars().enumerate() {
        if ch.is_ascii_uppercase() && index > 0 {
            out.push('-');
        }
        out.push(ch.to_ascii_lowercase());
    }
    out
}

fn endpoint_has_public_ip(endpoint: &sinomo_runtime::core::ClusterEndpointRecord) -> bool {
    endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
        || address_has_public_ip(&endpoint.address)
}

fn address_has_public_ip(address: &str) -> bool {
    address_ip(address).is_some_and(is_public_ip)
}

fn address_ip(address: &str) -> Option<IpAddr> {
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return Some(socket.ip());
    }
    let host = address
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']').map(|(host, _)| host))
        .or_else(|| address.rsplit_once(':').map(|(host, _)| host))
        .unwrap_or(address)
        .trim();
    host.parse::<IpAddr>().ok()
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_multicast()
                || ip.is_unspecified())
        }
        IpAddr::V6(ip) => {
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast())
        }
    }
}
