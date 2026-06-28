//! Cluster topology snapshot used by the graphical Cluster tab.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
pub use sinomo_cli::app_api::{AppClusterInvite, AppClusterStatus};
use sinomo_runtime::core::{
    ClusterEndpointPrivacy, ClusterMemberRecord, ClusterMemberState, ClusterNodeAvailability,
    ClusterNodeStatusRecord,
};
use sinomo_runtime::network::{
    self, ClusterConnectivityRecord, ClusterConnectivityState, ClusterHealthRow, ClusterPaths,
};
use sinomo_runtime::node::{cluster_member_expected_daemon_process, cluster_node_is_local_process};
use time::OffsetDateTime;

use crate::backend::{Backend, Connection};

const CLUSTER_LOCAL_SNAPSHOT_RPC_TIMEOUT: Duration = Duration::from_millis(1200);
const CLUSTER_REFRESH_RPC_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterHealthScope {
    Local,
    Full,
}

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
        self.cluster_snapshot_with_health_scope(ClusterHealthScope::Local)
    }

    pub fn refresh_cluster_snapshot(&self) -> ClusterSnapshot {
        self.cluster_snapshot_with_health_scope(ClusterHealthScope::Full)
    }

    fn cluster_snapshot_with_health_scope(
        &self,
        health_scope: ClusterHealthScope,
    ) -> ClusterSnapshot {
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

        let mut live_node_ids = BTreeSet::new();
        let mut rows = match state.as_ref() {
            Some(state) => {
                let cached_rows = rows_from_state(&paths, state, observed_at, &mut warnings);
                match self.connection.daemon_address() {
                    Some(address) => match fetch_daemon_health_rows(address, health_scope) {
                        Ok((live_rows, warning)) => {
                            if let Some(warning) = warning {
                                warnings.push(warning);
                            }
                            live_node_ids
                                .extend(live_rows.iter().map(|row| row.member.node_id.to_string()));
                            merge_health_rows(cached_rows, live_rows)
                        }
                        Err(error) => {
                            warnings.push(format!("daemon health unavailable: {error}"));
                            cached_rows
                        }
                    },
                    None => cached_rows,
                }
            }
            None => match self.connection.daemon_address() {
                Some(address) => match fetch_daemon_health_rows(address, health_scope) {
                    Ok((rows, warning)) => {
                        if let Some(warning) = warning {
                            warnings.push(warning);
                        }
                        warnings
                            .retain(|warning| !warning.starts_with("cluster state unavailable:"));
                        live_node_ids.extend(rows.iter().map(|row| row.member.node_id.to_string()));
                        rows
                    }
                    Err(error) => {
                        warnings.push(format!("daemon health unavailable: {error}"));
                        Vec::new()
                    }
                },
                None => Vec::new(),
            },
        };

        let connectivity = match network::read_cluster_connectivity_graph_from_kv(&paths) {
            Ok(records) => records,
            Err(error) => {
                warnings.push(format!("connectivity graph unavailable: {error}"));
                Vec::new()
            }
        };

        if let Some(state) = state.as_ref() {
            reconcile_cached_remote_availability(state, &mut rows, &connectivity, &live_node_ids);
        }

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

fn fetch_daemon_health_rows(
    address: &str,
    health_scope: ClusterHealthScope,
) -> Result<(Vec<ClusterHealthRow>, Option<String>), String> {
    match health_scope {
        ClusterHealthScope::Local => fetch_local_health_rows(address).map(|rows| (rows, None)),
        ClusterHealthScope::Full => match fetch_cluster_health_rows(address) {
            Ok(rows) => Ok((rows, None)),
            Err(cluster_error) => match fetch_local_health_rows(address) {
                Ok(rows) => Ok((
                    rows,
                    Some(format!(
                        "daemon health aggregation unavailable: {cluster_error}; showing local daemon health"
                    )),
                )),
                Err(local_error) => Err(format!(
                    "cluster health unavailable: {cluster_error}; local health unavailable: {local_error}"
                )),
            },
        },
    }
}

fn fetch_cluster_health_rows(address: &str) -> Result<Vec<ClusterHealthRow>, String> {
    let request = network::cluster_health_rpc_request().map_err(|error| error.to_string())?;
    let response =
        sinomo_runtime::daemon::send_daemon_rpc(address, &request, CLUSTER_REFRESH_RPC_TIMEOUT)
            .map_err(|error| error.to_string())?;
    network::decode_cluster_health_rpc_response(&response).map_err(|error| error.to_string())
}

fn fetch_local_health_rows(address: &str) -> Result<Vec<ClusterHealthRow>, String> {
    let request = network::cluster_local_health_rpc_request().map_err(|error| error.to_string())?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        address,
        &request,
        CLUSTER_LOCAL_SNAPSHOT_RPC_TIMEOUT,
    )
    .map_err(|error| error.to_string())?;
    network::decode_cluster_health_rpc_response(&response).map_err(|error| error.to_string())
}

fn rows_from_state(
    paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    observed_at: OffsetDateTime,
    warnings: &mut Vec<String>,
) -> Vec<ClusterHealthRow> {
    let cached_statuses = match network::read_cluster_node_statuses_from_kv(paths) {
        Ok(statuses) => statuses,
        Err(error) => {
            warnings.push(format!("cluster node status cache unavailable: {error}"));
            Vec::new()
        }
    };
    state
        .members
        .iter()
        .filter(|member| !matches!(member.state, ClusterMemberState::Removed))
        .map(|member| {
            let status = cached_statuses
                .iter()
                .find(|status| status_matches_member(status, member))
                .cloned()
                .unwrap_or_else(|| {
                    network::unknown_cluster_node_status(state, member, observed_at)
                });
            let availability = status.availability.clone();
            ClusterHealthRow {
                member: member.clone(),
                status,
                availability,
            }
        })
        .collect()
}

fn status_matches_member(status: &ClusterNodeStatusRecord, member: &ClusterMemberRecord) -> bool {
    status.node_id == member.node_id
        && cluster_member_expected_daemon_process(member)
            .map(|process| status.process == process)
            .unwrap_or(true)
}

fn merge_health_rows(
    mut rows: Vec<ClusterHealthRow>,
    live_rows: Vec<ClusterHealthRow>,
) -> Vec<ClusterHealthRow> {
    for live in live_rows {
        if let Some(existing) = rows
            .iter_mut()
            .find(|row| status_matches_member(&live.status, &row.member))
        {
            *existing = live;
        } else {
            rows.push(live);
        }
    }
    rows.sort_by(|left, right| left.member.node_id.cmp(&right.member.node_id));
    rows
}

fn reconcile_cached_remote_availability(
    state: &sinomo_runtime::core::ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    connectivity: &[ClusterConnectivityRecord],
    live_node_ids: &BTreeSet<String>,
) {
    for row in rows {
        if cluster_node_is_local_process(state, &row.member.node_id)
            || live_node_ids.contains(row.member.node_id.as_str())
        {
            continue;
        }

        if matches!(
            local_connectivity_reachability(state, &row.member.node_id, connectivity),
            Some(false)
        ) {
            row.availability = ClusterNodeAvailability::Offline;
            row.status.availability = ClusterNodeAvailability::Offline;
        }
    }
}

fn local_connectivity_reachability(
    state: &sinomo_runtime::core::ClusterBootstrapState,
    target_node_id: &sinomo_runtime::core::Id,
    connectivity: &[ClusterConnectivityRecord],
) -> Option<bool> {
    let mut saw_local_record = false;
    for record in connectivity.iter().filter(|record| {
        record.target_node_id.as_str() == target_node_id.as_str()
            && cluster_node_is_local_process(state, &record.source_node_id)
    }) {
        saw_local_record = true;
        if matches!(
            record.state,
            ClusterConnectivityState::Healthy | ClusterConnectivityState::Degraded
        ) {
            return Some(true);
        }
    }

    saw_local_record.then_some(false)
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
                            node_id == record.target_node_id.as_str() && address == &record.address
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
                    )) || (public_ip
                        && row.availability == ClusterNodeAvailability::Online);
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
    push_cluster_health_warnings(rows, &mut warnings);

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

fn push_cluster_health_warnings(rows: &[ClusterHealthRow], warnings: &mut Vec<String>) {
    let offline = rows
        .iter()
        .filter(|row| row.availability == ClusterNodeAvailability::Offline)
        .count();
    let unknown = rows
        .iter()
        .filter(|row| row.availability == ClusterNodeAvailability::Unknown)
        .count();
    if offline > 0 {
        warnings.push(format!("{offline} cluster node(s) offline"));
    }
    if unknown > 0 {
        warnings.push(format!("{unknown} cluster node(s) health unknown"));
    }
}

fn reachable_endpoint_ids(records: &[ClusterConnectivityRecord]) -> BTreeSet<(String, String)> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        ClusterBootstrapState, ClusterMemberRecord, ClusterTransportKind, Id,
    };

    fn id(name: &str) -> Id {
        Id::from_legacy_named(name).expect("test id")
    }

    fn member(cluster_id: &Id, node_id: &Id, name: &str) -> ClusterMemberRecord {
        ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            node_id.clone(),
            name,
            format!("{name}-key"),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn row(
        member: ClusterMemberRecord,
        availability: ClusterNodeAvailability,
        commit: &str,
        pid: u32,
    ) -> ClusterHealthRow {
        ClusterHealthRow {
            status: ClusterNodeStatusRecord {
                cluster_id: member.cluster_id.clone(),
                node_id: member.node_id.clone(),
                process: "daemon-main".to_string(),
                availability: availability.clone(),
                daemon_version: "0.1.0".to_string(),
                commit: Some(commit.to_string()),
                target_os: Some("windows".to_string()),
                target_arch: Some("x86_64".to_string()),
                pid: Some(pid),
                address: Some("127.0.0.1:21353".to_string()),
                bind_address: None,
                executable: None,
                rollout: None,
                daemon_started_at: None,
                observed_at: OffsetDateTime::UNIX_EPOCH,
            },
            member,
            availability,
        }
    }

    fn state_with_remote(
        cluster_id: &Id,
        local_node_id: &Id,
        remote: ClusterMemberRecord,
    ) -> ClusterBootstrapState {
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "app cluster",
            local_node_id.clone(),
            "local",
            "local-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        state.members.push(remote);
        state
    }

    fn connectivity(
        cluster_id: &Id,
        source_node_id: &Id,
        target_node_id: &Id,
        state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let unreachable = matches!(state, ClusterConnectivityState::Unreachable);
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            endpoint_id: id("endpoint_app_cluster_connectivity"),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.42:21353".to_string(),
            state,
            latency_ms: (!unreachable).then_some(12),
            consecutive_failures: if unreachable { 3 } else { 0 },
            last_success_at: (!unreachable).then_some("2026-05-06T00:00:00Z".to_string()),
            last_failure_at: unreachable.then_some("2026-05-06T00:00:00Z".to_string()),
            failure_reason: None,
            observed_at: OffsetDateTime::UNIX_EPOCH,
            expires_at: OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
        }
    }

    #[test]
    fn merge_health_rows_prefers_live_row_over_cached_row() {
        let cluster_id = id("cluster_app_health_merge");
        let node_id = id("node_app_health_merge");
        let member = member(&cluster_id, &node_id, "local");

        let cached = row(
            member.clone(),
            ClusterNodeAvailability::Offline,
            "cached",
            10,
        );
        let live = row(member, ClusterNodeAvailability::Online, "live", 20);
        let merged = merge_health_rows(vec![cached], vec![live]);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].availability, ClusterNodeAvailability::Online);
        assert_eq!(merged[0].status.commit.as_deref(), Some("live"));
        assert_eq!(merged[0].status.pid, Some(20));
    }

    #[test]
    fn snapshot_warns_for_cached_offline_health() {
        let cluster_id = id("cluster_app_health_warning");
        let node_id = id("node_app_health_warning");
        let member = member(&cluster_id, &node_id, "remote");
        let rows = vec![row(member, ClusterNodeAvailability::Offline, "cached", 10)];

        let snapshot = snapshot_from_parts(
            None,
            &rows,
            &[],
            "daemon 127.0.0.1:21353".to_string(),
            OffsetDateTime::UNIX_EPOCH,
            Vec::new(),
        );

        assert_eq!(snapshot.nodes[0].availability, "offline");
        assert!(
            snapshot
                .warnings
                .iter()
                .any(|warning| warning == "1 cluster node(s) offline")
        );
    }

    #[test]
    fn cached_remote_online_is_reconciled_offline_when_local_connectivity_fails() {
        let cluster_id = id("cluster_app_reconcile_offline");
        let local_node_id = id("node_app_reconcile_local");
        let remote_node_id = id("node_app_reconcile_remote");
        let remote = member(&cluster_id, &remote_node_id, "remote");
        let state = state_with_remote(&cluster_id, &local_node_id, remote.clone());
        let mut rows = vec![row(remote, ClusterNodeAvailability::Online, "cached", 10)];
        let records = vec![connectivity(
            &cluster_id,
            &local_node_id,
            &remote_node_id,
            ClusterConnectivityState::Unreachable,
        )];

        reconcile_cached_remote_availability(&state, &mut rows, &records, &BTreeSet::new());

        assert_eq!(rows[0].availability, ClusterNodeAvailability::Offline);
        assert_eq!(
            rows[0].status.availability,
            ClusterNodeAvailability::Offline
        );
    }

    #[test]
    fn cached_remote_online_remains_online_when_local_connectivity_is_healthy() {
        let cluster_id = id("cluster_app_reconcile_healthy");
        let local_node_id = id("node_app_reconcile_healthy_local");
        let remote_node_id = id("node_app_reconcile_healthy_remote");
        let remote = member(&cluster_id, &remote_node_id, "remote");
        let state = state_with_remote(&cluster_id, &local_node_id, remote.clone());
        let mut rows = vec![row(remote, ClusterNodeAvailability::Online, "cached", 10)];
        let records = vec![connectivity(
            &cluster_id,
            &local_node_id,
            &remote_node_id,
            ClusterConnectivityState::Healthy,
        )];

        reconcile_cached_remote_availability(&state, &mut rows, &records, &BTreeSet::new());

        assert_eq!(rows[0].availability, ClusterNodeAvailability::Online);
    }

    #[test]
    fn live_remote_row_is_not_downgraded_by_stale_connectivity() {
        let cluster_id = id("cluster_app_reconcile_live");
        let local_node_id = id("node_app_reconcile_live_local");
        let remote_node_id = id("node_app_reconcile_live_remote");
        let remote = member(&cluster_id, &remote_node_id, "remote");
        let state = state_with_remote(&cluster_id, &local_node_id, remote.clone());
        let mut rows = vec![row(remote, ClusterNodeAvailability::Online, "live", 20)];
        let records = vec![connectivity(
            &cluster_id,
            &local_node_id,
            &remote_node_id,
            ClusterConnectivityState::Unreachable,
        )];
        let live_node_ids = BTreeSet::from([remote_node_id.to_string()]);

        reconcile_cached_remote_availability(&state, &mut rows, &records, &live_node_ids);

        assert_eq!(rows[0].availability, ClusterNodeAvailability::Online);
        assert_eq!(rows[0].status.commit.as_deref(), Some("live"));
    }
}
