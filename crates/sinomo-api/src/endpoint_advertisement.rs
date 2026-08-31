use crate::{
    DaemonApiError, DaemonClusterStatus, DaemonPaths, DaemonProbeStatus, DaemonRunOptions,
    DaemonStatus, daemon_connect_address, daemon_options_from_status,
    daemon_status_matches_options, probe_daemon_status, probe_daemon_status_on_runtime,
    read_daemon_status,
};
use serde::{Deserialize, Serialize};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::node::{
    DAEMON_MAIN_CLUSTER_ROLE, DAEMON_SHADOW_CLUSTER_ROLE, NodeId, cluster_node_is_local_process,
};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterTransportKind,
};
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
#[cfg(test)]
use time::OffsetDateTime;

pub const SINOMO_DAEMON_ADVERTISE_HOST_ENV: &str = "SINOMO_DAEMON_ADVERTISE_HOST";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonClusterProcess {
    Main,
    Shadow,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonEndpointAdvertisementConfig {
    #[serde(alias = "main", alias = "endpoints")]
    pub main_endpoints: Vec<DaemonManualEndpointConfig>,
    #[serde(alias = "shadow")]
    pub shadow_endpoints: Vec<DaemonManualEndpointConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonManualEndpointConfig {
    pub address: String,
    #[serde(default, alias = "privacy", skip_serializing_if = "Option::is_none")]
    pub scope: Option<ClusterEndpointPrivacy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonClusterRegistration {
    pub process: DaemonClusterProcess,
    pub main_options: DaemonRunOptions,
    pub shadow_options: DaemonRunOptions,
}

pub fn daemon_cluster_registration(
    process: DaemonClusterProcess,
    main_options: DaemonRunOptions,
    shadow_options: DaemonRunOptions,
) -> DaemonClusterRegistration {
    DaemonClusterRegistration {
        process,
        main_options,
        shadow_options,
    }
}

pub fn daemon_cluster_status(
    state: &ClusterBootstrapState,
    process: DaemonClusterProcess,
) -> Option<DaemonClusterStatus> {
    let node_id = state.local_node_id.clone();
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == node_id)?;
    Some(DaemonClusterStatus {
        cluster_id: state.cluster_id.clone(),
        node_id,
        process: match process {
            DaemonClusterProcess::Main => DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            DaemonClusterProcess::Shadow => DAEMON_SHADOW_CLUSTER_ROLE.to_string(),
        },
        member_role: member.role.clone(),
        member_state: member.state.clone(),
    })
}

pub fn daemon_advertised_endpoints_for_state(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    let allow_public_ip_discovery = cluster_node_is_local_process(state, node_id);
    daemon_advertised_endpoints_from_ips(
        node_id,
        options,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
}

pub async fn daemon_advertised_endpoints_for_state_async(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    let allow_public_ip_discovery = cluster_node_is_local_process(state, node_id);
    daemon_advertised_endpoints_from_ips_async(
        node_id,
        options,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
    .await
}

pub fn daemon_endpoint_advertisement_config_file(data_dir: &Path) -> PathBuf {
    SinomoDataDir::new(data_dir.to_path_buf()).cluster_endpoint_config_file()
}

pub fn read_daemon_endpoint_advertisement_config(
    data_dir: &Path,
) -> Result<DaemonEndpointAdvertisementConfig, DaemonApiError> {
    let path = daemon_endpoint_advertisement_config_file(data_dir);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|source| DaemonApiError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(DaemonEndpointAdvertisementConfig::default())
        }
        Err(source) => Err(DaemonApiError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn daemon_advertised_endpoints_for_state_with_manual_config(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    manual_endpoints: &[DaemonManualEndpointConfig],
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = daemon_advertised_endpoints_for_state(
        state, node_id, options, priority, lan_ips, route_ips,
    );
    append_manual_daemon_endpoints(
        &mut endpoints,
        node_id,
        options.port,
        priority,
        manual_endpoints,
    );
    endpoints
}

pub async fn daemon_advertised_endpoints_for_state_with_manual_config_async(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    manual_endpoints: &[DaemonManualEndpointConfig],
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = daemon_advertised_endpoints_for_state_async(
        state, node_id, options, priority, lan_ips, route_ips,
    )
    .await;
    append_manual_daemon_endpoints(
        &mut endpoints,
        node_id,
        options.port,
        priority,
        manual_endpoints,
    );
    endpoints
}

fn daemon_advertised_endpoints_from_ips(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_endpoints: bool,
) -> Vec<ClusterEndpointRecord> {
    let advertise_host = std::env::var(SINOMO_DAEMON_ADVERTISE_HOST_ENV).ok();
    sinomo_core::network::daemon_advertised_endpoints_from_static_addresses(
        node_id,
        daemon_connect_address(&options.host, options.port),
        &options.host,
        options.port,
        priority,
        lan_ips,
        route_ips,
        allow_public_endpoints,
        advertise_host.as_deref(),
    )
}

async fn daemon_advertised_endpoints_from_ips_async(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<ClusterEndpointRecord> {
    let advertise_host = std::env::var(SINOMO_DAEMON_ADVERTISE_HOST_ENV).ok();
    sinomo_core::network::daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async(
        node_id,
        daemon_connect_address(&options.host, options.port),
        &options.host,
        options.port,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
        advertise_host.as_deref(),
    )
    .await
}

fn append_manual_daemon_endpoints(
    endpoints: &mut Vec<ClusterEndpointRecord>,
    node_id: &NodeId,
    default_port: u16,
    default_priority: u16,
    manual_endpoints: &[DaemonManualEndpointConfig],
) {
    let env_advertise_host = std::env::var(SINOMO_DAEMON_ADVERTISE_HOST_ENV)
        .ok()
        .map(|address| DaemonManualEndpointConfig {
            address,
            scope: None,
            priority: Some(default_priority.saturating_add(5)),
        });
    let configured = manual_endpoints.iter().cloned().chain(env_advertise_host);
    for manual in configured {
        let Some(parsed) = parse_manual_daemon_endpoint(&manual.address, node_id, default_port)
        else {
            continue;
        };
        let priority = manual
            .priority
            .unwrap_or_else(|| default_priority.saturating_add(5));
        let configured_scope = manual
            .scope
            .clone()
            .or_else(|| parsed.default_scope.clone());
        if let Some(endpoint) = endpoints.iter_mut().find(|endpoint| {
            (endpoint.transport == parsed.transport && endpoint.address == parsed.address)
                || parsed
                    .endpoint_id
                    .as_ref()
                    .is_some_and(|endpoint_id| endpoint.endpoint_id == *endpoint_id)
        }) {
            endpoint.source = ClusterEndpointSource::Static;
            endpoint.transport = parsed.transport.clone();
            endpoint.address = parsed.address;
            endpoint.priority = priority;
            if let Some(scope) = configured_scope {
                endpoint.privacy = scope;
            }
            continue;
        }
        let mut endpoint = parsed.into_endpoint(node_id, priority);
        endpoint.source = ClusterEndpointSource::Static;
        if let Some(scope) = manual.scope {
            endpoint.privacy = scope;
        }
        endpoints.push(endpoint);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedManualDaemonEndpoint {
    transport: ClusterTransportKind,
    address: String,
    endpoint_id: Option<sinomo_core::network::ClusterEndpointId>,
    default_scope: Option<ClusterEndpointPrivacy>,
}

impl ParsedManualDaemonEndpoint {
    fn into_endpoint(self, node_id: &NodeId, priority: u16) -> ClusterEndpointRecord {
        let mut endpoint = match self.transport {
            ClusterTransportKind::LanTcp => {
                sinomo_core::network::daemon_lan_endpoint(node_id, self.address, priority)
            }
            transport => {
                let mut endpoint =
                    sinomo_core::network::daemon_lan_endpoint(node_id, self.address, priority);
                endpoint.transport = transport;
                endpoint
            }
        };
        if let Some(endpoint_id) = self.endpoint_id {
            endpoint.endpoint_id = endpoint_id;
        }
        if let Some(default_scope) = self.default_scope {
            endpoint.privacy = default_scope;
        }
        endpoint
    }
}

fn parse_manual_daemon_endpoint(
    raw: &str,
    node_id: &NodeId,
    default_port: u16,
) -> Option<ParsedManualDaemonEndpoint> {
    let value = raw.trim();
    let Some((scheme, rest)) = value.split_once("://") else {
        return normalize_tcp_manual_endpoint_address(value, default_port).map(|address| {
            ParsedManualDaemonEndpoint {
                transport: ClusterTransportKind::LanTcp,
                address,
                endpoint_id: None,
                default_scope: None,
            }
        });
    };
    match scheme.to_ascii_lowercase().as_str() {
        "tcp" | "lan-tcp" => {
            normalize_tcp_manual_endpoint_address(rest, default_port).map(|address| {
                ParsedManualDaemonEndpoint {
                    transport: ClusterTransportKind::LanTcp,
                    address,
                    endpoint_id: None,
                    default_scope: None,
                }
            })
        }
        scheme => {
            let module = sinomo_core::network::network_module_registry()?
                .module_for_address_scheme(scheme)?;
            let transport = module.descriptor().transports.first().cloned()?;
            let address = format!("{scheme}://{rest}");
            Some(ParsedManualDaemonEndpoint {
                transport,
                endpoint_id: Some(sinomo_core::network::ClusterEndpointId::derived(
                    "sinomo.manual.daemon.endpoint",
                    format!("{}\0{scheme}\0{address}", node_id.as_str()),
                )),
                default_scope: default_scope_for_url_manual_endpoint(&address),
                address,
            })
        }
    }
}

/// Returns the normalized socket address for a manual TCP daemon endpoint.
///
/// Rollout preflight uses the same normalization as endpoint publication so it
/// can prove that the replacement main daemon will listen on an address capable
/// of serving every manually advertised TCP endpoint. Module-owned endpoint
/// schemes deliberately return `None`: their reachability is not governed by
/// the daemon's TCP bind host.
pub fn normalize_manual_daemon_tcp_endpoint_address(
    raw: &str,
    default_port: u16,
) -> Option<String> {
    let value = raw.trim();
    let Some((scheme, rest)) = value.split_once("://") else {
        return normalize_tcp_manual_endpoint_address(value, default_port);
    };
    match scheme.to_ascii_lowercase().as_str() {
        "tcp" | "lan-tcp" => normalize_tcp_manual_endpoint_address(rest, default_port),
        _ => None,
    }
}

fn default_scope_for_url_manual_endpoint(address: &str) -> Option<ClusterEndpointPrivacy> {
    let (_, query) = address.split_once('?')?;
    let has_relay = query.split('&').any(|part| {
        part.strip_prefix("relay=")
            .is_some_and(|value| !value.trim().is_empty())
    });
    Some(if has_relay {
        ClusterEndpointPrivacy::Relayed
    } else {
        ClusterEndpointPrivacy::InternetDirect
    })
}

fn normalize_tcp_manual_endpoint_address(raw: &str, default_port: u16) -> Option<String> {
    let host = raw.trim();
    if host.is_empty()
        || host.contains("://")
        || host.chars().any(|char| matches!(char, '/' | '?' | '#'))
    {
        return None;
    }
    let address = if let Ok(socket) = host.parse::<std::net::SocketAddr>() {
        if socket.ip().is_unspecified() {
            return None;
        }
        host.to_string()
    } else if let Ok(ip) = host.parse::<IpAddr>() {
        if ip.is_unspecified() {
            return None;
        }
        sinomo_core::network::format_ip_port(ip, default_port)
    } else if let Some((name, explicit_port)) = host.rsplit_once(':') {
        if !name.contains(':') && explicit_port.parse::<u16>().is_ok() {
            host.to_string()
        } else {
            format!("[{host}]:{default_port}")
        }
    } else {
        format!("{host}:{default_port}")
    };
    if sinomo_core::network::is_loopback_connect_address(&address) {
        return None;
    }
    Some(address)
}

pub fn current_shadow_daemon_options(data_dir: &Path, timeout: Duration) -> DaemonRunOptions {
    match probe_daemon_status(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        timeout,
    ) {
        DaemonProbeStatus::Running(status) => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status } => status
            .as_ref()
            .map(daemon_options_from_status)
            .unwrap_or_else(DaemonRunOptions::shadow_default),
        DaemonProbeStatus::NotRunning => DaemonRunOptions::shadow_default(),
    }
}

pub fn current_shadow_daemon_options_on_runtime(
    data_dir: &Path,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> DaemonRunOptions {
    match probe_daemon_status_on_runtime(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        timeout,
        runtime,
    ) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions::shadow_default()
        }
    }
}

pub fn current_daemon_cluster_options(
    data_dir: &Path,
) -> Option<(DaemonRunOptions, DaemonRunOptions)> {
    let main_options = read_daemon_status(&DaemonPaths::with_root(data_dir.join("daemon")))
        .map(|status| daemon_options_from_status(&status))
        .ok();
    let shadow_options =
        read_daemon_status(&DaemonPaths::with_root(data_dir.join("shadow-daemon")))
            .map(|status| daemon_options_from_status(&status))
            .ok();
    if main_options.is_none() && shadow_options.is_none() {
        return None;
    }
    Some((
        main_options.unwrap_or_default(),
        shadow_options.unwrap_or_else(DaemonRunOptions::shadow_default),
    ))
}

pub fn daemon_cluster_status_matches(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    status: &DaemonStatus,
) -> bool {
    status.cluster.as_ref().is_some_and(|cluster| {
        if cluster.node_id != endpoint.node_id {
            return false;
        }
        cluster.cluster_id == state.cluster_id
            || state
                .members
                .iter()
                .find(|member| member.node_id == endpoint.node_id)
                .is_some_and(|member| member.cluster_id == cluster.cluster_id)
    })
}

pub fn daemon_cluster_process_from_paths(
    data_dir: &Path,
    paths: &DaemonPaths,
) -> DaemonClusterProcess {
    if paths.root == data_dir.join("shadow-daemon") {
        DaemonClusterProcess::Shadow
    } else {
        DaemonClusterProcess::Main
    }
}

pub fn daemon_cluster_options_from_status(
    data_dir: &Path,
    paths: &DaemonPaths,
    status: &DaemonStatus,
    process: DaemonClusterProcess,
) -> (DaemonRunOptions, DaemonRunOptions) {
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let main_options = if process == DaemonClusterProcess::Main {
        daemon_options_from_status(status)
    } else {
        read_daemon_status(&main_paths)
            .map(|status| daemon_options_from_status(&status))
            .unwrap_or_else(|_| DaemonRunOptions::default())
    };
    let shadow_options = if paths.root == shadow_paths.root {
        daemon_options_from_status(status)
    } else {
        read_daemon_status(&shadow_paths)
            .map(|status| daemon_options_from_status(&status))
            .unwrap_or_else(|_| DaemonRunOptions::shadow_default())
    };
    (main_options, shadow_options)
}

pub fn invite_introducers_from_daemon_status(
    node_id: &NodeId,
    status: &DaemonStatus,
) -> Vec<sinomo_core::ClusterEndpointRecord> {
    if matches!(status.host.as_str(), "0.0.0.0" | "::") {
        let mut endpoints = Vec::new();
        for ip in sinomo_core::network::discover_default_lan_ip_candidates() {
            endpoints.push(sinomo_core::cluster::invite_introducer_endpoint(
                node_id,
                sinomo_core::network::format_ip_port(ip, status.port),
            ));
        }
        endpoints.push(sinomo_core::cluster::invite_introducer_endpoint(
            node_id,
            status.address.clone(),
        ));
        endpoints
    } else {
        vec![sinomo_core::cluster::invite_introducer_endpoint(
            node_id,
            status.address.clone(),
        )]
    }
}

pub fn daemon_probe_status_needs_restart_for_options(
    status: DaemonProbeStatus,
    host: &str,
    port: u16,
) -> bool {
    match status {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => !daemon_status_matches_options(&status, host, port),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::node::{ClusterId, shadow_daemon_node_id};
    use sinomo_core::{
        ClusterEndpointPrivacy, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
        ClusterTransportKind,
    };

    fn cluster_status(cluster_id: ClusterId, node_id: NodeId) -> DaemonClusterStatus {
        DaemonClusterStatus {
            cluster_id,
            node_id,
            process: DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::NonVoting,
            member_state: ClusterMemberState::NonVoting,
        }
    }

    fn daemon_status_for_cluster(cluster: DaemonClusterStatus) -> DaemonStatus {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-status-cluster-match");
        DaemonStatus::new(
            &DaemonRunOptions::default(),
            &DaemonPaths::with_root(root.join("daemon")),
            Some(cluster),
        )
    }

    #[test]
    fn local_shadow_advertisement_includes_public_route_ip() {
        let now = OffsetDateTime::now_utc();
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_shadow_public_route").expect("cluster"),
            "shadow public route".to_string(),
            NodeId::from_legacy_named("node_shadow_public_route").expect("node"),
            "shadow public route".to_string(),
            "main-public-key".to_string(),
            now,
        );
        let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
        let endpoints = daemon_advertised_endpoints_for_state(
            &state,
            &shadow_node_id,
            &DaemonRunOptions {
                host: "0.0.0.0".to_string(),
                port: 21_354,
                port_fallback: false,
            },
            10,
            &[],
            &["198.18.0.76".parse().expect("benchmark-network ip")],
        );

        assert!(endpoints.iter().any(|endpoint| {
            endpoint.node_id == shadow_node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && endpoint.address == "198.18.0.76:21354"
                && endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
        }));
    }

    #[test]
    fn advertised_host_adds_stable_hostname_endpoint_for_wildcard_bind() {
        let node_id = NodeId::from_legacy_named("node_advertise_hostname").expect("node");
        let endpoints = sinomo_core::network::daemon_advertised_endpoints_from_static_addresses(
            &node_id,
            "127.0.0.1:21353".to_string(),
            "0.0.0.0",
            21_353,
            0,
            &["172.24.0.4".parse().expect("lan ip")],
            &[],
            true,
            Some("node4"),
        );

        assert!(endpoints.iter().any(|endpoint| {
            endpoint.node_id == node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && endpoint.address == "node4:21353"
                && endpoint.privacy == ClusterEndpointPrivacy::Lan
                && endpoint.priority == 5
        }));
    }

    #[test]
    fn manual_endpoint_config_adds_public_endpoint() {
        let now = OffsetDateTime::now_utc();
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_manual_endpoint").expect("cluster"),
            "manual endpoint".to_string(),
            NodeId::from_legacy_named("node_manual_endpoint").expect("node"),
            "manual endpoint".to_string(),
            "main-public-key".to_string(),
            now,
        );
        let endpoints = daemon_advertised_endpoints_for_state_with_manual_config(
            &state,
            &state.local_node_id,
            &DaemonRunOptions {
                host: "0.0.0.0".to_string(),
                port: 21_353,
                port_fallback: false,
            },
            0,
            &[],
            &[],
            &[DaemonManualEndpointConfig {
                address: "tcp://public-node.example".to_string(),
                scope: Some(ClusterEndpointPrivacy::InternetDirect),
                priority: None,
            }],
        );

        assert!(endpoints.iter().any(|endpoint| {
            endpoint.node_id == state.local_node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && endpoint.address == "public-node.example:21353"
                && endpoint.source == ClusterEndpointSource::Static
                && endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
                && endpoint.priority == 5
        }));
    }

    #[test]
    fn manual_endpoint_config_applies_scope_and_priority() {
        let now = OffsetDateTime::now_utc();
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_manual_scoped_endpoint").expect("cluster"),
            "manual scoped endpoint".to_string(),
            NodeId::from_legacy_named("node_manual_scoped_endpoint").expect("node"),
            "manual scoped endpoint".to_string(),
            "main-public-key".to_string(),
            now,
        );
        let endpoints = daemon_advertised_endpoints_for_state_with_manual_config(
            &state,
            &state.local_node_id,
            &DaemonRunOptions::default(),
            0,
            &[],
            &[],
            &[DaemonManualEndpointConfig {
                address: "tcp://public-node.example".to_string(),
                scope: Some(ClusterEndpointPrivacy::Relayed),
                priority: Some(9),
            }],
        );

        assert!(endpoints.iter().any(|endpoint| {
            endpoint.node_id == state.local_node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && endpoint.address == "public-node.example:21353"
                && endpoint.source == ClusterEndpointSource::Static
                && endpoint.privacy == ClusterEndpointPrivacy::Relayed
                && endpoint.priority == 9
        }));
    }

    #[test]
    fn endpoint_advertisement_config_file_reads_scope_and_privacy_alias() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-endpoint-config");
        let config_path = daemon_endpoint_advertisement_config_file(&root);
        fs::create_dir_all(config_path.parent().expect("config parent")).expect("config dir");
        fs::write(
            &config_path,
            br#"{
                "main": [
                    {
                        "address": "tcp://public-node.example:21353",
                        "scope": "internet-direct"
                    }
                ],
                "shadow": [
                    {
                        "address": "public-node.example",
                        "privacy": "internet-direct",
                        "priority": 12
                    }
                ]
            }"#,
        )
        .expect("write config");

        let config = read_daemon_endpoint_advertisement_config(&root).expect("read config");

        assert_eq!(config.main_endpoints.len(), 1);
        assert_eq!(
            config.main_endpoints[0].address,
            "tcp://public-node.example:21353"
        );
        assert_eq!(
            config.main_endpoints[0].scope,
            Some(ClusterEndpointPrivacy::InternetDirect)
        );
        assert_eq!(config.shadow_endpoints.len(), 1);
        assert_eq!(config.shadow_endpoints[0].address, "public-node.example");
        assert_eq!(
            config.shadow_endpoints[0].scope,
            Some(ClusterEndpointPrivacy::InternetDirect)
        );
        assert_eq!(config.shadow_endpoints[0].priority, Some(12));
    }

    #[test]
    fn cluster_status_match_accepts_member_cluster_id_when_state_cluster_is_stale() {
        let now = OffsetDateTime::now_utc();
        let stale_cluster =
            ClusterId::from_legacy_named("cluster_status_match_stale").expect("cluster");
        let member_cluster =
            ClusterId::from_legacy_named("cluster_status_match_member").expect("cluster");
        let local = NodeId::from_legacy_named("node_status_match_local").expect("node");
        let remote = NodeId::from_legacy_named("node_status_match_remote").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            stale_cluster,
            "stale cluster",
            local,
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            member_cluster.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&remote, "172.23.0.2:21353".to_string(), 5);
        member.endpoints = vec![endpoint.clone()];
        state.members.push(member);

        let status = daemon_status_for_cluster(cluster_status(member_cluster, remote));

        assert!(daemon_cluster_status_matches(&state, &endpoint, &status));
    }

    #[test]
    fn cluster_status_match_rejects_unrelated_cluster_id() {
        let now = OffsetDateTime::now_utc();
        let state_cluster =
            ClusterId::from_legacy_named("cluster_status_match_state").expect("cluster");
        let member_cluster =
            ClusterId::from_legacy_named("cluster_status_match_member_two").expect("cluster");
        let other_cluster =
            ClusterId::from_legacy_named("cluster_status_match_other").expect("cluster");
        let local = NodeId::from_legacy_named("node_status_match_local_two").expect("node");
        let remote = NodeId::from_legacy_named("node_status_match_remote_two").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            state_cluster,
            "state cluster",
            local,
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            member_cluster,
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&remote, "172.23.0.3:21353".to_string(), 5);
        member.endpoints = vec![endpoint.clone()];
        state.members.push(member);

        let status = daemon_status_for_cluster(cluster_status(other_cluster, remote));

        assert!(!daemon_cluster_status_matches(&state, &endpoint, &status));
    }
}
