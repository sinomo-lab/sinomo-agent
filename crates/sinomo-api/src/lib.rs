//! Shared Sinomo wire contracts and common daemon RPC clients.
//!
//! This crate contains serializable request/response types, stable route
//! constants and lightweight clients. Domain values are namespaced below.
//! Server adapters live in sinomo-daemon; DFS transports live in sinomo-dfs.
//! This crate never depends on those implementations.

mod client;
mod endpoint_advertisement;
mod error;

mod idle_stats;
mod paths;

mod status;

pub use client::{
    DAEMON_API_PROBE_TIMEOUT, DaemonApiClient, RemoteConfigClient, send_daemon_rpc,
    send_daemon_rpc_async, send_daemon_rpc_on_runtime, send_daemon_rpc_stream,
    send_daemon_rpc_stream_async, send_daemon_rpc_stream_from_data_dir_async,
    send_daemon_rpc_with_timeouts,
};

pub use endpoint_advertisement::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonEndpointAdvertisementConfig,
    DaemonManualEndpointConfig, SINOMO_DAEMON_ADVERTISE_HOST_ENV, current_daemon_cluster_options,
    current_shadow_daemon_options, current_shadow_daemon_options_on_runtime,
    daemon_advertised_endpoints_for_state, daemon_advertised_endpoints_for_state_async,
    daemon_advertised_endpoints_for_state_with_manual_config,
    daemon_advertised_endpoints_for_state_with_manual_config_async,
    daemon_cluster_options_from_status, daemon_cluster_process_from_paths,
    daemon_cluster_registration, daemon_cluster_status, daemon_cluster_status_matches,
    daemon_endpoint_advertisement_config_file, daemon_probe_status_needs_restart_for_options,
    invite_introducers_from_daemon_status, normalize_manual_daemon_tcp_endpoint_address,
    read_daemon_endpoint_advertisement_config,
};
pub use error::DaemonApiError;

pub use idle_stats::{
    IdleStatsAtomics, IdleStatsSnapshot, install_process_idle_stats, process_idle_stats,
    register_io_observer,
};
pub use paths::{DaemonPaths, daemon_paths_from_data_dir, shadow_daemon_paths_from_data_dir};

pub use dfs::browser_api::*;
pub use status::{
    DaemonBuildInfo, DaemonClusterStatus, DaemonProbeStatus, DaemonRunOptions, DaemonStatus,
    SINOMO_APP_DAEMON_BINARY_ENV, SINOMO_APP_DAEMON_LAUNCH_ID_ENV,
    SINOMO_DAEMON_API_PROTOCOL_VERSION, SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT,
    SINOMO_DAEMON_PORT_ENV, SINOMO_DAEMON_PORT_FALLBACK_ENV, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    SINOMO_SHADOW_DAEMON_PORT_ENV, SINOMO_SHADOW_DAEMON_PORT_FALLBACK_ENV,
    cluster_node_status_from_daemon_status, daemon_bind_address, daemon_build_info,
    daemon_connect_address, daemon_data_dir_from_paths, daemon_git_commit, daemon_hostname,
    daemon_options_from_status, daemon_package_version, daemon_status_matches_options,
    default_main_daemon_status, install_daemon_build_info, probe_daemon_status,
    probe_daemon_status_async, probe_daemon_status_at_address,
    probe_daemon_status_at_address_on_runtime, probe_daemon_status_at_address_with_timeouts_async,
    probe_daemon_status_on_runtime, probe_daemon_status_with_timeouts_async, read_daemon_status,
    write_daemon_status,
};

pub const DAEMON_RPC_CATEGORY: &str = "daemon";
pub const DAEMON_STATUS_ROUTE: &str = "/daemon/status";
pub const DAEMON_SHUTDOWN_ROUTE: &str = "/daemon/shutdown";
/// Process-specific compatibility route for an authenticated cluster voter to
/// stop a shadow listener without granting remote shutdown authority over the
/// main daemon.
pub const DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE: &str = "/daemon/shadow/shutdown-remote";
pub const DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE: &str = "/daemon/cluster/maintenance-refresh";

pub const DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE: &str =
    "/daemon/cluster/local-connectivity-reload";
pub const DAEMON_NETWORK_CHUNK_METRICS_ROUTE: &str = "/daemon/network/chunk-metrics";
pub const DAEMON_OBJECT_RPC_FORWARD_ROUTE: &str = "/daemon/object-rpc/forward";
pub const DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE: &str = "/daemon/dfs/object-rpc/forward";
/// Fail-closed main-process forwarding route. Older daemons do not recognize
/// this route, so they cannot silently drop the process-affinity requirement
/// during a rolling upgrade.
pub const DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE: &str = "/daemon/object-rpc/forward-main";
/// DFS counterpart to [`DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE`]. Kept distinct
/// so the DFS proxy retains its independent body and route-policy surface.
pub const DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE: &str = "/daemon/dfs/object-rpc/forward-main";
pub const DAEMON_EVENT_SUBSCRIBE_ROUTE: &str = "/daemon/events/subscribe";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonEventSubscribeRequest {
    /// Event-kind prefixes to receive. An empty list subscribes to every kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_kind_prefixes: Vec<String>,
    /// DFS namespace path prefixes to receive. An empty list accepts events
    /// without path metadata as well as namespace events for every path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path_prefixes: Vec<String>,
}

impl DaemonEventSubscribeRequest {
    pub fn for_path_prefix(path_prefix: impl Into<String>) -> Self {
        let path_prefix = path_prefix.into();
        Self {
            event_kind_prefixes: vec![DFS_NAMESPACE_CHANGED_EVENT_KIND.to_string()],
            // Root means the complete namespace, including coalesced
            // invalidations whose exact changed paths are unavailable.
            path_prefixes: if path_prefix == "/" {
                Vec::new()
            } else {
                vec![path_prefix]
            },
        }
    }

    /// Subscribe to namespace and placement changes beneath one DFS path.
    pub fn for_dfs_path_prefix(path_prefix: impl Into<String>) -> Self {
        let path_prefix = path_prefix.into();
        Self {
            event_kind_prefixes: vec!["dfs.".to_string()],
            path_prefixes: if path_prefix == "/" {
                Vec::new()
            } else {
                vec![path_prefix]
            },
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonEventSubscribeResponse {
    pub subscription_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEventStreamItem {
    Ready {
        subscription_id: String,
    },
    Event {
        event: sinomo_core::EventEnvelope,
    },
    /// The subscriber fell behind the daemon's bounded live buffer. Consumers
    /// must reload their durable source of truth before processing later events.
    Lagged {
        skipped: u64,
    },
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum DaemonObjectRpcDestinationProcessAffinity {
    #[default]
    Any,
    MainDaemon,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonObjectRpcForwardRequest {
    pub target_node_id: sinomo_core::node::NodeId,
    pub request: sinomo_core::network::ObjectRpcRequest,
    /// The outer proxy body carries the inner call's typed value rather than a
    /// semantic attachment. This is framing metadata only; it adds no digest
    /// or content verification.
    #[serde(default)]
    pub streamed_value_body: bool,
    pub connect_timeout_millis: u64,
    /// Overrides the routed session's no-progress timeout. Missing values from
    /// older callers retain the daemon's default network timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overall_timeout_millis: Option<u64>,
    #[serde(default)]
    pub observe_connectivity: bool,
    /// Restricts the destination process selected by the daemon-owned routed
    /// transport. Omitted legacy requests retain process-agnostic routing.
    #[serde(default)]
    pub destination_process_affinity: DaemonObjectRpcDestinationProcessAffinity,
}

/// Retry-safety evidence carried only across the trusted local daemon-control
/// boundary. Remote object-RPC responses must not be allowed to assert that a
/// request was definitely not applied.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum DaemonObjectRpcForwardDeliveryState {
    DefinitelyNotSent,
    PossiblyApplied,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum DaemonObjectRpcForwardResponse {
    Success {
        response: sinomo_core::network::ObjectRpcResponse,
        /// The outer proxy body is the nested response's ordinary typed value,
        /// not a semantic attachment of the forwarding envelope.
        #[serde(default)]
        streamed_value_body: bool,
        /// Time spent inside the daemon on the routed object-RPC call. This
        /// excludes the caller-to-daemon loopback proxy RPC.
        #[serde(default)]
        forward_elapsed_micros: u64,
    },
    NetworkError {
        detail: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_state: Option<DaemonObjectRpcForwardDeliveryState>,
    },
}

/// Process-local ownership-native network allocator snapshot.
///
/// The cluster benchmark samples this on the source daemon before and after
/// its measured calls so copy and allocation results cover the routed carrier,
/// rather than only the CLI-to-daemon adapter process.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonNetworkChunkMetricsResponse {
    pub chunk_metrics: sinomo_core::network::NetworkChunkMetrics,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsObjectRpcForwardRequest {
    pub target_node_id: sinomo_core::node::NodeId,
    pub request: sinomo_core::network::ObjectRpcRequest,
    pub connect_timeout_millis: u64,
    #[serde(default)]
    pub observe_connectivity: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonLocalConnectivityReloadRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonLocalConnectivityReloadResponse;

/// Shared app-private commands and projections.
pub mod app;
pub mod cli;
pub mod dfs;
pub mod lm;
pub mod module;
pub mod observe;

/// Generic transport primitives for domain adapters. Deadlines, routing and retry evidence
/// remain in the common client implementation.
pub mod transport {
    pub use crate::client::{
        block_on_daemon_client, block_on_daemon_client_on_owner_runtime,
        call_daemon_address_rpc_async, daemon_api_overall_deadline,
        decode_object_rpc_value_response, run_daemon_api_with_overall_deadline,
        send_daemon_call_value_on_runtime, send_daemon_object_rpc_call_with_affinity_async,
    };
}
