//! Lightweight daemon wire API contracts.
//!
//! This crate contains serializable request/response types, stable route
//! constants, lightweight clients, and host-agnostic services for those
//! contracts. Runtime clients need not link the full daemon implementation.

mod auth_context;
mod client;
mod endpoint_advertisement;
mod error;
mod event_service;
mod idle_stats;
mod paths;
mod replay_protection;
mod service;
mod status;

pub use auth_context::{AuthenticatedDaemonCommand, DaemonSignerAuthority};
pub use client::{
    DAEMON_API_PROBE_TIMEOUT, DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_DEFAULT_TIMEOUT,
    DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_TIMEOUT, DaemonApiClient,
    DaemonDfsMountedMutationHandoffClient, DaemonDfsPeerTransport, RemoteConfigClient,
    send_daemon_rpc, send_daemon_rpc_async, send_daemon_rpc_on_runtime, send_daemon_rpc_stream,
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
pub use event_service::{
    DaemonEventHub, DaemonEventMirror, DaemonEventService, register_daemon_event_service,
};
pub use idle_stats::IdleStatsSnapshot;
pub use paths::{DaemonPaths, daemon_paths_from_data_dir, shadow_daemon_paths_from_data_dir};
pub use replay_protection::{ReplayNonceCachePoisoned, ScopedReplayNonceCache};
pub use service::{
    DaemonLifecycleFuture, DaemonLifecycleHost, DaemonOperationsFuture, DaemonOperationsHost,
    DaemonShutdownKind, HostedDaemonLifecycleService, HostedDaemonOperationsService,
    daemon_object_rpc_forward_options, daemon_object_rpc_forward_transport_timeouts,
    enforce_daemon_object_rpc_forward_route_affinity, register_hosted_daemon_lifecycle_service,
    register_hosted_daemon_operations_service,
};
pub use sinomo_dfs::browser_api::*;
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
/// Main-daemon local-command route which durably owns work committed by a
/// separately supervised FUSE or Cloud Files mount process.
///
/// The versioned route fails closed during a rolling upgrade: an older daemon
/// cannot acknowledge an arm or commit while silently discarding its durable
/// outbox preconditions.
pub const DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE: &str =
    "/daemon/dfs/mounted-mutation/handoff-v1";
/// Maximum encoded request accepted by the mounted-mutation handoff client.
/// Keep this no larger than the durable outbox state which receives it.
pub const DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES: usize =
    sinomo_dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES;
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
pub const DFS_NAMESPACE_CHANGED_EVENT_KIND: &str = "dfs.namespace.changed";
pub const DFS_PLACEMENT_CHANGED_EVENT_KIND: &str = "dfs.placement.changed";

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
        Self {
            event_kind_prefixes: vec![DFS_NAMESPACE_CHANGED_EVENT_KIND.to_string()],
            path_prefixes: vec![path_prefix.into()],
        }
    }

    /// Subscribe to namespace and placement changes beneath one DFS path.
    pub fn for_dfs_path_prefix(path_prefix: impl Into<String>) -> Self {
        Self {
            event_kind_prefixes: vec!["dfs.".to_string()],
            path_prefixes: vec![path_prefix.into()],
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

pub use sinomo_dfs::{
    MountedSifsMutationHandoffPhase as DaemonDfsMountedMutationHandoffPhase,
    MountedSifsMutationHandoffRequest as DaemonDfsMountedMutationHandoffRequest,
    MountedSifsMutationHandoffResponse as DaemonDfsMountedMutationHandoffResponse,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonLocalConnectivityReloadRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonLocalConnectivityReloadResponse;

#[cfg(test)]
mod tests {
    use super::*;

    fn mounted_mutation_test_ids() -> (
        sinomo_dfs::MountedSifsMutationMarkerId,
        sinomo_dfs::MountedSifsMutationNonce,
        sinomo_dfs::MountedSifsMutationArmRequestId,
    ) {
        (
            sinomo_core::Id::derived("test.mounted-mutation.marker", b"marker"),
            sinomo_core::Id::derived("test.mounted-mutation.nonce", b"nonce"),
            sinomo_core::Id::derived("test.mounted-mutation.arm", b"arm"),
        )
    }

    #[test]
    fn mounted_mutation_handoff_wire_is_versioned_and_typed() {
        assert_eq!(
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            "/daemon/dfs/mounted-mutation/handoff-v1"
        );
        assert_eq!(
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES,
            sinomo_dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES
        );

        let probe = serde_json::to_value(DaemonDfsMountedMutationHandoffRequest::Probe)
            .expect("encode probe");
        assert_eq!(probe, serde_json::json!({ "operation": "probe" }));

        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let arm = serde_json::to_value(DaemonDfsMountedMutationHandoffRequest::Arm {
            marker_id: marker_id.clone(),
            marker_nonce,
            arm_request_id: arm_request_id.clone(),
        })
        .expect("encode arm");
        assert_eq!(arm["operation"], "arm");
        assert_eq!(arm["marker_id"], marker_id.as_str());
        assert_eq!(arm["arm_request_id"], arm_request_id.as_str());

        let response = DaemonDfsMountedMutationHandoffResponse {
            marker_id: Some(marker_id),
            arm_request_id: Some(arm_request_id),
            phase: DaemonDfsMountedMutationHandoffPhase::DaemonAccepted,
            appended_repair_record_count: 2,
        };
        let encoded = serde_json::to_value(&response).expect("encode response");
        assert_eq!(encoded["phase"], "daemon-accepted");
        assert_eq!(encoded["appended_repair_record_count"], 2);
        assert_eq!(
            serde_json::from_value::<DaemonDfsMountedMutationHandoffResponse>(encoded)
                .expect("decode response"),
            response
        );
    }

    #[test]
    fn mounted_mutation_error_never_decodes_as_an_empty_applied_capture() {
        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let captured_work = sinomo_dfs::MountedSifsMutationCapturedWork {
            committed_work: sinomo_dfs::DfsCommittedWork::default(),
            committed_repair_records: Vec::new(),
        };
        let unknown = DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            observation: sinomo_dfs::MountedSifsMutationErrorObservation {
                captured_work,
                error_code: "backend-returned-error-after-entry".to_string(),
            },
        };
        let encoded = serde_json::to_value(&unknown).expect("encode unknown result");
        assert_eq!(encoded["operation"], "commit-unknown");
        assert!(encoded.get("observation").is_some());
        assert!(encoded.get("captured_work").is_none());
        assert_eq!(
            serde_json::from_value::<DaemonDfsMountedMutationHandoffRequest>(encoded)
                .expect("decode unknown result"),
            unknown
        );
        let wire = sinomo_core::network::SinomoObjectCodec::network_frame()
            .encode(&unknown)
            .expect("encode SOBJ request value");
        assert!(wire.len() <= DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES);
        assert_eq!(
            sinomo_core::network::SinomoObjectCodec::network_frame()
                .decode::<DaemonDfsMountedMutationHandoffRequest>(&wire)
                .expect("decode SOBJ request value"),
            unknown
        );

        let ambiguous_legacy = serde_json::json!({
            "operation": "commit",
            "marker_id": marker_id,
            "marker_nonce": marker_nonce,
            "arm_request_id": arm_request_id
        });
        assert!(
            serde_json::from_value::<DaemonDfsMountedMutationHandoffRequest>(ambiguous_legacy)
                .is_err(),
            "an unclassified error must not decode as an empty applied capture"
        );
    }

    #[test]
    fn mounted_mutation_handoff_validates_bounded_labels() {
        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let empty_proof = DaemonDfsMountedMutationHandoffRequest::ProveNoWrite {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            proof_code: String::new(),
        };
        let error = empty_proof.validate().expect_err("empty proof code");
        assert!(error.contains("proof_code cannot be empty"));

        let empty_error = DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            observation: sinomo_dfs::MountedSifsMutationErrorObservation {
                captured_work: sinomo_dfs::MountedSifsMutationCapturedWork::default(),
                error_code: String::new(),
            },
        };
        let error = empty_error.validate().expect_err("empty error code");
        assert!(error.contains("error_code cannot be empty"));

        let request = DaemonDfsMountedMutationHandoffRequest::ProveNoWrite {
            marker_id,
            marker_nonce,
            arm_request_id,
            proof_code: "x"
                .repeat(sinomo_dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES.saturating_add(1)),
        };
        let error = request.validate().expect_err("oversized proof code");
        assert!(error.contains("proof_code"));
        assert!(error.contains("max is"));
    }

    #[test]
    fn dfs_subscription_covers_namespace_and_placement_events() {
        let request = DaemonEventSubscribeRequest::for_dfs_path_prefix("/users/project");
        assert_eq!(request.event_kind_prefixes, vec!["dfs."]);
        assert_eq!(request.path_prefixes, vec!["/users/project"]);
    }

    #[test]
    fn older_browser_list_requests_default_to_lightweight_entries() {
        let request: DaemonDfsBrowserListRequest = serde_json::from_value(serde_json::json!({
            "path": "/users"
        }))
        .expect("decode legacy list request");
        assert!(!request.include_metadata);
    }

    #[test]
    fn older_browser_entry_metadata_defaults_live_placement_to_none() {
        let metadata: DaemonDfsBrowserEntryMetadata = serde_json::from_value(serde_json::json!({
            "object_id": "object-report",
            "version": "version-1",
            "modified_at_unix_nanos": 0,
            "policy": "replicated-eventual",
            "current_locations": ["node-a"]
        }))
        .expect("decode legacy entry metadata");
        assert!(metadata.placement.is_none());
    }

    #[test]
    fn conditional_replace_has_a_dedicated_fail_closed_route() {
        assert_ne!(
            DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
            DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
        );
        assert!(
            DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE)
        );

        let write = serde_json::to_value(DaemonDfsBrowserWriteFileRequest {
            path: "/notes.txt".to_string(),
            bytes: b"plain write".to_vec(),
        })
        .expect("serialize plain write");
        assert_eq!(write.as_object().map(|fields| fields.len()), Some(2));
        assert!(write.get("expected_object_id").is_none());
        assert!(write.get("expected_version").is_none());
    }

    #[test]
    fn placement_authority_admin_route_is_not_a_browser_local_command() {
        assert!(!DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE));
        assert_eq!(
            DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
            "/daemon/dfs/placement-authority"
        );
    }

    #[test]
    fn placement_routes_are_typed_and_inheritance_is_explicit() {
        for route in [
            DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE,
        ] {
            assert!(DAEMON_DFS_BROWSER_ROUTES.contains(&route));
        }
        let request = DaemonDfsBrowserPlacementUpdateRequest {
            path: "/users/team".to_string(),
            policy: None,
            scope: sinomo_dfs::DfsPlacementScope::SelfOnly,
        };
        let encoded = serde_json::to_vec(&request).expect("encode placement update");
        let decoded: DaemonDfsBrowserPlacementUpdateRequest =
            serde_json::from_slice(&encoded).expect("decode placement update");
        assert_eq!(decoded, request);
        assert!(decoded.policy.is_none());
    }

    #[test]
    fn placement_update_v2_requires_a_preview_token_on_a_dedicated_route() {
        assert_ne!(
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE
        );
        assert!(DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE));

        let missing_token = serde_json::json!({
            "path": "/users/team",
            "scope": "self-only"
        });
        assert!(
            serde_json::from_value::<DaemonDfsBrowserPlacementUpdateV2Request>(missing_token)
                .is_err(),
            "the fail-closed route must not default a missing preview token"
        );
    }

    #[test]
    fn placement_activity_request_defaults_to_a_bounded_unfiltered_page() {
        let request: DaemonDfsBrowserPlacementActivityRequest =
            serde_json::from_value(serde_json::json!({})).expect("default activity request");
        assert!(request.statuses.is_empty());
        assert!(request.path_prefix.is_none());
        assert!(request.cursor.is_none());
        assert_eq!(
            request.limit,
            sinomo_dfs::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
        );

        let encoded = serde_json::to_value(DaemonDfsBrowserPlacementActivityRequest {
            statuses: vec![sinomo_dfs::DfsPlacementReconciliationActivityStatus::Retrying],
            path_prefix: Some("/team".to_string()),
            cursor: Some(sinomo_dfs::DfsPlacementReconciliationActivityCursor {
                created_at_unix_nanos: 42,
                job_id: "activity-job".to_string(),
            }),
            limit: 25,
        })
        .expect("encode activity request");
        let decoded: DaemonDfsBrowserPlacementActivityRequest =
            serde_json::from_value(encoded).expect("decode activity request");
        assert_eq!(decoded.limit, 25);
        assert_eq!(
            decoded.statuses,
            vec![sinomo_dfs::DfsPlacementReconciliationActivityStatus::Retrying]
        );

        let legacy_page: DaemonDfsBrowserPlacementActivityResponse =
            serde_json::from_value(serde_json::json!({"page": {"activities": []}}))
                .expect("activity completeness flags are backward-compatible defaults");
        assert!(!legacy_page.page.partial);
        assert!(!legacy_page.page.history_truncated);
    }

    #[test]
    fn conditional_replace_outcomes_preserve_receipt_and_error_kinds() {
        let committed = DaemonDfsBrowserReplaceFileIfVersionResponse {
            outcome: DaemonDfsBrowserReplaceFileIfVersionOutcome::Committed {
                receipt: DaemonDfsBrowserWriteReceipt {
                    object_id: "object-1".to_string(),
                    version: "version-2".to_string(),
                    content_hash: Some("hash-2".to_string()),
                    size_bytes: Some(7),
                },
                maintenance_warning: Some("maintenance queued".to_string()),
            },
        };
        let encoded = serde_json::to_vec(&committed).expect("encode committed outcome");
        let decoded: DaemonDfsBrowserReplaceFileIfVersionResponse =
            serde_json::from_slice(&encoded).expect("decode committed outcome");
        assert_eq!(decoded, committed);
        assert!(decoded.outcome.is_committed());

        for outcome in [
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Conflict {
                detail: "stale base".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::NotFound {
                detail: "removed".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Rejected {
                detail: "not writable".to_string(),
            },
        ] {
            let encoded = serde_json::to_vec(&outcome).expect("encode typed outcome");
            let decoded: DaemonDfsBrowserReplaceFileIfVersionOutcome =
                serde_json::from_slice(&encoded).expect("decode typed outcome");
            assert_eq!(decoded, outcome);
            assert!(!decoded.is_committed());
        }
    }

    #[test]
    fn dfs_browser_read_file_wire_preserves_found_and_types_unavailability() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
        struct LegacyReadFileResponse {
            bytes: Vec<u8>,
        }

        let legacy: DaemonDfsBrowserReadFileResponse =
            serde_json::from_value(serde_json::json!({"bytes": [1, 2, 3]}))
                .expect("decode legacy successful read");
        assert_eq!(
            legacy.into_result().expect("legacy found bytes"),
            vec![1, 2, 3]
        );

        let found = DaemonDfsBrowserReadFileResponse::found(vec![4, 5]);
        let found_wire = serde_json::to_value(&found).expect("encode found response");
        assert_eq!(found_wire, serde_json::json!({"bytes": [4, 5]}));

        let found_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &found,
        )
        .expect("encode current success response");
        let legacy_found: LegacyReadFileResponse = found_rpc
            .decode_body()
            .expect("legacy client decodes current success response");
        assert_eq!(legacy_found.bytes, vec![4, 5]);

        let legacy_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &LegacyReadFileResponse { bytes: vec![6, 7] },
        )
        .expect("encode legacy success response");
        let current_from_legacy: DaemonDfsBrowserReadFileResponse = legacy_rpc
            .decode_body()
            .expect("current client decodes legacy success response");
        assert_eq!(
            current_from_legacy
                .into_result()
                .expect("legacy response contains bytes"),
            vec![6, 7]
        );

        let missing = DaemonDfsBrowserReadFileResponse::not_found("/missing");
        assert!(matches!(
            missing.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::NotFound { ref path }) if path == "/missing"
        ));

        let unavailable = DaemonDfsBrowserReadFileResponse::content_unavailable(
            "/catalog.sobj",
            "metadata is authoritative; content mirror is incomplete",
        );
        let unavailable_wire =
            serde_json::to_value(&unavailable).expect("encode unavailable response");
        assert!(unavailable_wire.get("bytes").is_none());
        let unavailable_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &unavailable,
        )
        .expect("encode unavailable response");
        assert!(
            unavailable_rpc
                .decode_body::<LegacyReadFileResponse>()
                .is_err(),
            "legacy clients must fail closed when bytes are unavailable"
        );
        let unavailable: DaemonDfsBrowserReadFileResponse = unavailable_rpc
            .decode_body()
            .expect("current client decodes typed unavailable response");
        assert!(matches!(
            unavailable.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::ContentUnavailable { ref path, .. })
                if path == "/catalog.sobj"
        ));
    }

    #[test]
    fn dfs_browser_stat_wire_preserves_success_and_types_confirmed_absence() {
        let legacy = DaemonDfsBrowserStatResponse {
            file_system_id: "sifs".to_string(),
            object_id: "object-stat-wire".to_string(),
            path: "/present.txt".to_string(),
            kind: sinomo_dfs::DfsObjectKind::File,
            version: "version-stat-wire".to_string(),
            content_hash: Some("blake3:stat-wire".to_string()),
            size_bytes: Some(3),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec!["node-stat-wire".to_string()],
            local_pins: Vec::new(),
            health: None,
            metadata_shard: None,
            placement: None,
        };

        let current = DaemonDfsBrowserStatOutcomeResponse::found(legacy.clone());
        assert_eq!(
            serde_json::to_value(&current).expect("encode current stat success"),
            serde_json::to_value(&legacy).expect("encode legacy stat success")
        );
        let current_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &current,
        )
        .expect("encode current stat success");
        let legacy_from_current: DaemonDfsBrowserStatResponse = current_rpc
            .decode_body()
            .expect("legacy client decodes current stat success");
        assert_eq!(legacy_from_current, legacy);

        let legacy_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &legacy,
        )
        .expect("encode legacy stat success");
        let current_from_legacy: DaemonDfsBrowserStatOutcomeResponse = legacy_rpc
            .decode_body()
            .expect("current client decodes legacy stat success");
        assert_eq!(
            current_from_legacy
                .into_result()
                .expect("legacy found stat"),
            legacy
        );

        let missing = DaemonDfsBrowserStatOutcomeResponse::not_found("/missing.txt");
        let missing_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &missing,
        )
        .expect("encode typed missing stat");
        assert!(
            missing_rpc
                .decode_body::<DaemonDfsBrowserStatResponse>()
                .is_err(),
            "legacy clients must fail decoding a typed stat failure"
        );
        let missing: DaemonDfsBrowserStatOutcomeResponse = missing_rpc
            .decode_body()
            .expect("current client decodes typed missing stat");
        assert!(matches!(
            missing.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::NotFound { ref path })
                if path == "/missing.txt"
        ));

        let mut mixed = serde_json::to_value(&legacy).expect("encode legacy stat success");
        mixed
            .as_object_mut()
            .expect("stat success is an object")
            .insert(
                "failure".to_string(),
                serde_json::to_value(DaemonDfsBrowserReadFileFailure::NotFound {
                    path: "/present.txt".to_string(),
                })
                .expect("encode typed stat failure"),
            );
        assert!(
            serde_json::from_value::<DaemonDfsBrowserStatOutcomeResponse>(mixed.clone()).is_err(),
            "a mixed success and failure stat body must fail closed"
        );
        let mixed_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &mixed,
        )
        .expect("encode adversarial mixed stat body");
        assert!(
            mixed_rpc
                .decode_body::<DaemonDfsBrowserStatOutcomeResponse>()
                .is_err(),
            "the SOBJ wire decoder must reject a mixed stat body"
        );
    }

    #[test]
    fn dfs_browser_prefix_wire_preserves_success_and_fails_closed_across_versions() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
        struct LegacyPrefixResponse {
            bytes: Vec<u8>,
            truncated: bool,
        }

        let found = DaemonDfsBrowserReadFilePrefixResponse::found(vec![1, 2, 3], true);
        let found_wire = serde_json::to_value(&found).expect("encode current prefix success");
        assert_eq!(
            found_wire,
            serde_json::json!({"bytes": [1, 2, 3], "truncated": true})
        );
        let found_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &found,
        )
        .expect("encode current prefix success");
        let legacy_found: LegacyPrefixResponse = found_rpc
            .decode_body()
            .expect("legacy client decodes current prefix success");
        assert_eq!(legacy_found.bytes, vec![1, 2, 3]);
        assert!(legacy_found.truncated);

        let legacy_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &LegacyPrefixResponse {
                bytes: vec![4, 5],
                truncated: false,
            },
        )
        .expect("encode legacy prefix success");
        let current: DaemonDfsBrowserReadFilePrefixResponse = legacy_rpc
            .decode_body()
            .expect("current client decodes legacy prefix success");
        assert_eq!(
            current.into_result().expect("legacy prefix success"),
            (vec![4, 5], false)
        );

        let unavailable = DaemonDfsBrowserReadFilePrefixResponse::content_unavailable(
            "/preview.bin",
            "metadata is authoritative; content mirror is incomplete",
        );
        let unavailable_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &unavailable,
        )
        .expect("encode current unavailable prefix");
        assert!(
            unavailable_rpc
                .decode_body::<LegacyPrefixResponse>()
                .is_err(),
            "legacy clients must fail decoding a typed unavailable prefix"
        );
        let current: DaemonDfsBrowserReadFilePrefixResponse = unavailable_rpc
            .decode_body()
            .expect("current client decodes unavailable prefix");
        assert!(matches!(
            current.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::ContentUnavailable { ref path, .. })
                if path == "/preview.bin"
        ));

        let missing = DaemonDfsBrowserReadFilePrefixResponse::not_found("/missing-preview.bin");
        assert!(matches!(
            missing.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::NotFound { ref path })
                if path == "/missing-preview.bin"
        ));
    }

    #[test]
    fn dfs_browser_bulk_read_wire_preserves_success_and_fails_closed_across_versions() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
        struct LegacyFileRead {
            path: String,
            bytes: Option<Vec<u8>>,
            error: Option<String>,
        }

        let found = DaemonDfsBrowserFileRead::found("/record.json", vec![1, 2, 3]);
        let found_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &found,
        )
        .expect("encode current bulk success");
        let legacy_found: LegacyFileRead = found_rpc
            .decode_body()
            .expect("legacy client decodes current bulk success");
        assert_eq!(
            legacy_found,
            LegacyFileRead {
                path: "/record.json".to_string(),
                bytes: Some(vec![1, 2, 3]),
                error: None,
            }
        );

        let legacy_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &LegacyFileRead {
                path: "/legacy.json".to_string(),
                bytes: Some(vec![4, 5]),
                error: None,
            },
        )
        .expect("encode legacy bulk success");
        let current: DaemonDfsBrowserFileRead = legacy_rpc
            .decode_body()
            .expect("current client decodes legacy bulk success");
        assert_eq!(current.into_result().expect("legacy success"), vec![4, 5]);

        let unavailable = DaemonDfsBrowserFileRead::content_unavailable(
            "/record.json",
            "authoritative metadata exists but content is not readable",
        );
        let unavailable_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &unavailable,
        )
        .expect("encode typed bulk unavailability");
        let legacy_unavailable: LegacyFileRead = unavailable_rpc
            .decode_body()
            .expect("legacy client decodes the compatible outer item");
        assert!(
            legacy_unavailable.bytes.is_none() && legacy_unavailable.error.is_none(),
            "legacy clients must receive no value that can be mistaken for successful content"
        );

        let legacy_error_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &LegacyFileRead {
                path: "/legacy-missing.json".to_string(),
                bytes: None,
                error: Some("SIFS path not found: /legacy-missing.json".to_string()),
            },
        )
        .expect("encode legacy untyped failure");
        let current: DaemonDfsBrowserFileRead = legacy_error_rpc
            .decode_body()
            .expect("current client decodes legacy untyped failure");
        assert!(matches!(
            current.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::InvalidResponse { .. })
        ));

        assert!(matches!(
            DaemonDfsBrowserFileRead::not_found("/confirmed-missing.json").into_result(),
            Err(DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));
    }

    #[test]
    fn daemon_api_error_preserves_possibly_applied_delivery_state() {
        let error = DaemonApiError::from(sinomo_core::network::NetworkError::timeout(
            "conditional replace response",
            sinomo_core::network::NetworkTimeoutPhase::FirstByte,
            std::time::Duration::from_secs(1),
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        ));
        assert_eq!(
            error.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
        );
    }
}
