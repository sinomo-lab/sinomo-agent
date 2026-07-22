//! Mounted SIFS adapter over local cluster state and DFS peer transports.
//!
//! This module owns the low-level glue required to open SIFS as a mounted
//! networked DFS service from a local data directory.

use sinomo_core::ClusterBootstrapState;
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{KvCompare, KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ObjectRpcResponse;
use sinomo_core::network::{
    ClusterPaths, NetworkError, ObjectRpcCall, ObjectRpcRequest, ObjectRpcResult,
};
use sinomo_core::node::NodeId;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    AsyncDfsPeerTransport, DfsControlPlaneKvClient, DfsError, DfsNodeAdvertisement, DfsNodeId,
    DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError,
    DfsPeerTransportFuture, DfsPosixError, NetworkedDfsService, cluster_dfs_peer_node_ids,
    content_services_prefix, fallback_dfs_node_id, object_rpc_request_from_kv_payload,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
};

pub type MountedSifsService = NetworkedDfsService<DfsNodeTransport>;

/// Read-only content capability detached from mounted DFS authority state.
///
/// Open file descriptors already carry an exact immutable object/version/hash
/// identity. Reading that identity needs the local content store and peer
/// routes, but it must not hold (or clone) the mutable mounted service while
/// disk or network I/O is in progress. Each call below creates request-local
/// retry/response state over these thread-safe shared capabilities.
#[derive(Clone, Debug)]
pub struct MountedSifsContentReadContext {
    content: crate::LocalContentStore,
    transport: DfsNodeTransport,
    cancellation: Option<crate::DfsCancellationFlag>,
}

impl MountedSifsContentReadContext {
    /// Read a bounded range from the exact content identity captured at open.
    ///
    /// This deliberately has no pathname metadata lookup and no control-plane
    /// handle. Local reads retain manifest/hash validation; remote reads retain
    /// the normal exact-version range request, response validation, retry, and
    /// timeout behavior.
    pub fn read_opened_file_range(
        &self,
        path: &str,
        opened: &crate::DfsStat,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, crate::DfsPosixError> {
        let mut request = NetworkedDfsService::new(
            self.transport.fork_read_only_request(),
            self.content.clone(),
        );
        request.set_cancellation(self.cancellation.clone());
        // A queued FUSE read can begin after unmount cancellation. Observe it
        // before the exact-content helper has any opportunity to send its
        // first remote request.
        request.check_cancelled("mounted opened-file read before content transfer")?;
        request.posix_read_file_range_for_opened_stat(path, opened, offset, max_bytes)
    }
}

impl NetworkedDfsService<DfsNodeTransport> {
    /// Snapshot the thread-safe capabilities needed by opened-descriptor reads.
    ///
    /// Metadata, coordinators, pending writes, namespace state, and the control
    /// plane remain exclusively owned by the mounted service.
    pub fn mounted_content_read_context(&self) -> MountedSifsContentReadContext {
        MountedSifsContentReadContext {
            content: self.content.clone(),
            transport: self.transport.fork_read_only_request(),
            cancellation: self.cancellation.clone(),
        }
    }
}

#[derive(Debug)]
pub struct OpenedMountedSifsService {
    pub service: MountedSifsService,
    pub replica_candidates: Vec<DfsNodeId>,
}

pub fn open_mounted_sifs_service(
    data_dir: Option<PathBuf>,
) -> Result<OpenedMountedSifsService, DfsError> {
    open_mounted_sifs_service_with_cancellation(data_dir, None)
}

/// Like [`open_mounted_sifs_service`], with a cooperative cancellation flag
/// installed on the service before any distributed work runs. Budgeted callers
/// (app catalog loads) flip the flag to abandon the open and every subsequent
/// distributed operation promptly when the cluster link is degraded.
pub fn open_mounted_sifs_service_with_cancellation(
    data_dir: Option<PathBuf>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let data_dir = match data_dir {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home().map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?,
    };
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let peer_transport = Arc::new(MountedSifsPeerTransport {
        data_dir: data_dir.clone(),
        cluster_paths: cluster_paths.clone(),
    });
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    let control_plane = if cluster_paths.kv_paths().snapshot.exists() {
        Some(Box::new(MountedSifsControlPlaneKvClient {
            cluster_paths: cluster_paths.clone(),
            peer_transport: Arc::clone(&sync_peer_transport),
        }) as Box<dyn DfsControlPlaneKvClient>)
    } else {
        None
    };
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        cancellation,
    )
}

/// Opens mounted SIFS with caller-owned peer transports and routes clustered
/// control-plane commits through the same transport.
///
/// Application mounts use this entry point with a local-daemon-backed peer so
/// both content/metadata traffic and cluster-KV transactions share the
/// daemon's canonical routes and tunnels. Reads continue to use the local KV
/// snapshot; no clustered write falls back to a standalone router.
pub fn open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let control_plane = cluster_paths.kv_paths().snapshot.exists().then(|| {
        Box::new(MountedSifsControlPlaneKvClient {
            cluster_paths: cluster_paths.clone(),
            peer_transport: Arc::clone(&sync_peer_transport),
        }) as Box<dyn DfsControlPlaneKvClient>
    });
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        None,
    )
}

/// Opens mounted SIFS with caller-owned DFS peer transports.
///
/// Daemon hosts use this entry point so browser operations share the daemon's
/// established cluster router instead of constructing a second router inside
/// the daemon process. Any authorized cluster node remains a DFS client; shard
/// ownership only selects the target node for the request.
pub fn open_mounted_sifs_service_with_peer_transports(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        None,
    )
}

fn mounted_sifs_cluster_context(
    data_dir: &SinomoDataDir,
) -> Result<
    (
        ClusterPaths,
        Option<ClusterBootstrapState>,
        NodeId,
        Vec<NodeId>,
    ),
    DfsError,
> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).ok()
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(fallback_dfs_node_id);
    let peer_node_ids = cluster_state
        .as_ref()
        .map(cluster_dfs_peer_node_ids)
        .unwrap_or_default();
    Ok((cluster_paths, cluster_state, local_node_id, peer_node_ids))
}

#[allow(clippy::too_many_arguments)]
fn open_mounted_sifs_service_from_context(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let mut service = open_cluster_dfs_service_with_peer_transports_for_nodes(
        data_dir,
        cluster_paths,
        local_node_id.clone(),
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
    )?;
    service.set_cancellation(cancellation);
    service
        .bootstrap_distributed_sifs()
        .map_err(|source| match source {
            DfsPosixError::Dfs(error) => error,
            other => DfsError::Metadata {
                message: other.to_string(),
            },
        })?;
    Ok(OpenedMountedSifsService {
        service,
        replica_candidates: mounted_sifs_replica_candidates_from_context(
            cluster_paths,
            cluster_state,
            local_node_id,
        ),
    })
}

fn mounted_sifs_replica_candidates_from_context(
    cluster_paths: &ClusterPaths,
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
) -> Vec<DfsNodeId> {
    if cluster_state.is_none() {
        return vec![local_node_id];
    }
    let advertisements = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        content_services_prefix(),
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok());
    mounted_sifs_content_replica_candidates(advertisements)
}

/// Returns the current content-service nodes eligible to store SIFS replicas.
///
/// Cluster membership grants client access but does not make a node a storage
/// target. In an initialized cluster this therefore returns only nodes with a
/// current `accepts_content` service advertisement. An unclustered standalone
/// mount keeps its local-only behavior.
pub fn discover_mounted_sifs_content_replica_candidates(
    data_dir: &SinomoDataDir,
) -> Result<Vec<DfsNodeId>, DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return Ok(vec![fallback_dfs_node_id()]);
    }
    let _state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DfsError::from_network)?;
    let entries = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        content_services_prefix(),
    )
    .map_err(DfsError::from)?;
    Ok(mounted_sifs_content_replica_candidates(
        entries
            .into_iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok()),
    ))
}

fn mounted_sifs_content_replica_candidates(
    advertisements: impl IntoIterator<Item = DfsNodeAdvertisement>,
) -> Vec<DfsNodeId> {
    let mut candidates = Vec::new();
    for advertisement in advertisements {
        if advertisement.accepts_content && !candidates.contains(&advertisement.node_id) {
            candidates.push(advertisement.node_id);
        }
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct RecordingMountedPeerTransport {
        targets: std::sync::Mutex<Vec<NodeId>>,
    }

    impl DfsPeerTransport for RecordingMountedPeerTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.targets
                .lock()
                .expect("targets")
                .push(target_node_id.clone());
            assert_eq!(call.request.route, sinomo_core::kv::CLUSTER_KV_TXN_ROUTE);
            let submitted =
                sinomo_core::kv::decode_cluster_kv_txn_rpc_request_body(&call.request.body)
                    .expect("transaction request");
            let payload = sinomo_core::kv::cluster_kv_txn_response_rpc_payload(
                &sinomo_core::kv::ClusterKvTxnCommitResult {
                    request_id: submitted.request_id,
                    snapshot: sinomo_core::kv::KvSnapshot {
                        revision: KvRevision(7),
                        compacted_revision: KvRevision::ZERO,
                        entries: Vec::new(),
                        leases: Vec::new(),
                        events: Vec::new(),
                        committed_requests: Vec::new(),
                        expired_requests: Vec::new(),
                    },
                    txn_response: Some(KvTxnResponse {
                        succeeded: true,
                        revision: KvRevision(7),
                        responses: Vec::new(),
                    }),
                    authority: sinomo_core::kv::KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: KvRevision(7),
                        basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                    replayed_committed_request: false,
                },
            )
            .expect("txn response payload");
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
        }
    }

    #[test]
    fn content_replica_candidates_exclude_client_only_nodes() {
        let client = NodeId::from_legacy_named("node_mounted_sifs_client").expect("client");
        let first_server =
            NodeId::from_legacy_named("node_mounted_sifs_content_a").expect("first server");
        let second_server =
            NodeId::from_legacy_named("node_mounted_sifs_content_b").expect("second server");
        let mut client_advertisement = DfsNodeAdvertisement::metadata(client);
        client_advertisement.accepts_content = false;

        assert_eq!(
            mounted_sifs_content_replica_candidates([
                client_advertisement,
                DfsNodeAdvertisement::content(first_server.clone()),
                DfsNodeAdvertisement::content(second_server.clone()),
            ]),
            vec![first_server, second_server]
        );
    }

    #[test]
    fn mounted_control_plane_commits_through_the_supplied_peer_transport() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-control-plane-peer");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_control_plane").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_control_plane")
                .expect("cluster id"),
            "mounted SIFS control plane",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let peer = Arc::new(RecordingMountedPeerTransport {
            targets: std::sync::Mutex::new(Vec::new()),
        });
        let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut client = MountedSifsControlPlaneKvClient {
            cluster_paths,
            peer_transport,
        };

        let response = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect("txn");

        assert_eq!(response.revision, KvRevision(7));
        assert_eq!(
            peer.targets.lock().expect("targets").as_slice(),
            &[local_node]
        );
    }

    #[test]
    fn mounted_content_read_context_overlaps_remote_opened_range_reads() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-content-read-context");
        let local = NodeId::from_legacy_named("node_mounted_read_local").expect("local node");
        let remote = NodeId::from_legacy_named("node_mounted_read_remote").expect("remote node");
        let object_id =
            crate::DfsObjectId::from_legacy_named("object_mounted_read").expect("object id");
        let version =
            crate::DfsVersion::from_legacy_named("version_mounted_read").expect("version");
        let bytes = b"request-local-read-context".to_vec();
        let content_hash = crate::content_hash(&bytes);
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_for_sender = Arc::clone(&release);
        let bytes_for_sender = bytes.clone();
        let sender: crate::DfsMessageSender = Arc::new(move |target_node_id, command, _options| {
            let request = match &command.routed_message.message {
                crate::DfsNetworkMessage::ReadRequest(request) => request,
                other => {
                    return Err(DfsPeerTransportError::new(format!(
                        "unexpected mounted read command: {other:?}"
                    )));
                }
            };
            entered_tx
                .send(())
                .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
            let (released, wake) = &*release_for_sender;
            let mut released = released.lock().expect("release lock");
            while !*released {
                released = wake.wait(released).expect("release wait");
            }
            let offset = request.range_offset.expect("range offset");
            let max_bytes = request.range_max_bytes.expect("range max bytes");
            let start = usize::try_from(offset).expect("range offset fits");
            let end = start.saturating_add(max_bytes).min(bytes_for_sender.len());
            let response = crate::DfsReadResponse {
                request_id: request.request_id.clone(),
                object_id: request.object_id.clone(),
                result: crate::DfsReadResult::RangeFound(crate::DfsReadRangeData {
                    object_id: request.object_id.clone(),
                    version: request.version.clone().expect("exact version"),
                    content_hash: request.content_hash.clone(),
                    offset,
                    total_size_bytes: request.content_size_bytes.expect("exact size"),
                    bytes: bytes_for_sender[start..end].to_vec(),
                }),
            };
            Ok(crate::DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![crate::DfsRoutedMessage {
                    source_node_id: target_node_id.clone(),
                    target_node_id: command.routed_message.source_node_id.clone(),
                    frame_id: command.routed_message.frame_id,
                    message_id: command.routed_message.message_id,
                    message: crate::DfsNetworkMessage::ReadResponse(response),
                }],
            })
        });
        let transport = DfsNodeTransport::with_message_sender_for_nodes(
            local,
            vec![remote.clone()],
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            sender,
        );
        let service = NetworkedDfsService::new(
            transport,
            crate::LocalContentStore::new(root.join("content")).expect("content store"),
        );
        let context = service.mounted_content_read_context();
        // The context must not borrow or clone the mutable service. Dropping it
        // before the workers also exercises that ownership boundary.
        drop(service);
        let opened = crate::DfsStat {
            file_system_id: crate::sifs_file_system_id(),
            object_id,
            path: "/opened.txt".to_string(),
            kind: crate::DfsObjectKind::File,
            version,
            content_hash: Some(content_hash),
            size_bytes: Some(bytes.len() as u64),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![remote],
        };
        let first_context = context.clone();
        let first_opened = opened.clone();
        let first = thread::spawn(move || {
            first_context.read_opened_file_range("/opened.txt", &first_opened, 0, 7)
        });
        let second =
            thread::spawn(move || context.read_opened_file_range("/opened.txt", &opened, 7, 6));

        let first_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let second_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        let first = first.join().expect("first read thread");
        let second = second.join().expect("second read thread");

        assert!(
            first_entered && second_entered,
            "remote reads did not overlap"
        );
        assert_eq!(first.expect("first opened range"), &bytes[..7]);
        assert_eq!(second.expect("second opened range"), &bytes[7..13]);
    }

    fn cancellation_test_opened_stat(remote: NodeId) -> crate::DfsStat {
        let bytes = b"cancelled-opened-read";
        crate::DfsStat {
            file_system_id: crate::sifs_file_system_id(),
            object_id: crate::DfsObjectId::from_legacy_named("object_cancelled_opened_read")
                .expect("object id"),
            path: "/cancelled.txt".to_string(),
            kind: crate::DfsObjectKind::File,
            version: crate::DfsVersion::from_legacy_named("version_cancelled_opened_read")
                .expect("version"),
            content_hash: Some(crate::content_hash(bytes)),
            size_bytes: Some(bytes.len() as u64),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![remote],
        }
    }

    fn cancellation_test_read_context(
        root: &std::path::Path,
        local: NodeId,
        remote: NodeId,
        sender: crate::DfsMessageSender,
        cancellation: crate::DfsCancellationFlag,
    ) -> MountedSifsContentReadContext {
        let transport = DfsNodeTransport::with_message_sender_for_nodes(
            local,
            [remote],
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            sender,
        );
        let mut service = NetworkedDfsService::new(
            transport,
            crate::LocalContentStore::new(root.join("content")).expect("content store"),
        );
        service.add_cancellation(cancellation);
        service.mounted_content_read_context()
    }

    #[test]
    fn cancelled_queued_opened_read_never_sends_its_first_remote_request() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-read-cancel-before-send");
        let local = NodeId::from_legacy_named("node_cancel_before_send_local").expect("local");
        let remote = NodeId::from_legacy_named("node_cancel_before_send_remote").expect("remote");
        let sends = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let sends_for_transport = Arc::clone(&sends);
        let sender: crate::DfsMessageSender = Arc::new(move |_target, _command, _options| {
            sends_for_transport.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(DfsPeerTransportError::new(
                "cancelled read must not reach transport",
            ))
        });
        let cancellation = crate::DfsCancellationFlag::new();
        let context = cancellation_test_read_context(
            &root,
            local,
            remote.clone(),
            sender,
            cancellation.clone(),
        );
        let opened = cancellation_test_opened_stat(remote);
        cancellation.cancel();

        let error = context
            .read_opened_file_range("/cancelled.txt", &opened, 0, 4)
            .expect_err("cancelled queued read");

        assert!(matches!(
            error,
            crate::DfsPosixError::Dfs(crate::DfsError::Cancelled {
                operation: "mounted opened-file read before content transfer"
            })
        ));
        assert_eq!(sends.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn cancellation_during_first_remote_send_stops_read_before_response_processing() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-read-cancel-in-flight");
        let local = NodeId::from_legacy_named("node_cancel_in_flight_local").expect("local");
        let remote = NodeId::from_legacy_named("node_cancel_in_flight_remote").expect("remote");
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_for_sender = Arc::clone(&release);
        let sender: crate::DfsMessageSender = Arc::new(move |target, command, _options| {
            entered_tx
                .send(())
                .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
            let (released, wake) = &*release_for_sender;
            let mut released = released.lock().expect("release lock");
            while !*released {
                released = wake.wait(released).expect("release wait");
            }
            let request = match &command.routed_message.message {
                crate::DfsNetworkMessage::ReadRequest(request) => request,
                other => {
                    return Err(DfsPeerTransportError::new(format!(
                        "unexpected cancellation command: {other:?}"
                    )));
                }
            };
            Ok(crate::DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![crate::DfsRoutedMessage {
                    source_node_id: target.clone(),
                    target_node_id: command.routed_message.source_node_id.clone(),
                    frame_id: command.routed_message.frame_id,
                    message_id: command.routed_message.message_id,
                    message: crate::DfsNetworkMessage::ReadResponse(crate::DfsReadResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: crate::DfsReadResult::NotFound,
                    }),
                }],
            })
        });
        let cancellation = crate::DfsCancellationFlag::new();
        let context = cancellation_test_read_context(
            &root,
            local,
            remote.clone(),
            sender,
            cancellation.clone(),
        );
        let opened = cancellation_test_opened_stat(remote);
        let read =
            thread::spawn(move || context.read_opened_file_range("/cancelled.txt", &opened, 0, 4));
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first remote send entered");
        cancellation.cancel();
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }

        let error = read
            .join()
            .expect("read thread")
            .expect_err("in-flight read cancelled");
        assert!(matches!(
            error,
            crate::DfsPosixError::Dfs(crate::DfsError::Cancelled {
                operation: "remote metadata range read wait"
            })
        ));
    }

    #[test]
    fn linked_cancellation_preserves_every_source() {
        let external = crate::DfsCancellationFlag::new();
        let mount = crate::DfsCancellationFlag::new();
        let linked = crate::DfsCancellationFlag::linked([external.clone(), mount.clone()]);
        assert!(!linked.is_cancelled());
        external.cancel();
        assert!(linked.is_cancelled());

        let external = crate::DfsCancellationFlag::new();
        let linked = crate::DfsCancellationFlag::linked([external, mount.clone()]);
        assert!(!linked.is_cancelled());
        mount.cancel();
        assert!(linked.is_cancelled());
    }
}

#[derive(Clone)]
struct MountedSifsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
}

impl std::fmt::Debug for MountedSifsControlPlaneKvClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountedSifsControlPlaneKvClient")
            .field("cluster_paths", &self.cluster_paths)
            .field("peer_transport", &"dyn DfsPeerTransport")
            .finish()
    }
}

impl DfsControlPlaneKvClient for MountedSifsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let state = sinomo_core::cluster::read_cluster_state(&self.cluster_paths)
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        let rpc_payload =
            sinomo_core::kv::cluster_kv_txn_rpc_payload(&request).map_err(|source| {
                KvError::Io {
                    path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
                    detail: source.to_string(),
                }
            })?;
        let request = object_rpc_request_from_kv_payload(rpc_payload);
        let response = self
            .peer_transport
            .call_object_rpc(
                &state.local_node_id,
                ObjectRpcCall::value(request),
                DfsObjectRpcSendOptions::default(),
            )
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        if !response.body.is_empty() {
            return Err(KvError::Io {
                path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
                detail: "cluster KV transaction returned an unexpected attachment".to_string(),
            });
        }
        let response = response.response;
        response
            .error_detail()
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        let result =
            sinomo_core::kv::decode_cluster_kv_txn_rpc_body(&response.body).map_err(|source| {
                KvError::Io {
                    path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
                    detail: source.to_string(),
                }
            })?;
        result
            .txn_response
            .ok_or_else(|| KvError::AuthorityUnavailable {
                detail: "cluster KV transaction response omitted its committed result".to_string(),
            })
    }

    fn linearizable_compare_value(
        &mut self,
        key: String,
        expected_value: Vec<u8>,
        barrier_key: String,
    ) -> Result<bool, KvError> {
        // A fresh write keeps this on the quorum-backed transaction path. A
        // compare-only transaction could otherwise be satisfied by the
        // cluster KV semantic no-effect shortcut.
        let barrier_value =
            sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
        Ok(self
            .txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key,
                    value: expected_value,
                }],
                success: vec![KvOp::Put {
                    key: barrier_key,
                    value: barrier_value,
                    lease_id: None,
                }],
                failure: Vec::new(),
            })?
            .succeeded)
    }
}

fn mounted_sifs_kv_io_error(
    cluster_paths: &ClusterPaths,
    source: impl std::fmt::Display,
) -> KvError {
    KvError::Io {
        path: cluster_paths.kv_paths().snapshot.display().to_string(),
        detail: source.to_string(),
    }
}

#[derive(Clone, Debug)]
struct MountedSifsPeerTransport {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
}

impl MountedSifsPeerTransport {
    async fn call_object_rpc_async_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state(&self.cluster_paths)?;
        let mut host = MountedSifsObjectRpcHost {
            data_dir: self.data_dir.clone(),
            runtime: runtime.clone(),
        };
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &self.cluster_paths);
        let result = client
            .call_target(&state, target_node_id, call, options.cluster_rpc_options())
            .await?;
        result.response.error_detail()?;
        Ok(result)
    }
}

impl DfsPeerTransport for MountedSifsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                let result = tokio::task::block_in_place(|| {
                    handle.block_on(self.call_object_rpc_async_inner(
                        target_node_id,
                        call,
                        options,
                        &handle,
                    ))
                });
                result.map_err(|source| DfsPeerTransportError::new(source.to_string()))
            } else {
                Err(DfsPeerTransportError::new(
                    "mounted SIFS sync peer transport cannot run inside a current-thread Tokio runtime; use the async transport",
                ))
            };
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = runtime.block_on(self.call_object_rpc_async_inner(
            target_node_id,
            call,
            options,
            runtime.handle(),
        ));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result.map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

impl AsyncDfsPeerTransport for MountedSifsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            self.call_object_rpc_async_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    }
}

struct MountedSifsObjectRpcHost {
    data_dir: SinomoDataDir,
    runtime: tokio::runtime::Handle,
}

impl sinomo_core::network::ClusterSignedObjectRpcAsyncHost for MountedSifsObjectRpcHost {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_core::network::ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
        let data_dir = self.data_dir.clone();
        let request = request.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_core::network::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "mounted SIFS object RPC signing".to_string(),
                    detail: format!("object RPC signing task failed: {source}"),
                })?
        })
    }
}
