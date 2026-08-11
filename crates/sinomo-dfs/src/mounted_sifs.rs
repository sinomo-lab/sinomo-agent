//! Mounted SIFS adapter over local cluster state and DFS peer transports.
//!
//! This module owns the low-level glue required to open SIFS as a mounted
//! networked DFS service from a local data directory.

use sinomo_core::ClusterBootstrapState;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{KvEntry, KvError, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterPaths, NetworkError, ObjectRpcCall, ObjectRpcResponse, ObjectRpcResult,
};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::node_transport::collect_dfs_incoming_body_sync;
use crate::{
    AsyncDfsPeerTransport, DfsCommittedWork, DfsControlPlaneKvClient, DfsError,
    DfsMessageTransport, DfsNamespaceChangeBatch, DfsNamespaceChangeCursor, DfsNodeAdvertisement,
    DfsNodeId, DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError,
    DfsPeerTransportFuture, DfsPosixError, DfsRepairAttemptRecord, NetworkedDfsService,
    cluster_dfs_peer_node_ids, content_services_prefix, fallback_dfs_node_id,
    open_cluster_dfs_service_with_peer_transports_for_nodes_lazy,
};

pub type MountedSifsService = NetworkedDfsService<DfsNodeTransport>;

/// Outcome plus the exact durable work produced by one mounted mutation.
///
/// Mounted adapters use this public seam instead of reaching into the
/// served-message implementation. The outcome is deliberately generic so an
/// adapter always receives the committed-work receipt even when its operation
/// returns an error after a durable metadata edge.
#[derive(Debug)]
pub struct MountedSifsMutationReport<R> {
    pub outcome: R,
    pub committed_work: DfsCommittedWork,
    /// Exact zero-attempt repair queue entries created by this mutation.
    ///
    /// This is a delta, not a snapshot of the service's accumulated repair
    /// inventory. A daemon handoff can therefore append it idempotently
    /// without replacing retry/backoff state owned by maintenance.
    pub committed_repair_records: Vec<DfsRepairAttemptRecord>,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Capture every durable edge produced by one direct mounted mutation.
    ///
    /// An already-installed [`crate::DfsCommittedWorkTracker`] remains in
    /// place and continues to receive the same edges. This method consumes
    /// only the operation-local receipt and repair-job delta that it begins.
    pub fn capture_mounted_mutation<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> MountedSifsMutationReport<R> {
        self.begin_message_committed_work();
        let outcome = operation(self);
        let committed_work = self.take_message_committed_work();
        let committed_repair_records = self.take_message_committed_repair_queue_records();
        MountedSifsMutationReport {
            outcome,
            committed_work,
            committed_repair_records,
        }
    }
}

const MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES: usize =
    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES: usize =
    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES: usize =
    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES;
const MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS: usize = 8;

fn mounted_sifs_authority_deadline_error() -> KvError {
    KvError::AuthorityUnavailable {
        detail: "metadata shard authority read deadline elapsed".to_string(),
    }
}

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

    /// Poll mounted SIFS namespace changes as independent shard-local batches.
    ///
    /// This is a named mounted-service seam for platform modules; it preserves
    /// the generic service contract that the returned vector has no implied
    /// ordering across shards.
    pub fn poll_mounted_namespace_changes_for_subtree(
        &mut self,
        file_system_id: crate::DfsFileSystemId,
        root_path: &str,
        cursors: &[DfsNamespaceChangeCursor],
    ) -> Result<Vec<DfsNamespaceChangeBatch>, DfsError> {
        self.poll_namespace_changes_for_subtree(file_system_id, root_path, cursors)
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
        cluster_paths: cluster_paths.clone(),
    });
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    let control_plane = if cluster_state.is_some() {
        Some(Box::new(MountedSifsControlPlaneKvClient::new_authoritative(
            cluster_paths.clone(),
            Arc::clone(&sync_peer_transport),
        )) as Box<dyn DfsControlPlaneKvClient>)
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
    open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
        data_dir,
        sync_peer_transport,
        async_peer_transport,
        None,
    )
}

/// Like [`open_mounted_sifs_service_with_peer_transports_and_default_control_plane`],
/// with a cooperative cancellation flag installed before namespace bootstrap.
///
/// Budgeted application mounts use this entry point so abandoning catalog
/// hydration also cancels the daemon-backed distributed work it started.
pub fn open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let control_plane = if cluster_state.is_some() {
        Some(Box::new(MountedSifsControlPlaneKvClient::new_authoritative(
            cluster_paths.clone(),
            Arc::clone(&sync_peer_transport),
        )) as Box<dyn DfsControlPlaneKvClient>)
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
        Some(
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
                .map_err(DfsError::from_network)?,
        )
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
    // A mounted client does not own maintenance or recovery. Hydrate only the
    // filesystem records needed to route canonical operations; path/object
    // metadata remains authoritative in the backend and is loaded on demand.
    // Full metadata hydration here made every short-lived app/CLI catalog load
    // scan the entire local SIFS replica before its first distributed read.
    let replica_candidates = mounted_sifs_replica_candidates_from_context(
        cluster_state,
        local_node_id.clone(),
        control_plane.as_deref(),
    )?;
    let mut service = open_cluster_dfs_service_with_peer_transports_for_nodes_lazy(
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
    // LookupPath is already a read-only authoritative query. Do not let an
    // eligible transient failure silently escalate a mounted stat/read into a
    // durable SyncPath apply outside the adapter's handoff. Directory listing
    // remains the explicit replaying read surface and is bracketed by adapters.
    service.disable_implicit_read_replica_ingest();
    Ok(OpenedMountedSifsService {
        service,
        replica_candidates,
    })
}

fn mounted_sifs_replica_candidates_from_context(
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
    control_plane: Option<&dyn DfsControlPlaneKvClient>,
) -> Result<Vec<DfsNodeId>, DfsError> {
    if cluster_state.is_none() {
        return Ok(vec![local_node_id]);
    }
    let control_plane = control_plane.ok_or_else(|| {
        DfsError::from(KvError::AuthorityUnavailable {
            detail: "clustered mounted SIFS requires an authoritative DFS control-plane client"
                .to_string(),
        })
    })?;
    let advertisements = control_plane
        .range_prefix(content_services_prefix())
        .map_err(DfsError::from)?
        .into_iter()
        .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok());
    Ok(mounted_sifs_content_replica_candidates(advertisements))
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
    let peer_transport: Arc<dyn DfsPeerTransport> = Arc::new(MountedSifsPeerTransport {
        cluster_paths: cluster_paths.clone(),
    });
    let control_plane =
        MountedSifsControlPlaneKvClient::new_authoritative(cluster_paths, peer_transport);
    let entries = control_plane
        .range_prefix(content_services_prefix())
        .map_err(DfsError::from)?;
    Ok(mounted_sifs_content_replica_candidates(
        entries
            .into_iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok()),
    ))
}

#[cfg(test)]
fn mounted_sifs_materialized_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, KvError> {
    cluster_paths
        .kv_paths()
        .local_cache()
        .cached_snapshot_arc_if_present()
}

#[cfg(test)]
fn mounted_sifs_required_materialized_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, DfsError> {
    mounted_sifs_materialized_snapshot(cluster_paths)
        .map_err(DfsError::from)?
        .ok_or_else(|| {
            DfsError::from(KvError::AuthorityUnavailable {
                detail: format!(
                    "clustered mounted SIFS requires the daemon materialized KV projection at {}",
                    cluster_paths.kv_paths().snapshot.display(),
                ),
            })
        })
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
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct MountedCaptureTransport {
        local_node_id: NodeId,
    }

    impl crate::DfsTransportIdentity for MountedCaptureTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for MountedCaptureTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &NodeId,
            _call: crate::DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
        }

        fn drain_incoming_messages(
            &mut self,
        ) -> Result<Vec<crate::DfsRoutedMessage>, NetworkError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn mounted_mutation_capture_returns_error_receipt_and_preserves_outer_tracker() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-mutation-capture");
        let local_node =
            NodeId::from_legacy_named("node_mounted_mutation_capture").expect("local node");
        let content =
            crate::LocalContentStore::new(root.path().join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            MountedCaptureTransport {
                local_node_id: local_node,
            },
            content,
        );
        let tracker = crate::DfsCommittedWorkTracker::new();
        service.set_committed_work_tracker(tracker.clone());

        let report = service.capture_mounted_mutation(|service| {
            service.record_message_committed_work(DfsCommittedWork {
                publish_metadata: true,
                namespace_maintenance: true,
                ..DfsCommittedWork::default()
            });
            Err::<(), _>("after-commit")
        });

        assert_eq!(report.outcome, Err("after-commit"));
        assert!(report.committed_work.publish_metadata);
        assert!(report.committed_work.namespace_maintenance);
        assert!(report.committed_repair_records.is_empty());
        let outer = tracker.take();
        assert!(outer.publish_metadata);
        assert!(outer.namespace_maintenance);
    }

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
            let request = crate::node_transport::materialize_dfs_value_call_sync(call, usize::MAX)
                .expect("materialize transaction request");
            let submitted = sinomo_core::kv::decode_cluster_kv_txn_rpc_request_body(&request.body)
                .expect("transaction request");
            let payload = sinomo_core::kv::cluster_kv_txn_response_rpc_payload(
                &sinomo_core::kv::ClusterKvTxnCommitResult {
                    request_id: submitted.request_id.clone(),
                    command_digest: submitted
                        .into_command_request()
                        .digest()
                        .expect("transaction digest"),
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

    impl AsyncDfsPeerTransport for RecordingMountedPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            let result = self.call_object_rpc(target_node_id, call, options);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum MountedTxnResponseFault {
        None,
        MismatchedRequestId,
        MismatchedDigest,
        InvalidAuthority,
        StaleAuthorityRevision,
        StalePostCommitRead,
        FailSecondPostCommitRead,
        MissingAuthoritativeEntry,
        WrongAuthoritativeEntryKey,
        OutOfPrefixAuthoritativeEntry,
        UnknownExactSetRoute,
        FailedExactSetRoute,
    }

    #[derive(Debug)]
    struct ScriptedMountedKvTransport {
        local_node_id: NodeId,
        succeeded: bool,
        fault: MountedTxnResponseFault,
        routes: Mutex<Vec<String>>,
        options: Mutex<Vec<DfsObjectRpcSendOptions>>,
        authoritative_entry_calls: AtomicUsize,
        exact_set_keys: Mutex<Vec<String>>,
    }

    impl ScriptedMountedKvTransport {
        fn new(local_node_id: NodeId, succeeded: bool, fault: MountedTxnResponseFault) -> Self {
            Self {
                local_node_id,
                succeeded,
                fault,
                routes: Mutex::new(Vec::new()),
                options: Mutex::new(Vec::new()),
                authoritative_entry_calls: AtomicUsize::new(0),
                exact_set_keys: Mutex::new(Vec::new()),
            }
        }

        fn authority(&self, revision: KvRevision) -> sinomo_core::kv::KvAuthorityProof {
            let (term, voter_acks, required_quorum) =
                if matches!(self.fault, MountedTxnResponseFault::InvalidAuthority) {
                    (0, 0, 1)
                } else {
                    (3, 2, 2)
                };
            sinomo_core::kv::KvAuthorityProof {
                term,
                commit_index: revision.0.saturating_add(10),
                revision,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks,
                    required_quorum,
                },
            }
        }

        fn routes(&self) -> Vec<String> {
            self.routes.lock().expect("routes").clone()
        }

        fn options(&self) -> Vec<DfsObjectRpcSendOptions> {
            self.options.lock().expect("options").clone()
        }
    }

    impl DfsPeerTransport for ScriptedMountedKvTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            assert_eq!(target_node_id, &self.local_node_id);
            self.options.lock().expect("options").push(options);
            let route = call.request.route.clone();
            self.routes.lock().expect("routes").push(route.clone());
            let request = crate::node_transport::materialize_dfs_value_call_sync(
                call,
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES
                    .saturating_add(1024 * 1024),
            )
            .expect("materialize mounted KV request");
            let payload = match route.as_str() {
                sinomo_core::kv::CLUSTER_KV_TXN_ROUTE => {
                    let submitted =
                        sinomo_core::kv::decode_cluster_kv_txn_rpc_request_body(&request.body)
                            .expect("decode mounted transaction");
                    let submitted_digest = submitted
                        .clone()
                        .into_command_request()
                        .digest()
                        .expect("mounted transaction digest");
                    let request_id =
                        if matches!(self.fault, MountedTxnResponseFault::MismatchedRequestId) {
                            sinomo_core::kv::KvRequestId::generate()
                        } else {
                            submitted.request_id
                        };
                    let command_digest =
                        if matches!(self.fault, MountedTxnResponseFault::MismatchedDigest) {
                            sinomo_core::kv::KvCommandDigest([0xA5; 32])
                        } else {
                            submitted_digest
                        };
                    let result_revision = KvRevision(9);
                    let authority_revision =
                        if matches!(self.fault, MountedTxnResponseFault::StaleAuthorityRevision) {
                            KvRevision(8)
                        } else {
                            result_revision
                        };
                    sinomo_core::kv::cluster_kv_txn_response_rpc_payload(
                        &sinomo_core::kv::ClusterKvTxnCommitResult {
                            request_id,
                            command_digest,
                            snapshot: empty_mounted_sifs_kv_snapshot(result_revision),
                            txn_response: Some(KvTxnResponse {
                                succeeded: self.succeeded,
                                revision: result_revision,
                                responses: Vec::new(),
                            }),
                            authority: self.authority(authority_revision),
                            replayed_committed_request: false,
                        },
                    )
                    .expect("encode mounted transaction response")
                }
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE => {
                    let request =
                        sinomo_core::kv::decode_cluster_kv_authoritative_entry_rpc_request_body(
                            &request.body,
                        )
                        .expect("decode mounted exact-entry read");
                    let entry_call = self
                        .authoritative_entry_calls
                        .fetch_add(1, Ordering::SeqCst);
                    if matches!(
                        self.fault,
                        MountedTxnResponseFault::FailSecondPostCommitRead
                    ) && entry_call == 1
                    {
                        return Err(DfsPeerTransportError::new(
                            "injected second post-commit authority-read failure",
                        ));
                    }
                    let read_revision =
                        if matches!(self.fault, MountedTxnResponseFault::StalePostCommitRead) {
                            KvRevision(8)
                        } else {
                            KvRevision(9)
                        };
                    let response_key = if matches!(
                        self.fault,
                        MountedTxnResponseFault::WrongAuthoritativeEntryKey
                    ) {
                        "/dfs/foreign/entry".to_string()
                    } else {
                        request.key
                    };
                    sinomo_core::kv::cluster_kv_authoritative_entry_response_rpc_payload(
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: if matches!(
                                self.fault,
                                MountedTxnResponseFault::MissingAuthoritativeEntry
                            ) {
                                None
                            } else {
                                Some(KvEntry {
                                    key: response_key,
                                    value: b"authoritative".to_vec(),
                                    create_revision: read_revision,
                                    modify_revision: read_revision,
                                    version: sinomo_core::kv::KvVersion(1),
                                    lease_id: None,
                                })
                            },
                            authority: self.authority(read_revision),
                        },
                    )
                    .expect("encode mounted exact-entry response")
                }
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE => {
                    let request =
                        sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_request_body(
                            &request.body,
                        )
                        .expect("decode mounted exact-key-set read");
                    *self.exact_set_keys.lock().expect("exact-set keys") = request.keys.clone();
                    if matches!(self.fault, MountedTxnResponseFault::UnknownExactSetRoute) {
                        return Err(DfsPeerTransportError::from_network(
                            sinomo_core::network::NetworkError::UnknownRoute {
                                route: route.clone(),
                            },
                        ));
                    }
                    if matches!(self.fault, MountedTxnResponseFault::FailedExactSetRoute) {
                        return Err(DfsPeerTransportError::new(
                            "injected exact-key-set transport failure",
                        ));
                    }
                    sinomo_core::kv::cluster_kv_authoritative_entries_response_rpc_payload(
                        &request.keys,
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: request
                                .keys
                                .iter()
                                .map(|key| {
                                    Some(KvEntry {
                                        key: key.clone(),
                                        value: format!("authoritative:{key}").into_bytes(),
                                        create_revision: KvRevision(9),
                                        modify_revision: KvRevision(9),
                                        version: sinomo_core::kv::KvVersion(1),
                                        lease_id: None,
                                    })
                                })
                                .collect(),
                            authority: self.authority(KvRevision(9)),
                        },
                    )
                    .expect("encode mounted exact-key-set response")
                }
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE => {
                    let _request =
                        sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_request_body(
                            &request.body,
                        )
                        .expect("decode mounted authoritative snapshot read");
                    let keys = self.exact_set_keys.lock().expect("exact-set keys").clone();
                    let mut snapshot = empty_mounted_sifs_kv_snapshot(KvRevision(9));
                    snapshot.entries = keys
                        .iter()
                        .map(|key| KvEntry {
                            key: key.clone(),
                            value: format!("authoritative:{key}").into_bytes(),
                            create_revision: KvRevision(9),
                            modify_revision: KvRevision(9),
                            version: sinomo_core::kv::KvVersion(1),
                            lease_id: None,
                        })
                        .collect();
                    snapshot.canonicalize();
                    sinomo_core::kv::cluster_kv_authoritative_snapshot_response_rpc_payload(
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: snapshot,
                            authority: self.authority(KvRevision(9)),
                        },
                    )
                    .expect("encode mounted authoritative snapshot response")
                }
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE => {
                    let request = sinomo_core::kv::decode_cluster_kv_authoritative_range_prefix_rpc_request_body(
                        &request.body,
                    )
                    .expect("decode mounted exact-prefix read");
                    sinomo_core::kv::cluster_kv_authoritative_range_prefix_response_rpc_payload(
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: if matches!(
                                self.fault,
                                MountedTxnResponseFault::OutOfPrefixAuthoritativeEntry
                            ) {
                                vec![KvEntry {
                                    key: format!(
                                        "{}-sibling",
                                        request.prefix.trim_end_matches('/')
                                    ),
                                    value: b"foreign".to_vec(),
                                    create_revision: KvRevision(9),
                                    modify_revision: KvRevision(9),
                                    version: sinomo_core::kv::KvVersion(1),
                                    lease_id: None,
                                }]
                            } else {
                                Vec::new()
                            },
                            authority: self.authority(KvRevision(9)),
                        },
                    )
                    .expect("encode mounted exact-prefix response")
                }
                other => panic!("unexpected mounted KV route {other}"),
            };
            assert!(
                payload.body.len()
                    <= sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES
                        .saturating_add(1024 * 1024),
                "mounted control-plane fixture must remain bounded"
            );
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
        }
    }

    impl AsyncDfsPeerTransport for ScriptedMountedKvTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            let result = self.call_object_rpc(target_node_id, call, options);
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct BlockingMountedKvTransport {
        local_node_id: NodeId,
        calls: AtomicUsize,
        routes: Mutex<Vec<String>>,
        first_call_entered: Mutex<Option<mpsc::Sender<()>>>,
        release_first_call: (Mutex<bool>, Condvar),
    }

    impl BlockingMountedKvTransport {
        fn new(local_node_id: NodeId) -> (Self, mpsc::Receiver<()>) {
            let (first_call_entered, entered) = mpsc::channel();
            (
                Self {
                    local_node_id,
                    calls: AtomicUsize::new(0),
                    routes: Mutex::new(Vec::new()),
                    first_call_entered: Mutex::new(Some(first_call_entered)),
                    release_first_call: (Mutex::new(false), Condvar::new()),
                },
                entered,
            )
        }

        fn release_first_call(&self) {
            let (released, wake) = &self.release_first_call;
            *released.lock().expect("release first call") = true;
            wake.notify_all();
        }

        fn routes(&self) -> Vec<String> {
            self.routes.lock().expect("routes").clone()
        }

        fn authority(revision: KvRevision) -> sinomo_core::kv::KvAuthorityProof {
            sinomo_core::kv::KvAuthorityProof {
                term: 4,
                commit_index: revision.0.saturating_add(10),
                revision,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            }
        }
    }

    impl DfsPeerTransport for BlockingMountedKvTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            assert_eq!(target_node_id, &self.local_node_id);
            assert_eq!(
                call.request.route,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE
            );
            self.routes
                .lock()
                .expect("routes")
                .push(call.request.route.clone());
            let request = crate::node_transport::materialize_dfs_value_call_sync(call, 1024 * 1024)
                .expect("materialize blocked authority request");
            let request = sinomo_core::kv::decode_cluster_kv_authoritative_entry_rpc_request_body(
                &request.body,
            )
            .expect("decode blocked authority request");
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                if let Some(entered) = self
                    .first_call_entered
                    .lock()
                    .expect("first call entered")
                    .take()
                {
                    entered.send(()).expect("report first authority call");
                }
                let (released, wake) = &self.release_first_call;
                let mut released = released.lock().expect("release first call");
                while !*released {
                    released = wake.wait(released).expect("wait for first call release");
                }
            }
            let revision = KvRevision(if call_index == 0 { 40 } else { 41 });
            let value = if call_index == 0 {
                b"stale-generation".to_vec()
            } else {
                b"fresh-generation".to_vec()
            };
            let payload = sinomo_core::kv::cluster_kv_authoritative_entry_response_rpc_payload(
                &sinomo_core::kv::KvAuthoritativeRead {
                    value: Some(KvEntry {
                        key: request.key,
                        value,
                        create_revision: revision,
                        modify_revision: revision,
                        version: sinomo_core::kv::KvVersion(1),
                        lease_id: None,
                    }),
                    authority: Self::authority(revision),
                },
            )
            .expect("encode blocked authority response");
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
        }
    }

    #[derive(Debug)]
    struct TransitioningMountedKvTransport {
        local_node_id: NodeId,
        shard_key: String,
        handoff_key: String,
        current_shard_value: Vec<u8>,
        calls: AtomicUsize,
        requested_keys: Mutex<Vec<String>>,
    }

    impl TransitioningMountedKvTransport {
        fn authority(revision: KvRevision) -> sinomo_core::kv::KvAuthorityProof {
            sinomo_core::kv::KvAuthorityProof {
                term: 5,
                commit_index: revision.0.saturating_add(10),
                revision,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            }
        }
    }

    impl DfsPeerTransport for TransitioningMountedKvTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            assert_eq!(target_node_id, &self.local_node_id);
            assert_eq!(
                call.request.route,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE
            );
            let request = crate::node_transport::materialize_dfs_value_call_sync(
                call,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES
                    .saturating_add(1024 * 1024),
            )
            .expect("materialize transition authority request");
            let request =
                sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_request_body(
                    &request.body,
                )
                .expect("decode transition authority request");
            self.requested_keys
                .lock()
                .expect("transition requested keys")
                .extend(request.keys.iter().cloned());
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            let revision = KvRevision(51);
            let values = request
                .keys
                .iter()
                .map(|key| match key.as_str() {
                    key if key == self.shard_key => Some(KvEntry {
                        key: key.to_string(),
                        value: self.current_shard_value.clone(),
                        create_revision: revision,
                        modify_revision: revision,
                        version: sinomo_core::kv::KvVersion(1),
                        lease_id: None,
                    }),
                    key if key == self.handoff_key => None,
                    key => panic!("unexpected transition exact key {key}"),
                })
                .collect();
            let payload = sinomo_core::kv::cluster_kv_authoritative_entries_response_rpc_payload(
                &request.keys,
                &sinomo_core::kv::KvAuthoritativeRead {
                    value: values,
                    authority: Self::authority(revision),
                },
            )
            .expect("encode transition authority response");
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
    fn authoritative_control_plane_does_not_trust_stale_critical_cache() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-critical-cache-fence");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_critical_cache_fence").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_critical_cache_fence",
            )
            .expect("cluster id"),
            "mounted SIFS critical cache fence",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let key = "/config/mounted-sifs-critical";
        let mut stale = empty_mounted_sifs_kv_snapshot(KvRevision(1));
        stale.entries.push(KvEntry {
            key: key.to_string(),
            value: b"stale".to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        });
        stale.canonicalize();
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &stale)
            .expect("write stale materialized projection");

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::None,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let client =
            MountedSifsControlPlaneKvClient::new_authoritative(cluster_paths, peer_transport);

        for _ in 0..2 {
            assert_eq!(
                client
                    .get(key)
                    .expect("critical authoritative read")
                    .expect("authoritative entry")
                    .value,
                b"authoritative",
            );
        }
        assert_eq!(
            transport.authoritative_entry_calls.load(Ordering::SeqCst),
            1,
            "the first critical read must cross authority and later reads reuse its scope"
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
        let snapshot = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection");
        let mut client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, snapshot);

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
    fn mounted_control_plane_txn_refreshes_only_the_executed_branch() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-control-plane-ryw");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_control_plane_ryw").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_control_plane_ryw",
            )
            .expect("cluster id"),
            "mounted SIFS control plane RYW",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let mut oversized = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection")
            .as_ref()
            .clone();
        for index in 0..512 {
            oversized.entries.push(KvEntry {
                key: format!("/unrelated/{index:04}"),
                value: vec![0; 32],
                create_revision: oversized.revision,
                modify_revision: oversized.revision,
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            });
        }
        oversized.canonicalize();
        let initial_snapshot = Arc::new(oversized);

        for succeeded in [true, false] {
            let transport = Arc::new(ScriptedMountedKvTransport::new(
                local_node.clone(),
                succeeded,
                MountedTxnResponseFault::None,
            ));
            let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
            let mut client = MountedSifsControlPlaneKvClient::new(
                cluster_paths.clone(),
                peer_transport,
                Arc::clone(&initial_snapshot),
            );
            let response = client
                .txn(KvTxn {
                    compares: vec![sinomo_core::kv::KvCompare::Missing {
                        key: "/dfs/test/compare".to_string(),
                    }],
                    success: vec![
                        KvOp::Put {
                            key: "/dfs/test/success".to_string(),
                            value: b"success".to_vec(),
                            lease_id: None,
                        },
                        KvOp::DeletePrefix {
                            prefix: "/dfs/test/deleted/".to_string(),
                        },
                    ],
                    failure: vec![KvOp::Put {
                        key: "/dfs/test/failure".to_string(),
                        value: b"failure".to_vec(),
                        lease_id: None,
                    }],
                })
                .expect("mounted transaction");

            assert_eq!(response.succeeded, succeeded);
            assert_eq!(response.revision, KvRevision(9));
            let cached = client.cached_snapshot().expect("bounded RYW projection");
            let expected_key = if succeeded {
                "/dfs/test/success"
            } else {
                "/dfs/test/failure"
            };
            let skipped_key = if succeeded {
                "/dfs/test/failure"
            } else {
                "/dfs/test/success"
            };
            assert_eq!(
                cached.get(expected_key).map(|entry| entry.value.as_slice()),
                Some(b"authoritative".as_slice()),
                "the selected transaction branch must be visible before return"
            );
            assert!(cached.get(skipped_key).is_none());
            assert!(
                cached.entries.len() < 8,
                "exact mounted overlays must not clone the complete materialized keyspace"
            );
            let expected_routes = if succeeded {
                vec![
                    sinomo_core::kv::CLUSTER_KV_TXN_ROUTE.to_string(),
                    sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                    sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                ]
            } else {
                vec![
                    sinomo_core::kv::CLUSTER_KV_TXN_ROUTE.to_string(),
                    sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                ]
            };
            assert_eq!(transport.routes(), expected_routes);

            client.refresh_latest().expect("repeat bounded refresh");
            let refreshed = client.cached_snapshot().expect("repeated projection");
            assert!(
                Arc::ptr_eq(&cached, &refreshed),
                "repeat refresh must not re-import or clone the daemon materialized snapshot"
            );
        }
    }

    #[test]
    fn mounted_authoritative_scopes_cross_once_per_refresh_generation() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-scope-generation");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_scope_generation").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_scope_generation",
            )
            .expect("cluster id"),
            "mounted SIFS authority scope generation",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let snapshot = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection");
        let observational_entry = snapshot
            .entries
            .first()
            .expect("cluster projection entry")
            .clone();
        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::None,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, snapshot);

        assert_eq!(
            client
                .get(&observational_entry.key)
                .expect("initial observational point read"),
            Some(observational_entry),
        );
        assert!(
            transport.routes().is_empty(),
            "initial mounted reads must remain observational"
        );

        for expected_generation in 1..=2 {
            client
                .refresh_latest()
                .expect("advance mounted authority generation");
            for _ in 0..16 {
                assert!(
                    client
                        .get("/dfs/test/exact")
                        .expect("reuse exact mounted authority scope")
                        .is_some()
                );
            }
            for _ in 0..16 {
                assert!(
                    client
                        .range_prefix("/dfs/catalog/")
                        .expect("reuse broad mounted prefix scope")
                        .is_empty()
                );
            }
            assert!(
                client
                    .range_prefix("/dfs/catalog/nested/")
                    .expect("broad prefix covers a narrower prefix")
                    .is_empty()
            );
            assert!(
                client
                    .get("/dfs/catalog/absent")
                    .expect("broad prefix proves exact absence")
                    .is_none()
            );
            assert_eq!(
                transport.routes().len(),
                expected_generation * 2,
                "each reused exact scope must cross authority once per generation"
            );
        }
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
            ]
        );
    }

    #[test]
    fn repeated_mounted_path_reads_reuse_the_metadata_shard_catalog_certificate() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-path-read-certificate");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = NodeId::from_legacy_named("node_mounted_sifs_path_read_certificate")
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_path_read_certificate",
            )
            .expect("cluster id"),
            "mounted SIFS ordinary path read certificate reuse",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let unexpected_network_send: crate::DfsMessageSender =
            Arc::new(|_target_node_id, _command, _options| {
                Err(DfsPeerTransportError::new(
                    "local ordinary read must not use the DFS network",
                ))
            });
        let transport = DfsNodeTransport::with_message_sender_for_nodes(
            local_node.clone(),
            Vec::<NodeId>::new(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            unexpected_network_send,
        );
        let content = crate::LocalContentStore::new(data_dir.dfs_dir().join("content"))
            .expect("content store");
        let mut service = NetworkedDfsService::new(transport, content);
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap local SIFS before attaching clustered routing");
        service
            .posix_create_file(
                "/ordinary-read.txt",
                b"ordinary-read",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 1).expect("local policy"),
                    replica_candidates: vec![local_node.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .expect("seed ordinary read file");

        let kv_transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::None,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = kv_transport.clone();
        let mut control_plane = MountedSifsControlPlaneKvClient::new(
            cluster_paths,
            peer_transport,
            Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision(8))),
        );
        control_plane
            .refresh_latest()
            .expect("start mounted read authority generation");
        let mut service = service.with_boxed_control_plane(Box::new(control_plane));

        for _ in 0..8 {
            assert_eq!(
                service
                    .posix_read_file_range_distributed("/ordinary-read.txt", 0, 8)
                    .expect("ordinary mounted path read"),
                b"ordinary"
            );
        }
        assert_eq!(
            kv_transport.routes(),
            vec![sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string()],
            "the ordinary path-read stack must reuse its metadata-shards prefix certificate instead of issuing one ReadIndex per read"
        );
    }

    #[test]
    fn stale_mounted_authority_response_cannot_repopulate_a_new_generation() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-stale-generation");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_stale_generation").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_stale_generation",
            )
            .expect("cluster id"),
            "mounted SIFS stale generation",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let snapshot = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection");
        let (transport, first_call_entered) = BlockingMountedKvTransport::new(local_node);
        let transport = Arc::new(transport);
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, snapshot);
        client
            .refresh_latest()
            .expect("begin first mounted authority generation");

        let stale_reader = client.clone();
        let stale_read = thread::spawn(move || {
            stale_reader
                .get("/dfs/test/generation-race")
                .expect("complete stale-generation read")
        });
        first_call_entered
            .recv_timeout(Duration::from_secs(5))
            .expect("first authority read entered transport");
        client
            .refresh_latest()
            .expect("advance generation while prior read is in flight");
        transport.release_first_call();
        assert_eq!(
            stale_read
                .join()
                .expect("join stale-generation read")
                .expect("replanned current-generation value")
                .value,
            b"fresh-generation"
        );

        {
            let cache = client.snapshot.lock().expect("mounted projection cache");
            assert_eq!(cache.authoritative_generation, Some(2));
            assert_eq!(
                cache
                    .snapshot
                    .get("/dfs/test/generation-race")
                    .map(|entry| entry.value.as_slice()),
                Some(b"fresh-generation".as_slice()),
                "the reader must transparently replan into the current generation"
            );
            assert!(
                cache
                    .validated_authoritative_entries
                    .contains("/dfs/test/generation-race"),
                "only the replanned response may certify the scope"
            );
        }

        assert_eq!(
            client
                .get("/dfs/test/generation-race")
                .expect("revalidate in current generation")
                .expect("current-generation value")
                .value,
            b"fresh-generation"
        );
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            ]
        );
    }

    #[test]
    fn metadata_shard_authority_never_accepts_a_mixed_revision_transition() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-shard-transition");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_shard_transition").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_shard_transition",
            )
            .expect("cluster id"),
            "mounted SIFS shard transition",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let snapshot = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection");

        let expected = crate::DfsMetadataShardConfig {
            file_system_id: sinomo_core::Id::from_legacy_named(
                "filesystem_mounted_sifs_shard_transition",
            )
            .expect("filesystem id"),
            shard_id: "root".to_string(),
            primary_node_id: local_node.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut current = expected.clone();
        current.epoch = 2;
        let shard_key = crate::metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key = crate::metadata_shard_membership_handoff_key(
            &expected.file_system_id,
            &expected.shard_id,
        );
        let transport = Arc::new(TransitioningMountedKvTransport {
            local_node_id: local_node,
            shard_key: shard_key.clone(),
            handoff_key: handoff_key.clone(),
            current_shard_value: sinomo_core::kv::encode_kv_value(&current)
                .expect("encode current shard"),
            calls: AtomicUsize::new(0),
            requested_keys: Mutex::new(Vec::new()),
        });
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, snapshot);

        assert!(
            !crate::DfsControlPlaneStore::new(&mut client)
                .metadata_shard_authority_is_current(&expected)
                .expect("revision-pinned shard authority check"),
            "the retired r50 assignment must not be combined with r51 handoff absence"
        );
        assert_eq!(
            transport
                .requested_keys
                .lock()
                .expect("transition requested keys")
                .as_slice(),
            &[shard_key.clone(), handoff_key.clone(),],
            "one exact-key-set request must observe the transition coherently"
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        let cache = client.snapshot.lock().expect("mounted projection cache");
        assert_eq!(cache.snapshot.revision, KvRevision(51));
        assert_eq!(
            cache
                .snapshot
                .get(&shard_key)
                .expect("coherent current shard")
                .decode::<crate::DfsMetadataShardConfig>()
                .expect("decode coherent current shard"),
            current
        );
        assert!(cache.snapshot.get(&handoff_key).is_none());
        assert!(cache.validated_authoritative_entries.contains(&shard_key));
        assert!(cache.validated_authoritative_entries.contains(&handoff_key));
    }

    #[test]
    fn mounted_exact_set_uses_one_snapshot_only_for_a_definite_legacy_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-legacy-exact-set");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_legacy_exact_set").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_legacy_exact_set",
            )
            .expect("cluster id"),
            "mounted SIFS legacy exact-set fallback",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let keys = vec![
            "/dfs/test/legacy-a".to_string(),
            "/dfs/test/legacy-b".to_string(),
        ];

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::UnknownExactSetRoute,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client = MountedSifsControlPlaneKvClient::new(
            cluster_paths.clone(),
            peer_transport,
            Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision(8))),
        );
        client
            .refresh_latest()
            .expect("begin mounted authority generation");
        let values = client
            .get_consistent(&keys)
            .expect("legacy daemon snapshot projection fallback");
        assert_eq!(values.len(), keys.len());
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(
                value.as_ref().map(|entry| entry.key.as_str()),
                Some(key.as_str())
            );
        }
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE.to_string(),
            ],
            "mixed-version fallback must project the complete key set from one authoritative snapshot"
        );

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::FailedExactSetRoute,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client = MountedSifsControlPlaneKvClient::new(
            cluster_paths,
            peer_transport,
            Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision(8))),
        );
        client
            .refresh_latest()
            .expect("begin strict mounted authority generation");
        let error = client
            .get_consistent(&keys)
            .expect_err("an ambiguous transport failure must not use compatibility fallback");
        assert!(
            error
                .to_string()
                .contains("exact-key-set transport failure")
        );
        assert_eq!(
            transport.routes(),
            vec![sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string()],
            "only a definite UnknownRoute may use the snapshot compatibility path"
        );
    }

    #[test]
    fn mounted_authority_scope_markers_are_bounded() {
        let mut cache = MountedSifsKvSnapshotCache {
            snapshot: Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision::ZERO)),
            authoritative_generation: None,
            validated_authoritative_entries: BTreeSet::new(),
            validated_authoritative_prefixes: BTreeSet::new(),
            validated_authoritative_scope_bytes: 0,
        };
        cache.begin_authoritative_generation();
        for index in 0..MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES.saturating_add(32) {
            cache.mark_entry_validated(&format!("/dfs/exact/{index}"));
            cache.mark_prefix_validated(&format!("/dfs/prefix/{index}/"));
        }
        assert_eq!(
            cache
                .validated_authoritative_entries
                .len()
                .saturating_add(cache.validated_authoritative_prefixes.len()),
            sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            "the aggregate exact-scope certificate set must remain bounded"
        );

        cache.clear_authoritative_scopes();
        let long_component = "x".repeat(4096);
        for index in 0..MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES {
            cache.mark_entry_validated(&format!("/dfs/byte-bounded/{index:04}/{long_component}"));
        }
        assert!(
            cache.validated_authoritative_scope_bytes
                <= MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES
        );
        assert!(
            cache.authoritative_scope_count() < MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES,
            "the aggregate byte bound must stop long absence certificates before the count bound"
        );
    }

    #[test]
    fn mounted_authority_projection_evicts_before_distinct_scope_capacity_growth() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-projection-capacity");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_projection_capacity").expect("node id");
        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::None,
        ));

        let mut snapshot = empty_mounted_sifs_kv_snapshot(KvRevision(1));
        snapshot.entries = (0..MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES)
            .map(|index| KvEntry {
                key: format!("/dfs/capacity/{index:04}"),
                value: vec![index as u8],
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            })
            .collect();
        snapshot.canonicalize();
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, Arc::new(snapshot));
        {
            let mut cache = client.snapshot.lock().expect("mounted projection cache");
            cache.authoritative_generation = Some(1);
            for index in 0..MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES {
                cache.mark_entry_validated(&format!("/dfs/capacity/{index:04}"));
            }
        }

        let new_key = "/dfs/capacity/overflow";
        assert!(
            client
                .merge_authoritative_entry(
                    new_key,
                    &sinomo_core::kv::KvAuthoritativeRead {
                        value: Some(KvEntry {
                            key: new_key.to_string(),
                            value: b"bounded".to_vec(),
                            create_revision: KvRevision(2),
                            modify_revision: KvRevision(2),
                            version: sinomo_core::kv::KvVersion(1),
                            lease_id: None,
                        }),
                        authority: transport.authority(KvRevision(2)),
                    },
                    1,
                )
                .expect("merge overflow scope")
        );
        let cache = client.snapshot.lock().expect("mounted projection cache");
        assert_eq!(cache.snapshot.entries.len(), 1);
        assert_eq!(cache.authoritative_scope_count(), 1);
        assert!(
            cache
                .snapshot
                .entries
                .iter()
                .all(|entry| cache.entry_is_validated(&entry.key)),
            "every retained projection entry must have an exact or prefix certificate"
        );
        assert!(
            cache
                .snapshot
                .entries
                .len()
                .saturating_add(cache.authoritative_scope_count())
                <= MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES.saturating_mul(2),
            "projection entries plus certificates must remain O(capacity)"
        );
        assert!(cache.validated_authoritative_entries.contains(new_key));
        assert!(mounted_sifs_authoritative_projection_entries_fit(
            &cache.snapshot.entries
        ));
        drop(cache);
        assert!(matches!(
            client
                .authoritative_entry_read_plan("/dfs/capacity/0000")
                .expect("plan evicted scope"),
            MountedSifsKvReadPlan::Authoritative { .. }
        ));
    }

    #[test]
    fn mounted_authority_absolute_deadline_bounds_dispatch_and_fallback() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-authority-deadline");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_authority_deadline").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_authority_deadline",
            )
            .expect("cluster id"),
            "mounted SIFS authority deadline",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::UnknownExactSetRoute,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client =
            MountedSifsControlPlaneKvClient::new_authoritative(cluster_paths, peer_transport);
        let keys = vec!["/dfs/deadline/key".to_string()];

        let error = client
            .get_authoritative_consistent_before(
                &keys,
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("past instant"),
            )
            .expect_err("expired deadline must reject before dispatch");
        assert!(error.to_string().contains("deadline elapsed"));
        assert!(transport.routes().is_empty());

        let budget = Duration::from_secs(2);
        client
            .get_authoritative_consistent_before(&keys, Instant::now() + budget)
            .expect("deadline-bounded exact read with mixed-version fallback");
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE.to_string(),
            ]
        );
        let options = transport.options();
        assert_eq!(options.len(), 2);
        assert!(options.iter().all(|options| {
            options
                .overall_timeout
                .is_some_and(|remaining| !remaining.is_zero() && remaining <= budget)
        }));
        assert!(
            options[1].overall_timeout <= options[0].overall_timeout,
            "fallback must consume the original absolute deadline"
        );
    }

    #[test]
    fn mounted_authority_rejects_entries_outside_the_requested_scope() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-authority-containment");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = NodeId::from_legacy_named("node_mounted_sifs_authority_containment")
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_authority_containment",
            )
            .expect("cluster id"),
            "mounted SIFS authority reply containment",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::WrongAuthoritativeEntryKey,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport;
        let mut exact_client = MountedSifsControlPlaneKvClient::new(
            cluster_paths.clone(),
            peer_transport,
            Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision(8))),
        );
        exact_client
            .refresh_latest()
            .expect("begin exact authority generation");
        let error = exact_client
            .get("/dfs/requested/entry")
            .expect_err("a mismatched exact-key reply must be rejected");
        assert!(error.to_string().contains("returned entry"));
        {
            let cache = exact_client
                .snapshot
                .lock()
                .expect("exact projection cache");
            assert!(!cache.entry_is_validated("/dfs/requested/entry"));
            assert!(cache.snapshot.get("/dfs/foreign/entry").is_none());
        }

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::OutOfPrefixAuthoritativeEntry,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport;
        let mut prefix_client = MountedSifsControlPlaneKvClient::new(
            cluster_paths,
            peer_transport,
            Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision(8))),
        );
        prefix_client
            .refresh_latest()
            .expect("begin prefix authority generation");
        let error = prefix_client
            .range_prefix("/dfs/requested/")
            .expect_err("an out-of-prefix reply must be rejected");
        assert!(error.to_string().contains("returned entry"));
        let cache = prefix_client
            .snapshot
            .lock()
            .expect("prefix projection cache");
        assert!(!cache.prefix_is_validated("/dfs/requested/"));
        assert!(cache.snapshot.get("/dfs/requested-sibling").is_none());
    }

    #[test]
    fn mounted_txn_revalidates_only_mutated_scopes_in_the_current_generation() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-txn-scope-invalidation");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = NodeId::from_legacy_named("node_mounted_sifs_txn_scope_invalidation")
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_txn_scope_invalidation",
            )
            .expect("cluster id"),
            "mounted SIFS transaction scope invalidation",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let snapshot = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection");
        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::None,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, snapshot);
        client
            .refresh_latest()
            .expect("begin mounted authority generation");
        client
            .get("/dfs/test/affected")
            .expect("validate affected key");
        client
            .get("/dfs/test/unaffected")
            .expect("validate unaffected key");
        client
            .range_prefix("/dfs/other/")
            .expect("validate unrelated prefix");
        assert_eq!(transport.routes().len(), 3);

        let response = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/dfs/test/affected".to_string(),
                    value: b"committed".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect("commit affected mounted scope");
        assert_eq!(response.revision, KvRevision(9));
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_TXN_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            ],
            "the transaction must revalidate only its executed mutation scope"
        );

        client
            .get("/dfs/test/affected")
            .expect("reuse revalidated affected key");
        client
            .get("/dfs/test/unaffected")
            .expect("reuse unaffected key certificate");
        client
            .range_prefix("/dfs/other/")
            .expect("reuse unrelated prefix certificate");
        assert_eq!(
            transport.routes().len(),
            5,
            "post-commit reads must reuse both the repaired and unaffected scopes"
        );
        let cached = client.cached_snapshot().expect("mounted exact projection");
        assert_eq!(cached.revision, KvRevision(9));
        assert_eq!(
            cached
                .get("/dfs/test/affected")
                .expect("affected key in exact projection")
                .modify_revision,
            KvRevision(9),
            "transaction revalidation must meet the commit revision"
        );
    }

    #[test]
    fn mounted_txn_failure_mid_revalidation_retires_every_committed_scope() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-txn-middle-read-failure");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = NodeId::from_legacy_named("node_mounted_sifs_txn_middle_read_failure")
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_txn_middle_read_failure",
            )
            .expect("cluster id"),
            "mounted SIFS transaction middle read failure",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let keys = [
            "/dfs/test/txn-a".to_string(),
            "/dfs/test/txn-b".to_string(),
            "/dfs/test/txn-c".to_string(),
        ];
        let mut snapshot = empty_mounted_sifs_kv_snapshot(KvRevision(8));
        snapshot.entries = keys
            .iter()
            .map(|key| KvEntry {
                key: key.clone(),
                value: b"stale".to_vec(),
                create_revision: KvRevision(8),
                modify_revision: KvRevision(8),
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            })
            .collect();
        snapshot.canonicalize();
        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::FailSecondPostCommitRead,
        ));
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let mut client =
            MountedSifsControlPlaneKvClient::new(cluster_paths, peer_transport, Arc::new(snapshot));
        {
            let mut cache = client.snapshot.lock().expect("mounted projection cache");
            cache.authoritative_generation = Some(1);
            for key in &keys {
                cache.mark_entry_validated(key);
            }
        }

        let error = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: keys
                    .iter()
                    .map(|key| KvOp::Put {
                        key: key.clone(),
                        value: b"committed".to_vec(),
                        lease_id: None,
                    })
                    .collect(),
                failure: Vec::new(),
            })
            .expect_err("second committed-scope read must fail");
        assert!(
            error
                .to_string()
                .contains("injected second post-commit authority-read failure"),
            "unexpected revalidation error: {error}"
        );
        {
            let cache = client.snapshot.lock().expect("mounted projection cache");
            assert!(cache.validated_authoritative_entries.contains(&keys[0]));
            assert!(!cache.validated_authoritative_entries.contains(&keys[1]));
            assert!(
                !cache.validated_authoritative_entries.contains(&keys[2]),
                "the unvisited third committed key must not retain its stale certificate"
            );
        }
        assert_eq!(
            client
                .get(&keys[2])
                .expect("unvisited key must cross authority")
                .expect("authoritative third key")
                .value,
            b"authoritative"
        );
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_core::kv::CLUSTER_KV_TXN_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            ]
        );
    }

    #[test]
    fn mounted_control_plane_rejects_unbound_or_stale_txn_proofs() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-control-plane-proof");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_control_plane_proof").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_control_plane_proof",
            )
            .expect("cluster id"),
            "mounted SIFS control plane proof",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let snapshot = mounted_sifs_required_materialized_snapshot(&cluster_paths)
            .expect("materialized projection");

        for fault in [
            MountedTxnResponseFault::MismatchedRequestId,
            MountedTxnResponseFault::MismatchedDigest,
            MountedTxnResponseFault::InvalidAuthority,
            MountedTxnResponseFault::StaleAuthorityRevision,
            MountedTxnResponseFault::StalePostCommitRead,
        ] {
            let transport = Arc::new(ScriptedMountedKvTransport::new(
                local_node.clone(),
                true,
                fault,
            ));
            let peer_transport: Arc<dyn DfsPeerTransport> = transport;
            let mut client = MountedSifsControlPlaneKvClient::new(
                cluster_paths.clone(),
                peer_transport,
                Arc::clone(&snapshot),
            );
            let error = client
                .txn(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key: "/dfs/test/rejected".to_string(),
                        value: Vec::new(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .expect_err("unbound or stale mounted transaction proof");
            assert!(
                matches!(
                    error,
                    KvError::Serialization { .. } | KvError::AuthorityUnavailable { .. }
                ),
                "unexpected rejection for {fault:?}: {error}"
            );
        }
    }

    #[test]
    fn clustered_mounted_open_uses_daemon_authority_without_a_materialized_projection() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-no-projection");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_no_projection").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_no_projection")
                .expect("cluster id"),
            "mounted SIFS no projection",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        assert!(!metadata_root.exists());

        let peer = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::MissingAuthoritativeEntry,
        ));
        let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let opened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
            data_dir.clone(),
            sync_peer_transport,
            async_peer_transport,
        )
        .expect("clustered mount from daemon authority");
        assert_eq!(opened.service.node_id(), &local_node);
        assert!(
            metadata_root.exists(),
            "successful open must author metadata"
        );
        assert!(peer.routes().iter().any(|route| {
            route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
        }));

        fs::write(&cluster_paths.kv_paths().snapshot, b"not a SOBJ snapshot")
            .expect("corrupt materialized projection");
        let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let reopened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
            data_dir,
            sync_peer_transport,
            async_peer_transport,
        )
        .expect("corrupt observational projection must not affect authoritative open");
        assert_eq!(reopened.service.node_id(), &local_node);
    }

    #[test]
    fn mounted_opener_never_replays_daemon_owned_raft() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-open-no-raft-replay");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_open_no_raft").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_open_no_raft")
                .expect("cluster id"),
            "mounted SIFS open without Raft replay",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("materialized cluster KV state");
        assert!(
            mounted_sifs_materialized_snapshot(&cluster_paths)
                .expect("materialized projection")
                .is_some()
        );

        // A durable-state marker makes any current-KV helper attempt cold
        // replay. The directory sentinel turns opening node.lock into a
        // deterministic failure even without a second process.
        let wal_paths = cluster_paths.kv_paths().raft_wal_paths(&local_node);
        fs::create_dir_all(&wal_paths.root).expect("Raft root");
        fs::write(&wal_paths.wal, b"").expect("durable-state marker");
        // Fixture initialization may leave the unlocked lock-file inode in
        // place. Replace that test-owned file with a directory so any later
        // attempt to open the daemon's Raft node fails deterministically.
        if wal_paths.lock.exists() {
            fs::remove_file(&wal_paths.lock).expect("remove fixture lock file");
        }
        fs::create_dir(&wal_paths.lock).expect("inaccessible node lock sentinel");

        let peer = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::MissingAuthoritativeEntry,
        ));
        let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let opened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
            data_dir,
            sync_peer_transport,
            async_peer_transport,
        )
        .expect("open mounted SIFS from daemon authority");

        assert_eq!(opened.service.node_id(), &local_node);
        let routes = peer.routes();
        assert!(!routes.is_empty(), "clustered open must consult authority");
        assert!(routes.iter().all(|route| {
            route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE
                || route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE
                || route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
        }));
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
pub struct MountedSifsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    snapshot: Arc<Mutex<MountedSifsKvSnapshotCache>>,
}

#[derive(Debug)]
struct MountedSifsKvSnapshotCache {
    snapshot: Arc<KvSnapshot>,
    /// `None` preserves the initial daemon-materialized observational view.
    /// Every explicit refresh advances the generation and requires each exact
    /// scope to cross one fresh authority barrier before it can be reused.
    authoritative_generation: Option<u64>,
    validated_authoritative_entries: BTreeSet<String>,
    validated_authoritative_prefixes: BTreeSet<String>,
    validated_authoritative_scope_bytes: usize,
}

impl MountedSifsKvSnapshotCache {
    fn authoritative_scope_count(&self) -> usize {
        self.validated_authoritative_entries
            .len()
            .saturating_add(self.validated_authoritative_prefixes.len())
    }

    fn clear_authoritative_scopes(&mut self) {
        self.validated_authoritative_entries.clear();
        self.validated_authoritative_prefixes.clear();
        self.validated_authoritative_scope_bytes = 0;
    }

    fn can_admit_authoritative_scopes(
        &self,
        additional_count: usize,
        additional_bytes: usize,
    ) -> bool {
        self.authoritative_scope_count()
            .saturating_add(additional_count)
            <= MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES
            && self
                .validated_authoritative_scope_bytes
                .saturating_add(additional_bytes)
                <= MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES
    }

    fn begin_authoritative_generation(&mut self) -> u64 {
        if self.authoritative_generation.is_none() {
            // Never retain the complete daemon projection once a caller asks
            // for authoritative data. The compact private view is populated
            // only by the exact keys and prefixes this mount actually reads.
            self.snapshot = Arc::new(empty_mounted_sifs_kv_snapshot(self.snapshot.revision));
        }
        let generation = self
            .authoritative_generation
            .and_then(|generation| generation.checked_add(1))
            .unwrap_or(1);
        self.authoritative_generation = Some(generation);
        self.clear_authoritative_scopes();
        generation
    }

    fn ensure_authoritative_generation(&mut self) -> u64 {
        match self.authoritative_generation {
            Some(generation) => generation,
            None => self.begin_authoritative_generation(),
        }
    }

    fn entry_is_validated(&self, key: &str) -> bool {
        self.validated_authoritative_entries.contains(key)
            || self
                .validated_authoritative_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
    }

    fn prefix_is_validated(&self, prefix: &str) -> bool {
        self.validated_authoritative_prefixes
            .iter()
            .any(|validated| prefix.starts_with(validated))
    }

    fn invalidate_entry_scope(&mut self, key: &str) {
        if self.validated_authoritative_entries.remove(key) {
            self.validated_authoritative_scope_bytes = self
                .validated_authoritative_scope_bytes
                .saturating_sub(key.len());
        }
        let removed_prefix_bytes = self
            .validated_authoritative_prefixes
            .iter()
            .filter(|prefix| key.starts_with(prefix.as_str()))
            .map(String::len)
            .sum::<usize>();
        self.validated_authoritative_prefixes
            .retain(|prefix| !key.starts_with(prefix));
        self.validated_authoritative_scope_bytes = self
            .validated_authoritative_scope_bytes
            .saturating_sub(removed_prefix_bytes);
    }

    fn invalidate_prefix_scope(&mut self, prefix: &str) {
        let removed_entry_bytes = self
            .validated_authoritative_entries
            .iter()
            .filter(|key| key.starts_with(prefix))
            .map(String::len)
            .sum::<usize>();
        self.validated_authoritative_entries
            .retain(|key| !key.starts_with(prefix));
        let removed_prefix_bytes = self
            .validated_authoritative_prefixes
            .iter()
            .filter(|validated| {
                validated.starts_with(prefix) || prefix.starts_with(validated.as_str())
            })
            .map(String::len)
            .sum::<usize>();
        self.validated_authoritative_prefixes
            .retain(|validated| !validated.starts_with(prefix) && !prefix.starts_with(validated));
        self.validated_authoritative_scope_bytes = self
            .validated_authoritative_scope_bytes
            .saturating_sub(removed_entry_bytes.saturating_add(removed_prefix_bytes));
    }

    fn mark_entry_validated(&mut self, key: &str) {
        if self.validated_authoritative_entries.contains(key) {
            return;
        }
        if self.can_admit_authoritative_scopes(1, key.len()) {
            self.validated_authoritative_entries.insert(key.to_string());
            self.validated_authoritative_scope_bytes = self
                .validated_authoritative_scope_bytes
                .saturating_add(key.len());
        }
    }

    fn mark_prefix_validated(&mut self, prefix: &str) {
        if self.validated_authoritative_prefixes.contains(prefix) {
            return;
        }
        if self.can_admit_authoritative_scopes(1, prefix.len()) {
            self.validated_authoritative_prefixes
                .insert(prefix.to_string());
            self.validated_authoritative_scope_bytes = self
                .validated_authoritative_scope_bytes
                .saturating_add(prefix.len());
        }
    }
}

fn mounted_sifs_authoritative_projection_entries_fit(entries: &[KvEntry]) -> bool {
    if entries.len() > MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES {
        return false;
    }
    entries
        .iter()
        .try_fold(0_usize, |payload_bytes, entry| {
            payload_bytes.checked_add(entry.key.len().saturating_add(entry.value.len()))
        })
        .is_some_and(|payload_bytes| {
            payload_bytes <= MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES
        })
}

#[derive(Debug)]
enum MountedSifsKvReadPlan<T> {
    Cached(T),
    Authoritative {
        generation: u64,
        minimum_revision: KvRevision,
    },
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

impl MountedSifsControlPlaneKvClient {
    /// Creates a compact private projection whose first read of each exact key
    /// or prefix is established through the daemon's ReadIndex route. The
    /// shared materialized snapshot is observational and is never used to seed
    /// correctness-sensitive DFS control-plane state.
    pub fn new_authoritative(
        cluster_paths: ClusterPaths,
        peer_transport: Arc<dyn DfsPeerTransport>,
    ) -> Self {
        let client = Self::new(
            cluster_paths,
            peer_transport,
            Arc::new(empty_mounted_sifs_kv_snapshot(KvRevision::ZERO)),
        );
        client
            .snapshot
            .lock()
            .expect("new mounted SIFS projection mutex")
            .begin_authoritative_generation();
        client
    }

    fn new(
        cluster_paths: ClusterPaths,
        peer_transport: Arc<dyn DfsPeerTransport>,
        snapshot: Arc<KvSnapshot>,
    ) -> Self {
        Self {
            cluster_paths,
            peer_transport,
            snapshot: Arc::new(Mutex::new(MountedSifsKvSnapshotCache {
                snapshot,
                authoritative_generation: None,
                validated_authoritative_entries: BTreeSet::new(),
                validated_authoritative_prefixes: BTreeSet::new(),
                validated_authoritative_scope_bytes: 0,
            })),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        mounted_sifs_kv_io_error(&self.cluster_paths, source)
    }

    #[cfg(test)]
    fn cached_snapshot(&self) -> Result<Arc<KvSnapshot>, KvError> {
        self.snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))
            .map(|cache| Arc::clone(&cache.snapshot))
    }

    fn begin_authoritative_refresh_generation(&self) -> Result<u64, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        Ok(cache.begin_authoritative_generation())
    }

    fn ensure_authoritative_projection(&self) -> Result<u64, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        Ok(cache.ensure_authoritative_generation())
    }

    fn authoritative_entry_read_plan(
        &self,
        key: &str,
    ) -> Result<MountedSifsKvReadPlan<Option<KvEntry>>, KvError> {
        let cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        let cached = || cache.snapshot.get(key).cloned();
        let Some(generation) = cache.authoritative_generation else {
            return Ok(MountedSifsKvReadPlan::Cached(cached()));
        };
        if cache.entry_is_validated(key) {
            return Ok(MountedSifsKvReadPlan::Cached(cached()));
        }
        Ok(MountedSifsKvReadPlan::Authoritative {
            generation,
            minimum_revision: cache.snapshot.revision,
        })
    }

    fn authoritative_prefix_read_plan(
        &self,
        prefix: &str,
    ) -> Result<MountedSifsKvReadPlan<Vec<KvEntry>>, KvError> {
        let cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        let cached = || {
            cache
                .snapshot
                .range_prefix_limited(prefix, sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        };
        let Some(generation) = cache.authoritative_generation else {
            return cached().map(MountedSifsKvReadPlan::Cached);
        };
        if cache.prefix_is_validated(prefix) {
            return cached().map(MountedSifsKvReadPlan::Cached);
        }
        Ok(MountedSifsKvReadPlan::Authoritative {
            generation,
            minimum_revision: cache.snapshot.revision,
        })
    }

    fn authoritative_read_minimum_revision(
        &self,
        requested: Option<KvRevision>,
    ) -> Result<KvRevision, KvError> {
        let cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        Ok(requested
            .unwrap_or(KvRevision::ZERO)
            .max(cache.snapshot.revision))
    }

    fn invalidate_authoritative_entry_scope(
        &self,
        generation: u64,
        key: &str,
    ) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        if cache.authoritative_generation == Some(generation) {
            cache.invalidate_entry_scope(key);
        }
        Ok(())
    }

    fn invalidate_authoritative_prefix_scope(
        &self,
        generation: u64,
        prefix: &str,
    ) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        if cache.authoritative_generation == Some(generation) {
            cache.invalidate_prefix_scope(prefix);
        }
        Ok(())
    }

    fn invalidate_authoritative_mutation_scopes(
        &self,
        generation: u64,
        keys: &BTreeSet<&str>,
        prefixes: &BTreeSet<&str>,
        invalidate_all: bool,
    ) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation) {
            return Ok(());
        }
        if invalidate_all {
            // Lease revocation can delete any key attached to the lease, and
            // the compact projection deliberately carries no lease index.
            cache.clear_authoritative_scopes();
            return Ok(());
        }
        for prefix in prefixes {
            cache.invalidate_prefix_scope(prefix);
        }
        for key in keys {
            cache.invalidate_entry_scope(key);
        }
        Ok(())
    }

    fn call_local_kv_rpc(
        &self,
        call: ObjectRpcCall,
        expected_route: &str,
        max_response_bytes: usize,
    ) -> Result<ObjectRpcResponse, KvError> {
        self.call_local_kv_rpc_raw(call, expected_route, max_response_bytes, None)
            .map_err(|source| self.kv_error(source))
    }

    fn call_local_kv_rpc_raw(
        &self,
        call: ObjectRpcCall,
        expected_route: &str,
        max_response_bytes: usize,
        deadline: Option<Instant>,
    ) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        // Compute the remaining budget immediately before transport dispatch;
        // reading local cluster state and any preceding mixed-version attempt
        // consumes the same absolute operation deadline.
        let options = match deadline {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(DfsPeerTransportError::new(
                        "metadata shard authority read deadline elapsed",
                    ));
                };
                if remaining.is_zero() {
                    return Err(DfsPeerTransportError::new(
                        "metadata shard authority read deadline elapsed",
                    ));
                }
                DfsObjectRpcSendOptions::default().with_overall_timeout(remaining)
            }
            None => DfsObjectRpcSendOptions::default(),
        };
        let result = self
            .peer_transport
            .call_object_rpc(&state.local_node_id, call, options)?;
        let response = mounted_sifs_value_response_from_rpc_result(result, max_response_bytes)?;
        if response.route != expected_route {
            return Err(DfsPeerTransportError::new(format!(
                "mounted SIFS cluster KV route {expected_route} returned mismatched route {}",
                response.route
            )));
        }
        response
            .error_detail()
            .map_err(DfsPeerTransportError::from_network)?;
        Ok(response)
    }

    fn read_authoritative_entry(
        &self,
        key: &str,
        minimum_revision: Option<KvRevision>,
        generation: u64,
    ) -> Result<Option<sinomo_core::kv::KvAuthoritativeRead<Option<KvEntry>>>, KvError> {
        let read = self.fetch_authoritative_entry(key, minimum_revision)?;
        if !self.merge_authoritative_entry(key, &read, generation)? {
            return Ok(None);
        }
        Ok(Some(read))
    }

    fn fetch_authoritative_entry(
        &self,
        key: &str,
        minimum_revision: Option<KvRevision>,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Option<KvEntry>>, KvError> {
        let minimum_revision = self.authoritative_read_minimum_revision(minimum_revision)?;
        let payload = sinomo_core::kv::cluster_kv_authoritative_entry_rpc_payload(key)?;
        let route = payload.route;
        let call = ObjectRpcCall::value(sinomo_core::network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        ));
        let response = self.call_local_kv_rpc(call, &route, 1024 * 1024)?;
        let read = sinomo_core::kv::decode_cluster_kv_authoritative_entry_rpc_body(&response.body)?;
        self.ensure_authoritative_revision(read.authority.revision, Some(minimum_revision), key)?;
        if read.value.as_ref().is_some_and(|entry| entry.key != key) {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative mounted SIFS read for {key:?} returned entry for {:?}",
                    read.value.as_ref().expect("checked entry").key,
                ),
            });
        }
        Ok(read)
    }

    fn fetch_authoritative_entries(
        &self,
        keys: &[String],
        minimum_revision: KvRevision,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        self.fetch_authoritative_entries_with_deadline(keys, minimum_revision, None)
    }

    fn fetch_authoritative_entries_before(
        &self,
        keys: &[String],
        minimum_revision: KvRevision,
        deadline: Instant,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        self.fetch_authoritative_entries_with_deadline(keys, minimum_revision, Some(deadline))
    }

    fn fetch_authoritative_entries_with_deadline(
        &self,
        keys: &[String],
        minimum_revision: KvRevision,
        deadline: Option<Instant>,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        let minimum_revision = self.authoritative_read_minimum_revision(Some(minimum_revision))?;
        sinomo_core::kv::validate_cluster_kv_authoritative_entries_request_keys(keys)?;
        let route = sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE;
        let call = ObjectRpcCall::encode_value(
            route,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            sinomo_core::kv::ClusterKvAuthoritativeEntriesRequest {
                keys: keys.to_vec(),
            },
        )
        .map_err(|source| self.kv_error(source))?;
        let response = match self.call_local_kv_rpc_raw(
            call,
            route,
            sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES
                .saturating_add(1024 * 1024),
            deadline,
        ) {
            Ok(response) => response,
            Err(error) if error.is_unknown_route(route) => {
                let payload = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
                let snapshot_route = payload.route;
                let call = ObjectRpcCall::value(
                    sinomo_core::network::ObjectRpcRequest::from_encoded_body(
                        payload.route,
                        payload.category,
                        payload.body,
                    ),
                );
                let response = self
                    .call_local_kv_rpc_raw(
                        call,
                        snapshot_route,
                        (sinomo_core::kv::MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES as usize)
                            .saturating_add(1024 * 1024),
                        deadline,
                    )
                    .map_err(|source| self.kv_error(source))?;
                let snapshot = sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(
                    &response.body,
                )?;
                self.ensure_authoritative_revision(
                    snapshot.authority.revision,
                    Some(minimum_revision),
                    "mixed-version exact-key snapshot",
                )?;
                return sinomo_core::kv::project_cluster_kv_authoritative_entries(&snapshot, keys);
            }
            Err(error) => return Err(self.kv_error(error)),
        };
        let read = sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_body(
            &response.body,
            keys,
        )?;
        self.ensure_authoritative_revision(
            read.authority.revision,
            Some(minimum_revision),
            "exact-key set",
        )?;
        Ok(read)
    }

    fn read_authoritative_prefix(
        &self,
        prefix: &str,
        minimum_revision: Option<KvRevision>,
        generation: u64,
    ) -> Result<Option<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>>, KvError> {
        let minimum_revision = self.authoritative_read_minimum_revision(minimum_revision)?;
        let payload = sinomo_core::kv::cluster_kv_authoritative_range_prefix_rpc_payload(prefix)?;
        let route = payload.route;
        let call = ObjectRpcCall::value(sinomo_core::network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        ));
        let response = self.call_local_kv_rpc(
            call,
            &route,
            sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES.saturating_add(1024 * 1024),
        )?;
        let read =
            sinomo_core::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(&response.body)?;
        self.ensure_authoritative_revision(
            read.authority.revision,
            Some(minimum_revision),
            prefix,
        )?;
        if let Some(entry) = read
            .value
            .iter()
            .find(|entry| !entry.key.starts_with(prefix))
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative mounted SIFS prefix read for {prefix:?} returned entry for {:?}",
                    entry.key,
                ),
            });
        }
        if !self.merge_authoritative_prefix(prefix, &read, generation)? {
            return Ok(None);
        }
        Ok(Some(read))
    }

    fn ensure_authoritative_revision(
        &self,
        observed: KvRevision,
        minimum: Option<KvRevision>,
        scope: &str,
    ) -> Result<(), KvError> {
        if minimum.is_some_and(|minimum| observed < minimum) {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "authoritative mounted SIFS read for {scope:?} returned revision {}, below committed revision {}",
                    observed.0,
                    minimum.expect("checked minimum").0,
                ),
            });
        }
        Ok(())
    }

    fn merge_authoritative_entry(
        &self,
        key: &str,
        read: &sinomo_core::kv::KvAuthoritativeRead<Option<KvEntry>>,
        generation: u64,
    ) -> Result<bool, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation)
            || cache.snapshot.revision > read.authority.revision
        {
            return Ok(false);
        }
        let mut reset_projection = !cache.can_admit_authoritative_scopes(1, key.len());
        let mut snapshot = if reset_projection {
            empty_mounted_sifs_kv_snapshot(read.authority.revision)
        } else {
            cache.snapshot.as_ref().clone()
        };
        snapshot.entries.retain(|entry| entry.key != key);
        if let Some(entry) = &read.value {
            snapshot.entries.push(entry.clone());
        }
        snapshot.revision = read.authority.revision;
        snapshot.canonicalize();
        if !mounted_sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            reset_projection = true;
            snapshot = empty_mounted_sifs_kv_snapshot(read.authority.revision);
            if let Some(entry) = &read.value {
                snapshot.entries.push(entry.clone());
            }
            snapshot.canonicalize();
        }
        snapshot.validate_resource_bounds()?;
        if reset_projection {
            cache.clear_authoritative_scopes();
        }
        if !mounted_sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            cache.snapshot = Arc::new(empty_mounted_sifs_kv_snapshot(read.authority.revision));
            return Ok(true);
        }
        // A point read does not revalidate any prefix which used to cover it.
        cache.invalidate_entry_scope(key);
        cache.mark_entry_validated(key);
        snapshot
            .entries
            .retain(|entry| cache.entry_is_validated(&entry.key));
        cache.snapshot = Arc::new(snapshot);
        Ok(true)
    }

    fn merge_authoritative_entries_at_one_revision(
        &self,
        keys: &[String],
        read: &sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>,
        generation: u64,
    ) -> Result<bool, KvError> {
        let revision = read.authority.revision;
        if keys.len() != read.value.len() {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative mounted SIFS exact-key set returned {} values for {} keys",
                    read.value.len(),
                    keys.len(),
                ),
            });
        }
        for (key, value) in keys.iter().zip(&read.value) {
            if value.as_ref().is_some_and(|entry| entry.key != *key) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "authoritative mounted SIFS read for {key:?} returned entry for {:?}",
                        value.as_ref().expect("checked entry").key,
                    ),
                });
            }
        }

        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation) || cache.snapshot.revision > revision
        {
            return Ok(false);
        }
        let requested_keys = keys.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let requested_key_bytes = requested_keys.iter().map(|key| key.len()).sum::<usize>();
        let mut reset_projection =
            !cache.can_admit_authoritative_scopes(requested_keys.len(), requested_key_bytes);
        let mut snapshot = if reset_projection {
            empty_mounted_sifs_kv_snapshot(revision)
        } else {
            cache.snapshot.as_ref().clone()
        };
        snapshot
            .entries
            .retain(|entry| !requested_keys.contains(entry.key.as_str()));
        snapshot
            .entries
            .extend(read.value.iter().filter_map(Clone::clone));
        snapshot.revision = revision;
        snapshot.canonicalize();
        if !mounted_sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            reset_projection = true;
            snapshot = empty_mounted_sifs_kv_snapshot(revision);
            snapshot
                .entries
                .extend(read.value.iter().filter_map(Clone::clone));
            snapshot.canonicalize();
        }
        snapshot.validate_resource_bounds()?;
        if reset_projection {
            cache.clear_authoritative_scopes();
        }
        if !mounted_sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            cache.snapshot = Arc::new(empty_mounted_sifs_kv_snapshot(revision));
            return Ok(true);
        }
        for key in keys {
            // A point read does not revalidate any prefix which used to cover
            // it, even when several point reads are committed atomically.
            cache.invalidate_entry_scope(key);
            cache.mark_entry_validated(key);
        }
        snapshot
            .entries
            .retain(|entry| cache.entry_is_validated(&entry.key));
        cache.snapshot = Arc::new(snapshot);
        Ok(true)
    }

    fn read_authoritative_entries_consistently(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.read_authoritative_entries_consistently_with_deadline(keys, None)
    }

    fn read_authoritative_entries_consistently_before(
        &self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.read_authoritative_entries_consistently_with_deadline(keys, Some(deadline))
    }

    fn read_authoritative_entries_consistently_with_deadline(
        &self,
        keys: &[String],
        deadline: Option<Instant>,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(mounted_sifs_authority_deadline_error());
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > MOUNTED_SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES {
            return Err(KvError::ResourceExhausted {
                operation: "mounted SIFS consistent exact-key read".to_string(),
                requested_bytes: keys.len().saturating_mul(std::mem::size_of::<String>()),
            });
        }
        let requested_key_bytes = keys
            .iter()
            .fold(0_usize, |bytes, key| bytes.saturating_add(key.len()));
        if requested_key_bytes > MOUNTED_SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES {
            return Err(KvError::ResourceExhausted {
                operation: "mounted SIFS consistent exact-key request".to_string(),
                requested_bytes: requested_key_bytes,
            });
        }

        for _ in 0..MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(mounted_sifs_authority_deadline_error());
            }
            let (generation, minimum_revision) = {
                let cache = self
                    .snapshot
                    .lock()
                    .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
                let Some(generation) = cache.authoritative_generation else {
                    return Ok(keys
                        .iter()
                        .map(|key| cache.snapshot.get(key).cloned())
                        .collect());
                };
                (generation, cache.snapshot.revision)
            };

            let read = match deadline {
                Some(deadline) => {
                    self.fetch_authoritative_entries_before(keys, minimum_revision, deadline)?
                }
                None => self.fetch_authoritative_entries(keys, minimum_revision)?,
            };
            if !self.merge_authoritative_entries_at_one_revision(keys, &read, generation)? {
                continue;
            }
            return Ok(read.value);
        }

        Err(self.superseded_authority_error("consistent exact-key set"))
    }

    fn merge_authoritative_prefix(
        &self,
        prefix: &str,
        read: &sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
        generation: u64,
    ) -> Result<bool, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("mounted SIFS KV projection lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation)
            || cache.snapshot.revision > read.authority.revision
        {
            return Ok(false);
        }
        let mut reset_projection = !cache.can_admit_authoritative_scopes(1, prefix.len());
        let mut snapshot = if reset_projection {
            empty_mounted_sifs_kv_snapshot(read.authority.revision)
        } else {
            cache.snapshot.as_ref().clone()
        };
        snapshot
            .entries
            .retain(|entry| !entry.key.starts_with(prefix));
        snapshot.entries.extend(read.value.iter().cloned());
        snapshot.revision = read.authority.revision;
        snapshot.canonicalize();
        if !mounted_sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            reset_projection = true;
            snapshot = empty_mounted_sifs_kv_snapshot(read.authority.revision);
            snapshot.entries.extend(read.value.iter().cloned());
            snapshot.canonicalize();
        }
        snapshot.validate_resource_bounds()?;
        if reset_projection {
            cache.clear_authoritative_scopes();
        }
        if !mounted_sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            cache.snapshot = Arc::new(empty_mounted_sifs_kv_snapshot(read.authority.revision));
            return Ok(true);
        }
        cache.invalidate_prefix_scope(prefix);
        cache.mark_prefix_validated(prefix);
        snapshot
            .entries
            .retain(|entry| cache.entry_is_validated(&entry.key));
        cache.snapshot = Arc::new(snapshot);
        Ok(true)
    }

    fn superseded_authority_error(&self, scope: &str) -> KvError {
        KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative mounted SIFS read for {scope:?} was superseded by {} consecutive projection changes",
                MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS,
            ),
        }
    }

    fn revalidate_mutated_entry(
        &self,
        key: &str,
        minimum_revision: KvRevision,
    ) -> Result<(), KvError> {
        for _ in 0..MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let generation = self.ensure_authoritative_projection()?;
            self.invalidate_authoritative_entry_scope(generation, key)?;
            if self
                .read_authoritative_entry(key, Some(minimum_revision), generation)?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(self.superseded_authority_error(key))
    }

    fn revalidate_mutated_prefix(
        &self,
        prefix: &str,
        minimum_revision: KvRevision,
    ) -> Result<(), KvError> {
        for _ in 0..MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let generation = self.ensure_authoritative_projection()?;
            self.invalidate_authoritative_prefix_scope(generation, prefix)?;
            if self
                .read_authoritative_prefix(prefix, Some(minimum_revision), generation)?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(self.superseded_authority_error(prefix))
    }
}

impl DfsControlPlaneKvClient for MountedSifsControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        // Exact subsequent keys and prefixes establish their own bounded
        // ReadIndex/barrier. Do not import a full snapshot or scan `/dfs/`.
        self.begin_authoritative_refresh_generation().map(|_| ())
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        for _ in 0..MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            match self.authoritative_entry_read_plan(key)? {
                MountedSifsKvReadPlan::Cached(value) => return Ok(value),
                MountedSifsKvReadPlan::Authoritative {
                    generation,
                    minimum_revision,
                } => {
                    if let Some(read) =
                        self.read_authoritative_entry(key, Some(minimum_revision), generation)?
                    {
                        return Ok(read.value);
                    }
                }
            }
        }
        Err(self.superseded_authority_error(key))
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        // One exact-set ReadIndex pins every key to the same committed view;
        // publish the complete result into the generation cache atomically.
        self.read_authoritative_entries_consistently(keys)
    }

    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        // Operation-bound authority validation always starts a fresh bounded
        // generation, then crosses exactly one exact-key-set ReadIndex. It
        // must not import the complete cluster KV snapshot first.
        self.begin_authoritative_refresh_generation()?;
        self.read_authoritative_entries_consistently(keys)
    }

    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if Instant::now() >= deadline {
            return Err(mounted_sifs_authority_deadline_error());
        }
        // The exact deadline covers generation reset, every replan, the
        // primary exact-set route, and its mixed-version snapshot fallback.
        self.begin_authoritative_refresh_generation()?;
        self.read_authoritative_entries_consistently_before(keys, deadline)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        for _ in 0..MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            match self.authoritative_prefix_read_plan(prefix)? {
                MountedSifsKvReadPlan::Cached(value) => return Ok(value),
                MountedSifsKvReadPlan::Authoritative {
                    generation,
                    minimum_revision,
                } => {
                    if let Some(read) =
                        self.read_authoritative_prefix(prefix, Some(minimum_revision), generation)?
                    {
                        return Ok(read.value);
                    }
                }
            }
        }
        Err(self.superseded_authority_error(prefix))
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<crate::DfsAuthoritativePrefixProjection, KvError> {
        for _ in 0..MOUNTED_SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let generation = self.begin_authoritative_refresh_generation()?;
            if let Some(read) = self.read_authoritative_prefix(prefix, None, generation)? {
                return crate::DfsAuthoritativePrefixProjection::complete(
                    prefix,
                    read.value,
                    read.authority.revision,
                );
            }
        }
        Err(self.superseded_authority_error("operation-bound prefix"))
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
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        let submitted_digest = sinomo_core::kv::kv_command_digest(
            &sinomo_core::kv::KvCommand::Txn(request.txn.clone()),
        )?;
        let call = ObjectRpcCall::encode_borrowed_value(
            sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &request,
        )
        .map_err(|source| self.kv_error(source))?;
        let response =
            self.call_local_kv_rpc(call, sinomo_core::kv::CLUSTER_KV_TXN_ROUTE, 1024 * 1024)?;
        let result = sinomo_core::kv::decode_cluster_kv_txn_rpc_body(&response.body)?;
        sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(&result.authority)?;
        if result.request_id != request.request_id {
            return Err(KvError::Serialization {
                detail: format!(
                    "mounted SIFS cluster KV transaction response request id {} does not match {}",
                    result.request_id, request.request_id,
                ),
            });
        }
        if result
            .submitted_command_digest
            .is_some_and(|digest| digest != submitted_digest)
        {
            return Err(KvError::Serialization {
                detail:
                    "mounted SIFS cluster KV transaction response does not match the submitted command"
                        .to_string(),
            });
        }
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "cluster KV returned a barrier result for a mounted SIFS transaction"
                .to_string(),
        })?;
        if result.authority.revision < response.revision {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "mounted SIFS cluster KV transaction proof revision {} predates result revision {}",
                    result.authority.revision.0, response.revision.0,
                ),
            });
        }

        let generation = self.ensure_authoritative_projection()?;
        let executed = if response.succeeded {
            &request.txn.success
        } else {
            &request.txn.failure
        };
        let mut refreshed_keys = BTreeSet::new();
        let mut refreshed_prefixes = BTreeSet::new();
        let mut invalidate_all = false;
        for operation in executed {
            match operation {
                KvOp::Put { key, .. } | KvOp::Delete { key } => {
                    refreshed_keys.insert(key.as_str());
                }
                KvOp::DeletePrefix { prefix } => {
                    refreshed_prefixes.insert(prefix.as_str());
                }
                KvOp::LeaseRevoke { .. } => invalidate_all = true,
                KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } => {}
            }
        }
        self.invalidate_authoritative_mutation_scopes(
            generation,
            &refreshed_keys,
            &refreshed_prefixes,
            invalidate_all,
        )?;
        for key in refreshed_keys {
            self.revalidate_mutated_entry(key, response.revision)?;
        }
        for prefix in refreshed_prefixes {
            self.revalidate_mutated_prefix(prefix, response.revision)?;
        }
        Ok(response)
    }
}

fn empty_mounted_sifs_kv_snapshot(revision: KvRevision) -> KvSnapshot {
    KvSnapshot {
        revision,
        compacted_revision: KvRevision::ZERO,
        entries: Vec::new(),
        leases: Vec::new(),
        events: Vec::new(),
        committed_requests: Vec::new(),
        expired_requests: Vec::new(),
    }
}

fn mounted_sifs_value_response_from_rpc_result(
    result: ObjectRpcResult,
    max_bytes: usize,
) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
    let streamed_value = result.has_streamed_value();
    let (mut response, body) = result.into_parts();
    if streamed_value {
        response
            .error_detail()
            .map_err(DfsPeerTransportError::from_network)?;
        response.body = collect_dfs_incoming_body_sync(body, max_bytes)?;
        return Ok(response);
    }
    if !body.is_empty() {
        return Err(DfsPeerTransportError::new(format!(
            "mounted SIFS KV route {} returned an unexpected attachment",
            response.route
        )));
    }
    if response.body.len() > max_bytes {
        return Err(DfsPeerTransportError::new(format!(
            "mounted SIFS KV route {} returned {} bytes, exceeding the {max_bytes} byte limit",
            response.route,
            response.body.len(),
        )));
    }
    Ok(response)
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
    cluster_paths: ClusterPaths,
}

impl MountedSifsPeerTransport {
    async fn call_object_rpc_async_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
        _runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)?;
        let mut host = MountedSifsObjectRpcHost;
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
                result.map_err(DfsPeerTransportError::from_network)
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
        result.map_err(DfsPeerTransportError::from_network)
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
                .map_err(DfsPeerTransportError::from_network)
        })
    }
}

struct MountedSifsObjectRpcHost;

impl sinomo_core::network::ClusterObjectRpcAsyncHost for MountedSifsObjectRpcHost {}
