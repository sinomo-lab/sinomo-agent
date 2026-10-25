use super::*;
use crate::service_write::{
    DfsDistributedWriteOptions, DfsWriteCreation, DfsWriteDispatch, DfsWriteRepair,
};
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::events::EventId;
use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterPaths, NetworkError, ObjectRpcCall, ObjectRpcIncomingBody, ObjectRpcRequest,
    ObjectRpcResponse, ObjectRpcResult,
};
use sinomo_core::object::Value;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};
use time::OffsetDateTime;

use crate::transfer::{PendingNetworkedRead, PendingNetworkedWrite};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

fn test_dfs_lookup_routed_message(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    frame_id: u64,
    path: &str,
) -> DfsRoutedMessage {
    build_dfs_routed_message(
        source_node_id,
        target_node_id,
        frame_id,
        frame_id,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: format!("dfs_test_lookup_{frame_id}"),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: source_node_id.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::LookupPath {
                path: path.to_string(),
                inline_range: None,
            },
        }),
    )
}

fn test_dfs_message_response_from_routed_messages(
    request_id: impl Into<String>,
    routed_messages: Vec<DfsRoutedMessage>,
) -> DfsMessageResponse {
    DfsMessageResponse {
        request_id: request_id.into(),
        outbound_messages: routed_messages,
    }
}

fn test_dfs_message_command(
    request_id: impl Into<String>,
    routed_message: DfsRoutedMessage,
) -> DfsMessageCommand {
    DfsMessageCommand {
        request_id: request_id.into(),
        routed_message,
    }
}

fn test_dfs_message_command_from_message(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    frame_id: u64,
    message: DfsNetworkMessage,
) -> DfsMessageCommand {
    DfsMessageCommand {
        request_id: format!("dfs_message_test_{frame_id}"),
        routed_message: build_dfs_routed_message(
            source_node_id,
            target_node_id,
            frame_id,
            frame_id,
            message,
        ),
    }
}

struct CanonicalDfsMessagePeerTransport {
    sender: DfsMessageSender,
}

impl CanonicalDfsMessagePeerTransport {
    async fn call_object_rpc_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let streamed_value = call.has_streamed_value_body();
        let ObjectRpcCall { mut request, body } = call;
        if !dfs_route_is_message_data_plane(&request.route) {
            return Err(DfsPeerTransportError::new(format!(
                "unexpected DFS message peer route {}",
                request.route
            )));
        }
        let command = if streamed_value {
            request.body = body
                .collect_to_vec(usize::MAX)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            decode_dfs_message_rpc_request(&request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        } else if body.is_empty() {
            decode_dfs_message_rpc_request(&request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        } else {
            let head = crate::body_rpc::decode_dfs_attachment_message_rpc_request(&request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let max_bytes = usize::try_from(head.attachment.total_size_bytes).map_err(|_| {
                DfsPeerTransportError::new(
                    "DFS request attachment does not fit this platform address space",
                )
            })?;
            let bytes = body
                .collect_to_vec(max_bytes)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            crate::body_rpc::attach_dfs_message_request_attachment(head, bytes)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        };
        let response = (self.sender)(target_node_id, &command, options)?;
        ObjectRpcResponse::encode_body(&request.route, &response)
            .map(ObjectRpcResult::value)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

impl DfsPeerTransport for CanonicalDfsMessagePeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = runtime.block_on(self.call_object_rpc_inner(target_node_id, call, options));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }
}

impl AsyncDfsPeerTransport for CanonicalDfsMessagePeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(self.call_object_rpc_inner(target_node_id, call, options))
    }
}

fn served_transport_with_canonical_message_peer(
    local_node_id: NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
    sender: DfsMessageSender,
    connect_timeout: Duration,
) -> DfsNodeTransport {
    let peer_transport: Arc<dyn DfsPeerTransport> =
        Arc::new(CanonicalDfsMessagePeerTransport { sender });
    DfsNodeTransport::new_with_peer_transport_for_nodes(
        local_node_id,
        peer_node_ids,
        peer_transport,
        connect_timeout,
    )
}

fn process_dfs_message_command_for_test(
    local_node_id: NodeId,
    command: DfsMessageCommand,
    content: LocalContentStore,
    metadata_backend: Box<dyn DfsMetadataBackend>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<DfsMessageProcessResult, DfsError> {
    let message_sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected DFS message hop to {} while processing test command {}",
            target_node_id, command.request_id
        )))
    });
    let transport = DfsNodeTransport::new_with_message_sender(
        local_node_id,
        Vec::new(),
        message_sender,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let mut service = NetworkedDfsService::new(transport, content);
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    let mut service = service.with_boxed_metadata_backend(metadata_backend)?;
    service.process_message_command(command)
}

fn object_field<'a>(value: &'a Value, key: &str) -> &'a Value {
    let Value::Object(object) = value else {
        panic!("expected object value for field {key}, got {value:?}");
    };
    object
        .fields
        .iter()
        .find(|field| field.key == key)
        .map(|field| &field.value)
        .unwrap_or_else(|| panic!("missing object field {key} in {object:?}"))
}

fn assert_native_bytes_field(value: &impl serde::Serialize, key: &str) {
    let value = sinomo_core::object::serde_ext::to_value(value).expect("serialize value");
    let field = object_field(&value, key);
    assert!(
        matches!(field, Value::Bytes(_)),
        "expected {key} to serialize as native bytes, got {field:?}"
    );
}

fn dfs_message_trace_label(message: &DfsNetworkMessage) -> String {
    match message {
        DfsNetworkMessage::ReadRequest(_) => "read-request".to_string(),
        DfsNetworkMessage::ReadResponse(_) => "read-response".to_string(),
        DfsNetworkMessage::BulkReadRequest(_) => "bulk-read-request".to_string(),
        DfsNetworkMessage::BulkReadResponse(_) => "bulk-read-response".to_string(),
        DfsNetworkMessage::WriteRequest(_) => "write-request".to_string(),
        DfsNetworkMessage::WriteResponse(_) => "write-response".to_string(),
        DfsNetworkMessage::DistributedWriteRequest(command) => {
            format!("distributed-write-request:{}", command.request_id)
        }
        DfsNetworkMessage::ExclusiveCreateRequest(command) => {
            format!("exclusive-create-request:{}", command.request_id)
        }
        DfsNetworkMessage::RetainedWriteRequest(command) => {
            format!("retained-write-request:{}", command.request_id)
        }
        DfsNetworkMessage::DistributedWriteResponse(response) => {
            format!("distributed-write-response:{}", response.request_id)
        }
        DfsNetworkMessage::StableWriteReconciliationRequest(request) => {
            format!("stable-write-reconciliation-request:{}", request.request_id)
        }
        DfsNetworkMessage::StableWriteReconciliationResponse(response) => {
            format!(
                "stable-write-reconciliation-response:{}",
                response.request_id
            )
        }
        DfsNetworkMessage::StableNamespaceReconciliationRequest(request) => {
            format!(
                "stable-namespace-reconciliation-request:{}",
                request.request_id
            )
        }
        DfsNetworkMessage::StableNamespaceReconciliationResponse(response) => {
            format!(
                "stable-namespace-reconciliation-response:{}",
                response.request_id
            )
        }
        DfsNetworkMessage::NamespaceRequest(_) => "namespace-request".to_string(),
        DfsNetworkMessage::NamespaceResponse(_) => "namespace-response".to_string(),
        DfsNetworkMessage::FetchBlob(command) => {
            format!("fetch-blob:{}", command.intent_id)
        }
        DfsNetworkMessage::FetchBlobResponse(response) => {
            format!("fetch-blob-response:{}", response.intent_id)
        }
        DfsNetworkMessage::StageBlob(stage) | DfsNetworkMessage::StageRetainedBlob(stage) => {
            format!("stage-blob:{}", stage.intent_id)
        }
        DfsNetworkMessage::RepairRetainedBlob(stage) => {
            format!("repair-retained-blob:{}", stage.intent_id)
        }
        DfsNetworkMessage::StageBlobResponse(response) => {
            format!("stage-blob-response:{}", response.intent_id)
        }
        DfsNetworkMessage::StagePatch(_) => "stage-patch".to_string(),
        DfsNetworkMessage::CommitVersion(_) => "commit-version".to_string(),
        DfsNetworkMessage::CommitVersionResponse(_) => "commit-version-response".to_string(),
        DfsNetworkMessage::RepairReplica(_) => "repair-replica".to_string(),
        DfsNetworkMessage::SelectedOriginalRepair(_) => "selected-original-repair".to_string(),
        DfsNetworkMessage::OriginalRepairHistoryRequest(_) => "original-repair-history-request".to_string(),
        DfsNetworkMessage::OriginalRepairHistoryResponse(_) => "original-repair-history-response".to_string(),
        #[cfg(test)]
        DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(_) => "selected-local-recovery-fence-request".to_string(),
        #[cfg(test)]
        DfsNetworkMessage::SelectedLocalRecoveryFenceResponse(_) => "selected-local-recovery-fence-response".to_string(),
        DfsNetworkMessage::RepairReplicaResponse(_) => "repair-replica-response".to_string(),
        DfsNetworkMessage::MetadataJournalBatch(_) => "metadata-journal-batch".to_string(),
        DfsNetworkMessage::MetadataJournalBatchResponse(_) => {
            "metadata-journal-batch-response".to_string()
        }
        DfsNetworkMessage::MetadataSnapshotInstall(_) => "metadata-snapshot-install".to_string(),
        DfsNetworkMessage::MetadataSnapshotInstallResponse(_) => {
            "metadata-snapshot-install-response".to_string()
        }
        DfsNetworkMessage::ContentRootCatalogPageRequest(_) => {
            "content-root-catalog-page-request".to_string()
        }
        DfsNetworkMessage::ContentRootCatalogPageResponse(_) => {
            "content-root-catalog-page-response".to_string()
        }
    }
}

fn protocol_test_bytes() -> Vec<u8> {
    vec![0, 1, 2, 3, 253, 254, 255]
}

fn protocol_test_write_request() -> DfsWriteRequest {
    DfsWriteRequest {
        file_system_id: sinomo_core::Id::from_legacy_named("fs").expect("fs id"),
        object_id: sinomo_core::Id::from_legacy_named("object").expect("object id"),
        target_version: sinomo_core::Id::from_legacy_named("version").expect("version id"),
        requesting_node_id: sinomo_core::Id::from_legacy_named("node").expect("node id"),
    }
}

fn test_routed_write_command(
    request_id: &str,
    requester: &NodeId,
    object_id: &DfsObjectId,
) -> DfsDistributedWriteCommand {
    DfsDistributedWriteCommand {
        publication: Default::default(),
        patch: None,
        request_id: request_id.to_string(),
        request: DfsWriteRequest {
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            target_version: DfsVersion::from_legacy_named(format!("version_{request_id}"))
                .expect("version"),
            requesting_node_id: requester.clone(),
        },
        expected_base_version: None,
        metadata_journal_after_sequence: None,
        bytes: Vec::new(),
        content_hash: Some(format!("blake3:{request_id}")),
        content_size_bytes: Some(0),
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![requester.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        object_metadata: None,
        authority_shard: None,
        touch_parent_on_create: true,
        foreground_replica_repair: true,
        exclusive_create: false,
    }
}

fn test_routed_authority_shard(primary_node_id: &NodeId, epoch: u64) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary_node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    }
}

fn test_routed_authority_control_plane(
    authority_shard: &DfsMetadataShardConfig,
) -> SharedDfsControlPlaneKvClient<InProcessKvStore> {
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(authority_shard)
            .expect("routed response authority shard")
    });
    control_plane
}

// Observation-only wrapper over the SAME native configuration client.
// A completed cached getter or its ordinary prefix fallback counts one actual
// shard snapshot; no replacement member rows, floors or permissions are supplied.
#[derive(Debug)]
struct CountingDfsControlPlaneKvClient {
    inner: Box<dyn DfsControlPlaneKvClient>,
    range_prefix_calls: Arc<AtomicUsize>,
}
impl CountingDfsControlPlaneKvClient {
    fn new(inner: Box<dyn DfsControlPlaneKvClient>, range_prefix_calls: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            range_prefix_calls,
        }
    }
}
impl DfsControlPlaneKvClient for CountingDfsControlPlaneKvClient {
    fn async_client(&mut self) -> Option<&mut dyn AsyncDfsControlPlaneKvClient> {
        self.inner.async_client()
    }
    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        self.inner.placement_authority_process_identity()
    }
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.inner.refresh_latest()
    }
    fn write_configuration_projection(
        &self,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, KvError> {
        self.inner.write_configuration_projection()
    }
    fn write_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, KvError> {
        self.inner.write_configuration_readiness()
    }
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.inner.get(key)
    }
    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner.get_consistent(keys)
    }
    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner.get_authoritative_consistent(keys)
    }
    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: std::time::Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner
            .get_authoritative_consistent_before(keys, deadline)
    }
    fn cached_metadata_shards(
        &self,
        fs: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, KvError> {
        let observed = self.inner.cached_metadata_shards(fs)?;
        if observed.is_some() {
            self.range_prefix_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(observed)
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.range_prefix_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.range_prefix(prefix)
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner.put(key, value)
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.inner.txn(txn)
    }
    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<crate::DfsNativeTxnCommit>), KvError> {
        self.inner.txn_with_native_authority(txn)
    }
}

fn native_read_fixture(
    name: &str,
    copies: usize,
    authoritative_reader: bool,
    bytes: Vec<u8>,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NodeId,
    DfsStat,
    Vec<u8>,
) {
    native_read_fixture_observing_materialization(
        name,
        copies,
        authoritative_reader,
        bytes,
        |_, _, _| {},
        |_, _, _, _| {},
    )
}

fn native_read_fixture_observing_materialization(
    name: &str,
    copies: usize,
    authoritative_reader: bool,
    bytes: Vec<u8>,
    before_write: impl FnOnce(&crate::test_runtime::DfsTestRuntime, &NodeId, &DfsMetadataShardId),
    mut on_lag: impl FnMut(
        &crate::test_runtime::DfsTestRuntime,
        &NodeId,
        &DfsStat,
        &crate::runtime_host::DfsTestMaterializationCapture,
    ),
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NodeId,
    DfsStat,
    Vec<u8>,
) {
    assert!((1..=2).contains(&copies));
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id;
    let shard = DfsMetadataShardId::new(config.shard_id);
    drop(service);
    let peers = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let (owner, replica, reader) = if authoritative_reader {
        (peers[0].clone(), peers[1].clone(), primary.clone())
    } else {
        (primary.clone(), peers[0].clone(), peers[1].clone())
    };
    let holders = if copies == 1 {
        vec![owner.clone()]
    } else {
        vec![owner.clone(), replica.clone()]
    };
    let path = format!("/services/{name}.bin");
    let mut policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, copies, copies).unwrap();
    policy.required_node_ids = holders.clone();
    policy.forbidden_node_ids = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| !holders.contains(node))
        .collect();
    let expected_holders = holders.iter().cloned().collect::<BTreeSet<_>>();
    let expected_forbidden = policy
        .forbidden_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut writer = runtime.client(&owner).unwrap();
    let (resolved, _, replication) = writer
        .managed_replication_policy_for_path(&path, DfsObjectPolicy::ReplicatedConfirmed, copies)
        .unwrap();
    assert_eq!(
        resolved.policy.consistency,
        DfsObjectPolicy::ReplicatedConfirmed
    );
    assert_eq!(replication.min_commit_replicas, copies);
    assert_eq!(replication.desired_replicas, copies);
    assert_eq!(
        replication
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_holders
    );
    assert_eq!(
        replication
            .forbidden_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_forbidden
    );
    before_write(&runtime, &reader, &shard);
    // Publication and reader readiness share the existing 45-second write
    // window, captured before the original write; no retry gets a new budget.
    let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    let previous_deadline = writer.transport.set_operation_deadline(Some(deadline));
    let written = writer.posix_write_file(
        &path,
        &bytes,
        DfsWriteOptions {
            replica_candidates: holders.clone(),
            policy: replication,
            wait_for_commit: true,
            foreground_replica_repair: true,
        },
    );
    writer.transport.set_operation_deadline(previous_deadline);
    let stat = written.unwrap();
    assert_eq!(
        stat.current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_holders
    );
    for holder in &holders {
        let client = runtime.client(holder).unwrap();
        let actual = client
            .content()
            .read_blob(stat.content_hash.as_deref().expect("actual published hash"))
            .expect("actual recorded holder bytes, inline or loose");
        assert_eq!(actual.len(), bytes.len());
        assert_eq!(Some(content_hash(&actual)), stat.content_hash);
        assert_eq!(actual, bytes);
    }
    // A successful primary write and content ACK do not prove that the read
    // fixture's local metadata has received/materialized the terminal commit.
    // Capture the genuine served primary's exact published cut, then drive one
    // normal publication turn only if needed and observe the existing owner.
    let source = LocalSobjMetadataBackend::open_for_inspection(
        shard.clone(),
        default_dfs_metadata_shards_root(runtime.data_dir(&primary)).join(&shard.0),
    )
    .unwrap();
    let published = source.get_object(&stat.object_id).unwrap().unwrap();
    assert_eq!(published.version, stat.version);
    assert_eq!(published.content_hash, stat.content_hash);
    let snapshot = source.export_table_snapshot();
    assert!(snapshot.prefix_proof.is_well_formed());
    assert_eq!(
        snapshot.prefix_proof.journal_sequence,
        snapshot.materialized_through_sequence
    );
    drop(source);
    let mut requested_publication = false;
    crate::runtime_host::materialization_test::wait_for_existing_materialization(
        runtime.data_dir(&reader),
        &stat.file_system_id,
        &shard,
        &runtime.runtime_handle(),
        snapshot.materialized_through_sequence,
        deadline,
        |observed| {
            on_lag(&runtime, &reader, &stat, observed);
            if !requested_publication {
                requested_publication = true;
                let publication = runtime
                    .maintenance_tick(
                        &primary,
                        deadline.saturating_duration_since(Instant::now())
                            .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                    )
                    .unwrap();
                assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
                assert_eq!(publication.omitted_kv_event_count, 0);
            }
        },
    )
    .expect("reader must observe the original publication inside the unchanged write window");
    let ready_reader = runtime.client(&reader).unwrap();
    let visible = ready_reader
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(visible.object_id, stat.object_id);
    assert_eq!(visible.version, stat.version);
    assert_eq!(visible.content_hash, stat.content_hash);
    assert_eq!(visible.content_size_bytes, stat.size_bytes);
    assert_eq!(
        visible
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_holders
    );
    drop(ready_reader);
    drop(writer);
    (runtime, owner, replica, reader, stat, bytes)
}

#[derive(Debug)]
struct RecordingMetadataJournalPublisherHost {
    root: PathBuf,
    opened_shards: Vec<DfsMetadataShardId>,
    cursor_requests: Vec<(NodeId, DfsMetadataShardId)>,
}

impl RecordingMetadataJournalPublisherHost {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            opened_shards: Vec::new(),
            cursor_requests: Vec::new(),
        }
    }

    fn shard_path(&self, shard_id: &DfsMetadataShardId) -> PathBuf {
        self.root.join(&shard_id.0)
    }
}

impl DfsMetadataJournalPublisherHost for RecordingMetadataJournalPublisherHost {
    fn open_primary_shard_backend(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
        self.opened_shards.push(shard_id.clone());
        Ok(Box::new(LocalSobjMetadataBackend::new(
            shard_id.clone(),
            self.shard_path(shard_id),
        )?))
    }

    fn metadata_replication_cursor(
        &mut self,
        backend: &mut dyn DfsMetadataBackend,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64, DfsError> {
        self.cursor_requests
            .push((target_node_id.clone(), shard_id.clone()));
        Ok(backend.metadata_replication_cursor(target_node_id, shard_id)?)
    }
}

#[derive(Default)]
struct RecordingMaintenanceHost {
    cursors: Vec<(DfsFileSystemId, DfsAntiEntropyCursorRecord)>,
    stored: Vec<DfsAntiEntropyCursorRecord>,
    cleared: Vec<DfsFileSystemId>,
    repair_attempt_records: Vec<DfsRepairAttemptRecord>,
    stored_repair_attempt_records: Vec<Vec<DfsRepairAttemptRecord>>,
    load_repair_attempt_records_count: Arc<AtomicUsize>,
    repair_page_owner: Option<(sinomo_testdir::TestDir, LocalDfsMaintenanceHost)>,
}

impl RecordingMaintenanceHost {
    fn repair_page_owner(&mut self) -> &mut LocalDfsMaintenanceHost {
        if self.repair_page_owner.is_none() {
            let root = sinomo_testdir::TestDir::new("dfs-shared-maintenance-pages");
            let data_dir = SinomoDataDir::new(root.path());
            let store =
                LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(&data_dir));
            store
                .append_enqueues(self.repair_attempt_records.clone())
                .unwrap();
            self.repair_page_owner = Some((root, LocalDfsMaintenanceHost::new(data_dir)));
        }
        &mut self.repair_page_owner.as_mut().unwrap().1
    }
}

impl DfsMaintenanceHost for RecordingMaintenanceHost {
    fn load_repair_maintenance_page(
        &mut self,
        max_records: usize,
    ) -> Result<DfsRepairQueueMaintenancePage, DfsError> {
        self.load_repair_attempt_records_count
            .fetch_add(1, Ordering::Relaxed);
        self.repair_page_owner()
            .load_repair_maintenance_page(max_records)
    }

    fn enroll_repair_maintenance_page(
        &mut self,
        page: &DfsRepairQueueMaintenancePage,
        max_records: usize,
        captured: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueueMaintenancePage, DfsError> {
        self.repair_page_owner()
            .enroll_repair_maintenance_page(page, max_records, captured)
    }

    fn checkpoint_repair_maintenance_page(
        &mut self,
        page: &DfsRepairQueueMaintenancePage,
        examined: &BTreeSet<(String, NodeId)>,
        desired: &[DfsRepairAttemptRecord],
        retired: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairMaintenancePageCheckpoint, DfsError> {
        let checkpoint = self
            .repair_page_owner()
            .checkpoint_repair_maintenance_page(page, examined, desired, retired)?;
        let records = self.repair_page_owner().load_repair_attempt_records()?;
        self.repair_attempt_records = records.clone();
        self.stored_repair_attempt_records.push(records);
        Ok(checkpoint)
    }

    fn load_anti_entropy_cursor(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsAntiEntropyCursorRecord>, DfsError> {
        Ok(self
            .cursors
            .iter()
            .find(|(saved_file_system_id, _)| saved_file_system_id == file_system_id)
            .map(|(_, cursor)| cursor.clone()))
    }

    fn store_anti_entropy_cursor(
        &mut self,
        cursor: &DfsAntiEntropyCursorRecord,
    ) -> Result<(), DfsError> {
        self.cursors
            .retain(|(saved_file_system_id, _)| saved_file_system_id != &cursor.file_system_id);
        self.cursors
            .push((cursor.file_system_id.clone(), cursor.clone()));
        self.stored.push(cursor.clone());
        Ok(())
    }

    fn clear_anti_entropy_cursor(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<(), DfsError> {
        self.cursors
            .retain(|(saved_file_system_id, _)| saved_file_system_id != file_system_id);
        self.cleared.push(file_system_id.clone());
        Ok(())
    }

    fn load_repair_attempt_records(&mut self) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        self.load_repair_attempt_records_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(self.repair_attempt_records.clone())
    }

    fn store_repair_attempt_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        self.repair_attempt_records = records.to_vec();
        self.stored_repair_attempt_records.push(records.to_vec());
        Ok(())
    }
}

fn file_system(owner: NodeId) -> DfsFileSystem {
    DfsFileSystem {
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
        display_name: "repo".to_string(),
        owner_node_id: owner,
        subtree_policies: Vec::new(),
    }
}

fn file_metadata(owner: NodeId) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id: DfsObjectId::from_legacy_named("obj_file").expect("object"),
        file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
        path: "src/lib.rs".to_string(),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: owner.clone(),
        current_locations: vec![owner],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named("version_1").expect("version"),
        policy: DfsObjectPolicy::LocalOwned,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
}

fn native_sifs_service(
    name: &str,
    node_id: NodeId,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
) {
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, node_id.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let service = runtime.client(&node_id).unwrap();
    (runtime, service)
}

fn deny_native_read_repair(
    runtime: &crate::test_runtime::DfsTestRuntime,
    primary: &NodeId,
    missing: &NodeId,
) {
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(primary.clone()),
            target: Some(missing.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
}

fn assert_malformed_remote_range_rejected(
    name: &str,
    mutate: impl FnOnce(&mut DfsReadRangeData),
    expected_reason: &str,
) {
    let (runtime, source, _, reader, stat, bytes) = native_read_fixture(
        name,
        1,
        true,
        b"authoritative ranged response bytes".to_vec(),
    );
    let mut dfs = runtime.client(&reader).unwrap();
    let offset = 3_u64;
    let max_bytes = 7_usize;
    assert_eq!(
        dfs.request_remote_object_range_read(&stat.object_id, offset, max_bytes)
            .unwrap(),
        vec![source.clone()]
    );
    let request_id = dfs.reads.pending_reads.keys().next().unwrap().clone();
    let original = dfs.transport.drain_incoming_messages().unwrap().into_iter().find(|routed| {
        matches!(&routed.message, DfsNetworkMessage::ReadResponse(response) if response.request_id == request_id)
    }).expect("actual native range request generated its own queued response");
    assert_eq!(original.source_node_id, source);
    assert_eq!(original.target_node_id, reader);
    let DfsNetworkMessage::ReadResponse(mut response) = original.message else {
        unreachable!()
    };
    assert_eq!(response.object_id, stat.object_id);
    let DfsReadResult::RangeFound(data) = &mut response.result else {
        panic!("actual native RangeFound required")
    };
    assert_eq!(data.object_id, stat.object_id);
    assert_eq!(data.version, stat.version);
    assert_eq!(data.content_hash, stat.content_hash);
    assert_eq!(data.offset, offset);
    assert_eq!(data.total_size_bytes, bytes.len() as u64);
    assert_eq!(
        data.bytes,
        bytes[offset as usize..offset as usize + max_bytes]
    );
    mutate(data);
    deny_native_read_repair(&runtime, &reader, &source);
    let event = dfs
        .handle_networked_read_response(source.clone(), response)
        .expect("reject negatively changed actual reply");
    assert!(
        matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        request_id: completed_request_id, result: DfsReadResult::Rejected { reason }, ..
    } if completed_request_id == request_id && reason.contains(expected_reason))
    );
    assert!(!dfs.reads.pending_reads.contains_key(&request_id));
    assert!(
        !dfs.current_read_object_metadata(&stat.object_id)
            .unwrap()
            .current_locations
            .contains(&source)
    );
    drop(dfs);
    runtime.clear_faults();
    runtime.close().unwrap();
}

// ---------------------------------------------------------------------------
// SIFS POSIX cluster correctness tests
//
// These tests share the ordinary embedded component runtime and DfsNodeTransport.
// Configuration changes and publication use the same native authority and RPC
// paths as normal operation. The fixture retains component/data ownership until
// all client lanes have dropped.
// ---------------------------------------------------------------------------

const SIFS_CLUSTER_PUMP_DEADLINE: Duration = Duration::from_secs(10);
const SIFS_CLUSTER_PUMP_IDLE_SLEEP: Duration = Duration::from_millis(2);

struct SifsPosixCluster {
    root: PathBuf,
    primary_node: NodeId,
    writer_node: NodeId,
    observer_node: NodeId,
    primary: NetworkedDfsService<DfsNodeTransport>,
    writer: NetworkedDfsService<DfsNodeTransport>,
    observer: NetworkedDfsService<DfsNodeTransport>,
    rejections: Vec<String>,
    _runtime: crate::test_runtime::DfsTestRuntime,
}

fn sifs_posix_cluster(name: &str, authority_path: &str) -> SifsPosixCluster {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new(name, nodes[0].clone()).expect("native runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("native member");
    }
    runtime.start_nodes().expect("native component startup");
    let shards = runtime.metadata_shards().expect("native configuration");
    let selected = crate::select_metadata_shard_config_for_path(
        &runtime.file_system().file_system_id,
        &shards,
        authority_path,
    )
    .expect("actual fixture path assignment")
    .expect("fixture path has a current metadata owner");
    let primary_node = selected.primary_node_id.clone();
    let routed_nodes = nodes
        .iter()
        .filter(|node| **node != primary_node)
        .cloned()
        .collect::<Vec<_>>();
    let [writer_node, observer_node] = routed_nodes.as_slice() else {
        panic!("fixture retains two distinct actual non-primary clients");
    };
    let root = runtime
        .data_dir(&primary_node)
        .root()
        .parent()
        .unwrap()
        .to_owned();
    let primary = runtime
        .client(&primary_node)
        .expect("native primary client");
    let writer = runtime.client(writer_node).expect("native routed client");
    let observer = runtime
        .client(observer_node)
        .expect("native observer client");
    SifsPosixCluster {
        root,
        primary_node,
        writer_node: writer_node.clone(),
        observer_node: observer_node.clone(),
        primary,
        writer,
        observer,
        rejections: Vec::new(),
        _runtime: runtime,
    }
}

fn sifs_posix_cluster_pump_round(cluster: &mut SifsPosixCluster) -> usize {
    let mut activity = 0;
    for (label, service) in [
        ("primary", &mut cluster.primary),
        ("writer", &mut cluster.writer),
        ("observer", &mut cluster.observer),
    ] {
        let events = service
            .process_incoming()
            .expect("cluster process_incoming");
        activity += events.len();
        for event in events {
            if let NetworkedDfsServiceEvent::MetadataJournalBatchRejected { reason, .. } = event {
                cluster.rejections.push(format!("{label}: {reason}"));
            }
        }
    }
    activity
}

fn sifs_posix_cluster_pump_until(
    cluster: &mut SifsPosixCluster,
    what: &str,
    mut done: impl FnMut(&mut SifsPosixCluster) -> bool,
) {
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        if done(cluster) {
            return;
        }
        let activity = sifs_posix_cluster_pump_round(cluster);
        if Instant::now() >= deadline {
            let writer_pending = cluster
                .writer
                .writes
                .pending_routed_writes
                .iter()
                .map(|(request_id, pending)| {
                    format!(
                        "{}=>canonical:{} remote:{:?} receipts:{}/{} command:{} result:{:?}",
                        request_id,
                        pending.canonical_request_id,
                        pending.remote_intent_id,
                        pending.durable_receipts,
                        pending.required_receipts,
                        pending.command.is_some(),
                        pending.result,
                    )
                })
                .collect::<Vec<_>>();
            panic!(
                "SIFS cluster did not converge within deadline: {what}; writer pending: {writer_pending:?}; primary pending responses: {:?}; journal rejections: {:?}",
                cluster.primary.writes.pending_remote_write_responses.keys(),
                cluster.rejections,
            );
        }
        if activity == 0 {
            std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        }
    }
}

fn sifs_cluster_blocking_distributed_read_with_pump(
    reader: &mut NetworkedDfsService<DfsNodeTransport>,
    path: &str,
    what: &str,
    _pump: impl FnMut(),
) -> Vec<u8> {
    reader
        .posix_read_file_distributed(path)
        .unwrap_or_else(|error| panic!("{what}: {error}"))
}

fn sifs_cluster_writer_call_with_pump<R: Send>(
    cluster: &mut SifsPosixCluster,
    _what: &str,
    call: impl FnOnce(&mut NetworkedDfsService<DfsNodeTransport>) -> R + Send,
) -> R {
    call(&mut cluster.writer)
}

fn sifs_cluster_write_options(node_id: &NodeId) -> DfsWriteOptions {
    DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("cluster write policy"),
        replica_candidates: vec![node_id.clone()],
        wait_for_commit: false,
        foreground_replica_repair: true,
    }
}

/// Issues a POSIX write on the cluster's writer node (which is not the
/// metadata-shard primary, so the write routes over the network) and pumps the
/// cluster until the writer observes the committed content at the path.
fn sifs_cluster_routed_posix_write(cluster: &mut SifsPosixCluster, path: &str, bytes: &[u8]) {
    let expected_hash = content_hash(bytes);
    let options = sifs_cluster_write_options(&cluster.writer_node.clone());
    match cluster.writer.posix_write_file(path, bytes, options) {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed POSIX write to {path} failed: {error}"),
    }
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    sifs_posix_cluster_pump_until(cluster, path, |cluster| {
        if !sifs_routed_writes_settled(&cluster.writer) {
            return false;
        }
        let previous = cluster.writer.transport.operation_deadline();
        cluster.writer.set_transport_operation_deadline(Some(
            previous.map_or(deadline, |previous| previous.min(deadline)),
        ));
        let observed = cluster.writer.posix_stat_distributed(path);
        cluster.writer.set_transport_operation_deadline(previous);
        observed.is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
}

/// Complete the asynchronous primary-node write through its normal Main route
/// before setup continues, using the same bounded observation as routed writes.
fn sifs_cluster_primary_posix_write(cluster: &mut SifsPosixCluster, path: &str, bytes: &[u8]) {
    let options = sifs_cluster_write_options(&cluster.primary_node.clone());
    match cluster.primary.posix_write_file(path, bytes, options) {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("primary POSIX write to {path} failed: {error}"),
    }
    let expected_hash = content_hash(bytes);
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    let mut completed = None;
    sifs_posix_cluster_pump_until(cluster, path, |cluster| {
        if !sifs_routed_writes_settled(&cluster.primary) {
            return false;
        }
        let previous = cluster.primary.transport.operation_deadline();
        cluster.primary.set_transport_operation_deadline(Some(
            previous.map_or(deadline, |previous| previous.min(deadline)),
        ));
        let observed = cluster.primary.posix_stat_distributed(path);
        cluster.primary.set_transport_operation_deadline(previous);
        if let Ok(stat) = observed
            && stat.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            completed = Some(stat);
            true
        } else {
            false
        }
    });
    assert_eq!(
        completed
            .expect("actual Main write completion")
            .content_hash
            .as_deref(),
        Some(expected_hash.as_str()),
        "primary POSIX write to {path} must commit before setup continues"
    );
}

/// Runs the existing Main publisher and observes actual replicated state
/// until `done` completes within the original bounded window.
fn sifs_cluster_publish_metadata_until(
    cluster: &mut SifsPosixCluster,
    what: &str,
    mut done: impl FnMut(&mut SifsPosixCluster) -> bool,
) {
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        if done(cluster) {
            return;
        }
        let outcomes = cluster
            ._runtime
            .maintenance_tick(
                &cluster.primary_node,
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .expect("ordinary Main metadata publication");
        assert!(outcomes.outcome.is_ok(), "{:?}", outcomes.outcome);
        assert_eq!(outcomes.omitted_kv_event_count, 0);
        sifs_posix_cluster_pump_round(cluster);
        if Instant::now() >= deadline {
            panic!(
                "SIFS cluster metadata publication did not converge: {what}; \
                 last outcomes: {outcomes:?}; journal rejections: {:?}",
                cluster.rejections
            );
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
}

/// Makes remote content locally readable on a node by requesting a remote
/// read and pumping the cluster until the local read succeeds.
fn sifs_cluster_fetch_for_local_read(
    cluster: &mut SifsPosixCluster,
    node: fn(&mut SifsPosixCluster) -> &mut NetworkedDfsService<DfsNodeTransport>,
    path: &str,
) -> Vec<u8> {
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        match node(cluster).posix_read_file(path) {
            Ok(bytes) => return bytes,
            Err(DfsPosixError::NotFound { .. }) => {}
            Err(error) => panic!("cluster read of {path} failed: {error}"),
        }
        node(cluster)
            .posix_request_remote_read(path)
            .expect("request remote read");
        sifs_posix_cluster_pump_round(cluster);
        if Instant::now() >= deadline {
            panic!("cluster read of {path} did not become locally readable");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
}

fn sifs_routed_writes_settled<T: DfsMessageTransport>(service: &NetworkedDfsService<T>) -> bool {
    service
        .writes
        .pending_routed_writes
        .keys()
        .all(|request_id| {
            !matches!(
                service.write_outcome(request_id),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        })
}

#[derive(Debug)]
#[allow(dead_code)]
struct PosixScenarioObservation {
    scenario: &'static str,
    operation_id: String,
    node_id: String,
    path: String,
    result: String,
    content_hash: Option<String>,
    version: Option<String>,
    start_us: u128,
    end_us: u128,
}

fn record_posix_observation(
    observations: &mut Vec<PosixScenarioObservation>,
    scenario: &'static str,
    operation_id: impl Into<String>,
    node_id: &NodeId,
    path: impl Into<String>,
    started_at: Instant,
    result: impl Into<String>,
    stat: Option<&DfsStat>,
) {
    let elapsed = started_at.elapsed().as_micros();
    observations.push(PosixScenarioObservation {
        scenario,
        operation_id: operation_id.into(),
        node_id: node_id.to_string(),
        path: path.into(),
        result: result.into(),
        content_hash: stat.and_then(|stat| stat.content_hash.clone()),
        version: stat.map(|stat| stat.version.to_string()),
        start_us: 0,
        end_us: elapsed,
    });
}

fn finish_posix_observations(observations: &[PosixScenarioObservation]) {
    assert!(
        !observations.is_empty(),
        "scenario tests must record at least one operation observation"
    );
    eprintln!("SIFS POSIX scenario observations: {observations:#?}");
}

fn assert_sifs_cluster_no_posix_leaks(cluster: &SifsPosixCluster, context: &str) {
    for (label, service) in [
        ("primary", &cluster.primary),
        ("writer", &cluster.writer),
        ("observer", &cluster.observer),
    ] {
        let metrics = service.operational_metrics().expect("operational metrics");
        assert_eq!(
            metrics.pending_routed_namespace_op_count, 0,
            "{context}: {label} leaked routed namespace ops"
        );
        assert_eq!(
            metrics.cross_shard_namespace_lock_count, 0,
            "{context}: {label} leaked cross-shard namespace locks"
        );
    }
    assert!(
        cluster.rejections.is_empty(),
        "{context}: metadata journal rejections: {:?}",
        cluster.rejections
    );
}

fn assert_two_primary_no_posix_leaks(cluster: &SifsTwoPrimaryCluster, context: &str) {
    for (label, service) in [
        ("users-primary", &cluster.users_primary),
        ("services-primary", &cluster.services_primary),
        ("replica", &cluster.replica),
    ] {
        let metrics = service.operational_metrics().expect("operational metrics");
        assert_eq!(
            metrics.pending_routed_namespace_op_count, 0,
            "{context}: {label} leaked routed namespace ops"
        );
        assert_eq!(
            metrics.cross_shard_namespace_lock_count, 0,
            "{context}: {label} leaked cross-shard namespace locks"
        );
    }
    assert!(
        cluster.rejections.is_empty(),
        "{context}: metadata journal rejections: {:?}",
        cluster.rejections
    );
}

fn read_content_hash(bytes: &[u8]) -> String {
    content_hash(bytes)
}

fn pump_dfs_services_collect_rejections<T: DfsMessageTransport>(
    services: &mut [(&str, &mut NetworkedDfsService<T>)],
    rejections: &mut Vec<String>,
) -> usize {
    let mut activity = 0;
    for (label, service) in services.iter_mut() {
        let events = service
            .process_incoming()
            .expect("cluster process_incoming");
        activity += events.len();
        for event in events {
            if let NetworkedDfsServiceEvent::MetadataJournalBatchRejected { reason, .. } = event {
                rejections.push(format!("{label}: {reason}"));
            }
        }
    }
    activity
}

// ---------------------------------------------------------------------------
// SIFS cross-shard-primary POSIX transaction tests
//
// Two-primary topology mirroring the production shard-transfer lifecycle: one
// node bootstraps SIFS as primary of every shard, replicas catch up through
// journal publication, and the `/services` shard is then transferred to a
// second primary. Renames and hardlinks across the two primaries must apply
// transactionally: the destination primary adopts atomically, the source
// primary completes its half through a durable recoverable saga, and no
// replica journal ever forks.
// ---------------------------------------------------------------------------

struct SifsTwoPrimaryCluster {
    root: PathBuf,
    users_primary_node: NodeId,
    services_primary_node: NodeId,
    replica_node: NodeId,
    shared_kv: SharedDfsControlPlaneKvClient<Box<dyn DfsControlPlaneKvClient>>,
    users_primary: NetworkedDfsService<DfsNodeTransport>,
    services_primary: NetworkedDfsService<DfsNodeTransport>,
    replica: NetworkedDfsService<DfsNodeTransport>,
    rejections: Vec<String>,
    // Implicit panic cleanup has the same order as close(): external clients
    // release before the actual component/runtime owner begins shutdown.
    _runtime: crate::test_runtime::DfsTestRuntime,
}
impl SifsTwoPrimaryCluster {
    fn close(self) {
        // Drop the external clients before joining the actual component owner.
        let SifsTwoPrimaryCluster {
            _runtime,
            users_primary,
            services_primary,
            replica,
            shared_kv,
            ..
        } = self;
        drop((users_primary, services_primary, replica, shared_kv));
        _runtime
            .close()
            .expect("normal embedded DFS fixture shutdown");
    }
}

fn sifs_two_primary_shard_configs(
    users_primary: &NodeId,
    services_primary: &NodeId,
    services_epoch: u64,
    others_for_users: Vec<NodeId>,
    others_for_services: Vec<NodeId>,
) -> Vec<DfsMetadataShardConfig> {
    vec![
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: users_primary.clone(),
            replica_node_ids: others_for_users.clone(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "users".to_string(),
            primary_node_id: users_primary.clone(),
            replica_node_ids: others_for_users,
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/users".to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "services".to_string(),
            primary_node_id: services_primary.clone(),
            replica_node_ids: others_for_services,
            epoch: services_epoch,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ]
}

fn sifs_two_primary_pump_round(cluster: &mut SifsTwoPrimaryCluster) -> usize {
    let mut activity = 0;
    for (label, service) in [
        ("users-primary", &mut cluster.users_primary),
        ("services-primary", &mut cluster.services_primary),
        ("replica", &mut cluster.replica),
    ] {
        let events = service
            .process_incoming()
            .expect("cluster process_incoming");
        activity += events.len();
        for event in events {
            if let NetworkedDfsServiceEvent::MetadataJournalBatchRejected { reason, .. } = event {
                cluster.rejections.push(format!("{label}: {reason}"));
            }
        }
    }
    activity
}

fn sifs_two_primary_publish_until(
    cluster: &mut SifsTwoPrimaryCluster,
    what: &str,
    mut done: impl FnMut(&mut SifsTwoPrimaryCluster) -> bool,
) {
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        if done(cluster) {
            return;
        }
        for primary in [&cluster.users_primary_node, &cluster.services_primary_node] {
            let outcome = cluster
                ._runtime
                .maintenance_tick(
                    primary,
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                )
                .expect("ordinary Main metadata publication");
            assert!(outcome.outcome.is_ok(), "{:?}", outcome.outcome);
            assert_eq!(outcome.omitted_kv_event_count, 0);
        }
        sifs_two_primary_pump_round(cluster);
        if Instant::now() >= deadline {
            panic!(
                "two-primary cluster did not converge: {what}; rejections: {:?}",
                cluster.rejections
            );
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
}

fn pump_sifs_service_until_stop<T: DfsMessageTransport>(
    mut service: NetworkedDfsService<T>,
    stop: Arc<AtomicBool>,
) -> NetworkedDfsService<T> {
    while !stop.load(Ordering::SeqCst) {
        service.process_incoming().expect("services primary pump");
        if !stop.load(Ordering::SeqCst) {
            let _ = service
                .transport
                .wait_for_incoming(Duration::from_millis(10));
        }
    }
    service
}

fn sifs_two_primary_cluster(name: &str) -> SifsTwoPrimaryCluster {
    let prefix = name.replace('-', "_");
    let users_primary_node =
        NodeId::from_legacy_named(format!("node_{prefix}_users")).expect("users");
    let services_primary_node =
        NodeId::from_legacy_named(format!("node_{prefix}_services")).expect("services");
    let replica_node =
        NodeId::from_legacy_named(format!("node_{prefix}_replica")).expect("replica");
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, users_primary_node.clone())
        .expect("normal embedded runtime");
    runtime
        .define_node(services_primary_node.clone())
        .expect("services node");
    runtime
        .define_node(replica_node.clone())
        .expect("replica node");
    runtime.start_nodes().expect("normal component startup");
    let shared_kv = runtime
        .shared_control_plane()
        .expect("actual native control plane");
    let nodes = [users_primary_node, services_primary_node, replica_node];
    let configs = runtime
        .metadata_shards()
        .expect("actual current shard configuration");
    let users_config = configs
        .iter()
        .find(|config| config.shard_id == "users")
        .expect("normal users shard")
        .clone();
    let mut services_config = configs
        .iter()
        .find(|config| config.shard_id == "services")
        .expect("normal services shard")
        .clone();
    let users_primary_node = users_config.primary_node_id.clone();
    if services_config.primary_node_id == users_primary_node {
        // Native genesis may expand under its original owner. This fixture's
        // second primary is an explicit product handoff, not a discovery rule.
        let members = std::iter::once(services_config.primary_node_id.clone())
            .chain(services_config.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        assert_eq!(members, nodes.iter().cloned().collect());
        let target = services_config
            .replica_node_ids
            .iter()
            .find(|node| **node != users_primary_node)
            .expect("actual assigned replica may become the second primary")
            .clone();
        let replicas = members
            .iter()
            .filter(|node| **node != target)
            .cloned()
            .collect();
        let activated = runtime
            .reassign_metadata_shard(
                &services_config.file_system_id,
                &services_config.shard_id,
                target.clone(),
                replicas,
            )
            .expect("ordinary topology-preserving services primary handoff");
        assert_eq!(activated.primary_node_id, target);
        assert_eq!(
            std::iter::once(activated.primary_node_id.clone())
                .chain(activated.replica_node_ids.iter().cloned())
                .collect::<BTreeSet<_>>(),
            members
        );
        let current = runtime
            .metadata_shards()
            .expect("actual activated configurations");
        assert_eq!(
            current.iter().find(|config| config.shard_id == "users"),
            Some(&users_config)
        );
        assert_eq!(
            current.iter().find(|config| config.shard_id == "services"),
            Some(&activated)
        );
        services_config = activated;
    }
    let proof = shared_kv
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
                &services_config.file_system_id,
                &services_config.shard_id,
            )
        })
        .expect("actual services activation proof read")
        .expect("services role is backed by a real native activation");
    assert_eq!(proof.desired_shard, services_config);
    assert!(
        crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
            &proof.retiring_shard,
            &proof.desired_shard,
            &proof.acknowledged_node_ids.iter().cloned().collect()
        )
    );
    let services_primary_node = services_config.primary_node_id.clone();
    assert_ne!(users_primary_node, services_primary_node);
    assert!(nodes.contains(&users_primary_node) && nodes.contains(&services_primary_node));
    let replica_node = nodes
        .into_iter()
        .find(|node| *node != users_primary_node && *node != services_primary_node)
        .expect("remaining real replica node");
    let mut users_primary = runtime.client(&users_primary_node).expect("users client");
    let mut services_primary = runtime
        .client(&services_primary_node)
        .expect("services client");
    let mut replica = runtime.client(&replica_node).expect("replica client");
    for service in [&mut users_primary, &mut services_primary, &mut replica] {
        service
            .rebind_completed_sifs_metadata_shard_membership_handoffs()
            .expect("actual active generation");
    }
    let root = runtime
        .data_dir(&users_primary_node)
        .root()
        .parent()
        .expect("test root")
        .to_owned();
    SifsTwoPrimaryCluster {
        root,
        _runtime: runtime,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Regression tests for the cluster-correctness review fixes.
// ---------------------------------------------------------------------------

/// A 3-node SIFS cluster where `observer` is NOT in the root shard's replica
/// set: the primary is the namespace authority, `writer` receives the pushed
/// journal stream, and the outsider observer only converges its local mirror
/// through routed-response batches and SyncPath pulls.
fn sifs_outsider_cluster(name: &str) -> SifsPosixCluster {
    let mut cluster = sifs_posix_cluster(name, "/users");
    for config in cluster
        ._runtime
        .metadata_shards()
        .expect("native source configuration")
    {
        let replicas = vec![cluster.writer_node.clone()];
        cluster
            ._runtime
            .reassign_metadata_shard(
                &config.file_system_id,
                &config.shard_id,
                cluster.primary_node.clone(),
                replicas,
            )
            .expect("native outsider membership handoff");
    }
    for service in [
        &mut cluster.primary,
        &mut cluster.writer,
        &mut cluster.observer,
    ] {
        service
            .rebind_completed_sifs_metadata_shard_membership_handoffs()
            .expect("native active generation");
    }
    cluster
}

// Shared fixtures remain in this parent so each focused suite can use
// private transport and storage helpers without widening production APIs.
mod authority;
mod bootstrap;
mod content;
mod cross_shard;

mod locks;
mod maintenance;
mod metadata_commit_delivery;
mod namespace;
mod namespace_prepare;
mod namespace_requests;
mod patch_writes;
mod placement;
mod protocol;
mod reads;
mod recovery;
mod replication;
mod requester_prestage;
mod routed_delivery;
mod routing;
mod writes;

mod repair;
mod retention;

mod contribution_authority;
mod semantic_extensions;
