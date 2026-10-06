use super::*;
use crate::service_write::{
    DfsDistributedWriteOptions, DfsWriteCreation, DfsWriteDispatch, DfsWriteRepair,
};
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::events::EventId;
use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    AsyncInProcessNetwork, ClusterPaths, InProcessNetwork, NetworkError, ObjectRpcCall,
    ObjectRpcIncomingBody, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcResult,
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

#[derive(Clone, Debug)]
struct ReceiveOnlyTransport {
    local_node_id: NodeId,
    incoming: VecDeque<DfsRoutedMessage>,
}

impl ReceiveOnlyTransport {
    fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            incoming: VecDeque::new(),
        }
    }

    fn push_message(
        &mut self,
        source_node_id: NodeId,
        target_node_id: NodeId,
        message: DfsNetworkMessage,
    ) {
        self.incoming.push_back(build_dfs_routed_message(
            &source_node_id,
            &target_node_id,
            1,
            1,
            message,
        ));
    }
}

impl DfsTransportIdentity for ReceiveOnlyTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for ReceiveOnlyTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        Err(NetworkError::UnknownNode {
            node_id: target_node_id.clone(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self.incoming.drain(..).collect())
    }
}

#[derive(Debug)]
struct RangeSignalingControlPlaneKvClient {
    inner: InProcessKvStore,
    first_metadata_shard_range: std::sync::mpsc::SyncSender<()>,
    armed: AtomicBool,
    metadata_shard_range_count: AtomicUsize,
}

impl RangeSignalingControlPlaneKvClient {
    fn new(first_metadata_shard_range: std::sync::mpsc::SyncSender<()>) -> Self {
        Self {
            inner: InProcessKvStore::new(),
            first_metadata_shard_range,
            armed: AtomicBool::new(false),
            metadata_shard_range_count: AtomicUsize::new(0),
        }
    }

    fn arm(&self) {
        self.metadata_shard_range_count.store(0, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn metadata_shard_range_count(&self) -> usize {
        self.metadata_shard_range_count.load(Ordering::SeqCst)
    }
}

impl Clone for RangeSignalingControlPlaneKvClient {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            first_metadata_shard_range: self.first_metadata_shard_range.clone(),
            armed: AtomicBool::new(self.armed.load(Ordering::SeqCst)),
            metadata_shard_range_count: AtomicUsize::new(
                self.metadata_shard_range_count.load(Ordering::SeqCst),
            ),
        }
    }
}

impl DfsControlPlaneKvClient for RangeSignalingControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        DfsControlPlaneKvClient::refresh_latest(&mut self.inner)
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        DfsControlPlaneKvClient::get(&self.inner, key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        let entries = DfsControlPlaneKvClient::range_prefix(&self.inner, prefix)?;
        if prefix.contains("/metadata-shards/") {
            self.metadata_shard_range_count
                .fetch_add(1, Ordering::SeqCst);
            if self.armed.swap(false, Ordering::SeqCst) {
                let _ = self.first_metadata_shard_range.send(());
            }
        }
        Ok(entries)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        DfsControlPlaneKvClient::put(&mut self.inner, key, value)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        DfsControlPlaneKvClient::txn(&mut self.inner, txn)
    }
}

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

fn node_transport_with_canonical_message_peer(
    local_node_id: NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
    connect_timeout: Duration,
    sender: DfsMessageSender,
) -> DfsNodeTransport {
    let peer = Arc::new(CanonicalDfsMessagePeerTransport { sender });
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer;
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        connect_timeout,
        peer_transport,
        async_peer_transport,
    )
}

fn served_transport_with_canonical_message_peer(
    local_node_id: NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
    sender: DfsMessageSender,
    connect_timeout: Duration,
) -> DfsNodeServedMessageTransport {
    let peer_transport: Arc<dyn DfsPeerTransport> =
        Arc::new(CanonicalDfsMessagePeerTransport { sender });
    DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
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
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
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

struct FailingSendTransport {
    local_node_id: NodeId,
}

impl DfsTransportIdentity for FailingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for FailingSendTransport {
    fn send_routed_call(
        &mut self,
        _target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        Err(NetworkError::Io {
            operation: "test send".to_string(),
            detail: "unreachable".to_string(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

struct CountingFailingSendTransport {
    local_node_id: NodeId,
    send_count: Arc<AtomicUsize>,
}

struct RecordingFailingSendTransport {
    local_node_id: NodeId,
    targets: Arc<Mutex<Vec<NodeId>>>,
}

struct RecordingRoutedCallTransport {
    local_node_id: NodeId,
    calls: Arc<Mutex<Vec<(NodeId, DfsRoutedCall)>>>,
}

impl DfsTransportIdentity for RecordingRoutedCallTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for RecordingRoutedCallTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        self.calls
            .lock()
            .expect("recorded routed calls")
            .push((target_node_id.clone(), call));
        Ok(())
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

impl DfsTransportIdentity for RecordingFailingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for RecordingFailingSendTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        self.targets
            .lock()
            .expect("recording targets lock")
            .push(target_node_id.clone());
        Err(NetworkError::Io {
            operation: "test recorded send".to_string(),
            detail: "unreachable".to_string(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

impl AsyncDfsMessageTransport for RecordingFailingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move { self.send_routed_call(target_node_id, call) })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async { Ok(Some(0)) })
    }
}

impl DfsTransportIdentity for CountingFailingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for CountingFailingSendTransport {
    fn send_routed_call(
        &mut self,
        _target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        self.send_count.fetch_add(1, Ordering::Relaxed);
        Err(NetworkError::Io {
            operation: "test send".to_string(),
            detail: "unreachable".to_string(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

impl AsyncDfsMessageTransport for CountingFailingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move { self.send_routed_call(target_node_id, call) })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async { Ok(Some(0)) })
    }
}

struct DroppingSendTransport {
    local_node_id: NodeId,
}

struct ToggleSendTransport {
    local_node_id: NodeId,
    fail_sends: Arc<AtomicBool>,
}

/// Delivers one conditional namespace request, waits until its response is
/// queued locally, advances the shard authority, then reports an ambiguous
/// transport failure to the caller. This models a lost response at the exact
/// point where retry must retain the caller-owned operation identity.
struct PromoteAfterConditionalNamespaceDeliveryTransport {
    inner: sinomo_core::network::InProcessTransport,
    promotion: Option<(
        SharedDfsControlPlaneKvClient<InProcessKvStore>,
        DfsMetadataShardConfig,
    )>,
    durable_promotion_ready: Arc<AtomicBool>,
}

impl DfsTransportIdentity for PromoteAfterConditionalNamespaceDeliveryTransport {
    fn local_node_id(&self) -> &NodeId {
        self.inner.local_node_id()
    }
}

impl DfsMessageTransport for PromoteAfterConditionalNamespaceDeliveryTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        let inject_ambiguous_delivery = self.promotion.is_some()
            && matches!(
                &call.message.message,
                DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                    operation: DfsNamespaceOperation::ConditionalUnlink { .. }
                        | DfsNamespaceOperation::ConditionalRmdir { .. }
                        | DfsNamespaceOperation::ConditionalRename { .. },
                    ..
                })
            );
        self.inner.send_routed_call(target_node_id, call)?;
        if !inject_ambiguous_delivery {
            return Ok(());
        }

        let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
        while !(self
            .inner
            .pending_incoming_count()?
            .is_some_and(|count| count > 0)
            && self.durable_promotion_ready.load(Ordering::SeqCst))
        {
            if Instant::now() >= deadline {
                let pending = self.inner.pending_incoming_count()?;
                let durable_ready = self.durable_promotion_ready.load(Ordering::SeqCst);
                return Err(NetworkError::Io {
                    operation: "inject ambiguous conditional namespace delivery".to_string(),
                    detail: format!(
                        "authority handoff did not become safe before the test deadline: pending={pending:?} durable_ready={durable_ready}"
                    ),
                });
            }
            self.inner.wait_for_incoming(Duration::from_millis(10))?;
        }
        let (control_plane, promoted_shard) =
            self.promotion.take().expect("armed namespace promotion");
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&promoted_shard)
                .expect("promote authority after delivered namespace operation");
        });
        Err(NetworkError::Io {
            operation: "receive conditional namespace response".to_string(),
            detail: "injected response loss after remote durable application".to_string(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        self.inner.drain_incoming_messages()
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        self.inner.pending_incoming_count()
    }

    fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        self.inner.wait_for_incoming(timeout)
    }
}

impl DfsTransportIdentity for ToggleSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for ToggleSendTransport {
    fn send_routed_call(
        &mut self,
        _target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        if self.fail_sends.load(Ordering::SeqCst) {
            return Err(NetworkError::Io {
                operation: "test toggled send".to_string(),
                detail: "unreachable".to_string(),
            });
        }
        Ok(())
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

impl DfsTransportIdentity for DroppingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

struct DeadlineDroppingSendTransport {
    local_node_id: NodeId,
    operation_deadline: Option<Instant>,
    observed_deadline_sets: Arc<Mutex<Vec<Option<Instant>>>>,
}

impl DfsTransportIdentity for DeadlineDroppingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for DeadlineDroppingSendTransport {
    fn send_routed_call(
        &mut self,
        _target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(Some(0))
    }

    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        self.observed_deadline_sets
            .lock()
            .expect("deadline sets")
            .push(deadline);
        std::mem::replace(&mut self.operation_deadline, deadline)
    }

    fn operation_deadline(&self) -> Option<Instant> {
        self.operation_deadline
    }
}

impl DfsMessageTransport for DroppingSendTransport {
    fn send_routed_call(
        &mut self,
        _target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

fn node_transport_with_direct_read_sender(
    local_node_id: NodeId,
    remote_node_id: NodeId,
    bytes: Vec<u8>,
    direct_error: Option<String>,
    direct_calls: Arc<Mutex<Vec<DfsDirectReadRequest>>>,
    message_send_calls: Arc<Mutex<usize>>,
) -> DfsNodeTransport {
    let direct_calls_for_sender = Arc::clone(&direct_calls);
    let bytes_for_sender = bytes.clone();
    let direct_sender: DfsDirectReadSender = Arc::new(move |_target_node_id, request, _timeout| {
        direct_calls_for_sender
            .lock()
            .expect("direct calls")
            .push(request.clone());
        if let Some(detail) = &direct_error {
            return Err(DfsPeerTransportError::new(detail.clone()));
        }
        Ok(DfsReadResponse {
            request_id: request.request_id.clone(),
            object_id: request.object_id.clone(),
            result: DfsReadResult::Found(DfsReadData {
                object_id: request.object_id.clone(),
                version: request.version.clone(),
                content_hash: Some(request.content_hash.clone()),
                bytes: bytes_for_sender.clone(),
            }),
        })
    });
    let message_send_calls_for_sender = Arc::clone(&message_send_calls);
    let message_sender: DfsMessageSender = Arc::new(move |_target_node_id, _command, _timeout| {
        *message_send_calls_for_sender
            .lock()
            .expect("message send calls") += 1;
        Err(DfsPeerTransportError::new(
            "message sender should not be used by direct-read-eligible messages",
        ))
    });
    let async_sender: AsyncDfsMessageSender = Arc::new(|_target_node_id, _command, _timeout| {
        Box::pin(async {
            Err(DfsPeerTransportError::new(
                "async sender should not be used by direct-read tests",
            ))
        })
    });
    DfsNodeTransport::new_with_peer_senders(
        local_node_id,
        vec![remote_node_id],
        message_sender,
        async_sender,
        direct_sender,
    )
}

fn test_routed_write_command(
    request_id: &str,
    requester: &NodeId,
    object_id: &DfsObjectId,
) -> DfsDistributedWriteCommand {
    DfsDistributedWriteCommand {
        publication: Default::default(),
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
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    }
}

fn test_pending_routed_write(
    request_id: &str,
    requester: &NodeId,
    object_id: &DfsObjectId,
    primary_node_id: &NodeId,
    authority_shard: DfsMetadataShardConfig,
) -> PendingRoutedWrite {
    PendingRoutedWrite {
        canonical_request_id: request_id.to_string(),
        object_id: object_id.clone(),
        primary_node_id: primary_node_id.clone(),
        authority_shard,
        command: Some(test_routed_write_command(request_id, requester, object_id)),
        attachment_bytes: None,
        authority_redirects_remaining: DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT,
        remote_intent_id: None,
        durable_receipts: 0,
        required_receipts: 1,
        result: None,
        compact_retained_at: None,
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

fn test_routed_response_journal_batch(
    metadata_root: &Path,
    primary_node_id: &NodeId,
    requester_node_id: &NodeId,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    path: &str,
) -> DfsMetadataJournalBatch {
    let file_system = file_system(primary_node_id.clone());
    let mut object = file_metadata(primary_node_id.clone());
    object.object_id = object_id.clone();
    object.file_system_id = file_system.file_system_id.clone();
    object.path = path.to_string();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.content_hash = Some("blake3:routed-response-authority".to_string());
    object.content_size_bytes = Some(34);
    object.current_locations = vec![primary_node_id.clone(), requester_node_id.clone()];

    let mut backend =
        open_root_local_metadata_backend(metadata_root).expect("routed response source metadata");
    backend
        .put_file_system(&file_system)
        .expect("routed response source filesystem");
    backend
        .put_object_and_version(
            &object,
            &object_version_record(&object, object.content_size_bytes),
        )
        .expect("routed response source object");
    backend
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("routed response source journal")
        .expect("routed response source journal batch")
}

#[derive(Clone, Debug)]
struct ClusterPathDfsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
}

impl ClusterPathDfsControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> sinomo_core::kv::KvError {
        sinomo_core::kv::KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

fn append_cluster_kv_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: sinomo_core::kv::KvCommand,
) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::network::NetworkError> {
    paths
        .kv_paths()
        .append_kv_command_and_publish(
            node_id,
            command,
            sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(sinomo_core::network::NetworkError::directory)
}

fn append_cluster_kv_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: sinomo_core::kv::KvCommand,
) -> Result<
    (
        sinomo_core::kv::KvSnapshot,
        Option<sinomo_core::kv::KvTxnResponse>,
    ),
    sinomo_core::network::NetworkError,
> {
    paths
        .kv_paths()
        .append_kv_command_with_response_and_publish(
            node_id,
            command,
            sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(sinomo_core::network::NetworkError::directory)
}

impl DfsControlPlaneKvClient for ClusterPathDfsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
        let response = self.txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![sinomo_core::kv::KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
        let state = sinomo_core::cluster::read_cluster_state(&self.cluster_paths)
            .map_err(|source| self.kv_error(source))?;
        let (snapshot, response) = append_cluster_kv_command_with_response(
            &self.cluster_paths,
            &state.local_node_id,
            sinomo_core::kv::KvCommand::Txn(txn),
        )
        .map_err(|source| self.kv_error(source))?;
        Ok(response.unwrap_or(sinomo_core::kv::KvTxnResponse {
            succeeded: true,
            revision: snapshot.revision,
            responses: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct CountingDfsControlPlaneKvClient {
    inner: InProcessKvStore,
    range_prefix_calls: Arc<AtomicUsize>,
}

impl CountingDfsControlPlaneKvClient {
    fn new(inner: InProcessKvStore, range_prefix_calls: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            range_prefix_calls,
        }
    }
}

impl DfsControlPlaneKvClient for CountingDfsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::get(&self.inner, key)
    }

    fn range_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.range_prefix_calls.fetch_add(1, Ordering::SeqCst);
        DfsControlPlaneKvClient::range_prefix(&self.inner, prefix)
    }

    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::put(&mut self.inner, key, value)
    }

    fn txn(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::txn(&mut self.inner, txn)
    }
}

#[derive(Debug)]
struct CountingSharedDfsControlPlaneKvClient {
    inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    range_prefix_calls: Arc<AtomicUsize>,
}

impl CountingSharedDfsControlPlaneKvClient {
    fn new(
        inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
        range_prefix_calls: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            range_prefix_calls,
        }
    }
}

impl DfsControlPlaneKvClient for CountingSharedDfsControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
        self.inner.with_client(|kv| kv.refresh_latest())
    }

    fn get(&self, key: &str) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::get(kv, key))
    }

    fn range_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.range_prefix_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::range_prefix(kv, prefix))
    }

    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::put(kv, key, value))
    }

    fn txn(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::txn(kv, txn))
    }
}

#[derive(Clone, Debug)]
struct FailingMetadataOperationFloorReadKvClient {
    inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    fail_operation_floor_reads: Arc<AtomicBool>,
    failed_operation_floor_reads: Arc<AtomicUsize>,
    refresh_calls: Arc<AtomicUsize>,
}

impl FailingMetadataOperationFloorReadKvClient {
    fn new(inner: SharedDfsControlPlaneKvClient<InProcessKvStore>) -> Self {
        Self {
            inner,
            fail_operation_floor_reads: Arc::new(AtomicBool::new(false)),
            failed_operation_floor_reads: Arc::new(AtomicUsize::new(0)),
            refresh_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn set_fail_operation_floor_reads(&self, fail: bool) {
        self.fail_operation_floor_reads
            .store(fail, Ordering::SeqCst);
    }
}

impl DfsControlPlaneKvClient for FailingMetadataOperationFloorReadKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.refresh_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.with_client(|kv| kv.refresh_latest())
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        if key.contains("/metadata-operation-floors/")
            && self.fail_operation_floor_reads.load(Ordering::SeqCst)
        {
            self.failed_operation_floor_reads
                .fetch_add(1, Ordering::SeqCst);
            return Err(KvError::Io {
                path: "test-cluster-authoritative-view".to_string(),
                detail: "resident Raft view changed after ReadIndex quorum; retry the read"
                    .to_string(),
            });
        }
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::get(kv, key))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::range_prefix(kv, prefix))
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::put(kv, key, value))
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::txn(kv, txn))
    }
}

#[derive(Clone, Debug)]
struct FailingFileSystemConfigReadKvClient {
    inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    fail_reads: Arc<AtomicBool>,
}

impl DfsControlPlaneKvClient for FailingFileSystemConfigReadKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.inner.with_client(|kv| kv.refresh_latest())
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        if self.fail_reads.load(Ordering::SeqCst)
            && key == file_system_config_key(&sifs_file_system_id())
        {
            return Err(KvError::AuthorityUnavailable {
                detail: "daemon DFS retained metadata authority generation 1 is not ready"
                    .to_string(),
            });
        }
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::get(kv, key))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::range_prefix(kv, prefix))
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::put(kv, key, value))
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::txn(kv, txn))
    }
}

#[derive(Clone)]
struct CountingEmptyTransport {
    local_node_id: NodeId,
    drain_calls: Arc<Mutex<usize>>,
}

impl CountingEmptyTransport {
    fn new(local_node_id: NodeId, drain_calls: Arc<Mutex<usize>>) -> Self {
        Self {
            local_node_id,
            drain_calls,
        }
    }
}

#[derive(Default)]
struct RecordingPeerTransport {
    calls: Mutex<Vec<String>>,
    options: Mutex<Vec<(String, bool)>>,
}

impl RecordingPeerTransport {
    fn call_materialized_value(
        &self,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        self.options
            .lock()
            .expect("options")
            .push((request.route.clone(), options.observe_connectivity));
        let response = match request.route.as_str() {
            route if dfs_route_is_message_data_plane(route) => {
                let command: DfsMessageCommand = request
                    .decode_body()
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                self.calls.lock().expect("calls").push(format!(
                    "rpc:{}:{}:{}",
                    request.route, target_node_id, command.request_id
                ));
                ObjectRpcResponse::encode_body(
                    route,
                    &DfsMessageResponse {
                        request_id: command.request_id,
                        outbound_messages: Vec::new(),
                    },
                )
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
            }
            DFS_DIRECT_READ_ROUTE => {
                let command: DfsDirectReadRequest = request
                    .decode_body()
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                self.calls.lock().expect("calls").push(format!(
                    "rpc:{}:{}:{}",
                    request.route, target_node_id, command.request_id
                ));
                return dfs_direct_read_rpc_result(DfsReadResponse {
                    request_id: command.request_id,
                    object_id: command.object_id.clone(),
                    result: DfsReadResult::Found(DfsReadData {
                        object_id: command.object_id,
                        version: command.version,
                        content_hash: Some(command.content_hash),
                        bytes: vec![1, 2, 3],
                    }),
                })
                .map_err(|source| DfsPeerTransportError::new(source.to_string()));
            }
            DFS_METADATA_JOURNAL_BATCH_ROUTE => {
                let command: DfsMetadataJournalBatchCommand = request
                    .decode_body()
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                self.calls.lock().expect("calls").push(format!(
                    "rpc:{}:{}:{}",
                    request.route, target_node_id, command.request_id
                ));
                ObjectRpcResponse::encode_body(
                    DFS_METADATA_JOURNAL_BATCH_ROUTE,
                    &DfsMetadataJournalBatchResponse {
                        request_id: command.request_id.clone(),
                        shard_id: command.batch.shard_id.clone(),
                        next_after_sequence: command.batch.next_after_sequence,
                        applied_entries: applied_metadata_journal_command_entry_count(&command),
                        verified_prefix_only: command.verify_prefix_only,
                        verified_authority_probe_id: applied_metadata_journal_command_confirmation(
                            &command,
                        ),
                        result: DfsMetadataJournalBatchResult::Applied,
                    },
                )
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
            }
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE => {
                let command: DfsMetadataSnapshotInstallCommand = request
                    .decode_body()
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                self.calls.lock().expect("calls").push(format!(
                    "rpc:{}:{}:{}",
                    request.route, target_node_id, command.request_id
                ));
                ObjectRpcResponse::encode_body(
                    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
                    &DfsMetadataSnapshotInstallResponse {
                        request_id: command.request_id,
                        shard_id: command.shard_id,
                        next_after_sequence: command.next_after_sequence,
                        result: DfsMetadataSnapshotInstallResult::Installed {
                            archived_existing: false,
                        },
                    },
                )
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
            }
            other => Err(DfsPeerTransportError::new(format!(
                "unexpected DFS object RPC route {other}"
            ))),
        }?;
        Ok(ObjectRpcResult::value(response))
    }
}

impl DfsPeerTransport for RecordingPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let request = crate::node_transport::materialize_dfs_value_call_sync(call, usize::MAX)?;
        self.call_materialized_value(target_node_id, request, options)
    }
}

impl AsyncDfsPeerTransport for RecordingPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async move {
            let request =
                crate::node_transport::materialize_dfs_value_call(call, usize::MAX).await?;
            self.call_materialized_value(target_node_id, request, options)
        })
    }
}

#[derive(Default)]
struct ConcurrentMetadataPeerTransport {
    active_calls: AtomicUsize,
    max_active_calls: AtomicUsize,
}

impl DfsPeerTransport for ConcurrentMetadataPeerTransport {
    fn call_object_rpc(
        &self,
        _target_node_id: &NodeId,
        call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let request = crate::node_transport::materialize_dfs_value_call_sync(call, usize::MAX)?;
        let active = self.active_calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_calls.fetch_max(active, Ordering::SeqCst);
        // Yield the worker long enough for the other scoped workers to enter.
        // A sequential implementation can never observe more than one active
        // call, independent of machine speed.
        std::thread::sleep(Duration::from_millis(25));
        self.active_calls.fetch_sub(1, Ordering::SeqCst);

        let command: DfsMetadataJournalBatchCommand = request
            .decode_body()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let verified_authority_probe_id = command
            .authority_probe
            .as_ref()
            .and_then(crate::service_replica::metadata_authority_probe_confirmation_id);
        ObjectRpcResponse::encode_body(
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            &DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id: command.batch.shard_id,
                next_after_sequence: command.batch.next_after_sequence,
                applied_entries: command.batch.entries.len(),
                verified_prefix_only: command.verify_prefix_only,
                verified_authority_probe_id,
                result: DfsMetadataJournalBatchResult::Applied,
            },
        )
        .map(ObjectRpcResult::value)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

fn recording_async_peer_transport(
    peer: Arc<RecordingPeerTransport>,
) -> Arc<dyn AsyncDfsPeerTransport> {
    peer
}

impl DfsTransportIdentity for CountingEmptyTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for CountingEmptyTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        _call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        Err(NetworkError::UnknownNode {
            node_id: target_node_id.clone(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        *self.drain_calls.lock().expect("drain calls") += 1;
        Ok(Vec::new())
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(Some(0))
    }
}

fn node_transport_with_streamed_direct_read_body(
    local: NodeId,
    remote: NodeId,
    body: ObjectRpcIncomingBody,
) -> DfsNodeTransport {
    let body = Arc::new(Mutex::new(Some(body)));
    let body_for_sender = Arc::clone(&body);
    let async_direct_sender: AsyncDfsDirectReadSender =
        Arc::new(move |_target_node_id, request, _options| {
            let request = request.clone();
            let body = body_for_sender.lock().expect("stream body slot").take();
            Box::pin(async move {
                let body = body.ok_or_else(|| {
                    DfsPeerTransportError::new("stream test body was requested more than once")
                })?;
                let content_size_bytes =
                    usize::try_from(request.content_size_bytes).map_err(|_| {
                        DfsPeerTransportError::new("stream test content length does not fit usize")
                    })?;
                let stream = DfsVerifiedReadStream::new(
                    body,
                    content_size_bytes,
                    Some(request.content_hash.clone()),
                )
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                Ok(DfsDirectReadClientResponse {
                    request_id: request.request_id,
                    object_id: request.object_id.clone(),
                    result: DfsDirectReadClientResult::Found(DfsDirectReadClientData {
                        object_id: request.object_id,
                        version: request.version,
                        content_hash: request.content_hash,
                        content_size_bytes,
                        stream,
                    }),
                })
            })
        });
    let async_sender: AsyncDfsMessageSender = Arc::new(|_target_node_id, _command, _options| {
        Box::pin(async {
            Err(DfsPeerTransportError::new(
                "stream-native direct read must not use the message sender",
            ))
        })
    });
    let sync_sender: DfsMessageSender = Arc::new(|_target_node_id, _command, _options| {
        Err(DfsPeerTransportError::new(
            "stream-native direct read must not use the synchronous message sender",
        ))
    });
    let sync_direct_sender: DfsDirectReadSender =
        Arc::new(|_target_node_id, _request, _options| {
            Err(DfsPeerTransportError::new(
                "stream-native direct read must not use the synchronous direct sender",
            ))
        });
    DfsNodeTransport::new_with_message_peer_senders(
        local,
        [remote],
        sync_sender,
        async_sender,
        sync_direct_sender,
        async_direct_sender,
    )
}

#[derive(Clone, Debug)]
struct ApplyingMetadataJournalTransport {
    target_node_id: NodeId,
    batches: usize,
    snapshot_installs: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct UnavailableAsyncMetadataJournalTransport;

impl AsyncDfsMetadataJournalReplicationTransport for UnavailableAsyncMetadataJournalTransport {
    fn has_metadata_replication_route(&self, _target_node_id: &NodeId) -> bool {
        false
    }

    fn send_metadata_journal_batch_async<'a>(
        &'a self,
        _target_node_id: &'a NodeId,
        _command: &'a DfsMetadataJournalBatchCommand,
        _timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>,
    > {
        Box::pin(async { Err(DfsMetadataJournalReplicationTransportError::MissingRoute) })
    }

    fn send_metadata_snapshot_install_async<'a>(
        &'a self,
        _target_node_id: &'a NodeId,
        _command: &'a DfsMetadataSnapshotInstallCommand,
        _timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>,
    > {
        Box::pin(async { Err(DfsMetadataJournalReplicationTransportError::MissingRoute) })
    }
}

fn applied_metadata_journal_command_entry_count(command: &DfsMetadataJournalBatchCommand) -> usize {
    command
        .durable_replication
        .as_ref()
        .map(|replication| replication.journal_frames.len())
        .or_else(|| {
            command.authority_probe.as_ref().and_then(|probe| {
                probe
                    .durable_journal_ack
                    .then_some(probe.journal_frames.len())
            })
        })
        .unwrap_or(command.batch.entries.len())
}

fn applied_metadata_journal_command_confirmation(
    command: &DfsMetadataJournalBatchCommand,
) -> Option<String> {
    command
        .durable_replication
        .as_ref()
        .map(|replication| replication.confirmation_id.clone())
        .or_else(|| {
            command
                .authority_probe
                .as_ref()
                .and_then(crate::service_replica::metadata_authority_probe_confirmation_id)
        })
}

impl ApplyingMetadataJournalTransport {
    fn new(target_node_id: NodeId) -> Self {
        Self {
            target_node_id,
            batches: 0,
            snapshot_installs: 0,
        }
    }
}

impl DfsMetadataJournalReplicationTransport for ApplyingMetadataJournalTransport {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
        target_node_id == &self.target_node_id
    }

    fn send_metadata_journal_batch(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        _timeout: Duration,
    ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError> {
        if !self.has_metadata_replication_route(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        self.batches += 1;
        Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id.clone(),
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence,
            applied_entries: applied_metadata_journal_command_entry_count(command),
            verified_prefix_only: command.verify_prefix_only,
            verified_authority_probe_id: applied_metadata_journal_command_confirmation(command),
            result: DfsMetadataJournalBatchResult::Applied,
        })
    }

    fn send_metadata_snapshot_install(
        &mut self,
        target_node_id: &NodeId,
        _command: &DfsMetadataSnapshotInstallCommand,
        _timeout: Duration,
    ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>
    {
        if !self.has_metadata_replication_route(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        self.snapshot_installs += 1;
        Err(DfsMetadataJournalReplicationTransportError::Failed {
            reason: "unexpected snapshot install".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct SequenceConflictMetadataJournalTransport {
    target_node_id: NodeId,
    batches: usize,
    snapshot_installs: usize,
    rejection_reason: String,
}

impl SequenceConflictMetadataJournalTransport {
    fn new(target_node_id: NodeId) -> Self {
        Self::new_with_reason(
            target_node_id,
            "metadata journal sequence 1 already exists with different exact bytes while replaying shard root",
        )
    }

    fn new_with_reason(target_node_id: NodeId, rejection_reason: impl Into<String>) -> Self {
        Self {
            target_node_id,
            batches: 0,
            snapshot_installs: 0,
            rejection_reason: rejection_reason.into(),
        }
    }
}

impl DfsMetadataJournalReplicationTransport for SequenceConflictMetadataJournalTransport {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
        target_node_id == &self.target_node_id
    }

    fn send_metadata_journal_batch(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        _timeout: Duration,
    ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError> {
        if !self.has_metadata_replication_route(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        self.batches += 1;
        Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id.clone(),
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            verified_authority_probe_id: None,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: self.rejection_reason.clone(),
                code: None,
            },
        })
    }

    fn send_metadata_snapshot_install(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataSnapshotInstallCommand,
        _timeout: Duration,
    ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>
    {
        if !self.has_metadata_replication_route(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        self.snapshot_installs += 1;
        Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id.clone(),
            shard_id: command.shard_id.clone(),
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed {
                archived_existing: true,
            },
        })
    }
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

#[derive(Debug)]
struct FixedSmokeHost {
    pending_repair_count: usize,
}

impl DfsSmokeHost for FixedSmokeHost {
    fn pending_repair_count(&mut self) -> Result<usize, DfsError> {
        Ok(self.pending_repair_count)
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
}

impl DfsMaintenanceHost for RecordingMaintenanceHost {
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
        metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
        display_name: "repo".to_string(),
        owner_node_id: owner,
        subtree_policies: Vec::new(),
    }
}

#[derive(Debug)]
struct RefreshingDfsControlPlaneKvClient {
    inner: InProcessKvStore,
    current: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    refreshes: Arc<AtomicUsize>,
}

impl RefreshingDfsControlPlaneKvClient {
    fn new(
        inner: InProcessKvStore,
        current: SharedDfsControlPlaneKvClient<InProcessKvStore>,
        refreshes: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            current,
            refreshes,
        }
    }
}

impl DfsControlPlaneKvClient for RefreshingDfsControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
        let snapshot = self.current.with_client(|kv| kv.try_snapshot())?;
        self.inner = InProcessKvStore::restore(snapshot)?;
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::get(&self.inner, key)
    }

    fn range_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::range_prefix(&self.inner, prefix)
    }

    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::put(&mut self.inner, key, value)
    }

    fn txn(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
        DfsControlPlaneKvClient::txn(&mut self.inner, txn)
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

fn open_shared_metadata_posix_service(
    root: &Path,
    metadata_root: &Path,
    node_id: NodeId,
    content_name: &str,
) -> NetworkedDfsService<ReceiveOnlyTransport> {
    let metadata = open_root_local_metadata_backend(metadata_root).expect("metadata backend");
    let content = LocalContentStore::new(root.join(content_name)).expect("content store");
    NetworkedDfsService::new(ReceiveOnlyTransport::new(node_id), content)
        .with_metadata_backend(metadata)
        .expect("shared metadata service")
}

fn networked_sifs_with_backend(
    root: &Path,
    node_id: NodeId,
) -> NetworkedDfsService<sinomo_core::network::InProcessTransport> {
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs
}

fn drain_dfs_messages(
    transport: &mut sinomo_core::network::InProcessTransport,
) -> Vec<DfsNetworkMessage> {
    DfsMessageTransport::drain_incoming_messages(transport)
        .expect("drain DFS messages")
        .into_iter()
        .map(|message| message.message)
        .collect()
}

trait InProcessDfsTestTransportExt {
    fn drain_dfs_incoming(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError>;
}

impl InProcessDfsTestTransportExt for sinomo_core::network::InProcessTransport {
    fn drain_dfs_incoming(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        DfsMessageTransport::drain_incoming_messages(self)
    }
}

struct StatelessPrimaryPeerTransport {
    primary_node_id: NodeId,
    content_root: PathBuf,
    metadata_root: PathBuf,
    control_plane: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    trace: Arc<Mutex<Vec<String>>>,
}

struct AttachmentCapableTestPeerTransport {
    sender: DfsMessageSender,
}

impl AttachmentCapableTestPeerTransport {
    async fn call_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let streamed_value = call.has_streamed_value_body();
        let ObjectRpcCall { mut request, body } = call;
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
                    "test DFS request attachment does not fit this platform address space",
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

impl DfsPeerTransport for AttachmentCapableTestPeerTransport {
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
        let result = runtime.block_on(self.call_inner(target_node_id, call, options));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }
}

impl AsyncDfsPeerTransport for AttachmentCapableTestPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(self.call_inner(target_node_id, call, options))
    }
}

fn node_transport_with_attachment_sender(
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    sender: DfsMessageSender,
) -> DfsNodeTransport {
    let peer = Arc::new(AttachmentCapableTestPeerTransport { sender });
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        Duration::from_millis(1),
        peer.clone(),
        peer,
    )
}

impl StatelessPrimaryPeerTransport {
    async fn call_object_rpc_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        if target_node_id != &self.primary_node_id {
            return Err(DfsPeerTransportError::new(format!(
                "unexpected stateless DFS target {target_node_id}"
            )));
        }
        let streamed_value = call.has_streamed_value_body();
        let ObjectRpcCall { mut request, body } = call;
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
                    "stateless DFS request attachment does not fit this platform address space",
                )
            })?;
            let bytes = body
                .collect_to_vec(max_bytes)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            crate::body_rpc::attach_dfs_message_request_attachment(head, bytes)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        };
        self.trace
            .lock()
            .expect("trace")
            .push(format!("in:{:?}", command.routed_message.message));
        let content = LocalContentStore::new(&self.content_root).expect("content");
        let metadata =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("services"), &self.metadata_root)
                .expect("metadata");
        let result = process_dfs_message_command_for_test(
            self.primary_node_id.clone(),
            command,
            content,
            Box::new(metadata),
            Some(Box::new(self.control_plane.clone())),
        )
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        for event in &result.events {
            self.trace
                .lock()
                .expect("trace")
                .push(format!("event:{event:?}"));
        }
        for message in &result.response.outbound_messages {
            self.trace
                .lock()
                .expect("trace")
                .push(format!("out:{:?}", message.message));
        }
        if let Some(attachment) = result.attachment {
            let max_bytes = usize::try_from(attachment.descriptor.total_size_bytes).map_err(|_| {
                DfsPeerTransportError::new(
                    "stateless DFS response attachment does not fit this platform address space",
                )
            })?;
            let bytes = attachment
                .source
                .collect_to_vec(max_bytes)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = crate::body_rpc::encode_dfs_attachment_message_rpc_response(
                &request.route,
                result.response,
                attachment.descriptor,
            )
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            return Ok(ObjectRpcResult::new(
                response,
                ObjectRpcIncomingBody::bytes(bytes),
            ));
        }
        ObjectRpcResponse::encode_body(&request.route, &result.response)
            .map(ObjectRpcResult::value)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

impl DfsPeerTransport for StatelessPrimaryPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = runtime.block_on(self.call_object_rpc_inner(target_node_id, call));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }
}

impl AsyncDfsPeerTransport for StatelessPrimaryPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(self.call_object_rpc_inner(target_node_id, call))
    }
}

fn seeded_durable_metadata_replica(
    root: PathBuf,
    shards: Vec<DfsMetadataShardConfig>,
    file_system: &DfsFileSystem,
    object: &DfsObjectMetadata,
) -> LocalShardedMetadataBackend {
    let mut backend =
        open_local_sharded_metadata_backend(root, shards).expect("metadata replica backend");
    backend
        .put_file_system(file_system)
        .expect("replicated filesystem metadata");
    let mut root_object = crate::namespace::directory_object(
        crate::namespace::SIFS_ROOT_PATH,
        file_system.owner_node_id.clone(),
    );
    root_object.file_system_id = file_system.file_system_id.clone();
    backend
        .put_object(&root_object)
        .expect("replicated root directory metadata");
    backend
        .put_object(object)
        .expect("replicated object metadata");
    backend
}

fn counting_failing_pending_stage_service(
    root: &Path,
    fixture_name: &str,
    pending_intent_count: usize,
) -> (
    NetworkedDfsService<CountingFailingSendTransport>,
    Arc<AtomicUsize>,
) {
    let owner = NodeId::from_legacy_named(format!("node_{fixture_name}_owner")).expect("owner");
    let replica =
        NodeId::from_legacy_named(format!("node_{fixture_name}_replica")).expect("replica");
    let send_count = Arc::new(AtomicUsize::new(0));
    let mut dfs = NetworkedDfsService::new(
        CountingFailingSendTransport {
            local_node_id: owner.clone(),
            send_count: Arc::clone(&send_count),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.add_file_system(file_system(owner.clone()))
        .expect("filesystem");

    for index in 0..pending_intent_count {
        let object_id =
            DfsObjectId::from_legacy_named(format!("obj_{fixture_name}_pending_stage_{index}"))
                .expect("object");
        let base_version = DfsVersion::from_legacy_named(format!(
            "version_{fixture_name}_pending_stage_{index}_base"
        ))
        .expect("base version");
        let target_version = DfsVersion::from_legacy_named(format!(
            "version_{fixture_name}_pending_stage_{index}_target"
        ))
        .expect("target version");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.path = format!("/services/{fixture_name}-{index}.bin");
        metadata.version = base_version.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(metadata).expect("object metadata");
        let bytes = format!("{fixture_name} pending stage body {index}").into_bytes();
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), target_version.as_str(), &bytes)
            .expect("target content");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(base_version),
                target_version.clone(),
                owner.clone(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("W2 policy"),
            )
            .expect("pending intent");
        let awaiting = dfs
            .coordinator
            .record_durable_receipt(&intent.intent_id, owner.clone())
            .expect("owner receipt");
        assert!(matches!(
            awaiting.state,
            DfsWriteIntentState::AwaitingReceipts
        ));
        dfs.writes.pending_writes.insert(
            intent.intent_id,
            PendingNetworkedWrite::new(
                object_id,
                target_version,
                manifest.content_hash,
                manifest.size_bytes,
            ),
        );
    }
    (dfs, send_count)
}

struct DurableCommitPromotionFixture {
    owner: NodeId,
    replica: NodeId,
    object_id: DfsObjectId,
    target_version: DfsVersion,
    intent_id: String,
    content_hash: String,
    metadata_root: PathBuf,
    content_root: PathBuf,
}

fn seed_durable_commit_promotion_fixture(
    root: &Path,
    suffix: &str,
) -> DurableCommitPromotionFixture {
    let owner =
        NodeId::from_legacy_named(format!("node_recovery_hydrate_owner_{suffix}")).expect("owner");
    let replica = NodeId::from_legacy_named(format!("node_recovery_hydrate_replica_{suffix}"))
        .expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named(format!("obj_recovery_hydrate_{suffix}")).expect("object");
    let base_version =
        DfsVersion::from_legacy_named(format!("version_recovery_hydrate_base_{suffix}"))
            .expect("base version");
    let target_version =
        DfsVersion::from_legacy_named(format!("version_recovery_hydrate_target_{suffix}"))
            .expect("target version");
    let metadata_root = root.join("metadata");
    let content_root = root.join("content");
    let content = LocalContentStore::new(&content_root).expect("content store");
    let manifest = content
        .put_object_version(
            object_id.as_str(),
            target_version.as_str(),
            format!("durable promotion recovery {suffix}").as_bytes(),
        )
        .expect("content manifest");
    let file_system = file_system(owner.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = format!("/services/recovery-hydrate-{suffix}.txt");
    object.version = target_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.content_hash = Some(manifest.content_hash.clone());
    object.content_size_bytes = Some(manifest.size_bytes);
    object.current_locations = vec![owner.clone(), replica.clone()];
    let intent_id = format!("dfs_intent_recovery_hydrate_{suffix}");
    let now = OffsetDateTime::now_utc();
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id,
        file_system_id: file_system.file_system_id.clone(),
        object_id: object_id.clone(),
        base_version: Some(base_version),
        target_version: target_version.clone(),
        coordinator_node_id: owner.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: vec![owner.clone(), replica.clone()],
        policy: DfsReplicationPolicy::new(2, 2).expect("replication policy"),
        status: DfsWriteIntentStatus::Committed,
        content_hash: Some(manifest.content_hash.clone()),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: now,
        updated_at: Some(now),
    };
    let receipts =
        [owner.clone(), replica.clone()].map(|replica_node_id| DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file_system.file_system_id.clone(),
                object_id: object_id.clone(),
                committed_version: target_version.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id,
            content_hash: Some(manifest.content_hash.clone()),
            received_at: now,
        });
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend
        .put_object_and_version(
            &object,
            &object_version_record(&object, Some(manifest.size_bytes)),
        )
        .expect("object and version");
    backend.put_intent(&intent).expect("committed intent");
    for receipt in receipts {
        backend.put_receipt(&receipt).expect("durable receipt");
    }
    DurableCommitPromotionFixture {
        owner,
        replica,
        object_id,
        target_version,
        intent_id: intent.intent_id,
        content_hash: manifest.content_hash,
        metadata_root,
        content_root,
    }
}

fn recovery_hydration_node_transport(
    owner: NodeId,
    replica: NodeId,
    detached_send_state: DfsNodeDetachedSendState,
) -> DfsNodeTransport {
    let peer = Arc::new(RecordingPeerTransport::default());
    let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
    let async_peer = recording_async_peer_transport(peer);
    DfsNodeTransport::with_peer_transports_for_nodes_and_detached_state(
        owner,
        vec![replica],
        Duration::from_secs(1),
        sync_peer,
        async_peer,
        detached_send_state,
    )
}

fn recovered_new_object_parent_metadata(
    touch_parent_on_create: bool,
) -> (DfsObjectMetadata, DfsObjectMetadata, DfsObjectMetadata) {
    let case = if touch_parent_on_create {
        "parent-touch"
    } else {
        "raw-write"
    };
    let root = temp_root(&format!("hydrate-new-object-{case}"));
    let owner = NodeId::from_legacy_named(format!("node_hydrate_{case}_primary")).expect("node");
    let requester =
        NodeId::from_legacy_named(format!("node_hydrate_{case}_requester")).expect("node");
    let replica = NodeId::from_legacy_named(format!("node_hydrate_{case}_replica")).expect("node");
    let object_id =
        DfsObjectId::from_legacy_named(format!("obj_hydrate_{case}_child")).expect("object");
    let parent_object_id =
        DfsObjectId::from_legacy_named(format!("obj_hydrate_{case}_parent")).expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let parent_version = DfsVersion::from_legacy_named("version_parent_1").expect("version");
    let bytes = format!("commit-ready {case} bytes").into_bytes();
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let file_system = file_system(owner.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "owner".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![requester.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
        .expect("metadata backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(backend)
    .expect("metadata backend");
    owner_dfs.add_file_system(file_system).expect("owner fs");

    let mut parent = file_metadata(owner.clone());
    parent.object_id = parent_object_id.clone();
    parent.path = "/services".to_string();
    parent.kind = DfsObjectKind::Directory;
    parent.version = parent_version;
    parent.content_hash = None;
    parent.content_size_bytes = None;
    parent.modified_at_unix_nanos = 10;
    parent.changed_at_unix_nanos = 20;
    owner_dfs
        .add_object_metadata(parent.clone())
        .expect("durable parent directory");

    let mut child = file_metadata(owner.clone());
    child.object_id = object_id.clone();
    child.path = format!("/services/{case}.txt");
    child.version = version_1.clone();
    child.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_transient_object_metadata(child)
        .expect("transient child");

    let write = owner_dfs
        .write_distributed_bytes_with_base(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: requester.clone(),
            },
            Some(version_1),
            bytes.clone(),
            vec![owner.clone(), requester.clone(), replica.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: None,
            },
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let persisted_intent = owner_dfs
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &intent_id)
        .expect("intent lookup")
        .expect("intent");
    assert_eq!(
        persisted_intent.touch_parent_on_create, touch_parent_on_create,
        "the durable intent must preserve the caller's create semantics"
    );
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica,
            content_hash: Some(content_hash(&bytes)),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("persist final receipt");
    assert!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object lookup")
            .is_none(),
        "test setup must leave the child invisible before restart"
    );
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(recovered_backend)
    .expect("commit-ready create should recover");
    assert_eq!(
        recovered
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("child lookup")
            .expect("recovered child")
            .version,
        version_2
    );
    let after_recovery = recovered
        .metadata_backend()
        .expect("backend")
        .get_object(&parent_object_id)
        .expect("parent lookup")
        .expect("parent");
    drop(recovered);

    let reopened_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("reopened backend");
    let reopened = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("reopened content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(reopened_backend)
    .expect("committed create should reopen");
    let after_second_restart = reopened
        .metadata_backend()
        .expect("backend")
        .get_object(&parent_object_id)
        .expect("parent lookup")
        .expect("parent");
    drop(reopened);
    std::fs::remove_dir_all(root).ok();
    (parent, after_recovery, after_second_restart)
}

fn assert_malformed_remote_range_rejected(
    name: &str,
    mutate: impl FnOnce(&mut DfsReadRangeData),
    expected_reason: &str,
) {
    let root = temp_root(name);
    let reader = NodeId::from_legacy_named(format!("{name}_reader")).expect("reader node");
    let source = NodeId::from_legacy_named(format!("{name}_source")).expect("source node");
    let object_id = DfsObjectId::from_legacy_named(format!("{name}_object")).expect("object");
    let version = DfsVersion::from_legacy_named(format!("{name}_version")).expect("version");
    let bytes = b"authoritative ranged response bytes".to_vec();
    let content_hash = content_hash(&bytes);
    let offset = 3_u64;
    let max_bytes = 7_usize;
    let request_id = format!("{name}_request");
    let mut dfs = networked_sifs_with_backend(&root, reader);
    let mut object = file_metadata(source.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    object.content_hash = Some(content_hash.clone());
    object.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(object).expect("object metadata");
    dfs.reads.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(content_hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            range_offset: Some(offset),
            range_max_bytes: Some(max_bytes),
            target_node_id: source.clone(),
            next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
            cache_response: false,
        },
    );
    let start = offset as usize;
    let mut data = DfsReadRangeData {
        object_id: object_id.clone(),
        version,
        content_hash: Some(content_hash),
        offset,
        total_size_bytes: bytes.len() as u64,
        bytes: bytes[start..start + max_bytes].to_vec(),
    };
    mutate(&mut data);

    let event = dfs
        .handle_networked_read_response(
            source.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::RangeFound(data),
            },
        )
        .expect("malformed range response handled");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            result: DfsReadResult::Rejected { reason },
            ..
        } if completed_request_id == request_id && reason.contains(expected_reason)
    ));
    assert!(!dfs.reads.pending_reads.contains_key(&request_id));
    assert!(
        !dfs.current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations
            .contains(&source),
        "source returning malformed range metadata must be pruned"
    );
}

// ---------------------------------------------------------------------------
// SIFS POSIX cluster correctness tests
//
// These tests run a real in-process SIFS cluster: one metadata-shard primary
// plus two replica nodes sharing one `InProcessNetwork` and one KV-backed
// control plane. They verify that the POSIX facade keeps its contract when
// operations route across nodes: read-your-write after routed writes,
// primary visibility, replica catch-up through journal publication, write
// serialization with explicit conflicts, sparse write-at/truncate semantics,
// namespace replication (rename/unlink tombstones), restart durability, and
// errno-style error mapping.
// ---------------------------------------------------------------------------

const SIFS_CLUSTER_PUMP_DEADLINE: Duration = Duration::from_secs(10);
const SIFS_CLUSTER_PUMP_IDLE_SLEEP: Duration = Duration::from_millis(2);

struct SifsPosixCluster {
    root: sinomo_testdir::TestDir,
    primary_node: NodeId,
    writer_node: NodeId,
    observer_node: NodeId,
    primary: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    writer: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    observer: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    rejections: Vec<String>,
}

fn sifs_posix_cluster_node(
    network: &InProcessNetwork,
    shared_kv: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
    root: &Path,
    name: &str,
    node_id: NodeId,
) -> NetworkedDfsService<sinomo_core::network::InProcessTransport> {
    let node_root = root.join(name);
    NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(node_root.join("content")).expect("cluster node content store"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend(
        open_root_local_metadata_backend(node_root.join("metadata"))
            .expect("cluster node metadata backend"),
    )
    .expect("cluster node metadata backend attach")
}

fn sifs_posix_cluster(name: &str) -> SifsPosixCluster {
    let root = temp_root(name);
    let node_prefix = name.replace('-', "_");
    let primary_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_primary")).expect("primary node");
    let writer_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_writer")).expect("writer node");
    let observer_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_observer")).expect("observer node");

    let network = InProcessNetwork::new();
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary_node.clone(),
            )))
            .expect("sifs filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: primary_node.clone(),
                replica_node_ids: vec![writer_node.clone(), observer_node.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("sifs root metadata shard config");
    });

    let mut primary =
        sifs_posix_cluster_node(&network, &shared_kv, &root, "primary", primary_node.clone());
    let writer =
        sifs_posix_cluster_node(&network, &shared_kv, &root, "writer", writer_node.clone());
    let observer = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "observer",
        observer_node.clone(),
    );

    // The shard primary is the namespace authority. Clustered bootstrap is a
    // normal DFS metadata publication, so the configured replicas must process
    // and acknowledge its exact prepared prefix before it becomes visible.
    let (writer, observer) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let writer_stop = Arc::clone(&stop);
        let observer_stop = Arc::clone(&stop);
        let writer = scope.spawn(move || pump_sifs_service_until_stop(writer, writer_stop));
        let observer = scope.spawn(move || pump_sifs_service_until_stop(observer, observer_stop));
        let bootstrap = primary.bootstrap_distributed_sifs();
        stop.store(true, Ordering::SeqCst);
        let writer = writer.join().expect("writer bootstrap pump");
        let observer = observer.join().expect("observer bootstrap pump");
        bootstrap.expect("primary SIFS bootstrap");
        (writer, observer)
    });

    let mut cluster = SifsPosixCluster {
        root,
        primary_node,
        writer_node,
        observer_node,
        primary,
        writer,
        observer,
        rejections: Vec::new(),
    };
    sifs_cluster_publish_metadata_until(&mut cluster, "initial follower catch-up", |cluster| {
        [SIFS_USERS_PATH, SIFS_SERVICES_PATH]
            .into_iter()
            .all(|path| {
                cluster.writer.posix_stat(path).is_ok() && cluster.observer.posix_stat(path).is_ok()
            })
    });
    // Follower-side bootstrap after catch-up must be a no-op: every namespace
    // anchor already exists from journal replay, so it must not append local
    // journal entries that would fork the follower from the primary's stream.
    cluster
        .writer
        .bootstrap_distributed_sifs()
        .expect("writer follower bootstrap");
    cluster
        .observer
        .bootstrap_distributed_sifs()
        .expect("observer follower bootstrap");
    assert!(
        cluster.rejections.is_empty(),
        "initial follower catch-up must not reject journal batches: {:?}",
        cluster.rejections
    );
    cluster
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
    reader: &mut NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    path: &str,
    what: &str,
    mut pump: impl FnMut(),
) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    let distributed_read_path = path.to_string();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let result = reader.posix_read_file_distributed(&distributed_read_path);
            tx.send(result).expect("send distributed read result");
        });
        let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
        loop {
            pump();
            match rx.try_recv() {
                Ok(result) => return result.unwrap_or_else(|error| panic!("{what}: {error}")),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("distributed read worker disconnected while waiting for {what}")
                }
            }
            if Instant::now() >= deadline {
                panic!("blocking distributed read did not complete: {what}");
            }
            std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        }
    })
}

fn sifs_cluster_writer_call_with_pump<R: Send>(
    cluster: &mut SifsPosixCluster,
    what: &str,
    call: impl FnOnce(&mut NetworkedDfsService<sinomo_core::network::InProcessTransport>) -> R + Send,
) -> R {
    let (tx, rx) = std::sync::mpsc::channel();
    let primary = &mut cluster.primary;
    let writer = &mut cluster.writer;
    let observer = &mut cluster.observer;
    let rejections = &mut cluster.rejections;
    std::thread::scope(|scope| {
        scope.spawn(move || {
            tx.send(call(writer)).expect("send writer call result");
        });
        let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
        loop {
            for (label, service) in [("primary", &mut *primary), ("observer", &mut *observer)] {
                for event in service
                    .process_incoming()
                    .expect("cluster process_incoming")
                {
                    if let NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                        reason, ..
                    } = event
                    {
                        rejections.push(format!("{label}: {reason}"));
                    }
                }
            }
            match rx.try_recv() {
                Ok(result) => return result,
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("writer call disconnected while waiting for {what}")
                }
            }
            if Instant::now() >= deadline {
                panic!(
                    "writer call did not complete: {what}; journal rejections: {:?}",
                    rejections
                );
            }
            std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        }
    })
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
    sifs_posix_cluster_pump_until(cluster, path, |cluster| {
        sifs_routed_writes_settled(&cluster.writer)
            && cluster
                .writer
                .posix_stat(path)
                .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
}

/// Writes on the primary node, which commits immediately through the local
/// fast path because the primary coordinates its own shard.
fn sifs_cluster_primary_posix_write(cluster: &mut SifsPosixCluster, path: &str, bytes: &[u8]) {
    let options = sifs_cluster_write_options(&cluster.primary_node.clone());
    let stat = cluster
        .primary
        .posix_write_file(path, bytes, options)
        .expect("primary POSIX write");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(bytes).as_str()),
        "primary POSIX write to {path} must commit synchronously"
    );
}

/// Publishes the primary's metadata journal to the configured replicas and
/// pumps the cluster until `done` observes the replicated state.
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
            .primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("primary metadata journal publish");
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
    node: fn(
        &mut SifsPosixCluster,
    ) -> &mut NetworkedDfsService<sinomo_core::network::InProcessTransport>,
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

fn sifs_routed_writes_settled(
    service: &NetworkedDfsService<sinomo_core::network::InProcessTransport>,
) -> bool {
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

fn pump_dfs_services_collect_rejections(
    services: &mut [(
        &str,
        &mut NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    )],
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
    root: sinomo_testdir::TestDir,
    users_primary_node: NodeId,
    services_primary_node: NodeId,
    replica_node: NodeId,
    shared_kv: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    users_primary: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    services_primary: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    replica: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    rejections: Vec<String>,
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
            namespace_prepare_codec: Default::default(),
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
            namespace_prepare_codec: Default::default(),
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
            namespace_prepare_codec: Default::default(),
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
        cluster
            .users_primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("users primary publish");
        cluster
            .services_primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("services primary publish");
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

fn pump_sifs_service_until_stop(
    mut service: NetworkedDfsService<sinomo_core::network::InProcessTransport>,
    stop: Arc<AtomicBool>,
) -> NetworkedDfsService<sinomo_core::network::InProcessTransport> {
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
    sifs_two_primary_cluster_with_codec(name, DfsMetadataNamespacePrepareCodec::Legacy)
}

fn sifs_two_primary_cluster_with_codec(
    name: &str,
    codec: DfsMetadataNamespacePrepareCodec,
) -> SifsTwoPrimaryCluster {
    let root = temp_root(name);
    let node_prefix = name.replace('-', "_");
    let users_primary_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_users")).expect("users primary");
    let services_primary_node = NodeId::from_legacy_named(format!("node_{node_prefix}_services"))
        .expect("services primary");
    let replica_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_replica")).expect("replica");

    let network = InProcessNetwork::new();
    let mut initial_kv = InProcessKvStore::new();
    if codec.required_capability().is_some() {
        let cluster_id = sinomo_core::Id::from_legacy_named(name).unwrap();
        for node_id in [&users_primary_node, &services_primary_node, &replica_node] {
            let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                node_id.clone(),
                "metadata codec member",
                format!("{node_id}-key"),
                time::OffsetDateTime::UNIX_EPOCH,
            );
            member.state = sinomo_core::ClusterMemberState::Active;
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            initial_kv
                .put_serde(
                    sinomo_core::node::cluster_member_key(node_id),
                    &member,
                    None,
                )
                .unwrap();
            initial_kv
                .put_serde(
                    sinomo_core::node::cluster_capability_policy_key(node_id),
                    &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
                    None,
                )
                .unwrap();
        }
    }
    let shared_kv = SharedDfsControlPlaneKvClient::new(initial_kv);
    // Genesis: one node is primary of every shard and materializes SIFS.
    let mut genesis_shards = sifs_two_primary_shard_configs(
        &users_primary_node,
        &users_primary_node,
        1,
        vec![services_primary_node.clone(), replica_node.clone()],
        vec![services_primary_node.clone(), replica_node.clone()],
    );
    for shard in &mut genesis_shards {
        shard.namespace_prepare_codec = codec;
    }
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                users_primary_node.clone(),
            )))
            .expect("sifs filesystem config");
        for shard in &genesis_shards {
            control.put_metadata_shard(shard).expect("genesis shard");
        }
    });

    let node = |node_id: &NodeId, dir: &str| {
        let node_root = root.join(dir);
        NetworkedDfsService::new(
            network.add_node(node_id.clone()),
            LocalContentStore::new(node_root.join("content")).expect("node content"),
        )
        .with_control_plane(shared_kv.clone())
        .with_metadata_backend(
            open_local_sharded_metadata_backend(node_root.join("metadata"), genesis_shards.clone())
                .expect("node sharded metadata"),
        )
        .expect("node backend attach")
    };
    let mut users_primary = node(&users_primary_node, "users-primary");
    let services_primary = node(&services_primary_node, "services-primary");
    let replica = node(&replica_node, "replica");

    let (services_primary, replica) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let services_stop = Arc::clone(&stop);
        let replica_stop = Arc::clone(&stop);
        let services_primary =
            scope.spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
        let replica = scope.spawn(move || pump_sifs_service_until_stop(replica, replica_stop));
        let bootstrap = users_primary.bootstrap_distributed_sifs();
        stop.store(true, Ordering::SeqCst);
        let services_primary = services_primary.join().expect("services bootstrap pump");
        let replica = replica.join().expect("replica bootstrap pump");
        bootstrap.expect("genesis SIFS bootstrap");
        (services_primary, replica)
    });

    let mut cluster = SifsTwoPrimaryCluster {
        root,
        users_primary_node: users_primary_node.clone(),
        services_primary_node: services_primary_node.clone(),
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    sifs_two_primary_publish_until(&mut cluster, "genesis follower catch-up", |cluster| {
        cluster
            .services_primary
            .posix_stat(SIFS_SERVICES_PATH)
            .is_ok()
            && cluster.services_primary.posix_stat(SIFS_USERS_PATH).is_ok()
            && cluster.replica.posix_stat(SIFS_SERVICES_PATH).is_ok()
            && cluster.replica.posix_stat(SIFS_USERS_PATH).is_ok()
    });
    cluster
        .services_primary
        .bootstrap_distributed_sifs()
        .expect("services follower bootstrap");
    cluster
        .replica
        .bootstrap_distributed_sifs()
        .expect("replica follower bootstrap");

    // Shard transfer: the services shard moves to its own primary, exactly
    // like a production primary transfer after the candidate replayed the
    // journal.
    let mut transferred = sifs_two_primary_shard_configs(
        &users_primary_node,
        &services_primary_node,
        2,
        vec![services_primary_node.clone(), cluster.replica_node.clone()],
        vec![users_primary_node.clone(), cluster.replica_node.clone()],
    );
    for (retiring, desired) in genesis_shards.iter().zip(&mut transferred) {
        desired.namespace_prepare_codec = codec;
        if codec != DfsMetadataNamespacePrepareCodec::Legacy
            && desired.primary_node_id != retiring.primary_node_id
        {
            desired.replica_node_ids =
                crate::control_plane::topology_preserving_metadata_shard_replicas(
                    retiring,
                    &desired.primary_node_id,
                )
                .expect("preserve the exact member ring");
        }
    }
    cluster.shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for (retiring, desired) in genesis_shards.iter().zip(&transferred) {
            if codec == DfsMetadataNamespacePrepareCodec::Legacy {
                control.put_metadata_shard(desired).expect("transfer shard");
            } else if retiring != desired {
                control
                    .begin_metadata_shard_membership_handoff(retiring, desired)
                    .expect("begin capability-validated transfer")
                    .expect("exact retiring assignment");
            }
        }
    });
    if codec != DfsMetadataNamespacePrepareCodec::Legacy {
        let stop = Arc::new(AtomicBool::new(false));
        let services_stop = stop.clone();
        let services = std::thread::spawn(move || {
            pump_sifs_service_until_stop(cluster.services_primary, services_stop)
        });
        let replica_stop = stop.clone();
        let replica =
            std::thread::spawn(move || pump_sifs_service_until_stop(cluster.replica, replica_stop));
        let result = cluster
            .users_primary
            .reconcile_pending_metadata_shard_membership_handoffs(
                &genesis_shards,
                Duration::from_secs(5),
            );
        stop.store(true, Ordering::SeqCst);
        cluster.services_primary = services.join().unwrap();
        cluster.replica = replica.join().unwrap();
        assert_eq!(
            result.expect("fence, replicate, and activate the transfer"),
            1
        );
        for service in [
            &mut cluster.users_primary,
            &mut cluster.services_primary,
            &mut cluster.replica,
        ] {
            service
                .rebind_completed_sifs_metadata_shard_membership_handoffs()
                .expect("bind completed authority generation");
        }
    }
    assert!(
        cluster.rejections.is_empty(),
        "two-primary genesis must not reject journal batches: {:?}",
        cluster.rejections
    );
    cluster
}

// ---------------------------------------------------------------------------
// Regression tests for the cluster-correctness review fixes.
// ---------------------------------------------------------------------------

/// A 3-node SIFS cluster where `observer` is NOT in the root shard's replica
/// set: the primary is the namespace authority, `writer` receives the pushed
/// journal stream, and the outsider observer only converges its local mirror
/// through routed-response batches and SyncPath pulls.
fn sifs_outsider_cluster(name: &str) -> SifsPosixCluster {
    let root = temp_root(name);
    let node_prefix = name.replace('-', "_");
    let primary_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_primary")).expect("primary node");
    let writer_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_writer")).expect("writer node");
    let observer_node =
        NodeId::from_legacy_named(format!("node_{node_prefix}_outsider")).expect("outsider node");

    let network = InProcessNetwork::new();
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary_node.clone(),
            )))
            .expect("sifs filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: primary_node.clone(),
                replica_node_ids: vec![writer_node.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("sifs root metadata shard config");
    });

    let mut primary =
        sifs_posix_cluster_node(&network, &shared_kv, &root, "primary", primary_node.clone());
    let writer =
        sifs_posix_cluster_node(&network, &shared_kv, &root, "writer", writer_node.clone());
    let observer = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "outsider",
        observer_node.clone(),
    );

    primary
        .bootstrap_distributed_sifs()
        .expect("primary SIFS bootstrap");

    let mut cluster = SifsPosixCluster {
        root,
        primary_node,
        writer_node,
        observer_node,
        primary,
        writer,
        observer,
        rejections: Vec::new(),
    };
    // Only the configured replica is fed by journal publication; the outsider
    // intentionally stays empty until it pulls.
    sifs_cluster_publish_metadata_until(&mut cluster, "replica catch-up", |cluster| {
        cluster.writer.posix_stat(SIFS_USERS_PATH).is_ok()
    });
    assert!(
        cluster.rejections.is_empty(),
        "replica catch-up must not reject journal batches: {:?}",
        cluster.rejections
    );
    assert!(
        cluster.observer.posix_stat(SIFS_USERS_PATH).is_err(),
        "outsider must not have been fed by the replica stream"
    );
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
