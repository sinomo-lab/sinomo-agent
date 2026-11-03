//! Reads regression tests.

use super::*;

#[tokio::test]
async fn direct_read_rpc_keeps_blob_in_the_session_attachment() {
    let root = temp_root("direct-read-rpc-attachment");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let object_id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let bytes = (0..(2 * 1024 * 1024 + 16384))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content");
    let manifest = content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("put object version");
    let direct_read = DfsDirectReadRequest {
        request_id: "direct-read-stream-request".to_string(),
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: manifest.content_hash.clone(),
        content_size_bytes: bytes.len() as u64,
        range_offset: None,
        range_max_bytes: None,
    };
    let direct_request = dfs_direct_read_rpc_request(&direct_read).expect("direct request");
    assert!(direct_request.is_response_retry_safe());

    let attachment_response =
        handle_dfs_direct_read_rpc_request(&data_dir, direct_request).expect("attachment");
    assert_eq!(attachment_response.response.route, DFS_DIRECT_READ_ROUTE);
    assert_eq!(
        attachment_response
            .response
            .metadata
            .get(DFS_DIRECT_READ_STATUS_METADATA_KEY)
            .map(String::as_str),
        Some(DFS_DIRECT_READ_STATUS_FOUND)
    );
    assert!(attachment_response.response.body.is_empty());
    let received = attachment_response
        .attachment
        .collect_to_vec(bytes.len())
        .await
        .expect("read attachment");
    assert_eq!(received, bytes);
    let decoded =
        dfs_direct_read_response_from_rpc_result(sinomo_core::network::ObjectRpcResult::new(
            attachment_response.response,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(received),
        ))
        .expect("decode")
        .collect_to_read_response()
        .await
        .expect("collect direct read");
    let DfsReadResult::Found(data) = decoded.result else {
        panic!("expected found response");
    };
    assert_eq!(data.object_id, object_id);
    assert_eq!(data.version, version);
    assert_eq!(data.bytes, bytes);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn direct_read_stream_checks_length_without_rehashing_bytes() {
    let object_id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let expected = (0..(512 * 1024))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_length = expected.len();
    let mut corrupted = expected.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0x5a;
    let response = DfsReadResponse {
        request_id: "direct-read-incremental-finalizer".to_string(),
        object_id: object_id.clone(),
        result: DfsReadResult::Found(DfsReadData {
            object_id,
            version,
            content_hash: Some(content_hash(&expected)),
            bytes: expected,
        }),
    };
    let rpc_result = dfs_direct_read_rpc_result(response).expect("direct-read response head");
    let (head, _) = rpc_result.into_parts();
    let streamed =
        dfs_direct_read_response_from_rpc_result(sinomo_core::network::ObjectRpcResult::new(
            head,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(corrupted),
        ))
        .expect("stream response head is valid");
    let DfsDirectReadClientResult::Found(mut data) = streamed.result else {
        panic!("expected found stream");
    };

    // Returning the response head and first chunk must not wait for a
    // whole-body allocation or a digest over a contiguous Vec.
    let first = data
        .stream
        .next_chunk()
        .await
        .expect("first stream chunk")
        .expect("non-empty stream");
    assert!(!first.is_empty());
    assert!(data.stream.completion().is_none());
    let completion = data
        .stream
        .finish()
        .await
        .expect("same-size bytes are not rehashed");
    assert_eq!(completion.content_size_bytes, expected_length);
}

#[tokio::test]
async fn direct_range_read_uses_the_same_stream_and_compatibility_collector() {
    let root = temp_root("direct-range-stream");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let object_id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let bytes = (0..(768 * 1024))
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content");
    let manifest = content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("put object version");
    let request = DfsDirectReadRequest {
        request_id: "direct-range-stream".to_string(),
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: manifest.content_hash.clone(),
        content_size_bytes: bytes.len() as u64,
        range_offset: Some(111_111),
        range_max_bytes: Some(300_000),
    };
    let attachment_response = handle_dfs_direct_read_rpc_request(
        &data_dir,
        dfs_direct_read_rpc_request(&request).expect("range request"),
    )
    .expect("range attachment");
    let received = attachment_response
        .attachment
        .collect_to_vec(300_000)
        .await
        .expect("collect test transport attachment");
    let streamed =
        dfs_direct_read_response_from_rpc_result(sinomo_core::network::ObjectRpcResult::new(
            attachment_response.response,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(received),
        ))
        .expect("decode range stream");
    let compatible = streamed
        .collect_to_read_response()
        .await
        .expect("explicit compatibility collection");
    let DfsReadResult::RangeFound(data) = compatible.result else {
        panic!("expected range result");
    };
    assert_eq!(data.object_id, object_id);
    assert_eq!(data.version, version);
    assert_eq!(data.offset, 111_111);
    assert_eq!(data.total_size_bytes, bytes.len() as u64);
    assert_eq!(data.bytes, bytes[111_111..411_111]);
}

#[test]
fn stream_native_read_is_bounded_for_direct_and_relayed_results() {
    const CHUNK_BYTES: usize = 32 * 1024;
    const HEAD_TIMEOUT: Duration = Duration::from_millis(100);
    const FIRST_CHUNK_TIMEOUT: Duration = Duration::from_millis(100);
    for relay_hops in [0_usize, 1] {
        let expected = (0..3 * CHUNK_BYTES)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let name = format!("native-bounded-stream-{relay_hops}");
        let (runtime, owner, _, reader_node, stat, expected) =
            native_read_fixture(&name, 1, false, expected);
        let mut reader = runtime.client(&reader_node).unwrap();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let forwarded = Arc::new(AtomicUsize::new(0));
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(reader_node.clone()),
            target: Some(owner.clone()),
            route: Some(DFS_DIRECT_READ_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: crate::test_runtime::FaultAction::PauseNativeReadBody {
                gate: Arc::clone(&gate),
                started: Arc::clone(&started),
                forwarded_bytes: Arc::clone(&forwarded),
                relay_hops,
            },
        });
        let request = DfsDirectReadRequest {
            request_id: reader.next_request_id(),
            object_id: stat.object_id.clone(),
            version: stat.version.clone(),
            content_hash: stat.content_hash.clone().unwrap(),
            content_size_bytes: expected.len() as u64,
            range_offset: None,
            range_max_bytes: None,
        };
        crate::test_runtime::executor().block_on(async {
            let response = tokio::time::timeout(
                HEAD_TIMEOUT,
                reader.transport.read_object_stream(&owner, &request),
            )
            .await
            .expect("native head arrives before its actual body source starts")
            .unwrap();
            assert_eq!(response.request_id, request.request_id);
            assert_eq!(response.object_id, stat.object_id);
            let DfsDirectReadClientResult::Found(mut data) = response.result else {
                panic!("actual native Found head required")
            };
            assert_eq!(data.stream.observed_size_bytes(), 0);
            assert!(
                !started.load(Ordering::Acquire),
                "the original native source has not produced any bytes"
            );
            assert_eq!(forwarded.load(Ordering::Acquire), 0);
            gate.add_permits(1);
            tokio::time::timeout(FIRST_CHUNK_TIMEOUT, async {
                while forwarded.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("first actual body chunk reaches the bounded native ownership ring");
            let produced = forwarded.load(Ordering::Acquire);
            assert!(produced > 0 && produced <= CHUNK_BYTES);
            tokio::task::yield_now().await;
            assert_eq!(
                forwarded.load(Ordering::Acquire),
                produced,
                "the paused producer cannot collect or forward the remaining body"
            );
            let first_started = Instant::now();
            let first = tokio::time::timeout(FIRST_CHUNK_TIMEOUT, data.stream.next_chunk())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(first_started.elapsed() < FIRST_CHUNK_TIMEOUT);
            assert!(first.len() <= CHUNK_BYTES);
            assert_eq!(first.as_ref(), &expected[..first.len()]);
            assert!(data.stream.completion().is_none());
            gate.add_permits(32);
            let mut actual = first.as_ref().to_vec();
            while let Some(chunk) = data.stream.next_chunk().await.unwrap() {
                actual.extend_from_slice(chunk.as_ref());
            }
            assert_eq!(actual, expected);
            assert_eq!(
                data.stream.finish().await.unwrap().content_size_bytes,
                expected.len()
            );
        });
        assert_eq!(forwarded.load(Ordering::Acquire), expected.len());
        drop(reader);
        runtime.clear_faults();
        runtime.close().unwrap();
    }
}

#[test]
fn node_transport_read_message_uses_direct_sender_and_queues_response() {
    let (runtime, owner, _, reader_node, stat, bytes) =
        native_read_fixture("native-transport-direct-sync", 1, false, vec![0x54; 4096]);
    let mut reader = runtime.client(&reader_node).unwrap();
    let request_id = reader.next_request_id();
    let message = reader.next_dfs_routed_message(
        &owner,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: request_id.clone(),
            object_id: stat.object_id.clone(),
            version: Some(stat.version.clone()),
            content_hash: stat.content_hash.clone(),
            content_size_bytes: Some(bytes.len() as u64),
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    let before = runtime.rpc_observations().len();
    reader
        .transport
        .send_routed_call(&owner, DfsRoutedCall::value(message))
        .unwrap();
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert_eq!(
        calls
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == owner
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        1
    );
    assert!(!calls.iter().any(|event| event.source == reader_node
        && event.route == crate::DFS_DATA_READ_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch));
    assert_eq!(reader.transport.pending_incoming_count().unwrap(), Some(1));
    let messages = reader.transport.drain_incoming_messages().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].source_node_id, owner);
    assert_eq!(messages[0].target_node_id, reader_node);
    assert!(
        matches!(&messages[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.request_id == request_id && response.object_id == stat.object_id
            && matches!(&response.result, DfsReadResult::Found(data)
                if data.object_id == stat.object_id && data.version == stat.version
                    && data.content_hash == stat.content_hash && data.bytes == bytes)),
        "the current normal adapter queues the actual native receiver's exact read tuple and body"
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn node_transport_async_read_message_uses_direct_sender_and_queues_response() {
    let (runtime, owner, _, reader_node, stat, bytes) =
        native_read_fixture("native-transport-direct-async", 1, false, vec![0x54; 4096]);
    let mut reader = runtime.client(&reader_node).unwrap();
    let request_id = reader.next_request_id();
    let message = reader.next_dfs_routed_message(
        &owner,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: request_id.clone(),
            object_id: stat.object_id.clone(),
            version: Some(stat.version.clone()),
            content_hash: stat.content_hash.clone(),
            content_size_bytes: Some(bytes.len() as u64),
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    let before = runtime.rpc_observations().len();
    crate::test_runtime::executor().block_on(async {
        AsyncDfsMessageTransport::send_routed_call_async(
            &mut reader.transport,
            &owner,
            DfsRoutedCall::value(message),
        )
        .await
        .unwrap();
    });
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert_eq!(
        calls
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == owner
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        1
    );
    assert!(!calls.iter().any(|event| event.source == reader_node
        && event.route == crate::DFS_DATA_READ_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch));
    let messages = crate::test_runtime::executor().block_on(async {
        AsyncDfsMessageTransport::drain_incoming_messages_async(&mut reader.transport)
            .await
            .unwrap()
    });
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].source_node_id, owner);
    assert_eq!(messages[0].target_node_id, reader_node);
    assert!(
        matches!(&messages[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.request_id == request_id && response.object_id == stat.object_id
            && matches!(&response.result, DfsReadResult::Found(data)
                if data.object_id == stat.object_id && data.version == stat.version
                    && data.content_hash == stat.content_hash && data.bytes == bytes)),
        "the current normal adapter queues the actual native receiver's exact read tuple and body"
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn read_only_backend_attachment_skips_bootstrap_scans_and_serves_targeted_reads() {
    let node =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-read-only-attachment", 1)
            .remove(0);
    let (mut runtime, mut writer) =
        native_sifs_service("native-read-only-attachment", node.clone());
    let path = "/services/read-only-attachment.txt";
    let stat = writer
        .posix_write_file(
            path,
            b"native read-only bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let expected = runtime
        .node_metadata(&node)
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let configs = runtime.metadata_shards().unwrap();
    let shard = select_metadata_shard_config_for_path(&stat.file_system_id, &configs, path)
        .unwrap()
        .unwrap();
    drop(writer);
    runtime.stop_node(&node).unwrap();
    let backend = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(shard.shard_id.clone()),
        default_dfs_metadata_shards_root(runtime.data_dir(&node)).join(&shard.shard_id),
    )
    .unwrap();
    let content =
        LocalContentStore::open_for_inspection(runtime.data_dir(&node).dfs_dir().join("content"));
    let mut service = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
        content,
    )
    .with_boxed_metadata_backend_for_read_only_requests(Box::new(backend));
    assert!(service.metadata.objects.is_empty());
    assert!(service.cross_shard_namespace_locks.is_empty());
    assert!(
        service
            .replication
            .local_metadata_journal_cursors
            .is_empty()
    );
    let actual = service
        .authoritative_metadata_object_for_path(&stat.file_system_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        service
            .current_read_object_metadata(&stat.object_id)
            .unwrap()
            .version,
        stat.version
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn mkdir_all_existing_deep_chain_uses_linear_metadata_lookups() {
    let node = NodeId::from_legacy_named("node_mkdir_all_linear_lookups").expect("node id");
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "mkdir_all_existing_deep_chain_uses_linear_metadata_lookups",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let mut path = "/services".to_string();
    for depth in 0..32 {
        path.push_str(&format!("/level-{depth:02}"));
    }
    dfs.posix_mkdir_all(&path).expect("create deep chain");

    crate::posix::reset_posix_metadata_raw_lookup_count();
    dfs.posix_mkdir_all(&path)
        .expect("repeat existing deep chain");
    let lookup_count = crate::posix::posix_metadata_raw_lookup_count();
    let component_count = path.trim_matches('/').split('/').count();
    let linear_lookup_bound = component_count + 1;
    assert!(
        lookup_count <= linear_lookup_bound,
        "deep-path resolution performed {lookup_count} raw lookups for {component_count} components; \
         one root ancestor check plus one exact lookup per component is the linear upper bound"
    );
}

#[test]
fn no_link_beneath_posix_primitives_confine_reads_lists_and_mutations() {
    let node = NodeId::from_legacy_named("node_no_link_beneath").expect("node id");
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "no_link_beneath_posix_primitives_confine_reads_lists_and_mutations",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");
    let options = DfsWriteOptions::replicated_eventual(vec![node]);

    dfs.posix_mkdir_all_beneath_no_links("/scope", "/scope/notes/nested")
        .expect("create missing scoped root and directory chain");
    let created = dfs
        .posix_create_file_beneath_no_links(
            "/scope",
            "/scope/notes/note.md",
            b"inside note",
            options.clone(),
        )
        .expect("guarded exclusive create");
    let (opened, prefix) = dfs
        .posix_read_file_prefix_beneath_no_links_distributed("/scope", "/scope/notes/note.md", 6)
        .expect("guarded exact prefix read");
    assert_eq!(opened.object_id, created.object_id);
    assert_eq!(opened.version, created.version);
    assert_eq!(prefix.bytes, b"inside");
    assert!(prefix.truncated);
    let (range_opened, range) = dfs
        .posix_read_file_range_beneath_no_links_distributed("/scope", "/scope/notes/note.md", 7, 64)
        .expect("guarded exact tail read");
    assert_eq!(range_opened.object_id, created.object_id);
    assert_eq!(range_opened.version, created.version);
    assert_eq!(range.bytes, b"note");
    assert!(!range.truncated);
    assert!(matches!(
        dfs.posix_read_file_range_beneath_no_links_distributed(
            "/scope",
            "/scope/notes/note.md",
            12,
            64,
        ),
        Err(DfsPosixError::InvalidPath { .. })
    ));

    let (directory, page) = dfs
        .posix_list_dir_beneath_no_links_distributed_page("/scope", "/scope/notes", None, 16)
        .expect("guarded directory page");
    assert_eq!(directory.kind, DfsObjectKind::Directory);
    assert!(page.entries.iter().any(|entry| {
        entry.name == "note.md"
            && entry.object_id == created.object_id
            && entry.version == created.version
    }));

    dfs.posix_mkdir_all("/outside").expect("create outside");
    dfs.posix_create_file("/outside/secret.md", b"outside", options.clone())
        .expect("create outside secret");
    dfs.posix_symlink("/outside", "/scope/escape", options.clone())
        .expect("create escaping link");
    assert!(matches!(
        dfs.posix_read_file_prefix_beneath_no_links_distributed(
            "/scope",
            "/scope/escape/secret.md",
            64,
        ),
        Err(DfsPosixError::InvalidPath { .. })
    ));
    assert!(matches!(
        dfs.posix_list_dir_beneath_no_links_distributed_page("/scope", "/scope/escape", None, 16,),
        Err(DfsPosixError::InvalidPath { .. })
    ));
    assert!(matches!(
        dfs.posix_create_file_beneath_no_links(
            "/scope",
            "/scope/escape/new.md",
            b"must not escape",
            options,
        ),
        Err(DfsPosixError::InvalidPath { .. })
    ));
    assert!(matches!(
        dfs.posix_namespace_lstat_distributed("/outside/new.md"),
        Err(DfsPosixError::NotFound { .. })
    ));

    let operation = DfsNamespaceOperation::MkdirAtNoLinks {
        path: "/scope/notes/routed".to_string(),
        expected_parent_object_id: directory.object_id,
        expected_parent_version: directory.version,
    };
    assert_eq!(operation.routing_path(), Some("/scope/notes/routed"));
    assert_eq!(operation.involved_paths(), vec!["/scope/notes/routed"]);
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let encoded = codec.encode(&operation).expect("encode no-link mkdir");
    assert_eq!(
        codec
            .decode::<DfsNamespaceOperation>(&encoded)
            .expect("decode no-link mkdir"),
        operation
    );
}

#[test]
fn local_primary_lightweight_reads_pin_one_metadata_generation() {
    let node_id =
        NodeId::from_legacy_named("node_local_primary_lightweight_snapshot").expect("node");
    let directory = "/lightweight-read-snapshot/deep";
    let path = "/lightweight-read-snapshot/deep/tiny.txt";
    let payload = b"resident metadata snapshot";
    let (runtime, client) = native_sifs_service(
        "local_primary_lightweight_reads_pin_one_metadata_generation",
        node_id.clone(),
    );
    drop(client);
    // This probe measures the actual current primary's retained generation,
    // rather than a public requester shell which delegates its writes to Main.
    let mut dfs = runtime
        .metadata_primary_service(path)
        .expect("current primary");
    dfs.posix_mkdir_all(directory).expect("fixture directory");
    dfs.posix_write_file(
        path,
        payload,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("fixture file");

    let range_prefix_calls = Arc::new(AtomicUsize::new(0));
    let actual = dfs.control_plane.take().expect("same native client");
    dfs = dfs.with_control_plane(CountingDfsControlPlaneKvClient::new(
        actual,
        Arc::clone(&range_prefix_calls),
    ));

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    range_prefix_calls.store(0, Ordering::SeqCst);
    let stat = dfs.posix_stat_distributed(path).expect("distributed stat");
    assert_eq!(stat.size_bytes, Some(payload.len() as u64));
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "one local-primary stat must not probe durable table files per path component"
    );
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "one local-primary stat must reuse one control-plane shard snapshot"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    range_prefix_calls.store(0, Ordering::SeqCst);
    let entries = dfs
        .posix_list_dir_distributed(directory)
        .expect("distributed list");
    assert!(entries.iter().any(|entry| entry.path == path));
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "one local-primary listing must retain its starting metadata generation"
    );
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "one local-primary listing must reuse one control-plane shard snapshot"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    range_prefix_calls.store(0, Ordering::SeqCst);
    let prefix = dfs
        .posix_read_file_prefix_distributed(path, payload.len())
        .expect("distributed prefix read");
    assert_eq!(prefix.bytes, payload);
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "one local-primary prefix read must retain its starting metadata generation"
    );
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "one local-primary prefix read must reuse one control-plane shard snapshot"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    range_prefix_calls.store(0, Ordering::SeqCst);
    assert_eq!(
        dfs.posix_read_file_distributed(path)
            .expect("distributed full read"),
        payload
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "one local-primary full read must retain its starting metadata generation"
    );
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "one local-primary full read must reuse one control-plane shard snapshot"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    dfs.posix_stat(path)
        .expect("ordinary stat after distributed reads");
    assert!(
        crate::metadata_read_snapshot::metadata_durable_table_probes() > 0,
        "the operation-scoped metadata snapshot must be released before returning"
    );
    drop(dfs);
    runtime.close().expect("ordinary native close");
}

#[test]
fn native_distributed_reader_streams_to_content_store() {
    let name = "put-local-object-version-from-reader";
    let node_id = NodeId::from_legacy_named("node_stream_local_object").expect("node");
    let (runtime, client) = native_sifs_service(name, node_id.clone());
    drop(client);
    let path = "/services/streamed.bin";
    let mut dfs = runtime
        .metadata_primary_service(path)
        .expect("actual current producer");
    let base = dfs
        .posix_write_file(
            path,
            b"reader predecessor",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("canonical recorded predecessor");
    let size = 256 * 1024 + 31;
    let reader = std::io::repeat(0x42).take(size);
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id.clone(),
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: node_id.clone(),
    };
    // Managed DFS publishes through its canonical native reader path. The
    // local object-version helper intentionally accepts only standalone work.
    let outcome = dfs
        .write_distributed_reader_with_candidates(
            request,
            reader,
            vec![node_id.clone()],
            DfsReplicationPolicy::new(1, 1).expect("one actual holder"),
            DfsDistributedWriteOptions::default(),
        )
        .expect("stream canonical native object");
    let outcome = dfs
        .settle_ready_write_outcome(outcome, true)
        .expect("actual publication");
    let NetworkedDfsWriteOutcome::Committed {
        content_hash: streamed_hash,
        ..
    } = outcome
    else {
        panic!("canonical streamed object must commit");
    };
    let object = dfs
        .current_read_object_metadata(&base.object_id)
        .expect("streamed object metadata");
    assert_eq!(object.content_hash.as_deref(), Some(streamed_hash.as_str()));
    assert_eq!(object.content_size_bytes, Some(size));
    let bytes = dfs
        .content
        .read_blob(&streamed_hash)
        .expect("streamed blob");
    assert_eq!(bytes.len() as u64, size);
    assert!(bytes.iter().all(|byte| *byte == 0x42));
    assert_eq!(streamed_hash, content_hash(&bytes));
    assert!(
        std::fs::read_dir(runtime.data_dir(&node_id).dfs_dir().join("content/staging"))
            .expect("actual content staging")
            .next()
            .is_none(),
        "streaming native object writes must clean temporary staging files"
    );
    drop(dfs);
    runtime.close().expect("ordinary native close");
}

#[test]
fn dfs_metadata_publish_scheduler_skips_read_only_events() {
    let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
    let node = NodeId::from_legacy_named("node_dfs_publish_scheduler_skip").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_dfs_publish_scheduler_skip").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_dfs_publish_scheduler_skip").expect("version");
    let content_hash =
        "blake3:2222222222222222222222222222222222222222222222222222222222222222".to_string();
    let events = [NetworkedDfsServiceEvent::BlobFetched {
        intent_id: "intent_scheduler_skip".to_string(),
        object_id,
        version,
        source_node_id: node,
        content_hash,
    }];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let start = schedule_dfs_metadata_publish_after_events_async_on_runtime(
        scheduler,
        runtime.handle().clone(),
        &events,
        DfsMetadataPublishScheduleOptions::default(),
        || async {
            Err::<DfsMetadataJournalReplicationReport, String>(
                "read-only events must not schedule a metadata publish".to_string(),
            )
        },
        |_report| {},
        |_error| {},
    );
    assert_eq!(start, None);
}

#[test]
fn default_dfs_metadata_publish_scheduler_skips_read_only_events() {
    let node = NodeId::from_legacy_named("node_dfs_default_publish_scheduler_skip").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_dfs_default_publish_scheduler_skip").expect("object");
    let version = DfsVersion::from_legacy_named("version_dfs_default_publish_scheduler_skip")
        .expect("version");
    let content_hash =
        "blake3:3333333333333333333333333333333333333333333333333333333333333333".to_string();
    let events = [NetworkedDfsServiceEvent::BlobFetched {
        intent_id: "intent_default_scheduler_skip".to_string(),
        object_id,
        version,
        source_node_id: node,
        content_hash,
    }];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let start = schedule_default_dfs_metadata_publish_after_events_async_on_runtime(
        runtime.handle().clone(),
        &events,
        DfsMetadataPublishScheduleOptions::default(),
        || async {
            Err::<DfsMetadataJournalReplicationReport, String>(
                "read-only events must not schedule a metadata publish".to_string(),
            )
        },
        |_report| {},
        |_error| {},
    );
    assert_eq!(start, None);
}

fn native_current_read_fixture(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NodeId,
    DfsStat,
    Vec<u8>,
) {
    native_read_fixture(
        name,
        2,
        false,
        (0..(2 * 1024 * 1024 + 8192))
            .map(|index| (index % 251) as u8)
            .collect(),
    )
}

/// The read engine plans cached reads, location prunes and retries from the
/// reader node's own store. The fixture's authoritative reader is the shard
/// primary, so its store-backed owner runs those reads; a delegating client
/// keeps no local object view to plan from.
fn primary_store_reader(
    runtime: &crate::test_runtime::DfsTestRuntime,
    reader_node: &NodeId,
    stat: &DfsStat,
) -> NetworkedDfsService<DfsNodeTransport> {
    let reader = runtime
        .metadata_primary_service(&stat.path)
        .expect("store-backed reader on the shard primary");
    assert_eq!(reader.node_id(), reader_node);
    reader
}

fn wait_for_actual_read_retry(reader: &NetworkedDfsService<DfsNodeTransport>, request: &str) {
    let deadline = reader
        .reads
        .pending_reads
        .get(request)
        .unwrap()
        .next_retry_at;
    let wait = deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1);
    crate::test_runtime::executor().block_on(async { tokio::time::sleep(wait).await });
}

#[test]
fn hinted_remote_read_serves_blob_without_local_metadata() {
    let name = "native-hinted-read-without-metadata";
    let (runtime, server, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let server_node = config.primary_node_id;
    let requester_node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != server_node)
        .unwrap();
    let object_id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    // Genuine immutable capture produces the hinted blob without publishing a
    // canonical object row, just as current manifest-write capture does.
    let manifest = server
        .content()
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .unwrap();
    let server_store_row = || {
        runtime
            .node_metadata(&server_node)
            .unwrap()
            .get_object(&object_id)
            .unwrap()
    };
    assert!(server_store_row().is_none());
    let mut requester = runtime.client(&requester_node).unwrap();
    let request = DfsDirectReadRequest {
        request_id: requester.next_request_id(),
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: manifest.content_hash,
        content_size_bytes: manifest.size_bytes,
        range_offset: None,
        range_max_bytes: None,
    };
    let before = runtime.rpc_observations().len();
    let response = crate::test_runtime::executor().block_on(async {
        requester
            .transport
            .read_object_stream(&server_node, &request)
            .await
            .unwrap()
            .collect_to_read_response()
            .await
            .unwrap()
    });
    assert_eq!(response.request_id, request.request_id);
    assert_eq!(response.object_id, object_id);
    assert!(
        matches!(response.result, DfsReadResult::Found(DfsReadData { version: actual_version, bytes: actual, .. })
        if actual_version == version && actual == bytes)
    );
    assert!(server_store_row().is_none());
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == requester_node
                && event.target == server_node
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    drop(requester);
    drop(server);
    runtime.close().unwrap();
}

#[test]
fn uncached_remote_read_arbitrary_size_uses_reader_response() {
    let (runtime, _, _, reader_node, stat, bytes) =
        native_current_read_fixture("native-uncached-large-read");
    let mut reader = runtime.client(&reader_node).unwrap();
    let hash = stat.content_hash.as_ref().unwrap();
    assert!(!reader.content().has_blob(hash).unwrap());
    let before = runtime.rpc_observations().len();
    // A delegating caller reads the object from its authority and requests
    // the uncached body for that metadata, as its distributed reads do.
    let object = reader
        .current_read_object_metadata_distributed(&stat.object_id)
        .unwrap();
    let targets = reader
        .request_remote_object_read_for_metadata_without_cache(&object)
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert!(stat.current_locations.contains(&targets[0]));
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteReadCompleted { object_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), .. }
            if object_id == &stat.object_id && actual == &bytes)));
    assert!(
        !reader.content().has_blob(hash).unwrap(),
        "uncached remote reads do not store the returned body"
    );
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == reader_node
                && event.target == targets[0]
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn uncached_remote_read_direct_transport_completes_without_protocol_frame() {
    let (runtime, _, _, reader_node, stat, bytes) =
        native_current_read_fixture("native-direct-read-no-frame");
    let mut reader = runtime.client(&reader_node).unwrap();
    let before = runtime.rpc_observations().len();
    // A delegating caller reads the object from its authority and requests
    // the uncached body for that metadata, as its distributed reads do.
    let object = reader
        .current_read_object_metadata_distributed(&stat.object_id)
        .unwrap();
    let targets = reader
        .request_remote_object_read_for_metadata_without_cache(&object)
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert!(
        !reader.reads.pending_reads.is_empty(),
        "actual direct reply re-enters the normal incoming queue"
    );
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert_eq!(
        calls
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == targets[0]
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        1
    );
    assert!(
        !calls.iter().any(|event| event.source == reader_node
            && event.route == crate::DFS_DATA_READ_ROUTE
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "current direct-read-eligible request avoids a duplicate framed DFS RPC"
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteReadCompleted { object_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), .. }
            if object_id == &stat.object_id && actual == &bytes)));
    assert!(reader.reads.pending_reads.is_empty());
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn failed_uncached_remote_direct_read_cleans_pending_state() {
    let (runtime, _, _, reader_node, stat, _) =
        native_current_read_fixture("native-direct-read-error-cleanup");
    let mut reader = runtime.client(&reader_node).unwrap();
    // A delegating caller reads the object from its authority and requests
    // the uncached body for that metadata, as its distributed reads do.
    let object = reader
        .current_read_object_metadata_distributed(&stat.object_id)
        .unwrap();
    let targets = reader.ranked_transfer_sources(object.current_locations.clone());
    let target = targets
        .into_iter()
        .find(|node| *node != reader_node)
        .unwrap();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(reader_node.clone()),
        target: Some(target.clone()),
        route: Some(DFS_DIRECT_READ_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let before = runtime.rpc_observations().len();
    let error = reader
        .request_remote_object_read_for_metadata_without_cache(&object)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("bounded injected carrier failure"),
        "unexpected native carrier error: {error}"
    );
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert_eq!(
        calls
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == target
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch)
            .count(),
        1
    );
    assert!(
        !calls.iter().any(|event| event.source == reader_node
            && event.route == crate::DFS_DATA_READ_ROUTE
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "failed direct call never creates a duplicate framed send"
    );
    assert!(
        reader.reads.pending_reads.is_empty(),
        "a failed synchronous direct request leaves no pending read"
    );
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn stale_pending_remote_read_can_be_retried() {
    let (runtime, owner, _, reader_node, stat, bytes) =
        native_read_fixture("native-read-stale-pending", 1, true, vec![0x41; 4096]);
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    let before = runtime.rpc_observations().len();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![owner.clone()]
    );
    let request = reader.reads.pending_reads.keys().next().unwrap().clone();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![owner.clone()]
    );
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == owner
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        1
    );
    wait_for_actual_read_retry(&reader, &request);
    assert!(
        !reader
            .has_pending_remote_read_for_current_object(&stat.object_id)
            .unwrap()
    );
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![owner.clone()]
    );
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == owner
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        2
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && actual == &bytes)
    ));
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn process_incoming_accepts_queued_remote_read_response_before_expiry_prune() {
    let (runtime, owner, _, reader_node, stat, bytes) =
        native_read_fixture("native-read-queued-before-prune", 1, true, vec![0x61; 4096]);
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![owner.clone()]
    );
    let request = reader.reads.pending_reads.keys().next().unwrap().clone();
    let original_deadline = runtime.expire_queued_native_read(&mut reader, &request);
    assert!(
        reader
            .reads
            .pending_reads
            .get(&request)
            .unwrap()
            .next_retry_at
            < original_deadline
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        request_id, object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }),
    } if request_id == &request && object_id == &stat.object_id && source_node_id == &owner && actual == &bytes)),
        "the real queued reply wins before normal stale pruning");
    assert!(!reader.reads.pending_reads.contains_key(&request));
    assert_eq!(
        reader
            .read_local_object(&stat.object_id)
            .unwrap()
            .unwrap()
            .bytes,
        bytes
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn native_read_fixture_observes_actual_publication_before_queued_read_setup() {
    use std::cell::Cell;
    use std::sync::{Arc, Mutex};
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let captured = Arc::new(Mutex::new(
        Vec::<crate::DfsMetadataJournalBatchCommand>::new(),
    ));
    let saw_gap = Cell::new(false);
    let wait_for_publication = || {
        let deadline = Instant::now() + Duration::from_secs(2);
        while captured.lock().unwrap().is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !captured.lock().unwrap().is_empty(),
            "actual committed-write publication must reach the carrier"
        );
    };
    let (runtime, owner, _, reader_node, stat, bytes) =
        native_read_fixture_observing_materialization(
            "native-read-controlled-publication",
            1,
            false,
            vec![0x61; 4096],
            |runtime, reader, _| {
                runtime.install_fault(crate::test_runtime::FaultRule {
                    source: None,
                    target: Some(reader.clone()),
                    route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
                    call_id: None,
                    origin: None,
                    remaining_hits: 1,
                    action: crate::test_runtime::FaultAction::PauseWriteCommitPublication {
                        gate: Arc::clone(&gate),
                        captured: Arc::clone(&captured),
                    },
                });
            },
            |runtime, reader_node, published, observed| {
                if saw_gap.get() {
                    return;
                }
                wait_for_publication();
                // The reader node's own store genuinely lacks the object
                // while its publication is held.
                assert!(
                    runtime
                        .node_metadata(reader_node)
                        .unwrap()
                        .get_object(&published.object_id)
                        .unwrap()
                        .is_none()
                );
                let command = captured.lock().unwrap()[0].clone();
                assert!(
                    observed.materialized_sequence.unwrap() < command.batch.next_after_sequence
                );
                assert_eq!(
                    command.file_system_id.as_ref(),
                    Some(&published.file_system_id)
                );
                assert!(
                    crate::runtime_host::materialization_test::wait_for_existing_materialization(
                        runtime.data_dir(reader_node),
                        &published.file_system_id,
                        &command.batch.shard_id,
                        &runtime.runtime_handle(),
                        command.batch.next_after_sequence,
                        Instant::now(),
                        |_| panic!("an expired readiness observation must not request work"),
                    )
                    .is_err()
                );
                eprintln!(
                    "QUEUED-READ controlled gap: expected={published:?} actual={observed:?} held={:?}",
                    command.batch.decoded().unwrap()
                );
                saw_gap.set(true);
                runtime.clear_faults();
                gate.add_permits(1);
            },
        );
    wait_for_publication();
    let view = runtime
        .node_metadata(&reader_node)
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap();
    // Always release actual paused ingress before asserting the old failure.
    runtime.clear_faults();
    if !saw_gap.get() {
        gate.add_permits(1);
    }
    if view
        .as_ref()
        .is_none_or(|object| object.version != stat.version)
    {
        eprintln!(
            "QUEUED-READ controlled premature fixture: stat={stat:?} view={view:?} held={:#?}",
            captured.lock().unwrap()[0].batch.decoded()
        );
        runtime.close().unwrap();
        panic!("fixture returned before its exact reader view was published: {view:?}");
    }
    // The reader's caller plans the queued read from the published object,
    // as a delegating caller's distributed read does.
    let mut reader = runtime.client(&reader_node).unwrap();
    let object = reader
        .current_read_object_metadata_distributed(&stat.object_id)
        .unwrap();
    assert_eq!(object.version, stat.version);
    let request = reader
        .request_remote_object_read_for_metadata_without_cache(&object)
        .unwrap();
    assert_eq!(request, vec![owner.clone()]);
    assert!(
        saw_gap.get(),
        "the actual reader view must be missing before releasing publication"
    );
    let request_id = reader.reads.pending_reads.keys().next().unwrap().clone();
    runtime.expire_queued_native_read(&mut reader, &request_id);
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: actual, object_id, source_node_id,
            result: DfsReadResult::Found(DfsReadData { version, bytes: actual_bytes, .. }),
        } if actual == &request_id && object_id == &stat.object_id && source_node_id == &owner
            && version == &stat.version && actual_bytes == &bytes)));
    assert!(!reader.reads.pending_reads.contains_key(&request_id));
    runtime.assert_fault_budgets().unwrap();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn remote_read_planning_deduplicates_duplicate_locations() {
    let (runtime, owner, _, reader_node, stat, bytes) = native_read_fixture(
        "native-read-duplicate-location-hint",
        1,
        false,
        vec![0x44; 4096],
    );
    let mut reader = runtime.client(&reader_node).unwrap();
    let actual = reader
        .current_read_object_metadata_distributed(&stat.object_id)
        .unwrap();
    let reader_store_row = || {
        runtime
            .node_metadata(&reader_node)
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap()
    };
    let stored = reader_store_row();
    let mut duplicate_hint = actual.clone();
    // Repeat only an existing real holder in a read-only caller descriptor.
    // No authoritative row or source identity is installed or widened.
    duplicate_hint
        .current_locations
        .extend(actual.current_locations.clone());
    let before = runtime.rpc_observations().len();
    assert_eq!(
        reader
            .request_remote_object_read_for_metadata_without_cache(&duplicate_hint)
            .unwrap(),
        vec![owner.clone()]
    );
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == reader_node
                && event.target == owner
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        1
    );
    // The reader node's store row is neither installed nor widened.
    assert_eq!(reader_store_row(), stored);
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && actual == &bytes)
    ));
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn remote_read_planning_uses_single_active_source_and_falls_back_after_miss() {
    let (runtime, _, _, reader_node, stat, bytes) =
        native_read_fixture("native-read-real-miss-fallback", 2, true, vec![0x4e; 4096]);
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    let before = runtime.rpc_observations().len();
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let fallback = stat
        .current_locations
        .iter()
        .find(|node| **node != first[0])
        .unwrap()
        .clone();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        first
    );
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == reader_node
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
            .count(),
        1,
        "one real active read suppresses every other holder"
    );
    deny_native_read_repair(&runtime, &reader_node, &first[0]);
    let captured = runtime.alter_queued_read_replies(
        &mut reader.transport,
        crate::test_runtime::ReadReplyFault::NotFound,
        1,
    );
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].source_node_id, first[0]);
    assert_eq!(captured[0].target_node_id, reader_node);
    assert!(
        matches!(&captured[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.object_id == stat.object_id && matches!(&response.result,
            DfsReadResult::Found(data) if data.object_id == stat.object_id && data.version == stat.version
                && data.content_hash == stat.content_hash && data.bytes == bytes)),
        "negative mutation starts from the real native response's exact tuple and bytes"
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        source_node_id, result: DfsReadResult::NotFound, ..
    } if source_node_id == &first[0])
    ));
    let current = reader
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(current.current_locations, vec![fallback.clone()]);
    assert_eq!(current.content_hash, stat.content_hash);
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
            .locations,
        vec![fallback.clone()],
        "the actual metadata primary authors the source prune normally"
    );
    let after_negative = runtime.rpc_observations().len();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![fallback.clone()]
    );
    assert!(
        runtime.rpc_observations()[after_negative..]
            .iter()
            .any(|event| event.source == reader_node
                && event.target == fallback
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && source_node_id == &fallback && actual == &bytes)));
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

fn prune_actual_read_source_with_second_read(
    runtime: &crate::test_runtime::DfsTestRuntime,
    primary: &NodeId,
    object_id: &DfsObjectId,
    missing: &NodeId,
) {
    deny_native_read_repair(runtime, primary, missing);
    // A second store-backed reader on the primary authors the prune.
    let path = runtime
        .node_metadata(primary)
        .unwrap()
        .get_object(object_id)
        .unwrap()
        .expect("the primary's store holds the object")
        .path;
    let mut second = runtime.metadata_primary_service(&path).unwrap();
    assert_eq!(second.node_id(), primary);
    let mut hint = second.current_read_object_metadata(object_id).unwrap();
    assert!(hint.current_locations.contains(missing));
    // Restrict a genuine read-only source hint to one already-known holder.
    // The current metadata row remains intact until the real reply is rejected.
    hint.current_locations.retain(|node| node == missing);
    assert_eq!(
        second
            .request_remote_object_read_for_metadata_without_cache(&hint)
            .unwrap(),
        vec![missing.clone()]
    );
    let captured = runtime.alter_queued_read_replies(
        &mut second.transport,
        crate::test_runtime::ReadReplyFault::NotFound,
        1,
    );
    assert_eq!(captured[0].source_node_id, *missing);
    let events = second.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id: actual, source_node_id, result: DfsReadResult::NotFound, ..
    } if actual == object_id && source_node_id == missing)
    ));
    assert!(
        !second
            .metadata_backend()
            .unwrap()
            .get_object(object_id)
            .unwrap()
            .unwrap()
            .current_locations
            .contains(missing)
    );
}

#[test]
fn pending_remote_read_to_removed_location_does_not_block_fallback() {
    let (runtime, _, _, primary, stat, bytes) = native_read_fixture(
        "native-read-removed-source-pending",
        2,
        true,
        vec![0x51; 4096],
    );
    let mut reader = primary_store_reader(&runtime, &primary, &stat);
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let old_request = reader.reads.pending_reads.keys().next().unwrap().clone();
    let fallback = stat
        .current_locations
        .iter()
        .find(|node| **node != first[0])
        .unwrap()
        .clone();
    prune_actual_read_source_with_second_read(&runtime, &primary, &stat.object_id, &first[0]);
    assert!(
        !reader
            .has_pending_remote_read_for_current_object(&stat.object_id)
            .unwrap(),
        "an actual removed location cannot keep its earlier request active"
    );
    let before = runtime.rpc_observations().len();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![fallback.clone()]
    );
    assert!(!reader.reads.pending_reads.contains_key(&old_request));
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == primary
                && event.target == fallback
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && source_node_id == &fallback && actual == &bytes)));
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn expired_remote_read_attempt_rotates_to_next_location_before_retrying_same_source() {
    let (runtime, _, _, reader_node, stat, bytes) =
        native_read_fixture("native-read-expiry-rotation", 2, true, vec![0x45; 4096]);
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let request = reader.reads.pending_reads.keys().next().unwrap().clone();
    wait_for_actual_read_retry(&reader, &request);
    let before = runtime.rpc_observations().len();
    let next = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(next.len(), 1);
    assert_ne!(
        next, first,
        "ordinary expiry rotates to the other genuine holder before same-source retry"
    );
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert!(!calls.iter().any(|event| event.source == reader_node
        && event.target == first[0]
        && event.route == DFS_DIRECT_READ_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch));
    assert!(calls.iter().any(|event| event.source == reader_node
        && event.target == next[0]
        && event.route == DFS_DIRECT_READ_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::Completed));
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if source_node_id == &next[0] && actual == &bytes)
    ));
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn late_remote_read_from_removed_location_does_not_clear_fallback_request() {
    let (runtime, _, _, primary, stat, bytes) = native_read_fixture(
        "native-read-removed-source-late-reply",
        2,
        true,
        vec![0x52; 4096],
    );
    let mut reader = primary_store_reader(&runtime, &primary, &stat);
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let old_request = reader.reads.pending_reads.keys().next().unwrap().clone();
    let fallback = stat
        .current_locations
        .iter()
        .find(|node| **node != first[0])
        .unwrap()
        .clone();
    prune_actual_read_source_with_second_read(&runtime, &primary, &stat.object_id, &first[0]);
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![fallback.clone()]
    );
    assert!(!reader.reads.pending_reads.contains_key(&old_request));
    let fallback_request = reader.reads.pending_reads.keys().next().unwrap().clone();
    // Both replies are the native decoder's actual owned frames. Deliver the
    // older frame first to observe its effect before the real fallback frame.
    let mut incoming = reader.transport.drain_incoming_messages().unwrap();
    let index = incoming
        .iter()
        .position(|routed| {
            matches!(&routed.message,
        DfsNetworkMessage::ReadResponse(response) if response.request_id == old_request)
        })
        .unwrap();
    let old = incoming.remove(index);
    assert_eq!(old.source_node_id, first[0]);
    assert!(
        matches!(&old.message, DfsNetworkMessage::ReadResponse(response)
        if response.object_id == stat.object_id && matches!(&response.result,
            DfsReadResult::Found(data) if data.object_id == stat.object_id && data.version == stat.version
                && data.content_hash == stat.content_hash && data.bytes == bytes))
    );
    let events = reader
        .process_routed_messages_with_options(vec![old], false)
        .unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        source_node_id, result: DfsReadResult::Rejected { reason }, ..
    } if source_node_id == &first[0] && reason.contains("unknown DFS read request"))
    ));
    assert!(
        reader.reads.pending_reads.contains_key(&fallback_request),
        "an actual late old-source frame cannot clear the new source's genuine request"
    );
    let events = reader
        .process_routed_messages_with_options(incoming, false)
        .unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && source_node_id == &fallback && actual == &bytes)));
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn remote_read_rejection_from_stale_location_falls_back_to_next_source() {
    let (runtime, _, _, reader_node, stat, bytes) = native_read_fixture(
        "native-read-real-stale-rejection",
        2,
        true,
        vec![0x4e; 4096],
    );
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    let before = runtime.rpc_observations().len();
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let fallback = stat
        .current_locations
        .iter()
        .find(|node| **node != first[0])
        .unwrap()
        .clone();
    deny_native_read_repair(&runtime, &reader_node, &first[0]);
    let captured = runtime.alter_queued_read_replies(
        &mut reader.transport,
        crate::test_runtime::ReadReplyFault::StaleLocation,
        1,
    );
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].source_node_id, first[0]);
    assert_eq!(captured[0].target_node_id, reader_node);
    assert!(
        matches!(&captured[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.object_id == stat.object_id && matches!(&response.result,
            DfsReadResult::Found(data) if data.object_id == stat.object_id && data.version == stat.version
                && data.content_hash == stat.content_hash && data.bytes == bytes)),
        "negative mutation starts from the real native response's exact tuple and bytes"
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        source_node_id, result: DfsReadResult::Rejected { reason }, ..
    } if source_node_id == &first[0] && reason.contains("not a current location"))
    ));
    let current = reader
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(current.current_locations, vec![fallback.clone()]);
    assert_eq!(current.content_hash, stat.content_hash);
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
            .locations,
        vec![fallback.clone()],
        "the actual metadata primary authors the source prune normally"
    );
    let after_negative = runtime.rpc_observations().len();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![fallback.clone()]
    );
    assert!(
        runtime.rpc_observations()[after_negative..]
            .iter()
            .any(|event| event.source == reader_node
                && event.target == fallback
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && source_node_id == &fallback && actual == &bytes)));
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn inline_remote_read_wrong_object_is_isolated_and_falls_back() {
    let (runtime, _, _, reader_node, stat, bytes) = native_read_fixture(
        "native-read-real-envelope-mismatch",
        2,
        true,
        vec![0x4e; 4096],
    );
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    let before = runtime.rpc_observations().len();
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let fallback = stat
        .current_locations
        .iter()
        .find(|node| **node != first[0])
        .unwrap()
        .clone();
    deny_native_read_repair(&runtime, &reader_node, &first[0]);
    let captured = runtime.alter_queued_read_replies(
        &mut reader.transport,
        crate::test_runtime::ReadReplyFault::WrongEnvelopeObject,
        1,
    );
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].source_node_id, first[0]);
    assert_eq!(captured[0].target_node_id, reader_node);
    assert!(
        matches!(&captured[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.object_id == stat.object_id && matches!(&response.result,
            DfsReadResult::Found(data) if data.object_id == stat.object_id && data.version == stat.version
                && data.content_hash == stat.content_hash && data.bytes == bytes)),
        "negative mutation starts from the real native response's exact tuple and bytes"
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        source_node_id, result: DfsReadResult::Rejected { reason }, ..
    } if source_node_id == &first[0] && reason.contains("unexpected DFS read response object"))
    ));
    let current = reader
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(current.current_locations, vec![fallback.clone()]);
    assert_eq!(current.content_hash, stat.content_hash);
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
            .locations,
        vec![fallback.clone()],
        "the actual metadata primary authors the source prune normally"
    );
    let after_negative = runtime.rpc_observations().len();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![fallback.clone()]
    );
    assert!(
        runtime.rpc_observations()[after_negative..]
            .iter()
            .any(|event| event.source == reader_node
                && event.target == fallback
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && source_node_id == &fallback && actual == &bytes)));
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn inline_remote_read_wrong_payload_object_is_isolated_and_falls_back() {
    let (runtime, _, _, reader_node, stat, bytes) = native_read_fixture(
        "native-read-real-payload-mismatch",
        2,
        true,
        vec![0x4e; 4096],
    );
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    let before = runtime.rpc_observations().len();
    let first = reader.request_remote_object_read(&stat.object_id).unwrap();
    assert_eq!(first.len(), 1);
    let fallback = stat
        .current_locations
        .iter()
        .find(|node| **node != first[0])
        .unwrap()
        .clone();
    deny_native_read_repair(&runtime, &reader_node, &first[0]);
    let captured = runtime.alter_queued_read_replies(
        &mut reader.transport,
        crate::test_runtime::ReadReplyFault::WrongPayloadObject,
        1,
    );
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].source_node_id, first[0]);
    assert_eq!(captured[0].target_node_id, reader_node);
    assert!(
        matches!(&captured[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.object_id == stat.object_id && matches!(&response.result,
            DfsReadResult::Found(data) if data.object_id == stat.object_id && data.version == stat.version
                && data.content_hash == stat.content_hash && data.bytes == bytes)),
        "negative mutation starts from the real native response's exact tuple and bytes"
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        source_node_id, result: DfsReadResult::Rejected { reason }, ..
    } if source_node_id == &first[0] && reason.contains("unexpected DFS read response payload object"))));
    let current = reader
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(current.current_locations, vec![fallback.clone()]);
    assert_eq!(current.content_hash, stat.content_hash);
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
            .locations,
        vec![fallback.clone()],
        "the actual metadata primary authors the source prune normally"
    );
    let after_negative = runtime.rpc_observations().len();
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![fallback.clone()]
    );
    assert!(
        runtime.rpc_observations()[after_negative..]
            .iter()
            .any(|event| event.source == reader_node
                && event.target == fallback
                && event.route == DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        object_id, source_node_id, result: DfsReadResult::Found(DfsReadData { bytes: actual, .. }), ..
    } if object_id == &stat.object_id && source_node_id == &fallback && actual == &bytes)));
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn remote_read_content_identity_mismatch_prunes_stale_location() {
    let (runtime, owner, _, reader_node, stat, bytes) = native_read_fixture(
        "native-read-captured-hash-mismatch",
        1,
        true,
        vec![0x62; 4096],
    );
    let mut reader = primary_store_reader(&runtime, &reader_node, &stat);
    assert_eq!(
        reader.request_remote_object_read(&stat.object_id).unwrap(),
        vec![owner.clone()]
    );
    let request = reader.reads.pending_reads.keys().next().unwrap().clone();
    deny_native_read_repair(&runtime, &reader_node, &owner);
    let captured = runtime.alter_queued_read_replies(
        &mut reader.transport,
        crate::test_runtime::ReadReplyFault::WrongContentHash,
        1,
    );
    assert!(
        matches!(&captured[0].message, DfsNetworkMessage::ReadResponse(response)
        if response.request_id == request && matches!(&response.result, DfsReadResult::Found(data)
            if data.object_id == stat.object_id && data.version == stat.version
                && data.content_hash == stat.content_hash && data.bytes == bytes))
    );
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
        request_id, source_node_id, result: DfsReadResult::Rejected { reason }, ..
    } if request_id == &request && source_node_id == &owner && reason.contains("expected content hash"))));
    assert!(!reader.reads.pending_reads.contains_key(&request));
    assert!(
        !reader
            .current_read_object_metadata(&stat.object_id)
            .unwrap()
            .current_locations
            .contains(&owner)
    );
    assert!(
        reader
            .request_remote_object_read(&stat.object_id)
            .unwrap()
            .is_empty()
    );
    runtime.clear_faults();
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn remote_read_location_prune_updates_stale_backend_version_record() {
    let (runtime, mut dfs, assignment) = crate::test_support::current_native_metadata_service(
        "native-read-location-prune",
        3,
        "/services",
    )
    .unwrap();
    let path = "/services/native-prune.txt";
    let nodes = std::iter::once(assignment.primary_node_id.clone())
        .chain(assignment.replica_node_ids.iter().cloned())
        .collect::<Vec<_>>();
    let stat = dfs
        .posix_write_file(
            path,
            b"actual durable prune bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    dfs.posix_sync_file(path, DfsPosixSyncKind::File).unwrap();
    // The prune is authored on the primary's store; the delegating client
    // keeps none.
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    assert_eq!(owner.node_id(), &assignment.primary_node_id);
    let backend = owner.metadata_backend().unwrap();
    let before = backend.get_object(&stat.object_id).unwrap().unwrap();
    let version = backend
        .get_version(&stat.object_id, &stat.version)
        .unwrap()
        .unwrap();
    let receipts = backend
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    let missing = receipts
        .iter()
        .find(|receipt| receipt.replica_node_id != assignment.primary_node_id)
        .unwrap()
        .replica_node_id
        .clone();
    assert!(before.current_locations.contains(&missing));
    assert!(version.locations.contains(&missing));
    owner
        .mark_remote_read_location_missing(
            &stat.object_id,
            &stat.version,
            &stat.content_hash,
            &missing,
        )
        .unwrap();
    let backend = owner.metadata_backend().unwrap();
    let object = backend.get_object(&stat.object_id).unwrap().unwrap();
    let after = backend
        .get_version(&stat.object_id, &stat.version)
        .unwrap()
        .unwrap();
    assert_eq!(object.version, before.version);
    assert_eq!(object.content_hash, before.content_hash);
    assert_eq!(object.content_size_bytes, before.content_size_bytes);
    assert_eq!(after.content_durability, version.content_durability);
    assert_eq!(
        object.current_locations,
        before
            .current_locations
            .into_iter()
            .filter(|node| node != &missing)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        after.locations,
        version
            .locations
            .into_iter()
            .filter(|node| node != &missing)
            .collect::<Vec<_>>()
    );
    assert!(
        !backend
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == missing)
    );
    drop(owner);
    assert_eq!(
        dfs.posix_read_file_distributed(path).unwrap(),
        b"actual durable prune bytes"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn replica_remote_read_cache_and_prune_do_not_append_authoritative_metadata() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-replica-read-prune", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-replica-read-prune", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-replica-read-prune.bin";
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let stat = writer
        .posix_write_file(
            path,
            b"native replica bytes",
            DfsWriteOptions::replicated_eventual(vec![nodes[0].clone()]),
        )
        .unwrap();
    writer
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    drop(writer);
    let mut reader = runtime.client(&nodes[1]).unwrap();
    reader.posix_stat_distributed(path).unwrap();
    let shard = DfsMetadataShardId::new(runtime.root_shard().unwrap().shard_id);
    // The replica node's own store is inspected directly; the delegating
    // reader keeps none.
    let replica_store = || runtime.node_metadata(&nodes[1]).unwrap();
    let before = replica_store()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    let before_object = replica_store()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    reader
        .mark_remote_read_location_missing(
            &stat.object_id,
            &stat.version,
            &stat.content_hash,
            &nodes[0],
        )
        .unwrap();
    let after = replica_store()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    assert_eq!(after.prefix_proof, before.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        before.semantic_state_fingerprint
    );
    assert_eq!(
        replica_store()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap(),
        before_object
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn read_current_object_uses_hash_addressed_blob_without_manifest_scan() {
    let node_id = NodeId::from_legacy_named("node_hash_addressed").expect("node");
    let bytes = b"shared content-addressed bytes";
    let (_runtime, mut dfs) = native_sifs_service(
        "read_current_object_uses_hash_addressed_blob_without_manifest_scan",
        node_id.clone(),
    );
    let committed = dfs
        .posix_write_file(
            "/services/hash-addressed.txt",
            bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("normal Main publication records the actual content generation");
    let object_id = committed.object_id;
    let version = committed.version;
    let content_hash = committed.content_hash.expect("actual committed hash");
    let manifest_path = _runtime
        .data_dir(&node_id)
        .dfs_dir()
        .join("content/objects")
        .join(object_id.as_str())
        .join(version.as_str())
        .join("manifest.sobj");
    assert!(
        !manifest_path.exists(),
        "normal Main publication uses hash-addressed content without an object manifest"
    );

    // Local object reads resolve through the node's store; the delegating
    // client keeps none, so the store-backed owner reads.
    drop(dfs);
    let dfs = _runtime
        .metadata_primary_service("/services/hash-addressed.txt")
        .unwrap();
    let outcome = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read");

    match outcome {
        NetworkedDfsReadOutcome::Found { data, freshness } => {
            assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
            assert_eq!(data.object_id, object_id);
            assert_eq!(data.version, version);
            assert_eq!(data.content_hash.as_deref(), Some(content_hash.as_str()));
            assert_eq!(data.bytes, bytes);
        }
        other => {
            panic!("expected current object bytes from hash-addressed blob, got {other:?}")
        }
    }
    drop(dfs);
    _runtime.close().expect("normal runtime shutdown");
}

#[test]
fn read_current_object_range_uses_bounded_hash_addressed_blob_slice() {
    let node_id = NodeId::from_legacy_named("node_hash_range").expect("node");
    let bytes = b"range-readable content-addressed bytes";
    let (_runtime, mut dfs) = native_sifs_service(
        "read_current_object_range_uses_bounded_hash_addressed_blob_slice",
        node_id.clone(),
    );
    let committed = dfs
        .posix_write_file(
            "/services/hash-range.txt",
            bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("normal Main publication records the actual content generation");
    let object_id = committed.object_id;
    let version = committed.version;
    let content_hash = committed.content_hash.expect("actual committed hash");
    let manifest_path = _runtime
        .data_dir(&node_id)
        .dfs_dir()
        .join("content/objects")
        .join(object_id.as_str())
        .join(version.as_str())
        .join("manifest.sobj");
    assert!(
        !manifest_path.exists(),
        "normal Main publication uses hash-addressed content without an object manifest"
    );

    // Local object reads resolve through the node's store; the delegating
    // client keeps none, so the store-backed owner reads.
    drop(dfs);
    let dfs = _runtime
        .metadata_primary_service("/services/hash-range.txt")
        .unwrap();
    let outcome = dfs
        .read_local_object_range_with_state(&object_id, 6, 8, false)
        .expect("range read");
    match outcome {
        NetworkedDfsRangeReadOutcome::Found { data, freshness } => {
            assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
            assert_eq!(data.object_id, object_id);
            assert_eq!(data.version, version);
            assert_eq!(data.content_hash.as_deref(), Some(content_hash.as_str()));
            assert_eq!(data.offset, 6);
            assert_eq!(data.total_size_bytes, bytes.len() as u64);
            assert_eq!(data.bytes, &bytes[6..14]);
        }
        other => panic!("expected current range bytes, got {other:?}"),
    }

    assert_eq!(
        dfs.posix_read_file_range("/services/hash-range.txt", 15, 7)
            .expect("posix range"),
        &bytes[15..22]
    );
    assert_eq!(
        dfs.posix_read_file_range("/services/hash-range.txt", bytes.len() as u64 + 1, 16)
            .expect("posix eof range"),
        Vec::<u8>::new()
    );

    drop(dfs);
    _runtime.close().expect("normal runtime shutdown");
}

#[test]
fn exact_size_local_prefix_read_does_not_rehash_content() {
    let node_id = NodeId::from_legacy_named("node_local_prefix_corrupt").expect("node");
    let path = "/services/local-prefix-corrupt.txt";
    let bytes = vec![0x5a; 20 * 1024];
    let (_runtime, mut dfs) = native_sifs_service(
        "exact_size_local_prefix_read_does_not_rehash_content",
        node_id.clone(),
    );
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("local file write");
    let hash = stat.content_hash.expect("committed content hash");
    let hex = hash.strip_prefix("blake3:").expect("blake3 hash");
    let blob_path = _runtime
        .data_dir(&node_id)
        .dfs_dir()
        .join("content")
        .join("blobs")
        .join(&hex[..2])
        .join(hex);
    let mut corrupt_bytes = bytes.clone();
    corrupt_bytes[0] ^= 0xff;
    std::fs::write(&blob_path, &corrupt_bytes).expect("corrupt local blob in place");

    let read = dfs
        .posix_read_file_prefix_distributed(path, bytes.len())
        .expect("ordinary prefix reads do not rehash content");
    assert_eq!(read.bytes, corrupt_bytes);

    drop(dfs);
    _runtime.close().expect("ordinary native close");
}

#[test]
fn stale_read_skips_missing_stale_blobs() {
    let node_id = NodeId::from_legacy_named("node_stale_skip").expect("node");
    let (runtime, client) =
        native_sifs_service("stale_read_skips_missing_stale_blobs", node_id.clone());
    drop(client);
    let path = "/services/stale-read-missing-blobs.bin";
    let mut dfs = runtime
        .metadata_primary_service(path)
        .expect("actual current producer");
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);
    let readable_stale_bytes = vec![b's'; 17 * 1024];
    let missing_stale_bytes = vec![b'm'; 17 * 1024];
    let missing_current_bytes = vec![b'c'; 17 * 1024];
    // Each version and its metadata head come from genuine native publication.
    // No synthetic object row stands in for a sealed same-object predecessor.
    let readable_stale = dfs
        .posix_write_file(path, &readable_stale_bytes, options.clone())
        .expect("readable stale publication");
    let missing_stale = dfs
        .posix_write_file(path, &missing_stale_bytes, options.clone())
        .expect("later stale publication");
    let current = dfs
        .posix_write_file(path, &missing_current_bytes, options)
        .expect("current publication");
    assert_ne!(readable_stale.version, missing_stale.version);
    assert_ne!(missing_stale.version, current.version);
    let readable_hash = readable_stale
        .content_hash
        .clone()
        .expect("readable stale hash");
    let missing_stale_hash = missing_stale
        .content_hash
        .clone()
        .expect("missing stale hash");
    let current_hash = current.content_hash.clone().expect("current hash");
    // Normal publication may keep only hash-addressed bytes. Cache the exact
    // genuinely published generations through the normal content store before
    // exercising stale-manifest selection; this creates no metadata receipt.
    for (stat, bytes, hash) in [
        (&readable_stale, &readable_stale_bytes, &readable_hash),
        (&missing_stale, &missing_stale_bytes, &missing_stale_hash),
        (&current, &missing_current_bytes, &current_hash),
    ] {
        let manifest = dfs
            .content
            .put_object_version(stat.object_id.as_str(), stat.version.as_str(), bytes)
            .expect("cache the exact published content generation");
        assert_eq!(&manifest.content_hash, hash);
        assert_eq!(manifest.size_bytes, bytes.len() as u64);
    }
    // All three payloads exceed the unchanged small-inline threshold. Inject
    // real missing byte faults in this runtime's actual store only.
    dfs.content
        .debug_remove_loose_blob(&missing_stale_hash)
        .expect("remove later stale bytes");
    dfs.content
        .debug_remove_loose_blob(&current_hash)
        .expect("remove current bytes");
    let outcome = dfs
        .read_local_object_with_state(&readable_stale.object_id, true)
        .expect("stale read skips missing blob");
    assert!(
        matches!(&outcome,
        NetworkedDfsReadOutcome::Found { data, freshness: NetworkedDfsReadFreshness::Stale { .. } }
            if data.version == readable_stale.version
                && data.content_hash.as_deref() == Some(readable_hash.as_str())
                && data.bytes == readable_stale_bytes),
        "actual stale-read outcome: {outcome:?}"
    );
    drop(dfs);
    runtime.close().expect("ordinary native close");
}

#[test]
fn sifs_posix_create_bulk_local_eventual_avoids_per_file_requester_lookups() {
    let root = temp_root("sifs-posix-create-bulk-requester-lookups");
    let node_id =
        NodeId::from_legacy_named("node_sifs_create_bulk_requester_lookups").expect("node");
    let work = "/services/sandbox/instances/sandbox_create_bulk_requester_lookups/work";
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_create_bulk_local_eventual_avoids_per_file_requester_lookups",
        node_id,
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    let owned_files = (0..64)
        .map(|index| {
            (
                format!("{work}/file-{index:03}.txt"),
                format!("bulk-{index:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();

    crate::posix::reset_posix_metadata_raw_lookup_count();
    crate::posix::reset_posix_bulk_parent_touch_constructions();
    let stats = dfs
        .posix_create_files_local_eventual_with_known_parents(
            owned_files
                .iter()
                .map(|(path, bytes)| (path.as_str(), bytes.as_slice())),
        )
        .expect("bulk create");
    let raw_lookup_count = crate::posix::posix_metadata_raw_lookup_count();

    assert_eq!(stats.len(), owned_files.len());
    assert_eq!(
        crate::posix::posix_bulk_parent_touch_constructions(),
        1,
        "one shared parent must produce one parent-touch record for the complete bulk create"
    );
    let parent_component_count = work.trim_matches('/').split('/').count();
    let single_parent_walk_bound = parent_component_count + 1;
    assert!(
        raw_lookup_count <= single_parent_walk_bound,
        "bulk create performed {raw_lookup_count} raw point lookups for {} files; \
         resolving {parent_component_count} parent components once requires at most \
         {single_parent_walk_bound} lookups, and commit must reuse that terminal observation",
        owned_files.len(),
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_read_updates_atime_without_changing_mtime() {
    let root = temp_root("sifs-posix-read-atime");
    let node_id = NodeId::from_legacy_named("node_sifs_read_atime").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_read_atime/work/file.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_read_updates_atime_without_changing_mtime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_read_atime/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old times");
    let before = dfs.posix_stat_distributed(file_path).expect("before stat");

    assert_eq!(
        dfs.posix_read_file_update_access(file_path)
            .expect("read update access"),
        b"content"
    );

    let stat = dfs.posix_stat_distributed(file_path).expect("stat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);
    assert_ne!(
        stat.version, before.version,
        "atime update must advance the DFS metadata version so it persists across distributed reconciliation"
    );
    assert_eq!(
        dfs.posix_read_file_distributed(file_path)
            .expect("metadata-only version remains content-readable"),
        b"content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_dir_page_walks_large_directory_incrementally() {
    let root = temp_root("sifs-posix-list-page");
    let node_id = NodeId::from_legacy_named("node_sifs_list_page").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_page/work";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_list_dir_page_walks_large_directory_incrementally",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    for index in 0..5 {
        dfs.posix_write_file(
            &format!("{dir_path}/file-{index}.txt"),
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write");
    }

    let mut cursor = None;
    let mut names = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = dfs
            .posix_list_dir_distributed_page(dir_path, cursor.as_ref(), 2)
            .expect("list page");
        assert!(page.entries.len() <= 2);
        if let Some(next_cursor) = page.next_cursor.as_ref() {
            assert!(
                next_cursor.dentry.is_some(),
                "overlay-free backend pages must preserve the bounded dentry cursor"
            );
            assert!(
                next_cursor.shard_id.is_some(),
                "owner pages qualify their exact shard"
            );
            assert_ne!(
                cursor.as_ref(),
                Some(next_cursor),
                "owner page must advance its cursor"
            );
        }
        names.extend(page.entries.into_iter().map(|entry| entry.name));
        pages = pages.saturating_add(1);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    names.sort();
    assert_eq!(
        names,
        vec![
            "file-0.txt",
            "file-1.txt",
            "file-2.txt",
            "file-3.txt",
            "file-4.txt"
        ]
    );
    assert_eq!(pages, 3);

    let full_names = dfs
        .posix_list_dir_distributed(dir_path)
        .expect("full list")
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    assert_eq!(
        full_names,
        vec![
            "file-0.txt",
            "file-1.txt",
            "file-2.txt",
            "file-3.txt",
            "file-4.txt"
        ]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_dir_page_preserves_transient_overlays_across_pages() {
    let root = temp_root("sifs-posix-list-page-transient-overlays");
    let node_id = NodeId::from_legacy_named("node_sifs_list_page_overlays").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_page_overlays/work";

    let (_runtime, client) = native_sifs_service(
        "sifs_posix_list_dir_page_preserves_transient_overlays_across_pages",
        node_id.clone(),
    );
    drop(client);
    let mut dfs = _runtime
        .metadata_primary_service(dir_path)
        .expect("actual primary overlay owner");
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    let mut persisted = Vec::new();
    for index in 0..5 {
        persisted.push(
            dfs.posix_write_file(
                &format!("{dir_path}/file-{index}.txt"),
                b"content",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write"),
        );
    }

    let durable_object = dfs
        .metadata()
        .object(&persisted[0].object_id)
        .expect("durable object metadata")
        .clone();
    dfs.add_transient_object_metadata(durable_object)
        .expect("identical durable metadata observation");
    assert!(
        !dfs.transient_metadata_object_ids
            .contains(&persisted[0].object_id),
        "an observation identical to the durable namespace must not become a pagination overlay"
    );
    assert!(
        dfs.transient_metadata_object_ids.is_empty(),
        "objects already identical to durable backend state must not force overlay pagination"
    );

    let mut tombstone = dfs
        .metadata()
        .object(&persisted[2].object_id)
        .expect("persisted object metadata")
        .clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = DfsVersion::generate();
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    dfs.add_transient_object_metadata(tombstone.clone())
        .expect("transient tombstone");

    let mut transient = file_metadata(node_id.clone());
    transient.file_system_id = sifs_file_system_id();
    transient.object_id =
        DfsObjectId::from_legacy_named("obj_sifs_list_transient_child").expect("object id");
    transient.path = format!("{dir_path}/memory-only.txt");
    transient.version = DfsVersion::generate();
    transient.content_hash = None;
    transient.content_size_bytes = Some(0);
    dfs.add_transient_object_metadata(transient.clone())
        .expect("memory-only child");
    for index in 0..64 {
        let mut unrelated = file_metadata(node_id.clone());
        unrelated.file_system_id = sifs_file_system_id();
        unrelated.object_id =
            DfsObjectId::from_legacy_named(format!("obj_sifs_list_unrelated_transient_{index}"))
                .expect("unrelated object id");
        unrelated.path = format!("/unrelated/transient-{index}.txt");
        unrelated.version = DfsVersion::generate();
        unrelated.content_hash = None;
        unrelated.content_size_bytes = Some(0);
        dfs.add_transient_object_metadata(unrelated)
            .expect("unrelated transient child");
    }
    assert_eq!(dfs.transient_metadata_object_ids.len(), 66);

    crate::posix::reset_posix_metadata_full_directory_collect_count();
    crate::posix::reset_posix_transient_child_candidate_visit_count();
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    let probe_page = dfs
        .posix_list_dir_page(dir_path, None, 2)
        .expect("bounded overlay page probe");
    assert!(probe_page.next_cursor.is_some());
    let snapshot_calls = crate::metadata_backend::metadata_current_tables_snapshot_calls();
    assert!(
        snapshot_calls <= 11,
        "overlay merge opened {snapshot_calls} metadata table snapshots for a two-entry page; \
         durable entries must retain the cursors produced by the indexed page scan"
    );
    assert_eq!(
        crate::posix::posix_transient_child_candidate_visit_count(),
        2,
        "directory pagination must not inspect transient objects belonging to other parents"
    );

    let mut cursor = None;
    let mut names = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = dfs
            .posix_list_dir_page(dir_path, cursor.as_ref(), 2)
            .expect("overlay list page");
        assert!(page.entries.len() <= 2);
        if let Some(next_cursor) = page.next_cursor.as_ref() {
            assert!(
                next_cursor.dentry.is_some(),
                "overlay pages must retain the bounded backend dentry cursor"
            );
        }
        names.extend(page.entries.into_iter().map(|entry| entry.name));
        pages = pages.saturating_add(1);
        if pages == 1 {
            let mut settlement = _runtime.client(&node_id).unwrap();
            settlement
                .posix_unlink(&tombstone.path)
                .expect("normal transient tombstone settlement");
            settlement
                .posix_write_file(
                    &transient.path,
                    b"",
                    DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
                )
                .expect("normal transient child settlement");
            drop(settlement);
            dfs.transient_metadata_object_ids
                .remove(&tombstone.object_id);
            dfs.transient_metadata_object_ids
                .remove(&transient.object_id);
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    names.sort();
    assert_eq!(
        names,
        vec![
            "file-0.txt",
            "file-1.txt",
            "file-3.txt",
            "file-4.txt",
            "memory-only.txt",
        ]
    );
    assert_eq!(pages, 3);
    assert_eq!(
        crate::posix::posix_metadata_full_directory_collect_count(),
        0,
        "a transient overlay must not materialize and sort the complete directory on any page"
    );
    assert_eq!(
        crate::posix::posix_transient_child_candidate_visit_count(),
        4,
        "settled transient children must leave the parent index while unrelated overlays stay isolated"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_contract_rename_atomicity_with_active_readers() {
    let mut cluster = sifs_posix_cluster("sifs-contract-rename-readers", "/users");
    let scenario = "rename-atomicity-with-active-readers";
    let source = "/users/contract-rename-source.txt";
    let destination = "/users/contract-rename-destination.txt";
    let bytes = b"rename readers see one path".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, source, &bytes);
    sifs_cluster_publish_metadata_until(&mut cluster, "pre-rename observer", |cluster| {
        node_store_object(&cluster._runtime, &cluster.observer_node, source).is_some()
    });
    for (op, node, result) in [
        (
            "pre-source",
            cluster.observer_node.clone(),
            cluster.observer.posix_stat_distributed(source),
        ),
        (
            "pre-destination",
            cluster.observer_node.clone(),
            cluster.observer.posix_stat_distributed(destination),
        ),
    ] {
        record_posix_observation(
            &mut observations,
            scenario,
            op,
            &node,
            if op == "pre-source" {
                source
            } else {
                destination
            },
            Instant::now(),
            if result.is_ok() { "present" } else { "absent" },
            result.as_ref().ok(),
        );
    }

    cluster
        .primary
        .posix_rename(source, destination)
        .expect("rename");
    let primary_source = cluster.primary.posix_stat_distributed(source);
    let primary_destination = cluster.primary.posix_stat_distributed(destination);
    assert!(
        primary_source.is_err() && primary_destination.is_ok(),
        "primary must expose the rename atomically"
    );
    sifs_cluster_publish_metadata_until(&mut cluster, "post-rename observer", |cluster| {
        let runtime = &cluster._runtime;
        node_store_object(runtime, &cluster.observer_node, source).is_none()
            && node_store_object(runtime, &cluster.observer_node, destination).is_some()
    });
    let entries = node_store_children(&cluster._runtime, &cluster.observer_node, "/users")
        .into_iter()
        .map(|(name, _, _)| format!("/users/{name}"))
        .collect::<Vec<_>>();
    assert!(
        !entries.iter().any(|entry| entry == source)
            && entries.iter().any(|entry| entry == destination),
        "observer must not see both source and destination as live entries: {entries:?}"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_stale_cache_current_read_invalidation() {
    let mut cluster = sifs_posix_cluster("sifs-contract-stale-cache-current", "/users");
    let scenario = "stale-cache-current-read-invalidation";
    let path = "/users/contract-current-read.txt";
    let old = b"old cache content".to_vec();
    let new = b"new current content".to_vec();
    let mut observations = Vec::new();
    sifs_cluster_routed_posix_write(&mut cluster, path, &old);
    let old_stat = cluster.writer.posix_stat_distributed(path).unwrap();
    let received_old = cluster
        .writer
        .posix_read_file_distributed(path)
        .expect("normal distributed old-version read");
    assert_eq!(received_old, old);
    assert_eq!(old_stat.content_hash, Some(content_hash(&received_old)));
    let cached = cluster
        .writer
        .content
        .put_object_version(
            old_stat.object_id.as_str(),
            old_stat.version.as_str(),
            &received_old,
        )
        .expect("cache only the exact genuinely returned content generation");
    assert_eq!(Some(cached.content_hash), old_stat.content_hash);
    assert_eq!(
        cluster
            .writer
            .content
            .read_object_version(old_stat.object_id.as_str(), old_stat.version.as_str())
            .unwrap()
            .1,
        old
    );
    cluster
        .primary
        .posix_write_file(
            path,
            &new,
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        )
        .expect("primary overwrite");
    sifs_cluster_publish_metadata_until(&mut cluster, "writer learns new version", |cluster| {
        node_store_content_hash(&cluster._runtime, &cluster.writer_node, path)
            .is_some_and(|hash| hash == content_hash(&new))
    });
    let read = {
        let primary = &mut cluster.primary;
        let writer = &mut cluster.writer;
        sifs_cluster_blocking_distributed_read_with_pump(
            writer,
            path,
            "current distributed read",
            || {
                primary.process_incoming().expect("primary pump");
            },
        )
    };
    let stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("writer current stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "current-read",
        &cluster.writer_node,
        path,
        Instant::now(),
        "read",
        Some(&stat),
    );
    assert_eq!(
        read, new,
        "current reads must refresh stale cached content unless explicitly stale"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn exact_resident_hydration_replaces_only_its_shard_namespace_guards() {
    let name = "native-exact-shard-guard-snapshot";
    let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(name, 3).unwrap();
    let fs = sifs_file_system_id();
    let root_primary = nodes[0].clone();
    let services_primary = nodes[1].clone();
    let users_primary = nodes[2].clone();
    for (shard, primary) in [
        ("root", &root_primary),
        ("services", &services_primary),
        ("users", &users_primary),
    ] {
        let current = runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|config| config.shard_id == shard)
            .unwrap();
        let replicas = nodes
            .iter()
            .filter(|node| *node != primary)
            .cloned()
            .collect::<Vec<_>>();
        if current.primary_node_id != *primary
            || current
                .replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                != replicas.iter().cloned().collect::<BTreeSet<_>>()
        {
            runtime
                .reassign_metadata_shard(&fs, shard, primary.clone(), replicas)
                .unwrap();
        }
    }
    let root_path = "/native-root-guard-source";
    let root_target = "/users/native-root-guard-completed";
    let services_path = "/services/native-live-guard-source";
    let services_target = "/users/native-services-guard-target";
    for (path, owner) in [
        (root_path, &root_primary),
        (services_path, &services_primary),
    ] {
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
        policy.required_node_ids = vec![owner.clone()];
        runtime
            .apply_placement_policy(owner, path, DfsPlacementScope::Subtree, policy)
            .unwrap();
    }
    let mut root = runtime.client(&root_primary).unwrap();
    let mut services = runtime.client(&services_primary).unwrap();
    root.posix_write_file(
        root_path,
        b"actual root namespace job",
        DfsWriteOptions::replicated_eventual(vec![root_primary.clone()]),
    )
    .unwrap();
    services
        .posix_write_file(
            services_path,
            b"actual services namespace job",
            DfsWriteOptions::replicated_eventual(vec![services_primary.clone()]),
        )
        .unwrap();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(root_primary.clone()),
        target: Some(users_primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let root_operation = sinomo_core::Id::generate().to_string();
    assert!(
        root.posix_rename_with_operation_id(root_path, root_target, &root_operation)
            .is_err()
    );
    let root_job = runtime
        .node_metadata(&root_primary)
        .unwrap()
        .list_namespace_jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.root_path == root_path && job.target_path.as_deref() == Some(root_target))
        .unwrap();
    assert_eq!(root_job.status, DfsNamespaceJobStatus::Running);
    // Pin only a private inspection view. The real services-node Root replica
    // remains available to acknowledge every subsequent native publication.
    let inspection_root = default_dfs_metadata_shards_root(runtime.data_dir(&services_primary));
    let mut inspection = LocalShardedMetadataBackend::open_for_inspection(
        &inspection_root,
        runtime.metadata_shards().unwrap(),
    )
    .unwrap();
    inspection
        .shard_backend_mut(&DfsMetadataShardId::new("root"))
        .unwrap()
        .begin_inspection_snapshot()
        .unwrap();
    let mut probe = runtime
        .client(&services_primary)
        .unwrap()
        .with_cross_shard_namespace_lock_registry(Default::default())
        .with_boxed_metadata_backend_lazy(Box::new(inspection))
        .unwrap();
    assert!(
        matches!(
            probe.ensure_path_not_in_cross_shard_flight(&fs, root_path),
            Err(DfsError::Busy { .. })
        ),
        "the replica reconstructed its genuine Running root-shard job guard"
    );
    // Keep the older Root read image without suppressing any assigned
    // participant's journal or snapshot route. Full native completion still
    // requires all three real metadata replicas.
    runtime.clear_faults();
    let completed = root
        .posix_resume_namespace_job_bounded(&root_job.job_id, 512)
        .unwrap();
    assert_eq!(completed.status, DfsNamespaceJobStatus::Completed);
    // The job and its original failed routed request are separate durable
    // lifetimes. Finish that SAME request before starting the Services case.
    let replayed = root
        .posix_rename_with_operation_id(root_path, root_target, &root_operation)
        .unwrap();
    assert_eq!(replayed.path, root_target);
    let root_requests = runtime
        .metadata_primary_service(root_path)
        .unwrap()
        .metadata_backend()
        .unwrap()
        .list_namespace_requests()
        .unwrap();
    assert!(
        root_requests
            .iter()
            .filter(|request| matches!(&request.operation,
        DfsNamespaceOperation::Rename { from, to } if from == root_path && to == root_target))
            .all(|request| request.status == DfsNamespaceRequestStatus::Completed),
        "the original Root request must not remain for background replay: {root_requests:?}"
    );

    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(services_primary.clone()),
        target: Some(users_primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let services_operation = sinomo_core::Id::generate().to_string();
    assert!(
        services
            .posix_rename_with_operation_id(services_path, services_target, &services_operation)
            .is_err()
    );
    let services_job = runtime
        .node_metadata(&services_primary)
        .unwrap()
        .list_namespace_jobs()
        .unwrap()
        .into_iter()
        .find(|job| {
            job.root_path == services_path && job.target_path.as_deref() == Some(services_target)
        })
        .unwrap();
    assert_eq!(services_job.status, DfsNamespaceJobStatus::Running);
    // Refresh the private Services view while its Root inspection is pinned.
    // Only actual durable job rows populate this non-authoritative guard cache.
    probe.hydrate_file_systems_from_metadata_backend().unwrap();
    assert!(matches!(
        probe.ensure_path_not_in_cross_shard_flight(&fs, root_path),
        Err(DfsError::Busy { .. })
    ));
    assert!(matches!(
        probe.ensure_path_not_in_cross_shard_flight(&fs, services_path),
        Err(DfsError::Busy { .. })
    ));
    let config = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "root")
        .unwrap();
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    // Main delegated the completion to its actual owner; its previously
    // opened inspection backend is not that owner's new writer generation.
    // Capture the complete native image from the fresh Core at a coherent cut.
    let mut snapshot_owner = runtime.metadata_primary_service(root_path).unwrap();
    assert_eq!(snapshot_owner.node_id(), &root_primary);
    let backend = snapshot_owner.metadata_backend_mut().unwrap();
    backend
        .begin_consistent_read_snapshot_for_path(&fs, root_path)
        .unwrap();
    let exported = (|| -> Result<_, MetadataBackendError> {
        let actual = backend
            .get_namespace_job(&root_job.job_id)?
            .expect("actual completed Root namespace job at the export cut");
        assert_eq!(actual.status, DfsNamespaceJobStatus::Completed);
        let image = backend
            .export_shard_snapshot_image(&shard)?
            .expect("actual native Root image");
        image.validate()?;
        Ok(image)
    })();
    backend.end_consistent_read_snapshot_for_path(&fs, root_path);
    let image = exported.unwrap();
    drop(snapshot_owner);
    let command = DfsMetadataSnapshotInstallCommand {
        request_id: sinomo_core::Id::generate().to_string(),
        source_node_id: root_primary.clone(),
        file_system_id: Some(fs.clone()),
        authority_epoch: Some(config.epoch),
        primary_handoff_id: None,
        shard_id: shard,
        next_after_sequence: image.materialized_through_sequence,
        snapshot: image.descriptor().clone(),
        image: Some(image),
        reason: "normal exact root snapshot after actual namespace completion".into(),
    };
    let response = runtime
        .rpc_call(
            &root_primary,
            &services_primary,
            crate::dfs_metadata_snapshot_install_rpc_call(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let installed = crate::decode_dfs_metadata_snapshot_install_rpc_response(&response).unwrap();
    assert_eq!(installed.request_id, command.request_id);
    assert_eq!(installed.next_after_sequence, command.next_after_sequence);
    assert_eq!(installed.shard_id, command.shard_id);
    assert!(
        matches!(
            &installed.result,
            DfsMetadataSnapshotInstallResult::Installed { .. }
        ),
        "actual native Root snapshot response {installed:?}; exported cut {:?}",
        command.snapshot,
    );
    // Hydrate the exact Root inspection with the production resident-snapshot
    // method. This pool wraps the real read-only native files, holds no writer
    // lock, and never substitutes for the runtime's available receiver pool.
    probe
        .metadata_backend_mut()
        .unwrap()
        .end_consistent_read_snapshot_for_path(&fs, root_path);
    let root_inspection = LocalShardedMetadataBackend::open_for_inspection(
        &inspection_root,
        runtime.metadata_shards().unwrap(),
    )
    .unwrap()
    .into_resident();
    let root_handle = root_inspection.scope_shard(&fs, &command.shard_id).unwrap();
    let root_lease = root_handle.checkout().unwrap();
    // A fresh Main opener would rebind Services while its genuine Running
    // source-adoption job still owns writer custody. This private read-only
    // projection reuses the already-live probe's actual transport and owner.
    let mut exact_shell = NetworkedDfsService::new(
        probe.transport.fork_read_only_request(),
        probe.content.clone(),
    );
    exact_shell
        .publication_provider
        .clone_from(&probe.publication_provider);
    let mut exact_probe = exact_shell
        .with_resident_metadata_shard_backend_lazy(root_lease)
        .unwrap()
        .with_cross_shard_namespace_lock_registry(probe.cross_shard_namespace_lock_registry());
    // Attach first, then share the stale private registry, so only this exact
    // snapshot hydration is allowed to remove its old Root job guard.
    assert!(matches!(
        exact_probe.ensure_path_not_in_cross_shard_flight(&fs, root_path),
        Err(DfsError::Busy { .. })
    ));
    assert!(matches!(
        exact_probe.ensure_path_not_in_cross_shard_flight(&fs, services_path),
        Err(DfsError::Busy { .. })
    ));
    exact_probe
        .hydrate_lazy_exact_replica_after_snapshot_install()
        .unwrap();
    drop(exact_probe);
    drop(root_inspection);
    probe
        .ensure_path_not_in_cross_shard_flight(&fs, root_path)
        .unwrap();
    assert!(
        matches!(
            probe.ensure_path_not_in_cross_shard_flight(&fs, services_path),
            Err(DfsError::Busy { .. })
        ),
        "the real exact Root snapshot cannot erase another shard's live guard"
    );
    runtime.clear_faults();
    services
        .posix_resume_namespace_job_bounded(&services_job.job_id, 512)
        .unwrap();
    drop(probe);
    drop(services);
    drop(root);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_contract_damaged_replica_under_concurrent_reads() {
    let mut cluster = sifs_posix_cluster("sifs-contract-damaged-replica", "/services");
    let scenario = "damaged-replica-under-concurrent-reads";
    let path = "/services/contract-damaged.bin";
    let old = vec![0x44; 64 * 1024];
    let new = vec![0x55; 64 * 1024];
    let old_hash = content_hash(&old);
    let mut observations = Vec::new();
    let holders = vec![cluster.writer_node.clone(), cluster.primary_node.clone()];
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = holders.clone();
    placement.forbidden_node_ids = vec![cluster.observer_node.clone()];
    cluster
        ._runtime
        .apply_placement_policy(
            &cluster.primary_node,
            path,
            DfsPlacementScope::SelfOnly,
            placement,
        )
        .unwrap();
    // Open the ordinary requester after the committed policy change so its
    // retained native projection describes this fixture's real holder contract.
    cluster.writer = cluster._runtime.client(&cluster.writer_node).unwrap();
    let (_, _, replication) = cluster
        .writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    assert_eq!(replication.min_commit_replicas, 1);
    assert_eq!(replication.desired_replicas, 2);
    assert_eq!(
        replication
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        holders.iter().cloned().collect()
    );
    let initial_write = cluster.writer.posix_write_file(
        path,
        &old,
        DfsWriteOptions {
            policy: replication,
            replica_candidates: holders.clone(),
            wait_for_commit: true,
            foreground_replica_repair: true,
        },
    );
    let initial_request = match &initial_write {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "initial-replicated-write",
                &cluster.writer_node,
                path,
                Instant::now(),
                "committed",
                Some(stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        Err(error) => panic!("initial replicated write failed unexpectedly: {error}"),
    };
    sifs_posix_cluster_pump_until(
        &mut cluster,
        "initial replicated write settles",
        |cluster| {
            initial_request.as_ref().is_none_or(|intent| {
                !matches!(
                    cluster.writer.write_outcome(intent),
                    Ok(NetworkedDfsWriteOutcome::Pending { .. })
                )
            }) && cluster
                .writer
                .posix_stat_distributed(path)
                .is_ok_and(|stat| stat.content_hash.as_deref() == Some(old_hash.as_str()))
        },
    );
    sifs_posix_cluster_pump_until(&mut cluster, "primary gets old replica", |cluster| {
        cluster
            .primary
            .content()
            .read_blob(&old_hash)
            .is_ok_and(|bytes| bytes == old)
    });
    // W1 makes the write visible before N2 necessarily has two physical copies.
    // Freeze the actual old identity and earn the normal full content barrier
    // before damaging a replica; control-plane placement alone is not a copy.
    let old_target = cluster
        .writer
        .posix_capture_file_sync_target(path, DfsPosixSyncKind::File)
        .expect("capture the actual old committed content identity");
    assert_eq!(old_target.content_hash, old_hash);
    let synced = cluster
        .writer
        .posix_sync_file_target(&old_target)
        .expect("normal exact-version N2 content and metadata durability");
    assert_eq!(synced.target, old_target);
    assert_eq!(synced.content_evidence.required_receipts, 2);
    assert_eq!(
        synced
            .content_evidence
            .receipt_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        holders.iter().cloned().collect()
    );
    let old_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("actual old metadata after its full content barrier");
    assert_eq!(old_stat.object_id, old_target.object_id);
    assert_eq!(old_stat.version, old_target.version);
    assert_eq!(old_stat.content_hash.as_deref(), Some(old_hash.as_str()));
    assert_eq!(old_stat.size_bytes, Some(old.len() as u64));
    assert_eq!(
        old_stat
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        holders.iter().cloned().collect()
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "old-full-content-sync",
        &cluster.writer_node,
        path,
        Instant::now(),
        format!(
            "intent={} receipt_nodes={:?}",
            synced.content_evidence.intent_id, synced.content_evidence.receipt_node_ids
        ),
        Some(&old_stat),
    );

    let writer_store = crate::runtime_host::open_dfs_runtime_content_store(
        cluster._runtime.data_dir(&cluster.writer_node),
    )
    .expect("actual writer native content store");
    let primary_store = crate::runtime_host::open_dfs_runtime_content_store(
        cluster._runtime.data_dir(&cluster.primary_node),
    )
    .expect("actual primary native content store");
    for (node, store) in [
        (&cluster.writer_node, &writer_store),
        (&cluster.primary_node, &primary_store),
    ] {
        let actual = store.read_blob(&old_hash).unwrap_or_else(|error| {
            panic!("actual old content on holder {node} is unavailable: {error}")
        });
        assert_eq!(actual.len(), old.len());
        assert_eq!(content_hash(&actual), old_target.content_hash);
        assert_eq!(actual, old);
    }
    assert_eq!(
        cluster
            .writer
            .content
            .read_blob(&old_hash)
            .expect("requester content store matches its real native holder"),
        old
    );
    writer_store
        .debug_remove_loose_blob(&old_hash)
        .expect("damage the exact old writer-native payload");
    assert!(
        !writer_store
            .has_blob(&old_hash)
            .expect("writer old payload availability after damage")
    );
    assert_eq!(
        primary_store
            .read_blob(&old_hash)
            .expect("primary old bytes survive writer damage"),
        old
    );
    drop(writer_store);
    drop(primary_store);
    let new_head = cluster
        .primary
        .posix_write_file(
            path,
            &new,
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        )
        .expect("primary concurrent overwrite");
    assert_eq!(new_head.file_system_id, old_target.file_system_id);
    assert_eq!(new_head.object_id, old_target.object_id);
    assert_ne!(new_head.version, old_target.version);
    assert_eq!(
        new_head.content_hash.as_deref(),
        Some(content_hash(&new).as_str())
    );
    assert_eq!(new_head.size_bytes, Some(new.len() as u64));
    record_posix_observation(
        &mut observations,
        scenario,
        "primary-overwrite-returned",
        &cluster.primary_node,
        path,
        Instant::now(),
        "returned",
        Some(&new_head),
    );

    // A local client stat is not a current-head witness after another Main
    // writer publishes. Use the normal routed lookup under the original 10s
    // window, without treating transport/authority errors as retryable absence.
    let previous_deadline = cluster.writer.transport.operation_deadline();
    let read_deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    cluster.writer.set_transport_operation_deadline(Some(
        previous_deadline.map_or(read_deadline, |previous| previous.min(read_deadline)),
    ));
    let convergence = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sifs_cluster_publish_metadata_until(
            &mut cluster,
            "writer current lookup sees exact new head",
            |cluster| match cluster.writer.posix_stat_distributed(path) {
                Ok(stat) => {
                    stat.file_system_id == new_head.file_system_id
                        && stat.object_id == new_head.object_id
                        && stat.version == new_head.version
                        && stat.content_hash == new_head.content_hash
                        && stat.size_bytes == new_head.size_bytes
                }
                Err(DfsPosixError::NotFound { .. }) => false,
                Err(error) => panic!("current-head lookup failed without a retry waiver: {error}"),
            },
        );
    }));
    cluster
        .writer
        .set_transport_operation_deadline(previous_deadline);

    // Copy fresh physical DATA even on a convergence failure, then preserve the
    // original panic. No physical read/cut is promoted to publication authority.
    let physical_head = (|| -> Result<(
        DfsMetadataShardConfig, Option<DfsObjectMetadata>, Option<crate::NativeHeldMetadataSnapshotV1>
    ), String> {
        let scope = crate::select_metadata_shard_config_for_path(&new_head.file_system_id,
            &cluster._runtime.metadata_shards()?, path)
            .map_err(|error| error.to_string())?.ok_or("new head has no actual metadata scope")?;
        let owner = cluster._runtime.metadata_primary_service(path)?;
        let backend = owner.metadata_backend().ok_or("fresh owning Core has no backend")?;
        let row = backend.get_object_by_path(&new_head.file_system_id, path)
            .map_err(|error| error.to_string())?;
        let cut = backend.native_held_snapshot_for_scope(&scope.file_system_id,
            &DfsMetadataShardId::new(scope.shard_id.clone()));
        drop(owner);
        Ok((scope, row, cut))
    })();
    if let Err(original_panic) = convergence {
        eprintln!(
            "damaged-read convergence failure DATA: primary_return={new_head:?}; writer_node_store={:?}; fresh_physical_head={physical_head:?}",
            node_store_object(&cluster._runtime, &cluster.writer_node, path)
        );
        std::panic::resume_unwind(original_panic);
    }
    let (scope, row, cut) = physical_head.expect("fresh physical head inspection");
    assert_eq!(scope.primary_node_id, cluster.primary_node);
    let row = row.expect("actual new head is physically materialized");
    let cut = cut.expect("actual new head's loaded physical cut");
    assert_eq!(row.file_system_id, new_head.file_system_id);
    assert_eq!(row.object_id, new_head.object_id);
    assert_eq!(row.version, new_head.version);
    assert_eq!(row.content_hash, new_head.content_hash);
    assert_eq!(row.content_size_bytes, new_head.size_bytes);
    assert!(
        cut.tables.objects.contains(&row),
        "the exact new row must belong to this actual Core cut"
    );
    let read = {
        let primary = &mut cluster.primary;
        let writer = &mut cluster.writer;
        sifs_cluster_blocking_distributed_read_with_pump(
            writer,
            path,
            "read around damaged replica",
            || {
                primary.process_incoming().expect("primary pump");
            },
        )
    };
    let stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("writer current stat after read");
    assert_eq!(stat.file_system_id, new_head.file_system_id);
    assert_eq!(stat.object_id, new_head.object_id);
    assert_eq!(stat.version, new_head.version);
    assert_eq!(stat.content_hash, new_head.content_hash);
    assert_eq!(stat.size_bytes, new_head.size_bytes);
    record_posix_observation(
        &mut observations,
        scenario,
        "damaged-current-read",
        &cluster.writer_node,
        path,
        Instant::now(),
        "read",
        Some(&stat),
    );
    assert_eq!(
        read, new,
        "current read must avoid damaged/stale local replica and fetch valid current content"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_distributed_read_refreshes_metadata_after_local_location_prune() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-stale-reader-prune", "/services");
    let path = "/services/stale-reader-prune.bin";
    let bytes = (0..64 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_hash = content_hash(&bytes);

    match cluster.writer.posix_write_file(
        path,
        &bytes,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            replica_candidates: vec![cluster.writer_node.clone(), cluster.primary_node.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    ) {
        Ok(stat) => assert_eq!(
            stat.content_hash.as_deref(),
            Some(expected_hash.as_str()),
            "committed writer stat should expose the written content hash"
        ),
        Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("async routed write failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "writer observes async write", |cluster| {
        cluster
            .writer
            .posix_stat_distributed(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
    sifs_posix_cluster_pump_until(&mut cluster, "primary async repair", |cluster| {
        cluster
            .primary
            .retry_pending_repairs()
            .expect("primary repair tick");
        // The primary node's store and content store hold the repaired copy.
        node_store_content_hash(&cluster._runtime, &cluster.primary_node, path)
            .is_some_and(|hash| hash == expected_hash)
            && cluster
                .primary
                .content()
                .read_blob(&expected_hash)
                .is_ok_and(|read| read == bytes)
    });
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "writer learns repaired primary location",
        |cluster| {
            node_store_object(&cluster._runtime, &cluster.writer_node, path).is_some_and(|row| {
                row.current_locations.contains(&cluster.writer_node)
                    && row.current_locations.contains(&cluster.primary_node)
            })
        },
    );
    let stale_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("writer stale stat");

    cluster
        .writer
        .content
        .debug_remove_loose_blob(&expected_hash)
        .expect("remove writer loose blob");
    assert!(
        !cluster.writer.content.has_blob(&expected_hash).unwrap(),
        "fault injection must remove the writer's local readable content"
    );

    // The primary authors the prune on its own store.
    cluster
        ._runtime
        .metadata_primary_service(path)
        .unwrap()
        .mark_remote_read_location_missing(
            &stale_stat.object_id,
            &stale_stat.version,
            &stale_stat.content_hash,
            &cluster.writer_node,
        )
        .expect("normal primary prunes the actual missing content location");

    let (tx, rx) = std::sync::mpsc::channel();
    let distributed_read_path = path.to_string();
    let primary = &mut cluster.primary;
    let writer = &mut cluster.writer;
    let recovered = std::thread::scope(|scope| {
        scope.spawn(move || {
            let result = writer.posix_read_file_distributed(&distributed_read_path);
            tx.send(result).expect("send distributed read result");
        });
        let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
        loop {
            primary
                .process_incoming()
                .expect("primary handles distributed read");
            match rx.try_recv() {
                Ok(result) => break result.expect("distributed read after backend prune"),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("distributed read worker disconnected")
                }
            }
            if Instant::now() >= deadline {
                panic!("distributed read after backend prune did not complete");
            }
            std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        }
    });
    assert_eq!(recovered, bytes);
    assert!(
        cluster.rejections.is_empty(),
        "journal replication should remain clean: {:?}",
        cluster.rejections
    );

    drop(cluster);
}

#[test]
fn sifs_non_replica_node_reads_through_the_primary() {
    let cluster = sifs_outsider_cluster("sifs-outsider-primary-reads");
    let SifsPosixCluster {
        _runtime,
        root,
        primary_node,
        primary,
        mut observer,
        ..
    } = cluster;

    let path = "/users/primary-read.txt";
    let bytes = b"read through the primary".to_vec();
    let mut primary = primary;
    primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary local write");

    // The outsider keeps no copy of the shard; the distributed stat asks the
    // primary and surfaces the file.
    let stat = observer
        .posix_stat_distributed(path)
        .expect("outsider stat reads the primary");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));

    // Content reads fetch remotely as before.
    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("outsider distributed read"),
        bytes
    );

    // A genuinely missing path reports the primary's authoritative miss.
    assert!(matches!(
        observer.posix_stat_distributed("/users/never-created.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    drop(primary);
    drop(observer);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
}

#[test]
fn sifs_non_replica_prefix_read_reuses_authoritative_metadata_for_remote_range() {
    let cluster = sifs_outsider_cluster("sifs-outsider-prefix-range-read");
    let SifsPosixCluster {
        _runtime,
        root,
        primary_node,
        primary,
        mut observer,
        ..
    } = cluster;

    let path = "/users/prefix-range.txt";
    let empty_path = "/users/empty-prefix-range.txt";
    let corrupt_path = "/users/corrupt-prefix-range.txt";
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let corrupt_source_bytes = (0..(2 * 1024 * 1024 + 4096))
        .map(|index| ((index * 7) % 251) as u8)
        .collect::<Vec<_>>();
    let mut primary = primary;
    primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary local write");
    primary
        .posix_write_file(
            empty_path,
            &[],
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary empty-file write");
    let corrupt_stat = primary
        .posix_write_file(
            corrupt_path,
            &corrupt_source_bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary soon-corrupt file write");
    let corrupt_hash = corrupt_stat
        .content_hash
        .as_ref()
        .expect("corrupt-file hash");
    assert_eq!(corrupt_hash, &content_hash(&corrupt_source_bytes));
    // Corrupt a genuinely readable recorded holder, not a legacy fixture
    // directory or an assumed requester/primary content role.
    let holder = corrupt_stat
        .current_locations
        .iter()
        .find(|node| {
            let store =
                crate::runtime_host::open_dfs_runtime_content_store(_runtime.data_dir(node))
                    .expect("actual native content store");
            store
                .read_blob(corrupt_hash)
                .is_ok_and(|bytes| bytes == corrupt_source_bytes)
        })
        .expect("published content has an actual readable recorded holder");
    let corrupt_hex = corrupt_hash
        .strip_prefix("blake3:")
        .expect("blake3 corrupt-file hash");
    let corrupt_blob_path = _runtime
        .data_dir(holder)
        .dfs_dir()
        .join("content")
        .join("blobs")
        .join(&corrupt_hex[..2])
        .join(corrupt_hex);
    let mut corrupted_bytes = corrupt_source_bytes.clone();
    corrupted_bytes[0] ^= 0xff;
    std::fs::write(&corrupt_blob_path, &corrupted_bytes)
        .expect("corrupt primary blob without changing its size");

    let prefix = observer
        .posix_read_file_prefix_distributed(path, 23)
        .expect("outsider authoritative prefix read");
    assert_eq!(prefix.bytes, bytes[..23]);
    assert!(prefix.truncated);
    // The observer's caller keeps no local view, so the observer node's
    // content store is the witness that nothing was materialized.
    assert!(
        !observer.content().has_blob(&content_hash(&bytes)).unwrap(),
        "an uncached prefix read must not materialize the full remote object locally"
    );

    let opened = observer
        .posix_stat_distributed(path)
        .expect("outsider authoritative open metadata");
    let opened_range = observer
        .posix_read_file_range_for_opened_object(path, &opened.object_id, 17, 31, true)
        .expect("outsider opened-object remote range read");
    assert_eq!(opened_range, bytes[17..48]);
    assert!(
        !observer.content().has_blob(&content_hash(&bytes)).unwrap(),
        "an opened-object range read must not materialize the full remote object locally"
    );

    let complete = observer
        .posix_read_file_prefix_distributed(path, bytes.len())
        .expect("outsider exact-size complete range read");
    assert_eq!(complete.bytes, bytes);
    assert!(!complete.truncated);

    let empty = observer
        .posix_read_file_prefix_distributed(empty_path, 0)
        .expect("outsider zero-length prefix read");
    assert!(empty.bytes.is_empty());
    assert!(!empty.truncated);

    let corrupt_read = observer
        .posix_read_file_prefix_distributed(corrupt_path, corrupt_source_bytes.len())
        .expect("remote complete ranges do not rehash stored content");
    assert_eq!(corrupt_read.bytes, corrupted_bytes);

    drop(primary);
    drop(observer);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
}

// One foreground window only. A late completion constructed before this cursor
// cannot stand in for the read's newly constructed native carrier invocation.
fn assert_fresh_replica_lookup(
    runtime: &crate::test_runtime::DfsTestRuntime,
    capture: &crate::test_runtime::NativeNamespaceReadCapture,
    cursor: usize,
    source: &NodeId,
    primary: &NodeId,
    path: &str,
) {
    use crate::test_runtime::{RpcOrigin, RpcPhase};

    let records = capture.records();
    assert!(records.iter().any(|record| {
        record.source_node_id == *source
            && record.target_node_id == *primary
            && matches!(&record.message, DfsNetworkMessage::NamespaceRequest(request)
                if request.file_system_id == sifs_file_system_id()
                    && request.requesting_node_id == *source
                    && !request.include_metadata_journal_batches
                    && matches!(&request.operation, DfsNamespaceOperation::LookupPath { path: actual, .. } if actual == path))
    }), "the actual Main owner must receive this reader's exact path lookup");

    assert_eq!(runtime.observation_dropped_count(), 0);
    let observations = runtime.rpc_observations();
    let fresh = &observations[cursor..];
    assert!(
        fresh.iter().any(|constructed| {
            constructed.source == *source
                && constructed.target == *primary
                && constructed.route == DFS_DATA_NAMESPACE_ROUTE
                && constructed.origin == RpcOrigin::RegisteredHost
                && constructed.phase == RpcPhase::Constructed
                && [RpcPhase::BeforeDispatch, RpcPhase::Completed]
                    .into_iter()
                    .all(|phase| {
                        fresh.iter().any(|event| {
                            event.call_id == constructed.call_id
                                && event.source == *source
                                && event.target == *primary
                                && event.route == DFS_DATA_NAMESPACE_ROUTE
                                && event.origin == RpcOrigin::RegisteredHost
                                && event.phase == phase
                        })
                    })
        }),
        "one native lookup call must be constructed, dispatched and completed after this read's cursor"
    );
}

#[test]
fn sifs_replica_distributed_read_prefers_primary_over_stale_local_hit() {
    let mut cluster = sifs_posix_cluster("sifs-replica-read-primary-current", "/users");
    let path = "/users/read-stale-live.txt";
    let old = b"writer-local stale payload should lose to primary current payload".to_vec();
    let new = b"primary current payload must win the distributed read".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &old);
    let old_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("actual old authoritative metadata");
    let old_bytes = cluster
        .writer
        .posix_read_file_distributed(path)
        .expect("actual old distributed content");
    assert_eq!(old_bytes, old);
    assert_eq!(
        old_stat.content_hash.as_deref(),
        Some(content_hash(&old_bytes).as_str())
    );
    let old_object = {
        let owner = cluster
            ._runtime
            .metadata_primary_service(path)
            .expect("actual old owning Core read service");
        let object = owner
            .metadata_backend()
            .expect("actual old owning backend")
            .get_object_by_path(&sifs_file_system_id(), path)
            .expect("actual old path lookup")
            .expect("actual published old object");
        assert_eq!(object.object_id, old_stat.object_id);
        assert_eq!(object.version, old_stat.version);
        assert_eq!(object.content_hash, old_stat.content_hash);
        assert_eq!(object.content_size_bytes, Some(old_bytes.len() as u64));
        // Release the ordinary owner before the next Main publication.
        drop(owner);
        object
    };
    // Distributed reads deliberately do not cache. Bind only the real returned
    // version and verified bytes in the normal content store, without metadata
    // changes, replica receipts or a synthetic location.
    let cached = cluster
        .writer
        .content
        .put_object_version(
            old_stat.object_id.as_str(),
            old_stat.version.as_str(),
            &old_bytes,
        )
        .expect("cache genuine old content version");
    assert_eq!(
        Some(cached.content_hash.as_str()),
        old_stat.content_hash.as_deref()
    );
    assert_eq!(cached.size_bytes, old_bytes.len() as u64);
    let assert_opened_old_content = |writer: &NetworkedDfsService<DfsNodeTransport>| {
        let NetworkedDfsReadOutcome::Found { data, .. } = writer
            .read_object_metadata_local_content_with_state(&old_object, false)
            .expect("read the genuine opened old identity from local content")
        else {
            panic!("the actual opened old content must remain locally readable");
        };
        assert_eq!(data.object_id, old_object.object_id);
        assert_eq!(data.version, old_object.version);
        assert_eq!(data.content_hash, old_object.content_hash);
        assert_eq!(data.bytes, old);
    };
    assert_opened_old_content(&cluster.writer);

    sifs_cluster_primary_posix_write(&mut cluster, path, &new);
    assert_eq!(
        cluster
            .primary
            .posix_read_file_distributed(path)
            .expect("primary new read"),
        new
    );
    // The current public path view may advance. The stale-hit witness is the
    // exact previously opened metadata identity and its actual local bytes,
    // independent of any client-local path projection.
    assert_opened_old_content(&cluster.writer);

    let capture = cluster
        ._runtime
        .capture_namespace_requests(&cluster.primary_node, 32)
        .expect("actual primary namespace request window");
    let cursor = cluster._runtime.rpc_observations().len();
    let read = cluster
        .writer
        .posix_read_file_distributed(path)
        .expect("replica current read prefers primary object");
    assert_eq!(
        read, new,
        "distributed/current reads must prefer the shard primary over a stale replica-local hit"
    );
    assert_fresh_replica_lookup(
        &cluster._runtime,
        &capture,
        cursor,
        &cluster.writer_node,
        &cluster.primary_node,
        path,
    );

    let SifsPosixCluster {
        _runtime,
        primary,
        writer,
        observer,
        ..
    } = cluster;
    drop(primary);
    drop(writer);
    drop(observer);
    _runtime
        .close_with_namespace_capture(capture)
        .expect("normal Main and native owner drain with complete namespace coverage");
}

#[test]
fn sifs_replica_read_miss_queries_primary_without_pulling_metadata() {
    let mut cluster = sifs_posix_cluster("sifs-replica-read-no-sync-pull", "/users");
    let path = "/users/replica-read-no-sync.txt";
    let bytes = b"replica reads query primary without local metadata writes".to_vec();
    sifs_cluster_primary_posix_write(&mut cluster, path, &bytes);
    // Let ordinary publication settle on the reader node first, so its store
    // can only change during the read if the read itself replays metadata.
    sifs_cluster_publish_metadata_until(&mut cluster, "reader node store settles", |cluster| {
        node_store_content_hash(&cluster._runtime, &cluster.observer_node, path)
            .is_some_and(|hash| hash == content_hash(&bytes))
    });
    let SifsPosixCluster {
        _runtime,
        primary_node,
        observer_node,
        primary,
        mut observer,
        ..
    } = cluster;

    let selected = crate::select_metadata_shard_config_for_path(
        &sifs_file_system_id(),
        &_runtime
            .metadata_shards()
            .expect("actual current configuration"),
        path,
    )
    .expect("actual path assignment")
    .expect("assigned physical shard");
    assert_eq!(selected.primary_node_id, primary_node);
    assert!(selected.replica_node_ids.contains(&observer_node));
    assert!(
        !observer.content().has_blob(&content_hash(&bytes)).unwrap(),
        "the reader must genuinely miss local content before the distributed read"
    );
    assert!(observer.metadata_backend().is_none());
    let shard = DfsMetadataShardId::new(selected.shard_id.clone());
    // The reader node's own store, which only its Main writes.
    let node_store = || {
        _runtime
            .node_metadata(&observer_node)
            .expect("actual reader node store")
            .export_shard_table_snapshot(&shard)
            .expect("actual replica shard export")
            .expect("the reader node replicates the assigned shard")
    };
    let before = node_store();
    let capture = _runtime
        .capture_namespace_requests(&primary_node, 32)
        .expect("actual primary namespace request window");
    let cursor = _runtime.rpc_observations().len();
    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("replica miss queries primary and fetches content"),
        bytes
    );
    let after = node_store();
    assert_eq!(
        (after.prefix_proof, after.semantic_state_fingerprint),
        (before.prefix_proof, before.semantic_state_fingerprint),
        "distributed reads must not replay metadata into the reader node's store"
    );
    assert_fresh_replica_lookup(
        &_runtime,
        &capture,
        cursor,
        &observer_node,
        &primary_node,
        path,
    );
    // A routed lookup may expose an in-memory object. posix_stat is therefore
    // not a witness of physical replay or of physical namespace absence.
    drop(primary);
    drop(observer);
    _runtime
        .close_with_namespace_capture(capture)
        .expect("normal Main and native owner drain with complete namespace coverage");
}

#[test]
fn native_replica_read_recovers_after_an_actual_content_location_is_lost() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-lost-read-location", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-lost-read-location", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-lost-read-location.bin";
    let bytes = vec![0x72; 32 * 1024];
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 2).unwrap();
    placement.required_node_ids = vec![nodes[0].clone(), nodes[1].clone()];
    let accepted = runtime
        .apply_placement_policy(&nodes[0], path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert_eq!(job.scanned_object_count, 0);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "actual empty-path Apply must finish within its existing network budget"
        );
        runtime
            .maintenance_tick(&nodes[0], remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    let mut primary = runtime.client(&nodes[0]).unwrap();
    let (_, _, policy) = primary
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    assert_eq!(
        policy
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([nodes[0].clone(), nodes[1].clone()])
    );
    let stat = primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                policy,
                replica_candidates: vec![nodes[0].clone(), nodes[1].clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    primary
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    drop(primary);
    let missing = runtime.client(&nodes[1]).unwrap();
    let hash = stat.content_hash.as_deref().unwrap();
    assert_eq!(
        missing.content().read_blob(hash).unwrap(),
        bytes,
        "the selected replica really holds the original 32KiB payload"
    );
    missing.content().debug_remove_loose_blob(hash).unwrap();
    assert!(
        missing.content().open_blob_stream(hash).is_err(),
        "the actual selected replica storage path is unavailable"
    );
    drop(missing);
    let mut observer = runtime.client(&nodes[2]).unwrap();
    assert!(
        !observer.content().has_blob(hash).unwrap(),
        "the reader starts without a local copy"
    );
    let shard = DfsMetadataShardId::new(runtime.root_shard().unwrap().shard_id);
    // The reader node's own store; the delegating reader keeps none.
    let node_store = || {
        runtime
            .node_metadata(&nodes[2])
            .unwrap()
            .export_shard_table_snapshot(&shard)
            .unwrap()
            .unwrap()
    };
    let before = node_store();
    assert_eq!(observer.posix_read_file_distributed(path).unwrap(), bytes);
    let after = node_store();
    assert_eq!(after.prefix_proof, before.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        before.semantic_state_fingerprint
    );
    drop(observer);
    runtime.close().unwrap();
}

#[test]
fn dfs_message_classifier_keeps_lookup_connectivity_observing() {
    let local = NodeId::from_legacy_named("node_read_classifier_local").expect("local");
    let remote = NodeId::from_legacy_named("node_read_classifier_remote").expect("remote");
    let file_system_id = sifs_file_system_id();
    let read = test_dfs_message_command_from_message(
        &local,
        &remote,
        1,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "read-request".to_string(),
            object_id: DfsObjectId::from_legacy_named("read_object").expect("object id"),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    assert!(dfs_message_is_read_only_request(&read));

    let hinted_read = test_dfs_message_command_from_message(
        &local,
        &remote,
        2,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "hinted-read-request".to_string(),
            object_id: DfsObjectId::from_legacy_named("hinted_read_object").expect("object id"),
            version: Some(DfsVersion::from_legacy_named("hinted_read_version").expect("version")),
            content_hash: Some("blake3:hinted-read".to_string()),
            content_size_bytes: Some(12),
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    assert!(dfs_message_is_read_only_request(&hinted_read));

    let lookup = test_dfs_message_command_from_message(
        &local,
        &remote,
        3,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "lookup-request".to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: local.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::LookupPath {
                path: "/services/example.txt".to_string(),
                inline_range: None,
            },
        }),
    );
    assert!(dfs_message_is_read_only_request(&lookup));

    let write = test_dfs_message_command_from_message(
        &local,
        &remote,
        6,
        DfsNetworkMessage::DistributedWriteRequest(test_routed_write_command(
            "write-request",
            &local,
            &DfsObjectId::from_legacy_named("write_object").expect("object id"),
        )),
    );
    assert!(!dfs_message_is_read_only_request(&write));

    let lookup_paths = test_dfs_message_command_from_message(
        &local,
        &remote,
        4,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "lookup-paths-request".to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: local.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::LookupPaths {
                paths: vec![
                    "/services/example-a.txt".to_string(),
                    "/services/example-b.txt".to_string(),
                ],
            },
        }),
    );
    assert!(dfs_message_is_read_only_request(&lookup_paths));

    let apply = test_dfs_message_command_from_message(
        &local,
        &remote,
        5,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "apply-request".to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: local.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::CommitLocalEventualBatch {
                transaction: DfsNamespaceTransaction {
                    content_reference_identity: None,
                    extension_expectations: Vec::new(),
                    contribution_digest: None,
                    file_version_pins: None,
                    transaction_id: "bulk-transaction".to_string(),
                    file_system_id,
                    objects: Vec::new(),
                    versions: Vec::new(),
                    advisory_lock_states: Vec::new(),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                },
                writes: Vec::new(),
            },
        }),
    );
    assert!(!dfs_message_is_read_only_request(&apply));
}
