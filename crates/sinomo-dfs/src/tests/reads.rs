//! Reads regression tests.

use super::*;

#[test]
fn content_addressed_read_requests_do_not_require_full_metadata_hydration() {
    let source = NodeId::from_legacy_named("node_protocol_fresh_source").expect("source");
    let target = NodeId::from_legacy_named("node_protocol_fresh_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_protocol_fresh").expect("object");
    let version = DfsVersion::from_legacy_named("version_protocol_fresh").expect("version");
    let read = test_dfs_message_command_from_message(
        &source,
        &target,
        1,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "dfs_read_fresh_metadata".to_string(),
            object_id: object_id.clone(),
            version: Some(version.clone()),
            content_hash: Some("blake3:readfresh".to_string()),
            content_size_bytes: Some(12),
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    let bulk = test_dfs_message_command_from_message(
        &source,
        &target,
        2,
        DfsNetworkMessage::BulkReadRequest(DfsBulkReadRequest {
            request_id: "dfs_bulk_read_fresh_metadata".to_string(),
            reads: vec![DfsBulkReadItem {
                request_id: "dfs_bulk_read_item".to_string(),
                object_id,
                version,
                content_hash: "blake3:bulkreadfresh".to_string(),
                content_size_bytes: 12,
            }],
        }),
    );

    assert!(!dfs_message_requires_full_metadata_hydrate(&read));
    assert!(!dfs_message_requires_full_metadata_hydrate(&bulk));
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
    assert!(
        !dfs_message_requires_full_metadata_hydrate(&read),
        "legacy unhinted reads use a targeted authoritative object lookup"
    );

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
    assert!(!dfs_message_requires_full_metadata_hydrate(&hinted_read));

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
    assert!(
        !dfs_message_requires_full_metadata_hydrate(&lookup),
        "lookup requests use targeted backend path reads instead of full daemon hydration"
    );

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
    assert!(
        !dfs_message_requires_full_metadata_hydrate(&write),
        "routed writes validate target metadata in the write path instead of forcing a full daemon hydrate"
    );

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
    assert!(
        !dfs_message_requires_full_metadata_hydrate(&lookup_paths),
        "batched lookup requests use targeted backend path reads instead of full daemon hydration"
    );

    let sync = test_dfs_message_command_from_message(
        &local,
        &remote,
        3,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "sync-request".to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: local.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::SyncPath {
                path: "/services/example.txt".to_string(),
            },
        }),
    );
    assert!(!dfs_message_is_read_only_request(&sync));
    assert!(
        !dfs_message_requires_full_metadata_hydrate(&sync),
        "sync requests use targeted backend path reads instead of full daemon hydration"
    );

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
    assert!(
        !dfs_message_requires_full_metadata_hydrate(&apply),
        "namespace transactions validate and apply through the namespace path"
    );
}

#[test]
fn dfs_direct_read_peer_transport_adapters_observe_connectivity() {
    let remote = NodeId::from_legacy_named("node_dfs_direct_read_observe_remote").expect("remote");
    let expected_bytes = [1, 2, 3];
    let request = DfsDirectReadRequest {
        request_id: "direct-observe-1".to_string(),
        object_id: DfsObjectId::generate(),
        version: DfsVersion::generate(),
        content_hash: content_hash(&expected_bytes),
        content_size_bytes: expected_bytes.len() as u64,
        range_offset: None,
        range_max_bytes: None,
    };

    let peer = Arc::new(RecordingPeerTransport::default());
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let direct_sender = dfs_direct_read_sender_from_peer_transport(peer_transport);
    direct_sender(
        &remote,
        &request,
        DfsObjectRpcSendOptions::new(Duration::from_secs(1)),
    )
    .expect("direct read send");
    assert_eq!(
        peer.options.lock().expect("options").as_slice(),
        &[(DFS_DIRECT_READ_ROUTE.to_string(), true)]
    );

    let async_peer = Arc::new(RecordingPeerTransport::default());
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = async_peer.clone();
    let async_direct_sender =
        async_dfs_direct_read_sender_from_peer_transport(async_peer_transport);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    runtime
        .block_on(async {
            async_direct_sender(
                &remote,
                &request,
                DfsObjectRpcSendOptions::new(Duration::from_secs(1)),
            )
            .await
        })
        .expect("async direct read send");
    assert_eq!(
        async_peer.options.lock().expect("options").as_slice(),
        &[(DFS_DIRECT_READ_ROUTE.to_string(), true)]
    );
}

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
    assert!(completion.verified_content_hash.is_none());
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

#[tokio::test]
async fn stream_native_read_is_bounded_for_direct_and_relayed_results() {
    const CHUNK_BYTES: usize = 32 * 1024;
    const HEAD_TIMEOUT: Duration = Duration::from_millis(100);
    const FIRST_CHUNK_TIMEOUT: Duration = Duration::from_millis(100);

    for relay_hops in [0_usize, 1] {
        let local = NodeId::from_legacy_named(format!("node_stream_read_source_{relay_hops}"))
            .expect("local");
        let remote = NodeId::from_legacy_named(format!("node_stream_read_target_{relay_hops}"))
            .expect("remote");
        let object_id = DfsObjectId::from_legacy_named(format!("object_stream_read_{relay_hops}"))
            .expect("object");
        let version = DfsVersion::from_legacy_named(format!("version_stream_read_{relay_hops}"))
            .expect("version");
        let chunks = (0..3)
            .map(|chunk_index| {
                (0..CHUNK_BYTES)
                    .map(|byte_index| ((chunk_index * 37 + byte_index) % 251) as u8)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expected = chunks.concat();
        let expected_hash = content_hash(&expected);

        let (source_sender, endpoint_capacity, endpoint_body, relay_task) = if relay_hops == 0 {
            let (sender, body) =
                ObjectRpcIncomingBody::test_channel(Some(expected.len() as u64), 1);
            (sender.clone(), sender, body, None)
        } else {
            let (source_sender, mut relay_input) =
                ObjectRpcIncomingBody::test_channel(Some(expected.len() as u64), 1);
            let (relay_sender, endpoint_body) =
                ObjectRpcIncomingBody::test_channel(Some(expected.len() as u64), 1);
            let endpoint_capacity = relay_sender.clone();
            // Model the opaque one-hop relay as an ownership transfer
            // between bounded records. Production relay buffer identity is
            // covered in sinomo-network; this test covers the DFS-facing
            // stream contract on top of that topology.
            let relay_task = tokio::spawn(async move {
                while let Some(chunk) =
                    relay_input
                        .next_chunk()
                        .await
                        .map_err(|source| NetworkError::Io {
                            operation: "read relayed DFS test chunk".to_string(),
                            detail: source.to_string(),
                        })?
                {
                    relay_sender.send_chunk(chunk).await?;
                }
                Ok::<(), NetworkError>(())
            });
            (
                source_sender,
                endpoint_capacity,
                endpoint_body,
                Some(relay_task),
            )
        };

        let transport =
            node_transport_with_streamed_direct_read_body(local, remote.clone(), endpoint_body);
        let request = DfsDirectReadRequest {
            request_id: format!("stream-read-{relay_hops}"),
            object_id,
            version,
            content_hash: expected_hash.clone(),
            content_size_bytes: expected.len() as u64,
            range_offset: None,
            range_max_bytes: None,
        };

        // The response head must arrive before the producer has supplied any
        // body bytes. This is the observable time-to-first-chunk boundary: the
        // caller can begin polling the body rather than waiting for collection.
        let response = tokio::time::timeout(
            HEAD_TIMEOUT,
            transport.read_object_stream(&remote, &request),
        )
        .await
        .expect("stream response head must not wait for the body")
        .expect("stream response head");
        let DfsDirectReadClientResult::Found(mut data) = response.result else {
            panic!("expected found stream for {relay_hops}-hop read");
        };
        assert_eq!(data.stream.observed_size_bytes(), 0);

        assert!(
            source_sender
                .try_send(chunks[0].clone())
                .expect("enqueue first test chunk"),
            "the empty source ring must accept the first chunk"
        );
        tokio::time::timeout(FIRST_CHUNK_TIMEOUT, async {
            while endpoint_capacity.available_chunk_capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first chunk must reach the DFS-facing bounded ring");
        assert!(
            !endpoint_capacity
                .try_send(vec![0; CHUNK_BYTES])
                .expect("probe bounded endpoint ring"),
            "the DFS-facing stream must not buffer a second chunk past its configured bound"
        );

        let first_chunk_started = Instant::now();
        let first = tokio::time::timeout(FIRST_CHUNK_TIMEOUT, data.stream.next_chunk())
            .await
            .expect("first DFS chunk time")
            .expect("read first DFS chunk")
            .expect("non-empty DFS body");
        let time_to_first_chunk = first_chunk_started.elapsed();
        assert_eq!(first.as_ref(), chunks[0].as_slice());
        assert!(
            time_to_first_chunk < FIRST_CHUNK_TIMEOUT,
            "first chunk exceeded the functional timeout"
        );

        drop(endpoint_capacity);
        let remaining_chunks = chunks[1..].to_vec();
        let producer = tokio::spawn(async move {
            for chunk in remaining_chunks {
                source_sender.send(chunk).await?;
            }
            Ok::<(), NetworkError>(())
        });
        let completion = data.stream.finish().await.expect("DFS length finalizer");
        producer
            .await
            .expect("join test body producer")
            .expect("produce test body");
        if let Some(relay_task) = relay_task {
            relay_task
                .await
                .expect("join one-hop relay")
                .expect("forward one-hop body");
        }
        assert_eq!(completion.content_size_bytes, expected.len());
        assert!(completion.verified_content_hash.is_none());
    }
}

#[test]
fn node_transport_read_message_uses_direct_sender_and_queues_response() {
    let local = NodeId::from_legacy_named("node_dfs_node_route_direct_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_node_route_direct_remote").expect("remote");
    let object_id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let bytes = b"direct node_route read bytes".to_vec();
    let direct_calls = Arc::new(Mutex::new(Vec::new()));
    let message_send_calls = Arc::new(Mutex::new(0));
    let mut transport = node_transport_with_direct_read_sender(
        local.clone(),
        remote.clone(),
        bytes.clone(),
        None,
        Arc::clone(&direct_calls),
        Arc::clone(&message_send_calls),
    );
    let message = DfsRoutedMessage {
        source_node_id: local.clone(),
        target_node_id: remote.clone(),
        frame_id: 7,
        message_id: 9,
        message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "node_route-direct-read".to_string(),
            object_id: object_id.clone(),
            version: Some(version.clone()),
            content_hash: Some("blake3:node_route-direct".to_string()),
            content_size_bytes: Some(bytes.len() as u64),
            range_offset: None,
            range_max_bytes: None,
        }),
    };

    transport
        .send_routed_call(&remote, DfsRoutedCall::value(message))
        .expect("send routed read message");

    let calls = direct_calls.lock().expect("direct calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].object_id, object_id);
    assert_eq!(calls[0].version, version);
    assert_eq!(calls[0].content_hash, "blake3:node_route-direct");
    assert_eq!(calls[0].content_size_bytes, bytes.len() as u64);
    drop(calls);
    assert_eq!(*message_send_calls.lock().expect("message send calls"), 0);
    assert_eq!(
        transport
            .pending_incoming_count()
            .expect("pending incoming messages"),
        Some(1)
    );
    let messages = transport
        .drain_incoming_messages()
        .expect("incoming messages");
    assert_eq!(messages.len(), 1);
    assert!(matches!(
        &messages[0].message,
        DfsNetworkMessage::ReadResponse(DfsReadResponse {
            result: DfsReadResult::Found(DfsReadData {
                object_id: response_object_id,
                version: response_version,
                bytes: response_bytes,
                ..
            }),
            ..
        }) if response_object_id == &object_id && response_version == &version && response_bytes == &bytes
    ));
}

#[test]
fn node_transport_async_read_message_uses_direct_sender_and_queues_response() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let local = NodeId::from_legacy_named("node_dfs_node_route_async_direct_local")
            .expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_node_route_async_direct_remote")
            .expect("remote");
        let object_id = DfsObjectId::from_legacy_named("object_async_node_route_direct")
            .expect("object");
        let version = DfsVersion::from_legacy_named("version_async_node_route_direct")
            .expect("version");
        let bytes = b"async direct node_route read bytes".to_vec();
        let bytes_hash = content_hash(&bytes);
        let direct_calls = Arc::new(Mutex::new(Vec::new()));
        let direct_calls_for_sender = Arc::clone(&direct_calls);
        let bytes_for_sender = bytes.clone();
        let async_direct_sender: AsyncDfsDirectReadSender =
            Arc::new(move |_target_node_id, request, _timeout| {
                let direct_calls_for_sender = Arc::clone(&direct_calls_for_sender);
                let request = request.clone();
                let bytes_for_sender = bytes_for_sender.clone();
                Box::pin(async move {
                    direct_calls_for_sender
                        .lock()
                        .expect("direct calls")
                        .push(request.clone());
                    Ok(DfsDirectReadClientResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: DfsDirectReadClientResult::Found(
                            DfsDirectReadClientData {
                                content_size_bytes: bytes_for_sender.len(),
                                stream: DfsVerifiedReadStream::new(
                                    sinomo_core::network::ObjectRpcIncomingBody::bytes(
                                        bytes_for_sender,
                                    ),
                                    request.content_size_bytes as usize,
                                    Some(request.content_hash.clone()),
                                )
                                .expect("valid direct-read stream"),
                                object_id: request.object_id.clone(),
                                version: request.version.clone(),
                                content_hash: request.content_hash.clone(),
                            },
                        ),
                    })
                })
            });
        let message_send_calls = Arc::new(Mutex::new(0usize));
        let message_send_calls_for_sender = Arc::clone(&message_send_calls);
        let async_sender: AsyncDfsMessageSender = Arc::new(move |_target_node_id, _command, _timeout| {
            let message_send_calls_for_sender = Arc::clone(&message_send_calls_for_sender);
            Box::pin(async move {
                *message_send_calls_for_sender
                    .lock()
                    .expect("message send calls") += 1;
                Err(DfsPeerTransportError::new(
                    "async message sender should not be used by direct-read-eligible messages",
                ))
            })
        });
        let sync_sender: DfsMessageSender = Arc::new(|_target_node_id, _command, _timeout| {
            Err(DfsPeerTransportError::new(
                "sync message sender should not be used by async direct-read tests",
            ))
        });
        let sync_direct_sender: DfsDirectReadSender = Arc::new(|_target_node_id, _request, _timeout| {
            Err(DfsPeerTransportError::new(
                "sync direct sender should not be used by async direct-read tests",
            ))
        });
        let mut transport = DfsNodeTransport::new_with_message_peer_senders(
            local.clone(),
            vec![remote.clone()],
            sync_sender,
            async_sender,
            sync_direct_sender,
            async_direct_sender,
        );
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 17,
            message_id: 19,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "node_route-async-direct-read".to_string(),
                object_id: object_id.clone(),
                version: Some(version.clone()),
                content_hash: Some(bytes_hash.clone()),
                content_size_bytes: Some(bytes.len() as u64),
                range_offset: None,
                range_max_bytes: None,
            }),
        };

        AsyncDfsMessageTransport::send_routed_call_async(
            &mut transport,
            &remote,
            DfsRoutedCall::value(message),
        )
            .await
            .expect("send async routed read message");

        {
            let calls = direct_calls.lock().expect("direct calls");
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].object_id, object_id);
            assert_eq!(calls[0].version, version);
            assert_eq!(calls[0].content_hash, bytes_hash);
            assert_eq!(calls[0].content_size_bytes, bytes.len() as u64);
        }
        assert_eq!(*message_send_calls.lock().expect("message send calls"), 0);
        let messages = AsyncDfsMessageTransport::drain_incoming_messages_async(&mut transport)
            .await
            .expect("incoming messages");
        assert_eq!(messages.len(), 1);
        assert!(matches!(
            &messages[0].message,
            DfsNetworkMessage::ReadResponse(DfsReadResponse {
                result: DfsReadResult::Found(DfsReadData {
                    object_id: response_object_id,
                    version: response_version,
                    bytes: response_bytes,
                    ..
                }),
                ..
            }) if response_object_id == &object_id && response_version == &version && response_bytes == &bytes
        ));
    });
}

#[test]
fn read_only_backend_attachment_skips_bootstrap_scans_and_serves_targeted_reads() {
    let root = temp_root("read-only-backend-attachment");
    let node_id = NodeId::from_legacy_named("node_read_only_attachment").expect("node");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = DfsFileSystem::sifs(node_id.clone());
    let extra_file_system = DfsFileSystem {
        file_history: Default::default(),
        file_system_id: DfsFileSystemId::from_legacy_named("read-only-extra-fs")
            .expect("extra filesystem"),
        display_name: "Must not be bootstrapped".to_string(),
        owner_node_id: node_id.clone(),
        subtree_policies: Vec::new(),
    };
    let mut object = file_metadata(node_id.clone());
    object.object_id =
        DfsObjectId::from_legacy_named("object_read_only_attachment").expect("object");
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/read-only-attachment.txt".to_string();
    object.version =
        DfsVersion::from_legacy_named("version_read_only_attachment").expect("version");
    object.content_hash = Some("blake3:read-only-attachment".to_string());
    object.content_size_bytes = Some(17);

    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("SIFS filesystem");
    backend
        .put_file_system(&extra_file_system)
        .expect("extra filesystem");
    backend.put_object(&object).expect("object metadata");
    backend
        .put_namespace_job(&DfsNamespaceJobRecord {
            job_id: "job_read_only_attachment".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/services/read-only-attachment.txt".to_string(),
            target_path: Some("/users/read-only-attachment.txt".to_string()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        })
        .expect("running namespace job");

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut service = NetworkedDfsService::new(network.add_node(node_id.clone()), content)
        .with_boxed_metadata_backend_for_read_only_requests(Box::new(backend));

    assert_eq!(
        service.metadata.filesystems.len(),
        1,
        "read-only attachment must retain only the constructor's SIFS identity"
    );
    assert!(
        !service
            .metadata
            .filesystems
            .contains_key(&extra_file_system.file_system_id),
        "read-only attachment must not enumerate the durable filesystem table"
    );
    assert!(service.metadata.objects.is_empty());
    assert!(
        service.cross_shard_namespace_locks.is_empty(),
        "read-only attachment must not scan running namespace jobs"
    );
    assert!(
        service
            .replication
            .local_metadata_journal_cursors
            .is_empty(),
        "read-only attachment must not scan every shard head"
    );

    let looked_up = service
        .authoritative_metadata_object_for_path(&file_system.file_system_id, &object.path)
        .expect("targeted path lookup")
        .expect("durable path object");
    assert_eq!(looked_up.object_id, object.object_id);
    let read = service
        .current_read_object_metadata(&object.object_id)
        .expect("targeted object read");
    assert_eq!(read.version, object.version);

    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let file_system_config = DfsFileSystemConfig {
        file_history: Default::default(),
        file_system_id: file_system.file_system_id.clone(),
        display_name: file_system.display_name.clone(),
        owner_node_id: node_id,
        subtree_policies: file_system.subtree_policies.clone(),
    };
    let request = DfsContentRootCatalogPageRequest {
        identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
        expected_topology_digest: dfs_content_root_catalog_topology_digest_with_file_systems(
            std::slice::from_ref(&file_system_config),
            std::slice::from_ref(&shard),
        )
        .expect("topology digest"),
        minimum_journal_sequence: 0,
        minimum_state_fingerprint: None,
        after: None,
        max_entries: 32,
    };
    let page = service
        .metadata_backend
        .as_mut()
        .expect("attached metadata backend")
        .content_root_catalog_page(&request)
        .expect("targeted content-root catalog page");
    assert!(page.is_complete());
    assert!(page.entries.contains(&DfsContentRootCatalogEntry::Version(
        DfsMetadataVersionIdentity::new(object.object_id.clone(), object.version.clone())
    )));
    assert!(
        page.entries
            .contains(&DfsContentRootCatalogEntry::ContentHash(
                object.content_hash.expect("content hash")
            ))
    );
}

#[test]
fn mkdir_all_existing_deep_chain_uses_linear_metadata_lookups() {
    let root = temp_root("mkdir-all-linear-lookups");
    let node = NodeId::from_legacy_named("node_mkdir_all_linear_lookups").expect("node id");
    let mut dfs =
        open_shared_metadata_posix_service(&root, &root.join("metadata"), node, "content");
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
    let root = temp_root("no-link-beneath-primitives");
    let node = NodeId::from_legacy_named("node_no_link_beneath").expect("node id");
    let mut dfs =
        open_shared_metadata_posix_service(&root, &root.join("metadata"), node.clone(), "content");
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
    let root = temp_root("local-primary-lightweight-read-snapshot");
    let node_id =
        NodeId::from_legacy_named("node_local_primary_lightweight_snapshot").expect("node");
    let directory = "/lightweight-read-snapshot/deep";
    let path = "/lightweight-read-snapshot/deep/tiny.txt";
    let payload = b"resident metadata snapshot";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(directory).expect("fixture directory");
    dfs.posix_write_file(
        path,
        payload,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("fixture file");

    let mut kv = InProcessKvStore::new();
    DfsControlPlaneStore::new(&mut kv)
        .put_metadata_shard(&DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node_id,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        })
        .expect("root metadata shard");
    let range_prefix_calls = Arc::new(AtomicUsize::new(0));
    dfs = dfs.with_control_plane(CountingDfsControlPlaneKvClient::new(
        kv,
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
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn put_local_object_version_from_reader_streams_to_content_store() {
    let root = temp_root("put-local-object-version-from-reader");
    let node_id = NodeId::from_legacy_named("node_stream_local_object").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stream_local_object").expect("object");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.path = "/services/streamed.bin".to_string();
    let size = 256 * 1024 + 31;
    let reader = std::io::repeat(0x42).take(size);

    let content_hash = dfs
        .put_local_object_version_from_reader(metadata, reader)
        .expect("stream object");

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("streamed object metadata");
    assert_eq!(object.content_hash.as_deref(), Some(content_hash.as_str()));
    assert_eq!(object.content_size_bytes, Some(size));
    let bytes = dfs.content.read_blob(&content_hash).expect("streamed blob");
    assert_eq!(bytes.len() as u64, size);
    assert!(bytes.iter().all(|byte| *byte == 0x42));
    assert!(
        std::fs::read_dir(root.join("content/staging"))
            .expect("content staging")
            .next()
            .is_none(),
        "streaming local object writes must clean temporary staging files"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_shard_primary_transfer_ready_requires_durable_replica_cursor() {
    let root = temp_root("metadata-primary-transfer-ready");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("root layout");
    let local = NodeId::from_legacy_named("node_meta_transfer_local").expect("local");
    let replica = NodeId::from_legacy_named("node_meta_transfer_replica").expect("replica");
    let remote_primary =
        NodeId::from_legacy_named("node_meta_transfer_remote_primary").expect("remote primary");
    let non_replica =
        NodeId::from_legacy_named("node_meta_transfer_non_replica").expect("non replica");
    let file_system = file_system(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let current = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };

    assert!(
        metadata_shard_primary_transfer_ready_from_local_backend(
            &data_dir,
            &local,
            &DfsMetadataShardConfig {
                primary_node_id: replica.clone(),
                replica_node_ids: vec![local.clone()],
                ..current.clone()
            },
            &replica,
        )
        .expect("already primary")
    );
    assert!(
        !metadata_shard_primary_transfer_ready_from_local_backend(
            &data_dir,
            &local,
            &DfsMetadataShardConfig {
                primary_node_id: remote_primary,
                ..current.clone()
            },
            &replica,
        )
        .expect("remote primary")
    );
    assert!(
        !metadata_shard_primary_transfer_ready_from_local_backend(
            &data_dir,
            &local,
            &current,
            &non_replica,
        )
        .expect("non replica")
    );

    let mut backend =
        open_local_metadata_shard_backend(&data_dir, shard_id.clone()).expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    let mut object = file_metadata(local.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_meta_transfer_ready").expect("object");
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/metadata/transfer-ready.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&object).expect("object");
    let journal_last_sequence = backend
        .metadata_shard_statuses()
        .expect("metadata shard statuses")
        .into_iter()
        .find(|status| status.shard_id == shard_id)
        .expect("shard status")
        .journal_last_sequence;
    assert!(journal_last_sequence > 0);
    drop(backend);

    assert!(
        !metadata_shard_primary_transfer_ready_from_local_backend(
            &data_dir, &local, &current, &replica,
        )
        .expect("missing durable replica cursor")
    );
    let mut backend =
        open_local_metadata_shard_backend(&data_dir, shard_id.clone()).expect("metadata backend");
    backend
        .put_metadata_replication_cursor(&replica, &shard_id, journal_last_sequence)
        .expect("candidate durable sender cursor");
    drop(backend);
    assert!(
        metadata_shard_primary_transfer_ready_from_local_backend(
            &data_dir, &local, &current, &replica,
        )
        .expect("caught-up durable replica cursor")
    );

    std::fs::remove_dir_all(root).ok();
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

#[test]
fn hydrate_from_metadata_backend_walks_multiple_conflict_pages() {
    let root = temp_root("hydrate-multiple-conflict-pages");
    let node_id = NodeId::from_legacy_named("node_hydrate_conflict_pages").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_conflict_pages").expect("object");
    let mut object = file_metadata(node_id.clone());
    object.object_id = object_id.clone();
    object.path = "/users/hydrate/conflicted.txt".to_string();
    object.version =
        DfsVersion::from_legacy_named("version_hydrate_conflict_remote").expect("remote version");
    dfs.add_object_metadata(object.clone())
        .expect("metadata object");

    let conflict_count = DFS_CONFLICT_SCAN_PAGE_SIZE + 1;
    for index in 0..conflict_count {
        let conflict = DfsConflictRecord {
            conflict_id: format!("conflict_hydrate_page_{index:04}"),
            file_system_id: object.file_system_id.clone(),
            object_id: object_id.clone(),
            path: object.path.clone(),
            base_version: Some(
                DfsVersion::from_legacy_named("version_hydrate_conflict_base")
                    .expect("base version"),
            ),
            local_version: DfsVersion::from_legacy_named(format!(
                "version_hydrate_conflict_local_{index}"
            ))
            .expect("local version"),
            remote_version: object.version.clone(),
            detected_by_node_id: node_id.clone(),
            reason: "restart hydration must page conflict records".to_string(),
            local_content_hash: Some(format!("blake3:local-hydrate-conflict-{index}")),
            remote_content_hash: object.content_hash.clone(),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        dfs.put_conflict_record(&conflict).expect("conflict record");
    }

    dfs.metadata = DfsMetadataStore::new();
    dfs.hydrate_from_metadata_backend()
        .expect("hydrate conflict pages");

    let loaded_conflicts = dfs
        .conflicts_for_object(&object_id)
        .expect("loaded conflicts");
    assert_eq!(
        loaded_conflicts.len(),
        conflict_count,
        "hydration must walk every conflict page for an object"
    );
    assert_eq!(
        loaded_conflicts.last().expect("tail conflict").conflict_id,
        format!("conflict_hydrate_page_{:04}", conflict_count - 1)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_touches_new_object_parent_atomically_once() {
    let (before, after, after_second_restart) = recovered_new_object_parent_metadata(true);
    assert_ne!(after.version, before.version);
    assert!(after.modified_at_unix_nanos > before.modified_at_unix_nanos);
    assert!(after.changed_at_unix_nanos > before.changed_at_unix_nanos);
    assert_eq!(
        after_second_restart, after,
        "child visibility must durably prove that the atomic parent touch already ran"
    );
}

#[test]
fn hinted_remote_read_serves_blob_without_local_metadata() {
    let root = temp_root("hinted-remote-read-without-metadata");
    let requester = NodeId::from_legacy_named("node_hinted_read_requester").expect("node");
    let server = NodeId::from_legacy_named("node_hinted_read_server").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hinted_read_no_metadata").expect("object");
    let version = DfsVersion::from_legacy_named("version_hinted_read").expect("version");
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let network = InProcessNetwork::new();
    let mut requester_transport = network.add_node(requester.clone());
    let content = LocalContentStore::new(root.join("server-content")).expect("server content");
    let manifest = content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("seed content blob");
    let mut dfs = NetworkedDfsService::new(network.add_node(server), content);

    let event = dfs
        .handle_networked_read_request(
            requester.clone(),
            DfsReadRequest {
                request_id: "dfs_net_req_hinted_read_no_metadata".to_string(),
                object_id: object_id.clone(),
                version: Some(version.clone()),
                content_hash: Some(manifest.content_hash.clone()),
                content_size_bytes: Some(manifest.size_bytes),
                range_offset: None,
                range_max_bytes: None,
            },
        )
        .expect("hinted read is served");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadServed {
            result: DfsReadResult::Found(DfsReadData { bytes: result_bytes, .. }),
            ..
        } if result_bytes.is_empty()
    ));
    let responses = drain_dfs_messages(&mut requester_transport);
    assert!(matches!(
        responses.as_slice(),
        [DfsNetworkMessage::ReadResponse(DfsReadResponse {
            result: DfsReadResult::Found(DfsReadData { bytes: result_bytes, .. }),
            ..
        })] if result_bytes == &bytes
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn uncached_remote_read_arbitrary_size_uses_reader_response() {
    let root = temp_root("uncached-remote-read-reader-response");
    let owner = NodeId::from_legacy_named("node_uncached_direct_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_uncached_direct_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner")).expect("owner content"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    let file_system = file_system(owner.clone());
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");

    let mut object = file_metadata(owner.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_uncached_direct_read").expect("object");
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    let object_id = object.object_id.clone();
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    owner_dfs
        .put_local_object_version(object, &bytes)
        .expect("seed owner object");
    reader_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read_without_cache(&object_id)
            .expect("request uncached read"),
        vec![owner.clone()]
    );
    let request = drain_dfs_messages(&mut owner_transport)
        .into_iter()
        .find_map(|message| match message {
            DfsNetworkMessage::ReadRequest(request) => Some(request),
            _ => None,
        })
        .expect("read request");

    owner_dfs
        .handle_networked_read_request(reader.clone(), request)
        .expect("owner sends direct response");
    let events = reader_dfs
        .process_incoming()
        .expect("reader accepts direct response");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes: result_bytes, .. }),
                ..
            } if completed_object_id == &object_id && result_bytes == &bytes
        )
    }));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn uncached_remote_read_direct_transport_completes_without_protocol_frame() {
    let root = temp_root("uncached-direct-transport-bypass");
    let owner = NodeId::from_legacy_named("node_uncached_bypass_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_uncached_bypass_reader").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_uncached_bypass").expect("object");
    let version = DfsVersion::from_legacy_named("version_uncached_bypass").expect("version");
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let owner_content = LocalContentStore::new(root.join("owner")).expect("owner content");
    let manifest = owner_content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("owner content blob");
    let direct_calls = Arc::new(Mutex::new(Vec::new()));
    let message_send_calls = Arc::new(Mutex::new(0));
    let transport = node_transport_with_direct_read_sender(
        reader.clone(),
        owner.clone(),
        bytes.clone(),
        None,
        Arc::clone(&direct_calls),
        Arc::clone(&message_send_calls),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("reader filesystem");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(manifest.content_hash.clone());
    object.content_size_bytes = Some(manifest.size_bytes);
    reader_dfs
        .add_object_metadata(object)
        .expect("reader metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read_without_cache(&object_id)
            .expect("request uncached read"),
        vec![owner.clone()]
    );
    assert_eq!(
        *message_send_calls.lock().expect("message send calls"),
        0,
        "direct-read-eligible message should avoid the normal DFS message RPC path"
    );
    let calls = direct_calls.lock().expect("direct calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].object_id, object_id);
    assert_eq!(calls[0].version, version);
    assert_eq!(calls[0].content_hash, manifest.content_hash);
    assert_eq!(calls[0].content_size_bytes, manifest.size_bytes);
    drop(calls);
    assert!(
        !reader_dfs.reads.pending_reads.is_empty(),
        "direct read responses now re-enter through the normal incoming message path"
    );

    let events = reader_dfs
        .process_incoming()
        .expect("reader drains completed direct read event");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes: result_bytes, .. }),
                ..
            } if completed_object_id == &object_id && result_bytes == &bytes
        )
    }));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn failed_uncached_remote_direct_read_cleans_pending_state() {
    let root = temp_root("uncached-direct-transport-error-cleanup");
    let owner = NodeId::from_legacy_named("node_uncached_error_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_uncached_error_reader").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_uncached_error").expect("object");
    let version = DfsVersion::from_legacy_named("version_uncached_error").expect("version");
    let direct_calls = Arc::new(Mutex::new(Vec::new()));
    let message_send_calls = Arc::new(Mutex::new(0));
    let transport = node_transport_with_direct_read_sender(
        reader.clone(),
        owner.clone(),
        Vec::new(),
        Some("direct read failed".to_string()),
        Arc::clone(&direct_calls),
        Arc::clone(&message_send_calls),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("reader filesystem");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some("blake3:uncached-error".to_string());
    object.content_size_bytes = Some(4096);
    reader_dfs
        .add_object_metadata(object)
        .expect("reader metadata");

    let error = reader_dfs
        .request_remote_object_read_without_cache(&object_id)
        .expect_err("direct read error should propagate");

    assert!(
        error.to_string().contains("direct read failed"),
        "unexpected error: {error}"
    );
    assert_eq!(direct_calls.lock().expect("direct calls").len(), 1);
    assert_eq!(
        *message_send_calls.lock().expect("message send calls"),
        0,
        "direct-read errors should not fall through to a duplicate DFS message send"
    );
    assert!(
        reader_dfs.reads.pending_reads.is_empty(),
        "failed synchronous direct reads must not leave stale pending entries"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_pending_remote_read_can_be_retried() {
    let root = temp_root("remote-read-stale-pending-retry");
    let owner = NodeId::from_legacy_named("node_read_retry_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_retry_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(content_hash(b"remote retry bytes"));
    object.content_size_bytes = Some(b"remote retry bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("initial read request"),
        vec![owner.clone()]
    );
    assert_eq!(
        owner_transport
            .drain_dfs_incoming()
            .expect("initial owner mailbox")
            .len(),
        1
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("active pending read is deduplicated"),
        vec![owner.clone()]
    );
    assert!(
        owner_transport
            .drain_dfs_incoming()
            .expect("deduped owner mailbox")
            .is_empty()
    );

    for pending in reader_dfs.reads.pending_reads.values_mut() {
        pending.next_retry_at = Instant::now() - Duration::from_millis(1);
    }
    assert!(
        !reader_dfs
            .has_pending_remote_read_for_current_object(&object_id)
            .expect("expired pending read is not active")
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("retry stale pending read"),
        vec![owner.clone()]
    );
    assert_eq!(
        owner_transport
            .drain_dfs_incoming()
            .expect("retried owner mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn process_incoming_accepts_queued_remote_read_response_before_expiry_prune() {
    let root = temp_root("remote-read-process-accepts-queued-expired");
    let owner = NodeId::from_legacy_named("node_read_late_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_late_reader").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_late").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let request_id = "dfs_net_req_read_late".to_string();
    let bytes = b"late but queued read bytes".to_vec();
    let hash = content_hash(&bytes);
    let mut transport = ReceiveOnlyTransport::new(reader.clone());
    transport.push_message(
        owner.clone(),
        reader.clone(),
        DfsNetworkMessage::ReadResponse(DfsReadResponse {
            request_id: request_id.clone(),
            object_id: object_id.clone(),
            result: DfsReadResult::Found(DfsReadData {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(hash.clone()),
                bytes: bytes.clone(),
            }),
        }),
    );
    let mut dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    dfs.add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(hash.clone());
    object.content_size_bytes = Some(bytes.len() as u64);
    object.version = version.clone();
    dfs.add_object_metadata(object).expect("object");
    dfs.reads.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            range_offset: None,
            range_max_bytes: None,
            target_node_id: owner.clone(),
            next_retry_at: Instant::now()
                - DFS_PENDING_TRANSFER_STALE_AFTER
                - Duration::from_millis(1),
            cache_response: true,
        },
    );

    let events = dfs.process_incoming().expect("process queued response");

    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: event_request_id,
                object_id: event_object_id,
                source_node_id,
                result: DfsReadResult::Found(_),
            } if event_request_id == &request_id
                && event_object_id == &object_id
                && source_node_id == &owner
        )
    }));
    assert!(!dfs.reads.pending_reads.contains_key(&request_id));
    assert_eq!(
        dfs.read_local_object(&object_id)
            .expect("local read")
            .expect("cached data")
            .bytes,
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_planning_deduplicates_duplicate_locations() {
    let root = temp_root("remote-read-dedupes-duplicate-locations");
    let owner = NodeId::from_legacy_named("node_read_dedupe_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_dedupe_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), owner.clone()];
    object.content_hash = Some(content_hash(b"duplicate location bytes"));
    object.content_size_bytes = Some(b"duplicate location bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("deduped read request"),
        vec![owner.clone()]
    );
    assert_eq!(
        owner_transport
            .drain_dfs_incoming()
            .expect("owner mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_planning_uses_single_active_source_and_falls_back_after_miss() {
    let root = temp_root("remote-read-single-active-fallback");
    let mut owners = [
        NodeId::from_legacy_named("node_read_source_a").expect("node"),
        NodeId::from_legacy_named("node_read_source_b").expect("node"),
    ];
    owners.sort();
    let [owner_a, owner_b] = owners;
    let reader = NodeId::from_legacy_named("node_read_single_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_a_transport = network.add_node(owner_a.clone());
    let mut owner_b_transport = network.add_node(owner_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner_a.clone(), owner_b.clone()];
    object.content_hash = Some(content_hash(b"single active read bytes"));
    object.content_size_bytes = Some(b"single active read bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![owner_a.clone()]
    );
    assert_eq!(
        owner_a_transport
            .drain_dfs_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    assert!(
        owner_b_transport
            .drain_dfs_incoming()
            .expect("source b mailbox")
            .is_empty(),
        "remote reads should not fan out to every replica when one source is active"
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("active request is reused"),
        vec![owner_a.clone()]
    );
    let request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == owner_a).then_some(request_id.clone())
        })
        .expect("pending source a read");
    reader_dfs
        .handle_networked_read_response(
            owner_a.clone(),
            DfsReadResponse {
                request_id,
                object_id: object_id.clone(),
                result: DfsReadResult::NotFound,
            },
        )
        .expect("source a miss is handled");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback request after miss"),
        vec![owner_b.clone()]
    );
    assert_eq!(
        owner_b_transport
            .drain_dfs_incoming()
            .expect("source b fallback mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn pending_remote_read_to_removed_location_does_not_block_fallback() {
    let root = temp_root("remote-read-removed-location-not-active");
    let source_a = NodeId::from_legacy_named("node_read_removed_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_removed_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_removed_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"removed location bytes"));
    object.content_size_bytes = Some(b"removed location bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs
        .add_object_metadata(object.clone())
        .expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_dfs_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let source_a_request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then(|| request_id.clone())
        })
        .expect("source a pending request");
    object.current_locations = vec![source_b.clone()];
    reader_dfs
        .add_object_metadata(object)
        .expect("advance object");

    assert!(
        !reader_dfs
            .has_pending_remote_read_for_current_object(&object_id)
            .expect("removed source should not count as active pending read")
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback request after location removal"),
        vec![source_b.clone()]
    );
    assert!(
        !reader_dfs
            .reads
            .pending_reads
            .contains_key(&source_a_request_id),
        "removed source pending read should be pruned when planning fallback"
    );
    assert_eq!(
        source_b_transport
            .drain_dfs_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn expired_remote_read_attempt_rotates_to_next_location_before_retrying_same_source() {
    let root = temp_root("remote-read-expired-rotates");
    let source_a = NodeId::from_legacy_named("node_read_expired_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_expired_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_expired_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"expired rotate bytes"));
    object.content_size_bytes = Some(b"expired rotate bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_dfs_incoming()
            .expect("source a first mailbox")
            .len(),
        1
    );
    for pending in reader_dfs.reads.pending_reads.values_mut() {
        pending.next_retry_at = Instant::now() - Duration::from_millis(1);
    }

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("rotated request"),
        vec![source_b.clone()]
    );
    assert!(
        source_a_transport
            .drain_dfs_incoming()
            .expect("source a retry mailbox")
            .is_empty(),
        "expired read should try a later replica before retrying the same silent source"
    );
    assert_eq!(
        source_b_transport
            .drain_dfs_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn late_remote_read_from_removed_location_does_not_clear_fallback_request() {
    let root = temp_root("remote-read-late-removed-location");
    let mut sources = [
        NodeId::from_legacy_named("node_read_late_a").expect("node"),
        NodeId::from_legacy_named("node_read_late_b").expect("node"),
    ];
    sources.sort();
    let [source_a, source_b] = sources;
    let reader = NodeId::from_legacy_named("node_read_late_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let bytes = b"late removed source bytes";
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(bytes));
    object.content_size_bytes = Some(bytes.len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let content_hash = object.content_hash.clone();
    reader_dfs
        .add_object_metadata(object.clone())
        .expect("object");

    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request source a");
    assert_eq!(
        source_a_transport
            .drain_dfs_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let source_a_request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then_some(request_id.clone())
        })
        .expect("source a request");
    object.current_locations = vec![source_b.clone()];
    reader_dfs
        .add_object_metadata(object)
        .expect("advance object");
    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("fallback source b");
    assert_eq!(
        source_b_transport
            .drain_dfs_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    let source_b_request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_b).then_some(request_id.clone())
        })
        .expect("source b request");
    assert!(
        !reader_dfs
            .reads
            .pending_reads
            .contains_key(&source_a_request_id),
        "fallback planning should prune the stale source a request"
    );

    let event = reader_dfs
        .handle_networked_read_response(
            source_a.clone(),
            DfsReadResponse {
                request_id: source_a_request_id,
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: object_id.clone(),
                    version,
                    content_hash,
                    bytes: bytes.to_vec(),
                }),
            },
        )
        .expect("late source a response");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            source_node_id,
            result: DfsReadResult::Rejected { reason },
            ..
        } if source_node_id == source_a && reason.contains("unknown DFS read request")
    ));
    assert!(
        reader_dfs
            .reads
            .pending_reads
            .contains_key(&source_b_request_id),
        "late response from removed source must not clear the fallback request"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_rejection_from_stale_location_falls_back_to_next_source() {
    let root = temp_root("remote-read-rejection-fallback");
    let source_a = NodeId::from_legacy_named("node_read_reject_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_reject_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_reject_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"stale rejection bytes"));
    object.content_size_bytes = Some(b"stale rejection bytes".len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let hash = object.content_hash.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    let requested = reader_dfs
        .request_remote_object_read(&object_id)
        .expect("first read request");
    assert_eq!(requested.len(), 1);
    let rejected_source = requested[0].clone();
    let fallback_source = if rejected_source == source_a {
        source_b.clone()
    } else {
        source_a.clone()
    };
    assert_eq!(
        if rejected_source == source_a {
            source_a_transport
                .drain_dfs_incoming()
                .expect("source a mailbox")
                .len()
        } else {
            source_b_transport
                .drain_dfs_incoming()
                .expect("source b mailbox")
                .len()
        },
        1
    );
    let request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == rejected_source).then_some(request_id.clone())
        })
        .expect("pending read for rejected source");
    reader_dfs
        .handle_networked_read_response(
            rejected_source.clone(),
            DfsReadResponse {
                request_id,
                object_id: object_id.clone(),
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "node {rejected_source} is not a current location for DFS object {object_id}"
                    ),
                },
            },
        )
        .expect("stale-location rejection handled");

    assert_eq!(
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations,
        vec![fallback_source.clone()]
    );
    assert_eq!(
        reader_dfs
            .metadata_backend
            .as_ref()
            .expect("backend")
            .get_version(&object_id, &version)
            .expect("version")
            .expect("version")
            .locations,
        vec![fallback_source.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback after stale-location rejection"),
        vec![fallback_source.clone()]
    );
    assert_eq!(
        if fallback_source == source_a {
            source_a_transport
                .drain_dfs_incoming()
                .expect("source a fallback mailbox")
                .len()
        } else {
            source_b_transport
                .drain_dfs_incoming()
                .expect("source b fallback mailbox")
                .len()
        },
        1
    );
    assert_eq!(
        hash,
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata after fallback")
            .content_hash
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn inline_remote_read_wrong_object_is_isolated_and_falls_back() {
    let root = temp_root("remote-read-wrong-object-isolated");
    let source_a = NodeId::from_legacy_named("node_read_wrong_object_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_wrong_object_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_wrong_object_reader").expect("node");
    let wrong_object_id =
        DfsObjectId::from_legacy_named("obj_wrong_read_response").expect("object");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"right remote bytes"));
    object.content_size_bytes = Some(b"right remote bytes".len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let hash = object.content_hash.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    let requested = reader_dfs
        .request_remote_object_read(&object_id)
        .expect("first read request");
    assert_eq!(requested.len(), 1);
    let wrong_source = requested[0].clone();
    let fallback_source = if wrong_source == source_a {
        source_b.clone()
    } else {
        source_a.clone()
    };
    assert_eq!(
        if wrong_source == source_a {
            source_a_transport
                .drain_dfs_incoming()
                .expect("source a mailbox")
                .len()
        } else {
            source_b_transport
                .drain_dfs_incoming()
                .expect("source b mailbox")
                .len()
        },
        1
    );
    let request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == wrong_source).then_some(request_id.clone())
        })
        .expect("pending read for wrong source");

    let event = reader_dfs
        .handle_networked_read_response(
            wrong_source.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: wrong_object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: wrong_object_id,
                    version: version.clone(),
                    content_hash: hash.clone(),
                    bytes: b"right remote bytes".to_vec(),
                }),
            },
        )
        .expect("wrong-object response is isolated");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            object_id: completed_object_id,
            source_node_id,
            result: DfsReadResult::Rejected { reason },
        } if completed_request_id == request_id
            && completed_object_id != object_id
            && source_node_id == wrong_source
            && reason.contains("unexpected DFS read response object")
    ));
    assert!(!reader_dfs.reads.pending_reads.contains_key(&request_id));
    assert_eq!(
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations,
        vec![fallback_source.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback after wrong-object response"),
        vec![fallback_source.clone()]
    );
    assert_eq!(
        if fallback_source == source_a {
            source_a_transport
                .drain_dfs_incoming()
                .expect("source a fallback mailbox")
                .len()
        } else {
            source_b_transport
                .drain_dfs_incoming()
                .expect("source b fallback mailbox")
                .len()
        },
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn inline_remote_read_wrong_payload_object_is_isolated_and_falls_back() {
    let root = temp_root("remote-read-wrong-payload-object-isolated");
    let mut sources = [
        NodeId::from_legacy_named("node_read_wrong_payload_a").expect("node"),
        NodeId::from_legacy_named("node_read_wrong_payload_b").expect("node"),
    ];
    sources.sort();
    let [source_a, source_b] = sources;
    let reader = NodeId::from_legacy_named("node_read_wrong_payload_reader").expect("node");
    let wrong_object_id = DfsObjectId::from_legacy_named("obj_wrong_read_payload").expect("object");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"right payload bytes"));
    object.content_size_bytes = Some(b"right payload bytes".len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let hash = object.content_hash.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_dfs_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let request_id = reader_dfs
        .reads
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then_some(request_id.clone())
        })
        .expect("pending source a read");

    let event = reader_dfs
        .handle_networked_read_response(
            source_a.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: wrong_object_id.clone(),
                    version: version.clone(),
                    content_hash: hash.clone(),
                    bytes: b"right payload bytes".to_vec(),
                }),
            },
        )
        .expect("wrong-payload response is isolated");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            object_id: completed_object_id,
            source_node_id,
            result: DfsReadResult::Rejected { reason },
        } if completed_request_id == request_id
            && completed_object_id == object_id
            && source_node_id == source_a
            && reason.contains("unexpected DFS read response payload object")
    ));
    assert!(!reader_dfs.reads.pending_reads.contains_key(&request_id));
    assert_eq!(
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations,
        vec![source_b.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback after wrong-payload response"),
        vec![source_b.clone()]
    );
    assert_eq!(
        source_b_transport
            .drain_dfs_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    assert!(matches!(
        reader_dfs.read_local_object(&wrong_object_id),
        Err(DfsError::UnknownObject { .. })
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_content_identity_mismatch_prunes_stale_location() {
    let root = temp_root("remote-read-mismatch-prunes-location");
    let reader = NodeId::from_legacy_named("node_read_mismatch_reader").expect("node");
    let source = NodeId::from_legacy_named("node_read_mismatch_source").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_mismatch").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let expected_hash = content_hash(b"expected remote bytes");
    let request_id = "dfs_net_req_read_mismatch".to_string();
    let mut dfs = networked_sifs_with_backend(&root, reader.clone());
    let mut object = file_metadata(source.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    object.content_hash = Some(expected_hash.clone());
    object.content_size_bytes = Some(b"expected remote bytes".len() as u64);
    dfs.add_object_metadata(object).expect("object metadata");
    dfs.reads.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(expected_hash.clone()),
            content_size_bytes: Some(b"expected remote bytes".len() as u64),
            range_offset: None,
            range_max_bytes: None,
            target_node_id: source.clone(),
            next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
            cache_response: true,
        },
    );

    let event = dfs
        .handle_networked_read_response(
            source.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: Some(content_hash(b"corrupt remote bytes")),
                    bytes: b"corrupt remote bytes".to_vec(),
                }),
            },
        )
        .expect("mismatch handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            source_node_id,
            result: DfsReadResult::Rejected { reason },
            ..
        } if completed_request_id == request_id
            && source_node_id == source
            && reason.contains("expected content hash")
    ));
    assert!(!dfs.reads.pending_reads.contains_key(&request_id));
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("current metadata");
    assert!(
        !object.current_locations.contains(&source),
        "source that returned a different content identity should be pruned"
    );
    assert_eq!(
        dfs.request_remote_object_read(&object_id)
            .expect("no stale source to retry"),
        Vec::<NodeId>::new()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_location_prune_updates_stale_backend_version_record() {
    let root = temp_root("read-prune-updates-version-record");
    let reader = NodeId::from_legacy_named("node_read_prune_reader").expect("node");
    let source = NodeId::from_legacy_named("node_read_prune_source").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_prune_version").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let hash = content_hash(b"version-location bytes");
    let mut dfs = networked_sifs_with_backend(&root, reader);
    let mut object = file_metadata(source.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations.clear();
    object.content_hash = Some(hash.clone());
    object.content_size_bytes = Some(b"version-location bytes".len() as u64);
    dfs.add_object_metadata(object.clone())
        .expect("object metadata");
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_version(&DfsObjectVersionRecord {
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(hash.clone()),
            size_bytes: object.content_size_bytes,
            locations: vec![source.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        })
        .expect("stale version locations");
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object_id.clone(),
                committed_version: version.clone(),
                owner_node_id: source.clone(),
                event_id: EventId::from_legacy_named("evt_read_prune_stale_receipt")
                    .expect("event"),
            },
            replica_node_id: source.clone(),
            content_hash: Some(hash.clone()),
            received_at: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("stale receipt");

    dfs.mark_remote_read_location_missing(&object_id, &version, &Some(hash), &source)
        .expect("prune stale version location");

    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .current_locations,
        Vec::<NodeId>::new()
    );
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("backend")
            .get_version(&object_id, &version)
            .expect("backend version")
            .expect("backend version")
            .locations,
        Vec::<NodeId>::new()
    );
    assert!(
        dfs.metadata_backend
            .as_ref()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("backend receipts")
            .is_empty(),
        "authoritative stale-location prune should remove the matching receipt"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn replica_remote_read_cache_and_prune_do_not_append_authoritative_metadata() {
    let root = temp_root("replica-read-cache-no-authoritative-metadata");
    let owner = NodeId::from_legacy_named("node_replica_read_cache_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica_read_cache_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_replica_read_cache").expect("object");
    let version = DfsVersion::from_legacy_named("version_replica_read_cache").expect("version");
    let bytes = b"replica remote read cache";
    let content_hash = content_hash(bytes);
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = "/services/read-cache/no-authority.txt".to_string();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(content_hash.clone());
    object.content_size_bytes = None;
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig {
                file_history: Default::default(),
                file_system_id: sifs_file_system_id(),
                display_name: "SIFS".to_string(),
                owner_node_id: owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![owner.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
    )
    .expect("metadata backend");
    let version_record = object_version_record(&object, None);
    {
        let backend = dfs.metadata_backend.as_mut().expect("metadata backend");
        backend
            .put_file_system(&distributed_sifs(owner.clone()))
            .expect("replicated filesystem metadata");
        backend.put_object(&object).expect("backend object");
        backend
            .put_version(&version_record)
            .expect("backend version");
    }
    dfs.apply_metadata_object_to_service_state(object.clone())
        .expect("service object");
    dfs.apply_metadata_version_to_service_state(&version_record);
    let before_journal = dfs
        .metadata_backend()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata status")
        .pop()
        .expect("root shard status")
        .journal_last_sequence;

    dfs.mark_remote_read_location_missing(
        &object_id,
        &version,
        &Some(content_hash.clone()),
        &owner,
    )
    .expect("transient stale location prune");

    let current = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    assert!(
        !current.current_locations.contains(&owner),
        "reader should remember the stale source transiently"
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .current_locations,
        vec![owner.clone()],
        "replica read prune must not rewrite the durable metadata mirror"
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("root shard status")
            .journal_last_sequence,
        before_journal,
        "replica read prune must not append to the authoritative shard journal"
    );

    dfs.store_remote_read_data(
        &owner,
        &DfsReadData {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(content_hash.clone()),
            bytes: bytes.to_vec(),
        },
    )
    .expect("store remote read cache");

    assert_eq!(
        dfs.read_local_object_with_state(&object_id, false)
            .expect("read local cache"),
        NetworkedDfsReadOutcome::Found {
            data: DfsReadData {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                bytes: bytes.to_vec(),
            },
            freshness: NetworkedDfsReadFreshness::Current,
        }
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_version(&object_id, &version)
            .expect("backend version")
            .expect("backend version")
            .size_bytes,
        None,
        "replica remote-read size discovery must stay local"
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("root shard status")
            .journal_last_sequence,
        before_journal,
        "replica remote-read cache must not append to the authoritative shard journal"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn read_current_object_uses_hash_addressed_blob_without_manifest_scan() {
    let root = temp_root("read-current-object-hash-addressed");
    let node_id = NodeId::from_legacy_named("node_hash_addressed").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hash_addressed").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"shared content-addressed bytes";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let seed_manifest = dfs
        .content
        .put_object_version("obj_other_hash_owner", "version_1", bytes)
        .expect("seed blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.path = "services/hash-addressed.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.content_hash = Some(seed_manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read");

    match outcome {
        NetworkedDfsReadOutcome::Found { data, freshness } => {
            assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
            assert_eq!(data.object_id, object_id);
            assert_eq!(data.version, version);
            assert_eq!(
                data.content_hash.as_deref(),
                Some(seed_manifest.content_hash.as_str())
            );
            assert_eq!(data.bytes, bytes);
        }
        other => {
            panic!("expected current object bytes from hash-addressed blob, got {other:?}")
        }
    }
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn read_current_object_range_uses_bounded_hash_addressed_blob_slice() {
    let root = temp_root("read-current-object-range-hash-addressed");
    let node_id = NodeId::from_legacy_named("node_hash_range").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hash_range").expect("object");
    let version = DfsVersion::from_legacy_named("version_range").expect("version");
    let bytes = b"range-readable content-addressed bytes";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let seed_manifest = dfs
        .content
        .put_object_version("obj_other_range_owner", "version_1", bytes)
        .expect("seed blob");
    let mut metadata = file_metadata(node_id.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/services/hash-range.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.content_hash = Some(seed_manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_range_with_state(&object_id, 6, 8, false)
        .expect("range read");
    match outcome {
        NetworkedDfsRangeReadOutcome::Found { data, freshness } => {
            assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
            assert_eq!(data.object_id, object_id);
            assert_eq!(data.version, version);
            assert_eq!(
                data.content_hash.as_deref(),
                Some(seed_manifest.content_hash.as_str())
            );
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn exact_size_local_prefix_read_does_not_rehash_content() {
    let root = temp_root("local-prefix-read-rejects-corrupt-blob");
    let node_id = NodeId::from_legacy_named("node_local_prefix_corrupt").expect("node");
    let path = "/services/local-prefix-corrupt.txt";
    let bytes = vec![0x5a; 20 * 1024];
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("local file write");
    let hash = stat.content_hash.expect("committed content hash");
    let hex = hash.strip_prefix("blake3:").expect("blake3 hash");
    let blob_path = root.join("content").join("blobs").join(&hex[..2]).join(hex);
    let mut corrupt_bytes = bytes.clone();
    corrupt_bytes[0] ^= 0xff;
    std::fs::write(&blob_path, &corrupt_bytes).expect("corrupt local blob in place");

    let read = dfs
        .posix_read_file_prefix_distributed(path, bytes.len())
        .expect("ordinary prefix reads do not rehash content");
    assert_eq!(read.bytes, corrupt_bytes);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_read_skips_missing_stale_blobs() {
    let root = temp_root("read-stale-skips-missing-blob");
    let node_id = NodeId::from_legacy_named("node_stale_skip").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_skip").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let readable_stale_bytes = vec![b's'; 17 * 1024];
    let missing_stale_bytes = vec![b'm'; 17 * 1024];
    let readable_stale = dfs
        .content
        .put_object_version(
            object_id.as_str(),
            version_1.as_str(),
            &readable_stale_bytes,
        )
        .expect("readable stale");
    let missing_stale = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), &missing_stale_bytes)
        .expect("missing stale");
    let hex = missing_stale
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove missing stale blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_3;
    metadata.content_hash = Some(content_hash(b"unavailable current"));
    metadata.content_size_bytes = Some(b"unavailable current".len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_with_state(&object_id, true)
        .expect("stale read skips missing blob");

    assert!(matches!(
        outcome,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Stale { .. },
        } if data.version == version_1
            && data.content_hash.as_deref() == Some(readable_stale.content_hash.as_str())
            && data.bytes == readable_stale_bytes
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_create_bulk_local_eventual_avoids_per_file_requester_lookups() {
    let root = temp_root("sifs-posix-create-bulk-requester-lookups");
    let node_id =
        NodeId::from_legacy_named("node_sifs_create_bulk_requester_lookups").expect("node");
    let work = "/services/sandbox/instances/sandbox_create_bulk_requester_lookups/work";
    let mut dfs = networked_sifs_with_backend(&root, node_id);
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
    let before = dfs.posix_stat(file_path).expect("before stat");

    assert_eq!(
        dfs.posix_read_file_update_access(file_path)
            .expect("read update access"),
        b"content"
    );

    let stat = dfs.posix_stat(file_path).expect("stat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);
    assert_ne!(
        stat.version, before.version,
        "atime update must advance the DFS metadata version so it persists across distributed reconciliation"
    );
    assert_eq!(
        dfs.posix_read_file(file_path)
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
            .posix_list_dir_page(dir_path, cursor.as_ref(), 2)
            .expect("list page");
        assert!(page.entries.len() <= 2);
        if let Some(next_cursor) = page.next_cursor.as_ref() {
            assert!(
                next_cursor.dentry.is_some(),
                "overlay-free backend pages must preserve the bounded dentry cursor"
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
        .posix_list_dir(dir_path)
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

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
            let backend = dfs.metadata_backend.as_mut().expect("metadata backend");
            backend
                .put_object(&tombstone)
                .expect("settle transient tombstone");
            backend
                .put_object(&transient)
                .expect("settle transient child");
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
    let mut cluster = sifs_posix_cluster("sifs-contract-rename-readers");
    let scenario = "rename-atomicity-with-active-readers";
    let source = "/users/contract-rename-source.txt";
    let destination = "/users/contract-rename-destination.txt";
    let bytes = b"rename readers see one path".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, source, &bytes);
    sifs_cluster_publish_metadata_until(&mut cluster, "pre-rename observer", |cluster| {
        cluster.observer.posix_stat(source).is_ok()
    });
    for (op, node, result) in [
        (
            "pre-source",
            cluster.observer_node.clone(),
            cluster.observer.posix_stat(source),
        ),
        (
            "pre-destination",
            cluster.observer_node.clone(),
            cluster.observer.posix_stat(destination),
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
    let primary_source = cluster.primary.posix_stat(source);
    let primary_destination = cluster.primary.posix_stat(destination);
    assert!(
        primary_source.is_err() && primary_destination.is_ok(),
        "primary must expose the rename atomically"
    );
    sifs_cluster_publish_metadata_until(&mut cluster, "post-rename observer", |cluster| {
        matches!(
            cluster.observer.posix_stat(source),
            Err(DfsPosixError::NotFound { .. })
        ) && cluster.observer.posix_stat(destination).is_ok()
    });
    let entries = cluster
        .observer
        .posix_list_dir("/users")
        .expect("observer list");
    assert!(
        !entries.iter().any(|entry| entry.path == source)
            && entries.iter().any(|entry| entry.path == destination),
        "observer must not see both source and destination as live entries: {entries:?}"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_stale_cache_current_read_invalidation() {
    let mut cluster = sifs_posix_cluster("sifs-contract-stale-cache-current");
    let scenario = "stale-cache-current-read-invalidation";
    let path = "/users/contract-current-read.txt";
    let old = b"old cache content".to_vec();
    let new = b"new current content".to_vec();
    let mut observations = Vec::new();
    sifs_cluster_routed_posix_write(&mut cluster, path, &old);
    assert_eq!(cluster.writer.posix_read_file(path).expect("old read"), old);
    cluster
        .primary
        .posix_write_file(
            path,
            &new,
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        )
        .expect("primary overwrite");
    sifs_cluster_publish_metadata_until(&mut cluster, "writer learns new version", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(content_hash(&new).as_str()))
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
        .posix_stat(path)
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
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn exact_resident_hydration_replaces_only_its_shard_namespace_guards() {
    let root = temp_root("exact-resident-namespace-guard-hydration");
    let node = NodeId::from_legacy_named("node_exact_guard_hydration").expect("node");
    let file_system_id = sifs_file_system_id();
    let root_shard_id = DfsMetadataShardId::new("root");
    let services_shard_id = DfsMetadataShardId::new("services");
    let shard = |shard_id: &str, assignment| DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: node.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let resident = LocalShardedMetadataBackend::new(
        root.join("metadata"),
        vec![
            shard("root", DfsMetadataShardAssignment::Root),
            shard(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend")
    .into_resident();
    let registry = DfsCrossShardNamespaceLockRegistry::default();
    registry.replace_jobs([
        (
            Some(root_shard_id.clone()),
            "stale-root-job".to_string(),
            file_system_id.clone(),
            "/root-stale".to_string(),
        ),
        (
            Some(services_shard_id),
            "live-services-job".to_string(),
            file_system_id.clone(),
            "/services/live".to_string(),
        ),
    ]);

    let service = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_cross_shard_namespace_lock_registry(registry)
    .with_resident_metadata_shard_backend_lazy(
        resident
            .scope_shard(&file_system_id, &root_shard_id)
            .expect("root shard handle")
            .checkout()
            .expect("root shard checkout"),
    )
    .expect("exact resident service");

    service
        .ensure_path_not_in_cross_shard_flight(&file_system_id, "/root-stale/child")
        .expect("empty exact root snapshot removes the stale root guard");
    assert!(matches!(
        service.ensure_path_not_in_cross_shard_flight(&file_system_id, "/services/live/child"),
        Err(DfsError::Busy { .. })
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_contract_damaged_replica_under_concurrent_reads() {
    let mut cluster = sifs_posix_cluster("sifs-contract-damaged-replica");
    let scenario = "damaged-replica-under-concurrent-reads";
    let path = "/services/contract-damaged.bin";
    let old = vec![0x44; 64 * 1024];
    let new = vec![0x55; 64 * 1024];
    let old_hash = content_hash(&old);
    let mut observations = Vec::new();
    let initial_write = cluster.writer.posix_write_file(
        path,
        &old,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            replica_candidates: vec![cluster.writer_node.clone(), cluster.primary_node.clone()],
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
                .posix_stat(path)
                .is_ok_and(|stat| stat.content_hash.as_deref() == Some(old_hash.as_str()))
        },
    );
    sifs_posix_cluster_pump_until(&mut cluster, "primary gets old replica", |cluster| {
        cluster
            .primary
            .posix_read_file(path)
            .is_ok_and(|bytes| bytes == old)
    });
    cluster
        .writer
        .content
        .debug_remove_loose_blob(&old_hash)
        .expect("damage writer blob");
    cluster
        .primary
        .posix_write_file(
            path,
            &new,
            sifs_cluster_write_options(&cluster.primary_node.clone()),
        )
        .expect("primary concurrent overwrite");
    sifs_cluster_publish_metadata_until(&mut cluster, "writer sees new metadata", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(content_hash(&new).as_str()))
    });
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
    let stat = cluster.writer.posix_stat(path).expect("writer stat");
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
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_distributed_read_refreshes_metadata_after_local_location_prune() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-stale-reader-prune");
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
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
    sifs_posix_cluster_pump_until(&mut cluster, "primary async repair", |cluster| {
        cluster
            .primary
            .retry_pending_repairs()
            .expect("primary repair tick");
        cluster
            .primary
            .posix_read_file(path)
            .is_ok_and(|read| read == bytes)
    });
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "writer learns repaired primary location",
        |cluster| {
            cluster.writer.posix_stat(path).is_ok_and(|stat| {
                stat.current_locations.contains(&cluster.writer_node)
                    && stat.current_locations.contains(&cluster.primary_node)
            })
        },
    );
    let stale_stat = cluster.writer.posix_stat(path).expect("writer stale stat");

    cluster
        .writer
        .content
        .debug_remove_loose_blob(&expected_hash)
        .expect("remove writer loose blob");
    assert!(
        matches!(
            cluster.writer.posix_read_file(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "fault injection must remove the writer's local readable content"
    );

    {
        let mut backend =
            open_root_local_metadata_backend(cluster.root.join("writer").join("metadata"))
                .expect("writer metadata backend");
        let mut object = backend
            .get_object(&stale_stat.object_id)
            .expect("writer backend object")
            .expect("writer backend object");
        object
            .current_locations
            .retain(|node_id| node_id != &cluster.writer_node);
        backend.put_object(&object).expect("prune writer object");
        let mut version = backend
            .get_version(&stale_stat.object_id, &stale_stat.version)
            .expect("writer backend version")
            .expect("writer backend version");
        version
            .locations
            .retain(|node_id| node_id != &cluster.writer_node);
        backend.put_version(&version).expect("prune writer version");
        backend
            .remove_receipt(
                &stale_stat.object_id,
                &stale_stat.version,
                &cluster.writer_node,
            )
            .expect("remove writer receipt");
    }

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

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_non_replica_node_reads_converge_via_sync_pulls() {
    let cluster = sifs_outsider_cluster("sifs-outsider-sync-pull-reads");
    let SifsPosixCluster {
        root,
        primary_node,
        primary,
        mut observer,
        ..
    } = cluster;

    let path = "/users/sync-pull.txt";
    let bytes = b"pulled through SyncPath".to_vec();
    let mut primary = primary;
    primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary local write");

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    // The outsider never received a journal push; the distributed stat must
    // pull the shard journal from the primary and surface the file.
    let stat = observer
        .posix_stat_distributed(path)
        .expect("outsider stat converges via sync pull");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));

    // Content reads fetch remotely as before.
    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("outsider distributed read"),
        bytes
    );

    // A genuinely missing path stays NotFound: the pull catches the mirror up
    // and then reports the authoritative miss instead of spinning.
    assert!(matches!(
        observer.posix_stat_distributed("/users/never-created.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_non_replica_prefix_read_reuses_authoritative_metadata_for_remote_range() {
    let cluster = sifs_outsider_cluster("sifs-outsider-prefix-range-read");
    let SifsPosixCluster {
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
            DfsWriteOptions::replicated_eventual(vec![primary_node]),
        )
        .expect("primary soon-corrupt file write");
    let corrupt_hash = corrupt_stat.content_hash.expect("corrupt-file hash");
    let corrupt_hex = corrupt_hash
        .strip_prefix("blake3:")
        .expect("blake3 corrupt-file hash");
    let corrupt_blob_path = root
        .join("primary")
        .join("content")
        .join("blobs")
        .join(&corrupt_hex[..2])
        .join(corrupt_hex);
    let mut corrupted_bytes = corrupt_source_bytes.clone();
    corrupted_bytes[0] ^= 0xff;
    std::fs::write(&corrupt_blob_path, &corrupted_bytes)
        .expect("corrupt primary blob without changing its size");

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    let prefix = observer
        .posix_read_file_prefix_distributed(path, 23)
        .expect("outsider authoritative prefix read");
    assert_eq!(prefix.bytes, bytes[..23]);
    assert!(prefix.truncated);
    assert!(
        observer.posix_read_file(path).is_err(),
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
        observer.posix_read_file(path).is_err(),
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

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_replica_distributed_read_prefers_primary_over_stale_local_hit() {
    let mut cluster = sifs_posix_cluster("sifs-replica-read-primary-current");
    let path = "/users/read-stale-live.txt";
    let old = b"writer-local stale payload should lose to primary current payload".to_vec();
    let new = b"primary current payload must win the distributed read".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &old);
    assert_eq!(
        cluster
            .writer
            .posix_read_file(path)
            .expect("writer old read"),
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
    assert_eq!(
        cluster
            .primary
            .posix_read_file(path)
            .expect("primary new read"),
        new
    );
    assert_eq!(
        cluster
            .writer
            .posix_read_file(path)
            .expect("writer stale read"),
        old,
        "test setup must leave a stale local live entry on the writer"
    );

    let read = {
        let primary = &mut cluster.primary;
        let writer = &mut cluster.writer;
        sifs_cluster_blocking_distributed_read_with_pump(
            writer,
            path,
            "replica current read prefers primary object",
            || {
                primary.process_incoming().expect("primary pump");
            },
        )
    };
    assert_eq!(
        read, new,
        "distributed/current reads must prefer the shard primary over a stale replica-local hit"
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_replica_read_miss_queries_primary_without_pulling_metadata() {
    let cluster = sifs_posix_cluster("sifs-replica-read-no-sync-pull");
    let SifsPosixCluster {
        root,
        primary_node,
        mut primary,
        mut observer,
        ..
    } = cluster;
    let path = "/users/replica-read-no-sync.txt";
    let bytes = b"replica reads query primary without local metadata writes".to_vec();
    primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary local write");

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("replica miss queries primary and fetches content"),
        bytes
    );
    assert!(
        matches!(
            observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "distributed reads on a configured replica must not pull and replay metadata"
    );

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_replica_primary_lookup_read_retries_after_stale_location_without_local_metadata() {
    let cluster = sifs_posix_cluster("sifs-replica-read-stale-source-no-local-metadata");
    let SifsPosixCluster {
        root,
        primary_node,
        writer_node,
        mut primary,
        mut writer,
        mut observer,
        ..
    } = cluster;
    let path = "/users/replica-read-stale-source.txt";
    let bytes = b"replica primary lookup should retry a stale advertised location".to_vec();
    let stat = primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
        )
        .expect("primary local write");

    let mut object = primary
        .metadata()
        .object(&stat.object_id)
        .expect("primary object")
        .clone();
    object.current_locations = vec![writer_node.clone(), primary_node.clone()];
    // Seed the current primary's authoritative test state without scheduling a
    // journal publication to the observer. The behavior under test starts
    // with no local file metadata and exercises only routed lookup plus exact
    // content retry; queued replica catch-up is covered separately.
    primary
        .metadata_backend
        .as_mut()
        .expect("primary metadata backend")
        .put_object(&object)
        .expect("primary backend stale location");
    primary
        .apply_metadata_object_to_service_state(object)
        .expect("primary in-memory stale location");

    assert!(
        matches!(
            observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "observer should not have local metadata before the distributed read"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop);
    let primary_thread = std::thread::spawn(move || {
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });
    let writer_stop = Arc::clone(&stop);
    let writer_thread = std::thread::spawn(move || {
        while !writer_stop.load(Ordering::SeqCst) {
            writer.process_incoming().expect("writer stale-source pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        writer
    });

    let started = Instant::now();
    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("observer retries from stale source to primary"),
        bytes
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "stale-source retry should not spend the full remote-read budget"
    );
    assert!(
        matches!(
            observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "read-only primary lookup must not install metadata on the replica"
    );

    stop.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    writer_thread.join().expect("writer thread");

    std::fs::remove_dir_all(&root).ok();
}
