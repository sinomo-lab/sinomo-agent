//! A positive Repair reply cannot be ordered against a later withdrawal of the
//! same receipt slot, so receipt-slot generations fence it. The race case
//! drives the ordinary (non-original) producer and the real receiver; only
//! reply delivery is held by the test.

use super::*;

/// The receiver's reply to one Repair target, once it has arrived. Every
/// other message delivered meanwhile is served normally.
fn await_repair_reply(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    intent_id: &str,
    replica: &NodeId,
) -> crate::DfsRoutedMessage {
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let (mut replies, others): (Vec<_>, Vec<_>) = service
            .transport
            .drain_incoming_messages()
            .unwrap()
            .into_iter()
            .partition(|routed| {
                matches!(&routed.message,
                    DfsNetworkMessage::RepairReplicaResponse(response)
                        if response.intent_id == intent_id && &response.replica_node_id == replica)
            });
        if !others.is_empty() {
            service
                .process_routed_messages_with_options(others, true)
                .unwrap();
        }
        if let Some(reply) = replies.pop() {
            assert!(replies.is_empty(), "exactly one actual receiver reply");
            return reply;
        }
        assert!(
            Instant::now() < deadline,
            "the receiver's reply did not arrive"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One pass of the node's repair executor that sends at most one repair and
/// leaves its reply queued for the test.
fn executor_send_once(
    runtime: &crate::test_runtime::DfsTestRuntime,
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    data_root: &Path,
) -> usize {
    let pass = service
        .run_repair_executor_pass(
            data_root,
            &runtime.metadata_shards().unwrap(),
            &crate::DfsMaintenanceExecutionExclusions::default(),
            1,
        )
        .unwrap();
    service
        .transport
        .checkpoint_background_replica_dispatch()
        .unwrap();
    pass.scheduled.len()
}

fn replica_receipt(
    service: &NetworkedDfsService<DfsNodeTransport>,
    stat: &DfsStat,
    replica: &NodeId,
) -> Vec<DfsReplicaReceiptRecord> {
    service
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap()
        .into_iter()
        .filter(|receipt| &receipt.replica_node_id == replica)
        .collect()
}

fn slot_generation(
    service: &NetworkedDfsService<DfsNodeTransport>,
    stat: &DfsStat,
    replica: &NodeId,
) -> u64 {
    service
        .backend_receipt_slot_generation(&stat.object_id, &stat.version, replica)
        .unwrap()
}

/// Reopen the primary's durable shard from its snapshot and table log, as a
/// restarted node does, and read the slot generation it materializes.
fn replayed_slot_generation(
    runtime: &crate::test_support::DfsTestRuntime,
    primary: &NodeId,
    stat: &DfsStat,
    replica: &NodeId,
) -> u64 {
    let root = crate::default_dfs_metadata_shards_root(runtime.data_dir(primary)).join("services");
    crate::LocalSobjMetadataBackend::open_for_inspection(DfsMetadataShardId::new("services"), root)
        .unwrap()
        .receipt_slot_generation(&stat.object_id, &stat.version, replica)
        .unwrap()
}

/// A receipt publication planned at `generation`, prepared directly by the
/// metadata writer without the publisher's own early compare.
fn receipt_publication(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    stat: &DfsStat,
    replica: &NodeId,
    generation: u64,
) -> crate::DfsPreparedOperationPayload {
    let object = service
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let scope = service
        .metadata_effect_scope_for_object(&object, format!("metadata:stale-receipt-{generation}"))
        .unwrap();
    crate::DfsPreparedOperationPayload::for_metadata_effects(
        scope,
        vec![DfsNamespaceObjectVersionExpectation {
            object_id: object.object_id.clone(),
            expected_version: Some(object.version.clone()),
        }],
        vec![DfsNamespacePathOccupantExpectation {
            path: object.path.clone(),
            expected_object_id: Some(object.object_id.clone()),
        }],
        vec![DfsPreparedMetadataEffect::PutReceipt {
            receipt: DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    committed_version: object.version.clone(),
                    owner_node_id: service.node_id().clone(),
                    event_id: sinomo_core::events::EventId::generate(),
                },
                replica_node_id: replica.clone(),
                content_hash: stat.content_hash.clone(),
                received_at: time::OffsetDateTime::now_utc(),
                generation,
            },
        }],
        time::OffsetDateTime::now_utc(),
    )
    .unwrap()
}

fn journal_head(service: &NetworkedDfsService<DfsNodeTransport>) -> u64 {
    service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap()
}

fn current_locations(
    service: &NetworkedDfsService<DfsNodeTransport>,
    stat: &DfsStat,
) -> Vec<NodeId> {
    service
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap()
        .current_locations
}

#[test]
fn receipt_publisher_rejects_a_superseded_generation_and_publishes_once() {
    let (runtime, mut dfs, stat, receipt) = native_withdrawn_local_replica_fixture(
        "native-receipt-generation-publisher",
        b"receipt generation publisher bytes",
    );
    let replica = receipt.replica_node_id.clone();
    let hash = receipt.content_hash.clone().unwrap();
    assert_eq!(
        receipt.generation, 0,
        "the fixture's receipt was published before any withdrawal"
    );
    assert_eq!(
        slot_generation(&dfs, &stat, &replica),
        1,
        "the fixture withdrew the slot"
    );
    let before = journal_head(&dfs);
    let error = dfs
        .put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &replica,
            &hash,
            receipt.generation,
        )
        .expect_err("evidence planned before the withdrawal is superseded");
    assert_eq!(
        error,
        DfsError::ReceiptGenerationSuperseded {
            object_id: stat.object_id.clone(),
            version: stat.version.clone(),
            replica_node_id: replica.clone(),
            expected: 0,
            current: 1,
        }
    );
    assert_eq!(
        journal_head(&dfs),
        before,
        "a superseded publication writes nothing"
    );
    assert!(replica_receipt(&dfs, &stat, &replica).is_empty());

    assert!(
        dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &replica,
            &hash,
            1,
        )
        .unwrap(),
        "evidence planned at the current generation publishes"
    );
    let published = replica_receipt(&dfs, &stat, &replica);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].generation, 1);
    let after = journal_head(&dfs);
    assert!(
        !dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &replica,
            &hash,
            1,
        )
        .unwrap(),
        "a redelivered publication at the same generation is a no-op"
    );
    assert_eq!(journal_head(&dfs), after);
    assert_eq!(replica_receipt(&dfs, &stat, &replica), published);
    assert!(matches!(
        dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &replica,
            &hash,
            0,
        ),
        Err(DfsError::ReceiptGenerationSuperseded {
            expected: 0,
            current: 1,
            ..
        })
    ));
    assert_eq!(journal_head(&dfs), after);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn transfer_messages_carry_the_planned_generation_explicitly() {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct UngenerationedRepairCommand {
        request_id: String,
        background_repair: bool,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        #[serde(default)]
        expected_base_version: Option<DfsVersion>,
        version: DfsVersion,
        content_hash: String,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
        #[serde(default)]
        object_metadata: Option<DfsObjectMetadata>,
        authority_shard: Option<DfsMetadataShardConfig>,
    }
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let mut command = DfsRepairReplicaCommand {
        background_repair: true,
        request_id: "receipt-generation-wire".into(),
        intent_id: "receipt-generation-intent".into(),
        file_system_id: sifs_file_system_id(),
        object_id: DfsObjectId::from_legacy_named("receipt-generation-object").unwrap(),
        expected_base_version: None,
        version: DfsVersion::from_legacy_named("receipt-generation-version").unwrap(),
        content_hash: "blake3:receipt-generation-wire".into(),
        bytes: Vec::new(),
        object_metadata: None,
        authority_shard: None,
        receipt_generation: 0,
    };
    // A never-withdrawn slot keeps the existing encoding.
    let encoded = codec.encode(&command).unwrap();
    assert!(
        serde_json::to_value(&command)
            .unwrap()
            .get("receipt_generation")
            .is_none()
    );
    assert!(
        codec
            .decode::<UngenerationedRepairCommand>(&encoded)
            .is_ok()
    );
    // Any advanced generation is explicit: a peer without generations
    // rejects the command instead of acting on it.
    command.receipt_generation = 4;
    let encoded = codec.encode(&command).unwrap();
    assert_eq!(
        codec.decode::<DfsRepairReplicaCommand>(&encoded).unwrap(),
        command
    );
    assert!(
        codec
            .decode::<UngenerationedRepairCommand>(&encoded)
            .is_err()
    );
    let response = DfsRepairReplicaResponse {
        request_id: command.request_id.clone(),
        intent_id: command.intent_id.clone(),
        file_system_id: command.file_system_id.clone(),
        object_id: command.object_id.clone(),
        version: command.version.clone(),
        replica_node_id: NodeId::from_legacy_named("receipt-generation-replica").unwrap(),
        result: DfsBlobTransferResult::Committed {
            content_hash: command.content_hash.clone(),
        },
        receipt_generation: command.receipt_generation,
    };
    let encoded = codec.encode(&response).unwrap();
    assert_eq!(
        codec.decode::<DfsRepairReplicaResponse>(&encoded).unwrap(),
        response
    );
    let mut stage = DfsStageBlobCommand {
        request_id: "receipt-generation-stage".into(),
        intent_id: command.intent_id.clone(),
        file_system_id: command.file_system_id.clone(),
        object_id: command.object_id.clone(),
        expected_base_version: None,
        version: command.version.clone(),
        content_hash: command.content_hash.clone(),
        bytes: Vec::new(),
        object_metadata: None,
        authority_shard: None,
        receipt_generation: 2,
    };
    let encoded = codec.encode(&stage).unwrap();
    assert_eq!(
        codec.decode::<DfsStageBlobCommand>(&encoded).unwrap(),
        stage
    );
    stage.receipt_generation = 0;
    let mut legacy = serde_json::to_value(&stage).unwrap();
    assert!(legacy.get("receipt_generation").is_none());
    legacy["receipt_generation"] = serde_json::json!(0);
    assert_eq!(
        serde_json::from_value::<DfsStageBlobCommand>(legacy).unwrap(),
        stage
    );
}

#[test]
fn repair_reply_planned_before_withdrawal_cannot_publish() {
    let name = "native-receipt-generation-late-reply";
    let (runtime, owner, replica, mut service) = native_missing_content_repair(name);
    let path = format!("/services/{name}.bin");
    let (execution, hold) = native_private_repair_owner(&runtime, &service, &path, ());
    let bytes = (0..64 * 1024 + 7)
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    let stat = service
        .posix_write_file(
            &path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    // The direct Core write commits at W=1; the shard tables' repair index
    // holds the replica as a need of the committed owner intent.
    let needs = indexed_repair_needs(&runtime, &service, &stat);
    assert_eq!(needs.len(), 1);
    let (target, owner_intent) = &needs[0];
    assert_eq!(target, &replica);
    let intent_id = owner_intent
        .clone()
        .expect("the committed write owns the replica's need");
    runtime.clear_faults();
    let data_root = runtime.data_dir(&owner).root();
    assert_eq!(
        executor_send_once(&runtime, &mut service, &data_root),
        1,
        "the executor attempts the need once"
    );
    // The real receiver stored the bytes; only its reply is still queued.
    let received = runtime.client(&replica).unwrap();
    assert_eq!(
        received
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .0
            .content_hash,
        stat.content_hash.clone().unwrap()
    );
    drop(received);
    let late = await_repair_reply(&mut service, &intent_id, &replica);
    let DfsNetworkMessage::RepairReplicaResponse(response) = &late.message else {
        unreachable!()
    };
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Committed { content_hash }
            if Some(content_hash) == stat.content_hash.as_ref()),
        "actual receiver accepted the bytes: {response:?}"
    );
    assert_eq!(
        response.receipt_generation, 0,
        "the attempt planned against the never-withdrawn slot"
    );
    assert!(replica_receipt(&service, &stat, &replica).is_empty());
    let locations_before = current_locations(&service, &stat);
    assert_eq!(locations_before, vec![owner.clone()]);

    // A failed read from the target withdraws its slot through the ordinary
    // location-withdrawal producer. The reply above was planned before it.
    service
        .mark_remote_read_location_missing(
            &stat.object_id,
            &stat.version,
            &stat.content_hash,
            &replica,
        )
        .unwrap();
    let locations_withdrawn = current_locations(&service, &stat);
    assert_eq!(locations_withdrawn, locations_before);
    assert_eq!(slot_generation(&service, &stat, &replica), 1);
    assert_eq!(
        replayed_slot_generation(&runtime, &owner, &stat, &replica),
        1,
        "the withdrawal tombstone is durable in the shard journal"
    );
    // The metadata writer itself enforces the compare, independent of the
    // publisher's early check, before anything becomes durable.
    let journal = service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let stale = receipt_publication(&mut service, &stat, &replica, 0);
    match service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(stale, None)
    {
        Err(MetadataBackendError::ReceiptGenerationSuperseded {
            expected, current, ..
        }) => assert_eq!((expected, current), (0, 1)),
        other => panic!("stale receipt preparation must be superseded: {other:?}"),
    }
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        journal
    );

    let events = service
        .process_routed_messages_with_options(vec![late.clone()], true)
        .unwrap();
    assert!(
        !events.iter().any(|event| matches!(event,
            NetworkedDfsServiceEvent::ReplicaRepaired { intent_id: repaired, replica_node_id, .. }
                if repaired == &intent_id && replica_node_id == &replica)),
        "a reply planned before the withdrawal must not settle the slot: {events:?}"
    );
    assert!(
        replica_receipt(&service, &stat, &replica).is_empty(),
        "no receipt is published for the withdrawn slot"
    );
    assert_eq!(current_locations(&service, &stat), locations_withdrawn);
    assert_eq!(
        indexed_repair_needs(&runtime, &service, &stat),
        vec![(replica.clone(), Some(intent_id.clone()))],
        "the superseded attempt leaves the deficit for a new attempt"
    );

    // The withdrawal changed the need's slot: the executor's next attempt
    // plans against the current slot and publishes once.
    assert_eq!(executor_send_once(&runtime, &mut service, &data_root), 1);
    let current = await_repair_reply(&mut service, &intent_id, &replica);
    let DfsNetworkMessage::RepairReplicaResponse(response) = &current.message else {
        unreachable!()
    };
    assert_eq!(
        response.receipt_generation, 1,
        "the receiver echoes the generation of the new attempt"
    );
    let events = service
        .process_routed_messages_with_options(vec![current.clone()], true)
        .unwrap();
    assert!(
        events.iter().any(|event| matches!(event,
            NetworkedDfsServiceEvent::ReplicaRepaired { intent_id: repaired, replica_node_id, .. }
                if repaired == &intent_id && replica_node_id == &replica)),
        "the current-generation reply settles the slot: {events:?}"
    );
    let published = replica_receipt(&service, &stat, &replica);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].content_hash, stat.content_hash);
    assert_eq!(published[0].generation, 1);
    assert_eq!(slot_generation(&service, &stat, &replica), 1);
    assert_eq!(
        replayed_slot_generation(&runtime, &owner, &stat, &replica),
        1
    );
    let mut settled = current_locations(&service, &stat);
    settled.sort();
    let mut expected = vec![owner.clone(), replica.clone()];
    expected.sort();
    assert_eq!(settled, expected);

    // Redelivering the same reply is idempotent: no metadata is published.
    let journal = service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    for message in [current, late] {
        let events = service
            .process_routed_messages_with_options(vec![message], true)
            .unwrap();
        assert!(
            !events.iter().any(|event| matches!(event,
                NetworkedDfsServiceEvent::ReplicaRepaired { intent_id: repaired, replica_node_id, .. }
                    if repaired == &intent_id && replica_node_id == &replica)),
            "a redelivered reply publishes nothing: {events:?}"
        );
    }
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        journal
    );
    assert_eq!(replica_receipt(&service, &stat, &replica), published);
    drop(execution);
    drop(service);
    drop(hold);
    runtime.close().unwrap();
}
