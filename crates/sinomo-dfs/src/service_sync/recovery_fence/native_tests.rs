//! Native owner + authoritative reservation controls. Fence results remain DATA.
use crate::control_plane::{
    DfsMetadataShardRecoveryBallotRequest, DfsRecoveryBallotBegin, metadata_shard_key,
    metadata_shard_membership_handoff_key,
};
use crate::metadata_api::{DfsLocalRecoveryInventoryKind, DfsSelectedLocalRecoveryFenceData};
use crate::test_runtime::DfsTestRuntime;
use crate::{
    DfsControlPlaneKvClient, DfsControlPlaneStore, DfsMessageTransport,
    DfsMetadataAuthorityOperation, DfsMetadataAuthorityProbe, DfsMetadataBackend,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataJournalFrameBatch, DfsMetadataPrefixProbe, DfsMetadataPrefixProof,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeTransport, DfsPlacementPolicy,
    DfsPlacementScope, DfsPosixSyncKind, DfsPreparedOperationRecord, DfsPreparedOperationStatus,
    DfsReplicationPolicy, DfsWriteOptions, NetworkedDfsService,
};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::kv::KvEntry;
use std::time::{Duration, Instant};

type Request = DfsMetadataShardRecoveryBallotRequest;
type Service = NetworkedDfsService<DfsNodeTransport>;

fn deadline() -> Instant {
    Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
}

struct NativeFenceFixture {
    runtime: DfsTestRuntime,
    shard: DfsMetadataShardConfig,
    acceptor: sinomo_core::node::NodeId,
    selected: bool,
    service: Option<Service>,
    control: Option<Box<dyn DfsControlPlaneKvClient>>,
}

impl NativeFenceFixture {
    fn new(name: &str, selected: bool) -> Self {
        let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
        let mut runtime = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        if selected {
            for node in &nodes {
                runtime
                    .select_original_data_fixture_before_start(node)
                    .unwrap();
            }
        }
        runtime.start_nodes().unwrap();
        let shard = crate::select_metadata_shard_config_for_path(
            &runtime.file_system().file_system_id,
            &runtime.metadata_shards().unwrap(),
            "/services/native-local-recovery-fence.bin",
        )
        .unwrap()
        .unwrap();
        assert_eq!(shard.replica_node_ids.len(), 2);
        // Ordinary native formation leaves real activated cooperative markers
        // on the initial primary. This primitive must not supersede them.
        // Use an existing replica and prove eligibility on its actual store.
        let acceptor = shard.replica_node_ids[0].clone();
        let service = runtime.client(&acceptor).unwrap();
        assert!(service.native_publication_owner_for_observation().is_some());
        let mut peer = runtime.client(&shard.replica_node_ids[0]).unwrap();
        let control = peer
            .control_plane
            .take()
            .expect("actual native peer Raft client");
        drop(peer);
        Self {
            runtime,
            shard,
            acceptor,
            selected,
            service: Some(service),
            control: Some(control),
        }
    }

    fn service(&mut self) -> &mut Service {
        self.service.as_mut().unwrap()
    }

    fn shard_id(&self) -> DfsMetadataShardId {
        DfsMetadataShardId::new(&self.shard.shard_id)
    }

    fn request(&mut self) -> Request {
        let mut store = DfsControlPlaneStore::new(self.control.as_mut().unwrap().as_mut());
        let floor = store
            .metadata_shard_recovery_ballot_floor_before(
                &self.shard.file_system_id,
                &self.shard.shard_id,
                deadline(),
            )
            .unwrap();
        assert_eq!(floor.serving_shard.as_ref(), Some(&self.shard));
        assert!(!floor.handoff_pending);
        Request::prepare(
            &self.shard,
            &self.shard.replica_node_ids[0],
            floor.attempt_floor,
        )
        .unwrap()
    }

    fn reserve(&mut self, request: &Request) {
        let mut store = DfsControlPlaneStore::new(self.control.as_mut().unwrap().as_mut());
        match store
            .begin_metadata_shard_recovery_ballot_reservation(request, deadline())
            .unwrap()
        {
            DfsRecoveryBallotBegin::Reserved(actual) => assert_eq!(&actual, request),
            other => panic!("expected actual authoritative reservation: {other:?}"),
        }
    }

    fn serving_cells(&mut self) -> Vec<Option<KvEntry>> {
        let keys = vec![
            metadata_shard_key(&self.shard.file_system_id, &self.shard.shard_id),
            metadata_shard_membership_handoff_key(&self.shard.file_system_id, &self.shard.shard_id),
        ];
        self.control
            .as_mut()
            .unwrap()
            .get_authoritative_consistent_before(&keys, deadline())
            .unwrap()
    }

    fn prefix(&self) -> DfsMetadataPrefixProof {
        self.service
            .as_ref()
            .unwrap()
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&self.shard_id())
            .unwrap()
            .unwrap()
    }

    fn no_cooperative_marker(&self) {
        let layout = if self.selected {
            Some(
                self.runtime
                    .selected_original_data_layout(&self.acceptor)
                    .unwrap(),
            )
        } else {
            None
        };
        assert!(
            crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                self.runtime.data_dir(&self.acceptor),
                &self.shard_id(),
                layout.as_deref(),
            )
            .unwrap()
            .is_none(),
            "fresh native setup must be eligible; never erase an existing cooperative marker"
        );
    }

    fn current_records(&mut self) -> Vec<DfsPreparedOperationRecord> {
        let id = self.shard_id();
        let backend = self.service().metadata_backend_mut().unwrap();
        backend
            .begin_consistent_read_snapshot_on_shard(&id)
            .unwrap();
        let records = backend.list_prepared_operations();
        backend.end_consistent_read_snapshot_on_shard(&id);
        records
            .unwrap()
            .into_iter()
            .filter(|record| record.digest.shard_id == id)
            .collect()
    }

    fn seed(&mut self, path: &str) -> DfsPreparedOperationRecord {
        let owner = &self.shard.primary_node_id;
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
        policy.required_node_ids = vec![owner.clone()];
        self.runtime
            .apply_placement_policy(owner, path, DfsPlacementScope::SelfOnly, policy)
            .unwrap();
        let mut caller = self.runtime.client(owner).unwrap();
        assert!(caller.transport.delegates_local_namespace_authority());
        let cut = deadline();
        let old = caller.set_transport_operation_deadline(Some(cut));
        let options = DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![owner.clone()],
            wait_for_commit: true,
            foreground_replica_repair: true,
        };
        let first = caller.posix_write_file(path, &[0x62; 4096], options.clone());
        let operation = format!("native-recovery-seed-{}", sinomo_core::Id::generate());
        let overwritten = first.as_ref().ok().map(|first| {
            caller.posix_replace_file_if_version_from_reader_with_operation_id(
                path,
                &first.object_id,
                &first.version,
                std::io::Cursor::new(vec![0x63; 4096]),
                options,
                &operation,
            )
        });
        let sync = if overwritten.as_ref().is_some_and(|result| result.is_ok()) {
            Some(caller.posix_sync_file(path, DfsPosixSyncKind::File))
        } else {
            None
        };
        caller.set_transport_operation_deadline(old);
        let first = first.unwrap();
        let overwritten = overwritten.unwrap().unwrap();
        assert_eq!(overwritten.object_id, first.object_id);
        assert_ne!(overwritten.version, first.version);
        sync.unwrap().unwrap();
        assert!(
            Instant::now() <= cut,
            "seed write and sync retain the same original 5s cap"
        );
        drop(caller);
        // A fresh Core handle alone need not refresh an existing native table
        // projection. Read the exact normal overwrite through the existing
        // canonical read scope, then release it before any journal lock.
        let metadata_operation = format!(
            "write:{}",
            crate::intent::stable_write_intent_id(owner, &first.object_id, &operation,)
        );
        let mut primary = self.runtime.metadata_primary_service(path).unwrap();
        let id = self.shard_id();
        let backend = primary.metadata_backend_mut().unwrap();
        backend
            .begin_consistent_read_snapshot_on_shard(&id)
            .unwrap();
        let record = backend.prepared_operation_record(&metadata_operation);
        backend.end_consistent_read_snapshot_on_shard(&id);
        let record = record
            .unwrap()
            .expect("actual Main stable overwrite has its original canonical V7 record");
        assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
        assert_eq!(record.digest.operation_id, metadata_operation);
        assert_eq!(record.digest.shard_id, id);
        assert_eq!(record.digest.owner_epoch, self.shard.epoch);
        assert_eq!(record.digest.file_system_id, self.shard.file_system_id);
        assert_eq!(
            record
                .payload
                .terminal_intent
                .as_ref()
                .unwrap()
                .target_version,
            overwritten.version
        );
        assert_eq!(
            record.digest.payload_digest,
            record.payload.canonical_digest().unwrap()
        );
        drop(primary);
        drop(self.service.take());
        self.service = Some(self.runtime.client(&self.acceptor).unwrap());
        self.current_records();
        record
    }
}

fn with_native(name: &str, selected: bool, check: impl FnOnce(&mut NativeFenceFixture)) {
    let mut fixture = NativeFenceFixture::new(name, selected);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&mut fixture)));
    drop(fixture.service.take());
    drop(fixture.control.take());
    fixture.runtime.clear_faults();
    let closed = fixture.runtime.close();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}

fn same_data(
    actual: &DfsSelectedLocalRecoveryFenceData,
    expected: &DfsSelectedLocalRecoveryFenceData,
) {
    assert_eq!(actual.fence, expected.fence);
    assert_eq!(actual.marker_bytes, expected.marker_bytes);
}

fn exact_frames(
    runtime: &DfsTestRuntime,
    node: &sinomo_core::node::NodeId,
    shard: &DfsMetadataShardConfig,
) -> Vec<Vec<u8>> {
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let root = crate::local_dfs_metadata_shard_path(runtime.data_dir(node), &id);
    let layout = runtime.selected_original_data_layout(node).unwrap();
    let cut = deadline();
    loop {
        layout.stage(&shard.file_system_id, &id, &root).unwrap();
        if let Some(_guard) = crate::metadata_lock::try_metadata_inspection_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .unwrap()
        {
            // Same actual selected physical shard, with its existing shared
            // writer fence held. Never call a locking backend wrapper here.
            let entries = crate::metadata_journal::durable_journal_entries_since_limited(
                &root,
                0,
                crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            )
            .unwrap();
            assert!(entries.len() < crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
            let frames = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
                &root,
                &entries
                    .iter()
                    .map(|entry| entry.sequence)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(entries.len(), frames.len());
            let result = entries
                .iter()
                .zip(frames)
                .map(|(entry, frame)| {
                    let frame = frame.expect("actual retained frame at the observed sequence");
                    assert_eq!(&entry.shard_id, &id);
                    assert_eq!(frame.entry(), entry);
                    frame.exact_bytes().to_vec()
                })
                .collect();
            layout.stage(&shard.file_system_id, &id, &root).unwrap();
            assert!(Instant::now() <= cut);
            return result;
        }
        assert!(Instant::now() < cut, "same physical inspection deadline");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn send_native(
    runtime: &DfsTestRuntime,
    source: &sinomo_core::node::NodeId,
    target: &sinomo_core::node::NodeId,
    command: &DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, String> {
    let mut client = runtime.client(source).unwrap();
    let cut = deadline();
    let old = client.set_transport_operation_deadline(Some(cut));
    let result = client
        .transport
        .send_metadata_journal_batches_direct(&[(target.clone(), command.clone())]);
    client.set_transport_operation_deadline(old);
    assert!(
        Instant::now() <= cut,
        "actual metadata RPC did not renew its 5s cap"
    );
    let replies = result
        .map_err(|error| error.to_string())?
        .ok_or("actual native metadata direct transport unavailable")?;
    if replies.is_empty() {
        return Err("native metadata transport returned no domain response".into());
    }
    assert_eq!(replies.len(), 1);
    let (actual_node, response) = replies.into_iter().next().unwrap();
    assert_eq!(&actual_node, target);
    assert_eq!(response.request_id, command.request_id);
    assert_eq!(response.shard_id, command.batch.shard_id);
    assert_eq!(
        response.next_after_sequence,
        command.batch.next_after_sequence
    );
    Ok(response)
}

#[test]
fn native_selected_local_recovery_fence_refuses_actual_replica_ingest_and_all_ack_purposes() {
    with_native("native-local-recovery-fence-ingress", true, |fixture| {
        let record = fixture.seed("/services/native-local-recovery-fence.bin");
        fixture.no_cooperative_marker();
        let shard_id = fixture.shard_id();
        let primary = fixture.shard.primary_node_id.clone();
        let target = fixture.acceptor.clone();
        let mut source = fixture
            .runtime
            .metadata_primary_service("/services/native-local-recovery-fence.bin")
            .unwrap();
        let backend = source.metadata_backend_mut().unwrap();
        backend
            .begin_consistent_read_snapshot_on_shard(&shard_id)
            .unwrap();
        let current = backend.prepared_operation_record(&record.digest.operation_id);
        let prefix = backend.metadata_prefix_proof(&shard_id);
        backend.end_consistent_read_snapshot_on_shard(&shard_id);
        assert_eq!(current.unwrap(), Some(record.clone()));
        let prefix = prefix.unwrap().unwrap();
        let backend = source.metadata_backend().unwrap();
        let digest = record.digest;
        let batch = backend
            .journal_batch_for_shard_since_limited(
                &shard_id,
                0,
                crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
            .unwrap();
        assert!(!batch.entries.is_empty());
        assert!(
            batch.entries.len() < crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            "bounded small fixture must capture the complete real prefix"
        );
        let frames =
            crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
                .unwrap();
        assert_eq!(batch.next_after_sequence, prefix.journal_sequence);
        drop(source);
        let probe = DfsMetadataAuthorityProbe {
            probe_id: "native-local-recovery-actual-authority".into(),
            expected_shard: fixture.shard.clone(),
            operation: DfsMetadataAuthorityOperation::PreparedOperation {
                operation_id: digest.operation_id,
                file_system_id: digest.file_system_id,
                shard_id: digest.shard_id,
                owner_epoch: digest.owner_epoch,
                prepare_sequence: digest.prepare_sequence,
                payload_digest: digest.payload_digest,
            },
            prepared_prefix: prefix.clone(),
        };
        let nonempty = DfsMetadataJournalBatchCommand {
            request_id: "native-local-recovery-nonempty-authority".into(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(fixture.shard.file_system_id.clone()),
            authority_epoch: Some(fixture.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(probe.clone()),
            durable_replication: None,
            batch: frames,
        };
        let mut empty = nonempty.clone();
        empty.request_id = "native-local-recovery-empty-authority".into();
        empty.batch.journal_frames.clear();
        let prefix_command = DfsMetadataJournalBatchCommand {
            request_id: "native-local-recovery-prefix".into(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(fixture.shard.file_system_id.clone()),
            authority_epoch: Some(fixture.shard.epoch),
            verify_prefix_only: true,
            prefix_probe: Some(DfsMetadataPrefixProbe {
                barrier_id: "native-local-recovery-prefix-barrier".into(),
                file_system_id: Some(fixture.shard.file_system_id.clone()),
                authority_epoch: Some(fixture.shard.epoch),
                shard_id: shard_id.clone(),
                journal_sequence: prefix.journal_sequence,
                state_fingerprint: prefix.digest.clone(),
                primary_handoff_id: None,
                repair_custody: None,
            }),
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalFrameBatch {
                shard_id: shard_id.clone(),
                journal_frames: Vec::new(),
                next_after_sequence: prefix.journal_sequence,
            },
        };
        let commands = [nonempty, empty, prefix_command];
        for command in &commands {
            let response = send_native(&fixture.runtime, &primary, &target, command).unwrap();
            assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
            if command.verify_prefix_only {
                assert!(response.verified_prefix_only);
            } else {
                assert_eq!(
                    response.verified_authority_probe_id,
                    crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
                );
            }
        }
        let mut receiver = crate::runtime_host::open_dfs_runtime_service(
            fixture.runtime.data_dir(&target),
            Some(&fixture.runtime.runtime_handle()),
        )
        .unwrap();
        assert_eq!(receiver.node_id(), &target);
        assert!(
            receiver
                .native_publication_owner_for_observation()
                .is_some()
        );
        assert!(
            receiver
                .metadata_backend()
                .unwrap()
                .metadata_shard_write_freeze(&shard_id)
                .unwrap()
                .is_none()
        );
        let backend = receiver.metadata_backend().unwrap();
        let prefix_floor = backend
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        let authority_floor = backend
            .metadata_authority_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(prefix_floor.durable_prefix_proof, prefix);
        assert_eq!(authority_floor.durable_prefix_proof, prefix);
        let request = fixture.request();
        fixture.reserve(&request);
        let serving = fixture.serving_cells();
        let installed = receiver
            .install_selected_local_recovery_fence(&request, deadline())
            .unwrap();
        assert!(installed.reservation_current);
        assert_eq!(installed.data.fence.acceptor_node_id, target);
        assert_eq!(installed.data.fence.prefix, prefix);
        let retained_frames = exact_frames(&fixture.runtime, &target, &fixture.shard);
        for command in &commands {
            let error = match send_native(&fixture.runtime, &primary, &target, command) {
                Ok(response) => {
                    assert!(!response.verified_prefix_only);
                    assert!(response.verified_authority_probe_id.is_none());
                    match response.result {
                        DfsMetadataJournalBatchResult::Rejected { reason, .. } => reason,
                        DfsMetadataJournalBatchResult::Applied => {
                            panic!("post-fence native ACK: {response:?}")
                        }
                    }
                }
                Err(error) => error,
            };
            assert!(
                error.contains("recovery fence"),
                "exact native rejection required: {error}"
            );
            let backend = receiver.metadata_backend().unwrap();
            assert_eq!(
                backend.metadata_prefix_ack_floor(&shard_id).unwrap(),
                Some(prefix_floor.clone())
            );
            assert_eq!(
                backend.metadata_authority_ack_floor(&shard_id).unwrap(),
                Some(authority_floor.clone())
            );
            assert_eq!(
                backend.metadata_prefix_proof(&shard_id).unwrap(),
                Some(prefix.clone())
            );
            assert_eq!(
                exact_frames(&fixture.runtime, &target, &fixture.shard),
                retained_frames
            );
        }
        // Even repeating the exact historical floors cannot mint a new ACK.
        let backend = receiver.metadata_backend_mut().unwrap();
        assert!(
            backend
                .put_metadata_prefix_ack_floor(&prefix_floor)
                .is_err()
        );
        assert!(
            backend
                .put_metadata_authority_ack_floor(&authority_floor)
                .is_err()
        );
        same_data(
            &backend
                .selected_local_recovery_fence(&shard_id)
                .unwrap()
                .unwrap(),
            &installed.data,
        );
        drop(receiver);
        assert_eq!(fixture.serving_cells(), serving);
    });
}

#[test]
fn native_selected_local_recovery_fence_recertifies_without_serving_or_writer_authority() {
    with_native("native-local-recovery-fence", true, |fixture| {
        fixture.seed("/services/native-local-recovery-fence.bin");
        fixture.no_cooperative_marker();
        let shard_id = fixture.shard_id();
        let request = fixture.request();
        fixture.reserve(&request);
        let serving = fixture.serving_cells();
        let unresolved = fixture
            .current_records()
            .into_iter()
            .filter(|record| record.status == DfsPreparedOperationStatus::Prepared)
            .map(|record| record.digest)
            .collect::<Vec<_>>();
        let prefix = fixture.prefix();
        let prefix_floor = fixture
            .service()
            .metadata_backend()
            .unwrap()
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        let authority_floor = fixture
            .service()
            .metadata_backend()
            .unwrap()
            .metadata_authority_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        let primary_marker = crate::local_dfs_metadata_shard_path(
            fixture.runtime.data_dir(&fixture.shard.primary_node_id),
            &shard_id,
        )
        .join("journal/membership-handoff-write-freeze.sobj");
        let primary_bytes = std::fs::read(&primary_marker).unwrap();
        let result = fixture
            .service()
            .install_selected_local_recovery_fence(&request, deadline())
            .unwrap();
        assert!(
            result.reservation_current,
            "{:?}",
            result.post_verification_error
        );
        assert!(result.post_verification_error.is_none());
        assert_eq!(result.data.fence.reservation, request);
        assert_eq!(result.data.fence.acceptor_node_id, fixture.acceptor);
        assert_eq!(result.data.fence.prefix, prefix);
        let retained_frames = exact_frames(&fixture.runtime, &fixture.acceptor, &fixture.shard);
        assert_eq!(
            result.data.fence.inventory_kind,
            DfsLocalRecoveryInventoryKind::LegacyPrimaryAuthoredV7
        );
        let mut actual = result.data.fence.unresolved_prepares.clone();
        let mut expected = unresolved;
        actual.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        expected.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        assert_eq!(
            actual, expected,
            "complete inventory at the actual local cut"
        );
        let root = crate::local_dfs_metadata_shard_path(
            fixture.runtime.data_dir(&fixture.acceptor),
            &shard_id,
        );
        let marker = root.join("journal/membership-handoff-write-freeze.sobj");
        assert_eq!(std::fs::read(&marker).unwrap(), result.data.marker_bytes);
        let duplicate = fixture
            .service()
            .install_selected_local_recovery_fence(&request, deadline())
            .unwrap();
        assert!(duplicate.reservation_current);
        same_data(&duplicate.data, &result.data);
        same_data(
            &fixture
                .service()
                .metadata_backend()
                .unwrap()
                .selected_local_recovery_fence(&shard_id)
                .unwrap()
                .unwrap(),
            &result.data,
        );
        assert_eq!(fixture.prefix(), prefix);
        assert_eq!(
            fixture.serving_cells(),
            serving,
            "reservation/fence is not serving activation"
        );

        let layout = fixture
            .runtime
            .selected_original_data_layout(&fixture.acceptor)
            .unwrap();
        let cold = crate::LocalSobjMetadataBackend::new_with_selected_layout(
            fixture.shard.file_system_id.clone(),
            shard_id.clone(),
            root,
            layout,
        )
        .unwrap();
        same_data(
            &cold
                .selected_local_recovery_fence(&shard_id)
                .unwrap()
                .unwrap(),
            &result.data,
        );
        drop(cold);
        // This fences B only. A retains its serving assignment and its exact
        // cooperative marker; do not reinterpret this as revoking A's ability
        // to finish an already-proved legacy operation.
        let backend = fixture.service().metadata_backend_mut().unwrap();
        assert!(
            backend
                .put_metadata_prefix_ack_floor(&prefix_floor)
                .is_err()
        );
        assert!(
            backend
                .put_metadata_authority_ack_floor(&authority_floor)
                .is_err()
        );
        assert_eq!(
            backend.metadata_prefix_ack_floor(&shard_id).unwrap(),
            Some(prefix_floor)
        );
        assert_eq!(
            backend.metadata_authority_ack_floor(&shard_id).unwrap(),
            Some(authority_floor)
        );
        assert_eq!(std::fs::read(&marker).unwrap(), result.data.marker_bytes);
        assert_eq!(fixture.prefix(), prefix);
        assert_eq!(
            exact_frames(&fixture.runtime, &fixture.acceptor, &fixture.shard),
            retained_frames
        );
        assert_eq!(fixture.serving_cells(), serving);
        assert_eq!(std::fs::read(&primary_marker).unwrap(), primary_bytes);
    });
}

#[test]
fn native_selected_local_recovery_fence_refuses_expired_malformed_and_spent_requests() {
    with_native("native-local-recovery-fence-refusals", true, |fixture| {
        fixture.no_cooperative_marker();
        let shard_id = fixture.shard_id();
        let request = fixture.request();
        fixture.reserve(&request);
        let prefix = fixture.prefix();
        assert!(
            fixture
                .service()
                .install_selected_local_recovery_fence(&request, Instant::now())
                .is_err()
        );
        let mut changed = request.clone();
        changed.reservation_id.push_str("-changed");
        assert!(
            fixture
                .service()
                .install_selected_local_recovery_fence(&changed, deadline())
                .is_err()
        );
        let mut store = DfsControlPlaneStore::new(fixture.control.as_mut().unwrap().as_mut());
        assert!(
            store
                .cancel_metadata_shard_recovery_ballot_reservation(&request, deadline())
                .unwrap()
        );
        let newer = fixture.request();
        fixture.reserve(&newer);
        assert!(
            fixture
                .service()
                .install_selected_local_recovery_fence(&request, deadline())
                .is_err()
        );
        assert!(
            fixture
                .service()
                .metadata_backend()
                .unwrap()
                .selected_local_recovery_fence(&shard_id)
                .unwrap()
                .is_none()
        );
        fixture.no_cooperative_marker();
        assert_eq!(fixture.prefix(), prefix);
        let result = fixture
            .service()
            .install_selected_local_recovery_fence(&newer, deadline())
            .unwrap();
        assert!(result.reservation_current);
        assert_eq!(result.data.fence.reservation, newer);
    });
}

#[test]
fn native_default_owner_cannot_install_a_selected_local_recovery_fence() {
    with_native(
        "native-default-local-recovery-fence-refusal",
        false,
        |fixture| {
            fixture.no_cooperative_marker();
            let request = fixture.request();
            fixture.reserve(&request);
            let prefix = fixture.prefix();
            let serving = fixture.serving_cells();
            assert!(
                fixture
                    .service()
                    .install_selected_local_recovery_fence(&request, deadline())
                    .is_err()
            );
            fixture.no_cooperative_marker();
            assert_eq!(fixture.prefix(), prefix);
            assert_eq!(fixture.serving_cells(), serving);
        },
    );
}

#[test]
fn native_local_recovery_refuses_pending_and_activated_cooperative_markers_without_rewriting_them()
{
    with_native(
        "native-recovery-preserves-pending-cooperative",
        true,
        |fixture| {
            fixture.no_cooperative_marker();
            let request = fixture.request();
            fixture.reserve(&request);
            let original = fixture.shard.clone();
            let shard_id = fixture.shard_id();
            let mut source = fixture
                .runtime
                .metadata_primary_service("/services/native-local-recovery-fence.bin")
                .unwrap();
            // This is the existing source publication fence and actual accepted
            // cooperative proposal. It supplies no recovery/native RPC authority.
            let source_fence = source
                .metadata_shard_fence_guard(&original.file_system_id, &original.shard_id)
                .unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let pending = control
                .with_client(|kv| {
                    DfsControlPlaneStore::new(kv.as_mut()).request_metadata_shard_primary_handoff(
                        &original,
                        &original.replica_node_ids[0],
                    )
                })
                .unwrap()
                .unwrap();
            let freeze = source
                .metadata_backend_mut()
                .unwrap()
                .freeze_metadata_shard_for_membership_handoff(
                    &shard_id,
                    &pending.handoff_id,
                    original.epoch,
                )
                .unwrap()
                .unwrap();
            assert_eq!(freeze.handoff_id, pending.handoff_id);
            assert_eq!(freeze.retiring_epoch, original.epoch);
            assert!(freeze.activated_epoch.is_none());
            let path = crate::local_dfs_metadata_shard_path(
                fixture.runtime.data_dir(&original.primary_node_id),
                &shard_id,
            )
            .join("journal/membership-handoff-write-freeze.sobj");
            let bytes = std::fs::read(&path).unwrap();
            let serving = fixture.serving_cells();
            assert!(
                source
                    .install_selected_local_recovery_fence(&request, deadline())
                    .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(
                source
                    .metadata_backend()
                    .unwrap()
                    .metadata_shard_write_freeze(&shard_id)
                    .unwrap(),
                Some(freeze)
            );
            assert_eq!(fixture.serving_cells(), serving);
            drop(source_fence);
            drop(source);
        },
    );
    with_native(
        "native-recovery-preserves-activated-cooperative",
        true,
        |fixture| {
            fixture.no_cooperative_marker();
            let original = fixture.shard.clone();
            let promoted = original.replica_node_ids[0].clone();
            let replicas = crate::control_plane::topology_preserving_metadata_shard_replicas(
                &original, &promoted,
            )
            .unwrap();
            // The maintained native handoff performs the actual freeze, full joint
            // participant proof, assignment transition and promoted rebind.
            let handoff_started = Instant::now();
            let active = fixture
                .runtime
                .reassign_metadata_shard(
                    &original.file_system_id,
                    &original.shard_id,
                    promoted.clone(),
                    replicas,
                )
                .unwrap();
            assert!(handoff_started.elapsed() <= Duration::from_secs(45));
            assert_eq!(active.primary_node_id, promoted);
            assert!(active.epoch > original.epoch);
            drop(fixture.service.take());
            fixture.service = Some(
                fixture
                    .runtime
                    .metadata_primary_service("/services/native-local-recovery-fence.bin")
                    .unwrap(),
            );
            fixture.shard = active.clone();
            fixture.acceptor = active.primary_node_id.clone();
            let shard_id = fixture.shard_id();
            let marker = fixture
                .service()
                .metadata_backend()
                .unwrap()
                .metadata_shard_write_freeze(&shard_id)
                .unwrap()
                .unwrap();
            assert_eq!(marker.activated_epoch, Some(active.epoch));
            let path = crate::local_dfs_metadata_shard_path(
                fixture.runtime.data_dir(&active.primary_node_id),
                &shard_id,
            )
            .join("journal/membership-handoff-write-freeze.sobj");
            let bytes = std::fs::read(&path).unwrap();
            let request = fixture.request();
            fixture.reserve(&request);
            let serving = fixture.serving_cells();
            let prefix = fixture.prefix();
            let error = fixture
                .service()
                .install_selected_local_recovery_fence(&request, deadline())
                .expect_err("an actual activated cooperative marker is not supersedable");
            assert!(error.to_string().contains("cooperative"), "{error}");
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(
                fixture
                    .service()
                    .metadata_backend()
                    .unwrap()
                    .metadata_shard_write_freeze(&shard_id)
                    .unwrap(),
                Some(marker)
            );
            assert_eq!(fixture.prefix(), prefix);
            assert_eq!(fixture.serving_cells(), serving);
        },
    );
}

#[test]
fn native_recovery_context_refuses_a_different_actual_data_owner_before_marker_publication() {
    with_native("native-recovery-foreign-owner", true, |fixture| {
        fixture.no_cooperative_marker();
        let owner_node = fixture.acceptor.clone();
        let foreign_node = fixture.shard.primary_node_id.clone();
        let (_, owner) = fixture
            .service()
            .native_publication_owner_for_observation()
            .unwrap();
        let foreign_layout = fixture
            .runtime
            .selected_original_data_layout(&foreign_node)
            .unwrap();
        let prefix = fixture.prefix();
        let shard_id = fixture.shard_id();
        let path =
            crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&owner_node), &shard_id)
                .join("journal/membership-handoff-write-freeze.sobj");
        assert!(!path.exists());
        assert!(crate::metadata_api::DfsSelectedLocalRecoveryContext::from_owner(
            foreign_layout, owner,
        ).is_err(), "actual selected layout and actual DATA owner must belong together");
        assert!(!path.exists());
        assert_eq!(fixture.prefix(), prefix);
        fixture.no_cooperative_marker();
    });
}
