//! Owning local-file/codec controls. The reservation and seeded V7 bodies here
//! are DATA inputs, not a Raft verification, authenticated RPC or chosen value.
use super::*;
use crate::control_plane::DfsMetadataShardRecoveryBallotRequest;
use crate::metadata_api::{
    DfsSelectedLocalRecoveryContext, DfsSelectedLocalRecoveryFenceData,
    SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT,
};

struct Fixture {
    backend: LocalSobjMetadataBackend,
    context: DfsSelectedLocalRecoveryContext,
    layout: Arc<crate::DfsSelectedOriginalDataLayout>,
    request: DfsMetadataShardRecoveryBallotRequest,
    _root: sinomo_testdir::TestDir,
}

fn fixture(name: &str) -> Fixture {
    let root = sinomo_testdir::TestDir::new(name);
    let data = sinomo_core::local::SinomoDataDir::new(root.path());
    data.ensure_root_layout().unwrap();
    let local = NodeId::generate();
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        sinomo_core::node::ClusterId::generate(),
        name,
        local.clone(),
        name,
        format!("fixture-{local}"),
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(
        &sinomo_core::network::ClusterPaths::new(&data),
        &state,
    )
    .unwrap();
    let mut owner = crate::runtime_host::DfsDataOwnerLock::acquire(
        &crate::runtime_host::DfsRuntimePaths::new(&data),
    )
    .unwrap();
    let layout = owner.admit_selected_original_fixture(None).unwrap();
    let context =
        DfsSelectedLocalRecoveryContext::from_owner(Arc::clone(&layout), Arc::new(owner)).unwrap();
    let serving = DfsMetadataShardConfig {
        file_system_id: file_system_id(),
        shard_id: "local-recovery-file-control".into(),
        primary_node_id: local,
        replica_node_ids: vec![NodeId::generate(), NodeId::generate()],
        epoch: 7,
        assignment: crate::DfsMetadataShardAssignment::Root,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let request =
        DfsMetadataShardRecoveryBallotRequest::prepare(&serving, &serving.replica_node_ids[0], 0)
            .unwrap();
    let shard = DfsMetadataShardId::new(&serving.shard_id);
    let path = crate::default_dfs_metadata_shards_root(&data).join(&serving.shard_id);
    let backend = LocalSobjMetadataBackend::new_with_selected_layout(
        serving.file_system_id.clone(),
        shard,
        path,
        Arc::clone(&layout),
    )
    .unwrap();
    Fixture {
        backend,
        context,
        layout,
        request,
        _root: root,
    }
}

fn install(f: &mut Fixture) -> Result<DfsSelectedLocalRecoveryFenceData> {
    f.backend
        .install_selected_local_recovery_fence(&f.request, &f.context)
}

fn marker(f: &Fixture) -> PathBuf {
    f.backend.membership_handoff_write_freeze_path()
}

fn seed_prepare_data(f: &mut Fixture, count: usize) {
    // These are synthetic valid codec inputs written through the actual
    // journal owner under its lock, then replayed by the real backend. This
    // deliberately does NOT mint a native author/replica permission.
    let _writer = f.backend.metadata_write_lock().unwrap();
    f.backend.refresh_after_metadata_write_lock().unwrap();
    let filesystem = DfsFileSystem {
        file_system_id: f.request.serving_shard.file_system_id.clone(),
        display_name: "file-backed inventory DATA".into(),
        owner_node_id: f.context.local_node_id().clone(),
        subtree_policies: Vec::new(),
        retention_policies: Default::default(),
        module_metadata: Default::default(),
    };
    for index in 0..count {
        let operation_id = format!("inventory-data-{index:04}");
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: filesystem.file_system_id.clone(),
                shard_id: f.backend.shard_id.clone(),
                owner_epoch: f.request.serving_shard.epoch,
                operation_id: operation_id.clone(),
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: filesystem.clone(),
            }],
            OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let sequence = f.backend.next_sequence;
        let record = DfsPreparedOperationRecord {
            digest: crate::DfsPreparedOperationDigest::at_sequence(&payload, sequence).unwrap(),
            payload: Arc::new(payload),
            status: DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        record.validate().unwrap();
        let entry = DfsMetadataJournalEntry {
            shard_id: f.backend.shard_id.clone(),
            sequence,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PrepareOperationV7 {
                file_system_id: filesystem.file_system_id.clone(),
                operation_id,
                record: Box::new(record),
            },
        };
        let frame = crate::metadata_journal::DfsMetadataJournalFrame::encode(&entry).unwrap();
        crate::metadata_journal::write_local_journal_frame_with_bytes(
            &f.backend.root,
            &mut f.backend.next_sequence,
            &frame,
        )
        .unwrap();
    }
    f.backend.refresh_after_metadata_write_lock().unwrap();
    assert_eq!(
        f.backend
            .tables
            .prepared_operations_v7
            .values()
            .filter(|record| record.status == DfsPreparedOperationStatus::Prepared)
            .count(),
        count
    );
}

#[test]
fn selected_recovery_file_fence_preserves_exact_duplicate_checkpoint_and_cold_cut() {
    let mut f = fixture("selected-recovery-local-duplicate");
    seed_prepare_data(&mut f, 2);
    let first = install(&mut f).unwrap();
    assert_eq!(first.fence.unresolved_prepares.len(), 2);
    assert_eq!(first.fence.acceptor_node_id, *f.context.local_node_id());
    assert_eq!(f.backend.instance_authority_epoch, None);
    assert_eq!(install(&mut f).unwrap(), first);
    assert_eq!(std::fs::read(marker(&f)).unwrap(), first.marker_bytes);
    assert!(f.backend.begin_metadata_write().is_err());
    let checkpoint = f.backend.persist_current_metadata_checkpoint_for_test();
    if let Err(error) = checkpoint {
        assert!(error.to_string().contains("selected local recovery fence"));
    }
    assert_eq!(std::fs::read(marker(&f)).unwrap(), first.marker_bytes);
    let mut cold = LocalSobjMetadataBackend::new_with_selected_layout(
        f.request.serving_shard.file_system_id.clone(),
        f.backend.shard_id.clone(),
        f.backend.root.clone(),
        Arc::clone(&f.layout),
    )
    .unwrap();
    assert_eq!(cold.instance_authority_epoch, None);
    assert_eq!(
        cold.selected_local_recovery_fence(&cold.shard_id).unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        cold.install_selected_local_recovery_fence(&f.request, &f.context)
            .unwrap(),
        first
    );
    let later = DfsMetadataShardRecoveryBallotRequest::prepare(
        &f.request.serving_shard,
        &f.request.ballot.candidate_node_id,
        1,
    )
    .unwrap();
    let advanced = cold
        .install_selected_local_recovery_fence(&later, &f.context)
        .unwrap();
    assert_eq!(advanced.fence.prefix, first.fence.prefix);
    assert_eq!(
        advanced.fence.unresolved_prepares,
        first.fence.unresolved_prepares
    );
    assert!(
        cold.install_selected_local_recovery_fence(&f.request, &f.context)
            .is_err()
    );
    assert_eq!(std::fs::read(marker(&f)).unwrap(), advanced.marker_bytes);
}

#[test]
fn selected_recovery_namespace_failure_withholds_reply_until_same_bytes_recertify() {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::metadata_api::FAIL_SELECTED_LOCAL_RECOVERY_NAMESPACE.set(self.0);
        }
    }
    let mut f = fixture("selected-recovery-local-namespace");
    let _restore =
        Restore(crate::metadata_api::FAIL_SELECTED_LOCAL_RECOVERY_NAMESPACE.replace(true));
    let error = install(&mut f).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected selected local recovery namespace failure")
    );
    assert_eq!(f.backend.instance_authority_epoch, None);
    let historical = f
        .backend
        .selected_local_recovery_fence(&f.backend.shard_id)
        .unwrap()
        .unwrap();
    assert!(
        f.backend
            .metadata_shard_authoritative_writes_frozen(&f.backend.shard_id)
            .unwrap()
    );
    crate::metadata_api::FAIL_SELECTED_LOCAL_RECOVERY_NAMESPACE.set(true);
    assert!(
        install(&mut f)
            .unwrap_err()
            .to_string()
            .contains("namespace failure")
    );
    assert_eq!(std::fs::read(marker(&f)).unwrap(), historical.marker_bytes);
    assert_eq!(install(&mut f).unwrap(), historical);
}

#[test]
fn selected_recovery_prepare_inventory_is_complete_at_bound_and_overflow_does_not_publish() {
    let mut full = fixture("selected-recovery-inventory-bound");
    seed_prepare_data(&mut full, SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT);
    let data = install(&mut full).unwrap();
    assert_eq!(
        data.fence.unresolved_prepares.len(),
        SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT
    );
    assert_eq!(
        data.fence.prefix.journal_sequence,
        SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT as u64
    );
    assert!(
        data.fence
            .unresolved_prepares
            .windows(2)
            .all(|pair| pair[0].operation_id < pair[1].operation_id)
    );
    let mut over = fixture("selected-recovery-inventory-overflow");
    seed_prepare_data(&mut over, SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT + 1);
    let before = over.backend.prefix_proof.clone();
    assert!(
        install(&mut over)
            .unwrap_err()
            .to_string()
            .contains("inventory exceeds its bound")
    );
    assert!(!marker(&over).exists());
    assert_eq!(over.backend.prefix_proof, before);
    assert_eq!(
        over.backend.tables.prepared_operations_v7.len(),
        SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT + 1
    );
}

#[test]
fn selected_recovery_malformed_and_mixed_tags_cannot_issue_floors_or_erase_bytes() {
    let mut f = fixture("selected-recovery-malformed-envelope");
    let prefix = f.backend.prefix_proof.clone().unwrap();
    let prefix_floor = DfsMetadataPrefixAckFloor {
        shard_id: f.backend.shard_id.clone(),
        journal_sequence: prefix.journal_sequence,
        state_fingerprint: String::new(),
        durable_prefix_proof: prefix.clone(),
    };
    let authority_floor = DfsMetadataAuthorityAckFloor {
        shard_id: f.backend.shard_id.clone(),
        journal_sequence: prefix.journal_sequence,
        durable_prefix_proof: prefix.clone(),
    };
    f.backend
        .put_metadata_prefix_ack_floor(&prefix_floor)
        .unwrap();
    f.backend
        .put_metadata_authority_ack_floor(&authority_floor)
        .unwrap();
    let cooperative = f
        .backend
        .freeze_for_membership_handoff("old-cooperative-data", f.request.serving_shard.epoch)
        .unwrap();
    let cooperative_bytes = std::fs::read(marker(&f)).unwrap();
    assert!(install(&mut f).is_err());
    assert_eq!(std::fs::read(marker(&f)).unwrap(), cooperative_bytes);
    #[derive(serde::Serialize)]
    struct Malformed {
        selected_local_recovery_fence_v1: String,
    }
    // A VALID cooperative envelope beside the new malformed tag must not win
    // an untagged-deserializer fallback. These are codec DATA, not a handoff.
    let mut desired = f.request.serving_shard.clone();
    desired.primary_node_id = f.request.ballot.candidate_node_id.clone();
    desired.replica_node_ids = vec![
        f.request.serving_shard.primary_node_id.clone(),
        f.request.serving_shard.replica_node_ids[1].clone(),
    ];
    desired.epoch += 1;
    let cut = crate::DfsRepairHandoffCut {
        version: 1,
        handoff: crate::DfsMetadataShardMembershipHandoff {
            cause: crate::DfsMetadataShardHandoffCause::Membership,
            handoff_id: cooperative.handoff_id.clone(),
            retiring_shard: f.request.serving_shard.clone(),
            desired_shard: desired,
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: Some(crate::DfsMetadataShardMembershipHandoffFence {
                handoff_id: cooperative.handoff_id.clone(),
                primary_node_id: f.request.serving_shard.primary_node_id.clone(),
                journal_sequence: cooperative.journal_sequence,
                state_fingerprint: cooperative.state_fingerprint.clone(),
            }),
        },
        queue_sequence: 0,
        page_count: 1,
        record_count: 0,
        digest: "0".repeat(64),
        selected_original_data: None,
    };
    cut.validate().unwrap();
    #[derive(Clone, serde::Serialize)]
    struct CooperativePayload {
        handoff_id: String,
        retiring_epoch: u64,
        activated_epoch: Option<u64>,
        journal_sequence: u64,
        state_fingerprint: String,
        rebind_prefix_proof: DfsMetadataPrefixProof,
        repair_custody: Option<crate::DfsRepairHandoffCut>,
    }
    let payload = CooperativePayload {
        handoff_id: cooperative.handoff_id.clone(),
        retiring_epoch: cooperative.retiring_epoch,
        activated_epoch: None,
        journal_sequence: cooperative.journal_sequence,
        state_fingerprint: cooperative.state_fingerprint.clone(),
        rebind_prefix_proof: cooperative.rebind_prefix_proof.clone(),
        repair_custody: Some(cut),
    };
    #[derive(serde::Serialize)]
    struct CooperativeEnvelope {
        repair_custody_v1: CooperativePayload,
    }
    let old_bytes = crate::metadata_io::encode_sobj(&CooperativeEnvelope {
        repair_custody_v1: payload.clone(),
    })
    .unwrap();
    assert!(
        sinomo_core::object::serde_ext::from_bytes::<DfsMetadataShardWriteFreeze>(&old_bytes)
            .is_ok()
    );
    #[derive(serde::Serialize)]
    struct Mixed {
        selected_local_recovery_fence_v1: String,
        repair_custody_v1: CooperativePayload,
    }
    for bytes in [
        crate::metadata_io::encode_sobj(&Malformed {
            selected_local_recovery_fence_v1: "not a record".into(),
        })
        .unwrap(),
        crate::metadata_io::encode_sobj(&Mixed {
            selected_local_recovery_fence_v1: "bad tagged value".into(),
            repair_custody_v1: payload,
        })
        .unwrap(),
    ] {
        // Deliberately malformed DATA, not a durable/native acknowledgement.
        std::fs::write(marker(&f), &bytes).unwrap();
        assert!(install(&mut f).is_err());
        assert!(f.backend.membership_handoff_write_freeze().is_err());
        assert!(f.backend.begin_metadata_write().is_err());
        assert!(
            f.backend
                .put_metadata_prefix_ack_floor(&prefix_floor)
                .is_err()
        );
        assert!(
            f.backend
                .put_metadata_authority_ack_floor(&authority_floor)
                .is_err()
        );
        assert_eq!(
            f.backend
                .metadata_prefix_ack_floor(&f.backend.shard_id)
                .unwrap(),
            Some(prefix_floor.clone())
        );
        assert_eq!(
            f.backend
                .metadata_authority_ack_floor(&f.backend.shard_id)
                .unwrap(),
            Some(authority_floor.clone())
        );
        assert_eq!(std::fs::read(marker(&f)).unwrap(), bytes);
    }
}

#[test]
fn selected_recovery_physical_replacement_refuses_identical_copied_marker() {
    fn copy(source: &Path, target: &Path) {
        std::fs::create_dir(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let to = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), to).unwrap();
            }
        }
    }
    struct Restore {
        root: PathBuf,
        moved: PathBuf,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            if self.root.exists() {
                std::fs::remove_dir_all(&self.root).unwrap();
            }
            std::fs::rename(&self.moved, &self.root).unwrap();
        }
    }
    let mut f = fixture("selected-recovery-physical-copy");
    let data = install(&mut f).unwrap();
    let root = f.backend.root.clone();
    let moved = root.with_extension("retained-original");
    std::fs::rename(&root, &moved).unwrap();
    let restore = Restore {
        root: root.clone(),
        moved: moved.clone(),
    };
    copy(&moved, &root);
    assert_eq!(std::fs::read(marker(&f)).unwrap(), data.marker_bytes);
    assert!(install(&mut f).is_err());
    assert!(
        f.backend
            .selected_local_recovery_fence(&f.backend.shard_id)
            .is_err()
    );
    assert_eq!(std::fs::read(marker(&f)).unwrap(), data.marker_bytes);
    drop(restore);
    assert_eq!(install(&mut f).unwrap(), data);
}

#[test]
fn selected_recovery_refuses_valid_snapshot_and_unreadable_replacement_without_erasing_data() {
    fn retained_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn read(
            root: &Path,
            path: &Path,
            files: &mut BTreeMap<PathBuf, Vec<u8>>,
            bytes: &mut usize,
        ) {
            for entry in std::fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                assert!(!kind.is_symlink());
                if kind.is_dir() {
                    read(root, &entry.path(), files, bytes);
                } else {
                    assert!(kind.is_file());
                    assert!(files.len() < 256, "bounded fixture inventory");
                    let value = std::fs::read(entry.path()).unwrap();
                    *bytes += value.len();
                    assert!(*bytes <= 4 * 1024 * 1024, "bounded fixture bytes");
                    files.insert(entry.path().strip_prefix(root).unwrap().into(), value);
                }
            }
        }
        let mut files = BTreeMap::new();
        let mut bytes = 0;
        read(root, root, &mut files, &mut bytes);
        files
    }

    let mut f = fixture("selected-recovery-snapshot-refusal");
    seed_prepare_data(&mut f, 2);
    let shard = f.backend.shard_id.clone();
    let sequence = f.backend.journal_last_sequence();
    let image = f.backend.export_snapshot_image().unwrap();
    image.validate().unwrap();
    // A fixture-local permit supplies the otherwise-valid exact filesystem,
    // shard and source fields to the real storage owner. This is not evidence
    // of native ingress authorization or a committed Raft assignment.
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        f.request.serving_shard.file_system_id.clone(),
        shard.clone(),
        f.request.serving_shard.primary_node_id.clone(),
        f.request.serving_shard.epoch,
    );
    assert!(
        !f.backend
            .replace_with_replicated_snapshot_with_replica_permit(
                &permit,
                shard.clone(),
                sequence,
                image.clone(),
            )
            .unwrap()
    );

    // Independently prove the same image/permit can drive the real unreadable
    // replacement owner. That API has no selected-layout context: its later
    // refusal on the selected root is existing default-reader protection,
    // not an isolated recovery-tag cause or supported selected recovery.
    let ordinary = sinomo_testdir::TestDir::new("recovery-snapshot-ordinary-prerequisite");
    drop(LocalSobjMetadataBackend::new(shard.clone(), ordinary.path()).unwrap());
    std::fs::write(
        metadata_table_snapshot_path(ordinary.path()),
        b"deliberately unreadable fixture snapshot",
    )
    .unwrap();
    let (replaced, archived) =
        LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
            &permit,
            shard.clone(),
            ordinary.path(),
            shard.clone(),
            sequence,
            image.clone(),
        )
        .unwrap();
    assert!(archived);
    assert_eq!(replaced.prefix_proof.as_ref(), Some(&image.prefix_proof));
    assert_eq!(replaced.tables, image.state().unwrap().tables);
    drop(replaced);

    let fence = install(&mut f).unwrap();
    let shard_before = retained_files(&f.backend.root);
    let custody_root = f.layout.queue_layout().root();
    let custody_before = retained_files(custody_root);
    let error = f
        .backend
        .replace_with_replicated_snapshot_with_replica_permit(
            &permit,
            shard.clone(),
            sequence,
            image.clone(),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("selected local recovery fence is not a cooperative handoff")
    );
    assert_eq!(retained_files(&f.backend.root), shard_before);
    assert_eq!(retained_files(custody_root), custody_before);
    let error = match LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
        &permit,
        shard.clone(),
        f.backend.root.clone(),
        shard,
        sequence,
        image.clone(),
    ) {
        Ok(_) => panic!("default unreadable replacement must refuse selected physical layout"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("default reader rejects selected original physical layout")
    );
    assert_eq!(std::fs::read(marker(&f)).unwrap(), fence.marker_bytes);
    assert_eq!(retained_files(&f.backend.root), shard_before);
    assert_eq!(retained_files(custody_root), custody_before);
}
