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

#[test]
fn selected_layout_router_inspection_preserves_files_and_rejects_another_owner() {
    fn files(root: &Path) -> BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)> {
        fn visit(
            root: &Path,
            dir: &Path,
            result: &mut BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)>,
        ) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                let metadata = std::fs::symlink_metadata(&path).unwrap();
                assert!(!metadata.file_type().is_symlink());
                if metadata.is_dir() {
                    visit(root, &path, result);
                } else {
                    assert!(metadata.is_file());
                    result.insert(
                        path.strip_prefix(root).unwrap().into(),
                        (std::fs::read(&path).unwrap(), metadata.modified().unwrap()),
                    );
                }
            }
        }
        let mut result = BTreeMap::new();
        visit(root, root, &mut result);
        result
    }

    // This is a local file/owner boundary control, not a serving-authority grant.
    let f = fixture("selected-router-inspection");
    let other = fixture("selected-router-inspection-other-owner");
    let root = f.backend.root.parent().unwrap().to_path_buf();
    let before = files(&root);
    let config = f.request.serving_shard.clone();
    assert!(
        crate::LocalShardedMetadataBackend::open_for_inspection_at_node(
            &root,
            vec![config.clone()],
            f.context.local_node_id(),
        )
        .is_err(),
        "default inspection cannot bypass selected-layout ownership"
    );
    assert!(
        crate::LocalShardedMetadataBackend::open_for_inspection_at_node_with_layout(
            root.clone(),
            vec![config.clone()],
            f.context.local_node_id(),
            Arc::clone(&other.layout),
        )
        .is_err(),
        "another owner's layout cannot inspect this root"
    );
    let inspection = crate::LocalShardedMetadataBackend::open_for_inspection_at_node_with_layout(
        root.clone(),
        vec![config],
        f.context.local_node_id(),
        Arc::clone(&f.layout),
    )
    .unwrap();
    assert_eq!(
        inspection
            .metadata_prefix_proof(&f.backend.shard_id)
            .unwrap(),
        Some(f.backend.metadata_prefix_proof().unwrap()),
    );
    drop(inspection);
    assert_eq!(
        files(&root),
        before,
        "inspection must not initialize, repair or persist hints"
    );
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
fn selected_recovery_writer_hooks_bind_fresh_scope_and_original_deadline() {
    use crate::metadata_api::selected_local_recovery::{
        SelectedRecoveryWriterCut as Cut, SelectedRecoveryWriterHook as Hook,
        SelectedRecoveryWriterObservation,
    };
    use std::sync::Mutex;
    use std::time::Instant;
    let fresh_deadline = || Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut f = fixture("selected-recovery-writer-hook-scope");
    let root = f.backend.root.clone();
    let request_at = |floor| {
        DfsMetadataShardRecoveryBallotRequest::prepare(
            &f.request.serving_shard,
            &f.request.ballot.candidate_node_id,
            floor,
        )
        .unwrap()
    };
    let requests: Vec<_> = (0..=5).map(request_at).collect();
    fn unexpected(_: &SelectedRecoveryWriterObservation) -> Result<()> {
        panic!("unrelated or duplicate publication must not observe a cut");
    }
    let wrong_root = Hook::install(
        root.with_extension("another-physical-root"),
        requests[0].clone(),
        Cut::BeforeMarkerPublication,
        fresh_deadline(),
        unexpected,
    )
    .unwrap();
    let first = install(&mut f).unwrap();
    assert_eq!(wrong_root.hits(), 0);
    drop(wrong_root);

    let duplicate = Hook::install(
        root.clone(),
        requests[0].clone(),
        Cut::AfterMarkerNamespaceSync,
        fresh_deadline(),
        unexpected,
    )
    .unwrap();
    assert_eq!(install(&mut f).unwrap(), first);
    assert_eq!(duplicate.hits(), 0);
    assert!(
        Hook::install(
            root.clone(),
            requests[0].clone(),
            Cut::BeforeMarkerPublication,
            fresh_deadline(),
            unexpected,
        )
        .is_err(),
        "occupied registration cannot replace its owner"
    );
    drop(duplicate);

    let wrong_request = Hook::install(
        root.clone(),
        requests[2].clone(),
        Cut::BeforeMarkerPublication,
        fresh_deadline(),
        unexpected,
    )
    .unwrap();
    f.request = requests[1].clone();
    install(&mut f).unwrap();
    assert_eq!(wrong_request.hits(), 0);
    drop(wrong_request);

    for (request, cut) in [
        (requests[2].clone(), Cut::BeforeMarkerPublication),
        (requests[3].clone(), Cut::AfterMarkerNamespaceSync),
    ] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let hook = Hook::install(
            root.clone(),
            request.clone(),
            cut,
            fresh_deadline(),
            move |data| {
                captured.lock().unwrap().push(data.clone());
                Ok(())
            },
        )
        .unwrap();
        // No wait thread is needed for this scope control; a release never
        // prevents observation or changes the fixed deadline check.
        hook.release();
        f.request = request;
        let installed = install(&mut f).unwrap();
        assert_eq!(hook.hits(), 1);
        let actual = observed.lock().unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].cut, cut);
        assert_eq!(actual[0].marker_path, marker(&f));
        assert_eq!(actual[0].fence, installed.fence);
        assert_eq!(
            actual[0].stored_marker_bytes,
            match cut {
                Cut::BeforeMarkerPublication => None,
                Cut::AfterMarkerNamespaceSync => Some(installed.marker_bytes.clone()),
            }
        );
        drop(actual);
        assert_eq!(install(&mut f).unwrap(), installed);
        assert_eq!(
            hook.hits(),
            1,
            "duplicate recertification is not a fresh cut"
        );
        drop(hook);
    }

    f.request = requests[4].clone();
    let before = std::fs::read(marker(&f)).unwrap();
    let expired = Hook::install(
        root.clone(),
        f.request.clone(),
        Cut::BeforeMarkerPublication,
        Instant::now(),
        unexpected,
    )
    .unwrap();
    expired.release();
    assert!(
        install(&mut f)
            .unwrap_err()
            .to_string()
            .contains("original deadline expired")
    );
    assert_eq!(expired.hits(), 0);
    assert_eq!(std::fs::read(marker(&f)).unwrap(), before);
    drop(expired);
    assert!(
        Hook::install(
            root.clone(),
            f.request.clone(),
            Cut::BeforeMarkerPublication,
            fresh_deadline() + std::time::Duration::from_secs(60),
            unexpected,
        )
        .is_err(),
        "the hook cannot enlarge the existing call budget"
    );
    assert_eq!(
        install(&mut f).unwrap().fence.reservation,
        f.request,
        "dropping the negative scope does not leave a hidden admission flag"
    );

    // Exercise the real bounded pause after synchronization. Its expiration
    // withholds the return while retaining the actual historical marker DATA.
    f.request = requests[5].clone();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&observed);
    let post_sync = Hook::install(
        root,
        f.request.clone(),
        Cut::AfterMarkerNamespaceSync,
        fresh_deadline(),
        move |data| {
            captured.lock().unwrap().push(data.clone());
            Ok(())
        },
    )
    .unwrap();
    assert!(
        install(&mut f)
            .unwrap_err()
            .to_string()
            .contains("original deadline expired")
    );
    assert_eq!(post_sync.hits(), 1);
    let historical = f
        .backend
        .selected_local_recovery_fence(&f.backend.shard_id)
        .unwrap()
        .unwrap();
    let actual = observed.lock().unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].fence, historical.fence);
    assert_eq!(
        actual[0].stored_marker_bytes.as_deref(),
        Some(historical.marker_bytes.as_slice())
    );
    assert_eq!(std::fs::read(marker(&f)).unwrap(), historical.marker_bytes);
    drop(actual);
    drop(post_sync);
    assert_eq!(install(&mut f).unwrap(), historical);
}
