//! Bounded layout publication races and saved-fence recovery.
use super::*;

const NAMESPACE: &str = "org.example.layout-race";

struct Fixture {
    _root: sinomo_testdir::TestDir,
    data: SinomoDataDir,
    fs: DfsFileSystem,
    local: NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    object: crate::DfsObjectMetadata,
    version: DfsObjectVersionRecord,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = sinomo_testdir::TestDir::new(label);
        let data = SinomoDataDir::new(root.join("data"));
        data.ensure_root_layout().unwrap();
        let local = NodeId::from_legacy_named(label).unwrap();
        let fs = distributed_sifs(local.clone());
        let shards = ["root", "users", "services"]
            .map(|name| DfsMetadataShardConfig {
                file_system_id: fs.file_system_id.clone(),
                shard_id: name.into(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: if name == "root" {
                    crate::DfsMetadataShardAssignment::Root
                } else {
                    crate::DfsMetadataShardAssignment::PathPrefix {
                        prefix: format!("/{name}"),
                    }
                },
                namespace_prepare_codec: crate::DfsMetadataNamespacePrepareCodec::PackedV6,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            })
            .to_vec();
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone()).remove(0);
        object.object_id =
            crate::DfsObjectId::from_legacy_named(format!("{label}-object")).unwrap();
        object.path = "/services/layout-race-source".into();
        object.kind = DfsObjectKind::File;
        object.current_locations = vec![local.clone()];
        object.content_hash = Some(format!("blake3:{}", blake3::hash(b"source bytes").to_hex()));
        object.content_size_bytes = Some(12);
        let mut version = crate::object_version_record(&object, object.content_size_bytes);
        version.retention.insert(
            NAMESPACE.into(),
            crate::DfsRevisionRetentionState {
                order: 1,
                grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                    object: Box::new(object.clone()),
                    granted_at_unix_nanos: dfs_now_unix_nanos(),
                    origin: crate::DfsRetentionGrantOrigin::ExplicitOperation,
                    minimum: None,
                })),
            },
        );
        let mut source =
            open_local_metadata_shard_backend(&data, DfsMetadataShardId::new("root")).unwrap();
        source.set_namespace_prepare_codec(crate::DfsMetadataNamespacePrepareCodec::PackedV6);
        source.put_file_system(&fs).unwrap();
        source.put_object_and_version(&object, &version).unwrap();
        drop(source);
        Self {
            _root: root,
            data,
            fs,
            local,
            shards,
            object,
            version,
        }
    }

    fn open(&self) -> LocalShardedMetadataBackend {
        LocalShardedMetadataBackend::new(self.metadata_root(), self.shards.clone()).unwrap()
    }

    fn metadata_root(&self) -> PathBuf {
        crate::default_dfs_metadata_shards_root(&self.data)
    }

    fn plan(&self, backend: &LocalShardedMetadataBackend) -> DfsDefaultSifsPendingMigration {
        let writable = self
            .shards
            .iter()
            .map(|shard| DfsMetadataShardId::new(&shard.shard_id))
            .collect();
        let mut report = DfsMetadataShardMigrationReport {
            file_system_id: self.fs.file_system_id.clone(),
            scanned_objects: 0,
            indexed_objects: 0,
            migrated_objects: 0,
            skipped_stale_sources: 0,
            reached_limit: false,
        };
        plan_default_sifs_pending_migration(
            &self.metadata_root(),
            backend,
            vec![(DfsMetadataShardId::new("root"), self.object.clone())],
            &self.shards,
            &writable,
            &BTreeSet::new(),
            &self.local,
            &mut report,
            &mut false,
        )
        .unwrap()
        .unwrap()
    }

    fn advance(
        &self,
        backend: &mut LocalShardedMetadataBackend,
        pending: &mut DfsDefaultSifsPendingMigration,
    ) -> DfsDefaultSifsPendingProgress {
        let mut budget = DfsDefaultSifsMigrationPassBudget::new(1);
        let mut report = DfsMetadataShardMigrationReport {
            file_system_id: self.fs.file_system_id.clone(),
            scanned_objects: 0,
            indexed_objects: 0,
            migrated_objects: 0,
            skipped_stale_sources: 0,
            reached_limit: false,
        };
        let progress = advance_default_sifs_pending_migration(
            &self.metadata_root(),
            backend,
            &self.fs,
            pending,
            &mut budget,
            &mut report,
        )
        .unwrap();
        assert!(budget.physical_mutations <= 1);
        progress
    }
}

fn reference_prepare(
    source: &LocalSobjMetadataBackend,
    object: &crate::DfsObjectMetadata,
    version: &DfsObjectVersionRecord,
    add: Option<crate::DfsContentReferenceRecord>,
    remove: Option<crate::DfsContentReferenceRecord>,
) -> crate::DfsNamespacePrepareRecord {
    let operation_id = sinomo_core::Id::generate().to_string();
    let identity = crate::DfsRevisionReference {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: version.version.clone(),
    };
    let command = crate::DfsContentReferenceRequest {
        wire_format: crate::DfsContentReferenceFormat::NamespacedV6,
        namespace: NAMESPACE.into(),
        path: object.path.clone(),
        identity: identity.clone(),
        operation: if add.is_some() {
            crate::DfsContentReferenceOperation::Retain {
                reference_id: "leaf".into(),
            }
        } else {
            crate::DfsContentReferenceOperation::Release {
                reference_id: "leaf".into(),
            }
        },
        operation_id: operation_id.clone(),
    };
    let transaction = crate::DfsNamespaceTransaction {
        transaction_id: format!("file-history:{operation_id}"),
        file_system_id: object.file_system_id.clone(),
        objects: Vec::new(),
        versions: vec![version.clone()],
        file_version_pins: Some(crate::DfsFileVersionPinMutation {
            add: add.into_iter().collect(),
            remove: remove.into_iter().collect(),
        }),
        advisory_lock_states: Vec::new(),
        extension_expectations: Vec::new(),
        contribution_digest: Some(
            blake3::hash(&serde_json::to_vec(&command).unwrap())
                .to_hex()
                .to_string(),
        ),
        content_reference_identity: Some(identity),
        created_at: OffsetDateTime::now_utc(),
    };
    let shard = source.shard_id().clone();
    let (expected_object_versions, expected_path_occupants, _) = source
        .namespace_prepare_expectations(&shard, &transaction)
        .unwrap();
    let mut prepare = crate::DfsNamespacePrepareRecord {
        operation_id: format!("namespace:file-history:{operation_id}"),
        file_system_id: object.file_system_id.clone(),
        shard_id: shard,
        owner_epoch: 1,
        operation_digest: String::new(),
        expected_object_versions,
        expected_path_occupants,
        content_receipts: Vec::new(),
        transaction,
        completed_request: None,
        completed_request_completion: None,
        status: crate::DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    prepare
}

fn commit_reference(
    source: &mut LocalSobjMetadataBackend,
    prepare: &crate::DfsNamespacePrepareRecord,
) {
    source.prepare_namespace_transaction(prepare).unwrap();
    source
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
}

#[test]
fn layout_first_reference_after_pin_scan_catches_up_before_publication_without_reanimation() {
    let fixture = Fixture::new("layout-late-first-reference");
    let mut backend = fixture.open();
    let mut pending = fixture.plan(&backend);
    assert!(
        pending.source_fence_required,
        "an empty outcome scan must not exempt a writable P6 source"
    );
    let target = DfsMetadataShardId::new("services");
    backend
        .shard_backend_mut(&target)
        .unwrap()
        .put_file_system(&fixture.fs)
        .unwrap();
    backend
        .shard_backend_mut(&target)
        .unwrap()
        .put_version(&fixture.version)
        .unwrap();
    // The version, pin and outcome scans all observed an empty reference set.
    // Pause precisely before acquiring the source publication fence.
    pending.stage = DfsDefaultSifsPendingMigrationStage::SourceFence;
    let pin = crate::DfsContentReferenceRecord {
        namespace: Some(NAMESPACE.into()),
        file_system_id: fixture.fs.file_system_id.clone(),
        object_id: fixture.object.object_id.clone(),
        version: fixture.version.version.clone(),
        pin_id: crate::content_reference_storage_id(NAMESPACE, "leaf"),
        created_at: OffsetDateTime::now_utc(),
    };
    let source_id = DfsMetadataShardId::new("root");
    let original = reference_prepare(
        backend.shard_backend(&source_id).unwrap(),
        &fixture.object,
        &fixture.version,
        Some(pin.clone()),
        None,
    );
    commit_reference(backend.shard_backend_mut(&source_id).unwrap(), &original);
    assert!(matches!(
        fixture.advance(&mut backend, &mut pending),
        DfsDefaultSifsPendingProgress::Incomplete
    ));
    assert!(pending.source_fence.is_some());
    assert_eq!(
        backend
            .shard_backend(&source_id)
            .unwrap()
            .get_namespace_prepare(&pending.source_fence.as_ref().unwrap().operation_id)
            .unwrap()
            .as_ref(),
        pending.source_fence.as_deref(),
        "the saved fence and durable prepare must share the canonical path read set"
    );
    assert!(
        backend
            .shard_backend(&target)
            .unwrap()
            .get_object(&fixture.object.object_id)
            .unwrap()
            .is_none()
    );
    let blocked = reference_prepare(
        backend.shard_backend(&source_id).unwrap(),
        &fixture.object,
        &fixture.version,
        None,
        Some(pin),
    );
    assert!(
        backend
            .shard_backend_mut(&source_id)
            .unwrap()
            .prepare_namespace_transaction(&blocked)
            .unwrap_err()
            .to_string()
            .contains("overlaps")
    );
    let mut published = false;
    for _ in 0..128 {
        fixture.advance(&mut backend, &mut pending);
        let target_backend = backend.shard_backend(&target).unwrap();
        if target_backend
            .get_object(&fixture.object.object_id)
            .unwrap()
            .is_some()
        {
            assert_eq!(
                target_backend
                    .version_pins_for_object(&fixture.object.object_id)
                    .unwrap()
                    .len(),
                1
            );
            let receipt = target_backend
                .get_namespace_transaction_receipt(&original.transaction.transaction_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                receipt.contribution_digest,
                original.transaction.contribution_digest
            );
            published = true;
            break;
        }
    }
    assert!(published);
    let target_backend = backend.shard_backend(&target).unwrap();
    let pin = target_backend
        .version_pins_for_object(&fixture.object.object_id)
        .unwrap()
        .pop()
        .unwrap();
    let release = reference_prepare(
        target_backend,
        &fixture.object,
        &fixture.version,
        None,
        Some(pin),
    );
    commit_reference(backend.shard_backend_mut(&target).unwrap(), &release);
    let mut complete = false;
    for _ in 0..128 {
        let progress = fixture.advance(&mut backend, &mut pending);
        assert!(
            backend
                .shard_backend(&target)
                .unwrap()
                .version_pins_for_object(&fixture.object.object_id)
                .unwrap()
                .is_empty(),
            "old source Retain must not resurrect the target's released reference"
        );
        if matches!(progress, DfsDefaultSifsPendingProgress::Complete) {
            complete = true;
            break;
        }
    }
    assert!(complete);
}

#[test]
fn layout_missing_observed_committed_prepare_preserves_the_saved_source_prefix() {
    let fixture = Fixture::new("layout-compacted-late-prepare-prefix");
    let mut backend = fixture.open();
    let mut pending = fixture.plan(&backend);
    let source = DfsMetadataShardId::new("root");
    let target = DfsMetadataShardId::new("services");
    backend
        .shard_backend_mut(&target)
        .unwrap()
        .put_file_system(&fixture.fs)
        .unwrap();
    backend
        .shard_backend_mut(&target)
        .unwrap()
        .put_version(&fixture.version)
        .unwrap();
    pending.stage = DfsDefaultSifsPendingMigrationStage::SourceFence;
    let pin = crate::DfsContentReferenceRecord {
        namespace: Some(NAMESPACE.into()),
        file_system_id: fixture.fs.file_system_id.clone(),
        object_id: fixture.object.object_id.clone(),
        version: fixture.version.version.clone(),
        pin_id: crate::content_reference_storage_id(NAMESPACE, "leaf"),
        created_at: OffsetDateTime::now_utc(),
    };
    let original = reference_prepare(
        backend.shard_backend(&source).unwrap(),
        &fixture.object,
        &fixture.version,
        Some(pin),
        None,
    );
    backend
        .shard_backend_mut(&source)
        .unwrap()
        .prepare_namespace_transaction(&original)
        .unwrap();
    let committed = backend
        .shard_backend_mut(&source)
        .unwrap()
        .commit_prepared_namespace_transaction(
            &original.operation_id,
            &original.operation_digest,
            original.owner_epoch,
        )
        .unwrap();
    fixture.advance(&mut backend, &mut pending);
    assert!(pending.source_fence.is_some());
    let now = dfs_now_unix_nanos().saturating_add(1_000_000_000);
    for _ in 0..4 {
        backend
            .shard_backend_mut(&source)
            .unwrap()
            .gc_metadata_history_authoritative(
                crate::DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_now_unix_nanos(now)
                    .with_max_removals(32),
            )
            .unwrap();
        if backend
            .shard_backend(&source)
            .unwrap()
            .get_namespace_prepare(&original.operation_id)
            .unwrap()
            .is_none()
        {
            break;
        }
    }
    assert!(
        backend
            .shard_backend(&source)
            .unwrap()
            .get_namespace_prepare(&original.operation_id)
            .unwrap()
            .is_none(),
        "the source retains its terminal outcome without a full prepared table body"
    );
    // The first real suffix entry supplies the original Prepared input. The
    // driver must retain that exact input until the later Commit is projected.
    for _ in 0..32 {
        if pending.source_replayed_through_sequence >= committed.sequence - 1 {
            break;
        }
        fixture.advance(&mut backend, &mut pending);
    }
    assert_eq!(
        pending.source_replayed_through_sequence,
        committed.sequence - 1
    );
    assert_eq!(
        pending.source_prepare.as_deref(),
        Some(&crate::metadata_records::normalized_namespace_prepare(
            &original
        ))
    );
    // Simulate a restored cursor without its required optional input, while
    // retaining the real source journal and compact terminal table outcome.
    // Absence of the full table row alone does not imply lost journal evidence.
    pending.source_prepare = None;
    let before = pending.source_replayed_through_sequence;
    let source_before = pending
        .writable_sources
        .iter()
        .find(|row| row.shard_id == source)
        .unwrap()
        .replayed_through_sequence;
    let mut budget = DfsDefaultSifsMigrationPassBudget::new(1);
    let mut report = DfsMetadataShardMigrationReport {
        file_system_id: fixture.fs.file_system_id.clone(),
        scanned_objects: 0,
        indexed_objects: 0,
        migrated_objects: 0,
        skipped_stale_sources: 0,
        reached_limit: false,
    };
    let error = match advance_default_sifs_pending_migration(
        &fixture.metadata_root(),
        &mut backend,
        &fixture.fs,
        &mut pending,
        &mut budget,
        &mut report,
    ) {
        Err(error) => error,
        Ok(_) => panic!("a missing committed body must reject prefix advancement"),
    };
    assert!(
        error.to_string().contains("requires source prepare"),
        "{error}"
    );
    assert_eq!(pending.source_replayed_through_sequence, before);
    assert_eq!(
        pending
            .writable_sources
            .iter()
            .find(|row| row.shard_id == source)
            .unwrap()
            .replayed_through_sequence,
        source_before
    );
    assert!(
        backend
            .shard_backend(&target)
            .unwrap()
            .get_object(&fixture.object.object_id)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .shard_backend(&target)
            .unwrap()
            .get_namespace_transaction_receipt(&original.transaction.transaction_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn layout_reopen_cursor_reset_retires_evicted_fence_after_target_head_advanced() {
    let fixture = Fixture::new("layout-fence-reopen-new-target-head");
    let mut backend = fixture.open();
    let mut pending = fixture.plan(&backend);
    let source = DfsMetadataShardId::new("root");
    let target = DfsMetadataShardId::new("services");
    let mut evicted_before_abort = false;
    for _ in 0..256 {
        fixture.advance(&mut backend, &mut pending);
        let old = backend.shard_backend(&source).unwrap();
        if old
            .object_or_compacted_tombstone(&fixture.object.object_id)
            .unwrap()
            .is_some_and(|object| {
                matches!(
                    object.tombstone_provenance,
                    Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
                )
            })
            && old
                .default_layout_migration_fence_for_object(
                    &fixture.fs.file_system_id,
                    &fixture.object.object_id,
                )
                .unwrap()
                .is_some()
        {
            evicted_before_abort = true;
            break;
        }
    }
    assert!(
        evicted_before_abort,
        "budget1 must expose the durable eviction/abort crash boundary"
    );
    let saved = pending.source_fence.as_ref().unwrap().as_ref().clone();
    let mut edited = fixture.object.clone();
    edited.version = DfsVersion::generate();
    edited.changed_at_unix_nanos += 1;
    edited.mode ^= 0o100;
    let mut edited_version = crate::object_version_record(&edited, edited.content_size_bytes);
    edited_version.retention.insert(
        NAMESPACE.into(),
        crate::DfsRevisionRetentionState {
            order: 1,
            grant: None,
        },
    );
    backend
        .shard_backend_mut(&target)
        .unwrap()
        .put_object_and_version(&edited, &edited_version)
        .unwrap();
    // Lose the private pending cursor while retaining real durable source and
    // target tables; the target's new head must not hide the old reservation.
    let active = fixture
        .shards
        .iter()
        .map(|shard| DfsMetadataShardId::new(&shard.shard_id))
        .collect();
    let cursor = DfsDefaultSifsMigrationCursor {
        format_version: DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_CURSOR_FORMAT_VERSION - 1,
        generation: default_sifs_migration_generation(
            &fixture.fs.file_system_id,
            &fixture.local,
            &fixture.shards,
            &active,
            &BTreeSet::new(),
        ),
        catalog_complete: true,
        after_object_id: Some(fixture.object.object_id.clone()),
        observed_journal_sequences: BTreeMap::new(),
        dirty_object_ids: BTreeSet::new(),
        pending_journal_replay: None,
        pending_migration: None,
    };
    write_default_sifs_migration_cursor(&fixture.metadata_root(), &cursor).unwrap();
    drop(backend);
    let mut complete = false;
    for _ in 0..256 {
        let mut reopened = fixture.open();
        let report = migrate_default_sifs_metadata_on_active_primary_shards(
            &fixture.metadata_root(),
            &mut reopened,
            &fixture.fs,
            &fixture.shards,
            &active,
            &fixture.local,
            1,
        )
        .unwrap();
        assert_eq!(
            reopened
                .shard_backend(&target)
                .unwrap()
                .get_object(&fixture.object.object_id)
                .unwrap(),
            Some(edited.clone())
        );
        if !report.migration.reached_limit {
            complete = true;
            break;
        }
    }
    assert!(complete);
    let reopened = fixture.open();
    let source_backend = reopened.shard_backend(&source).unwrap();
    assert!(
        source_backend
            .default_layout_migration_fence_for_object(
                &fixture.fs.file_system_id,
                &fixture.object.object_id
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        source_backend
            .get_namespace_prepare(&saved.operation_id)
            .unwrap()
            .unwrap()
            .status,
        crate::DfsNamespacePrepareStatus::Aborted
    );
}
