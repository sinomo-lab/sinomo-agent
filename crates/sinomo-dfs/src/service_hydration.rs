//! Metadata backend hydration and journal replay for the networked DFS service.
//!
//! This module keeps durable metadata replay, missing-payload journal recovery,
//! and in-memory service-state restoration out of the live data-plane protocol
//! code.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Instant;

use sinomo_core::DfsObjectPolicy;
use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;
use time::OffsetDateTime;

use crate::transfer::PendingNetworkedWrite;
use crate::*;

type HydrationReceiptsByVersion = BTreeMap<(DfsObjectId, DfsVersion), Vec<DfsReplicaReceiptRecord>>;

#[cfg(test)]
thread_local! {
    static FULL_METADATA_HYDRATION_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_full_metadata_hydration_calls() {
    FULL_METADATA_HYDRATION_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn full_metadata_hydration_calls() -> usize {
    FULL_METADATA_HYDRATION_CALLS.get()
}

fn hydration_never_cancelled() -> bool {
    false
}

pub(crate) fn run_metadata_hydration_runtime_probe(
    root: &Path,
    options: crate::DfsMetadataHydrationRuntimeProbeOptions,
) -> Result<crate::DfsMetadataHydrationRuntimeProbeReport, DfsError> {
    if options.object_count == 0 {
        return Err(DfsError::Metadata {
            message: "metadata hydration runtime probe object count must be positive".to_string(),
        });
    }
    if root.exists()
        && fs::read_dir(root)
            .map_err(|source| DfsError::Metadata {
                message: format!("read hydration probe root {}: {source}", root.display()),
            })?
            .next()
            .is_some()
    {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata hydration runtime probe requires an empty root: {}",
                root.display()
            ),
        });
    }

    let total_started = Instant::now();
    let node_id =
        NodeId::from_legacy_named("node_metadata_hydration_runtime_probe").map_err(|source| {
            DfsError::Metadata {
                message: format!("construct hydration probe node id: {source}"),
            }
        })?;
    let file_system_id =
        DfsFileSystemId::from_legacy_named("sifs").map_err(|source| DfsError::Metadata {
            message: format!("construct hydration probe filesystem id: {source}"),
        })?;
    let network = sinomo_core::network::InProcessNetwork::new();
    let transport = network.add_node(node_id.clone());
    let content = LocalContentStore::new(root.join("content"))?;
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))?;
    let mut dfs = NetworkedDfsService::new(transport, content).with_metadata_backend(backend)?;
    dfs.add_file_system(DfsFileSystem {
        file_history: Default::default(),
        file_system_id: file_system_id.clone(),
        display_name: "metadata hydration runtime probe".to_string(),
        owner_node_id: node_id.clone(),
        subtree_policies: Vec::new(),
    })?;

    let write_started = Instant::now();
    let mut tail_object_id = None;
    let mut objects = Vec::with_capacity(options.object_count.saturating_add(1));
    for index in 0..options.object_count {
        let object_id =
            DfsObjectId::from_legacy_named(format!("obj_hydration_runtime_probe_{index:06}"))
                .map_err(|source| DfsError::Metadata {
                    message: format!("construct hydration probe object id: {source}"),
                })?;
        let version =
            DfsVersion::from_legacy_named(format!("version_hydration_runtime_probe_{index:06}"))
                .map_err(|source| DfsError::Metadata {
                    message: format!("construct hydration probe version: {source}"),
                })?;
        if index + 1 == options.object_count {
            tail_object_id = Some(object_id.clone());
        }
        objects.push(DfsObjectMetadata {
            object_id,
            file_system_id: file_system_id.clone(),
            path: format!("/runtime-probes/hydration/page-{index:06}.txt"),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node_id.clone(),
            current_locations: vec![node_id.clone()],
            content_hash: None,
            content_size_bytes: Some(0),
            link_target_object_id: None,
            mode: 0o600,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version,
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        });
    }
    let tombstone_id = DfsObjectId::from_legacy_named("obj_hydration_runtime_probe_tombstone")
        .map_err(|source| DfsError::Metadata {
            message: format!("construct hydration probe tombstone id: {source}"),
        })?;
    let tombstone_path = "/runtime-probes/hydration/deleted.txt".to_string();
    objects.push(DfsObjectMetadata {
        object_id: tombstone_id,
        file_system_id: file_system_id.clone(),
        path: tombstone_path.clone(),
        kind: DfsObjectKind::Tombstone,
        tombstone_provenance: None,
        owner_node_id: node_id.clone(),
        current_locations: vec![node_id],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named("version_hydration_runtime_probe_tombstone")
            .map_err(|source| DfsError::Metadata {
                message: format!("construct hydration probe tombstone version: {source}"),
            })?,
        policy: DfsObjectPolicy::LocalOwned,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    });
    dfs.add_object_metadata_batch(objects)?;
    let write_elapsed = write_started.elapsed();

    dfs.reset_reconstructable_metadata_projection();
    let hydration_started = Instant::now();
    dfs.hydrate_from_metadata_backend()?;
    let hydration_elapsed = hydration_started.elapsed();

    let tail_object_id = tail_object_id.ok_or_else(|| DfsError::Metadata {
        message: "metadata hydration runtime probe did not create a tail object".to_string(),
    })?;
    let expected_tail_path = format!(
        "/runtime-probes/hydration/page-{:06}.txt",
        options.object_count - 1
    );
    let tail_object_restored = dfs
        .current_read_object_metadata(&tail_object_id)
        .is_ok_and(|object| object.path == expected_tail_path);
    let tombstone_restored = dfs
        .tombstones_by_metadata_path
        .contains_key(&(file_system_id, metadata_path(&tombstone_path)));
    if !tail_object_restored || !tombstone_restored {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata hydration runtime probe lost durable state: tail_restored={tail_object_restored} tombstone_restored={tombstone_restored}"
            ),
        });
    }

    Ok(crate::DfsMetadataHydrationRuntimeProbeReport {
        object_count: options.object_count as u64,
        page_size: DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE as u64,
        crossed_page_boundary: options.object_count > DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
        write_elapsed_ms: write_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        hydration_elapsed_ms: hydration_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        total_elapsed_ms: total_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
        tail_object_restored,
        tombstone_restored,
    })
}

/// Whether one hydrated committed intent can still own repair work for the
/// authoritative visible object.
///
/// A later metadata-only version may retain the same immutable content, and a
/// committed target may still need publication while the visible object is at
/// its base. A later authoritative tombstone is also positive proof that the
/// old content target is terminal. Missing hashes on a live object or a
/// mismatched durable identity are ambiguous and remain fail-closed. Only this
/// positive supersession evidence may discard reconstructed repair work.
fn hydrated_committed_intent_repair_may_be_actionable(
    intent: &DfsWriteIntentRecord,
    visible_object: Option<&DfsObjectMetadata>,
) -> bool {
    let Some(visible_object) = visible_object else {
        return true;
    };
    if visible_object.file_system_id != intent.file_system_id
        || visible_object.object_id != intent.object_id
    {
        return true;
    }
    if visible_object.version == intent.target_version {
        return true;
    }
    if intent
        .base_version
        .as_ref()
        .is_some_and(|base_version| visible_object.version == *base_version)
    {
        return true;
    }
    if visible_object.kind == DfsObjectKind::Tombstone {
        return false;
    }
    let (Some(visible_content_hash), Some(intent_content_hash)) = (
        visible_object.content_hash.as_deref(),
        intent.content_hash.as_deref(),
    ) else {
        return true;
    };
    if visible_content_hash == intent_content_hash {
        return true;
    }
    false
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Intent history owns repair/outcome records, not the current CAS head.
    /// Restore that head from the durable visible view after replaying a
    /// historical committed intent, regardless of opaque version ordering.
    pub(crate) fn restore_metadata_successor_head(
        &mut self,
        record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !record.publication.is_current() || record.status != DfsWriteIntentStatus::Committed {
            return Ok(());
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let object = backend.get_object(&record.object_id)?;
        let Some(object) = object.filter(|object| {
            object.file_system_id == record.file_system_id
                && object.version != record.target_version
        }) else {
            return Ok(());
        };
        self.coordinator.set_committed_version(
            object.file_system_id,
            object.object_id,
            object.version,
        );
        Ok(())
    }

    /// Drop the reconstructable metadata mirror while retaining the service's
    /// transport, durable backend capability, and operation-independent state.
    ///
    /// Both complete hydration and exact-replica snapshot replacement start
    /// from this boundary. Keeping the reset in one place prevents a new
    /// durable projection from leaving stale objects, tombstones, intents, or
    /// pending transfers from the generation it replaced.
    fn reset_reconstructable_metadata_projection(&mut self) {
        self.metadata = DfsMetadataStore::new();
        self.tombstones_by_metadata_path.clear();
        self.transient_metadata_object_ids.clear();
        self.transient_metadata_predecessor_object_ids.clear();
        self.coordinator = DistributedEventualCoordinator::new();
        self.writes.pending_writes.clear();
        self.reads.pending_reads.clear();
        self.clear_metadata_lookup_cache();
    }

    fn restore_durable_intent_counter_floor(&mut self) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let intent_counter_floor = backend.write_intent_counter_floor()?;
        self.coordinator
            .observe_restored_intent_counter_floor(intent_counter_floor);
        Ok(())
    }

    /// Refresh an exact replica lane after atomically replacing its shard.
    ///
    /// Replica tables are the authoritative local copy. Materializing every
    /// object, version, intent, and receipt into this long-lived request shell
    /// would retain the namespace twice and is unnecessary until promotion,
    /// whose handoff path performs a complete authoritative hydration.
    pub(crate) fn hydrate_lazy_exact_replica_after_snapshot_install(
        &mut self,
    ) -> Result<(), DfsError> {
        let exact_shard_id = self
            .metadata_backend
            .as_ref()
            .and_then(|backend| backend.resident_exact_shard_id())
            .cloned()
            .ok_or_else(|| DfsError::Metadata {
                message: "lazy replica snapshot hydration requires an exact resident shard"
                    .to_string(),
            })?;
        self.reset_reconstructable_metadata_projection();
        // A replacement snapshot is an authoritative cursor reset. Do not let
        // the previous generation's in-memory high-water mark win via `max`.
        self.replication
            .local_metadata_journal_cursors
            .remove(&exact_shard_id);
        self.hydrate_file_systems_from_metadata_backend()
    }

    /// Discard one completed exact namespace request's rebuildable object
    /// projection while retaining its shard runtime identity and cursors.
    ///
    /// Single-shard namespace operations are terminal and replayable through
    /// the durable request ledger before the daemon calls this method. Their
    /// visible objects and coordinator versions are therefore caches of the
    /// resident shard tables, not cross-request ownership. Keeping only the
    /// filesystem catalog, journal cursor, and shared job guards prevents both
    /// accumulated duplicate state and stale reads after later global work.
    pub fn collapse_exact_namespace_request_projection(&mut self) -> Result<(), DfsError> {
        if self
            .metadata_backend
            .as_ref()
            .and_then(|backend| backend.resident_exact_shard_id())
            .is_none()
        {
            return Err(DfsError::Metadata {
                message: "namespace projection collapse requires an exact resident shard"
                    .to_string(),
            });
        }
        let file_systems = std::mem::take(&mut self.metadata.filesystems);
        self.reset_reconstructable_metadata_projection();
        self.metadata.filesystems = file_systems;
        self.namespace_execution.clear();
        self.routed_namespace_settle_depth = 0;
        self.restore_durable_intent_counter_floor()
    }
}

fn try_walk_metadata_pages_until_cancelled<C, E>(
    should_cancel: &(dyn Fn() -> bool + Sync),
    mut fetch_page: impl FnMut(Option<&C>) -> std::result::Result<Option<C>, E>,
) -> std::result::Result<bool, E> {
    let mut cursor = None;
    loop {
        if should_cancel() {
            return Ok(false);
        }
        let next_cursor = fetch_page(cursor.as_ref())?;
        let Some(next_cursor) = next_cursor else {
            return Ok(!should_cancel());
        };
        cursor = Some(next_cursor);
    }
}

fn durable_path_object_fences_replayed_create(
    durable_path_object: &DfsObjectMetadata,
    intent_record: &DfsWriteIntentRecord,
) -> bool {
    durable_path_object.kind != DfsObjectKind::Tombstone
        || intent_record
            .create_predecessor_object_id
            .as_ref()
            .is_none_or(|predecessor_object_id| {
                predecessor_object_id == &intent_record.object_id
                    || predecessor_object_id != &durable_path_object.object_id
            })
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn seed_local_metadata_journal_cursors_from_backend(
        &mut self,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        for status in backend.metadata_shard_statuses()? {
            let cursor = self
                .replication
                .local_metadata_journal_cursors
                .entry(status.shard_id)
                .or_insert(0);
            *cursor = (*cursor).max(status.journal_last_sequence);
        }
        Ok(())
    }

    fn rebuild_cross_shard_namespace_locks(&mut self) -> Result<(), DfsError> {
        let completed =
            self.rebuild_cross_shard_namespace_locks_until_cancelled(&hydration_never_cancelled)?;
        debug_assert!(
            completed,
            "non-cancellable DFS namespace-lock hydration was cancelled"
        );
        Ok(())
    }

    fn rebuild_cross_shard_namespace_locks_until_cancelled(
        &mut self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        let exact_shard_id = self
            .metadata_backend
            .as_ref()
            .and_then(|backend| backend.resident_exact_shard_id())
            .cloned();
        let Some(backend) = self.metadata_backend.as_ref() else {
            self.cross_shard_namespace_locks
                .replace_jobs(std::iter::empty::<(
                    Option<DfsMetadataShardId>,
                    String,
                    DfsFileSystemId,
                    Vec<String>,
                )>());
            return Ok(true);
        };
        let mut locks = Vec::new();
        if !try_walk_metadata_pages_until_cancelled(should_cancel, |cursor| {
            let page =
                backend.list_namespace_jobs_page(cursor, DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE)?;
            for job in page.jobs {
                if job.status == DfsNamespaceJobStatus::Running
                    && matches!(
                        job.kind,
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
                            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                    )
                {
                    let shard_id = match exact_shard_id.as_ref() {
                        Some(shard_id) => Some(shard_id.clone()),
                        None => backend
                            .metadata_shard_id_for_path(&job.file_system_id, &job.root_path)?,
                    };
                    let mut paths = vec![job.root_path.clone()];
                    if matches!(
                        job.kind,
                        DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                    ) && let Some(target_path) = job.target_path.as_ref()
                        && target_path != &job.root_path
                    {
                        paths.push(target_path.clone());
                    }
                    locks.push((
                        shard_id,
                        job.job_id.clone(),
                        job.file_system_id.clone(),
                        paths,
                    ));
                }
            }
            Ok::<_, DfsError>(page.next_cursor)
        })? {
            return Ok(false);
        }
        if should_cancel() {
            return Ok(false);
        }
        if let Some(exact_shard_id) = exact_shard_id {
            // A snapshot install can remove a previously running job. Replace
            // this shard's complete job-owned projection so that obsolete
            // guards disappear without disturbing sibling shards.
            self.cross_shard_namespace_locks.replace_shard_jobs(
                &exact_shard_id,
                locks
                    .into_iter()
                    .map(|(_, job_id, file_system_id, paths)| (job_id, file_system_id, paths)),
            );
        } else {
            self.cross_shard_namespace_locks.replace_jobs(locks);
        }
        Ok(!should_cancel())
    }

    pub(crate) fn apply_cross_shard_namespace_job_guard(
        &mut self,
        job: &DfsNamespaceJobRecord,
    ) -> Result<(), DfsError> {
        if !matches!(
            job.kind,
            DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                | DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
                | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
        ) {
            return Ok(());
        }
        if job.status == DfsNamespaceJobStatus::Running {
            let shard_id = self
                .metadata_backend
                .as_ref()
                .map(|backend| {
                    backend.metadata_shard_id_for_path(&job.file_system_id, &job.root_path)
                })
                .transpose()?
                .flatten();
            let mut paths = vec![job.root_path.clone()];
            if matches!(
                job.kind,
                DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
            ) && let Some(target_path) = job.target_path.as_ref()
                && target_path != &job.root_path
            {
                paths.push(target_path.clone());
            }
            self.cross_shard_namespace_locks.upsert_job(
                shard_id,
                job.job_id.clone(),
                job.file_system_id.clone(),
                paths,
            );
        } else {
            self.cross_shard_namespace_locks.remove_job(&job.job_id);
        }
        Ok(())
    }

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        let completed = self
            .hydrate_from_metadata_backend_with_promotion_recovery_until_cancelled(
                false,
                &hydration_never_cancelled,
            )?;
        debug_assert!(completed, "non-cancellable DFS hydration was cancelled");
        Ok(())
    }

    /// Hydrate durable service state and reconstruct post-response replica
    /// promotions in the same object/receipt/intent scan.
    ///
    /// This is deliberately separate from ordinary node-transport hydration:
    /// a maintenance service may share the served lanes' detached outbox
    /// without claiming that its own foreground writes can respond before
    /// replica repair. Keeping recovery in this one pass also avoids opening a
    /// normally hydrated service and then scanning the full backend again.
    pub(crate) fn hydrate_from_metadata_backend_for_commit_promotion_recovery(
        &mut self,
    ) -> Result<(), DfsError> {
        let completed = self
            .hydrate_from_metadata_backend_with_promotion_recovery_until_cancelled(
                true,
                &hydration_never_cancelled,
            )?;
        debug_assert!(
            completed,
            "non-cancellable DFS recovery hydration was cancelled"
        );
        Ok(())
    }

    pub(crate) fn hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
        &mut self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        self.hydrate_from_metadata_backend_with_promotion_recovery_until_cancelled(
            true,
            should_cancel,
        )
    }

    fn hydrate_from_metadata_backend_with_promotion_recovery_until_cancelled(
        &mut self,
        reconstruct_commit_promotions: bool,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(true);
        }
        if should_cancel() {
            return Ok(false);
        }
        self.metadata_backend
            .as_deref_mut()
            .expect("metadata backend checked above")
            .begin_consistent_read_snapshot()?;
        let result = self
            .hydrate_from_consistent_metadata_snapshot_with_promotion_recovery_until_cancelled(
                reconstruct_commit_promotions,
                should_cancel,
            );
        self.metadata_backend
            .as_deref_mut()
            .expect("metadata backend remains installed during hydration")
            .end_consistent_read_snapshot();
        result
    }

    fn hydrate_from_consistent_metadata_snapshot_with_promotion_recovery_until_cancelled(
        &mut self,
        reconstruct_commit_promotions: bool,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        #[cfg(test)]
        FULL_METADATA_HYDRATION_CALLS.set(FULL_METADATA_HYDRATION_CALLS.get().saturating_add(1));

        let file_systems = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend checked above")
            .list_file_systems()?;
        if should_cancel() {
            return Ok(false);
        }

        self.reset_reconstructable_metadata_projection();
        self.restore_durable_intent_counter_floor()?;
        if !self.rebuild_cross_shard_namespace_locks_until_cancelled(should_cancel)? {
            return Ok(false);
        }
        for file_system in &file_systems {
            if should_cancel() {
                return Ok(false);
            }
            self.metadata.add_file_system(file_system.clone());
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }

        // Hidden namespace prepares are not part of the visible object scan.
        // A certified current primary may safely rebind and finish standalone
        // prepares now; write-coupled create prepares remain gated on their
        // durable write receipts below.
        if !self.recover_durable_namespace_prepares_until_cancelled(should_cancel)? {
            return Ok(false);
        }
        for file_system in &file_systems {
            if !try_walk_metadata_pages_until_cancelled(should_cancel, |cursor| {
                let page = self
                    .metadata_backend
                    .as_ref()
                    .expect("metadata backend checked above")
                    .objects_for_file_system_page(
                        &file_system.file_system_id,
                        cursor,
                        DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
                    )?;
                for object in page.objects {
                    self.coordinator.set_committed_version(
                        object.file_system_id.clone(),
                        object.object_id.clone(),
                        object.version.clone(),
                    );
                    self.track_hydrated_metadata_tombstone(&object);
                    self.metadata.add_object(object.clone())?;
                }
                Ok::<_, DfsError>(page.next_cursor)
            })? {
                return Ok(false);
            }
        }
        // Conflicts already have a global cursor-backed index. Scanning that
        // index once avoids one empty per-object lookup (and its table refresh
        // checks) for every hydrated object. On a mount with thousands of
        // objects and no conflicts, the old N+1 path dominated startup.
        if !try_walk_metadata_pages_until_cancelled(should_cancel, |cursor| {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .conflict_records_page(cursor, DFS_CONFLICT_SCAN_PAGE_SIZE)?;
            for conflict in page.conflicts {
                self.metadata.add_conflict(conflict);
            }
            Ok::<_, DfsError>(page.next_cursor)
        })? {
            return Ok(false);
        }
        // Receipts already have a global cursor-backed index. Materialize it
        // once for restart hydration instead of doing two object/version scans
        // for every restorable intent (coordinator receipt restore followed by
        // visible-location recovery). Besides being linear, this avoids one
        // object-shard-index lookup per intent scan on sharded backends.
        let mut receipts_by_version = HydrationReceiptsByVersion::new();
        if !try_walk_metadata_pages_until_cancelled(should_cancel, |cursor| {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .receipt_records_page(cursor, DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE)?;
            for receipt in page.receipts {
                receipts_by_version
                    .entry((
                        receipt.receipt.object_id.clone(),
                        receipt.receipt.committed_version.clone(),
                    ))
                    .or_default()
                    .push(receipt);
            }
            Ok::<_, DfsError>(page.next_cursor)
        })? {
            return Ok(false);
        }
        let Some(durable_write_intent_shards) =
            self.durable_write_intent_shards_until_cancelled(should_cancel)?
        else {
            return Ok(false);
        };
        let mut pending_write_records = Vec::new();
        let mut recovered_commit_promotion_records = Vec::new();
        let mut repair_actionable_intent_ids = BTreeSet::new();
        if !try_walk_metadata_pages_until_cancelled(should_cancel, |intent_scan_cursor| {
            let intent_page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .write_intents_page(intent_scan_cursor, DFS_METADATA_OBJECT_INTENT_PAGE_SIZE)?;
            for mut intent_record in intent_page.intents {
                self.coordinator
                    .observe_restored_intent_id(&intent_record.intent_id);
                if !write_intent_status_requires_hydrate_restore(&intent_record.status) {
                    continue;
                }
                if !intent_record.publication.is_current() {
                    // A retained operation requires an existing identity anchor.
                    // Its saved descriptor must never become transient namespace metadata.
                    if self.metadata.object(&intent_record.object_id).is_none() {
                        continue;
                    }
                    let durable_shard_id = durable_write_intent_shards.get(&(
                        intent_record.object_id.clone(),
                        intent_record.intent_id.clone(),
                    ));
                    let locally_adopted_intent = self.adopt_prepared_write_intent_for_recovery(
                        &mut intent_record,
                        durable_shard_id,
                    )?;
                    self.restore_retained_content_intent(&intent_record)?;
                    let retained_live = intent_record
                        .content_hash
                        .as_deref()
                        .map(|hash| {
                            self.retained_file_content_is_live(
                                &intent_record.object_id,
                                &intent_record.target_version,
                                hash,
                            )
                        })
                        .transpose()?
                        .unwrap_or(false);
                    if retained_live
                        && matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    {
                        repair_actionable_intent_ids.insert(intent_record.intent_id.clone());
                    }
                    if retained_live
                        || matches!(
                            intent_record.status,
                            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                        )
                    {
                        pending_write_records.push((intent_record, locally_adopted_intent));
                    }
                    continue;
                }
                if self.metadata.object(&intent_record.object_id).is_none() {
                    if !matches!(
                        intent_record.status,
                        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                    ) {
                        continue;
                    }
                    let Some(object) = intent_record.object_metadata.clone() else {
                        continue;
                    };
                    if object.object_id != intent_record.object_id
                        || object.file_system_id != intent_record.file_system_id
                    {
                        continue;
                    }
                    let durable_path_object = self
                        .metadata_backend
                        .as_ref()
                        .expect("metadata backend checked above")
                        .get_object_by_path(&object.file_system_id, &object.path)?;
                    if durable_path_object.as_ref().is_some_and(|durable| {
                        durable_path_object_fences_replayed_create(durable, &intent_record)
                    }) {
                        // The create lost a later namespace race while its
                        // response was detached. A live object or compacted
                        // tombstone other than the exact predecessor recorded
                        // when the create began is authoritative, even if a
                        // damaged index happens to reuse this object ID.
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                }
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) && self
                    .metadata
                    .object(&intent_record.object_id)
                    .is_some_and(|object| {
                        object.file_system_id == intent_record.file_system_id
                            && Some(&object.version) != intent_record.base_version.as_ref()
                            && object.version != intent_record.target_version
                    })
                {
                    // The pinned visible snapshot has already superseded this
                    // pending write's CAS base. It cannot be resumed, but must
                    // not prevent mount/rebind or recovery of unrelated work.
                    // Leave its durable status to authority-owned maintenance:
                    // this hydration may also be reading a fenced replica.
                    continue;
                }
                self.repair_visible_committed_pending_intent_record(&mut intent_record)?;
                let durable_shard_id = durable_write_intent_shards.get(&(
                    intent_record.object_id.clone(),
                    intent_record.intent_id.clone(),
                ));
                let locally_adopted_intent = self.adopt_prepared_write_intent_for_recovery(
                    &mut intent_record,
                    durable_shard_id,
                )?;
                let intent = distributed_intent_from_record(&intent_record);
                // Bulk restore: repair jobs are rebuilt once after the scan.
                // Rebuilding per restored intent/receipt made hydration
                // quadratic over an unsettled-write backlog and burned seconds
                // of CPU on every SIFS open of an affected data dir.
                match self
                    .coordinator
                    .restore_intent_deferring_repair_rebuild(intent)
                {
                    Ok(()) => {}
                    Err(DfsDistributedError::PendingIntentConflict { .. })
                        if matches!(
                            intent_record.status,
                            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                        ) =>
                    {
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                    Err(
                        DfsDistributedError::DuplicateIntentId { .. }
                        | DfsDistributedError::DuplicateIntent { .. },
                    ) => {
                        if matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                            && self
                                .current_read_object_metadata(&intent_record.object_id)
                                .is_ok_and(|object| {
                                    object.version == intent_record.target_version
                                        && intent_record.content_hash.as_deref().is_none_or(
                                            |content_hash| {
                                                object.content_hash.as_deref() == Some(content_hash)
                                            },
                                        )
                                })
                        {
                            let receipts = receipts_by_version
                                .entry((
                                    intent_record.object_id.clone(),
                                    intent_record.target_version.clone(),
                                ))
                                .or_default();
                            self.restore_visible_committed_object_location_receipts_from_hydration_scan(
                                &intent_record,
                                receipts,
                                false,
                            )?;
                            recovered_commit_promotion_records.push(intent_record.clone());
                            continue;
                        }
                        self.mark_replayed_intent_as_rejected(&intent_record)?;
                        continue;
                    }
                    Err(error) => return Err(DfsError::from(error)),
                }
                self.restore_metadata_successor_head(&intent_record)?;
                let receipts = receipts_by_version
                    .entry((
                        intent_record.object_id.clone(),
                        intent_record.target_version.clone(),
                    ))
                    .or_default();
                self.restore_backend_receipts_for_intent_from_hydration_scan(
                    &intent_record,
                    receipts,
                    false,
                )?;
                self.restore_visible_committed_object_location_receipts_from_hydration_scan(
                    &intent_record,
                    receipts,
                    false,
                )?;
                if matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
                    recovered_commit_promotion_records.push(intent_record.clone());
                    if hydrated_committed_intent_repair_may_be_actionable(
                        &intent_record,
                        self.metadata.object(&intent_record.object_id),
                    ) || intent_record
                        .content_hash
                        .as_deref()
                        .map(|hash| {
                            self.retained_file_content_is_live(
                                &intent_record.object_id,
                                &intent_record.target_version,
                                hash,
                            )
                        })
                        .transpose()?
                        .unwrap_or(false)
                    {
                        repair_actionable_intent_ids.insert(intent_record.intent_id.clone());
                    }
                }
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging
                        | DfsWriteIntentStatus::ReceiptsPending
                        | DfsWriteIntentStatus::Committed
                ) {
                    pending_write_records.push((intent_record, locally_adopted_intent));
                }
            }
            Ok(intent_page.next_cursor)
        })? {
            return Ok(false);
        }
        self.coordinator
            .rebuild_repair_jobs_after_bulk_restore(&repair_actionable_intent_ids);
        if reconstruct_commit_promotions {
            for intent_record in &recovered_commit_promotion_records {
                if should_cancel() {
                    return Ok(false);
                }
                self.queue_recovered_commit_promotions(intent_record)?;
            }
        }

        let repair_intent_ids: BTreeSet<String> = self
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| job.intent_id.clone())
            .collect();
        let mut recovered_commit_ready_intent_ids = Vec::new();
        for (intent_record, locally_adopted_intent) in pending_write_records {
            if should_cancel() {
                return Ok(false);
            }
            if !self
                .coordinator
                .intent(&intent_record.intent_id)
                .is_some_and(|restored_intent| {
                    distributed_intent_matches_record(restored_intent, &intent_record)
                })
            {
                continue;
            }
            let commit_ready = self
                .coordinator
                .is_commit_ready(&intent_record.intent_id)
                .unwrap_or(false);
            let missing_visible_object = self.metadata.object(&intent_record.object_id).is_none();
            if missing_visible_object
                && (!matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || !commit_ready)
            {
                continue;
            }
            let authority_object = self
                .metadata
                .object(&intent_record.object_id)
                .cloned()
                .or_else(|| intent_record.object_metadata.clone());
            let locally_authoritative_intent = if matches!(
                intent_record.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            ) && self.control_plane.is_some()
            {
                locally_adopted_intent
            } else if matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                && self.control_plane.is_some()
            {
                // Repair execution is owned by the current metadata primary.
                // Keeping recovery tied to the historical writer loses the
                // durable retry queue after handoff: the new primary has no
                // sender state, while the old coordinator is correctly fenced.
                match authority_object.as_ref() {
                    Some(object) => self.is_local_metadata_primary_for_object(object)?,
                    None => false,
                }
            } else {
                intent_record.coordinator_node_id == self.node_id
            };
            let needs_pending_write = locally_authoritative_intent
                && (matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || (matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && repair_intent_ids.contains(&intent_record.intent_id)));
            if needs_pending_write && let Some(content_hash) = intent_record.content_hash.clone() {
                let size_hint = self.pending_write_size_hint(
                    &intent_record.object_id,
                    &intent_record.target_version,
                    &content_hash,
                )?;
                if missing_visible_object && size_hint.is_none() {
                    continue;
                }
                if missing_visible_object {
                    let Some(object) = authority_object else {
                        continue;
                    };
                    // A create intent is durable before its first visible object.
                    // Expose its base metadata only once durable receipts prove the
                    // write commit-ready, immediately before finalizing it.
                    self.add_transient_object_metadata(object)?;
                }
                self.writes.pending_writes.insert(
                    intent_record.intent_id.clone(),
                    PendingNetworkedWrite::new(
                        intent_record.object_id.clone(),
                        intent_record.target_version.clone(),
                        content_hash,
                        size_hint.unwrap_or(0),
                    )
                    .with_foreground_replica_repair(false),
                );
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) && commit_ready
                {
                    recovered_commit_ready_intent_ids.push(intent_record.intent_id.clone());
                }
            }
        }
        for intent_id in recovered_commit_ready_intent_ids {
            if should_cancel() {
                return Ok(false);
            }
            self.apply_recovered_commit_ready_intent(&intent_id)?;
            self.cleanup_committed_pending_write_if_repaired(&intent_id);
        }
        // Request-owned namespace prepares were completed before the object
        // scan above. Any admission which crashed before it could persist a
        // prepare is now safe to resume from its exact durable operation. The
        // bounded pass keeps service open finite; reactive maintenance repeats
        // it until no locally authoritative request remains pending.
        let _ = self.recover_pending_namespace_requests_bounded(
            crate::service_routed_namespace::DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT,
        )?;
        if should_cancel() {
            return Ok(false);
        }
        self.seed_local_metadata_journal_cursors_from_backend()?;
        Ok(!should_cancel())
    }

    /// Reconstruct post-response CommitVersion work from durable committed
    /// intents and receipts. The transport outbox deduplicates successful
    /// identities for this process; after a restart, replaying an already
    /// applied exact version/hash is safe and closes the crash window between
    /// visible metadata commit and the original detached notification.
    fn queue_recovered_commit_promotions(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
            return Ok(());
        }
        // This runs only after the hydration pass has rebuilt `metadata` from
        // the authoritative object scan and merged the already-materialized
        // global receipt scan into its locations. Going back through
        // `current_read_object_metadata` here would repeat a per-version
        // receipt lookup for every recovered promotion, defeating the
        // one-pass recovery algorithm this path exists to provide.
        let object = match self.metadata.object(&intent_record.object_id).cloned() {
            Some(object)
                if object.file_system_id == intent_record.file_system_id
                    && object.version == intent_record.target_version =>
            {
                object
            }
            _ => return Ok(()),
        };
        // Replica promotion is an authoritative metadata command. An object
        // owner is often the original content source, but placement repair can
        // install a committed intent coordinated by another node. Replaying
        // that intent from the owner would be rejected by every target and can
        // leave a detached retry loop alive indefinitely. Only the durable
        // intent coordinator, or the current metadata primary after failover,
        // may reconstruct this work.
        if intent_record.coordinator_node_id != self.node_id
            && !self.is_local_metadata_primary_for_object(&object)?
        {
            return Ok(());
        }
        let Some(content_hash) = object
            .content_hash
            .as_deref()
            .or(intent_record.content_hash.as_deref())
            .map(str::to_string)
        else {
            return Ok(());
        };
        if intent_record
            .content_hash
            .as_deref()
            .is_some_and(|expected| object.content_hash.as_deref() != Some(expected))
        {
            return Ok(());
        }
        let targets = self
            .coordinator
            .receipts_for_intent(&intent_record.intent_id)
            .into_iter()
            .filter(|receipt| {
                receipt.replica_node_id != self.node_id
                    && receipt.replica_node_id != intent_record.coordinator_node_id
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            return Ok(());
        }
        let object_metadata = Some(object);
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        for target_node_id in targets {
            let request_id = self.next_request_id();
            self.send_best_effort_message_if_routable_detached(
                &target_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_record.intent_id.clone(),
                    file_system_id: intent_record.file_system_id.clone(),
                    object_id: intent_record.object_id.clone(),
                    expected_base_version: intent_record.base_version.clone(),
                    version: intent_record.target_version.clone(),
                    content_hash: content_hash.clone(),
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
        }
        Ok(())
    }

    pub(crate) fn hydrate_file_systems_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        for file_system in backend.list_file_systems()? {
            self.metadata.add_file_system(file_system);
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }
        // Lazy mutation services still allocate intent identifiers. Restore
        // the durable floor without materializing the intent bodies so a
        // rebuilt request shell cannot reuse an identifier after restart or
        // explicit disaster recovery.
        self.restore_durable_intent_counter_floor()?;
        // Lazy services still process mutations. Restore the durable guards for
        // running cross-shard namespace jobs without paying for a full object,
        // version, intent, and receipt scan.
        self.rebuild_cross_shard_namespace_locks()?;
        self.seed_local_metadata_journal_cursors_from_backend()?;
        Ok(())
    }

    fn repair_visible_committed_pending_intent_record(
        &mut self,
        intent_record: &mut DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !matches!(
            intent_record.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) {
            return Ok(());
        }
        let Some(object) = self.metadata.object(&intent_record.object_id) else {
            return Ok(());
        };
        if object.file_system_id != intent_record.file_system_id
            || object.version != intent_record.target_version
        {
            return Ok(());
        }
        if let Some(content_hash) = intent_record.content_hash.as_deref()
            && object.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(());
        }

        if self.control_plane.is_none()
            && let Some(updated) = self.reconcile_observed_write_intent(intent_record, false)?
        {
            *intent_record = updated;
        }
        // The visible snapshot is sufficient for the in-memory projection.
        // Clustered durable reconciliation belongs to the certified primary's
        // maintenance lane, never to a replica/read-side hydration pass.
        intent_record.status = DfsWriteIntentStatus::Committed;
        Ok(())
    }

    /// Whether the backend's current record for the intent is in a terminal
    /// state (the write finished one way or another), making a replayed
    /// snapshot of that intent historical rather than actionable.
    fn backend_intent_record_is_terminal(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        Ok(backend
            .get_intent(object_id, intent_id)?
            .is_some_and(|record| {
                matches!(
                    record.status,
                    DfsWriteIntentStatus::Committed
                        | DfsWriteIntentStatus::Rejected
                        | DfsWriteIntentStatus::Conflict
                        | DfsWriteIntentStatus::Aborted
                )
            }))
    }

    pub(crate) fn restore_visible_committed_object_location_receipts(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        self.restore_visible_committed_object_location_receipts_with_repair_rebuild(
            intent_record,
            true,
        )
    }

    /// See [`restore_backend_durable_receipt_with_repair_rebuild`](Self::restore_backend_durable_receipt_with_repair_rebuild)
    /// for the `rebuild_repair_jobs` contract; bulk hydration passes `false`
    /// and rebuilds once afterwards.
    pub(crate) fn restore_visible_committed_object_location_receipts_with_repair_rebuild(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        let mut receipts = self.backend_receipts_for_version(
            &intent_record.object_id,
            &intent_record.target_version,
        )?;
        self.restore_visible_committed_object_location_receipts_from_hydration_scan(
            intent_record,
            &mut receipts,
            rebuild_repair_jobs,
        )
    }

    fn restore_backend_receipts_for_intent_from_hydration_scan(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        for receipt in receipts {
            if let Some(expected_hash) = intent_record.content_hash.as_deref()
                && receipt.content_hash.as_deref() != Some(expected_hash)
            {
                continue;
            }
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id.clone(),
                false,
                rebuild_repair_jobs,
            )?;
        }
        Ok(())
    }

    fn restore_visible_committed_object_location_receipts_from_hydration_scan(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        receipts: &mut Vec<DfsReplicaReceiptRecord>,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        if !matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
            return Ok(());
        }
        let Some(object) = self.metadata.object(&intent_record.object_id).cloned() else {
            return Ok(());
        };
        if object.file_system_id != intent_record.file_system_id
            || object.version != intent_record.target_version
        {
            return Ok(());
        }
        if let Some(content_hash) = intent_record.content_hash.as_deref()
            && object.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(());
        }
        let Some(content_hash) = object
            .content_hash
            .as_deref()
            .or(intent_record.content_hash.as_deref())
        else {
            return Ok(());
        };

        let mut existing_receipts = BTreeSet::new();
        for receipt in receipts.iter() {
            if receipt.content_hash.as_deref() != Some(content_hash) {
                continue;
            }
            existing_receipts.insert(receipt.replica_node_id.clone());
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id.clone(),
                false,
                rebuild_repair_jobs,
            )?;
        }

        let owner_node_id = object.owner_node_id.clone();
        for replica_node_id in object.current_locations {
            if !intent_record
                .desired_replica_node_ids
                .contains(&replica_node_id)
            {
                continue;
            }
            let has_verified_local_blob = replica_node_id == self.node_id
                && self
                    .ensure_local_repair_manifest(&object.object_id, &object.version, content_hash)
                    .unwrap_or(false);
            if !existing_receipts.contains(&replica_node_id)
                && has_verified_local_blob
                && let Some(backend) = self.metadata_backend.as_mut()
            {
                let recovered_receipt = DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: intent_record.file_system_id.clone(),
                        object_id: intent_record.object_id.clone(),
                        committed_version: intent_record.target_version.clone(),
                        owner_node_id: owner_node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                };
                backend.put_receipt(&recovered_receipt)?;
                if let Some(existing_receipt) = receipts
                    .iter_mut()
                    .find(|receipt| receipt.replica_node_id == replica_node_id)
                {
                    *existing_receipt = recovered_receipt;
                } else {
                    receipts.push(recovered_receipt);
                }
                existing_receipts.insert(replica_node_id.clone());
            }
            if existing_receipts.contains(&replica_node_id) {
                self.restore_backend_durable_receipt_with_repair_rebuild(
                    &intent_record.intent_id,
                    replica_node_id,
                    false,
                    rebuild_repair_jobs,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_metadata_journal_batch_to_service_state(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        for entry in &batch.entries {
            self.apply_metadata_journal_entry_to_service_state(entry)?;
        }
        self.refresh_metadata_journal_batch_objects_from_backend(batch)?;
        let cursor = self
            .replication
            .local_metadata_journal_cursors
            .entry(batch.shard_id.clone())
            .or_insert(0);
        *cursor = (*cursor).max(batch.next_after_sequence);
        Ok(())
    }

    /// Refresh only the affected visible objects from the final durable view.
    /// Routed writers can retain optimistic create metadata until their reply
    /// is replayed; replace that projection without restoring historical intent
    /// coordinators or originating any metadata mutation on this replica.
    pub(crate) fn refresh_metadata_journal_batch_objects_from_backend(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        if self.metadata_backend.is_some() {
            let affected_object_ids = batch
                .entries
                .iter()
                .filter(|entry| entry.mutation.changes_visible_object_placement())
                .flat_map(|entry| entry.mutation.affected_object_ids())
                .collect::<BTreeSet<_>>();
            if !affected_object_ids.is_empty() {
                self.metadata_backend
                    .as_mut()
                    .expect("metadata backend checked above")
                    .begin_consistent_read_snapshot()?;
                let apply_result: Result<(), DfsError> = (|| {
                    for object_id in affected_object_ids {
                        // The raw payload order is shard-local, while the sharded
                        // backend has just reconciled global object placement. End
                        // the replay with one pinned authoritative view so a late
                        // source-shard payload cannot leave the service cache on the
                        // pre-move path and a bulk commit does not refresh the table
                        // generation once per object.
                        self.apply_backend_object_to_service_state(&object_id)?;
                    }
                    Ok(())
                })();
                self.metadata_backend
                    .as_mut()
                    .expect("metadata backend remains installed")
                    .end_consistent_read_snapshot();
                apply_result?;
            }
        }
        Ok(())
    }

    /// Observe a journal batch that was durably applied as replica state.
    ///
    /// Replica tables are the authoritative local copy. Mirroring every
    /// visible object, version, receipt, intent, and conflict into the service
    /// shell duplicates the complete namespace on every replica and repeats
    /// object lookups after the backend already applied the same batch. The
    /// lazy mutation lane only needs the in-memory guards that fence active
    /// cross-shard namespace work plus its replay cursor. If this replica is
    /// later promoted, membership rebind performs one authoritative hydration
    /// before admitting mutations.
    pub(crate) fn observe_replicated_metadata_journal_batch(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        for entry in &batch.entries {
            let DfsMetadataMutation::PutNamespaceJob { job_id, job, .. } = &entry.mutation else {
                continue;
            };
            let job = match job {
                Some(job) => Some(job.clone()),
                None => self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_namespace_job(job_id))
                    .transpose()?
                    .flatten(),
            };
            if let Some(job) = job.as_ref() {
                self.apply_cross_shard_namespace_job_guard(job)?;
            }
        }

        let cursor = self
            .replication
            .local_metadata_journal_cursors
            .entry(batch.shard_id.clone())
            .or_insert(0);
        *cursor = (*cursor).max(batch.next_after_sequence);
        Ok(())
    }

    /// Collect only the repaired shard's job guards while its exact snapshot
    /// read is pinned. Callers publish the complete replacement together with
    /// the matching cursor, after every bounded page was read successfully.
    pub(crate) fn replica_snapshot_namespace_guard_projection(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Vec<(String, DfsFileSystemId, Vec<String>)>, DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "snapshot guard refresh requires a metadata backend".into(),
            })?;
        let mut cursor = None;
        let mut guards = Vec::new();
        loop {
            let page = backend.replica_snapshot_namespace_jobs_page(
                shard_id,
                cursor.as_ref(),
                DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
            )?;
            for job in page.jobs {
                if job.status != DfsNamespaceJobStatus::Running
                    || !matches!(
                        job.kind,
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                            | DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
                            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                    )
                {
                    continue;
                }
                let mut paths = vec![job.root_path.clone()];
                if job.kind == DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                    && let Some(target) = job.target_path
                    && target != job.root_path
                {
                    paths.push(target);
                }
                guards.push((job.job_id, job.file_system_id, paths));
            }
            let Some(next) = page.next_cursor else {
                return Ok(guards);
            };
            cursor = Some(next);
        }
    }

    /// Snapshot replacement may remove objects altogether. Retire only this
    /// shard's rebuildable object views so a missing durable row cannot fall
    /// back to an old memory object. Live write ownership and caller planning
    /// predicates remain in place; they still settle or fail their own fences.
    pub(crate) fn replica_snapshot_object_projections_to_retire(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Vec<(DfsFileSystemId, DfsObjectId)>, DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "snapshot projection refresh requires a metadata backend".into(),
            })?;
        let mut objects = BTreeMap::new();
        for object in self
            .metadata
            .objects
            .values()
            .chain(self.tombstones_by_metadata_path.values())
        {
            if self
                .coordinator
                .has_open_intent_for_object(&object.object_id)
                || self.writes.pending_routed_writes.values().any(|pending| {
                    pending.object_id == object.object_id && pending.result.is_none()
                })
                || backend
                    .metadata_shard_id_for_path(&object.file_system_id, &object.path)?
                    .as_ref()
                    != Some(shard_id)
            {
                continue;
            }
            objects.insert(object.object_id.clone(), object.file_system_id.clone());
        }
        Ok(objects
            .into_iter()
            .map(|(object, file_system)| (file_system, object))
            .collect())
    }

    pub(crate) fn retire_replica_snapshot_object_projections(
        &mut self,
        objects: Vec<(DfsFileSystemId, DfsObjectId)>,
    ) {
        let ids = objects
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<BTreeSet<_>>();
        self.tombstones_by_metadata_path
            .retain(|_, object| !ids.contains(&object.object_id));
        self.reads
            .transient_remote_read_pruned_locations
            .retain(|(object_id, _, _), _| !ids.contains(object_id));
        for (file_system_id, object_id) in objects {
            self.remove_transient_object_metadata(&object_id);
            self.coordinator
                .forget_committed_version(&file_system_id, &object_id);
        }
    }

    fn apply_metadata_journal_entry_to_service_state(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<(), DfsError> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                let Some(file_system) = file_system.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.metadata.add_file_system(file_system.clone());
            }
            DfsMetadataMutation::PutObject { object, .. } => {
                let Some(object) = object.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_object_to_service_state(object.clone())?;
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_version_to_service_state(version_record);
            }
            DfsMetadataMutation::CaptureFileVersion { .. }
            | DfsMetadataMutation::RemoveVersion { .. } => {}
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } => {
                if receipt.is_none() {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                }
                self.apply_metadata_receipt_to_service_state(object_id, version, replica_node_id)?;
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if let Some(object) = self.metadata.objects.get_mut(object_id)
                    && object.version == *version
                {
                    object
                        .current_locations
                        .retain(|node_id| node_id != replica_node_id);
                }
            }
            DfsMetadataMutation::PutIntent { intent, .. } => {
                let Some(intent) = intent.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_intent_to_service_state(intent)?;
            }
            DfsMetadataMutation::PutIntentReceipt { receipt, .. } => {
                self.apply_metadata_intent_to_service_state(&receipt.to_intent_record())?;
            }
            DfsMetadataMutation::RemoveRetainedRepairOwner { intent_id, .. } => {
                self.retire_committed_repair(intent_id);
            }
            DfsMetadataMutation::RemoveIntentReceipt { .. }
            | DfsMetadataMutation::MergeIntentReceiptRetention { .. } => {}
            DfsMetadataMutation::CommitVisibleVersion {
                committed_object,
                visible_version,
                object_id,
                committed_version,
                ..
            } => {
                let Some(committed_object) = committed_object.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                let Some(visible_version) = visible_version.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_object_to_service_state(committed_object.clone())?;
                self.apply_metadata_version_to_service_state(visible_version);
                self.restore_backend_receipts_for_visible_version(object_id, committed_version)?;
            }
            DfsMetadataMutation::RemoveVisibleCommit { .. } => {}
            DfsMetadataMutation::PutVersionPin { .. }
            | DfsMetadataMutation::RemoveVersionPin { .. }
            | DfsMetadataMutation::ReconcileCrossShardPinTransfer { .. }
            | DfsMetadataMutation::RemoveCrossShardPinTransfer { .. }
            | DfsMetadataMutation::FinalizeCrossShardSourceEviction { .. } => {}
            DfsMetadataMutation::PutConflict { conflict, .. } => {
                let Some(conflict) = conflict.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.metadata.add_conflict(conflict.clone());
            }
            // Advisory locks are consumed directly from the authoritative
            // metadata backend. Namespace jobs have no legacy object mirror,
            // but their running/terminal state owns the replica-local
            // failover guard and must update it on every replayed transition.
            DfsMetadataMutation::PutAdvisoryLockState { .. }
            | DfsMetadataMutation::PutNamespacePrepare { .. }
            | DfsMetadataMutation::CommitNamespacePrepare { .. }
            | DfsMetadataMutation::PutNamespaceRequest { .. }
            | DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
            | DfsMetadataMutation::RemoveNamespaceTransactionReceipt { .. }
            | DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
            | DfsMetadataMutation::RemoveNamespacePrepareReceipt { .. }
            | DfsMetadataMutation::PutNamespaceJobReceipt { .. }
            | DfsMetadataMutation::RemoveNamespaceJobReceipt { .. }
            | DfsMetadataMutation::PutNamespaceRequestReceipt { .. } => {}
            DfsMetadataMutation::RemoveNamespaceRequestReceipt { .. } => {}
            DfsMetadataMutation::PutNamespaceJob { job, .. } => {
                let Some(job) = job.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_cross_shard_namespace_job_guard(job)?;
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                let Some(transaction) = transaction.as_ref() else {
                    self.apply_missing_payload_metadata_journal_entry_to_service_state(entry)?;
                    return Ok(());
                };
                self.apply_metadata_namespace_transaction_to_service_state(transaction)?;
            }
        }
        Ok(())
    }

    fn apply_missing_payload_metadata_journal_entry_to_service_state(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<(), DfsError> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system_id, .. } => {
                let file_system = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_file_system(file_system_id))
                    .transpose()?
                    .flatten();
                if let Some(file_system) = file_system {
                    self.metadata.add_file_system(file_system);
                }
            }
            DfsMetadataMutation::PutObject { object_id, .. } => {
                self.apply_backend_object_to_service_state(object_id)?;
            }
            DfsMetadataMutation::PutVersion {
                object_id, version, ..
            } => {
                let version_record = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_version(object_id, version))
                    .transpose()?
                    .flatten();
                if let Some(version_record) = version_record {
                    self.apply_metadata_version_to_service_state(&version_record);
                }
            }
            DfsMetadataMutation::CaptureFileVersion { .. }
            | DfsMetadataMutation::RemoveVersion { .. } => {}
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if self
                    .backend_receipt_for_replica(object_id, version, replica_node_id)?
                    .is_some()
                {
                    self.apply_metadata_receipt_to_service_state(
                        object_id,
                        version,
                        replica_node_id,
                    )?;
                }
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if let Some(object) = self.metadata.objects.get_mut(object_id)
                    && object.version == *version
                {
                    object
                        .current_locations
                        .retain(|node_id| node_id != replica_node_id);
                }
            }
            DfsMetadataMutation::PutIntent {
                object_id,
                intent_id,
                ..
            } => {
                let intent = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_intent(object_id, intent_id))
                    .transpose()?
                    .flatten();
                if let Some(intent) = intent {
                    self.apply_metadata_intent_to_service_state(&intent)?;
                }
            }
            DfsMetadataMutation::PutIntentReceipt { receipt, .. } => {
                self.apply_metadata_intent_to_service_state(&receipt.to_intent_record())?;
            }
            DfsMetadataMutation::RemoveRetainedRepairOwner { intent_id, .. } => {
                self.retire_committed_repair(intent_id);
            }
            DfsMetadataMutation::RemoveIntentReceipt { .. }
            | DfsMetadataMutation::MergeIntentReceiptRetention { .. } => {}
            DfsMetadataMutation::CommitVisibleVersion {
                object_id,
                committed_version,
                ..
            } => {
                self.apply_backend_object_to_service_state(object_id)?;
                let version_record = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_version(object_id, committed_version))
                    .transpose()?
                    .flatten();
                if let Some(version_record) = version_record {
                    self.apply_metadata_version_to_service_state(&version_record);
                }
                self.restore_backend_receipts_for_visible_version(object_id, committed_version)?;
            }
            DfsMetadataMutation::RemoveVisibleCommit { .. } => {}
            DfsMetadataMutation::PutVersionPin { .. }
            | DfsMetadataMutation::RemoveVersionPin { .. }
            | DfsMetadataMutation::ReconcileCrossShardPinTransfer { .. }
            | DfsMetadataMutation::RemoveCrossShardPinTransfer { .. }
            | DfsMetadataMutation::FinalizeCrossShardSourceEviction { .. } => {}
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                ..
            } => {
                let conflict = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_conflict(object_id, conflict_id))
                    .transpose()?
                    .flatten();
                if let Some(conflict) = conflict {
                    self.metadata.add_conflict(conflict);
                } else {
                    self.metadata.remove_conflict(object_id, conflict_id);
                }
            }
            DfsMetadataMutation::PutAdvisoryLockState { .. }
            | DfsMetadataMutation::PutNamespacePrepare { .. }
            | DfsMetadataMutation::CommitNamespacePrepare { .. }
            | DfsMetadataMutation::PutNamespaceRequest { .. }
            | DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
            | DfsMetadataMutation::RemoveNamespaceTransactionReceipt { .. }
            | DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
            | DfsMetadataMutation::RemoveNamespacePrepareReceipt { .. }
            | DfsMetadataMutation::PutNamespaceJobReceipt { .. }
            | DfsMetadataMutation::RemoveNamespaceJobReceipt { .. }
            | DfsMetadataMutation::PutNamespaceRequestReceipt { .. } => {}
            DfsMetadataMutation::RemoveNamespaceRequestReceipt { .. } => {}
            DfsMetadataMutation::PutNamespaceJob { job_id, .. } => {
                let job = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_namespace_job(job_id))
                    .transpose()?
                    .flatten();
                if let Some(job) = job.as_ref() {
                    self.apply_cross_shard_namespace_job_guard(job)?;
                }
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction_id, .. } => {
                let transaction = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_namespace_transaction(transaction_id))
                    .transpose()?
                    .flatten();
                if let Some(transaction) = transaction {
                    self.apply_metadata_namespace_transaction_to_service_state(&transaction)?;
                } else {
                    return Err(DfsError::Metadata {
                        message: format!(
                            "metadata journal entry {}:{} cannot apply namespace transaction {} without its transaction payload",
                            entry.shard_id.0, entry.sequence, transaction_id
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn apply_metadata_namespace_transaction_to_service_state(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        let mut objects = transaction.objects.clone();
        objects.sort_by(|left, right| {
            let left_is_tombstone = left.kind == DfsObjectKind::Tombstone;
            let right_is_tombstone = right.kind == DfsObjectKind::Tombstone;
            right_is_tombstone
                .cmp(&left_is_tombstone)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        for object in objects {
            self.apply_metadata_object_to_service_state(object)?;
        }
        for version in &transaction.versions {
            self.apply_metadata_version_to_service_state(version);
        }
        Ok(())
    }

    pub(crate) fn apply_backend_object_to_service_state(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let object = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_object(object_id))
            .transpose()?
            .flatten();
        if let Some(object) = object {
            self.apply_metadata_object_to_service_state(object)?;
        }
        Ok(())
    }

    fn restore_backend_receipts_for_visible_version(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<(), DfsError> {
        for receipt in self.backend_receipts_for_version(object_id, version)? {
            self.apply_metadata_receipt_to_service_state(
                object_id,
                version,
                &receipt.replica_node_id,
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_metadata_object_to_service_state(
        &mut self,
        mut object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        normalize_current_locations(&mut object.current_locations);
        self.transient_metadata_object_ids.remove(&object.object_id);
        self.transient_metadata_predecessor_object_ids
            .remove(&object.object_id);
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            self.metadata.add_file_system(DfsFileSystem {
                file_history: Default::default(),
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)?;
        Ok(())
    }

    pub(crate) fn apply_metadata_version_to_service_state(
        &mut self,
        version: &DfsObjectVersionRecord,
    ) {
        if !version.visible {
            return;
        }
        let Some(object) = self.metadata.objects.get_mut(&version.object_id) else {
            return;
        };
        if object.version != version.version {
            return;
        }
        if object.content_hash.is_none() {
            object.content_hash = version.content_hash.clone();
        }
        if version.size_bytes.is_some() {
            object.content_size_bytes = version.size_bytes;
        }
        if !version.locations.is_empty() {
            object.current_locations = version.locations.clone();
            normalize_current_locations(&mut object.current_locations);
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
    }

    fn apply_metadata_intent_to_service_state(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !intent_record.publication.is_current() {
            return self.restore_retained_content_intent(intent_record);
        }
        let intent = distributed_intent_from_record(intent_record);
        match self.coordinator.restore_intent(intent) {
            Ok(()) => {}
            Err(DfsDistributedError::PendingIntentConflict { .. }) => {
                self.mark_replayed_intent_as_rejected(intent_record)?;
                return Ok(());
            }
            Err(
                DfsDistributedError::DuplicateIntentId { .. }
                | DfsDistributedError::DuplicateIntent { .. },
            ) => {
                if self
                    .coordinator
                    .intent(&intent_record.intent_id)
                    .is_some_and(|intent| distributed_intent_matches_record(intent, intent_record))
                {
                    // Same intent replayed after local state had already advanced.
                } else if matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && self
                        .current_read_object_metadata(&intent_record.object_id)
                        .is_ok_and(|object| {
                            object.version == intent_record.target_version
                                && intent_record.content_hash.as_deref().is_none_or(
                                    |content_hash| {
                                        object.content_hash.as_deref() == Some(content_hash)
                                    },
                                )
                        })
                {
                    self.restore_visible_committed_object_location_receipts(intent_record)?;
                } else {
                    self.mark_replayed_intent_as_rejected(intent_record)?;
                    return Ok(());
                }
            }
            Err(DfsDistributedError::BaseVersionConflict { .. }) => {
                if self
                    .current_read_object_metadata(&intent_record.object_id)
                    .is_ok_and(|object| {
                        object.version == intent_record.target_version
                            && intent_record
                                .content_hash
                                .as_deref()
                                .is_none_or(|content_hash| {
                                    object.content_hash.as_deref() == Some(content_hash)
                                })
                    })
                {
                    if matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
                        self.restore_visible_committed_object_location_receipts(intent_record)?;
                    }
                    return Ok(());
                }
                // Journal replay can revisit an intent that later history in
                // the same stream already superseded — either because the
                // backend applied the whole batch before service-state replay
                // or because a republished batch duplicates entries this node
                // replayed earlier. The backend's current intent record is
                // authoritative: a terminal record makes the replayed entry
                // historical rather than inconsistent.
                if self.backend_intent_record_is_terminal(
                    &intent_record.object_id,
                    &intent_record.intent_id,
                )? {
                    return Ok(());
                }
                self.mark_replayed_intent_as_rejected(intent_record)?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
        self.restore_metadata_successor_head(intent_record)?;

        if matches!(
            intent_record.status,
            DfsWriteIntentStatus::Staging
                | DfsWriteIntentStatus::ReceiptsPending
                | DfsWriteIntentStatus::Committed
        ) && let Some(content_hash) = intent_record.content_hash.clone()
            && !self
                .writes
                .pending_writes
                .contains_key(&intent_record.intent_id)
        {
            let size_bytes = self
                .pending_write_size_hint(
                    &intent_record.object_id,
                    &intent_record.target_version,
                    &content_hash,
                )?
                .unwrap_or(0);
            self.writes.pending_writes.insert(
                intent_record.intent_id.clone(),
                PendingNetworkedWrite::new(
                    intent_record.object_id.clone(),
                    intent_record.target_version.clone(),
                    content_hash,
                    size_bytes,
                )
                .with_foreground_replica_repair(false),
            );
        }
        Ok(())
    }

    fn mark_replayed_intent_as_rejected(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if self.control_plane.is_none() {
            self.reconcile_observed_write_intent_with_policy(intent_record, false, true)?;
        }
        self.remove_pending_write_state(&intent_record.intent_id);
        Ok(())
    }

    fn apply_metadata_receipt_to_service_state(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        let intent_ids = self.backend_intent_ids_for_version(object_id, version)?;
        for intent_id in intent_ids {
            self.restore_backend_durable_receipt(&intent_id, replica_node_id.clone(), true)?;
        }
        Ok(())
    }

    pub(crate) fn pending_write_size_hint(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<Option<u64>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes.is_some()
        {
            return Ok(version_record.size_bytes);
        }
        match self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
        {
            Ok(manifest) if manifest.content_hash == content_hash => Ok(Some(manifest.size_bytes)),
            Ok(_) => Ok(None),
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                match self.content.open_blob_reader(content_hash) {
                    Ok(reader) => Ok(Some(reader.size_bytes())),
                    Err(ContentStoreError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        Ok(None)
                    }
                    Err(error) => Err(error.into()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}
