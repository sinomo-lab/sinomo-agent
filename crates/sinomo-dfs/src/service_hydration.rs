//! Metadata backend hydration and journal replay for the networked DFS service.
//!
//! This module keeps durable metadata replay, missing-payload journal recovery,
//! and in-memory service-state restoration out of the live data-plane protocol
//! code.

use std::collections::{BTreeMap, BTreeSet};

use crate::transfer::PendingNetworkedWrite;
use crate::*;

type HydrationReceiptsByVersion = BTreeMap<(DfsObjectId, DfsVersion), Vec<DfsReplicaReceiptRecord>>;

fn hydration_never_cancelled() -> bool {
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
        self.coordinator.reset_projection();
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
        // Guards belong to shards this node replicates. A local copy of any
        // other shard is not kept current, so its Running jobs would block
        // paths after they finished; that shard's primary enforces its own.
        let replicated = if exact_shard_id.is_none() {
            self.locally_replicated_sifs_metadata_shards()
        } else {
            None
        };
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
                    && let Some(paths) =
                        crate::service_state::cross_shard_namespace_job_guard_paths(&job)
                {
                    let shard_id = match exact_shard_id.as_ref() {
                        Some(shard_id) => Some(shard_id.clone()),
                        None => backend
                            .metadata_shard_id_for_path(&job.file_system_id, &job.root_path)?,
                    };
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
        // The scan left closed copies out; their guards, installed earlier by
        // replica ingest or snapshot install, stay until the shard opens and a
        // later hydration reads its jobs.
        let closed = backend.closed_metadata_shard_ids();
        if let Some(replicated) = replicated.as_ref() {
            let sifs = sifs_file_system_id();
            locks.retain(|(shard_id, _, file_system_id, _)| {
                file_system_id != &sifs
                    || shard_id
                        .as_ref()
                        .is_none_or(|shard_id| replicated.contains(shard_id))
            });
        }
        #[cfg(test)]
        if let Some((root, _)) = self.native_publication_owner_for_observation()
            && crate::native_boundary_diagnostics::enabled(&root)
            && (!locks.is_empty() || !self.cross_shard_namespace_locks.is_empty())
        {
            crate::native_boundary_diagnostics::record(
                &root,
                "namespace-guard-rebuild",
                || {
                    serde_json::json!({
                        "node": self.node_id, "exact_shard": exact_shard_id, "incoming": locks,
                        "previous": format!("{:?}", self.cross_shard_namespace_locks),
                    })
                },
                true,
            );
        }
        let selected = self
            .metadata_backend
            .as_ref()
            .and_then(|backend| backend.resident_selected_shard_ids())
            .cloned();
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
        } else if let Some(selected) = selected {
            // A several-shard lease read only its own shards' jobs: replace
            // each of those shards' guards and leave every other shard's.
            for shard_id in &selected {
                if closed.contains(shard_id) {
                    continue;
                }
                self.cross_shard_namespace_locks.replace_shard_jobs(
                    shard_id,
                    locks
                        .iter()
                        .filter(|(lock_shard, ..)| lock_shard.as_ref() == Some(shard_id))
                        .map(|(_, job_id, file_system_id, paths)| {
                            (job_id.clone(), file_system_id.clone(), paths.clone())
                        }),
                );
            }
        } else {
            self.cross_shard_namespace_locks
                .replace_jobs_keeping_shards(locks, &closed);
        }
        Ok(!should_cancel())
    }

    /// The SIFS metadata shards this node holds as primary or replica, or
    /// `None` when the shard configuration cannot be read; callers then keep
    /// treating every local shard as the node's own.
    pub(crate) fn locally_replicated_sifs_metadata_shards(
        &mut self,
    ) -> Option<BTreeSet<DfsMetadataShardId>> {
        let kv = self.control_plane.as_mut()?;
        let shards = DfsControlPlaneStore::new(kv.as_mut())
            .list_metadata_shards(&sifs_file_system_id())
            .ok()?;
        Some(
            shards
                .into_iter()
                .filter(|shard| {
                    (shard.primary_node_id == self.node_id
                        || shard.replica_node_ids.contains(&self.node_id))
                        && !self.metadata_lease_omits_shard(&shard.shard_id)
                })
                .map(|shard| DfsMetadataShardId::new(shard.shard_id))
                .collect(),
        )
    }

    /// Whether this service's resident lease leaves `shard_id` out: a
    /// several-shard lease holds only the shards this node is primary of in
    /// one filesystem, and its service reads any other shard from that
    /// shard's primary, as a node outside the shard's replica set does.
    pub(crate) fn metadata_lease_omits_shard(&self, shard_id: &str) -> bool {
        self.metadata_backend
            .as_ref()
            .and_then(|backend| backend.resident_selected_shard_ids())
            .is_some_and(|selected| !selected.contains(&DfsMetadataShardId::new(shard_id)))
    }

    /// Whether `path` lies in a SIFS shard outside `replicated`. An unknown
    /// replicated set (`None`) keeps every local shard.
    pub(crate) fn path_in_unreplicated_sifs_shard(
        &self,
        replicated: Option<&BTreeSet<DfsMetadataShardId>>,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<bool, DfsError> {
        let (Some(replicated), Some(backend)) = (replicated, self.metadata_backend.as_ref()) else {
            return Ok(false);
        };
        if *file_system_id != sifs_file_system_id() {
            return Ok(false);
        }
        Ok(backend
            .metadata_shard_id_for_path(file_system_id, path)?
            .is_some_and(|shard_id| !replicated.contains(&shard_id)))
    }

    pub(crate) fn apply_cross_shard_namespace_job_guard(
        &mut self,
        job: &DfsNamespaceJobRecord,
    ) -> Result<(), DfsError> {
        if crate::service_state::cross_shard_namespace_job_guard_paths(job).is_none() {
            return Ok(());
        }
        #[cfg(test)]
        if let Some((root, _)) = self.native_publication_owner_for_observation()
            && crate::native_boundary_diagnostics::enabled(&root)
        {
            crate::native_boundary_diagnostics::record(
                &root,
                "namespace-guard-apply",
                || {
                    serde_json::json!({
                        "node": self.node_id, "incoming_job": job,
                        "previous": format!("{:?}", self.cross_shard_namespace_locks),
                    })
                },
                true,
            );
        }
        let shard_id = if job.status == DfsNamespaceJobStatus::Running {
            self.metadata_backend
                .as_ref()
                .map(|backend| {
                    backend.metadata_shard_id_for_path(&job.file_system_id, &job.root_path)
                })
                .transpose()?
                .flatten()
        } else {
            None
        };
        self.cross_shard_namespace_locks.observe_job(shard_id, job);
        Ok(())
    }

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        let completed = self
            .hydrate_from_metadata_backend_with_promotion_recovery_until_cancelled(
                false,
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
                false,
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
            true,
            should_cancel,
        )
    }

    #[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.metadata.hydration",
        skip_all, fields(stage = "dfs.metadata.hydration", reconstruct_commit_promotions,
        outcome = tracing::field::Empty))]
    fn hydrate_from_metadata_backend_with_promotion_recovery_until_cancelled(
        &mut self,
        reconstruct_commit_promotions: bool,
        try_writer: bool,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(true);
        }
        if should_cancel() {
            return Ok(false);
        }
        let mut observation =
            crate::background_observation::DfsBackgroundObservation::new("hydration", "rebuild");
        let result = (|| {
            let backend = self.metadata_backend.as_deref_mut().expect("metadata backend checked above");
            if try_writer {
                if !backend.try_begin_consistent_read_snapshot()? { return Ok(false); }
            } else {
                backend.begin_consistent_read_snapshot()?;
            }
            // Preserve the original coherent catalog/reset setup, then end
            // that read scope before recovery can publish or observe a commit.
            let setup = (|| -> Result<bool, DfsError> {
                let file_systems = self
                    .metadata_backend
                    .as_ref()
                    .expect("metadata backend checked above")
                    .list_file_systems()?;
                if should_cancel() {
                    return Ok(false);
                }

                if !self
                    .project_hydration_file_systems_until_cancelled(&file_systems, should_cancel)
                {
                    return Ok(false);
                }
                // A cancelled or failed rebuild returns this same request
                // shell to its owner. Keep its complete coherent catalog at
                // every handback boundary, even before job/object replay.
                let catalog = std::mem::take(&mut self.metadata.filesystems);
                // These are live protocol owners, not a metadata projection.
                // In particular an outstanding Fetch may still own the only
                // genuine pre-dispatch facts for its later N-W repair. Keep
                // them even when this rebuild yields before restoring intents.
                let pending_writes = std::mem::take(&mut self.writes.pending_writes);
                self.reset_reconstructable_metadata_projection();
                self.writes.pending_writes = pending_writes;
                self.metadata.filesystems = catalog;
                self.restore_durable_intent_counter_floor()?;
                if !self.rebuild_cross_shard_namespace_locks_until_cancelled(should_cancel)? {
                    return Ok(false);
                }

                Ok(true)
            })();
            self.metadata_backend
                .as_deref_mut()
                .expect("metadata backend remains installed during hydration")
                .end_consistent_read_snapshot();
            if !setup? {
                return Ok(false);
            }
            let recovery = self.recover_durable_namespace_prepares_pass(should_cancel)?;
            if !recovery.completed {
                // A live unknown prepare must be reported to the owner, not
                // mistaken for callback cancellation (or a completed rebuild).
                // Keep its hidden effects out of the hydration projection.
                if let Some(unknown) = recovery.first_unknown {
                    return Err(unknown);
                }
                return Ok(false);
            }
            let backend = self.metadata_backend.as_deref_mut().expect("metadata backend checked above");
            if try_writer {
                if !backend.try_begin_consistent_read_snapshot()? { return Ok(false); }
            } else {
                backend.begin_consistent_read_snapshot()?;
            }
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
        })();
        observation.finish(match &result {
            Ok(true) => "complete",
            Ok(false) => "cancelled",
            Err(_) => "error",
        });
        result
    }

    fn project_hydration_file_systems_until_cancelled(
        &mut self,
        file_systems: &[DfsFileSystem],
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> bool {
        let mut catalog = BTreeMap::new();
        for file_system in file_systems {
            if should_cancel() {
                return false;
            }
            catalog.insert(file_system.file_system_id.clone(), file_system.clone());
        }
        catalog
            .entry(sifs_file_system_id())
            .or_insert_with(|| DfsFileSystem::sifs(self.node_id.clone()));
        self.metadata.filesystems = catalog;
        true
    }

    fn hydrate_from_consistent_metadata_snapshot_with_promotion_recovery_until_cancelled(
        &mut self,
        reconstruct_commit_promotions: bool,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        // Only shards this node replicates are kept current; a local copy of
        // any other shard stays out of the service's view.
        let replicated = self.locally_replicated_sifs_metadata_shards();
        let file_systems = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend checked above")
            .list_file_systems()?;
        if should_cancel() {
            return Ok(false);
        }

        if !self.project_hydration_file_systems_until_cancelled(&file_systems, should_cancel) {
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
                crate::background_observation::hydration_items("objects", page.objects.len());
                for object in page.objects {
                    if self.path_in_unreplicated_sifs_shard(
                        replicated.as_ref(),
                        &object.file_system_id,
                        &object.path,
                    )? {
                        continue;
                    }
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
            crate::background_observation::hydration_items("receipts", page.receipts.len());
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
        if !try_walk_metadata_pages_until_cancelled(should_cancel, |intent_scan_cursor| {
            let intent_page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .write_intents_page(intent_scan_cursor, DFS_METADATA_OBJECT_INTENT_PAGE_SIZE)?;
            crate::background_observation::hydration_items("intents", intent_page.intents.len());
            for mut intent_record in intent_page.intents {
                self.coordinator
                    .observe_restored_intent_id(&intent_record.intent_id);
                if !write_intent_status_requires_hydrate_restore(&intent_record.status) {
                    continue;
                }
                if replicated.as_ref().is_some_and(|replicated| {
                    intent_record.file_system_id == sifs_file_system_id()
                        && durable_write_intent_shards
                            .get(&(
                                intent_record.object_id.clone(),
                                intent_record.intent_id.clone(),
                            ))
                            .is_some_and(|shard_id| !replicated.contains(shard_id))
                }) {
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
                    // Before W, retained staging has no published version yet.
                    // Only committed repair owners require a live retained root;
                    // pending owners continue through the original staging path.
                    let retained_live = intent_record.status == DfsWriteIntentStatus::Committed
                        && self
                            .metadata
                            .object(&intent_record.object_id)
                            .map(|object| {
                                self.committed_repair_content_is_active(object, &intent_record)
                            })
                            .transpose()?
                            .unwrap_or(false);
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
                match self.coordinator.restore_intent(intent) {
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
                            self.restore_backend_receipts_for_intent_from_hydration_scan(
                                &intent_record,
                                receipts,
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
                )?;
                if matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
                    recovered_commit_promotion_records.push(intent_record.clone());
                    if self
                        .metadata
                        .object(&intent_record.object_id)
                        .map(|object| {
                            self.committed_repair_content_is_active(object, &intent_record)
                        })
                        .transpose()?
                        .unwrap_or(false)
                    {}
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
        if reconstruct_commit_promotions {
            for intent_record in &recovered_commit_promotion_records {
                if should_cancel() {
                    return Ok(false);
                }
                self.queue_recovered_commit_promotions(intent_record)?;
            }
        }

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
                && !matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                )
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
            // A committed write's missing replicas are needs in the repair
            // index; only an unfinished write keeps its transfer state.
            let needs_pending_write = locally_authoritative_intent
                && matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                );
            if needs_pending_write && let Some(content_hash) = intent_record.content_hash.clone() {
                // The size the write's row recorded when it opened, or that of
                // matching content this node holds.
                let size_hint = self.open_write_content_size(&intent_record, &content_hash)?;
                if missing_visible_object && commit_ready && size_hint.is_none() {
                    continue;
                }
                if missing_visible_object && commit_ready {
                    let Some(object) = authority_object else {
                        continue;
                    };
                    // Only a commit-ready create enters the namespace commit
                    // view. Pre-W transfer reads its original durable descriptor
                    // through the write-only lookup without exposing an object.
                    self.add_transient_object_metadata(object)?;
                }
                if let Some(pending) = self.writes.pending_writes.get(&intent_record.intent_id) {
                    if pending.object_id != intent_record.object_id
                        || pending.version != intent_record.target_version
                        || pending.content_hash != content_hash
                    {
                        return Err(DfsError::Metadata {
                            message: "hydrated intent differs from its live pending write".into(),
                        });
                    }
                } else {
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
                }
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
        // Request-owned prepares were completed before the object scan. Resume
        // pending operations in ordinary maintenance, after this consistent-read
        // snapshot ends, so its existing host can release admission for remote work.
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
        // Placement uses RepairReplica, whose successful acknowledgement
        // already commits the target binding and metadata. These intents never
        // stage a write awaiting a separate CommitVersion notification.
        if crate::placement::placement_reconciliation_job_id_from_intent_id(
            &intent_record.intent_id,
        )
        .is_some()
        {
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
        // Recovery has one owner. A clustered object's current metadata
        // primary adopts its promotions; the historical write coordinator must
        // not independently enqueue the same remote work after every restart.
        // Standalone objects retain their original coordinator.
        let authority_shard = self.select_metadata_shard_for_object(&object)?;
        let recovery_owner = authority_shard
            .as_ref()
            .map(|shard| &shard.primary_node_id)
            .unwrap_or(&intent_record.coordinator_node_id);
        if recovery_owner != &self.node_id {
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
        crate::background_observation::hydration_items("commit_promotions", targets.len());
        let object_metadata = Some(object);
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
        tracing::trace!(
            target: "sinomo.dfs.perf",
            node = %self.node_id,
            phase = "before-filesystems",
            "resident metadata lazy hydration"
        );
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
        tracing::trace!(
            target: "sinomo.dfs.perf",
            node = %self.node_id,
            phase = "filesystems-loaded",
            "resident metadata lazy hydration"
        );
        self.restore_durable_intent_counter_floor()?;
        tracing::trace!(
            target: "sinomo.dfs.perf",
            node = %self.node_id,
            phase = "intent-floor-loaded",
            "resident metadata lazy hydration"
        );
        // Lazy services still process mutations. Restore the durable guards for
        // running cross-shard namespace jobs without paying for a full object,
        // version, intent, and receipt scan.
        self.rebuild_cross_shard_namespace_locks()?;
        tracing::trace!(
            target: "sinomo.dfs.perf",
            node = %self.node_id,
            phase = "namespace-locks-loaded",
            "resident metadata lazy hydration"
        );
        self.seed_local_metadata_journal_cursors_from_backend()?;
        tracing::trace!(
            target: "sinomo.dfs.perf",
            node = %self.node_id,
            phase = "journal-cursors-loaded",
            "resident metadata lazy hydration"
        );
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

    pub(crate) fn restore_recorded_content_receipts(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        let receipts = self.backend_receipts_for_version(
            &intent_record.object_id,
            &intent_record.target_version,
        )?;
        self.restore_backend_receipts_for_intent_from_hydration_scan(intent_record, &receipts)
    }


    fn restore_backend_receipts_for_intent_from_hydration_scan(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<(), DfsError> {
        for receipt in receipts {
            if let Some(expected_hash) = intent_record.content_hash.as_deref()
                && receipt.content_hash.as_deref() != Some(expected_hash)
            {
                continue;
            }
            self.restore_backend_durable_receipt(
                &intent_record.intent_id,
                receipt.replica_node_id.clone(),
                false,
            )?;
        }
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
            let payloads = crate::metadata_replication::committed_payloads_for_batch(
                self.metadata_backend
                    .as_ref()
                    .expect("replica backend")
                    .as_ref(),
                batch,
            )?;
            let affected_object_ids = payloads
                .iter()
                .flat_map(|payload| {
                    payload
                        .transaction
                        .objects
                        .iter()
                        .map(|object| object.object_id.clone())
                        .chain(
                            payload
                                .transaction
                                .versions
                                .iter()
                                .map(|version| version.object_id.clone()),
                        )
                        .chain(
                            payload
                                .metadata_effects
                                .iter()
                                .flat_map(|effect| effect.affected_object_ids()),
                        )
                })
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
        // Ingest can precede queued table materialization. Share the normal
        // exact durable-body resolver with data-plane and object projections.
        let payloads = crate::metadata_replication::committed_payloads_for_batch(
            self.metadata_backend
                .as_deref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "replicated commit has no durable backend".into(),
                })?,
            batch,
        )?;
        for payload in payloads {
            for effect in &payload.metadata_effects {
                if let DfsPreparedMetadataEffect::PutNamespaceJob { job } = effect {
                    self.apply_cross_shard_namespace_job_guard(job)?;
                }
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
    pub(crate) fn replica_namespace_guard_projection(
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
            let page = backend.replica_projection_namespace_jobs_page(
                shard_id,
                cursor.as_ref(),
                DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
            )?;
            for job in page.jobs {
                if job.status != DfsNamespaceJobStatus::Running {
                    continue;
                }
                let Some(paths) = crate::service_state::cross_shard_namespace_job_guard_paths(&job)
                else {
                    continue;
                };
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
    pub(crate) fn replica_object_projections_to_retire(
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

    pub(crate) fn retire_replica_object_projections(
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

    pub(crate) fn refresh_recorded_prepared_rows(
        &mut self,
        payload: &DfsPreparedOperationPayload,
    ) -> Result<(), DfsError> {
        let mut object_ids = payload
            .transaction
            .objects
            .iter()
            .map(|object| object.object_id.clone())
            .chain(
                payload
                    .transaction
                    .versions
                    .iter()
                    .map(|version| version.object_id.clone()),
            )
            .collect::<BTreeSet<_>>();
        for effect in &payload.metadata_effects {
            match effect {
                DfsPreparedMetadataEffect::PutObject { object }
                | DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction {
                    tombstone: object,
                    ..
                } => {
                    object_ids.insert(object.object_id.clone());
                }
                DfsPreparedMetadataEffect::PutVersion { version } => {
                    object_ids.insert(version.object_id.clone());
                }
                DfsPreparedMetadataEffect::ReconcileCrossShardPinTransfer { versions, .. } => {
                    object_ids.extend(versions.iter().map(|version| version.object_id.clone()));
                }
                _ => {}
            }
        }
        self.metadata_backend
            .as_mut()
            .expect("projection backend")
            .begin_consistent_read_snapshot()?;
        let result = (|| {
            // Replaying a historical digest never restores its old visible head.
            // Consult the actual current committed generation once instead.
            for object_id in object_ids {
                let object = self
                    .metadata_backend
                    .as_ref()
                    .expect("projection backend")
                    .get_object(&object_id)?;
                if let Some(object) = object {
                    self.apply_metadata_object_to_service_state(object)?;
                } else {
                    self.metadata.remove_object(&object_id);
                    self.retire_replica_object_projections(vec![(
                        payload.file_system_id.clone(),
                        object_id,
                    )]);
                }
            }
            self.clear_metadata_lookup_cache();
            Ok(())
        })();
        self.metadata_backend
            .as_mut()
            .expect("projection backend")
            .end_consistent_read_snapshot();
        result
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
                catalog_generation: 0,
                retention_policies: Default::default(),
                module_metadata: Default::default(),
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

#[cfg(test)]
mod cancelled_catalog_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn cancelled_native_hydration_retains_the_complete_filesystem_catalog() {
        let (runtime, initial, _) = crate::test_support::current_native_metadata_service(
            "native-cancelled-hydration-catalog",
            1,
            "/services",
        )
        .unwrap();
        drop(initial);
        for cancel_at in 3..=8 {
            let mut service = runtime.metadata_primary_service("/services").unwrap();
            let before = service.metadata.filesystems.clone();
            let actual = service
                .metadata_backend()
                .unwrap()
                .get_file_system(&sifs_file_system_id())
                .unwrap()
                .unwrap();
            assert_eq!(before.get(&actual.file_system_id), Some(&actual));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let checked = AtomicUsize::new(0);
                let completed = service
                    .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
                        &|| checked.fetch_add(1, Ordering::SeqCst) + 1 >= cancel_at,
                    )
                    .unwrap();
                assert!(
                    !completed,
                    "the requested native cancellation point must be exercised"
                );

                assert_eq!(
                    service.metadata.filesystems, before,
                    "cancellation at native checkpoint {cancel_at} must hand back a complete catalog"
                );
                assert_eq!(
                    service
                        .metadata_backend()
                        .unwrap()
                        .get_file_system(&actual.file_system_id)
                        .unwrap()
                        .as_ref(),
                    Some(&actual)
                );
                let reached = checked.load(Ordering::SeqCst);
                if reached == cancel_at {
                    break;
                }
                // The recovery variant yields, after its first check and
                // before it reads anything, while a resident writer holds its
                // snapshot. An attempt that stopped anywhere else stopped at
                // a checkpoint nobody asked for.
                assert_eq!(
                    reached, 1,
                    "hydration stopped at checkpoint {reached}: neither the writer yield nor the requested checkpoint {cancel_at}"
                );
                assert!(
                    std::time::Instant::now() < deadline,
                    "a resident writer held the snapshot until hydration could not reach checkpoint {cancel_at}"
                );
                std::thread::yield_now();
            }
            drop(service);
        }
        runtime.close().unwrap();
    }
}
