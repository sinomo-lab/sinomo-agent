//! Cross-shard namespace saga helpers for the POSIX facade.
//!
//! This module owns coordinated SIFS metadata-shard moves: cross-primary
//! rename, hardlink adoption, resumable job driving, and destination-side
//! adoption. Local namespace operations and generic namespace-job storage stay
//! in the parent `posix` module.

use super::{
    POSIX_LIST_DIR_COLLECT_PAGE_SIZE, POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE,
    POSIX_READ_FRESHNESS_SYNC_TIMEOUT, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
    POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE, namespace_job_id, now_unix_nanos,
};
use crate::metadata_pages::{try_walk_metadata_pages, walk_metadata_pages};
use crate::namespace::{
    initial_version, namespace_transaction_id_for_batch,
    namespace_transaction_id_for_versioned_path, next_version,
};
use crate::posix_model::{DfsPosixError, Result};
use crate::posix_path::{
    normalize_posix_path, normalize_posix_path_with_trailing_directory,
    posix_path_is_at_or_under_prefix, subtree_prefix,
};
use crate::{
    DfsDirectoryPageCursor, DfsError, DfsFileSystemId, DfsMessageTransport, DfsMetadataShardId,
    DfsMetadataVersionPinRecord, DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobOrigin,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsNamespaceObjectPrecondition,
    DfsNamespaceObjectVersionExpectation, DfsNamespaceOperation,
    DfsNamespacePathOccupantExpectation, DfsNamespaceTransaction, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectVersionRecord, DfsPosixCoordinationResult, DfsReplicaReceiptRecord,
    DfsWriteIntentReceipt, DfsWriteIntentReceiptRetentionState, DfsWriteIntentState,
    DfsWriteIntentStatus, NetworkedDfsService, object_version_record, sifs_file_system_id,
};
use sinomo_core::node::NodeId;

mod closed_storage_parks;
mod destination_reservation;
#[cfg(any(test, feature = "test-support"))]
pub use destination_reservation::DfsDirectoryMoveScanRefusal;
#[cfg(test)]
pub(crate) use destination_reservation::fail_directory_move_scan_of;
mod parent_counts;
#[cfg(test)]
pub(crate) use parent_counts::{
    ParentCountTestPoint, hold_parent_apply_commit, install_parent_count_test_hook,
    parent_apply_commit_is_held,
};

#[derive(Clone, Debug)]
enum CrossShardRenameKind {
    Rename,
    UnlinkPromotion {
        expected_alias: DfsNamespaceObjectPrecondition,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HardlinkAliasMutation {
    Unlink,
    Rename,
}

impl HardlinkAliasMutation {
    fn label(self) -> &'static str {
        match self {
            Self::Unlink => "unlink",
            Self::Rename => "rename",
        }
    }
    fn count_delta(self) -> i64 {
        match self {
            Self::Unlink => -1,
            Self::Rename => 0,
        }
    }
}

fn source_eviction_operation_id(
    moved: &DfsObjectMetadata,
    old_path: &str,
    source_parent_delta: i64,
) -> Result<String> {
    // Completion retains this immutable move even after its physical source
    // row is retired. Source CAS and pin predicates remain sealed in the body.
    let identity =
        serde_json::to_vec(&(moved, old_path, source_parent_delta)).map_err(|error| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: error.to_string(),
            })
        })?;
    Ok(format!(
        "source-eviction:{}",
        blake3::hash(&identity).to_hex()
    ))
}

fn hardlink_target_fence(
    source_job_id: &str,
    source_path: &str,
    expected_target: &DfsNamespaceObjectPrecondition,
    committed_target: &DfsObjectMetadata,
    origin: Option<DfsNamespaceJobOrigin>,
    now: time::OffsetDateTime,
) -> DfsNamespaceJobRecord {
    DfsNamespaceJobRecord {
        job_id: namespace_job_id(
            "hardlink-target-fence",
            &committed_target.file_system_id,
            &format!("{source_job_id}:{source_path}->{}", committed_target.path),
            &expected_target.object_id,
            &expected_target.version,
        ),
        file_system_id: committed_target.file_system_id.clone(),
        origin,
        kind: DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
        status: DfsNamespaceJobStatus::Running,
        root_path: committed_target.path.clone(),
        target_path: None,
        pending_frames: vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path: source_path.into(),
            expected_target: expected_target.clone(),
            committed_target: committed_target.clone(),
        }],
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    }
}

#[cfg(test)]
type HardlinkAliasAfterCommitTestHook = Box<dyn FnOnce() -> Result<()> + Send>;

#[cfg(test)]
static HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK: std::sync::Mutex<
    Option<(String, HardlinkAliasAfterCommitTestHook)>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) struct HardlinkAliasAfterCommitTestHookGuard;

#[cfg(test)]
impl Drop for HardlinkAliasAfterCommitTestHookGuard {
    fn drop(&mut self) {
        HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }
}

#[cfg(test)]
pub(crate) fn install_hardlink_alias_after_commit_test_hook(
    source_path: impl Into<String>,
    hook: impl FnOnce() -> Result<()> + Send + 'static,
) -> HardlinkAliasAfterCommitTestHookGuard {
    let previous = HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .replace((source_path.into(), Box::new(hook)));
    assert!(
        previous.is_none(),
        "hardlink-alias after-commit test hook is already installed"
    );
    HardlinkAliasAfterCommitTestHookGuard
}

#[cfg(test)]
fn run_hardlink_alias_after_commit_test_hook(source_path: &str) -> Result<()> {
    let hook = {
        let mut slot = HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot
            .as_ref()
            .is_some_and(|(expected_path, _)| expected_path == source_path)
        {
            slot.take().map(|(_, hook)| hook)
        } else {
            None
        }
    };
    match hook {
        Some(hook) => hook(),
        None => Ok(()),
    }
}

#[cfg(test)]
type HardlinkCountAfterCommitTestHook = Box<dyn FnOnce() -> Result<()> + Send>;

#[cfg(test)]
static HARDLINK_COUNT_AFTER_COMMIT_TEST_HOOK: std::sync::Mutex<
    Option<(String, HardlinkCountAfterCommitTestHook)>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) struct HardlinkCountAfterCommitTestHookGuard;

#[cfg(test)]
impl Drop for HardlinkCountAfterCommitTestHookGuard {
    fn drop(&mut self) {
        HARDLINK_COUNT_AFTER_COMMIT_TEST_HOOK
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }
}

#[cfg(test)]
pub(crate) fn install_hardlink_count_after_commit_test_hook(
    source_path: impl Into<String>,
    hook: impl FnOnce() -> Result<()> + Send + 'static,
) -> HardlinkCountAfterCommitTestHookGuard {
    let previous = HARDLINK_COUNT_AFTER_COMMIT_TEST_HOOK
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .replace((source_path.into(), Box::new(hook)));
    assert!(
        previous.is_none(),
        "hardlink-count after-commit test hook is already installed"
    );
    HardlinkCountAfterCommitTestHookGuard
}

#[cfg(test)]
fn run_hardlink_count_after_commit_test_hook(source_path: &str) -> Result<()> {
    let hook = {
        let mut slot = HARDLINK_COUNT_AFTER_COMMIT_TEST_HOOK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot
            .as_ref()
            .is_some_and(|(expected_path, _)| expected_path == source_path)
        {
            slot.take().map(|(_, hook)| hook)
        } else {
            None
        }
    };
    match hook {
        Some(hook) => hook(),
        None => Ok(()),
    }
}

pub(crate) fn cross_shard_object_version_record(
    object: &DfsObjectMetadata,
) -> DfsObjectVersionRecord {
    let mut version = object_version_record(object, object.content_size_bytes);
    // Cross-shard adoption can reconstruct this record from a durable job
    // frame after a crash. Bind its timestamp to the frame generation instead
    // of wall-clock retry time so the immutable record is byte-for-byte
    // idempotent on every destination attempt.
    version.created_at =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(object.changed_at_unix_nanos))
            .expect("u64 nanoseconds since the Unix epoch fit OffsetDateTime");
    version
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(super) fn hidden_alias_tombstone_path_on_destination_shard(
        &mut self,
        destination_path: &str,
        alias_object_id: &DfsObjectId,
    ) -> Result<String> {
        let Some(shards) = self.refresh_sifs_metadata_shards()? else {
            return Ok(crate::posix_path::hidden_alias_tombstone_path(
                alias_object_id,
            ));
        };
        let destination = self
            .select_sifs_metadata_shard_for_path_from_snapshot(destination_path, &shards)?
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "destination {destination_path} has no metadata shard for hardlink promotion"
                    ),
                })
            })?;
        for nonce in 0..64u8 {
            let candidate = format!(
                "{destination_path}.sinomo-hardlink-tombstone-{}-{nonce}",
                alias_object_id.as_str()
            );
            if self
                .select_sifs_metadata_shard_for_path_from_snapshot(&candidate, &shards)?
                .is_some_and(|shard| shard.shard_id == destination.shard_id)
            {
                return Ok(candidate);
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Metadata {
            message: format!(
                "could not place hidden alias tombstone for {alias_object_id} on destination shard {}",
                destination.shard_id
            ),
        }))
    }

    /// Coordinates a rename whose destination lives in a metadata shard owned
    /// by another primary. The move is a durable two-step saga: the
    /// destination primary atomically adopts the object record (with its
    /// version and receipt history) at the new path, then this node — the
    /// source-shard authority — atomically tombstones the old path and evicts
    /// the moved record from the source shard. A persisted namespace job and
    /// an in-flight path lock make the window crash-recoverable and serialize
    /// concurrent mutations; after the adoption commits, recovery is forward
    /// only, so the move either fully applies or cleanly rejects.
    pub(crate) fn coordinate_cross_shard_rename(
        &mut self,
        from: &str,
        to: &str,
        destination_primary: NodeId,
    ) -> Result<()> {
        self.coordinate_cross_shard_rename_kind(
            from,
            to,
            destination_primary,
            CrossShardRenameKind::Rename,
        )
    }

    pub(crate) fn coordinate_cross_shard_unlink_promotion(
        &mut self,
        from: &str,
        to: &str,
        destination_primary: NodeId,
        expected_alias: DfsNamespaceObjectPrecondition,
    ) -> Result<()> {
        self.coordinate_cross_shard_rename_kind(
            from,
            to,
            destination_primary,
            CrossShardRenameKind::UnlinkPromotion { expected_alias },
        )
    }

    fn coordinate_cross_shard_rename_kind(
        &mut self,
        from: &str,
        to: &str,
        destination_primary: NodeId,
        kind: CrossShardRenameKind,
    ) -> Result<()> {
        let promotion_alias = match kind {
            CrossShardRenameKind::Rename => None,
            CrossShardRenameKind::UnlinkPromotion { expected_alias } => Some(expected_alias),
        };
        let (from, from_requires_directory) = normalize_posix_path_with_trailing_directory(from)?;
        let (to, to_requires_directory) = normalize_posix_path_with_trailing_directory(to)?;
        let from = self.resolve_posix_path_distributed(&from, false)?;
        if from_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&from)?;
        }
        let to = self.resolve_posix_path_distributed(&to, false)?;
        if from == "/" || to == "/" {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to}"),
            });
        }
        if from == to {
            return Ok(());
        }
        let source = self
            .lookup_sifs_path(&from)?
            .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: from });
        }
        if source.kind == DfsObjectKind::Directory && promotion_alias.is_some() {
            return Err(DfsPosixError::IsDirectory { path: from });
        }
        if source.kind == DfsObjectKind::Directory {
            // Directory renames migrate the whole subtree between shard
            // authorities through a paged, resumable saga.
            let child_prefix = subtree_prefix(&from);
            if to.starts_with(&child_prefix) {
                return Err(DfsPosixError::InvalidPath {
                    path: format!("{from} -> {to}"),
                });
            }
            let job_id = namespace_job_id(
                "rename-dir-cross-shard",
                &source.file_system_id,
                &format!("{from}->{to}"),
                &source.object_id,
                &source.version,
            );
            if let Some(job) = self.namespace_job(&job_id)? {
                return self.drive_cross_shard_namespace_job(job);
            }
            let file_system_id = sifs_file_system_id();
            self.ensure_path_not_in_cross_shard_flight(&file_system_id, &from)?;
            self.ensure_path_not_in_cross_shard_flight(&file_system_id, &to)?;
            return self.coordinate_cross_shard_directory_rename(&from, &to, source);
        }
        if to_requires_directory {
            return Err(DfsPosixError::NotDirectory { path: to });
        }
        let destination = self.lookup_sifs_path_direct_distributed_synced(&to)?;
        if let Some(expected_alias) = promotion_alias.as_ref() {
            if source.kind == DfsObjectKind::Hardlink
                || destination.as_ref().is_none_or(|alias| {
                    alias.kind != DfsObjectKind::Hardlink
                        || alias.object_id != expected_alias.object_id
                        || alias.version != expected_alias.version
                        || alias.link_target_object_id.as_ref() != Some(&source.object_id)
                })
            {
                return Err(DfsPosixError::Conflict {
                    path: to.clone(),
                    reason: "selected canonical-promotion alias changed before planning".into(),
                });
            }
        } else if let Some(destination) = destination
            .as_ref()
            .filter(|row| row.kind != DfsObjectKind::Tombstone)
        {
            let source_id = if source.kind == DfsObjectKind::Hardlink {
                source
                    .link_target_object_id
                    .as_ref()
                    .ok_or_else(|| DfsPosixError::InvalidPath { path: from.clone() })?
            } else {
                &source.object_id
            };
            let destination_id = if destination.kind == DfsObjectKind::Hardlink {
                destination
                    .link_target_object_id
                    .as_ref()
                    .ok_or_else(|| DfsPosixError::InvalidPath { path: to.clone() })?
            } else {
                &destination.object_id
            };
            if source_id == destination_id {
                return Ok(());
            }
        }
        let job_id = namespace_job_id(
            if promotion_alias.is_some() {
                "unlink-promotion-cross-shard"
            } else {
                "rename-cross-shard"
            },
            &source.file_system_id,
            &format!("{from}->{to}"),
            &source.object_id,
            &source.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return self.drive_cross_shard_namespace_job(job);
        }
        let file_system_id = sifs_file_system_id();
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &from)?;
        // Guards come only from shards this node replicates, which replication
        // keeps current; the destination primary enforces its own.
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &to)?;
        // Reject up front while writes are still settling so the caller sees
        // a clean retriable error with no job created.
        self.ensure_no_open_write_state_for_object(&source, &from)?;
        // Likewise while a moved alias's target shard is converting.
        self.ensure_hardlink_target_storage_active([&source])?;

        let mut moved = source.clone();
        if source.kind != DfsObjectKind::Hardlink {
            moved.namespace_link_count = Some(super::metadata::updated_namespace_link_count(
                &source,
                if promotion_alias.is_some() { -1 } else { 0 },
            )?);
        }
        moved.path = to.clone();
        moved.changed_at_unix_nanos =
            now_unix_nanos().max(source.changed_at_unix_nanos.saturating_add(1));
        moved.version = next_version(&source.version);
        let job = match self.namespace_job(&job_id)? {
            Some(job) => job,
            None => {
                let now = time::OffsetDateTime::now_utc();
                DfsNamespaceJobRecord {
                    job_id,
                    file_system_id: source.file_system_id.clone(),
                    origin: self
                        .namespace_execution
                        .request()
                        .map(DfsNamespaceJobOrigin::from_request),
                    kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: from.clone(),
                    target_path: Some(to.clone()),
                    pending_frames: vec![
                        DfsNamespaceJobFrame::CrossShardComplete {
                            moved_object: moved.clone(),
                            old_path: from.clone(),
                        },
                        DfsNamespaceJobFrame::CrossShardAdopt {
                            moved_object: moved,
                            old_path: from.clone(),
                            promotion_alias: promotion_alias.clone(),
                            destination_primary_node_id: destination_primary,
                        },
                    ],
                    processed_object_count: 0,
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                    failure_reason: None,
                }
            }
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    fn require_count_parent_coowned(&mut self, child_path: &str) -> Result<()> {
        let Some(parent) = crate::posix_path::parent_posix_path(child_path) else {
            return Ok(());
        };
        if self.control_plane.is_some() {
            let scopes = self.namespace_shards_for_exact_paths(
                &sifs_file_system_id(),
                [child_path, parent.as_str()],
            )?;
            if scopes.len() != 1 {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "foreign-parent directory counts require declared original-job participants before effects".into(),
                }));
            }
        }
        Ok(())
    }

    fn counted_parent_successor(
        &mut self,
        child_path: &str,
        delta: i64,
    ) -> Result<Option<(DfsObjectMetadata, DfsObjectMetadata)>> {
        let Some(parent_path) = crate::posix_path::parent_posix_path(child_path) else {
            return Ok(None);
        };
        self.require_count_parent_coowned(child_path)?;
        let before = self
            .lookup_sifs_path(&parent_path)?
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
            .ok_or_else(|| DfsPosixError::MissingParent {
                path: parent_path.clone(),
            })?;
        if before.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path: parent_path });
        }
        let mut successor = before.clone();
        successor.namespace_link_count = Some(super::metadata::updated_namespace_link_count(
            &before, delta,
        )?);
        let now = now_unix_nanos()
            .max(before.modified_at_unix_nanos.saturating_add(1))
            .max(before.changed_at_unix_nanos.saturating_add(1));
        successor.modified_at_unix_nanos = now;
        successor.changed_at_unix_nanos = now;
        successor.version = next_version(&before.version);
        Ok(Some((before, successor)))
    }

    fn recorded_adoption_parent_successor(
        &self,
        transaction_id: &str,
        destination: &DfsObjectMetadata,
        parent_path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let backend = self.metadata_backend.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: "counted adoption has no actual backend".into(),
            })
        })?;
        let mut ids = Vec::new();
        if let Some(request) = self.namespace_execution.request() {
            ids.push(format!(
                "namespace-request:{}:{transaction_id}",
                request.operation_id
            ));
        }
        ids.push(format!("namespace:{transaction_id}"));
        for id in ids {
            if let Some(record) = backend.prepared_operation_record(&id)? {
                record.validate()?;
                if record.payload.transaction.transaction_id != transaction_id
                    || !record
                        .payload
                        .transaction
                        .objects
                        .iter()
                        .any(|object| object == destination)
                {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: "counted adoption retry changed its original destination".into(),
                    }));
                }
                return record
                    .payload
                    .transaction
                    .objects
                    .iter()
                    .find(|object| {
                        object.path == parent_path && object.kind == DfsObjectKind::Directory
                    })
                    .cloned()
                    .map(Some)
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message: "original adoption omitted its exact counted parent".into(),
                        })
                    });
            }
        }
        Ok(None)
    }

    /// Coordinates a directory rename whose destination lives in a metadata
    /// shard owned by another primary: the root record moves first (the
    /// destination primary validates and adopts it atomically, like a local
    /// rename over an empty destination), then the subtree is walked in
    /// paged, resumable frames, adopting every child at its target path and
    /// evicting the source record. The source root path is subtree-locked
    /// for the whole saga, so no write can fork a half-moved object.
    fn coordinate_cross_shard_directory_rename(
        &mut self,
        from: &str,
        to: &str,
        source: DfsObjectMetadata,
    ) -> Result<()> {
        // Existing in-flight work remains the original operation, including
        // older job IDs. A settled abort, however, must not occupy the identity
        // of a genuinely distinct later caller request on the unchanged source.
        if self.resume_running_cross_shard_namespace_job_for_paths(
            &source.file_system_id,
            from,
            to,
            &[DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries],
        )? {
            return Ok(());
        }
        let operation_path = match self.namespace_execution.request() {
            Some(request) => format!(
                "{from}->{to}\0{}\0{}",
                request.requester_node_id, request.request_id
            ),
            None => format!("{from}->{to}"),
        };
        let job_id = namespace_job_id(
            "rename-dir-cross-shard",
            &source.file_system_id,
            &operation_path,
            &source.object_id,
            &source.version,
        );
        let job = match self.namespace_job(&job_id)? {
            Some(job) => job,
            None => {
                super::metadata::updated_namespace_link_count(&source, 0)?;
                let destination = self.lookup_sifs_path_direct_distributed_synced(to)?;
                let expected_destination = match destination
                    .as_ref()
                    .filter(|row| row.kind != DfsObjectKind::Tombstone)
                {
                    Some(row) if row.kind == DfsObjectKind::Directory => {
                        crate::DfsNamespaceDestinationPrecondition::Exact(
                            DfsNamespaceObjectPrecondition {
                                object_id: row.object_id.clone(),
                                version: row.version.clone(),
                            },
                        )
                    }
                    Some(_) => return Err(DfsPosixError::NotDirectory { path: to.into() }),
                    None => crate::DfsNamespaceDestinationPrecondition::Absent,
                };
                let same_parent = crate::posix_path::parent_posix_path(from)
                    == crate::posix_path::parent_posix_path(to);
                let replaces = matches!(
                    expected_destination,
                    crate::DfsNamespaceDestinationPrecondition::Exact(_)
                );
                let now = time::OffsetDateTime::now_utc();
                let mut count_plan = crate::metadata_records::DfsDirectoryMoveCounts {
                    parent_participants: None,
                    expected_destination,
                    source_parent_delta: if same_parent && !replaces { 0 } else { -1 },
                    target_parent_delta: if same_parent || replaces { 0 } else { 1 },
                };
                count_plan.parent_participants =
                    self.plan_foreign_parent_counts(&job_id, from, to, &source, &count_plan, now)?;
                DfsNamespaceJobRecord {
                    job_id,
                    file_system_id: source.file_system_id.clone(),
                    origin: self
                        .namespace_execution
                        .request()
                        .map(DfsNamespaceJobOrigin::from_request),
                    kind: DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: from.to_string(),
                    target_path: Some(to.to_string()),
                    pending_frames: vec![
                        DfsNamespaceJobFrame::CrossShardDirFinish {
                            source_root: from.to_string(),
                            count_plan: Some(count_plan),
                        },
                        DfsNamespaceJobFrame::CrossShardDirScan {
                            source_path: from.to_string(),
                            target_path: to.to_string(),
                            cursor: None,
                        },
                        DfsNamespaceJobFrame::CrossShardDirStart {
                            source_object_id: source.object_id.clone(),
                            source_root: from.to_string(),
                            target_root: to.to_string(),
                        },
                    ],
                    processed_object_count: 0,
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                    failure_reason: None,
                }
            }
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    /// Moves the directory-rename root record across primaries: the
    /// destination primary adopts it, then the old root path is tombstoned
    /// (hiding the not-yet-moved children under it) and the record evicted
    /// from the source shard. Idempotent against resumes.
    fn run_cross_shard_dir_start(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        source_object_id: &DfsObjectId,
        source_root: &str,
        target_root: &str,
    ) -> Result<()> {
        let current =
            self.object_by_id(source_object_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: source_root.to_owned(),
                })?;
        if current.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_root.to_owned(),
            });
        }
        let current_path = normalize_posix_path(&current.path)?;
        let mut moved = current.clone();
        if current_path == source_root {
            self.ensure_no_open_write_state_for_object(&current, source_root)?;
            moved.path = target_root.to_owned();
            moved.changed_at_unix_nanos =
                now_unix_nanos().max(current.changed_at_unix_nanos.saturating_add(1));
            moved.version = next_version(&current.version);
        } else if current_path != target_root {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{current_path} -> {target_root}"),
            });
        }
        // The ordinary driver persists these exact existing frames before its
        // next step. Source capture then binds a durable destination generation,
        // rather than recreating a new version whenever a directory resumes.
        self.schedule_cross_shard_move(job, moved, source_root)?;
        Ok(())
    }

    /// Pages one directory of a cross-primary subtree move: every child file
    /// is adopted at its target path and evicted from the source shard;
    /// child directories are adopted, evicted, and pushed as their own scan
    /// frames (depth first). Already-moved children are skipped, so resumes
    /// and re-sent adoptions stay idempotent.
    fn run_cross_shard_dir_scan(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        source_path: &str,
        target_path: &str,
        cursor: Option<&DfsDirectoryPageCursor>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-support"))]
        destination_reservation::directory_move_scan_test_failure(source_path)?;
        let source_prefix = subtree_prefix(source_path);
        let target_prefix = subtree_prefix(target_path);
        let recorded_job = self.namespace_job(&job.job_id)?.ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: "source directory scan has no original durable namespace job".into(),
            })
        })?;
        // Enumeration selects identities, not the original head of a nested
        // authority. Record only the owning shard's selected object below.
        let speculative = self.namespace_execution.begin_independent_planning();
        let page = if let Some(source) = self.routed_namespace_shard_for_path(source_path)? {
            match self.query_metadata_authority_impl(
                &source,
                crate::DfsMetadataSelection::DirectoryPageForNamespaceJob {
                    job: Box::new(recorded_job.clone()),
                    path: source_path.to_string(),
                    after: cursor.cloned(),
                    limit: POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE,
                },
            )? {
                crate::DfsMetadataQueryResult::DirectoryPage { entries, after } => {
                    crate::DfsDirectoryPage {
                        objects: entries.into_iter().map(|(object, _)| object).collect(),
                        next_cursor: after,
                    }
                }
                _ => {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: "source directory authority returned another query kind".into(),
                    }));
                }
            }
        } else {
            self.child_objects_for_sifs_dir_page(
                source_path,
                cursor,
                POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE,
            )?
        };
        drop(speculative);
        let mut resolved = Vec::new();
        for object in page.objects {
            let old_path = normalize_posix_path(&object.path)?;
            let Some(suffix) = old_path.strip_prefix(&source_prefix) else {
                continue;
            };
            let new_path = format!("{target_prefix}{suffix}");
            let current = if let Some(source) = self.routed_namespace_shard_for_path(&old_path)? {
                self.namespace_job_source_object(
                    &source,
                    &recorded_job,
                    &object.object_id,
                    &old_path,
                )?
            } else {
                self.object_by_id(&object.object_id)?
            };
            let Some(current) = current else {
                continue;
            };
            if current.object_id != object.object_id {
                return Err(DfsPosixError::InvalidPath { path: old_path });
            }
            if current.kind == DfsObjectKind::Tombstone {
                continue;
            }
            let current_path = normalize_posix_path(&current.path)?;
            let mut moved = current.clone();
            if current_path == old_path {
                self.ensure_no_open_write_state_for_object(&current, &old_path)?;
                moved.path = new_path.clone();
                moved.changed_at_unix_nanos =
                    now_unix_nanos().max(current.changed_at_unix_nanos.saturating_add(1));
                moved.version = next_version(&current.version);
            } else if current_path != new_path {
                continue;
            }
            resolved.push((moved, old_path, new_path));
        }
        // Before any frame of this page: while a carried alias's target shard
        // is not Active, the job keeps this scan frame and stays Running.
        self.park_until_hardlink_target_storage_active(
            job,
            &DfsNamespaceJobFrame::CrossShardDirScan {
                source_path: source_path.to_owned(),
                target_path: target_path.to_owned(),
                cursor: cursor.cloned(),
            },
            resolved.iter().map(|(moved, _, _)| moved),
        )?;
        if let Some(next_cursor) = page.next_cursor {
            job.pending_frames
                .push(DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path: source_path.to_owned(),
                    target_path: target_path.to_owned(),
                    cursor: Some(next_cursor),
                });
        }
        // LIFO order: adopt, complete its exact source retirement, then scan a
        // moved child directory. Every destination/version is durable job DATA
        // before any source Actor or target receiver is invoked.
        for (moved, old_path, new_path) in resolved.into_iter().rev() {
            if moved.kind == DfsObjectKind::Directory {
                job.pending_frames
                    .push(DfsNamespaceJobFrame::CrossShardDirScan {
                        source_path: old_path.clone(),
                        target_path: new_path,
                        cursor: None,
                    });
            }
            self.schedule_cross_shard_move(job, moved, &old_path)?;
        }
        Ok(())
    }

    fn namespace_job_source_object(
        &mut self,
        source: &crate::DfsMetadataShardConfig,
        job: &DfsNamespaceJobRecord,
        object_id: &DfsObjectId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        // Retain the previous source-lookup budget and the enclosing operation
        // deadline. A rejected source read never becomes a replica fallback.
        let previous = self.transport.operation_deadline();
        let requested = std::time::Instant::now() + POSIX_READ_FRESHNESS_SYNC_TIMEOUT;
        self.transport
            .set_operation_deadline(Some(previous.map_or(requested, |old| old.min(requested))));
        let response = self.query_metadata_authority_impl(
            source,
            crate::DfsMetadataSelection::CurrentObjectForNamespaceJob {
                job: Box::new(job.clone()),
                source_path: path.to_owned(),
                object_id: object_id.clone(),
            },
        );
        self.transport.set_operation_deadline(previous);
        let crate::DfsMetadataQueryResult::CurrentObject { object } = response? else {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: "namespace source authority returned another query kind".into(),
            }));
        };
        let object = object.map(|object| *object);
        if object.as_ref().is_some_and(|object| {
            object.object_id != *object_id
                || object.file_system_id != source.file_system_id
                || object.path != path
        }) {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: "namespace source authority changed the selected identity or path".into(),
            }));
        }
        Ok(self.record_selected_synced_path(path, object))
    }

    fn schedule_cross_shard_move(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        moved: DfsObjectMetadata,
        old_path: &str,
    ) -> Result<()> {
        let destination = self
            .namespace_shards_for_exact_paths(
                &moved.file_system_id,
                std::iter::once(moved.path.as_str()),
            )?
            .into_iter()
            .next()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "directory move has no actual destination assignment".into(),
                })
            })?;
        job.pending_frames
            .push(DfsNamespaceJobFrame::CrossShardComplete {
                moved_object: moved.clone(),
                old_path: old_path.to_owned(),
            });
        job.pending_frames
            .push(DfsNamespaceJobFrame::CrossShardAdopt {
                promotion_alias: None,
                moved_object: moved,
                old_path: old_path.to_owned(),
                destination_primary_node_id: destination.primary_node_id,
            });
        Ok(())
    }

    /// Cross-shard moves must not race in-flight distributed write state: a
    /// write or repair that settles after the record changes shard authority
    /// would journal its metadata into a shard this node no longer owns.
    /// Surfaces `Busy` (retriable) so coordinators reject up front and parked
    /// sagas wait for the write to drain.
    fn ensure_no_open_write_state_for_object(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
    ) -> Result<()> {
        let busy = || {
            Err(DfsPosixError::Dfs(DfsError::Busy {
                path: path.to_string(),
            }))
        };
        for (intent_id, pending) in &self.writes.pending_writes {
            if pending.object_id != object.object_id {
                continue;
            }
            // Committed repair executors retain their original data version
            // after metadata-only successors. Prove publication from the exact
            // durable owner and version, not the coordinator's W-ready state.
            let committed_visible = if let (Some(intent), Some(backend)) = (
                self.coordinator.intent(intent_id),
                self.metadata_backend.as_ref(),
            ) && matches!(intent.state, DfsWriteIntentState::Committed)
                && intent.file_system_id == object.file_system_id
                && intent.object_id == pending.object_id
                && intent.target_version == pending.version
            {
                let terminal = backend.get_intent(&object.object_id, intent_id)?;
                let version = backend.get_version(&object.object_id, &pending.version)?;
                terminal.as_ref().is_some_and(|record| {
                    record.intent_id == *intent_id
                        && record.status == DfsWriteIntentStatus::Committed
                        && crate::intent::distributed_intent_matches_record(intent, record)
                        && record.content_hash.as_deref() == Some(pending.content_hash.as_str())
                }) && version.as_ref().is_some_and(|version| {
                    version.file_system_id == object.file_system_id
                        && version.object_id == pending.object_id
                        && version.version == pending.version
                        && version.content_hash.as_deref() == Some(pending.content_hash.as_str())
                        && version.size_bytes == Some(pending.size_bytes)
                        && (!intent.publication.is_current() || version.visible)
                })
            } else {
                false
            };
            if !committed_visible {
                return busy();
            }
        }
        if self
            .coordinator
            .has_open_intent_for_object(&object.object_id)
        {
            return busy();
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut has_blocking_intent = false;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_page(
                    &object.object_id,
                    cursor,
                    POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
                )?;
                if page.intents.iter().any(|intent| {
                    matches!(intent.status, DfsWriteIntentStatus::Staging)
                        || (matches!(intent.status, DfsWriteIntentStatus::ReceiptsPending)
                            && intent.target_version != object.version)
                }) {
                    has_blocking_intent = true;
                    return Ok(None);
                }
                Ok(page.next_cursor)
            })?;
            if has_blocking_intent {
                return busy();
            }
        }
        Ok(())
    }

    /// A cross-shard move of a hardlink alias keeps its object id and never
    /// touches its canonical target, yet it moves the alias's count
    /// contribution to the target's shard from one shard to another. While
    /// that shard's storage is not Active (its record is Converting, or its
    /// copy refuses to open because its storage is closed), its contributions
    /// must stay where its conversion reads them. Surfaces `Busy` (retriable)
    /// before any effect: a coordinator rejects up front, and a parked
    /// directory saga keeps the page until the shard is Active again (see
    /// [`Self::park_until_hardlink_target_storage_active`]).
    fn ensure_hardlink_target_storage_active<'a>(
        &mut self,
        moved: impl IntoIterator<Item = &'a DfsObjectMetadata>,
    ) -> Result<()> {
        match self.hardlink_target_storage_wait(moved)? {
            Some((path, _)) => Err(DfsPosixError::Dfs(DfsError::Busy { path })),
            None => Ok(()),
        }
    }

    /// [`Self::ensure_hardlink_target_storage_active`] for a job step that
    /// stays Running while it waits: the refusal records the shards it waits
    /// on for this node, so their activation wakes its maintenance, and
    /// records and announces the park of `job` at its next frame `frame`
    /// (see `posix/job_events.rs`).
    fn park_until_hardlink_target_storage_active<'a>(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
        moved: impl IntoIterator<Item = &'a DfsObjectMetadata>,
    ) -> Result<()> {
        let Some((path, shards)) = self.hardlink_target_storage_wait(moved)? else {
            return Ok(());
        };
        Err(self.park_namespace_step_on_shard_storage(job, frame, shards, path))
    }

    /// The fence commit of `job` at its next frame `frame`, for the fence on
    /// `committed_target`: while the shard that holds that fence has a
    /// storage record naming a conversion, the step is refused with `Busy`
    /// and the job keeps its frame, as a directory saga waits for a carried
    /// alias's target shard. The refusal records the shard for this node, so
    /// its activation wakes this node's maintenance, and records and
    /// announces the park of `job`. The fence's shard settles the fence from
    /// the job's reported phase while it converts; the commit that runs once
    /// it is Active finds the fence Completed.
    fn park_until_hardlink_fence_storage_active(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
        committed_target: &DfsObjectMetadata,
    ) -> Result<()> {
        if self.control_plane.is_none() {
            return Ok(());
        }
        let catalog = self.metadata_shard_authorities(&committed_target.file_system_id)?;
        let owner = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &committed_target.file_system_id,
            &catalog,
            &committed_target.path,
        )
        .map_err(DfsError::from)?;
        let Some(owner) = owner else {
            return Ok(());
        };
        if !self.shard_storage_record_is_closed(&owner)? {
            return Ok(());
        }
        self.note_namespace_job_parked(job, frame, std::slice::from_ref(&owner));
        crate::maintenance::storage_waits::record_parked_on_shard_storage(
            &self.node_id,
            [(
                owner.file_system_id.clone(),
                crate::DfsMetadataShardId::new(owner.shard_id.clone()),
            )],
        );
        Err(DfsPosixError::Dfs(DfsError::Busy {
            path: committed_target.path.clone(),
        }))
    }

    /// The first moved alias whose target's shard is not Active, with the
    /// shards it waits on; `None` when every target's shard is Active.
    ///
    /// The target is resolved through the current shard catalog, and its
    /// owner's record is read from the retained configuration projection. A
    /// shard whose copy refuses because its storage is closed cannot answer
    /// that resolution: the search goes on past it, and a target found
    /// nowhere else is unknown, not dangling, so the alias waits on those
    /// shards. A failed resolution while any shard is closed waits on the
    /// closed shards. A dangling alias names no target and moves no
    /// contribution.
    fn hardlink_target_storage_wait<'a>(
        &mut self,
        moved: impl IntoIterator<Item = &'a DfsObjectMetadata>,
    ) -> Result<Option<(String, Vec<crate::DfsMetadataShardConfig>)>> {
        let aliases = moved
            .into_iter()
            .filter(|object| object.kind == DfsObjectKind::Hardlink)
            .collect::<Vec<_>>();
        let Some(first) = aliases.first() else {
            return Ok(None);
        };
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let catalog = self.metadata_shard_authorities(&first.file_system_id)?;
        for alias in aliases {
            let mut fan_out = crate::file_history_query::DfsAuthorityFanOut::default();
            let resolved = {
                // A storage check, not a mutation input: keep its target read
                // out of the caller's recorded read set.
                let _check = self.namespace_execution.begin_independent_planning();
                self.resolve_hardlink_target_distributed_noting_closed(
                    alias,
                    Some(&catalog),
                    &mut fan_out,
                )
            };
            let mut closed = fan_out.closed().to_vec();
            let target = match resolved {
                Ok(target) => target,
                Err(DfsPosixError::NotFound { .. } | DfsPosixError::InvalidPath { .. })
                    if closed.is_empty() =>
                {
                    continue;
                }
                Err(DfsPosixError::NotFound { .. } | DfsPosixError::InvalidPath { .. }) => {
                    return Ok(Some((alias.path.clone(), closed)));
                }
                Err(error) => {
                    for shard in &catalog {
                        if !closed.contains(shard) && self.shard_storage_record_is_closed(shard)? {
                            closed.push(shard.clone());
                        }
                    }
                    if closed.is_empty() {
                        return Err(error);
                    }
                    return Ok(Some((alias.path.clone(), closed)));
                }
            };
            let owner = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &alias.file_system_id,
                &catalog,
                &target.path,
            )
            .map_err(DfsError::from)?;
            if let Some(owner) = owner
                && (closed.contains(&owner) || self.shard_storage_record_is_closed(&owner)?)
            {
                return Ok(Some((alias.path.clone(), vec![owner])));
            }
        }
        Ok(None)
    }

    /// Whether `shard`'s storage is closed, by its storage record in the
    /// retained configuration projection: the record names a conversion in
    /// progress, or there is no record, so migration has yet to convert the
    /// shard's storage.
    pub(super) fn shard_storage_record_is_closed(
        &mut self,
        shard: &crate::DfsMetadataShardConfig,
    ) -> Result<bool> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        let record = crate::DfsControlPlaneStore::new(kv.as_mut())
            .get_metadata_shard_storage(&shard.file_system_id, &shard.shard_id)
            .map_err(DfsError::from)?;
        Ok(record.is_none_or(|record| !record.is_active()))
    }

    fn run_cross_shard_adopt_for_job(
        &mut self,
        frame_object: &DfsObjectMetadata,
        old_path: &str,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        let old_path = normalize_posix_path(old_path)?;
        let target_path = normalize_posix_path(&frame_object.path)?;
        let phase_key = job.map(|job| {
            Self::cross_shard_job_phase_operation_id(
                job,
                &format!(
                    "adopt-object-family\0{}\0{old_path}",
                    frame_object.object_id
                ),
            )
        });
        // The native target phase may already have retired the physical source.
        // Consume its exact correlated receipt before attempting a new source
        // read; the original frame still owns the transfer identity.
        if let Some(key) = phase_key.as_ref()
            && let Some(outcome) = self.completed_remote_namespace_phases.remove(key)
        {
            let transfer_id =
                crate::protocol::cross_shard_pin_transfer_id(frame_object, &old_path)?;
            return self.finish_completed_source_adoption_phase(
                frame_object,
                &old_path,
                job,
                &transfer_id,
                outcome,
            );
        }
        let prepared_source = phase_key
            .as_ref()
            .and_then(|key| self.prepared_remote_sources.get(key))
            .map(|prepared| prepared.family.source_object.clone());
        let mut current = if let Some(source) = prepared_source {
            // The resumed phase already owns this exact source capture. Never
            // re-read through the writer it holds or replace its original head.
            Some(source)
        } else {
            let speculative = self.namespace_execution.begin_independent_planning();
            let candidate = self.object_by_id(&frame_object.object_id)?;
            drop(speculative);
            let needs_source = candidate.as_ref().is_none_or(|object| {
                object.path == old_path || object.kind == DfsObjectKind::Tombstone
            });
            if needs_source
                && let Some(job) = job
                && let Some(source) = self.routed_namespace_shard_for_path(&old_path)?
            {
                let recorded = self.namespace_job(&job.job_id)?.ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "source adoption read has no full durable namespace job".into(),
                    })
                })?;
                self.namespace_job_source_object(
                    &source,
                    &recorded,
                    &frame_object.object_id,
                    &old_path,
                )?
            } else if needs_source && self.routed_namespace_primary_for_path(&old_path)?.is_some() {
                // An ancestor shard may retain the original genesis row of a
                // nested shard root. Select this phase's input from its actual
                // source authority before constructing the immutable prepare.
                let source = self.lookup_sifs_path_direct_distributed_synced(&old_path)?;
                if source
                    .as_ref()
                    .is_some_and(|object| object.object_id != frame_object.object_id)
                {
                    return Err(DfsPosixError::InvalidPath { path: old_path });
                }
                source
            } else {
                candidate
            }
        };
        if current
            .as_ref()
            .is_none_or(|current| current.kind == DfsObjectKind::Tombstone)
        {
            // A duplicate saga runner can retain the adoption frame after
            // another runner has already adopted and evicted the source-side
            // object. Read the destination from its authority before
            // classifying that exact replay as a missing source.
            current = match self.lookup_sifs_path_direct_distributed_synced(&target_path)? {
                Some(object) if object.object_id == frame_object.object_id => Some(object),
                _ => self.object_by_id(&frame_object.object_id)?,
            };
        }
        let Some(current) = current else {
            return Err(DfsPosixError::NotFound { path: old_path });
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: old_path });
        }
        let current_path = normalize_posix_path(&current.path)?;
        if current_path == target_path {
            // The target may have become authoritative just before a crash,
            // while the physical source family and pins were still retained.
            // Recover from the explicit old-path shard; the destination-side
            // reconciliation preserves target-only pins.
            return self.adopt_moved_object_on_destination_primary(
                &current,
                &old_path,
                None,
                Some(frame_object),
                job,
            );
        }
        if current_path != old_path {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{current_path} -> {target_path}"),
            });
        }
        self.ensure_no_open_write_state_for_object(&current, &old_path)?;
        // The durable job frame owns the entire destination body, including
        // timestamps and provenance. The independently captured current source
        // remains an exact source precondition; it must not rebuild or rebase
        // the destination that the actual Running job already reserved.
        self.adopt_moved_object_on_destination_primary(
            frame_object,
            &old_path,
            Some(&current),
            None,
            job,
        )
    }

    /// Routes the adoption of a moved record to the destination path's
    /// current primary (re-resolved from the control plane on every attempt),
    /// or adopts locally when a shard transfer made this node the authority.
    fn adopt_moved_object_on_destination_primary(
        &mut self,
        moved: &DfsObjectMetadata,
        source_path: &str,
        expected_source: Option<&DfsObjectMetadata>,
        finalized_proof_object: Option<&DfsObjectMetadata>,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        let native_owners = self.publication_execution.current();
        let source_actor = native_owners
            .as_ref()
            .map(|owners| crate::source_adoption::actor_for_root(owners.data_root()))
            .transpose()?;
        let phase_key = job.map(|job| {
            Self::cross_shard_job_phase_operation_id(
                job,
                &format!("adopt-object-family\0{}\0{source_path}", moved.object_id),
            )
        });
        let phase_already_completed = phase_key
            .as_ref()
            .is_some_and(|key| self.completed_remote_namespace_phases.contains_key(key));
        let mut prepared_remote = phase_key
            .as_ref()
            .and_then(|key| self.prepared_remote_sources.remove(key));
        let source_config = self
            .namespace_shards_for_exact_paths(&moved.file_system_id, std::iter::once(source_path))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source phase has no actual source assignment".into(),
                })
            })?;
        if prepared_remote.is_none()
            && !phase_already_completed
            && source_actor
                .as_ref()
                .is_some_and(|actor| !actor.is_local_primary(&source_config))
        {
            let job = job.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source preparation requires its actual Running job".into(),
                })
            })?;
            let target_config = self
                .namespace_shards_for_exact_paths(
                    &moved.file_system_id,
                    std::iter::once(moved.path.as_str()),
                )?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "source preparation has no actual target assignment".into(),
                    })
                })?;
            let job_config = self
                .namespace_shards_for_exact_paths(
                    &moved.file_system_id,
                    std::iter::once(job.root_path.as_str()),
                )?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "source preparation has no actual coordinator assignment".into(),
                    })
                })?;
            // The driver popped its active frame from the local work stack.
            // Custody binds the actual full durable Running row, including that
            // active frame, and revalidates it under the real job writer.
            let captured_job = self.namespace_job(&job.job_id)?.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source phase has no full durable Running job".into(),
                })
            })?;
            let plan = crate::source_adoption::SourcePreparationPlan {
                request_id: phase_key.clone().expect("actual job owns phase identity"),
                job: captured_job,
                job_config,
                source_config,
                target_config,
                destination: moved.clone(),
                source_path: source_path.into(),
                expected_source: expected_source.cloned(),
                transfer_id: crate::protocol::cross_shard_pin_transfer_id(moved, source_path)?,
            };
            self.namespace_execution.record_job();
            self.remote_namespace_phase = Some(Box::new(
                crate::source_adoption::RemoteNamespacePhase::PrepareSource {
                    actor: source_actor.clone().expect("checked actual actor"),
                    plan,
                    deadline: self.transport.operation_deadline().unwrap_or_else(|| {
                        std::time::Instant::now() + crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT
                    }),
                },
            ));
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "durable namespace job yielded actual source-primary capture".into(),
            }));
        }
        let local_preparation = if prepared_remote.is_none()
            && !phase_already_completed
            && source_actor
                .as_ref()
                .is_some_and(|actor| actor.is_local_primary(&source_config))
        {
            let job = job.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "local source preparation requires its actual Running job".into(),
                })
            })?;
            let captured_job = self.namespace_job(&job.job_id)?.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "local source phase has no full durable Running job".into(),
                })
            })?;
            let target_config = self
                .namespace_shards_for_exact_paths(
                    &moved.file_system_id,
                    std::iter::once(moved.path.as_str()),
                )?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "local source preparation has no actual target assignment".into(),
                    })
                })?;
            let job_config = self
                .namespace_shards_for_exact_paths(
                    &moved.file_system_id,
                    std::iter::once(job.root_path.as_str()),
                )?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "local source preparation has no actual coordinator assignment"
                            .into(),
                    })
                })?;
            let plan = crate::source_adoption::SourcePreparationPlan {
                request_id: phase_key
                    .clone()
                    .expect("actual job owns local phase identity"),
                job: captured_job,
                job_config,
                source_config: source_config.clone(),
                target_config,
                destination: moved.clone(),
                source_path: source_path.into(),
                expected_source: expected_source.cloned(),
                transfer_id: crate::protocol::cross_shard_pin_transfer_id(moved, source_path)?,
            };
            prepared_remote = source_actor
                .as_ref()
                .expect("checked actual local actor")
                .inspect_source_preparation(&self.node_id, &plan)?;
            Some(plan)
        } else {
            None
        };
        let mut source_capture = None;
        let source_family = if let Some(prepared) = prepared_remote.as_ref() {
            Some(prepared.family.clone())
        } else {
            match self.metadata_backend.as_mut() {
                Some(backend) if source_actor.is_some() && !phase_already_completed => {
                    let job = job.ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message:
                                "native source adoption requires its real durable namespace job"
                                    .into(),
                        })
                    })?;
                    match backend.capture_source_adoption(
                        &moved.object_id,
                        source_path,
                        &job.job_id,
                    ) {
                        Ok(capture) => {
                            let family = capture.family().clone();
                            source_capture = Some(capture);
                            Some(family)
                        }
                        Err(crate::MetadataBackendError::UnknownObject { object_id })
                            if object_id == moved.object_id && expected_source.is_none() =>
                        {
                            None
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                Some(backend) => {
                    match backend.capture_cross_shard_source_family(&moved.object_id, source_path) {
                        Ok(family) => Some(family),
                        Err(crate::MetadataBackendError::UnknownObject { object_id })
                            if object_id == moved.object_id && expected_source.is_none() =>
                        {
                            None
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                None => None,
            }
        };
        if let (Some(expected_source), Some(source_family)) =
            (expected_source, source_family.as_ref())
            && &source_family.source_object != expected_source
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "source object {} changed while capturing its cross-shard metadata family; retry before destination publication",
                    moved.object_id
                ),
            }));
        }
        if source_family.is_none() && expected_source.is_none() {
            // A stale runner with only a hydrated target has no authority to
            // reconcile an empty pin set: doing so could erase the transfer
            // witness while the real source still owns durable pins. Ask the
            // fresh destination primary for a read-only exact-generation
            // proof instead. Any active transfer, mismatch, or old peer fails
            // without mutating destination provenance.
            return self.verify_cross_shard_adoption_finalized(
                finalized_proof_object.unwrap_or(moved),
                source_path,
                job,
            );
        }
        let mut transfer_identity_object = moved.clone();
        if let Some(source_family) = source_family.as_ref()
            && matches!(
                source_family.source_object.tombstone_provenance,
                Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
            )
        {
            transfer_identity_object.version = source_family.source_object.version.clone();
        }
        let pin_transfer_id =
            crate::protocol::cross_shard_pin_transfer_id(&transfer_identity_object, source_path)
                .map_err(DfsPosixError::from)?;
        if let Some(crate::DfsTombstoneProvenance::SourceEviction {
            pin_transfer_id: finalize_transfer_id,
            ..
        }) = source_family
            .as_ref()
            .and_then(|family| family.source_object.tombstone_provenance.as_ref())
        {
            let Some(finalize_transfer_id) = finalize_transfer_id.as_deref() else {
                if source_family.as_ref().is_some_and(|family| {
                    family.version_pins.is_empty() && family.pin_transfers.is_empty()
                }) {
                    return self.verify_cross_shard_adoption_finalized(
                        finalized_proof_object.unwrap_or(moved),
                        source_path,
                        job,
                    );
                }
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "pinned source eviction has no exact transfer identity".into(),
                }));
            };
            if finalize_transfer_id != pin_transfer_id {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "source-eviction witness for {} names pin transfer {}, expected {}",
                        moved.object_id, finalize_transfer_id, pin_transfer_id
                    ),
                }));
            }
            return self.finalize_cross_shard_pin_transfer(
                &transfer_identity_object,
                source_path,
                finalize_transfer_id,
                job,
            );
        }
        let (
            mut versions,
            version_pins,
            receipts,
            intents,
            intent_receipts,
            intent_receipt_retention,
            content_reference_outcomes,
            content_reference_outcome_retention,
            advisory_lock_state,
        ) = source_family
            .as_ref()
            .map(|family| {
                (
                    family.versions.clone(),
                    family.version_pins.clone(),
                    family.receipts.clone(),
                    family.intents.clone(),
                    family.intent_receipts.clone(),
                    family.intent_receipt_retention.clone(),
                    family.content_reference_outcomes.clone(),
                    family.content_reference_outcome_retention.clone(),
                    family.advisory_lock_state.clone(),
                )
            })
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "current cross-shard adoption lost its complete source family".into(),
                })
            })?;
        if !content_reference_outcomes.is_empty()
            || content_reference_outcome_retention.is_some()
            || versions.iter().any(|version| {
                !version.retention.is_empty()
                    || !version.module_metadata.is_empty()
                    || version.content_durability.is_some()
            })
        {
            self.require_file_history_shard_support(&moved.file_system_id, &moved.path)
                .map_err(DfsPosixError::from)?;
        }
        let mut moved_version = cross_shard_object_version_record(moved);
        if let Some(source_version) = source_family.as_ref().and_then(|family| {
            family
                .versions
                .iter()
                .find(|version| version.version == family.source_object.version)
        }) && source_version.content_durability.is_some()
        {
            if source_version.file_system_id != moved.file_system_id
                || source_version.object_id != moved.object_id
                || source_version.content_hash != moved.content_hash
                || source_version.size_bytes != moved.content_size_bytes
            {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message:
                        "cross-shard metadata move cannot change the recorded content identity"
                            .into(),
                }));
            }
            moved_version.content_durability = source_version.content_durability.clone();
        }
        for (namespace, order) in
            crate::metadata_summary::retention_orders_for_versions(versions.iter())
        {
            let state = moved_version.retention.entry(namespace).or_default();
            state.order = state.order.max(order);
        }
        if let Some(existing) = versions.iter_mut().find(|version| {
            version.object_id == moved_version.object_id && version.version == moved_version.version
        }) {
            if existing.content_durability.is_none() {
                existing.content_durability = moved_version.content_durability;
            }
        } else {
            versions.push(moved_version);
        }
        let version_pin_set_fingerprint =
            crate::protocol::cross_shard_version_pin_set_fingerprint(moved, &version_pins)
                .map_err(DfsPosixError::from)?;
        let target_cfg = match local_preparation.as_ref() {
            Some(plan) => plan.target_config.clone(),
            None => self
                .namespace_shards_for_exact_paths(
                    &moved.file_system_id,
                    std::iter::once(moved.path.as_str()),
                )?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "cross-shard adoption has no actual target assignment".into(),
                    })
                })?,
        };
        let pin_transfer_installed = {
            let primary = target_cfg.primary_node_id.clone();
            let source_offer = if let Some(prepared) = prepared_remote {
                Some(prepared.offer)
            } else if let Some(capture) = source_capture.take() {
                Some(
                    source_actor
                        .as_ref()
                        .ok_or_else(|| {
                            DfsPosixError::Dfs(DfsError::Metadata {
                                message: "native source actor unavailable".into(),
                            })
                        })?
                        .register(
                            capture,
                            local_preparation.ok_or_else(|| {
                                DfsPosixError::Dfs(DfsError::Metadata {
                                    message:
                                        "local source capture lost its exact stable preparation"
                                            .into(),
                                })
                            })?,
                        )?,
                )
            } else {
                None
            };
            // A completed phase is consumed before a new command/offer is
            // needed. Its actual response was validated by the normal host.
            if let Some(key) = phase_key.as_ref() {
                if let Some(outcome) = self.completed_remote_namespace_phases.remove(key) {
                    return self.finish_completed_source_adoption_phase(
                        moved,
                        source_path,
                        job,
                        &pin_transfer_id,
                        outcome,
                    );
                }
            }
            let source_offer = source_offer.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "clustered source adoption lacks its native owned capture".into(),
                })
            })?;
            let result = self.route_cross_shard_job_phase_with_result(
                job,
                &format!("adopt-object-family\0{}\0{source_path}", moved.object_id),
                primary,
                DfsNamespaceOperation::AdoptObjectAtPath {
                    source_offer,
                    object: moved.clone(),
                    source_path: source_path.to_string(),
                    pin_transfer_id: Some(pin_transfer_id.clone()),
                    versions,
                    version_pins,
                    version_pin_set_fingerprint: Some(version_pin_set_fingerprint),
                    receipts,
                    intents,
                    intent_receipts,
                    intent_receipt_retention,
                    content_reference_outcomes,
                    content_reference_outcome_retention,
                    advisory_lock_state,
                },
            )?;
            match result {
                Some(DfsPosixCoordinationResult::ObjectFamilyAdoption {
                    pin_transfer_id: acknowledged_transfer_id,
                    pin_transfer_active,
                    ..
                }) if acknowledged_transfer_id == pin_transfer_id => pin_transfer_active,
                _ => false,
            }
        };
        if let Some(source_family) = source_family.as_ref() {
            self.publish_cross_shard_source_eviction(
                moved,
                source_path,
                source_family,
                pin_transfer_installed.then_some(pin_transfer_id.as_str()),
                if moved.kind == DfsObjectKind::Directory {
                    match job.filter(|job| {
                        crate::namespace::metadata_path(&job.root_path)
                            == crate::namespace::metadata_path(source_path)
                    }) {
                        Some(job) => job
                            .directory_move_counts()?
                            .map_or(0, |plan| plan.source_effect_delta(source_path)),
                        None => 0,
                    }
                } else {
                    0
                },
            )?;
        }
        if pin_transfer_installed {
            self.finalize_cross_shard_pin_transfer(
                &transfer_identity_object,
                source_path,
                &pin_transfer_id,
                job,
            )?;
        }
        Ok(())
    }

    fn finish_completed_source_adoption_phase(
        &mut self,
        moved: &DfsObjectMetadata,
        source_path: &str,
        job: Option<&DfsNamespaceJobRecord>,
        pin_transfer_id: &str,
        outcome: crate::service_state::PendingRoutedNamespaceResult,
    ) -> Result<()> {
        if let crate::DfsNamespaceResult::Rejected {
            reason,
            posix_error,
            ..
        } = outcome.result
        {
            return Err(crate::posix_model::posix_error_for_namespace_rejection(
                reason,
                posix_error,
            ));
        }
        let active = match outcome.posix_coordination_result {
            Some(DfsPosixCoordinationResult::ObjectFamilyAdoption {
                pin_transfer_id: confirmed,
                pin_transfer_active,
                ..
            }) if confirmed == pin_transfer_id => pin_transfer_active,
            _ => {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "native adoption omitted its exact family receipt".into(),
                }));
            }
        };
        // The released host phase already retired the source through its
        // actual primary and normal guarded effect. A coordinator replica is
        // not a second source author.
        if active {
            self.finalize_cross_shard_pin_transfer(moved, source_path, pin_transfer_id, job)?;
        }
        Ok(())
    }

    fn verify_cross_shard_adoption_finalized(
        &mut self,
        moved: &DfsObjectMetadata,
        source_path: &str,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        let pin_transfer_id = crate::protocol::cross_shard_pin_transfer_id(moved, source_path)
            .map_err(DfsPosixError::from)?;
        let operation = DfsNamespaceOperation::VerifyObjectFamilyAdoption {
            object: moved.clone(),
            source_path: source_path.to_string(),
            pin_transfer_id: pin_transfer_id.clone(),
        };
        let result = match self.routed_namespace_primary_for_path(&moved.path)? {
            Some(primary) => self.route_cross_shard_job_phase_with_result(
                job,
                &format!(
                    "verify-object-family-adoption\0{}\0{source_path}",
                    moved.object_id
                ),
                primary,
                operation,
            )?,
            None => self.execute_posix_coordination_operation_local(&operation)?,
        };
        match result {
            Some(DfsPosixCoordinationResult::ObjectFamilyAdoptionVerified {
                object_id,
                target_version,
                pin_transfer_id: acknowledged_transfer_id,
                ..
            }) if object_id == moved.object_id
                && target_version == moved.version
                && acknowledged_transfer_id == pin_transfer_id =>
            {
                Ok(())
            }
            _ => Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "destination authority did not prove finalized cross-shard adoption of {} at {} version {}",
                    moved.object_id, moved.path, moved.version
                ),
            })),
        }
    }

    fn finalize_cross_shard_pin_transfer(
        &mut self,
        transfer_object: &DfsObjectMetadata,
        source_path: &str,
        pin_transfer_id: &str,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        match self.routed_namespace_primary_for_path(&transfer_object.path)? {
            Some(primary) => self.route_cross_shard_job_phase(
                job,
                &format!(
                    "finalize-object-family-adoption\0{}\0{source_path}",
                    transfer_object.object_id
                ),
                primary,
                DfsNamespaceOperation::FinalizeObjectFamilyAdoption {
                    path: transfer_object.path.clone(),
                    source_path: source_path.to_string(),
                    object_id: transfer_object.object_id.clone(),
                    target_version: transfer_object.version.clone(),
                    pin_transfer_id: pin_transfer_id.to_string(),
                },
            ),
            None => {
                self.finalize_object_family_pin_transfer(
                    transfer_object,
                    source_path,
                    pin_transfer_id,
                )?;
                let metadata_target = self.capture_metadata_quorum_sync_target(transfer_object)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )
                .map(drop)
                .map_err(DfsPosixError::from)
            }
        }
    }

    /// Coordinates a hardlink whose alias path lives in a metadata shard
    /// owned by another primary: the link-path primary atomically adopts the
    /// alias record, then this node — the target's shard authority — records
    /// the link-count change on the target.
    pub(crate) fn coordinate_cross_shard_hardlink(
        &mut self,
        existing_path: &str,
        link_path: &str,
        destination_primary: NodeId,
    ) -> Result<()> {
        let (existing_path, existing_requires_directory) =
            normalize_posix_path_with_trailing_directory(existing_path)?;
        let existing_path = self.resolve_posix_path_distributed(&existing_path, false)?;
        if existing_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&existing_path)?;
        }
        let (link_path, link_requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path_distributed(&link_path, false)?;
        if link_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        let existing =
            self.lookup_sifs_path(&existing_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: existing_path.clone(),
                })?;
        if existing.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: existing_path,
            });
        }
        let target = self.resolve_hardlink_target(&existing)?;
        match target.kind {
            DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory {
                    path: existing_path,
                });
            }
            DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::NotFound {
                    path: existing_path,
                });
            }
            _ => {}
        }
        // The durable job retains this id across retries and restarts.
        let alias_object_id = DfsObjectId::generate();
        let alias = DfsObjectMetadata {
            object_id: alias_object_id,
            file_system_id: target.file_system_id.clone(),
            path: link_path.clone(),
            kind: DfsObjectKind::Hardlink,
            namespace_link_count: None,
            tombstone_provenance: None,
            owner_node_id: self.node_id.clone(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: Some(target.object_id.clone()),
            mode: target.mode,
            uid: target.uid,
            gid: target.gid,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: initial_version(),
            policy: target.policy.clone(),
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let job_id = namespace_job_id(
            "hardlink-cross-shard",
            &target.file_system_id,
            &format!("{existing_path}->{link_path}"),
            &target.object_id,
            &target.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return self.drive_cross_shard_namespace_job(job);
        }
        let active_request = self.namespace_execution.request().cloned().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Distributed {
                message: "cross-shard hardlink has no durable outer namespace admission".into(),
            })
        })?;
        let expected_target = DfsNamespaceObjectPrecondition {
            object_id: target.object_id.clone(),
            version: target.version.clone(),
        };
        let mut committed_target = target.clone();
        committed_target.namespace_link_count =
            Some(super::metadata::updated_namespace_link_count(&target, 1)?);
        committed_target.changed_at_unix_nanos =
            now_unix_nanos().max(target.changed_at_unix_nanos.saturating_add(1));
        committed_target.version = next_version(&target.version);
        let origin = Some(DfsNamespaceJobOrigin::from_request(&active_request));
        self.publish_namespace_request_record(&active_request)?;
        let file_system_id = sifs_file_system_id();
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &existing_path)?;
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &link_path)?;
        let job = match self.namespace_job(&job_id)? {
            Some(job) => job,
            None => {
                let now = time::OffsetDateTime::now_utc();
                let fence = hardlink_target_fence(
                    &job_id,
                    &link_path,
                    &expected_target,
                    &committed_target,
                    origin.clone(),
                    now,
                );
                DfsNamespaceJobRecord {
                    job_id,
                    file_system_id: target.file_system_id.clone(),
                    origin,
                    kind: DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: existing_path.clone(),
                    target_path: Some(link_path.clone()),
                    pending_frames: vec![
                        DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                            fence_job_id: fence.job_id.clone(),
                            source_path: link_path.clone(),
                            expected_target,
                            committed_target,
                        },
                        DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                            alias,
                            target_object_id: target.object_id.clone(),
                            destination_primary_node_id: destination_primary,
                        },
                        DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                            fence: Box::new(
                                crate::metadata_records::DfsHardlinkTargetFence::from_job(fence)?,
                            ),
                            source_path: link_path.clone(),
                        },
                    ],
                    processed_object_count: 0,
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                    failure_reason: None,
                }
            }
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    fn cross_shard_job_phase_operation_id(job: &DfsNamespaceJobRecord, phase: &str) -> String {
        let identity = format!("{}\0{}", job.job_id, phase);
        let digest = blake3::hash(identity.as_bytes()).to_hex();
        let order_token = job.origin.as_ref().and_then(|origin| {
            crate::intent::stable_operation_order_token(&origin.operation_id)
                .or_else(|| crate::intent::stable_operation_order_token(&origin.request_id))
        });
        match order_token {
            Some(order_token) => format!("xsj_{}_{}", &digest.as_str()[..24], order_token),
            None => format!("xsj_{}", &digest.as_str()[..32]),
        }
    }

    fn hardlink_alias_job_phase_operation_id(job: &DfsNamespaceJobRecord, phase: &str) -> String {
        let identity = format!("{}\0{}", job.job_id, phase);
        let digest = blake3::hash(identity.as_bytes()).to_hex();
        let order_token = job.origin.as_ref().and_then(|origin| {
            crate::intent::stable_operation_order_token(&origin.operation_id)
                .or_else(|| crate::intent::stable_operation_order_token(&origin.request_id))
        });
        match order_token {
            Some(order_token) => format!("hlj_{}_{}", &digest.as_str()[..24], order_token),
            None => format!("hlj_{}", &digest.as_str()[..32]),
        }
    }

    fn route_cross_shard_job_phase(
        &mut self,
        job: Option<&DfsNamespaceJobRecord>,
        phase: &str,
        primary: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<()> {
        self.route_cross_shard_job_phase_with_result(job, phase, primary, operation)
            .map(drop)
    }

    fn route_cross_shard_job_phase_with_result(
        &mut self,
        job: Option<&DfsNamespaceJobRecord>,
        phase: &str,
        primary: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<Option<DfsPosixCoordinationResult>> {
        let canonical_fence_phase = match &operation {
            DfsNamespaceOperation::InstallHardlinkTargetFence { .. } => {
                Some("install-target-fence")
            }
            DfsNamespaceOperation::CommitHardlinkTargetFence { .. } => Some("commit-target-fence"),
            _ => None,
        };
        // A plain service owns no Main admission to release. Preserve both
        // original canonical-fence identities and its existing direct route.
        if self.publication_execution.current().is_none()
            && let Some(phase) = canonical_fence_phase
        {
            let job = job.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "canonical target phase has no durable source job".into(),
                })
            })?;
            return self.route_namespace_operation_with_request_id_and_result(
                Self::hardlink_alias_job_phase_operation_id(job, phase),
                primary,
                operation,
            );
        }
        // Native Main may own the source's global mutation admission. Its
        // target alias publication needs this node's target-shard replica ACK,
        // so use the existing owned phase and release that admission first.
        // Plain service callers own no such runtime admission and retain their
        // existing synchronous route.
        if matches!(operation, DfsNamespaceOperation::AdoptHardlinkAlias { .. })
            && self.publication_execution.current().is_none()
        {
            return self.route_namespace_operation_with_result(primary, operation);
        }
        if matches!(
            operation,
            DfsNamespaceOperation::AdoptObjectAtPath { .. }
                | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
                | DfsNamespaceOperation::AdoptHardlinkAlias { .. }
                | DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
                | DfsNamespaceOperation::ParentCountParticipant { .. }
                | DfsNamespaceOperation::ReleaseDirectoryMoveDestination { .. }
                | DfsNamespaceOperation::CommitHardlinkTargetFence { .. }
        ) {
            let job = job.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "remote source phase has no durable job".into(),
                })
            })?;
            let request = match canonical_fence_phase {
                Some(phase) => Self::hardlink_alias_job_phase_operation_id(job, phase),
                None => Self::cross_shard_job_phase_operation_id(job, phase),
            };
            if matches!(
                operation,
                DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
                    | DfsNamespaceOperation::AdoptHardlinkAlias { .. }
                    | DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
                    | DfsNamespaceOperation::ParentCountParticipant { .. }
                    | DfsNamespaceOperation::ReleaseDirectoryMoveDestination { .. }
                    | DfsNamespaceOperation::CommitHardlinkTargetFence { .. }
            ) && let Some(outcome) = self.completed_remote_namespace_phases.remove(&request)
            {
                if let crate::DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    ..
                } = outcome.result
                {
                    return Err(crate::posix_model::posix_error_for_namespace_rejection(
                        reason,
                        posix_error,
                    ));
                }
                super::routing::validate_cross_shard_adoption_receipt(
                    &operation,
                    outcome.posix_coordination_result.as_ref(),
                )?;
                return Ok(outcome.posix_coordination_result);
            }
            let fs = job.file_system_id.clone();
            let owners = self.publication_execution.current().ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "remote source phase has no actual runtime custody".into(),
                })
            })?;
            let actor = crate::source_adoption::actor_for_root(owners.data_root())?;
            let (command, pending) = self
                .prepare_routed_namespace_request(&request, fs, &primary, operation, false, false)
                .map_err(DfsPosixError::from)?;
            // The captured native job is real durable pending custody even
            // when this invocation resumed it rather than creating it.
            self.namespace_execution.record_job();
            self.remote_namespace_phase = Some(Box::new(
                crate::source_adoption::RemoteNamespacePhase::PublishTarget(
                    crate::source_adoption::RemoteTargetNamespacePhase {
                        actor,
                        command,
                        pending,
                        deadline: self.transport.operation_deadline().unwrap_or_else(|| {
                            let timeout = if canonical_fence_phase.is_some() {
                                crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT
                            } else {
                                crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT
                            };
                            std::time::Instant::now() + timeout
                        }),
                    },
                ),
            ));
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "durable namespace job yielded its owned remote phase".into(),
            }));
        }
        match job {
            Some(job) => self.route_namespace_operation_with_request_id_and_result(
                Self::cross_shard_job_phase_operation_id(job, phase),
                primary,
                operation,
            ),
            None => self.route_namespace_operation_with_result(primary, operation),
        }
    }

    /// Coordinate one exact alias-shard transaction with the canonical target
    /// authority. Both alias paths and the canonical path remain hidden behind
    /// durable Busy fences until the two visible generations have committed.
    pub(crate) fn coordinate_cross_primary_hardlink_alias_mutation(
        &mut self,
        alias: &DfsObjectMetadata,
        result_path: &str,
        transaction: DfsNamespaceTransaction,
        mutation: HardlinkAliasMutation,
    ) -> Result<()> {
        let reason = mutation.label();
        let source_path = normalize_posix_path(&alias.path)?;
        let result_path = normalize_posix_path(result_path)?;
        let job_id = namespace_job_id(
            "hardlink-alias-mutation-cross-primary",
            &alias.file_system_id,
            &format!("{reason}:{source_path}->{result_path}"),
            &alias.object_id,
            &alias.version,
        );
        let active_request = self.namespace_execution.request().cloned().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "cross-primary hardlink alias mutation for {source_path} has no durable outer namespace admission"
                ),
            })
        })?;
        // Unlike a single-shard namespace prepare, this job cannot fuse the
        // outer terminal receipt into its first visible transaction: the
        // canonical target generation still has to commit on another
        // authority. Persist Pending admission before either durable fence so
        // a process crash always leaves enough command identity to resume and
        // eventually complete the exact caller operation.
        self.publish_namespace_request_record(&active_request)?;
        if let Some(job) = self.namespace_job(&job_id)? {
            return self.drive_cross_shard_namespace_job(job);
        }
        let original_reads = self.capture_namespace_transaction_read_set(&transaction)?;
        let target_object_id =
            alias
                .link_target_object_id
                .as_ref()
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: source_path.clone(),
                })?;
        let target = self.resolve_hardlink_target_distributed(alias, None)?;
        if target.kind == DfsObjectKind::Tombstone
            || target.kind == DfsObjectKind::Hardlink
            || target.object_id != *target_object_id
        {
            return Err(DfsPosixError::Conflict {
                path: source_path,
                reason: format!(
                    "canonical hardlink target generation changed; expected object {target_object_id}"
                ),
            });
        }
        let target_path = normalize_posix_path(&target.path)?;
        let target_primary = self.routed_namespace_primary_for_path(&target_path)?;
        if target_primary.is_none() {
            // One node can own two distinct physical writers. Use the same
            // durable saga and local Core phases, never a combined transaction
            // or a self-network request. Both exact current assignments still
            // qualify every independent publication below.
            let shards = self
                .metadata_shard_config_snapshot(&alias.file_system_id)?
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "same-node alias saga has no current configuration projection"
                            .into(),
                    })
                })?;
            let source = crate::select_metadata_shard_config_for_path(
                &alias.file_system_id,
                &shards,
                &source_path,
            )
            .map_err(DfsError::from)?;
            let target = crate::select_metadata_shard_config_for_path(
                &alias.file_system_id,
                &shards,
                &target_path,
            )
            .map_err(DfsError::from)?;
            if !matches!((source.as_ref(), target.as_ref()), (Some(source), Some(target))
                if source.shard_id != target.shard_id
                    && source.primary_node_id == self.node_id
                    && target.primary_node_id == self.node_id)
            {
                return Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "hardlink alias saga for {source_path} has no two distinct locally owned physical shards at {target_path}"
                    ),
                }));
            }
        }
        let file_system_id = alias.file_system_id.clone();
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &source_path)?;
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &result_path)?;

        let expected_target = DfsNamespaceObjectPrecondition {
            object_id: target.object_id.clone(),
            version: target.version.clone(),
        };
        let mut committed_target = target.clone();
        committed_target.namespace_link_count = Some(
            super::metadata::updated_namespace_link_count(&target, mutation.count_delta())?,
        );
        committed_target.changed_at_unix_nanos =
            now_unix_nanos().max(target.changed_at_unix_nanos.saturating_add(1));
        committed_target.version = next_version(&target.version);
        let now = time::OffsetDateTime::now_utc();
        let origin = Some(DfsNamespaceJobOrigin::from_request(&active_request));
        let fence = hardlink_target_fence(
            &job_id,
            &source_path,
            &expected_target,
            &committed_target,
            origin.clone(),
            now,
        );
        let fence_job_id = fence.job_id.clone();
        let job = DfsNamespaceJobRecord {
            job_id,
            file_system_id,
            origin,
            kind: DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: source_path.clone(),
            target_path: Some(result_path),
            pending_frames: vec![
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                    fence_job_id,
                    source_path: source_path.clone(),
                    expected_target,
                    committed_target,
                },
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit {
                    transaction,
                    shard_id: original_reads.shard_id,
                    expected_object_versions: original_reads
                        .expected_object_versions
                        .into_iter()
                        .map(
                            |(object_id, expected_version)| DfsNamespaceObjectVersionExpectation {
                                object_id,
                                expected_version,
                            },
                        )
                        .collect(),
                    expected_path_occupants: original_reads
                        .expected_path_occupants
                        .into_iter()
                        .map(
                            |(path, expected_object_id)| DfsNamespacePathOccupantExpectation {
                                path,
                                expected_object_id,
                            },
                        )
                        .collect(),
                },
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                    fence: Box::new(crate::metadata_records::DfsHardlinkTargetFence::from_job(
                        fence,
                    )?),
                    source_path,
                },
            ],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    fn commit_hardlink_alias_transaction(
        &mut self,
        job: &DfsNamespaceJobRecord,
        transaction: DfsNamespaceTransaction,
        shard_id: DfsMetadataShardId,
        expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
        expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
    ) -> Result<()> {
        let origin = job.origin.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "cross-primary hardlink alias job {} has no durable caller provenance",
                    job.job_id
                ),
            })
        })?;
        let previous_active_request = self.namespace_execution.take_request();
        if previous_active_request
            .as_ref()
            .is_some_and(|request| !origin.matches_request(request))
        {
            self.namespace_execution
                .restore_request(previous_active_request);
            return Err(DfsPosixError::Dfs(DfsError::Busy {
                path: job.root_path.clone(),
            }));
        }
        // Keep the exact caller operation in the visible journal/feed, but do
        // not attach a Completed request record to this intermediate alias
        // generation. The outer request becomes terminal only after the
        // canonical target frame commits and the complete job returns.
        let saved_execution = self.namespace_execution.suspend_request_prepare();
        let outcome = self.apply_namespace_transaction_with_recorded_read_set(
            transaction,
            &origin.request_id,
            shard_id,
            expected_object_versions,
            expected_path_occupants,
        );
        self.namespace_execution
            .restore_request_prepare(saved_execution);
        self.namespace_execution
            .restore_request(previous_active_request);
        outcome
    }

    fn apply_namespace_transaction_with_feed_provenance_only(
        &mut self,
        transaction: DfsNamespaceTransaction,
        operation_id: &str,
    ) -> Result<()> {
        let saved_execution = self.namespace_execution.suspend_request_prepare();
        let outcome =
            self.apply_namespace_transaction_with_journal_operation_id(transaction, operation_id);
        self.namespace_execution
            .restore_request_prepare(saved_execution);
        outcome.map_err(DfsPosixError::from)
    }

    fn install_hardlink_target_fence(
        &mut self,
        source_job: &DfsNamespaceJobRecord,
        fence: &DfsNamespaceJobRecord,
        source_path: &str,
    ) -> Result<()> {
        let operation = DfsNamespaceOperation::InstallHardlinkTargetFence {
            fence: Box::new(fence.clone()),
            source_path: source_path.to_string(),
        };
        match self.routed_namespace_primary_for_path(&fence.root_path)? {
            Some(primary) => self.route_cross_shard_job_phase(
                Some(source_job),
                "install-target-fence",
                primary,
                operation,
            ),
            None => self.install_hardlink_target_fence_local(fence, source_path),
        }
    }

    fn commit_hardlink_target_fence(
        &mut self,
        source_job: &DfsNamespaceJobRecord,
        fence_job_id: &str,
        source_path: &str,
        expected_target: &DfsNamespaceObjectPrecondition,
        committed_target: &DfsObjectMetadata,
    ) -> Result<()> {
        let operation = DfsNamespaceOperation::CommitHardlinkTargetFence {
            fence_job_id: fence_job_id.to_string(),
            source_path: source_path.to_string(),
            target_path: committed_target.path.clone(),
            expected_target: expected_target.clone(),
            committed_target: committed_target.clone(),
        };
        match self.routed_namespace_primary_for_path(&committed_target.path)? {
            Some(primary) => self.route_cross_shard_job_phase(
                Some(source_job),
                "commit-target-fence",
                primary,
                operation,
            ),
            None => self.commit_hardlink_target_fence_local(
                fence_job_id,
                source_path,
                &committed_target.path,
                expected_target,
                committed_target,
            ),
        }
    }

    pub(crate) fn install_hardlink_target_fence_local(
        &mut self,
        fence: &DfsNamespaceJobRecord,
        source_path: &str,
    ) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let target_path = normalize_posix_path(&fence.root_path)?;
        if fence.kind != DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            || fence.status != DfsNamespaceJobStatus::Running
            || fence.root_path != target_path
            || fence.target_path.is_some()
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("invalid hardlink target fence {}", fence.job_id),
            }));
        }
        let Some(DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path: reserved_source_path,
            expected_target,
            committed_target,
        }) = fence.pending_frames.as_slice().first()
        else {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "hardlink target fence {} has no exact reservation",
                    fence.job_id
                ),
            }));
        };
        if fence.pending_frames.len() != 1
            || normalize_posix_path(reserved_source_path)? != normalize_posix_path(source_path)?
            || normalize_posix_path(&committed_target.path)? != target_path
            || committed_target.object_id != expected_target.object_id
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "hardlink target fence {} reservation does not match its command",
                    fence.job_id
                ),
            }));
        }
        let scope = self.metadata_effect_scope_for_object(
            committed_target,
            format!("install-target-fence:{}", fence.job_id),
        )?;
        let _execution = self.acquire_prepared_execution(&scope)?;
        if let Some(existing) = self.namespace_job(&fence.job_id)? {
            if existing == *fence {
                self.apply_cross_shard_namespace_job_guard(&existing)?;
                return Ok(());
            }
            if existing.status == DfsNamespaceJobStatus::Completed
                && existing.job_id == fence.job_id
                && existing.file_system_id == fence.file_system_id
                && existing.root_path == fence.root_path
                && existing.origin == fence.origin
                && existing.pending_frames == fence.pending_frames
                && existing.created_at == fence.created_at
            {
                let current = self.lookup_sifs_path(&target_path)?;
                if current.as_ref() == Some(committed_target) {
                    return Ok(());
                }
            }
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "hardlink target fence {} conflicts with an existing durable record",
                    fence.job_id
                ),
            }));
        }
        let current = self.lookup_sifs_path(&target_path)?;
        if current.as_ref() == Some(committed_target) {
            // The target-side terminal job/receipt may have been compacted
            // after the exact pre-minted generation committed. Full metadata
            // equality is an immutable completion proof for the source frame.
            return Ok(());
        }
        // Two alias owners must not reserve the same canonical generation.
        // The only overlapping source job allowed here is the real producer
        // whose immutable pending frame contains this exact reservation.
        for job_id in self
            .cross_shard_namespace_locks
            .overlapping_job_ids(&fence.file_system_id, &target_path)
        {
            let mut original_producer = false;
            if let Some(job) = self.namespace_job(&job_id)?
                && job.status == DfsNamespaceJobStatus::Running
                && job.file_system_id == fence.file_system_id
                && matches!(
                    job.kind,
                    DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                        | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                )
                && hardlink_target_fence(
                    &job.job_id,
                    source_path,
                    expected_target,
                    committed_target,
                    job.origin.clone(),
                    job.created_at,
                ) == *fence
            {
                for frame in &job.pending_frames {
                    if let DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                        fence: recorded,
                        source_path: recorded_source,
                    } = frame
                        && normalize_posix_path(recorded_source)?
                            == normalize_posix_path(source_path)?
                        && recorded.as_ref().clone().into_job()? == *fence
                    {
                        original_producer = true;
                    }
                }
            }
            if !original_producer {
                return Err(DfsPosixError::Dfs(DfsError::Busy { path: target_path }));
            }
        }
        if current.as_ref().is_none_or(|target| {
            target.kind == DfsObjectKind::Tombstone
                || target.kind == DfsObjectKind::Hardlink
                || target.object_id != expected_target.object_id
                || target.version != expected_target.version
        }) {
            return Err(DfsPosixError::Conflict {
                path: target_path,
                reason: format!(
                    "canonical hardlink target generation changed before fence installation; expected {}@{}",
                    expected_target.object_id, expected_target.version
                ),
            });
        }
        self.persist_namespace_job(fence)
    }

    pub(crate) fn commit_hardlink_target_fence_local(
        &mut self,
        fence_job_id: &str,
        source_path: &str,
        target_path: &str,
        expected_target: &DfsNamespaceObjectPrecondition,
        committed_target: &DfsObjectMetadata,
    ) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let target_path = normalize_posix_path(target_path)?;
        let fence = self.namespace_job(fence_job_id)?;
        if fence.is_none()
            && self.lookup_sifs_path(&target_path)?.as_ref() == Some(committed_target)
        {
            return Ok(());
        }
        let mut fence = fence.ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: format!("hardlink target fence {fence_job_id} is missing"),
            })
        })?;
        let reservation_matches = matches!(
            fence.pending_frames.as_slice(),
            [DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: reserved_source_path,
                expected_target: reserved_expected,
                committed_target: reserved_committed,
            }] if normalize_posix_path(reserved_source_path).ok().as_deref()
                    == normalize_posix_path(source_path).ok().as_deref()
                && reserved_expected == expected_target
                && reserved_committed == committed_target
        );
        if fence.kind != DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            || fence.root_path != target_path
            || !reservation_matches
        {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!("hardlink target fence {fence_job_id} does not match its commit"),
            }));
        }
        // A Completed fence is committed: by an earlier run of this step, or
        // by its shard's conversion, which settles a fence from its
        // coordinator's recorded phase. Committing it again changes nothing,
        // whatever its target has become since.
        if fence.status == DfsNamespaceJobStatus::Completed {
            return Ok(());
        }
        let current = self.lookup_sifs_path(&target_path)?;
        if current.as_ref() != Some(committed_target) {
            if current.as_ref().is_none_or(|target| {
                target.kind == DfsObjectKind::Tombstone
                    || target.kind == DfsObjectKind::Hardlink
                    || target.object_id != expected_target.object_id
                    || target.version != expected_target.version
            }) {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "canonical hardlink target changed while fence {fence_job_id} was held"
                    ),
                }));
            }
            let journal_operation_id = fence
                .origin
                .as_ref()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "hardlink target fence {fence_job_id} has no durable caller provenance"
                        ),
                    })
                })?
                .request_id
                .clone();
            self.apply_namespace_transaction_with_feed_provenance_only(
                DfsNamespaceTransaction {
                    extension_expectations: Vec::new(),
                    contribution_digest: None,
                    content_reference_identity: None,
                    file_version_pins: None,
                    transaction_id: namespace_transaction_id_for_batch(
                        "hardlink-target-fence-commit",
                        &committed_target.file_system_id,
                        std::slice::from_ref(committed_target),
                        &[],
                    ),
                    file_system_id: committed_target.file_system_id.clone(),
                    objects: vec![committed_target.clone()],
                    versions: Vec::new(),
                    advisory_lock_states: Vec::new(),
                    created_at: fence.created_at,
                },
                &journal_operation_id,
            )?;
        }
        if fence.status != DfsNamespaceJobStatus::Completed {
            let now = time::OffsetDateTime::now_utc();
            fence.status = DfsNamespaceJobStatus::Completed;
            fence.processed_object_count = 1;
            fence.updated_at = now;
            fence.completed_at = Some(now);
            self.persist_namespace_job(&fence)?;
        }
        Ok(())
    }

    /// Drives a cross-shard saga job to completion: the foreign adoption is
    /// idempotent and may be re-sent after restarts or timeouts; the local
    /// completion is idempotent against current state. Structured rejections
    /// from the destination abort the job before anything mutated locally;
    /// transport failures leave the job running for maintenance to resume.
    /// Runs a cross-shard saga inline to completion, in bounded slices.
    fn drive_cross_shard_namespace_job(&mut self, mut job: DfsNamespaceJobRecord) -> Result<()> {
        loop {
            job = self.run_cross_shard_namespace_job(job, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS)?;
            match job.status {
                DfsNamespaceJobStatus::Completed => return Ok(()),
                DfsNamespaceJobStatus::Running => continue,
                DfsNamespaceJobStatus::Failed => {
                    if job.processed_object_count == 0
                        && job.directory_move_counts()?.and_then(|counts| counts.parent_participants.as_ref()).is_some_and(|plan|
                            plan.phase == crate::DfsParentCountPhase::Aborting
                                && plan.confirmed == plan.participants.len()
                                && matches!(plan.rejection, Some(crate::DfsParentCountRejection::ParentChanged
                                    | crate::DfsParentCountRejection::Contended | crate::DfsParentCountRejection::AssignmentChanged))) {
                        // This is a definite rejected attempt: every reservation
                        // is withdrawn and no namespace effect was issued. Keep
                        // its terminal identity while letting a fresh caller
                        // replan from the changed parent/assignment.
                        return Err(DfsPosixError::Conflict {
                            path: job.root_path.clone(),
                            reason: "directory parent changed before namespace effects; retry with a fresh operation".into(),
                        });
                    }
                    let reason = job
                        .failure_reason
                        .unwrap_or_else(|| format!("cross-shard job {} failed", job.job_id));
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: reason,
                    }));
                }
            }
        }
    }

    pub(crate) fn run_cross_shard_namespace_job(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        // The caller may retain an older Running snapshot while the reactive
        // maintenance owner advances the same durable job. Always begin from
        // the authoritative row, and never reinterpret a terminal saga as a
        // fresh source-path mutation after its object was already evicted.
        if let Some(current) = self.namespace_job(&job.job_id)? {
            job = current;
        }
        if job.status != DfsNamespaceJobStatus::Running {
            return Ok(job);
        }
        if job.pending_frames.iter().any(|frame| {
            matches!(
                frame,
                DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. }
            )
        }) {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: "legacy hardlink job has no sealed count successor; explicit migration is required".into(),
            }));
        }
        let file_system_id = job.file_system_id.clone();
        // Snapshot-copied or stale jobs must not run on a node that is no
        // longer the source-side authority.
        let source_shards =
            self.namespace_shards_for_exact_paths(&file_system_id, [job.root_path.as_str()])?;
        if let Some(shard) = source_shards
            .iter()
            .find(|shard| shard.primary_node_id != self.node_id)
        {
            // Replicas retain the running job as a failover reservation, but
            // must never append a local failure or otherwise advance its
            // journal. Only the current source-shard primary may drive the
            // saga; after a certified primary transfer this same copy becomes
            // executable under the new authority epoch.
            return Err(DfsPosixError::Dfs(DfsError::StaleOwnerEpoch {
                file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            }));
        }
        self.apply_cross_shard_namespace_job_guard(&job)?;
        let mut steps = 0usize;
        while !job.pending_frames.is_empty() {
            if steps >= max_steps {
                job.updated_at = time::OffsetDateTime::now_utc();
                self.persist_namespace_job(&job)?;
                return Ok(job);
            }
            let owns_parent_plan = job.directory_move_counts()?.is_some_and(|counts| counts.parent_participants.is_some());
            let parent_progress = self.advance_parent_count_plan(&mut job);
            if owns_parent_plan {
                // Phase replies can fail without appending another job row.
                // Preserve the caller's Pending ownership from the actual
                // durable Running job, or observe its terminal abort status.
                let actual = self.namespace_job(&job.job_id)?.ok_or_else(|| DfsPosixError::Dfs(DfsError::Metadata {
                    message: "parent phase lost its original durable coordinator".into(),
                }))?;
                self.namespace_execution.observe_coordinator_job(&actual);
                job = actual;
                if job.status != DfsNamespaceJobStatus::Running {
                    return Ok(job);
                }
            }
            if !parent_progress? { return Ok(job); }
            let frame = job.pending_frames.pop().expect("nonempty job frames");
            steps = steps.saturating_add(1);
            if matches!(frame, DfsNamespaceJobFrame::CrossShardDirAbort { .. }) {
                self.park_step_on_converting_shard(&mut job, &frame)?;
                return self.run_directory_move_abort(job, frame);
            }
            let hardlink_alias_coordination_frame = matches!(
                &frame,
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { .. }
                    | DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
                    | DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { .. }
            );
            #[cfg(test)]
            let hardlink_alias_commit_frame = matches!(
                &frame,
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
            );
            if let DfsNamespaceJobFrame::CrossShardAdopt {
                moved_object,
                old_path,
                ..
            } = &frame
            {
                let phase = Self::cross_shard_job_phase_operation_id(
                    &job,
                    &format!(
                        "adopt-object-family\0{}\0{old_path}",
                        moved_object.object_id
                    ),
                );
                if let Some(outcome) = self.completed_remote_namespace_phases.get(&phase)
                    && let crate::DfsNamespaceResult::Rejected {
                        reason,
                        posix_error,
                        ..
                    } = &outcome.result
                {
                    let error = crate::posix_model::posix_error_for_namespace_rejection(
                        reason.clone(),
                        posix_error.clone(),
                    );
                    self.completed_remote_namespace_phases.remove(&phase);
                    let structured = matches!(
                        &error,
                        DfsPosixError::NotFound { .. }
                            | DfsPosixError::MissingParent { .. }
                            | DfsPosixError::IsDirectory { .. }
                            | DfsPosixError::NotDirectory { .. }
                            | DfsPosixError::AlreadyExists { .. }
                            | DfsPosixError::InvalidPath { .. }
                            | DfsPosixError::Conflict { .. }
                            | DfsPosixError::DirectoryNotEmpty { .. }
                    );
                    let parent_effects_issued = job
                        .directory_move_counts()?
                        .and_then(|counts| counts.parent_participants.as_ref())
                        .is_some_and(|plan| {
                            matches!(
                                plan.phase,
                                crate::DfsParentCountPhase::Issued
                                    | crate::DfsParentCountPhase::Settling
                            )
                        });
                    if structured
                        && (!parent_effects_issued
                            || (job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                                && job.root_path == *old_path && job.processed_object_count == 1))
                        && let Some(execution) = self.publication_execution.current()
                    {
                        let actor = crate::source_adoption::actor_for_root(execution.data_root())?;
                        let retained = self.namespace_job(&job.job_id)?.ok_or_else(|| {
                            DfsPosixError::Dfs(DfsError::Metadata {
                                message: "rejected adoption lost its exact original job".into(),
                            })
                        })?;
                        let configs = self.namespace_shards_for_exact_paths(
                            &retained.file_system_id,
                            [old_path.as_str()],
                        )?;
                        if let [source] = configs.as_slice() {
                            let physical = self
                                .metadata_backend
                                .as_ref()
                                .ok_or_else(|| {
                                    DfsPosixError::Dfs(DfsError::Metadata {
                                        message: "rejected adoption has no current backend".into(),
                                    })
                                })?
                                .publication_owner_scope(
                                    &retained.file_system_id,
                                    &crate::DfsMetadataShardId::new(&source.shard_id),
                                )?;
                            let rejected_root = retained.initial_directory_root_adoption().is_some();
                            let mut failed = retained.clone();
                            if actor.finish_locally_cancelled_adoption(
                                &phase,
                                &retained,
                                moved_object,
                                old_path,
                                &physical,
                                &execution,
                                || {
                                    if rejected_root {
                                        // The existing source cancellation owner excludes
                                        // all late claims while this exact decision commits.
                                        self.begin_parent_count_abort(&mut failed, crate::DfsParentCountRejection::RootAdoptionRejected)
                                    } else {
                                        self.fail_namespace_job(failed, error.to_string()).map(|_| ())
                                    }
                                },
                            )? {
                                if rejected_root {
                                    self.namespace_execution.record_job();
                                    job = self.namespace_job(&job.job_id)?.ok_or_else(|| DfsPosixError::Dfs(DfsError::Metadata {
                                        message: "root withdrawal lost its original coordinator".into(),
                                    }))?;
                                    continue;
                                }
                                return Err(error);
                            }
                        }
                    }
                    // The native host already correlated this exact target
                    // response. An admitted target can still retain the actual
                    // source/job writers. Preserve the original durable Running
                    // row and propagate its rejection before recapturing that
                    // source or appending a redundant error-only job update.
                    self.namespace_execution.record_job();
                    // A destination whose storage is closed took no effect:
                    // the adoption waits there.
                    let mut durable = job.clone();
                    durable.pending_frames.push(frame.clone());
                    if let Some(parked) = self.park_refused_namespace_step(&durable, &frame, &error)? {
                        return Err(parked);
                    }
                    return Err(error);
                }
            }
            self.park_step_on_converting_shard(&mut job, &frame)?;
            let outcome = match frame.clone() {
                DfsNamespaceJobFrame::CrossShardAdopt {
                    moved_object,
                    old_path,
                    destination_primary_node_id,
                    ..
                } => {
                    let _ = destination_primary_node_id;
                    self.run_cross_shard_adopt_for_job(&moved_object, &old_path, Some(&job))
                }
                DfsNamespaceJobFrame::CrossShardComplete {
                    moved_object,
                    old_path,
                } => self.complete_cross_shard_move_locally(
                    &job,
                    &moved_object,
                    &old_path,
                    job.kind != DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                    if job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                        && job.root_path == old_path
                    {
                        job.directory_move_counts()?
                            .map_or(0, |plan| plan.source_effect_delta(&old_path))
                    } else {
                        0
                    },
                ),
                DfsNamespaceJobFrame::CrossShardDirStart {
                    source_object_id,
                    source_root,
                    target_root,
                } => self.run_cross_shard_dir_start(
                    &mut job,
                    &source_object_id,
                    &source_root,
                    &target_root,
                ),
                DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path,
                    target_path,
                    cursor,
                } => self.run_cross_shard_dir_scan(
                    &mut job,
                    &source_path,
                    &target_path,
                    cursor.as_ref(),
                ),
                DfsNamespaceJobFrame::CrossShardDirFinish {
                    source_root,
                    count_plan,
                } => self.release_directory_move_destination(&job).and_then(|()| {
                    if count_plan
                        .as_ref()
                        .is_some_and(|plan| plan.parent_is_participant(&source_root))
                    {
                        Ok(())
                    } else {
                        self.touch_parent_directory_if_local_primary(&source_root)
                    }
                }),
                DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                    alias,
                    target_object_id,
                    destination_primary_node_id,
                } => {
                    let _ = (target_object_id, destination_primary_node_id);
                    let source_path = job.root_path.clone();
                    match self.routed_namespace_primary_for_path(&alias.path)? {
                        Some(primary) => self.route_cross_shard_job_phase(
                            Some(&job),
                            "adopt-hardlink-alias",
                            primary,
                            DfsNamespaceOperation::AdoptHardlinkAlias { alias, source_path },
                        ),
                        None => self.adopt_hardlink_alias_local(&alias),
                    }
                }
                DfsNamespaceJobFrame::CrossShardHardlinkComplete { target_object_id } => {
                    self.complete_cross_shard_hardlink_locally(&target_object_id)
                }
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                    fence,
                    source_path,
                } => self.install_hardlink_target_fence(&job, &fence.into_job()?, &source_path),
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit {
                    transaction,
                    shard_id,
                    expected_object_versions,
                    expected_path_occupants,
                } => self.commit_hardlink_alias_transaction(
                    &job,
                    transaction,
                    shard_id,
                    expected_object_versions,
                    expected_path_occupants,
                ),
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                    fence_job_id,
                    source_path,
                    expected_target,
                    committed_target,
                } => self
                    .park_until_hardlink_fence_storage_active(&job, &frame, &committed_target)
                    .and_then(|()| {
                        self.commit_hardlink_target_fence(
                            &job,
                            &fence_job_id,
                            &source_path,
                            &expected_target,
                            &committed_target,
                        )
                    }),
                DfsNamespaceJobFrame::HardlinkTargetFenceReservation { .. } => {
                    Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "target-side hardlink fence {} cannot be run as a source saga",
                            job.job_id
                        ),
                    }))
                }
                other => Err(DfsPosixError::InvalidPath {
                    path: format!("{}: unexpected frame {other:?}", job.job_id),
                }),
            };
            if self.remote_namespace_phase.is_some() {
                // The phase owns actual source/job writer custody. Its full
                // Running row, including this active frame, was durable before
                // capture. Return the yield before changing/re-publishing that
                // row: a new PutNamespaceJob would wait on our retained writer
                // and prevent the host from releasing its service/global lane.
                let retained = self.namespace_job(&job.job_id)?.ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "captured remote phase lost its original durable Running job"
                            .into(),
                    })
                })?;
                if retained.file_system_id != job.file_system_id
                    || retained.status != DfsNamespaceJobStatus::Running
                    || !retained.pending_frames.contains(&frame)
                {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message:
                            "captured remote phase changed its durable job identity/active frame"
                                .into(),
                    }));
                }
                return match outcome {
                    Err(error @ DfsPosixError::Dfs(DfsError::Distributed { .. })) => Err(error),
                    _ => Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message:
                            "captured remote namespace phase did not return its intentional yield"
                                .into(),
                    })),
                };
            }
            if let Some(current) = self.namespace_job(&job.job_id)?
                && current.status != DfsNamespaceJobStatus::Running
            {
                return Ok(current);
            }
            match outcome {
                Ok(()) => {
                    #[cfg(test)]
                    if job.kind == DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                        && matches!(
                            &frame,
                            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { .. }
                        )
                    {
                        // The target successor is durable, but this source
                        // frame is still pending in the actual original job.
                        run_hardlink_count_after_commit_test_hook(&job.root_path)?;
                    }
                    job.processed_object_count = job.processed_object_count.saturating_add(1);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    // Parent declarations remain durable until the final
                    // coordinator terminal commit, never a Running empty job.
                    let finishing_parent_plan = matches!(&frame, DfsNamespaceJobFrame::CrossShardDirFinish { count_plan: Some(counts), .. } if counts.parent_participants.is_some());
                    if !finishing_parent_plan {
                        self.persist_namespace_job(&job)?;
                    }
                    #[cfg(test)]
                    if hardlink_alias_commit_frame {
                        run_hardlink_alias_after_commit_test_hook(&job.root_path)?;
                    }
                }
                Err(error @ (DfsPosixError::Dfs(_) | DfsPosixError::Metadata(_))) => {
                    // Transport or internal failure: the saga stays running
                    // (and the path stays locked) so maintenance resumes it.
                    job.pending_frames.push(frame.clone());
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                    if let Some(parked) = self.park_refused_namespace_step(&job, &frame, &error)? {
                        return Err(parked);
                    }
                    return Err(error);
                }
                Err(error)
                    if hardlink_alias_coordination_frame
                        || job
                            .directory_move_counts()?
                            .and_then(|counts| counts.parent_participants.as_ref())
                            .is_some_and(|plan| {
                                matches!(
                                    plan.phase,
                                    crate::DfsParentCountPhase::Issued
                                        | crate::DfsParentCountPhase::Settling
                                )
                            }) =>
                {
                    // Once a target fence may exist, failure is forward-only:
                    // retain both durable reservations and let exact retry or
                    // authority recovery settle the same generation.
                    job.pending_frames.push(frame);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                    return Err(error);
                }
                Err(error) if job.directory_move_root_adopted() => {
                    // The root's adoption installed the destination
                    // reservation: the abort frame releases it before failing.
                    self.begin_directory_move_abort(&mut job, frame, &error)?;
                }
                Err(error) => {
                    // Structured POSIX rejection from validation: abort before
                    // any local mutation happened.
                    self.fail_namespace_job(job, error.to_string())?;
                    return Err(error);
                }
            }
        }
        if let Some(current) = self.namespace_job(&job.job_id)?
            && current.status != DfsNamespaceJobStatus::Running
        {
            return Ok(current);
        }
        let now = time::OffsetDateTime::now_utc();
        job.status = DfsNamespaceJobStatus::Completed;
        job.updated_at = now;
        job.completed_at = Some(now);
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    /// The source-side half of a cross-shard move, applied after the
    /// destination primary adopted the record: tombstone the old path, evict
    /// the moved record from the source shard, and (for top-level moves)
    /// touch the source parent. Idempotent so crash recovery can re-run it.
    fn complete_cross_shard_move_locally(
        &mut self,
        job: &DfsNamespaceJobRecord,
        moved_object: &DfsObjectMetadata,
        old_path: &str,
        touch_parent: bool,
        source_parent_delta: i64,
    ) -> Result<()> {
        self.clear_metadata_lookup_cache();
        let old_path = normalize_posix_path(old_path)?;
        // Native adoption retires the original source before this frame runs.
        // Observe that exact committed effect on its known physical shard before
        // attempting to recapture a family whose live authority row is gone.
        let sources = self.namespace_shards_for_exact_paths(
            &moved_object.file_system_id,
            std::iter::once(old_path.as_str()),
        )?;
        if let [source] = sources.as_slice()
            && source.primary_node_id == self.node_id
            && let Some(backend) = self.metadata_backend.as_ref()
        {
            let shard_id = DfsMetadataShardId::new(source.shard_id.clone());
            let operation_id =
                source_eviction_operation_id(moved_object, &old_path, source_parent_delta)?;
            if let (_, Some(original)) = backend.prepared_operation_read_context(
                &shard_id,
                &moved_object.file_system_id,
                &operation_id,
            )? {
                original.validate()?;
                let payload = &original.payload;
                let exact_eviction = payload.shard_id == shard_id
                    && payload.file_system_id == moved_object.file_system_id
                    && payload.operation_id == operation_id
                    && payload.metadata_effects.iter().any(|effect| matches!(effect,
                        crate::DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction { tombstone, .. }
                            if tombstone.object_id == moved_object.object_id
                                && tombstone.file_system_id == moved_object.file_system_id
                                && crate::namespace::metadata_path(&tombstone.path) == crate::namespace::metadata_path(&old_path)
                                && matches!(&tombstone.tombstone_provenance,
                                    Some(crate::DfsTombstoneProvenance::SourceEviction { source_shard_id, .. })
                                        if *source_shard_id == shard_id)
                    ));
                if !exact_eviction {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: "recorded source retirement differs from its original move".into(),
                    }));
                }
                if backend
                    .committed_prepared_operation(&original.digest)?
                    .is_some()
                {
                    // Retirement was admitted only after the original target
                    // proof. A later destination generation cannot invalidate it.
                    self.apply_metadata_object_to_service_state(moved_object.clone())?;
                    self.clear_metadata_lookup_cache();
                    if touch_parent {
                        self.touch_parent_directory_if_local_primary(&old_path)?;
                    }
                    return Ok(());
                }
            }
        }
        // A parked saga may resume after the adoption advanced the record
        // beyond the frame snapshot; complete against the adopted state when
        // it is visible.
        let target_path = normalize_posix_path(&moved_object.path)?;
        let refreshed = self
            .lookup_metadata_path(&moved_object.file_system_id, &target_path)?
            .filter(|record| {
                record.object_id == moved_object.object_id
                    && record.kind != DfsObjectKind::Tombstone
            })
            .unwrap_or_else(|| moved_object.clone());
        let moved_object = &refreshed;
        let needs_tombstone = !matches!(
            self.lookup_metadata_path_raw(&moved_object.file_system_id, &old_path)?,
            Some(existing) if existing.kind == DfsObjectKind::Tombstone
        );
        let _namespace_plan = self.namespace_execution.begin_independent_planning();
        // Adoption can already hide the source from logical path lookups while
        // its physical shard row still awaits eviction. Cleanup compares that
        // source row, not the destination generation of the same object id.
        let source_shards = self.namespace_shards_for_exact_paths(
            &moved_object.file_system_id,
            std::iter::once(old_path.as_str()),
        )?;
        if let [source] = source_shards.as_slice()
            && source.primary_node_id != self.node_id
        {
            // Native adoption retires a remote source at its actual owner
            // before advancing this durable completion frame. On resume,
            // corroborate source invisibility and the exact finalized target;
            // the coordinator must never author a replica-side tombstone.
            let recorded = self.namespace_job(&job.job_id)?.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source retirement read has no original durable namespace job".into(),
                })
            })?;
            // The original job still reserves this source path. Its scoped
            // read checks both physical identity and path occupancy without
            // treating the coordinator as an unrelated public reader.
            let current = self.namespace_job_source_object(
                source,
                &recorded,
                &moved_object.object_id,
                &old_path,
            )?;
            if current
                .as_ref()
                .is_some_and(|object| object.tombstone_provenance.is_none())
            {
                return Err(DfsPosixError::Conflict {
                    path: old_path,
                    reason: "remote source authority has not completed the original retirement"
                        .into(),
                });
            }
            self.verify_cross_shard_adoption_finalized(moved_object, &old_path, Some(&recorded))?;
            self.apply_metadata_object_to_service_state(moved_object.clone())?;
            self.clear_metadata_lookup_cache();
            if touch_parent {
                self.touch_parent_directory_if_local_primary(&old_path)?;
            }
            return Ok(());
        }
        let source_object = if let Some(backend) = self.metadata_backend.as_ref() {
            if let [shard] = source_shards.as_slice() {
                backend.get_object_by_path_on_shard(
                    &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
                    &moved_object.file_system_id,
                    &old_path,
                )?
            } else {
                backend.get_object_by_path(&moved_object.file_system_id, &old_path)?
            }
        } else {
            self.lookup_metadata_path_raw(&moved_object.file_system_id, &old_path)?
        };
        self.namespace_execution.observe_path(
            &moved_object.file_system_id,
            &old_path,
            source_object.as_ref(),
        );
        if source_object.as_ref().is_some_and(|source| {
            source.kind != DfsObjectKind::Tombstone && source.object_id != moved_object.object_id
        }) {
            return Err(DfsPosixError::Conflict {
                path: old_path,
                reason: "cross-shard source path was replaced before cleanup".into(),
            });
        }
        if needs_tombstone {
            let mut old_path_tombstone = moved_object.clone();
            old_path_tombstone.object_id = DfsObjectId::generate();
            let old_path_tombstone =
                self.tombstone_path_object_at(old_path_tombstone, old_path.clone());
            self.apply_namespace_transaction(DfsNamespaceTransaction {
                extension_expectations: Vec::new(),
                contribution_digest: None,
                content_reference_identity: None,
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_versioned_path(
                    "rename-cross-shard",
                    &moved_object.object_id,
                    &moved_object.version,
                    &old_path,
                ),
                file_system_id: moved_object.file_system_id.clone(),
                objects: vec![old_path_tombstone],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::now_utc(),
            })?;
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            let family =
                backend.capture_cross_shard_source_family(&moved_object.object_id, &old_path)?;
            self.publish_cross_shard_source_eviction(
                moved_object,
                &old_path,
                &family,
                None,
                source_parent_delta,
            )?;
        }
        self.clear_metadata_lookup_cache();
        self.apply_metadata_object_to_service_state(moved_object.clone())?;
        self.clear_metadata_lookup_cache();
        if touch_parent {
            self.touch_parent_directory_if_local_primary(&old_path)?;
        }
        Ok(())
    }

    /// An old target-id-only frame cannot establish an exact count successor.
    /// Retain its original job for explicit migration rather than re-increment.
    fn complete_cross_shard_hardlink_locally(
        &mut self,
        _target_object_id: &DfsObjectId,
    ) -> Result<()> {
        Err(DfsPosixError::Dfs(DfsError::Metadata {
            message: "legacy hardlink completion has no exact count successor".into(),
        }))
    }

    /// Atomically installs an object record moved here from another primary's
    /// shard, validating destination preconditions in the same step.
    /// Idempotent against re-sent adoptions.
    pub(crate) fn publish_cross_shard_source_eviction(
        &mut self,
        moved: &DfsObjectMetadata,
        old_path: &str,
        family: &crate::DfsCrossShardSourceFamily,
        transfer_id: Option<&str>,
        source_parent_delta: i64,
    ) -> Result<()> {
        let source_scope = self.metadata_effect_scope_for_object(
            &family.source_object,
            source_eviction_operation_id(moved, old_path, source_parent_delta)?,
        )?;
        let _execution = self.acquire_prepared_execution(&source_scope)?;
        if let Some(original) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source retirement has no backend".into(),
                })
            })?
            .prepared_operation_record(&source_scope.operation_id)?
        {
            original.validate()?;
            self.publish_metadata_effect_payload(original.payload.as_ref().clone())?;
            return Ok(());
        }
        let target_shard = match self.select_metadata_shard_for_object(moved)? {
            Some(shard) => crate::DfsMetadataShardId::new(shard.shard_id),
            None if self.control_plane.is_none() => self.local_prepared_operation_shard()?,
            None => {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source retirement has no exact current destination assignment".into(),
                }));
            }
        };
        let plan = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "source retirement requires the current durable backend".into(),
                })
            })?
            .plan_foreign_shard_source_eviction(
                moved,
                old_path,
                family,
                transfer_id,
                &target_shard,
            )?;
        if plan.physical_source_shard != source_scope.shard_id {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: "source retirement planner changes its physical authority scope".into(),
            }));
        }
        if plan.effects.is_empty() {
            return Ok(());
        }
        let mut payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            source_scope,
            plan.expected_objects,
            plan.expected_paths,
            plan.effects,
            time::OffsetDateTime::now_utc(),
        )?;
        if source_parent_delta != 0 && family.source_object.kind == DfsObjectKind::Directory {
            if let Some((before, successor)) =
                self.counted_parent_successor(old_path, source_parent_delta)?
            {
                payload
                    .expected_object_versions
                    .push(DfsNamespaceObjectVersionExpectation {
                        object_id: before.object_id.clone(),
                        expected_version: Some(before.version),
                    });
                payload
                    .expected_path_occupants
                    .push(DfsNamespacePathOccupantExpectation {
                        path: before.path,
                        expected_object_id: Some(before.object_id),
                    });
                payload.transaction.objects.push(successor);
                payload.normalize();
            }
        }
        payload.expected_journal_sequence = Some(plan.expected_journal_sequence);
        self.publish_metadata_effect_payload(payload)?;
        Ok(())
    }

    /// Publish the complete current object family on its destination authority.
    /// Exact retries retain later destination generations.
    pub(crate) fn adopt_object_at_path_local(
        &mut self,
        object: &DfsObjectMetadata,
        source_path: &str,
        pin_transfer_id: Option<&str>,
        versions: &[DfsObjectVersionRecord],
        version_pins: &[DfsMetadataVersionPinRecord],
        version_pin_set_fingerprint: Option<&str>,
        receipts: &[DfsReplicaReceiptRecord],
        intents: &[crate::DfsWriteIntentRecord],
        intent_receipts: &[DfsWriteIntentReceipt],
        intent_receipt_retention: &DfsWriteIntentReceiptRetentionState,
        content_reference_outcomes: &[crate::DfsNamespaceTransactionReceipt],
        content_reference_outcome_retention: Option<
            &crate::metadata_records::DfsContentReferenceOutcomeRetention,
        >,
        advisory_lock_state: Option<&crate::DfsAdvisoryLockStateRecord>,
    ) -> Result<bool> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = normalize_posix_path(&object.path)?;
        self.admit_directory_move_adoption(&object.file_system_id, &path)?;
        let mut destination_reservation = None;
        let directory_root_plan = if object.kind == DfsObjectKind::Directory {
            let offer = self.source_adoption.offer().ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "counted directory adoption has no original source offer".into(),
                })
            })?;
            let owners = self.publication_execution.current().ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "counted directory adoption has no actual execution owner".into(),
                })
            })?;
            let actor = crate::source_adoption::actor_for_root(owners.data_root())?;
            let job = actor.bound_original_job(&offer, source_path)?;
            let target_root = job.target_path.as_ref().ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "original directory job omitted its target root".into(),
                })
            })?;
            if job.kind != DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                || !posix_path_is_at_or_under_prefix(&path, target_root)
            {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "directory count adoption has no original tree-move scope".into(),
                }));
            }
            destination_reservation =
                Self::directory_move_destination_reservation_for_root(&job, &path)?;
            // Descendants materialize an already-counted transported subtree.
            // Only its root changes membership of an external parent.
            let plan = job
                .directory_move_counts()?
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "directory adoption omitted original count plan".into(),
                    })
                })?
                .clone();
            if let Some(participants) = &plan.parent_participants {
                participants.validate_for_job(&job)?;
                if participants.phase != crate::DfsParentCountPhase::Issued {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: "directory parent reservation has no bound issuance decision".into(),
                    }));
                }
            }
            (path == *target_root).then_some(plan)
        } else {
            None
        };
        let target_parent_delta = directory_root_plan
            .as_ref()
            .map_or(0, |plan| plan.target_effect_delta(&path));
        if self.metadata_backend.is_some() {
            let destination_shards = self.namespace_shards_for_exact_paths(
                &object.file_system_id,
                std::iter::once(path.as_str()),
            )?;
            let backend = self
                .metadata_backend
                .as_ref()
                .expect("backend checked above");
            let physical_destination = if let [shard] = destination_shards.as_slice() {
                backend.get_object_on_shard(
                    &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
                    &object.object_id,
                )?
            } else {
                backend.get_object(&object.object_id)?
            };
            if let Some(eviction) = physical_destination.as_ref()
                && eviction.kind == DfsObjectKind::Tombstone
                && matches!(
                    eviction.tombstone_provenance,
                    Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
                )
            {
                // A returning object can replace its old physical source
                // eviction even though logical path lookup hides that row.
                // Keep the exact destination-shard head as a writer-checked
                // predicate; the destination path remains independently read.
                self.namespace_execution.observe_object(
                    &object.file_system_id,
                    &object.object_id,
                    Some(eviction),
                );
                let previous_path_occupant = if let [shard] = destination_shards.as_slice() {
                    backend.get_object_by_path_on_shard(
                        &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
                        &object.file_system_id,
                        &eviction.path,
                    )?
                } else {
                    backend.get_object_by_path(&object.file_system_id, &eviction.path)?
                };
                // The old path may index the separate tombstone created by
                // source cleanup, rather than the original object's eviction.
                // The prepare footprint checks that actual occupant too; bind
                // its original head and path, or its actual physical absence.
                self.namespace_execution.observe_path(
                    &object.file_system_id,
                    &eviction.path,
                    previous_path_occupant.as_ref(),
                );
            }
        }
        if !content_reference_outcomes.is_empty()
            || content_reference_outcome_retention.is_some()
            || versions.iter().any(|version| {
                !version.retention.is_empty()
                    || !version.module_metadata.is_empty()
                    || version.content_durability.is_some()
            })
        {
            self.require_file_history_shard_support(&object.file_system_id, &path)
                .map_err(DfsPosixError::from)?;
        }
        let pin_transfer_id = pin_transfer_id.filter(|id| !id.is_empty()).ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Distributed {
                message: "current object-family adoption requires its exact transfer identity"
                    .into(),
            })
        })?;
        let pin_transfer_id = Some(pin_transfer_id);
        let fingerprint = version_pin_set_fingerprint.filter(|value| !value.is_empty()).ok_or_else(|| DfsPosixError::Dfs(DfsError::Distributed { message: "current object-family adoption requires its complete source pin-set fingerprint".into() }))?;
        let supplied_version_pin_set_fingerprint = Some(fingerprint.to_owned());
        let canonical_version_pin_set_fingerprint =
            crate::protocol::cross_shard_version_pin_set_fingerprint(object, version_pins)
                .map_err(DfsPosixError::from)?;
        if supplied_version_pin_set_fingerprint
            .as_ref()
            .is_some_and(|supplied| supplied != &canonical_version_pin_set_fingerprint)
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "cross-shard adoption of {} supplied a mismatched durable version-pin fingerprint",
                    object.object_id
                ),
            }));
        }
        let transfer = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "current object-family adoption requires a durable backend".into(),
                })
            })?
            .plan_cross_shard_pin_transfer(
                pin_transfer_id.expect("validated current transfer identity"),
                object,
                source_path,
                versions,
                version_pins,
            )?;
        let pin_transfer_active = transfer.active;
        let mut family_effects = transfer.effects;
        let mut transaction_objects = Vec::new();
        let mut already_adopted = false;
        let destination_before = self.lookup_sifs_path(&path)?;
        if let Some(destination) = destination_before.clone() {
            if destination.object_id == object.object_id && destination.kind == object.kind {
                if destination.kind == DfsObjectKind::Hardlink {
                    if destination.namespace_link_count.is_some() {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: "adopted alias owns an invalid canonical count".into(),
                        }));
                    }
                } else {
                    super::metadata::updated_namespace_link_count(&destination, 0)?;
                    if destination.version == object.version
                        && destination.namespace_link_count != object.namespace_link_count
                    {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message:
                                "same adopted generation changed its canonical namespace count"
                                    .into(),
                        }));
                    }
                }
                let adopted_version_is_ancestor = if destination.version == object.version {
                    true
                } else if let Some(backend) = self.metadata_backend.as_ref() {
                    backend
                        .get_version(&object.object_id, &object.version)?
                        .is_some()
                } else {
                    false
                };
                if adopted_version_is_ancestor {
                    // A reply can be lost after adoption and before the
                    // source observes it. If the destination subsequently
                    // advances the same object, the migrated version remains
                    // in its history and proves that this is an exact retry;
                    // never roll the newer visible object back to the frame.
                    already_adopted = true;
                } else {
                    return Err(DfsPosixError::Conflict {
                        path: path.clone(),
                        reason: format!(
                            "destination object {} at {path} has unrelated version {} while adopting {}",
                            destination.object_id, destination.version, object.version
                        ),
                    });
                }
            } else if destination.kind != DfsObjectKind::Tombstone {
                match (&object.kind, &destination.kind) {
                    // A moved directory may replace an empty directory, like
                    // a local rename over an empty destination directory.
                    (DfsObjectKind::Directory, DfsObjectKind::Directory) => {
                        if destination.object_id != object.object_id
                            && self.sifs_dir_has_children(&path)?
                        {
                            return Err(DfsPosixError::DirectoryNotEmpty { path });
                        }
                    }
                    (DfsObjectKind::Directory, _) => {
                        return Err(DfsPosixError::NotDirectory { path });
                    }
                    (_, DfsObjectKind::Directory) => {
                        return Err(DfsPosixError::IsDirectory { path });
                    }
                    (_, DfsObjectKind::Hardlink)
                        if destination.link_target_object_id.as_ref()
                            == Some(&object.object_id) =>
                    {
                        // Hardlink promotion: the moved object replaces its
                        // own alias at this path.
                    }
                    (_, DfsObjectKind::Hardlink) => {
                        return Err(DfsPosixError::InvalidPath {
                            path: format!(
                                "{path} is a hardlink to another object; cross-shard replace is not supported"
                            ),
                        });
                    }
                    _ => {
                        if super::metadata::updated_namespace_link_count(&destination, 0)? > 1 {
                            return Err(DfsPosixError::InvalidPath {
                                path: format!(
                                    "{path} has hardlink aliases; cross-shard replace is not supported"
                                ),
                            });
                        }
                    }
                }
                let destination_tombstone = if destination.kind == DfsObjectKind::Hardlink
                    && destination.link_target_object_id.as_ref() == Some(&object.object_id)
                {
                    // Promotion replaces the alias at this path with its
                    // canonical record. Park the alias under a hidden
                    // tombstone key so it cannot remain a second live link at
                    // the newly canonical path.
                    let hidden_path = self.hidden_alias_tombstone_path_on_destination_shard(
                        &path,
                        &destination.object_id,
                    )?;
                    self.tombstone_path_object_at(destination.clone(), hidden_path)
                } else {
                    self.tombstone_path_object(destination)
                };
                transaction_objects.push(destination_tombstone);
            }
        }
        let rebound_advisory_lock_state = advisory_lock_state
            .map(crate::advisory_lock::prepare_advisory_lock_state_for_migration)
            .transpose()
            .map_err(|source| {
                DfsPosixError::Dfs(DfsError::Serialization {
                    message: format!(
                        "encode migrated advisory-lock state for {}: {source}",
                        object.object_id
                    ),
                })
            })?
            .map(|(state, _)| state);
        if let Some(backend) = self.metadata_backend.as_ref() {
            for version in versions {
                if backend
                    .get_version(&version.object_id, &version.version)?
                    .is_none()
                {
                    family_effects.push(crate::DfsPreparedMetadataEffect::PutVersion {
                        version: version.clone(),
                    });
                }
            }
        }
        for receipt in receipts {
            let exists = self
                .backend_receipt_for_replica(
                    &receipt.receipt.object_id,
                    &receipt.receipt.committed_version,
                    &receipt.replica_node_id,
                )?
                .is_some();
            if !exists {
                // Receipt-slot generations belong to the authority that
                // publishes the slot. Adopted evidence enters at this shard's
                // current slot generation; the authority change already
                // fences replies planned against the source shard.
                let mut adopted = receipt.clone();
                adopted.generation = self.backend_receipt_slot_generation(
                    &receipt.receipt.object_id,
                    &receipt.receipt.committed_version,
                    &receipt.replica_node_id,
                )?;
                family_effects
                    .push(crate::DfsPreparedMetadataEffect::PutReceipt { receipt: adopted });
            }
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            family_effects.extend(content_reference_outcomes.iter().cloned().map(|receipt| {
                crate::DfsPreparedMetadataEffect::PutNamespaceTransactionReceipt { receipt }
            }));
            if let Some(retention) = content_reference_outcome_retention {
                family_effects.push(
                    crate::DfsPreparedMetadataEffect::MergeContentReferenceOutcomeRetention {
                        object_id: object.object_id.clone(),
                        retention: retention.clone(),
                    },
                );
            }
            for intent in intents {
                if let Some(receipt) =
                    backend.get_intent_receipt(&intent.object_id, &intent.intent_id)?
                {
                    if !receipt.matches_intent(intent) {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "destination has conflicting compact intent receipt {}/{} during cross-shard adoption",
                                intent.object_id, intent.intent_id
                            ),
                        }));
                    }
                    continue;
                }
                match backend.get_intent(&intent.object_id, &intent.intent_id)? {
                    Some(existing) if existing != *intent => {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "destination has conflicting full intent {}/{} during cross-shard adoption",
                                intent.object_id, intent.intent_id
                            ),
                        }));
                    }
                    Some(_) => {}
                    None => {
                        family_effects.push(crate::DfsPreparedMetadataEffect::PutIntent {
                            intent: intent.clone(),
                        });
                    }
                }
            }
            for receipt in intent_receipts {
                match backend.get_intent_receipt(&receipt.object_id, &receipt.intent_id)? {
                    Some(existing) if existing != *receipt => {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "destination has conflicting compact intent receipt {}/{} during cross-shard adoption",
                                receipt.object_id, receipt.intent_id
                            ),
                        }));
                    }
                    Some(_) => {}
                    None => {
                        if let Some(existing) =
                            backend.get_intent(&receipt.object_id, &receipt.intent_id)?
                            && !receipt.matches_intent(&existing)
                        {
                            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                                message: format!(
                                    "destination has conflicting full intent {}/{} during cross-shard adoption",
                                    receipt.object_id, receipt.intent_id
                                ),
                            }));
                        }
                        family_effects.push(crate::DfsPreparedMetadataEffect::PutIntentReceipt {
                            receipt: receipt.clone(),
                        });
                    }
                }
            }
            if !intent_receipt_retention.is_empty() {
                family_effects.push(
                    crate::DfsPreparedMetadataEffect::MergeIntentReceiptRetention {
                        retention: intent_receipt_retention.clone(),
                    },
                );
            }
        }
        if !already_adopted {
            if let Some(plan) = &directory_root_plan {
                let matches = match &plan.expected_destination {
                    crate::DfsNamespaceDestinationPrecondition::Absent => destination_before
                        .as_ref()
                        .is_none_or(|row| row.kind == DfsObjectKind::Tombstone),
                    crate::DfsNamespaceDestinationPrecondition::Exact(expected) => {
                        destination_before.as_ref().is_some_and(|row| {
                            row.kind == DfsObjectKind::Directory
                                && row.object_id == expected.object_id
                                && row.version == expected.version
                        })
                    }
                };
                if !matches {
                    return Err(DfsPosixError::Conflict {
                        path: path.clone(),
                        reason: "directory adoption destination changed its original count plan"
                            .into(),
                    });
                }
            }
            if !directory_root_plan.as_ref().is_some_and(|plan| plan.parent_is_participant(&path)) {
                self.ensure_parent_directory(&path)?;
            }
            // Otherwise the actual bound Issued job proves its exact parent
            // was reserved before any adoption. Its write reservation cannot
            // abort after issuance or apply until these child effects finish.
            // Do not fabricate a target-local parent row or read-set entry.
            transaction_objects.push(object.clone());
            if target_parent_delta != 0 {
                let parent_path = crate::posix_path::parent_posix_path(&path)
                    .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
                let transaction_id = namespace_transaction_id_for_versioned_path(
                    "adopt-cross-shard",
                    &object.object_id,
                    &object.version,
                    &path,
                );
                let parent = match self.recorded_adoption_parent_successor(
                    &transaction_id,
                    object,
                    &parent_path,
                )? {
                    Some(parent) => parent,
                    None => {
                        self.counted_parent_successor(&path, target_parent_delta)?
                            .ok_or_else(|| DfsPosixError::MissingParent {
                                path: parent_path.clone(),
                            })?
                            .1
                    }
                };
                transaction_objects.push(parent);
            }
            // The root's adoption reserves the destination subtree in the same
            // transaction; an exact retry finds it installed with the root.
            family_effects.extend(destination_reservation.take().map(|job| {
                crate::DfsPreparedMetadataEffect::PutNamespaceJob { job }
            }));
            let mut visible_version = cross_shard_object_version_record(object);
            visible_version.content_durability = versions
                .iter()
                .find(|version| {
                    version.object_id == object.object_id && version.version == object.version
                })
                .and_then(|version| version.content_durability.clone());
            self.apply_namespace_transaction_with_metadata_effects(
                DfsNamespaceTransaction {
                    extension_expectations: Vec::new(),
                    contribution_digest: None,
                    content_reference_identity: None,
                    file_version_pins: None,
                    transaction_id: namespace_transaction_id_for_versioned_path(
                        "adopt-cross-shard",
                        &object.object_id,
                        &object.version,
                        &path,
                    ),
                    file_system_id: object.file_system_id.clone(),
                    objects: transaction_objects,
                    versions: vec![visible_version],
                    advisory_lock_states: rebound_advisory_lock_state.iter().cloned().collect(),
                    created_at: time::OffsetDateTime::now_utc(),
                },
                family_effects,
            )?;
        } else if !family_effects.is_empty() {
            // Actual current retries retain the newer visible head. Family
            // reconciliation cannot restore the older destination frame.
            let scope = self.metadata_effect_scope_for_object(
                object,
                format!(
                    "adoption-family:{}:{}",
                    pin_transfer_id.expect("validated current transfer identity"),
                    sinomo_core::Id::generate()
                ),
            )?;
            self.publish_captured_object_metadata_effects(object, scope, family_effects)?;
        }
        if !already_adopted
            && target_parent_delta == 0
            && !directory_root_plan
                .as_ref()
                .is_some_and(|plan| plan.parent_is_participant(&path))
        {
            self.touch_parent_directory_if_local_primary(&path)?;
        }
        Ok(pin_transfer_active)
    }

    /// Atomically installs a hardlink alias created by another primary's
    /// coordinator. Idempotent against re-sent adoptions.
    pub(crate) fn adopt_hardlink_alias_local(&mut self, alias: &DfsObjectMetadata) -> Result<()> {
        let path = normalize_posix_path(&alias.path)?;
        if let Some(existing) = self.lookup_sifs_path(&path)? {
            if existing.object_id == alias.object_id && existing.kind == DfsObjectKind::Hardlink {
                return Ok(());
            }
            if existing.kind != DfsObjectKind::Tombstone {
                return Err(DfsPosixError::AlreadyExists { path });
            }
        }
        self.ensure_parent_directory(&path)?;
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            extension_expectations: Vec::new(),
            contribution_digest: None,
            content_reference_identity: None,
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_versioned_path(
                "adopt-hardlink-cross-shard",
                &alias.object_id,
                &alias.version,
                &path,
            ),
            file_system_id: alias.file_system_id.clone(),
            objects: vec![alias.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_if_local_primary(&path)?;
        Ok(())
    }

    pub(crate) fn resume_running_cross_shard_namespace_job_for_paths(
        &mut self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
    ) -> Result<bool> {
        let Some(job) = self.running_cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            target_path,
            kinds,
        )?
        else {
            return Ok(false);
        };
        self.drive_cross_shard_namespace_job(job)?;
        Ok(true)
    }

    /// Continue the exact saga owned by the active outer request before its
    /// own path reservation or changed CAS generation can reject its retry.
    pub(crate) fn resume_or_replay_cross_shard_mutation_for_active_request(
        &mut self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: Option<&str>,
        kinds: &[DfsNamespaceJobKind],
    ) -> Result<bool> {
        let Some(request) = self.namespace_execution.request().cloned() else {
            return Ok(false);
        };
        let origin = DfsNamespaceJobOrigin::from_request(&request);
        if let Some(job) = self.cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            target_path,
            kinds,
            DfsNamespaceJobStatus::Running,
            Some(request.created_at),
            Some(&origin),
        )? {
            self.drive_cross_shard_namespace_job(job)?;
            return Ok(true);
        }
        if let Some(target_path) = target_path {
            return self.completed_cross_shard_namespace_job_for_paths_since(
                file_system_id,
                root_path,
                target_path,
                kinds,
                request.created_at,
                &origin,
            );
        }
        // Pending outer requests retain their complete terminal job through
        // ordinary GC. Canonical unlink's destination belongs to that exact
        // job because its promoted alias was discovered during planning.
        self.cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            None,
            kinds,
            DfsNamespaceJobStatus::Completed,
            Some(request.created_at),
            Some(&origin),
        )
        .map(|job| job.is_some())
    }

    fn running_cross_shard_namespace_job_for_paths(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
    ) -> Result<Option<DfsNamespaceJobRecord>> {
        self.cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            Some(target_path),
            kinds,
            DfsNamespaceJobStatus::Running,
            None,
            None,
        )
    }

    pub(crate) fn completed_cross_shard_namespace_job_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<bool> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let root_path = match normalize_posix_path(root_path) {
            Ok(path) => path,
            Err(_) => return Ok(false),
        };
        let target_path = match normalize_posix_path(target_path) {
            Ok(path) => path,
            Err(_) => return Ok(false),
        };
        if backend
            .completed_namespace_job_receipt_for_paths_since(
                file_system_id,
                &root_path,
                &target_path,
                kinds,
                created_at,
                origin,
            )?
            .is_some()
        {
            return Ok(true);
        }
        self.cross_shard_namespace_job_for_paths(
            file_system_id,
            &root_path,
            Some(&target_path),
            kinds,
            DfsNamespaceJobStatus::Completed,
            Some(created_at),
            Some(origin),
        )
        .map(|job| job.is_some())
    }

    fn cross_shard_namespace_job_for_paths(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: Option<&str>,
        kinds: &[DfsNamespaceJobKind],
        status: DfsNamespaceJobStatus,
        created_at_not_before: Option<time::OffsetDateTime>,
        origin: Option<&DfsNamespaceJobOrigin>,
    ) -> Result<Option<DfsNamespaceJobRecord>> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let root_path = match normalize_posix_path(root_path) {
            Ok(path) => path,
            Err(_) => return Ok(None),
        };
        let target_path = match target_path.map(normalize_posix_path).transpose() {
            Ok(path) => path,
            Err(_) => return Ok(None),
        };
        let mut existing_job = None;
        try_walk_metadata_pages(|cursor| {
            let page = backend
                .list_namespace_jobs_page(cursor, POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE)?;
            for job in page.jobs {
                if job.file_system_id != *file_system_id
                    || job.status != status
                    || !kinds.contains(&job.kind)
                    || created_at_not_before.is_some_and(|created_at| job.created_at < created_at)
                    || origin.is_some_and(|expected| job.origin.as_ref() != Some(expected))
                {
                    continue;
                }
                let Ok(job_root_path) = normalize_posix_path(&job.root_path) else {
                    continue;
                };
                if job_root_path != root_path {
                    continue;
                }
                let Some(job_target_path) = job.target_path.as_ref() else {
                    continue;
                };
                let Ok(job_target_path) = normalize_posix_path(job_target_path) else {
                    continue;
                };
                if target_path
                    .as_ref()
                    .is_none_or(|expected| &job_target_path == expected)
                {
                    existing_job = Some(job);
                    return Ok(None);
                }
            }
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        Ok(existing_job)
    }

    /// Reject object-scoped coordination while a durable cross-shard move can
    /// still copy or evict that object's metadata family. Unlike the in-memory
    /// path guard, this survives a coordinator restart before job recovery has
    /// rebuilt its transient lock set.
    pub(super) fn ensure_no_running_cross_shard_move_for_path(&self, path: &str) -> Result<()> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let path = self.resolve_posix_dot_components(path)?;
        let mut blocked = false;
        try_walk_metadata_pages(|cursor| {
            let page = backend
                .list_namespace_jobs_page(cursor, POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE)?;
            for job in &page.jobs {
                if job.file_system_id != sifs_file_system_id()
                    || job.status != DfsNamespaceJobStatus::Running
                    || !matches!(
                        job.kind,
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                    )
                {
                    continue;
                }
                let Ok(root_path) = normalize_posix_path(&job.root_path) else {
                    continue;
                };
                let target_path = job
                    .target_path
                    .as_deref()
                    .and_then(|target| normalize_posix_path(target).ok());
                if posix_path_is_at_or_under_prefix(&path, &root_path)
                    || posix_path_is_at_or_under_prefix(&root_path, &path)
                    || target_path.as_ref().is_some_and(|target_path| {
                        posix_path_is_at_or_under_prefix(&path, target_path)
                            || posix_path_is_at_or_under_prefix(target_path, &path)
                    })
                {
                    blocked = true;
                    return Ok(None);
                }
            }
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        if blocked {
            return Err(DfsPosixError::Dfs(DfsError::Busy { path }));
        }
        Ok(())
    }
}
