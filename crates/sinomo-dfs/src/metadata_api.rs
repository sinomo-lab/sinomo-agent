//! Metadata backend API surface.
//!
//! This module owns the public metadata backend contract and default scan
//! helpers. Concrete local storage lives in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::node::NodeId;

use crate::advisory_lock::{
    DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord, DfsAdvisoryLockTransitionContext,
};
use crate::content_root_catalog::{
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest,
};
use crate::control_plane::DfsMetadataShardConfig;
use crate::intent::write_intent_status_is_active;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::{object_is_hardlink_to_target, sort_hardlinks_by_path};
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
};
use crate::metadata_journal::bounded_metadata_journal_batch;
use crate::metadata_keys::{normalize_posix_path, write_intent_page_cursor};
use crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE;
use crate::metadata_pages::{
    collect_filtered_cursor_page, collect_filtered_metadata_pages, collect_metadata_pages,
    walk_metadata_pages,
};
use crate::metadata_records::*;
use crate::metadata_summary::count_pending_repairs_for_object;
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

/// Durable shard-local freeze installed while a same-primary membership
/// handoff carries the retiring authority's exact state.
///
/// This bounded marker is local coordination state, not file metadata. A
/// pending marker rejects every authoritative mutation. After activation, only
/// the backend instance explicitly rebound to the marker's exact active epoch
/// may mutate; independently opened stale instances remain fenced. Read,
/// snapshot, prefix-proof, and replication-cursor operations remain usable.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardWriteFreeze {
    pub handoff_id: String,
    pub retiring_epoch: u64,
    /// Active authority generation installed after the handoff commits.
    ///
    /// `None` is a pending write freeze. `Some(epoch)` remains durable after
    /// activation so already-open backend instances bound to the retiring
    /// generation cannot resume writes merely because another process
    /// completed the handoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activated_epoch: Option<u64>,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
    /// Exact durable descendant from which a freshly opened lane can resume
    /// validating the prefix after older journal segments are checkpointed.
    ///
    /// New markers start at the handoff prefix. Checkpoint publication
    /// advances this proof before pruning the journal history needed to
    /// reconstruct it. Current markers always carry the exact proof.
    pub rebind_prefix_proof: crate::DfsMetadataPrefixProof,
}

/// Capability for one replica ingest from an already validated active source.
///
/// The metadata replication boundary mints this only after matching the
/// request's filesystem, shard, source node, and authority epoch to the exact
/// committed shard assignment. Before taking its write lock, the durable
/// backend rechecks the filesystem on every journal payload or on the complete
/// canonical snapshot state. It then rechecks the shard and, when an author
/// fence exists, the activated epoch while holding its write lock. Keeping
/// construction crate-private prevents external backend callers from bypassing
/// that source-validation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataReplicaIngestPermit {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    source_node_id: NodeId,
    source_epoch: u64,
    pending_primary_handoff_id: Option<String>,
}

/// Verify the immutable original preparation independently of the current cut.
/// A digest-only eviction or a revoked body cannot supply publication evidence.
pub(crate) fn validated_metadata_authority_operation_digest(
    record: &DfsPreparedOperationRecord,
    operation: &crate::DfsMetadataAuthorityOperation,
) -> Result<DfsPreparedOperationDigest> {
    record.validate()?;
    let proof = &record.digest;
    if !matches!(
        record.status,
        DfsPreparedOperationStatus::Prepared | DfsPreparedOperationStatus::Committed
    ) || !matches!(operation, crate::DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id, file_system_id, shard_id, owner_epoch, prepare_sequence, payload_digest
        } if operation_id == &proof.operation_id && file_system_id == &proof.file_system_id
            && shard_id == &proof.shard_id && *owner_epoch == proof.owner_epoch
            && *prepare_sequence == proof.prepare_sequence && payload_digest == &proof.payload_digest)
    {
        return Err(MetadataBackendError::Serde(
            "authority operation differs from its complete live preparation".into(),
        ));
    }
    Ok(proof.clone())
}

/// Result of one replica journal apply plus any proof computed from entries
/// that became durable during that exact call.
#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsMetadataReplicaApplyReport {
    applied_entries: usize,
    namespace_authority_proof: Option<DfsPreparedOperationDigest>,
    durable_prefix_proof: Option<DfsMetadataPrefixProof>,
    materialization_pending: bool,
}

impl DfsMetadataReplicaApplyReport {
    pub(crate) fn with_durable_ingest(
        applied_entries: usize,
        namespace_authority_proof: Option<DfsPreparedOperationDigest>,
        durable_prefix_proof: DfsMetadataPrefixProof,
        materialization_pending: bool,
    ) -> Self {
        Self {
            applied_entries,
            namespace_authority_proof,
            durable_prefix_proof: Some(durable_prefix_proof),
            materialization_pending,
        }
    }

    pub(crate) fn applied_entries(&self) -> usize {
        self.applied_entries
    }

    pub(crate) fn durable_prefix_proof(&self) -> Option<&DfsMetadataPrefixProof> {
        self.durable_prefix_proof.as_ref()
    }

    pub(crate) fn materialization_pending(&self) -> bool {
        self.materialization_pending
    }

    pub(crate) fn namespace_authority_proof_for(
        &self,
        shard: &DfsMetadataShardConfig,
        operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Option<&DfsPreparedOperationDigest> {
        self.namespace_authority_proof
            .as_ref()
            .filter(|proof| matches!(operation, crate::DfsMetadataAuthorityOperation::PreparedOperation {operation_id,file_system_id,shard_id,owner_epoch,prepare_sequence,payload_digest} if operation_id==&proof.operation_id && file_system_id==&proof.file_system_id && shard_id==&proof.shard_id && *owner_epoch==proof.owner_epoch && *prepare_sequence==proof.prepare_sequence && payload_digest==&proof.payload_digest && proof.file_system_id==shard.file_system_id && proof.shard_id.0==shard.shard_id && proof.owner_epoch==shard.epoch))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataHistoryEffectsPlan {
    pub report: DfsMetadataHistoryGcReport,
    pub effects_by_scope:
        BTreeMap<(DfsFileSystemId, DfsMetadataShardId), Vec<crate::DfsPreparedMetadataEffect>>,
}

/// One bounded durable view used to admit an idempotent namespace request.
///
/// Admission needs several related indexes. Reading each index through an
/// independently refreshed shard snapshot can decode the same checkpoint
/// repeatedly and, more importantly, can observe different durable
/// generations. Durable sharded backends return all evidence from one stable
/// table image per retained shard.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsNamespaceRequestAdmissionSnapshot {
    pub request: Option<DfsNamespaceRequestRecord>,
    pub receipt: Option<DfsNamespaceRequestReceipt>,
    pub prepared: Option<DfsPreparedOperationRecord>,
    pub history_expired: bool,
}

/// Applied outcomes and hidden reservations from one exact physical table view.
/// A read must not combine pre-commit absence with post-commit reservation
/// removal and then incorrectly certify that an operation was never applied.
#[derive(Clone, Debug)]
pub struct DfsWriteOperationAdmissionSnapshot {
    pub intent: Option<DfsWriteIntentRecord>,
    pub target_size_bytes: Option<u64>,
    pub prepared: Vec<DfsPreparedOperationRecord>,
    pub retention: DfsWriteIntentReceiptRetentionState,
}

impl DfsMetadataReplicaIngestPermit {
    /// Mint evidence only after the caller has validated all four values
    /// against the same exact committed shard assignment.
    pub(crate) fn new_validated(
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        source_node_id: NodeId,
        source_epoch: u64,
    ) -> Self {
        Self {
            file_system_id,
            shard_id,
            source_node_id,
            source_epoch,
            pending_primary_handoff_id: None,
        }
    }

    /// Mint an exact, proof-bound repair permit for the promoted replica of a
    /// still-pending primary handoff. The caller must validate the committed
    /// retiring assignment, durable fence, successor identity, and snapshot
    /// checkpoint before constructing this capability.
    pub(crate) fn new_validated_pending_primary_handoff(
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        source_node_id: NodeId,
        source_epoch: u64,
        handoff_id: String,
    ) -> Self {
        Self {
            file_system_id,
            shard_id,
            source_node_id,
            source_epoch,
            pending_primary_handoff_id: Some(handoff_id),
        }
    }

    pub(crate) fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }

    pub(crate) fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub(crate) fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    pub(crate) fn source_epoch(&self) -> u64 {
        self.source_epoch
    }

    pub(crate) fn pending_primary_handoff_id(&self) -> Option<&str> {
        self.pending_primary_handoff_id.as_deref()
    }

    pub(crate) fn admits_write_freeze(&self, freeze: &DfsMetadataShardWriteFreeze) -> bool {
        freeze
            .activated_epoch
            .is_some_and(|activated_epoch| self.source_epoch >= activated_epoch)
            || (freeze.activated_epoch.is_none()
                && self.pending_primary_handoff_id() == Some(freeze.handoff_id.as_str())
                && self.source_epoch == freeze.retiring_epoch)
    }
}

/// One object's slice of the global `(object, version)` scan, read through
/// the object-local version index. Scan cursors keep their global meaning, so
/// a cursor for an earlier object restarts this object and a later one ends it.
fn object_version_records_page<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
    after: Option<&DfsObjectVersionScanCursor>,
    max_records: usize,
) -> Result<DfsObjectVersionScanPage> {
    let after = match after {
        Some(after) if &after.object_id == object_id => Some(DfsObjectVersionPageCursor {
            version: after.version.clone(),
            shard_id: None,
        }),
        Some(after) if &after.object_id > object_id => {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        _ => None,
    };
    let page = backend.versions_for_object_page(object_id, after.as_ref(), max_records)?;
    Ok(DfsObjectVersionScanPage {
        versions: page.versions,
        next_cursor: page.next_cursor.map(|cursor| DfsObjectVersionScanCursor {
            object_id: object_id.clone(),
            version: cursor.version,
        }),
    })
}

pub(crate) fn namespace_prepare_expectations_from_lookups(
    transaction: &DfsNamespaceTransaction,
    mut object_lookup: impl FnMut(&DfsObjectId) -> Result<Option<DfsObjectMetadata>>,
    mut path_lookup: impl FnMut(&DfsFileSystemId, &str) -> Result<Option<DfsObjectMetadata>>,
) -> Result<(
    Vec<DfsNamespaceObjectVersionExpectation>,
    Vec<DfsNamespacePathOccupantExpectation>,
    BTreeSet<String>,
)> {
    let mut object_ids = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .chain(
            transaction
                .versions
                .iter()
                .map(|version| version.object_id.clone()),
        )
        .chain(
            transaction
                .content_reference_identity
                .iter()
                .map(|identity| identity.object_id.clone()),
        )
        .chain(
            transaction
                .advisory_lock_states
                .iter()
                .map(|state| state.object_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    let mut paths = transaction
        .objects
        .iter()
        .map(|object| object.path.clone())
        .collect::<BTreeSet<_>>();
    for object_id in &object_ids {
        if let Some(object) = object_lookup(object_id)? {
            paths.insert(object.path);
        }
    }
    let mut expected_path_occupants = Vec::with_capacity(paths.len());
    for path in &paths {
        let occupant = path_lookup(&transaction.file_system_id, path)?;
        if let Some(occupant) = occupant.as_ref() {
            object_ids.insert(occupant.object_id.clone());
        }
        expected_path_occupants.push(DfsNamespacePathOccupantExpectation {
            path: path.clone(),
            expected_object_id: occupant.map(|object| object.object_id),
        });
    }
    let mut expected_object_versions = Vec::with_capacity(object_ids.len());
    for object_id in object_ids {
        let current = object_lookup(&object_id)?;
        expected_object_versions.push(DfsNamespaceObjectVersionExpectation {
            object_id,
            expected_version: current.map(|object| object.version),
        });
    }
    Ok((expected_object_versions, expected_path_occupants, paths))
}

/// Durable DFS metadata store interface.
///
/// Implementations own authoritative filesystem/object metadata, object
/// versions, write intents, replica receipts, conflict records, namespace
/// transactions, and metadata journals. The trait intentionally exposes paged
/// scans as primitives so service and operator code can walk large namespaces
/// without materializing every record class at once.
#[derive(Clone, Debug)]
pub struct DfsCrossShardPinTransferPlan {
    pub effects: Vec<crate::DfsPreparedMetadataEffect>,
    pub active: bool,
}
#[derive(Clone, Debug)]
pub struct DfsForeignShardSourceEvictionPlan {
    pub physical_source_shard: DfsMetadataShardId,
    pub expected_objects: Vec<DfsNamespaceObjectVersionExpectation>,
    pub expected_paths: Vec<DfsNamespacePathOccupantExpectation>,
    pub expected_journal_sequence: u64,
    pub effects: Vec<crate::DfsPreparedMetadataEffect>,
}

pub trait DfsMetadataBackend: std::fmt::Debug + Send {
    fn plan_cross_shard_pin_transfer(
        &self,
        transfer_id: &str,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        incoming_versions: &[DfsObjectVersionRecord],
        incoming_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<DfsCrossShardPinTransferPlan>;
    fn plan_foreign_shard_source_eviction(
        &self,
        moved_object: &DfsObjectMetadata,
        old_path: &str,
        captured_source_family: &DfsCrossShardSourceFamily,
        transfer_id: Option<&str>,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<DfsForeignShardSourceEvictionPlan>;
    fn prepared_operation_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<crate::DfsPreparedOperationRecord>>;
    /// Observe the filesystem and original operation from one physical shard.
    /// These are read inputs; publication still requires the native writer guard.
    fn prepared_operation_read_context(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
    ) -> Result<(
        Option<DfsFileSystem>,
        Option<crate::DfsPreparedOperationRecord>,
    )> {
        Ok((
            self.get_file_system_on_shard(shard_id, file_system_id)?,
            self.prepared_operation_record(operation_id)?,
        ))
    }
    fn list_prepared_operations(&self) -> Result<Vec<crate::DfsPreparedOperationRecord>>;
    /// Read only the prepared bodies reserving this operation's footprint.
    /// This grants no authority to resolve them; the native author must use
    /// the existing guarded recovery path with the original body/readset.
    fn prepared_operation_reservations(
        &self,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> Result<Vec<crate::DfsPreparedOperationRecord>> {
        let (objects, paths) = crate::metadata_tables::prepared_payload_reservation_keys(payload);
        Ok(self
            .list_prepared_operations()?
            .into_iter()
            .filter(|record| {
                if record.status != crate::DfsPreparedOperationStatus::Prepared
                    || record.file_system_id != payload.file_system_id
                    || record.shard_id != payload.shard_id
                    || record.operation_id == payload.operation_id
                {
                    return false;
                }
                let footprint = crate::metadata_tables::namespace_prepare_footprint(record);
                !objects.is_disjoint(footprint.object_ids())
                    || !paths.is_disjoint(footprint.normalized_paths())
            })
            .collect())
    }

    fn prepared_operation_for_request(
        &self,
        requester_node_id: &sinomo_core::node::NodeId,
        request_id: &str,
    ) -> Result<Option<crate::DfsPreparedOperationRecord>>;

    /// Shared observation-only view of already-held native storage. This performs
    /// no IO, opens no backend and conveys no writer or publication authority.
    #[cfg(any(test, feature = "test-support"))]
    fn native_held_snapshot_for_scope(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Option<crate::NativeHeldMetadataSnapshotV1>;

    /// Persist one complete immutable v7 operation. Publication effects remain
    /// hidden until its exact digest is committed with an owned writer guard.
    fn prepare_operation(
        &mut self,
        _payload: crate::metadata_records::DfsPreparedOperationPayload,
        _source: Option<&crate::source_adoption::DfsSourceAdoptionWitness>,
    ) -> Result<crate::metadata_records::DfsPreparedOperationDigest>;

    fn prepared_operation_digest(
        &self,
        _operation_id: &str,
    ) -> Result<Option<crate::metadata_records::DfsPreparedOperationDigest>>;

    fn prepared_operation_payload(
        &self,
        _operation_id: &str,
    ) -> Result<Option<std::sync::Arc<crate::metadata_records::DfsPreparedOperationPayload>>>;

    /// Read-only exact terminal evidence. No acknowledgement round or new
    /// publication marker is required for an already materialized commit.
    fn committed_prepared_operation(
        &self,
        _digest: &crate::metadata_records::DfsPreparedOperationDigest,
    ) -> Result<Option<DfsMetadataJournalEntry>>;

    /// Physical owner identity is an index key, never authority.
    fn publication_owner_scope(
        &self,
        _file_system_id: &DfsFileSystemId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<crate::publication_guard::DfsPublicationOwnerScope>;

    /// Acquire and retain the actual writer lock before final authority reads.
    fn publication_writer_session(
        &mut self,
        _file_system_id: &DfsFileSystemId,
        _shard_id: &DfsMetadataShardId,
        _digest: &crate::metadata_records::DfsPreparedOperationDigest,
        _source: Option<&crate::DfsSourceAdoptionWitness>,
        provider: Option<&crate::publication_guard::DfsPublicationExecutionProvider>,
        execution: Option<&crate::publication_guard::DfsPublicationExecutionOwners>,
        clustered: Option<&crate::DfsMetadataShardConfig>,
    ) -> Result<crate::publication_guard::DfsPublicationWriterSession>;

    fn publication_writer_file_system(
        &self,
        _file_system_id: &DfsFileSystemId,
        _session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsFileSystem>>;

    fn publication_writer_object(
        &self,
        _file_system_id: &DfsFileSystemId,
        _object_id: &DfsObjectId,
        _session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Consume actual authorization and publish only the previously sealed body.
    fn commit_prepared_operation(
        &mut self,
        _digest: &crate::metadata_records::DfsPreparedOperationDigest,
        _guard: crate::publication_guard::DfsPublicationGuard,
        enrollment: Option<&mut dyn FnMut(&[crate::DfsRepairAttemptRecord]) -> Result<()>>,
    ) -> Result<crate::metadata_records::DfsPreparedOperationCommitResult>;

    /// Pin one coherent in-memory metadata generation for a bounded bulk read.
    ///
    /// Full-catalog hydration calls many paging and point-read methods through
    /// this trait. Durable local backends may refresh once at the start of the
    /// scope and then serve those reads from that generation instead of
    /// polling the same snapshot, log, and checkpoint files for every record.
    /// Implementations must resume ordinary freshness checks after the
    /// matching end call.
    fn begin_consistent_read_snapshot(&mut self) -> Result<()> {
        Ok(())
    }

    /// End one bounded coherent-read scope opened above.
    fn end_consistent_read_snapshot(&mut self) {}

    /// Pin exactly one physical authority scope. Unlike a path lookup, this
    /// must not refresh ancestor fallbacks or unrelated shard backlogs.
    fn has_physical_read_scope(&self, _shard_id: &DfsMetadataShardId) -> bool {
        false
    }

    fn begin_consistent_read_snapshot_on_shard(
        &mut self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support exact-shard read snapshots".into(),
        ))
    }

    fn end_consistent_read_snapshot_on_shard(&mut self, _shard_id: &DfsMetadataShardId) {}

    /// Pin an existing materialized read view without acquiring a writer or
    /// running journal recovery. Unsupported backends remain unavailable.
    fn begin_stat_read_snapshot(&mut self) -> Result<()> {
        Err(MetadataBackendError::Serde(
            "backend has no stat read-view capability".into(),
        ))
    }
    fn end_stat_read_snapshot(&mut self) {}
    fn begin_stat_read_snapshot_on_shard(&mut self, _id: &DfsMetadataShardId) -> Result<()> {
        Err(MetadataBackendError::Serde(
            "backend has no exact stat read-view capability".into(),
        ))
    }
    fn end_stat_read_snapshot_on_shard(&mut self, _id: &DfsMetadataShardId) {}
    fn begin_stat_read_snapshot_on_shards(&mut self, ids: &[DfsMetadataShardId]) -> Result<()> {
        for (index, id) in ids.iter().enumerate() {
            if let Err(error) = self.begin_stat_read_snapshot_on_shard(id) {
                for pinned in ids[..index].iter().rev() {
                    self.end_stat_read_snapshot_on_shard(pinned);
                }
                return Err(error);
            }
        }
        Ok(())
    }
    fn end_stat_read_snapshot_on_shards(&mut self, ids: &[DfsMetadataShardId]) {
        for id in ids.iter().rev() {
            self.end_stat_read_snapshot_on_shard(id);
        }
    }
    fn begin_stat_read_snapshot_for_path(
        &mut self,
        _fs: &DfsFileSystemId,
        _path: &str,
    ) -> Result<()> {
        self.begin_stat_read_snapshot()
    }
    fn end_stat_read_snapshot_for_path(&mut self, _fs: &DfsFileSystemId, _path: &str) {
        self.end_stat_read_snapshot();
    }

    /// Pin the metadata generation needed to serve one exact namespace path.
    ///
    /// The default is deliberately conservative for unsharded and third-party
    /// backends. A sharded router can override this to refresh only the active
    /// path shard and any retained routing fallbacks instead of reopening every
    /// unrelated shard for a point lookup.
    fn begin_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<()> {
        let _ = (file_system_id, path);
        self.begin_consistent_read_snapshot()
    }

    /// End a path-scoped coherent-read scope opened above.
    fn end_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let _ = (file_system_id, path);
        self.end_consistent_read_snapshot();
    }

    /// Fetch a filesystem record by id.
    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>>;
    fn get_file_system_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>>;

    /// List all filesystem records known to this backend.
    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>>;

    /// Fetch an object record by object id.
    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch an object record from one exact metadata shard.
    ///
    /// Namespace prepares use this point read after selecting their immutable
    /// request-scoped shard. A sharded router must not resolve the object
    /// through its global object index here: during a cross-shard adoption the
    /// same object id is intentionally still visible on the source shard, but
    /// destination preconditions describe only destination-shard state.
    fn get_object_on_shard(
        &self,
        _shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch the durable whole-file advisory-lock aggregate for an object.
    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>>;

    /// Fetch the visible object at a normalized filesystem path.
    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch the visible object at a path from one exact metadata shard.
    ///
    /// Single-shard backends delegate to their normal lookup. Sharded routers
    /// override this so prepare expectations cannot accidentally span shard
    /// authorities while a namespace saga is moving an object.
    fn get_object_by_path_on_shard(
        &self,
        _shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Capture every object-version and path-occupant precondition for one
    /// namespace prepare on an exact shard.
    ///
    /// Object paths, path occupants and occupant versions must observe one
    /// immutable table generation. Prepare and commit revalidate these exact
    /// predicates under the publication writer lock.
    fn namespace_prepare_expectations(
        &self,
        shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )>;

    /// Compatibility helper that drains all object pages for a filesystem.
    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of objects for a filesystem.
    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;

    /// Return the subtree root and descendants using a path-index range.
    /// The backend must retain its ordinary shard-aware continuation contract.
    fn objects_for_subtree_page(
        &self,
        file_system_id: &DfsFileSystemId,
        subtree_path: &str,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;

    /// List durable direct-child tombstones below a directory. Backends with a
    /// path index should override this so targeted namespace synchronization
    /// does not scan the entire filesystem.
    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                let rest = object_path.strip_prefix(&prefix)?;
                (object.kind == DfsObjectKind::Tombstone && !rest.is_empty() && !rest.contains('/'))
                    .then_some(object)
            },
        )
    }

    /// Count visible objects in a filesystem without requiring callers to
    /// materialize every record.
    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        const COUNT_PAGE_SIZE: usize = 4096;
        let mut count = 0usize;
        walk_metadata_pages(|cursor| {
            let page =
                self.objects_for_file_system_page(file_system_id, cursor, COUNT_PAGE_SIZE)?;
            count = count.saturating_add(page.objects.len());
            Ok(page.next_cursor)
        })?;
        Ok(count)
    }

    /// Build the page cursor that resumes after `object`.
    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: None,
        })
    }

    /// Build the directory-order cursor for an object that is visible through
    /// an in-memory overlay. Indexed backends override this so callers can
    /// merge a bounded transient set into durable pages without abandoning the
    /// backend's directory index.
    fn directory_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsDirectoryPageCursor>;

    /// Return one indexed directory page together with each entry's exact
    /// resume cursor. Overlay-aware callers use these cursors directly instead
    /// of re-reading backend state once per durable object.
    /// Entries follow ascending cursor order: shard, dentry fragment/hash/name,
    /// then path and object id. A resumed page must continue strictly after its
    /// indexed cursor, so concatenating pages preserves this order.
    fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<(
        Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
        Option<DfsDirectoryPageCursor>,
    )>;

    /// Compatibility helper that lists direct children below a directory path.
    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let mut children = collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    return None;
                }
                let rest = object_path.trim_start_matches(&prefix);
                (!rest.is_empty() && !rest.contains('/')).then_some(object)
            },
        )?;
        children.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(children)
    }

    /// Return one cursor-bounded page of direct directory children.
    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let object_cursor = after.map(|cursor| DfsObjectPageCursor {
            path: cursor.path.clone(),
            object_id: cursor.object_id.clone(),
            shard_id: cursor.shard_id.clone(),
        });
        let (objects, next_cursor) = collect_filtered_cursor_page(
            object_cursor,
            max_entries,
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    return Ok(None);
                }
                let rest = object_path.trim_start_matches(&prefix);
                if rest.is_empty() || rest.contains('/') {
                    return Ok(None);
                }
                let object_cursor = self.object_page_cursor_for_object(&object)?;
                let cursor = DfsDirectoryPageCursor {
                    path: object_cursor.path,
                    object_id: object_cursor.object_id,
                    dentry: None,
                    shard_id: object_cursor.shard_id,
                };
                Ok(Some((object, cursor)))
            },
        )?;
        Ok(DfsDirectoryPage {
            objects,
            next_cursor,
        })
    }

    /// Compatibility helper that lists all hardlink records targeting an object.
    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                object_is_hardlink_to_target(&object, file_system_id, target_object_id)
                    .then_some(object)
            },
        )?;
        sort_hardlinks_by_path(&mut hardlinks);
        Ok(hardlinks)
    }

    /// Return one cursor-bounded page of hardlink records for a target object.
    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let (hardlinks, next_cursor) = collect_filtered_cursor_page(
            after.cloned(),
            max_entries,
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                if !object_is_hardlink_to_target(&object, file_system_id, target_object_id) {
                    return Ok(None);
                }
                let cursor = self.object_page_cursor_for_object(&object)?;
                Ok(Some((object, cursor)))
            },
        )?;
        Ok(DfsObjectPage {
            objects: hardlinks,
            next_cursor,
        })
    }

    /// Count hardlink records targeting an object.
    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut count = 0usize;
        walk_metadata_pages(|cursor| {
            let page =
                self.hardlinks_to_object_page(file_system_id, target_object_id, cursor, 1024)?;
            count = count.saturating_add(page.objects.len());
            Ok(page.next_cursor)
        })?;
        Ok(count)
    }

    /// Return the first hardlink record targeting an object, if one exists.
    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }

    /// Return the first hardlink target outside `prefix`, used by namespace
    /// cleanup to avoid removing still-referenced content.
    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        let mut found = None;
        walk_metadata_pages(|cursor| {
            let page =
                self.hardlinks_to_object_page(file_system_id, target_object_id, cursor, 1024)?;
            for object in page.objects {
                if !posix_path_is_at_or_under_prefix(&object.path, &prefix) {
                    found = Some(object);
                    return Ok(None);
                }
            }
            Ok(page.next_cursor)
        })?;
        Ok(found)
    }

    /// List all durable version pins attached to one object. The object-family
    /// migration path uses this to preserve checkpoint roots with authority.
    ///
    /// Backends that do not implement durable version pins must fail closed:
    /// silently returning an empty set could let migration or GC discard a
    /// version still protected by an implementation-specific checkpoint.
    fn version_pins_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsMetadataVersionPinRecord>> {
        let _ = object_id;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support durable version pin scans".to_string(),
        ))
    }

    /// Return one bounded page of durable pins in `(version, pin_id)` order.
    ///
    /// Migration code must use this page API instead of first materializing
    /// the complete pin set so its per-pass record budget remains meaningful.
    fn version_pins_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsMetadataVersionPinPageCursor>,
        max_entries: usize,
    ) -> Result<DfsMetadataVersionPinPage> {
        let _ = (object_id, after, max_entries);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support bounded durable version pin scans".to_string(),
        ))
    }

    /// Inspect current prepared-reference owners through the object index.
    /// The second result is false when the bounded scan is incomplete.
    fn prepared_content_reference_protection(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        max_prepares: usize,
    ) -> Result<(bool, bool)>;

    /// Return active target-side pin-transfer provenance for one object.
    fn cross_shard_pin_transfers_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsCrossShardPinTransferRecord>> {
        let _ = object_id;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support cross-shard pin-transfer scans".to_string(),
        ))
    }

    /// Capture the exact source-authority pin set and its journal head under
    /// one shard writer lock. The destination payload and later source
    /// eviction fence must use this same capture.
    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        let _ = object_id;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support cross-shard source pin fences".to_string(),
        ))
    }

    /// Capture the complete physical source family from the shard selected by
    /// `source_path`, without following an object authority index that may
    /// already point at the destination after a crash.
    fn capture_cross_shard_source_family(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsCrossShardSourceFamily> {
        let _ = (object_id, source_path);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support path-anchored cross-shard source capture"
                .to_string(),
        ))
    }

    /// Capture only the actual source family under its physical writer and
    /// handoff fences. The Actor must separately verify current source authority.
    fn capture_source_adoption_source(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionSourceCapture>;

    /// Capture the actual original Running job under its physical writer and
    /// handoff fences. Remote job rows or source families cannot create custody.
    fn capture_source_adoption_job(
        &mut self,
        job_id: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionJobCapture>;

    /// Capture the physical source family while retaining its existing writer
    /// and handoff fences. This is owned source custody, not author permission.
    fn capture_source_adoption(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
        job_id: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionCapture>;

    /// Fetch one object-version record by object id and version id.
    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>>;

    /// Fetch object/version pairs in caller-provided order.
    fn get_object_version_pairs(
        &self,
        keys: &[(DfsObjectId, DfsVersion)],
    ) -> Result<Vec<(Option<DfsObjectMetadata>, Option<DfsObjectVersionRecord>)>> {
        keys.iter()
            .map(|(object_id, version)| {
                Ok((
                    self.get_object(object_id)?,
                    self.get_version(object_id, version)?,
                ))
            })
            .collect()
    }

    /// Bounded candidates for a generic retained-revision projection. Indexed
    /// backends may skip unrelated rows, but must include grants whose captured
    /// path or current object path is within `path_prefix`, in global
    /// `(object, version)` order. Candidates do not prove a live reference.
    /// The default preserves compatibility with backends without an index.
    fn retained_revision_records_page(
        &self,
        namespace: Option<&str>,
        file_system_id: &DfsFileSystemId,
        object_id: Option<&DfsObjectId>,
        path_prefix: Option<&str>,
        after: Option<&DfsObjectVersionScanCursor>,
        max_records: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        let _ = (namespace, file_system_id, path_prefix);
        match object_id {
            Some(object_id) => object_version_records_page(self, object_id, after, max_records),
            None => self.version_records_page(after, max_records),
        }
    }

    /// Bounded generic grant projection over immutable version candidates.
    /// A filtered page may be empty; its cursor advances over candidates.
    /// Namespace filtering never combines distinct objects at a path.
    fn retained_revision_descriptors_page(
        &self,
        namespace: Option<&str>,
        file_system_id: &DfsFileSystemId,
        object_id: Option<&DfsObjectId>,
        path_prefix: Option<&str>,
        after: Option<&DfsObjectVersionScanCursor>,
        max_records: usize,
    ) -> Result<(
        Vec<crate::DfsRetainedRevisionDescriptor>,
        Option<DfsObjectVersionScanCursor>,
    )> {
        if max_records == 0 || max_records > 128 {
            return Err(MetadataBackendError::Serde(
                "retained revision page requires 1..=128 records".into(),
            ));
        }
        if let Some(namespace) = namespace {
            crate::validate_metadata_namespace(namespace).map_err(MetadataBackendError::Serde)?;
        }
        let page = self.retained_revision_records_page(
            namespace,
            file_system_id,
            object_id,
            path_prefix,
            after,
            max_records,
        )?;
        let mut entries = Vec::new();
        let mut pins_by_object = std::collections::BTreeMap::new();
        let mut current_by_object = std::collections::BTreeMap::new();
        let mut orders_by_object = std::collections::BTreeMap::<
            DfsObjectId,
            std::collections::BTreeMap<String, u64>,
        >::new();
        let now = crate::dfs_now_unix_nanos();
        for version in page.versions {
            if &version.file_system_id != file_system_id
                || object_id.is_some_and(|id| id != &version.object_id)
            {
                continue;
            }
            if !version.retention.iter().any(|(owner, state)| {
                namespace.is_none_or(|namespace| namespace == owner.as_str())
                    && state.grant.is_some()
            }) {
                continue;
            }
            crate::file_history::validate_file_version_record(&version)?;
            if !current_by_object.contains_key(&version.object_id) {
                current_by_object.insert(
                    version.object_id.clone(),
                    self.get_object(&version.object_id)?,
                );
            }
            let Some(current) = current_by_object
                .get(&version.object_id)
                .and_then(Option::as_ref)
                .filter(|object| object.tombstone_provenance.is_none())
            else {
                continue;
            };
            if let Some(prefix) = path_prefix {
                let matches = |path: &str| {
                    let path = crate::metadata_keys::normalize_posix_path(path);
                    prefix == "/"
                        || path == prefix
                        || path
                            .strip_prefix(prefix)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                };
                if !matches(&current.path)
                    && !version.retention.iter().any(|(owner, state)| {
                        namespace.is_none_or(|namespace| namespace == owner.as_str())
                            && state
                                .grant
                                .as_ref()
                                .is_some_and(|grant| matches(&grant.object.path))
                    })
                {
                    continue;
                }
            }
            if !pins_by_object.contains_key(&version.object_id) {
                pins_by_object.insert(
                    version.object_id.clone(),
                    self.version_pins_for_object(&version.object_id)?,
                );
            }
            let references = pins_by_object[&version.object_id]
                .iter()
                .filter(|pin| pin.version == version.version)
                .cloned()
                .collect::<Vec<_>>();
            let has_reference = references
                .iter()
                .any(|pin| namespace.is_none_or(|owner| pin.namespace == owner));
            if !crate::file_history::retained_namespace_is_live(
                &version,
                namespace,
                has_reference,
                now,
                || {
                    if let Some(orders) = orders_by_object.get(&version.object_id) {
                        return Ok(orders.clone());
                    }
                    let orders = crate::file_history::retention_orders_from_backend(
                        self,
                        &version.object_id,
                    )?;
                    orders_by_object.insert(version.object_id.clone(), orders.clone());
                    Ok(orders)
                },
            )? {
                continue;
            }
            entries.push(crate::DfsRetainedRevisionDescriptor {
                version,
                current_object: current.clone(),
                references,
            });
        }
        Ok((entries, page.next_cursor))
    }

    fn query_file_history_on_shard(
        &self,
        query: &crate::DfsMetadataShardQuery,
    ) -> Result<crate::DfsMetadataQueryResult> {
        let _ = query;
        Err(MetadataBackendError::Serde(
            "backend does not support shard-scoped file history".into(),
        ))
    }

    /// Compatibility helper that drains all versions for one object.
    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of versions for one object.
    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage>;

    /// Compatibility helper that drains all receipts for one object version.
    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of receipts for one object version.
    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage>;

    /// Inspect the active object/version intent index for a placement-owned
    /// repair contract. Compacted retry history is not a live obligation.
    fn has_current_placement_repair_owner(&self, object: &DfsObjectMetadata) -> Result<bool>;

    /// Actual unit-test scan counters from already initialized physical children.
    /// These memory-only observations never open a backend or grant authority.
    #[cfg(test)]
    fn test_receipt_scan_counts_by_shard(&self) -> BTreeMap<DfsMetadataShardId, (usize, usize)>;
    #[cfg(test)]
    fn reset_test_receipt_scan_counts_by_shard(&self);

    /// Return full intents, compact receipts, and conservative eviction floors
    /// from one shard-local metadata snapshot.
    fn object_write_intent_history(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectWriteIntentHistory>;

    /// Fetch the exact compact representation for one intent, if compaction
    /// has replaced its full body.
    fn get_intent_receipt(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentReceipt>> {
        Ok(self
            .object_write_intent_history(object_id)?
            .receipts
            .into_iter()
            .find(|receipt| receipt.intent_id == intent_id))
    }

    /// Return the conservative receipt-eviction floor owned by the object's
    /// current metadata shard.
    fn intent_receipt_retention_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsWriteIntentReceiptRetentionState> {
        Ok(self.object_write_intent_history(object_id)?.retention)
    }

    /// Fetch a write intent by object id and intent id.
    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;

    /// Observe one write identity and its object's indexed reservations without
    /// opening unrelated shards or granting publication authority.
    fn write_operation_admission_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<DfsWriteOperationAdmissionSnapshot>;

    /// Fetch one exact write-intent key from one physical local metadata
    /// shard. Unlike [`Self::get_intent`], this never follows an object index
    /// or merges historical shard copies. Bounded recovery uses it to prove
    /// that an active intent has exactly one durable shard provenance before
    /// making an invisible create visible.
    fn get_intent_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;

    /// Fetch one exact receipt key from one physical local metadata shard.
    /// This is the receipt counterpart to [`Self::get_intent_on_shard`] and
    /// avoids relying on a visible-object routing index during create recovery.
    fn get_receipt_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>>;

    /// Highest generated intent counter which must not be reused after compact
    /// receipts have been evicted.
    fn write_intent_counter_floor(&self) -> Result<u64> {
        Ok(0)
    }

    /// Compatibility helper that drains all intents for one object.
    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.intents_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of intents for one object.
    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage>;

    /// Return one cursor-bounded page of intents for one object and target
    /// version.
    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let (intents, next_cursor) = collect_filtered_cursor_page(
            after.cloned(),
            max_entries,
            |cursor| {
                let page = self.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.intents, page.next_cursor))
            },
            |intent| {
                if intent.target_version != *target_version {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor(&intent);
                Ok(Some((intent, cursor)))
            },
        )?;
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    /// Return one cursor-bounded global page of write intents.
    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage>;

    /// Spend a raw-record budget in at most one physical shard, before active
    /// or ownership filtering. A continuation is advisory scan progress only.
    /// Public inventory paging cannot provide this work bound after filtering,
    /// so unsupported backends must not silently fall back to that API.
    fn write_intent_maintenance_page(
        &self,
        _after: Option<&crate::DfsWriteIntentMaintenanceCursor>,
        _max_records: usize,
    ) -> Result<crate::DfsWriteIntentMaintenancePage> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support physically bounded intent maintenance".into(),
        ))
    }

    /// Apply the existing merged-scan visibility predicate to one physically
    /// observed active row. This is separate from bounded enumeration because
    /// ownership point lookups may cross shard boundaries. Recovery must still
    /// perform its normal exact authority and durable-record rechecks.
    fn write_intent_maintenance_candidate_is_listable(
        &self,
        _shard_id: &DfsMetadataShardId,
        _intent: &DfsWriteIntentRecord,
    ) -> Result<bool> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support physical intent visibility checks".into(),
        ))
    }

    /// Return one cursor-bounded page of durable write intents from one exact
    /// local metadata shard.
    ///
    /// Recovery uses this shard-scoped view to retain intent provenance across
    /// authority movement. It must include historical duplicates which the
    /// merged global scan deliberately filters from ordinary callers.
    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>>;

    /// Return one cursor-bounded global page of object-version records.
    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage>;

    /// Return one cursor-bounded global page of replica receipts.
    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage>;

    /// Return one cursor-bounded global page of conflict records.

    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_versions(&self) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.version_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_receipts(&self) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipt_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.receipts, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.write_intents_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.

    /// Add one object's versions, receipts, intents, conflicts, and repair
    /// count into an in-progress summary.
    fn accumulate_metadata_summary_for_object(
        &self,
        summary: &mut DfsMetadataSummary,
        object: &DfsObjectMetadata,
    ) -> Result<()> {
        summary.object_count = summary.object_count.saturating_add(1);

        let mut object_intents = Vec::new();
        walk_metadata_pages(|intent_cursor| {
            let intent_page = self.intents_for_object_page(
                &object.object_id,
                intent_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.intent_count = summary
                .intent_count
                .saturating_add(intent_page.intents.len());
            summary.pending_write_count = summary.pending_write_count.saturating_add(
                intent_page
                    .intents
                    .iter()
                    .filter(|intent| write_intent_status_is_active(&intent.status))
                    .count(),
            );
            object_intents.extend(intent_page.intents);
            Ok(intent_page.next_cursor)
        })?;

        let committed_targets = object_intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .map(|intent| intent.target_version.clone())
            .collect::<BTreeSet<_>>();
        let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
        let mut repair_versions = BTreeMap::new();
        walk_metadata_pages(|version_cursor| {
            let version_page = self.versions_for_object_page(
                &object.object_id,
                version_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.version_count = summary
                .version_count
                .saturating_add(version_page.versions.len());
            for version in version_page.versions {
                repair_versions.insert(version.version.clone(), version.clone());
                let collect_for_repair = committed_targets.contains(&version.version);
                walk_metadata_pages(|receipt_cursor| {
                    let receipt_page = self.receipts_for_version_page(
                        &object.object_id,
                        &version.version,
                        receipt_cursor,
                        OBJECT_LOCAL_METADATA_PAGE_SIZE,
                    )?;
                    summary.receipt_count = summary
                        .receipt_count
                        .saturating_add(receipt_page.receipts.len());
                    if collect_for_repair {
                        receipts_by_version
                            .entry(version.version.clone())
                            .or_default()
                            .extend(receipt_page.receipts);
                    }
                    Ok(receipt_page.next_cursor)
                })?;
            }
            Ok(version_page.next_cursor)
        })?;

        summary.pending_repair_count =
            summary
                .pending_repair_count
                .saturating_add(count_pending_repairs_for_object(
                    object,
                    &object_intents,
                    &receipts_by_version,
                    &repair_versions,
                    &self
                        .version_pins_for_object(&object.object_id)?
                        .into_iter()
                        .map(|pin| pin.version)
                        .collect(),
                ));
        Ok(())
    }

    /// Add all object-level counts for one filesystem into an in-progress
    /// summary.
    fn accumulate_metadata_summary_for_file_system(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        walk_metadata_pages(|object_cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                self.accumulate_metadata_summary_for_object(summary, &object)?;
            }
            Ok(page.next_cursor)
        })?;
        Ok(())
    }

    /// Add namespace-job counts into an in-progress summary, optionally scoped
    /// to one filesystem.
    fn accumulate_namespace_job_summary(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_scope: Option<&DfsFileSystemId>,
    ) -> Result<()> {
        walk_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                if file_system_scope
                    .is_some_and(|file_system_id| job.file_system_id != *file_system_id)
                {
                    continue;
                }
                match job.status {
                    DfsNamespaceJobStatus::Running => {
                        summary.pending_namespace_job_count =
                            summary.pending_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Failed => {
                        summary.failed_namespace_job_count =
                            summary.failed_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Completed => {}
                }
            }
            Ok(page.next_cursor)
        })?;
        Ok(())
    }

    /// Compute a metadata summary across every filesystem visible to the
    /// backend.
    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let filesystems = self.list_file_systems()?;
        let mut summary = DfsMetadataSummary {
            file_system_count: filesystems.len(),
            ..DfsMetadataSummary::default()
        };
        for file_system in &filesystems {
            self.accumulate_metadata_summary_for_file_system(
                &mut summary,
                &file_system.file_system_id,
            )?;
        }
        self.accumulate_namespace_job_summary(&mut summary, None)?;
        Ok(summary)
    }

    /// Compute the operational metadata counts owned by this node's current
    /// primary shards.
    ///
    /// A non-sharded backend is wholly local, so the default is the complete
    /// summary. Topology-aware inspection backends override this to exclude
    /// replica-only backlog without reopening a second subset of shard stores.
    fn metadata_summary_for_local_primary_shards(&self) -> Result<DfsMetadataSummary> {
        self.metadata_summary()
    }

    /// Compute the complete and local-primary operational summaries from one
    /// coherent table observation.
    ///
    /// The default backend is wholly local, so both projections are identical.
    /// Sharded backends override this to avoid rescanning primary tables after
    /// building the complete operator summary.
    fn metadata_status_summaries(&self) -> Result<(DfsMetadataSummary, DfsMetadataSummary)> {
        let summary = self.metadata_summary()?;
        Ok((summary.clone(), summary))
    }

    /// Compute a metadata summary for one filesystem, returning `None` when
    /// the filesystem is unknown.
    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        self.accumulate_metadata_summary_for_file_system(&mut summary, file_system_id)?;
        self.accumulate_namespace_job_summary(&mut summary, Some(file_system_id))?;
        Ok(Some(summary))
    }

    /// Fetch a recorded namespace transaction by id when the backend supports
    /// transaction recovery.

    /// Exact compact outcome identity retained after a transaction body is
    /// checkpointed. Backends without durable receipts fail closed for callers
    /// which require replayable module contribution effects.
    fn get_namespace_transaction_receipt(
        &self,
        transaction_id: &str,
    ) -> Result<Option<crate::DfsNamespaceTransactionReceipt>>;

    /// One bounded object-indexed page of durable reference outcome witnesses.
    fn content_reference_outcomes_for_object_page(
        &self,
        _file_system_id: &DfsFileSystemId,
        _object_id: &DfsObjectId,
        _after: Option<&str>,
        _max_entries: usize,
    ) -> Result<crate::DfsContentReferenceOutcomePage>;

    fn content_reference_outcome_retention(
        &self,
        _file_system_id: &DfsFileSystemId,
        _object_id: &DfsObjectId,
    ) -> Result<Option<crate::DfsContentReferenceOutcomeRetention>>;

    fn content_reference_outcome_history_expired(
        &self,
        identity: &crate::DfsRevisionReference,
        operation_id: &str,
    ) -> Result<bool> {
        let Some(retention) = self
            .content_reference_outcome_retention(&identity.file_system_id, &identity.object_id)?
        else {
            return Ok(false);
        };
        Ok(
            match crate::intent::stable_operation_order_token(operation_id) {
                Some(token) => retention
                    .max_evicted_operation_order_token
                    .as_ref()
                    .is_some_and(|floor| token <= *floor),
                None => retention.opaque_operation_history_evicted,
            },
        )
    }

    /// Project retention-eligible tombstones without taking a write lock.
    ///
    /// Reactive maintenance schedulers use the returned due count and next
    /// eligibility deadline to avoid periodic full passes on idle metadata.
    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        Ok(DfsTombstoneCompactionReport::for_options(&options))
    }

    /// Compact eligible tombstone records according to backend policy.
    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let _ = options;
        Ok(DfsTombstoneCompactionReport::default())
    }

    /// Project retention-eligible metadata history and its next deadline
    /// without authoring journal entries.
    fn metadata_history_gc_projection(
        &self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        Ok(DfsMetadataHistoryGcReport::for_options(&options))
    }

    /// Append and materialize a bounded authoritative metadata-history GC
    /// batch. Implementations must journal every observable removal.
    fn plan_metadata_history_gc(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryEffectsPlan>;

    /// Return one bounded, stable page of shard-local content reachability
    /// roots from an exact durable metadata cut.
    ///
    /// Clustered callers validate primary authority and topology before
    /// invoking this method. Backends which cannot expose an authoritative
    /// shard image fail closed with a structured, rootless page.
    fn content_root_catalog_page(
        &mut self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage> {
        request.validate()?;
        Ok(DfsContentRootCatalogPage::incomplete_for_request(
            request,
            DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
            crate::metadata_tombstone::metadata_now_unix_nanos(),
        ))
    }

    /// Return the exact metadata roots which local version-manifest and blob
    /// reclamation must preserve.
    fn metadata_content_protection(&self) -> Result<DfsMetadataContentProtection>;

    /// Fetch a durable namespace request by authenticated requester and id.
    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>>;

    /// Fetch the compact canonical outcome retained after a completed request
    /// has been folded into an authoritative metadata snapshot.
    fn get_namespace_request_receipt(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestReceipt>> {
        let _ = (requester_node_id, request_id);
        Ok(None)
    }

    /// Return whether a missing request identity is at or below this shard's
    /// durable idempotency-retention floor.
    ///
    /// Callers must reject such an identity as expired/unknown. Treating it as
    /// new could reapply an operation after its exact terminal receipt was
    /// evicted from the bounded checkpoint window.
    fn namespace_request_history_expired_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<bool> {
        let _ = (shard_id, requester_node_id, request_id);
        Ok(false)
    }

    /// Read all durable idempotency evidence needed to admit one namespace
    /// request from a single coherent metadata generation.
    ///
    /// Inspect each retained shard once and read all admission indexes from
    /// the same immutable table generation.
    fn namespace_request_admission_snapshot(
        &mut self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<DfsNamespaceRequestAdmissionSnapshot>;

    /// Return durable routed namespace requests for bounded autonomous
    /// inspection. Recovery should use `list_pending_namespace_requests_page`
    /// so terminal idempotency records are never cloned or filtered.
    ///
    /// Completed records remain complete until the next checkpoint folds them
    /// into bounded canonical receipts. Pending records are never pruned.
    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>>;

    /// Return one bounded, oldest-attempt-first page of pending namespace
    /// requests. Concrete durable backends serve this from a derived status
    /// index rather than scanning retained terminal idempotency records.
    fn list_pending_namespace_requests_page(
        &self,
        after: Option<&DfsPendingNamespaceRequestPageCursor>,
        max_entries: usize,
    ) -> Result<DfsPendingNamespaceRequestPage>;

    /// Fetch a namespace maintenance job by id.
    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>>;

    /// Fetch compact terminal namespace-job evidence by durable job id.
    fn get_namespace_job_receipt(&self, job_id: &str) -> Result<Option<DfsNamespaceJobReceipt>>;

    /// Find completed job evidence for an exact durable routed request.
    ///
    /// Match the actual durable origin identity; diagnostic path/time values
    /// do not replace routed request provenance.
    fn completed_namespace_job_receipt_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<Option<DfsNamespaceJobReceipt>>;

    /// Collect all namespace jobs through the bounded paging primitive.
    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of namespace jobs.
    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let _ = after;
        let _ = max_entries;
        Ok(DfsNamespaceJobPage {
            jobs: Vec::new(),
            next_cursor: None,
        })
    }

    /// Read all journal entries after `after_sequence`.
    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>>;

    /// Highest journal sequence known across this backend.
    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .map(|status| status.journal_last_sequence)
            .max()
            .unwrap_or(0))
    }

    /// Export a table snapshot for replication or repair.
    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot>;

    /// Snapshot of one shard's tables, for replica snapshot repair. Backends
    /// that cannot scope an export to a single shard return `None` and the
    /// repair is skipped.
    /// Capture the sole current native record image. This is DATA only and
    /// cannot mint the replica permit consumed by install.
    fn export_shard_snapshot_image(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataSnapshotImage>>;

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        let _ = shard_id;
        Ok(None)
    }

    /// Return the O(1), durable identity of one shard's exact materialized
    /// journal prefix. Implementations must return `None` instead of deriving
    /// a partial or sequence-only identity.
    fn metadata_prefix_proof(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>>;

    /// Observe authorized journal ingest and snapshot replacement across local
    /// backend handles. Notifications are hints, not substitutes for reading
    /// durable state; a requester retains its subscription between operations.
    fn subscribe_metadata_replica_progress(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataReplicaObserver>> {
        Ok(None)
    }

    /// Refresh and pin exactly one shard after observed durable replica progress.
    /// Return its materialized exact prefix only if it equals or verifiably
    /// extends the observed prefix. `Some` must be paired with `end` on every
    /// path; its job pages and prefix come from that same pinned generation.
    fn begin_replica_projection_read(
        &mut self,
        _installed: &crate::DfsMetadataReplicaObservation,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        Ok(None)
    }

    fn end_replica_projection_read(&mut self, _shard_id: &DfsMetadataShardId) {}

    /// One bounded job page from the shard pinned by `begin_replica_projection_read`.
    fn replica_projection_namespace_jobs_page(
        &self,
        _shard_id: &DfsMetadataShardId,
        _after: Option<&DfsNamespaceJobPageCursor>,
        _max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support exact-shard snapshot job pages".into(),
        ))
    }

    /// Refresh one shard's in-memory metadata view from its durable tables and
    /// journal before a cold-path exact-prefix attestation. Implementations
    /// with no cached durable state may retain the no-op default.
    fn refresh_metadata_shard_durable_view(
        &mut self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        Ok(())
    }

    /// Refresh one shard only when its cached durable generation changed.
    ///
    /// The default preserves compatibility for backends that cannot compare a
    /// cached generation cheaply. Sharded local backends use this cold-path
    /// variant during service rebind so an unchanged activated marker does not
    /// rebuild the complete derived object-to-shard index on every open.
    fn refresh_metadata_shard_durable_view_if_changed(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.refresh_metadata_shard_durable_view(shard_id)?;
        Ok(true)
    }

    /// Refresh only the owned shard's table/journal view before an exact
    /// prefix attestation or durability-head read. Derived cross-shard object
    /// indexes and unrelated shard generations must remain untouched.
    fn refresh_metadata_shard_attestation_view(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        self.refresh_metadata_change_feed_view(shard_id)
    }

    /// Refresh the exact shard view needed by its change feed. This does not
    /// require rebuilding cross-shard object indexes from a full snapshot.
    fn refresh_metadata_change_feed_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        self.refresh_metadata_shard_durable_view(shard_id)
    }

    /// Return the durable membership-handoff write freeze for one shard.
    fn metadata_shard_write_freeze(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        Ok(None)
    }

    /// Whether this backend instance is currently fenced from authoritative
    /// shard mutations. An activated durable marker still fences an
    /// independently opened lane until exact completion evidence rebinds that
    /// process-local instance.
    fn metadata_shard_authoritative_writes_frozen(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        Ok(false)
    }

    /// The instance-local fence without a cached topology/primary-role filter.
    /// Mutation admission uses this only for a shard stamped as locally owned,
    /// then verifies the current assignment and exact completion proof. A
    /// former replica's stale routing catalog must not hide its own fence.
    fn metadata_shard_instance_writes_frozen(&self, shard_id: &DfsMetadataShardId) -> Result<bool> {
        self.metadata_shard_authoritative_writes_frozen(shard_id)
    }

    /// Atomically freeze authoritative mutations and capture the exact current
    /// shard state under the backend's cross-process write lock.
    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        Ok(None)
    }

    /// Durably mark the exact matching freeze as activated without binding
    /// this backend instance to the successor authority generation. This lets
    /// a retiring instance record control-plane completion while remaining
    /// fenced. A fresh authority observer must separately call
    /// [`clear_metadata_shard_membership_handoff_freeze`](Self::clear_metadata_shard_membership_handoff_freeze)
    /// after proving the active assignment and completion evidence.
    fn mark_metadata_shard_membership_handoff_activated(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _activated_epoch: u64,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Advance the exact matching freeze to a durable active generation after
    /// the desired assignment is active. Implementations must install the
    /// complete active shard configuration before clearing the pending freeze
    /// or running append-producing recovery. The calling backend instance is
    /// rebound to that exact generation; independently opened stale instances
    /// stay fenced until they separately prove and observe it. The exact
    /// prefix checked by the caller is revalidated under the refreshed writer
    /// before epoch binding, marker changes, rebind checkpointing or explicit recovery.
    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _active_shard: &DfsMetadataShardConfig,
        _checked_prefix: &DfsMetadataPrefixProof,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Read at most `max_entries` journal entries after `after_sequence`.
    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = self.journal_since(after_sequence)?;
        entries.truncate(max_entries);
        Ok(entries)
    }

    /// Build a bounded journal batch after `after_sequence`.
    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        bounded_metadata_journal_batch(
            DfsMetadataShardId::new("default"),
            after_sequence,
            entries,
            max_entries,
            max_encoded_bytes,
        )
    }

    /// Build a bounded journal batch for one shard. Single-shard backends may
    /// delegate to `journal_batch_since_limited`.
    fn journal_batch_for_shard_since_limited(
        &self,
        _shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.journal_batch_since_limited(after_sequence, max_entries, max_encoded_bytes)
    }

    /// Return the exact immutable SOBJ frames for a previously selected
    /// journal batch. These must be the bytes already stored in its
    /// authoritative journal; decoded records cannot be re-encoded as evidence.
    fn journal_frames_for_batch(
        &self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Vec<DfsMetadataJournalFrameRecord>>;

    /// Share fresh, already verified foreground ACKs with other local sender
    /// handles. This is a non-durable scheduling hint, never publication authority.
    fn observe_metadata_replication_cursors(
        &self,
        _shard_id: &DfsMetadataShardId,
        _cursors: &[(NodeId, u64)],
    ) {
    }

    /// Highest known delivered prefix, combining the saved checkpoint with
    /// verified in-process ACK hints. This is a suffix-selection cursor, not
    /// fresh evidence of receiver durability or publication authority.
    fn metadata_replication_cursor(
        &self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<u64>;

    /// Checkpoint the next journal sequence to send to a target for a shard.
    /// This derived hint may be lost on crash; it cannot certify receiver
    /// durability or authorize publication without a fresh exact acknowledgement.
    fn put_metadata_replication_cursor(
        &mut self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
        _next_after_sequence: u64,
    ) -> Result<()>;

    /// Return the replica-local exact-checkpoint floor persisted before a
    /// successful prefix acknowledgement.
    fn metadata_prefix_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>>;

    /// Persist a monotonic exact-checkpoint floor before acknowledging the
    /// corresponding prefix probe.
    fn put_metadata_prefix_ack_floor(&mut self, _floor: &DfsMetadataPrefixAckFloor) -> Result<()>;

    /// Return the replica-local floor persisted before an exact ordinary-write
    /// authority acknowledgement.
    fn metadata_authority_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>>;

    /// Persist a monotonic ordinary-write authority floor before returning the
    /// corresponding request nonce.
    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()>;

    /// Return the durable replay cursor for journal entries originating from
    /// `source_shard_id`. Local durable backends require this to match their
    /// logical shard identity.
    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .flat_map(|status| status.replay_cursors)
            .find(|cursor| cursor.source_shard_id == *source_shard_id)
            .map(|cursor| cursor.replayed_through_sequence)
            .unwrap_or(0))
    }

    /// Return per-shard metadata journal and replay status.
    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>>;

    /// Resolve the durable shard which owns one namespace path when this
    /// backend can prove that routing identity.
    ///
    /// Unsharded backends inherit the single status identity. Sharded routers
    /// override this with their canonical path routing table. Returning `None`
    /// is conservative: callers retain an unscoped guard which exact-shard
    /// replacement cannot discard without positive ownership evidence.
    fn metadata_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardId>> {
        let _ = (file_system_id, path);
        let statuses = self.metadata_shard_statuses()?;
        Ok((statuses.len() == 1).then(|| statuses[0].shard_id.clone()))
    }

    /// Persist the original replica frames under their validated source permit.
    /// Durability acknowledgement does not require the rebuildable tables to
    /// reach the same cut; authoring and handoff retain their refresh fences.
    fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport>;

    /// Persist the complete current source cut and independently verify its
    /// named original preparation, including the full body and actual slot.
    /// Later frames remain part of the exact prefix obligation. A delayed cut
    /// must still extend to any later locally acknowledged durable history.
    fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
        expected_operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Result<DfsMetadataReplicaApplyReport>;

    /// Materialize any journal suffix previously admitted at the durable ACK
    /// boundary. Promotion, snapshots, and ordinary authoritative writes must
    /// continue to use their existing synchronous refresh fences.
    fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        Ok(0)
    }

    /// Replace local metadata tables using exact source evidence validated by
    /// the metadata replication boundary. The local durable implementation
    /// preserves any activated author fence and never binds authority through
    /// this path.
    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        _permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: crate::DfsMetadataSnapshotImage,
    ) -> Result<bool>;

    /// Create a subscription cursor for journal polling.
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;

    /// Poll journal entries for a subscription.
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}
