//! Metadata backend API surface.
//!
//! This module owns the public metadata backend contract and default scan
//! helpers. Concrete local storage lives in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::node::NodeId;

use crate::advisory_lock::{
    DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord,
    DfsAdvisoryLockTransitionContext,
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

#[cfg(test)]
std::thread_local! {
    static NAMESPACE_PREPARE_NORMALIZATION_PASSES: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
pub(crate) fn record_namespace_prepare_normalization_pass() {
    NAMESPACE_PREPARE_NORMALIZATION_PASSES.with(|passes| {
        passes.set(passes.get().saturating_add(1));
    });
}

#[cfg(test)]
pub(crate) fn reset_namespace_prepare_normalization_passes() {
    NAMESPACE_PREPARE_NORMALIZATION_PASSES.with(|passes| passes.set(0));
}

#[cfg(test)]
pub(crate) fn namespace_prepare_normalization_passes() -> usize {
    NAMESPACE_PREPARE_NORMALIZATION_PASSES.with(std::cell::Cell::get)
}

/// Proof that a mutable namespace prepare is in canonical normalized order.
///
/// The wrapped record remains owned by the caller, but safe external code
/// cannot forge this proof because construction and mutable access are kept at
/// the DFS normalization boundary. Backend implementations may inspect the
/// canonical record and either use the default borrowed fallback or consume
/// the proof in an optimized implementation.
pub struct DfsNormalizedNamespacePrepare<'a> {
    prepare: &'a mut DfsNamespacePrepareRecord,
}

impl<'a> DfsNormalizedNamespacePrepare<'a> {
    pub(crate) fn new(prepare: &'a mut DfsNamespacePrepareRecord) -> Self {
        #[cfg(test)]
        record_namespace_prepare_normalization_pass();
        crate::metadata_records::normalize_namespace_prepare_in_place(prepare);
        Self { prepare }
    }

    /// Borrow the canonical record without weakening its normalization proof.
    pub fn as_record(&self) -> &DfsNamespacePrepareRecord {
        self.prepare
    }

    pub(crate) fn set_operation_digest(&mut self, operation_digest: String) {
        self.prepare.operation_digest = operation_digest;
    }
}

/// Result of fencing a replica-location update against the current object.
///
/// `None` from [`DfsMetadataBackend::add_object_location_if_current`] means
/// the object no longer names the expected visible version. A returned value
/// always carries the current object and explicitly says whether the backend
/// appended a journal entry, so callers can retain the matching replication
/// wake across partial receipt/location retries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsObjectLocationUpdate {
    pub object: DfsObjectMetadata,
    pub journal_appended: bool,
}

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
    /// reconstruct it. `None` is accepted only for markers written by older
    /// binaries and is upgraded during their first exact-proof rebind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebind_prefix_proof: Option<crate::DfsMetadataPrefixProof>,
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

/// Compact evidence derived from one validated namespace-prepare payload.
///
/// The durable-journal ACK path constructs this from the identity retained in
/// the exact, structurally validated journal payload. It therefore does not
/// need to expand a packed prepare into the rebuildable materialized tables
/// before acknowledging it. The ordinary apply path can construct the same
/// evidence from its canonical allocation, and retries without fresh evidence
/// retain the durable verification fallback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsValidatedNamespaceAuthorityProof {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    owner_epoch: u64,
    operation_id: String,
    status: DfsNamespacePrepareStatus,
    prepare_sequence: u64,
    payload_digest: String,
}

impl DfsValidatedNamespaceAuthorityProof {
    pub(crate) fn from_validated_payload(
        payload: &DfsNamespacePreparePayloadRef<'_>,
        prepare_sequence: u64,
    ) -> Option<Self> {
        if prepare_sequence == 0 {
            return None;
        }
        let (file_system_id, operation_id, status, shard_id, owner_epoch, payload_digest) =
            match payload {
                DfsNamespacePreparePayloadRef::Canonical {
                    file_system_id,
                    operation_id,
                    status,
                    prepare,
                } => {
                    if prepare.file_system_id != **file_system_id
                        || prepare.operation_id != *operation_id
                        || prepare.status != **status
                    {
                        return None;
                    }
                    (
                        *file_system_id,
                        *operation_id,
                        *status,
                        &prepare.shard_id,
                        prepare.owner_epoch,
                        prepare.operation_digest.as_str(),
                    )
                }
                DfsNamespacePreparePayloadRef::PackedV1 {
                    file_system_id,
                    operation_id,
                    status,
                    prepare,
                } => (
                    *file_system_id,
                    *operation_id,
                    *status,
                    prepare.shard_id(),
                    prepare.owner_epoch(),
                    prepare.operation_digest(),
                ),
                DfsNamespacePreparePayloadRef::PackedV2 {
                    file_system_id,
                    operation_id,
                    status,
                    prepare,
                } => (
                    *file_system_id,
                    *operation_id,
                    *status,
                    prepare.shard_id(),
                    prepare.owner_epoch(),
                    prepare.operation_digest(),
                ),
            };
        Some(Self {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            owner_epoch,
            operation_id: operation_id.to_string(),
            status: status.clone(),
            prepare_sequence,
            payload_digest: payload_digest.to_string(),
        })
    }

    pub(crate) fn from_canonical_prepare(
        prepare: &DfsNamespacePrepareRecord,
        prepare_sequence: u64,
        canonical_payload_digest: String,
    ) -> Option<Self> {
        (prepare_sequence > 0 && prepare.operation_digest == canonical_payload_digest).then(|| {
            Self {
                file_system_id: prepare.file_system_id.clone(),
                shard_id: prepare.shard_id.clone(),
                owner_epoch: prepare.owner_epoch,
                operation_id: prepare.operation_id.clone(),
                status: prepare.status.clone(),
                prepare_sequence,
                payload_digest: canonical_payload_digest,
            }
        })
    }

    pub(crate) fn confirms(
        &self,
        shard: &DfsMetadataShardConfig,
        operation: &crate::DfsMetadataAuthorityOperation,
    ) -> bool {
        let crate::DfsMetadataAuthorityOperation::NamespaceMutation {
            operation_id,
            file_system_id,
            prepare_sequence,
            payload_digest,
        } = operation
        else {
            return false;
        };
        self.status == DfsNamespacePrepareStatus::Prepared
            && self.file_system_id == shard.file_system_id
            && self.shard_id.0 == shard.shard_id
            && self.owner_epoch == shard.epoch
            && &self.operation_id == operation_id
            && &self.file_system_id == file_system_id
            && self.prepare_sequence == *prepare_sequence
            && &self.payload_digest == payload_digest
    }
}

/// Result of one replica journal apply plus any proof computed from entries
/// that became durable during that exact call.
#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsMetadataReplicaApplyReport {
    applied_entries: usize,
    namespace_authority_proof: Option<DfsValidatedNamespaceAuthorityProof>,
    durable_prefix_proof: Option<DfsMetadataPrefixProof>,
    materialization_pending: bool,
}

impl DfsMetadataReplicaApplyReport {
    pub(crate) fn without_authority_proofs(applied_entries: usize) -> Self {
        Self {
            applied_entries,
            namespace_authority_proof: None,
            durable_prefix_proof: None,
            materialization_pending: false,
        }
    }

    pub(crate) fn with_namespace_authority_proof(
        applied_entries: usize,
        namespace_authority_proof: Option<DfsValidatedNamespaceAuthorityProof>,
    ) -> Self {
        Self {
            applied_entries,
            namespace_authority_proof,
            durable_prefix_proof: None,
            materialization_pending: false,
        }
    }

    pub(crate) fn with_durable_ingest(
        applied_entries: usize,
        namespace_authority_proof: Option<DfsValidatedNamespaceAuthorityProof>,
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
    ) -> Option<&DfsValidatedNamespaceAuthorityProof> {
        self.namespace_authority_proof
            .as_ref()
            .filter(|proof| proof.confirms(shard, operation))
    }
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
    pub prepared: Option<DfsNamespacePrepareRecord>,
    pub history_expired: bool,
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
pub trait DfsMetadataBackend: std::fmt::Debug + Send {
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

    /// Create or replace a filesystem record and journal the mutation.
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry>;

    /// Fetch a filesystem record by id.
    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>>;

    /// Ensure a filesystem record exists, returning a journal entry only when
    /// storage changed.
    fn ensure_file_system(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        if self
            .get_file_system(&file_system.file_system_id)?
            .as_ref()
            .is_some_and(|current| {
                crate::placement::dfs_file_system_semantically_eq(current, file_system)
            })
        {
            return Ok(None);
        }
        self.put_file_system(file_system).map(Some)
    }

    /// Ensure a filesystem record exists on one exact metadata shard.
    ///
    /// Single-shard backends delegate to their ordinary ensure. Sharded
    /// routers override this so a primary never authors the same filesystem
    /// record on sibling shards for which it is only a replica.
    fn ensure_file_system_on_shard(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        self.ensure_file_system(file_system)
    }

    /// List all filesystem records known to this backend.
    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>>;

    /// Create or replace an object metadata record and journal the mutation.
    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry>;

    /// Add a replica location only while the durable object still names the
    /// expected visible version and content. Implementations must fence the
    /// check and update as one metadata write so a concurrent rename or unlink
    /// cannot be followed by a stale location update that resurrects the old
    /// path. Returns the current object plus whether a journal entry was
    /// appended, or `None` when the durable object has moved, advanced,
    /// disappeared, or become a tombstone.
    fn add_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: &str,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectLocationUpdate>>;

    /// Store several object records using the backend's default single-record
    /// path unless the implementation overrides it with an atomic batch.
    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::with_capacity(objects.len());
        for object in objects {
            entries.push(self.put_object(object)?);
        }
        Ok(entries)
    }

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
    ) -> Result<Option<DfsObjectMetadata>> {
        self.get_object(object_id)
    }

    /// Fetch the durable whole-file advisory-lock aggregate for an object.
    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>>;

    /// Apply one advisory-lock command as a single journaled state transition.
    ///
    /// Synchronous distributed callers must replicate the result's shard
    /// through `durable_through_sequence` before acknowledging success.
    fn apply_advisory_lock_command(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockApplyResult>;

    /// Install an exact aggregate as part of a serialized object-shard move.
    ///
    /// This is not a lock-operation API. The destination must install the
    /// object first, reject a conflicting aggregate, and epoch-fence/warm the
    /// copied state before issuing any new grant.
    fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>>;

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
    ) -> Result<Option<DfsObjectMetadata>> {
        self.get_object_by_path(file_system_id, path)
    }

    /// Capture every object-version and path-occupant precondition for one
    /// namespace prepare on an exact shard.
    ///
    /// The compatibility implementation uses the existing point-read API.
    /// Backends that expose immutable table snapshots should override this so
    /// discovery of current object paths, path occupants, and occupant versions
    /// all observe one table generation. Prepare and commit still revalidate
    /// the returned values while holding the metadata write lock.
    fn namespace_prepare_expectations(
        &self,
        shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )> {
        namespace_prepare_expectations_from_lookups(
            transaction,
            |object_id| self.get_object_on_shard(shard_id, object_id),
            |file_system_id, path| self.get_object_by_path_on_shard(shard_id, file_system_id, path),
        )
    }

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

    /// Create or replace an object-version record and journal the mutation.
    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry>;

    /// Durably retain one immutable metadata version for an independent
    /// checkpoint or recovery consumer.
    ///
    /// This is intentionally unrelated to namespace path pins or content
    /// placement pins. Implementations must journal the pin before reporting
    /// success.
    fn put_version_pin(
        &mut self,
        pin: &DfsMetadataVersionPinRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _ = pin;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support durable version pins".to_string(),
        ))
    }

    /// Remove one exact consumer's durable version pin. Other pin identities
    /// for the same immutable version remain authoritative GC roots.
    fn remove_version_pin(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        pin_id: &str,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _ = (file_system_id, object_id, version, pin_id);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support durable version pins".to_string(),
        ))
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

    /// Preinstall the exact immutable-version and durable-pin family on the
    /// shard selected by `object.path`, before a cross-shard adoption makes
    /// that object visible there. Implementations must reconcile stale copied
    /// pins to the exact supplied set.
    fn install_cross_shard_version_pin_family(
        &mut self,
        object: &DfsObjectMetadata,
        versions: &[DfsObjectVersionRecord],
        pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<()> {
        let _ = (object, versions, pins);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support fenced cross-shard version-pin installation"
                .to_string(),
        ))
    }

    /// Reconcile one deterministic cross-primary handoff's source pin set on
    /// the destination. Implementations must update provenance and every
    /// provenance-owned pin as one journal mutation.
    fn reconcile_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
        versions: &[DfsObjectVersionRecord],
        source_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<bool> {
        let _ = (transfer_id, object, source_path, versions, source_pins);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support durable cross-shard pin transfers".to_string(),
        ))
    }

    /// Journal the completion of a destination pin transfer after the source
    /// shard's fenced eviction is durable. Missing provenance is an
    /// idempotent already-finalized result.
    fn finalize_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let _ = (transfer_id, object, source_path);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support cross-shard pin-transfer finalization".to_string(),
        ))
    }

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

    /// Install one exact active pin-transfer record while physically moving an
    /// authoritative object family between metadata shards.
    ///
    /// Unlike ordinary handoff reconciliation, this operation preserves the
    /// source record's identity, timestamps, and owned-pin set exactly. The
    /// transfer and every owned pin must become durable in one journal
    /// mutation so a crash cannot leave unowned copied pins behind.
    fn install_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        transfer: &DfsCrossShardPinTransferRecord,
        versions: &[DfsObjectVersionRecord],
    ) -> Result<()> {
        let _ = (transfer, versions);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support authoritative pin-transfer migration".to_string(),
        ))
    }

    /// Remove one target-side transfer record before replacing a physically
    /// migrated object's durable pin family. Missing provenance is an
    /// idempotent already-removed result; owned pins remain for the subsequent
    /// explicit pin-clear stage.
    fn remove_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        transfer_id: &str,
    ) -> Result<()> {
        let _ = (file_system_id, object_id, transfer_id);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support authoritative pin-transfer removal".to_string(),
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

    /// Store an object record and its version. Implementations may override
    /// this to persist both records in one durable metadata batch.
    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object_entry = self.put_object(object)?;
        let version_entry = self.put_version(version)?;
        Ok(vec![object_entry, version_entry])
    }

    /// Atomically commit an existing object's next visible content version
    /// together with its write intent and durable receipts.
    ///
    /// Implementations must validate the intent base version and receipt
    /// threshold while holding the same metadata write lock used to publish the
    /// visible-version transition. New-object publication remains a namespace
    /// transaction and must not use this method.
    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>>;

    /// Atomically publish a new object and its parent-directory touch together
    /// with the write intent and durable receipts that authorize the first
    /// visible version.
    ///
    /// The namespace transaction must contain the child and its first visible
    /// version. Implementations must preserve replay-prefix safety: the child
    /// remains invisible before the transaction entry, and any visible prefix
    /// already contains the commit-ready intent and receipt threshold.
    fn commit_create_intent_receipts_and_namespace(
        &mut self,
        pending_intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        transaction: &DfsNamespaceTransaction,
        committed_intent: &DfsWriteIntentRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>>;

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

    /// Create or replace a replica receipt and journal the mutation.
    fn put_receipt(&mut self, receipt: &DfsReplicaReceiptRecord)
    -> Result<DfsMetadataJournalEntry>;

    /// Remove a replica receipt and journal the mutation when one existed.
    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>>;

    /// Store a receipt with optional object context. Sharded backends use the
    /// object to avoid an extra lookup when routing the receipt.
    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_receipt(receipt)
    }

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

    /// Create or replace a write-intent record and journal the mutation.
    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry>;

    /// Store a write intent with optional object context for sharded routing.
    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_intent(intent)
    }

    /// Install one full intent from a source shard that is still authoritative
    /// for the object's metadata family.
    ///
    /// Unlike an ordinary retry, this may cross a destination retention floor:
    /// the source's durable record is the exact history being transferred, not
    /// an unproven request being admitted as fresh.
    fn put_migrated_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Install one exact compact intent receipt while moving an authoritative
    /// object metadata family. Implementations must preserve the receipt shape
    /// instead of expanding it through `DfsWriteIntentRecord`.
    fn put_intent_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsWriteIntentReceipt,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Merge source-shard receipt-eviction floors into the destination through
    /// the normal authoritative journal/table path.
    fn merge_intent_receipt_retention_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        retention: &DfsWriteIntentReceiptRetentionState,
    ) -> Result<DfsMetadataJournalEntry>;

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

    /// Store a write intent and its receipts. Implementations may override
    /// this to persist them in one durable metadata batch.
    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::with_capacity(1 + receipts.len());
        entries.push(self.put_intent_for_object(object, intent)?);
        for receipt in receipts {
            entries.push(self.put_receipt_for_object(object, receipt)?);
        }
        Ok(entries)
    }

    /// Fetch a write intent by object id and intent id.
    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;

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
    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage>;
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
    fn all_conflicts(&self) -> Result<Vec<DfsConflictRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.conflict_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.conflicts, page.next_cursor))
        })
    }

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

        walk_metadata_pages(|conflict_cursor| {
            let conflict_page = self.conflicts_for_object_page(
                &object.object_id,
                conflict_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.open_conflict_count = summary.open_conflict_count.saturating_add(
                conflict_page
                    .conflicts
                    .iter()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count(),
            );
            Ok(conflict_page.next_cursor)
        })?;

        summary.pending_repair_count =
            summary
                .pending_repair_count
                .saturating_add(count_pending_repairs_for_object(
                    object,
                    &object_intents,
                    &receipts_by_version,
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

    /// Publish a version as the visible version of an object after validating
    /// expected-version and receipt requirements.
    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry>;

    /// Atomically apply a namespace transaction and journal the mutation.
    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Durably reserve one shard-local namespace transaction without making
    /// any of its object or path changes visible.
    ///
    /// Implementations must validate all object-version and path-occupant
    /// expectations, reject overlap with another active prepare, and persist
    /// only the hidden prepare record while holding the metadata write lock.
    fn prepare_namespace_transaction(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// In-place counterpart used by large foreground prepares.
    ///
    /// The caller keeps ownership for later in-memory publication and for
    /// precise conflict-to-path error mapping. The default normalizes this
    /// allocation once and delegates to the canonical entry point below.
    fn prepare_namespace_transaction_in_place(
        &mut self,
        prepare: &mut DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        self.prepare_normalized_namespace_transaction_in_place(DfsNormalizedNamespacePrepare::new(
            prepare,
        ))
    }

    /// Persist a namespace prepare that is already in canonical normalized
    /// order, without repeating the large-vector normalization passes.
    ///
    /// The unforgeable wrapper proves that the DFS boundary normalized the
    /// record and retained its exclusive mutable borrow. Backends that validate
    /// routing or persist packed records should override this method so the
    /// same allocation flows through every layer. The default preserves
    /// compatibility with borrowed-only backends.
    fn prepare_normalized_namespace_transaction_in_place(
        &mut self,
        prepare: DfsNormalizedNamespacePrepare<'_>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.prepare_namespace_transaction(prepare.as_record())
    }

    /// Move an exact durable namespace prepare to a newer shard-owner epoch.
    ///
    /// Implementations must compare `expected_operation_digest` and
    /// `expected_owner_epoch` while holding the metadata write lock, require
    /// the current and rebound records to remain `Prepared`, and preserve the
    /// operation id, filesystem/shard, expectations, and transaction exactly.
    /// Only the owner epoch and its derived operation digest may change.
    fn rebind_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
        rebound_prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Revalidate and atomically publish an existing hidden namespace prepare.
    ///
    /// The digest and owner epoch fence operation-id reuse. Implementations
    /// must apply the prepared namespace transaction and mark the record
    /// committed in one durable metadata table batch.
    fn commit_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Commit a prepare whose exact durable shard is already known.
    ///
    /// The default preserves compatibility for unsharded backends. Sharded
    /// routers override this to avoid probing every local shard after the
    /// authority protocol has already returned the prepare and its shard.
    fn commit_prepared_namespace_transaction_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = shard_id;
        self.commit_prepared_namespace_transaction(
            operation_id,
            expected_operation_digest,
            expected_owner_epoch,
        )
    }

    /// Release an exact hidden namespace prepare without publishing it.
    ///
    /// The digest and owner epoch fence operation-id reuse. Implementations
    /// must reject a committed prepare and durably move a prepared record to
    /// `Aborted`, releasing its active object/path footprint atomically with
    /// the journaled terminal marker.
    fn abort_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Fetch a durable hidden namespace prepare by operation id.
    fn get_namespace_prepare(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let _ = operation_id;
        Ok(None)
    }

    /// Return only hidden namespace prepares which still require recovery.
    ///
    /// Replica snapshot export is intentionally not the inspection API here:
    /// it clones and fingerprints every metadata table even though recovery
    /// needs only this bounded active subset.
    fn list_prepared_namespace_prepares(&self) -> Result<Vec<DfsNamespacePrepareRecord>>;

    /// Move an exact non-terminal write intent to a newer shard-owner epoch.
    ///
    /// The compare and replacement must be atomic. Implementations must reject
    /// any payload/status change and may change only `owner_epoch` plus the
    /// bookkeeping `updated_at` timestamp.
    fn rebind_prepared_write_intent(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        expected_intent: &DfsWriteIntentRecord,
        rebound_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Fetch a recorded namespace transaction by id when the backend supports
    /// transaction recovery.
    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let _ = transaction_id;
        Ok(None)
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
    fn gc_metadata_history(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let _ = options;
        Ok(DfsMetadataHistoryGcReport::default())
    }

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

    /// Records that an object moved to a metadata shard owned by another
    /// node: journals a same-object-id tombstone in the shard that held the
    /// record (so that shard's replicas observe the departure) and repoints
    /// the local object→shard index at the destination shard. Single-shard
    /// backends never split authority, so the default is a no-op.
    fn evict_object_record_for_foreign_shard_move(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let _ = (moved_object, source_path);
        Ok(())
    }

    /// Complete a cross-primary source eviction only if the source journal
    /// head and exact durable pin set still match what the destination
    /// acknowledged. Implementations must remove the copied source pins and
    /// author the source-eviction marker under one metadata writer fence.
    fn evict_object_record_for_foreign_shard_move_with_pin_fence(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        expected_source_object: &DfsObjectMetadata,
        expected_source_journal_sequence: u64,
        expected_version_pins: &[DfsMetadataVersionPinRecord],
        pin_transfer_id: Option<&str>,
    ) -> Result<()> {
        let _ = (
            moved_object,
            source_path,
            expected_source_object,
            expected_source_journal_sequence,
            expected_version_pins,
            pin_transfer_id,
        );
        Err(MetadataBackendError::Serde(
            "metadata backend does not support fenced cross-shard source eviction".to_string(),
        ))
    }

    /// Durably create or advance one routed namespace request admission record
    /// on the exact authority shard that will execute the operation.
    fn put_namespace_request_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        request: &DfsNamespaceRequestRecord,
    ) -> Result<DfsMetadataJournalEntry>;

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
    /// Sharded implementations inspect each locally retained shard once and
    /// read all indexes from that table image. This compatibility
    /// implementation preserves existing backend behavior; durable local
    /// backends override it with a single table snapshot per shard.
    fn namespace_request_admission_snapshot(
        &mut self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<DfsNamespaceRequestAdmissionSnapshot> {
        let receipt = self.get_namespace_request_receipt(requester_node_id, request_id)?;
        let request = self.get_namespace_request(requester_node_id, request_id)?;
        let prepared =
            self.get_prepared_namespace_prepare_for_request(requester_node_id, request_id)?;
        let history_expired = if receipt.is_none() && request.is_none() && prepared.is_none() {
            self.namespace_request_history_expired_on_shard(
                shard_id,
                requester_node_id,
                request_id,
            )?
        } else {
            false
        };
        Ok(DfsNamespaceRequestAdmissionSnapshot {
            request,
            receipt,
            prepared,
            history_expired,
        })
    }

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

    /// Return the hidden prepared transaction, if any, that owns the terminal
    /// outcome for an exact still-pending routed namespace request.
    fn get_prepared_namespace_prepare_for_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>>;

    /// Create or replace a namespace maintenance job.
    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _ = job;
        Ok(())
    }

    /// Fetch a namespace maintenance job by id.
    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let _ = job_id;
        Ok(None)
    }

    /// Persist compact terminal evidence for a namespace job.
    ///
    /// This does not remove the full job. Metadata history GC journals that
    /// transition separately after every recovery root has been checked.
    fn put_namespace_job_receipt(&mut self, receipt: &DfsNamespaceJobReceipt) -> Result<()> {
        let _ = receipt;
        Ok(())
    }

    /// Fetch compact terminal namespace-job evidence by durable job id.
    fn get_namespace_job_receipt(&self, job_id: &str) -> Result<Option<DfsNamespaceJobReceipt>> {
        let _ = job_id;
        Ok(None)
    }

    /// Find completed job evidence for an exact durable routed request.
    ///
    /// Receipts written from legacy jobs have no `origin`; implementations
    /// conservatively retain the historical path/kind/time fallback for those
    /// rows while requiring an exact identity match for new receipts.
    fn completed_namespace_job_receipt_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<Option<DfsNamespaceJobReceipt>> {
        let _ = (
            file_system_id,
            root_path,
            target_path,
            kinds,
            created_at,
            origin,
        );
        Ok(None)
    }

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

    /// Create or replace a conflict record and journal the mutation.
    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry>;

    /// Fetch one conflict record by object id and conflict id.
    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;

    /// Compatibility helper that drains all conflicts for one object.
    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.conflicts_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.conflicts, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of conflicts for one object.
    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage>;

    /// Remove one conflict record and journal the mutation when one existed.
    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;

    /// Update one conflict status and journal the mutation when the record
    /// changed.
    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>>;

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
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        Ok(None)
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
    /// stay fenced until they separately prove and observe it.
    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _active_shard: &DfsMetadataShardConfig,
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
    /// journal batch. Durable local backends override this to borrow the bytes
    /// already stored in the authoritative journal; the default remains a
    /// deterministic encode-once compatibility path.
    fn journal_frames_for_batch(
        &self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Vec<DfsMetadataJournalFrameRecord>> {
        batch
            .entries
            .iter()
            .map(crate::metadata_journal::encode_metadata_journal_frame_record)
            .collect()
    }

    /// Return the next journal sequence that should be sent to a target node
    /// for a shard.
    fn metadata_replication_cursor(
        &self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        Ok(0)
    }

    /// Persist the next journal sequence that should be sent to a target node
    /// for a shard.
    fn put_metadata_replication_cursor(
        &mut self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
        _next_after_sequence: u64,
    ) -> Result<()> {
        Ok(())
    }

    /// Return the replica-local exact-checkpoint floor persisted before a
    /// successful prefix acknowledgement.
    fn metadata_prefix_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>> {
        Ok(None)
    }

    /// Persist a monotonic exact-checkpoint floor before acknowledging the
    /// corresponding prefix probe.
    fn put_metadata_prefix_ack_floor(&mut self, _floor: &DfsMetadataPrefixAckFloor) -> Result<()> {
        Ok(())
    }

    /// Return the replica-local floor persisted before an exact ordinary-write
    /// authority acknowledgement.
    fn metadata_authority_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
        Ok(None)
    }

    /// Persist a monotonic ordinary-write authority floor before returning the
    /// corresponding request nonce.
    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()> {
        Err(MetadataBackendError::Serde(format!(
            "metadata backend cannot durably fence authority acknowledgement for shard {} sequence {}",
            floor.shard_id.0, floor.journal_sequence
        )))
    }

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

    /// Persist and materialize a replicated journal batch.
    ///
    /// `batch.shard_id` is the logical shard identity and must match the
    /// target backend's shard. Replicas use distinct node/storage identities,
    /// not distinct logical shard IDs.
    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize>;

    /// Persist a replicated batch using exact source evidence validated by the
    /// metadata replication boundary. Implementations with no membership
    /// author fence may retain the ordinary mutation path; local durable
    /// backends override this to admit only an exact activated generation when
    /// the ordinary path is fenced.
    fn apply_journal_batch_with_replica_permit(
        &mut self,
        _permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        self.apply_journal_batch(batch)
    }

    /// Apply a replicated batch and return compact authority evidence derived
    /// from records newly materialized by this exact call.
    ///
    /// Backends that do not retain the decoded apply result use the compatible
    /// default below. Callers then verify an authority probe from durable tables
    /// exactly as before.
    fn apply_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.apply_journal_batch_with_replica_permit(permit, batch)
            .map(DfsMetadataReplicaApplyReport::without_authority_proofs)
    }

    /// Persist and validate an exact replica batch without requiring the
    /// rebuildable materialized table cache to reach the same boundary before
    /// returning. Durable local backends override this for versioned authority
    /// probes; compatibility backends retain the stronger synchronous apply.
    fn persist_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.apply_journal_batch_with_replica_permit_report(permit, batch)
    }

    fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport> {
        if !batch.entries.is_empty() || frames.is_empty() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} exact-frame ingest requires one carrier",
                batch.shard_id.0
            )));
        }
        let entries = frames
            .iter()
            .map(crate::metadata_journal::decode_metadata_journal_frame_record)
            .collect::<Result<Vec<_>>>()?;
        self.persist_journal_batch_with_replica_permit_report(
            permit,
            &DfsMetadataJournalBatch {
                shard_id: batch.shard_id.clone(),
                entries,
                next_after_sequence: batch.next_after_sequence,
            },
        )
    }

    /// Persist an authority operation whose exact target prefix is known.
    ///
    /// Durable local backends use `expected_target_prefix` to authenticate a
    /// delayed authority operation after a later ordinary catch-up prefix has
    /// already materialized. Compatibility backends keep their stronger
    /// synchronous apply boundary; the common authority verifier still checks
    /// their resulting materialized prefix before acknowledging the probe.
    fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        if !expected_target_prefix.is_well_formed()
            || expected_target_prefix.journal_sequence != batch.next_after_sequence
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} authority ingest has an invalid expected prefix at sequence {}",
                batch.shard_id.0, batch.next_after_sequence
            )));
        }
        self.persist_journal_frames_with_replica_permit_report(permit, batch, frames)
    }

    /// Materialize any journal suffix previously admitted at the durable ACK
    /// boundary. Promotion, snapshots, and ordinary authoritative writes must
    /// continue to use their existing synchronous refresh fences.
    fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        Ok(0)
    }

    /// Replace local metadata tables with a replicated snapshot.
    ///
    /// `source_shard_id` must match the target backend's logical shard
    /// identity; snapshot replication does not adopt or rename shards.
    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool>;

    /// Replace local metadata tables using exact source evidence validated by
    /// the metadata replication boundary. The local durable implementation
    /// preserves any activated author fence and never binds authority through
    /// this path.
    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        _permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        self.replace_with_replicated_snapshot(source_shard_id, replayed_through_sequence, snapshot)
    }

    /// Create a subscription cursor for journal polling.
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;

    /// Poll journal entries for a subscription.
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}
