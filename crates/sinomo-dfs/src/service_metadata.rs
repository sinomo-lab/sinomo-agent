//! Service-local metadata mutations and tombstone index maintenance.
//!
//! Public methods here update DFS metadata state and keep the in-memory service
//! view synchronized with the durable metadata backend when one is configured.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use sinomo_core::node::NodeId;
use time::OffsetDateTime;

use crate::metadata_pages::collect_metadata_pages;
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::posix_model;
use crate::{
    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE, DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
    DfsControlPlaneStore, DfsDistributedError, DfsError, DfsFileSystem, DfsFileSystemConfig,
    DfsFileSystemId, DfsLocalEventualBatchWriteProof, DfsMessageTransport, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsNamespaceObjectVersionExpectation, DfsNamespacePathOccupantExpectation,
    DfsNamespaceTransaction, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsPreparedMetadataEffect, DfsPreparedOperationPayload, DfsPreparedOperationScope,
    DfsReplicaReceiptRecord, DfsVersion, DfsWriteIntentRecord, MetadataBackendError,
    NetworkedDfsService, dfs_perf_trace, metadata_path, normalize_current_locations,
    path_index_key,
};

#[derive(Debug)]
enum NamespaceTransactionApplyError {
    Dfs(DfsError),
    PrepareConflict {
        path: String,
        object_id: DfsObjectId,
        expected: DfsVersion,
        current: DfsVersion,
    },
}

impl NamespaceTransactionApplyError {
    fn into_dfs_error(self) -> DfsError {
        match self {
            Self::Dfs(error) => error,
            Self::PrepareConflict {
                object_id,
                expected,
                current,
                ..
            } => DfsError::from(MetadataBackendError::Conflict {
                object_id,
                expected,
                current,
            }),
        }
    }

    fn into_posix_error(self) -> posix_model::DfsPosixError {
        match self {
            Self::Dfs(error) => posix_model::DfsPosixError::from(error),
            Self::PrepareConflict {
                path,
                object_id,
                expected,
                current,
            } => posix_model::DfsPosixError::Conflict {
                path,
                reason: format!(
                    "object {object_id} changed from version {expected} to {current} after namespace expectations were captured"
                ),
            },
        }
    }
}

impl From<DfsError> for NamespaceTransactionApplyError {
    fn from(error: DfsError) -> Self {
        Self::Dfs(error)
    }
}

impl From<MetadataBackendError> for NamespaceTransactionApplyError {
    fn from(error: MetadataBackendError) -> Self {
        Self::Dfs(DfsError::from(error))
    }
}

#[cfg(test)]
type NamespacePrepareAfterExpectationsTestHook =
    Box<dyn FnOnce(&mut dyn crate::DfsMetadataBackend)>;

#[cfg(test)]
std::thread_local! {
    static NAMESPACE_PREPARE_AFTER_EXPECTATIONS_TEST_HOOK:
        std::cell::RefCell<Option<NamespacePrepareAfterExpectationsTestHook>> =
            const { std::cell::RefCell::new(None) };
    static NAMESPACE_PREPARE_EXPECTATION_CAPTURE_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) struct NamespacePrepareAfterExpectationsTestHookGuard;

#[cfg(test)]
impl Drop for NamespacePrepareAfterExpectationsTestHookGuard {
    fn drop(&mut self) {
        NAMESPACE_PREPARE_AFTER_EXPECTATIONS_TEST_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
pub(crate) fn install_namespace_prepare_after_expectations_test_hook(
    hook: impl FnOnce(&mut dyn crate::DfsMetadataBackend) + 'static,
) -> NamespacePrepareAfterExpectationsTestHookGuard {
    NAMESPACE_PREPARE_AFTER_EXPECTATIONS_TEST_HOOK.with(|slot| {
        let previous = slot.borrow_mut().replace(Box::new(hook));
        assert!(
            previous.is_none(),
            "namespace-prepare expectation test hook is already installed on this thread"
        );
    });
    NamespacePrepareAfterExpectationsTestHookGuard
}

#[cfg(test)]
fn run_namespace_prepare_after_expectations_test_hook(backend: &mut dyn crate::DfsMetadataBackend) {
    NAMESPACE_PREPARE_AFTER_EXPECTATIONS_TEST_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook(backend);
        }
    });
}

#[cfg(test)]
pub(crate) fn reset_namespace_prepare_expectation_capture_calls() {
    NAMESPACE_PREPARE_EXPECTATION_CAPTURE_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn namespace_prepare_expectation_capture_calls() -> usize {
    NAMESPACE_PREPARE_EXPECTATION_CAPTURE_CALLS.get()
}

pub(crate) struct NamespacePrepareExpectationBundle {
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) expected_object_versions: BTreeMap<DfsObjectId, Option<DfsVersion>>,
    pub(crate) expected_path_occupants: BTreeMap<String, Option<DfsObjectId>>,
}

fn carries_namespace_content(object: &DfsObjectMetadata) -> bool {
    matches!(
        object.kind,
        DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
    ) && object.content_hash.is_some()
}

// Complete only metadata successors of an actual same-object head. The kernel
// still checks the captured head, unchanged grant, and its original W receipts.
fn complete_namespace_carried_versions(
    backend: &dyn crate::DfsMetadataBackend,
    transaction: &mut DfsNamespaceTransaction,
) -> Result<(), DfsError> {
    for object in transaction
        .objects
        .iter()
        .filter(|object| carries_namespace_content(object))
    {
        let supplied = transaction.versions.iter_mut().find(|version| {
            version.file_system_id == object.file_system_id
                && version.object_id == object.object_id
                && version.version == object.version
        });
        if supplied
            .as_ref()
            .is_some_and(|version| version.content_durability.is_some())
        {
            continue;
        }
        let Some(current) = backend.get_object(&object.object_id)? else {
            continue;
        };
        if current.version == object.version
            || current.file_system_id != transaction.file_system_id
            || object.file_system_id != transaction.file_system_id
            || current.content_hash != object.content_hash
            || current.content_size_bytes != object.content_size_bytes
        {
            continue;
        }
        let original = backend
            .get_version(&current.object_id, &current.version)?
            .ok_or_else(|| DfsError::Metadata {
                message: "namespace metadata successor has no actual original version".into(),
            })?;
        if !original.visible
            || original.file_system_id != object.file_system_id
            || original.object_id != object.object_id
            || original.content_hash != object.content_hash
            || original.size_bytes != object.content_size_bytes
            || original.size_bytes.is_none()
            || original.content_durability.is_none()
        {
            return Err(DfsError::Metadata {
                message: "namespace metadata successor has no unchanged recorded data generation"
                    .into(),
            });
        }
        if let Some(version) = supplied {
            if !version.visible
                || version.content_hash != object.content_hash
                || version.size_bytes != object.content_size_bytes
            {
                return Err(DfsError::Metadata {
                    message: "namespace metadata successor supplied a conflicting version".into(),
                });
            }
            version.content_durability = original.content_durability;
        } else {
            let mut version = crate::object_version_record(object, object.content_size_bytes);
            version.created_at = transaction.created_at;
            version.content_durability = original.content_durability;
            transaction.versions.push(version);
        }
    }
    Ok(())
}

// Exact retries obtain the generated version rows from the already committed
// immutable body. A later head must not turn completion into fresh planning.
fn complete_recorded_namespace_carried_versions(
    transaction: &mut DfsNamespaceTransaction,
    recorded: &DfsNamespaceTransaction,
) {
    for version in &recorded.versions {
        if !transaction.objects.iter().any(|object| {
            carries_namespace_content(object)
                && recorded.objects.contains(object)
                && version.visible
                && version.file_system_id == object.file_system_id
                && version.object_id == object.object_id
                && version.version == object.version
                && version.content_hash == object.content_hash
                && version.size_bytes == object.content_size_bytes
                && version.content_durability.is_some()
        }) {
            continue;
        }
        if let Some(supplied) = transaction.versions.iter_mut().find(|supplied| {
            supplied.file_system_id == version.file_system_id
                && supplied.object_id == version.object_id
                && supplied.version == version.version
        }) {
            if supplied.content_durability.is_none() {
                supplied.content_durability = version.content_durability.clone();
            }
        } else {
            transaction.versions.push(version.clone());
        }
    }
}

fn namespace_transaction_shard_paths(
    backend: &dyn crate::DfsMetadataBackend,
    transaction: &DfsNamespaceTransaction,
) -> Result<BTreeSet<String>, DfsError> {
    let transaction_object_ids = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    let mut paths = transaction
        .objects
        .iter()
        .map(|object| object.path.clone())
        .collect::<BTreeSet<_>>();
    let pathless_object_ids = transaction
        .versions
        .iter()
        .map(|version| version.object_id.clone())
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
        .filter(|object_id| !transaction_object_ids.contains(object_id))
        .collect::<BTreeSet<_>>();
    for object_id in pathless_object_ids {
        if let Some(object) = backend.get_object(&object_id)? {
            paths.insert(object.path);
        }
    }
    Ok(paths)
}

pub(crate) fn namespace_prepare_expectations(
    backend: &dyn crate::DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    transaction: &DfsNamespaceTransaction,
) -> Result<
    (
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    ),
    DfsError,
> {
    #[cfg(test)]
    NAMESPACE_PREPARE_EXPECTATION_CAPTURE_CALLS.with(|calls| {
        calls.set(calls.get().saturating_add(1));
    });
    backend
        .namespace_prepare_expectations(shard_id, transaction)
        .map_err(DfsError::from)
}

/// Read-only inputs from the same service generation. This does not acquire
/// admission or authorize publication; the existing complete-payload kernel
/// still consumes the original expectations and real durable receipts.
struct LocalEventualBatchReadView<'a> {
    node_id: &'a NodeId,
    metadata: &'a crate::DfsMetadataStore,
    metadata_backend: Option<&'a dyn crate::DfsMetadataBackend>,
    namespace_execution: &'a crate::service_state::DfsNamespaceExecution,
}

fn validate_local_eventual_batch_commit_from_view(
    view: LocalEventualBatchReadView<'_>,
    requester_node_id: &NodeId,
    transaction: &DfsNamespaceTransaction,
    writes: &[DfsLocalEventualBatchWriteProof],
    selected_shard: Option<&DfsMetadataShardConfig>,
) -> Result<Option<NamespacePrepareExpectationBundle>, posix_model::DfsPosixError> {
    let reject =
        |message: String| posix_model::DfsPosixError::Dfs(DfsError::Distributed { message });
    if transaction.objects.is_empty() || writes.is_empty() {
        return Err(reject(
            "local-eventual batch requires at least one proven file write".to_string(),
        ));
    }
    if !transaction.advisory_lock_states.is_empty()
        || transaction.file_version_pins.is_some()
        || transaction
            .versions
            .iter()
            .any(|version| !version.retention.is_empty() || !version.module_metadata.is_empty())
    {
        return Err(reject(
            "local-eventual batch may not mutate advisory-lock state".to_string(),
        ));
    }

    let authority_node_id = selected_shard
        .map(|shard| &shard.primary_node_id)
        .unwrap_or(view.node_id);
    let shard_id = selected_shard.map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()));
    let current_object =
        |object_id: &DfsObjectId| -> Result<Option<DfsObjectMetadata>, posix_model::DfsPosixError> {
            if let Some(backend) = view.metadata_backend {
                let object = match shard_id.as_ref() {
                    Some(shard_id) => backend.get_object_on_shard(shard_id, object_id),
                    None => backend.get_object(object_id),
                }
                .map_err(DfsError::from)
                .map_err(posix_model::DfsPosixError::from)?;
                view.namespace_execution.observe_object(
                    &transaction.file_system_id,
                    object_id,
                    object.as_ref(),
                );
                if let Some(object) = object.as_ref() {
                    view.namespace_execution.observe_path(
                        &object.file_system_id,
                        &object.path,
                        Some(object),
                    );
                }
                return Ok(object);
            }
            Ok(view.metadata.object(object_id).cloned())
        };
    let current_path =
        |path: &str| -> Result<Option<DfsObjectMetadata>, posix_model::DfsPosixError> {
            if let Some(backend) = view.metadata_backend {
                let object = match shard_id.as_ref() {
                    Some(shard_id) => backend.get_object_by_path_on_shard(
                        shard_id,
                        &transaction.file_system_id,
                        path,
                    ),
                    None => backend.get_object_by_path(&transaction.file_system_id, path),
                }
                .map_err(DfsError::from)
                .map_err(posix_model::DfsPosixError::from)?;
                view.namespace_execution.observe_path(
                    &transaction.file_system_id,
                    path,
                    object.as_ref(),
                );
                return Ok(object);
            }
            Ok(view
                .metadata
                .object_by_normalized_posix_path(
                    &transaction.file_system_id,
                    &crate::posix_path::normalize_posix_path(path)
                        .map_err(|_| reject(format!("invalid local-eventual batch path {path}")))?,
                )
                .cloned())
        };

    let objects = transaction
        .objects
        .iter()
        .map(|object| (&object.object_id, object))
        .collect::<BTreeMap<_, _>>();
    if objects.len() != transaction.objects.len() {
        return Err(reject(
            "local-eventual batch repeats an object id".to_string(),
        ));
    }
    let paths = transaction
        .objects
        .iter()
        .map(|object| object.path.as_str())
        .collect::<BTreeSet<_>>();
    if paths.len() != transaction.objects.len() {
        return Err(reject(
            "local-eventual batch repeats a namespace path".to_string(),
        ));
    }
    let versions = transaction
        .versions
        .iter()
        .map(|version| (&version.object_id, version))
        .collect::<BTreeMap<_, _>>();
    if versions.len() != transaction.versions.len()
        || transaction.versions.len() != transaction.objects.len()
    {
        return Err(reject(
            "local-eventual batch requires exactly one visible version per object".to_string(),
        ));
    }
    let prepare_expectations = match (view.metadata_backend, shard_id.as_ref()) {
        (Some(backend), Some(shard_id)) => {
            let (expected_object_versions, expected_path_occupants, _) =
                namespace_prepare_expectations(backend, shard_id, transaction)?;
            Some(NamespacePrepareExpectationBundle {
                shard_id: shard_id.clone(),
                expected_object_versions: expected_object_versions
                    .into_iter()
                    .map(|expectation| (expectation.object_id, expectation.expected_version))
                    .collect(),
                expected_path_occupants: expected_path_occupants
                    .into_iter()
                    .map(|expectation| (expectation.path, expectation.expected_object_id))
                    .collect(),
            })
        }
        _ => None,
    };
    let batch_expected_object_versions = prepare_expectations
        .as_ref()
        .map(|expectations| &expectations.expected_object_versions);
    let batch_expected_path_occupants = prepare_expectations
        .as_ref()
        .map(|expectations| &expectations.expected_path_occupants);

    let mut writes_by_object = BTreeMap::new();
    for write in writes {
        let receipt = &write.receipt;
        let object_id = &receipt.receipt.object_id;
        if writes_by_object.insert(object_id, write).is_some() {
            return Err(reject(format!(
                "local-eventual batch repeats the write proof for object {object_id}"
            )));
        }
        let Some(object) = objects.get(object_id) else {
            return Err(reject(format!(
                "local-eventual batch write proof references unknown object {object_id}"
            )));
        };
        if object.kind != DfsObjectKind::File
            || object.policy != sinomo_core::DfsObjectPolicy::ReplicatedEventual
            || &object.owner_node_id != requester_node_id
            || object.current_locations.as_slice() != std::slice::from_ref(requester_node_id)
            || object.content_hash.is_none()
            || object.content_size_bytes.is_none()
        {
            return Err(reject(format!(
                "local-eventual batch object {object_id} is not a caller-owned single-replica file"
            )));
        }
        if receipt.receipt.file_system_id != transaction.file_system_id
            || receipt.receipt.committed_version != object.version
            || receipt.receipt.owner_node_id != *authority_node_id
            || receipt.replica_node_id != *requester_node_id
            || receipt.content_hash != object.content_hash
            || object.last_modified_by_event_id.as_ref() != Some(&receipt.receipt.event_id)
        {
            return Err(reject(format!(
                "local-eventual batch durable receipt does not match object {object_id}"
            )));
        }
        let version = versions.get(object_id).ok_or_else(|| {
            reject(format!(
                "local-eventual batch is missing visible version {} for object {object_id}",
                object.version
            ))
        })?;
        if !version.visible
            || version.file_system_id != transaction.file_system_id
            || version.version != object.version
            || version.content_hash != object.content_hash
            || version.size_bytes != object.content_size_bytes
            || version.locations != object.current_locations
            || version.created_by_event_id != object.last_modified_by_event_id
        {
            return Err(reject(format!(
                "local-eventual batch visible version does not match object {object_id}"
            )));
        }
        if version
            .content_durability
            .as_ref()
            .is_some_and(|plan| plan != &crate::content_durability::from_local_receipt(receipt))
        {
            return Err(reject(format!(
                "local-eventual batch durability plan does not match its single-replica proof for object {object_id}"
            )));
        }

        let current = match (
            write.expected_base_version.as_ref(),
            batch_expected_object_versions.and_then(|expectations| expectations.get(object_id)),
        ) {
            // The exact shard snapshot already proves that a create's
            // deterministic object id is absent. Avoid reopening the same
            // immutable table generation once per file.
            (None, Some(None)) => None,
            _ => current_object(object_id)?,
        };
        let create_path_is_occupied = if write.expected_base_version.is_none() {
            match batch_expected_path_occupants
                .and_then(|expectations| expectations.get(&object.path))
            {
                Some(None) => false,
                Some(Some(occupant_id)) => {
                    if current
                        .as_ref()
                        .is_some_and(|current| &current.object_id == occupant_id)
                    {
                        current
                            .as_ref()
                            .is_some_and(|current| current.kind != DfsObjectKind::Tombstone)
                    } else {
                        current_object(occupant_id)?
                            .is_some_and(|occupant| occupant.kind != DfsObjectKind::Tombstone)
                    }
                }
                None => current_path(&object.path)?
                    .is_some_and(|occupant| occupant.kind != DfsObjectKind::Tombstone),
            }
        } else {
            false
        };
        if write.expected_base_version.is_none() && create_path_is_occupied {
            return Err(posix_model::DfsPosixError::AlreadyExists {
                path: object.path.clone(),
            });
        }
        let observed_base = current
            .as_ref()
            .filter(|current| current.kind != DfsObjectKind::Tombstone)
            .map(|current| current.version.to_string())
            .unwrap_or_else(|| "absent-or-tombstoned".to_string());
        let expected_base = write
            .expected_base_version
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "absent-or-tombstoned".to_string());
        match (&write.expected_base_version, current) {
            (Some(expected_base), Some(current))
                if current.kind != DfsObjectKind::Tombstone
                    && current.version == *expected_base
                    && object.version != *expected_base
                    && current.path == object.path =>
            {
                let mut allowed = current;
                allowed.version = object.version.clone();
                allowed.content_hash = object.content_hash.clone();
                allowed.content_size_bytes = object.content_size_bytes;
                allowed.current_locations = object.current_locations.clone();
                allowed.modified_at_unix_nanos = object.modified_at_unix_nanos;
                allowed.changed_at_unix_nanos = object.changed_at_unix_nanos;
                allowed.last_modified_by_event_id = object.last_modified_by_event_id.clone();
                if allowed != **object {
                    return Err(reject(format!(
                        "local-eventual batch changes unauthorized metadata for object {object_id}"
                    )));
                }
            }
            (None, None)
            | (
                None,
                Some(DfsObjectMetadata {
                    kind: DfsObjectKind::Tombstone,
                    ..
                }),
            ) => {}
            _ => {
                return Err(posix_model::DfsPosixError::Conflict {
                    path: object.path.clone(),
                    reason: format!(
                        "object {object_id} base changed from {expected_base} to {observed_base} before the batch committed"
                    ),
                });
            }
        }
    }

    for object in &transaction.objects {
        if writes_by_object.contains_key(&object.object_id) {
            continue;
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(reject(format!(
                "local-eventual batch object {} has no durable write proof",
                object.object_id
            )));
        }
        let Some(current) = current_object(&object.object_id)? else {
            return Err(posix_model::DfsPosixError::Conflict {
                path: object.path.clone(),
                reason: "parent directory disappeared before the batch committed".to_string(),
            });
        };
        let mut parent_write_count = 0_usize;
        let mut expected_parent_version = None;
        for write in writes_by_object.values().filter(|write| {
            write.expected_base_version.is_none()
                && objects
                    .get(&write.receipt.receipt.object_id)
                    .and_then(|file| crate::posix_path::parent_posix_path(&file.path))
                    .as_deref()
                    == Some(object.path.as_str())
        }) {
            parent_write_count += 1;
            let (Some(parent_object_id), Some(parent_version)) = (
                write.expected_parent_object_id.as_ref(),
                write.expected_parent_version.as_ref(),
            ) else {
                return Err(reject(format!(
                    "local-eventual batch write proof for parent {} has no exact parent generation",
                    object.path
                )));
            };
            if parent_object_id != &object.object_id {
                return Err(reject(format!(
                    "local-eventual batch write proof for parent {} has no exact parent generation",
                    object.path
                )));
            }
            match expected_parent_version {
                None => expected_parent_version = Some(parent_version),
                Some(expected) if expected == parent_version => {}
                Some(_) => {
                    return Err(reject(format!(
                        "local-eventual batch write proofs disagree on the parent generation for {}",
                        object.path
                    )));
                }
            }
        }
        if parent_write_count == 0 {
            return Err(reject(format!(
                "local-eventual batch directory touch {} is not the parent of a proven file",
                object.path
            )));
        }
        let expected_parent_version = expected_parent_version.expect("count proves a value");
        if &current.version != expected_parent_version {
            return Err(DfsError::StaleDirectoryVersion {
                path: object.path.clone(),
                expected_version: expected_parent_version.clone(),
                current_version: current.version,
            }
            .into());
        }
        let mut allowed = current.clone();
        allowed.version = object.version.clone();
        allowed.modified_at_unix_nanos = object.modified_at_unix_nanos;
        allowed.changed_at_unix_nanos = object.changed_at_unix_nanos;
        allowed.last_modified_by_event_id = object.last_modified_by_event_id.clone();
        if current.kind != DfsObjectKind::Directory
            || current.path != object.path
            || current.version == object.version
            || allowed != *object
        {
            return Err(reject(format!(
                "local-eventual batch contains an invalid directory touch for {}",
                object.path
            )));
        }
        let version = versions
            .get(&object.object_id)
            .ok_or_else(|| reject(format!("missing directory version for {}", object.path)))?;
        if !version.visible
            || version.version != object.version
            || version.content_hash.is_some()
            || version.size_bytes.is_some()
            || version.locations != object.current_locations
        {
            return Err(reject(format!(
                "local-eventual batch contains an invalid directory version for {}",
                object.path
            )));
        }
    }
    Ok(prepare_expectations)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn resolve_prepared_operation(
        &mut self,
        record: &crate::DfsPreparedOperationRecord,
        status: crate::DfsPreparedOperationStatus,
        mut effects: Vec<crate::DfsPreparedMetadataEffect>,
    ) -> Result<(), DfsError> {
        if record.status == status {
            return Ok(());
        }
        if record.status != crate::DfsPreparedOperationStatus::Prepared
            || !matches!(
                status,
                crate::DfsPreparedOperationStatus::Aborted
                    | crate::DfsPreparedOperationStatus::Superseded
            )
        {
            return Err(DfsError::Metadata {
                message: "only an actual hidden preparation can be resolved".into(),
            });
        }
        let epoch = if let Some(control) = self.control_plane.as_mut() {
            let shard = DfsControlPlaneStore::new(control.as_mut())
                .get_metadata_shard(&record.digest.file_system_id, &record.digest.shard_id.0)?
                .ok_or_else(|| DfsError::Metadata {
                    message: "preparation resolution has no current assignment".into(),
                })?;
            if shard.primary_node_id != self.node_id {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: shard.file_system_id,
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id,
                    epoch: shard.epoch,
                });
            }
            shard.epoch
        } else {
            0
        };
        let operation_id = format!("resolve:{}:{status:?}:{epoch}", record.digest.operation_id);
        if let Some(digest) = self
            .metadata_backend
            .as_ref()
            .expect("resolution backend")
            .prepared_operation_digest(&operation_id)?
        {
            self.commit_stored_prepared_operation(&digest)?;
            return Ok(());
        }
        effects.push(crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
            digest: record.digest.clone(),
            status,
        });
        self.publish_metadata_effects_on_shard(
            DfsPreparedOperationScope {
                file_system_id: record.digest.file_system_id.clone(),
                shard_id: record.digest.shard_id.clone(),
                owner_epoch: epoch,
                operation_id,
            },
            Vec::new(),
            Vec::new(),
            effects,
        )?;
        Ok(())
    }

    pub(crate) fn publish_metadata_effects_on_shard(
        &mut self,
        scope: DfsPreparedOperationScope,
        expected_objects: Vec<DfsNamespaceObjectVersionExpectation>,
        expected_paths: Vec<DfsNamespacePathOccupantExpectation>,
        effects: Vec<crate::DfsPreparedMetadataEffect>,
    ) -> Result<bool, DfsError> {
        let payload = DfsPreparedOperationPayload::for_metadata_effects(
            scope,
            expected_objects,
            expected_paths,
            effects,
            OffsetDateTime::now_utc(),
        )?;
        self.publish_metadata_effect_payload(payload)
    }

    pub(crate) fn publish_metadata_effect_payload(
        &mut self,
        payload: DfsPreparedOperationPayload,
    ) -> Result<bool, DfsError> {
        if self.recover_matching_metadata_receipt_prepare(&payload)? {
            return Ok(true);
        }
        let published = self.publish_prepared_operation(payload)?;
        self.refresh_recorded_prepared_rows(&published.payload)?;
        if published.published {
            self.record_message_committed_work(crate::DfsMessageCommittedWork {
                publish_metadata: true,
                ..Default::default()
            });
        }
        Ok(published.published)
    }

    /// Receipt producers can retry after their durable prepare but before its
    /// publication was acknowledged. Only the same receipt-only body/readset
    /// may resume here. Keep its original event, time and operation identity;
    /// another receipt, namespace effect or authority round is not equivalent.
    fn recover_matching_metadata_receipt_prepare(
        &mut self,
        proposed: &DfsPreparedOperationPayload,
    ) -> Result<bool, DfsError> {
        let [DfsPreparedMetadataEffect::PutReceipt { receipt: incoming }] =
            proposed.metadata_effects.as_slice()
        else {
            return Ok(false);
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "receipt publication requires its native metadata backend".into(),
            })?;
        let mut matching = None;
        for record in backend.prepared_operation_reservations(proposed)? {
            let original = record.payload.as_ref();
            let [DfsPreparedMetadataEffect::PutReceipt { receipt: stored }] =
                original.metadata_effects.as_slice()
            else {
                continue;
            };
            let mut same_receipt = incoming.clone();
            same_receipt.receipt.event_id = stored.receipt.event_id.clone();
            same_receipt.received_at = stored.received_at;
            if same_receipt != *stored {
                continue;
            }
            // These fields are minted by for_metadata_effects and the receipt
            // producer on each call. Every other field, including owner epoch,
            // object/path expectations and the complete effect set, must match.
            if proposed.transaction.transaction_id != format!("metadata:{}", proposed.operation_id)
                || original.transaction.transaction_id
                    != format!("metadata:{}", original.operation_id)
            {
                continue;
            }
            let mut same_body = proposed.clone();
            same_body.operation_id = original.operation_id.clone();
            same_body.transaction.transaction_id = original.transaction.transaction_id.clone();
            same_body.transaction.created_at = original.transaction.created_at;
            same_body.metadata_effects = original.metadata_effects.clone();
            if same_body != *original {
                continue;
            }
            if matching.replace(record).is_some() {
                return Err(DfsError::Metadata {
                    message: "multiple prepared publications claim the same exact receipt".into(),
                });
            }
        }
        let Some(record) = matching else {
            return Ok(false);
        };
        if !self.recover_one_prepared_operation(record)? {
            return Err(DfsError::Metadata {
                message: "original receipt prepare requires current native recovery authority"
                    .into(),
            });
        }
        Ok(true)
    }

    pub(crate) fn metadata_effect_scope_for_object(
        &mut self,
        object: &DfsObjectMetadata,
        operation_id: String,
    ) -> Result<DfsPreparedOperationScope, DfsError> {
        let (shard_id, owner_epoch) = match self.select_metadata_shard_for_object(object)? {
            Some(shard) if shard.primary_node_id == self.node_id => {
                (DfsMetadataShardId::new(shard.shard_id), shard.epoch)
            }
            Some(shard) => {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: object.file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id,
                    epoch: shard.epoch,
                });
            }
            None if self.control_plane.is_none() => (self.local_prepared_operation_shard()?, 0),
            None => {
                return Err(DfsError::Metadata {
                    message: "metadata effect has no current shard assignment".into(),
                });
            }
        };
        Ok(DfsPreparedOperationScope {
            file_system_id: object.file_system_id.clone(),
            shard_id,
            owner_epoch,
            operation_id,
        })
    }

    pub(crate) fn publish_object_metadata_effects(
        &mut self,
        object: &DfsObjectMetadata,
        effects: Vec<crate::DfsPreparedMetadataEffect>,
    ) -> Result<bool, DfsError> {
        let scope = self.metadata_effect_scope_for_object(
            object,
            format!("metadata:{}", sinomo_core::Id::generate()),
        )?;
        self.publish_captured_object_metadata_effects(object, scope, effects)
    }

    pub(crate) fn publish_captured_object_metadata_effects(
        &mut self,
        object: &DfsObjectMetadata,
        scope: DfsPreparedOperationScope,
        effects: Vec<crate::DfsPreparedMetadataEffect>,
    ) -> Result<bool, DfsError> {
        if scope.file_system_id != object.file_system_id {
            return Err(DfsError::Metadata {
                message: "metadata effect scope differs from its object".into(),
            });
        }
        let _execution = self.acquire_prepared_execution(&scope)?;
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata effect requires a current durable backend".into(),
            })?;
        let current = backend.get_object_on_shard(&scope.shard_id, &object.object_id)?;
        let occupant = backend.get_object_by_path_on_shard(
            &scope.shard_id,
            &object.file_system_id,
            &object.path,
        )?;
        self.publish_metadata_effects_on_shard(
            scope,
            vec![DfsNamespaceObjectVersionExpectation {
                object_id: object.object_id.clone(),
                expected_version: current.map(|row| row.version),
            }],
            vec![DfsNamespacePathOccupantExpectation {
                path: object.path.clone(),
                expected_object_id: occupant.map(|row| row.object_id),
            }],
            effects,
        )
    }

    fn publish_namespace_prepared_operation(
        &mut self,
        payload: DfsPreparedOperationPayload,
    ) -> Result<crate::publication_guard::DfsPreparedPublication, NamespaceTransactionApplyError>
    {
        let _execution = self.acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: payload.file_system_id.clone(),
            shard_id: payload.shard_id.clone(),
            owner_epoch: payload.owner_epoch,
            operation_id: payload.operation_id.clone(),
        })?;
        let paths = payload
            .transaction
            .objects
            .iter()
            .map(|object| (object.object_id.clone(), object.path.clone()))
            .collect::<BTreeMap<_, _>>();
        let digest =
            self.prepare_payload_for_publication(payload)
                .map_err(|error| match error {
                    MetadataBackendError::Conflict {
                        object_id,
                        expected,
                        current,
                    } => NamespaceTransactionApplyError::PrepareConflict {
                        path: paths
                            .get(&object_id)
                            .cloned()
                            .unwrap_or_else(|| format!("object:{object_id}")),
                        object_id,
                        expected,
                        current,
                    },
                    error => NamespaceTransactionApplyError::Dfs(DfsError::from(error)),
                })?;
        self.commit_stored_prepared_operation(&digest)
            .map_err(NamespaceTransactionApplyError::from)
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) -> Result<(), DfsError> {
        let mut scopes = Vec::new();
        if let Some(kv) = self.control_plane.as_mut() {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            let config = DfsFileSystemConfig::from(&file_system);
            if store
                .get_file_system_config(&file_system.file_system_id)?
                .as_ref()
                .is_none_or(|current| {
                    !crate::placement::dfs_file_system_config_semantically_eq(current, &config)
                })
            {
                store.put_file_system_config(&config)?;
            }
            for shard in store.list_metadata_shards(&file_system.file_system_id)? {
                if shard.primary_node_id == self.node_id {
                    scopes.push(DfsPreparedOperationScope {
                        file_system_id: file_system.file_system_id.clone(),
                        shard_id: DfsMetadataShardId::new(shard.shard_id),
                        owner_epoch: shard.epoch,
                        operation_id: format!("filesystem:{}", sinomo_core::Id::generate()),
                    });
                }
            }
        } else {
            scopes.push(DfsPreparedOperationScope {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: self.local_prepared_operation_shard()?,
                owner_epoch: 0,
                operation_id: format!("filesystem:{}", sinomo_core::Id::generate()),
            });
        }
        // Configuration alone does not authorize a metadata effect. Every local
        // filesystem row is published through the same exact-shard capability.
        for scope in scopes {
            // A prior attempt can have the requested filesystem body durably
            // prepared even though its ALL-assigned probe failed. Reconcile
            // that exact original body before equality can skip publication or
            // a new operation could leave the old preparation unresolved.
            let recorded = self
                .metadata_backend
                .as_ref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "filesystem publication requires a current backend".into(),
                })?
                .list_prepared_operations()?;
            if let Some(record) =
                original_filesystem_preparation_for_request(recorded, &scope, &file_system)?
            {
                let recovered = self.commit_stored_prepared_operation(&record.digest)?;
                self.refresh_recorded_prepared_rows(&recovered.payload)?;
                if recovered.published {
                    self.record_message_committed_work(crate::DfsMessageCommittedWork {
                        publish_metadata: true,
                        ..Default::default()
                    });
                }
            }
            let existing = self
                .metadata_backend
                .as_ref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "filesystem publication requires a current backend".into(),
                })?
                .get_file_system(&file_system.file_system_id)?;
            if existing.as_ref() != Some(&file_system) {
                self.publish_metadata_effects_on_shard(
                    scope,
                    Vec::new(),
                    Vec::new(),
                    vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
                        file_system: file_system.clone(),
                    }],
                )?;
            }
        }
        self.metadata.add_file_system(file_system);
        self.clear_metadata_lookup_cache();
        Ok(())
    }

    pub fn add_object_metadata(&mut self, mut object: DfsObjectMetadata) -> Result<(), DfsError> {
        normalize_current_locations(&mut object.current_locations);
        self.put_visible_object_in_metadata_backend(&object)?;
        Ok(())
    }

    pub fn add_object_metadata_batch(
        &mut self,
        objects: impl IntoIterator<Item = DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        for object in objects {
            self.add_object_metadata(object)?;
        }
        Ok(())
    }

    pub fn add_transient_object_metadata(
        &mut self,
        mut object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.clear_metadata_lookup_cache();
        normalize_current_locations(&mut object.current_locations);
        let object_id = object.object_id.clone();
        let predecessor_object_id = match self.metadata_backend.as_ref() {
            Some(backend) => backend
                .get_object_by_path(&object.file_system_id, &object.path)
                .ok()
                .flatten(),
            None => self
                .tombstones_by_metadata_path
                .get(&(object.file_system_id.clone(), metadata_path(&object.path)))
                .cloned(),
        }
        .filter(|predecessor| {
            predecessor.kind == DfsObjectKind::Tombstone
                && predecessor.object_id != object.object_id
        })
        .map(|predecessor| predecessor.object_id);
        let backend_object = self
            .metadata_backend
            .as_ref()
            // This lookup only classifies pagination overlays. A backend read
            // failure must not turn an already committed write into a
            // caller-visible error; treating it as absent conservatively keeps
            // the in-memory namespace object visible until a later durable
            // observation clears the transient marker.
            .and_then(|backend| backend.get_object(&object_id).ok().flatten());
        let changes_namespace_view = backend_object.as_ref().is_none_or(|backend_object| {
            backend_object.file_system_id != object.file_system_id
                || backend_object.path != object.path
                || backend_object.kind != object.kind
        });
        self.track_metadata_tombstone(&object);
        let result = self.metadata.add_object(object);
        if result.is_ok() {
            if changes_namespace_view {
                let transient_object = self
                    .metadata
                    .object(&object_id)
                    .expect("successfully added transient metadata object")
                    .clone();
                self.transient_metadata_object_ids
                    .insert_object(&transient_object);
                if let Some(predecessor_object_id) = predecessor_object_id {
                    self.transient_metadata_predecessor_object_ids
                        .insert(object_id, predecessor_object_id);
                } else {
                    self.transient_metadata_predecessor_object_ids
                        .remove(&object_id);
                }
            } else {
                self.transient_metadata_object_ids.remove(&object_id);
                self.transient_metadata_predecessor_object_ids
                    .remove(&object_id);
            }
        }
        self.clear_metadata_lookup_cache();
        result
    }

    pub(crate) fn track_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        let key = (object.file_system_id.clone(), metadata_path(&object.path));
        if object.kind == DfsObjectKind::Tombstone {
            let replace = self
                .tombstones_by_metadata_path
                .get(&key)
                .is_none_or(|existing| existing.version < object.version);
            if replace {
                self.tombstones_by_metadata_path.insert(key, object.clone());
            }
            return;
        }
        self.tombstones_by_metadata_path.remove(&key);
    }

    pub(crate) fn track_hydrated_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        if object.kind != DfsObjectKind::Tombstone {
            let key = (object.file_system_id.clone(), metadata_path(&object.path));
            if self
                .tombstones_by_metadata_path
                .get(&key)
                .is_some_and(|existing| existing.version > object.version)
            {
                return;
            }
            self.track_metadata_tombstone(object);
            return;
        }
        let live_path = path_index_key(&object.file_system_id, &object.path).map(|(_, path)| path);
        let visible_object = self.metadata.object_by_normalized_posix_path(
            &object.file_system_id,
            live_path.as_deref().unwrap_or(&object.path),
        );
        if visible_object.is_none_or(|visible| visible.version < object.version) {
            self.track_metadata_tombstone(object);
        }
    }

    /// Resolve one logical namespace path without treating the service's
    /// merged tombstone map as durable authority. Cross-shard migration keeps
    /// a source eviction tombstone for history, and replicas may observe that
    /// row after the path-selected live destination. With a backend present,
    /// its routed path lookup wins unless an explicit in-flight local write
    /// owns the memory overlay.
    pub(crate) fn authoritative_metadata_object_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        normalized_posix_path: &str,
    ) -> crate::metadata_error::Result<Option<DfsObjectMetadata>> {
        let object = self.authoritative_metadata_object_for_path_unobserved(
            file_system_id,
            normalized_posix_path,
        )?;
        if object.as_ref().is_none_or(|object| {
            !self
                .transient_metadata_object_ids
                .contains(&object.object_id)
        }) {
            self.namespace_execution.observe_path(
                file_system_id,
                normalized_posix_path,
                object.as_ref(),
            );
        }
        Ok(object)
    }

    fn authoritative_metadata_object_for_path_unobserved(
        &self,
        file_system_id: &DfsFileSystemId,
        normalized_posix_path: &str,
    ) -> crate::metadata_error::Result<Option<DfsObjectMetadata>> {
        let metadata_key = metadata_path(normalized_posix_path);
        let live_memory_object = self
            .metadata
            .object_by_normalized_posix_path(file_system_id, normalized_posix_path);
        let tombstone_memory_object = self
            .tombstones_by_metadata_path
            .get(&(file_system_id.clone(), metadata_key.clone()));
        let memory_object = match (live_memory_object, tombstone_memory_object) {
            (Some(live), Some(_))
                if self.transient_metadata_object_ids.contains(&live.object_id) =>
            {
                Some(live)
            }
            (Some(_), Some(tombstone))
                if self
                    .transient_metadata_object_ids
                    .contains(&tombstone.object_id) =>
            {
                Some(tombstone)
            }
            (Some(live), Some(tombstone)) if tombstone.version > live.version => Some(tombstone),
            (Some(live), Some(_)) => Some(live),
            (Some(live), None) => Some(live),
            (None, Some(tombstone)) => Some(tombstone),
            (None, None) => None,
        };
        if let Some(memory) = memory_object
            && self
                .transient_metadata_object_ids
                .contains(&memory.object_id)
        {
            return Ok(Some(memory.clone()));
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(memory_object.cloned());
        };
        let backend_object = backend.get_object_by_path(file_system_id, &metadata_key)?;
        Ok(match (memory_object, backend_object) {
            (Some(memory), Some(backend))
                if self
                    .transient_metadata_object_ids
                    .contains(&memory.object_id)
                    || self.local_write_view_overrides_backend(
                        &memory.object_id,
                        memory,
                        Some(&backend),
                    ) =>
            {
                Some(memory.clone())
            }
            (_, Some(backend)) => Some(backend),
            (Some(memory), None) => Some(memory.clone()),
            (None, None) => None,
        })
    }

    pub fn remove_transient_object_metadata(&mut self, object_id: &DfsObjectId) {
        self.clear_metadata_lookup_cache();
        self.transient_metadata_object_ids.remove(object_id);
        self.transient_metadata_predecessor_object_ids
            .remove(object_id);
        self.metadata.remove_object(object_id);
        self.clear_metadata_lookup_cache();
    }

    // Shared metadata-backend read helpers for service paths that need durable
    // receipts or intents while preserving the service's no-backend fallback.
    pub(crate) fn backend_receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Vec::new());
        };
        let receipts = collect_metadata_pages(|cursor| {
            let page = backend.receipts_for_version_page(
                object_id,
                version,
                cursor,
                DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })?;
        Ok(receipts)
    }

    pub(crate) fn backend_intent_ids_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<String>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Vec::new());
        };
        let intents = collect_metadata_pages(|cursor| {
            let page = backend.intents_for_object_version_page(
                object_id,
                version,
                cursor,
                DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
            )?;
            Ok((page.intents, page.next_cursor))
        })?;
        Ok(intents.into_iter().map(|intent| intent.intent_id).collect())
    }

    pub(crate) fn backend_receipt_for_replica(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(receipt_for_replica_from_backend(
            backend.as_ref(),
            object_id,
            version,
            replica_node_id,
            DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
        )?)
    }

    /// `rebuild_repair_jobs` selects between the eager per-receipt repair
    /// rebuild and the deferred one for bulk restore loops: hydration restores
    /// every receipt of an unsettled-write backlog, and rebuilding per receipt
    /// made SIFS opens burn seconds of CPU once a degraded cluster link had
    /// let that backlog accumulate.
    pub(crate) fn restore_backend_durable_receipt_with_repair_rebuild(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
        allow_unknown_intent: bool,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        // Raw object rows describe publication; ordinary reads merge receipt
        // locations and would conceal a durable receipt's unfinished journal
        // effect. Preserve all receipt durability while retaining that target
        // in the existing repair mechanism until the normal handler settles it.
        let unsettled_location = self.coordinator.intent(intent_id).and_then(|intent| {
            if !matches!(
                intent.state,
                crate::distributed::DfsWriteIntentState::Committed
            ) || !intent.publication.is_current()
            {
                return Some(false);
            }
            self.metadata.object(&intent.object_id).map(|object| {
                object.file_system_id == intent.file_system_id
                    && object.version == intent.target_version
                    && object.content_hash.is_some()
                    && !object.current_locations.contains(&replica_node_id)
            })
        });
        if let Some(unsettled) = unsettled_location {
            self.coordinator.restore_receipt_location_settlement(
                intent_id,
                &replica_node_id,
                unsettled,
            );
        }
        let restored = if rebuild_repair_jobs {
            self.coordinator
                .restore_durable_receipt(intent_id, replica_node_id)
        } else {
            self.coordinator
                .restore_durable_receipt_deferring_repair_rebuild(intent_id, replica_node_id)
                .map(drop)
        };
        match restored {
            Ok(()) => Ok(()),
            Err(DfsDistributedError::UnknownIntent { .. }) if allow_unknown_intent => Ok(()),
            Err(
                DfsDistributedError::UnexpectedReplica { .. }
                | DfsDistributedError::IntentClosed { .. },
            ) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn restore_backend_receipt_pages_for_intent(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        for receipt in self
            .backend_receipts_for_version(&intent_record.object_id, &intent_record.target_version)?
        {
            if let Some(expected_hash) = intent_record.content_hash.as_deref()
                && receipt.content_hash.as_deref() != Some(expected_hash)
            {
                continue;
            }
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id,
                false,
                rebuild_repair_jobs,
            )?;
        }
        Ok(())
    }

    pub fn apply_namespace_transaction(
        &mut self,
        transaction: DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        self.apply_namespace_transaction_with_selected_shard(
            transaction,
            None,
            Vec::new(),
            None,
            None,
            Vec::new(),
        )
        .map_err(NamespaceTransactionApplyError::into_dfs_error)
    }

    pub(crate) fn apply_namespace_transaction_with_metadata_effects(
        &mut self,
        transaction: DfsNamespaceTransaction,
        effects: Vec<DfsPreparedMetadataEffect>,
    ) -> Result<(), DfsError> {
        self.apply_namespace_transaction_with_selected_shard(
            transaction,
            None,
            Vec::new(),
            None,
            None,
            effects,
        )
        .map_err(NamespaceTransactionApplyError::into_dfs_error)
    }

    pub(crate) fn apply_atomic_metadata_transaction(
        &mut self,
        transaction: DfsNamespaceTransaction,
        selected_shard: Option<DfsMetadataShardConfig>,
        heads: &[crate::DfsContributionHeadExpectation],
    ) -> Result<(), DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "contributions require durable metadata".into(),
            })?;
        let mut expected_path_occupants = BTreeMap::new();
        for head in heads {
            let occupant = match &selected_shard {
                Some(shard) => backend.get_object_by_path_on_shard(
                    &DfsMetadataShardId::new(&shard.shard_id),
                    &transaction.file_system_id,
                    &head.path,
                )?,
                None => backend.get_object_by_path(&transaction.file_system_id, &head.path)?,
            };
            // A retained object's tombstone can name a path now occupied by a
            // different object. Capture the actual path witness; identity and
            // ownership remain bound to the supplied source head.
            expected_path_occupants
                .insert(head.path.clone(), occupant.map(|object| object.object_id));
        }
        let expectations = NamespacePrepareExpectationBundle {
            // Local execution uses only the supplied head/path read set. This
            // label is never published as a metadata authority.
            shard_id: selected_shard.as_ref().map_or_else(
                || DfsMetadataShardId::new("local-contribution"),
                |shard| DfsMetadataShardId::new(&shard.shard_id),
            ),
            expected_object_versions: heads
                .iter()
                .map(|head| (head.object_id.clone(), Some(head.version.clone())))
                .collect(),
            expected_path_occupants,
        };
        self.apply_namespace_transaction_with_selected_shard(
            transaction,
            selected_shard,
            Vec::new(),
            Some(expectations),
            None,
            Vec::new(),
        )
        .map_err(NamespaceTransactionApplyError::into_dfs_error)
    }

    /// Capture comparisons from the original producer before its durable
    /// job yields. This is conditional DATA; publication still needs every
    /// normal current-owner, writer, content and assigned participant guard.
    pub(crate) fn capture_namespace_transaction_read_set(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<NamespacePrepareExpectationBundle, DfsError> {
        let original =
            self.namespace_execution
                .captured_reads()
                .ok_or_else(|| DfsError::Metadata {
                    message: "durable namespace frame has no original planning scope".into(),
                })?;
        let shard_id = if self.control_plane.is_some() {
            let paths = namespace_transaction_shard_paths(
                self.metadata_backend
                    .as_ref()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "durable namespace capture has no metadata backend".into(),
                    })?
                    .as_ref(),
                transaction,
            )?;
            let shards = self.namespace_shards_for_exact_paths(
                &transaction.file_system_id,
                paths.iter().map(String::as_str),
            )?;
            let [shard] = shards.as_slice() else {
                return Err(DfsError::Metadata {
                    message: "durable alias transaction has no single original physical shard"
                        .into(),
                });
            };
            if shard.primary_node_id != self.node_id {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: transaction.file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id.clone(),
                    epoch: shard.epoch,
                });
            }
            DfsMetadataShardId::new(&shard.shard_id)
        } else {
            self.local_prepared_operation_shard()?
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "durable namespace capture has no metadata backend".into(),
            })?;
        let (mut objects, mut paths, _) =
            namespace_prepare_expectations(backend.as_ref(), &shard_id, transaction)?;
        original.bind_recorded_expectations(
            &transaction.file_system_id,
            &mut objects,
            &mut paths,
        )?;
        Ok(NamespacePrepareExpectationBundle {
            shard_id,
            expected_object_versions: objects
                .into_iter()
                .map(|e| (e.object_id, e.expected_version))
                .collect(),
            expected_path_occupants: paths
                .into_iter()
                .map(|e| {
                    (
                        crate::namespace::metadata_path(&e.path),
                        e.expected_object_id,
                    )
                })
                .collect(),
        })
    }

    pub(crate) fn apply_namespace_transaction_with_recorded_read_set(
        &mut self,
        transaction: DfsNamespaceTransaction,
        operation_id: &str,
        shard_id: DfsMetadataShardId,
        objects: Vec<DfsNamespaceObjectVersionExpectation>,
        paths: Vec<DfsNamespacePathOccupantExpectation>,
    ) -> Result<(), posix_model::DfsPosixError> {
        let object_count = objects.len();
        let path_count = paths.len();
        let expected = NamespacePrepareExpectationBundle {
            shard_id,
            expected_object_versions: objects
                .into_iter()
                .map(|e| (e.object_id, e.expected_version))
                .collect(),
            expected_path_occupants: paths
                .into_iter()
                .map(|e| {
                    (
                        crate::namespace::metadata_path(&e.path),
                        e.expected_object_id,
                    )
                })
                .collect(),
        };
        let mut required_objects = transaction
            .objects
            .iter()
            .map(|o| o.object_id.clone())
            .chain(transaction.versions.iter().map(|v| v.object_id.clone()))
            .chain(
                transaction
                    .content_reference_identity
                    .iter()
                    .map(|i| i.object_id.clone()),
            )
            .chain(
                transaction
                    .advisory_lock_states
                    .iter()
                    .map(|a| a.object_id.clone()),
            )
            .collect::<BTreeSet<_>>();
        required_objects.extend(expected.expected_path_occupants.values().flatten().cloned());
        if object_count != expected.expected_object_versions.len()
            || path_count != expected.expected_path_occupants.len()
            || required_objects
                .iter()
                .any(|id| !expected.expected_object_versions.contains_key(id))
            || transaction.objects.iter().any(|object| {
                !expected
                    .expected_path_occupants
                    .contains_key(&crate::namespace::metadata_path(&object.path))
            })
        {
            return Err(posix_model::DfsPosixError::Dfs(DfsError::Metadata {
                message: "durable alias frame has an incomplete or ambiguous original read set"
                    .into(),
            }));
        }
        self.apply_namespace_transaction_with_selected_shard(
            transaction,
            None,
            Vec::new(),
            Some(expected),
            Some(operation_id.to_string()),
            Vec::new(),
        )
        .map_err(NamespaceTransactionApplyError::into_posix_error)
    }

    pub(crate) fn apply_namespace_transaction_with_journal_operation_id(
        &mut self,
        transaction: DfsNamespaceTransaction,
        operation_id: &str,
    ) -> Result<(), DfsError> {
        self.apply_namespace_transaction_with_selected_shard(
            transaction,
            None,
            Vec::new(),
            None,
            Some(operation_id.to_string()),
            Vec::new(),
        )
        .map_err(NamespaceTransactionApplyError::into_dfs_error)
    }

    fn validate_local_eventual_batch_commit(
        &self,
        requester_node_id: &NodeId,
        transaction: &DfsNamespaceTransaction,
        writes: &[DfsLocalEventualBatchWriteProof],
        selected_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<Option<NamespacePrepareExpectationBundle>, posix_model::DfsPosixError> {
        validate_local_eventual_batch_commit_from_view(
            LocalEventualBatchReadView {
                node_id: &self.node_id,
                metadata: &self.metadata,
                metadata_backend: self.metadata_backend.as_deref(),
                namespace_execution: &self.namespace_execution,
            },
            requester_node_id,
            transaction,
            writes,
            selected_shard,
        )
    }

    fn local_eventual_batch_committed_replay(
        &self,
        requester_node_id: &NodeId,
        transaction: &DfsNamespaceTransaction,
        writes: &[DfsLocalEventualBatchWriteProof],
        selected_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<Option<std::sync::Arc<DfsPreparedOperationPayload>>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        if backend
            .get_namespace_transaction_receipt(&transaction.transaction_id)?
            .is_none()
        {
            return Ok(None);
        }
        // Match the same identities used by namespace publication. Routed
        // completion uses its original request identity; direct batches use
        // the transaction identity. Pending-only listings are not commit proof.
        let mut operation_ids = Vec::new();
        if let Some(request) = self.namespace_execution.request() {
            operation_ids.push(format!(
                "namespace-request:{}:{}",
                request.operation_id, transaction.transaction_id,
            ));
        }
        operation_ids.push(format!("namespace:{}", transaction.transaction_id));
        let mut retained = None;
        for operation_id in operation_ids {
            if let Some(record) = backend.prepared_operation_record(&operation_id)? {
                record.validate()?;
                if record.status == crate::DfsPreparedOperationStatus::Committed {
                    if retained.replace(record).is_some() {
                        return Err(DfsError::Metadata {
                            message:
                                "local-eventual batch has competing recorded operation identities"
                                    .into(),
                        });
                    }
                }
            }
        }
        let prepare = retained.ok_or_else(|| DfsError::Metadata {
            message: "local-eventual batch retained its receipt but lost its complete recorded body; reconcile instead of replaying".into(),
        })?;
        let payload = &prepare.payload;
        let mut replay_transaction = transaction.clone();
        crate::metadata_records::normalize_namespace_transaction_in_place(&mut replay_transaction);
        complete_recorded_namespace_carried_versions(&mut replay_transaction, &payload.transaction);
        let mut proven_objects = BTreeSet::new();
        let proofs_match = writes.iter().all(|write| {
            let object_id = &write.receipt.receipt.object_id;
            if !proven_objects.insert(object_id)
                || write.receipt.replica_node_id != *requester_node_id
                || !payload.content_receipts.iter().any(|existing| {
                    crate::metadata_receipt::replay_receipt_matches_existing(
                        existing,
                        &write.receipt,
                    )
                })
            {
                return false;
            }
            let Some(object) = payload
                .transaction
                .objects
                .iter()
                .find(|object| &object.object_id == object_id)
            else {
                return false;
            };
            if object.owner_node_id != *requester_node_id {
                return false;
            }
            match write.expected_base_version.as_ref() {
                Some(version) => {
                    payload.expected_object_versions.iter().any(|row| {
                        row.object_id == *object_id
                            && row.expected_version.as_ref() == Some(version)
                    }) && write.expected_parent_object_id.is_none()
                        && write.expected_parent_version.is_none()
                }
                None => {
                    // Creates seal the exact parent generation, including
                    // recreation over a tombstone. Never compare to today's head.
                    let parent_path = crate::posix_path::parent_posix_path(&object.path);
                    let parent = payload.transaction.objects.iter().find(|parent| {
                        parent.kind == DfsObjectKind::Directory
                            && parent_path.as_deref() == Some(parent.path.as_str())
                    });
                    parent.is_some_and(|parent| {
                        write.expected_parent_object_id.as_ref() == Some(&parent.object_id)
                            && write.expected_parent_version.is_some()
                            && payload.expected_object_versions.iter().any(|row| {
                                row.object_id == parent.object_id
                                    && row.expected_version == write.expected_parent_version
                            })
                    })
                }
            }
        });
        if payload.file_system_id != transaction.file_system_id
            || payload.transaction != replay_transaction
            || selected_shard.is_some_and(|shard| {
                prepare.digest.shard_id.0 != shard.shard_id
                    || prepare.digest.file_system_id != shard.file_system_id
            })
            || payload.content_receipts.len() != writes.len()
            || !proofs_match
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "local-eventual batch {} does not match its exact recorded body, read set and receipts",
                    transaction.transaction_id,
                ),
            });
        }
        Ok(Some(prepare.payload))
    }

    fn materialize_namespace_transaction_in_memory(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .chain(
                transaction
                    .objects
                    .iter()
                    .filter(|object| object.kind != DfsObjectKind::Tombstone),
            )
            .cloned()
        {
            self.transient_metadata_object_ids.remove(&object.object_id);
            self.transient_metadata_predecessor_object_ids
                .remove(&object.object_id);
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            self.track_metadata_tombstone(&object);
            self.metadata.add_object(object)?;
        }
        self.clear_metadata_lookup_cache();
        Ok(())
    }

    pub(crate) fn apply_local_eventual_batch_commit(
        &mut self,
        requester_node_id: &NodeId,
        transaction: DfsNamespaceTransaction,
        writes: Vec<DfsLocalEventualBatchWriteProof>,
        selected_shard: Option<DfsMetadataShardConfig>,
    ) -> Result<bool, posix_model::DfsPosixError> {
        if let Some(payload) = self
            .local_eventual_batch_committed_replay(
                requester_node_id,
                &transaction,
                &writes,
                selected_shard.as_ref(),
            )
            .map_err(posix_model::DfsPosixError::from)?
        {
            self.refresh_recorded_prepared_rows(&payload)
                .map_err(posix_model::DfsPosixError::from)?;
            return Ok(false);
        }
        let prepare_expectations = self.validate_local_eventual_batch_commit(
            requester_node_id,
            &transaction,
            &writes,
            selected_shard.as_ref(),
        )?;
        let receipts = writes.into_iter().map(|write| write.receipt).collect();
        self.apply_namespace_transaction_with_selected_shard(
            transaction,
            selected_shard,
            receipts,
            prepare_expectations,
            None,
            Vec::new(),
        )
        .map_err(NamespaceTransactionApplyError::into_posix_error)?;
        Ok(true)
    }

    /// Apply a transaction using the immutable shard view already selected by
    /// the enclosing routed namespace request.
    ///
    /// The selected shard is request-scoped: callers must derive it from all
    /// affected transaction paths. Reusing it here avoids a second control
    /// plane prefix scan in the same compound operation without caching shard
    /// configuration across independent filesystem operations.
    fn apply_namespace_transaction_with_selected_shard(
        &mut self,
        mut transaction: DfsNamespaceTransaction,
        selected_shard: Option<DfsMetadataShardConfig>,
        content_receipts: Vec<DfsReplicaReceiptRecord>,
        prepare_expectations: Option<NamespacePrepareExpectationBundle>,
        journal_operation_id: Option<String>,
        metadata_effects: Vec<DfsPreparedMetadataEffect>,
    ) -> Result<(), NamespaceTransactionApplyError> {
        if metadata_effects
            .iter()
            .any(|effect| matches!(effect, DfsPreparedMetadataEffect::PutFileSystem { .. }))
        {
            return Err(DfsError::Metadata {
                message:
                    "namespace filesystem initialization belongs to its captured scoped prepare"
                        .into(),
            }
            .into());
        }
        if self.metadata_backend.is_none() {
            return Err(DfsError::Metadata {
                message:
                    "namespace publication requires the current durable prepared-operation backend"
                        .into(),
            }
            .into());
        }
        let replay_journal_operation_id = journal_operation_id.as_ref().map(|raw_id| {
            self.namespace_execution
                .request()
                .and_then(|active| {
                    self.durable_namespace_request_completion_for_transaction(
                        &transaction,
                        &active.authority_shard_id,
                    )
                    .map(|request| request.operation_id)
                    .or_else(|| {
                        self.compact_durable_namespace_request_completion_for_transaction(
                            &transaction,
                            &active.authority_shard_id,
                        )
                        .map(|completion| completion.operation_id)
                    })
                })
                .unwrap_or_else(|| raw_id.clone())
        });
        // Recorded outcomes precede replanning and durability preparation. A
        // newer head or parent must not turn an exact old receipt into a write.
        if !transaction.transaction_id.is_empty() && self.metadata_backend.is_some() {
            let mut operation_ids = Vec::new();
            if let Some(journal_id) = replay_journal_operation_id.as_ref() {
                operation_ids.push(crate::metadata_records::compound_prepared_operation_id(
                    journal_id,
                    &transaction.transaction_id,
                ));
            } else {
                if let Some(request) = self.namespace_execution.request() {
                    operation_ids.push(format!(
                        "namespace-request:{}:{}",
                        request.operation_id, transaction.transaction_id
                    ));
                }
                operation_ids.push(format!("namespace:{}", transaction.transaction_id));
            }
            for operation_id in operation_ids {
                let backend = self.metadata_backend.as_ref().expect("replay backend");
                let Some(digest) = backend.prepared_operation_digest(&operation_id)? else {
                    continue;
                };
                if backend.committed_prepared_operation(&digest)?.is_none() {
                    continue;
                }
                let payload = backend
                    .prepared_operation_payload(&operation_id)?
                    .ok_or_else(|| DfsError::Metadata {
                        message: "recorded namespace outcome lost its immutable body".into(),
                    })?;
                let mut replay_transaction = transaction.clone();
                crate::metadata_records::normalize_namespace_transaction_in_place(
                    &mut replay_transaction,
                );
                complete_recorded_namespace_carried_versions(
                    &mut replay_transaction,
                    &payload.transaction,
                );
                let recorded_read_set_matches =
                    prepare_expectations.as_ref().is_none_or(|expected| {
                        expected.shard_id == payload.shard_id
                            && payload
                                .expected_object_versions
                                .iter()
                                .map(|row| (row.object_id.clone(), row.expected_version.clone()))
                                .collect::<BTreeMap<_, _>>()
                                == expected.expected_object_versions
                            && payload
                                .expected_path_occupants
                                .iter()
                                .map(|row| {
                                    (
                                        crate::namespace::metadata_path(&row.path),
                                        row.expected_object_id.clone(),
                                    )
                                })
                                .collect::<BTreeMap<_, _>>()
                                == expected.expected_path_occupants
                    });
                if !recorded_read_set_matches
                    || payload.transaction != replay_transaction
                    || payload.journal_operation_id != replay_journal_operation_id
                    || !payload.metadata_effects.iter().filter(|effect| !matches!(effect,
                        DfsPreparedMetadataEffect::PutFileSystem { file_system } if file_system.file_system_id == payload.file_system_id))
                        .eq(metadata_effects.iter())
                    || payload.file_system_id != transaction.file_system_id
                {
                    return Err(DfsError::Metadata {
                        message: "namespace retry changes its recorded transaction".into(),
                    }
                    .into());
                }
                let replay = self.commit_stored_prepared_operation(&digest)?;
                debug_assert!(!replay.published);
                return Ok(());
            }
        }
        // Freeze the planner's observations before any configuration lookup,
        // durability establishment, or authority round can refresh storage.
        let planned_reads = (!transaction.objects.is_empty())
            .then(|| self.namespace_execution.captured_reads())
            .flatten();
        self.clear_metadata_lookup_cache();
        let trace_started = Instant::now();
        let trace_object_count = transaction.objects.len();
        let mut trace_expectations_ms = 0;

        let mut trace_memory_coordinator_ms = 0;
        let mut trace_memory_tombstone_ms = 0;
        let mut trace_memory_add_ms = 0;
        let configured_file_system =
            self.refresh_file_history_configuration(&transaction.file_system_id)?;
        if transaction.file_version_pins.is_some()
            || !transaction.extension_expectations.is_empty()
            || transaction.contribution_digest.is_some()
            || transaction
                .versions
                .iter()
                .any(|version| !version.retention.is_empty() || !version.module_metadata.is_empty())
            || configured_file_system
                .as_ref()
                .is_some_and(|fs| !fs.retention_policies.is_empty())
        {
            let paths = if let Some(backend) = self.metadata_backend.as_ref() {
                namespace_transaction_shard_paths(backend.as_ref(), &transaction)?
            } else {
                transaction
                    .objects
                    .iter()
                    .map(|object| object.path.clone())
                    .collect()
            };
            self.require_file_history_shards_support(
                &transaction.file_system_id,
                paths.iter().map(String::as_str),
            )?;
        }
        let mut history_repair_roots = self.file_history_repair_roots_before_objects(
            &transaction.objects,
            configured_file_system.as_ref(),
        )?;
        self.establish_rule_capture_durability(&history_repair_roots)?;
        if let Some(pins) = &transaction.file_version_pins {
            history_repair_roots.extend(
                pins.add
                    .iter()
                    .chain(&pins.remove)
                    .map(|pin| (pin.object_id.clone(), pin.version.clone())),
            );
        }
        complete_namespace_carried_versions(
            self.metadata_backend
                .as_ref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "namespace publication requires the current durable prepared-operation backend".into(),
                })?
                .as_ref(),
            &mut transaction,
        )?;
        let trace_version_count = transaction.versions.len();
        let backend_apply_started = Instant::now();
        if self.control_plane.is_some() {
            if self.metadata_backend.is_none() {
                return Err(DfsError::Metadata {
                    message: "clustered namespace mutation requires a durable metadata backend"
                        .to_string(),
                }
                .into());
            }
            let mut shard = selected_shard;
            if shard.is_none()
                && !transaction.objects.is_empty()
                && let Some(context) = self
                    .namespace_execution
                    .context()
                    .filter(|context| context.file_system_id == transaction.file_system_id)
            {
                let shards = Self::namespace_transaction_shards_from_snapshot(
                    &transaction.file_system_id,
                    &transaction,
                    &context.shards,
                )?;
                let [selected] = shards.as_slice() else {
                    let shard_ids = shards
                        .iter()
                        .map(|candidate| candidate.shard_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(DfsError::Distributed {
                        message: format!(
                            "clustered namespace transaction {} must resolve to exactly one metadata shard, found [{}]",
                            transaction.transaction_id, shard_ids
                        ),
                    }
                    .into());
                };
                shard = Some(selected.clone());
            }
            if shard.is_none() && !transaction.objects.is_empty() {
                let initial_paths = transaction
                    .objects
                    .iter()
                    .map(|object| object.path.as_str())
                    .collect::<Vec<_>>();
                let shards = self.namespace_shards_for_exact_paths(
                    &transaction.file_system_id,
                    initial_paths.iter().copied(),
                )?;
                let [selected] = shards.as_slice() else {
                    let shard_ids = shards
                        .iter()
                        .map(|candidate| candidate.shard_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(DfsError::Distributed {
                        message: format!(
                            "clustered namespace transaction {} must resolve to exactly one metadata shard, found [{}]",
                            transaction.transaction_id, shard_ids
                        ),
                    }
                    .into());
                };
                shard = Some(selected.clone());
            }
            if let Some(selected) = shard.as_ref() {
                if selected.primary_node_id != self.node_id {
                    return Err(DfsError::StaleOwnerEpoch {
                        file_system_id: transaction.file_system_id.clone(),
                        local_node_id: self.node_id.clone(),
                        primary_node_id: selected.primary_node_id.clone(),
                        epoch: selected.epoch,
                    }
                    .into());
                }
            }
            let paths = namespace_transaction_shard_paths(
                self.metadata_backend
                    .as_ref()
                    .expect("clustered metadata backend checked above")
                    .as_ref(),
                &transaction,
            )?;
            if paths.is_empty() {
                return Err(DfsError::Metadata {
                    message: format!(
                        "clustered namespace transaction {} has no shard-addressable path",
                        transaction.transaction_id
                    ),
                }
                .into());
            }
            let shard = if let Some(shard) = shard {
                shard
            } else {
                let path_refs = paths.iter().map(String::as_str).collect::<Vec<_>>();
                let shards = self.namespace_shards_for_exact_paths(
                    &transaction.file_system_id,
                    path_refs.iter().copied(),
                )?;
                let [shard] = shards.as_slice() else {
                    let shard_ids = shards
                        .iter()
                        .map(|shard| shard.shard_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(DfsError::Distributed {
                        message: format!(
                            "clustered namespace transaction {} must resolve to exactly one metadata shard, found [{}]",
                            transaction.transaction_id, shard_ids
                        ),
                    }
                    .into());
                };
                if shard.primary_node_id != self.node_id {
                    return Err(DfsError::StaleOwnerEpoch {
                        file_system_id: transaction.file_system_id.clone(),
                        local_node_id: self.node_id.clone(),
                        primary_node_id: shard.primary_node_id.clone(),
                        epoch: shard.epoch,
                    }
                    .into());
                }
                shard.clone()
            };
            let prepare_shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            let expectations_started = Instant::now();
            let (expected_object_versions, expected_path_occupants) = if let Some(expectations) =
                prepare_expectations
            {
                if expectations.shard_id != prepare_shard_id {
                    return Err(DfsError::Metadata {
                            message: format!(
                                "namespace transaction {} captured expectations for shard {}, not selected shard {}",
                                transaction.transaction_id,
                                expectations.shard_id.0,
                                prepare_shard_id.0,
                            ),
                        }
                        .into());
                }
                (
                    expectations
                        .expected_object_versions
                        .into_iter()
                        .map(
                            |(object_id, expected_version)| DfsNamespaceObjectVersionExpectation {
                                object_id,
                                expected_version,
                            },
                        )
                        .collect(),
                    expectations
                        .expected_path_occupants
                        .into_iter()
                        .map(
                            |(path, expected_object_id)| DfsNamespacePathOccupantExpectation {
                                path,
                                expected_object_id,
                            },
                        )
                        .collect(),
                )
            } else {
                let (mut expected_object_versions, mut expected_path_occupants, _) =
                    namespace_prepare_expectations(
                        self.metadata_backend
                            .as_ref()
                            .expect("clustered metadata backend checked above")
                            .as_ref(),
                        &prepare_shard_id,
                        &transaction,
                    )?;
                if let Some(reads) = planned_reads.as_ref() {
                    reads.bind_expectations(
                        &transaction.file_system_id,
                        &mut expected_object_versions,
                        &mut expected_path_occupants,
                    );
                } else if !transaction.objects.is_empty() {
                    let backend = self
                        .metadata_backend
                        .as_ref()
                        .expect("backend checked above");
                    for object in &transaction.objects {
                        if backend
                            .get_object(&object.object_id)?
                            .as_ref()
                            .is_some_and(|current| current != object)
                        {
                            return Err(DfsError::Metadata {
                                message: format!(
                                    "namespace replacement of {} requires its original read set",
                                    object.object_id
                                ),
                            }
                            .into());
                        }
                    }
                    for path in &mut expected_path_occupants {
                        if !transaction.objects.iter().any(|object| {
                            object.path == path.path
                                && path.expected_object_id.as_ref() == Some(&object.object_id)
                        }) {
                            path.expected_object_id = None;
                        }
                    }
                }
                (expected_object_versions, expected_path_occupants)
            };
            trace_expectations_ms = expectations_started.elapsed().as_millis();
            #[cfg(test)]
            run_namespace_prepare_after_expectations_test_hook(
                self.metadata_backend
                    .as_mut()
                    .expect("clustered metadata backend checked above")
                    .as_mut(),
            );
            let completed_request = self.durable_namespace_request_completion_for_transaction(
                &transaction,
                &prepare_shard_id,
            );
            let completed_request_completion = self
                .compact_durable_namespace_request_completion_for_transaction(
                    &transaction,
                    &prepare_shard_id,
                );
            if completed_request.is_none()
                && completed_request_completion.is_none()
                && let Some(active_request) = self.namespace_execution.request().cloned()
                && self
                    .metadata_backend
                    .as_ref()
                    .expect("clustered metadata backend checked above")
                    .get_namespace_request(
                        &active_request.requester_node_id,
                        &active_request.request_id,
                    )?
                    .is_none()
            {
                // A deferred admission may share the request-owned prepare
                // only when this exact transaction proves the operation's
                // terminal outcome. Preserve the original admission barrier
                // for symlink-resolved or otherwise non-proving transactions.
                self.publish_metadata_effects_on_shard(
                    DfsPreparedOperationScope {
                        file_system_id: transaction.file_system_id.clone(),
                        shard_id: prepare_shard_id.clone(),
                        owner_epoch: shard.epoch,
                        operation_id: format!("request-admission:{}", active_request.operation_id),
                    },
                    Vec::new(),
                    Vec::new(),
                    vec![crate::DfsPreparedMetadataEffect::PutNamespaceRequest {
                        request: Box::new(active_request),
                    }],
                )?;
            }
            let completed_operation_id = completed_request
                .as_ref()
                .map(|request| &request.operation_id)
                .or_else(|| {
                    completed_request_completion
                        .as_ref()
                        .map(|completion| &completion.operation_id)
                });
            let captured_journal_operation_id = journal_operation_id.as_ref().map(|raw_id| {
                completed_operation_id
                    .cloned()
                    .unwrap_or_else(|| raw_id.clone())
            });
            let operation_id =
                if let Some(journal_operation_id) = captured_journal_operation_id.as_ref() {
                    crate::metadata_records::compound_prepared_operation_id(
                        journal_operation_id,
                        &transaction.transaction_id,
                    )
                } else if let Some(completed_operation_id) = completed_operation_id {
                    format!(
                        "namespace-request:{}:{}",
                        completed_operation_id, transaction.transaction_id
                    )
                } else if transaction.transaction_id.is_empty() {
                    format!("namespace:{}", sinomo_core::Id::generate())
                } else {
                    format!("namespace:{}", transaction.transaction_id)
                };
            let mut payload = DfsPreparedOperationPayload::for_namespace_transaction(
                DfsPreparedOperationScope {
                    operation_id,
                    file_system_id: transaction.file_system_id.clone(),
                    shard_id: prepare_shard_id,
                    owner_epoch: shard.epoch,
                },
                expected_object_versions,
                expected_path_occupants,
                transaction,
                content_receipts,
                None,
                None,
                Vec::new(),
                completed_request,
                completed_request_completion,
            )?;
            payload.journal_operation_id = captured_journal_operation_id;
            payload.metadata_effects.extend(metadata_effects);
            let published = self.publish_namespace_prepared_operation(payload)?;
            if !published.published {
                return Ok(());
            }
            transaction = published.payload.transaction.clone();
        } else if self.metadata_backend.is_some() {
            let shard_id = self.local_prepared_operation_shard()?;
            let recorded_expectations = prepare_expectations.is_some();
            let (mut objects, mut paths, _) = if let Some(expectations) = prepare_expectations {
                if expectations.shard_id != shard_id {
                    return Err(DfsError::Metadata {
                        message: "recorded namespace comparisons name a different physical shard"
                            .into(),
                    }
                    .into());
                }
                (
                    expectations
                        .expected_object_versions
                        .into_iter()
                        .map(
                            |(object_id, expected_version)| DfsNamespaceObjectVersionExpectation {
                                object_id,
                                expected_version,
                            },
                        )
                        .collect(),
                    expectations
                        .expected_path_occupants
                        .into_iter()
                        .map(
                            |(path, expected_object_id)| DfsNamespacePathOccupantExpectation {
                                path,
                                expected_object_id,
                            },
                        )
                        .collect(),
                    BTreeSet::<String>::new(),
                )
            } else {
                crate::metadata_api::namespace_prepare_expectations_from_lookups(
                    &transaction,
                    |id| {
                        self.metadata_backend
                            .as_ref()
                            .expect("local backend")
                            .get_object(id)
                    },
                    |fs, path| {
                        self.metadata_backend
                            .as_ref()
                            .expect("local backend")
                            .get_object_by_path(fs, path)
                    },
                )?
            };
            if !recorded_expectations && let Some(reads) = planned_reads.as_ref() {
                reads.bind_expectations(&transaction.file_system_id, &mut objects, &mut paths);
            }
            #[cfg(test)]
            run_namespace_prepare_after_expectations_test_hook(
                self.metadata_backend
                    .as_mut()
                    .expect("local backend")
                    .as_mut(),
            );
            let operation_id = journal_operation_id
                .as_ref()
                .map(|id| {
                    crate::metadata_records::compound_prepared_operation_id(
                        id,
                        &transaction.transaction_id,
                    )
                })
                .unwrap_or_else(|| {
                    if transaction.transaction_id.is_empty() {
                        format!("namespace:{}", sinomo_core::Id::generate())
                    } else {
                        format!("namespace:{}", transaction.transaction_id)
                    }
                });
            let mut payload = DfsPreparedOperationPayload::for_namespace_transaction(
                DfsPreparedOperationScope {
                    operation_id,
                    file_system_id: transaction.file_system_id.clone(),
                    shard_id,
                    owner_epoch: 0,
                },
                objects,
                paths,
                transaction,
                content_receipts,
                None,
                None,
                Vec::new(),
                None,
                None,
            )?;
            payload.journal_operation_id = journal_operation_id.clone();
            payload.metadata_effects.extend(metadata_effects);
            let published = self.publish_namespace_prepared_operation(payload)?;
            if !published.published {
                return Ok(());
            }
            transaction = published.payload.transaction.clone();
        }
        if !transaction.objects.is_empty()
            || !transaction.versions.is_empty()
            || transaction.content_reference_identity.is_some()
            || transaction.contribution_digest.is_some()
            || transaction
                .file_version_pins
                .as_ref()
                .is_some_and(|pins| !pins.add.is_empty() || !pins.remove.is_empty())
        {
            self.record_message_committed_work(crate::DfsMessageCommittedWork {
                publish_metadata: true,
                semantic_changes: true,
                ..Default::default()
            });
        }
        let trace_backend_apply_ms = backend_apply_started.elapsed().as_millis();
        self.namespace_execution
            .publication_completed(&transaction.objects);
        let (tombstones, live_objects): (Vec<_>, Vec<_>) = transaction
            .objects
            .into_iter()
            .partition(|object| object.kind == DfsObjectKind::Tombstone);
        for object in tombstones.into_iter().chain(live_objects) {
            let coordinator_started = Instant::now();
            self.transient_metadata_object_ids.remove(&object.object_id);
            self.transient_metadata_predecessor_object_ids
                .remove(&object.object_id);
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            trace_memory_coordinator_ms += coordinator_started.elapsed().as_millis();
            let tombstone_started = Instant::now();
            self.track_metadata_tombstone(&object);
            trace_memory_tombstone_ms += tombstone_started.elapsed().as_millis();
            let add_started = Instant::now();
            self.metadata.add_object(object)?;
            trace_memory_add_ms += add_started.elapsed().as_millis();
        }
        self.activate_file_history_repair_roots(history_repair_roots)?;
        self.clear_metadata_lookup_cache();
        dfs_perf_trace(format_args!(
            "apply-namespace-transaction node={} objects={} versions={} expectations_ms={} backend_apply_ms={} memory_coordinator_ms={} memory_tombstone_ms={} memory_add_ms={} total_ms={}",
            self.node_id,
            trace_object_count,
            trace_version_count,
            trace_expectations_ms,
            trace_backend_apply_ms,
            trace_memory_coordinator_ms,
            trace_memory_tombstone_ms,
            trace_memory_add_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(())
    }
}

/// A filesystem retry may recover only one exact original current-era body.
/// Actual native commit/current provider/ALL-assigned guards remain separate.
fn original_filesystem_preparation_for_request(
    records: Vec<crate::DfsPreparedOperationRecord>,
    scope: &DfsPreparedOperationScope,
    requested: &DfsFileSystem,
) -> Result<Option<crate::DfsPreparedOperationRecord>, DfsError> {
    let mut active = records
        .into_iter()
        .filter(|record| {
            record.status == crate::DfsPreparedOperationStatus::Prepared
                && record.digest.file_system_id == scope.file_system_id
                && record.digest.shard_id == scope.shard_id
                && (record.digest.operation_id.starts_with("filesystem:")
                    || record.payload.metadata_effects.iter().any(|effect| {
                        matches!(
                            effect,
                            crate::DfsPreparedMetadataEffect::PutFileSystem { .. }
                        )
                    }))
        })
        .collect::<Vec<_>>();
    if active.len() > 1 {
        return Err(DfsError::Metadata {
            message: "filesystem publication has ambiguous active preparations; original native recovery is required".into(),
        });
    }
    let Some(record) = active.pop() else {
        return Ok(None);
    };
    record.validate()?;
    if record.digest.owner_epoch != scope.owner_epoch {
        return Err(DfsError::Metadata {
            message: "filesystem publication has a preparation from another epoch; original native recovery is required".into(),
        });
    }
    let payload = &record.payload;
    if !record
        .digest
        .operation_id
        .strip_prefix("filesystem:")
        .is_some_and(|id| !id.is_empty())
        || payload.transaction.transaction_id != format!("metadata:{}", record.digest.operation_id)
        || !payload.transaction.objects.is_empty()
        || !payload.transaction.versions.is_empty()
        || !payload.transaction.advisory_lock_states.is_empty()
        || payload.transaction.content_reference_identity.is_some()
        || !payload.transaction.extension_expectations.is_empty()
        || payload.transaction.contribution_digest.is_some()
        || payload.transaction.file_version_pins.is_some()
        || !payload.expected_object_versions.is_empty()
        || !payload.expected_path_occupants.is_empty()
        || !payload.content_receipts.is_empty()
        || !payload.required_replica_node_ids.is_empty()
        || payload.prepared_intent.is_some()
        || payload.terminal_intent.is_some()
        || payload.completed_request.is_some()
        || payload.completed_request_completion.is_some()
        || !matches!(payload.metadata_effects.as_slice(),
            [crate::DfsPreparedMetadataEffect::PutFileSystem { file_system }] if file_system == requested)
    {
        return Err(DfsError::Metadata {
            message: "filesystem publication conflicts with an active original body; original native recovery is required".into(),
        });
    }
    Ok(Some(record))
}

#[cfg(test)]
mod filesystem_preparation_recovery_tests {
    use super::*;
    use crate::DfsMetadataBackend;

    #[test]
    fn native_filesystem_retry_rejects_ambiguous_bodies_and_recovers_each_original() {
        let (mut native, mut requester, shard) =
            crate::test_support::current_native_metadata_service(
                "native-filesystem-original-preparation-ambiguity",
                3,
                "/",
            )
            .unwrap();
        assert_eq!(shard.replica_node_ids.len(), 2);
        let receiver = shard.replica_node_ids[0].clone();
        let initial = native.metadata_primary_service("/").unwrap();
        let original = initial
            .metadata_backend()
            .unwrap()
            .get_file_system_on_shard(
                &crate::DfsMetadataShardId::new(&shard.shard_id),
                &shard.file_system_id,
            )
            .unwrap()
            .unwrap();
        drop(initial);
        let mut requested = original.clone();
        requested.display_name = "actual independent filesystem requests".into();
        native.stop_node(&receiver).unwrap();
        let first = requester.add_file_system(requested.clone());
        assert!(matches!(
            first,
            Err(DfsError::MetadataRecoveryQuorumUnavailable {
                required: 3,
                verified: 2,
                ..
            })
        ));
        drop(requester);
        let mut source = native.metadata_primary_service("/").unwrap();
        let scope = DfsPreparedOperationScope {
            file_system_id: shard.file_system_id.clone(),
            shard_id: crate::DfsMetadataShardId::new(&shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: format!("filesystem:{}", sinomo_core::Id::generate()),
        };
        let before = source
            .metadata_backend()
            .unwrap()
            .list_prepared_operations()
            .unwrap();
        let selected = original_filesystem_preparation_for_request(before, &scope, &requested)
            .unwrap()
            .unwrap();
        let mut stale_scope = scope.clone();
        stale_scope.owner_epoch += 1;
        assert!(
            matches!(original_filesystem_preparation_for_request(vec![selected.clone()], &stale_scope, &requested),
            Err(DfsError::Metadata { message }) if message == "filesystem publication has a preparation from another epoch; original native recovery is required")
        );
        let mut other = requested.clone();
        other.file_system_id = sinomo_core::Id::generate();
        assert!(
            matches!(original_filesystem_preparation_for_request(vec![selected.clone()], &scope, &other),
            Err(DfsError::Metadata { message }) if message == "filesystem publication conflicts with an active original body; original native recovery is required")
        );
        let mut non_singleton = selected.clone();
        std::sync::Arc::make_mut(&mut non_singleton.payload)
            .metadata_effects
            .push(crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: requested.clone(),
            });
        assert!(
            original_filesystem_preparation_for_request(vec![non_singleton], &scope, &requested)
                .is_err()
        );
        let second = source.publish_metadata_effects_on_shard(
            scope.clone(),
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: requested.clone(),
            }],
        );
        assert!(matches!(
            second,
            Err(DfsError::MetadataRecoveryQuorumUnavailable {
                required: 3,
                verified: 2,
                ..
            })
        ));
        let originals = source
            .metadata_backend()
            .unwrap()
            .list_prepared_operations()
            .unwrap()
            .into_iter()
            .filter(|record| {
                record.status == crate::DfsPreparedOperationStatus::Prepared
                    && record.digest.file_system_id == scope.file_system_id
                    && record.digest.shard_id == scope.shard_id
            })
            .collect::<Vec<_>>();
        assert_eq!(originals.len(), 2);
        assert_ne!(originals[0].digest, originals[1].digest);
        let prefix = source
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&scope.shard_id)
            .unwrap();
        let ambiguous = source.add_file_system(requested.clone());
        assert!(matches!(ambiguous, Err(DfsError::Metadata { message })
            if message == "filesystem publication has ambiguous active preparations; original native recovery is required"));
        assert_eq!(
            source
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&scope.shard_id)
                .unwrap(),
            prefix
        );
        assert_eq!(
            source
                .metadata_backend()
                .unwrap()
                .get_file_system(&scope.file_system_id)
                .unwrap(),
            Some(original)
        );
        drop(source);
        native.reopen_node(receiver).unwrap();
        let mut source = native.metadata_primary_service("/").unwrap();
        source.recover_durable_namespace_prepares().unwrap();
        for original in originals {
            let actual = source
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&original.digest.operation_id)
                .unwrap()
                .unwrap();
            assert_eq!(actual.digest, original.digest);
            assert_eq!(actual.payload, original.payload);
            assert_eq!(actual.status, crate::DfsPreparedOperationStatus::Committed);
            assert!(actual.committed_entry.is_some());
        }
        assert_eq!(
            source
                .metadata_backend()
                .unwrap()
                .get_file_system(&scope.file_system_id)
                .unwrap(),
            Some(requested)
        );
        drop(source);
        native.close().unwrap();
    }
}
