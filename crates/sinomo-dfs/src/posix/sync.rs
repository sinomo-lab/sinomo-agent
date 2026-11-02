//! POSIX file and directory durability barriers.
//!
//! Public entry points capture an immutable target, route it to the current
//! metadata primary when necessary, and invoke the corresponding `*_local`
//! authority operation there.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::Result;
use crate::posix_model::posix_error_for_namespace_rejection;
use crate::{
    DfsError, DfsMessageTransport, DfsMetadataQuorumSyncCertificate, DfsNamespaceOperation,
    DfsNamespaceResult, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsPosixCoordinationResult,
    DfsPosixDirectorySyncTarget, DfsPosixError, DfsPosixFileSyncEvidence, DfsPosixFileSyncTarget,
    DfsPosixSyncKind, DfsWriteIntentStatus, NetworkedDfsService,
};

const POSIX_SYNC_IDLE_WAIT: Duration = Duration::from_millis(10);

const POSIX_SYNC_AUTHORITY_REDIRECT_LIMIT: usize = 3;
const DFS_POSIX_SYNC_RPC_TIMEOUT: Duration = Duration::from_secs(50);

/// Certificate returned after content and metadata satisfy a file barrier.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPosixFileSyncCertificate {
    pub target: DfsPosixFileSyncTarget,
    pub content_evidence: DfsPosixFileSyncEvidence,
    pub metadata_certificate: DfsMetadataQuorumSyncCertificate,
}

/// Certificate returned after a directory generation reaches metadata quorum.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPosixDirectorySyncCertificate {
    pub target: DfsPosixDirectorySyncTarget,
    pub metadata_certificate: DfsMetadataQuorumSyncCertificate,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Flush and certify the file prefix visible when this call starts.
    pub fn posix_sync_file(
        &mut self,
        path: &str,
        kind: DfsPosixSyncKind,
    ) -> Result<DfsPosixFileSyncCertificate> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let target = self.posix_capture_file_sync_target(path, kind)?;
        self.posix_sync_file_target(&target)
    }

    /// Certify one immutable object/version/hash captured by an open handle.
    /// The routing hint may be stale after rename or unlink; the stable object
    /// id determines the current metadata authority whenever it is still
    /// materialized.
    pub fn posix_sync_file_target(
        &mut self,
        target: &DfsPosixFileSyncTarget,
    ) -> Result<DfsPosixFileSyncCertificate> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let current_object = if self.metadata_backend.is_none()
            && self.transport.delegates_local_namespace_authority()
        {
            self.current_object_from_authority(&target.file_system_id, &target.object_id)?
        } else {
            self.current_read_object_metadata(&target.object_id).ok()
        };
        let authority_path = current_object
            .filter(|object| object.file_system_id == target.file_system_id)
            .map(|object| object.path)
            .unwrap_or_else(|| target.path.clone());
        let operation = DfsNamespaceOperation::FileSync {
            path: authority_path.clone(),
            target: target.clone(),
        };
        let Some(primary) = self.routed_namespace_primary_for_path(&authority_path)? else {
            return self.posix_sync_file_local(target, DFS_POSIX_SYNC_RPC_TIMEOUT);
        };
        match self.route_posix_sync_operation(primary, operation)? {
            DfsPosixCoordinationResult::FileSync(certificate) => Ok(certificate),
            _ => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "file-sync response carried the wrong typed result".to_string(),
            })),
        }
    }

    /// Certify the directory generation visible when this call starts.
    pub fn posix_sync_directory(&mut self, path: &str) -> Result<DfsPosixDirectorySyncCertificate> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let target = self.posix_capture_directory_sync_target(path)?;
        let operation = DfsNamespaceOperation::DirectorySync {
            path: target.path.clone(),
            target: target.clone(),
        };
        let Some(primary) = self.routed_namespace_primary_for_path(&target.path)? else {
            return self.posix_sync_directory_local(&target, DFS_POSIX_SYNC_RPC_TIMEOUT);
        };
        match self.route_posix_sync_operation(primary, operation)? {
            DfsPosixCoordinationResult::DirectorySync(certificate) => Ok(certificate),
            _ => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "directory-sync response carried the wrong typed result".to_string(),
            })),
        }
    }

    /// Capture the file prefix visible to the caller before `fsync` or
    /// `fdatasync` starts.
    pub fn posix_capture_file_sync_target(
        &mut self,
        path: &str,
        kind: DfsPosixSyncKind,
    ) -> Result<DfsPosixFileSyncTarget> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if !kind.is_file() {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{path}: directory sync kind used for a file"),
            });
        }
        let (path, object) = self.lookup_sifs_path_distributed_synced(path, true)?;
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let content_hash = object.content_hash.clone().ok_or_else(|| {
            sync_target_conflict(
                &path,
                format!(
                    "visible file {} version {} has no content hash",
                    object.object_id, object.version
                ),
            )
        })?;
        // A hardlink alias may be routed by a different namespace shard. The
        // stable target object owns durability, so route using its canonical
        // metadata path while preserving the object/version/hash identity.
        let authority_path = object.path.clone();
        Ok(DfsPosixFileSyncTarget {
            kind,
            file_system_id: object.file_system_id,
            object_id: object.object_id,
            path: authority_path,
            version: object.version,
            content_hash,
        })
    }

    /// Capture the current prefix of a stable opened object. Unlike a cached
    /// per-handle version, this includes writes that returned through another
    /// descriptor or mount before this sync call began.
    pub fn posix_capture_file_sync_target_for_object(
        &mut self,
        object_id: &DfsObjectId,
        kind: DfsPosixSyncKind,
    ) -> Result<DfsPosixFileSyncTarget> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if !kind.is_file() {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{object_id}: directory sync kind used for a file"),
            });
        }
        let object = if self.metadata_backend.is_none()
            && self.transport.delegates_local_namespace_authority()
        {
            self.current_object_from_authority(&super::sifs_file_system_id(), object_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: object_id.to_string(),
                })?
        } else {
            self.current_read_object_metadata(object_id)
                .map_err(DfsPosixError::from)?
        };
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path: object.path });
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: object.path });
        }
        let content_hash = object.content_hash.clone().ok_or_else(|| {
            sync_target_conflict(
                &object.path,
                format!(
                    "visible file {} version {} has no content hash",
                    object.object_id, object.version
                ),
            )
        })?;
        Ok(DfsPosixFileSyncTarget {
            kind,
            file_system_id: object.file_system_id,
            object_id: object.object_id,
            path: object.path,
            version: object.version,
            content_hash,
        })
    }

    /// Validate that a frozen file target has a durable committed intent and
    /// enough exact-version, exact-hash receipts for the intent's configured
    /// durability target. A normal write may become visible at the lower
    /// commit threshold, but `fsync`/`fdatasync` deliberately wait for every
    /// desired replica so the acknowledged bytes survive metadata failover.
    pub fn posix_validate_file_sync_target_local(
        &self,
        target: &DfsPosixFileSyncTarget,
    ) -> Result<DfsPosixFileSyncEvidence> {
        if !target.kind.is_file() {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{}: directory sync kind used for a file", target.path),
            });
        }
        let backend = self.metadata_backend.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: "POSIX file sync requires a durable metadata backend".to_string(),
            })
        })?;

        let version = backend
            .get_version(&target.object_id, &target.version)?
            .ok_or_else(|| {
                sync_target_conflict(
                    &target.path,
                    format!(
                        "frozen version {} for object {} is not durably recorded",
                        target.version, target.object_id
                    ),
                )
            })?;
        if version.file_system_id != target.file_system_id
            || version.object_id != target.object_id
            || version.content_hash.as_deref() != Some(target.content_hash.as_str())
            || !version.visible
        {
            return Err(sync_target_conflict(
                &target.path,
                format!(
                    "durable version record does not match frozen object/version/hash {} {} {}",
                    target.object_id, target.version, target.content_hash
                ),
            ));
        }

        let mut intents = backend
            .intents_for_object(&target.object_id)?
            .into_iter()
            .filter(|intent| {
                intent.file_system_id == target.file_system_id
                    && intent.object_id == target.object_id
                    && intent.target_version == target.version
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.content_hash.as_deref() == Some(target.content_hash.as_str())
                    && intent.object_metadata.as_ref().is_none_or(|object| {
                        object.file_system_id == target.file_system_id
                            && object.object_id == target.object_id
                            && object.version == target.version
                            && object.content_hash.as_deref() == Some(target.content_hash.as_str())
                    })
            })
            .collect::<Vec<_>>();
        intents.sort_by(|left, right| {
            right
                .policy
                .desired_replicas
                .cmp(&left.policy.desired_replicas)
                .then_with(|| {
                    right
                        .policy
                        .min_commit_replicas
                        .cmp(&left.policy.min_commit_replicas)
                })
                .then_with(|| left.intent_id.cmp(&right.intent_id))
        });
        let required_receipts = intents
            .first()
            .ok_or_else(|| {
                sync_target_conflict(
                    &target.path,
                    format!(
                        "no committed intent matches frozen object/version/hash {} {} {}",
                        target.object_id, target.version, target.content_hash
                    ),
                )
            })?
            .policy
            .desired_replicas
            .max(1);
        // Placement can durably move the exact frozen bytes to another named
        // replica set and compact the original write owner. Certify a complete
        // committed plan at the strongest replica threshold rather than
        // waiting forever for the retired owner's former replica set.
        let receipts = backend.receipts_for_version(&target.object_id, &target.version)?;
        let mut pending = None;
        for intent in intents {
            if intent.policy.desired_replicas.max(1) != required_receipts {
                continue;
            }
            let desired_nodes = intent
                .desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if !desired_nodes.is_empty() && desired_nodes.len() != intent.policy.desired_replicas {
                return Err(sync_target_conflict(
                    &target.path,
                    format!(
                        "committed intent {} names {} unique desired nodes but policy requires exactly {}",
                        intent.intent_id,
                        desired_nodes.len(),
                        intent.policy.desired_replicas
                    ),
                ));
            }

            let mut receipt_nodes = receipts
                .iter()
                .filter(|receipt| {
                    receipt.receipt.file_system_id == target.file_system_id
                        && receipt.receipt.object_id == target.object_id
                        && receipt.receipt.committed_version == target.version
                        && receipt.content_hash.as_deref() == Some(target.content_hash.as_str())
                        && (desired_nodes.is_empty()
                            || desired_nodes.contains(&receipt.replica_node_id))
                })
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<Vec<_>>();
            receipt_nodes.sort();
            receipt_nodes.dedup();
            let named_desired_nodes_complete = desired_nodes.is_empty()
                || receipt_nodes.iter().cloned().collect::<BTreeSet<_>>() == desired_nodes;
            if receipt_nodes.len() < required_receipts || !named_desired_nodes_complete {
                let archived = backend
                    .get_intent_receipt(&target.object_id, &intent.intent_id)?
                    .is_some();
                let error = DfsPosixError::PendingWrite {
                    intent_id: intent.intent_id,
                    durable_receipts: receipt_nodes.len(),
                    required_receipts,
                };
                if pending
                    .as_ref()
                    .is_none_or(|(_, previous_archived)| *previous_archived && !archived)
                {
                    pending = Some((error, archived));
                }
                continue;
            }
            return Ok(DfsPosixFileSyncEvidence {
                intent_id: intent.intent_id,
                required_receipts,
                receipt_node_ids: receipt_nodes,
            });
        }
        Err(pending.expect("a strongest committed plan was selected").0)
    }

    /// Certify a previously captured file prefix on the current metadata
    /// primary. `fdatasync` currently executes the same physical metadata
    /// barrier as `fsync`, which is stronger than its minimum contract.
    pub fn posix_sync_file_local(
        &mut self,
        target: &DfsPosixFileSyncTarget,
        timeout: Duration,
    ) -> Result<DfsPosixFileSyncCertificate> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let started_at = Instant::now();
        // This is only a scheduling cursor within the existing request budget.
        // Every visit reloads exact durable owners; it grants no publication
        // permission and cannot settle a previous carrier's unknown outcome.
        let mut repair_owner_after = None;
        let content_evidence = loop {
            self.check_cancelled("file sync content receipts")?;
            match self.posix_validate_file_sync_target_local(target) {
                Ok(evidence) => break evidence,
                Err(validation_error) => {
                    let Some((intent_id, durable_receipts, required_receipts)) =
                        self.pending_file_sync_receipt_state(target)?
                    else {
                        return Err(validation_error);
                    };

                    let repair_owner = self
                        .file_sync_repair_owner_after(
                            target,
                            required_receipts,
                            repair_owner_after.as_deref(),
                        )?
                        .unwrap_or_else(|| intent_id.clone());
                    repair_owner_after = Some(repair_owner.clone());
                    self.record_local_pending_write_receipt_if_desired(
                        &repair_owner,
                        &target.object_id,
                        &target.version,
                        &target.content_hash,
                    )?;
                    self.process_incoming()?;
                    if let Ok(evidence) = self.posix_validate_file_sync_target_local(target) {
                        break evidence;
                    }
                    // Served namespace lanes reopen the backend lazily and may
                    // have no coordinator state for this already committed
                    // prefix. Restore its exact durable owner before driving
                    // repairs; waiting while holding this lane cannot rely on
                    // a later maintenance pass to hydrate it for us.
                    self.restore_pending_write_for_intent_if_missing(
                        &repair_owner,
                        &target.object_id,
                    )?;
                    let intent_ids = BTreeSet::from([repair_owner]);
                    let (_scheduled, _events) = self.retry_pending_repairs_for_intents_best_effort(
                        &intent_ids,
                        required_receipts,
                    );
                    let elapsed = started_at.elapsed();
                    if elapsed >= timeout {
                        let (intent_id, durable_receipts, required_receipts) = self
                            .pending_file_sync_receipt_state(target)?
                            .unwrap_or((intent_id, durable_receipts, required_receipts));
                        return Err(DfsPosixError::PendingWrite {
                            intent_id,
                            durable_receipts,
                            required_receipts,
                        });
                    }
                    let wait = POSIX_SYNC_IDLE_WAIT.min(timeout.saturating_sub(elapsed));
                    self.transport
                        .wait_for_incoming(wait)
                        .map_err(crate::DfsError::from_network)?;
                }
            }
        };
        let object = self.current_object_for_file_sync_target(target)?;
        let metadata_target = self.capture_metadata_quorum_sync_target(&object)?;
        let remaining = timeout.saturating_sub(started_at.elapsed());
        let metadata_certificate = self
            .sync_metadata_shard_to_quorum_intersecting(
                &metadata_target,
                &content_evidence.receipt_node_ids,
                remaining,
            )
            .map_err(DfsPosixError::from)?;

        if !content_evidence
            .receipt_node_ids
            .iter()
            .any(|node_id| metadata_certificate.acknowledged_node_ids.contains(node_id))
        {
            return Err(DfsPosixError::Dfs(crate::DfsError::Distributed {
                message: format!(
                    "POSIX sync content receipts for {} {} do not intersect the metadata recovery quorum",
                    target.object_id, target.version
                ),
            }));
        }
        Ok(DfsPosixFileSyncCertificate {
            target: target.clone(),
            content_evidence,
            metadata_certificate,
        })
    }

    /// Capture the namespace generation visible before directory `fsync`.
    pub fn posix_capture_directory_sync_target(
        &mut self,
        path: &str,
    ) -> Result<DfsPosixDirectorySyncTarget> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (path, object) = self.lookup_sifs_path_distributed_synced(path, true)?;
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path });
        }
        Ok(DfsPosixDirectorySyncTarget {
            file_system_id: object.file_system_id,
            object_id: object.object_id,
            path,
            version: object.version,
        })
    }

    /// Certify a previously captured directory generation on the current
    /// metadata primary. No content receipt is consulted or implied.
    pub fn posix_sync_directory_local(
        &mut self,
        target: &DfsPosixDirectorySyncTarget,
        timeout: Duration,
    ) -> Result<DfsPosixDirectorySyncCertificate> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let object = self.current_object_for_directory_sync_target(target)?;
        let metadata_target = self.capture_metadata_quorum_sync_target(&object)?;
        let metadata_certificate = self
            .sync_metadata_shard_to_quorum(&metadata_target, timeout)
            .map_err(DfsPosixError::from)?;
        Ok(DfsPosixDirectorySyncCertificate {
            target: target.clone(),
            metadata_certificate,
        })
    }

    fn current_object_for_file_sync_target(
        &self,
        target: &DfsPosixFileSyncTarget,
    ) -> Result<DfsObjectMetadata> {
        let backend = self.metadata_backend.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: "POSIX file sync requires a durable metadata backend".to_string(),
            })
        })?;
        let object =
            backend
                .get_object(&target.object_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: target.path.clone(),
                })?;
        if object.file_system_id != target.file_system_id || object.kind == DfsObjectKind::Directory
        {
            return Err(sync_target_conflict(
                &target.path,
                format!(
                    "current object identity cannot host frozen file prefix {} {} {}",
                    target.object_id, target.version, target.content_hash
                ),
            ));
        }
        Ok(object)
    }

    /// A durable placement plan can remain as source/custody evidence after
    /// its repair executor is retired. It must not starve another actual
    /// committed plan for the same frozen bytes. Visit one strongest current
    /// owner per existing repair turn; the repair path still revalidates native
    /// authority and preserves every unresolved transfer identity.
    fn file_sync_repair_owner_after(
        &self,
        target: &DfsPosixFileSyncTarget,
        required_receipts: usize,
        after: Option<&str>,
    ) -> Result<Option<String>> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let owners = backend
            .intents_for_object(&target.object_id)?
            .into_iter()
            .filter(|intent| {
                intent.file_system_id == target.file_system_id
                    && intent.object_id == target.object_id
                    && intent.target_version == target.version
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.publication.is_current()
                    && intent.policy.desired_replicas.max(1) == required_receipts
                    && intent.content_hash.as_deref() == Some(target.content_hash.as_str())
                    && intent.object_metadata.as_ref().is_none_or(|object| {
                        object.file_system_id == target.file_system_id
                            && object.object_id == target.object_id
                            && object.version == target.version
                            && object.content_hash.as_deref() == Some(target.content_hash.as_str())
                    })
            })
            .map(|intent| intent.intent_id)
            .collect::<BTreeSet<_>>();
        Ok(owners
            .iter()
            .find(|owner| after.is_none_or(|after| owner.as_str() > after))
            .or_else(|| owners.first())
            .cloned())
    }

    fn pending_file_sync_receipt_state(
        &self,
        target: &DfsPosixFileSyncTarget,
    ) -> Result<Option<(String, usize, usize)>> {
        if let Err(DfsPosixError::PendingWrite {
            intent_id,
            durable_receipts,
            required_receipts,
        }) = self.posix_validate_file_sync_target_local(target)
        {
            return Ok(Some((intent_id, durable_receipts, required_receipts)));
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let mut intents = backend
            .intents_for_object(&target.object_id)?
            .into_iter()
            .filter(|intent| {
                intent.file_system_id == target.file_system_id
                    && intent.object_id == target.object_id
                    && intent.target_version == target.version
                    && matches!(
                        intent.status,
                        DfsWriteIntentStatus::Staging
                            | DfsWriteIntentStatus::ReceiptsPending
                            | DfsWriteIntentStatus::Committed
                    )
                    && intent.content_hash.as_deref() == Some(target.content_hash.as_str())
                    && intent.object_metadata.as_ref().is_none_or(|object| {
                        object.file_system_id == target.file_system_id
                            && object.object_id == target.object_id
                    })
            })
            .collect::<Vec<_>>();
        intents.sort_by(|left, right| {
            right
                .policy
                .desired_replicas
                .cmp(&left.policy.desired_replicas)
                .then_with(|| left.intent_id.cmp(&right.intent_id))
        });
        let Some(intent) = intents.into_iter().next() else {
            return Ok(None);
        };
        let desired_nodes = intent
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut receipt_nodes = backend
            .receipts_for_version(&target.object_id, &target.version)?
            .into_iter()
            .filter(|receipt| {
                receipt.receipt.file_system_id == target.file_system_id
                    && receipt.receipt.object_id == target.object_id
                    && receipt.receipt.committed_version == target.version
                    && receipt.content_hash.as_deref() == Some(target.content_hash.as_str())
                    && (desired_nodes.is_empty()
                        || desired_nodes.contains(&receipt.replica_node_id))
            })
            .map(|receipt| receipt.replica_node_id)
            .collect::<Vec<_>>();
        receipt_nodes.sort();
        receipt_nodes.dedup();
        Ok(Some((
            intent.intent_id,
            receipt_nodes.len(),
            intent.policy.desired_replicas.max(1),
        )))
    }

    fn current_object_for_directory_sync_target(
        &self,
        target: &DfsPosixDirectorySyncTarget,
    ) -> Result<DfsObjectMetadata> {
        let backend = self.metadata_backend.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: "POSIX directory sync requires a durable metadata backend".to_string(),
            })
        })?;
        let version = backend
            .get_version(&target.object_id, &target.version)?
            .ok_or_else(|| {
                sync_target_conflict(
                    &target.path,
                    format!(
                        "frozen directory generation {} for object {} is not durably recorded",
                        target.version, target.object_id
                    ),
                )
            })?;
        if version.file_system_id != target.file_system_id
            || version.object_id != target.object_id
            || !version.visible
        {
            return Err(sync_target_conflict(
                &target.path,
                format!(
                    "durable directory generation does not match frozen object/version {} {}",
                    target.object_id, target.version
                ),
            ));
        }
        let object =
            backend
                .get_object(&target.object_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: target.path.clone(),
                })?;
        if object.file_system_id != target.file_system_id || object.kind != DfsObjectKind::Directory
        {
            return Err(sync_target_conflict(
                &target.path,
                format!(
                    "current directory identity cannot host frozen generation {} {}",
                    target.object_id, target.version
                ),
            ));
        }
        Ok(object)
    }

    fn route_posix_sync_operation(
        &mut self,
        mut primary: sinomo_core::node::NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<DfsPosixCoordinationResult> {
        let file_system_id = crate::sifs_file_system_id();
        for _ in 0..POSIX_SYNC_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    DFS_POSIX_SYNC_RPC_TIMEOUT,
                    false,
                    false,
                )
                .map_err(DfsPosixError::from)?;
            match settled.result {
                DfsNamespaceResult::Applied => {
                    return settled.posix_coordination_result.ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Distributed {
                            message: "POSIX sync response omitted its typed certificate"
                                .to_string(),
                        })
                    });
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id
                    && !self.transport.delegates_local_namespace_authority() =>
                {
                    return self
                        .execute_posix_coordination_operation_local(&operation)?
                        .ok_or_else(|| {
                            DfsPosixError::Dfs(DfsError::Distributed {
                                message: "local POSIX sync omitted its typed certificate"
                                    .to_string(),
                            })
                        });
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    primary = self
                        .refreshed_primary_after_authority_redirect(
                            &file_system_id,
                            &operation,
                            &primary,
                            current,
                        )
                        .map_err(DfsPosixError::from)?;
                }
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    ..
                } => return Err(posix_error_for_namespace_rejection(reason, posix_error)),
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "POSIX sync exceeded {POSIX_SYNC_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        }))
    }
}

fn sync_target_conflict(path: &str, reason: String) -> DfsPosixError {
    DfsPosixError::Conflict {
        path: path.to_string(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use sinomo_core::node::NodeId;

    use super::*;
    use crate::{DfsReplicationPolicy, DfsWriteOptions, NetworkedDfsService};

    struct PosixSyncFixture {
        _runtime: crate::test_runtime::DfsTestRuntime,
        owner: NodeId,
        service: NetworkedDfsService<crate::DfsNodeTransport>,
    }

    fn sync_fixture(name: &str) -> PosixSyncFixture {
        let owner = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, owner.clone()).unwrap();
        runtime.start_nodes().unwrap();
        let service = runtime.client(&owner).unwrap();
        PosixSyncFixture {
            owner,
            service,
            _runtime: runtime,
        }
    }

    fn local_write_options(owner: &NodeId) -> DfsWriteOptions {
        DfsWriteOptions::replicated_eventual(vec![owner.clone()])
    }

    #[test]
    fn file_sync_certifies_the_frozen_prefix_after_a_later_write() {
        let mut fixture = sync_fixture("frozen_prefix");
        fixture
            .service
            .posix_write_file(
                "/services/sync-prefix.txt",
                b"first durable prefix",
                local_write_options(&fixture.owner),
            )
            .expect("first write");
        let target = fixture
            .service
            .posix_capture_file_sync_target("/services/sync-prefix.txt", DfsPosixSyncKind::FileData)
            .expect("capture first prefix");
        fixture
            .service
            .posix_write_file(
                "/services/sync-prefix.txt",
                b"later write",
                local_write_options(&fixture.owner),
            )
            .expect("second write");

        let certificate = fixture
            .service
            .posix_sync_file_local(&target, Duration::ZERO)
            .expect("the captured historical version remains independently certifiable");
        assert_eq!(certificate.target, target);
    }

    #[test]
    fn directory_sync_certifies_a_captured_generation_after_a_later_mutation() {
        let mut fixture = sync_fixture("directory_generation");
        let target = fixture
            .service
            .posix_capture_directory_sync_target("/services")
            .expect("capture directory generation");
        fixture
            .service
            .posix_write_file(
                "/services/created-after-capture.txt",
                b"later namespace generation",
                local_write_options(&fixture.owner),
            )
            .expect("later namespace mutation");
        assert_ne!(
            fixture
                .service
                .posix_stat("/services")
                .expect("current directory")
                .version,
            target.version
        );

        let certificate = fixture
            .service
            .posix_sync_directory_local(&target, Duration::ZERO)
            .expect("the captured directory generation remains independently certifiable");
        assert_eq!(certificate.target, target);
    }

    #[test]
    fn file_sync_reports_an_uncommitted_returned_write_as_pending() {
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("pending_write", 2);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("pending_write", nodes[0].clone()).unwrap();
        runtime.define_node(nodes[1].clone()).unwrap();
        runtime.start_nodes().unwrap();
        let mut configuration = runtime.client(&nodes[0]).unwrap();
        let mut file_system = runtime.file_system();
        let mut policy = crate::DfsSubtreePolicy::new(
            "/services/pending-sync.txt",
            sinomo_core::DfsObjectPolicy::ReplicatedConfirmed,
            2,
            2,
        );
        policy.required_node_ids = nodes.clone();
        file_system.subtree_policies.push(policy);
        configuration.add_file_system(file_system).unwrap();
        drop(configuration);
        let service = runtime
            .metadata_primary_service("/services/pending-sync.txt")
            .unwrap();
        let owner = service.node_id().clone();
        let missing = nodes.iter().find(|node| **node != owner).unwrap().clone();
        // Both nodes remain eligible and available to metadata authority. Only
        // the actual second content transfer fails, leaving exactly one receipt.
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: None,
            target: Some(missing),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
        let mut fixture = PosixSyncFixture {
            owner,
            service,
            _runtime: runtime,
        };
        let stat = fixture
            .service
            .posix_write_file(
                "/services/pending-sync.txt",
                b"not yet at the two-replica commit threshold",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(2, 2).expect("confirmed policy"),
                    replica_candidates: nodes,
                    wait_for_commit: false,
                    foreground_replica_repair: false,
                },
            )
            .expect("detached write returns");
        let target = fixture
            .service
            .posix_capture_file_sync_target("/services/pending-sync.txt", DfsPosixSyncKind::File)
            .expect("capture returned write");
        assert_eq!(target.version, stat.version);

        let error = fixture
            .service
            .posix_sync_file_local(&target, Duration::ZERO)
            .expect_err("uncommitted write cannot be certified");
        assert!(
            matches!(
                &error,
                DfsPosixError::PendingWrite {
                    durable_receipts: 1,
                    required_receipts: 2,
                    ..
                }
            ),
            "unexpected sync result: {error:?}"
        );
        let pending = fixture
            .service
            .writes
            .pending_writes
            .iter()
            .find(|(_, pending)| {
                pending.object_id == stat.object_id && pending.version == stat.version
            })
            .map(|(intent, _)| intent.clone())
            .expect("same actual pending producer");
        let aborted = fixture.service.abort_pending_write(&pending).unwrap();
        assert_eq!(aborted.state, crate::DfsWriteIntentState::Aborted);
        let PosixSyncFixture {
            service, _runtime, ..
        } = fixture;
        drop(service);
        _runtime.clear_faults();
        _runtime.close().unwrap();
    }
}
