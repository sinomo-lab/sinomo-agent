//! POSIX coordination operations served by a metadata-shard primary.
//!
//! These operations reuse the namespace RPC envelope because it already
//! authenticates the frame source, routes by the current shard assignment,
//! rejects stale primaries, and suppresses mismatched responses. The payload
//! and response remain separately typed so a lock outcome can never be
//! mistaken for a namespace mutation acknowledgement.

use crate::posix::locks::DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT;
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn execute_posix_coordination_operation_local(
        &mut self,
        operation: &DfsNamespaceOperation,
    ) -> posix_model::Result<Option<DfsPosixCoordinationResult>> {
        match operation {
            DfsNamespaceOperation::BeginContentCopy {
                path,
                request,
                options,
            } => self
                .begin_content_copy_job(path, request, options)
                .map(DfsPosixCoordinationResult::NamespaceJob)
                .map(Some)
                .map_err(DfsPosixError::from),
            DfsNamespaceOperation::MetadataQuery { query } => self
                .serve_metadata_query(query)
                .map(DfsPosixCoordinationResult::MetadataQuery)
                .map(Some)
                .map_err(DfsPosixError::from),
            DfsNamespaceOperation::RenameDirectoryBounded {
                from,
                to,
                max_steps,
            } => self
                .posix_rename_directory_bounded(from, to, usize_from_bounded_steps(*max_steps))
                .map(DfsPosixCoordinationResult::NamespaceJob)
                .map(Some),
            DfsNamespaceOperation::ResumeNamespaceJobBounded {
                job_id, max_steps, ..
            } => self
                .posix_resume_namespace_job_bounded(job_id, usize_from_bounded_steps(*max_steps))
                .map(DfsPosixCoordinationResult::NamespaceJob)
                .map(Some),
            DfsNamespaceOperation::AdvisoryLock {
                path,
                object_id,
                command,
                timeout_millis,
            } => self
                .posix_apply_advisory_lock_local(
                    path,
                    object_id,
                    command,
                    std::time::Duration::from_millis(*timeout_millis)
                        .min(DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT)
                        .max(std::time::Duration::from_millis(1)),
                )
                .map(DfsPosixCoordinationResult::AdvisoryLock)
                .map(Some),
            DfsNamespaceOperation::FileSync { target, .. } => self
                .posix_sync_file_local(target, DEFAULT_DFS_POSIX_SYNC_TIMEOUT)
                .map(DfsPosixCoordinationResult::FileSync)
                .map(Some),
            DfsNamespaceOperation::DirectorySync { target, .. } => self
                .posix_sync_directory_local(target, DEFAULT_DFS_POSIX_SYNC_TIMEOUT)
                .map(DfsPosixCoordinationResult::DirectorySync)
                .map(Some),
            DfsNamespaceOperation::AdvisoryLockMigrationPrepare {
                object_id,
                state_fingerprint,
                ..
            } => {
                if state_fingerprint.is_empty() {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: "advisory-lock migration prepare omitted its state fingerprint"
                            .to_string(),
                    }));
                }
                Ok(Some(
                    DfsPosixCoordinationResult::AdvisoryLockMigrationPrepared {
                        object_id: object_id.clone(),
                        state_fingerprint: state_fingerprint.clone(),
                    },
                ))
            }
            DfsNamespaceOperation::AdoptObjectAtPath {
                object,
                source_path,
                pin_transfer_id: Some(pin_transfer_id),
                versions,
                version_pins,
                version_pin_set_fingerprint: Some(expected_pin_fingerprint),
                receipts,
                intents,
                intent_receipts,
                intent_receipt_retention,
                content_reference_outcomes,
                content_reference_outcome_retention,
                conflicts,
                advisory_lock_state,
            } => {
                let canonical_pin_fingerprint =
                    crate::protocol::cross_shard_version_pin_set_fingerprint(object, version_pins)
                        .map_err(DfsPosixError::from)?;
                if &canonical_pin_fingerprint != expected_pin_fingerprint {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "cross-shard adoption of {} requested a mismatched durable version-pin set",
                            object.object_id
                        ),
                    }));
                }
                let pin_transfer_active = self.adopt_object_at_path_local(
                    object,
                    source_path,
                    Some(pin_transfer_id),
                    versions,
                    version_pins,
                    Some(expected_pin_fingerprint),
                    receipts,
                    intents,
                    intent_receipts,
                    intent_receipt_retention,
                    content_reference_outcomes,
                    content_reference_outcome_retention.as_ref(),
                    conflicts,
                    advisory_lock_state.as_ref(),
                )?;

                let installed_pins = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.version_pins_for_object(&object.object_id))
                    .transpose()?
                    .unwrap_or_default();
                if !version_pins
                    .iter()
                    .all(|expected| installed_pins.iter().any(|installed| installed == expected))
                {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "cross-shard adoption did not retain every durable source pin for {}",
                            object.object_id
                        ),
                    }));
                }

                let advisory_lock_state_fingerprint = if let Some(source_lock_state) =
                    advisory_lock_state
                {
                    let (_expected_lock_state, state_fingerprint) =
                        crate::advisory_lock::prepare_advisory_lock_state_for_migration(
                            source_lock_state,
                        )
                        .map_err(|source| {
                            DfsPosixError::Dfs(DfsError::Serialization {
                                message: format!(
                                    "encode migrated advisory-lock state for {}: {source}",
                                    object.object_id
                                ),
                            })
                        })?;
                    let installed_lock_state = self
                            .metadata_backend
                            .as_ref()
                            .ok_or_else(|| {
                                DfsPosixError::Dfs(DfsError::Metadata {
                                    message: "lock-bearing cross-shard adoption requires a durable metadata backend"
                                        .to_string(),
                                })
                            })?
                            .get_advisory_lock_state(
                                &object.file_system_id,
                                &object.object_id,
                            )?;
                    if installed_lock_state.as_ref().is_none_or(|installed| {
                        !installed
                            .migration_source_fingerprints
                            .contains(&state_fingerprint)
                    }) {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "cross-shard adoption did not install the exact advisory-lock aggregate for {}",
                                object.object_id
                            ),
                        }));
                    }
                    Some(state_fingerprint)
                } else {
                    None
                };

                // The exact reconciliation ran before namespace publication.
                // Always carry it through a quorum barrier, including the
                // empty set: a retry may have removed pins left by a partial
                // first copy even when no object index exposed them yet.
                let metadata_target = self
                    .capture_metadata_quorum_sync_target(object)
                    .map_err(DfsPosixError::from)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )
                .map_err(DfsPosixError::from)?;
                Ok(Some(DfsPosixCoordinationResult::ObjectFamilyAdoption {
                    object_id: object.object_id.clone(),
                    target_version: object.version.clone(),
                    pin_transfer_id: pin_transfer_id.clone(),
                    pin_transfer_active,
                    version_pin_set_fingerprint: canonical_pin_fingerprint,
                    advisory_lock_state_fingerprint,
                    content_reference_outcome_fingerprint:
                        crate::protocol::content_reference_outcome_family_fingerprint(
                            object,
                            content_reference_outcomes,
                            content_reference_outcome_retention.as_ref(),
                        )
                        .map_err(DfsPosixError::from)?,
                    durable_through_sequence: metadata_target.journal_sequence,
                }))
            }
            DfsNamespaceOperation::VerifyObjectFamilyAdoption {
                object,
                source_path,
                pin_transfer_id,
            } => {
                let expected_transfer_id =
                    crate::protocol::cross_shard_pin_transfer_id(object, source_path)
                        .map_err(DfsPosixError::from)?;
                if &expected_transfer_id != pin_transfer_id {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "cross-shard adoption proof for {} named transfer {}, expected {}",
                            object.object_id, pin_transfer_id, expected_transfer_id
                        ),
                    }));
                }
                let backend = self.metadata_backend.as_ref().ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "cross-shard adoption proof requires a durable metadata backend"
                            .to_string(),
                    })
                })?;
                let current = backend.get_object(&object.object_id)?.ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "cross-shard adoption proof target object {} is missing",
                            object.object_id
                        ),
                    })
                })?;
                if current.kind == DfsObjectKind::Tombstone
                    || current.file_system_id != object.file_system_id
                    || crate::metadata_keys::normalize_path(&current.path)
                        != crate::metadata_keys::normalize_path(&object.path)
                    || current.version != object.version
                {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "cross-shard adoption proof target {} is not the exact live generation at {} version {}",
                            object.object_id, object.path, object.version
                        ),
                    }));
                }
                let active_transfers =
                    backend.cross_shard_pin_transfers_for_object(&object.object_id)?;
                if !active_transfers.is_empty() {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "cross-shard adoption proof target {} still has an active pin transfer",
                            object.object_id
                        ),
                    }));
                }
                let metadata_target = self
                    .capture_metadata_quorum_sync_target(object)
                    .map_err(DfsPosixError::from)?;
                Ok(Some(
                    DfsPosixCoordinationResult::ObjectFamilyAdoptionVerified {
                        object_id: object.object_id.clone(),
                        target_version: object.version.clone(),
                        pin_transfer_id: pin_transfer_id.clone(),
                        durable_through_sequence: metadata_target.journal_sequence,
                    },
                ))
            }
            DfsNamespaceOperation::FinalizeObjectFamilyAdoption {
                path: _,
                source_path,
                object_id,
                target_version,
                pin_transfer_id,
            } => {
                let object = self
                    .metadata_backend
                    .as_ref()
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message:
                                "pin-transfer finalization requires a durable metadata backend"
                                    .to_string(),
                        })
                    })?
                    .get_object(object_id)?
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "pin-transfer finalization target object {object_id} is missing"
                            ),
                        })
                    })?;
                if object.version != *target_version
                    && self
                        .metadata_backend
                        .as_ref()
                        .expect("metadata backend checked above")
                        .get_version(object_id, target_version)?
                        .is_none()
                {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "pin-transfer finalization target {object_id} does not contain acknowledged version {target_version}; current visible version is {}",
                            object.version
                        ),
                    }));
                }
                let mut transfer_object = object;
                transfer_object.version = target_version.clone();
                self.metadata_backend
                    .as_mut()
                    .expect("metadata backend checked above")
                    .finalize_cross_shard_pin_transfer(
                        pin_transfer_id,
                        &transfer_object,
                        source_path,
                    )?;
                let metadata_target = self
                    .capture_metadata_quorum_sync_target(&transfer_object)
                    .map_err(DfsPosixError::from)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )
                .map_err(DfsPosixError::from)?;
                Ok(Some(
                    DfsPosixCoordinationResult::ObjectFamilyAdoptionFinalized {
                        object_id: object_id.clone(),
                        pin_transfer_id: pin_transfer_id.clone(),
                        durable_through_sequence: metadata_target.journal_sequence,
                    },
                ))
            }
            DfsNamespaceOperation::AdoptObjectAtPath {
                object,
                advisory_lock_state: Some(source_lock_state),
                ..
            } => {
                self.execute_namespace_operation_local(operation)?;
                let (_expected_lock_state, state_fingerprint) =
                    crate::advisory_lock::prepare_advisory_lock_state_for_migration(
                        source_lock_state,
                    )
                    .map_err(|source| {
                        DfsPosixError::Dfs(DfsError::Serialization {
                            message: format!(
                                "encode migrated advisory-lock state for {}: {source}",
                                object.object_id
                            ),
                        })
                    })?;
                let installed_lock_state = self
                    .metadata_backend
                    .as_ref()
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message: "lock-bearing cross-shard adoption requires a durable metadata backend"
                                .to_string(),
                        })
                    })?
                    .get_advisory_lock_state(&object.file_system_id, &object.object_id)?;
                if installed_lock_state.as_ref().is_none_or(|installed| {
                    !installed
                        .migration_source_fingerprints
                        .contains(&state_fingerprint)
                }) {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "cross-shard adoption did not install the exact advisory-lock aggregate for {}",
                            object.object_id
                        ),
                    }));
                }
                let metadata_target = self
                    .capture_metadata_quorum_sync_target(object)
                    .map_err(DfsPosixError::from)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )
                .map_err(DfsPosixError::from)?;
                Ok(Some(DfsPosixCoordinationResult::AdvisoryLockMigration {
                    object_id: object.object_id.clone(),
                    state_fingerprint,
                    durable_through_sequence: metadata_target.journal_sequence,
                }))
            }
            _ => self
                .execute_namespace_operation_local(operation)
                .map(|()| None),
        }
    }
}

fn usize_from_bounded_steps(max_steps: u64) -> usize {
    usize::try_from(max_steps).unwrap_or(usize::MAX)
}
