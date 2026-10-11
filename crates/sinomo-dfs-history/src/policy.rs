//! History settings and capture command policy.

use crate::*;

fn history_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

fn content_reference_request(
    request: &DfsFileHistoryPinRequest,
) -> Result<DfsContentReferenceRequest, DfsError> {
    let operation = match &request.operation {
        DfsFileHistoryPinOperation::Capture { pin_id } => {
            DfsContentReferenceOperation::CaptureCurrent {
                reference_id: pin_id.clone(),
                metadata: Some(DfsModuleMetadataValue {
                    schema_version: 1,
                    payload: serde_json::to_value(DfsFileVersionSource::Checkpoint {
                        operation_id: request.operation_id.clone(),
                    })
                    .map_err(|error| history_error(error.to_string()))?,
                }),
            }
        }
        DfsFileHistoryPinOperation::Pin { pin_id } => DfsContentReferenceOperation::Retain {
            reference_id: pin_id.clone(),
        },
        DfsFileHistoryPinOperation::Unpin { pin_id } => DfsContentReferenceOperation::Release {
            reference_id: pin_id.clone(),
        },
    };
    Ok(DfsContentReferenceRequest {
        namespace: HISTORY_MODULE_ID.into(),
        path: request.path.clone(),
        identity: request.identity.clone(),
        operation_id: request.operation_id.clone(),
        operation,
    })
}

impl<T: DfsMessageTransport> DfsHistoryService<'_, T> {
    /// sinomo:kv-configuration: Read this module's namespaced retention settings through the existing filesystem configuration owner.
    pub fn file_history_policy(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<DfsFileHistoryPolicy, DfsError> {
        let kv = self
            .core
            .control_plane_client_mut()
            .ok_or_else(|| history_error("retention settings require cluster configuration"))?;
        let config = DfsControlPlaneStore::new(kv)
            .get_file_system_config(file_system_id)?
            .ok_or_else(|| history_error("filesystem configuration is missing"))?;
        Ok(crate::metadata::history_policy(
            config.retention_policies.get(HISTORY_MODULE_ID),
        ))
    }

    /// sinomo:kv-configuration: Compare and replace only this module's retention namespace through the filesystem owner's configuration CAS.
    pub fn update_file_history_policy(
        &mut self,
        request: DfsFileHistoryPolicyUpdateRequest,
    ) -> Result<DfsFileHistoryPolicy, DfsError> {
        request.policy.validate().map_err(history_error)?;
        let kv = self
            .core
            .control_plane_client_mut()
            .ok_or_else(|| history_error("retention settings require cluster configuration"))?;
        let mut store = DfsControlPlaneStore::new(kv);
        for _ in 0..8 {
            let current = store
                .get_file_system_config(&request.file_system_id)?
                .ok_or_else(|| history_error("filesystem configuration is missing"))?;
            let current_policy =
                crate::metadata::history_policy(current.retention_policies.get(HISTORY_MODULE_ID));
            if current_policy == request.policy {
                return Ok(current_policy);
            }
            if current_policy != request.expected {
                return Err(history_error(
                    "retention configuration changed; read the current policy before retrying",
                ));
            }
            let mut desired = current.clone();
            if request.policy.is_empty() {
                desired.retention_policies.remove(HISTORY_MODULE_ID);
            } else {
                desired.retention_policies.insert(
                    HISTORY_MODULE_ID.into(),
                    crate::metadata::core_policy(&request.policy),
                );
            }
            if store.compare_and_put_file_system_config(Some(&current), &desired)? {
                self.core
                    .refresh_file_system_configuration(&request.file_system_id)?;
                return Ok(request.policy);
            }
        }
        Err(history_error(
            "retention configuration is busy; retry against its current value",
        ))
    }

    pub fn preserve_local_file_version<R: std::io::Read>(
        &mut self,
        request: DfsFileHistoryPreserveRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryPreserveOutcome, DfsError> {
        if request.local_operation_id.trim().is_empty()
            || request.local_operation_id.len() > 256
            || request.local_operation_id.contains('\0')
        {
            return Err(history_error(
                "local preservation requires a bounded operation identity",
            ));
        }
        let source = DfsFileVersionSource::LocalOperation {
            operation_id: request.local_operation_id,
            base_version: request.base_version,
        };
        let publication = DfsRetainedContentPublication {
            namespace: HISTORY_MODULE_ID.into(),
            reference_id: request.pin_id,
            metadata: Some(DfsModuleMetadataValue {
                schema_version: 1,
                payload: serde_json::to_value(source)
                    .map_err(|error| history_error(error.to_string()))?,
            }),
        };
        publication.validate().map_err(history_error)?;
        self.core.preserve_content_revision(
            DfsPreservedContentRequest {
                path: request.path,
                file_system_id: request.file_system_id,
                object_id: request.object_id,
                operation_id: request.operation_id,
                publication,
            },
            reader,
            options,
        )
    }

    pub fn file_history_pin(
        &mut self,
        mut request: DfsFileHistoryPinRequest,
    ) -> Result<DfsFileHistoryPinResponse, DfsError> {
        request.path = sinomo_dfs::normalize_semantic_path(&request.path)
            .map_err(|error| history_error(error.to_string()))?;
        let reference = content_reference_request(&request)?;
        let mut ambiguous_outcome = None;
        // Reconcile the exact terminal outcome before inspecting a revision
        // or planning another effect. Collection or changed configuration
        // cannot turn a completed retry into a fresh semantic operation.
        if self.core.is_distributed() {
            let operation = DfsNamespaceOperation::ContentReference {
                request: reference.clone(),
            };
            match self
                .core
                .reconcile_stable_namespace_operation(&request.operation_id, &operation)?
            {
                DfsStableNamespaceOperationReconciliation::Applied { .. } => {
                    self.core
                        .reconcile_content_reference_maintenance(&request.identity)?;
                    return Ok(DfsFileHistoryPinResponse {
                        identity: request.identity,
                    });
                }
                DfsStableNamespaceOperationReconciliation::NotApplied => {}
                DfsStableNamespaceOperationReconciliation::Conflict { reason } => {
                    return Err(history_error(reason));
                }
                DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired { reason } => {
                    ambiguous_outcome = Some(reason)
                }
            }
        } else if let Some(response) = self.core.reconcile_content_reference(&reference)? {
            self.core
                .reconcile_content_reference_maintenance(&response.identity)?;
            return Ok(DfsFileHistoryPinResponse {
                identity: response.identity,
            });
        }
        let mut canonical_path_changed = false;
        let mut missing_history_capture = false;
        if matches!(
            request.operation,
            DfsFileHistoryPinOperation::Capture { .. }
        ) {
            // Keep the original hint on a failed lookup: the durable request
            // receipt may still prove an exact retry after deletion or rename.
            if request.identity.file_system_id == sifs_file_system_id()
                && let Ok(object) = self.core.current_content_object(&request.path)
                && object.object_id == request.identity.object_id
            {
                canonical_path_changed =
                    request.path != sinomo_dfs::normalize_semantic_path_lossy(&object.path);
            }
        } else {
            // Selection is metadata-only: a live source object may repin an
            // alternative revision after its last reference was released.
            // This module still requires its own capture provenance; the core
            // writer separately authorizes retention of tombstoned sources.
            let descriptor = self.core.revision_metadata(&request.identity)?;
            missing_history_capture =
                matches!(request.operation, DfsFileHistoryPinOperation::Pin { .. })
                    && descriptor
                        .as_ref()
                        .map(|descriptor| {
                            crate::metadata::capture_from_version(&descriptor.version)
                        })
                        .transpose()?
                        .flatten()
                        .is_none();
            if let Some(descriptor) = descriptor {
                canonical_path_changed = request.path
                    != sinomo_dfs::normalize_semantic_path_lossy(&descriptor.current_object.path);
            }
        }
        if missing_history_capture || canonical_path_changed {
            // An exact terminal outcome outlives both its source and its path.
            // The existing owner-routed read-only protocol authenticates the
            // requester and checks the complete command digest. Its typed
            // Applied proof satisfies the module's retry precondition.
            if let Some(reason) = ambiguous_outcome {
                return Err(history_error(reason));
            }
        }
        if missing_history_capture {
            return Err(history_error("History pin requires a captured version"));
        }
        self.core
            .retain_content_revision(content_reference_request(&request)?)?;
        Ok(DfsFileHistoryPinResponse {
            identity: request.identity,
        })
    }
}
