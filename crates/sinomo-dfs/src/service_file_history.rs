//! Explicit file captures and pins use the existing namespace authority lane.

use crate::*;
use std::time::Duration;
use time::OffsetDateTime;

fn history_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

fn content_reference_digest(
    request: &DfsContentReferenceRequest,
) -> Result<Option<String>, DfsError> {
    let bytes = serde_json::to_vec(request).map_err(|error| DfsError::Serialization {
        message: error.to_string(),
    })?;
    Ok(Some(blake3::hash(&bytes).to_hex().to_string()))
}

fn content_reference_posix_error(error: DfsPosixError) -> DfsError {
    match error {
        DfsPosixError::Dfs(error) => error,
        error => history_error(error.to_string()),
    }
}

/// Classification from the actual namespace execution result, before diagnostics.
pub(crate) enum DfsContentReferenceMutationError {
    Rejected { reason: String },
    Uncertain(DfsError),
}
impl From<DfsError> for DfsContentReferenceMutationError {
    fn from(error: DfsError) -> Self {
        Self::Uncertain(error)
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn refresh_file_history_configuration(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>, DfsError> {
        if let Some(kv) = self.control_plane.as_mut() {
            let config = tracing::trace_span!(target: "sinomo.perf", "dfs.history.refresh",
                stage = "dfs.history.refresh", phase = "fetch_configuration")
            .in_scope(|| {
                DfsControlPlaneStore::new(kv.as_mut()).get_file_system_config(file_system_id)
            })?;
            if let Some(config) = config {
                let _cache = tracing::trace_span!(target: "sinomo.perf", "dfs.history.refresh",
                    stage = "dfs.history.refresh", phase = "cache_configuration")
                .entered();
                let file_system = DfsFileSystem::from(config);
                self.metadata.add_file_system(file_system.clone());
                return Ok(Some(file_system));
            }
        }
        Ok(self.metadata.filesystems.get(file_system_id).cloned())
    }

    /// Bind every content-publication path, including batched commits, to the
    /// current authoritative policy captured in its complete data prepare.
    pub(crate) fn refresh_file_history_for_publication(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let Some(file_system) = self.refresh_file_history_configuration(&object.file_system_id)?
        else {
            return Ok(());
        };
        let path = crate::metadata_keys::normalize_posix_path(&object.path);
        if !file_system.retention_policies.is_empty() {
            tracing::trace_span!(target: "sinomo.perf", "dfs.history.refresh",
                stage = "dfs.history.refresh", phase = "validate_codec_support")
            .in_scope(|| self.require_file_history_shard_support(&object.file_system_id, &path))?;
        }
        let resolve_span = tracing::trace_span!(target: "sinomo.perf", "dfs.history.refresh",
            stage = "dfs.history.refresh", phase = "resolve_shards")
        .entered();
        self.namespace_shards_for_exact_paths(&object.file_system_id, [path.as_str()])?;
        drop(resolve_span);
        // The actual scoped filesystem definition is sealed into the same
        // prepared body as the content/namespace effect by the common facade.

        Ok(())
    }

    pub(crate) fn require_file_history_shard_support(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<(), DfsError> {
        self.require_file_history_shards_support(file_system_id, [path])
    }

    pub(crate) fn require_file_history_shards_support<'a>(
        &mut self,
        file_system_id: &DfsFileSystemId,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), DfsError> {
        if self.control_plane.is_none() {
            return Ok(());
        }
        let shards = self.namespace_shards_for_exact_paths(file_system_id, paths)?;
        if shards.is_empty() {
            return Err(history_error(
                "retention mutation has no current metadata shard assignment",
            ));
        }
        // Current configuration admission requires the sole native protocol.
        Ok(())
    }

    /// sinomo:dfs-operation
    pub fn retain_content_revision(
        &mut self,
        request: DfsContentReferenceRequest,
    ) -> Result<DfsContentReferenceResponse, DfsError> {
        self.retain_content_revision_classified(request)
            .map_err(|error| match error {
                DfsContentReferenceMutationError::Rejected { reason } => history_error(reason),
                DfsContentReferenceMutationError::Uncertain(error) => error,
            })
    }

    pub(crate) fn retain_content_revision_classified(
        &mut self,
        mut request: DfsContentReferenceRequest,
    ) -> Result<DfsContentReferenceResponse, DfsContentReferenceMutationError> {
        request.validate()?;
        {
            request.path = crate::normalize_semantic_path(&request.path)
                .map_err(content_reference_posix_error)?;
            if let Some(response) = self.reconcile_content_reference(&request)? {
                self.reconcile_content_reference_maintenance(&response.identity)?;
                return Ok(response);
            }
        }
        let mut routing_path = request.path.clone();
        {
            if let Some(object) = self.resolve_file_version_current_object(&request.identity)? {
                routing_path = crate::metadata_keys::normalize_posix_path(&object.path);
            }
            if self.control_plane.is_some() && routing_path != request.path {
                let operation = DfsNamespaceOperation::ContentReference {
                    request: request.clone(),
                };
                match self
                    .reconcile_stable_namespace_operation(&request.operation_id, &operation)?
                {
                    DfsStableNamespaceOperationReconciliation::Applied { .. } => {
                        self.reconcile_content_reference_maintenance(&request.identity)?;
                        return Ok(DfsContentReferenceResponse {
                            identity: request.identity,
                        });
                    }
                    DfsStableNamespaceOperationReconciliation::NotApplied => {}
                    DfsStableNamespaceOperationReconciliation::Conflict { reason }
                    | DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired { reason } => {
                        return Err(history_error(reason).into());
                    }
                }
            }
        }
        self.require_file_history_shard_support(&request.identity.file_system_id, &routing_path)?;
        let operation = DfsNamespaceOperation::ContentReference {
            request: request.clone(),
        };
        let shards = self.namespace_shards_for_exact_paths(
            &request.identity.file_system_id,
            [routing_path.as_str()],
        )?;
        if self.control_plane.is_some() {
            let [shard] = shards.as_slice() else {
                return Err(history_error(
                    "file history mutation has no unique metadata authority",
                )
                .into());
            };
            if shard.primary_node_id == self.node_id
                && !self.transport.delegates_local_namespace_authority()
            {
                let result = self
                    .execute_local_primary_namespace_result_for_file_system(
                        request.identity.file_system_id.clone(),
                        request.operation_id.clone(),
                        operation,
                    )
                    .map_err(content_reference_posix_error)?;
                if let DfsNamespaceResult::Rejected { reason, .. } = result {
                    return Err(DfsContentReferenceMutationError::Rejected { reason });
                }
            } else {
                let result = self
                    .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                        request.operation_id.clone(),
                        request.identity.file_system_id.clone(),
                        shard.primary_node_id.clone(),
                        operation,
                        Duration::from_secs(60),
                        false,
                        false,
                    )?;
                if let DfsNamespaceResult::Rejected { reason, .. } = result.result {
                    return Err(DfsContentReferenceMutationError::Rejected { reason });
                }
            }
        } else {
            self.execute_namespace_operation_local(&operation)
                .map_err(content_reference_posix_error)?;
        }
        Ok(DfsContentReferenceResponse {
            identity: request.identity,
        })
    }

    /// Prove an exact standalone reference retry from its durable transaction
    /// or compact witness. Clustered callers use authenticated owner-routed
    /// namespace reconciliation instead. Absence grants no fresh authority.
    pub fn reconcile_content_reference(
        &self,
        request: &DfsContentReferenceRequest,
    ) -> Result<Option<DfsContentReferenceResponse>, DfsError> {
        request.validate()?;
        if self.control_plane.is_some() {
            return Ok(None);
        }
        let mut normalized = request.clone();
        {
            normalized.path = crate::normalize_semantic_path(&normalized.path)
                .map_err(content_reference_posix_error)?;
        }
        if self.content_reference_transaction_replayed(&normalized)? {
            return Ok(Some(DfsContentReferenceResponse {
                identity: request.identity.clone(),
            }));
        }
        Ok(None)
    }

    fn content_reference_transaction_replayed(
        &self,
        request: &DfsContentReferenceRequest,
    ) -> Result<bool, DfsError> {
        let Some(digest) = content_reference_digest(request)? else {
            return Ok(false);
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| history_error("content reference requires durable metadata"))?;
        let transaction_id = format!("file-history:{}", request.operation_id);
        let recorded = backend
            .get_namespace_transaction_receipt(&transaction_id)?
            .map(|receipt| (receipt.file_system_id, receipt.contribution_digest));
        let Some((file_system_id, recorded_digest)) = recorded else {
            if backend.content_reference_outcome_history_expired(
                &request.identity,
                &request.operation_id,
            )? {
                return Err(history_error(
                    "content reference operation outcome expired; authoritative reconciliation is required",
                ));
            }
            return Ok(false);
        };
        let Some(recorded_digest) = recorded_digest else {
            return Err(history_error(
                "content reference operation identity belongs to a transaction without its logical contribution digest",
            ));
        };
        if file_system_id != request.identity.file_system_id || recorded_digest != digest {
            return Err(history_error(
                "content reference operation identity was reused with different input",
            ));
        }
        Ok(true)
    }

    pub(crate) fn apply_content_reference_local(
        &mut self,
        request: &DfsContentReferenceRequest,
    ) -> Result<(), DfsError> {
        request.validate()?;
        if self
            .namespace_execution
            .context()
            .is_some_and(|context| context.file_system_id != request.identity.file_system_id)
            || self
                .namespace_execution
                .request()
                .is_some_and(|operation| operation.request_id != request.operation_id)
        {
            return Err(history_error(
                "file history request does not match its authenticated namespace envelope",
            ));
        }
        if self.content_reference_transaction_replayed(request)? {
            return Ok(());
        }
        validate_metadata_namespace(&request.namespace).map_err(history_error)?;
        let reference_id = request.operation.reference_id();
        let pin_id = content_reference_storage_id(&request.namespace, reference_id);
        if pin_id.trim().is_empty()
            || reference_id.len() > 256
            || pin_id.len() > 1024
            || pin_id.contains('\0')
            || request.operation_id.trim().is_empty()
            || request.operation_id.len() > 256
            || request.operation_id.contains('\0')
        {
            return Err(history_error(
                "file history requires bounded nonempty pin and operation identities",
            ));
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| history_error("file history requires durable metadata"))?;
        let object = backend
            .get_object(&request.identity.object_id)?
            .ok_or_else(|| history_error("file history object is unknown"))?;
        if object.file_system_id != request.identity.file_system_id
            || object.tombstone_provenance.is_some()
        {
            return Err(history_error(
                "file history routing path no longer identifies the requested object",
            ));
        }
        self.require_file_history_shard_support(&request.identity.file_system_id, &object.path)?;
        let backend = self
            .metadata_backend
            .as_ref()
            .expect("content reference metadata");
        let existing_pin = backend
            .version_pins_for_object(&request.identity.object_id)?
            .into_iter()
            .find(|pin| {
                pin.version == request.identity.version
                    && content_reference_record_id(pin) == pin_id
            });
        let record = backend.get_version(&request.identity.object_id, &request.identity.version)?;
        let mut version = record
            .ok_or_else(|| history_error("file version is unavailable or was not retained"))?;
        let previously_retained = version.has_retention_grant();
        let captured_at = version
            .retention_grant(&request.namespace)
            .map(|v| v.granted_at_unix_nanos)
            .unwrap_or_else(crate::metadata_records::dfs_now_unix_nanos);
        let newly_named_reference = existing_pin.is_none();
        let pin = existing_pin
            .clone()
            .unwrap_or_else(|| DfsMetadataVersionPinRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: version.version.clone(),
                namespace: request.namespace.clone(),
                pin_id: pin_id.clone(),
                created_at: { OffsetDateTime::now_utc() },
            });
        let capture_requires_current = matches!(
            request.operation,
            DfsContentReferenceOperation::CaptureCurrent { .. }
        ) && existing_pin.is_none();
        let mut pins = DfsFileVersionPinMutation::default();
        match &request.operation {
            DfsContentReferenceOperation::CaptureCurrent { .. } => {
                if existing_pin.is_none()
                    && (object.version != request.identity.version
                        || object.kind == DfsObjectKind::Tombstone)
                {
                    return Err(history_error("file changed before the requested capture"));
                }
                if existing_pin.is_none()
                    && !matches!(
                        object.kind,
                        DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
                    )
                {
                    return Err(history_error(
                        "only file content can be captured; directories have no content history",
                    ));
                }
                if version.retention_grant(&request.namespace).is_none() {
                    version
                        .retention
                        .entry(request.namespace.clone())
                        .or_default()
                        .grant = Some(Box::new(DfsRevisionRetentionGrant {
                        object: Box::new(object.clone()),
                        granted_at_unix_nanos: captured_at,
                        origin: DfsRetentionGrantOrigin::ExplicitOperation,
                        minimum: None,
                    }));
                    if let DfsContentReferenceOperation::CaptureCurrent {
                        metadata: Some(metadata),
                        ..
                    } = &request.operation
                    {
                        version
                            .module_metadata
                            .insert(request.namespace.clone(), metadata.clone());
                    } else {
                    }
                }
                pins.add.push(pin);
            }
            DfsContentReferenceOperation::Retain { .. } => {
                if !version.has_retention_grant() {
                    return Err(history_error(
                        "internal commit generations cannot be pinned as historical files; capture the current file explicitly",
                    ));
                }
                pins.add.push(pin);
            }
            DfsContentReferenceOperation::Release { .. } => {
                if let Some(pin) = existing_pin {
                    pins.remove.push(pin);
                } else {
                }
            }
        }
        if !pins.add.is_empty() && version.content_durability.is_none() {
            if previously_retained || !capture_requires_current {
                return Err(history_error(
                    "retained source lacks its required durability; storage conversion must complete it before current use",
                ));
            }
            version.content_durability =
                Some(self.establish_file_history_durability(&object, &version)?);
        }
        crate::file_history::validate_file_version_record(&version)?;
        if capture_requires_current
            && self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .get_object(&object.object_id)?
                .is_none_or(|current| {
                    current.version != request.identity.version
                        || current.kind == DfsObjectKind::Tombstone
                        || current.path != object.path
                })
        {
            return Err(history_error(
                "file changed while establishing capture durability",
            ));
        }
        // Reserve at the owner decision time after any content durability
        // work. This persisted time governs prepared/replayed liveness;
        // transaction creation time continues to govern idempotency eviction.
        if newly_named_reference {
            let decision_time = OffsetDateTime::now_utc();
            for reference in &mut pins.add {
                reference.created_at = decision_time;
            }
        }
        // A new pin operation is not as old as the content it protects.
        // Use durable request admission time, or the caller's stable ordered
        // identity for standalone execution. Never restamp an expired retry
        // with the current wall clock merely to get past a GC floor.
        let created_at = if let Some(admission) = self.namespace_execution.request() {
            admission.created_at
        } else if let Some(token) =
            crate::intent::stable_operation_order_token(&request.operation_id)
        {
            let id = sinomo_core::Id::new(token).expect("validated operation token");
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(id.timestamp_millis()) * 1_000_000)
                .map_err(|error| history_error(error.to_string()))?
        } else {
            // Legacy opaque identities retain their conservative expiry rule.
            OffsetDateTime::from_unix_timestamp_nanos(captured_at.into())
                .expect("u64 nanos is supported")
        };
        let no_op_release = matches!(
            request.operation,
            DfsContentReferenceOperation::Release { .. }
        ) && pins.add.is_empty()
            && pins.remove.is_empty();
        self.apply_prepared_retention_contribution(
            &request.namespace,
            DfsNamespaceTransaction {
                contribution_digest: content_reference_digest(request)?,
                content_reference_identity: Some(request.identity.clone()),
                transaction_id: format!("file-history:{}", request.operation_id),
                file_system_id: request.identity.file_system_id.clone(),
                objects: Vec::new(),
                versions: if no_op_release {
                    Vec::new()
                } else {
                    vec![version]
                },
                file_version_pins: Some(pins),
                advisory_lock_states: Vec::new(),
                extension_expectations: Vec::new(),
                created_at,
            },
        )
    }
}
