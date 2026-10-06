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
    if request.wire_format == DfsContentReferenceFormat::Legacy {
        return Ok(None);
    }
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
    /// current authoritative policy and journal it before its data prepare.
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
        let shards =
            self.namespace_shards_for_exact_paths(&object.file_system_id, [path.as_str()])?;
        drop(resolve_span);
        let _persist = tracing::trace_span!(target: "sinomo.perf", "dfs.history.refresh",
            stage = "dfs.history.refresh", phase = "ensure_backend_configuration")
        .entered();
        if let Some(backend) = self.metadata_backend.as_mut() {
            if let Some(shard) = shards.first() {
                backend.ensure_file_system_on_shard(
                    &DfsMetadataShardId::new(shard.shard_id.clone()),
                    &file_system,
                )?;
            } else {
                backend.ensure_file_system(&file_system)?;
            }
        }
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
        if shards.is_empty()
            || shards.iter().any(|shard| {
                !shard
                    .namespace_prepare_codec
                    .supports_retention_metadata_mutations()
            })
        {
            return Err(history_error(
                "retention mutations require a packed-v6 metadata shard; finish the capability-validated shard upgrade first",
            ));
        }
        // Participant capabilities are admitted atomically with the shard
        // configuration and revalidated by configuration transitions. Normal
        // operations use that installed codec contract; repeating member and
        // capability-policy KV reads here adds one barrier per record without
        // strengthening the operation-bound publication fence.
        Ok(())
    }

    /// sinomo:dfs-operation
    pub fn retain_content_revision(
        &mut self,
        mut request: DfsContentReferenceRequest,
    ) -> Result<DfsContentReferenceResponse, DfsError> {
        request.validate()?;
        if request.wire_format == DfsContentReferenceFormat::NamespacedV6 {
            request.path = crate::normalize_semantic_path(&request.path)
                .map_err(content_reference_posix_error)?;
            if let Some(response) = self.reconcile_content_reference(&request)? {
                self.reconcile_content_reference_maintenance(&response.identity)?;
                return Ok(response);
            }
        }
        let mut routing_path = request.path.clone();
        if request.wire_format == DfsContentReferenceFormat::NamespacedV6 {
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
                        return Err(history_error(reason));
                    }
                }
            }
        } else if matches!(
            request.operation,
            DfsContentReferenceOperation::CaptureCurrent { .. }
        ) {
            // Keep the original hint on a failed lookup: the durable request
            // receipt may still prove an exact retry after deletion or rename.
            if request.identity.file_system_id == sifs_file_system_id()
                && let Ok(object) = self.file_history_capture_object(&request.path)
                && object.object_id == request.identity.object_id
            {
                request.path = crate::metadata_keys::normalize_posix_path(&object.path);
                routing_path = request.path.clone();
            }
        } else if let Some(descriptor) = self.resolve_retained_revision(&request.identity)? {
            request.path =
                crate::metadata_keys::normalize_posix_path(&descriptor.current_object.path);
            routing_path = request.path.clone();
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
                ));
            };
            if shard.primary_node_id == self.node_id {
                self.execute_local_primary_namespace_operation_for_file_system(
                    request.identity.file_system_id.clone(),
                    request.operation_id.clone(),
                    operation,
                )
                .map_err(content_reference_posix_error)?;
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
                    return Err(history_error(reason));
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
        if normalized.wire_format == DfsContentReferenceFormat::NamespacedV6 {
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
        let recorded =
            if let Some(transaction) = backend.get_namespace_transaction(&transaction_id)? {
                Some((transaction.file_system_id, transaction.contribution_digest))
            } else {
                backend
                    .get_namespace_transaction_receipt(&transaction_id)?
                    .map(|receipt| (receipt.file_system_id, receipt.contribution_digest))
            };
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
        // Old commands retain their isolated legacy request/digest mapping.
        // Their transactions did not persist a logical contribution digest.
        let Some(recorded_digest) = recorded_digest else {
            return Ok(false);
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
        if request.wire_format == DfsContentReferenceFormat::Legacy
            && (request.namespace != crate::lifecycle::legacy_reference_namespace()
                || matches!(
                    &request.operation,
                    DfsContentReferenceOperation::CaptureCurrent {
                        metadata: Some(_),
                        ..
                    }
                ))
        {
            return Err(history_error(
                "legacy content reference has unencoded namespace or metadata",
            ));
        }
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
            || (request.wire_format == DfsContentReferenceFormat::Legacy
                && crate::metadata_keys::normalize_posix_path(&object.path) != request.path)
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
        version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
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
                namespace: Some(request.namespace.clone()),
                pin_id: pin_id.clone(),
                created_at: if request.wire_format == DfsContentReferenceFormat::NamespacedV6 {
                    OffsetDateTime::now_utc()
                } else {
                    OffsetDateTime::from_unix_timestamp_nanos(captured_at.into())
                        .expect("u64 nanos is supported")
                },
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
                    } else if request.wire_format == DfsContentReferenceFormat::Legacy {
                        version.module_metadata.insert(
                            request.namespace.clone(),
                            sinomo_api::dfs::file_history::legacy_checkpoint_reference_metadata(
                                &request.operation_id,
                            ),
                        );
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
                } else if request.wire_format == DfsContentReferenceFormat::Legacy {
                    return Ok(());
                }
            }
        }
        if !pins.add.is_empty() && version.content_durability.is_none() {
            version.content_durability =
                crate::content_durability::recover_from_backend(&version, backend.as_ref())?;
            if version.content_durability.is_none() {
                version.content_durability =
                    Some(self.establish_file_history_durability(&object, &version)?);
            }
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
        if newly_named_reference && request.wire_format == DfsContentReferenceFormat::NamespacedV6 {
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
        let no_op_release = request.wire_format == DfsContentReferenceFormat::NamespacedV6
            && matches!(
                request.operation,
                DfsContentReferenceOperation::Release { .. }
            )
            && pins.add.is_empty()
            && pins.remove.is_empty();
        self.apply_prepared_retention_contribution(
            &request.namespace,
            DfsNamespaceTransaction {
                contribution_digest: content_reference_digest(request)?,
                content_reference_identity: (request.wire_format
                    == DfsContentReferenceFormat::NamespacedV6)
                    .then(|| request.identity.clone()),
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
