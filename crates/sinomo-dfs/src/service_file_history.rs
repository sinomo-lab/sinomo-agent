//! Explicit file captures and pins use the existing namespace authority lane.

use crate::*;
use std::time::Duration;
use time::OffsetDateTime;

fn history_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn file_history_policy(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<DfsFileHistoryPolicy, DfsError> {
        let kv = self
            .control_plane
            .as_mut()
            .ok_or_else(|| history_error("retention settings require cluster configuration"))?;
        let config = DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(file_system_id)?
            .ok_or_else(|| history_error("filesystem configuration is missing"))?;
        Ok(config.file_history)
    }

    pub fn update_file_history_policy(
        &mut self,
        request: DfsFileHistoryPolicyUpdateRequest,
    ) -> Result<DfsFileHistoryPolicy, DfsError> {
        request.policy.validate().map_err(history_error)?;
        let kv = self
            .control_plane
            .as_mut()
            .ok_or_else(|| history_error("retention settings require cluster configuration"))?;
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
        for _ in 0..8 {
            let current = store
                .get_file_system_config(&request.file_system_id)?
                .ok_or_else(|| history_error("filesystem configuration is missing"))?;
            if current.file_history == request.policy {
                return Ok(current.file_history);
            }
            if current.file_history != request.expected {
                return Err(history_error(
                    "retention configuration changed; read the current policy before retrying",
                ));
            }
            let mut desired = current.clone();
            desired.file_history = request.policy.clone();
            if store.compare_and_put_file_system_config(Some(&current), &desired)? {
                self.metadata.add_file_system(DfsFileSystem::from(desired));
                return Ok(request.policy);
            }
        }
        Err(history_error(
            "retention configuration is busy; retry against its current value",
        ))
    }

    pub(crate) fn refresh_file_history_configuration(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>, DfsError> {
        if let Some(kv) = self.control_plane.as_mut()
            && let Some(config) =
                DfsControlPlaneStore::new(kv.as_mut()).get_file_system_config(file_system_id)?
        {
            let file_system = DfsFileSystem::from(config);
            self.metadata.add_file_system(file_system.clone());
            return Ok(Some(file_system));
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
        if !file_system.file_history.is_empty() {
            self.require_file_history_shard_support(&object.file_system_id, &path)?;
        }
        let shards =
            self.namespace_shards_for_exact_paths(&object.file_system_id, [path.as_str()])?;
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
                    .supports_file_history_mutations()
            })
        {
            return Err(history_error(
                "file history mutations require a history-capable packed-v3 metadata shard; finish the capability-validated shard upgrade first",
            ));
        }
        if let Some(control) = self.control_plane.as_mut() {
            let store = crate::DfsControlPlaneStore::new(control.as_mut());
            for shard in &shards {
                store.require_metadata_codec_capabilities(shard)?;
            }
        }
        Ok(())
    }

    pub fn file_history_pin(
        &mut self,
        mut request: DfsFileHistoryPinRequest,
    ) -> Result<DfsFileHistoryPinResponse, DfsError> {
        if matches!(
            request.operation,
            DfsFileHistoryPinOperation::Capture { .. }
        ) {
            // Keep the original hint on a failed lookup: the durable request
            // receipt may still prove an exact retry after deletion or rename.
            if request.identity.file_system_id == sifs_file_system_id()
                && let Ok(object) = self.file_history_capture_object(&request.path)
                && object.object_id == request.identity.object_id
            {
                request.path = crate::metadata_keys::normalize_posix_path(&object.path);
            }
        } else if let Some(descriptor) = self.resolve_file_history_version(&request.identity)? {
            request.path =
                crate::metadata_keys::normalize_posix_path(&descriptor.current_object.path);
        }
        self.require_file_history_shard_support(&request.identity.file_system_id, &request.path)?;
        let operation = DfsNamespaceOperation::FileHistoryPin {
            request: request.clone(),
        };
        let shards = self.namespace_shards_for_exact_paths(
            &request.identity.file_system_id,
            [request.path.as_str()],
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
                .map_err(|error| history_error(error.to_string()))?;
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
                .map_err(|error| history_error(error.to_string()))?;
        }
        Ok(DfsFileHistoryPinResponse {
            identity: request.identity,
        })
    }

    pub(crate) fn apply_file_history_pin_local(
        &mut self,
        request: &DfsFileHistoryPinRequest,
    ) -> Result<(), DfsError> {
        self.require_file_history_shard_support(&request.identity.file_system_id, &request.path)?;
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
        let pin_id = match &request.operation {
            DfsFileHistoryPinOperation::Capture { pin_id }
            | DfsFileHistoryPinOperation::Pin { pin_id }
            | DfsFileHistoryPinOperation::Unpin { pin_id } => pin_id,
        };
        if pin_id.trim().is_empty()
            || pin_id.len() > 256
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
            || crate::metadata_keys::normalize_posix_path(&object.path) != request.path
        {
            return Err(history_error(
                "file history routing path no longer identifies the requested object",
            ));
        }
        let existing_pin = backend
            .version_pins_for_object(&request.identity.object_id)?
            .into_iter()
            .find(|pin| pin.version == request.identity.version && pin.pin_id == *pin_id);
        let record = backend.get_version(&request.identity.object_id, &request.identity.version)?;
        let mut version = record
            .ok_or_else(|| history_error("file version is unavailable or was not retained"))?;
        let captured_at = version
            .retained
            .as_ref()
            .map(|v| v.captured_at_unix_nanos)
            .unwrap_or_else(crate::metadata_records::dfs_now_unix_nanos);
        let pin = existing_pin
            .clone()
            .unwrap_or_else(|| DfsMetadataVersionPinRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: version.version.clone(),
                pin_id: pin_id.clone(),
                created_at: OffsetDateTime::from_unix_timestamp_nanos(captured_at.into())
                    .expect("u64 nanos is supported"),
            });
        let capture_requires_current = matches!(
            request.operation,
            DfsFileHistoryPinOperation::Capture { .. }
        ) && existing_pin.is_none();
        let mut pins = DfsFileVersionPinMutation::default();
        match &request.operation {
            DfsFileHistoryPinOperation::Capture { .. } => {
                if existing_pin.is_none()
                    && (object.version != request.identity.version
                        || object.kind == DfsObjectKind::Tombstone)
                {
                    return Err(history_error("file changed before the requested capture"));
                }
                if version.retained.is_none() {
                    version.retained = Some(Box::new(DfsRetainedFileVersion {
                        object: Box::new(DfsObjectMetadata {
                            path: request.path.clone(),
                            ..object.clone()
                        }),
                        captured_at_unix_nanos: captured_at,
                        source: DfsFileVersionSource::Checkpoint {
                            operation_id: request.operation_id.clone(),
                        },
                        policy_capture: None,
                    }));
                }
                pins.add.push(pin);
            }
            DfsFileHistoryPinOperation::Pin { .. } => {
                if version.retained.is_none() {
                    return Err(history_error(
                        "internal commit generations cannot be pinned as historical files; capture the current file explicitly",
                    ));
                }
                pins.add.push(pin);
            }
            DfsFileHistoryPinOperation::Unpin { .. } => {
                let Some(pin) = existing_pin else {
                    return Ok(());
                };
                pins.remove.push(pin);
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
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: format!("file-history:{}", request.operation_id),
            file_system_id: request.identity.file_system_id.clone(),
            objects: Vec::new(),
            versions: vec![version],
            file_version_pins: Some(pins),
            advisory_lock_states: Vec::new(),
            created_at,
        })
    }
}
