//! Authoritative object lifecycle and operation context for filesystem extensions.
//!
//! Feature components use these owner-checked projections instead of treating
//! a path lookup, a disconnected replica, or a missing feature as GC authority.

use crate::*;

/// Evidence gathered from every current metadata authority for one identity.
/// An absent object is deliberately not reclaimable: absence is not deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsObjectLifecycle {
    pub object: Option<DfsObjectMetadata>,
    pub reclaimable: bool,
}

/// The low-level authority projection is shared by feature modules. These
/// aliases keep its existing wire representation during implementation moves.
pub type DfsContentReferenceRecord = DfsMetadataVersionPinRecord;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Inspect object identity and retention in the same authority snapshot.
    /// Unreachable authorities, migration fences and malformed evidence fail
    /// closed. Paths are returned as locators, never used as object identity.
    /// sinomo:dfs-operation
    pub fn object_lifecycle(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectLifecycle, DfsError> {
        let authorities = self.metadata_shard_authorities(file_system_id)?;
        let mut deleted = None;
        let mut retained = false;
        let mut fan_out = crate::file_history_query::DfsAuthorityFanOut::default();
        for shard in &authorities {
            let Some(result) = self.query_metadata_authority_in_fan_out(
                &mut fan_out,
                shard,
                DfsMetadataSelection::ObjectLifecycle {
                    object_id: object_id.clone(),
                },
                None,
            )?
            else {
                continue;
            };
            let DfsMetadataQueryResult::ObjectLifecycle {
                object,
                has_content_references,
                references_complete,
            } = result
            else {
                return Err(DfsError::Metadata {
                    message: "object lifecycle authority omitted its typed evidence".into(),
                });
            };
            retained |= has_content_references || !references_complete;
            let Some(object) = object else { continue };
            if object.object_id != *object_id
                || object.file_system_id != *file_system_id
                || object.tombstone_provenance.is_some()
            {
                return Err(DfsError::Metadata {
                    message: "foreign object lifecycle evidence".into(),
                });
            }
            if object.kind != DfsObjectKind::Tombstone {
                return Ok(DfsObjectLifecycle {
                    object: Some(*object),
                    reclaimable: false,
                });
            }
            deleted = Some(*object);
        }
        // Without a live record, a closed authority could hold the object or
        // a reference to it: reclaimability is undecided until it answers.
        fan_out.undecided(
            format!("object {object_id}"),
            DfsObjectLifecycle {
                reclaimable: deleted.is_some() && !retained,
                object: deleted,
            },
        )
    }

    /// sinomo:dfs-operation
    pub fn metadata_authorities(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if !self.is_distributed() {
            return Ok(Vec::new());
        }
        self.metadata_shard_authorities(file_system_id)
    }

    /// sinomo:dfs-operation
    pub fn query_metadata_authority(
        &mut self,
        shard: &DfsMetadataShardConfig,
        selection: DfsMetadataSelection,
    ) -> Result<DfsMetadataQueryResult, DfsError> {
        self.query_metadata_authority_impl(shard, selection)
    }

    /// Extension configuration uses the same explicit control-plane boundary
    /// as existing filesystem configuration administration. Normal data access
    /// continues to use the resident configuration projection.
    pub fn control_plane_client_mut(
        &mut self,
    ) -> Option<&mut (dyn DfsControlPlaneKvClient + 'static)> {
        self.control_plane.as_deref_mut()
    }

    pub fn operation_deadline(&self) -> Option<std::time::Instant> {
        self.transport.operation_deadline()
    }

    pub fn check_operation_cancellation(&self, operation: &'static str) -> Result<(), DfsError> {
        self.check_cancelled(operation)
    }

    pub fn check_operation_cancelled(&self, operation: &'static str) -> Result<(), DfsError> {
        self.check_operation_cancellation(operation)
    }

    /// sinomo:dfs-operation
    pub fn refresh_file_system_configuration(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>, DfsError> {
        self.refresh_file_history_configuration(file_system_id)
    }

    pub fn current_content_object(&mut self, path: &str) -> Result<DfsObjectMetadata, DfsError> {
        self.file_history_capture_object(path)
            .map_err(|error| DfsError::Metadata {
                message: error.to_string(),
            })
    }

    /// sinomo:dfs-operation
    pub fn require_retention_shard_support(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<(), DfsError> {
        self.require_file_history_shard_support(file_system_id, path)
    }

    /// sinomo:dfs-operation
    pub fn read_content_revision_range(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadOutcome, DfsError> {
        if max_bytes == 0 || max_bytes > 64 * 1024 {
            return Err(DfsError::Metadata {
                message: "content revision ranges require 1..=65536 bytes".into(),
            });
        }
        let identity = DfsFileVersionIdentity {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
        };
        let current = self.resolve_file_version_current_object(&identity)?;
        let authorized = if let Some(current) = current.filter(|current| {
            current.version == identity.version && current.kind != DfsObjectKind::Tombstone
        }) {
            current
        } else {
            let descriptor =
                self.resolve_retained_revision(&identity)?
                    .ok_or_else(|| DfsError::Metadata {
                        message:
                            "content revision is neither current nor protected by a live grant"
                                .into(),
                    })?;
            crate::file_history_query::retained_revision_content_object(&descriptor)?
        };
        if authorized.content_hash != object.content_hash
            || authorized.content_size_bytes != object.content_size_bytes
        {
            return Err(DfsError::Metadata {
                message: "content revision identity differs from authoritative metadata".into(),
            });
        }
        self.read_immutable_object_metadata_range_or_fetch_remote_without_cache(
            &authorized,
            offset,
            max_bytes,
        )
        .map_err(|error| DfsError::Metadata {
            message: error.to_string(),
        })
    }

    pub fn reserved_content_destination_path(
        &mut self,
        request: &DfsContentCopyRequest,
    ) -> Result<String, DfsPosixError> {
        self.content_copy_destination_path(request)
    }

    pub fn begin_reserved_content_job(
        &mut self,
        path: &str,
        request: &DfsContentCopyRequest,
        options: &DfsWriteOptions,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        self.begin_content_copy_job(path, request, options)
    }

    pub fn resume_namespace_job_bounded(
        &mut self,
        job_id: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        self.posix_resume_namespace_job_bounded(job_id, max_steps)
            .map_err(|error| DfsError::Metadata {
                message: error.to_string(),
            })
    }

    pub fn is_distributed(&self) -> bool {
        self.control_plane.is_some()
    }
}

/// Canonical namespace normalization shared by semantic feature modules.
pub fn normalize_semantic_path(path: &str) -> Result<String, DfsPosixError> {
    crate::posix_path::normalize_posix_path(path)
}

pub fn normalize_semantic_path_lossy(path: &str) -> String {
    crate::metadata_keys::normalize_posix_path(path)
}

/// A feature contributes declarative data, never code to run inside a storage
/// lock. The core commits the contribution through its canonical namespace
/// transaction and authority/publication machinery.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsAtomicContribution {
    pub namespace: String,
    pub file_system_id: DfsFileSystemId,
    pub operation_id: String,
    /// UTC millisecond timestamp encoded by the generated operation ULID.
    /// Retrying the same ID cannot acquire a fresh retention window.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    /// Current object heads observed when this immutable request was authored.
    /// These are independent of the historical revisions being annotated.
    pub expected_heads: Vec<DfsContributionHeadExpectation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<DfsContributionAuthorityExpectation>,
    pub updates: Vec<DfsRevisionMetadataContribution>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsContributionHeadExpectation {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsContributionAuthorityExpectation {
    pub shard_id: DfsMetadataShardId,
    pub primary_node_id: NodeId,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsRevisionMetadataContribution {
    pub identity: DfsRevisionReference,
    #[serde(default)]
    pub expected_metadata: Option<DfsModuleMetadataValue>,
    #[serde(default)]
    pub metadata: Option<DfsModuleMetadataValue>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsContentReferenceOperation {
    CaptureCurrent {
        reference_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<DfsModuleMetadataValue>,
    },
    Retain {
        reference_id: String,
    },
    Release {
        reference_id: String,
    },
}

impl DfsContentReferenceOperation {
    pub fn reference_id(&self) -> &str {
        match self {
            Self::CaptureCurrent { reference_id, .. }
            | Self::Retain { reference_id }
            | Self::Release { reference_id } => reference_id,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsContentReferenceRequest {
    pub namespace: String,
    pub path: String,
    pub identity: DfsRevisionReference,
    pub operation: DfsContentReferenceOperation,
    pub operation_id: String,
}

impl DfsContentReferenceRequest {
    pub fn validate(&self) -> Result<(), DfsError> {
        let invalid = |message: String| DfsError::Metadata { message };
        validate_metadata_namespace(&self.namespace).map_err(invalid)?;
        validate_write_operation_id(&self.operation_id)?;
        let key = self.operation.reference_id();
        if key.trim().is_empty()
            || key.len() > 256
            || key.contains('\0')
            || self.path.len() > 4096
            || self.path.contains('\0')
        {
            return Err(invalid(
                "content reference requires a bounded locator and reference identity".into(),
            ));
        }

        if let DfsContentReferenceOperation::CaptureCurrent {
            metadata: Some(metadata),
            ..
        } = &self.operation
        {
            validate_module_metadata_map(&std::collections::BTreeMap::from([(
                self.namespace.clone(),
                metadata.clone(),
            )]))
            .map_err(invalid)?;
        }
        Ok(())
    }
}

pub fn content_reference_storage_id(namespace: &str, reference_id: &str) -> String {
    format!("ref/{namespace}/{reference_id}")
}

pub fn content_reference_record_id(record: &DfsContentReferenceRecord) -> String {
    record.pin_id.clone()
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsContentReferenceResponse {
    pub identity: DfsRevisionReference,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// sinomo:dfs-operation
    pub fn apply_atomic_contribution(
        &mut self,
        contribution: DfsAtomicContribution,
    ) -> Result<(), DfsError> {
        validate_write_operation_id(&contribution.operation_id)?;
        let invalid = |message: &str| DfsError::Metadata {
            message: message.into(),
        };
        let namespace = &contribution.namespace;
        validate_metadata_namespace(namespace).map_err(|error| invalid(&error))?;
        let operation_id = sinomo_core::Id::new(&contribution.operation_id).map_err(|error| {
            invalid(&format!(
                "contributions require a generated operation ULID: {error}"
            ))
        })?;
        let created_at = time::OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(operation_id.timestamp_millis()) * 1_000_000,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        if contribution.created_at != created_at
            || contribution.created_at.offset() != time::UtcOffset::UTC
        {
            return Err(invalid(
                "contribution timestamp differs from its immutable operation identity",
            ));
        }
        if contribution.updates.is_empty() || contribution.updates.len() > 128 {
            return Err(invalid("metadata contribution requires 1..=128 updates"));
        }
        let bytes =
            serde_json::to_vec(&contribution).map_err(|error| invalid(&error.to_string()))?;
        if bytes.len() > 1024 * 1024 {
            return Err(invalid("metadata contribution exceeds its byte bound"));
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| invalid("contributions require durable metadata"))?;
        let contribution_digest = blake3::hash(&bytes).to_hex().to_string();
        let transaction_id = format!(
            "metadata-contribution:{namespace}:{}",
            contribution.operation_id
        );
        if let Some(receipt) = backend.get_namespace_transaction_receipt(&transaction_id)? {
            if receipt.contribution_digest.as_deref() == Some(contribution_digest.as_str()) {
                return Ok(());
            }
            return Err(invalid(
                "contribution operation identity was reused with different input",
            ));
        }
        let update_objects = contribution
            .updates
            .iter()
            .map(|update| update.identity.object_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut head_objects = std::collections::BTreeSet::new();
        for head in &contribution.expected_heads {
            if !head_objects.insert(head.object_id.clone())
                || normalize_semantic_path(&head.path)
                    .map_err(|error| invalid(&error.to_string()))?
                    != head.path
            {
                return Err(invalid(
                    "contribution contains duplicate or noncanonical head expectations",
                ));
            }
        }
        if head_objects != update_objects {
            return Err(invalid(
                "contribution requires the original current head of every updated object",
            ));
        }
        let selected_shard = if self.is_distributed() {
            let expected = contribution.authority.as_ref().ok_or_else(|| {
                invalid("distributed contributions require their original authority expectation")
            })?;
            let shards = self.namespace_shards_for_exact_paths(
                &contribution.file_system_id,
                contribution
                    .expected_heads
                    .iter()
                    .map(|head| head.path.as_str()),
            )?;
            let [shard] = shards.as_slice() else {
                return Err(invalid(
                    "metadata contributions require exactly one current authority",
                ));
            };
            if shard.shard_id != expected.shard_id.0
                || shard.primary_node_id != expected.primary_node_id
                || shard.epoch != expected.owner_epoch
                || shard.primary_node_id != self.node_id
            {
                return Err(invalid(
                    "contribution authority changed without an exact durable outcome",
                ));
            }
            Some(shard.clone())
        } else {
            if contribution.authority.is_some() {
                return Err(invalid(
                    "a distributed contribution cannot acquire a new local authority",
                ));
            }
            None
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .expect("backend checked above");
        for head in &contribution.expected_heads {
            let current = match &selected_shard {
                Some(shard) => backend.get_object_on_shard(
                    &DfsMetadataShardId::new(&shard.shard_id),
                    &head.object_id,
                )?,
                None => backend.get_object(&head.object_id)?,
            };
            if current.as_ref().is_none_or(|current| {
                current.file_system_id != contribution.file_system_id
                    || current.object_id != head.object_id
                    || current.version != head.version
                    || current.path != head.path
            }) {
                return Err(invalid(
                    "contribution source head changed without an exact durable outcome",
                ));
            }
        }
        let mut versions = Vec::new();
        let mut expectations = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for update in contribution.updates {
            if update.identity.file_system_id != contribution.file_system_id
                || !seen.insert((
                    update.identity.object_id.clone(),
                    update.identity.version.clone(),
                ))
            {
                return Err(invalid("contribution has foreign or duplicate revisions"));
            }
            let mut version = backend
                .get_version(&update.identity.object_id, &update.identity.version)?
                .ok_or_else(|| invalid("contribution revision is unavailable"))?;
            if version.file_system_id != contribution.file_system_id
                || version.module_metadata.get(namespace) != update.expected_metadata.as_ref()
            {
                return Err(invalid("contribution namespace metadata changed"));
            }
            expectations.push(DfsRevisionMetadataExpectation {
                object_id: version.object_id.clone(),
                version: version.version.clone(),
                expected: Some(Box::new(version.clone())),
            });
            match update.metadata {
                Some(metadata) => {
                    version.module_metadata.insert(namespace.clone(), metadata);
                }
                None => {
                    version.module_metadata.remove(namespace);
                }
            }
            version
                .validate_extension_metadata()
                .map_err(|error| invalid(&error))?;
            versions.push(version);
        }
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            transaction_id,
            file_system_id: contribution.file_system_id,
            objects: Vec::new(),
            versions,
            file_version_pins: None,
            advisory_lock_states: Vec::new(),
            extension_expectations: expectations,
            contribution_digest: Some(contribution_digest),
            created_at,
        };
        let prepared_bytes =
            serde_json::to_vec(&transaction).map_err(|error| invalid(&error.to_string()))?;
        if prepared_bytes.len() > 1024 * 1024 {
            return Err(invalid(
                "prepared metadata contribution exceeds its byte bound",
            ));
        }
        self.apply_atomic_metadata_transaction(
            transaction,
            selected_shard,
            &contribution.expected_heads,
        )
    }

    /// Core-authored reference and grant plans retain immutable fields. Exact
    /// row expectations are checked again under the authoritative writer lock.
    pub(crate) fn apply_prepared_retention_contribution(
        &mut self,
        namespace: &str,
        mut transaction: DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        let invalid = |message: &str| DfsError::Metadata {
            message: message.into(),
        };
        if !transaction.objects.is_empty() || !transaction.advisory_lock_states.is_empty() {
            return Err(invalid(
                "retention contributions cannot change namespace objects or locks",
            ));
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| invalid("contributions require durable metadata"))?;
        for proposed in &transaction.versions {
            proposed
                .validate_extension_metadata()
                .map_err(|error| invalid(&error))?;
            let existing = backend.get_version(&proposed.object_id, &proposed.version)?;
            if let Some(existing) = &existing {
                if proposed.file_system_id != existing.file_system_id
                    || proposed.object_id != existing.object_id
                    || proposed.version != existing.version
                    || proposed.content_hash != existing.content_hash
                    || proposed.size_bytes != existing.size_bytes
                    || proposed.locations != existing.locations
                    || proposed.created_by_event_id != existing.created_by_event_id
                    || proposed.created_at != existing.created_at
                    || proposed.visible != existing.visible
                {
                    return Err(invalid(
                        "retention contribution changed immutable revision fields",
                    ));
                }
                for (owner, value) in &existing.retention {
                    if owner != namespace && proposed.retention.get(owner) != Some(value) {
                        return Err(invalid("foreign namespace retention changed"));
                    }
                }
                for (owner, value) in &proposed.retention {
                    if owner != namespace && existing.retention.get(owner) != Some(value) {
                        return Err(invalid("foreign namespace retention added"));
                    }
                }
                for (owner, value) in &existing.module_metadata {
                    if owner != namespace && proposed.module_metadata.get(owner) != Some(value) {
                        return Err(invalid("foreign module metadata changed"));
                    }
                }
                for (owner, value) in &proposed.module_metadata {
                    if owner != namespace && existing.module_metadata.get(owner) != Some(value) {
                        return Err(invalid("foreign module metadata added"));
                    }
                }
            }
            transaction
                .extension_expectations
                .push(DfsRevisionMetadataExpectation {
                    object_id: proposed.object_id.clone(),
                    version: proposed.version.clone(),
                    expected: existing.map(Box::new),
                });
        }
        if let Some(references) = &transaction.file_version_pins {
            for reference in references.add.iter().chain(&references.remove) {
                if reference.file_system_id != transaction.file_system_id
                    || !content_reference_record_id(reference)
                        .starts_with(&format!("ref/{namespace}/"))
                {
                    return Err(invalid("foreign namespace content reference"));
                }
            }
        }
        self.apply_namespace_transaction(transaction)
    }

    pub fn content_reference_present(
        &mut self,
        request: &DfsContentReferenceRequest,
    ) -> Result<bool, DfsError> {
        request.validate()?;
        let id = content_reference_storage_id(&request.namespace, request.operation.reference_id());
        let mut fan_out = crate::file_history_query::DfsAuthorityFanOut::default();
        for shard in self.metadata_shard_authorities(&request.identity.file_system_id)? {
            let Some(result) = self.query_metadata_authority_in_fan_out(
                &mut fan_out,
                &shard,
                DfsMetadataSelection::VersionPin {
                    identity: request.identity.clone(),
                    pin_id: id.clone(),
                },
                None,
            )?
            else {
                continue;
            };
            match result {
                DfsMetadataQueryResult::VersionPin {
                    identity,
                    pin_id,
                    present,
                } if identity == request.identity && pin_id == id => {
                    if present {
                        return Ok(true);
                    }
                }
                _ => {
                    return Err(DfsError::Metadata {
                        message: "content reference authority returned mismatched evidence".into(),
                    });
                }
            }
        }
        fan_out.undecided(format!("object {}", request.identity.object_id), false)
    }
}
