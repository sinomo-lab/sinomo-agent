//! Explicit local source ownership. A missing cluster client never mints this
//! capability; the caller must choose this factory and retain normal ownership.
use super::*;
use crate::metadata::{
    StandaloneMetadataImportCertificate, StandaloneMetadataImportPlan,
    StandaloneMetadataImportProposal, StandaloneSourceOwnerEvidence,
};

pub(crate) struct StandaloneStorageImportCapability {
    binding: String,
    data_root: PathBuf,
    source_file_identity: (u64, u64),
    owner_evidence: Vec<StandaloneSourceOwnerEvidence>,
    _data_owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    _source_fence: Arc<std::fs::File>,
}
impl StandaloneStorageImportCapability {
    pub(crate) fn binding(&self) -> &str {
        &self.binding
    }
    pub(crate) fn validate_import(
        &self,
        proposal: &StandaloneMetadataImportProposal,
        actual_source_root: &Path,
        actual_image_root: &Path,
    ) -> Result<()> {
        let source = actual_source_root.canonicalize().map_err(error)?;
        let image = actual_image_root.canonicalize().map_err(error)?;
        if !source.starts_with(&self.data_root)
            || !image.starts_with(&self.data_root)
            || proposal.owner_binding != self.binding
            || proposal.source_owner_evidence != self.owner_evidence
            || file_identity(
                &std::fs::File::open(source.join("journal/write.lock")).map_err(error)?,
            )? != self.source_file_identity
            || file_identity(&self._source_fence)? != self.source_file_identity
        {
            return Err(error(
                "standalone import differs from its actual retained owner/session scope",
            ));
        }
        crate::metadata::validate_retained_source_identity(
            &source,
            &proposal.source_identity,
            &proposal.source_snapshot_fingerprint,
        )?;
        let actual = crate::metadata::inspect_import_candidate(&image)?;
        if actual.state_fingerprint != proposal.current_image_fingerprint
            || Some(&actual.prefix_proof) != proposal.target_exact_prefix.as_ref()
        {
            return Err(error(
                "standalone current image differs from its exact owned proposal",
            ));
        }
        Ok(())
    }
}

pub(crate) struct ExplicitStandaloneSourceFactory {
    data_root: PathBuf,
    runtime_data_dir: sinomo_core::local::SinomoDataDir,
    content_root: PathBuf,
    node_id: sinomo_core::node::NodeId,
    data_owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    owner_file_identity: (u64, u64),
    factory_role: &'static str,
}
impl ExplicitStandaloneSourceFactory {
    /// The explicit factory selection is separate from default native import.
    /// Actual ownership and principal checks are still required for each source.
    pub(crate) fn from_context(context: &sinomo_module::ComponentContext) -> Result<Self> {
        let factory_role = match context.host_role() {
            sinomo_module::ComponentHostRole::Main => "main",
            sinomo_module::ComponentHostRole::Embedded => "embedded",
            sinomo_module::ComponentHostRole::Shadow => {
                return Err(error("shadow cannot mint standalone source ownership"));
            }
        };
        if context.cluster_access().is_some()
            || sinomo_dfs::runtime_host::registered_component_cluster(context)
                .map_err(error)?
                .is_some()
        {
            return Err(error(
                "a native cluster context, including native q1, requires native import authority",
            ));
        }
        let data_owner =
            sinomo_dfs::runtime_host::retain_component_data_owner(context).map_err(error)?;
        let data_root = context.data_dir_root().canonicalize().map_err(error)?;
        let node_id = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(context.data_dir()),
        )
        .map_err(error)?
        .local_node_id;
        let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
            context.data_dir(),
            context.host_role(),
        );
        let owner_file_identity = data_owner.storage_owner_file_identity().map_err(error)?;
        if file_identity(&std::fs::File::open(paths.data_owner_lock).map_err(error)?)?
            != owner_file_identity
        {
            return Err(error(
                "standalone factory owner path does not identify its actual held lease",
            ));
        }
        Ok(Self {
            data_root,
            runtime_data_dir: context.data_dir().clone(),
            content_root: context.data_dir().dfs_dir().join("content"),
            node_id,
            data_owner,
            owner_file_identity,
            factory_role,
        })
    }
    pub(crate) fn binding_for_source(
        &self,
        source: &MetadataSourceCut,
        evidence: &[StandaloneSourceOwnerEvidence],
        actual_source_root: &Path,
    ) -> Result<String> {
        let root = actual_source_root.canonicalize().map_err(error)?;
        let ids = evidence
            .iter()
            .map(|e| e.file_system_id.clone())
            .collect::<BTreeSet<_>>();
        if !root.starts_with(&self.data_root)
            || evidence.is_empty()
            || ids != source.file_system_ids.iter().cloned().collect()
            || evidence.iter().any(|e| {
                e.owner_epoch != 0
                    || e.owner_node_id != self.node_id
                    || e.shard_id != source.scope_shard
            })
        {
            return Err(error(
                "explicit source binding has no actual epoch0 principal ownership",
            ));
        }
        let fence = crate::metadata::retain_source_fence(&root)?;
        crate::metadata::validate_standalone_source_owner_evidence(&root, evidence, &self.node_id)?;
        crate::metadata::validate_retained_source_identity(
            &root,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
        )?;
        digest(&(
            "explicit-standalone-storage-source-v1",
            &self.data_root,
            self.factory_role,
            &self.node_id,
            self.owner_file_identity,
            file_identity(&fence)?,
            evidence,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
        ))
    }
    pub(crate) fn mint(
        &self,
        proposal: &StandaloneMetadataImportProposal,
        actual_source_root: &Path,
        actual_image_root: &Path,
    ) -> Result<Arc<StandaloneStorageImportCapability>> {
        let root = actual_source_root.canonicalize().map_err(error)?;
        if !root.starts_with(&self.data_root) {
            return Err(error(
                "standalone source lies outside its actual data owner",
            ));
        }
        let scope_ids = proposal
            .source_owner_evidence
            .iter()
            .map(|e| e.file_system_id.clone())
            .collect::<BTreeSet<_>>();
        if proposal.source_owner_evidence.is_empty()
            || scope_ids != proposal.file_system_ids.iter().cloned().collect()
            || proposal.source_owner_evidence.iter().any(|e| {
                e.owner_epoch != 0
                    || e.owner_node_id != self.node_id
                    || e.shard_id != proposal.scope_shard
            })
        {
            return Err(error(
                "explicit standalone source requires actual epoch0 ownership by the factory principal for every filesystem",
            ));
        }
        // Re-read original source evidence under the actual source writer fence;
        // serialized blueprint claims cannot grant local ownership.
        let source_fence = crate::metadata::retain_source_fence(&root)?;
        crate::metadata::validate_standalone_source_owner_evidence(
            &root,
            &proposal.source_owner_evidence,
            &self.node_id,
        )?;
        crate::metadata::validate_retained_source_identity(
            &root,
            &proposal.source_identity,
            &proposal.source_snapshot_fingerprint,
        )?;
        let source_file_identity = file_identity(&source_fence)?;
        let binding = digest(&(
            "explicit-standalone-storage-source-v1",
            &self.data_root,
            self.factory_role,
            &self.node_id,
            self.owner_file_identity,
            source_file_identity,
            &proposal.source_owner_evidence,
            &proposal.source_identity,
            &proposal.source_snapshot_fingerprint,
        ))?;
        if proposal.owner_binding != binding {
            return Err(error(
                "standalone proposal lacks this actual factory/source ownership binding",
            ));
        }
        let capability = Arc::new(StandaloneStorageImportCapability {
            binding,
            data_root: self.data_root.clone(),
            source_file_identity,
            owner_evidence: proposal.source_owner_evidence.clone(),
            _data_owner: Arc::clone(&self.data_owner),
            _source_fence: source_fence,
        });
        capability.validate_import(proposal, &root, actual_image_root)?;
        Ok(capability)
    }
}

struct StandaloneImportAuthority {
    factory: ExplicitStandaloneSourceFactory,
}
impl StandaloneImportAuthority {
    fn source_evidence(
        &self,
        source: &MetadataSourceCut,
        actual_source_root: &Path,
    ) -> Result<Vec<StandaloneSourceOwnerEvidence>> {
        let evidence = source
            .file_system_ids
            .iter()
            .map(|file_system_id| StandaloneSourceOwnerEvidence {
                file_system_id: file_system_id.clone(),
                shard_id: source.scope_shard.clone(),
                owner_node_id: self.factory.node_id.clone(),
                owner_epoch: 0,
            })
            .collect::<Vec<_>>();
        // These are proposed bindings, not authority: the decoder checks each
        // one against the actual retained source under its writer fence.
        self.factory
            .binding_for_source(source, &evidence, actual_source_root)?;
        Ok(evidence)
    }

    fn certify_at(
        &self,
        proposal: &ImportProposal,
        actual_source_root: &Path,
        actual_image_root: &Path,
    ) -> Result<ImportCertificate> {
        let ImportProposal::Standalone(proposal) = proposal else {
            return Err(error("explicit local owner cannot certify native import"));
        };
        if !proposal.expected_assignments.is_empty()
            || !proposal.source_cut.projection_assignments.is_empty()
        {
            return Err(error("standalone import contains native assignments"));
        }
        let owner = self
            .factory
            .mint(proposal, actual_source_root, actual_image_root)?;
        let prefix = proposal
            .target_exact_prefix
            .clone()
            .ok_or_else(|| error("standalone import lacks its exact native target prefix"))?;
        // Source heights never stand in for source ancestry. Preserve and prove
        // every actual acknowledged source floor before installing the image.
        for file_system_id in &proposal.file_system_ids {
            let floors = crate::metadata::ack_floor::acknowledged_source_floors_for_root(
                actual_source_root,
                file_system_id,
                proposal.source_cut.source_format_version,
                &proposal.source_cut.scope_shard,
            )?;
            let mut reader = crate::metadata::source_ancestry::source_ancestry_reader(
                &proposal.source_cut,
                actual_source_root,
                &floors,
                &[],
            )?;
            while reader
                .next_chunk(crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES)?
                .is_some()
            {}
        }
        Ok(ImportCertificate::Standalone(
            StandaloneMetadataImportCertificate {
                facts: MetadataImportCertificateFacts {
                    proposal_fingerprint: digest(&ImportProposal::Standalone(proposal.clone()))?,
                    current_image_fingerprint: proposal.current_image_fingerprint.clone(),
                    target_exact_prefix: prefix,
                },
                owner,
            },
        ))
    }
}

impl MetadataImportAuthority for StandaloneImportAuthority {
    fn queue_original_topology_after_install(
        &self,
        source: &MetadataSourceCut,
        _certificate: &ImportCertificate,
        _installed_root: &Path,
    ) -> Result<()> {
        if source.original_handoff.is_some() {
            return Err(error(
                "explicit standalone import cannot schedule native original topology",
            ));
        }
        Ok(())
    }
    fn queue_published_original_topology(
        &self,
        source: &MetadataSourceCut,
        _installed_root: &Path,
    ) -> Result<()> {
        if source.original_handoff.is_some() {
            return Err(error(
                "explicit standalone image cannot restore native original topology",
            ));
        }
        Ok(())
    }
    fn original_handoff_reservation(
        &self,
        _shard: &sinomo_dfs::DfsMetadataShardId,
        _file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Option<crate::metadata::OriginalMetadataHandoffReservation>> {
        // This adapter exists only after the explicit standalone factory has
        // validated actual owner/session custody, never from absent native KV.
        Ok(None)
    }

    fn resume_imported_namespace_jobs(
        &self,
        source: &MetadataSourceCut,
        certificate: &ImportCertificate,
        installed_root: &Path,
        jobs: &[sinomo_dfs::DfsNamespaceJobRecord],
    ) -> Result<()> {
        let ImportCertificate::Standalone(certificate) = certificate else {
            return Err(error("explicit local job wake received native authority"));
        };
        if !Arc::ptr_eq(&certificate.owner._data_owner, &self.factory.data_owner)
            || jobs != crate::metadata::imported_namespace_jobs(source)?
        {
            return Err(error(
                "local job wake differs from actual captured source ownership",
            ));
        }
        if jobs.is_empty() {
            return Ok(());
        }
        // Explicit local import preserves the entire physical source. Its jobs
        // are already native DATA in that exact certified image, never copied
        // into another coordinator's root by this local-owner capability.
        let image = sinomo_dfs::read_metadata_candidate(installed_root)?;
        if image.semantic_state_fingerprint != certificate.current_image_fingerprint
            || image.prefix_proof != certificate.target_exact_prefix
            || jobs
                .iter()
                .any(|job| !image.tables.namespace_jobs.contains(job))
        {
            return Err(error(
                "local job DATA is absent from its exact installed image",
            ));
        }
        let data_dir = self.factory.runtime_data_dir.clone();
        sinomo_dfs::runtime_host::wake_namespace_reconciliation(&data_dir, &self.factory.data_owner)
            .map_err(error)
    }
    fn kind(&self) -> crate::metadata::MetadataImportAuthorityKind {
        crate::metadata::MetadataImportAuthorityKind::Standalone
    }
    fn validate_owned_source(
        &self,
        root: &Path,
        scope: &sinomo_dfs::DfsMetadataShardId,
        file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<()> {
        let root = root.canonicalize().map_err(error)?;
        if !root.starts_with(&self.factory.data_root) {
            return Err(error("standalone source lies outside its actual owner"));
        }
        let _fence = crate::metadata::retain_source_fence(&root)?;
        let evidence = file_system_ids
            .iter()
            .map(|file_system_id| StandaloneSourceOwnerEvidence {
                file_system_id: file_system_id.clone(),
                shard_id: scope.clone(),
                owner_node_id: self.factory.node_id.clone(),
                owner_epoch: 0,
            })
            .collect::<Vec<_>>();
        crate::metadata::validate_standalone_source_owner_evidence(
            &root,
            &evidence,
            &self.factory.node_id,
        )
    }
    fn local_node_id(&self) -> Result<sinomo_core::node::NodeId> {
        Ok(self.factory.node_id.clone())
    }
    fn pending_original_source_scopes(
        &self,
        source: &MetadataSourceCut,
    ) -> Result<Vec<(sinomo_dfs::DfsFileSystemId, sinomo_dfs::DfsMetadataShardId)>> {
        self.validate_owned_source(
            &source.source_root,
            &source.scope_shard,
            &source.file_system_ids,
        )?;
        Ok(Vec::new())
    }
    fn fulfill_retained_content(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
        locator: &crate::metadata::MetadataSourceContentLocator,
        policy: sinomo_dfs::DfsReplicationPolicy,
        targets: Vec<sinomo_core::node::NodeId>,
    ) -> Result<super::content_work::MetadataImportedRetainedContent> {
        let _source_fence = crate::metadata::retain_source_fence(&source.source_root)?;
        self.validate_owned_source(
            &source.source_root,
            &source.scope_shard,
            &source.file_system_ids,
        )?;
        crate::metadata::validate_retained_source_identity(
            &source.source_root,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
        )?;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || !source.file_system_ids.contains(&version.file_system_id)
            || !matches!(
                object.kind,
                sinomo_dfs::DfsObjectKind::File
                    | sinomo_dfs::DfsObjectKind::Artifact
                    | sinomo_dfs::DfsObjectKind::Symlink
                    | sinomo_dfs::DfsObjectKind::Tombstone
            )
            || targets != vec![self.factory.node_id.clone()]
            || policy.min_commit_replicas != 1
            || policy.desired_replicas != 1
        {
            return Err(error(
                "explicit standalone content work requires exact source identity and its actual W1/N1 owner",
            ));
        }
        let evidence = crate::metadata::lookup_owned_metadata_source_version(
            source,
            &version.file_system_id,
            &version.object_id,
            &version.version,
        )?
        .ok_or_else(|| error("standalone retained work lacks its actual original member"))?;
        if evidence.object != *object
            || evidence.locator != *locator
            || evidence.version.content_hash != version.content_hash
            || evidence.version.size_bytes != version.size_bytes
        {
            return Err(error(
                "standalone retained work changes its actual source witness",
            ));
        }
        let content =
            sinomo_dfs::LocalContentStore::new(self.factory.content_root.clone()).map_err(error)?;
        super::content_work::verify_local_retained_content(&content, version, locator)?;
        Ok(super::content_work::MetadataImportedRetainedContent {
            durability: sinomo_dfs::DfsContentDurability {
                source_version: version.version.clone(),
                coordinator_node_id: self.factory.node_id.clone(),
                owner_epoch: 0,
                desired_replica_node_ids: targets,
                policy,
            },
            receipts: vec![sinomo_dfs::DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: version.file_system_id.clone(),
                    object_id: version.object_id.clone(),
                    committed_version: version.version.clone(),
                    owner_node_id: self.factory.node_id.clone(),
                    event_id: sinomo_core::events::EventId::generate(),
                },
                replica_node_id: self.factory.node_id.clone(),
                content_hash: version.content_hash.clone(),
                received_at: time::OffsetDateTime::now_utc(),
            }],
        })
    }
    fn source_policy_candidate_count(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
    ) -> Result<usize> {
        let root = source.source_root.canonicalize().map_err(error)?;
        if !root.starts_with(&self.factory.data_root)
            || self
                .factory
                .data_owner
                .storage_owner_file_identity()
                .map_err(error)?
                != self.factory.owner_file_identity
            || !source.projection_assignments.is_empty()
        {
            return Err(error(
                "source policy context lacks its explicit held standalone owner",
            ));
        }
        let _source_fence = crate::metadata::retain_source_fence(&root)?;
        crate::metadata::validate_retained_source_identity(
            &root,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
        )?;
        // This is the factory's actual sole candidate context, not permission
        // to publish. Decoded source principal/epoch checks still gate work and install.
        Ok(1)
    }
    fn current_retained_content_placement(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
    ) -> Result<(
        sinomo_dfs::DfsReplicationPolicy,
        Vec<sinomo_core::node::NodeId>,
    )> {
        self.validate_owned_source(
            &source.source_root,
            &source.scope_shard,
            &source.file_system_ids,
        )?;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || object.owner_node_id != self.factory.node_id
        {
            return Err(error(
                "explicit local import placement lacks the actual source owner",
            ));
        }
        Ok((
            sinomo_dfs::DfsReplicationPolicy::new(1, 1).map_err(error)?,
            vec![self.factory.node_id.clone()],
        ))
    }
    fn received(
        &self,
        _source: &crate::metadata::MetadataSourceContentWorkScope,
        _source_exact_prefix: Option<&sinomo_dfs::DfsMetadataPrefixProof>,
    ) -> Result<Option<(ImportProposal, ImportCertificate)>> {
        // This explicitly local factory owns no peer receiver or native votes.
        Ok(None)
    }
    fn assignments(
        &self,
        _file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Vec<DfsMetadataShardConfig>> {
        // A local source-owner plan carries actual source ownership separately;
        // it must not manufacture native Raft assignments.
        Ok(Vec::new())
    }
    fn plan(&self, source: &MetadataSourceCut) -> Result<ImportPlan> {
        if !source.projection_assignments.is_empty() {
            return Err(error("standalone source has native assignment evidence"));
        }
        let evidence = self.source_evidence(source, &source.source_root)?;
        let owner_binding =
            self.factory
                .binding_for_source(source, &evidence, &source.source_root)?;
        let actual = sinomo_dfs::inspect_metadata_candidate(&source.staged_root)?;
        if actual.state_fingerprint != source.current_image_fingerprint
            || actual.prefix_proof != source.target_exact_prefix
        {
            return Err(error(
                "standalone candidate changed after its owned source cut",
            ));
        }
        Ok(ImportPlan::Standalone(StandaloneMetadataImportPlan {
            facts: MetadataImportPlanFacts {
                expected_assignments: Vec::new(),
                desired_assignments: Vec::new(),
                target_exact_prefix: source.target_exact_prefix.clone(),
                current_image_fingerprint: source.current_image_fingerprint.clone(),
                pending_operation_mapping: source.pending_operations.clone(),
                combined_layout_source_fences: Vec::new(),
                source_cut: source.clone(),
            },
            owner_binding,
            source_owner_evidence: evidence,
        }))
    }
    fn certify(&self, proposal: &ImportProposal) -> Result<ImportCertificate> {
        self.certify_at(proposal, &proposal.source_root, &proposal.staged_root)
    }
    fn certify_recovered(
        &self,
        proposal: &ImportProposal,
        actual_source_root: &Path,
        resolved_image_root: &Path,
        _provenance_plan_root: &Path,
    ) -> Result<ImportCertificate> {
        // A persisted certificate is never authority. Recover the unchanged
        // proposal under a newly retained actual owner/source capability.
        self.certify_at(proposal, actual_source_root, resolved_image_root)
    }
    fn received_recovered(
        &self,
        _proposal: &ImportProposal,
        _actual_source_root: &Path,
        _resolved_image_root: &Path,
        _provenance_plan_root: &Path,
    ) -> Result<Option<ImportCertificate>> {
        Ok(None)
    }
}

/// Select explicit local source conversion. The ordinary native factory never
/// selects this mode from a missing cluster client.
pub fn storage_preparation_provider(
    context: &sinomo_module::ComponentContext,
) -> std::result::Result<DfsStoragePreparationProvider, sinomo_module::ComponentError> {
    let factory = ExplicitStandaloneSourceFactory::from_context(context)
        .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
    let owner = Arc::clone(&factory.data_owner);
    let authority = Arc::new(StandaloneImportAuthority { factory });
    let metadata = MetadataConverter::with_authority(authority);
    let content = crate::content::ContentConverter::default();
    let local_state = crate::local_state::LocalStateConverter::default();
    let dfs_root = context.data_dir().dfs_dir();
    let metadata_root = dfs_root.join("metadata");
    Ok(Arc::new(move |root, scope, budget| {
        let _owner = &owner;
        if matches!(scope, DfsStoragePreparationScope::MetadataRouting) {
            if root != metadata_root {
                return Err(error("standalone routing state changes actual owner root"));
            }
            return if super::routing_cache::current(root)? {
                Ok(sinomo_dfs::DfsStoragePreparation::Ready)
            } else {
                Err(error(
                    "retained routing topology requires actual native configuration authority",
                ))
            };
        }
        let local = local_state.prepare(&dfs_root, budget)?;
        if matches!(local, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
            return Ok(local);
        }
        match scope {
            DfsStoragePreparationScope::Metadata(shard) => {
                let preparation = content.prepare(&dfs_root.join("content"), budget)?;
                if matches!(
                    preparation,
                    sinomo_dfs::DfsStoragePreparation::Pending { .. }
                ) {
                    return Ok(preparation);
                }
                let combined = metadata_root.join("root");
                let source = if root != combined && !root.exists() && combined.exists() {
                    combined.as_path()
                } else {
                    root
                };
                metadata.prepare_with_source(root, source, shard, budget)
            }
            DfsStoragePreparationScope::Content => content.prepare(root, budget),
            DfsStoragePreparationScope::MetadataRouting => {
                unreachable!("handled by actual routing owner")
            }
            DfsStoragePreparationScope::LocalState => {
                if root != dfs_root {
                    return Err(error("standalone local state changes actual owner root"));
                }
                Ok(sinomo_dfs::DfsStoragePreparation::Ready)
            }
        }
    }))
}

fn file_identity(file: &std::fs::File) -> Result<(u64, u64)> {
    sinomo_dfs::runtime_host::storage_file_identity(file).map_err(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_cannot_select_explicit_standalone_source_ownership() {
        let root = sinomo_testdir::TestDir::new("standalone-source-shadow");
        let context = sinomo_module::ComponentContext::new(Arc::new(
            sinomo_core::local::SinomoDataDir::new(root.path()),
        ))
        .with_host_role(sinomo_module::ComponentHostRole::Shadow);
        let error = ExplicitStandaloneSourceFactory::from_context(&context)
            .err()
            .expect("shadow must not acquire source ownership");
        assert!(error.to_string().contains("shadow"));
        assert!(!root.path().join("journal/write.lock").exists());
    }

    #[test]
    fn missing_cluster_client_does_not_grant_standalone_source_ownership() {
        let root = sinomo_testdir::TestDir::new("standalone-source-no-owner");
        let context = sinomo_module::ComponentContext::new(Arc::new(
            sinomo_core::local::SinomoDataDir::new(root.path()),
        ));
        assert!(context.cluster_access().is_none());
        assert!(ExplicitStandaloneSourceFactory::from_context(&context).is_err());
        assert!(!root.path().join("daemon/data-owner.lock").exists());
        assert!(!root.path().join("journal/write.lock").exists());
    }
}
