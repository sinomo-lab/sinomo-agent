//! Native import control-plane boundary. Conversion bytes stay in migration
//! storage; only compact source/image/cut identities cross existing Raft.
use crate::metadata::{
    MetadataCapturedKvEntry, MetadataConverter, MetadataImportAuthority,
    MetadataImportCertificate as ImportCertificate, MetadataImportCertificateFacts,
    MetadataImportPlan as ImportPlan, MetadataImportPlanFacts,
    MetadataImportProposal as ImportProposal, MetadataImportProposalFacts, MetadataSourceCut,
    NativeMetadataImportCertificate as MetadataImportCertificate,
    NativeMetadataImportPlan as MetadataImportPlan,
    NativeMetadataImportProposal as MetadataImportProposal,
};
use serde::{Deserialize, Serialize};
use sinomo_core::kv::{
    KvAuthorityBasis, KvAuthorityProof, KvCommand, KvCompare, KvEntry, KvError, KvOp, KvRequestId,
    KvRevision, KvTxn, KvTxnRequest, KvTxnResponse,
};
use sinomo_core::service::{ClusterAccess, ServiceError};
use sinomo_dfs::{
    DfsControlPlaneKvClient, DfsControlPlaneStore, DfsMetadataShardConfig, DfsNativeTxnCommit,
    DfsStoragePreparationProvider, DfsStoragePreparationScope, MetadataBackendError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod configuration;
pub(crate) mod content_work;
mod handoff;
mod namespace_work;
pub(crate) mod placement_job_source;
mod placement_work;
mod plan_wire;
mod routing_cache;
pub(crate) mod source_retirement;
mod staged_service;
pub(crate) mod standalone;

type Result<T> = std::result::Result<T, MetadataBackendError>;
fn error(value: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("native metadata import: {value}"))
}
/// Peer source effects require the actual main-daemon owner. Same-node
/// effects retain their existing direct handlers, including Embedded hosts.
fn source_effect_rpc_options() -> sinomo_core::network::ClusterObjectRpcOptions {
    sinomo_core::network::ClusterObjectRpcOptions::default().with_destination_process_affinity(
        sinomo_core::network::DestinationProcessAffinity::MainDaemon,
    )
}
fn current_authority(proof: &KvAuthorityProof) -> Result<()> {
    match proof.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } if proof.term != 0 && required_quorum != 0 && voter_acks >= required_quorum => Ok(()),
        _ => Err(error("fresh native current-term authority is unavailable")),
    }
}
fn block_native<T, E: std::fmt::Display>(
    runtime: &tokio::runtime::Handle,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if current.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(error(
                "native conversion cannot block a current-thread executor",
            ));
        }
        tokio::task::block_in_place(|| runtime.block_on(future)).map_err(error)
    } else {
        runtime.block_on(future).map_err(error)
    }
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(value).map_err(error)?)
        .to_hex()
        .to_string())
}
fn captured_compares(entries: &[MetadataCapturedKvEntry]) -> Vec<KvCompare> {
    entries
        .iter()
        .flat_map(|captured| match &captured.entry {
            Some(entry) => vec![
                KvCompare::ValueEquals {
                    key: captured.key.clone(),
                    value: entry.value.clone(),
                },
                KvCompare::ModifyRevisionEquals {
                    key: captured.key.clone(),
                    revision: entry.modify_revision,
                },
                KvCompare::LeaseEquals {
                    key: captured.key.clone(),
                    lease_id: entry.lease_id,
                },
            ],
            None => vec![KvCompare::Missing {
                key: captured.key.clone(),
            }],
        })
        .collect()
}

#[derive(Clone)]
struct NativeImportAuthority {
    component_context: sinomo_module::ComponentContext,
    lifecycle: tokio::sync::watch::Receiver<bool>,
    data_root: PathBuf,
    metadata_root: PathBuf,
    receiver_root: PathBuf,
    cluster: Option<Arc<dyn ClusterAccess>>,
    runtime: tokio::runtime::Handle,
    transport: Option<Arc<dyn sinomo_module::ComponentObjectRpcTransport>>,
    owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    namespace_work: Arc<Mutex<BTreeMap<String, namespace_work::JobOutcome>>>,
    content_plan_contexts: Arc<Mutex<content_work::verification_context::PlanContexts>>,
    source_verifiers:
        Arc<Mutex<BTreeMap<PathBuf, Arc<Mutex<staged_service::SourceTransferVerifier>>>>>,
}
impl NativeImportAuthority {
    fn cluster(&self) -> Result<&Arc<dyn ClusterAccess>> {
        self.cluster.as_ref().ok_or_else(|| error("retained standalone metadata requires an explicit sealed standalone source-owner certificate"))
    }
    fn validate_source_root(&self, root: &Path) -> Result<()> {
        let root = root.canonicalize().map_err(error)?;
        if !root.starts_with(&self.data_root) {
            return Err(error(
                "source root is outside the component's actual data owner",
            ));
        }
        Ok(())
    }
    fn read_exact(
        &self,
        keys: &[String],
    ) -> Result<(Vec<MetadataCapturedKvEntry>, KvAuthorityProof)> {
        let read = block_native(
            &self.runtime,
            self.cluster()?
                .kv_get_authoritative_exact_set_async(keys.to_vec()),
        )?;
        current_authority(&read.authority)?;
        if read.value.len() != keys.len() {
            return Err(error(
                "native exact-set response changed its key cardinality",
            ));
        }
        let entries = keys
            .iter()
            .cloned()
            .zip(read.value)
            .map(|(key, entry)| MetadataCapturedKvEntry { key, entry })
            .collect();
        Ok((entries, read.authority))
    }
    fn commit(&self, request: KvTxnRequest) -> Result<DfsNativeTxnCommit> {
        let result = block_native(
            &self.runtime,
            self.cluster()?.kv_commit_compact_async(request.clone()),
        )?;
        let submitted = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
            .map_err(error)?;
        if result.request_id != request.request_id
            || result.submitted_command_digest != Some(submitted)
        {
            return Err(error(
                "native commit receipt differs from the exact submitted import request",
            ));
        }
        let response = result
            .txn_response
            .as_ref()
            .ok_or_else(|| error("native commit returned no transaction response"))?;
        if response.revision != result.authority.revision {
            return Err(error(
                "native import CAS did not commit against its captured control-plane state",
            ));
        }
        Ok(DfsNativeTxnCommit { request, result })
    }
    fn require_committed(commit: &DfsNativeTxnCommit) -> Result<()> {
        if !commit
            .result
            .txn_response
            .as_ref()
            .is_some_and(|r| r.succeeded)
        {
            return Err(error(
                "exact native import request did not commit its success branch",
            ));
        }
        match commit.result.authority.basis {
            KvAuthorityBasis::CurrentTermQuorum { .. } => {
                current_authority(&commit.result.authority)
            }
            KvAuthorityBasis::ExactCommittedRequest if commit.result.authority.term != 0 => Ok(()),
            _ => Err(error(
                "native import outcome has no exact committed authority",
            )),
        }
    }
    fn save_blueprint(root: &Path, name: &str, value: &impl Serialize) -> Result<()> {
        let path = root.join(name);
        let bytes = serde_json::to_vec(value).map_err(error)?;
        if path.exists() {
            if std::fs::read(&path).map_err(error)? != bytes {
                return Err(error(format!(
                    "immutable native import blueprint changed: {name}"
                )));
            }
            return Ok(());
        }
        sinomo_core::storage::durable_file::write_file_atomic(
            &path,
            &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                .sync_all(),
        )
        .map_err(error)
    }
    fn blueprint_commit(&self, root: &Path, request: KvTxnRequest) -> Result<DfsNativeTxnCommit> {
        Self::save_blueprint(
            root,
            &format!("native-request-{}.json", request.request_id),
            &request,
        )?;
        self.distribute_activation_blueprint(root, &request)?;
        self.commit(request)
    }
    fn control_client<'a>(&'a self, root: &'a Path) -> Result<NativeImportKv<'a>> {
        // Provider construction can precede native host registration. Borrow
        // the SAME normal client only when actual owned conversion work runs.
        let normal_custody = sinomo_dfs::runtime_host::owned_storage_import_control_plane_client(
            &self.component_context,
            &self.owner,
        )
        .map_err(error)?;
        Ok(NativeImportKv {
            normal_custody,
            owner: self,
            root,
            last_commit: None,
            original_handoff: None,
        })
    }
    fn require_current_assignment_members(
        &self,
        assignments: &[DfsMetadataShardConfig],
        captured: &[MetadataCapturedKvEntry],
    ) -> Result<()> {
        for node in assignments
            .iter()
            .flat_map(|cfg| std::iter::once(&cfg.primary_node_id).chain(&cfg.replica_node_ids))
        {
            let member: sinomo_core::ClusterMemberRecord = captured
                .iter()
                .find(|row| row.key == sinomo_core::node::cluster_member_key(node))
                .and_then(|row| row.entry.as_ref())
                .ok_or_else(|| error("assigned native member is missing"))?
                .decode()
                .map_err(error)?;
            let policy = captured
                .iter()
                .find(|row| row.key == sinomo_core::node::cluster_capability_policy_key(node))
                .and_then(|row| row.entry.as_ref())
                .map(|entry| entry.decode::<sinomo_core::node::ClusterCapabilityPolicyRecord>())
                .transpose()
                .map_err(error)?;
            if member.node_id != *node
                || !member.state.is_authorized()
                || policy.as_ref().is_some_and(|policy| {
                    policy.node_id != *node || policy.cluster_id != member.cluster_id
                })
                || !member.capabilities.iter().any(|cap| {
                    cap.as_str() == sinomo_core::node::DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY
                })
                || !policy
                    .as_ref()
                    .map_or(member.capabilities.as_slice(), |policy| {
                        policy.capabilities.as_slice()
                    })
                    .iter()
                    .any(|cap| {
                        cap.as_str()
                            == sinomo_core::node::DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY
                    })
            {
                return Err(error(format!(
                    "assigned member {node} must upgrade before configuration conversion"
                )));
            }
        }
        Ok(())
    }

    fn current_file_system_config_for_import(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
        file_system_id: &sinomo_dfs::DfsFileSystemId,
        candidate_count: usize,
    ) -> Result<sinomo_dfs::DfsFileSystemConfig> {
        let (cfg, _) = self.content_work_authority(source, file_system_id)?;
        let keys = [
            sinomo_dfs::file_system_config_key(file_system_id),
            sinomo_dfs::metadata_shard_key(file_system_id, &source.scope_shard.0),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                file_system_id,
                &source.scope_shard.0,
            ),
        ];
        let (captured, _) = self.read_exact(&keys)?;
        let actual_cfg: DfsMetadataShardConfig = captured[1]
            .entry
            .as_ref()
            .ok_or_else(|| error("current import scope disappeared"))?
            .decode()
            .map_err(error)?;
        if actual_cfg != cfg {
            return Err(error(
                "current policy scope changed before migration capture",
            ));
        }
        let entry = captured[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("new current grant has no actual filesystem policy"))?;
        if let Ok(config) = entry.decode::<sinomo_dfs::DfsFileSystemConfig>() {
            if &config.file_system_id != file_system_id {
                return Err(error(
                    "current filesystem policy is bound to another native key",
                ));
            }
            return Ok(config);
        }
        let old: serde_json::Value = entry.decode().map_err(error)?;
        let normalized = crate::legacy_wire::convert_file_system_wire_with_current_import_defaults(
            old,
            Some(candidate_count),
        )
        .map_err(error)?;
        let config: sinomo_dfs::DfsFileSystemConfig =
            serde_json::from_value(normalized).map_err(error)?;
        if &config.file_system_id != file_system_id {
            return Err(error(
                "converted policy changes its native filesystem identity",
            ));
        }
        let record_binding = digest(&(entry, &config))?;
        Self::save_blueprint(
            &self.receiver_root,
            &format!("original-fs-config-{record_binding}.json"),
            entry,
        )?;
        let request = KvTxnRequest::new(
            KvRequestId::derived("dfs-current-fs-config-import-v7", record_binding.as_bytes()),
            KvTxn {
                compares: captured_compares(&captured),
                success: vec![KvOp::Put {
                    key: entry.key.clone(),
                    value: sinomo_core::kv::encode_kv_value(&config).map_err(error)?,
                    lease_id: entry.lease_id,
                }],
                failure: Vec::new(),
            },
        );
        Self::save_blueprint(
            &self.receiver_root,
            &format!("fs-config-request-{record_binding}.json"),
            &request,
        )?;
        let commit = self.commit(request)?;
        Self::require_committed(&commit)?;
        let (observed, _) = self.read_exact(&[keys[0].clone()])?;
        let observed: sinomo_dfs::DfsFileSystemConfig = observed[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("converted filesystem policy disappeared"))?
            .decode()
            .map_err(error)?;
        if observed != config {
            return Err(error(
                "filesystem policy changed before current import planning",
            ));
        }
        Ok(config)
    }
}

impl MetadataImportAuthority for NativeImportAuthority {
    fn queue_original_topology_after_install(
        &self,
        source: &MetadataSourceCut,
        certificate: &ImportCertificate,
        installed_root: &Path,
    ) -> Result<()> {
        self.queue_native_original_topology(source, certificate, installed_root)
    }
    fn queue_published_original_topology(
        &self,
        source: &MetadataSourceCut,
        installed_root: &Path,
    ) -> Result<()> {
        NativeImportAuthority::queue_published_original_topology(self, source, installed_root)
    }
    fn original_handoff_reservation(
        &self,
        shard: &sinomo_dfs::DfsMetadataShardId,
        file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Option<crate::metadata::OriginalMetadataHandoffReservation>> {
        self.capture_original_handoff(shard, file_system_ids)
    }
    fn resume_imported_namespace_jobs(
        &self,
        source: &MetadataSourceCut,
        certificate: &ImportCertificate,
        installed_root: &Path,
        jobs: &[sinomo_dfs::DfsNamespaceJobRecord],
    ) -> Result<()> {
        self.resume_current_imported_jobs(source, certificate, installed_root, jobs)
    }
    fn kind(&self) -> crate::metadata::MetadataImportAuthorityKind {
        crate::metadata::MetadataImportAuthorityKind::Native
    }
    fn validate_owned_source(
        &self,
        root: &Path,
        _scope: &sinomo_dfs::DfsMetadataShardId,
        _file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<()> {
        self.validate_source_root(root)?;
        let _fence = crate::metadata::retain_source_fence(root)?;
        Ok(())
    }
    fn local_node_id(&self) -> Result<sinomo_core::node::NodeId> {
        self.local_node()
    }
    fn pending_original_source_scopes(
        &self,
        source: &MetadataSourceCut,
    ) -> Result<Vec<(sinomo_dfs::DfsFileSystemId, sinomo_dfs::DfsMetadataShardId)>> {
        self.pending_original_sources(source)
    }
    fn fulfill_retained_content(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
        locator: &crate::metadata::MetadataSourceContentLocator,
        policy: sinomo_dfs::DfsReplicationPolicy,
        targets: Vec<sinomo_core::node::NodeId>,
    ) -> Result<content_work::MetadataImportedRetainedContent> {
        self.establish_retained_content(source, object, version, locator, policy, targets)
    }
    fn source_policy_candidate_count(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
    ) -> Result<usize> {
        self.validate_source_root(&source.source_root)?;
        let mut client = self.control_client(&self.receiver_root)?;
        let view = DfsControlPlaneStore::new(&mut client)
            .cluster_view()
            .map_err(error)?;
        let eligible = view
            .eligible_content_node_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        // Zero is unavailable current planning DATA. The controller retains its
        // source cursor until the ordinary host publishes actual eligibility.
        Ok(eligible.len())
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
        self.validate_source_root(&source.source_root)?;
        if object.file_system_id != version.file_system_id || object.object_id != version.object_id
        {
            return Err(error(
                "current import placement changes actual source object identity",
            ));
        }
        let mut client = self.control_client(&self.receiver_root)?;
        let store = DfsControlPlaneStore::new(&mut client);
        let view = store.cluster_view().map_err(error)?;
        let candidates = view
            .content_nodes
            .into_iter()
            .filter(|node| node.accepts_content)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(error(
                "current import placement has no eligible content candidates",
            ));
        }
        let config = self.current_file_system_config_for_import(
            source,
            &object.file_system_id,
            candidates.len(),
        )?;
        let resolved = sinomo_dfs::resolve_placement_policy(
            &config,
            &object.path,
            object.policy.clone(),
            candidates.len().clamp(1, 2),
        )
        .map_err(error)?;
        let zones = candidates
            .iter()
            .filter_map(|node| node.zone.clone().map(|zone| (node.node_id.clone(), zone)))
            .collect();
        let policy = resolved.policy.replication_policy(zones).map_err(error)?;
        if object.policy == sinomo_core::DfsObjectPolicy::LocalOwned {
            if policy.min_commit_replicas != 1 || policy.desired_replicas != 1 {
                return Err(error("local-owned current import policy is not W1/N1"));
            }
            return Ok((policy, vec![object.owner_node_id.clone()]));
        }
        let plan = sinomo_dfs::plan_replicas(&sinomo_dfs::DfsReplicaPlacementRequest {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            target_version: version.version.clone(),
            coordinator_node_id: self.local_node()?,
            policy: policy.clone(),
            candidates: candidates
                .into_iter()
                .map(|node| {
                    sinomo_dfs::DfsReplicaCandidate::weighted(node.node_id, node.capacity_weight)
                        .map(|candidate| candidate.with_zone(node.zone))
                })
                .collect::<std::result::Result<_, _>>()
                .map_err(error)?,
        })
        .map_err(error)?;
        Ok((policy, plan.desired_replica_nodes))
    }
    fn received(
        &self,
        source: &crate::metadata::MetadataSourceContentWorkScope,
        source_exact_prefix: Option<&sinomo_dfs::DfsMetadataPrefixProof>,
    ) -> Result<Option<(ImportProposal, ImportCertificate)>> {
        self.validate_source_root(&source.source_root)?;
        let targets = source
            .projection_assignments
            .iter()
            .filter(|cfg| cfg.shard_id == source.scope_shard.0)
            .collect::<Vec<_>>();
        let [target] = targets.as_slice() else {
            return Err(error(
                "received physical import requires one exact filesystem/shard assignment",
            ));
        };
        let keys = vec![
            sinomo_dfs::metadata_shard_key(&target.file_system_id, &target.shard_id),
            sinomo_dfs::metadata_shard_membership_handoff_key(
                &target.file_system_id,
                &target.shard_id,
            ),
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &target.file_system_id,
                &target.shard_id,
            ),
        ];
        let (entries, _) = self.read_exact(&keys)?;
        if entries[1].entry.is_some() {
            return Ok(None);
        }
        let Some(entry) = &entries[2].entry else {
            return Ok(None);
        };
        let proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof =
            entry.decode().map_err(error)?;
        if !matches!(
            proof.cause,
            sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
        ) {
            return Ok(None);
        }
        let current: DfsMetadataShardConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("activated receiver assignment disappeared"))?
            .decode()
            .map_err(error)?;
        if current != proof.desired_shard {
            return Err(error(
                "received native import proof does not describe the current assignment",
            ));
        }
        let local = self.local_node()?;
        if !proof.acknowledged_node_ids.contains(&local) {
            return Ok(None);
        }
        let root = self.receiver_root_for(
            &proof.retiring_shard.file_system_id,
            &proof.retiring_shard.shard_id,
            &proof.handoff_id,
        )?;
        if !root.join("native-import-plan.json").exists() {
            return Ok(None);
        }
        let plan: MetadataImportPlan = serde_json::from_slice(
            &std::fs::read(root.join("native-import-plan.json")).map_err(error)?,
        )
        .map_err(error)?;
        Self::validate_plan(&plan)?;
        let actual = self
            .check_handoff_state(&plan)?
            .ok_or_else(|| error("receiver activation ceased to be current"))?;
        if actual != proof {
            return Err(error(
                "receiver blueprint is not bound to the actual native activation",
            ));
        }
        let _source_fences = self.validate_local_source_floors(
            &target.file_system_id,
            &source.scope_shard,
            &plan.source_cut,
        )?;
        let candidate = sinomo_dfs::inspect_metadata_candidate(&root)?;
        if candidate.prefix_proof != plan.target_exact_prefix
            || candidate.state_fingerprint != plan.current_image_fingerprint
        {
            return Err(error("received native image changed after its durable ACK"));
        }
        let request = self.find_native_request(&root, &plan.expected_handoff, true)?;
        let request_proof = Self::activation_proof(&request)?
            .ok_or_else(|| error("received activation request omits its proof"))?;
        self.validate_activation_request(&request, &plan, &request_proof)?;
        if request_proof != actual {
            return Err(error(
                "received activation request differs from the retained native joint proof",
            ));
        }
        let stage_commit = self.commit(plan.stage_request.clone())?;
        Self::require_committed(&stage_commit)?;
        let activation_commit = self.commit(request.clone())?;
        Self::require_committed(&activation_commit)?;
        if self.check_handoff_state(&plan)? != Some(actual.clone()) {
            return Err(error(
                "native import moved while exact receipts were recovered",
            ));
        }
        let proposal = MetadataImportProposal {
            facts: MetadataImportProposalFacts {
                source_root: source.source_root.clone(),
                source_identity: source.source_identity.clone(),
                source_snapshot_fingerprint: source.source_snapshot_fingerprint.clone(),
                source_exact_prefix: source_exact_prefix.cloned(),
                current_image_fingerprint: plan.current_image_fingerprint.clone(),
                staged_root: root,
                scope_shard: source.scope_shard.clone(),
                file_system_ids: source.file_system_ids.clone(),
                expected_assignments: plan.expected_assignments.clone(),
                target_exact_prefix: Some(plan.target_exact_prefix.clone()),
                preserved_pending_operations: plan.pending_operation_mapping.clone(),
                captured_floor_and_watermark_entries: plan
                    .captured_entries
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(error)?,
                source_cut: plan.source_cut.clone(),
            },
            stage_request: plan.stage_request.clone(),
            expected_handoff: plan.expected_handoff.clone(),
            activation_request: Some(request),
        };
        let source_retirements = self.verify_original_source_retirements(
            &plan.source_cut,
            &plan,
            &stage_commit,
            &activation_commit,
            &actual,
        )?;
        let data_reservation_retirements = self.verify_data_reservation_retirements(
            &plan.source_cut,
            &plan,
            &stage_commit,
            &activation_commit,
            &actual,
        )?;
        let certificate = MetadataImportCertificate {
            facts: MetadataImportCertificateFacts {
                proposal_fingerprint: digest(&ImportProposal::Native(proposal.clone()))?,
                current_image_fingerprint: candidate.state_fingerprint,
                target_exact_prefix: candidate.prefix_proof,
            },
            stage_commit,
            activation_commit,
            joint_handoff_proof: actual,
            source_retirements,
            data_reservation_retirements,
        };
        Ok(Some((
            ImportProposal::Native(proposal),
            ImportCertificate::Native(certificate),
        )))
    }
    fn assignments(
        &self,
        file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Vec<DfsMetadataShardConfig>> {
        let cluster = self.cluster()?;
        let mut initial = BTreeMap::new();
        for fs in file_system_ids {
            let read = block_native(
                &self.runtime,
                cluster.kv_range_prefix_authoritative_async(format!(
                    "{}/",
                    sinomo_dfs::metadata_shards_prefix(fs)
                )),
            )?;
            current_authority(&read.authority)?;
            for entry in read.value {
                initial.insert(entry.key.clone(), entry);
            }
        }
        if initial.is_empty() {
            return Err(error(
                "retained data has no authoritative assignments; native genesis is forbidden",
            ));
        }
        let keys = initial.keys().cloned().collect::<Vec<_>>();
        let (captured, _) = self.read_exact(&keys)?;
        let mut assignments = Vec::with_capacity(captured.len());
        let mut conversions = Vec::new();
        for capture in captured {
            let entry = capture.entry.ok_or_else(|| {
                error("assignment disappeared during exact native partition planning")
            })?;
            if initial.get(&capture.key) != Some(&entry) {
                return Err(error(
                    "assignment changed while partitioning the source cut",
                ));
            }
            let mut value: serde_json::Value = entry.decode().map_err(error)?;
            let fields = value
                .as_object_mut()
                .ok_or_else(|| error("stored assignment is not a record"))?;
            let retired_codec = fields.remove("namespace_prepare_codec");
            if let Some(codec) = &retired_codec {
                if !matches!(
                    codec.as_str(),
                    Some(
                        "legacy"
                            | "packed-v1"
                            | "packed-v2"
                            | "packed-v3"
                            | "packed-v6"
                            | "prepared-v7"
                    )
                ) {
                    return Err(error(
                        "unknown retired metadata codec requires an explicit converter",
                    ));
                }
            }
            let cfg: DfsMetadataShardConfig = serde_json::from_value(value).map_err(error)?;
            if !file_system_ids.contains(&cfg.file_system_id)
                || sinomo_dfs::metadata_shard_key(&cfg.file_system_id, &cfg.shard_id) != capture.key
            {
                return Err(error(
                    "native assignment identity differs from its actual KV key",
                ));
            }
            if retired_codec.is_some() {
                conversions.push((
                    entry.clone(),
                    sinomo_core::kv::encode_kv_value(&cfg).map_err(error)?,
                ));
            }
            assignments.push(cfg);
        }
        for fs in file_system_ids {
            if !assignments.iter().any(|cfg| &cfg.file_system_id == fs) {
                return Err(error(
                    "retained filesystem has no exact native routing assignment",
                ));
            }
        }
        assignments.sort_by(|a, b| {
            (&a.file_system_id, &a.shard_id).cmp(&(&b.file_system_id, &b.shard_id))
        });
        if !conversions.is_empty() {
            let mut guarded_keys = keys.clone();
            for cfg in &assignments {
                guarded_keys.push(sinomo_dfs::metadata_shard_membership_handoff_key(
                    &cfg.file_system_id,
                    &cfg.shard_id,
                ));
                guarded_keys.push(sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                    &cfg.file_system_id,
                    &cfg.shard_id,
                ));
                for node in std::iter::once(&cfg.primary_node_id).chain(&cfg.replica_node_ids) {
                    guarded_keys.push(sinomo_core::node::cluster_member_key(node));
                    guarded_keys.push(sinomo_core::node::cluster_capability_policy_key(node));
                }
            }
            guarded_keys.sort();
            guarded_keys.dedup();
            let (guarded, _) = self.read_exact(&guarded_keys)?;
            for initial in initial.values() {
                if guarded
                    .iter()
                    .find(|row| row.key == initial.key)
                    .and_then(|row| row.entry.as_ref())
                    != Some(initial)
                {
                    return Err(error(
                        "source assignment changed before native schema conversion",
                    ));
                }
            }
            self.require_current_assignment_members(&assignments, &guarded)?;
            let binding = digest(&conversions)?;
            Self::save_blueprint(
                &self.receiver_root,
                &format!("original-assignment-config-{binding}.json"),
                &conversions,
            )?;
            let request = KvTxnRequest::new(
                KvRequestId::derived(
                    "dfs-current-assignment-config-import-v7",
                    binding.as_bytes(),
                ),
                KvTxn {
                    compares: captured_compares(&guarded),
                    success: conversions
                        .into_iter()
                        .map(|(entry, value)| KvOp::Put {
                            key: entry.key,
                            value,
                            lease_id: entry.lease_id,
                        })
                        .collect(),
                    failure: Vec::new(),
                },
            );
            Self::save_blueprint(
                &self.receiver_root,
                &format!("assignment-config-request-{binding}.json"),
                &request,
            )?;
            let commit = self.commit(request)?;
            Self::require_committed(&commit)?;
            let (after, _) = self.read_exact(&keys)?;
            let mut observed = after
                .into_iter()
                .map(|row| {
                    row.entry
                        .ok_or_else(|| error("converted assignment disappeared"))?
                        .decode::<DfsMetadataShardConfig>()
                        .map_err(error)
                })
                .collect::<Result<Vec<_>>>()?;
            observed.sort_by(|a, b| {
                (&a.file_system_id, &a.shard_id).cmp(&(&b.file_system_id, &b.shard_id))
            });
            if observed != assignments {
                return Err(error(
                    "converted assignments changed before source partition",
                ));
            }
        }
        Ok(assignments)
    }

    fn plan(&self, source: &MetadataSourceCut) -> Result<ImportPlan> {
        self.validate_source_root(&source.source_root)?;

        if let Some(plan) = self.recover_plan(source)? {
            return Ok(ImportPlan::Native(plan));
        }
        if source.file_system_ids.is_empty() {
            return Err(error("source cut contains no filesystem identity"));
        }
        let cluster = self.cluster()?;
        let mut shard_entries = Vec::new();
        for fs in &source.file_system_ids {
            let read = block_native(
                &self.runtime,
                cluster.kv_range_prefix_authoritative_async(sinomo_dfs::metadata_shards_prefix(fs)),
            )?;
            current_authority(&read.authority)?;
            shard_entries.extend(read.value);
        }
        let mut assignments = Vec::new();
        let mut conversions = Vec::new();
        for entry in &shard_entries {
            let mut value: serde_json::Value = entry.decode().map_err(error)?;
            let fields = value
                .as_object_mut()
                .ok_or_else(|| error("stored shard config is not a record"))?;
            let old_codec = fields.remove("namespace_prepare_codec");
            if old_codec.as_ref().is_some_and(|codec| {
                !matches!(
                    codec.as_str(),
                    Some(
                        "legacy"
                            | "packed-v1"
                            | "packed-v2"
                            | "packed-v3"
                            | "packed-v6"
                            | "prepared-v7"
                    )
                )
            }) {
                return Err(error(
                    "unknown retired assignment codec requires an explicit converter",
                ));
            }
            let cfg: DfsMetadataShardConfig = serde_json::from_value(value).map_err(error)?;
            if old_codec.is_some() {
                conversions.push((
                    entry.clone(),
                    sinomo_core::kv::encode_kv_value(&cfg).map_err(error)?,
                ));
            }
            assignments.push(cfg);
        }
        assignments.sort_by(|a, b| {
            (&a.file_system_id, &a.shard_id).cmp(&(&b.file_system_id, &b.shard_id))
        });
        if assignments.is_empty()
            || !assignments
                .iter()
                .any(|cfg| cfg.shard_id == source.scope_shard.0)
        {
            return Err(error(
                "retained source has no actual target assignment; genesis is forbidden",
            ));
        }
        if assignments.len() != source.projection_assignments.len()
            || source.projection_assignments.iter().any(|original| {
                let Some(actual) = assignments.iter().find(|cfg| {
                    cfg.file_system_id == original.file_system_id
                        && cfg.shard_id == original.shard_id
                }) else {
                    return true;
                };
                if original.shard_id == source.scope_shard.0 {
                    return actual != original;
                }
                let mut routing = actual.clone();
                routing.epoch = original.epoch;
                actual.epoch < original.epoch || routing != *original
            })
        {
            return Err(error(
                "source target assignment or sibling routing changed before native import planning",
            ));
        }
        let mut keys = BTreeSet::new();
        for cfg in &assignments {
            keys.extend([
                sinomo_dfs::metadata_shard_key(&cfg.file_system_id, &cfg.shard_id),
                sinomo_dfs::metadata_shard_operation_floor_key(&cfg.file_system_id, &cfg.shard_id),
                sinomo_dfs::metadata_shard_head_watermark_key(&cfg.file_system_id, &cfg.shard_id),
                sinomo_dfs::metadata_shard_membership_handoff_key(
                    &cfg.file_system_id,
                    &cfg.shard_id,
                ),
                sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                    &cfg.file_system_id,
                    &cfg.shard_id,
                ),
            ]);
            for node in std::iter::once(&cfg.primary_node_id).chain(&cfg.replica_node_ids) {
                keys.insert(sinomo_core::node::cluster_member_key(node));
                keys.insert(sinomo_core::node::cluster_capability_policy_key(node));
            }
        }
        let keys: Vec<_> = keys.into_iter().collect();
        let (mut captured, mut authority) = self.read_exact(&keys)?;
        for initial in &shard_entries {
            if captured
                .iter()
                .find(|row| row.key == initial.key)
                .and_then(|row| row.entry.as_ref())
                != Some(initial)
            {
                return Err(error(
                    "assignment changed while the source import scope was planned",
                ));
            }
        }
        self.require_current_assignment_members(&assignments, &captured)?;
        if !conversions.is_empty() {
            let request = KvTxnRequest::new(
                KvRequestId::derived(
                    "dfs-current-config-import-v7",
                    digest(&conversions)?.as_bytes(),
                ),
                KvTxn {
                    compares: captured_compares(&captured),
                    success: conversions
                        .into_iter()
                        .map(|(entry, value)| KvOp::Put {
                            key: entry.key,
                            value,
                            lease_id: entry.lease_id,
                        })
                        .collect(),
                    failure: Vec::new(),
                },
            );
            let commit = self.blueprint_commit(&source.staged_root, request)?;
            Self::require_committed(&commit)?;
            (captured, authority) = self.read_exact(&keys)?;
        }
        let target = source.target_exact_prefix.clone();
        if target.version != 2 || target.journal_sequence < source.source_journal_sequence {
            return Err(error(
                "native mapped target cut cannot omit the fenced source prefix",
            ));
        }
        Self::save_blueprint(
            &source.staged_root,
            "native-import-input.json",
            &(source, &captured, &authority),
        )?;
        let plan = self.stage_handoff(source, captured, authority)?;
        let bytes = serde_json::to_vec(&plan).map_err(error)?;
        sinomo_core::storage::durable_file::write_file_atomic(
            &source.staged_root.join("native-import-plan.json"),
            &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                .sync_all(),
        )
        .map_err(error)?;
        Ok(ImportPlan::Native(plan))
    }

    fn certify(&self, proposal: &ImportProposal) -> Result<ImportCertificate> {
        self.certify_recovered(
            proposal,
            &proposal.source_root,
            &proposal.staged_root,
            &proposal.staged_root,
        )
    }

    fn received_recovered(
        &self,
        proposal: &ImportProposal,
        actual_source_root: &Path,
        resolved_image_root: &Path,
        provenance_plan_root: &Path,
    ) -> Result<Option<ImportCertificate>> {
        let proposal = proposal.native().ok_or_else(|| {
            error("native authority cannot consume a standalone source capability")
        })?;
        self.validate_source_root(actual_source_root)?;
        let _source_fence = crate::metadata::retain_source_fence(actual_source_root)?;
        crate::metadata::validate_retained_source_identity(
            actual_source_root,
            &proposal.source_identity,
            &proposal.source_snapshot_fingerprint,
        )?;
        let plan: MetadataImportPlan = serde_json::from_slice(
            &std::fs::read(provenance_plan_root.join("native-import-plan.json")).map_err(error)?,
        )
        .map_err(error)?;
        Self::validate_plan(&plan)?;
        if digest(&plan.source_cut)? != digest(&proposal.source_cut)?
            || plan.stage_request != proposal.stage_request
            || plan.expected_handoff != proposal.expected_handoff
            || plan.expected_assignments != proposal.expected_assignments
            || plan.current_image_fingerprint != proposal.current_image_fingerprint
            || Some(&plan.target_exact_prefix) != proposal.target_exact_prefix.as_ref()
        {
            return Err(error(
                "recovered receiver proposal changed its immutable source/image/request binding",
            ));
        }
        let Some(proof) = self.check_handoff_state(&plan)? else {
            return Ok(None);
        };
        if !proof.acknowledged_node_ids.contains(&self.local_node()?) {
            return Ok(None);
        }
        let _ancestry_fences =
            self.verify_recovered_source_ancestry(&plan, provenance_plan_root, actual_source_root)?;
        let candidate = crate::metadata::inspect_import_candidate(resolved_image_root)?;
        if candidate.prefix_proof != plan.target_exact_prefix
            || candidate.state_fingerprint != plan.current_image_fingerprint
        {
            return Err(error(
                "recovered received candidate changed after native activation",
            ));
        }
        let request =
            self.find_native_request(provenance_plan_root, &plan.expected_handoff, true)?;
        let request_proof = Self::activation_proof(&request)?
            .ok_or_else(|| error("recovered receiver request has no exact activation proof"))?;
        self.validate_activation_request(&request, &plan, &request_proof)?;
        if request_proof != proof
            || proposal
                .activation_request
                .as_ref()
                .is_some_and(|expected| expected != &request)
        {
            return Err(error(
                "recovered receiver request differs from actual native activation",
            ));
        }
        let stage_commit = self.commit(plan.stage_request.clone())?;
        Self::require_committed(&stage_commit)?;
        let activation_commit = self.commit(request)?;
        Self::require_committed(&activation_commit)?;
        if self.check_handoff_state(&plan)? != Some(proof.clone()) {
            return Err(error(
                "receiver activation moved during exact native recovery",
            ));
        }
        let source_retirements = self.verify_original_source_retirements(
            &plan.source_cut,
            &plan,
            &stage_commit,
            &activation_commit,
            &proof,
        )?;
        let data_reservation_retirements = self.verify_data_reservation_retirements(
            &plan.source_cut,
            &plan,
            &stage_commit,
            &activation_commit,
            &proof,
        )?;
        Ok(Some(ImportCertificate::Native(MetadataImportCertificate {
            facts: MetadataImportCertificateFacts {
                proposal_fingerprint: digest(&ImportProposal::Native(proposal.clone()))?,
                current_image_fingerprint: candidate.state_fingerprint,
                target_exact_prefix: candidate.prefix_proof,
            },
            stage_commit,
            activation_commit,
            joint_handoff_proof: proof,
            source_retirements,
            data_reservation_retirements,
        })))
    }

    fn certify_recovered(
        &self,
        proposal: &ImportProposal,
        actual_source_root: &Path,
        resolved_image_root: &Path,
        provenance_plan_root: &Path,
    ) -> Result<ImportCertificate> {
        let proposal = proposal.native().ok_or_else(|| {
            error("native authority cannot consume a standalone source capability")
        })?;
        self.validate_source_root(actual_source_root)?;
        let _source_fence = crate::metadata::retain_source_fence(actual_source_root)?;
        crate::metadata::validate_retained_source_identity(
            actual_source_root,
            &proposal.source_identity,
            &proposal.source_snapshot_fingerprint,
        )?;
        let path = provenance_plan_root.join("native-import-plan.json");
        let plan: MetadataImportPlan =
            serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
        Self::validate_plan(&plan)?;
        if digest(&plan.source_cut)? != digest(&proposal.source_cut)?
            || plan.stage_request != proposal.stage_request
            || plan.expected_handoff != proposal.expected_handoff
            || plan.expected_assignments != proposal.expected_assignments
            || plan.current_image_fingerprint != proposal.current_image_fingerprint
            || Some(&plan.target_exact_prefix) != proposal.target_exact_prefix.as_ref()
        {
            return Err(error(
                "final source/image/cut differs from its actual native plan",
            ));
        }
        let (stage_commit, activation_commit, proof) =
            self.activate_handoff(&plan, resolved_image_root, provenance_plan_root)?;
        let source_retirements = self.verify_original_source_retirements(
            &plan.source_cut,
            &plan,
            &stage_commit,
            &activation_commit,
            &proof,
        )?;
        let data_reservation_retirements = self.verify_data_reservation_retirements(
            &plan.source_cut,
            &plan,
            &stage_commit,
            &activation_commit,
            &proof,
        )?;
        let proposal_fingerprint = digest(&ImportProposal::Native(proposal.clone()))?;
        Ok(ImportCertificate::Native(MetadataImportCertificate {
            facts: MetadataImportCertificateFacts {
                proposal_fingerprint,
                current_image_fingerprint: proposal.current_image_fingerprint.clone(),
                target_exact_prefix: plan.target_exact_prefix.clone(),
            },
            stage_commit,
            activation_commit,
            joint_handoff_proof: proof,
            source_retirements,
            data_reservation_retirements,
        }))
    }
}

struct NativeImportKv<'a> {
    normal_custody: Box<dyn DfsControlPlaneKvClient>,
    owner: &'a NativeImportAuthority,
    root: &'a Path,
    last_commit: Option<DfsNativeTxnCommit>,
    original_handoff: Option<KvEntry>,
}
impl std::fmt::Debug for NativeImportKv<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeImportKv")
    }
}
impl DfsControlPlaneKvClient for NativeImportKv<'_> {
    fn placement_authority_process_identity(
        &self,
    ) -> Option<sinomo_dfs::DfsPlacementAuthorityProcessIdentity> {
        if *self.owner.lifecycle.borrow() || self.owner.lifecycle.has_changed().is_err() {
            return None;
        }
        let actual =
            sinomo_dfs::runtime_host::retain_component_data_owner(&self.owner.component_context)
                .ok()?;
        if !Arc::ptr_eq(&actual, &self.owner.owner) {
            return None;
        }
        self.normal_custody.placement_authority_process_identity()
    }
    fn storage_import_placement_snapshot(&self) -> Option<Arc<sinomo_dfs::DfsPlacementSnapshot>> {
        self.normal_custody.storage_import_placement_snapshot()
    }

    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &sinomo_core::node::NodeId,
        current_process: &sinomo_dfs::DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        self.placement_authority_process_identity().as_ref() == Some(current_process)
            && self
                .normal_custody
                .authorizes_prior_process_placement_authority_recovery(
                    local_node_id,
                    current_process,
                )
    }
    fn get(&self, key: &str) -> std::result::Result<Option<KvEntry>, KvError> {
        let entry = self
            .owner
            .read_exact(&[key.into()])
            .map(|(mut entries, _)| entries.remove(0).entry)
            .map_err(|e| KvError::Serialization {
                detail: e.to_string(),
            })?;
        if let Some(original) = self
            .original_handoff
            .as_ref()
            .filter(|entry| entry.key == key)
            && entry.as_ref() == Some(original)
        {
            // The ordinary stage builder creates fresh StorageImport DATA;
            // its Missing predicate is replaced atomically at submission.
            return Ok(None);
        }
        Ok(entry)
    }
    fn get_consistent(
        &self,
        keys: &[String],
    ) -> std::result::Result<Vec<Option<KvEntry>>, KvError> {
        self.owner
            .read_exact(keys)
            .map(|(entries, _)| entries.into_iter().map(|entry| entry.entry).collect())
            .map_err(|e| KvError::Serialization {
                detail: e.to_string(),
            })
    }
    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> std::result::Result<Vec<Option<KvEntry>>, KvError> {
        self.get_consistent(keys)
    }
    /// sinomo:kv-configuration: Bootstrap inventory and post-validation borrow
    /// one complete native configuration cut; narrower source scopes retain
    /// the ordinary native prefix bounds.
    fn range_prefix(&self, prefix: &str) -> std::result::Result<Vec<KvEntry>, KvError> {
        let cluster = self.owner.cluster().map_err(|e| KvError::Serialization {
            detail: e.to_string(),
        })?;
        let read = block_native(
            &self.owner.runtime,
            if matches!(prefix, "/" | "/dfs/filesystems/") {
                cluster.kv_range_prefix_complete_authoritative_async(prefix.into())
            } else {
                cluster.kv_range_prefix_authoritative_async(prefix.into())
            },
        )
        .map_err(|e| KvError::Serialization {
            detail: e.to_string(),
        })?;
        current_authority(&read.authority).map_err(|e| KvError::Serialization {
            detail: e.to_string(),
        })?;
        Ok(read.value)
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> std::result::Result<KvRevision, KvError> {
        self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })
        .map(|response| response.revision)
    }
    fn txn(&mut self, txn: KvTxn) -> std::result::Result<KvTxnResponse, KvError> {
        self.txn_with_native_authority(txn)
            .map(|(response, _)| response)
    }
    fn txn_with_native_authority(
        &mut self,
        mut txn: KvTxn,
    ) -> std::result::Result<(KvTxnResponse, Option<DfsNativeTxnCommit>), KvError> {
        if let Some(original) = &self.original_handoff
            && txn.compares.iter().any(
                |compare| matches!(compare, KvCompare::Missing { key } if key == &original.key),
            )
        {
            let [
                KvOp::Put {
                    key,
                    value,
                    lease_id,
                },
            ] = txn.success.as_mut_slice()
            else {
                return Err(KvError::Serialization {
                    detail: "original handoff replacement changes the ordinary stage effect".into(),
                });
            };
            let replacement: sinomo_dfs::DfsMetadataShardMembershipHandoff =
                sinomo_core::kv::decode_kv_value(value)?;
            if key != &original.key
                || !matches!(
                    replacement.cause,
                    sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
                )
            {
                return Err(KvError::Serialization {
                    detail: "original handoff replacement is not a fresh bound StorageImport"
                        .into(),
                });
            }
            *lease_id = original.lease_id;
            txn.compares.retain(
                |compare| !matches!(compare, KvCompare::Missing { key } if key == &original.key),
            );
            txn.compares.extend([
                KvCompare::ValueEquals {
                    key: original.key.clone(),
                    value: original.value.clone(),
                },
                KvCompare::ModifyRevisionEquals {
                    key: original.key.clone(),
                    revision: original.modify_revision,
                },
                KvCompare::LeaseEquals {
                    key: original.key.clone(),
                    lease_id: original.lease_id,
                },
            ]);
        }
        let id = KvRequestId::derived(
            "dfs-storage-import-control-v7",
            digest(&txn)
                .map_err(|e| KvError::Serialization {
                    detail: e.to_string(),
                })?
                .as_bytes(),
        );
        let request = KvTxnRequest::new(id, txn);
        if let Some(handoff) =
            NativeImportAuthority::stage_record(&request).map_err(|e| KvError::Serialization {
                detail: e.to_string(),
            })?
        {
            let (source, captured_entries, captured_authority): (
                MetadataSourceCut,
                Vec<MetadataCapturedKvEntry>,
                KvAuthorityProof,
            ) = serde_json::from_slice(
                &std::fs::read(self.root.join("native-import-input.json")).map_err(|e| {
                    KvError::Serialization {
                        detail: e.to_string(),
                    }
                })?,
            )
            .map_err(|e| KvError::Serialization {
                detail: e.to_string(),
            })?;
            let mut desired_assignments = source.projection_assignments.clone();
            for config in &mut desired_assignments {
                if *config == handoff.retiring_shard {
                    *config = handoff.desired_shard.clone();
                }
            }
            let plan = MetadataImportPlan {
                facts: MetadataImportPlanFacts {
                    expected_assignments: source.projection_assignments.clone(),
                    desired_assignments,
                    target_exact_prefix: source.target_exact_prefix.clone(),
                    current_image_fingerprint: source.current_image_fingerprint.clone(),
                    pending_operation_mapping: source.pending_operations.clone(),
                    combined_layout_source_fences: Vec::new(),
                    source_cut: source,
                },
                captured_entries,
                captured_authority,
                stage_request: request.clone(),
                expected_handoff: handoff,
                activation_request: None,
            };
            NativeImportAuthority::validate_plan(&plan).map_err(|e| KvError::Serialization {
                detail: e.to_string(),
            })?;
            NativeImportAuthority::save_blueprint(self.root, "native-import-plan.json", &plan)
                .map_err(|e| KvError::Serialization {
                    detail: e.to_string(),
                })?;
        }
        let commit = self
            .owner
            .blueprint_commit(self.root, request)
            .map_err(|e| KvError::Serialization {
                detail: e.to_string(),
            })?;
        self.last_commit = Some(commit.clone());
        Ok((
            commit
                .result
                .txn_response
                .clone()
                .expect("validated native response"),
            Some(commit),
        ))
    }
}

struct MigrationRuntime {
    authority: Arc<NativeImportAuthority>,
    metadata: Arc<MetadataConverter>,
    content: Arc<crate::content::ContentConverter>,
    local_state: crate::local_state::LocalStateConverter,
    _owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    metadata_root: PathBuf,
    // A representation-readiness fact for this actual factory/owner lifetime.
    // Publication still consumes current native configuration/guard evidence.
    configuration_ready: std::sync::atomic::AtomicBool,
    routing_cache_lock: Mutex<()>,
    placement_retirement_cursor: Mutex<placement_work::PlacementRetirementCursor>,
    lifecycle: tokio::sync::watch::Receiver<bool>,
}
impl MigrationRuntime {
    /// Advance already captured original placement DATA through the SAME bounded
    /// native source/image/import and retirement continuation. This does not
    /// reset configuration readiness or discover legacy KV on current calls.
    fn prepare_captured_placement_goals(
        &self,
        goals: &[placement_work::OriginalPlacementGoal],
        budget: usize,
    ) -> Result<sinomo_dfs::DfsStoragePreparation> {
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("storage provider component lifecycle closed"));
        }
        let required = self.authority.required_original_placement_sources(goals)?;
        if let Some(required) = required.first() {
            let dfs_root = self
                .metadata_root
                .parent()
                .ok_or_else(|| error("metadata owner has no DFS storage root"))?;
            let content = self.content.prepare(&dfs_root.join("content"), budget)?;
            if matches!(content, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
                return Ok(content);
            }
            let shard = &required.assignment.shard_id;
            if Path::new(shard).components().count() != 1
                || !matches!(
                    Path::new(shard).components().next(),
                    Some(std::path::Component::Normal(_))
                )
            {
                return Err(error(
                    "captured original placement scope is not an owned path segment",
                ));
            }
            let destination = self.metadata_root.join(shard);
            let combined = self.metadata_root.join("root");
            let source = if !destination.exists() && combined.exists() {
                combined.as_path()
            } else {
                destination.as_path()
            };
            let prepared = self.metadata.prepare_metadata_data_reservations(
                &destination,
                source,
                &sinomo_dfs::DfsMetadataShardId::new(shard),
                &required.capsules,
                budget,
            )?;
            return Ok(match prepared {
                sinomo_dfs::DfsStoragePreparation::Pending { processed_records } => {
                    sinomo_dfs::DfsStoragePreparation::Pending { processed_records }
                }
                sinomo_dfs::DfsStoragePreparation::Ready => {
                    sinomo_dfs::DfsStoragePreparation::Pending {
                        processed_records: 0,
                    }
                }
            });
        }
        let retired = {
            let mut cursor = self
                .placement_retirement_cursor
                .lock()
                .map_err(|_| error("placement retirement continuation poisoned"))?;
            self.authority
                .retire_and_queue_placement_goals(goals, budget, &mut cursor)?
        };
        if !retired {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                processed_records: budget,
            });
        }
        Ok(sinomo_dfs::DfsStoragePreparation::Ready)
    }
}
static RUNTIMES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<MigrationRuntime>>>> = OnceLock::new();
fn source_effect_host(context: &sinomo_module::ComponentContext) -> bool {
    matches!(
        context.host_role(),
        sinomo_module::ComponentHostRole::Main | sinomo_module::ComponentHostRole::Embedded
    )
}
fn shared_runtime(
    context: &sinomo_module::ComponentContext,
    lifecycle: tokio::sync::watch::Receiver<bool>,
) -> std::result::Result<Arc<MigrationRuntime>, sinomo_module::ComponentError> {
    if !source_effect_host(context) {
        return Err(sinomo_module::ComponentError::component(
            "dfs-migration",
            "shadow host cannot own source conversion, staging, or activation",
        ));
    }
    let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
        context.data_dir(),
        context.host_role(),
    );
    let owner = sinomo_dfs::runtime_host::retain_component_data_owner(context)
        .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
    if *lifecycle.borrow() || lifecycle.has_changed().is_err() {
        return Err(sinomo_module::ComponentError::component(
            "dfs-migration",
            "storage provider component lifecycle is closed",
        ));
    }
    let mut roots = RUNTIMES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(runtime) = roots.get(&paths.root).and_then(Weak::upgrade) {
        if Arc::ptr_eq(&runtime._owner, &owner)
            && !*runtime.lifecycle.borrow()
            && runtime.lifecycle.has_changed().is_ok()
        {
            if !runtime.lifecycle.same_channel(&lifecycle) {
                return Err(sinomo_module::ComponentError::component(
                    "dfs-migration",
                    "actual data owner already has a live migration controller under another provider lifecycle",
                ));
            }
            return Ok(runtime);
        }
    }
    let runtime = context.runtime().cloned().ok_or_else(|| {
        sinomo_module::ComponentError::RuntimeUnavailable {
            component_id: "dfs-migration".into(),
        }
    })?;
    let data_root = context
        .data_dir_root()
        .canonicalize()
        .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
    let registered_cluster = sinomo_dfs::runtime_host::registered_component_cluster(context)
        .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
    let authority = Arc::new(NativeImportAuthority {
        component_context: context.clone(),
        lifecycle: lifecycle.clone(),
        data_root,
        metadata_root: context.data_dir().dfs_dir().join("metadata"),
        receiver_root: paths.root.join("dfs-migration/receivers"),
        cluster: registered_cluster.or_else(|| context.cluster_access()),
        runtime,
        transport: context.object_rpc_transport(),
        owner: Arc::clone(&owner),
        namespace_work: Arc::new(Mutex::new(BTreeMap::new())),
        content_plan_contexts: Arc::new(Mutex::new(Default::default())),
        source_verifiers: Arc::new(Mutex::new(BTreeMap::new())),
    });
    let value = Arc::new(MigrationRuntime {
        metadata: Arc::new(MetadataConverter::with_authority(authority.clone())),
        authority,
        content: Arc::new(crate::content::ContentConverter::default()),
        local_state: crate::local_state::LocalStateConverter::default(),
        _owner: owner,
        metadata_root: context.data_dir().dfs_dir().join("metadata"),
        configuration_ready: std::sync::atomic::AtomicBool::new(false),
        routing_cache_lock: Mutex::new(()),
        placement_retirement_cursor: Mutex::new(Default::default()),
        lifecycle,
    });
    roots.insert(paths.root, Arc::downgrade(&value));
    Ok(value)
}

/// Mount after the normal DFS component has acquired its real storage lease.
/// The service exposes only hidden native candidates, never normal DFS routes.
pub fn staged_import_component_factory()
-> std::result::Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    staged_service::factory()
}

/// Built after the component factory has retained its real data-owner lease.
pub fn storage_preparation_provider(
    context: &sinomo_module::ComponentContext,
    lifecycle: tokio::sync::watch::Receiver<bool>,
) -> std::result::Result<DfsStoragePreparationProvider, sinomo_module::ComponentError> {
    if !source_effect_host(context) {
        let owner = sinomo_dfs::runtime_host::retain_component_data_owner(context)
            .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
        return Ok(Arc::new(move |root, scope, _budget| {
            let _owner = &owner;
            if *lifecycle.borrow() || lifecycle.has_changed().is_err() {
                return Err(error("read-only storage provider lifecycle closed"));
            }
            let current = match scope {
                DfsStoragePreparationScope::Metadata(_) => crate::metadata::current_root(root)?,
                DfsStoragePreparationScope::MetadataRouting => routing_cache::current(root)?,
                DfsStoragePreparationScope::Content => crate::content::current(root)?,
                DfsStoragePreparationScope::LocalState => {
                    !root.join("mounted-mutation-outbox").exists()
                        || std::fs::read(root.join("local-state-converted-v8"))
                            .ok()
                            .as_deref()
                            == Some(b"dfs-local-state-converted-v8\n")
                }
            };
            Ok(if current {
                sinomo_dfs::DfsStoragePreparation::Ready
            } else {
                sinomo_dfs::DfsStoragePreparation::Pending {
                    processed_records: 0,
                }
            })
        }));
    }
    let runtime = shared_runtime(context, lifecycle)?;
    Ok(Arc::new(move |root, scope, budget| {
        if *runtime.lifecycle.borrow() || runtime.lifecycle.has_changed().is_err() {
            return Err(error("storage provider component lifecycle closed"));
        }
        if matches!(scope, DfsStoragePreparationScope::LocalState) {
            // Preparation supplies the physical root, while the original
            // component context remains the registered runtime identity.
            let actual =
                sinomo_core::local::SinomoDataDir::new(&runtime.authority.data_root).dfs_dir();
            if root != actual {
                return Err(error(
                    "local-state preparation changed its actual owner root",
                ));
            }
            return runtime.local_state.prepare(&actual, budget);
        }
        if matches!(scope, DfsStoragePreparationScope::MetadataRouting) {
            let _routing = runtime
                .routing_cache_lock
                .lock()
                .map_err(|_| error("routing cache preparation lock poisoned"))?;
            runtime.authority.validate_routing_cache_root(root)?;
            // A current routing representation cannot certify the native
            // configuration schema consumed before the physical-shard hooks.
            let configuration = runtime.authority.prepare_configuration_records(budget)?;
            if matches!(
                configuration,
                sinomo_dfs::DfsStoragePreparation::Pending { .. }
            ) {
                return Ok(configuration);
            }
            if routing_cache::current(root)? {
                return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
            }
            return runtime
                .authority
                .prepare_routing_topology_cache(root, budget);
        }
        if matches!(scope, DfsStoragePreparationScope::Content) {
            let actual_dfs_root =
                sinomo_core::local::SinomoDataDir::new(&runtime.authority.data_root).dfs_dir();
            if root != actual_dfs_root.join("content") {
                return Err(error("content preparation changed its actual owner root"));
            }
            let local = runtime.local_state.prepare(&actual_dfs_root, budget)?;
            if matches!(local, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
                return Ok(local);
            }
            // Content is a dependency of native metadata import. Its cold-open
            // cannot re-enter that unfinished metadata/placement controller.
            // Ready here certifies only the actual content representation.
            return runtime.content.prepare(root, budget);
        }
        if !runtime
            .configuration_ready
            .load(std::sync::atomic::Ordering::Acquire)
        {
            let configuration = runtime.authority.prepare_configuration_records(budget)?;
            if matches!(
                configuration,
                sinomo_dfs::DfsStoragePreparation::Pending { .. }
            ) {
                return Ok(configuration);
            }
            let Some(goals) = runtime.authority.capture_original_placement_goals(budget)? else {
                return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                    processed_records: budget,
                });
            };
            let prepared = runtime.prepare_captured_placement_goals(&goals, budget)?;
            if matches!(prepared, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
                return Ok(prepared);
            }
            let original_sources = match runtime.authority.required_original_metadata_sources() {
                Ok(sources) => sources,
                Err(MetadataBackendError::StoragePreparationPending {
                    processed_records, ..
                }) => {
                    return Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records });
                }
                Err(error) => return Err(error),
            };
            if let Some(original) = original_sources.first() {
                let dfs_root = runtime
                    .metadata_root
                    .parent()
                    .ok_or_else(|| error("metadata owner has no DFS storage root"))?;
                let content = runtime.content.prepare(&dfs_root.join("content"), budget)?;
                if matches!(content, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
                    return Ok(content);
                }
                let shard = &original.retiring_shard.shard_id;
                if Path::new(shard).components().count() != 1
                    || !matches!(
                        Path::new(shard).components().next(),
                        Some(std::path::Component::Normal(_))
                    )
                {
                    return Err(error(
                        "original configured shard is not an owned path segment",
                    ));
                }
                let destination = runtime.metadata_root.join(shard);
                let combined = runtime.metadata_root.join("root");
                let source = if !destination.exists() && combined.exists() {
                    combined.as_path()
                } else {
                    destination.as_path()
                };
                return match runtime.metadata.prepare_original_reservation(
                    &destination,
                    source,
                    original,
                    budget,
                )? {
                    sinomo_dfs::DfsStoragePreparation::Pending { processed_records } => {
                        Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records })
                    }
                    sinomo_dfs::DfsStoragePreparation::Ready =>
                    // Re-read the native reservation set on the next bounded
                    // call; completion of one scope cannot certify another.
                    {
                        Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                            processed_records: 0,
                        })
                    }
                };
            }
            runtime
                .configuration_ready
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let dfs_root = runtime
            .metadata_root
            .parent()
            .ok_or_else(|| error("metadata owner has no DFS storage root"))?;
        let local = runtime.local_state.prepare(dfs_root, budget)?;
        if matches!(local, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
            return Ok(local);
        }
        match scope {
            DfsStoragePreparationScope::Metadata(shard) => {
                let content = runtime.content.prepare(&dfs_root.join("content"), budget)?;
                if matches!(content, sinomo_dfs::DfsStoragePreparation::Pending { .. }) {
                    return Ok(content);
                }
                let combined = runtime.metadata_root.join("root");
                let source = if root != combined && !root.exists() && combined.exists() {
                    combined.as_path()
                } else {
                    root
                };
                // prepare_with_source owns format discovery and verifies the retained
                // source. A missing destination never proves that history is empty.
                runtime
                    .metadata
                    .prepare_with_source(root, source, shard, budget)
            }
            DfsStoragePreparationScope::Content => {
                unreachable!("handled before metadata preparation")
            }
            DfsStoragePreparationScope::MetadataRouting => {
                unreachable!("handled before physical-shard preparation")
            }
            DfsStoragePreparationScope::LocalState => {
                unreachable!("handled by actual local-state owner")
            }
        }
    }))
}

/// Explicit epoch-zero local-source conversion. Native one-member runtimes
/// continue to use `storage_preparation_provider` and real native authority.
pub fn standalone_storage_preparation_provider(
    context: &sinomo_module::ComponentContext,
) -> std::result::Result<DfsStoragePreparationProvider, sinomo_module::ComponentError> {
    standalone::storage_preparation_provider(context)
}

#[cfg(test)]
mod provider_lifecycle_tests {
    use super::*;

    #[test]
    fn native_retired_root_codec_is_normalized_before_cold_current_authority() {
        use sinomo_dfs::{DfsFileSystemConfig, test_support::DfsTestRuntime};
        use std::sync::atomic::{AtomicBool, Ordering};
        let name = "native-retired-root-codec-admission";
        let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        let inject = Arc::new(AtomicBool::new(false));
        let captured = Arc::new(Mutex::new(None::<(Vec<KvEntry>, DfsFileSystemConfig)>));
        let mount_inject = Arc::clone(&inject);
        let mount_capture = Arc::clone(&captured);
        let builder: sinomo_dfs::DfsStoragePreparationProviderBuilder =
            Arc::new(move |context, lifecycle| {
                // Seed actual original native rows before the cold DFS host exists.
                // Normal live refresh cannot consume it before its source is captured.
                if mount_inject.swap(false, Ordering::SeqCst) {
                    let cluster = context.cluster_access().unwrap();
                    let runtime = context.runtime().unwrap();
                    let fs = sinomo_dfs::sifs_file_system_id();
                    let keys = vec![
                        sinomo_dfs::metadata_shard_key(&fs, "root"),
                        sinomo_dfs::file_system_config_key(&fs),
                    ];
                    let before = block_native(
                        runtime,
                        cluster.kv_get_authoritative_exact_set_async(keys.clone()),
                    )
                    .unwrap();
                    sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(
                        &before.authority,
                    )
                    .unwrap();
                    let before = before
                        .value
                        .into_iter()
                        .map(Option::unwrap)
                        .collect::<Vec<_>>();
                    for (entry, key) in before.iter().zip(&keys) {
                        assert_eq!(&entry.key, key);
                    }
                    let assignment = before[0].decode::<DfsMetadataShardConfig>().unwrap();
                    let mut expected_fs = before[1].decode::<DfsFileSystemConfig>().unwrap();
                    let mut assignment_source = serde_json::to_value(&assignment).unwrap();
                    assignment_source["namespace_prepare_codec"] = serde_json::json!("packed-v6");
                    let mut fs_source = serde_json::to_value(&expected_fs).unwrap();
                    // Released schema omits counts or records them as null. Current
                    // import grants must come from actual native membership, not a
                    // replicated rule which current bootstrap no longer persists.
                    let rules = fs_source["subtree_policies"].as_array_mut().unwrap();
                    rules.push(serde_json::json!({
                        "path_prefix":"/users", "policy":"replicated-eventual"
                    }));
                    rules.push(serde_json::json!({
                        "path_prefix":"/services", "policy":"replicated-eventual",
                        "desired_replicas":null, "min_commit_replicas":null
                    }));
                    for path in ["/users", "/services"] {
                        expected_fs
                            .subtree_policies
                            .push(sinomo_dfs::DfsSubtreePolicy::new(
                                path,
                                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                                1,
                                1,
                            ));
                    }
                    let source_bytes = [assignment_source, fs_source]
                        .iter()
                        .map(sinomo_core::kv::encode_kv_value)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .unwrap();
                    let captured_before = before
                        .iter()
                        .map(|entry| MetadataCapturedKvEntry {
                            key: entry.key.clone(),
                            entry: Some(entry.clone()),
                        })
                        .collect::<Vec<_>>();
                    let request = KvTxnRequest::generated(KvTxn {
                        compares: captured_compares(&captured_before),
                        success: before
                            .iter()
                            .zip(&source_bytes)
                            .map(|(entry, value)| KvOp::Put {
                                key: entry.key.clone(),
                                value: value.clone(),
                                lease_id: entry.lease_id,
                            })
                            .collect(),
                        failure: Vec::new(),
                    });
                    let command_digest =
                        sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
                            .unwrap();
                    let request_id = request.request_id.clone();
                    let committed =
                        block_native(runtime, cluster.kv_commit_compact_async(request)).unwrap();
                    assert_eq!(committed.request_id, request_id);
                    assert_eq!(committed.submitted_command_digest, Some(command_digest));
                    assert!(committed.txn_response.unwrap().succeeded);
                    let original =
                        block_native(runtime, cluster.kv_get_authoritative_exact_set_async(keys))
                            .unwrap();
                    sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(
                        &original.authority,
                    )
                    .unwrap();
                    let original = original
                        .value
                        .into_iter()
                        .map(Option::unwrap)
                        .collect::<Vec<_>>();
                    for ((entry, value), prior) in original.iter().zip(source_bytes).zip(&before) {
                        assert_eq!(entry.key, prior.key);
                        assert_eq!(entry.value, value);
                        assert_eq!(entry.lease_id, prior.lease_id);
                    }
                    assert!(
                        original[0]
                            .decode::<DfsMetadataShardConfig>()
                            .unwrap_err()
                            .to_string()
                            .contains("namespace_prepare_codec")
                    );
                    assert!(original[1].decode::<DfsFileSystemConfig>().is_err());
                    *mount_capture.lock().unwrap() = Some((original, expected_fs));
                }
                storage_preparation_provider(context, lifecycle)
            });
        native
            .configure_component_factories(vec![
                Arc::new(sinomo_dfs::component_factory_with_storage_preparation(builder).unwrap()),
                Arc::new(staged_import_component_factory().unwrap()),
            ])
            .unwrap();
        native.start_nodes().unwrap();
        let path = "/native-root-codec-preserved";
        let first_bytes = b"actual native bytes before authority normalization";
        let mut client = native.client(&node).unwrap();
        client
            .posix_write_file(
                path,
                first_bytes,
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        drop(client);
        let metadata_root = native.data_dir(&node).dfs_dir().join("metadata");
        assert!(crate::metadata::current_root(&metadata_root.join("root")).unwrap());
        assert!(routing_cache::current(&metadata_root).unwrap());
        let expected = native.root_shard().unwrap();
        native.stop_node(&node).unwrap();
        inject.store(true, Ordering::SeqCst);
        native.reopen_node(node.clone()).expect(
            "normal cold startup prepares known original authority before strict current decoding",
        );
        assert!(!inject.load(Ordering::SeqCst));
        let (originals, expected_fs) = captured.lock().unwrap().clone().unwrap();
        let original = &originals[0];
        let context = native.component_context(&node).unwrap();
        let cluster = context.cluster_access().unwrap();
        let runtime = native.runtime_handle();
        let current = runtime
            .block_on(cluster.kv_get_authoritative_exact_set_async(
                originals.iter().map(|entry| entry.key.clone()).collect(),
            ))
            .unwrap();
        sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(&current.authority)
            .unwrap();
        let current_fs = current.value[1].as_ref().unwrap();
        assert_eq!(current_fs.key, originals[1].key);
        assert_eq!(
            current_fs.decode::<DfsFileSystemConfig>().unwrap(),
            expected_fs
        );
        assert_eq!(
            current_fs.value,
            sinomo_core::kv::encode_kv_value(&expected_fs).unwrap()
        );
        assert_eq!(current_fs.lease_id, originals[1].lease_id);
        assert_eq!(current_fs.create_revision, originals[1].create_revision);
        assert!(current_fs.modify_revision > originals[1].modify_revision);
        let current = current.value[0].clone().unwrap();
        assert_eq!(current.key, original.key);
        assert_eq!(
            current.decode::<DfsMetadataShardConfig>().unwrap(),
            expected
        );
        assert_eq!(
            current.value,
            sinomo_core::kv::encode_kv_value(&expected).unwrap()
        );
        assert_eq!(current.lease_id, original.lease_id);
        assert_eq!(current.create_revision, original.create_revision);
        assert!(current.modify_revision > original.modify_revision);
        let migration = placement_work::native_tests::canonical_migration_runtime(&context);
        let archive = migration
            .authority
            .receiver_root
            .join("configuration-v7")
            .join(format!("original-{}.json", digest(original).unwrap()));
        assert_eq!(
            std::fs::read(&archive).unwrap(),
            serde_json::to_vec(original).unwrap()
        );
        let fs_archive = migration
            .authority
            .receiver_root
            .join("configuration-v7")
            .join(format!("original-{}.json", digest(&originals[1]).unwrap()));
        assert_eq!(
            std::fs::read(&fs_archive).unwrap(),
            serde_json::to_vec(&originals[1]).unwrap()
        );
        drop(migration);
        let mut client = native.client(&node).unwrap();
        assert_eq!(
            client.posix_read_file_distributed(path).unwrap(),
            first_bytes
        );
        let final_bytes = b"actual native writer after authority normalization";
        client
            .posix_write_file(
                path,
                final_bytes,
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        assert_eq!(
            client.posix_read_file_distributed(path).unwrap(),
            final_bytes
        );
        let shared_path = "/services/native-missing-policy-counts";
        client
            .posix_write_file(
                shared_path,
                final_bytes,
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        assert_eq!(
            client.posix_read_file_distributed(shared_path).unwrap(),
            final_bytes
        );
        drop(client);
        drop(cluster);
        drop(context);
        native.stop_node(&node).unwrap();
        native.reopen_node(node.clone()).unwrap();
        let mut client = native.client(&node).unwrap();
        assert_eq!(
            client.posix_read_file_distributed(path).unwrap(),
            final_bytes
        );
        assert_eq!(
            client.posix_read_file_distributed(shared_path).unwrap(),
            final_bytes
        );
        assert_eq!(
            std::fs::read(archive).unwrap(),
            serde_json::to_vec(original).unwrap()
        );
        assert_eq!(
            std::fs::read(fs_archive).unwrap(),
            serde_json::to_vec(&originals[1]).unwrap()
        );
        drop(client);
        native.close().unwrap();
    }

    #[test]
    fn native_large_complete_filesystem_prefix_reopens_before_normal_dfs_write() {
        use sinomo_dfs::test_support::DfsTestRuntime;
        const VALUE_BYTES: usize = 3 * 1024 * 1024;
        const PREFIX: &str = "/dfs/filesystems/";
        let name = "native-large-complete-prefix-bootstrap";
        let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        native
            .configure_component_factories(vec![
                Arc::new(
                    sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                        storage_preparation_provider,
                    ))
                    .unwrap(),
                ),
                Arc::new(staged_import_component_factory().unwrap()),
            ])
            .unwrap();
        native.start_nodes().unwrap();
        let context = native.component_context(&node).unwrap();
        let cluster = context.cluster_access().unwrap();
        let runtime = native.runtime_handle();
        let fs = sinomo_dfs::sifs_file_system_id();
        let keys = (0..3)
            .map(|index| format!("{PREFIX}{fs}/extensions/native-bootstrap/{index}"))
            .collect::<Vec<_>>();
        // Opaque extension DATA is neither a config nor an intent/receipt/grant.
        // The provider preserves unknown keys while decoding only owned schemas.
        for (index, key) in keys.iter().enumerate() {
            let request = KvTxnRequest::generated(KvTxn {
                compares: vec![KvCompare::Missing { key: key.clone() }],
                success: vec![KvOp::Put {
                    key: key.clone(),
                    value: vec![index as u8; VALUE_BYTES],
                    lease_id: None,
                }],
                failure: Vec::new(),
            });
            let submitted =
                sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone())).unwrap();
            let id = request.request_id.clone();
            let commit = runtime
                .block_on(cluster.kv_commit_compact_async(request))
                .unwrap();
            assert_eq!(commit.request_id, id);
            assert_eq!(commit.submitted_command_digest, Some(submitted));
            assert!(commit.txn_response.unwrap().succeeded);
        }
        let ordinary = runtime
            .block_on(cluster.kv_range_prefix_authoritative_async(PREFIX.into()))
            .unwrap_err();
        assert!(ordinary.to_string().contains("payload bytes"), "{ordinary}");
        assert!(
            ordinary
                .to_string()
                .contains(&sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES.to_string()),
            "{ordinary}"
        );
        let before = runtime
            .block_on(cluster.kv_range_prefix_complete_authoritative_async(PREFIX.into()))
            .unwrap();
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(
            PREFIX, &before,
        )
        .unwrap();
        let payload = before
            .value
            .iter()
            .map(|entry| entry.key.len() + entry.value.len())
            .sum::<usize>();
        assert!(payload > sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES);
        assert!(payload < sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES);
        for (index, key) in keys.iter().enumerate() {
            let entry = before.value.iter().find(|entry| &entry.key == key).unwrap();
            assert_eq!(entry.value, vec![index as u8; VALUE_BYTES]);
        }
        drop(cluster);
        drop(context);
        native.stop_node(&node).unwrap();
        native.reopen_node(node.clone()).unwrap();
        let context = native.component_context(&node).unwrap();
        let cluster = context.cluster_access().unwrap();
        let after = runtime
            .block_on(cluster.kv_range_prefix_complete_authoritative_async(PREFIX.into()))
            .unwrap();
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(
            PREFIX, &after,
        )
        .unwrap();
        assert!(after.authority.revision >= before.authority.revision);
        for key in &keys {
            assert_eq!(
                after.value.iter().find(|entry| &entry.key == key).unwrap(),
                before.value.iter().find(|entry| &entry.key == key).unwrap()
            );
        }
        let mut client = native.client(&node).unwrap();
        let path = "/services/after-large-complete-prefix-bootstrap";
        client
            .posix_write_file(
                path,
                b"real native complete prefix is ready",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        assert_eq!(
            client.posix_read_file_distributed(path).unwrap(),
            b"real native complete prefix is ready"
        );
        drop(client);
        drop(cluster);
        drop(context);
        native.close().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn actual_aliased_host_prepares_physical_local_state_and_queues_on_its_own_runtime() {
        let name = "migration-actual-aliased-runtime-root";
        let node = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native =
            sinomo_dfs::test_support::DfsTestRuntime::new_with_data_dir_alias(name, node.clone())
                .unwrap();
        native.start_nodes().unwrap();
        let context = native.component_context(&node).unwrap();
        let scope = sinomo_module::ComponentScope::new(sinomo_module::ComponentDescriptor::new(
            "migration-aliased-provider",
            "1",
        ));
        let runtime = shared_runtime(&context, scope.shutdown_receiver()).unwrap();
        assert_ne!(runtime.authority.data_root, context.data_dir_root());
        let provider = storage_preparation_provider(&context, scope.shutdown_receiver()).unwrap();
        let physical = context.data_dir().dfs_dir().canonicalize().unwrap();
        for _ in 0..4096 {
            if provider(&physical, &DfsStoragePreparationScope::LocalState, 4).unwrap()
                == sinomo_dfs::DfsStoragePreparation::Ready
            {
                break;
            }
        }
        assert_eq!(
            provider(&physical, &DfsStoragePreparationScope::LocalState, 4).unwrap(),
            sinomo_dfs::DfsStoragePreparation::Ready
        );
        // A sibling physical root is never admitted as this provider's source.
        assert!(
            provider(
                physical.parent().unwrap(),
                &DfsStoragePreparationScope::LocalState,
                4
            )
            .is_err()
        );
        let actual = runtime.authority.component_context.data_dir().clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        sinomo_dfs::runtime_host::queue_namespace_reconciliation_work(
            &actual,
            Arc::clone(&runtime._owner),
            "actual-aliased-owner-work".into(),
            false,
            Arc::new(move || {
                observed.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                Ok(true)
            }),
        )
        .unwrap();
        for _ in 0..64 {
            let pass = native
                .maintenance_tick(&node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .unwrap();
            assert!(pass.outcome.is_ok());
            if calls.load(std::sync::atomic::Ordering::Acquire) > 0 {
                break;
            }
        }
        assert!(
            calls.load(std::sync::atomic::Ordering::Acquire) > 0,
            "SAME real aliased registered owner must run retained work"
        );
        drop(provider);
        drop(runtime);
        drop(scope);
        drop(context);
        native.close().unwrap();
    }

    #[test]
    fn incomplete_permit_source_is_archived_and_rejected_without_deletion() {
        let name = "malformed-permit-source-retention";
        let node = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = sinomo_dfs::test_support::DfsTestRuntime::new(name, node.clone()).unwrap();
        native.start_nodes().unwrap();
        let data_dir = native.data_dir(&node).clone();
        let context = sinomo_module::ComponentContext::new(Arc::new(data_dir.clone()))
            .with_host_role(sinomo_module::ComponentHostRole::Embedded)
            .with_runtime(native.runtime_handle());
        let scope = sinomo_module::ComponentScope::new(sinomo_module::ComponentDescriptor::new(
            "malformed-permit-source-control",
            "1",
        ));
        let runtime = shared_runtime(&context, scope.shutdown_receiver()).unwrap();
        let cluster = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&data_dir),
        )
        .unwrap();
        let fs = sinomo_dfs::sifs_file_system_id();
        let key = sinomo_dfs::placement_authority_permit_key(&fs);
        for mask in 0..7 {
            // Deliberately malformed SOURCE input. This does not model a
            // successful old producer or create a healthy authority receipt.
            let mut raw = serde_json::json!({"job_id":"malformed-source-job", "holder_id":"malformed-source-holder"});
            if mask & 1 != 0 {
                raw["holder_cluster_id"] = serde_json::json!(cluster.cluster_id);
            }
            if mask & 2 != 0 {
                raw["holder_node_id"] = serde_json::json!(node);
            }
            if mask & 4 != 0 {
                raw["holder_session_id"] = serde_json::json!(sinomo_core::Id::generate());
            }
            let (old, _) = runtime.authority.read_exact(&[key.clone()]).unwrap();
            let committed = runtime
                .authority
                .commit(KvTxnRequest::generated(KvTxn {
                    compares: captured_compares(&old),
                    success: vec![KvOp::Put {
                        key: key.clone(),
                        value: sinomo_core::kv::encode_kv_value(&raw).unwrap(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                }))
                .unwrap();
            NativeImportAuthority::require_committed(&committed).unwrap();
            let (before, _) = runtime.authority.read_exact(&[key.clone()]).unwrap();
            assert!(matches!(
                runtime.authority.prepare_configuration_records(1),
                Err(MetadataBackendError::Rejected {
                    code: sinomo_dfs::DfsMetadataRejectionCode::Other,
                    ..
                })
            ));
            let (after, _) = runtime.authority.read_exact(&[key.clone()]).unwrap();
            assert_eq!(
                after[0].entry, before[0].entry,
                "raw value/revisions/lease remain unchanged"
            );
            let original = before[0].entry.as_ref().unwrap();
            let archive = runtime
                .authority
                .receiver_root
                .join("configuration-v7")
                .join(format!("original-{}.json", digest(original).unwrap()));
            let captured: KvEntry =
                serde_json::from_slice(&std::fs::read(archive).unwrap()).unwrap();
            assert_eq!(&captured, original);
        }
        drop(runtime);
        drop(scope);
        drop(context);
        native.close().unwrap();
    }

    #[test]
    fn terminal_v0_goals_archive_and_retire_over_32_across_budget_and_owner_restart() {
        // Exact first released source shape, independent of the current DTO.
        // These are converter SOURCE rows written through actual native Raft.
        #[derive(Serialize)]
        struct OriginalTerminalJob<'a> {
            job_id: &'a str,
            file_system_id: sinomo_dfs::DfsFileSystemId,
            path: &'a str,
            scope: &'a str,
            state: &'a str,
            total_object_count: usize,
            scanned_object_count: usize,
            matched_object_count: usize,
            updated_object_count: usize,
            scheduled_repair_count: usize,
            completed_repair_count: usize,
            pending_repair_count: usize,
            revision: u64,
            created_at_unix_nanos: u64,
            updated_at_unix_nanos: u64,
        }
        const COUNT: usize = 37;
        const BUDGET: usize = 3;
        let name = "migration-terminal-v0-over-32";
        let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
        let mut native =
            sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        native.start_nodes().unwrap();
        let data_dir = native.data_dir(&nodes[0]).clone();
        let context = native.component_context(&nodes[0]).unwrap();
        let first_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("native-terminal-placement-source", "1"),
        );
        let first = shared_runtime(&context, first_scope.shutdown_receiver()).unwrap();
        let fs = sinomo_dfs::sifs_file_system_id();
        let keys = (0..COUNT)
            .map(|index| {
                format!("/dfs/filesystems/{fs}/placement-reconciliations/old-terminal-{index:03}")
            })
            .collect::<Vec<_>>();
        let (prior, _) = first.authority.read_exact(&keys).unwrap();
        assert!(prior.iter().all(|row| row.entry.is_none()));
        let mut source_ops = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let id = format!("old-terminal-{index:03}");
            let path = format!("/services/terminal-source-{index:03}");
            let bytes = sinomo_core::object::serde_ext::to_bytes(
                &OriginalTerminalJob {
                    job_id: &id,
                    file_system_id: fs.clone(),
                    path: &path,
                    scope: "self-only",
                    state: "completed",
                    total_object_count: 2,
                    scanned_object_count: 2,
                    matched_object_count: 2,
                    updated_object_count: 2,
                    scheduled_repair_count: 1,
                    completed_repair_count: 1,
                    pending_repair_count: 0,
                    revision: 4,
                    created_at_unix_nanos: 100,
                    updated_at_unix_nanos: 200,
                },
                1,
                sinomo_core::object::LengthMode::U32,
            )
            .unwrap();
            source_ops.push(KvOp::Put {
                key: key.clone(),
                value: bytes,
                lease_id: None,
            });
        }
        let seeded = first
            .authority
            .commit(KvTxnRequest::generated(KvTxn {
                compares: captured_compares(&prior),
                success: source_ops,
                failure: Vec::new(),
            }))
            .unwrap();
        NativeImportAuthority::require_committed(&seeded).unwrap();
        let (originals, _) = first.authority.read_exact(&keys).unwrap();
        let originals = originals
            .into_iter()
            .map(|row| row.entry.unwrap())
            .collect::<Vec<_>>();
        let mut goals = None;
        for _ in 0..COUNT + 1 {
            if let Some(captured) = first
                .authority
                .capture_original_placement_goals(BUDGET)
                .unwrap()
            {
                goals = Some(captured);
                break;
            }
        }
        let goals = goals.expect("bounded original source capture completes");
        assert_eq!(goals.len(), COUNT);
        assert!(
            !first
                .authority
                .retire_and_queue_placement_goals(
                    &goals,
                    BUDGET,
                    &mut first.placement_retirement_cursor.lock().unwrap()
                )
                .unwrap()
        );
        let (after_first, _) = first.authority.read_exact(&keys).unwrap();
        assert_eq!(
            after_first.iter().filter(|row| row.entry.is_none()).count(),
            BUDGET,
            "one native retirement pass obeys the actual source budget"
        );
        assert!(
            !first
                .configuration_ready
                .load(std::sync::atomic::Ordering::Acquire)
        );
        let actual_owner = Arc::clone(&first._owner);
        drop(first_scope);
        let second_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("native-terminal-placement-source", "1"),
        );
        let second = shared_runtime(&context, second_scope.shutdown_receiver()).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(
            Arc::ptr_eq(&actual_owner, &second._owner),
            "actual native FD is retained"
        );
        let mut restored = None;
        for _ in 0..COUNT + 1 {
            if let Some(captured) = second
                .authority
                .capture_original_placement_goals(BUDGET)
                .unwrap()
            {
                restored = Some(captured);
                break;
            }
        }
        let restored = restored.expect("exact archive is the restart inventory after deletes");
        assert_eq!(restored.len(), COUNT);
        assert_eq!(
            serde_json::to_value(&restored).unwrap(),
            serde_json::to_value(&goals).unwrap()
        );
        // Restart revalidates the first saved native receipts; it cannot trust
        // the abandoned in-memory cursor as permission or skip original rows.
        assert!(
            !second
                .authority
                .retire_and_queue_placement_goals(
                    &restored,
                    BUDGET,
                    &mut second.placement_retirement_cursor.lock().unwrap()
                )
                .unwrap()
        );
        let (after_replay, _) = second.authority.read_exact(&keys).unwrap();
        assert_eq!(
            after_replay
                .iter()
                .filter(|row| row.entry.is_none())
                .count(),
            BUDGET
        );
        let mut done = false;
        let mut retired = BUDGET;
        for _ in 0..COUNT + 1 {
            done = second
                .authority
                .retire_and_queue_placement_goals(
                    &restored,
                    BUDGET,
                    &mut second.placement_retirement_cursor.lock().unwrap(),
                )
                .unwrap();
            let (now, _) = second.authority.read_exact(&keys).unwrap();
            let count = now.iter().filter(|row| row.entry.is_none()).count();
            assert!(count >= retired && count - retired <= BUDGET);
            retired = count;
            if done {
                break;
            }
        }
        assert!(done);
        assert_eq!(retired, COUNT);
        let archive = second.authority.receiver_root.join("configuration-v7");
        let mut new_keys = Vec::new();
        for original in &originals {
            let source = placement_job_source::OriginalPlacementJob::decode(original).unwrap();
            let saved: serde_json::Value = serde_json::from_slice(
                &std::fs::read(archive.join(format!(
                    "placement-goal-{}.json",
                    source.source_digest().unwrap()
                )))
                .unwrap(),
            )
            .unwrap();
            let exact: KvEntry = serde_json::from_value(saved["job"]["entry"].clone()).unwrap();
            assert_eq!(
                &exact, original,
                "raw SOBJ, scope, revisions, clocks and lease remain exact"
            );
            let proofs: Vec<namespace_work::RetirementBlueprint> = serde_json::from_slice(
                &std::fs::read(archive.join(format!(
                    "placement-retirements-{}.json",
                    source.source_digest().unwrap()
                )))
                .unwrap(),
            )
            .unwrap();
            assert!(
                proofs.is_empty(),
                "terminal outcomes need no fabricated active reservation"
            );
            assert!(
                !archive
                    .join(format!(
                        "placement-publication-{}.json",
                        source.source_digest().unwrap()
                    ))
                    .exists(),
                "terminal DATA is never reissued as executable NEW work"
            );
            new_keys.push(source.current_job_key().unwrap());
        }
        let (new_jobs, _) = second.authority.read_exact(&new_keys).unwrap();
        assert!(new_jobs.iter().all(|row| row.entry.is_none()));
        drop(second_scope);
        drop(second);
        drop(first);
        drop(actual_owner);
        drop(context);
        native.close().unwrap();
    }

    #[test]
    fn fresh_component_watch_cannot_reuse_retired_ready_runtime_with_the_same_owner() {
        let name = "migration-real-owner-new-provider-lifecycle";
        let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
        let mut native =
            sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        native.start_nodes().unwrap();
        let data_dir = native.data_dir(&nodes[0]).clone();
        let context = sinomo_module::ComponentContext::new(Arc::new(data_dir.clone()))
            .with_host_role(sinomo_module::ComponentHostRole::Embedded)
            .with_runtime(native.runtime_handle());
        let first_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("native-provider-lifecycle-control", "1"),
        );
        let first_watch = first_scope.shutdown_receiver();
        let first = shared_runtime(&context, first_watch.clone()).unwrap();
        let provider = storage_preparation_provider(&context, first_watch.clone()).unwrap();
        assert!(Arc::ptr_eq(
            &first,
            &shared_runtime(&context, first_watch.clone()).unwrap()
        ));
        let unrelated_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("unrelated-provider-lifecycle", "1"),
        );
        assert!(
            shared_runtime(&context, unrelated_scope.shutdown_receiver()).is_err(),
            "another open scope must not replace the actual owner's live controller"
        );
        assert!(Arc::ptr_eq(
            &first,
            &shared_runtime(&context, first_watch.clone()).unwrap()
        ));
        drop(unrelated_scope);
        let content_deadline =
            std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            assert!(
                std::time::Instant::now() < content_deadline,
                "actual content continuation must settle within its existing native budget"
            );
            match provider(
                &data_dir.dfs_dir().join("content"),
                &DfsStoragePreparationScope::Content,
                256,
            )
            .unwrap()
            {
                sinomo_dfs::DfsStoragePreparation::Ready => break,
                sinomo_dfs::DfsStoragePreparation::Pending { processed_records } => {
                    assert!(processed_records <= 256);
                    assert!(
                        !first
                            .configuration_ready
                            .load(std::sync::atomic::Ordering::Acquire),
                        "bounded actual Content continuation does not certify metadata"
                    );
                }
            }
        }
        assert!(
            !first
                .configuration_ready
                .load(std::sync::atomic::Ordering::Acquire),
            "actual Content readiness does not certify metadata/configuration",
        );
        assert_eq!(
            provider(
                &data_dir.dfs_dir().join("metadata/root"),
                &DfsStoragePreparationScope::Metadata(sinomo_dfs::DfsMetadataShardId::new("root")),
                256,
            )
            .unwrap(),
            sinomo_dfs::DfsStoragePreparation::Ready,
        );
        assert!(
            first
                .configuration_ready
                .load(std::sync::atomic::Ordering::Acquire)
        );
        let actual_owner = Arc::clone(&first._owner);
        drop(first_scope);
        assert!(
            provider(
                &data_dir.dfs_dir().join("content"),
                &DfsStoragePreparationScope::Content,
                256,
            )
            .is_err(),
            "a held old callback cannot mutate after its real scope closes"
        );
        let second_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("native-provider-lifecycle-control", "1"),
        );
        let second = shared_runtime(&context, second_scope.shutdown_receiver()).unwrap();
        assert!(
            Arc::ptr_eq(&actual_owner, &second._owner),
            "the control retains the SAME real native owner FD"
        );
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(!second.lifecycle.same_channel(&first.lifecycle));
        assert!(
            !second
                .configuration_ready
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(shared_runtime(&context, first_watch).is_err());
        drop(second_scope);
        drop(provider);
        drop(second);
        drop(first);
        drop(actual_owner);
        native.close().unwrap();
    }
}
