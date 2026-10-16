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
/// Keep native read availability distinct from domain rejection until the
/// ordinary wrapper reports it. This is private MIG evaluation state, no wire.
#[derive(Debug)]
pub(self) enum NativeEvaluationError {
    Native(sinomo_core::service::ServiceError),
    Domain(sinomo_dfs::MetadataBackendError),
}
impl From<sinomo_dfs::MetadataBackendError> for NativeEvaluationError {
    fn from(error: sinomo_dfs::MetadataBackendError) -> Self {
        Self::Domain(error)
    }
}
impl NativeEvaluationError {
    fn into_metadata(self) -> sinomo_dfs::MetadataBackendError {
        match self {
            Self::Native(native) => error(native),
            Self::Domain(domain) => domain,
        }
    }
}

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
    fn current_receiver_stage_pending(
        &self,
        destination: &Path,
        shard: &sinomo_dfs::DfsMetadataShardId,
    ) -> Result<bool> {
        let directories = match std::fs::read_dir(&self.receiver_root) {
            Ok(directories) => directories,
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(cause) => return Err(error(cause)),
        };
        let marker = destination.join("metadata-import-v7.json");
        let installed: Option<serde_json::Value> = if marker.is_file() {
            Some(serde_json::from_slice(&std::fs::read(marker).map_err(error)?).map_err(error)?)
        } else {
            None
        };
        for directory in directories {
            let directory = directory.map_err(error)?;
            if !directory.file_type().map_err(error)?.is_dir() {
                continue;
            }
            let root = directory.path();
            if !root.join("native-activation-request.json").is_file() {
                continue;
            }
            let path = root.join("native-import-plan.json");
            if !path.is_file() {
                continue;
            }
            let plan: MetadataImportPlan =
                serde_json::from_slice(&std::fs::read(path).map_err(error)?).map_err(error)?;
            Self::validate_plan(&plan)?;
            if plan.source_cut.scope_shard != *shard {
                continue;
            }
            if let Some(marker) = installed.as_ref().filter(|marker| {
                marker["installation_complete"].as_bool() == Some(true)
                    && marker["source_cut"]["scope_shard"] == serde_json::json!(shard)
            }) {
                let exact_installed = marker["authority_binding"].as_str()
                    == Some(plan.expected_handoff.handoff_id.as_str())
                    && marker["native_image"]["prefix_proof"]
                        == serde_json::json!(plan.target_exact_prefix)
                    && marker["native_image"]["state_fingerprint"]
                        == serde_json::json!(plan.current_image_fingerprint);
                let installed_assignment: Option<DfsMetadataShardConfig> = marker
                    .get("installed_native_assignment")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(error)?;
                let superseded = installed_assignment.as_ref().is_some_and(|installed| {
                    installed.file_system_id == plan.expected_handoff.desired_shard.file_system_id
                        && installed.shard_id == plan.expected_handoff.desired_shard.shard_id
                        && installed.epoch > plan.expected_handoff.desired_shard.epoch
                });
                if exact_installed || superseded {
                    continue;
                }
            }
            // Files select a possible receiver only. received() obtains the real
            // current assignment, activation, native receipts and exact ancestry.
            return Ok(true);
        }
        Ok(false)
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
        let legacy_local =
            crate::metadata::legacy_receiver_prefix(&source.source_root, source_exact_prefix)?;
        if legacy_local.is_some()
            && source.source_root.canonicalize().map_err(error)?
                != self
                    .metadata_root
                    .join(&source.scope_shard.0)
                    .canonicalize()
                    .map_err(error)?
        {
            return Err(error(
                "legacy receiver source is not its actual owned shard",
            ));
        }
        let source_fences = self.validate_local_source_floors(
            &target.file_system_id,
            &source.scope_shard,
            &plan.source_cut,
            legacy_local.as_ref().map(|(_, prefix)| prefix),
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
        let stage_commit =
            self.observe_original_import_commit(&plan, plan.stage_request.clone())?;
        Self::require_committed(&stage_commit)?;
        let activation_commit = self.observe_original_import_commit(&plan, request.clone())?;
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
        let legacy_local_source =
            legacy_local.map(
                |(format, prefix)| crate::metadata::VerifiedLegacyReceiverSource {
                    original_root: source.source_root.clone(),
                    actual_root: source.source_root.clone(),
                    source_identity: source.source_identity.clone(),
                    snapshot_fingerprint: source.source_snapshot_fingerprint.clone(),
                    source_format_version: format,
                    source_prefix: prefix,
                    source_fences,
                },
            );
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
            legacy_local_source,
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
        let local = self.local_node()?;
        if local == plan.expected_handoff.retiring_shard.primary_node_id {
            // The original primary recovers its own certified exchange through
            // certify_recovered. Its conversion stage is not a received image
            // and does not own a receiver ancestry challenge. Node identity,
            // never pathname equality or missing files, selects this role.
            return Ok(None);
        }
        if !proof.acknowledged_node_ids.contains(&local) {
            return Ok(None);
        }
        let legacy_local = crate::metadata::legacy_receiver_prefix(
            actual_source_root,
            proposal.source_exact_prefix.as_ref(),
        )?;
        let source_fences = self.verify_recovered_source_ancestry(
            &plan,
            provenance_plan_root,
            actual_source_root,
            &proposal.source_root,
            legacy_local.as_ref().map(|(_, prefix)| prefix),
        )?;
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
        let stage_commit =
            self.observe_original_import_commit(&plan, plan.stage_request.clone())?;
        Self::require_committed(&stage_commit)?;
        let activation_commit = self.observe_original_import_commit(&plan, request)?;
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
        let legacy_local_source =
            legacy_local.map(
                |(format, prefix)| crate::metadata::VerifiedLegacyReceiverSource {
                    original_root: proposal.source_root.clone(),
                    actual_root: actual_source_root.to_owned(),
                    source_identity: proposal.source_identity.clone(),
                    snapshot_fingerprint: proposal.source_snapshot_fingerprint.clone(),
                    source_format_version: format,
                    source_prefix: prefix,
                    source_fences,
                },
            );
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
            legacy_local_source,
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
            legacy_local_source: None,
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
    fn native_released_seven_field_namespace_source_reopens_with_current_authority() {
        native_released_namespace_source_reopens(false, false, false, false);
    }

    #[test]
    fn native_original_write_plan_survives_a_released_version_without_durability() {
        native_released_namespace_source_reopens(true, false, false, false);
    }

    #[test]
    fn native_original_publication_grant_survives_real_same_version_placement() {
        native_released_namespace_source_reopens(true, true, false, false);
    }

    #[test]
    fn native_interrupted_metadata_prepare_recovers_after_cold_conversion() {
        native_released_namespace_source_reopens(true, true, true, false);
    }

    #[test]
    fn native_certified_root_exchange_fences_retained_bound_backend_and_recovers_cold() {
        native_released_namespace_source_reopens(false, false, false, true);
    }

    #[test]
    fn native_metadata_preparation_advances_multiple_batches_before_ready() {
        native_released_namespace_source_reopens_scaled(false, false, false, false, 256, None);
    }

    #[test]
    fn native_production_sized_metadata_preparation_keeps_original_browser_deadline() {
        // An additional production-cardinality fixture, not a replacement for
        // the existing 256-file test with its original five-second deadline.
        native_released_namespace_source_reopens_with_browser_budget(
            false,
            false,
            false,
            false,
            3072,
            None,
            Some(std::time::Duration::from_secs(60)),
        );
    }

    #[test]
    fn native_candidate_checkpoint_restarts_with_exact_source_and_real_authority() {
        native_released_namespace_source_reopens_scaled(
            false,
            false,
            false,
            false,
            0,
            Some("source-cut.json"),
        );
    }

    #[test]
    fn native_proposal_checkpoint_restarts_with_exact_source_and_real_authority() {
        native_released_namespace_source_reopens_scaled(
            false,
            false,
            false,
            false,
            0,
            Some("import-proposal.json"),
        );
    }

    fn native_released_namespace_source_reopens(
        omit_source_durability: bool,
        same_version_placement: bool,
        interrupt_metadata_confirmation: bool,
        retained_root_exchange: bool,
    ) {
        native_released_namespace_source_reopens_scaled(
            omit_source_durability,
            same_version_placement,
            interrupt_metadata_confirmation,
            retained_root_exchange,
            0,
            None,
        );
    }

    fn native_released_namespace_source_reopens_scaled(
        omit_source_durability: bool,
        same_version_placement: bool,
        interrupt_metadata_confirmation: bool,
        retained_root_exchange: bool,
        additional_source_files: usize,
        pause_finalization: Option<&'static str>,
    ) {
        native_released_namespace_source_reopens_with_browser_budget(
            omit_source_durability,
            same_version_placement,
            interrupt_metadata_confirmation,
            retained_root_exchange,
            additional_source_files,
            pause_finalization,
            None,
        );
    }

    fn native_released_namespace_source_reopens_with_browser_budget(
        omit_source_durability: bool,
        same_version_placement: bool,
        interrupt_metadata_confirmation: bool,
        retained_root_exchange: bool,
        additional_source_files: usize,
        pause_finalization: Option<&'static str>,
        production_browser_budget: Option<std::time::Duration>,
    ) {
        use sinomo_core::object::{LengthMode, serde_ext};
        use sinomo_dfs::{
            DfsMetadataShardId, DfsNamespaceObjectVersionExpectation,
            DfsNamespacePathOccupantExpectation, DfsNamespaceTransaction,
            DfsPreparedOperationStatus, test_support::DfsTestRuntime,
        };
        let name = if production_browser_budget.is_some() {
            "native-production-sized-metadata-preparation"
        } else if pause_finalization == Some("source-cut.json") {
            "native-finalization-candidate-restart"
        } else if pause_finalization.is_some() {
            "native-finalization-proposal-restart"
        } else if additional_source_files > 0 {
            "native-metadata-preparation-multiple-batches"
        } else if retained_root_exchange {
            "native-certified-retained-root-exchange-cold"
        } else if interrupt_metadata_confirmation {
            "native-interrupted-metadata-prepare-cold-conversion"
        } else if same_version_placement {
            "native-original-grant-after-real-placement"
        } else if omit_source_durability {
            "native-original-write-plan-cold-admission"
        } else {
            "native-seven-field-namespace-cold-admission"
        };
        let nodes = DfsTestRuntime::ordered_node_ids(
            name,
            if same_version_placement || retained_root_exchange {
                2
            } else {
                1
            },
        );
        let node = nodes[0].clone();
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        let pending_batches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observe_pending = Arc::clone(&pending_batches);
        let hold_checkpoint = Arc::new(std::sync::atomic::AtomicBool::new(
            pause_finalization.is_some(),
        ));
        let checkpoint: Arc<Mutex<Option<(PathBuf, PathBuf, Vec<u8>)>>> =
            Arc::new(Mutex::new(None));
        let provider_hold = Arc::clone(&hold_checkpoint);
        let provider_checkpoint = Arc::clone(&checkpoint);
        let builder: sinomo_dfs::DfsStoragePreparationProviderBuilder =
            Arc::new(move |context, lifecycle| {
                let provider = storage_preparation_provider(context, lifecycle)?;
                let pending = Arc::clone(&observe_pending);
                let hold = Arc::clone(&provider_hold);
                let observed = Arc::clone(&provider_checkpoint);
                Ok(Arc::new(move |root, scope, budget| {
                    if hold.load(std::sync::atomic::Ordering::SeqCst)
                        && observed
                            .lock()
                            .unwrap()
                            .as_ref()
                            .is_some_and(|(actual, _, _)| actual == root)
                    {
                        // Negative admission only, after the real provider has
                        // completed and acknowledged its actual checkpoint.
                        return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                            processed_records: 0,
                        });
                    }
                    let result = provider(root, scope, budget)?;
                    if matches!(scope, sinomo_dfs::DfsStoragePreparationScope::Metadata(_)) {
                        if let sinomo_dfs::DfsStoragePreparation::Pending { processed_records } =
                            result
                        {
                            assert_eq!(budget, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET);
                            assert!(processed_records <= budget);
                            if processed_records > 0 {
                                pending.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                if hold.load(std::sync::atomic::Ordering::SeqCst)
                                    && let Some(name) = pause_finalization
                                    && let sinomo_dfs::DfsStoragePreparationScope::Metadata(shard) =
                                        scope
                                    && let Some(path) =
                                        crate::metadata::finalization_checkpoint_for_test(
                                            root, shard, name,
                                        )?
                                {
                                    let bytes = std::fs::read(&path).unwrap();
                                    *observed.lock().unwrap() =
                                        Some((root.to_owned(), path, bytes));
                                }
                            }
                        }
                    }
                    Ok(result)
                }))
            });
        native
            .configure_component_factories(vec![
                Arc::new(sinomo_dfs::component_factory_with_storage_preparation(builder).unwrap()),
                Arc::new(staged_import_component_factory().unwrap()),
            ])
            .unwrap();
        for peer in &nodes[1..] {
            native.define_node(peer.clone()).unwrap();
        }
        native.start_nodes().unwrap();
        if retained_root_exchange {
            // Earn a real durable epoch bind using the ordinary joint handoff,
            // not a test setter. Return the real primary to this source owner.
            let fs = native.file_system().file_system_id;
            native
                .reassign_metadata_shard(&fs, "root", nodes[1].clone(), vec![node.clone()])
                .unwrap();
            native
                .reassign_metadata_shard(&fs, "root", node.clone(), vec![nodes[1].clone()])
                .unwrap();
        }
        let path = "/native-seven-field-preserved";
        let first = b"normal native content before released-source conversion";
        if same_version_placement {
            let mut policy = sinomo_dfs::DfsPlacementPolicy::new(
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                1,
                2,
            )
            .unwrap();
            policy.required_node_ids = nodes.clone();
            native
                .apply_placement_policy(
                    &node,
                    path,
                    sinomo_dfs::DfsPlacementScope::SelfOnly,
                    policy,
                )
                .unwrap();
        }
        let options = if same_version_placement {
            sinomo_dfs::DfsWriteOptions {
                replica_candidates: nodes.clone(),
                policy: sinomo_dfs::DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            }
        } else {
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()])
        };
        let mut client = if retained_root_exchange {
            native.metadata_primary_service(path).unwrap()
        } else {
            native.client(&node).unwrap()
        };
        let object = client
            .posix_write_file(path, first, options.clone())
            .unwrap();
        if additional_source_files > 0 {
            client.posix_mkdir_all("/preparation-records").unwrap();
            let paths = (0..additional_source_files)
                .map(|index| format!("/preparation-records/file-{index:04}"))
                .collect::<Vec<_>>();
            for chunk in paths.chunks(64) {
                let committed = client
                    .posix_write_files_local_eventual_with_known_parents(chunk.iter().map(|path| {
                        (
                            path.as_str(),
                            b"native bounded-preparation source".as_slice(),
                        )
                    }))
                    .unwrap();
                assert_eq!(committed.len(), chunk.len());
            }
        }

        if same_version_placement {
            client
                .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
                .unwrap();
        }
        let retained_backend = if retained_root_exchange {
            let actual = native.root_shard().unwrap();
            let backend = client.test_into_retained_metadata_backend().unwrap();
            let held = backend
                .native_held_snapshot_for_scope(
                    &actual.file_system_id,
                    &DfsMetadataShardId::new(&actual.shard_id),
                )
                .expect("actual normal bound backend was opened");
            assert_eq!(held.backend_instance_authority_epoch, Some(actual.epoch));
            Some(backend)
        } else {
            drop(client);
            None
        };
        let placement = if same_version_placement {
            let mut policy = sinomo_dfs::DfsPlacementPolicy::new(
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                1,
                1,
            )
            .unwrap();
            policy.required_node_ids = vec![node.clone()];
            policy.forbidden_node_ids = vec![nodes[1].clone()];
            let job = native
                .apply_placement_policy(
                    &node,
                    path,
                    sinomo_dfs::DfsPlacementScope::SelfOnly,
                    policy,
                )
                .unwrap();
            let control = native.shared_control_plane().unwrap();
            let deadline =
                std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            loop {
                let current = control
                    .with_client(|kv| {
                        sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut()).placement_reconciliation(
                            &sinomo_dfs::sifs_file_system_id(),
                            &job.job_id,
                        )
                    })
                    .unwrap()
                    .unwrap();
                if current.state == sinomo_dfs::DfsPlacementReconciliationState::Completed {
                    break;
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "normal placement must finish within its existing network budget"
                );
                let pass = native.maintenance_tick(&node, remaining).unwrap();
                assert!(
                    pass.outcome.is_ok(),
                    "actual normal served placement: {:?}",
                    pass.outcome
                );
            }
            Some(job.job_id)
        } else {
            None
        };
        let mut interrupted_body = None;
        if interrupt_metadata_confirmation {
            let mut source_owner = native.metadata_primary_service(path).unwrap();
            let mut changed = source_owner
                .metadata_backend()
                .unwrap()
                .get_object(&object.object_id)
                .unwrap()
                .unwrap();
            // A same-version metadata annotation, as used by ordinary reads.
            changed.accessed_at_unix_nanos += 1;
            native.install_fault(sinomo_dfs::test_support::DfsTestFaultRule {
                source: Some(node.clone()),
                target: Some(nodes[1].clone()),
                route: None,
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: sinomo_dfs::test_support::DfsTestFaultAction::BudgetExhausted,
            });
            let result = source_owner.add_object_metadata(changed.clone());
            assert!(
                matches!(
                    result,
                    Err(sinomo_dfs::DfsError::MetadataRecoveryQuorumUnavailable {
                        required: 2,
                        verified: 1,
                        ..
                    })
                ),
                "actual missing participant must deny publication: {result:?}"
            );
            interrupted_body = Some(changed);
            drop(source_owner);
        }
        let assignment = native.root_shard().unwrap();
        assert_eq!(assignment.primary_node_id, node);
        let root = native.data_dir(&node).dfs_dir().join("metadata/root");
        let baseline = native
            .data_dir(&node)
            .root()
            .join("native-current-prefix-baseline");
        native.stop_node(&node).unwrap();
        if interrupt_metadata_confirmation {
            native.clear_faults();
        }
        assert!(crate::metadata::current_root(&root).unwrap());
        // The normal host has drained its writers. The existing strict kernel
        // certifies/replays the genuine journal before exporting the complete cut.
        let runtime = native.runtime_handle();
        let entered = runtime.enter();
        let observed = sinomo_dfs::LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(&assignment.shard_id),
            &root,
        )
        .unwrap();
        let snapshot = observed.export_table_snapshot();
        if additional_source_files > 0 {
            assert!(snapshot.tables.objects.len() >= additional_source_files);
            assert!(snapshot.tables.versions.len() >= additional_source_files);
            eprintln!(
                "ACTUAL_METADATA_SOURCE objects={} versions={} receipts={}",
                snapshot.tables.objects.len(),
                snapshot.tables.versions.len(),
                snapshot.tables.receipts.len()
            );
        }

        let exchange_source_checkpoint = retained_root_exchange.then(|| {
            // An image owns a staged file at the original root pathname. Copy
            // it while this stopped-source kernel and its real export are live,
            // before releasing the kernel or relocating the original root.
            let image = observed.export_snapshot_image().unwrap();
            assert_eq!(
                image.materialized_through_sequence,
                snapshot.materialized_through_sequence
            );
            assert_eq!(image.prefix_proof, snapshot.prefix_proof);
            let checkpoint = native
                .data_dir(&node)
                .root()
                .join("native-root-exchange-full-checkpoint-image");
            sinomo_dfs::write_metadata_candidate_image(
                &checkpoint,
                &image,
                &assignment.file_system_id,
            )
            .unwrap();
            let copied = sinomo_dfs::inspect_metadata_candidate(&checkpoint).unwrap();
            assert_eq!(copied.prefix_proof, image.prefix_proof);
            assert_eq!(copied.state_fingerprint, image.semantic_state_fingerprint);
            checkpoint
        });
        let interrupted_record = interrupted_body.as_ref().map(|changed| {
            let actual = snapshot.tables.prepared_operations_v7.iter().find(|record|
                record.status == DfsPreparedOperationStatus::Prepared && record.metadata_effects.iter().any(|effect|
                    matches!(effect, sinomo_dfs::DfsPreparedMetadataEffect::PutObject {object} if object == changed)))
                .expect("actual native body must be Prepared at the stopped source cut");
            actual.validate().unwrap();
            actual.clone()
        });
        assert!(snapshot.materialized_through_sequence > 0);
        assert_eq!(
            observed.journal_last_sequence(),
            snapshot.materialized_through_sequence
        );
        assert!(
            observed
                .journal_since_limited(snapshot.materialized_through_sequence, 1)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            snapshot.prefix_proof.journal_sequence,
            snapshot.materialized_through_sequence
        );
        assert!(
            snapshot
                .tables
                .objects
                .iter()
                .any(|row| row.object_id == object.object_id && row.version == object.version)
        );
        let original_version = snapshot
            .tables
            .versions
            .iter()
            .find(|row| row.object_id == object.object_id && row.version == object.version)
            .unwrap()
            .clone();
        let original_receipts = snapshot
            .tables
            .receipts
            .iter()
            .filter(|row| {
                row.receipt.object_id == object.object_id
                    && row.receipt.committed_version == object.version
            })
            .cloned()
            .collect::<Vec<_>>();
        let original_fenced = snapshot
            .tables
            .prepared_operations_v7
            .iter()
            .filter(|row| row.payload.expected_journal_sequence.is_some())
            .cloned()
            .collect::<Vec<_>>();
        if same_version_placement {
            assert!(
                !original_fenced.is_empty(),
                "normal placement must produce a real journal-fenced operation"
            );
            for record in &original_fenced {
                let source = serde_json::to_value(record).unwrap();
                crate::legacy_wire::validate_original_v7_record(&source).unwrap();
                let mut wrong_slot = source;
                wrong_slot["digest"]["prepare_sequence"] =
                    serde_json::json!(record.digest.prepare_sequence + 1);
                assert!(
                    crate::legacy_wire::validate_original_v7_record(&wrong_slot)
                        .unwrap_err()
                        .to_string()
                        .contains("journal-cut precondition")
                );
            }
        }
        let original_intents = snapshot.tables.intents.clone();
        let original_intent_receipts = snapshot.tables.intent_receipts.clone();
        assert!(original_version.content_durability.is_some());
        if let Some(job_id) = placement.as_ref() {
            let id = format!("placement:{job_id}:{}:{}", object.object_id, object.version);
            let actual = snapshot
                .tables
                .intents
                .iter()
                .find(|row| row.intent_id == id)
                .unwrap();
            assert_eq!(actual.status, sinomo_dfs::DfsWriteIntentStatus::Committed);
            assert_eq!(actual.base_version.as_ref(), Some(&object.version));
            assert_eq!(actual.target_version, object.version);
            assert!(actual.operation_id.is_none());
            assert_eq!(actual.desired_replica_node_ids, vec![node.clone()]);
            assert_eq!(actual.policy.desired_replicas, 1);
            let publication = snapshot
                .tables
                .intent_receipts
                .iter()
                .find(|row| {
                    row.object_id == object.object_id
                        && row.target_version == object.version
                        && row.base_version.as_ref() != Some(&object.version)
                        && row.status == sinomo_dfs::DfsWriteIntentStatus::Committed
                })
                .unwrap();
            assert_eq!(publication.policy.desired_replicas, 2);
            assert_eq!(
                original_version
                    .content_durability
                    .as_ref()
                    .unwrap()
                    .policy
                    .desired_replicas,
                2
            );
            assert_ne!(actual.policy, publication.policy);
        }
        assert!(!original_receipts.is_empty());
        drop(observed);
        drop(entered);
        // Keep every genuine native file. This modeled released SOURCE checkpoint
        // copies ALL current table rows; it is not a new journal ACK or author permit.
        std::fs::rename(&root, &baseline).unwrap();
        let expected_objects = vec![DfsNamespaceObjectVersionExpectation {
            object_id: object.object_id.clone(),
            expected_version: Some(object.version.clone()),
        }];
        let expected_paths = vec![DfsNamespacePathOccupantExpectation {
            path: path.trim_matches('/').into(),
            expected_object_id: Some(object.object_id.clone()),
        }];
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            transaction_id: "released-seven-no-effect".into(),
            file_system_id: assignment.file_system_id.clone(),
            objects: Vec::new(),
            versions: Vec::new(),
            file_version_pins: None,
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        };
        let operation = "namespace:released-seven-aborted-read-set";
        // Actual July 17 producer: seven ordered typed fields, no outcome attachment.
        // Independently serialize that contract; never use the verifier as oracle.
        let original_digest = format!(
            "blake3:{}",
            blake3::hash(
                &serde_json::to_vec(&(
                    operation,
                    &assignment.file_system_id,
                    &assignment.shard_id,
                    assignment.epoch,
                    &expected_objects,
                    &expected_paths,
                    &transaction,
                ))
                .unwrap()
            )
            .to_hex()
        );
        let original_prepare = serde_json::json!({
            "operation_id":operation,"file_system_id":assignment.file_system_id,"shard_id":assignment.shard_id,
            "owner_epoch":assignment.epoch,"expected_object_versions":expected_objects,"expected_path_occupants":expected_paths,
            "transaction":transaction,"operation_digest":original_digest,"content_receipts":[],"status":"aborted"
        });
        let mut source = serde_json::to_value(&snapshot).unwrap();
        source["format_version"] = serde_json::json!(2);
        source["tables"]["namespace_prepares"] = serde_json::json!([original_prepare]);
        if omit_source_durability {
            let version = source["tables"]["versions"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|row| {
                    row["object_id"] == object.object_id.as_str()
                        && row["version"] == object.version.as_str()
                })
                .unwrap();
            assert!(
                version
                    .as_object_mut()
                    .unwrap()
                    .remove("content_durability")
                    .is_some()
            );
        }
        // Only the SOURCE representation checksum changes. Every logical row
        // and the real cut remain; all original native files stay in baseline.
        let mut checksum = source.clone();
        checksum
            .as_object_mut()
            .unwrap()
            .remove("semantic_state_fingerprint");
        checksum.as_object_mut().unwrap().remove("prefix_proof");
        source["semantic_state_fingerprint"] = serde_json::json!(format!(
            "blake3:{}",
            blake3::hash(&serde_json::to_vec(&checksum).unwrap()).to_hex()
        ));
        let source_path = root.join("records/tables/snapshot.sobj");
        std::fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        let source_bytes = if let Some(checkpoint) = exchange_source_checkpoint {
            use sinomo_core::storage::migration::stream::{
                MigrationStreamReader, MigrationStreamWriter, StreamCursor,
            };
            #[derive(Serialize)]
            struct Released3Header<'a> {
                format_version: u16,
                materialized_through_sequence: Option<u64>,
                prefix_proof: Option<&'a sinomo_dfs::DfsMetadataPrefixProof>,
                generation: String,
            }
            // Released 9c0287c6 SnapshotHeader/snapshot_header: the physical
            // stream keeps its own real V2 clock and exact generation binding.
            // Its original native row bytes and journal ancestry are unchanged.
            let header = Released3Header {
                format_version: 3,
                materialized_through_sequence: Some(snapshot.materialized_through_sequence),
                prefix_proof: Some(&snapshot.prefix_proof),
                generation: blake3::hash(
                    &serde_json::to_vec(&(
                        3u16,
                        Some(snapshot.materialized_through_sequence),
                        Some(&snapshot.prefix_proof),
                    ))
                    .unwrap(),
                )
                .to_hex()
                .to_string(),
            };
            let mut reader =
                MigrationStreamReader::open(&checkpoint.join("records/tables/snapshot.sobj"))
                    .unwrap();
            let mut writer = MigrationStreamWriter::resume(
                &source_path,
                &serde_ext::to_bytes(&header, 3, LengthMode::U32).unwrap(),
                &StreamCursor::default(),
            )
            .unwrap();
            let mut rows = Vec::new();
            while let Some(row) = reader.next_record(64 * 1024 * 1024).unwrap() {
                writer.append(&row).unwrap();
                rows.push(row);
            }
            assert!(!rows.is_empty());
            // The independently serialized seven-field namespace source body
            // remains exactly the same conversion DATA in the released stream.
            let namespace = serde_json::json!({"Table":{"op":{
                "type":"put-namespace-prepare",
                "prepare":source["tables"]["namespace_prepares"][0]
            }}});
            let namespace = serde_ext::to_bytes(&namespace, 3, LengthMode::U32).unwrap();
            writer.append(&namespace).unwrap();
            rows.push(namespace);
            writer.finish().unwrap();
            drop(writer);
            let mut copied = MigrationStreamReader::open(&source_path).unwrap();
            for row in rows {
                assert_eq!(copied.next_record(64 * 1024 * 1024).unwrap().unwrap(), row);
            }
            assert!(copied.next_record(64 * 1024 * 1024).unwrap().is_none());
            drop(copied);
            let actual = crate::metadata::legacy_source_checkpoint_header(&root)
                .unwrap()
                .expect("actual released physical source header");
            assert_eq!(actual.source_format_version, 3);
            assert_eq!(actual.prefix_proof, snapshot.prefix_proof);
            std::fs::read(&source_path).unwrap()
        } else {
            let bytes = serde_ext::to_bytes(&source, 1, LengthMode::U32).unwrap();
            std::fs::write(&source_path, &bytes).unwrap();
            bytes
        };
        if same_version_placement || retained_root_exchange {
            // Keep the actual original frame chain. A replica may have promised
            // an earlier floor than the captured snapshot; its ancestry must
            // reach this cut through real bytes, not a checkpoint/height claim.
            fn copy_original_journal(source: &Path, target: &Path) {
                std::fs::create_dir_all(target).unwrap();
                for entry in std::fs::read_dir(source).unwrap() {
                    let entry = entry.unwrap();
                    let path = target.join(entry.file_name());
                    let kind = entry.file_type().unwrap();
                    if kind.is_dir() {
                        copy_original_journal(&entry.path(), &path);
                    } else {
                        assert!(kind.is_file());
                        std::fs::copy(entry.path(), &path).unwrap();
                        assert_eq!(
                            std::fs::read(entry.path()).unwrap(),
                            std::fs::read(path).unwrap()
                        );
                    }
                }
            }
            copy_original_journal(&baseline.join("journal"), &root.join("journal"));
        }
        #[derive(Default)]
        struct RetainedExchangeFacts {
            old_backend: Option<Box<dyn sinomo_dfs::DfsMetadataBackend>>,
            proof: Option<sinomo_dfs::DfsMetadataShardMembershipHandoffProof>,
            target: Option<sinomo_dfs::DfsMetadataCandidateIdentity>,
            before_epoch: Option<u64>,
            before_prefix: Option<sinomo_dfs::DfsMetadataPrefixProof>,
            old_denied: bool,
            fresh_unbound_denied: bool,
            pending_seen: bool,
            published_seen: bool,
            foreign_seal_denied: bool,
            forked_target_denied: bool,
        }
        let exchange_facts = Arc::new(Mutex::new(RetainedExchangeFacts {
            old_backend: retained_backend,
            ..Default::default()
        }));
        let pause_completed_install =
            Arc::new(std::sync::atomic::AtomicBool::new(retained_root_exchange));
        let install_hook = retained_root_exchange.then(|| {
            let observed = Arc::clone(&exchange_facts);
            let paused = Arc::clone(&pause_completed_install);
            let requested_root = root.clone();
            crate::metadata::native_import_install_hook_for_test(
                &root,
                Arc::new(move |point, proposal, certificate, image_root, identity| {
                    use crate::metadata::NativeImportInstallTestPoint;
                    use sinomo_dfs::DfsMetadataBackend;
                    let crate::metadata::MetadataImportCertificate::Native(seal) = certificate
                    else {
                        panic!("normal clustered import must earn its native seal");
                    };
                    let proof = &seal.joint_handoff_proof;
                    let shard = sinomo_dfs::DfsMetadataShardId::new(&proof.desired_shard.shard_id);
                    let mut facts = observed.lock().unwrap();
                    facts.proof = Some(proof.clone()); // DATA only, no certificate/owner clone.
                    facts.target = Some(identity.clone());
                    if point == NativeImportInstallTestPoint::TargetFrozen {
                        let target = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                            shard.clone(),
                            image_root,
                        )?;
                        let freeze = target
                            .metadata_shard_write_freeze(&shard)?
                            .expect("actual target freeze");
                        assert_eq!(freeze.handoff_id, proof.handoff_id);
                        assert_eq!(freeze.retiring_epoch, proof.retiring_shard.epoch);
                        assert_eq!(freeze.activated_epoch, Some(proof.desired_shard.epoch));
                        assert_eq!(
                            freeze.journal_sequence,
                            identity.prefix_proof.journal_sequence
                        );
                        assert_eq!(freeze.state_fingerprint, identity.prefix_proof.digest);
                        assert_eq!(freeze.rebind_prefix_proof, identity.prefix_proof);
                        facts.pending_seen = true;
                        // Negative field changes cannot produce another writer
                        // generation from this actual sealed candidate.
                        let mut foreign = proof.clone();
                        foreign.handoff_id.push_str("-foreign");
                        facts.foreign_seal_denied =
                            crate::metadata::freeze_native_import_candidate_for_test(
                                image_root, &shard, &foreign, identity,
                            )
                            .is_err();
                        let mut fork = proof.clone();
                        let sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { cut } =
                            &mut fork.cause
                        else {
                            panic!("actual native import cause");
                        };
                        cut.target_prefix.digest.push('0');
                        facts.forked_target_denied =
                            crate::metadata::freeze_native_import_candidate_for_test(
                                image_root, &shard, &fork, identity,
                            )
                            .is_err();
                    } else {
                        assert_eq!(image_root, requested_root);
                        let (held, payload, old_denied) = {
                            let old = facts
                                .old_backend
                                .as_mut()
                                .expect("actual retained old backend");
                            let held = old
                                .native_held_snapshot_for_scope(
                                    &proof.retiring_shard.file_system_id,
                                    &shard,
                                )
                                .expect("same retained backend projection");
                            assert_eq!(
                                held.backend_instance_authority_epoch,
                                Some(proof.retiring_shard.epoch)
                            );
                            let fs = held
                                .tables
                                .file_systems
                                .iter()
                                .find(|fs| fs.file_system_id == proof.retiring_shard.file_system_id)
                                .expect("actual retained filesystem")
                                .clone();
                            let payload =
                                sinomo_dfs::DfsPreparedOperationPayload::for_metadata_effects(
                                    sinomo_dfs::DfsPreparedOperationScope {
                                        file_system_id: proof.retiring_shard.file_system_id.clone(),
                                        shard_id: shard.clone(),
                                        owner_epoch: proof.retiring_shard.epoch,
                                        operation_id:
                                            "retained-old-writer-after-real-native-exchange".into(),
                                    },
                                    Vec::new(),
                                    Vec::new(),
                                    vec![sinomo_dfs::DfsPreparedMetadataEffect::PutFileSystem {
                                        file_system: fs,
                                    }],
                                    time::OffsetDateTime::now_utc(),
                                )?;
                            let old_denied = old.prepare_operation(payload.clone(), None).is_err();
                            (held, payload, old_denied)
                        };
                        if facts.before_epoch.is_none() {
                            facts.before_epoch = held.backend_instance_authority_epoch;
                            facts.before_prefix = held.certified_prefix.clone();
                        }
                        let mut fresh = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                            shard.clone(),
                            image_root,
                        )?;
                        let before = fresh.metadata_prefix_proof(&shard)?;
                        let fresh_unbound_denied =
                            fresh.prepare_operation(payload.clone(), None).is_err();
                        assert_eq!(fresh.metadata_prefix_proof(&shard)?, before);
                        assert!(
                            fresh
                                .prepared_operation_digest(&payload.operation_id)?
                                .is_none()
                        );
                        facts.old_denied = if facts.published_seen {
                            facts.old_denied && old_denied
                        } else {
                            old_denied
                        };
                        facts.fresh_unbound_denied = if facts.published_seen {
                            facts.fresh_unbound_denied && fresh_unbound_denied
                        } else {
                            fresh_unbound_denied
                        };
                        facts.published_seen = true;
                        assert_eq!(proposal.scope_shard, shard);
                        if paused.load(std::sync::atomic::Ordering::Acquire) {
                            // This stops only the test invocation after a genuine
                            // certificate/exchange. Existing startup budget stays
                            // unchanged; a later cold owner must recover the intent.
                            return Err(
                                sinomo_dfs::MetadataBackendError::StoragePreparationPending {
                                    root: requested_root.clone(),
                                    processed_records: 0,
                                },
                            );
                        }
                    }
                    Ok(())
                }),
            )
        });
        if retained_root_exchange {
            assert!(
                native.reopen_node(node.clone()).is_err(),
                "unfinished genuine installation cannot be Ready"
            );
            assert!(exchange_facts.lock().unwrap().published_seen);
            assert!(
                !serde_json::from_slice::<serde_json::Value>(
                    &std::fs::read(root.join("metadata-import-v7.json")).unwrap()
                )
                .unwrap()["installation_complete"]
                    .as_bool()
                    .unwrap()
            );
            native.stop_node(&node).unwrap();
            pause_completed_install.store(false, std::sync::atomic::Ordering::Release);
        }
        assert!(!crate::metadata::current_root(&root).unwrap() || retained_root_exchange);
        if production_browser_budget.is_some() {
            let records = crate::metadata::count_stopped_source_records_for_test(&root).unwrap();
            assert!(
                records >= 19_808,
                "fixture must cover the retained real root's record count: {records}"
            );
            eprintln!("PRODUCTION_SHAPE_SOURCE records={records} files={additional_source_files}");
        }
        let retained_checkpoint = if pause_finalization.is_some() {
            let blocked = native.reopen_node(node.clone());
            assert!(
                blocked.is_err(),
                "a checkpoint alone cannot make native storage ready"
            );
            let (actual_root, path, bytes) = checkpoint
                .lock()
                .unwrap()
                .clone()
                .expect("real provider reached the selected durable checkpoint");
            assert_eq!(actual_root, root);
            assert!(!crate::metadata::current_root(&root).unwrap());
            let competing = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.join("journal/write.lock"))
                .unwrap();
            assert_eq!(
                fs2::FileExt::try_lock_exclusive(&competing)
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::WouldBlock,
                "Pending retains the actual source writer fence"
            );
            drop(competing);
            native.stop_node(&node).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            hold_checkpoint.store(false, std::sync::atomic::Ordering::SeqCst);
            Some((path, bytes))
        } else {
            None
        };
        let recovery_started = std::time::Instant::now();
        let recovered = if let Some(budget) = production_browser_budget {
            native.reopen_node_with_startup_deadline(
                node.clone(),
                tokio::time::Instant::now() + budget,
            )
        } else {
            native.reopen_node(node.clone())
        };
        if let Some(budget) = production_browser_budget {
            eprintln!(
                "PRODUCTION_SHAPE_RECOVERY elapsed_ms={} budget_ms={} result={recovered:?}",
                recovery_started.elapsed().as_millis(),
                budget.as_millis()
            );
            assert!(
                recovery_started.elapsed() < budget,
                "original browser completion deadline"
            );
        }
        recovered.expect("normal cold Main and real MIG provider must admit the released source");
        if let Some((path, original)) = retained_checkpoint {
            assert_eq!(
                std::fs::read(path).unwrap(),
                original,
                "restart must preserve the exact checkpoint and original native request identities"
            );
        }
        assert!(crate::metadata::current_root(&root).unwrap());
        let current = native.root_shard().unwrap();
        assert_eq!(current.file_system_id, assignment.file_system_id);
        assert_eq!(current.shard_id, assignment.shard_id);
        assert_eq!(current.primary_node_id, assignment.primary_node_id);
        assert!(current.epoch > assignment.epoch);
        let marker: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("metadata-import-v7.json")).unwrap())
                .unwrap();
        let retained = PathBuf::from(marker["retained_source_root"].as_str().unwrap());
        assert_eq!(
            std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap(),
            source_bytes
        );
        assert_eq!(
            marker["source_cut"]["source_exact_prefix"],
            serde_json::to_value(&snapshot.prefix_proof).unwrap()
        );
        let owner = native.metadata_primary_service(path).unwrap();
        if same_version_placement {
            let proof = native
                .shared_control_plane()
                .unwrap()
                .with_client(|kv| {
                    sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut())
                        .get_metadata_shard_membership_handoff_proof(
                            &current.file_system_id,
                            &current.shard_id,
                        )
                })
                .unwrap()
                .expect("actual native import completion");
            assert_eq!(proof.desired_shard, current);
            let sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { cut } = &proof.cause
            else {
                panic!("actual native import target clock");
            };
            let exact_target: sinomo_dfs::DfsMetadataPrefixProof =
                serde_json::from_value(marker["source_cut"]["target_exact_prefix"].clone())
                    .unwrap();
            assert_eq!(cut.target_prefix, exact_target);
            for peer in &current.replica_node_ids {
                if proof.acknowledged_node_ids.contains(peer) {
                    assert!(
                        owner
                            .metadata_backend()
                            .unwrap()
                            .metadata_replication_cursor(
                                peer,
                                &DfsMetadataShardId::new(&current.shard_id)
                            )
                            .unwrap()
                            >= exact_target.journal_sequence,
                        "real sealed target ACK supplies only the current publication suffix hint"
                    );
                }
            }
        }
        let imported = owner
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(operation)
            .unwrap()
            .unwrap();
        assert_eq!(imported.status, DfsPreparedOperationStatus::Aborted);
        assert_eq!(imported.payload.expected_object_versions, expected_objects);
        assert_eq!(imported.payload.expected_path_occupants, expected_paths);
        assert!(imported.payload.completed_request.is_none());
        assert!(imported.payload.completed_request_completion.is_none());
        if omit_source_durability {
            let snapshot = owner
                .metadata_backend()
                .unwrap()
                .export_table_snapshot()
                .unwrap();
            if same_version_placement {
                assert_eq!(snapshot.tables.intents, original_intents);
                assert_eq!(snapshot.tables.intent_receipts, original_intent_receipts);
                for original in &original_fenced {
                    let current = snapshot
                        .tables
                        .prepared_operations_v7
                        .iter()
                        .find(|row| row.digest.operation_id == original.digest.operation_id)
                        .unwrap();
                    current.validate().unwrap();
                    assert_eq!(
                        current.payload.expected_journal_sequence.unwrap() + 1,
                        current.digest.prepare_sequence
                    );
                    assert_ne!(
                        current.payload.expected_journal_sequence,
                        original.payload.expected_journal_sequence
                    );
                    let mapping = marker["source_cut"]["pending_operations"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|row| {
                            row["source_operation_digest"] == original.digest.payload_digest
                        })
                        .unwrap();
                    assert_eq!(
                        mapping["source_prepare_sequence"],
                        original.digest.prepare_sequence
                    );
                    assert_eq!(
                        mapping["source_payload"],
                        serde_json::to_value(original.payload.as_ref()).unwrap()
                    );
                    assert_eq!(
                        mapping["payload"],
                        serde_json::to_value(current.payload.as_ref()).unwrap()
                    );
                    assert_eq!(
                        mapping["current_digest"],
                        serde_json::to_value(&current.digest).unwrap()
                    );
                }
            }
            let version = snapshot
                .tables
                .versions
                .iter()
                .find(|row| row.object_id == object.object_id && row.version == object.version)
                .unwrap();
            assert_eq!(
                version.content_durability,
                original_version.content_durability
            );
            let receipts = snapshot
                .tables
                .receipts
                .iter()
                .filter(|row| {
                    row.receipt.object_id == object.object_id
                        && row.receipt.committed_version == object.version
                })
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                receipts, original_receipts,
                "schema conversion must preserve actual original receipt identities"
            );
        }
        drop(owner);
        let mut client = native.client(&node).unwrap();
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), first);
        if production_browser_budget.is_some() {
            // Check every original generated pathname and body after actual
            // native publication. No sampled subset or count-only acceptance.
            let mut checkpointed = native.checkpoint_rpc_observation_window().unwrap();
            for index in 0..additional_source_files {
                assert_eq!(
                    client
                        .posix_read_file_distributed(&format!(
                            "/preparation-records/file-{index:04}"
                        ))
                        .unwrap(),
                    b"native bounded-preparation source"
                );
                if (index + 1) % 64 == 0 {
                    checkpointed += native.checkpoint_rpc_observation_window().unwrap();
                }
            }
            checkpointed += native.checkpoint_rpc_observation_window().unwrap();
            assert!(
                checkpointed > 4096,
                "full source validation exceeds one observation window"
            );
            assert_eq!(native.observation_dropped_count(), 0);
            eprintln!(
                "PRODUCTION_SHAPE_EXACT_READS files={additional_source_files} checkpointed_rpc_records={checkpointed} dropped=0"
            );
        }
        let after = b"normal native writer after released-source admission";
        let after_options = if same_version_placement {
            let (_, _, policy) = client
                .managed_replication_policy_for_path(
                    path,
                    sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                    nodes.len(),
                )
                .unwrap();
            assert_eq!(policy.desired_replicas, 1);
            sinomo_dfs::DfsWriteOptions {
                replica_candidates: nodes.clone(),
                policy,
                wait_for_commit: true,
                foreground_replica_repair: true,
            }
        } else {
            options
        };
        client.posix_write_file(path, after, after_options).unwrap();
        if retained_root_exchange {
            let proof = native
                .shared_control_plane()
                .unwrap()
                .with_client(|kv| {
                    sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut())
                        .get_metadata_shard_membership_handoff_proof(
                            &current.file_system_id,
                            &current.shard_id,
                        )
                })
                .unwrap()
                .expect("actual current native completion proof");
            let facts = exchange_facts.lock().unwrap();
            assert!(
                facts.pending_seen
                    && facts.published_seen
                    && facts.old_denied
                    && facts.fresh_unbound_denied
            );
            assert!(facts.foreign_seal_denied && facts.forked_target_denied);
            assert_eq!(facts.before_epoch, Some(assignment.epoch));
            assert_eq!(facts.proof.as_ref(), Some(&proof));
            assert_eq!(proof.desired_shard, current);
            let sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { cut } = &proof.cause
            else {
                panic!("normal import must preserve its sealed cause");
            };
            let target = facts.target.as_ref().unwrap();
            assert_eq!(cut.target_prefix, target.prefix_proof);
            assert_eq!(cut.converted_image_fingerprint, target.state_fingerprint);
            assert_ne!(facts.before_prefix.as_ref(), Some(&target.prefix_proof));
            let mut actual = native.metadata_primary_service(path).unwrap();
            // Opening may be lazy; a normal guarded write earns the existing bind.
            let rebound = actual.posix_write_file(
                "/native-certified-current-rebind",
                b"current writer",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            );
            rebound.unwrap();
            let held = actual
                .metadata_backend()
                .unwrap()
                .native_held_snapshot_for_scope(
                    &current.file_system_id,
                    &sinomo_dfs::DfsMetadataShardId::new(&current.shard_id),
                )
                .expect("actual rebound backend");
            assert_eq!(held.backend_instance_authority_epoch, Some(current.epoch));
            drop(actual);
        }
        if let Some(original) = &interrupted_record {
            let recovered = native.metadata_primary_service(path).unwrap();
            let actual = recovered
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&original.digest.operation_id)
                .unwrap()
                .unwrap();
            assert_eq!(actual.status, DfsPreparedOperationStatus::Superseded);
            assert_eq!(actual.payload, original.payload);
            let recovery_id = format!("recover:{}:{}", original.digest.operation_id, current.epoch);
            let replacement = recovered
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&recovery_id)
                .unwrap()
                .expect("guarded current author retains its exact replacement");
            replacement.validate().unwrap();
            assert!(replacement.metadata_effects.iter().any(|effect|
                matches!(effect, sinomo_dfs::DfsPreparedMetadataEffect::ResolvePreparedOperation {digest,status}
                    if digest == &actual.digest && *status == DfsPreparedOperationStatus::Superseded)));
            assert_eq!(replacement.status, DfsPreparedOperationStatus::Committed);
        }
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), after);
        drop(client);
        native.stop_node(&node).unwrap();
        native.reopen_node(node.clone()).unwrap();
        let mut client = native.client(&node).unwrap();
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), after);
        assert_eq!(
            std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap(),
            source_bytes
        );
        assert!(crate::metadata::current_root(&baseline).unwrap());
        drop(client);
        drop(install_hook);
        drop(exchange_facts);
        if additional_source_files > 0 {
            assert!(
                pending_batches.load(std::sync::atomic::Ordering::SeqCst) >= 2,
                "actual metadata source must require multiple unchanged preparation batches"
            );
        }
        native.close().unwrap();
    }

    #[test]
    fn native_legacy3_users_receiver_checkpoint_excludes_independent_retained_root_clock() {
        use sinomo_core::object::{LengthMode, serde_ext};
        use sinomo_core::storage::migration::stream::{
            MigrationStreamReader, MigrationStreamWriter, StreamCursor,
        };
        use sinomo_dfs::DfsMetadataShardId;

        fn copy_original(source: &Path, target: &Path) {
            if !source.exists() {
                return;
            }
            std::fs::create_dir_all(target).unwrap();
            for entry in std::fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                let path = target.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    copy_original(&entry.path(), &path);
                } else {
                    assert!(entry.file_type().unwrap().is_file());
                    std::fs::copy(entry.path(), &path).unwrap();
                    assert_eq!(
                        std::fs::read(entry.path()).unwrap(),
                        std::fs::read(path).unwrap()
                    );
                }
            }
        }
        #[derive(Serialize)]
        struct Released3Header<'a> {
            format_version: u16,
            materialized_through_sequence: Option<u64>,
            prefix_proof: Option<&'a sinomo_dfs::DfsMetadataPrefixProof>,
            generation: String,
        }

        let name = "native-legacy3-users-receiver-checkpoint-root-clock";
        let (mut native, nodes) = placement_work::native_tests::native_migration_cluster(name, 3);
        let fs = sinomo_dfs::sifs_file_system_id();
        let scope = "users";
        let users = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|cfg| cfg.shard_id == scope)
            .unwrap();
        assert_eq!(users.replica_node_ids.len(), 2);
        let primary = nodes[1].clone(); // Genuine receiving replica, ROOT source owner below.
        assert!(users.replica_node_ids.contains(&primary));
        let replicas = nodes
            .iter()
            .filter(|node| **node != primary)
            .cloned()
            .collect::<Vec<_>>();
        native
            .reassign_metadata_shard(&fs, "root", nodes[0].clone(), nodes[1..].to_vec())
            .unwrap();
        let path = "/users/native-local-original-checkpoint";
        let before = b"normal USERS body before modeled receiver checkpoint admission";
        let options = sinomo_dfs::DfsWriteOptions {
            policy: sinomo_dfs::DfsReplicationPolicy::new(2, 3).unwrap(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: true,
        };
        // Keep the actual default primary. Its normal native authority probes
        // leave real acknowledgements on receiving replicas, never on itself.
        let mut client = native.client(&users.primary_node_id).unwrap();
        let written = client
            .posix_write_file(path, before, options.clone())
            .unwrap();
        client
            .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
            .unwrap();
        drop(client);
        // ROOT advances through normal writes while the eventual source owner
        // really receives that independent stream as a replica.
        let mut root_client = native.client(&nodes[0]).unwrap();
        for index in 0..12 {
            let root_path = format!("/native-independent-root-floor-{index}");
            root_client
                .posix_write_file(&root_path, b"normal ROOT data", options.clone())
                .unwrap();
            root_client
                .posix_sync_file(&root_path, sinomo_dfs::DfsPosixSyncKind::File)
                .unwrap();
        }
        drop(root_client);
        let root_assignment = native
            .reassign_metadata_shard(&fs, "root", primary.clone(), replicas)
            .unwrap();
        assert_eq!(root_assignment.replica_node_ids.len(), 2);
        let metadata_root = native.data_dir(&primary).dfs_dir().join("metadata");
        let root_path = metadata_root.join("root");
        let root_floor = sinomo_dfs::read_current_metadata_acknowledgement_floors(
            &root_path,
            &DfsMetadataShardId::new("root"),
        )
        .unwrap();
        let root_height = root_floor
            .0
            .as_ref()
            .map(|floor| floor.journal_sequence)
            .into_iter()
            .chain(root_floor.1.as_ref().map(|floor| floor.journal_sequence))
            .max()
            .expect("normal receiving ROOT owner retained an actual acknowledgement");

        let context = native.component_context(&primary).unwrap();
        let actual = placement_work::native_tests::canonical_migration_runtime(&context);
        let (raft, _) = actual
            .authority
            .read_exact(&[sinomo_core::kv::cluster_kv_raft_config_key()])
            .unwrap();
        let raft: sinomo_core::kv::ClusterKvRaftConfigRecord =
            raft[0].entry.as_ref().unwrap().decode().unwrap();
        assert_eq!(raft.voters, vec![nodes[0].clone()]);
        assert_ne!(
            primary, nodes[0],
            "ordinary stage must cross the actual native authority"
        );
        drop(actual);
        drop(context);
        let submitted_stage = Arc::new(Mutex::new(None));
        let observed_stage = Arc::clone(&submitted_stage);
        let source_owner = primary.clone();
        let request_observer = native
            .native_kv_request_hook(&primary, move |request| {
                if let Some(handoff) = NativeImportAuthority::stage_record(request).unwrap()
                    && handoff.retiring_shard.shard_id == "root"
                    && handoff.retiring_shard.primary_node_id == source_owner
                    && matches!(
                        handoff.cause,
                        sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
                    )
                {
                    let mut captured = observed_stage.lock().unwrap();
                    if let Some(previous) = captured.as_ref() {
                        assert_eq!(previous, request);
                    } else {
                        *captured = Some(request.clone());
                    }
                }
                Ok(())
            })
            .unwrap();
        let forwarding_start = native.rpc_observations().len();
        // First earn a real ROOT import/archive marker using the existing plain
        // released-source fixture contract. No marker or native receipt is made.
        native.stop_node(&primary).unwrap();
        let runtime = native.runtime_handle();
        let entered = runtime.enter();
        let backend =
            sinomo_dfs::LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root_path)
                .unwrap();
        let root_snapshot = backend.export_table_snapshot();
        assert_eq!(
            backend.journal_last_sequence(),
            root_snapshot.materialized_through_sequence
        );
        drop(backend);
        drop(entered);
        let root_baseline = native
            .data_dir(&primary)
            .root()
            .join("native-root-current-baseline");
        std::fs::rename(&root_path, &root_baseline).unwrap();
        let mut source = serde_json::to_value(&root_snapshot).unwrap();
        source["format_version"] = serde_json::json!(2);
        let mut checksum = source.clone();
        checksum
            .as_object_mut()
            .unwrap()
            .remove("semantic_state_fingerprint");
        checksum.as_object_mut().unwrap().remove("prefix_proof");
        source["semantic_state_fingerprint"] = serde_json::json!(format!(
            "blake3:{}",
            blake3::hash(&serde_json::to_vec(&checksum).unwrap()).to_hex()
        ));
        let root_source_bytes = serde_ext::to_bytes(&source, 1, LengthMode::U32).unwrap();
        std::fs::create_dir_all(root_path.join("records/tables")).unwrap();
        std::fs::write(
            root_path.join("records/tables/snapshot.sobj"),
            &root_source_bytes,
        )
        .unwrap();
        copy_original(&root_baseline.join("journal"), &root_path.join("journal"));
        let mut original_root_floors = Vec::new();
        // Model the released OPTIONAL-proof ACK wire from actual observed
        // current rows. Their required-proof SOBJ tags cannot be copied into
        // the old optional field. Every original current file stays in baseline.
        for (kind, floor) in [
            (
                "prefix",
                root_floor.0.as_ref().map(|floor| {
                    crate::metadata::ack_floor::LegacyAcknowledgement {
                        shard_id: floor.shard_id.clone(),
                        journal_sequence: floor.journal_sequence,
                        state_fingerprint: floor.state_fingerprint.clone(),
                        durable_prefix_proof: Some(floor.durable_prefix_proof.clone()),
                    }
                }),
            ),
            (
                "authority",
                root_floor.1.as_ref().map(|floor| {
                    crate::metadata::ack_floor::LegacyAcknowledgement {
                        shard_id: floor.shard_id.clone(),
                        journal_sequence: floor.journal_sequence,
                        state_fingerprint: String::new(),
                        durable_prefix_proof: Some(floor.durable_prefix_proof.clone()),
                    }
                }),
            ),
        ] {
            if let Some(floor) = floor {
                original_root_floors.push(floor.clone());
                let path =
                    root_path.join(format!("coordination/metadata-{kind}-ack-floors/root.sobj"));
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                let bytes = serde_ext::to_bytes(&floor, 1, LengthMode::U32).unwrap();
                std::fs::write(&path, &bytes).unwrap();
                let observed: crate::metadata::ack_floor::LegacyAcknowledgement =
                    serde_ext::from_bytes(&std::fs::read(&path).unwrap()).unwrap();
                assert_eq!(observed, floor);
            }
        }
        // A normal snapshot repair may have archived all original segments.
        // Copying only journal/head and the freeze would omit a lower ACK's
        // required ancestry. Retain the real frames after checking every ACK
        // independently against this actual exported checkpoint.
        crate::metadata::retain_fixture_original_journal_ancestry(
            &root_baseline,
            &root_path,
            &DfsMetadataShardId::new("root"),
            &root_snapshot.prefix_proof,
            &original_root_floors,
        )
        .unwrap();
        native.reopen_node(primary.clone()).unwrap();
        let root_marker: serde_json::Value = serde_json::from_slice(
            &std::fs::read(root_path.join("metadata-import-v7.json")).unwrap(),
        )
        .unwrap();
        assert!(root_marker["installation_complete"].as_bool().unwrap());
        let retained_root = PathBuf::from(root_marker["retained_source_root"].as_str().unwrap());
        drop(request_observer);
        let original_request = submitted_stage
            .lock()
            .unwrap()
            .clone()
            .expect("actual ordinary stage submission");
        let (root_plan, _) = crate::metadata::retained_native_import_plan(
            &root_path,
            &fs,
            &DfsMetadataShardId::new("root"),
            root_marker["authority_binding"].as_str().unwrap(),
        )
        .unwrap()
        .expect("qualified installed original ROOT plan");
        assert_eq!(root_plan.stage_request, original_request);
        let observe_stage = |native: &sinomo_dfs::test_support::DfsTestRuntime| {
            let context = native.component_context(&primary).unwrap();
            let actual = placement_work::native_tests::canonical_migration_runtime(&context);
            let outcome = block_native(
                &actual.authority.runtime,
                actual
                    .authority
                    .cluster()
                    .unwrap()
                    .kv_request_outcome_authoritative_async(original_request.request_id.clone()),
            )
            .unwrap();
            sinomo_core::kv::validate_cluster_kv_request_outcome_read(&outcome).unwrap();
            current_authority(&outcome.authority).unwrap();
            let sinomo_core::kv::KvRequestOutcome::Committed(record) = outcome.outcome else {
                panic!("ordinary forwarded stage must retain its actual native receipt")
            };
            NativeImportAuthority::validate_original_command_preimage(
                &original_request,
                &primary,
                &root_plan.captured_entries,
                &record.command_digest,
            )
            .unwrap();
            record
        };
        let original_record = observe_stage(&native);
        assert_ne!(
            original_record.command_digest,
            sinomo_core::kv::kv_command_digest(&KvCommand::Txn(original_request.txn.clone()))
                .unwrap(),
            "normal Remote ingress must preserve its exact physical authorization suffix"
        );
        assert!(
            native.rpc_observations()[forwarding_start..]
                .iter()
                .any(|call| call.source == primary
                    && call.target == nodes[0]
                    && call.route == sinomo_core::kv::CLUSTER_KV_TXN_ROUTE
                    && call.phase == sinomo_dfs::test_support::DfsTestRpcPhase::Completed),
            "actual existing Hosted route must carry the ordinary forwarded stage"
        );
        assert_eq!(
            std::fs::read(retained_root.join("records/tables/snapshot.sobj")).unwrap(),
            root_source_bytes
        );
        assert!(native.root_shard().unwrap().epoch > root_assignment.epoch);

        // Model released3 DATA on the actual USERS primary and one genuine
        // receiving replica. The primary keeps every actual ACK anchor/absence;
        // the receiver models the released height-only authority representation.
        // All original current trees remain intact in separate baselines.
        let users = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|cfg| cfg.shard_id == scope)
            .unwrap();
        let source_primary = users.primary_node_id.clone();
        assert_ne!(source_primary, primary);
        assert!(users.replica_node_ids.contains(&primary));
        for node in &nodes {
            native.stop_node(node).unwrap();
        }
        let mut modeled = Vec::new();
        let mut receiver_checkpoint = None;
        for node in [&source_primary, &primary] {
            let users_path = native.data_dir(node).dfs_dir().join("metadata/users");
            let entered = runtime.enter();
            let backend = sinomo_dfs::LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new(scope),
                &users_path,
            )
            .unwrap();
            let snapshot = backend.export_table_snapshot();
            let image = backend.export_snapshot_image().unwrap();
            assert_eq!(
                backend.journal_last_sequence(),
                snapshot.materialized_through_sequence
            );
            assert!(snapshot.materialized_through_sequence > 0);
            assert!(root_height > snapshot.materialized_through_sequence);
            let actual = sinomo_dfs::read_current_metadata_acknowledgement_floors(
                &users_path,
                &DfsMetadataShardId::new(scope),
            )
            .unwrap();
            let is_receiver = node == &primary;
            if is_receiver {
                let authority = actual.1.as_ref().expect(
                    "normal USERS receiving replica retained its actual authority acknowledgement",
                );
                assert!(authority.journal_sequence > 0);
                assert!(authority.journal_sequence <= snapshot.materialized_through_sequence);
                receiver_checkpoint = Some(snapshot.prefix_proof.clone());
            }
            drop(backend);
            drop(entered);
            let checkpoint_image = native
                .data_dir(node)
                .root()
                .join("native-users-full-checkpoint-image");
            sinomo_dfs::write_metadata_candidate_image(&checkpoint_image, &image, &fs).unwrap();
            let baseline = native
                .data_dir(node)
                .root()
                .join("native-users-current-baseline");
            std::fs::rename(&users_path, &baseline).unwrap();
            let generation = blake3::hash(
                &serde_json::to_vec(&(
                    3u16,
                    Some(snapshot.materialized_through_sequence),
                    Some(&snapshot.prefix_proof),
                ))
                .unwrap(),
            )
            .to_hex()
            .to_string();
            let header = Released3Header {
                format_version: 3,
                materialized_through_sequence: Some(snapshot.materialized_through_sequence),
                prefix_proof: Some(&snapshot.prefix_proof),
                generation,
            };
            let source_path = users_path.join("records/tables/snapshot.sobj");
            std::fs::create_dir_all(source_path.parent().unwrap()).unwrap();
            let mut reader =
                MigrationStreamReader::open(&checkpoint_image.join("records/tables/snapshot.sobj"))
                    .unwrap();
            let mut writer = MigrationStreamWriter::resume(
                &source_path,
                &serde_ext::to_bytes(&header, 3, LengthMode::U32).unwrap(),
                &StreamCursor::default(),
            )
            .unwrap();
            let mut original_rows = Vec::new();
            while let Some(record) = reader.next_record(64 * 1024 * 1024).unwrap() {
                writer.append(&record).unwrap();
                original_rows.push(record);
            }
            assert!(!original_rows.is_empty());
            writer.finish().unwrap();
            drop(writer);
            let mut copied = MigrationStreamReader::open(&source_path).unwrap();
            for original in original_rows {
                assert_eq!(
                    copied.next_record(64 * 1024 * 1024).unwrap().unwrap(),
                    original
                );
            }
            assert!(copied.next_record(64 * 1024 * 1024).unwrap().is_none());
            drop(copied);
            copy_original(&baseline.join("journal"), &users_path.join("journal"));
            // The modeled released receiver has only its real authority
            // height; every current prefix ACK stays intact in the baseline.
            // Primary anchors/absence are preserved without this modeling.
            let prefix_floor = if is_receiver {
                None
            } else {
                actual
                    .0
                    .as_ref()
                    .map(|floor| crate::metadata::ack_floor::LegacyAcknowledgement {
                        shard_id: floor.shard_id.clone(),
                        journal_sequence: floor.journal_sequence,
                        state_fingerprint: floor.state_fingerprint.clone(),
                        durable_prefix_proof: Some(floor.durable_prefix_proof.clone()),
                    })
            };
            let authority_floor =
                actual
                    .1
                    .as_ref()
                    .map(|floor| crate::metadata::ack_floor::LegacyAcknowledgement {
                        shard_id: floor.shard_id.clone(),
                        journal_sequence: floor.journal_sequence,
                        state_fingerprint: String::new(),
                        durable_prefix_proof: if is_receiver {
                            None
                        } else {
                            Some(floor.durable_prefix_proof.clone())
                        },
                    });
            let mut recorded_floors = Vec::new();
            for (kind, floor) in [("prefix", prefix_floor), ("authority", authority_floor)] {
                if let Some(floor) = floor {
                    let floor_path = users_path.join(format!(
                        "coordination/metadata-{kind}-ack-floors/users.sobj"
                    ));
                    std::fs::create_dir_all(floor_path.parent().unwrap()).unwrap();
                    let bytes = serde_ext::to_bytes(&floor, 1, LengthMode::U32).unwrap();
                    std::fs::write(&floor_path, &bytes).unwrap();
                    let observed: crate::metadata::ack_floor::LegacyAcknowledgement =
                        serde_ext::from_bytes(&std::fs::read(&floor_path).unwrap()).unwrap();
                    assert_eq!(observed, floor);
                    recorded_floors.push((kind.to_owned(), bytes));
                }
            }
            if is_receiver {
                assert_eq!(recorded_floors.len(), 1);
            }
            assert!(!crate::metadata::current_root(&users_path).unwrap());
            modeled.push((
                (*node).clone(),
                users_path,
                std::fs::read(&source_path).unwrap(),
                recorded_floors,
                snapshot.tables.receipts.clone(),
                snapshot.prefix_proof,
            ));
        }
        // The existing public whole-roster start mounts every stopped component
        // before normal preparation; its KV bootstrap is already complete.
        native.start_nodes().unwrap();
        assert_eq!(
            observe_stage(&native),
            original_record,
            "cold USERS admission cannot restamp the original ROOT body/clock/outcome"
        );
        let current_users = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|cfg| cfg.shard_id == scope)
            .unwrap();
        assert_eq!(current_users.primary_node_id, source_primary);
        assert_eq!(current_users.replica_node_ids.len(), 2);
        assert!(current_users.epoch > users.epoch);
        let mut retained_sources = Vec::new();
        for (node, users_path, source_bytes, floors, _receipts, source_prefix) in &modeled {
            assert!(crate::metadata::current_root(users_path).unwrap());
            let marker: serde_json::Value = serde_json::from_slice(
                &std::fs::read(users_path.join("metadata-import-v7.json")).unwrap(),
            )
            .unwrap();
            assert!(marker["installation_complete"].as_bool().unwrap());
            let retained = PathBuf::from(marker["retained_source_root"].as_str().unwrap());
            assert_eq!(
                std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap(),
                *source_bytes
            );
            for (kind, bytes) in floors {
                assert_eq!(
                    std::fs::read(retained.join(format!(
                        "coordination/metadata-{kind}-ack-floors/users.sobj"
                    )))
                    .unwrap(),
                    *bytes
                );
            }
            assert_eq!(
                marker["source_cut"]["source_exact_prefix"],
                serde_json::to_value(source_prefix).unwrap()
            );
            retained_sources.push((node.clone(), retained));
        }
        let context = native.component_context(&primary).unwrap();
        let checkpoint_root = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
            native.data_dir(&primary),
            context.host_role(),
        )
        .root
        .join("dfs-migration/receivers/source-ancestry");
        let mut checkpoint_seen = false;
        for entry in std::fs::read_dir(checkpoint_root).unwrap() {
            let file = entry.unwrap().path().join("source-checkpoints.json");
            if file.is_file() {
                let captures: Vec<crate::metadata::SourceCheckpointCapture> =
                    serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
                checkpoint_seen |= captures.iter().any(|capture| {
                    capture.source_format_version == 3
                        && capture.anchor.shard_id == DfsMetadataShardId::new(scope)
                        && Some(&capture.anchor.prefix_proof) == receiver_checkpoint.as_ref()
                });
            }
        }
        assert!(
            checkpoint_seen,
            "actual receiving replica persisted its independently captured original checkpoint"
        );
        let owner = native.metadata_primary_service(path).unwrap();
        let imported = owner
            .metadata_backend()
            .unwrap()
            .export_table_snapshot()
            .unwrap();
        let primary_original = modeled
            .iter()
            .find(|(node, ..)| node == &source_primary)
            .unwrap();
        assert_eq!(imported.tables.receipts, primary_original.4);
        assert!(imported.tables.objects.iter().any(
            |object| object.object_id == written.object_id && object.version == written.version
        ));
        drop(owner);
        let mut client = native.client(&source_primary).unwrap();
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), before);
        let after = b"normal USERS body after original receiver checkpoint admission";
        client.posix_write_file(path, after, options).unwrap();
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), after);
        drop(client);
        for node in &nodes {
            native.stop_node(node).unwrap();
        }
        native.start_nodes().unwrap();
        assert_eq!(
            observe_stage(&native),
            original_record,
            "second cold reopen preserves the exact original native record"
        );
        let mut client = native.client(&source_primary).unwrap();
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), after);
        for ((_, _, source_bytes, floors, ..), (_, retained)) in
            modeled.iter().zip(&retained_sources)
        {
            assert_eq!(
                std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap(),
                *source_bytes
            );
            for (kind, bytes) in floors {
                assert_eq!(
                    std::fs::read(retained.join(format!(
                        "coordination/metadata-{kind}-ack-floors/users.sobj"
                    )))
                    .unwrap(),
                    *bytes
                );
            }
        }
        assert_eq!(
            std::fs::read(retained_root.join("records/tables/snapshot.sobj")).unwrap(),
            root_source_bytes
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
