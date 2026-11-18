//! Native control-plane boundary of the migration component. Conversion bytes
//! stay in migration storage; only compact identities cross existing Raft.
use crate::metadata::{MetadataCapturedKvEntry, MetadataConverter, MetadataImportAuthority, MetadataSourceCut};
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
pub(crate) mod contribution_source;
mod contribution_extraction;
pub(crate) mod contribution_sources;
mod owner;
use owner::MigrationOwner;
pub(crate) mod placement_job_source;
mod placement_work;
#[cfg(test)]
mod native_fixtures;
mod routing_cache;
mod staged_service;

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
    contribution_extractions: Arc<Mutex<contribution_extraction::Extractions>>,
    source_verifiers:
        Arc<Mutex<BTreeMap<PathBuf, Arc<Mutex<staged_service::SourceTransferVerifier>>>>>,
}
impl NativeImportAuthority {
    pub(super) fn local_node(&self) -> Result<sinomo_core::node::NodeId> {
        Ok(sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(
                &self.data_root,
            )),
        )
        .map_err(error)?
        .local_node_id)
    }
    fn cluster(&self) -> Result<&Arc<dyn ClusterAccess>> {
        self.cluster.as_ref().ok_or_else(|| error("retained metadata conversion requires the node's cluster access"))
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
        &self, keys: &[String],
    ) -> Result<(Vec<MetadataCapturedKvEntry>, KvAuthorityProof)> {
        self.read_exact_evaluated(keys).map_err(|failure| match failure {
            NativeEvaluationError::Native(native) => self.native_read_error(native),
            NativeEvaluationError::Domain(domain) => domain,
        })
    }
    /// Native read admission can be temporarily unavailable during startup or
    /// membership convergence. Keep that typed absence of authority pending;
    /// malformed data and failed proof validation still reject the unit.
    fn native_read_error(&self, failure: sinomo_core::service::ServiceError) -> MetadataBackendError {
        use sinomo_core::network::NetworkError as Network;
        let retryable = match &failure {
            ServiceError::Network(network) => matches!(network,
                Network::ClusterReadOnly { .. } | Network::VerifiedClusterNonLeader { .. }
                | Network::Timeout { .. } | Network::RetryExhausted { .. }
                | Network::DeliveryFailed { .. } | Network::OutcomeUnknown { .. }
                | Network::Io { .. } | Network::MailboxFull { .. }
                | Network::UnknownNode { .. } | Network::UnknownRoute { .. }
                | Network::ObjectRpcRouteSaturated { .. } | Network::ObjectFrameMemoryUnavailable { .. }),
            ServiceError::Kv(kv) => matches!(kv, KvError::Busy { .. } | KvError::AuthorityUnavailable { .. }
                | KvError::LeaderOffline { .. } | KvError::QuorumUnavailable { .. }
                | KvError::StaleLeadershipTerm { .. }),
            _ => false,
        };
        if retryable {
            tracing::debug!(root=%self.metadata_root.display(), error=%failure,
                "migration awaits native read authority");
            MetadataBackendError::StoragePreparationPending {
                root: self.metadata_root.clone(), processed_records: 0,
            }
        } else { error(failure) }
    }
    fn read_native<T>(&self, future: impl Future<Output = std::result::Result<T, sinomo_core::service::ServiceError>>) -> Result<T> {
        block_native(&self.runtime, async { Ok::<_, MetadataBackendError>(future.await) })?
            .map_err(|failure| self.native_read_error(failure))
    }
    // Read-only probes and idempotent DATA enrollment retain typed transport
    // failures until the owner chooses a retry. This does not infer an effect.
    fn read_peer<T>(&self, future: impl Future<Output = std::result::Result<T, sinomo_core::network::NetworkError>>) -> Result<T> {
        let result = block_native(&self.runtime, async { Ok::<_, MetadataBackendError>(future.await) })?;
        match result {
            // These callers already selected an exact admitted participant.
            // Its transport/route can be absent while its component mounts.
            Err(failure @ (sinomo_core::network::NetworkError::UnknownNode { .. }
                | sinomo_core::network::NetworkError::UnknownRoute { .. })) => {
                tracing::debug!(error=%failure, "migration awaits its admitted peer transport");
                Err(MetadataBackendError::StoragePreparationPending {
                    root: self.metadata_root.clone(), processed_records: 0,
                })
            }
            result => result.map_err(|failure| self.native_read_error(ServiceError::Network(failure))),
        }
    }
    pub(super) fn read_exact_evaluated(
        &self, keys: &[String],
    ) -> std::result::Result<(Vec<MetadataCapturedKvEntry>, KvAuthorityProof), NativeEvaluationError> {
        let read = block_native(&self.runtime, async {
            Ok::<_, MetadataBackendError>(self.cluster()?.kv_get_authoritative_exact_set_async(keys.to_vec()).await)
        })?.map_err(NativeEvaluationError::Native)?;
        current_authority(&read.authority)?;
        if read.value.len() != keys.len() {
            return Err(error(
                "native exact-set response changed its key cardinality",
            ).into());
        }
        if keys.iter().zip(&read.value).any(|(expected, actual)|
            actual.as_ref().is_some_and(|entry| &entry.key != expected)) {
            return Err(error("native exact-set response changed its requested key scope").into());
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
        self.commit_evaluated(request).map_err(NativeEvaluationError::into_metadata)
    }
    /// Preserve native availability separately from invalid command/outcome DATA.
    /// Callers retain the same exact request while its outcome is uncertain.
    pub(super) fn commit_evaluated(&self, request: KvTxnRequest)
        -> std::result::Result<DfsNativeTxnCommit, NativeEvaluationError> {
        let result = block_native(&self.runtime, async {
            Ok::<_, MetadataBackendError>(self.cluster()?.kv_commit_compact_async(request.clone()).await)
        })?.map_err(NativeEvaluationError::Native)?;
        let submitted = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
            .map_err(error)?;
        if result.request_id != request.request_id
            || result.submitted_command_digest != Some(submitted)
        {
            return Err(error(
                "native commit receipt differs from the exact submitted import request",
            ).into());
        }
        let response = result
            .txn_response
            .as_ref()
            .ok_or_else(|| error("native commit returned no transaction response"))?;
        if response.revision != result.authority.revision {
            return Err(error(
                "native import CAS did not commit against its captured control-plane state",
            ).into());
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
    fn control_client(&self) -> NativeImportKv<'_> {
        NativeImportKv { owner: self }
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
}

impl MetadataImportAuthority for NativeImportAuthority {
    fn original_handoff_reservation(
        &self,
        shard: &sinomo_dfs::DfsMetadataShardId,
        file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Option<crate::metadata::OriginalMetadataHandoffReservation>> {
        self.capture_original_handoff(shard, file_system_ids)
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
    fn source_policy_candidate_count(&self, source_root: &Path) -> Result<usize> {
        self.validate_source_root(source_root)?;
        let mut client = self.control_client();
        let view = DfsControlPlaneStore::new(&mut client)
            .cluster_view()
            .map_err(|failure| self.native_read_error(ServiceError::Kv(failure)))?;
        let eligible = view
            .eligible_content_node_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        // Zero is unavailable current planning DATA. The controller retains its
        // source cursor until the ordinary host publishes actual eligibility.
        Ok(eligible.len())
    }
    fn assignments(
        &self,
        file_system_ids: &[sinomo_dfs::DfsFileSystemId],
    ) -> Result<Vec<DfsMetadataShardConfig>> {
        let cluster = self.cluster()?;
        let mut initial = BTreeMap::new();
        for fs in file_system_ids {
            let read = self.read_native(
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
                // This two-read observation has not captured a source cut yet.
                // A sibling activation can advance the catalogue between the
                // reads; retry observation without replacing any stored cut.
                return Err(MetadataBackendError::StoragePreparationPending {
                    root: self.metadata_root.clone(), processed_records: 0,
                });
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
}

/// A read-only control-plane view over the owner's native authoritative
/// reads: the cluster view a capture binds its import policy to, and the
/// configuration inventory the configuration conversion reads.
struct NativeImportKv<'a> {
    owner: &'a NativeImportAuthority,
}
fn native_read_to_kv(failure: MetadataBackendError) -> KvError {
    match failure {
        MetadataBackendError::StoragePreparationPending { .. } => KvError::AuthorityUnavailable {
            detail: failure.to_string(),
        },
        failure => KvError::Serialization { detail: failure.to_string() },
    }
}
impl std::fmt::Debug for NativeImportKv<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeImportKv")
    }
}
impl DfsControlPlaneKvClient for NativeImportKv<'_> {
    fn get(&self, key: &str) -> std::result::Result<Option<KvEntry>, KvError> {
        self.owner
            .read_exact(&[key.into()])
            .map(|(mut entries, _)| entries.remove(0).entry)
            .map_err(native_read_to_kv)
    }
    fn get_consistent(
        &self,
        keys: &[String],
    ) -> std::result::Result<Vec<Option<KvEntry>>, KvError> {
        self.owner
            .read_exact(keys)
            .map(|(entries, _)| entries.into_iter().map(|entry| entry.entry).collect())
            .map_err(native_read_to_kv)
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
        let read = self.owner.read_native(
            if matches!(prefix, "/" | "/dfs/filesystems/") {
                cluster.kv_range_prefix_complete_authoritative_async(prefix.into())
            } else {
                cluster.kv_range_prefix_authoritative_async(prefix.into())
            },
        )
        .map_err(native_read_to_kv)?;
        current_authority(&read.authority).map_err(|e| KvError::Serialization {
            detail: e.to_string(),
        })?;
        Ok(read.value)
    }
    fn put(&mut self, key: String, _value: Vec<u8>) -> std::result::Result<KvRevision, KvError> {
        Err(read_only_view(&key))
    }
    fn txn(&mut self, txn: KvTxn) -> std::result::Result<KvTxnResponse, KvError> {
        Err(read_only_view(&format!("{} operations", txn.success.len())))
    }
}
fn read_only_view(target: &str) -> KvError {
    KvError::AuthorityUnavailable {
        detail: format!("migration's control-plane view is read-only: {target}"),
    }
}

static OWNERS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<MigrationOwner>>>> = OnceLock::new();
fn source_effect_host(context: &sinomo_module::ComponentContext) -> bool {
    matches!(
        context.host_role(),
        sinomo_module::ComponentHostRole::Main | sinomo_module::ComponentHostRole::Embedded
    )
}
fn shared_owner(
    context: &sinomo_module::ComponentContext,
    lifecycle: tokio::sync::watch::Receiver<bool>,
) -> std::result::Result<Arc<MigrationOwner>, sinomo_module::ComponentError> {
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
    let mut roots = OWNERS
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
        contribution_extractions: Arc::new(Mutex::new(Default::default())),
        source_verifiers: Arc::new(Mutex::new(BTreeMap::new())),
    });
    let metadata_authority: Arc<dyn MetadataImportAuthority> = authority.clone();
    let value = Arc::new(MigrationOwner {
        work: owner::new_owner_work(&authority.data_root).map_err(|error| sinomo_module::ComponentError::component("dfs-migration", error))?,
        metadata: Arc::new(MetadataConverter::with_authority(metadata_authority)),
        authority,
        content: Arc::new(crate::content::ContentConverter::default()),
        content_v3: Mutex::new(Default::default()),
        local_state: crate::local_state::LocalStateConverter::default(),
        _owner: owner,
        metadata_root: context.data_dir().dfs_dir().join("metadata"),
        configuration_ready: std::sync::atomic::AtomicBool::new(false),
        routing_cache_lock: Mutex::new(()),
        lifecycle,
        #[cfg(test)]
        held_conversions: Mutex::new(None),
    });
    roots.insert(paths.root, Arc::downgrade(&value));
    Ok(value)
}

/// Mount after the normal DFS component has acquired its real storage lease.
/// The service exposes only hidden native candidates, never normal DFS routes.
pub fn staged_import_component_factory()
-> std::result::Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    staged_service::factory()
        .map(|factory| factory.with_storage_paths(crate::register_storage_paths))
}

/// Built after the component factory has retained its real data-owner lease.
pub fn storage_preparation_provider(
    context: &sinomo_module::ComponentContext,
    scope: &mut sinomo_module::ComponentScope,
    lifecycle: tokio::sync::watch::Receiver<bool>,
) -> std::result::Result<sinomo_dfs::DfsStoragePreparationProviderBinding, sinomo_module::ComponentError> {
    if !source_effect_host(context) {
        let held_owner = sinomo_dfs::runtime_host::retain_component_data_owner(context)
            .map_err(|failure| sinomo_module::ComponentError::component("dfs-migration", failure))?;
        let data_root = context.data_dir_root().canonicalize()
            .map_err(|failure| sinomo_module::ComponentError::component("dfs-migration", failure))?;
        let (changed, readiness) = tokio::sync::watch::channel(0);
        return Ok(sinomo_dfs::DfsStoragePreparationProviderBinding {
            provider: Arc::new(move |root, unit, _budget| {
                let _actual_owner = &held_owner;
                let _readiness_lifetime = &changed;
                if *lifecycle.borrow() || lifecycle.has_changed().is_err() {
                    return Err(error("read-only migration observation lifecycle is closed"));
                }
                owner::validate_managed_unit(&data_root, root, unit)?;
                owner::inspect_current_unit(root, unit)
            }),
            readiness,
        });
    }
    let owner = shared_owner(context, lifecycle)?;
    owner.attach(scope).map_err(|error| sinomo_module::ComponentError::component("dfs-migration", error))?;
    Ok(owner.binding())
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
            Arc::new(move |context, component_scope, lifecycle| {
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
                storage_preparation_provider(context, component_scope, lifecycle)
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
        let migration = native_fixtures::canonical_migration_runtime(&context);
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
        native_released_namespace_source_reopens(false, false, false);
    }

    #[test]
    fn native_original_write_plan_survives_a_released_version_without_durability() {
        native_released_namespace_source_reopens(true, false, false);
    }

    #[test]
    fn native_original_publication_grant_survives_real_same_version_placement() {
        native_released_namespace_source_reopens(true, true, false);
    }

    #[test]
    fn native_an_interrupted_metadata_prepare_stops_its_shards_conversion() {
        native_released_namespace_source_reopens(true, true, true);
    }

    #[test]
    fn native_metadata_preparation_advances_multiple_batches_before_ready() {
        native_released_namespace_source_reopens_scaled(false, false, false, 256);
    }

    #[test]
    fn native_production_sized_metadata_preparation_keeps_original_browser_deadline() {
        // An additional production-cardinality fixture, not a replacement for
        // the existing 256-file test with its original five-second deadline.
        native_released_namespace_source_reopens_with_browser_budget(
            false,
            false,
            false,
            3072,
            Some(std::time::Duration::from_secs(60)),
        );
    }

    fn native_released_namespace_source_reopens(
        omit_source_durability: bool,
        same_version_placement: bool,
        interrupt_metadata_confirmation: bool,
    ) {
        native_released_namespace_source_reopens_scaled(
            omit_source_durability,
            same_version_placement,
            interrupt_metadata_confirmation,
            0,
        );
    }

    fn native_released_namespace_source_reopens_scaled(
        omit_source_durability: bool,
        same_version_placement: bool,
        interrupt_metadata_confirmation: bool,
        additional_source_files: usize,
    ) {
        native_released_namespace_source_reopens_with_browser_budget(
            omit_source_durability,
            same_version_placement,
            interrupt_metadata_confirmation,
            additional_source_files,
            None,
        );
    }

    /// Mount normally and observe admission of the exact physical unit within
    /// the same caller deadline. Startup completion can park unrelated work.
    fn reopen_prepared_metadata_root(
        native: &mut sinomo_dfs::test_support::DfsTestRuntime,
        node: sinomo_core::node::NodeId,
        root: &Path,
        shard: &sinomo_dfs::DfsMetadataShardId,
        budget: Option<std::time::Duration>,
    ) -> std::result::Result<(), String> {
        if let Some(budget) = budget {
            let deadline = tokio::time::Instant::now() + budget;
            native.reopen_node_with_startup_deadline(node.clone(), deadline)?;
            return wait_prepared_metadata_root(native, &node, root, shard, deadline);
        }
        let runtime_root = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
            native.data_dir(&node), sinomo_module::ComponentHostRole::Embedded,
        ).root;
        // Ordinary reopen starts its five-second completion window after host
        // setup. Use that existing action boundary, sharing its same deadline
        // with exact-unit admission and the normal startup-completion check.
        native.reopen_node_with_startup_action(node, Box::new(move |remaining| {
            let owner = OWNERS.get().and_then(|owners|
                owners.lock().ok()?.get(&runtime_root)?.upgrade())
                .ok_or_else(|| "mounted component has no actual migration owner".to_owned())?;
            wait_prepared_metadata_owner(&owner, root, shard, tokio::time::Instant::now() + remaining)
        }))
    }

    fn wait_prepared_metadata_root(
        native: &sinomo_dfs::test_support::DfsTestRuntime,
        node: &sinomo_core::node::NodeId,
        root: &Path,
        shard: &sinomo_dfs::DfsMetadataShardId,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<(), String> {
        let context = native.component_context(node)?;
        let owner = native_fixtures::canonical_migration_runtime(&context);
        wait_prepared_metadata_owner(&owner, root, shard, deadline)
    }

    fn wait_prepared_metadata_owner(
        owner: &Arc<MigrationOwner>,
        root: &Path,
        shard: &sinomo_dfs::DfsMetadataShardId,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<(), String> {
        let binding = owner.binding();
        let mut changed = binding.readiness;
        block_native(&owner.authority.runtime, async {
            loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(error(format!("exact metadata admission reached original deadline: {}", root.display())));
                }
                match (binding.provider)(root, &sinomo_dfs::DfsStoragePreparationScope::Metadata(shard.clone()),
                    sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET)? {
                    sinomo_dfs::DfsStoragePreparation::Ready => return Ok::<_, MetadataBackendError>(()),
                    sinomo_dfs::DfsStoragePreparation::Pending { .. } => {}
                }
                tokio::time::timeout_at(deadline, changed.changed()).await
                    .map_err(|_| error(format!("exact metadata admission reached original deadline: {}", root.display())))?
                    .map_err(error)?;
            }
        }).map_err(|failure| failure.to_string())
    }

    /// A released legacy copy of the root shard, without a storage record,
    /// converts per shard when its node reopens, and the current DFS then
    /// serves and writes it. With `interrupt_metadata_confirmation`, the copy
    /// also holds a V7 prepare whose outcome it does not record: the shard's
    /// conversion stops for operator repair with that crash-left work named,
    /// and the shard keeps its original copy.
    fn native_released_namespace_source_reopens_with_browser_budget(
        omit_source_durability: bool,
        same_version_placement: bool,
        interrupt_metadata_confirmation: bool,
        additional_source_files: usize,
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
        } else if additional_source_files > 0 {
            "native-metadata-preparation-multiple-batches"
        } else if interrupt_metadata_confirmation {
            "native-interrupted-metadata-prepare-cold-conversion"
        } else if same_version_placement {
            "native-original-grant-after-real-placement"
        } else if omit_source_durability {
            "native-original-write-plan-cold-admission"
        } else {
            "native-seven-field-namespace-cold-admission"
        };
        let nodes = DfsTestRuntime::ordered_node_ids(name, if same_version_placement { 2 } else { 1 });
        let node = nodes[0].clone();
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        let pending_batches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observe_pending = Arc::clone(&pending_batches);
        let builder: sinomo_dfs::DfsStoragePreparationProviderBuilder = Arc::new(
            move |context, component_scope, lifecycle| {
                let binding = storage_preparation_provider(context, component_scope, lifecycle.clone())?;
                let owner = shared_owner(context, lifecycle)?;
                let pending = Arc::clone(&observe_pending);
                owner.metadata.observe_steps_for_test(Arc::new(move |_root, _shard, budget, progress| {
                    assert_eq!(budget, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET);
                    if let Some(sinomo_dfs::DfsStoragePreparation::Pending { processed_records }) = progress {
                        assert!(processed_records <= budget);
                        if processed_records > 0 {
                            pending.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    Ok(())
                }));
                Ok(binding)
            },
        );
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
        let mut client = native.client(&node).unwrap();
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
        drop(client);
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
                    Err(sinomo_dfs::DfsError::MetadataCommitOutcomeUnknown {
                        cause: sinomo_dfs::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing {
                            required: 2,
                            acknowledged: 1,
                            ..
                        },
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
        // A deployment that predates storage records has none for the shard:
        // its copy is old storage that migration converts per shard.
        native_fixtures::remove_storage_record(
            &native_fixtures::canonical_migration_runtime(&native.component_context(&node).unwrap()),
            &assignment.file_system_id,
            &assignment.shard_id,
        );
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
        let snapshot = observed.export_table_snapshot().unwrap();
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
        let source_bytes = serde_ext::to_bytes(&source, 1, LengthMode::U32).unwrap();
        std::fs::write(&source_path, &source_bytes).unwrap();
        if same_version_placement {
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
        assert!(!crate::metadata::current_root(&root).unwrap());
        if production_browser_budget.is_some() {
            let records = crate::metadata::count_stopped_source_records_for_test(&root).unwrap();
            assert!(
                records >= 19_808,
                "fixture must cover the retained real root's record count: {records}"
            );
            eprintln!("PRODUCTION_SHAPE_SOURCE records={records} files={additional_source_files}");
        }
        let recovery_capture = (additional_source_files > 0).then(||
            sinomo_dfs::test_support::capture_metadata_operation_boundaries(native.data_dir(&node).root()));
        let recovery_started = std::time::Instant::now();
        let recovered = reopen_prepared_metadata_root(&mut native, node.clone(), &root,
            &DfsMetadataShardId::new("root"), production_browser_budget);
        eprintln!(
            "NATIVE_METADATA_RECOVERY files={additional_source_files} elapsed_ms={} result={recovered:?}",
            recovery_started.elapsed().as_millis()
        );
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
        if recovered.is_err() && let Some(capture) = &recovery_capture {
            capture.checkpoint("metadata-recovery-deadline");
        }
        drop(recovery_capture);
        if let Some(interrupted) = &interrupted_record {
            let refused = recovered.expect_err("a copy with an interrupted prepare must not convert");
            assert!(
                refused.contains("original work remains unresolved")
                    && refused.contains(&format!("prepared-operation-v7:{}", interrupted.digest.operation_id)),
                "{refused}"
            );
            assert!(!crate::metadata::current_root(&root).unwrap(), "the stopped shard keeps its original copy");
            let migration = native_fixtures::canonical_migration_runtime(&native.component_context(&node).unwrap());
            let key = sinomo_dfs::metadata_shard_storage_key(&assignment.file_system_id, &assignment.shard_id);
            let (entries, _) = migration.authority.read_exact(std::slice::from_ref(&key)).unwrap();
            let record: sinomo_dfs::DfsMetadataShardStorage =
                entries[0].entry.as_ref().expect("the conversion recorded its attempt").decode().unwrap();
            assert!(record.converting_attempt().is_some(), "the shard stays closed: {record:?}");
            drop(migration);
            native.close().unwrap();
            return;
        }
        recovered.expect("normal cold Main and real MIG provider must admit the released source");
        assert!(crate::metadata::current_root(&root).unwrap());
        let current = native.root_shard().unwrap();
        assert_eq!(current.file_system_id, assignment.file_system_id);
        assert_eq!(current.shard_id, assignment.shard_id);
        assert_eq!(current.primary_node_id, assignment.primary_node_id);
        // The conversion activates under the shard's own records: its serving
        // record keeps its epoch, and its storage record names the attempt.
        assert_eq!(current.epoch, assignment.epoch);
        let owner = native.metadata_primary_service(path).unwrap();
        let migration = native_fixtures::canonical_migration_runtime(&native.component_context(&node).unwrap());
        let key = sinomo_dfs::metadata_shard_storage_key(&current.file_system_id, &current.shard_id);
        let (entries, _) = migration.authority.read_exact(std::slice::from_ref(&key)).unwrap();
        let record: sinomo_dfs::DfsMetadataShardStorage =
            entries[0].entry.as_ref().expect("the conversion recorded the shard's storage").decode().unwrap();
        let (attempt, _) = record.converted_activation().expect("the shard is Active for its conversion");
        assert_eq!(attempt.source.source_journal_sequence, snapshot.prefix_proof.journal_sequence);
        let retained = migration.metadata_root.join(".metadata-retained")
            .join(format!("shard-conversion-{}", attempt.attempt_id));
        assert_eq!(
            std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap(),
            source_bytes
        );
        drop(migration);
        // The original seven-field body remains opaque source DATA. It must
        // not become an executable current prepare or a relabelled V7 receipt.
        let captured: crate::metadata::canonical_generation::CapturedMetadataUnit =
            serde_json::from_slice(&std::fs::read(
                crate::metadata::shard_capture_path(&root, &DfsMetadataShardId::new("root")).unwrap()
            ).unwrap()).unwrap();
        assert_eq!(captured.source.exact_prefix.as_ref(), Some(&snapshot.prefix_proof));
        captured.verify_retained_source_at(&retained).unwrap();
        assert!(captured.original_outcomes.iter().any(|outcome|
            outcome.original_id == operation
                && outcome.original_digest.as_deref() == Some(original_digest.as_str())
                && outcome.original_status == "aborted"));
        assert!(owner.metadata_backend().unwrap().prepared_operation_record(operation).unwrap().is_none());
        assert!(!captured.proven_v7_terminals.iter().any(|record| record.digest.operation_id == operation));
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
                    assert!(!snapshot.tables.prepared_operations_v7.iter()
                        .any(|row| row.digest.operation_id == original.digest.operation_id));
                    let terminal = snapshot.tables.prepared_operation_evictions.iter()
                        .find(|row| row.digest.operation_id == original.digest.operation_id).unwrap();
                    assert_eq!(terminal, &sinomo_dfs::DfsPreparedOperationEviction::from_terminal_record(original).unwrap());
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
        if additional_source_files > 0 {
            assert!(
                pending_batches.load(std::sync::atomic::Ordering::SeqCst) >= 2,
                "actual metadata source must require multiple unchanged preparation batches"
            );
        }
        native.close().unwrap();
    }

    #[test]
    fn native_cold_client_continuation_resumes_negative_pending_but_not_errors() {
        use sinomo_dfs::runtime_host::DfsRuntimeError;
        let (native, nodes) = native_fixtures::native_migration_cluster(
            "native-cold-client-preparation-control", 1,
        );
        let node = &nodes[0];
        let root = sinomo_dfs::default_dfs_metadata_shards_root(native.data_dir(node))
            .join("root").canonicalize().unwrap();
        let open = |deny_first: bool| {
            let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let mut actual_opens = 0;
            let result = native_fixtures::continue_native_source_step(&root, deadline, || {
                let actual = native.client_with_startup_deadline_once(
                    node, tokio::time::Instant::from_std(deadline),
                );
                let result = actual.and_then(|client| {
                    actual_opens += 1;
                    if actual_opens != 1 {
                        return Ok(client);
                    }
                    // Negative-only consumer-boundary control after a genuine
                    // open. Drop that shell before yielding; no successful
                    // provider result, authority or receipt is fabricated.
                    drop(client);
                    if deny_first {
                        Err(DfsRuntimeError::StoragePreparationPending {
                            root: root.clone(), processed_records: 0,
                        })
                    } else {
                        Err(DfsRuntimeError::Parse {
                            detail: "controlled native preparation failure".into(),
                        })
                    }
                });
                result.map_err(|value| match value {
                    DfsRuntimeError::StoragePreparationPending { root, processed_records } =>
                        MetadataBackendError::StoragePreparationPending { root, processed_records },
                    other => error(other),
                })
            });
            (result, actual_opens)
        };
        let (positive, pending_opens) = open(true);
        let positive = match positive {
            Ok(mut client) => client.posix_stat_distributed("/")
                .map(|stat| stat.file_system_id).map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        let (terminal, terminal_opens) = open(false);
        let terminal_error = terminal.err().map(|error| error.to_string());
        std::fs::write(native.data_dir(node).root().join("boundary-diagnostic-client-preparation-control.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "positive": positive, "pending_actual_opens": pending_opens,
                "terminal_error": terminal_error, "terminal_actual_opens": terminal_opens,
                "qualification": "Negative-only consumer-boundary injection after genuine registered native opens. Complements actual source-writer-release and typed-error controls; does not reproduce historical provider contention."
            })).unwrap()).unwrap();
        native.close().unwrap();
        assert_eq!(positive.unwrap(), sinomo_dfs::sifs_file_system_id());
        assert_eq!(pending_opens, 2, "Pending must reopen through the actual native owner");
        assert!(terminal_error.unwrap().contains("controlled native preparation failure"));
        assert_eq!(terminal_opens, 1, "an unrelated error must not turn into another open");
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
        let mut scope = sinomo_module::ComponentScope::new(sinomo_module::ComponentDescriptor::new(
            "migration-aliased-provider",
            "1",
        ));
        let runtime = shared_owner(&context, scope.shutdown_receiver()).unwrap();
        assert_ne!(runtime.authority.data_root, context.data_dir_root());
        let lifecycle = scope.shutdown_receiver();
        let provider = storage_preparation_provider(&context, &mut scope, lifecycle).unwrap().provider;
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
        let runtime = shared_owner(&context, scope.shutdown_receiver()).unwrap();
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
    fn fresh_component_watch_cannot_reuse_retired_ready_runtime_with_the_same_owner() {
        let name = "migration-real-owner-new-provider-lifecycle";
        let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
        let mut native =
            sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        native.start_nodes().unwrap();
        let data_dir = native.data_dir(&nodes[0]).clone();
        let context = native.component_context(&nodes[0]).unwrap();
        let mut first_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("native-provider-lifecycle-control", "1"),
        );
        let first_watch = first_scope.shutdown_receiver();
        let first = shared_owner(&context, first_watch.clone()).unwrap();
        assert!(!first.configuration_ready.load(std::sync::atomic::Ordering::Acquire));
        let binding = storage_preparation_provider(&context, &mut first_scope, first_watch.clone()).unwrap();
        let provider = binding.provider;
        let mut changed = binding.readiness;
        assert!(Arc::ptr_eq(
            &first,
            &shared_owner(&context, first_watch.clone()).unwrap()
        ));
        let unrelated_scope = sinomo_module::ComponentScope::new(
            sinomo_module::ComponentDescriptor::new("unrelated-provider-lifecycle", "1"),
        );
        assert!(
            shared_owner(&context, unrelated_scope.shutdown_receiver()).is_err(),
            "another open scope must not replace the actual owner's live controller"
        );
        assert!(Arc::ptr_eq(
            &first,
            &shared_owner(&context, first_watch.clone()).unwrap()
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

                }
            }
        }
        // The independent owner may have normalized configuration while
        // content was requested. Only its routing continuation certifies that
        // readiness, and a new component lifetime must qualify it again.
        block_native(&native.runtime_handle(), async {
            let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            loop {
                if provider(&data_dir.dfs_dir().join("metadata"),
                    &DfsStoragePreparationScope::MetadataRouting, 256)?
                    == sinomo_dfs::DfsStoragePreparation::Ready { break; }
                tokio::time::timeout_at(deadline, changed.changed()).await
                    .map_err(|_| error("owned routing configuration did not become ready"))?
                    .map_err(error)?;
            }
            Ok::<_, MetadataBackendError>(())
        }).unwrap();
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
        let second = shared_owner(&context, second_scope.shutdown_receiver()).unwrap();
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
        assert!(shared_owner(&context, first_watch).is_err());
        drop(second_scope);
        drop(provider);
        drop(second);
        drop(first);
        drop(actual_owner);
        native.close().unwrap();
    }
}
