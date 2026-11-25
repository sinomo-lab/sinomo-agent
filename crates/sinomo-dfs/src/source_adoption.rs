//! Owned native source custody for a current cross-shard publication.
use crate::metadata_error::{MetadataBackendError, Result};
use crate::runtime_host::DfsDataOwnerLock;
use crate::{
    DfsCrossShardSourceFamily, DfsMetadataAuthorityRuntime, DfsMetadataShardConfig,
    DfsNamespaceJobStatus, DfsPreparedOperationDigest, DfsPreparedOperationPayload,
    DfsPreparedOperationScope, DfsSourceAdoptionCapture,
};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod job_custody;
mod preparation;
mod retained;
pub use job_custody::DfsSourceAdoptionJobOffer;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use preparation::SourcePreparationToken;
pub(crate) use preparation::{PreparedSourceAdoption, SourcePreparationPlan};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use retained::retained_custody_blocks_cleanup;
pub(crate) use retained::{DfsSourceAdoptionCloseDisposition, retained_owner_for_mount};

pub(crate) const SOURCE_ADOPTION_ROUTE: &str = "/dfs/source-adoption-v7";
fn error(reason: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("source adoption custody: {reason}"))
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsSourceAdoptionOffer {
    pub source_node_id: NodeId,
    pub session_id: String,
    pub source_config: DfsMetadataShardConfig,
    pub target_config: DfsMetadataShardConfig,
    pub object_id: crate::DfsObjectId,
    pub job_id: String,
    pub job_offer: DfsSourceAdoptionJobOffer,
    pub transfer_id: String,
    pub destination: crate::DfsObjectMetadata,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum DfsSourceAdoptionRequest {
    AcquireSource {
        plan: preparation::SourcePreparationPlan,
    },
    InspectSourcePreparation {
        plan: preparation::SourcePreparationPlan,
    },
    RevokeSourcePreparation {
        token: preparation::SourcePreparationToken,
    },
    BindSource {
        token: preparation::SourcePreparationToken,
        job_offer: DfsSourceAdoptionJobOffer,
    },
    InspectOriginalJob {
        offer: DfsSourceAdoptionJobOffer,
    },
    ReleaseOriginalJob {
        offer: DfsSourceAdoptionOffer,
        digest: Option<DfsPreparedOperationDigest>,
    },
    CompleteSource {
        offer: DfsSourceAdoptionOffer,
    },
    CancelUnclaimed {
        offer: DfsSourceAdoptionOffer,
    },
    Claim {
        offer: DfsSourceAdoptionOffer,
        target_scope: DfsPreparedOperationScope,
        payload_digest: String,
    },
    Validate {
        offer: DfsSourceAdoptionOffer,
        digest: DfsPreparedOperationDigest,
    },
    Release {
        offer: DfsSourceAdoptionOffer,
        digest: DfsPreparedOperationDigest,
    },
    InspectOutcome {
        offer: DfsSourceAdoptionOffer,
        target_scope: DfsPreparedOperationScope,
        payload_digest: String,
        expected_digest: Option<DfsPreparedOperationDigest>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DfsSourceAdoptionResponse {
    offer: DfsSourceAdoptionOffer,
    family: Option<DfsCrossShardSourceFamily>,
    target_scope: DfsPreparedOperationScope,
    payload_digest: String,
    outcome: Option<DfsSourceAdoptionOutcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result", content = "body", rename_all = "kebab-case")]
pub(crate) enum DfsSourceAdoptionRpcResponse {
    SourceClaim(DfsSourceAdoptionResponse),
    OriginalJob(job_custody::OriginalJobReceipt),
    SourcePrepared(preparation::SourcePreparationToken),
    SourcePreparationRevoked(preparation::SourcePreparationRevocation),
    SourceBound(preparation::PreparedSourceAdoption),
    SourcePreparationInspected {
        prepared: Option<preparation::PreparedSourceAdoption>,
    },
    OriginalJobReleased {
        offer: DfsSourceAdoptionJobOffer,
    },
    SourceCompleted {
        offer: DfsSourceAdoptionOffer,
    },
    SourceCancelled {
        offer: DfsSourceAdoptionOffer,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum DfsSourceAdoptionOutcome {
    Unavailable,
    Active,
    Committed {
        digest: DfsPreparedOperationDigest,
        commit: crate::DfsPreparedOperationCommitReference,
    },
    Inactive {
        digest: Option<DfsPreparedOperationDigest>,
        status: Option<crate::DfsPreparedOperationStatus>,
    },
}
#[derive(Default)]
struct TargetClaim {
    witness: Option<Weak<DfsSourceAdoptionWitness>>,
    revoked: bool,
    // Constructed only at the guarded native kernel's materialized commit.
    // This is a live terminal fact, not incoming DATA or publication authority.
    terminal: Option<NativeTargetTerminal>,
}
#[derive(Clone)]
struct NativeTargetTerminal {
    offer: DfsSourceAdoptionOffer,
    scope: crate::publication_guard::DfsPublicationOwnerScope,
    configuration: DfsMetadataShardConfig,
    generation: u64,
    storage: crate::metadata_table_store::MetadataTableStorageGeneration,
    prefix: crate::DfsMetadataPrefixProof,
    digest: DfsPreparedOperationDigest,
    commit: crate::DfsPreparedOperationCommitReference,
}
struct SourceSession {
    original_job: Arc<crate::DfsNamespaceJobRecord>,
    capture: Arc<crate::DfsSourceAdoptionSourceCapture>,
    generation: u64,
    offer: DfsSourceAdoptionOffer,
    claim: Arc<
        Mutex<
            Option<(
                DfsPreparedOperationScope,
                String,
                Option<DfsPreparedOperationDigest>,
            )>,
        >,
    >,
}
#[derive(Clone)]
struct CompletedSource {
    original_job: Arc<crate::DfsNamespaceJobRecord>,
    offer: DfsSourceAdoptionOffer,
    family: DfsCrossShardSourceFamily,
    scope: DfsPreparedOperationScope,
    hash: String,
    digest: DfsPreparedOperationDigest,
}
/// Whether `committed`, reported as the commit of the adoption that the
/// target prepared as `original`, is that prepare or a `recover:` replacement
/// of it: prepared-change recovery republishes the original at each newer
/// epoch of the target shard under its recovery id, one step per epoch.
fn commits_adoption(
    original: &DfsPreparedOperationDigest,
    committed: &DfsPreparedOperationDigest,
) -> bool {
    if committed.operation_id == original.operation_id {
        return committed == original;
    }
    if committed.protocol_version != original.protocol_version
        || committed.file_system_id != original.file_system_id
        || committed.shard_id != original.shard_id
        || committed.prepare_sequence <= original.prepare_sequence
    {
        return false;
    }
    let (mut operation_id, mut epoch) = (committed.operation_id.as_str(), committed.owner_epoch);
    while let Some(recovered) = crate::metadata_records::recovered_operation_id(operation_id, epoch)
    {
        if recovered == original.operation_id {
            return original.owner_epoch < epoch;
        }
        // An intermediate replacement names its own, older epoch.
        let Some(older) = recovered
            .strip_prefix("recover:")
            .and_then(|rest| rest.rsplit_once(':'))
            .and_then(|(_, suffix)| suffix.parse::<u64>().ok())
            .filter(|older| *older < epoch)
        else {
            return false;
        };
        (operation_id, epoch) = (recovered, older);
    }
    false
}
fn digest_for_release(
    scope: &DfsPreparedOperationScope,
    hash: &str,
    session: &SourceSession,
) -> Result<Option<DfsPreparedOperationDigest>> {
    let claim = session
        .claim
        .lock()
        .map_err(|_| error("source claim poisoned"))?;
    let Some((actual_scope, actual_hash, digest)) = claim.as_ref() else {
        return Err(error("released source has no actual target claim"));
    };
    if actual_scope != scope || actual_hash != hash {
        return Err(error("source release changes actual target claim"));
    }
    Ok(digest.clone())
}
impl std::fmt::Debug for SourceSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSession")
            .field("offer", &self.offer)
            .finish_non_exhaustive()
    }
}
static RETAINED: OnceLock<Mutex<BTreeMap<(PathBuf, String), Arc<DfsSourceAdoptionActor>>>> =
    OnceLock::new();
static ACTORS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<DfsSourceAdoptionActor>>>> = OnceLock::new();
pub(crate) fn actor_for_root(root: &std::path::Path) -> Result<Arc<DfsSourceAdoptionActor>> {
    ACTORS
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| error("actor registry poisoned"))?
        .get(root)
        .and_then(Weak::upgrade)
        .ok_or_else(|| error("live native source actor unavailable"))
}
pub(crate) struct DfsSourceAdoptionActor {
    data_root: PathBuf,
    cluster_id: sinomo_core::Id,
    node: NodeId,
    _owner: Arc<DfsDataOwnerLock>,
    authority: Arc<DfsMetadataAuthorityRuntime>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    transport: Arc<dyn sinomo_module::ComponentObjectRpcTransport>,
    runtime: tokio::runtime::Handle,
    next_frame: std::sync::atomic::AtomicU64,
    sessions: Mutex<BTreeMap<String, Arc<SourceSession>>>,
    target_claims: Arc<Mutex<BTreeMap<String, TargetClaim>>>,
    job_sessions: Mutex<BTreeMap<String, Arc<job_custody::OriginalJobSession>>>,
    retired_jobs:
        Mutex<BTreeMap<String, (DfsSourceAdoptionOffer, Option<DfsPreparedOperationDigest>)>>,
    completed_sources: Mutex<BTreeMap<String, CompletedSource>>,
    source_preparations: Mutex<BTreeMap<String, Arc<preparation::PendingSourcePreparation>>>,
    predecessors: Vec<Arc<DfsSourceAdoptionActor>>,
    reconciling: std::sync::atomic::AtomicBool,
}
impl std::fmt::Debug for DfsSourceAdoptionActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfsSourceAdoptionActor")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}
impl DfsSourceAdoptionActor {
    pub(crate) fn is_local_primary(&self, config: &DfsMetadataShardConfig) -> bool {
        config.primary_node_id == self.node
    }
    pub(crate) fn from_component(
        context: &sinomo_module::ComponentContext,
        owner: Arc<DfsDataOwnerLock>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<Arc<Self>, sinomo_module::ComponentError> {
        let create = || -> Result<Arc<Self>> {
            if !matches!(
                context.host_role(),
                sinomo_module::ComponentHostRole::Main | sinomo_module::ComponentHostRole::Embedded
            ) {
                return Err(error("Shadow cannot own source effects"));
            }
            let paths = crate::runtime_host::DfsRuntimePaths::for_host_role(
                context.data_dir(),
                context.host_role(),
            );
            let registered =
                crate::runtime_host::require_daemon_dfs_data_owner_lock(&paths).map_err(error)?;
            if !Arc::ptr_eq(&registered, &owner) {
                return Err(error("not the actual registered main data owner"));
            }
            let runtime = context
                .runtime()
                .ok_or_else(|| error("normal runtime unavailable"))?;
            context
                .cluster_access()
                .ok_or_else(|| error("native cluster authority unavailable"))?;
            let transport = context
                .object_rpc_transport()
                .ok_or_else(|| error("normal object transport unavailable"))?;
            let cluster_paths = sinomo_core::network::ClusterPaths::new(context.data_dir());
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
                .map_err(error)?;
            let authority =
                crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
                    &cluster_paths,
                    &state.cluster_id,
                    &state.local_node_id,
                    Some(runtime),
                )
                .map_err(error)?;
            let predecessors = retained::predecessors(
                &context.data_dir().root(),
                &state.local_node_id,
                &state.cluster_id,
                &owner,
            )?;
            let target_claims = ACTORS
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| error("source actor registry poisoned"))?
                .get(&context.data_dir().root())
                .and_then(Weak::upgrade)
                .filter(|previous| {
                    previous.node == state.local_node_id
                        && previous.cluster_id == state.cluster_id
                        && Arc::ptr_eq(&previous._owner, &owner)
                        && (*previous.shutdown.borrow() || previous.shutdown.has_changed().is_err())
                })
                .map(|previous| previous.target_claims.clone())
                .unwrap_or_default();
            let actor = Arc::new(Self {
                data_root: context.data_dir().root(),
                cluster_id: state.cluster_id.clone(),
                node: state.local_node_id.clone(),
                _owner: owner.clone(),
                authority,
                shutdown: shutdown.clone(),
                transport,
                runtime: runtime.clone(),
                next_frame: std::sync::atomic::AtomicU64::new(1),
                sessions: Mutex::new(BTreeMap::new()),
                target_claims,
                job_sessions: Mutex::new(BTreeMap::new()),
                retired_jobs: Mutex::new(BTreeMap::new()),
                completed_sources: Mutex::new(BTreeMap::new()),
                source_preparations: Mutex::new(BTreeMap::new()),
                predecessors,
                reconciling: std::sync::atomic::AtomicBool::new(false),
            });
            ACTORS
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| error("actor registry poisoned"))?
                .insert(actor.data_root.clone(), Arc::downgrade(&actor));
            Ok(actor)
        };
        create().map_err(|error| sinomo_module::ComponentError::Component {
            component_id: crate::SINOMO_DFS_MODULE_LEGACY_ID.into(),
            detail: error.to_string(),
        })
    }
    fn require_current(&self, offer: &DfsSourceAdoptionOffer) -> Result<()> {
        if *self.shutdown.borrow() || self.shutdown.has_changed().is_err() {
            return Err(error("native component closed"));
        }
        if offer.source_node_id != self.node || offer.source_config.primary_node_id != self.node {
            return Err(error("source is not the actual current metadata primary"));
        }
        let cfgs = self
            .authority
            .configuration_snapshot()
            .metadata_shards(&offer.source_config.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("source configuration projection unavailable"))?;
        if !cfgs.iter().any(|cfg| cfg == &offer.source_config)
            || !cfgs.iter().any(|cfg| cfg == &offer.target_config)
        {
            return Err(error("source/target assignment advanced"));
        }
        Ok(())
    }
    fn register_source_capture(
        self: &Arc<Self>,
        capture: Arc<crate::DfsSourceAdoptionSourceCapture>,
        source_config: DfsMetadataShardConfig,
        target_config: DfsMetadataShardConfig,
        transfer_id: String,
        destination: crate::DfsObjectMetadata,
        job_offer: DfsSourceAdoptionJobOffer,
    ) -> Result<DfsSourceAdoptionOffer> {
        let offer = DfsSourceAdoptionOffer {
            source_node_id: self.node.clone(),
            session_id: sinomo_core::Id::generate().to_string(),
            object_id: capture.family().source_object.object_id.clone(),
            job_id: job_offer.job_id.clone(),
            job_offer,
            source_config,
            target_config,
            transfer_id,
            destination,
        };
        self.require_current(&offer)?;
        let original_job = Arc::new(self.bound_original_job(&offer, &capture.family().source_object.path)?);
        if !capture.scope().root().starts_with(&self.data_root)
            || capture.scope().file_system_id() != &offer.source_config.file_system_id
            || capture.scope().shard_id().0 != offer.source_config.shard_id
        {
            return Err(error("source capture lacks its actual physical owner"));
        }
        let generation = self.authority.shard_projection_state(
            &offer.source_config.file_system_id,
            &crate::DfsMetadataShardId::new(&offer.source_config.shard_id),
        );
        if !generation.1 {
            return Err(error("actual source scope is not ready"));
        }
        self.sessions
            .lock()
            .map_err(|_| error("source sessions poisoned"))?
            .insert(
                offer.session_id.clone(),
                Arc::new(SourceSession {
                    original_job,
                    capture,
                    generation: generation.0,
                    offer: offer.clone(),
                    claim: Arc::new(Mutex::new(None)),
                }),
            );
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .insert(
                (self.data_root.clone(), offer.session_id.clone()),
                Arc::clone(self),
            );
        Ok(offer)
    }

    fn verify_original_job(&self, offer: &DfsSourceAdoptionOffer, source_path: &str) -> Result<()> {
        self.bound_original_job(offer, source_path).map(drop)
    }

    pub(crate) fn bound_original_job(
        &self,
        offer: &DfsSourceAdoptionOffer,
        source_path: &str,
    ) -> Result<crate::DfsNamespaceJobRecord> {
        let reply = if offer.job_offer.coordinator_node_id == self.node {
            self.inspect_original_job(&self.node, &offer.job_offer)?
        } else {
            match self.call_rpc(
                &offer.job_offer.coordinator_node_id,
                &DfsSourceAdoptionRequest::InspectOriginalJob {
                    offer: offer.job_offer.clone(),
                },
            )? {
                DfsSourceAdoptionRpcResponse::OriginalJob(reply) => reply,
                _ => return Err(error("coordinator returned another native job proof kind")),
            }
        };
        if reply.offer != offer.job_offer
            || reply.job.job_id != offer.job_id
            || !job_custody::job_binds_destination(&reply.job, &offer.destination, source_path)
        {
            return Err(error(
                "actual original job does not bind this source family destination",
            ));
        }
        Ok(reply.job)
    }
    pub(crate) fn wake_reconciliation(self: &Arc<Self>) {
        if !self.has_pending_custody().unwrap_or(true)
            || self
                .reconciling
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let data_dir = sinomo_core::local::SinomoDataDir::new(&self.data_root);
        let host = match crate::runtime_host::registered_dfs_runtime_host(&data_dir) {
            Ok(Some(host)) => host,
            _ => {
                self.reconciling
                    .store(false, std::sync::atomic::Ordering::Release);
                return;
            }
        };
        let actor = self.clone();
        let wait_host = Arc::clone(&host);
        let task = self.runtime.spawn_blocking(move || {
            struct Reset(Arc<DfsSourceAdoptionActor>);
            impl Drop for Reset {
                fn drop(&mut self) {
                    self.0
                        .reconciling
                        .store(false, std::sync::atomic::Ordering::Release);
                }
            }
            let _reset = Reset(actor.clone());
            // Wait for the shared publication before this finite outcome pass
            // reads routing. A remount can retain real captures while its
            // current authoritative projection is still completing.
            let generation = actor.authority.generation();
            actor.runtime.block_on(async {
                tokio::select! {
                    _ = tokio::time::timeout(
                        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                        actor
                            .authority
                            .ensure_complete_prefix_for_generation_async(generation),
                    ) => {}
                    () = wait_host.wait_for_shutdown() => {}
                }
            });
            let current_generation = actor.authority.generation();
            let current_revision = actor
                .authority
                .retained_complete_prefix_revision_for_generation(current_generation);
            if current_revision.is_none() {
                tracing::warn!("native source outcome routing remains unavailable");
                return;
            }
            if let Err(error) = actor.reconcile_pending_once() {
                tracing::warn!(%error,"native source outcome remains unavailable");
            }
        });
        host.own_task(task);
    }
    /// A finite native recovery pass. No shadow journal or visibility-head
    /// inference substitutes for the exact retained target operation.
    pub(crate) fn reconcile_pending_once(&self) -> Result<usize> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| error("source sessions poisoned"))?
            .values()
            .cloned()
            .map(|session| (self, session))
            .collect::<Vec<_>>();
        for previous in &self.predecessors {
            sessions.extend(
                previous
                    .sessions
                    .lock()
                    .map_err(|_| error("retained source sessions poisoned"))?
                    .values()
                    .cloned()
                    .map(|session| (previous.as_ref(), session)),
            );
        }
        let mut released = 0;
        for (custody_owner, session) in sessions {
            let claim = session
                .claim
                .lock()
                .map_err(|_| error("source claim poisoned"))?
                .clone();
            let Some((scope, hash, digest)) = claim else {
                continue;
            };
            let cfgs = self
                .authority
                .configuration_snapshot()
                .metadata_shards(&scope.file_system_id)
                .map_err(error)?
                .ok_or_else(|| error("native outcome routing unavailable"))?;
            let target = cfgs
                .iter()
                .find(|cfg| cfg.shard_id == scope.shard_id.0)
                .ok_or_else(|| error("native target outcome scope missing"))?;
            tracing::trace!(target: "sinomo.dfs.source-adoption",
                node = %self.node, root = %self.data_root.display(),
                session = %session.offer.session_id, job = %session.offer.job_id,
                operation = %scope.operation_id, target = %target.primary_node_id,
                "native source custody outcome query");
            let reply = match self.call(
                &target.primary_node_id,
                &DfsSourceAdoptionRequest::InspectOutcome {
                    offer: session.offer.clone(),
                    target_scope: scope.clone(),
                    payload_digest: hash.clone(),
                    expected_digest: digest.clone(),
                },
            ) {
                Ok(reply) => reply,
                Err(error) => {
                    tracing::trace!(target: "sinomo.dfs.source-adoption",
                        node = %self.node, session = %session.offer.session_id,
                        operation = %scope.operation_id, target = %target.primary_node_id,
                        %error, "native source custody outcome query failed");
                    continue;
                }
            };
            tracing::trace!(target: "sinomo.dfs.source-adoption",
                node = %self.node, session = %session.offer.session_id,
                operation = %scope.operation_id, target = %target.primary_node_id,
                outcome = match reply.outcome.as_ref() {
                    Some(DfsSourceAdoptionOutcome::Unavailable) => "unavailable",
                    Some(DfsSourceAdoptionOutcome::Active) => "active",
                    Some(DfsSourceAdoptionOutcome::Committed { .. }) => "committed",
                    Some(DfsSourceAdoptionOutcome::Inactive { .. }) => "inactive",
                    None => "missing",
                },
                "native source custody outcome received");
            if reply.offer != session.offer
                || reply.target_scope != scope
                || reply.payload_digest != hash
            {
                return Err(error("native outcome response changed exact source claim"));
            }
            let terminal_digest = match reply.outcome {
                Some(DfsSourceAdoptionOutcome::Committed {
                    digest: actual,
                    commit,
                }) => {
                    commit.validate(&actual)?;
                    if actual.operation_id != scope.operation_id {
                        // Committed through a `recover:` replacement of the
                        // bound prepare; the release names that prepare.
                        match digest.as_ref() {
                            Some(bound) if commits_adoption(bound, &actual) => Some(bound.clone()),
                            _ => {
                                return Err(error("native terminal outcome changed exact digest"));
                            }
                        }
                    } else {
                        if actual.file_system_id != scope.file_system_id
                            || actual.shard_id != scope.shard_id
                            || actual.owner_epoch != scope.owner_epoch
                            || actual.payload_digest != hash
                            || digest.as_ref().is_some_and(|old| old != &actual)
                        {
                            return Err(error("native terminal outcome changed exact digest"));
                        }
                        Some(actual)
                    }
                }
                Some(DfsSourceAdoptionOutcome::Inactive { digest: actual, .. }) => {
                    if let Some(actual) = actual.as_ref() {
                        if actual.file_system_id != scope.file_system_id
                            || actual.shard_id != scope.shard_id
                            || actual.owner_epoch != scope.owner_epoch
                            || actual.operation_id != scope.operation_id
                            || actual.payload_digest != hash
                            || digest.as_ref().is_some_and(|old| old != actual)
                        {
                            return Err(error("retired native source claim changed digest"));
                        }
                    }
                    actual
                }
                // Active or unavailable: a prepare in the target's chain may
                // still commit. The session, its capture and its row fences
                // stay until a later wake reads a terminal outcome.
                _ => continue,
            };
            // The target atomically retired this offer against new acquisition
            // or supplied its exact native Commit. Drop only this same session.
            custody_owner.release_original_job(&session.offer, terminal_digest.as_ref())?;
            let mut registry = custody_owner
                .sessions
                .lock()
                .map_err(|_| error("source sessions poisoned"))?;
            if registry
                .get(&session.offer.session_id)
                .is_some_and(|current| Arc::ptr_eq(current, &session))
            {
                if let Some(actual) = terminal_digest {
                    custody_owner
                        .completed_sources
                        .lock()
                        .map_err(|_| error("completed sources poisoned"))?
                        .insert(
                            session.offer.session_id.clone(),
                            CompletedSource {
                                original_job: Arc::clone(&session.original_job),
                                offer: session.offer.clone(),
                                family: session.capture.family().clone(),
                                scope: scope.clone(),
                                hash: hash.clone(),
                                digest: actual,
                            },
                        );
                }
                registry.remove(&session.offer.session_id);
                RETAINED
                    .get_or_init(Default::default)
                    .lock()
                    .map_err(|_| error("source custody registry poisoned"))?
                    .remove(&(self.data_root.clone(), session.offer.session_id.clone()));
                released += 1;
                tracing::trace!(target: "sinomo.dfs.source-adoption",
                    node = %self.node, session = %session.offer.session_id,
                    job = %session.offer.job_id, operation = %scope.operation_id,
                    "native source custody released after verified terminal outcome");
            }
        }
        Ok(released)
    }
    fn has_pending_custody(&self) -> Result<bool> {
        let actors = std::iter::once(self).chain(self.predecessors.iter().map(Arc::as_ref));
        for actor in actors {
            if !actor
                .sessions
                .lock()
                .map_err(|_| error("source sessions poisoned"))?
                .is_empty()
                || !actor
                    .job_sessions
                    .lock()
                    .map_err(|_| error("original jobs poisoned"))?
                    .is_empty()
            {
                return Ok(true);
            }
            for preparation in actor
                .source_preparations
                .lock()
                .map_err(|_| error("source preparations poisoned"))?
                .values()
            {
                if preparation.holds_unbound()? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn cancel_undispatched_local_offer(&self, offer: &DfsSourceAdoptionOffer) -> Result<bool> {
        if offer.source_node_id != self.node || offer.job_offer.coordinator_node_id != self.node {
            return Ok(false);
        }
        // Presence and complete offer equality come from the actual retained
        // owner. Missing custody is not evidence authorizing a different release.
        drop(self.source_session(offer, false)?);
        self.cancel_unclaimed(offer)?;
        Ok(true)
    }

    pub(crate) fn cancel_unclaimed(&self, offer: &DfsSourceAdoptionOffer) -> Result<()> {
        if offer.source_node_id != self.node {
            return match self.call_rpc(
                &offer.source_node_id,
                &DfsSourceAdoptionRequest::CancelUnclaimed {
                    offer: offer.clone(),
                },
            )? {
                DfsSourceAdoptionRpcResponse::SourceCancelled { offer: actual }
                    if actual == *offer =>
                {
                    Ok(())
                }
                _ => Err(error("source cancellation changed exact native offer")),
            };
        }
        if !self
            .sessions
            .lock()
            .map_err(|_| error("source sessions poisoned"))?
            .contains_key(&offer.session_id)
        {
            for previous in &self.predecessors {
                if previous
                    .sessions
                    .lock()
                    .map_err(|_| error("retained source sessions poisoned"))?
                    .get(&offer.session_id)
                    .is_some_and(|session| session.offer == *offer)
                {
                    return previous.cancel_unclaimed(offer);
                }
            }
        }
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| error("source sessions poisoned"))?;
        if let Some(session) = sessions.get(&offer.session_id) {
            if session.offer != *offer {
                return Err(error("unclaimed source offer changed"));
            }
            if session
                .claim
                .lock()
                .map_err(|_| error("source claim poisoned"))?
                .is_some()
            {
                return Err(error("target may still own an admitted effect"));
            }
            let cancelled_scope = session.capture.scope().clone();
            let cancelled_generation = session.generation;
            let cancelled_storage_generation = self.authority.storage_generation();
            self.release_original_job(offer, None)?;
            sessions.remove(&offer.session_id);
            RETAINED
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| error("source custody registry poisoned"))?
                .remove(&(self.data_root.clone(), offer.session_id.clone()));
            // Only this present/exact/unclaimed guarded retirement revokes a
            // Bound memo. Missing/completed sessions prove no cancellation.
            // Release the registry before preparation locks (normal register
            // acquires those locks in the opposite nesting order).
            drop(sessions);
            self.revoke_cancelled_bound_preparation(
                offer,
                cancelled_scope,
                cancelled_generation,
                cancelled_storage_generation,
            )?;
        }
        Ok(())
    }
    pub(crate) fn handle(
        &self,
        authenticated_target: &NodeId,
        request: DfsSourceAdoptionRequest,
    ) -> Result<DfsSourceAdoptionResponse> {
        if let DfsSourceAdoptionRequest::InspectOutcome {
            offer,
            target_scope,
            payload_digest,
            expected_digest,
        } = &request
        {
            return self.inspect_target_outcome(
                authenticated_target,
                offer,
                target_scope,
                payload_digest,
                expected_digest.as_ref(),
            );
        }
        let (offer, scope, hash, digest, release) = match request {
            DfsSourceAdoptionRequest::AcquireSource { .. }
            | DfsSourceAdoptionRequest::BindSource { .. }
            | DfsSourceAdoptionRequest::InspectSourcePreparation { .. }
            | DfsSourceAdoptionRequest::RevokeSourcePreparation { .. } => {
                return Err(error(
                    "source preparation requires its typed native response lane",
                ));
            }
            DfsSourceAdoptionRequest::InspectOriginalJob { .. } => {
                return Err(error(
                    "original job inspection requires its typed response lane",
                ));
            }
            DfsSourceAdoptionRequest::ReleaseOriginalJob { .. } => {
                return Err(error(
                    "original job release requires its typed response lane",
                ));
            }
            DfsSourceAdoptionRequest::CompleteSource { .. } => {
                return Err(error(
                    "source completion requires its typed current effect lane",
                ));
            }
            DfsSourceAdoptionRequest::CancelUnclaimed { .. } => {
                return Err(error(
                    "source cancellation requires its typed response lane",
                ));
            }
            DfsSourceAdoptionRequest::Claim {
                offer,
                target_scope,
                payload_digest,
            } => (offer, Some(target_scope), Some(payload_digest), None, false),
            DfsSourceAdoptionRequest::Validate { offer, digest } => {
                (offer, None, None, Some(digest), false)
            }
            DfsSourceAdoptionRequest::Release { offer, digest } => {
                (offer, None, None, Some(digest), true)
            }
            DfsSourceAdoptionRequest::InspectOutcome { .. } => {
                unreachable!("handled by exact native inspection")
            }
        };
        if authenticated_target != &offer.target_config.primary_node_id {
            return Err(error("claim is not from the bound target primary"));
        }
        if !release {
            self.require_current(&offer)?;
        }
        let session = self.source_session(&offer, !release)?;
        if !release {
            let (generation, ready) = self.authority.shard_projection_state(
                &offer.source_config.file_system_id,
                &crate::DfsMetadataShardId::new(offer.source_config.shard_id.clone()),
            );
            if !ready || generation != session.generation {
                return Err(error("source scope generation advanced"));
            }
            self.verify_original_job(&offer, &session.capture.family().source_object.path)?;
        }
        if session.offer != offer {
            return Err(error("source offer identity changed"));
        }
        let mut claim = session
            .claim
            .lock()
            .map_err(|_| error("source claim poisoned"))?;
        if let (Some(scope), Some(hash)) = (scope, hash) {
            if scope.file_system_id != offer.target_config.file_system_id
                || scope.shard_id.0 != offer.target_config.shard_id
                || scope.owner_epoch != offer.target_config.epoch
                || hash.is_empty()
            {
                return Err(error("target payload scope changed"));
            }
            match claim.as_ref() {
                Some((old, old_hash, _)) if old != &scope || old_hash != &hash => {
                    return Err(error("target claim changed"));
                }
                None => *claim = Some((scope, hash, None)),
                _ => {}
            }
        }
        let (scope, hash, bound) = claim
            .as_mut()
            .ok_or_else(|| error("source was not claimed by a target"))?;
        if let Some(digest) = digest {
            if digest.file_system_id != scope.file_system_id
                || digest.shard_id != scope.shard_id
                || digest.owner_epoch != scope.owner_epoch
                || digest.operation_id != scope.operation_id
                || digest.payload_digest != *hash
            {
                return Err(error("target digest changed"));
            }
            match bound {
                Some(old) if old != &digest => {
                    return Err(error("target native prepare slot changed"));
                }
                None => *bound = Some(digest),
                _ => {}
            }
        }
        let response = DfsSourceAdoptionResponse {
            offer: offer.clone(),
            family: (!release).then(|| session.capture.family().clone()),
            target_scope: scope.clone(),
            payload_digest: hash.clone(),
            outcome: None,
        };
        drop(claim);
        if release {
            let released_digest =
                digest_for_release(&response.target_scope, &response.payload_digest, &session)?;
            self.release_original_job(&offer, released_digest.as_ref())?;
            let actual_digest = released_digest
                .ok_or_else(|| error("released target has no bound native digest"))?;
            self.completed_sources
                .lock()
                .map_err(|_| error("completed sources poisoned"))?
                .insert(
                    offer.session_id.clone(),
                    CompletedSource {
                        original_job: Arc::clone(&session.original_job),
                        offer: offer.clone(),
                        family: session.capture.family().clone(),
                        scope: response.target_scope.clone(),
                        hash: response.payload_digest.clone(),
                        digest: actual_digest,
                    },
                );
            self.sessions
                .lock()
                .map_err(|_| error("source session registry poisoned"))?
                .remove(&offer.session_id);
            RETAINED
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| error("source custody registry poisoned"))?
                .remove(&(self.data_root.clone(), offer.session_id.clone()));
        }
        Ok(response)
    }
    pub(crate) fn call(
        &self,
        target: &NodeId,
        request: &DfsSourceAdoptionRequest,
    ) -> Result<DfsSourceAdoptionResponse> {
        match self.call_rpc(target, request)? {
            DfsSourceAdoptionRpcResponse::SourceClaim(response) => Ok(response),
            _ => Err(error("source claim returned another native response kind")),
        }
    }
    pub(crate) fn handle_rpc(
        self: &Arc<Self>,
        authenticated_source: &NodeId,
        request: DfsSourceAdoptionRequest,
    ) -> Result<DfsSourceAdoptionRpcResponse> {
        if let DfsSourceAdoptionRequest::Release { offer, .. } = &request {
            if !self
                .sessions
                .lock()
                .map_err(|_| error("source sessions poisoned"))?
                .contains_key(&offer.session_id)
            {
                for previous in &self.predecessors {
                    if previous
                        .sessions
                        .lock()
                        .map_err(|_| error("retained sources poisoned"))?
                        .get(&offer.session_id)
                        .is_some_and(|session| session.offer == *offer)
                    {
                        return previous
                            .handle(authenticated_source, request)
                            .map(DfsSourceAdoptionRpcResponse::SourceClaim);
                    }
                }
            }
        }
        match &request {
            DfsSourceAdoptionRequest::RevokeSourcePreparation { token } => {
                return self
                    .revoke_unbound_preparation(authenticated_source, token)
                    .map(DfsSourceAdoptionRpcResponse::SourcePreparationRevoked);
            }
            DfsSourceAdoptionRequest::CancelUnclaimed { offer } => {
                if authenticated_source != &offer.job_offer.coordinator_node_id
                    && authenticated_source != &offer.target_config.primary_node_id
                {
                    return Err(error(
                        "source cancellation is not from its actual bound coordinator/target",
                    ));
                }
                self.cancel_unclaimed(offer)?;
                return Ok(DfsSourceAdoptionRpcResponse::SourceCancelled {
                    offer: offer.clone(),
                });
            }
            DfsSourceAdoptionRequest::InspectSourcePreparation { plan } => {
                return self
                    .inspect_source_preparation(authenticated_source, plan)
                    .map(
                        |prepared| DfsSourceAdoptionRpcResponse::SourcePreparationInspected {
                            prepared,
                        },
                    );
            }
            DfsSourceAdoptionRequest::AcquireSource { plan } => {
                return self
                    .acquire_remote_source(authenticated_source, plan.clone())
                    .map(DfsSourceAdoptionRpcResponse::SourcePrepared);
            }
            DfsSourceAdoptionRequest::BindSource { token, job_offer } => {
                return self
                    .bind_remote_source(authenticated_source, token.clone(), job_offer.clone())
                    .map(DfsSourceAdoptionRpcResponse::SourceBound);
            }
            DfsSourceAdoptionRequest::ReleaseOriginalJob { offer, digest } => {
                self.release_original_job_received(authenticated_source, offer, digest.as_ref())?;
                return Ok(DfsSourceAdoptionRpcResponse::OriginalJobReleased {
                    offer: offer.job_offer.clone(),
                });
            }
            DfsSourceAdoptionRequest::CompleteSource { offer } => {
                self.complete_source_received(authenticated_source, offer)?;
                return Ok(DfsSourceAdoptionRpcResponse::SourceCompleted {
                    offer: offer.clone(),
                });
            }
            _ => {}
        }
        if let DfsSourceAdoptionRequest::InspectOriginalJob { offer } = request {
            return self
                .inspect_original_job(authenticated_source, &offer)
                .map(DfsSourceAdoptionRpcResponse::OriginalJob);
        }
        self.handle(authenticated_source, request)
            .map(DfsSourceAdoptionRpcResponse::SourceClaim)
    }
    fn call_rpc(
        &self,
        target: &NodeId,
        request: &DfsSourceAdoptionRequest,
    ) -> Result<DfsSourceAdoptionRpcResponse> {
        self.call_rpc_before(target, request, None)
    }
    fn call_rpc_before(
        &self,
        target: &NodeId,
        request: &DfsSourceAdoptionRequest,
        deadline: Option<std::time::Instant>,
    ) -> Result<DfsSourceAdoptionRpcResponse> {
        // Shadow hosts deliberately have no DFS routes or source custody.
        let options = sinomo_core::network::ClusterObjectRpcOptions::default()
            .with_destination_process_affinity(
                sinomo_core::network::DestinationProcessAffinity::MainDaemon,
            );
        let options = if let Some(deadline) = deadline {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| error("native source phase deadline elapsed before dispatch"))?;
            options.with_overall_timeout(remaining)
        } else {
            options
        };
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            SOURCE_ADOPTION_ROUTE,
            crate::DFS_RPC_CATEGORY,
            request,
        )
        .map_err(error)?;
        let mut result = self
            .transport
            .call(
                target,
                sinomo_core::network::ObjectRpcCall::value(request),
                options,
                true,
            )
            .map_err(error)?;
        tokio::task::block_in_place(|| {
            self.runtime
                .block_on(result.materialize_value_body(64 * 1024 * 1024))
        })
        .map_err(error)?;
        result.response.decode_body().map_err(error)
    }
    pub(crate) fn claim(
        self: &Arc<Self>,
        offer: &DfsSourceAdoptionOffer,
        payload: &DfsPreparedOperationPayload,
    ) -> Result<Arc<DfsSourceAdoptionWitness>> {
        if *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
            || self.node != offer.target_config.primary_node_id
        {
            return Err(error("target actor is not actual target primary"));
        }
        let configs = self
            .authority
            .configuration_snapshot()
            .metadata_shards(&offer.target_config.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("actual target configuration unavailable"))?;
        if !configs.contains(&offer.target_config) {
            return Err(error(
                "actual target assignment advanced before source claim",
            ));
        }
        let scope = DfsPreparedOperationScope {
            file_system_id: payload.file_system_id.clone(),
            shard_id: payload.shard_id.clone(),
            owner_epoch: payload.owner_epoch,
            operation_id: payload.operation_id.clone(),
        };
        let hash = payload.canonical_digest()?;
        let mut target_claims = self
            .target_claims
            .lock()
            .map_err(|_| error("target custody registry poisoned"))?;
        let target = target_claims.entry(offer.session_id.clone()).or_default();
        if target.revoked {
            return Err(error("source effect claim was atomically retired"));
        }
        if target.witness.as_ref().and_then(Weak::upgrade).is_some() {
            return Err(error(
                "another actual target effect still holds source custody",
            ));
        }
        let reply = self.call(
            &offer.source_node_id,
            &DfsSourceAdoptionRequest::Claim {
                offer: offer.clone(),
                target_scope: scope.clone(),
                payload_digest: hash.clone(),
            },
        )?;
        if reply.offer != *offer || reply.target_scope != scope || reply.payload_digest != hash {
            return Err(error("source claim reply changed"));
        }
        let family = reply
            .family
            .ok_or_else(|| error("source omitted its actual family"))?;
        let witness = Arc::new(DfsSourceAdoptionWitness {
            actor: self.clone(),
            offer: offer.clone(),
            purpose: DfsSourceAdoptionPurpose::Adoption(family),
            target_scope: scope,
            target_payload_digest: hash,
            bound_digest: OnceLock::new(),
        });
        target.witness = Some(Arc::downgrade(&witness));
        #[cfg(test)]
        {
            drop(target_claims);
            let observer = CLAIM_OBSERVERS.lock().unwrap_or_else(|error| error.into_inner()).remove(&self.data_root);
            if let Some(observer) = observer { observer(self, offer); }
        }
        Ok(witness)
    }

    /// Borrow only the actual local original-job writer. This does not supply
    /// source-family authority; the ordinary current kernel/guard validates
    /// every metadata effect in the independently sealed body.
    pub(crate) fn job_writer_lease(
        self: &Arc<Self>,
        offer: &DfsSourceAdoptionOffer,
        payload: &DfsPreparedOperationPayload,
    ) -> Result<Option<Arc<DfsSourceAdoptionWitness>>> {
        if offer.job_offer.coordinator_node_id != self.node
            || offer.job_offer.job_config.file_system_id != payload.file_system_id
            || offer.job_offer.job_config.shard_id != payload.shard_id.0
        {
            return Ok(None);
        }
        self.inspect_original_job(&offer.source_node_id, &offer.job_offer)?;
        let mut pending_requests = 0usize;
        for effect in &payload.metadata_effects {
            match effect {
                crate::DfsPreparedMetadataEffect::PutNamespaceRequest { request }
                    if request.status == crate::DfsNamespaceRequestStatus::Pending
                        && request.file_system_id == offer.target_config.file_system_id
                        && request.authority_shard_id == payload.shard_id
                        && request.requester_node_id == offer.job_offer.coordinator_node_id =>
                {
                    match &request.operation {
                        crate::DfsNamespaceOperation::AdoptObjectAtPath {
                            source_offer,
                            object,
                            source_path,
                            ..
                        } if source_offer == offer && object == &offer.destination => {
                            let job = self.original_job_session(&offer.job_offer, true)?;
                            if source_path != &job.source_path {
                                return Err(error("pending request changes actual source path"));
                            }
                        }
                        _ => {
                            return Err(error(
                                "job writer lease belongs only to its exact pending adoption request",
                            ));
                        }
                    }
                    pending_requests += 1;
                }
                _ => {
                    return Err(error(
                        "job writer lease cannot change source/job/content effects",
                    ));
                }
            }
        }
        if pending_requests != 1
            || !payload.transaction.objects.is_empty()
            || !payload.transaction.versions.is_empty()
            || !payload.transaction.advisory_lock_states.is_empty()
            || !payload.content_receipts.is_empty()
            || payload.prepared_intent.is_some()
            || payload.terminal_intent.is_some()
            || payload.completed_request.is_some()
            || payload.completed_request_completion.is_some()
        {
            return Err(error(
                "job writer lease contains another effectful publication body",
            ));
        }
        if offer.target_config != offer.job_offer.job_config
            || payload.owner_epoch != offer.job_offer.job_config.epoch
        {
            return Err(error(
                "metadata body cannot borrow another original-job scope",
            ));
        }
        Ok(Some(Arc::new(DfsSourceAdoptionWitness {
            actor: Arc::clone(self),
            offer: offer.clone(),
            purpose: DfsSourceAdoptionPurpose::JobWriterLease,
            target_scope: DfsPreparedOperationScope {
                file_system_id: payload.file_system_id.clone(),
                shard_id: payload.shard_id.clone(),
                owner_epoch: payload.owner_epoch,
                operation_id: payload.operation_id.clone(),
            },
            target_payload_digest: payload.canonical_digest()?,
            bound_digest: OnceLock::new(),
        })))
    }
    fn inspect_kernel_terminal(
        &self,
        offer: &DfsSourceAdoptionOffer,
        scope: &DfsPreparedOperationScope,
        hash: &str,
        expected: Option<&DfsPreparedOperationDigest>,
        current: &DfsMetadataShardConfig,
        terminal: &NativeTargetTerminal,
    ) -> Result<Option<DfsSourceAdoptionOutcome>> {
        if terminal.offer != *offer
            || terminal.digest.file_system_id != scope.file_system_id
            || terminal.digest.shard_id != scope.shard_id
            || terminal.digest.owner_epoch != scope.owner_epoch
            || terminal.digest.operation_id != scope.operation_id
            || terminal.digest.payload_digest != hash
            || expected.is_some_and(|expected| expected != &terminal.digest)
        {
            return Err(error(
                "native target outcome changes its guarded kernel terminal identity",
            ));
        }
        terminal.commit.validate(&terminal.digest)?;
        if *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
            || current != &terminal.configuration
            || current != &offer.target_config
            || terminal.prefix.version != 2
            || !terminal.prefix.is_well_formed()
            || terminal.prefix.journal_sequence < terminal.commit.sequence
        {
            return Ok(None);
        }
        let (generation, ready) = self
            .authority
            .shard_projection_state(&scope.file_system_id, &scope.shard_id);
        if !ready || generation != terminal.generation {
            return Ok(None);
        }
        let root = crate::default_dfs_metadata_shards_root(
            &sinomo_core::local::SinomoDataDir::new(&self.data_root),
        )
        .join(crate::metadata_keys::key_segment(&scope.shard_id.0));
        let actual = crate::publication_guard::DfsPublicationOwnerScope::for_backend(
            &root,
            scope.file_system_id.clone(),
            scope.shard_id.clone(),
        )?;
        let registered = crate::runtime_host::publication_runtime_owner(&root).map_err(error)?;
        if actual != terminal.scope
            || registered
                .as_ref()
                .is_none_or(|owner| !Arc::ptr_eq(owner, &self._owner))
        {
            return Ok(None);
        }
        let before = crate::metadata_table_store::metadata_table_storage_generation(&root)?;
        if !terminal.storage.is_compatible_with(&before)
            || terminal.storage.active_log_len().is_some_and(|captured| {
                before
                    .active_log_len()
                    .is_none_or(|current| current < captured)
            })
        {
            return Ok(None);
        }
        let after = crate::metadata_table_store::metadata_table_storage_generation(&root)?;
        let current_scope = crate::publication_guard::DfsPublicationOwnerScope::for_backend(
            &root,
            scope.file_system_id.clone(),
            scope.shard_id.clone(),
        )?;
        let owner = crate::runtime_host::publication_runtime_owner(&root).map_err(error)?;
        // One publication supplies both the shard fence and its routing.
        let snapshot = self.authority.configuration_snapshot();
        let configurations = snapshot
            .metadata_shards(&scope.file_system_id)
            .map_err(error)?;
        if before != after
            || current_scope != terminal.scope
            || owner
                .as_ref()
                .is_none_or(|owner| !Arc::ptr_eq(owner, &self._owner))
            || snapshot.generation() != terminal.generation
            || !snapshot.contains_shard_key(&scope.file_system_id, &scope.shard_id)
            || configurations
                .as_ref()
                .is_none_or(|configurations| !configurations.contains(current))
            || *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
        {
            return Ok(None);
        }
        Ok(Some(DfsSourceAdoptionOutcome::Committed {
            digest: terminal.digest.clone(),
            commit: terminal.commit.clone(),
        }))
    }
    fn inspect_target_outcome(
        &self,
        source: &NodeId,
        offer: &DfsSourceAdoptionOffer,
        scope: &DfsPreparedOperationScope,
        hash: &str,
        expected: Option<&DfsPreparedOperationDigest>,
    ) -> Result<DfsSourceAdoptionResponse> {
        if source != &offer.source_node_id
            || scope.file_system_id != offer.target_config.file_system_id
            || scope.shard_id.0 != offer.target_config.shard_id
            || scope.owner_epoch != offer.target_config.epoch
        {
            return Err(error(
                "native outcome query changed authenticated source/scope",
            ));
        }
        let cfgs = self
            .authority
            .configuration_snapshot()
            .metadata_shards(&scope.file_system_id)
            .map_err(error)?
            .ok_or_else(|| error("native target outcome configuration unavailable"))?;
        let cfg = cfgs
            .iter()
            .find(|cfg| cfg.shard_id == scope.shard_id.0)
            .ok_or_else(|| error("target shard no longer configured"))?;
        if cfg.primary_node_id != self.node || cfg.epoch < scope.owner_epoch {
            return Err(error(
                "outcome lookup is not at the current native target primary",
            ));
        }
        let mut claims = self
            .target_claims
            .lock()
            .map_err(|_| error("target custody registry poisoned"))?;
        let state = claims.entry(offer.session_id.clone()).or_default();
        let retained = state
            .terminal
            .as_ref()
            .map(|terminal| {
                self.inspect_kernel_terminal(offer, scope, hash, expected, cfg, terminal)
            })
            .transpose()?
            .flatten();
        let outcome = if let Some(outcome) = retained {
            outcome
        } else if state.witness.as_ref().and_then(Weak::upgrade).is_some() {
            DfsSourceAdoptionOutcome::Active
        } else {
            // No live witness: at a target that took over, or once the
            // target's own effect ended without a commit. The shard's rows
            // decide; a prepare that can still commit reads as active.
            let outcome = self.retained_target_outcome(scope, hash, expected)?;
            if matches!(outcome, DfsSourceAdoptionOutcome::Inactive { .. }) {
                state.revoked = true;
            }
            outcome
        };
        Ok(DfsSourceAdoptionResponse {
            offer: offer.clone(),
            family: None,
            target_scope: scope.clone(),
            payload_digest: hash.into(),
            outcome: Some(outcome),
        })
    }
    /// The outcome this node's retained rows of the target shard give for the
    /// adoption prepared in `scope`: the outcome of its whole prepare chain.
    fn retained_target_outcome(
        &self,
        scope: &DfsPreparedOperationScope,
        hash: &str,
        expected: Option<&DfsPreparedOperationDigest>,
    ) -> Result<DfsSourceAdoptionOutcome> {
        let root = crate::default_dfs_metadata_shards_root(
            &sinomo_core::local::SinomoDataDir::new(&self.data_root),
        )
        .join(crate::metadata_keys::key_segment(&scope.shard_id.0));
        // An original coordinator's custody holds row fences, not this
        // shard's writer, so the lookup takes its own inspection fence.
        match crate::metadata_lock::try_metadata_inspection_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )? {
            None => {
                tracing::warn!(
                    target: "sinomo.dfs.source-adoption",
                    reason = "inspection-writer-unavailable",
                    node = %self.node,
                    file_system = %scope.file_system_id,
                    shard = %scope.shard_id.0.chars().take(96).collect::<String>(),
                    owner_epoch = scope.owner_epoch,
                    operation = %scope.operation_id.chars().take(192).collect::<String>(),
                    expected_prepare_sequence = ?expected.map(|digest| digest.prepare_sequence),
                    "native target outcome remains unavailable"
                );
                Ok(DfsSourceAdoptionOutcome::Unavailable)
            }
            Some(_actual_writer_cut) => {
                crate::LocalSobjMetadataBackend::open_for_inspection(scope.shard_id.clone(), root)?
                    .source_adoption_outcome(scope, hash, expected)
            }
        }
    }
}

/// The source custody an actor still holds for one offer.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct HeldSourceCustody {
    /// The target scope, payload digest and bound prepare of the target's
    /// claim, once a target claimed the source.
    pub(crate) claim: Option<(
        DfsPreparedOperationScope,
        String,
        Option<DfsPreparedOperationDigest>,
    )>,
    /// The physical scope the source family is fenced in.
    pub(crate) scope: crate::publication_guard::DfsPublicationOwnerScope,
    pub(crate) object_id: crate::DfsObjectId,
}
#[cfg(test)]
impl HeldSourceCustody {
    /// Whether the source family is still fenced.
    pub(crate) fn fenced(&self) -> bool {
        crate::publication_guard::source_rows_fenced(&self.scope, &self.object_id)
    }
}
#[cfg(test)]
impl DfsSourceAdoptionActor {
    /// The custody this actor, or a retained predecessor, still holds for
    /// `offer`.
    pub(crate) fn held_source_custody(
        &self,
        offer: &DfsSourceAdoptionOffer,
    ) -> Option<HeldSourceCustody> {
        let session = std::iter::once(self)
            .chain(self.predecessors.iter().map(Arc::as_ref))
            .find_map(|actor| {
                actor
                    .sessions
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&offer.session_id)
                    .cloned()
            })?;
        let claim = session
            .claim
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        Some(HeldSourceCustody {
            claim,
            scope: session.capture.scope().clone(),
            object_id: session.capture.family().source_object.object_id.clone(),
        })
    }
    /// Ask `target`, as `offer`'s source, for the outcome of the adoption
    /// prepared in `scope`, as reconciliation asks.
    pub(crate) fn query_target_outcome(
        &self,
        target: &NodeId,
        offer: &DfsSourceAdoptionOffer,
        scope: &DfsPreparedOperationScope,
        hash: &str,
        expected: Option<&DfsPreparedOperationDigest>,
    ) -> Result<Option<DfsSourceAdoptionOutcome>> {
        let reply = self.call(
            target,
            &DfsSourceAdoptionRequest::InspectOutcome {
                offer: offer.clone(),
                target_scope: scope.clone(),
                payload_digest: hash.into(),
                expected_digest: expected.cloned(),
            },
        )?;
        Ok(reply.outcome)
    }
    /// What this node's rows of the target shard give for the adoption
    /// prepared in `scope`, with no live witness.
    pub(crate) fn retained_target_outcome_for_test(
        &self,
        scope: &DfsPreparedOperationScope,
        hash: &str,
        expected: Option<&DfsPreparedOperationDigest>,
    ) -> Result<DfsSourceAdoptionOutcome> {
        self.retained_target_outcome(scope, hash, expected)
    }
}
/// Whether the source would accept `committed` as the commit of the
/// adoption that its target prepared as `original`.
#[cfg(test)]
pub(crate) fn commits_adoption_for_test(
    original: &DfsPreparedOperationDigest,
    committed: &DfsPreparedOperationDigest,
) -> bool {
    commits_adoption(original, committed)
}
/// Count-changing promotion is authorized by the same retained original job
/// as the source family. Its selected destination alias remains an atomic
/// prepare predicate, so a vanished/replaced alias cannot become an ordinary
/// move with an incorrectly decremented canonical count.
fn validate_adoption_namespace_count(
    job: &crate::DfsNamespaceJobRecord,
    source: &crate::DfsObjectMetadata,
    destination: &crate::DfsObjectMetadata,
    payload: &DfsPreparedOperationPayload,
) -> Result<()> {
    if let Some(plan) = job.directory_move_counts()?.and_then(|counts| counts.parent_participants.as_ref()) {
        plan.validate_for_job(job)?;
        if plan.phase != crate::DfsParentCountPhase::Issued {
            return Err(error("parent-bound namespace effects have no durable issuance decision"));
        }
    }
    let mut promotion = None;
    for frame in &job.pending_frames {
        if let crate::DfsNamespaceJobFrame::CrossShardAdopt {
            moved_object, old_path, promotion_alias: Some(alias), ..
        } = frame {
            if moved_object == destination
                && crate::namespace::metadata_path(old_path) == crate::namespace::metadata_path(&source.path)
            {
                if promotion.as_ref().is_some_and(|prior| *prior != alias) {
                    return Err(error("original promotion job changes its selected alias"));
                }
                promotion = Some(alias);
            }
        }
    }
    if source.object_id != destination.object_id || source.file_system_id != destination.file_system_id
        || source.kind != destination.kind
        || (promotion.is_some() && source.kind == crate::DfsObjectKind::Directory)
    {
        return Err(error("counted adoption changed its canonical identity or kind"));
    }
    if source.kind == crate::DfsObjectKind::Hardlink {
        if promotion.is_some() || destination.kind != source.kind
            || source.namespace_link_count.is_some() || destination.namespace_link_count.is_some()
        {
            return Err(error("alias adoption cannot author a canonical namespace count"));
        }
        return Ok(());
    }
    let expected_count = crate::posix::updated_namespace_link_count(
        source, if promotion.is_some() { -1 } else { 0 },
    ).map_err(error)?;
    if destination.namespace_link_count != Some(expected_count) {
        return Err(error("adoption changes the original canonical namespace count"));
    }
    if let Some(alias) = promotion {
        let changes_namespace = !payload.transaction.objects.is_empty()
            || payload.metadata_effects.iter().any(|effect| matches!(effect,
                crate::DfsPreparedMetadataEffect::PutObject { .. }
                | crate::DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction { .. }));
        // Exact replay may reconcile retained content without rewriting any
        // namespace row. It must not restore or decrement the canonical head.
        if !changes_namespace {
            return Ok(());
        }
        if !payload.transaction.objects.iter().any(|object|
            object.object_id == destination.object_id
                && object.namespace_link_count == destination.namespace_link_count)
            || !payload.expected_object_versions.iter().any(|expected|
                expected.object_id == alias.object_id
                    && expected.expected_version.as_ref() == Some(&alias.version))
            || !payload.expected_path_occupants.iter().any(|expected|
                crate::namespace::metadata_path(&expected.path)
                    == crate::namespace::metadata_path(&destination.path)
                    && expected.expected_object_id.as_ref() == Some(&alias.object_id))
        {
            return Err(error("promotion prepare omits the original selected alias predicate"));
        }
    }
    Ok(())
}

/// Live, private source authorization; serialized source rows create none.
pub struct DfsSourceAdoptionWitness {
    actor: Arc<DfsSourceAdoptionActor>,
    offer: DfsSourceAdoptionOffer,
    purpose: DfsSourceAdoptionPurpose,
    target_scope: DfsPreparedOperationScope,
    target_payload_digest: String,
    bound_digest: OnceLock<DfsPreparedOperationDigest>,
}
enum DfsSourceAdoptionPurpose {
    Adoption(DfsCrossShardSourceFamily),
    JobWriterLease,
}
impl std::fmt::Debug for DfsSourceAdoptionWitness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfsSourceAdoptionWitness")
            .field("offer", &self.offer)
            .field("target", &self.target_scope)
            .finish_non_exhaustive()
    }
}
impl DfsSourceAdoptionWitness {
    /// Reuse only the exact job-root writer already held by this actual node.
    /// This shares real custody; fresh target authorization remains separate.
    pub(crate) fn target_writer_custody(
        &self,
        scope: &crate::DfsPublicationOwnerScope,
    ) -> Result<Option<crate::metadata_backend::DfsSourceAdoptionWriterCustody>> {
        if self.offer.job_offer.coordinator_node_id != self.actor.node {
            return Ok(None);
        }
        let session = self
            .actor
            .original_job_session(&self.offer.job_offer, true)?;
        if session.capture.scope() != scope {
            return Ok(None);
        }
        self.actor
            .inspect_original_job(&self.offer.source_node_id, &self.offer.job_offer)?;
        if session.target != self.offer.target_config
            || session.offer != self.offer.job_offer
            || self.target_scope.file_system_id != *scope.file_system_id()
            || self.target_scope.shard_id != *scope.shard_id()
            || self.target_scope.owner_epoch != session.offer.job_config.epoch
        {
            return Err(error(
                "target differs from actual retained original-job writer",
            ));
        }
        let custody = session.capture.writer_custody();
        custody.validate_scope(scope)?;
        Ok(Some(custody))
    }
    pub(crate) fn validate_for<'a>(
        &'a self,
        payload: &DfsPreparedOperationPayload,
        digest: &DfsPreparedOperationDigest,
    ) -> Result<Option<&'a DfsCrossShardSourceFamily>> {
        if digest.file_system_id != self.target_scope.file_system_id
            || digest.shard_id != self.target_scope.shard_id
            || digest.owner_epoch != self.target_scope.owner_epoch
            || digest.operation_id != self.target_scope.operation_id
            || digest.payload_digest != self.target_payload_digest
            || DfsPreparedOperationDigest::at_sequence(payload, digest.prepare_sequence)? != *digest
        {
            return Err(error("target body/digest changed"));
        }
        let bound = self.bound_digest.get_or_init(|| digest.clone());
        if bound != digest {
            return Err(error("actual target prepare slot changed"));
        }
        if self.supplies_source_family() {
            if payload.completed_request.as_ref().is_some_and(|request| {
                request.requester_node_id != self.offer.job_offer.coordinator_node_id
            }) || payload
                .completed_request_completion
                .as_ref()
                .is_some_and(|completion| {
                    completion.requester_node_id != self.offer.job_offer.coordinator_node_id
                })
            {
                return Err(error(
                    "adoption body changes its independently verified original coordinator",
                ));
            }
        }
        if matches!(self.purpose, DfsSourceAdoptionPurpose::JobWriterLease) {
            self.actor
                .inspect_original_job(&self.offer.source_node_id, &self.offer.job_offer)?;
            let session = self
                .actor
                .job_sessions
                .lock()
                .map_err(|_| error("original jobs poisoned"))?
                .get(&self.offer.job_offer.session_id)
                .cloned()
                .ok_or_else(|| error("job writer lease lost its native capture"))?;
            if self
                .target_writer_custody(session.capture.scope())?
                .is_none()
            {
                return Err(error("job writer lease lost its actual custody"));
            }
            return Ok(None);
        }
        if let DfsSourceAdoptionPurpose::Adoption(family) = &self.purpose {
            let original_job = self.actor.bound_original_job(
                &self.offer, &family.source_object.path,
            )?;
            validate_adoption_namespace_count(
                &original_job, &family.source_object, &self.offer.destination, payload,
            )?;
        }
        let reply = self.actor.call(
            &self.offer.source_node_id,
            &DfsSourceAdoptionRequest::Validate {
                offer: self.offer.clone(),
                digest: digest.clone(),
            },
        )?;
        if reply.offer != self.offer
            || reply.target_scope != self.target_scope
            || reply.payload_digest != self.target_payload_digest
        {
            return Err(error("source validation response changed"));
        }
        let DfsSourceAdoptionPurpose::Adoption(family) = &self.purpose else {
            unreachable!()
        };
        Ok(Some(family))
    }
    pub(crate) fn record_kernel_terminal(
        &self,
        scope: &crate::publication_guard::DfsPublicationOwnerScope,
        configuration: &DfsMetadataShardConfig,
        owner: &Arc<DfsDataOwnerLock>,
        digest: &DfsPreparedOperationDigest,
        commit: &crate::DfsPreparedOperationCommitReference,
        prefix: &crate::DfsMetadataPrefixProof,
        storage: crate::metadata_table_store::MetadataTableStorageGeneration,
    ) -> Result<()> {
        if !self.supplies_source_family() {
            return Ok(());
        }
        commit.validate(digest)?;
        if !Arc::ptr_eq(owner, &self.actor._owner)
            || configuration != &self.offer.target_config
            || configuration.primary_node_id != self.actor.node
            || scope.file_system_id() != &digest.file_system_id
            || scope.shard_id() != &digest.shard_id
            || digest.file_system_id != self.target_scope.file_system_id
            || digest.shard_id != self.target_scope.shard_id
            || digest.owner_epoch != self.target_scope.owner_epoch
            || digest.operation_id != self.target_scope.operation_id
            || digest.payload_digest != self.target_payload_digest
            || prefix.version != 2
            || !prefix.is_well_formed()
            || prefix.journal_sequence < commit.sequence
        {
            return Err(error(
                "kernel terminal cut changes its actual source witness/owner/scope",
            ));
        }
        let bound = self.bound_digest.get_or_init(|| digest.clone());
        if bound != digest {
            return Err(error(
                "kernel terminal cut changes the actual native prepare slot",
            ));
        }
        let (generation, ready) = self
            .actor
            .authority
            .shard_projection_state(&digest.file_system_id, &digest.shard_id);
        if !ready || *self.actor.shutdown.borrow() || self.actor.shutdown.has_changed().is_err() {
            return Err(error(
                "kernel terminal cut lost its current native target context",
            ));
        }
        let mut claims = self
            .actor
            .target_claims
            .lock()
            .map_err(|_| error("target custody registry poisoned"))?;
        let state = claims
            .get_mut(&self.offer.session_id)
            .ok_or_else(|| error("kernel terminal cut has no actual target claim"))?;
        if state.revoked
            || state
                .witness
                .as_ref()
                .and_then(Weak::upgrade)
                .is_none_or(|witness| !std::ptr::eq(witness.as_ref(), self))
        {
            return Err(error(
                "kernel terminal cut is not its actual claimed source witness",
            ));
        }
        if let Some(terminal) = &state.terminal {
            if terminal.offer != self.offer
                || terminal.scope != *scope
                || terminal.configuration != *configuration
                || terminal.digest != *digest
                || terminal.commit != *commit
            {
                return Err(error(
                    "kernel terminal replay changes its original native outcome",
                ));
            }
            return Ok(());
        }
        state.terminal = Some(NativeTargetTerminal {
            offer: self.offer.clone(),
            scope: scope.clone(),
            configuration: configuration.clone(),
            generation,
            storage,
            prefix: prefix.clone(),
            digest: digest.clone(),
            commit: commit.clone(),
        });
        Ok(())
    }
    pub(crate) fn supplies_source_family(&self) -> bool {
        matches!(self.purpose, DfsSourceAdoptionPurpose::Adoption(_))
    }
    pub(crate) fn offer_session_id(&self) -> &str {
        &self.offer.session_id
    }
    pub(crate) fn release_after_effect(&self, digest: &DfsPreparedOperationDigest) -> Result<()> {
        if !self.supplies_source_family() {
            return Ok(());
        }
        let reply = self.actor.call(
            &self.offer.source_node_id,
            &DfsSourceAdoptionRequest::Release {
                offer: self.offer.clone(),
                digest: digest.clone(),
            },
        )?;
        if reply.offer != self.offer
            || reply.target_scope != self.target_scope
            || reply.payload_digest != self.target_payload_digest
        {
            return Err(error("source release response changed"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum RemoteNamespacePhase {
    PrepareSource {
        actor: Arc<DfsSourceAdoptionActor>,
        plan: SourcePreparationPlan,
        /// The requester's budget, if the request this phase runs for set one.
        deadline: Option<std::time::Instant>,
    },
    PublishTarget(RemoteTargetNamespacePhase),
    /// A file sync waits for repair progress on its object's primary with
    /// the lane and admission released, so the executor and its replies run.
    AwaitRepairProgress(AwaitRepairProgressPhase),
    /// A request on several shard lanes reads another node's shard from that
    /// shard's primary with its lanes and admission released, so the
    /// answering primary never waits on a lane the request holds.
    ReadAtPrimary(RemoteReadNamespacePhase),
    /// A served request whose job another driver's step holds waits for that
    /// step's claim to be released with its lanes and admission released,
    /// then runs again and reads its job afresh.
    AwaitJobStepClaim(AwaitJobStepClaimPhase),
}
#[derive(Debug)]
pub(crate) struct AwaitJobStepClaimPhase {
    pub(crate) node_id: sinomo_core::node::NodeId,
    pub(crate) job_id: String,
    pub(crate) deadline: Option<std::time::Instant>,
}
#[derive(Debug)]
pub(crate) struct RemoteReadNamespacePhase {
    /// The read's identity within its request, the same on the re-run.
    pub(crate) key: String,
    pub(crate) read: RemoteTargetNamespacePhase,
}
#[derive(Debug)]
pub(crate) struct AwaitRepairProgressPhase {
    pub(crate) node_id: sinomo_core::node::NodeId,
    pub(crate) object_id: crate::DfsObjectId,
    pub(crate) version: crate::DfsVersion,
    pub(crate) seen: u64,
    pub(crate) deadline: Option<std::time::Instant>,
}
#[derive(Debug)]
pub(crate) struct RemoteTargetNamespacePhase {
    pub(crate) actor: Arc<DfsSourceAdoptionActor>,
    pub(crate) command: crate::DfsNamespaceCommand,
    pub(crate) pending: crate::service_state::PendingRoutedNamespaceOp,
    /// The requester's budget, if the request this phase runs for set one.
    pub(crate) deadline: Option<std::time::Instant>,
}
#[derive(Debug)]
pub(crate) enum CompletedRemoteNamespacePhase {
    SourcePrepared {
        request_id: String,
        prepared: PreparedSourceAdoption,
    },
    TargetPublished(CompletedRemoteTargetNamespacePhase),
    /// A file sync's repair wait ended: progress advanced or the deadline came.
    RepairProgress {
        object_id: crate::DfsObjectId,
        version: crate::DfsVersion,
        deadline: Option<std::time::Instant>,
    },
    /// The step claim a request waited for was released, or the request's
    /// deadline came.
    JobStepClaimReleased,
    /// A read the request yielded came back from its primary, or failed as
    /// its send would have failed on the request's own run.
    ReadAnswered {
        key: String,
        answer: std::result::Result<CompletedRemoteTargetNamespacePhase, crate::DfsError>,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct CompletedRemoteTargetNamespacePhase {
    pub(crate) command: crate::DfsNamespaceCommand,
    pub(crate) pending: crate::service_state::PendingRoutedNamespaceOp,
    pub(crate) response: crate::DfsNamespaceResponse,
    pub(crate) target: NodeId,
}
impl RemoteNamespacePhase {
    /// The host has not returned this phase for dispatch. Release only an
    /// exact local source/job offer that the real actor still proves unclaimed.
    pub(crate) fn cancel_local_before_failed_restore(&self) -> Result<bool> {
        match self {
            Self::PublishTarget(phase) => match phase.source_offer()? {
                Some(offer) => phase.actor.cancel_undispatched_local_offer(offer),
                None => Ok(false),
            },
            _ => Ok(false),
        }
    }

    pub(crate) fn cancel_before_dispatch(&self) -> Result<()> {
        match self {
            Self::PrepareSource { .. }
            | Self::AwaitRepairProgress(_)
            | Self::ReadAtPrimary(_)
            | Self::AwaitJobStepClaim(_) => Ok(()),
            Self::PublishTarget(phase) => match phase.source_offer()? {
                Some(offer) => phase.actor.cancel_unclaimed(offer),
                None => Ok(()),
            },
        }
    }
    /// Runs after the normal host releases its mutation/service lane. The
    /// actor registry continues to own the actual physical source fences.
    pub(crate) fn dispatch(
        self,
    ) -> std::result::Result<CompletedRemoteNamespacePhase, crate::DfsError> {
        match self {
            Self::PrepareSource {
                actor,
                plan,
                deadline,
            } => {
                if deadline.is_some_and(|deadline| deadline <= std::time::Instant::now()) {
                    return Err(crate::DfsError::NetworkDelivery {
                        delivery_state:
                            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                        message: "native source preparation expired before dispatch".into(),
                    });
                }
                let request_id = plan.request_id.clone();
                let prepared = actor.prepare_cross_node_source(plan, deadline)?;
                Ok(CompletedRemoteNamespacePhase::SourcePrepared {
                    request_id,
                    prepared,
                })
            }
            Self::PublishTarget(phase) => phase
                .dispatch()
                .map(CompletedRemoteNamespacePhase::TargetPublished),
            Self::AwaitRepairProgress(phase) => {
                crate::repair_executor::repair_executor_wait_for_progress(
                    &phase.node_id,
                    phase.seen,
                    phase.deadline,
                );
                Ok(CompletedRemoteNamespacePhase::RepairProgress {
                    object_id: phase.object_id,
                    version: phase.version,
                    deadline: phase.deadline,
                })
            }
            Self::AwaitJobStepClaim(phase) => {
                crate::maintenance::job_claims::wait_for_release(
                    &phase.node_id,
                    &phase.job_id,
                    phase.deadline,
                );
                Ok(CompletedRemoteNamespacePhase::JobStepClaimReleased)
            }
            // The read's own failure is its answer: the re-run meets it where
            // the read would have been sent.
            Self::ReadAtPrimary(phase) => Ok(CompletedRemoteNamespacePhase::ReadAnswered {
                key: phase.key,
                answer: phase.read.dispatch(),
            }),
        }
    }
}
#[cfg(test)]
impl RemoteNamespacePhase {
    pub(crate) fn trace_undispatched_custody(&self) {
        let Self::PublishTarget(phase) = self else { return; };
        let Ok(Some(offer)) = phase.source_offer() else {
            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "undispatched-namespace-custody", source_offer = false, operation = ?phase.command.operation);
            return;
        };
        let observation = std::iter::once(phase.actor.as_ref()).chain(phase.actor.predecessors.iter().map(Arc::as_ref))
            .find_map(|actor| actor.sessions.lock().ok().and_then(|sessions| sessions.get(&offer.session_id).map(|session| (
                session.offer == *offer,
                session.claim.try_lock().ok().map(|claim| claim.is_some()),
                format!("{:?}", session.capture.scope()),
                Arc::strong_count(&session.capture),
            ))));
        tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "undispatched-namespace-custody", source_offer = true,
            source_session = %offer.session_id, source_node = %offer.source_node_id, job = %offer.job_id,
            actor_node = %phase.actor.node, target = %phase.pending.primary_node_id, held_source = ?observation);
    }
}

/// Call `target`. When `requester` is set, the call is driven as a future and
/// ends as soon as the requester leaves: the serving node sees its stream end.
fn call_until_requester_leaves(
    transport: &dyn sinomo_module::ComponentObjectRpcTransport,
    runtime: &tokio::runtime::Handle,
    target: &sinomo_core::node::NodeId,
    call: sinomo_core::network::ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    requester: Option<crate::DfsCancellationFlag>,
) -> std::result::Result<sinomo_core::network::ObjectRpcResult, sinomo_core::network::NetworkError>
{
    let Some(requester) = requester else {
        return transport.call(target, call, options, true);
    };
    let run = || {
        runtime.block_on(async {
            tokio::select! {
                biased;
                () = requester.cancelled() => Err(sinomo_core::network::NetworkError::delivery_failed(
                    "read with lanes released",
                    sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    "the requester left before the read was answered",
                )),
                result = transport.call_async(target, call, options, true) => result,
            }
        })
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(run)
        }
        _ => run(),
    }
}

impl RemoteTargetNamespacePhase {
    fn source_offer(&self) -> Result<Option<&DfsSourceAdoptionOffer>> {
        match &self.command.operation {
            crate::DfsNamespaceOperation::AdoptObjectAtPath { source_offer, .. } => {
                Ok(Some(source_offer))
            }
            crate::DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
            | crate::DfsNamespaceOperation::AdoptHardlinkAlias { .. }
            | crate::DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
            | crate::DfsNamespaceOperation::ParentCountParticipant { .. }
            | crate::DfsNamespaceOperation::ReleaseDirectoryMoveDestination { .. }
            | crate::DfsNamespaceOperation::CommitHardlinkTargetFence { .. }
            | crate::DfsNamespaceOperation::LookupPath { .. }
            | crate::DfsNamespaceOperation::LookupPaths { .. }
            | crate::DfsNamespaceOperation::MetadataQuery { .. } => Ok(None),
            _ => Err(error(
                "owned target phase contains another namespace operation",
            )),
        }
    }
    fn dispatch(self) -> std::result::Result<CompletedRemoteTargetNamespacePhase, crate::DfsError> {
        let actor = self.actor.clone();
        let local_offer = self
            .source_offer()?
            .filter(|offer| {
                offer.source_node_id == actor.node
                    && offer.job_offer.coordinator_node_id == actor.node
            })
            .cloned();
        let outcome = self.dispatch_inner();
        if outcome.as_ref().is_ok_and(|completed| {
            !matches!(
                completed.response.result,
                crate::DfsNamespaceResult::Applied
            )
        }) {
            // A refused target may have claimed the source before refusing:
            // settle that claim now so its row fences end with it.
            actor.wake_reconciliation();
        }
        if outcome.is_err() {
            // Delivery uncertainty says nothing about current source custody.
            // The existing guarded cancellation can revoke only an exact,
            // still-unclaimed offer and excludes every later claim atomically.
            // A claimed effect keeps its writers and original recovery owner.
            // Restrict this cleanup to local custody: no fresh RPC deadline is
            // added after the original carrier budget has expired.
            if let Some(offer) = local_offer {
                if let Err(failure) = actor.cancel_unclaimed(&offer) {
                    tracing::debug!(source_session=%offer.session_id, job=%offer.job_id,
                        error=%failure, "failed carrier retains claimed or unresolved source custody");
                }
            }
            actor.wake_reconciliation();
        }
        outcome
    }
    fn dispatch_inner(
        self,
    ) -> std::result::Result<CompletedRemoteTargetNamespacePhase, crate::DfsError> {
        let offer = self.source_offer()?;
        // Without a budget the call ends on its answer or the network's
        // verdict.
        let remaining = self.deadline.map(|deadline| {
            deadline
                .checked_duration_since(std::time::Instant::now())
                .filter(|value| !value.is_zero())
        });
        let Some(remaining) = remaining.map_or(Some(None), |remaining| remaining.map(Some)) else {
            if let Some(offer) = offer {
                self.actor
                    .cancel_unclaimed(offer)
                    .map_err(crate::DfsError::from)?;
            }
            return Err(crate::DfsError::NetworkDelivery {
                delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                message: "remote namespace phase expired before dispatch".into(),
            });
        };
        let frame = self
            .actor
            .next_frame
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let message = crate::build_dfs_routed_message(
            &self.actor.node,
            &self.pending.primary_node_id,
            frame,
            frame,
            crate::DfsNetworkMessage::NamespaceRequest(self.command.clone()),
        );
        let (envelope, call) = crate::dfs_routed_call_rpc_call(crate::DfsRoutedCall {
            message,
            attachment: None,
        })
        .map_err(crate::DfsError::from_network)?;
        // A test pauses an owned target phase; a read is not one.
        #[cfg(any(test, feature = "test-support"))]
        if !matches!(
            self.command.operation,
            crate::DfsNamespaceOperation::LookupPath { .. }
                | crate::DfsNamespaceOperation::LookupPaths { .. }
                | crate::DfsNamespaceOperation::MetadataQuery { .. }
        ) {
            crate::source_adoption_test::capture_and_pause(
                &self.actor.data_root,
                &envelope,
                &self.actor.runtime,
            );
        }
        let options = sinomo_core::network::ClusterObjectRpcOptions::default()
            .with_destination_process_affinity(
                sinomo_core::network::DestinationProcessAffinity::MainDaemon,
            );
        let (options, call) = match remaining {
            Some(remaining) => (options.with_overall_timeout(remaining), {
                let mut call = call;
                call.request = crate::routes::with_dfs_request_budget(call.request, remaining);
                call
            }),
            None => (options, call),
        };
        let mut result = match call_until_requester_leaves(
            self.actor.transport.as_ref(),
            &self.actor.runtime,
            &self.pending.primary_node_id,
            call,
            options,
            crate::node_transport::current_dfs_send_cancellation(),
        ) {
            Ok(result) => result,
            Err(cause) => {
                // THIS native carrier attests that this attempt never reached
                // the target. Release only its exact still-unclaimed source
                // offer through the existing custody owner. The durable job
                // and original command remain unfinished for normal recovery.
                // An earlier admitted claim makes cancellation fail closed;
                // unknown delivery and all post-dispatch failures retain it.
                // Both custodians must be this actor: local retirement adds no
                // network wait beyond the original absolute operation budget.
                if cause.delivery_state()
                    == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
                    && let Some(offer) = offer
                    && offer.source_node_id == self.actor.node
                    && offer.job_offer.coordinator_node_id == self.actor.node
                {
                    if let Err(error) = self.actor.cancel_unclaimed(offer) {
                        tracing::warn!(
                            source_session = %offer.session_id,
                            job = %offer.job_id,
                            carrier_error = %cause,
                            custody_error = %error,
                            "definitely-not-sent namespace source custody could not be cancelled"
                        );
                        return Err(crate::DfsError::from(error));
                    }
                }
                return Err(crate::DfsError::from_network(cause));
            }
        };
        tokio::task::block_in_place(|| {
            self.actor
                .runtime
                .block_on(result.materialize_value_body(64 * 1024 * 1024))
        })
        .map_err(crate::DfsError::from_network)?;
        let response: crate::DfsMessageResponse = result
            .response
            .decode_body()
            .map_err(crate::DfsError::from_network)?;
        if response.request_id != envelope.request_id {
            return Err(crate::DfsError::NetworkDelivery {
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                message: "remote namespace response changed its native envelope identity".into(),
            });
        }
        let namespace = response
            .outbound_messages
            .into_iter()
            .find_map(|message| {
                if message.source_node_id != self.pending.primary_node_id
                    || message.target_node_id != self.actor.node
                {
                    return None;
                }
                match message.message {
                    crate::DfsNetworkMessage::NamespaceResponse(response)
                        if response.request_id == self.command.request_id =>
                    {
                        Some(response)
                    }
                    _ => None,
                }
            })
            .ok_or_else(|| crate::DfsError::NetworkDelivery {
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                message: "remote namespace phase omitted its correlated native response".into(),
            })?;
        if matches!(namespace.result, crate::DfsNamespaceResult::Applied)
            && let Some(offer) = offer
        {
            self.actor.complete_remote_source(offer)?;
        }
        // Source release is performed by the actual target commit. A rejected
        // or missing response does not assert that an admitted effect ended.
        // A retained exact completion does not claim a new source session.
        // Cancelling an unclaimed session is safe even after dispatch: every
        // fresh target effect must first acquire that same session.
        if let Some(offer) = offer {
            let _ = self.actor.cancel_unclaimed(offer);
        }
        Ok(CompletedRemoteTargetNamespacePhase {
            target: self.pending.primary_node_id.clone(),
            command: self.command,
            pending: self.pending,
            response: namespace,
        })
    }
}
impl<T: crate::DfsMessageTransport> crate::NetworkedDfsService<T> {
    pub(crate) fn complete_remote_namespace_phase(
        &mut self,
        completed: CompletedRemoteNamespacePhase,
    ) -> std::result::Result<(), crate::DfsError> {
        if !matches!(
            completed,
            CompletedRemoteNamespacePhase::ReadAnswered { .. }
        ) {
            // A phase that changed another shard ends what the reads before
            // it observed: the re-run reads that shard again.
            self.answered_remote_reads.clear();
        }
        let completed = match completed {
            CompletedRemoteNamespacePhase::SourcePrepared {
                request_id,
                prepared,
            } => {
                self.prepared_remote_sources.insert(request_id, prepared);
                return Ok(());
            }
            CompletedRemoteNamespacePhase::TargetPublished(completed) => completed,
            CompletedRemoteNamespacePhase::ReadAnswered { key, answer } => {
                // The re-run's read takes this answer in place of a send.
                self.answered_remote_reads.insert(key, answer);
                return Ok(());
            }
            // The re-run reads its job again.
            CompletedRemoteNamespacePhase::JobStepClaimReleased => return Ok(()),
            CompletedRemoteNamespacePhase::RepairProgress {
                object_id,
                version,
                deadline,
            } => {
                // The re-run file sync keeps the original wait's deadline.
                self.file_sync_wait_deadline = Some((object_id, version, deadline));
                return Ok(());
            }
        };
        let request = completed.command.request_id.clone();
        self.pending_routed_namespace_ops
            .insert(request.clone(), completed.pending);
        self.handle_namespace_response(completed.target, completed.response)?;
        let pending = self
            .pending_routed_namespace_ops
            .remove(&request)
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "remote phase lost its exact pending response fence".into(),
            })?;
        let outcome = pending.result.ok_or_else(|| crate::DfsError::Metadata {
            message: "remote phase response did not settle its exact request".into(),
        })?;
        self.completed_remote_namespace_phases
            .insert(request, outcome);
        Ok(())
    }
}

#[cfg(test)]
type ClaimObserver = Box<dyn FnOnce(&DfsSourceAdoptionActor, &DfsSourceAdoptionOffer) + Send>;
#[cfg(test)]
static CLAIM_OBSERVERS: Mutex<BTreeMap<PathBuf, ClaimObserver>> = Mutex::new(BTreeMap::new());
/// Run `observer` once, at the next claim the target actor at `root` gets
/// from a source, before the target prepares its effect.
#[cfg(test)]
pub(crate) fn observe_next_claim(
    root: &std::path::Path,
    observer: impl FnOnce(&DfsSourceAdoptionActor, &DfsSourceAdoptionOffer) + Send + 'static,
) {
    CLAIM_OBSERVERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(root.to_owned(), Box::new(observer));
}

#[cfg(test)]
mod guarded_kernel_terminal_tests {
    use super::*;
    use crate::{DfsNamespaceOperation, DfsNetworkMessage, DfsWriteOptions};

    #[test]
    fn undispatched_restore_cleanup_rejects_changed_or_claimed_native_custody() {
        let node = NodeId::generate();
        let mut native = crate::test_runtime::DfsTestRuntime::new("native-undispatched-claimed-custody", node.clone()).unwrap();
        native.start_nodes().unwrap();
        let root = native.data_dir(&node).root().to_owned();
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&observed);
        CLAIM_OBSERVERS.lock().unwrap().insert(root.clone(), Box::new(move |actor, offer| {
            let (physical, scope, object_id) = {
                let session = actor.source_session(offer, false).unwrap();
                assert!(session.claim.lock().unwrap().is_some(), "actual production target owns the claim");
                (session.capture.scope().root().to_owned(), session.capture.scope().clone(), session.capture.family().source_object.object_id.clone())
            };
            let mut changed = offer.clone();
            changed.transfer_id.push_str("-changed");
            assert!(actor.cancel_undispatched_local_offer(&changed).is_err());
            let mut foreign = offer.clone();
            foreign.source_node_id = NodeId::generate();
            assert!(!actor.cancel_undispatched_local_offer(&foreign).unwrap());
            assert!(actor.cancel_undispatched_local_offer(offer).is_err(), "claimed actual effect must retain custody");
            assert!(actor.source_session(offer, false).unwrap().claim.lock().unwrap().is_some());
            assert!(crate::metadata_lock::try_metadata_inspection_lock(physical.clone(), crate::metadata_cursor::journal_write_lock_path(&physical)).unwrap().is_some(), "the source holds no shard writer across the adopt step");
            assert!(crate::publication_guard::source_rows_fenced(&scope, &object_id), "the claimed source family stays fenced");
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }));
        let mut client = native.client(&node).unwrap();
        client.posix_write_file("/services/claimed-restore-source", b"claimed bytes", DfsWriteOptions::replicated_eventual(vec![node.clone()])).unwrap();
        client.posix_rename("/services/claimed-restore-source", "/users/claimed-restore-target").unwrap();
        assert_eq!(observed.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(client.posix_read_file_distributed("/users/claimed-restore-target").unwrap(), b"claimed bytes");
        CLAIM_OBSERVERS.lock().unwrap().remove(&root);
        drop(client);
        native.close().unwrap();
    }

    #[test]
    fn actual_native_terminal_cut_survives_a_busy_writer_without_authoring_or_missing_proof() {
        let node = NodeId::generate();
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-source-kernel-terminal-busy-writer",
            node.clone(),
        )
        .unwrap();
        runtime.start_nodes().unwrap();
        let from = "/users/kernel-terminal-source";
        let to = "/services/kernel-terminal-target";
        let mut requester = runtime.client(&node).unwrap();
        let original = requester
            .posix_write_file(
                from,
                b"actual source terminal bytes",
                DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let pause = runtime.pause_remote_namespace_phase(&node).unwrap();
        let (captured, moved) = std::thread::scope(|scope| {
            let work = scope.spawn(move || requester.posix_rename(from, to));
            let captured = pause.wait_captured(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
            // Always release and join the actual native producer before assertions.
            pause.release();
            (captured, work.join().unwrap())
        });
        let captured = captured.expect("actual native adoption offer");
        let DfsNetworkMessage::NamespaceRequest(command) = captured.routed_message.message else {
            panic!("actual native namespace request");
        };
        let DfsNamespaceOperation::AdoptObjectAtPath {
            source_offer: offer,
            ..
        } = command.operation
        else {
            panic!("actual native target adoption");
        };
        assert_eq!(moved.unwrap().object_id, original.object_id);
        let actor = actor_for_root(&runtime.data_dir(&node).root()).unwrap();
        let terminal = actor
            .target_claims
            .lock()
            .unwrap()
            .get(&offer.session_id)
            .unwrap()
            .terminal
            .clone()
            .expect("only the guarded actual native kernel records this cut");
        assert_eq!(terminal.offer, offer);
        terminal.commit.validate(&terminal.digest).unwrap();
        let shard = terminal.digest.shard_id.clone();
        let actual_prefix = || {
            runtime
                .metadata_primary_service(to)
                .unwrap()
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&shard)
                .unwrap()
                .unwrap()
        };
        let before = actual_prefix();
        let query = DfsSourceAdoptionRequest::InspectOutcome {
            offer: offer.clone(),
            target_scope: DfsPreparedOperationScope {
                file_system_id: terminal.digest.file_system_id.clone(),
                shard_id: terminal.digest.shard_id.clone(),
                owner_epoch: terminal.digest.owner_epoch,
                operation_id: terminal.digest.operation_id.clone(),
            },
            payload_digest: terminal.digest.payload_digest.clone(),
            expected_digest: Some(terminal.digest.clone()),
        };
        let root = terminal.scope.root().to_owned();
        let writer = crate::metadata_lock::metadata_write_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .unwrap();
        let result = actor.call(&node, &query).unwrap();
        assert!(matches!(result.outcome,
            Some(DfsSourceAdoptionOutcome::Committed { digest, commit })
                if digest == terminal.digest && commit == terminal.commit));
        let mut changed = query.clone();
        let DfsSourceAdoptionRequest::InspectOutcome {
            expected_digest, ..
        } = &mut changed
        else {
            unreachable!();
        };
        expected_digest.as_mut().unwrap().prepare_sequence += 1;
        let rejected = actor.call(&node, &changed).unwrap_err();
        assert!(
            rejected
                .to_string()
                .contains("native target outcome changes its guarded kernel terminal identity")
        );
        let retained = actor
            .target_claims
            .lock()
            .unwrap()
            .get_mut(&offer.session_id)
            .unwrap()
            .terminal
            .take()
            .unwrap();
        let missing = actor.call(&node, &query).unwrap();
        assert!(
            matches!(missing.outcome, Some(DfsSourceAdoptionOutcome::Unavailable)),
            "a busy actual writer without its kernel cut remains unavailable"
        );
        actor
            .target_claims
            .lock()
            .unwrap()
            .get_mut(&offer.session_id)
            .unwrap()
            .terminal = Some(retained);
        drop(writer);
        assert_eq!(
            actual_prefix(),
            before,
            "inspection and rejection publish no frames"
        );
        assert_eq!(
            runtime
                .client(&node)
                .unwrap()
                .posix_read_file_distributed(to)
                .unwrap(),
            b"actual source terminal bytes"
        );
        drop(actor);
        runtime.close().unwrap();
    }
}

#[cfg(test)]
mod requester_departure_tests {
    use super::*;

    /// A carrier whose call is never answered.
    struct SilentCarrier;

    impl sinomo_module::ComponentObjectRpcTransport for SilentCarrier {
        fn call(
            &self,
            _target: &NodeId,
            _call: sinomo_core::network::ObjectRpcCall,
            _options: sinomo_core::network::ClusterObjectRpcOptions,
            _observe: bool,
        ) -> std::result::Result<
            sinomo_core::network::ObjectRpcResult,
            sinomo_core::network::NetworkError,
        > {
            Err(sinomo_core::network::NetworkError::delivery_failed(
                "silent carrier",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                "the blocking call is not the one a leaving requester drives",
            ))
        }

        fn call_async<'a>(
            &'a self,
            _target: &'a NodeId,
            _call: sinomo_core::network::ObjectRpcCall,
            _options: sinomo_core::network::ClusterObjectRpcOptions,
            _observe: bool,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = std::result::Result<
                            sinomo_core::network::ObjectRpcResult,
                            sinomo_core::network::NetworkError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(std::future::pending())
        }
    }

    /// A read made with its request's lanes released ends when its requester
    /// leaves, not at its budget or the network's verdict.
    #[test]
    fn a_read_with_lanes_released_ends_when_its_requester_leaves() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let requester = crate::DfsCancellationFlag::new();
        let leaving = requester.clone();
        let handle = runtime.handle().clone();
        let read = std::thread::spawn(move || {
            let call = sinomo_core::network::ObjectRpcCall {
                request: sinomo_core::network::ObjectRpcRequest::from_encoded_body(
                    crate::DFS_DATA_NAMESPACE_ROUTE,
                    "dfs",
                    Vec::new(),
                ),
                body: sinomo_core::network::ObjectRpcBodySource::empty(),
            };
            call_until_requester_leaves(
                &SilentCarrier,
                &handle,
                &NodeId::generate(),
                call,
                sinomo_core::network::ClusterObjectRpcOptions::default(),
                Some(requester),
            )
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!read.is_finished(), "the read waits for its answer");
        let left = std::time::Instant::now();
        leaving.cancel();
        let ended = read.join().unwrap();
        assert!(left.elapsed() < std::time::Duration::from_secs(5));
        assert!(
            matches!(
                &ended,
                Err(error) if error.delivery_state()
                    == Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
            ),
            "{:?}",
            ended.as_ref().err()
        );
    }
}
