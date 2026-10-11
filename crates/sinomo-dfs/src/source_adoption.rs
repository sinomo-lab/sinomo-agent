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
pub(crate) use preparation::{
    PreparedSourceAdoption, SourcePreparationPlan, SourcePreparationToken,
};
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
}
struct SourceSession {
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
    offer: DfsSourceAdoptionOffer,
    family: DfsCrossShardSourceFamily,
    scope: DfsPreparedOperationScope,
    hash: String,
    digest: DfsPreparedOperationDigest,
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
        let generation = self.authority.projection_generation();
        let cfgs = self
            .authority
            .cached_metadata_shards(&offer.source_config.file_system_id, generation)
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
        self.verify_original_job(&offer, &capture.family().source_object.path)?;
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
        Ok(())
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
            // Join the existing native projection owner before this finite
            // outcome pass reads routing. A remount can retain real captures
            // while its current authoritative projection is still completing.
            let generation = actor.authority.generation();
            let _ = actor.runtime.block_on(
                actor
                    .authority
                    .ensure_complete_prefix_for_generation_async(generation),
            );
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
            let generation = self.authority.projection_generation();
            let cfgs = self
                .authority
                .cached_metadata_shards(&scope.file_system_id, generation)
                .map_err(error)?
                .ok_or_else(|| error("native outcome routing unavailable"))?;
            let target = cfgs
                .iter()
                .find(|cfg| cfg.shard_id == scope.shard_id.0)
                .ok_or_else(|| error("native target outcome scope missing"))?;
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
                Err(_) => continue,
            };
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
                    if actual.file_system_id != scope.file_system_id
                        || actual.shard_id != scope.shard_id
                        || actual.owner_epoch != scope.owner_epoch
                        || actual.operation_id != scope.operation_id
                        || actual.payload_digest != hash
                        || digest.as_ref().is_some_and(|old| old != &actual)
                    {
                        return Err(error("native terminal outcome changed exact digest"));
                    }
                    Some(actual)
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
            self.release_original_job(offer, None)?;
            sessions.remove(&offer.session_id);
            RETAINED
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| error("source custody registry poisoned"))?
                .remove(&(self.data_root.clone(), offer.session_id.clone()));
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
        let options = if let Some(deadline) = deadline {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| error("native source phase deadline elapsed before dispatch"))?;
            sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(remaining)
        } else {
            sinomo_core::network::ClusterObjectRpcOptions::default()
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
            .cached_metadata_shards(
                &offer.target_config.file_system_id,
                self.authority.projection_generation(),
            )
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
        let projection = self.authority.projection_generation();
        let cfgs = self
            .authority
            .cached_metadata_shards(&scope.file_system_id, projection)
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
        let outcome = if state.witness.as_ref().and_then(Weak::upgrade).is_some() {
            DfsSourceAdoptionOutcome::Active
        } else {
            let root = crate::default_dfs_metadata_shards_root(
                &sinomo_core::local::SinomoDataDir::new(&self.data_root),
            )
            .join(crate::metadata_keys::key_segment(&scope.shard_id.0));
            let inspect = || -> Result<DfsSourceAdoptionOutcome> {
                let backend = crate::LocalSobjMetadataBackend::open_for_inspection(
                    scope.shard_id.clone(),
                    root.clone(),
                )?;
                backend.source_adoption_outcome(scope, hash, expected)
            };
            // The original coordinator may still retain this very writer.
            // Borrow its opaque native custody; acquiring a second FD would
            // turn an exact terminal lookup into a permanent lock cycle.
            let borrowed = if offer.job_offer.coordinator_node_id == self.node
                && offer.job_offer.job_config.shard_id == scope.shard_id.0
            {
                self.original_job_session(&offer.job_offer, false)
                    .ok()
                    .filter(|job| {
                        job.capture.scope().root() == root.as_path()
                            && job.capture.scope().file_system_id() == &scope.file_system_id
                    })
                    .map(|job| job.capture.writer_custody())
            } else {
                None
            };
            let outcome = if let Some(custody) = borrowed {
                let _actual_writer = custody.writer();
                let _actual_fence = custody.fence();
                inspect()?
            } else {
                match crate::metadata_lock::try_metadata_inspection_lock(
                    root.clone(),
                    crate::metadata_cursor::journal_write_lock_path(&root),
                )? {
                    None => DfsSourceAdoptionOutcome::Unavailable,
                    Some(_actual_writer_cut) => inspect()?,
                }
            };
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
        deadline: std::time::Instant,
    },
    PublishTarget(RemoteTargetNamespacePhase),
}
#[derive(Debug)]
pub(crate) struct RemoteTargetNamespacePhase {
    pub(crate) actor: Arc<DfsSourceAdoptionActor>,
    pub(crate) command: crate::DfsNamespaceCommand,
    pub(crate) pending: crate::service_state::PendingRoutedNamespaceOp,
    pub(crate) deadline: std::time::Instant,
}
#[derive(Debug)]
pub(crate) enum CompletedRemoteNamespacePhase {
    SourcePrepared {
        request_id: String,
        prepared: PreparedSourceAdoption,
    },
    TargetPublished(CompletedRemoteTargetNamespacePhase),
}
#[derive(Debug)]
pub(crate) struct CompletedRemoteTargetNamespacePhase {
    pub(crate) command: crate::DfsNamespaceCommand,
    pub(crate) pending: crate::service_state::PendingRoutedNamespaceOp,
    pub(crate) response: crate::DfsNamespaceResponse,
    pub(crate) target: NodeId,
}
impl RemoteNamespacePhase {
    pub(crate) fn cancel_before_dispatch(&self) -> Result<()> {
        match self {
            Self::PrepareSource { .. } => Ok(()),
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
                if deadline <= std::time::Instant::now() {
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
        }
    }
}
impl RemoteTargetNamespacePhase {
    fn source_offer(&self) -> Result<Option<&DfsSourceAdoptionOffer>> {
        match &self.command.operation {
            crate::DfsNamespaceOperation::AdoptObjectAtPath { source_offer, .. } => {
                Ok(Some(source_offer))
            }
            crate::DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. } => Ok(None),
            _ => Err(error(
                "owned target phase contains another namespace operation",
            )),
        }
    }
    fn dispatch(self) -> std::result::Result<CompletedRemoteTargetNamespacePhase, crate::DfsError> {
        let actor = self.actor.clone();
        let outcome = self.dispatch_inner();
        if outcome.is_err() {
            actor.wake_reconciliation();
        }
        outcome
    }
    fn dispatch_inner(
        self,
    ) -> std::result::Result<CompletedRemoteTargetNamespacePhase, crate::DfsError> {
        let offer = self.source_offer()?;
        let Some(remaining) = self
            .deadline
            .checked_duration_since(std::time::Instant::now())
            .filter(|value| !value.is_zero())
        else {
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
        #[cfg(any(test, feature = "test-support"))]
        crate::source_adoption_test::capture_and_pause(
            &self.actor.data_root,
            &envelope,
            &self.actor.runtime,
        );
        let mut result = self
            .actor
            .transport
            .call(
                &self.pending.primary_node_id,
                call,
                sinomo_core::network::ClusterObjectRpcOptions::default()
                    .with_overall_timeout(remaining),
                true,
            )
            .map_err(crate::DfsError::from_network)?;
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
        let completed = match completed {
            CompletedRemoteNamespacePhase::SourcePrepared {
                request_id,
                prepared,
            } => {
                self.prepared_remote_sources.insert(request_id, prepared);
                return Ok(());
            }
            CompletedRemoteNamespacePhase::TargetPublished(completed) => completed,
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
