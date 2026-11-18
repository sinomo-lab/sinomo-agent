//! Sole component-owned migration orchestration. Codec modules retain their
//! own checkpoint formats; caller admission does not execute their conversion.
use super::*;

mod inventory;
pub(super) mod shard_conversion;
pub(super) mod conversion_receiver;
mod retained_conversion;
mod superseded_copy;
mod scheduler;
mod source_job_wakes;
mod source_job_recheck;
mod discovery;
mod content_exposure;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod content_tests;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod scheduler_tests;
#[cfg(test)]
mod shard_conversion_tests;
#[cfg(test)]
mod shard_conversion_native_tests;
#[cfg(test)]
mod shard_conversion_legacy_tests;
use scheduler::OwnerWork;

pub(super) struct MigrationOwner {
    pub(super) work: OwnerWork,
    pub(super) authority: Arc<NativeImportAuthority>,
    pub(super) metadata: Arc<MetadataConverter>,
    pub(super) content: Arc<crate::content::ContentConverter>,
    pub(super) content_v3: Mutex<crate::content::OriginalV3Converter>,
    pub(super) local_state: crate::local_state::LocalStateConverter,
    pub(super) _owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    pub(super) metadata_root: PathBuf,
    // A representation-readiness fact for this actual factory/owner lifetime.
    // Publication still consumes current native configuration/guard evidence.
    pub(super) configuration_ready: std::sync::atomic::AtomicBool,
    pub(super) routing_cache_lock: Mutex<()>,
    pub(super) lifecycle: tokio::sync::watch::Receiver<bool>,
    /// Shards a test converts by hand: their own `Metadata` units wait while
    /// held, so only the test's driver steps their conversion.
    #[cfg(test)]
    pub(super) held_conversions: Mutex<Option<Arc<Mutex<std::collections::BTreeSet<String>>>>>,
}
impl MigrationOwner {
    #[cfg(test)]
    pub(in crate::authority) fn diagnostic_scheduler(&self) -> String {
        format!("{:?}", self.work.scheduler.lock().unwrap())
    }
    fn current_content_initialization(&self, root: &Path) -> Result<bool> {
        if !sinomo_dfs::LocalContentStore::has_current_initialization(root).map_err(error)? {
            return Ok(false);
        }
        let mut original_job = self.content.existing_job_reference(root)?.is_some();
        for scope in [DfsStoragePreparationScope::Content, DfsStoragePreparationScope::ContentInventory] {
            let key = inventory::UnitKey::from_scope(&self.authority.data_root, root, &scope)?;
            original_job |= self.work.inventory.read(&key)?.is_some_and(|reference| reference.job.is_some());
        }
        if original_job || content_exposure::retained_publication(&self.authority.data_root)?.is_some()
            || content_exposure::has_retained_sources(&self.authority.data_root)? {
            return Err(error("current content initialization conflicts with an original conversion owner"));
        }
        Ok(true)
    }
}

pub(super) fn new_owner_work(root: &Path) -> Result<OwnerWork> { OwnerWork::new(root) }

impl MigrationOwner {
    /// Worker-only adapter while the source-specific transforms retain their
    /// established checkpoint formats. Never called by a foreground callback.
    fn advance_transform(&self, root: &Path, scope: &DfsStoragePreparationScope, budget: usize) -> Result<sinomo_dfs::DfsStoragePreparation> {
        let runtime = self;
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
            let prepared = if routing_cache::current(root)? {
                sinomo_dfs::DfsStoragePreparation::Ready
            } else { runtime
                .authority
                .prepare_routing_topology_cache(root, budget)? };
            if prepared == sinomo_dfs::DfsStoragePreparation::Ready {
                runtime.configuration_ready.store(true, std::sync::atomic::Ordering::Release);
            }
            return Ok(prepared);
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
        Err(error("scoped metadata and content work must use their owning unit continuation"))
    }
}

/// Bounded current-format DATA checks; no conversion or control-plane write.
/// Core still validates activation/native authority after this representation gate.
pub(super) fn inspect_current_unit(root: &Path, scope: &DfsStoragePreparationScope) -> Result<sinomo_dfs::DfsStoragePreparation> {
    use sinomo_dfs::{DfsMetadataGenerationState, DfsStoragePreparation as State};
    if root.file_name().is_some_and(|name| name == "content")
        && root.parent().and_then(|path| path.file_name()).is_some_and(|name| name == "dfs") {
        let data_root = root.parent().and_then(Path::parent).ok_or_else(|| error("content role has no data owner"))?;
        if content_exposure::frontend_target(data_root)? == Some(false) {
            return Ok(State::Pending { processed_records: 0 });
        }
    }
    let ready = match scope {
        DfsStoragePreparationScope::Metadata(shard) => {
            sinomo_dfs::inspect_metadata_storage_generation(root).is_ok_and(|header|
                header.identity.shard_id == *shard && matches!(header.state, DfsMetadataGenerationState::Active { .. }))
        }
        DfsStoragePreparationScope::MetadataGeneration { identity } => {
            sinomo_dfs::inspect_metadata_storage_generation(root).is_ok_and(|header|
                header.identity == *identity && matches!(header.state, DfsMetadataGenerationState::Active { .. }))
        }
        DfsStoragePreparationScope::MetadataRouting => routing_cache::current(root)?,
        DfsStoragePreparationScope::Content => crate::content::current(root)?,
        DfsStoragePreparationScope::ContentVersion { .. }
        | DfsStoragePreparationScope::ContentObject { .. }
        | DfsStoragePreparationScope::ContentBlob { .. }
        | DfsStoragePreparationScope::ContentInventory => {
            if !crate::content::current(root)? { false }
            else { sinomo_dfs::LocalContentStore::open_for_inspection(root).inventory_import_pending().map_err(error)?.is_none() }
        }
        DfsStoragePreparationScope::LocalState => crate::local_state::current(root)?,
    };
    Ok(if ready { State::Ready } else { State::Pending { processed_records: 0 } })
}

impl MigrationOwner {
    pub(super) fn binding(self: &Arc<Self>) -> sinomo_dfs::DfsStoragePreparationProviderBinding {
        let owner = Arc::clone(self);
        sinomo_dfs::DfsStoragePreparationProviderBinding {
            provider: Arc::new(move |root, scope, _caller_budget| owner.demand(root, scope)),
            readiness: self.work.changed.subscribe(),
        }
    }
    fn demand(&self, root: &Path, scope: &DfsStoragePreparationScope) -> Result<sinomo_dfs::DfsStoragePreparation> {
        use inventory::ObservedState;
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("migration owner lifecycle is closed"));
        }
        let key = inventory::UnitKey::from_scope(&self.authority.data_root, root, scope)?;
        if matches!(scope, DfsStoragePreparationScope::Content) && self.current_content_initialization(root)? {
            // Only the existing core initializer may finish this exact intent.
            // Ready admits that initializer; it does not initialize or convert.
            return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
        }
        if root == self.authority.data_root.join("dfs/content") {
            if let Some(false) = content_exposure::frontend_target(&self.authority.data_root)? {
                self.work.scheduler.lock().map_err(error)?.enqueue(key, true)?;
                self.work.wake.notify_one();
                return Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: 0 });
            }
        }
        if let DfsStoragePreparationScope::Metadata(shard) = scope {
            if superseded_copy::awaits_snapshot(root, shard)? {
                // The shard converted without this member: its old copy is
                // kept aside and the authenticated snapshot repair installs
                // the activated image.
                return Err(MetadataBackendError::Rejected {
                    code: sinomo_dfs::DfsMetadataRejectionCode::ReplicaUnreadable,
                    message: "metadata replica missed its shard's conversion install; authenticated snapshot repair is required".into(),
                });
            }
            if self.work.inventory.read(&key)?.is_none_or(|reference| reference.job.is_none())
                && crate::metadata::original_unit_absent(root, shard)? {
                // Preserve the core's existing authenticated missing-replica
                // bootstrap. Unreadable is not a serving/initialization grant.
                return Err(MetadataBackendError::Rejected {
                    code: sinomo_dfs::DfsMetadataRejectionCode::ReplicaUnreadable,
                    message: "metadata replica has no original local unit; authenticated snapshot bootstrap is required".into(),
                });
            }
        }
        if matches!(scope, DfsStoragePreparationScope::Content) && content_exposure::live_root_absent(root)? {
            // Virgin current initialization belongs to core. An original job or
            // retained source makes absence ambiguous and must keep its owner.
            let inventory_key = inventory::UnitKey::from_scope(&self.authority.data_root, root, &DfsStoragePreparationScope::ContentInventory)?;
            let has_job = [&key, &inventory_key].into_iter().map(|unit| self.work.inventory.read(unit))
                .collect::<Result<Vec<_>>>()?.into_iter().flatten().any(|reference| reference.job.is_some());
            let has_retained = content_exposure::has_retained_sources(&self.authority.data_root)?;
            if !has_job && !has_retained && content_exposure::retained_publication(&self.authority.data_root)?.is_none() {
                return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
            }
        }
        if root == self.authority.data_root.join("dfs/content")
            && content_exposure::retained_publication(&self.authority.data_root)?.is_none() {
            let actual = self.content.existing_job_reference(root)?.map(|job| inventory::JobReference {
                checkpoint: Path::new("dfs/content").join(job.checkpoint), identity: job.identity,
            });
            let original_complete = actual.is_some() && self.content.existing_job_complete(root)?;
            for role in [DfsStoragePreparationScope::Content, DfsStoragePreparationScope::ContentInventory] {
                let unit = inventory::UnitKey::from_scope(&self.authority.data_root, root, &role)?;
                if let Some(expected) = self.work.inventory.read(&unit)?.and_then(|reference| reference.job) {
                    if actual.as_ref() != Some(&expected) { return Err(error("current content root lost or replaced its original historical job")); }
                }
                if actual.is_some() && self.work.scheduler.lock().map_err(error)?.state(&unit)? != Some(&inventory::ObservedState::VerifiedCurrent) {
                    self.work.scheduler.lock().map_err(error)?.enqueue(unit, false)?;
                    self.work.wake.notify_one();
                }
            }
            if actual.is_some() && !original_complete && matches!(scope, DfsStoragePreparationScope::ContentInventory) {
                return Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: 0 });
            }
        }
        if matches!(scope, DfsStoragePreparationScope::Metadata(_))
            && let Some(ObservedState::Blocked { evidence }) = self.work.scheduler.lock().map_err(error)?.state(&key)?
        {
            // A shard's stopped conversion is refused whatever its copy's
            // header says: this refusal is how the stop reaches DFS.
            return Err(error(format!("migration unit is blocked: {evidence}")));
        }
        if !self.routing_configuration_pending(scope)
            && matches!(inspect_current_unit(root, scope)?, sinomo_dfs::DfsStoragePreparation::Ready) {
            return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
        }
        // Fresh native bootstrap belongs to core. Its original commit changes
        // the bounded header; no legacy converter is scheduled for that stage.
        if matches!(scope, DfsStoragePreparationScope::Metadata(_))
            && sinomo_dfs::inspect_metadata_storage_generation(root).is_ok_and(|header|
                matches!(header.identity.origin, sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesis)) {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: 0 });
        }
        if matches!(scope, DfsStoragePreparationScope::ContentVersion { .. } | DfsStoragePreparationScope::ContentObject { .. } | DfsStoragePreparationScope::ContentBlob { .. })
            && crate::content::current(root)? {
            let store = sinomo_dfs::LocalContentStore::open_for_inspection(root);
            if let Some(inventory) = store.inventory_import_pending().map_err(error)? {
                let incarnation = store.store_incarnation().map_err(error)?;
                if self.work.scheduler.lock().map_err(error)?.content_verified(&key, &inventory, &incarnation)? {
                    return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
                }
            }
        }
        let mut scheduler = self.work.scheduler.lock().map_err(error)?;
        if let Some(ObservedState::Blocked { evidence }) = scheduler.state(&key)? {
            return Err(error(format!("migration unit is blocked: {evidence}")));
        }
        scheduler.enqueue(key, true)?;
        drop(scheduler);
        self.work.wake.notify_one();
        Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: 0 })
    }
    fn routing_configuration_pending(&self, scope: &DfsStoragePreparationScope) -> bool {
        matches!(scope, DfsStoragePreparationScope::MetadataRouting)
            && !self.configuration_ready.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(super) fn attach(self: &Arc<Self>, scope: &mut sinomo_module::ComponentScope) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.work.attached.swap(true, Ordering::AcqRel) { return Ok(()); }
        let owner = Arc::clone(self);
        let task = self.authority.runtime.spawn(async move { owner.run().await; });
        scope.own_task("dfs-migration-owner", task);
        let weak = Arc::downgrade(self);
        let paths = sinomo_core::network::ClusterPaths::new(self.authority.component_context.data_dir());
        let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(&paths, move |observation| {
            if let Some(owner) = weak.upgrade() {
                let changed = owner.work.scheduler.lock().is_ok_and(|mut scheduler| scheduler.native_dependencies_changed(observation));
                if changed { owner.work.wake.notify_one(); }
            }
        });
        scope.defer(move || drop(subscription));
        // Units parked on an unreachable node wake on its reachability, not
        // on a timer.
        if let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) {
            // Conversions waiting on another shard's job wake on its events.
            self.listen_for_source_jobs(&state.local_node_id, scope);
            let owner = Arc::clone(self);
            let task = self.authority.runtime.spawn(owner.watch_peer_reachability(paths, state));
            scope.own_task("dfs-migration-peer-reachability", task);
        }
        let owner = Arc::clone(self);
        scope.on_task_drain("dfs-migration-owned-steps", move |deadline| {
            let owner = Arc::clone(&owner);
            async move {
                owner.work.cancelled.store(true, Ordering::Release);
                loop {
                    let changed = owner.work.drained.notified();
                    if owner.work.active.load(Ordering::Acquire) == 0 {
                        owner.metadata.release_checkpointed_sessions().map_err(|failure| failure.to_string())?;
                        owner.content.release_checkpointed_sessions().map_err(|failure| failure.to_string())?;
                        *owner.content_v3.lock().map_err(|failure| failure.to_string())? = Default::default();
                        return Ok(());
                    }
                    tokio::time::timeout_at(deadline, changed).await.map_err(|_| "migration source/effect owner has not drained".to_owned())?;
                }
            }
        });
        Ok(())
    }
    async fn run(self: Arc<Self>) {
        let mut shutdown = self.lifecycle.clone();
        loop {
            if *shutdown.borrow() || shutdown.has_changed().is_err() { break; }
            if self.work.discovery.lock().map(|discovery| discovery.has_work()).unwrap_or(false) {
                let owner = Arc::clone(&self);
                let held = ActiveStep::new(Arc::clone(&self));
                let discovered = tokio::task::spawn_blocking(move || {
                    let _held = held;
                    owner.discover_step()
                }).await;
                if let Ok(Err(failure)) = discovered {
                    tracing::error!(error=%failure, "migration managed discovery remains incomplete");
                }
            }
            let work = match self.work.scheduler.lock() {
                Ok(mut scheduler) => scheduler.next(),
                Err(_) => break,
            };
            let Some((id, reference)) = work else {
                let job_retry = self.work.scheduler.lock().ok().and_then(|scheduler| scheduler.next_retry());
                let discovery_retry = self.work.discovery.lock().ok().and_then(|discovery| discovery.next_retry());
                let retry = job_retry.into_iter().chain(discovery_retry).min();
                if let Some(at) = retry {
                    // Only already-owned pending jobs get this bounded retry;
                    // no observational query creates a maintenance sweep.
                    tokio::select! { _ = shutdown.changed() => {}, _ = self.work.wake.notified() => {}, _ = tokio::time::sleep_until(at.into()) => {} }
                } else {
                    tokio::select! { _ = shutdown.changed() => {}, _ = self.work.wake.notified() => {} }
                }
                continue;
            };
            let original_reference = reference.clone();
            let owner = Arc::clone(&self);
            let held = ActiveStep::new(Arc::clone(&self));
            // The component owns this blocking step even if the async waiter
            // is cancelled. on_task_drain joins its real lifetime before lease release.
            let result = tokio::task::spawn_blocking(move || {
                let _held = held;
                owner.advance_reference(reference)
            }).await;
            match result {
                Ok(Ok(reference)) => {
                    let again = matches!(reference.state, inventory::ObservedState::Runnable);
                    let key = reference.unit.clone();
                    let mut readiness_changed = false;
                    if let Ok(mut scheduler) = self.work.scheduler.lock() {
                        readiness_changed = scheduler.finish(&id, reference);
                        if again { let _ = scheduler.enqueue(key, false); }
                    }
                    if again || readiness_changed { self.work.notify(); }
                }
                Ok(Err(failure)) => {
                    self.finish_failed_step(&id, original_reference, failure);
                }
                Err(failure) => {
                    self.finish_failed_step(&id, original_reference, error(failure));
                }
            }
        }
    }
    /// Hold this node's peer-reachability subscription only while some unit
    /// watches a node, requeue the watchers of each node that becomes
    /// reachable, and requeue the verdict watchers of each node whose verdict
    /// changes. Nothing here polls: each wake is a verdict change or a new
    /// watch.
    async fn watch_peer_reachability(
        self: Arc<Self>,
        paths: sinomo_core::network::ClusterPaths,
        state: sinomo_core::ClusterBootstrapState,
    ) {
        use sinomo_core::network::PeerPathVerdict;
        let mut shutdown = self.lifecycle.clone();
        let mut subscription: Option<sinomo_core::network::PeerReachabilitySubscription> = None;
        loop {
            if *shutdown.borrow() || shutdown.has_changed().is_err() { break; }
            let Ok(watched) = self.work.scheduler.lock().map(|scheduler| scheduler.watched_nodes()) else { break };
            if watched.is_empty() {
                subscription = None;
            } else if subscription.is_none() {
                let created = sinomo_core::network::subscribe_peer_reachability(&paths, &state);
                // A node that became reachable before this subscription
                // existed is no change to it: deliver the current verdicts once.
                let reachable = created.current().peers.iter()
                    .filter(|(node, peer)| watched.contains(*node) && peer.verdict == PeerPathVerdict::Path)
                    .map(|(node, _)| node.clone()).collect();
                subscription = Some(created);
                self.nodes_became_reachable(&reachable);
                // Verdict watchers registered before the subscription read
                // the verdicts it starts from once.
                self.node_verdicts_changed(&watched);
            }
            let changes = async {
                match subscription.as_mut() {
                    Some(subscription) => subscription.changed().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = shutdown.changed() => {}
                _ = self.work.reachability.notified() => {}
                changes = changes => {
                    let changed = changes.iter().map(|change| change.peer_node_id.clone()).collect();
                    let reachable = changes.into_iter()
                        .filter(|change| change.current.as_ref().is_some_and(|peer| peer.verdict == PeerPathVerdict::Path)
                            && change.previous.as_ref().is_none_or(|peer| peer.verdict != PeerPathVerdict::Path))
                        .map(|change| change.peer_node_id).collect();
                    self.nodes_became_reachable(&reachable);
                    self.node_verdicts_changed(&changed);
                }
            }
        }
    }
    fn nodes_became_reachable(&self, nodes: &BTreeSet<sinomo_core::node::NodeId>) {
        if nodes.is_empty() { return; }
        let changed = self.work.scheduler.lock().is_ok_and(|mut scheduler| scheduler.nodes_became_reachable(nodes));
        if changed { self.work.wake.notify_one(); }
    }
    fn node_verdicts_changed(&self, nodes: &BTreeSet<sinomo_core::node::NodeId>) {
        if nodes.is_empty() { return; }
        let changed = self.work.scheduler.lock().is_ok_and(|mut scheduler| scheduler.node_verdicts_changed(nodes));
        if changed { self.work.wake.notify_one(); }
    }
    fn discover_step(&self) -> Result<()> {
        let mut discovery = self.work.discovery.lock().map_err(error)?;
        let result = (|| -> Result<()> {
            let references = discovery.step(16)?;
            for mut reference in references {
                match self.work.inventory.read(&reference.unit) {
                    Ok(Some(original)) => reference = original,
                    Ok(None) => {},
                    Err(failure) => {
                        reference.state = inventory::ObservedState::Blocked { evidence: failure.to_string().chars().take(2048).collect() };
                        let id = reference.unit.id()?;
                        self.work.scheduler.lock().map_err(error)?.finish(&id, reference);
                        continue;
                    }
                }
                self.work.inventory.record(&reference)?;
                self.work.scheduler.lock().map_err(error)?.recovered(reference)?;
            }
            discovery.commit()
        })();
        if let Err(failure) = result {
            let _ = discovery.restart_uncommitted();
            return Err(failure);
        }
        if discovery.has_work() { self.work.wake.notify_one(); }
        Ok(())
    }
    fn finish_failed_step(&self, id: &str, mut original: inventory::UnitReference, failure: MetadataBackendError) {
        // Do not rewrite a corrupt/uncertain durable reference merely to record
        // the error. Release only the process-local running slot, retaining the
        // original job identity and bytes for actual recovery.
        let retryable = matches!(&failure, MetadataBackendError::StoragePreparationPending { .. })
            || matches!(&failure, MetadataBackendError::Io { source, .. }
                if matches!(source.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted | std::io::ErrorKind::TimedOut));
        let evidence = failure.to_string().chars().take(2048).collect();
        original.state = if retryable { inventory::ObservedState::Waiting { evidence } }
            else { inventory::ObservedState::Blocked { evidence } };
        let mut readiness_changed = false;
        if let Ok(mut scheduler) = self.work.scheduler.lock() {
            readiness_changed = scheduler.finish(id, original);
            if retryable { scheduler.retry_failed(id); }
        }
        if retryable { tracing::debug!(unit=%id, error=%failure, "migration operation awaits retry at its original checkpoint"); }
        else { tracing::error!(unit=%id, error=%failure, "migration step blocked without discarding source ownership"); }
        if readiness_changed { self.work.notify(); }
    }
    fn advance_content_unit(&self, scope: &DfsStoragePreparationScope, reference: &mut inventory::UnitReference, byte_progress: &mut bool) -> Result<sinomo_dfs::DfsStoragePreparation> {
        use crate::content::{OriginalV3Budget, OriginalV3Selection, OriginalV3UnitResult};
        use sinomo_dfs::DfsStoragePreparation as State;
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
        let data_root = &self.authority.data_root;
        let live = data_root.join("dfs/content");
        let existing = content_exposure::retained_publication(data_root)?;
        if self.current_content_initialization(&live)? {
            return Ok(State::Ready);
        }
        if content_exposure::live_root_absent(&live)? && existing.is_none() {
            if reference.job.is_some() || content_exposure::has_retained_sources(data_root)? {
                return Err(error("missing live content root has retained source/job evidence; fresh initialization is forbidden"));
            }
            return Ok(State::Ready);
        }
        if existing.is_none() {
            match crate::content::source_format(&live)? {
                None | Some(1 | 2) => {
                    // Historical codecs remain subordinate background work.
                    // Their final catalog must be known before a missing key
                    // can be reported; no foreground callback runs this path.
                    if let Some(job) = self.content.enroll(&live)? {
                        let original = inventory::JobReference { checkpoint: Path::new("dfs/content").join(job.checkpoint), identity: job.identity };
                        if reference.job.as_ref().is_some_and(|retained| retained != &original) {
                            return Err(error("historical content source changed its original conversion job"));
                        }
                        reference.job = Some(original);
                        self.work.inventory.record(reference)?;
                    }
                    return self.content.prepare_bounded(&live, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET, 8 * 1024 * 1024, deadline);
                }
                Some(3) => {},
                Some(version) if version == sinomo_dfs::LocalContentStore::FORMAT_VERSION => {
                    if let Some(job) = self.content.existing_job_reference(&live)? {
                        let expected = inventory::JobReference { checkpoint: Path::new("dfs/content").join(job.checkpoint), identity: job.identity };
                        if reference.job.as_ref() != Some(&expected) { return Err(error("current content continuation changed its original historical job")); }
                        return self.content.prepare_bounded(&live, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET, 8 * 1024 * 1024, deadline);
                    }
                    return Err(error("pending current content root has no original publication job"));
                }
                Some(_) => return Err(error("content source generation is unsupported; source preserved")),
            }
        }
        let publication = content_exposure::prepare(data_root, |publication| {
            let successor = inventory::JobReference { checkpoint: content_exposure::checkpoint_relative(), identity: publication.inventory_id.clone() };
            if let Some(original) = reference.job.as_ref().filter(|original| *original != &successor) {
                let source = content_exposure::retained_source_root(data_root, publication)?;
                let proof = self.content.published_v3_successor(&source)?
                    .ok_or_else(|| error("source3 exposure has no exact original historical handoff"))?;
                if original.identity != proof.predecessor.identity
                    || original.checkpoint != Path::new("dfs/content").join(&proof.predecessor.checkpoint)
                    || !content_exposure::matches_source_identity(publication, &proof.source) {
                    return Err(error("source3 exposure substitutes the original historical content job"));
                }
                // Keep the original logical checkpoint reference. The existing
                // publication resolves its relocated physical source on restart.
            } else { reference.job = Some(successor); }
            self.work.inventory.record(reference)
        })?;
        if matches!(scope, DfsStoragePreparationScope::Content) { return Ok(State::Ready); }
        #[cfg(test)]
        if matches!(scope, DfsStoragePreparationScope::ContentInventory) {
            content_exposure::observe(data_root, "before-inventory-step")?;
        }
        let selection = match scope {
            DfsStoragePreparationScope::ContentVersion { object_id, version } => OriginalV3Selection::Version { object_id: object_id.clone(), version: version.clone() },
            DfsStoragePreparationScope::ContentObject { object_id } => OriginalV3Selection::Object(object_id.clone()),
            DfsStoragePreparationScope::ContentBlob { hash } => OriginalV3Selection::Blob(hash.clone()),
            _ => OriginalV3Selection::All,
        };
        let step = self.content_v3.lock().map_err(error)?.step(&data_root.join(&publication.retained), &live,
            &publication.inventory_id, &selection, &OriginalV3Budget { max_records: 256, max_bytes: 8 * 1024 * 1024,
                deadline, cancelled: &self.work.cancelled })?;
        // A durable copy/hash continuation can advance without completing a
        // manifest record. Keep it runnable without inventing a record count.
        *byte_progress = step.processed_bytes > 0;
        if step.coverage_complete {
            content_exposure::mark_covered(data_root, &publication)?;
            return Ok(State::Ready);
        }
        match step.unit_result {
            OriginalV3UnitResult::Covered | OriginalV3UnitResult::Absent => {
                self.work.scheduler.lock().map_err(error)?.mark_content_verified(&reference.unit,
                    publication.inventory_id, publication.target_incarnation)?;
                Ok(State::Ready)
            }
            OriginalV3UnitResult::Blocked(reason) => Err(error(reason)),
            OriginalV3UnitResult::Pending | OriginalV3UnitResult::PendingIndex => Ok(State::Pending { processed_records: step.processed_records }),
        }
    }
    fn advance_reference(&self, reference: inventory::UnitReference) -> Result<inventory::UnitReference> {
        use inventory::UnitRole;
        let phase = match &reference.unit.role {
            UnitRole::Metadata { .. } | UnitRole::MetadataGeneration { .. } => "metadata",
            UnitRole::MetadataRouting => "routing",
            UnitRole::ContentRoot => "content-root",
            UnitRole::ContentVersion { .. } | UnitRole::ContentBlob { .. } | UnitRole::ContentObject { .. } => "content-unit",
            UnitRole::ContentInventory => "content-inventory",
            UnitRole::LocalState => "local-state",
            UnitRole::RetainedArtifact => "retained",
        };
        let span = tracing::trace_span!(target: "sinomo.perf", "dfs.migration.step",
            stage = "dfs.migration.step", phase, outcome = tracing::field::Empty);
        let _entered = span.enter();
        let result = self.advance_reference_inner(reference);
        span.record("outcome", if result.is_ok() { "ok" } else { "error" });
        result
    }
    fn advance_reference_inner(&self, mut reference: inventory::UnitReference) -> Result<inventory::UnitReference> {
        use inventory::ObservedState;
        // Demand never overwrites an existing original job reference.
        if let Some(retained) = self.work.inventory.read(&reference.unit)? { reference = retained; }
        self.work.inventory.record(&reference)?;
        let root = self.authority.data_root.join(&reference.unit.relative_root);
        let Some(scope) = reference.unit.scope() else {
            if self.shard_conversion_original_observed(&reference)? {
                reference.state = ObservedState::RetainedOriginal;
                self.work.inventory.record(&reference)?;
                return Ok(reference);
            }
            if let Some(job) = content_exposure::completed_original_job(&self.authority.data_root, &reference.unit.relative_root)? {
                if reference.job.as_ref().is_some_and(|original| original != &job) { return Err(error("retained archive changed its original job reference")); }
                reference.job = Some(job);
                reference.state = ObservedState::RetainedOriginal;
                self.work.inventory.record(&reference)?;
                return Ok(reference);
            }
            reference.state = ObservedState::Waiting { evidence: "retained original job requires source codec reconciliation".into() };
            self.work.inventory.record(&reference)?;
            return Ok(reference);
        };
        let mut retained_legacy_content = false;
        if root == self.authority.data_root.join("dfs/content") && content_exposure::retained_publication(&self.authority.data_root)?.is_none() {
            if let Some(job) = self.content.existing_job_reference(&root)? {
                let expected = inventory::JobReference { checkpoint: Path::new("dfs/content").join(job.checkpoint), identity: job.identity };
                if reference.job.as_ref().is_some_and(|original| original != &expected) { return Err(error("historical content continuation replaced its original job")); }
                reference.job = Some(expected);
                self.work.inventory.record(&reference)?;
                retained_legacy_content = true;
            }
        }
        // A shard's storage record, not its copy's header, says whether the
        // copy is current: the shard's own unit reads the record.
        let current = if retained_legacy_content || self.routing_configuration_pending(&scope)
            || matches!(scope, DfsStoragePreparationScope::Metadata(_)) { Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: 0 }) }
            else { inspect_current_unit(&root, &scope) };
        let mut byte_progress = false;
        let result = current.and_then(|current| {
            if current == sinomo_dfs::DfsStoragePreparation::Ready { Ok(current) }
            else if matches!(scope, DfsStoragePreparationScope::Content | DfsStoragePreparationScope::ContentVersion { .. }
                | DfsStoragePreparationScope::ContentObject { .. } | DfsStoragePreparationScope::ContentBlob { .. } | DfsStoragePreparationScope::ContentInventory) {
                self.advance_content_unit(&scope, &mut reference, &mut byte_progress)
            } else {
                match &scope {
                    #[cfg(test)]
                    DfsStoragePreparationScope::Metadata(shard)
                        if self.held_conversions.lock().map_err(error)?.as_ref()
                            .is_some_and(|held| held.lock().unwrap().contains(&shard.0)) =>
                    {
                        Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: 0 })
                    }
                    DfsStoragePreparationScope::Metadata(shard) => self.advance_metadata_unit(&reference.unit, shard),
                    // Only a per-shard conversion stages a generation on a member.
                    DfsStoragePreparationScope::MetadataGeneration { identity } => match conversion_receiver::conversion_attempt_of(identity) {
                        Some(attempt_id) => self.advance_received_conversion(&reference.unit, identity, attempt_id),
                        None => Err(error("a staged generation names no per-shard conversion attempt")),
                    },
                    _ => self.advance_transform(&root, &scope, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET),
                }
            }
        });
        reference.state = match result {
            Ok(sinomo_dfs::DfsStoragePreparation::Ready) => ObservedState::VerifiedCurrent,
            Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records }) if processed_records > 0 || byte_progress => ObservedState::Runnable,
            Ok(sinomo_dfs::DfsStoragePreparation::Pending { .. }) => ObservedState::Waiting { evidence: "source or native dependency pending at original checkpoint".into() },
            Err(failure) => return Err(failure),
        };
        self.work.inventory.record(&reference)?;
        if matches!(reference.state, ObservedState::VerifiedCurrent)
            && matches!(scope, DfsStoragePreparationScope::Metadata(_) | DfsStoragePreparationScope::MetadataGeneration { .. }) {
            self.record_shard_conversion_original(&root)?;
        }
        Ok(reference)
    }
}
struct ActiveStep(Arc<MigrationOwner>);
impl ActiveStep {
    fn new(owner: Arc<MigrationOwner>) -> Self {
        owner.work.active.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self(owner)
    }
}
impl Drop for ActiveStep {
    fn drop(&mut self) {
        self.0.work.active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.0.work.drained.notify_waiters();
    }
}

pub(super) fn validate_managed_unit(data_root: &Path, root: &Path, scope: &DfsStoragePreparationScope) -> Result<()> {
    inventory::UnitKey::from_scope(data_root, root, scope).map(|_| ())
}
