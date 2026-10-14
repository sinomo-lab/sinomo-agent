use super::*;

const DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS: usize = 8;

#[cfg(test)]
#[derive(Debug)]
pub(super) enum DaemonDfsTestAuthoritativeExactSetOutcome {
    Error(KvError),
    Entries(Vec<Option<KvEntry>>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DaemonDfsRetainedMetadataSync {
    Current,
    ClientProjectionNewer,
}

#[derive(Clone, Debug)]
pub(crate) struct DaemonDfsControlPlaneKvClient {
    pub(super) cluster_paths: ClusterPaths,
    pub(super) runtime: tokio::runtime::Handle,
    /// Compact exact/prefix scopes established by daemon-owned ReadIndex
    /// requests. The derived materialized checkpoint is deliberately never
    /// admitted into this correctness-sensitive view.
    pub(super) projection: Arc<Mutex<DaemonDfsAuthoritativeProjection>>,
    /// Daemon-owned DFS clients borrow the process-wide, delta-maintained
    /// `/dfs/filesystems/` authority certificate. The observed generation is
    /// shared with cloned client handles, so a warm read pays one atomic load
    /// and never takes the retained projection's mutex. A retained prefix
    /// change or gap advances the shared projection generation and forces one
    /// bounded local reseed before this client can use DFS state again.
    pub(super) retained_metadata_authority: Option<Arc<DaemonDfsMetadataAuthorityGeneration>>,
    pub(super) write_configuration_projection_required: bool,
    pub(super) retained_metadata_projection_generation: Arc<AtomicU64>,
    /// Cached and short-lived services normally retain only a weak capability;
    /// their surrounding blocking task owns the strong guard. This prevents a
    /// static message cache from pinning an old daemon's OS lock after shutdown.
    pub(super) data_owner_lock: Option<Weak<DaemonDataOwnerLock>>,
    /// Sandbox services may outlive the handler which opened them, so those
    /// services explicitly retain the strong capability for their lifetime.
    #[cfg(test)]
    pub(super) full_snapshot_refresh_calls: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(super) authoritative_exact_set_calls: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(super) authoritative_prefix_calls: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(super) authoritative_exact_set_deadlines: Arc<Mutex<Vec<Instant>>>,
    #[cfg(test)]
    pub(super) authoritative_exact_set_outcomes:
        Arc<Mutex<VecDeque<DaemonDfsTestAuthoritativeExactSetOutcome>>>,
}

impl DaemonDfsControlPlaneKvClient {
    pub(crate) fn new_on_runtime(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let data_owner_lock = Self::discover_data_owner_lock(cluster_paths);
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: runtime.clone(),
            projection: Arc::new(Mutex::new(DaemonDfsAuthoritativeProjection::default())),
            retained_metadata_authority: None,
            write_configuration_projection_required: false,
            retained_metadata_projection_generation: Arc::new(AtomicU64::new(0)),
            data_owner_lock: data_owner_lock.as_ref().map(Arc::downgrade),
            #[cfg(test)]
            full_snapshot_refresh_calls: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            authoritative_exact_set_calls: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            authoritative_prefix_calls: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            authoritative_exact_set_deadlines: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            authoritative_exact_set_outcomes: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub(super) fn new_daemon_client_on_runtime(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let mut client = Self::new_on_runtime(cluster_paths, runtime);
        client.retained_metadata_authority =
            Self::discover_retained_metadata_authority(cluster_paths, runtime);
        client.write_configuration_projection_required = true;
        client
    }

    pub(super) fn new_seeded_on_runtime(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
        authoritative_prefix: Option<sinomo_dfs::DfsAuthoritativePrefixProjection>,
    ) -> Self {
        let client = Self::new_daemon_client_on_runtime(cluster_paths, runtime);
        let Some(authoritative_prefix) = authoritative_prefix else {
            return client;
        };
        if !authoritative_prefix.is_complete() {
            return client;
        }
        let (entries, revision, _) = authoritative_prefix.into_parts();
        let Ok(mut projection) = client.projection.lock() else {
            return client;
        };
        projection.revision = revision;
        projection
            .prefix_entries
            .insert(DAEMON_DFS_METADATA_AUTHORITY_PREFIX.to_string(), entries);
        if !projection.fits_resource_bounds() {
            projection.clear_scopes();
        }
        // The caller's seed may have raced a topology generation change after
        // it was captured. Force the first read to compare it with the shared
        // generation before admitting any cached value.
        client.invalidate_retained_metadata_authority_seed();
        drop(projection);
        client
    }

    pub(super) fn discover_retained_metadata_authority(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Arc<DaemonDfsMetadataAuthorityGeneration>> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths).ok()?;
        daemon_dfs_metadata_authority_generation_for_identity(
            cluster_paths,
            &state.cluster_id,
            &state.local_node_id,
            Some(runtime),
        )
        .ok()
    }

    pub(super) fn retained_metadata_authority_covers_key(&self, key: &str) -> bool {
        self.retained_metadata_authority.is_some()
            && key.starts_with(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
    }

    pub(super) fn retained_metadata_authority_covers_prefix(&self, prefix: &str) -> bool {
        self.retained_metadata_authority.is_some()
            && prefix.starts_with(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
    }

    pub(super) fn retained_metadata_authority_is_current(&self) -> bool {
        self.retained_metadata_authority
            .as_ref()
            .is_some_and(|authority| {
                self.retained_metadata_projection_generation
                    .load(Ordering::Acquire)
                    == authority.projection_generation()
            })
    }

    pub(super) fn invalidate_retained_metadata_authority_seed(&self) {
        self.retained_metadata_projection_generation
            .store(0, Ordering::Release);
    }

    /// Import the daemon's already-authorized DFS prefix into this client's
    /// private compact view. The hot path is an atomic generation comparison;
    /// only a cold client or a committed topology change clones the bounded
    /// retained prefix. A client can already hold a newer exact/prefix
    /// ReadIndex proof while the shared committed-delta observer is catching
    /// up. Preserve that newer proof and request one shared refresh; callers
    /// may use covered private scopes and issue one bounded direct read only
    /// for a scope the newer projection does not cover.
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.authority_sync",
        skip_all,
        fields(stage = "dfs.control_plane.authority_sync")
    )]
    pub(super) fn synchronize_retained_metadata_authority(
        &self,
    ) -> Result<DaemonDfsRetainedMetadataSync, KvError> {
        if let Some(synchronized) = self.try_synchronize_retained_configuration()? {
            return Ok(synchronized);
        }
        // Browser/restore workers are synchronous, but a missing certificate
        // must join the shared ReadIndex before this operation reads metadata.
        // No private projection lock is held across the runtime bridge.
        block_on_dfs_cluster_future(&self.runtime, async {
            Ok(self.synchronize_retained_metadata_authority_async().await)
        })?
    }

    async fn synchronize_retained_metadata_authority_async(
        &self,
    ) -> Result<DaemonDfsRetainedMetadataSync, KvError> {
        if let Some(synchronized) = self.try_synchronize_retained_configuration()? {
            return Ok(synchronized);
        }
        let authority = self.retained_metadata_authority.as_ref().ok_or_else(|| {
            self.superseded_authority_error("missing retained DFS metadata authority")
        })?;
        let refresh = async {
            for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
                let generation = authority.generation();
                // One in-flight shared refresh owns the proof. Its completion
                // can change topology or be superseded by a newer revision;
                // replan against the resulting generation, never the old one.
                let proof = authority
                    .ensure_complete_prefix_for_generation_async(
                        generation,
                        #[cfg(test)]
                        None,
                    )
                    .await;
                if proof.is_some() && self.write_configuration_projection_required {
                    // The filesystem prefix publishes before the shared
                    // content/provider bundle. Join that same owner within
                    // this synchronization's original timeout and replan cap.
                    authority
                        .ensure_write_configuration_ready_async()
                        .await
                        .map_err(|error| KvError::AuthorityUnavailable {
                            detail: error.to_string(),
                        })?;
                }
                if let Some(synchronized) = self.try_synchronize_retained_configuration()? {
                    return Ok(synchronized);
                }
            }
            Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "daemon DFS retained metadata authority generation {} is not ready after bounded refresh",
                    authority.generation(),
                ),
            })
        };
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, refresh)
            .await
            .map_err(|_| KvError::AuthorityUnavailable {
                detail: format!(
                    "daemon DFS retained metadata authority generation {} is not ready: refresh deadline elapsed",
                    authority.generation(),
                ),
            })?
    }

    /// sinomo:kv-configuration: A daemon client requiring the shared configuration may lend its compact prefix only after the same retained bundle is ready at the native floor.
    fn try_synchronize_retained_configuration(
        &self,
    ) -> Result<Option<DaemonDfsRetainedMetadataSync>, KvError> {
        let synchronized = self.try_synchronize_retained_metadata_authority()?;
        if synchronized.is_some() && self.write_configuration_projection_required {
            let authority = self.retained_metadata_authority.as_ref().ok_or_else(|| {
                self.superseded_authority_error("missing required write configuration owner")
            })?;
            if matches!(
                authority.write_configuration_readiness().map_err(|error| {
                    KvError::AuthorityUnavailable {
                        detail: error.to_string(),
                    }
                })?,
                crate::DfsWriteConfigurationReadiness::Pending
            ) {
                return Ok(None);
            }
        }
        Ok(synchronized)
    }

    fn try_synchronize_retained_metadata_authority(
        &self,
    ) -> Result<Option<DaemonDfsRetainedMetadataSync>, KvError> {
        let Some(authority) = self.retained_metadata_authority.as_ref() else {
            return Ok(Some(DaemonDfsRetainedMetadataSync::Current));
        };
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            let projection_generation = authority.projection_generation();
            if self
                .retained_metadata_projection_generation
                .load(Ordering::Acquire)
                == projection_generation
            {
                return Ok(Some(DaemonDfsRetainedMetadataSync::Current));
            }
            let authority_generation = authority.generation();
            #[cfg(any(test, feature = "test-support"))]
            let (authoritative_prefix, native_seed) = if let (Some(root), Some(owner)) =
                (self.cluster_paths.root.parent(), self.data_owner_lock())
            {
                crate::native_shard_trace::capture_retained_metadata_seed(root, &owner, || {
                    authority.load_complete_prefix_for_generation(authority_generation)
                })
            } else {
                (
                    authority.load_complete_prefix_for_generation(authority_generation),
                    None,
                )
            };
            #[cfg(not(any(test, feature = "test-support")))]
            let authoritative_prefix =
                authority.load_complete_prefix_for_generation(authority_generation);
            let Some(authoritative_prefix) = authoritative_prefix else {
                self.invalidate_retained_metadata_authority_seed();
                return Ok(None);
            };
            if authority.generation() != authority_generation
                || authority.projection_generation() != projection_generation
            {
                continue;
            }
            let (entries, revision, _) = authoritative_prefix.into_parts();
            let mut projection = self
                .projection
                .lock()
                .map_err(|_| self.projection_lock_error())?;
            if projection.revision > revision {
                let required_revision = projection.revision;
                drop(projection);
                authority.require_authoritative_revision(required_revision);
                self.invalidate_retained_metadata_authority_seed();
                return Ok(Some(DaemonDfsRetainedMetadataSync::ClientProjectionNewer));
            }
            projection.clear_scopes();
            projection.revision = revision;
            projection
                .prefix_entries
                .insert(DAEMON_DFS_METADATA_AUTHORITY_PREFIX.to_string(), entries);
            if !projection.fits_resource_bounds() {
                projection.clear_scopes();
                drop(projection);
                self.invalidate_retained_metadata_authority_seed();
                return Err(KvError::ResourceExhausted {
                    operation: "daemon DFS retained metadata authority projection".to_string(),
                    requested_bytes: DFS_AUTHORITY_PROJECTION_MAX_BYTES.saturating_add(1),
                });
            }
            #[cfg(any(test, feature = "test-support"))]
            {
                projection.native_retained_seed = native_seed.filter(|seed| {
                    seed.revision == revision
                        && seed.projection_generation == projection_generation
                        && projection
                            .prefix_entries
                            .get(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
                            == Some(&seed.entries)
                });
            }
            self.retained_metadata_projection_generation
                .store(projection_generation, Ordering::Release);
            drop(projection);
            if authority.generation() == authority_generation
                && authority.projection_generation() == projection_generation
            {
                #[cfg(any(test, feature = "test-support"))]
                self.record_native_retained_use(
                    crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedPrefixSeed {
                        prefix: DAEMON_DFS_METADATA_AUTHORITY_PREFIX,
                    },
                    None,
                );
                return Ok(Some(DaemonDfsRetainedMetadataSync::Current));
            }
            self.invalidate_retained_metadata_authority_seed();
        }
        Err(self.superseded_authority_error("shared retained DFS metadata prefix"))
    }

    pub(super) fn discover_data_owner_lock(
        cluster_paths: &ClusterPaths,
    ) -> Option<Arc<DaemonDataOwnerLock>> {
        cluster_paths
            .root
            .parent()
            .map(SinomoDataDir::new)
            .map(|data_dir| DaemonPaths::new(&data_dir))
            .and_then(|paths| daemon_dfs_data_owner_lock(&paths).ok().flatten())
    }

    #[cfg(any(test, feature = "test-support"))]
    fn record_native_retained_use(
        &self,
        scope: crate::native_shard_trace::NativeControlConsumptionScopeV1,
        expected: Option<(u64, KvRevision)>,
    ) {
        let (Some(root), Some(owner)) = (self.cluster_paths.root.parent(), self.data_owner_lock())
        else {
            return;
        };
        if !crate::native_shard_trace::has_root_interest(root, &owner) {
            return;
        }
        let Ok(projection) = self.projection.lock() else {
            return;
        };
        if expected.is_some_and(|(generation, revision)| {
            projection.generation != generation || projection.revision != revision
        }) {
            return;
        }
        let Some(seed) = projection.native_retained_seed.as_ref() else {
            return;
        };
        if seed.revision != projection.revision
            || self
                .retained_metadata_projection_generation
                .load(Ordering::Acquire)
                != seed.projection_generation
            || projection
                .prefix_entries
                .get(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
                != Some(&seed.entries)
        {
            return;
        }
        crate::native_shard_trace::record_retained_control_use(root, &owner, seed, scope);
    }

    pub(super) fn data_owner_lock(&self) -> Option<Arc<DaemonDataOwnerLock>> {
        self.data_owner_lock.as_ref().and_then(Weak::upgrade)
    }

    fn cluster_access(&self) -> Result<Arc<dyn sinomo_core::service::ClusterAccess>, KvError> {
        dfs_runtime_host_for_cluster_paths(&self.cluster_paths)
            .map(|host| Arc::clone(&host.cluster))
            .map_err(|error| KvError::AuthorityUnavailable {
                detail: error.to_string(),
            })
    }

    pub(super) fn projection_lock_error(&self) -> KvError {
        KvError::Io {
            path: self.cluster_paths.root.display().to_string(),
            detail: "DFS authoritative KV projection lock is poisoned".to_string(),
        }
    }

    pub(super) fn begin_authoritative_generation(&self) -> Result<(), KvError> {
        self.invalidate_retained_metadata_authority_seed();
        self.projection
            .lock()
            .map_err(|_| self.projection_lock_error())?
            .begin_generation();
        Ok(())
    }

    pub(super) fn begin_authoritative_generation_at_revision(
        &self,
        revision: KvRevision,
    ) -> Result<(), KvError> {
        self.projection
            .lock()
            .map_err(|_| self.projection_lock_error())?
            .begin_generation_at_revision(revision);
        Ok(())
    }

    pub(super) fn entry_read_plan(
        &self,
        key: &str,
    ) -> Result<(u64, KvRevision, Option<Option<KvEntry>>), KvError> {
        let projection = self
            .projection
            .lock()
            .map_err(|_| self.projection_lock_error())?;
        Ok((
            projection.generation,
            projection.revision,
            projection.cached_entry(key),
        ))
    }

    pub(super) fn entries_read_plan(
        &self,
        keys: &[String],
    ) -> Result<(u64, KvRevision, Option<Vec<Option<KvEntry>>>), KvError> {
        let projection = self
            .projection
            .lock()
            .map_err(|_| self.projection_lock_error())?;
        Ok((
            projection.generation,
            projection.revision,
            projection.cached_entries(keys),
        ))
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.prefix_cache",
        skip_all,
        fields(stage = "dfs.control_plane.prefix_cache")
    )]
    pub(super) fn prefix_read_plan(
        &self,
        prefix: &str,
    ) -> Result<(u64, KvRevision, Option<Vec<KvEntry>>), KvError> {
        let projection = self
            .projection
            .lock()
            .map_err(|_| self.projection_lock_error())?;
        Ok((
            projection.generation,
            projection.revision,
            projection.cached_prefix(prefix),
        ))
    }

    pub(super) fn merge_authoritative_entries(
        &self,
        generation: u64,
        keys: &[String],
        read: &sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>,
    ) -> Result<bool, KvError> {
        if keys.len() != read.value.len() {
            return Err(KvError::Serialization {
                detail: format!(
                    "daemon DFS exact-key authority returned {} values for {} keys",
                    read.value.len(),
                    keys.len(),
                ),
            });
        }
        for (key, entry) in keys.iter().zip(&read.value) {
            if entry.as_ref().is_some_and(|entry| entry.key != *key) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "daemon DFS exact-key authority returned an entry outside requested key {key:?}"
                    ),
                });
            }
        }
        let mut projection = self
            .projection
            .lock()
            .map_err(|_| self.projection_lock_error())?;
        let revision = read.authority.revision;
        if projection.generation != generation || revision < projection.revision {
            return Ok(false);
        }
        if revision > projection.revision {
            self.invalidate_retained_metadata_authority_seed();
            projection.clear_scopes();
            projection.revision = revision;
        }
        for (key, entry) in keys.iter().zip(&read.value) {
            projection.exact_entries.insert(key.clone(), entry.clone());
        }
        if !projection.fits_resource_bounds() {
            self.invalidate_retained_metadata_authority_seed();
            projection.clear_scopes();
            for (key, entry) in keys.iter().zip(&read.value) {
                projection.exact_entries.insert(key.clone(), entry.clone());
            }
        }
        if !projection.fits_resource_bounds() {
            self.invalidate_retained_metadata_authority_seed();
            projection.clear_scopes();
            return Err(KvError::ResourceExhausted {
                operation: "daemon DFS authoritative exact-key projection".to_string(),
                requested_bytes: keys.iter().zip(&read.value).fold(
                    0_usize,
                    |bytes, (key, entry)| {
                        bytes.saturating_add(key.len()).saturating_add(
                            entry.as_ref().map_or(0, |entry| {
                                entry.key.len().saturating_add(entry.value.len())
                            }),
                        )
                    },
                ),
            });
        }
        Ok(true)
    }

    pub(super) fn merge_authoritative_prefix(
        &self,
        generation: u64,
        prefix: &str,
        read: &sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
    ) -> Result<bool, KvError> {
        if let Some(entry) = read
            .value
            .iter()
            .find(|entry| !entry.key.starts_with(prefix))
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "daemon DFS prefix authority for {prefix:?} returned entry {:?}",
                    entry.key,
                ),
            });
        }
        let mut projection = self
            .projection
            .lock()
            .map_err(|_| self.projection_lock_error())?;
        let revision = read.authority.revision;
        if projection.generation != generation || revision < projection.revision {
            return Ok(false);
        }
        if revision > projection.revision {
            self.invalidate_retained_metadata_authority_seed();
            projection.clear_scopes();
            projection.revision = revision;
        }
        projection
            .prefix_entries
            .insert(prefix.to_string(), read.value.clone());
        if !projection.fits_resource_bounds() {
            self.invalidate_retained_metadata_authority_seed();
            projection.clear_scopes();
            projection
                .prefix_entries
                .insert(prefix.to_string(), read.value.clone());
        }
        if !projection.fits_resource_bounds() {
            self.invalidate_retained_metadata_authority_seed();
            projection.clear_scopes();
            return Err(KvError::ResourceExhausted {
                operation: "daemon DFS authoritative prefix projection".to_string(),
                requested_bytes: read.value.iter().fold(prefix.len(), |bytes, entry| {
                    bytes
                        .saturating_add(entry.key.len())
                        .saturating_add(entry.value.len())
                }),
            });
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn take_test_authoritative_exact_set_outcome(
        &self,
        deadline: Instant,
    ) -> Option<Result<Vec<Option<KvEntry>>, KvError>> {
        self.authoritative_exact_set_deadlines
            .lock()
            .expect("test authoritative exact-set deadlines")
            .push(deadline);
        self.authoritative_exact_set_outcomes
            .lock()
            .expect("test authoritative exact-set outcomes")
            .pop_front()
            .map(|outcome| match outcome {
                DaemonDfsTestAuthoritativeExactSetOutcome::Error(error) => Err(error),
                DaemonDfsTestAuthoritativeExactSetOutcome::Entries(entries) => Ok(entries),
            })
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.entries_fetch",
        skip_all,
        fields(stage = "dfs.control_plane.entries_fetch", key_count = keys.len())
    )]
    pub(super) fn fetch_authoritative_entries(
        &self,
        keys: &[String],
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let cluster = self.cluster_access()?;
        block_on_dfs_cluster_future(
            &self.runtime,
            cluster.kv_get_authoritative_exact_set_async(keys.to_vec()),
        )
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.entries_fetch",
        skip_all,
        fields(stage = "dfs.control_plane.entries_fetch", phase = "deadline", key_count = keys.len())
    )]
    pub(super) fn fetch_authoritative_entries_before(
        &self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let cluster = self.cluster_access()?;
        block_on_dfs_cluster_future(&self.runtime, async move {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                cluster.kv_get_authoritative_exact_set_async(keys.to_vec()),
            )
            .await
            .map_err(|_| {
                sinomo_core::service::ServiceError::Service(
                    "DFS authoritative exact-set read exceeded its deadline".to_string(),
                )
            })?
        })
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.prefix_fetch",
        skip_all,
        fields(
            stage = "dfs.control_plane.prefix_fetch",
            prefix,
            outcome = "unknown",
            error = tracing::field::Empty
        )
    )]
    pub(super) fn fetch_authoritative_prefix(
        &self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_prefix_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        }
        let result = (|| {
            let cluster = self.cluster_access()?;
            block_on_dfs_cluster_future(
                &self.runtime,
                cluster.kv_range_prefix_authoritative_async(prefix.to_string()),
            )
        })();
        let span = tracing::Span::current();
        span.record("outcome", if result.is_ok() { "ok" } else { "error" });
        if let Err(error) = &result {
            span.record("error", tracing::field::display(error));
        }
        result
    }

    async fn fetch_authoritative_entries_async(
        &self,
        keys: Vec<String>,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        self.cluster_access()?
            .kv_get_authoritative_exact_set_async(keys)
            .await
            .map_err(dfs_cluster_service_error)
    }

    async fn fetch_authoritative_entries_before_async(
        &self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let cluster = self.cluster_access()?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            cluster.kv_get_authoritative_exact_set_async(keys),
        )
        .await
        .map_err(|_| KvError::AuthorityUnavailable {
            detail: "DFS authoritative exact-set read exceeded its deadline".to_string(),
        })?
        .map_err(dfs_cluster_service_error)
    }

    async fn fetch_authoritative_prefix_async(
        &self,
        prefix: String,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_prefix_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        }
        self.cluster_access()?
            .kv_range_prefix_authoritative_async(prefix)
            .await
            .map_err(dfs_cluster_service_error)
    }

    pub(super) fn superseded_authority_error(&self, scope: &str) -> KvError {
        KvError::AuthorityUnavailable {
            detail: format!(
                "daemon DFS authoritative projection for {scope:?} was superseded by {} consecutive generation changes",
                DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS,
            ),
        }
    }
}

pub(super) fn require_dfs_placement_authority_admin_owner(
    paths: &DaemonPaths,
) -> Result<Arc<DaemonDataOwnerLock>, DaemonError> {
    daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| DaemonError::Parse {
        detail: "DFS placement-authority administration requires the live exclusive main-daemon data-owner capability"
            .to_string(),
    })
}

/// sinomo:kv-consistency: Inspect the current global placement permit and exact recovery token for an explicit administrator request.
pub(crate) fn inspect_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
) -> Result<Option<sinomo_dfs::DfsPlacementAuthorityPermitStatus>, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .placement_authority_permit_status(file_system_id)
        .map_err(Into::into)
}

/// sinomo:kv-consistency: Recover an exact abandoned placement permit after proving global removal of its former owning node.
pub(crate) fn recover_removed_node_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
    expected_recovery_token: &str,
    acknowledge_owner_stopped: bool,
) -> Result<sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .recover_removed_node_placement_authority_permit(
            file_system_id,
            &state.cluster_id.to_string(),
            &state.local_node_id,
            expected_recovery_token,
            acknowledge_owner_stopped,
        )
        .map_err(Into::into)
}

/// sinomo:kv-consistency: Perform the explicitly authorized recovery of an exact abandoned global placement permit.
pub(crate) fn break_glass_recover_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
    expected_recovery_token: &str,
    acknowledgement: &str,
) -> Result<sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .break_glass_recover_placement_authority_permit(
            file_system_id,
            &state.cluster_id.to_string(),
            expected_recovery_token,
            acknowledgement,
        )
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn recover_prior_process_default_dfs_placement_authority(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    recover_prior_process_default_dfs_placement_authority_from_observation(paths, runtime, true)
}

/// Recover only a prior-process placement permit owned by this main daemon's
/// local cluster node. Callers are required to hold `DaemonLock` and
/// `DaemonDataOwnerLock`; the control loop acquires both for its full lifetime
/// before this helper is reachable. Exact-value CAS in the DFS store prevents
/// deleting a concurrently replaced holder.
pub(super) fn recover_prior_process_default_dfs_placement_authority_from_observation(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    refresh_latest: bool,
) -> Result<bool, DaemonError> {
    let _data_owner_lock = daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| {
        DaemonError::Parse {
            detail: "refusing placement-authority recovery without a live exclusive main-daemon data-owner lock"
                .to_string(),
        }
    })?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(false);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    let current_process =
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&client)
            .ok_or_else(|| DaemonError::Parse {
                detail: "main daemon has no DFS placement-authority process identity".to_string(),
            })?;
    if refresh_latest {
        sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    }
    let recovered = sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .recover_prior_process_placement_authority_permit_for_local_node(
            &sinomo_dfs::sifs_file_system_id(),
            &state.local_node_id,
            &current_process,
        )?;
    if recovered {
        tracing::info!(
            target: "sinomo.dfs.placement",
            node_id = %state.local_node_id,
            "recovered exact prior-process SIFS placement authority permit"
        );
    }
    Ok(recovered)
}

pub(super) fn daemon_dfs_txn_puts_local_membership_handoff(
    cluster_paths: &ClusterPaths,
    txn: &KvTxn,
) -> bool {
    if !txn.success.iter().any(
        |op| matches!(op, KvOp::Put { key, .. } if daemon_dfs_membership_handoff_key_is_exact(key)),
    ) {
        return false;
    }
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) else {
        return false;
    };
    daemon_dfs_txn_puts_membership_handoff_for_node(txn, &state.local_node_id)
}

pub(super) fn daemon_dfs_membership_handoff_key_is_exact(key: &str) -> bool {
    sinomo_dfs::dfs_metadata_shard_membership_handoff_key_is_exact(key)
}

pub(super) fn daemon_dfs_txn_puts_membership_handoff_for_node(
    txn: &KvTxn,
    local_node_id: &NodeId,
) -> bool {
    txn.success.iter().any(|op| {
        let KvOp::Put { key, value, .. } = op else {
            return false;
        };
        if !daemon_dfs_membership_handoff_key_is_exact(key) {
            return false;
        }
        sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value)
            .is_ok_and(|handoff| handoff.retiring_shard.primary_node_id == *local_node_id)
    })
}

impl sinomo_dfs::DfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn async_client(&mut self) -> Option<&mut dyn sinomo_dfs::AsyncDfsControlPlaneKvClient> {
        Some(self)
    }

    fn write_configuration_projection(
        &self,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, KvError> {
        match self.write_configuration_readiness()? {
            crate::DfsWriteConfigurationReadiness::Ready(view) => Ok(view),
            crate::DfsWriteConfigurationReadiness::Pending => Err(KvError::AuthorityUnavailable {
                detail: "DFS write configuration is awaiting a coherent authoritative projection"
                    .into(),
            }),
        }
    }

    fn write_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, KvError> {
        let Some(authority) = self.retained_metadata_authority.as_ref() else {
            if !self.write_configuration_projection_required {
                return Ok(crate::DfsWriteConfigurationReadiness::Ready(None));
            }
            return Err(KvError::AuthorityUnavailable {
                detail: "daemon write configuration owner is unavailable".into(),
            });
        };
        let generation = authority.generation();
        let readiness = authority.write_configuration_readiness().map_err(|error| {
            KvError::AuthorityUnavailable {
                detail: error.to_string(),
            }
        })?;
        if matches!(
            &readiness,
            crate::DfsWriteConfigurationReadiness::Ready(Some(_))
        ) && (!matches!(
            self.try_synchronize_retained_metadata_authority()?,
            Some(DaemonDfsRetainedMetadataSync::Current)
        ) || authority.generation() != generation)
        {
            return Ok(crate::DfsWriteConfigurationReadiness::Pending);
        }
        #[cfg(any(test, feature = "test-support"))]
        if let crate::DfsWriteConfigurationReadiness::Ready(Some(view)) = &readiness {
            if let Some(root) = self.cluster_paths.root.parent() {
                crate::native_shard_trace::record_configuration_cut(
                    root,
                    self.data_owner_lock().as_ref(),
                    view,
                    generation,
                );
            }
        }
        Ok(readiness)
    }

    fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<crate::DfsMetadataShardConfig>>, KvError> {
        let Some(authority) = self.retained_metadata_authority.as_ref() else {
            return Ok(None);
        };
        if self.synchronize_retained_metadata_authority()? != DaemonDfsRetainedMetadataSync::Current
        {
            return Ok(None);
        }
        authority.cached_metadata_shards(
            file_system_id,
            self.retained_metadata_projection_generation
                .load(Ordering::Acquire),
        )
    }

    fn refresh_latest(&mut self) -> Result<(), KvError> {
        // Invalidate only this client's compact view. The next requested exact
        // key or prefix establishes a fresh bounded ReadIndex projection; a
        // refresh must never import the complete materialized checkpoint.
        self.begin_authoritative_generation()
    }

    fn placement_authority_process_identity(
        &self,
    ) -> Option<sinomo_dfs::DfsPlacementAuthorityProcessIdentity> {
        // The shadow daemon shares cluster state but never owns local data.
        // Only a process which currently retains the main daemon's exclusive
        // data-owner lock may advertise placement mutation authority.
        let _data_owner_lock = self.data_owner_lock()?;
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths).ok()?;
        Some(sinomo_dfs::DfsPlacementAuthorityProcessIdentity {
            cluster_id: state.cluster_id.to_string(),
            process_session_id: DAEMON_DFS_PLACEMENT_AUTHORITY_SESSION_ID
                .get_or_init(|| sinomo_core::Id::generate().to_string())
                .clone(),
        })
    }

    #[cfg(feature = "storage-conversion")]
    fn storage_import_placement_snapshot(&self) -> Option<Arc<sinomo_dfs::DfsPlacementSnapshot>> {
        let _owner = self.data_owner_lock()?;
        let data_dir = self.cluster_paths.root.parent().map(SinomoDataDir::new)?;
        super::placement_environment::retained_storage_import_placement_snapshot(&data_dir)
    }

    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &NodeId,
        current_process: &sinomo_dfs::DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        let Some(_data_owner_lock) = self.data_owner_lock() else {
            return false;
        };
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
        else {
            return false;
        };
        state.local_node_id == *local_node_id
            && sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(self)
                .as_ref()
                == Some(current_process)
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        let keys = [key.to_string()];
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            let retained = self.retained_metadata_authority_covers_key(key);
            let retained_sync = retained
                .then(|| self.synchronize_retained_metadata_authority())
                .transpose()?
                .unwrap_or(DaemonDfsRetainedMetadataSync::Current);
            let (generation, _minimum_revision, cached) = self.entry_read_plan(key)?;
            if let Some(cached) = cached {
                if retained
                    && retained_sync == DaemonDfsRetainedMetadataSync::Current
                    && !self.retained_metadata_authority_is_current()
                {
                    self.invalidate_retained_metadata_authority_seed();
                    continue;
                }
                #[cfg(any(test, feature = "test-support"))]
                if self
                    .retained_metadata_authority
                    .as_ref()
                    .is_some_and(|authority| {
                        crate::test_runtime::native_operation_floor_read_is_denied(
                            authority.as_ref(),
                            key,
                        )
                    })
                {
                    return Err(KvError::Io {
                        path: self.cluster_paths.root.display().to_string(),
                        detail: "resident Raft view changed after ReadIndex quorum; retry the read (injected native operation-floor return)".into(),
                    });
                }
                #[cfg(any(test, feature = "test-support"))]
                if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                    self.record_native_retained_use(
                        crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedKeysReturned { keys: keys.to_vec(), values: vec![cached.clone()] },
                        Some((generation, _minimum_revision)),
                    );
                }
                return Ok(cached);
            }
            if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                // A complete retained prefix proves both presence and absence.
                // A miss here means another operation replaced the private
                // projection after synchronization; reseed instead of issuing
                // an independent exact-key ReadIndex.
                self.invalidate_retained_metadata_authority_seed();
                continue;
            }
            let read = self.fetch_authoritative_entries(&keys)?;
            if self.merge_authoritative_entries(generation, &keys, &read)? {
                #[cfg(any(test, feature = "test-support"))]
                if self
                    .retained_metadata_authority
                    .as_ref()
                    .is_some_and(|authority| {
                        crate::test_runtime::native_operation_floor_read_is_denied(
                            authority.as_ref(),
                            key,
                        )
                    })
                {
                    return Err(KvError::Io {
                        path: self.cluster_paths.root.display().to_string(),
                        detail: "resident Raft view changed after ReadIndex quorum; retry the read (injected native operation-floor return)".into(),
                    });
                }
                return Ok(read.value.into_iter().next().flatten());
            }
        }
        Err(self.superseded_authority_error(key))
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            let retained = keys
                .iter()
                .any(|key| self.retained_metadata_authority_covers_key(key));
            let retained_sync = retained
                .then(|| self.synchronize_retained_metadata_authority())
                .transpose()?
                .unwrap_or(DaemonDfsRetainedMetadataSync::Current);
            let (generation, _minimum_revision, cached) = self.entries_read_plan(keys)?;
            if let Some(cached) = cached {
                if retained
                    && retained_sync == DaemonDfsRetainedMetadataSync::Current
                    && !self.retained_metadata_authority_is_current()
                {
                    self.invalidate_retained_metadata_authority_seed();
                    continue;
                }
                #[cfg(any(test, feature = "test-support"))]
                if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                    self.record_native_retained_use(
                        crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedKeysReturned { keys: keys.to_vec(), values: cached.clone() },
                        Some((generation, _minimum_revision)),
                    );
                }
                return Ok(cached);
            }
            if retained
                && retained_sync == DaemonDfsRetainedMetadataSync::Current
                && keys
                    .iter()
                    .all(|key| self.retained_metadata_authority_covers_key(key))
            {
                self.invalidate_retained_metadata_authority_seed();
                continue;
            }
            let read = self.fetch_authoritative_entries(keys)?;
            if self.merge_authoritative_entries(generation, keys, &read)? {
                return Ok(read.value);
            }
        }
        Err(self.superseded_authority_error("consistent exact-key set"))
    }

    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        // An operation-bound authority check never reuses an older generation.
        // It performs one exact-key-set ReadIndex and publishes only that
        // bounded response into the new projection.
        self.begin_authoritative_generation()?;
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            let (generation, _minimum_revision, _) = self.entries_read_plan(keys)?;
            let read = self.fetch_authoritative_entries(keys)?;
            if self.merge_authoritative_entries(generation, keys, &read)? {
                return Ok(read.value);
            }
        }
        Err(self.superseded_authority_error("operation-bound exact-key set"))
    }

    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if Instant::now() >= deadline {
            return Err(KvError::AuthorityUnavailable {
                detail: "metadata shard authority read deadline elapsed".to_string(),
            });
        }
        self.begin_authoritative_generation()?;
        #[cfg(test)]
        if let Some(result) = self.take_test_authoritative_exact_set_outcome(deadline) {
            return result;
        }
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            if Instant::now() >= deadline {
                return Err(KvError::AuthorityUnavailable {
                    detail: "metadata shard authority read deadline elapsed".to_string(),
                });
            }
            let (generation, _minimum_revision, _) = self.entries_read_plan(keys)?;
            let read = self.fetch_authoritative_entries_before(keys, deadline)?;
            if self.merge_authoritative_entries(generation, keys, &read)? {
                #[cfg(any(test, feature = "test-support"))]
                if let Some(data_root) = self.cluster_paths.root.parent() {
                    crate::native_shard_trace::record_authoritative_read(
                        data_root,
                        self.data_owner_lock().as_ref(),
                        keys,
                        &read,
                        generation,
                    );
                }
                return Ok(read.value);
            }
        }
        Err(self.superseded_authority_error("operation-bound exact-key set"))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            let retained = self.retained_metadata_authority_covers_prefix(prefix);
            let retained_sync = retained
                .then(|| self.synchronize_retained_metadata_authority())
                .transpose()?
                .unwrap_or(DaemonDfsRetainedMetadataSync::Current);
            let (generation, _minimum_revision, cached) = self.prefix_read_plan(prefix)?;
            if let Some(cached) = cached {
                if retained
                    && retained_sync == DaemonDfsRetainedMetadataSync::Current
                    && !self.retained_metadata_authority_is_current()
                {
                    self.invalidate_retained_metadata_authority_seed();
                    continue;
                }
                #[cfg(any(test, feature = "test-support"))]
                if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                    self.record_native_retained_use(
                        crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedPrefixReturned { prefix: prefix.to_string(), entries: cached.clone() },
                        Some((generation, _minimum_revision)),
                    );
                }
                return Ok(cached);
            }
            if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                self.invalidate_retained_metadata_authority_seed();
                continue;
            }
            let read = self.fetch_authoritative_prefix(prefix)?;
            if self.merge_authoritative_prefix(generation, prefix, &read)? {
                return Ok(read.value);
            }
        }
        Err(self.superseded_authority_error(prefix))
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<sinomo_dfs::DfsAuthoritativePrefixProjection, KvError> {
        // Destructive callers must never inherit an older retained prefix
        // certificate. Start a fresh generation and cross exactly one bounded
        // prefix ReadIndex (apart from a concurrent-generation replan).
        self.begin_authoritative_generation()?;
        for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
            let (generation, _minimum_revision, _) = self.prefix_read_plan(prefix)?;
            let read = self.fetch_authoritative_prefix(prefix)?;
            if self.merge_authoritative_prefix(generation, prefix, &read)? {
                return sinomo_dfs::DfsAuthoritativePrefixProjection::complete(
                    prefix,
                    read.value,
                    read.authority.revision,
                );
            }
        }
        Err(self.superseded_authority_error("operation-bound prefix"))
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let notify_handoff =
            daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
        let cluster = self.cluster_access()?;
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        #[cfg(any(test, feature = "test-support"))]
        let native_request = self.cluster_paths.root.parent().and_then(|root| {
            let owner = self.data_owner_lock()?;
            crate::native_shard_trace::has_root_interest(root, &owner).then(|| request.clone())
        });
        let result =
            block_on_dfs_cluster_future(&self.runtime, cluster.kv_commit_compact_async(request))?;
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(root), Some(request)) =
            (self.cluster_paths.root.parent(), native_request.as_ref())
        {
            crate::native_shard_trace::record_native_control_commit(
                root,
                self.data_owner_lock().as_ref(),
                request,
                &result,
            );
        }
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "component cluster commit returned no transaction response".into(),
        })?;
        if response.succeeded && notify_handoff {
            if let Some(root) = self.cluster_paths.root.parent() {
                notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
            }
        }
        self.begin_authoritative_generation_at_revision(response.revision)?;
        Ok(response)
    }

    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<crate::DfsNativeTxnCommit>), KvError> {
        let notify_handoff =
            daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
        let cluster = self.cluster_access()?;
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        let result = block_on_dfs_cluster_future(
            &self.runtime,
            cluster.kv_commit_compact_async(request.clone()),
        )?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(root) = self.cluster_paths.root.parent() {
            crate::native_shard_trace::record_native_control_commit(
                root,
                self.data_owner_lock().as_ref(),
                &request,
                &result,
            );
        }
        let response = result
            .txn_response
            .clone()
            .ok_or_else(|| KvError::Serialization {
                detail: "component cluster commit returned no transaction response".into(),
            })?;
        if response.succeeded && notify_handoff {
            if let Some(root) = self.cluster_paths.root.parent() {
                notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
            }
        }
        self.begin_authoritative_generation_at_revision(response.revision)?;
        Ok((
            response,
            Some(crate::DfsNativeTxnCommit { request, result }),
        ))
    }
}

impl sinomo_dfs::AsyncDfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn refresh_latest_async<'a>(&'a mut self) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, ()> {
        let result = self.begin_authoritative_generation();
        Box::pin(async move { result })
    }

    fn get_async<'a>(
        &'a self,
        key: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move {
            let keys = vec![key.to_string()];
            for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
                let retained = self.retained_metadata_authority_covers_key(key);
                let retained_sync = if retained {
                    self.synchronize_retained_metadata_authority_async().await?
                } else {
                    DaemonDfsRetainedMetadataSync::Current
                };
                let (generation, _minimum_revision, cached) = self.entry_read_plan(key)?;
                if let Some(cached) = cached {
                    if retained
                        && retained_sync == DaemonDfsRetainedMetadataSync::Current
                        && !self.retained_metadata_authority_is_current()
                    {
                        self.invalidate_retained_metadata_authority_seed();
                        continue;
                    }
                    #[cfg(any(test, feature = "test-support"))]
                    if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                        self.record_native_retained_use(
                        crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedKeysReturned { keys: keys.clone(), values: vec![cached.clone()] },
                        Some((generation, _minimum_revision)),
                    );
                    }
                    return Ok(cached);
                }
                if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                    self.invalidate_retained_metadata_authority_seed();
                    continue;
                }
                let read = self.fetch_authoritative_entries_async(keys.clone()).await?;
                if self.merge_authoritative_entries(generation, &keys, &read)? {
                    return Ok(read.value.into_iter().next().flatten());
                }
            }
            Err(self.superseded_authority_error(key))
        })
    }

    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        Box::pin(async move {
            for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
                let retained = keys
                    .iter()
                    .any(|key| self.retained_metadata_authority_covers_key(key));
                let retained_sync = if retained {
                    self.synchronize_retained_metadata_authority_async().await?
                } else {
                    DaemonDfsRetainedMetadataSync::Current
                };
                let (generation, _minimum_revision, cached) = self.entries_read_plan(&keys)?;
                if let Some(cached) = cached {
                    if retained
                        && retained_sync == DaemonDfsRetainedMetadataSync::Current
                        && !self.retained_metadata_authority_is_current()
                    {
                        self.invalidate_retained_metadata_authority_seed();
                        continue;
                    }
                    #[cfg(any(test, feature = "test-support"))]
                    if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                        self.record_native_retained_use(
                        crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedKeysReturned { keys: keys.clone(), values: cached.clone() },
                        Some((generation, _minimum_revision)),
                    );
                    }
                    return Ok(cached);
                }
                if retained
                    && retained_sync == DaemonDfsRetainedMetadataSync::Current
                    && keys
                        .iter()
                        .all(|key| self.retained_metadata_authority_covers_key(key))
                {
                    self.invalidate_retained_metadata_authority_seed();
                    continue;
                }
                let read = self.fetch_authoritative_entries_async(keys.clone()).await?;
                if self.merge_authoritative_entries(generation, &keys, &read)? {
                    return Ok(read.value);
                }
            }
            Err(self.superseded_authority_error("consistent exact-key set"))
        })
    }

    fn get_authoritative_consistent_async<'a>(
        &'a mut self,
        keys: Vec<String>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        let generation_result = self.begin_authoritative_generation();
        Box::pin(async move {
            generation_result?;
            for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
                let (generation, _minimum_revision, _) = self.entries_read_plan(&keys)?;
                let read = self.fetch_authoritative_entries_async(keys.clone()).await?;
                if self.merge_authoritative_entries(generation, &keys, &read)? {
                    return Ok(read.value);
                }
            }
            Err(self.superseded_authority_error("operation-bound exact-key set"))
        })
    }

    fn get_authoritative_consistent_before_async<'a>(
        &'a mut self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        if Instant::now() >= deadline {
            return Box::pin(async {
                Err(KvError::AuthorityUnavailable {
                    detail: "metadata shard authority read deadline elapsed".to_string(),
                })
            });
        }
        let generation_result = self.begin_authoritative_generation();
        Box::pin(async move {
            generation_result?;
            #[cfg(test)]
            if let Some(result) = self.take_test_authoritative_exact_set_outcome(deadline) {
                return result;
            }
            for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
                if Instant::now() >= deadline {
                    return Err(KvError::AuthorityUnavailable {
                        detail: "metadata shard authority read deadline elapsed".to_string(),
                    });
                }
                let (generation, _minimum_revision, _) = self.entries_read_plan(&keys)?;
                let read = self
                    .fetch_authoritative_entries_before_async(keys.clone(), deadline)
                    .await?;
                if self.merge_authoritative_entries(generation, &keys, &read)? {
                    return Ok(read.value);
                }
            }
            Err(self.superseded_authority_error("operation-bound exact-key set"))
        })
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move {
            for _ in 0..DAEMON_DFS_AUTHORITY_PROJECTION_REPLAN_MAX_ATTEMPTS {
                let retained = self.retained_metadata_authority_covers_prefix(prefix);
                let retained_sync = if retained {
                    self.synchronize_retained_metadata_authority_async().await?
                } else {
                    DaemonDfsRetainedMetadataSync::Current
                };
                let (generation, _minimum_revision, cached) = self.prefix_read_plan(prefix)?;
                if let Some(cached) = cached {
                    if retained
                        && retained_sync == DaemonDfsRetainedMetadataSync::Current
                        && !self.retained_metadata_authority_is_current()
                    {
                        self.invalidate_retained_metadata_authority_seed();
                        continue;
                    }
                    #[cfg(any(test, feature = "test-support"))]
                    if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                        self.record_native_retained_use(
                        crate::native_shard_trace::NativeControlConsumptionScopeV1::RetainedPrefixReturned { prefix: prefix.to_string(), entries: cached.clone() },
                        Some((generation, _minimum_revision)),
                    );
                    }
                    return Ok(cached);
                }
                if retained && retained_sync == DaemonDfsRetainedMetadataSync::Current {
                    self.invalidate_retained_metadata_authority_seed();
                    continue;
                }
                let read = self
                    .fetch_authoritative_prefix_async(prefix.to_string())
                    .await?;
                if self.merge_authoritative_prefix(generation, prefix, &read)? {
                    return Ok(read.value);
                }
            }
            Err(self.superseded_authority_error(prefix))
        })
    }

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move {
            let response = self
                .txn_async(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key,
                        value,
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .await?;
            Ok(response.revision)
        })
    }

    fn txn_async<'a>(
        &'a mut self,
        txn: KvTxn,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            let notify_handoff =
                daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
            let request = sinomo_core::kv::KvTxnRequest::generated(txn);
            #[cfg(any(test, feature = "test-support"))]
            let native_request = self.cluster_paths.root.parent().and_then(|root| {
                let owner = self.data_owner_lock()?;
                crate::native_shard_trace::has_root_interest(root, &owner).then(|| request.clone())
            });
            let result = self
                .cluster_access()?
                .kv_commit_compact_async(request)
                .await
                .map_err(dfs_cluster_service_error)?;
            #[cfg(any(test, feature = "test-support"))]
            if let (Some(root), Some(request)) =
                (self.cluster_paths.root.parent(), native_request.as_ref())
            {
                crate::native_shard_trace::record_native_control_commit(
                    root,
                    self.data_owner_lock().as_ref(),
                    request,
                    &result,
                );
            }
            let response = result.txn_response.ok_or_else(|| KvError::Serialization {
                detail: "component cluster commit returned no transaction response".to_string(),
            })?;
            if response.succeeded && notify_handoff {
                if let Some(root) = self.cluster_paths.root.parent() {
                    notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
                }
            }
            self.begin_authoritative_generation_at_revision(response.revision)?;
            Ok(response)
        })
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct NativePlacementProgressRpcGate {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
    pub(crate) cancelled: std::sync::atomic::AtomicBool,
    pub(crate) armed: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
#[derive(Debug)]
struct GatedNativePlacementControlPlane {
    client: DaemonDfsControlPlaneKvClient,
    gate: Arc<NativePlacementProgressRpcGate>,
}

#[cfg(test)]
pub(crate) fn gated_native_placement_control_plane_for_test(
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    entries: Vec<KvEntry>,
    gate: Arc<NativePlacementProgressRpcGate>,
) -> Box<dyn sinomo_dfs::DfsControlPlaneKvClient> {
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(cluster_paths, runtime);
    // This isolated test exercises the real native RPC, rather than the
    // independently tested configuration-projection bootstrap owner.
    client.write_configuration_projection_required = false;
    for entry in entries {
        sinomo_dfs::DfsControlPlaneKvClient::put(&mut client, entry.key, entry.value)
            .expect("seed placement state through native compact commits");
    }
    gate.armed.store(true, Ordering::Release);
    Box::new(GatedNativePlacementControlPlane { client, gate })
}

#[cfg(test)]
impl sinomo_dfs::DfsControlPlaneKvClient for GatedNativePlacementControlPlane {
    fn async_client(&mut self) -> Option<&mut dyn sinomo_dfs::AsyncDfsControlPlaneKvClient> {
        Some(self)
    }
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut self.client)
    }
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::get(&self.client, key)
    }
    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::get_consistent(&self.client, keys)
    }
    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::get_authoritative_consistent(&mut self.client, keys)
    }
    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::get_authoritative_consistent_before(
            &mut self.client,
            keys,
            deadline,
        )
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::range_prefix(&self.client, prefix)
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        sinomo_dfs::DfsControlPlaneKvClient::put(&mut self.client, key, value)
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let is_progress = txn.success.iter().any(|op| {
            matches!(
                op, KvOp::Put { key, .. } if key.ends_with("/placement-test-job")
            )
        });
        assert!(
            !is_progress || !self.gate.armed.load(Ordering::Acquire),
            "async placement progress must not enter synchronous native RPC"
        );
        sinomo_dfs::DfsControlPlaneKvClient::txn(&mut self.client, txn)
    }
    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<crate::DfsNativeTxnCommit>), KvError> {
        let is_progress = txn.success.iter().chain(&txn.failure).any(|op| {
            matches!(
                op, KvOp::Put { key, .. } if key.ends_with("/placement-test-job")
            )
        });
        assert!(
            !is_progress || !self.gate.armed.load(Ordering::Acquire),
            "async placement progress must not enter synchronous native RPC"
        );
        sinomo_dfs::DfsControlPlaneKvClient::txn_with_native_authority(&mut self.client, txn)
    }
}

#[cfg(test)]
impl sinomo_dfs::AsyncDfsControlPlaneKvClient for GatedNativePlacementControlPlane {
    fn refresh_latest_async<'a>(&'a mut self) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, ()> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::refresh_latest_async(&mut self.client)
    }
    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::get_consistent_async(&self.client, keys)
    }
    fn get_authoritative_consistent_async<'a>(
        &'a mut self,
        keys: Vec<String>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::get_authoritative_consistent_async(
            &mut self.client,
            keys,
        )
    }
    fn get_authoritative_consistent_before_async<'a>(
        &'a mut self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::get_authoritative_consistent_before_async(
            &mut self.client,
            keys,
            deadline,
        )
    }
    fn get_async<'a>(
        &'a self,
        key: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(&self.client, key)
    }
    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::range_prefix_async(&self.client, prefix)
    }
    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvRevision> {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::put_async(&mut self.client, key, value)
    }
    fn txn_async<'a>(
        &'a mut self,
        txn: KvTxn,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            struct PendingRpc<'a> {
                gate: &'a NativePlacementProgressRpcGate,
                completed: bool,
            }
            impl Drop for PendingRpc<'_> {
                fn drop(&mut self) {
                    if !self.completed {
                        self.gate.cancelled.store(true, Ordering::Release);
                    }
                }
            }
            let mut native =
                sinomo_dfs::AsyncDfsControlPlaneKvClient::txn_async(&mut self.client, txn);
            if self.gate.armed.swap(false, Ordering::AcqRel) {
                let mut pending_rpc = PendingRpc {
                    gate: &self.gate,
                    completed: false,
                };
                // Poll the genuine compact-quorum future, whose first native
                // storage step is asynchronous. Gate that same pending future;
                // no substitute response, detached mutation or sleep is used.
                std::future::poll_fn(|cx| {
                    assert!(
                        native.as_mut().poll(cx).is_pending(),
                        "native RPC must be pending at the held boundary"
                    );
                    std::task::Poll::Ready(())
                })
                .await;
                self.gate.entered.notify_one();
                self.gate.release.notified().await;
                let result = native.await;
                pending_rpc.completed = true;
                return result;
            }
            native.await
        })
    }
}

#[cfg(test)]
pub(crate) async fn test_retained_client_configuration_sync(
    authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    paths: &ClusterPaths,
) -> Result<(), KvError> {
    let mut client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(paths, &tokio::runtime::Handle::current());
    client.retained_metadata_authority = Some(authority);
    client.write_configuration_projection_required = true;
    client
        .synchronize_retained_metadata_authority_async()
        .await?;
    sinomo_dfs::DfsControlPlaneKvClient::write_configuration_projection(&client).map(|_| ())
}
