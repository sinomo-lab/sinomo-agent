use super::*;

#[derive(Clone, Copy, Debug)]
enum Injection {
    Bookkeeping { remaining: usize },
    Configuration,
    SameJob,
    AppliedThenLostReply,
}

#[derive(Debug)]
struct RacingPlacementKv {
    inner: InProcessKvStore,
    observed: BTreeMap<String, KvEntry>,
    expected: DfsFileSystemConfig,
    job: DfsPlacementReconciliationJob,
    injection: Injection,
    publication_attempts: usize,
    authoritative_refreshes: usize,
    background_ids: Vec<String>,
}

impl RacingPlacementKv {
    fn new(injection: Injection) -> (Self, DfsFileSystemConfig) {
        let owner = node("placement-retry-owner");
        let expected = placement_authority_test_config(&owner, 2);
        let desired = placement_authority_test_config(&owner, 3);
        let job = placement_authority_test_job(
            "reviewed-stable-job",
            &expected.file_system_id,
            &owner,
            99,
        );
        let mut inner = InProcessKvStore::new();
        inner
            .put_serde(
                file_system_config_key(&expected.file_system_id),
                &expected,
                None,
            )
            .unwrap();
        let observed = inner
            .snapshot()
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        (
            Self {
                inner,
                observed,
                expected,
                job,
                injection,
                publication_attempts: 0,
                authoritative_refreshes: 0,
                background_ids: Vec::new(),
            },
            desired,
        )
    }

    fn refresh(&mut self) {
        self.observed = self
            .inner
            .snapshot()
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
    }

    fn publish_background(&mut self, same_job: bool) {
        let mut background = self.job.clone();
        if !same_job {
            background.job_id = format!("concurrent-background-{}", self.publication_attempts);
            background.path = format!("/background/{}", self.publication_attempts);
            background.created_at_unix_nanos = self.publication_attempts as u64;
            background.updated_at_unix_nanos = background.created_at_unix_nanos;
        }
        assert!(
            DfsControlPlaneStore::new(&mut self.inner)
                .compare_and_put_placement_update(&self.expected, &self.expected, &background)
                .unwrap()
        );
        self.background_ids.push(background.job_id);
    }

    fn apply(
        &mut self,
        desired: &DfsFileSystemConfig,
        asynchronous: bool,
    ) -> Result<bool, KvError> {
        let expected = self.expected.clone();
        let job = self.job.clone();
        if asynchronous {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(
                    DfsControlPlaneAsyncStore::new(self)
                        .compare_and_put_placement_update(&expected, desired, &job),
                )
        } else {
            DfsControlPlaneStore::new(self)
                .compare_and_put_placement_update(&expected, desired, &job)
        }
    }
}

impl DfsControlPlaneKvClient for RacingPlacementKv {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.authoritative_refreshes += 1;
        self.refresh();
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        Ok(self.observed.get(key).cloned())
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        Ok(self
            .observed
            .values()
            .filter(|entry| entry.key.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner.put(key, value, None)
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.publication_attempts += 1;
        match self.injection {
            Injection::Bookkeeping { remaining } if remaining > 0 => {
                self.injection = Injection::Bookkeeping {
                    remaining: remaining - 1,
                };
                self.publish_background(false);
            }
            Injection::Configuration if self.publication_attempts == 1 => {
                let mut changed = self.expected.clone();
                changed.display_name = "concurrent reviewed edit".into();
                self.inner.put_serde(
                    file_system_config_key(&changed.file_system_id),
                    &changed,
                    None,
                )?;
            }
            Injection::SameJob if self.publication_attempts == 1 => self.publish_background(true),
            Injection::AppliedThenLostReply => {
                let result = self.inner.txn(txn)?;
                assert!(result.succeeded);
                return Err(KvError::Io {
                    path: "ambiguous-placement-commit".into(),
                    detail: "reply lost after durable application".into(),
                });
            }
            _ => {}
        }
        // The observed image deliberately remains stale after rejection until
        // the explicit authoritative consistency boundary refreshes it.
        self.inner.txn(txn)
    }
}

impl AsyncDfsControlPlaneKvClient for RacingPlacementKv {
    fn refresh_latest_async<'a>(&'a mut self) -> DfsControlPlaneKvFuture<'a, ()> {
        Box::pin(async move { DfsControlPlaneKvClient::refresh_latest(self) })
    }
    fn get_async<'a>(&'a self, key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move { DfsControlPlaneKvClient::get(self, key) })
    }
    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        Box::pin(async move { DfsControlPlaneKvClient::get_consistent(self, &keys) })
    }
    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move { DfsControlPlaneKvClient::range_prefix(self, prefix) })
    }
    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move { DfsControlPlaneKvClient::put(self, key, value) })
    }
    fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move { DfsControlPlaneKvClient::txn(self, txn) })
    }
}

#[test]
fn placement_publication_retries_queue_and_activity_races_without_losing_background_work() {
    for asynchronous in [false, true] {
        let (mut kv, desired) = RacingPlacementKv::new(Injection::Bookkeeping { remaining: 3 });
        assert!(kv.apply(&desired, asynchronous).unwrap());
        assert_eq!(kv.publication_attempts, 4);
        assert_eq!(kv.authoritative_refreshes, 6);
        let file_system = &desired.file_system_id;
        let queue = kv
            .inner
            .get(&placement_reconciliation_queue_key(file_system))
            .unwrap()
            .decode::<DfsPlacementReconciliationQueue>()
            .unwrap();
        assert_eq!(queue.job_ids.len(), 4);
        for id in &kv.background_ids {
            assert!(
                queue.job_ids.contains(id),
                "concurrent background work cannot be overwritten"
            );
            assert!(
                kv.inner
                    .get(&placement_reconciliation_key(file_system, id))
                    .is_some()
            );
        }
        assert!(queue.job_ids.contains(&kv.job.job_id));
        let activity = kv
            .inner
            .get(&placement_reconciliation_activity_index_key(file_system))
            .unwrap()
            .decode::<DfsPlacementReconciliationActivityIndex>()
            .unwrap();
        assert_eq!(activity.recent.len(), 4);
        let committed = kv
            .inner
            .get(&placement_reconciliation_key(file_system, &kv.job.job_id))
            .unwrap()
            .decode::<DfsPlacementReconciliationJob>()
            .unwrap();
        assert_eq!(committed.job_id, kv.job.job_id);
        assert_eq!(committed.candidate_snapshot, kv.job.candidate_snapshot);
        assert_eq!(
            committed.accepted_config,
            scoped_placement_config_for_job(&desired, &kv.job)
        );
    }
}

#[test]
fn placement_publication_rejects_changed_config_and_existing_job_identity() {
    for asynchronous in [false, true] {
        for injection in [Injection::Configuration, Injection::SameJob] {
            let (mut kv, desired) = RacingPlacementKv::new(injection);
            assert!(!kv.apply(&desired, asynchronous).unwrap());
            assert_eq!(
                kv.publication_attempts, 1,
                "new approved inputs cannot be substituted"
            );
            assert_eq!(kv.authoritative_refreshes, 1);
            assert_ne!(
                kv.inner
                    .get(&file_system_config_key(&desired.file_system_id))
                    .unwrap()
                    .decode::<DfsFileSystemConfig>()
                    .unwrap(),
                desired
            );
        }
    }
}

// Negative stale observations/counters around an actual native CP client.
// This wrapper owns no private quorum state or placement holder factory.
#[derive(Debug)]
struct NativePlacementObservationKv {
    inner: Box<dyn DfsControlPlaneKvClient>,
    observed: Option<BTreeMap<String, Option<KvEntry>>>,
    publication_attempts: usize,
    authoritative_refreshes: usize,
}
impl DfsControlPlaneKvClient for NativePlacementObservationKv {
    fn async_client(&mut self) -> Option<&mut dyn AsyncDfsControlPlaneKvClient> {
        Some(self)
    }
    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        self.inner.placement_authority_process_identity()
    }
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.inner.refresh_latest()?;
        self.authoritative_refreshes += 1;
        self.observed = None;
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        if let Some(entry) = self
            .observed
            .as_ref()
            .and_then(|observed| observed.get(key))
        {
            return Ok(entry.clone());
        }
        self.inner.get(key)
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.inner.range_prefix(prefix)
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner.put(key, value)
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.publication_attempts += 1;
        self.inner.txn(txn)
    }
    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<DfsNativeTxnCommit>), KvError> {
        self.inner.txn_with_native_authority(txn)
    }
}
impl AsyncDfsControlPlaneKvClient for NativePlacementObservationKv {
    fn refresh_latest_async<'a>(&'a mut self) -> DfsControlPlaneKvFuture<'a, ()> {
        Box::pin(async move {
            self.inner
                .async_client()
                .expect("actual native async CP")
                .refresh_latest_async()
                .await?;
            self.authoritative_refreshes += 1;
            self.observed = None;
            Ok(())
        })
    }
    fn get_async<'a>(&'a self, key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        let result = DfsControlPlaneKvClient::get(self, key);
        Box::pin(async move { result })
    }
    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        let result = DfsControlPlaneKvClient::get_consistent(self, &keys);
        Box::pin(async move { result })
    }
    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        let result = DfsControlPlaneKvClient::range_prefix(self, prefix);
        Box::pin(async move { result })
    }
    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move {
            self.inner
                .async_client()
                .expect("actual native async CP")
                .put_async(key, value)
                .await
        })
    }
    fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            self.publication_attempts += 1;
            self.inner
                .async_client()
                .expect("actual native async CP")
                .txn_async(txn)
                .await
        })
    }
}

#[test]
fn placement_publication_stops_on_live_permit_after_definite_cas_rejection() {
    for asynchronous in [false, true] {
        let name = format!("native-placement-live-permit-{asynchronous}");
        let (runtime, nodes) =
            crate::test_support::DfsTestRuntime::start_cluster(&name, 1).unwrap();
        let owner = &nodes[0];
        let mut policy =
            crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
        policy.required_node_ids = nodes.clone();
        let actual = runtime
            .apply_placement_policy(
                owner,
                "/native-placement-held",
                DfsPlacementScope::SelfOnly,
                policy,
            )
            .unwrap();
        let mut planner = runtime.client(owner).unwrap();
        let candidates = planner.placement_content_candidates().unwrap();
        let snapshot = planner
            .placement_snapshot_cache
            .snapshot()
            .map(|snapshot| (*snapshot).clone());
        let mut inner = planner.control_plane.take().unwrap();
        drop(planner);
        let config = DfsControlPlaneStore::new(inner.as_mut())
            .get_file_system_config(&actual.file_system_id)
            .unwrap()
            .unwrap();
        // SAME normal Apply builder, with real native candidates and snapshot.
        // This new request has not yet been committed or scanned.
        let job = build_placement_reconciliation_job(
            sinomo_core::Id::generate().to_string(),
            &config,
            actual.path.clone(),
            actual.scope,
            BTreeSet::new(),
            candidates,
            snapshot,
            crate::metadata_records::dfs_now_unix_nanos(),
        )
        .unwrap();
        let publication = PlacementUpdatePublication::new(&config, &config, &job).unwrap();
        let keys = publication.keys;
        let entries = inner.get_authoritative_consistent(&keys).unwrap();
        let mut kv = NativePlacementObservationKv {
            inner,
            observed: Some(keys.into_iter().zip(entries).collect()),
            publication_attempts: 0,
            authoritative_refreshes: 0,
        };
        runtime
            .with_current_placement_permit(owner, &actual.file_system_id, &actual.job_id, || {
                let result = if asynchronous {
                    runtime.runtime_handle().block_on(async {
                        DfsControlPlaneAsyncStore::new(&mut kv)
                            .compare_and_put_placement_update(&config, &config, &job)
                            .await
                    })
                } else {
                    DfsControlPlaneStore::new(&mut kv)
                        .compare_and_put_placement_update(&config, &config, &job)
                };
                assert!(matches!(result, Err(KvError::Busy { .. })));
                assert_eq!(kv.publication_attempts, 1);
                assert_eq!(kv.authoritative_refreshes, 1);
                assert!(
                    kv.inner
                        .get(&placement_reconciliation_key(
                            &job.file_system_id,
                            &job.job_id
                        ))
                        .unwrap()
                        .is_none()
                );
            })
            .unwrap();
        drop(kv);
        runtime.close().unwrap();
    }
}

#[test]
fn placement_publication_never_replays_an_ambiguous_applied_transaction() {
    for asynchronous in [false, true] {
        let (mut kv, desired) = RacingPlacementKv::new(Injection::AppliedThenLostReply);
        assert!(matches!(
            kv.apply(&desired, asynchronous),
            Err(KvError::Io { .. })
        ));
        assert_eq!(kv.publication_attempts, 1);
        assert_eq!(kv.authoritative_refreshes, 0);
        assert!(
            kv.inner
                .get(&placement_reconciliation_key(
                    &desired.file_system_id,
                    &kv.job.job_id
                ))
                .is_some()
        );
        assert_eq!(
            kv.inner
                .get(&file_system_config_key(&desired.file_system_id))
                .unwrap()
                .decode::<DfsFileSystemConfig>()
                .unwrap(),
            desired
        );
    }
}

#[test]
fn placement_publication_exhaustion_keeps_original_config_and_all_background_jobs() {
    for asynchronous in [false, true] {
        let (mut kv, desired) = RacingPlacementKv::new(Injection::Bookkeeping {
            remaining: PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES + 1,
        });
        assert!(!kv.apply(&desired, asynchronous).unwrap());
        assert_eq!(
            kv.publication_attempts,
            PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES
        );
        assert_eq!(
            kv.authoritative_refreshes,
            2 * (PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES - 1)
        );
        let queue = kv
            .inner
            .get(&placement_reconciliation_queue_key(&desired.file_system_id))
            .unwrap()
            .decode::<DfsPlacementReconciliationQueue>()
            .unwrap();
        assert_eq!(
            queue.job_ids.len(),
            PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES
        );
        assert!(
            kv.inner
                .get(&placement_reconciliation_key(
                    &desired.file_system_id,
                    &kv.job.job_id
                ))
                .is_none()
        );
        assert_eq!(
            kv.inner
                .get(&file_system_config_key(&desired.file_system_id))
                .unwrap()
                .decode::<DfsFileSystemConfig>()
                .unwrap(),
            kv.expected
        );
    }
}

#[test]
fn missing_placement_queue_ignores_history_and_explicit_queue_bounds_active_jobs() {
    let (kv, desired) = RacingPlacementKv::new(Injection::Bookkeeping { remaining: 0 });
    let publication = PlacementUpdatePublication::new(&kv.expected, &desired, &kv.job).unwrap();
    let entry = |index: usize, terminal: bool| {
        let mut job = kv.job.clone();
        job.job_id = format!("legacy-queue-job-{index}");
        job.path = format!("/background/{index}");
        job.accepted_config = scoped_placement_config_for_job(&kv.expected, &job);
        if terminal {
            job.state = DfsPlacementReconciliationState::Completed;
        }
        KvEntry {
            key: placement_reconciliation_key(&job.file_system_id, &job.job_id),
            value: sinomo_core::kv::encode_kv_value(&job).unwrap(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        }
    };
    let inputs = || {
        publication
            .inputs(DfsControlPlaneKvClient::get_consistent(&kv, &publication.keys).unwrap())
            .unwrap()
            .unwrap()
    };
    let permit = || KvCompare::Missing {
        key: placement_authority_permit_key(&kv.expected.file_system_id),
    };
    let mut history = (0..MAX_ACTIVE_PLACEMENT_RECONCILIATIONS + 1)
        .map(|index| entry(index, true))
        .collect::<Vec<_>>();
    history.push(entry(MAX_ACTIVE_PLACEMENT_RECONCILIATIONS + 2, false));
    let txn = publication
        .transaction(inputs(), history, permit())
        .unwrap();
    let queue_key = placement_reconciliation_queue_key(&kv.expected.file_system_id);
    let encoded_queue = txn
        .success
        .iter()
        .find_map(|op| match op {
            KvOp::Put { key, value, .. } if key == &queue_key => Some(value),
            _ => None,
        })
        .unwrap();
    let queue =
        sinomo_core::kv::decode_kv_value::<DfsPlacementReconciliationQueue>(encoded_queue).unwrap();
    assert_eq!(
        queue.job_ids.len(),
        1,
        "missing current queue cannot resurrect an unindexed historical job"
    );
    let active = (0..MAX_ACTIVE_PLACEMENT_RECONCILIATIONS + 1)
        .map(|index| entry(index, false))
        .collect::<Vec<_>>();
    // Pure builder DATA: current execution work is named by its explicit queue,
    // not recovered from an unrelated terminal-history scan. No txn is applied.
    let mut explicit = inputs();
    explicit.queue = Some(KvEntry {
        key: queue_key,
        value: sinomo_core::kv::encode_kv_value(&DfsPlacementReconciliationQueue {
            schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
            job_ids: (0..MAX_ACTIVE_PLACEMENT_RECONCILIATIONS + 1)
                .map(|index| format!("legacy-queue-job-{index}"))
                .collect(),
        })
        .unwrap(),
        create_revision: KvRevision(1),
        modify_revision: KvRevision(1),
        version: sinomo_core::kv::KvVersion(1),
        lease_id: None,
    });
    assert!(matches!(
        publication.transaction(explicit, active, permit()),
        Err(KvError::RangeLimit {
            max_entries: MAX_ACTIVE_PLACEMENT_RECONCILIATIONS,
            ..
        })
    ));
}

#[derive(Debug)]
struct CountingPageAuthorityKv {
    inner: InProcessKvStore,
    read_keys: Vec<Vec<String>>,
    txn_calls: usize,
    read_error: bool,
    lost_progress_reply: bool,
}

impl DfsControlPlaneKvClient for CountingPageAuthorityKv {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        Ok(self.inner.get(key).cloned())
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.inner.range_prefix(prefix)
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner.put(key, value, None)
    }
    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        _deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.read_keys.push(keys.to_vec());
        if self.read_error {
            return Err(KvError::Io {
                path: "page-read-index".into(),
                detail: "unknown response".into(),
            });
        }
        DfsControlPlaneKvClient::get_consistent(&self.inner, keys)
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.txn_calls += 1;
        let response = self.inner.txn(txn)?;
        if self.lost_progress_reply && response.succeeded {
            return Err(KvError::Io {
                path: "page-progress-commit".into(),
                detail: "reply lost after application".into(),
            });
        }
        Ok(response)
    }
}

fn page_authority_fixture() -> (
    CountingPageAuthorityKv,
    DfsFileSystemConfig,
    DfsMetadataShardConfig,
    DfsPlacementReconciliationJob,
) {
    // Exact-entry comparison DATA captured from one real normal Apply. This
    // private matcher returns no native transaction certificate or author guard
    // and never publishes scanner progress into the mounted source owner.
    let (inner, committed) = native_placement_activity_fixture_data("native-page-authority-data");
    let config = inner
        .get(&file_system_config_key(&committed.file_system_id))
        .unwrap()
        .decode::<DfsFileSystemConfig>()
        .unwrap();
    let shard = inner
        .get(&metadata_shard_key(&committed.file_system_id, "root"))
        .unwrap()
        .decode::<DfsMetadataShardConfig>()
        .unwrap();
    (
        CountingPageAuthorityKv {
            inner,
            read_keys: Vec::new(),
            txn_calls: 0,
            read_error: false,
            lost_progress_reply: false,
        },
        config,
        shard,
        committed,
    )
}

#[test]
fn placement_page_authority_one_read_binds_config_shard_handoff_and_noop_progress() {
    let (mut kv, config, shard, job) = page_authority_fixture();
    let compares = DfsControlPlaneStore::new(&mut kv)
        .placement_page_authority_compares_before(&config, std::slice::from_ref(&shard), None)
        .unwrap()
        .unwrap();
    assert_eq!(
        kv.read_keys,
        vec![vec![
            file_system_config_key(&config.file_system_id),
            metadata_shard_key(&config.file_system_id, "root"),
            metadata_shard_membership_handoff_key(&config.file_system_id, "root")
        ]]
    );
    let mut progressed = job.clone();
    progressed.state = DfsPlacementReconciliationState::Scanning;
    progressed.scanned_object_count = 64;
    progressed.revision += 1;
    assert!(
        DfsControlPlaneStore::new(&mut kv)
            .compare_and_put_placement_reconciliation_with_guards(&job, &progressed, compares)
            .unwrap()
    );
    assert_eq!(kv.txn_calls, 1);
    let durable = DfsControlPlaneStore::new(&mut kv)
        .placement_reconciliation(&config.file_system_id, &job.job_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.scanned_object_count, 64);
}

#[test]
fn placement_page_authority_changes_discard_checkpoint_progress() {
    for change in ["epoch", "handoff", "configuration"] {
        let (mut kv, config, mut shard, job) = page_authority_fixture();
        let compares = DfsControlPlaneStore::new(&mut kv)
            .placement_page_authority_compares_before(&config, std::slice::from_ref(&shard), None)
            .unwrap()
            .unwrap();
        match change {
            "epoch" => {
                shard.epoch += 1;
                kv.inner
                    .put_serde(
                        metadata_shard_key(&config.file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .unwrap();
            }
            "handoff" => {
                kv.inner
                    .put(
                        metadata_shard_membership_handoff_key(&config.file_system_id, "root"),
                        b"pending-handoff".to_vec(),
                        None,
                    )
                    .unwrap();
            }
            _ => {
                let mut changed = config.clone();
                changed.display_name = "changed config".into();
                kv.inner
                    .put_serde(
                        file_system_config_key(&config.file_system_id),
                        &changed,
                        None,
                    )
                    .unwrap();
            }
        }
        let mut progressed = job.clone();
        progressed.scanned_object_count = 64;
        progressed.revision += 1;
        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_placement_reconciliation_with_guards(&job, &progressed, compares)
                .unwrap(),
            "{change} must fence stale page progress"
        );
        let durable = DfsControlPlaneStore::new(&mut kv)
            .placement_reconciliation(&config.file_system_id, &job.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(durable, job);
    }
}

#[test]
fn placement_page_authority_unknown_read_and_progress_outcomes_are_not_replayed() {
    let (mut kv, config, shard, job) = page_authority_fixture();
    kv.read_error = true;
    assert!(matches!(
        DfsControlPlaneStore::new(&mut kv).placement_page_authority_compares_before(
            &config,
            std::slice::from_ref(&shard),
            None
        ),
        Err(KvError::Io { .. })
    ));
    assert_eq!(kv.read_keys.len(), 1);
    assert_eq!(kv.txn_calls, 0);
    kv.read_error = false;
    let compares = DfsControlPlaneStore::new(&mut kv)
        .placement_page_authority_compares_before(&config, &[shard], None)
        .unwrap()
        .unwrap();
    kv.lost_progress_reply = true;
    let mut progressed = job.clone();
    progressed.scanned_object_count = 64;
    progressed.revision += 1;
    assert!(matches!(
        DfsControlPlaneStore::new(&mut kv).compare_and_put_placement_reconciliation_with_guards(
            &job,
            &progressed,
            compares
        ),
        Err(KvError::Io { .. })
    ));
    assert_eq!(
        kv.txn_calls, 1,
        "applied progress cannot be replayed after an unknown reply"
    );
    let durable = DfsControlPlaneStore::new(&mut kv)
        .placement_reconciliation(&config.file_system_id, &job.job_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.scanned_object_count, 64);
}

#[test]
fn native_placement_supersession_rejects_changed_classifying_configuration() {
    for asynchronous in [false, true] {
        let name = format!("native-placement-supersession-fence-{asynchronous}");
        let (runtime, nodes) =
            crate::test_support::DfsTestRuntime::start_cluster(&name, 1).unwrap();
        let owner = &nodes[0];
        let path = "/native-placement-supersession-file";
        let mut planner = runtime.client(owner).unwrap();
        planner
            .posix_write_file(
                path,
                b"actual current content",
                crate::DfsWriteOptions::replicated_eventual(nodes.clone()),
            )
            .unwrap();
        let candidates = planner.placement_content_candidates().unwrap();
        let snapshot = planner
            .placement_snapshot_cache
            .snapshot()
            .map(|snapshot| (*snapshot).clone());
        drop(planner);
        let control = runtime.shared_control_plane().unwrap();
        // Retain the existing normal scheduler admission while the ordinary
        // CoreEffect and native KV path publish and race this bounded page.
        let scheduler =
            crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(owner)).unwrap();
        let admission = crate::test_runtime::executor()
            .block_on(scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive))
            .unwrap();
        let mut service = runtime.metadata_primary_service(path).unwrap();
        let (job, accepted, changed) = control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            let current = store
                .get_file_system_config(&crate::sifs_file_system_id())
                .unwrap()
                .unwrap();
            let mut accepted = current.clone();
            crate::upsert_placement_rule(
                &mut accepted.subtree_policies,
                crate::normalize_placement_rule(
                    path,
                    DfsPlacementScope::SelfOnly,
                    crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                        .unwrap(),
                )
                .unwrap(),
            );
            let job = build_placement_reconciliation_job(
                sinomo_core::Id::generate().to_string(),
                &accepted,
                path.into(),
                DfsPlacementScope::SelfOnly,
                BTreeSet::new(),
                candidates.clone(),
                snapshot.clone(),
                crate::metadata_records::dfs_now_unix_nanos(),
            )
            .unwrap();
            assert!(
                store
                    .compare_and_put_placement_update(&current, &accepted, &job)
                    .unwrap()
            );
            let mut changed = accepted.clone();
            crate::upsert_placement_rule(
                &mut changed.subtree_policies,
                crate::normalize_placement_rule(
                    path,
                    DfsPlacementScope::SelfOnly,
                    crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1)
                        .unwrap(),
                )
                .unwrap(),
            );
            assert!(
                store
                    .compare_and_put_file_system_config(Some(&accepted), &changed)
                    .unwrap()
            );
            (job, accepted, changed)
        });
        let keys = vec![
            placement_reconciliation_key(&job.file_system_id, &job.job_id),
            placement_reconciliation_queue_key(&job.file_system_id),
        ];
        let before = control.with_client(|kv| kv.get_authoritative_consistent(&keys).unwrap());
        let restored = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let request_seen = std::sync::Arc::new(std::sync::Mutex::new(None));
        let changed_once = std::sync::Arc::clone(&restored);
        let observed = std::sync::Arc::clone(&request_seen);
        let reverting = control.clone();
        let job_key = keys[0].clone();
        let config_key = file_system_config_key(&job.file_system_id);
        let classify_value = sinomo_core::kv::encode_kv_value(&changed).unwrap();
        let accepted_again = accepted.clone();
        let changed_before_cas = changed.clone();
        let hook = runtime.native_kv_request_hook(owner, move |request| {
            let superseding = request.txn.success.iter().any(|op| {
                let KvOp::Put { key, value, .. } = op else { return false; };
                key == &job_key && sinomo_core::kv::decode_kv_value::<DfsPlacementReconciliationJob>(value)
                    .is_ok_and(|job| job.state == DfsPlacementReconciliationState::Failed
                        && job.last_error.as_deref() == Some(sinomo_api::dfs::placement::PLACEMENT_RECONCILIATION_SUPERSEDED_ERROR))
            });
            if superseding && !changed_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                *observed.lock().unwrap() = Some(request.clone());
                reverting.with_client(|kv| {
                    let mut store = DfsControlPlaneStore::new(kv.as_mut());
                    if !store.compare_and_put_file_system_config(Some(&changed_before_cas), &accepted_again)? {
                        return Err(KvError::Serialization { detail: "actual concurrent configuration changed again".into() });
                    }
                    Ok::<_, KvError>(())
                }).map_err(sinomo_core::service::ServiceError::Kv)?;
            }
            Ok(())
        }).unwrap();
        let result = if asynchronous {
            runtime
                .runtime_handle()
                .block_on(service.run_placement_reconciliation_bounded_async(1))
        } else {
            service.run_placement_reconciliation_bounded(1)
        }
        .unwrap();
        assert!(
            restored.load(std::sync::atomic::Ordering::SeqCst),
            "actual before-CAS configuration race reached"
        );
        assert!(
            result.is_none(),
            "changed classification must not retire the actual job"
        );
        let submitted = request_seen.lock().unwrap().clone().unwrap();
        assert!(submitted.txn.compares.iter().any(|compare| matches!(compare,
            KvCompare::ValueEquals { key, value } if key == &config_key && value == &classify_value)),
            "supersession must compare the same actual configuration used to classify it");
        assert_eq!(
            control.with_client(|kv| kv.get_authoritative_consistent(&keys).unwrap()),
            before,
            "definite config-CAS rejection leaves the whole current job and queue unchanged"
        );
        drop(hook);
        drop(service);
        drop(admission);
        // The same actual job now resumes through the normal Main owner.
        let pass = runtime
            .maintenance_tick(owner, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert!(
            pass.outcome.is_ok(),
            "normal fresh placement owner: {:?}",
            pass.outcome
        );
        let retried = control.with_client(|kv| {
            let rows = kv.get_authoritative_consistent(&keys).unwrap();
            rows[0]
                .as_ref()
                .expect("the same durable placement job remains")
                .decode::<DfsPlacementReconciliationJob>()
                .unwrap()
        });
        assert_eq!(retried.job_id, job.job_id);
        assert_ne!(retried.state, DfsPlacementReconciliationState::Failed);
        assert!(
            retried.revision > job.revision,
            "normal fresh owner advanced the same actual job"
        );
        drop(control);
        runtime.close().unwrap();
    }
}
