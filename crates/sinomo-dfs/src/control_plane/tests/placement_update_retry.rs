use super::*;

#[derive(Clone, Copy, Debug)]
enum Injection {
    Bookkeeping { remaining: usize },
    Configuration,
    SameJob,
    LivePermit,
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
            Injection::LivePermit if self.publication_attempts == 1 => {
                let record = placement_authority_operator_test_record(
                    None,
                    Some(self.expected.owner_node_id.clone()),
                    None,
                );
                self.inner.put_serde(
                    placement_authority_permit_key(&self.expected.file_system_id),
                    &record,
                    None,
                )?;
            }
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
            Some(scoped_placement_config_for_job(&desired, &kv.job))
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

#[test]
fn placement_publication_stops_on_live_permit_after_definite_cas_rejection() {
    for asynchronous in [false, true] {
        let (mut kv, desired) = RacingPlacementKv::new(Injection::LivePermit);
        assert!(matches!(
            kv.apply(&desired, asynchronous),
            Err(KvError::Busy { .. })
        ));
        assert_eq!(kv.publication_attempts, 1);
        assert_eq!(kv.authoritative_refreshes, 1);
        assert!(
            kv.inner
                .get(&placement_reconciliation_key(
                    &desired.file_system_id,
                    &kv.job.job_id
                ))
                .is_none()
        );
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
fn missing_placement_queue_bounds_active_jobs_without_capping_terminal_history() {
    let (kv, desired) = RacingPlacementKv::new(Injection::Bookkeeping { remaining: 0 });
    let publication = PlacementUpdatePublication::new(&kv.expected, &desired, &kv.job).unwrap();
    let entry = |index: usize, terminal: bool| {
        let mut job = kv.job.clone();
        job.job_id = format!("legacy-queue-job-{index}");
        job.path = format!("/background/{index}");
        job.accepted_config = Some(scoped_placement_config_for_job(&kv.expected, &job));
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
        2,
        "terminal history does not consume active queue capacity"
    );
    let active = (0..MAX_ACTIVE_PLACEMENT_RECONCILIATIONS + 1)
        .map(|index| entry(index, false))
        .collect();
    assert!(matches!(
        publication.transaction(inputs(), active, permit()),
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
    let owner = node("page-proof-owner");
    let config = placement_authority_test_config(&owner, 2);
    let shard = DfsMetadataShardConfig {
        file_system_id: config.file_system_id.clone(),
        shard_id: "root".into(),
        primary_node_id: owner.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let job = placement_authority_test_job("page-proof-job", &config.file_system_id, &owner, 1);
    let mut inner = InProcessKvStore::new();
    let committed = {
        let mut store = DfsControlPlaneStore::new(&mut inner);
        store.put_file_system_config(&config).unwrap();
        store.put_metadata_shard(&shard).unwrap();
        assert!(
            store
                .compare_and_put_placement_update(&config, &config, &job)
                .unwrap()
        );
        store
            .placement_reconciliation(&config.file_system_id, &job.job_id)
            .unwrap()
            .unwrap()
    };
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
