//! Converting takeover across native nodes: a closed shard whose primary is
//! lost is converted by the first-ranked member of a promise quorum, whose
//! activation also makes it primary at the next epoch.
//!
//! Node 0 holds cluster KV, so the lost primary is always another node. A
//! node's verdict of its peers is set through the takeover's test verdicts.
//! Each candidate's conversion is stepped by the test, as its unit runs when
//! woken; members answer promises and stage images on their own services.
//! Only the target shard is closed; the others serve, so contributions come
//! from their current tables.
use super::*;
use crate::authority::owner::shard_conversion::takeover::promise::{
    PeerReach, PromiseState, TakeoverRank, rank_takeover_candidates, verdicts_for_test,
};

fn owner_of(cluster: &Cluster, index: usize) -> Arc<MigrationOwner> {
    canonical_migration_runtime(
        &cluster
            .native
            .component_context(&cluster.nodes[index])
            .unwrap(),
    )
}

/// `observers` judge node `lost` as `reach`.
fn judge(cluster: &Cluster, observers: &[usize], lost: usize, reach: PeerReach) {
    for observer in observers {
        verdicts_for_test::set(
            &owner_of(cluster, *observer).authority.data_root,
            &cluster.nodes[lost],
            reach,
        );
    }
}

/// Switch `shard` to the write-set protocol over the members at `members`.
fn switch_write_set_to(cluster: &Cluster, shard: &str, members: &[usize]) {
    for node in &cluster.nodes {
        cluster
            .native
            .advertise_metadata_write_set_capability(node)
            .unwrap();
    }
    let mut control = cluster.native.configuration_publisher(&cluster.nodes[0]);
    let mut store = sinomo_dfs::DfsControlPlaneStore::new(control.as_mut());
    let cut = store
        .read_metadata_shard_record_cut(&cluster.fs, shard, &cluster.nodes)
        .unwrap()
        .unwrap();
    let members = members
        .iter()
        .map(|index| cluster.nodes[*index].clone())
        .collect();
    let switch = sinomo_dfs::build_metadata_write_set_switch(&cut, &members).unwrap();
    assert_eq!(
        store.submit_metadata_write_set_change(&switch),
        sinomo_dfs::DfsMetadataWriteSetTxnOutcome::Applied
    );
}

fn stop(cluster: &mut Cluster, index: usize) {
    let node = cluster.nodes[index].clone();
    cluster.native.stop_node(&node).unwrap();
}

/// Reopen node `index` with ordinary opens of the held shards still held.
fn reopen(cluster: &mut Cluster, index: usize) {
    let failure = cluster
        .native
        .reopen_node_with_startup_action(
            cluster.nodes[index].clone(),
            Box::new(|_| Err(HELD.into())),
        )
        .unwrap_err();
    assert!(failure.contains(HELD), "{failure}");
}

/// A conversion environment that records every decided submission and can
/// halt before an activation is submitted, as a node that dies there.
struct Observed<'a, C: ContributionSource> {
    inner: OwnerConversionEnv<'a, C>,
    halt_before_activation: bool,
    halted: std::cell::Cell<bool>,
    submitted: RefCell<Vec<(KvTxnRequest, Submitted)>>,
}

impl<'a, C: ContributionSource> Observed<'a, C> {
    fn new(inner: OwnerConversionEnv<'a, C>, halt_before_activation: bool) -> Self {
        Self {
            inner,
            halt_before_activation,
            halted: Default::default(),
            submitted: Default::default(),
        }
    }
}

impl<C: ContributionSource> ShardConversionEnv for Observed<'_, C> {
    type Source = C;
    fn local_node(&self) -> Result<NodeId> {
        self.inner.local_node()
    }
    fn data_root(&self) -> &Path {
        self.inner.data_root()
    }
    fn read_exact(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>> {
        self.inner.read_exact(keys)
    }
    fn catalog(&self, file_system_id: &DfsFileSystemId) -> Result<Vec<DfsMetadataShardConfig>> {
        self.inner.catalog(file_system_id)
    }
    fn submit(&self, request: &KvTxnRequest) -> Result<Submitted> {
        if self.halt_before_activation && activates(request) {
            self.halted.set(true);
            return Err(MetadataBackendError::Serde(
                "the converter stops before its activation".into(),
            ));
        }
        let outcome = self.inner.submit(request)?;
        self.submitted.borrow_mut().push((request.clone(), outcome));
        Ok(outcome)
    }
    fn local_copy_current(&self, shard: &DfsMetadataShardConfig) -> Result<bool> {
        self.inner.local_copy_current(shard)
    }
    fn capture(&self, shard: &DfsMetadataShardConfig) -> Result<CaptureProgress> {
        self.inner.capture(shard)
    }
    fn contribution_source(&self) -> &C {
        self.inner.contribution_source()
    }
    fn block_on<F: std::future::Future>(&self, future: F) -> Result<F::Output> {
        self.inner.block_on(future)
    }
    fn install(
        &self,
        node: &NodeId,
        shard: &DfsMetadataShardConfig,
        attempt: &DfsMetadataShardConversion,
        image: &Path,
        source: &ConversionSourceCut,
    ) -> Result<InstallReply> {
        self.inner.install(node, shard, attempt, image, source)
    }
    fn activate_local(
        &self,
        shard: &DfsMetadataShardConfig,
        record: &DfsMetadataShardStorage,
        image: &Path,
    ) -> Result<()> {
        self.inner.activate_local(shard, record, image)
    }
    fn watch_keys(&self, keys: BTreeSet<String>) -> Result<()> {
        self.inner.watch_keys(keys)
    }
    fn watch_nodes(&self, nodes: BTreeSet<NodeId>) -> Result<()> {
        self.inner.watch_nodes(nodes)
    }
    fn takeover(
        &self,
    ) -> Option<&dyn crate::authority::owner::shard_conversion::takeover::TakeoverEnv> {
        self.inner.takeover()
    }
}

/// Run the primary's conversion of `shard` until it would submit its
/// activation: its attempt is recorded, sealed and installed.
fn convert_until_activation(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str) {
    let unit = metadata_unit(owner, shard);
    let env = Observed::new(
        OwnerConversionEnv::new(
            owner,
            &unit,
            NativeContributionSource::new(Arc::clone(&owner.authority)),
        ),
        true,
    );
    let deadline = Instant::now() + Duration::from_secs(180);
    while !env.halted.get() {
        match advance_shard_conversion(&env, fs, shard) {
            _ if env.halted.get() => {}
            Ok(ConversionProgress::Pending { .. }) => {}
            Ok(progress) => {
                panic!("{shard} conversion stopped before its activation: {progress:?}")
            }
            Err(failure) if retryable(&failure) => std::thread::sleep(Duration::from_millis(20)),
            Err(failure) => panic!("{shard} conversion step failed: {failure}"),
        }
        assert!(
            Instant::now() < deadline,
            "{shard} conversion never reached its activation"
        );
    }
}

/// One settled step of `shard`'s conversion on node `index`, with what it
/// submitted.
fn step_on(
    cluster: &Cluster,
    index: usize,
    shard: &str,
) -> (ConversionProgress, Vec<(KvTxnRequest, Submitted)>) {
    let owner = owner_of(cluster, index);
    let unit = metadata_unit(&owner, shard);
    let env = Observed::new(
        OwnerConversionEnv::new(
            &owner,
            &unit,
            NativeContributionSource::new(Arc::clone(&owner.authority)),
        ),
        false,
    );
    let progress = settle(&env, &cluster.fs, shard);
    (progress, env.submitted.into_inner())
}

/// Step every candidate in turn, as each one's unit runs when a promise
/// request, a verdict or the shard's keys wake it, until one is Ready.
fn converge(
    cluster: &Cluster,
    candidates: &[usize],
    shard: &str,
) -> (usize, Vec<(usize, KvTxnRequest, Submitted)>) {
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut submitted = Vec::new();
    loop {
        for index in candidates {
            let (progress, decided) = step_on(cluster, *index, shard);
            submitted.extend(
                decided
                    .into_iter()
                    .map(|(request, outcome)| (*index, request, outcome)),
            );
            if progress == ConversionProgress::Ready {
                return (*index, submitted);
            }
        }
        assert!(Instant::now() < deadline, "no candidate converted {shard}");
    }
}

/// The takeover attempts `submitted` recorded Converting, by the node that
/// submitted each, and the record each one replaced.
fn recorded_takeovers(
    submitted: &[(usize, KvTxnRequest, Submitted)],
) -> Vec<(
    usize,
    DfsMetadataShardConversion,
    Option<DfsMetadataShardStorage>,
)> {
    submitted
        .iter()
        .filter(|(_, _, outcome)| *outcome == Submitted::Committed)
        .filter_map(|(index, request, _)| {
            let written = request.txn.success.iter().find_map(|op| match op {
                KvOp::Put { key, value, .. } if key.contains("/metadata-storage/") => {
                    sinomo_core::kv::decode_kv_value::<DfsMetadataShardStorage>(value).ok()
                }
                _ => None,
            })?;
            let attempt = written
                .converting_attempt()
                .filter(|attempt| attempt.seal.is_none())?
                .clone();
            attempt.takeover.as_ref()?;
            let replaced = request
                .txn
                .compares
                .iter()
                .find_map(|compare| match compare {
                    KvCompare::ValueEquals { key, value } if key.contains("/metadata-storage/") => {
                        sinomo_core::kv::decode_kv_value::<DfsMetadataShardStorage>(value).ok()
                    }
                    _ => None,
                });
            Some((*index, attempt, replaced))
        })
        .collect()
}

/// The owners of the running nodes at `indices`.
fn owners_at(cluster: &Cluster, indices: &[usize]) -> Vec<Arc<MigrationOwner>> {
    indices
        .iter()
        .map(|index| owner_of(cluster, *index))
        .collect()
}

/// Wait until every running node at `indices` serves the Active generation
/// of `shard` from its own copy.
fn await_activated_at(cluster: &Cluster, indices: &[usize], shard: &str) {
    let record = storage(&owner_of(cluster, indices[0]), &cluster.fs, shard).unwrap();
    let digest = record
        .converted_activation()
        .expect("an activated conversion")
        .1
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(60);
    for index in indices {
        let owner = owner_of(cluster, *index);
        while !header(&owner, shard).is_some_and(|header| {
            header.identity == record.generation
                && matches!(&header.state, sinomo_dfs::DfsMetadataGenerationState::Active { activation_digest, .. }
                    if *activation_digest == digest)
        }) {
            assert!(
                Instant::now() < deadline,
                "node {index} did not activate {shard}: header {:?}, scheduler {}",
                header(&owner, shard),
                owner.diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Wait until no running node at `indices` holds a staged image of
/// `attempt_id`.
fn await_unstaged_at(cluster: &Cluster, indices: &[usize], shard: &str, attempt_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for index in indices {
        let owner = owner_of(cluster, *index);
        while stage_root(&owner, &cluster.fs, shard, attempt_id).exists() {
            assert!(
                Instant::now() < deadline,
                "node {index} kept its staged image of {attempt_id}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn promises(cluster: &Cluster, index: usize, shard: &str) -> Option<PromiseState> {
    owner_of(cluster, index)
        .authority
        .takeover_promises(&cluster.fs, shard)
        .unwrap()
}

/// The first-ranked of `candidates` by the ranks they recorded of their own
/// copies.
fn first_ranked(cluster: &Cluster, candidates: &[usize], shard: &str) -> usize {
    let ranks = candidates
        .iter()
        .map(|index| TakeoverRank {
            node_id: cluster.nodes[*index].clone(),
            source_journal_sequence: promises(cluster, *index, shard)
                .and_then(|state| state.own)
                .expect("every candidate ranked its own copy"),
        })
        .collect();
    let first = rank_takeover_candidates(ranks).remove(0).node_id;
    cluster
        .nodes
        .iter()
        .position(|node| *node == first)
        .unwrap()
}

/// The shard after a takeover by node `converter` from `before`: primary at
/// the next epoch with every configured member kept, its storage Active for
/// the converter's takeover attempt.
fn assert_taken_over(
    cluster: &Cluster,
    shard: &str,
    before: &DfsMetadataShardConfig,
    converter: usize,
) -> (DfsMetadataShardConfig, DfsMetadataShardStorage) {
    let owner = owner_of(cluster, 0);
    let after = shard_config(&owner, &cluster.fs, shard);
    assert_eq!(
        (after.epoch, &after.primary_node_id),
        (before.epoch + 1, &cluster.nodes[converter])
    );
    assert_eq!(
        configured(&after),
        configured(before),
        "the takeover keeps every configured member"
    );
    let record = storage(&owner, &cluster.fs, shard).unwrap();
    let (attempt, _) = record
        .converted_activation()
        .expect("an activated conversion");
    assert_eq!(attempt.epoch, before.epoch);
    assert_eq!(attempt.primary_node_id, cluster.nodes[converter]);
    assert_eq!(
        attempt
            .takeover
            .as_ref()
            .map(|takeover| &takeover.previous_primary),
        Some(&before.primary_node_id)
    );
    (after, record)
}

/// The primary records, seals and installs its own attempt on its write set
/// and dies before activating it. With nobody coordinating, the write-set
/// members judge it lost; the first-ranked of them takes over from its own
/// copy, replacing the lost primary's attempt, installs on the write set
/// without it and activates as primary at the next epoch. The other member's
/// staged image of the old attempt is deleted, and the old primary returns
/// superseded, with nothing to convert and its copy not admitted.
#[test]
fn native_primary_lost_mid_conversion_is_taken_over_by_a_write_set_member() {
    let mut cluster = start(
        "shard-takeover-mid-conversion",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 0, &[1, 2]),
            ("users", 1, &[0, 2]),
        ],
    );
    write_namespace(&cluster);
    switch_write_set_to(&cluster, "users", &[0, 1, 2]);
    hold(&mut cluster, &["users"]);
    let fs = cluster.fs.clone();
    close(&owner_of(&cluster, 0), &fs, "users");
    let before = shard_config(&owner_of(&cluster, 0), &fs, "users");
    convert_until_activation(&owner_of(&cluster, 1), &fs, "users");
    let lost = storage(&owner_of(&cluster, 0), &fs, "users")
        .unwrap()
        .converting_attempt()
        .unwrap()
        .clone();
    assert!(lost.sealed().is_some() && lost.takeover.is_none());
    assert_eq!(lost.primary_node_id, cluster.nodes[1]);
    for member in [0, 2] {
        let stage = stage_root(&owner_of(&cluster, member), &fs, "users", &lost.attempt_id);
        assert!(
            stage.join("staged.json").is_file(),
            "member {member} staged the primary's image"
        );
    }
    stop(&mut cluster, 1);
    judge(&cluster, &[0, 2], 1, PeerReach::NoPath);

    let (converter, submitted) = converge(&cluster, &[0, 2], "users");
    assert_eq!(
        converter,
        first_ranked(&cluster, &[0, 2], "users"),
        "the first-ranked member converts"
    );
    let recorded = recorded_takeovers(&submitted);
    assert_eq!(
        recorded.len(),
        1,
        "one takeover attempt is recorded: {recorded:?}"
    );
    let (by, attempt, replaced) = &recorded[0];
    assert_eq!(
        (*by, &attempt.primary_node_id),
        (converter, &cluster.nodes[converter])
    );
    assert_eq!(
        replaced
            .as_ref()
            .and_then(|record| record.converting_attempt())
            .map(|attempt| &attempt.attempt_id),
        Some(&lost.attempt_id),
        "the takeover replaced the lost primary's attempt"
    );
    let (after, record) = assert_taken_over(&cluster, "users", &before, converter);
    let write_set: sinomo_dfs::DfsMetadataShardWriteSet = entry(
        &owner_of(&cluster, 0),
        sinomo_dfs::metadata_shard_write_set_key(&fs, "users"),
    )
    .unwrap()
    .decode()
    .unwrap();
    write_set.validate_against(&after).unwrap();
    assert_eq!(
        write_set.write_set,
        vec![cluster.nodes[0].clone(), cluster.nodes[2].clone()]
    );
    assert_eq!(
        write_set.last_change.kind,
        sinomo_dfs::DfsMetadataShardWriteSetChangeKind::Takeover {
            previous_primary_node_id: cluster.nodes[1].clone()
        }
    );
    let (activated, _) = record.converted_activation().unwrap();
    assert_eq!(
        write_set.epoch_start.as_ref(),
        activated.seal.as_ref().map(|seal| &seal.prefix_proof)
    );
    await_activated_at(&cluster, &[0, 2], "users");
    await_unstaged_at(&cluster, &[0, 2], "users", &lost.attempt_id);

    // The old primary returns superseded: its unit hands over, it removes
    // what it held of its attempt, and its copy is not admitted.
    reopen(&mut cluster, 1);
    let (returned, decided) = step_on(&cluster, 1, "users");
    assert_eq!(returned, ConversionProgress::Handover);
    assert!(decided.is_empty(), "the old primary submits nothing");
    let old = owner_of(&cluster, 1);
    assert!(!attempt_dir(&old, &fs, "users", &lost.attempt_id).exists());
    assert!(header(&old, "users").is_none_or(|header| !record.admits_header(&header)));
    assert_eq!(
        shard_config(&old, &fs, "users").primary_node_id,
        cluster.nodes[converter]
    );
    drop(old);
    release(&cluster, &owners(&cluster), "users");
    for reader in [0, 2] {
        await_serving(&cluster, reader, &after, "/users/doc");
    }
    cluster.native.close().unwrap();
}

/// A closed shard whose primary is offline before any attempt of its epoch
/// exists, on the all-member rule: the first-ranked member of a majority of
/// the configured members takes over with a fifth member offline too, and
/// the members that hold the image activate it under the new serving record.
#[test]
fn native_closed_unswitched_shard_with_its_primary_offline_is_taken_over_by_a_majority() {
    let mut cluster = start(
        "shard-takeover-offline-primary",
        5,
        &[
            ("root", 0, &[1, 2, 3, 4]),
            ("services", 0, &[1, 2, 3, 4]),
            ("users", 1, &[0, 2, 3, 4]),
        ],
    );
    write_namespace(&cluster);
    hold(&mut cluster, &["users"]);
    let fs = cluster.fs.clone();
    close(&owner_of(&cluster, 0), &fs, "users");
    let before = shard_config(&owner_of(&cluster, 0), &fs, "users");
    let closed = storage(&owner_of(&cluster, 0), &fs, "users").unwrap();
    assert!(
        closed
            .converting_attempt()
            .is_some_and(|attempt| attempt.epoch < before.epoch)
    );
    stop(&mut cluster, 4);
    stop(&mut cluster, 1);
    judge(&cluster, &[0, 2, 3], 1, PeerReach::NoPath);
    judge(&cluster, &[0, 2, 3], 4, PeerReach::NoPath);

    let (converter, submitted) = converge(&cluster, &[0, 2, 3], "users");
    assert_eq!(converter, first_ranked(&cluster, &[0, 2, 3], "users"));
    let recorded = recorded_takeovers(&submitted);
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(
        recorded[0].2.as_ref(),
        Some(&closed),
        "it replaced the superseded attempt"
    );
    let (after, _) = assert_taken_over(&cluster, "users", &before, converter);
    assert!(
        entry(
            &owner_of(&cluster, 0),
            sinomo_dfs::metadata_shard_write_set_key(&fs, "users")
        )
        .is_none(),
        "an unswitched shard stays unswitched"
    );
    // Each member that promised promised the converter.
    for member in [0, 2, 3] {
        assert_eq!(
            promises(&cluster, member, "users").unwrap().promised,
            Some(cluster.nodes[converter].clone())
        );
    }
    await_activated_at(&cluster, &[0, 2, 3], "users");
    release(&cluster, &owners_at(&cluster, &[0, 2, 3]), "users");
    for reader in [0, 2, 3] {
        await_serving(&cluster, reader, &after, "/users/doc");
    }
    cluster.native.close().unwrap();
}

/// Two members judge the lost primary lost at once and race. Exactly one
/// converts; the other's attempt is never recorded, and no recorded attempt
/// replaces another member's at the epoch.
#[test]
fn native_concurrent_takeover_candidates_convert_once_without_replacing_each_other() {
    let mut cluster = start(
        "shard-takeover-concurrent",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 0, &[1, 2]),
            ("users", 1, &[0, 2]),
        ],
    );
    write_namespace(&cluster);
    hold(&mut cluster, &["users"]);
    let fs = cluster.fs.clone();
    close(&owner_of(&cluster, 0), &fs, "users");
    let before = shard_config(&owner_of(&cluster, 0), &fs, "users");
    stop(&mut cluster, 1);
    judge(&cluster, &[0, 2], 1, PeerReach::NoPath);

    let candidates = [0, 2].map(|index| (index, owner_of(&cluster, index)));
    let outcomes = std::thread::scope(|scope| {
        let runs = candidates
            .iter()
            .map(|(index, owner)| {
                let fs = fs.clone();
                scope.spawn(move || {
                    let deadline = Instant::now() + Duration::from_secs(240);
                    let mut submitted = Vec::new();
                    loop {
                        let unit = metadata_unit(owner, "users");
                        let env = Observed::new(
                            OwnerConversionEnv::new(
                                owner,
                                &unit,
                                NativeContributionSource::new(Arc::clone(&owner.authority)),
                            ),
                            false,
                        );
                        let progress = settle(&env, &fs, "users");
                        submitted.extend(
                            env.submitted
                                .into_inner()
                                .into_iter()
                                .map(|(request, outcome)| (*index, request, outcome)),
                        );
                        let active =
                            storage(owner, &fs, "users").is_some_and(|record| record.is_active());
                        if progress == ConversionProgress::Ready
                            || (active && progress == ConversionProgress::Handover)
                        {
                            return (*index, progress, submitted);
                        }
                        assert!(
                            Instant::now() < deadline,
                            "member {index} never settled: {progress:?}"
                        );
                        // As the unit's next wake would.
                        std::thread::sleep(Duration::from_millis(100));
                    }
                })
            })
            .collect::<Vec<_>>();
        runs.into_iter()
            .map(|run| run.join().unwrap())
            .collect::<Vec<_>>()
    });
    drop(candidates);
    let ready = outcomes
        .iter()
        .filter(|(_, progress, _)| *progress == ConversionProgress::Ready)
        .map(|(index, _, _)| *index)
        .collect::<Vec<_>>();
    assert_eq!(ready.len(), 1, "exactly one member converts: {outcomes:?}");
    let converter = ready[0];
    let submitted = outcomes
        .into_iter()
        .flat_map(|(_, _, submitted)| submitted)
        .collect::<Vec<_>>();
    let recorded = recorded_takeovers(&submitted);
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].0, converter);
    for (_, _, replaced) in &recorded {
        assert!(
            replaced
                .as_ref()
                .and_then(|record| record.converting_attempt())
                .is_none_or(|attempt| attempt.takeover.is_none() || attempt.epoch < before.epoch),
            "no takeover attempt at the epoch was replaced: {replaced:?}"
        );
    }
    assert_eq!(converter, first_ranked(&cluster, &[0, 2], "users"));
    assert_taken_over(&cluster, "users", &before, converter);
    cluster.native.close().unwrap();
}

/// A member that still reaches the primary vetoes: the member that judges it
/// lost records no takeover, the vetoing member does not start one, and the
/// primary then converts the shard itself.
#[test]
fn native_a_member_that_reaches_the_primary_vetoes_the_takeover() {
    let mut cluster = start(
        "shard-takeover-veto",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 0, &[1, 2]),
            ("users", 1, &[0, 2]),
        ],
    );
    write_namespace(&cluster);
    hold(&mut cluster, &["users"]);
    let fs = cluster.fs.clone();
    close(&owner_of(&cluster, 0), &fs, "users");
    let closed = storage(&owner_of(&cluster, 0), &fs, "users").unwrap();
    judge(&cluster, &[2], 1, PeerReach::NoPath);
    judge(&cluster, &[0], 1, PeerReach::Path);

    let (vetoed, submitted) = step_on(&cluster, 2, "users");
    assert!(
        matches!(&vetoed, ConversionProgress::Parked { evidence }
            if evidence.contains(&format!("{}: reaches the shard's primary", cluster.nodes[0]))),
        "{vetoed:?}"
    );
    assert!(submitted.is_empty(), "nothing is submitted under a veto");
    assert_eq!(
        storage(&owner_of(&cluster, 0), &fs, "users").as_ref(),
        Some(&closed)
    );
    let vetoing = promises(&cluster, 0, "users").unwrap();
    assert_eq!(vetoing.promised, None);
    assert!(
        vetoing.known.contains_key(&cluster.nodes[2]),
        "the vetoing member learned the candidate"
    );
    let (idle, submitted) = step_on(&cluster, 0, "users");
    assert!(
        matches!(&idle, ConversionProgress::Parked { evidence } if evidence.contains("does not judge it lost")),
        "{idle:?}"
    );
    assert!(submitted.is_empty());

    // The primary's own conversion is untouched by the vetoed attempt.
    assert_eq!(
        drive(&owner_of(&cluster, 1), &fs, "users"),
        ConversionProgress::Ready
    );
    let record = storage(&owner_of(&cluster, 0), &fs, "users").unwrap();
    let (attempt, _) = record.converted_activation().unwrap();
    assert!(attempt.takeover.is_none());
    assert_eq!(attempt.primary_node_id, cluster.nodes[1]);
    cluster.native.close().unwrap();
}
