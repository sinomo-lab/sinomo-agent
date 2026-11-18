//! Per-shard conversion across three native nodes: shards convert one at a
//! time while converted shards serve, and a serving change during an
//! attempt supersedes it.
//!
//! The fixture's copies are current-format copies born native genesis,
//! standing in for old storage. A shard to convert is closed the way an
//! interrupted earlier conversion leaves it: its storage record names a
//! Converting attempt of an earlier serving record, which the shard's
//! primary replaces with its own attempt. Ordinary opens of a shard are held
//! while it converts, so only migration reads its copies; the hold is
//! released once its storage is Active. The test drives each primary's
//! conversion step by step; members stage and activate on their own owners.
//!
//! A closed contribution source answers from the capture its own conversion
//! took. Each test therefore runs every closed shard's first steps (capture,
//! then Converting) before any shard can finish reading contributions: a
//! driver that reads a source not captured yet parks until it is. From then
//! on a converting shard reads each closed source from its capture and each
//! converted source from its current tables.
//!
//! Every node holds a copy of every shard. The shard-by-shard test checks
//! that each converted shard serves its members' reads and accepts writes
//! while the others are closed, and that every shard accepts writes once
//! none is. The takeover
//! test keeps every shard closed and reads contributions only through
//! migration routes.
use super::conversion_receiver::conversion_stage_root;
use super::shard_conversion::{
    CaptureProgress, ConversionProgress, InstallReply, OwnerConversionEnv, ShardConversionEnv,
    Submitted, advance_shard_conversion,
};
use super::*;
use crate::authority::contribution_source::ContributionSource;
use crate::authority::contribution_sources::NativeContributionSource;
use crate::authority::native_fixtures::canonical_migration_runtime;
use crate::authority::staged_service::conversion_install::ConversionSourceCut;
use sinomo_core::node::NodeId;
use sinomo_dfs::test_support::DfsTestRuntime;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataShardConversion, DfsMetadataShardConversionInstallRule,
    DfsMetadataShardStorage,
};
use std::cell::RefCell;
use std::time::{Duration, Instant};

mod alias_history;
mod closed_fences;
mod closed_pair;
mod member_restart;
mod moves;
mod passive;
mod record_less;
mod status;
mod takeover;

const HELD: &str = "ordinary opens held while shards convert";

struct Cluster {
    native: DfsTestRuntime,
    nodes: Vec<NodeId>,
    held: Arc<Mutex<BTreeSet<String>>>,
    fs: DfsFileSystemId,
}

/// `count` nodes; each assignment names a shard, its primary and its other
/// members by node index.
fn start(name: &str, count: usize, assignments: &[(&str, usize, &[usize])]) -> Cluster {
    let nodes = DfsTestRuntime::ordered_node_ids(name, count);
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    let held = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let factory_held = Arc::clone(&held);
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    move |context, scope, lifecycle| {
                        let binding = storage_preparation_provider(context, scope, lifecycle.clone())?;
                        let provider = binding.provider;
                        let held = Arc::clone(&factory_held);
                        *crate::authority::shared_owner(context, lifecycle)?
                            .held_conversions
                            .lock()
                            .unwrap() = Some(Arc::clone(&held));
                        Ok(sinomo_dfs::DfsStoragePreparationProviderBinding {
                            readiness: binding.readiness,
                            provider: Arc::new(move |root, unit, budget| {
                                let shard = match unit {
                                    DfsStoragePreparationScope::Metadata(shard) => Some(&shard.0),
                                    DfsStoragePreparationScope::MetadataGeneration { identity } => {
                                        Some(&identity.shard_id.0)
                                    }
                                    _ => None,
                                };
                                if shard.is_some_and(|shard| held.lock().unwrap().contains(shard)) {
                                    return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                                        processed_records: 0,
                                    });
                                }
                                provider(root, unit, budget)
                            }),
                        })
                    },
                ))
                .unwrap(),
            ),
            Arc::new(staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    let fs = native.file_system().file_system_id;
    for (shard, primary, replicas) in assignments {
        let replicas = replicas.iter().map(|index| nodes[*index].clone()).collect();
        native
            .reassign_metadata_shard(&fs, shard, nodes[*primary].clone(), replicas)
            .unwrap();
    }
    Cluster { native, nodes, held, fs }
}

fn options(cluster: &Cluster) -> sinomo_dfs::DfsWriteOptions {
    sinomo_dfs::DfsWriteOptions::replicated_eventual(cluster.nodes.clone())
}

/// A users file with a services alias, a services file with a local alias,
/// and a directory in each.
fn write_namespace(cluster: &Cluster) {
    let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
    client.posix_write_file("/users/doc", b"users bytes", options(cluster)).unwrap();
    client.posix_write_file("/services/file", b"services bytes", options(cluster)).unwrap();
    client.posix_hardlink("/services/file", "/services/alias").unwrap();
    client.posix_hardlink("/users/doc", "/services/doc-alias").unwrap();
    client.posix_mkdir("/services/dir").unwrap();
    client.posix_mkdir("/users/sub").unwrap();
}

/// Restart every node with ordinary opens of `shards` held, and with each
/// node's own conversion units of `shards` waiting, so that the test alone
/// steps their conversion until it releases them.
fn hold(cluster: &mut Cluster, shards: &[&str]) {
    for node in cluster.nodes.clone().iter().rev() {
        cluster.native.stop_node(node).unwrap();
    }
    cluster.held.lock().unwrap().extend(shards.iter().map(|shard| shard.to_string()));
    for node in cluster.nodes.clone() {
        let failure = cluster
            .native
            .reopen_node_with_startup_action(node, Box::new(|_| Err(HELD.into())))
            .unwrap_err();
        assert!(failure.contains(HELD), "{failure}");
    }
}

fn release(cluster: &Cluster, owners: &[Arc<MigrationOwner>], shard: &str) {
    cluster.held.lock().unwrap().remove(shard);
    for owner in owners {
        owner.work.notify();
    }
}

fn owners(cluster: &Cluster) -> Vec<Arc<MigrationOwner>> {
    cluster
        .nodes
        .iter()
        .map(|node| canonical_migration_runtime(&cluster.native.component_context(node).unwrap()))
        .collect()
}

fn shard_config(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str) -> DfsMetadataShardConfig {
    owner
        .authority
        .assignments(std::slice::from_ref(fs))
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == shard)
        .unwrap()
}

fn entry(owner: &MigrationOwner, key: String) -> Option<KvEntry> {
    let (rows, _) = owner.authority.read_exact(&[key]).unwrap();
    rows.into_iter().next().unwrap().entry
}

fn storage(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str) -> Option<DfsMetadataShardStorage> {
    entry(owner, sinomo_dfs::metadata_shard_storage_key(fs, shard)).map(|entry| entry.decode().unwrap())
}

/// Close `shard` as an interrupted earlier conversion leaves it: its storage
/// record names an unsealed attempt of the previous epoch, or, at the first
/// epoch, of another member.
fn close(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str) {
    let key = sinomo_dfs::metadata_shard_storage_key(fs, shard);
    let before = entry(owner, key.clone()).expect("a born shard carries a storage record");
    let config = shard_config(owner, fs, shard);
    let (epoch, converter) = match config.epoch.checked_sub(1) {
        Some(epoch) => (epoch, config.primary_node_id.clone()),
        None => (config.epoch, config.replica_node_ids[0].clone()),
    };
    let source = sinomo_dfs::DfsMetadataConversionSource {
        source_identity: "an-earlier-attempt".into(),
        source_format: None,
        source_journal_sequence: 1,
        source_state_fingerprint: "an-earlier-state".into(),
        source_cut_digest: "a".repeat(64),
        pending_mapping_digest: "b".repeat(64),
    };
    let attempt_id = sinomo_dfs::metadata_shard_conversion_attempt_id(
        fs,
        shard,
        &config.assignment,
        epoch,
        &converter,
        &source,
        None,
    )
    .unwrap();
    let record = DfsMetadataShardStorage::converting(
        &config,
        DfsMetadataShardConversion {
            attempt_id,
            epoch,
            primary_node_id: converter,
            takeover: None,
            source,
            seal: None,
            contributions_digest: None,
        },
    );
    record.validate_against(&config).unwrap();
    let committed = owner
        .authority
        .commit(KvTxnRequest::new(
            KvRequestId::generate(),
            KvTxn {
                compares: vec![KvCompare::ValueEquals { key: key.clone(), value: before.value }],
                success: vec![KvOp::Put {
                    key,
                    value: sinomo_core::kv::encode_kv_value(&record).unwrap(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        ))
        .unwrap();
    NativeImportAuthority::require_committed(&committed).unwrap();
}

fn metadata_unit(owner: &MigrationOwner, shard: &str) -> inventory::UnitKey {
    let unit = inventory::UnitKey {
        relative_root: Path::new("dfs/metadata").join(shard),
        role: inventory::UnitRole::Metadata { shard: sinomo_dfs::DfsMetadataShardId::new(shard) },
    };
    owner
        .work
        .scheduler
        .lock()
        .unwrap()
        .recovered(inventory::UnitReference::discovered(unit.clone()))
        .unwrap();
    unit
}

fn retryable(failure: &MetadataBackendError) -> bool {
    matches!(failure, MetadataBackendError::StoragePreparationPending { .. })
        || matches!(failure, MetadataBackendError::Io { source, .. }
            if matches!(source.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted))
}

/// Step `shard`'s conversion until it stops advancing on its own. A
/// retryable step error is retried, as the owner's bounded retry does.
fn settle<E: ShardConversionEnv>(env: &E, fs: &DfsFileSystemId, shard: &str) -> ConversionProgress {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        match advance_shard_conversion(env, fs, shard) {
            Ok(ConversionProgress::Pending { .. }) => {}
            Ok(progress) => return progress,
            Err(failure) if retryable(&failure) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(failure) => panic!("{shard} conversion step failed: {failure}"),
        }
        assert!(Instant::now() < deadline, "{shard} conversion kept advancing without settling");
    }
}

fn drive(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str) -> ConversionProgress {
    let unit = metadata_unit(owner, shard);
    let source = NativeContributionSource::new(Arc::clone(&owner.authority));
    settle(&OwnerConversionEnv::new(owner, &unit, source), fs, shard)
}

fn header(owner: &MigrationOwner, shard: &str) -> Option<sinomo_dfs::DfsMetadataGenerationHeader> {
    sinomo_dfs::inspect_metadata_storage_generation(&owner.metadata_root.join(shard)).ok()
}

/// Wait until every node in `members` serves the record's Active generation
/// of `shard` from its own copy.
fn await_activated(
    cluster: &Cluster,
    owners: &[Arc<MigrationOwner>],
    shard: &str,
    members: &BTreeSet<NodeId>,
) -> DfsMetadataShardStorage {
    let record = storage(&owners[0], &cluster.fs, shard).unwrap();
    let digest = record.converted_activation().expect("an activated conversion").1.to_owned();
    let deadline = Instant::now() + Duration::from_secs(60);
    for (owner, node) in owners.iter().zip(&cluster.nodes).filter(|(_, node)| members.contains(*node)) {
        while !header(owner, shard).is_some_and(|header| {
            header.identity == record.generation
                && matches!(&header.state, sinomo_dfs::DfsMetadataGenerationState::Active { activation_digest, .. }
                    if *activation_digest == digest)
        }) {
            assert!(
                Instant::now() < deadline,
                "{node} did not activate {shard}: header {:?}, scheduler {}",
                header(owner, shard),
                owner.diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    record
}

fn configured(config: &DfsMetadataShardConfig) -> BTreeSet<NodeId> {
    std::iter::once(config.primary_node_id.clone()).chain(config.replica_node_ids.iter().cloned()).collect()
}

/// Wait until `node` serves `path` of the shard that owns it.
fn await_serving(cluster: &Cluster, node: usize, config: &DfsMetadataShardConfig, path: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut client = cluster.native.client(&cluster.nodes[node]).unwrap();
    loop {
        let read = client.query_metadata_authority(
            config,
            sinomo_dfs::DfsMetadataSelection::StatAtPathV2 { path: path.into() },
        );
        if matches!(read, Ok(sinomo_dfs::DfsMetadataQueryResult::StatAtPathV2 { object: Some(_) })) {
            return;
        }
        assert!(Instant::now() < deadline, "{} does not serve {path}: {read:?}", config.shard_id);
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Write `path` from `node` and read it back. Right after a shard activates,
/// its primary's own preparation still reports the old copy pending until the
/// shard's migration unit, woken by the storage record, re-advances; a write
/// in that short window is refused or only possibly applied, so the write is
/// repeated (it is idempotent) until it lands.
fn write_beside_closed(cluster: &Cluster, node: usize, path: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut client = cluster.native.client(&cluster.nodes[node]).unwrap();
    loop {
        let written = client
            .posix_write_file(path, path.as_bytes(), options(cluster))
            .and_then(|_| client.posix_read_file_distributed(path));
        match written {
            Ok(bytes) => {
                assert_eq!(bytes, path.as_bytes());
                return;
            }
            Err(error) => assert!(Instant::now() < deadline, "{path} is not written: {error:?}"),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn stage_root(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str, attempt_id: &str) -> PathBuf {
    conversion_stage_root(&owner.authority.receiver_root, fs, shard, attempt_id).unwrap()
}

fn attempt_dir(owner: &MigrationOwner, fs: &DfsFileSystemId, shard: &str, attempt_id: &str) -> PathBuf {
    owner
        .authority
        .data_root
        .join("dfs/migration-owner/shards")
        .join(fs.to_string())
        .join(shard)
        .join(attempt_id)
}

/// Every member's staged image of `attempt_id` is gone once its unit acted.
fn await_unstaged(cluster: &Cluster, owners: &[Arc<MigrationOwner>], shard: &str, attempt_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for (owner, node) in owners.iter().zip(&cluster.nodes) {
        let stage = stage_root(owner, &cluster.fs, shard, attempt_id);
        while stage.exists() {
            assert!(
                Instant::now() < deadline,
                "{node} kept its staged image of {shard} attempt {attempt_id}: {}",
                owner.diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Shards convert one at a time, each installed on and activated by all its
/// members. A converted shard serves while the others are still closed, no
/// conversion writes a filesystem key, and every shard accepts writes once
/// all are converted.
#[test]
fn native_shards_convert_one_at_a_time_while_converted_shards_serve() {
    let mut cluster = start(
        "shard-conversion-one-at-a-time",
        3,
        &[("root", 0, &[1, 2]), ("services", 1, &[0, 2]), ("users", 2, &[0, 1])],
    );
    write_namespace(&cluster);
    hold(&mut cluster, &["root", "services", "users"]);
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    let filesystem = entry(&owners[0], sinomo_dfs::file_system_config_key(&fs)).unwrap();
    for shard in ["root", "services", "users"] {
        close(&owners[0], &fs, shard);
    }
    // Services and users record their own attempts, then wait for sources
    // that have not captured their copies yet.
    for (shard, primary) in [("services", 1), ("users", 2)] {
        let parked = drive(&owners[primary], &fs, shard);
        assert!(
            matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("no member that can answer")),
            "{shard}: {parked:?}"
        );
        let attempt = storage(&owners[0], &fs, shard).unwrap().converting_attempt().unwrap().clone();
        assert_eq!(
            (attempt.epoch, &attempt.primary_node_id),
            (shard_config(&owners[0], &fs, shard).epoch, &cluster.nodes[primary])
        );
    }
    let mut converted = Vec::new();
    for (shard, primary, path) in [
        ("root", 0, "/"),
        ("users", 2, "/users/doc"),
        ("services", 1, "/services/file"),
    ] {
        assert_eq!(drive(&owners[primary], &fs, shard), ConversionProgress::Ready, "{shard}");
        let config = shard_config(&owners[0], &fs, shard);
        let members = configured(&config);
        let record = await_activated(&cluster, &owners, shard, &members);
        let attempt = record.converted_activation().unwrap().0.clone();
        assert_eq!(attempt.primary_node_id, cluster.nodes[primary]);
        await_unstaged(&cluster, &owners, shard, &attempt.attempt_id);
        converted.push(shard);
        release(&cluster, &owners, shard);
        for other in ["root", "services", "users"].iter().filter(|other| !converted.contains(*other)) {
            assert!(
                storage(&owners[0], &fs, other).is_some_and(|record| !record.is_active()),
                "{other} stays closed while {shard} serves"
            );
        }
        // The converted shard serves its members while the others are closed,
        // and accepts writes from a node that still holds the closed copies.
        for reader in 0..cluster.nodes.len() {
            await_serving(&cluster, reader, &config, path);
        }
        let written = match shard {
            "root" => "/written-beside-closed".to_string(),
            other => format!("/{other}/written-beside-closed"),
        };
        write_beside_closed(&cluster, primary, &written);
        // No conversion writes the filesystem configuration or bumps the
        // catalog generation.
        let now = entry(&owners[0], sinomo_dfs::file_system_config_key(&fs)).unwrap();
        assert_eq!((&now.value, now.modify_revision), (&filesystem.value, filesystem.modify_revision));
    }
    cluster.native.wait_startup_completion().unwrap();
    {
        let mut client = cluster.native.client(&cluster.nodes[1]).unwrap();
        client.posix_mkdir("/users/after-conversion").unwrap();
        client.posix_hardlink("/users/doc", "/users/doc-link").unwrap();
        client.posix_mkdir("/after-conversion").unwrap();
    }
    for node in 0..cluster.nodes.len() {
        let mut client = cluster.native.client(&cluster.nodes[node]).unwrap();
        assert_eq!(client.posix_read_file_distributed("/services/doc-alias").unwrap(), b"users bytes");
        assert_eq!(client.posix_stat_distributed("/users/doc").unwrap().link_count, 3);
        assert_eq!(client.posix_stat_distributed("/services/file").unwrap().link_count, 2);
        assert_eq!(client.posix_stat_distributed("/users").unwrap().link_count, 4);
        for shard_path in ["/root-file", "/users/file", "/services/new-file"] {
            let path = format!("{shard_path}-{node}");
            client.posix_write_file(&path, path.as_bytes(), options(&cluster)).unwrap();
            assert_eq!(client.posix_read_file_distributed(&path).unwrap(), path.as_bytes());
        }
    }
    drop(owners);
    cluster.native.close().unwrap();
}

/// The converting primary's environment, with a hook that runs once just
/// before its activation is submitted.
struct BeforeActivation<'a, C: ContributionSource> {
    inner: OwnerConversionEnv<'a, C>,
    before_activation: RefCell<Option<Box<dyn FnOnce() + 'a>>>,
    activations: RefCell<Vec<(KvTxnRequest, Submitted)>>,
}

fn activates(request: &KvTxnRequest) -> bool {
    request.txn.success.iter().any(|op| match op {
        KvOp::Put { key, value, .. } if key.contains("/metadata-storage/") => {
            sinomo_core::kv::decode_kv_value::<DfsMetadataShardStorage>(value).is_ok_and(|record| record.is_active())
        }
        _ => false,
    })
}

impl<C: ContributionSource> ShardConversionEnv for BeforeActivation<'_, C> {
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
        if !activates(request) {
            return self.inner.submit(request);
        }
        if let Some(before_activation) = self.before_activation.borrow_mut().take() {
            before_activation();
        }
        let outcome = self.inner.submit(request)?;
        self.activations.borrow_mut().push((request.clone(), outcome));
        Ok(outcome)
    }
    fn capture(&self, shard: &DfsMetadataShardConfig) -> Result<CaptureProgress> {
        self.inner.capture(shard)
    }
    fn placement_evidence(
        &self,
        shard: &DfsMetadataShardConfig,
    ) -> Result<Option<Vec<crate::metadata::RetainedPlacementReservation>>> {
        self.inner.placement_evidence(shard)
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
    fn activate_local(&self, shard: &DfsMetadataShardConfig, record: &DfsMetadataShardStorage, image: &Path) -> Result<()> {
        self.inner.activate_local(shard, record, image)
    }
    fn watch_keys(&self, keys: BTreeSet<String>) -> Result<()> {
        self.inner.watch_keys(keys)
    }
    fn watch_nodes(&self, nodes: BTreeSet<NodeId>) -> Result<()> {
        self.inner.watch_nodes(nodes)
    }
}

/// Switch `shard` to the write-set protocol over all its members.
fn switch_write_set(cluster: &Cluster, shard: &str) {
    for node in &cluster.nodes {
        cluster.native.advertise_metadata_write_set_capability(node).unwrap();
    }
    let mut control = cluster.native.configuration_publisher(&cluster.nodes[0]);
    let mut store = sinomo_dfs::DfsControlPlaneStore::new(control.as_mut());
    let cut = store.read_metadata_shard_record_cut(&cluster.fs, shard, &cluster.nodes).unwrap().unwrap();
    let members = configured(&cut.shard);
    let switch = sinomo_dfs::build_metadata_write_set_switch(&cut, &members).unwrap();
    assert_eq!(
        store.submit_metadata_write_set_change(&switch),
        sinomo_dfs::DfsMetadataWriteSetTxnOutcome::Applied
    );
}

/// Take `shard` over by `candidate` through the write-set takeover swap; the
/// candidate's copy's head starts the new epoch.
fn take_over(cluster: &Cluster, shard: &str, candidate: &NodeId, candidate_root: &Path) -> DfsMetadataShardConfig {
    use sinomo_dfs::DfsMetadataBackend as _;
    let mut control = cluster.native.configuration_publisher(candidate);
    let mut store = sinomo_dfs::DfsControlPlaneStore::new(control.as_mut());
    let cut = store.read_metadata_shard_record_cut(&cluster.fs, shard, &cluster.nodes).unwrap().unwrap();
    let id = sinomo_dfs::DfsMetadataShardId::new(shard);
    let epoch_start = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(id.clone(), candidate_root)
        .unwrap()
        .metadata_prefix_proof(&id)
        .unwrap()
        .expect("the candidate's copy has a head");
    let swap =
        sinomo_dfs::build_metadata_takeover_swap(&cut, candidate, &epoch_start, &KvRequestId::generate()).unwrap();
    assert_eq!(
        store.submit_metadata_write_set_change(&swap),
        sinomo_dfs::DfsMetadataWriteSetTxnOutcome::Applied
    );
    store.read_metadata_shard_record_cut(&cluster.fs, shard, &cluster.nodes).unwrap().unwrap().shard
}

/// A takeover moves the epoch after the old primary installed its image and
/// before its activation commits. That activation is rejected, the new
/// primary converts under a new attempt and activates it, and every node
/// removes what it held of the superseded attempt.
#[test]
fn native_takeover_during_a_conversion_supersedes_its_attempt_on_every_node() {
    let mut cluster = start(
        "shard-conversion-takeover",
        3,
        &[("root", 0, &[1, 2]), ("services", 1, &[0, 2]), ("users", 0, &[1, 2])],
    );
    write_namespace(&cluster);
    switch_write_set(&cluster, "users");
    hold(&mut cluster, &["root", "services", "users"]);
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    for shard in ["root", "services", "users"] {
        close(&owners[0], &fs, shard);
    }
    // Root and services stay closed; users reads them from their captures.
    for (shard, primary) in [("root", 0), ("services", 1)] {
        let parked = drive(&owners[primary], &fs, shard);
        assert!(matches!(parked, ConversionProgress::Parked { .. }), "{shard}: {parked:?}");
    }
    let before = shard_config(&owners[0], &fs, "users");
    assert_eq!(before.primary_node_id, cluster.nodes[0]);

    // The old primary converts, installs on the other members, and is taken
    // over just before its activation commits.
    let stale = RefCell::new(None::<DfsMetadataShardConversion>);
    let unit = metadata_unit(&owners[0], "users");
    let env = BeforeActivation {
        inner: OwnerConversionEnv::new(&owners[0], &unit, NativeContributionSource::new(Arc::clone(&owners[0].authority))),
        before_activation: RefCell::new(Some(Box::new(|| {
            let attempt = storage(&owners[0], &fs, "users").unwrap().converting_attempt().unwrap().clone();
            assert!(attempt.sealed().is_some());
            for owner in &owners[1..] {
                let stage = stage_root(owner, &fs, "users", &attempt.attempt_id);
                assert!(stage.join("staged.json").is_file(), "{} staged the old attempt", owner.metadata_root.display());
                assert!(stage.join("image/metadata-generation.sobj").is_file());
            }
            let taken_over =
                take_over(&cluster, "users", &cluster.nodes[1], &owners[1].metadata_root.join("users"));
            assert_eq!((taken_over.epoch, &taken_over.primary_node_id), (before.epoch + 1, &cluster.nodes[1]));
            *stale.borrow_mut() = Some(attempt);
        }))),
        activations: RefCell::new(Vec::new()),
    };
    let parked = settle(&env, &fs, "users");
    assert!(
        matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("primary")),
        "{parked:?}"
    );
    let outcomes = env.activations.borrow().iter().map(|(_, outcome)| *outcome).collect::<Vec<_>>();
    drop(env);
    assert_eq!(outcomes, vec![Submitted::Rejected], "the old attempt's activation is rejected");
    let stale = stale.into_inner().expect("the epoch moved before the old activation");
    assert!(
        !attempt_dir(&owners[0], &fs, "users", &stale.attempt_id).exists(),
        "the old primary deleted its superseded attempt"
    );
    assert!(
        header(&owners[0], "users").is_some_and(|header| header.identity
            != sinomo_dfs::metadata_shard_converted_generation(&fs, "users", &stale.attempt_id)),
        "the old primary never activated its superseded image"
    );

    // The new primary converts from its own copy under a new attempt.
    assert_eq!(drive(&owners[1], &fs, "users"), ConversionProgress::Ready);
    let current = shard_config(&owners[0], &fs, "users");
    let write_set = entry(&owners[0], sinomo_dfs::metadata_shard_write_set_key(&fs, "users"));
    let install_set = DfsMetadataShardConversionInstallRule::for_shard(&current, write_set.as_ref())
        .unwrap()
        .install_set();
    let record = await_activated(&cluster, &owners, "users", &install_set);
    let (attempt, _) = record.converted_activation().unwrap();
    assert_ne!(attempt.attempt_id, stale.attempt_id);
    assert_eq!((attempt.epoch, &attempt.primary_node_id), (current.epoch, &cluster.nodes[1]));
    // Nothing of either attempt stays staged on any member, and the old
    // primary's attempt files are gone.
    await_unstaged(&cluster, &owners, "users", &stale.attempt_id);
    await_unstaged(&cluster, &owners, "users", &attempt.attempt_id);
    for owner in &owners {
        assert!(!attempt_dir(owner, &fs, "users", &stale.attempt_id).exists());
    }
    drop(owners);
    cluster.native.close().unwrap();
}

/// Step `shard`'s conversion until a step fails with a reason a retry does
/// not change, and return that reason.
fn settle_until_stopped<E: ShardConversionEnv>(env: &E, fs: &DfsFileSystemId, shard: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        match advance_shard_conversion(env, fs, shard) {
            Ok(ConversionProgress::Pending { .. }) => {}
            Ok(progress) => panic!("{shard} conversion settled without stopping: {progress:?}"),
            Err(failure) if retryable(&failure) => std::thread::sleep(Duration::from_millis(20)),
            Err(failure) => return failure.to_string(),
        }
        assert!(Instant::now() < deadline, "{shard} conversion neither settled nor stopped");
    }
}

/// Have the services primary adopt an alias whose canonical target no shard
/// has, as only a lost or damaged canonical row leaves one: the adopt step of
/// a cross-shard hardlink, sent by the primary of its source path, does not
/// read the target. Returns the missing target.
fn adopt_dangling_alias(cluster: &Cluster) -> sinomo_dfs::DfsObjectId {
    let target = sinomo_dfs::DfsObjectId::from_legacy_named("native-dangling-target").unwrap();
    let now = sinomo_dfs::test_support::now_unix_nanos();
    let dangling = sinomo_dfs::DfsObjectMetadata {
        object_id: sinomo_dfs::DfsObjectId::from_legacy_named("native-dangling-alias").unwrap(),
        file_system_id: cluster.fs.clone(),
        path: "/services/dangling".into(),
        kind: sinomo_dfs::DfsObjectKind::Hardlink,
        namespace_link_count: None,
        tombstone_provenance: None,
        owner_node_id: cluster.nodes[1].clone(),
        current_locations: Vec::new(),
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: Some(target.clone()),
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: now,
        modified_at_unix_nanos: now,
        created_at_unix_nanos: now,
        changed_at_unix_nanos: now,
        version: sinomo_dfs::DfsVersion::from_legacy_named("native-dangling-alias-version").unwrap(),
        policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    {
        let mut services = cluster.native.client(&cluster.nodes[1]).unwrap();
        let reply = services
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                cluster.fs.clone(),
                cluster.nodes[1].clone(),
                sinomo_dfs::DfsNamespaceOperation::AdoptHardlinkAlias {
                    alias: dangling.clone(),
                    source_path: "/services/file".into(),
                },
                Duration::from_secs(30),
                false,
                false,
            )
            .unwrap();
        assert_eq!(reply.result, sinomo_dfs::DfsNamespaceResult::Applied);
    }
    target
}

/// A hardlink alias whose target exists in no shard stops only its own
/// shard's conversion, with a typed reason naming the alias and its target.
/// The filesystem's other shards convert and serve, every member records the
/// old copy it keeps as a retained original, and the stopped shard stays
/// closed.
#[test]
fn native_dangling_alias_stops_only_its_own_shard() {
    let mut cluster = start(
        "shard-conversion-dangling",
        3,
        &[("root", 0, &[1, 2]), ("services", 1, &[0, 2]), ("users", 2, &[0, 1])],
    );
    write_namespace(&cluster);
    let target = adopt_dangling_alias(&cluster);
    hold(&mut cluster, &["root", "services", "users"]);
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    for shard in ["root", "services", "users"] {
        close(&owners[0], &fs, shard);
    }
    for (shard, primary) in [("services", 1), ("users", 2)] {
        let parked = drive(&owners[primary], &fs, shard);
        assert!(matches!(parked, ConversionProgress::Parked { .. }), "{shard}: {parked:?}");
    }
    for (shard, primary, path) in [("root", 0, "/"), ("users", 2, "/users/doc")] {
        assert_eq!(drive(&owners[primary], &fs, shard), ConversionProgress::Ready, "{shard}");
        let config = shard_config(&owners[0], &fs, shard);
        let members = configured(&config);
        await_activated(&cluster, &owners, shard, &members);
        release(&cluster, &owners, shard);
        for reader in 0..cluster.nodes.len() {
            await_serving(&cluster, reader, &config, path);
        }
        // The primary's unit records its retained original when its step
        // reports the shard ready; the members' units record theirs on their
        // own owners.
        owners[primary].record_shard_conversion_original(&owners[primary].metadata_root.join(shard)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        for (owner, node) in owners.iter().zip(&cluster.nodes).filter(|(_, node)| members.contains(*node)) {
            while !owner.shard_conversion_original_recorded(&owner.metadata_root.join(shard)).unwrap() {
                assert!(Instant::now() < deadline, "{node} did not record the retained original of {shard}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
    let unit = metadata_unit(&owners[1], "services");
    let env = OwnerConversionEnv::new(&owners[1], &unit, NativeContributionSource::new(Arc::clone(&owners[1].authority)));
    let stopped = settle_until_stopped(&env, &fs, "services");
    assert!(
        stopped.contains("dangling alias /services/dangling") && stopped.contains(target.as_str()),
        "{stopped}"
    );
    drop(env);
    let services = storage(&owners[0], &fs, "services").unwrap();
    assert!(
        services.converting_attempt().is_some_and(|attempt| attempt.sealed().is_none()),
        "the stopped shard stays closed: {services:?}"
    );
    for (shard, path) in [("root", "/written-beside-stopped"), ("users", "/users/written-beside-stopped")] {
        assert!(storage(&owners[0], &fs, shard).unwrap().is_active());
        write_beside_closed(&cluster, 2, path);
    }
    drop(owners);
    cluster.native.close().unwrap();
}
