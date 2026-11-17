//! The per-shard conversion driver against an in-process KV store that keeps
//! the native request-identity ledger, with scripted contribution sources
//! and member installs.
use super::inventory::{ObservedState, UnitKey, UnitRole};
use super::scheduler::Scheduler;
use super::shard_conversion::*;
use crate::authority::contribution_source::{
    ContributionSource, SourceContributionAnswer, SourceContributionRead, SourceJobTouchingTarget,
};
use crate::metadata::canonical_generation::CapturedMetadataUnit;
use crate::namespace_counts::tests::{object, source};
use crate::namespace_counts::{NamespaceCountAliasContribution, ShardCountContributions};
use sinomo_core::kv::{InProcessKvStore, KvCommand, KvEntry, KvOp, KvTxnRequest};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataShardStorage, DfsObjectId, DfsObjectKind, MetadataBackendError,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, MetadataBackendError>;

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn fs_id() -> sinomo_dfs::DfsFileSystemId {
    sinomo_dfs::sifs_file_system_id()
}

fn shard_config(shard: &str, primary: &str, replicas: &[&str], epoch: u64) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: fs_id(),
        shard_id: shard.into(),
        primary_node_id: node(primary),
        replica_node_ids: replicas.iter().map(|name| node(name)).collect(),
        epoch,
        assignment: if shard == "root" {
            DfsMetadataShardAssignment::Root
        } else {
            DfsMetadataShardAssignment::PathPrefix { prefix: format!("/{shard}") }
        },
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    }
}

fn member_record(node_id: &NodeId) -> sinomo_core::ClusterMemberRecord {
    let mut record = sinomo_core::ClusterMemberRecord::local_voter(
        sinomo_core::Id::from_legacy_named("cluster_shard_conversion_tests").unwrap(),
        node_id.clone(),
        node_id.to_string(),
        format!("pk-{node_id}"),
        time::OffsetDateTime::UNIX_EPOCH,
    );
    record.capabilities = sinomo_core::node::main_daemon_capabilities(&record.role);
    record
}

fn write_set(shard: &DfsMetadataShardConfig, members: &[&str], generation: u64) -> sinomo_dfs::DfsMetadataShardWriteSet {
    let mut write_set = members.iter().map(|name| node(name)).collect::<Vec<_>>();
    write_set.sort();
    sinomo_dfs::DfsMetadataShardWriteSet {
        format: sinomo_dfs::DFS_METADATA_SHARD_WRITE_SET_FORMAT,
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard.shard_id.clone(),
        epoch: shard.epoch,
        primary_node_id: shard.primary_node_id.clone(),
        generation,
        write_set,
        epoch_start: None,
        switched_at_epoch: shard.epoch,
        substitutes: Vec::new(),
        raised_n: None,
        last_change: sinomo_dfs::DfsMetadataShardWriteSetChange {
            kind: sinomo_dfs::DfsMetadataShardWriteSetChangeKind::Switch,
            change_id: format!("change-{generation}"),
        },
    }
}

/// The cluster KV store and its request ledger, shared by every node's env.
#[derive(Default)]
struct Cluster {
    store: InProcessKvStore,
    /// Request id to the submitted command digest and, once applied, its
    /// outcome. A second command under one id is a request-id conflict.
    ledger: BTreeMap<String, (sinomo_core::kv::KvCommandDigest, Option<Submitted>)>,
}

impl Cluster {
    fn put(&mut self, key: String, value: &impl serde::Serialize) {
        self.store.put(key, sinomo_core::kv::encode_kv_value(value).unwrap(), None).unwrap();
    }
    fn storage(&self, shard: &str) -> Option<DfsMetadataShardStorage> {
        self.store
            .get(&sinomo_dfs::metadata_shard_storage_key(&fs_id(), shard))
            .map(|entry| entry.decode().unwrap())
    }
}

/// Answers each source's current scripted read, counting reads.
#[derive(Default)]
struct ScriptedSource {
    answers: Mutex<BTreeMap<String, SourceContributionRead>>,
    reads: Mutex<usize>,
}

impl ScriptedSource {
    fn set(&self, shard: &str, read: SourceContributionRead) {
        self.answers.lock().unwrap().insert(shard.into(), read);
    }
}

impl ContributionSource for ScriptedSource {
    fn read(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
    ) -> impl std::future::Future<Output = Result<SourceContributionRead>> + Send {
        assert_ne!(target.shard_id, source.shard_id, "a shard never contributes to itself");
        *self.reads.lock().unwrap() += 1;
        let read = self.answers.lock().unwrap().get(&source.shard_id).cloned();
        async move { read.ok_or_else(|| MetadataBackendError::Serde("unscripted source".into())) }
    }
}

type SubmitHook = Box<dyn FnOnce(&mut Cluster)>;

struct Env {
    root: sinomo_testdir::TestDir,
    local: NodeId,
    cluster: Rc<RefCell<Cluster>>,
    captured: Arc<CapturedMetadataUnit>,
    source: ScriptedSource,
    available: RefCell<BTreeSet<NodeId>>,
    submitted: RefCell<Vec<KvTxnRequest>>,
    /// Lose the outcome of the next request this matches, before it applies.
    lose: RefCell<Option<Box<dyn Fn(&KvTxnRequest) -> bool>>>,
    /// Change the cluster just before the next request this matches applies.
    before: RefCell<Option<(Box<dyn Fn(&KvTxnRequest) -> bool>, SubmitHook)>>,
    activated: RefCell<Vec<DfsMetadataShardStorage>>,
    watched_keys: RefCell<BTreeSet<String>>,
    watched_nodes: RefCell<BTreeSet<NodeId>>,
    /// The local copy is already current format.
    current: std::cell::Cell<bool>,
    captures: std::cell::Cell<usize>,
    runtime: tokio::runtime::Runtime,
}

impl ShardConversionEnv for Env {
    type Source = ScriptedSource;
    fn local_node(&self) -> Result<NodeId> {
        Ok(self.local.clone())
    }
    fn data_root(&self) -> &Path {
        self.root.path()
    }
    fn read_exact(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>> {
        let cluster = self.cluster.borrow();
        Ok(keys.iter().map(|key| cluster.store.get(key).cloned()).collect())
    }
    fn catalog(&self, file_system_id: &sinomo_dfs::DfsFileSystemId) -> Result<Vec<DfsMetadataShardConfig>> {
        let cluster = self.cluster.borrow();
        let mut catalog = cluster
            .store
            .range_prefix(&sinomo_dfs::metadata_shards_prefix(file_system_id))
            .unwrap()
            .into_iter()
            .map(|entry| entry.decode::<DfsMetadataShardConfig>().unwrap())
            .collect::<Vec<_>>();
        catalog.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(catalog)
    }
    fn submit(&self, request: &KvTxnRequest) -> Result<Submitted> {
        let digest = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone())).unwrap();
        let id = request.request_id.to_string();
        let mut cluster = self.cluster.borrow_mut();
        self.submitted.borrow_mut().push(request.clone());
        match cluster.ledger.get(&id) {
            Some((recorded, _)) if *recorded != digest => {
                return Err(MetadataBackendError::Serde(format!("request id conflict: {id}")));
            }
            Some((_, Some(outcome))) => return Ok(*outcome),
            _ => {}
        }
        cluster.ledger.insert(id.clone(), (digest, None));
        if self.lose.borrow().as_ref().is_some_and(|lose| lose(request)) {
            self.lose.borrow_mut().take();
            return Err(MetadataBackendError::StoragePreparationPending {
                root: self.root.path().to_owned(),
                processed_records: 0,
            });
        }
        let hook = match self.before.borrow().as_ref() {
            Some((matches, _)) => matches(request),
            None => false,
        };
        if hook {
            let (_, change) = self.before.borrow_mut().take().unwrap();
            change(&mut cluster);
        }
        let outcome = if cluster.store.txn(request.txn.clone()).unwrap().succeeded {
            Submitted::Committed
        } else {
            Submitted::Rejected
        };
        cluster.ledger.insert(id, (digest, Some(outcome)));
        Ok(outcome)
    }
    fn local_copy_current(&self, _shard: &DfsMetadataShardConfig) -> Result<bool> {
        Ok(self.current.get())
    }
    fn capture(&self, _shard: &DfsMetadataShardConfig) -> Result<CaptureProgress> {
        self.captures.set(self.captures.get() + 1);
        Ok(CaptureProgress::Captured(Arc::clone(&self.captured)))
    }
    fn contribution_source(&self) -> &ScriptedSource {
        &self.source
    }
    fn block_on<F: std::future::Future>(&self, future: F) -> Result<F::Output> {
        Ok(self.runtime.block_on(future))
    }
    fn install(
        &self,
        node: &NodeId,
        _shard: &DfsMetadataShardConfig,
        attempt: &sinomo_dfs::DfsMetadataShardConversion,
        image: &Path,
        source: &crate::authority::staged_service::conversion_install::ConversionSourceCut,
    ) -> Result<InstallReply> {
        assert!(attempt.sealed().is_some(), "only a sealed image is installed");
        assert!(image.join("metadata-generation.sobj").is_file());
        assert_eq!(
            crate::authority::staged_service::conversion_install::source_cut_digest(
                &source.source,
                &source.projection_fingerprint
            )
            .unwrap(),
            attempt.source.source_cut_digest,
            "members receive the source the attempt binds"
        );
        Ok(if self.available.borrow().contains(node) {
            InstallReply::Installed
        } else {
            InstallReply::Unavailable { reason: "unreachable".into() }
        })
    }
    fn activate_local(
        &self,
        _shard: &DfsMetadataShardConfig,
        record: &DfsMetadataShardStorage,
        _image: &Path,
    ) -> Result<()> {
        assert!(record.is_active());
        self.activated.borrow_mut().push(record.clone());
        Ok(())
    }
    fn watch_keys(&self, keys: BTreeSet<String>) -> Result<()> {
        self.watched_keys.borrow_mut().extend(keys);
        Ok(())
    }
    fn watch_nodes(&self, nodes: BTreeSet<NodeId>) -> Result<()> {
        self.watched_nodes.borrow_mut().extend(nodes);
        Ok(())
    }
}

fn file_object() -> sinomo_dfs::DfsObjectMetadata {
    object("users-file", "/users/file", DfsObjectKind::File)
}

/// The users shard's captured copy: its root directory, a file with a
/// local alias, and a subdirectory.
fn captured(root: &Path, catalog: &[DfsMetadataShardConfig]) -> Arc<CapturedMetadataUnit> {
    let file = file_object();
    let mut alias = object("users-alias", "/users/alias", DfsObjectKind::Hardlink);
    alias.link_target_object_id = Some(file.object_id.clone());
    source(
        root,
        catalog,
        "users",
        vec![
            object("users-dir", "/users", DfsObjectKind::Directory),
            file,
            alias,
            object("users-sub", "/users/sub", DfsObjectKind::Directory),
        ],
    )
    .captured
}

fn answer(source: &str, aliases: usize, basis: &str) -> SourceContributionRead {
    SourceContributionRead::Answered(SourceContributionAnswer {
        contributions: ShardCountContributions {
            source_shard: DfsMetadataShardId::new(source),
            target_shard: DfsMetadataShardId::new("users"),
            aliases: (0..aliases)
                .map(|n| NamespaceCountAliasContribution {
                    file_system_id: fs_id(),
                    alias_object_id: DfsObjectId::from_legacy_named(format!("{source}-alias-{n}")).unwrap(),
                    alias_path: format!("/{source}/alias-{n}"),
                    target_object_id: file_object().object_id,
                })
                .collect(),
            child_directories: Vec::new(),
        },
        running_jobs_touching_target: Vec::new(),
        basis: basis.into(),
    })
}

fn with_job(read: SourceContributionRead) -> SourceContributionRead {
    let SourceContributionRead::Answered(mut answer) = read else { unreachable!() };
    answer.running_jobs_touching_target.push(SourceJobTouchingTarget {
        job_id: "services-rename".into(),
        family: "rename-across-shard-primaries".into(),
        phase: "adopt".into(),
    });
    SourceContributionRead::Answered(answer)
}

/// A filesystem of root, services and users; users is converted. `members`
/// are its configured members, `switched` its write set if any.
fn cluster(name: &str, members: &[&str], switched: Option<&[&str]>) -> Rc<RefCell<Cluster>> {
    let cluster = Rc::new(RefCell::new(Cluster::default()));
    let mut kv = cluster.borrow_mut();
    let users = shard_config("users", members[0], &members[1..], 3);
    for shard in [shard_config("root", members[0], &[], 1), shard_config("services", members[0], &[], 1), users.clone()] {
        kv.put(sinomo_dfs::metadata_shard_key(&shard.file_system_id, &shard.shard_id), &shard);
    }
    if let Some(write_set_members) = switched {
        kv.put(
            sinomo_dfs::metadata_shard_write_set_key(&users.file_system_id, &users.shard_id),
            &write_set(&users, write_set_members, 1),
        );
    }
    for member in members.iter().chain(["takeover-x"].iter()) {
        let node = node(member);
        kv.put(sinomo_core::node::cluster_member_key(&node), &member_record(&node));
    }
    drop(kv);
    let _ = name;
    cluster
}

fn env(name: &str, local: &str, cluster: &Rc<RefCell<Cluster>>) -> Env {
    let root = sinomo_testdir::TestDir::new(name);
    let catalog = {
        let kv = cluster.borrow();
        let mut catalog = kv
            .store
            .range_prefix(&sinomo_dfs::metadata_shards_prefix(&fs_id()))
            .unwrap()
            .into_iter()
            .map(|entry| entry.decode::<DfsMetadataShardConfig>().unwrap())
            .collect::<Vec<_>>();
        catalog.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        catalog
    };
    let captured = captured(&root.path().join("source"), &catalog);
    let env = Env {
        local: node(local),
        cluster: Rc::clone(cluster),
        captured,
        source: ScriptedSource::default(),
        available: RefCell::new(BTreeSet::new()),
        submitted: RefCell::new(Vec::new()),
        lose: RefCell::new(None),
        before: RefCell::new(None),
        activated: RefCell::new(Vec::new()),
        watched_keys: RefCell::new(BTreeSet::new()),
        watched_nodes: RefCell::new(BTreeSet::new()),
        current: std::cell::Cell::new(false),
        captures: std::cell::Cell::new(0),
        runtime: tokio::runtime::Builder::new_current_thread().build().unwrap(),
        root,
    };
    env.source.set("root", answer("root", 0, "root-basis"));
    env.source.set("services", answer("services", 1, "services-basis"));
    env
}

fn advance(env: &Env) -> Result<ConversionProgress> {
    advance_shard_conversion(env, &fs_id(), "users")
}

/// Advance until the driver stops making progress on its own.
fn settle(env: &Env) -> Result<ConversionProgress> {
    for _ in 0..32 {
        match advance(env)? {
            ConversionProgress::Pending { .. } => continue,
            other => return Ok(other),
        }
    }
    panic!("conversion keeps advancing without settling");
}

/// Whether `request` writes the users storage record with `active` state.
fn writes_storage(request: &KvTxnRequest, active: bool) -> bool {
    request.txn.success.iter().any(|op| match op {
        KvOp::Put { key, value, .. } if *key == sinomo_dfs::metadata_shard_storage_key(&fs_id(), "users") => {
            sinomo_core::kv::decode_kv_value::<DfsMetadataShardStorage>(value)
                .is_ok_and(|record| record.is_active() == active)
        }
        _ => false,
    })
}

fn activations(env: &Env) -> Vec<KvTxnRequest> {
    env.submitted.borrow().iter().filter(|request| writes_storage(request, true)).cloned().collect()
}

fn attempt_dirs(env: &Env) -> Vec<String> {
    let root = env.root.path().join("dfs/migration-owner/shards").join(fs_id().to_string()).join("users");
    match std::fs::read_dir(root) {
        Ok(entries) => entries.map(|entry| entry.unwrap().file_name().into_string().unwrap()).collect(),
        Err(_) => Vec::new(),
    }
}

/// The counts the activated image carries for the users file.
fn file_count(env: &Env) -> Option<u64> {
    let record = env.cluster.borrow().storage("users").unwrap();
    let (attempt, _) = record.converted_activation().unwrap();
    let image = env
        .root
        .path()
        .join("dfs/migration-owner/shards")
        .join(fs_id().to_string())
        .join("users")
        .join(&attempt.attempt_id)
        .join("image");
    let tables = sinomo_dfs::read_metadata_candidate(&image).unwrap().tables;
    tables.objects.iter().find(|object| object.object_id == file_object().object_id).unwrap().namespace_link_count
}

#[test]
fn a_lost_activation_outcome_is_resubmitted_unchanged() {
    let cluster = cluster("lost-activation", &["primary-p", "replica-a", "replica-b"], None);
    let env = env("shard-conversion-lost-activation", "primary-p", &cluster);
    env.available.borrow_mut().insert(node("replica-a"));
    *env.lose.borrow_mut() = Some(Box::new(|request| writes_storage(request, true)));
    // The first activation is sent with replica-b not yet installed, and its
    // outcome is lost before it applies.
    let lost = settle(&env).unwrap_err();
    assert!(matches!(lost, MetadataBackendError::StoragePreparationPending { .. }), "{lost}");
    assert!(cluster.borrow().storage("users").is_some_and(|record| !record.is_active()));
    // replica-b installs before the retry, so a rebuilt activation would
    // compare one more member under the same request identity.
    env.available.borrow_mut().insert(node("replica-b"));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    let sent = activations(&env);
    assert_eq!(sent.len(), 2, "the activation is sent once and resubmitted once");
    assert_eq!(
        serde_json::to_vec(&sent[0]).unwrap(),
        serde_json::to_vec(&sent[1]).unwrap(),
        "the lost activation is resubmitted byte for byte"
    );
    assert!(cluster.borrow().storage("users").unwrap().is_active());
    assert_eq!(env.activated.borrow().len(), 1);
    // The image counts the local alias and the services alias.
    assert_eq!(file_count(&env), Some(3));
}

#[test]
fn a_rejected_activation_is_replanned_not_blocked() {
    let members = ["primary-p", "replica-a", "replica-b"];
    let cluster = cluster("rejected-activation", &members, Some(&members));
    let env = env("shard-conversion-rejected-activation", "primary-p", &cluster);
    env.available.borrow_mut().extend([node("replica-a"), node("replica-b")]);
    // A keeper shrink at the same epoch lands between the activation's read
    // and its commit, so its write-set compare fails.
    *env.before.borrow_mut() = Some((
        Box::new(|request| writes_storage(request, true)),
        Box::new(|cluster: &mut Cluster| {
            let users = shard_config("users", "primary-p", &["replica-a", "replica-b"], 3);
            cluster.put(
                sinomo_dfs::metadata_shard_write_set_key(&users.file_system_id, &users.shard_id),
                &write_set(&users, &["primary-p", "replica-a"], 2),
            );
        }),
    ));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready, "a failed compare is re-planned");
    let sent = activations(&env);
    assert_eq!(sent.len(), 2);
    assert_ne!(sent[0].request_id, sent[1].request_id, "the re-planned activation binds the new write set");
    assert!(cluster.borrow().storage("users").unwrap().is_active());
}

#[test]
fn install_under_the_joint_rule_tolerates_one_member_down() {
    let members = ["primary-p", "replica-a", "replica-b"];
    // Two of three members down: the primary alone is not a majority.
    let cluster = cluster("joint-install", &members, None);
    let env = env("shard-conversion-joint-install", "primary-p", &cluster);
    let parked = settle(&env).unwrap();
    assert!(matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("install rule")), "{parked:?}");
    assert!(env.watched_nodes.borrow().is_superset(&[node("replica-a"), node("replica-b")].into()));
    assert!(activations(&env).is_empty());
    // One member back: the primary and a majority hold the image.
    env.available.borrow_mut().insert(node("replica-a"));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    assert!(cluster.borrow().storage("users").unwrap().is_active());
}

#[test]
fn install_under_the_write_set_rule_after_a_keeper_shrink() {
    let members = ["primary-p", "replica-a", "replica-b"];
    let cluster = cluster("write-set-install", &members, Some(&members));
    let env = env("shard-conversion-write-set-install", "primary-p", &cluster);
    env.available.borrow_mut().insert(node("replica-a"));
    // A switched shard needs every write-set member, a majority is not enough.
    let parked = settle(&env).unwrap();
    assert!(matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("install rule")), "{parked:?}");
    assert!(activations(&env).is_empty());
    // The keeper shrinks the write set past the lost member.
    {
        let users = shard_config("users", "primary-p", &["replica-a", "replica-b"], 3);
        cluster.borrow_mut().put(
            sinomo_dfs::metadata_shard_write_set_key(&users.file_system_id, &users.shard_id),
            &write_set(&users, &["primary-p", "replica-a"], 2),
        );
    }
    let attempt = attempt_dirs(&env);
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    assert_eq!(attempt_dirs(&env), attempt, "a keeper change keeps the attempt");
    assert!(cluster.borrow().storage("users").unwrap().is_active());
}

#[test]
fn contribution_rounds_accept_only_after_two_equal_rounds() {
    let cluster = cluster("rounds-agree", &["primary-p"], None);
    let env = env("shard-conversion-rounds-agree", "primary-p", &cluster);
    // Round one: one alias. Round two: two aliases, so it disagrees.
    let mut outcomes = Vec::new();
    for (aliases, basis) in [(1, "basis-1"), (2, "basis-2"), (2, "basis-2")] {
        env.source.set("services", answer("services", aliases, basis));
        loop {
            let before = *env.source.reads.lock().unwrap();
            let progress = advance(&env).unwrap();
            if *env.source.reads.lock().unwrap() > before || progress == ConversionProgress::Ready {
                outcomes.push(progress);
                break;
            }
        }
    }
    assert_eq!(
        outcomes[..2],
        [ConversionProgress::Pending { processed_records: 1 }, ConversionProgress::Pending { processed_records: 1 }],
        "two rounds that differ accept nothing"
    );
    assert!(!outcomes[..2].contains(&ConversionProgress::Ready));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    // The sealed contributions are those of the two agreeing rounds.
    assert_eq!(file_count(&env), Some(4));
}

#[test]
fn a_job_touching_the_shard_keeps_rounds_from_agreeing_until_it_is_gone() {
    let cluster = cluster("rounds-job", &["primary-p"], None);
    let env = env("shard-conversion-rounds-job", "primary-p", &cluster);
    env.source.set("services", with_job(answer("services", 1, "basis-1")));
    for _ in 0..4 {
        let parked = settle(&env).unwrap();
        assert!(
            matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("services")),
            "{parked:?}"
        );
    }
    let storage = cluster.borrow().storage("users").unwrap();
    assert!(storage.converting_attempt().unwrap().sealed().is_none(), "equal rounds naming a job never seal");
    env.source.set("services", answer("services", 1, "basis-2"));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    assert_eq!(file_count(&env), Some(3));
}

#[test]
fn a_waiting_source_parks_the_unit_without_a_timer() {
    let cluster = cluster("rounds-waiting", &["primary-p"], None);
    let env = env("shard-conversion-rounds-waiting", "primary-p", &cluster);
    let services_key = sinomo_dfs::metadata_shard_storage_key(&fs_id(), "services");
    env.source.set(
        "services",
        SourceContributionRead::Waiting { nodes: [node("services-member")].into(), keys: [services_key.clone()].into() },
    );
    let progress = settle(&env).unwrap();
    let ConversionProgress::Parked { evidence } = &progress else { panic!("{progress:?}") };
    assert!(evidence.contains("no member that can answer"), "{evidence}");
    assert!(env.watched_nodes.borrow().contains(&node("services-member")));
    assert!(env.watched_keys.borrow().contains(&services_key));
    // Converting is written before the wait: the shard's contributions are
    // frozen while it waits.
    assert!(cluster.borrow().storage("users").unwrap().converting_attempt().is_some());
    // The owner parks the unit; only the watched node's reachability wakes it.
    let mut scheduler = Scheduler::new();
    let unit = UnitKey {
        relative_root: "dfs/metadata/users".into(),
        role: UnitRole::Metadata { shard: DfsMetadataShardId::new("users") },
    };
    scheduler.enqueue(unit.clone(), false).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    scheduler.watch_reachable_nodes(&unit, env.watched_nodes.borrow().clone()).unwrap();
    reference.state = observed_state(&progress);
    assert!(matches!(reference.state, ObservedState::Waiting { .. }));
    scheduler.finish(&id, reference);
    assert!(scheduler.next().is_none());
    assert!(scheduler.next_retry().is_none(), "a parked conversion owns no timer");
    assert!(scheduler.nodes_became_reachable(&[node("services-member")].into()));
    assert!(scheduler.next().is_some());
    env.source.set("services", answer("services", 1, "basis-1"));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
}

/// A takeover moves the epoch while the old primary's activation is
/// pending: that activation is rejected, the old primary deletes its
/// attempt, and the new primary converts from its own copy under a new
/// attempt whose Converting record replaces the stale one.
#[test]
fn a_takeover_supersedes_the_attempt_and_its_files() {
    let members = ["primary-p", "replica-a", "replica-b"];
    let cluster = cluster("takeover", &members, None);
    let old = env("shard-conversion-takeover-old", "primary-p", &cluster);
    old.available.borrow_mut().insert(node("replica-a"));
    *old.lose.borrow_mut() = Some(Box::new(|request| writes_storage(request, true)));
    assert!(settle(&old).is_err());
    let stale = cluster.borrow().storage("users").unwrap();
    let stale_attempt = stale.converting_attempt().unwrap().clone();
    assert!(stale_attempt.sealed().is_some());
    assert_eq!(attempt_dirs(&old), vec![stale_attempt.attempt_id.clone()]);
    let pending_activation = activations(&old).pop().unwrap();
    // replica-a takes over at the next epoch.
    let successor = shard_config("users", "replica-a", &["primary-p", "replica-b"], 4);
    cluster.borrow_mut().put(sinomo_dfs::metadata_shard_key(&fs_id(), "users"), &successor);
    // The old primary's activation can no longer commit.
    let mut kv = cluster.borrow_mut();
    assert!(!kv.store.txn(pending_activation.txn.clone()).unwrap().succeeded);
    drop(kv);
    let parked = settle(&old).unwrap();
    assert!(matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("primary")), "{parked:?}");
    assert!(attempt_dirs(&old).is_empty(), "the superseded attempt's files are deleted");
    let new = env("shard-conversion-takeover-new", "replica-a", &cluster);
    new.available.borrow_mut().insert(node("replica-b"));
    assert_eq!(settle(&new).unwrap(), ConversionProgress::Ready);
    let record = cluster.borrow().storage("users").unwrap();
    let (attempt, _) = record.converted_activation().unwrap();
    assert_ne!(attempt.attempt_id, stale_attempt.attempt_id);
    assert_eq!((attempt.epoch, &attempt.primary_node_id), (4, &node("replica-a")));
    assert_eq!(attempt_dirs(&new), vec![attempt.attempt_id.clone()]);
    // The old primary hands over once the shard is active.
    assert_eq!(settle(&old).unwrap(), ConversionProgress::Handover);
}

/// An exchange interrupted after the local copy moved aside finishes when it
/// is repeated, and a repeat after it finished changes nothing.
#[test]
fn an_interrupted_image_exchange_finishes_on_repeat() {
    let root = sinomo_testdir::TestDir::new("shard-conversion-exchange");
    let destination = root.path().join("metadata/users");
    std::fs::create_dir_all(&destination).unwrap();
    std::fs::write(destination.join("old-copy"), b"retained source").unwrap();
    let generation = sinomo_dfs::metadata_shard_converted_generation(&fs_id(), "users", &"a".repeat(64));
    let image = root.path().join("attempt/image");
    let writer = sinomo_dfs::DfsMetadataCandidateWriter::open_generation(&image, generation.clone()).unwrap();
    let candidate = writer
        .write(
            Default::default(),
            sinomo_dfs::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 0,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
            },
        )
        .unwrap();
    writer.seal_storage_generation(&candidate).unwrap();
    drop(writer);
    let retained = root.path().join("metadata/.metadata-retained/shard-conversion-users");
    std::fs::create_dir_all(retained.parent().unwrap()).unwrap();
    // Interrupted after the first rename: the copy is aside, the image is not in place.
    std::fs::rename(&destination, &retained).unwrap();
    install_activated_image(&destination, &image, &retained, &generation).unwrap();
    assert_eq!(sinomo_dfs::inspect_metadata_storage_generation(&destination).unwrap().identity, generation);
    assert_eq!(std::fs::read(retained.join("old-copy")).unwrap(), b"retained source");
    assert!(!image.exists());
    let header = std::fs::read(destination.join("metadata-generation.sobj")).unwrap();
    install_activated_image(&destination, &image, &retained, &generation).unwrap();
    assert_eq!(std::fs::read(destination.join("metadata-generation.sobj")).unwrap(), header);
    assert_eq!(std::fs::read(retained.join("old-copy")).unwrap(), b"retained source");
}

/// A native shard born before storage records existed has no record and a
/// current local copy: the driver leaves it alone, writing no Converting.
#[test]
fn a_record_less_shard_with_a_current_copy_is_not_converted() {
    let cluster = cluster("record-less-current", &["primary-p"], None);
    let env = env("shard-conversion-record-less-current", "primary-p", &cluster);
    env.current.set(true);
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    assert!(cluster.borrow().storage("users").is_none(), "no Converting is written");
    assert!(env.submitted.borrow().is_empty());
    assert_eq!(env.captures.get(), 0, "the current copy is never captured as a source");
}

/// A legacy copy without a storage record keeps the complete-filesystem
/// conversion: its members can retain floors the per-shard stage refuses, so
/// writing Converting would strand the shard. A native-genesis record keeps it
/// too; any other record-less copy converts per shard.
#[test]
fn a_legacy_copy_without_a_record_keeps_the_complete_filesystem_conversion() {
    use super::shard_conversion::{MetadataUnitDispatch, metadata_unit_dispatch};
    assert_eq!(metadata_unit_dispatch(None, true), MetadataUnitDispatch::CompleteFileSystem);
    assert_eq!(metadata_unit_dispatch(None, false), MetadataUnitDispatch::PerShard);
    let shard = shard_config("users", "primary-a", &["replica-b"], 3);
    let born = sinomo_dfs::DfsMetadataShardStorage::native_genesis(
        &shard,
        sinomo_dfs::DfsMetadataGenerationIdentity {
            file_system_id: shard.file_system_id.clone(),
            shard_id: sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id),
            generation: "native-genesis".into(),
            origin: sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesis,
        },
    );
    for legacy_copy in [false, true] {
        assert_eq!(
            metadata_unit_dispatch(Some(&born), legacy_copy),
            MetadataUnitDispatch::CompleteFileSystem
        );
    }
}
