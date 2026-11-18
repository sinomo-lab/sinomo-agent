//! The per-shard conversion driver against an in-process KV store that keeps
//! the native request-identity ledger, with scripted contribution sources
//! and member installs.
use super::inventory::{ObservedState, UnitKey, UnitRole};
use super::scheduler::Scheduler;
use super::shard_conversion::*;
use crate::authority::contribution_source::{
    ContributionSource, SourceContributionAnswer, SourceContributionRead, SourceHoldings,
    SourceJobTouchingTarget, SourceQuestion,
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

mod recheck;

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
    /// The object ids, paths and job ids each source holds, answering a
    /// round's question.
    holds: Mutex<BTreeMap<String, BTreeSet<String>>>,
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
    fn read_asking(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
        question: &SourceQuestion,
    ) -> impl std::future::Future<Output = Result<SourceContributionRead>> + Send {
        let read = self.read(target, source);
        let holds = self.holds.lock().unwrap().get(&source.shard_id).cloned().unwrap_or_default();
        let question = question.clone();
        async move {
            let mut read = read.await?;
            if let SourceContributionRead::Answered(answer) = &mut read {
                answer.held = SourceHoldings {
                    answered: question.len() as u64,
                    objects: question.objects.iter().filter(|id| holds.contains(id.as_str())).cloned().collect(),
                    directories: question.directories.iter().filter(|path| holds.contains(*path)).cloned().collect(),
                    jobs: question.jobs.iter().filter(|job| holds.contains(*job)).cloned().collect(),
                };
            }
            Ok(read)
        }
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
    /// The shard's share of the original placement goals.
    placement: RefCell<Vec<crate::metadata::RetainedPlacementReservation>>,
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
    fn capture(&self, _shard: &DfsMetadataShardConfig) -> Result<CaptureProgress> {
        self.captures.set(self.captures.get() + 1);
        Ok(CaptureProgress::Captured(Arc::clone(&self.captured)))
    }
    fn placement_evidence(
        &self,
        _shard: &DfsMetadataShardConfig,
    ) -> Result<Option<Vec<crate::metadata::RetainedPlacementReservation>>> {
        Ok(Some(self.placement.borrow().clone()))
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
            moving_directories: Vec::new(),
        },
        running_jobs_touching_target: Vec::new(),
        basis: basis.into(),
        held: SourceHoldings::default(),
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
        placement: RefCell::new(Vec::new()),
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

/// The services directory move whose next page carries an alias of the
/// users file, as its source reports it: its phase and, when its primary
/// recorded its step waiting for the users shard's storage, that park.
fn directory_move(parked: bool) -> SourceJobTouchingTarget {
    let mut phase = "cross-shard-dir-scan".to_owned();
    if parked {
        phase.push_str(";parked-on-target");
    }
    SourceJobTouchingTarget {
        job_id: "services-directory-move".into(),
        family: "rename-directory-across-shard-primaries".into(),
        phase,
    }
}

/// `read` with `jobs` touching the users shard.
fn with_jobs(read: SourceContributionRead, jobs: impl IntoIterator<Item = SourceJobTouchingTarget>) -> SourceContributionRead {
    let SourceContributionRead::Answered(mut answer) = read else { unreachable!() };
    answer.running_jobs_touching_target.extend(jobs);
    answer.running_jobs_touching_target.sort();
    SourceContributionRead::Answered(answer)
}

#[test]
fn rounds_agree_beside_a_job_parked_on_the_shard() {
    let cluster = cluster("rounds-parked-job", &["primary-p"], None);
    let env = env("shard-conversion-rounds-parked-job", "primary-p", &cluster);
    // The job's step waits for this shard's storage, so it moves none of the
    // shard's contributions before the shard activates.
    env.source.set("services", with_jobs(answer("services", 1, "basis-1"), [directory_move(true)]));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    assert!(cluster.borrow().storage("users").unwrap().is_active());
    assert_eq!(file_count(&env), Some(3));
}

#[test]
fn a_job_touching_the_shard_but_not_parked_on_it_still_holds_the_rounds() {
    let cluster = cluster("rounds-unparked-job", &["primary-p"], None);
    let env = env("shard-conversion-rounds-unparked-job", "primary-p", &cluster);
    // Running toward its parked step: the job may still move the shard's
    // contributions.
    env.source.set("services", with_jobs(answer("services", 1, "basis-1"), [directory_move(false)]));
    let held = |env: &Env, by: &str| {
        for _ in 0..3 {
            let parked = settle(env).unwrap();
            assert!(
                matches!(&parked, ConversionProgress::Parked { evidence }
                    if evidence == &format!("jobs of shards {by} touch this shard")),
                "{parked:?}"
            );
        }
        assert!(cluster.borrow().storage("users").unwrap().converting_attempt().unwrap().sealed().is_none());
    };
    held(&env, "services");
    // Parked on the shard, beside another source's job that is not, the
    // rounds still wait for that other job.
    env.source.set("services", with_jobs(answer("services", 1, "basis-2"), [directory_move(true)]));
    env.source.set(
        "root",
        with_jobs(
            answer("root", 0, "root-basis"),
            [SourceJobTouchingTarget {
                job_id: "root-rename".into(),
                family: "rename-object-across-shard-primaries".into(),
                phase: "cross-shard-adopt".into(),
            }],
        ),
    );
    held(&env, "root");
    // Once only the parked job is left, two equal rounds agree.
    env.source.set("root", answer("root", 0, "root-basis"));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    assert_eq!(file_count(&env), Some(3));
}

/// The alias the parked job carries at its next page is still a live row at
/// its old path in its source, and is counted from there. Wherever another
/// answer also holds that alias, it counts once, by object id.
#[test]
fn a_parked_jobs_carried_alias_counts_once() {
    let cluster = cluster("rounds-parked-carried", &["primary-p"], None);
    let env = env("shard-conversion-rounds-parked-carried", "primary-p", &cluster);
    let carried = |path: &str| NamespaceCountAliasContribution {
        file_system_id: fs_id(),
        alias_object_id: DfsObjectId::from_legacy_named("tree-carried-alias").unwrap(),
        alias_path: path.into(),
        target_object_id: file_object().object_id,
    };
    let with_alias = |read: SourceContributionRead, path: &str| {
        let SourceContributionRead::Answered(mut answer) = read else { unreachable!() };
        answer.contributions.aliases.push(carried(path));
        SourceContributionRead::Answered(answer)
    };
    env.source.set(
        "services",
        with_jobs(with_alias(answer("services", 0, "services-basis"), "/services/tree/alias"), [directory_move(true)]),
    );
    env.source.set("root", with_alias(answer("root", 0, "root-basis"), "/tree/alias"));
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    // The file's own link and the local alias, plus the carried alias once.
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

/// A copy without a storage record, legacy or not, converts per shard, as
/// does a shard whose conversion wrote a record. A native-genesis record
/// holds no old storage and needs no conversion.
#[test]
fn a_legacy_copy_without_a_record_converts_per_shard() {
    use super::shard_conversion::{MetadataUnitDispatch, metadata_unit_dispatch};
    assert_eq!(metadata_unit_dispatch(None), MetadataUnitDispatch::PerShard);
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
    assert_eq!(metadata_unit_dispatch(Some(&born)), MetadataUnitDispatch::Current);
    let source = sinomo_dfs::DfsMetadataConversionSource {
        source_identity: "legacy-source".into(),
        source_format: Some(3),
        source_journal_sequence: 9,
        source_state_fingerprint: "legacy-state".into(),
        source_cut_digest: "a".repeat(64),
        pending_mapping_digest: "b".repeat(64),
    };
    let converting = sinomo_dfs::DfsMetadataShardStorage::converting(
        &shard,
        sinomo_dfs::DfsMetadataShardConversion {
            attempt_id: sinomo_dfs::metadata_shard_conversion_attempt_id(
                &shard.file_system_id,
                &shard.shard_id,
                &shard.assignment,
                shard.epoch,
                &shard.primary_node_id,
                &source,
                None,
            )
            .unwrap(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            takeover: None,
            source,
            seal: None,
            contributions_digest: None,
        },
    );
    converting.validate_against(&shard).unwrap();
    assert_eq!(metadata_unit_dispatch(Some(&converting)), MetadataUnitDispatch::PerShard);
}

/// A projects move of `/users/projects`, the projects shard's root, to
/// `/services/old-projects`, parked on users at its settling step: users
/// holds the move's parent participant Reserved, as its captured rows record
/// it, and a real capture names that Running row as unresolved original
/// work. The rounds accept the parked move, users applies the participant's
/// successor from the move's reported plan phase, its counts agree with the
/// contributions (projects no longer holds `/users/projects`), and the shard
/// activates with the participant recorded Applied.
#[test]
fn a_parked_move_with_a_parent_participant_in_the_shard_converges() {
    let cluster = cluster("parked-participant", &["primary-p"], None);
    let mut projects = shard_config("projects", "primary-p", &[], 1);
    projects.assignment = DfsMetadataShardAssignment::PathPrefix { prefix: "/users/projects".into() };
    cluster.borrow_mut().put(sinomo_dfs::metadata_shard_key(&fs_id(), "projects"), &projects);
    let mut env = env("shard-conversion-parked-participant", "primary-p", &cluster);
    let catalog = env.catalog(&fs_id()).unwrap();
    let users = catalog.iter().find(|shard| shard.shard_id == "users").unwrap().clone();
    let mut parent = object("users-dir", "/users", DfsObjectKind::Directory);
    parent.namespace_link_count = Some(4);
    let mut successor = parent.clone();
    successor.namespace_link_count = Some(3);
    successor.version = sinomo_dfs::DfsVersion::from_legacy_named("users-dir-after-move").unwrap();
    successor.modified_at_unix_nanos = 9;
    successor.changed_at_unix_nanos = 9;
    let participant = sinomo_dfs::DfsParentCountParticipant {
        participant_id: String::new(),
        binding: sinomo_dfs::DfsParentCountBinding {
            job_id: "projects-move".into(),
            file_system_id: fs_id(),
            root_path: "/users/projects".into(),
            target_path: "/services/old-projects".into(),
            coordinator_scope: projects.clone(),
            source: object("projects-dir", "/users/projects", DfsObjectKind::Directory),
            expected_destination: sinomo_dfs::DfsNamespaceDestinationPrecondition::Absent,
            origin: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        },
        parent_scope: users,
        expected: parent.clone(),
        successor: successor.clone(),
        delta: -1,
    }
    .with_derived_id_for_test()
    .unwrap();
    let held = participant.recorded_job(sinomo_dfs::DfsParentCountState::Reserved, time::OffsetDateTime::UNIX_EPOCH);
    let file = file_object();
    let mut alias = object("users-alias", "/users/alias", DfsObjectKind::Hardlink);
    alias.link_target_object_id = Some(file.object_id.clone());
    let base = source(
        &env.root.path().join("source-participant"),
        &catalog,
        "users",
        vec![parent, file, alias, object("users-sub", "/users/sub", DfsObjectKind::Directory)],
    )
    .captured;
    let mut rows = base.projected_rows.clone();
    rows.namespace_jobs = vec![held.clone()];
    env.captured = Arc::new(
        CapturedMetadataUnit::from_test_projection(base.source.clone(), rows, Vec::new())
            .unwrap()
            .with_original_work_for_test(vec![crate::metadata::canonical_generation::OriginalWorkRef {
                family: "namespace_jobs".into(),
                original_id: held.job_id.clone(),
                original_digest: None,
                reason: "original namespace job has not completed".into(),
            }])
            .unwrap(),
    );
    let id = &participant.participant_id;
    env.source.set(
        "projects",
        with_jobs(
            answer("projects", 0, "projects-basis"),
            [SourceJobTouchingTarget {
                job_id: "projects-move".into(),
                family: "rename-directory-across-shard-primaries".into(),
                phase: format!("cross-shard-dir-finish;parent-counts=settling;participant={id};parked-on-target"),
            }],
        ),
    );
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    let record = cluster.borrow().storage("users").unwrap();
    assert!(record.is_active());
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
    let applied = tables.objects.iter().find(|object| object.object_id == successor.object_id).unwrap();
    assert_eq!(*applied, successor, "the image carries the participant's successor");
    let job = tables.namespace_jobs.iter().find(|job| job.job_id == *id).unwrap();
    assert_eq!(job.status, sinomo_dfs::DfsNamespaceJobStatus::Completed);
    assert!(matches!(
        job.pending_frames.as_slice(),
        [sinomo_dfs::DfsNamespaceJobFrame::ParentCountParticipant {
            state: sinomo_dfs::DfsParentCountState::Applied,
            ..
        }]
    ));
    assert!(tables.versions.contains(&participant.applied_version().unwrap()));
    assert_eq!(file_count(&env), Some(3));
}

/// The users shard's capture, as [`captured`] gives it, with `extra` rows.
fn captured_with(env: &Env, extra: Vec<sinomo_dfs::DfsObjectMetadata>) -> Arc<CapturedMetadataUnit> {
    let file = file_object();
    let mut alias = object("users-alias", "/users/alias", DfsObjectKind::Hardlink);
    alias.link_target_object_id = Some(file.object_id.clone());
    let mut objects = vec![
        object("users-dir", "/users", DfsObjectKind::Directory),
        file,
        alias,
        object("users-sub", "/users/sub", DfsObjectKind::Directory),
    ];
    objects.extend(extra);
    source(&env.root.path().join("source-with"), &env.catalog(&fs_id()).unwrap(), "users", objects).captured
}

/// An alias whose target no shard holds stops the shard's conversion with a
/// typed reason naming the alias and the target, after the shard is closed
/// and only once every other shard answered; a shard that cannot answer is
/// waited for, never taken to hold nothing. Once a shard holds the target,
/// the conversion goes on.
#[test]
fn a_dangling_alias_stops_the_conversion_only_once_every_shard_answered() {
    let cluster = cluster("dangling", &["primary-p"], None);
    let mut env = env("shard-conversion-dangling", "primary-p", &cluster);
    let target = DfsObjectId::from_legacy_named("dangling-target").unwrap();
    let mut dangling = object("users-dangling", "/users/dangling", DfsObjectKind::Hardlink);
    dangling.link_target_object_id = Some(target.clone());
    env.captured = captured_with(&env, vec![dangling]);
    env.source.set(
        "services",
        SourceContributionRead::Waiting { nodes: [node("services-member")].into(), keys: BTreeSet::new() },
    );
    let parked = settle(&env).unwrap();
    assert!(matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("no member that can answer")), "{parked:?}");
    env.source.set("services", answer("services", 1, "services-basis"));
    for _ in 0..2 {
        let stopped = settle(&env).unwrap_err().to_string();
        assert!(
            stopped.contains("dangling alias /users/dangling") && stopped.contains(target.as_str()),
            "{stopped}"
        );
    }
    let storage = cluster.borrow().storage("users").unwrap();
    assert!(storage.converting_attempt().unwrap().sealed().is_none(), "a stopped shard stays closed and unsealed");
    env.source.holds.lock().unwrap().insert("services".into(), [target.to_string()].into());
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
}

/// The attempt binds the shard's share of the original placement goals: the
/// capture carries it, the attempt's files record it, and a capture without
/// it is refused.
#[test]
fn the_attempt_binds_the_shards_share_of_the_placement_goals() {
    let cluster = cluster("placement", &["primary-p"], None);
    let mut env = env("shard-conversion-placement", "primary-p", &cluster);
    let catalog = env.catalog(&fs_id()).unwrap();
    let goal = crate::authority::native_fixtures::pending_v0_goal_for_test("per-shard-goal", &catalog);
    let evidence = crate::authority::placement_work::shard_placement_share(
        std::slice::from_ref(&goal),
        &fs_id(),
        &DfsMetadataShardId::new("users"),
    )
    .unwrap();
    assert_eq!(evidence.len(), 1);
    *env.placement.borrow_mut() = evidence.clone();
    let refused = settle(&env).unwrap_err().to_string();
    assert!(refused.contains("share of the original placement goals"), "{refused}");
    let mut facts = env.captured.source.clone();
    facts.retained_placement_reservations = evidence.clone();
    env.captured = Arc::new(
        CapturedMetadataUnit::from_test_projection(
            facts,
            env.captured.projected_rows.clone(),
            env.captured.proven_v7_terminals.clone(),
        )
        .unwrap(),
    );
    assert_eq!(settle(&env).unwrap(), ConversionProgress::Ready);
    let attempt = attempt_dirs(&env).pop().unwrap();
    let recorded = std::fs::read(
        env.root
            .path()
            .join("dfs/migration-owner/shards")
            .join(fs_id().to_string())
            .join("users")
            .join(attempt)
            .join("placement-evidence.json"),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<crate::metadata::RetainedPlacementReservation>>(&recorded).unwrap(),
        evidence
    );
}
