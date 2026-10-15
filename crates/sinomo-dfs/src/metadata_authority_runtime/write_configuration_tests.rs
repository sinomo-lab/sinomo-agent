//! Runtime ownership and native bootstrap tests. The host queues tasks and
//! supplies native read fixtures; no test substitutes discovery counts for proof.

use super::*;
use sinomo_core::kv::{
    ClusterKvCommittedKeyState, ClusterKvRaftConfigRecord, KvAuthoritativeRead, KvAuthorityBasis,
    KvAuthorityProof, KvEventKind, KvVersion, encode_kv_value,
};
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;

#[derive(Clone)]
struct NativeInputs {
    authority: KvAuthorityProof,
    floor: KvLocalRead<Option<KvEntry>>,
    scopes: [Vec<KvEntry>; 3],
    floor_unavailable: bool,
}

#[derive(Default)]
struct PrefixGate {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct NativeProjectionHost {
    local: sinomo_core::node::NodeId,
    inputs: Mutex<NativeInputs>,
    reads: Mutex<Vec<String>>,
    tasks: Mutex<VecDeque<DfsMetadataAuthorityTask>>,
    spawned: AtomicUsize,
    content_gate: PrefixGate,
    floor_observer: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

fn fixture_entry(key: &str, value: Vec<u8>, revision: u64) -> KvEntry {
    KvEntry {
        key: key.into(),
        value,
        create_revision: KvRevision(revision),
        modify_revision: KvRevision(revision),
        version: KvVersion(1),
        lease_id: None,
    }
}

impl NativeProjectionHost {
    fn new(configured: bool) -> Arc<Self> {
        let local = sinomo_core::Id::derived("write-configuration-runtime", b"local");
        let filesystem = crate::namespace::distributed_sifs(local.clone());
        let config = crate::DfsFileSystemConfig::from(&filesystem);
        let mut content = crate::DfsNodeAdvertisement::content(local.clone());
        content.zone = Some("native-zone".into());
        let mut provider =
            sinomo_core::service::ServiceInstanceRecord::new("native-dfs", local.clone());
        provider
            .provider_capabilities
            .push("dfs.object.transfer".into());
        let host = Arc::new(Self {
            local: local.clone(),
            inputs: Mutex::new(NativeInputs {
                authority: KvAuthorityProof {
                    term: 7,
                    commit_index: 110,
                    revision: KvRevision(10),
                    basis: KvAuthorityBasis::CurrentTermQuorum {
                        voter_acks: 1,
                        required_quorum: 1,
                    },
                },
                floor: KvLocalRead {
                    value: None,
                    revision: KvRevision(10),
                    observed_term: 7,
                    applied_index: 110,
                    known_leader: Some(local.clone()),
                },
                scopes: [
                    vec![fixture_entry(
                        &crate::file_system_config_key(&filesystem.file_system_id),
                        encode_kv_value(&config).unwrap(),
                        8,
                    )],
                    vec![fixture_entry(
                        &crate::content_service_key(&local),
                        encode_kv_value(&content).unwrap(),
                        8,
                    )],
                    vec![fixture_entry(
                        &sinomo_core::service::cluster_service_instance_key(
                            &provider.service_id,
                            &local,
                        ),
                        encode_kv_value(&provider).unwrap(),
                        8,
                    )],
                ],
                floor_unavailable: false,
            }),
            reads: Mutex::new(Vec::new()),
            tasks: Mutex::new(VecDeque::new()),
            spawned: AtomicUsize::new(0),
            content_gate: PrefixGate::default(),
            floor_observer: Mutex::new(None),
        });
        if configured {
            host.set_configuration(10);
        } else {
            let mut inputs = host.inputs.lock().unwrap();
            inputs.authority.revision = KvRevision::ZERO;
            inputs.authority.commit_index = 1;
            inputs.floor.revision = KvRevision::ZERO;
            inputs.floor.applied_index = 1;
            inputs.scopes = std::array::from_fn(|_| Vec::new());
        }
        host
    }

    fn set_configuration(&self, revision: u64) -> KvEntry {
        let config = ClusterKvRaftConfigRecord {
            cluster_id: sinomo_core::Id::derived("write-configuration-runtime", b"cluster"),
            term: 7,
            kv_revision: revision,
            voters: vec![self.local.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let entry = fixture_entry(
            &sinomo_core::kv::cluster_kv_raft_config_key(),
            encode_kv_value(&config).unwrap(),
            revision,
        );
        let mut inputs = self.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(revision);
        inputs.authority.commit_index = revision + 100;
        inputs.floor.value = Some(entry.clone());
        inputs.floor.revision = KvRevision(revision);
        inputs.floor.applied_index = revision + 100;
        entry
    }

    fn take_task(&self) -> Option<DfsMetadataAuthorityTask> {
        self.tasks.lock().unwrap().pop_front()
    }

    async fn run_next(&self) {
        self.take_task()
            .expect("one explicitly requested refresh task")
            .await;
    }

    fn read_count(&self) -> usize {
        self.reads.lock().unwrap().len()
    }

    fn queued_count(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    fn discard_queued(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        // Releasing a refresh owner may touch the host queue; never drop it
        // while holding that queue's mutex.
        drop(tasks);
    }
}

impl DfsMetadataAuthorityHost for NativeProjectionHost {
    fn read_authoritative_prefix(&self) -> DfsMetadataAuthorityReadFuture<'_> {
        self.read_authoritative_configuration_prefix(DFS_METADATA_AUTHORITY_PREFIX)
    }

    fn supports_write_configuration_projection(&self) -> bool {
        true
    }

    fn allows_unconfigured_single_node_write_configuration(
        &self,
        authority: &KvAuthorityProof,
        floor: &KvLocalRead<Option<KvEntry>>,
    ) -> bool {
        matches!(
            authority.basis,
            KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1
            }
        ) && authority.term > 0
            && authority.revision == KvRevision::ZERO
            && floor.value.is_none()
            && floor.observed_term == authority.term
            && floor.applied_index >= authority.commit_index
            && floor.known_leader.as_ref() == Some(&self.local)
    }

    fn read_authoritative_configuration_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsMetadataAuthorityReadFuture<'a> {
        Box::pin(async move {
            let scope = DFS_WRITE_CONFIGURATION_PREFIXES
                .iter()
                .position(|candidate| *candidate == prefix)
                .expect("one bounded configuration scope");
            self.reads.lock().unwrap().push(prefix.into());
            let read = {
                let inputs = self.inputs.lock().unwrap();
                KvAuthoritativeRead {
                    value: inputs.scopes[scope].clone(),
                    authority: inputs.authority,
                }
            };
            if scope == 1 && self.content_gate.armed.swap(false, Ordering::AcqRel) {
                self.content_gate.entered.notify_one();
                self.content_gate.release.notified().await;
            }
            Ok(read)
        })
    }

    fn write_configuration_floor(&self) -> Result<KvLocalRead<Option<KvEntry>>, DfsError> {
        let observer = self.floor_observer.lock().unwrap().take();
        if let Some(observer) = observer {
            observer();
        }
        let inputs = self.inputs.lock().unwrap();
        if inputs.floor_unavailable {
            return Err(DfsError::Network {
                message: "native resident configuration floor unavailable".into(),
            });
        }
        Ok(inputs.floor.clone())
    }

    fn spawn(&self, task: DfsMetadataAuthorityTask) {
        self.spawned.fetch_add(1, Ordering::AcqRel);
        self.tasks.lock().unwrap().push_back(task);
    }

    fn authority_changed(&self) {}
}

fn attach(host: &Arc<NativeProjectionHost>) -> Arc<DfsMetadataAuthorityRuntime> {
    let runtime = Arc::new(DfsMetadataAuthorityRuntime::new());
    assert!(runtime.attach_host(host.clone()));
    runtime
}

fn configuration_delta(revision: u64, entry: Option<KvEntry>) -> ClusterKvCommittedDelta {
    let key = sinomo_core::kv::cluster_kv_raft_config_key();
    ClusterKvCommittedDelta {
        revision: KvRevision(revision),
        compacted_revision: KvRevision::ZERO,
        events: vec![KvEvent {
            revision: KvRevision(revision),
            key: key.clone(),
            kind: entry
                .as_ref()
                .map_or(KvEventKind::Delete, |entry| KvEventKind::Put {
                    value: entry.value.clone(),
                }),
        }]
        .into(),
        final_key_states: vec![ClusterKvCommittedKeyState { key, entry }].into(),
    }
}

#[tokio::test]
async fn write_configuration_runtime_single_local_certificate_activates_then_config_deletion_stays_closed()
 {
    let host = NativeProjectionHost::new(false);
    let runtime = attach(&host);
    assert!(
        runtime.write_configuration_projection().is_err(),
        "an unconfigured floor without a native certificate is unknown"
    );
    assert_eq!(host.read_count(), 0);
    assert_eq!(
        host.queued_count(),
        0,
        "getter cannot start native prefix work"
    );
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert_eq!(
        host.read_count(),
        1,
        "single-local bootstrap requires a successful native filesystem proof"
    );
    assert!(
        matches!(runtime.write_configuration_projection(), Ok(None)),
        "the original native single-local bootstrap path remains available"
    );
    assert_eq!(host.read_count(), 1);
    let config = host.set_configuration(1);
    runtime.observe_delta(&configuration_delta(1, Some(config)));
    assert!(runtime.write_configuration_projection().is_err());
    assert_eq!(host.queued_count(), 1);
    host.run_next().await;
    let view = runtime.write_configuration_projection().unwrap().unwrap();
    assert_eq!(view.revision(), KvRevision(1));
    let reads = host.read_count();
    for _ in 0..3 {
        assert!(Arc::ptr_eq(
            &view,
            &runtime.write_configuration_projection().unwrap().unwrap()
        ));
    }
    assert_eq!(
        host.read_count(),
        reads,
        "warm getters use the published view without prefix IO"
    );
    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.floor.value = None;
        inputs.floor.revision = KvRevision(2);
        inputs.floor.applied_index = 102;
        inputs.authority.revision = KvRevision(2);
        inputs.authority.commit_index = 102;
    }
    runtime.observe_delta(&configuration_delta(2, None));
    assert!(
        runtime.write_configuration_projection().is_err(),
        "once configured, deletion cannot reopen bootstrap compatibility"
    );
    if host.queued_count() > 0 {
        host.run_next().await;
    }
    assert!(runtime.write_configuration_projection().is_err());
    assert_eq!(
        host.queued_count(),
        0,
        "an unchanged unavailable floor is not polled"
    );
}

#[tokio::test]
async fn write_configuration_runtime_bootstrap_rejects_foreign_incomplete_or_nonlocal_proof() {
    for failure in [
        "unknown-floor",
        "foreign-leader",
        "short-applied-floor",
        "wrong-term",
        "nonlocal-quorum",
    ] {
        let host = NativeProjectionHost::new(false);
        {
            let mut inputs = host.inputs.lock().unwrap();
            match failure {
                "unknown-floor" => inputs.floor_unavailable = true,
                "foreign-leader" => {
                    inputs.floor.known_leader = Some(sinomo_core::Id::derived(
                        "write-configuration-runtime",
                        b"foreign",
                    ))
                }
                "short-applied-floor" => inputs.floor.applied_index = 0,
                "wrong-term" => inputs.floor.observed_term = 8,
                "nonlocal-quorum" => {
                    inputs.authority.basis = KvAuthorityBasis::CurrentTermQuorum {
                        voter_acks: 2,
                        required_quorum: 2,
                    }
                }
                _ => unreachable!(),
            }
        }
        let runtime = attach(&host);
        runtime.request_authoritative_refresh();
        host.run_next().await;
        let reads = host.read_count();
        assert!(
            runtime.write_configuration_projection().is_err(),
            "{failure}"
        );
        assert_eq!(
            host.read_count(),
            reads,
            "a rejected getter performs no native prefix IO: {failure}"
        );
        assert_eq!(host.queued_count(), 0);
    }
}

#[tokio::test]
async fn write_configuration_runtime_member_transition_withdraws_cached_bootstrap_certificate() {
    let host = NativeProjectionHost::new(false);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert!(matches!(runtime.write_configuration_projection(), Ok(None)));
    let key = "/cluster/members/new-native-member".to_string();
    let entry = fixture_entry(&key, b"membership-transition".to_vec(), 1);
    runtime.observe_delta(&ClusterKvCommittedDelta {
        revision: KvRevision(1),
        compacted_revision: KvRevision::ZERO,
        events: vec![KvEvent {
            revision: KvRevision(1),
            key: key.clone(),
            kind: KvEventKind::Put {
                value: entry.value.clone(),
            },
        }]
        .into(),
        final_key_states: vec![ClusterKvCommittedKeyState {
            key,
            entry: Some(entry),
        }]
        .into(),
    });
    let reads = host.read_count();
    // The resident floor intentionally still resembles the old local voter.
    // The committed membership transition itself must withdraw that certificate.
    assert!(runtime.write_configuration_projection().is_err());
    assert_eq!(host.read_count(), reads);
    host.discard_queued();
}

#[tokio::test]
async fn write_configuration_runtime_restart_does_not_treat_nonzero_absence_as_bootstrap() {
    let host = NativeProjectionHost::new(false);
    let original = attach(&host);
    original.request_authoritative_refresh();
    host.run_next().await;
    assert!(matches!(
        original.write_configuration_projection(),
        Ok(None)
    ));
    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(5);
        inputs.authority.commit_index = 105;
        inputs.floor.revision = KvRevision(5);
        inputs.floor.applied_index = 105;
    }
    let restarted = attach(&host);
    assert!(restarted.write_configuration_projection().is_err());
    restarted.request_authoritative_refresh();
    host.run_next().await;
    let reads = host.read_count();
    assert!(
        restarted.write_configuration_projection().is_err(),
        "fresh quorum1/1 at nonzero revision cannot prove pristine absent-config bootstrap"
    );
    assert_eq!(host.read_count(), reads);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn write_configuration_runtime_nonzero_absent_snapshot_revokes_pristine_bootstrap() {
    let host = NativeProjectionHost::new(false);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert!(matches!(runtime.write_configuration_projection(), Ok(None)));

    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(5);
        inputs.authority.commit_index = 105;
        inputs.floor.revision = KvRevision(5);
        inputs.floor.applied_index = 105;
    }
    let replacement = KvSnapshot {
        revision: KvRevision(5),
        compacted_revision: KvRevision::ZERO,
        entries: Vec::new(),
        leases: Vec::new(),
        events: Vec::new(),
        committed_requests: Vec::new(),
        expired_requests: Vec::new(),
    };
    runtime.observe_snapshot(&replacement);
    let reads = host.read_count();
    let queued = host.queued_count();
    for _ in 0..3 {
        assert!(
            runtime.write_configuration_projection().is_err(),
            "unchanged filesystem entries cannot prove unseen config put/delete history"
        );
    }
    assert_eq!(host.read_count(), reads, "getter performs no prefix IO");
    assert_eq!(host.queued_count(), queued, "getter schedules no prefix IO");
    host.discard_queued();
}

#[tokio::test]
async fn write_configuration_runtime_contiguous_initialization_preserves_pristine_bootstrap() {
    let host = NativeProjectionHost::new(false);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert!(matches!(runtime.write_configuration_projection(), Ok(None)));
    let filesystem = crate::namespace::distributed_sifs(host.local.clone());
    let config = crate::DfsFileSystemConfig::from(&filesystem);
    let mut provider =
        sinomo_core::service::ServiceInstanceRecord::new("initialized-dfs", host.local.clone());
    provider
        .provider_capabilities
        .push("dfs.object.transfer".into());
    let filesystem_entry = fixture_entry(
        &crate::file_system_config_key(&filesystem.file_system_id),
        encode_kv_value(&config).unwrap(),
        1,
    );
    let provider_entry = fixture_entry(
        &sinomo_core::service::cluster_service_instance_key(&provider.service_id, &host.local),
        encode_kv_value(&provider).unwrap(),
        2,
    );
    let reads = host.read_count();
    for (revision, scope, entry) in [(1, 0, filesystem_entry), (2, 2, provider_entry)] {
        {
            let mut inputs = host.inputs.lock().unwrap();
            inputs.authority.revision = KvRevision(revision);
            inputs.authority.commit_index = revision + 100;
            inputs.floor.revision = KvRevision(revision);
            inputs.floor.applied_index = revision + 100;
            inputs.scopes[scope].push(entry.clone());
        }
        let delta = ClusterKvCommittedDelta {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(revision),
                key: entry.key.clone(),
                kind: KvEventKind::Put {
                    value: entry.value.clone(),
                },
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: entry.key.clone(),
                entry: Some(entry),
            }]
            .into(),
        };
        assert!(runtime.observe_delta(&delta));
        assert!(
            matches!(runtime.write_configuration_projection(), Ok(None)),
            "complete non-authority initialization retains the original pristine certificate"
        );
    }
    assert_eq!(host.read_count(), reads);
    assert_eq!(
        host.queued_count(),
        0,
        "contiguous initialization needs no refresh"
    );
}

#[tokio::test]
async fn write_configuration_runtime_gapped_history_cannot_reopen_bootstrap_after_prefix_refresh() {
    let host = NativeProjectionHost::new(false);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert!(matches!(runtime.write_configuration_projection(), Ok(None)));
    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(3);
        inputs.authority.commit_index = 103;
        inputs.floor.revision = KvRevision(3);
        inputs.floor.applied_index = 103;
    }
    let entry = fixture_entry("/unrelated/telemetry", b"after-gap".to_vec(), 3);
    let gapped = ClusterKvCommittedDelta {
        revision: KvRevision(3),
        compacted_revision: KvRevision::ZERO,
        events: vec![KvEvent {
            revision: KvRevision(3),
            key: entry.key.clone(),
            kind: KvEventKind::Put {
                value: entry.value.clone(),
            },
        }]
        .into(),
        final_key_states: vec![ClusterKvCommittedKeyState {
            key: entry.key.clone(),
            entry: Some(entry),
        }]
        .into(),
    };
    assert!(!runtime.observe_delta(&gapped));
    assert!(runtime.write_configuration_projection().is_err());
    let reads = host.read_count();
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert_eq!(
        host.read_count(),
        reads + 1,
        "the existing metadata-only refresh reads one filesystem prefix"
    );
    assert_eq!(
        runtime.state.current_complete_prefix_revision(),
        Some(KvRevision(3)),
        "the filesystem watermark advances despite missing config history"
    );
    let reads = host.read_count();
    for _ in 0..3 {
        assert!(
            runtime.write_configuration_projection().is_err(),
            "a fresh filesystem certificate cannot recreate the lost pristine anchor"
        );
    }
    assert_eq!(host.read_count(), reads);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn write_configuration_runtime_term_only_floor_advance_queues_one_refresh_and_recovers() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    let original = runtime.write_configuration_projection().unwrap().unwrap();
    let original_generation = runtime.generation();
    let reads = host.read_count();
    assert_eq!(reads, DFS_WRITE_CONFIGURATION_PREFIXES.len());
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(host.queued_count(), 0);

    {
        let mut inputs = host.inputs.lock().unwrap();
        // No committed delta or configuration mutation accompanies this fence.
        // A higher native term alone must retire the old current-term proof.
        inputs.floor.observed_term += 1;
        assert_eq!(inputs.floor.value, original.floor.value);
        assert_eq!(inputs.floor.revision, original.floor.revision);
        assert_eq!(inputs.floor.applied_index, original.floor.applied_index);
        assert_eq!(inputs.scopes, original.scopes);
    }
    for _ in 0..3 {
        assert!(
            runtime.write_configuration_projection().is_err(),
            "a term-only resident advance must reject the old native certificate"
        );
    }
    assert!(runtime.generation() > original_generation);
    assert_eq!(
        host.read_count(),
        reads,
        "rejected getters do not execute native prefix reads"
    );
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(
        host.queued_count(),
        1,
        "repeated floor rejections share one existing refresh owner"
    );

    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.term = inputs.floor.observed_term;
    }
    host.run_next().await;
    let recovered = runtime.write_configuration_projection().unwrap().unwrap();
    assert_eq!(recovered.revision(), original.revision());
    assert_eq!(recovered.scopes, original.scopes);
    assert_eq!(recovered.floor.value, original.floor.value);
    assert!(
        recovered
            .authority
            .iter()
            .all(|proof| proof.term == original.floor.observed_term + 1),
        "recovery needs new native certificates even when all scope data is equal"
    );
    assert_eq!(
        host.read_count(),
        reads + DFS_WRITE_CONFIGURATION_PREFIXES.len()
    );
    let recovered_reads = host.read_count();
    for _ in 0..3 {
        assert!(Arc::ptr_eq(
            &recovered,
            &runtime.write_configuration_projection().unwrap().unwrap()
        ));
    }
    assert_eq!(host.read_count(), recovered_reads);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn write_configuration_runtime_regressed_revision_or_applied_floor_rejects_cached_proof() {
    for failure in ["revision", "applied-index"] {
        let host = NativeProjectionHost::new(true);
        let runtime = attach(&host);
        runtime.request_authoritative_refresh();
        host.run_next().await;
        let original = runtime.write_configuration_projection().unwrap().unwrap();
        let reads = host.read_count();
        {
            let mut inputs = host.inputs.lock().unwrap();
            match failure {
                "revision" => inputs.floor.revision = KvRevision(original.floor.revision.0 - 1),
                "applied-index" => inputs.floor.applied_index = original.floor.applied_index - 1,
                _ => unreachable!(),
            }
            assert_eq!(inputs.floor.value, original.floor.value);
            assert_eq!(inputs.floor.observed_term, original.floor.observed_term);
            assert_eq!(inputs.scopes, original.scopes);
        }
        for _ in 0..3 {
            assert!(
                runtime.write_configuration_projection().is_err(),
                "retained configuration bytes cannot hide a regressed native {failure} floor"
            );
        }
        assert_eq!(host.read_count(), reads);
        assert_eq!(host.spawned.load(Ordering::Acquire), 2);
        assert_eq!(host.queued_count(), 1);
        host.inputs.lock().unwrap().floor = original.floor.clone();
        host.run_next().await;
        let recovered = runtime.write_configuration_projection().unwrap().unwrap();
        assert_eq!(recovered.revision(), original.revision());
        assert_eq!(recovered.scopes, original.scopes);
        assert_eq!(
            host.read_count(),
            reads + DFS_WRITE_CONFIGURATION_PREFIXES.len()
        );
        let recovered_reads = host.read_count();
        assert!(Arc::ptr_eq(
            &recovered,
            &runtime.write_configuration_projection().unwrap().unwrap()
        ));
        assert_eq!(host.read_count(), recovered_reads);
        assert_eq!(host.spawned.load(Ordering::Acquire), 2);
        assert_eq!(host.queued_count(), 0);
    }
}

#[tokio::test]
async fn write_configuration_runtime_inflight_invalidation_queues_one_refresh_handoff() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let first = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .expect("content read reached the controlled native future");
    let changed = host.set_configuration(11);
    let delta = configuration_delta(11, Some(changed));
    runtime.observe_delta(&delta);
    for _ in 0..3 {
        runtime.request_authoritative_refresh();
    }
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(
        host.queued_count(),
        0,
        "replacement waits until the current owner releases"
    );
    assert!(runtime.write_configuration_projection().is_err());
    host.content_gate.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(
        host.queued_count(),
        1,
        "one invalidated owner hands off one coalesced replacement"
    );
    host.run_next().await;
    let view = runtime.write_configuration_projection().unwrap().unwrap();
    assert_eq!(view.revision(), KvRevision(11));
    let reads = host.read_count();
    runtime.observe_delta(&delta);
    for _ in 0..3 {
        assert!(runtime.write_configuration_projection().unwrap().is_some());
    }
    assert_eq!(host.read_count(), reads);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(
        host.queued_count(),
        0,
        "identical revisions do not create a polling loop"
    );
}

#[tokio::test]
async fn write_configuration_runtime_cancelled_owner_hands_off_one_coalesced_refresh() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let first = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .expect("native content read reached its cancellation point");
    for _ in 0..3 {
        runtime.request_authoritative_refresh();
    }
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(host.queued_count(), 0);
    first.abort();
    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(1), first)
        .await
        .unwrap()
        .expect_err("the old native read owner was cancelled");
    assert!(cancelled.is_cancelled());
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(
        host.queued_count(),
        1,
        "cancellation releases bootstrap buffering and transfers one queued edge"
    );
    host.run_next().await;
    let view = runtime.write_configuration_projection().unwrap().unwrap();
    assert_eq!(view.revision(), KvRevision(10));
    let reads = host.read_count();
    assert!(runtime.write_configuration_projection().unwrap().is_some());
    assert_eq!(host.read_count(), reads);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(host.queued_count(), 0);
}

async fn require_pending<F: std::future::Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|context| {
        assert!(
            std::future::Future::poll(future.as_mut(), context).is_pending(),
            "an intermediate refresh handoff must keep the original waiter pending"
        );
        std::task::Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn write_configuration_same_generation_waiters_join_latched_refresh_successor() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let first = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .unwrap();
    let generation = runtime.generation();
    runtime.require_authoritative_revision(KvRevision(11));
    let changed = host.set_configuration(11);
    runtime.observe_delta(&configuration_delta(11, Some(changed)));
    assert_eq!(runtime.generation(), generation);
    let mut metadata = Box::pin(runtime.ensure_complete_prefix_for_generation_async(
        generation,
        #[cfg(test)]
        None,
    ));
    let mut configuration = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(metadata.as_mut()).await;
    require_pending(configuration.as_mut()).await;
    host.content_gate.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(host.queued_count(), 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    require_pending(metadata.as_mut()).await;
    require_pending(configuration.as_mut()).await;
    host.run_next().await;
    assert_eq!(metadata.await, Some(KvRevision(11)));
    configuration.await.unwrap();
    assert_eq!(runtime.generation(), generation);
    let reads = host.read_count();
    runtime
        .ensure_write_configuration_ready_async()
        .await
        .unwrap();
    assert_eq!(
        runtime
            .ensure_complete_prefix_for_generation_async(
                generation,
                #[cfg(test)]
                None,
            )
            .await,
        Some(KvRevision(11))
    );
    assert_eq!(
        host.read_count(),
        reads,
        "ready successors add no native prefix reads"
    );
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn write_configuration_terminal_native_failure_does_not_retry_shared_owner() {
    let host = NativeProjectionHost::new(true);
    // The native host's malformed scope must be rejected before any retained
    // filesystem proof or configuration readiness can be published.
    host.inputs.lock().unwrap().scopes[0].push(fixture_entry("/outside/dfs-scope", vec![1], 10));
    let runtime = attach(&host);
    let generation = runtime.generation();
    assert_eq!(
        runtime
            .ensure_complete_prefix_for_generation_async(
                generation,
                #[cfg(test)]
                None,
            )
            .await,
        None
    );
    assert_eq!(runtime.generation(), generation);
    assert_eq!(host.read_count(), 1);
    assert_eq!(host.queued_count(), 0);
    assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));

    let host = NativeProjectionHost::new(true);
    host.inputs.lock().unwrap().floor_unavailable = true;
    let runtime = attach(&host);
    let mut configuration = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(configuration.as_mut()).await;
    assert_eq!(host.queued_count(), 1);
    host.run_next().await;
    assert!(configuration.await.is_err());
    assert_eq!(host.read_count(), 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(
        host.queued_count(),
        0,
        "a native floor failure is terminal, not another refresh edge"
    );
}

#[tokio::test]
async fn write_configuration_cancelled_bootstrap_missing_view_joins_one_needed_owner() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let first = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));
    assert!(
        !runtime
            .configuration_refresh_in_flight
            .load(Ordering::Acquire)
    );
    assert_eq!(host.queued_count(), 0);
    assert!(matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    ));
    assert!(
        runtime
            .retained_complete_prefix_revision_for_generation(runtime.generation())
            .is_some()
    );
    let mut configuration = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(configuration.as_mut()).await;
    assert_eq!(host.queued_count(), 1);
    host.run_next().await;
    configuration.await.unwrap();
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    let reads = host.read_count();
    runtime
        .ensure_write_configuration_ready_async()
        .await
        .unwrap();
    assert_eq!(host.read_count(), reads);
}

#[tokio::test]
async fn write_configuration_gap_diagnostic_reports_only_the_actual_proof_withdrawal() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    let key = "/unrelated/gap";
    let delta = |revision| ClusterKvCommittedDelta {
        revision: KvRevision(revision),
        compacted_revision: KvRevision::ZERO,
        events: vec![KvEvent {
            revision: KvRevision(revision),
            key: key.into(),
            kind: KvEventKind::Put { value: vec![1] },
        }]
        .into(),
        final_key_states: vec![ClusterKvCommittedKeyState {
            key: key.into(),
            entry: Some(fixture_entry(key, vec![1], revision)),
        }]
        .into(),
    };
    let first = runtime.observe_delta_observation(&delta(12));
    assert!(!first.accepted);
    assert!(first.proof_withdrawn);
    let pending = runtime.observe_delta_observation(&delta(13));
    assert!(!pending.accepted);
    assert!(!pending.proof_withdrawn);
    assert_eq!(host.queued_count(), 1);
    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(13);
        inputs.authority.commit_index = 113;
        inputs.floor.revision = KvRevision(13);
        inputs.floor.applied_index = 113;
    }
    host.run_next().await;
    runtime
        .ensure_write_configuration_ready_async()
        .await
        .unwrap();
    assert!(
        runtime
            .retained_complete_prefix_revision_for_generation(runtime.generation())
            .is_some()
    );
}

#[tokio::test]
async fn write_configuration_pending_bundle_publication_wakes_existing_readiness_waiter() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    let inputs = host.inputs.lock().unwrap().clone();
    let mut reads: [KvAuthoritativeRead<Vec<KvEntry>>; 3] =
        std::array::from_fn(|scope| KvAuthoritativeRead {
            value: inputs.scopes[scope].clone(),
            authority: inputs.authority,
        });
    reads[1].authority.revision = KvRevision(11);
    reads[1].authority.commit_index = 111;
    reads[2].authority.revision = KvRevision(11);
    reads[2].authority.commit_index = 111;
    let mut floor = inputs.floor;
    floor.revision = KvRevision(11);
    floor.applied_index = 111;
    host.inputs.lock().unwrap().floor = floor.clone();
    assert!(
        runtime.install_authoritative_projection(
            DfsAuthoritativePrefixProjection::complete(
                DFS_METADATA_AUTHORITY_PREFIX,
                reads[0].value.clone(),
                KvRevision(10),
            )
            .unwrap()
        )
    );
    {
        let mut configuration = runtime.write_configuration.lock().unwrap();
        let token = configuration.begin_refresh();
        assert!(
            !configuration
                .install_bootstrap(token, reads, floor)
                .unwrap()
        );
    }
    let mut waiter = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(waiter.as_mut()).await;
    assert_eq!(host.spawned.load(Ordering::Acquire), 0);
    let mut entry = inputs.scopes[0][0].clone();
    entry.modify_revision = KvRevision(11);
    entry.version = KvVersion(2);
    assert!(
        runtime.observe_delta(&ClusterKvCommittedDelta {
            revision: KvRevision(11),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(11),
                key: entry.key.clone(),
                kind: KvEventKind::Put {
                    value: entry.value.clone()
                }
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: entry.key.clone(),
                entry: Some(entry)
            }]
            .into(),
        })
    );
    waiter.await.unwrap();
    assert_eq!(host.read_count(), 0);
    assert_eq!(
        host.spawned.load(Ordering::Acquire),
        0,
        "a coherent delta finishes the existing bundle without a new ReadIndex"
    );
}

#[tokio::test]
async fn write_configuration_refresh_release_waits_for_token_in_both_cancellation_drop_orders() {
    for outer_first in [false, true] {
        let host = NativeProjectionHost::new(true);
        let runtime = attach(&host);
        let owner = runtime.try_claim_refresh().unwrap();
        let token = runtime.write_configuration.lock().unwrap().begin_refresh();
        runtime
            .configuration_refresh_in_flight
            .store(true, Ordering::Release);
        let configuration_owner = DfsWriteConfigurationRefreshOwner {
            runtime: Arc::clone(&runtime),
            token,
        };
        let completion = runtime.refresh_completion.subscribe();
        runtime.request_authoritative_refresh();
        if outer_first {
            drop(owner);
            assert!(
                runtime.refresh_in_flight.load(Ordering::Acquire),
                "singleflight remains reserved until the cancelled token is released"
            );
            assert!(!completion.has_changed().unwrap());
            assert_eq!(host.queued_count(), 0);
            drop(configuration_owner);
        } else {
            drop(configuration_owner);
            assert!(runtime.refresh_in_flight.load(Ordering::Acquire));
            assert!(!completion.has_changed().unwrap());
            assert_eq!(host.queued_count(), 0);
            drop(owner);
        }
        assert!(completion.has_changed().unwrap());
        assert!(
            runtime.refresh_in_flight.load(Ordering::Acquire),
            "latched successor is reserved before waiter completion"
        );
        assert_eq!(host.queued_count(), 1);
        host.run_next().await;
        runtime
            .ensure_write_configuration_ready_async()
            .await
            .unwrap();
        assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));
    }
}

#[tokio::test]
async fn write_configuration_background_warm_budget_is_not_restarted_by_owner_handoff() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let first = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .unwrap();
    runtime.require_authoritative_revision(KvRevision(11));
    let changed = host.set_configuration(11);
    runtime.observe_delta(&configuration_delta(11, Some(changed)));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(15);
    let mut warm = Box::pin(
        crate::runtime_host::warm_daemon_dfs_metadata_authority_before_deadline(
            Arc::clone(&runtime),
            deadline,
            #[cfg(test)]
            None,
        ),
    );
    require_pending(warm.as_mut()).await;
    // Exhaust the caller's original budget while its first owner is blocked.
    // The successor must inherit the expired budget, not start another wait.
    tokio::time::sleep_until(deadline).await;
    host.content_gate.release.notify_one();
    first.await.unwrap();
    assert_eq!(host.queued_count(), 1);
    let reads = host.read_count();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), warm)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("projection deadline elapsed"));
    assert_eq!(host.read_count(), reads);
    assert_eq!(
        host.queued_count(),
        1,
        "timing out a waiter must not cancel the component's successor owner"
    );
    host.run_next().await;
    let reads = host.read_count();
    crate::runtime_host::warm_daemon_dfs_metadata_authority_before_deadline(
        Arc::clone(&runtime),
        tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        #[cfg(test)]
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        host.read_count(),
        reads,
        "a ready follow-up remains retained-only"
    );
}

#[tokio::test]
async fn write_configuration_background_warm_timeout_releases_its_owned_bootstrap_token() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(15);
    let mut warm = Box::pin(
        crate::runtime_host::warm_daemon_dfs_metadata_authority_before_deadline(
            Arc::clone(&runtime),
            deadline,
            #[cfg(test)]
            None,
        ),
    );
    require_pending(warm.as_mut()).await;
    host.content_gate.entered.notified().await;
    let error = warm.await.unwrap_err();
    assert!(error.to_string().contains("projection deadline elapsed"));
    assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));
    assert!(
        !runtime
            .configuration_refresh_in_flight
            .load(Ordering::Acquire)
    );
    assert!(!runtime.refresh_release_pending.load(Ordering::Acquire));
    assert_eq!(host.queued_count(), 0);
    let mut readiness = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(readiness.as_mut()).await;
    assert_eq!(host.queued_count(), 1);
    host.run_next().await;
    readiness.await.unwrap();
    assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));
}

#[tokio::test]
async fn equal_revision_discontinuity_withdraws_proofs_once_and_recovers() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert_eq!(
        runtime
            .write_configuration_projection()
            .unwrap()
            .unwrap()
            .revision(),
        KvRevision(10)
    );
    let generation = runtime.generation();
    let storage_generation = runtime.storage_generation();
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(generation),
        Some(KvRevision(10))
    );

    let gap = runtime.observe_discontinuity(KvRevision(10));
    assert!(!gap.accepted);
    assert!(gap.proof_withdrawn);
    assert!(gap.authority_changed);
    assert_eq!(runtime.generation(), generation + 1);
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        None,
        "an unchanged revision cannot certify unseen committed observation history"
    );
    assert!(
        runtime
            .write_configuration
            .lock()
            .unwrap()
            .snapshot()
            .is_none()
    );
    assert!(matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    ));
    assert_eq!(host.queued_count(), 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);

    let repeated = runtime.observe_discontinuity(KvRevision(10));
    assert!(!repeated.accepted);
    assert!(!repeated.proof_withdrawn);
    assert!(!repeated.authority_changed);
    assert_eq!(runtime.generation(), generation + 1);
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.queued_count(), 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);

    host.run_next().await;
    assert_eq!(
        runtime
            .write_configuration_projection()
            .unwrap()
            .unwrap()
            .revision(),
        KvRevision(10)
    );
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        Some(KvRevision(10))
    );
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.queued_count(), 0);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn discontinuity_during_native_read_rejects_old_bootstrap_and_hands_off() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    let storage_generation = runtime.storage_generation();
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let first = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .expect("old native bootstrap reached its controlled content read");
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        Some(KvRevision(10))
    );

    let gap = runtime.observe_discontinuity(KvRevision(10));
    assert!(!gap.accepted);
    assert!(gap.proof_withdrawn);
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(host.queued_count(), 0);
    assert!(
        runtime
            .write_configuration
            .lock()
            .unwrap()
            .snapshot()
            .is_none()
    );
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        None
    );

    host.content_gate.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), first)
        .await
        .expect("old owner completed after the controlled native read released")
        .unwrap();
    assert!(
        runtime
            .write_configuration
            .lock()
            .unwrap()
            .snapshot()
            .is_none()
    );
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        None,
        "the old read cannot restore filesystem proof after an equal-revision gap"
    );
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(
        host.queued_count(),
        1,
        "the existing owner hands off one fresh native job"
    );

    host.run_next().await;
    assert_eq!(
        runtime
            .write_configuration_projection()
            .unwrap()
            .unwrap()
            .revision(),
        KvRevision(10)
    );
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        Some(KvRevision(10))
    );
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn terminal_native_failure_after_discontinuity_does_not_spin() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert!(runtime.write_configuration_projection().unwrap().is_some());
    let storage_generation = runtime.storage_generation();
    let reads = host.read_count();
    host.inputs.lock().unwrap().floor_unavailable = true;

    assert!(
        runtime
            .observe_discontinuity(KvRevision(10))
            .proof_withdrawn
    );
    let mut readiness = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(readiness.as_mut()).await;
    assert_eq!(host.queued_count(), 1);
    host.run_next().await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), readiness)
        .await
        .expect("the existing waiter receives the terminal native failure")
        .expect_err("an unavailable native floor cannot restore configuration proof");
    assert!(matches!(error, DfsError::Network { .. }));
    assert!(
        runtime
            .write_configuration
            .lock()
            .unwrap()
            .snapshot()
            .is_none()
    );
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.read_count(), reads + 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    assert_eq!(host.queued_count(), 0);
    assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));
    assert!(
        !runtime
            .configuration_refresh_in_flight
            .load(Ordering::Acquire)
    );
    tokio::task::yield_now().await;
    assert_eq!(
        host.queued_count(),
        0,
        "terminal failure produces no autonomous refresh loop"
    );
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn zero_revision_discontinuity_requires_fresh_native_bootstrap_certificate() {
    let host = NativeProjectionHost::new(false);
    let runtime = attach(&host);
    let mut initial = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(initial.as_mut()).await;
    assert_eq!(host.queued_count(), 1);
    host.run_next().await;
    initial.await.unwrap();
    assert!(
        runtime
            .single_node_bootstrap_authority
            .lock()
            .unwrap()
            .is_some()
    );
    assert!(matches!(runtime.write_configuration_projection(), Ok(None)));
    assert_eq!(host.read_count(), 1);
    let storage_generation = runtime.storage_generation();

    let gap = runtime.observe_discontinuity(KvRevision::ZERO);
    assert!(!gap.accepted);
    assert!(gap.proof_withdrawn);
    assert!(
        runtime
            .single_node_bootstrap_authority
            .lock()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        runtime.retained_complete_prefix_revision_for_generation(runtime.generation()),
        None
    );
    assert!(runtime.write_configuration_projection().is_err());
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.queued_count(), 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);

    let repeated = runtime.observe_discontinuity(KvRevision::ZERO);
    assert!(!repeated.accepted);
    assert!(!repeated.proof_withdrawn);
    assert!(
        runtime
            .single_node_bootstrap_authority
            .lock()
            .unwrap()
            .is_none()
    );
    assert_eq!(host.queued_count(), 1);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
    let mut recovery = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(recovery.as_mut()).await;
    assert_eq!(
        host.read_count(),
        1,
        "cached access cannot recreate native absence proof"
    );

    host.run_next().await;
    recovery.await.unwrap();
    assert!(
        runtime
            .single_node_bootstrap_authority
            .lock()
            .unwrap()
            .is_some()
    );
    assert!(matches!(runtime.write_configuration_projection(), Ok(None)));
    assert_eq!(
        host.read_count(),
        2,
        "the successor obtains a fresh native ReadIndex and floor proof"
    );
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.queued_count(), 0);
    assert_eq!(host.spawned.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn required_revision_withdraws_configuration_before_queued_refresh_runs() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    let before = runtime.write_configuration_projection().unwrap().unwrap();
    assert_eq!(before.revision, KvRevision(10));
    let storage_generation = runtime.storage_generation();
    let reads = host.read_count();
    let spawned = host.spawned.load(Ordering::Acquire);
    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(11);
        inputs.authority.commit_index = 111;
        inputs.floor.revision = KvRevision(11);
        inputs.floor.applied_index = 111;
        let mut content = crate::DfsNodeAdvertisement::content(host.local.clone());
        content.zone = Some("new-authoritative-zone".into());
        inputs.scopes[1] = vec![fixture_entry(
            &crate::content_service_key(&host.local),
            encode_kv_value(&content).unwrap(),
            11,
        )];
    }
    // The normal owner is queued but has not yet polled its refresh body. A
    // synchronous caller must not open on the previous configuration here.
    runtime.require_authoritative_revision(KvRevision(11));
    assert_eq!(runtime.state.current_complete_prefix_revision(), None);
    assert!(matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    ));
    assert_eq!(host.read_count(), reads);
    assert_eq!(host.queued_count(), 1);
    let mut ready = Box::pin(runtime.ensure_write_configuration_ready_async());
    require_pending(ready.as_mut()).await;
    assert_eq!(host.spawned.load(Ordering::Acquire), spawned + 1);
    host.run_next().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), ready)
        .await
        .unwrap()
        .unwrap();
    let current = runtime.write_configuration_projection().unwrap().unwrap();
    assert_eq!(current.revision, KvRevision(11));
    assert_eq!(
        runtime.state.current_complete_prefix_revision(),
        Some(KvRevision(11))
    );
    assert_eq!(
        current.content_advertisements().unwrap()[0].zone.as_deref(),
        Some("new-authoritative-zone")
    );
    assert_eq!(host.read_count(), reads + 3);
    assert_eq!(host.queued_count(), 0);
    assert_eq!(runtime.storage_generation(), storage_generation);
}

#[tokio::test]
async fn native_host_preflight_joins_configuration_after_first_prefix_is_published() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let refresh = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        runtime.state.current_complete_prefix_revision(),
        Some(KvRevision(10))
    );
    assert!(matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    ));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    let mut preflight = Box::pin(
        crate::runtime_host::warm_daemon_dfs_metadata_authority_before_deadline(
            Arc::clone(&runtime),
            deadline,
            None,
        ),
    );
    require_pending(preflight.as_mut()).await;
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    host.content_gate.release.notify_one();
    refresh.await.unwrap();
    let _admitted = tokio::time::timeout_at(deadline, preflight)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        runtime.state.current_complete_prefix_revision(),
        Some(KvRevision(10))
    );
    assert!(matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Ready(Some(_))
    ));
    assert_eq!(host.read_count(), 3);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn admission_fence_rejects_partial_configuration_after_same_generation_refresh() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    let generation = runtime.generation();
    assert!(crate::runtime_host::test_retained_authority_admission_is_ready(&runtime, generation));
    {
        let mut inputs = host.inputs.lock().unwrap();
        inputs.authority.revision = KvRevision(11);
        inputs.authority.commit_index = 111;
        inputs.floor.revision = KvRevision(11);
        inputs.floor.applied_index = 111;
    }
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.require_authoritative_revision(KvRevision(11));
    let refresh = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .unwrap();
    let paused_generation = runtime.generation();
    let prefix = runtime.state.current_complete_prefix_revision();
    let pending = matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    );
    let admitted =
        crate::runtime_host::test_retained_authority_admission_is_ready(&runtime, generation);
    // Drain the original shared owner before any diagnostic assertion fails.
    host.content_gate.release.notify_one();
    refresh.await.unwrap();
    assert_eq!(paused_generation, generation);
    assert_eq!(prefix, Some(KvRevision(11)));
    assert!(pending);
    assert!(
        !admitted,
        "metadata-prefix readiness alone must not admit execution while the shared configuration is incomplete"
    );
    assert!(
        crate::runtime_host::test_retained_authority_admission_is_ready(
            &runtime,
            runtime.generation()
        )
    );
    assert_eq!(host.read_count(), 6);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn configuration_published_between_pending_read_and_waiter_check_is_revalidated() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    let original = runtime.write_configuration_projection().unwrap().unwrap();
    let generation = runtime.generation();
    let reads = host.read_count();
    let publications = host.spawned.load(Ordering::Acquire);
    // Retain the actual three-prefix native publication. Deliver it at the
    // precise read/check boundary, as the existing refresh owner can do.
    let published = Mutex::new(Some(std::mem::take(
        &mut *runtime.write_configuration.lock().unwrap(),
    )));
    let publish = || {
        if let Some(configuration) = published.lock().unwrap().take() {
            *runtime.write_configuration.lock().unwrap() = configuration;
        }
    };
    let result = runtime
        .ensure_write_configuration_ready_with_pending_observer(Some(&publish))
        .await;
    assert!(published.lock().unwrap().is_none());
    assert_eq!(host.read_count(), reads);
    assert_eq!(host.spawned.load(Ordering::Acquire), publications);
    assert_eq!(runtime.generation(), generation);
    assert!(Arc::ptr_eq(
        &original,
        &runtime.write_configuration_projection().unwrap().unwrap()
    ));
    result.expect(
        "a concurrently completed native publication must replace the earlier pending observation",
    );
}

#[tokio::test]
async fn retained_control_client_joins_configuration_after_early_filesystem_prefix() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    host.content_gate.armed.store(true, Ordering::Release);
    runtime.request_authoritative_refresh();
    let refresh = tokio::spawn(host.take_task().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.content_gate.entered.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        runtime.state.current_complete_prefix_revision(),
        Some(KvRevision(10))
    );
    assert!(matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    ));
    let directory = sinomo_testdir::TestDir::new("retained-client-configuration-boundary");
    let paths = sinomo_core::network::ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(
        directory.path(),
    ));
    let mut synchronization = Box::pin(
        crate::runtime_host::test_retained_client_configuration_sync(Arc::clone(&runtime), &paths),
    );
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(synchronization.as_mut().poll(context))
    })
    .await;
    // Drain the original native-proof owner before asserting the boundary.
    host.content_gate.release.notify_one();
    refresh.await.unwrap();
    assert!(
        first_poll.is_pending(),
        "a required configuration client cannot return after only the filesystem prefix: {first_poll:?}"
    );
    synchronization.await.unwrap();
    crate::runtime_host::test_retained_client_configuration_sync(Arc::clone(&runtime), &paths)
        .await
        .unwrap();
    host.inputs.lock().unwrap().floor_unavailable = true;
    assert!(
        crate::runtime_host::test_retained_client_configuration_sync(Arc::clone(&runtime), &paths)
            .await
            .is_err(),
        "an unavailable native floor remains terminal on a warm client"
    );
    assert_eq!(host.read_count(), 3);
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn queued_refresh_preserves_configuration_completed_by_committed_delta() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    let inputs = host.inputs.lock().unwrap().clone();
    let mut reads: [KvAuthoritativeRead<Vec<KvEntry>>; 3] =
        std::array::from_fn(|scope| KvAuthoritativeRead {
            value: inputs.scopes[scope].clone(),
            authority: inputs.authority,
        });
    for read in &mut reads[1..] {
        read.authority.revision = KvRevision(11);
        read.authority.commit_index = 111;
    }
    let mut floor = inputs.floor;
    floor.revision = KvRevision(11);
    floor.applied_index = 111;
    host.inputs.lock().unwrap().floor = floor.clone();
    assert!(
        runtime.install_authoritative_projection(
            DfsAuthoritativePrefixProjection::complete(
                DFS_METADATA_AUTHORITY_PREFIX,
                reads[0].value.clone(),
                KvRevision(10)
            )
            .unwrap()
        )
    );
    {
        let mut configuration = runtime.write_configuration.lock().unwrap();
        let token = configuration.begin_refresh();
        assert!(
            !configuration
                .install_bootstrap(token, reads, floor)
                .unwrap()
        );
    }
    // Queue the real singleflight while the complete native bundle still
    // awaits its filesystem delta. Do not poll this owner until that delta
    // has actually published the already-validated bundle.
    runtime.require_authoritative_revision(KvRevision(11));
    assert_eq!(host.queued_count(), 1);
    let mut entry = inputs.scopes[0][0].clone();
    entry.modify_revision = KvRevision(11);
    entry.version = KvVersion(2);
    {
        let mut current = host.inputs.lock().unwrap();
        current.authority.revision = KvRevision(11);
        current.authority.commit_index = 111;
        current.scopes[0][0] = entry.clone();
    }
    let observation = runtime.observe_delta_observation(&ClusterKvCommittedDelta {
        revision: KvRevision(11),
        compacted_revision: KvRevision::ZERO,
        events: vec![KvEvent {
            revision: KvRevision(11),
            key: entry.key.clone(),
            kind: KvEventKind::Put {
                value: entry.value.clone(),
            },
        }]
        .into(),
        final_key_states: vec![ClusterKvCommittedKeyState {
            key: entry.key.clone(),
            entry: Some(entry),
        }]
        .into(),
    });
    assert!(
        !observation.accepted,
        "the withdrawn standalone prefix requires the full bundle rebind"
    );
    assert_eq!(
        runtime.state.current_complete_prefix_revision(),
        Some(KvRevision(11))
    );
    let ready = runtime.write_configuration_projection().unwrap().unwrap();
    let generation = runtime.generation();
    let storage_generation = runtime.storage_generation();
    let reads_before = host.read_count();
    host.content_gate.armed.store(true, Ordering::Release);
    let mut refresh = host.take_task().unwrap();
    let first_poll =
        std::future::poll_fn(|context| std::task::Poll::Ready(refresh.as_mut().poll(context)))
            .await;
    let remained_ready = matches!(
        runtime.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Ready(Some(_))
    );
    // Drain a regressing implementation before failing its assertion.
    if first_poll.is_pending() {
        host.content_gate.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), refresh)
            .await
            .unwrap();
    }
    assert!(
        remained_ready,
        "a delayed refresh owner must not withdraw the configuration already completed by its committed delta"
    );
    assert!(first_poll.is_ready());
    assert!(Arc::ptr_eq(
        &ready,
        &runtime.write_configuration_projection().unwrap().unwrap()
    ));
    assert_eq!(host.read_count(), reads_before);
    assert_eq!(runtime.generation(), generation);
    assert_eq!(runtime.storage_generation(), storage_generation);
    assert_eq!(host.spawned.load(Ordering::Acquire), 1);
    assert_eq!(host.queued_count(), 0);
}

#[tokio::test]
async fn retained_control_client_revalidates_a_ready_generation_changed_during_floor_read() {
    let host = NativeProjectionHost::new(true);
    let runtime = attach(&host);
    runtime.request_authoritative_refresh();
    host.run_next().await;
    assert_eq!(
        runtime
            .write_configuration_projection()
            .unwrap()
            .unwrap()
            .revision(),
        KvRevision(10)
    );
    let generation = runtime.generation();
    let weak_host = Arc::downgrade(&host);
    let weak_runtime = Arc::downgrade(&runtime);
    *host.floor_observer.lock().unwrap() = Some(Box::new(move || {
        let host = weak_host.upgrade().unwrap();
        let runtime = weak_runtime.upgrade().unwrap();
        let mut advertisement = crate::DfsNodeAdvertisement::content(host.local.clone());
        advertisement.zone = Some("updated-during-floor-read".into());
        let entry = fixture_entry(
            &crate::content_service_key(&host.local),
            encode_kv_value(&advertisement).unwrap(),
            11,
        );
        {
            let mut inputs = host.inputs.lock().unwrap();
            inputs.authority.revision = KvRevision(11);
            inputs.authority.commit_index = 111;
            inputs.floor.revision = KvRevision(11);
            inputs.floor.applied_index = 111;
            inputs.scopes[1] = vec![entry.clone()];
        }
        runtime.observe_delta(&ClusterKvCommittedDelta {
            revision: KvRevision(11),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(11),
                key: entry.key.clone(),
                kind: KvEventKind::Put {
                    value: entry.value.clone(),
                },
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: entry.key.clone(),
                entry: Some(entry),
            }]
            .into(),
        });
        assert!(runtime.generation() > generation);
    }));
    let directory = sinomo_testdir::TestDir::new("retained-client-ready-generation-change");
    let paths = sinomo_core::network::ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(
        directory.path(),
    ));
    let view = crate::runtime_host::test_retained_client_configuration_projection(
        Arc::clone(&runtime),
        &paths,
    )
    .expect(
        "a complete changed generation must be revalidated within the existing local replan bound",
    )
    .unwrap();
    assert_eq!(view.revision(), KvRevision(11));
    let entry = &view.entries(DFS_WRITE_CONFIGURATION_PREFIXES[1]).unwrap()[0];
    let advertisement: crate::DfsNodeAdvertisement = entry.decode().unwrap();
    assert_eq!(
        advertisement.zone.as_deref(),
        Some("updated-during-floor-read")
    );
    assert_eq!(
        host.read_count(),
        3,
        "revalidation uses the already-owned complete bundle"
    );
    assert_eq!(host.queued_count(), 0);
    host.inputs.lock().unwrap().floor_unavailable = true;
    assert!(
        crate::runtime_host::test_retained_client_configuration_projection(runtime, &paths)
            .is_err()
    );
}
