//! Shared opt-in capture of facts returned by the normal mounted DFS kernel.
//! These raw observations do not assert refinement or grant effect authority.
use crate::metadata_backend::prepared_operation::NativeHeldMetadataSnapshotV1;
use crate::runtime_host::DfsDataOwnerLock;
use crate::{
    DfsMetadataJournalEntry, DfsMetadataShardId, DfsPreparedOperationDigest, MetadataBackendError,
};
use serde::Serialize;
use std::cell::RefCell;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::watch;

pub(crate) const MAX_PACKETS: usize = 4096;
pub(crate) const MAX_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "observation", content = "value", rename_all = "kebab-case")]
pub enum ObservedNative<T> {
    NotObserved,
    Absent,
    Present(T),
}
impl<T> From<Option<T>> for ObservedNative<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::Present(value),
            None => Self::Absent,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum KernelPhase {
    PrepareDurable,
    PrepareMaterialized,
    CommitDurable,
    CommitMaterialized,
    ExactPreparedReplay,
    ExactCommittedReplay,
}
#[derive(Debug, Serialize)]
struct Packet {
    owner_label: String,
    owner_ordinal: String,
    operation: DfsPreparedOperationDigest,
    phase: KernelPhase,
    before_held_json: Vec<u8>,
    after_held_json: Vec<u8>,
    exact_entry: Option<DfsMetadataJournalEntry>,
    exact_frame: Option<Vec<u8>>,
}
#[derive(Debug, Serialize)]
struct NativeAuthoritativeRead {
    owner_label: String,
    owner_ordinal: String,
    requested_keys: Vec<String>,
    read: sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>,
    accepted_projection_generation: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct NativeMountedIdentityV1 {
    pub cluster_id: sinomo_core::node::ClusterId,
    pub local_node_id: sinomo_core::node::NodeId,
    pub observed_term: String,
    pub observed_kv_revision: String,
    /// Normal mount's cached configuration facts; alone not a Raft quorum proof.
    pub configured_members: Vec<NativeMountedMemberV1>,
}
#[derive(Clone, Debug, Serialize)]
pub struct NativeMountedMemberV1 {
    pub node_id: sinomo_core::node::NodeId,
    pub role: sinomo_core::ClusterMemberRole,
    pub state: sinomo_core::ClusterMemberState,
}
struct Mount {
    root: PathBuf,
    label: String,
    shutdown: watch::Receiver<bool>,
    owner: Weak<DfsDataOwnerLock>,
    identity: Mutex<Option<NativeMountedIdentityV1>>,
    cluster_paths: sinomo_core::network::ClusterPaths,
}
static MOUNTS: OnceLock<Mutex<Vec<Weak<Mount>>>> = OnceLock::new();
static CAPTURES: OnceLock<Mutex<Vec<Weak<Capture>>>> = OnceLock::new();
pub(crate) struct NativeTraceMountRegistration(Arc<Mount>);
/// Called only with the ordinary component's existing owner and shutdown channel.
pub(crate) fn register_mount(
    root: PathBuf,
    shutdown: watch::Receiver<bool>,
    owner: &Arc<DfsDataOwnerLock>,
    cluster_paths: &sinomo_core::network::ClusterPaths,
) -> NativeTraceMountRegistration {
    let mount = Arc::new(Mount {
        root,
        label: format!("native-mount:{}", sinomo_core::Id::generate()),
        shutdown,
        owner: Arc::downgrade(owner),
        identity: Mutex::new(None),
        cluster_paths: cluster_paths.clone(),
    });
    MOUNTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Arc::downgrade(&mount));
    if let Some(requests) = REQUESTS.get() {
        let mut requests = requests.lock().unwrap_or_else(|e| e.into_inner());
        requests.retain(|request| request.strong_count() != 0);
        for request in requests
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|request| request.root == mount.root)
        {
            let mut selected = request.capture.lock().unwrap_or_else(|e| e.into_inner());
            if selected.is_some() {
                continue;
            }
            let capture = new_capture(Arc::clone(&mount), request.shard.clone());
            CAPTURES
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Arc::downgrade(&capture));
            *selected = Some(capture);
        }
    }
    NativeTraceMountRegistration(mount)
}
impl NativeTraceMountRegistration {
    /// Forward the SAME normal lifecycle read. This issues no new read and
    /// does not promote a cached member projection into voting authority.
    pub(crate) fn observe_mounted_identity(&self, state: &sinomo_core::ClusterBootstrapState) {
        *self.0.identity.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(NativeMountedIdentityV1 {
                cluster_id: state.cluster_id.clone(),
                local_node_id: state.local_node_id.clone(),
                observed_term: state.term.to_string(),
                observed_kv_revision: state.kv_revision.to_string(),
                configured_members: state
                    .members
                    .iter()
                    .map(|member| NativeMountedMemberV1 {
                        node_id: member.node_id.clone(),
                        role: member.role.clone(),
                        state: member.state.clone(),
                    })
                    .collect(),
            });
    }
}
impl Drop for NativeTraceMountRegistration {
    fn drop(&mut self) {
        // Release observation interest when this SAME normal mount stops,
        // even if an export handle still retains the raw capture.
        let captures = CAPTURES
            .get()
            .map(|registry| {
                registry
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter(|capture| Arc::ptr_eq(&capture.mount, &self.0))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for capture in captures {
            capture
                .original_commit_subscription
                .include_original_current_term_commits(false);
        }
        // The actual owner and watch channel remain the lifecycle evidence.
    }
}
#[derive(Default)]
struct State {
    packets: Vec<Packet>,
    boundary_packets: Vec<NativeBoundaryPacketV1>,
    control_commits: Vec<NativeControlCommitObservationV1>,
    original_control_commits: Vec<NativeOriginalControlCommitObservationV1>,
    content_receipts: Vec<NativeContentReceiptObservationV1>,
    configuration_cuts: Vec<NativeConfigurationCutObservationV1>,
    metadata_cache_cuts: Vec<NativeMetadataCacheCutV1>,
    authoritative_reads: Vec<NativeAuthoritativeRead>,
    guard_events: Vec<NativeGuardEventV1>,
    bytes: usize,
    dropped: u64,
    incomplete: u64,
    in_flight: u64,
    pending_boundary_packets: usize,
    next_ordinal: u64,
    closed: bool,
}
impl State {
    fn packet_count(&self) -> usize {
        self.packets
            .len()
            .saturating_add(self.boundary_packets.len())
            .saturating_add(self.control_commits.len())
            .saturating_add(self.original_control_commits.len())
            .saturating_add(self.authoritative_reads.len())
            .saturating_add(self.guard_events.len())
            .saturating_add(self.content_receipts.len())
            .saturating_add(self.configuration_cuts.len())
            .saturating_add(self.metadata_cache_cuts.len())
            .saturating_add(self.pending_boundary_packets)
    }
}
struct Request {
    root: PathBuf,
    shard: DfsMetadataShardId,
    capture: Mutex<Option<Arc<Capture>>>,
}
static REQUESTS: OnceLock<Mutex<Vec<Weak<Request>>>> = OnceLock::new();
/// A scoped observation request can precede a normal mount. It grants no owner
/// or effect authority; only the real mounted component supplies those bindings.
pub struct NativeShardTraceRequest(Arc<Request>);
pub fn request_native_shard_trace(
    root: &Path,
    shard: DfsMetadataShardId,
) -> Result<NativeShardTraceRequest, MetadataBackendError> {
    let mut requests = REQUESTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    requests.retain(|request| request.strong_count() != 0);
    if requests
        .iter()
        .filter_map(Weak::upgrade)
        .any(|request| request.root == root && request.shard == shard)
    {
        return Err(MetadataBackendError::Serde(
            "duplicate native observation request for actual root/shard".into(),
        ));
    }
    let request = Arc::new(Request {
        root: root.to_owned(),
        shard,
        capture: Mutex::new(None),
    });
    requests.push(Arc::downgrade(&request));
    Ok(NativeShardTraceRequest(request))
}
impl NativeShardTraceRequest {
    pub fn capture(&self) -> Result<NativeShardTraceCapture, MetadataBackendError> {
        self.0
            .capture
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .cloned()
            .map(NativeShardTraceCapture)
            .ok_or_else(|| {
                MetadataBackendError::Serde(
                    "normal mounted native owner has not fulfilled the observation request".into(),
                )
            })
    }
}
struct Capture {
    mount: Arc<Mount>,
    shard: DfsMetadataShardId,
    state: Mutex<State>,
    original_commit_subscription: sinomo_core::kv::KvCommittedObservationSubscription,
}
fn new_capture(mount: Arc<Mount>, shard: DfsMetadataShardId) -> Arc<Capture> {
    Arc::new_cyclic(|capture: &Weak<Capture>| {
        let observed_capture = capture.clone();
        let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            &mount.cluster_paths,
            move |observation| {
                let (Some(capture), Some(facts)) = (
                    observed_capture.upgrade(),
                    observation.original_current_term_commits.as_ref(),
                ) else {
                    return;
                };
                for fact in facts.iter() {
                    let mount = &capture.mount;
                    if fact.root != mount.cluster_paths.root
                        || *mount.shutdown.borrow()
                        || mount.owner.upgrade().is_none()
                    {
                        continue;
                    }
                    let actual_node = mount
                        .identity
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                        .map(|identity| identity.local_node_id.clone());
                    if actual_node.as_ref() != Some(&fact.node_id) {
                        continue;
                    }
                    record_native_original_control_commit(&capture, fact);
                }
            },
        );
        subscription.include_original_current_term_commits(true);
        Capture {
            mount,
            shard,
            state: Mutex::new(State::default()),
            original_commit_subscription: subscription,
        }
    })
}
/// An observation handle to a real mounted component; it owns no author capability.
pub struct NativeShardTraceCapture(Arc<Capture>);
/// Resolve actual mounted custody. This lookup performs no filesystem/KV reads.
pub fn capture_native_shard_trace(
    data_root: &Path,
    shard: DfsMetadataShardId,
) -> Result<NativeShardTraceCapture, MetadataBackendError> {
    let unavailable = || {
        MetadataBackendError::Serde(
            "native trace has no live mounted DFS owner at the exact supplied root".into(),
        )
    };
    let registry = MOUNTS.get().ok_or_else(unavailable)?;
    let mut mounts = registry.lock().unwrap_or_else(|e| e.into_inner());
    mounts.retain(|mount| mount.strong_count() != 0);
    let mut matches = mounts.iter().filter_map(Weak::upgrade).filter(|mount| {
        mount.root == data_root && mount.owner.upgrade().is_some() && !*mount.shutdown.borrow()
    });
    let mount = matches.next().ok_or_else(unavailable)?;
    if matches.next().is_some() {
        return Err(MetadataBackendError::Serde(
            "native trace mount identity is ambiguous".into(),
        ));
    }
    let capture = new_capture(mount, shard);
    let mut captures = CAPTURES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    captures.retain(|capture| capture.strong_count() != 0);
    if captures.iter().filter_map(Weak::upgrade).any(|existing| {
        existing.shard == capture.shard && Arc::ptr_eq(&existing.mount, &capture.mount)
    }) {
        return Err(MetadataBackendError::Serde(
            "native trace already has a capture for this actual mount and shard".into(),
        ));
    }
    captures.push(Arc::downgrade(&capture));
    Ok(NativeShardTraceCapture(capture))
}
impl NativeShardTraceCapture {
    /// Stop new observation admission before the normal runtime is drained.
    pub fn close(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed = true;
        self.0
            .original_commit_subscription
            .include_original_current_term_commits(false);
    }
    /// Export raw facts, never a predicted finite state. JSON u64 values in native
    /// blobs require the lossless Python parser; envelope counters are decimal.
    pub fn export_raw(&self, output: impl Write) -> Result<(), MetadataBackendError> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let actual_owner_stopped = self.0.mount.owner.upgrade().is_none()
            && (*self.0.mount.shutdown.borrow() || self.0.mount.shutdown.has_changed().is_err());
        #[derive(Serialize)]
        struct Export<'a> {
            raw_observation_version: u16,
            classification: &'static str,
            actual_mount_root: &'a Path,
            actual_mount_label: &'a str,
            shard: &'a DfsMetadataShardId,
            packets: &'a [Packet],
            boundary_packets: &'a [NativeBoundaryPacketV1],
            control_commits: &'a [NativeControlCommitObservationV1],
            original_control_commits: &'a [NativeOriginalControlCommitObservationV1],
            authoritative_reads: &'a [NativeAuthoritativeRead],
            guard_events: &'a [NativeGuardEventV1],
            content_receipts: &'a [NativeContentReceiptObservationV1],
            configuration_cuts: &'a [NativeConfigurationCutObservationV1],
            metadata_cache_cuts: &'a [NativeMetadataCacheCutV1],
            closed: bool,
            actual_owner_stopped: bool,
            in_flight: String,
            dropped: String,
            incomplete: String,
            required_unavailable_facts: &'static [&'static str],
        }
        serde_json::to_writer(
            output,
            &Export {
                raw_observation_version: 1,
                classification: "raw-native-kernel-observation",
                actual_mount_root: &self.0.mount.root,
                actual_mount_label: &self.0.mount.label,
                shard: &self.0.shard,
                packets: &state.packets,
                boundary_packets: &state.boundary_packets,
                control_commits: &state.control_commits,
                original_control_commits: &state.original_control_commits,
                authoritative_reads: &state.authoritative_reads,
                guard_events: &state.guard_events,
                content_receipts: &state.content_receipts,
                configuration_cuts: &state.configuration_cuts,
                metadata_cache_cuts: &state.metadata_cache_cuts,
                closed: state.closed,
                actual_owner_stopped,
                in_flight: state.in_flight.to_string(),
                dropped: state.dropped.to_string(),
                incomplete: state.incomplete.to_string(),
                required_unavailable_facts: &[
                    "source/build/schema/model hash envelope binding",
                    "certified complete initial cut",
                    "same consumed retained configuration, all assigned ACKs and actual writer/freeze custody",
                    "actual lane/writer/fence/admission identities",
                    "replica/handoff/reply causal observations",
                ],
            },
        )
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))
    }
}
struct HeldPacket {
    owner_ordinal: u64,
    digest: DfsPreparedOperationDigest,
    phase: KernelPhase,
    before: NativeHeldMetadataSnapshotV1,
    after: NativeHeldMetadataSnapshotV1,
    frame: Option<(DfsMetadataJournalEntry, Vec<u8>)>,
}
/// Declare this BEFORE the native writer/guard. Reverse Rust drop order releases
/// the actual native lease before serializing or delivering these held copies.
pub(crate) struct KernelObservation {
    capture: Arc<Capture>,
    held: Vec<HeldPacket>,
    complete: bool,
}
pub(crate) fn has_interest(root: &Path, shard: &DfsMetadataShardId) -> bool {
    let Some(registry) = CAPTURES.get() else {
        return false;
    };
    registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .any(|capture| {
            capture.shard == *shard
                && root.starts_with(&capture.mount.root)
                && {
                    let state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
                    !state.closed || state.in_flight != 0
                }
                && capture.mount.owner.upgrade().is_some()
                && !*capture.mount.shutdown.borrow()
        })
}
pub(crate) fn interested(root: &Path, shard: &DfsMetadataShardId) -> Option<KernelObservation> {
    let registry = CAPTURES.get()?;
    let mut captures = registry.lock().unwrap_or_else(|e| e.into_inner());
    captures.retain(|capture| capture.strong_count() != 0);
    let capture = captures.iter().filter_map(Weak::upgrade).find(|capture| {
        capture.shard == *shard
            && root.starts_with(&capture.mount.root)
            && capture.mount.owner.upgrade().is_some()
            && !*capture.mount.shutdown.borrow()
    })?;
    {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return None;
        }
        state.in_flight = state.in_flight.checked_add(1)?;
    }
    Some(KernelObservation {
        capture,
        held: Vec::new(),
        complete: false,
    })
}
impl KernelObservation {
    pub(crate) fn observe(
        &mut self,
        digest: &DfsPreparedOperationDigest,
        phase: KernelPhase,
        before: NativeHeldMetadataSnapshotV1,
        after: NativeHeldMetadataSnapshotV1,
        frame: Option<(DfsMetadataJournalEntry, Vec<u8>)>,
    ) {
        // Assign sequence at the actual held observation boundary. Delivery
        // remains deferred until native writer/guard release, but that delay
        // cannot make a completed commit appear after its guard's release.
        let owner_ordinal = {
            let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
            let Some(ordinal) = state.next_ordinal.checked_add(1) else {
                state.dropped = state.dropped.saturating_add(1);
                return;
            };
            state.next_ordinal = ordinal;
            ordinal
        };
        self.held.push(HeldPacket {
            owner_ordinal,
            digest: digest.clone(),
            phase,
            before,
            after,
            frame,
        });
    }
    pub(crate) fn complete(&mut self) {
        self.complete = true;
    }
}
impl Drop for KernelObservation {
    fn drop(&mut self) {
        let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
        state.in_flight = state.in_flight.saturating_sub(1);
        if !self.complete {
            state.incomplete = state.incomplete.saturating_add(1);
        }
        for held in self.held.drain(..) {
            let (Ok(before), Ok(after)) = (
                serde_json::to_vec(&held.before),
                serde_json::to_vec(&held.after),
            ) else {
                state.incomplete = state.incomplete.saturating_add(1);
                continue;
            };
            let size = before
                .len()
                .saturating_add(after.len())
                .saturating_add(held.frame.as_ref().map_or(0, |(_, bytes)| bytes.len()));
            if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(size) > MAX_BYTES {
                state.dropped = state.dropped.saturating_add(1);
                continue;
            }
            state.bytes = state.bytes.saturating_add(size);
            let (exact_entry, exact_frame) = match held.frame {
                Some((entry, bytes)) => (Some(entry), Some(bytes)),
                None => (None, None),
            };
            state.packets.push(Packet {
                owner_label: self.capture.mount.label.clone(),
                owner_ordinal: held.owner_ordinal.to_string(),
                operation: held.digest,
                phase: held.phase,
                before_held_json: before,
                after_held_json: after,
                exact_entry,
                exact_frame,
            });
        }
    }
}

/// Retain the SAME successful native read after its ordinary generation merge.
/// Only a currently mounted, pointer-equal OS owner can match a capture. Rows
/// and native authority stay typed until export, outside the client mutex.
pub(crate) fn record_authoritative_read(
    data_root: &Path,
    actual_owner: Option<&Arc<DfsDataOwnerLock>>,
    keys: &[String],
    read: &sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>,
    accepted_generation: u64,
) {
    let (Some(registry), Some(owner)) = (CAPTURES.get(), actual_owner) else {
        return;
    };
    let captures = registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|capture| {
            capture.mount.root == data_root
                && capture
                    .mount
                    .owner
                    .upgrade()
                    .is_some_and(|mounted| Arc::ptr_eq(&mounted, owner))
                && !*capture.mount.shutdown.borrow()
                && keys.iter().any(|key| {
                    key.ends_with(&format!(
                        "/metadata-shards/{}",
                        capture.shard.0.trim_matches('/').replace('/', "_")
                    ))
                })
        })
        .collect::<Vec<_>>();
    for capture in captures {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            continue;
        }
        let size = keys.iter().map(String::len).sum::<usize>().saturating_add(
            read.value
                .iter()
                .flatten()
                .map(|entry| entry.value.len())
                .sum::<usize>(),
        );
        if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(size) > MAX_BYTES {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        };
        state.next_ordinal = ordinal;
        state.bytes = state.bytes.saturating_add(size);
        state.authoritative_reads.push(NativeAuthoritativeRead {
            owner_label: capture.mount.label.clone(),
            owner_ordinal: ordinal.to_string(),
            requested_keys: keys.to_vec(),
            read: read.clone(),
            accepted_projection_generation: accepted_generation.to_string(),
        });
    }
}

#[derive(Clone)]
struct RetainedConfigurationContext {
    capture: Weak<Capture>,
    guard_id: String,
}
thread_local! { static RETAINED_CONFIGURATION_CONTEXT:RefCell<Option<RetainedConfigurationContext>>=const {RefCell::new(None)}; }
/// Observation-only dynamic scope, never an admission or authority capability.
pub(crate) struct NativeRetainedConfigurationScope {
    previous: Option<RetainedConfigurationContext>,
}
impl Drop for NativeRetainedConfigurationScope {
    fn drop(&mut self) {
        RETAINED_CONFIGURATION_CONTEXT.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}
#[derive(Debug, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub(crate) enum NativeGuardAuthorityV1 {
    Local {
        node_id: sinomo_core::node::NodeId,
    },
    FullShard {
        expected: crate::DfsMetadataShardConfig,
        authenticated_acknowledged_node_ids: Vec<sinomo_core::node::NodeId>,
    },
}
#[derive(Debug, Serialize)]
pub(crate) struct NativeGuardScopeV1 {
    pub canonical_root: PathBuf,
    pub file_system_id: crate::DfsFileSystemId,
    pub shard_id: crate::DfsMetadataShardId,
    /// Copied from normal DfsPublicationOwnerScope construction, no new stat.
    pub cached_root_identity: Option<(String, String)>,
}
#[derive(Debug, Serialize)]
pub(crate) struct NativeGuardExecutionV1 {
    /// Observation-local aliases of ACTUAL retained native handles.
    pub execution_owner_handle: String,
    pub admission_scheduler_handle: String,
    pub execution_scope_debug: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct NativeGuardFactsV1 {
    pub actor_node_id: sinomo_core::node::NodeId,
    pub authority: NativeGuardAuthorityV1,
    pub physical_scope: NativeGuardScopeV1,
    pub execution: Option<NativeGuardExecutionV1>,
}
#[derive(Debug, Serialize)]
#[serde(tag = "phase", rename_all = "kebab-case")]
enum NativeGuardEventV1 {
    Admitted {
        owner_label: String,
        owner_ordinal: String,
        guard_id: String,
        digest: DfsPreparedOperationDigest,
        facts: NativeGuardFactsV1,
        held: Option<NativeHeldMetadataSnapshotV1>,
        matches_actual_mounted_owner: bool,
    },
    Released {
        owner_label: String,
        owner_ordinal: String,
        guard_id: String,
        digest: DfsPreparedOperationDigest,
    },
}
/// Retain as the LAST actual guard field. Its drop records this guard ending
/// after its writer/fence/execution leases drop. Source custody may retain the
/// same shared writer and fence; this event does not observe their final release.
pub(crate) struct NativeGuardTrace {
    capture: Arc<Capture>,
    guard_id: String,
    digest: DfsPreparedOperationDigest,
    admitted: bool,
}
pub(crate) fn begin_guard_trace(
    root: &Path,
    shard: &DfsMetadataShardId,
    digest: &DfsPreparedOperationDigest,
) -> Option<NativeGuardTrace> {
    let registry = CAPTURES.get()?;
    let capture = registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .find(|capture| {
            capture.shard == *shard
                && root.starts_with(&capture.mount.root)
                && capture.mount.owner.upgrade().is_some()
                && !*capture.mount.shutdown.borrow()
        })?;
    {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return None;
        }
        state.in_flight = state.in_flight.checked_add(1)?;
    }
    Some(NativeGuardTrace {
        capture,
        guard_id: format!("native-guard:{}", sinomo_core::Id::generate()),
        digest: digest.clone(),
        admitted: false,
    })
}
impl NativeGuardTrace {
    pub(crate) fn enter_retained_configuration(&self) -> NativeRetainedConfigurationScope {
        let context = RetainedConfigurationContext {
            capture: Arc::downgrade(&self.capture),
            guard_id: self.guard_id.clone(),
        };
        NativeRetainedConfigurationScope {
            previous: RETAINED_CONFIGURATION_CONTEXT
                .with(|slot| slot.borrow_mut().replace(context)),
        }
    }
    /// Called only at the normal successful mint with copies of its actual
    /// owned proof fields. No serialization or user callback runs here.
    pub(crate) fn admitted(
        &mut self,
        facts: NativeGuardFactsV1,
        held: Option<NativeHeldMetadataSnapshotV1>,
        actual_owner: Option<&Arc<DfsDataOwnerLock>>,
    ) {
        let matches_actual_mounted_owner = actual_owner.is_some_and(|owner| {
            self.capture
                .mount
                .owner
                .upgrade()
                .is_some_and(|mounted| Arc::ptr_eq(&mounted, owner))
        });
        let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.admitted {
            state.incomplete = state.incomplete.saturating_add(1);
            return;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            return;
        };
        if state.packet_count() >= MAX_PACKETS {
            state.dropped = state.dropped.saturating_add(1);
            return;
        }
        state.next_ordinal = ordinal;
        self.admitted = true;
        state.guard_events.push(NativeGuardEventV1::Admitted {
            owner_label: self.capture.mount.label.clone(),
            owner_ordinal: ordinal.to_string(),
            guard_id: self.guard_id.clone(),
            digest: self.digest.clone(),
            facts,
            held,
            matches_actual_mounted_owner,
        });
    }
}
impl Drop for NativeGuardTrace {
    fn drop(&mut self) {
        let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
        state.in_flight = state.in_flight.saturating_sub(1);
        if !self.admitted {
            state.incomplete = state.incomplete.saturating_add(1);
            return;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            return;
        };
        if state.packet_count() >= MAX_PACKETS {
            state.dropped = state.dropped.saturating_add(1);
            return;
        }
        state.next_ordinal = ordinal;
        state.guard_events.push(NativeGuardEventV1::Released {
            owner_label: self.capture.mount.label.clone(),
            owner_ordinal: ordinal.to_string(),
            guard_id: self.guard_id.clone(),
            digest: self.digest.clone(),
        });
    }
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTraceArtifactBindingV1 {
    pub implementation_revision: String,
    pub implementation_build_id: String,
    pub executable_sha256: String,
    pub source_manifest_sha256: String,
    pub schema_sha256: String,
    pub model_sha256: String,
}

/// Copies from successful ordinary persistence/coordinator boundaries. These
/// are observation DTOs and never confer content or publication authority.
#[derive(Debug, Serialize)]
#[serde(tag = "origin", rename_all = "kebab-case")]
pub(crate) enum NativeContentReceiptFactsV1 {
    LocalPersisted {
        file_system_id: crate::DfsFileSystemId,
        manifest: crate::ContentManifest,
        persistence: NativeContentPersistenceKindV1,
    },
    CoordinatorDurable {
        intent: crate::DistributedWriteIntent,
        receipt: crate::DistributedReplicaReceipt,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum NativeContentPersistenceKindV1 {
    BlobOnly,
    ManifestBound,
}

#[derive(Debug, Serialize)]
struct NativeContentReceiptObservationV1 {
    owner_label: String,
    owner_ordinal: String,
    shard_id: DfsMetadataShardId,
    actual_mounted_node_id: Option<sinomo_core::node::NodeId>,
    facts: NativeContentReceiptFactsV1,
}

/// Only SAME normal return values may call this hook. The actual held owner
/// must match the existing normal mount; no registry lookup acquires custody.
pub(crate) fn record_content_receipt(
    data_root: &Path,
    actual_owner: &Arc<DfsDataOwnerLock>,
    shard: &DfsMetadataShardId,
    facts: NativeContentReceiptFactsV1,
) {
    let Some(registry) = CAPTURES.get() else {
        return;
    };
    let capture = registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .find(|capture| {
            capture.shard == *shard
                && capture.mount.root == data_root
                && !*capture.mount.shutdown.borrow()
                && capture
                    .mount
                    .owner
                    .upgrade()
                    .is_some_and(|owner| Arc::ptr_eq(&owner, actual_owner))
        });
    let Some(capture) = capture else {
        return;
    };
    let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
    if state.closed {
        return;
    }
    let size = match serde_json::to_vec(&facts) {
        Ok(bytes) => bytes.len(),
        Err(_) => {
            state.incomplete = state.incomplete.saturating_add(1);
            return;
        }
    };
    if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(size) > MAX_BYTES {
        state.dropped = state.dropped.saturating_add(1);
        return;
    }
    let Some(ordinal) = state.next_ordinal.checked_add(1) else {
        state.dropped = state.dropped.saturating_add(1);
        return;
    };
    state.next_ordinal = ordinal;
    state.bytes = state.bytes.saturating_add(size);
    let actual_mounted_node_id = capture
        .mount
        .identity
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .map(|identity| identity.local_node_id.clone());
    state
        .content_receipts
        .push(NativeContentReceiptObservationV1 {
            owner_label: capture.mount.label.clone(),
            owner_ordinal: ordinal.to_string(),
            shard_id: shard.clone(),
            actual_mounted_node_id,
            facts,
        });
}

#[derive(Debug, Serialize)]
struct NativeJournalFrameDecodeV1 {
    owner_ordinal: String,
    decoded_entry: DfsMetadataJournalEntry,
}
#[derive(Debug, Serialize)]
struct NativeReceivedFrameDecodeV1 {
    source_owner_ordinal: String,
    frame_index: usize,
    decoded_entry: DfsMetadataJournalEntry,
}
impl NativeShardTraceCapture {
    /// Export only actual collected facts. Build/source hashes come from the
    /// compile runner's immutable manifest, not artifact generation or epoch.
    /// Cached member facts remain identified as configuration observations.
    pub fn export_native_header(
        &self,
        binding: &NativeTraceArtifactBindingV1,
        output: impl Write,
    ) -> Result<(), MetadataBackendError> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let identity = self
            .0
            .mount
            .identity
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        #[derive(Serialize)]
        struct Header<'a> {
            native_header_type: &'static str,
            artifact_binding: &'a NativeTraceArtifactBindingV1,
            actual_mount_label: &'a str,
            actual_data_root: &'a Path,
            shard: &'a DfsMetadataShardId,
            normal_mounted_identity: &'a Option<NativeMountedIdentityV1>,
            actual_first_held_json: Option<&'a [u8]>,
            actual_last_held_json: Option<&'a [u8]>,
            actual_append_entries: Vec<&'a DfsMetadataJournalEntry>,
            actual_append_frames: Vec<&'a [u8]>,
            actual_frame_decodes: Vec<NativeJournalFrameDecodeV1>,
            actual_received_frame_decodes: Vec<NativeReceivedFrameDecodeV1>,
            actual_native_reads: &'a [NativeAuthoritativeRead],
            actual_guard_events: &'a [NativeGuardEventV1],
            actual_boundary_packets: &'a [NativeBoundaryPacketV1],
            actual_control_commits: &'a [NativeControlCommitObservationV1],
            actual_original_control_commits: &'a [NativeOriginalControlCommitObservationV1],
            actual_content_receipts: &'a [NativeContentReceiptObservationV1],
            actual_configuration_cuts: &'a [NativeConfigurationCutObservationV1],
            actual_metadata_cache_cuts: &'a [NativeMetadataCacheCutV1],
            actual_control_decodes: Vec<NativeControlDecodeV1>,
            actual_request_value_decodes: Vec<NativeRequestValueDecodeV1<'a>>,
            unavailable: &'static [&'static str],
        }
        // Use the SAME normal decoder on the already-retained append bytes.
        // This runs only at opt-in export, after native writer release, and
        // supplies an actual decode observation rather than a certificate flag.
        let actual_frame_decodes = state
            .packets
            .iter()
            .filter_map(|packet| packet.exact_frame.as_deref().map(|bytes| (packet, bytes)))
            .map(|(packet, bytes)| {
                let decoded_entry: DfsMetadataJournalEntry =
                    sinomo_core::object::serde_ext::from_bytes(bytes)
                        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
                let retained_entry = packet.exact_entry.as_ref().ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "native append frame lacks its actual retained typed entry".into(),
                    )
                })?;
                if &decoded_entry != retained_entry {
                    return Err(MetadataBackendError::Serde(
                        "normal journal decoder differs from actual retained append entry".into(),
                    ));
                }
                Ok(NativeJournalFrameDecodeV1 {
                    owner_ordinal: packet.owner_ordinal.clone(),
                    decoded_entry,
                })
            })
            .collect::<Result<Vec<_>, MetadataBackendError>>()?;
        let mut actual_received_frame_decodes = Vec::new();
        for packet in &state.boundary_packets {
            if let NativeBoundaryFactsV1::ReplicaJournalPersisted { command, .. } = &packet.facts {
                for (frame_index, frame) in command.batch.journal_frames.iter().enumerate() {
                    let decoded_entry =
                        crate::metadata_journal::decode_metadata_journal_frame_record(frame)?;
                    if decoded_entry.shard_id != command.batch.shard_id {
                        return Err(MetadataBackendError::Serde(
                            "actual received journal frame decodes into a different shard".into(),
                        ));
                    }
                    actual_received_frame_decodes.push(NativeReceivedFrameDecodeV1 {
                        source_owner_ordinal: packet.owner_ordinal.clone(),
                        frame_index,
                        decoded_entry,
                    });
                }
            }
        }
        let mut actual_control_decodes = Vec::new();
        let captured_entries = state
            .configuration_cuts
            .iter()
            .flat_map(|cut| {
                cut.scopes[0]
                    .iter()
                    .map(move |entry| (&cut.owner_ordinal, entry))
            })
            .chain(state.metadata_cache_cuts.iter().flat_map(|cut| {
                cut.entries
                    .iter()
                    .map(move |entry| (&cut.owner_ordinal, entry))
            }))
            .chain(state.authoritative_reads.iter().flat_map(|read| {
                read.read
                    .value
                    .iter()
                    .filter_map(Option::as_ref)
                    .map(move |entry| (&read.owner_ordinal, entry))
            }));
        for (ordinal, entry) in captured_entries {
            if let Some((native_type, decoded_value)) =
                crate::control_plane::decode_native_control_observation(entry)
                    .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            {
                actual_control_decodes.push(NativeControlDecodeV1 {
                    source_owner_ordinal: ordinal.clone(),
                    exact_entry: entry.clone(),
                    native_type,
                    decoded_value,
                });
            }
        }
        // Decode only SAME retained request bytes, after actual work has
        // drained. The source tuple identifies its exact request field; these
        // values never become an invented persisted Entry or author capability.
        let mut actual_request_value_decodes = Vec::new();
        for commit in &state.control_commits {
            let txn = &commit.commit.request.txn;
            let compares = txn
                .compares
                .iter()
                .enumerate()
                .filter_map(|(index, compare)| {
                    if let sinomo_core::kv::KvCompare::ValueEquals { key, value } = compare {
                        Some(("compare", index, key.as_str(), value.as_slice()))
                    } else {
                        None
                    }
                });
            let puts = txn.success.iter().enumerate().filter_map(|(index, op)| {
                if let sinomo_core::kv::KvOp::Put { key, value, .. } = op {
                    Some(("success", index, key.as_str(), value.as_slice()))
                } else {
                    None
                }
            });
            for (request_section, field_index, key, exact_value) in compares.chain(puts) {
                if let Some((native_type, decoded_value)) =
                    crate::control_plane::decode_native_control_value_observation(key, exact_value)
                        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
                {
                    actual_request_value_decodes.push(NativeRequestValueDecodeV1 {
                        source_owner_ordinal: &commit.owner_ordinal,
                        request_section,
                        field_index,
                        key,
                        exact_value,
                        native_type,
                        decoded_value,
                    });
                }
            }
        }
        serde_json::to_writer(
            output,
            &Header {
                native_header_type: "DfsNativeShardHeaderV1",
                artifact_binding: binding,
                actual_mount_label: &self.0.mount.label,
                actual_data_root: &self.0.mount.root,
                shard: &self.0.shard,
                normal_mounted_identity: &identity,
                actual_first_held_json: state
                    .packets
                    .first()
                    .map(|packet| packet.before_held_json.as_slice()),
                actual_last_held_json: state
                    .packets
                    .last()
                    .map(|packet| packet.after_held_json.as_slice()),
                actual_append_entries: state
                    .packets
                    .iter()
                    .filter_map(|packet| packet.exact_entry.as_ref())
                    .collect(),
                actual_append_frames: state
                    .packets
                    .iter()
                    .filter_map(|packet| packet.exact_frame.as_deref())
                    .collect(),
                actual_frame_decodes,
                actual_received_frame_decodes,
                actual_native_reads: &state.authoritative_reads,
                actual_guard_events: &state.guard_events,
                actual_boundary_packets: &state.boundary_packets,
                actual_control_commits: &state.control_commits,
                actual_original_control_commits: &state.original_control_commits,
                actual_content_receipts: &state.content_receipts,
                actual_configuration_cuts: &state.configuration_cuts,
                actual_metadata_cache_cuts: &state.metadata_cache_cuts,
                actual_control_decodes,
                actual_request_value_decodes,
                unavailable: &[
                    "peer held cuts/ACK and snapshot transfer ancestry",
                    "certified terminal baseline and active dependencies",
                    "actual fixed voter configuration certification",
                    "actual reply/handoff/cancel return observations",
                ],
            },
        )
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))
    }
}

/// One complete configuration already accepted by the ordinary readiness path.
/// The original per-scope native authority and observed floor remain separate.
#[derive(Debug, Serialize)]
struct NativeConfigurationCutObservationV1 {
    owner_label: String,
    owner_ordinal: String,
    requested_scope_prefixes:
        [&'static str; crate::write_configuration_projection::DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
    revision: String,
    accepted_authority_generation: String,
    scopes: [Vec<sinomo_core::kv::KvEntry>;
        crate::write_configuration_projection::DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
    authority: [sinomo_core::kv::KvAuthorityProof;
        crate::write_configuration_projection::DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
    retained_projection_floor: sinomo_core::kv::KvLocalRead<Option<sinomo_core::kv::KvEntry>>,
    // SAME normal accepted immutable view consumed at this actual guard's
    // final recheck. Correlation is observational and grants no authority.
    guard_context_id: Option<String>,
}
pub(crate) fn record_configuration_cut(
    data_root: &Path,
    actual_owner: Option<&Arc<DfsDataOwnerLock>>,
    view: &crate::DfsWriteConfigurationProjection,
    accepted_authority_generation: u64,
) {
    let (Some(registry), Some(owner)) = (CAPTURES.get(), actual_owner) else {
        return;
    };
    let captures = registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|capture| {
            capture.mount.root == data_root
                && !*capture.mount.shutdown.borrow()
                && capture
                    .mount
                    .owner
                    .upgrade()
                    .is_some_and(|mounted| Arc::ptr_eq(&mounted, owner))
        })
        .collect::<Vec<_>>();
    for capture in captures {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            continue;
        }
        let size = view
            .scopes
            .iter()
            .flatten()
            .map(|row| row.key.len().saturating_add(row.value.len()))
            .sum::<usize>();
        if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(size) > MAX_BYTES {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        };
        state.next_ordinal = ordinal;
        state.bytes = state.bytes.saturating_add(size);
        state
            .configuration_cuts
            .push(NativeConfigurationCutObservationV1 {
                owner_label: capture.mount.label.clone(),
                owner_ordinal: ordinal.to_string(),
                requested_scope_prefixes:
                    crate::write_configuration_projection::DFS_WRITE_CONFIGURATION_PREFIXES,
                revision: view.revision.0.to_string(),
                accepted_authority_generation: accepted_authority_generation.to_string(),
                scopes: view.scopes.clone(),
                authority: view.authority.clone(),
                retained_projection_floor: view.floor.clone(),
                guard_context_id: RETAINED_CONFIGURATION_CONTEXT.with(|slot| {
                    slot.borrow()
                        .as_ref()
                        .filter(|context| {
                            context
                                .capture
                                .upgrade()
                                .is_some_and(|actual| Arc::ptr_eq(&actual, &capture))
                        })
                        .map(|context| context.guard_id.clone())
                }),
            });
    }
}

#[derive(Clone)]
struct NativeMetadataCacheContext {
    capture: Weak<Capture>,
    scope: crate::DfsPreparedOperationScope,
}
thread_local! {
    static METADATA_CACHE_CONTEXT: std::cell::RefCell<Option<NativeMetadataCacheContext>> = const { std::cell::RefCell::new(None) };
}
pub(crate) struct NativeMetadataCacheContextLease(Option<NativeMetadataCacheContext>);
impl Drop for NativeMetadataCacheContextLease {
    fn drop(&mut self) {
        METADATA_CACHE_CONTEXT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
pub(crate) fn has_root_interest(root: &Path, owner: &Arc<DfsDataOwnerLock>) -> bool {
    CAPTURES.get().is_some_and(|registry| {
        registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .any(|capture| {
                capture.mount.root == root
                    && !*capture.mount.shutdown.borrow()
                    && capture
                        .mount
                        .owner
                        .upgrade()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
                    && !capture
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .closed
            })
    })
}
pub(crate) fn enter_metadata_cache_context(
    root: &Path,
    owner: &Arc<DfsDataOwnerLock>,
    scope: &crate::DfsPreparedOperationScope,
) -> NativeMetadataCacheContextLease {
    let capture = CAPTURES.get().and_then(|registry| {
        registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .find(|capture| {
                capture.mount.root == root
                    && capture.shard == scope.shard_id
                    && !*capture.mount.shutdown.borrow()
                    && capture
                        .mount
                        .owner
                        .upgrade()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
            })
    });
    let context = capture.map(|capture| NativeMetadataCacheContext {
        capture: Arc::downgrade(&capture),
        scope: scope.clone(),
    });
    NativeMetadataCacheContextLease(METADATA_CACHE_CONTEXT.with(|slot| slot.replace(context)))
}
#[derive(Debug, Serialize)]
struct NativeMetadataCacheCutV1 {
    owner_label: String,
    owner_ordinal: String,
    actual_preparation_scope: Option<crate::DfsPreparedOperationScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_consumption_scope: Option<NativeControlConsumptionScopeV1>,
    prefix: &'static str,
    cached_revision: Option<String>,
    required_revision: String,
    projection_generation: String,
    entries: Vec<sinomo_core::kv::KvEntry>,
    original_native_provenance:
        Option<crate::metadata_authority_runtime::NativeMetadataCacheEvidenceV1>,
    provenance_loss_count: String,
}
pub(crate) fn record_metadata_cache_cut(
    generation: u64,
    revision: Option<sinomo_core::kv::KvRevision>,
    required: sinomo_core::kv::KvRevision,
    entries: &[sinomo_core::kv::KvEntry],
    provenance: Option<&crate::metadata_authority_runtime::NativeMetadataCacheEvidenceV1>,
    provenance_loss_count: u64,
) {
    let context = METADATA_CACHE_CONTEXT.with(|slot| slot.borrow().clone());
    let Some(context) = context else {
        return;
    };
    let Some(capture) = context.capture.upgrade() else {
        return;
    };
    let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
    if state.closed {
        return;
    }
    let size = entries
        .iter()
        .map(|entry| entry.key.len().saturating_add(entry.value.len()))
        .sum::<usize>();
    let provenance_size = provenance.map_or(0, |e| e.retained_bytes());
    if state.packet_count() >= MAX_PACKETS
        || state
            .bytes
            .saturating_add(size)
            .saturating_add(provenance_size)
            > MAX_BYTES
    {
        state.dropped = state.dropped.saturating_add(1);
        return;
    }
    let Some(ordinal) = state.next_ordinal.checked_add(1) else {
        state.dropped = state.dropped.saturating_add(1);
        return;
    };
    state.next_ordinal = ordinal;
    state.bytes = state
        .bytes
        .saturating_add(size)
        .saturating_add(provenance_size);
    state.metadata_cache_cuts.push(NativeMetadataCacheCutV1 {
        owner_label: capture.mount.label.clone(),
        owner_ordinal: ordinal.to_string(),
        actual_preparation_scope: Some(context.scope),
        actual_consumption_scope: None,
        prefix: crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX,
        cached_revision: revision.map(|revision| revision.0.to_string()),
        required_revision: required.0.to_string(),
        projection_generation: generation.to_string(),
        entries: entries.to_vec(),
        original_native_provenance: provenance.cloned(),
        provenance_loss_count: provenance_loss_count.to_string(),
    });
}

/// The normal CP consumer's actual scope, separate from a prepared operation.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(crate) enum NativeControlConsumptionScopeV1 {
    RetainedPrefixSeed {
        prefix: &'static str,
    },
    RetainedKeysReturned {
        keys: Vec<String>,
        values: Vec<Option<sinomo_core::kv::KvEntry>>,
    },
    RetainedPrefixReturned {
        prefix: String,
        entries: Vec<sinomo_core::kv::KvEntry>,
    },
}
#[derive(Debug)]
pub(crate) struct NativeRetainedMetadataSeed {
    root: PathBuf,
    owner: Weak<DfsDataOwnerLock>,
    captures: Vec<Weak<Capture>>,
    pub(crate) projection_generation: u64,
    pub(crate) revision: sinomo_core::kv::KvRevision,
    required: sinomo_core::kv::KvRevision,
    pub(crate) entries: Vec<sinomo_core::kv::KvEntry>,
    provenance: Option<crate::metadata_authority_runtime::NativeMetadataCacheEvidenceV1>,
    provenance_loss_count: u64,
}
struct NativeRetainedSeedContext {
    root: PathBuf,
    owner: Weak<DfsDataOwnerLock>,
    captures: Vec<Weak<Capture>>,
    seed: Arc<Mutex<Option<Arc<NativeRetainedMetadataSeed>>>>,
}
thread_local! {
    static RETAINED_SEED_CONTEXT: std::cell::RefCell<Option<NativeRetainedSeedContext>> =
        const { std::cell::RefCell::new(None) };
}
struct NativeRetainedSeedContextRestore(Option<NativeRetainedSeedContext>);
impl Drop for NativeRetainedSeedContextRestore {
    fn drop(&mut self) {
        RETAINED_SEED_CONTEXT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
/// Wrap only the already-required normal load. No read or owner is acquired.
pub(crate) fn capture_retained_metadata_seed<T>(
    root: &Path,
    owner: &Arc<DfsDataOwnerLock>,
    load: impl FnOnce() -> T,
) -> (T, Option<Arc<NativeRetainedMetadataSeed>>) {
    let captures = CAPTURES.get().map_or_else(Vec::new, |registry| {
        registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|capture| {
                capture.mount.root == root
                    && !*capture.mount.shutdown.borrow()
                    && capture
                        .mount
                        .owner
                        .upgrade()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
            })
            .map(|capture| Arc::downgrade(&capture))
            .collect::<Vec<_>>()
    });
    if captures.is_empty() {
        return (load(), None);
    }
    let seed = Arc::new(Mutex::new(None));
    let context = NativeRetainedSeedContext {
        root: root.to_path_buf(),
        owner: Arc::downgrade(owner),
        captures,
        seed: Arc::clone(&seed),
    };
    let _restore = NativeRetainedSeedContextRestore(
        RETAINED_SEED_CONTEXT.with(|slot| slot.replace(Some(context))),
    );
    let result = load();
    let observed = seed.lock().unwrap_or_else(|e| e.into_inner()).take();
    (result, observed)
}
/// Copy the exact projection and provenance while its normal mutex is held.
pub(crate) fn retain_metadata_seed_observation(
    generation: u64,
    revision: sinomo_core::kv::KvRevision,
    required: sinomo_core::kv::KvRevision,
    entries: &[sinomo_core::kv::KvEntry],
    provenance: Option<&crate::metadata_authority_runtime::NativeMetadataCacheEvidenceV1>,
    provenance_loss_count: u64,
) {
    RETAINED_SEED_CONTEXT.with(|slot| {
        let borrowed = slot.borrow();
        let Some(context) = borrowed.as_ref() else {
            return;
        };
        let bytes = entries
            .iter()
            .fold(0usize, |n, e| {
                n.saturating_add(e.key.len()).saturating_add(e.value.len())
            })
            .saturating_add(provenance.map_or(0, |p| p.retained_bytes()));
        if bytes > MAX_BYTES {
            for capture in context.captures.iter().filter_map(Weak::upgrade) {
                let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
                if !state.closed {
                    state.dropped = state.dropped.saturating_add(1);
                }
            }
            return;
        }
        *context.seed.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(Arc::new(NativeRetainedMetadataSeed {
                root: context.root.clone(),
                owner: context.owner.clone(),
                captures: context.captures.clone(),
                projection_generation: generation,
                revision,
                required,
                entries: entries.to_vec(),
                provenance: provenance.cloned(),
                provenance_loss_count,
            }));
    });
}
/// Emit only an actually accepted seed or successful cached return.
pub(crate) fn record_retained_control_use(
    root: &Path,
    owner: &Arc<DfsDataOwnerLock>,
    seed: &NativeRetainedMetadataSeed,
    scope: NativeControlConsumptionScopeV1,
) {
    if seed.root != root
        || !seed
            .owner
            .upgrade()
            .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
    {
        return;
    }
    for capture in seed.captures.iter().filter_map(Weak::upgrade) {
        if capture.mount.root != root
            || *capture.mount.shutdown.borrow()
            || !capture
                .mount
                .owner
                .upgrade()
                .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
        {
            continue;
        }
        let segment = capture.shard.0.trim_matches('/').replace('/', "_");
        let applies = match &scope {
            NativeControlConsumptionScopeV1::RetainedPrefixSeed { prefix } => {
                *prefix == crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX
            }
            NativeControlConsumptionScopeV1::RetainedKeysReturned { keys, .. } => {
                keys.iter().any(|key| {
                    let parts = key.split('/').collect::<Vec<_>>();
                    parts.len() == 6
                        && parts[1] == "dfs"
                        && parts[2] == "filesystems"
                        && !parts[3].is_empty()
                        && parts[5] == segment
                        && matches!(
                            parts[4],
                            "metadata-shards"
                                | "metadata-membership-handoffs"
                                | "metadata-operation-floors"
                                | "metadata-head-watermarks"
                        )
                })
            }
            NativeControlConsumptionScopeV1::RetainedPrefixReturned { prefix, .. } => {
                prefix == crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX
            }
        };
        if !applies {
            continue;
        }
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            continue;
        }
        let size = seed
            .entries
            .iter()
            .fold(0usize, |n, e| {
                n.saturating_add(e.key.len()).saturating_add(e.value.len())
            })
            .saturating_add(seed.provenance.as_ref().map_or(0, |p| p.retained_bytes()))
            .saturating_add(match &scope {
                NativeControlConsumptionScopeV1::RetainedKeysReturned { keys, values } => keys
                    .iter()
                    .fold(0usize, |n, k| n.saturating_add(k.len()))
                    .saturating_add(values.iter().flatten().fold(0usize, |n, e| {
                        n.saturating_add(e.key.len()).saturating_add(e.value.len())
                    })),
                NativeControlConsumptionScopeV1::RetainedPrefixReturned { prefix, entries } => {
                    prefix
                        .len()
                        .saturating_add(entries.iter().fold(0usize, |n, e| {
                            n.saturating_add(e.key.len()).saturating_add(e.value.len())
                        }))
                }
                NativeControlConsumptionScopeV1::RetainedPrefixSeed { prefix } => prefix.len(),
            });
        if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(size) > MAX_BYTES {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        };
        state.next_ordinal = ordinal;
        state.bytes = state.bytes.saturating_add(size);
        state.metadata_cache_cuts.push(NativeMetadataCacheCutV1 {
            owner_label: capture.mount.label.clone(),
            owner_ordinal: ordinal.to_string(),
            actual_preparation_scope: None,
            actual_consumption_scope: Some(scope.clone()),
            prefix: crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX,
            cached_revision: Some(seed.revision.0.to_string()),
            required_revision: seed.required.0.to_string(),
            projection_generation: seed.projection_generation.to_string(),
            entries: seed.entries.clone(),
            original_native_provenance: seed.provenance.clone(),
            provenance_loss_count: seed.provenance_loss_count.to_string(),
        });
    }
}

#[derive(Debug, Serialize)]
struct NativeControlDecodeV1 {
    source_owner_ordinal: String,
    exact_entry: sinomo_core::kv::KvEntry,
    native_type: &'static str,
    decoded_value: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct NativeRequestValueDecodeV1<'a> {
    source_owner_ordinal: &'a str,
    request_section: &'static str,
    field_index: usize,
    key: &'a str,
    exact_value: &'a [u8],
    native_type: &'static str,
    decoded_value: serde_json::Value,
}

/// Normal boundary return facts only. All native typed input/output remains
/// distinct from model state and observation never authorizes an effect.
#[derive(Debug, Serialize)]
#[serde(tag = "phase", rename_all = "kebab-case")]
pub(crate) enum NativeBoundaryFactsV1 {
    ContentStageDurableReturn {
        authenticated_requester_node_id: sinomo_core::node::NodeId,
        command: NativeContentStageIdentityV1,
        response: crate::DfsStageBlobResponse,
    },
    ReplicaWriteFreezeRead {
        read_id: String,
        permit: NativeReplicaPermitTupleV1,
        observed_freeze: ObservedNative<crate::metadata_api::DfsMetadataShardWriteFreeze>,
        actual_writer_token: String,
    },
    ReplicaJournalPersisted {
        local_node_id: sinomo_core::node::NodeId,
        actual_source_config: crate::DfsMetadataShardConfig,
        actual_pending_handoff: Option<crate::DfsMetadataShardMembershipHandoff>,
        command: crate::DfsMetadataJournalBatchCommand,
        applied_entries: usize,
        permit: NativeReplicaPermitTupleV1,
        durable_prefix: Option<crate::DfsMetadataPrefixProof>,
        materialization_pending: bool,
    },
    ReplicaJournalPersistenceFailed {
        local_node_id: sinomo_core::node::NodeId,
        actual_source_config: crate::DfsMetadataShardConfig,
        command: crate::DfsMetadataJournalBatchCommand,
        error_detail: String,
    },
    ReplicaJournalResponseConstructed {
        response: crate::DfsMetadataJournalBatchResponse,
    },
    ReplicaMaterializationCompleted {
        file_system_id: crate::DfsFileSystemId,
        shard_id: crate::DfsMetadataShardId,
        actual_materialized_entries: usize,
    },
    ReplicaPrefixProbeAccepted {
        local_node_id: sinomo_core::node::NodeId,
        actual_source_config: crate::DfsMetadataShardConfig,
        actual_pending_handoff: Option<crate::DfsMetadataShardMembershipHandoff>,
        command: crate::DfsMetadataJournalBatchCommand,
        response: crate::DfsMetadataJournalBatchResponse,
    },
    ReplicaSnapshotAccepted {
        local_node_id: sinomo_core::node::NodeId,
        actual_source_config: crate::DfsMetadataShardConfig,
        actual_pending_handoff: Option<crate::DfsMetadataShardMembershipHandoff>,
        command: crate::DfsMetadataSnapshotInstallCommand,
        response: crate::DfsMetadataSnapshotInstallResponse,
        permit: NativeReplicaPermitTupleV1,
    },
    SourceJournalResponseAccepted {
        authenticated_source_node_id: sinomo_core::node::NodeId,
        response: crate::DfsMetadataJournalBatchResponse,
        expected_target_node_id: sinomo_core::node::NodeId,
        expected_next_after_sequence: u64,
        expected_prefix_only: bool,
        expected_confirmation_id: Option<String>,
    },
    HandoffActivationCompleted {
        handoff: crate::DfsMetadataShardMembershipHandoff,
        proof: crate::DfsMetadataShardMembershipHandoffProof,
    },
    RetiringFreezePersisted {
        pending: crate::DfsMetadataShardMembershipHandoff,
        freeze: crate::metadata_api::DfsMetadataShardWriteFreeze,
    },
    SuccessorRebound {
        active: crate::DfsMetadataShardConfig,
        proof: crate::DfsMetadataShardMembershipHandoffProof,
        original_freeze: crate::metadata_api::DfsMetadataShardWriteFreeze,
        prior_instance_epoch: Option<u64>,
    },
    DirectJournalRpcAttemptStarted {
        attempt_id: String,
        target_node_id: sinomo_core::node::NodeId,
        route: String,
        command: Arc<crate::DfsMetadataJournalBatchCommand>,
    },
    DirectJournalRpcAttemptReturned {
        attempt_id: String,
        response: Option<crate::DfsMetadataJournalBatchResponse>,
        error_detail: Option<String>,
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    },
    /// Dropping an unresolved direct-call observer asserts no terminal outcome.
    DirectJournalRpcAttemptAbandoned { attempt_id: String },
    RpcAttemptStarted {
        attempt_id: String,
        target_node_id: sinomo_core::node::NodeId,
        command: crate::DfsMessageCommand,
    },
    RpcAttemptReturned {
        attempt_id: String,
        response: Option<crate::DfsMessageResponse>,
        error_detail: Option<String>,
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    },
    /// The future/stack stopped owning an unresolved actual call. This alone
    /// does not identify cancellation, delivery failure, or a terminal effect.
    RpcAttemptAbandoned { attempt_id: String },
}
/// Compact SAME validated StageBlob command; no content-body copy or permission.
#[derive(Debug, Serialize)]
pub(crate) struct NativeContentStageIdentityV1 {
    request_id: String,
    intent_id: String,
    file_system_id: crate::DfsFileSystemId,
    object_id: crate::DfsObjectId,
    version: crate::DfsVersion,
    content_hash: String,
}
impl From<&crate::DfsStageBlobCommand> for NativeContentStageIdentityV1 {
    fn from(command: &crate::DfsStageBlobCommand) -> Self {
        Self {
            request_id: command.request_id.clone(),
            intent_id: command.intent_id.clone(),
            file_system_id: command.file_system_id.clone(),
            object_id: command.object_id.clone(),
            version: command.version.clone(),
            content_hash: command.content_hash.clone(),
        }
    }
}
#[derive(Debug, Serialize)]
pub(crate) struct NativeReplicaPermitTupleV1 {
    file_system_id: crate::DfsFileSystemId,
    shard_id: crate::DfsMetadataShardId,
    source_node_id: sinomo_core::node::NodeId,
    source_epoch: u64,
    pending_primary_handoff_id: Option<String>,
}
impl NativeReplicaPermitTupleV1 {
    pub(crate) fn observed(permit: &crate::DfsMetadataReplicaIngestPermit) -> Self {
        Self {
            file_system_id: permit.file_system_id().clone(),
            shard_id: permit.shard_id().clone(),
            source_node_id: permit.source_node_id().clone(),
            source_epoch: permit.source_epoch(),
            pending_primary_handoff_id: permit.pending_primary_handoff_id().map(str::to_owned),
        }
    }
}
/// SAME checked-out replica resident and mutation-admission custody.
/// These aliases label actual retained handles; no inode or author proof is
/// inferred, and the transient native journal writer may already be released.
#[derive(Clone, Debug, Serialize)]
struct NativeResidentReplicaCustodyV1 {
    actual_mount_label: String,
    local_node_id: sinomo_core::node::NodeId,
    backend_root: PathBuf,
    file_system_id: crate::DfsFileSystemId,
    shard_id: crate::DfsMetadataShardId,
    resident_handle_token: String,
    data_owner_token: String,
    admission_scheduler_token: String,
    admitted_scope: NativeResidentAdmittedScopeV1,
    admission_kind: &'static str,
    observer_owner_ordinal: String,
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum NativeResidentAdmittedScopeV1 {
    ExactShard {
        file_system_id: crate::DfsFileSystemId,
        shard_id: crate::DfsMetadataShardId,
    },
    GlobalExclusive,
}
#[derive(Debug, Serialize)]
struct NativeBoundaryPacketV1 {
    owner_label: String,
    owner_ordinal: String,
    facts: NativeBoundaryFactsV1,
    resident_custody: Option<NativeResidentReplicaCustodyV1>,
    normal_replica_freeze_read_id: Option<String>,
    before_held_json: Option<Vec<u8>>,
    after_held_json: Option<Vec<u8>>,
}
struct HeldBoundaryPacket {
    ordinal: u64,
    facts: NativeBoundaryFactsV1,
    resident_custody: Option<NativeResidentReplicaCustodyV1>,
    normal_replica_freeze_read_id: Option<String>,
    before: Option<NativeHeldMetadataSnapshotV1>,
    after: Option<NativeHeldMetadataSnapshotV1>,
}
pub(crate) struct NativeBoundaryObservation {
    capture: Arc<Capture>,
    held: Vec<HeldBoundaryPacket>,
    resident_custody: Option<NativeResidentReplicaCustodyV1>,
}
pub(crate) fn boundary_interest(
    root: &Path,
    owner: Option<&Arc<DfsDataOwnerLock>>,
    shard: &DfsMetadataShardId,
) -> Option<NativeBoundaryObservation> {
    let actual = owner?;
    let registry = CAPTURES.get()?;
    let capture = registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .find(|capture| {
            capture.shard == *shard
                && root.starts_with(&capture.mount.root)
                && !*capture.mount.shutdown.borrow()
                && capture
                    .mount
                    .owner
                    .upgrade()
                    .is_some_and(|mounted| Arc::ptr_eq(&mounted, actual))
        })?;
    {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return None;
        }
        state.in_flight = state.in_flight.checked_add(1)?;
    }
    Some(NativeBoundaryObservation {
        capture,
        held: Vec::new(),
        resident_custody: None,
    })
}
impl NativeBoundaryObservation {
    pub(crate) fn uncertain(&self) {
        let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
        state.incomplete = state.incomplete.saturating_add(1);
    }
    /// Ordinal assignment happens at the SAME completed normal boundary.
    /// Declare the observation before actual writer/fence leases so drop/encode
    /// follows their release. It retains no owner/writer capability itself.
    pub(crate) fn observe(
        &mut self,
        facts: NativeBoundaryFactsV1,
        before: Option<NativeHeldMetadataSnapshotV1>,
        after: Option<NativeHeldMetadataSnapshotV1>,
    ) {
        let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.packet_count() >= MAX_PACKETS {
            state.dropped = state.dropped.saturating_add(1);
            return;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            return;
        };
        state.next_ordinal = ordinal;
        state.pending_boundary_packets = state.pending_boundary_packets.saturating_add(1);
        self.held.push(HeldBoundaryPacket {
            ordinal,
            facts,
            resident_custody: self.resident_custody.clone().map(|mut actual| {
                actual.observer_owner_ordinal = ordinal.to_string();
                actual
            }),
            normal_replica_freeze_read_id: BOUNDARY_CONTEXT.with(|slot| {
                let context = slot.borrow();
                context
                    .as_ref()
                    .filter(|context| {
                        context
                            .capture
                            .upgrade()
                            .is_some_and(|capture| Arc::ptr_eq(&capture, &self.capture))
                    })
                    .and_then(|context| context.normal_replica_freeze_read_id.clone())
            }),
            before,
            after,
        });
    }
}
impl Drop for NativeBoundaryObservation {
    fn drop(&mut self) {
        let mut state = self.capture.state.lock().unwrap_or_else(|e| e.into_inner());
        state.in_flight = state.in_flight.saturating_sub(1);
        for held in self.held.drain(..) {
            state.pending_boundary_packets = state.pending_boundary_packets.saturating_sub(1);
            let before = held.before.as_ref().map(serde_json::to_vec).transpose();
            let after = held.after.as_ref().map(serde_json::to_vec).transpose();
            let (Ok(before), Ok(after), Ok(facts), Ok(custody)) = (
                before,
                after,
                serde_json::to_vec(&held.facts),
                serde_json::to_vec(&held.resident_custody),
            ) else {
                state.incomplete = state.incomplete.saturating_add(1);
                continue;
            };
            let size = facts
                .len()
                .saturating_add(before.as_ref().map_or(0, Vec::len))
                .saturating_add(after.as_ref().map_or(0, Vec::len))
                .saturating_add(custody.len());
            if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(size) > MAX_BYTES {
                state.dropped = state.dropped.saturating_add(1);
                continue;
            }
            state.bytes = state.bytes.saturating_add(size);
            state.boundary_packets.push(NativeBoundaryPacketV1 {
                owner_label: self.capture.mount.label.clone(),
                owner_ordinal: held.ordinal.to_string(),
                facts: held.facts,
                resident_custody: held.resident_custody,
                normal_replica_freeze_read_id: held.normal_replica_freeze_read_id,
                before_held_json: before,
                after_held_json: after,
            });
        }
    }
}

struct NativeBoundaryContext {
    capture: Weak<Capture>,
    resident_custody: Option<NativeResidentReplicaCustodyV1>,
    normal_replica_freeze_read_id: Option<String>,
    // These copy-only packets serialize when the outer synchronous callback
    // releases its context, after the ordinary metadata writer has stopped.
    pending_read_observations: Vec<NativeBoundaryObservation>,
}
thread_local! { static BOUNDARY_CONTEXT: std::cell::RefCell<Option<NativeBoundaryContext>> = const { std::cell::RefCell::new(None) }; }
pub(crate) struct NativeBoundaryContextLease(Option<NativeBoundaryContext>);
impl Drop for NativeBoundaryContextLease {
    fn drop(&mut self) {
        BOUNDARY_CONTEXT.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
pub(crate) fn enter_boundary_context(
    root: &Path,
    owner: &Arc<DfsDataOwnerLock>,
    shard: &DfsMetadataShardId,
) -> NativeBoundaryContextLease {
    let capture = CAPTURES.get().and_then(|registry| {
        registry.lock().ok().and_then(|captures| {
            captures.iter().filter_map(Weak::upgrade).find(|capture| {
                capture.shard == *shard
                    && root.starts_with(&capture.mount.root)
                    && !*capture.mount.shutdown.borrow()
                    && capture
                        .mount
                        .owner
                        .upgrade()
                        .is_some_and(|mounted| Arc::ptr_eq(&mounted, owner))
            })
        })
    });
    let value = capture.map(|capture| NativeBoundaryContext {
        capture: Arc::downgrade(&capture),
        resident_custody: None,
        normal_replica_freeze_read_id: None,
        pending_read_observations: Vec::new(),
    });
    NativeBoundaryContextLease(BOUNDARY_CONTEXT.with(|slot| slot.replace(value)))
}
/// Copy the normal already-admitted replica operation. The TLS lease is
/// confined to its synchronous with_backend callback and never spans await.
#[allow(clippy::too_many_arguments)]
pub(crate) fn enter_resident_replica_boundary_context(
    root: &Path,
    owner: &Arc<DfsDataOwnerLock>,
    handle: &crate::metadata_sharded_backend::ResidentLocalMetadataShardHandle,
    local_node: &sinomo_core::node::NodeId,
    admission: &crate::runtime_admission::DfsMutationAdmission,
) -> NativeBoundaryContextLease {
    let prior = enter_boundary_context(root, owner, handle.shard_id());
    BOUNDARY_CONTEXT.with(|slot| {
        let mut borrowed = slot.borrow_mut();
        let Some(context) = borrowed.as_mut() else {
            return;
        };
        let Some(capture) = context.capture.upgrade() else {
            return;
        };
        let (backend_root, resident_handle_token) = handle.native_replica_observation_identity();
        let (admitted_scope, admission_kind) = if admission.is_global() {
            (
                NativeResidentAdmittedScopeV1::GlobalExclusive,
                "global-exclusive",
            )
        } else if admission.is_exact() {
            (
                NativeResidentAdmittedScopeV1::ExactShard {
                    file_system_id: handle.file_system_id().clone(),
                    shard_id: handle.shard_id().clone(),
                },
                "exact-shard",
            )
        } else {
            return;
        };
        context.resident_custody = Some(NativeResidentReplicaCustodyV1 {
            actual_mount_label: capture.mount.label.clone(),
            local_node_id: local_node.clone(),
            backend_root,
            file_system_id: handle.file_system_id().clone(),
            shard_id: handle.shard_id().clone(),
            resident_handle_token,
            data_owner_token: format!("{:p}", Arc::as_ptr(owner)),
            admission_scheduler_token: format!("{:p}", Arc::as_ptr(&admission.scheduler())),
            admitted_scope,
            admission_kind,
            observer_owner_ordinal: String::new(),
        });
    });
    prior
}
/// Copy the SAME successful marker read performed by replica ingestion under
/// its already-held writer. No new read, writer or publication capability.
pub(crate) fn observe_replica_write_freeze_read(
    writer: &crate::metadata_lock::MetadataWriteLock,
    permit: &crate::DfsMetadataReplicaIngestPermit,
    observed_freeze: &Option<crate::metadata_api::DfsMetadataShardWriteFreeze>,
    backend: &crate::LocalSobjMetadataBackend,
) {
    let Some(mut trace) = current_boundary_interest() else {
        return;
    };
    let Some(custody) = trace.resident_custody.as_ref() else {
        return;
    };
    let held = backend.native_prepared_kernel_held_snapshot_v1(None);
    if custody.backend_root != held.backend_root
        || custody.file_system_id != *permit.file_system_id()
        || custody.shard_id != *permit.shard_id()
        || held.shard_id != *permit.shard_id()
    {
        return;
    }
    let read_id = format!("native-replica-freeze-read:{}", sinomo_core::Id::generate());
    BOUNDARY_CONTEXT.with(|slot| {
        if let Some(context) = slot.borrow_mut().as_mut() {
            context.normal_replica_freeze_read_id = Some(read_id.clone());
        }
    });
    // The getter's earlier NotObserved bytes are preserved on both sides.
    // The read result is a separate ordinary-I/O observation with its own
    // ordinal, assigned now before any durable ingest can occur.
    trace.observe(
        NativeBoundaryFactsV1::ReplicaWriteFreezeRead {
            read_id,
            permit: NativeReplicaPermitTupleV1::observed(permit),
            observed_freeze: observed_freeze.clone().into(),
            actual_writer_token: format!("{:p}", writer),
        },
        Some(held.clone()),
        Some(held),
    );
    BOUNDARY_CONTEXT.with(|slot| {
        if let Some(context) = slot.borrow_mut().as_mut() {
            context.pending_read_observations.push(trace);
        }
    });
}

pub(crate) fn current_boundary_interest() -> Option<NativeBoundaryObservation> {
    let (capture, resident_custody) = BOUNDARY_CONTEXT.with(|slot| {
        let slot = slot.borrow();
        let current = slot.as_ref()?;
        Some((current.capture.upgrade()?, current.resident_custody.clone()))
    })?;
    {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return None;
        }
        state.in_flight = state.in_flight.checked_add(1)?;
    }
    Some(NativeBoundaryObservation {
        capture,
        held: Vec::new(),
        resident_custody,
    })
}

/// Captures only bounded ordinary transaction input/output. Its serialization
/// is deferred to explicit export, so a control-plane callback never formats
/// evidence while a higher caller retains the actual shard writer/fence.
#[derive(Debug, Serialize)]
struct NativeControlCommitObservationV1 {
    owner_label: String,
    owner_ordinal: String,
    commit: crate::DfsNativeTxnCommit,
}
/// Conservative byte charge for this exact current native receipt schema.
/// It walks existing bounded fields without encoding or creating temporary
/// buffers. Six bytes cover worst JSON string escaping; fixed overhead covers
/// enum/field names, digests, authority scalars, optional IDs and punctuation.
fn native_control_commit_byte_bound(
    request: &sinomo_core::kv::KvTxnRequest,
    result: &sinomo_core::kv::ClusterKvTxnCompactCommitResult,
) -> usize {
    native_control_txn_byte_bound(
        &request.txn,
        &request.request_id,
        &result.request_id,
        result.txn_response.as_ref(),
    )
}
fn native_control_txn_byte_bound(
    txn: &sinomo_core::kv::KvTxn,
    request_id: &sinomo_core::kv::KvRequestId,
    result_request_id: &sinomo_core::kv::KvRequestId,
    response: Option<&sinomo_core::kv::KvTxnResponse>,
) -> usize {
    use sinomo_core::kv::{KvCompare, KvOp, KvOpResponse};
    let string = |len: usize| len.saturating_mul(6);
    let mut size = 4096usize
        .saturating_add(string(request_id.as_str().len()))
        .saturating_add(string(result_request_id.as_str().len()));
    for compare in &txn.compares {
        let (key, value_len) = match compare {
            KvCompare::Exists { key }
            | KvCompare::Missing { key }
            | KvCompare::VersionEquals { key, .. }
            | KvCompare::CreateRevisionEquals { key, .. }
            | KvCompare::ModifyRevisionEquals { key, .. }
            | KvCompare::LeaseEquals { key, .. } => (key, 0),
            KvCompare::ValueEquals { key, value } => (key, value.len()),
        };
        size = size
            .saturating_add(256)
            .saturating_add(string(key.len()))
            .saturating_add(value_len.saturating_mul(4));
    }
    for op in txn.success.iter().chain(&txn.failure) {
        let (key_len, value_len) = match op {
            KvOp::Put { key, value, .. } => (key.len(), value.len()),
            KvOp::Delete { key } => (key.len(), 0),
            KvOp::DeletePrefix { prefix } => (prefix.len(), 0),
            KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. } => {
                (0, 0)
            }
        };
        size = size
            .saturating_add(256)
            .saturating_add(string(key_len))
            .saturating_add(value_len.saturating_mul(4));
    }
    if let Some(response) = response {
        for op in &response.responses {
            let key_len = match op {
                KvOpResponse::Put { key, .. } | KvOpResponse::Delete { key, .. } => key.len(),
                KvOpResponse::DeletePrefix { prefix, .. } => prefix.len(),
                KvOpResponse::LeaseGrant { .. }
                | KvOpResponse::LeaseKeepAlive { .. }
                | KvOpResponse::LeaseRevoke { .. } => 0,
            };
            size = size.saturating_add(256).saturating_add(string(key_len));
        }
    }
    size
}
/// The original producer fact is separate from any later caller replay result.
#[derive(Debug, Serialize)]
struct NativeOriginalControlCommitObservationV1 {
    owner_label: String,
    owner_ordinal: String,
    original: Arc<sinomo_core::kv::KvOriginalCurrentTermCommit>,
}
fn record_native_original_control_commit(
    capture: &Arc<Capture>,
    original: &Arc<sinomo_core::kv::KvOriginalCurrentTermCommit>,
) {
    let sinomo_core::kv::KvCommand::Txn(txn) = &original.request.command else {
        return;
    };
    // Shard suffix selects observation interest only: Mount has no filesystem
    // scope. Retain the full literal keys for independent exact-scope validation.
    if !txn.compares.iter().any(|compare| match compare {
        sinomo_core::kv::KvCompare::ValueEquals { key, .. }
        | sinomo_core::kv::KvCompare::Missing { key } => key.ends_with(&format!(
            "/metadata-shards/{}",
            capture.shard.0.trim_matches('/').replace('/', "_")
        )),
        _ => false,
    }) {
        return;
    }
    let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
    if state.closed
        || state
            .original_control_commits
            .iter()
            .any(|prior| prior.original == *original)
    {
        return;
    }
    let bytes = native_control_txn_byte_bound(
        txn,
        &original.request.request_id,
        &original.committed.request_id,
        original.committed.txn_response.as_ref(),
    )
    .saturating_add(original.root.as_os_str().len().saturating_mul(6))
    .saturating_add(capture.mount.label.len().saturating_mul(6));
    if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(bytes) > MAX_BYTES {
        state.dropped = state.dropped.saturating_add(1);
        return;
    }
    let Some(ordinal) = state.next_ordinal.checked_add(1) else {
        state.dropped = state.dropped.saturating_add(1);
        return;
    };
    state.next_ordinal = ordinal;
    state.bytes = state.bytes.saturating_add(bytes);
    state
        .original_control_commits
        .push(NativeOriginalControlCommitObservationV1 {
            owner_label: capture.mount.label.clone(),
            owner_ordinal: ordinal.to_string(),
            original: Arc::clone(original),
        });
}
pub(crate) fn record_native_control_commit(
    root: &Path,
    owner: Option<&Arc<DfsDataOwnerLock>>,
    request: &sinomo_core::kv::KvTxnRequest,
    result: &sinomo_core::kv::ClusterKvTxnCompactCommitResult,
) {
    let (Some(owner), Some(registry)) = (owner, CAPTURES.get()) else {
        return;
    };
    let captures = registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|capture| {
            capture.mount.root == root
                && !*capture.mount.shutdown.borrow()
                && capture
                    .mount
                    .owner
                    .upgrade()
                    .is_some_and(|mounted| Arc::ptr_eq(&mounted, owner))
        })
        .filter(|capture| {
            request.txn.compares.iter().any(|compare| match compare {
                sinomo_core::kv::KvCompare::ValueEquals { key, .. }
                | sinomo_core::kv::KvCompare::Missing { key } => key.ends_with(&format!(
                    "/metadata-shards/{}",
                    capture.shard.0.trim_matches('/').replace('/', "_")
                )),
                _ => false,
            })
        })
        .collect::<Vec<_>>();
    for capture in captures {
        let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            continue;
        }
        let bytes = native_control_commit_byte_bound(request, result)
            .saturating_add(capture.mount.label.len().saturating_mul(6));
        if state.packet_count() >= MAX_PACKETS || state.bytes.saturating_add(bytes) > MAX_BYTES {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        }
        let Some(ordinal) = state.next_ordinal.checked_add(1) else {
            state.dropped = state.dropped.saturating_add(1);
            continue;
        };
        state.next_ordinal = ordinal;
        state.bytes = state.bytes.saturating_add(bytes);
        state
            .control_commits
            .push(NativeControlCommitObservationV1 {
                owner_label: capture.mount.label.clone(),
                owner_ordinal: ordinal.to_string(),
                commit: crate::DfsNativeTxnCommit {
                    request: request.clone(),
                    result: result.clone(),
                },
            });
    }
}

/// A copy-only observer for a normal outgoing call. Never retains a data owner,
/// writer, admission, or transport future and never changes retry behavior.
pub(crate) struct NativeRpcAttemptObservation {
    trace: NativeBoundaryObservation,
    attempt_id: String,
    returned: bool,
    direct_journal: bool,
}
#[derive(Clone)]
pub(crate) struct NativeRpcAttemptContext(Weak<Capture>);
pub(crate) fn rpc_attempt_context() -> Option<NativeRpcAttemptContext> {
    BOUNDARY_CONTEXT.with(|slot| {
        Some(NativeRpcAttemptContext(
            slot.borrow().as_ref()?.capture.clone(),
        ))
    })
}
/// Resolve only a copy observer for this normal source's already-owned root.
/// Registry lookup creates no owner, authority read, or transport. The returned
/// weak context carries no owner/file/admission lease into the actual future.
pub(crate) fn direct_journal_attempt_context(
    data_dir: &sinomo_core::local::SinomoDataDir,
    command: &crate::DfsMetadataJournalBatchCommand,
) -> Option<NativeRpcAttemptContext> {
    CAPTURES.get()?;
    let source = command.source_node_id.as_ref()?;
    let root = crate::local_dfs_metadata_shard_path(data_dir, &command.batch.shard_id);
    let owner = crate::runtime_host::publication_runtime_owner(&root).ok()??;
    let trace = boundary_interest(&root, Some(&owner), &command.batch.shard_id)?;
    let matches_source = trace
        .capture
        .mount
        .identity
        .lock()
        .ok()?
        .as_ref()
        .is_some_and(|actual| &actual.local_node_id == source);
    matches_source.then(|| NativeRpcAttemptContext(Arc::downgrade(&trace.capture)))
}

impl NativeRpcAttemptContext {
    pub(crate) fn accepted_direct_journal(
        self,
        target: &sinomo_core::node::NodeId,
        command: &crate::DfsMetadataJournalBatchCommand,
        response: &crate::DfsMetadataJournalBatchResponse,
    ) {
        if let Some(mut trace) = self.into_boundary() {
            trace.observe(
                NativeBoundaryFactsV1::SourceJournalResponseAccepted {
                    authenticated_source_node_id: target.clone(),
                    response: response.clone(),
                    expected_target_node_id: target.clone(),
                    expected_next_after_sequence: command.batch.next_after_sequence,
                    expected_prefix_only: command.verify_prefix_only,
                    expected_confirmation_id: command
                        .authority_probe
                        .as_ref()
                        .map(|probe| probe.probe_id.clone())
                        .or_else(|| {
                            command
                                .prefix_probe
                                .as_ref()
                                .and_then(|probe| probe.primary_handoff_id.clone())
                        })
                        .or_else(|| {
                            command
                                .durable_replication
                                .as_ref()
                                .map(|proof| proof.confirmation_id.clone())
                        }),
                },
                None,
                None,
            );
        }
    }
    fn into_boundary(self) -> Option<NativeBoundaryObservation> {
        let capture = self.0.upgrade()?;
        if *capture.mount.shutdown.borrow() || capture.mount.owner.upgrade().is_none() {
            return None;
        }
        {
            let mut state = capture.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.closed {
                return None;
            }
            state.in_flight = state.in_flight.checked_add(1)?;
        }
        Some(NativeBoundaryObservation {
            capture,
            held: Vec::new(),
            resident_custody: None,
        })
    }
    /// Called only after ordinary encoding succeeds and immediately before
    /// polling the actual native peer call. Future construction emits nothing.
    pub(crate) fn begin(
        self,
        target: &sinomo_core::node::NodeId,
        command: &crate::DfsMessageCommand,
    ) -> Option<NativeRpcAttemptObservation> {
        begin_rpc_attempt_with_trace(self.into_boundary()?, target, command)
    }
    pub(crate) fn begin_direct_journal(
        self,
        target: &sinomo_core::node::NodeId,
        route: &str,
        command: Arc<crate::DfsMetadataJournalBatchCommand>,
    ) -> Option<NativeRpcAttemptObservation> {
        let mut trace = self.into_boundary()?;
        let attempt_id = format!("native-rpc-attempt:{}", sinomo_core::Id::generate());
        trace.observe(
            NativeBoundaryFactsV1::DirectJournalRpcAttemptStarted {
                attempt_id: attempt_id.clone(),
                target_node_id: target.clone(),
                route: route.to_owned(),
                command,
            },
            None,
            None,
        );
        Some(NativeRpcAttemptObservation {
            trace,
            attempt_id,
            returned: false,
            direct_journal: true,
        })
    }
}
pub(crate) fn begin_rpc_attempt(
    target: &sinomo_core::node::NodeId,
    command: &crate::DfsMessageCommand,
) -> Option<NativeRpcAttemptObservation> {
    begin_rpc_attempt_with_trace(current_boundary_interest()?, target, command)
}
fn begin_rpc_attempt_with_trace(
    mut trace: NativeBoundaryObservation,
    target: &sinomo_core::node::NodeId,
    command: &crate::DfsMessageCommand,
) -> Option<NativeRpcAttemptObservation> {
    let attempt_id = format!("native-rpc-attempt:{}", sinomo_core::Id::generate());
    trace.observe(
        NativeBoundaryFactsV1::RpcAttemptStarted {
            attempt_id: attempt_id.clone(),
            target_node_id: target.clone(),
            command: command.clone(),
        },
        None,
        None,
    );
    Some(NativeRpcAttemptObservation {
        trace,
        attempt_id,
        returned: false,
        direct_journal: false,
    })
}
impl NativeRpcAttemptObservation {
    pub(crate) fn returned_direct_journal(
        &mut self,
        response: Option<&crate::DfsMetadataJournalBatchResponse>,
        error_detail: Option<String>,
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    ) {
        self.trace.observe(
            NativeBoundaryFactsV1::DirectJournalRpcAttemptReturned {
                attempt_id: self.attempt_id.clone(),
                response: response.cloned(),
                error_detail,
                delivery_state,
            },
            None,
            None,
        );
        self.returned = true;
    }
    pub(crate) fn returned(
        &mut self,
        actual: &Result<crate::DfsMessageResponse, crate::DfsPeerTransportError>,
    ) {
        let (response, error_detail, delivery_state) = match actual {
            Ok(response) => (Some(response.clone()), None, None),
            Err(error) => (None, Some(error.to_string()), error.delivery_state()),
        };
        self.trace.observe(
            NativeBoundaryFactsV1::RpcAttemptReturned {
                attempt_id: self.attempt_id.clone(),
                response,
                error_detail,
                delivery_state,
            },
            None,
            None,
        );
        self.returned = true;
    }
}
impl Drop for NativeRpcAttemptObservation {
    fn drop(&mut self) {
        if !self.returned {
            self.trace.observe(
                if self.direct_journal {
                    NativeBoundaryFactsV1::DirectJournalRpcAttemptAbandoned {
                        attempt_id: self.attempt_id.clone(),
                    }
                } else {
                    NativeBoundaryFactsV1::RpcAttemptAbandoned {
                        attempt_id: self.attempt_id.clone(),
                    }
                },
                None,
                None,
            );
        }
    }
}
