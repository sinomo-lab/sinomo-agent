//! Selected local fence DATA. No authenticated return, chosen value, custody
//! coverage, promise, serving authority or activation is constructed here.
use super::*;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub(crate) const SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT: usize = 256;
const MAX_MARKER_BYTES: u64 = 1024 * 1024;

/// Negative-only cuts in one fresh selected marker publication. Neither cut
/// is a returned durable result, native RPC acknowledgement or authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectedRecoveryWriterCut {
    BeforeMarkerPublication,
    AfterMarkerNamespaceSync,
}

#[derive(Clone, Debug)]
pub(crate) struct SelectedRecoveryWriterObservation {
    pub(crate) cut: SelectedRecoveryWriterCut,
    pub(crate) marker_path: PathBuf,
    pub(crate) fence: DfsSelectedLocalRecoveryFence,
    /// None before the atomic publisher; exact checked bytes after namespace
    /// sync. This field is observation DATA, never a durable response token.
    pub(crate) stored_marker_bytes: Option<Vec<u8>>,
}

struct SelectedRecoveryWriterHookState {
    shard_root: PathBuf,
    request: crate::control_plane::DfsMetadataShardRecoveryBallotRequest,
    cut: SelectedRecoveryWriterCut,
    deadline: Instant,
    observer: Box<dyn Fn(&SelectedRecoveryWriterObservation) -> Result<()>>,
    hits: std::cell::Cell<usize>,
    released: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
}

thread_local! {
    // The actual synchronous selected writer and its owning test worker are
    // the only participants. No process-wide registry or admission bit.
    static SELECTED_RECOVERY_WRITER_HOOK: std::cell::RefCell<Option<std::rc::Rc<SelectedRecoveryWriterHookState>>> = const { std::cell::RefCell::new(None) };
}

/// Same-worker scope; Rc deliberately prevents moving a live registration to
/// another worker. Drop always releases its own pause without renewing time.
pub(crate) struct SelectedRecoveryWriterHook {
    state: std::rc::Rc<SelectedRecoveryWriterHookState>,
}

impl SelectedRecoveryWriterHook {
    pub(crate) fn install(
        shard_root: PathBuf,
        request: crate::control_plane::DfsMetadataShardRecoveryBallotRequest,
        cut: SelectedRecoveryWriterCut,
        deadline: Instant,
        observer: impl Fn(&SelectedRecoveryWriterObservation) -> Result<()> + 'static,
    ) -> Result<Self> {
        if deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| remaining > sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        {
            return Err(selected_layout_rejection(
                "selected recovery writer hook exceeds the existing call budget",
            ));
        }
        let state = std::rc::Rc::new(SelectedRecoveryWriterHookState {
            shard_root,
            request,
            cut,
            deadline,
            observer: Box::new(observer),
            hits: std::cell::Cell::new(0),
            released: std::sync::Mutex::new(false),
            release: std::sync::Condvar::new(),
        });
        SELECTED_RECOVERY_WRITER_HOOK.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_some() {
                return Err(selected_layout_rejection(
                    "selected recovery writer hook already registered on this worker",
                ));
            }
            *slot = Some(std::rc::Rc::clone(&state));
            Ok(Self { state })
        })
    }

    pub(crate) fn hits(&self) -> usize {
        self.state.hits.get()
    }

    pub(crate) fn release(&self) {
        *self
            .state
            .released
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = true;
        self.state.release.notify_all();
    }
}

impl Drop for SelectedRecoveryWriterHook {
    fn drop(&mut self) {
        self.release();
        SELECTED_RECOVERY_WRITER_HOOK.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|current| std::rc::Rc::ptr_eq(current, &self.state))
            {
                *slot = None;
            }
        });
    }
}

fn observe_selected_recovery_writer_cut(
    path: &Path,
    fence: &DfsSelectedLocalRecoveryFence,
    cut: SelectedRecoveryWriterCut,
    stored_marker_bytes: Option<&[u8]>,
) -> Result<()> {
    let hook = SELECTED_RECOVERY_WRITER_HOOK.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|hook| {
                hook.cut == cut
                    && hook.shard_root == fence.physical.shard_root
                    && hook.request == fence.reservation
                    && path
                        == hook
                            .shard_root
                            .join("journal/membership-handoff-write-freeze.sobj")
            })
            .cloned()
    });
    let Some(hook) = hook else {
        return Ok(());
    };
    let expired =
        || selected_layout_rejection("selected recovery writer hook original deadline expired");
    if Instant::now() >= hook.deadline {
        return Err(expired());
    }
    if hook.hits.replace(hook.hits.get().saturating_add(1)) != 0 {
        return Err(selected_layout_rejection(
            "selected recovery writer hook fresh-publication budget exhausted",
        ));
    }
    // The thread-local slot is no longer borrowed. The observer may publish
    // bounded Ready DATA, but does not receive the layout, writer or a permit.
    (hook.observer)(&SelectedRecoveryWriterObservation {
        cut,
        marker_path: path.to_path_buf(),
        fence: fence.clone(),
        stored_marker_bytes: stored_marker_bytes.map(<[u8]>::to_vec),
    })?;
    let mut released = hook
        .released
        .lock()
        .map_err(|_| selected_layout_rejection("selected recovery writer hook release poisoned"))?;
    loop {
        let remaining = hook
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(expired)?;
        if *released {
            break;
        }
        released = hook
            .release
            .wait_timeout(released, remaining)
            .map_err(|_| selected_layout_rejection("selected recovery writer hook wait poisoned"))?
            .0;
    }
    // A release which was observed only after descheduling cannot extend the
    // original call deadline or turn an expired cut into a successful result.
    if Instant::now() >= hook.deadline {
        return Err(expired());
    }
    Ok(())
}

thread_local! {
    /// Negative-only boundary in this selected writer; tests restore it on
    /// every exit. It cannot publish or acknowledge any DATA.
    pub(crate) static FAIL_SELECTED_LOCAL_RECOVERY_NAMESPACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn sync_namespace(
    stage: &crate::repair_queue_store::SelectedOriginalFixtureIdentity,
    path: &Path,
) -> Result<()> {
    if FAIL_SELECTED_LOCAL_RECOVERY_NAMESPACE.replace(false) {
        return Err(selected_layout_rejection(
            "injected selected local recovery namespace failure",
        ));
    }
    crate::repair_handoff::sync_selected_namespace(stage, path)
        .map_err(|error| selected_layout_rejection(error.to_string()))
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DfsLocalRecoveryInventoryKind {
    LegacyPrimaryAuthoredV7,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DfsSelectedLocalRecoveryPhysicalIdentity {
    pub(crate) data_root: PathBuf,
    pub(crate) shard_root: PathBuf,
    pub(crate) data_device: u64,
    pub(crate) data_inode: u64,
    pub(crate) owner_device: u64,
    pub(crate) owner_inode: u64,
    pub(crate) shard_device: u64,
    pub(crate) shard_inode: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DfsSelectedLocalRecoveryFence {
    pub(crate) reservation: crate::control_plane::DfsMetadataShardRecoveryBallotRequest,
    pub(crate) acceptor_node_id: NodeId,
    pub(crate) physical: DfsSelectedLocalRecoveryPhysicalIdentity,
    pub(crate) prefix: crate::DfsMetadataPrefixProof,
    pub(crate) unresolved_prepares: Vec<crate::DfsPreparedOperationDigest>,
    pub(crate) inventory_kind: DfsLocalRecoveryInventoryKind,
}

/// Historical bytes only. Reading this type does not re-certify durability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsSelectedLocalRecoveryFenceData {
    pub(crate) fence: DfsSelectedLocalRecoveryFence,
    pub(crate) marker_bytes: Vec<u8>,
}

/// The actual local holder is independent from the requested candidate. This
/// context is neither serialized nor reconstructed from the marker.
pub(crate) struct DfsSelectedLocalRecoveryContext {
    pub(super) layout: Arc<DfsSelectedOriginalDataLayout>,
    owner: Arc<crate::runtime_host::DfsDataOwnerLock>,
    local_node_id: NodeId,
}

impl DfsSelectedLocalRecoveryContext {
    pub(crate) fn from_owner(
        layout: Arc<DfsSelectedOriginalDataLayout>,
        owner: Arc<crate::runtime_host::DfsDataOwnerLock>,
    ) -> Result<Self> {
        layout.check_owner(&owner)?;
        let data = sinomo_core::local::SinomoDataDir::new(&layout.data_root);
        let local_node_id = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&data),
        )
        .map_err(|error| selected_layout_rejection(error.to_string()))?
        .local_node_id;
        Ok(Self {
            layout,
            owner,
            local_node_id,
        })
    }
    pub(crate) fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
    pub(crate) fn layout(&self) -> &Arc<DfsSelectedOriginalDataLayout> {
        &self.layout
    }
    pub(crate) fn check(&self) -> Result<()> {
        self.layout.check_owner(&self.owner)?;
        let data = sinomo_core::local::SinomoDataDir::new(&self.layout.data_root);
        let local = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&data),
        )
        .map_err(|error| selected_layout_rejection(error.to_string()))?
        .local_node_id;
        if local != self.local_node_id {
            return Err(selected_layout_rejection(
                "selected recovery local identity changed",
            ));
        }
        Ok(())
    }
    pub(crate) fn physical_identity(
        &self,
        fs: &DfsFileSystemId,
        shard: &crate::DfsMetadataShardId,
        root: &Path,
    ) -> Result<DfsSelectedLocalRecoveryPhysicalIdentity> {
        self.check()?;
        self.layout.stage(fs, shard, root)?;
        physical_identity(&self.layout, root)
    }
}

fn physical_identity(
    layout: &DfsSelectedOriginalDataLayout,
    root: &Path,
) -> Result<DfsSelectedLocalRecoveryPhysicalIdentity> {
    check_handoff_layout_path(
        layout,
        &root.join("journal/membership-handoff-write-freeze.sobj"),
    )?;
    let (shard_device, shard_inode) = DfsSelectedOriginalDataLayout::directory_identity(root)?;
    Ok(DfsSelectedLocalRecoveryPhysicalIdentity {
        data_root: layout.data_root.clone(),
        shard_root: root.to_path_buf(),
        data_device: layout.data_identity.0,
        data_inode: layout.data_identity.1,
        owner_device: layout.owner_identity.0,
        owner_inode: layout.owner_identity.1,
        shard_device,
        shard_inode,
    })
}

impl DfsSelectedLocalRecoveryFence {
    pub(crate) fn validate(&self) -> Result<()> {
        let request = &self.reservation;
        let canonical = crate::control_plane::DfsMetadataShardRecoveryBallotRequest::prepare(
            &request.serving_shard,
            &request.ballot.candidate_node_id,
            request.expected_attempt_floor,
        )
        .map_err(|reason| {
            selected_layout_rejection(format!("invalid selected recovery reservation: {reason:?}"))
        })?;
        if &canonical != request
            || (self.acceptor_node_id != request.serving_shard.primary_node_id
                && !request
                    .serving_shard
                    .replica_node_ids
                    .contains(&self.acceptor_node_id))
            || !self.prefix.is_well_formed()
            || self.unresolved_prepares.len() > SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT
        {
            return Err(selected_layout_rejection(
                "selected recovery fence identity or inventory bound differs",
            ));
        }
        let mut previous: Option<&str> = None;
        for digest in &self.unresolved_prepares {
            if digest.protocol_version != 7
                || digest.file_system_id != request.serving_shard.file_system_id
                || digest.shard_id.0 != request.serving_shard.shard_id
                || digest.owner_epoch > request.serving_shard.epoch
                || digest.prepare_sequence == 0
                || digest.prepare_sequence > self.prefix.journal_sequence
                || digest.operation_id.is_empty()
                || !digest
                    .payload_digest
                    .strip_prefix("dfs-prepared-operation-v7:blake3:")
                    .is_some_and(|hash| {
                        hash.len() == 64
                            && hash
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    })
                || previous.is_some_and(|value| value >= digest.operation_id.as_str())
            {
                return Err(selected_layout_rejection(
                    "selected recovery inventory is malformed, unsorted or foreign",
                ));
            }
            previous = Some(&digest.operation_id);
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    selected_local_recovery_fence_v1: DfsSelectedLocalRecoveryFence,
}

fn read_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    let before = match std::fs::symlink_metadata(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(selected_layout_rejection(error.to_string())),
    };
    if !before.is_file() || before.file_type().is_symlink() || before.len() > MAX_MARKER_BYTES {
        return Err(selected_layout_rejection(
            "selected recovery marker is not a bounded regular file",
        ));
    }
    let mut file =
        std::fs::File::open(path).map_err(|error| selected_layout_rejection(error.to_string()))?;
    if !crate::repair_queue_store::same_file_observation(
        &before,
        &file
            .metadata()
            .map_err(|e| selected_layout_rejection(e.to_string()))?,
    ) {
        return Err(selected_layout_rejection(
            "selected recovery marker changed before read",
        ));
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    file.by_ref()
        .take(MAX_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| selected_layout_rejection(e.to_string()))?;
    let after =
        std::fs::symlink_metadata(path).map_err(|e| selected_layout_rejection(e.to_string()))?;
    if bytes.len() as u64 != before.len()
        || !crate::repair_queue_store::same_file_observation(&before, &after)
        || !crate::repair_queue_store::same_file_observation(
            &after,
            &file
                .metadata()
                .map_err(|e| selected_layout_rejection(e.to_string()))?,
        )
    {
        return Err(selected_layout_rejection(
            "selected recovery marker changed during read",
        ));
    }
    Ok(Some(bytes))
}

pub(crate) fn read_selected_local_recovery_fence(
    path: &Path,
    layout: &DfsSelectedOriginalDataLayout,
) -> Result<Option<DfsSelectedLocalRecoveryFenceData>> {
    check_handoff_layout_path(layout, path)?;
    let Some(marker_bytes) = read_bytes(path)? else {
        return Ok(None);
    };
    let wire: DfsMetadataShardWriteFreezeWire =
        sinomo_core::object::serde_ext::from_bytes(&marker_bytes)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
    if !matches!(
        wire,
        DfsMetadataShardWriteFreezeWire::LocalRecoveryV1 { .. }
    ) {
        return Ok(None);
    }
    let envelope: Envelope = sinomo_core::object::serde_ext::from_bytes(&marker_bytes)
        .map_err(|error| selected_layout_rejection(error.to_string()))?;
    let fence = envelope.selected_local_recovery_fence_v1;
    fence.validate()?;
    if crate::metadata_io::encode_sobj(&Envelope {
        selected_local_recovery_fence_v1: fence.clone(),
    })? != marker_bytes
    {
        return Err(selected_layout_rejection(
            "selected recovery marker is not its exact canonical envelope",
        ));
    }
    let root = path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| selected_layout_rejection("selected recovery marker has no shard root"))?;
    if fence.physical != physical_identity(layout, root)? {
        return Err(selected_layout_rejection(
            "selected recovery marker belongs to another physical owner",
        ));
    }
    layout.stage(
        &fence.reservation.serving_shard.file_system_id,
        &crate::DfsMetadataShardId::new(&fence.reservation.serving_shard.shard_id),
        root,
    )?;
    check_handoff_layout_path(layout, path)?;
    Ok(Some(DfsSelectedLocalRecoveryFenceData {
        fence,
        marker_bytes,
    }))
}

/// Synchronize the SAME current marker bytes without re-encoding a duplicate.
fn recertify(
    path: &Path,
    expected: &[u8],
    stage: &crate::repair_queue_store::SelectedOriginalFixtureIdentity,
) -> Result<()> {
    let before =
        std::fs::symlink_metadata(path).map_err(|e| selected_layout_rejection(e.to_string()))?;
    if !before.is_file() || before.file_type().is_symlink() || before.len() != expected.len() as u64
    {
        return Err(selected_layout_rejection(
            "selected recovery re-certification changed file type/size",
        ));
    }
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| selected_layout_rejection(e.to_string()))?;
    if !crate::repair_queue_store::same_file_observation(
        &before,
        &file
            .metadata()
            .map_err(|e| selected_layout_rejection(e.to_string()))?,
    ) {
        return Err(selected_layout_rejection(
            "selected recovery re-certification changed descriptor",
        ));
    }
    let mut actual = Vec::new();
    file.by_ref()
        .take(MAX_MARKER_BYTES + 1)
        .read_to_end(&mut actual)
        .map_err(|e| selected_layout_rejection(e.to_string()))?;
    if actual != expected {
        return Err(selected_layout_rejection(
            "selected recovery re-certification changed bytes",
        ));
    }
    sinomo_core::storage::durable_file::sync_open_file(
        &mut file,
        path,
        sinomo_core::storage::durable_file::DurableFileSync::All,
    )
    .map_err(|e| selected_layout_rejection(e.to_string()))?;
    sync_namespace(stage, path.parent().expect("checked journal path"))?;
    let after =
        std::fs::symlink_metadata(path).map_err(|e| selected_layout_rejection(e.to_string()))?;
    if !crate::repair_queue_store::same_file_observation(&before, &after)
        || !crate::repair_queue_store::same_file_observation(
            &after,
            &file
                .metadata()
                .map_err(|e| selected_layout_rejection(e.to_string()))?,
        )
    {
        return Err(selected_layout_rejection(
            "selected recovery marker changed across durability barriers",
        ));
    }
    stage
        .check()
        .map_err(|e| selected_layout_rejection(e.to_string()))
}

pub(crate) fn write_selected_local_recovery_fence(
    path: &Path,
    fence: &DfsSelectedLocalRecoveryFence,
    layout: &DfsSelectedOriginalDataLayout,
) -> Result<DfsSelectedLocalRecoveryFenceData> {
    check_handoff_layout_path(layout, path)?;
    fence.validate()?;
    let root = path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| selected_layout_rejection("selected recovery marker has no shard root"))?;
    let stage = layout.stage(
        &fence.reservation.serving_shard.file_system_id,
        &crate::DfsMetadataShardId::new(&fence.reservation.serving_shard.shard_id),
        root,
    )?;
    if fence.physical != physical_identity(layout, root)? {
        return Err(selected_layout_rejection(
            "selected recovery writer changed physical owner",
        ));
    }
    let envelope = Envelope {
        selected_local_recovery_fence_v1: fence.clone(),
    };
    let expected = crate::metadata_io::encode_sobj(&envelope)?;
    if expected.len() as u64 > MAX_MARKER_BYTES {
        return Err(selected_layout_rejection(
            "selected recovery inventory encoding exceeds its bound",
        ));
    }
    match read_selected_local_recovery_fence(path, layout)? {
        Some(existing) if existing.fence == *fence => {
            recertify(path, &existing.marker_bytes, &stage)?;
            check_handoff_layout_path(layout, path)?;
            return Ok(existing);
        }
        None if path.exists() => {
            return Err(selected_layout_rejection(
                "selected recovery refuses every cooperative handoff marker",
            ));
        }
        _ => {}
    }
    observe_selected_recovery_writer_cut(
        path,
        fence,
        SelectedRecoveryWriterCut::BeforeMarkerPublication,
        None,
    )?;
    let generation = crate::repair_handoff::publish_and_certify_selected(path, &envelope)
        .map_err(|e| selected_layout_rejection(e.to_string()))?;
    sync_namespace(&stage, path.parent().expect("checked journal path"))?;
    let after =
        std::fs::symlink_metadata(path).map_err(|e| selected_layout_rejection(e.to_string()))?;
    if !crate::repair_queue_store::same_file_observation(&generation, &after) {
        return Err(selected_layout_rejection(
            "selected recovery publication changed during namespace sync",
        ));
    }
    let stored = read_selected_local_recovery_fence(path, layout)?
        .ok_or_else(|| selected_layout_rejection("selected recovery publication disappeared"))?;
    if stored.fence != *fence || stored.marker_bytes != expected {
        return Err(selected_layout_rejection(
            "selected recovery publication differs from captured bytes",
        ));
    }
    observe_selected_recovery_writer_cut(
        path,
        fence,
        SelectedRecoveryWriterCut::AfterMarkerNamespaceSync,
        Some(&stored.marker_bytes),
    )?;
    Ok(stored)
}
