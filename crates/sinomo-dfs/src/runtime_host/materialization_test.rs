//! Shared native materialization boundary for durability regressions.
use super::*;

#[derive(Clone, Debug)]
pub struct DfsTestMaterializationCapture {
    pub physical_root: PathBuf,
    pub file_system_id: crate::DfsFileSystemId,
    pub shard_id: crate::DfsMetadataShardId,
    pub durable_sequence: Option<u64>,
    pub materialized_sequence: Option<u64>,
    pub prefix: Option<crate::DfsMetadataPrefixProof>,
    pub unavailable: Option<String>,
}

struct Gate {
    captures: Mutex<Vec<DfsTestMaterializationCapture>>,
    entered: tokio::sync::Semaphore,
    released: tokio::sync::Semaphore,
    worker: Mutex<Option<tokio::task::AbortHandle>>,
    worker_registered: tokio::sync::Semaphore,
    blocking_owner: Mutex<Option<std::sync::Weak<DaemonDataOwnerLock>>>,
}
static GATES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Gate>>>> = OnceLock::new();

/// Observe only the already-installed native slot. A caller shell's cached
/// replay cursor is not the resident materializer's completion boundary.
#[cfg(test)]
pub(crate) fn observe_existing_materialization(
    data_dir: &SinomoDataDir,
    file_system_id: &crate::DfsFileSystemId,
    shard_id: &crate::DfsMetadataShardId,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<DfsTestMaterializationCapture>, String> {
    let paths = ClusterPaths::new(data_dir);
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .map_err(|error| error.to_string())?
        .ok_or("native peer identity is unavailable")?;
    let key = DaemonDfsMutationLaneKey {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let shard =
        daemon_dfs_message_cache_exact_admission_handle(data_dir, &paths, &peer, &key, runtime)
            .map_err(|error| error.to_string())?
            .ok_or(
                "the exact current resident shard is unavailable; observation cannot open another",
            )?;
    let Some(backend) = shard.try_checkout().map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    let (durable, materialized, prefix) = backend.replica_materialization_boundary_state();
    Ok(Some(DfsTestMaterializationCapture {
        physical_root: crate::default_dfs_metadata_shards_root(data_dir).join(&shard_id.0),
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
        durable_sequence: Some(durable),
        materialized_sequence: Some(materialized),
        prefix,
        unavailable: None,
    }))
}

pub struct DfsTestMaterializationPause {
    root: PathBuf,
    gate: Arc<Gate>,
}
impl DfsTestMaterializationPause {
    pub(super) fn blocking_replay_holds_owner(&self, owner: &Arc<DaemonDataOwnerLock>) -> bool {
        self.gate
            .blocking_owner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|held| held.ptr_eq(&Arc::downgrade(owner)))
    }
    /// Cancel only the actual worker recorded at the normal scheduling return.
    /// The native host retains its original JoinHandle and blocking custody.
    pub async fn abort_worker(&self, budget: std::time::Duration) -> Result<(), String> {
        let permit = tokio::time::timeout(budget, self.gate.worker_registered.acquire())
            .await
            .map_err(|_| "actual materialization worker was not registered".to_string())?
            .map_err(|_| "actual materialization registration was closed".to_string())?;
        permit.forget();
        self.gate
            .worker
            .lock()
            .map_err(|_| "actual materialization worker lock poisoned".to_string())?
            .as_ref()
            .ok_or_else(|| "actual materialization worker is unavailable".to_string())?
            .abort();
        Ok(())
    }
    pub async fn wait(
        &self,
        budget: std::time::Duration,
    ) -> Result<DfsTestMaterializationCapture, String> {
        let permit = tokio::time::timeout(budget, self.gate.entered.acquire())
            .await
            .map_err(|_| "native materialization boundary was not reached".to_string())?
            .map_err(|_| "native materialization capture was closed".to_string())?;
        permit.forget();
        self.gate
            .captures
            .lock()
            .map_err(|_| "materialization capture lock poisoned".to_string())?
            .last()
            .cloned()
            .ok_or_else(|| "native materializer emitted no capture".to_string())
    }
    pub fn release(&self) {
        self.gate.released.close();
    }
}
impl Drop for DfsTestMaterializationPause {
    fn drop(&mut self) {
        if let Some(gates) = GATES.get() {
            let mut gates = gates.lock().unwrap_or_else(|error| error.into_inner());
            if gates
                .get(&self.root)
                .is_some_and(|gate| Arc::ptr_eq(gate, &self.gate))
            {
                gates.remove(&self.root);
            }
        }
        self.release();
    }
}

pub(crate) fn install(root: &Path) -> Result<DfsTestMaterializationPause, String> {
    let root = root.canonicalize().map_err(|error| error.to_string())?;
    let mut gates = GATES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| "materialization gate registry poisoned".to_string())?;
    if gates.contains_key(&root) {
        return Err("this physical root already owns a materialization pause".into());
    }
    let gate = Arc::new(Gate {
        captures: Mutex::new(Vec::new()),
        entered: tokio::sync::Semaphore::new(0),
        released: tokio::sync::Semaphore::new(0),
        worker: Mutex::new(None),
        worker_registered: tokio::sync::Semaphore::new(0),
        blocking_owner: Mutex::new(None),
    });
    gates.insert(root.clone(), Arc::clone(&gate));
    Ok(DfsTestMaterializationPause { root, gate })
}

/// Observe the actual blocking call's borrowed owner without adding a strong
/// reference that could itself keep the capability alive across cancellation.
pub(super) struct BlockingOwnerObservation<'a> {
    gate: Arc<Gate>,
    _owner: &'a Arc<DaemonDataOwnerLock>,
}
impl Drop for BlockingOwnerObservation<'_> {
    fn drop(&mut self) {
        *self.gate.blocking_owner.lock().unwrap() = None;
    }
}
pub(super) fn observe_blocking_owner<'a>(
    data_dir: &SinomoDataDir,
    shard_id: &crate::DfsMetadataShardId,
    owner: &'a Arc<DaemonDataOwnerLock>,
) -> Option<BlockingOwnerObservation<'a>> {
    let root = crate::default_dfs_metadata_shards_root(data_dir)
        .join(&shard_id.0)
        .canonicalize()
        .ok()?;
    let gate = GATES.get()?.lock().ok()?.get(&root).cloned()?;
    let previous = gate
        .blocking_owner
        .lock()
        .unwrap()
        .replace(Arc::downgrade(owner));
    assert!(
        previous.is_none(),
        "one exact shard has one active blocking replay"
    );
    Some(BlockingOwnerObservation {
        gate,
        _owner: owner,
    })
}

pub(super) struct PendingCapture {
    gate: Arc<Gate>,
    capture: DfsTestMaterializationCapture,
}
pub(super) fn capture(
    data_dir: &SinomoDataDir,
    shard: &crate::ResidentLocalMetadataShardHandle,
) -> Option<PendingCapture> {
    let root = crate::default_dfs_metadata_shards_root(data_dir).join(&shard.shard_id().0);
    let root = root.canonicalize().ok()?;
    let gate = GATES.get()?.lock().ok()?.get(&root).cloned()?;
    let mut capture = DfsTestMaterializationCapture {
        physical_root: root,
        file_system_id: shard.file_system_id().clone(),
        shard_id: shard.shard_id().clone(),
        durable_sequence: None,
        materialized_sequence: None,
        prefix: None,
        unavailable: None,
    };
    match shard.try_checkout() {
        Ok(Some(backend)) => {
            let (durable, materialized, prefix) = backend.replica_materialization_boundary_state();
            capture.durable_sequence = Some(durable);
            capture.materialized_sequence = Some(materialized);
            capture.prefix = prefix;
        }
        Ok(None) => capture.unavailable = Some("native resident checkout is busy".into()),
        Err(error) => capture.unavailable = Some(error.to_string()),
    }
    Some(PendingCapture { gate, capture })
}
impl PendingCapture {
    pub(super) fn task_observer(&self) -> MaterializationTaskObserver {
        MaterializationTaskObserver {
            gate: self.gate.clone(),
        }
    }
    pub(super) async fn wait(self) {
        self.gate
            .captures
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(self.capture);
        self.gate.entered.add_permits(1);
        // Park before mutation admission/resident checkout; the real task and
        // data owner stay retained, while later ingress can validate the suffix.
        let _ = self.gate.released.acquire().await;
    }
}
pub(super) struct MaterializationTaskObserver {
    gate: Arc<Gate>,
}
impl MaterializationTaskObserver {
    pub(super) fn record(&self, task: &tokio::task::JoinHandle<()>) {
        let mut worker = self
            .gate
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.is_none() {
            *worker = Some(task.abort_handle());
            self.gate.worker_registered.add_permits(1);
        }
    }
}
