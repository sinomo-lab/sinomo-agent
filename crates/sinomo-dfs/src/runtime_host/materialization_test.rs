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
}
static GATES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Gate>>>> = OnceLock::new();

pub struct DfsTestMaterializationPause {
    root: PathBuf,
    gate: Arc<Gate>,
}
impl DfsTestMaterializationPause {
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
    });
    gates.insert(root.clone(), Arc::clone(&gate));
    Ok(DfsTestMaterializationPause { root, gate })
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
