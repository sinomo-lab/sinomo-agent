//! Negative-only control over an actual received deferred response.
use super::*;
use std::sync::Condvar;

#[derive(Clone, Debug)]
pub struct DfsTestDeferredResponseCapture {
    pub physical_root: PathBuf,
    pub command: DfsMessageCommand,
    pub original: Option<crate::DfsRoutedMessage>,
    pub owner_file_identity: (u64, u64),
    owner: Weak<DfsDataOwnerLock>,
}
impl DfsTestDeferredResponseCapture {
    pub fn owner_still_held(&self) -> bool {
        self.owner.strong_count() != 0
    }
}
#[derive(Default)]
struct State {
    capture: Option<DfsTestDeferredResponseCapture>,
    released: bool,
}
#[derive(Default)]
struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
static GATES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Gate>>>> = OnceLock::new();

pub struct DfsTestDeferredResponsePause {
    root: PathBuf,
    gate: Arc<Gate>,
}
impl DfsTestDeferredResponsePause {
    pub fn wait(&self, budget: Duration) -> Result<DfsTestDeferredResponseCapture, String> {
        let state = self.gate.state.lock().unwrap();
        let (state, _) = self
            .gate
            .changed
            .wait_timeout_while(state, budget, |state| state.capture.is_none())
            .unwrap();
        state
            .capture
            .clone()
            .ok_or_else(|| "actual deferred response did not reach its native owner".into())
    }
    pub fn release(&self) {
        self.gate.state.lock().unwrap().released = true;
        self.gate.changed.notify_all();
    }
}
impl Drop for DfsTestDeferredResponsePause {
    fn drop(&mut self) {
        let mut gates = GATES.get().unwrap().lock().unwrap();
        if gates
            .get(&self.root)
            .is_some_and(|gate| Arc::ptr_eq(gate, &self.gate))
        {
            gates.remove(&self.root);
        }
        drop(gates);
        self.release();
    }
}
pub(crate) fn install(data_dir: &SinomoDataDir) -> Result<DfsTestDeferredResponsePause, String> {
    let owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(data_dir))
        .map_err(|error| error.to_string())?;
    let root = owner.physical_data_root.clone();
    let mut gates = GATES.get_or_init(Default::default).lock().unwrap();
    if gates.contains_key(&root) {
        return Err("physical native owner already has a deferred-response pause".into());
    }
    let gate = Arc::new(Gate::default());
    gates.insert(root.clone(), gate.clone());
    Ok(DfsTestDeferredResponsePause { root, gate })
}

pub(super) fn before_response(
    owner: &Arc<DfsDataOwnerLock>,
    command: &DfsMessageCommand,
    original: Option<&crate::DfsRoutedMessage>,
) -> Result<(), DfsRuntimeError> {
    if !matches!(
        command.routed_message.message,
        crate::DfsNetworkMessage::RepairReplicaResponse(_)
    ) || !matches!(
        original.map(|message| &message.message),
        Some(crate::DfsNetworkMessage::RepairReplica(_))
    ) {
        return Ok(());
    }
    let gate = GATES
        .get()
        .and_then(|gates| gates.lock().ok()?.get(&owner.physical_data_root).cloned());
    let Some(gate) = gate else {
        return Ok(());
    };
    let mut state = gate.state.lock().unwrap();
    if state.capture.is_some() {
        return Ok(());
    }
    state.capture = Some(DfsTestDeferredResponseCapture {
        physical_root: owner.physical_data_root.clone(),
        command: command.clone(),
        original: original.cloned(),
        owner_file_identity: owner.storage_owner_file_identity().map_err(|error| {
            DfsRuntimeError::Parse {
                detail: error.to_string(),
            }
        })?,
        owner: Arc::downgrade(owner),
    });
    gate.changed.notify_all();
    let (state, _) = gate
        .changed
        .wait_timeout_while(
            state,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            |state| !state.released,
        )
        .unwrap();
    if !state.released {
        return Err(DfsRuntimeError::Parse {
            detail: "native deferred-response control exhausted its existing network deadline"
                .into(),
        });
    }
    Ok(())
}
