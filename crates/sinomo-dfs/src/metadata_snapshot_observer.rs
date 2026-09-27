//! Process-local notification of successful authorized snapshot replacement.
//!
//! A notification only requests a fresh durable read. It never authorizes a
//! mutation or substitutes for the exact prefix proof in the installed store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::{
    DfsFileSystemId, DfsMetadataPrefixProof, DfsMetadataReplicaIngestPermit, DfsMetadataShardId,
    NodeId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataSnapshotInstallObservation {
    pub(crate) generation: u64,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) source_node_id: NodeId,
    pub(crate) source_epoch: u64,
    pub(crate) prefix: DfsMetadataPrefixProof,
}

type ObservationCell = Mutex<Option<DfsMetadataSnapshotInstallObservation>>;
type ObserverRegistry = Mutex<BTreeMap<PathBuf, Weak<ObservationCell>>>;
static OBSERVERS: OnceLock<ObserverRegistry> = OnceLock::new();

/// A subscriber owns this cell only while its routed mirror needs repair.
/// Dropping every subscriber releases the process-local observation state.
#[derive(Clone, Debug)]
pub struct DfsMetadataSnapshotInstallObserver {
    cell: Arc<ObservationCell>,
}

impl DfsMetadataSnapshotInstallObserver {
    pub(crate) fn latest(&self) -> Option<DfsMetadataSnapshotInstallObservation> {
        self.cell
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

fn root_identity(root: &Path) -> PathBuf {
    // Installation and subscription both own an existing metadata root. If
    // external removal races observation, a missed hint remains conservative.
    std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

pub(crate) fn subscribe(root: &Path) -> DfsMetadataSnapshotInstallObserver {
    let mut observers = OBSERVERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    observers.retain(|_, observer| observer.strong_count() > 0);
    let key = root_identity(root);
    let cell = match observers.get(&key).and_then(Weak::upgrade) {
        Some(cell) => cell,
        None => {
            let cell = Arc::new(Mutex::new(None));
            observers.insert(key, Arc::downgrade(&cell));
            cell
        }
    };
    DfsMetadataSnapshotInstallObserver { cell }
}

pub(crate) fn publish_installed(
    root: &Path,
    permit: &DfsMetadataReplicaIngestPermit,
    prefix: &DfsMetadataPrefixProof,
) {
    let Some(observers) = OBSERVERS.get() else {
        return;
    };
    let cell = observers
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&root_identity(root))
        .and_then(Weak::upgrade);
    let Some(cell) = cell else {
        return;
    };
    let mut latest = cell.lock().unwrap_or_else(|error| error.into_inner());
    let generation = latest
        .as_ref()
        .map_or(1, |latest| latest.generation.saturating_add(1));
    *latest = Some(DfsMetadataSnapshotInstallObservation {
        generation,
        file_system_id: permit.file_system_id().clone(),
        shard_id: permit.shard_id().clone(),
        source_node_id: permit.source_node_id().clone(),
        source_epoch: permit.source_epoch(),
        prefix: prefix.clone(),
    });
}
