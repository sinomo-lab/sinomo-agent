//! Process-local notification of authorized durable replica progress.
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
pub struct DfsMetadataReplicaObservation {
    pub(crate) generation: u64,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) source_node_id: NodeId,
    pub(crate) source_epoch: u64,
    pub(crate) prefix: DfsMetadataPrefixProof,
}

#[derive(Debug, Default)]
struct ObservationState {
    latest: Option<DfsMetadataReplicaObservation>,
    outbound_cursors: BTreeMap<NodeId, u64>,
    replacement_generation: u64,
    unprepared: BTreeMap<NodeId, PreparationWait>,
}

#[derive(Debug)]
struct PreparationWait {
    file_system_id: DfsFileSystemId,
    epoch: u64,
    cursor: u64,
    retry_at: std::time::Instant,
}

type ObservationCell = Mutex<ObservationState>;
type ObserverRegistry = Mutex<BTreeMap<PathBuf, Weak<ObservationCell>>>;
static OBSERVERS: OnceLock<ObserverRegistry> = OnceLock::new();

/// Backend handles and requesters share one hint per durable root. Dropping
/// every handle/subscriber releases the process-local observation state.
#[derive(Clone, Debug)]
pub struct DfsMetadataReplicaObserver {
    cell: Arc<ObservationCell>,
}

impl DfsMetadataReplicaObserver {
    pub(crate) fn latest(&self) -> Option<DfsMetadataReplicaObservation> {
        self.cell
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .latest
            .clone()
    }
}

fn root_identity(root: &Path) -> PathBuf {
    // Installation and subscription both own an existing metadata root. If
    // external removal races observation, a missed hint remains conservative.
    std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

pub(crate) fn subscribe(root: &Path) -> DfsMetadataReplicaObserver {
    let mut observers = OBSERVERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    observers.retain(|_, observer| observer.strong_count() > 0);
    let key = root_identity(root);
    let cell = match observers.get(&key).and_then(Weak::upgrade) {
        Some(cell) => cell,
        None => {
            let cell = Arc::new(Mutex::new(ObservationState::default()));
            observers.insert(key, Arc::downgrade(&cell));
            cell
        }
    };
    DfsMetadataReplicaObserver { cell }
}

impl DfsMetadataReplicaObserver {
    /// Publish only after authorized durable ingest succeeds, while the shard
    /// writer lock still orders this hint with the corresponding exact prefix.
    pub(crate) fn publish(
        &self,
        permit: &DfsMetadataReplicaIngestPermit,
        prefix: &DfsMetadataPrefixProof,
        replaced: bool,
    ) {
        let mut state = self.cell.lock().unwrap_or_else(|error| error.into_inner());
        if replaced {
            state.outbound_cursors.clear();
            state.unprepared.clear();
            state.replacement_generation += 1;
        }
        let latest = &mut state.latest;
        if !replaced
            && latest.as_ref().is_some_and(|previous| {
                previous.file_system_id == *permit.file_system_id()
                    && previous.source_node_id == *permit.source_node_id()
                    && previous.source_epoch == permit.source_epoch()
                    && (previous.prefix == *prefix
                        || previous.prefix.journal_sequence > prefix.journal_sequence)
            })
        {
            return;
        }
        let generation = latest
            .as_ref()
            .map_or(1, |previous| previous.generation.saturating_add(1));
        *latest = Some(DfsMetadataReplicaObservation {
            generation,
            file_system_id: permit.file_system_id().clone(),
            shard_id: permit.shard_id().clone(),
            source_node_id: permit.source_node_id().clone(),
            source_epoch: permit.source_epoch(),
            prefix: prefix.clone(),
        });
    }
}

impl DfsMetadataReplicaObserver {
    /// Fresh verified ACKs are delivery hints only. They do not replace the
    /// operation nonce, receiver floor or a fresh caught-up prefix probe.
    pub(crate) fn observe_outbound(&self, cursors: &[(NodeId, u64)]) {
        let mut state = self.cell.lock().unwrap_or_else(|error| error.into_inner());
        for (node, sequence) in cursors {
            if state
                .unprepared
                .get(node)
                .is_some_and(|wait| *sequence > wait.cursor)
            {
                state.unprepared.remove(node);
            }
            let cursor = state.outbound_cursors.entry(node.clone()).or_default();
            *cursor = (*cursor).max(*sequence);
        }
    }

    pub(crate) fn preparation_watch(self) -> ReplicaPreparationWatch {
        let generation = self
            .cell
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replacement_generation;
        ReplicaPreparationWatch {
            observer: self,
            generation,
        }
    }

    pub(crate) fn outbound_cursor(&self, node: &NodeId) -> Option<u64> {
        self.cell
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .outbound_cursors
            .get(node)
            .copied()
    }
}

/// Recheck an already prepared background batch against foreground ACKs which
/// arrived after preparation. No backend is opened and no durable cursor moves.
pub(crate) fn outbound_cursor(root: &Path, node: &NodeId) -> Option<u64> {
    let cell = OBSERVERS
        .get()?
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&root_identity(root))
        .and_then(Weak::upgrade)?;
    let state = cell.lock().unwrap_or_else(|error| error.into_inner());
    state.outbound_cursors.get(node).copied()
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicaPreparationWatch {
    observer: DfsMetadataReplicaObserver,
    generation: u64,
}

impl ReplicaPreparationWatch {
    /// A scheduling hint scoped to this physical root. A confirmed advance,
    /// new assignment epoch, snapshot replacement or the retry deadline reopens it.
    pub(crate) fn preparation_pending(
        &self,
        node: &NodeId,
        fs: &DfsFileSystemId,
        epoch: u64,
        cursor: u64,
    ) -> bool {
        let state = self
            .observer
            .cell
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.replacement_generation == self.generation
            && state.unprepared.get(node).is_some_and(|wait| {
                wait.file_system_id == *fs
                    && wait.epoch == epoch
                    && wait.cursor == cursor
                    && wait.retry_at > std::time::Instant::now()
            })
    }

    pub(crate) fn wait_for_preparation(
        &self,
        node: &NodeId,
        fs: &DfsFileSystemId,
        epoch: u64,
        cursor: u64,
        retry_at: std::time::Instant,
    ) {
        let mut state = self
            .observer
            .cell
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = std::time::Instant::now();
        state.unprepared.retain(|_, wait| wait.retry_at > now);
        if state.replacement_generation == self.generation
            && state
                .outbound_cursors
                .get(node)
                .is_none_or(|confirmed| *confirmed <= cursor)
        {
            state.unprepared.insert(
                node.clone(),
                PreparationWait {
                    file_system_id: fs.clone(),
                    epoch,
                    cursor,
                    retry_at,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn preparation_hint_expires_without_new_input_and_cannot_park_after_progress() {
        let root = sinomo_testdir::TestDir::new("dfs-replica-preparation-wait");
        let observer = subscribe(root.path());
        let watch = observer.clone().preparation_watch();
        let target = NodeId::from_legacy_named("dfs-unprepared-target").unwrap();
        let fs = crate::sifs_file_system_id();
        let now = Instant::now();
        watch.wait_for_preparation(&target, &fs, 3, 4, now);
        assert!(!watch.preparation_pending(&target, &fs, 3, 4));
        let future = now + Duration::from_secs(30);
        watch.wait_for_preparation(&target, &fs, 3, 4, future);
        assert!(watch.preparation_pending(&target, &fs, 3, 4));
        assert!(!watch.preparation_pending(&target, &fs, 4, 4));
        observer.observe_outbound(&[(target.clone(), 5)]);
        watch.wait_for_preparation(&target, &fs, 3, 4, future);
        assert!(!watch.preparation_pending(&target, &fs, 3, 4));
    }
}
