use crate::NetworkError;
use sinomo_local::SinomoDataDir;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterPaths {
    pub root: PathBuf,
    pub state: PathBuf,
    pub iroh: PathBuf,
    pub connectivity_local: PathBuf,
    pub kv_snapshot: PathBuf,
    pub kv_raft: PathBuf,
    pub kv_watches: PathBuf,
    pub kv_watches_lock: PathBuf,
    pub kv_watches_next_id: PathBuf,
    pub kv_watch_events: PathBuf,
    pub kv_watch_events_log: PathBuf,
}

impl ClusterPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("cluster");
        let kv = sinomo_kv::ClusterKvPaths::new(&root);
        Self {
            state: root.join("state.sobj"),
            iroh: root.join("iroh"),
            connectivity_local: root.join("connectivity-local.sobj"),
            kv_snapshot: kv.snapshot,
            kv_raft: kv.raft,
            kv_watches: kv.watches,
            kv_watches_lock: kv.watches_lock,
            kv_watches_next_id: kv.watches_next_id,
            kv_watch_events: kv.watch_events,
            kv_watch_events_log: kv.watch_events_log,
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), NetworkError> {
        fs::create_dir_all(&self.root).map_err(|source| NetworkError::StorageIo {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    pub fn kv_paths(&self) -> sinomo_kv::ClusterKvPaths {
        sinomo_kv::ClusterKvPaths {
            root: self.root.clone(),
            snapshot: self.kv_snapshot.clone(),
            raft: self.kv_raft.clone(),
            watches: self.kv_watches.clone(),
            watches_lock: self.kv_watches_lock.clone(),
            watches_next_id: self.kv_watches_next_id.clone(),
            watch_events: self.kv_watch_events.clone(),
            watch_events_log: self.kv_watch_events_log.clone(),
        }
    }
}
