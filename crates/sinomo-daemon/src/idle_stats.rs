use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct IdleStatsAtomics {
    pub ticks_total: AtomicU64,
    pub ticks_with_work: AtomicU64,
    pub disk_reads_cluster_state: AtomicU64,
    pub disk_reads_iroh_endpoints: AtomicU64,
    pub disk_reads_dfs_metadata: AtomicU64,
    pub disk_writes_object_file_actual: AtomicU64,
    pub disk_writes_object_file_skipped: AtomicU64,
    pub kv_snapshot_writes_actual: AtomicU64,
    pub kv_snapshot_writes_skipped: AtomicU64,
    pub kv_publish_node_status: AtomicU64,
    pub kv_publish_network_info: AtomicU64,
    pub kv_publish_member: AtomicU64,
    pub connectivity_record_sets_written: AtomicU64,
    pub mdns_announces_sent: AtomicU64,
    pub mdns_announces_recv: AtomicU64,
    pub raft_appends_sent: AtomicU64,
    pub raft_appends_recv: AtomicU64,
    pub connectivity_probes_sent: AtomicU64,
    pub dfs_metadata_replications_sent: AtomicU64,
    pub disk_bytes_read: AtomicU64,
    pub disk_bytes_written: AtomicU64,
    pub net_bytes_sent: AtomicU64,
    pub net_bytes_recv: AtomicU64,
}

impl IdleStatsAtomics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> IdleStatsSnapshot {
        IdleStatsSnapshot {
            ticks_total: self.ticks_total.load(Ordering::Relaxed),
            ticks_with_work: self.ticks_with_work.load(Ordering::Relaxed),
            disk_reads_cluster_state: self.disk_reads_cluster_state.load(Ordering::Relaxed),
            disk_reads_iroh_endpoints: self.disk_reads_iroh_endpoints.load(Ordering::Relaxed),
            disk_reads_dfs_metadata: self.disk_reads_dfs_metadata.load(Ordering::Relaxed),
            disk_writes_object_file_actual: self
                .disk_writes_object_file_actual
                .load(Ordering::Relaxed),
            disk_writes_object_file_skipped: self
                .disk_writes_object_file_skipped
                .load(Ordering::Relaxed),
            kv_snapshot_writes_actual: self.kv_snapshot_writes_actual.load(Ordering::Relaxed),
            kv_snapshot_writes_skipped: self.kv_snapshot_writes_skipped.load(Ordering::Relaxed),
            kv_publish_node_status: self.kv_publish_node_status.load(Ordering::Relaxed),
            kv_publish_network_info: self.kv_publish_network_info.load(Ordering::Relaxed),
            kv_publish_member: self.kv_publish_member.load(Ordering::Relaxed),
            connectivity_record_sets_written: self
                .connectivity_record_sets_written
                .load(Ordering::Relaxed),
            mdns_announces_sent: self.mdns_announces_sent.load(Ordering::Relaxed),
            mdns_announces_recv: self.mdns_announces_recv.load(Ordering::Relaxed),
            raft_appends_sent: self.raft_appends_sent.load(Ordering::Relaxed),
            raft_appends_recv: self.raft_appends_recv.load(Ordering::Relaxed),
            connectivity_probes_sent: self.connectivity_probes_sent.load(Ordering::Relaxed),
            dfs_metadata_replications_sent: self
                .dfs_metadata_replications_sent
                .load(Ordering::Relaxed),
            disk_bytes_read: self.disk_bytes_read.load(Ordering::Relaxed),
            disk_bytes_written: self.disk_bytes_written.load(Ordering::Relaxed),
            net_bytes_sent: self.net_bytes_sent.load(Ordering::Relaxed),
            net_bytes_recv: self.net_bytes_recv.load(Ordering::Relaxed),
        }
    }

    pub fn incr_ticks_total(&self) {
        self.ticks_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_ticks_with_work(&self) {
        self.ticks_with_work.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_disk_reads_cluster_state(&self) {
        self.disk_reads_cluster_state
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_disk_reads_iroh_endpoints(&self) {
        self.disk_reads_iroh_endpoints
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_disk_reads_dfs_metadata(&self) {
        self.disk_reads_dfs_metadata.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_kv_publish_node_status(&self) {
        self.kv_publish_node_status.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_kv_publish_network_info(&self) {
        self.kv_publish_network_info.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_kv_publish_member(&self) {
        self.kv_publish_member.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_connectivity_record_sets_written(&self) {
        self.connectivity_record_sets_written
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_mdns_announces_sent(&self) {
        self.mdns_announces_sent.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_mdns_announces_recv(&self, count: u64) {
        if count > 0 {
            self.mdns_announces_recv.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn incr_raft_appends_sent(&self) {
        self.raft_appends_sent.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_raft_appends_recv(&self) {
        self.raft_appends_recv.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_connectivity_probes_sent(&self, count: u64) {
        if count > 0 {
            self.connectivity_probes_sent
                .fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn incr_dfs_metadata_replications_sent(&self) {
        self.dfs_metadata_replications_sent
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdleStatsSnapshot {
    pub ticks_total: u64,
    pub ticks_with_work: u64,
    pub disk_reads_cluster_state: u64,
    pub disk_reads_iroh_endpoints: u64,
    pub disk_reads_dfs_metadata: u64,
    pub disk_writes_object_file_actual: u64,
    pub disk_writes_object_file_skipped: u64,
    pub kv_snapshot_writes_actual: u64,
    pub kv_snapshot_writes_skipped: u64,
    pub kv_publish_node_status: u64,
    pub kv_publish_network_info: u64,
    pub kv_publish_member: u64,
    #[serde(default)]
    pub connectivity_record_sets_written: u64,
    pub mdns_announces_sent: u64,
    pub mdns_announces_recv: u64,
    pub raft_appends_sent: u64,
    pub raft_appends_recv: u64,
    pub connectivity_probes_sent: u64,
    pub dfs_metadata_replications_sent: u64,
    #[serde(default)]
    pub disk_bytes_read: u64,
    #[serde(default)]
    pub disk_bytes_written: u64,
    #[serde(default)]
    pub net_bytes_sent: u64,
    #[serde(default)]
    pub net_bytes_recv: u64,
}

static PROCESS_IDLE_STATS: OnceLock<Arc<IdleStatsAtomics>> = OnceLock::new();

pub fn install_process_idle_stats(stats: Arc<IdleStatsAtomics>) {
    let _ = PROCESS_IDLE_STATS.set(stats);
}

pub fn process_idle_stats() -> Option<Arc<IdleStatsAtomics>> {
    PROCESS_IDLE_STATS.get().cloned()
}

struct DaemonIoObserver {
    stats: Arc<IdleStatsAtomics>,
}

impl sinomo_core::network::IoObserver for DaemonIoObserver {
    fn on_disk_read(&self, bytes: u64) {
        self.stats
            .disk_bytes_read
            .fetch_add(bytes, Ordering::Relaxed);
    }
    fn on_disk_write(&self, bytes: u64) {
        self.stats
            .disk_bytes_written
            .fetch_add(bytes, Ordering::Relaxed);
    }
    fn on_object_file_actual_write(&self) {
        self.stats
            .disk_writes_object_file_actual
            .fetch_add(1, Ordering::Relaxed);
    }
    fn on_object_file_skipped_write(&self) {
        self.stats
            .disk_writes_object_file_skipped
            .fetch_add(1, Ordering::Relaxed);
    }
    fn on_kv_snapshot_actual_write(&self) {
        self.stats
            .kv_snapshot_writes_actual
            .fetch_add(1, Ordering::Relaxed);
    }
    fn on_kv_snapshot_skipped_write(&self) {
        self.stats
            .kv_snapshot_writes_skipped
            .fetch_add(1, Ordering::Relaxed);
    }
    fn on_net_sent(&self, bytes: u64) {
        self.stats
            .net_bytes_sent
            .fetch_add(bytes, Ordering::Relaxed);
    }
    fn on_net_recv(&self, bytes: u64) {
        self.stats
            .net_bytes_recv
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

pub fn register_io_observer(stats: Arc<IdleStatsAtomics>) {
    let observer = Arc::new(DaemonIoObserver { stats });
    let _ = sinomo_core::network::set_io_observer(Some(observer));
}
