#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
