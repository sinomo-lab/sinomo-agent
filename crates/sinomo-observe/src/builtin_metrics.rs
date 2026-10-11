//! Built-in metric catalog and exporters for cluster, DFS, and node resources.

use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ClusterPaths;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use sysinfo::{Disks, Networks, ProcessRefreshKind, ProcessesToUpdate, System};

const DURABLE_INVENTORY_METRIC_CACHE_TTL: Duration = Duration::from_secs(2 * 60);

/// Register Sinomo's built-in metric descriptors and snapshot exporters.
///
/// Application-owned exporters remain an independent composition hook; this
/// function installs only the owner-crate catalog and resource samplers.
pub fn register_builtin_observe_metric_exporters() {
    register_observe_metric_descriptors();
    initialize_node_resource_sampling();
    // DFS owns its bounded component exporter. A second inventory exporter
    // here bypasses that ownership and cold-opens every metadata shard even
    // for requests for the process-local DFS background counters.
    crate::register_observe_metric_exporter_with_policy(
        "sinomo-kv",
        ["sinomo.kv."],
        Some(DURABLE_INVENTORY_METRIC_CACHE_TTL),
        kv_observe_metrics,
    );
    crate::register_observe_metric_exporter_with_policy(
        "sinomo-node-resources",
        ["sinomo.node."],
        Some(Duration::from_secs(5)),
        node_resource_observe_metrics,
    );
}

fn register_observe_metric_descriptors() {
    use crate::{
        ObserveMetricDescriptor, ObserveMetricKind, ObserveMetricSemantics, ObserveMetricUnit,
    };

    let register = |name: &str, kind: ObserveMetricKind, unit: ObserveMetricUnit, help: &str| {
        let descriptor = ObserveMetricDescriptor::new(name, kind, Some(unit), help);
        let descriptor = match observe_metric_domain(name) {
            Some(domain) => descriptor.with_semantics(ObserveMetricSemantics::new(
                domain,
                observe_metric_aggregation(name, kind),
                observe_metric_labels(name).iter().copied(),
            )),
            None => descriptor,
        };
        crate::register_observe_metric_descriptor(descriptor);
    };

    for (name, help) in [
        (
            "sinomo.daemon.service.requests",
            "Daemon object RPC requests.",
        ),
        ("sinomo.kv.reads", "Cluster KV reads."),
        ("sinomo.kv.commands", "Cluster KV commands."),
        (
            "sinomo.kv.storage.operations",
            "Cluster KV storage operations.",
        ),
        ("sinomo.kv.compaction.runs", "Cluster KV compaction runs."),
        (
            "sinomo.kv.replication.target_results",
            "KV replication target results.",
        ),
        ("sinomo.kv.replication.attempts", "KV replication attempts."),
        (
            "sinomo.kv.replication.backtracks",
            "KV replication backtracks.",
        ),
        (
            "sinomo.kv.fanout.coalesced",
            "KV background fanout requests coalesced.",
        ),
        ("sinomo.kv.fanout.passes", "KV background fanout passes."),
        (
            "sinomo.kv.fanout.watch_event_batches",
            "KV watch event batches fanned out.",
        ),
        (
            "sinomo.kv.fanout.target_results",
            "KV background fanout target results.",
        ),
        (
            "sinomo.network.rpc.requests",
            "Canonical object RPC calls made by this node.",
        ),
        (
            "sinomo.network.rpc.attempts",
            "Physical channel or session attempts made for routed and relayed object RPCs.",
        ),
        (
            "sinomo.network.endpoint.opens",
            "Concrete network endpoint open attempts.",
        ),
        (
            "sinomo.network.route.plans",
            "Cluster delivery route planning attempts.",
        ),
        (
            "sinomo.dfs.operations",
            "DFS control-plane and data-plane operations handled by this node.",
        ),
    ] {
        register(
            name,
            ObserveMetricKind::Counter,
            ObserveMetricUnit::Count,
            help,
        );
    }
    for (name, help) in [
        (
            "sinomo.kv.wal.append.bytes",
            "Bytes appended to the cluster KV WAL.",
        ),
        (
            "sinomo.kv.wal.rewrite.bytes",
            "Bytes written while rewriting the cluster KV WAL.",
        ),
        (
            "sinomo.kv.snapshot.write.bytes",
            "Bytes written to cluster KV snapshots.",
        ),
        (
            "sinomo.node.network.received.bytes",
            "Bytes received by this node's network interfaces since observation started.",
        ),
        (
            "sinomo.node.network.transmitted.bytes",
            "Bytes transmitted by this node's network interfaces since observation started.",
        ),
        (
            "sinomo.network.rpc.request_head.bytes",
            "Object RPC request-head bytes sent by this node.",
        ),
        (
            "sinomo.network.rpc.response_head.bytes",
            "Object RPC response-head bytes received by this node.",
        ),
    ] {
        register(
            name,
            ObserveMetricKind::Counter,
            ObserveMetricUnit::Bytes,
            help,
        );
    }
    for (name, help) in [
        (
            "sinomo.daemon.service.context.us",
            "Time spent preparing daemon service request context.",
        ),
        (
            "sinomo.daemon.service.dispatch.us",
            "Time spent dispatching daemon service requests.",
        ),
        (
            "sinomo.daemon.service.total.us",
            "End-to-end daemon service request latency.",
        ),
        (
            "sinomo.kv.command.phase.us",
            "Cluster KV command phase latency.",
        ),
        (
            "sinomo.kv.read.total.us",
            "End-to-end cluster KV read latency.",
        ),
        (
            "sinomo.kv.command.total.us",
            "End-to-end cluster KV command latency.",
        ),
        ("sinomo.kv.wal.read.us", "Cluster KV WAL read latency."),
        ("sinomo.kv.wal.append.us", "Cluster KV WAL append latency."),
        (
            "sinomo.kv.wal.rewrite.us",
            "Cluster KV WAL rewrite latency.",
        ),
        ("sinomo.kv.compaction.us", "Cluster KV compaction latency."),
        (
            "sinomo.kv.raft.open.us",
            "Cluster KV Raft store open latency.",
        ),
        ("sinomo.kv.apply.us", "Cluster KV command apply latency."),
        (
            "sinomo.kv.snapshot.write.us",
            "Cluster KV snapshot write latency.",
        ),
        (
            "sinomo.kv.snapshot.restore.us",
            "Cluster KV snapshot restore latency.",
        ),
        (
            "sinomo.kv.replication.target.us",
            "KV replication target latency.",
        ),
        (
            "sinomo.kv.fanout.pass.us",
            "KV background fanout pass latency.",
        ),
        (
            "sinomo.kv.fanout.target.us",
            "KV background fanout target latency.",
        ),
        (
            "sinomo.network.rpc.total.us",
            "End-to-end stream-native object RPC latency, including route selection and retries.",
        ),
        (
            "sinomo.network.endpoint.open.us",
            "Concrete network endpoint open latency.",
        ),
        (
            "sinomo.network.route.plan.us",
            "Cluster delivery route planning latency.",
        ),
        (
            "sinomo.dfs.operation.total.us",
            "End-to-end DFS operation handling latency.",
        ),
    ] {
        register(
            name,
            ObserveMetricKind::Histogram,
            ObserveMetricUnit::Microseconds,
            help,
        );
    }
    for (name, help) in [
        (
            "sinomo.kv.wal.read.entries",
            "Entries returned by cluster KV WAL reads.",
        ),
        (
            "sinomo.kv.wal.append.entries",
            "Entries appended to the cluster KV WAL.",
        ),
        (
            "sinomo.kv.wal.rewrite.entries",
            "Entries written during cluster KV WAL rewrites.",
        ),
        (
            "sinomo.kv.quorum.acks",
            "Acknowledgements observed for cluster KV quorum commits.",
        ),
        (
            "sinomo.kv.compaction.removed_log_entries",
            "Raft log entries removed by KV compaction.",
        ),
        (
            "sinomo.kv.raft.open.log_entries",
            "Raft log entries loaded while opening cluster KV.",
        ),
        (
            "sinomo.kv.replication.entries",
            "Entries carried by a KV replication attempt.",
        ),
        (
            "sinomo.kv.replication.catchup_attempts",
            "Attempts needed for KV follower catch-up.",
        ),
        (
            "sinomo.kv.fanout.attempted_nodes",
            "Nodes attempted by a KV background fanout pass.",
        ),
        (
            "sinomo.kv.fanout.replicated_nodes",
            "Nodes updated by a KV background fanout pass.",
        ),
    ] {
        register(
            name,
            ObserveMetricKind::Histogram,
            ObserveMetricUnit::Count,
            help,
        );
    }
    for (name, help) in [
        (
            "sinomo.kv.commit.queue_depth",
            "KV commits waiting for the ordered proposal stream.",
        ),
        (
            "sinomo.kv.commit.inflight",
            "KV commits currently in the ordered proposal stream.",
        ),
        (
            "sinomo.kv.fanout.pending",
            "Whether a KV background fanout pass is pending.",
        ),
        (
            "sinomo.kv.fanout.active",
            "Whether a KV background fanout pass is active.",
        ),
        (
            "sinomo.observe.metric_exporter.errors",
            "Snapshot metric exporters currently failing.",
        ),
        (
            "sinomo.node.cpu.logical",
            "Logical processors available to this node.",
        ),
        (
            "sinomo.node.load.average",
            "Operating-system load average for a bounded sampling period.",
        ),
    ] {
        register(
            name,
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Count,
            help,
        );
    }
    for (name, help) in [
        ("sinomo.kv.wal.bytes", "Current cluster KV WAL size."),
        (
            "sinomo.kv.raft_snapshot.bytes",
            "Current cluster KV Raft snapshot size.",
        ),
        (
            "sinomo.kv.materialized_snapshot.bytes",
            "Current materialized cluster KV snapshot size.",
        ),
        ("sinomo.node.memory.total.bytes", "Total node memory."),
        ("sinomo.node.memory.used.bytes", "Used node memory."),
        (
            "sinomo.node.memory.available.bytes",
            "Available node memory.",
        ),
        ("sinomo.node.swap.used.bytes", "Used node swap space."),
        (
            "sinomo.node.data_disk.total.bytes",
            "Total size of the filesystem containing the Sinomo data directory.",
        ),
        (
            "sinomo.node.data_disk.available.bytes",
            "Available space on the filesystem containing the Sinomo data directory.",
        ),
        (
            "sinomo.node.process.resident_memory.bytes",
            "Resident memory used by the Sinomo daemon process.",
        ),
    ] {
        register(
            name,
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Bytes,
            help,
        );
    }
    register(
        "sinomo.kv.peer.progress_age.seconds",
        ObserveMetricKind::Gauge,
        ObserveMetricUnit::Seconds,
        "Age of the latest known KV peer replication progress.",
    );
    register(
        "sinomo.kv.raft.is_leader",
        ObserveMetricKind::Gauge,
        ObserveMetricUnit::Ratio,
        "Whether this node is the current cluster KV leader.",
    );
    register(
        "sinomo.node.cpu.utilization",
        ObserveMetricKind::Gauge,
        ObserveMetricUnit::Ratio,
        "Fraction of aggregate node CPU currently in use.",
    );
    register(
        "sinomo.node.uptime.seconds",
        ObserveMetricKind::Gauge,
        ObserveMetricUnit::Seconds,
        "Operating-system uptime.",
    );

    for name in [
        "sinomo.dfs.filesystems",
        "sinomo.dfs.objects",
        "sinomo.dfs.versions",
        "sinomo.dfs.receipts",
        "sinomo.dfs.intents",
        "sinomo.dfs.pending_writes",
        "sinomo.dfs.pending_repairs",
        "sinomo.dfs.pending_namespace_jobs",
        "sinomo.dfs.failed_namespace_jobs",
        "sinomo.dfs.metadata_shard.journal_next_sequence",
        "sinomo.dfs.metadata_shard.journal_last_sequence",
        "sinomo.dfs.metadata_shard.replicas",
        "sinomo.dfs.metadata_shard.replica_lag_entries",
        "sinomo.dfs.metadata_shard.replica_replicated_through_sequence",
        "sinomo.dfs.metadata_shard.replay_replayed_through_sequence",
        "sinomo.kv.revision",
        "sinomo.kv.compacted_revision",
        "sinomo.kv.entries",
        "sinomo.kv.leases",
        "sinomo.kv.retained_events",
        "sinomo.kv.raft.term",
        "sinomo.kv.raft.commit_index",
        "sinomo.kv.raft.applied_index",
        "sinomo.kv.raft.last_log_index",
        "sinomo.kv.raft.snapshot_index",
        "sinomo.kv.raft.uncommitted_entries",
        "sinomo.kv.raft.unsnapshotted_entries",
        "sinomo.kv.raft.retained_log_entries",
        "sinomo.kv.peer.progress_known",
        "sinomo.kv.peer.lag_entries",
        "sinomo.kv.peer.match_index",
    ] {
        register(
            name,
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Count,
            "Current daemon snapshot value.",
        );
    }
}

fn observe_metric_domain(name: &str) -> Option<&'static str> {
    if name.starts_with("sinomo.daemon.") {
        Some("daemon")
    } else if name.starts_with("sinomo.kv.") {
        Some("kv")
    } else if name.starts_with("sinomo.dfs.") {
        Some("dfs")
    } else if name.starts_with("sinomo.observe.") {
        Some("observe")
    } else if name.starts_with("sinomo.node.") {
        Some("node")
    } else if name.starts_with("sinomo.network.") {
        Some("network")
    } else {
        None
    }
}

fn observe_metric_aggregation(
    name: &str,
    kind: crate::ObserveMetricKind,
) -> crate::ObserveMetricAggregation {
    use crate::{ObserveMetricAggregation, ObserveMetricKind};

    match kind {
        ObserveMetricKind::Counter => ObserveMetricAggregation::Sum,
        ObserveMetricKind::Histogram => ObserveMetricAggregation::MergeHistogram,
        ObserveMetricKind::Gauge => match name {
            "sinomo.kv.commit.queue_depth"
            | "sinomo.kv.commit.inflight"
            | "sinomo.kv.fanout.pending"
            | "sinomo.kv.fanout.active"
            | "sinomo.observe.metric_exporter.errors"
            | "sinomo.kv.wal.bytes"
            | "sinomo.kv.raft_snapshot.bytes"
            | "sinomo.kv.materialized_snapshot.bytes"
            | "sinomo.kv.raft.is_leader" => ObserveMetricAggregation::Sum,
            "sinomo.node.cpu.utilization" | "sinomo.node.load.average" => {
                ObserveMetricAggregation::Max
            }
            "sinomo.node.uptime.seconds" => ObserveMetricAggregation::Min,
            name if name.starts_with("sinomo.node.") => ObserveMetricAggregation::Sum,
            "sinomo.kv.peer.progress_known" => ObserveMetricAggregation::Min,
            "sinomo.kv.peer.lag_entries" | "sinomo.kv.peer.progress_age.seconds" => {
                ObserveMetricAggregation::Max
            }
            _ => ObserveMetricAggregation::None,
        },
    }
}

fn observe_metric_labels(name: &str) -> &'static [&'static str] {
    match name {
        "sinomo.daemon.service.requests"
        | "sinomo.daemon.service.dispatch.us"
        | "sinomo.daemon.service.total.us" => &["route", "category", "outcome"],
        "sinomo.daemon.service.context.us" => &["route", "category"],
        "sinomo.network.rpc.requests" | "sinomo.network.rpc.total.us" => &["path", "outcome"],
        "sinomo.network.rpc.attempts" => &["path"],
        "sinomo.network.endpoint.opens" | "sinomo.network.endpoint.open.us" => {
            &["path", "transport", "outcome"]
        }
        "sinomo.network.route.plans" | "sinomo.network.route.plan.us" => &["cache", "outcome"],
        "sinomo.dfs.operations" | "sinomo.dfs.operation.total.us" => {
            &["operation", "plane", "outcome"]
        }
        "sinomo.network.rpc.request_head.bytes" | "sinomo.network.rpc.response_head.bytes" => {
            &["path"]
        }
        "sinomo.kv.reads" | "sinomo.kv.read.total.us" => &["authority", "source", "outcome"],
        "sinomo.kv.commands" | "sinomo.kv.command.total.us" => &["command", "outcome"],
        "sinomo.kv.storage.operations" => &["operation", "outcome"],
        "sinomo.kv.compaction.runs"
        | "sinomo.kv.wal.read.us"
        | "sinomo.kv.wal.append.us"
        | "sinomo.kv.wal.rewrite.us"
        | "sinomo.kv.compaction.us"
        | "sinomo.kv.raft.open.us"
        | "sinomo.kv.apply.us"
        | "sinomo.kv.snapshot.write.us"
        | "sinomo.kv.snapshot.restore.us"
        | "sinomo.kv.replication.target_results"
        | "sinomo.kv.replication.target.us"
        | "sinomo.kv.replication.catchup_attempts"
        | "sinomo.kv.fanout.passes"
        | "sinomo.kv.fanout.pass.us"
        | "sinomo.kv.fanout.target_results"
        | "sinomo.kv.fanout.target.us" => &["outcome"],
        "sinomo.kv.replication.attempts" | "sinomo.kv.replication.entries" => &["payload"],
        "sinomo.kv.command.phase.us" => &["phase"],
        "sinomo.kv.quorum.acks" => &["kind"],
        "sinomo.observe.metric_exporter.errors" => &["exporter", "reason"],
        "sinomo.node.load.average" => &["period"],
        "sinomo.dfs.metadata_shard.journal_next_sequence"
        | "sinomo.dfs.metadata_shard.journal_last_sequence"
        | "sinomo.dfs.metadata_shard.replicas" => {
            &["shard_id", "file_system_id", "primary_node_id"]
        }
        "sinomo.dfs.metadata_shard.replica_lag_entries"
        | "sinomo.dfs.metadata_shard.replica_replicated_through_sequence" => &[
            "shard_id",
            "file_system_id",
            "primary_node_id",
            "target_node_id",
        ],
        "sinomo.dfs.metadata_shard.replay_replayed_through_sequence" => &[
            "shard_id",
            "file_system_id",
            "primary_node_id",
            "source_shard_id",
        ],
        "sinomo.kv.revision"
        | "sinomo.kv.compacted_revision"
        | "sinomo.kv.entries"
        | "sinomo.kv.leases"
        | "sinomo.kv.retained_events"
        | "sinomo.kv.wal.bytes"
        | "sinomo.kv.raft_snapshot.bytes"
        | "sinomo.kv.materialized_snapshot.bytes"
        | "sinomo.kv.raft.term"
        | "sinomo.kv.raft.commit_index"
        | "sinomo.kv.raft.applied_index"
        | "sinomo.kv.raft.last_log_index"
        | "sinomo.kv.raft.snapshot_index"
        | "sinomo.kv.raft.uncommitted_entries"
        | "sinomo.kv.raft.unsnapshotted_entries"
        | "sinomo.kv.raft.retained_log_entries"
        | "sinomo.kv.raft.is_leader" => &["node_id"],
        "sinomo.kv.peer.progress_known"
        | "sinomo.kv.peer.lag_entries"
        | "sinomo.kv.peer.match_index"
        | "sinomo.kv.peer.progress_age.seconds" => &["node_id", "peer_node_id"],
        _ => &[],
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct NodeResourceSnapshot {
    uptime_seconds: u64,
    logical_cpu_count: usize,
    cpu_utilization: f64,
    load_one: f64,
    load_five: f64,
    load_fifteen: f64,
    memory_total_bytes: u64,
    memory_available_bytes: u64,
    swap_used_bytes: u64,
    data_disk_total_bytes: u64,
    data_disk_available_bytes: u64,
    network_received_bytes: u64,
    network_transmitted_bytes: u64,
    process_resident_memory_bytes: u64,
}

static NODE_RESOURCE_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();
static NODE_NETWORK_BASELINE: OnceLock<(u64, u64)> = OnceLock::new();

fn initialize_node_resource_sampling() {
    if !sysinfo::IS_SUPPORTED_SYSTEM {
        return;
    }
    let _ = node_resource_system();
    let _ = NODE_NETWORK_BASELINE.set(node_network_totals());
}

fn node_resource_system() -> &'static Mutex<System> {
    NODE_RESOURCE_SYSTEM.get_or_init(|| Mutex::new(System::new()))
}

fn refresh_node_resource_system(system: &mut System) {
    system.refresh_cpu_usage();
    system.refresh_memory();
    let Ok(pid) = sysinfo::get_current_pid() else {
        return;
    };
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory().without_tasks(),
    );
}

fn node_network_totals() -> (u64, u64) {
    Networks::new_with_refreshed_list().iter().fold(
        (0_u64, 0_u64),
        |(received, transmitted), (_, network)| {
            (
                received.saturating_add(network.total_received()),
                transmitted.saturating_add(network.total_transmitted()),
            )
        },
    )
}

fn node_resource_observe_metrics(
    data_dir: &SinomoDataDir,
) -> Result<Vec<crate::ObserveMetricRecord>, crate::ObserveError> {
    if !sysinfo::IS_SUPPORTED_SYSTEM {
        return Ok(Vec::new());
    }
    let mut system = node_resource_system()
        .lock()
        .map_err(|_| crate::ObserveError::Runtime {
            detail: "node resource sampler lock is poisoned".to_string(),
        })?;
    // Host gauges need global CPU/memory and this daemon's RSS, not a complete
    // process inventory. `refresh_all` walks every `/proc/<pid>/task/<tid>` on
    // Linux and made one dashboard pull issue tens of thousands of reads.
    refresh_node_resource_system(&mut system);
    let load = System::load_average();
    let disks = Disks::new_with_refreshed_list();
    let data_disk = disks
        .list()
        .iter()
        .filter(|disk| data_dir.root().starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count());
    let (network_received_total, network_transmitted_total) = node_network_totals();
    let (network_received_baseline, network_transmitted_baseline) =
        *NODE_NETWORK_BASELINE.get_or_init(|| (network_received_total, network_transmitted_total));
    let process_resident_memory_bytes = sysinfo::get_current_pid()
        .ok()
        .and_then(|pid| system.process(pid))
        .map(|process| process.memory())
        .unwrap_or_default();
    let snapshot = NodeResourceSnapshot {
        uptime_seconds: System::uptime(),
        logical_cpu_count: system.cpus().len(),
        cpu_utilization: f64::from(system.global_cpu_usage()) / 100.0,
        load_one: load.one,
        load_five: load.five,
        load_fifteen: load.fifteen,
        memory_total_bytes: system.total_memory(),
        memory_available_bytes: system.available_memory(),
        swap_used_bytes: system.used_swap(),
        data_disk_total_bytes: data_disk.map(|disk| disk.total_space()).unwrap_or_default(),
        data_disk_available_bytes: data_disk
            .map(|disk| disk.available_space())
            .unwrap_or_default(),
        network_received_bytes: network_received_total.saturating_sub(network_received_baseline),
        network_transmitted_bytes: network_transmitted_total
            .saturating_sub(network_transmitted_baseline),
        process_resident_memory_bytes,
    };
    Ok(node_resource_metric_records(snapshot))
}

fn node_resource_metric_records(snapshot: NodeResourceSnapshot) -> Vec<crate::ObserveMetricRecord> {
    let gauge =
        |name, value| crate::ObserveMetricRecord::gauge(name, BTreeMap::new(), value as f64);
    let mut records = vec![
        gauge("sinomo.node.uptime.seconds", snapshot.uptime_seconds),
        gauge("sinomo.node.cpu.logical", snapshot.logical_cpu_count as u64),
        crate::ObserveMetricRecord::gauge(
            "sinomo.node.cpu.utilization",
            BTreeMap::new(),
            snapshot.cpu_utilization.clamp(0.0, 1.0),
        ),
        gauge(
            "sinomo.node.memory.total.bytes",
            snapshot.memory_total_bytes,
        ),
        gauge(
            "sinomo.node.memory.used.bytes",
            snapshot
                .memory_total_bytes
                .saturating_sub(snapshot.memory_available_bytes),
        ),
        gauge(
            "sinomo.node.memory.available.bytes",
            snapshot.memory_available_bytes,
        ),
        gauge("sinomo.node.swap.used.bytes", snapshot.swap_used_bytes),
        gauge(
            "sinomo.node.data_disk.total.bytes",
            snapshot.data_disk_total_bytes,
        ),
        gauge(
            "sinomo.node.data_disk.available.bytes",
            snapshot.data_disk_available_bytes,
        ),
        crate::ObserveMetricRecord::counter(
            "sinomo.node.network.received.bytes",
            BTreeMap::new(),
            snapshot.network_received_bytes,
        ),
        crate::ObserveMetricRecord::counter(
            "sinomo.node.network.transmitted.bytes",
            BTreeMap::new(),
            snapshot.network_transmitted_bytes,
        ),
        gauge(
            "sinomo.node.process.resident_memory.bytes",
            snapshot.process_resident_memory_bytes,
        ),
    ];
    for (period, value) in [
        ("1m", snapshot.load_one),
        ("5m", snapshot.load_five),
        ("15m", snapshot.load_fifteen),
    ] {
        records.push(crate::ObserveMetricRecord::gauge(
            "sinomo.node.load.average",
            BTreeMap::from([("period".to_string(), period.to_string())]),
            value,
        ));
    }
    records
}

/// Convert a bounded DFS status image into the built-in metric contract.
/// Hosts with resident runtimes can use this without reopening durable state.
pub fn dfs_status_observe_metrics(
    status: sinomo_dfs::DfsOperatorStatus,
) -> Vec<crate::ObserveMetricRecord> {
    sinomo_dfs::dfs_status_observe_metrics(status)
}

fn kv_observe_metrics(
    data_dir: &SinomoDataDir,
) -> Result<Vec<crate::ObserveMetricRecord>, crate::ObserveError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let state =
        match sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths) {
            Ok(state) => state,
            Err(_) if !cluster_paths.state.exists() => return Ok(Vec::new()),
            Err(source) => {
                return Err(crate::ObserveError::Runtime {
                    detail: format!(
                        "kv snapshot metric export failed to read cluster state: {source}"
                    ),
                });
            }
        };
    let kv_paths = cluster_paths.kv_paths();
    let resident_snapshot =
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
            &cluster_paths,
        )
        .map_err(|source| crate::ObserveError::Runtime {
            detail: format!("kv resident snapshot metric export failed: {source}"),
        })?;
    let durable_snapshot;
    let snapshot = if let Some(snapshot) = resident_snapshot.as_deref() {
        snapshot
    } else {
        durable_snapshot = sinomo_core::kv::read_cluster_kv_current_snapshot_for_node(
            &kv_paths,
            state.local_node_id.clone(),
        )
        .map_err(|source| crate::ObserveError::Runtime {
            detail: format!("kv snapshot metric export failed: {source}"),
        })?;
        &durable_snapshot
    };
    let node_labels = labels([("node_id", state.local_node_id.to_string())]);
    let mut metrics = vec![
        crate::ObserveMetricRecord::gauge(
            "sinomo.kv.revision",
            node_labels.clone(),
            snapshot.revision.0 as f64,
        ),
        crate::ObserveMetricRecord::gauge(
            "sinomo.kv.compacted_revision",
            node_labels.clone(),
            snapshot.compacted_revision.0 as f64,
        ),
        crate::ObserveMetricRecord::gauge(
            "sinomo.kv.entries",
            node_labels.clone(),
            snapshot.entries.len() as f64,
        ),
        crate::ObserveMetricRecord::gauge(
            "sinomo.kv.leases",
            node_labels.clone(),
            snapshot.leases.len() as f64,
        ),
        crate::ObserveMetricRecord::gauge(
            "sinomo.kv.retained_events",
            node_labels.clone(),
            snapshot.events.len() as f64,
        ),
    ];
    let wal_paths = kv_paths.raft_wal_paths(&state.local_node_id);
    metrics.push(crate::ObserveMetricRecord::gauge(
        "sinomo.kv.wal.bytes",
        node_labels.clone(),
        file_len(&wal_paths.wal) as f64,
    ));
    metrics.push(crate::ObserveMetricRecord::gauge(
        "sinomo.kv.raft_snapshot.bytes",
        node_labels.clone(),
        file_len(&wal_paths.snapshot) as f64,
    ));
    metrics.push(crate::ObserveMetricRecord::gauge(
        "sinomo.kv.materialized_snapshot.bytes",
        node_labels.clone(),
        file_len(&kv_paths.snapshot) as f64,
    ));

    if let Some(live) =
        sinomo_core::kv::read_cluster_kv_raft_live_metrics(&kv_paths, &state.local_node_id)
    {
        for (name, value) in [
            ("sinomo.kv.raft.term", live.current_term),
            ("sinomo.kv.raft.commit_index", live.commit_index),
            ("sinomo.kv.raft.applied_index", live.applied_index),
            ("sinomo.kv.raft.last_log_index", live.last_log_index),
            ("sinomo.kv.raft.snapshot_index", live.snapshot_index),
            (
                "sinomo.kv.raft.uncommitted_entries",
                live.last_log_index.saturating_sub(live.commit_index),
            ),
            (
                "sinomo.kv.raft.unsnapshotted_entries",
                live.applied_index.saturating_sub(live.snapshot_index),
            ),
        ] {
            metrics.push(crate::ObserveMetricRecord::gauge(
                name,
                node_labels.clone(),
                value as f64,
            ));
        }
        metrics.push(crate::ObserveMetricRecord::gauge(
            "sinomo.kv.raft.retained_log_entries",
            node_labels.clone(),
            live.retained_log_entries as f64,
        ));
        metrics.push(crate::ObserveMetricRecord::gauge(
            "sinomo.kv.raft.is_leader",
            node_labels.clone(),
            if live.leader_id.as_ref() == Some(&state.local_node_id) {
                1.0
            } else {
                0.0
            },
        ));

        let peer_progress =
            sinomo_core::kv::read_cluster_kv_raft_peer_progress(&kv_paths).unwrap_or_default();
        let now = time::OffsetDateTime::now_utc();
        for member in state
            .members
            .iter()
            .filter(|member| member.node_id != state.local_node_id)
        {
            let mut peer_labels = node_labels.clone();
            peer_labels.insert("peer_node_id".to_string(), member.node_id.to_string());
            let progress = peer_progress
                .iter()
                .find(|progress| progress.peer_node_id == member.node_id);
            metrics.push(crate::ObserveMetricRecord::gauge(
                "sinomo.kv.peer.progress_known",
                peer_labels.clone(),
                if progress.is_some() { 1.0 } else { 0.0 },
            ));
            let Some(progress) = progress else {
                continue;
            };
            let age_seconds = (now - progress.observed_at).whole_seconds().max(0) as f64;
            for (name, value) in [
                (
                    "sinomo.kv.peer.lag_entries",
                    live.last_log_index.saturating_sub(progress.match_index) as f64,
                ),
                ("sinomo.kv.peer.match_index", progress.match_index as f64),
                ("sinomo.kv.peer.progress_age.seconds", age_seconds),
            ] {
                metrics.push(crate::ObserveMetricRecord::gauge(
                    name,
                    peer_labels.clone(),
                    value,
                ));
            }
        }
    }
    Ok(metrics)
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn labels<const N: usize>(labels: [(&str, String); N]) -> BTreeMap<String, String> {
    labels
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_metrics_do_not_invent_a_dfs_inventory_without_its_component() {
        let root = sinomo_testdir::TestDir::new("observe-no-implicit-dfs-inventory");
        let data_dir = SinomoDataDir::new(root.path());
        register_builtin_observe_metric_exporters();
        let response = crate::read_observe_metrics_for_data_dir(
            &data_dir,
            &crate::ObserveMetricsRequest {
                name_prefix: Some("sinomo.dfs.objects".into()),
                limit: 0,
                ..Default::default()
            },
        );
        assert!(!response.partial);
        assert!(
            response
                .metrics
                .iter()
                .all(|metric| metric.name != "sinomo.dfs.objects"),
            "only the DFS component may publish its inventory, never an implicit disk reader"
        );
        assert!(!data_dir.dfs_dir().exists());
    }

    #[test]
    fn extended_metric_descriptors_publish_units_and_bounded_dimensions() {
        register_observe_metric_descriptors();
        let descriptors = crate::observe_metric_descriptors();
        let descriptor = |name: &str| {
            descriptors
                .iter()
                .find(|descriptor| descriptor.name == name)
                .unwrap_or_else(|| panic!("missing descriptor {name}"))
        };

        let network = descriptor("sinomo.network.rpc.total.us");
        assert_eq!(network.unit, Some(crate::ObserveMetricUnit::Microseconds));
        assert_eq!(
            network.semantics.labels,
            ["outcome".to_string(), "path".to_string()]
        );
        let dfs = descriptor("sinomo.dfs.operations");
        assert_eq!(
            dfs.semantics.labels,
            [
                "operation".to_string(),
                "outcome".to_string(),
                "plane".to_string()
            ]
        );
        let node = descriptor("sinomo.node.network.received.bytes");
        assert_eq!(node.unit, Some(crate::ObserveMetricUnit::Bytes));
        assert!(node.semantics.labels.is_empty());
    }

    #[test]
    fn node_resource_records_are_aggregated_and_identifier_free() {
        let records = node_resource_metric_records(NodeResourceSnapshot {
            uptime_seconds: 120,
            logical_cpu_count: 8,
            cpu_utilization: 0.25,
            load_one: 1.0,
            load_five: 0.5,
            load_fifteen: 0.25,
            memory_total_bytes: 1_000,
            memory_available_bytes: 400,
            swap_used_bytes: 10,
            data_disk_total_bytes: 2_000,
            data_disk_available_bytes: 750,
            network_received_bytes: 100,
            network_transmitted_bytes: 200,
            process_resident_memory_bytes: 300,
        });

        assert_eq!(
            records
                .iter()
                .find(|record| record.name == "sinomo.node.memory.used.bytes")
                .and_then(|record| record.value),
            Some(600.0)
        );
        assert_eq!(
            records
                .iter()
                .find(|record| record.name == "sinomo.node.cpu.utilization")
                .and_then(|record| record.value),
            Some(0.25)
        );
        assert!(records.iter().all(|record| {
            record.labels.is_empty()
                || (record.name == "sinomo.node.load.average"
                    && record.labels.keys().all(|label| label == "period"))
        }));
        assert_eq!(
            records
                .iter()
                .filter(|record| record.name == "sinomo.node.load.average")
                .count(),
            3
        );
    }

    #[test]
    fn node_resource_refresh_keeps_only_the_current_process() {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return;
        }
        let mut system = System::new();
        refresh_node_resource_system(&mut system);
        let pid = sysinfo::get_current_pid().expect("current pid");

        assert!(system.process(pid).is_some());
        assert_eq!(
            system.processes().len(),
            1,
            "node metrics must not inventory unrelated processes or their tasks"
        );
    }

    #[test]
    fn node_resource_exporter_collects_the_supported_host() {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return;
        }
        initialize_node_resource_sampling();
        let root = sinomo_testdir::TestDir::new("node-resource-observe");
        let data_dir = SinomoDataDir::new(root.path());

        let records = node_resource_observe_metrics(&data_dir).expect("node resource metrics");

        assert!(
            records
                .iter()
                .any(|record| record.name == "sinomo.node.cpu.utilization")
        );
        assert!(records.iter().any(|record| {
            record.name == "sinomo.node.memory.total.bytes"
                && record.value.is_some_and(|value| value > 0.0)
        }));
        assert!(records.iter().all(|record| {
            record.labels.is_empty()
                || (record.name == "sinomo.node.load.average"
                    && record.labels.keys().all(|label| label == "period"))
        }));
    }
}
