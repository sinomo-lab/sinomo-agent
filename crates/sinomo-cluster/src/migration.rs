//! Single-node cluster migration: plan, storage quarantine, migration
//! records, and node-local config import.
//!
//! Stage G2 of docs/plans/r0-r1-crate-boundary-refactor.md moved this
//! machinery here from the CLI so the daemon's `/cluster/control/join` can
//! migrate a settled node to another cluster. The flow is: plan (validate the
//! invite against the source cluster), write the migration record into the
//! source KV, quarantine the source cluster storage, run a fresh join against
//! the target, then import the node-local config keys from the quarantined
//! source snapshot — rolling the quarantine back if the join fails.
//!
//! Ring note: everything here is transport-record and file-system level; the
//! quarantine accepts caller-provided extra move pairs so r1 surfaces (the
//! DFS metadata shards) can ride along without this r0 crate knowing their
//! layout.

use crate::control_plane_options::ClusterKvTxnQuorumOptions;
use crate::invite::{invite_has_embedded_signature, verify_cluster_join_invite_signature};
use crate::kv_local_store::append_cluster_raft_command;
use crate::kv_txn_quorum::{
    ClusterKvTxnQuorumHost, commit_cluster_kv_txn_with_quorum_response_with_host,
};
use crate::local_state_store::read_cluster_state;
use sinomo_base::cluster::{
    ClusterMigrationMode, ClusterMigrationPhase, ClusterMigrationRecord, cluster_migration_key,
};
use sinomo_base::{ClusterJoinInvite, ClusterMemberState, Id};
use sinomo_kv::{KvCommand, KvOp, KvTxn};
use sinomo_network::{
    ClusterPaths, NetworkError, cluster_is_settled_solo, cluster_state_needs_join_registration,
};
use sinomo_node::{ClusterId, ClusterInviteId, NodeId, cluster_member_is_local_process};
use std::fs;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

/// Everything a caller needs to describe (or print) a migration plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMigrationPlanReport {
    pub migration: ClusterMigrationRecord,
    pub target_invite_id: ClusterInviteId,
    pub source_member_count: usize,
    pub source_remote_member_count: usize,
    pub target_introducer_count: usize,
    pub target_communication_channel_count: usize,
}

fn migration_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::InvalidClusterCommand {
        detail: detail.into(),
    }
}

fn migration_io_error(path: &Path, detail: impl std::fmt::Display) -> NetworkError {
    NetworkError::StorageIo {
        path: path.display().to_string(),
        detail: detail.to_string(),
    }
}

/// Validates `invite` against the local source cluster and produces the
/// migration record for it. `mode` selects the planned strategy; only
/// [`ClusterMigrationMode::SingleNode`] is executable today.
pub fn plan_cluster_migration(
    paths: &ClusterPaths,
    invite: &ClusterJoinInvite,
    mode: ClusterMigrationMode,
    now: OffsetDateTime,
) -> Result<ClusterMigrationPlanReport, NetworkError> {
    paths.ensure()?;
    let source = read_cluster_state(paths).map_err(|error| match error {
        NetworkError::ClusterOrphan => migration_error(
            "local node has no initialized source cluster; use `sinomo cluster join <invite>` for a fresh node",
        ),
        other => other,
    })?;
    if cluster_state_needs_join_registration(&source) {
        return Err(migration_error(
            "local cluster state is an incomplete legacy join; run `sinomo cluster join <invite>` again for a fresh one-shot attempt or `sinomo cluster leave` to discard it",
        ));
    }
    if mode == ClusterMigrationMode::SingleNode && !cluster_is_settled_solo(&source) {
        return Err(migration_error(
            "single-node migration requires a settled source cluster with no remote peers; use `--mode cluster-merge` to plan a cluster merge",
        ));
    }

    if invite_has_embedded_signature(invite) {
        verify_cluster_join_invite_signature(invite)?;
    }
    if invite.expires_at != OffsetDateTime::UNIX_EPOCH && invite.expires_at <= now {
        return Err(migration_error(format!(
            "cluster invite expired at {}",
            invite.expires_at
        )));
    }
    if invite.cluster_id == source.cluster_id {
        return Err(migration_error(format!(
            "source cluster `{}` already matches the target invite cluster",
            source.cluster_id
        )));
    }

    let source_member_count = source
        .members
        .iter()
        .filter(|member| !matches!(member.state, ClusterMemberState::Removed))
        .count();
    let source_remote_member_count = source
        .members
        .iter()
        .filter(|member| {
            !matches!(member.state, ClusterMemberState::Removed)
                && !cluster_member_is_local_process(&source, member)
        })
        .count();
    let migration = ClusterMigrationRecord {
        migration_id: Id::generate(),
        mode,
        phase: ClusterMigrationPhase::Planned,
        source_cluster_id: source.cluster_id.clone(),
        source_cluster_name: source.cluster_name.clone(),
        target_cluster_id: invite.cluster_id.clone(),
        target_cluster_name: invite.cluster_name.clone(),
        source_local_node_id: source.local_node_id.clone(),
        target_node_id: source.local_node_id.clone(),
        preserved_node_ids: vec![source.local_node_id.clone()],
        remapped_node_ids: Vec::new(),
        evidence_refs: vec![format!("cluster-state:{}", paths.state.display())],
        detail: Some(
            "migration plan only; execution must freeze the source, quarantine old cluster KV/Raft state, register with the target cluster, then verify imported state"
                .to_string(),
        ),
        created_at: now,
        updated_at: now,
    };

    Ok(ClusterMigrationPlanReport {
        migration,
        target_invite_id: invite.invite_id.clone(),
        source_member_count,
        source_remote_member_count,
        target_introducer_count: invite.introducers.len(),
        target_communication_channel_count: invite.communication_channels.len(),
    })
}

/// A reversible move of the source cluster's storage into the migration's
/// quarantine directory. `moved` remembers each rename so a failed target
/// join can restore the source exactly.
#[derive(Clone, Debug)]
pub struct ClusterMigrationQuarantine {
    pub root: PathBuf,
    moved: Vec<ClusterMigrationQuarantinedPath>,
}

#[derive(Clone, Debug)]
struct ClusterMigrationQuarantinedPath {
    original: PathBuf,
    quarantined: PathBuf,
}

/// Moves the source cluster's storage (state, connectivity, KV snapshot,
/// raft, watches) under `<cluster>/migrations/<id>/source`.
///
/// `extra_moves` lets the caller quarantine surfaces this crate does not know
/// the layout of (the daemon adds the DFS metadata shards); each pair is
/// `(original, destination)` and participates in rollback like the built-in
/// paths.
pub fn quarantine_cluster_storage_for_migration(
    paths: &ClusterPaths,
    migration: &ClusterMigrationRecord,
    extra_moves: &[(PathBuf, PathBuf)],
) -> Result<ClusterMigrationQuarantine, NetworkError> {
    let root = paths
        .root
        .join("migrations")
        .join(migration.migration_id.to_string())
        .join("source");
    fs::create_dir_all(&root).map_err(|source| migration_io_error(&root, source))?;

    let mut quarantine = ClusterMigrationQuarantine {
        root,
        moved: Vec::new(),
    };
    let kv_paths = crate::kv_local_store::ClusterKvPathsExt::kv_paths(paths);
    let built_in_moves = [
        (paths.state.clone(), quarantine.root.join("state.sobj")),
        (
            paths.connectivity_local.clone(),
            quarantine.root.join("connectivity-local.sobj"),
        ),
        (
            kv_paths.snapshot.clone(),
            quarantine.root.join("kv-snapshot.sobj"),
        ),
        (kv_paths.raft.clone(), quarantine.root.join("kv-raft")),
        (kv_paths.watches.clone(), quarantine.root.join("watches")),
        (
            kv_paths.watches_lock.clone(),
            quarantine.root.join("watches.lock"),
        ),
        (
            kv_paths.watches_next_id.clone(),
            quarantine.root.join("watches-next.sobj"),
        ),
        (
            kv_paths.watch_events.clone(),
            quarantine.root.join("watch-events.sobj"),
        ),
        (
            kv_paths.watch_events_log.clone(),
            quarantine.root.join("watch-events.sobjlog"),
        ),
    ];
    for (original, destination) in built_in_moves
        .into_iter()
        .chain(extra_moves.iter().cloned())
    {
        move_cluster_path_if_exists(original, destination, &mut quarantine)?;
    }
    Ok(quarantine)
}

/// Moves the cluster's storage under
/// `<cluster>/migrations/<leave-id>/leave-source` for a `cluster leave`
/// (same reversible machinery as the migration quarantine, different root).
pub fn quarantine_cluster_storage_for_leave(
    paths: &ClusterPaths,
    leave_id: &Id,
) -> Result<ClusterMigrationQuarantine, NetworkError> {
    let root = paths
        .root
        .join("migrations")
        .join(leave_id.to_string())
        .join("leave-source");
    fs::create_dir_all(&root).map_err(|source| migration_io_error(&root, source))?;

    let mut quarantine = ClusterMigrationQuarantine {
        root,
        moved: Vec::new(),
    };
    let kv_paths = crate::kv_local_store::ClusterKvPathsExt::kv_paths(paths);
    let moves = [
        (paths.state.clone(), quarantine.root.join("state.sobj")),
        (
            paths.connectivity_local.clone(),
            quarantine.root.join("connectivity-local.sobj"),
        ),
        (
            kv_paths.snapshot.clone(),
            quarantine.root.join("kv-snapshot.sobj"),
        ),
        (kv_paths.raft.clone(), quarantine.root.join("kv-raft")),
        (kv_paths.watches.clone(), quarantine.root.join("watches")),
        (
            kv_paths.watches_lock.clone(),
            quarantine.root.join("watches.lock"),
        ),
        (
            kv_paths.watches_next_id.clone(),
            quarantine.root.join("watches-next.sobj"),
        ),
        (
            kv_paths.watch_events.clone(),
            quarantine.root.join("watch-events.sobj"),
        ),
        (
            kv_paths.watch_events_log.clone(),
            quarantine.root.join("watch-events.sobjlog"),
        ),
    ];
    for (original, destination) in moves {
        move_cluster_path_if_exists(original, destination, &mut quarantine)?;
    }
    Ok(quarantine)
}

/// Whether any of the cluster's on-disk storage artifacts exist (a leave or
/// migration has something to quarantine).
pub fn cluster_storage_has_live_artifacts(paths: &ClusterPaths) -> bool {
    let kv_paths = crate::kv_local_store::ClusterKvPathsExt::kv_paths(paths);
    paths.state.exists()
        || paths.connectivity_local.exists()
        || kv_paths.snapshot.exists()
        || kv_paths.raft.exists()
        || kv_paths.watches.exists()
        || kv_paths.watches_lock.exists()
        || kv_paths.watches_next_id.exists()
        || kv_paths.watch_events.exists()
        || kv_paths.watch_events_log.exists()
}

fn move_cluster_path_if_exists(
    original: PathBuf,
    quarantined: PathBuf,
    quarantine: &mut ClusterMigrationQuarantine,
) -> Result<(), NetworkError> {
    if !original.exists() {
        return Ok(());
    }
    if quarantined.exists() {
        return Err(migration_io_error(
            &quarantined,
            "migration quarantine target already exists",
        ));
    }
    if let Some(parent) = quarantined.parent() {
        fs::create_dir_all(parent).map_err(|source| migration_io_error(parent, source))?;
    }
    fs::rename(&original, &quarantined).map_err(|source| migration_io_error(&original, source))?;
    quarantine.moved.push(ClusterMigrationQuarantinedPath {
        original,
        quarantined,
    });
    Ok(())
}

/// Restores every quarantined path to its original location (used when the
/// target join fails). Refuses to overwrite a path that reappeared.
pub fn rollback_cluster_storage_quarantine(
    quarantine: &ClusterMigrationQuarantine,
) -> Result<(), NetworkError> {
    for moved in quarantine.moved.iter().rev() {
        if !moved.quarantined.exists() {
            continue;
        }
        if moved.original.exists() {
            return Err(migration_io_error(
                &moved.original,
                "cannot rollback migration quarantine because original path exists",
            ));
        }
        if let Some(parent) = moved.original.parent() {
            fs::create_dir_all(parent).map_err(|source| migration_io_error(parent, source))?;
        }
        fs::rename(&moved.quarantined, &moved.original)
            .map_err(|source| migration_io_error(&moved.quarantined, source))?;
    }
    Ok(())
}

/// Persists the migration record beside the quarantined storage
/// (`<quarantine>/migration.json`) so operators can audit or resume it.
pub fn write_local_cluster_migration_record(
    quarantine_root: &Path,
    migration: &ClusterMigrationRecord,
) -> Result<(), NetworkError> {
    fs::create_dir_all(quarantine_root)
        .map_err(|source| migration_io_error(quarantine_root, source))?;
    let path = quarantine_root.join("migration.json");
    let json = serde_json::to_vec_pretty(migration).map_err(|source| NetworkError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(&path, json).map_err(|source| migration_io_error(&path, source))
}

/// Appends the migration record into the SOURCE cluster's KV (raft command on
/// the local store) before the source is frozen, leaving departure evidence
/// for any later audit of the source cluster.
pub fn write_cluster_migration_record_to_source_kv(
    paths: &ClusterPaths,
    migration: &ClusterMigrationRecord,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(
        cluster_migration_key(&migration.migration_id),
        migration,
        None,
    )
    .map_err(crate::kv_local_store::kv_err)?;
    append_cluster_raft_command(paths, &migration.source_local_node_id, KvCommand::Op(op))
        .map(|_| ())
}

const CLUSTER_MIGRATION_NODE_LOCAL_CONFIG_ROOT: &str = "/config/nodes/";
const CLUSTER_MIGRATION_KEY_SAMPLE_LIMIT: usize = 32;

/// What the node-local config import did: which keys were copied into the
/// target cluster's KV, which were deferred, and what was skipped (the target
/// cluster's own config stays authoritative). Written as
/// `<quarantine>/kv-import.json`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClusterMigrationKvImportReport {
    pub source_cluster_id: ClusterId,
    pub target_cluster_id: ClusterId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub target_kv_authoritative: bool,
    pub source_snapshot_present: bool,
    pub source_entry_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imported_node_local_config_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred_node_local_config_keys: Vec<String>,
    pub skipped_cluster_level_config_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_cluster_level_config_key_samples: Vec<String>,
    pub skipped_runtime_or_source_count: usize,
}

fn cluster_migration_node_local_config_prefix(node_id: &NodeId) -> String {
    format!("{CLUSTER_MIGRATION_NODE_LOCAL_CONFIG_ROOT}{node_id}/")
}

pub fn cluster_migration_kv_import_report_path(quarantine_root: &Path) -> PathBuf {
    quarantine_root.join("kv-import.json")
}

fn write_cluster_migration_kv_import_report(
    quarantine_root: &Path,
    report: &ClusterMigrationKvImportReport,
) -> Result<(), NetworkError> {
    fs::create_dir_all(quarantine_root)
        .map_err(|source| migration_io_error(quarantine_root, source))?;
    let path = cluster_migration_kv_import_report_path(quarantine_root);
    let json = serde_json::to_vec_pretty(report).map_err(|source| NetworkError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(&path, json).map_err(|source| migration_io_error(&path, source))
}

/// Copies the source node's `/config/nodes/<node>/…` keys from the
/// quarantined source snapshot into the TARGET cluster's KV through `host`
/// (quorum commit). Cluster-level `/config/...` keys are deliberately skipped
/// — the target cluster's configuration is authoritative — and everything is
/// recorded in the returned report (also persisted beside the quarantine).
///
/// `target_state` is the freshly joined target cluster state; when it still
/// needs join registration the import is deferred rather than committed.
pub fn import_node_local_configs_from_source_kv_with_host(
    paths: &ClusterPaths,
    target_state: &mut sinomo_base::ClusterBootstrapState,
    quarantine_root: &Path,
    migration: &ClusterMigrationRecord,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterMigrationKvImportReport, NetworkError> {
    let mut report = ClusterMigrationKvImportReport {
        source_cluster_id: migration.source_cluster_id.clone(),
        target_cluster_id: migration.target_cluster_id.clone(),
        source_node_id: migration.source_local_node_id.clone(),
        target_node_id: migration.target_node_id.clone(),
        target_kv_authoritative: true,
        source_snapshot_present: false,
        source_entry_count: 0,
        imported_node_local_config_keys: Vec::new(),
        deferred_node_local_config_keys: Vec::new(),
        skipped_cluster_level_config_count: 0,
        skipped_cluster_level_config_key_samples: Vec::new(),
        skipped_runtime_or_source_count: 0,
    };

    let source_kv_paths = sinomo_kv::ClusterKvPaths::new(quarantine_root.to_path_buf());
    if !source_kv_paths.snapshot.exists() {
        write_cluster_migration_kv_import_report(quarantine_root, &report)?;
        return Ok(report);
    }

    let source_snapshot = sinomo_kv::read_cluster_kv_cached_snapshot(&source_kv_paths)
        .map_err(crate::kv_local_store::kv_err)?;
    report.source_snapshot_present = true;
    report.source_entry_count = source_snapshot.entries.len();

    let source_prefix = cluster_migration_node_local_config_prefix(&migration.source_local_node_id);
    let target_prefix = cluster_migration_node_local_config_prefix(&migration.target_node_id);
    let mut import_ops = Vec::new();
    let mut import_keys = Vec::new();
    for entry in &source_snapshot.entries {
        if let Some(suffix) = entry.key.strip_prefix(&source_prefix) {
            let target_key = format!("{target_prefix}{suffix}");
            import_keys.push(target_key.clone());
            import_ops.push(KvOp::Put {
                key: target_key,
                value: entry.value.clone(),
                lease_id: None,
            });
            continue;
        }

        if entry.key.starts_with("/config/") {
            report.skipped_cluster_level_config_count =
                report.skipped_cluster_level_config_count.saturating_add(1);
            if report.skipped_cluster_level_config_key_samples.len()
                < CLUSTER_MIGRATION_KEY_SAMPLE_LIMIT
            {
                report
                    .skipped_cluster_level_config_key_samples
                    .push(entry.key.clone());
            }
        } else {
            report.skipped_runtime_or_source_count =
                report.skipped_runtime_or_source_count.saturating_add(1);
        }
    }

    if import_ops.is_empty() {
        write_cluster_migration_kv_import_report(quarantine_root, &report)?;
        return Ok(report);
    }

    if cluster_state_needs_join_registration(target_state) {
        report.deferred_node_local_config_keys = import_keys;
        write_cluster_migration_kv_import_report(quarantine_root, &report)?;
        return Ok(report);
    }

    commit_cluster_kv_txn_with_quorum_response_with_host(
        paths,
        target_state,
        KvTxn {
            compares: Vec::new(),
            success: import_ops,
            failure: Vec::new(),
        },
        options,
        host,
    )?;
    report.imported_node_local_config_keys = import_keys;
    write_cluster_migration_kv_import_report(quarantine_root, &report)?;
    Ok(report)
}
