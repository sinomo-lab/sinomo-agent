//! Cluster lifecycle runtime test cases.
//!
//! These probes are intended for live-cluster orchestration scripts. The
//! migration probes let a Docker harness seed an initialized single-node source
//! cluster, join it into a live target cluster, then verify the resulting target
//! KV and local quarantine evidence.

use sinomo_runtime::core::{ClusterMigrationPhase, ClusterMigrationRecord};
use sinomo_runtime::dfs::{default_dfs_metadata_shards_root, dfs_import_metadata_root};
use sinomo_runtime::kv::{KvCommand, KvOp, KvTxn};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_NODE_CONFIG_SUFFIX: &str = "local-test";
const DEFAULT_NODE_CONFIG_PAYLOAD: &str = "node-local";
const DEFAULT_CLUSTER_CONFIG_KEY: &str = "/config/source-cluster-only";
const DEFAULT_CLUSTER_CONFIG_PAYLOAD: &str = "source-cluster";
const MIGRATION_DFS_MARKER_RELATIVE: &[&str] = &["migration-source", "marker.txt"];

pub fn register_cluster_migration_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(cluster_migration_seed_source())
        .register(cluster_migration_verify_joined())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn cluster_migration_seed_source(ctx) {
        id: "cluster.migration.seed-source",
        summary: "Seed a settled single-node source cluster with migration fixture KV and DFS metadata",
        tags: ["cluster-migration", "probe"],
        requirements: [],
        run: {
            run_cluster_migration_seed_source(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn cluster_migration_verify_joined(ctx) {
        id: "cluster.migration.verify-joined",
        summary: "Verify a live single-node cluster join migration finalized with target KV authority",
        tags: ["cluster-migration", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_cluster_migration_verify_joined(ctx)
        }
    }
}

fn run_cluster_migration_seed_source(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let paths = ClusterPaths::new(&data_dir);
    let state = network::read_cluster_state_without_kv(&paths)
        .map_err(|source| SinomoTestError::failed(format!("cannot read source state: {source}")))?;
    if !network::cluster_is_settled_solo(&state) {
        return Ok(SinomoTestOutcome::failed(
            "source cluster must be a settled single-node cluster before migration",
        ));
    }

    let node_config_key = migration_node_config_key(ctx, &state.local_node_id.to_string())?;
    let node_config_payload =
        parameter_or_default(ctx, "node-config-payload", DEFAULT_NODE_CONFIG_PAYLOAD).to_string();
    let cluster_config_key =
        parameter_or_default(ctx, "cluster-config-key", DEFAULT_CLUSTER_CONFIG_KEY).to_string();
    if !cluster_config_key.starts_with("/config/") {
        return Err(SinomoTestError::InvalidParameter {
            name: "cluster-config-key".to_string(),
            detail: "must start with /config/ so migration treats it as cluster-level config"
                .to_string(),
        });
    }
    let cluster_config_payload = parameter_or_default(
        ctx,
        "cluster-config-payload",
        DEFAULT_CLUSTER_CONFIG_PAYLOAD,
    )
    .to_string();

    let snapshot = paths
        .kv_paths()
        .append_kv_command_and_publish(
            &state.local_node_id,
            KvCommand::Txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: node_config_key.clone(),
                        value: node_config_payload.as_bytes().to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: cluster_config_key.clone(),
                        value: cluster_config_payload.as_bytes().to_vec(),
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            }),
            sinomo_runtime::kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(|source| {
            SinomoTestError::failed(format!("cannot seed source cluster KV: {source}"))
        })?;

    let dfs_marker = migration_dfs_marker_path(&data_dir);
    if let Some(parent) = dfs_marker.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot create migration DFS marker parent {}: {source}",
                parent.display()
            ))
        })?;
    }
    fs::write(&dfs_marker, b"source metadata marker").map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot write migration DFS marker {}: {source}",
            dfs_marker.display()
        ))
    })?;

    ctx.record("source_cluster_id", state.cluster_id.to_string());
    ctx.record("source_node_id", state.local_node_id.to_string());
    ctx.record("node_config_key", &node_config_key);
    ctx.record("cluster_config_key", &cluster_config_key);
    ctx.record("source_kv_revision", snapshot.revision.0);
    ctx.record("dfs_marker", dfs_marker.display().to_string());
    Ok(SinomoTestOutcome::passed(
        "source cluster migration fixture seeded",
    ))
}

fn run_cluster_migration_verify_joined(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let paths = ClusterPaths::new(&data_dir);
    let state = network::read_cluster_state_without_kv(&paths)
        .map_err(|source| SinomoTestError::failed(format!("cannot read target state: {source}")))?;
    let migration = find_migration_record_for_target(&paths, &state.cluster_id)?;
    let source_quarantine = paths
        .root
        .join("migrations")
        .join(migration.migration_id.to_string())
        .join("source");
    let import_report_path = source_quarantine.join("kv-import.json");
    let import_report = read_json(&import_report_path)?;

    let node_config_key = migration_node_config_key(ctx, &state.local_node_id.to_string())?;
    let node_config_payload =
        parameter_or_default(ctx, "node-config-payload", DEFAULT_NODE_CONFIG_PAYLOAD).to_string();
    let cluster_config_key =
        parameter_or_default(ctx, "cluster-config-key", DEFAULT_CLUSTER_CONFIG_KEY).to_string();
    let snapshot = ctx.cluster_snapshot().ok_or_else(|| {
        SinomoTestError::failed("verify-joined requires a cluster KV snapshot in the test context")
    })?;

    assert_equal(
        "target_node_id",
        migration.target_node_id.to_string(),
        state.local_node_id.to_string(),
    )?;
    assert_equal(
        "source_local_node_id",
        migration.source_local_node_id.to_string(),
        state.local_node_id.to_string(),
    )?;
    if migration.phase != ClusterMigrationPhase::Finalized {
        return Ok(SinomoTestOutcome::failed(format!(
            "migration phase is {}, expected finalized",
            migration.phase.as_str()
        )));
    }
    if snapshot.get(&cluster_config_key).is_some() {
        return Ok(SinomoTestOutcome::failed(format!(
            "source cluster-level config key {cluster_config_key} was imported into target KV"
        )));
    }
    let Some(imported_node_config) = snapshot.get(&node_config_key) else {
        return Ok(SinomoTestOutcome::failed(format!(
            "target KV is missing imported node-local config key {node_config_key}"
        )));
    };
    if imported_node_config.value != node_config_payload.as_bytes() {
        return Ok(SinomoTestOutcome::failed(format!(
            "target KV node-local config payload mismatch for {node_config_key}"
        )));
    }
    json_array_contains_string(
        &import_report,
        "imported_node_local_config_keys",
        &node_config_key,
    )?;
    json_array_is_empty(&import_report, "deferred_node_local_config_keys")?;
    let skipped_count = import_report
        .get("skipped_cluster_level_config_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if skipped_count == 0 {
        return Ok(SinomoTestOutcome::failed(
            "migration import report did not skip any cluster-level source config keys",
        ));
    }
    json_array_contains_string(
        &import_report,
        "skipped_cluster_level_config_key_samples",
        &cluster_config_key,
    )?;

    for path in [
        source_quarantine.join("state.sobj"),
        source_quarantine.join("kv-snapshot.sobj"),
        source_quarantine.join("kv-raft"),
    ] {
        if !path.exists() {
            return Ok(SinomoTestOutcome::failed(format!(
                "migration quarantine is missing {}",
                path.display()
            )));
        }
    }
    let imported_marker =
        migration_imported_dfs_marker_path(&data_dir, &migration.source_cluster_id.to_string());
    if !imported_marker.exists() {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS metadata marker was not moved into import root: {}",
            imported_marker.display()
        )));
    }

    ctx.record("target_cluster_id", state.cluster_id.to_string());
    ctx.record("migrated_node_id", state.local_node_id.to_string());
    ctx.record("source_cluster_id", migration.source_cluster_id.to_string());
    ctx.record("migration_id", migration.migration_id.to_string());
    ctx.record("node_config_key", &node_config_key);
    ctx.record("cluster_config_key", &cluster_config_key);
    ctx.record("source_quarantine", source_quarantine.display().to_string());
    ctx.record("kv_import_report", import_report_path.display().to_string());
    ctx.record("dfs_import_marker", imported_marker.display().to_string());
    Ok(SinomoTestOutcome::passed(
        "single-node cluster join migration finalized with target KV authority",
    ))
}

fn parameter_or_default<'a>(ctx: &'a SinomoTestContext, key: &str, default: &'a str) -> &'a str {
    ctx.parameter(key)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(default)
}

fn migration_node_config_key(
    ctx: &SinomoTestContext,
    node_id: &str,
) -> Result<String, SinomoTestError> {
    let suffix = parameter_or_default(ctx, "node-config-key-suffix", DEFAULT_NODE_CONFIG_SUFFIX)
        .trim()
        .trim_start_matches('/');
    if suffix.is_empty() || suffix.contains("..") {
        return Err(SinomoTestError::InvalidParameter {
            name: "node-config-key-suffix".to_string(),
            detail: "must be a non-empty relative config suffix".to_string(),
        });
    }
    Ok(format!("/config/nodes/{node_id}/{suffix}"))
}

fn migration_dfs_marker_path(data_dir: &SinomoDataDir) -> PathBuf {
    MIGRATION_DFS_MARKER_RELATIVE.iter().fold(
        default_dfs_metadata_shards_root(data_dir),
        |path, segment| path.join(segment),
    )
}

fn migration_imported_dfs_marker_path(
    data_dir: &SinomoDataDir,
    source_cluster_id: &str,
) -> PathBuf {
    MIGRATION_DFS_MARKER_RELATIVE.iter().fold(
        dfs_import_metadata_root(data_dir, source_cluster_id),
        |path, segment| path.join(segment),
    )
}

fn find_migration_record_for_target(
    paths: &ClusterPaths,
    target_cluster_id: &sinomo_runtime::node::ClusterId,
) -> Result<ClusterMigrationRecord, SinomoTestError> {
    let migrations_root = paths.root.join("migrations");
    let entries = fs::read_dir(&migrations_root).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot read migrations root {}: {source}",
            migrations_root.display()
        ))
    })?;
    let mut target_matches = Vec::new();
    let mut summaries = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|source| {
                SinomoTestError::failed(format!("cannot read migration dir: {source}"))
            })?
            .path()
            .join("source")
            .join("migration.json");
        if !path.exists() {
            continue;
        }
        let migration: ClusterMigrationRecord = read_json(&path)?;
        summaries.push(format!(
            "{} phase={} source={} target={}",
            migration.migration_id,
            migration.phase.as_str(),
            migration.source_cluster_id,
            migration.target_cluster_id
        ));
        if migration.target_cluster_id == *target_cluster_id {
            target_matches.push(migration);
        }
    }
    if target_matches.len() != 1 {
        return Err(SinomoTestError::failed(format!(
            "expected exactly one migration for target cluster {target_cluster_id}, found {}; records: {}",
            target_matches.len(),
            summaries.join("; ")
        )));
    }
    Ok(target_matches.remove(0))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, SinomoTestError> {
    let bytes = fs::read(path).map_err(|source| {
        SinomoTestError::failed(format!("cannot read {}: {source}", path.display()))
    })?;
    serde_json::from_slice(&bytes).map_err(|source| {
        SinomoTestError::failed(format!("cannot decode {}: {source}", path.display()))
    })
}

fn assert_equal(name: &str, actual: String, expected: String) -> Result<(), SinomoTestError> {
    if actual == expected {
        Ok(())
    } else {
        Err(SinomoTestError::failed(format!(
            "{name} mismatch: got {actual}, expected {expected}"
        )))
    }
}

fn json_array_contains_string(
    value: &serde_json::Value,
    key: &str,
    expected: &str,
) -> Result<(), SinomoTestError> {
    let Some(array) = value.get(key).and_then(serde_json::Value::as_array) else {
        return Err(SinomoTestError::failed(format!(
            "import report field {key} is not an array"
        )));
    };
    if array
        .iter()
        .any(|value| value.as_str().is_some_and(|actual| actual == expected))
    {
        Ok(())
    } else {
        Err(SinomoTestError::failed(format!(
            "import report field {key} does not contain {expected}"
        )))
    }
}

fn json_array_is_empty(value: &serde_json::Value, key: &str) -> Result<(), SinomoTestError> {
    let Some(array) = value.get(key).and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    if array.is_empty() {
        Ok(())
    } else {
        Err(SinomoTestError::failed(format!(
            "import report field {key} is not empty: {array:?}"
        )))
    }
}
