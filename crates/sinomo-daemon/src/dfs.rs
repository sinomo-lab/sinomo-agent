use crate::{
    DaemonError, DaemonPaths, DaemonRunOptions, cluster_has_local_quorum,
    put_cluster_kv_control_record,
};
use sinomo_core::DfsObjectPolicy;
use sinomo_dfs::{
    DfsExposeStore, DfsFileSystemConfig, DfsMetadataBackend, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeAdvertisement, DfsObjectVersionRecord,
    LocalJsonMetadataBackend, content_service_key, distributed_sifs,
    distributed_sifs_bootstrap_objects, fallback_dfs_node_id, file_system_config_key,
    metadata_service_key, metadata_shard_key,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::ClusterPaths;
use time::OffsetDateTime;

pub fn ensure_default_local_dfs_expose(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = if cluster_paths.state.exists() {
        sinomo_network::read_cluster_state_without_kv(&cluster_paths)
            .map(|state| state.local_node_id)
            .unwrap_or_else(|_| fallback_dfs_node_id())
    } else {
        fallback_dfs_node_id()
    };
    DfsExposeStore::new(data_dir.dfs_expose_config_file())
        .ensure_default(&data_dir.root(), node_id)?;
    Ok(())
}

pub fn handle_daemon_dfs_expose_config(paths: &DaemonPaths) -> Result<String, DaemonError> {
    ensure_default_local_dfs_expose(paths)?;
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let config_path = data_dir.dfs_expose_config_file();
    let config = DfsExposeStore::new(&config_path).load()?;
    serde_json::to_string_pretty(&config).map_err(|source| DaemonError::Json {
        path: config_path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn ensure_default_distributed_dfs(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let mut backend = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    if backend.get_file_system(&file_system.file_system_id)? != Some(file_system.clone()) {
        backend.put_file_system(&file_system)?;
    }
    for object in distributed_sifs_bootstrap_objects(state.local_node_id.clone()) {
        if backend
            .get_object_by_path(&object.file_system_id, object.path.trim_matches('/'))?
            .is_none()
        {
            backend.put_object(&object)?;
            backend.put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })?;
        }
    }
    if !publish_cluster_control || !cluster_has_local_quorum(&cluster_paths, &state) {
        return Ok(());
    }

    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        file_system_config_key(&file_system.file_system_id),
        &DfsFileSystemConfig::from(&file_system),
    )?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_shard_key(&file_system.file_system_id, "root"),
        &DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: state.local_node_id.clone(),
            replica_node_ids: state
                .members
                .iter()
                .filter(|member| member.node_id != state.local_node_id)
                .take(2)
                .map(|member| member.node_id.clone())
                .collect(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    )?;
    let endpoint = format!("loopback://{}:{}", options.host, options.port);
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_service_key(&state.local_node_id),
        &DfsNodeAdvertisement::metadata(state.local_node_id.clone(), endpoint.clone()),
    )?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        content_service_key(&state.local_node_id),
        &DfsNodeAdvertisement::content(state.local_node_id.clone(), endpoint),
    )?;
    Ok(())
}

fn daemon_data_dir(paths: &DaemonPaths) -> Result<&std::path::Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no data directory parent".to_string(),
    })
}
