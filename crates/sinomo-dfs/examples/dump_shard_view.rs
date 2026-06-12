// One-off debug tool: print this data dir's local view of the SIFS metadata
// shard assignments (read from the local cluster KV snapshot, exactly like the
// CLI's SIFS backend does).
use sinomo_dfs::{DfsMetadataShardConfig, metadata_shards_prefix, sifs_file_system_id};

fn main() {
    let cluster_root = std::env::args()
        .nth(1)
        .expect("usage: dump_shard_view <cluster-dir>");
    let kv_paths = sinomo_kv::ClusterKvPaths::new(&cluster_root);
    let prefix = format!("{}/", metadata_shards_prefix(&sifs_file_system_id()));
    let entries =
        sinomo_kv::read_cluster_kv_prefix_entries(&kv_paths, &prefix).expect("read kv prefix");
    if entries.is_empty() {
        println!("no shard configs under {prefix}");
    }
    for entry in entries {
        let config: DfsMetadataShardConfig = entry.decode().expect("decode shard config");
        println!(
            "{}: primary={} epoch={} replicas={:?}",
            entry.key, config.primary_node_id, config.epoch, config.replica_node_ids
        );
    }
}
