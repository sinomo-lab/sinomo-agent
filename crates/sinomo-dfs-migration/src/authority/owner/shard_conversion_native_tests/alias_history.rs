//! Shards whose history holds a cross-shard alias unlink convert per shard:
//! the alias's unlink commits the alias shard's change and the canonical
//! target's fence, so the jobs, frames and requests it leaves in both shards'
//! histories are read by the conversion's capture.
use super::*;

fn link_count(cluster: &Cluster, path: &str) -> u64 {
    cluster
        .native
        .client(&cluster.nodes[0])
        .unwrap()
        .posix_stat_distributed(path)
        .unwrap()
        .link_count
}

#[test]
fn native_shards_whose_history_holds_a_cross_shard_alias_unlink_convert() {
    const CANONICAL: &str = "/services/unlinked-canonical";
    const ALIAS: &str = "/users/unlinked-alias";
    let cluster = start(
        "shard-conversion-alias-unlink-history",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    {
        let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
        client
            .posix_write_file(CANONICAL, b"canonical bytes", options(&cluster))
            .unwrap();
        client.posix_hardlink(CANONICAL, ALIAS).unwrap();
        assert_eq!(link_count(&cluster, CANONICAL), 2);
        client.posix_unlink(ALIAS).unwrap();
    }
    assert_eq!(link_count(&cluster, CANONICAL), 1);

    // The alias shard, then the canonical target's shard, converts from a
    // copy whose history holds the unlink.
    for (shard, primary, path) in [("users", 2, "/users"), ("services", 1, CANONICAL)] {
        close(&owners[0], &fs, shard);
        for node in &cluster.nodes {
            cluster
                .native
                .wait_published_configuration_current(node)
                .unwrap();
        }
        assert_eq!(
            drive(&owners[primary], &fs, shard),
            ConversionProgress::Ready,
            "{shard}"
        );
        let config = shard_config(&owners[0], &fs, shard);
        await_activated(&cluster, &owners, shard, &configured(&config));
        for reader in 0..cluster.nodes.len() {
            await_serving(&cluster, reader, &config, path);
        }
    }
    assert_eq!(link_count(&cluster, CANONICAL), 1);
    assert!(
        cluster
            .native
            .client(&cluster.nodes[0])
            .unwrap()
            .posix_lstat_distributed(ALIAS)
            .is_err()
    );
    write_beside_closed(&cluster, 0, "/users/after-conversion");
    write_beside_closed(&cluster, 1, "/services/after-conversion");
    drop(owners);
    cluster.native.close().unwrap();
}
