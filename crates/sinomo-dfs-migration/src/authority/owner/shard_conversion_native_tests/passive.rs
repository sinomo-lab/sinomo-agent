//! A node that is primary of no shard, holding a closed copy. Its startup
//! prepares every copy it holds; a closed copy does not hold that startup
//! back, so the node serves the other shards while the copy converts, and
//! serves the shard once its storage activates.
use super::*;

/// The prefix of `node`'s own copy of `shard`, read without opening it for
/// writing.
fn local_prefix(
    cluster: &Cluster,
    node: usize,
    shard: &str,
) -> Option<sinomo_dfs::DfsMetadataPrefixProof> {
    use sinomo_dfs::DfsMetadataBackend as _;
    let owner = canonical_migration_runtime(
        &cluster
            .native
            .component_context(&cluster.nodes[node])
            .unwrap(),
    );
    let id = sinomo_dfs::DfsMetadataShardId::new(shard);
    sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
        id.clone(),
        owner.metadata_root.join(shard),
    )
    .ok()?
    .metadata_prefix_proof(&id)
    .ok()?
}

/// Node 2 is a member of every shard and primary of none. It restarts while
/// users is closed (held, with a Converting record). Its startup completes,
/// and it serves reads and writes of root and services; users' own primary
/// restarts and completes its startup too. Users then converts on its
/// primary and activates on its members; node 2 serves users and its own
/// copy follows the primary's writes.
#[test]
fn native_a_node_primary_of_no_shard_starts_and_serves_beside_a_closed_copy() {
    let mut cluster = start(
        "shard-conversion-passive-startup",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 1, &[0, 2]),
        ],
    );
    write_namespace(&cluster);
    for node in cluster.nodes.clone().iter().rev() {
        cluster.native.stop_node(node).unwrap();
    }
    cluster.held.lock().unwrap().insert("users".into());
    for index in [0, 1] {
        let failure = cluster
            .native
            .reopen_node_with_startup_action(
                cluster.nodes[index].clone(),
                Box::new(|_| Err(HELD.into())),
            )
            .unwrap_err();
        assert!(failure.contains(HELD), "{failure}");
    }
    let fs = cluster.fs.clone();
    close(
        &canonical_migration_runtime(&cluster.native.component_context(&cluster.nodes[0]).unwrap()),
        &fs,
        "users",
    );

    // The ordinary reopen waits for node 2's startup generation to complete.
    cluster
        .native
        .reopen_node(cluster.nodes[2].clone())
        .unwrap();
    assert!(
        storage(
            &canonical_migration_runtime(
                &cluster.native.component_context(&cluster.nodes[2]).unwrap()
            ),
            &fs,
            "users"
        )
        .is_some_and(|record| record.converting_attempt().is_some()),
        "users is still closed"
    );
    {
        let mut client = cluster.native.client(&cluster.nodes[2]).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/services/file")
                .unwrap(),
            b"services bytes"
        );
    }
    write_beside_closed(&cluster, 2, "/written-beside-closed");
    write_beside_closed(&cluster, 2, "/services/written-beside-closed");
    // Users' own primary also completes its startup while its copy is closed.
    cluster.native.stop_node(&cluster.nodes[1].clone()).unwrap();
    cluster
        .native
        .reopen_node(cluster.nodes[1].clone())
        .unwrap();
    write_beside_closed(&cluster, 2, "/services/written-after-primary-restart");

    let owners = owners(&cluster);
    assert_eq!(drive(&owners[1], &fs, "users"), ConversionProgress::Ready);
    let config = shard_config(&owners[0], &fs, "users");
    let record = await_activated(&cluster, &owners, "users", &configured(&config));
    await_unstaged(
        &cluster,
        &owners,
        "users",
        &record.converted_activation().unwrap().0.attempt_id,
    );
    release(&cluster, &owners, "users");

    await_serving(&cluster, 2, &config, "/users/doc");
    write_beside_closed(&cluster, 2, "/users/written-after-activation");
    {
        let mut client = cluster.native.client(&cluster.nodes[2]).unwrap();
        assert_eq!(
            client.posix_read_file_distributed("/users/doc").unwrap(),
            b"users bytes"
        );
        assert_eq!(
            client
                .posix_read_file_distributed("/services/file")
                .unwrap(),
            b"services bytes"
        );
    }
    // Node 2's own copy of users takes the primary's writes again.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (primary, local) = (
            local_prefix(&cluster, 1, "users"),
            local_prefix(&cluster, 2, "users"),
        );
        if primary.is_some() && primary == local {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 2's users copy did not follow its primary: primary {primary:?}, local {local:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(owners);
    cluster.native.close().unwrap();
}
