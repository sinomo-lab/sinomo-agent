//! The operator status's per-shard storage conversion entries.
use super::*;
use sinomo_dfs::DfsMetadataShardStorageMigrationState as State;

/// `node`'s operator status entry for each shard. Like an operator's status
/// read, it is retried while the node's resident KV is checked out (busy).
fn migrations(cluster: &Cluster, node: usize) -> BTreeMap<String, State> {
    let data_dir = cluster.native.data_dir(&cluster.nodes[node]);
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        let busy = match sinomo_dfs::inspect_cluster_aware_dfs_status(
            data_dir,
            &sinomo_core::network::ClusterPaths::new(data_dir),
        ) {
            Ok(status)
                if !status
                    .incomplete_reasons
                    .iter()
                    .any(|reason| reason.contains("raft node is busy")) =>
            {
                break status;
            }
            Ok(status) => format!("{:?}", status.incomplete_reasons),
            Err(failure) if format!("{failure:?}").contains("raft node is busy") => {
                format!("{failure:?}")
            }
            Err(failure) => panic!("node {node} status: {failure:?}"),
        };
        assert!(Instant::now() < deadline, "node {node} status stays busy: {busy}");
        std::thread::sleep(Duration::from_millis(20));
    };
    status
        .storage_migrations
        .into_iter()
        .map(|entry| {
            assert_eq!(entry.file_system_id, cluster.fs);
            (entry.shard_id.0, entry.state)
        })
        .collect()
}

/// Services loses its storage record and holds an alias whose target no
/// shard has. Every node's status reports services as old storage and the
/// born shards as Active. Once services' primary converts it on its own
/// schedule and stops for the dangling alias, the primary's status reports
/// the stop with its attempt and reason, and a member's status reports the
/// attempt Converting.
#[test]
fn native_operator_status_reports_each_shards_storage_conversion() {
    let mut cluster = start(
        "shard-conversion-operator-status",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    write_namespace(&cluster);
    let target = adopt_dangling_alias(&cluster);
    let fs = cluster.fs.clone();
    let owner =
        canonical_migration_runtime(&cluster.native.component_context(&cluster.nodes[0]).unwrap());
    crate::authority::native_fixtures::remove_storage_record(&owner, &fs, "services");
    drop(owner);
    for node in 0..cluster.nodes.len() {
        let born = State::Active {
            attempt_id: None,
            takeover_round: None,
        };
        assert_eq!(
            migrations(&cluster, node),
            BTreeMap::from([
                ("root".to_string(), born.clone()),
                ("services".to_string(), State::Old),
                ("users".to_string(), born),
            ]),
            "node {node}"
        );
    }

    hold(&mut cluster, &[]);
    let deadline = Instant::now() + Duration::from_secs(120);
    let (attempt_id, reason) = loop {
        if let Some(State::Stopped {
            attempt_id: Some(attempt_id),
            takeover_round: None,
            reason,
        }) = migrations(&cluster, 1).remove("services")
        {
            break (attempt_id, reason);
        }
        assert!(
            Instant::now() < deadline,
            "services' primary does not report its stop: {:?}",
            migrations(&cluster, 1)
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        reason.contains("dangling alias /services/dangling") && reason.contains(target.as_str()),
        "{reason}"
    );
    let owners = owners(&cluster);
    let recorded = storage(&owners[0], &fs, "services").unwrap();
    assert_eq!(
        recorded.converting_attempt().unwrap().attempt_id,
        attempt_id
    );
    assert_eq!(
        migrations(&cluster, 0).remove("services"),
        Some(State::Converting {
            attempt_id,
            takeover_round: None
        })
    );
    drop(owners);
    cluster.native.close().unwrap();
}
