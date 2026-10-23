use super::*;

#[test]
fn native_ready_configuration_cut_survives_later_rebuild_without_granting_authority() {
    let (native, service, shard) = crate::test_support::current_native_metadata_service(
        "native-effect-open-ready-cut",
        1,
        "/services",
    )
    .unwrap();
    let node = shard.primary_node_id.clone();
    drop(service);
    let paths = ClusterPaths::new(native.data_dir(&node));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let runtime = native.runtime_handle();
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &paths,
        &state.cluster_id,
        &node,
        Some(&runtime),
    )
    .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let cut = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout_at(
            deadline.into(),
            authority.ready_write_configuration_projection_async(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    });
    let original_revision = cut.revision();
    let prefix = format!("{}/", crate::metadata_shards_prefix(&shard.file_system_id));
    assert!(
        cut.entries(&prefix)
            .unwrap()
            .iter()
            .any(|row| { row.decode::<crate::DfsMetadataShardConfig>().unwrap() == shard })
    );
    let paused = native.pause_next_configuration_rebuild(&node).unwrap();
    let (members, _) =
        message_host::read_native_dfs_peer_configuration(&paths, &state, deadline).unwrap();
    let (key, mut member) = members
        .into_iter()
        .map(|entry| {
            let member = entry.decode::<sinomo_core::ClusterMemberRecord>().unwrap();
            (entry.key, member)
        })
        .find(|(_, member)| member.node_id == node)
        .unwrap();
    member.display_name.push_str("-changed-after-ready");
    let control = native.shared_control_plane().unwrap();
    control.with_client(|kv| {
        kv.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(key, &member, None).unwrap()],
            failure: Vec::new(),
        })
        .unwrap()
    });
    paused
        .wait_rebuild(deadline.saturating_duration_since(Instant::now()))
        .unwrap();
    assert!(matches!(
        authority.write_configuration_readiness().unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    ));
    assert!(
        authority.write_configuration_projection().is_err(),
        "the old second read fails at the real post-readiness transition"
    );
    assert_eq!(cut.revision(), original_revision);
    assert!(
        cut.entries(&prefix)
            .unwrap()
            .iter()
            .any(|row| { row.decode::<crate::DfsMetadataShardConfig>().unwrap() == shard })
    );
    assert!(
        cut.entries(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
            .is_some()
    );
    // The retained read cut cannot satisfy a new readiness request. Join the
    // real paused owner; no replacement projection or successful reply is given.
    crate::test_runtime::executor().block_on(async {
        let mut next = std::pin::pin!(authority.ready_write_configuration_projection_async());
        let poll = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(next.as_mut(), cx))
        })
        .await;
        assert!(poll.is_pending());
        paused.release();
        let newer = tokio::time::timeout_at(deadline.into(), next)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(newer.revision() > original_revision);
    });
    drop(paused);
    let mut reopened = native.metadata_primary_service("/services").unwrap();
    reopened
        .posix_mkdir("/services/after-actual-ready")
        .unwrap();
    drop((reopened, authority, cut, control));
    native.close().unwrap();
    assert!(Instant::now() < deadline);
}
