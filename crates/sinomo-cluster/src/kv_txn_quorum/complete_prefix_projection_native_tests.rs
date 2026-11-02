// Real one-voter native owner; no remote acknowledgement is synthesized.
mod complete_prefix_projection_native_tests {
    use super::*;

    fn worker_counter(runtime: &tokio::runtime::Runtime) -> (String, u64) {
        runtime.block_on(async {
            runtime
                .spawn_blocking(|| {
                    (
                        format!("{:?}", std::thread::current().id()),
                        sinomo_kv::InProcessKvStore::complete_snapshot_materializations_for_test(),
                    )
                })
                .await
                .expect("sample actual projection worker")
        })
    }

    #[test]
    fn native_complete_prefix_keeps_real_no_effect_memo_without_full_materialization() {
        let root = sinomo_testdir::TestDir::new("native-complete-prefix-no-full-memo");
        let data = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data);
        paths.ensure().unwrap();
        let local = NodeId::generate();
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::generate(),
            "complete-prefix-native",
            local.clone(),
            "local",
            "public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        crate::write_cluster_state(&paths, &state).unwrap();
        let _resident = sinomo_kv::pin_resident_raft_node_registry_for_test(
            &paths.kv_paths().raft_wal_paths(&local),
        )
        .expect("pin only the actual native coordination slot");
        let append = |key: &str, value: Vec<u8>| {
            crate::append_cluster_raft_command(
                &paths,
                &local,
                KvCommand::Op(sinomo_kv::KvOp::Put {
                    key: key.into(),
                    value,
                    lease_id: None,
                }),
            )
            .expect("ordinary native one-voter commit")
        };
        // Publish the real one-voter membership through the same ordinary
        // native command producer before asking for a newer-state ReadIndex.
        // No caller state/header is substituted for the committed config row.
        let membership = append(
            &crate::keyspace::cluster_member_key(&local),
            sinomo_kv::encode_kv_value(&state.members[0]).unwrap(),
        );
        let config = sinomo_kv::ClusterKvRaftConfigRecord {
            cluster_id: state.cluster_id.clone(),
            term: state.term,
            kv_revision: membership.revision.0,
            voters: vec![local.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: state.updated_at,
        };
        let configured = append(
            &sinomo_kv::cluster_kv_raft_config_key(),
            sinomo_kv::encode_kv_value(&config).unwrap(),
        );
        let state = crate::read_cluster_consensus_state(&paths)
            .expect("derive actual current caller topology from committed native config");
        assert_eq!(state.local_node_id, local);
        assert_eq!(
            sinomo_kv::cluster_kv_raft_voter_ids(&state).unwrap(),
            vec![local.clone()]
        );
        // The topology view can trail native data; the actual ReadIndex below
        // must capture the current committed revision independently.
        std::fs::write(
            root.join("complete-prefix-membership-prerequisite.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "caller":state,"native_configured_revision":configured.revision,
                "committed_membership":config,
            }))
            .unwrap(),
        )
        .unwrap();
        let value = b"current exact prefix value".to_vec();
        append("/complete-scope/a", value.clone());
        append("/complete-scope/b", b"second exact prefix value".to_vec());
        let initial = append("/unrelated/payload", vec![0x31; 64 * 1024]);
        let mut committed = initial.clone();
        for _ in 0..64 {
            committed = append("/complete-scope/a", value.clone());
        }
        assert_eq!(committed.revision, initial.revision);
        assert!(committed.committed_requests.len() >= initial.committed_requests.len() + 64);
        let expected = committed.range_prefix("/complete-scope/").unwrap();
        assert_eq!(expected.len(), 2);

        // The existing counter is thread-local. One ordinary blocking worker
        // makes its scope explicit without a new counter/observer or authority.
        // This is a measurement fixture, not a throughput or timing control.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let calibration_paths = paths.kv_paths();
        let calibration_local = local.clone();
        let (worker, calibration_before, calibration_after) = runtime.block_on(async {
            runtime
                .spawn_blocking(move || {
                    let before =
                        sinomo_kv::InProcessKvStore::complete_snapshot_materializations_for_test();
                    let actual = sinomo_kv::read_cluster_kv_local_snapshot(
                        &calibration_paths,
                        &calibration_local,
                    )
                    .expect("calibrate against actual native full-snapshot read");
                    assert!(!actual.value.committed_requests.is_empty());
                    (
                        format!("{:?}", std::thread::current().id()),
                        before,
                        sinomo_kv::InProcessKvStore::complete_snapshot_materializations_for_test(),
                    )
                })
                .await
                .unwrap()
        });
        assert!(
            calibration_after > calibration_before,
            "counter must observe a real full native read"
        );
        let before = worker_counter(&runtime);
        assert_eq!(before.0, worker, "same actual measurement worker");
        let mut caller = state.clone();
        let mut host = ExactSetReadIndexHost::default();
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        let actual = runtime.block_on(
            read_cluster_kv_authoritative_complete_range_prefix_async_with_host(
                runtime.handle(),
                &paths,
                &mut caller,
                "/complete-scope/".into(),
                options,
                &mut host,
            ),
        );
        let after = worker_counter(&runtime);
        let unchanged = sinomo_kv::read_cluster_kv_local_snapshot(&paths.kv_paths(), &local)
            .expect("independent actual native cut after read")
            .value;
        std::fs::write(root.join("complete-prefix-native-observation.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "qualification":"native one-voter prefix read after64 real no-effect commits; same actual single blocking worker/TLS counter; not multi-voter/network/primary-loss proof",
                "actual":actual.as_ref().map_err(ToString::to_string),
                "worker_before":before.0,"worker_after":after.0,
                "materializations_before":before.1,"materializations_after":after.1,
                "calibration_before":calibration_before,"calibration_after":calibration_after,
                "native_before":committed,"native_after":unchanged,
                "remote_quorum_waves":host.quorum_waves,
            })).unwrap()).unwrap();
        let actual = actual.expect("actual complete prefix authority");
        assert_eq!(actual.value, expected);
        assert_eq!(actual.authority.revision, committed.revision);
        assert_eq!(caller.kv_revision, actual.authority.revision.0);
        assert_eq!(
            actual.authority.basis,
            sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1,
            }
        );
        assert_eq!(
            host.quorum_waves, 0,
            "no remote ACK was required or supplied"
        );
        assert_eq!(
            unchanged, committed,
            "prefix read changed native rows, memo or expiry"
        );
        assert_eq!(after.0, before.0, "counter worker must remain exact");
        assert_eq!(
            after.1 - before.1,
            0,
            "complete prefix must not materialize unrelated entries/memo"
        );
    }
}
