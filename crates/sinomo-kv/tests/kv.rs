include!("../src/root.rs");

use crate::internal::*;
use std::io::Write;

#[cfg(test)]
#[path = "kv/cluster_hardening.rs"]
mod cluster_hardening;
#[cfg(test)]
#[path = "kv/power_loss.rs"]
mod power_loss;

#[cfg(test)]
mod tests {
    use super::*;

    static OBJECT_FILE_WRITE_MARK_CACHE_TEST_MUTEX: std::sync::Mutex<()> =
        std::sync::Mutex::new(());
    static KV_READ_CACHE_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct TestRecord {
        name: String,
        count: u64,
    }

    #[test]
    fn cluster_kv_rpc_routes_group_storage_routes() {
        assert_eq!(CLUSTER_KV_RPC_CATEGORY, "cluster.kv");
        assert!(CLUSTER_KV_RPC_ROUTES.contains(&CLUSTER_KV_SNAPSHOT_ROUTE));
        assert!(CLUSTER_KV_RPC_ROUTES.contains(&CLUSTER_KV_TXN_ROUTE));
        for route in CLUSTER_KV_LOCAL_READ_WATCH_ROUTES
            .iter()
            .copied()
            .chain([CLUSTER_KV_WATCH_EVENTS_ROUTE])
        {
            assert!(CLUSTER_KV_RPC_ROUTES.contains(&route));
        }
        assert_eq!(
            CLUSTER_KV_RPC_ROUTE_METADATA.len(),
            CLUSTER_KV_RPC_ROUTES.len()
        );
        assert_eq!(
            CLUSTER_KV_RPC_ROUTE_POLICIES.len(),
            CLUSTER_KV_RPC_ROUTES.len()
        );
        for (index, route) in CLUSTER_KV_RPC_ROUTES.iter().copied().enumerate() {
            assert_eq!(
                CLUSTER_KV_RPC_ROUTE_METADATA[index],
                (route, CLUSTER_KV_RPC_CATEGORY)
            );
            let policy = CLUSTER_KV_RPC_ROUTE_POLICIES[index];
            assert_eq!(policy.route, route);
            assert_eq!(policy.category, CLUSTER_KV_RPC_CATEGORY);
            assert_eq!(cluster_kv_rpc_route_policy(route).copied(), Some(policy));
        }
        assert_eq!(
            cluster_kv_rpc_route_policy(CLUSTER_KV_SNAPSHOT_ROUTE)
                .expect("snapshot policy")
                .auth,
            ClusterKvRouteAuth::Authenticated
        );
        assert_eq!(
            cluster_kv_rpc_route_policy(CLUSTER_KV_TXN_ROUTE)
                .expect("txn policy")
                .required_capabilities,
            [CLUSTER_KV_WRITE_CAPABILITY]
        );
        assert_eq!(
            cluster_kv_rpc_route_policy(CLUSTER_KV_RANGE_PREFIX_ROUTE)
                .expect("range policy")
                .required_capabilities,
            [CLUSTER_KV_READ_CAPABILITY]
        );
        assert!(!CLUSTER_KV_LOCAL_READ_WATCH_ROUTES.contains(&CLUSTER_KV_TXN_ROUTE));
    }

    #[test]
    fn cluster_kv_raft_config_helpers_report_roles_and_voters() {
        let voter = NodeId::from_legacy_named("node_config_voter").expect("voter");
        let learner = NodeId::from_legacy_named("node_config_learner").expect("learner");
        let non_voting = NodeId::from_legacy_named("node_config_non_voting").expect("non-voting");
        let missing = NodeId::from_legacy_named("node_config_missing").expect("missing");
        let config = ClusterKvRaftConfigRecord {
            cluster_id: Id::from_legacy_named("cluster_config").expect("cluster"),
            term: 3,
            kv_revision: 9,
            voters: vec![voter.clone()],
            learners: vec![learner.clone()],
            non_voting: vec![non_voting.clone()],
            updated_at: OffsetDateTime::now_utc(),
        };

        assert_eq!(
            cluster_kv_raft_config_member_role(&config, &voter),
            Some(ClusterMemberRole::Voter)
        );
        assert_eq!(
            cluster_kv_raft_config_member_role(&config, &learner),
            Some(ClusterMemberRole::Learner)
        );
        assert_eq!(
            cluster_kv_raft_config_member_role(&config, &non_voting),
            Some(ClusterMemberRole::NonVoting)
        );
        assert_eq!(cluster_kv_raft_config_member_role(&config, &missing), None);

        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_kv_raft_config_key(), &config, None)
            .expect("store config");
        assert!(cluster_kv_store_member_is_raft_voter(&store, &voter).expect("voter check"));
        assert!(!cluster_kv_store_member_is_raft_voter(&store, &learner).expect("learner check"));
        assert!(!cluster_kv_store_member_is_raft_voter(&store, &missing).expect("missing check"));
    }

    #[test]
    fn cluster_kv_rpc_payloads_encode_storage_owned_bodies() {
        let snapshot_payload = cluster_kv_snapshot_rpc_payload().expect("snapshot rpc payload");
        assert_eq!(snapshot_payload.route, CLUSTER_KV_SNAPSHOT_ROUTE);
        assert_eq!(snapshot_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_snapshot_rpc_request_body(&snapshot_payload.body)
                .expect("snapshot request body"),
            ()
        );

        let txn = KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key: "/rpc/payload".to_string(),
                value: b"value".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        };
        let txn_payload = cluster_kv_txn_rpc_payload(&txn).expect("txn rpc payload");
        assert_eq!(txn_payload.route, CLUSTER_KV_TXN_ROUTE);
        assert_eq!(txn_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_txn_rpc_request_body(&txn_payload.body).expect("txn request body"),
            txn
        );

        let snapshot = KvSnapshot {
            revision: KvRevision(7),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
        };
        let snapshot_response_payload =
            cluster_kv_snapshot_response_rpc_payload(&snapshot).expect("snapshot response payload");
        assert_eq!(snapshot_response_payload.route, CLUSTER_KV_SNAPSHOT_ROUTE);
        assert_eq!(snapshot_response_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_snapshot_rpc_body(&snapshot_response_payload.body)
                .expect("snapshot response body"),
            snapshot
        );

        let txn_response = KvTxnResponse {
            succeeded: true,
            revision: KvRevision(9),
            responses: Vec::new(),
        };
        let txn_response_payload =
            cluster_kv_txn_response_rpc_payload(&txn_response).expect("txn response payload");
        assert_eq!(txn_response_payload.route, CLUSTER_KV_TXN_ROUTE);
        assert_eq!(txn_response_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_txn_rpc_body(&txn_response_payload.body).expect("txn response body"),
            txn_response
        );

        let range_payload =
            cluster_kv_range_prefix_rpc_payload("/rpc/").expect("range prefix payload");
        assert_eq!(range_payload.route, CLUSTER_KV_RANGE_PREFIX_ROUTE);
        assert_eq!(range_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_rpc_body::<String>(&range_payload.body).expect("range prefix body"),
            "/rpc/"
        );
    }

    #[test]
    fn cluster_raft_rpc_payloads_encode_kv_owned_bodies() {
        let node_id = NodeId::from_legacy_named("node_kv_raft_rpc_payloads").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            sinomo_core::Id::from_legacy_named("cluster_kv_raft_rpc_payloads").expect("cluster"),
            "kv raft rpc payloads",
            node_id.clone(),
            "node",
            "pk",
            OffsetDateTime::now_utc(),
        );
        let append_request = ClusterRaftAppendRequest {
            state: state.clone(),
            append: RaftAppendEntriesRequest {
                term: 2,
                leader_id: node_id.clone(),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: Vec::new(),
                leader_commit: 1,
            },
            snapshot: None,
        };
        let append_payload =
            cluster_raft_append_rpc_payload(&append_request).expect("append payload");
        assert_eq!(append_payload.route, CLUSTER_RAFT_APPEND_ROUTE);
        assert_eq!(append_payload.category, CLUSTER_RAFT_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_raft_append_rpc_request_body(&append_payload.body)
                .expect("append request body"),
            append_request
        );
        let append_response = ClusterRaftAppendResponse {
            accepted: true,
            detail: "accepted".to_string(),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 2,
                success: true,
                match_index: 1,
            },
        };
        let append_response_payload = cluster_raft_append_response_rpc_payload(&append_response)
            .expect("append response payload");
        assert_eq!(
            decode_cluster_raft_append_rpc_response_body(&append_response_payload.body)
                .expect("append response body"),
            append_response
        );

        let vote_request = ClusterRaftVoteRequest {
            state: state.clone(),
            vote: RaftRequestVoteRequest {
                term: 3,
                candidate_id: node_id,
                last_log_index: 1,
                last_log_term: 2,
            },
        };
        let vote_payload = cluster_raft_vote_rpc_payload(&vote_request).expect("vote payload");
        assert_eq!(vote_payload.route, CLUSTER_RAFT_VOTE_ROUTE);
        assert_eq!(vote_payload.category, CLUSTER_RAFT_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_raft_vote_rpc_request_body(&vote_payload.body)
                .expect("vote request body"),
            vote_request
        );
        let vote_response = ClusterRaftVoteResponse {
            accepted: true,
            detail: "granted".to_string(),
            state,
            vote: RaftRequestVoteResponse {
                term: 3,
                vote_granted: true,
            },
        };
        let vote_response_payload =
            cluster_raft_vote_response_rpc_payload(&vote_response).expect("vote response payload");
        assert_eq!(
            decode_cluster_raft_vote_rpc_response_body(&vote_response_payload.body)
                .expect("vote response body"),
            vote_response
        );
        assert_eq!(
            CLUSTER_RAFT_RPC_ROUTE_METADATA,
            [
                (CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
                (CLUSTER_RAFT_VOTE_ROUTE, CLUSTER_RAFT_RPC_CATEGORY)
            ]
        );
        assert_eq!(
            CLUSTER_RAFT_RPC_ROUTES,
            [CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_VOTE_ROUTE]
        );
        assert_eq!(
            CLUSTER_RAFT_RPC_ROUTE_POLICIES,
            [
                ClusterKvRpcRoutePolicy {
                    route: CLUSTER_RAFT_APPEND_ROUTE,
                    category: CLUSTER_RAFT_RPC_CATEGORY,
                    auth: ClusterKvRouteAuth::LocalOrRaftVoter,
                    required_capabilities: &[CLUSTER_RAFT_APPEND_CAPABILITY],
                },
                ClusterKvRpcRoutePolicy {
                    route: CLUSTER_RAFT_VOTE_ROUTE,
                    category: CLUSTER_RAFT_RPC_CATEGORY,
                    auth: ClusterKvRouteAuth::LocalOrRaftVoter,
                    required_capabilities: &[CLUSTER_RAFT_VOTE_CAPABILITY],
                }
            ]
        );
        assert_eq!(
            cluster_kv_rpc_route_policy(CLUSTER_RAFT_APPEND_ROUTE)
                .expect("raft append policy")
                .auth,
            ClusterKvRouteAuth::LocalOrRaftVoter
        );

        let dispatched_append = handle_cluster_raft_rpc_payload::<_, _, KvError>(
            append_payload.route,
            &append_payload.body,
            |request| {
                assert_eq!(request, append_request);
                Ok(append_response.clone())
            },
            |_| -> Result<ClusterRaftVoteResponse, KvError> {
                panic!("append route must not call vote handler")
            },
        )
        .expect("dispatch append")
        .expect("append route");
        assert_eq!(dispatched_append.route, CLUSTER_RAFT_APPEND_ROUTE);
        assert_eq!(
            decode_cluster_raft_append_rpc_response_body(&dispatched_append.body)
                .expect("dispatched append response"),
            append_response
        );

        let dispatched_vote = handle_cluster_raft_rpc_payload::<_, _, KvError>(
            vote_payload.route,
            &vote_payload.body,
            |_| -> Result<ClusterRaftAppendResponse, KvError> {
                panic!("vote route must not call append handler")
            },
            |request| {
                assert_eq!(request, vote_request);
                Ok(vote_response.clone())
            },
        )
        .expect("dispatch vote")
        .expect("vote route");
        assert_eq!(dispatched_vote.route, CLUSTER_RAFT_VOTE_ROUTE);
        assert_eq!(
            decode_cluster_raft_vote_rpc_response_body(&dispatched_vote.body)
                .expect("dispatched vote response"),
            vote_response
        );

        let unknown = handle_cluster_raft_rpc_payload::<_, _, KvError>(
            "/cluster/raft/unknown",
            &[],
            |_| -> Result<ClusterRaftAppendResponse, KvError> {
                panic!("unknown route must not call append handler")
            },
            |_| -> Result<ClusterRaftVoteResponse, KvError> {
                panic!("unknown route must not call vote handler")
            },
        )
        .expect("unknown route");
        assert!(unknown.is_none());
    }

    #[derive(Debug, Default)]
    struct FixedLocalReadWatchHost {
        seen_range_prefix: Option<String>,
        created_watch: Option<ClusterKvWatchPrefixRequest>,
        drained_watch: Option<ClusterKvWatchDrainRequest>,
        cancelled_watch: Option<ClusterKvWatchCancelRequest>,
    }

    impl ClusterKvLocalReadWatchHost for FixedLocalReadWatchHost {
        type Error = KvError;

        fn range_prefix(&mut self, prefix: &str) -> Result<Vec<KvEntry>, Self::Error> {
            self.seen_range_prefix = Some(prefix.to_string());
            Ok(vec![KvEntry {
                key: format!("{prefix}/value"),
                value: b"value".to_vec(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: KvVersion(1),
                lease_id: None,
            }])
        }

        fn create_watch(
            &mut self,
            request: ClusterKvWatchPrefixRequest,
        ) -> Result<ClusterKvWatchRegistration, Self::Error> {
            self.created_watch = Some(request.clone());
            Ok(ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: request.prefix,
                next_revision: request.start_revision,
            })
        }

        fn drain_watch(
            &mut self,
            request: ClusterKvWatchDrainRequest,
        ) -> Result<ClusterKvWatchDrainResponse, Self::Error> {
            self.drained_watch = Some(request.clone());
            Ok(ClusterKvWatchDrainResponse {
                watch_id: request.watch_id,
                events: Vec::new(),
                next_revision: KvRevision(11),
            })
        }

        fn cancel_watch(
            &mut self,
            request: ClusterKvWatchCancelRequest,
        ) -> Result<ClusterKvWatchCancelResponse, Self::Error> {
            self.cancelled_watch = Some(request.clone());
            Ok(ClusterKvWatchCancelResponse {
                watch_id: request.watch_id,
                cancelled: true,
            })
        }
    }

    #[test]
    fn cluster_kv_local_read_watch_rpc_dispatch_is_storage_owned() {
        let mut host = FixedLocalReadWatchHost::default();

        let range_body =
            encode_cluster_kv_rpc_body(&"/svc".to_string()).expect("range request body");
        let range_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_RANGE_PREFIX_ROUTE,
            &range_body,
            &mut host,
        )
        .expect("range dispatch")
        .expect("range route");
        assert_eq!(range_payload.route, CLUSTER_KV_RANGE_PREFIX_ROUTE);
        assert_eq!(range_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(host.seen_range_prefix.as_deref(), Some("/svc"));
        assert_eq!(
            decode_cluster_kv_rpc_body::<Vec<KvEntry>>(&range_payload.body)
                .expect("range response body")
                .first()
                .map(|entry| entry.key.as_str()),
            Some("/svc/value")
        );

        let watch_request = ClusterKvWatchPrefixRequest {
            prefix: "/svc".to_string(),
            start_revision: KvRevision(3),
        };
        let watch_body = encode_cluster_kv_rpc_body(&watch_request).expect("watch request body");
        let watch_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_WATCH_PREFIX_ROUTE,
            &watch_body,
            &mut host,
        )
        .expect("watch dispatch")
        .expect("watch route");
        assert_eq!(watch_payload.route, CLUSTER_KV_WATCH_PREFIX_ROUTE);
        assert_eq!(host.created_watch, Some(watch_request));
        assert_eq!(
            decode_cluster_kv_rpc_body::<ClusterKvWatchRegistration>(&watch_payload.body)
                .expect("watch response body")
                .watch_id,
            WatchId(7)
        );

        let drain_request = ClusterKvWatchDrainRequest {
            watch_id: WatchId(7),
            wait_timeout_millis: 5,
        };
        let drain_body = encode_cluster_kv_rpc_body(&drain_request).expect("drain request body");
        let drain_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_WATCH_DRAIN_ROUTE,
            &drain_body,
            &mut host,
        )
        .expect("drain dispatch")
        .expect("drain route");
        assert_eq!(drain_payload.route, CLUSTER_KV_WATCH_DRAIN_ROUTE);
        assert_eq!(host.drained_watch, Some(drain_request));
        assert_eq!(
            decode_cluster_kv_rpc_body::<ClusterKvWatchDrainResponse>(&drain_payload.body)
                .expect("drain response body")
                .next_revision,
            KvRevision(11)
        );

        let cancel_request = ClusterKvWatchCancelRequest {
            watch_id: WatchId(7),
        };
        let cancel_body = encode_cluster_kv_rpc_body(&cancel_request).expect("cancel request body");
        let cancel_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_WATCH_CANCEL_ROUTE,
            &cancel_body,
            &mut host,
        )
        .expect("cancel dispatch")
        .expect("cancel route");
        assert_eq!(cancel_payload.route, CLUSTER_KV_WATCH_CANCEL_ROUTE);
        assert_eq!(host.cancelled_watch, Some(cancel_request));
        assert!(
            decode_cluster_kv_rpc_body::<ClusterKvWatchCancelResponse>(&cancel_payload.body)
                .expect("cancel response body")
                .cancelled
        );

        assert!(
            handle_cluster_kv_local_read_watch_rpc_payload(
                CLUSTER_KV_WATCH_EVENTS_ROUTE,
                &[],
                &mut host,
            )
            .expect("watch events stays external")
            .is_none()
        );
    }

    #[test]
    fn cluster_kv_paths_define_cluster_storage_layout() {
        let root = PathBuf::from("/sinomo-cluster");
        let paths = ClusterKvPaths::new(&root);

        assert_eq!(paths.snapshot, root.join("kv-snapshot.sobj"));
        assert_eq!(paths.raft, root.join("kv-raft"));
        assert_eq!(paths.watches, root.join("watches"));
        assert_eq!(paths.watches_lock, root.join("watches.lock"));
        assert_eq!(paths.watches_next_id, root.join("watches-next.sobj"));
        assert_eq!(paths.watch_events, root.join("watch-events.sobj"));
        assert_eq!(paths.watch_events_log, root.join("watch-events.sobjlog"));
        let node_id = NodeId::from_legacy_named("node_path_layout").expect("node");
        assert_eq!(
            paths.raft_wal_paths(&node_id).root,
            root.join("kv-raft").join(node_id.as_str())
        );
        assert_eq!(
            paths.watch_path(WatchId(7)),
            root.join("watches/watch-7.sobj")
        );
    }

    #[test]
    fn cluster_kv_paths_prefix_read_uses_materialized_snapshot_limits() {
        let root = temp_raft_root("sinomo-kv-cluster-prefix-read");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let cache = paths.local_cache();
        let mut store = InProcessKvStore::new();
        for (key, value) in [
            ("/apps/a", b"one".to_vec()),
            ("/apps/b", b"two".to_vec()),
            ("/other/c", b"three".to_vec()),
        ] {
            store.put(key, value, None).expect("put");
        }
        cache
            .write_snapshot(&store.snapshot().expect("snapshot"))
            .expect("write materialized snapshot");

        let entries = read_cluster_kv_prefix_entries(&paths, "/apps/").expect("read prefix");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/a", "/apps/b"]
        );

        let error = read_cluster_kv_prefix_entries_limited(&paths, "/apps/", 1)
            .expect_err("prefix entry limit");
        assert_eq!(
            error,
            KvError::RangeLimit {
                prefix: "/apps/".to_string(),
                max_entries: 1,
            }
        );
    }

    #[test]
    fn cluster_kv_paths_read_missing_materialized_snapshot_as_empty() {
        let root = temp_raft_root("sinomo-kv-cluster-missing-snapshot-read");
        let paths = ClusterKvPaths::new(root.to_path_buf());

        assert_eq!(
            read_cluster_kv_entry(&paths, "/missing").expect("read missing entry"),
            None
        );
        assert_eq!(
            read_cluster_kv_prefix_entries(&paths, "/missing/").expect("read missing prefix"),
            Vec::new()
        );
    }

    #[test]
    fn cluster_kv_watch_drain_events_filter_sort_and_dedupe() {
        let duplicate = test_kv_put_event(5, "/svc/b", b"b");
        let persisted_events = vec![
            test_kv_put_event(3, "/svc/c", b"c"),
            test_kv_put_event(2, "/svc/old", b"old"),
            test_kv_put_event(4, "/other/a", b"other"),
            duplicate.clone(),
        ];
        let snapshot = test_kv_snapshot_with_events(vec![
            test_kv_put_event(4, "/svc/a", b"a"),
            duplicate,
            test_kv_delete_event(6, "/svc/d"),
        ]);
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(9),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };

        let events = collect_cluster_kv_watch_drain_events(
            &persisted_events,
            &snapshot,
            &registration,
            usize::MAX,
        )
        .expect("collect watch drain events");

        assert_eq!(
            events
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(3, "/svc/c"), (4, "/svc/a"), (5, "/svc/b"), (6, "/svc/d")]
        );
    }

    #[test]
    fn cluster_kv_watch_drain_payload_limit_keeps_revision_groups() {
        let events = vec![
            test_kv_put_event(7, "/svc/a", b"a"),
            test_kv_put_event(7, "/svc/b", b"larger-than-limit"),
            test_kv_put_event(8, "/svc/c", b"c"),
        ];
        let snapshot = test_kv_snapshot_with_events(Vec::new());
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(10),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(7),
        };
        let max_payload_bytes = cluster_kv_watch_event_payload_bytes(&events[0]);

        let drained = collect_cluster_kv_watch_drain_events(
            &events,
            &snapshot,
            &registration,
            max_payload_bytes,
        )
        .expect("collect watch drain events");

        assert_eq!(
            drained
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(7, "/svc/a"), (7, "/svc/b")]
        );
    }

    #[test]
    fn cluster_kv_watch_registration_cursor_persist_policy_uses_storage_thresholds() {
        assert!(!cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            true,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS,
            KvRevision(2),
            KvRevision(1),
            true,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG + 1),
            KvRevision(1),
            true,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            false,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            true,
            None,
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            true,
            Some(CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE),
        ));
    }

    #[test]
    fn cluster_kv_materialized_snapshot_deferral_is_key_predicate_driven() {
        let events = vec![
            test_kv_put_event(1, "/deferred/a", b"a"),
            test_kv_delete_event(2, "/deferred/b"),
        ];

        assert!(cluster_kv_events_can_defer_materialized_snapshot(
            &events,
            |key| key.starts_with("/deferred/"),
        ));

        let mixed_events = vec![
            test_kv_put_event(1, "/deferred/a", b"a"),
            test_kv_put_event(2, "/durable/a", b"a"),
        ];
        assert!(!cluster_kv_events_can_defer_materialized_snapshot(
            &mixed_events,
            |key| key.starts_with("/deferred/"),
        ));
        assert!(!cluster_kv_events_can_defer_materialized_snapshot(
            &[],
            |key| key.starts_with("/deferred/"),
        ));
    }

    #[test]
    fn cluster_kv_materialized_snapshot_default_defer_policy_covers_high_churn_status_keys() {
        assert!(cluster_kv_key_can_defer_materialized_snapshot(
            "/cluster/network/connectivity/node-a/node-b"
        ));
        assert!(cluster_kv_key_can_defer_materialized_snapshot(
            "/cluster/network/info/node-a"
        ));
        assert!(cluster_kv_key_can_defer_materialized_snapshot(
            "/cluster/node-status/node-a"
        ));
        assert!(!cluster_kv_key_can_defer_materialized_snapshot(
            "/cluster/members/node-a"
        ));
        assert!(!cluster_kv_key_can_defer_materialized_snapshot(
            "/config/models/default"
        ));
    }

    #[test]
    fn cluster_kv_snapshot_delta_defers_only_predicate_matched_changes() {
        let entry = |key: &str, value: &[u8]| KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let snapshot = |revision: u64, entries: Vec<KvEntry>| KvSnapshot {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: Vec::new(),
            events: Vec::new(),
        };
        let key_can_defer = |key: &str| key.starts_with("/deferred/");

        let existing = snapshot(
            1,
            vec![entry("/deferred/a", b"old"), entry("/durable/a", b"same")],
        );
        let deferable_change = snapshot(
            2,
            vec![entry("/deferred/a", b"new"), entry("/durable/a", b"same")],
        );
        let durable_change = snapshot(
            3,
            vec![entry("/deferred/a", b"old"), entry("/durable/a", b"new")],
        );

        assert!(cluster_kv_snapshot_delta_only_deferable(
            &existing,
            &deferable_change,
            key_can_defer,
        ));
        assert!(!cluster_kv_snapshot_delta_only_deferable(
            &existing,
            &durable_change,
            key_can_defer,
        ));
        assert!(!cluster_kv_snapshot_delta_only_deferable(
            &existing,
            &existing,
            key_can_defer,
        ));
    }

    #[test]
    fn imported_cluster_kv_snapshot_defers_only_predicate_matched_materialized_delta() {
        let root = temp_raft_root("sinomo-kv-imported-snapshot-deferral");
        let paths = ClusterKvPaths::new(&root);
        let key_can_defer = |key: &str| key.starts_with("/deferred/");

        let mut store = InProcessKvStore::new();
        store
            .put("/durable/a", b"same".to_vec(), None)
            .expect("write durable seed");
        store
            .put("/deferred/a", b"old".to_vec(), None)
            .expect("write deferred seed");
        write_cluster_kv_snapshot(&paths, &store.snapshot().expect("snapshot"))
            .expect("write initial snapshot");

        let mut imported_store =
            InProcessKvStore::restore(store.snapshot().expect("snapshot")).expect("restore");
        imported_store
            .put("/deferred/a", b"new".to_vec(), None)
            .expect("write deferred import");
        let wrote = write_imported_cluster_kv_snapshot_if_changed(
            &paths,
            &imported_store.snapshot().expect("snapshot"),
            key_can_defer,
        )
        .expect("write imported deferred snapshot");
        assert!(!wrote);
        let cached = read_cluster_kv_cached_snapshot(&paths).expect("read cached snapshot");
        assert_eq!(
            cached
                .get("/deferred/a")
                .expect("deferred entry")
                .value
                .as_slice(),
            b"old"
        );

        let mut durable_store = InProcessKvStore::restore(cached).expect("restore");
        durable_store
            .put("/durable/a", b"new".to_vec(), None)
            .expect("write durable import");
        let wrote = write_imported_cluster_kv_snapshot_if_changed(
            &paths,
            &durable_store.snapshot().expect("snapshot"),
            key_can_defer,
        )
        .expect("write imported durable snapshot");
        assert!(wrote);
        let cached = read_cluster_kv_cached_snapshot(&paths).expect("read updated snapshot");
        assert_eq!(
            cached
                .get("/durable/a")
                .expect("durable entry")
                .value
                .as_slice(),
            b"new"
        );
    }

    #[test]
    fn cluster_kv_raft_publish_helpers_materialize_snapshot_in_kv() {
        let root = temp_raft_root("sinomo-kv-raft-publish-commit");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let mut kv = InProcessKvStore::new();
        kv.put("/kv/a", b"a".to_vec(), None)
            .expect("write commit entry");
        let snapshot = kv.snapshot().expect("snapshot");
        let result = ClusterKvRaftCommitResult {
            snapshot: snapshot.clone(),
            txn_response: None,
            events: snapshot.events.clone(),
        };

        let wrote = store
            .publish_commit_result(&result, |_| false)
            .expect("publish commit result");
        assert!(wrote);
        let wrote = store
            .publish_commit_result(&result, |_| false)
            .expect("publish unchanged commit result");
        assert!(!wrote);
        let cached = read_cluster_kv_cached_snapshot(&paths).expect("read published snapshot");
        assert_eq!(
            cached
                .get("/kv/a")
                .expect("published entry")
                .value
                .as_slice(),
            b"a"
        );

        let restored_root = temp_raft_root("sinomo-kv-raft-publish-restored");
        let restored_paths = ClusterKvPaths::new(&restored_root);
        let restored_store = restored_paths.raft_store();
        let outcome = RaftAppendEntriesApplyOutcome {
            response: RaftAppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 1,
            },
            snapshot: snapshot.clone(),
            snapshot_restored: true,
            committed_events: Vec::new(),
        };
        let wrote = restored_store
            .publish_append_entries_apply_outcome(&outcome, |_| true)
            .expect("publish restored append outcome");
        assert!(wrote);
        let cached =
            read_cluster_kv_cached_snapshot(&restored_paths).expect("read restored snapshot");
        assert_eq!(
            cached
                .get("/kv/a")
                .expect("restored entry")
                .value
                .as_slice(),
            b"a"
        );
    }

    #[test]
    fn cluster_kv_materialized_snapshot_freshness_uses_storage_thresholds() {
        assert!(cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(10),
            KvRevision(11),
            Some(Duration::from_secs(1)),
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(12),
            KvRevision(11),
            Some(Duration::from_secs(1)),
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(1),
            KvRevision(1 + CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG),
            Some(Duration::from_secs(1)),
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(10),
            KvRevision(11),
            None,
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(10),
            KvRevision(11),
            Some(CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL),
        ));
    }

    #[test]
    fn cluster_kv_event_containment_and_snapshot_compare_are_storage_semantics() {
        let retained = vec![
            test_kv_put_event(1, "/kv/a", b"a"),
            test_kv_delete_event(2, "/kv/b"),
        ];
        assert!(cluster_kv_events_contain_all(
            &retained,
            &[test_kv_put_event(1, "/kv/a", b"a")],
        ));
        assert!(!cluster_kv_events_contain_all(
            &retained,
            &[test_kv_put_event(1, "/kv/a", b"changed")],
        ));

        let cluster_id = Id::from_legacy_named("cluster_kv_watch_fanout_needed").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_kv_watch_fanout_needed_leader").expect("leader");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "kv watch fanout needed",
            leader_id.clone(),
            "leader",
            "pk-leader",
            OffsetDateTime::now_utc(),
        );
        let first = test_kv_put_event(1, "/kv/watch/fanout/first", b"first");
        let second = test_kv_put_event(2, "/kv/watch/fanout/second", b"second");
        let append = RaftAppendEntriesRequest {
            term: 1,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 1,
        };
        let mut request = ClusterRaftAppendRequest {
            state,
            append,
            snapshot: None,
        };
        assert!(!cluster_raft_append_request_needs_watch_event_fanout(
            &request,
            std::slice::from_ref(&first),
        ));
        let mut fanout_snapshot = InProcessKvStore::new().snapshot().expect("snapshot");
        fanout_snapshot.events = vec![first.clone()];
        request.snapshot = Some(RaftSnapshot {
            last_included_index: 1,
            last_included_term: 1,
            kv: fanout_snapshot,
        });
        assert!(!cluster_raft_append_request_needs_watch_event_fanout(
            &request,
            std::slice::from_ref(&first),
        ));
        assert!(cluster_raft_append_request_needs_watch_event_fanout(
            &request,
            &[first, second],
        ));

        let snapshot = KvSnapshot {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            entries: vec![KvEntry {
                key: "/kv/a".to_string(),
                value: b"a".to_vec(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(3),
                version: KvVersion(2),
                lease_id: Some(LeaseId(7)),
            }],
            leases: Vec::new(),
            events: Vec::new(),
        };
        assert!(cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::Exists {
                key: "/kv/a".to_string(),
            },
        ));
        assert!(cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::ValueEquals {
                key: "/kv/a".to_string(),
                value: b"a".to_vec(),
            },
        ));
        assert!(cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::LeaseEquals {
                key: "/kv/a".to_string(),
                lease_id: Some(LeaseId(7)),
            },
        ));
        assert!(!cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::ModifyRevisionEquals {
                key: "/kv/a".to_string(),
                revision: KvRevision(2),
            },
        ));
    }

    #[test]
    fn cluster_kv_watch_event_payload_bytes_count_keys_and_put_values() {
        let put = test_kv_put_event(1, "/svc/value", b"payload");
        let delete = test_kv_delete_event(2, "/svc/deleted");

        assert_eq!(
            cluster_kv_watch_event_payload_bytes(&put),
            "/svc/value".len() + "payload".len()
        );
        assert_eq!(
            cluster_kv_watch_event_payload_bytes(&delete),
            "/svc/deleted".len()
        );
        assert_eq!(
            cluster_kv_watch_events_payload_bytes(&[put, delete]),
            "/svc/value".len() + "payload".len() + "/svc/deleted".len()
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_batch_validation_enforces_storage_limits() {
        let events = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_put_event(2, "/svc/b", b"larger"),
            test_kv_delete_event(3, "/svc/c"),
        ];

        assert_eq!(
            validate_cluster_kv_watch_event_fanout_batch_with_limits(&events, 2, usize::MAX),
            Err(ClusterKvWatchEventFanoutBatchLimit::EventCount {
                observed_events: 3,
                max_events: 2,
            })
        );

        let payload_bytes = cluster_kv_watch_events_payload_bytes(&events);
        assert_eq!(
            validate_cluster_kv_watch_event_fanout_batch_with_limits(
                &events,
                events.len(),
                payload_bytes - 1,
            ),
            Err(ClusterKvWatchEventFanoutBatchLimit::PayloadBytes {
                observed_bytes: payload_bytes,
                max_bytes: payload_bytes - 1,
            })
        );

        assert!(
            validate_cluster_kv_watch_event_fanout_batch_with_limits(
                &events,
                events.len(),
                payload_bytes,
            )
            .is_ok()
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_batches_are_storage_limited() {
        let events = (0..5)
            .map(|index| test_kv_put_event(index + 1, &format!("/svc/{index}"), b"payload"))
            .collect::<Vec<_>>();

        let count_limited =
            cluster_kv_watch_event_fanout_batches_with_limits(&events, 2, usize::MAX);
        assert_eq!(
            count_limited
                .iter()
                .map(|batch| batch.len())
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        let payload_limited =
            cluster_kv_watch_event_fanout_batches_with_limits(&events, events.len(), 20);
        assert_eq!(
            payload_limited
                .iter()
                .map(|batch| batch.len())
                .collect::<Vec<_>>(),
            vec![1, 1, 1, 1, 1]
        );
        assert_eq!(
            payload_limited
                .iter()
                .flat_map(|batch| batch.iter().map(|event| event.revision))
                .collect::<Vec<_>>(),
            events
                .iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cluster_kv_watch_event_compaction_sorts_dedupes_and_bounds_retained_events() {
        let duplicate = test_kv_put_event(3, "/svc/b", b"b");
        let compacted = compact_cluster_kv_watch_events(vec![
            duplicate.clone(),
            test_kv_put_event(1, "/svc/a", b"a"),
            duplicate,
            test_kv_delete_event(2, "/svc/deleted"),
        ]);

        assert_eq!(
            compacted
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "/svc/a"), (2, "/svc/deleted"), (3, "/svc/b")]
        );

        let bounded = (0..(MAX_CLUSTER_KV_WATCH_EVENTS + 4))
            .map(|index| test_kv_put_event(index as u64 + 1, &format!("/svc/{index}"), b"value"))
            .collect::<Vec<_>>();
        let compacted = compact_cluster_kv_watch_events(bounded);
        assert_eq!(compacted.len(), MAX_CLUSTER_KV_WATCH_EVENTS);
        assert_eq!(
            compacted.first().expect("first retained").revision,
            KvRevision(5)
        );
    }

    #[test]
    fn cluster_kv_watch_event_compaction_keeps_cutoff_revision_group() {
        let mut events = vec![test_kv_put_event(1, "/svc/old", b"old")];
        for index in 0..5 {
            events.push(test_kv_put_event(
                2,
                &format!("/svc/group/{index}"),
                b"group",
            ));
        }
        for index in 0..(MAX_CLUSTER_KV_WATCH_EVENTS - 2) {
            events.push(test_kv_put_event(
                index as u64 + 3,
                &format!("/svc/new/{index}"),
                b"new",
            ));
        }

        let compacted = compact_cluster_kv_watch_events(events);
        assert_eq!(compacted.len(), MAX_CLUSTER_KV_WATCH_EVENTS + 3);
        assert_eq!(
            compacted.first().expect("first retained").revision,
            KvRevision(2)
        );
        assert!(
            compacted
                .iter()
                .take(5)
                .all(|event| event.revision == KvRevision(2))
        );
    }

    #[test]
    fn cluster_kv_watch_event_incremental_compaction_skips_duplicates() {
        let first = test_kv_put_event(1, "/svc/a", b"a");
        let second = test_kv_put_event(2, "/svc/b", b"b");

        let retained = compact_cluster_kv_watch_events_incremental(
            vec![first.clone()],
            &[first.clone(), second.clone()],
        );

        assert_eq!(retained, vec![first.clone(), second.clone()]);
        assert!(cluster_kv_events_contain(&retained, &first));
        assert!(cluster_kv_events_contain(&retained, &second));
    }

    #[test]
    fn cluster_kv_watch_event_append_checkpoint_policy_uses_storage_limits() {
        assert!(!cluster_kv_watch_event_append_should_checkpoint(
            1, 1, 0, 0, 1
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            MAX_CLUSTER_KV_WATCH_EVENTS + 1,
            1,
            0,
            0,
            1,
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            1,
            1,
            MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS - 1,
            0,
            1,
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            1,
            1,
            0,
            MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES - 1,
            1,
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            1,
            1,
            usize::MAX,
            u64::MAX,
            u64::MAX,
        ));
    }

    #[test]
    fn cluster_kv_watch_event_log_open_for_append_creates_and_reopens_log() {
        let root = temp_raft_root("sinomo-kv-watch-event-log-open");
        let paths = ClusterKvPaths::new(root.to_path_buf());

        let first = open_cluster_kv_watch_event_log_for_append(&paths).expect("open new log");
        lock_cluster_kv_watch_event_log_for_append(&paths, &first).expect("lock new log");
        assert!(paths.watch_events_log.exists());
        first.unlock().expect("unlock new log");
        drop(first);

        let mut second =
            open_cluster_kv_watch_event_log_for_append(&paths).expect("reopen existing log");
        lock_cluster_kv_watch_event_log_for_append(&paths, &second).expect("lock reopened log");
        let batch =
            encode_cluster_kv_watch_event_log_batch(&[test_kv_put_event(1, "/svc/a", b"a")])
                .expect("encode log");
        append_cluster_kv_watch_event_log_batch_to_file(&paths, &mut second, &batch)
            .expect("append reopened log");
        second.unlock().expect("unlock reopened log");
        drop(second);

        let read = read_cluster_kv_watch_event_log(&paths).expect("read log");
        assert_eq!(read.records, 1);
    }

    #[test]
    fn cluster_kv_watch_event_log_batch_encodes_records_and_bytes() {
        let events = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_delete_event(2, "/svc/b"),
        ];

        let batch = encode_cluster_kv_watch_event_log_batch(&events).expect("encode batch");

        assert_eq!(batch.records.len(), events.len());
        assert_eq!(
            batch.bytes,
            batch
                .records
                .iter()
                .map(|record| 4_u64 + record.len() as u64)
                .sum::<u64>()
        );
        let decoded = batch
            .records
            .iter()
            .map(|record| decode_cluster_kv_watch_event_log_record(record))
            .collect::<Result<Vec<_>, _>>()
            .expect("decode records");
        assert_eq!(decoded, events);
    }

    #[test]
    fn cluster_kv_watch_event_log_batch_appends_to_log_file() {
        let root = temp_raft_root("sinomo-kv-watch-event-log-append");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let parent = paths.watch_events_log.parent().expect("log parent");
        std::fs::create_dir_all(parent).expect("create log parent");
        let mut log_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&paths.watch_events_log)
            .expect("open log");
        let events = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_delete_event(2, "/svc/b"),
        ];
        let batch = encode_cluster_kv_watch_event_log_batch(&events).expect("encode log");

        assert_eq!(
            append_cluster_kv_watch_event_log_batch_to_file(&paths, &mut log_file, &batch)
                .expect("append log"),
            batch.bytes
        );
        drop(log_file);

        let read = read_cluster_kv_watch_event_log(&paths).expect("read log");
        assert_eq!(read.events, events);
        assert_eq!(read.records, 2);
    }

    #[test]
    fn cluster_kv_watch_event_log_truncates_log_file() {
        let root = temp_raft_root("sinomo-kv-watch-event-log-truncate");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let parent = paths.watch_events_log.parent().expect("log parent");
        std::fs::create_dir_all(parent).expect("create log parent");
        let mut log_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&paths.watch_events_log)
            .expect("open log");
        let batch = encode_cluster_kv_watch_event_log_batch(&[
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_delete_event(2, "/svc/b"),
        ])
        .expect("encode log");
        append_cluster_kv_watch_event_log_batch_to_file(&paths, &mut log_file, &batch)
            .expect("append log");

        truncate_cluster_kv_watch_event_log_file(&paths, &mut log_file).expect("truncate log");
        drop(log_file);

        let read = read_cluster_kv_watch_event_log(&paths).expect("read log");
        assert!(read.events.is_empty());
        assert_eq!(read.records, 0);
        assert_eq!(
            std::fs::metadata(&paths.watch_events_log)
                .expect("log metadata")
                .len(),
            0
        );
    }

    #[test]
    fn cluster_kv_watch_events_read_merges_checkpoint_and_log() {
        let root = temp_raft_root("sinomo-kv-watch-events-read");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let checkpoint = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_put_event(2, "/svc/b", b"b"),
        ];
        write_cluster_kv_watch_event_checkpoint(&paths, &checkpoint).expect("checkpoint");

        let log_events = vec![
            test_kv_put_event(3, "/svc/c", b"c"),
            test_kv_put_event(4, "/svc/d", b"d"),
        ];
        let batch = encode_cluster_kv_watch_event_log_batch(&log_events).expect("encode log");
        let parent = paths.watch_events_log.parent().expect("log parent");
        std::fs::create_dir_all(parent).expect("create log parent");
        let mut log_file = std::fs::File::create(&paths.watch_events_log).expect("create log");
        for record in &batch.records {
            let len = u32::try_from(record.len()).expect("record length");
            log_file
                .write_all(&len.to_be_bytes())
                .and_then(|()| log_file.write_all(record))
                .expect("write log record");
        }
        log_file.sync_data().expect("sync log");

        let mut observed_read_bytes = 0_u64;
        let read = read_cluster_kv_watch_events_with_observer(&paths, |bytes| {
            observed_read_bytes = observed_read_bytes.saturating_add(bytes);
        })
        .expect("read watch events");

        assert_eq!(read.log_records, log_events.len());
        assert!(read.append_safe);
        assert_eq!(
            read.events
                .iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>(),
            vec![KvRevision(1), KvRevision(2), KvRevision(3), KvRevision(4)]
        );
        assert_eq!(observed_read_bytes, batch.bytes);
    }

    #[test]
    fn cluster_kv_watch_events_append_to_storage_uses_cache_view() {
        let root = temp_raft_root("sinomo-kv-watch-events-append-storage");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let first = test_kv_put_event(1, "/svc/a", b"a");
        let second = test_kv_put_event(2, "/svc/b", b"b");
        let mut observed_read_bytes = 0_u64;
        let mut observed_write_bytes = 0_u64;

        let first_report = append_cluster_kv_watch_events_to_storage_with_observers(
            &paths,
            &[first.clone(), second.clone()],
            None,
            |bytes| observed_read_bytes = observed_read_bytes.saturating_add(bytes),
            |bytes| observed_write_bytes = observed_write_bytes.saturating_add(bytes),
        )
        .expect("append first batch");

        assert_eq!(first_report.appended, vec![first.clone(), second.clone()]);
        assert_eq!(first_report.retained, vec![first.clone(), second.clone()]);
        assert_eq!(first_report.log_records, 2);
        assert!(first_report.append_safe);
        assert_eq!(observed_read_bytes, 0);
        assert!(observed_write_bytes > 0);

        let log_bytes = std::fs::metadata(&paths.watch_events_log)
            .expect("log metadata")
            .len();
        let cache = ClusterKvWatchEventAppendCacheView {
            events: first_report.retained.clone(),
            log_records: first_report.log_records,
            log_bytes,
            append_safe: first_report.append_safe,
        };
        observed_read_bytes = 0;
        observed_write_bytes = 0;
        let duplicate_report = append_cluster_kv_watch_events_to_storage_with_observers(
            &paths,
            std::slice::from_ref(&first),
            Some(&cache),
            |bytes| observed_read_bytes = observed_read_bytes.saturating_add(bytes),
            |bytes| observed_write_bytes = observed_write_bytes.saturating_add(bytes),
        )
        .expect("append duplicate");

        assert!(duplicate_report.appended.is_empty());
        assert_eq!(duplicate_report.retained, cache.events);
        assert_eq!(duplicate_report.log_records, cache.log_records);
        assert_eq!(observed_read_bytes, 0);
        assert_eq!(observed_write_bytes, 0);

        let third = test_kv_put_event(3, "/svc/c", b"c");
        observed_read_bytes = 0;
        observed_write_bytes = 0;
        let second_cache = ClusterKvWatchEventAppendCacheView {
            events: duplicate_report.retained.clone(),
            log_records: duplicate_report.log_records,
            log_bytes,
            append_safe: duplicate_report.append_safe,
        };
        let second_report = append_cluster_kv_watch_events_to_storage_with_observers(
            &paths,
            std::slice::from_ref(&third),
            Some(&second_cache),
            |bytes| observed_read_bytes = observed_read_bytes.saturating_add(bytes),
            |bytes| observed_write_bytes = observed_write_bytes.saturating_add(bytes),
        )
        .expect("append second batch");

        assert_eq!(second_report.appended, vec![third.clone()]);
        assert_eq!(
            second_report.retained,
            vec![first.clone(), second.clone(), third.clone()]
        );
        assert_eq!(second_report.log_records, 3);
        assert_eq!(observed_read_bytes, 0);
        assert!(observed_write_bytes > 0);
        assert_eq!(
            read_cluster_kv_watch_events(&paths)
                .expect("read retained events")
                .events,
            second_report.retained
        );
    }

    #[test]
    fn cluster_kv_watch_registration_file_validates_watch_id_and_removes() {
        let root = temp_raft_root("sinomo-kv-watch-registration-file");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let path = paths.watch_path(WatchId(7));
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(7),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };

        write_cluster_kv_watch_registration_file(&path, &registration).expect("write registration");
        assert_eq!(
            read_cluster_kv_watch_registration_file(&path, WatchId(7)).expect("read registration"),
            registration
        );
        assert!(
            !write_cluster_kv_watch_registration_file_create_new(&path, &registration)
                .expect("create existing registration"),
            "create-new registration writes must not clobber existing watch files"
        );
        assert_eq!(
            read_cluster_kv_watch_registration_file(&path, WatchId(8)),
            Err(KvError::UnknownWatch {
                watch_id: WatchId(8),
            })
        );
        assert!(
            remove_cluster_kv_watch_registration_file_if_exists(&path)
                .expect("remove registration")
        );
        assert!(
            !remove_cluster_kv_watch_registration_file_if_exists(&path)
                .expect("remove absent registration")
        );
    }

    #[test]
    fn cluster_kv_watch_allocator_state_defaults_and_normalizes_next_id() {
        let root = temp_raft_root("sinomo-kv-watch-allocator-state");
        let paths = ClusterKvPaths::new(root.to_path_buf());

        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths).expect("default allocator"),
            ClusterKvWatchAllocatorState::initial_for_missing_state(1)
        );

        let persisted = ClusterKvWatchAllocatorState {
            next_watch_id: 0,
            active_watch_count: Some(3),
            pruned_compacted_revision: Some(KvRevision(11)),
        };
        write_cluster_kv_watch_allocator_state(&paths, &persisted).expect("write allocator");
        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths).expect("read allocator"),
            ClusterKvWatchAllocatorState {
                next_watch_id: 1,
                active_watch_count: Some(3),
                pruned_compacted_revision: Some(KvRevision(11)),
            }
        );
    }

    #[test]
    fn cluster_kv_watch_allocator_lock_creates_durable_lock_file() {
        let root = temp_raft_root("sinomo-kv-watch-allocator-lock");
        let paths = ClusterKvPaths::new(root.to_path_buf());

        let lock = lock_cluster_kv_watch_allocator(&paths).expect("lock allocator");

        assert!(paths.watches_lock.exists());
        lock.unlock().expect("unlock allocator");
    }

    #[test]
    fn cluster_kv_watch_allocator_scan_policy_uses_trust_count_and_compaction() {
        let allocator = ClusterKvWatchAllocatorState {
            next_watch_id: 5,
            active_watch_count: Some(2),
            pruned_compacted_revision: Some(KvRevision(7)),
        };

        assert!(cluster_kv_watch_allocator_needs_scan(
            false,
            &allocator,
            KvRevision(7)
        ));
        assert!(!cluster_kv_watch_allocator_needs_scan(
            true,
            &allocator,
            KvRevision(7)
        ));
        assert!(cluster_kv_watch_allocator_needs_scan(
            true,
            &allocator,
            KvRevision(8)
        ));
        assert!(cluster_kv_watch_allocator_needs_scan(
            true,
            &ClusterKvWatchAllocatorState {
                active_watch_count: None,
                ..allocator.clone()
            },
            KvRevision(7)
        ));
        assert!(cluster_kv_watch_allocator_needs_scan(
            true,
            &ClusterKvWatchAllocatorState {
                pruned_compacted_revision: None,
                ..allocator
            },
            KvRevision(7)
        ));
    }

    #[test]
    fn cluster_kv_watch_allocator_after_scan_repairs_next_id_and_counts() {
        let allocator = ClusterKvWatchAllocatorState {
            next_watch_id: 2,
            active_watch_count: None,
            pruned_compacted_revision: None,
        };
        let scan = ClusterKvWatchScanResult {
            scanned: true,
            removed: 1,
            retained: 3,
            max_seen_id: 9,
            removed_paths: Vec::new(),
            retained_registrations: Vec::new(),
        };

        assert_eq!(
            cluster_kv_watch_allocator_after_scan(allocator, &scan, KvRevision(4)),
            ClusterKvWatchAllocatorState {
                next_watch_id: 10,
                active_watch_count: Some(3),
                pruned_compacted_revision: Some(KvRevision(4)),
            }
        );
    }

    #[test]
    fn cluster_kv_watch_allocator_after_registration_removed_updates_known_count() {
        assert_eq!(
            cluster_kv_watch_allocator_after_registration_removed(ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: Some(3),
                pruned_compacted_revision: Some(KvRevision(4)),
            }),
            ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: Some(2),
                pruned_compacted_revision: Some(KvRevision(4)),
            }
        );
        assert_eq!(
            cluster_kv_watch_allocator_after_registration_removed(ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: Some(0),
                pruned_compacted_revision: Some(KvRevision(4)),
            })
            .active_watch_count,
            Some(0)
        );
        assert_eq!(
            cluster_kv_watch_allocator_after_registration_removed(ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: None,
                pruned_compacted_revision: Some(KvRevision(4)),
            })
            .active_watch_count,
            None
        );
    }

    #[test]
    fn cluster_kv_watch_registration_create_allocates_and_reports_allocator() {
        let root = temp_raft_root("sinomo-kv-watch-create");
        let paths = ClusterKvPaths::new(root.to_path_buf());

        let first = create_cluster_kv_watch_registration(
            &paths,
            ClusterKvWatchPrefixRequest {
                prefix: "/svc/first/".to_string(),
                start_revision: KvRevision(1),
            },
            KvRevision::ZERO,
            false,
            MAX_CLUSTER_KV_ACTIVE_WATCHES,
        )
        .expect("create first registration");

        assert_eq!(first.registration.watch_id, WatchId(1));
        assert!(first.watch_path.exists());
        assert_eq!(
            first.allocator,
            ClusterKvWatchAllocatorState {
                next_watch_id: 2,
                active_watch_count: Some(1),
                pruned_compacted_revision: Some(KvRevision::ZERO),
            }
        );
        assert!(
            first.scan.as_ref().is_some_and(|scan| scan.scanned),
            "untrusted allocator should force a watch directory scan"
        );
        write_cluster_kv_watch_allocator_state(&paths, &first.allocator)
            .expect("persist allocator");

        let second = create_cluster_kv_watch_registration(
            &paths,
            ClusterKvWatchPrefixRequest {
                prefix: "/svc/second/".to_string(),
                start_revision: KvRevision(1),
            },
            KvRevision::ZERO,
            true,
            MAX_CLUSTER_KV_ACTIVE_WATCHES,
        )
        .expect("create second registration");

        assert_eq!(second.registration.watch_id, WatchId(2));
        assert!(second.scan.is_none());
        assert_eq!(second.allocator.active_watch_count, Some(2));
    }

    #[test]
    fn cluster_kv_watch_lifecycle_helpers_create_drain_append_and_cancel() {
        let root = temp_raft_root("sinomo-kv-watch-lifecycle-helper");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let node_id = NodeId::from_legacy_named("node_kv_watch_lifecycle").expect("node");
        let mut store = InProcessKvStore::new();
        store
            .put("/watch/lifecycle/a", b"a".to_vec(), None)
            .expect("seed snapshot event");
        write_cluster_kv_snapshot(&paths, &store.snapshot().expect("snapshot"))
            .expect("write snapshot");

        let watch = create_cluster_kv_watch_for_node(
            &paths,
            node_id.clone(),
            ClusterKvWatchPrefixRequest {
                prefix: "/watch/lifecycle/".to_string(),
                start_revision: KvRevision(1),
            },
        )
        .expect("create watch");
        assert_eq!(watch.watch_id, WatchId(1));
        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths)
                .expect("allocator after create")
                .active_watch_count,
            Some(1)
        );

        let drained = drain_cluster_kv_watch_for_node(
            &paths,
            node_id.clone(),
            ClusterKvWatchDrainRequest {
                watch_id: watch.watch_id,
                wait_timeout_millis: 0,
            },
        )
        .expect("drain snapshot event");
        assert_eq!(
            drained
                .events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/watch/lifecycle/a"]
        );
        assert_eq!(drained.next_revision, KvRevision(2));

        let fanout = KvEvent {
            revision: KvRevision(2),
            key: "/watch/lifecycle/b".to_string(),
            kind: KvEventKind::Put {
                value: b"b".to_vec(),
            },
        };
        assert_eq!(
            append_cluster_kv_watch_events_for_active_watches_for_node(
                &paths,
                node_id.clone(),
                std::slice::from_ref(&fanout),
            )
            .expect("append fanout event"),
            1
        );
        let drained = drain_cluster_kv_watch_for_node(
            &paths,
            node_id,
            ClusterKvWatchDrainRequest {
                watch_id: watch.watch_id,
                wait_timeout_millis: 0,
            },
        )
        .expect("drain fanout event");
        assert_eq!(drained.events, vec![fanout]);
        assert_eq!(drained.next_revision, KvRevision(3));

        let cancelled = cancel_cluster_kv_watch(
            &paths,
            ClusterKvWatchCancelRequest {
                watch_id: watch.watch_id,
            },
        )
        .expect("cancel watch");
        assert!(cancelled.cancelled);
        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths)
                .expect("allocator after cancel")
                .active_watch_count,
            Some(0)
        );
        assert!(
            !cancel_cluster_kv_watch(
                &paths,
                ClusterKvWatchCancelRequest {
                    watch_id: watch.watch_id,
                },
            )
            .expect("cancel already removed watch")
            .cancelled
        );
    }

    #[test]
    fn cluster_kv_watch_event_handler_skips_storage_without_active_watchers() {
        let root = temp_raft_root("sinomo-kv-watch-fanout-no-active-watchers");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        write_cluster_kv_snapshot(
            &paths,
            &InProcessKvStore::new().snapshot().expect("snapshot"),
        )
        .expect("write empty snapshot");
        let cluster_id =
            Id::from_legacy_named("cluster_kv_watch_fanout_no_active").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_kv_watch_fanout_no_active_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "kv watch fanout no active",
            leader_id.clone(),
            "leader",
            "pk-leader",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;

        let response = handle_cluster_kv_watch_events(
            &paths,
            &state,
            ClusterKvWatchEventsRequest {
                state: state.clone(),
                leader_id,
                events: vec![test_kv_put_event(1, "/watch/fanout/no-active", b"value")],
            },
        )
        .expect("watch event fanout response");

        assert!(response.accepted);
        assert_eq!(response.stored_events, 0);
        assert!(
            !paths.watch_events.exists(),
            "no active watcher should avoid checkpoint writes"
        );
        assert!(
            !paths.watch_events_log.exists(),
            "no active watcher should avoid log appends"
        );
    }

    #[test]
    fn cluster_kv_watch_event_handler_rejects_invalid_source_before_storage() {
        let root = temp_raft_root("sinomo-kv-watch-fanout-reject-invalid-source");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let cluster_id = Id::from_legacy_named("cluster_kv_watch_fanout_reject").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_kv_watch_fanout_reject_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "kv watch fanout reject",
            leader_id.clone(),
            "leader",
            "pk-leader",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;

        let peer_cluster = Id::from_legacy_named("cluster_kv_watch_fanout_other").expect("cluster");
        let mut peer_state = state.clone();
        peer_state.cluster_id = peer_cluster;
        let wrong_cluster = handle_cluster_kv_watch_events(
            &paths,
            &state,
            ClusterKvWatchEventsRequest {
                state: peer_state,
                leader_id: leader_id.clone(),
                events: vec![test_kv_put_event(
                    1,
                    "/watch/fanout/wrong-cluster",
                    b"value",
                )],
            },
        )
        .expect("wrong cluster response");
        assert!(!wrong_cluster.accepted);
        assert_eq!(wrong_cluster.stored_events, 0);
        assert!(
            wrong_cluster
                .detail
                .contains("does not match local cluster"),
            "unexpected detail: {}",
            wrong_cluster.detail
        );

        let unauthorized = handle_cluster_kv_watch_events(
            &paths,
            &state,
            ClusterKvWatchEventsRequest {
                state: state.clone(),
                leader_id: NodeId::from_legacy_named("node_kv_watch_fanout_intruder")
                    .expect("node"),
                events: vec![test_kv_put_event(2, "/watch/fanout/intruder", b"value")],
            },
        )
        .expect("unauthorized leader response");
        assert!(!unauthorized.accepted);
        assert_eq!(unauthorized.stored_events, 0);
        assert!(
            unauthorized.detail.contains("unauthorized leader"),
            "unexpected detail: {}",
            unauthorized.detail
        );
        assert!(!paths.watch_events.exists());
        assert!(!paths.watch_events_log.exists());
    }

    #[test]
    fn cluster_kv_watch_registration_create_prunes_and_keeps_high_watermark() {
        let root = temp_raft_root("sinomo-kv-watch-create-scan");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let stale_path = paths.watch_path(WatchId(7));
        let live_path = paths.watch_path(WatchId(9));
        write_cluster_kv_watch_registration_file(
            &stale_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: "/svc/stale/".to_string(),
                next_revision: KvRevision(1),
            },
        )
        .expect("write stale");
        write_cluster_kv_watch_registration_file(
            &live_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(9),
                prefix: "/svc/live/".to_string(),
                next_revision: KvRevision(2),
            },
        )
        .expect("write live");

        let report = create_cluster_kv_watch_registration(
            &paths,
            ClusterKvWatchPrefixRequest {
                prefix: "/svc/new/".to_string(),
                start_revision: KvRevision(2),
            },
            KvRevision(1),
            false,
            MAX_CLUSTER_KV_ACTIVE_WATCHES,
        )
        .expect("create after pruning");

        assert!(!stale_path.exists());
        assert!(live_path.exists());
        assert_eq!(report.registration.watch_id, WatchId(10));
        assert_eq!(report.allocator.active_watch_count, Some(2));
        let scan = report.scan.expect("scan report");
        assert_eq!(scan.retained, 1);
        assert_eq!(scan.removed, 1);
        assert_eq!(scan.max_seen_id, 9);
    }

    #[test]
    fn cluster_kv_watch_registration_drain_advances_cursor_with_cache_action() {
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(3),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(1),
        };
        let snapshot = InProcessKvStore::new().snapshot().expect("snapshot");
        let events = vec![
            KvEvent {
                revision: KvRevision(1),
                key: "/svc/one".to_string(),
                kind: KvEventKind::Put {
                    value: b"one".to_vec(),
                },
            },
            KvEvent {
                revision: KvRevision(2),
                key: "/svc/two".to_string(),
                kind: KvEventKind::Put {
                    value: b"two".to_vec(),
                },
            },
            KvEvent {
                revision: KvRevision(2),
                key: "/other/two".to_string(),
                kind: KvEventKind::Put {
                    value: b"ignored".to_vec(),
                },
            },
        ];

        let report = drain_cluster_kv_watch_registration(
            registration,
            KvRevision(1),
            &events,
            &snapshot,
            MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
            true,
            Some(Duration::ZERO),
        )
        .expect("drain registration");

        assert_eq!(report.response.watch_id, WatchId(3));
        assert_eq!(report.response.next_revision, KvRevision(3));
        assert_eq!(
            report
                .response
                .events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/svc/one", "/svc/two"]
        );
        assert_eq!(report.registration.next_revision, KvRevision(3));
        assert_eq!(
            report.cursor_action,
            ClusterKvWatchDrainCursorAction::CacheOnly {
                durable_next_revision: KvRevision(1),
            }
        );
    }

    #[test]
    fn cluster_kv_watch_registration_drain_reports_persist_and_compaction() {
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(5),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(1),
        };
        let mut store = InProcessKvStore::new();
        store
            .put("/svc/one", b"one".to_vec(), None)
            .expect("put snapshot event");
        let snapshot = store.snapshot().expect("snapshot");

        let report = drain_cluster_kv_watch_registration(
            registration.clone(),
            KvRevision(1),
            &[],
            &snapshot,
            MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
            false,
            Some(Duration::ZERO),
        )
        .expect("drain registration");

        assert_eq!(report.response.next_revision, KvRevision(2));
        assert_eq!(
            report.cursor_action,
            ClusterKvWatchDrainCursorAction::Persist
        );

        store.compact(KvRevision(1)).expect("compact snapshot");
        assert!(matches!(
            drain_cluster_kv_watch_registration(
                registration,
                KvRevision(1),
                &[],
                &store.snapshot().expect("snapshot"),
                MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
                true,
                Some(Duration::ZERO),
            ),
            Err(KvError::Compacted {
                requested: KvRevision(1),
                compacted: KvRevision(1),
            })
        ));
    }

    #[test]
    fn cluster_kv_watch_registration_remove_reports_allocator_update() {
        let root = temp_raft_root("sinomo-kv-watch-remove");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(7),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };
        write_cluster_kv_watch_registration_file(
            &paths.watch_path(registration.watch_id),
            &registration,
        )
        .expect("write registration");
        write_cluster_kv_watch_allocator_state(
            &paths,
            &ClusterKvWatchAllocatorState {
                next_watch_id: 8,
                active_watch_count: Some(2),
                pruned_compacted_revision: Some(KvRevision(1)),
            },
        )
        .expect("write allocator");

        let report = remove_cluster_kv_watch_registration_with_allocator(&paths, WatchId(7))
            .expect("remove registration");

        assert!(report.removed);
        assert!(!report.watch_path.exists());
        assert_eq!(
            report.allocator,
            Some(ClusterKvWatchAllocatorState {
                next_watch_id: 8,
                active_watch_count: Some(1),
                pruned_compacted_revision: Some(KvRevision(1)),
            })
        );
        assert!(
            !remove_cluster_kv_watch_registration_with_allocator(&paths, WatchId(7))
                .expect("remove absent registration")
                .removed
        );
    }

    #[test]
    fn cluster_kv_watch_drain_cursor_persistence_writes_only_when_needed() {
        let root = temp_raft_root("sinomo-kv-watch-drain-cursor-persist");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let watch_path = paths.watch_path(WatchId(7));
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(7),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };
        write_cluster_kv_watch_registration_file(&watch_path, &registration)
            .expect("write initial registration");
        let advanced = ClusterKvWatchRegistration {
            next_revision: KvRevision(9),
            ..registration.clone()
        };
        let persist_report = ClusterKvWatchDrainRegistrationReport {
            registration: advanced.clone(),
            response: ClusterKvWatchDrainResponse {
                watch_id: advanced.watch_id,
                events: Vec::new(),
                next_revision: advanced.next_revision,
            },
            cursor_action: ClusterKvWatchDrainCursorAction::Persist,
        };

        assert_eq!(
            persist_cluster_kv_watch_drain_cursor(&watch_path, &persist_report)
                .expect("persist cursor"),
            ClusterKvWatchDrainCursorPersistence::Persisted {
                registration: advanced.clone()
            }
        );
        assert_eq!(
            read_cluster_kv_watch_registration_file(&watch_path, WatchId(7))
                .expect("read persisted registration"),
            advanced
        );

        let cache_only = ClusterKvWatchRegistration {
            next_revision: KvRevision(11),
            ..registration.clone()
        };
        let cache_report = ClusterKvWatchDrainRegistrationReport {
            registration: cache_only.clone(),
            response: ClusterKvWatchDrainResponse {
                watch_id: cache_only.watch_id,
                events: Vec::new(),
                next_revision: cache_only.next_revision,
            },
            cursor_action: ClusterKvWatchDrainCursorAction::CacheOnly {
                durable_next_revision: advanced.next_revision,
            },
        };
        assert_eq!(
            persist_cluster_kv_watch_drain_cursor(&watch_path, &cache_report)
                .expect("cache-only cursor"),
            ClusterKvWatchDrainCursorPersistence::CacheOnly {
                registration: cache_only,
                durable_next_revision: advanced.next_revision
            }
        );
        assert_eq!(
            read_cluster_kv_watch_registration_file(&watch_path, WatchId(7))
                .expect("read unchanged durable registration"),
            advanced
        );
    }

    #[test]
    fn cluster_kv_watch_scan_prunes_stale_and_invalid_registrations() {
        let root = temp_raft_root("sinomo-kv-watch-scan");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let stale_path = paths.watch_path(WatchId(7));
        let live_path = paths.watch_path(WatchId(9));
        let mismatched_path = paths.watch_path(WatchId(11));
        let corrupt_path = paths.watch_path(WatchId(13));
        write_cluster_kv_watch_registration_file(
            &stale_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: "/svc/stale/".to_string(),
                next_revision: KvRevision(1),
            },
        )
        .expect("write stale");
        let live = ClusterKvWatchRegistration {
            watch_id: WatchId(9),
            prefix: "/svc/live/".to_string(),
            next_revision: KvRevision(2),
        };
        write_cluster_kv_watch_registration_file(&live_path, &live).expect("write live");
        write_cluster_kv_watch_registration_file(
            &mismatched_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(1),
                prefix: "/svc/mismatched/".to_string(),
                next_revision: KvRevision(2),
            },
        )
        .expect("write mismatched");
        std::fs::write(&corrupt_path, b"not-a-sinomo-object").expect("write corrupt");

        let scan =
            scan_cluster_kv_watch_registrations(&paths, KvRevision(1)).expect("scan watches");

        assert!(scan.scanned);
        assert_eq!(scan.retained, 1);
        assert_eq!(scan.removed, 3);
        assert_eq!(scan.max_seen_id, 13);
        assert_eq!(
            scan.retained_registrations,
            vec![ClusterKvWatchScanRegistration {
                path: live_path.clone(),
                registration: live,
            }]
        );
        assert!(!stale_path.exists(), "stale registration should be pruned");
        assert!(live_path.exists(), "live registration should remain");
        assert!(
            !mismatched_path.exists(),
            "mismatched registration should be pruned"
        );
        assert!(
            !corrupt_path.exists(),
            "corrupt registration should be pruned"
        );
        assert!(scan.removed_paths.contains(&stale_path));
        assert!(scan.removed_paths.contains(&mismatched_path));
        assert!(scan.removed_paths.contains(&corrupt_path));
    }

    #[test]
    fn cluster_kv_active_watch_registration_report_updates_allocator_after_scan() {
        let root = temp_raft_root("sinomo-kv-active-watch-report");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let stale = ClusterKvWatchRegistration {
            watch_id: WatchId(2),
            prefix: "/stale/".to_string(),
            next_revision: KvRevision(2),
        };
        write_cluster_kv_watch_registration_file(&paths.watch_path(stale.watch_id), &stale)
            .expect("write stale registration");
        let retained = ClusterKvWatchRegistration {
            watch_id: WatchId(4),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(9),
        };
        write_cluster_kv_watch_registration_file(&paths.watch_path(retained.watch_id), &retained)
            .expect("write retained registration");
        write_cluster_kv_watch_allocator_state(
            &paths,
            &ClusterKvWatchAllocatorState {
                next_watch_id: 3,
                active_watch_count: Some(99),
                pruned_compacted_revision: Some(KvRevision(1)),
            },
        )
        .expect("write stale allocator");

        let report = cluster_kv_active_watch_registration_report(&paths, KvRevision(5))
            .expect("active watch report");

        assert!(report.has_active);
        assert_eq!(
            report.allocator,
            Some(ClusterKvWatchAllocatorState {
                next_watch_id: 5,
                active_watch_count: Some(1),
                pruned_compacted_revision: Some(KvRevision(5)),
            })
        );
        let scan = report.scan.expect("scan report");
        assert_eq!(scan.retained, 1);
        assert_eq!(scan.removed, 1);
        assert_eq!(scan.retained_registrations[0].registration, retained);
        assert!(
            !paths.watch_path(stale.watch_id).exists(),
            "stale registration should be pruned by report scan"
        );
    }

    #[test]
    fn cluster_kv_watch_id_file_names_parse_only_watch_objects() {
        assert_eq!(
            cluster_kv_watch_id_from_file_name("watch-42.sobj"),
            Some(42)
        );
        assert_eq!(cluster_kv_watch_id_from_file_name("watch-.sobj"), None);
        assert_eq!(cluster_kv_watch_id_from_file_name("watch-42.tmp"), None);
        assert_eq!(cluster_kv_watch_id_from_file_name("other-42.sobj"), None);
    }

    #[test]
    fn cluster_kv_watch_registration_listing_ignores_invalid_files() {
        let root = temp_raft_root("sinomo-kv-watch-registration-list");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let first = ClusterKvWatchRegistration {
            watch_id: WatchId(3),
            prefix: "/svc/a/".to_string(),
            next_revision: KvRevision(2),
        };
        let second = ClusterKvWatchRegistration {
            watch_id: WatchId(5),
            prefix: "/svc/b/".to_string(),
            next_revision: KvRevision(4),
        };
        write_cluster_kv_watch_registration_file(&paths.watch_path(first.watch_id), &first)
            .expect("write first");
        write_cluster_kv_watch_registration_file(&paths.watch_path(second.watch_id), &second)
            .expect("write second");
        write_cluster_kv_watch_registration_file(
            &paths.watch_path(WatchId(7)),
            &ClusterKvWatchRegistration {
                watch_id: WatchId(1),
                prefix: "/svc/mismatch/".to_string(),
                next_revision: KvRevision(1),
            },
        )
        .expect("write mismatched");
        std::fs::write(paths.watches.join("watch-9.sobj"), b"not-a-sinomo-object")
            .expect("write corrupt");
        std::fs::write(paths.watches.join("ignore.tmp"), b"ignored").expect("write ignored");

        let mut registrations =
            list_cluster_kv_watch_registrations(&paths).expect("list registrations");
        registrations.sort_by_key(|registration| registration.watch_id);

        assert_eq!(registrations, vec![first, second]);
    }

    #[test]
    fn cluster_kv_watch_events_matching_registrations_filters_by_prefix_and_revision() {
        let registrations = vec![
            ClusterKvWatchRegistration {
                watch_id: WatchId(1),
                prefix: "/svc/a/".to_string(),
                next_revision: KvRevision(3),
            },
            ClusterKvWatchRegistration {
                watch_id: WatchId(2),
                prefix: "/svc/b/".to_string(),
                next_revision: KvRevision(5),
            },
        ];
        let events = vec![
            test_kv_put_event(2, "/svc/a/old", b"old"),
            test_kv_put_event(3, "/svc/a/current", b"current"),
            test_kv_put_event(5, "/svc/b/current", b"current"),
            test_kv_put_event(6, "/svc/c/ignored", b"ignored"),
        ];

        let matched = cluster_kv_watch_events_matching_registrations(&registrations, &events);

        assert_eq!(
            matched
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(3, "/svc/a/current"), (5, "/svc/b/current")]
        );
    }

    #[test]
    fn cluster_kv_watch_ids_matching_events_are_deduped_and_ordered() {
        let registrations = vec![
            ClusterKvWatchRegistration {
                watch_id: WatchId(9),
                prefix: "/svc/a/".to_string(),
                next_revision: KvRevision(2),
            },
            ClusterKvWatchRegistration {
                watch_id: WatchId(3),
                prefix: "/svc/a/".to_string(),
                next_revision: KvRevision(4),
            },
            ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: "/svc/b/".to_string(),
                next_revision: KvRevision(5),
            },
        ];
        let events = vec![
            test_kv_put_event(3, "/svc/a/one", b"one"),
            test_kv_put_event(5, "/svc/a/two", b"two"),
            test_kv_put_event(5, "/svc/b/three", b"three"),
        ];

        assert_eq!(
            cluster_kv_watch_ids_matching_events(&registrations, &events),
            vec![WatchId(3), WatchId(7), WatchId(9)]
        );
    }

    fn test_kv_snapshot_with_events(events: Vec<KvEvent>) -> KvSnapshot {
        KvSnapshot {
            revision: events
                .iter()
                .map(|event| event.revision)
                .max()
                .unwrap_or(KvRevision::ZERO),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events,
        }
    }

    fn test_kv_put_event(revision: u64, key: &str, value: &[u8]) -> KvEvent {
        KvEvent {
            revision: KvRevision(revision),
            key: key.to_string(),
            kind: KvEventKind::Put {
                value: value.to_vec(),
            },
        }
    }

    fn test_kv_delete_event(revision: u64, key: &str) -> KvEvent {
        KvEvent {
            revision: KvRevision(revision),
            key: key.to_string(),
            kind: KvEventKind::Delete,
        }
    }

    fn put_test_prefix_keys(store: &mut InProcessKvStore, prefix: &str, count: usize) {
        for index in 0..count {
            let key = format!("{prefix}{index:05}");
            store.put(key, vec![index as u8], None).expect("put");
        }
    }

    fn put_test_lease_keys(
        store: &mut InProcessKvStore,
        prefix: &str,
        count: usize,
        lease_id: LeaseId,
    ) {
        for index in 0..count {
            let key = format!("{prefix}{index:05}");
            store
                .put(key, vec![index as u8], Some(lease_id))
                .expect("leased put");
        }
    }

    #[test]
    fn write_object_file_skips_identical_and_rewrites_after_external_change() {
        let _guard = OBJECT_FILE_WRITE_MARK_CACHE_TEST_MUTEX
            .lock()
            .expect("object mark cache mutex");
        // Unique path so the test is independent of any shared state.
        let root = sinomo_testdir::TestDir::new("sinomo-kv-objfile-guard");
        let path = root.join("obj.sobj");

        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };
        write_object_file(&path, &record).expect("initial write");
        let mtime_after_write = std::fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("mtime");

        // An identical re-write takes the fast path and leaves the file untouched.
        write_object_file(&path, &record).expect("identical re-write");
        assert_eq!(
            std::fs::metadata(&path)
                .expect("metadata")
                .modified()
                .expect("mtime"),
            mtime_after_write,
            "an identical re-write must be skipped without rewriting the file"
        );

        // Another writer replaces the content behind our back with the same byte
        // length. This must be detected by the file-identity guard, not just length.
        let original_len = std::fs::metadata(&path).expect("metadata").len() as usize;
        let replacement = vec![b'x'; original_len];
        let replacement_path = root.join("replacement.sobj");
        std::fs::write(&replacement_path, replacement).expect("external write");
        std::fs::rename(&replacement_path, &path).expect("external replace");

        // Writing our content again must detect the change via the stat guard, fall
        // back to read + compare, and actually rewrite -- never falsely skip.
        write_object_file(&path, &record).expect("rewrite after external change");
        let restored: TestRecord = read_object_file(&path).expect("read back");
        assert_eq!(
            restored, record,
            "a file changed since our last write must be rewritten, not skipped"
        );
    }

    #[test]
    fn object_file_write_mark_cache_retains_recent_entry_at_capacity() {
        let _guard = OBJECT_FILE_WRITE_MARK_CACHE_TEST_MUTEX
            .lock()
            .expect("object mark cache mutex");
        LAST_WRITTEN_OBJECT_FILES.clear();
        LAST_WRITTEN_OBJECT_FILES.reset_clock();
        let hash = blake3::hash(b"stable");
        let hot_path = PathBuf::from("/sinomo-kv-object-mark-lru/000-hot.sobj");
        for index in 0..FILE_CACHE_CAPACITY.saturating_sub(1) {
            LAST_WRITTEN_OBJECT_FILES.record_mark(
                &PathBuf::from(format!(
                    "/sinomo-kv-object-mark-lru/100-quiet-{index:04}.sobj"
                )),
                hash,
                6,
                Some(SystemTime::UNIX_EPOCH),
                index as u64,
            );
        }
        LAST_WRITTEN_OBJECT_FILES.record_mark(
            &hot_path,
            hash,
            6,
            Some(SystemTime::UNIX_EPOCH),
            FILE_CACHE_CAPACITY as u64,
        );
        assert!(
            LAST_WRITTEN_OBJECT_FILES.refresh(&hot_path),
            "looking up the hot object write mark should refresh its recency"
        );

        LAST_WRITTEN_OBJECT_FILES.record_mark(
            &PathBuf::from("/sinomo-kv-object-mark-lru/999-extra.sobj"),
            hash,
            6,
            Some(SystemTime::UNIX_EPOCH),
            (FILE_CACHE_CAPACITY + 1) as u64,
        );

        assert!(
            LAST_WRITTEN_OBJECT_FILES.refresh(&hot_path),
            "recent object write mark should survive capacity churn"
        );
        LAST_WRITTEN_OBJECT_FILES.retain_not_under_root(Path::new("/sinomo-kv-object-mark-lru"));
    }

    #[test]
    fn write_object_file_cleans_temp_file_after_replace_failure() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-objfile-temp-cleanup");
        let path = root.join("obj.sobj");
        std::fs::create_dir(&path).expect("target directory");

        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };
        let error = write_object_file(&path, &record).expect_err("directory replace should fail");
        assert!(
            error.to_string().contains("obj.sobj"),
            "unexpected error: {error}"
        );
        let leftovers = std::fs::read_dir(&root)
            .expect("read temp dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".obj.sobj.tmp-"))
            })
            .count();
        assert_eq!(
            leftovers, 0,
            "failed object replacement should not leave staged temp files"
        );
    }

    #[test]
    fn read_optional_object_returns_none_only_for_missing_file() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-optional-objfile");
        let missing_path = root.join("missing.sobj");
        assert_eq!(
            read_optional_object::<TestRecord>(&missing_path).expect("missing optional object"),
            None
        );

        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };
        let object_path = root.join("record.sobj");
        write_object_file(&object_path, &record).expect("write object");
        assert_eq!(
            read_optional_object::<TestRecord>(&object_path).expect("present optional object"),
            Some(record)
        );

        let directory_path = root.join("directory.sobj");
        std::fs::create_dir(&directory_path).expect("directory");
        let error =
            read_optional_object::<TestRecord>(&directory_path).expect_err("directory is not none");
        assert!(
            error.to_string().contains("directory.sobj"),
            "present unreadable objects should stay errors, got {error}"
        );
    }

    #[test]
    fn serde_values_use_sinomo_object() {
        let mut store = InProcessKvStore::new();
        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };

        store
            .put_serde("/typed/object", &record, None)
            .expect("put object value");
        let object_entry = store.get("/typed/object").expect("object entry");
        assert!(object_entry.value.starts_with(sinomo_object::MAGIC));
        assert_eq!(
            store
                .get_serde::<TestRecord>("/typed/object")
                .expect("decode object"),
            Some(record)
        );
    }

    #[test]
    fn range_prefix_uses_ordered_bounds_and_enforces_limit() {
        let mut store = InProcessKvStore::new();
        for key in [
            "/apps/service/a",
            "/apps/service/b",
            "/apps/servicez/not-a-child",
            "/apps/servicf/not-a-child",
            "/other/service/c",
        ] {
            store.put(key, key.as_bytes().to_vec(), None).expect("put");
        }

        let entries = store.range_prefix("/apps/service/").expect("range prefix");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );

        let error = store
            .range_prefix_limited("/apps/service/", 1)
            .expect_err("range limit");
        assert_eq!(
            error,
            KvError::RangeLimit {
                prefix: "/apps/service/".to_string(),
                max_entries: 1,
            }
        );

        let error = store
            .range_prefix_limited_by_payload_bytes("/apps/service/", 4, "/apps/service/a".len())
            .expect_err("range payload limit");
        assert_eq!(
            error,
            KvError::RangePayloadLimit {
                prefix: "/apps/service/".to_string(),
                observed_bytes: "/apps/service/a".len() + "/apps/service/a".len(),
                max_bytes: "/apps/service/a".len(),
            }
        );
    }

    #[test]
    fn typed_range_prefix_reads_enforce_entry_and_payload_limits() {
        let mut store = InProcessKvStore::new();
        for key in ["/apps/typed/a", "/apps/typed/b"] {
            store.put_serde(key, &key.to_string(), None).expect("put");
        }

        let values = store
            .range_prefix_serde_limited_by_payload_bytes::<String>("/apps/typed/", 2, usize::MAX)
            .expect("typed range");
        assert_eq!(
            values,
            vec!["/apps/typed/a".to_string(), "/apps/typed/b".to_string()]
        );

        assert_eq!(
            store
                .range_prefix_serde_limited_by_payload_bytes::<String>(
                    "/apps/typed/",
                    1,
                    usize::MAX,
                )
                .expect_err("entry limit"),
            KvError::RangeLimit {
                prefix: "/apps/typed/".to_string(),
                max_entries: 1,
            }
        );

        let error = store
            .range_prefix_serde_limited_by_payload_bytes::<String>("/apps/typed/", 2, 1)
            .expect_err("payload limit");
        assert!(matches!(
            error,
            KvError::RangePayloadLimit {
                ref prefix,
                max_bytes: 1,
                ..
            } if prefix == "/apps/typed/"
        ));
    }

    #[test]
    fn snapshot_point_and_prefix_reads_preserve_store_semantics() {
        let entry = |key: &str, value: &[u8]| KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let sorted = KvSnapshot {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            entries: vec![
                entry("/apps/service/a", b"a"),
                entry("/apps/service/b", b"b"),
                entry("/apps/servicez/not-child", b"z"),
                entry("/other/service/c", b"c"),
            ],
            leases: Vec::new(),
            events: Vec::new(),
        };

        assert_eq!(
            sorted.get("/apps/service/b").expect("point read").value,
            b"b"
        );
        assert!(sorted.get("/apps/service/missing").is_none());
        assert_eq!(
            sorted
                .range_prefix("/apps/service/")
                .expect("range prefix")
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );
        assert_eq!(
            sorted
                .prefix_entries("/apps/service/")
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );

        let unsorted = KvSnapshot {
            entries: vec![
                entry("/apps/service/b", b"b"),
                entry("/apps/service/a", b"stale-a"),
                entry("/other/service/c", b"c"),
                entry("/apps/service/a", b"a"),
            ],
            ..sorted
        }
        .canonicalized();
        assert_eq!(
            unsorted
                .entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b", "/other/service/c"]
        );
        assert_eq!(
            unsorted.get("/apps/service/a").expect("point read").value,
            b"a"
        );
        assert_eq!(
            unsorted
                .range_prefix("/apps/service/")
                .expect("range prefix")
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );
        assert_eq!(
            unsorted
                .range_prefix_limited("/apps/service/", 1)
                .expect_err("range limit"),
            KvError::RangeLimit {
                prefix: "/apps/service/".to_string(),
                max_entries: 1,
            }
        );
    }

    #[test]
    fn snapshot_resource_validation_rejects_oversized_entry_values() {
        let snapshot = KvSnapshot {
            revision: KvRevision(1),
            compacted_revision: KvRevision::ZERO,
            entries: vec![KvEntry {
                key: "/snapshot/large-entry".to_string(),
                value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: KvVersion(1),
                lease_id: None,
            }],
            leases: Vec::new(),
            events: Vec::new(),
        };

        assert_eq!(
            snapshot
                .validate_resource_bounds()
                .expect_err("entry value limit"),
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
    }

    #[test]
    fn snapshot_resource_validation_rejects_oversized_event_values() {
        let value = vec![0_u8; MAX_KV_VALUE_BYTES + 1];
        let snapshot = test_kv_snapshot_with_events(vec![test_kv_put_event(
            1,
            "/snapshot/large-event",
            &value,
        )]);

        assert_eq!(
            snapshot
                .validate_resource_bounds()
                .expect_err("event value limit"),
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
    }

    #[test]
    fn snapshot_resource_validation_rejects_over_retained_event_limit() {
        let events = (0..=MAX_RETAINED_EVENTS)
            .map(|index| test_kv_delete_event(index as u64 + 1, "/snapshot/event"))
            .collect::<Vec<_>>();
        let snapshot = test_kv_snapshot_with_events(events);

        assert_eq!(
            snapshot
                .validate_resource_bounds()
                .expect_err("retained event limit"),
            KvError::SnapshotEventLimit {
                observed_events: MAX_RETAINED_EVENTS + 1,
                max_events: MAX_RETAINED_EVENTS,
            }
        );
    }

    #[test]
    fn snapshot_rpc_decode_rejects_over_retained_event_limit() {
        let events = (0..=MAX_RETAINED_EVENTS)
            .map(|index| test_kv_delete_event(index as u64 + 1, "/snapshot/rpc-event"))
            .collect::<Vec<_>>();
        let snapshot = test_kv_snapshot_with_events(events);
        let body = encode_cluster_kv_rpc_body(&snapshot).expect("encode snapshot body");

        assert_eq!(
            decode_cluster_kv_snapshot_rpc_body(&body).expect_err("snapshot event limit"),
            KvError::SnapshotEventLimit {
                observed_events: MAX_RETAINED_EVENTS + 1,
                max_events: MAX_RETAINED_EVENTS,
            }
        );
    }

    #[test]
    fn put_rejects_values_larger_than_metadata_limit() {
        let mut store = InProcessKvStore::new();
        let error = store
            .put("/large/value", vec![0_u8; MAX_KV_VALUE_BYTES + 1], None)
            .expect_err("value limit");

        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
        assert!(store.get("/large/value").is_none());
        assert_eq!(store.revision(), KvRevision::ZERO);
    }

    #[test]
    fn txn_rejects_large_values_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/large/value".to_string(),
                        value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("value limit aborts txn");

        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.get("/large/value").is_none());
        assert_eq!(store.revision(), KvRevision(1));
    }

    #[test]
    fn txn_rejects_aggregate_payload_over_command_budget() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let first_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let second_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let expected_bytes =
            first_value.len() + second_value.len() + "/payload/one".len() + "/payload/two".len();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/payload/one".to_string(),
                        value: first_value,
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/payload/two".to_string(),
                        value: second_value,
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("aggregate payload limit");

        assert_eq!(
            error,
            KvError::CommandPayloadTooLarge {
                actual_bytes: expected_bytes,
                max_bytes: MAX_KV_COMMAND_PAYLOAD_BYTES,
            }
        );
        assert!(store.get("/payload/one").is_none());
        assert!(store.get("/payload/two").is_none());
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert_eq!(store.revision(), KvRevision(1));
    }

    #[test]
    fn txn_rejects_large_compare_payloads() {
        let mut store = InProcessKvStore::new();
        let first_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let second_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let expected_bytes =
            first_value.len() + second_value.len() + "/compare/one".len() + "/compare/two".len();

        let error = store
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: "/compare/one".to_string(),
                        value: first_value,
                    },
                    KvCompare::ValueEquals {
                        key: "/compare/two".to_string(),
                        value: second_value,
                    },
                ],
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect_err("compare payload limit");

        assert_eq!(
            error,
            KvError::CommandPayloadTooLarge {
                actual_bytes: expected_bytes,
                max_bytes: MAX_KV_COMMAND_PAYLOAD_BYTES,
            }
        );
        assert_eq!(store.revision(), KvRevision::ZERO);
    }

    #[test]
    fn delete_prefix_uses_ordered_bounds() {
        let mut store = InProcessKvStore::new();
        for key in [
            "/apps/service/a",
            "/apps/service/b",
            "/apps/servicez/not-a-child",
            "/apps/servicf/not-a-child",
        ] {
            store.put(key, key.as_bytes().to_vec(), None).expect("put");
        }

        let response = store
            .apply_op(KvOp::DeletePrefix {
                prefix: "/apps/service/".to_string(),
            })
            .expect("delete prefix");

        assert!(matches!(
            response,
            KvOpResponse::DeletePrefix {
                deleted_count: 2,
                ..
            }
        ));
        assert!(store.get("/apps/service/a").is_none());
        assert!(store.get("/apps/service/b").is_none());
        assert!(store.get("/apps/servicez/not-a-child").is_some());
        assert!(store.get("/apps/servicf/not-a-child").is_some());
    }

    #[test]
    fn delete_prefix_rejects_oversized_prefix_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS + 1);
        store
            .put("/other/key", b"keep".to_vec(), None)
            .expect("outside put");
        let revision_before = store.revision();

        let error = store
            .apply_op(KvOp::DeletePrefix {
                prefix: "/huge/".to_string(),
            })
            .expect_err("delete-prefix limit");

        assert_eq!(
            error,
            KvError::DeletePrefixLimit {
                prefix: "/huge/".to_string(),
                observed_keys: MAX_DELETE_PREFIX_KEYS + 1,
                max_keys: MAX_DELETE_PREFIX_KEYS,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert!(store.get("/huge/00000").is_some());
        assert!(
            store
                .get(&format!("/huge/{:05}", MAX_DELETE_PREFIX_KEYS))
                .is_some()
        );
        assert!(store.get("/other/key").is_some());
    }

    #[test]
    fn txn_rejects_delete_prefix_over_mutation_budget_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS);
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::DeletePrefix {
                        prefix: "/huge/".to_string(),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("mutation budget aborts txn");

        assert_eq!(
            error,
            KvError::MutationEventLimit {
                observed_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1,
                max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.get("/huge/00000").is_some());
        assert!(
            store
                .get(&format!("/huge/{:05}", MAX_DELETE_PREFIX_KEYS - 1))
                .is_some()
        );
    }

    #[test]
    fn txn_stable_put_does_not_consume_mutation_budget_before_delete_prefix() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS);
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::DeletePrefix {
                        prefix: "/huge/".to_string(),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("stable put should not consume mutation budget");

        assert!(response.succeeded);
        assert_eq!(
            response.responses,
            vec![
                KvOpResponse::Put {
                    key: "/config/a".to_string(),
                    revision: revision_before,
                },
                KvOpResponse::DeletePrefix {
                    prefix: "/huge/".to_string(),
                    deleted_count: MAX_DELETE_PREFIX_KEYS,
                },
            ]
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );
        assert_eq!(store.get("/config/a").expect("stable entry").value, b"old");
        assert!(
            store
                .range_prefix("/huge/")
                .expect("range prefix")
                .is_empty()
        );
    }

    #[test]
    fn txn_delete_prefix_budget_accounts_for_prior_branch_deletes() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS);
        let revision_before = store.revision();

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Delete {
                        key: "/huge/00000".to_string(),
                    },
                    KvOp::DeletePrefix {
                        prefix: "/huge/".to_string(),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("txn within mutation budget");

        assert!(response.succeeded);
        assert_eq!(
            response.responses,
            vec![
                KvOpResponse::Delete {
                    key: "/huge/00000".to_string(),
                    deleted: true,
                },
                KvOpResponse::DeletePrefix {
                    prefix: "/huge/".to_string(),
                    deleted_count: MAX_DELETE_PREFIX_KEYS - 1,
                },
            ]
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );
        assert!(
            store
                .range_prefix("/huge/")
                .expect("range prefix")
                .is_empty()
        );
    }

    #[test]
    fn put_and_compare_swap_txn_updates_revision_and_version() {
        let mut store = InProcessKvStore::new();
        let rev1 = store.put("/config/a", b"old".to_vec(), None).expect("put");
        let response = store
            .txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                }],
                success: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                }],
                failure: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"failed".to_vec(),
                    lease_id: None,
                }],
            })
            .expect("txn");

        let entry = store.get("/config/a").expect("entry");
        assert_eq!(rev1, KvRevision(1));
        assert!(response.succeeded);
        assert_eq!(entry.value, b"new");
        assert_eq!(entry.create_revision, KvRevision(1));
        assert_eq!(entry.modify_revision, KvRevision(2));
        assert_eq!(entry.version, KvVersion(2));
    }

    #[test]
    fn txn_error_does_not_partially_apply_ops() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/config/b".to_string(),
                        value: b"bad-lease".to_vec(),
                        lease_id: Some(LeaseId(99)),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("unknown lease aborts txn");

        assert_eq!(
            error,
            KvError::UnknownLease {
                lease_id: LeaseId(99)
            }
        );
        assert_eq!(store.revision(), KvRevision(1));
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.get("/config/b").is_none());
    }

    #[test]
    fn txn_empty_selected_branch_response_is_read_only() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let response = store
            .txn_response_if_selected_branch_empty(&KvTxn {
                compares: vec![KvCompare::Missing {
                    key: "/config/a".to_string(),
                }],
                success: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect("empty selected branch");

        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: false,
                revision: revision_before,
                responses: Vec::new(),
            })
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");

        assert_eq!(
            store
                .txn_response_if_selected_branch_empty(&KvTxn {
                    compares: vec![KvCompare::Exists {
                        key: "/config/a".to_string(),
                    }],
                    success: vec![KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating selected branch"),
            None
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");
    }

    #[test]
    fn txn_no_effect_selected_branch_response_is_read_only() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 10,
            })
            .expect("lease grant");
        let revision_before = store.revision();

        let response = store
            .txn_response_if_selected_branch_no_effect(&KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Delete {
                        key: "/missing".to_string(),
                    },
                    KvOp::DeletePrefix {
                        prefix: "/missing-prefix/".to_string(),
                    },
                    KvOp::LeaseGrant {
                        lease_id: LeaseId(7),
                        ttl_ticks: 10,
                    },
                    KvOp::LeaseKeepAlive {
                        lease_id: LeaseId(7),
                    },
                    KvOp::LeaseRevoke {
                        lease_id: LeaseId(99),
                    },
                ],
                failure: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"failed".to_vec(),
                    lease_id: None,
                }],
            })
            .expect("no-effect selected branch");

        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: true,
                revision: revision_before,
                responses: vec![
                    KvOpResponse::Put {
                        key: "/config/a".to_string(),
                        revision: revision_before,
                    },
                    KvOpResponse::Delete {
                        key: "/missing".to_string(),
                        deleted: false,
                    },
                    KvOpResponse::DeletePrefix {
                        prefix: "/missing-prefix/".to_string(),
                        deleted_count: 0,
                    },
                    KvOpResponse::LeaseGrant {
                        lease_id: LeaseId(7),
                    },
                    KvOpResponse::LeaseKeepAlive {
                        lease_id: LeaseId(7),
                    },
                    KvOpResponse::LeaseRevoke {
                        lease_id: LeaseId(99),
                        deleted_count: 0,
                    },
                ],
            })
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");

        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Delete {
                        key: "/config/a".to_string(),
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating delete"),
            None
        );
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .expect("changed put value"),
            None
        );
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"old".to_vec(),
                        lease_id: Some(LeaseId(7)),
                    }],
                    failure: Vec::new(),
                })
                .expect("changed put lease"),
            None
        );

        store.tick_leases(1).expect("age lease");
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::LeaseKeepAlive {
                        lease_id: LeaseId(7),
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating keepalive"),
            None
        );
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::LeaseGrant {
                        lease_id: LeaseId(7),
                        ttl_ticks: 10,
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating lease grant refresh"),
            None
        );
        store
            .apply_op(KvOp::LeaseKeepAlive {
                lease_id: LeaseId(7),
            })
            .expect("refresh lease");
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::LeaseGrant {
                        lease_id: LeaseId(7),
                        ttl_ticks: 20,
                    }],
                    failure: Vec::new(),
                })
                .expect("changed lease ttl"),
            None
        );

        let error = store
            .txn_response_if_selected_branch_no_effect(&KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/too-large".to_string(),
                    value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect_err("no-effect preflight still validates payloads");
        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
    }

    #[test]
    fn txn_skips_no_effect_ops_inside_mixed_selected_branch() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/stable", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();
        let watch_id = store
            .watch_prefix("/config/", revision_before.next())
            .expect("watch future config changes");

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/stable".to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/config/changed".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })
            .expect("mixed txn");

        assert_eq!(
            response,
            KvTxnResponse {
                succeeded: true,
                revision: revision_before.next(),
                responses: vec![
                    KvOpResponse::Put {
                        key: "/config/stable".to_string(),
                        revision: revision_before,
                    },
                    KvOpResponse::Put {
                        key: "/config/changed".to_string(),
                        revision: revision_before.next(),
                    },
                ],
            }
        );
        assert_eq!(
            store.revision(),
            revision_before.next(),
            "mixed txns should not allocate a revision for stable no-effect ops"
        );
        assert_eq!(
            store.get("/config/stable").expect("stable entry").version,
            KvVersion(1),
            "stable no-effect put should not rewrite entry metadata"
        );
        let events = store.drain_watch(watch_id).expect("watch events");
        assert_eq!(
            events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/config/changed"],
            "stable no-effect ops inside mixed txns should not emit watch events"
        );
    }

    #[test]
    fn op_no_effect_response_is_read_only_and_validated() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 10,
            })
            .expect("lease grant");
        let revision_before = store.revision();

        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                    lease_id: None,
                })
                .expect("stable put"),
            Some(KvOpResponse::Put {
                key: "/config/a".to_string(),
                revision: revision_before,
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Delete {
                    key: "/missing".to_string(),
                })
                .expect("missing delete"),
            Some(KvOpResponse::Delete {
                key: "/missing".to_string(),
                deleted: false,
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::DeletePrefix {
                    prefix: "/missing-prefix/".to_string(),
                })
                .expect("empty delete prefix"),
            Some(KvOpResponse::DeletePrefix {
                prefix: "/missing-prefix/".to_string(),
                deleted_count: 0,
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseGrant {
                    lease_id: LeaseId(7),
                    ttl_ticks: 10,
                })
                .expect("stable lease grant"),
            Some(KvOpResponse::LeaseGrant {
                lease_id: LeaseId(7),
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseRevoke {
                    lease_id: LeaseId(99),
                })
                .expect("missing lease revoke"),
            Some(KvOpResponse::LeaseRevoke {
                lease_id: LeaseId(99),
                deleted_count: 0,
            })
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");

        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Delete {
                    key: "/config/a".to_string(),
                })
                .expect("mutating delete"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                })
                .expect("changed put value"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                    lease_id: Some(LeaseId(7)),
                })
                .expect("changed put lease"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseGrant {
                    lease_id: LeaseId(7),
                    ttl_ticks: 20,
                })
                .expect("changed lease grant"),
            None
        );
        store.tick_leases(1).expect("age lease");
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseGrant {
                    lease_id: LeaseId(7),
                    ttl_ticks: 10,
                })
                .expect("aged lease grant"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseKeepAlive {
                    lease_id: LeaseId(99),
                })
                .expect_err("unknown keepalive"),
            KvError::UnknownLease {
                lease_id: LeaseId(99),
            }
        );
    }

    #[test]
    fn txn_rejects_oversized_compares_and_branches() {
        let mut store = InProcessKvStore::new();
        let compares = (0..=MAX_KV_TXN_COMPARES)
            .map(|index| KvCompare::Missing {
                key: format!("/compare/{index}"),
            })
            .collect::<Vec<_>>();

        let error = store
            .txn(KvTxn {
                compares,
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect_err("compare limit");
        assert_eq!(
            error,
            KvError::TxnLimit {
                compares: MAX_KV_TXN_COMPARES + 1,
                success_ops: 0,
                failure_ops: 0,
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            }
        );

        let failure = (0..=MAX_KV_TXN_OPS_PER_BRANCH)
            .map(|index| KvOp::Delete {
                key: format!("/unused/failure/{index}"),
            })
            .collect::<Vec<_>>();
        let error = store
            .txn(KvTxn {
                compares: vec![KvCompare::Missing {
                    key: "/selected/success".to_string(),
                }],
                success: Vec::new(),
                failure,
            })
            .expect_err("unselected branch still counts");
        assert_eq!(
            error,
            KvError::TxnLimit {
                compares: 1,
                success_ops: 0,
                failure_ops: MAX_KV_TXN_OPS_PER_BRANCH + 1,
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            }
        );
    }

    #[test]
    fn bounded_txn_success_op_reports_success_branch_limit() {
        let mut ops = (0..MAX_KV_TXN_OPS_PER_BRANCH)
            .map(|index| KvOp::Delete {
                key: format!("/success/{index}"),
            })
            .collect::<Vec<_>>();

        let error = push_bounded_txn_success_op(
            &mut ops,
            KvOp::Delete {
                key: "/success/overflow".to_string(),
            },
        )
        .expect_err("success branch limit");

        assert_eq!(ops.len(), MAX_KV_TXN_OPS_PER_BRANCH);
        assert_eq!(
            error,
            KvError::TxnLimit {
                compares: 0,
                success_ops: MAX_KV_TXN_OPS_PER_BRANCH + 1,
                failure_ops: 0,
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            }
        );
    }

    #[test]
    fn txn_preflight_tracks_lease_state_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        for index in 1..=MAX_ACTIVE_LEASES {
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id: LeaseId(index as u64),
                    ttl_ticks: 10,
                })
                .expect("lease grant");
        }

        let replacement_lease = LeaseId((MAX_ACTIVE_LEASES + 1) as u64);
        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::LeaseRevoke {
                        lease_id: LeaseId(1),
                    },
                    KvOp::LeaseGrant {
                        lease_id: replacement_lease,
                        ttl_ticks: 20,
                    },
                    KvOp::Put {
                        key: "/leased/replacement".to_string(),
                        value: b"ok".to_vec(),
                        lease_id: Some(replacement_lease),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("revoke then grant");
        assert!(response.succeeded);
        assert!(!store.leases.contains_key(&LeaseId(1)));
        assert!(store.leases.contains_key(&replacement_lease));
        assert_eq!(
            store
                .get("/leased/replacement")
                .expect("replacement entry")
                .lease_id,
            Some(replacement_lease)
        );

        let rejected_lease = LeaseId((MAX_ACTIVE_LEASES + 2) as u64);
        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::LeaseGrant {
                        lease_id: rejected_lease,
                        ttl_ticks: 20,
                    },
                    KvOp::LeaseRevoke {
                        lease_id: LeaseId(2),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("grant before revoke hits lease limit");
        assert_eq!(
            error,
            KvError::LeaseLimit {
                max: MAX_ACTIVE_LEASES
            }
        );
        assert!(store.leases.contains_key(&LeaseId(2)));
        assert!(!store.leases.contains_key(&rejected_lease));
    }

    #[test]
    fn leased_put_rejects_over_key_limit_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(50);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(&mut store, "/lease/full/", MAX_KEYS_PER_LEASE, lease_id);
        let revision_before = store.revision();

        let error = store
            .put(
                format!("/lease/full/{:05}", MAX_KEYS_PER_LEASE),
                b"too-many".to_vec(),
                Some(lease_id),
            )
            .expect_err("lease key limit");

        assert_eq!(
            error,
            KvError::LeaseKeyLimit {
                lease_id,
                observed_keys: MAX_KEYS_PER_LEASE + 1,
                max_keys: MAX_KEYS_PER_LEASE,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert!(
            store
                .get(&format!("/lease/full/{:05}", MAX_KEYS_PER_LEASE))
                .is_none()
        );

        store
            .put("/lease/full/00000", b"updated".to_vec(), Some(lease_id))
            .expect("update existing leased key at limit");
        assert_eq!(
            store.get("/lease/full/00000").expect("updated").value,
            b"updated"
        );
    }

    #[test]
    fn txn_rejects_leased_put_over_key_limit_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(51);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(&mut store, "/lease/txn-full/", MAX_KEYS_PER_LEASE, lease_id);
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: format!("/lease/txn-full/{:05}", MAX_KEYS_PER_LEASE),
                        value: b"too-many".to_vec(),
                        lease_id: Some(lease_id),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("lease key limit aborts txn");

        assert_eq!(
            error,
            KvError::LeaseKeyLimit {
                lease_id,
                observed_keys: MAX_KEYS_PER_LEASE + 1,
                max_keys: MAX_KEYS_PER_LEASE,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(
            store
                .get(&format!("/lease/txn-full/{:05}", MAX_KEYS_PER_LEASE))
                .is_none()
        );
    }

    #[test]
    fn txn_lease_key_limit_accounts_for_prior_branch_delete() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(52);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(&mut store, "/lease/rotate/", MAX_KEYS_PER_LEASE, lease_id);
        let revision_before = store.revision();

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Delete {
                        key: "/lease/rotate/00000".to_string(),
                    },
                    KvOp::Put {
                        key: format!("/lease/rotate/{:05}", MAX_KEYS_PER_LEASE),
                        value: b"replacement".to_vec(),
                        lease_id: Some(lease_id),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("delete frees lease capacity");

        assert!(response.succeeded);
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0.saturating_add(2))
        );
        assert!(store.get("/lease/rotate/00000").is_none());
        assert!(
            store
                .get(&format!("/lease/rotate/{:05}", MAX_KEYS_PER_LEASE))
                .is_some()
        );
        assert_eq!(
            store.lease_keys.get(&lease_id).expect("lease keys").len(),
            MAX_KEYS_PER_LEASE
        );
    }

    #[test]
    fn lease_revoke_rejects_over_mutation_budget_without_partial_apply() {
        let lease_id = LeaseId(42);
        let entries = (0..=MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .map(|index| KvEntry {
                key: format!("/leased/huge/{index:05}"),
                value: vec![index as u8],
                create_revision: KvRevision((index + 1) as u64),
                modify_revision: KvRevision((index + 1) as u64),
                version: KvVersion(1),
                lease_id: Some(lease_id),
            })
            .collect::<Vec<_>>();
        let mut store = InProcessKvStore::restore(KvSnapshot {
            revision: KvRevision(entries.len() as u64),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: vec![KvLease {
                lease_id,
                ttl_ticks: 10,
                remaining_ticks: 10,
                granted_at: OffsetDateTime::now_utc(),
            }],
            events: Vec::new(),
        })
        .expect("restore");
        let revision_before = store.revision();

        let error = store
            .apply_op(KvOp::LeaseRevoke { lease_id })
            .expect_err("lease revoke limit");

        assert_eq!(
            error,
            KvError::MutationEventLimit {
                observed_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1,
                max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert!(store.leases.contains_key(&lease_id));
        assert!(store.get("/leased/huge/00000").is_some());
        assert!(
            store
                .get(&format!(
                    "/leased/huge/{:05}",
                    MAX_KV_MUTATION_EVENTS_PER_COMMAND
                ))
                .is_some()
        );
    }

    #[test]
    fn txn_rejects_lease_revoke_over_mutation_budget_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(43);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(
            &mut store,
            "/leased/budget/",
            MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            lease_id,
        );
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::LeaseRevoke { lease_id },
                ],
                failure: Vec::new(),
            })
            .expect_err("mutation budget aborts txn");

        assert_eq!(
            error,
            KvError::MutationEventLimit {
                observed_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1,
                max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.leases.contains_key(&lease_id));
        assert!(store.get("/leased/budget/00000").is_some());
    }

    #[test]
    fn watch_prefix_receives_backlog_and_future_events() {
        let mut store = InProcessKvStore::new();
        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put");
        store.put("/other/a", b"skip".to_vec(), None).expect("put");
        let watch_id = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("watch");

        let backlog = store.drain_watch(watch_id).expect("backlog");
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog[0].key, "/services/a");

        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put");
        let future = store.drain_watch(watch_id).expect("future");
        assert_eq!(future.len(), 1);
        assert_eq!(future[0].key, "/services/b");
    }

    #[test]
    fn watch_prefix_index_routes_only_matching_prefixes() {
        let mut store = InProcessKvStore::new();
        let root = store.watch_prefix("", KvRevision(1)).expect("root watch");
        let services = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("services watch");
        let exact = store
            .watch_prefix("/services/a", KvRevision(1))
            .expect("exact watch");
        let other = store
            .watch_prefix("/other/", KvRevision(1))
            .expect("other watch");

        assert_eq!(
            store.matching_watch_ids_for_event_key_for_test("/services/a"),
            vec![root, services, exact],
            "event routing should enumerate only watches whose prefixes match the changed key"
        );

        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put services");
        assert_eq!(store.drain_watch(root).expect("root events").len(), 1);
        assert_eq!(
            store.drain_watch(services).expect("services events").len(),
            1
        );
        assert_eq!(store.drain_watch(exact).expect("exact events").len(), 1);
        assert!(
            store.drain_watch(other).expect("other events").is_empty(),
            "unmatched watches should not receive the event"
        );

        assert_eq!(store.cancel_watch(services), Ok(true));
        assert_eq!(
            store.matching_watch_ids_for_event_key_for_test("/services/b"),
            vec![root],
            "canceled watches must be removed from the prefix index"
        );

        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put after cancel");
        assert_eq!(store.drain_watch(root).expect("root second").len(), 1);
        assert!(
            store.drain_watch(exact).expect("exact second").is_empty(),
            "exact key watch should not match a sibling key"
        );
        assert_eq!(
            store.drain_watch(services),
            Err(KvError::UnknownWatch { watch_id: services })
        );
    }

    #[test]
    fn matching_watch_queues_receive_equivalent_event_payloads() {
        let mut store = InProcessKvStore::new();
        let first = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("first watch");
        let second = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("second watch");
        let value = vec![7_u8; 128 * 1024];

        store
            .put("/services/shared", value.clone(), None)
            .expect("put shared event");

        {
            let first_event = store
                .watches
                .get(&first)
                .expect("first watch state")
                .pending
                .front()
                .expect("first pending event");
            let second_event = store
                .watches
                .get(&second)
                .expect("second watch state")
                .pending
                .front()
                .expect("second pending event");
            let history_event = store.events.last().expect("history event");

            assert_eq!(first_event, second_event);
            assert_eq!(first_event, history_event);
        }

        let drained = store.drain_watch(first).expect("drain first watch");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].key, "/services/shared");
        assert_eq!(drained[0].kind, KvEventKind::Put { value });
    }

    #[test]
    fn slow_watch_queue_is_bounded_and_reports_compaction() {
        let mut store = InProcessKvStore::new();
        let watch_id = store
            .watch_prefix("/events/", KvRevision(1))
            .expect("watch");

        for index in 0..=MAX_PENDING_EVENTS_PER_WATCH {
            store
                .put(
                    format!("/events/{index}"),
                    index.to_string().into_bytes(),
                    None,
                )
                .expect("put event");
        }

        let error = store
            .drain_watch(watch_id)
            .expect_err("lagged watch must fail instead of silently dropping events");
        assert_eq!(
            error,
            KvError::Compacted {
                requested: KvRevision(1),
                compacted: KvRevision(1),
            }
        );
        assert_eq!(
            store.drain_watch(watch_id),
            Err(KvError::UnknownWatch { watch_id })
        );
        assert!(
            store
                .matching_watch_ids_for_event_key_for_test("/events/next")
                .is_empty(),
            "lagged watches removed after compaction must leave the prefix index"
        );
    }

    #[test]
    fn cancelled_watch_releases_registration() {
        let mut store = InProcessKvStore::new();
        let watch_id = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("watch");
        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put");

        assert_eq!(store.cancel_watch(watch_id), Ok(true));
        assert_eq!(store.cancel_watch(watch_id), Ok(false));
        assert_eq!(
            store.drain_watch(watch_id),
            Err(KvError::UnknownWatch { watch_id })
        );
        assert!(
            store
                .matching_watch_ids_for_event_key_for_test("/services/b")
                .is_empty(),
            "canceled watches must leave the prefix index"
        );
        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put after cancel");
    }

    #[test]
    fn active_watch_count_is_bounded_and_cancel_frees_slot() {
        let mut store = InProcessKvStore::new();
        let mut watch_ids = Vec::new();
        for index in 0..MAX_ACTIVE_WATCHES {
            watch_ids.push(
                store
                    .watch_prefix(format!("/watch/{index}/"), KvRevision(1))
                    .expect("watch within limit"),
            );
        }

        assert_eq!(
            store.watch_prefix("/watch/overflow/", KvRevision(1)),
            Err(KvError::WatchLimit {
                max: MAX_ACTIVE_WATCHES,
            })
        );
        assert_eq!(store.cancel_watch(watch_ids[0]), Ok(true));

        let replacement = store
            .watch_prefix("/watch/replacement/", KvRevision(1))
            .expect("watch after cancel");
        assert_eq!(replacement, WatchId(MAX_ACTIVE_WATCHES as u64 + 1));
    }

    #[test]
    fn event_history_is_bounded_and_advances_compacted_revision() {
        let mut store = InProcessKvStore::new();
        for index in 0..(MAX_RETAINED_EVENTS + 10) {
            store
                .put(
                    format!("/events/{index}"),
                    index.to_string().into_bytes(),
                    None,
                )
                .expect("put event");
        }

        let snapshot = store.snapshot().expect("snapshot");
        assert_eq!(snapshot.events.len(), MAX_RETAINED_EVENTS);
        assert_eq!(snapshot.compacted_revision, KvRevision(10));
        assert_eq!(
            snapshot.events.first().expect("first retained").revision,
            KvRevision(11)
        );
        assert!(matches!(
            store.watch_prefix("/events/", KvRevision(10)),
            Err(KvError::Compacted { .. })
        ));
        assert!(store.watch_prefix("/events/", KvRevision(11)).is_ok());
    }

    #[test]
    fn lease_expiration_deletes_attached_keys() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 2,
            })
            .expect("lease");
        store
            .put("/services/live", b"yes".to_vec(), Some(LeaseId(1)))
            .expect("put leased");

        assert!(store.get("/services/live").is_some());
        assert_eq!(store.tick_leases(1).expect("tick"), Vec::<LeaseId>::new());
        assert!(store.get("/services/live").is_some());
        assert_eq!(store.tick_leases(1).expect("expire"), vec![LeaseId(1)]);
        assert!(store.get("/services/live").is_none());
    }

    #[test]
    fn lease_deadline_index_preserves_snapshot_remaining_ticks_and_keepalive() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 10,
            })
            .expect("long lease");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(2),
                ttl_ticks: 2,
            })
            .expect("short lease");
        store
            .put("/lease/short", b"short".to_vec(), Some(LeaseId(2)))
            .expect("leased short");

        assert_eq!(
            store.tick_leases(2).expect("expire short"),
            vec![LeaseId(2)]
        );
        assert!(store.get("/lease/short").is_none());
        let remaining = store
            .snapshot()
            .expect("snapshot")
            .leases
            .into_iter()
            .find(|lease| lease.lease_id == LeaseId(1))
            .expect("long lease")
            .remaining_ticks;
        assert_eq!(
            remaining, 8,
            "snapshot materialization should expose aged remaining ticks without scanning all leases"
        );
        assert!(
            store
                .op_response_if_no_effect(&KvOp::LeaseKeepAlive {
                    lease_id: LeaseId(1)
                })
                .expect("keepalive no-effect check")
                .is_none(),
            "aged leases still need a real keepalive"
        );

        store
            .apply_op(KvOp::LeaseKeepAlive {
                lease_id: LeaseId(1),
            })
            .expect("keepalive");
        let refreshed = store
            .snapshot()
            .expect("snapshot")
            .leases
            .into_iter()
            .find(|lease| lease.lease_id == LeaseId(1))
            .expect("refreshed lease")
            .remaining_ticks;
        assert_eq!(refreshed, 10);
    }

    #[test]
    fn lease_deadline_index_is_restored_from_snapshot_remaining_ticks() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 5,
            })
            .expect("lease");
        store
            .put(
                "/lease/restored-deadline",
                b"value".to_vec(),
                Some(LeaseId(7)),
            )
            .expect("leased value");
        assert_eq!(
            store.tick_leases(2).expect("age lease"),
            Vec::<LeaseId>::new()
        );

        let mut restored =
            InProcessKvStore::restore(store.snapshot().expect("snapshot")).expect("restore");
        assert_eq!(
            restored.tick_leases(2).expect("not expired yet"),
            Vec::<LeaseId>::new()
        );
        assert!(restored.get("/lease/restored-deadline").is_some());
        assert_eq!(restored.tick_leases(1).expect("expire"), vec![LeaseId(7)]);
        assert!(restored.get("/lease/restored-deadline").is_none());
    }

    #[test]
    fn tick_leases_spreads_large_expiration_batches_across_calls() {
        let mut store = InProcessKvStore::new();
        for lease_id in [LeaseId(1), LeaseId(2)] {
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id,
                    ttl_ticks: 1,
                })
                .expect("lease grant");
        }
        put_test_lease_keys(&mut store, "/lease/one/", 3000, LeaseId(1));
        put_test_lease_keys(&mut store, "/lease/two/", 3000, LeaseId(2));
        let revision_before = store.revision();

        assert_eq!(
            store.tick_leases(1).expect("first expiry"),
            vec![LeaseId(1)]
        );
        assert!(!store.leases.contains_key(&LeaseId(1)));
        assert!(store.leases.contains_key(&LeaseId(2)));
        assert!(store.get("/lease/one/00000").is_none());
        assert!(store.get("/lease/two/00000").is_none());
        assert!(store.get("/lease/two/01095").is_none());
        assert!(store.get("/lease/two/01096").is_some());
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );

        assert_eq!(
            store.tick_leases(0).expect("second expiry"),
            vec![LeaseId(2)]
        );
        assert!(!store.leases.contains_key(&LeaseId(2)));
        assert!(
            store
                .range_prefix("/lease/")
                .expect("range prefix")
                .is_empty()
        );
        assert_eq!(store.revision(), KvRevision(revision_before.0 + 6000));
    }

    #[test]
    fn tick_leases_partially_drains_restored_oversized_lease() {
        let lease_id = LeaseId(9);
        let entries = (0..=MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .map(|index| KvEntry {
                key: format!("/lease/restored/{index:05}"),
                value: vec![index as u8],
                create_revision: KvRevision((index + 1) as u64),
                modify_revision: KvRevision((index + 1) as u64),
                version: KvVersion(1),
                lease_id: Some(lease_id),
            })
            .collect::<Vec<_>>();
        let mut store = InProcessKvStore::restore(KvSnapshot {
            revision: KvRevision(entries.len() as u64),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: vec![KvLease {
                lease_id,
                ttl_ticks: 1,
                remaining_ticks: 1,
                granted_at: OffsetDateTime::now_utc(),
            }],
            events: Vec::new(),
        })
        .expect("restore");
        let revision_before = store.revision();

        assert_eq!(
            store.tick_leases(1).expect("partial expiry"),
            Vec::<LeaseId>::new()
        );
        assert!(store.leases.contains_key(&lease_id));
        assert_eq!(
            store
                .lease_keys
                .get(&lease_id)
                .expect("remaining restored lease keys")
                .len(),
            1
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );

        assert_eq!(store.tick_leases(0).expect("finish expiry"), vec![lease_id]);
        assert!(!store.leases.contains_key(&lease_id));
        assert!(
            store
                .range_prefix("/lease/restored/")
                .expect("range prefix")
                .is_empty()
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64 + 1)
        );
    }

    #[test]
    fn lease_revoke_uses_index_and_tracks_reassignment() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 10,
            })
            .expect("lease one");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(2),
                ttl_ticks: 10,
            })
            .expect("lease two");
        store
            .put("/lease/a", b"a".to_vec(), Some(LeaseId(1)))
            .expect("leased a");
        store
            .put("/lease/b", b"b".to_vec(), Some(LeaseId(1)))
            .expect("leased b");
        store
            .put("/lease/a", b"moved".to_vec(), Some(LeaseId(2)))
            .expect("move a to lease two");
        store
            .put("/lease/free", b"free".to_vec(), None)
            .expect("free");

        assert_eq!(
            store
                .lease_keys
                .get(&LeaseId(1))
                .expect("lease one keys")
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["/lease/b".to_string()]
        );
        assert!(
            store
                .lease_keys
                .get(&LeaseId(2))
                .expect("lease two keys")
                .iter()
                .any(|key| key == "/lease/a")
        );

        let response = store
            .apply_op(KvOp::LeaseRevoke {
                lease_id: LeaseId(1),
            })
            .expect("revoke lease one");
        assert_eq!(
            response,
            KvOpResponse::LeaseRevoke {
                lease_id: LeaseId(1),
                deleted_count: 1,
            }
        );
        assert!(store.get("/lease/b").is_none());
        assert!(store.get("/lease/a").is_some());
        assert!(store.get("/lease/free").is_some());
        assert!(!store.lease_keys.contains_key(&LeaseId(1)));
    }

    #[test]
    fn lease_index_is_restored_from_snapshot() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 10,
            })
            .expect("lease");
        store
            .put("/lease/restored", b"value".to_vec(), Some(LeaseId(7)))
            .expect("leased value");

        let mut restored =
            InProcessKvStore::restore(store.snapshot().expect("snapshot")).expect("restore");
        assert!(
            restored
                .lease_keys
                .get(&LeaseId(7))
                .expect("restored lease keys")
                .iter()
                .any(|key| key == "/lease/restored")
        );

        restored
            .apply_op(KvOp::LeaseRevoke {
                lease_id: LeaseId(7),
            })
            .expect("revoke restored lease");
        assert!(restored.get("/lease/restored").is_none());
    }

    #[test]
    fn active_lease_count_is_bounded_and_existing_grants_can_refresh() {
        let mut store = InProcessKvStore::new();
        for index in 1..=MAX_ACTIVE_LEASES {
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id: LeaseId(index as u64),
                    ttl_ticks: 10,
                })
                .expect("lease within limit");
        }

        assert_eq!(
            store.apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(MAX_ACTIVE_LEASES as u64 + 1),
                ttl_ticks: 10,
            }),
            Err(KvError::LeaseLimit {
                max: MAX_ACTIVE_LEASES,
            })
        );
        assert!(
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id: LeaseId(1),
                    ttl_ticks: 20,
                })
                .is_ok(),
            "refreshing an existing lease id should not consume another active slot"
        );
    }

    #[test]
    fn snapshot_restore_preserves_entries_and_revisions() {
        let mut store = InProcessKvStore::new();
        store
            .put("/dfs/workspaces/w1", b"meta".to_vec(), None)
            .expect("put");
        let snapshot = store.snapshot().expect("snapshot");

        let restored = InProcessKvStore::restore(snapshot).expect("restore");

        assert_eq!(restored.revision(), KvRevision(1));
        assert_eq!(
            restored.get("/dfs/workspaces/w1").expect("entry").value,
            b"meta"
        );
    }

    #[test]
    fn materialized_cache_round_trips_snapshot_as_store() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-materialized-cache");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store
            .put("alpha", b"one".to_vec(), None)
            .expect("seed cache");

        assert!(!cache.exists());
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("write cache")
        );
        assert!(cache.exists());
        assert!(
            !cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("skip unchanged cache")
        );
        let restored = cache.read_store().expect("read cache");
        assert_eq!(restored.get("alpha").expect("alpha").value, b"one");
    }

    #[test]
    fn materialized_cache_initial_write_skips_required_snapshot_read() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-materialized-cache-initial-write");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store
            .put("alpha", b"one".to_vec(), None)
            .expect("seed cache");

        reset_required_object_file_reads_for_test(cache.path());
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("initial write")
        );
        assert_eq!(
            required_object_file_read_count_for_test(cache.path()),
            0,
            "initial materialized-cache writes should use the optional snapshot read path, not a required missing-file read"
        );
    }

    #[test]
    fn materialized_cache_optional_snapshot_read_handles_absent_present_and_invalid() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-materialized-optional");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        assert!(
            cache
                .read_snapshot_arc_if_present()
                .expect("absent optional snapshot")
                .is_none()
        );

        let mut store = InProcessKvStore::new();
        store
            .put("alpha", b"one".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&store.snapshot().expect("snapshot"))
            .expect("write materialized snapshot");
        let snapshot = cache
            .read_snapshot_arc_if_present()
            .expect("present optional snapshot")
            .expect("snapshot");
        assert_eq!(snapshot.get("alpha").expect("alpha").value, b"one");

        std::fs::remove_file(cache.path()).expect("remove snapshot");
        std::fs::create_dir(cache.path()).expect("replace snapshot with directory");
        let error = cache
            .read_snapshot_arc_if_present()
            .expect_err("directory is not absence");
        assert!(
            error.to_string().contains("kv-cache.sobj"),
            "present invalid snapshots should stay errors, got {error}"
        );
    }

    #[test]
    fn materialized_cache_canonicalizes_external_snapshot_entries_on_read() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-materialized-canonical-read");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let entry = |key: &str, value: &[u8]| KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let snapshot = KvSnapshot {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            entries: vec![
                entry("beta", b"two"),
                entry("alpha", b"stale"),
                entry("alpha", b"one"),
            ],
            leases: vec![
                KvLease {
                    lease_id: LeaseId(7),
                    ttl_ticks: 10,
                    remaining_ticks: 10,
                    granted_at: OffsetDateTime::UNIX_EPOCH,
                },
                KvLease {
                    lease_id: LeaseId(7),
                    ttl_ticks: 20,
                    remaining_ticks: 20,
                    granted_at: OffsetDateTime::UNIX_EPOCH,
                },
            ],
            events: Vec::new(),
        };
        write_object_file(cache.path(), &snapshot).expect("write external snapshot");

        let read = cache.read_snapshot().expect("read canonical snapshot");
        assert_eq!(
            read.entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert_eq!(read.get("alpha").expect("alpha").value, b"one");
        assert_eq!(read.leases.len(), 1);
        assert_eq!(read.leases[0].ttl_ticks, 20);
    }

    #[test]
    fn materialized_cache_skips_rewrite_on_revision_only_changes() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-durable-skip");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store.put("alpha", b"one".to_vec(), None).expect("seed");
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("initial write")
        );

        // Re-put the identical value: modify_revision/version advance and an event is
        // appended, but the durable key/value state is unchanged -> no rewrite.
        store.put("alpha", b"one".to_vec(), None).expect("re-put");
        let snap = store.snapshot().expect("snapshot");
        assert!(snap.revision.0 > 1, "revision should have advanced");
        assert!(
            !cache
                .write_snapshot_if_changed(&snap)
                .expect("durable-equal skip"),
            "re-putting an identical value must not rewrite the snapshot"
        );

        // A real value change still rewrites.
        store.put("alpha", b"two".to_vec(), None).expect("change");
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("changed write"),
            "a real value change must rewrite the snapshot"
        );
        assert_eq!(
            cache
                .read_store()
                .expect("read")
                .get("alpha")
                .expect("a")
                .value,
            b"two"
        );
    }

    #[test]
    fn materialized_cache_raft_view_read_skips_extra_durable_state_stats() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        let root = temp_raft_root("sinomo-kv-raft-view-single-identity-probe");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let wal_paths = KvWalPaths::new(root.join("raft"));
        let node_id =
            NodeId::from_legacy_named("node_raft_view_single_identity_probe").expect("node id");

        {
            let mut node = DurableRaftKvNode::open_with_materialized_cache(
                node_id.clone(),
                wal_paths.clone(),
                &cache,
            )
            .expect("open durable node");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/raft-view/single-probe".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append command");
            node.commit_to(entry.index).expect("commit command");
        }

        reset_durable_state_path_exists_probes_for_test(&wal_paths);
        let snapshot = cache
            .read_snapshot_with_raft_view(node_id, wal_paths.clone())
            .expect("read raft view");
        assert_eq!(
            snapshot
                .get("/raft-view/single-probe")
                .expect("committed value")
                .value,
            b"value"
        );
        assert_eq!(
            durable_state_path_exists_probe_count_for_test(&wal_paths),
            0,
            "raft-view reads should derive durable-state presence from the identity probe"
        );
    }

    #[test]
    fn materialized_cache_persists_lease_decay() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-lease-decay");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 10,
            })
            .expect("lease");
        store
            .put("/lease/live", b"yes".to_vec(), Some(LeaseId(1)))
            .expect("put leased");
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("initial write")
        );

        store.tick_leases(4).expect("age lease");
        assert_eq!(
            store
                .snapshot()
                .expect("snapshot")
                .leases
                .iter()
                .find(|lease| lease.lease_id == LeaseId(1))
                .expect("aged lease")
                .remaining_ticks,
            6
        );
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("lease decay write"),
            "lease countdown state is restart-visible and must be persisted"
        );

        let restored = cache.read_store().expect("read");
        assert_eq!(
            restored
                .snapshot()
                .expect("snapshot")
                .leases
                .iter()
                .find(|lease| lease.lease_id == LeaseId(1))
                .expect("restored lease")
                .remaining_ticks,
            6
        );
    }

    #[test]
    fn materialized_cache_reports_exact_materialized_snapshot_changes() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-materialized-exact-change");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store.put("alpha", b"one".to_vec(), None).expect("seed");
        let initial = store.snapshot().expect("snapshot");

        assert!(
            cache
                .write_snapshot_if_materialized_changed(&initial)
                .expect("initial write")
        );
        assert!(
            !cache
                .write_snapshot_if_materialized_changed(&initial)
                .expect("skip exact match"),
            "identical materialized snapshots should be skipped"
        );

        store.put("alpha", b"one".to_vec(), None).expect("re-put");
        let event_only_change = store.snapshot().expect("snapshot");
        assert!(
            kv_snapshot_durable_eq(&initial, &event_only_change),
            "test setup should differ only in revision/event metadata"
        );
        assert!(
            cache
                .write_snapshot_if_materialized_changed(&event_only_change)
                .expect("event-only materialized write"),
            "revision/event changes must be reported when callers publish snapshot-backed events"
        );

        let persisted = cache.read_snapshot().expect("read exact snapshot");
        assert_eq!(persisted.revision, event_only_change.revision);
        assert_eq!(persisted.events, event_only_change.events);
    }

    #[test]
    fn materialized_cache_compacts_event_history_to_configured_limit() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-materialized-compact");
        let cache =
            KvMaterializedCache::new(root.join("kv-cache.sobj")).with_max_materialized_events(4);
        let mut store = InProcessKvStore::new();
        for index in 0..20 {
            store
                .put(format!("k{index}"), vec![index as u8], None)
                .expect("seed");
        }
        assert!(store.snapshot().expect("snapshot").events.len() > 4);
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("write cache")
        );
        let persisted = cache.read_snapshot().expect("read cache");
        assert!(
            persisted.events.len() <= 4,
            "persisted event history should be compacted to the limit, got {}",
            persisted.events.len()
        );
        // Entries are preserved even though the event tail is trimmed.
        assert_eq!(persisted.entries.len(), 20);
        // Re-writing the same store is a no-op (compaction is deterministic).
        assert!(
            !cache
                .write_snapshot_if_changed(&store.snapshot().expect("snapshot"))
                .expect("skip unchanged")
        );
    }

    #[test]
    fn materialized_cache_does_not_replace_existing_raft_log() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-cache-does-not-replace-log");
        let node_id = NodeId::from_legacy_named("node_cache_existing_log").expect("node");
        let wal_paths = KvWalPaths::new(root.join("raft"));
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), wal_paths.clone())
                .expect("open raft node");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-log".to_string(),
                    value: b"raft".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append uncommitted");
        }

        let cache = KvMaterializedCache::new(root.join("cache.sobj"));
        let mut cache_store = InProcessKvStore::new();
        cache_store
            .put("/cache", b"cache".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&cache_store.snapshot().expect("snapshot"))
            .expect("write cache");

        let node = DurableRaftKvNode::open_with_materialized_cache(node_id, wal_paths, &cache)
            .expect("open with cache");
        assert_eq!(node.last_log_index(), 1);
        assert!(node.store().get("/cache").is_none());
    }

    #[test]
    fn local_cache_current_snapshot_does_not_initialize_empty_raft_root() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-empty-raft-read-only");
        let node_id = NodeId::from_legacy_named("node_empty_raft_read_only").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let wal_paths = cache.raft_wal_paths(&node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("empty raft root");

        let mut store = InProcessKvStore::new();
        store
            .put("/cached", b"available".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&store.snapshot().expect("snapshot"))
            .expect("write materialized cache");

        let snapshot = cache
            .current_snapshot_arc(node_id)
            .expect("read current snapshot");
        assert_eq!(
            snapshot.get("/cached").expect("cached value").value,
            b"available"
        );
        assert!(!wal_paths.lock.exists(), "read should not create raft lock");
        assert!(
            !wal_paths.hard_state.exists(),
            "read should not create raft hard state"
        );
        assert!(!wal_paths.wal.exists(), "read should not create raft wal");
        assert!(
            !wal_paths.snapshot.exists(),
            "read should not create raft snapshot"
        );
    }

    #[test]
    fn local_cache_empty_snapshot_does_not_initialize_empty_raft_root() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-empty-raft-empty-read-only");
        let node_id = NodeId::from_legacy_named("node_empty_raft_empty_read_only").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let wal_paths = cache.raft_wal_paths(&node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("empty raft root");

        let snapshot = cache
            .current_snapshot_arc(node_id)
            .expect("read empty current snapshot");
        assert_eq!(snapshot.entries.len(), 0);
        assert!(!wal_paths.lock.exists(), "read should not create raft lock");
        assert!(
            !wal_paths.hard_state.exists(),
            "read should not create raft hard state"
        );
        assert!(!wal_paths.wal.exists(), "read should not create raft wal");
        assert!(
            !wal_paths.snapshot.exists(),
            "read should not create raft snapshot"
        );
    }

    #[test]
    fn materialized_cache_raft_view_reports_invalid_snapshot_without_initializing_empty_raft() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-empty-raft-invalid-cache-read-only");
        let node_id = NodeId::from_legacy_named("node_invalid_cache_read_only").expect("node");
        let cache = KvMaterializedCache::new(root.join("cache.sobj"));
        let wal_paths = KvWalPaths::new(root.join("raft"));
        std::fs::create_dir_all(&wal_paths.root).expect("empty raft root");
        std::fs::create_dir_all(cache.path()).expect("invalid materialized cache path");

        let error = cache
            .read_snapshot_with_raft_view(node_id, wal_paths.clone())
            .expect_err("invalid materialized cache should stay an error");

        assert!(
            error.to_string().contains("cache.sobj"),
            "error should identify the invalid materialized cache path, got {error}"
        );
        assert!(!wal_paths.lock.exists(), "read should not create raft lock");
        assert!(
            !wal_paths.hard_state.exists(),
            "read should not create raft hard state"
        );
        assert!(!wal_paths.wal.exists(), "read should not create raft wal");
        assert!(
            !wal_paths.snapshot.exists(),
            "read should not create raft snapshot"
        );
    }

    #[test]
    fn materialized_cache_open_validates_fallback_before_quarantining_raft() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-invalid-fallback-before-quarantine");
        let node_id =
            NodeId::from_legacy_named("node_invalid_fallback_before_quarantine").expect("node");
        let wal_paths = KvWalPaths::new(root.join("raft"));
        std::fs::create_dir_all(&wal_paths.snapshot).expect("invalid raft snapshot");
        let cache = KvMaterializedCache::new(root.join("cache.sobj"));
        std::fs::create_dir_all(cache.path()).expect("invalid materialized cache path");

        let error =
            DurableRaftKvNode::open_with_materialized_cache(node_id, wal_paths.clone(), &cache)
                .expect_err("invalid fallback cache should prevent raft quarantine");

        assert!(
            error.to_string().contains("cache.sobj"),
            "fallback cache should be validated before quarantine, got {error}"
        );
        assert!(
            wal_paths.root.exists(),
            "unreadable raft state should remain in place when fallback cache is invalid"
        );
        let quarantine_prefix = format!(
            "{}.corrupt-{}-",
            wal_paths
                .root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("raft"),
            std::process::id()
        );
        let quarantined = std::fs::read_dir(&root)
            .expect("read root")
            .filter_map(Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(&quarantine_prefix))
            });
        assert!(
            !quarantined,
            "invalid fallback cache must not create a raft quarantine directory"
        );
    }

    #[test]
    fn local_cache_current_store_prefers_raft_and_refreshes_materialized_cache() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache");
        let node_id = NodeId::from_legacy_named("node_local_cache").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-current".to_string(),
                    value: b"current".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append raft value");
            node.commit_to(1).expect("commit raft value");
        }
        let mut stale_store = InProcessKvStore::new();
        stale_store
            .put("/cache-only", b"stale".to_vec(), None)
            .expect("seed stale cache");
        cache
            .write_snapshot(&stale_store.snapshot().expect("snapshot"))
            .expect("write stale cache");

        let current = cache.current_store(node_id).expect("read current store");
        assert!(current.get("/cache-only").is_none());
        assert_eq!(
            current.get("/raft-current").expect("raft value").value,
            b"current"
        );
        let refreshed = cache.cached_store().expect("read refreshed cache");
        assert_eq!(
            refreshed
                .get("/raft-current")
                .expect("cached raft value")
                .value,
            b"current"
        );
    }

    #[test]
    fn local_cache_current_store_falls_back_to_snapshot_when_raft_node_is_busy() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache-busy-fallback");
        let node_id = NodeId::from_legacy_named("node_local_cache_busy").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let mut cached_store = InProcessKvStore::new();
        cached_store
            .put("/cached", b"available".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&cached_store.snapshot().expect("snapshot"))
            .expect("write cache");
        let _held_node = cache
            .open_raft_node(node_id.clone())
            .expect("hold raft lock");

        let current = cache
            .current_store(node_id)
            .expect("busy raft should fall back to materialized snapshot");
        assert_eq!(
            current.get("/cached").expect("cached value").value,
            b"available"
        );
    }

    #[test]
    fn local_cache_current_store_uses_raft_view_cache_when_lock_is_busy() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache-raft-view-cache");
        let node_id = NodeId::from_legacy_named("node_local_cache_raft_view_cache").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-cached".to_string(),
                    value: b"current".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append raft value");
            node.commit_to(1).expect("commit raft value");
        }

        let current = cache
            .current_store(node_id.clone())
            .expect("seed raft view cache");
        assert_eq!(
            current.get("/raft-cached").expect("raft value").value,
            b"current"
        );

        let mut stale_store = InProcessKvStore::new();
        stale_store
            .put("/cache-only", b"stale".to_vec(), None)
            .expect("seed stale cache");
        cache
            .write_snapshot(&stale_store.snapshot().expect("snapshot"))
            .expect("write stale materialized cache");
        let _held_node = cache
            .open_raft_node(node_id.clone())
            .expect("hold raft lock");

        let cached = cache
            .current_store(node_id)
            .expect("busy raft should use validated raft view cache");
        assert!(cached.get("/cache-only").is_none());
        assert_eq!(
            cached.get("/raft-cached").expect("cached raft value").value,
            b"current"
        );
    }

    #[test]
    fn local_cache_point_and_prefix_reads_use_cached_raft_view_when_lock_is_busy() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache-snapshot-read-cache");
        let node_id = NodeId::from_legacy_named("node_local_cache_snapshot_read").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            for (index, (key, value)) in [
                ("/apps/service/a", b"a".as_slice()),
                ("/apps/service/b", b"b".as_slice()),
                ("/other/service/c", b"c".as_slice()),
            ]
            .into_iter()
            .enumerate()
            {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: key.to_string(),
                            value: value.to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append raft value");
                node.commit_to(entry.index)
                    .unwrap_or_else(|error| panic!("commit raft value {index}: {error}"));
            }
        }

        let first = cache
            .current_get(node_id.clone(), "/apps/service/a")
            .expect("seed raft view cache")
            .expect("raft value");
        assert_eq!(first.value, b"a");

        let mut stale_store = InProcessKvStore::new();
        stale_store
            .put("/apps/service/stale", b"stale".to_vec(), None)
            .expect("seed stale cache");
        cache
            .write_snapshot(&stale_store.snapshot().expect("snapshot"))
            .expect("write stale materialized cache");
        let _held_node = cache
            .open_raft_node(node_id.clone())
            .expect("hold raft lock");

        assert_eq!(
            cache
                .current_get(node_id.clone(), "/apps/service/b")
                .expect("cached point read")
                .expect("raft value")
                .value,
            b"b"
        );
        assert!(
            cache
                .current_get(node_id.clone(), "/apps/service/stale")
                .expect("cached point read")
                .is_none(),
            "point reads must not fall back to stale materialized snapshots when a valid raft view is cached"
        );
        assert_eq!(
            cache
                .current_range_prefix(node_id, "/apps/service/")
                .expect("cached prefix read")
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );
    }

    #[test]
    fn local_cache_raft_view_cache_invalidates_after_wal_change() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache-raft-view-invalidate");
        let node_id =
            NodeId::from_legacy_named("node_local_cache_raft_view_invalidate").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            let first = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/raft/first".to_string(),
                        value: b"first".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append first");
            node.commit_to(first.index).expect("commit first");
        }
        let first_view = cache
            .current_store(node_id.clone())
            .expect("seed raft view cache");
        assert!(first_view.get("/raft/first").is_some());
        assert!(first_view.get("/raft/second").is_none());

        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("reopen raft");
            let second = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/raft/second".to_string(),
                        value: b"second".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append second");
            node.commit_to(second.index).expect("commit second");
        }

        let refreshed = cache
            .current_store(node_id)
            .expect("read after wal changed");
        assert!(refreshed.get("/raft/first").is_some());
        assert!(refreshed.get("/raft/second").is_some());
    }

    #[test]
    fn durable_raft_node_open_waits_for_transient_lock_holder() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-raft-lock-wait");
        let node_id = NodeId::from_legacy_named("node_raft_lock_wait").expect("node");
        let paths = KvWalPaths::new(root.join("raft"));
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let holder_node_id = node_id.clone();
        let holder_paths = paths.clone();
        let holder = std::thread::spawn(move || {
            let _node =
                DurableRaftKvNode::open(holder_node_id, holder_paths).expect("hold raft node");
            held_tx.send(()).expect("notify lock held");
            std::thread::sleep(Duration::from_millis(100));
        });
        held_rx.recv().expect("wait for holder");

        let node = DurableRaftKvNode::open(node_id, paths).expect("wait for raft node lock");
        assert_eq!(node.last_log_index(), 0);
        holder.join().expect("join holder");
    }

    #[test]
    fn raft_node_lock_retry_delay_backs_off_and_caps() {
        assert_eq!(raft_node_lock_retry_delay(0), RAFT_NODE_LOCK_RETRY_INITIAL);
        assert_eq!(
            raft_node_lock_retry_delay(1),
            RAFT_NODE_LOCK_RETRY_INITIAL * 2
        );
        assert_eq!(
            raft_node_lock_retry_delay(4),
            RAFT_NODE_LOCK_RETRY_INITIAL * 16
        );
        assert_eq!(raft_node_lock_retry_delay(5), RAFT_NODE_LOCK_RETRY_MAX);
        assert_eq!(raft_node_lock_retry_delay(32), RAFT_NODE_LOCK_RETRY_MAX);
    }

    #[test]
    fn local_cache_reads_entries_without_exposing_snapshot_files() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache-read-interface");
        let node_id = NodeId::from_legacy_named("node_local_cache_read").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let mut store = InProcessKvStore::new();
        store
            .put_serde("/dfs/services/content/a", &"node-a", None)
            .expect("put a");
        store
            .put_serde("/dfs/services/content/b", &"node-b", None)
            .expect("put b");
        cache
            .write_snapshot(&store.snapshot().expect("snapshot"))
            .expect("write cache snapshot");

        assert_eq!(
            cache
                .current_get_serde::<String>(node_id.clone(), "/dfs/services/content/a")
                .expect("read value"),
            Some("node-a".to_string())
        );
        assert_eq!(
            cache
                .current_range_prefix_serde::<String>(node_id, "/dfs/services/content/")
                .expect("range values"),
            vec!["node-a".to_string(), "node-b".to_string()]
        );
    }

    #[test]
    fn local_cache_typed_prefix_reads_are_bounded() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-local-cache-typed-range-limit");
        let node_id =
            NodeId::from_legacy_named("node_local_cache_typed_range_limit").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let mut store = InProcessKvStore::new();
        for key in [
            "/dfs/services/content/a",
            "/dfs/services/content/b",
            "/dfs/services/content/c",
        ] {
            store.put_serde(key, &key.to_string(), None).expect("put");
        }
        cache
            .write_snapshot(&store.snapshot().expect("snapshot"))
            .expect("write cache snapshot");

        let error = cache
            .current_range_prefix_serde_limited_by_payload_bytes::<String>(
                node_id.clone(),
                "/dfs/services/content/",
                2,
                usize::MAX,
            )
            .expect_err("entry limit");
        assert_eq!(
            error,
            KvError::RangeLimit {
                prefix: "/dfs/services/content/".to_string(),
                max_entries: 2,
            }
        );

        let error = cache
            .current_range_prefix_serde_limited_by_payload_bytes::<String>(
                node_id,
                "/dfs/services/content/",
                3,
                1,
            )
            .expect_err("payload limit");
        assert!(matches!(
            error,
            KvError::RangePayloadLimit {
                ref prefix,
                max_bytes: 1,
                ..
            } if prefix == "/dfs/services/content/"
        ));
    }

    #[test]
    fn compacted_revision_rejects_old_watches() {
        let mut store = InProcessKvStore::new();
        store.put("/a", b"1".to_vec(), None).expect("put");
        store.compact(KvRevision(1)).expect("compact");

        let error = store
            .watch_prefix("/", KvRevision(1))
            .expect_err("compacted watch");
        assert!(matches!(error, KvError::Compacted { .. }));
    }

    #[test]
    fn raft_cluster_replicates_committed_writes_to_online_voters() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");

        cluster.put("/cluster/meta", b"ok".to_vec()).expect("put");

        assert_eq!(
            cluster
                .store(&node_b)
                .expect("store")
                .get("/cluster/meta")
                .expect("entry")
                .value,
            b"ok"
        );
        assert_eq!(
            cluster.store(&node_c).expect("store").revision(),
            KvRevision(1)
        );
    }

    #[test]
    fn raft_cluster_rejects_writes_without_quorum() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster.set_node_online(&node_b, false).unwrap();
        cluster.set_node_online(&node_c, false).unwrap();

        let error = cluster
            .put("/cluster/meta", b"no".to_vec())
            .expect_err("no quorum");

        assert!(matches!(error, KvError::QuorumUnavailable { .. }));
    }

    #[derive(Debug)]
    enum QuorumCommitTestError {
        Kv(KvError),
        ReadOnly(String),
    }

    impl std::fmt::Display for QuorumCommitTestError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Kv(error) => write!(formatter, "{error}"),
                Self::ReadOnly(detail) => write!(formatter, "cluster is read-only: {detail}"),
            }
        }
    }

    impl From<KvError> for QuorumCommitTestError {
        fn from(error: KvError) -> Self {
            Self::Kv(error)
        }
    }

    fn quorum_commit_test_state(local: &NodeId, remote: &NodeId) -> ClusterBootstrapState {
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_kv_quorum_driver").expect("cluster"),
            "kv quorum driver",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let main_capability =
            sinomo_core::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY.to_string())
                .expect("main daemon capability");
        state.members[0].role = ClusterMemberRole::Voter;
        state.members[0].state = ClusterMemberState::Voter;
        state.members[0].capabilities = vec![main_capability.clone()];
        state.members[0].allowed_capabilities = vec![main_capability.clone()];
        let mut remote_member = state.members[0].clone();
        remote_member.node_id = remote.clone();
        remote_member.display_name = "remote".to_string();
        remote_member.node_public_key = "pk-remote".to_string();
        remote_member.joined_at = now;
        remote_member.updated_at = now;
        state.members.push(remote_member);
        state
    }

    #[derive(Default)]
    struct QuorumCommitTestHost {
        targets: Vec<NodeId>,
        accepting_targets: BTreeSet<NodeId>,
        appended: Option<KvLogEntry>,
        committed_indices: Vec<u64>,
        discarded: bool,
        replicated_targets: Vec<NodeId>,
        statuses: Vec<(NodeId, ClusterKvRaftTargetReplicationStatus)>,
        fanout_targets: Vec<NodeId>,
    }

    impl ClusterKvRaftCommandStorage for QuorumCommitTestHost {
        type Error = QuorumCommitTestError;

        fn append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self,
            command: KvCommand,
        ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error> {
            let entry = KvLogEntry {
                index: 7,
                term: 1,
                command,
                revision: KvRevision(1),
            };
            self.appended = Some(entry.clone());
            Ok(ClusterKvRaftUncommittedAppend::Appended(entry))
        }

        fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self,
        ) -> Result<(), Self::Error> {
            self.discarded = true;
            Ok(())
        }

        fn commit_cluster_raft_index_with_events(
            &mut self,
            index: u64,
        ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
            self.committed_indices.push(index);
            let mut store = InProcessKvStore::new();
            if let Some(KvLogEntry {
                command:
                    KvCommand::Op(KvOp::Put {
                        key,
                        value,
                        lease_id,
                    }),
                ..
            }) = self.appended.clone()
            {
                store.put(key, value, lease_id)?;
            }
            Ok(ClusterKvRaftCommitResult {
                snapshot: store.snapshot().expect("snapshot"),
                txn_response: None,
                events: vec![test_kv_put_event(1, "/kv/quorum/fanout", b"ok")],
            })
        }
    }

    impl ClusterKvRaftQuorumCommitHost for QuorumCommitTestHost {
        type Target = NodeId;

        fn remote_voter_targets(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<Vec<Self::Target>, Self::Error> {
            Ok(self.targets.clone())
        }

        fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
            target
        }

        fn replicate_cluster_raft_to_target(
            &mut self,
            state: &ClusterBootstrapState,
            target: &Self::Target,
            _timeout: Duration,
        ) -> Result<ClusterRaftAppendResponse, Self::Error> {
            self.replicated_targets.push(target.clone());
            Ok(ClusterRaftAppendResponse {
                accepted: self.accepting_targets.contains(target),
                detail: "test append".to_string(),
                state: state.clone(),
                append: RaftAppendEntriesResponse {
                    term: 1,
                    success: self.accepting_targets.contains(target),
                    match_index: 7,
                },
            })
        }

        fn record_target_replication_status(
            &mut self,
            _state: &ClusterBootstrapState,
            target: &Self::Target,
            status: ClusterKvRaftTargetReplicationStatus,
            _latency_ms: Option<u64>,
        ) -> Result<(), Self::Error> {
            self.statuses.push((target.clone(), status));
            Ok(())
        }

        fn fan_out_committed_cluster_raft_command(
            &mut self,
            _state: ClusterBootstrapState,
            _events: Vec<KvEvent>,
            targets: Vec<Self::Target>,
        ) -> Result<(), Self::Error> {
            self.fanout_targets = targets;
            Ok(())
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    impl ClusterKvRaftQuorumCommitAsyncHost for QuorumCommitTestHost {
        type Error = QuorumCommitTestError;
        type Target = NodeId;

        fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
            &'a mut self,
            command: KvCommand,
        ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftUncommittedAppend, Self::Error> {
            Box::pin(async move {
                ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
                    self, command,
                )
            })
        }

        fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
            &'a mut self,
        ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
            Box::pin(async move {
                ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
                    self,
                )
            })
        }

        fn commit_cluster_raft_index_with_events_async<'a>(
            &'a mut self,
            index: u64,
        ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCommitResult, Self::Error> {
            Box::pin(async move {
                ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(self, index)
            })
        }

        fn remote_voter_targets_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
        ) -> ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, Self::Error> {
            Box::pin(async move { Ok(self.targets.clone()) })
        }

        fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
            target
        }

        fn replicate_cluster_raft_to_target_async<'a>(
            &'a mut self,
            state: &'a ClusterBootstrapState,
            target: &'a Self::Target,
            _timeout: Duration,
        ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
            Box::pin(async move {
                self.replicated_targets.push(target.clone());
                Ok(ClusterRaftAppendResponse {
                    accepted: self.accepting_targets.contains(target),
                    detail: "test append".to_string(),
                    state: state.clone(),
                    append: RaftAppendEntriesResponse {
                        term: 1,
                        success: self.accepting_targets.contains(target),
                        match_index: 7,
                    },
                })
            })
        }

        fn record_target_replication_status_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            target: &'a Self::Target,
            status: ClusterKvRaftTargetReplicationStatus,
            _latency_ms: Option<u64>,
        ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.statuses.push((target.clone(), status));
                Ok(())
            })
        }

        fn fan_out_committed_cluster_raft_command_async<'a>(
            &'a mut self,
            _state: ClusterBootstrapState,
            _events: Vec<KvEvent>,
            targets: Vec<Self::Target>,
        ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.fanout_targets = targets;
                Ok(())
            })
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    #[derive(Default)]
    struct TxnQuorumDriverTestHost {
        local_leader: bool,
        forwarded: Option<ClusterKvTxnCommitResult>,
        committed_txns: usize,
        elected: bool,
        reachable_voters: usize,
    }

    impl ClusterKvTxnQuorumHost for TxnQuorumDriverTestHost {
        type Error = QuorumCommitTestError;

        fn local_is_raft_leader(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<bool, Self::Error> {
            Ok(self.local_leader)
        }

        fn forward_cluster_kv_txn_to_voter(
            &mut self,
            _state: &ClusterBootstrapState,
            _txn: &KvTxn,
        ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error> {
            Ok(self.forwarded.clone())
        }

        fn elect_cluster_raft_leader(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<bool, Self::Error> {
            self.elected = true;
            self.local_leader = true;
            Ok(true)
        }

        fn reachable_voter_count(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<usize, Self::Error> {
            Ok(self.reachable_voters)
        }

        fn semantic_txn_no_effect_response(
            &mut self,
            _txn: &KvTxn,
        ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error> {
            Ok(None)
        }

        fn commit_cluster_raft_txn_with_quorum(
            &mut self,
            _state: &mut ClusterBootstrapState,
            txn: KvTxn,
        ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error> {
            self.committed_txns = self.committed_txns.saturating_add(1);
            let mut store = InProcessKvStore::new();
            let txn_response = store.txn(txn)?;
            Ok(ClusterKvRaftQuorumCommitResult {
                snapshot: store.snapshot().expect("snapshot"),
                txn_response: Some(txn_response),
            })
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    /// Storage-backed quorum-commit host mirroring the production hosts: a
    /// shared on-disk raft store plus the commit-sequence lock. Used to prove
    /// concurrent committers no longer clobber each other's uncommitted
    /// entries (each append used to discard the other committer's tail, so the
    /// first commit step failed with `missing committed raft entry` and the
    /// publishers livelocked retrying).
    struct RacingCommitTestHost {
        kv_paths: ClusterKvPaths,
        node_id: NodeId,
    }

    impl ClusterKvRaftCommandStorage for RacingCommitTestHost {
        type Error = QuorumCommitTestError;

        fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<File>, Self::Error> {
            lock_cluster_kv_commit_sequence(&self.kv_paths, CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT)
                .map(Some)
                .map_err(Into::into)
        }

        fn append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self,
            command: KvCommand,
        ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error> {
            self.kv_paths
                .raft_store()
                .locked_append_command_uncommitted_or_no_effect(&self.node_id, command)
                .map_err(Into::into)
        }

        fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self,
        ) -> Result<(), Self::Error> {
            self.kv_paths
                .raft_store()
                .discard_uncommitted_tail_and_clear_leader(&self.node_id)
                .map(|_| ())
                .map_err(Into::into)
        }

        fn commit_cluster_raft_index_with_events(
            &mut self,
            index: u64,
        ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
            self.kv_paths
                .raft_store()
                .commit_index_with_events(&self.node_id, index)
                .map_err(Into::into)
        }
    }

    impl ClusterKvRaftQuorumCommitHost for RacingCommitTestHost {
        type Target = NodeId;

        fn remote_voter_targets(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<Vec<Self::Target>, Self::Error> {
            Ok(Vec::new())
        }

        fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
            target
        }

        fn replicate_cluster_raft_to_target(
            &mut self,
            state: &ClusterBootstrapState,
            _target: &Self::Target,
            _timeout: Duration,
        ) -> Result<ClusterRaftAppendResponse, Self::Error> {
            Ok(ClusterRaftAppendResponse {
                accepted: false,
                detail: "no remote voters in this test".to_string(),
                state: state.clone(),
                append: RaftAppendEntriesResponse {
                    term: 1,
                    success: false,
                    match_index: 0,
                },
            })
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    #[test]
    fn become_leader_refuses_terms_below_observed() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-become-leader-stale-term");
        let node_id = NodeId::from_legacy_named("node_become_leader_stale").expect("node");
        let mut node =
            DurableRaftKvNode::open(node_id, KvWalPaths::new(root.join("raft"))).expect("open");
        assert!(node.observe_term(10).expect("observe term"));
        assert!(matches!(
            node.become_leader(5),
            Err(KvError::StaleLeadershipTerm {
                term: 5,
                current_term: 10
            })
        ));
        node.become_leader(10)
            .expect("become leader at observed term");
        assert_eq!(node.hard_state().current_term, 10);
    }

    #[test]
    fn quorum_commit_sequence_lock_serializes_concurrent_committers() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-commit-sequence-race");
        let kv_paths = ClusterKvPaths::new(&root);
        let local = NodeId::from_legacy_named("node_commit_sequence_race").expect("local");
        let remote = NodeId::from_legacy_named("node_commit_sequence_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        state.members.truncate(1); // single-voter cluster: quorum of one, no remote targets

        const COMMITS_PER_THREAD: u64 = 20;
        let threads = (0..2)
            .map(|writer| {
                let kv_paths = kv_paths.clone();
                let node_id = local.clone();
                let state = state.clone();
                std::thread::spawn(move || {
                    let mut host = RacingCommitTestHost { kv_paths, node_id };
                    let mut state = state;
                    for index in 0..COMMITS_PER_THREAD {
                        commit_cluster_kv_raft_command_with_quorum(
                            &mut state,
                            KvCommand::Op(KvOp::Put {
                                key: format!("/kv/race/{writer}/{index}"),
                                value: b"committed".to_vec(),
                                lease_id: None,
                            }),
                            ClusterKvRaftQuorumCommitOptions {
                                timeout: Duration::from_secs(5),
                            },
                            &mut host,
                        )
                        .unwrap_or_else(|error| {
                            panic!("writer {writer} commit {index} failed: {error}")
                        });
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().expect("writer thread");
        }

        let node = kv_paths
            .raft_store()
            .open_node(&local)
            .expect("reopen node");
        let total = 2 * COMMITS_PER_THREAD;
        assert_eq!(node.applied_index(), total);
        assert_eq!(node.last_log_index(), total);
        let snapshot = node.store().snapshot().expect("snapshot");
        for writer in 0..2 {
            for index in 0..COMMITS_PER_THREAD {
                assert!(
                    snapshot
                        .get(&format!("/kv/race/{writer}/{index}"))
                        .is_some(),
                    "missing committed key /kv/race/{writer}/{index}"
                );
            }
        }
    }

    #[test]
    fn kv_txn_quorum_driver_commits_when_local_voter_is_leader() {
        let local = NodeId::from_legacy_named("node_txn_driver_local").expect("local");
        let remote = NodeId::from_legacy_named("node_txn_driver_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = TxnQuorumDriverTestHost {
            local_leader: true,
            reachable_voters: 2,
            ..TxnQuorumDriverTestHost::default()
        };

        let result = commit_cluster_kv_txn_with_quorum_with_host(
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/kv/txn-driver/local".to_string(),
                    value: b"committed".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
            ClusterKvTxnQuorumDriverOptions {
                allow_forwarding: false,
            },
            &mut host,
        )
        .expect("commit local leader txn");

        assert_eq!(host.committed_txns, 1);
        assert!(!host.elected);
        assert_eq!(
            result
                .snapshot
                .get("/kv/txn-driver/local")
                .expect("committed key")
                .value,
            b"committed".to_vec()
        );
        assert_eq!(
            result.txn_response.expect("txn response").revision,
            result.snapshot.revision
        );
    }

    #[test]
    fn kv_txn_quorum_driver_forwards_when_local_node_is_not_voter() {
        let local = NodeId::from_legacy_named("node_txn_driver_learner").expect("local");
        let remote = NodeId::from_legacy_named("node_txn_driver_voter").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        state.members[0].role = ClusterMemberRole::Learner;
        state.members[0].state = ClusterMemberState::Learner;
        let mut store = InProcessKvStore::new();
        let txn_response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/kv/txn-driver/forwarded".to_string(),
                    value: b"forwarded".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect("seed forwarded response");
        let forwarded = ClusterKvTxnCommitResult {
            snapshot: store.snapshot().expect("snapshot"),
            txn_response: Some(txn_response),
        };
        let mut host = TxnQuorumDriverTestHost {
            forwarded: Some(forwarded.clone()),
            reachable_voters: 1,
            ..TxnQuorumDriverTestHost::default()
        };

        let result = commit_cluster_kv_txn_with_quorum_with_host(
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/kv/txn-driver/forwarded".to_string(),
                    value: b"forwarded".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
            ClusterKvTxnQuorumDriverOptions {
                allow_forwarding: true,
            },
            &mut host,
        )
        .expect("forward learner txn");

        assert_eq!(result, forwarded);
        assert_eq!(host.committed_txns, 0);
        assert!(!host.elected);
    }

    struct AppendReplicationTestHost {
        last_index: u64,
        requested_next_indices: Vec<u64>,
        responses: VecDeque<ClusterRaftAppendResponse>,
        sent_count: usize,
    }

    impl ClusterKvRaftAppendReplicationHost for AppendReplicationTestHost {
        type Error = QuorumCommitTestError;
        type Target = NodeId;

        fn last_local_raft_log_index(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<u64, Self::Error> {
            Ok(self.last_index)
        }

        fn build_raft_append_request(
            &mut self,
            state: &ClusterBootstrapState,
            next_index: u64,
        ) -> Result<ClusterRaftAppendRequest, Self::Error> {
            self.requested_next_indices.push(next_index);
            Ok(ClusterRaftAppendRequest {
                state: state.clone(),
                append: RaftAppendEntriesRequest {
                    term: 1,
                    leader_id: state.local_node_id.clone(),
                    prev_log_index: next_index.saturating_sub(1),
                    prev_log_term: 1,
                    entries: Vec::new(),
                    leader_commit: next_index.saturating_sub(1),
                },
                snapshot: Some(RaftSnapshot {
                    last_included_index: next_index.saturating_sub(1),
                    last_included_term: 1,
                    kv: test_kv_snapshot_with_events(Vec::new()),
                }),
            })
        }

        fn append_attempt_timeout(
            &mut self,
            _target: &Self::Target,
            remaining: Duration,
            _request: &ClusterRaftAppendRequest,
        ) -> Duration {
            remaining.min(Duration::from_secs(1))
        }

        fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
            self.sent_count = self.sent_count.saturating_add(1);
            Ok(())
        }

        fn send_raft_append(
            &mut self,
            _target: &Self::Target,
            _request: &ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> Result<ClusterRaftAppendResponse, Self::Error> {
            self.responses
                .pop_front()
                .ok_or_else(|| QuorumCommitTestError::ReadOnly("missing append response".into()))
        }
    }

    fn test_cluster_raft_append_response(
        state: &ClusterBootstrapState,
        accepted: bool,
        match_index: u64,
    ) -> ClusterRaftAppendResponse {
        ClusterRaftAppendResponse {
            accepted,
            detail: "test append".to_string(),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 1,
                success: accepted,
                match_index,
            },
        }
    }

    #[test]
    fn cluster_kv_raft_append_replication_retries_rejected_next_index_in_kv() {
        let local = NodeId::from_legacy_named("node_kv_append_replication_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_kv_append_replication_remote").expect("remote");
        let state = quorum_commit_test_state(&local, &remote);
        let watch_event = test_kv_put_event(1, "/kv/append/watch", b"ok");
        let mut host = AppendReplicationTestHost {
            last_index: 5,
            requested_next_indices: Vec::new(),
            responses: VecDeque::from([
                test_cluster_raft_append_response(&state, false, 2),
                test_cluster_raft_append_response(&state, true, 5),
            ]),
            sent_count: 0,
        };

        let outcome = replicate_cluster_kv_raft_to_target_with_host(
            &state,
            &remote,
            &[watch_event],
            ClusterKvRaftAppendReplicationOptions {
                max_attempts: 3,
                timeout: Duration::from_secs(5),
            },
            &mut host,
        )
        .expect("append replication");

        assert!(outcome.response.accepted);
        assert!(outcome.watch_event_fanout_needed);
        assert_eq!(host.requested_next_indices, vec![6, 3]);
        assert_eq!(host.sent_count, 2);
    }

    #[test]
    fn cluster_kv_raft_quorum_driver_commits_after_remote_voter_ack() {
        let local = NodeId::from_legacy_named("node_kv_quorum_driver_local").expect("local");
        let remote = NodeId::from_legacy_named("node_kv_quorum_driver_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            accepting_targets: [remote.clone()].into_iter().collect(),
            ..QuorumCommitTestHost::default()
        };

        let result = commit_cluster_kv_raft_command_with_quorum(
            &mut state,
            KvCommand::Op(KvOp::Put {
                key: "/kv/quorum/committed".to_string(),
                value: b"ok".to_vec(),
                lease_id: None,
            }),
            ClusterKvRaftQuorumCommitOptions {
                timeout: Duration::from_secs(1),
            },
            &mut host,
        )
        .expect("quorum commit");

        assert_eq!(
            result
                .snapshot
                .get("/kv/quorum/committed")
                .expect("committed value")
                .value,
            b"ok".to_vec()
        );
        assert_eq!(host.committed_indices, vec![7]);
        assert_eq!(host.replicated_targets, vec![remote.clone()]);
        assert_eq!(
            host.statuses,
            vec![(
                remote.clone(),
                ClusterKvRaftTargetReplicationStatus::Accepted
            )]
        );
        assert_eq!(host.fanout_targets, vec![remote]);
        assert!(!host.discarded);
    }

    #[test]
    fn cluster_kv_raft_quorum_async_driver_commits_after_remote_voter_ack() {
        let local = NodeId::from_legacy_named("node_kv_quorum_async_driver_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_kv_quorum_async_driver_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            accepting_targets: [remote.clone()].into_iter().collect(),
            ..QuorumCommitTestHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let result = runtime
            .block_on(commit_cluster_kv_raft_command_with_quorum_async(
                &mut state,
                KvCommand::Op(KvOp::Put {
                    key: "/kv/quorum/async-committed".to_string(),
                    value: b"ok".to_vec(),
                    lease_id: None,
                }),
                ClusterKvRaftQuorumCommitOptions {
                    timeout: Duration::from_secs(1),
                },
                &mut host,
            ))
            .expect("async quorum commit");

        assert_eq!(
            result
                .snapshot
                .get("/kv/quorum/async-committed")
                .expect("committed value")
                .value,
            b"ok".to_vec()
        );
        assert_eq!(host.committed_indices, vec![7]);
        assert_eq!(host.replicated_targets, vec![remote.clone()]);
        assert_eq!(
            host.statuses,
            vec![(
                remote.clone(),
                ClusterKvRaftTargetReplicationStatus::Accepted
            )]
        );
        assert_eq!(host.fanout_targets, vec![remote]);
        assert!(!host.discarded);
    }

    #[test]
    fn cluster_kv_raft_quorum_driver_discards_uncommitted_entry_without_quorum() {
        let local =
            NodeId::from_legacy_named("node_kv_quorum_driver_no_quorum_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_kv_quorum_driver_no_quorum_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            ..QuorumCommitTestHost::default()
        };

        let error = commit_cluster_kv_raft_command_with_quorum(
            &mut state,
            KvCommand::Op(KvOp::Put {
                key: "/kv/quorum/rejected".to_string(),
                value: b"no".to_vec(),
                lease_id: None,
            }),
            ClusterKvRaftQuorumCommitOptions {
                timeout: Duration::from_secs(1),
            },
            &mut host,
        )
        .expect_err("no quorum");

        assert!(matches!(error, QuorumCommitTestError::ReadOnly(_)));
        assert!(host.discarded);
        assert!(host.committed_indices.is_empty());
        assert_eq!(
            host.statuses,
            vec![(remote, ClusterKvRaftTargetReplicationStatus::Rejected)]
        );
    }

    #[test]
    fn cluster_kv_raft_quorum_async_driver_discards_uncommitted_entry_without_quorum() {
        let local = NodeId::from_legacy_named("node_kv_quorum_async_driver_no_quorum_local")
            .expect("local");
        let remote = NodeId::from_legacy_named("node_kv_quorum_async_driver_no_quorum_remote")
            .expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            ..QuorumCommitTestHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let error = runtime
            .block_on(commit_cluster_kv_raft_command_with_quorum_async(
                &mut state,
                KvCommand::Op(KvOp::Put {
                    key: "/kv/quorum/async-rejected".to_string(),
                    value: b"no".to_vec(),
                    lease_id: None,
                }),
                ClusterKvRaftQuorumCommitOptions {
                    timeout: Duration::from_secs(1),
                },
                &mut host,
            ))
            .expect_err("no quorum");

        assert!(matches!(error, QuorumCommitTestError::ReadOnly(_)));
        assert!(host.discarded);
        assert!(host.committed_indices.is_empty());
        assert_eq!(
            host.statuses,
            vec![(remote, ClusterKvRaftTargetReplicationStatus::Rejected)]
        );
    }

    #[test]
    fn learner_can_catch_up_from_snapshot() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let learner = NodeId::from_legacy_named("node_learner").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone()]).expect("cluster");
        cluster.put("/policy/a", b"allow".to_vec()).expect("put");
        let snapshot = cluster
            .store(&node_a)
            .expect("leader store")
            .snapshot()
            .expect("snapshot");

        cluster
            .add_learner_from_snapshot(learner.clone(), snapshot)
            .expect("learner");

        assert_eq!(
            cluster
                .store(&learner)
                .expect("learner store")
                .get("/policy/a")
                .expect("entry")
                .value,
            b"allow"
        );
    }

    #[test]
    fn raft_cluster_catches_up_partitioned_voter_from_committed_log() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");

        cluster.set_node_online(&node_c, false).unwrap();
        cluster.put("/config/a", b"one".to_vec()).expect("put");
        assert!(
            cluster
                .store(&node_c)
                .expect("partitioned store")
                .get("/config/a")
                .is_none()
        );

        cluster.set_node_online(&node_c, true).unwrap();

        assert_eq!(
            cluster
                .store(&node_c)
                .expect("caught up store")
                .get("/config/a")
                .expect("entry")
                .value,
            b"one"
        );
        assert_eq!(
            cluster
                .hard_state(&node_c)
                .expect("hard state")
                .commit_index,
            cluster.commit_index()
        );
    }

    #[test]
    fn raft_cluster_partition_blocks_isolated_leader_and_recovers_after_majority_leader_change() {
        let node_a = NodeId::from_legacy_named("node_partition_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_partition_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_partition_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster
            .put("/config/epoch", b"one".to_vec())
            .expect("initial put");

        cluster.set_node_online(&node_a, false).unwrap();
        let isolated_write = cluster.put("/config/minority", b"blocked".to_vec());
        assert!(matches!(
            isolated_write,
            Err(KvError::LeaderOffline { leader_id }) if leader_id == node_a
        ));

        cluster
            .transfer_leader(node_b.clone())
            .expect("majority leader transfer");
        cluster
            .put("/config/majority", b"committed".to_vec())
            .expect("majority write");
        assert!(
            cluster
                .store(&node_a)
                .expect("isolated old leader")
                .get("/config/majority")
                .is_none()
        );

        cluster.set_node_online(&node_a, true).unwrap();

        assert_eq!(
            cluster
                .store(&node_a)
                .expect("healed old leader")
                .get("/config/majority")
                .expect("caught up majority write")
                .value,
            b"committed"
        );
        assert_eq!(
            cluster
                .hard_state(&node_a)
                .expect("old leader hard state")
                .commit_index,
            cluster.commit_index()
        );
    }

    #[test]
    fn raft_cluster_autonomous_election_replaces_lost_leader_with_majority_quorum() {
        let node_a = NodeId::from_legacy_named("node_elect_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_elect_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_elect_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster
            .put("/config/a", b"one".to_vec())
            .expect("initial write");

        cluster.set_node_online(&node_a, false).unwrap();

        assert!(
            cluster
                .elect_leader(node_b.clone())
                .expect("elect majority leader")
        );
        assert_eq!(cluster.leader_id(), &node_b);
        assert_eq!(
            cluster
                .hard_state(&node_b)
                .expect("new leader hard state")
                .leader_id
                .as_ref(),
            Some(&node_b)
        );
        cluster
            .put("/config/b", b"two".to_vec())
            .expect("write through elected leader");
        assert!(
            cluster
                .store(&node_a)
                .expect("offline old leader")
                .get("/config/b")
                .is_none()
        );

        cluster.set_node_online(&node_a, true).unwrap();

        assert_eq!(
            cluster
                .store(&node_a)
                .expect("healed old leader")
                .get("/config/b")
                .expect("caught up write")
                .value,
            b"two"
        );
    }

    #[test]
    fn raft_cluster_autonomous_election_fails_without_majority() {
        let node_a = NodeId::from_legacy_named("node_elect_fail_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_elect_fail_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_elect_fail_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster.set_node_online(&node_a, false).unwrap();
        cluster.set_node_online(&node_c, false).unwrap();

        assert!(
            !cluster
                .elect_leader(node_b.clone())
                .expect("minority election")
        );
        assert_ne!(cluster.leader_id(), &node_b);
        assert!(matches!(
            cluster.put("/config/blocked", b"blocked".to_vec()),
            Err(KvError::LeaderOffline { .. })
        ));
    }

    #[test]
    fn cluster_kv_raft_leader_election_driver_sets_leader_after_remote_quorum() {
        struct RecordingElectionHost {
            sent_votes: Vec<(usize, NodeId, u64)>,
            observed_terms: Vec<u64>,
            leader_terms: Vec<u64>,
        }

        impl ClusterKvRaftLeaderElectionHost for RecordingElectionHost {
            type Error = KvError;

            fn local_vote_metadata(
                &mut self,
            ) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error> {
                Ok(ClusterKvRaftLocalVoteMetadata {
                    current_term: 1,
                    last_log_index: 7,
                    last_log_term: 1,
                })
            }

            fn apply_local_vote(
                &mut self,
                vote: RaftRequestVoteRequest,
            ) -> Result<RaftRequestVoteResponse, Self::Error> {
                Ok(RaftRequestVoteResponse {
                    term: vote.term,
                    vote_granted: true,
                })
            }

            fn request_remote_vote(
                &mut self,
                target_index: usize,
                voter_id: &NodeId,
                vote: &RaftRequestVoteRequest,
            ) -> Result<RaftRequestVoteResponse, Self::Error> {
                self.sent_votes
                    .push((target_index, voter_id.clone(), vote.term));
                Ok(RaftRequestVoteResponse {
                    term: vote.term,
                    vote_granted: true,
                })
            }

            fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error> {
                self.observed_terms.push(term);
                Ok(())
            }

            fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error> {
                self.leader_terms.push(term);
                Ok(())
            }
        }

        let local_node_id = NodeId::from_legacy_named("node_kv_election_local").expect("node");
        let peer_a = NodeId::from_legacy_named("node_kv_election_peer_a").expect("node");
        let peer_b = NodeId::from_legacy_named("node_kv_election_peer_b").expect("node");
        let mut host = RecordingElectionHost {
            sent_votes: Vec::new(),
            observed_terms: Vec::new(),
            leader_terms: Vec::new(),
        };

        let elected = elect_cluster_kv_raft_leader_with_host(
            &local_node_id,
            vec![local_node_id.clone(), peer_a.clone(), peer_b.clone()],
            vec![peer_a.clone(), peer_b],
            ClusterKvRaftLeaderElectionOptions { max_attempts: 1 },
            &mut host,
        )
        .expect("election");

        assert!(elected);
        let expected_term = 1 + CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP;
        assert_eq!(host.sent_votes, vec![(0, peer_a, expected_term)]);
        assert!(host.observed_terms.is_empty());
        assert_eq!(host.leader_terms, vec![expected_term]);
    }

    #[test]
    fn raft_votes_require_current_candidate_log() {
        let voter = NodeId::from_legacy_named("node_voter").expect("node");
        let stale = NodeId::from_legacy_named("node_stale").expect("node");
        let current = NodeId::from_legacy_named("node_current").expect("node");
        let mut cluster = InProcessRaftKvCluster::new(vec![voter.clone()]).expect("cluster");
        cluster.put("/config/a", b"one".to_vec()).expect("put");

        let rejected = cluster
            .request_vote(
                &voter,
                RaftRequestVoteRequest {
                    term: 2,
                    candidate_id: stale,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .expect("vote response");
        assert!(!rejected.vote_granted);

        let granted = cluster
            .request_vote(
                &voter,
                RaftRequestVoteRequest {
                    term: 3,
                    candidate_id: current,
                    last_log_index: 1,
                    last_log_term: 1,
                },
            )
            .expect("vote response");
        assert!(granted.vote_granted);
    }

    #[test]
    fn durable_raft_node_replays_wal_and_snapshot() {
        let root = temp_raft_root("sinomo-kv-wal");
        let node_id = NodeId::from_legacy_named("node_wal").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let first = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append first");
            node.commit_to(first.index).expect("commit first");
            let snapshot = node.save_snapshot().expect("snapshot");
            assert_eq!(snapshot.kv.revision, KvRevision(1));
            let second = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/b".to_string(),
                        value: b"two".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append second");
            node.commit_to(second.index).expect("commit second");
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen");

        assert_eq!(node.store().revision(), KvRevision(2));
        assert_eq!(node.store().get("/config/a").expect("a").value, b"one");
        assert_eq!(node.store().get("/config/b").expect("b").value, b"two");
        assert_eq!(node.hard_state().commit_index, 2);
    }

    #[test]
    fn durable_raft_save_snapshot_skips_identical_wal_tail_rewrite() {
        let root = temp_raft_root("sinomo-kv-snapshot-no-wal-rewrite");
        let node_id = NodeId::from_legacy_named("node_snapshot_no_wal_rewrite").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/snapshot/no-rewrite".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
            node.save_snapshot().expect("initial snapshot");

            let before = fs::metadata(&paths.wal).expect("wal metadata before");
            let snapshot_before = fs::metadata(&paths.snapshot).expect("snapshot metadata before");
            thread::sleep(Duration::from_millis(20));
            node.save_snapshot().expect("unchanged snapshot");
            let after = fs::metadata(&paths.wal).expect("wal metadata after");
            let snapshot_after = fs::metadata(&paths.snapshot).expect("snapshot metadata after");

            assert_eq!(before.len(), after.len());
            assert_eq!(file_ino(&before), file_ino(&after));
            assert_eq!(
                before.modified().ok(),
                after.modified().ok(),
                "unchanged snapshot compaction should not rewrite an identical WAL tail"
            );
            assert_eq!(snapshot_before.len(), snapshot_after.len());
            assert_eq!(file_ino(&snapshot_before), file_ino(&snapshot_after));
            assert_eq!(
                snapshot_before.modified().ok(),
                snapshot_after.modified().ok(),
                "unchanged snapshot save should not rewrite an identical snapshot object"
            );
        }
    }

    #[test]
    fn cluster_kv_raft_compaction_policy_skips_idle_retained_tail() {
        let root = temp_raft_root("sinomo-kv-compaction-policy-idle");
        let node_id = NodeId::from_legacy_named("node_compaction_policy_idle").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
        let entry = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/compaction/idle".to_string(),
                    value: b"value".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append");
        node.commit_to(entry.index).expect("commit");
        node.save_snapshot().expect("initial snapshot");
        let snapshot_before = fs::metadata(&paths.snapshot).expect("snapshot before");
        thread::sleep(Duration::from_millis(20));

        let compacted = compact_cluster_kv_raft_node_if_needed(
            &mut node,
            snapshot_before.len(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        )
        .expect("compaction policy");

        let snapshot_after = fs::metadata(&paths.snapshot).expect("snapshot after");
        assert!(!compacted);
        assert_eq!(snapshot_before.len(), snapshot_after.len());
        assert_eq!(file_ino(&snapshot_before), file_ino(&snapshot_after));
        assert_eq!(
            snapshot_before.modified().ok(),
            snapshot_after.modified().ok()
        );
        drop(node);
    }

    struct ClusterKvRaftSnapshotByteLimitGuard {
        previous: u64,
    }

    impl ClusterKvRaftSnapshotByteLimitGuard {
        fn install(limit: u64) -> Self {
            Self {
                previous: set_cluster_kv_raft_snapshot_byte_limit_for_test(limit),
            }
        }
    }

    impl Drop for ClusterKvRaftSnapshotByteLimitGuard {
        fn drop(&mut self) {
            set_cluster_kv_raft_snapshot_byte_limit_for_test(self.previous);
        }
    }

    #[test]
    fn cluster_kv_raft_store_oversized_snapshot_with_compacted_events_is_idle_on_open() {
        let _limit_guard = ClusterKvRaftSnapshotByteLimitGuard::install(1);
        let root = temp_raft_root("sinomo-kv-raft-oversized-snapshot-idle");
        let node_id = NodeId::from_legacy_named("node_oversized_snapshot_idle").expect("node");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let wal_paths = paths.raft_wal_paths(&node_id);

        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), wal_paths.clone()).expect("open");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/oversized/snapshot/value".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
            node.compact_event_history_to(MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
            node.save_snapshot().expect("snapshot");
            assert!(
                node.store().event_history_len() <= MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
                "fixture should have no compactable event-history tail"
            );
        }

        let before = fs::metadata(&wal_paths.snapshot).expect("snapshot before");
        assert!(
            before.len() > 1,
            "test threshold should make this ordinary snapshot look oversized"
        );
        thread::sleep(Duration::from_millis(20));
        let store = ClusterKvRaftStore::new(paths, default_cluster_kv_raft_compaction_policy());
        let reopened = store.open_node(&node_id).expect("open compacted snapshot");
        assert_eq!(reopened.snapshot_index(), 1);
        drop(reopened);
        let after = fs::metadata(&wal_paths.snapshot).expect("snapshot after");

        assert_eq!(before.len(), after.len());
        assert_eq!(
            before.modified().ok(),
            after.modified().ok(),
            "oversized snapshots without compactable event history should not be re-saved on idle open"
        );
    }

    #[test]
    fn cluster_kv_raft_compaction_policy_compacts_after_progress_watermark() {
        let root = temp_raft_root("sinomo-kv-compaction-policy-progress");
        let node_id = NodeId::from_legacy_named("node_compaction_policy_progress").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths).expect("open");
        let command_count = RAFT_COMPACTION_RETAINED_ENTRIES
            .saturating_mul(2)
            .saturating_add(1);
        for index in 1..=command_count {
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: format!("/compaction/progress/{index}"),
                        value: index.to_be_bytes().to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
        }

        assert!(cluster_kv_raft_node_should_compact(
            &node,
            0,
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        ));
        assert!(
            compact_cluster_kv_raft_node_if_needed(
                &mut node,
                0,
                ClusterKvRaftCompactionPolicy {
                    max_snapshot_bytes: u64::MAX,
                    max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
                },
            )
            .expect("compact")
        );
        assert_eq!(node.snapshot_index(), command_count);
        assert!(node.log().len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize);
        drop(node);
    }

    #[test]
    fn cluster_kv_raft_store_open_node_async_compacts_after_progress_watermark() {
        let root = temp_raft_root("sinomo-kv-raft-store-async-open-compacts");
        let node_id =
            NodeId::from_legacy_named("node_raft_store_async_open_compacts").expect("node");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let wal_paths = paths.raft_wal_paths(&node_id);
        let command_count = RAFT_COMPACTION_RETAINED_ENTRIES
            .saturating_mul(2)
            .saturating_add(1);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), wal_paths.clone()).expect("open");
            for index in 1..=command_count {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: format!("/async-open/compaction/{index}"),
                            value: index.to_be_bytes().to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append");
                node.commit_to(entry.index).expect("commit");
            }
            assert_eq!(node.snapshot_index(), 0);
        }

        let store = ClusterKvRaftStore::new(
            paths,
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let node = runtime
            .block_on(store.open_node_async(&node_id))
            .expect("open async");

        assert_eq!(node.snapshot_index(), command_count);
        assert!(node.log().len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize);
        drop(node);
    }

    #[test]
    fn cluster_kv_raft_store_applies_vote_and_leader_updates() {
        let root = temp_raft_root("sinomo-kv-raft-store-vote-leader");
        let node_id = NodeId::from_legacy_named("node_raft_store_vote_leader").expect("node");
        let candidate_id =
            NodeId::from_legacy_named("node_raft_store_candidate").expect("candidate");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );

        let metadata = store
            .local_vote_metadata(&node_id)
            .expect("initial vote metadata");
        assert_eq!(
            metadata,
            ClusterKvRaftLocalVoteMetadata {
                current_term: 1,
                last_log_index: 0,
                last_log_term: 0,
            }
        );
        assert_eq!(store.last_log_index(&node_id).expect("last log index"), 0);

        let vote = store
            .apply_vote(
                &node_id,
                RaftRequestVoteRequest {
                    term: 2,
                    candidate_id: candidate_id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .expect("apply vote");
        assert!(vote.vote_granted);
        assert_eq!(vote.term, 2);
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after vote");
        assert_eq!(hard_state.current_term, 2);
        assert_eq!(hard_state.voted_for.as_ref(), Some(&candidate_id));
        assert!(hard_state.leader_id.is_none());

        let observed = store.observe_term(&node_id, 3).expect("observe term");
        assert!(observed.is_some());
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after term");
        assert_eq!(hard_state.current_term, 3);
        assert!(hard_state.voted_for.is_none());
        assert!(hard_state.leader_id.is_none());

        store.set_leader(&node_id, 3).expect("set leader");
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after leader");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_ref(), Some(&node_id));
        assert_eq!(hard_state.leader_id.as_ref(), Some(&node_id));
        assert_eq!(
            store
                .known_leader_id(&node_id)
                .expect("known leader after set")
                .as_ref(),
            Some(&node_id)
        );

        store.clear_leader(&node_id).expect("clear leader");
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after clear");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_ref(), Some(&node_id));
        assert!(hard_state.leader_id.is_none());
        assert_eq!(
            store
                .known_leader_id(&node_id)
                .expect("known leader after clear"),
            None
        );
    }

    #[test]
    fn cluster_kv_raft_store_applies_append_and_reports_rejections() {
        let root = temp_raft_root("sinomo-kv-raft-store-append");
        let follower_id =
            NodeId::from_legacy_named("node_raft_store_append_follower").expect("follower");
        let leader_id = NodeId::from_legacy_named("node_raft_store_append_leader").expect("leader");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );

        let (outcome, wrote_snapshot) = store
            .apply_append_entries_with_optional_snapshot_and_publish(
                &follower_id,
                None,
                RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![KvLogEntry {
                        index: 1,
                        term: 2,
                        command: KvCommand::Op(KvOp::Put {
                            key: "/raft-store/append".to_string(),
                            value: b"value".to_vec(),
                            lease_id: None,
                        }),
                        revision: KvRevision::ZERO,
                    }],
                    leader_commit: 1,
                },
                |_| false,
            )
            .expect("apply append");

        assert!(outcome.response.success);
        assert!(wrote_snapshot);
        assert!(!outcome.snapshot_restored);
        assert_eq!(outcome.committed_events.len(), 1);
        assert_eq!(outcome.committed_events[0].key, "/raft-store/append");
        assert_eq!(
            outcome
                .snapshot
                .get("/raft-store/append")
                .expect("entry")
                .value,
            b"value".to_vec()
        );
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published snapshot")
                .get("/raft-store/append")
                .expect("published entry")
                .value,
            b"value".to_vec()
        );

        let append_rejection = store
            .append_entries_rejection_response(&follower_id)
            .expect("append rejection");
        assert_eq!(
            append_rejection,
            RaftAppendEntriesResponse {
                term: 2,
                success: false,
                match_index: 1,
            }
        );
        let vote_rejection = store
            .request_vote_rejection_response(&follower_id)
            .expect("vote rejection");
        assert_eq!(
            vote_rejection,
            RaftRequestVoteResponse {
                term: 2,
                vote_granted: false,
            }
        );
    }

    #[test]
    fn cluster_kv_raft_store_builds_vote_responses() {
        let root = temp_raft_root("sinomo-kv-raft-store-vote-response");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let local_id =
            NodeId::from_legacy_named("node_raft_store_vote_response_local").expect("local");
        let candidate_id = NodeId::from_legacy_named("node_raft_store_vote_response_candidate")
            .expect("candidate");
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_raft_store_vote_response").expect("cluster"),
            "raft vote response",
            local_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        assert!(cluster_kv_raft_candidate_is_authorized_voter(
            &state, &local_id
        ));

        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let granted = store
            .apply_vote_response(
                &state,
                RaftRequestVoteRequest {
                    term: 1,
                    candidate_id: candidate_id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .expect("vote response");
        assert!(granted.accepted);
        assert_eq!(granted.detail, "raft vote granted");
        assert_eq!(granted.state, state);
        assert_eq!(
            granted.vote,
            RaftRequestVoteResponse {
                term: 1,
                vote_granted: true,
            }
        );

        let rejected = store
            .cluster_raft_vote_rejection_response(
                &granted.state,
                "raft vote rejected for unauthorized or mismatched candidate".to_string(),
            )
            .expect("vote rejection response");
        assert!(!rejected.accepted);
        assert!(!rejected.vote.vote_granted);
        assert_eq!(rejected.vote.term, 1);
        assert!(rejected.detail.contains("unauthorized"));
    }

    #[test]
    fn cluster_kv_raft_store_appends_commits_and_detects_no_effect_commands() {
        let root = temp_raft_root("sinomo-kv-raft-store-command");
        let node_id = NodeId::from_legacy_named("node_raft_store_command").expect("node");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let put = KvCommand::Op(KvOp::Put {
            key: "/raft-store/command".to_string(),
            value: b"value".to_vec(),
            lease_id: None,
        });

        let (committed, wrote_snapshot) = store
            .append_command_with_response_and_publish_report(&node_id, put.clone(), |_| false)
            .expect("append and commit");
        assert_eq!(committed.snapshot.revision, KvRevision(1));
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].key, "/raft-store/command");
        assert!(committed.txn_response.is_none());
        assert!(wrote_snapshot);
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published command snapshot")
                .get("/raft-store/command")
                .expect("published command")
                .value,
            b"value".to_vec()
        );
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after commit");
        assert_eq!(hard_state.commit_index, 1);

        let (no_effect_commit, no_effect_wrote_snapshot) = store
            .append_command_with_response_and_publish_report(&node_id, put.clone(), |_| false)
            .expect("append no-effect command");
        assert_eq!(no_effect_commit.snapshot.revision, KvRevision(1));
        assert!(no_effect_commit.events.is_empty());
        assert!(!no_effect_wrote_snapshot);

        let no_effect_uncommitted = store
            .append_command_uncommitted_or_no_effect(&node_id, put)
            .expect("uncommitted no-effect command");
        match no_effect_uncommitted {
            ClusterKvRaftUncommittedAppend::NoEffect {
                snapshot,
                txn_response,
            } => {
                assert_eq!(snapshot.revision, KvRevision(1));
                assert!(txn_response.is_none());
            }
            ClusterKvRaftUncommittedAppend::Appended(entry) => {
                panic!("expected no-effect append, got entry {entry:?}");
            }
        }

        let append = store
            .append_command_uncommitted(
                &node_id,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-store/pending".to_string(),
                    value: b"pending".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append uncommitted command");
        assert_eq!(append.index, 2);
        let (committed, wrote_snapshot) = store
            .commit_index_with_events_and_publish_report(&node_id, append.index, |_| false)
            .expect("commit uncommitted command");
        assert_eq!(committed.snapshot.revision, KvRevision(2));
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].key, "/raft-store/pending");
        assert!(wrote_snapshot);
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published pending snapshot")
                .get("/raft-store/pending")
                .expect("published pending")
                .value,
            b"pending".to_vec()
        );
    }

    #[test]
    fn cluster_kv_raft_local_command_storage_appends_commits_and_discards() {
        let root = temp_raft_root("sinomo-kv-local-command-storage");
        let node_id = NodeId::from_legacy_named("node_kv_local_command_storage").expect("node");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let mut storage = ClusterKvRaftLocalCommandStorage::new(store, node_id, |_| false);

        let entry = match storage
            .append_uncommitted_or_no_effect(KvCommand::Op(KvOp::Put {
                key: "/raft-local-storage/committed".to_string(),
                value: b"committed".to_vec(),
                lease_id: None,
            }))
            .expect("append command")
        {
            ClusterKvRaftUncommittedAppend::Appended(entry) => entry,
            ClusterKvRaftUncommittedAppend::NoEffect { .. } => {
                panic!("fresh command should append")
            }
        };
        let commit = storage
            .commit_index_with_events(entry.index)
            .expect("commit command");
        assert_eq!(commit.events.len(), 1);
        assert_eq!(storage.take_last_snapshot_publish_wrote(), Some(true));
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published snapshot")
                .get("/raft-local-storage/committed")
                .expect("committed key")
                .value,
            b"committed".to_vec()
        );

        let _ = storage
            .append_uncommitted_or_no_effect(KvCommand::Op(KvOp::Put {
                key: "/raft-local-storage/discarded".to_string(),
                value: b"discarded".to_vec(),
                lease_id: None,
            }))
            .expect("append discarded command");
        let discarded = storage
            .discard_uncommitted_tail_and_clear_leader()
            .expect("discard tail");
        assert!(discarded.get("/raft-local-storage/discarded").is_none());
        assert_eq!(storage.take_last_snapshot_publish_wrote(), Some(false));
    }

    #[test]
    fn cluster_kv_raft_store_builds_append_request_from_retained_tail() {
        let root = temp_raft_root("sinomo-kv-raft-store-append-request-tail");
        let node_id = NodeId::from_legacy_named("node_raft_store_append_request").expect("node");
        let leader_id =
            NodeId::from_legacy_named("node_raft_store_append_request_leader").expect("leader");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let mut node = DurableRaftKvNode::open(node_id.clone(), paths.raft_wal_paths(&node_id))
            .expect("open node");
        for index in 1..=400u64 {
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: format!("/append-request/{index:03}"),
                        value: index.to_le_bytes().to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
        }
        node.save_snapshot().expect("save snapshot");
        let snapshot_index = node.snapshot_index();
        let earliest_retained = node.log().first().map(|entry| entry.index).expect("tail");
        let last_log_index = node.last_log_index();
        assert!(snapshot_index > 0);
        assert!(!node.log().is_empty());
        assert!(node.log().len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize);
        drop(node);

        let heartbeat = store
            .build_append_entries_request_from_next_index(
                &node_id,
                &leader_id,
                last_log_index.saturating_add(1),
            )
            .expect("heartbeat");
        assert!(heartbeat.snapshot.is_none());
        assert!(heartbeat.append.entries.is_empty());
        assert_eq!(&heartbeat.append.leader_id, &leader_id);
        assert_eq!(heartbeat.append.prev_log_index, snapshot_index);

        let near = store
            .build_append_entries_request_from_next_index(
                &node_id,
                &leader_id,
                earliest_retained + 1,
            )
            .expect("near");
        assert!(near.snapshot.is_none());
        assert!(!near.append.entries.is_empty());

        let far = store
            .build_append_entries_request_from_next_index(
                &node_id,
                &leader_id,
                earliest_retained.saturating_sub(1).max(1),
            )
            .expect("far");
        assert!(far.snapshot.is_some());
    }

    #[test]
    fn durable_raft_compaction_retains_log_tail_across_reopen() {
        let root = temp_raft_root("sinomo-kv-retain-tail");
        let node_id = NodeId::from_legacy_named("node_retain_tail").expect("node");
        let paths = KvWalPaths::new(&root);
        let snapshot_index;
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            for index in 1..=400u64 {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: format!("/retain/{index:03}"),
                            value: index.to_le_bytes().to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append");
                node.commit_to(entry.index).expect("commit");
            }
            node.save_snapshot().expect("snapshot");
            snapshot_index = node.snapshot_index();
            assert_eq!(snapshot_index, 400);
            // Compaction keeps a bounded committed tail rather than truncating to empty.
            assert!(!node.log().is_empty());
            assert!(node.log().len() <= 256);
        }

        // The critical property: reopening from disk reloads the tail (the daemon
        // opens a fresh node per replication call), so incremental replication keeps
        // working instead of falling back to a full snapshot every time.
        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen");
        assert!(
            !node.log().is_empty(),
            "retained tail must survive reopen for incremental replication"
        );
        assert!(node.log().len() <= 256);
        assert_eq!(node.snapshot_index(), snapshot_index);
        // State is intact and not double-applied.
        assert_eq!(node.applied_index(), 400);
        assert_eq!(node.store().revision(), KvRevision(400));
        assert_eq!(
            node.store().get("/retain/400").expect("entry").value,
            400u64.to_le_bytes().to_vec()
        );
    }

    #[test]
    fn durable_raft_node_opens_from_tokio_without_blocking_runtime() {
        let root = temp_raft_root("sinomo-kv-async-open");
        let node_id = NodeId::from_legacy_named("node_async_open").expect("node");
        let paths = KvWalPaths::new(&root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");

        let node = runtime
            .block_on(DurableRaftKvNode::open_async(node_id.clone(), paths))
            .expect("open async");

        assert_eq!(node.node_id(), &node_id);
    }

    #[test]
    fn durable_raft_power_loss_before_commit_does_not_apply_uncommitted_wal() {
        let root = temp_raft_root("sinomo-kv-powerloss-uncommitted");
        let node_id = NodeId::from_legacy_named("node_powerloss_uncommitted").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/uncommitted".to_string(),
                    value: b"lost".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append uncommitted");
        }

        let mut node = DurableRaftKvNode::open(node_id, paths).expect("reopen after power loss");

        assert!(node.store().get("/config/uncommitted").is_none());
        assert_eq!(node.hard_state().commit_index, 0);
        assert_eq!(node.last_log_index(), 1);
        node.commit_to(1).expect("commit recovered wal entry");
        assert_eq!(
            node.store()
                .get("/config/uncommitted")
                .expect("committed after recovery")
                .value,
            b"lost"
        );
    }

    #[test]
    fn durable_raft_append_local_command_rolls_back_after_wal_failure() {
        let root = temp_raft_root("sinomo-kv-local-append-rollback");
        let node_id = NodeId::from_legacy_named("node_local_append_rollback").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
        std::fs::create_dir(&paths.wal).expect("block WAL appends");

        let error = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/rollback/local".to_string(),
                    value: b"not-durable".to_vec(),
                    lease_id: None,
                }),
            )
            .expect_err("WAL append should fail");

        assert!(
            error.to_string().contains("wal.sobjlog"),
            "unexpected error: {error}"
        );
        assert_eq!(
            node.last_log_index(),
            0,
            "failed WAL append must not leave an in-memory log entry"
        );
        assert_eq!(node.hard_state(), &RaftHardState::default());
        assert!(node.store().get("/rollback/local").is_none());
    }

    #[test]
    fn durable_raft_append_entries_rolls_back_after_wal_failure() {
        let leader_root = temp_raft_root("sinomo-kv-follower-append-rollback-leader");
        let follower_root = temp_raft_root("sinomo-kv-follower-append-rollback");
        let leader_id = NodeId::from_legacy_named("node_append_rollback_leader").expect("leader");
        let follower_id =
            NodeId::from_legacy_named("node_append_rollback_follower").expect("follower");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let entry = {
            let mut leader =
                DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("open leader");
            let entry = leader
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/rollback/follower".to_string(),
                        value: b"not-durable".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append leader");
            leader.commit_to(entry.index).expect("commit leader");
            entry
        };
        let mut follower =
            DurableRaftKvNode::open(follower_id, follower_paths.clone()).expect("open follower");
        std::fs::create_dir(&follower_paths.wal).expect("block follower WAL appends");

        let error = follower
            .append_entries(RaftAppendEntriesRequest {
                term: 1,
                leader_id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry],
                leader_commit: 1,
            })
            .expect_err("WAL append should fail");

        assert!(
            error.to_string().contains("wal.sobjlog"),
            "unexpected error: {error}"
        );
        assert_eq!(
            follower.last_log_index(),
            0,
            "failed follower append must not leave an in-memory log entry"
        );
        assert_eq!(follower.hard_state(), &RaftHardState::default());
        assert!(follower.store().get("/rollback/follower").is_none());
    }

    #[test]
    fn durable_raft_append_entries_rolls_back_after_hard_state_failure() {
        let leader_root = temp_raft_root("sinomo-kv-follower-hard-state-rollback-leader");
        let follower_root = temp_raft_root("sinomo-kv-follower-hard-state-rollback");
        let leader_id =
            NodeId::from_legacy_named("node_append_hard_state_rollback_leader").expect("leader");
        let follower_id = NodeId::from_legacy_named("node_append_hard_state_rollback_follower")
            .expect("follower");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let entry = {
            let mut leader =
                DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("open leader");
            let entry = leader
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/rollback/follower-hard-state".to_string(),
                        value: b"not-committed".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append leader");
            leader.commit_to(entry.index).expect("commit leader");
            entry
        };
        let mut follower =
            DurableRaftKvNode::open(follower_id, follower_paths.clone()).expect("open follower");
        std::fs::remove_file(&follower_paths.hard_state).ok();
        std::fs::create_dir(&follower_paths.hard_state).expect("block hard-state writes");

        let error = follower
            .append_entries(RaftAppendEntriesRequest {
                term: 1,
                leader_id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry],
                leader_commit: 1,
            })
            .expect_err("hard-state persist should fail");

        assert!(
            error.to_string().contains("hard-state.sobj"),
            "unexpected error: {error}"
        );
        assert_eq!(
            follower.hard_state().commit_index,
            0,
            "failed hard-state persistence must roll back in-memory commit index"
        );
        assert_eq!(follower.last_log_index(), 0);
        assert_eq!(follower.applied_index(), 0);
        assert!(
            follower
                .store()
                .get("/rollback/follower-hard-state")
                .is_none(),
            "failed hard-state persistence must not expose the committed value"
        );
    }

    #[test]
    fn durable_raft_commit_rolls_back_after_hard_state_failure() {
        let root = temp_raft_root("sinomo-kv-commit-rollback");
        let node_id = NodeId::from_legacy_named("node_commit_rollback").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
        let entry = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/rollback/commit".to_string(),
                    value: b"not-committed".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append");
        std::fs::remove_file(&paths.hard_state).expect("remove hard state file");
        std::fs::create_dir(&paths.hard_state).expect("block hard-state writes");

        let error = node
            .commit_to(entry.index)
            .expect_err("hard-state persist should fail");

        assert!(
            error.to_string().contains("hard-state.sobj"),
            "unexpected error: {error}"
        );
        assert_eq!(
            node.hard_state().commit_index,
            0,
            "failed commit persistence must roll back in-memory hard state"
        );
        assert_eq!(node.applied_index(), 0);
        assert!(
            node.store().get("/rollback/commit").is_none(),
            "failed commit persistence must not expose the applied value"
        );
        assert_eq!(
            node.last_log_index(),
            entry.index,
            "the durable uncommitted WAL entry should remain available in memory"
        );
    }

    #[test]
    fn durable_raft_rejects_commit_beyond_local_log() {
        let root = temp_raft_root("sinomo-kv-commit-beyond-log");
        let node_id = NodeId::from_legacy_named("node_commit_beyond_log").expect("node");
        let paths = KvWalPaths::new(&root);

        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let error = node.commit_to(1).expect_err("missing log entry");
            assert_eq!(error, KvError::MissingCommittedEntry { index: 1 });
            assert_eq!(
                node.hard_state().commit_index,
                0,
                "rejected commits must not advance in-memory hard state"
            );
        }

        let reopened = DurableRaftKvNode::open(node_id, paths.clone()).expect("reopen");
        assert_eq!(
            reopened.hard_state().commit_index,
            0,
            "rejected commits must not persist impossible hard state"
        );
    }

    #[test]
    fn durable_raft_open_repairs_commit_index_beyond_local_log() {
        let root = temp_raft_root("sinomo-kv-repair-commit-beyond-log");
        let node_id = NodeId::from_legacy_named("node_repair_commit_beyond_log").expect("node");
        let paths = KvWalPaths::new(&root);
        write_object_file(
            &paths.hard_state,
            &RaftHardState {
                commit_index: 7,
                ..RaftHardState::default()
            },
        )
        .expect("write impossible hard state");

        {
            let node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open repairs");
            assert_eq!(
                node.hard_state().commit_index,
                0,
                "open should clamp persisted commit index to the available local log"
            );
        }

        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read repaired hard state")
                .commit_index,
            0,
            "repair should be durable so the next open stays cheap"
        );
    }

    #[test]
    fn persisted_hard_state_reads_use_identity_cache() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        let root = temp_raft_root("sinomo-kv-hard-state-cache");
        let paths = KvWalPaths::new(&root);
        hard_state_cache().lock().expect("hard-state cache").clear();
        reset_persisted_hard_state_actual_reads_for_test(&paths.hard_state);

        assert_eq!(
            paths.read_persisted_hard_state().expect("read absent"),
            RaftHardState::default()
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "the first absent-file read should touch the filesystem"
        );
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read absent again"),
            RaftHardState::default()
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "unchanged absent hard state should be served from cache"
        );

        let first_leader = NodeId::from_legacy_named("hard_state_cache_first").expect("leader");
        let first = RaftHardState {
            current_term: 3,
            voted_for: Some(first_leader.clone()),
            leader_id: Some(first_leader),
            commit_index: 5,
        };
        paths
            .write_persisted_hard_state(&first)
            .expect("write hard state");
        reset_persisted_hard_state_actual_reads_for_test(&paths.hard_state);
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read write-through cache"),
            first
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            0,
            "local hard-state writes should refresh the read cache"
        );

        let second_leader = NodeId::from_legacy_named("hard_state_cache_second").expect("leader");
        let second = RaftHardState {
            current_term: 4,
            voted_for: Some(second_leader.clone()),
            leader_id: Some(second_leader),
            commit_index: 6,
        };
        write_object_file(&paths.hard_state, &second).expect("external hard-state replace");
        reset_persisted_hard_state_actual_reads_for_test(&paths.hard_state);
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read external replacement"),
            second
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "a changed file identity must force one real read"
        );
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read cached replacement"),
            second
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "unchanged replacement should be cached after the first read"
        );
    }

    #[test]
    fn replicated_node_commit_gap_does_not_partially_apply() {
        let mut node = ReplicatedKvNode {
            store: InProcessKvStore::new(),
            log: vec![
                KvLogEntry {
                    index: 1,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/gap/one".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                KvLogEntry {
                    index: 3,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/gap/three".to_string(),
                        value: b"three".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
            ],
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: KvTxnResponseIndex::default(),
        };

        let error = node.commit_to(3).expect_err("missing middle entry");

        assert_eq!(error, KvError::MissingCommittedEntry { index: 2 });
        assert_eq!(node.applied_index, 0);
        assert_eq!(node.hard_state.commit_index, 0);
        assert_eq!(node.store.revision(), KvRevision::ZERO);
        assert!(node.store.get("/gap/one").is_none());
        assert!(node.store.get("/gap/three").is_none());
    }

    #[test]
    fn replicated_node_single_entry_commit_uses_single_entry_plan() {
        let node = ReplicatedKvNode {
            store: InProcessKvStore::new(),
            log: vec![
                KvLogEntry {
                    index: 1,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/commit-plan/one".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                KvLogEntry {
                    index: 2,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/commit-plan/two".to_string(),
                        value: b"two".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
            ],
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: KvTxnResponseIndex::default(),
        };

        assert!(
            matches!(
                node.entries_to_commit(1).expect("single commit plan"),
                CommitEntries::Single(_)
            ),
            "the common next-entry commit should avoid allocating a one-entry batch"
        );
        assert!(
            matches!(
                node.entries_to_commit(2).expect("batch commit plan"),
                CommitEntries::Batch(entries) if entries.len() == 2
            ),
            "catch-up commits still need the contiguous batch path"
        );
    }

    #[test]
    fn replicated_node_rejects_same_term_conflicting_log_entry() {
        let mut node = ReplicatedKvNode {
            store: InProcessKvStore::new(),
            log: vec![KvLogEntry {
                index: 1,
                term: 1,
                command: KvCommand::Op(KvOp::Put {
                    key: "/conflict/original".to_string(),
                    value: b"original".to_vec(),
                    lease_id: None,
                }),
                revision: KvRevision::ZERO,
            }],
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: KvTxnResponseIndex::default(),
        };

        let error = node
            .append_entries(vec![KvLogEntry {
                index: 1,
                term: 1,
                command: KvCommand::Op(KvOp::Put {
                    key: "/conflict/replacement".to_string(),
                    value: b"replacement".to_vec(),
                    lease_id: None,
                }),
                revision: KvRevision::ZERO,
            }])
            .expect_err("same-term command conflict");

        assert_eq!(error, KvError::ConflictingLogEntry { index: 1 });
        assert_eq!(node.log.len(), 1);
        assert_eq!(
            node.log[0].command,
            KvCommand::Op(KvOp::Put {
                key: "/conflict/original".to_string(),
                value: b"original".to_vec(),
                lease_id: None,
            })
        );
    }

    #[test]
    fn durable_raft_discard_uncommitted_tail_keeps_committed_prefix() {
        let root = temp_raft_root("sinomo-kv-discard-uncommitted-tail");
        let node_id = NodeId::from_legacy_named("node_discard_uncommitted_tail").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let committed = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/committed".to_string(),
                        value: b"kept".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(committed.index).expect("commit first");
            let uncommitted = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/uncommitted".to_string(),
                        value: b"dropped".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append uncommitted");
            assert_eq!(uncommitted.index, 2);
            assert_eq!(node.last_log_index(), 2);
        }

        {
            let mut node =
                DurableRaftKvNode::open_discarding_uncommitted_tail(node_id.clone(), paths.clone())
                    .expect("open and discard");
            assert_eq!(node.hard_state().commit_index, 1);
            assert_eq!(node.last_log_index(), 1);
            assert_eq!(
                node.store()
                    .get("/config/committed")
                    .expect("committed value")
                    .value,
                b"kept"
            );
            assert!(node.store().get("/config/uncommitted").is_none());
            assert!(
                !node
                    .discard_uncommitted_tail()
                    .expect("second discard is a no-op")
            );
            let replacement = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/replacement".to_string(),
                        value: b"next".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append replacement");
            assert_eq!(replacement.index, 2);
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen after discard");
        assert_eq!(node.last_log_index(), 2);
        assert_eq!(node.hard_state().commit_index, 1);
        assert!(node.store().get("/config/replacement").is_none());
    }

    #[test]
    fn durable_raft_observe_higher_term_clears_vote_and_leader() {
        let root = temp_raft_root("sinomo-kv-observe-higher-term");
        let node_id = NodeId::from_legacy_named("node_observe_higher_term").expect("node");
        let leader_id = NodeId::from_legacy_named("node_observe_higher_term_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            node.request_vote(RaftRequestVoteRequest {
                term: 2,
                candidate_id: leader_id.clone(),
                last_log_index: 0,
                last_log_term: 0,
            })
            .expect("vote");
            let append = node
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                })
                .expect("append");
            assert!(append.success);
            assert!(node.hard_state().voted_for.is_some());
            assert!(node.hard_state().leader_id.is_some());
            assert!(node.observe_term(5).expect("observe higher term"));
            assert_eq!(node.hard_state().current_term, 5);
            assert!(node.hard_state().voted_for.is_none());
            assert!(node.hard_state().leader_id.is_none());
            assert!(!node.observe_term(4).expect("ignore lower term"));
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen");
        assert_eq!(node.hard_state().current_term, 5);
        assert!(node.hard_state().voted_for.is_none());
        assert!(node.hard_state().leader_id.is_none());
    }

    #[test]
    fn durable_raft_power_loss_after_commit_replays_committed_state() {
        let root = temp_raft_root("sinomo-kv-powerloss-committed");
        let node_id = NodeId::from_legacy_named("node_powerloss_committed").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/committed".to_string(),
                        value: b"survives".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(entry.index).expect("persist commit");
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen after power loss");

        assert_eq!(node.hard_state().commit_index, 1);
        assert_eq!(
            node.store()
                .get("/config/committed")
                .expect("committed value")
                .value,
            b"survives"
        );
    }

    #[test]
    fn durable_raft_persists_higher_term_from_rejected_append() {
        let root = temp_raft_root("sinomo-kv-rejected-append-term");
        let follower_id = NodeId::from_legacy_named("node_rejected_append_follower").expect("node");
        let leader_id = NodeId::from_legacy_named("node_rejected_append_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut follower =
                DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open follower");
            let entry = follower
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/local".to_string(),
                        value: b"local".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append local");
            follower.commit_to(entry.index).expect("commit local");

            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 99,
                    prev_log_term: 2,
                    entries: Vec::new(),
                    leader_commit: 0,
                })
                .expect("rejected append response");

            assert!(!response.success);
            assert_eq!(follower.hard_state().current_term, 2);
            assert_eq!(follower.hard_state().leader_id.as_ref(), Some(&leader_id));
        }

        let reopened =
            DurableRaftKvNode::open(follower_id, paths.clone()).expect("reopen follower");
        assert_eq!(reopened.hard_state().current_term, 2);
        assert_eq!(reopened.hard_state().leader_id.as_ref(), Some(&leader_id));
        assert_eq!(
            reopened
                .store()
                .get("/config/local")
                .expect("committed local value")
                .value,
            b"local"
        );
    }

    #[test]
    fn durable_raft_unchanged_heartbeat_does_not_rewrite_hard_state() {
        let root = temp_raft_root("sinomo-kv-unchanged-heartbeat");
        let follower_id =
            NodeId::from_legacy_named("node_unchanged_heartbeat_follower").expect("node");
        let leader_id = NodeId::from_legacy_named("node_unchanged_heartbeat_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        let heartbeat = RaftAppendEntriesRequest {
            term: 2,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        {
            let mut follower =
                DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open");
            let response = follower
                .append_entries(heartbeat.clone())
                .expect("initial heartbeat");
            assert!(response.success);
        }
        let before = fs::metadata(&paths.hard_state)
            .expect("hard state metadata")
            .modified()
            .expect("hard state modified");
        thread::sleep(Duration::from_millis(20));
        {
            let mut follower = DurableRaftKvNode::open(follower_id, paths.clone()).expect("reopen");
            let response = follower
                .append_entries(heartbeat)
                .expect("unchanged heartbeat");
            assert!(response.success);
        }
        let after = fs::metadata(&paths.hard_state)
            .expect("hard state metadata after")
            .modified()
            .expect("hard state modified after");

        assert_eq!(before, after);
    }

    #[test]
    fn durable_raft_unchanged_heartbeat_with_wal_tail_does_not_rewrite_files() {
        let root = temp_raft_root("sinomo-kv-unchanged-heartbeat-wal");
        let follower_id =
            NodeId::from_legacy_named("node_unchanged_heartbeat_wal_follower").expect("node");
        let leader_id =
            NodeId::from_legacy_named("node_unchanged_heartbeat_wal_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        let entry = KvLogEntry {
            index: 1,
            term: 2,
            command: KvCommand::Op(KvOp::Put {
                key: "/config/heartbeat-wal".to_string(),
                value: b"reachable".to_vec(),
                lease_id: None,
            }),
            revision: KvRevision::ZERO,
        };

        {
            let mut follower =
                DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open");
            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![entry],
                    leader_commit: 0,
                })
                .expect("append leader entry");
            assert!(response.success);
            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 1,
                    prev_log_term: 2,
                    entries: Vec::new(),
                    leader_commit: 1,
                })
                .expect("commit leader entry");
            assert!(response.success);
        }

        let hard_state_before = fs::metadata(&paths.hard_state)
            .expect("hard state metadata")
            .modified()
            .expect("hard state modified");
        let wal_before = fs::metadata(&paths.wal)
            .expect("wal metadata")
            .modified()
            .expect("wal modified");
        thread::sleep(Duration::from_millis(20));

        {
            let mut follower = DurableRaftKvNode::open(follower_id, paths.clone()).expect("reopen");
            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id,
                    prev_log_index: 1,
                    prev_log_term: 2,
                    entries: Vec::new(),
                    leader_commit: 1,
                })
                .expect("unchanged heartbeat");
            assert!(response.success);
        }

        let hard_state_after = fs::metadata(&paths.hard_state)
            .expect("hard state metadata after")
            .modified()
            .expect("hard state modified after");
        let wal_after = fs::metadata(&paths.wal)
            .expect("wal metadata after")
            .modified()
            .expect("wal modified after");

        assert_eq!(hard_state_before, hard_state_after);
        assert_eq!(wal_before, wal_after);
    }

    #[test]
    fn durable_raft_noop_requests_do_not_create_hard_state_file() {
        let root = temp_raft_root("sinomo-kv-noop-hard-state");
        let node_id = NodeId::from_legacy_named("node_noop_hard_state").expect("node");
        let candidate_id = NodeId::from_legacy_named("node_noop_candidate").expect("candidate");
        let leader_id = NodeId::from_legacy_named("node_noop_leader").expect("leader");
        let paths = KvWalPaths::new(&root);

        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let vote = node
                .request_vote(RaftRequestVoteRequest {
                    term: 0,
                    candidate_id,
                    last_log_index: 0,
                    last_log_term: 0,
                })
                .expect("stale vote");
            assert!(!vote.vote_granted);
            let append = node
                .append_entries(RaftAppendEntriesRequest {
                    term: 0,
                    leader_id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                })
                .expect("stale append");
            assert!(!append.success);
        }
        assert!(
            !paths.hard_state.exists(),
            "rejected requests that do not change hard state should not create a hard-state file"
        );

        {
            let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("reopen");
            assert!(node.observe_term(2).expect("observe higher term"));
        }
        assert!(
            paths.hard_state.exists(),
            "real hard-state changes must still be persisted"
        );
    }

    #[test]
    fn durable_raft_duplicate_append_entries_do_not_grow_wal() {
        let leader_root = temp_raft_root("sinomo-kv-duplicate-append-leader");
        let follower_root = temp_raft_root("sinomo-kv-duplicate-append-follower");
        let leader_id = NodeId::from_legacy_named("node_duplicate_append_leader").expect("leader");
        let follower_id =
            NodeId::from_legacy_named("node_duplicate_append_follower").expect("follower");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let entry = {
            let mut leader =
                DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("open leader");
            let entry = leader
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/duplicate".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append leader entry");
            leader.commit_to(entry.index).expect("commit leader entry");
            entry
        };
        let request = RaftAppendEntriesRequest {
            term: 1,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry],
            leader_commit: 1,
        };

        {
            let mut follower = DurableRaftKvNode::open(follower_id.clone(), follower_paths.clone())
                .expect("open follower");
            assert!(
                follower
                    .append_entries(request.clone())
                    .expect("append")
                    .success
            );
        }
        let before_len = fs::metadata(&follower_paths.wal)
            .expect("follower wal metadata")
            .len();
        {
            let mut follower = DurableRaftKvNode::open(follower_id, follower_paths.clone())
                .expect("reopen follower");
            assert!(
                follower
                    .append_entries(request)
                    .expect("duplicate append")
                    .success
            );
        }
        let after_len = fs::metadata(&follower_paths.wal)
            .expect("follower wal metadata after")
            .len();

        assert_eq!(before_len, after_len);
    }

    #[test]
    fn delayed_stale_append_entries_do_not_roll_back_committed_state() {
        let leader_root = temp_raft_root("sinomo-kv-latency-leader");
        let follower_root = temp_raft_root("sinomo-kv-latency-follower");
        let leader_id = NodeId::from_legacy_named("node_latency_leader").expect("node");
        let follower_id = NodeId::from_legacy_named("node_latency_follower").expect("node");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let mut leader =
            DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("leader open");
        let mut follower =
            DurableRaftKvNode::open(follower_id.clone(), follower_paths).expect("follower open");

        let first = leader
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"one".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append first");
        leader.commit_to(first.index).expect("commit first");
        let stale_append = RaftAppendEntriesRequest {
            term: 1,
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: leader.log().to_vec(),
            leader_commit: 1,
        };
        let second = leader
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/b".to_string(),
                    value: b"two".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append second");
        leader.commit_to(second.index).expect("commit second");
        let current_append = RaftAppendEntriesRequest {
            term: 1,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: leader.log().to_vec(),
            leader_commit: 2,
        };

        assert!(
            follower
                .append_entries(current_append)
                .expect("current append")
                .success
        );
        assert_eq!(
            follower
                .store()
                .get("/config/b")
                .expect("second value")
                .value,
            b"two"
        );
        assert!(
            follower
                .append_entries(stale_append)
                .expect("stale append")
                .success
        );

        assert_eq!(follower.hard_state().commit_index, 2);
        assert_eq!(
            follower
                .store()
                .get("/config/a")
                .expect("first value")
                .value,
            b"one"
        );
        assert_eq!(
            follower
                .store()
                .get("/config/b")
                .expect("second value")
                .value,
            b"two"
        );
    }

    #[test]
    fn optional_snapshot_append_restores_newer_snapshot() {
        let leader_root = temp_raft_root("sinomo-kv-optional-snapshot-append-leader");
        let follower_root = temp_raft_root("sinomo-kv-optional-snapshot-append-follower");
        let leader_id = NodeId::from_legacy_named("node_optional_snapshot_leader").expect("node");
        let follower_id =
            NodeId::from_legacy_named("node_optional_snapshot_follower").expect("node");
        let mut leader = DurableRaftKvNode::open(leader_id.clone(), KvWalPaths::new(&leader_root))
            .expect("leader open");
        let mut follower = DurableRaftKvNode::open(follower_id, KvWalPaths::new(&follower_root))
            .expect("follower open");

        let entry = leader
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/snapshot".to_string(),
                    value: b"from-snapshot".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append leader command");
        leader
            .commit_to(entry.index)
            .expect("commit leader command");
        let snapshot = leader.save_snapshot().expect("save leader snapshot");

        let outcome = follower
            .apply_append_entries_with_optional_snapshot(
                Some(snapshot.clone()),
                RaftAppendEntriesRequest {
                    term: 1,
                    leader_id,
                    prev_log_index: snapshot.last_included_index,
                    prev_log_term: snapshot.last_included_term,
                    entries: Vec::new(),
                    leader_commit: snapshot.last_included_index,
                },
            )
            .expect("apply append with snapshot");

        assert!(outcome.response.success);
        assert!(outcome.snapshot_restored);
        assert!(outcome.committed_events.is_empty());
        assert_eq!(
            follower.hard_state().commit_index,
            snapshot.last_included_index
        );
        assert_eq!(
            follower
                .store()
                .get("/config/snapshot")
                .expect("restored value")
                .value,
            b"from-snapshot"
        );
    }

    #[test]
    fn snapshot_restore_preserves_watchable_event_history() {
        let mut store = InProcessKvStore::new();
        store.put("/watch/a", b"one".to_vec(), None).expect("put");
        let snapshot = store.snapshot().expect("snapshot");
        let mut restored = InProcessKvStore::restore(snapshot).expect("restore");

        let watch_id = restored
            .watch_prefix("/watch/", KvRevision(1))
            .expect("watch after restore");
        let events = restored.drain_watch(watch_id).expect("events");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "/watch/a");
    }

    #[test]
    fn durable_raft_node_recovers_by_truncating_corrupt_wal_tail() {
        let root = temp_raft_root("sinomo-kv-corrupt-wal");
        let node_id = NodeId::from_legacy_named("node_corrupt_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
        let before_snapshot = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/before-snapshot".to_string(),
                    value: b"one".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append before snapshot");
        node.commit_to(before_snapshot.index)
            .expect("commit before snapshot");
        node.save_snapshot().expect("snapshot");
        let after_snapshot = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/after-snapshot".to_string(),
                    value: b"two".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append after snapshot");
        node.commit_to(after_snapshot.index)
            .expect("commit after snapshot");
        drop(node);

        let mut wal = OpenOptions::new()
            .append(true)
            .open(&paths.wal)
            .expect("open wal");
        writeln!(wal, "{{\"index\":{{broken").expect("corrupt wal");
        writeln!(wal, "still broken").expect("corrupt wal continuation");
        drop(wal);

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get("/after-snapshot")
                .expect("valid entry after snapshot")
                .value,
            b"two"
        );
        let rewritten = std::fs::read(&paths.wal).expect("rewritten wal");
        assert!(
            !rewritten
                .windows(b"broken".len())
                .any(|window| window == b"broken")
        );
        assert!(
            fs::read_dir(&paths.root)
                .expect("raft dir")
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
                .all(|name| !name.starts_with(".wal.sobjlog.tmp-")),
            "WAL repair must not leave rewrite temp files behind"
        );
    }

    #[test]
    fn durable_raft_open_repairs_wal_records_already_covered_by_snapshot() {
        let root = temp_raft_root("sinomo-kv-snapshot-covered-wal");
        let node_id = NodeId::from_legacy_named("node_snapshot_covered_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        let command_count = RAFT_COMPACTION_RETAINED_ENTRIES + 20;
        let overgrown_log = {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            for index in 1..=command_count {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: format!("/covered/{index}"),
                            value: index.to_be_bytes().to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append command");
                node.commit_to(entry.index).expect("commit command");
            }
            let overgrown_log = node.log().to_vec();
            assert!(
                overgrown_log.len() > RAFT_COMPACTION_RETAINED_ENTRIES as usize,
                "fixture should start with a WAL larger than the retained tail"
            );
            node.save_snapshot().expect("snapshot");
            rewrite_wal_entries(&paths.wal, &overgrown_log).expect("restore overgrown wal");
            overgrown_log
        };

        let before = read_wal_entries(&paths.wal, None, 0).expect("read overgrown wal");
        assert_eq!(before.entries.len(), overgrown_log.len());
        assert!(!before.truncated);

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get(&format!("/covered/{command_count}"))
                .expect("last committed value")
                .value,
            command_count.to_be_bytes().to_vec()
        );
        drop(recovered);

        let after = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert!(!after.truncated);
        assert!(
            after.entries.len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize,
            "recovery should rewrite snapshot-covered WAL records out of the file"
        );
        assert_eq!(
            after.entries.last().map(|entry| entry.index),
            Some(command_count)
        );
        assert!(
            after
                .entries
                .first()
                .is_some_and(|entry| entry.index > overgrown_log[0].index),
            "repaired WAL should start after the redundant pre-retention records"
        );
    }

    #[test]
    fn durable_raft_open_repairs_gapped_valid_wal_tail() {
        let root = temp_raft_root("sinomo-kv-gapped-valid-wal");
        let node_id = NodeId::from_legacy_named("node_gapped_valid_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            let first = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/gap/one".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append first");
            node.commit_to(first.index).expect("commit first");
        }
        {
            let mut wal = OpenOptions::new()
                .append(true)
                .open(&paths.wal)
                .expect("open wal");
            write_wal_record(
                &mut wal,
                &KvLogEntry {
                    index: 3,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/gap/three".to_string(),
                        value: b"three".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                &paths.wal,
            )
            .expect("write gapped valid tail");
            wal.sync_data().expect("sync gapped tail");
        }

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered.store().get("/gap/one").expect("kept").value,
            b"one"
        );
        assert!(
            recovered.store().get("/gap/three").is_none(),
            "gapped valid WAL tail must not be applied"
        );
        assert_eq!(recovered.hard_state().commit_index, 1);
        drop(recovered);

        let repaired = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert_eq!(
            repaired
                .entries
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![1],
            "WAL rewrite should keep only the contiguous safe prefix"
        );
        assert!(!repaired.truncated);
    }

    #[test]
    fn durable_raft_open_repairs_same_term_conflicting_wal_tail() {
        let root = temp_raft_root("sinomo-kv-conflicting-valid-wal");
        let node_id = NodeId::from_legacy_named("node_conflicting_valid_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/conflict/original".to_string(),
                    value: b"original".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append original");
        }
        {
            let mut wal = OpenOptions::new()
                .append(true)
                .open(&paths.wal)
                .expect("open wal");
            write_wal_record(
                &mut wal,
                &KvLogEntry {
                    index: 1,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/conflict/replacement".to_string(),
                        value: b"replacement".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                &paths.wal,
            )
            .expect("write conflicting valid tail");
            wal.sync_data().expect("sync conflicting tail");
        }

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(recovered.last_log_index(), 1);
        assert_eq!(
            recovered.log()[0].command,
            KvCommand::Op(KvOp::Put {
                key: "/conflict/original".to_string(),
                value: b"original".to_vec(),
                lease_id: None,
            })
        );
        drop(recovered);

        let repaired = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert_eq!(repaired.entries.len(), 1);
        assert_eq!(
            repaired.entries[0].command,
            KvCommand::Op(KvOp::Put {
                key: "/conflict/original".to_string(),
                value: b"original".to_vec(),
                lease_id: None,
            })
        );
        assert!(!repaired.truncated);
    }

    #[test]
    fn wal_rewrite_cleans_temp_file_after_replace_failure() {
        let root = temp_raft_root("sinomo-kv-wal-temp-cleanup");
        let path = root.join("wal.sobjlog");
        std::fs::create_dir_all(&path).expect("target directory");
        let entry = KvLogEntry {
            index: 1,
            term: 1,
            command: KvCommand::Op(KvOp::Delete {
                key: "/wal/temp-cleanup".to_string(),
            }),
            revision: KvRevision::ZERO,
        };

        let error = rewrite_wal_entries(&path, &[entry]).expect_err("directory replace fails");
        assert!(
            error.to_string().contains("wal.sobjlog"),
            "unexpected error: {error}"
        );
        assert!(
            fs::read_dir(&root)
                .expect("raft dir")
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
                .all(|name| !name.starts_with(".wal.sobjlog.tmp-")),
            "failed WAL rewrite must not leave staged temp files behind"
        );
    }

    #[test]
    fn durable_raft_ignores_leftover_wal_rewrite_temp_file() {
        let root = temp_raft_root("sinomo-kv-leftover-wal-temp");
        let node_id = NodeId::from_legacy_named("node_leftover_wal_temp").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            let committed = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/committed".to_string(),
                        value: b"kept".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(committed.index).expect("commit");
        }

        let temp_path = paths.root.join(".wal.sobjlog.tmp-leftover");
        {
            let mut temp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .expect("open leftover temp");
            write_wal_record(
                &mut temp,
                &KvLogEntry {
                    index: 2,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/temp-only".to_string(),
                        value: b"ignored".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                &temp_path,
            )
            .expect("write temp entry");
            temp.sync_all().expect("sync temp");
        }

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get("/committed")
                .expect("committed value")
                .value,
            b"kept"
        );
        assert!(
            recovered.store().get("/temp-only").is_none(),
            "leftover WAL rewrite temp files must not be replayed"
        );
    }

    #[test]
    fn durable_raft_rejects_oversized_command_before_wal_write() {
        let root = temp_raft_root("sinomo-kv-oversized-command-before-wal");
        let node_id = NodeId::from_legacy_named("node_oversized_command").expect("node id");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open node");

        let error = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/large/value".to_string(),
                    value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                    lease_id: None,
                }),
            )
            .expect_err("value limit");

        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
        assert_eq!(node.last_log_index(), 0);
        assert!(
            !paths.wal.exists(),
            "oversized command should be rejected before WAL creation"
        );
    }

    #[test]
    fn durable_raft_rejects_oversized_wal_record_before_allocation() {
        let root = temp_raft_root("sinomo-kv-oversized-wal-record");
        let node_id = NodeId::from_legacy_named("node_oversized_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            let committed = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/before-oversized".to_string(),
                        value: b"kept".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(committed.index).expect("commit");
        }

        let mut wal = OpenOptions::new()
            .append(true)
            .open(&paths.wal)
            .expect("open wal");
        wal.write_all(&((MAX_WAL_RECORD_BYTES as u32 + 1).to_be_bytes()))
            .expect("write oversized length");
        wal.write_all(b"ignored").expect("write tail");
        drop(wal);

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get("/before-oversized")
                .expect("valid entry before oversized record")
                .value,
            b"kept"
        );
        let rewritten = std::fs::read(&paths.wal).expect("rewritten wal");
        assert!(
            !rewritten
                .windows(b"ignored".len())
                .any(|window| window == b"ignored"),
            "oversized WAL tail should be truncated without allocating its advertised size"
        );
    }

    #[test]
    fn snapshot_cache_validates_full_identity_and_stays_bounded() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        // Inode discrimination: a same-(mtime, len) rewrite lands on a fresh inode
        // (temp + atomic rename), so it must miss rather than serve a stale parse.
        let root = temp_raft_root("kv-snapshot-cache-identity");
        let path = root.join("snap.sobj");
        let snapshot = Arc::new(InProcessKvStore::new().snapshot().expect("snapshot"));
        let mtime = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000));
        let id = FileIdentity {
            mtime,
            len: 4096,
            ino: 11,
        };
        cache_store_snapshot(&path, id, snapshot.clone());

        assert!(
            cache_lookup_snapshot(&path, id).is_some(),
            "exact identity must hit"
        );
        assert!(
            cache_lookup_snapshot(&path, FileIdentity { ino: 12, ..id }).is_none(),
            "same mtime+len but a new inode must miss (closes the mtime-granularity window)"
        );
        assert!(
            cache_lookup_snapshot(&path, FileIdentity { len: 4097, ..id }).is_none(),
            "a different length must miss"
        );
        assert!(
            cache_lookup_snapshot(&path, FileIdentity { mtime: None, ..id }).is_none(),
            "an unverifiable mtime must never hit"
        );
        materialized_snapshot_cache()
            .lock()
            .expect("cache")
            .remove(&path);

        // Capacity bound: inserting past the cap must never grow the map beyond it.
        // (Asserted as an invariant — robust even while other tests touch the cache.)
        for i in 0..(FILE_CACHE_CAPACITY + 64) {
            let path = std::path::PathBuf::from(format!("/sinomo-kv-cap-test/{i}.sobj"));
            cache_store_snapshot(
                &path,
                FileIdentity {
                    mtime,
                    len: 1,
                    ino: i as u64,
                },
                snapshot.clone(),
            );
        }
        let len = materialized_snapshot_cache().lock().expect("cache").len();
        assert!(
            len <= FILE_CACHE_CAPACITY,
            "materialized snapshot cache must stay within capacity, got {len}"
        );
        materialized_snapshot_cache()
            .lock()
            .expect("cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-cap-test"));
    }

    #[test]
    fn kv_read_caches_retain_recent_entries_at_capacity() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        materialized_snapshot_cache().lock().expect("cache").clear();
        raft_view_cache().lock().expect("raft cache").clear();
        hard_state_cache().lock().expect("hard-state cache").clear();
        KV_READ_CACHE_CLOCK.store(1, Ordering::Relaxed);

        let snapshot = Arc::new(InProcessKvStore::new().snapshot().expect("snapshot"));
        let hard_state = RaftHardState::default();
        let mtime = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2_000));
        for index in 0..FILE_CACHE_CAPACITY.saturating_sub(1) {
            cache_store_snapshot(
                &PathBuf::from(format!("/sinomo-kv-lru-test/100-quiet-{index:04}.sobj")),
                FileIdentity {
                    mtime,
                    len: 1,
                    ino: (index + 1) as u64,
                },
                snapshot.clone(),
            );
            cache_store_hard_state(
                &PathBuf::from(format!(
                    "/sinomo-kv-lru-test/100-quiet-hard-{index:04}.sobj"
                )),
                None,
                hard_state.clone(),
            );
            let quiet_paths =
                KvWalPaths::new(format!("/sinomo-kv-lru-test/100-quiet-raft-{index:04}"));
            cache_store_raft_view(
                &quiet_paths,
                RaftViewIdentity {
                    hard_state: None,
                    wal: None,
                    snapshot: None,
                },
                snapshot.clone(),
            );
        }

        let hot_snapshot_path = PathBuf::from("/sinomo-kv-lru-test/000-hot.sobj");
        let hot_snapshot_identity = FileIdentity {
            mtime,
            len: 1,
            ino: (FILE_CACHE_CAPACITY + 1) as u64,
        };
        cache_store_snapshot(&hot_snapshot_path, hot_snapshot_identity, snapshot.clone());
        let hot_hard_state_path = PathBuf::from("/sinomo-kv-lru-test/000-hot-hard.sobj");
        let hot_hard_state_identity = None;
        cache_store_hard_state(
            &hot_hard_state_path,
            hot_hard_state_identity,
            hard_state.clone(),
        );
        let hot_raft_paths = KvWalPaths::new("/sinomo-kv-lru-test/000-hot-raft");
        let hot_raft_identity = RaftViewIdentity {
            hard_state: None,
            wal: None,
            snapshot: None,
        };
        cache_store_raft_view(&hot_raft_paths, hot_raft_identity, snapshot.clone());

        assert!(
            cache_lookup_snapshot(&hot_snapshot_path, hot_snapshot_identity).is_some(),
            "looking up the hot materialized snapshot should refresh its recency"
        );
        assert!(
            cache_lookup_hard_state(&hot_hard_state_path, hot_hard_state_identity).is_some(),
            "looking up the hot hard-state cache entry should refresh its recency"
        );
        assert!(
            cache_lookup_raft_view(&hot_raft_paths, hot_raft_identity).is_some(),
            "looking up the hot raft view should refresh its recency"
        );

        cache_store_snapshot(
            &PathBuf::from("/sinomo-kv-lru-test/999-extra.sobj"),
            FileIdentity {
                mtime,
                len: 1,
                ino: (FILE_CACHE_CAPACITY + 2) as u64,
            },
            snapshot.clone(),
        );
        cache_store_hard_state(
            &PathBuf::from("/sinomo-kv-lru-test/999-extra-hard.sobj"),
            None,
            hard_state,
        );
        cache_store_raft_view(
            &KvWalPaths::new("/sinomo-kv-lru-test/999-extra-raft"),
            hot_raft_identity,
            snapshot,
        );

        assert!(
            cache_lookup_snapshot(&hot_snapshot_path, hot_snapshot_identity).is_some(),
            "recent materialized snapshot cache entry should survive capacity churn"
        );
        assert!(
            cache_lookup_hard_state(&hot_hard_state_path, hot_hard_state_identity).is_some(),
            "recent hard-state cache entry should survive capacity churn"
        );
        assert!(
            cache_lookup_raft_view(&hot_raft_paths, hot_raft_identity).is_some(),
            "recent raft-view cache entry should survive capacity churn"
        );
        materialized_snapshot_cache()
            .lock()
            .expect("cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-lru-test"));
        raft_view_cache()
            .lock()
            .expect("raft cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-lru-test"));
        hard_state_cache()
            .lock()
            .expect("hard-state cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-lru-test"));
    }

    #[test]
    fn concurrent_write_object_file_to_same_path_is_safe() {
        let root = temp_raft_root("kv-concurrent-object-file");
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("object.sobj");
        let value: Vec<u32> = vec![7, 8, 9, 10];

        let threads = 8usize;
        let writes_per_thread = 32usize;
        let barrier = Arc::new(std::sync::Barrier::new(threads));
        let mut handles = Vec::new();
        for _ in 0..threads {
            let path = path.clone();
            let value = value.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..writes_per_thread {
                    write_object_file(&path, &value).expect("write object file");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread");
        }

        let read: Vec<u32> = read_object_file(&path).expect("read object file");
        assert_eq!(
            read, value,
            "concurrent identical writes must leave valid, complete content"
        );
        let leftover: Vec<_> = std::fs::read_dir(&root)
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains(".tmp-"))
            })
            .collect();
        assert!(
            leftover.is_empty(),
            "no temporary object files should remain, found {leftover:?}"
        );
    }

    fn temp_raft_root(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }
}
