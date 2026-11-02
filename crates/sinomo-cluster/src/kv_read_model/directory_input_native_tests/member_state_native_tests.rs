//! Cluster-state preparation reuses the actual indexed membership cut.
use super::*;

fn export(root: &sinomo_testdir::TestDir, name: &str, value: serde_json::Value) {
    std::fs::write(root.join(name), serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

#[test]
fn native_member_state_reads_preserve_rich_memo_without_full_snapshot_materialization() {
    let (root, paths, state, initial) = resident("native-member-state-rich-memo");
    let mut committed = initial.clone();
    // Real committed no-effect requests grow memo history while retaining the
    // exact same member payload and semantic KV revision.
    for _ in 0..64 {
        committed = crate::append_cluster_raft_command(
            &paths,
            &state.local_node_id,
            KvCommand::Op(
                KvOp::put_serde(
                    cluster_member_key(&state.local_node_id),
                    &state.members[0],
                    None,
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    assert_eq!(committed.revision, initial.revision);
    assert!(committed.committed_requests.len() >= initial.committed_requests.len() + 64);
    let before = sinomo_kv::InProcessKvStore::complete_snapshot_materializations_for_test();
    let actual_state = crate::read_cluster_state(&paths);
    let actual_consensus = crate::read_cluster_consensus_state(&paths);
    let after = sinomo_kv::InProcessKvStore::complete_snapshot_materializations_for_test();
    let unchanged = full_native(&paths, &state);
    export(
        &root,
        "member-state-read-outcome.json",
        serde_json::json!({
            "qualification":"two actual production state/consensus reads on the current native owner; existing thread-local counter measures only this calling thread; no fake request memo or projection snapshot",
            "initial_revision":initial.revision,"actual_committed_revision":committed.revision,
            "initial_memo_count":initial.committed_requests.len(),"actual_memo_count":committed.committed_requests.len(),
            "snapshot_materializations_before":before,"snapshot_materializations_after":after,
            "state":actual_state.as_ref().map_err(ToString::to_string),
            "consensus":actual_consensus.as_ref().map_err(ToString::to_string),
            "full_native_memo_before":committed.committed_requests,
            "full_native_memo_after":unchanged.committed_requests,
            "full_native_expired_before":committed.expired_requests,
            "full_native_expired_after":unchanged.expired_requests,
        }),
    );
    assert_eq!(actual_state.unwrap().members, state.members);
    assert_eq!(actual_consensus.unwrap().members, state.members);
    assert_eq!(unchanged.entries, committed.entries);
    assert_eq!(unchanged.committed_requests, committed.committed_requests);
    assert_eq!(unchanged.expired_requests, committed.expired_requests);
    assert_eq!(
        after - before,
        0,
        "ready member-state reads must not clone unrelated complete KV history"
    );
}

#[test]
fn native_member_state_reads_keep_imported_tie_busy_shadow_and_owner_fences() {
    let (root, paths, state, _initial_native) = resident("native-member-state-selection");
    let original = crate::read_cluster_state(&paths).unwrap();
    let evicted = sinomo_kv::evict_resident_raft_node_for_test(
        &paths.kv_paths().raft_wal_paths(&state.local_node_id),
    );
    let cold = crate::read_cluster_state(&paths);
    export(
        &root,
        "member-state-cold.json",
        serde_json::json!({
            "evicted_existing_resident":evicted,
            "actual":cold.as_ref().map_err(ToString::to_string),
            "qualification":"existing fixture eviction drops only the resident slot; unchanged real DATA supplies the ordinary cold fallback",
        }),
    );
    assert!(evicted);
    assert_eq!(cold.unwrap().members, original.members);
    // Rewarm through a genuine no-effect native command, never a fabricated
    // read-cache row or authority certificate.
    let native = crate::append_cluster_raft_command(
        &paths,
        &state.local_node_id,
        KvCommand::Op(
            KvOp::put_serde(
                cluster_member_key(&state.local_node_id),
                &state.members[0],
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let joined = ClusterMemberRecord::pending_learner(
        state.cluster_id.clone(),
        NodeId::generate(),
        "imported-member",
        "pk-imported",
        OffsetDateTime::UNIX_EPOCH,
    );
    let mut import_store = sinomo_kv::InProcessKvStore::restore(native.clone()).unwrap();
    import_store
        .put_serde(cluster_member_key(&joined.node_id), &joined, None)
        .unwrap();
    let imported = import_store.try_snapshot().unwrap();
    // Existing materialized importer DATA seam, not a new quorum certificate.
    crate::write_imported_cluster_kv_snapshot_if_changed(&paths, &imported).unwrap();
    let selected = crate::read_cluster_state(&paths);
    export(
        &root,
        "member-state-imported.json",
        serde_json::json!({
            "native_revision":native.revision,"imported_revision":imported.revision,
            "actual":selected.as_ref().map_err(ToString::to_string),"joined":joined,
        }),
    );
    assert!(
        !selected
            .unwrap()
            .members
            .iter()
            .any(|member| member.node_id == joined.node_id),
        "member state reads the resident Raft view, not a newer materialized file"
    );
    // Both same-revision inputs have valid, nonempty but different members.
    // The real native command advances to the same semantic revision as the
    // earlier imported DATA; no snapshot revision or row is forged for a tie.
    let native_joined = ClusterMemberRecord::pending_learner(
        state.cluster_id.clone(),
        NodeId::generate(),
        "native-tie-member",
        "pk-native-tie",
        OffsetDateTime::UNIX_EPOCH,
    );
    let native = crate::append_cluster_raft_command(
        &paths,
        &state.local_node_id,
        KvCommand::Op(
            KvOp::put_serde(
                cluster_member_key(&native_joined.node_id),
                &native_joined,
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(native.revision, imported.revision);
    crate::write_cluster_kv_snapshot(&paths, &imported).unwrap();
    let tied = crate::read_cluster_state(&paths);
    export(
        &root,
        "member-state-valid-tie.json",
        serde_json::json!({
            "qualification":"actual native command and existing imported DATA seam at equal real revisions; both contain distinct nonempty member sets, so fallback cannot mask a wrong selection",
            "native_revision":native.revision,"imported_revision":imported.revision,
            "native_joined":native_joined,"imported_joined":joined,
            "actual":tied.as_ref().map_err(ToString::to_string),
        }),
    );
    let tied = tied.unwrap();
    assert!(
        tied.members
            .iter()
            .any(|member| member.node_id == native_joined.node_id)
    );
    assert!(
        !tied
            .members
            .iter()
            .any(|member| member.node_id == joined.node_id)
    );
    let expected_members = tied.members;
    // Restore the actual native fallback before borrowing its mutable owner.
    crate::write_cluster_kv_snapshot(&paths, &native).unwrap();
    let mut owner = paths
        .kv_paths()
        .local_raft_command_storage(state.local_node_id.clone(), |_| false);
    owner.read_index_state().unwrap();
    let busy = crate::read_cluster_state(&paths);
    export(
        &root,
        "member-state-busy.json",
        serde_json::json!({
            "actual":busy.as_ref().map_err(ToString::to_string),"expected":expected_members,
        }),
    );
    assert_eq!(busy.unwrap().members, expected_members);
    drop(owner);
    let denial = ClearDenial(paths.kv_paths().raft.clone());
    sinomo_kv::deny_process_local_raft_access_for_root(denial.0.clone()).unwrap();
    assert_eq!(
        crate::read_cluster_state(&paths).unwrap().members,
        expected_members
    );
    drop(denial);

    let actual_committed_config = full_native(&paths, &state)
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .cloned();
    let before_projection = sinomo_kv::with_cluster_kv_resident_read_projection(
        &paths.kv_paths().raft_wal_paths(&state.local_node_id),
        &state.local_node_id,
        |projection| projection.read_index_state(),
    );
    let read_projection_before = before_projection
        .as_ref()
        .is_some_and(|result| result.is_ok());
    let actual_before_projection = match &before_projection {
        None => serde_json::json!({"state":"unavailable-no-resident-cut"}),
        Some(Err(error)) => serde_json::json!({"state":"error","detail":error.to_string()}),
        Some(Ok(read)) => serde_json::json!({"state":"available","revision":read.revision,
            "observed_term":read.observed_term,"applied_index":read.applied_index}),
    };
    export(
        &root,
        "member-state-before-term-floor.json",
        serde_json::json!({
            "actual_read_projection":actual_before_projection,
            "actual_committed_config_entry":actual_committed_config,
            "process_local_role_denial":sinomo_kv::process_local_raft_access_is_denied(&paths.kv_paths().raft),
            "qualification":"Shadow denial evicts idle resident state; clearing denial does not warm another owner. An unavailable pre-floor cut is preserved, not interpreted as a healthy authority lease or term rejection.",
        }),
    );
    let store = paths.kv_paths().raft_store();
    store
        .publish_pending_higher_term(&state.local_node_id, 2)
        .unwrap();
    let pending_before = store.pending_higher_term(&state.local_node_id).unwrap();
    let projection = sinomo_kv::with_cluster_kv_resident_read_projection(
        &paths.kv_paths().raft_wal_paths(&state.local_node_id),
        &state.local_node_id,
        |projection| projection.read_index_state(),
    );
    let projection_rejected = !projection.as_ref().is_some_and(|result| result.is_ok());
    let projection_error = projection
        .as_ref()
        .and_then(|result| result.as_ref().err())
        .map(ToString::to_string);
    let higher_term = crate::read_cluster_consensus_state(&paths);
    let pending_after = store.pending_higher_term(&state.local_node_id).unwrap();
    export(
        &root,
        "member-state-higher-term.json",
        serde_json::json!({
            "actual":higher_term.as_ref().map_err(ToString::to_string),
            "actual_committed_config_entry":actual_committed_config,
            "pending_floor_before":pending_before,"pending_floor_after":pending_after,
            "native_read_projection_available_before_floor":read_projection_before,
        "native_read_projection_rejected":projection_rejected,
            "native_read_projection_error":projection_error,
            "qualification":"cold/advisory compatibility only: an unavailable pre-floor cut does not establish healthy-lease rejection. The indexed API must not lend success with the pending floor. The established consensus-state constructor may return Ok with absent committed config; that is not a ReadIndex/publication grant and must not clear the floor. Separate existing direct native controls prove healthy authority fencing.",
        }),
    );
    assert!(
        actual_committed_config.is_none(),
        "this real fixture has no committed config row"
    );
    assert_eq!(pending_before, Some(2));
    assert!(projection_rejected);
    assert_eq!(higher_term.unwrap().members, expected_members);
    assert_eq!(pending_after, pending_before);
    store.observe_term_compact(&state.local_node_id, 2).unwrap();
    drop(store);

    let (replacement_root, paths, state, old) = resident("native-member-state-replacement");
    std::fs::rename(
        &paths.kv_paths().raft,
        replacement_root.join("retained-original-raft"),
    )
    .unwrap();
    let replacement = ClusterMemberRecord::pending_learner(
        state.cluster_id.clone(),
        NodeId::generate(),
        "replacement-member",
        "pk-replacement",
        OffsetDateTime::UNIX_EPOCH,
    );
    let actual = crate::append_cluster_raft_command(
        &paths,
        &state.local_node_id,
        KvCommand::Op(
            KvOp::put_serde(cluster_member_key(&replacement.node_id), &replacement, None).unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(actual.revision, old.revision);
    let replaced = crate::read_cluster_state(&paths);
    export(
        &replacement_root,
        "member-state-replacement.json",
        serde_json::json!({
            "old_revision":old.revision,"actual_revision":actual.revision,
            "actual":replaced.as_ref().map_err(ToString::to_string),"replacement_member":replacement,
        }),
    );
    assert!(
        replaced
            .unwrap()
            .members
            .iter()
            .any(|member| member.node_id == replacement.node_id),
        "same revision cannot reuse a physically replaced native member cut"
    );
}

#[test]
fn member_state_keeps_old_per_member_endpoint_bounds_without_a_new_global_cap() {
    let (root, paths, state, _native) = resident("native-member-state-endpoint-aggregate");
    let other = ClusterMemberRecord::pending_learner(
        state.cluster_id.clone(),
        NodeId::generate(),
        "endpoint-member",
        "pk-endpoints",
        OffsetDateTime::UNIX_EPOCH,
    );
    let mut ops = vec![KvOp::put_serde(cluster_member_key(&other.node_id), &other, None).unwrap()];
    let per_member = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES / 2 + 1;
    assert!(per_member <= CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES);
    let mut expected = Vec::new();
    for (member_index, member) in [&state.members[0], &other].into_iter().enumerate() {
        let mut endpoint_ids = Vec::new();
        for i in 0..per_member {
            let endpoint = sinomo_network::daemon_lan_endpoint(
                &member.node_id,
                format!("198.51.100.{}:{}", member_index + 1, 20_000 + i),
                10,
            );
            ops.push(
                KvOp::put_serde(
                    crate::keyspace::cluster_endpoint_key(&member.node_id, &endpoint),
                    &endpoint,
                    None,
                )
                .unwrap(),
            );
            endpoint_ids.push(endpoint.endpoint_id);
        }
        expected.push((member.node_id.clone(), endpoint_ids));
    }
    crate::kv_local_store::append_cluster_raft_ops_in_batches(&paths, &state.local_node_id, ops)
        .unwrap();
    let actual = crate::read_cluster_state(&paths);
    let directory = read_cluster_directory_input(&paths);
    export(
        &root,
        "member-state-endpoint-bounds.json",
        serde_json::json!({
            "qualification":"native Raft commands, not a fabricated owner or quorum certificate; each member stays within its original endpoint range bound",
            "per_member_endpoint_count":per_member,"aggregate_endpoint_count":per_member * 2,
            "directory_original_aggregate_limit":CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            "actual_state":actual.as_ref().map_err(ToString::to_string),
            "directory_error":directory.as_ref().err().map(ToString::to_string),
        }),
    );
    let actual = actual.unwrap();
    for (node, ids) in expected {
        let member = actual
            .members
            .iter()
            .find(|member| member.node_id == node)
            .unwrap();
        assert!(
            ids.iter().all(|id| member
                .endpoints
                .iter()
                .any(|endpoint| &endpoint.endpoint_id == id)),
            "valid per-member endpoints must not be discarded by a new aggregate cap"
        );
    }
    assert!(
        directory.is_err(),
        "Directory's existing aggregate bound remains unchanged"
    );
}
