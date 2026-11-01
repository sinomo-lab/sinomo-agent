//! Ignored controls: real resident native Raft owner, no fabricated commit.
//! Imported/read-cache replacement cases label their DATA boundary explicitly.
use super::*;
use crate::keyspace::cluster_member_key;
use sinomo_base::{ClusterBootstrapState, ClusterMemberState};
use sinomo_node::ClusterId;
use sinomo_kv::{KvCommand, KvOp, KvRevision};

fn resident(
    name: &str,
) -> (
    sinomo_testdir::TestDir,
    ClusterPaths,
    ClusterBootstrapState,
    KvSnapshot,
) {
    let root = sinomo_testdir::TestDir::new(name);
    let data = sinomo_local::SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data);
    paths.ensure().unwrap();
    let node = NodeId::generate();
    let state = ClusterBootstrapState::initialized_locally(
        ClusterId::generate(),
        name,
        node.clone(),
        "local",
        "public-key",
        OffsetDateTime::UNIX_EPOCH,
    );
    crate::write_cluster_state(&paths, &state).unwrap();
    let snapshot = crate::append_cluster_raft_command(
        &paths,
        &node,
        KvCommand::Op(KvOp::put_serde(cluster_member_key(&node), &state.members[0], None).unwrap()),
    )
    .unwrap();
    // The existing observational fallback must be a real native snapshot,
    // not a synthetic empty read model when the actual owner is unavailable.
    crate::write_cluster_kv_snapshot(&paths, &snapshot).unwrap();
    assert!(
        sinomo_kv::with_cluster_kv_resident_read_projection(
            &paths.kv_paths().raft_wal_paths(&node),
            &node,
            |p| p.read_index_state().revision
        )
        .unwrap()
        .is_ok()
    );
    (root, paths, state, snapshot)
}
fn members(input: &ClusterDirectoryKvInput) -> Vec<ClusterMemberRecord> {
    read_cluster_members_from_directory_entries(input).unwrap()
}
fn full_native(paths: &ClusterPaths, state: &ClusterBootstrapState) -> KvSnapshot {
    sinomo_kv::read_cluster_kv_local_snapshot(&paths.kv_paths(), &state.local_node_id)
        .unwrap()
        .value
}

#[test]
fn resident_directory_cut_preserves_full_memo_after_real_no_effect_commit() {
    let (_root, paths, state, before) = resident("directory-native-no-effect-memo");
    let first = read_cluster_directory_input(&paths).unwrap().unwrap();
    let projection =
        read_cluster_directory_projection_from_input_at_module_generation(&paths, first.clone(), 0)
            .unwrap();
    let actual = crate::append_cluster_raft_command(
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
    assert_eq!(
        actual.revision, before.revision,
        "actual semantic no-effect, not a made-up generation"
    );
    assert!(actual.committed_requests.len() > before.committed_requests.len());
    let second = read_cluster_directory_input(&paths).unwrap().unwrap();
    assert_eq!(first.revision, second.revision);
    assert_eq!(first.entries, second.entries);
    assert!(second.entries.iter().all(|entry| {
        CLUSTER_DIRECTORY_PREFIXES
            .iter()
            .any(|prefix| entry.key.starts_with(prefix))
    }));
    let after_projection =
        read_cluster_directory_projection_from_input_at_module_generation(&paths, second, 0)
            .unwrap();
    assert!(
        Arc::ptr_eq(&projection, &after_projection),
        "unchanged routing payload reuses its existing decoded owner"
    );
    let full = full_native(&paths, &state);
    assert_eq!(
        full.committed_requests, actual.committed_requests,
        "full native read keeps real no-effect request memo"
    );
    assert_eq!(full.expired_requests, actual.expired_requests);
}

#[test]
fn resident_directory_uses_newer_imported_rows_and_native_rows_on_revision_tie() {
    let (_root, paths, state, native) = resident("directory-native-imported-selection");
    let original = members(&read_cluster_directory_input(&paths).unwrap().unwrap());
    let mut import_store = sinomo_kv::InProcessKvStore::restore(native.clone()).unwrap();
    let joined = ClusterMemberRecord::pending_learner(
        state.cluster_id.clone(),
        NodeId::generate(),
        "imported-joined",
        "pk-joined",
        OffsetDateTime::UNIX_EPOCH,
    );
    import_store
        .put_serde(cluster_member_key(&joined.node_id), &joined, None)
        .unwrap();
    let imported = import_store.try_snapshot().unwrap();
    // This is the normal materialized importer seam, using explicitly DATA
    // membership input as existing latest-observed controls do. It is NOT a
    // claim that these rows are a new authenticated quorum certificate.
    crate::write_imported_cluster_kv_snapshot_if_changed(&paths, &imported).unwrap();
    let selected = read_cluster_directory_input(&paths).unwrap().unwrap();
    assert_eq!(selected.revision, imported.revision);
    assert!(
        members(&selected)
            .iter()
            .any(|m| m.node_id == joined.node_id)
    );
    assert_eq!(
        full_native(&paths, &state).entries,
        native.entries,
        "import leaves actual resident state unchanged"
    );
    let mut tie = native.clone();
    tie.entries
        .retain(|entry| !entry.key.starts_with("/cluster/members/"));
    // Actual same-revision read-cache replacement must not win over a readable
    // native cut. Do not counterfeit an authority proof or native current row.
    crate::write_cluster_kv_snapshot(&paths, &tie).unwrap();
    assert_eq!(
        members(&read_cluster_directory_input(&paths).unwrap().unwrap()),
        original
    );
}

struct ClearDenial(std::path::PathBuf);
impl Drop for ClearDenial {
    fn drop(&mut self) {
        sinomo_kv::clear_process_local_raft_access_denial_for_test(&self.0);
    }
}
#[test]
fn checked_out_and_shadow_directory_keep_existing_materialized_fallback() {
    let (_root, paths, state, native) = resident("directory-native-busy-shadow");
    let expected = members(&read_cluster_directory_input(&paths).unwrap().unwrap());
    let mut owner = paths
        .kv_paths()
        .local_raft_command_storage(state.local_node_id.clone(), |_| false);
    owner.read_index_state().unwrap();
    assert!(
        sinomo_kv::with_cluster_kv_resident_read_projection(
            &paths.kv_paths().raft_wal_paths(&state.local_node_id),
            &state.local_node_id,
            |p| p.read_index_state().revision
        )
        .is_none()
    );
    assert_eq!(
        members(&read_cluster_directory_input(&paths).unwrap().unwrap()),
        expected
    );
    drop(owner);
    let gate = ClearDenial(paths.kv_paths().raft.clone());
    sinomo_kv::deny_process_local_raft_access_for_root(gate.0.clone()).unwrap();
    assert!(
        sinomo_kv::with_cluster_kv_resident_read_projection(
            &paths.kv_paths().raft_wal_paths(&state.local_node_id),
            &state.local_node_id,
            |p| p.read_index_state().revision
        )
        .is_none()
    );
    let shadow = read_cluster_directory_input(&paths).unwrap().unwrap();
    assert_eq!(shadow.revision, native.revision);
    assert_eq!(members(&shadow), expected);
    drop(gate);
}

#[test]
fn directory_higher_term_fence_and_actual_root_replacement_never_reuse_old_rows() {
    let (_term_root, paths, state, _) = resident("directory-native-owner-term-fence");
    let store = paths.kv_paths().raft_store();
    store
        .publish_pending_higher_term(&state.local_node_id, 2)
        .unwrap();
    assert!(
        read_cluster_directory_input(&paths).is_err(),
        "pending higher term must not fall back to old directory success"
    );
    store.observe_term_compact(&state.local_node_id, 2).unwrap();
    drop(store);
    // A higher-term floor is rooted at the real pathname and intentionally
    // outlives physical DATA replacement. Use another actual native owner for
    // the independent replacement negative, without clearing that floor.
    let (root, paths, state, old) = resident("directory-native-owner-replacement");
    let first = read_cluster_directory_input(&paths).unwrap().unwrap();
    // Explicit physical DATA replacement negative. Preserve the old tree;
    // write the new tree only through the ordinary native Raft producer.
    std::fs::rename(&paths.kv_paths().raft, root.join("original-raft-retained")).unwrap();
    let mut replacement = state.members[0].clone();
    replacement.state = ClusterMemberState::Learner;
    let fresh = crate::append_cluster_raft_command(
        &paths,
        &state.local_node_id,
        KvCommand::Op(
            KvOp::put_serde(cluster_member_key(&state.local_node_id), &replacement, None).unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(fresh.revision, KvRevision(1));
    assert_eq!(
        fresh.revision, old.revision,
        "same revision alone cannot fence physical replacement"
    );
    let second = read_cluster_directory_input(&paths).unwrap().unwrap();
    assert_ne!(first.entries, second.entries);
    assert_eq!(members(&second)[0].state, ClusterMemberState::Learner);
}

#[path = "directory_input_native_tests/member_state_native_tests.rs"]
mod member_state_native_tests;
