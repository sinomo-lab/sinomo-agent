use super::*;
use sinomo_base::{ClusterMemberRole, Id};
use sinomo_kv::{KvAuthorityProof, KvEntry, KvRevision, KvVersion, encode_kv_value};
use time::OffsetDateTime;

struct Fixture {
    _root: sinomo_testdir::TestDir,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    request: EndpointRecoveryRequest,
    proof: KvAuthoritativeRead<KvSnapshot>,
    calls: usize,
}

fn entry<T: Serialize>(key: String, value: &T) -> KvEntry {
    KvEntry {
        key,
        value: encode_kv_value(value).unwrap(),
        create_revision: KvRevision(1),
        modify_revision: KvRevision(57),
        version: KvVersion(1),
        lease_id: None,
    }
}

impl Fixture {
    fn new() -> Self {
        let root = sinomo_testdir::TestDir::new("endpoint-recovery");
        let data = sinomo_local::SinomoDataDir::new(root.path());
        data.ensure_root_layout().unwrap();
        let paths = ClusterPaths::new(&data);
        paths.ensure().unwrap();
        let local = NodeId::generate();
        let peer = NodeId::generate();
        let other = NodeId::generate();
        let cluster = Id::generate();
        let now = OffsetDateTime::now_utc();
        let key = sinomo_node::load_or_create_main_node_identity(&data.root()).unwrap();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster.clone(),
            "isolated",
            local.clone(),
            "learner",
            key.public_key,
            now,
        );
        crate::apply_cluster_raft_role(&mut state.members[0], ClusterMemberRole::Learner, now);
        state.members.push(ClusterMemberRecord::local_voter(
            cluster.clone(),
            peer.clone(),
            "peer",
            "peer-key",
            now,
        ));
        state.members.push(ClusterMemberRecord::local_voter(
            cluster.clone(),
            other.clone(),
            "other",
            "other-key",
            now,
        ));
        state.term = 1;
        state.kv_revision = 0;
        crate::write_cluster_state(&paths, &state).unwrap();
        let endpoint = sinomo_network::daemon_lan_endpoint(&peer, "127.0.0.1:21353".into(), 5);
        let mut published_peer = state.members[1].clone();
        published_peer.endpoints.push(endpoint.clone());
        let config = ClusterKvRaftConfigRecord {
            cluster_id: cluster.clone(),
            term: 1,
            kv_revision: 57,
            voters: vec![peer.clone(), other],
            learners: vec![local.clone()],
            non_voting: vec![],
            updated_at: now,
        };
        let mut entries = vec![
            entry(cluster_member_key(&local), &state.members[0]),
            entry(cluster_member_key(&peer), &published_peer),
            entry(cluster_kv_raft_config_key(), &config),
        ];
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        let proof = KvAuthoritativeRead {
            value: KvSnapshot {
                revision: KvRevision(57),
                entries,
                compacted_revision: KvRevision(0),
                leases: vec![],
                events: vec![],
                committed_requests: vec![],
                expired_requests: vec![],
            },
            authority: KvAuthorityProof {
                term: 1,
                revision: KvRevision(57),
                commit_index: 57,
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let request = EndpointRecoveryRequest {
            spec: EndpointRecoverySpec {
                operation_id: Id::generate(),
                cluster_id: cluster,
                local_node_id: local,
                peer_node_id: peer,
                peer_public_key: "peer-key".into(),
                endpoint_id: endpoint.endpoint_id,
                address: endpoint.address,
                expected_state_digest: endpoint_recovery_state_digest(&state).unwrap(),
            },
            action: EndpointRecoveryAction::Apply,
        };
        Self {
            _root: root,
            paths,
            state,
            request,
            proof,
            calls: 0,
        }
    }
    fn run(
        &mut self,
        action: EndpointRecoveryAction,
    ) -> Result<EndpointRecoveryResponse, NetworkError> {
        let paths = self.paths.clone();
        let mut request = self.request.clone();
        request.action = action;
        recover_endpoint(&paths, &request, self)
    }
    fn stored(&self) -> ClusterBootstrapState {
        read_object_file(&self.paths.state).unwrap()
    }
    fn modify_member(&mut self, id: NodeId, f: impl FnOnce(&mut ClusterMemberRecord)) {
        let e = self
            .proof
            .value
            .entries
            .iter_mut()
            .find(|e| e.key == cluster_member_key(&id))
            .unwrap();
        let mut m = e.decode().unwrap();
        f(&mut m);
        e.value = encode_kv_value(&m).unwrap();
    }
    fn reject_unchanged(&mut self) {
        let before = std::fs::read(&self.paths.state).unwrap();
        assert!(self.run(EndpointRecoveryAction::Apply).is_err());
        assert_eq!(std::fs::read(&self.paths.state).unwrap(), before);
        assert!(!receipt_path(&self.paths, &self.request.spec).exists());
    }
}
impl EndpointRecoveryHost for Fixture {
    fn authoritative_read(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        assert_eq!(state, &self.state);
        assert_eq!(endpoint.node_id, self.request.spec.peer_node_id);
        self.calls += 1;
        Ok(self.proof.clone())
    }
}

#[test]
fn lost_route_adds_only_endpoint_and_cold_receipt_replay_is_noop() {
    let mut f = Fixture::new();
    let before = f.state.clone();
    assert_eq!(
        f.run(EndpointRecoveryAction::Apply).unwrap().outcome,
        EndpointRecoveryOutcome::Applied
    );
    let mut after = f.stored();
    assert_eq!(after.members[1].endpoints.len(), 1);
    after.members[1].endpoints.clear();
    assert_eq!(after, before);
    let bytes = std::fs::read(&f.paths.state).unwrap();
    assert_eq!(
        f.run(EndpointRecoveryAction::Status).unwrap().outcome,
        EndpointRecoveryOutcome::Applied
    );
    assert_eq!(
        f.run(EndpointRecoveryAction::Apply).unwrap().outcome,
        EndpointRecoveryOutcome::Applied
    );
    assert_eq!(f.calls, 1);
    assert_eq!(std::fs::read(&f.paths.state).unwrap(), bytes);
}

#[test]
fn rollback_restores_original_bytes_and_original_id_cannot_resurrect_route() {
    let mut f = Fixture::new();
    let before = std::fs::read(&f.paths.state).unwrap();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    assert_eq!(
        f.run(EndpointRecoveryAction::Rollback).unwrap().outcome,
        EndpointRecoveryOutcome::RolledBack
    );
    assert_eq!(
        f.run(EndpointRecoveryAction::Apply).unwrap().outcome,
        EndpointRecoveryOutcome::RolledBack
    );
    assert_eq!(std::fs::read(&f.paths.state).unwrap(), before);
    assert_eq!(f.calls, 1);
}

#[test]
fn wrong_peer_key_is_rejected() {
    let mut f = Fixture::new();
    f.request.spec.peer_public_key = "wrong".into();
    f.reject_unchanged();
    assert_eq!(f.calls, 0);
}
#[test]
fn wrong_local_identity_is_rejected() {
    let mut f = Fixture::new();
    f.request.spec.local_node_id = Id::generate();
    f.reject_unchanged();
}
#[test]
fn foreign_cluster_is_rejected() {
    let mut f = Fixture::new();
    f.request.spec.cluster_id = Id::generate();
    f.reject_unchanged();
}
#[test]
fn unregistered_peer_is_rejected() {
    let mut f = Fixture::new();
    f.request.spec.peer_node_id = Id::generate();
    f.reject_unchanged();
}
#[test]
fn authoritative_stale_peer_key_is_rejected() {
    let mut f = Fixture::new();
    f.modify_member(f.request.spec.peer_node_id.clone(), |m| {
        m.node_public_key = "rotated".into()
    });
    f.reject_unchanged();
}
#[test]
fn authoritative_stale_local_key_is_rejected() {
    let mut f = Fixture::new();
    f.modify_member(f.request.spec.local_node_id.clone(), |m| {
        m.node_public_key = "rotated".into()
    });
    f.reject_unchanged();
}
#[test]
fn authoritative_foreign_member_is_rejected() {
    let mut f = Fixture::new();
    f.modify_member(f.request.spec.peer_node_id.clone(), |m| {
        m.cluster_id = Id::generate()
    });
    f.reject_unchanged();
}
#[test]
fn unpublished_endpoint_is_rejected() {
    let mut f = Fixture::new();
    f.modify_member(f.request.spec.peer_node_id.clone(), |m| m.endpoints.clear());
    f.reject_unchanged();
}
#[test]
fn changed_endpoint_address_is_rejected() {
    let mut f = Fixture::new();
    f.modify_member(f.request.spec.peer_node_id.clone(), |m| {
        m.endpoints[0].address = "127.0.0.1:21354".into()
    });
    f.reject_unchanged();
}
#[test]
fn old_term_is_rejected() {
    let mut f = Fixture::new();
    f.proof.authority.term = 0;
    f.reject_unchanged();
}
#[test]
fn incomplete_quorum_is_rejected() {
    let mut f = Fixture::new();
    f.proof.authority.basis = KvAuthorityBasis::CurrentTermQuorum {
        voter_acks: 1,
        required_quorum: 2,
    };
    f.reject_unchanged();
}
#[test]
fn receipt_authority_cannot_replace_current_quorum() {
    let mut f = Fixture::new();
    f.proof.authority.basis = KvAuthorityBasis::ExactCommittedRequest;
    f.reject_unchanged();
}
#[test]
fn inconsistent_revision_is_rejected() {
    let mut f = Fixture::new();
    f.proof.authority.revision = KvRevision(56);
    f.reject_unchanged();
}
#[test]
fn stale_precondition_is_rejected_without_network() {
    let mut f = Fixture::new();
    f.request.spec.expected_state_digest = "stale".into();
    f.reject_unchanged();
    assert_eq!(f.calls, 0);
}

#[test]
fn repeated_id_changed_body_is_rejected() {
    let mut f = Fixture::new();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let before = f.stored();
    f.request.spec.address = "127.0.0.1:21400".into();
    assert!(f.run(EndpointRecoveryAction::Apply).is_err());
    assert_eq!(f.stored(), before);
    assert_eq!(f.calls, 1);
}

#[test]
fn rollback_does_not_overwrite_later_state() {
    let mut f = Fixture::new();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let mut newer = f.stored();
    newer.kv_revision = 58;
    newer.members[0].display_name = "concurrently renamed".into();
    crate::write_cluster_state(&f.paths, &newer).unwrap();
    assert!(f.run(EndpointRecoveryAction::Rollback).is_err());
    assert_eq!(f.stored(), newer);
    let result = f.run(EndpointRecoveryAction::Status).unwrap();
    assert_eq!(result.outcome, EndpointRecoveryOutcome::Applied);
    assert!(!result.matches_recorded_state);
}

#[test]
fn prepared_failure_retries_only_same_body_with_fresh_authority() {
    let mut f = Fixture::new();
    FAIL_AFTER.set(Some("prepared"));
    assert!(f.run(EndpointRecoveryAction::Apply).is_err());
    assert_eq!(f.stored(), f.state);
    assert_eq!(
        f.run(EndpointRecoveryAction::Status).unwrap().outcome,
        EndpointRecoveryOutcome::Prepared
    );
    f.run(EndpointRecoveryAction::Apply).unwrap();
    assert_eq!(f.calls, 2);
}

#[test]
fn lost_terminal_receipt_resolves_exact_applied_state_without_second_effect() {
    let mut f = Fixture::new();
    FAIL_AFTER.set(Some("applied-state"));
    assert!(f.run(EndpointRecoveryAction::Apply).is_err());
    let bytes = std::fs::read(&f.paths.state).unwrap();
    assert_eq!(
        f.run(EndpointRecoveryAction::Status).unwrap().outcome,
        EndpointRecoveryOutcome::Applied
    );
    assert_eq!(
        f.run(EndpointRecoveryAction::Apply).unwrap().outcome,
        EndpointRecoveryOutcome::Applied
    );
    assert_eq!(f.calls, 1);
    assert_eq!(std::fs::read(&f.paths.state).unwrap(), bytes);
}

#[test]
fn ambiguous_state_drift_after_interrupted_write_stays_conflict() {
    let mut f = Fixture::new();
    FAIL_AFTER.set(Some("applied-state"));
    assert!(f.run(EndpointRecoveryAction::Apply).is_err());
    let mut newer = f.stored();
    newer.kv_revision = 58;
    newer.members[0].display_name = "concurrently renamed".into();
    crate::write_cluster_state(&f.paths, &newer).unwrap();
    assert_eq!(
        f.run(EndpointRecoveryAction::Status).unwrap().outcome,
        EndpointRecoveryOutcome::Conflict
    );
    assert!(f.run(EndpointRecoveryAction::Apply).is_err());
    assert_eq!(f.stored(), newer);
}

#[test]
fn interrupted_rollback_retains_direction_and_never_reapplies() {
    for stage in ["rollback-intent", "rollback-state"] {
        let mut f = Fixture::new();
        f.run(EndpointRecoveryAction::Apply).unwrap();
        FAIL_AFTER.set(Some(stage));
        assert!(f.run(EndpointRecoveryAction::Rollback).is_err());
        let result = f.run(EndpointRecoveryAction::Apply);
        if stage == "rollback-intent" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().outcome, EndpointRecoveryOutcome::RolledBack);
        }
        assert_eq!(
            f.run(EndpointRecoveryAction::Rollback).unwrap().outcome,
            EndpointRecoveryOutcome::RolledBack
        );
        assert_eq!(f.stored(), f.state);
    }
}

#[test]
fn concurrent_local_change_during_authority_read_is_not_overwritten() {
    struct RacingHost {
        paths: ClusterPaths,
        proof: KvAuthoritativeRead<KvSnapshot>,
    }
    impl EndpointRecoveryHost for RacingHost {
        fn authoritative_read(
            &mut self,
            state: &ClusterBootstrapState,
            _: &ClusterEndpointRecord,
        ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
            let mut newer = state.clone();
            newer.members[0].display_name = "changed during network read".into();
            crate::write_cluster_state(&self.paths, &newer)?;
            Ok(self.proof.clone())
        }
    }
    let f = Fixture::new();
    let mut host = RacingHost {
        paths: f.paths.clone(),
        proof: f.proof.clone(),
    };
    assert!(recover_endpoint(&f.paths, &f.request, &mut host).is_err());
    let current = f.stored();
    assert_eq!(
        current.members[0].display_name,
        "changed during network read"
    );
    assert!(current.members[1].endpoints.is_empty());
    assert!(!receipt_path(&f.paths, &f.request.spec).exists());
}

#[test]
fn revoked_authoritative_local_member_is_rejected() {
    let mut f = Fixture::new();
    f.modify_member(f.request.spec.local_node_id.clone(), |m| {
        m.state = sinomo_base::ClusterMemberState::Removed
    });
    f.reject_unchanged();
}

#[test]
fn canonical_endpoint_replacement_rejects_stale_embedded_route() {
    let mut f = Fixture::new();
    let member: ClusterMemberRecord = f
        .proof
        .value
        .get(&cluster_member_key(&f.request.spec.peer_node_id))
        .unwrap()
        .decode()
        .unwrap();
    let mut replaced = member.endpoints[0].clone();
    replaced.address = "127.0.0.1:24000".into();
    f.proof.value.entries.push(entry(
        crate::cluster_endpoint_key(&f.request.spec.peer_node_id, &replaced),
        &replaced,
    ));
    f.proof.value.entries.sort_by(|a, b| a.key.cmp(&b.key));
    f.reject_unchanged();
}

#[test]
fn verified_route_survives_older_projection_without_importing_authority() {
    let mut f = Fixture::new();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let stored = f.stored();
    let mut projected = f.state.members.clone();
    projected[2].display_name = "new projection attribute".into();
    let before = projected.clone();
    preserve_verified_recovery_routes(&f.paths, &stored, &mut projected, KvRevision(1)).unwrap();
    assert_eq!(projected[1].endpoints, stored.members[1].endpoints);
    projected[1].endpoints.clear();
    assert_eq!(projected, before);
    assert_eq!(
        f.stored().kv_revision,
        0,
        "receipt must never advance applied/observed authority"
    );
}

#[test]
fn caught_up_projection_retires_recovered_route_normally() {
    let mut f = Fixture::new();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let mut projected = f.state.members.clone();
    preserve_verified_recovery_routes(&f.paths, &f.stored(), &mut projected, KvRevision(57))
        .unwrap();
    assert!(projected[1].endpoints.is_empty());
}

#[test]
fn revoked_or_rotated_peer_cannot_keep_recovery_hint() {
    for revoke in [false, true] {
        let mut f = Fixture::new();
        f.run(EndpointRecoveryAction::Apply).unwrap();
        let mut projected = f.state.members.clone();
        if revoke {
            projected[1].state = sinomo_base::ClusterMemberState::Removed;
        } else {
            projected[1].node_public_key = "new peer key".into();
        }
        let expected = projected.clone();
        preserve_verified_recovery_routes(&f.paths, &f.stored(), &mut projected, KvRevision(1))
            .unwrap();
        assert_eq!(projected, expected);
    }
}

#[test]
fn ordinary_numeric_listener_without_receipt_is_not_revived() {
    let f = Fixture::new();
    let mut stored = f.state.clone();
    stored.members[1]
        .endpoints
        .push(sinomo_network::daemon_lan_endpoint(
            &f.request.spec.peer_node_id,
            f.request.spec.address.clone(),
            0,
        ));
    let mut projected = f.state.members.clone();
    preserve_verified_recovery_routes(&f.paths, &stored, &mut projected, KvRevision(1)).unwrap();
    assert!(projected[1].endpoints.is_empty());
}

#[test]
fn rollback_tombstone_blocks_hint_from_stale_bootstrap_reader() {
    let mut f = Fixture::new();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let stale = f.stored();
    f.run(EndpointRecoveryAction::Rollback).unwrap();
    let mut projected = f.state.members.clone();
    preserve_verified_recovery_routes(&f.paths, &stale, &mut projected, KvRevision(1)).unwrap();
    assert!(projected[1].endpoints.is_empty());
}

#[test]
fn recovery_hint_does_not_replace_conflicting_projected_endpoint() {
    let mut f = Fixture::new();
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let stored = f.stored();
    let mut projected = f.state.members.clone();
    let mut conflict = stored.members[1].endpoints[0].clone();
    conflict.address = "127.0.0.1:24001".into();
    projected[1].endpoints.push(conflict);
    let expected = projected.clone();
    preserve_verified_recovery_routes(&f.paths, &stored, &mut projected, KvRevision(1)).unwrap();
    assert_eq!(projected, expected);
}

#[test]
fn routing_directory_cache_tracks_apply_and_rollback_without_kv_revision_change() {
    use sinomo_network::ClusterDirectory;
    let mut f = Fixture::new();
    let mut snapshot = f.proof.value.clone();
    snapshot.revision = KvRevision(1);
    snapshot
        .entries
        .retain(|e| e.key.starts_with("/cluster/members/"));
    for e in &mut snapshot.entries {
        let mut m: ClusterMemberRecord = e.decode().unwrap();
        m.endpoints.clear();
        e.value = encode_kv_value(&m).unwrap();
        e.create_revision = KvRevision(1);
        e.modify_revision = KvRevision(1);
    }
    crate::write_cluster_kv_snapshot(&f.paths, &snapshot).unwrap();
    let directory = crate::KvClusterDirectory::new(f.paths.clone());
    let (before, _, endpoints, _) = directory.routing_records_snapshot().unwrap().into_parts();
    assert!(endpoints.is_empty());
    f.run(EndpointRecoveryAction::Apply).unwrap();
    let (applied, _, endpoints, _) = directory.routing_records_snapshot().unwrap().into_parts();
    assert_ne!(before, applied);
    assert!(
        endpoints
            .iter()
            .any(|e| e.endpoint_id == f.request.spec.endpoint_id)
    );
    assert_eq!(directory.version().unwrap().0, 1);
    f.run(EndpointRecoveryAction::Rollback).unwrap();
    let (rolled_back, _, endpoints, _) = directory.routing_records_snapshot().unwrap().into_parts();
    assert_ne!(rolled_back, applied);
    assert!(endpoints.is_empty());
    assert_eq!(directory.version().unwrap().0, 1);
}

#[test]
fn uncertain_state_write_reconciles_route_cache_under_original_operation() {
    use sinomo_network::ClusterDirectory;
    let mut f = Fixture::new();
    let mut snapshot = f.proof.value.clone();
    snapshot.revision = KvRevision(1);
    snapshot
        .entries
        .retain(|e| e.key.starts_with("/cluster/members/"));
    for e in &mut snapshot.entries {
        let mut m: ClusterMemberRecord = e.decode().unwrap();
        m.endpoints.clear();
        e.value = encode_kv_value(&m).unwrap();
        e.create_revision = KvRevision(1);
        e.modify_revision = KvRevision(1);
    }
    crate::write_cluster_kv_snapshot(&f.paths, &snapshot).unwrap();
    let directory = crate::KvClusterDirectory::new(f.paths.clone());
    let (before, _, _, _) = directory.routing_records_snapshot().unwrap().into_parts();
    FAIL_AFTER.set(Some("applied-before-invalidation"));
    assert!(f.run(EndpointRecoveryAction::Apply).is_err());
    assert_eq!(
        f.run(EndpointRecoveryAction::Status).unwrap().outcome,
        EndpointRecoveryOutcome::Applied
    );
    assert_eq!(
        directory.routing_version().unwrap(),
        before,
        "status must not perform recovery work"
    );
    let resumed = f.run(EndpointRecoveryAction::Apply).unwrap();
    assert!(resumed.needs_transport_refresh);
    assert_eq!(f.calls, 1);
    let (applied, _, routes, _) = directory.routing_records_snapshot().unwrap().into_parts();
    assert_ne!(applied, before);
    assert_eq!(routes.len(), 1);
    FAIL_AFTER.set(Some("rollback-before-invalidation"));
    assert!(f.run(EndpointRecoveryAction::Rollback).is_err());
    assert_eq!(
        f.run(EndpointRecoveryAction::Status).unwrap().outcome,
        EndpointRecoveryOutcome::RolledBack
    );
    assert_eq!(directory.routing_version().unwrap(), applied);
    assert!(
        f.run(EndpointRecoveryAction::Rollback)
            .unwrap()
            .needs_transport_refresh
    );
    let (rolled_back, _, routes, _) = directory.routing_records_snapshot().unwrap().into_parts();
    assert_ne!(rolled_back, applied);
    assert!(routes.is_empty());
}
