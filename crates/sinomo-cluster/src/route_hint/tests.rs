use super::*;
use crate::local_state_store::{read_cluster_state, write_cluster_state};
use crate::member_mutation::sync_cluster_state_to_local_raft_unchecked;
use sinomo_base::{ClusterMemberRecord, ClusterMemberState};
use time::OffsetDateTime;

struct Fixture {
    _root: sinomo_testdir::TestDir,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    peer: NodeId,
}

fn fixture(name: &str) -> Fixture {
    let root = sinomo_testdir::TestDir::new(name);
    let data = sinomo_local::SinomoDataDir::new(root.path());
    data.ensure_root_layout().unwrap();
    let paths = ClusterPaths::new(&data);
    paths.ensure().unwrap();
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_node::ClusterId::from_legacy_named(format!("cluster_{name}")).unwrap();
    let local = NodeId::from_legacy_named(format!("node_{name}_local")).unwrap();
    let peer = NodeId::from_legacy_named(format!("node_{name}_peer")).unwrap();
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        name,
        local,
        "local",
        "pk-local",
        now,
    );
    let mut member =
        ClusterMemberRecord::local_voter(cluster_id, peer.clone(), "peer", "pk-peer", now);
    member.state = ClusterMemberState::Active;
    member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
        &peer,
        "10.9.0.2:21353".to_string(),
        0,
    )];
    state.members.push(member);
    write_cluster_state(&paths, &state).unwrap();
    Fixture {
        _root: root,
        paths,
        state,
        peer,
    }
}

fn hint(fixture: &Fixture, address: &str, remove: bool) -> ClusterRouteHintRequest {
    ClusterRouteHintRequest {
        node_id: fixture.peer.clone(),
        address: address.to_string(),
        remove,
    }
}

fn stored_peer_addresses(fixture: &Fixture) -> Vec<String> {
    let state: ClusterBootstrapState = read_object_file(&fixture.paths.state).unwrap();
    state
        .members
        .iter()
        .find(|member| member.node_id == fixture.peer)
        .unwrap()
        .endpoints
        .iter()
        .map(|endpoint| endpoint.address.clone())
        .collect()
}

#[test]
fn records_repeats_and_removes_a_hint_for_an_authorized_peer() {
    let fixture = fixture("route-hint-apply");
    let first =
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, "10.9.1.2:21353", false)).unwrap();
    assert!(first.changed);
    assert!(is_cluster_route_hint_endpoint(&first.endpoint));
    assert_eq!(
        stored_peer_addresses(&fixture),
        ["10.9.0.2:21353", "10.9.1.2:21353"]
    );

    let repeat =
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, "10.9.1.2:21353", false)).unwrap();
    assert!(!repeat.changed);
    let published =
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, "10.9.0.2:21353", false)).unwrap();
    assert!(
        !published.changed,
        "an address the peer publishes needs no hint"
    );

    let removed =
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, "10.9.1.2:21353", true)).unwrap();
    assert!(removed.changed);
    assert_eq!(stored_peer_addresses(&fixture), ["10.9.0.2:21353"]);
}

#[test]
fn rejects_the_local_node_unknown_members_and_unusable_addresses() {
    let fixture = fixture("route-hint-reject");
    let mut local = hint(&fixture, "10.9.1.2:21353", false);
    local.node_id = fixture.state.local_node_id.clone();
    assert!(apply_cluster_route_hint(&fixture.paths, &local).is_err());
    let mut unknown = hint(&fixture, "10.9.1.2:21353", false);
    unknown.node_id = NodeId::from_legacy_named("node_route_hint_unknown").unwrap();
    assert!(matches!(
        apply_cluster_route_hint(&fixture.paths, &unknown),
        Err(NetworkError::UnauthorizedNode { .. })
    ));
    for address in [
        "10.9.1.2",
        "10.9.1.2:0",
        ":21353",
        "0.0.0.0:21353",
        "224.0.0.1:21353",
    ] {
        assert!(
            apply_cluster_route_hint(&fixture.paths, &hint(&fixture, address, false)).is_err(),
            "{address}"
        );
    }
    assert_eq!(stored_peer_addresses(&fixture), ["10.9.0.2:21353"]);
}

#[test]
fn hint_survives_the_kv_merge_until_the_peer_publishes_its_address() {
    let fixture = fixture("route-hint-merge");
    sync_cluster_state_to_local_raft_unchecked(&fixture.paths, &fixture.state).unwrap();
    apply_cluster_route_hint(&fixture.paths, &hint(&fixture, "10.9.1.2:21353", false)).unwrap();

    let merged = read_cluster_state(&fixture.paths).unwrap();
    let peer = merged
        .members
        .iter()
        .find(|member| member.node_id == fixture.peer)
        .unwrap();
    assert!(
        peer.endpoints
            .iter()
            .any(|endpoint| is_cluster_route_hint_endpoint(endpoint)
                && endpoint.address == "10.9.1.2:21353"),
        "committed KV lacks the address, so the hint stays: {:?}",
        peer.endpoints
    );

    let mut published = fixture.state.clone();
    published
        .members
        .iter_mut()
        .find(|member| member.node_id == fixture.peer)
        .unwrap()
        .endpoints
        .push(sinomo_network::daemon_lan_endpoint(
            &fixture.peer,
            "10.9.1.2:21353".to_string(),
            0,
        ));
    sync_cluster_state_to_local_raft_unchecked(&fixture.paths, &published).unwrap();
    let merged = read_cluster_state(&fixture.paths).unwrap();
    let at_address = merged
        .members
        .iter()
        .find(|member| member.node_id == fixture.peer)
        .unwrap()
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.address == "10.9.1.2:21353")
        .collect::<Vec<_>>();
    assert_eq!(at_address.len(), 1, "{at_address:?}");
    assert!(!is_cluster_route_hint_endpoint(at_address[0]));
}

#[test]
fn stale_background_writes_preserve_hint_addition_and_removal() {
    let fixture = fixture("route-hint-background-writer");
    let address = "127.0.0.1:32353";
    apply_cluster_route_hint(&fixture.paths, &hint(&fixture, address, false)).unwrap();
    write_cluster_state(&fixture.paths, &fixture.state).unwrap();
    assert!(stored_peer_addresses(&fixture).contains(&address.to_string()));
    let stale_with_hint: ClusterBootstrapState = read_object_file(&fixture.paths.state).unwrap();
    apply_cluster_route_hint(&fixture.paths, &hint(&fixture, address, true)).unwrap();
    write_cluster_state(&fixture.paths, &stale_with_hint).unwrap();
    assert!(!stored_peer_addresses(&fixture).contains(&address.to_string()));
}

#[test]
fn background_hint_preservation_does_not_cross_key_rotation_or_removal() {
    for rotate in [true, false] {
        let fixture = fixture("route-hint-authority-boundary");
        let address = "127.0.0.1:32353";
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, address, false)).unwrap();
        let mut updated = fixture.state.clone();
        let peer = updated
            .members
            .iter_mut()
            .find(|member| member.node_id == fixture.peer)
            .unwrap();
        if rotate {
            peer.node_public_key = "rotated-key".into();
        } else {
            peer.state = ClusterMemberState::Removed;
        }
        write_cluster_state(&fixture.paths, &updated).unwrap();
        assert!(!stored_peer_addresses(&fixture).contains(&address.to_string()));
    }
}

#[test]
fn raft_and_accepted_projection_writers_preserve_owned_hints() {
    for accepted in [false, true] {
        let fixture = fixture("route-hint-raft-projection");
        let address = "127.0.0.1:32353";
        let write = |state: &ClusterBootstrapState| {
            let _lock = acquire_local_cluster_state_mutation_lock(&fixture.paths).unwrap();
            if accepted {
                crate::local_state_store::write_accepted_cluster_state_unlocked(
                    &fixture.paths,
                    state,
                )
                .unwrap();
            } else {
                crate::local_state_store::write_cluster_state_unlocked(&fixture.paths, state)
                    .unwrap();
            }
        };
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, address, false)).unwrap();
        write(&fixture.state);
        assert!(stored_peer_addresses(&fixture).contains(&address.to_string()));
        let stale: ClusterBootstrapState = read_object_file(&fixture.paths.state).unwrap();
        apply_cluster_route_hint(&fixture.paths, &hint(&fixture, address, true)).unwrap();
        write(&stale);
        assert!(!stored_peer_addresses(&fixture).contains(&address.to_string()));
    }
}
