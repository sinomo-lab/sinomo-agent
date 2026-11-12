//! Write-set record, guarded transactions, bounds and selection over a real
//! in-process KV store. Each transaction is applied through the store's
//! ordinary compare-and-swap, which is the model's linearizable register.

use super::*;
use crate::{DfsPlacementScope, DfsSubtreePolicy};

const CLUSTER: &str = "cluster_write_set_tests";

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).expect("node id")
}

fn fs() -> DfsFileSystemId {
    crate::sifs_file_system_id()
}

fn shard(primary: &NodeId, replicas: &[&NodeId], epoch: u64) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: fs(),
        shard_id: "root".into(),
        primary_node_id: primary.clone(),
        replica_node_ids: replicas.iter().map(|node| (*node).clone()).collect(),
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    }
}

fn file_system_config(rules: Vec<DfsSubtreePolicy>) -> DfsFileSystemConfig {
    DfsFileSystemConfig {
        file_system_id: fs(),
        display_name: "sifs".into(),
        owner_node_id: node("node_ws_a"),
        subtree_policies: rules,
        retention_policies: BTreeMap::new(),
        module_metadata: BTreeMap::new(),
    }
}

fn member(node_id: &NodeId, write_set_capability: bool) -> ClusterMemberRecord {
    let mut record = ClusterMemberRecord::local_voter(
        sinomo_core::Id::from_legacy_named(CLUSTER).unwrap(),
        node_id.clone(),
        node_id.to_string(),
        format!("pk-{node_id}"),
        time::OffsetDateTime::UNIX_EPOCH,
    );
    record.capabilities = sinomo_core::node::main_daemon_capabilities(&record.role);
    if write_set_capability {
        record
            .capabilities
            .push(sinomo_core::CapabilityName::new(DFS_METADATA_WRITE_SET_V1_CAPABILITY).unwrap());
    }
    record
}

struct Fixture {
    kv: InProcessKvStore,
    nodes: Vec<NodeId>,
}

impl Fixture {
    /// A three-member root shard at epoch 3 whose members all advertise the
    /// write-set capability, plus a fourth member outside the shard.
    fn new() -> Self {
        let nodes = ["node_ws_a", "node_ws_b", "node_ws_c", "node_ws_d"]
            .map(node)
            .to_vec();
        let mut kv = InProcessKvStore::new();
        let config = shard(&nodes[0], &[&nodes[1], &nodes[2]], 3);
        put(&mut kv, metadata_shard_key(&fs(), "root"), &config);
        put(
            &mut kv,
            file_system_config_key(&fs()),
            &file_system_config(Vec::new()),
        );
        for node in &nodes {
            put(&mut kv, cluster_member_key(node), &member(node, true));
        }
        Self { kv, nodes }
    }

    fn cut(&mut self) -> DfsMetadataShardRecordCut {
        let nodes = self.nodes.clone();
        DfsControlPlaneStore::new(&mut self.kv)
            .read_metadata_shard_record_cut(&fs(), "root", &nodes)
            .unwrap()
            .expect("shard exists")
    }

    fn apply(&mut self, change: &DfsMetadataWriteSetChangeRequest) -> bool {
        DfsControlPlaneKvClient::txn(&mut self.kv, change.request.txn.clone())
            .unwrap()
            .succeeded
    }

    fn switch_all(&mut self) -> DfsMetadataShardWriteSet {
        let cut = self.cut();
        let initial = self.nodes[..3].iter().cloned().collect();
        let change = build_metadata_write_set_switch(&cut, &initial).unwrap();
        assert!(self.apply(&change));
        self.cut().write_set_state().switched().cloned().unwrap()
    }
}

fn put<T: Serialize>(kv: &mut InProcessKvStore, key: String, value: &T) {
    InProcessKvStore::put(
        kv,
        key,
        sinomo_core::kv::encode_kv_value(value).unwrap(),
        None,
    )
    .unwrap();
}

fn sorted(mut nodes: Vec<NodeId>) -> Vec<NodeId> {
    nodes.sort();
    nodes
}

fn bounds(w_floor: usize, n: usize) -> DfsMetadataShardBounds {
    DfsMetadataShardBounds { w_floor, n }
}

fn proof(sequence: u64, digit: char) -> crate::DfsMetadataPrefixProof {
    crate::DfsMetadataPrefixProof::from_digest(
        sequence,
        format!(
            "dfs-metadata-prefix-v2:blake3:{}",
            digit.to_string().repeat(64)
        ),
    )
    .unwrap()
}

#[test]
fn write_set_key_is_an_authority_key_outside_the_shard_scan_and_storage_classifier() {
    let key = metadata_shard_write_set_key(&fs(), "root");
    assert!(dfs_metadata_shard_write_set_key_is_exact(&key));
    assert!(crate::dfs_metadata_authority_key_is_exact(&key));
    assert!(!key.starts_with(&format!("{}/", metadata_shards_prefix(&fs()))));
    let entry = |value: &[u8]| KvEntry {
        key: key.clone(),
        value: value.to_vec(),
        create_revision: KvRevision(1),
        modify_revision: KvRevision(1),
        version: sinomo_core::kv::KvVersion(1),
        lease_id: None,
    };
    // A write-set change never reopens metadata stores.
    assert!(
        !crate::metadata_authority_projection::metadata_storage_configuration_changed(
            &key,
            Some(&entry(b"before")),
            Some(&entry(b"after")),
        )
    );
}

#[test]
fn switch_leaves_the_shard_bytes_unchanged_and_other_readers_ignore_the_write_set() {
    let mut fixture = Fixture::new();
    let before = fixture.cut().shard_value;
    let write_set = fixture.switch_all();
    let after = fixture.cut();
    assert_eq!(
        after.shard_value, before,
        "the shard key keeps its exact bytes"
    );
    // A reader of the shard config, as a node that predates write sets is,
    // decodes the same record.
    let decoded: DfsMetadataShardConfig =
        sinomo_core::kv::decode_kv_value(&after.shard_value).unwrap();
    assert_eq!(decoded, after.shard);
    assert_eq!(write_set.generation, 1);
    assert_eq!(write_set.switched_at_epoch, 3);
    assert_eq!(write_set.epoch_start, None);
    // The shard index decodes only shard configurations.
    let entries = fixture.kv.range_prefix("/dfs/filesystems/").unwrap();
    assert!(
        entries
            .iter()
            .any(|entry| dfs_metadata_shard_write_set_key_is_exact(&entry.key))
    );
    let index =
        crate::metadata_authority_projection::metadata_shard_index_from_authority_entries(&entries);
    assert_eq!(index[&fs().to_string()].as_ref().unwrap().len(), 1);
}

#[test]
fn an_unreadable_write_set_is_scoped_to_its_shard_and_grants_nothing() {
    let mut fixture = Fixture::new();
    put(
        &mut fixture.kv,
        metadata_shard_write_set_key(&fs(), "root"),
        &"not a write set",
    );
    let cut = fixture.cut();
    assert!(matches!(
        cut.write_set_state(),
        DfsMetadataShardWriteSetState::Unreadable { .. }
    ));
    let entries = fixture.kv.range_prefix("/dfs/filesystems/").unwrap();
    let index =
        crate::metadata_authority_projection::metadata_shard_index_from_authority_entries(&entries);
    assert!(
        index[&fs().to_string()].is_ok(),
        "routing for the filesystem is unaffected"
    );
    let context = crate::metadata_replica_context_from_authority_entries(
        &entries,
        &fixture.nodes[1],
        &fs(),
        &crate::DfsMetadataShardId::new("root"),
    )
    .unwrap()
    .unwrap();
    assert_eq!(context.write_set, None);
    // No write-set change, including a re-switch, applies to it.
    let initial = fixture.nodes[..3].iter().cloned().collect();
    assert!(build_metadata_write_set_switch(&cut, &initial).is_err());
    assert!(build_metadata_write_set_shrink(&cut, &fixture.nodes[1], &bounds(1, 3)).is_err());
}

#[test]
fn a_write_set_no_longer_bound_to_the_shard_is_stale_and_a_switch_overwrites_it() {
    let mut fixture = Fixture::new();
    let mut write_set = fixture.switch_all();
    // A writer that predates write sets advanced the epoch without it.
    let advanced = shard(
        &fixture.nodes[1],
        &[&fixture.nodes[2], &fixture.nodes[0]],
        4,
    );
    put(
        &mut fixture.kv,
        metadata_shard_key(&fs(), "root"),
        &advanced,
    );
    let cut = fixture.cut();
    assert!(matches!(
        cut.write_set_state(),
        DfsMetadataShardWriteSetState::Stale { .. }
    ));
    assert!(
        build_metadata_takeover_swap(
            &cut,
            &fixture.nodes[2],
            &proof(1, 'a'),
            &KvRequestId::generate()
        )
        .is_err()
    );
    let initial = fixture.nodes[..3].iter().cloned().collect();
    let change = build_metadata_write_set_switch(&cut, &initial).unwrap();
    assert!(fixture.apply(&change));
    write_set.generation += 1;
    let current = fixture.cut().write_set_state().switched().cloned().unwrap();
    assert_eq!(current.generation, write_set.generation);
    assert_eq!(current.epoch, 4);
    assert_eq!(current.primary_node_id, fixture.nodes[1]);
}

#[test]
fn switch_requires_every_configured_member_to_advertise_the_capability_itself() {
    let mut fixture = Fixture::new();
    let initial: BTreeSet<_> = fixture.nodes[..3].iter().cloned().collect();
    // A member that does not advertise the capability blocks the switch.
    put(
        &mut fixture.kv,
        cluster_member_key(&fixture.nodes[2]),
        &member(&fixture.nodes[2], false),
    );
    assert!(build_metadata_write_set_switch(&fixture.cut(), &initial).is_err());
    // A capability policy cannot add a capability the member does not advertise.
    let policy = ClusterCapabilityPolicyRecord {
        cluster_id: sinomo_core::Id::from_legacy_named(CLUSTER).unwrap(),
        node_id: fixture.nodes[2].clone(),
        capabilities: vec![
            sinomo_core::CapabilityName::new(DFS_METADATA_WRITE_SET_V1_CAPABILITY).unwrap(),
        ],
        allowed_capabilities: Vec::new(),
        updated_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    put(
        &mut fixture.kv,
        cluster_capability_policy_key(&fixture.nodes[2]),
        &policy,
    );
    assert!(build_metadata_write_set_switch(&fixture.cut(), &initial).is_err());
    // A policy may restrict it away.
    put(
        &mut fixture.kv,
        cluster_member_key(&fixture.nodes[2]),
        &member(&fixture.nodes[2], true),
    );
    let restricting = ClusterCapabilityPolicyRecord {
        capabilities: Vec::new(),
        ..policy
    };
    put(
        &mut fixture.kv,
        cluster_capability_policy_key(&fixture.nodes[2]),
        &restricting,
    );
    assert!(build_metadata_write_set_switch(&fixture.cut(), &initial).is_err());
    InProcessKvStore::delete(
        &mut fixture.kv,
        cluster_capability_policy_key(&fixture.nodes[2]),
    )
    .unwrap();
    // The evidence is compared: a member record changing between the read
    // and the commit fails the switch.
    let change = build_metadata_write_set_switch(&fixture.cut(), &initial).unwrap();
    put(
        &mut fixture.kv,
        cluster_member_key(&fixture.nodes[1]),
        &member(&fixture.nodes[1], false),
    );
    assert!(!fixture.apply(&change));
    assert_eq!(
        fixture.cut().write_set_state(),
        DfsMetadataShardWriteSetState::Unswitched
    );
}

#[test]
fn switch_is_refused_while_a_handoff_or_historical_floor_is_pending() {
    let mut fixture = Fixture::new();
    let initial: BTreeSet<_> = fixture.nodes[..3].iter().cloned().collect();
    put(
        &mut fixture.kv,
        metadata_shard_operation_floor_key(&fs(), "root"),
        &"historical",
    );
    assert!(build_metadata_write_set_switch(&fixture.cut(), &initial).is_err());
    InProcessKvStore::delete(
        &mut fixture.kv,
        metadata_shard_operation_floor_key(&fs(), "root"),
    )
    .unwrap();
    put(
        &mut fixture.kv,
        metadata_shard_membership_handoff_key(&fs(), "root"),
        &"pending",
    );
    assert!(build_metadata_write_set_switch(&fixture.cut(), &initial).is_err());
    InProcessKvStore::delete(
        &mut fixture.kv,
        metadata_shard_membership_handoff_key(&fs(), "root"),
    )
    .unwrap();
    // The initial write set must contain the primary and only configured members.
    let without_primary = fixture.nodes[1..3].iter().cloned().collect();
    assert!(build_metadata_write_set_switch(&fixture.cut(), &without_primary).is_err());
    let outsider = [&fixture.nodes[0], &fixture.nodes[3]]
        .into_iter()
        .cloned()
        .collect();
    assert!(build_metadata_write_set_switch(&fixture.cut(), &outsider).is_err());
    // Members without whole-journal evidence stay outside the initial set.
    let partial = [&fixture.nodes[0], &fixture.nodes[1]]
        .into_iter()
        .cloned()
        .collect();
    let change = build_metadata_write_set_switch(&fixture.cut(), &partial).unwrap();
    assert!(fixture.apply(&change));
    assert_eq!(
        change.write_set.write_set,
        sorted(vec![fixture.nodes[0].clone(), fixture.nodes[1].clone()])
    );
}

#[test]
fn shrink_never_goes_below_the_floor_and_is_ordered_with_policy_changes() {
    let mut fixture = Fixture::new();
    fixture.switch_all();
    let cut = fixture.cut();
    assert!(build_metadata_write_set_shrink(&cut, &fixture.nodes[0], &bounds(1, 3)).is_err());
    assert!(build_metadata_write_set_shrink(&cut, &fixture.nodes[3], &bounds(1, 3)).is_err());
    assert!(build_metadata_write_set_shrink(&cut, &fixture.nodes[1], &bounds(3, 3)).is_err());
    // A policy change between the read and the commit fails the shrink.
    let change = build_metadata_write_set_shrink(&cut, &fixture.nodes[1], &bounds(2, 3)).unwrap();
    let rules = vec![DfsSubtreePolicy::new(
        "/",
        DfsObjectPolicy::ReplicatedEventual,
        3,
        3,
    )];
    put(
        &mut fixture.kv,
        file_system_config_key(&fs()),
        &file_system_config(rules),
    );
    assert!(!fixture.apply(&change));
    let cut = fixture.cut();
    let change = build_metadata_write_set_shrink(&cut, &fixture.nodes[1], &bounds(2, 3)).unwrap();
    assert!(fixture.apply(&change));
    let current = fixture.cut().write_set_state().switched().cloned().unwrap();
    assert_eq!(
        current.write_set,
        sorted(vec![fixture.nodes[0].clone(), fixture.nodes[2].clone()])
    );
    assert_eq!(current.generation, 2);
    assert_eq!(
        fixture.cut().shard_value,
        cut.shard_value,
        "shrink rewrites only the write set"
    );
}

#[test]
fn an_add_built_from_an_earlier_record_can_never_apply() {
    let mut fixture = Fixture::new();
    fixture.switch_all();
    let cut = fixture.cut();
    assert!(
        build_metadata_write_set_add(&cut, &fixture.nodes[1]).is_err(),
        "already a member"
    );
    assert!(
        build_metadata_write_set_add(&cut, &fixture.nodes[3]).is_err(),
        "not configured"
    );
    let shrink = build_metadata_write_set_shrink(&cut, &fixture.nodes[2], &bounds(1, 3)).unwrap();
    assert!(fixture.apply(&shrink));
    let shrunk = fixture.cut();
    let add = build_metadata_write_set_add(&shrunk, &fixture.nodes[2]).unwrap();
    // Any change to the record, here a second shrink, makes the add stale:
    // the generation only advances, so the record never equals its cut again.
    let other = build_metadata_write_set_shrink(&shrunk, &fixture.nodes[1], &bounds(1, 3)).unwrap();
    assert!(fixture.apply(&other));
    assert!(!fixture.apply(&add));
    let current = fixture.cut();
    assert!(!current.same_record(&shrunk));
    let add = build_metadata_write_set_add(&current, &fixture.nodes[2]).unwrap();
    assert!(fixture.apply(&add));
    let write_set = fixture.cut().write_set_state().switched().cloned().unwrap();
    assert_eq!(
        write_set.write_set,
        sorted(vec![fixture.nodes[0].clone(), fixture.nodes[2].clone()])
    );
}

#[test]
fn recruit_and_remove_keep_n_and_the_floor_and_record_substitutes() {
    let mut fixture = Fixture::new();
    fixture.switch_all();
    let cut = fixture.cut();
    assert!(
        build_metadata_write_set_recruit(&cut, &fixture.nodes[3], &bounds(2, 3)).is_err(),
        "write set already has N"
    );
    let shrink = build_metadata_write_set_shrink(&cut, &fixture.nodes[2], &bounds(2, 3)).unwrap();
    assert!(fixture.apply(&shrink));
    let cut = fixture.cut();
    let recruit = build_metadata_write_set_recruit(&cut, &fixture.nodes[3], &bounds(2, 3)).unwrap();
    assert!(fixture.apply(&recruit));
    let cut = fixture.cut();
    assert!(cut.shard.replica_node_ids.contains(&fixture.nodes[3]));
    assert_eq!(cut.shard.epoch, 3, "recruitment never advances the epoch");
    let write_set = cut.write_set_state().switched().cloned().unwrap();
    assert_eq!(write_set.substitutes.len(), 1);
    assert_eq!(write_set.substitutes[0].node_id, fixture.nodes[3]);
    // The primary is never removed, nor a set that takes the write set
    // below the floor.
    let primary = [fixture.nodes[0].clone()].into_iter().collect();
    assert!(build_metadata_write_set_remove(&cut, &primary, &bounds(1, 3), 1, false).is_err());
    let below = [fixture.nodes[1].clone()].into_iter().collect();
    assert!(build_metadata_write_set_remove(&cut, &below, &bounds(2, 3), 1, false).is_err());
    // Retire refuses a write-set member.
    assert!(build_metadata_write_set_remove(&cut, &below, &bounds(1, 3), 1, true).is_err());
    // A head watermark acknowledging a removed member is deleted with it.
    put(
        &mut fixture.kv,
        metadata_shard_head_watermark_key(&fs(), "root"),
        &"watermark",
    );
    let cut = fixture.cut();
    let retire = [fixture.nodes[2].clone()].into_iter().collect();
    let change = build_metadata_write_set_remove(&cut, &retire, &bounds(2, 3), 3, true).unwrap();
    assert!(fixture.apply(&change));
    let cut = fixture.cut();
    assert!(!cut.shard.replica_node_ids.contains(&fixture.nodes[2]));
    assert_eq!(cut.watermark_value, None);
}

#[test]
fn a_swap_compares_the_whole_record_not_the_epoch_alone() {
    let mut fixture = Fixture::new();
    fixture.switch_all();
    let candidate_cut = fixture.cut();
    let request_id = KvRequestId::generate();
    let swap = build_metadata_takeover_swap(
        &candidate_cut,
        &fixture.nodes[2],
        &proof(4, 'c'),
        &request_id,
    )
    .unwrap();
    // The primary removes the candidate at the same epoch; the candidate's
    // swap must not apply.
    let shrink =
        build_metadata_write_set_shrink(&candidate_cut, &fixture.nodes[2], &bounds(2, 3)).unwrap();
    assert!(fixture.apply(&shrink));
    assert_eq!(fixture.cut().shard.epoch, candidate_cut.shard.epoch);
    assert!(!fixture.apply(&swap));
    // A candidate outside the write set cannot build a swap at all.
    assert!(
        build_metadata_takeover_swap(
            &fixture.cut(),
            &fixture.nodes[2],
            &proof(4, 'c'),
            &request_id
        )
        .is_err()
    );
}

#[test]
fn concurrent_swaps_apply_once_and_name_their_request() {
    let mut fixture = Fixture::new();
    fixture.switch_all();
    let cut = fixture.cut();
    let first_id = KvRequestId::generate();
    let second_id = KvRequestId::generate();
    let first =
        build_metadata_takeover_swap(&cut, &fixture.nodes[1], &proof(5, 'b'), &first_id).unwrap();
    let again =
        build_metadata_takeover_swap(&cut, &fixture.nodes[1], &proof(5, 'b'), &first_id).unwrap();
    assert_eq!(first, again, "a swap is a pure function of its inputs");
    let second =
        build_metadata_takeover_swap(&cut, &fixture.nodes[2], &proof(4, 'c'), &second_id).unwrap();
    assert!(fixture.apply(&first));
    assert!(!fixture.apply(&second));
    let after = fixture.cut();
    assert_eq!(after.shard.primary_node_id, fixture.nodes[1]);
    assert_eq!(after.shard.epoch, 4);
    assert!(
        after.shard.replica_node_ids.contains(&fixture.nodes[0]),
        "the old primary keeps its copy"
    );
    let write_set = after.write_set_state().switched().cloned().unwrap();
    assert!(!write_set.contains(&fixture.nodes[0]));
    assert_eq!(write_set.epoch_start, Some(proof(5, 'b')));
    assert_eq!(
        takeover_settlement_from_record(
            &first_id,
            &cut,
            Some(&after.shard),
            after.write_set_value.as_deref()
        ),
        DfsMetadataTakeoverSettlement::Applied
    );
    assert_eq!(
        takeover_settlement_from_record(
            &second_id,
            &cut,
            Some(&after.shard),
            after.write_set_value.as_deref()
        ),
        DfsMetadataTakeoverSettlement::RejectedEpochAdvanced
    );
    assert_eq!(
        takeover_settlement_from_record(
            &second_id,
            &cut,
            Some(&cut.shard),
            cut.write_set_value.as_deref()
        ),
        DfsMetadataTakeoverSettlement::RejectedEpochUnchanged
    );
}

#[test]
fn a_swap_during_a_pending_handoff_deletes_it_and_the_completion_loses() {
    let mut fixture = Fixture::new();
    fixture.switch_all();
    put(
        &mut fixture.kv,
        metadata_shard_membership_handoff_key(&fs(), "root"),
        &"pending",
    );
    put(
        &mut fixture.kv,
        metadata_shard_head_watermark_key(&fs(), "root"),
        &"watermark",
    );
    let cut = fixture.cut();
    // Ordinary write-set changes wait for the handoff.
    assert!(build_metadata_write_set_shrink(&cut, &fixture.nodes[1], &bounds(1, 3)).is_err());
    let swap = build_metadata_takeover_swap(
        &cut,
        &fixture.nodes[2],
        &proof(4, 'c'),
        &KvRequestId::generate(),
    )
    .unwrap();
    assert!(fixture.apply(&swap));
    let after = fixture.cut();
    assert_eq!(after.handoff_value, None);
    assert_eq!(after.watermark_value, None);
    // A completion compares the retiring shard bytes, which the swap changed.
    let completion = KvTxn {
        compares: vec![KvCompare::ValueEquals {
            key: metadata_shard_key(&fs(), "root"),
            value: cut.shard_value.clone(),
        }],
        success: vec![KvOp::Delete {
            key: metadata_shard_key(&fs(), "root"),
        }],
        failure: Vec::new(),
    };
    assert!(
        !DfsControlPlaneKvClient::txn(&mut fixture.kv, completion)
            .unwrap()
            .succeeded
    );
}

#[test]
fn a_switched_shard_admits_only_a_rotation_handoff_and_its_completion_moves_the_write_set() {
    let mut fixture = Fixture::new();
    let write_set = fixture.switch_all();
    let cut = fixture.cut();
    let entry = KvEntry {
        key: metadata_shard_write_set_key(&fs(), "root"),
        value: cut.write_set_value.clone().unwrap(),
        create_revision: KvRevision(1),
        modify_revision: KvRevision(1),
        version: sinomo_core::kv::KvVersion(1),
        lease_id: None,
    };
    let mut membership = cut.shard.clone();
    membership.epoch += 1;
    membership.replica_node_ids.push(fixture.nodes[3].clone());
    let cause = DfsMetadataShardHandoffCause::Membership;
    assert!(
        metadata_handoff_write_set_ops(&cut.shard, &membership, &cause, Some(&entry), None)
            .is_err()
    );
    // The store's begin refuses it with the same rule.
    assert!(
        DfsControlPlaneStore::new(&mut fixture.kv)
            .begin_metadata_shard_membership_handoff(&cut.shard, &membership)
            .is_err()
    );
    let rotation = metadata_shard_primary_handoff_target(&cut.shard, &fixture.nodes[1]).unwrap();
    let proof_record = DfsMetadataShardMembershipHandoffProof {
        cause: cause.clone(),
        handoff_id: "rotation".into(),
        retiring_shard: cut.shard.clone(),
        desired_shard: rotation.clone(),
        journal_sequence: 7,
        state_fingerprint: proof(7, 'e').digest,
        acknowledged_node_ids: Vec::new(),
    };
    let (compare, put_op) = metadata_handoff_write_set_ops(
        &cut.shard,
        &rotation,
        &cause,
        Some(&entry),
        Some(&proof_record),
    )
    .unwrap();
    assert!(matches!(compare, KvCompare::ValueEquals { .. }));
    let Some(KvOp::Put { value, .. }) = put_op else {
        panic!("a switched shard's completion writes its next write set");
    };
    let next: DfsMetadataShardWriteSet = sinomo_core::kv::decode_kv_value(&value).unwrap();
    assert_eq!(next.primary_node_id, fixture.nodes[1]);
    assert_eq!(next.epoch, 4);
    assert_eq!(next.generation, write_set.generation + 1);
    assert!(
        next.contains(&fixture.nodes[0]),
        "the old primary stays in the write set"
    );
    assert_eq!(next.epoch_start, Some(proof(7, 'e')));
    next.validate_against(&rotation).unwrap();
    // An unswitched shard's handoff compares the missing write-set key and
    // writes nothing there.
    let (compare, put_op) =
        metadata_handoff_write_set_ops(&cut.shard, &membership, &cause, None, None).unwrap();
    assert!(matches!(compare, KvCompare::Missing { .. }));
    assert!(put_op.is_none());
}

#[test]
fn bounds_follow_the_rules_that_can_apply_to_the_shards_own_paths() {
    let primary = node("node_ws_a");
    let prefix = |shard_id: &str, prefix: &str| DfsMetadataShardConfig {
        shard_id: shard_id.into(),
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: prefix.into(),
        },
        ..shard(&primary, &[], 1)
    };
    let root = shard(&primary, &[], 1);
    let users = prefix("users", "/users");
    let alice = prefix("alice", "/users/alice");
    let hashed = DfsMetadataShardConfig {
        shard_id: "hashed".into(),
        assignment: DfsMetadataShardAssignment::NameHashRange {
            start_inclusive: 0,
            end_exclusive: 10,
        },
        ..shard(&primary, &[], 1)
    };
    let shards = vec![root.clone(), users.clone(), alice.clone(), hashed.clone()];
    let mut self_only = DfsSubtreePolicy::new("/users", DfsObjectPolicy::ReplicatedEventual, 3, 4);
    self_only.scope = DfsPlacementScope::SelfOnly;
    let config = file_system_config(vec![
        DfsSubtreePolicy::new("/users", DfsObjectPolicy::ReplicatedEventual, 2, 3),
        self_only,
        DfsSubtreePolicy::new("/users/bob", DfsObjectPolicy::ReplicatedEventual, 2, 2),
        DfsSubtreePolicy::new("/users/alice", DfsObjectPolicy::ReplicatedConfirmed, 4, 4),
        DfsSubtreePolicy::new("/other", DfsObjectPolicy::ReplicatedEventual, 2, 2),
    ]);
    let of = |shard: &DfsMetadataShardConfig| {
        metadata_shard_replication_bounds(&config, shard, &shards, 5).unwrap()
    };
    // The root owns /other and the paths no prefix assignment owns; with no
    // rule at or above `/`, the default policy also applies.
    assert_eq!(of(&root), bounds(2, 2));
    // /users: its SelfOnly rule, its subtree rule and /users/bob, but not the
    // nested /users/alice assignment.
    assert_eq!(of(&users), bounds(3, 4));
    assert_eq!(of(&alice), bounds(4, 4));
    // Any path can land on a hash range: every rule applies.
    assert_eq!(of(&hashed), bounds(4, 4));
    // The default policy alone: W 1 and N up to 2.
    let empty = file_system_config(Vec::new());
    assert_eq!(
        metadata_shard_replication_bounds(&empty, &root, &shards, 5).unwrap(),
        bounds(1, 2)
    );
    assert_eq!(
        metadata_shard_replication_bounds(&empty, &root, &shards, 1).unwrap(),
        bounds(1, 1)
    );
    // A commit covers an object or per-write W above the shard's floor.
    assert_eq!(metadata_commit_write_set_floor(&bounds(1, 2), 2), 2);
    assert_eq!(metadata_commit_write_set_floor(&bounds(2, 3), 1), 2);
}

#[test]
fn selection_keeps_the_primary_prefers_non_mobile_members_and_uses_mobile_only_for_the_floor() {
    let primary = node("node_ws_a");
    let config = shard(&primary, &[], 1);
    let candidate = |name: &str, mobile: bool| DfsMetadataWriteSetCandidate {
        node_id: node(name),
        mobile,
        in_write_set: true,
        reachable: true,
    };
    let candidates = vec![
        candidate("node_ws_a", false),
        candidate("node_ws_b", false),
        candidate("node_ws_m", true),
        candidate("node_ws_c", false),
        candidate("node_ws_d", false),
    ];
    let selected = select_metadata_shard_members(&config, &candidates, &bounds(2, 3), false);
    assert_eq!(selected.len(), 3);
    assert_eq!(selected[0], primary);
    assert!(!selected.contains(&node("node_ws_m")));
    assert_eq!(
        selected,
        select_metadata_shard_members(&config, &candidates, &bounds(2, 3), false)
    );
    // Too few non-mobile members for the floor: a mobile member fills it, but
    // only up to the floor, not N.
    let few = vec![candidate("node_ws_m", true), candidate("node_ws_n", true)];
    let selected = select_metadata_shard_members(&config, &few, &bounds(2, 3), false);
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0], primary);
    // Out-of-sync members rank after write-set members.
    let mut lagging = candidate("node_ws_b", false);
    lagging.in_write_set = false;
    let selected = select_metadata_shard_members(
        &config,
        &[lagging, candidate("node_ws_c", false)],
        &bounds(1, 2),
        false,
    );
    assert_eq!(selected, vec![primary, node("node_ws_c")]);
}

#[test]
fn write_set_record_round_trips_and_rejects_an_unknown_format() {
    let mut fixture = Fixture::new();
    let mut write_set = fixture.switch_all();
    let bytes = sinomo_core::kv::encode_kv_value(&write_set).unwrap();
    assert_eq!(
        sinomo_core::kv::decode_kv_value::<DfsMetadataShardWriteSet>(&bytes).unwrap(),
        write_set
    );
    write_set.format = 2;
    let bytes = sinomo_core::kv::encode_kv_value(&write_set).unwrap();
    assert!(matches!(
        metadata_shard_write_set_state(&fixture.cut().shard, Some(&bytes)),
        DfsMetadataShardWriteSetState::Unreadable { .. }
    ));
}
