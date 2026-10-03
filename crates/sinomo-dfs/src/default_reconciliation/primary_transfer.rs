//! Reactive scheduling for default-primary transfer selection only.
//!
//! A sender hint can make a target worth probing; it never activates authority.
//! Dependencies belong to the existing live reconciliation wake and expire with
//! that owner. Normal publisher checkpoints without a waiting consumer are inert.

use super::*;
use sinomo_core::local::SinomoDataDir;

#[derive(Debug, Default)]
pub(super) struct PrimaryTransferSelectionState {
    // Ignore KV revision and the process-normalized placement permit: only
    // semantic selection inputs can invalidate the captured dependency.
    inputs: Option<Vec<(String, Vec<u8>)>>,
    revision: Option<KvRevision>,
    generation: u64,
    waits: BTreeMap<String, PrimaryTransferWait>,
}

#[derive(Debug)]
struct PrimaryTransferWait {
    current: DfsMetadataShardConfig,
    target: NodeId,
    head: u64,
    token: Arc<()>,
}

pub(crate) struct PrimaryTransferSelectionEpoch {
    wake: Weak<DefaultDfsReconciliationWake>,
    generation: u64,
}

pub(crate) struct PrimaryTransferSelectionRegistration {
    wake: Weak<DefaultDfsReconciliationWake>,
    shard: String,
    token: Arc<()>,
    retained: bool,
}

impl PrimaryTransferSelectionRegistration {
    pub(crate) fn keep_waiting(mut self) {
        self.retained = true;
    }
}

impl Drop for PrimaryTransferSelectionRegistration {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        let Some(wake) = self.wake.upgrade() else {
            return;
        };
        let mut state = wake
            .primary_transfer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state
            .waits
            .get(&self.shard)
            .is_some_and(|wait| Arc::ptr_eq(&wait.token, &self.token))
        {
            state.waits.remove(&self.shard);
        }
    }
}

fn current_wake(data_dir: &SinomoDataDir) -> Option<Arc<DefaultDfsReconciliationWake>> {
    let paths = ClusterPaths::new(data_dir);
    DEFAULT_DFS_RECONCILIATION_WAKES
        .get()?
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&paths.root)
        .and_then(Weak::upgrade)
}

pub(crate) fn capture_primary_transfer_selection(
    data_dir: &SinomoDataDir,
) -> Option<PrimaryTransferSelectionEpoch> {
    let wake = current_wake(data_dir)?;
    let generation = wake
        .primary_transfer
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .generation;
    Some(PrimaryTransferSelectionEpoch {
        wake: Arc::downgrade(&wake),
        generation,
    })
}

impl PrimaryTransferSelectionEpoch {
    pub(crate) fn register(
        &self,
        current: &DfsMetadataShardConfig,
        target: &NodeId,
        head: u64,
    ) -> Option<PrimaryTransferSelectionRegistration> {
        if current.file_system_id != crate::sifs_file_system_id()
            || !matches!(current.shard_id.as_str(), "root" | "services" | "users")
            || current.primary_node_id == *target
            || !current.replica_node_ids.contains(target)
        {
            return None;
        }
        let wake = self.wake.upgrade()?;
        let mut state = wake
            .primary_transfer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.generation != self.generation {
            return None;
        }
        if let Some(inputs) = &state.inputs {
            let key = crate::metadata_shard_key(&current.file_system_id, &current.shard_id);
            let observed = inputs
                .iter()
                .find(|(name, _)| name == &key)
                .and_then(|(_, value)| {
                    sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(value).ok()
                });
            if observed.as_ref() != Some(current) {
                return None;
            }
        }
        let token = Arc::new(());
        state.waits.insert(
            current.shard_id.clone(),
            PrimaryTransferWait {
                current: current.clone(),
                target: target.clone(),
                head,
                token: Arc::clone(&token),
            },
        );
        Some(PrimaryTransferSelectionRegistration {
            wake: Arc::downgrade(&wake),
            shard: current.shard_id.clone(),
            token,
            retained: false,
        })
    }
}

pub(super) fn observe_projection(
    wake: &Arc<DefaultDfsReconciliationWake>,
    projection: &DefaultDfsReconciliationProjection,
) {
    let mut state = wake
        .primary_transfer
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    // pending_work can hold an older cloned projection while the committed
    // callback has already installed a newer one. Do not regress that view.
    if state.revision.is_some_and(|previous| {
        projection
            .revision
            .is_none_or(|incoming| incoming < previous)
    }) {
        return;
    }
    state.revision = projection.revision.or(state.revision);
    if state.inputs.as_ref() == Some(&projection.inputs) {
        return;
    }
    // This includes authority and the membership/provider inputs which select
    // the desired target. A pass captured before that change cannot register.
    state.generation = state.generation.wrapping_add(1);
    state.inputs = Some(projection.inputs.clone());
    state.waits.clear();
}

pub(super) fn clear_dependencies(wake: &Arc<DefaultDfsReconciliationWake>) {
    let mut state = wake
        .primary_transfer
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    state.generation = state.generation.wrapping_add(1);
    state.waits.clear();
}

pub(crate) fn notify_primary_transfer_checkpoint(
    data_dir: &SinomoDataDir,
    target: &NodeId,
    shard: &DfsMetadataShardId,
    checkpoint: u64,
) {
    let Some(wake) = current_wake(data_dir) else {
        return;
    };
    let satisfied = {
        let mut state = wake
            .primary_transfer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let matches = state.waits.get(&shard.0).is_some_and(|wait| {
            wait.current.file_system_id == crate::sifs_file_system_id()
                && wait.current.shard_id == shard.0
                && wait.target == *target
                && checkpoint >= wait.head
        });
        if matches {
            state.waits.remove(&shard.0);
        }
        matches
    };
    if satisfied {
        // Consume exactly once. This only requests a new selection pass, whose
        // current authority/retiring fence/prefix certificates remain required.
        wake.generation_tx
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

#[cfg(test)]
mod independent_adversarial_tests {
    use super::*;
    use crate::default_reconciliation::{
        DefaultDfsReconciliationProjection, DefaultDfsReconciliationRuntime,
    };
    use sinomo_core::kv::{KvRevision, encode_kv_value};
    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::network::ClusterPaths;
    use sinomo_core::node::NodeId;

    struct Fixture {
        _root: sinomo_testdir::TestDir,
        data_dir: SinomoDataDir,
        runtime: DefaultDfsReconciliationRuntime,
        shard: crate::DfsMetadataShardConfig,
        target: NodeId,
        inputs: Vec<(String, Vec<u8>)>,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let root = sinomo_testdir::TestDir::new(name);
            let data_dir = SinomoDataDir::new(root.path());
            data_dir.ensure_root_layout().unwrap();
            let primary = NodeId::from_legacy_named(format!("{name}-primary")).unwrap();
            let target = NodeId::from_legacy_named(format!("{name}-target")).unwrap();
            let shard = crate::DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "users".into(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![target.clone()],
                epoch: 2,
                assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".into(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            };
            let runtime = DefaultDfsReconciliationRuntime::new(
                &ClusterPaths::new(&data_dir),
                &primary,
                None,
                None,
            )
            .unwrap();
            let inputs = vec![(
                crate::metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                encode_kv_value(&shard).unwrap(),
            )];
            let mut fixture = Self {
                _root: root,
                data_dir,
                runtime,
                shard,
                target,
                inputs,
            };
            fixture.publish_projection_and_complete(1);
            fixture
        }

        fn publish_projection_and_complete(&mut self, revision: u64) {
            self.runtime
                .projection_sender()
                .send_replace(DefaultDfsReconciliationProjection {
                    revision: Some(KvRevision(revision)),
                    inputs: self.inputs.clone(),
                    placement_authority_permit: None,
                });
            let work = self
                .runtime
                .pending_work()
                .expect("new semantic generation");
            self.runtime.complete_prior_process_recovery(&work);
            self.runtime.complete_default_reconciliation(&work);
            assert!(self.runtime.pending_work().is_none());
        }

        fn shard_id(&self) -> crate::DfsMetadataShardId {
            crate::DfsMetadataShardId::new(self.shard.shard_id.clone())
        }
    }

    #[test]
    fn ordinary_checkpoints_without_dependency_do_not_dirty_default_reconciliation() {
        let mut fixture = Fixture::new("dfs-transfer-no-ordinary-wake");
        for sequence in [1, 7, 70, 70] {
            notify_primary_transfer_checkpoint(
                &fixture.data_dir,
                &fixture.target,
                &fixture.shard_id(),
                sequence,
            );
            assert!(
                fixture.runtime.pending_work().is_none(),
                "ordinary ACK is not a dependency"
            );
        }
    }

    #[test]
    fn dependency_ignores_other_root_shard_target_and_incomplete_prefix_then_wakes_once() {
        let mut fixture = Fixture::new("dfs-transfer-exact-dependency");
        let mut other = Fixture::new("dfs-transfer-other-root");
        let epoch = capture_primary_transfer_selection(&fixture.data_dir).unwrap();
        epoch
            .register(&fixture.shard, &fixture.target, 7)
            .unwrap()
            .keep_waiting();
        notify_primary_transfer_checkpoint(
            &other.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            7,
        );
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &crate::DfsMetadataShardId::new("services"),
            7,
        );
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &NodeId::generate(),
            &fixture.shard_id(),
            7,
        );
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            6,
        );
        assert!(fixture.runtime.pending_work().is_none());
        assert!(other.runtime.pending_work().is_none());
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            7,
        );
        let work = fixture
            .runtime
            .pending_work()
            .expect("matching completed prefix wakes idle consumer");
        fixture.runtime.complete_default_reconciliation(&work);
        assert!(fixture.runtime.pending_work().is_none());
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            7,
        );
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            8,
        );
        assert!(
            fixture.runtime.pending_work().is_none(),
            "consumed dependency cannot become per-ACK wake"
        );
    }

    #[test]
    fn stale_registration_drop_cannot_erase_replacement_at_a_newer_head() {
        let mut fixture = Fixture::new("dfs-transfer-stale-registration-drop");
        let epoch = capture_primary_transfer_selection(&fixture.data_dir).unwrap();
        let old = epoch.register(&fixture.shard, &fixture.target, 7).unwrap();
        epoch
            .register(&fixture.shard, &fixture.target, 9)
            .unwrap()
            .keep_waiting();
        drop(old);
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            7,
        );
        assert!(fixture.runtime.pending_work().is_none());
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            9,
        );
        assert!(
            fixture.runtime.pending_work().is_some(),
            "old completion must not clear a newer wait token"
        );
    }

    #[test]
    fn epoch_and_desired_selection_input_changes_invalidate_waits_and_old_capture() {
        for epoch_change in [false, true] {
            let mut fixture = Fixture::new("dfs-transfer-semantic-invalidation");
            let captured = capture_primary_transfer_selection(&fixture.data_dir).unwrap();
            let original = fixture.shard.clone();
            captured
                .register(&original, &fixture.target, 7)
                .unwrap()
                .keep_waiting();
            if epoch_change {
                fixture.shard.epoch += 1;
                fixture.inputs[0].1 = encode_kv_value(&fixture.shard).unwrap();
            } else {
                // Metadata advertisements are semantic reconciliation inputs.
                // A changed input invalidates an older selection pass even
                // when the current assignment and epoch are unchanged.
                let mut advertisement =
                    crate::DfsNodeAdvertisement::metadata(fixture.target.clone());
                advertisement.capacity_weight = 2;
                fixture.inputs.push((
                    crate::metadata_service_key(&fixture.target),
                    encode_kv_value(&advertisement).unwrap(),
                ));
                fixture.inputs.sort_by(|left, right| left.0.cmp(&right.0));
            }
            fixture.publish_projection_and_complete(2);
            assert!(
                captured.register(&original, &fixture.target, 7).is_none(),
                "old in-flight planner cannot rearm after invalidation"
            );
            notify_primary_transfer_checkpoint(
                &fixture.data_dir,
                &fixture.target,
                &fixture.shard_id(),
                7,
            );
            assert!(
                fixture.runtime.pending_work().is_none(),
                "old dependency was invalidated"
            );
            let current = capture_primary_transfer_selection(&fixture.data_dir).unwrap();
            if epoch_change {
                assert!(
                    current.register(&original, &fixture.target, 7).is_none(),
                    "stale config cannot register under a new capture"
                );
            }
            current
                .register(&fixture.shard, &fixture.target, 8)
                .unwrap()
                .keep_waiting();
            notify_primary_transfer_checkpoint(
                &fixture.data_dir,
                &fixture.target,
                &fixture.shard_id(),
                8,
            );
            assert!(fixture.runtime.pending_work().is_some());
        }
    }

    #[test]
    fn completed_read_unregisters_its_own_wait_without_waking() {
        let mut fixture = Fixture::new("dfs-transfer-already-ready");
        let captured = capture_primary_transfer_selection(&fixture.data_dir).unwrap();
        let registration = captured
            .register(&fixture.shard, &fixture.target, 7)
            .unwrap();
        drop(registration); // Read observed readiness or returned an I/O error.
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            7,
        );
        assert!(fixture.runtime.pending_work().is_none());
    }
    #[test]
    fn destroyed_reactor_owner_cannot_be_kept_alive_or_rearmed_by_capture() {
        let fixture = Fixture::new("dfs-transfer-owner-drop");
        let capture = capture_primary_transfer_selection(&fixture.data_dir).unwrap();
        capture
            .register(&fixture.shard, &fixture.target, 7)
            .unwrap()
            .keep_waiting();
        let Fixture {
            _root,
            data_dir,
            runtime,
            shard,
            target,
            ..
        } = fixture;
        drop(runtime);
        assert!(capture_primary_transfer_selection(&data_dir).is_none());
        assert!(capture.register(&shard, &target, 8).is_none());
        notify_primary_transfer_checkpoint(
            &data_dir,
            &target,
            &crate::DfsMetadataShardId::new("users"),
            8,
        );
        assert!(capture_primary_transfer_selection(&data_dir).is_none());
    }
    #[test]
    fn stale_cloned_projection_cannot_regress_dependency_and_revision_only_change_is_inert() {
        let mut fixture = Fixture::new("dfs-transfer-out-of-order-projection");
        let old_inputs = fixture.inputs.clone();
        fixture.shard.epoch += 1;
        fixture.inputs[0].1 = encode_kv_value(&fixture.shard).unwrap();
        fixture.publish_projection_and_complete(2);
        capture_primary_transfer_selection(&fixture.data_dir)
            .unwrap()
            .register(&fixture.shard, &fixture.target, 7)
            .unwrap()
            .keep_waiting();
        let wake = current_wake(&fixture.data_dir).unwrap();
        observe_projection(
            &wake,
            &DefaultDfsReconciliationProjection {
                revision: Some(KvRevision(1)),
                inputs: old_inputs.clone(),
                placement_authority_permit: None,
            },
        );
        fixture
            .runtime
            .projection_sender()
            .send_replace(DefaultDfsReconciliationProjection {
                revision: Some(KvRevision(10)),
                inputs: fixture.inputs.clone(),
                placement_authority_permit: None,
            });
        assert!(
            fixture.runtime.pending_work().is_none(),
            "revision alone is not a default input change"
        );
        // A no-op semantic observation must still advance the revision fence:
        // rev10/A followed by delayed rev5/B cannot invalidate A's dependency.
        observe_projection(
            &wake,
            &DefaultDfsReconciliationProjection {
                revision: Some(KvRevision(5)),
                inputs: old_inputs,
                placement_authority_permit: None,
            },
        );
        notify_primary_transfer_checkpoint(
            &fixture.data_dir,
            &fixture.target,
            &fixture.shard_id(),
            7,
        );
        assert!(
            fixture.runtime.pending_work().is_some(),
            "old cloned view did not erase the current epoch wait"
        );
    }
}
