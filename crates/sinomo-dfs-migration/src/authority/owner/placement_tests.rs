//! Interrupted original placement work runs through the managed filesystem owner.
use super::inventory::{ObservedState, UnitKey, UnitRole};
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, PartialEq)]
enum Cut {
    BeforeEnrollment,
    PartialActivation,
    RetirementReply,
    CurrentPublication,
}

#[test]
fn placement_owner_restart_before_enrollment_preserves_original_evidence() {
    interrupted(Cut::BeforeEnrollment, "placement-before-enrollment", false);
}
#[test]
fn placement_owner_restart_after_partial_activation_preserves_original_evidence() {
    interrupted(
        Cut::PartialActivation,
        "placement-after-partial-activation",
        false,
    );
}
#[test]
fn placement_owner_lost_retirement_reply_replays_the_same_native_request() {
    interrupted(Cut::RetirementReply, "placement-retirement-reply", false);
}
#[test]
fn placement_owner_restart_before_current_publication_reuses_original_goal() {
    interrupted(
        Cut::CurrentPublication,
        "placement-current-publication",
        false,
    );
}

fn captured_checkpoints(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    for directory in std::fs::read_dir(root.join("dfs/migration-owner/filesystems")).unwrap() {
        let directory = directory.unwrap().path();
        for name in ["invocation.json", "pending-request.json", "cohort.json"] {
            let path = directory.join(name);
            match std::fs::read(&path) {
                Ok(bytes) => {
                    result.insert(path.strip_prefix(root).unwrap().to_owned(), bytes);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("{}: {error}", path.display()),
            }
        }
    }
    assert!(
        result
            .keys()
            .any(|path| path.file_name().unwrap() == "invocation.json")
    );
    result
}

fn reject_blocked(owner: &MigrationOwner, filesystem: &UnitKey) -> Result<()> {
    let scheduler = owner.work.scheduler.lock().unwrap();
    for unit in [
        filesystem.clone(),
        UnitKey {
            relative_root: "dfs/metadata".into(),
            role: UnitRole::PlacementGoals,
        },
    ] {
        if let Some(ObservedState::Blocked { evidence }) = scheduler.state(&unit)? {
            return Err(error(evidence));
        }
    }
    Ok(())
}

#[test]
fn placement_v1_owner_restart_before_publication_preserves_original_evidence() {
    interrupted(
        Cut::CurrentPublication,
        "placement-v1-current-publication",
        true,
    );
}

fn interrupted(cut: Cut, name: &str, v1: bool) {
    let (mut native, nodes) = placement_work::native_tests::native_migration_cluster(name, 1);
    let node = nodes[0].clone();
    let mut client = native.client(&node).unwrap();
    let ready_version = client
        .posix_write_file(
            "/services/ready-during-placement",
            b"ready immutable content survives placement",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    drop(client);
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let root = owner.authority.data_root.clone();
    let fs = native.file_system().file_system_id;
    let (rows, _) = owner
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let original_config: sinomo_dfs::DfsFileSystemConfig =
        rows[0].entry.as_ref().unwrap().decode().unwrap();
    let filesystem = UnitKey {
        relative_root: "dfs/metadata".into(),
        role: UnitRole::FileSystem {
            file_system_id: fs.clone(),
            source_catalog_generation: original_config.catalog_generation,
            target_format: u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
        },
    };
    let held = Arc::new(AtomicBool::new(true));
    let reached = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(tokio::sync::Notify::new());
    let committed_wake = Arc::clone(&observed);
    let _committed = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &sinomo_core::network::ClusterPaths::new(context.data_dir()),
        move |_| committed_wake.notify_one(),
    );
    let requests = Arc::new(Mutex::new(Vec::<(KvTxnRequest, bool)>::new()));
    let h = Arc::clone(&held);
    let r = Arc::clone(&reached);
    let wake = Arc::clone(&observed);
    let source_hook = filesystem::install_scope_hook(
        &root,
        Arc::new(move |owner, shard, point| {
            if !h.load(Ordering::Acquire) {
                return Ok(());
            }
            let selected = cut == Cut::BeforeEnrollment && point == "before-enrollment"
                || cut == Cut::PartialActivation
                    && point == "after"
                    && owner
                        .authority
                        .recover_scope_retirement(
                            &shard.file_system_id,
                            &sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id),
                        )?
                        .is_some();
            if selected {
                r.store(true, Ordering::Release);
                wake.notify_one();
                return Err(MetadataBackendError::StoragePreparationPending {
                    root: owner.metadata_root.clone(),
                    processed_records: 0,
                });
            }
            Ok(())
        }),
    );
    let h = Arc::clone(&held);
    let r = Arc::clone(&reached);
    let wake = Arc::clone(&observed);
    let captured = Arc::clone(&requests);
    let goal_hook = placement_work::native_tests::install_goal_hook(
        &root,
        Arc::new(move |point, commit| {
            let selected = cut == Cut::RetirementReply && point == "retired-before-reply"
                || cut == Cut::CurrentPublication && point == "before-current-publication";
            if !selected {
                return true;
            }
            if let Some(commit) = commit {
                let mut requests = captured.lock().unwrap();
                if let Some((original, _)) = requests.first() {
                    assert_eq!(original, &commit.request);
                }
                requests.push((
                    commit.request.clone(),
                    commit.result.replayed_committed_request,
                ));
            }
            if h.load(Ordering::Acquire) {
                r.store(true, Ordering::Release);
                wake.notify_one();
                return false;
            }
            true
        }),
    );
    let goal = if v1 {
        placement_work::native_tests::seed_pending_v1_goal(
            &native,
            &node,
            &owner,
            "original-interrupted-goal",
        )
    } else {
        placement_work::native_tests::seed_pending_v0_goal(&owner, "original-interrupted-goal")
    };
    let old = goal.original_entry().clone();
    let current_key = goal.current_job_key().unwrap();
    owner.schedule_placement_work().unwrap();
    let mut changed = owner.binding().readiness;
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        while !reached.load(Ordering::Acquire) {
            reject_blocked(&owner, &filesystem)?;
            tokio::time::timeout_at(deadline, async {
                tokio::select! { _ = observed.notified() => {}, _ = changed.changed() => {} }
            })
            .await
            .map_err(|_| {
                error("original placement did not reach its actual interruption boundary")
            })?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    let (at_cut, _) = owner
        .authority
        .read_exact(&[old.key.clone(), current_key.clone()])
        .unwrap();
    assert!(
        at_cut[1].entry.is_none(),
        "no current job before the tested publication boundary"
    );
    if matches!(cut, Cut::BeforeEnrollment | Cut::PartialActivation) {
        assert_eq!(at_cut[0].entry.as_ref(), Some(&old));
    } else {
        assert!(
            at_cut[0].entry.is_none(),
            "retirement actually committed before its reply was withheld"
        );
    }
    let content_root = root.join("dfs/content");
    assert_eq!(
        (owner.binding().provider)(&content_root, &DfsStoragePreparationScope::Content, 256)
            .unwrap(),
        sinomo_dfs::DfsStoragePreparation::Ready
    );
    let (_, bytes) = sinomo_dfs::LocalContentStore::open_for_inspection(&content_root)
        .read_object_version(
            ready_version.object_id.as_str(),
            ready_version.version.as_str(),
        )
        .unwrap();
    assert_eq!(bytes, b"ready immutable content survives placement");
    let checkpoints = captured_checkpoints(&root);
    drop(changed);
    drop(owner);
    drop(context);
    native.stop_node(&node).unwrap();
    held.store(false, Ordering::Release);
    native.reopen_node(node.clone()).unwrap();
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let mut changed = owner.binding().readiness;
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            reject_blocked(&owner, &filesystem)?;
            let (rows, _) = owner
                .authority
                .read_exact(&[old.key.clone(), current_key.clone()])?;
            if let Some(current) = &rows[1].entry {
                let job: sinomo_dfs::DfsPlacementReconciliationJob =
                    current.decode().map_err(error)?;
                assert!(rows[0].entry.is_none());
                assert_eq!(job.file_system_id, fs);
                assert_eq!(job.path, "/users");
                break;
            }
            tokio::time::timeout_at(deadline, async {
                tokio::select! { _ = observed.notified() => {}, _ = changed.changed() => {} }
            })
            .await
            .map_err(|_| error("same original placement did not resume current publication"))?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    for (path, bytes) in checkpoints {
        assert_eq!(std::fs::read(root.join(path)).unwrap(), bytes);
    }
    if cut == Cut::RetirementReply {
        let requests = requests.lock().unwrap();
        assert!(requests.len() >= 2);
        assert!(
            requests.iter().skip(1).any(|(_, replayed)| *replayed),
            "lost reply must reconcile the same previously committed native request"
        );
    }
    drop(changed);
    drop(owner);
    drop(context);
    drop(source_hook);
    drop(goal_hook);
    native.close().unwrap();
}

#[test]
fn received_uncommitted_v0_custody_cannot_author_its_first_effect() {
    use crate::authority::placement_work::{
        PlacementCustodyEnvelope, PlacementCustodyPhase, PlacementCustodyRequest,
    };
    let (native, nodes) =
        placement_work::native_tests::native_migration_cluster("placement-v0-received-only", 1);
    let context = native.component_context(&nodes[0]).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let unit = UnitKey {
        relative_root: "dfs/metadata".into(),
        role: UnitRole::PlacementGoals,
    };
    let mut changed = owner.binding().readiness;
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            if matches!(
                owner.work.scheduler.lock().unwrap().state(&unit)?,
                Some(ObservedState::VerifiedCurrent)
            ) {
                break;
            }
            tokio::time::timeout_at(deadline, changed.changed())
                .await
                .map_err(|_| error("initial native placement inventory did not settle"))?
                .map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    let goal = placement_work::native_tests::seed_pending_v0_goal(&owner, "received-only-original");
    let request = NativeImportAuthority::original_v0_retirement_request(&goal).unwrap();
    let coordinator = owner
        .authority
        .placement_coordinator(&goal)
        .unwrap()
        .unwrap();
    let envelope = PlacementCustodyEnvelope {
        goal: goal.clone(),
        phase: PlacementCustodyPhase::Retirement,
        coordinator,
        request: request.clone(),
        blueprints: Vec::new(),
    };
    owner
        .authority
        .receive_placement_custody(
            nodes[0].clone(),
            PlacementCustodyRequest::Store {
                envelope: envelope.clone(),
            },
        )
        .unwrap();
    assert!(
        !owner
            .authority
            .evaluate_received_placement_custody(&goal, &envelope)
            .unwrap()
    );
    let read = block_native(
        &native.runtime_handle(),
        owner
            .authority
            .cluster()
            .unwrap()
            .kv_request_outcome_authoritative_async(request.request_id.clone()),
    )
    .unwrap();
    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&read).unwrap();
    assert!(matches!(
        read.outcome,
        sinomo_core::kv::KvRequestOutcome::Unknown { .. }
    ));
    let (rows, _) = owner
        .authority
        .read_exact(&[goal.original_entry().key.clone()])
        .unwrap();
    assert_eq!(rows[0].entry.as_ref(), Some(goal.original_entry()));
    let mut altered = envelope;
    altered.request.txn.success.clear();
    assert!(
        owner
            .authority
            .receive_placement_custody(
                nodes[0].clone(),
                PlacementCustodyRequest::Store { envelope: altered }
            )
            .is_err()
    );
    drop(changed);
    drop(owner);
    drop(context);
    native.close().unwrap();
}
