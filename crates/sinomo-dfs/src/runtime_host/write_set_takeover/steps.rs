//! The blocking steps of a takeover: confirm, promise and swap; settling an
//! unknown swap from its own outcome; and a new primary's activation. Each
//! runs on a blocking thread and reports to the candidate role.

use super::super::*;
use super::ShardKey;
use crate::{
    DfsControlPlaneStore, DfsMetadataTakeoverPromise, DfsMetadataTakeoverPromiseEnd,
    DfsMetadataTakeoverSettlement,
};
use sinomo_core::kv::KvRequestId;
use std::collections::BTreeSet;

/// How confirm, promise and swap ended.
pub(in crate::runtime_host) enum TakeOutcome {
    /// Nothing was promised: the record no longer names the primary judged
    /// lost at the attempt's epoch, with this node in the write set.
    NotTaken {
        follow: bool,
        reason: String,
    },
    /// Nothing was promised: the confirm read itself failed. This may be
    /// momentary, while the KV voters elect, and says nothing about the
    /// record.
    Unconfirmed {
        reason: String,
    },
    /// The promise is durable; the swap's own outcome is known or not.
    Settled {
        promise: DfsMetadataTakeoverPromise,
        settlement: DfsMetadataTakeoverSettlement,
    },
    Failed {
        reason: String,
    },
}

/// Confirm the record with one authoritative read: it must still name the
/// primary judged lost at the attempt's epoch, with this node in the write
/// set. Only a node that can reach a KV quorum can read it, so a node on the
/// minority side of a partition fails here.
pub(in crate::runtime_host) fn confirm_metadata_takeover_record(
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    local: &NodeId,
    key: &ShardKey,
    epoch: u64,
    primary: &NodeId,
    members: &[NodeId],
) -> Result<crate::DfsMetadataShardRecordCut, TakeOutcome> {
    let Some(mut client) = daemon_dfs_control_plane_client_on_runtime(cluster_paths, Some(runtime))
    else {
        return Err(TakeOutcome::NotTaken {
            follow: false,
            reason: "no cluster control-plane client".into(),
        });
    };
    let mut members = members.to_vec();
    if !members.contains(local) {
        members.push(local.clone());
    }
    let cut = match DfsControlPlaneStore::new(client.as_mut())
        .read_metadata_shard_record_cut(&key.0, &key.1.0, &members)
    {
        Ok(Some(cut)) => cut,
        Ok(None) => {
            return Err(TakeOutcome::NotTaken {
                follow: true,
                reason: "the shard no longer exists".into(),
            });
        }
        Err(error) => {
            return Err(TakeOutcome::Unconfirmed {
                reason: format!("the confirm read failed: {error}"),
            });
        }
    };
    let in_write_set = cut
        .write_set_state()
        .switched()
        .is_some_and(|write_set| write_set.contains(local));
    if cut.shard.epoch != epoch || &cut.shard.primary_node_id != primary || !in_write_set {
        return Err(TakeOutcome::NotTaken {
            follow: cut.shard.epoch != epoch,
            reason: format!(
                "the record names primary {} at epoch {}, this node {} the write set",
                cut.shard.primary_node_id,
                cut.shard.epoch,
                if in_write_set { "in" } else { "outside" }
            ),
        });
    }
    Ok(cut)
}

/// Confirm the record, promise, and submit the swap.
pub(in crate::runtime_host) fn take_metadata_shard(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    local: &NodeId,
    key: &ShardKey,
    epoch: u64,
    primary: &NodeId,
    members: &[NodeId],
) -> TakeOutcome {
    let cut = match confirm_metadata_takeover_record(
        cluster_paths,
        runtime,
        local,
        key,
        epoch,
        primary,
        members,
    ) {
        Ok(cut) => cut,
        Err(outcome) => return outcome,
    };
    let swap_request_id = KvRequestId::generate();
    let promised = with_daemon_dfs_main_maintenance_service(data_dir, runtime, |service| {
        service
            .metadata_backend
            .as_deref_mut()
            .map(|backend| backend.promise_metadata_takeover(&key.1, &cut, &swap_request_id))
    });
    let epoch_start = match promised {
        Ok(Some(Ok(epoch_start))) => epoch_start,
        Ok(Some(Err(error))) => {
            return TakeOutcome::Failed {
                reason: format!("the promise was refused: {error}"),
            };
        }
        Ok(None) => {
            return TakeOutcome::Failed {
                reason: "no metadata backend to promise from".into(),
            };
        }
        Err(error) => {
            return TakeOutcome::Failed {
                reason: format!("the promise could not run: {error}"),
            };
        }
    };
    let promise = DfsMetadataTakeoverPromise {
        cut,
        epoch_start,
        swap_request_id,
    };
    #[cfg(any(test, feature = "test-support"))]
    if test_hooks::stops_after_promise(data_dir) {
        return TakeOutcome::Failed {
            reason: "stopped after the promise".into(),
        };
    }
    settle_takeover_promise(data_dir, cluster_paths, runtime, local, &key.1, promise)
}

/// Submit, or re-send, the promise's identical swap and end the promise by
/// the swap's own outcome. An unknown outcome keeps the promise.
pub(in crate::runtime_host) fn settle_takeover_promise(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    local: &NodeId,
    shard_id: &crate::DfsMetadataShardId,
    promise: DfsMetadataTakeoverPromise,
) -> TakeOutcome {
    #[cfg(any(test, feature = "test-support"))]
    if test_hooks::stops_after_promise(data_dir) {
        return TakeOutcome::Failed {
            reason: "stopped after the promise".into(),
        };
    }
    let Some(mut client) = daemon_dfs_control_plane_client_on_runtime(cluster_paths, Some(runtime))
    else {
        return TakeOutcome::Settled {
            promise,
            settlement: DfsMetadataTakeoverSettlement::Unknown {
                detail: "no cluster control-plane client".into(),
            },
        };
    };
    let settlement = match DfsControlPlaneStore::new(client.as_mut()).settle_metadata_takeover_swap(
        &promise.cut,
        local,
        &promise.epoch_start,
        &promise.swap_request_id,
    ) {
        Ok(settlement) => settlement,
        Err(error) => DfsMetadataTakeoverSettlement::Unknown {
            detail: error.to_string(),
        },
    };
    let end = match settlement {
        DfsMetadataTakeoverSettlement::Applied
        | DfsMetadataTakeoverSettlement::RejectedEpochAdvanced => {
            DfsMetadataTakeoverPromiseEnd::Kept
        }
        DfsMetadataTakeoverSettlement::RejectedEpochUnchanged => {
            DfsMetadataTakeoverPromiseEnd::Released
        }
        DfsMetadataTakeoverSettlement::Unknown { .. } => {
            return TakeOutcome::Settled {
                promise,
                settlement,
            };
        }
    };
    let ended = with_daemon_dfs_main_maintenance_service(data_dir, runtime, |service| {
        service.metadata_backend.as_deref_mut().map(|backend| {
            backend.settle_metadata_takeover_promise(shard_id, &promise.swap_request_id, end)
        })
    });
    match ended {
        Ok(Some(Ok(()))) => TakeOutcome::Settled {
            promise,
            settlement,
        },
        // The swap's outcome is known but the promise is not ended yet: the
        // next settlement repeats the same submission and ends it.
        other => TakeOutcome::Settled {
            promise,
            settlement: DfsMetadataTakeoverSettlement::Unknown {
                detail: format!("the promise end was not recorded: {other:?}"),
            },
        },
    }
}

/// Activation of a new primary at a takeover epoch: rebind the store in
/// place, align every reachable write-set member with the epoch start, and
/// request the reconciliation that resumes the shard's work. Idempotent.
pub(in crate::runtime_host) fn activate_metadata_takeover(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    local: &NodeId,
    key: &ShardKey,
    members: &[NodeId],
    unreachable: &BTreeSet<NodeId>,
) -> Result<Vec<(NodeId, crate::DfsWriteSetAlignment)>, String> {
    #[cfg(any(test, feature = "test-support"))]
    while test_hooks::holds_activation(data_dir) {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let cut = read_cut(cluster_paths, runtime, key, members)?;
    if &cut.shard.primary_node_id != local {
        return Err("this node is no longer the shard primary".into());
    }
    // A promise from the read epoch ends kept, and the fence it left is
    // retired: this node's own record is at the new epoch.
    if let Some(promise) = read_takeover_promise(data_dir, &key.1)
        && promise.cut.shard.epoch < cut.shard.epoch
    {
        with_daemon_dfs_main_maintenance_service(data_dir, runtime, |service| {
            service.metadata_backend.as_deref_mut().map(|backend| {
                backend.settle_metadata_takeover_promise(
                    &key.1,
                    &promise.swap_request_id,
                    DfsMetadataTakeoverPromiseEnd::Kept,
                )
            })
        })
        .map_err(|error| error.to_string())?
        .transpose()
        .map_err(|error| error.to_string())?;
    }
    retire_takeover_fence(data_dir, runtime, &key.1, cut.shard.epoch)?;
    // The maintenance checkout alone rebinds the store in place. It waits on
    // no member, so it may hold the node's global admission.
    with_daemon_dfs_main_maintenance_service(data_dir, runtime, |_| ())
        .map_err(|error| error.to_string())?;
    let write_set_members = cut
        .write_set_state()
        .switched()
        .map(|write_set| {
            write_set
                .write_set
                .iter()
                .filter(|member| *member != local && !unreachable.contains(*member))
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let aligned = align_members(data_dir, runtime, key, &cut, &write_set_members)?;
    if let Err(error) =
        request_daemon_dfs_maintenance_reconciliation(&DfsRuntimePaths::new(data_dir))
    {
        return Err(format!("the reconciliation was not requested: {error}"));
    }
    Ok(aligned)
}

/// Retire this member's acceptance fence once its record view reached it.
/// A no-op while a promise is unsettled or the view is below the fence.
pub(in crate::runtime_host) fn retire_takeover_fence(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    shard_id: &crate::DfsMetadataShardId,
    observed_epoch: u64,
) -> Result<(), String> {
    with_daemon_dfs_main_maintenance_service(data_dir, runtime, |service| {
        service
            .metadata_backend
            .as_deref_mut()
            .map(|backend| backend.retire_write_set_member_fence(shard_id, observed_epoch))
    })
    .map_err(|error| error.to_string())?
    .transpose()
    .map(drop)
    .map_err(|error| error.to_string())
}

/// Align `targets`, members a commit or the publisher found unaligned.
pub(in crate::runtime_host) fn align_metadata_takeover_members(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    key: &ShardKey,
    members: &[NodeId],
    targets: &BTreeSet<NodeId>,
) -> Result<Vec<(NodeId, crate::DfsWriteSetAlignment)>, String> {
    let cut = read_cut(cluster_paths, runtime, key, members)?;
    align_members(data_dir, runtime, key, &cut, targets)
}

fn read_cut(
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    key: &ShardKey,
    members: &[NodeId],
) -> Result<crate::DfsMetadataShardRecordCut, String> {
    let mut client = daemon_dfs_control_plane_client_on_runtime(cluster_paths, Some(runtime))
        .ok_or_else(|| "no cluster control-plane client".to_string())?;
    DfsControlPlaneStore::new(client.as_mut())
        .read_metadata_shard_record_cut(&key.0, &key.1.0, members)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "the shard no longer exists".to_string())
}

/// One alignment probe per member under the shard's admission, so no commit
/// races a member that still holds an old tail, while the members' answers
/// to other primaries' probes are admitted beside it. A member whose journal
/// differs from the epoch's history is repaired through the ordinary
/// durability barrier, which installs a snapshot.
fn align_members(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    key: &ShardKey,
    cut: &crate::DfsMetadataShardRecordCut,
    targets: &BTreeSet<NodeId>,
) -> Result<Vec<(NodeId, crate::DfsWriteSetAlignment)>, String> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let (peer, _) = daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
    let sender = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        data_dir, peer,
    );
    with_daemon_dfs_exact_shard_service(data_dir, runtime, &cut.shard, "write-set-alignment", |service| {
        let mut aligned = Vec::new();
        for member in targets {
            let alignment = service
                .align_write_set_member(cut, member, &sender)
                .unwrap_or_else(|error| crate::DfsWriteSetAlignment::NotReady {
                    reason: error.to_string(),
                });
            if matches!(alignment, crate::DfsWriteSetAlignment::NeedsSnapshot { .. })
                && let Ok(Some(head)) = service
                    .metadata_backend
                    .as_deref()
                    .map(|backend| backend.metadata_prefix_proof(&key.1))
                    .transpose()
                    .map(Option::flatten)
            {
                let _ = service
                    .catch_up_metadata_shard_member(&cut.shard, &key.1, &head, member, None, true);
            }
            aligned.push((member.clone(), alignment));
        }
        aligned
    })
    .map_err(|error| error.to_string())
}

/// The unsettled promise this node holds for the shard, if any.
pub(in crate::runtime_host) fn read_takeover_promise(
    data_dir: &SinomoDataDir,
    shard_id: &crate::DfsMetadataShardId,
) -> Option<DfsMetadataTakeoverPromise> {
    let path = crate::default_dfs_metadata_shards_root(data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0))
        .join("coordination")
        .join("write-set-member-state.sobj");
    crate::metadata_io::read_sobj_optional::<crate::DfsMetadataWriteSetMemberState>(&path)
        .ok()
        .flatten()
        .and_then(|state| state.promise)
}

#[cfg(any(test, feature = "test-support"))]
pub(in crate::runtime_host) mod test_hooks {
    //! Test support: stop a candidate between its promise and its swap, as a
    //! crash there would, or hold a new primary's activation.

    use super::*;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex};

    static STOP_AFTER_PROMISE: LazyLock<Mutex<BTreeSet<PathBuf>>> = LazyLock::new(Default::default);
    static HOLD_ACTIVATION: LazyLock<Mutex<BTreeSet<PathBuf>>> = LazyLock::new(Default::default);

    fn set(set: &Mutex<BTreeSet<PathBuf>>, root: PathBuf, on: bool) {
        let mut set = set.lock().unwrap_or_else(|error| error.into_inner());
        if on {
            set.insert(root);
        } else {
            set.remove(&root);
        }
    }

    pub(crate) fn stop_takeover_after_promise_for_test(root: PathBuf, stop: bool) {
        set(&STOP_AFTER_PROMISE, root, stop);
    }

    pub(crate) fn hold_takeover_activation_for_test(root: PathBuf, hold: bool) {
        set(&HOLD_ACTIVATION, root, hold);
    }

    pub(super) fn stops_after_promise(data_dir: &SinomoDataDir) -> bool {
        STOP_AFTER_PROMISE
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&data_dir.root())
    }

    pub(super) fn holds_activation(data_dir: &SinomoDataDir) -> bool {
        HOLD_ACTIVATION
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&data_dir.root())
    }
}
