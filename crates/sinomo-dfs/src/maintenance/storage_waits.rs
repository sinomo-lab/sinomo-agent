//! Namespace work parked on a shard whose storage is closed.
//!
//! A namespace job step that must wait for a shard's storage to be Active (a
//! directory move whose carried alias targets a converting or unprepared
//! shard) is refused with `Busy` and stays Running. The refusal records the
//! shard here for the node that ran the step: the refusal is the event, and
//! nothing scans jobs to rebuild the record. When shards' storage activates,
//! the node's host takes the records naming them and wakes maintenance only
//! if there was one, so a node with no parked work runs no pass.
//!
//! The refusal also records, per job, the step it refused and the shards it
//! waits on. While the job's durable record still runs that step next, the
//! coordinator reports the job as waiting on those shards (in its
//! count-contribution pages); any later step of the job changes its next
//! frame and ends the report, and an activation of one of the shards drops
//! the job's record.
//!
//! The records live in memory. After a restart the startup maintenance pass
//! runs every Running job again, and a step that still waits records its
//! shard anew.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use sinomo_core::node::NodeId;

use crate::{
    DfsFileSystemId, DfsMetadataShardId, DfsMetadataShardStorageKey, DfsNamespaceJobFrame,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus,
};

static PARKED: Mutex<BTreeMap<NodeId, BTreeSet<DfsMetadataShardStorageKey>>> =
    Mutex::new(BTreeMap::new());

/// One job's refused step and the shards it waits on.
struct JobPark {
    frame: DfsNamespaceJobFrame,
    shards: BTreeSet<DfsMetadataShardId>,
}

type JobKey = (DfsFileSystemId, String);

static JOB_PARKS: Mutex<BTreeMap<NodeId, BTreeMap<JobKey, JobPark>>> = Mutex::new(BTreeMap::new());

/// `job`'s step at `frame` on `node` was refused until each of `shards` of
/// the job's filesystem has Active storage. `frame` is the job's next frame
/// as its durable record keeps it.
pub(crate) fn record_job_parked_on_shard_storage(
    node: &NodeId,
    job: &DfsNamespaceJobRecord,
    frame: &DfsNamespaceJobFrame,
    shards: impl IntoIterator<Item = DfsMetadataShardId>,
) {
    let shards = shards.into_iter().collect::<BTreeSet<_>>();
    if shards.is_empty() {
        return;
    }
    JOB_PARKS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(node.clone())
        .or_default()
        .insert(
            (job.file_system_id.clone(), job.job_id.clone()),
            JobPark {
                frame: frame.clone(),
                shards,
            },
        );
}

/// The shards `job`, as its current record stands, waits on at `node`: those
/// its refused step named, while that step is still its next frame and it is
/// Running. Empty otherwise.
pub(crate) fn job_storage_waits(
    node: &NodeId,
    job: &DfsNamespaceJobRecord,
) -> Vec<DfsMetadataShardId> {
    if job.status != DfsNamespaceJobStatus::Running {
        return Vec::new();
    }
    let parks = JOB_PARKS.lock().unwrap_or_else(|error| error.into_inner());
    parks
        .get(node)
        .and_then(|parks| parks.get(&(job.file_system_id.clone(), job.job_id.clone())))
        .filter(|park| job.pending_frames.last() == Some(&park.frame))
        .map(|park| park.shards.iter().cloned().collect())
        .unwrap_or_default()
}

/// Remove `node`'s record of the job `job_id`, returning the shards it last
/// waited on.
pub(crate) fn take_job_storage_waits(
    node: &NodeId,
    file_system_id: &DfsFileSystemId,
    job_id: &str,
) -> BTreeSet<DfsMetadataShardId> {
    let mut parks = JOB_PARKS.lock().unwrap_or_else(|error| error.into_inner());
    let Some(node_parks) = parks.get_mut(node) else {
        return BTreeSet::new();
    };
    let taken = node_parks
        .remove(&(file_system_id.clone(), job_id.to_owned()))
        .map(|park| park.shards)
        .unwrap_or_default();
    if node_parks.is_empty() {
        parks.remove(node);
    }
    taken
}

/// A job step on `node` was refused until each of `shards` has Active storage.
pub(crate) fn record_parked_on_shard_storage(
    node: &NodeId,
    shards: impl IntoIterator<Item = DfsMetadataShardStorageKey>,
) {
    let mut shards = shards.into_iter().peekable();
    if shards.peek().is_none() {
        return;
    }
    PARKED
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(node.clone())
        .or_default()
        .extend(shards);
}

/// Take `node`'s records for the `activated` shards. Returns whether any work
/// on the node was parked on one of them.
pub(crate) fn take_parked_on_activated_shard_storage(
    node: &NodeId,
    activated: &BTreeSet<DfsMetadataShardStorageKey>,
) -> bool {
    // A job waiting on an activated shard runs its step again in the
    // maintenance pass this wakes, and records what it still waits on.
    let mut parks = JOB_PARKS.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(node_parks) = parks.get_mut(node) {
        node_parks.retain(|(file_system_id, _), park| {
            !park
                .shards
                .iter()
                .any(|shard| activated.contains(&(file_system_id.clone(), shard.clone())))
        });
        if node_parks.is_empty() {
            parks.remove(node);
        }
    }
    drop(parks);
    let mut parked = PARKED.lock().unwrap_or_else(|error| error.into_inner());
    let Some(shards) = parked.get_mut(node) else {
        return false;
    };
    let before = shards.len();
    shards.retain(|shard| !activated.contains(shard));
    let resumed = shards.len() < before;
    if shards.is_empty() {
        parked.remove(node);
    }
    resumed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsMetadataShardId;

    fn shard(name: &str) -> DfsMetadataShardStorageKey {
        (crate::sifs_file_system_id(), DfsMetadataShardId::new(name))
    }

    #[test]
    fn an_activation_resumes_only_the_work_parked_on_its_shard() {
        let parked = NodeId::from_legacy_named("storage-waits-parked").unwrap();
        let idle = NodeId::from_legacy_named("storage-waits-idle").unwrap();
        record_parked_on_shard_storage(&parked, [shard("users")]);
        let activated = BTreeSet::from([shard("users")]);
        assert!(!take_parked_on_activated_shard_storage(&idle, &activated));
        assert!(!take_parked_on_activated_shard_storage(
            &parked,
            &BTreeSet::from([shard("services")])
        ));
        assert!(take_parked_on_activated_shard_storage(&parked, &activated));
        // Taken once: a later activation of the same shard finds no work.
        assert!(!take_parked_on_activated_shard_storage(&parked, &activated));
    }

    fn scan(cursor: Option<&str>) -> DfsNamespaceJobFrame {
        DfsNamespaceJobFrame::CrossShardDirScan {
            source_path: "/tree".into(),
            target_path: "/services/tree".into(),
            cursor: cursor.map(|path| crate::DfsDirectoryPageCursor {
                path: path.into(),
                object_id: crate::DfsObjectId::from_legacy_named("storage-waits-cursor").unwrap(),
                dentry: None,
                shard_id: None,
            }),
        }
    }

    fn job(frames: Vec<DfsNamespaceJobFrame>) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: "storage-waits-job".into(),
            file_system_id: crate::sifs_file_system_id(),
            origin: None,
            kind: crate::DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/tree".into(),
            target_path: Some("/services/tree".into()),
            pending_frames: frames,
            processed_object_count: 0,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        }
    }

    #[test]
    fn a_job_waits_only_while_its_refused_step_is_next() {
        let node = NodeId::from_legacy_named("storage-waits-job-node").unwrap();
        let parked = job(vec![scan(None)]);
        let users = DfsMetadataShardId::new("users");
        record_job_parked_on_shard_storage(&node, &parked, &scan(None), [users.clone()]);
        assert_eq!(job_storage_waits(&node, &parked), vec![users.clone()]);
        let other = NodeId::from_legacy_named("storage-waits-job-other").unwrap();
        assert!(job_storage_waits(&other, &parked).is_empty(), "the record is the node's");
        // A later step moved the job on: its next frame is another page.
        assert!(job_storage_waits(&node, &job(vec![scan(Some("/tree/b"))])).is_empty());
        let mut completed = parked.clone();
        completed.status = DfsNamespaceJobStatus::Completed;
        assert!(job_storage_waits(&node, &completed).is_empty());
        // The shard's activation drops the record.
        take_parked_on_activated_shard_storage(
            &node,
            &BTreeSet::from([(crate::sifs_file_system_id(), users.clone())]),
        );
        assert!(job_storage_waits(&node, &parked).is_empty());
        record_job_parked_on_shard_storage(&node, &parked, &scan(None), [users.clone()]);
        assert_eq!(
            take_job_storage_waits(&node, &parked.file_system_id, &parked.job_id),
            BTreeSet::from([users])
        );
        assert!(job_storage_waits(&node, &parked).is_empty());
    }
}
