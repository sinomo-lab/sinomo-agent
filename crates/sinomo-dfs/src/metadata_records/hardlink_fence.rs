//! Exact finite target-fence payload carried by a hardlink coordinator.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsHardlinkTargetFence {
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    pub origin: Option<DfsNamespaceJobOrigin>,
    pub kind: DfsNamespaceJobKind,
    pub status: DfsNamespaceJobStatus,
    pub root_path: String,
    pub target_path: Option<String>,
    pub processed_object_count: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    pub failure_reason: Option<String>,
    pub source_path: String,
    pub expected_target: DfsNamespaceObjectPrecondition,
    pub committed_target: DfsObjectMetadata,
}
impl DfsHardlinkTargetFence {
    pub fn from_job(mut job: DfsNamespaceJobRecord) -> crate::metadata_error::Result<Self> {
        if job.kind != DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            || job.pending_frames.len() != 1
        {
            return Err(crate::MetadataBackendError::Serde(
                "hardlink target fence must preserve exactly one reservation obligation".into(),
            ));
        }
        let DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path,
            expected_target,
            committed_target,
        } = job.pending_frames.pop().expect("one checked obligation")
        else {
            return Err(crate::MetadataBackendError::Serde(
                "hardlink target fence cannot contain executable or nested job frames".into(),
            ));
        };
        Ok(Self {
            job_id: job.job_id,
            file_system_id: job.file_system_id,
            origin: job.origin,
            kind: job.kind,
            status: job.status,
            root_path: job.root_path,
            target_path: job.target_path,
            processed_object_count: job.processed_object_count,
            created_at: job.created_at,
            updated_at: job.updated_at,
            completed_at: job.completed_at,
            failure_reason: job.failure_reason,
            source_path,
            expected_target,
            committed_target,
        })
    }
    pub fn into_job(self) -> crate::metadata_error::Result<DfsNamespaceJobRecord> {
        if self.kind != DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary {
            return Err(crate::MetadataBackendError::Serde(
                "compact hardlink target fence changed its exact job kind".into(),
            ));
        }
        Ok(DfsNamespaceJobRecord {
            job_id: self.job_id,
            file_system_id: self.file_system_id,
            origin: self.origin,
            kind: self.kind,
            status: self.status,
            root_path: self.root_path,
            target_path: self.target_path,
            pending_frames: vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: self.source_path,
                expected_target: self.expected_target,
                committed_target: self.committed_target,
            }],
            processed_object_count: self.processed_object_count,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
            failure_reason: self.failure_reason,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_fence_preserves_every_job_and_reservation_field() {
        let owner = NodeId::from_legacy_named("finite-hardlink-fence-owner").unwrap();
        let object: DfsObjectMetadata = serde_json::from_value(serde_json::json!({
            "object_id":DfsObjectId::generate(),"file_system_id":DfsFileSystemId::generate(),
            "path":"/target","kind":"file","owner_node_id":owner,
            "current_locations":[owner.clone()],"version":DfsVersion::generate(),
            "policy":"local-owned","mode":0o640,"uid":42,"gid":91,
            "content_hash":"exact-reserved-content","content_size_bytes":17,
            "created_at_unix_nanos":11,"changed_at_unix_nanos":13,
            "modified_at_unix_nanos":12,"accessed_at_unix_nanos":14
        }))
        .unwrap();
        let expected = DfsNamespaceObjectPrecondition {
            object_id: object.object_id.clone(),
            version: DfsVersion::generate(),
        };
        let job = DfsNamespaceJobRecord {
            job_id: "fence-id".into(),
            file_system_id: object.file_system_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/target".into(),
            target_path: Some("/exact-target-context".into()),
            pending_frames: vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: "/source".into(),
                expected_target: expected,
                committed_target: object,
            }],
            processed_object_count: 7,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: Some(OffsetDateTime::UNIX_EPOCH),
            failure_reason: Some("retained-exact-context".into()),
        };
        let fence = DfsHardlinkTargetFence::from_job(job.clone()).unwrap();
        let wire = serde_json::to_vec(&fence).unwrap();
        let decoded: DfsHardlinkTargetFence = serde_json::from_slice(&wire).unwrap();
        assert_eq!(decoded.into_job().unwrap(), job);
        let mut extra = job.clone();
        extra
            .pending_frames
            .push(DfsNamespaceJobFrame::CompleteDirectory {
                path: "/extra-obligation".into(),
            });
        assert!(DfsHardlinkTargetFence::from_job(extra).is_err());
        let mut nested = job.clone();
        nested.pending_frames = vec![DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
            fence: Box::new(fence),
            source_path: "/source".into(),
        }];
        assert!(DfsHardlinkTargetFence::from_job(nested).is_err());
    }
}
