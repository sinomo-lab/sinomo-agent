//! Publish a frozen inode patch set against the base observed by the mount.
use super::*;
use std::collections::{BTreeMap, VecDeque};

struct PackedRanges<'a>(VecDeque<Box<dyn Read + 'a>>);
impl Read for PackedRanges<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while let Some(part) = self.0.front_mut() {
            let n = part.read(out)?;
            if n != 0 || out.is_empty() {
                return Ok(n);
            }
            self.0.pop_front();
        }
        Ok(0)
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Mounted-adapter flush of a frozen inode patch set.
    #[doc(hidden)]
    pub fn posix_publish_dirty_ranges(
        &mut self,
        base: &DfsStat,
        dirty: &BTreeMap<u64, Vec<u8>>,
        operation_id: &str,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let _plan = self.namespace_execution.begin_planning();
        crate::service_routed_write::validate_write_operation_id(operation_id)?;
        let mut object = self.current_read_object_metadata_distributed(&base.object_id)?;
        if object.file_system_id != base.file_system_id || object.kind != DfsObjectKind::File {
            return Err(DfsPosixError::NotFound {
                path: base.path.clone(),
            });
        }
        let Some(base_size) = base.size_bytes else {
            return Err(DfsPosixError::Conflict {
                path: object.path,
                reason: "dirty range base has no length".into(),
            });
        };
        let target = stable_write_target_version(&self.node_id, &base.object_id, operation_id);
        if object.version != base.version && object.version != target {
            return Err(DfsPosixError::Conflict {
                path: object.path,
                reason: "dirty range base was replaced".into(),
            });
        }
        if object.version == base.version
            && (object.content_hash != base.content_hash
                || object.content_size_bytes != base.size_bytes)
        {
            return Err(DfsPosixError::Conflict {
                path: object.path,
                reason: "dirty ranges name a different base content identity".into(),
            });
        }
        let path = object.path.clone();
        self.ensure_path_not_in_cross_shard_flight(&object.file_system_id, &path)?;
        self.ensure_path_not_reserved_by_directory_move(&object.file_system_id, &path)?;
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let request = DfsWriteRequest {
            file_system_id: base.file_system_id.clone(),
            object_id: base.object_id.clone(),
            target_version: target,
            requesting_node_id: self.node_id.clone(),
        };
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;
        let mut parts = PackedRanges(VecDeque::new());
        let mut layout = Vec::with_capacity(dirty.len());
        let mut end = base_size;
        let mut packed_len = 0u64;
        for (&offset, data) in dirty {
            if data.is_empty() {
                continue;
            }
            let gap = offset.saturating_sub(end);
            if gap > 0 {
                parts.0.push_back(Box::new(std::io::repeat(0).take(gap)));
            }
            let size = gap
                .checked_add(data.len() as u64)
                .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
            let start = offset - gap;
            end = end.max(
                start
                    .checked_add(size)
                    .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?,
            );
            packed_len = packed_len
                .checked_add(size)
                .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
            layout.push((start, size));
            parts.0.push_back(Box::new(data.as_slice()));
        }
        if layout.is_empty() {
            return self.committed_write_stat(&path);
        }
        // Replays carry precisely the original base identity even if the original
        // reply was lost after publication. The receiver checks the extent layout.
        object.content_hash = base.content_hash.clone();
        object.content_size_bytes = Some(base_size);
        object.version = base.version.clone();
        self.writes
            .follow_write(&request.object_id, &request.target_version, true);
        let outcome = self.publish_packed_range_write(
            request,
            object,
            operation_id,
            layout,
            packed_len,
            &mut parts,
            plan.desired_replica_nodes,
            options,
        )?;
        match self.settle_ready_write_outcome(outcome, false)? {
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash,
                ..
            } => {
                let stat = self.committed_write_stat(&path)?;
                if stat.object_id != base.object_id
                    || stat.version != version
                    || stat.content_hash.as_ref() != Some(&content_hash)
                {
                    return Err(DfsPosixError::Conflict {
                        path,
                        reason: "the published range revision is no longer current".into(),
                    });
                }
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => Err(DfsPosixError::PendingWrite {
                intent_id,
                durable_receipts,
                required_receipts,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disjoint_inode_ranges_publish_one_version_and_replay_that_version() {
        let (runtime, mut client, shard) = crate::test_support::current_native_metadata_service(
            "native-packed-inode-ranges",
            3,
            "/",
        )
        .unwrap();
        let opts = DfsWriteOptions::replicated_eventual(
            std::iter::once(shard.primary_node_id.clone())
                .chain(shard.replica_node_ids.clone())
                .collect(),
        );
        let path = "/packed-inode";
        let base = client
            .posix_write_file(path, b"0123456789", opts.clone())
            .unwrap();
        let ranges = BTreeMap::from([
            (0, b"ab".to_vec()),
            (5, b"cd".to_vec()),
            (14, b"ef".to_vec()),
        ]);
        let result = client
            .posix_publish_dirty_ranges(&base, &ranges, "packed_test_one", opts.clone())
            .unwrap();
        let bytes = b"ab234cd789\0\0\0\0ef";
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), bytes);
        assert_eq!(
            result.content_hash.as_deref(),
            Some(crate::content_hash(bytes).as_str())
        );
        let replay = client
            .posix_publish_dirty_ranges(&base, &ranges, "packed_test_one", opts.clone())
            .unwrap();
        assert_eq!(result.version, replay.version);
        let different = BTreeMap::from([
            (1, b"ab".to_vec()),
            (5, b"cd".to_vec()),
            (14, b"ef".to_vec()),
        ]);
        assert!(
            client
                .posix_publish_dirty_ranges(&base, &different, "packed_test_one", opts)
                .is_err()
        );
        let inspector = crate::LocalSobjMetadataBackend::open_for_inspection(
            crate::DfsMetadataShardId::new(&shard.shard_id),
            crate::local_dfs_metadata_shard_path(
                runtime.data_dir(&shard.primary_node_id),
                &crate::DfsMetadataShardId::new(&shard.shard_id),
            ),
        )
        .unwrap();
        let snapshot = inspector.export_table_snapshot().unwrap();
        let versions: Vec<_> = snapshot
            .tables
            .versions
            .iter()
            .filter(|v| v.object_id == base.object_id)
            .collect();
        assert!(
            versions
                .iter()
                .all(|v| v.version == base.version || v.version == result.version)
        );
        assert!(versions.iter().any(|v| v.version == result.version));
        drop(client);
        runtime.close().unwrap();
    }
}
