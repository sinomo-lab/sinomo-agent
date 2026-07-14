//! POSIX read and read-through helpers.
//!
//! This module owns local POSIX reads, distributed read-through, remote read
//! requests, stream/range read adapters, and bulk-read batching for the POSIX
//! facade. Write paths may reuse the read-through helper when they need the
//! current file bytes before applying a range patch.

use super::{
    POSIX_READ_FRESHNESS_SYNC_TIMEOUT, POSIX_REMOTE_READ_IDLE_WAIT_INITIAL,
    POSIX_REMOTE_READ_IDLE_WAIT_MAX, POSIX_REMOTE_READ_TIMEOUT, now_unix_nanos,
};
use crate::namespace::next_version;
use crate::posix_model::{
    DfsPosixError, DfsPosixFileRangeReadStream, DfsPosixFileReadStream, DfsStat, Result,
    read_conflict_error,
};
use crate::posix_path::normalize_posix_path;
use crate::{
    DfsBulkReadItem, DfsBulkReadItemResponse, DfsBulkReadRequest, DfsError, DfsMessageTransport,
    DfsNetworkMessage, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsPosixFilePrefixRead,
    DfsReadData, DfsReadRangeData, DfsReadResult, DfsVersion, NetworkedDfsRangeReadOutcome,
    NetworkedDfsRangeReadStreamOutcome, NetworkedDfsReadFreshness, NetworkedDfsReadOutcome,
    NetworkedDfsReadStreamOutcome, NetworkedDfsService, NetworkedDfsServiceEvent,
    sifs_file_system_id,
};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Clone, Debug)]
struct PosixPendingBulkRead {
    index: usize,
    path: String,
    request_id: String,
    object_id: DfsObjectId,
    version: DfsVersion,
    content_hash: String,
    content_size_bytes: u64,
}

#[derive(Clone, Debug)]
enum PosixBulkReadPlan {
    Local(Vec<u8>),
    Remote {
        source: NodeId,
        pending: PosixPendingBulkRead,
    },
    Fallback,
}

fn validate_complete_posix_range_content_hash(
    object: &DfsObjectMetadata,
    data: &DfsReadRangeData,
) -> std::result::Result<(), DfsError> {
    // Truncated ranges have no whole-object content proof. When a range covers
    // the complete object, preserve full-read integrity by hashing the bytes
    // against the authoritative metadata before exposing them to the caller.
    if data.offset != 0 || u64::try_from(data.bytes.len()).ok() != Some(data.total_size_bytes) {
        return Ok(());
    }
    let actual_hash = crate::content_hash(&data.bytes);
    let expected_hash = object
        .content_hash
        .as_deref()
        .unwrap_or("committed content hash");
    if actual_hash != expected_hash {
        return Err(DfsError::ContentHashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }
    Ok(())
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn with_posix_remote_read_deadline<R>(
        &mut self,
        operation: impl FnOnce(&mut Self, Instant) -> Result<R>,
    ) -> Result<R> {
        let requested_deadline = Instant::now() + POSIX_REMOTE_READ_TIMEOUT;
        let previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let effective_deadline = previous_deadline
            .filter(|existing| *existing < requested_deadline)
            .unwrap_or(requested_deadline);
        if effective_deadline != requested_deadline {
            self.transport
                .set_operation_deadline(Some(effective_deadline));
        }
        let result = operation(self, effective_deadline);
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    fn posix_file_content_object(
        &self,
        path: String,
        object: DfsObjectMetadata,
    ) -> Result<(String, DfsObjectMetadata)> {
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        Ok((path, object))
    }

    fn local_posix_file_content_object(
        &self,
        path: &str,
        follow_final: bool,
    ) -> Result<(String, DfsObjectMetadata)> {
        let path = self.resolve_existing_posix_path(path, follow_final)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        self.posix_file_content_object(path, object)
    }

    fn distributed_posix_file_content_object(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<(String, DfsObjectMetadata)> {
        let (path, object) = self.lookup_sifs_path_distributed_synced(path, follow_final)?;
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        self.posix_file_content_object(path, object)
    }

    pub fn posix_read_file(&self, path: &str) -> Result<Vec<u8>> {
        self.with_metadata_lookup_cache(|service| {
            let (path, object) = service.local_posix_file_content_object(path, true)?;
            match service.read_object_metadata_local_content_with_state(&object, false)? {
                NetworkedDfsReadOutcome::Found {
                    data: DfsReadData { bytes, .. },
                    ..
                } => Ok(bytes),
                NetworkedDfsReadOutcome::PendingReplication { .. }
                | NetworkedDfsReadOutcome::NotConfirmed { .. }
                | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
                NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                    Err(read_conflict_error(path, conflicts))
                }
            }
        })
    }

    pub fn posix_read_file_range(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.with_metadata_lookup_cache(|service| {
            let (path, object) = service.local_posix_file_content_object(path, true)?;
            match service
                .read_object_metadata_local_range_with_state(&object, offset, max_bytes, false)?
            {
                NetworkedDfsRangeReadOutcome::Found {
                    data: DfsReadRangeData { bytes, .. },
                    ..
                } => Ok(bytes),
                NetworkedDfsRangeReadOutcome::PendingReplication { .. }
                | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
                | NetworkedDfsRangeReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
                NetworkedDfsRangeReadOutcome::Conflicted { conflicts, .. } => {
                    Err(read_conflict_error(path, conflicts))
                }
            }
        })
    }

    pub fn posix_open_file_stream(&self, path: &str) -> Result<DfsPosixFileReadStream> {
        let (path, object) = self.local_posix_file_content_object(path, true)?;
        match self.open_local_object_metadata_stream_with_state(&object)? {
            NetworkedDfsReadStreamOutcome::Found(stream) => Ok(DfsPosixFileReadStream {
                object_id: stream.data.object_id,
                version: stream.data.version,
                content_hash: stream.data.content_hash,
                size_bytes: stream.total_size_bytes,
                reader: stream.reader,
            }),
            NetworkedDfsReadStreamOutcome::PendingReplication
            | NetworkedDfsReadStreamOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadStreamOutcome::Conflicted { conflicts } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_open_file_range_stream(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsPosixFileRangeReadStream> {
        let (path, object) = self.local_posix_file_content_object(path, true)?;
        match self.open_local_object_metadata_range_stream_with_state(&object, offset, max_bytes)? {
            NetworkedDfsRangeReadStreamOutcome::Found(stream) => Ok(DfsPosixFileRangeReadStream {
                object_id: stream.data.object_id,
                version: stream.data.version,
                content_hash: stream.data.content_hash,
                offset: stream.data.offset,
                total_size_bytes: stream.data.total_size_bytes,
                range_size_bytes: stream.range_size_bytes,
                reader: stream.reader,
            }),
            NetworkedDfsRangeReadStreamOutcome::PendingReplication
            | NetworkedDfsRangeReadStreamOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsRangeReadStreamOutcome::Conflicted { conflicts } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_read_file_distributed(&mut self, path: &str) -> Result<Vec<u8>> {
        self.check_cancelled("distributed read")?;
        let trace_started = Instant::now();
        let lookup_started = Instant::now();
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        let lookup_elapsed_ms = lookup_started.elapsed().as_millis();
        let fetch_started = Instant::now();
        let outcome = self.read_object_metadata_or_fetch_remote_without_cache(&object)?;
        let fetch_elapsed_ms = fetch_started.elapsed().as_millis();
        match outcome {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => {
                crate::dfs_perf_trace(format_args!(
                    "posix-read-distributed path={} object={} bytes={} lookup_ms={} fetch_ms={} total_ms={}",
                    path,
                    object.object_id,
                    bytes.len(),
                    lookup_elapsed_ms,
                    fetch_elapsed_ms,
                    trace_started.elapsed().as_millis()
                ));
                Ok(bytes)
            }
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    /// Reads at most `max_bytes` from the authoritative version of `path` and
    /// reports whether more content remains. Path metadata is resolved once,
    /// then reused for the range fetch so callers do not perform a separate
    /// distributed stat and read freshness round.
    pub fn posix_read_file_prefix_distributed(
        &mut self,
        path: &str,
        max_bytes: usize,
    ) -> Result<DfsPosixFilePrefixRead> {
        self.check_cancelled("distributed prefix read")?;
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        match self
            .read_object_metadata_range_or_fetch_remote_without_cache(&object, 0, max_bytes)?
        {
            NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
                validate_complete_posix_range_content_hash(&object, &data)?;
                Ok(DfsPosixFilePrefixRead {
                    bytes: data.bytes,
                    truncated: data.total_size_bytes > max_bytes_u64,
                })
            }
            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
            | NetworkedDfsRangeReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsRangeReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    /// Reads one bounded range from the authoritative version of `path`, fetching
    /// it from a healthy replica when the content is not local.
    pub fn posix_read_file_range_distributed(
        &mut self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.check_cancelled("distributed range read")?;
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        match self
            .read_object_metadata_range_or_fetch_remote_without_cache(&object, offset, max_bytes)?
        {
            NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                validate_complete_posix_range_content_hash(&object, &data)?;
                Ok(data.bytes)
            }
            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
            | NetworkedDfsRangeReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsRangeReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_read_files_distributed(&mut self, paths: &[String]) -> Vec<Result<Vec<u8>>> {
        if paths.len() <= 1 {
            return paths
                .iter()
                .map(|path| self.posix_read_file_distributed(path))
                .collect();
        }

        let mut results = (0..paths.len()).map(|_| None).collect::<Vec<_>>();
        let mut grouped = BTreeMap::<NodeId, Vec<PosixPendingBulkRead>>::new();

        let prefetched_objects = self.prefetch_sifs_paths_for_bulk_read(paths);

        for (index, input_path) in paths.iter().enumerate() {
            match self.prepare_posix_bulk_read(input_path, index, &prefetched_objects) {
                Ok(PosixBulkReadPlan::Local(bytes)) => {
                    results[index] = Some(Ok(bytes));
                }
                Ok(PosixBulkReadPlan::Remote { source, pending }) => {
                    grouped.entry(source).or_default().push(pending);
                }
                Ok(PosixBulkReadPlan::Fallback) => {}
                Err(error) => {
                    results[index] = Some(Err(error));
                }
            }
        }

        for (source_node_id, pending_reads) in grouped {
            match self.request_posix_bulk_read_from_source(&source_node_id, &pending_reads) {
                Ok(responses) => {
                    let mut pending_by_request_id = pending_reads
                        .into_iter()
                        .map(|pending| (pending.request_id.clone(), pending))
                        .collect::<BTreeMap<_, _>>();
                    for response in responses {
                        let Some(pending) = pending_by_request_id.remove(&response.request_id)
                        else {
                            continue;
                        };
                        results[pending.index] =
                            Some(self.validate_posix_bulk_read_response(&pending, response));
                    }
                    for pending in pending_by_request_id.into_values() {
                        results[pending.index] =
                            Some(self.posix_read_file_distributed(&pending.path));
                    }
                }
                Err(_) => {
                    for pending in pending_reads {
                        results[pending.index] =
                            Some(self.posix_read_file_distributed(&pending.path));
                    }
                }
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.unwrap_or_else(|| self.posix_read_file_distributed(&paths[index]))
            })
            .collect()
    }

    fn prefetch_sifs_paths_for_bulk_read(
        &mut self,
        paths: &[String],
    ) -> BTreeMap<String, DfsObjectMetadata> {
        if paths.len() <= 1 {
            return BTreeMap::new();
        }
        let trace_started = Instant::now();
        let mut grouped = BTreeMap::<NodeId, Vec<String>>::new();
        let mut seen = BTreeSet::<(NodeId, String)>::new();
        let mut prefetched = BTreeMap::new();
        for input_path in paths {
            let resolved = match self.resolve_existing_posix_path_distributed(input_path, true) {
                Ok(resolved) => resolved,
                Err(DfsPosixError::NotFound { .. }) => {
                    let Ok(normalized) = normalize_posix_path(input_path) else {
                        continue;
                    };
                    normalized
                }
                Err(_) => continue,
            };
            if self.lookup_sifs_path(&resolved).ok().flatten().is_some() {
                continue;
            }
            let Ok(Some(shard)) = self.routed_namespace_shard_for_path(&resolved) else {
                continue;
            };
            if !self.is_local_metadata_replica_for_shard(&shard) {
                continue;
            }
            let key = (shard.primary_node_id.clone(), resolved.clone());
            if seen.insert(key) {
                grouped
                    .entry(shard.primary_node_id)
                    .or_default()
                    .push(resolved);
            }
        }
        if grouped.is_empty() {
            return prefetched;
        }
        let mut requested_paths = 0_usize;
        let mut returned_objects = 0_usize;
        for (primary_node_id, lookup_paths) in grouped {
            requested_paths = requested_paths.saturating_add(lookup_paths.len());
            if let Ok(objects) = self.route_namespace_lookup_paths_to_metadata_primary(
                sifs_file_system_id(),
                primary_node_id,
                lookup_paths,
                POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
            ) {
                returned_objects = returned_objects.saturating_add(objects.len());
                for object in objects {
                    prefetched.insert(object.path.clone(), object);
                }
            }
        }
        crate::dfs_perf_trace(format_args!(
            "bulk-read-prefetch paths={} objects={} elapsed_ms={}",
            requested_paths,
            returned_objects,
            trace_started.elapsed().as_millis()
        ));
        prefetched
    }

    fn prepare_posix_bulk_read(
        &mut self,
        input_path: &str,
        index: usize,
        prefetched_objects: &BTreeMap<String, DfsObjectMetadata>,
    ) -> Result<PosixBulkReadPlan> {
        let resolved = self.resolve_existing_posix_path_distributed(input_path, true)?;
        let (path, object) = match prefetched_objects.get(&resolved) {
            Some(object) => self.posix_file_content_object(resolved, object.clone())?,
            None => self.distributed_posix_file_content_object(input_path, true)?,
        };
        self.prepare_posix_bulk_read_for_object(path, object, index)
    }

    fn prepare_posix_bulk_read_for_object(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
        index: usize,
    ) -> Result<PosixBulkReadPlan> {
        match self.read_object_metadata_local_if_current_location(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => return Ok(PosixBulkReadPlan::Local(bytes)),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                return Err(read_conflict_error(path, conflicts));
            }
            NetworkedDfsReadOutcome::NotConfirmed { .. } | NetworkedDfsReadOutcome::NotFound => {
                return Err(DfsPosixError::NotFound { path });
            }
            NetworkedDfsReadOutcome::PendingReplication { .. } => {}
        }

        let (Some(content_hash), Some(content_size_bytes)) =
            (object.content_hash.clone(), object.content_size_bytes)
        else {
            return Ok(PosixBulkReadPlan::Fallback);
        };
        if content_size_bytes > crate::DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            return Ok(PosixBulkReadPlan::Fallback);
        }
        let Some(source) = object
            .current_locations
            .iter()
            .find(|node_id| *node_id != &self.node_id)
            .cloned()
        else {
            return Ok(PosixBulkReadPlan::Fallback);
        };
        let request_id = format!("dfs_bulk_read_{}_{}", index, EventId::generate());
        Ok(PosixBulkReadPlan::Remote {
            source,
            pending: PosixPendingBulkRead {
                index,
                path,
                request_id,
                object_id: object.object_id,
                version: object.version,
                content_hash,
                content_size_bytes,
            },
        })
    }

    fn request_posix_bulk_read_from_source(
        &mut self,
        source_node_id: &NodeId,
        pending_reads: &[PosixPendingBulkRead],
    ) -> Result<Vec<DfsBulkReadItemResponse>> {
        self.with_posix_remote_read_deadline(|service, deadline| {
            service.request_posix_bulk_read_from_source_with_deadline(
                source_node_id,
                pending_reads,
                deadline,
            )
        })
    }

    fn request_posix_bulk_read_from_source_with_deadline(
        &mut self,
        source_node_id: &NodeId,
        pending_reads: &[PosixPendingBulkRead],
        deadline: Instant,
    ) -> Result<Vec<DfsBulkReadItemResponse>> {
        let request_id = format!("dfs_bulk_read_batch_{}", EventId::generate());
        let request = DfsBulkReadRequest {
            request_id: request_id.clone(),
            reads: pending_reads
                .iter()
                .map(|pending| DfsBulkReadItem {
                    request_id: pending.request_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    content_size_bytes: pending.content_size_bytes,
                })
                .collect(),
        };
        if !self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::BulkReadRequest(request),
        )? {
            return Err(DfsPosixError::Dfs(DfsError::Network {
                message: format!("bulk read source {source_node_id} is not routable"),
            }));
        }

        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
            self.check_cancelled("bulk read wait")?;
            let events = self.process_incoming()?;
            if !events.is_empty() {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            }
            for event in events {
                if let NetworkedDfsServiceEvent::RemoteBulkReadCompleted {
                    request_id: event_request_id,
                    source_node_id: event_source_node_id,
                    results,
                } = event
                    && event_request_id == request_id
                    && event_source_node_id == *source_node_id
                {
                    return Ok(results);
                }
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait = idle_wait.min(deadline.saturating_duration_since(now));
            if self
                .transport
                .wait_for_incoming(wait)
                .map_err(DfsError::from_network)?
            {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            } else {
                idle_wait = idle_wait
                    .saturating_mul(2)
                    .min(POSIX_REMOTE_READ_IDLE_WAIT_MAX);
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Network {
            message: format!("bulk read response from {source_node_id} was not delivered"),
        }))
    }

    fn validate_posix_bulk_read_response(
        &self,
        pending: &PosixPendingBulkRead,
        response: DfsBulkReadItemResponse,
    ) -> Result<Vec<u8>> {
        if response.object_id != pending.object_id {
            return Err(DfsPosixError::Dfs(DfsError::ContentHashMismatch {
                expected: pending.object_id.to_string(),
                actual: response.object_id.to_string(),
            }));
        }
        match response.result {
            DfsReadResult::Found(data) => {
                if data.object_id != pending.object_id {
                    return Err(DfsPosixError::Dfs(DfsError::ContentHashMismatch {
                        expected: pending.object_id.to_string(),
                        actual: data.object_id.to_string(),
                    }));
                }
                if data.version != pending.version {
                    return Err(DfsPosixError::Dfs(DfsError::ContentHashMismatch {
                        expected: pending.version.to_string(),
                        actual: data.version.to_string(),
                    }));
                }
                if data.content_hash.as_deref() != Some(pending.content_hash.as_str()) {
                    return Err(DfsPosixError::Dfs(DfsError::ContentHashMismatch {
                        expected: pending.content_hash.clone(),
                        actual: data.content_hash.unwrap_or_else(|| "none".to_string()),
                    }));
                }
                if data.bytes.len() as u64 != pending.content_size_bytes {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "bulk read size mismatch for {}: expected {}, actual {}",
                            pending.path,
                            pending.content_size_bytes,
                            data.bytes.len()
                        ),
                    }));
                }
                let actual_hash = crate::content_hash(&data.bytes);
                if actual_hash != pending.content_hash {
                    return Err(DfsPosixError::Dfs(DfsError::ContentHashMismatch {
                        expected: pending.content_hash.clone(),
                        actual: actual_hash,
                    }));
                }
                Ok(data.bytes)
            }
            DfsReadResult::RangeFound(_) => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("bulk read for {} returned a range result", pending.path),
            })),
            DfsReadResult::NotFound => Err(DfsPosixError::NotFound {
                path: pending.path.clone(),
            }),
            DfsReadResult::Rejected { reason } => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: reason,
            })),
        }
    }

    pub fn posix_read_file_update_access(&mut self, path: &str) -> Result<Vec<u8>> {
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        match self.read_object_metadata_or_fetch_remote_without_cache(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => {
                let mut object = self
                    .object_by_id(&object.object_id)?
                    .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                (_, object) = self.posix_file_content_object(path.clone(), object)?;
                // Only the metadata authority may journal the atime update;
                // appending it on a replica would fork the replica's journal
                // from the shard primary's stream (noatime-style degradation
                // on non-primary nodes).
                if !self.is_local_metadata_primary_for_object(&object)? {
                    return Ok(bytes);
                }
                object.accessed_at_unix_nanos = now_unix_nanos();
                object.version = next_version(&object.version);
                self.add_object_metadata(object)?;
                Ok(bytes)
            }
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    /// Read through a stable opened-object identity. A removed object fails
    /// instead of resolving the descriptor's old pathname to a replacement.
    /// Full open-unlinked lifetime is intentionally deferred; this method is
    /// the fail-closed boundary until orphan retention is implemented.
    pub fn posix_read_file_for_object(&mut self, object_id: &DfsObjectId) -> Result<Vec<u8>> {
        let object = self
            .current_read_object_metadata(object_id)
            .map_err(DfsPosixError::from)?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: object.path });
        }
        let path = object.path.clone();
        let (_, object) = self.posix_file_content_object(path.clone(), object)?;
        match self.read_object_metadata_or_fetch_remote_without_cache(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => {
                if self.is_local_metadata_primary_for_object(&object)? {
                    let mut current = self
                        .object_by_id(&object.object_id)?
                        .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                    if current.kind == DfsObjectKind::Tombstone {
                        return Err(DfsPosixError::NotFound { path });
                    }
                    current.accessed_at_unix_nanos = now_unix_nanos();
                    current.version = next_version(&current.version);
                    self.add_object_metadata(current)?;
                }
                Ok(bytes)
            }
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    /// Read one bounded range through a stable opened-object identity.
    ///
    /// This is the range counterpart to [`Self::posix_read_file_for_object`]
    /// used by FUSE file handles. It avoids materializing the complete object
    /// for each kernel read while retaining rename-safe opened-file semantics.
    pub fn posix_read_file_range_for_object(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.posix_read_file_range_for_object_with_access_update(
            object_id, offset, max_bytes, false,
        )
    }

    /// Read one opened-object range and record access time when this node owns
    /// the authoritative metadata shard.
    pub fn posix_read_file_range_for_object_update_access(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.posix_read_file_range_for_object_with_access_update(object_id, offset, max_bytes, true)
    }

    fn posix_read_file_range_for_object_with_access_update(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
        update_access: bool,
    ) -> Result<Vec<u8>> {
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let object = self
            .current_read_object_metadata(object_id)
            .map_err(DfsPosixError::from)?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: object.path });
        }
        let path = object.path.clone();
        let (_, object) = self.posix_file_content_object(path.clone(), object)?;
        self.posix_read_file_range_for_opened_metadata(
            path,
            object,
            offset,
            max_bytes,
            update_access,
        )
    }

    /// Read one range for a FUSE-style open handle using a fresh authoritative
    /// path lookup while fencing against pathname replacement.
    pub fn posix_read_file_range_for_opened_object(
        &mut self,
        path: &str,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
        update_access: bool,
    ) -> Result<Vec<u8>> {
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        if object.object_id != *object_id {
            // Open-unlinked lifetime is not implemented. Fail closed if the
            // pathname was removed or replaced rather than reading a new file
            // through the old handle.
            return Err(DfsPosixError::NotFound { path });
        }
        self.posix_read_file_range_for_opened_metadata(
            path,
            object,
            offset,
            max_bytes,
            update_access,
        )
    }

    /// Read one immutable range through the exact identity captured when a
    /// file handle was opened.
    ///
    /// Unlike [`Self::posix_read_file_range_for_opened_object`], this does not
    /// resolve the pathname or current object metadata. Content versions are
    /// immutable, so the opened version/hash/location tuple is sufficient for
    /// a local read or an exact remote range request. This preserves an open
    /// descriptor across rename/unlink and removes a metadata-authority RTT
    /// from every kernel read. Missing or mismatched content still fails
    /// closed through the existing exact-version and hash validation.
    pub fn posix_read_file_range_for_opened_stat(
        &mut self,
        path: &str,
        opened: &DfsStat,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let path = normalize_posix_path(path)?;
        if opened.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if opened.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }

        // The exact-content read path does not consult metadata ownership.
        // DfsStat deliberately omits owner_node_id, so use the local node as a
        // non-authoritative placeholder while reconstructing the immutable
        // content identity consumed by the range transport.
        let object = DfsObjectMetadata {
            object_id: opened.object_id.clone(),
            file_system_id: opened.file_system_id.clone(),
            path: path.clone(),
            kind: opened.kind.clone(),
            owner_node_id: self.node_id.clone(),
            current_locations: opened.current_locations.clone(),
            content_hash: opened.content_hash.clone(),
            content_size_bytes: opened.size_bytes,
            link_target_object_id: None,
            mode: opened.mode,
            uid: opened.uid,
            gid: opened.gid,
            accessed_at_unix_nanos: opened.accessed_at_unix_nanos,
            modified_at_unix_nanos: opened.modified_at_unix_nanos,
            created_at_unix_nanos: opened.created_at_unix_nanos,
            changed_at_unix_nanos: opened.changed_at_unix_nanos,
            version: opened.version.clone(),
            policy: opened.policy.clone(),
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        self.posix_read_file_range_for_opened_metadata(path, object, offset, max_bytes, false)
    }

    fn posix_read_file_range_for_opened_metadata(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        update_access: bool,
    ) -> Result<Vec<u8>> {
        match self
            .read_object_metadata_range_or_fetch_remote_without_cache(&object, offset, max_bytes)?
        {
            NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                validate_complete_posix_range_content_hash(&object, &data)?;
                if update_access && self.is_local_metadata_primary_for_object(&object)? {
                    let mut current = self
                        .object_by_id(&object.object_id)?
                        .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                    if current.kind == DfsObjectKind::Tombstone {
                        return Err(DfsPosixError::NotFound { path });
                    }
                    current.accessed_at_unix_nanos = now_unix_nanos();
                    current.version = next_version(&current.version);
                    self.add_object_metadata(current)?;
                }
                Ok(data.bytes)
            }
            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
            | NetworkedDfsRangeReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsRangeReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub(super) fn read_current_object_or_fetch_remote(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<NetworkedDfsReadOutcome> {
        self.with_posix_remote_read_deadline(|service, deadline| {
            service.read_current_object_or_fetch_remote_with_deadline(object_id, deadline)
        })
    }

    fn read_current_object_or_fetch_remote_with_deadline(
        &mut self,
        object_id: &DfsObjectId,
        deadline: Instant,
    ) -> Result<NetworkedDfsReadOutcome> {
        let initial = self.read_local_object_with_state(object_id, false)?;
        if !matches!(initial, NetworkedDfsReadOutcome::PendingReplication { .. }) {
            return Ok(initial);
        }
        let requested = self.request_remote_object_read(object_id)?;
        if requested.is_empty() {
            return Ok(initial);
        }
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
            self.check_cancelled("remote object read wait")?;
            let events = self.process_incoming()?;
            if !events.is_empty() {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            }
            let current = self.read_local_object_with_state(object_id, false)?;
            if !matches!(current, NetworkedDfsReadOutcome::PendingReplication { .. }) {
                return Ok(current);
            }
            if !self.has_pending_remote_read_for_current_object(object_id)? {
                if Instant::now() >= deadline {
                    return Ok(current);
                }
                if self.request_remote_object_read(object_id)?.is_empty() {
                    return Ok(current);
                }
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(current);
            }
            let remaining = deadline.saturating_duration_since(now);
            let until_retry = self
                .next_pending_remote_read_retry_for_current_object(object_id)?
                .map(|next_retry_at| next_retry_at.saturating_duration_since(now))
                .unwrap_or(remaining);
            // An already-due retry must still pace the loop: on a degraded link
            // every retry fails fast and stays due, and a zero wait here spun
            // this loop at a full core until the read timeout.
            let wait = idle_wait
                .min(remaining)
                .min(until_retry)
                .max(POSIX_REMOTE_READ_IDLE_WAIT_INITIAL.min(remaining));
            if self
                .transport
                .wait_for_incoming(wait)
                .map_err(DfsError::from_network)?
            {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            } else {
                idle_wait = idle_wait
                    .saturating_mul(2)
                    .min(POSIX_REMOTE_READ_IDLE_WAIT_MAX);
            }
        }
    }

    pub(super) fn read_object_metadata_or_fetch_remote_without_cache(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<NetworkedDfsReadOutcome> {
        self.with_posix_remote_read_deadline(|service, deadline| {
            service
                .read_object_metadata_or_fetch_remote_without_cache_with_deadline(object, deadline)
        })
    }

    fn read_object_metadata_or_fetch_remote_without_cache_with_deadline(
        &mut self,
        object: &DfsObjectMetadata,
        deadline: Instant,
    ) -> Result<NetworkedDfsReadOutcome> {
        // `object` came from the authoritative routed namespace lookup.
        // No-cache reads deliberately reuse it instead of re-resolving through a
        // non-replica's incomplete local object shard.
        let mut object = object.clone();
        self.apply_transient_remote_read_location_prunes(&mut object);
        let initial = self.read_object_metadata_local_if_current_location(&object)?;
        if !matches!(initial, NetworkedDfsReadOutcome::PendingReplication { .. }) {
            return Ok(initial);
        }
        let requested = self.request_remote_object_read_for_metadata_without_cache(&object)?;
        if requested.is_empty() {
            return Ok(initial);
        }
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
            self.check_cancelled("remote metadata read wait")?;
            let events = self.process_incoming()?;
            if !events.is_empty() {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            }
            for event in events {
                if let NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: event_object_id,
                    result: DfsReadResult::Found(data),
                    ..
                } = event
                    && event_object_id == object.object_id
                {
                    return Ok(NetworkedDfsReadOutcome::Found {
                        data,
                        freshness: NetworkedDfsReadFreshness::Current,
                    });
                }
            }
            self.apply_transient_remote_read_location_prunes(&mut object);
            let current = self.read_object_metadata_local_if_current_location(&object)?;
            if !matches!(current, NetworkedDfsReadOutcome::PendingReplication { .. }) {
                return Ok(current);
            }
            if !self.has_pending_remote_read_for_metadata(&object) {
                if Instant::now() >= deadline {
                    return Ok(current);
                }
                if self
                    .request_remote_object_read_for_metadata_without_cache(&object)?
                    .is_empty()
                {
                    return Ok(current);
                }
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(current);
            }
            let remaining = deadline.saturating_duration_since(now);
            let until_retry = self
                .next_pending_remote_read_retry_for_metadata(&object)
                .map(|next_retry_at| next_retry_at.saturating_duration_since(now))
                .unwrap_or(remaining);
            // Same pacing floor as `read_current_object_or_fetch_remote`: an
            // always-due retry on a degraded link must not spin this loop hot.
            let wait = idle_wait
                .min(remaining)
                .min(until_retry)
                .max(POSIX_REMOTE_READ_IDLE_WAIT_INITIAL.min(remaining));
            if self
                .transport
                .wait_for_incoming(wait)
                .map_err(DfsError::from_network)?
            {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            } else {
                idle_wait = idle_wait
                    .saturating_mul(2)
                    .min(POSIX_REMOTE_READ_IDLE_WAIT_MAX);
            }
        }
    }

    fn read_object_metadata_range_or_fetch_remote_without_cache(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        self.with_posix_remote_read_deadline(|service, deadline| {
            service.read_object_metadata_range_or_fetch_remote_without_cache_with_deadline(
                object, offset, max_bytes, deadline,
            )
        })
    }

    fn read_object_metadata_range_or_fetch_remote_without_cache_with_deadline(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        deadline: Instant,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        // Preserve the same authoritative metadata for the remote range
        // request; this node may intentionally have no local object shard.
        let mut object = object.clone();
        self.apply_transient_remote_read_location_prunes(&mut object);
        let initial = self.read_authoritative_object_metadata_local_range_or_pending(
            &object, offset, max_bytes,
        )?;
        if !matches!(
            initial,
            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
        ) {
            return Ok(initial);
        }
        let mut request_ids = self
            .request_remote_object_range_read_for_metadata_without_cache(
                &object, offset, max_bytes,
            )?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if request_ids.is_empty() {
            return Ok(initial);
        }
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
            self.check_cancelled("remote metadata range read wait")?;
            let events = self.process_incoming()?;
            if !events.is_empty() {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            }
            for event in events {
                match event {
                    NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id,
                        object_id: event_object_id,
                        source_node_id,
                        result: DfsReadResult::RangeFound(data),
                    } if request_ids.contains(&request_id)
                        && event_object_id == object.object_id =>
                    {
                        if let Err(error) =
                            validate_complete_posix_range_content_hash(&object, &data)
                        {
                            self.mark_remote_read_location_missing(
                                &object.object_id,
                                &object.version,
                                &object.content_hash,
                                &source_node_id,
                            )?;
                            return Err(error.into());
                        }
                        return Ok(NetworkedDfsRangeReadOutcome::Found {
                            data,
                            freshness: NetworkedDfsReadFreshness::Current,
                        });
                    }
                    NetworkedDfsServiceEvent::RemoteReadCompleted { request_id, .. }
                        if request_ids.contains(&request_id) => {}
                    _ => {}
                }
            }
            self.apply_transient_remote_read_location_prunes(&mut object);
            let current = self.read_authoritative_object_metadata_local_range_or_pending(
                &object, offset, max_bytes,
            )?;
            if !matches!(
                current,
                NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            ) {
                return Ok(current);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(current);
            }
            let next_retry =
                self.next_pending_remote_range_read_retry_for_metadata(&object, offset, max_bytes);
            if next_retry.is_none_or(|retry_at| retry_at <= now) {
                let next_request_ids = self
                    .request_remote_object_range_read_for_metadata_without_cache(
                        &object, offset, max_bytes,
                    )?;
                if next_request_ids.is_empty() {
                    return Ok(current);
                }
                request_ids.extend(next_request_ids);
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            }
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            let until_retry = self
                .next_pending_remote_range_read_retry_for_metadata(&object, offset, max_bytes)
                .map(|retry_at| retry_at.saturating_duration_since(now))
                .unwrap_or(remaining);
            let wait = idle_wait
                .min(remaining)
                .min(until_retry)
                .max(POSIX_REMOTE_READ_IDLE_WAIT_INITIAL.min(remaining));
            if self
                .transport
                .wait_for_incoming(wait)
                .map_err(DfsError::from_network)?
            {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            } else {
                idle_wait = idle_wait
                    .saturating_mul(2)
                    .min(POSIX_REMOTE_READ_IDLE_WAIT_MAX);
            }
        }
    }

    fn read_authoritative_object_metadata_local_range_or_pending(
        &self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        if !object.current_locations.contains(&self.node_id) && object.content_hash.is_some() {
            return Ok(NetworkedDfsRangeReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            });
        }
        match self.read_object_metadata_local_range_with_state(object, offset, max_bytes, false) {
            Ok(outcome) => Ok(outcome),
            Err(DfsError::UnknownObject { object_id })
                if object_id == object.object_id && object.content_hash.is_some() =>
            {
                // Routed namespace lookup supplied the authoritative object,
                // but this node intentionally has no local metadata shard for
                // it. Preserve that authority and proceed to the hinted remote
                // range request instead of treating local absence as a global
                // object miss.
                Ok(NetworkedDfsRangeReadOutcome::PendingReplication {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: object.content_hash.clone(),
                    current_locations: object.current_locations.clone(),
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn read_object_metadata_local_if_current_location(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<NetworkedDfsReadOutcome> {
        if !object.current_locations.contains(&self.node_id) && object.content_hash.is_some() {
            return Ok(NetworkedDfsReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            });
        }
        Ok(self.read_object_metadata_local_content_with_state(object, false)?)
    }

    pub fn posix_request_remote_read(&mut self, path: &str) -> Result<Vec<NodeId>> {
        self.posix_request_remote_read_with_follow(path, true)
    }

    pub fn posix_request_remote_read_no_follow(&mut self, path: &str) -> Result<Vec<NodeId>> {
        self.posix_request_remote_read_with_follow(path, false)
    }

    fn posix_request_remote_read_with_follow(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<Vec<NodeId>> {
        let (path, object) = self.local_posix_file_content_object(path, follow_final)?;
        match self.read_object_metadata_local_content_with_state(&object, false)? {
            NetworkedDfsReadOutcome::Found { .. } => Ok(Vec::new()),
            NetworkedDfsReadOutcome::PendingReplication { .. } => self
                .request_remote_object_read(&object.object_id)
                .map_err(DfsPosixError::from),
            NetworkedDfsReadOutcome::NotConfirmed { .. } | NetworkedDfsReadOutcome::NotFound => {
                Err(DfsPosixError::NotFound { path })
            }
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }
}

#[cfg(test)]
mod opened_stat_tests {
    use super::*;
    use crate::{DfsRoutedMessage, DfsTransportIdentity, LocalContentStore};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::network::NetworkError;

    #[derive(Clone, Debug)]
    struct NoNetworkTransport {
        local_node_id: NodeId,
    }

    impl DfsTransportIdentity for NoNetworkTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for NoNetworkTransport {
        fn send_routed_message(
            &mut self,
            target_node_id: &NodeId,
            _message: DfsRoutedMessage,
        ) -> std::result::Result<(), NetworkError> {
            Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
        }

        fn drain_incoming_messages(
            &mut self,
        ) -> std::result::Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn opened_stat_read_keeps_exact_version_without_resolving_current_path_metadata() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-opened-stat-read");
        let local = NodeId::from_legacy_named("node_opened_stat_read").expect("node id");
        let object_id =
            DfsObjectId::from_legacy_named("object_opened_stat_read").expect("object id");
        let mut service = NetworkedDfsService::new(
            NoNetworkTransport {
                local_node_id: local.clone(),
            },
            LocalContentStore::new(root.join("content")).expect("content store"),
        );
        let mut object = DfsObjectMetadata {
            object_id,
            file_system_id: sifs_file_system_id(),
            path: "/opened.txt".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: local.clone(),
            current_locations: vec![local],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("version_opened_stat_1").expect("first version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        service
            .put_local_object_version(object.clone(), b"version-one")
            .expect("publish first version");
        let opened = service.posix_stat("/opened.txt").expect("opened stat");

        object.version =
            DfsVersion::from_legacy_named("version_opened_stat_2").expect("second version");
        service
            .put_local_object_version(object, b"version-two")
            .expect("publish second version");
        assert_ne!(
            service
                .posix_stat("/opened.txt")
                .expect("current stat")
                .version,
            opened.version
        );

        assert_eq!(
            service
                .posix_read_file_range_for_opened_stat(
                    "/renamed-or-unlinked-after-open.txt",
                    &opened,
                    0,
                    usize::MAX,
                )
                .expect("read exact opened version"),
            b"version-one"
        );
    }
}
