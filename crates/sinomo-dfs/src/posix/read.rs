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
    DfsPosixError, DfsPosixFileRangeReadStream, DfsPosixFileReadStream, Result, read_conflict_error,
};
use crate::posix_path::normalize_posix_path;
use crate::{
    DfsBulkReadItem, DfsBulkReadItemResponse, DfsBulkReadRequest, DfsError, DfsMessageTransport,
    DfsNetworkMessage, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsReadData,
    DfsReadRangeData, DfsReadResult, DfsVersion, NetworkedDfsRangeReadOutcome,
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

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
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
        let (path, object) = self.local_posix_file_content_object(path, true)?;
        match self.read_object_metadata_local_content_with_state(&object, false)? {
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
    }

    pub fn posix_read_file_range(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        let (path, object) = self.local_posix_file_content_object(path, true)?;
        match self.read_object_metadata_local_range_with_state(&object, offset, max_bytes, false)? {
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
        match self
            .open_local_object_metadata_range_stream_with_state(&object, offset, max_bytes, 0)?
        {
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

    pub fn posix_read_files_distributed(&mut self, paths: &[String]) -> Vec<Result<Vec<u8>>> {
        if paths.len() <= 1 {
            return paths
                .iter()
                .map(|path| self.posix_read_file_distributed(path))
                .collect();
        }

        let mut results = (0..paths.len()).map(|_| None).collect::<Vec<_>>();
        let mut grouped = BTreeMap::<NodeId, Vec<PosixPendingBulkRead>>::new();

        self.prefetch_sifs_paths_for_bulk_read(paths);

        for (index, input_path) in paths.iter().enumerate() {
            match self.prepare_posix_bulk_read(input_path, index) {
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

    fn prefetch_sifs_paths_for_bulk_read(&mut self, paths: &[String]) {
        if paths.len() <= 1 {
            return;
        }
        let trace_started = Instant::now();
        let mut grouped = BTreeMap::<NodeId, Vec<String>>::new();
        let mut seen = BTreeSet::<(NodeId, String)>::new();
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
            return;
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
                    let _ = self.metadata.add_object(object);
                }
            }
        }
        crate::dfs_perf_trace(format_args!(
            "bulk-read-prefetch paths={} objects={} elapsed_ms={}",
            requested_paths,
            returned_objects,
            trace_started.elapsed().as_millis()
        ));
    }

    fn prepare_posix_bulk_read(
        &mut self,
        input_path: &str,
        index: usize,
    ) -> Result<PosixBulkReadPlan> {
        let (path, object) = self.distributed_posix_file_content_object(input_path, true)?;
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

        let deadline = Instant::now() + POSIX_REMOTE_READ_TIMEOUT;
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
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

    pub(super) fn read_current_object_or_fetch_remote(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<NetworkedDfsReadOutcome> {
        let initial = self.read_local_object_with_state(object_id, false)?;
        if !matches!(initial, NetworkedDfsReadOutcome::PendingReplication { .. }) {
            return Ok(initial);
        }
        let requested = self.request_remote_object_read(object_id)?;
        if requested.is_empty() {
            return Ok(initial);
        }
        let deadline = Instant::now() + POSIX_REMOTE_READ_TIMEOUT;
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
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
            let wait = idle_wait.min(remaining).min(until_retry);
            if wait.is_zero() {
                continue;
            }
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
        let mut object = self.current_read_object_metadata_or_path_object(object)?;
        let initial = self.read_object_metadata_local_if_current_location(&object)?;
        if !matches!(initial, NetworkedDfsReadOutcome::PendingReplication { .. }) {
            return Ok(initial);
        }
        let requested = self.request_remote_object_read_for_metadata_without_cache(&object)?;
        if requested.is_empty() {
            return Ok(initial);
        }
        let deadline = Instant::now() + POSIX_REMOTE_READ_TIMEOUT;
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
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
            object = self.current_read_object_metadata_or_path_object(&object)?;
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
            let wait = idle_wait.min(remaining).min(until_retry);
            if wait.is_zero() {
                continue;
            }
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

    fn current_read_object_metadata_or_path_object(
        &self,
        path_object: &DfsObjectMetadata,
    ) -> Result<DfsObjectMetadata> {
        let memory_object = self.metadata.object(&path_object.object_id).cloned();
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(&path_object.object_id)?
        } else {
            None
        };
        let refreshed = match (memory_object.as_ref(), backend_object.as_ref()) {
            (Some(memory_object), Some(backend_object)) => {
                Some(self.resolve_authoritative_object_metadata(
                    &path_object.object_id,
                    Some(memory_object),
                    Some(backend_object),
                )?)
            }
            (Some(memory_object), None) => Some(memory_object.clone()),
            (None, Some(backend_object)) => Some(backend_object.clone()),
            (None, None) => None,
        };
        let mut object = match refreshed {
            Some(refreshed)
                if refreshed.version == path_object.version
                    && refreshed.content_hash == path_object.content_hash =>
            {
                refreshed
            }
            _ => path_object.clone(),
        };
        object = self.object_metadata_with_receipt_locations(object)?;
        self.apply_transient_remote_read_location_prunes(&mut object);
        Ok(object)
    }

    fn read_object_metadata_local_if_current_location(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<NetworkedDfsReadOutcome> {
        let local = self.read_object_metadata_local_content_with_state(object, false)?;
        if matches!(
            local,
            NetworkedDfsReadOutcome::Found { .. } | NetworkedDfsReadOutcome::Conflicted { .. }
        ) {
            return Ok(local);
        }
        if object.current_locations.contains(&self.node_id) {
            return Ok(local);
        }
        if object.content_hash.is_some() {
            return Ok(NetworkedDfsReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            });
        }
        Ok(NetworkedDfsReadOutcome::NotFound)
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
