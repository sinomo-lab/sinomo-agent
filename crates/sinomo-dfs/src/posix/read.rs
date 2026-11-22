//! POSIX read and read-through helpers.
//!
//! This module owns local POSIX reads, distributed read-through, remote read
//! requests, stream/range read adapters, and bulk-read batching for the POSIX
//! facade. Write paths may reuse the read-through helper when they need the
//! current file bytes before applying a range patch.

use super::{POSIX_REMOTE_READ_IDLE_WAIT_INITIAL, POSIX_REMOTE_READ_IDLE_WAIT_MAX, now_unix_nanos};
use crate::namespace::next_version;
use crate::posix_model::{
    DfsPosixDistributedFilePrefixReadOutcome, DfsPosixDistributedFileReadOutcome,
    DfsPosixDistributedOpenedFileRangeReadOutcome, DfsPosixError, DfsPosixFileRangeReadStream,
    DfsPosixFileReadStream, DfsStat, Result,
};
use crate::posix_path::{normalize_posix_path, path_after_dot_resolution};
use crate::{
    DfsBulkReadItem, DfsBulkReadItemResponse, DfsBulkReadRequest, DfsDirectReadClientResult,
    DfsError, DfsMessageTransport, DfsMetadataShardConfig, DfsNetworkMessage, DfsNodeTransport,
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsPosixFilePrefixRead,
    DfsPosixRemoteFileRangeReadStream, DfsPosixRemoteFileReadStream, DfsReadData, DfsReadRangeData,
    DfsReadResult, DfsVersion, NetworkedDfsRangeReadOutcome, NetworkedDfsRangeReadStreamOutcome,
    NetworkedDfsReadFreshness, NetworkedDfsReadOutcome, NetworkedDfsReadStreamOutcome,
    NetworkedDfsService, NetworkedDfsServiceEvent, sifs_file_system_id,
};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum RangeReadContext {
    Namespace,
    Immutable,
}

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
    Immediate(DfsPosixDistributedFileReadOutcome),
    Remote {
        source: NodeId,
        pending: PosixPendingBulkRead,
    },
    Fallback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DistributedPosixFileContentObjectOutcome {
    Found {
        path: String,
        object: DfsObjectMetadata,
    },
    NotFound {
        path: String,
    },
    ContentUnavailable {
        path: String,
        detail: String,
    },
}

fn distributed_file_content_outcome(
    path: String,
    object: &DfsObjectMetadata,
    outcome: NetworkedDfsReadOutcome,
) -> Result<DfsPosixDistributedFileReadOutcome> {
    match outcome {
        NetworkedDfsReadOutcome::Found {
            data: DfsReadData { bytes, .. },
            ..
        } => Ok(DfsPosixDistributedFileReadOutcome::Found { bytes }),
        NetworkedDfsReadOutcome::PendingReplication {
            object_id,
            version,
            current_locations,
            ..
        } => Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
            path,
            detail: format!(
                "content for object {object_id} version {version} is pending replication; current locations: {current_locations:?}"
            ),
        }),
        NetworkedDfsReadOutcome::NotConfirmed {
            object_id,
            version,
            confirmed_locations,
            required_locations,
        } => Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
            path,
            detail: format!(
                "content for object {object_id} version {version} is not confirmed; confirmed locations: {confirmed_locations:?}; required locations: {required_locations:?}"
            ),
        }),
        NetworkedDfsReadOutcome::NotFound => {
            Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                path,
                detail: format!(
                    "authoritative metadata exists for object {} version {}, but its content is unavailable from every eligible replica",
                    object.object_id, object.version
                ),
            })
        }
    }
}

fn distributed_file_read_result(outcome: DfsPosixDistributedFileReadOutcome) -> Result<Vec<u8>> {
    match outcome {
        DfsPosixDistributedFileReadOutcome::Found { bytes } => Ok(bytes),
        DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            Err(DfsPosixError::NotFound { path })
        }
        DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => {
            Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("content for {path} is unavailable: {detail}"),
            }))
        }
    }
}

fn distributed_file_range_content_outcome(
    path: String,
    object: &DfsObjectMetadata,
    outcome: NetworkedDfsRangeReadOutcome,
) -> Result<DfsPosixDistributedFileReadOutcome> {
    match outcome {
        NetworkedDfsRangeReadOutcome::Found { data, .. } => {
            validate_posix_range_content_identity(object, &data)?;
            Ok(DfsPosixDistributedFileReadOutcome::Found { bytes: data.bytes })
        }
        NetworkedDfsRangeReadOutcome::PendingReplication {
            object_id,
            version,
            current_locations,
            ..
        } => Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
            path,
            detail: format!(
                "content for object {object_id} version {version} is pending replication; current locations: {current_locations:?}"
            ),
        }),
        NetworkedDfsRangeReadOutcome::NotConfirmed {
            object_id,
            version,
            confirmed_locations,
            required_locations,
        } => Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
            path,
            detail: format!(
                "content for object {object_id} version {version} is not confirmed; confirmed locations: {confirmed_locations:?}; required locations: {required_locations:?}"
            ),
        }),
        NetworkedDfsRangeReadOutcome::NotFound => {
            Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                path,
                detail: format!(
                    "authoritative metadata exists for object {} version {}, but its content is unavailable from every eligible replica",
                    object.object_id, object.version
                ),
            })
        }
    }
}

fn distributed_file_prefix_content_outcome(
    path: String,
    object: &DfsObjectMetadata,
    outcome: NetworkedDfsRangeReadOutcome,
    max_bytes: usize,
) -> Result<DfsPosixDistributedFilePrefixReadOutcome> {
    match outcome {
        NetworkedDfsRangeReadOutcome::Found { data, .. } => {
            let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
            validate_posix_range_content_identity(object, &data)?;
            Ok(DfsPosixDistributedFilePrefixReadOutcome::Found {
                read: DfsPosixFilePrefixRead {
                    bytes: data.bytes,
                    truncated: data.total_size_bytes > max_bytes_u64,
                },
            })
        }
        NetworkedDfsRangeReadOutcome::PendingReplication {
            object_id,
            version,
            current_locations,
            ..
        } => Ok(
            DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
                path,
                detail: format!(
                    "content for object {object_id} version {version} is pending replication; current locations: {current_locations:?}"
                ),
            },
        ),
        NetworkedDfsRangeReadOutcome::NotConfirmed {
            object_id,
            version,
            confirmed_locations,
            required_locations,
        } => Ok(
            DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
                path,
                detail: format!(
                    "content for object {object_id} version {version} is not confirmed; confirmed locations: {confirmed_locations:?}; required locations: {required_locations:?}"
                ),
            },
        ),
        NetworkedDfsRangeReadOutcome::NotFound => Ok(
            DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
                path,
                detail: format!(
                    "authoritative metadata exists for object {} version {}, but its content is unavailable from every eligible replica",
                    object.object_id, object.version
                ),
            },
        ),
    }
}

fn distributed_opened_file_range_read_result(
    outcome: DfsPosixDistributedOpenedFileRangeReadOutcome,
) -> Result<(DfsStat, DfsPosixFilePrefixRead)> {
    match outcome {
        DfsPosixDistributedOpenedFileRangeReadOutcome::Found { stat, read } => Ok((stat, read)),
        DfsPosixDistributedOpenedFileRangeReadOutcome::NotFound { path } => {
            Err(DfsPosixError::NotFound { path })
        }
        DfsPosixDistributedOpenedFileRangeReadOutcome::ContentUnavailable { path, detail } => {
            Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("content for {path} is unavailable: {detail}"),
            }))
        }
    }
}

fn distributed_file_prefix_read_result(
    outcome: DfsPosixDistributedFilePrefixReadOutcome,
) -> Result<DfsPosixFilePrefixRead> {
    match outcome {
        DfsPosixDistributedFilePrefixReadOutcome::Found { read } => Ok(read),
        DfsPosixDistributedFilePrefixReadOutcome::NotFound { path } => {
            Err(DfsPosixError::NotFound { path })
        }
        DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable { path, detail } => {
            Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("content for {path} is unavailable: {detail}"),
            }))
        }
    }
}

fn validate_posix_range_content_identity(
    object: &DfsObjectMetadata,
    data: &DfsReadRangeData,
) -> std::result::Result<(), DfsError> {
    // Reads validate the selected immutable identity and length. Scrub verifies
    // stored bytes; rehashing complete reads would make full and ranged reads
    // have different integrity and throughput behavior.
    if data.content_hash != object.content_hash {
        return Err(DfsError::ContentHashMismatch {
            expected: object
                .content_hash
                .clone()
                .unwrap_or_else(|| "none".to_string()),
            actual: data
                .content_hash
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        });
    }
    if object
        .content_size_bytes
        .is_some_and(|size| size != data.total_size_bytes)
        || data.bytes.len() as u64 > data.total_size_bytes.saturating_sub(data.offset)
    {
        return Err(DfsError::Distributed {
            message: "read range does not match committed content size".to_string(),
        });
    }
    Ok(())
}

fn validate_inline_namespace_range(
    object: &DfsObjectMetadata,
    data: &DfsReadRangeData,
    requested_offset: u64,
    requested_max_bytes: usize,
) -> std::result::Result<(), DfsError> {
    if data.object_id != object.object_id
        || data.version != object.version
        || data.content_hash != object.content_hash
    {
        return Err(DfsError::Distributed {
            message: format!(
                "authoritative namespace range identity mismatch for object {} version {}",
                object.object_id, object.version
            ),
        });
    }
    let Some(expected_size) = object.content_size_bytes else {
        return Err(DfsError::Distributed {
            message: format!(
                "authoritative namespace range for object {} lacks metadata content size",
                object.object_id
            ),
        });
    };
    let expected_offset = requested_offset.min(expected_size);
    let expected_bytes = expected_size
        .saturating_sub(expected_offset)
        .min(u64::try_from(requested_max_bytes).unwrap_or(u64::MAX));
    if data.total_size_bytes != expected_size
        || data.offset != expected_offset
        || u64::try_from(data.bytes.len()).unwrap_or(u64::MAX) != expected_bytes
    {
        return Err(DfsError::Distributed {
            message: format!(
                "authoritative namespace range shape mismatch for object {}: expected offset {expected_offset}, size {expected_size}, bytes {expected_bytes}; actual offset {}, size {}, bytes {}",
                object.object_id,
                data.offset,
                data.total_size_bytes,
                data.bytes.len()
            ),
        });
    }
    validate_posix_range_content_identity(object, data)
}

/// How long one pass of a remote content read waits for incoming work: its
/// idle interval, ended sooner by a due retry or the caller's deadline, and
/// never shorter than the initial idle interval while time remains.
fn posix_remote_read_wait(
    idle_wait: Duration,
    until_retry: Option<Duration>,
    deadline: Option<Instant>,
    now: Instant,
) -> Duration {
    let remaining = deadline.map(|deadline| deadline.saturating_duration_since(now));
    let floor = remaining.map_or(POSIX_REMOTE_READ_IDLE_WAIT_INITIAL, |remaining| {
        POSIX_REMOTE_READ_IDLE_WAIT_INITIAL.min(remaining)
    });
    [remaining, until_retry]
        .into_iter()
        .flatten()
        .fold(idle_wait, Duration::min)
        .max(floor)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Run one content read that may fetch from a remote replica. The read
    /// adds no deadline of its own: `operation` receives the deadline its
    /// caller installed, if any. Without one the read ends on its answer, on
    /// a definite outcome, when no source is left to ask, or on cancellation.
    fn with_posix_remote_read_deadline<R>(
        &mut self,
        operation: impl FnOnce(&mut Self, Option<Instant>) -> Result<R>,
    ) -> Result<R> {
        let deadline = self.transport.operation_deadline();
        operation(self, deadline)
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

    fn posix_file_content_object_candidate_outcome(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
    ) -> Result<DistributedPosixFileContentObjectOutcome> {
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(DistributedPosixFileContentObjectOutcome::NotFound { path });
        }
        let candidate_kind = object.kind.clone();
        let candidate_object_id = object.object_id.clone();
        let candidate_target_id = object.link_target_object_id.clone();
        let object = match self.resolve_hardlink_target_distributed(&object, None) {
            Ok(object) => object,
            Err(DfsPosixError::NotFound { .. }) if candidate_kind == DfsObjectKind::Hardlink => {
                return Ok(
                    DistributedPosixFileContentObjectOutcome::ContentUnavailable {
                        path,
                        detail: format!(
                            "hardlink object {candidate_object_id} references unavailable target {}",
                            candidate_target_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "<missing target id>".to_string())
                        ),
                    },
                );
            }
            Err(error) => return Err(error),
        };
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        Ok(DistributedPosixFileContentObjectOutcome::Found { path, object })
    }

    fn distributed_posix_file_content_object_outcome(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<DistributedPosixFileContentObjectOutcome> {
        let lookup = self.lookup_sifs_path_distributed_synced(path, follow_final);
        self.distributed_posix_file_content_object_outcome_from_lookup(lookup)
    }

    fn distributed_posix_file_content_object_outcome_from_snapshot(
        &mut self,
        path: &str,
        follow_final: bool,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<DistributedPosixFileContentObjectOutcome> {
        let lookup = self.lookup_sifs_path_distributed_synced_from_snapshot(
            path,
            follow_final,
            shard_snapshot,
        );
        self.distributed_posix_file_content_object_outcome_from_lookup(lookup)
    }

    fn distributed_posix_file_content_object_outcome_from_lookup(
        &mut self,
        lookup: Result<(String, Option<DfsObjectMetadata>)>,
    ) -> Result<DistributedPosixFileContentObjectOutcome> {
        let (path, object) = match lookup {
            Ok(lookup) => lookup,
            Err(DfsPosixError::NotFound { path }) => {
                // Path resolution follows hardlinks before returning the
                // terminal dentry. Probe the exact failing path only on this
                // error path so a present hardlink whose target metadata is
                // unavailable cannot be mistaken for namespace absence. Raw
                // path absence, tombstones, and absent symlink targets still
                // have no live hardlink at the failing path and remain
                // authoritative NotFound outcomes.
                let candidate = self.lookup_sifs_path_direct_distributed_synced(&path)?;
                if let Some(candidate) = candidate
                    && candidate.kind == DfsObjectKind::Hardlink
                {
                    return self.posix_file_content_object_candidate_outcome(path, candidate);
                }
                return Ok(DistributedPosixFileContentObjectOutcome::NotFound { path });
            }
            Err(error) => return Err(error),
        };
        let Some(object) = object else {
            return Ok(DistributedPosixFileContentObjectOutcome::NotFound { path });
        };
        self.posix_file_content_object_candidate_outcome(path, object)
    }

    fn distributed_posix_file_content_object_with_inline_range_outcome(
        &mut self,
        path: &str,
        follow_final: bool,
        offset: u64,
        max_bytes: usize,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<(
        DistributedPosixFileContentObjectOutcome,
        Option<DfsReadRangeData>,
    )> {
        let (path, object, inline_range) = match self
            .lookup_sifs_path_distributed_synced_with_inline_range_from_snapshot(
                path,
                follow_final,
                offset,
                max_bytes,
                shard_snapshot,
            ) {
            Ok(lookup) => lookup,
            Err(DfsPosixError::NotFound { path }) => {
                let candidate = self.lookup_sifs_path_direct_distributed_synced(&path)?;
                if let Some(candidate) = candidate
                    && candidate.kind == DfsObjectKind::Hardlink
                {
                    return self
                        .posix_file_content_object_candidate_outcome(path, candidate)
                        .map(|outcome| (outcome, None));
                }
                return Ok((
                    DistributedPosixFileContentObjectOutcome::NotFound { path },
                    None,
                ));
            }
            Err(error) => return Err(error),
        };
        let Some(object) = object else {
            return Ok((
                DistributedPosixFileContentObjectOutcome::NotFound { path },
                None,
            ));
        };
        self.posix_file_content_object_candidate_outcome(path, object)
            .map(|outcome| (outcome, inline_range))
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
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        // A hardlink's target may live only at its authority.
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
        self.posix_file_content_object(path, object)
    }

    pub fn posix_read_file(&self, path: &str) -> Result<Vec<u8>> {
        self.ensure_local_view_readable(path)?;
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
            }
        })
    }

    pub fn posix_read_file_range(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.ensure_local_view_readable(path)?;
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
            }
        })
    }

    pub fn posix_open_file_stream(&self, path: &str) -> Result<DfsPosixFileReadStream> {
        self.ensure_local_view_readable(path)?;
        let (path, object) = self.local_posix_file_content_object(path, true)?;
        self.open_local_posix_file_stream_for_object(path, object)
    }

    /// Resolve the current distributed namespace object before opening its
    /// exact version from the local content store.
    ///
    /// Unlike [`Self::posix_open_file_stream`], this does not trust a live
    /// service shell's local metadata hit: a different writer may have advanced
    /// the path at its metadata-shard primary. Callers may fall back to the
    /// remote stream API when the current exact version is not present locally.
    pub fn posix_open_current_file_stream(&mut self, path: &str) -> Result<DfsPosixFileReadStream> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.check_cancelled("distributed local stream read")?;
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        self.open_local_posix_file_stream_for_object(path, object)
    }

    fn open_local_posix_file_stream_for_object(
        &self,
        path: String,
        object: DfsObjectMetadata,
    ) -> Result<DfsPosixFileReadStream> {
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
        }
    }

    pub fn posix_open_file_range_stream(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsPosixFileRangeReadStream> {
        self.ensure_local_view_readable(path)?;
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
        }
    }

    pub fn posix_read_file_distributed(&mut self, path: &str) -> Result<Vec<u8>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        distributed_file_read_result(self.posix_read_file_distributed_outcome(path)?)
    }

    /// Read a distributed file without collapsing content availability into
    /// namespace absence.
    ///
    /// The metadata lookup establishes whether the path exists. Once it has
    /// returned an exact object/version, every non-conflict content miss is an
    /// availability failure, including a replica-layer `NotFound`: it cannot
    /// revoke the already confirmed namespace object.
    pub fn posix_read_file_distributed_outcome(
        &mut self,
        path: &str,
    ) -> Result<DfsPosixDistributedFileReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let requested = self.resolve_posix_dot_components_distributed(path)?;
        // Keep a trailing slash: it requires the target to be a directory.
        let lookup = path_after_dot_resolution(path, &requested);
        self.with_local_primary_metadata_read_snapshot(&requested, |service, shard_snapshot| {
            service.posix_read_file_distributed_outcome_from_snapshot(&lookup, shard_snapshot)
        })
    }

    fn posix_read_file_distributed_outcome_from_snapshot(
        &mut self,
        path: &str,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<DfsPosixDistributedFileReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.check_cancelled("distributed read")?;
        let trace_started = Instant::now();
        let lookup_started = Instant::now();
        let (path, object) = match self
            .distributed_posix_file_content_object_outcome_from_snapshot(
                path,
                true,
                shard_snapshot,
            )? {
            DistributedPosixFileContentObjectOutcome::Found { path, object } => (path, object),
            DistributedPosixFileContentObjectOutcome::NotFound { path } => {
                return Ok(DfsPosixDistributedFileReadOutcome::NotFound { path });
            }
            DistributedPosixFileContentObjectOutcome::ContentUnavailable { path, detail } => {
                return Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail });
            }
        };
        let lookup_elapsed_ms = lookup_started.elapsed().as_millis();
        let fetch_started = Instant::now();
        let outcome = self.read_object_metadata_or_fetch_remote_without_cache(&object)?;
        let fetch_elapsed_ms = fetch_started.elapsed().as_millis();
        let outcome = distributed_file_content_outcome(path.clone(), &object, outcome)?;
        if let DfsPosixDistributedFileReadOutcome::Found { bytes } = &outcome {
            crate::dfs_perf_trace(format_args!(
                "posix-read-distributed path={} object={} bytes={} lookup_ms={} fetch_ms={} total_ms={}",
                path,
                object.object_id,
                bytes.len(),
                lookup_elapsed_ms,
                fetch_elapsed_ms,
                trace_started.elapsed().as_millis()
            ));
        }
        Ok(outcome)
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let outcome = self.posix_read_file_prefix_distributed_outcome(path, max_bytes)?;
        distributed_file_prefix_read_result(outcome)
    }

    /// Read a bounded distributed file prefix without collapsing content
    /// availability into namespace absence.
    pub fn posix_read_file_prefix_distributed_outcome(
        &mut self,
        path: &str,
        max_bytes: usize,
    ) -> Result<DfsPosixDistributedFilePrefixReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let requested = self.resolve_posix_dot_components_distributed(path)?;
        let lookup = path_after_dot_resolution(path, &requested);
        self.with_local_primary_metadata_read_snapshot(&requested, |service, shard_snapshot| {
            service.posix_read_file_prefix_distributed_outcome_from_snapshot(
                &lookup,
                max_bytes,
                shard_snapshot,
            )
        })
    }

    fn posix_read_file_prefix_distributed_outcome_from_snapshot(
        &mut self,
        path: &str,
        max_bytes: usize,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<DfsPosixDistributedFilePrefixReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.check_cancelled("distributed prefix read")?;
        let (lookup, inline_range) = self
            .distributed_posix_file_content_object_with_inline_range_outcome(
                path,
                true,
                0,
                max_bytes,
                shard_snapshot,
            )?;
        let (path, object) = match lookup {
            DistributedPosixFileContentObjectOutcome::Found { path, object } => (path, object),
            DistributedPosixFileContentObjectOutcome::NotFound { path } => {
                return Ok(DfsPosixDistributedFilePrefixReadOutcome::NotFound { path });
            }
            DistributedPosixFileContentObjectOutcome::ContentUnavailable { path, detail } => {
                return Ok(
                    DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable { path, detail },
                );
            }
        };
        if let Some(data) = inline_range {
            validate_inline_namespace_range(&object, &data, 0, max_bytes)?;
            return distributed_file_prefix_content_outcome(
                path,
                &object,
                NetworkedDfsRangeReadOutcome::Found {
                    data,
                    freshness: NetworkedDfsReadFreshness::Current,
                },
                max_bytes,
            );
        }
        let outcome =
            self.read_object_metadata_range_or_fetch_remote_without_cache(&object, 0, max_bytes)?;
        distributed_file_prefix_content_outcome(path, &object, outcome, max_bytes)
    }

    /// Read a bounded prefix through a no-link scoped open.
    ///
    /// The namespace walk returns an exact object/version identity and the
    /// content transfer consumes that identity directly. A concurrent rename
    /// or link replacement after the open therefore cannot redirect the read
    /// outside `root`.
    pub fn posix_read_file_prefix_beneath_no_links_distributed(
        &mut self,
        root: &str,
        path: &str,
        max_bytes: usize,
    ) -> Result<(DfsStat, DfsPosixFilePrefixRead)> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        distributed_opened_file_range_read_result(
            self.posix_read_file_prefix_beneath_no_links_distributed_outcome(
                root, path, max_bytes,
            )?,
        )
    }

    /// Typed counterpart of
    /// [`Self::posix_read_file_prefix_beneath_no_links_distributed`].
    ///
    /// Preserve exact opened-file availability for callers making optional-read decisions.
    pub fn posix_read_file_prefix_beneath_no_links_distributed_outcome(
        &mut self,
        root: &str,
        path: &str,
        max_bytes: usize,
    ) -> Result<DfsPosixDistributedOpenedFileRangeReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_read_file_range_beneath_no_links_distributed_outcome(root, path, 0, max_bytes)
    }

    /// Read one bounded range through a no-link scoped open.
    ///
    /// The receipt identifies the exact immutable object/version consumed by
    /// the content transfer. `truncated` is true exactly when bytes remain
    /// after the returned range. Offsets beyond the exact opened size fail so
    /// callers cannot confuse an invalid continuation with EOF.
    pub fn posix_read_file_range_beneath_no_links_distributed(
        &mut self,
        root: &str,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(DfsStat, DfsPosixFilePrefixRead)> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        distributed_opened_file_range_read_result(
            self.posix_read_file_range_beneath_no_links_distributed_outcome(
                root, path, offset, max_bytes,
            )?,
        )
    }

    /// Typed counterpart of
    /// [`Self::posix_read_file_range_beneath_no_links_distributed`].
    pub fn posix_read_file_range_beneath_no_links_distributed_outcome(
        &mut self,
        root: &str,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsPosixDistributedOpenedFileRangeReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let opened = match self.posix_open_beneath_no_links_distributed(root, path) {
            Ok(opened) => opened,
            Err(DfsPosixError::NotFound { path }) => {
                return Ok(DfsPosixDistributedOpenedFileRangeReadOutcome::NotFound { path });
            }
            Err(error) => return Err(error),
        };
        if opened.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory {
                path: opened.path.clone(),
            });
        }
        let size_bytes = opened
            .size_bytes
            .ok_or_else(|| DfsPosixError::InvalidPath {
                path: opened.path.clone(),
            })?;
        if offset > size_bytes {
            return Err(DfsPosixError::InvalidPath {
                path: opened.path.clone(),
            });
        }
        match self.posix_read_file_range_for_opened_stat_outcome(
            &opened.path,
            &opened,
            offset,
            max_bytes,
        )? {
            DfsPosixDistributedFileReadOutcome::Found { bytes } => {
                let range_end =
                    offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                let truncated = size_bytes > range_end;
                Ok(DfsPosixDistributedOpenedFileRangeReadOutcome::Found {
                    stat: opened,
                    read: DfsPosixFilePrefixRead { bytes, truncated },
                })
            }
            DfsPosixDistributedFileReadOutcome::NotFound { path } => {
                Ok(DfsPosixDistributedOpenedFileRangeReadOutcome::NotFound { path })
            }
            DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => Ok(
                DfsPosixDistributedOpenedFileRangeReadOutcome::ContentUnavailable { path, detail },
            ),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.check_cancelled("distributed range read")?;
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        match self
            .read_object_metadata_range_or_fetch_remote_without_cache(&object, offset, max_bytes)?
        {
            NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                validate_posix_range_content_identity(&object, &data)?;
                Ok(data.bytes)
            }
            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
            | NetworkedDfsRangeReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
        }
    }

    pub fn posix_read_files_distributed(&mut self, paths: &[String]) -> Vec<Result<Vec<u8>>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_read_files_distributed_outcomes(paths)
            .into_iter()
            .map(|result| result.and_then(distributed_file_read_result))
            .collect()
    }

    /// Bulk-read distributed files without collapsing unavailable content into
    /// namespace absence.
    ///
    /// Metadata lookup and remote content transfer remain grouped exactly like
    /// [`Self::posix_read_files_distributed`]. Each item retains the typed
    /// distinction needed by optional-read service boundaries.
    pub fn posix_read_files_distributed_outcomes(
        &mut self,
        paths: &[String],
    ) -> Vec<Result<DfsPosixDistributedFileReadOutcome>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if paths.len() <= 1 {
            return paths
                .iter()
                .map(|path| self.posix_read_file_distributed_outcome(path))
                .collect();
        }

        let mut results = (0..paths.len()).map(|_| None).collect::<Vec<_>>();
        let mut grouped = BTreeMap::<NodeId, Vec<PosixPendingBulkRead>>::new();

        let prefetched_objects = self.prefetch_sifs_paths_for_bulk_read(paths);

        for (index, input_path) in paths.iter().enumerate() {
            match self.prepare_posix_bulk_read(input_path, index, &prefetched_objects) {
                Ok(PosixBulkReadPlan::Immediate(outcome)) => {
                    results[index] = Some(Ok(outcome));
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
                            Some(self.posix_read_file_distributed_outcome(&pending.path));
                    }
                }
                Err(_) => {
                    for pending in pending_reads {
                        results[pending.index] =
                            Some(self.posix_read_file_distributed_outcome(&pending.path));
                    }
                }
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.unwrap_or_else(|| self.posix_read_file_distributed_outcome(&paths[index]))
            })
            .collect()
    }

    fn prefetch_sifs_paths_for_bulk_read(
        &mut self,
        paths: &[String],
    ) -> BTreeMap<String, DfsObjectMetadata> {
        let Ok(Some(shards)) = self.refresh_sifs_metadata_shards() else {
            return BTreeMap::new();
        };
        // A plain path and its ancestors can be read together at their shard
        // owners. Share exact directory observations across sibling inputs.
        // Special paths retain the ordinary component and link resolver.
        let mut components = BTreeSet::new();
        for path in paths {
            if normalize_posix_path(path).ok().as_ref() != Some(path) || path == "/" {
                continue;
            }
            for (end, _) in path.match_indices('/').skip(1) {
                components.insert(path[..end].to_owned());
            }
            components.insert(path.clone());
        }
        let components: Vec<_> = components.into_iter().collect();
        let Ok(records) = self.prefetch_exact_path_components(&components, Some(&shards)) else {
            // A speculative batch cannot decide an error for an unrelated
            // input. Each read obtains its own authoritative outcome below.
            return BTreeMap::new();
        };
        let mut files = BTreeMap::new();
        for path in paths {
            let Some(Some(file)) = records.get(path) else {
                continue;
            };
            if !matches!(file.kind, DfsObjectKind::File | DfsObjectKind::Artifact) {
                continue;
            }
            let parents = path.match_indices('/').skip(1).map(|(end, _)| &path[..end]);
            if parents.clone().all(|parent| {
                records
                    .get(parent)
                    .and_then(Option::as_ref)
                    .is_some_and(|row| row.kind == DfsObjectKind::Directory)
            }) {
                for selected in parents.chain(std::iter::once(path.as_str())) {
                    self.namespace_execution.observe_path(
                        &file.file_system_id,
                        selected,
                        records[selected].as_ref(),
                    );
                }
                files.insert(path.clone(), file.clone());
            }
        }
        files
    }

    fn prepare_posix_bulk_read(
        &mut self,
        input_path: &str,
        index: usize,
        prefetched_objects: &BTreeMap<String, DfsObjectMetadata>,
    ) -> Result<PosixBulkReadPlan> {
        if let Some(file) = prefetched_objects.get(input_path) {
            return self.prepare_posix_bulk_read_for_object(
                input_path.to_owned(),
                file.clone(),
                index,
            );
        }
        let path = self.resolve_posix_dot_components_distributed(input_path)?;
        let lookup = path_after_dot_resolution(input_path, &path);
        let candidate =
            self.with_local_primary_metadata_read_snapshot(&path, |service, shards| {
                service.distributed_posix_file_content_object_outcome_from_snapshot(
                    &lookup, true, shards,
                )
            })?;
        match candidate {
            DistributedPosixFileContentObjectOutcome::Found { path, object } => {
                self.prepare_posix_bulk_read_for_object(path, object, index)
            }
            DistributedPosixFileContentObjectOutcome::NotFound { path } => Ok(
                PosixBulkReadPlan::Immediate(DfsPosixDistributedFileReadOutcome::NotFound { path }),
            ),
            DistributedPosixFileContentObjectOutcome::ContentUnavailable { path, detail } => {
                Ok(PosixBulkReadPlan::Immediate(
                    DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail },
                ))
            }
        }
    }

    fn prepare_posix_bulk_read_for_object(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
        index: usize,
    ) -> Result<PosixBulkReadPlan> {
        match self.read_authoritative_object_metadata_local_or_pending(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => {
                return Ok(PosixBulkReadPlan::Immediate(
                    DfsPosixDistributedFileReadOutcome::Found { bytes },
                ));
            }

            outcome @ NetworkedDfsReadOutcome::NotConfirmed { .. }
            | outcome @ NetworkedDfsReadOutcome::NotFound => {
                return distributed_file_content_outcome(path, &object, outcome)
                    .map(PosixBulkReadPlan::Immediate);
            }
            NetworkedDfsReadOutcome::PendingReplication { .. } => {}
        }

        let (Some(content_hash), Some(content_size_bytes)) =
            (object.content_hash.clone(), object.content_size_bytes)
        else {
            return Ok(PosixBulkReadPlan::Fallback);
        };
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
        deadline: Option<Instant>,
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
            if deadline.is_some_and(|deadline| now >= deadline) {
                break;
            }
            let wait = posix_remote_read_wait(idle_wait, None, deadline, now);
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
    ) -> Result<DfsPosixDistributedFileReadOutcome> {
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

                Ok(DfsPosixDistributedFileReadOutcome::Found { bytes: data.bytes })
            }
            DfsReadResult::RangeFound(_) => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("bulk read for {} returned a range result", pending.path),
            })),
            DfsReadResult::NotFound => Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                path: pending.path.clone(),
                detail: format!(
                    "authoritative metadata exists for object {} version {}, but bulk-read source returned no content",
                    pending.object_id, pending.version
                ),
            }),
            DfsReadResult::Rejected { reason } => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: reason,
            })),
        }
    }

    pub fn posix_read_file_update_access(&mut self, path: &str) -> Result<Vec<u8>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        match self.read_object_metadata_or_fetch_remote_without_cache(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => {
                if let Err(error) = self.record_posix_access(&object) {
                    // The content read above is already authoritative and
                    // complete. Access-time bookkeeping must never turn that
                    // success into a false data-access failure.
                    tracing::warn!(
                        target: "sinomo.dfs.posix",
                        path = %path,
                        error = %error,
                        "completed distributed read without access-time update"
                    );
                }
                Ok(bytes)
            }
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
        }
    }

    /// Read through a stable opened-object identity. A removed object fails
    /// instead of resolving the descriptor's old pathname to a replacement.
    /// Full open-unlinked lifetime is intentionally deferred; this method is
    /// the fail-closed boundary until orphan retention is implemented.
    pub fn posix_read_file_for_object(&mut self, object_id: &DfsObjectId) -> Result<Vec<u8>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let object = self
            .current_read_object_metadata_distributed(object_id)
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
                self.record_posix_access(&object)?;
                Ok(bytes)
            }
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_read_file_range_for_object_with_access_update(object_id, offset, max_bytes, true)
    }

    fn posix_read_file_range_for_object_with_access_update(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
        update_access: bool,
    ) -> Result<Vec<u8>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let object = self
            .current_read_object_metadata_distributed(object_id)
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        distributed_file_read_result(
            self.posix_read_file_range_for_opened_stat_outcome(path, opened, offset, max_bytes)?,
        )
    }

    /// Typed exact-content range read for an already opened metadata receipt.
    ///
    /// Guarded VFS reads use this outcome so a retained
    /// receipt with an unavailable immutable content version remains distinct
    /// from namespace absence.
    pub fn posix_read_file_range_for_opened_stat_outcome(
        &mut self,
        path: &str,
        opened: &DfsStat,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsPosixDistributedFileReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if max_bytes == 0 {
            return Ok(DfsPosixDistributedFileReadOutcome::Found { bytes: Vec::new() });
        }
        let path = self.resolve_posix_dot_components_distributed(path)?;
        if opened.kind == DfsObjectKind::Tombstone {
            return Ok(DfsPosixDistributedFileReadOutcome::NotFound { path });
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
            namespace_link_count: Some(opened.link_count),
            kind: opened.kind.clone(),
            tombstone_provenance: None,
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
        self.posix_read_file_range_for_opened_metadata_outcome(
            path, object, offset, max_bytes, false,
        )
    }

    fn posix_read_file_range_for_opened_metadata(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        update_access: bool,
    ) -> Result<Vec<u8>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        distributed_file_read_result(self.posix_read_file_range_for_opened_metadata_outcome(
            path,
            object,
            offset,
            max_bytes,
            update_access,
        )?)
    }

    fn posix_read_file_range_for_opened_metadata_outcome(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        update_access: bool,
    ) -> Result<DfsPosixDistributedFileReadOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let outcome = self
            .read_object_metadata_range_or_fetch_remote_without_cache(&object, offset, max_bytes)?;
        let read = distributed_file_range_content_outcome(path.clone(), &object, outcome)?;
        if matches!(read, DfsPosixDistributedFileReadOutcome::Found { .. }) && update_access {
            match self.record_posix_access(&object) {
                Ok(()) => {}
                Err(DfsPosixError::NotFound { .. }) => {
                    return Ok(DfsPosixDistributedFileReadOutcome::NotFound { path });
                }
                Err(error) => return Err(error),
            }
        }
        Ok(read)
    }

    /// Record a completed read's access time. Only the metadata authority
    /// journals atime; replicas and outside readers use noatime semantics. A
    /// caller delegating to its Main keeps no metadata store, so Main records it.
    pub(super) fn record_posix_access(&mut self, object: &DfsObjectMetadata) -> Result<()> {
        if !self.is_local_metadata_primary_for_object(object)? {
            return Ok(());
        }
        if self.transport.delegates_local_namespace_authority() && self.control_plane.is_some() {
            let main = self.node_id.clone();
            return self.route_namespace_operation(
                main,
                crate::DfsNamespaceOperation::RecordAccess {
                    path: object.path.clone(),
                    object_id: object.object_id.clone(),
                },
            );
        }
        self.record_posix_access_local(&object.object_id, &object.path)
    }

    pub(crate) fn record_posix_access_local(
        &mut self,
        object_id: &DfsObjectId,
        path: &str,
    ) -> Result<()> {
        let mut current = self
            .object_by_id(object_id)?
            .filter(|current| current.kind != DfsObjectKind::Tombstone)
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        current.accessed_at_unix_nanos = now_unix_nanos();
        current.version = next_version(&current.version);
        self.add_object_metadata(current)?;
        Ok(())
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
        deadline: Option<Instant>,
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
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Ok(current);
                }
                if self.request_remote_object_read(object_id)?.is_empty() {
                    return Ok(current);
                }
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
                continue;
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(current);
            }
            let until_retry = self
                .next_pending_remote_read_retry_for_current_object(object_id)?
                .map(|next_retry_at| next_retry_at.saturating_duration_since(now));
            // An already-due retry must still pace the loop: on a degraded link
            // every retry fails fast and stays due, and a zero wait here spun
            // this loop at a full core.
            let wait = posix_remote_read_wait(idle_wait, until_retry, deadline, now);
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
        deadline: Option<Instant>,
    ) -> Result<NetworkedDfsReadOutcome> {
        // `object` came from the authoritative routed namespace lookup.
        // No-cache reads deliberately reuse it instead of re-resolving through a
        // non-replica's incomplete local object shard.
        let mut object = object.clone();
        self.apply_transient_remote_read_location_prunes(&mut object);
        let initial = self.read_authoritative_object_metadata_local_or_pending(&object)?;
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
            let current = self.read_authoritative_object_metadata_local_or_pending(&object)?;
            if !matches!(current, NetworkedDfsReadOutcome::PendingReplication { .. }) {
                return Ok(current);
            }
            if !self.has_pending_remote_read_for_metadata(&object) {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
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
            if deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(current);
            }
            let until_retry = self
                .next_pending_remote_read_retry_for_metadata(&object)
                .map(|next_retry_at| next_retry_at.saturating_duration_since(now));
            // Same pacing floor as `read_current_object_or_fetch_remote`: an
            // always-due retry on a degraded link must not spin this loop hot.
            let wait = posix_remote_read_wait(idle_wait, until_retry, deadline, now);
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

    pub(crate) fn read_object_metadata_range_or_fetch_remote_without_cache(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        self.with_posix_remote_read_deadline(|service, deadline| {
            service.read_object_metadata_range_or_fetch_remote_without_cache_with_deadline(
                object,
                offset,
                max_bytes,
                deadline,
                RangeReadContext::Namespace,
            )
        })
    }

    /// The caller must resolve a current or explicitly retained descriptor from
    /// metadata authority before calling. Reading that immutable identity does
    /// select a namespace winner.
    pub(crate) fn read_immutable_object_metadata_range_or_fetch_remote_without_cache(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        self.with_posix_remote_read_deadline(|service, deadline| {
            service.read_object_metadata_range_or_fetch_remote_without_cache_with_deadline(
                object,
                offset,
                max_bytes,
                deadline,
                RangeReadContext::Immutable,
            )
        })
    }

    fn read_object_metadata_range_or_fetch_remote_without_cache_with_deadline(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        deadline: Option<Instant>,
        context: RangeReadContext,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        // Preserve the same authoritative metadata for the remote range
        // request; this node may intentionally have no local object shard.
        let mut object = object.clone();
        self.apply_transient_remote_read_location_prunes(&mut object);
        let initial = self.read_authoritative_object_metadata_local_range_or_pending(
            &object, offset, max_bytes, context,
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
                        if let Err(error) = validate_posix_range_content_identity(&object, &data) {
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
                &object, offset, max_bytes, context,
            )?;
            if !matches!(
                current,
                NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            ) {
                return Ok(current);
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
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
            let until_retry = self
                .next_pending_remote_range_read_retry_for_metadata(&object, offset, max_bytes)
                .map(|retry_at| retry_at.saturating_duration_since(now));
            let wait = posix_remote_read_wait(idle_wait, until_retry, deadline, now);
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
        context: RangeReadContext,
    ) -> Result<NetworkedDfsRangeReadOutcome> {
        // `current_locations` is a routing hint, not an authority check. A
        // metadata-only version change can race the receipt that adds this node
        // to the latest location set while the exact immutable content is
        // already local. Always try that content identity before going remote.
        let local = match context {
            RangeReadContext::Namespace => {
                self.read_object_metadata_local_range_with_state(object, offset, max_bytes, false)
            }
            RangeReadContext::Immutable => {
                self.read_object_metadata_local_range_contents(object, offset, max_bytes, false)
            }
        };
        match local {
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

    fn read_authoritative_object_metadata_local_or_pending(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<NetworkedDfsReadOutcome> {
        // The authoritative metadata's content identity is stronger evidence
        // than a lagging location projection. Exact matching local content
        // remains readable even when a metadata-only mutation advanced the
        // visible version before the replica receipt updated locations.
        Ok(self.read_object_metadata_local_content_with_state(object, false)?)
    }

    pub fn posix_request_remote_read(&mut self, path: &str) -> Result<Vec<NodeId>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_request_remote_read_with_follow(path, true)
    }

    pub fn posix_request_remote_read_no_follow(&mut self, path: &str) -> Result<Vec<NodeId>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_request_remote_read_with_follow(path, false)
    }

    fn posix_request_remote_read_with_follow(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<Vec<NodeId>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (path, object) = self.local_posix_file_content_object(path, follow_final)?;
        match self.read_object_metadata_local_content_with_state(&object, false)? {
            NetworkedDfsReadOutcome::Found { .. } => Ok(Vec::new()),
            NetworkedDfsReadOutcome::PendingReplication { .. } => self
                .request_remote_object_read(&object.object_id)
                .map_err(DfsPosixError::from),
            NetworkedDfsReadOutcome::NotConfirmed { .. } | NetworkedDfsReadOutcome::NotFound => {
                Err(DfsPosixError::NotFound { path })
            }
        }
    }
}

impl NetworkedDfsService<DfsNodeTransport> {
    /// Resolves current path metadata and opens the remote full-file body
    /// without materializing it into a `Vec`.
    ///
    /// Local reads continue to use [`Self::posix_open_file_stream`]. This
    /// method is the public POSIX boundary for a caller that needs a remote
    /// replica and wants ownership-bearing object-RPC chunks.
    pub async fn posix_open_remote_file_stream(
        &mut self,
        path: &str,
    ) -> Result<DfsPosixRemoteFileReadStream> {
        self.check_cancelled("distributed stream read")?;
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        let response = self
            .read_remote_object_metadata_stream(&object, None)
            .await?;
        match response.result {
            DfsDirectReadClientResult::Found(data) => Ok(DfsPosixRemoteFileReadStream {
                object_id: data.object_id,
                version: data.version,
                content_hash: data.content_hash,
                size_bytes: data.content_size_bytes,
                stream: data.stream,
            }),
            DfsDirectReadClientResult::NotFound => Err(DfsPosixError::NotFound { path }),
            DfsDirectReadClientResult::Rejected { reason } => {
                Err(DfsError::Distributed { message: reason }.into())
            }
            DfsDirectReadClientResult::RangeFound(_) => Err(DfsError::Serialization {
                message: "full POSIX remote read received a range response".to_string(),
            }
            .into()),
        }
    }

    /// Resolves current path metadata and opens a remote bounded range through
    /// the same stream-native object-RPC carrier as a full read.
    pub async fn posix_open_remote_file_range_stream(
        &mut self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsPosixRemoteFileRangeReadStream> {
        self.check_cancelled("distributed range stream read")?;
        let (path, object) = self.distributed_posix_file_content_object(path, true)?;
        let response = self
            .read_remote_object_metadata_stream(&object, Some((offset, max_bytes)))
            .await?;
        match response.result {
            DfsDirectReadClientResult::RangeFound(data) => Ok(DfsPosixRemoteFileRangeReadStream {
                object_id: data.object_id,
                version: data.version,
                content_hash: data.content_hash,
                offset: data.offset,
                total_size_bytes: data.total_size_bytes,
                range_size_bytes: data.range_size_bytes,
                stream: data.stream,
            }),
            DfsDirectReadClientResult::NotFound => Err(DfsPosixError::NotFound { path }),
            DfsDirectReadClientResult::Rejected { reason } => {
                Err(DfsError::Distributed { message: reason }.into())
            }
            DfsDirectReadClientResult::Found(_) => Err(DfsError::Serialization {
                message: "POSIX remote range read received a full-object response".to_string(),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod opened_stat_tests {
    use super::*;
    use crate::{DfsMetadataBackend as _, DfsWriteOptions};

    use sinomo_core::DfsObjectPolicy;

    #[test]
    fn distributed_content_outcome_keeps_replica_unavailability_distinct_from_path_absence() {
        let local = NodeId::from_legacy_named("node_typed_content_unavailable").expect("node id");
        let object_id =
            DfsObjectId::from_legacy_named("object_typed_content_unavailable").expect("object id");
        let version =
            DfsVersion::from_legacy_named("version_typed_content_unavailable").expect("version");
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: sifs_file_system_id(),
            path: "/catalog.sobj".to_string(),
            namespace_link_count: (DfsObjectKind::File).initial_namespace_link_count(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: local.clone(),
            current_locations: vec![local.clone()],
            content_hash: Some("blake3:catalog".to_string()),
            content_size_bytes: Some(7),
            link_target_object_id: None,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };

        for outcome in [
            NetworkedDfsReadOutcome::PendingReplication {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: vec![local.clone()],
            },
            NetworkedDfsReadOutcome::NotConfirmed {
                object_id: object_id.clone(),
                version: version.clone(),
                confirmed_locations: Vec::new(),
                required_locations: vec![local.clone()],
            },
            // A content-layer miss after authoritative metadata lookup is not
            // proof that the namespace path is absent.
            NetworkedDfsReadOutcome::NotFound,
        ] {
            assert!(matches!(
                distributed_file_content_outcome("/catalog.sobj".to_string(), &object, outcome,)
                    .expect("typed content outcome"),
                DfsPosixDistributedFileReadOutcome::ContentUnavailable { .. }
            ));
        }

        for outcome in [
            NetworkedDfsRangeReadOutcome::PendingReplication {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: vec![local.clone()],
            },
            NetworkedDfsRangeReadOutcome::NotConfirmed {
                object_id: object_id.clone(),
                version: version.clone(),
                confirmed_locations: Vec::new(),
                required_locations: vec![local.clone()],
            },
            NetworkedDfsRangeReadOutcome::NotFound,
        ] {
            assert!(matches!(
                distributed_file_prefix_content_outcome(
                    "/catalog.sobj".to_string(),
                    &object,
                    outcome,
                    4,
                )
                .expect("typed prefix content outcome"),
                DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable { .. }
            ));
        }

        let (native, mut service) = crate::test_support::DfsTestRuntime::start_single_node(
            "native-hardlink-content-unavailable",
            local.clone(),
        )
        .expect("normal initialized native owner");
        // The normal producer creates the real dentry and immutable payload.
        // One bounded disk-loss negative removes only its actual loose bytes.
        let bytes = vec![b'a'; 16 * 1024 + 1];
        let source = service
            .posix_write_file(
                "/catalog.sobj",
                &bytes,
                DfsWriteOptions::replicated_eventual(vec![local.clone()]),
            )
            .expect("normal source publication");
        service
            .posix_hardlink("/catalog.sobj", "/catalog-link.sobj")
            .expect("normal hardlink publication");
        // The owner's durable store; the client keeps no store of its own.
        let hardlink = native
            .node_metadata(&local)
            .unwrap()
            .get_object_by_path(&source.file_system_id, "/catalog-link.sobj")
            .unwrap()
            .unwrap();
        assert_eq!(hardlink.kind, DfsObjectKind::Hardlink);
        service
            .content()
            .debug_remove_loose_blob(source.content_hash.as_ref().unwrap())
            .expect("remove actual referenced loose payload");
        assert!(matches!(
            service
                .posix_read_file_distributed_outcome(&hardlink.path)
                .expect("read present hardlink with unavailable target"),
            DfsPosixDistributedFileReadOutcome::ContentUnavailable { .. }
        ));
        assert!(matches!(
            service.posix_read_file_distributed(&hardlink.path),
            Err(DfsPosixError::Dfs(DfsError::Distributed { .. }))
        ));
        assert!(matches!(
            service
                .posix_read_file_prefix_distributed_outcome(&hardlink.path, 4)
                .expect("prefix-read present hardlink with unavailable target"),
            DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable { .. }
        ));
        assert!(matches!(
            service.posix_read_file_prefix_distributed(&hardlink.path, 4),
            Err(DfsPosixError::Dfs(DfsError::Distributed { .. }))
        ));
        assert!(matches!(
            service
                .posix_read_file_prefix_distributed_outcome("/missing-prefix.sobj", 4)
                .expect("classify missing prefix path"),
            DfsPosixDistributedFilePrefixReadOutcome::NotFound { .. }
        ));

        let bulk_paths = vec![hardlink.path.clone(), "/missing-catalog.sobj".to_string()];
        let bulk_outcomes = service.posix_read_files_distributed_outcomes(&bulk_paths);
        assert!(matches!(
            &bulk_outcomes[0],
            Ok(DfsPosixDistributedFileReadOutcome::ContentUnavailable { .. })
        ));
        assert!(matches!(
            &bulk_outcomes[1],
            Ok(DfsPosixDistributedFileReadOutcome::NotFound { .. })
        ));
        let bulk = service.posix_read_files_distributed(&bulk_paths);
        assert!(matches!(
            &bulk[0],
            Err(DfsPosixError::Dfs(DfsError::Distributed { .. }))
        ));
        assert!(matches!(&bulk[1], Err(DfsPosixError::NotFound { .. })));

        let pending = PosixPendingBulkRead {
            index: 0,
            path: hardlink.path.clone(),
            request_id: "typed-bulk-content-miss".to_string(),
            object_id: hardlink.object_id.clone(),
            version: hardlink.version.clone(),
            content_hash: "blake3:missing".to_string(),
            content_size_bytes: 7,
        };
        let remote_miss = DfsBulkReadItemResponse {
            request_id: pending.request_id.clone(),
            object_id: pending.object_id.clone(),
            result: DfsReadResult::NotFound,
        };
        assert!(matches!(
            service
                .validate_posix_bulk_read_response(&pending, remote_miss)
                .expect("classify a bulk source content miss"),
            DfsPosixDistributedFileReadOutcome::ContentUnavailable { .. }
        ));

        let mut tombstone = object;
        tombstone.kind = DfsObjectKind::Tombstone;
        assert!(matches!(
            service
                .posix_file_content_object_candidate_outcome(tombstone.path.clone(), tombstone,)
                .expect("classify tombstone"),
            DistributedPosixFileContentObjectOutcome::NotFound { .. }
        ));

        drop(service);
        native.close().expect("normal native owner shutdown");
    }

    #[test]
    fn opened_stat_read_keeps_exact_version_without_resolving_current_path_metadata() {
        let local = NodeId::from_legacy_named("node_opened_stat_read").expect("node id");
        let (native, mut service) = crate::test_support::DfsTestRuntime::start_single_node(
            "native-opened-stat-read",
            local.clone(),
        )
        .expect("normal initialized native owner");
        let options = DfsWriteOptions::replicated_eventual(vec![local]);
        service
            .posix_write_file("/opened.txt", b"version-one", options.clone())
            .expect("normal first version publication");
        let opened = service
            .posix_stat_distributed("/opened.txt")
            .expect("actual opened stat");
        service
            .posix_write_file("/opened.txt", b"version-two", options)
            .expect("normal second version publication");
        assert_ne!(
            service
                .posix_stat_distributed("/opened.txt")
                .expect("current stat")
                .version,
            opened.version
        );

        service
            .posix_rename("/opened.txt", "/moved-after-open.txt")
            .expect("normal rename after open");
        assert!(matches!(
            service.posix_stat_distributed("/opened.txt"),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            service
                .posix_read_file_range_for_opened_stat("/opened.txt", &opened, 0, usize::MAX,)
                .expect("read exact opened version"),
            b"version-one"
        );

        drop(service);
        native.close().expect("normal native owner shutdown");
    }
}
