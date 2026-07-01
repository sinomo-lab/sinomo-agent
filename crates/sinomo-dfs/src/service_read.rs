//! Local content publication, metadata resolution, and read-path helpers.
//!
//! This module owns the service-facing object read surface: local object
//! publication, remote-read request scheduling, authoritative metadata
//! resolution, local stream opening, and confirmed-read checks. Data-plane
//! message handlers remain in the root service protocol until their send/receive
//! paths are split together.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::Instant;

use sinomo_core::DfsObjectPolicy;
use sinomo_node::NodeId;

use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, DFS_READ_CHUNK_WINDOW_SIZE, PendingNetworkedRead, chunk_prefix_bytes,
    dfs_byte_count_to_usize, range_transfer_size_bytes, read_response_window_range_len,
};
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn put_local_object_version(
        &mut self,
        object: DfsObjectMetadata,
        bytes: &[u8],
    ) -> Result<String, DfsError> {
        let manifest = self.content.put_object_version(
            object.object_id.as_str(),
            object.version.as_str(),
            bytes,
        )?;
        self.publish_local_object_manifest(object, manifest)
    }

    pub fn put_local_object_version_from_reader<R: Read>(
        &mut self,
        object: DfsObjectMetadata,
        reader: R,
    ) -> Result<String, DfsError> {
        let manifest = self.content.put_object_version_from_reader(
            object.object_id.as_str(),
            object.version.as_str(),
            reader,
        )?;
        self.publish_local_object_manifest(object, manifest)
    }

    fn publish_local_object_manifest(
        &mut self,
        mut object: DfsObjectMetadata,
        manifest: ContentManifest,
    ) -> Result<String, DfsError> {
        object.content_hash = Some(manifest.content_hash.clone());
        object.content_size_bytes = Some(manifest.size_bytes);
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.put_visible_object_in_metadata_backend(&object)?;
        self.put_object_version_size_in_metadata_backend(&object, Some(manifest.size_bytes))?;
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)?;
        Ok(manifest.content_hash)
    }

    pub fn read_local_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsReadData>, DfsError> {
        Ok(match self.read_local_object_with_state(object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data,
                freshness:
                    NetworkedDfsReadFreshness::Current | NetworkedDfsReadFreshness::Confirmed { .. },
            } => Some(data),
            NetworkedDfsReadOutcome::Found { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::Conflicted { .. }
            | NetworkedDfsReadOutcome::NotFound => None,
        })
    }

    pub fn request_remote_object_read(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<NodeId>, DfsError> {
        self.request_remote_object_read_inner(object_id, None, true)
    }

    pub fn request_remote_object_read_without_cache(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<NodeId>, DfsError> {
        self.request_remote_object_read_inner(object_id, None, false)
    }

    pub(crate) fn request_remote_object_read_for_metadata_without_cache(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Vec<NodeId>, DfsError> {
        self.request_remote_object_read_for_metadata_inner(object, None, false)
    }

    pub fn request_remote_object_range_read(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<NodeId>, DfsError> {
        self.request_remote_object_read_inner(object_id, Some((offset, max_bytes)), true)
    }

    fn request_remote_object_read_inner(
        &mut self,
        object_id: &DfsObjectId,
        range: Option<(u64, usize)>,
        cache_response: bool,
    ) -> Result<Vec<NodeId>, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.request_remote_object_read_for_metadata_inner(&object, range, cache_response)
    }

    fn request_remote_object_read_for_metadata_inner(
        &mut self,
        object: &DfsObjectMetadata,
        range: Option<(u64, usize)>,
        cache_response: bool,
    ) -> Result<Vec<NodeId>, DfsError> {
        self.prune_pending_remote_reads_to_removed_locations(object);
        if object.content_hash.is_none() {
            return Ok(Vec::new());
        }

        let mut requested = Vec::new();
        let now = Instant::now();
        let mut seen_locations = BTreeSet::new();
        let mut expired_candidates = Vec::new();
        for node_id in &object.current_locations {
            if node_id == &self.node_id {
                continue;
            }
            if !seen_locations.insert(node_id.clone()) {
                continue;
            }
            if let Some((request_id, pending)) =
                self.pending_remote_read_for_object_target(object, node_id, range, cache_response)
            {
                if pending.next_retry_at > now {
                    requested.push(node_id.clone());
                    break;
                }
                if self.pending_remote_read_has_resume_state(&request_id, &pending)
                    && self.retry_remote_read_request_to(&request_id, &pending, node_id, now)?
                {
                    requested.push(node_id.clone());
                    break;
                }
                self.pending_reads.remove(&request_id);
                self.incoming_read_transfers.remove(&request_id);
                self.incoming_read_memory_transfers.remove(&request_id);
                expired_candidates.push(node_id.clone());
                continue;
            }
            if self.send_remote_read_request_to(object, node_id, now, range, cache_response)? {
                requested.push(node_id.clone());
                break;
            }
        }
        if requested.is_empty() {
            for node_id in expired_candidates {
                if self.send_remote_read_request_to(object, &node_id, now, range, cache_response)? {
                    requested.push(node_id);
                    break;
                }
            }
        }
        Ok(requested)
    }

    fn send_remote_read_request_to(
        &mut self,
        object: &DfsObjectMetadata,
        target_node_id: &NodeId,
        now: Instant,
        range: Option<(u64, usize)>,
        cache_response: bool,
    ) -> Result<bool, DfsError> {
        let request_id = self.next_request_id();
        let chunked_range =
            range.is_some_and(|(_, max_bytes)| max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES);
        let transfer_size_bytes = range
            .map(|(_, max_bytes)| max_bytes as u64)
            .or(object.content_size_bytes)
            .unwrap_or((DFS_BLOB_CHUNK_BYTES as u64) * u64::from(DFS_READ_CHUNK_WINDOW_SIZE));
        let adaptive_read_window =
            self.adaptive_read_chunk_window(target_node_id, transfer_size_bytes);
        let direct_uncached_full_read = !cache_response
            && range.is_none()
            && object
                .content_size_bytes
                .is_some_and(|size| size <= DFS_DIRECT_REMOTE_READ_MAX_BYTES);
        let (start_chunk_index, max_chunks, window_end_chunk_index) = match range {
            Some(_) if chunked_range => (
                0,
                adaptive_read_window,
                Self::read_window_end_chunk_index(0, adaptive_read_window),
            ),
            Some(_) => (0, 0, None),
            None if direct_uncached_full_read => (0, 0, None),
            None => (
                0,
                adaptive_read_window,
                Self::read_window_end_chunk_index(0, adaptive_read_window),
            ),
        };
        let read_request = DfsReadRequest {
            request_id: request_id.clone(),
            object_id: object.object_id.clone(),
            version: Some(object.version.clone()),
            content_hash: object.content_hash.clone(),
            content_size_bytes: object.content_size_bytes,
            range_offset: range.map(|(offset, _)| offset),
            range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
            start_chunk_index,
            max_chunks,
        };
        let pending = PendingNetworkedRead {
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            content_size_bytes: object.content_size_bytes,
            range_offset: range.map(|(offset, _)| offset),
            range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
            next_chunk_index: 0,
            window_end_chunk_index,
            target_node_id: target_node_id.clone(),
            next_retry_at: now + DFS_REMOTE_READ_RETRY_DELAY,
            cache_response,
        };
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::ReadRequest(read_request),
        )?;
        if sent {
            self.pending_reads.insert(request_id, pending);
        }
        Ok(sent)
    }

    fn prune_pending_remote_reads_to_removed_locations(&mut self, object: &DfsObjectMetadata) {
        let request_ids = self
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == object.object_id
                    && !object.current_locations.contains(&pending.target_node_id)
            })
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
            self.incoming_read_memory_transfers.remove(&request_id);
        }
    }

    fn pending_remote_read_has_resume_state(
        &self,
        request_id: &str,
        pending: &PendingNetworkedRead,
    ) -> bool {
        pending.next_chunk_index > 0
            || self.incoming_read_transfers.contains_key(request_id)
            || self.incoming_read_memory_transfers.contains_key(request_id)
    }

    fn retry_remote_read_request_to(
        &mut self,
        request_id: &str,
        pending: &PendingNetworkedRead,
        target_node_id: &NodeId,
        now: Instant,
    ) -> Result<bool, DfsError> {
        let start_chunk_index = pending.next_chunk_index;
        let transfer_size_bytes = pending
            .range_max_bytes
            .map(|max_bytes| max_bytes as u64)
            .or(pending.content_size_bytes)
            .unwrap_or((DFS_BLOB_CHUNK_BYTES as u64) * u64::from(DFS_READ_CHUNK_WINDOW_SIZE));
        let max_chunks = self.adaptive_read_chunk_window(target_node_id, transfer_size_bytes);
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.to_string(),
                object_id: pending.object_id.clone(),
                version: Some(pending.version.clone()),
                content_hash: pending.content_hash.clone(),
                content_size_bytes: pending.content_size_bytes,
                range_offset: pending.range_offset,
                range_max_bytes: pending.range_max_bytes,
                start_chunk_index,
                max_chunks,
            }),
        )?;
        if sent && let Some(pending) = self.pending_reads.get_mut(request_id) {
            pending.target_node_id = target_node_id.clone();
            pending.next_retry_at = now + DFS_REMOTE_READ_RETRY_DELAY;
            pending.window_end_chunk_index =
                Self::read_window_end_chunk_index(start_chunk_index, max_chunks);
        }
        Ok(sent)
    }

    pub(crate) fn has_pending_remote_read_for_current_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<bool, DfsError> {
        Ok(self
            .next_pending_remote_read_retry_for_current_object(object_id)?
            .is_some_and(|next_retry_at| next_retry_at > Instant::now()))
    }

    pub(crate) fn has_pending_remote_read_for_metadata(&self, object: &DfsObjectMetadata) -> bool {
        self.next_pending_remote_read_retry_for_metadata(object)
            .is_some_and(|next_retry_at| next_retry_at > Instant::now())
    }

    pub(crate) fn next_pending_remote_read_retry_for_current_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<Instant>, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        Ok(self.next_pending_remote_read_retry_for_metadata(&object))
    }

    pub(crate) fn next_pending_remote_read_retry_for_metadata(
        &self,
        object: &DfsObjectMetadata,
    ) -> Option<Instant> {
        self.pending_reads
            .values()
            .filter(|pending| {
                pending.object_id == object.object_id
                    && pending.version == object.version
                    && pending.content_hash == object.content_hash
                    && pending.content_size_bytes == object.content_size_bytes
                    && pending.range_offset.is_none()
                    && pending.range_max_bytes.is_none()
                    && object.current_locations.contains(&pending.target_node_id)
            })
            .map(|pending| pending.next_retry_at)
            .min()
    }

    fn pending_remote_read_for_object_target(
        &self,
        object: &DfsObjectMetadata,
        target_node_id: &NodeId,
        range: Option<(u64, usize)>,
        cache_response: bool,
    ) -> Option<(String, PendingNetworkedRead)> {
        let range_offset = range.map(|(offset, _)| offset);
        let range_max_bytes = range.map(|(_, max_bytes)| max_bytes);
        self.pending_reads
            .iter()
            .find(|(_, pending)| {
                pending.object_id == object.object_id
                    && pending.target_node_id == *target_node_id
                    && pending.version == object.version
                    && pending.content_hash == object.content_hash
                    && pending.content_size_bytes == object.content_size_bytes
                    && pending.range_offset == range_offset
                    && pending.range_max_bytes == range_max_bytes
                    && pending.cache_response == cache_response
            })
            .map(|(request_id, pending)| (request_id.clone(), pending.clone()))
    }

    pub(crate) fn current_read_object_metadata(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            None
        };
        let mut object = self.resolve_authoritative_object_metadata(
            object_id,
            memory_object.as_ref(),
            backend_object.as_ref(),
        )?;
        object = self.object_metadata_with_receipt_locations(object)?;
        self.apply_transient_remote_read_location_prunes(&mut object);
        Ok(object)
    }

    pub(crate) fn object_metadata_with_receipt_locations(
        &self,
        mut object: DfsObjectMetadata,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let Some(content_hash) = object.content_hash.as_deref() else {
            return Ok(object);
        };
        let mut locations = object
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for receipt in self.backend_receipts_for_version(&object.object_id, &object.version)? {
            if receipt.content_hash.as_deref() == Some(content_hash) {
                locations.insert(receipt.replica_node_id);
            }
        }
        object.current_locations = locations.into_iter().collect();
        Ok(object)
    }

    pub(crate) fn apply_transient_remote_read_location_prunes(
        &self,
        object: &mut DfsObjectMetadata,
    ) {
        let key = (
            object.object_id.clone(),
            object.version.clone(),
            object.content_hash.clone(),
        );
        if let Some(pruned) = self.transient_remote_read_pruned_locations.get(&key) {
            object
                .current_locations
                .retain(|node_id| !pruned.contains(node_id));
        }
    }

    pub(crate) fn record_transient_remote_read_location_prune(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
        source_node_id: &NodeId,
    ) {
        self.transient_remote_read_pruned_locations
            .entry((object_id.clone(), version.clone(), content_hash.clone()))
            .or_default()
            .insert(source_node_id.clone());
    }

    pub(crate) fn clear_transient_remote_read_location_prunes_for_object(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
    ) {
        self.transient_remote_read_pruned_locations.remove(&(
            object_id.clone(),
            version.clone(),
            content_hash.clone(),
        ));
    }

    pub(crate) fn resolve_authoritative_object_metadata(
        &self,
        object_id: &DfsObjectId,
        memory_object: Option<&DfsObjectMetadata>,
        backend_object: Option<&DfsObjectMetadata>,
    ) -> Result<DfsObjectMetadata, DfsError> {
        match (memory_object, backend_object) {
            (Some(memory_object), Some(backend_object))
                if self.metadata_backend.is_some()
                    && self.local_write_view_overrides_backend(
                        object_id,
                        memory_object,
                        Some(backend_object),
                    ) =>
            {
                Ok(memory_object.clone())
            }
            (Some(memory_object), Some(backend_object))
                if self.metadata_backend.is_none()
                    && memory_object.version > backend_object.version =>
            {
                Ok(memory_object.clone())
            }
            (_, Some(backend_object)) => Ok(backend_object.clone()),
            (Some(memory_object), None) => Ok(memory_object.clone()),
            (None, None) => Err(DfsError::UnknownObject {
                object_id: object_id.clone(),
            }),
        }
    }

    pub(crate) fn local_write_view_overrides_backend(
        &self,
        object_id: &DfsObjectId,
        memory_object: &DfsObjectMetadata,
        backend_object: Option<&DfsObjectMetadata>,
    ) -> bool {
        match backend_object {
            None => true,
            Some(backend_object) if memory_object.version > backend_object.version => {
                self.has_pending_write_targeting(object_id, &memory_object.version)
                    || self.has_awaiting_intent_targeting(object_id, &memory_object.version)
            }
            Some(backend_object)
                if memory_object.kind != DfsObjectKind::Tombstone
                    && backend_object.kind == DfsObjectKind::Tombstone
                    && memory_object.version >= backend_object.version =>
            {
                self.has_pending_write_targeting(object_id, &memory_object.version)
                    || self.has_awaiting_intent_targeting(object_id, &memory_object.version)
            }
            _ => false,
        }
    }

    fn has_pending_write_targeting(&self, object_id: &DfsObjectId, version: &DfsVersion) -> bool {
        self.pending_writes
            .values()
            .any(|pending| &pending.object_id == object_id && &pending.version == version)
    }

    fn has_awaiting_intent_targeting(&self, object_id: &DfsObjectId, version: &DfsVersion) -> bool {
        self.coordinator
            .has_awaiting_intent_targeting(object_id, version)
    }

    pub fn read_local_object_with_state(
        &self,
        object_id: &DfsObjectId,
        allow_stale: bool,
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.read_object_metadata_local_content_with_state(&object, allow_stale)
    }

    pub(crate) fn read_object_metadata_local_content_with_state(
        &self,
        object: &DfsObjectMetadata,
        allow_stale: bool,
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let open_conflicts = self.open_conflicts_for_object(&object.object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadOutcome::Conflicted {
                object_id: object.object_id.clone(),
                visible_version: object.version.clone(),
                visible_content_hash: object.content_hash.clone(),
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok((manifest, bytes)) = self
            .content
            .read_object_version(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            return Ok(NetworkedDfsReadOutcome::Found {
                data: DfsReadData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(manifest.content_hash),
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(bytes) = self.content.read_blob(expected_hash)
        {
            return Ok(NetworkedDfsReadOutcome::Found {
                data: DfsReadData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(expected_hash.clone()),
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if allow_stale {
            let stale_read = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .filter(|manifest| manifest.version != object.version.as_str())
                .find_map(|manifest| {
                    self.content
                        .read_object_version(&manifest.object_id, &manifest.version)
                        .ok()
                });
            if let Some((manifest, bytes)) = stale_read {
                return Ok(NetworkedDfsReadOutcome::Found {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: DfsVersion::deserialize_migrating_legacy(manifest.version.clone())
                            .map_err(|error| DfsError::ContentStore {
                                message: error.to_string(),
                            })?,
                        content_hash: Some(manifest.content_hash.clone()),
                        bytes,
                    },
                    freshness: NetworkedDfsReadFreshness::Stale {
                        current_version: object.version.clone(),
                        current_content_hash: object.content_hash.clone(),
                    },
                });
            }
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            })
        } else {
            Ok(NetworkedDfsReadOutcome::NotFound)
        }
    }

    pub(crate) fn open_local_current_object_stream_with_state(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<NetworkedDfsReadStreamOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.open_local_object_metadata_stream_with_state(&object)
    }

    pub(crate) fn open_local_object_metadata_stream_with_state(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<NetworkedDfsReadStreamOutcome, DfsError> {
        let open_conflicts = self.open_conflicts_for_object(&object.object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadStreamOutcome::Conflicted {
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok((manifest, reader)) = self
            .content
            .open_object_version_stream(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash),
                        bytes: Vec::new(),
                    },
                    total_size_bytes: manifest.size_bytes,
                    reader,
                },
            ));
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(reader) = self.content.open_blob_stream(expected_hash)
            && object
                .content_size_bytes
                .is_none_or(|expected_size| expected_size == reader.size_bytes())
        {
            let total_size_bytes = reader.size_bytes();
            let reader = ObjectVersionReader::from_blob(reader);
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(expected_hash.clone()),
                        bytes: Vec::new(),
                    },
                    total_size_bytes,
                    reader,
                },
            ));
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
        } else {
            Ok(NetworkedDfsReadStreamOutcome::NotFound)
        }
    }

    pub(crate) fn open_local_current_object_read_window_stream_with_state(
        &self,
        object_id: &DfsObjectId,
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Result<NetworkedDfsReadStreamOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.open_local_object_metadata_read_window_stream_with_state(
            &object,
            start_chunk_index,
            max_chunks,
        )
    }

    fn open_local_object_metadata_read_window_stream_with_state(
        &self,
        object: &DfsObjectMetadata,
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Result<NetworkedDfsReadStreamOutcome, DfsError> {
        let open_conflicts = self.open_conflicts_for_object(&object.object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadStreamOutcome::Conflicted {
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            let range_len =
                read_response_window_range_len(start_chunk_index, manifest.size_bytes, max_chunks)?;
            let (manifest, reader) = self.content.open_object_version_range_stream(
                object.object_id.as_str(),
                object.version.as_str(),
                u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64,
                range_len,
            )?;
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash),
                        bytes: Vec::new(),
                    },
                    total_size_bytes: manifest.size_bytes,
                    reader,
                },
            ));
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(reader) = self.content.open_blob_stream(expected_hash)
            && object
                .content_size_bytes
                .is_none_or(|expected_size| expected_size == reader.size_bytes())
        {
            let total_size_bytes = reader.size_bytes();
            let range_len =
                read_response_window_range_len(start_chunk_index, total_size_bytes, max_chunks)?;
            let reader = self.content.open_blob_range_stream(
                expected_hash,
                u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64,
                range_len,
            )?;
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(expected_hash.clone()),
                        bytes: Vec::new(),
                    },
                    total_size_bytes,
                    reader: ObjectVersionReader::from_blob_range(reader),
                },
            ));
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
        } else {
            Ok(NetworkedDfsReadStreamOutcome::NotFound)
        }
    }

    pub(crate) fn open_local_current_object_range_stream_with_state(
        &self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
        start_chunk_index: u32,
    ) -> Result<NetworkedDfsRangeReadStreamOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.open_local_object_metadata_range_stream_with_state(
            &object,
            offset,
            max_bytes,
            start_chunk_index,
        )
    }

    pub(crate) fn open_local_object_metadata_range_stream_with_state(
        &self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        start_chunk_index: u32,
    ) -> Result<NetworkedDfsRangeReadStreamOutcome, DfsError> {
        let open_conflicts = self.open_conflicts_for_object(&object.object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsRangeReadStreamOutcome::Conflicted {
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            let range_offset = offset.min(manifest.size_bytes);
            let range_size_bytes =
                range_transfer_size_bytes(manifest.size_bytes, offset, max_bytes);
            let window_prefix_bytes = chunk_prefix_bytes(range_size_bytes, start_chunk_index);
            let window_offset = range_offset.saturating_add(window_prefix_bytes);
            let window_max_bytes = dfs_byte_count_to_usize(
                "DFS range read window",
                range_size_bytes.saturating_sub(window_prefix_bytes),
            )?;
            let (manifest, reader) = self.content.open_object_version_range_stream(
                object.object_id.as_str(),
                object.version.as_str(),
                window_offset,
                window_max_bytes,
            )?;
            return Ok(NetworkedDfsRangeReadStreamOutcome::Found(
                NetworkedDfsRangeReadStream {
                    data: DfsReadRangeData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash),
                        offset: range_offset,
                        total_size_bytes: manifest.size_bytes,
                        bytes: Vec::new(),
                    },
                    range_size_bytes,
                    reader,
                },
            ));
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(reader) = self.content.open_blob_stream(expected_hash)
            && object
                .content_size_bytes
                .is_none_or(|expected_size| expected_size == reader.size_bytes())
        {
            let object_total_size_bytes = reader.size_bytes();
            let range_offset = offset.min(object_total_size_bytes);
            let range_size_bytes =
                range_transfer_size_bytes(object_total_size_bytes, offset, max_bytes);
            let window_prefix_bytes = chunk_prefix_bytes(range_size_bytes, start_chunk_index);
            let window_offset = range_offset.saturating_add(window_prefix_bytes);
            let window_max_bytes = dfs_byte_count_to_usize(
                "DFS range read window",
                range_size_bytes.saturating_sub(window_prefix_bytes),
            )?;
            let reader = self.content.open_blob_range_stream(
                expected_hash,
                window_offset,
                window_max_bytes,
            )?;
            return Ok(NetworkedDfsRangeReadStreamOutcome::Found(
                NetworkedDfsRangeReadStream {
                    data: DfsReadRangeData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(expected_hash.clone()),
                        offset: range_offset,
                        total_size_bytes: object_total_size_bytes,
                        bytes: Vec::new(),
                    },
                    range_size_bytes,
                    reader: ObjectVersionReader::from_blob_range(reader),
                },
            ));
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsRangeReadStreamOutcome::PendingReplication)
        } else {
            Ok(NetworkedDfsRangeReadStreamOutcome::NotFound)
        }
    }

    pub fn read_local_object_range_with_state(
        &self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
        allow_stale: bool,
    ) -> Result<NetworkedDfsRangeReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.read_object_metadata_local_range_with_state(&object, offset, max_bytes, allow_stale)
    }

    pub(crate) fn read_object_metadata_local_range_with_state(
        &self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        allow_stale: bool,
    ) -> Result<NetworkedDfsRangeReadOutcome, DfsError> {
        let open_conflicts = self.open_conflicts_for_object(&object.object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsRangeReadOutcome::Conflicted {
                object_id: object.object_id.clone(),
                visible_version: object.version.clone(),
                visible_content_hash: object.content_hash.clone(),
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok((manifest, bytes)) = self.content.read_object_version_range(
            object.object_id.as_str(),
            object.version.as_str(),
            offset,
            max_bytes,
        ) && object
            .content_hash
            .as_ref()
            .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            return Ok(NetworkedDfsRangeReadOutcome::Found {
                data: DfsReadRangeData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(manifest.content_hash),
                    offset: offset.min(manifest.size_bytes),
                    total_size_bytes: manifest.size_bytes,
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(mut reader) =
                self.content
                    .open_blob_range_stream(expected_hash, offset, max_bytes)
        {
            let total_size_bytes = reader.size_bytes();
            let range_offset = reader.offset();
            let mut bytes =
                Vec::with_capacity(usize::try_from(reader.remaining_bytes()).unwrap_or(usize::MAX));
            reader
                .read_to_end(&mut bytes)
                .map_err(|source| DfsError::ContentStore {
                    message: source.to_string(),
                })?;
            return Ok(NetworkedDfsRangeReadOutcome::Found {
                data: DfsReadRangeData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(expected_hash.clone()),
                    offset: range_offset,
                    total_size_bytes,
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if allow_stale {
            let stale_read = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .filter(|manifest| manifest.version != object.version.as_str())
                .find_map(|manifest| {
                    self.content
                        .read_object_version_range(
                            &manifest.object_id,
                            &manifest.version,
                            offset,
                            max_bytes,
                        )
                        .ok()
                });
            if let Some((manifest, bytes)) = stale_read {
                return Ok(NetworkedDfsRangeReadOutcome::Found {
                    data: DfsReadRangeData {
                        object_id: object.object_id.clone(),
                        version: DfsVersion::deserialize_migrating_legacy(manifest.version.clone())
                            .map_err(|error| DfsError::ContentStore {
                                message: error.to_string(),
                            })?,
                        content_hash: Some(manifest.content_hash.clone()),
                        offset: offset.min(manifest.size_bytes),
                        total_size_bytes: manifest.size_bytes,
                        bytes,
                    },
                    freshness: NetworkedDfsReadFreshness::Stale {
                        current_version: object.version.clone(),
                        current_content_hash: object.content_hash.clone(),
                    },
                });
            }
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsRangeReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            })
        } else {
            Ok(NetworkedDfsRangeReadOutcome::NotFound)
        }
    }

    pub fn read_local_object_confirmed(
        &self,
        object_id: &DfsObjectId,
        required_replica_node_ids: &[NodeId],
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        let confirmed_locations =
            self.confirmed_locations_for_object(&object, required_replica_node_ids)?;
        let confirmed_set: BTreeSet<&NodeId> = confirmed_locations.iter().collect();
        let missing_required = required_replica_node_ids
            .iter()
            .any(|node_id| !confirmed_set.contains(node_id));
        if missing_required {
            return Ok(NetworkedDfsReadOutcome::NotConfirmed {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                confirmed_locations,
                required_locations: required_replica_node_ids.to_vec(),
            });
        }

        match self.read_local_object_with_state(object_id, false)? {
            NetworkedDfsReadOutcome::Found { data, .. } => Ok(NetworkedDfsReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Confirmed {
                    replica_node_ids: confirmed_locations,
                },
            }),
            other => Ok(other),
        }
    }
}
