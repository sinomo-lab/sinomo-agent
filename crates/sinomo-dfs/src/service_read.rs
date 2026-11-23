//! Local content publication, metadata resolution, and read-path helpers.
//!
//! This module owns the service-facing object read surface: local object
//! publication, remote-read request scheduling, authoritative metadata
//! resolution, local stream opening, and confirmed-read checks. Network read
//! message handlers live in `service_read_transfer`, and pending-write fetch
//! recovery lives in `service_fetch_transfer`.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::Instant;

use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;

use crate::transfer::{PendingNetworkedRead, range_transfer_size_bytes};
use crate::*;

#[cfg(test)]
thread_local! {
    static REMOTE_METADATA_RANGE_READ_REQUEST_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_remote_metadata_range_read_request_count() {
    REMOTE_METADATA_RANGE_READ_REQUEST_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn remote_metadata_range_read_request_count() -> usize {
    REMOTE_METADATA_RANGE_READ_REQUEST_COUNT.get()
}

/// Where a service reads an object's current record.
pub(crate) enum DfsObjectReadSource {
    Local,
    Authority {
        file_system_id: DfsFileSystemId,
        hinted_shard: Option<DfsMetadataShardConfig>,
    },
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn ensure_standalone_local_object_publication(&self) -> Result<(), DfsError> {
        if self.control_plane.is_some() {
            return Err(DfsError::Distributed {
                message: "local object-version publication is standalone-only; clustered DFS metadata must be published through the canonical distributed write path"
                    .to_string(),
            });
        }
        Ok(())
    }

    pub fn put_local_object_version(
        &mut self,
        object: DfsObjectMetadata,
        bytes: &[u8],
    ) -> Result<String, DfsError> {
        self.ensure_standalone_local_object_publication()?;
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
        self.ensure_standalone_local_object_publication()?;
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

    pub(crate) fn request_remote_object_range_read_for_metadata_without_cache(
        &mut self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<String>, DfsError> {
        #[cfg(test)]
        REMOTE_METADATA_RANGE_READ_REQUEST_COUNT.with(|count| {
            count.set(count.get().saturating_add(1));
        });
        let requested_nodes = self.request_remote_object_read_for_metadata_inner(
            object,
            Some((offset, max_bytes)),
            false,
        )?;
        Ok(self
            .reads
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == object.object_id
                    && pending.version == object.version
                    && pending.content_hash == object.content_hash
                    && pending.content_size_bytes == object.content_size_bytes
                    && pending.range_offset == Some(offset)
                    && pending.range_max_bytes == Some(max_bytes)
                    && !pending.cache_response
                    && requested_nodes.contains(&pending.target_node_id)
            })
            .map(|(request_id, _)| request_id.clone())
            .collect())
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
        let ranked_sources = self.ranked_transfer_sources(object.current_locations.clone());
        for node_id in &ranked_sources {
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
                self.reads.pending_reads.remove(&request_id);
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
        let read_request = DfsReadRequest {
            request_id: request_id.clone(),
            object_id: object.object_id.clone(),
            version: Some(object.version.clone()),
            content_hash: object.content_hash.clone(),
            content_size_bytes: object.content_size_bytes,
            range_offset: range.map(|(offset, _)| offset),
            range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
        };
        let pending = PendingNetworkedRead {
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            content_size_bytes: object.content_size_bytes,
            range_offset: range.map(|(offset, _)| offset),
            range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
            target_node_id: target_node_id.clone(),
            next_retry_at: now + DFS_REMOTE_READ_RETRY_DELAY,
            cache_response,
        };
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::ReadRequest(read_request),
        )?;
        if sent {
            self.reads.pending_reads.insert(request_id, pending);
        }
        Ok(sent)
    }

    fn prune_pending_remote_reads_to_removed_locations(&mut self, object: &DfsObjectMetadata) {
        let request_ids = self
            .reads
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == object.object_id
                    && !object.current_locations.contains(&pending.target_node_id)
            })
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.reads.pending_reads.remove(&request_id);
        }
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
        self.reads
            .pending_reads
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

    pub(crate) fn next_pending_remote_range_read_retry_for_metadata(
        &self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Option<Instant> {
        self.reads
            .pending_reads
            .values()
            .filter(|pending| {
                pending.object_id == object.object_id
                    && pending.version == object.version
                    && pending.content_hash == object.content_hash
                    && pending.content_size_bytes == object.content_size_bytes
                    && pending.range_offset == Some(offset)
                    && pending.range_max_bytes == Some(max_bytes)
                    && !pending.cache_response
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
        self.reads
            .pending_reads
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

    /// The current object behind an object id or open handle, read locally
    /// or from its authority as [`Self::object_read_source`] decides.
    pub(crate) fn current_read_object_metadata_distributed(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let source = self.object_read_source(object_id, None)?;
        self.current_read_object_metadata_from_source(object_id, source)
    }

    /// Read `object_id` from the source [`Self::object_read_source`] chose.
    pub(crate) fn current_read_object_metadata_from_source(
        &mut self,
        object_id: &DfsObjectId,
        source: DfsObjectReadSource,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let DfsObjectReadSource::Authority {
            file_system_id,
            hinted_shard,
        } = source
        else {
            return self.current_read_object_metadata(object_id);
        };
        let object = self
            .current_object_from_hinted_authority(&file_system_id, object_id, hinted_shard)?
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;
        Ok(self.observed_authority_read_object(object))
    }

    /// Where the current record of `object_id` is read. A service answers
    /// from its own metadata only for objects whose path lies in a shard it
    /// reads locally (see `reads_shard_from_local_metadata`); any other object
    /// is read from its authority, starting with the shard its last known
    /// path names.
    pub(crate) fn object_read_source(
        &mut self,
        object_id: &DfsObjectId,
        file_system_hint: Option<&DfsFileSystemId>,
    ) -> Result<DfsObjectReadSource, DfsError> {
        if self.control_plane.is_none() || self.transient_metadata_object_ids.contains(object_id) {
            return Ok(DfsObjectReadSource::Local);
        }
        let local = match self.metadata.object(object_id) {
            Some(object) => Some((object.file_system_id.clone(), object.path.clone())),
            None => match self.metadata_backend.as_ref() {
                Some(backend) => backend
                    .get_object(object_id)?
                    .map(|object| (object.file_system_id, object.path)),
                None => None,
            },
        };
        // A service holding no record of the object may have just resolved its
        // path at an authority. That path names the shard to ask first; the
        // record itself is still read from the authority.
        let local = local.or_else(|| self.routed_lookup_path_of_object(object_id));
        let file_system_id = match (&local, file_system_hint) {
            (Some((file_system_id, _)), _) => file_system_id.clone(),
            (None, Some(file_system_id)) => file_system_id.clone(),
            (None, None) => crate::sifs_file_system_id(),
        };
        let shards = self
            .metadata_shard_config_snapshot(&file_system_id)?
            .filter(|shards| !shards.is_empty());
        let Some(shards) = shards else {
            return Ok(if self.transport.delegates_local_namespace_authority() {
                DfsObjectReadSource::Authority {
                    file_system_id,
                    hinted_shard: None,
                }
            } else {
                DfsObjectReadSource::Local
            });
        };
        let Some((_, path)) = local else {
            return Ok(DfsObjectReadSource::Authority {
                file_system_id,
                hinted_shard: None,
            });
        };
        let hinted_shard = crate::control_plane::select_metadata_shard_config_for_path(
            &file_system_id,
            &shards,
            &path,
        )?;
        Ok(match hinted_shard {
            Some(shard) if !self.reads_shard_from_local_metadata(&shard) => {
                DfsObjectReadSource::Authority {
                    file_system_id,
                    hinted_shard: Some(shard),
                }
            }
            _ => DfsObjectReadSource::Local,
        })
    }

    /// Read `object_id` from the hinted shard's authority, then from every
    /// authority, since the object may have moved since the hint was taken. A
    /// hinted shard whose storage is closed or whose primary was not reached
    /// cannot answer now; the fan-out over every authority then decides (see
    /// [`crate::file_history_query::DfsAuthorityFanOut`]).
    pub(crate) fn current_object_from_hinted_authority(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        hinted_shard: Option<DfsMetadataShardConfig>,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let mut fan_out = crate::file_history_query::DfsAuthorityFanOut::default();
        if let Some(shard) = hinted_shard
            && let Some(crate::DfsMetadataQueryResult::CurrentObject {
                object: Some(current),
            }) = self.query_metadata_authority_in_fan_out(
                &mut fan_out,
                &shard,
                crate::DfsMetadataSelection::CurrentObject {
                    object_id: object_id.clone(),
                },
                None,
            )?
            && current.object_id == *object_id
            && current.file_system_id == *file_system_id
        {
            return Ok(Some(*current));
        }
        self.current_object_in_fan_out(&mut fan_out, file_system_id, object_id)
    }

    fn observed_authority_read_object(&self, mut object: DfsObjectMetadata) -> DfsObjectMetadata {
        self.apply_transient_remote_read_location_prunes(&mut object);
        if !self
            .transient_metadata_object_ids
            .contains(&object.object_id)
        {
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(&object),
            );
        }
        object
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
        #[cfg(test)]
        let placement_prune_input = self
            .placement_read_prune_capture_selected(&object)
            .then(|| object.current_locations.clone());
        self.apply_transient_remote_read_location_prunes(&mut object);
        #[cfg(test)]
        if let Some(before) = placement_prune_input {
            self.capture_placement_read_prune(backend_object.as_ref(), &before, &object);
        }
        if !self.transient_metadata_object_ids.contains(object_id) {
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(&object),
            );
        }
        Ok(object)
    }

    pub(crate) fn object_metadata_with_receipt_locations(
        &self,
        mut object: DfsObjectMetadata,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let Some(content_hash) = object.content_hash.as_deref() else {
            return Ok(object);
        };
        let mut locations = Vec::new();
        for node_id in std::mem::take(&mut object.current_locations) {
            if !locations.contains(&node_id) {
                locations.push(node_id);
            }
        }
        for receipt in self.backend_receipts_for_version(&object.object_id, &object.version)? {
            if receipt.content_hash.as_deref() == Some(content_hash)
                && !locations.contains(&receipt.replica_node_id)
            {
                locations.push(receipt.replica_node_id);
            }
        }
        object.current_locations = locations;
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
        if let Some(pruned) = self.reads.transient_remote_read_pruned_locations.get(&key) {
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
        self.reads
            .transient_remote_read_pruned_locations
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
        self.reads.transient_remote_read_pruned_locations.remove(&(
            object_id.clone(),
            version.clone(),
            content_hash.clone(),
        ));
    }

    pub(crate) fn clear_transient_remote_read_location_prune(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
        source_node_id: &NodeId,
    ) {
        let key = (object_id.clone(), version.clone(), content_hash.clone());
        let Some(pruned) = self
            .reads
            .transient_remote_read_pruned_locations
            .get_mut(&key)
        else {
            return;
        };
        pruned.remove(source_node_id);
        if pruned.is_empty() {
            self.reads
                .transient_remote_read_pruned_locations
                .remove(&key);
        }
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
        self.writes
            .pending_writes
            .iter()
            .any(|(intent_id, pending)| {
                &pending.object_id == object_id
                    && &pending.version == version
                    // This map also owns repairs after a write commits. A
                    // repair of an older opaque version is not a newer local
                    // namespace view, even if its identifier sorts higher.
                    && self.coordinator.intent(intent_id).is_some_and(|intent| {
                        intent.publication.is_current()
                            && intent.state == DfsWriteIntentState::AwaitingReceipts
                    })
            })
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

        // Metadata-only changes (for example atime updates) advance the
        // visible object version without changing its immutable content
        // identity. Packed and extent-backed values may not have a loose blob
        // at `content_hash`, so recover them through the per-object
        // content-identity index before falling back to a raw blob lookup.
        if let (Some(expected_hash), Some(size_bytes)) =
            (object.content_hash.as_ref(), object.content_size_bytes)
            && let Ok(Some((_manifest, bytes))) = self.content.read_object_content_range(
                object.object_id.as_str(),
                expected_hash,
                size_bytes,
                0,
                usize::MAX,
            )
            && bytes.len() as u64 == size_bytes
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
                        version: DfsVersion::new(manifest.version.clone()).map_err(|error| {
                            DfsError::ContentStore {
                                message: error.to_string(),
                            }
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

    pub(crate) fn open_local_current_object_range_stream_with_state(
        &self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadStreamOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.open_local_object_metadata_range_stream_with_state(&object, offset, max_bytes)
    }

    pub(crate) fn open_local_object_metadata_range_stream_with_state(
        &self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
    ) -> Result<NetworkedDfsRangeReadStreamOutcome, DfsError> {
        if let Ok((manifest, reader)) = self.content.open_object_version_range_stream_shared(
            object.object_id.as_str(),
            object.version.as_str(),
            offset,
            max_bytes,
        ) && object
            .content_hash
            .as_ref()
            .is_none_or(|expected_hash| expected_hash == &manifest.manifest().content_hash)
        {
            let manifest = manifest.manifest();
            let range_offset = offset.min(manifest.size_bytes);
            let range_size_bytes =
                range_transfer_size_bytes(manifest.size_bytes, offset, max_bytes);
            return Ok(NetworkedDfsRangeReadStreamOutcome::Found(
                NetworkedDfsRangeReadStream {
                    data: DfsReadRangeData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash.clone()),
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
            let reader =
                self.content
                    .open_blob_range_stream(expected_hash, range_offset, max_bytes)?;
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

        if let (Some(expected_hash), Some(expected_size)) =
            (object.content_hash.as_ref(), object.content_size_bytes)
            && let Ok(Some((manifest, reader))) = self.content.open_object_content_range_stream(
                object.object_id.as_str(),
                expected_hash,
                expected_size,
                offset,
                max_bytes,
            )
        {
            let range_offset = offset.min(manifest.size_bytes);
            let range_size_bytes =
                range_transfer_size_bytes(manifest.size_bytes, offset, max_bytes);
            return Ok(NetworkedDfsRangeReadStreamOutcome::Found(
                NetworkedDfsRangeReadStream {
                    data: DfsReadRangeData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(expected_hash.clone()),
                        offset: range_offset,
                        total_size_bytes: manifest.size_bytes,
                        bytes: Vec::new(),
                    },
                    range_size_bytes,
                    reader,
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
        self.read_object_metadata_local_range_contents(object, offset, max_bytes, allow_stale)
    }

    /// Read bytes for a descriptor whose admission the caller already checked.
    /// Namespace callers apply legacy conflict handling above. Explicit version
    /// reads bind an authorized immutable identity and disable stale fallback.
    pub(crate) fn read_object_metadata_local_range_contents(
        &self,
        object: &DfsObjectMetadata,
        offset: u64,
        max_bytes: usize,
        allow_stale: bool,
    ) -> Result<NetworkedDfsRangeReadOutcome, DfsError> {
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

        if let (Some(expected_hash), Some(expected_size)) =
            (object.content_hash.as_ref(), object.content_size_bytes)
            && let Ok(Some((manifest, bytes))) = self.content.read_object_content_range(
                object.object_id.as_str(),
                expected_hash,
                expected_size,
                offset,
                max_bytes,
            )
        {
            return Ok(NetworkedDfsRangeReadOutcome::Found {
                data: DfsReadRangeData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(expected_hash.clone()),
                    offset: offset.min(manifest.size_bytes),
                    total_size_bytes: manifest.size_bytes,
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
                        version: DfsVersion::new(manifest.version.clone()).map_err(|error| {
                            DfsError::ContentStore {
                                message: error.to_string(),
                            }
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

    pub(crate) fn pending_read_metadata_advanced(
        &self,
        pending: &PendingNetworkedRead,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version != pending.version
            || object.content_hash != pending.content_hash
            || object.content_size_bytes != pending.content_size_bytes)
    }

    pub(crate) fn pending_read_source_still_current(
        &self,
        pending: &PendingNetworkedRead,
        source_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version == pending.version
            && object.content_hash == pending.content_hash
            && object.content_size_bytes == pending.content_size_bytes
            && object.current_locations.contains(source_node_id))
    }

    pub(crate) fn clear_pending_remote_reads_for_object_version(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
    ) {
        let request_ids: Vec<String> = self
            .reads
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == *object_id
                    && pending.version == *version
                    && pending.content_hash == *content_hash
            })
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in request_ids {
            self.reads.pending_reads.remove(&request_id);
        }
    }

    pub(crate) fn store_remote_read_data(
        &mut self,
        _source_node_id: &NodeId,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let (expected_hash, size_bytes) = self.validate_remote_read_data(data)?;
        self.store_command_bytes(&data.object_id, &data.version, &expected_hash, &data.bytes)?;
        self.store_remote_read_manifest(data, size_bytes)
    }

    pub(crate) fn validate_remote_read_data(
        &self,
        data: &DfsReadData,
    ) -> Result<(String, u64), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        self.validate_remote_read_data_against(
            data,
            &object.object_id,
            &object.version,
            object.content_hash.as_deref(),
            object.content_size_bytes,
            "object metadata",
        )
    }

    pub(crate) fn validate_remote_read_data_for_pending(
        &self,
        pending: &PendingNetworkedRead,
        data: &DfsReadData,
    ) -> Result<(String, u64), DfsError> {
        self.validate_remote_read_data_against(
            data,
            &pending.object_id,
            &pending.version,
            pending.content_hash.as_deref(),
            pending.content_size_bytes,
            "pending metadata",
        )
    }

    fn validate_remote_read_data_against(
        &self,
        data: &DfsReadData,
        expected_object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: Option<&str>,
        expected_size_bytes: Option<u64>,
        size_context: &str,
    ) -> Result<(String, u64), DfsError> {
        if expected_object_id != &data.object_id {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_object_id.to_string(),
                actual: data.object_id.to_string(),
            });
        }
        if expected_version != &data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = expected_content_hash else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash.clone(),
            });
        }
        let size_bytes = data.bytes.len() as u64;
        if let Some(expected_size) = expected_size_bytes
            && expected_size != size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote read size does not match {size_context}: expected {expected_size}, actual {size_bytes}"
                ),
            });
        }
        Ok((expected_hash.to_string(), size_bytes))
    }

    pub(crate) fn store_remote_read_manifest(
        &mut self,
        data: &DfsReadData,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        if object.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: object.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = object.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        if let Some(expected_size) = object.content_size_bytes
            && expected_size != size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote read size does not match object metadata: expected {expected_size}, actual {size_bytes}"
                ),
            });
        }
        let mut object = object;
        object.content_size_bytes = Some(size_bytes);
        if self.is_local_metadata_primary_for_object(&object)? {
            self.put_object_version_size_in_metadata_backend(&object, Some(size_bytes))?;
        }
        self.metadata.add_object(object)?;
        self.clear_transient_remote_read_location_prunes_for_object(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(())
    }
}
