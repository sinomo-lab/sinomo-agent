//! Current stat is assembled exclusively from authenticated authority views.

use crate::posix_model::{DfsPosixError, DfsStat, Result};
use crate::posix_object::{object_mode_or_default, object_timestamp_or_default};
use crate::*;

fn invalid(message: &str) -> DfsPosixError {
    DfsError::Metadata {
        message: message.to_owned(),
    }
    .into()
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(super) fn stat_path_from_authorities(
        &mut self,
        path: &str,
        snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<DfsStat> {
        if self.control_plane.is_none() {
            let object = self
                .lookup_sifs_path(path)?
                .filter(|r| r.kind != DfsObjectKind::Tombstone)
                .ok_or_else(|| DfsPosixError::NotFound { path: path.into() })?;
            return self.stat_object_for_path(&object, path);
        }
        let shards = snapshot.ok_or_else(|| invalid("stat has no routing catalog"))?;
        let shard = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &sifs_file_system_id(),
            shards,
            path,
        )
        .map_err(DfsError::from)?
        .ok_or_else(|| invalid("stat path has no authority"))?;
        self.check_stat_deadline()?;
        let result = self.query_metadata_authority_from_snapshot(
            &shard,
            DfsMetadataSelection::StatAtPathV2 { path: path.into() },
            Some(shards),
        )?;
        let DfsMetadataQueryResult::StatAtPathV2 { object } = result else {
            return Err(invalid("authority does not support maintained-count stat"));
        };
        self.check_stat_deadline()?;
        if object.as_deref().is_some_and(|row| {
            row.file_system_id != shard.file_system_id
                || row.path != path
                || row.tombstone_provenance.is_some()
        }) {
            return Err(invalid("stat path returned a foreign namespace identity"));
        }
        self.namespace_execution
            .observe_path(&shard.file_system_id, path, object.as_deref());
        let object = object
            .filter(|row| row.kind != DfsObjectKind::Tombstone)
            .ok_or_else(|| DfsPosixError::NotFound { path: path.into() })?;
        if object.kind == DfsObjectKind::Hardlink {
            return self.stat_object_from_authorities(&object, path, Some(shards));
        }
        let count = maintained_link_count(&object)?;
        stat_from_projection(self, *object, path, count)
    }

    // Retained call boundary for callers that already hold an opened identity.
    // A current-owner metadata read does not retry unrelated shard generations.
    pub(super) fn with_stat_read(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<DfsStat>,
    ) -> Result<DfsStat> {
        self.check_stat_deadline()?;
        let result = read(self);
        self.check_stat_deadline()?;
        result
    }

    pub(crate) fn with_stat_deadline<R>(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<R>,
    ) -> Result<R> {
        let requested = std::time::Instant::now() + super::POSIX_READ_FRESHNESS_SYNC_TIMEOUT;
        let previous = self.transport.set_operation_deadline(Some(requested));
        self.transport
            .set_operation_deadline(Some(previous.map_or(requested, |old| old.min(requested))));
        let result = run(self);
        self.transport.set_operation_deadline(previous);
        result
    }

    fn check_stat_deadline(&self) -> Result<()> {
        self.check_cancelled("authoritative stat")?;
        if self
            .transport
            .operation_deadline()
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(invalid("authoritative stat deadline expired"));
        }
        Ok(())
    }

    pub(super) fn stat_object_from_authorities(
        &mut self,
        initial: &DfsObjectMetadata,
        path: &str,
        snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<DfsStat> {
        if self.control_plane.is_none() {
            return self.stat_object_for_path(initial, path);
        }
        let shards = match snapshot {
            Some(shards) => shards.to_vec(),
            None => self.metadata_shard_authorities(&initial.file_system_id)?,
        };
        let owns = |shard: &DfsMetadataShardConfig, row: &DfsObjectMetadata| -> Result<bool> {
            Ok(row.file_system_id == initial.file_system_id
                && row.tombstone_provenance.is_none()
                && crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &initial.file_system_id,
                    &shards,
                    &row.path,
                )
                .map_err(DfsError::from)?
                .as_ref()
                    == Some(shard))
        };
        if initial.kind == DfsObjectKind::Hardlink {
            self.check_stat_deadline()?;
            let target = self.resolve_hardlink_target_distributed(initial, Some(&shards))?;
            self.check_stat_deadline()?;
            let count = maintained_link_count(&target)?;
            return stat_from_projection(self, target, path, count);
        }
        let target = initial;
        if matches!(
            target.kind,
            DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
        ) {
            return Err(invalid("stat target is not a live canonical object"));
        }
        // The initial object may come from an opened path or write completion.
        // Read its current canonical owner, never an unrelated alias authority.
        let owner = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &target.file_system_id,
            &shards,
            &target.path,
        )
        .map_err(DfsError::from)?
        .ok_or_else(|| invalid("stat inode has no authority"))?;
        self.check_stat_deadline()?;
        let result = self.query_metadata_authority_from_snapshot(
            &owner,
            DfsMetadataSelection::StatObjectV1 {
                object_id: target.object_id.clone(),
            },
            Some(&shards),
        )?;
        let DfsMetadataQueryResult::StatObjectV1 {
            object: Some(object),
            prefix,
        } = result
        else {
            return Err(invalid("stat canonical owner omitted its object"));
        };
        if !prefix.is_well_formed()
            || !owns(&owner, &object)?
            || object.object_id != target.object_id
            || object.kind != target.kind
            || object.path != target.path
        {
            return Err(invalid(
                "stat canonical owner returned a different identity",
            ));
        }
        if object.version == target.version && object.content_hash != target.content_hash {
            return Err(invalid(
                "stat content hash changed within one immutable version",
            ));
        }
        self.check_stat_deadline()?;
        let count = maintained_link_count(&object)?;
        self.namespace_execution
            .observe_path(&object.file_system_id, &object.path, Some(&object));
        stat_from_projection(self, *object, path, count)
    }

    #[cfg(test)]
    fn stat_projection_page(
        &mut self,
        shard: &DfsMetadataShardConfig,
        request: &DfsStatProjectionRequest,
        catalog: &[DfsMetadataShardConfig],
    ) -> Result<DfsStatProjectionPage> {
        let result = self.query_metadata_authority_from_snapshot(
            shard,
            DfsMetadataSelection::StatProjectionV1 {
                request: Box::new(request.clone()),
            },
            Some(catalog),
        )?;
        let DfsMetadataQueryResult::StatProjectionV1 { page } = result else {
            return Err(invalid("authority does not support stat projection v1"));
        };
        if page.request != *request
            || !page.prefix.is_well_formed()
            || request
                .expected_prefix
                .as_ref()
                .is_some_and(|prefix| *prefix != page.prefix)
            || page.entries.len() > request.limit
            || page.complete != page.after.is_none()
            || (page.after.is_some() && (page.entries.is_empty() || page.after == request.after))
            || (request.fence_only && (!page.entries.is_empty() || page.after.is_some()))
        {
            return Err(invalid("invalid or incomplete authoritative stat view"));
        }
        Ok(*page)
    }
}

pub(super) fn maintained_link_count(object: &DfsObjectMetadata) -> Result<u64> {
    let count = object.namespace_link_count.ok_or_else(|| {
        invalid("namespace link count is uninitialized; explicit metadata maintenance is required")
    })?;
    if object.kind == DfsObjectKind::Hardlink
        || object.kind == DfsObjectKind::Tombstone
        || count == 0
        || (object.kind == DfsObjectKind::Directory && count < 2)
    {
        return Err(invalid("invalid canonical namespace link count"));
    }
    Ok(count)
}

pub(super) fn stat_from_projection<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
    mut object: DfsObjectMetadata,
    path: &str,
    count: u64,
) -> Result<DfsStat> {
    if matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink)
        && object.content_size_bytes.is_none()
        && let Some(hash) = object.content_hash.as_deref()
    {
        // The authority fences above already fixed this immutable identity.
        // Older metadata can omit its size while the exact content is local.
        object.content_size_bytes = service.local_object_version_manifest_size_if_matches(
            &object.object_id,
            &object.version,
            hash,
        );
    }
    Ok(DfsStat {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        path: crate::posix_path::normalize_posix_path(path)?,
        kind: object.kind.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink)
            .then_some(object.content_size_bytes)
            .flatten(),
        link_count: count,
        mode: object_mode_or_default(&object),
        uid: object.uid,
        gid: object.gid,
        accessed_at_unix_nanos: object_timestamp_or_default(object.accessed_at_unix_nanos),
        modified_at_unix_nanos: object_timestamp_or_default(object.modified_at_unix_nanos),
        created_at_unix_nanos: object_timestamp_or_default(object.created_at_unix_nanos),
        changed_at_unix_nanos: object_timestamp_or_default(object.changed_at_unix_nanos),
        policy: object.policy,
        current_locations: object.current_locations,
    })
}

#[cfg(test)]
mod native_tests;

#[cfg(test)]
mod size_projection_tests {
    use super::*;

    #[test]
    fn authority_stat_projection_preserves_legacy_size_from_verified_local_content() {
        // Algorithm/content DATA only: this old-shaped projection is not a
        // fabricated native publication or a grant of namespace authority.
        let root = sinomo_testdir::TestDir::new("stat-legacy-local-content-size");
        let node = NodeId::from_legacy_named("stat-legacy-content-owner").unwrap();
        let object_id = DfsObjectId::from_legacy_named("stat-legacy-content-object").unwrap();
        let version = DfsVersion::from_legacy_named("stat-legacy-content-version").unwrap();
        let content = LocalContentStore::new(root.path().join("content")).unwrap();
        let bytes = b"legacy content with no metadata size";
        let manifest = content
            .put_object_version_blob_only(object_id.as_str(), version.as_str(), bytes)
            .unwrap();
        let service = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
            content,
        );
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: sifs_file_system_id(),
            path: "/legacy-size".into(),
            namespace_link_count: (DfsObjectKind::File).initial_namespace_link_count(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node.clone(),
            current_locations: vec![node],
            content_hash: Some(manifest.content_hash.clone()),
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o600,
            uid: 1001,
            gid: 1002,
            accessed_at_unix_nanos: 11,
            modified_at_unix_nanos: 12,
            created_at_unix_nanos: 13,
            changed_at_unix_nanos: 14,
            version: version.clone(),
            policy: sinomo_core::DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let projected = stat_from_projection(&service, object.clone(), &object.path, 3).unwrap();
        assert_eq!(projected.size_bytes, Some(bytes.len() as u64));
        assert_eq!(projected.object_id, object_id);
        assert_eq!(projected.version, version);
        assert_eq!(projected.content_hash, object.content_hash);
        assert_eq!(projected.path, object.path);
        assert_eq!(projected.link_count, 3);
        assert_eq!(
            (projected.mode, projected.uid, projected.gid),
            (0o600, 1001, 1002)
        );
        assert!(
            service.metadata_backend.is_none(),
            "size lookup needs no native table attachment"
        );

        let mut known = object.clone();
        known.content_size_bytes = Some(7);
        assert_eq!(
            stat_from_projection(&service, known, &object.path, 3)
                .unwrap()
                .size_bytes,
            Some(7),
            "a known authority size is never replaced by a local hint",
        );
        let mut wrong_hash = object.clone();
        wrong_hash.content_hash = Some(format!("blake3:{}", blake3::hash(b"absent other content")));
        assert_eq!(
            stat_from_projection(&service, wrong_hash, &object.path, 3)
                .unwrap()
                .size_bytes,
            None,
            "the exact version's manifest cannot supply another hash's size",
        );
        let mut directory = object.clone();
        directory.kind = DfsObjectKind::Directory;
        assert_eq!(
            stat_from_projection(&service, directory, &object.path, 3)
                .unwrap()
                .size_bytes,
            None,
        );
        let mut symlink = object.clone();
        symlink.kind = DfsObjectKind::Symlink;
        assert_eq!(
            stat_from_projection(&service, symlink, &object.path, 3)
                .unwrap()
                .size_bytes,
            Some(bytes.len() as u64),
        );
        let empty_root = sinomo_testdir::TestDir::new("stat-legacy-content-unavailable");
        let unavailable = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(NodeId::generate()),
            LocalContentStore::new(empty_root.path().join("content")).unwrap(),
        );
        assert_eq!(
            stat_from_projection(&unavailable, object.clone(), &object.path, 3)
                .unwrap()
                .size_bytes,
            None,
            "missing local content remains unknown instead of becoming an empty file",
        );
    }
}
