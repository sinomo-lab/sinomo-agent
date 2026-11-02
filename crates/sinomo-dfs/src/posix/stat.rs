//! Current stat is assembled exclusively from authenticated authority views.

use crate::posix_model::{DfsPosixError, DfsStat, Result};
use crate::posix_object::{object_mode_or_default, object_timestamp_or_default};
use crate::*;
use std::collections::BTreeSet;

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
        let mut shards = snapshot
            .ok_or_else(|| invalid("stat has no complete routing catalog"))?
            .to_vec();
        shards.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
        let shard = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &sifs_file_system_id(),
            &shards,
            path,
        )
        .map_err(DfsError::from)?
        .ok_or_else(|| invalid("stat path has no authority"))?;
        // Co-owned capture already consumes the existing read-round budget at
        // Main; never multiply it by wrapping that path in another retry loop.
        if shards
            .iter()
            .all(|candidate| candidate.primary_node_id == shard.primary_node_id)
        {
            return self.collect_stat_path_from_authorities(path, &shard, &shards);
        }
        self.with_stat_read_rounds(|service| {
            service.collect_stat_path_from_authorities(path, &shard, &shards)
        })
    }

    pub(super) fn with_stat_read_rounds(
        &mut self,
        mut collect: impl FnMut(&mut Self) -> Result<DfsStat>,
    ) -> Result<DfsStat> {
        let mut changed = None;
        for _ in 0..super::POSIX_ROUTED_SYNC_RETRY_ROUNDS {
            self.check_stat_deadline()?;
            match collect(self) {
                Err(
                    error @ DfsPosixError::Dfs(DfsError::MetadataRejection {
                        code: DfsMetadataRejectionCode::PrefixMismatch,
                        ..
                    }),
                ) => changed = Some(error),
                result => return result,
            }
        }
        Err(changed.unwrap_or_else(|| invalid("stat exhausted its existing read rounds")))
    }

    fn collect_stat_path_from_authorities(
        &mut self,
        path: &str,
        shard: &DfsMetadataShardConfig,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<DfsStat> {
        let request = DfsStatPathRequest {
            path: path.into(),
            read_id: self.next_request_id(),
            catalog_digest: crate::stat_projection::stat_catalog_digest(shards)?,
        };
        self.check_stat_deadline()?;
        let result = self.query_metadata_authority_from_snapshot(
            shard,
            DfsMetadataSelection::StatAtPathV1 {
                request: Box::new(request.clone()),
            },
            Some(shards),
        )?;
        let DfsMetadataQueryResult::StatAtPathV1 { view } = result else {
            return Err(invalid("authority does not support stat path v1"));
        };
        if view.request != request
            || view.integrity != view.integrity_digest()?
            || view.prefixes.iter().any(|cut| !cut.prefix.is_well_formed())
            || (!view.complete && view.link_count.is_some())
            || view.path_object.is_some() != view.object.is_some()
            || view.path_object.as_deref().is_some_and(|row| {
                row.file_system_id != sifs_file_system_id()
                    || row.path != path
                    || row.tombstone_provenance.is_some()
                    || row.kind == DfsObjectKind::Tombstone
            })
        {
            return Err(invalid(
                "stat path returned inconsistent or stale view evidence",
            ));
        }
        let expected: Vec<_> = if view.complete {
            shards
                .iter()
                .map(|s| DfsMetadataShardId::new(s.shard_id.clone()))
                .collect()
        } else {
            vec![DfsMetadataShardId::new(shard.shard_id.clone())]
        };
        let observed = view
            .prefixes
            .iter()
            .map(|cut| cut.shard_id.clone())
            .collect::<Vec<_>>();
        let all_local = shards
            .iter()
            .all(|s| s.primary_node_id == shard.primary_node_id);
        let incomplete_all_scopes = !view.complete
            && all_local
            && observed
                == shards
                    .iter()
                    .map(|s| DfsMetadataShardId::new(s.shard_id.clone()))
                    .collect::<Vec<_>>();
        if view.complete && !all_local || observed != expected && !incomplete_all_scopes {
            return Err(invalid("stat path omitted a required authority scope"));
        }
        if view.complete {
            self.validate_routed_namespace_response_authority(
                &shard.primary_node_id,
                Some(shard),
                shards,
            )?;
        }
        self.check_stat_deadline()?;
        self.namespace_execution.observe_path(
            &sifs_file_system_id(),
            path,
            view.path_object.as_deref(),
        );
        let object = view
            .object
            .ok_or_else(|| DfsPosixError::NotFound { path: path.into() })?;
        if object.file_system_id != sifs_file_system_id()
            || object.tombstone_provenance.is_some()
            || object.kind == DfsObjectKind::Tombstone
        {
            return Err(invalid("stat path returned a foreign or retired object"));
        }
        if view.complete {
            let count = view
                .link_count
                .ok_or_else(|| invalid("complete stat path omitted its count"))?;
            if object.kind == DfsObjectKind::Hardlink {
                return Err(invalid("stat path omitted canonical target"));
            }
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(&object),
            );
            return stat_from_projection(self, *object, path, count);
        }
        self.stat_object_from_authorities(&object, path, Some(shards))
    }

    pub(super) fn with_stat_deadline<R>(
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
        let mut target = initial.clone();
        // Preserve the existing sixteen-hop hardlink bound, without consulting
        // a client-local object table to locate the canonical inode.
        for _ in 0..16 {
            if target.kind != DfsObjectKind::Hardlink {
                break;
            }
            let id = target
                .link_target_object_id
                .as_ref()
                .ok_or_else(|| invalid("hardlink has no target identity"))?
                .clone();
            let mut found = None;
            for shard in &shards {
                self.check_stat_deadline()?;
                let result = self.query_metadata_authority_from_snapshot(
                    shard,
                    DfsMetadataSelection::StatObjectV1 {
                        object_id: id.clone(),
                    },
                    Some(&shards),
                )?;
                let DfsMetadataQueryResult::StatObjectV1 { object, prefix } = result else {
                    return Err(invalid("authority does not support stat object v1"));
                };
                if !prefix.is_well_formed() {
                    return Err(invalid("stat object has invalid generation"));
                }
                if let Some(row) = object {
                    if row.object_id != id {
                        return Err(invalid("stat object reply changed target identity"));
                    }
                    if owns(shard, &row)? {
                        self.namespace_execution.observe_path(
                            &row.file_system_id,
                            &row.path,
                            Some(&row),
                        );
                        if found.replace(*row).is_some() {
                            return Err(invalid("stat object has multiple authorities"));
                        }
                    }
                }
            }
            target = found.ok_or_else(|| invalid("hardlink target authority is unavailable"))?;
        }
        if matches!(
            target.kind,
            DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
        ) {
            return Err(invalid("stat target is not a live canonical object"));
        }
        let mut canonical = None;
        let mut identities = BTreeSet::new();
        let mut count = if target.kind == DfsObjectKind::Directory {
            2u64
        } else {
            1u64
        };
        let mut fences = Vec::new();
        // A directory's nlink counts only its immediate child directories.
        // Reuse the listing scope decision while retaining the full catalog
        // for ownership, query binding and the final generation comparison.
        // File aliases can live anywhere, so their existing global scan stays.
        let projection_shards = if target.kind == DfsObjectKind::Directory {
            super::routing::directory_child_authority_shards(&target.path, &shards)?
        } else {
            shards.iter().collect()
        };
        for shard in projection_shards {
            let mut request = DfsStatProjectionRequest {
                object_id: target.object_id.clone(),
                object_path: target.path.clone(),
                object_kind: target.kind.clone(),
                object_version: target.version.clone(),
                limit: crate::file_history_query::HISTORY_PAGE_LIMIT,
                after: None,
                expected_prefix: None,
                fence_only: false,
            };
            let mut original_object = None;
            loop {
                self.check_stat_deadline()?;
                let page = self.stat_projection_page(shard, &request, &shards)?;
                if request.expected_prefix.is_none() {
                    request.expected_prefix = Some(page.prefix.clone());
                    original_object = page.object.clone();
                } else if page.object != original_object {
                    return Err(invalid("stat object changed during pagination"));
                }
                if let Some(row) = page.object.as_deref()
                    && owns(shard, row)?
                {
                    if row.object_id != target.object_id
                        || row.path != target.path
                        || row.kind != target.kind
                    {
                        return Err(invalid("stat canonical object identity changed"));
                    }
                    if row.version == target.version && row.content_hash != target.content_hash {
                        return Err(invalid(
                            "stat content hash changed within one immutable version",
                        ));
                    }
                    if row.version != target.version {
                        // A genuine current-owner page can observe a publication
                        // after path resolution. Discard the whole collection;
                        // never combine its derived count with the older inode.
                        return Err(DfsError::MetadataRejection {
                            code: DfsMetadataRejectionCode::PrefixMismatch,
                            message: "stat canonical object generation changed".into(),
                        }
                        .into());
                    }
                    canonical = Some(row.clone());
                }
                for row in &page.entries {
                    if row.file_system_id != target.file_system_id {
                        return Err(invalid("stat projection contains another filesystem"));
                    }
                    if !owns(shard, row)? {
                        continue;
                    }
                    if row.kind == DfsObjectKind::Tombstone {
                        continue;
                    }
                    if !identities.insert(row.object_id.clone()) {
                        return Err(invalid("stat projection repeated a namespace identity"));
                    }
                    let contributes = if target.kind == DfsObjectKind::Directory {
                        if crate::posix_path::parent_posix_path(&row.path).as_deref()
                            != Some(&target.path)
                        {
                            return Err(invalid(
                                "stat directory projection contains a foreign child",
                            ));
                        }
                        row.kind == DfsObjectKind::Directory
                    } else {
                        if row.kind != DfsObjectKind::Hardlink
                            || row.link_target_object_id.as_ref() != Some(&target.object_id)
                        {
                            return Err(invalid("stat projection contains a foreign alias"));
                        }
                        true
                    };
                    if contributes {
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| invalid("stat link count overflow"))?;
                    }
                }
                request.after = page.after;
                if request.after.is_none() {
                    break;
                }
            }
            request.fence_only = true;
            fences.push((shard, request, original_object));
        }
        for (shard, request, object) in fences {
            self.check_stat_deadline()?;
            let page = self.stat_projection_page(shard, &request, &shards)?;
            if page.object != object {
                return Err(invalid("stat object changed before view validation"));
            }
        }
        self.check_stat_deadline()?;
        if self.metadata_shard_authorities(&initial.file_system_id)? != shards {
            return Err(invalid("stat authority catalog changed during collection"));
        }
        let object =
            canonical.ok_or_else(|| invalid("stat canonical authority omitted its object"))?;
        self.namespace_execution
            .observe_path(&object.file_system_id, &object.path, Some(&object));
        stat_from_projection(self, object, path, count)
    }

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

fn stat_from_projection<T: DfsMessageTransport>(
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
