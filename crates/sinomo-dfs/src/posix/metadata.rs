//! Shared POSIX metadata, path, and stat helpers.
//!
//! These helpers are the internal backbone for POSIX namespace code: path
//! resolution, metadata lookup, hardlink resolution, directory validation,
//! directory timestamp touches, and `DfsStat` projection. They stay private to
//! the POSIX facade so writes, reads, routed operations, jobs, and queries all
//! share the same interpretation without adding to the crate-level API.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::{POSIX_LINK_COUNT_PAGE_SIZE, POSIX_LIST_DIR_COLLECT_PAGE_SIZE, now_unix_nanos};
use crate::metadata_pages::try_walk_metadata_pages;
use crate::namespace::{metadata_path, next_version};
use crate::posix_model::{DfsPosixError, DfsStat, Result, read_conflict_error};
use crate::posix_object::{object_mode_or_default, object_timestamp_or_default};
use crate::posix_path::{
    append_posix_components, join_posix_path, normalize_posix_path,
    normalize_posix_path_with_trailing_directory, normalize_posix_walk_path, parent_posix_path,
    posix_path_has_dot_components, posix_path_is_at_or_under_prefix, resolve_posix_symlink_target,
};
use crate::{
    DfsDirectoryPage, DfsDirectoryPageCursor, DfsFileSystemId, DfsMessageTransport,
    DfsNamespaceOperation, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsReadData,
    NetworkedDfsReadOutcome, NetworkedDfsService, sifs_file_system_id,
};

#[cfg(test)]
std::thread_local! {
    static POSIX_METADATA_RAW_LOOKUP_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static POSIX_TRANSIENT_CHILD_CANDIDATE_VISIT_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
pub(super) struct ResolvedPosixPath {
    pub(super) path: String,
    pub(super) components: Vec<ResolvedPosixPathComponent>,
    terminal_object: Option<DfsObjectMetadata>,
}

#[derive(Debug)]
pub(super) struct ResolvedPosixPathComponent {
    pub(super) path: String,
    pub(super) object: Option<DfsObjectMetadata>,
}

#[cfg(test)]
pub(crate) fn reset_posix_metadata_raw_lookup_count() {
    POSIX_METADATA_RAW_LOOKUP_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn posix_metadata_raw_lookup_count() -> usize {
    POSIX_METADATA_RAW_LOOKUP_COUNT.get()
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_posix_metadata_full_directory_collect_count() {
    POSIX_METADATA_FULL_DIRECTORY_COLLECT_COUNT.set(0);
}

#[cfg(any(test, feature = "test-support"))]
pub fn posix_metadata_full_directory_collect_count() -> usize {
    POSIX_METADATA_FULL_DIRECTORY_COLLECT_COUNT.get()
}

#[cfg(test)]
pub(crate) fn reset_posix_transient_child_candidate_visit_count() {
    POSIX_TRANSIENT_CHILD_CANDIDATE_VISIT_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn posix_transient_child_candidate_visit_count() -> usize {
    POSIX_TRANSIENT_CHILD_CANDIDATE_VISIT_COUNT.get()
}

fn append_unobserved_components(
    observed: &mut Vec<ResolvedPosixPathComponent>,
    mut path: String,
    components: &[String],
) -> String {
    for component in components {
        path = join_posix_path(&path, component);
        observed.push(ResolvedPosixPathComponent {
            path: path.clone(),
            object: None,
        });
    }
    path
}

fn sifs_dir_direct_child_path(
    file_system_id: &DfsFileSystemId,
    path: &str,
    prefix: &str,
    object: &DfsObjectMetadata,
) -> Option<String> {
    if object.file_system_id != *file_system_id {
        return None;
    }
    let Ok(object_path) = normalize_posix_path(&object.path) else {
        return None;
    };
    if object_path == path || !object_path.starts_with(prefix) {
        return None;
    }
    let rest = object_path.trim_start_matches(prefix);
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(object_path)
}

fn compare_directory_page_cursors(
    left: &DfsDirectoryPageCursor,
    right: &DfsDirectoryPageCursor,
) -> Ordering {
    left.shard_id
        .cmp(&right.shard_id)
        .then_with(|| match (&left.dentry, &right.dentry) {
            (Some(left), Some(right)) => left
                .dirfrag_id
                .cmp(&right.dirfrag_id)
                .then_with(|| left.name_hash.cmp(&right.name_hash))
                .then_with(|| left.name.cmp(&right.name)),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        })
        .then_with(|| left.path.cmp(&right.path))
        .then_with(|| left.object_id.cmp(&right.object_id))
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn lookup_sifs_path(&self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        self.lookup_metadata_path(&sifs_file_system_id(), path)
    }

    /// Look up one component after a caller has already observed every parent
    /// component as live. The ordinary lookup must scan ancestors so an exact
    /// child record cannot escape a parent tombstone. A forward component walk
    /// has already established that invariant, so repeating the scan would
    /// turn resolving a depth-N path into O(N²) metadata lookups.
    pub(super) fn lookup_metadata_path_with_known_live_ancestors(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        known_live_ancestors: bool,
    ) -> Result<Option<DfsObjectMetadata>> {
        if known_live_ancestors {
            self.lookup_metadata_path_raw(file_system_id, path)
        } else {
            self.lookup_metadata_path(file_system_id, path)
        }
    }

    /// Look up one overwrite-preparation object without hydrating read
    /// locations from replica receipts.
    ///
    /// Bulk overwrite replaces `current_locations` with the local publisher
    /// before it constructs either metadata or receipt records. Avoid hydrating
    /// receipt-derived read locations that this write path cannot observe.
    /// Parent revalidation passes `false` to retain tombstoned-ancestor
    /// semantics; child lookups pass `true` only after that exact parent walk
    /// succeeded in the same pinned generation.
    pub(super) fn lookup_metadata_path_for_local_overwrite(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        known_live_ancestors: bool,
    ) -> Result<Option<DfsObjectMetadata>> {
        if !known_live_ancestors
            && self.has_tombstone_ancestor_without_receipt_locations(file_system_id, path)?
        {
            return Ok(None);
        }
        self.lookup_metadata_path_raw_without_receipt_locations(file_system_id, path)
    }

    fn lookup_metadata_path_raw_without_receipt_locations(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        #[cfg(test)]
        POSIX_METADATA_RAW_LOOKUP_COUNT
            .set(POSIX_METADATA_RAW_LOOKUP_COUNT.get().saturating_add(1));
        self.authoritative_metadata_object_for_path(file_system_id, path)
            .map_err(Into::into)
    }

    /// Resolve dot components before using an input as a metadata or routing key.
    /// Ordinary canonical paths retain their existing lookup-free fast path.
    pub(super) fn resolve_posix_dot_components(&self, path: &str) -> Result<String> {
        if posix_path_has_dot_components(path) {
            self.resolve_posix_path(path, false)
        } else {
            normalize_posix_path(path)
        }
    }

    pub(super) fn resolve_posix_dot_components_distributed(
        &mut self,
        path: &str,
    ) -> Result<String> {
        if posix_path_has_dot_components(path) {
            self.resolve_posix_path_distributed(path, false)
        } else {
            normalize_posix_path(path)
        }
    }

    pub(super) fn resolve_posix_path(&self, path: &str, follow_final: bool) -> Result<String> {
        let file_system_id = sifs_file_system_id();
        let mut current = normalize_posix_walk_path(path)?;
        for _ in 0..40 {
            let components = current
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut resolved = "/".to_string();
            let mut known_live_ancestors = false;
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let dot_component = matches!(component.as_str(), "." | "..");
                let candidate = match component.as_str() {
                    "." => resolved.clone(),
                    ".." => parent_posix_path(&resolved).unwrap_or_else(|| "/".to_string()),
                    _ => join_posix_path(&resolved, component),
                };
                let final_component = index + 1 == components.len();
                if final_component && !follow_final && !dot_component {
                    resolved = candidate;
                    continue;
                }
                let Some(object) = self.lookup_metadata_path_with_known_live_ancestors(
                    &file_system_id,
                    &candidate,
                    known_live_ancestors,
                )?
                else {
                    if components[index + 1..]
                        .iter()
                        .any(|part| matches!(part.as_str(), "." | ".."))
                    {
                        return Err(DfsPosixError::NotFound { path: candidate });
                    }
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                };
                if object.kind == DfsObjectKind::Tombstone {
                    if components[index + 1..]
                        .iter()
                        .any(|part| matches!(part.as_str(), "." | ".."))
                    {
                        return Err(DfsPosixError::NotFound { path: candidate });
                    }
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                }
                known_live_ancestors = true;
                let object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Symlink {
                    let target = self.symlink_target_for_object(&candidate, &object)?;
                    current = resolve_posix_symlink_target(&candidate, &target)?;
                    for rest in &components[index + 1..] {
                        current = join_posix_path(&current, rest);
                    }
                    current = normalize_posix_walk_path(&current)?;
                    restarted = true;
                    break;
                }
                if !final_component && object.kind != DfsObjectKind::Directory {
                    return Err(DfsPosixError::NotDirectory { path: candidate });
                }
                resolved = candidate;
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        })
    }

    pub(super) fn resolve_posix_path_distributed(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        Ok(self
            .resolve_posix_path_distributed_walk(path, follow_final, false)?
            .path)
    }

    /// Resolve a path while retaining the exact terminal metadata observed by
    /// the final post-symlink walk. The returned object has already followed a
    /// hardlink, matching the object that subsequent POSIX kind checks use.
    pub(super) fn resolve_posix_path_distributed_with_terminal_object(
        &mut self,
        path: &str,
    ) -> Result<(String, Option<DfsObjectMetadata>)> {
        let mut resolved = self.resolve_posix_path_distributed_walk(path, true, false)?;
        // The component walker has no component to observe for the filesystem
        // root, so retain that one exact lookup here.
        if resolved.path == "/" {
            resolved.terminal_object = self
                .lookup_sifs_path("/")?
                .map(|object| self.resolve_hardlink_target(&object))
                .transpose()?;
        }
        Ok((resolved.path, resolved.terminal_object))
    }

    /// Resolve a path and retain the exact component metadata observed during
    /// the final (post-symlink) walk. `mkdir_all` consumes this result directly
    /// instead of resolving the path and then looking up the same chain again.
    pub(super) fn resolve_posix_path_distributed_for_mkdir_all(
        &mut self,
        path: &str,
    ) -> Result<ResolvedPosixPath> {
        self.resolve_posix_path_distributed_walk(path, true, true)
    }

    fn resolve_posix_path_distributed_walk(
        &mut self,
        path: &str,
        follow_final: bool,
        capture_components: bool,
    ) -> Result<ResolvedPosixPath> {
        let file_system_id = sifs_file_system_id();
        let mut current = normalize_posix_walk_path(path)?;
        let mut followed_symlink = None::<String>;
        let resolve_authoritatively = posix_path_has_dot_components(path);
        let mut target_shard_snapshot = if resolve_authoritatively {
            self.refresh_sifs_metadata_shards()?
        } else {
            None
        };
        for _ in 0..40 {
            let components = current
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut resolved = "/".to_string();
            let mut known_live_ancestors = false;
            let mut observed = capture_components.then(|| Vec::with_capacity(components.len()));
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let dot_component = matches!(component.as_str(), "." | "..");
                let candidate = match component.as_str() {
                    "." => resolved.clone(),
                    ".." => parent_posix_path(&resolved).unwrap_or_else(|| "/".to_string()),
                    _ => join_posix_path(&resolved, component),
                };
                let final_component = index + 1 == components.len();
                if final_component && !follow_final && !dot_component {
                    resolved = candidate;
                    continue;
                }
                // Ordinary component walks remain local: callers commonly
                // resolve a not-yet-created final path before routing its
                // mutation, and an authority lookup here would add a second
                // namespace request that their primary pump cannot service.
                // Once a symlink redirects the walk, however, the target may
                // live on a different shard that this node does not mirror.
                let object = if let Some(link_path) = followed_symlink.as_deref() {
                    match self.lookup_sifs_path_direct_distributed_synced_from_snapshot(
                        &candidate,
                        target_shard_snapshot.as_deref(),
                    ) {
                        Ok(object) => object,
                        Err(error) => {
                            tracing::warn!(
                                target: "sinomo.dfs.posix",
                                symlink_path = %link_path,
                                target_path = %candidate,
                                stage = "resolve-target-metadata",
                                error = %error,
                                "distributed symlink follow could not resolve target metadata authority"
                            );
                            return Err(error);
                        }
                    }
                } else if resolve_authoritatively {
                    self.lookup_sifs_path_direct_distributed_synced_from_snapshot(
                        &candidate,
                        target_shard_snapshot.as_deref(),
                    )?
                } else {
                    self.lookup_metadata_path_with_known_live_ancestors(
                        &file_system_id,
                        &candidate,
                        known_live_ancestors,
                    )?
                };
                let Some(object) = object else {
                    if components[index + 1..]
                        .iter()
                        .any(|part| matches!(part.as_str(), "." | ".."))
                    {
                        return Err(DfsPosixError::NotFound { path: candidate });
                    }
                    if let Some(link_path) = followed_symlink.as_deref() {
                        tracing::debug!(
                            target: "sinomo.dfs.posix",
                            symlink_path = %link_path,
                            target_path = %candidate,
                            stage = "resolve-target-metadata",
                            outcome = "not-found",
                            "distributed symlink target is authoritatively absent"
                        );
                    }
                    if let Some(observed) = observed.as_mut() {
                        observed.push(ResolvedPosixPathComponent {
                            path: candidate.clone(),
                            object: None,
                        });
                        resolved = append_unobserved_components(
                            observed,
                            candidate,
                            &components[index + 1..],
                        );
                    } else {
                        resolved = append_posix_components(candidate, &components[index + 1..])?;
                    }
                    return Ok(ResolvedPosixPath {
                        path: resolved,
                        components: observed.unwrap_or_default(),
                        terminal_object: None,
                    });
                };
                if object.kind == DfsObjectKind::Tombstone {
                    if components[index + 1..]
                        .iter()
                        .any(|part| matches!(part.as_str(), "." | ".."))
                    {
                        return Err(DfsPosixError::NotFound { path: candidate });
                    }
                    let terminal_object = final_component.then(|| object.clone());
                    if let Some(observed) = observed.as_mut() {
                        observed.push(ResolvedPosixPathComponent {
                            path: candidate.clone(),
                            object: Some(object),
                        });
                        resolved = append_unobserved_components(
                            observed,
                            candidate,
                            &components[index + 1..],
                        );
                    } else {
                        resolved = append_posix_components(candidate, &components[index + 1..])?;
                    }
                    return Ok(ResolvedPosixPath {
                        path: resolved,
                        components: observed.unwrap_or_default(),
                        terminal_object,
                    });
                }
                if let Some(observed) = observed.as_mut() {
                    if dot_component {
                        observed.retain(|entry: &ResolvedPosixPathComponent| {
                            entry.path != candidate
                                && posix_path_is_at_or_under_prefix(&candidate, &entry.path)
                        });
                    }
                    observed.push(ResolvedPosixPathComponent {
                        path: candidate.clone(),
                        object: Some(object.clone()),
                    });
                }
                known_live_ancestors = true;
                let object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Symlink {
                    let target = self.symlink_target_for_object_distributed(&candidate, &object)?;
                    followed_symlink = Some(candidate.clone());
                    target_shard_snapshot = self.refresh_sifs_metadata_shards()?;
                    current = resolve_posix_symlink_target(&candidate, &target)?;
                    for rest in &components[index + 1..] {
                        current = join_posix_path(&current, rest);
                    }
                    current = normalize_posix_walk_path(&current)?;
                    restarted = true;
                    break;
                }
                if !final_component && object.kind != DfsObjectKind::Directory {
                    return Err(DfsPosixError::NotDirectory { path: candidate });
                }
                let terminal_object = final_component.then_some(object);
                resolved = candidate;
                if terminal_object.is_some() {
                    return Ok(ResolvedPosixPath {
                        path: resolved,
                        components: observed.unwrap_or_default(),
                        terminal_object,
                    });
                }
            }
            if !restarted {
                return Ok(ResolvedPosixPath {
                    path: resolved,
                    components: observed.unwrap_or_default(),
                    terminal_object: None,
                });
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        })
    }

    /// Resolve every symlink in a POSIX path against authoritative distributed
    /// metadata. Security boundaries use this before authorizing a path prefix;
    /// checking the caller-provided spelling alone is insufficient.
    pub fn posix_resolve_path_distributed(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.resolve_posix_path_distributed(path, follow_final)
    }

    pub(super) fn resolve_existing_posix_path(
        &self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    pub(super) fn resolve_existing_posix_path_distributed(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path =
            self.resolve_posix_path_distributed(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    fn symlink_target_for_object(&self, path: &str, object: &DfsObjectMetadata) -> Result<String> {
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: path.to_string(),
            });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path.to_string(), conflicts))
            }
        }
    }

    fn symlink_target_for_object_distributed(
        &mut self,
        path: &str,
        object: &DfsObjectMetadata,
    ) -> Result<String> {
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: path.to_string(),
            });
        }
        let outcome = self.read_object_metadata_or_fetch_remote_without_cache(object)?;
        self.symlink_target_from_distributed_read_outcome(path, outcome)
    }

    pub(super) fn symlink_target_from_distributed_read_outcome(
        &self,
        path: &str,
        outcome: NetworkedDfsReadOutcome,
    ) -> Result<String> {
        match outcome {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::PendingReplication {
                object_id,
                version,
                current_locations,
                ..
            } => {
                let detail = format!(
                    "replication pending for object {object_id} version {version} at {current_locations:?}"
                );
                tracing::debug!(
                    target: "sinomo.dfs.posix",
                    symlink_path = %path,
                    object_id = %object_id,
                    version = %version,
                    current_locations = ?current_locations,
                    stage = "fetch-link-body",
                    outcome = "pending-replication",
                    "distributed symlink follow is waiting for link content"
                );
                Err(DfsPosixError::SymlinkContentPending {
                    path: path.to_string(),
                    stage: "fetch-link-body".to_string(),
                    detail,
                })
            }
            NetworkedDfsReadOutcome::NotConfirmed {
                object_id,
                version,
                confirmed_locations,
                required_locations,
            } => {
                let detail = format!(
                    "object {object_id} version {version} is confirmed at {confirmed_locations:?}, required {required_locations:?}"
                );
                tracing::debug!(
                    target: "sinomo.dfs.posix",
                    symlink_path = %path,
                    object_id = %object_id,
                    version = %version,
                    confirmed_locations = ?confirmed_locations,
                    required_locations = ?required_locations,
                    stage = "fetch-link-body",
                    outcome = "not-confirmed",
                    "distributed symlink follow is waiting for confirmed link content"
                );
                Err(DfsPosixError::SymlinkContentPending {
                    path: path.to_string(),
                    stage: "fetch-link-body".to_string(),
                    detail,
                })
            }
            NetworkedDfsReadOutcome::NotFound => {
                tracing::warn!(
                    target: "sinomo.dfs.posix",
                    symlink_path = %path,
                    stage = "fetch-link-body",
                    outcome = "content-not-found",
                    "authoritative symlink metadata has no readable link content"
                );
                Err(DfsPosixError::SymlinkContentPending {
                    path: path.to_string(),
                    stage: "fetch-link-body".to_string(),
                    detail: "authoritative symlink metadata has no readable content location"
                        .to_string(),
                })
            }
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path.to_string(), conflicts))
            }
        }
    }

    pub(super) fn lookup_metadata_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if self.has_tombstone_ancestor(file_system_id, path)? {
            return Ok(None);
        }
        self.lookup_metadata_path_raw(file_system_id, path)
    }

    pub(super) fn lookup_metadata_path_raw(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        #[cfg(test)]
        POSIX_METADATA_RAW_LOOKUP_COUNT
            .set(POSIX_METADATA_RAW_LOOKUP_COUNT.get().saturating_add(1));
        let cache_active = self.metadata_lookup_cache_is_active();
        let normalized_metadata_path = metadata_path(path);
        let cache_key = (file_system_id.clone(), normalized_metadata_path.clone());
        if cache_active
            && let Ok(cache) = self.metadata_lookup_cache.lock()
            && let Some(cached) = cache.get(&cache_key)
        {
            if cached.as_ref().is_none_or(|object| {
                !self
                    .transient_metadata_object_ids
                    .contains(&object.object_id)
            }) {
                self.namespace_execution
                    .observe_path(file_system_id, path, cached.as_ref());
            }
            return Ok(cached.clone());
        }

        let object = self
            .authoritative_metadata_object_for_path(file_system_id, path)
            .map_err(DfsPosixError::from)?
            .map(|object| self.object_metadata_with_receipt_locations(object))
            .transpose()
            .map_err(DfsPosixError::from)?;
        if cache_active && let Ok(mut cache) = self.metadata_lookup_cache.lock() {
            cache.insert(cache_key, object.clone());
        }
        Ok(object)
    }

    fn has_tombstone_ancestor(&self, file_system_id: &DfsFileSystemId, path: &str) -> Result<bool> {
        let path = normalize_posix_path(path)?;
        if path == "/" {
            return Ok(false);
        }
        let mut parent = path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("/");
        if parent.is_empty() {
            parent = "/";
        }
        loop {
            match self.lookup_metadata_path_raw(file_system_id, parent) {
                Ok(Some(object)) if object.kind == DfsObjectKind::Tombstone => return Ok(true),
                Ok(_) => {}
                Err(DfsPosixError::Metadata(
                    crate::metadata_error::MetadataBackendError::UnknownFileSystem { .. },
                )) => {
                    // A node may own or replicate a nested path-prefix shard
                    // without opening its ancestor shard. Crossing that local
                    // shard boundary is not evidence of a tombstoned
                    // ancestor; namespace moves/deletes are fenced when the
                    // shard journal is authored. The exact target lookup still
                    // fails closed below if its own shard is unavailable.
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
            if parent == "/" {
                return Ok(false);
            }
            parent = parent
                .rsplit_once('/')
                .map(|(ancestor, _)| if ancestor.is_empty() { "/" } else { ancestor })
                .unwrap_or("/");
        }
    }

    fn has_tombstone_ancestor_without_receipt_locations(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<bool> {
        let path = normalize_posix_path(path)?;
        if path == "/" {
            return Ok(false);
        }
        let mut parent = path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("/");
        if parent.is_empty() {
            parent = "/";
        }
        loop {
            match self.lookup_metadata_path_raw_without_receipt_locations(file_system_id, parent) {
                Ok(Some(object)) if object.kind == DfsObjectKind::Tombstone => return Ok(true),
                Ok(_) => {}
                Err(DfsPosixError::Metadata(
                    crate::metadata_error::MetadataBackendError::UnknownFileSystem { .. },
                )) => return Ok(false),
                Err(error) => return Err(error),
            }
            if parent == "/" {
                return Ok(false);
            }
            parent = parent
                .rsplit_once('/')
                .map(|(ancestor, _)| if ancestor.is_empty() { "/" } else { ancestor })
                .unwrap_or("/");
        }
    }

    pub(crate) fn child_objects_for_sifs_dir(&self, path: &str) -> Result<Vec<DfsObjectMetadata>> {
        #[cfg(any(test, feature = "test-support"))]
        POSIX_METADATA_FULL_DIRECTORY_COLLECT_COUNT.set(
            POSIX_METADATA_FULL_DIRECTORY_COLLECT_COUNT
                .get()
                .saturating_add(1),
        );
        let file_system_id = sifs_file_system_id();
        let mut children = Vec::new();
        if let Some(backend) = self.metadata_backend.as_ref() {
            try_walk_metadata_pages(|cursor| {
                let page = backend.child_objects_for_directory_page(
                    &file_system_id,
                    path,
                    cursor,
                    POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
                )?;
                children.extend(page.objects);
                Ok::<_, DfsPosixError>(page.next_cursor)
            })?;
        }
        self.merge_memory_child_overlay_for_sifs_dir(&file_system_id, path, &mut children)?;
        for object in &children {
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(object),
            );
        }
        Ok(children)
    }

    fn merge_memory_child_overlay_for_sifs_dir(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        children: &mut Vec<DfsObjectMetadata>,
    ) -> Result<()> {
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        let mut overlays = BTreeMap::<String, DfsObjectMetadata>::new();
        for ((tombstone_file_system_id, _), tombstone) in &self.tombstones_by_metadata_path {
            if tombstone_file_system_id != file_system_id {
                continue;
            }
            let Some(tombstone_path) =
                sifs_dir_direct_child_path(file_system_id, path, &prefix, tombstone)
            else {
                continue;
            };
            overlays.insert(tombstone_path, tombstone.clone());
        }
        for object in self.metadata.objects.values() {
            let Some(object_path) =
                sifs_dir_direct_child_path(file_system_id, path, &prefix, object)
            else {
                continue;
            };
            let replace = overlays.get(&object_path).is_none_or(|existing| {
                existing.version < object.version
                    || (existing.version == object.version
                        && existing.kind == DfsObjectKind::Tombstone
                        && object.kind != DfsObjectKind::Tombstone)
            });
            if replace {
                overlays.insert(object_path, object.clone());
            }
        }

        let mut children_by_path = BTreeMap::<String, DfsObjectMetadata>::new();
        for object in children.drain(..) {
            let object_path = normalize_posix_path(&object.path)?;
            let replace = children_by_path
                .get(&object_path)
                .is_none_or(|existing| existing.version < object.version);
            if replace {
                children_by_path.insert(object_path, object);
            }
        }
        for (object_path, overlay) in overlays {
            let backend_object = self
                .metadata_backend
                .as_ref()
                .map(|backend| {
                    backend.get_object_by_path(file_system_id, &metadata_path(&object_path))
                })
                .transpose()?
                .flatten();
            let authoritative = match backend_object {
                Some(backend_object)
                    if self
                        .transient_metadata_object_ids
                        .contains(&overlay.object_id)
                        || self.local_write_view_overrides_backend(
                            &overlay.object_id,
                            &overlay,
                            Some(&backend_object),
                        ) =>
                {
                    overlay
                }
                Some(backend_object) => backend_object,
                None => overlay,
            };
            match authoritative.kind {
                DfsObjectKind::Tombstone => {
                    children_by_path.remove(&object_path);
                }
                _ => {
                    children_by_path.insert(object_path, authoritative);
                }
            }
        }
        children.extend(children_by_path.into_values());
        Ok(())
    }

    fn has_transient_child_overlay_for_sifs_dir(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> bool {
        self.transient_metadata_object_ids
            .child_object_ids(file_system_id, path)
            .is_some_and(|object_ids| !object_ids.is_empty())
    }

    pub(super) fn child_objects_for_sifs_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let page = self.child_objects_for_sifs_dir_page_unobserved(path, after, max_entries)?;
        for object in &page.objects {
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(object),
            );
        }
        Ok(page)
    }

    fn child_objects_for_sifs_dir_page_unobserved(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let file_system_id = sifs_file_system_id();
        let path_cursor_active =
            after.is_some_and(|cursor| cursor.dentry.is_none() && cursor.shard_id.is_none());
        let needs_overlay_merge =
            self.has_transient_child_overlay_for_sifs_dir(&file_system_id, path);
        // Preserve legacy path-only cursors emitted by older processes during
        // a rolling upgrade. New backed walks never emit this mode, including
        // when they merge transient namespace entries.
        if self.metadata_backend.is_none() || path_cursor_active {
            let mut objects = self.child_objects_for_sifs_dir(path)?;
            objects.sort_by(|left, right| {
                left.path
                    .cmp(&right.path)
                    .then_with(|| left.object_id.cmp(&right.object_id))
            });
            if let Some(after) = after {
                objects.retain(|object| {
                    (&object.path, &object.object_id) > (&after.path, &after.object_id)
                });
            }
            let has_more = objects.len() > max_entries;
            objects.truncate(max_entries);
            let next_cursor = has_more.then(|| {
                let object = objects
                    .last()
                    .expect("a truncated directory page has a last object");
                DfsDirectoryPageCursor {
                    path: object.path.clone(),
                    object_id: object.object_id.clone(),
                    dentry: None,
                    shard_id: None,
                }
            });
            return Ok(DfsDirectoryPage {
                objects,
                next_cursor,
            });
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .expect("backend cursor path requires a metadata backend");

        // Missing transient children do not have a durable dentry yet. Derive
        // the exact cursor the backend would assign and merge only this bounded
        // in-memory set into the indexed stream. A live durable entry for the
        // same path will be visited below and receives its overlay there.
        let mut entries = Vec::<(DfsDirectoryPageCursor, DfsObjectMetadata)>::new();
        if needs_overlay_merge {
            let prefix = if path == "/" {
                "/".to_string()
            } else {
                format!("{}/", path.trim_end_matches('/'))
            };
            let mut transient_by_path = BTreeMap::<String, DfsObjectMetadata>::new();
            for object_id in self
                .transient_metadata_object_ids
                .child_object_ids(&file_system_id, path)
                .into_iter()
                .flatten()
            {
                #[cfg(test)]
                POSIX_TRANSIENT_CHILD_CANDIDATE_VISIT_COUNT.set(
                    POSIX_TRANSIENT_CHILD_CANDIDATE_VISIT_COUNT
                        .get()
                        .saturating_add(1),
                );
                let Some(object) = self.metadata.object(object_id) else {
                    continue;
                };
                let Some(object_path) =
                    sifs_dir_direct_child_path(&file_system_id, path, &prefix, object)
                else {
                    continue;
                };
                let replace = transient_by_path.get(&object_path).is_none_or(|existing| {
                    existing.version < object.version
                        || (existing.version == object.version
                            && existing.kind == DfsObjectKind::Tombstone
                            && object.kind != DfsObjectKind::Tombstone)
                });
                if replace {
                    transient_by_path.insert(object_path, object.clone());
                }
            }
            for (object_path, overlay) in transient_by_path {
                let backend_object =
                    backend.get_object_by_path(&file_system_id, &metadata_path(&object_path))?;
                let backend_will_emit = backend_object
                    .as_ref()
                    .is_some_and(|object| object.kind != DfsObjectKind::Tombstone);
                let authoritative = match backend_object {
                    Some(backend_object)
                        if self
                            .transient_metadata_object_ids
                            .contains(&overlay.object_id)
                            || self.local_write_view_overrides_backend(
                                &overlay.object_id,
                                &overlay,
                                Some(&backend_object),
                            ) =>
                    {
                        overlay
                    }
                    Some(backend_object) => backend_object,
                    None => overlay,
                };
                if backend_will_emit || authoritative.kind == DfsObjectKind::Tombstone {
                    continue;
                }
                let cursor = backend.directory_page_cursor_for_object(&authoritative)?;
                if after.is_none_or(|after| {
                    compare_directory_page_cursors(&cursor, after) == Ordering::Greater
                }) {
                    entries.push((cursor, authoritative));
                }
            }
        }

        // Preserve the backend's bounded dentry/shard cursor instead of
        // collecting, sorting, and truncating the complete directory. A
        // transient tombstone can remove durable entries, so advance bounded
        // backend pages until max_entries + 1 live candidates are available or
        // the backend is exhausted.
        let target_entries = max_entries.saturating_add(1);
        let mut cursor = after.cloned();
        let mut backend_entry_count = 0usize;
        let mut backend_exhausted = false;
        loop {
            let remaining = target_entries.saturating_sub(backend_entry_count);
            if remaining == 0 {
                break;
            }
            let (backend_entries, next_cursor) = backend.child_objects_for_directory_page_entries(
                &file_system_id,
                path,
                cursor.as_ref(),
                remaining,
            )?;
            for (backend_object, entry_cursor) in backend_entries {
                let object_path = normalize_posix_path(&backend_object.path)?;
                let tombstone_key = (file_system_id.clone(), metadata_path(&object_path));
                let mut memory_overlay = self
                    .tombstones_by_metadata_path
                    .get(&tombstone_key)
                    .cloned();
                if let Some(memory_object) = self
                    .metadata
                    .object_by_normalized_posix_path(&file_system_id, &object_path)
                {
                    let replace = memory_overlay.as_ref().is_none_or(|existing| {
                        existing.version < memory_object.version
                            || (existing.version == memory_object.version
                                && existing.kind == DfsObjectKind::Tombstone
                                && memory_object.kind != DfsObjectKind::Tombstone)
                    });
                    if replace {
                        memory_overlay = Some(memory_object.clone());
                    }
                }
                let authoritative = match memory_overlay {
                    Some(memory_overlay)
                        if self
                            .transient_metadata_object_ids
                            .contains(&memory_overlay.object_id)
                            || self.local_write_view_overrides_backend(
                                &memory_overlay.object_id,
                                &memory_overlay,
                                Some(&backend_object),
                            ) =>
                    {
                        memory_overlay
                    }
                    _ => backend_object,
                };
                if authoritative.kind != DfsObjectKind::Tombstone {
                    entries.push((entry_cursor, authoritative));
                    backend_entry_count = backend_entry_count.saturating_add(1);
                }
            }

            let Some(next_cursor) = next_cursor else {
                backend_exhausted = true;
                break;
            };
            if cursor.as_ref() == Some(&next_cursor) {
                backend_exhausted = true;
                break;
            }
            cursor = Some(next_cursor);
        }

        entries.sort_by(|(left_cursor, _), (right_cursor, _)| {
            compare_directory_page_cursors(left_cursor, right_cursor)
        });
        // A transient candidate may settle between its point lookup above and
        // the backend page snapshot. Stable sorting keeps the overlay first;
        // collapse the duplicate durable observation before page sizing.
        let mut deduplicated_entries = Vec::with_capacity(entries.len());
        for entry in entries {
            let duplicate = deduplicated_entries.last().is_some_and(
                |(cursor, object): &(DfsDirectoryPageCursor, DfsObjectMetadata)| {
                    cursor == &entry.0 && object.object_id == entry.1.object_id
                },
            );
            if !duplicate {
                deduplicated_entries.push(entry);
            }
        }
        let mut entries = deduplicated_entries;
        let has_more = entries.len() > max_entries || !backend_exhausted;
        entries.truncate(max_entries);
        let next_cursor = has_more
            .then(|| entries.last().map(|(cursor, _)| cursor.clone()))
            .flatten();
        Ok(DfsDirectoryPage {
            objects: entries.into_iter().map(|(_, object)| object).collect(),
            next_cursor,
        })
    }

    pub(super) fn sifs_dir_has_children(&self, path: &str) -> Result<bool> {
        Ok(!self
            .child_objects_for_sifs_dir_page(path, None, 1)?
            .objects
            .is_empty())
    }

    pub(super) fn hardlink_count_to_object(&self, target_object_id: &DfsObjectId) -> Result<usize> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.hardlink_count_to_object(&file_system_id, target_object_id)?);
        }
        Ok(self
            .metadata
            .objects
            .values()
            .filter(|candidate| {
                candidate.file_system_id == file_system_id
                    && candidate.kind == DfsObjectKind::Hardlink
                    && candidate.link_target_object_id.as_ref() == Some(target_object_id)
            })
            .count())
    }

    pub(super) fn first_hardlink_to_object(
        &self,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let result = self.first_hardlink_to_object_unobserved(target_object_id)?;
        if let Some(object) = result.as_ref() {
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(object),
            );
        }
        Ok(result)
    }

    fn first_hardlink_to_object_unobserved(
        &self,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.first_hardlink_to_object(&file_system_id, target_object_id)?);
        }
        let mut first = None::<DfsObjectMetadata>;
        for candidate in self.metadata.objects.values() {
            if candidate.file_system_id != file_system_id
                || candidate.kind != DfsObjectKind::Hardlink
                || candidate.link_target_object_id.as_ref() != Some(target_object_id)
            {
                continue;
            }
            if first.as_ref().is_none_or(|current| {
                candidate
                    .path
                    .cmp(&current.path)
                    .then_with(|| candidate.object_id.cmp(&current.object_id))
                    .is_lt()
            }) {
                first = Some(candidate.clone());
            }
        }
        Ok(first)
    }

    fn child_directory_count_for_sifs_dir(&self, path: &str) -> Result<u64> {
        let mut count = 0u64;
        try_walk_metadata_pages(|cursor| {
            let page =
                self.child_objects_for_sifs_dir_page(path, cursor, POSIX_LINK_COUNT_PAGE_SIZE)?;
            count = count.saturating_add(
                page.objects
                    .iter()
                    .filter(|child| child.kind == DfsObjectKind::Directory)
                    .count() as u64,
            );
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        Ok(count)
    }

    pub(crate) fn object_by_id(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let memory_object = self.metadata.object(object_id).cloned();
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            None
        };
        let object = match (memory_object.as_ref(), backend_object.as_ref()) {
            (None, None) => return Ok(None),
            _ => self.resolve_authoritative_object_metadata(
                object_id,
                memory_object.as_ref(),
                backend_object.as_ref(),
            )?,
        };
        if !self.transient_metadata_object_ids.contains(object_id) {
            self.namespace_execution.observe_path(
                &object.file_system_id,
                &object.path,
                Some(&object),
            );
        }
        self.object_metadata_with_receipt_locations(object)
            .map(Some)
            .map_err(Into::into)
    }

    pub(crate) fn resolve_hardlink_target(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectMetadata> {
        let mut current = object.clone();
        for _ in 0..16 {
            if current.kind != DfsObjectKind::Hardlink {
                return Ok(current);
            }
            let Some(target_id) = current.link_target_object_id.clone() else {
                return Err(DfsPosixError::NotFound { path: current.path });
            };
            current = self
                .object_by_id(&target_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: current.path.clone(),
                })?;
        }
        Err(DfsPosixError::InvalidPath {
            path: object.path.clone(),
        })
    }

    /// Stat for a path that a routed unlink/rmdir just tombstoned; the
    /// response journal batches were replayed before this runs, so the local
    /// lookup observes the authoritative tombstone like a local removal does.
    pub(super) fn stat_replayed_tombstone(&self, path: &str) -> Result<DfsStat> {
        let resolved = self.resolve_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&resolved)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: resolved.clone(),
            })?;
        self.stat_object(&object)
    }

    pub(super) fn stat_target_for_path(&self, path: &str) -> Result<DfsObjectMetadata> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        self.resolve_hardlink_target(&object)
    }

    pub(super) fn ensure_parent_directory(&self, path: &str) -> Result<()> {
        let Some(parent) = path.rsplit_once('/').map(|(parent, _)| parent) else {
            return Ok(());
        };
        let parent = if parent.is_empty() { "/" } else { parent };
        match self.lookup_sifs_path(parent)? {
            Some(object) if object.kind == DfsObjectKind::Directory => Ok(()),
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                Err(DfsPosixError::MissingParent {
                    path: parent.to_string(),
                })
            }
            Some(_) => Err(DfsPosixError::NotDirectory {
                path: parent.to_string(),
            }),
            None => Err(DfsPosixError::MissingParent {
                path: parent.to_string(),
            }),
        }
    }

    pub(super) fn ensure_resolved_path_is_directory(&self, path: &str) -> Result<()> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            Ok(())
        } else {
            Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            })
        }
    }

    pub(super) fn ensure_resolved_path_is_directory_no_follow(&self, path: &str) -> Result<()> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            Ok(())
        } else {
            Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            })
        }
    }

    pub(crate) fn touch_parent_directory_modified(&mut self, path: &str) -> Result<()> {
        let Some(parent) = parent_posix_path(path) else {
            return Ok(());
        };
        self.touch_directory_modified(&parent)
    }

    pub fn posix_touch_directory_modified(&mut self, path: &str) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            return self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::TouchDirectoryModified { path },
            );
        }
        self.touch_directory_modified(&path)
    }

    pub fn posix_touch_directory_modified_without_local_replay(
        &mut self,
        path: &str,
    ) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            return self.route_namespace_operation_without_local_replay(
                primary,
                DfsNamespaceOperation::TouchDirectoryModified { path },
            );
        }
        self.touch_directory_modified(&path)
    }

    pub(super) fn touch_directory_modified(&mut self, path: &str) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_independent_planning();
        self.clear_metadata_lookup_cache();
        let object = self.directory_modified_metadata(path)?;
        self.add_object_metadata(object)?;
        Ok(())
    }

    pub(super) fn directory_modified_metadata(&mut self, path: &str) -> Result<DfsObjectMetadata> {
        let mut object =
            self.lookup_sifs_path(path)?
                .ok_or_else(|| DfsPosixError::MissingParent {
                    path: path.to_string(),
                })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::MissingParent {
                path: path.to_string(),
            });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let now = now_unix_nanos()
            .max(object.modified_at_unix_nanos.saturating_add(1))
            .max(object.changed_at_unix_nanos.saturating_add(1));
        object.modified_at_unix_nanos = now;
        object.changed_at_unix_nanos = now;
        object.version = next_version(&object.version);
        Ok(object)
    }

    pub(super) fn stat_object(&self, object: &DfsObjectMetadata) -> Result<DfsStat> {
        self.stat_object_for_path(object, &object.path)
    }

    /// Project the namespace record itself instead of resolving the internal
    /// hardlink record to its target. This is intentionally separate from the
    /// POSIX stat projection: capability-scoped VFS users need to recognize
    /// every kind of link before following it.
    pub(super) fn namespace_lstat_object_for_path(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
    ) -> Result<DfsStat> {
        let size_bytes = self.object_content_size_hint(object)?;
        let link_count = self.link_count_for_object(object)?;
        Ok(DfsStat {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: normalize_posix_path(path)?,
            kind: object.kind.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes,
            link_count,
            mode: object_mode_or_default(object),
            uid: object.uid,
            gid: object.gid,
            accessed_at_unix_nanos: object_timestamp_or_default(object.accessed_at_unix_nanos),
            modified_at_unix_nanos: object_timestamp_or_default(object.modified_at_unix_nanos),
            created_at_unix_nanos: object_timestamp_or_default(object.created_at_unix_nanos),
            changed_at_unix_nanos: object_timestamp_or_default(object.changed_at_unix_nanos),
            policy: object.policy.clone(),
            current_locations: object.current_locations.clone(),
        })
    }

    pub(super) fn stat_object_for_path(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
    ) -> Result<DfsStat> {
        self.stat_object_for_path_with_link_count_hint(object, path, None)
    }

    pub(super) fn stat_object_for_path_with_link_count_hint(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
        link_count_hint: Option<u64>,
    ) -> Result<DfsStat> {
        let object = self.resolve_hardlink_target(object)?;
        let size_bytes = self.object_content_size_hint(&object)?;
        let link_count = match link_count_hint {
            Some(count)
                if object.kind != DfsObjectKind::Directory
                    && object.kind != DfsObjectKind::Hardlink =>
            {
                count
            }
            _ => self.link_count_for_object(&object)?,
        };
        Ok(DfsStat {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: normalize_posix_path(path)?,
            kind: object.kind.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes,
            link_count,
            mode: object_mode_or_default(&object),
            uid: object.uid,
            gid: object.gid,
            accessed_at_unix_nanos: object_timestamp_or_default(object.accessed_at_unix_nanos),
            modified_at_unix_nanos: object_timestamp_or_default(object.modified_at_unix_nanos),
            created_at_unix_nanos: object_timestamp_or_default(object.created_at_unix_nanos),
            changed_at_unix_nanos: object_timestamp_or_default(object.changed_at_unix_nanos),
            policy: object.policy.clone(),
            current_locations: object.current_locations.clone(),
        })
    }

    pub(super) fn link_count_for_object(&self, object: &DfsObjectMetadata) -> Result<u64> {
        if object.kind == DfsObjectKind::Hardlink {
            let target = self.resolve_hardlink_target(object)?;
            return self.link_count_for_object(&target);
        }
        if object.kind != DfsObjectKind::Directory {
            let count = self.hardlink_count_to_object(&object.object_id)?;
            return Ok(1u64.saturating_add(count as u64));
        }
        let path = normalize_posix_path(&object.path)?;
        let child_directory_count = self.child_directory_count_for_sifs_dir(&path)?;
        Ok(2u64.saturating_add(child_directory_count))
    }

    pub(super) fn first_hardlink_alias_outside_prefix(
        &self,
        object: &DfsObjectMetadata,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let result = self.first_hardlink_alias_outside_prefix_unobserved(object, prefix)?;
        if let Some(alias) = result.as_ref() {
            self.namespace_execution
                .observe_path(&alias.file_system_id, &alias.path, Some(alias));
        }
        Ok(result)
    }

    fn first_hardlink_alias_outside_prefix_unobserved(
        &self,
        object: &DfsObjectMetadata,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix)?;
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.first_hardlink_to_object_outside_prefix(
                &file_system_id,
                &object.object_id,
                &prefix,
            )?);
        }
        let mut first = None::<DfsObjectMetadata>;
        for candidate in self.metadata.objects.values() {
            if candidate.file_system_id != file_system_id
                || candidate.kind != DfsObjectKind::Hardlink
                || candidate.link_target_object_id.as_ref() != Some(&object.object_id)
            {
                continue;
            }
            let Ok(path) = normalize_posix_path(&candidate.path) else {
                continue;
            };
            if posix_path_is_at_or_under_prefix(&path, &prefix) {
                continue;
            }
            if first.as_ref().is_none_or(|current| {
                candidate
                    .path
                    .cmp(&current.path)
                    .then_with(|| candidate.object_id.cmp(&current.object_id))
                    .is_lt()
            }) {
                first = Some(candidate.clone());
            }
        }
        Ok(first)
    }
}

#[cfg(test)]
mod symlink_follow_tests {
    use super::*;
    use crate::{DfsVersion, LocalContentStore};
    use sinomo_core::network::InProcessNetwork;
    use sinomo_core::node::NodeId;

    #[test]
    fn transient_symlink_read_outcomes_remain_retryable_and_diagnostic() {
        let root = sinomo_testdir::TestDir::new("dfs-symlink-transient-read-outcomes");
        let node = NodeId::from_legacy_named("node_symlink_outcome_reader").expect("node");
        let service = NetworkedDfsService::new(
            InProcessNetwork::new().add_node(node.clone()),
            LocalContentStore::new(root.join("content")).expect("content store"),
        );
        let object_id = DfsObjectId::from_legacy_named("obj_symlink_outcome").expect("object id");
        let version = DfsVersion::from_legacy_named("version_symlink_outcome").expect("version");

        let pending = service
            .symlink_target_from_distributed_read_outcome(
                "/services/link",
                NetworkedDfsReadOutcome::PendingReplication {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: Some("blake3:pending".to_string()),
                    current_locations: vec![node.clone()],
                },
            )
            .expect_err("pending replication cannot resolve a symlink yet");
        assert!(
            matches!(
                &pending,
                DfsPosixError::SymlinkContentPending {
                    path,
                    stage,
                    detail,
                } if path == "/services/link"
                    && stage == "fetch-link-body"
                    && detail.contains("replication pending")
            ),
            "unexpected pending replication mapping: {pending:?}"
        );

        let not_confirmed = service
            .symlink_target_from_distributed_read_outcome(
                "/services/link",
                NetworkedDfsReadOutcome::NotConfirmed {
                    object_id,
                    version,
                    confirmed_locations: vec![node.clone()],
                    required_locations: vec![node],
                },
            )
            .expect_err("unconfirmed content cannot resolve a symlink yet");
        assert!(
            matches!(
                &not_confirmed,
                DfsPosixError::SymlinkContentPending {
                    path,
                    stage,
                    detail,
                } if path == "/services/link"
                    && stage == "fetch-link-body"
                    && detail.contains("is confirmed at")
            ),
            "unexpected not-confirmed mapping: {not_confirmed:?}"
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
std::thread_local! {
    static POSIX_METADATA_FULL_DIRECTORY_COLLECT_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
