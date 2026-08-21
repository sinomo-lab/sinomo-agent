use sinomo_core::kv::{KvEntry, KvRevision};
use std::collections::BTreeMap;

/// Whether a metadata prefix probe is a non-mutating verification request.
/// Exact validation lives with DFS protocol semantics so hosts cannot disagree
/// about which background probes may avoid foreground-maintenance preemption.
pub fn dfs_metadata_prefix_probe_is_verification_only(
    command: &crate::DfsMetadataJournalBatchCommand,
    authenticated_source_node_id: &sinomo_core::node::NodeId,
    local_node_id: &sinomo_core::node::NodeId,
) -> bool {
    let Some(prefix_probe) = command.prefix_probe.as_ref() else {
        return false;
    };
    command.verify_prefix_only
        && command.source_node_id.as_ref() == Some(authenticated_source_node_id)
        && prefix_probe.primary_handoff_id.is_none()
        && !prefix_probe.barrier_id.is_empty()
        && !prefix_probe.state_fingerprint.is_empty()
        && command.file_system_id.as_ref() == prefix_probe.file_system_id.as_ref()
        && command.file_system_id.is_some()
        && command.authority_epoch == prefix_probe.authority_epoch
        && command.authority_epoch.is_some()
        && command.batch.shard_id == prefix_probe.shard_id
        && command.batch.next_after_sequence == prefix_probe.journal_sequence
        && command.request_id
            == format!(
                "dfs_meta_probe_{}_{}_{}",
                authenticated_source_node_id, local_node_id, prefix_probe.barrier_id
            )
        && command.authority_probe.is_none()
        && command.durable_replication.is_none()
        && command.batch.entries.is_empty()
}

pub const DFS_AUTHORITY_PROJECTION_MAX_SCOPES: usize =
    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub const DFS_AUTHORITY_PROJECTION_MAX_ENTRIES: usize =
    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub const DFS_AUTHORITY_PROJECTION_MAX_BYTES: usize =
    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES;

/// A compact collection of exact and prefix scopes backed by authoritative KV
/// reads. This is DFS cache policy: hosts provide linearized reads, while DFS
/// owns scope reuse, generation invalidation, and resource bounds.
#[derive(Debug)]
pub struct DfsAuthoritativeProjectionCache {
    pub generation: u64,
    pub revision: KvRevision,
    pub exact_entries: BTreeMap<String, Option<KvEntry>>,
    pub prefix_entries: BTreeMap<String, Vec<KvEntry>>,
}

impl Default for DfsAuthoritativeProjectionCache {
    fn default() -> Self {
        Self {
            generation: 1,
            revision: KvRevision::ZERO,
            exact_entries: BTreeMap::new(),
            prefix_entries: BTreeMap::new(),
        }
    }
}

impl DfsAuthoritativeProjectionCache {
    pub fn clear_scopes(&mut self) {
        self.exact_entries.clear();
        self.prefix_entries.clear();
    }

    pub fn begin_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.clear_scopes();
    }

    pub fn begin_generation_at_revision(&mut self, revision: KvRevision) {
        self.revision = self.revision.max(revision);
        self.begin_generation();
    }

    pub fn cached_entry(&self, key: &str) -> Option<Option<KvEntry>> {
        if let Some(entry) = self.exact_entries.get(key) {
            return Some(entry.clone());
        }
        self.prefix_entries
            .iter()
            .filter(|(prefix, _)| key.starts_with(prefix.as_str()))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, entries)| entries.iter().find(|entry| entry.key == key).cloned())
    }

    pub fn cached_entries(&self, keys: &[String]) -> Option<Vec<Option<KvEntry>>> {
        keys.iter().map(|key| self.cached_entry(key)).collect()
    }

    pub fn cached_prefix(&self, prefix: &str) -> Option<Vec<KvEntry>> {
        self.prefix_entries
            .iter()
            .filter(|(validated, _)| prefix.starts_with(validated.as_str()))
            .max_by_key(|(validated, _)| validated.len())
            .map(|(_, entries)| {
                entries
                    .iter()
                    .filter(|entry| entry.key.starts_with(prefix))
                    .cloned()
                    .collect()
            })
    }

    pub fn scope_count(&self) -> usize {
        self.exact_entries
            .len()
            .saturating_add(self.prefix_entries.len())
    }

    pub fn entry_count(&self) -> usize {
        self.exact_entries
            .values()
            .filter(|entry| entry.is_some())
            .count()
            .saturating_add(self.prefix_entries.values().map(Vec::len).sum::<usize>())
    }

    pub fn payload_bytes(&self) -> usize {
        let exact_bytes = self
            .exact_entries
            .iter()
            .fold(0_usize, |bytes, (key, entry)| {
                bytes.saturating_add(key.len()).saturating_add(
                    entry
                        .as_ref()
                        .map_or(0, |entry| entry.key.len().saturating_add(entry.value.len())),
                )
            });
        self.prefix_entries
            .iter()
            .fold(exact_bytes, |bytes, (prefix, entries)| {
                entries
                    .iter()
                    .fold(bytes.saturating_add(prefix.len()), |bytes, entry| {
                        bytes
                            .saturating_add(entry.key.len())
                            .saturating_add(entry.value.len())
                    })
            })
    }

    pub fn fits_resource_bounds(&self) -> bool {
        self.scope_count() <= DFS_AUTHORITY_PROJECTION_MAX_SCOPES
            && self.entry_count() <= DFS_AUTHORITY_PROJECTION_MAX_ENTRIES
            && self.payload_bytes() <= DFS_AUTHORITY_PROJECTION_MAX_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::KvVersion;

    fn entry(key: &str, value: &[u8]) -> KvEntry {
        KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        }
    }

    #[test]
    fn longest_prefix_supplies_cached_exact_lookup() {
        let mut cache = DfsAuthoritativeProjectionCache::default();
        cache.prefix_entries.insert(
            "/dfs/".to_string(),
            vec![entry("/dfs/filesystems/sifs/config", b"broad")],
        );
        cache.prefix_entries.insert(
            "/dfs/filesystems/".to_string(),
            vec![entry("/dfs/filesystems/sifs/config", b"exact")],
        );
        assert_eq!(
            cache
                .cached_entry("/dfs/filesystems/sifs/config")
                .flatten()
                .map(|entry| entry.value),
            Some(b"exact".to_vec())
        );
    }

    #[test]
    fn generation_invalidation_drops_all_cached_scopes() {
        let mut cache = DfsAuthoritativeProjectionCache::default();
        cache
            .exact_entries
            .insert("/dfs/a".to_string(), Some(entry("/dfs/a", b"a")));
        cache
            .prefix_entries
            .insert("/dfs/".to_string(), vec![entry("/dfs/b", b"b")]);
        cache.begin_generation_at_revision(KvRevision(7));
        assert_eq!(cache.generation, 2);
        assert_eq!(cache.revision, KvRevision(7));
        assert_eq!(cache.scope_count(), 0);
    }
}
