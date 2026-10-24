//! Bounded write configuration derived from native proofs and committed deltas.
//!
//! This state owns no watcher, task, transport, or storage. Its caller starts
//! buffering before issuing the four native reads and supplies the exact
//! resident consensus floor. Native certificates keep their original revisions;
//! `revision` is the watermark reached by contiguous committed replay.

use crate::DfsAuthoritativePrefixProjection;
use sinomo_core::kv::{
    CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES,
    CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    ClusterKvCommittedDelta, ClusterKvRaftConfigRecord, KvAuthoritativeRead, KvAuthorityBasis,
    KvAuthorityProof, KvEntry, KvError, KvEventKind, KvLocalRead, KvRevision,
    MAX_CLUSTER_KV_WATCH_EVENTS, cluster_kv_raft_config_key, cluster_kv_raft_quorum_size,
    validate_cluster_kv_current_term_authority_proof,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

pub(crate) const DFS_WRITE_CONFIGURATION_PREFIXES: [&str; 4] = [
    "/dfs/filesystems/",
    "/dfs/services/content/",
    "/cluster/services/",
    "/cluster/members/",
];

pub(crate) const DFS_WRITE_CONFIGURATION_SCOPE_COUNT: usize =
    DFS_WRITE_CONFIGURATION_PREFIXES.len();

#[derive(Clone, Debug)]
pub struct DfsWriteConfigurationProjection {
    pub(crate) revision: KvRevision,
    pub(crate) scopes: [Vec<KvEntry>; DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
    pub(crate) authority: [KvAuthorityProof; DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
    pub(crate) floor: KvLocalRead<Option<KvEntry>>,
}

impl DfsWriteConfigurationProjection {
    pub fn revision(&self) -> KvRevision {
        self.revision
    }

    /// Contiguous replay keeps these native bootstrap inputs unchanged. This
    /// relation is not permission: callers still need a fresh resident-floor
    /// check, a stable current capture, and their native authority/store fences.
    pub(crate) fn has_same_native_proof_origin(&self, previous: &Self) -> bool {
        self.authority == previous.authority
            && self.floor == previous.floor
            && self.revision >= previous.revision
    }

    /// Returns a complete borrowed range only inside one of the proven scopes.
    pub fn entries(&self, prefix: &str) -> Option<&[KvEntry]> {
        let scope = DFS_WRITE_CONFIGURATION_PREFIXES
            .iter()
            .position(|scope| prefix.starts_with(scope))?;
        let entries = &self.scopes[scope];
        let start = entries.partition_point(|entry| entry.key.as_str() < prefix);
        let count = entries[start..].partition_point(|entry| entry.key.starts_with(prefix));
        Some(&entries[start..start + count])
    }

    /// This is an observed-floor check, not a new authority certificate.
    pub fn matches_floor(&self, floor: &KvLocalRead<Option<KvEntry>>) -> bool {
        floor.value.is_some()
            && floor.value == self.floor.value
            && floor.revision >= self.floor.revision
            && floor.applied_index >= self.floor.applied_index
            && floor.observed_term >= self.floor.observed_term
            && self
                .authority
                .iter()
                .all(|proof| proof.term >= floor.observed_term)
    }
}

#[derive(Debug, Default)]
pub(crate) struct DfsWriteConfigurationState {
    published: Option<Arc<DfsWriteConfigurationProjection>>,
    pending: Option<DfsWriteConfigurationProjection>,
    journal: VecDeque<ClusterKvCommittedDelta>,
    required_revision: KvRevision,
    refresh_revision_floor: KvRevision,
    refresh_token: u64,
    bootstrap_active: bool,
}

impl DfsWriteConfigurationState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn begin_refresh(&mut self) -> u64 {
        self.invalidate(self.required_revision);
        self.bootstrap_active = true;
        self.refresh_revision_floor = self.required_revision;
        self.refresh_token
    }

    /// A cancelled/read-failed owner must not leave buffering active forever.
    /// A validated pending bundle continues to await its later committed deltas.
    pub(crate) fn finish_refresh(&mut self, token: u64) {
        if token == self.refresh_token && self.pending.is_none() && self.published.is_none() {
            self.invalidate(self.required_revision);
        }
    }

    pub(crate) fn snapshot(&self) -> Option<Arc<DfsWriteConfigurationProjection>> {
        self.published.clone()
    }

    pub(crate) fn needs_refresh(&self) -> bool {
        self.published.is_none() && !self.bootstrap_active
    }

    pub(crate) fn bootstrap_pending(&self) -> bool {
        self.published.is_none() && self.bootstrap_active
    }

    pub(crate) fn observed_revision(&self) -> KvRevision {
        self.published
            .as_ref()
            .map_or(self.required_revision, |view| {
                self.required_revision.max(view.revision)
            })
    }

    pub(crate) fn invalidate(&mut self, required_revision: KvRevision) {
        self.required_revision = self.observed_revision().max(required_revision);
        self.refresh_token = self.refresh_token.wrapping_add(1).max(1);
        self.published = None;
        self.pending = None;
        self.journal.clear();
        self.bootstrap_active = false;
    }

    pub(crate) fn install_bootstrap(
        &mut self,
        token: u64,
        reads: [KvAuthoritativeRead<Vec<KvEntry>>; DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
        floor: KvLocalRead<Option<KvEntry>>,
    ) -> Result<bool, KvError> {
        if token != self.refresh_token || !self.bootstrap_active {
            return Ok(false);
        }
        if reads
            .iter()
            .any(|read| read.authority.revision < self.refresh_revision_floor)
        {
            self.invalidate(self.required_revision);
            return Err(unavailable(
                "write configuration refresh predates its committed revision floor",
            ));
        }
        let candidate = match validate_bootstrap(reads, floor) {
            Ok(candidate) => candidate,
            Err(error) => {
                self.invalidate(self.required_revision);
                return Err(error);
            }
        };
        self.required_revision = self.required_revision.max(candidate.revision);
        self.pending = Some(candidate);
        if let Err(error) = check_state_bounds(self.pending.as_ref(), &self.journal) {
            self.invalidate(self.required_revision);
            return Err(error);
        }
        match self.try_publish_pending() {
            Ok(published) => Ok(published),
            Err(error) => {
                self.invalidate(self.required_revision);
                Err(error)
            }
        }
    }

    /// Returns true only if a complete published view remains available.
    pub(crate) fn observe_delta(&mut self, delta: &ClusterKvCommittedDelta) -> bool {
        self.required_revision = self.required_revision.max(delta.revision);
        if validate_delta(delta).is_err() {
            self.invalidate(delta.revision);
            return false;
        }
        if let Some(published) = &self.published {
            if delta.revision <= published.revision {
                return true;
            }
            if touches_authority_floor(delta)
                || !delta_covers(published.revision, delta)
                || published.revision < delta.compacted_revision
            {
                self.invalidate(delta.revision);
                return false;
            }
            let mut next = (**published).clone();
            for (scope, entries) in next.scopes.iter_mut().enumerate() {
                apply_scope_delta(entries, scope, published.revision, delta);
            }
            next.revision = delta.revision;
            if check_state_bounds(Some(&next), &VecDeque::new()).is_err() {
                self.invalidate(delta.revision);
                return false;
            }
            self.published = Some(Arc::new(next));
            return true;
        }
        if !self.bootstrap_active {
            return false;
        }
        if touches_authority_floor(delta) {
            self.invalidate(delta.revision);
            return false;
        }
        // A log-only no-op cannot advance a KV watermark or occupy journal space.
        if delta.events.is_empty() {
            return false;
        }
        if let Some(previous) = self.journal.back() {
            if delta == previous {
                return false;
            }
            if !delta_covers(previous.revision, delta)
                || previous.revision < delta.compacted_revision
            {
                self.invalidate(delta.revision);
                return false;
            }
        }
        if check_journal_append_bounds(self.pending.as_ref(), &self.journal, delta).is_err() {
            self.invalidate(delta.revision);
            return false;
        }
        self.journal.push_back(delta.clone());
        match self.try_publish_pending() {
            Ok(published) => published,
            Err(_) => {
                self.invalidate(delta.revision);
                false
            }
        }
    }

    fn try_publish_pending(&mut self) -> Result<bool, KvError> {
        let Some(pending) = &self.pending else {
            return Ok(false);
        };
        let watermark = pending.revision.max(self.required_revision);
        let mut candidate = pending.clone();
        for scope in 0..DFS_WRITE_CONFIGURATION_PREFIXES.len() {
            let mut revision = pending.authority[scope].revision;
            for delta in &self.journal {
                if delta.revision <= revision {
                    continue;
                }
                if revision < delta.compacted_revision || !delta_covers(revision, delta) {
                    return Err(unavailable(
                        "write configuration committed replay has a revision gap",
                    ));
                }
                apply_scope_delta(&mut candidate.scopes[scope], scope, revision, delta);
                revision = delta.revision;
            }
            if revision < watermark {
                return Ok(false);
            }
        }
        candidate.revision = watermark;
        check_state_bounds(Some(&candidate), &VecDeque::new())?;
        self.published = Some(Arc::new(candidate));
        self.pending = None;
        self.journal.clear();
        self.bootstrap_active = false;
        Ok(true)
    }
}

fn unavailable(detail: &str) -> KvError {
    KvError::AuthorityUnavailable {
        detail: detail.to_string(),
    }
}

fn validate_bootstrap(
    reads: [KvAuthoritativeRead<Vec<KvEntry>>; DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
    floor: KvLocalRead<Option<KvEntry>>,
) -> Result<DfsWriteConfigurationProjection, KvError> {
    let config_entry = floor.value.as_ref().ok_or_else(|| {
        unavailable("write configuration resident consensus floor is unavailable")
    })?;
    if config_entry.key != cluster_kv_raft_config_key()
        || config_entry.create_revision > config_entry.modify_revision
        || config_entry.modify_revision > floor.revision
    {
        return Err(unavailable(
            "write configuration resident consensus floor is malformed",
        ));
    }
    let config: ClusterKvRaftConfigRecord = config_entry.decode()?;
    let voters = config.voters.iter().collect::<BTreeSet<_>>();
    if voters.is_empty() || voters.len() != config.voters.len() {
        return Err(unavailable(
            "write configuration consensus floor has invalid voters",
        ));
    }
    let quorum = cluster_kv_raft_quorum_size(voters.len());
    let authority = reads.each_ref().map(|read| read.authority);
    let term = authority[0].term;
    for proof in &authority {
        validate_cluster_kv_current_term_authority_proof(proof)?;
        if proof.term != term
            || proof.term < floor.observed_term
            || proof.revision < config_entry.modify_revision
            || !matches!(proof.basis, KvAuthorityBasis::CurrentTermQuorum { voter_acks, required_quorum }
                if required_quorum == quorum && voter_acks <= voters.len())
        {
            return Err(unavailable(
                "write configuration native proofs do not share the resident consensus floor",
            ));
        }
    }
    let mut scopes = reads.map(|read| read.value);
    for (index, entries) in scopes.iter_mut().enumerate() {
        let validated = DfsAuthoritativePrefixProjection::complete(
            DFS_WRITE_CONFIGURATION_PREFIXES[index],
            std::mem::take(entries),
            authority[index].revision,
        )?;
        *entries = validated.into_parts().0;
    }
    Ok(DfsWriteConfigurationProjection {
        revision: authority
            .iter()
            .map(|proof| proof.revision)
            .max()
            .unwrap_or(KvRevision::ZERO),
        scopes,
        authority,
        floor,
    })
}

fn touches_authority_floor(delta: &ClusterKvCommittedDelta) -> bool {
    delta.events.iter().any(|event| {
        event.key == "/cluster/raft/config"
            || event.key.starts_with("/cluster/members/")
            || event.key.starts_with("/cluster/policy/capabilities/")
    })
}

fn validate_delta(delta: &ClusterKvCommittedDelta) -> Result<(), KvError> {
    check_bounds(None, std::iter::once(delta))?;
    if delta.compacted_revision > delta.revision
        || delta.events.len() > MAX_CLUSTER_KV_WATCH_EVENTS
        || delta.final_key_states.len() > CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES
        || delta
            .events
            .last()
            .is_some_and(|event| event.revision != delta.revision)
        || delta
            .events
            .first()
            .is_some_and(|event| event.revision == KvRevision::ZERO)
        || delta.events.windows(2).any(|pair| {
            pair[1].revision != pair[0].revision && pair[1].revision != pair[0].revision.next()
        })
    {
        return Err(unavailable(
            "write configuration committed delta has malformed revisions",
        ));
    }
    let mut last_events = BTreeMap::new();
    for event in delta.events.iter() {
        last_events.insert(event.key.as_str(), event);
    }
    let mut final_keys = BTreeSet::new();
    for state in delta.final_key_states.iter() {
        if !final_keys.insert(state.key.as_str()) {
            return Err(unavailable(
                "write configuration committed delta repeats a final key",
            ));
        }
        let Some(event) = last_events.get(state.key.as_str()) else {
            return Err(unavailable(
                "write configuration committed delta has an unobserved final key",
            ));
        };
        let matches = match (&event.kind, &state.entry) {
            (KvEventKind::Delete, None) => true,
            (KvEventKind::Put { value }, Some(entry)) => {
                entry.key == state.key
                    && entry.create_revision <= entry.modify_revision
                    && entry.modify_revision == event.revision
                    && entry.modify_revision <= delta.revision
                    && entry.value == *value
            }
            _ => false,
        };
        if !matches {
            return Err(unavailable(
                "write configuration committed delta final state does not match its event",
            ));
        }
    }
    if final_keys.len() != last_events.len() {
        return Err(unavailable(
            "write configuration committed delta omits a final key",
        ));
    }
    Ok(())
}

fn delta_covers(previous: KvRevision, delta: &ClusterKvCommittedDelta) -> bool {
    if delta.revision <= previous {
        return false;
    }
    delta
        .events
        .iter()
        .find(|event| event.revision > previous)
        .is_some_and(|event| event.revision == previous.next())
}

fn apply_scope_delta(
    entries: &mut Vec<KvEntry>,
    scope: usize,
    previous: KvRevision,
    delta: &ClusterKvCommittedDelta,
) {
    for state in delta.final_key_states.iter() {
        if !state
            .key
            .starts_with(DFS_WRITE_CONFIGURATION_PREFIXES[scope])
            || !delta
                .events
                .iter()
                .any(|event| event.key == state.key && event.revision > previous)
        {
            continue;
        }
        match (
            entries.binary_search_by(|entry| entry.key.cmp(&state.key)),
            &state.entry,
        ) {
            (Ok(index), Some(entry)) => entries[index] = entry.clone(),
            (Err(index), Some(entry)) => entries.insert(index, entry.clone()),
            (Ok(index), None) => {
                entries.remove(index);
            }
            (Err(_), None) => {}
        }
    }
}

fn check_state_bounds(
    view: Option<&DfsWriteConfigurationProjection>,
    journal: &VecDeque<ClusterKvCommittedDelta>,
) -> Result<(), KvError> {
    check_bounds(view, journal.iter())
}

fn check_journal_append_bounds(
    view: Option<&DfsWriteConfigurationProjection>,
    journal: &VecDeque<ClusterKvCommittedDelta>,
    delta: &ClusterKvCommittedDelta,
) -> Result<(), KvError> {
    check_bounds(view, journal.iter().chain(std::iter::once(delta)))
}

fn check_bounds<'a>(
    view: Option<&DfsWriteConfigurationProjection>,
    journal: impl Iterator<Item = &'a ClusterKvCommittedDelta>,
) -> Result<(), KvError> {
    let mut entries = 0_usize;
    let mut events = 0_usize;
    let mut bytes = 0_usize;
    if let Some(view) = view {
        for entry in view.scopes.iter().flatten().chain(view.floor.value.iter()) {
            entries = entries.saturating_add(1);
            bytes = bytes
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len());
        }
    }
    for delta in journal {
        entries = entries.saturating_add(delta.final_key_states.len());
        events = events.saturating_add(delta.events.len());
        for event in delta.events.iter() {
            bytes = bytes.saturating_add(event.key.len());
            if let KvEventKind::Put { value } = &event.kind {
                bytes = bytes.saturating_add(value.len());
            }
        }
        for state in delta.final_key_states.iter() {
            bytes = bytes.saturating_add(state.key.len());
            if let Some(entry) = &state.entry {
                bytes = bytes
                    .saturating_add(entry.key.len())
                    .saturating_add(entry.value.len());
            }
        }
    }
    if entries > CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES
        || events > MAX_CLUSTER_KV_WATCH_EVENTS
        || bytes > CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES
    {
        return Err(KvError::ResourceExhausted {
            operation: "DFS write configuration retained view and bootstrap journal".to_string(),
            requested_bytes: bytes.max(entries).max(events),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::Id;
    use sinomo_core::kv::{ClusterKvCommittedKeyState, KvEvent, KvVersion, encode_kv_value};

    fn entry(key: &str, value: &[u8], revision: u64) -> KvEntry {
        KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(revision),
            version: KvVersion(revision),
            lease_id: None,
        }
    }

    fn floor(revision: u64) -> KvLocalRead<Option<KvEntry>> {
        let config = ClusterKvRaftConfigRecord {
            cluster_id: Id::derived("write-configuration-test", b"cluster"),
            term: 7,
            kv_revision: 1,
            voters: [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
                .map(|name| Id::derived("write-configuration-test", name))
                .to_vec(),
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        KvLocalRead {
            value: Some(entry(
                &cluster_kv_raft_config_key(),
                &encode_kv_value(&config).unwrap(),
                1,
            )),
            revision: KvRevision(revision),
            observed_term: 7,
            applied_index: revision + 100,
            known_leader: None,
        }
    }

    fn reads(revisions: [u64; 4]) -> [KvAuthoritativeRead<Vec<KvEntry>>; 4] {
        std::array::from_fn(|scope| KvAuthoritativeRead {
            value: vec![entry(
                &format!("{}fixture", DFS_WRITE_CONFIGURATION_PREFIXES[scope]),
                b"original",
                5,
            )],
            authority: KvAuthorityProof {
                term: 7,
                commit_index: revisions[scope] + 100,
                revision: KvRevision(revisions[scope]),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        })
    }

    fn delta(revision: u64, changes: Vec<(String, Option<KvEntry>)>) -> ClusterKvCommittedDelta {
        ClusterKvCommittedDelta {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            events: changes
                .iter()
                .map(|(key, value)| KvEvent {
                    revision: KvRevision(revision),
                    key: key.clone(),
                    kind: value
                        .as_ref()
                        .map_or(KvEventKind::Delete, |entry| KvEventKind::Put {
                            value: entry.value.clone(),
                        }),
                })
                .collect::<Vec<_>>()
                .into(),
            final_key_states: changes
                .into_iter()
                .map(|(key, entry)| ClusterKvCommittedKeyState { key, entry })
                .collect::<Vec<_>>()
                .into(),
        }
    }

    fn put(revision: u64, key: &str, value: &[u8]) -> ClusterKvCommittedDelta {
        delta(
            revision,
            vec![(key.to_string(), Some(entry(key, value, revision)))],
        )
    }

    fn ready() -> DfsWriteConfigurationState {
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        assert!(
            state
                .install_bootstrap(token, reads([10; 4]), floor(10))
                .unwrap()
        );
        state
    }

    #[test]
    fn write_configuration_differing_prefix_revisions_replay_complete_zones() {
        let mut native = reads([10, 11, 12, 12]);
        native[1].value = vec![entry(
            "/dfs/services/content/node-a",
            br#"{"zones":["east","west"]}"#,
            5,
        )];
        native[2].value = vec![entry(
            "/cluster/services/provider-a",
            br#"{"zone":"east","ready":true}"#,
            5,
        )];
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        assert!(!state.observe_delta(&put(11, "/dfs/filesystems/fixture", b"updated-config")));
        assert!(!state.observe_delta(&put(
            12,
            "/dfs/services/content/node-b",
            br#"{"zones":["north","south"]}"#
        )));
        assert!(state.install_bootstrap(token, native, floor(12)).unwrap());
        let view = state.snapshot().unwrap();
        assert_eq!(view.revision(), KvRevision(12));
        assert_eq!(
            view.authority.map(|proof| proof.revision),
            [
                KvRevision(10),
                KvRevision(11),
                KvRevision(12),
                KvRevision(12)
            ]
        );
        assert_eq!(
            view.entries("/dfs/filesystems/").unwrap()[0].value,
            b"updated-config"
        );
        let content = view.entries("/dfs/services/content/").unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&content[0].value).unwrap()["zones"],
            serde_json::json!(["east", "west"])
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&content[1].value).unwrap()["zones"],
            serde_json::json!(["north", "south"])
        );
        assert_eq!(
            view.entries("/dfs/services/content/node-b").unwrap(),
            &content[1..]
        );
        assert_eq!(view.entries("/cluster/services/").unwrap().len(), 1);
        assert!(view.entries("/dfs/").is_none());
        let deletion = delta(13, vec![("/dfs/services/content/node-a".into(), None)]);
        assert!(state.observe_delta(&deletion));
        assert_eq!(
            state
                .snapshot()
                .unwrap()
                .entries("/dfs/services/content/")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            content.len(),
            2,
            "an existing published Arc stays immutable"
        );
    }

    #[test]
    fn write_configuration_membership_read_reaches_the_same_committed_watermark() {
        let native = reads([12, 12, 12, 10]);
        let member = native[3].value.clone();
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        assert!(!state.observe_delta(&put(11, "/unrelated/progress", b"observed")));
        assert!(!state.observe_delta(&put(12, "/cluster/services/fixture", b"current")));
        assert!(state.install_bootstrap(token, native, floor(12)).unwrap());
        let view = state.snapshot().unwrap();
        assert_eq!(view.revision(), KvRevision(12));
        assert_eq!(view.authority[3].revision, KvRevision(10));
        assert_eq!(view.entries("/cluster/members/").unwrap(), member);
        assert!(view.matches_floor(&floor(12)));
    }

    #[test]
    fn write_configuration_pending_bundle_publishes_only_after_later_deltas() {
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        assert!(
            !state
                .install_bootstrap(token, reads([10, 11, 12, 12]), floor(12))
                .unwrap()
        );
        assert!(state.snapshot().is_none());
        state.finish_refresh(token);
        assert!(
            !state.needs_refresh(),
            "a bounded pending bundle still owns delta buffering"
        );
        assert!(!state.observe_delta(&put(11, "/dfs/filesystems/fixture", b"changed")));
        assert!(state.snapshot().is_none());
        assert!(state.observe_delta(&put(12, "/dfs/services/content/new", b"all-zones")));
        assert_eq!(state.snapshot().unwrap().revision(), KvRevision(12));
        assert_eq!(
            state
                .snapshot()
                .unwrap()
                .entries("/dfs/filesystems/")
                .unwrap()[0]
                .value,
            b"changed"
        );
    }

    #[test]
    fn write_configuration_grouped_delta_can_overlap_a_native_read_revision() {
        let a = put(11, "/dfs/filesystems/fixture", b"a");
        let b = put(12, "/dfs/services/content/fixture", b"b");
        let grouped = ClusterKvCommittedDelta {
            revision: KvRevision(12),
            compacted_revision: KvRevision::ZERO,
            events: a
                .events
                .iter()
                .chain(b.events.iter())
                .cloned()
                .collect::<Vec<_>>()
                .into(),
            final_key_states: a
                .final_key_states
                .iter()
                .chain(b.final_key_states.iter())
                .cloned()
                .collect::<Vec<_>>()
                .into(),
        };
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        assert!(!state.observe_delta(&grouped));
        assert!(
            state
                .install_bootstrap(token, reads([10, 11, 12, 12]), floor(12))
                .unwrap()
        );
        let view = state.snapshot().unwrap();
        assert_eq!(view.entries("/dfs/filesystems/").unwrap()[0].value, b"a");
        assert_eq!(
            view.entries("/dfs/services/content/").unwrap()[0].value,
            b"b"
        );
    }

    #[test]
    fn write_configuration_gaps_compaction_and_out_of_order_buffering_revoke_readiness() {
        let mut state = ready();
        assert!(!state.observe_delta(&put(12, "/dfs/services/content/new", b"gap")));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        let token = state.begin_refresh();
        assert!(
            state
                .install_bootstrap(token, reads([10; 4]), floor(10))
                .is_err(),
            "native reads older than the pre-refresh committed floor cannot replace it"
        );
        let mut state = ready();
        let mut compacted = put(11, "/cluster/services/new", b"compacted");
        compacted.compacted_revision = KvRevision(11);
        assert!(!state.observe_delta(&compacted));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        assert!(!state.observe_delta(&put(11, "/dfs/filesystems/new", b"new")));
        assert!(!state.observe_delta(&put(10, "/dfs/filesystems/old", b"old")));
        assert!(
            !state
                .install_bootstrap(token, reads([12; 4]), floor(12))
                .unwrap()
        );
        assert!(state.snapshot().is_none() && state.needs_refresh());
    }

    #[test]
    fn write_configuration_stale_refresh_and_cancel_do_not_replace_newer_publication() {
        let mut state = DfsWriteConfigurationState::new();
        let old = state.begin_refresh();
        let current = state.begin_refresh();
        assert!(
            state
                .install_bootstrap(current, reads([20; 4]), floor(20))
                .unwrap()
        );
        let published = state.snapshot().unwrap();
        assert!(
            !state
                .install_bootstrap(old, reads([10; 4]), floor(10))
                .unwrap()
        );
        state.finish_refresh(old);
        assert!(Arc::ptr_eq(&published, &state.snapshot().unwrap()));
        let cancelled = state.begin_refresh();
        assert!(!state.needs_refresh());
        state.finish_refresh(cancelled);
        assert!(state.needs_refresh() && state.snapshot().is_none());
    }

    #[test]
    fn write_configuration_exact_config_identity_and_membership_touches_invalidate() {
        let mut state = ready();
        let view = state.snapshot().unwrap();
        let mut changed = floor(11);
        let mut config: ClusterKvRaftConfigRecord =
            changed.value.as_ref().unwrap().decode().unwrap();
        config.voters[0] = Id::derived("write-configuration-test", b"same-count-new-voter");
        changed.value = Some(entry(
            &cluster_kv_raft_config_key(),
            &encode_kv_value(&config).unwrap(),
            11,
        ));
        assert!(
            !view.matches_floor(&changed),
            "voter count equality does not preserve config identity"
        );
        let change = delta(
            11,
            vec![(cluster_kv_raft_config_key(), changed.value.clone())],
        );
        assert!(!state.observe_delta(&change));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        let mut identity_only = floor(10);
        identity_only.value.as_mut().unwrap().version = KvVersion(2);
        assert!(!view.matches_floor(&identity_only));
        for key in [
            "/cluster/members/node-a",
            "/cluster/policy/capabilities/node-a",
        ] {
            let mut state = ready();
            assert!(!state.observe_delta(&put(11, key, b"changed")));
            assert!(state.snapshot().is_none() && state.needs_refresh());
        }
    }

    #[test]
    fn write_configuration_resident_revision_and_applied_index_rollbacks_revoke_floor() {
        let mut state = ready();
        let captured = floor(10);
        let view = state.snapshot().unwrap();
        assert!(view.matches_floor(&captured));

        let mut regressed_revision = captured.clone();
        regressed_revision.revision = KvRevision(9);
        assert!(!view.matches_floor(&regressed_revision));
        let mut regressed_applied_index = captured.clone();
        regressed_applied_index.applied_index -= 1;
        assert!(!view.matches_floor(&regressed_applied_index));

        let mut advanced_revision = captured.clone();
        advanced_revision.revision = KvRevision(11);
        assert!(view.matches_floor(&advanced_revision));
        let mut advanced_applied_index = captured.clone();
        advanced_applied_index.applied_index += 1;
        assert!(view.matches_floor(&advanced_applied_index));

        assert!(state.observe_delta(&put(11, "/unrelated/telemetry", b"value")));
        assert!(
            state.snapshot().unwrap().matches_floor(&captured),
            "completed replay does not require the resident floor to reach the newer view revision"
        );
    }

    #[test]
    fn write_configuration_unrelated_revision_progress_keeps_the_exact_floor_ready() {
        let mut state = ready();
        let before = state.snapshot().unwrap();
        assert!(
            before.matches_floor(&floor(1000)),
            "resident global progress is not a configuration change"
        );
        assert!(state.observe_delta(&put(11, "/unrelated/telemetry", b"value")));
        let after = state.snapshot().unwrap();
        assert_eq!(after.revision(), KvRevision(11));
        assert_eq!(after.scopes, before.scopes);
        assert_eq!(
            after.authority, before.authority,
            "committed replay does not manufacture a new native certificate"
        );
        let mut newer_term = floor(11);
        newer_term.observed_term = 8;
        assert!(!after.matches_floor(&newer_term));
        let mut absent = floor(11);
        absent.value = None;
        assert!(!after.matches_floor(&absent));
    }

    #[test]
    fn write_configuration_unavailable_or_incoherent_bootstrap_stays_unpublished() {
        for failure in [
            "missing-floor",
            "wrong-key",
            "newer-term",
            "mixed-terms",
            "bad-quorum",
            "out-of-scope",
            "membership-mixed-term",
            "membership-out-of-scope",
        ] {
            let mut state = DfsWriteConfigurationState::new();
            let token = state.begin_refresh();
            let mut native = reads([10; 4]);
            let mut observed = floor(10);
            match failure {
                "missing-floor" => observed.value = None,
                "wrong-key" => observed.value.as_mut().unwrap().key = "/wrong/config".into(),
                "newer-term" => observed.observed_term = 8,
                "mixed-terms" => native[1].authority.term = 8,
                "bad-quorum" => {
                    native[1].authority.basis = KvAuthorityBasis::CurrentTermQuorum {
                        voter_acks: 1,
                        required_quorum: 1,
                    }
                }
                "out-of-scope" => native[1].value[0].key = "/wrong/scope".into(),
                "membership-mixed-term" => native[3].authority.term = 8,
                "membership-out-of-scope" => native[3].value[0].key = "/wrong/member".into(),
                _ => unreachable!(),
            }
            assert!(
                state.install_bootstrap(token, native, observed).is_err(),
                "{failure}"
            );
            assert!(
                state.snapshot().is_none() && state.needs_refresh(),
                "{failure}"
            );
        }
    }

    #[test]
    fn write_configuration_malformed_final_state_and_bounded_journal_fail_closed() {
        let mut state = ready();
        let mut malformed = put(11, "/dfs/filesystems/fixture", b"new");
        std::sync::Arc::make_mut(&mut malformed.final_key_states)[0]
            .entry
            .as_mut()
            .unwrap()
            .value = b"different".to_vec();
        assert!(!state.observe_delta(&malformed));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        let mut state = DfsWriteConfigurationState::new();
        state.begin_refresh();
        for revision in 1..=MAX_CLUSTER_KV_WATCH_EVENTS as u64 {
            assert!(!state.observe_delta(&put(revision, "/unrelated/key", b"bounded")));
        }
        assert!(!state.needs_refresh());
        assert!(!state.observe_delta(&put(
            MAX_CLUSTER_KV_WATCH_EVENTS as u64 + 1,
            "/unrelated/key",
            b"overflow"
        )));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        let mut state = DfsWriteConfigurationState::new();
        let token = state.begin_refresh();
        let mut native = reads([10; 4]);
        native[0].value = (0..CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES)
            .map(|index| entry(&format!("/dfs/filesystems/{index:08}"), b"bounded", 5))
            .collect();
        assert!(
            state.install_bootstrap(token, native, floor(10)).is_err(),
            "the total view, including the native floor, stays bounded"
        );
        assert!(state.snapshot().is_none() && state.needs_refresh());
    }
}
