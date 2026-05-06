use crate::error::NetworkError;
use crate::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterPaths {
    pub root: PathBuf,
    pub state: PathBuf,
    pub invites: PathBuf,
    pub iroh: PathBuf,
    pub kv_snapshot: PathBuf,
    pub kv_raft: PathBuf,
    pub kv_watches: PathBuf,
    pub kv_watch_events: PathBuf,
}

impl ClusterPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("cluster");
        Self {
            state: root.join("state.json"),
            invites: root.join("invites"),
            iroh: root.join("iroh"),
            kv_snapshot: root.join("kv-snapshot.json"),
            kv_raft: root.join("kv-raft"),
            kv_watches: root.join("watches"),
            kv_watch_events: root.join("watch-events.jsonl"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), NetworkError> {
        fs::create_dir_all(&self.root).map_err(|source| NetworkError::StorageIo {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    pub fn invite_path(&self, invite_id: &ClusterInviteId) -> PathBuf {
        self.invites.join(format!("{}.json", invite_id.as_str()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationRequest {
    pub invite: sinomo_core::ClusterJoinInvite,
    pub invitation_secret: String,
    pub members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendRequest {
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<RaftSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftVoteRequest {
    pub state: ClusterBootstrapState,
    pub vote: RaftRequestVoteRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftVoteResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub vote: RaftRequestVoteResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchEventsRequest {
    pub state: ClusterBootstrapState,
    pub leader_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<KvEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchEventsResponse {
    pub accepted: bool,
    pub detail: String,
    pub stored_events: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchPrefixRequest {
    pub prefix: String,
    pub start_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchRegistration {
    pub watch_id: WatchId,
    pub prefix: String,
    pub next_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchDrainRequest {
    pub watch_id: WatchId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchDrainResponse {
    pub watch_id: WatchId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<KvEvent>,
    pub next_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterIdentityRecord {
    pub cluster_id: ClusterId,
    pub cluster_name: String,
    pub local_node_id: NodeId,
    pub local_node_display_name: String,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftConfigRecord {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voters: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub learners: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_voting: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterCapabilityPolicyRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<CapabilityName>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitationStateRecord {
    pub cluster_id: ClusterId,
    pub invite_id: ClusterInviteId,
    pub state: String,
    pub issuer_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutDesiredRecord {
    pub cluster_id: ClusterId,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkInfoRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub environment_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<ClusterEndpointRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterConnectivityState {
    Healthy,
    Degraded,
    Unreachable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterConnectivityRecord {
    pub cluster_id: ClusterId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub endpoint_id: ClusterEndpointId,
    pub transport: ClusterTransportKind,
    pub address: String,
    pub state: ClusterConnectivityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub consecutive_failures: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

pub fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, NetworkError> {
    if !paths.state.exists() {
        return Err(NetworkError::ClusterOrphan);
    }
    let content = fs::read_to_string(&paths.state).map_err(|source| NetworkError::StorageIo {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut state: ClusterBootstrapState =
        serde_json::from_str(&content).map_err(|source| NetworkError::Json {
            path: paths.state.display().to_string(),
            detail: source.to_string(),
        })?;
    let kv_members = match read_cluster_members_from_kv(paths) {
        Ok(members) => members,
        Err(error) => {
            eprintln!(
                "warning: ignoring unreadable cluster kv snapshot while loading members: {error}"
            );
            Vec::new()
        }
    };
    if !kv_members.is_empty() {
        state.members = kv_members;
    }
    Ok(state)
}

pub fn write_cluster_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    write_json_file(&paths.state, state)
}

pub fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    let content =
        fs::read_to_string(&paths.kv_snapshot).map_err(|source| NetworkError::StorageIo {
            path: paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        })?;
    serde_json::from_str(&content).map_err(|source| NetworkError::Json {
        path: paths.kv_snapshot.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    write_json_file(&paths.kv_snapshot, snapshot)
}

pub fn read_cluster_kv_store(paths: &ClusterPaths) -> Result<InProcessKvStore, NetworkError> {
    if paths.kv_raft.exists() {
        let state = read_cluster_state_without_kv(paths)?;
        let node = open_cluster_raft_kv_node(paths, &state.local_node_id)?;
        return Ok(node.store().clone());
    }
    if paths.kv_snapshot.exists() {
        return read_cluster_kv_snapshot(paths).map(InProcessKvStore::restore);
    }
    Ok(InProcessKvStore::new())
}

pub fn cluster_raft_wal_paths(paths: &ClusterPaths, node_id: &NodeId) -> KvWalPaths {
    KvWalPaths::new(paths.kv_raft.join(node_id.as_str()))
}

pub fn open_cluster_raft_kv_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    let wal_paths = cluster_raft_wal_paths(paths, node_id);
    let mut node = DurableRaftKvNode::open(node_id.clone(), wal_paths)?;
    if node.hard_state().commit_index == 0 && paths.kv_snapshot.exists() {
        let snapshot = read_cluster_kv_snapshot(paths)?;
        node.restore_snapshot(RaftSnapshot {
            last_included_index: snapshot.revision.0,
            last_included_term: 1,
            kv: snapshot,
        })?;
    }
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    Ok(node)
}

pub fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvSnapshot, NetworkError> {
    append_cluster_raft_command_with_response(paths, node_id, command).map(|(snapshot, _)| snapshot)
}

pub fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    let term = node.hard_state().current_term.max(1);
    let entry = node.append_local_command(term, command)?;
    node.commit_to(entry.index)?;
    let txn_response = node.txn_response_at(entry.index).cloned();
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node).map(|snapshot| (snapshot, txn_response))
}

pub fn append_cluster_raft_command_uncommitted(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    let term = node.hard_state().current_term.max(1);
    let entry = node.append_local_command(term, command)?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node)?;
    Ok(entry)
}

pub fn commit_cluster_raft_index(
    paths: &ClusterPaths,
    node_id: &NodeId,
    commit_index: u64,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.commit_to(commit_index)?;
    let txn_response = node.txn_response_at(commit_index).cloned();
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node).map(|snapshot| (snapshot, txn_response))
}

pub fn apply_cluster_raft_append(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: Option<RaftSnapshot>,
    append: RaftAppendEntriesRequest,
) -> Result<RaftAppendEntriesResponse, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    if let Some(snapshot) = snapshot {
        let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
            && (append.prev_log_index != snapshot.last_included_index
                || append.prev_log_term == snapshot.last_included_term);
        let snapshot_is_newer = snapshot.last_included_index > node.applied_index();
        let snapshot_kv_is_newer = snapshot.kv.revision > node.store().snapshot().revision;
        let append_term_is_current = append.term >= node.hard_state().current_term;
        if append_term_is_current
            && snapshot_is_newer
            && snapshot_kv_is_newer
            && snapshot_matches_append
        {
            node.restore_snapshot(snapshot)?;
        }
    }
    let response = node.append_entries(append)?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node)?;
    Ok(response)
}

pub fn apply_cluster_raft_vote(
    paths: &ClusterPaths,
    node_id: &NodeId,
    vote: RaftRequestVoteRequest,
) -> Result<RaftRequestVoteResponse, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.request_vote(vote).map_err(Into::into)
}

pub fn set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.become_leader(term)?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node)
}

pub fn clear_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.clear_known_leader()?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node)
}

pub fn compact_cluster_raft_node_if_needed(
    paths: &ClusterPaths,
    node: &mut DurableRaftKvNode,
) -> Result<(), NetworkError> {
    const MAX_UNCOMPACTED_LOG_ENTRIES: usize = 1024;
    const MAX_WAL_BYTES: u64 = 4 * 1024 * 1024;
    const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

    let wal_paths = cluster_raft_wal_paths(paths, node.node_id());
    let wal_bytes = fs::metadata(&wal_paths.wal)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let snapshot_bytes = fs::metadata(&wal_paths.snapshot)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    if node.log().len() > MAX_UNCOMPACTED_LOG_ENTRIES
        || wal_bytes > MAX_WAL_BYTES
        || snapshot_bytes > MAX_SNAPSHOT_BYTES
    {
        node.save_snapshot()?;
    }
    Ok(())
}

pub fn mirror_cluster_raft_snapshot(
    paths: &ClusterPaths,
    node: &DurableRaftKvNode,
) -> Result<KvSnapshot, NetworkError> {
    let snapshot = node.store().snapshot();
    write_cluster_kv_snapshot(paths, &snapshot)?;
    Ok(snapshot)
}

pub fn create_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let snapshot = read_cluster_kv_snapshot(paths)?;
    if request.start_revision <= snapshot.compacted_revision
        && snapshot.compacted_revision != KvRevision::ZERO
    {
        return Err(NetworkError::Kv(KvError::Compacted {
            requested: request.start_revision,
            compacted: snapshot.compacted_revision,
        }));
    }
    fs::create_dir_all(&paths.kv_watches).map_err(|source| NetworkError::StorageIo {
        path: paths.kv_watches.display().to_string(),
        detail: source.to_string(),
    })?;
    let watch_id = next_cluster_kv_watch_id(paths)?;
    let registration = ClusterKvWatchRegistration {
        watch_id,
        prefix: request.prefix,
        next_revision: request.start_revision,
    };
    write_json_file(&cluster_kv_watch_path(paths, watch_id), &registration)?;
    Ok(registration)
}

pub fn drain_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    let watch_path = cluster_kv_watch_path(paths, request.watch_id);
    let content = fs::read_to_string(&watch_path).map_err(|source| NetworkError::StorageIo {
        path: watch_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut registration: ClusterKvWatchRegistration =
        serde_json::from_str(&content).map_err(|source| NetworkError::Json {
            path: watch_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let snapshot = read_cluster_kv_snapshot(paths)?;
    if registration.next_revision <= snapshot.compacted_revision
        && snapshot.compacted_revision != KvRevision::ZERO
    {
        return Err(NetworkError::Kv(KvError::Compacted {
            requested: registration.next_revision,
            compacted: snapshot.compacted_revision,
        }));
    }
    let mut events = cluster_kv_watch_events(paths)?;
    events.extend(snapshot.events);
    events.sort_by(|left, right| {
        (left.revision, left.key.as_str()).cmp(&(right.revision, right.key.as_str()))
    });
    events.dedup_by(|left, right| {
        left.revision == right.revision && left.key == right.key && left.kind == right.kind
    });
    let events = events
        .into_iter()
        .filter(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        })
        .collect::<Vec<_>>();
    if let Some(last) = events.last() {
        registration.next_revision = KvRevision(last.revision.0.saturating_add(1));
        write_json_file(&watch_path, &registration)?;
    }
    Ok(ClusterKvWatchDrainResponse {
        watch_id: registration.watch_id,
        events,
        next_revision: registration.next_revision,
    })
}

pub fn append_cluster_kv_watch_events(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() {
        return Ok(0);
    }
    if let Some(parent) = paths.kv_watch_events.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut existing = cluster_kv_watch_events(paths)?;
    let mut stored = 0;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.kv_watch_events)
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events.display().to_string(),
            detail: source.to_string(),
        })?;
    for event in incoming {
        if existing.iter().any(|candidate| {
            candidate.revision == event.revision
                && candidate.key == event.key
                && candidate.kind == event.kind
        }) {
            continue;
        }
        serde_json::to_writer(&mut file, event).map_err(|source| NetworkError::Json {
            path: paths.kv_watch_events.display().to_string(),
            detail: source.to_string(),
        })?;
        writeln!(file).map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events.display().to_string(),
            detail: source.to_string(),
        })?;
        existing.push(event.clone());
        stored += 1;
    }
    file.sync_data().map_err(|source| NetworkError::StorageIo {
        path: paths.kv_watch_events.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(stored)
}

pub fn cluster_kv_watch_events(paths: &ClusterPaths) -> Result<Vec<KvEvent>, NetworkError> {
    if !paths.kv_watch_events.exists() {
        return Ok(Vec::new());
    }
    let file =
        fs::File::open(&paths.kv_watch_events).map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut events = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events.display().to_string(),
            detail: source.to_string(),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        events.push(
            serde_json::from_str(&line).map_err(|source| NetworkError::Json {
                path: paths.kv_watch_events.display().to_string(),
                detail: source.to_string(),
            })?,
        );
    }
    Ok(events)
}

fn cluster_kv_watch_path(paths: &ClusterPaths, watch_id: WatchId) -> PathBuf {
    paths.kv_watches.join(format!("watch-{}.json", watch_id.0))
}

fn next_cluster_kv_watch_id(paths: &ClusterPaths) -> Result<WatchId, NetworkError> {
    let mut max_id = 0_u64;
    if paths.kv_watches.exists() {
        for entry in fs::read_dir(&paths.kv_watches).map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watches.display().to_string(),
            detail: source.to_string(),
        })? {
            let entry = entry.map_err(|source| NetworkError::StorageIo {
                path: paths.kv_watches.display().to_string(),
                detail: source.to_string(),
            })?;
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Some(id) = name
                .strip_prefix("watch-")
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|id| id.parse::<u64>().ok())
            {
                max_id = max_id.max(id);
            }
        }
    }
    Ok(WatchId(max_id.saturating_add(1).max(1)))
}

pub fn read_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    if !paths.state.exists() {
        return Err(NetworkError::ClusterOrphan);
    }
    let content = fs::read_to_string(&paths.state).map_err(|source| NetworkError::StorageIo {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| NetworkError::Json {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn read_cluster_members_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
    if !paths.kv_snapshot.exists() {
        return Ok(Vec::new());
    }
    let store = read_cluster_kv_store(paths)?;
    let mut members: Vec<ClusterMemberRecord> = Vec::new();
    for entry in store.range_prefix("/cluster/members/") {
        members.push(entry.decode()?);
    }
    let endpoints = read_cluster_endpoints_from_store(&store)?;
    let policies = read_cluster_capability_policies_from_store(&store)?;
    for member in &mut members {
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        if let Some(member_endpoints) = endpoints.get(&member.node_id) {
            for endpoint in member_endpoints {
                if !member
                    .endpoints
                    .iter()
                    .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
                {
                    member.endpoints.push(endpoint.clone());
                }
            }
        }
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(members)
}

pub fn read_cluster_endpoints_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    if !paths.kv_snapshot.exists() {
        return Ok(Vec::new());
    }
    let store = read_cluster_kv_store(paths)?;
    let mut endpoints = read_cluster_endpoints_from_store(&store)?
        .into_values()
        .flatten()
        .collect::<Vec<_>>();
    endpoints.sort_by(|left, right| {
        (
            left.node_id.as_str(),
            left.priority,
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.node_id.as_str(),
                right.priority,
                right.endpoint_id.as_str(),
            ))
    });
    Ok(endpoints)
}

fn read_cluster_endpoints_from_store(
    store: &InProcessKvStore,
) -> Result<BTreeMap<NodeId, Vec<ClusterEndpointRecord>>, NetworkError> {
    let mut endpoints: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> = BTreeMap::new();
    for entry in store.range_prefix("/cluster/endpoints/") {
        let endpoint: ClusterEndpointRecord = entry.decode()?;
        endpoints
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    Ok(endpoints)
}

fn read_cluster_capability_policies_from_store(
    store: &InProcessKvStore,
) -> Result<BTreeMap<NodeId, ClusterCapabilityPolicyRecord>, NetworkError> {
    let mut policies = BTreeMap::new();
    for entry in store.range_prefix("/cluster/policy/capabilities/") {
        let policy: ClusterCapabilityPolicyRecord = entry.decode()?;
        policies.insert(policy.node_id.clone(), policy);
    }
    Ok(policies)
}

pub fn read_cluster_node_statuses_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNodeStatusRecord>, NetworkError> {
    if !paths.kv_snapshot.exists() {
        return Ok(Vec::new());
    }
    let store = read_cluster_kv_store(paths)?;
    let mut statuses: Vec<ClusterNodeStatusRecord> = Vec::new();
    for entry in store.range_prefix("/cluster/node-status/") {
        statuses.push(entry.decode()?);
    }
    statuses.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(statuses)
}

pub fn read_cluster_network_info_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNetworkInfoRecord>, NetworkError> {
    if !paths.kv_snapshot.exists() {
        return Ok(Vec::new());
    }
    let store = read_cluster_kv_store(paths)?;
    let mut records: Vec<ClusterNetworkInfoRecord> = Vec::new();
    for entry in store.range_prefix("/cluster/network/info/") {
        records.push(entry.decode()?);
    }
    records.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(records)
}

pub fn read_cluster_connectivity_graph_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    if !paths.kv_snapshot.exists() {
        return Ok(Vec::new());
    }
    let store = read_cluster_kv_store(paths)?;
    let mut records: Vec<ClusterConnectivityRecord> = Vec::new();
    for entry in store.range_prefix("/cluster/network/connectivity/") {
        records.push(entry.decode()?);
    }
    records.sort_by(|left, right| {
        (
            left.source_node_id.as_str(),
            left.target_node_id.as_str(),
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.source_node_id.as_str(),
                right.target_node_id.as_str(),
                right.endpoint_id.as_str(),
            ))
    });
    Ok(records)
}

pub fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, NetworkError> {
    let initial_snapshot = open_cluster_raft_kv_node(paths, &state.local_node_id)
        .map(|node| node.store().snapshot())
        .or_else(|error| {
            eprintln!("warning: replacing unreadable cluster kv raft state: {error}");
            write_cluster_kv_snapshot(paths, &InProcessKvStore::new().snapshot())?;
            Ok::<KvSnapshot, NetworkError>(InProcessKvStore::new().snapshot())
        })?;
    let rollout_desired_exists = initial_snapshot
        .entries
        .iter()
        .any(|entry| entry.key == cluster_rollout_desired_key());
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_identity_key(),
        &ClusterIdentityRecord::from(state),
    )?;
    let mut snapshot = put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_raft_config_key(),
        &ClusterRaftConfigRecord::from(state),
    )?;
    if !rollout_desired_exists {
        snapshot = put_cluster_kv_value(
            paths,
            &state.local_node_id,
            cluster_rollout_desired_key(),
            &ClusterRolloutDesiredRecord {
                cluster_id: state.cluster_id.clone(),
                state: "idle".to_string(),
                source: None,
                mode: None,
                updated_at: state.updated_at,
            },
        )?;
    }
    for member in &state.members {
        let command = KvCommand::PutClusterMember(member.clone());
        append_cluster_raft_command(paths, &state.local_node_id, command)?;
        snapshot = put_cluster_kv_value(
            paths,
            &state.local_node_id,
            cluster_capability_policy_key(&member.node_id),
            &ClusterCapabilityPolicyRecord::from(member),
        )?;
        for endpoint in &member.endpoints {
            snapshot = put_cluster_kv_value(
                paths,
                &state.local_node_id,
                cluster_endpoint_key(&member.node_id, endpoint),
                endpoint,
            )?;
        }
    }
    Ok(snapshot)
}

pub fn sync_cluster_node_status_to_kv(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    let key = cluster_node_status_key(&status.node_id);
    let node_id = status.node_id.clone();
    let rollout = status.rollout.clone();
    let snapshot = append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Op(KvOp::put_serde(key, &status, None)?),
    )?;
    if let Some(rollout) = rollout {
        return sync_cluster_rollout_node_to_kv(paths, &rollout, &node_id);
    }
    Ok(snapshot)
}

pub fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    let key = cluster_network_info_key(&info.node_id);
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Op(KvOp::put_serde(key, &info, None)?),
    )
}

pub fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    let mut success = vec![KvOp::DeletePrefix {
        prefix: cluster_connectivity_source_prefix(source_node_id),
    }];
    for record in records {
        let key = cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        success.push(KvOp::put_serde(key, &record, None)?);
    }
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        }),
    )
}

pub fn expire_cluster_network_records(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    let store = read_cluster_kv_store(paths)?;
    let mut success = Vec::new();
    for entry in store.range_prefix("/cluster/network/info/") {
        let Ok(record) = entry.decode::<ClusterNetworkInfoRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete { key: entry.key });
        }
    }
    for entry in store.range_prefix("/cluster/network/connectivity/") {
        let Ok(record) = entry.decode::<ClusterConnectivityRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete { key: entry.key });
        }
    }
    if success.is_empty() {
        return read_cluster_kv_snapshot(paths);
    }
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        }),
    )
}

pub fn cluster_node_status_key(node_id: &NodeId) -> String {
    format!("/cluster/node-status/{node_id}")
}

pub fn cluster_network_info_key(node_id: &NodeId) -> String {
    format!("/cluster/network/info/{node_id}")
}

pub fn cluster_connectivity_source_prefix(source_node_id: &NodeId) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/")
}

pub fn cluster_connectivity_key(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &ClusterEndpointId,
) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/{target_node_id}/{endpoint_id}")
}

pub fn cluster_identity_key() -> String {
    "/cluster/config/identity".to_string()
}

pub fn cluster_raft_config_key() -> String {
    "/cluster/raft/config".to_string()
}

pub fn cluster_endpoint_key(node_id: &NodeId, endpoint: &ClusterEndpointRecord) -> String {
    format!("/cluster/endpoints/{node_id}/{}", endpoint.endpoint_id)
}

pub fn cluster_capability_policy_key(node_id: &NodeId) -> String {
    format!("/cluster/policy/capabilities/{node_id}")
}

pub fn cluster_certificate_key(certificate: &ClusterCertificate) -> String {
    format!("/cluster/certificates/{}", certificate.certificate_id)
}

pub fn cluster_invitation_key(invite_id: &ClusterInviteId) -> String {
    format!("/cluster/invitations/{invite_id}")
}

pub fn cluster_invitation_state_key(invite_id: &ClusterInviteId) -> String {
    format!("/cluster/invitations/{invite_id}/state")
}

pub fn cluster_rollout_desired_key() -> String {
    "/cluster/rollout/desired".to_string()
}

pub fn cluster_rollout_node_key(node_id: &NodeId) -> String {
    format!("/cluster/rollout/nodes/{node_id}")
}

pub fn read_cluster_invitation_from_kv(
    paths: &ClusterPaths,
    invite_id: &ClusterInviteId,
) -> Result<sinomo_core::ClusterJoinInvite, NetworkError> {
    let store = read_cluster_kv_store(paths)?;
    let key = cluster_invitation_key(invite_id);
    let entry = store.get(&key).ok_or_else(|| NetworkError::StorageIo {
        path: key.clone(),
        detail: "cluster invitation is not published".to_string(),
    })?;
    entry.decode().map_err(Into::into)
}

pub fn sync_cluster_invitation_to_kv(
    paths: &ClusterPaths,
    invite: &sinomo_core::ClusterJoinInvite,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_invitation_key(&invite.invite_id),
        invite,
    )?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_invitation_state_key(&invite.invite_id),
        &ClusterInvitationStateRecord {
            cluster_id: invite.cluster_id.clone(),
            invite_id: invite.invite_id.clone(),
            state: "active".to_string(),
            issuer_node_id: invite.issuer_node_id.clone(),
            expires_at: invite.expires_at,
            updated_at: OffsetDateTime::now_utc(),
        },
    )
}

pub fn sync_cluster_certificate_to_kv(
    paths: &ClusterPaths,
    certificate: &ClusterCertificate,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_certificate_key(certificate),
        certificate,
    )
}

pub fn sync_cluster_rollout_node_to_kv(
    paths: &ClusterPaths,
    status: &sinomo_core::ClusterNodeRolloutStatus,
    node_id: &NodeId,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_rollout_node_key(node_id),
        status,
    )
}

fn put_cluster_kv_value<T: Serialize + ?Sized>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    append_cluster_raft_command(
        paths,
        node_id,
        KvCommand::Op(KvOp::put_serde(key, value, None)?),
    )
}

impl From<&ClusterBootstrapState> for ClusterIdentityRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        Self {
            cluster_id: state.cluster_id.clone(),
            cluster_name: state.cluster_name.clone(),
            local_node_id: state.local_node_id.clone(),
            local_node_display_name: state.local_node_display_name.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            initialized_at: state.initialized_at,
            updated_at: state.updated_at,
        }
    }
}

impl From<&ClusterBootstrapState> for ClusterRaftConfigRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        let mut voters = Vec::new();
        let mut learners = Vec::new();
        let mut non_voting = Vec::new();
        for member in &state.members {
            if !member.state.is_authorized() {
                continue;
            }
            match member.role {
                ClusterMemberRole::Voter => voters.push(member.node_id.clone()),
                ClusterMemberRole::Learner => learners.push(member.node_id.clone()),
                ClusterMemberRole::NonVoting => non_voting.push(member.node_id.clone()),
            }
        }
        voters.sort();
        learners.sort();
        non_voting.sort();
        Self {
            cluster_id: state.cluster_id.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            voters,
            learners,
            non_voting,
            updated_at: state.updated_at,
        }
    }
}

impl From<&ClusterMemberRecord> for ClusterCapabilityPolicyRecord {
    fn from(member: &ClusterMemberRecord) -> Self {
        Self {
            cluster_id: member.cluster_id.clone(),
            node_id: member.node_id.clone(),
            capabilities: member.capabilities.clone(),
            allowed_capabilities: member.allowed_capabilities.clone(),
            updated_at: member.updated_at,
        }
    }
}

pub fn merge_cluster_state_from_peer(
    local: &mut ClusterBootstrapState,
    peer: ClusterBootstrapState,
) -> bool {
    if peer.cluster_id != local.cluster_id {
        return false;
    }
    let mut changed = false;
    for member in peer.members {
        changed |= merge_member_record(local, member);
    }
    if peer.kv_revision > local.kv_revision {
        local.kv_revision = peer.kv_revision;
    }
    if peer.updated_at > local.updated_at {
        local.updated_at = peer.updated_at;
    }
    changed
}

pub fn merge_member_record(
    local: &mut ClusterBootstrapState,
    incoming: ClusterMemberRecord,
) -> bool {
    if incoming.cluster_id != local.cluster_id {
        return false;
    }
    if let Some(existing) = local
        .members
        .iter_mut()
        .find(|member| member.node_id == incoming.node_id)
    {
        let mut changed = false;
        if incoming.updated_at > existing.updated_at {
            *existing = incoming;
            return true;
        }
        for endpoint in incoming.endpoints {
            changed |= upsert_member_endpoint(existing, endpoint);
        }
        changed
    } else {
        local.members.push(incoming);
        true
    }
}

pub fn upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    endpoint: ClusterEndpointRecord,
) -> bool {
    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            false
        } else {
            *existing = endpoint;
            true
        }
    } else if member.endpoints.iter().any(|candidate| {
        candidate.transport == endpoint.transport && candidate.address == endpoint.address
    }) {
        false
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

pub fn cluster_sync_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        if !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "daemon.main.run")
        {
            continue;
        }
        let mut endpoints = member.endpoints.clone();
        endpoints.sort_by_key(|endpoint| {
            (
                endpoint.privacy != ClusterEndpointPrivacy::Lan,
                endpoint.priority,
                endpoint.address.clone(),
            )
        });
        targets.extend(endpoints.into_iter().filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::Iroh
                )
        }));
    }
    targets
}

pub(crate) fn write_json_file<T: Serialize>(path: &PathBuf, value: &T) -> Result<(), NetworkError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|source| NetworkError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(path, format!("{content}\n")).map_err(|source| NetworkError::StorageIo {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}
