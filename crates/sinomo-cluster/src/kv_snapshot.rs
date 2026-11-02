#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{locked_kv_state_authority_id, read_cluster_kv_snapshot};
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{InProcessKvStore, KvAuthoritativeRead, KvSnapshot};
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvSnapshotRefreshOptions {
    pub without_local_snapshot_timeout: Duration,
    pub with_local_snapshot_timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterKvSnapshotRefreshOptions {
    fn refresh_budget(&self, has_local_snapshot: bool) -> Duration {
        if has_local_snapshot {
            self.with_local_snapshot_timeout
        } else {
            self.without_local_snapshot_timeout
        }
    }

    pub(crate) fn routed_node_command_timeout(&self, remaining: Duration) -> Duration {
        remaining.min(self.tcp_command_timeout.max(self.iroh_command_timeout))
    }

    pub(crate) fn routed_node_attempt_timeout(&self, remaining: Duration) -> Duration {
        let attempt_cap = self
            .tcp_foreground_attempt_timeout
            .max(self.public_foreground_attempt_timeout)
            .max(self.iroh_foreground_attempt_timeout);
        self.routed_node_command_timeout(remaining.min(attempt_cap))
    }
}

pub trait ClusterKvSnapshotRefreshHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

struct LocalClusterKvSnapshotRefreshHost<F> {
    fetch_cluster_kv_snapshot: F,
}

impl<F> ClusterKvSnapshotRefreshHost for LocalClusterKvSnapshotRefreshHost<F>
where
    F: FnMut(&NodeId, Duration) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>,
{
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_kv_state_authority_id(paths, state)
    }

    fn call_node_object_rpc(
        &mut self,
        _state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        if request.route != sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE {
            return Err(NetworkError::Io {
                operation: "cluster KV snapshot refresh".to_string(),
                detail: format!(
                    "unsupported authoritative snapshot RPC route {}",
                    request.route
                ),
            });
        }
        let read = (self.fetch_cluster_kv_snapshot)(target_node_id, timeout)?;
        let payload = sinomo_kv::cluster_kv_authoritative_snapshot_response_rpc_payload(&read)
            .map_err(kv_err)?;
        Ok(ObjectRpcResponse::from_encoded_body(
            payload.route,
            payload.body,
        ))
    }
}

#[derive(Debug)]
pub struct ClusterKvSnapshotRefreshOutcome {
    pub store: InProcessKvStore,
    pub fetched_remote: bool,
}

pub fn refresh_current_cluster_kv_store_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<InProcessKvStore, NetworkError> {
    refresh_current_cluster_kv_store_with_outcome(paths, state, options, host)
        .map(|outcome| outcome.store)
}

pub fn refresh_current_cluster_kv_store_with_outcome(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<ClusterKvSnapshotRefreshOutcome, NetworkError> {
    let local_snapshot = read_cluster_kv_snapshot(paths).ok();
    // Snapshot refresh is itself the mechanism that makes a cold KV
    // projection available. Selecting targets through the KV-backed cluster
    // directory first is circular: it can synchronously hydrate that same
    // full snapshot/WAL merely to discover the voter IDs already present in
    // the caller's committed membership fence.
    let mut targets = sinomo_kv::cluster_kv_raft_voter_ids(state).map_err(kv_err)?;
    targets.retain(|node_id| node_id != &state.local_node_id);
    if let Ok(Some(leader_id)) = host.known_kv_state_authority_id(paths, state)
        && let Some(position) = targets.iter().position(|node_id| node_id == &leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let deadline = Instant::now() + options.refresh_budget(local_snapshot.is_some());
    let mut authoritative_snapshot = None;
    for (index, target_node_id) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let remaining_targets = targets.len().saturating_sub(index).max(1);
        let target_count = remaining_targets.min(u32::MAX as usize) as u32;
        let fair_share = remaining
            .checked_div(target_count)
            .unwrap_or(remaining)
            .max(Duration::from_millis(1));
        if let Ok(snapshot) = fetch_cluster_kv_snapshot_from_state_authority_node(
            state,
            target_node_id,
            options.routed_node_attempt_timeout(fair_share),
            host,
        ) {
            authoritative_snapshot = Some(snapshot);
            break;
        }
    }
    let fetched_remote = authoritative_snapshot.is_some();
    if let (Some(authoritative), Some(local)) =
        (authoritative_snapshot.as_ref(), local_snapshot.as_ref())
        && local.revision > authoritative.revision
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "local KV projection revision {:?} is ahead of fresh authoritative revision {:?}; refusing snapshot arbitration and requiring Raft/state reconciliation",
                local.revision, authoritative.revision
            ),
        });
    }
    // A fresh quorum-backed read is authoritative even when a local materialized
    // projection reports a different revision. Never arbitrate consensus state
    // by comparing plain snapshot revisions.
    let snapshot =
        authoritative_snapshot
            .or(local_snapshot)
            .ok_or_else(|| NetworkError::ClusterReadOnly {
                detail:
                    "no local KV projection or reachable authoritative KV snapshot is available"
                        .to_string(),
            })?;
    // The refreshed store is this caller's authoritative read. The local
    // view converges through Raft, not by importing the remote snapshot.
    let store = InProcessKvStore::restore(snapshot).map_err(kv_err)?;
    Ok(ClusterKvSnapshotRefreshOutcome {
        store,
        fetched_remote,
    })
}

fn fetch_cluster_kv_snapshot_from_state_authority_node(
    state: &ClusterBootstrapState,
    authority_node_id: &NodeId,
    timeout: Duration,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<KvSnapshot, NetworkError> {
    let payload = sinomo_kv::cluster_kv_authoritative_snapshot_rpc_payload().map_err(kv_err)?;
    let request =
        ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = host.call_node_object_rpc(state, authority_node_id, &request, timeout)?;
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
        .map(|read| read.value)
        .map_err(kv_err)
}

pub fn refresh_current_cluster_kv_store_with_local_storage_and_transport<F>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    fetch_cluster_kv_snapshot: F,
) -> Result<InProcessKvStore, NetworkError>
where
    F: FnMut(&NodeId, Duration) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>,
{
    let mut host = LocalClusterKvSnapshotRefreshHost {
        fetch_cluster_kv_snapshot,
    };
    refresh_current_cluster_kv_store_with_host(paths, state, options, &mut host)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{CapabilityName, ClusterMemberRecord};
    use sinomo_kv::KvRevision;
    use sinomo_node::{ClusterId, DAEMON_MAIN_RUN_CAPABILITY};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use time::OffsetDateTime;

    struct PanickingSnapshotRefreshDirectory;

    impl sinomo_network::ClusterDirectory for PanickingSnapshotRefreshDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            panic!("snapshot refresh must not read the dynamic cluster directory")
        }

        fn endpoints(&self) -> Result<Vec<sinomo_base::ClusterEndpointRecord>, NetworkError> {
            panic!("snapshot refresh must not read the dynamic cluster directory")
        }

        fn node_statuses(
            &self,
        ) -> Result<BTreeMap<NodeId, sinomo_base::ClusterNodeStatusRecord>, NetworkError> {
            panic!("snapshot refresh must not read the dynamic cluster directory")
        }

        fn connectivity_records(
            &self,
        ) -> Result<Vec<sinomo_network::ClusterConnectivityRecord>, NetworkError> {
            panic!("snapshot refresh must not read the dynamic cluster directory")
        }

        fn version(&self) -> Result<sinomo_network::DirectoryVersion, NetworkError> {
            panic!("snapshot refresh must not read the dynamic cluster directory")
        }
    }

    struct SnapshotRefreshDirectoryActivation(Arc<AtomicBool>);

    impl Drop for SnapshotRefreshDirectoryActivation {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    fn install_panicking_snapshot_refresh_directory() -> SnapshotRefreshDirectoryActivation {
        let active = Arc::new(AtomicBool::new(true));
        let owner_thread = std::thread::current().id();
        let provider_active = Arc::clone(&active);
        sinomo_network::set_cluster_directory_provider(Arc::new(move |_| {
            let directory: Arc<dyn sinomo_network::ClusterDirectory> = if provider_active
                .load(Ordering::Acquire)
                && std::thread::current().id() == owner_thread
            {
                Arc::new(PanickingSnapshotRefreshDirectory)
            } else {
                Arc::new(sinomo_network::EmptyClusterDirectory)
            };
            directory
        }));
        SnapshotRefreshDirectoryActivation(active)
    }

    struct RecordingSnapshotRefreshHost {
        known_authority: NodeId,
        snapshots: BTreeMap<NodeId, KvSnapshot>,
        stalled_target: Option<NodeId>,
        calls: Vec<(NodeId, Duration)>,
    }

    impl ClusterKvSnapshotRefreshHost for RecordingSnapshotRefreshHost {
        fn known_kv_state_authority_id(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, NetworkError> {
            Ok(Some(self.known_authority.clone()))
        }

        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            target_node_id: &NodeId,
            request: &ObjectRpcRequest,
            timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            assert_eq!(
                request.route,
                sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE
            );
            self.calls.push((target_node_id.clone(), timeout));
            if self.stalled_target.as_ref() == Some(target_node_id) {
                std::thread::sleep(timeout);
            }
            let Some(snapshot) = self.snapshots.get(target_node_id) else {
                return Err(NetworkError::Io {
                    operation: "test cluster KV snapshot refresh".to_string(),
                    detail: format!("node {target_node_id} is unavailable"),
                });
            };
            let payload = sinomo_kv::cluster_kv_authoritative_snapshot_response_rpc_payload(
                &sinomo_kv::KvAuthoritativeRead {
                    value: snapshot.clone(),
                    authority: sinomo_kv::KvAuthorityProof {
                        term: 1,
                        commit_index: snapshot.revision.0.max(1),
                        revision: snapshot.revision,
                        basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 2,
                            required_quorum: 2,
                        },
                    },
                },
            )
            .expect("snapshot response payload");
            Ok(ObjectRpcResponse::from_encoded_body(
                payload.route,
                payload.body,
            ))
        }
    }

    fn snapshot(revision: u64) -> KvSnapshot {
        KvSnapshot {
            revision: KvRevision(revision),
            compacted_revision: KvRevision(0),
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        }
    }

    fn snapshot_refresh_test_state() -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        ClusterBootstrapState,
        Vec<NodeId>,
    ) {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-kv-snapshot-refresh");
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_snapshot_refresh").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_kv_snapshot_refresh_local").expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "KV snapshot refresh",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0] = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            local,
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            vec![CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("daemon capability")];
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();

        for index in 0..3_u16 {
            let node_id =
                NodeId::from_legacy_named(format!("node_kv_snapshot_refresh_remote_{index}"))
                    .expect("remote node");
            let mut member = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id.clone(),
                format!("remote {index}"),
                format!("pk-remote-{index}"),
                now,
            );
            member.capabilities =
                vec![CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("daemon capability")];
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
                &node_id,
                format!("10.0.0.{}:21353", index + 2),
                0,
            )];
            state.members.push(member);
        }

        let mut targets =
            sinomo_kv::cluster_kv_raft_voter_ids(&state).expect("projected voter targets");
        targets.retain(|node_id| node_id != &state.local_node_id);
        assert_eq!(targets.len(), 3, "test requires three remote authorities");
        (root, paths, state, targets)
    }

    fn snapshot_refresh_options(timeout: Duration) -> ClusterKvSnapshotRefreshOptions {
        ClusterKvSnapshotRefreshOptions {
            without_local_snapshot_timeout: timeout,
            with_local_snapshot_timeout: timeout,
            tcp_command_timeout: timeout,
            iroh_command_timeout: timeout,
            tcp_foreground_attempt_timeout: timeout,
            public_foreground_attempt_timeout: timeout,
            iroh_foreground_attempt_timeout: timeout,
        }
    }

    #[test]
    fn snapshot_refresh_bounds_stalled_preferred_authority_and_uses_reachable_fallback() {
        let _directory_activation = install_panicking_snapshot_refresh_directory();
        let (_root, paths, state, targets) = snapshot_refresh_test_state();
        let preferred = targets[2].clone();
        let fallback = targets[0].clone();
        let tail = targets[1].clone();
        let total_budget = Duration::from_millis(300);
        let mut host = RecordingSnapshotRefreshHost {
            known_authority: preferred.clone(),
            snapshots: BTreeMap::from([(fallback.clone(), snapshot(2))]),
            stalled_target: Some(preferred.clone()),
            calls: Vec::new(),
        };

        let outcome = refresh_current_cluster_kv_store_with_outcome(
            &paths,
            &state,
            snapshot_refresh_options(total_budget),
            &mut host,
        )
        .expect("fallback snapshot refresh");

        assert!(outcome.fetched_remote);
        assert_eq!(outcome.store.revision(), KvRevision(2));
        assert_eq!(
            host.calls
                .iter()
                .map(|(node_id, _)| node_id)
                .collect::<Vec<_>>(),
            vec![&preferred, &fallback]
        );
        assert!(!host.calls.iter().any(|(node_id, _)| node_id == &tail));
        assert!(
            host.calls[0].1 <= total_budget / 3,
            "the preferred authority must receive only its fair share"
        );
    }

    #[test]
    fn snapshot_refresh_rejects_local_projection_ahead_of_current_raft_authority() {
        let (_root, paths, state, targets) = snapshot_refresh_test_state();
        let preferred = targets[2].clone();
        crate::kv_local_store::write_cluster_kv_snapshot(&paths, &snapshot(9))
            .expect("local snapshot");
        let mut host = RecordingSnapshotRefreshHost {
            known_authority: preferred.clone(),
            snapshots: BTreeMap::from([(preferred.clone(), snapshot(4))]),
            stalled_target: None,
            calls: Vec::new(),
        };

        let error = refresh_current_cluster_kv_store_with_outcome(
            &paths,
            &state,
            snapshot_refresh_options(Duration::from_secs(1)),
            &mut host,
        )
        .expect_err("a plain local snapshot cannot outrank fresh Raft authority");

        assert_eq!(host.calls.len(), 1);
        assert_eq!(host.calls[0].0, preferred);
        assert!(
            error.to_string().contains("refusing snapshot arbitration"),
            "error should require Raft/state reconciliation: {error}"
        );
    }
}
