//! Test-only retained native fixture DATA and existing-owner reconstruction.
//! Manifests cannot supply authority or reconstruct private transport evidence.
use super::*;
use std::path::{Path, PathBuf};

impl DfsTestRuntime {
    pub(super) fn capture_retained_native_nodes(
        &self,
    ) -> Result<(PathBuf, Vec<RetainedNativeGenesisNode>), String> {
        let fixture_root = self
            .root
            .as_ref()
            .map(|root| root.path().to_path_buf())
            .or_else(|| self.retained_fixture_root.clone())
            .ok_or("actual fixture root unavailable")?;
        let mut nodes = Vec::new();
        for (id, node) in &self.nodes {
            let data = &node.data_dir;
            if data.root() != fixture_root.join(id.as_str()) {
                return Err("retained restart requires exact nonaliased node roots".into());
            }
            let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
                &crate::runtime_host::DfsRuntimePaths::new(data),
            )
            .map_err(|e| e.to_string())?;
            if owner
                .selected_original_data_layout()
                .map_err(|e| e.to_string())?
                .is_some()
            {
                return Err("retained restart refuses selected physical representations".into());
            }
            let paths = ClusterPaths::new(data);
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
                .map_err(|e| e.to_string())?;
            let wal = paths.kv_paths().raft_wal_paths(id);
            for path in [&wal.hard_state, &wal.wal] {
                retained_native_path_identity(path, false)?;
            }
            let hard = wal.read_persisted_hard_state().map_err(|e| e.to_string())?;
            nodes.push(RetainedNativeGenesisNode {
                node_id: id.clone(),
                data_root: data.root(),
                data_identity: retained_native_path_identity(&data.root(), true)?,
                owner_identity: owner
                    .storage_owner_file_identity()
                    .map_err(|e| e.to_string())?,
                raft_root_identity: retained_native_path_identity(&wal.root, true)?,
                raft_lock_identity: retained_native_path_identity(&wal.lock, false)?,
                cluster_state: state,
                raft_term: hard.current_term,
                raft_commit_index: hard.commit_index,
            });
        }
        Ok((fixture_root, nodes))
    }

    // Return the actual locks so specialized original/prefix checks retain
    // them until their entire read-only preflight has finished.
    pub(super) fn validate_retained_native_nodes(
        fixture_root: &Path,
        primary_id: &NodeId,
        nodes: &[RetainedNativeGenesisNode],
    ) -> Result<Vec<std::fs::File>, String> {
        let ids = nodes
            .iter()
            .map(|n| n.node_id.clone())
            .collect::<BTreeSet<_>>();
        if ids.len() != 3 || !ids.contains(primary_id) {
            return Err("retained native fixture roster or original primary differs".into());
        }
        let mut locks = Vec::new();
        let primary = nodes
            .iter()
            .find(|n| n.node_id == *primary_id)
            .ok_or("retained primary missing")?;
        for node in nodes {
            if node.data_root != fixture_root.join(node.node_id.as_str())
                || retained_native_path_identity(&node.data_root, true)? != node.data_identity
            {
                return Err("retained native node root was aliased, foreign or replaced".into());
            }
            let data = SinomoDataDir::new(&node.data_root);
            let paths = ClusterPaths::new(&data);
            let owner = node.data_root.join("daemon/data-owner.lock");
            if retained_native_path_identity(&owner, false)? != node.owner_identity {
                return Err("retained native DATA owner file changed".into());
            }
            let file = std::fs::File::open(&owner).map_err(|e| e.to_string())?;
            fs2::FileExt::try_lock_exclusive(&file)
                .map_err(|e| format!("retained native DATA root is still owned: {e}"))?;
            locks.push(file);
            retained_native_path_identity(&paths.state, false)?;
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
                .map_err(|e| e.to_string())?;
            if state.local_node_id != node.node_id
                || state.cluster_id != primary.cluster_state.cluster_id
                || state.cluster_id != node.cluster_state.cluster_id
                || state.members.len() != 3
                || state.members != node.cluster_state.members
                || state.initialized_at != node.cluster_state.initialized_at
                || state.term < node.cluster_state.term
                || state.kv_revision < node.cluster_state.kv_revision
                || state
                    .members
                    .iter()
                    .map(|m| m.node_id.clone())
                    .collect::<BTreeSet<_>>()
                    != ids
            {
                return Err(
                    "retained native node/cluster/membership state differs from precrash DATA"
                        .into(),
                );
            }
            crate::selected_fixture::SelectedOriginalFixtureIdentity::reject_default_root(
                &crate::default_dfs_metadata_shards_root(&data),
            )
            .map_err(|e| e.to_string())?;
            let wal = paths.kv_paths().raft_wal_paths(&node.node_id);
            if retained_native_path_identity(&wal.root, true)? != node.raft_root_identity
                || retained_native_path_identity(&wal.lock, false)? != node.raft_lock_identity
            {
                return Err("retained native Raft physical owner changed".into());
            }
            for path in [&wal.hard_state, &wal.wal] {
                retained_native_path_identity(path, false)?;
            }
            if wal.snapshot.exists() {
                retained_native_path_identity(&wal.snapshot, false)?;
            }
            let hard = wal.read_persisted_hard_state().map_err(|e| e.to_string())?;
            if hard.current_term < node.raft_term
                || hard.commit_index < node.raft_commit_index
                || hard.commit_index == 0
            {
                return Err("retained native Raft term/commit regressed or is empty".into());
            }
            if retained_native_path_identity(&node.data_root, true)? != node.data_identity
                || retained_native_path_identity(&wal.root, true)? != node.raft_root_identity
            {
                return Err("retained native physical roots changed during validation".into());
            }
        }
        Ok(locks)
    }

    // In-memory reconstruction only; the caller installs any negative original
    // completion scope before opening the existing WAL owners or mounting DFS.
    pub(super) fn reconstruct_retained_native_fixture(
        fixture_root: &Path,
        primary_id: &NodeId,
        nodes: &[RetainedNativeGenesisNode],
    ) -> Result<(Self, SinomoDataDir), String> {
        let primary_data = SinomoDataDir::new(
            &nodes
                .iter()
                .find(|n| n.node_id == *primary_id)
                .ok_or("retained primary missing")?
                .data_root,
        );
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(&primary_data))
                .map_err(|e| e.to_string())?;
        let kv_peers = Arc::new(crate::runtime_host::DfsTestClusterPeerBridge::default());
        let authority = crate::runtime_host::test_cluster_access_with_peers_for_runtime(
            &primary_data,
            Some(Arc::clone(&kv_peers)),
        );
        let nodes = nodes
            .iter()
            .map(|n| {
                (
                    n.node_id.clone(),
                    Node {
                        data_dir: Arc::new(SinomoDataDir::new(&n.data_root)),
                        kv_available: Arc::new(AtomicBool::new(false)),
                        resources: sinomo_device::NodeResourceMonitor::default(),
                        reconciler: None,
                        mounted_context: None,
                        selected_original_fixture: None,
                    },
                )
            })
            .collect();
        let actual = Self {
            root: None,
            retained_fixture_root: Some(fixture_root.to_path_buf()),
            state,
            primary: primary_id.clone(),
            authority,
            kv_peers,
            kv_bootstrapped: true,
            kv_voters: false,
            resident_pins: BTreeMap::new(),
            fabric: Arc::new(Fabric {
                dropped_observations: AtomicUsize::new(0),
                exhausted_budgets: AtomicUsize::new(0),
                next_call: AtomicU64::new(1),
                nodes: Mutex::new(BTreeMap::new()),
                faults: Mutex::new(Vec::new()),
                observations: Mutex::new(Vec::new()),
                observation_checkpoints: Mutex::new(Vec::new()),
                transfers: Mutex::new(Vec::new()),
            }),
            nodes,
            closed: false,
            component_factories: Vec::new(),
        };
        Ok((actual, primary_data))
    }

    pub(super) fn open_retained_native_raft(
        &mut self,
        nodes: &[RetainedNativeGenesisNode],
    ) -> Result<(), String> {
        for node in nodes {
            let paths = ClusterPaths::new(self.data_dir(&node.node_id));
            // Open exactly the existing native WAL owner. This API preserves
            // pending history and does not bootstrap, set a leader or copy logs.
            paths
                .kv_paths()
                .locked_initialize_raft_resident(&node.node_id)
                .map_err(|e| e.to_string())?;
            let pin = sinomo_core::kv::pin_resident_raft_node_registry_for_test(
                &paths.kv_paths().raft_wal_paths(&node.node_id),
            )
            .map_err(|e| e.to_string())?;
            self.resident_pins.insert(node.node_id.clone(), pin);
            let owned = &self.nodes[&node.node_id];
            owned.kv_available.store(true, Ordering::Release);
            self.kv_peers.register(
                node.node_id.clone(),
                (*owned.data_dir).clone(),
                Arc::clone(&owned.kv_available),
            );
        }
        Ok(())
    }
}

/// Parent-owned expected DATA. No transport capability, native owner or
/// Prepared original is serialized by the healthy reconstruction path.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedNativeHealthyFixture {
    pub(crate) format_version: u32,
    pub(crate) fixture_root: PathBuf,
    pub(crate) fixture_identity: (u64, u64),
    pub(crate) primary: NodeId,
    pub(crate) nodes: Vec<RetainedNativeGenesisNode>,
    pub(crate) shard: DfsMetadataShardConfig,
    pub(crate) prefix: DfsMetadataPrefixProof,
    pub(crate) shards: Vec<RetainedNativeHealthyShard>,
    pub(crate) handoff_proof: Option<DfsMetadataShardMembershipHandoffProof>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedNativeHealthyShard {
    pub(crate) node_id: NodeId,
    pub(crate) shard_root: PathBuf,
    pub(crate) shard_identity: (u64, u64),
    pub(crate) prefix: DfsMetadataPrefixProof,
    pub(crate) freeze: Option<DfsMetadataShardWriteFreeze>,
    pub(crate) freeze_bytes: Option<Vec<u8>>,
}

fn healthy_members(shard: &DfsMetadataShardConfig) -> Result<BTreeSet<NodeId>, String> {
    let members = std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    if shard.epoch == 0 || shard.replica_node_ids.len() != 2 || members.len() != 3 {
        return Err("retained healthy scope is not an exact three-member assignment".into());
    }
    Ok(members)
}

fn healthy_freeze_compatible(
    node: &NodeId,
    shard: &DfsMetadataShardConfig,
    prefix: &DfsMetadataPrefixProof,
    root: &Path,
    freeze: &DfsMetadataShardWriteFreeze,
    proof: Option<&DfsMetadataShardMembershipHandoffProof>,
) -> Result<(), String> {
    let proof = proof.ok_or("activated local marker has no actual completed handoff proof")?;
    if proof.cause != DfsMetadataShardHandoffCause::Membership
        || proof.handoff_id != freeze.handoff_id
        || freeze.handoff_id.is_empty()
        || proof.retiring_shard.file_system_id != shard.file_system_id
        || proof.retiring_shard.shard_id != shard.shard_id
        || proof.retiring_shard.epoch != freeze.retiring_epoch
        || proof.desired_shard != *shard
        || freeze.retiring_epoch.checked_add(1) != Some(shard.epoch)
        || freeze.activated_epoch != Some(shard.epoch)
        || freeze.journal_sequence != proof.journal_sequence
        || freeze.state_fingerprint != proof.state_fingerprint
        || !proof.acknowledged_node_ids.contains(node)
        || !crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
            &proof.retiring_shard,
            &proof.desired_shard,
            &proof.acknowledged_node_ids.iter().cloned().collect(),
        )
        || crate::service_sync::inspect_metadata_prefix_descendant(
            root,
            &DfsMetadataShardId::new(&shard.shard_id),
            &freeze.rebind_prefix_proof,
        )
        .map_err(|e| e.to_string())?
        .as_ref()
            != Some(prefix)
    {
        return Err(
            "retained healthy scope has a pending, conflicting or unsupported local freeze".into(),
        );
    }
    Ok(())
}

/// Read under the existing noncreating physical inspection fence. This does
/// not certify or recover storage. A materialized prefix must already match
/// the retained exact target, with no later durable journal frame.
fn observe_healthy_shard(
    node: &NodeId,
    data: &SinomoDataDir,
    shard: &DfsMetadataShardConfig,
    expected_prefix: &DfsMetadataPrefixProof,
    proof: Option<&DfsMetadataShardMembershipHandoffProof>,
) -> Result<RetainedNativeHealthyShard, String> {
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let root = crate::local_dfs_metadata_shard_path(data, &id);
    let identity = retained_native_path_identity(&root, true)?;
    crate::selected_fixture::SelectedOriginalFixtureIdentity::reject_default_root(&root)
        .map_err(|e| e.to_string())?;
    let _guard = crate::metadata_lock::try_metadata_inspection_lock(
        root.clone(),
        crate::metadata_cursor::journal_write_lock_path(&root),
    )
    .map_err(|e| e.to_string())?
    .ok_or("retained healthy shard is busy or lacks its physical writer-lock file")?;
    let mut backend =
        crate::LocalSobjMetadataBackend::open_for_inspection(id.clone(), root.clone())
            .map_err(|e| e.to_string())?;
    backend
        .begin_inspection_snapshot()
        .map_err(|e| e.to_string())?;
    // Reuse the normal genesis owner's read-only record/eviction and exact
    // commit-reference checks. Full3 agreement is not initialization proof;
    // missing or still-Prepared native genesis must never reach a writable
    // reopen which could complete it on behalf of this expected-DATA helper.
    let operation_id = crate::default_cluster::genesis::genesis_operation_id(shard);
    let tables = backend
        .current_tables_snapshot()
        .map_err(|e| e.to_string())?;
    let (digest, status) = if let Some(record) = tables.prepared_operations_v7.get(&operation_id) {
        record.validate().map_err(|e| e.to_string())?;
        (&record.digest, record.status)
    } else if let Some(witness) = tables.prepared_operation_evictions.get(&operation_id) {
        witness.validate().map_err(|e| e.to_string())?;
        (&witness.digest, witness.status)
    } else {
        return Err("retained healthy shard lacks its actual committed native genesis".into());
    };
    if digest.file_system_id != shard.file_system_id
        || digest.shard_id != id
        || digest.owner_epoch != 1
        || digest.operation_id != operation_id
        || status != DfsPreparedOperationStatus::Committed
        || backend
            .committed_prepared_operation(digest)
            .map_err(|e| e.to_string())?
            .is_none()
    {
        return Err(
            "retained healthy shard genesis is incomplete or has a foreign original".into(),
        );
    }
    let prefix = backend.metadata_prefix_proof().map_err(|e| e.to_string())?;
    let hinted_next = crate::metadata_journal::read_next_journal_sequence_lenient(&root)
        .map_err(|e| e.to_string())?;
    let materialized_next = prefix.journal_sequence.checked_add(1)
        .ok_or("retained healthy prefix sequence overflow")?;
    // Same read-only join as LocalSobjMetadataBackend::open_for_inspection:
    // head.sobj is a derived hint, not a certified metadata prefix. A stale
    // lower hint cannot erase materialized history; an ahead hint still refuses.
    let logical_next = hinted_next.max(materialized_next).max(1);
    let later = crate::metadata_journal::durable_journal_entries_since_limited(
        &root, prefix.journal_sequence, 1,
    ).map_err(|e| e.to_string())?;
    eprintln!("RETAINED_HEALTHY_PREFIX node={node} actual={prefix:?} expected={expected_prefix:?} hint_next={hinted_next} logical_next={logical_next} later_sequences={:?}",
        later.iter().map(|entry|entry.sequence).collect::<Vec<_>>());
    if !expected_prefix.is_well_formed()
        || &prefix != expected_prefix
        || logical_next != materialized_next
        || !later.is_empty()
    {
        return Err("retained healthy shard prefix differs from the complete native round".into());
    }
    let path = root.join("journal/membership-handoff-write-freeze.sobj");
    let freeze_bytes = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if metadata.len() > 1024 * 1024 {
                return Err("retained healthy freeze exceeds 1MiB".into());
            }
            retained_native_path_identity(&path, false)?;
            Some(std::fs::read(&path).map_err(|e| e.to_string())?)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.to_string()),
    };
    let freeze = crate::metadata_api::read_handoff_freeze_with_layout(&path, None)
        .map_err(|e| e.to_string())?;
    if let Some(freeze) = &freeze {
        healthy_freeze_compatible(node, shard, &prefix, &root, freeze, proof)?;
    }
    if freeze.is_some() != freeze_bytes.is_some()
        || freeze_bytes
            .as_ref()
            .is_some_and(|bytes| std::fs::read(&path).as_ref().ok() != Some(bytes))
        || retained_native_path_identity(&root, true)? != identity
    {
        return Err("retained healthy shard identity or freeze changed during inspection".into());
    }
    Ok(RetainedNativeHealthyShard {
        node_id: node.clone(),
        shard_root: root,
        shard_identity: identity,
        prefix,
        freeze,
        freeze_bytes,
    })
}

impl DfsTestRuntime {
    /// Actual native authoritative read, never a materialized KV file or a
    /// completed-handoff interpretation of a still-present pending record.
    fn retained_healthy_control(
        &self,
        shard: &DfsMetadataShardConfig,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>, String> {
        let keys = vec![
            crate::metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            crate::metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id),
            crate::metadata_shard_membership_handoff_proof_key(
                &shard.file_system_id,
                &shard.shard_id,
            ),
        ];
        let mut proof = None;
        let authority = Arc::clone(&self.authority);
        run_unit(Box::pin(async {
            let read = tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                authority.kv_get_authoritative_exact_set_async(keys.clone()),
            )
            .await
            .map_err(|_| "retained healthy real ReadIndex exceeded five seconds".to_string())?
            .map_err(|e| e.to_string())?;
            let [Some(assignment), None, completed] = read.value.as_slice() else {
                return Err("retained healthy ReadIndex requires the exact assignment and no pending handoff".into());
            };
            if read
                .value
                .iter()
                .zip(&keys)
                .any(|(entry, key)| entry.as_ref().is_some_and(|entry| &entry.key != key))
                || assignment
                    .decode::<DfsMetadataShardConfig>()
                    .map_err(|e| e.to_string())?
                    != *shard
            {
                return Err("retained healthy authoritative assignment changed".into());
            }
            proof = completed
                .as_ref()
                .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoffProof>())
                .transpose()
                .map_err(|e| e.to_string())?;
            Ok(())
        }))?;
        Ok(proof)
    }

    pub(crate) fn capture_retained_healthy_fixture(
        &self,
        round: &crate::node_transport::OwnedNativeMetadataPrefixRound,
    ) -> Result<RetainedNativeHealthyFixture, String> {
        if !round.is_complete()
            || self.nodes.len() != 3
            || !self.component_factories.is_empty()
            || self
                .nodes
                .values()
                .any(|node| node.selected_original_fixture.is_some() || node.reconciler.is_none())
        {
            return Err("healthy retained capture requires a complete native round and three mounted default P0 owners".into());
        }
        let source = round
            .common_source_observation()
            .ok_or("native round has no common source")?;
        let shard = source.assignment();
        let members = healthy_members(shard)?;
        if members != self.nodes.keys().cloned().collect()
            || round.peer_outcomes().len() != 2
            || round
                .peer_outcomes()
                .iter()
                .map(|peer| peer.peer().clone())
                .collect::<BTreeSet<_>>()
                != shard.replica_node_ids.iter().cloned().collect()
        {
            return Err("native round and actual fixture membership differ".into());
        }
        let source_data = self.data_dir(&shard.primary_node_id);
        for peer in round.peer_outcomes() {
            peer.native_return()
                .ok_or("complete round lacks an actual native peer return")?
                .validate_binding(peer.peer(), peer.command(), &source_data.root())
                .map_err(|e| e.to_string())?;
        }
        let proof = self.retained_healthy_control(shard)?;
        let (fixture_root, nodes) = self.capture_retained_native_nodes()?;
        if nodes.iter().any(|node| {
            node.cluster_state.local_node_id != node.node_id
                || node.cluster_state.cluster_id != self.state.cluster_id
                || node.cluster_state.members.len() != 3
                || node
                    .cluster_state
                    .members
                    .iter()
                    .map(|member| member.node_id.clone())
                    .collect::<BTreeSet<_>>()
                    != members
                || node.raft_commit_index == 0
        }) {
            return Err(
                "actual healthy node/cluster/Raft roster is not the complete fixture".into(),
            );
        }
        let source_node = nodes
            .iter()
            .find(|node| node.node_id == shard.primary_node_id)
            .ok_or("native source owner missing")?;
        let physical = source.physical_owner();
        if physical.data_root != source_node.data_root
            || physical.data_identity != source_node.data_identity
            || physical.owner_identity != source_node.owner_identity
        {
            return Err("native round source physical owner differs from actual fixture".into());
        }
        let mut shards = Vec::new();
        for node in &nodes {
            let data = self.data_dir(&node.node_id);
            let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
                &crate::runtime_host::DfsRuntimePaths::new(data),
            )
            .map_err(|e| e.to_string())?;
            let observed =
                observe_healthy_shard(&node.node_id, data, shard, source.prefix(), proof.as_ref())?;
            if !owner.owns_backend_root(&observed.shard_root)
                || (node.node_id == shard.primary_node_id
                    && (physical.shard_root != observed.shard_root
                        || physical.shard_identity != observed.shard_identity))
            {
                return Err("healthy shard is not the round's actual owning physical root".into());
            }
            shards.push(observed);
        }
        if self.retained_healthy_control(shard)? != proof {
            return Err("healthy assignment/proof changed while capturing physical DATA".into());
        }
        let result = RetainedNativeHealthyFixture {
            format_version: 1,
            fixture_identity: retained_native_path_identity(&fixture_root, true)?,
            fixture_root,
            primary: self.primary.clone(),
            nodes,
            shard: shard.clone(),
            prefix: source.prefix().clone(),
            shards,
            handoff_proof: proof,
        };
        if serde_json::to_vec(&result)
            .map_err(|e| e.to_string())?
            .len()
            > 1024 * 1024
        {
            return Err("retained healthy fixture exceeds 1MiB".into());
        }
        Ok(result)
    }

    pub(crate) fn validate_retained_healthy_fixture(
        expected: &RetainedNativeHealthyFixture,
    ) -> Result<(), String> {
        let members = healthy_members(&expected.shard)?;
        if expected.format_version != 1
            || expected.nodes.len() != 3
            || expected.shards.len() != 3
            || serde_json::to_vec(expected)
                .map_err(|e| e.to_string())?
                .len()
                > 1024 * 1024
            || retained_native_path_identity(&expected.fixture_root, true)?
                != expected.fixture_identity
            || members != expected.nodes.iter().map(|n| n.node_id.clone()).collect()
            || members != expected.shards.iter().map(|s| s.node_id.clone()).collect()
        {
            return Err("retained healthy version, bounds, roster or fixture root differs".into());
        }
        let _locks = Self::validate_retained_native_nodes(
            &expected.fixture_root,
            &expected.primary,
            &expected.nodes,
        )?;
        for shard in &expected.shards {
            let node = expected
                .nodes
                .iter()
                .find(|node| node.node_id == shard.node_id)
                .ok_or("retained healthy shard owner missing")?;
            let observed = observe_healthy_shard(
                &node.node_id,
                &SinomoDataDir::new(&node.data_root),
                &expected.shard,
                &expected.prefix,
                expected.handoff_proof.as_ref(),
            )?;
            if observed != *shard {
                return Err(
                    "retained healthy physical shard, prefix or exact freeze bytes changed".into(),
                );
            }
        }
        if retained_native_path_identity(&expected.fixture_root, true)? != expected.fixture_identity
        {
            return Err("retained healthy fixture root changed during validation".into());
        }
        Ok(())
    }

    pub(crate) fn reopen_retained_healthy_fixture(
        expected: RetainedNativeHealthyFixture,
    ) -> Result<(Self, Result<(), String>), String> {
        Self::validate_retained_healthy_fixture(&expected)?;
        let (mut actual, _) = Self::reconstruct_retained_native_fixture(
            &expected.fixture_root,
            &expected.primary,
            &expected.nodes,
        )?;
        // No genesis selection/hook, bootstrap, set-leader, log copy or
        // projection preparation. Only the pre-existing native WAL owners.
        actual.open_retained_native_raft(&expected.nodes)?;
        if actual.retained_healthy_control(&expected.shard)? != expected.handoff_proof {
            return Err(
                "retained healthy completed handoff proof changed at actual ReadIndex".into(),
            );
        }
        let ids = expected
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect();
        let startup = actual.start_nodes_after_projection(
            ids,
            crate::service_trace::DfsPhaseTimer::new("retained-native-healthy-restart"),
        );
        if startup.is_ok() {
            let check = (|| {
                if actual.retained_healthy_control(&expected.shard)? != expected.handoff_proof {
                    return Err("healthy assignment/proof changed during native startup".into());
                }
                for node in &expected.nodes {
                    actual.component_context(&node.node_id)?;
                    let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
                        &crate::runtime_host::DfsRuntimePaths::new(actual.data_dir(&node.node_id)),
                    )
                    .map_err(|e| e.to_string())?;
                    if owner.physical_data_root() != node.data_root
                        || owner
                            .storage_owner_file_identity()
                            .map_err(|e| e.to_string())?
                            != node.owner_identity
                        || retained_native_path_identity(&node.data_root, true)?
                            != node.data_identity
                        || owner
                            .selected_original_data_layout()
                            .map_err(|e| e.to_string())?
                            .is_some()
                    {
                        return Err("healthy native startup changed its actual DATA owner".into());
                    }
                    let observed = observe_healthy_shard(
                        &node.node_id,
                        actual.data_dir(&node.node_id),
                        &expected.shard,
                        &expected.prefix,
                        expected.handoff_proof.as_ref(),
                    )?;
                    let before = expected
                        .shards
                        .iter()
                        .find(|s| s.node_id == node.node_id)
                        .ok_or("healthy shard expectation missing")?;
                    if observed != *before {
                        return Err(
                            "healthy native startup changed retained physical prefix/freeze DATA"
                                .into(),
                        );
                    }
                }
                Ok::<(), String>(())
            })();
            if let Err(error) = check {
                let cleanup = actual.close_inner(None);
                return Err(format!("{error}; native cleanup: {cleanup:?}"));
            }
        }
        Ok((actual, startup))
    }
}
