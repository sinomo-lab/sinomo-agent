//! Complete-filesystem continuation owned by the same component worker as
//! physical units. Peer handlers only retain immutable requests and wake work;
//! existing source sessions and native import plans own conversion/effects.
use super::*;
use crate::metadata::canonical_generation::{
    CapturedMetadataUnit, decode_captured_unit, encode_captured_unit,
};
use crate::namespace_count_cohort::{NamespaceCountCohortSeal, storage_catalog_digest};
use crate::namespace_counts::{NamespaceCountMaintenanceLimits, NamespaceCountMaintenanceSource};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsFileSystemConfig, DfsFileSystemId, DfsMetadataShardId, DfsMetadataStorageState,
};

pub(in crate::authority) const ROUTE: &str = "/dfs-migration/filesystem-work-v1";
const MAX_WIRE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SOURCE_RECORDS: usize = 1_000_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct Invocation {
    version: u16,
    original_file_system: KvEntry,
    catalog: Vec<DfsMetadataShardConfig>,
    catalog_entries: Vec<KvEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    placement_goals: Vec<placement_work::OriginalPlacementGoal>,
    coordinator: NodeId,
    transition_id: String,
    #[serde(with = "invocation_time")]
    at: time::OffsetDateTime,
}
impl Invocation {
    fn expected_transition_id(&self) -> Result<String> {
        let original = digest(&("metadata-storage-transition", &self.original_file_system, &self.catalog_entries, self.at))?;
        if self.placement_goals.is_empty() { Ok(original) }
        else { digest(&("metadata-storage-placement-evidence-v1", original, &self.placement_goals)) }
    }
    fn placement_evidence(&self, shard: &DfsMetadataShardId) -> Result<Vec<crate::metadata::RetainedPlacementReservation>> {
        let mut evidence = self.placement_goals.iter().map(|goal| goal.evidence(shard)).collect::<Result<Vec<_>>>()?;
        evidence.sort_by_key(|item| item.reference().expect("validated original capsule"));
        Ok(evidence)
    }
    fn validate_placement_sources(&self, sources: &[NamespaceCountMaintenanceSource]) -> Result<()> {
        for source in sources {
            source.captured.validate()?;
            if source.captured.source.retained_placement_reservations
                != self.placement_evidence(&source.captured.source.physical_shard)? {
                return Err(error("filesystem source changes or omits original placement evidence"));
            }
        }
        Ok(())
    }
    fn file_system(&self) -> Result<DfsFileSystemConfig> {
        let config: DfsFileSystemConfig = self.original_file_system.decode().map_err(error)?;
        let mut goals = BTreeSet::new();
        for goal in &self.placement_goals {
            goal.validate_catalog(&self.catalog)?;
            if goal.file_system_id() != &config.file_system_id || !goals.insert(digest(goal)?) {
                return Err(error("filesystem invocation changes or repeats an original placement goal"));
            }
        }
        config
            .catalog_generation
            .checked_add(u64::try_from(self.catalog.len()).map_err(error)?)
            .ok_or_else(|| {
                error("original filesystem catalog generation cannot hold the complete activation")
            })?;
        for shard in &self.catalog {
            shard
                .epoch
                .checked_add(1)
                .ok_or_else(|| error("original source authority epoch is exhausted"))?;
            inventory::UnitKey {
                relative_root: Path::new("dfs/metadata").join(&shard.shard_id),
                role: inventory::UnitRole::Metadata {
                    shard: DfsMetadataShardId::new(&shard.shard_id),
                },
            }
            .validate()?;
        }
        let decoded = self
            .catalog_entries
            .iter()
            .map(|entry| {
                let shard: DfsMetadataShardConfig = entry.decode().map_err(error)?;
                if entry.key
                    != sinomo_dfs::metadata_shard_key(&shard.file_system_id, &shard.shard_id)
                {
                    return Err(error("original catalog key differs from source scope"));
                }
                Ok(shard)
            })
            .collect::<Result<Vec<_>>>()?;
        if decoded != self.catalog
            || self.version != 1
            || self.catalog.is_empty()
            || self.original_file_system.key
                != sinomo_dfs::file_system_config_key(&config.file_system_id)
            || self
                .catalog
                .iter()
                .any(|shard| shard.file_system_id != config.file_system_id)
            || self
                .catalog
                .windows(2)
                .any(|pair| pair[0].shard_id >= pair[1].shard_id)
            || self
                .catalog
                .iter()
                .map(|shard| &shard.primary_node_id)
                .min()
                != Some(&self.coordinator)
            || self.transition_id != self.expected_transition_id()?
        {
            return Err(error(
                "filesystem continuation changed its original complete catalog invocation",
            ));
        }
        if matches!(
            config.metadata_storage,
            Some(DfsMetadataStorageState::PendingTransition { .. })
        ) {
            return Err(error(
                "filesystem invocation predecessor is already pending",
            ));
        }
        Ok(config)
    }
    fn unit(&self) -> Result<inventory::UnitKey> {
        let config = self.file_system()?;
        Ok(inventory::UnitKey {
            relative_root: "dfs/metadata".into(),
            role: inventory::UnitRole::FileSystem {
                file_system_id: config.file_system_id,
                source_catalog_generation: config.catalog_generation,
                target_format: u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
            },
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) enum Request {
    Wake {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    },
    Capture {
        invocation: Invocation,
    },
    Enroll {
        invocation: Invocation,
        pending: DfsFileSystemConfig,
        sources: Vec<Vec<u8>>,
    },
    Status {
        invocation: Invocation,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::authority) enum Response {
    Pending,
    Captured { sources: Vec<Vec<u8>> },
    Enrolled { cohort_id: String },
    Activated,
}

fn write_immutable(path: &Path, value: &impl Serialize) -> Result<()> {
    crate::metadata::write_count_cohort_checkpoint(path, value)
}
fn read_bounded<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes =
        sinomo_core::storage::durable_file::read_object_file_bytes(path, MAX_WIRE_BYTES as u64)
            .map_err(error)?;
    serde_json::from_slice(&bytes).map_err(error)
}
fn source_set(wires: &[Vec<u8>]) -> Result<Vec<NamespaceCountMaintenanceSource>> {
    let total = wires
        .iter()
        .try_fold(0usize, |total, wire| total.checked_add(wire.len()))
        .ok_or_else(|| error("filesystem captured source byte budget overflow"))?;
    if total > MAX_WIRE_BYTES {
        return Err(error(
            "filesystem source set exceeds its bounded DATA budget",
        ));
    }
    wires
        .iter()
        .map(|wire| {
            decode_captured_unit(wire, MAX_WIRE_BYTES, MAX_SOURCE_RECORDS).map(|captured| {
                NamespaceCountMaintenanceSource {
                    captured: Arc::new(captured),
                }
            })
        })
        .collect()
}

impl MigrationOwner {
    fn filesystem_job_root(&self, invocation: &Invocation) -> Result<PathBuf> {
        let base = self
            .authority
            .data_root
            .join("dfs/migration-owner/filesystems");
        sinomo_core::storage::durable_file::ensure_directory(&base).map_err(error)?;
        let root = base.join(invocation.unit()?.id()?);
        sinomo_core::storage::durable_file::ensure_directory(&root).map_err(error)?;
        Ok(root)
    }
    fn retain_filesystem_invocation(&self, invocation: &Invocation) -> Result<PathBuf> {
        let root = self.filesystem_job_root(invocation)?;
        write_replay_compatible(&root.join("invocation.json"), invocation)?;
        let unit = invocation.unit()?;
        let mut reference = self
            .work
            .inventory
            .read(&unit)?
            .unwrap_or_else(|| inventory::UnitReference::discovered(unit.clone()));
        reference.job = Some(inventory::JobReference {
            checkpoint: root
                .join("invocation.json")
                .strip_prefix(&self.authority.data_root)
                .map_err(error)?
                .to_owned(),
            identity: invocation.transition_id.clone(),
        });
        self.work.inventory.record(&reference)?;
        let mut scheduler = self.work.scheduler.lock().map_err(error)?;
        scheduler.recovered(reference)?;
        scheduler.enqueue(unit, false)?;
        drop(scheduler);
        self.work.wake.notify_one();
        Ok(root)
    }
    pub(in crate::authority) fn receive_filesystem_work(
        &self,
        source: &NodeId,
        request: Request,
    ) -> Result<Response> {
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("filesystem work owner lifecycle closed"));
        }
        if let Request::Wake {
            file_system_id,
            shard_id,
        } = &request
        {
            let (entry, catalog, _) = self.filesystem_catalog(file_system_id)?;
            let config: DfsFileSystemConfig = entry.decode().map_err(error)?;
            if catalog.iter().map(|shard| &shard.primary_node_id).min()
                != Some(&self.authority.local_node()?)
                || !catalog
                    .iter()
                    .any(|shard| shard.shard_id == shard_id.0 && &shard.primary_node_id == source)
            {
                return Err(error(
                    "filesystem wake differs from its actual retiring source/coordinator",
                ));
            }
            if matches!(
                config.metadata_storage,
                Some(DfsMetadataStorageState::Active { target_format, .. }) if target_format >= u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
            ) {
                return Ok(Response::Pending);
            }
            self.enqueue_filesystem(&config)?;
            return Ok(Response::Pending);
        }
        let invocation = match &request {
            Request::Wake { .. } => unreachable!("handled wake"),
            Request::Capture { invocation }
            | Request::Enroll { invocation, .. }
            | Request::Status { invocation } => invocation,
        };
        let config = invocation.file_system()?;
        let local_node = self.authority.local_node()?;
        if source != &invocation.coordinator
            || !invocation
                .catalog
                .iter()
                .any(|shard| shard.primary_node_id == local_node)
        {
            return Err(error(
                "filesystem request is not from its deterministic original coordinator to a retiring primary",
            ));
        }
        let (entries, _) = self
            .authority
            .read_exact(&[sinomo_dfs::file_system_config_key(&config.file_system_id)])?;
        let actual: DfsFileSystemConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("filesystem request has no actual native configuration"))?
            .decode()
            .map_err(error)?;
        let same_original = entries[0].entry.as_ref() == Some(&invocation.original_file_system);
        let same_pending = matches!(&actual.metadata_storage, Some(DfsMetadataStorageState::PendingTransition { transition_id, source_catalog_generation, .. })
            if transition_id == &invocation.transition_id && *source_catalog_generation == config.catalog_generation);
        let root = self.filesystem_job_root(invocation)?;
        let completed = root.join("cohort.json").is_file() && {
            let seal: NamespaceCountCohortSeal = read_bounded(&root.join("cohort.json"))?;
            self.authority.published_count_cohort(&seal)?
        };
        if !same_original && !same_pending && !completed {
            return Err(error(
                "filesystem work no longer matches its original configuration or exact transition",
            ));
        }
        if let Request::Enroll { pending, .. } = &request {
            if pending.file_system_id != config.file_system_id
                || pending.catalog_generation != config.catalog_generation
                || (!completed && pending.metadata_storage != actual.metadata_storage)
            {
                return Err(error(
                    "filesystem enrollment differs from the actual original Pending transition",
                ));
            }
        }
        // Immutable inbox DATA is durable before acknowledgement. Conversion,
        // census and native writes only run in the already-owned worker.
        let root = self.retain_filesystem_invocation(invocation)?;
        if matches!(request, Request::Enroll { .. }) {
            write_replay_compatible(&root.join("enroll.json"), &request)?;
        }
        if completed {
            return Ok(Response::Activated);
        }
        if root.join("cohort.json").is_file() {
            let seal: NamespaceCountCohortSeal = read_bounded(&root.join("cohort.json"))?;
            return Ok(Response::Enrolled {
                cohort_id: seal.cohort_id,
            });
        }
        if root.join("captured.json").is_file() {
            return Ok(Response::Captured {
                sources: self.filesystem_sources(&root)?,
            });
        }
        Ok(Response::Pending)
    }
    fn filesystem_peer(&self, target: &NodeId, request: Request) -> Result<Response> {
        let local = self.authority.local_node()?;
        if target == &local {
            return self.receive_filesystem_work(&local, request);
        }
        let transport = self
            .authority
            .transport
            .as_ref()
            .ok_or_else(|| error("filesystem continuation has no native peer transport"))?;
        let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
            ROUTE,
            "dfs-migration",
            &request,
        )
        .map_err(|failure| error(format!("filesystem work RPC encoding failed: {failure}")))?;
        let response = match block_native(
            &self.authority.runtime,
            transport.call_async(target, call, source_effect_rpc_options(), true),
        ) {
            Ok(response) => response,
            Err(failure) => {
                // No acknowledged DATA or effect is inferred from transport
                // failure. The same immutable request remains owned for retry.
                tracing::debug!(target=%target, error=%failure, "filesystem source peer remains unavailable");
                return Ok(Response::Pending);
            }
        };
        block_native(
            &self.authority.runtime,
            response.into_materialized_value_response(MAX_WIRE_BYTES),
        )?
        .decode_body()
        .map_err(error)
    }
}

pub(in crate::authority) fn receive(
    context: &sinomo_module::ComponentContext,
    source: NodeId,
    request: Request,
) -> Result<Response> {
    let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
        context.data_dir(),
        context.host_role(),
    );
    let owner = OWNERS
        .get()
        .ok_or_else(|| error("filesystem request has no mounted migration owner"))?
        .lock()
        .map_err(error)?
        .get(&paths.root)
        .and_then(Weak::upgrade)
        .ok_or_else(|| error("filesystem request owner is unavailable"))?;
    owner.receive_filesystem_work(&source, request)
}

impl MigrationOwner {
    fn filesystem_sources(&self, root: &Path) -> Result<Vec<Vec<u8>>> {
        let paths: Vec<PathBuf> = read_bounded(&root.join("captured.json"))?;
        paths
            .iter()
            .map(|path| {
                if !path.starts_with(&self.authority.data_root)
                    || path.file_name().and_then(|name| name.to_str()) != Some("captured-unit.json")
                {
                    return Err(error(
                        "filesystem source reference leaves its actual source owner",
                    ));
                }
                let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(
                    path,
                    MAX_WIRE_BYTES as u64,
                )
                .map_err(error)?;
                let mut captured = decode_captured_unit(&bytes, MAX_WIRE_BYTES, MAX_SOURCE_RECORDS)?;
                captured.load_recovery_evidence(path)?;
                encode_captured_unit(&captured, MAX_WIRE_BYTES)
            })
            .collect()
    }
    fn capture_filesystem_sources(&self, invocation: &Invocation, root: &Path) -> Result<sinomo_dfs::DfsStoragePreparation> {
        if root.join("captured.json").is_file() {
            if root.join("enroll.json").is_file() {
                // This immutable inbox was written only after complete-source
                // qualification. Enrollment below still replays each local source.
                return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
            }
            // An older complete capture can still contain unresolved V7 work.
            // Re-enter its fenced replay to qualify new recovery evidence;
            // never replace the original capture or treat missing evidence as Ready.
            let paths: Vec<PathBuf> = read_bounded(&root.join("captured.json"))?;
            for path in paths {
                if !path.starts_with(&self.authority.data_root)
                    || path.file_name().and_then(|name| name.to_str()) != Some("captured-unit.json") {
                    return Err(error("filesystem source reference leaves its actual source owner"));
                }
                let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(&path, MAX_WIRE_BYTES as u64).map_err(error)?;
                let mut captured = decode_captured_unit(&bytes, MAX_WIRE_BYTES, MAX_SOURCE_RECORDS)?;
                captured.load_recovery_evidence(&path)?;
                if captured.has_unresolved_work() {
                    let shard = &captured.source.physical_shard;
                    let (progress, resumed) = self.metadata.resume_captured_metadata_unit(
                        &self.metadata_root.join(&shard.0), shard, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
                    )?;
                    if resumed.is_none() { return Ok(progress); }
                }
            }
            return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
        }
        let local = self.authority.local_node()?;
        let mut captured_paths = Vec::new();
        for shard in invocation
            .catalog
            .iter()
            .filter(|shard| shard.primary_node_id == local)
        {
            let physical = DfsMetadataShardId::new(&shard.shard_id);
            let (progress, source) = self.metadata.capture_namespace_count_source(
                &self.metadata_root.join(&shard.shard_id),
                &physical,
                &invocation.catalog,
                &invocation.placement_evidence(&physical)?,
                sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
            )?;
            let Some(source) = source else { return Ok(progress); };
            captured_paths.push(
                source
                    .captured
                    .source
                    .evidence_root
                    .join("captured-unit.json"),
            );
        }
        if captured_paths.is_empty() {
            return Err(error(
                "filesystem capture has no locally owned original source",
            ));
        }
        write_immutable(&root.join("captured.json"), &captured_paths)?;
        Ok(sinomo_dfs::DfsStoragePreparation::Ready)
    }
    fn pending_filesystem(
        &self,
        invocation: &Invocation,
        root: &Path,
        sources: &[NamespaceCountMaintenanceSource],
    ) -> Result<Option<DfsFileSystemConfig>> {
        let original = invocation.file_system()?;
        invocation.validate_placement_sources(sources)?;
        original
            .catalog_generation
            .checked_add(u64::try_from(invocation.catalog.len()).map_err(error)?)
            .ok_or_else(|| {
                error("filesystem catalog generation cannot hold every original source activation")
            })?;
        for shard in &invocation.catalog {
            shard
                .epoch
                .checked_add(1)
                .ok_or_else(|| error("filesystem source epoch cannot advance before Pending"))?;
        }
        let pending_path = root.join("pending-request.json");
        let request: KvTxnRequest = if pending_path.is_file() {
            read_bounded(&pending_path)?
        } else {
            // Complete-cut validation precedes Pending. Unfinished historical
            // operations are explicit blockers; no live target opens them.
            crate::namespace_counts::plan_namespace_count_maintenance(
                &invocation.catalog,
                sources,
                NamespaceCountMaintenanceLimits {
                    max_objects: MAX_SOURCE_RECORDS,
                    max_encoded_bytes: MAX_WIRE_BYTES,
                    deadline: std::time::Instant::now()
                        + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                },
            )?;
            let generation = digest(&(
                "current-metadata-generation",
                &invocation.transition_id,
                sources
                    .iter()
                    .map(|source| source.captured.projection_fingerprint())
                    .collect::<Vec<_>>(),
                sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            ))?;
            let (fs_compare, fs_put) = sinomo_dfs::metadata_storage_pending_transition_update(
                &invocation.original_file_system,
                invocation.transition_id.clone(),
                generation,
                u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
                storage_catalog_digest(&invocation.catalog)?,
            )
            .map_err(error)?;
            let mut compares = vec![fs_compare];
            for entry in &invocation.catalog_entries {
                compares.push(KvCompare::ValueEquals {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                });
            }
            for goal in &invocation.placement_goals { compares.extend(goal.source_compares()); }
            let request = KvTxnRequest::new(
                KvRequestId::derived(
                    "storage-transition-pending",
                    invocation.transition_id.as_bytes(),
                ),
                KvTxn {
                    compares,
                    success: vec![fs_put],
                    failure: Vec::new(),
                },
            );
            write_immutable(&pending_path, &request)?;
            request
        };
        let committed = match self.authority.commit_evaluated(request) {
            Ok(committed) => committed,
            Err(NativeEvaluationError::Native(failure)) => {
                tracing::warn!(error=%failure, "filesystem Pending retains its exact uncertain original request");
                return Ok(None);
            }
            Err(NativeEvaluationError::Domain(failure)) => return Err(failure),
        };
        NativeImportAuthority::require_committed(&committed)?;
        let (entries, _) = self
            .authority
            .read_exact(&[sinomo_dfs::file_system_config_key(&original.file_system_id)])?;
        let config: DfsFileSystemConfig = entries[0]
            .entry
            .as_ref()
            .ok_or_else(|| error("committed pending filesystem disappeared"))?
            .decode()
            .map_err(error)?;
        if !matches!(&config.metadata_storage, Some(DfsMetadataStorageState::PendingTransition { transition_id, source_catalog_generation, .. })
            if transition_id == &invocation.transition_id && *source_catalog_generation == original.catalog_generation)
        {
            return Err(error(
                "filesystem Pending receipt differs from actual transition",
            ));
        }
        Ok(Some(config))
    }
}

impl MigrationOwner {
    fn filesystem_catalog(
        &self,
        fs: &DfsFileSystemId,
    ) -> Result<(KvEntry, Vec<DfsMetadataShardConfig>, Vec<KvEntry>)> {
        // Read the generation-bearing filesystem first. Every catalog mutation
        // changes that same entry, closing the insertion/removal phantom cut.
        let (entries, _) = self
            .authority
            .read_exact(&[sinomo_dfs::file_system_config_key(fs)])?;
        let entry = entries[0]
            .entry
            .clone()
            .ok_or_else(|| error("filesystem conversion has no current filesystem"))?;
        let prefix = sinomo_dfs::metadata_shards_prefix(fs);
        let read = self.authority.read_native(
            self.authority
                .cluster()?
                .kv_range_prefix_complete_authoritative_async(prefix.clone()),
        )?;
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(
            &prefix, &read,
        )
        .map_err(error)?;
        let mut catalog_entries = read.value;
        catalog_entries.sort_by(|a, b| a.key.cmp(&b.key));
        let mut catalog = catalog_entries
            .iter()
            .map(|entry| entry.decode::<DfsMetadataShardConfig>().map_err(error))
            .collect::<Result<Vec<_>>>()?;
        catalog.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
        if catalog.is_empty() || catalog.iter().any(|shard| &shard.file_system_id != fs) {
            return Err(error(
                "filesystem conversion has no complete owning catalog",
            ));
        }
        Ok((entry, catalog, catalog_entries))
    }
    pub(super) fn enqueue_filesystem(
        &self,
        config: &DfsFileSystemConfig,
    ) -> Result<inventory::UnitKey> {
        let source_catalog_generation = match &config.metadata_storage {
            Some(DfsMetadataStorageState::PendingTransition {
                source_catalog_generation,
                ..
            }) => *source_catalog_generation,
            _ => config.catalog_generation,
        };
        let unit = inventory::UnitKey {
            relative_root: "dfs/metadata".into(),
            role: inventory::UnitRole::FileSystem {
                file_system_id: config.file_system_id.clone(),
                source_catalog_generation,
                target_format: u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
            },
        };
        let reference = self
            .work
            .inventory
            .read(&unit)?
            .unwrap_or_else(|| inventory::UnitReference::discovered(unit.clone()));
        self.work.inventory.record(&reference)?;
        let mut scheduler = self.work.scheduler.lock().map_err(error)?;
        scheduler.recovered(reference)?;
        scheduler.enqueue(unit.clone(), false)?;
        drop(scheduler);
        self.work.wake.notify_one();
        Ok(unit)
    }
    fn start_filesystem_invocation(&self, unit: &inventory::UnitKey) -> Result<Invocation> {
        let inventory::UnitRole::FileSystem {
            file_system_id,
            source_catalog_generation,
            target_format,
        } = &unit.role
        else {
            return Err(error("not a filesystem work unit"));
        };
        let (entry, catalog, catalog_entries) = self.filesystem_catalog(file_system_id)?;
        let config: DfsFileSystemConfig = entry.decode().map_err(error)?;
        let predecessor_allowed = match &config.metadata_storage {
            None => true,
            Some(DfsMetadataStorageState::Active {
                target_format: prior_format,
                ..
            }) => *prior_format < *target_format,
            Some(DfsMetadataStorageState::PendingTransition { .. }) => false,
        };
        if config.catalog_generation != *source_catalog_generation
            || !predecessor_allowed
            || *target_format != u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        {
            return Err(error(
                "new filesystem invocation cannot replace an active or pending storage lineage",
            ));
        }
        let coordinator = catalog
            .iter()
            .map(|shard| shard.primary_node_id.clone())
            .min()
            .ok_or_else(|| error("empty filesystem catalog"))?;
        if coordinator != self.authority.local_node()? {
            return Err(error(
                "only deterministic retiring coordinator may create an invocation",
            ));
        }
        self.authority.require_count_capabilities(&catalog)?;
        let Some(goals) = self.authority.capture_original_placement_goals(sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET)? else {
            return Err(MetadataBackendError::StoragePreparationPending { root: self.metadata_root.clone(), processed_records: 0 });
        };
        let placement_goals = goals.into_iter().filter(|goal| !goal.is_terminal() && goal.file_system_id() == file_system_id).collect();
        let at = time::OffsetDateTime::now_utc();
        let mut invocation = Invocation {
            version: 1,
            transition_id: String::new(),
            original_file_system: entry,
            catalog,
            catalog_entries,
            placement_goals,
            coordinator,
            at,
        };
        invocation.transition_id = invocation.expected_transition_id()?;
        invocation.file_system()?;
        self.retain_filesystem_invocation(&invocation)?;
        Ok(invocation)
    }
}

impl MigrationOwner {
    fn filesystem_invocation_for(
        &self,
        reference: &mut inventory::UnitReference,
    ) -> Result<(Invocation, PathBuf)> {
        if let Some(job) = &reference.job {
            let path = self.authority.data_root.join(&job.checkpoint);
            let invocation: Invocation = read_bounded(&path)?;
            if invocation.unit()? != reference.unit || invocation.transition_id != job.identity {
                return Err(error(
                    "filesystem inventory changed its original invocation",
                ));
            }
            let root = self.filesystem_job_root(&invocation)?;
            if path != root.join("invocation.json") {
                return Err(error(
                    "filesystem inventory points at a foreign original job",
                ));
            }
            return Ok((invocation, root));
        }
        let invocation = self.start_filesystem_invocation(&reference.unit)?;
        let root = self.filesystem_job_root(&invocation)?;
        reference.job = Some(inventory::JobReference {
            checkpoint: root
                .join("invocation.json")
                .strip_prefix(&self.authority.data_root)
                .map_err(error)?
                .to_owned(),
            identity: invocation.transition_id.clone(),
        });
        self.work.inventory.record(reference)?;
        Ok((invocation, root))
    }
    pub(super) fn advance_filesystem(
        &self,
        reference: &mut inventory::UnitReference,
    ) -> Result<sinomo_dfs::DfsStoragePreparation> {
        let pending = || sinomo_dfs::DfsStoragePreparation::Pending {
            processed_records: 0,
        };
        let (invocation, root) = self.filesystem_invocation_for(reference)?;
        let local = self.authority.local_node()?;
        if let progress @ sinomo_dfs::DfsStoragePreparation::Pending { .. } =
            self.capture_filesystem_sources(&invocation, &root)? {
            return Ok(progress);
        }
        let inbox = root.join("enroll.json");
        let mut fresh_enrollment = None;
        let mut qualified_sources = None;
        if !inbox.is_file() && invocation.coordinator == local {
            let mut wires = self.filesystem_sources(&root)?;
            let peers = invocation
                .catalog
                .iter()
                .map(|shard| shard.primary_node_id.clone())
                .collect::<BTreeSet<_>>();
            for peer in peers.iter().filter(|peer| **peer != local) {
                match self.filesystem_peer(
                    peer,
                    Request::Capture {
                        invocation: invocation.clone(),
                    },
                )? {
                    Response::Captured { sources } => wires.extend(sources),
                    Response::Pending => return Ok(pending()),
                    _ => {
                        return Err(error(
                            "peer enrolled before original complete source set was retained",
                        ));
                    }
                }
            }
            let sources = source_set(&wires)?;
            let Some(config) = self.pending_filesystem(&invocation, &root, &sources)? else {
                return Ok(pending());
            };
            let enrollment = Request::Enroll {
                invocation: invocation.clone(), pending: config, sources: wires,
            };
            write_immutable(&inbox, &enrollment)?;
            fresh_enrollment = Some(enrollment);
            qualified_sources = Some(sources);
        }
        if !inbox.is_file() {
            return Ok(pending());
        }
        let cohort_path = root.join("cohort.json");
        let peers = if invocation.coordinator == local {
            invocation.catalog.iter().map(|shard| shard.primary_node_id.clone())
                .filter(|peer| *peer != local).collect::<BTreeSet<_>>()
        } else { BTreeSet::new() };
        // Once enrollment has a qualified immutable seal, its original source
        // and native install owners recheck publication. Re-decoding every
        // source row on each publication turn would repeat the completed census.
        // Keep the full inbox only while staging or sending original peer DATA.
        let enrollment = if !cohort_path.is_file() || !peers.is_empty() {
            let enrollment = match fresh_enrollment.take() {
                Some(enrollment) => enrollment,
                None => read_bounded::<Request>(&inbox)?,
            };
            let Request::Enroll { invocation: enrolled, pending, sources } = enrollment
            else { return Err(error("filesystem enrollment inbox is not its original enrollment")); };
            if enrolled != invocation { return Err(error("filesystem enrollment substitutes original invocation")); }
            Some((pending, sources))
        } else { None };
        let owned = invocation
            .catalog
            .iter()
            .filter(|shard| shard.primary_node_id == local)
            .collect::<Vec<_>>();
        let seal = if cohort_path.is_file() {
            let retained: NamespaceCountCohortSeal = read_bounded(&cohort_path)?;
            retained.validate()?;
            if retained.transition_id != invocation.transition_id || retained.catalog != invocation.catalog {
                return Err(error("filesystem continuation changed its retained qualified cohort"));
            }
            for member in &retained.members {
                if member.original_source.retained_placement_reservations
                    != invocation.placement_evidence(&member.original_source.physical_shard)? {
                    return Err(error("retained cohort changed its original placement evidence"));
                }
            }
            // Enrollment owns preinstallation construction only. Existing
            // native source-cut/install recovery below resumes its same plan,
            // including after an earlier member exchanged its physical root.
            retained
        } else {
            let (config, wires) = enrollment.as_ref().expect("new enrollment retains its original DATA");
            // Reuse only this turn's freshly verified DATA. Restarts and peer
            // enrollment still decode the exact immutable inbox below.
            let sources = match qualified_sources.take() {
                Some(sources) => sources,
                None => source_set(wires)?,
            };
            invocation.validate_placement_sources(&sources)?;
            let owned_roots = owned.iter().map(|shard| {
                let physical = DfsMetadataShardId::new(&shard.shard_id);
                crate::metadata::namespace_count_plan_root(&self.metadata_root.join(&shard.shard_id), &physical)
                    .map(|root| (physical, root))
            }).collect::<Result<BTreeMap<_, _>>>()?;
            // Restore every local source fence before a shared pure staging
            // pass can inspect or construct any of these members.
            for shard in &owned {
                let physical = DfsMetadataShardId::new(&shard.shard_id);
                let destination = self.metadata_root.join(&shard.shard_id);
                let (progress, source) = self.metadata.resume_captured_metadata_unit(
                    &destination, &physical, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
                )?;
                if source.is_none() { return Ok(progress); }
            }
            let mut seal = None;
            for shard in &owned {
                let physical = DfsMetadataShardId::new(&shard.shard_id);
                let destination = self.metadata_root.join(&shard.shard_id);
                #[cfg(test)]
                observe_scope_step(self, shard, "before-enrollment")?;
                let actual = self.metadata.enroll_namespace_count_source(
                    &destination,
                    &physical,
                    &config,
                    &sources,
                    &invocation.catalog,
                    invocation.at,
                    NamespaceCountMaintenanceLimits {
                        max_objects: MAX_SOURCE_RECORDS,
                        max_encoded_bytes: MAX_WIRE_BYTES,
                        deadline: std::time::Instant::now()
                            + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                    },
                    &owned_roots,
                    seal.as_ref(),
                )?;
                if seal
                    .as_ref()
                    .is_some_and(|prior: &NamespaceCountCohortSeal| prior != &actual)
                {
                    return Err(error(
                        "local sources derived different complete filesystem seals",
                    ));
                }
                seal = Some(actual);
                #[cfg(test)]
                observe_scope_step(self, shard, "enrolled")?;
            }
            let seal =
                seal.ok_or_else(|| error("filesystem continuation lost all actual local sources"))?;
            write_immutable(&cohort_path, &seal)?;
            seal
        };
        for peer in &peers {
            let (config, wires) = enrollment.as_ref().expect("peer enrollment retains its original DATA");
            match self.filesystem_peer(
                peer,
                Request::Enroll {
                    invocation: invocation.clone(),
                    pending: config.clone(),
                    sources: wires.clone(),
                },
            )? {
                Response::Enrolled { cohort_id } if cohort_id != seal.cohort_id => {
                    return Err(error("remote owner derived a different complete cohort"));
                }
                _ => {}
            }
        }
        let mut processed_records = 0usize;
        for shard in &owned {
            #[cfg(test)]
            match observe_scope_step(self, shard, "before") {
                Ok(()) => {}
                Err(MetadataBackendError::StoragePreparationPending { .. }) => continue,
                Err(failure) => return Err(failure),
            }
            let destination = self.metadata_root.join(&shard.shard_id);
            match self.metadata.prepare_with_source(
                &destination,
                &destination,
                &DfsMetadataShardId::new(&shard.shard_id),
                sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
            ) {
                Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records: count }) => {
                    processed_records = processed_records.saturating_add(count);
                }
                Ok(sinomo_dfs::DfsStoragePreparation::Ready)
                | Err(MetadataBackendError::StoragePreparationPending { .. }) => {}
                Err(failure) => return Err(failure),
            }
            #[cfg(test)]
            observe_scope_step(self, shard, "after")?;
        }
        if invocation.coordinator == local && !self.authority.release_count_cohort(&seal, &root)? {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records });
        }
        if !self.authority.published_count_cohort(&seal)? {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending { processed_records });
        }
        let mut complete = true;
        for shard in owned {
            let destination = self.metadata_root.join(&shard.shard_id);
            if crate::metadata::installed_metadata_source_cut(
                &destination, &DfsMetadataShardId::new(&shard.shard_id),
            )?.is_none() {
                // Native cohort publication can precede completion of a local
                // exchange. The original installation owner must finish first.
                complete = false;
                continue;
            }
            let member = seal.member(&DfsMetadataShardId::new(&shard.shard_id))?;
            block_native(
                &self.authority.runtime,
                sinomo_dfs::activate_metadata_storage_generation(
                    &self.authority.component_context,
                    &destination,
                    &member.initialized_generation,
                    &member.initialized_seal,
                ),
            )?;
            match self.metadata.prepare_with_source(
                &destination,
                &destination,
                &DfsMetadataShardId::new(&shard.shard_id),
                sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
            )? {
                sinomo_dfs::DfsStoragePreparation::Ready => {}
                sinomo_dfs::DfsStoragePreparation::Pending { processed_records: count } => {
                    complete = false;
                    processed_records = processed_records.saturating_add(count);
                }
            }
        }
        if complete {
            if !invocation.placement_goals.is_empty() { self.schedule_placement_work()?; }
            for shard in invocation
                .catalog
                .iter()
                .filter(|shard| shard.primary_node_id == local)
            {
                self.record_completed_metadata_archives(&self.metadata_root.join(&shard.shard_id))?;
            }
        }
        Ok(if complete {
            sinomo_dfs::DfsStoragePreparation::Ready
        } else {
            sinomo_dfs::DfsStoragePreparation::Pending { processed_records }
        })
    }
}

impl MigrationOwner {
    pub(super) fn queue_filesystem_for_placement(&self, source: &placement_work::OriginalPlacementSource) -> Result<()> {
        let assignment = &source.assignment;
        let (entry, catalog, _) = self.filesystem_catalog(&assignment.file_system_id)?;
        let config: DfsFileSystemConfig = entry.decode().map_err(error)?;
        if matches!(&config.metadata_storage, Some(DfsMetadataStorageState::Active { target_format, .. })
            if *target_format >= u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)) {
            if let Some((plan, _, _, _)) = self.authority.recover_scope_retirement(
                &assignment.file_system_id, &DfsMetadataShardId::new(&assignment.shard_id),
            )? {
                let retained = crate::metadata::data_reservation_refs(&plan.source_cut)?;
                let expected = source.capsules.iter()
                    .map(placement_job_source::OriginalPlacementJob::verify_capsule).collect::<Result<Vec<_>>>()?;
                if !expected.is_empty() && expected.iter().all(|reference| retained.contains(reference)) {
                    // The existing received/filesystem unit still owns local
                    // activation. Never start another transition after release.
                    return Ok(());
                }
            }
            return Err(error("active filesystem does not bind this original placement reservation; late evidence cannot replace its captured cut"));
        }
        let local = self.authority.local_node()?;
        if assignment.primary_node_id != local { return Err(error("placement source requires its actual original primary")); }
        let coordinator = catalog.iter().map(|shard| &shard.primary_node_id).min()
            .ok_or_else(|| error("placement source has no complete catalog"))?;
        if coordinator == &local { self.enqueue_filesystem(&config)?; }
        else {
            self.filesystem_peer(coordinator, Request::Wake {
                file_system_id: assignment.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(&assignment.shard_id),
            })?;
        }
        Ok(())
    }
    pub(super) fn queue_filesystem_for_shard(
        &self,
        shard: &DfsMetadataShardId,
    ) -> Result<sinomo_dfs::DfsStoragePreparation> {
        let physical_root = self.metadata_root.join(&shard.0);
        if let Ok(header) = sinomo_dfs::inspect_metadata_storage_generation(&physical_root) {
            if matches!(
                header.identity.origin,
                sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesis
                    | sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesisInGeneration { .. }
            ) {
                return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                    processed_records: 0,
                });
            }
            if let sinomo_dfs::DfsMetadataGenerationState::Staged { seal: Some(seal) } =
                header.state
            {
                if crate::metadata::installed_metadata_source_cut(&physical_root, shard)?.is_none() {
                    return self.metadata.prepare_with_source(&physical_root, &physical_root,
                        shard, sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET);
                }
                let (entries, _) =
                    self.authority
                        .read_exact(&[sinomo_dfs::file_system_config_key(
                            &header.identity.file_system_id,
                        )])?;
                let config: DfsFileSystemConfig = entries[0]
                    .entry
                    .as_ref()
                    .ok_or_else(|| error("installed staged generation has no actual filesystem"))?
                    .decode()
                    .map_err(error)?;
                if matches!(
                    config.metadata_storage,
                    Some(DfsMetadataStorageState::Active { .. })
                ) {
                    block_native(
                        &self.authority.runtime,
                        sinomo_dfs::activate_metadata_storage_generation(
                            &self.authority.component_context,
                            &physical_root,
                            &header.identity,
                            &seal,
                        ),
                    )?;
                    return Ok(sinomo_dfs::DfsStoragePreparation::Ready);
                }
            }
        }
        if !crate::metadata::original_source_present(&physical_root)? {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        let prefix = "/dfs/filesystems/";
        let read = self.authority.read_native(
            self.authority
                .cluster()?
                .kv_range_prefix_complete_authoritative_async(prefix.into()),
        )?;
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(
            prefix, &read,
        )
        .map_err(error)?;
        let mut matches = Vec::new();
        for entry in read.value {
            if entry
                .key
                .rsplit_once("/metadata-shards/")
                .is_some_and(|(_, tail)| tail == shard.0)
            {
                let config: DfsMetadataShardConfig = entry.decode().map_err(error)?;
                if config.shard_id != shard.0
                    || entry.key
                        != sinomo_dfs::metadata_shard_key(&config.file_system_id, &config.shard_id)
                {
                    return Err(error("physical source catalog key/body changed scope"));
                }
                matches.push(config);
            }
        }
        let [assignment] = matches.as_slice() else {
            return Err(error(
                "legacy physical source has no unique current filesystem assignment",
            ));
        };
        if let Some(received) = self.metadata.prepare_received_metadata_unit(
            &physical_root,
            shard,
            &assignment.file_system_id,
            sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
        )? {
            return Ok(received);
        }
        let (entry, catalog, _) = self.filesystem_catalog(&assignment.file_system_id)?;
        let config: DfsFileSystemConfig = entry.decode().map_err(error)?;
        // An old replica under an already-active generation needs native image
        // repair, never a new migration of an unqualified old local copy.
        if matches!(
            config.metadata_storage,
            Some(DfsMetadataStorageState::Active { target_format, .. }) if target_format >= u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        ) {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        let local = self.authority.local_node()?;
        if assignment.primary_node_id != local {
            return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        let coordinator = catalog
            .iter()
            .map(|shard| &shard.primary_node_id)
            .min()
            .ok_or_else(|| error("empty source catalog"))?;
        if coordinator == &local {
            self.enqueue_filesystem(&config)?;
        } else {
            self.filesystem_peer(
                coordinator,
                Request::Wake {
                    file_system_id: assignment.file_system_id.clone(),
                    shard_id: shard.clone(),
                },
            )?;
        }
        Ok(sinomo_dfs::DfsStoragePreparation::Pending {
            processed_records: 0,
        })
    }
}

#[cfg(test)]
type ScopeHook =
    Arc<dyn Fn(&MigrationOwner, &DfsMetadataShardConfig, &str) -> Result<()> + Send + Sync>;
#[cfg(test)]
static SCOPE_HOOKS: OnceLock<Mutex<BTreeMap<PathBuf, ScopeHook>>> = OnceLock::new();
#[cfg(test)]
pub(super) struct ScopeHookGuard(PathBuf);
#[cfg(test)]
impl Drop for ScopeHookGuard {
    fn drop(&mut self) {
        if let Some(hooks) = SCOPE_HOOKS.get() {
            hooks.lock().unwrap().remove(&self.0);
        }
    }
}
#[cfg(test)]
pub(super) fn install_scope_hook(root: &Path, hook: ScopeHook) -> ScopeHookGuard {
    SCOPE_HOOKS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(root.to_owned(), hook);
    ScopeHookGuard(root.to_owned())
}
#[cfg(test)]
fn observe_scope_step(
    owner: &MigrationOwner,
    shard: &DfsMetadataShardConfig,
    point: &str,
) -> Result<()> {
    let hook = SCOPE_HOOKS.get().and_then(|hooks| {
        hooks
            .lock()
            .unwrap()
            .get(&owner.authority.data_root)
            .cloned()
    });
    if let Some(hook) = hook {
        hook(owner, shard, point)?;
    }
    Ok(())
}

/// Observe only after the real all-member stage/fence qualification succeeds.
/// Reuse the existing test hook; this supplies no success or native authority.
#[cfg(test)]
pub(in crate::authority) fn observe_cohort_qualified(
    authority: &NativeImportAuthority,
    plan: &MetadataImportPlan,
) -> Result<()> {
    observe_plan_step(authority, plan, "cohort-qualified")
}
#[cfg(test)]
pub(in crate::authority) fn observe_cohort_activation_wait(
    authority: &NativeImportAuthority,
    plan: &MetadataImportPlan,
) -> Result<()> {
    observe_plan_step(authority, plan, "activation-waits-predecessor")
}
#[cfg(test)]
fn observe_plan_step(authority: &NativeImportAuthority, plan: &MetadataImportPlan, point: &str) -> Result<()> {
    let has_hook = SCOPE_HOOKS
        .get()
        .is_some_and(|hooks| hooks.lock().unwrap().contains_key(&authority.data_root));
    if !has_hook {
        return Ok(());
    }
    let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
        authority.component_context.data_dir(),
        authority.component_context.host_role(),
    );
    let owner = OWNERS
        .get()
        .and_then(|owners| {
            owners
                .lock()
                .ok()
                .and_then(|owners| owners.get(&paths.root).and_then(Weak::upgrade))
        })
        .ok_or_else(|| error("registered cohort observer has no actual mounted owner"))?;
    {
        observe_scope_step(
            &owner,
            &plan.expected_handoff.retiring_shard,
            point,
        )?;
    }
    Ok(())
}

// The native wire carries timestamps as RFC3339, like the cohort seal. Earlier
// captured invocation DATA used time's heterogeneous tuple; accept that original
// representation only on read, preserving the actual timestamp and request hash.
mod invocation_time {
    use serde::{Deserialize, Serialize};
    pub fn serialize<S: serde::Serializer>(
        value: &time::OffsetDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        time::serde::rfc3339::serialize(value, serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<time::OffsetDateTime, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(value) => {
                time::OffsetDateTime::parse(&value, &time::format_description::well_known::Rfc3339)
                    .map_err(serde::de::Error::custom)
            }
            original => serde_json::from_value(original).map_err(serde::de::Error::custom),
        }
    }
}
fn write_replay_compatible<T: Serialize + serde::de::DeserializeOwned + PartialEq>(
    path: &Path,
    value: &T,
) -> Result<()> {
    if path.try_exists().map_err(error)? {
        if &read_bounded::<T>(path)? != value {
            return Err(error("immutable filesystem invocation/enrollment changed"));
        }
        return Ok(());
    }
    write_immutable(path, value)
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Timestamp {
        #[serde(with = "invocation_time")]
        at: time::OffsetDateTime,
    }
    #[derive(Serialize)]
    struct OriginalTimestamp {
        at: time::OffsetDateTime,
    }
    #[test]
    fn filesystem_timestamp_wire_preserves_original_checkpoint_bytes() {
        let value = Timestamp {
            at: time::OffsetDateTime::from_unix_timestamp(1_794_656_070)
                .unwrap()
                .replace_nanosecond(123_456_789)
                .unwrap(),
        };
        let encoded = sinomo_core::object::serde_ext::to_bytes(
            &value,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let decoded: Timestamp = sinomo_core::object::serde_ext::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, value);
        let root = sinomo_testdir::TestDir::new("filesystem-original-timestamp");
        let path = root.join("invocation.json");
        let original = serde_json::to_vec(&OriginalTimestamp { at: value.at }).unwrap();
        std::fs::write(&path, &original).unwrap();
        write_replay_compatible(&path, &value).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(read_bounded::<Timestamp>(&path).unwrap(), value);
    }
}

/// Retain the already-qualified receiver plan before acknowledging its blueprint.
/// This only schedules the existing owner; no metadata installation runs here.
pub(in crate::authority) fn enqueue_received_generation(
    context: &sinomo_module::ComponentContext,
    plan: &MetadataImportPlan,
    receiver_root: &Path,
) -> Result<()> {
    let Some(cohort) = plan.source_cut.namespace_count_cohort.as_deref() else {
        return Ok(());
    };
    cohort.validate_bound_cut(&plan.source_cut)?;
    let member = cohort.member(&plan.source_cut.scope_shard)?;
    let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
        context.data_dir(),
        context.host_role(),
    );
    let owner = OWNERS
        .get()
        .ok_or_else(|| error("receiver has no migration owner"))?
        .lock()
        .map_err(error)?
        .get(&paths.root)
        .and_then(Weak::upgrade)
        .ok_or_else(|| error("receiver migration owner is unavailable"))?;
    let scope = DfsStoragePreparationScope::MetadataGeneration {
        identity: member.initialized_generation.clone(),
    };
    let destination = owner
        .authority
        .data_root
        .join("dfs/metadata")
        .join(&member.original_source.physical_shard.0);
    let unit = inventory::UnitKey::from_scope(&owner.authority.data_root, &destination, &scope)?;
    let mut reference = owner
        .work
        .inventory
        .read(&unit)?
        .unwrap_or_else(|| inventory::UnitReference::discovered(unit.clone()));
    let job = inventory::JobReference {
        checkpoint: receiver_root
            .join("native-import-plan.json")
            .strip_prefix(&owner.authority.data_root)
            .map_err(error)?
            .to_owned(),
        identity: plan.expected_handoff.handoff_id.clone(),
    };
    if reference
        .job
        .as_ref()
        .is_some_and(|original| original != &job)
    {
        return Err(error(
            "received generation changed its original native plan",
        ));
    }
    reference.job = Some(job);
    owner.work.inventory.record(&reference)?;
    let mut scheduler = owner.work.scheduler.lock().map_err(error)?;
    scheduler.recovered(reference)?;
    scheduler.enqueue(unit, false)?;
    drop(scheduler);
    owner.work.wake.notify_one();
    Ok(())
}

impl MigrationOwner {
    pub(super) fn advance_received_generation(
        &self,
        root: &Path,
        identity: &sinomo_dfs::DfsMetadataGenerationIdentity,
        reference: &inventory::UnitReference,
    ) -> Result<sinomo_dfs::DfsStoragePreparation> {
        let pending = || sinomo_dfs::DfsStoragePreparation::Pending {
            processed_records: 0,
        };
        let Some(job) = &reference.job else {
            return self.queue_filesystem_for_shard(&identity.shard_id);
        };
        let path = self.authority.data_root.join(&job.checkpoint);
        let relocated = if path.try_exists().map_err(error)? {
            None
        } else {
            Some(
                crate::metadata::installed_metadata_source_cut(root, &identity.shard_id)?
                    .ok_or_else(|| {
                        error("missing receiver plan has no verified installed original source cut")
                    })?,
            )
        };
        let actual_path = if relocated.is_some() {
            root.join("native-import-plan.json")
        } else {
            path
        };
        let plan = self.authority.retained_plan_file.read(&actual_path)?;
        if let Some(installed) = &relocated {
            if !plan_file::same_source_cut(installed, &plan.source_cut)
                || self.authority.check_handoff_state(&plan)?.is_none()
            {
                return Err(error(
                    "relocated receiver plan has no exact original native activation",
                ));
            }
        }
        let Some(cohort) = plan.source_cut.namespace_count_cohort.as_deref() else {
            return Err(error("received generation omitted its original cohort"));
        };
        if plan.expected_handoff.handoff_id != job.identity
            || &cohort.member(&identity.shard_id)?.initialized_generation != identity
        {
            return Err(error(
                "received generation job differs from its immutable plan",
            ));
        }
        let _ = self.metadata.prepare_received_metadata_unit(
            root,
            &identity.shard_id,
            &identity.file_system_id,
            sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
        )?;
        let Ok(header) = sinomo_dfs::inspect_metadata_storage_generation(root) else {
            return Ok(pending());
        };
        if header.identity != *identity {
            return Ok(pending());
        }
        if let sinomo_dfs::DfsMetadataGenerationState::Staged { seal: Some(seal) } = header.state {
            if crate::metadata::installed_metadata_source_cut(root, &identity.shard_id)?.is_none() {
                return Ok(pending());
            }
            if !self.authority.published_count_cohort(cohort)? {
                return Ok(pending());
            }
            block_native(
                &self.authority.runtime,
                sinomo_dfs::activate_metadata_storage_generation(
                    &self.authority.component_context,
                    root,
                    identity,
                    &seal,
                ),
            )?;
        }
        let _ = self.metadata.prepare_received_metadata_unit(
            root,
            &identity.shard_id,
            &identity.file_system_id,
            sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
        )?;
        inspect_current_unit(
            root,
            &DfsStoragePreparationScope::MetadataGeneration {
                identity: identity.clone(),
            },
        )
    }
}
