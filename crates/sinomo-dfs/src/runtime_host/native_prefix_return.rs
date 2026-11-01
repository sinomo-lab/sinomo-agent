//! Capture-only provenance for one actual native direct PREFIX return.
//! No durable intake, aggregation, initialization or serving authority lives here.
use super::*;
use crate::node_transport::{
    DfsMetadataPrefixEvidence, DfsMetadataPrefixProbeAttempt, DfsMetadataPrefixProbeReturn,
};
use crate::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
};
use sinomo_core::network::NetworkError;

#[derive(Clone)]
pub(super) struct NativePrefixOwnerContext {
    pub(super) data_dir: SinomoDataDir,
    pub(super) runtime: tokio::runtime::Handle,
}
impl NativePrefixOwnerContext {
    pub(super) fn new(data_dir: &SinomoDataDir, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            data_dir: data_dir.clone(),
            runtime: runtime.clone(),
        }
    }
}

/// Observed LOCAL identities only. Paths reported by a remote peer are absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeMetadataPrefixPhysicalOwner {
    pub data_root: PathBuf,
    pub data_identity: (u64, u64),
    pub owner_identity: (u64, u64),
    pub shard_root: PathBuf,
    pub shard_identity: (u64, u64),
}

/// Only `call_native_prefix_probe` below constructs this value. It has no
/// serde/Clone/raw constructor and retains NO owner/host/runtime/admission Arc.
#[derive(Debug)]
pub struct OwnedNativeMetadataPrefixReturn {
    attempt_id: String,
    command: DfsMetadataJournalBatchCommand,
    peer: NodeId,
    response: DfsMetadataJournalBatchResponse,
    assignment: crate::DfsMetadataShardConfig,
    prefix: crate::DfsMetadataPrefixProof,
    physical: NativeMetadataPrefixPhysicalOwner,
}

impl OwnedNativeMetadataPrefixReturn {
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
    pub fn command(&self) -> &DfsMetadataJournalBatchCommand {
        &self.command
    }
    pub fn peer(&self) -> &NodeId {
        &self.peer
    }
    pub fn response(&self) -> &DfsMetadataJournalBatchResponse {
        &self.response
    }
    pub fn source_assignment(&self) -> &crate::DfsMetadataShardConfig {
        &self.assignment
    }
    pub fn source_prefix(&self) -> &crate::DfsMetadataPrefixProof {
        &self.prefix
    }
    pub fn local_physical_owner(&self) -> &NativeMetadataPrefixPhysicalOwner {
        &self.physical
    }

    /// Historical call binding only, never current effect/serving permission.
    pub fn validate_binding(
        &self,
        expected_peer: &NodeId,
        expected_command: &DfsMetadataJournalBatchCommand,
        expected_local_root: &Path,
    ) -> Result<(), NetworkError> {
        if &self.peer != expected_peer
            || &self.command != expected_command
            || self.physical.data_root.as_path() != expected_local_root
        {
            return Err(NetworkError::Serialization {
                detail:
                    "native prefix return belongs to another peer, attempt or local physical root"
                        .into(),
            });
        }
        let mismatch = |error: String| NetworkError::Serialization {
            detail: format!("native prefix historical physical binding is unavailable: {error}"),
        };
        let owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(&SinomoDataDir::new(
            expected_local_root,
        )))
        .map_err(|error| mismatch(error.to_string()))?;
        if owner.physical_data_root() != expected_local_root
            || owner
                .storage_owner_file_identity()
                .map_err(|error| mismatch(error.to_string()))?
                != self.physical.owner_identity
            || directory_identity(expected_local_root)
                .map_err(|error| mismatch(error.to_string()))?
                != self.physical.data_identity
            || directory_identity(&self.physical.shard_root)
                .map_err(|error| mismatch(error.to_string()))?
                != self.physical.shard_identity
        {
            return Err(mismatch(
                "actual local physical owner differs from this captured call".into(),
            ));
        }
        let named_path = expected_local_root.join("daemon/data-owner.lock");
        let named =
            std::fs::symlink_metadata(&named_path).map_err(|error| mismatch(error.to_string()))?;
        if !named.is_file()
            || named.file_type().is_symlink()
            || storage_file_identity(
                &std::fs::File::open(&named_path).map_err(|error| mismatch(error.to_string()))?,
            )
            .map_err(|error| mismatch(error.to_string()))?
                != self.physical.owner_identity
        {
            return Err(mismatch(
                "actual named DATA owner file differs from this captured call".into(),
            ));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn validate_attempt_binding(
        &self,
        attempt: &DfsMetadataPrefixProbeAttempt,
        response: &DfsMetadataJournalBatchResponse,
    ) -> Result<(), NetworkError> {
        if self.attempt_id != attempt.attempt_id()
            || self.peer != *attempt.peer()
            || self.command != *attempt.command()
            || self.response != *response
        {
            return Err(NetworkError::Serialization { detail:"native prefix evidence cannot be reused for another producer attempt or response".into() });
        }
        Ok(())
    }
}

fn invalid(detail: impl std::fmt::Display) -> DfsPeerTransportError {
    DfsPeerTransportError::new(format!("native metadata prefix return: {detail}"))
}
fn after_dispatch(detail: impl std::fmt::Display) -> DfsPeerTransportError {
    DfsPeerTransportError::from_network(NetworkError::delivery_failed(
        "capture native metadata prefix return",
        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        detail.to_string(),
    ))
}
fn remaining(attempt: &DfsMetadataPrefixProbeAttempt) -> Result<Duration, DfsPeerTransportError> {
    attempt
        .deadline()
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| invalid("original attempt deadline expired"))
}
fn directory_identity(path: &Path) -> Result<(u64, u64), DfsPeerTransportError> {
    let named = std::fs::symlink_metadata(path).map_err(invalid)?;
    if !named.is_dir() || named.file_type().is_symlink() {
        return Err(invalid("source root is not an actual physical directory"));
    }
    let file = std::fs::File::open(path).map_err(invalid)?;
    let opened = storage_file_identity(&file).map_err(invalid)?;
    let after = std::fs::symlink_metadata(path).map_err(invalid)?;
    if !after.is_dir() || after.file_type().is_symlink() {
        return Err(invalid(
            "source physical directory type changed during capture",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (named.dev(), named.ino()) != opened || (after.dev(), after.ino()) != opened {
            return Err(invalid("source physical directory changed during capture"));
        }
    }
    #[cfg(not(unix))]
    return Err(invalid(
        "native prefix directory identity is unsupported on this platform",
    ));
    Ok(opened)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceData {
    assignment: crate::DfsMetadataShardConfig,
    prefix: crate::DfsMetadataPrefixProof,
    physical: NativeMetadataPrefixPhysicalOwner,
    authority_generation: u64,
}

fn capture_source(
    context: &NativePrefixOwnerContext,
    host: &Arc<DfsRuntimeHost>,
    owner: &Arc<DfsDataOwnerLock>,
    attempt: &DfsMetadataPrefixProbeAttempt,
) -> Result<(SourceData, Arc<crate::DfsMetadataAuthorityRuntime>), DfsPeerTransportError> {
    remaining(attempt)?;
    let registered = registered_dfs_runtime_host(&context.data_dir)
        .map_err(invalid)?
        .ok_or_else(|| invalid("actual registered host is unavailable"))?;
    let current_owner =
        require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(&context.data_dir))
            .map_err(invalid)?;
    if !Arc::ptr_eq(host, &registered)
        || !Arc::ptr_eq(owner, &current_owner)
        || host.shutting_down.is_cancelled()
    {
        return Err(invalid("actual host or DATA owner changed"));
    }
    let command = attempt.command();
    crate::node_transport::validate_ordinary_metadata_prefix_command(command)?;
    let probe = command
        .prefix_probe
        .as_ref()
        .expect("validated prefix probe");
    let paths = ClusterPaths::new(&context.data_dir);
    let identity = sinomo_core::cluster::read_cluster_state_without_kv(&paths).map_err(invalid)?;
    if command.source_node_id.as_ref() != Some(&identity.local_node_id)
        || attempt.peer() == &identity.local_node_id
    {
        return Err(invalid(
            "request source/target differs from the actual native source",
        ));
    }
    // Borrow only the already published native owner generation. Creating or
    // warming an authority observer here would be a forbidden proof fallback.
    // This required call uses normal short cache ownership, not a status peek.
    // Recheck the original deadline after waiting; never dispatch an old attempt.
    let slot = message_host::daemon_dfs_message_service_cache()
        .existing_root(&context.data_dir.root())
        .map_err(invalid)?
        .ok_or_else(|| invalid("no existing native source authority cache"))?;
    let (authority, generation) = {
        let cache = slot
            .lock()
            .map_err(|_| invalid("native source authority cache is poisoned"))?;
        let entry = cache
            .as_ref()
            .ok_or_else(|| invalid("native source authority generation is unavailable"))?;
        if entry.signature.local_node_id != identity.local_node_id
            || entry.signature.cluster_id != identity.cluster_id
        {
            return Err(invalid(
                "retained authority cache belongs to another native identity",
            ));
        }
        (
            Arc::clone(&entry.metadata_authority_generation),
            entry.signature.metadata_authority_generation,
        )
    };
    remaining(attempt)?;
    message_host::daemon_dfs_require_retained_authority(&authority, generation)
        .map_err(|e| invalid(e.into_dfs_error()))?;
    let projection = authority
        .retained_complete_prefix_for_generation(generation)
        .ok_or_else(|| invalid("no complete retained authority prefix"))?;
    let replica = crate::metadata_replica_context_from_authority_projection(
        &projection,
        &identity.local_node_id,
        command.file_system_id.as_ref().expect("validated fs"),
        &command.batch.shard_id,
    )
    .map_err(invalid)?
    .ok_or_else(|| invalid("no current assigned native source context"))?;
    if replica.pending_membership_handoff.is_some()
        || replica.shard.primary_node_id != identity.local_node_id
        || Some(replica.shard.epoch) != command.authority_epoch
        || !replica.shard.replica_node_ids.contains(attempt.peer())
    {
        return Err(invalid(
            "ordinary prefix source/epoch/peer assignment is unavailable or changed",
        ));
    }
    let physical_data = SinomoDataDir::new(owner.physical_data_root());
    let root = crate::local_dfs_metadata_shard_path(&physical_data, &command.batch.shard_id);
    if !owner.owns_backend_root(&root) {
        return Err(invalid("prefix source shard belongs to another DATA owner"));
    }
    let layout = owner.selected_original_data_layout().map_err(invalid)?;
    if let Some(layout) = &layout {
        layout.check_owner(owner).map_err(invalid)?;
        layout
            .stage(
                &replica.shard.file_system_id,
                &command.batch.shard_id,
                &root,
            )
            .map_err(invalid)?;
    }
    let physical = NativeMetadataPrefixPhysicalOwner {
        data_root: owner.physical_data_root().to_path_buf(),
        data_identity: directory_identity(owner.physical_data_root())?,
        owner_identity: owner.storage_owner_file_identity().map_err(invalid)?,
        shard_root: root.clone(),
        shard_identity: directory_identity(&root)?,
    };
    let owner_path = physical.data_root.join("daemon/data-owner.lock");
    let named_owner = std::fs::symlink_metadata(&owner_path).map_err(invalid)?;
    if !named_owner.is_file() || named_owner.file_type().is_symlink() {
        return Err(invalid("source owner is not its named physical file"));
    }
    let named_owner_file = std::fs::File::open(&owner_path).map_err(invalid)?;
    if storage_file_identity(&named_owner_file).map_err(invalid)? != physical.owner_identity {
        return Err(invalid(
            "source DATA owner pathname differs from its held native lock",
        ));
    }
    let _fence = crate::metadata_lock::try_metadata_inspection_lock(
        root.clone(),
        crate::metadata_cursor::journal_write_lock_path(&root),
    )
    .map_err(invalid)?
    .ok_or_else(|| invalid("actual prefix source is busy or unavailable"))?;
    if crate::metadata_api::read_handoff_freeze_with_layout(
        &root.join("journal/membership-handoff-write-freeze.sobj"),
        layout.as_deref(),
    )
    .map_err(invalid)?
    .is_some_and(|freeze| freeze.activated_epoch != command.authority_epoch)
    {
        return Err(invalid("ordinary source prefix is fenced"));
    }
    let prefix = crate::metadata_journal::admission::current_owned_materialized_prefix(
        &root,
        &command.batch.shard_id,
    )
    .map_err(invalid)?
    .ok_or_else(|| {
        invalid("source has no existing owned certified prefix; no recovery fallback")
    })?;
    if prefix.journal_sequence != probe.journal_sequence || prefix.digest != probe.state_fingerprint
    {
        return Err(invalid(
            "actual certified source prefix differs from the captured probe",
        ));
    }
    if directory_identity(&root)? != physical.shard_identity
        || directory_identity(owner.physical_data_root())? != physical.data_identity
    {
        return Err(invalid(
            "source physical identity changed across prefix capture",
        ));
    }
    message_host::daemon_dfs_require_retained_authority(&authority, generation)
        .map_err(|e| invalid(e.into_dfs_error()))?;
    remaining(attempt)?;
    Ok((
        SourceData {
            assignment: replica.shard,
            prefix,
            physical,
            authority_generation: generation,
        },
        authority,
    ))
}

fn dispatch_native_prefix_call(
    host: &DfsRuntimeHost,
    attempt: &DfsMetadataPrefixProbeAttempt,
    call: sinomo_core::network::ObjectRpcCall,
    options: DfsObjectRpcSendOptions,
) -> Result<sinomo_core::network::ObjectRpcResult, DfsPeerTransportError> {
    host.object_rpc
        .call(
            attempt.peer(),
            call,
            options.cluster_rpc_options(),
            options.observe_connectivity,
        )
        .map_err(DfsPeerTransportError::from_network)
}

pub(super) fn call_native_prefix_probe(
    context: &NativePrefixOwnerContext,
    host: &Arc<DfsRuntimeHost>,
    attempt: &DfsMetadataPrefixProbeAttempt,
    mut options: DfsObjectRpcSendOptions,
) -> Result<DfsMetadataPrefixProbeReturn, DfsPeerTransportError> {
    let owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(&context.data_dir))
        .map_err(invalid)?;
    let (before, before_authority) = capture_source(context, host, &owner, attempt)?;
    // The inspection fence and all registry/projection locks above are dropped.
    // Only the actual source owner/host and immutable attempt survive the call.
    let call = crate::dfs_metadata_journal_batch_rpc_call(attempt.command())
        .map_err(DfsPeerTransportError::from_network)?;
    let left = remaining(attempt)?;
    options.overall_timeout = Some(options.overall_timeout.map_or(left, |old| old.min(left)));
    let result = dispatch_native_prefix_call(host, attempt, call, options)?;
    let response = {
        let _runtime = context.runtime.enter();
        crate::node_transport::collect_metadata_prefix_probe_return(result, attempt)
            .map_err(after_dispatch)?
    };
    if !response.verified_prefix_only
        || response.applied_entries != 0
        || response.verified_authority_probe_id.is_some()
        || response.result != DfsMetadataJournalBatchResult::Applied
    {
        return Err(after_dispatch(format!(
            "actual return did not verify this ordinary PREFIX attempt: {response:?}"
        )));
    }
    let (after, after_authority) =
        capture_source(context, host, &owner, attempt).map_err(after_dispatch)?;
    if after != before || !Arc::ptr_eq(&before_authority, &after_authority) {
        return Err(after_dispatch(
            "source owner/assignment/prefix changed during the actual RPC",
        ));
    }
    remaining(attempt).map_err(after_dispatch)?;
    let evidence = OwnedNativeMetadataPrefixReturn {
        attempt_id: attempt.attempt_id().to_owned(),
        command: attempt.command().clone(),
        peer: attempt.peer().clone(),
        response: response.clone(),
        assignment: before.assignment,
        prefix: before.prefix,
        physical: before.physical,
    };
    // In-flight owner is released here. Historical evidence has no strong lease.
    drop(owner);
    Ok(DfsMetadataPrefixProbeReturn {
        response,
        evidence: DfsMetadataPrefixEvidence::Native(evidence),
    })
}
