//! One explicit native call. Its result is remote-reported local-fence DATA,
//! never a PromiseAck, chosen/custody certificate or serving permission.
use super::*;
use crate::control_plane::DfsRecoveryBallotVerify;
use crate::metadata_api::{DfsSelectedLocalRecoveryContext, DfsSelectedLocalRecoveryFence};
use crate::{
    DfsError, DfsMessageCommand, DfsNetworkMessage, DfsRoutedMessage,
    DfsSelectedLocalRecoveryFenceRpcData, DfsSelectedLocalRecoveryFenceRpcRequest,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// Carrier IDs are process-local correlation only. Neither these IDs nor time
// create a new logical operation: (full reservation, target) is unchanged.
static NEXT_CARRIER: AtomicU64 = AtomicU64::new(1_u64 << 62);
const MAX_RETURN_BYTES: usize = 2 * 1024 * 1024;

/// Private construction is confined to the successful actual native call
/// below. No serialization, Clone, conversion or raw-frame constructor exists.
pub(crate) struct OwnedSelectedLocalRecoveryFenceReturn {
    request: DfsSelectedLocalRecoveryFenceRpcRequest,
    command: DfsMessageCommand,
    remote_reported: DfsSelectedLocalRecoveryFenceRpcData,
    _owner: Arc<DfsDataOwnerLock>,
}

impl OwnedSelectedLocalRecoveryFenceReturn {
    pub(crate) fn request(&self) -> &DfsSelectedLocalRecoveryFenceRpcRequest {
        &self.request
    }
    pub(crate) fn actual_carrier(&self) -> &DfsRoutedMessage {
        &self.command.routed_message
    }
    pub(crate) fn outer_request_id(&self) -> &str {
        &self.command.request_id
    }
    pub(crate) fn remote_reported_fence(&self) -> &DfsSelectedLocalRecoveryFence {
        &self.remote_reported.fence
    }
    /// BLAKE3 of the remote's reported stored marker, not a local proof.
    pub(crate) fn remote_reported_marker_digest(&self) -> &str {
        &self.remote_reported.marker_digest
    }
    pub(crate) fn remote_reported_marker_bytes(&self) -> u64 {
        self.remote_reported.marker_bytes
    }
}

fn invalid(message: impl std::fmt::Display) -> DfsError {
    DfsError::Metadata {
        message: format!("native selected local-fence call: {message}"),
    }
}

fn uncertain(message: impl std::fmt::Display) -> DfsError {
    DfsError::NetworkDelivery {
        message: format!("native selected local-fence call: {message}"),
        delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
    }
}

fn remaining(deadline: Instant) -> Result<std::time::Duration, DfsError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|budget| !budget.is_zero())
        .ok_or_else(|| invalid("original deadline expired"))
}

fn verify_pending(
    control: &mut dyn crate::DfsControlPlaneKvClient,
    request: &DfsSelectedLocalRecoveryFenceRpcRequest,
    local: &NodeId,
    deadline: Instant,
) -> Result<(), DfsError> {
    remaining(deadline)?;
    match crate::DfsControlPlaneStore::new(control).verify_metadata_shard_recovery_ballot_before(
        &request.reservation,
        local,
        deadline,
    )? {
        DfsRecoveryBallotVerify::Verified(actual) if actual == request.reservation => Ok(()),
        other => Err(invalid(format!(
            "exact pending reservation refused: {other:?}"
        ))),
    }
}

/// Explicit, test-only native observation. There is no periodic caller or
/// durable time field, implicit retry, generic sender/inbox fallback or effect
/// consumer. The supplied absolute deadline covers local checks, RPC and decode.
pub(crate) fn call_native_selected_local_recovery_fence(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    request: &DfsSelectedLocalRecoveryFenceRpcRequest,
    deadline: Instant,
) -> Result<OwnedSelectedLocalRecoveryFenceReturn, DfsError> {
    if remaining(deadline)? > sinomo_core::network::DEFAULT_NETWORK_TIMEOUT {
        return Err(invalid("call budget exceeds five seconds"));
    }
    let canonical = crate::control_plane::DfsMetadataShardRecoveryBallotRequest::prepare(
        &request.reservation.serving_shard,
        &request.reservation.ballot.candidate_node_id,
        request.reservation.expected_attempt_floor,
    )
    .map_err(|error| invalid(format!("reservation: {error:?}")))?;
    if canonical != request.reservation {
        return Err(invalid("full reservation differs from canonical request"));
    }
    let paths = DfsRuntimePaths::new(data_dir);
    let owner = require_daemon_dfs_data_owner_lock(&paths).map_err(invalid)?;
    let host = registered_dfs_runtime_host(data_dir)
        .map_err(invalid)?
        .ok_or_else(|| invalid("actual registered native host missing"))?;
    if host.shutting_down.is_cancelled() {
        return Err(invalid("native host is shutting down"));
    }
    let layout = owner
        .selected_original_data_layout()
        .map_err(invalid)?
        .ok_or_else(|| invalid("default DATA owner has no selected layout"))?;
    let context =
        DfsSelectedLocalRecoveryContext::from_owner(Arc::clone(&layout), Arc::clone(&owner))?;
    let local = context.local_node_id().clone();
    let members = &request.reservation.serving_shard;
    if local != request.reservation.ballot.candidate_node_id
        || request.expected_target == local
        || (request.expected_target != members.primary_node_id
            && !members.replica_node_ids.contains(&request.expected_target))
    {
        return Err(invalid("candidate or assigned remote target differs"));
    }
    let mut control =
        daemon_dfs_control_plane_client_on_runtime(&ClusterPaths::new(data_dir), Some(runtime))
            .ok_or_else(|| invalid("actual authoritative control client unavailable"))?;
    verify_pending(control.as_mut(), request, &local, deadline)?;
    context.check()?;
    let frame = NEXT_CARRIER
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .map_err(|_| invalid("carrier identity exhausted"))?;
    let command = DfsMessageCommand {
        request_id: format!("dfs-selected-local-fence-{}", sinomo_core::Id::generate()),
        routed_message: DfsRoutedMessage {
            source_node_id: local,
            target_node_id: request.expected_target.clone(),
            frame_id: frame,
            message_id: frame,
            message: DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request.clone()),
        },
    };
    let call = crate::dfs_message_rpc_call(&command).map_err(DfsError::from_network)?;
    let expected_route = call.request.route.clone();
    let before_host = registered_dfs_runtime_host(data_dir)
        .map_err(invalid)?
        .ok_or_else(|| invalid("actual native host disappeared before dispatch"))?;
    let before_owner = require_daemon_dfs_data_owner_lock(&paths).map_err(invalid)?;
    if !Arc::ptr_eq(&host, &before_host)
        || !Arc::ptr_eq(&owner, &before_owner)
        || before_host.shutting_down.is_cancelled()
    {
        return Err(invalid("native host or DATA owner changed before dispatch"));
    }
    context.check()?;
    // Only host/physical owner and immutable DATA survive across this RPC. All
    // registry/control-client locks above have been released; no namespace
    // admission, metadata fence, queue writer or publication guard is held.
    let peer = data_plane::DaemonDfsPeerTransport {
        host: Arc::clone(&host),
        native_prefix: Some(super::native_prefix_return::NativePrefixOwnerContext {data_dir:data_dir.clone(),runtime:runtime.clone()}),
    };
    let result = peer
        .host
        .object_rpc
        .call(
            &request.expected_target,
            call,
            crate::DfsObjectRpcSendOptions::default()
                .with_overall_timeout(remaining(deadline)?)
                .cluster_rpc_options(),
            true,
        )
        .map_err(|error| uncertain(error.to_string()))?;
    remaining(deadline).map_err(uncertain)?;
    let decode = async {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            result.into_materialized_value_response(MAX_RETURN_BYTES),
        )
        .await
        .map_err(|_| uncertain("original deadline expired during native decode"))?
        .map_err(|error| uncertain(error.to_string()))
    };
    let response = match tokio::runtime::Handle::try_current() {
        Ok(current) if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| runtime.block_on(decode))
        }
        Ok(_) => {
            return Err(uncertain(
                "current-thread runtime cannot block for native decode",
            ));
        }
        Err(_) => runtime.block_on(decode),
    }?;
    if response.route != expected_route || response.body.len() > MAX_RETURN_BYTES {
        return Err(uncertain("native reply route or bounded value differs"));
    }
    let decoded = crate::decode_dfs_message_rpc_response(&response)
        .map_err(|error| uncertain(error.to_string()))?;
    let remote_reported =
        crate::node_transport::validate_selected_local_recovery_fence_rpc_return(&command, decoded)
            .map_err(|error| uncertain(error.to_string()))?;
    let current_owner = require_daemon_dfs_data_owner_lock(&paths).map_err(uncertain)?;
    let current_host = registered_dfs_runtime_host(data_dir)
        .map_err(uncertain)?
        .ok_or_else(|| uncertain("registered native host disappeared"))?;
    if !Arc::ptr_eq(&owner, &current_owner)
        || !Arc::ptr_eq(&host, &current_host)
        || current_host.shutting_down.is_cancelled()
    {
        return Err(uncertain("native host or DATA owner changed during call"));
    }
    let current_layout = current_owner
        .selected_original_data_layout()
        .map_err(uncertain)?
        .ok_or_else(|| uncertain("selected physical owner disappeared"))?;
    if !Arc::ptr_eq(&layout, &current_layout) {
        return Err(uncertain("selected physical layout changed during call"));
    }
    context.check().map_err(uncertain)?;
    verify_pending(control.as_mut(), request, context.local_node_id(), deadline)
        .map_err(uncertain)?;
    context.check().map_err(uncertain)?;
    remaining(deadline).map_err(uncertain)?;
    Ok(OwnedSelectedLocalRecoveryFenceReturn {
        request: request.clone(),
        command,
        remote_reported,
        _owner: owner,
    })
}
