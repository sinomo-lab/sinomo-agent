//! Local selected acceptor fencing only. The fresh reservation checks and
//! durable physical DATA are separate; neither is an RPC/serving certificate.
use super::*;
use crate::control_plane::{DfsMetadataShardRecoveryBallotRequest, DfsRecoveryBallotVerify};
use crate::metadata_api::{DfsSelectedLocalRecoveryContext, DfsSelectedLocalRecoveryFenceData};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsSelectedLocalRecoveryFenceResult {
    pub(crate) data: DfsSelectedLocalRecoveryFenceData,
    /// A fresh exact reservation read passed after local sync. This Boolean
    /// is not a lease, authenticated sender, promise or permission to serve.
    pub(crate) reservation_current: bool,
    pub(crate) post_verification_error: Option<String>,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn install_selected_local_recovery_fence(
        &mut self,
        request: &DfsMetadataShardRecoveryBallotRequest,
        deadline: Instant,
    ) -> Result<DfsSelectedLocalRecoveryFenceResult, DfsError> {
        let invalid = |message: String| DfsError::Metadata { message };
        let shard = DfsMetadataShardId::new(&request.serving_shard.shard_id);
        let (_, owner) = self
            .native_publication_owner_for_observation()
            .ok_or_else(|| {
                invalid(
                    "selected local recovery requires an actual retained native DATA owner".into(),
                )
            })?;
        let layout = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| invalid("selected local recovery has no local metadata backend".into()))?
            .selected_original_data_layout(&request.serving_shard.file_system_id, &shard)?
            .ok_or_else(|| {
                invalid("default service cannot install selected local recovery".into())
            })?;
        let context = DfsSelectedLocalRecoveryContext::from_owner(layout, owner)?;
        if context.local_node_id() != &self.node_id {
            return Err(invalid(
                "selected local recovery service differs from actual local identity".into(),
            ));
        }
        let before = {
            let control = self.control_plane.as_mut().ok_or_else(|| {
                invalid("selected local recovery has no authoritative control-plane owner".into())
            })?;
            DfsControlPlaneStore::new(control.as_mut())
                .verify_metadata_shard_recovery_ballot_before(
                    request,
                    context.local_node_id(),
                    deadline,
                )?
        };
        if let DfsRecoveryBallotVerify::Refused(reason) = before {
            return Err(invalid(format!(
                "selected local recovery reservation refused before persistence: {reason:?}"
            )));
        }
        // The backend owns only its normal local writer. No admission or
        // metadata-shard fence is reacquired around the authoritative RPCs.
        let data = self
            .metadata_backend
            .as_mut()
            .expect("backend checked")
            .install_selected_local_recovery_fence(request, &context)?;
        let rechecked = (|| -> Result<(), DfsError> {
            let control = self.control_plane.as_mut().ok_or_else(|| {
                invalid("selected local recovery control-plane owner disappeared".into())
            })?;
            let after = DfsControlPlaneStore::new(control.as_mut())
                .verify_metadata_shard_recovery_ballot_before(
                    request,
                    context.local_node_id(),
                    deadline,
                )?;
            if let DfsRecoveryBallotVerify::Refused(reason) = after {
                return Err(invalid(format!(
                    "selected local recovery reservation refused after persistence: {reason:?}"
                )));
            }
            if context.physical_identity(
                &request.serving_shard.file_system_id,
                &shard,
                &data.fence.physical.shard_root,
            )? != data.fence.physical
            {
                return Err(invalid(
                    "selected local recovery physical owner changed after persistence".into(),
                ));
            }
            Ok(())
        })();
        // Never withdraw a durable fence after cancellation, deadline or an
        // unavailable fresh view. Return the historical DATA without a proof.
        Ok(DfsSelectedLocalRecoveryFenceResult {
            data,
            reservation_current: rechecked.is_ok(),
            post_verification_error: rechecked.err().map(|error| error.to_string()),
        })
    }
}

mod native_tests;
