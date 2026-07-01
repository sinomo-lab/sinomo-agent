//! Shared validation for replica-side write transfer messages.
//!
//! Stage, commit, and repair handlers all need the same stale-base,
//! conflicting-write, and response-vs-pending-intent checks. Keeping those
//! checks here prevents the transfer protocol handlers from growing separate
//! versions of the same replica write invariants.

use sinomo_node::NodeId;

use crate::service_content::object_content_identity;
use crate::transfer::PendingNetworkedWrite;
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn ensure_command_base_matches_current(
        &self,
        operation: &str,
        object_id: &DfsObjectId,
        expected_base_version: Option<&DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let current = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            self.metadata.object(object_id).cloned()
        };
        let Some(current) = current else {
            return Ok(());
        };
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash == content_hash
        {
            return Ok(());
        }
        if let Some(expected_base_version) = expected_base_version
            && &current.version != expected_base_version
        {
            // Content is addressed by hash, so a repair/stage carrying the exact
            // bytes the object currently references is never stale in a harmful
            // way when metadata-only operations have bumped the version label.
            if current.content_hash.as_deref() != Some(content_hash) {
                return Err(DfsError::Distributed {
                    message: format!(
                        "stale DFS {operation} base for {object_id}: expected {}, current {}, command {}",
                        expected_base_version, current.version, version
                    ),
                });
            }
        }
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: current_hash.to_string(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn patch_base_identity_for_current_object(
        &self,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
    ) -> Result<Option<ContentVersionIdentity>, DfsError> {
        let current = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            self.metadata.object(object_id).cloned()
        };
        Ok(current
            .as_ref()
            .filter(|object| object.version == *base_version)
            .and_then(object_content_identity))
    }

    pub(crate) fn ensure_no_conflicting_pending_replica_write(
        &self,
        operation: &str,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        if let Some(stage) = self.active_replica_stages.get(object_id) {
            if stage.intent_id != intent_id
                && (stage.version != *version || stage.content_hash != content_hash)
            {
                return Err(DfsError::Distributed {
                    message: format!(
                        "conflicting active DFS {operation} for {object_id}: local stage {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                        stage.intent_id,
                        stage.requester_node_id,
                        stage.version,
                        stage.content_hash,
                        requester_node_id,
                        version,
                        content_hash
                    ),
                });
            }
        }
        for (pending_intent_id, pending) in &self.pending_writes {
            if pending.object_id != *object_id {
                continue;
            }
            let Some(intent) = self.coordinator.intent(pending_intent_id) else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            if pending_intent_id == intent_id && intent.requesting_node_id == *requester_node_id {
                continue;
            }
            if pending.version == *version && pending.content_hash == content_hash {
                continue;
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting pending DFS {operation} for {object_id}: local intent {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                    pending_intent_id,
                    intent.requesting_node_id,
                    pending.version,
                    pending.content_hash,
                    requester_node_id,
                    version,
                    content_hash
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_response_matches_pending_write(
        &self,
        label: &str,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        pending: &PendingNetworkedWrite,
    ) -> Result<(), DfsError> {
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown DFS {label} response intent: {intent_id}"),
            })?;
        if &intent.file_system_id != file_system_id
            || &intent.object_id != object_id
            || &intent.target_version != version
        {
            return Err(DfsError::Distributed {
                message: format!("DFS {label} response metadata does not match intent {intent_id}"),
            });
        }
        if !intent.desired_replica_nodes.contains(replica_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS {label} response from undesired replica {replica_node_id} for intent {intent_id}"
                ),
            });
        }
        if &pending.object_id != object_id
            || &pending.version != version
            || pending.content_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: pending.content_hash.clone(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }
}
