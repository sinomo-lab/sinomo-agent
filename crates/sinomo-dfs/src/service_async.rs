//! Async data-plane draining for the networked DFS service.
//!
//! This module owns async transport polling, bounded quiescence waits,
//! and deferred routed-write response flushing. Synchronous protocol
//! message handlers stay in `service_protocol`; transfer assembly is split
//! across the focused transfer modules and `service_outbound`.

use crate::*;

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    pub async fn process_incoming_async(
        &mut self,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.prune_idle_transfer_state();
        if self
            .transport
            .pending_incoming_count_async()
            .await
            .map_err(DfsError::from_network)?
            == Some(0)
        {
            self.flush_deferred_untracked_routed_write_responses_async()
                .await?;
            return Ok(Vec::new());
        }
        let inbound_messages = self
            .transport
            .drain_incoming_messages_async()
            .await
            .map_err(DfsError::from_network)?;
        let events = self.process_routed_messages_with_options(inbound_messages, true)?;
        self.flush_deferred_untracked_routed_write_responses_async()
            .await?;
        Ok(events)
    }

    pub(crate) async fn process_incoming_async_until_quiescent(
        &mut self,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        let mut events = Vec::new();
        for _ in 0..DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES {
            let batch = self.process_incoming_async().await?;
            events.extend(batch);
            if self
                .transport
                .pending_incoming_count_async()
                .await
                .map_err(DfsError::from_network)?
                == Some(0)
            {
                return Ok(events);
            }
        }
        Err(DfsError::Network {
            message: format!(
                "DFS async data-plane maintenance did not quiesce after {DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES} passes"
            ),
        })
    }

    async fn flush_deferred_untracked_routed_write_responses_async(
        &mut self,
    ) -> Result<(), DfsError> {
        if self
            .writes
            .deferred_untracked_routed_write_responses
            .is_empty()
        {
            return Ok(());
        }
        let pending = self
            .writes
            .deferred_untracked_routed_write_responses
            .iter()
            .map(|(intent_id, object_id)| (intent_id.clone(), object_id.clone()))
            .collect::<Vec<_>>();
        for (intent_id, object_id) in pending {
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                self.writes
                    .deferred_untracked_routed_write_responses
                    .remove(&intent_id);
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::Committed)
                || intent.requesting_node_id == self.node_id
                || self
                    .writes
                    .pending_remote_write_responses
                    .contains_key(&intent_id)
            {
                self.writes
                    .deferred_untracked_routed_write_responses
                    .remove(&intent_id);
                continue;
            }
            let outcome = self.write_outcome(&intent_id)?;
            if !matches!(outcome, NetworkedDfsWriteOutcome::Committed { .. }) {
                self.writes
                    .deferred_untracked_routed_write_responses
                    .remove(&intent_id);
                continue;
            }
            let result = DfsDistributedWriteResult::Outcome { outcome };
            let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
                &object_id,
                &intent.requesting_node_id,
                &result,
                None,
                DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT,
            )?;
            let sent = self
                .send_dfs_message_if_routable_async(
                    &intent.requesting_node_id,
                    DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                        request_id: intent.intent_id.clone(),
                        object_id: object_id.clone(),
                        result,
                        metadata_journal_batches,
                    }),
                )
                .await?;
            if sent {
                self.writes
                    .deferred_untracked_routed_write_responses
                    .remove(&intent_id);
            }
        }
        Ok(())
    }
}
