use crate::{DaemonPaths, client::DaemonObjectRpcAsyncHost};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord,
    cluster::{ENDPOINT_RECOVERY_PROOF_TIMEOUT, EndpointRecoveryHost},
    kv::{KvAuthoritativeRead, KvSnapshot},
    network::{
        ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterPaths, DaemonRpcTimeouts,
        NetworkError, ObjectRpcCall, ObjectRpcRequest,
    },
};

pub(crate) struct DaemonEndpointRecoveryHost<'a> {
    paths: &'a DaemonPaths,
    runtime: &'a tokio::runtime::Handle,
    observed: Option<(ClusterEndpointRecord, u64, u64, std::time::Instant)>,
}

impl<'a> DaemonEndpointRecoveryHost<'a> {
    pub fn new(paths: &'a DaemonPaths, runtime: &'a tokio::runtime::Handle) -> Self {
        Self {
            paths,
            runtime,
            observed: None,
        }
    }

    /// Publish only the real channel observed during this invocation, after the
    /// cluster owner has validated the snapshot and committed the endpoint.
    /// A receipt replay must never invent fresh reachability from old proof.
    pub fn record_validated_read(
        &mut self,
        spec: &sinomo_core::cluster::EndpointRecoverySpec,
    ) -> Result<(), NetworkError> {
        let Some((hint, sequence, latency_ms, started)) = self.observed.take() else {
            return Ok(());
        };
        let paths = crate::cluster::cluster_paths_from_daemon_paths(self.paths)
            .map_err(|e| NetworkError::directory(e.to_string()))?;
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
        let endpoint = state
            .members
            .iter()
            .find(|m| {
                m.node_id == spec.peer_node_id
                    && m.node_public_key == spec.peer_public_key
                    && m.state.is_authorized()
            })
            .and_then(|m| {
                m.endpoints
                    .iter()
                    .find(|e| e.endpoint_id == hint.endpoint_id && e.address == hint.address)
            });
        if state.cluster_id != spec.cluster_id
            || state.local_node_id != spec.local_node_id
            || endpoint.is_none()
            || started.elapsed() > ENDPOINT_RECOVERY_PROOF_TIMEOUT
        {
            return Err(NetworkError::outcome_unknown(
                "publish recovered endpoint observation",
                Some(spec.operation_id.as_str()),
                "endpoint effect was recorded, but its current identity/freshness changed; inspect the original operation",
            ));
        }
        let endpoint = endpoint.expect("checked");
        let coordinator = crate::client::daemon_rpc_observation_coordinator();
        let key = sinomo_core::cluster::cluster_command_observation_key(Some(&paths), endpoint);
        let mut result = Ok(());
        if let Some(reservation) = coordinator.try_reserve_success(key, sequence) {
            reservation.publish(|| {
                result = sinomo_core::cluster::ClusterCommandConnectivityObservation::from_rpc(
                    sinomo_core::network::ClusterObjectRpcObservation::success(Some(latency_ms)),
                )
                .record_daemon_owned(&paths, &state, endpoint)
                .map(|_| ());
            });
        }
        result.map_err(|e| {
            NetworkError::outcome_unknown(
                "publish recovered endpoint observation",
                Some(spec.operation_id.as_str()),
                e.to_string(),
            )
        })
    }
}

impl EndpointRecoveryHost for DaemonEndpointRecoveryHost<'_> {
    fn authoritative_read(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        let root = self
            .paths
            .root
            .parent()
            .ok_or_else(|| NetworkError::directory("daemon DATA root absent"))?;
        let paths = ClusterPaths::new(&sinomo_core::local::SinomoDataDir::new(root));
        let sequence = crate::client::daemon_rpc_observation_coordinator().next_sequence();
        let started = std::time::Instant::now();
        let proof = self.runtime.block_on(async {
            tokio::time::timeout(ENDPOINT_RECOVERY_PROOF_TIMEOUT, async {
                let mut host =
                    DaemonObjectRpcAsyncHost::new(self.runtime, false).with_paths(self.paths);
                // call_endpoint authenticates both existing node principals using
                // the supplied membership fence; the hint never substitutes a key.
                let mut client = ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(
                    &mut host, &paths, state,
                )?;
                let payload = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()
                    .map_err(|e| NetworkError::directory(e.to_string()))?;
                let request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                let result = client
                    .call_endpoint(
                        state,
                        endpoint,
                        ObjectRpcCall::value(request),
                        ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                            ENDPOINT_RECOVERY_PROOF_TIMEOUT,
                        ))
                        .with_overall_timeout(ENDPOINT_RECOVERY_PROOF_TIMEOUT),
                    )
                    .await?;
                let response = result.into_materialized_value_response(usize::MAX).await?;
                sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
                    .map_err(|e| NetworkError::directory(e.to_string()))
            })
            .await
            .map_err(|_| NetworkError::directory("endpoint authority read deadline exceeded"))?
        })?;
        self.observed = Some((
            endpoint.clone(),
            sequence,
            started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            started,
        ));
        Ok(proof)
    }
}
