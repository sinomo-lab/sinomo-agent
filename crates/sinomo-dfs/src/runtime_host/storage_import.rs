//! Pre-mount content ingress for owned storage conversion. Metadata visibility
//! remains unavailable; bytes use the ordinary retained-blob receiver.
use super::*;

pub struct DfsStorageImportContentReceiver {
    service: crate::NetworkedDfsService<crate::DfsNodeTransport>,
    requester: NodeId,
    _owner: Arc<DfsDataOwnerLock>,
}

impl DfsStorageImportContentReceiver {
    pub fn for_authenticated_request(
        context: &sinomo_module::ComponentContext,
        request_context: &sinomo_core::service::ServiceContext,
    ) -> Result<Self, crate::DfsError> {
        if !matches!(
            context.host_role(),
            ComponentHostRole::Main | ComponentHostRole::Embedded
        ) {
            return Err(crate::DfsError::Metadata {
                message: "shadow cannot own storage-import content ingress".into(),
            });
        }
        let authenticated_requester = request_context
            .signer
            .cluster_identity()
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "storage-import content ingress requires an authenticated cluster signer"
                    .into(),
            })?
            .0
            .parse::<NodeId>()
            .map_err(|error| crate::DfsError::Metadata {
                message: error.to_string(),
            })?;
        let owner =
            retain_component_data_owner(context).map_err(|error| crate::DfsError::Metadata {
                message: error.to_string(),
            })?;
        // Require the actual registered native host. A missing context client or
        // a disconnected local facade cannot provide this receiver's authority.
        let host = registered_dfs_runtime_host(context.data_dir())
            .map_err(|error| crate::DfsError::Metadata {
                message: error.to_string(),
            })?
            .ok_or_else(|| crate::DfsError::Metadata {
                message:
                    "storage-import content ingress requires the actual registered native host"
                        .into(),
            })?;
        let runtime = context.runtime().ok_or_else(|| crate::DfsError::Metadata {
            message: "storage-import content ingress has no owned runtime".into(),
        })?;
        let paths = ClusterPaths::new(context.data_dir());
        let local_node = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(crate::DfsError::from_network)?
            .local_node_id;
        let peer = Arc::new(data_plane::DaemonDfsPeerTransport { host, native_prefix: Some(super::native_prefix_return::NativePrefixOwnerContext {data_dir: context.data_dir().clone(), runtime:runtime.clone()}) });
        let transport = crate::DfsNodeTransport::new_with_peer_transports_for_nodes(
            local_node,
            [authenticated_requester.clone()],
            peer.clone(),
            peer,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        );
        // The ordinary native exact-read client validates the current stamped
        // authority; conversion cannot depend on metadata mounts being ready.
        let control = control_plane::DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime);
        let content = LocalContentStore::new(context.data_dir().dfs_dir().join("content"))?;
        Ok(Self {
            service: crate::NetworkedDfsService::new(transport, content)
                .with_boxed_control_plane(Box::new(control)),
            requester: authenticated_requester,
            _owner: owner,
        })
    }

    pub fn content_store(&self) -> &LocalContentStore {
        &self.service.content
    }

    pub fn process(
        &mut self,
        command: crate::DfsMessageCommand,
        blob: Option<crate::ContentStoreQuarantinedBlob>,
    ) -> Result<crate::DfsMessageProcessResult, crate::DfsError> {
        if command.routed_message.source_node_id != self.requester
            || !matches!(
                &command.routed_message.message,
                crate::DfsNetworkMessage::StageRetainedBlob(_)
            )
        {
            return Err(crate::DfsError::Metadata {
                message:
                    "storage-import ingress accepts only its authenticated retained-content stage"
                        .into(),
            });
        }
        self.service
            .process_message_command_report_with_preingested_blob(command, blob)
            .outcome
    }
}
