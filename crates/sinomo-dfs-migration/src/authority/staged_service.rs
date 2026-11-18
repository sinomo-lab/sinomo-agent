//! The migration component's peer service: the per-shard conversion's
//! install exchange on a shard's members, a converting takeover's promises,
//! and other shards' contribution reads. Normal DFS roots stay untouched until
//! the shard's activation.
use super::*;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

pub(super) mod conversion_install;
const ROUTES: &[&str] = &[
    super::contribution_extraction::ROUTE,
    conversion_install::FLOORS_ROUTE,
    conversion_install::CHUNK_ROUTE,
    conversion_install::STAGE_ROUTE,
    super::owner::shard_conversion::takeover::promise::ROUTE,
];

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceFloor {
    pub(super) shard_id: sinomo_dfs::DfsMetadataShardId,
    pub(super) journal_sequence: u64,
    pub(super) state_fingerprint: String,
    pub(super) durable_prefix_proof: Option<sinomo_dfs::DfsMetadataPrefixProof>,
}
impl From<&crate::metadata::ack_floor::LegacyAcknowledgement> for SourceFloor {
    fn from(value: &crate::metadata::ack_floor::LegacyAcknowledgement) -> Self {
        Self {
            shard_id: value.shard_id.clone(),
            journal_sequence: value.journal_sequence,
            state_fingerprint: value.state_fingerprint.clone(),
            durable_prefix_proof: value.durable_prefix_proof.clone(),
        }
    }
}
impl From<SourceFloor> for crate::metadata::ack_floor::LegacyAcknowledgement {
    fn from(value: SourceFloor) -> Self {
        Self {
            shard_id: value.shard_id,
            journal_sequence: value.journal_sequence,
            state_fingerprint: value.state_fingerprint,
            durable_prefix_proof: value.durable_prefix_proof,
        }
    }
}

pub(super) struct SourceTransferVerifier {
    verifier: crate::metadata::source_ancestry::SourceAncestryVerifier,
    next_ordinal: u64,
}

pub(super) struct StagedImportService {
    pub(super) authority: Arc<NativeImportAuthority>,
    pub(super) data_owner: Arc<sinomo_dfs::runtime_host::DfsDataOwnerLock>,
    pub(super) component_context: sinomo_module::ComponentContext,
}
impl NativeImportAuthority {
    fn replay_source_chunks(
        root: &Path,
        verifier: &mut crate::metadata::source_ancestry::SourceAncestryVerifier,
        through: Option<u64>,
    ) -> Result<u64> {
        let mut ordinal = 0u64;
        loop {
            if through.is_some_and(|end| ordinal == end) {
                break;
            }
            let path = root.join(format!("{ordinal:020}.sobj"));
            if !path.is_file() {
                if through.is_some() {
                    return Err(error("source ancestry durable chunk cursor has a gap"));
                }
                break;
            }
            let limit = crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES as u64
                + 64 * 1024;
            let file = std::fs::File::open(&path).map_err(error)?;
            if file.metadata().map_err(error)?.len() > limit {
                return Err(error(
                    "durable source ancestry chunk exceeds its bounded encoding",
                ));
            }
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut std::io::Read::take(file, limit + 1), &mut bytes)
                .map_err(error)?;
            if bytes.len() as u64 > limit {
                return Err(error(
                    "durable source ancestry chunk grew beyond its bounded encoding",
                ));
            }
            let chunk = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
            verifier.apply(&chunk)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| error("source ancestry chunk ordinal overflow"))?;
        }
        Ok(ordinal)
    }
}

impl Service for StagedImportService {
    fn name(&self) -> &'static str {
        "dfs-migration"
    }
    fn routes(&self) -> &'static [&'static str] {
        ROUTES
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        ROUTES
            .iter()
            .map(|route| {
                let stage = *route == conversion_install::STAGE_ROUTE;
                let route = ServiceRoute::new(*route, "dfs-migration")
                    .with_auth_scope("cluster-member")
                    .require_capability("dfs.metadata.replicate");
                if stage { route.with_request_body_mode(ServiceBodyMode::Optional) } else { route }
            })
            .collect()
    }
    fn handle(&self, mut context: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let _owner = Arc::clone(&self.data_owner);
            let signer = context.signer.cluster_identity().ok_or_else(|| {
                ServiceError::Service(
                    "staged metadata requires an authenticated cluster signer".into(),
                )
            })?;
            let source = signer
                .0
                .parse::<sinomo_core::node::NodeId>()
                .map_err(|e| ServiceError::Service(e.to_string()))?;
            match request.route.as_str() {
                super::owner::shard_conversion::takeover::promise::ROUTE => {
                    use super::owner::shard_conversion::takeover::promise;
                    let ask = context
                        .decode_request_body_bounded::<promise::PromiseRequest>(&request, promise::MAX_REQUEST_BYTES)
                        .await?;
                    let response = tokio::task::block_in_place(|| self.authority.receive_takeover_promise(&source, ask))
                        .map_err(|error| ServiceError::Service(error.to_string()))?;
                    context.respond_value(promise::ROUTE, response)?;
                }
                super::contribution_extraction::ROUTE => {
                    let extraction = context
                        .decode_request_body_bounded::<super::contribution_extraction::Request>(
                            &request,
                            super::contribution_extraction::MAX_REQUEST_BYTES,
                        )
                        .await?;
                    let response = tokio::task::block_in_place(|| {
                        self.authority.receive_contribution_extraction(&source, extraction)
                    })
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    context.respond_value(super::contribution_extraction::ROUTE, response)?;
                }
                conversion_install::FLOORS_ROUTE => {
                    let challenge = context
                        .decode_request_body_bounded::<conversion_install::FloorsRequest>(
                            &request,
                            conversion_install::MAX_FLOORS_REQUEST_BYTES,
                        )
                        .await?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority.receive_conversion_floors(
                            &self.component_context,
                            source,
                            challenge,
                        )
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(conversion_install::FLOORS_ROUTE, result)?;
                }
                conversion_install::CHUNK_ROUTE => {
                    let transfer = context
                        .decode_request_body::<conversion_install::ChunkTransfer>(&request)
                        .await?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority.receive_conversion_chunk(source, transfer)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(conversion_install::CHUNK_ROUTE, result)?;
                }
                conversion_install::STAGE_ROUTE => {
                    let head = context
                        .decode_request_body::<conversion_install::StageHead>(&request)
                        .await?;
                    tokio::task::block_in_place(|| {
                        self.authority.validate_conversion_stage_head(&source, &head)
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    if !context.has_request_attachment() {
                        return Err(ServiceError::Service(
                            "a conversion stage requires its image attachment".into(),
                        ));
                    }
                    let image = sinomo_dfs::receive_metadata_candidate_image(
                        self.authority.data_root.clone(),
                        head.command.snapshot.clone(),
                        context.take_request_body(),
                        Arc::clone(&_owner),
                    )
                    .await
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    let result = tokio::task::block_in_place(|| {
                        self.authority.receive_conversion_stage(
                            &self.component_context,
                            source,
                            head,
                            image,
                        )
                    })
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                    context.respond_value(conversion_install::STAGE_ROUTE, result)?;
                }
                _ => return Err(ServiceError::UnknownRoute(request.route)),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub(super) fn factory()
-> std::result::Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    sinomo_module::StaticComponentFactory::new(
        sinomo_module::ComponentDescriptor::new("dfs-migration", env!("CARGO_PKG_VERSION"))
            .require(sinomo_dfs::dfs_core_component_requirement()),
        move |context, scope| {
            if !super::source_effect_host(context) {
                return Ok(());
            }
            if context.cluster_access().is_none()
                && sinomo_dfs::runtime_host::registered_component_cluster(context)
                    .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?
                    .is_none()
            {
                // A node without cluster access has no peers to serve.
                return Ok(());
            }
            let owner = sinomo_dfs::runtime_host::retain_component_data_owner(context)
                .map_err(|e| sinomo_module::ComponentError::component("dfs-migration", e))?;
            let data_root = context.data_dir_root().canonicalize().map_err(|error| {
                sinomo_module::ComponentError::component("dfs-migration", error)
            })?;
            let lifecycle =
                sinomo_dfs::dfs_storage_preparation_lifecycle(&data_root).ok_or_else(|| {
                    sinomo_module::ComponentError::component(
                        "dfs-migration",
                        "staged service has no actual current provider lifecycle",
                    )
                })?;
            let runtime = super::shared_owner(context, lifecycle)?;
            scope.set_services(
                ServiceRegistryBuilder::default()
                    .register(StagedImportService {
                        authority: Arc::clone(&runtime.authority),
                        data_owner: owner,
                        component_context: context.clone(),
                    })
                    .build(),
            );
            Ok(())
        },
    )
}

#[cfg(test)]
mod ancestry_ledger_tests {
    use super::*;

    fn empty_cut(root: &Path) -> MetadataSourceCut {
        // No ACKs, cfg, native receipt or permission is created by this
        // filesystem-boundary fixture. The verifier tests only durable framing.
        MetadataSourceCut {
            retained_placement_reservations: Vec::new(),
            source_root: root.to_owned(),
            source_identity: "ledger-fixture".into(),
            scope_shard: sinomo_dfs::DfsMetadataShardId::new("root"),
            file_system_ids: Vec::new(),
            source_snapshot_fingerprint: "snapshot-fixture".into(),
            source_format_version: None,
            source_exact_prefix: None,
            converted_image_fingerprint: "projection-fixture".into(),
            current_image_fingerprint: "image-fixture".into(),
            target_exact_prefix: sinomo_dfs::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 0,
                digest: "target-not-used-by-source-verifier".into(),
            },
            source_journal_sequence: 0,
            source_state_fingerprint: "source-fixture".into(),
            projection_assignments: Vec::new(),
            staged_root: root.join("current-image"),
            pending_operations: Vec::new(),
            original_handoff: None,
        }
    }
    #[test]
    fn source_ancestry_durable_cursor_rejects_missing_chunk() {
        let root = sinomo_testdir::TestDir::new("source-ancestry-cursor-gap");
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &empty_cut(root.path()),
            &[],
            &[],
        )
        .unwrap();
        assert!(
            NativeImportAuthority::replay_source_chunks(root.path(), &mut verifier, Some(1))
                .is_err()
        );
    }
    #[test]
    fn source_ancestry_durable_cursor_rejects_oversized_encoding_before_read() {
        let root = sinomo_testdir::TestDir::new("source-ancestry-cursor-oversized");
        let file = std::fs::File::create(root.join("00000000000000000000.sobj")).unwrap();
        file.set_len(
            crate::metadata::source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES as u64
                + 64 * 1024
                + 1,
        )
        .unwrap();
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &empty_cut(root.path()),
            &[],
            &[],
        )
        .unwrap();
        assert!(
            NativeImportAuthority::replay_source_chunks(root.path(), &mut verifier, None).is_err()
        );
    }
    #[test]
    fn source_ancestry_durable_cursor_rejects_truncated_serialized_chunk() {
        let root = sinomo_testdir::TestDir::new("source-ancestry-cursor-truncated");
        std::fs::write(root.join("00000000000000000000.sobj"), [0xff, 0x03, 0x01]).unwrap();
        let mut verifier = crate::metadata::source_ancestry::SourceAncestryVerifier::new(
            &empty_cut(root.path()),
            &[],
            &[],
        )
        .unwrap();
        assert!(
            NativeImportAuthority::replay_source_chunks(root.path(), &mut verifier, None).is_err()
        );
    }
}

fn candidate_publication_busy(failure: &MetadataBackendError) -> bool {
    matches!(failure, MetadataBackendError::Io { source, .. } if source.kind() == std::io::ErrorKind::WouldBlock)
}
#[cfg(test)]
mod candidate_contention_tests {
    use super::*;
    #[test]
    fn only_actual_candidate_would_block_is_pending() {
        let busy = MetadataBackendError::Io { path: "owned/metadata-candidate.lock".into(), source: std::io::Error::from(std::io::ErrorKind::WouldBlock) };
        assert!(candidate_publication_busy(&busy));
        assert!(!candidate_publication_busy(&MetadataBackendError::Serde("malformed image".into())));
        assert!(!candidate_publication_busy(&MetadataBackendError::Io { path: "owned/image".into(), source: std::io::Error::from(std::io::ErrorKind::InvalidData) }));
    }
}
