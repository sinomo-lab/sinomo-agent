//! In-service metadata journal and snapshot exchange.
//!
//! This module handles synchronous DFS data-plane messages that replicate
//! metadata journal batches, advance replication cursors, and install snapshot
//! repairs after a replica or routed-response mirror falls behind. The
//! background publish planner remains in `metadata_replication`.

use std::path::Path;
use std::time::Instant;

use sinomo_core::node::NodeId;

use crate::content_root_catalog::DfsContentRootCatalogFragmentBuilder;
use crate::service_state::PendingMetadataReplicationResponseKind;
use crate::*;

struct ValidatedMetadataJournalBatchSource {
    permit: DfsMetadataReplicaIngestPermit,
    replica_context: DfsMetadataReplicaShardContext,
}

fn content_root_catalog_file_system_config_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("config"),
            None,
        ) if !file_system_id.is_empty()
    )
}

fn content_root_catalog_metadata_shard_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-shards"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

fn content_root_catalog_membership_handoff_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-membership-handoffs"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsContentRootCatalogTopologySnapshot {
    file_systems: Vec<DfsFileSystemConfig>,
    shards: Vec<DfsMetadataShardConfig>,
    pending_handoffs: Vec<DfsMetadataShardMembershipHandoff>,
}

impl DfsContentRootCatalogTopologySnapshot {
    fn canonical(
        mut file_systems: Vec<DfsFileSystemConfig>,
        mut shards: Vec<DfsMetadataShardConfig>,
        mut pending_handoffs: Vec<DfsMetadataShardMembershipHandoff>,
    ) -> Self {
        file_systems.sort_by(|left, right| left.file_system_id.cmp(&right.file_system_id));
        for shard in &mut shards {
            shard.replica_node_ids.sort();
            shard.replica_node_ids.dedup();
        }
        shards.sort_by(|left, right| {
            left.file_system_id
                .cmp(&right.file_system_id)
                .then_with(|| left.shard_id.cmp(&right.shard_id))
        });
        pending_handoffs.sort_by(|left, right| {
            left.retiring_shard
                .file_system_id
                .cmp(&right.retiring_shard.file_system_id)
                .then_with(|| {
                    left.retiring_shard
                        .shard_id
                        .cmp(&right.retiring_shard.shard_id)
                })
                .then_with(|| left.handoff_id.cmp(&right.handoff_id))
        });
        Self {
            file_systems,
            shards,
            pending_handoffs,
        }
    }

    fn file_system_ids(&self) -> Vec<DfsFileSystemId> {
        self.file_systems
            .iter()
            .map(|config| config.file_system_id.clone())
            .collect()
    }

    fn has_duplicate_identities(&self) -> bool {
        self.file_systems
            .windows(2)
            .any(|pair| pair[0].file_system_id == pair[1].file_system_id)
            || self.shards.windows(2).any(|pair| {
                pair[0].file_system_id == pair[1].file_system_id
                    && pair[0].shard_id == pair[1].shard_id
            })
    }

    fn deduplicated_identities(&self) -> Self {
        let mut topology = self.clone();
        topology
            .file_systems
            .dedup_by(|left, right| left.file_system_id == right.file_system_id);
        topology.shards.dedup_by(|left, right| {
            left.file_system_id == right.file_system_id && left.shard_id == right.shard_id
        });
        topology
    }

    fn topology_digest(&self) -> Result<String, DfsError> {
        dfs_content_root_catalog_topology_digest_with_file_systems(&self.file_systems, &self.shards)
            .map_err(DfsError::from)
    }

    fn incomplete_reasons(&self) -> Vec<DfsContentRootCatalogIncompleteReason> {
        let mut reasons = Vec::new();
        if self.file_systems.is_empty() {
            reasons.push(DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured);
        }
        if self.shards.is_empty() {
            reasons.push(DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured);
        }
        for file_system in &self.file_systems {
            if !self
                .shards
                .iter()
                .any(|shard| shard.file_system_id == file_system.file_system_id)
            {
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                        file_system_id: file_system.file_system_id.clone(),
                    },
                );
            }
        }
        for handoff in &self.pending_handoffs {
            reasons.push(
                DfsContentRootCatalogIncompleteReason::MembershipHandoffInProgress {
                    file_system_id: handoff.retiring_shard.file_system_id.clone(),
                    shard_id: DfsMetadataShardId::new(handoff.retiring_shard.shard_id.clone()),
                    handoff_id: handoff.handoff_id.clone(),
                },
            );
        }
        reasons
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn global_content_root_catalog_topology(
        &mut self,
    ) -> Result<Option<DfsContentRootCatalogTopologySnapshot>, DfsError> {
        self.refresh_control_plane_latest()?;
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let mut file_systems = Vec::new();
        let mut shards = Vec::new();
        let mut pending_handoffs = Vec::new();
        for entry in control_plane.range_prefix("/dfs/filesystems/")? {
            if content_root_catalog_file_system_config_key_is_exact(&entry.key) {
                file_systems.push(entry.decode::<DfsFileSystemConfig>().map_err(|error| {
                    DfsError::Metadata {
                        message: format!(
                            "content-root catalog filesystem config {} is invalid: {error}",
                            entry.key
                        ),
                    }
                })?);
            } else if content_root_catalog_metadata_shard_key_is_exact(&entry.key) {
                shards.push(entry.decode::<DfsMetadataShardConfig>().map_err(|error| {
                    DfsError::Metadata {
                        message: format!(
                            "content-root catalog metadata shard {} is invalid: {error}",
                            entry.key
                        ),
                    }
                })?);
            } else if content_root_catalog_membership_handoff_key_is_exact(&entry.key) {
                pending_handoffs.push(
                    entry
                        .decode::<DfsMetadataShardMembershipHandoff>()
                        .map_err(|error| DfsError::Metadata {
                            message: format!(
                                "content-root catalog membership handoff {} is invalid: {error}",
                                entry.key
                            ),
                        })?,
                );
            }
        }
        Ok(Some(DfsContentRootCatalogTopologySnapshot::canonical(
            file_systems,
            shards,
            pending_handoffs,
        )))
    }

    fn install_incomplete_content_root_catalog(
        &self,
        expected_topology: &DfsContentRootCatalogTopologySnapshot,
        mut reasons: Vec<DfsContentRootCatalogIncompleteReason>,
        content_root: &Path,
    ) -> Result<DfsContentRootCatalog, DfsError> {
        for reason in expected_topology.incomplete_reasons() {
            if !reasons.contains(&reason) {
                reasons.push(reason);
            }
        }
        let expected_topology_digest = expected_topology.topology_digest()?;
        let identities = expected_topology
            .shards
            .iter()
            .map(DfsContentRootCatalogShardIdentity::from_config)
            .collect();
        let generated_at_unix_nanos = crate::metadata_tombstone::metadata_now_unix_nanos();
        let catalog = DfsContentRootCatalog::from_topology_fragments(
            expected_topology_digest,
            expected_topology.file_system_ids(),
            identities,
            generated_at_unix_nanos,
            reasons,
            Vec::new(),
        )?;
        LocalDfsContentRootCatalogStore::new(content_root)
            .install(catalog)
            .map_err(DfsError::from)
    }

    /// Persist a fail-closed refresh marker even when the supplied topology is
    /// itself malformed and cannot be represented as an ordinary incomplete
    /// catalog (for example, duplicate filesystem or shard identities).
    fn install_content_root_catalog_refresh_failure(
        &self,
        expected_topology: &DfsContentRootCatalogTopologySnapshot,
        error: &DfsError,
        content_root: &Path,
    ) -> Result<DfsContentRootCatalog, DfsError> {
        let reason = DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed {
            reason: error.to_string(),
        };
        if expected_topology.has_duplicate_identities() {
            let fallback_topology = expected_topology.deduplicated_identities();
            return self.install_incomplete_content_root_catalog(
                &fallback_topology,
                vec![reason],
                content_root,
            );
        }
        match self.install_incomplete_content_root_catalog(
            expected_topology,
            vec![reason.clone()],
            content_root,
        ) {
            Ok(catalog) => Ok(catalog),
            Err(expected_topology_error) => {
                let fallback_topology = expected_topology.deduplicated_identities();
                self.install_incomplete_content_root_catalog(
                    &fallback_topology,
                    vec![DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed {
                        reason: format!(
                            "{error}; expected topology could not be encoded in the failure marker: {expected_topology_error}"
                        ),
                    }],
                    content_root,
                )
            }
        }
    }

    /// Refresh the exact content-GC root catalog from every current metadata
    /// shard primary and atomically install the result under `content_root`.
    ///
    /// The caller supplies the all-filesystem shard snapshot it just read from
    /// cluster KV plus post-metadata-GC journal watermarks. Every page
    /// is bound to that topology, primary epoch, durable sequence, and state
    /// fingerprint. Any missing authority, handoff fence, transport gap, or
    /// pagination budget exhaustion installs an incomplete fail-closed catalog.
    pub fn refresh_content_root_catalog(
        &mut self,
        file_systems: &[DfsFileSystemConfig],
        shards: &[DfsMetadataShardConfig],
        pending_handoffs: &[DfsMetadataShardMembershipHandoff],
        content_root: impl AsRef<Path>,
        head_watermarks: &[DfsMetadataShardHeadWatermark],
        max_pages_per_shard: usize,
    ) -> Result<DfsContentRootCatalog, DfsError> {
        let content_root = content_root.as_ref();
        let result = self.refresh_content_root_catalog_inner(
            file_systems,
            shards,
            pending_handoffs,
            content_root,
            head_watermarks,
            max_pages_per_shard,
        );
        if let Err(error) = &result {
            let expected_topology = DfsContentRootCatalogTopologySnapshot::canonical(
                file_systems.to_vec(),
                shards.to_vec(),
                pending_handoffs.to_vec(),
            );
            if let Err(install_error) = self.install_content_root_catalog_refresh_failure(
                &expected_topology,
                error,
                content_root,
            ) {
                return Err(DfsError::Metadata {
                    message: format!(
                        "content-root catalog refresh failed ({error}) and its durable failure marker could not be installed ({install_error})"
                    ),
                });
            }
        }
        result
    }

    fn refresh_content_root_catalog_inner(
        &mut self,
        file_systems: &[DfsFileSystemConfig],
        shards: &[DfsMetadataShardConfig],
        pending_handoffs: &[DfsMetadataShardMembershipHandoff],
        content_root: impl AsRef<Path>,
        head_watermarks: &[DfsMetadataShardHeadWatermark],
        max_pages_per_shard: usize,
    ) -> Result<DfsContentRootCatalog, DfsError> {
        let trace_started = Instant::now();
        let content_root = content_root.as_ref();
        let expected_topology = DfsContentRootCatalogTopologySnapshot::canonical(
            file_systems.to_vec(),
            shards.to_vec(),
            pending_handoffs.to_vec(),
        );
        if max_pages_per_shard == 0 {
            return Err(DfsError::Metadata {
                message: "content-root catalog page budget must be greater than zero".to_string(),
            });
        }
        if expected_topology.has_duplicate_identities() {
            return Err(DfsError::Metadata {
                message: "content-root catalog topology contains duplicate identities".to_string(),
            });
        }
        let expected_incomplete_reasons = expected_topology.incomplete_reasons();
        if !expected_incomplete_reasons.is_empty() {
            return self.install_incomplete_content_root_catalog(
                &expected_topology,
                expected_incomplete_reasons,
                content_root,
            );
        }
        let expected_topology_digest = expected_topology.topology_digest()?;
        let identities = expected_topology
            .shards
            .iter()
            .map(DfsContentRootCatalogShardIdentity::from_config)
            .collect::<Vec<_>>();

        let current_topology = self.global_content_root_catalog_topology()?;
        if current_topology.as_ref() != Some(&expected_topology) {
            let observed_topology_digest = current_topology
                .as_ref()
                .map(DfsContentRootCatalogTopologySnapshot::topology_digest)
                .transpose()?;
            return self.install_incomplete_content_root_catalog(
                &expected_topology,
                vec![DfsContentRootCatalogIncompleteReason::TopologyChanged {
                    expected_topology_digest,
                    observed_topology_digest,
                }],
                content_root,
            );
        }

        let mut fragments = Vec::with_capacity(identities.len());
        let mut total_pages = 0usize;
        let mut total_entries = 0usize;
        for identity in &identities {
            let shard_started = Instant::now();
            let mut request = DfsContentRootCatalogPageRequest {
                identity: identity.clone(),
                expected_topology_digest: expected_topology_digest.clone(),
                minimum_journal_sequence: head_watermarks
                    .iter()
                    .find(|watermark| {
                        watermark.file_system_id == identity.file_system_id
                            && watermark.shard_id == identity.shard_id.0
                            && watermark.primary_node_id == identity.primary_node_id
                            && watermark.epoch == identity.epoch
                    })
                    .map(|watermark| watermark.journal_sequence)
                    .unwrap_or(0),
                minimum_state_fingerprint: head_watermarks
                    .iter()
                    .find(|watermark| {
                        watermark.file_system_id == identity.file_system_id
                            && watermark.shard_id == identity.shard_id.0
                            && watermark.primary_node_id == identity.primary_node_id
                            && watermark.epoch == identity.epoch
                    })
                    .map(|watermark| watermark.state_fingerprint.clone()),
                after: None,
                max_entries: DEFAULT_DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRIES,
            };
            let mut builder = DfsContentRootCatalogFragmentBuilder::new(&request)?;
            let mut page_count = 0usize;
            let mut used_legacy_page_limit = false;
            loop {
                let page = if identity.primary_node_id == self.node_id {
                    self.authoritative_content_root_catalog_page(&request)?
                } else {
                    let response = self.transport.request_content_root_catalog_page_direct(
                        &identity.primary_node_id,
                        &request,
                    );
                    let response = match response {
                        Err(error)
                            if page_count == 0
                                && request.max_entries
                                    > DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES =>
                        {
                            // The catalog wire shape predates the larger page
                            // limit. During a rolling upgrade an older primary
                            // safely rejects the larger request, so retry the first
                            // page at the historical 256-entry contract. Any
                            // second failure remains fail-closed.
                            request.max_entries = DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES;
                            builder = DfsContentRootCatalogFragmentBuilder::new(&request)?;
                            used_legacy_page_limit = true;
                            crate::dfs_perf_trace(format_args!(
                                "content-root-catalog-page-limit-fallback node={} filesystem={} shard={} primary={} first_error={}",
                                self.node_id,
                                identity.file_system_id,
                                identity.shard_id.0,
                                identity.primary_node_id,
                                error
                            ));
                            self.transport.request_content_root_catalog_page_direct(
                                &identity.primary_node_id,
                                &request,
                            )
                        }
                        response => response,
                    };
                    match response {
                        Ok(Some(page)) => page,
                        Ok(None) => DfsContentRootCatalogPage::incomplete_for_request(
                            &request,
                            DfsContentRootCatalogIncompleteReason::TransportUnavailable {
                                primary_node_id: identity.primary_node_id.clone(),
                                reason:
                                    "DFS transport does not support direct content-root catalog requests"
                                        .to_string(),
                            },
                            crate::metadata_tombstone::metadata_now_unix_nanos(),
                        ),
                        Err(error) => DfsContentRootCatalogPage::incomplete_for_request(
                            &request,
                            DfsContentRootCatalogIncompleteReason::TransportUnavailable {
                                primary_node_id: identity.primary_node_id.clone(),
                                reason: error.to_string(),
                            },
                            crate::metadata_tombstone::metadata_now_unix_nanos(),
                        ),
                    }
                };
                page_count = page_count.saturating_add(1);
                let Some(next_request) = builder.push_page(page)? else {
                    break;
                };
                if page_count >= max_pages_per_shard {
                    builder.push_page(DfsContentRootCatalogPage::incomplete_for_request(
                        &next_request,
                        DfsContentRootCatalogIncompleteReason::PageBudgetExceeded {
                            max_pages: max_pages_per_shard,
                        },
                        crate::metadata_tombstone::metadata_now_unix_nanos(),
                    ))?;
                    break;
                }
                request = next_request;
            }
            let fragment = builder.finish()?;
            total_pages = total_pages.saturating_add(page_count);
            total_entries = total_entries.saturating_add(fragment.entries.len());
            crate::dfs_perf_trace(format_args!(
                "content-root-catalog-shard-refresh node={} filesystem={} shard={} primary={} page_entry_limit={} legacy_limit={} pages={} entries={} complete={} total_ms={}",
                self.node_id,
                identity.file_system_id,
                identity.shard_id.0,
                identity.primary_node_id,
                request.max_entries,
                used_legacy_page_limit,
                page_count,
                fragment.entries.len(),
                fragment.coverage.is_complete(),
                shard_started.elapsed().as_millis()
            ));
            fragments.push(fragment);
        }

        let current_topology = self.global_content_root_catalog_topology()?;
        if current_topology.as_ref() != Some(&expected_topology) {
            let observed_topology_digest = current_topology
                .as_ref()
                .map(DfsContentRootCatalogTopologySnapshot::topology_digest)
                .transpose()?;
            return self.install_incomplete_content_root_catalog(
                &expected_topology,
                vec![DfsContentRootCatalogIncompleteReason::TopologyChanged {
                    expected_topology_digest,
                    observed_topology_digest,
                }],
                content_root,
            );
        }

        let generated_at_unix_nanos = fragments
            .iter()
            .map(|fragment| fragment.generated_at_unix_nanos)
            // Catalog freshness is only as recent as its oldest shard
            // observation. Using the newest fragment would overstate
            // cluster-wide coverage and could advance absence aging early.
            .min()
            .unwrap_or_else(crate::metadata_tombstone::metadata_now_unix_nanos);
        let identity_count = identities.len();
        let catalog_complete = fragments
            .iter()
            .all(DfsContentRootCatalogFragment::is_complete);
        let catalog = DfsContentRootCatalog::from_topology_fragments(
            expected_topology_digest,
            expected_topology.file_system_ids(),
            identities,
            generated_at_unix_nanos,
            Vec::new(),
            fragments,
        )?;
        let catalog = LocalDfsContentRootCatalogStore::new(content_root)
            .install(catalog)
            .map_err(DfsError::from)?;
        crate::dfs_perf_trace(format_args!(
            "content-root-catalog-refresh node={} shards={} pages={} entries={} complete={} total_ms={}",
            self.node_id,
            identity_count,
            total_pages,
            total_entries,
            catalog_complete,
            trace_started.elapsed().as_millis()
        ));
        Ok(catalog)
    }

    fn authoritative_content_root_catalog_page(
        &mut self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage, DfsError> {
        request.validate()?;
        let generated_at_unix_nanos = crate::metadata_tombstone::metadata_now_unix_nanos();
        if request.identity.primary_node_id != self.node_id {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::NotAuthoritativePrimary {
                    local_node_id: Some(self.node_id.clone()),
                    primary_node_id: request.identity.primary_node_id.clone(),
                },
                generated_at_unix_nanos,
            ));
        }

        if request.after.is_none() {
            // Establish authority and handoff state from the latest cluster-KV
            // view once, before creating the immutable exact-cut snapshot.
            // Continuations are already bound to that snapshot's sequence,
            // fingerprint, timestamp, and cursor. They remain protected by
            // the per-page durable backend freeze check below and by the
            // caller's final topology refresh, so another remote KV round trip
            // per catalog page adds no authority evidence.
            self.refresh_control_plane_latest()?;

            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                    request,
                    DfsContentRootCatalogIncompleteReason::NotAuthoritativePrimary {
                        local_node_id: Some(self.node_id.clone()),
                        primary_node_id: request.identity.primary_node_id.clone(),
                    },
                    generated_at_unix_nanos,
                ));
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            let current = store.get_metadata_shard(
                &request.identity.file_system_id,
                &request.identity.shard_id.0,
            )?;
            let Some(current) = current else {
                return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                    request,
                    DfsContentRootCatalogIncompleteReason::UnknownShard,
                    generated_at_unix_nanos,
                ));
            };
            let current_identity = DfsContentRootCatalogShardIdentity::from_config(&current);
            if current_identity != request.identity {
                return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                    request,
                    DfsContentRootCatalogIncompleteReason::AuthorityChanged {
                        observed: Some(current_identity),
                    },
                    generated_at_unix_nanos,
                ));
            }
            if let Some(handoff) = store.get_metadata_shard_membership_handoff(
                &request.identity.file_system_id,
                &request.identity.shard_id.0,
            )? {
                return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                    request,
                    DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                        handoff_id: Some(handoff.handoff_id),
                    },
                    generated_at_unix_nanos,
                ));
            }
        }

        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
                generated_at_unix_nanos,
            ));
        };
        if backend.metadata_shard_authoritative_writes_frozen(&request.identity.shard_id)? {
            let handoff_id = backend
                .metadata_shard_write_freeze(&request.identity.shard_id)?
                .map(|freeze| freeze.handoff_id);
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen { handoff_id },
                generated_at_unix_nanos,
            ));
        }
        match backend.content_root_catalog_page(request) {
            Ok(page) => {
                page.validate_shape_for_request(request)?;
                Ok(page)
            }
            Err(_) => Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
                generated_at_unix_nanos,
            )),
        }
    }

    pub(crate) fn handle_content_root_catalog_page_request(
        &mut self,
        source_node_id: NodeId,
        request: DfsContentRootCatalogPageRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let page = self.authoritative_content_root_catalog_page(&request)?;
        let event = NetworkedDfsServiceEvent::ContentRootCatalogPageServed {
            requester_node_id: source_node_id.clone(),
            file_system_id: page.identity.file_system_id.clone(),
            shard_id: page.identity.shard_id.clone(),
            entry_count: page.entries.len(),
            complete: page.is_complete(),
        };
        self.send_dfs_message_if_routable(
            &source_node_id,
            DfsNetworkMessage::ContentRootCatalogPageResponse(page),
        )?;
        Ok(event)
    }

    pub(crate) fn handle_content_root_catalog_page_response(
        &mut self,
        source_node_id: NodeId,
        page: DfsContentRootCatalogPage,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if page.identity.primary_node_id != source_node_id {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id,
                reason: "content-root catalog response source does not match its primary authority"
                    .to_string(),
            });
        }
        let complete = page.is_complete();
        Ok(NetworkedDfsServiceEvent::ContentRootCatalogPageReceived {
            primary_node_id: source_node_id,
            file_system_id: page.identity.file_system_id,
            shard_id: page.identity.shard_id,
            entry_count: page.entries.len(),
            complete,
        })
    }

    pub(crate) fn handle_metadata_journal_batch_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataJournalBatchResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if let Some(pending) = self
            .replication
            .pending_metadata_authority_probes
            .get(&response.request_id)
            .cloned()
        {
            let response_identity_matches = pending.target_node_id == source_node_id
                && response.shard_id.0 == pending.expected_shard.shard_id
                && response.next_after_sequence == pending.expected_next_after_sequence
                && !response.verified_prefix_only;
            let accepted = response_identity_matches
                && matches!(response.result, DfsMetadataJournalBatchResult::Applied)
                && response.verified_authority_probe_id.as_deref()
                    == Some(pending.confirmation_id.as_str());
            let rejected = response_identity_matches
                && matches!(
                    response.result,
                    DfsMetadataJournalBatchResult::Rejected { .. }
                )
                && response.verified_authority_probe_id.is_none();
            if !accepted && !rejected {
                // A mismatched response must not consume the request identity:
                // the exact response may still be in flight behind it.
                return Ok(None);
            }
            self.replication
                .pending_metadata_authority_probes
                .remove(&response.request_id);
            self.replication
                .metadata_authority_probe_results
                .insert(response.request_id, accepted);
            return Ok(None);
        }
        let pending = self
            .replication
            .pending_metadata_replications
            .get(&response.request_id)
            .cloned();
        let Some(pending) = pending else {
            if let DfsMetadataJournalBatchResult::Rejected { reason, code } = &response.result
                && DfsMetadataRejectionCode::resolve(*code, reason).allows_snapshot_repair()
                && DfsMetadataRejectionCode::resolve(*code, reason)
                    != DfsMetadataRejectionCode::PrefixMismatch
            {
                self.send_metadata_snapshot_repair(
                    &source_node_id,
                    &response.shard_id,
                    reason.clone(),
                )?;
            }
            return Ok(None);
        };
        if pending.expected_response_kind != PendingMetadataReplicationResponseKind::JournalBatch {
            return Ok(None);
        }
        if pending.target_node_id != source_node_id
            || pending.shard_id != response.shard_id
            || pending.next_after_sequence != response.next_after_sequence
        {
            // Keep the request live. A response for the same request id but a
            // different source or journal boundary is not its terminal result.
            return Ok(None);
        }
        let result_identity_matches = match &response.result {
            DfsMetadataJournalBatchResult::Applied => {
                response.verified_prefix_only == pending.verify_prefix_only
                    && response.verified_authority_probe_id.as_deref()
                        == pending.required_authority_confirmation_id.as_deref()
            }
            DfsMetadataJournalBatchResult::Rejected { .. } => {
                !response.verified_prefix_only && response.verified_authority_probe_id.is_none()
            }
        };
        if !result_identity_matches {
            // A successful acknowledgement must carry the complete expected
            // proof identity, while a rejection must not impersonate one.
            // Preserve the pending request on mismatch so a delayed valid
            // response can still settle it.
            return Ok(None);
        }

        if matches!(&response.result, DfsMetadataJournalBatchResult::Applied)
            && !pending.verify_prefix_only
        {
            self.advance_metadata_replication_cursor(
                &source_node_id,
                &response.shard_id,
                response.next_after_sequence,
                pending.next_after_sequence,
            )?;
        }
        self.replication
            .pending_metadata_replications
            .remove(&response.request_id);
        Ok(match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    applied_entries: response.applied_entries,
                    next_after_sequence: response.next_after_sequence,
                    verified_prefix_only: response.verified_prefix_only,
                })
            }
            DfsMetadataJournalBatchResult::Rejected { reason, code } => {
                if DfsMetadataRejectionCode::resolve(code, &reason).allows_snapshot_repair()
                    && !pending.verify_prefix_only
                {
                    self.send_metadata_snapshot_repair(
                        &source_node_id,
                        &response.shard_id,
                        reason.clone(),
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    fn advance_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
        pending_next_after_sequence: u64,
    ) -> Result<u64, DfsError> {
        let cursor_key = (target_node_id.clone(), shard_id.clone());
        let cursor = self
            .replication
            .metadata_replication_cursors
            .get(&cursor_key)
            .copied()
            .unwrap_or(0)
            .max(next_after_sequence)
            .max(pending_next_after_sequence);
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_metadata_replication_cursor(target_node_id, shard_id, cursor)?;
        }
        self.replication
            .metadata_replication_cursors
            .insert(cursor_key, cursor);
        Ok(cursor)
    }

    /// A replica whose journal forked from this primary's stream (for
    /// example a locally bootstrapped node joining as a metadata mirror) cannot
    /// apply batches; repair it by installing a compact table snapshot, the
    /// same reconciliation the standalone publisher performs.
    fn send_metadata_snapshot_repair(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        reason: String,
    ) -> Result<(), DfsError> {
        let shard = match self.metadata_shard_config_for_shard_id(shard_id)? {
            Some(shard) if shard.primary_node_id == self.node_id => shard,
            _ => return Ok(()),
        };
        let next_after_sequence = self
            .metadata_journal_last_sequence_for_shard(shard_id)?
            .unwrap_or(0);
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(snapshot) = backend.export_shard_table_snapshot(shard_id)? else {
            return Ok(());
        };
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: format!(
                "dfs_meta_snapshot_{}_{}_{}",
                self.node_id, target_node_id, next_after_sequence
            ),
            source_node_id: self.node_id.clone(),
            file_system_id: Some(shard.file_system_id),
            authority_epoch: Some(shard.epoch),
            primary_handoff_id: None,
            shard_id: shard_id.clone(),
            next_after_sequence,
            snapshot,
            reason,
        };
        let request_id = command.request_id.clone();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::MetadataSnapshotInstall(command),
        )?;
        if sent {
            self.replication.pending_metadata_replications.insert(
                request_id,
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    expected_response_kind: PendingMetadataReplicationResponseKind::SnapshotInstall,
                    verify_prefix_only: false,
                    required_authority_confirmation_id: None,
                    created_at: std::time::Instant::now(),
                },
            );
        }
        Ok(())
    }

    /// Resolves a shard configuration by shard id across the filesystems this
    /// service knows about.
    fn metadata_shard_config_for_shard_id(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let file_system_ids: Vec<DfsFileSystemId> =
            self.metadata.filesystems.keys().cloned().collect();
        for file_system_id in file_system_ids {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked above");
            if let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&file_system_id, &shard_id.0)
                .map_err(DfsError::from)?
            {
                return Ok(Some(shard));
            }
        }
        Ok(None)
    }

    /// Replica side of in-service snapshot repair: validates that the sender
    /// is the shard's configured primary and that this node is a configured
    /// replica, replaces the local shard state with the snapshot, and
    /// rehydrates in-memory service state from the replaced backend.
    pub(crate) fn handle_metadata_snapshot_install(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataSnapshotInstallCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let shard_id = command.shard_id.clone();
        let next_after_sequence = command.next_after_sequence;
        // Snapshot authority is checked against the exact committed shard
        // assignment below. Refresh even for ordinary replica repair: a
        // receiver may have remained online with an older cached KV snapshot
        // while the shard epoch advanced without changing its membership.
        self.refresh_control_plane_latest()?;
        let authority_shard = if command.source_node_id != source_node_id {
            Err(DfsMetadataRejection::new(
                DfsMetadataRejectionCode::Other,
                "metadata snapshot source does not match transport source".to_string(),
            ))
        } else if self.control_plane.is_none() {
            Err(DfsMetadataRejection::new(
                DfsMetadataRejectionCode::Other,
                "metadata snapshot ingest requires a committed control-plane shard view"
                    .to_string(),
            ))
        } else if let (Some(file_system_id), Some(_)) =
            (command.file_system_id.as_ref(), command.authority_epoch)
        {
            let control_plane = self
                .control_plane
                .as_mut()
                .expect("snapshot control plane checked above");
            match DfsControlPlaneStore::new(control_plane.as_mut())
                .get_metadata_shard(file_system_id, &shard_id.0)?
            {
                None => Err(DfsMetadataRejection::new(
                    DfsMetadataRejectionCode::Other,
                    format!(
                        "metadata snapshot install has no configured shard {}",
                        shard_id.0
                    ),
                )),
                Some(shard)
                    if command.file_system_id.as_ref() != Some(&shard.file_system_id)
                        || command.authority_epoch != Some(shard.epoch) =>
                {
                    Err(DfsMetadataRejection::new(
                        DfsMetadataRejectionCode::Other,
                        format!(
                            "metadata snapshot authority identity does not match filesystem {} shard {} epoch {}",
                            shard.file_system_id, shard.shard_id, shard.epoch
                        ),
                    ))
                }
                Some(shard) if shard.primary_node_id != source_node_id => {
                    Err(DfsMetadataRejection::new(
                        DfsMetadataRejectionCode::PrimaryMismatch,
                        format!(
                            "metadata snapshot source {source_node_id} is not shard {} primary {}",
                            shard.shard_id, shard.primary_node_id
                        ),
                    ))
                }
                Some(shard) if shard.primary_node_id == self.node_id => {
                    Err(DfsMetadataRejection::new(
                        DfsMetadataRejectionCode::Other,
                        "metadata primary must not install a replica snapshot".to_string(),
                    ))
                }
                // Non-replica metadata mirrors accept primary-sourced snapshot
                // repair too; their journals are fed by routed-response and
                // SyncPath batches and can need the same divergence repair as
                // configured replicas.
                Some(shard) => Ok(shard),
            }
        } else {
            Err(DfsMetadataRejection::new(DfsMetadataRejectionCode::Other, "metadata snapshot install is missing exact filesystem and authority epoch identity"
                    .to_string()))
        };
        let (permit, rejection) = match authority_shard {
            Ok(shard) => {
                let pending_membership_handoff = if command.primary_handoff_id.is_some() {
                    let control_plane = self
                        .control_plane
                        .as_mut()
                        .expect("snapshot control plane checked above");
                    DfsControlPlaneStore::new(control_plane.as_mut())
                        .get_metadata_shard_membership_handoff(
                            &shard.file_system_id,
                            &shard.shard_id,
                        )?
                } else {
                    None
                };
                let context = crate::metadata_replication::DfsMetadataReplicaShardContext {
                    local_node_id: self.node_id.clone(),
                    shard: shard.clone(),
                    pending_membership_handoff,
                };
                match crate::metadata_replication::validated_pending_primary_handoff_snapshot_id(
                    &context, &command,
                ) {
                    Ok(Some(handoff_id)) => (
                        Some(
                            DfsMetadataReplicaIngestPermit::new_validated_pending_primary_handoff(
                                shard.file_system_id.clone(),
                                shard_id.clone(),
                                source_node_id.clone(),
                                shard.epoch,
                                handoff_id,
                            ),
                        ),
                        None,
                    ),
                    Ok(None) => (
                        Some(DfsMetadataReplicaIngestPermit::new_validated(
                            shard.file_system_id.clone(),
                            shard_id.clone(),
                            source_node_id.clone(),
                            shard.epoch,
                        )),
                        None,
                    ),
                    Err(reason) => (None, Some(reason.into())),
                }
            }
            Err(reason) => (None, Some(reason)),
        };
        let rejection = if rejection.is_none() {
            self.validate_metadata_snapshot_against_local_state(&command)?
                .map(DfsMetadataRejection::from)
        } else {
            rejection
        };
        let result = if let Some(reason) = rejection {
            DfsMetadataSnapshotInstallResult::from_rejection(reason)
        } else if let Some(backend) = self.metadata_backend.as_mut() {
            match backend.replace_with_replicated_snapshot_with_replica_permit(
                permit
                    .as_ref()
                    .expect("validated metadata snapshot ingest has a permit"),
                shard_id.clone(),
                next_after_sequence,
                command.snapshot,
            ) {
                Ok(archived_existing) => {
                    // Replacing the backend is the durable replica-progress
                    // boundary. Retain that edge before rehydration or ACK
                    // delivery, either of which may still fail after the
                    // snapshot has committed.
                    self.record_message_committed_work(DfsMessageCommittedWork {
                        metadata_replica_progress: true,
                        ..DfsMessageCommittedWork::default()
                    });
                    // The backend was replaced wholesale; in-memory state
                    // must follow it. Exact resident replica lanes deliberately
                    // retain only filesystem, cursor, and namespace-job guard
                    // state. Owned/global services may coordinate foreground
                    // operations and therefore keep their complete projection.
                    if self
                        .metadata_backend
                        .as_ref()
                        .and_then(|backend| backend.resident_exact_shard_id())
                        .is_some()
                    {
                        self.hydrate_lazy_exact_replica_after_snapshot_install()?;
                    } else {
                        self.hydrate_from_metadata_backend()?;
                    }
                    DfsMetadataSnapshotInstallResult::Installed { archived_existing }
                }
                Err(error) => DfsMetadataSnapshotInstallResult::from_rejection(error),
            }
        } else {
            DfsMetadataSnapshotInstallResult::Rejected {
                reason: "local DFS metadata backend is not configured".to_string(),
                code: Some(DfsMetadataRejectionCode::Other),
            }
        };
        let installed = matches!(result, DfsMetadataSnapshotInstallResult::Installed { .. });
        let reason = match &result {
            DfsMetadataSnapshotInstallResult::Rejected { reason, .. } => Some(reason.clone()),
            DfsMetadataSnapshotInstallResult::Installed { .. } => None,
        };
        let _ = self.send_dfs_message_if_routable(
            &source_node_id,
            DfsNetworkMessage::MetadataSnapshotInstallResponse(
                DfsMetadataSnapshotInstallResponse {
                    request_id: request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    result,
                },
            ),
        )?;
        Ok(if installed {
            NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                request_id,
                source_node_id,
                shard_id,
                next_after_sequence,
            }
        } else {
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id,
                source_node_id,
                shard_id,
                reason: reason.unwrap_or_else(|| "snapshot install rejected".to_string()),
            }
        })
    }

    fn validate_metadata_snapshot_against_local_state(
        &mut self,
        command: &DfsMetadataSnapshotInstallCommand,
    ) -> Result<Option<String>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_deref()
            && let Some(reason) =
                crate::metadata_replication::validate_dfs_metadata_snapshot_install_state(
                    backend, command,
                )?
        {
            return Ok(Some(reason));
        }
        Ok(None)
    }

    /// Primary side of in-service snapshot repair: a successful install
    /// advances the replica's replication cursor to the snapshot head.
    pub(crate) fn handle_metadata_snapshot_install_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataSnapshotInstallResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self
            .replication
            .pending_metadata_replications
            .get(&response.request_id)
            .cloned()
        else {
            return Ok(None);
        };
        if pending.expected_response_kind != PendingMetadataReplicationResponseKind::SnapshotInstall
        {
            return Ok(None);
        }
        if pending.target_node_id != source_node_id
            || pending.shard_id != response.shard_id
            || pending.next_after_sequence != response.next_after_sequence
        {
            return Ok(None);
        }
        let installed_cursor = if matches!(
            &response.result,
            DfsMetadataSnapshotInstallResult::Installed { .. }
        ) {
            Some(self.advance_metadata_replication_cursor(
                &source_node_id,
                &response.shard_id,
                response.next_after_sequence,
                pending.next_after_sequence,
            )?)
        } else {
            None
        };
        self.replication
            .pending_metadata_replications
            .remove(&response.request_id);
        Ok(match response.result {
            DfsMetadataSnapshotInstallResult::Installed { .. } => {
                Some(NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    next_after_sequence: installed_cursor
                        .expect("installed snapshot advanced its replication cursor"),
                })
            }
            DfsMetadataSnapshotInstallResult::Rejected { reason, .. } => {
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    pub(crate) fn metadata_journal_batch_since(
        &self,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_since_limited(
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    pub(crate) fn metadata_journal_batch_for_shard_since(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        self.metadata_journal_batch_for_shard_since_limited(
            shard_id,
            after_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
    }

    pub(crate) fn metadata_journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_for_shard_since_limited(
                shard_id,
                after_sequence,
                max_entries,
                max_encoded_bytes,
            )
            .map_err(Into::into)
    }

    pub(crate) fn metadata_journal_last_sequence_for_shard(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<u64>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(backend
            .metadata_shard_statuses()?
            .into_iter()
            .find(|status| &status.shard_id == shard_id)
            .map(|status| status.journal_last_sequence))
    }

    pub(crate) fn handle_metadata_journal_batch(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataJournalBatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let shard_id = command.batch.shard_id.clone();
        let next_after_sequence = command.batch.next_after_sequence;
        if let Err(reason) =
            crate::metadata_replication::validate_durable_metadata_journal_frames(&command)
        {
            self.send_metadata_journal_batch_rejection(
                &source_node_id,
                &command.request_id,
                &shard_id,
                next_after_sequence,
                &reason,
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason: reason.to_string(),
            });
        }
        let validated_source =
            self.validate_metadata_journal_batch_source(&source_node_id, &command)?;
        let validated_source = match validated_source {
            Ok(validated_source) => validated_source,
            Err(reason) => {
                self.send_metadata_journal_batch_rejection(
                    &source_node_id,
                    &command.request_id,
                    &shard_id,
                    next_after_sequence,
                    &reason,
                )?;
                return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    reason: reason.to_string(),
                });
            }
        };

        if self.metadata_backend.is_none() {
            let reason = "local DFS metadata backend is not configured".to_string();
            self.send_metadata_journal_batch_rejection(
                &source_node_id,
                &command.request_id,
                &shard_id,
                next_after_sequence,
                &reason,
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason: reason.to_string(),
            });
        }

        if command.verify_prefix_only {
            let verified_primary_handoff_id = match self.verify_metadata_journal_prefix_probe(
                &source_node_id,
                &command,
                &validated_source.replica_context,
            )? {
                Ok(verified_primary_handoff_id) => verified_primary_handoff_id,
                Err(reason) => {
                    self.send_metadata_journal_batch_rejection(
                        &source_node_id,
                        &command.request_id,
                        &shard_id,
                        next_after_sequence,
                        &reason,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                        request_id: command.request_id,
                        source_node_id,
                        shard_id,
                        reason: reason.to_string(),
                    });
                }
            };
            let _ = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    applied_entries: 0,
                    verified_prefix_only: true,
                    // Reuse the rolling-upgrade-safe authority confirmation
                    // slot. An older peer cannot echo this identity and its
                    // otherwise valid prefix ACK will not count for promotion.
                    verified_authority_probe_id: verified_primary_handoff_id,
                    result: DfsMetadataJournalBatchResult::Applied,
                }),
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                applied_entries: 0,
                next_after_sequence,
                verified_prefix_only: true,
            });
        }

        let apply_result = {
            let permit = &validated_source.permit;
            let backend = self
                .metadata_backend
                .as_mut()
                .expect("metadata backend checked above");
            if let Some(replication) = command.durable_replication.as_ref() {
                backend.persist_journal_frames_with_replica_permit_report(
                    permit,
                    &command.batch,
                    &replication.journal_frames,
                )
            } else if command
                .authority_probe
                .as_ref()
                .is_some_and(|probe| probe.durable_journal_ack)
            {
                let probe = command
                    .authority_probe
                    .as_ref()
                    .expect("durable authority probe checked above");
                backend.persist_authority_journal_frames_with_replica_permit_report(
                    permit,
                    &command.batch,
                    &probe.journal_frames,
                    probe
                        .prepared_prefix
                        .as_ref()
                        .expect("durable authority prefix validated above"),
                )
            } else if command.authority_probe.is_some() {
                backend.apply_journal_batch_with_replica_permit_report(permit, &command.batch)
            } else {
                backend
                    .apply_journal_batch_with_replica_permit(permit, &command.batch)
                    .map(DfsMetadataReplicaApplyReport::without_authority_proofs)
            }
        };

        match apply_result {
            Ok(apply_report) => {
                let applied_entries = apply_report.applied_entries();
                // A successful backend apply is the durable replica-progress
                // boundary. Record it before authority-proof persistence,
                // service-state hydration, or ACK delivery so a later error
                // cannot discard the committed edge.
                self.record_message_committed_work(DfsMessageCommittedWork {
                    metadata_replica_progress: true,
                    metadata_materialization: apply_report.materialization_pending(),
                    ..DfsMessageCommittedWork::default()
                });
                let verified_durable_replication_id = {
                    let backend = self
                        .metadata_backend
                        .as_mut()
                        .expect("metadata backend checked above");
                    match crate::metadata_replication::persist_durable_metadata_replication_ack(
                        backend.as_mut(),
                        &command,
                        &apply_report,
                    ) {
                        Ok(confirmation_id) => confirmation_id,
                        Err(reason) => {
                            self.send_metadata_journal_batch_rejection(
                                &source_node_id,
                                &command.request_id,
                                &shard_id,
                                next_after_sequence,
                                &reason,
                            )?;
                            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                                request_id: command.request_id,
                                source_node_id,
                                shard_id,
                                reason: reason.to_string(),
                            });
                        }
                    }
                };
                let verified_authority_probe_id = if verified_durable_replication_id.is_some() {
                    verified_durable_replication_id
                } else if let Some(probe) = command.authority_probe.as_ref() {
                    let backend = self
                        .metadata_backend
                        .as_mut()
                        .expect("metadata backend checked above");
                    let verification =
                        crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
                            backend.as_ref(),
                            &probe.expected_shard,
                            probe,
                            &apply_report,
                        )
                        .and_then(|confirmation_id| {
                            backend
                                .put_metadata_authority_ack_floor(
                                    &crate::DfsMetadataAuthorityAckFloor {
                                        shard_id: shard_id.clone(),
                                        journal_sequence: next_after_sequence,
                                        durable_prefix_proof: apply_report
                                            .durable_prefix_proof()
                                            .cloned(),
                                    },
                                )
                                .map_err(DfsError::from)
                                .map(|_| confirmation_id)
                        });
                    match verification {
                        Ok(confirmation_id) => Some(confirmation_id),
                        Err(error) => {
                            let reason = error.to_string();
                            self.send_metadata_journal_batch_rejection(
                                &source_node_id,
                                &command.request_id,
                                &shard_id,
                                next_after_sequence,
                                error,
                            )?;
                            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                                request_id: command.request_id,
                                source_node_id,
                                shard_id,
                                reason: reason.to_string(),
                            });
                        }
                    }
                } else {
                    None
                };
                self.observe_replicated_metadata_journal_batch(&command.batch)?;
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries,
                            verified_prefix_only: false,
                            verified_authority_probe_id,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    applied_entries,
                    next_after_sequence,
                    verified_prefix_only: false,
                })
            }
            Err(error) => {
                let reason = error.to_string();
                self.send_metadata_journal_batch_rejection(
                    &source_node_id,
                    &command.request_id,
                    &shard_id,
                    next_after_sequence,
                    error,
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    reason: reason.to_string(),
                })
            }
        }
    }

    fn verify_metadata_journal_prefix_probe(
        &mut self,
        source_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        context: &DfsMetadataReplicaShardContext,
    ) -> Result<std::result::Result<Option<String>, DfsMetadataRejection>, DfsError> {
        let backend = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata prefix proof requires a durable metadata backend".to_string(),
            })?;
        let verification_started = Instant::now();
        let verification = crate::metadata_replication::verify_dfs_metadata_journal_prefix(
            backend.as_mut(),
            context,
            source_node_id,
            command,
        );
        crate::dfs_perf_trace(format_args!(
            "metadata-prefix-verification node={} source={} shard={} sequence={} primary_handoff={} accepted={} verify_ms={}",
            self.node_id,
            source_node_id,
            command.batch.shard_id.0,
            command.batch.next_after_sequence,
            command
                .prefix_probe
                .as_ref()
                .and_then(|probe| probe.primary_handoff_id.as_ref())
                .is_some(),
            verification.is_ok(),
            verification_started.elapsed().as_millis(),
        ));
        if verification.is_ok()
            || command
                .prefix_probe
                .as_ref()
                .and_then(|probe| probe.primary_handoff_id.as_ref())
                .is_some()
        {
            // A handoff verifier persists the freeze marker before the prefix
            // floor. Conservatively retain the committed-work edge even when
            // the later floor write rejects; a spurious maintenance wake for
            // an invalid handoff probe is harmless and bounded by the request.
            self.record_message_committed_work(DfsMessageCommittedWork {
                metadata_replica_progress: true,
                ..DfsMessageCommittedWork::default()
            });
        }
        Ok(verification)
    }

    fn send_metadata_journal_batch_rejection(
        &mut self,
        source_node_id: &NodeId,
        request_id: &str,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
        reason: impl Into<DfsMetadataRejection>,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                request_id: request_id.to_string(),
                shard_id: shard_id.clone(),
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: false,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::from_rejection(reason),
            }),
        )?;
        Ok(())
    }

    fn validate_metadata_journal_batch_source(
        &mut self,
        source_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
    ) -> Result<Result<ValidatedMetadataJournalBatchSource, DfsMetadataRejection>, DfsError> {
        let batch = &command.batch;
        if command.source_node_id.as_ref() != Some(source_node_id) {
            return Ok(Err(
                ("metadata journal batch source does not match transport source".to_string())
                    .into(),
            ));
        }
        // Do not reject a current primary from a stale cached KV snapshot.
        // The permit minted below is an authority boundary, so it must use a
        // freshly committed control-plane view on every ingest attempt.
        let validation_started = Instant::now();
        let refresh_started = Instant::now();
        self.refresh_control_plane_latest()?;
        let refresh_ms = refresh_started.elapsed().as_millis();
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Err(
                ("metadata journal ingest requires a committed control-plane shard view"
                    .to_string())
                .into(),
            ));
        };
        let ordinary_applied_batch =
            !command.verify_prefix_only && command.authority_probe.is_none();
        if ordinary_applied_batch
            && (command.file_system_id.is_none() || command.authority_epoch.is_none())
        {
            return Ok(Err(
                ("metadata journal batch is missing exact filesystem and authority epoch identity"
                    .to_string())
                .into(),
            ));
        }
        let file_system_id = metadata_journal_batch_file_system_id(batch)
            .or_else(|| {
                command
                    .prefix_probe
                    .as_ref()
                    .and_then(|probe| probe.file_system_id.clone())
            })
            .or_else(|| {
                command
                    .authority_probe
                    .as_ref()
                    .map(|probe| probe.expected_shard.file_system_id.clone())
            })
            .or_else(|| command.file_system_id.clone());
        let Some(file_system_id) = file_system_id else {
            return Ok(Err(
                ("metadata journal batch is missing an exact filesystem identity".to_string())
                    .into(),
            ));
        };
        let lookup_started = Instant::now();
        let store = DfsControlPlaneStore::new(control_plane.as_mut());
        let Some(shard) = store.get_metadata_shard(&file_system_id, &batch.shard_id.0)? else {
            return Ok(Err((format!(
                "metadata journal batch has no configured shard {}",
                batch.shard_id.0
            ))
            .into()));
        };
        let pending_membership_handoff = if command
            .prefix_probe
            .as_ref()
            .and_then(|probe| probe.primary_handoff_id.as_ref())
            .is_some()
        {
            store.get_metadata_shard_membership_handoff(&file_system_id, &batch.shard_id.0)?
        } else {
            None
        };
        let lookup_ms = lookup_started.elapsed().as_millis();
        crate::dfs_perf_trace(format_args!(
            "metadata-journal-source-validation node={} source={} filesystem={} shard={} sequence={} handoff_lookup={} refresh_ms={} lookup_ms={} total_ms={}",
            self.node_id,
            source_node_id,
            file_system_id,
            batch.shard_id.0,
            batch.next_after_sequence,
            command
                .prefix_probe
                .as_ref()
                .and_then(|probe| probe.primary_handoff_id.as_ref())
                .is_some(),
            refresh_ms,
            lookup_ms,
            validation_started.elapsed().as_millis(),
        ));
        if ordinary_applied_batch
            && (command.file_system_id.as_ref() != Some(&shard.file_system_id)
                || command.authority_epoch != Some(shard.epoch))
        {
            return Ok(Err((format!(
                "metadata journal authority identity does not match filesystem {} shard {} epoch {}",
                shard.file_system_id, shard.shard_id, shard.epoch
            )).into()));
        }
        if shard.primary_node_id != *source_node_id {
            return Ok(Err(DfsMetadataRejection::new(
                DfsMetadataRejectionCode::PrimaryMismatch,
                format!(
                    "metadata journal source {source_node_id} is not shard {} primary {}",
                    shard.shard_id, shard.primary_node_id
                ),
            )));
        }
        if let Some(probe) = command.authority_probe.as_ref() {
            let has_authority_payload = !batch.entries.is_empty()
                || (probe.durable_journal_ack && !probe.journal_frames.is_empty());
            if command.verify_prefix_only
                || command.prefix_probe.is_some()
                || !has_authority_payload
                || probe.probe_id.is_empty()
                || probe.expected_shard != shard
                || !crate::service_replica::metadata_authority_operation_is_well_formed_for_shard(
                    &probe.operation,
                    &shard,
                )
                || crate::service_replica::metadata_authority_operation_prepare_sequence(
                    &probe.operation,
                ) != batch.next_after_sequence
            {
                return Ok(Err((format!(
                    "metadata authority probe does not match the exact committed assignment for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                )).into()));
            }
            if !shard.replica_node_ids.contains(&self.node_id) {
                return Ok(Err((format!(
                    "local node {} is not a configured replica for metadata shard {}",
                    self.node_id, shard.shard_id
                ))
                .into()));
            }
        }
        if command.verify_prefix_only
            && command
                .prefix_probe
                .as_ref()
                .is_none_or(|probe| probe.authority_epoch != Some(shard.epoch))
        {
            return Ok(Err((format!(
                "metadata journal prefix probe epoch does not match shard {} epoch {}",
                shard.shard_id, shard.epoch
            ))
            .into()));
        }
        if shard.primary_node_id == self.node_id {
            return Ok(Err(
                ("metadata primary must not apply a replica journal batch".to_string()).into(),
            ));
        }
        // Routed operations still accept primary-sourced journal catch-up
        // batches so requesters keep read-your-write even for custom shards
        // where they are not durable replicas.
        Ok(Ok(ValidatedMetadataJournalBatchSource {
            permit: DfsMetadataReplicaIngestPermit::new_validated(
                shard.file_system_id.clone(),
                batch.shard_id.clone(),
                source_node_id.clone(),
                shard.epoch,
            ),
            replica_context: DfsMetadataReplicaShardContext {
                local_node_id: self.node_id.clone(),
                shard,
                pending_membership_handoff,
            },
        }))
    }
}

#[cfg(test)]
mod response_bookkeeping_tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::network::InProcessNetwork;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("test node id")
    }

    fn pending_replication(
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
        expected_response_kind: PendingMetadataReplicationResponseKind,
        verify_prefix_only: bool,
        required_authority_confirmation_id: Option<&str>,
    ) -> PendingMetadataReplication {
        PendingMetadataReplication {
            target_node_id,
            shard_id,
            next_after_sequence,
            expected_response_kind,
            verify_prefix_only,
            required_authority_confirmation_id: required_authority_confirmation_id
                .map(str::to_string),
            created_at: Instant::now(),
        }
    }

    fn applied_journal_response(
        request_id: &str,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
        verified_prefix_only: bool,
        confirmation_id: Option<&str>,
    ) -> DfsMetadataJournalBatchResponse {
        DfsMetadataJournalBatchResponse {
            request_id: request_id.to_string(),
            shard_id,
            next_after_sequence,
            applied_entries: 1,
            verified_prefix_only,
            verified_authority_probe_id: confirmation_id.map(str::to_string),
            result: DfsMetadataJournalBatchResult::Applied,
        }
    }

    fn installed_snapshot_response(
        request_id: &str,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> DfsMetadataSnapshotInstallResponse {
        DfsMetadataSnapshotInstallResponse {
            request_id: request_id.to_string(),
            shard_id,
            next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed {
                archived_existing: false,
            },
        }
    }

    #[test]
    fn journal_response_mismatch_does_not_consume_pending_ack() {
        let root = sinomo_testdir::TestDir::new("dfs-journal-response-bookkeeping");
        let local = node("node_journal_response_local");
        let target = node("node_journal_response_target");
        let wrong_source = node("node_journal_response_wrong_source");
        let shard_id = DfsMetadataShardId::new("root");
        let request_id = "journal-response-request";
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(local),
            LocalContentStore::new(root.join("content")).expect("content store"),
        );
        service.replication.pending_metadata_replications.insert(
            request_id.to_string(),
            pending_replication(
                target.clone(),
                shard_id.clone(),
                17,
                PendingMetadataReplicationResponseKind::JournalBatch,
                false,
                Some("durable-confirmation"),
            ),
        );

        let exact = applied_journal_response(
            request_id,
            shard_id.clone(),
            17,
            false,
            Some("durable-confirmation"),
        );
        assert_eq!(
            service
                .handle_metadata_snapshot_install_response(
                    target.clone(),
                    installed_snapshot_response(request_id, shard_id.clone(), 17),
                )
                .expect("ignore snapshot response for journal request"),
            None
        );
        assert!(
            service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(wrong_source, exact.clone())
                .expect("ignore wrong source"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.shard_id = DfsMetadataShardId::new("other");
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong shard"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.next_after_sequence += 1;
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong sequence"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.verified_prefix_only = true;
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong proof kind"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.verified_authority_probe_id = Some("wrong-confirmation".to_string());
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong confirmation"),
            None
        );
        assert!(
            service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );

        let event = service
            .handle_metadata_journal_batch_response(target.clone(), exact)
            .expect("accept exact response")
            .expect("exact response event");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                next_after_sequence: 17,
                ..
            }
        ));
        assert!(
            !service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );
        assert_eq!(
            service
                .replication
                .metadata_replication_cursors
                .get(&(target, shard_id)),
            Some(&17)
        );
    }

    #[test]
    fn authority_response_mismatch_does_not_consume_pending_probe() {
        let root = sinomo_testdir::TestDir::new("dfs-authority-response-bookkeeping");
        let local = node("node_authority_response_local");
        let target = node("node_authority_response_target");
        let wrong_source = node("node_authority_response_wrong_source");
        let shard_id = DfsMetadataShardId::new("root");
        let request_id = "authority-response-request";
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(local.clone()),
            LocalContentStore::new(root.join("content")).expect("content store"),
        );
        service
            .replication
            .pending_metadata_authority_probes
            .insert(
                request_id.to_string(),
                PendingMetadataAuthorityProbe {
                    target_node_id: target.clone(),
                    confirmation_id: "authority-confirmation".to_string(),
                    expected_shard: DfsMetadataShardConfig {
                        file_system_id: sifs_file_system_id(),
                        shard_id: shard_id.0.clone(),
                        primary_node_id: local,
                        replica_node_ids: vec![target.clone()],
                        epoch: 4,
                        assignment: DfsMetadataShardAssignment::Root,
                        namespace_prepare_codec: Default::default(),
                        default_policy: DfsObjectPolicy::ReplicatedEventual,
                    },
                    expected_next_after_sequence: 23,
                },
            );
        let exact = applied_journal_response(
            request_id,
            shard_id.clone(),
            23,
            false,
            Some("authority-confirmation"),
        );

        assert_eq!(
            service
                .handle_metadata_journal_batch_response(wrong_source, exact.clone())
                .expect("ignore wrong source"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.shard_id = DfsMetadataShardId::new("other");
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong shard"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.next_after_sequence += 1;
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong sequence"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.verified_prefix_only = true;
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong proof kind"),
            None
        );
        let mut mismatch = exact.clone();
        mismatch.verified_authority_probe_id = Some("wrong-confirmation".to_string());
        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target.clone(), mismatch)
                .expect("ignore wrong confirmation"),
            None
        );
        assert!(
            service
                .replication
                .pending_metadata_authority_probes
                .contains_key(request_id)
        );
        assert!(
            !service
                .replication
                .metadata_authority_probe_results
                .contains_key(request_id)
        );

        assert_eq!(
            service
                .handle_metadata_journal_batch_response(target, exact)
                .expect("accept exact authority response"),
            None
        );
        assert!(
            !service
                .replication
                .pending_metadata_authority_probes
                .contains_key(request_id)
        );
        assert_eq!(
            service
                .replication
                .metadata_authority_probe_results
                .get(request_id),
            Some(&true)
        );
    }

    #[test]
    fn snapshot_response_requires_the_exact_pending_sequence() {
        let root = sinomo_testdir::TestDir::new("dfs-snapshot-response-bookkeeping");
        let local = node("node_snapshot_response_local");
        let target = node("node_snapshot_response_target");
        let shard_id = DfsMetadataShardId::new("root");
        let request_id = "snapshot-response-request";
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(local),
            LocalContentStore::new(root.join("content")).expect("content store"),
        );
        service.replication.pending_metadata_replications.insert(
            request_id.to_string(),
            pending_replication(
                target.clone(),
                shard_id.clone(),
                31,
                PendingMetadataReplicationResponseKind::SnapshotInstall,
                false,
                None,
            ),
        );

        assert_eq!(
            service
                .handle_metadata_journal_batch_response(
                    target.clone(),
                    applied_journal_response(request_id, shard_id.clone(), 31, false, None),
                )
                .expect("ignore journal response for snapshot request"),
            None
        );
        assert!(
            service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );

        assert_eq!(
            service
                .handle_metadata_snapshot_install_response(
                    target.clone(),
                    installed_snapshot_response(request_id, shard_id.clone(), 32),
                )
                .expect("ignore wrong snapshot sequence"),
            None
        );
        assert!(
            service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );
        assert!(
            !service
                .replication
                .metadata_replication_cursors
                .contains_key(&(target.clone(), shard_id.clone()))
        );

        let event = service
            .handle_metadata_snapshot_install_response(
                target.clone(),
                installed_snapshot_response(request_id, shard_id.clone(), 31),
            )
            .expect("accept exact snapshot response")
            .expect("snapshot response event");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                next_after_sequence: 31,
                ..
            }
        ));
        assert!(
            !service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );
        assert_eq!(
            service
                .replication
                .metadata_replication_cursors
                .get(&(target, shard_id)),
            Some(&31)
        );
    }

    #[test]
    fn cursor_persistence_failure_keeps_memory_and_pending_response_behind() {
        let root = sinomo_testdir::TestDir::new("dfs-cursor-response-durability");
        let metadata_root = root.join("metadata");
        let local = node("node_cursor_response_local");
        let target = node("node_cursor_response_target");
        let shard_id = DfsMetadataShardId::new("root");
        let request_id = "cursor-response-request";
        let network = InProcessNetwork::new();
        let backend = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            network.add_node(local),
            LocalContentStore::new(root.join("content")).expect("content store"),
        )
        .with_metadata_backend(backend)
        .expect("attach metadata backend");
        service.replication.pending_metadata_replications.insert(
            request_id.to_string(),
            pending_replication(
                target.clone(),
                shard_id.clone(),
                41,
                PendingMetadataReplicationResponseKind::JournalBatch,
                false,
                None,
            ),
        );
        let cursor_path = crate::metadata_cursor::metadata_replication_cursor_path(
            &metadata_root,
            &target,
            &shard_id,
        );
        std::fs::create_dir_all(&cursor_path).expect("block cursor file with directory");
        let response = applied_journal_response(request_id, shard_id.clone(), 41, false, None);

        service
            .handle_metadata_journal_batch_response(target.clone(), response.clone())
            .expect_err("cursor persistence must fail");
        assert!(
            service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );
        assert!(
            !service
                .replication
                .metadata_replication_cursors
                .contains_key(&(target.clone(), shard_id.clone()))
        );

        std::fs::remove_dir_all(&cursor_path).expect("unblock cursor file");
        service
            .handle_metadata_journal_batch_response(target.clone(), response)
            .expect("retry exact acknowledgement")
            .expect("retry event");
        assert!(
            !service
                .replication
                .pending_metadata_replications
                .contains_key(request_id)
        );
        assert_eq!(
            service
                .replication
                .metadata_replication_cursors
                .get(&(target.clone(), shard_id.clone())),
            Some(&41)
        );
        assert_eq!(
            service
                .metadata_backend
                .as_ref()
                .expect("metadata backend")
                .metadata_replication_cursor(&target, &shard_id)
                .expect("durable cursor"),
            41
        );
    }

    #[test]
    fn malformed_durable_frame_carrier_returns_a_typed_rejection() {
        let root = sinomo_testdir::TestDir::new("dfs-malformed-frame-rejection");
        let local = node("node_malformed_frame_local");
        let source = node("node_malformed_frame_source");
        let shard_id = DfsMetadataShardId::new("root");
        let request_id = "malformed-frame-request";
        let network = InProcessNetwork::new();
        let mut source_transport = network.add_node(source.clone());
        let mut service = NetworkedDfsService::new(
            network.add_node(local),
            LocalContentStore::new(root.join("content")).expect("content store"),
        );
        let command = DfsMetadataJournalBatchCommand {
            request_id: request_id.to_string(),
            source_node_id: Some(source.clone()),
            file_system_id: Some(sifs_file_system_id()),
            authority_epoch: Some(1),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: Some(DfsMetadataDurableReplication {
                confirmation_id: String::new(),
                journal_frames: Vec::new(),
            }),
            batch: DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries: Vec::new(),
                next_after_sequence: 1,
            },
        };

        let event = service
            .handle_metadata_journal_batch(source.clone(), command)
            .expect("malformed carrier returns a typed event");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                ref request_id,
                ref source_node_id,
                ref shard_id,
                ..
            } if request_id == "malformed-frame-request"
                && source_node_id == &source
                && shard_id == &DfsMetadataShardId::new("root")
        ));
        let messages = DfsMessageTransport::drain_incoming_messages(&mut source_transport)
            .expect("typed rejection message");
        assert!(matches!(
            messages.as_slice(),
            [DfsRoutedMessage {
                message: DfsNetworkMessage::MetadataJournalBatchResponse(
                    DfsMetadataJournalBatchResponse {
                        request_id: response_request_id,
                        shard_id: response_shard_id,
                        result: DfsMetadataJournalBatchResult::Rejected { .. },
                        ..
                    }
                ),
                ..
            }] if response_request_id == request_id && response_shard_id == &shard_id
        ));
    }
}
