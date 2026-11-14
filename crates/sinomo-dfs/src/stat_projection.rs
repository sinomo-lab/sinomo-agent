//! Bounded, generation-bound inputs to authoritative stat. These are read data,
//! never a replica image or publication authority.

use crate::*;
use serde::{Deserialize, Serialize};

mod path;
pub use path::{DfsStatPathRequest, DfsStatPathView, DfsStatShardPrefix};
pub(crate) use path::{query_stat_path, stat_catalog_digest};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsStatProjectionCursor {
    Hardlinks { after: DfsObjectPageCursor },
    Children { after: DfsDirectoryPageCursor },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsStatProjectionRequest {
    pub object_id: DfsObjectId,
    pub object_path: String,
    pub object_kind: DfsObjectKind,
    pub object_version: DfsVersion,
    pub limit: usize,
    pub after: Option<DfsStatProjectionCursor>,
    pub expected_prefix: Option<DfsMetadataPrefixProof>,
    /// Revalidate a completed collection without enumerating it again.
    pub fence_only: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsStatProjectionPage {
    pub request: DfsStatProjectionRequest,
    pub prefix: DfsMetadataPrefixProof,
    pub object: Option<Box<DfsObjectMetadata>>,
    pub entries: Vec<DfsObjectMetadata>,
    /// Explicitly required on the wire; omission is not an empty final page.
    pub complete: bool,
    /// None is complete only for this exact shard, target and prefix.
    pub after: Option<DfsStatProjectionCursor>,
}

pub(crate) fn query_stat_projection<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    query: &DfsMetadataShardQuery,
    request: &DfsStatProjectionRequest,
) -> Result<DfsMetadataQueryResult, MetadataBackendError> {
    let invalid = |message: &str| MetadataBackendError::Serde(message.to_owned());
    if crate::posix_path::normalize_posix_path(&request.object_path)
        .ok()
        .as_ref()
        != Some(&request.object_path)
        || matches!(
            request.object_kind,
            DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
        )
        || (request.after.is_some() && request.expected_prefix.is_none())
        || request
            .expected_prefix
            .as_ref()
            .is_some_and(|prefix| !prefix.is_well_formed())
        || request.limit == 0
        || request.limit > crate::file_history_query::HISTORY_PAGE_LIMIT
        || (request.fence_only && (request.expected_prefix.is_none() || request.after.is_some()))
    {
        return Err(invalid("invalid authoritative stat target or continuation"));
    }
    let prefix = backend
        .metadata_prefix_proof(&query.shard_id)?
        .filter(DfsMetadataPrefixProof::is_well_formed)
        .ok_or_else(|| invalid("authoritative stat has no exact materialized prefix"))?;
    if request
        .expected_prefix
        .as_ref()
        .is_some_and(|expected| expected != &prefix)
    {
        return Ok(DfsMetadataQueryResult::StatProjectionChangedV1 {
            request: Box::new(request.clone()),
            current_prefix: prefix,
        });
    }
    let mut object = backend.get_object(&request.object_id)?.filter(|row| {
        row.file_system_id == query.file_system_id
            && row.tombstone_provenance.is_none()
            && row.path == request.object_path
    });
    enrich_stat_object(backend, &mut object)?;
    let (entries, after) = if request.fence_only {
        (Vec::new(), None)
    } else if request.object_kind == DfsObjectKind::Directory {
        let after = match request.after.as_ref() {
            Some(DfsStatProjectionCursor::Children { after }) if after.shard_id.is_none() => {
                Some(after)
            }
            None => None,
            _ => {
                return Err(invalid(
                    "authoritative stat directory cursor has wrong scope",
                ));
            }
        };
        let (entries, next) = backend.child_objects_for_directory_page_entries(
            &query.file_system_id,
            &request.object_path,
            after,
            request.limit,
        )?;
        (
            entries.into_iter().map(|(object, _)| object).collect(),
            next.map(|after| DfsStatProjectionCursor::Children { after }),
        )
    } else {
        let after = match request.after.as_ref() {
            Some(DfsStatProjectionCursor::Hardlinks { after }) => Some(after),
            None => None,
            _ => return Err(invalid("authoritative stat hardlink cursor has wrong kind")),
        };
        let page = backend.hardlinks_to_object_page(
            &query.file_system_id,
            &request.object_id,
            after,
            request.limit,
        )?;
        (
            page.objects,
            page.next_cursor
                .map(|after| DfsStatProjectionCursor::Hardlinks { after }),
        )
    };
    if entries.len() > request.limit
        || (after.is_some() && (entries.is_empty() || after == request.after))
    {
        return Err(invalid(
            "authoritative stat page is oversized or made no progress",
        ));
    }
    Ok(DfsMetadataQueryResult::StatProjectionV1 {
        page: Box::new(DfsStatProjectionPage {
            request: request.clone(),
            prefix,
            object: object.map(Box::new),
            entries,
            complete: after.is_none(),
            after,
        }),
    })
}

/// Enrich only the selected inode from its held authority snapshot.
/// This never enumerates aliases, children, or another shard.
pub(crate) fn enrich_stat_object<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object: &mut Option<DfsObjectMetadata>,
) -> Result<(), MetadataBackendError> {
    let invalid = |message: &str| MetadataBackendError::Serde(message.to_owned());
    if let Some(row) = object.as_mut()
        && row.content_size_bytes.is_none()
        && let Some(version) = backend.get_version(&row.object_id, &row.version)?
        && version.content_hash == row.content_hash
    {
        row.content_size_bytes = version.size_bytes;
    }
    if let Some(row) = object.as_mut().filter(|row| row.content_hash.is_some()) {
        // Preserve ordinary stat's current-version receipt-derived locations,
        // under this same authority cut, with an explicit object-local bound.
        let receipts = backend.receipts_for_version_page(
            &row.object_id,
            &row.version,
            None,
            crate::file_history_query::HISTORY_PAGE_LIMIT,
        )?;
        if receipts.next_cursor.is_some() {
            return Err(invalid(
                "stat receipt projection is incomplete within its record limit",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        row.current_locations
            .retain(|node| seen.insert(node.clone()));
        for receipt in receipts.receipts {
            if receipt.receipt.file_system_id != row.file_system_id
                || receipt.receipt.object_id != row.object_id
                || receipt.receipt.committed_version != row.version
            {
                return Err(invalid(
                    "stat receipt projection returned a foreign identity",
                ));
            }
            if receipt.content_hash == row.content_hash
                && !row.current_locations.contains(&receipt.replica_node_id)
            {
                row.current_locations.push(receipt.replica_node_id);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::object::serde_ext as sinomo_object_serde;

    #[test]
    fn maintained_stat_wire_requires_explicit_authoritative_absence() {
        let absent = DfsMetadataQueryResult::StatAtPathV2 { object: None };
        let encoded =
            sinomo_object_serde::to_bytes(&absent, 1, sinomo_core::object::LengthMode::U32)
                .unwrap();
        let decoded: DfsMetadataQueryResult = sinomo_object_serde::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, absent);
        assert!(
            serde_json::from_value::<DfsMetadataQueryResult>(serde_json::json!({
                "kind": "stat-at-path-v2"
            }))
            .is_err()
        );
        assert_eq!(
            serde_json::from_value::<DfsMetadataQueryResult>(serde_json::json!({
                "kind": "stat-at-path-v2", "object": null
            }))
            .unwrap(),
            absent
        );
    }

    #[test]
    fn stat_path_wire_roundtrip_preserves_named_prefix_records_and_catalog_binding() {
        // Codec DATA only; this creates neither a service nor authority custody.
        let node = sinomo_core::node::NodeId::from_legacy_named("stat-codec-owner").unwrap();
        let root = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: node,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::LocalOwned,
        };
        let services = DfsMetadataShardConfig {
            shard_id: "services".into(),
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".into(),
            },
            ..root.clone()
        };
        let catalog = vec![root, services];
        let prefixes = catalog
            .iter()
            .map(|s| {
                let shard_id = DfsMetadataShardId::new(s.shard_id.clone());
                let prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard_id);
                DfsStatShardPrefix { shard_id, prefix }
            })
            .collect::<Vec<_>>();
        let old_pairs = prefixes
            .iter()
            .map(|p| (p.shard_id.clone(), p.prefix.clone()))
            .collect::<Vec<_>>();
        eprintln!(
            "stat codec historical tuple diagnostic: {:?}",
            sinomo_object_serde::to_bytes(&old_pairs, 1, sinomo_core::object::LengthMode::U32)
                .map(|b| b.len())
        );
        eprintln!(
            "stat codec mixed catalog diagnostic: {:?}",
            sinomo_object_serde::to_bytes(&catalog, 1, sinomo_core::object::LengthMode::U32)
                .map(|b| b.len())
        );
        let mut view = DfsStatPathView {
            request: DfsStatPathRequest {
                path: "/services/missing".into(),
                read_id: "stat-codec-read-1".into(),
                catalog_digest: stat_catalog_digest(&catalog).unwrap(),
            },
            path_object: None,
            object: None,
            link_count: None,
            complete: true,
            prefixes,
            integrity: String::new(),
        };
        view.integrity = view.integrity_digest().unwrap();
        let response = DfsMetadataQueryResult::StatAtPathV1 {
            view: Box::new(view),
        };
        let bytes =
            sinomo_object_serde::to_bytes(&response, 1, sinomo_core::object::LengthMode::U32)
                .unwrap();
        assert_eq!(
            sinomo_object_serde::from_bytes::<DfsMetadataQueryResult>(&bytes).unwrap(),
            response
        );
    }

    #[test]
    fn native_stat_page_preserves_alias_identity_and_rejects_changed_generation() {
        let node =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("stat-page-v1", 1).remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("stat-page-v1", node.clone()).unwrap();
        runtime.start_nodes().unwrap();
        let mut caller = runtime.client(&node).unwrap();
        let path = "/users/stat-page-target";
        let alias = "/users/stat-page-alias";
        caller
            .posix_write_file(
                path,
                b"stat bytes",
                DfsWriteOptions::replicated_eventual(vec![node]),
            )
            .unwrap();
        let object = caller.file_history_capture_object(path).unwrap();
        caller.posix_hardlink(path, alias).unwrap();
        let authorities = caller
            .metadata_shard_authorities(&object.file_system_id)
            .unwrap();
        let shard = crate::select_metadata_shard_config_for_path(
            &object.file_system_id,
            &authorities,
            path,
        )
        .unwrap()
        .unwrap();
        let mut request = DfsStatProjectionRequest {
            object_id: object.object_id.clone(),
            object_path: path.to_owned(),
            object_kind: object.kind.clone(),
            object_version: object.version.clone(),
            limit: crate::file_history_query::HISTORY_PAGE_LIMIT,
            after: None,
            expected_prefix: None,
            fence_only: false,
        };
        let query = |caller: &mut NetworkedDfsService<DfsNodeTransport>,
                     request: &DfsStatProjectionRequest| {
            caller.query_metadata_authority_impl(
                &shard,
                DfsMetadataSelection::StatProjectionV1 {
                    request: Box::new(request.clone()),
                },
            )
        };
        let DfsMetadataQueryResult::StatProjectionV1 { page } =
            query(&mut caller, &request).unwrap()
        else {
            panic!("wrong typed stat response")
        };
        assert_eq!(page.request, request);
        assert!(page.prefix.is_well_formed());
        assert!(page.after.is_none());
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].path, alias);
        assert_eq!(
            page.entries[0].link_target_object_id.as_ref(),
            Some(&object.object_id)
        );
        assert_eq!(page.object.as_ref().unwrap().object_id, object.object_id);
        request.expected_prefix = Some(page.prefix.clone());
        request.fence_only = true;
        let DfsMetadataQueryResult::StatProjectionV1 { page: fence } =
            query(&mut caller, &request).unwrap()
        else {
            panic!("wrong typed stat fence")
        };
        assert_eq!(fence.prefix, page.prefix);
        assert_eq!(fence.object, page.object);
        assert!(fence.entries.is_empty() && fence.after.is_none());
        caller.posix_unlink(alias).unwrap();
        assert!(
            query(&mut caller, &request).is_err(),
            "an old prefix cannot certify the count after unlink"
        );
        assert_eq!(
            query(&mut caller, &request)
                .unwrap_err()
                .metadata_rejection_code(),
            DfsMetadataRejectionCode::PrefixMismatch,
            "the native wire preserves a typed forward-generation refusal"
        );
        request.expected_prefix = None;
        request.fence_only = false;
        let DfsMetadataQueryResult::StatProjectionV1 { page: after } =
            query(&mut caller, &request).unwrap()
        else {
            panic!("wrong typed stat response")
        };
        assert!(after.entries.is_empty() && after.after.is_none());
        assert_ne!(after.prefix, page.prefix);
        request.fence_only = true;
        assert!(
            query(&mut caller, &request).is_err(),
            "a fence must name its original prefix"
        );
        let mut future = after.prefix.clone();
        future.journal_sequence += 1;
        request.expected_prefix = Some(future);
        assert_ne!(
            query(&mut caller, &request)
                .unwrap_err()
                .metadata_rejection_code(),
            DfsMetadataRejectionCode::PrefixMismatch,
            "a backward or equal-sequence conflict cannot authorize retry"
        );
        assert!(caller.pending_routed_namespace_ops.is_empty());
        assert_eq!(caller.routed_namespace_settle_depth, 0);
        drop(caller);
        runtime.close().unwrap();
    }

    #[test]
    fn stat_wire_does_not_default_missing_evidence_or_unknown_versions() {
        assert!(
            serde_json::from_str::<DfsMetadataSelection>(
                r#"{"kind":"stat-projection-v2","request":{}}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<DfsMetadataQueryResult>(
                r#"{"kind":"stat-projection-v1","page":{"entries":[],"after":null}}"#
            )
            .is_err()
        );
        // Existing selections retain their exact tagged representation.
        let existing = DfsMetadataSelection::CurrentObject {
            object_id: DfsObjectId::generate(),
        };
        assert_eq!(
            serde_json::from_slice::<DfsMetadataSelection>(&serde_json::to_vec(&existing).unwrap())
                .unwrap(),
            existing
        );
    }
}
