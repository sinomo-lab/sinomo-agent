//! Distributed SIFS namespace bootstrap helpers.

use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsSubtreePolicy, DfsVersion, sifs_file_system_id,
};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;

pub const SIFS_ROOT_PATH: &str = "/";
pub const SIFS_SINOMO_PATH: &str = "/sinomo";
pub const SIFS_NODES_PATH: &str = "/sinomo/nodes";
pub const SIFS_USERS_PATH: &str = "/users";
pub const SIFS_SERVICES_PATH: &str = "/services";

pub fn distributed_sifs(owner_node_id: NodeId) -> DfsFileSystem {
    let node_files_path = sifs_node_files_path(&owner_node_id);
    DfsFileSystem {
        file_system_id: sifs_file_system_id(),
        display_name: "sifs".to_string(),
        owner_node_id,
        subtree_policies: vec![
            DfsSubtreePolicy::legacy(node_files_path, DfsObjectPolicy::LocalOwned),
            DfsSubtreePolicy::legacy(SIFS_USERS_PATH, DfsObjectPolicy::ReplicatedEventual),
            DfsSubtreePolicy::legacy(SIFS_SERVICES_PATH, DfsObjectPolicy::ReplicatedEventual),
        ],
    }
}

pub fn distributed_sifs_bootstrap_objects(owner_node_id: NodeId) -> Vec<DfsObjectMetadata> {
    let node_path = sifs_node_path(&owner_node_id);
    let node_files_path = sifs_node_files_path(&owner_node_id);
    [
        SIFS_ROOT_PATH.to_string(),
        SIFS_SINOMO_PATH.to_string(),
        SIFS_NODES_PATH.to_string(),
        node_path,
        node_files_path,
        SIFS_USERS_PATH.to_string(),
        SIFS_SERVICES_PATH.to_string(),
    ]
    .into_iter()
    .map(|path| {
        let policy = if path == sifs_node_files_path(&owner_node_id) {
            DfsObjectPolicy::LocalOwned
        } else {
            DfsObjectPolicy::ReplicatedEventual
        };
        directory_object_with_policy(&path, owner_node_id.clone(), policy)
    })
    .collect()
}

pub fn distributed_sifs_bootstrap_objects_for_nodes(
    node_ids: impl IntoIterator<Item = NodeId>,
    fallback_node_id: NodeId,
) -> Vec<DfsObjectMetadata> {
    let mut seen = std::collections::BTreeSet::new();
    let mut objects = Vec::new();
    let mut node_ids = node_ids.into_iter().collect::<Vec<_>>();
    if node_ids.is_empty() {
        node_ids.push(fallback_node_id);
    }
    for node_id in node_ids {
        for object in distributed_sifs_bootstrap_objects(node_id) {
            if seen.insert(object.path.clone()) {
                objects.push(object);
            }
        }
    }
    objects
}

pub fn directory_object(path: &str, owner_node_id: NodeId) -> DfsObjectMetadata {
    directory_object_with_policy(path, owner_node_id, DfsObjectPolicy::ReplicatedEventual)
}

pub fn directory_object_with_policy(
    path: &str,
    owner_node_id: NodeId,
    policy: DfsObjectPolicy,
) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id: object_id_for_path(path),
        file_system_id: sifs_file_system_id(),
        path: normalize_sifs_path(path),
        kind: DfsObjectKind::Directory,
        tombstone_provenance: None,
        owner_node_id: owner_node_id.clone(),
        current_locations: vec![owner_node_id],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o755,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: initial_version(),
        policy,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

pub fn sifs_node_path(node_id: &NodeId) -> String {
    format!("{SIFS_NODES_PATH}/{node_id}")
}

pub fn sifs_node_files_path(node_id: &NodeId) -> String {
    format!("{}/files", sifs_node_path(node_id))
}

pub fn object_id_for_path(path: &str) -> DfsObjectId {
    let normalized = normalize_sifs_path(path);
    DfsObjectId::derived("sinomo.sifs.object.path", normalized.as_bytes())
}

pub fn normalize_sifs_path(path: &str) -> String {
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

pub fn metadata_path(path: &str) -> String {
    normalize_sifs_path(path).trim_matches('/').to_string()
}

pub fn initial_version() -> DfsVersion {
    DfsVersion::from_legacy_named("version_1").expect("valid initial version")
}

pub fn next_version(version: &DfsVersion) -> DfsVersion {
    let suffix = version
        .as_str()
        .strip_prefix("version_")
        .unwrap_or(version.as_str());
    if let Ok(number) = suffix.parse::<u64>() {
        return DfsVersion::from_legacy_named(format!("version_{}", number.saturating_add(1)))
            .expect("valid incremented version");
    }
    DfsVersion::generate()
}

pub(crate) fn namespace_transaction_id_for_path(
    kind: &str,
    object_id: &DfsObjectId,
    path: &str,
) -> String {
    format!(
        "namespace_{}_{}",
        kind,
        blake3::hash(format!("{object_id}\0{path}").as_bytes()).to_hex()
    )
}

pub(crate) fn namespace_transaction_id_for_object(
    kind: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> String {
    format!(
        "namespace_{}_{}",
        kind,
        blake3::hash(format!("{object_id}\0{version}").as_bytes()).to_hex()
    )
}

pub(crate) fn namespace_transaction_id_for_batch(
    kind: &str,
    file_system_id: &DfsFileSystemId,
    objects: &[DfsObjectMetadata],
    versions: &[DfsObjectVersionRecord],
) -> String {
    // This is an idempotency identity, so it must cover the complete durable
    // payload. In particular, placement reconciliation changes policy and
    // replica locations without minting a new content version. The former
    // hand-maintained fingerprint omitted those fields, causing a later
    // policy transition to reuse an existing transaction ID with a different
    // payload.
    let input = serde_json::to_vec(&(file_system_id, objects, versions))
        .expect("DFS namespace transaction payload is serializable");
    format!("namespace_{kind}_{}", blake3::hash(&input).to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributed_sifs_bootstrap_exposes_users_and_services() {
        let node = NodeId::from_legacy_named("node_alpha").expect("node");
        let filesystem = distributed_sifs(node.clone());
        let objects = distributed_sifs_bootstrap_objects(node);

        assert_eq!(filesystem.file_system_id, sifs_file_system_id());
        assert!(
            filesystem
                .subtree_policies
                .iter()
                .any(|policy| policy.path_prefix == SIFS_USERS_PATH)
        );
        assert!(filesystem.subtree_policies.iter().any(|policy| {
            policy.path_prefix == sifs_node_files_path(&filesystem.owner_node_id)
                && policy.policy == DfsObjectPolicy::LocalOwned
        }));
        assert!(objects.iter().any(|object| object.path == SIFS_SINOMO_PATH));
        assert!(objects.iter().any(|object| object.path == SIFS_NODES_PATH));
        assert!(
            objects
                .iter()
                .any(|object| object.path == sifs_node_path(&filesystem.owner_node_id))
        );
        assert!(objects.iter().any(|object| {
            object.path == sifs_node_files_path(&filesystem.owner_node_id)
                && object.policy == DfsObjectPolicy::LocalOwned
        }));
        assert!(objects.iter().any(|object| object.path == SIFS_USERS_PATH));
        assert!(
            objects
                .iter()
                .any(|object| object.path == SIFS_SERVICES_PATH)
        );
    }

    #[test]
    fn distributed_sifs_bootstrap_for_nodes_deduplicates_shared_paths() {
        let node_a = NodeId::from_legacy_named("node_bootstrap_a").expect("node a");
        let node_b = NodeId::from_legacy_named("node_bootstrap_b").expect("node b");
        let objects = distributed_sifs_bootstrap_objects_for_nodes(
            vec![node_a.clone(), node_b.clone()],
            node_a.clone(),
        );

        assert_eq!(
            objects
                .iter()
                .filter(|object| object.path == SIFS_ROOT_PATH)
                .count(),
            1
        );
        assert!(
            objects
                .iter()
                .any(|object| object.path == sifs_node_files_path(&node_a))
        );
        assert!(
            objects
                .iter()
                .any(|object| object.path == sifs_node_files_path(&node_b))
        );
    }

    #[test]
    fn namespace_batch_transaction_id_covers_policy_only_metadata_changes() {
        let node = NodeId::from_legacy_named("node_namespace_batch_payload").expect("node");
        let mut object = distributed_sifs_bootstrap_objects(node)
            .into_iter()
            .find(|object| object.path == SIFS_SERVICES_PATH)
            .expect("services object");
        let file_system_id = object.file_system_id.clone();
        let eventual = namespace_transaction_id_for_batch(
            "visible-object",
            &file_system_id,
            std::slice::from_ref(&object),
            &[],
        );
        assert_eq!(
            eventual,
            namespace_transaction_id_for_batch(
                "visible-object",
                &file_system_id,
                std::slice::from_ref(&object),
                &[],
            ),
            "an exact retry must keep the same transaction identity"
        );

        object.policy = DfsObjectPolicy::ReplicatedConfirmed;
        let confirmed = namespace_transaction_id_for_batch(
            "visible-object",
            &file_system_id,
            std::slice::from_ref(&object),
            &[],
        );
        assert_ne!(
            eventual, confirmed,
            "a policy-only mutation is a different durable transaction payload"
        );
    }
}
