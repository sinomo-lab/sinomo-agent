//! Closed attributed mkdir commands through the ordinary native namespace owner.

use super::*;
use crate::test_runtime::DfsTestRuntime;
use crate::*;
use serde::{Deserialize, Serialize};

const PARENT: &str = "/services/attributed-mkdir-native/parent";

fn fixture(
    name: &str,
) -> (
    DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
    DfsMetadataShardConfig,
) {
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 3, PARENT).unwrap();
    drop(initial);
    let caller = DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .find(|node| *node != shard.primary_node_id)
        .unwrap();
    let mut client = runtime.client(&caller).unwrap();
    assert!(client.transport.delegates_local_namespace_authority());
    client.posix_mkdir_all(PARENT).unwrap();
    (runtime, client, shard)
}

fn operation(parent: &DfsStat, child: &str, attrs: DfsPosixCreateAttrs) -> DfsNamespaceOperation {
    DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(DfsMkdirAtNoLinksWithAttrs {
        path: join_posix_path(&parent.path, child),
        expected_parent_object_id: parent.object_id.clone(),
        expected_parent_version: parent.version.clone(),
        attrs,
    })
}

fn wire(value: &impl Serialize) -> sinomo_core::network::ObjectRpcRequest {
    sinomo_core::network::ObjectRpcRequest::encode_body(DFS_DATA_NAMESPACE_ROUTE, "dfs-test", value)
        .unwrap()
}

#[test]
fn attributed_mkdir_wire_is_closed_and_legacy_guard_encoding_is_unchanged() {
    // Preserve the actual legacy prefix, including its derived enum ordinals.
    #[derive(Serialize, Deserialize)]
    #[serde(rename = "DfsNamespaceOperation", rename_all = "kebab-case")]
    enum LegacyDirectoryOperation {
        BeginContentCopy {
            path: String,
            request: DfsContentCopyRequest,
            options: DfsWriteOptions,
        },
        MetadataQuery {
            query: DfsMetadataShardQuery,
        },
        ContentReference {
            request: DfsContentReferenceRequest,
        },
        PollNamespaceChanges {
            request: DfsNamespaceChangePollRequest,
        },
        Mkdir {
            path: String,
        },
        MkdirWithAttrs {
            path: String,
            attrs: DfsPosixCreateAttrs,
        },
        MkdirAtNoLinks {
            path: String,
            expected_parent_object_id: DfsObjectId,
            expected_parent_version: DfsVersion,
        },
        MkdirAll {
            path: String,
        },
    }
    let id = DfsObjectId::generate();
    let version = DfsVersion::generate();
    let legacy = DfsNamespaceOperation::MkdirAtNoLinks {
        path: format!("{PARENT}/legacy"),
        expected_parent_object_id: id.clone(),
        expected_parent_version: version.clone(),
    };
    let before = LegacyDirectoryOperation::MkdirAtNoLinks {
        path: format!("{PARENT}/legacy"),
        expected_parent_object_id: id.clone(),
        expected_parent_version: version.clone(),
    };
    assert_eq!(wire(&legacy).body, wire(&before).body);
    assert!(
        wire(&legacy)
            .decode_body::<LegacyDirectoryOperation>()
            .is_ok()
    );
    let new = DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(DfsMkdirAtNoLinksWithAttrs {
        path: format!("{PARENT}/private"),
        expected_parent_object_id: id,
        expected_parent_version: version,
        attrs: DfsPosixCreateAttrs::new(0o700, 1234, 2345),
    });
    assert_eq!(
        wire(&new).decode_body::<DfsNamespaceOperation>().unwrap(),
        new
    );
    assert!(
        wire(&new)
            .decode_body::<LegacyDirectoryOperation>()
            .is_err()
    );
    let original = serde_json::to_value(&new).unwrap();
    let mut unknown = original.clone();
    unknown["mkdir-at-no-links-with-attrs"]["unknown_outer_field"] = true.into();
    assert!(serde_json::from_value::<DfsNamespaceOperation>(unknown).is_err());
    for bad in [
        serde_json::json!(-1),
        serde_json::json!("0700"),
        serde_json::Value::Null,
    ] {
        let mut malformed = original.clone();
        malformed["mkdir-at-no-links-with-attrs"]["attrs"]["mode"] = bad;
        assert!(serde_json::from_value::<DfsNamespaceOperation>(malformed).is_err());
    }
    let mut missing = original.clone();
    missing["mkdir-at-no-links-with-attrs"]["attrs"]
        .as_object_mut()
        .unwrap()
        .remove("uid");
    assert!(serde_json::from_value::<DfsNamespaceOperation>(missing).is_err());
    let mut leaf_extension = original;
    leaf_extension["mkdir-at-no-links-with-attrs"]["attrs"]["legacy_leaf_extension"] = true.into();
    assert_eq!(
        serde_json::from_value::<DfsNamespaceOperation>(leaf_extension).unwrap(),
        new
    );
    assert!(new.changes_visible_namespace());
    let path = format!("{PARENT}/private");
    assert_eq!(new.routing_path(), Some(path.as_str()));
    assert_eq!(new.involved_paths(), vec![path.as_str()]);
}

#[test]
fn attributed_mkdir_remote_create_publishes_attrs_and_keeps_default_opcode() {
    let (runtime, mut client, shard) = fixture("native-attributed-mkdir-create");
    let opened = client
        .posix_open_beneath_no_links_distributed("/", PARENT)
        .unwrap();
    let attrs = DfsPosixCreateAttrs::new(0o700, 1234, 2345);
    let expected = operation(&opened, "private", attrs);
    let capture = runtime
        .capture_namespace_requests(&shard.primary_node_id, 32)
        .unwrap();
    let created = client
        .posix_mkdir_at_opened_directory_no_links_with_attrs(&opened, "private", attrs)
        .unwrap();
    let calls = capture.records();
    drop(capture);
    let commands: Vec<_> = calls
        .iter()
        .filter_map(|head| {
            let DfsNetworkMessage::NamespaceRequest(request) = &head.message else {
                return None;
            };
            (request.operation == expected).then_some(request)
        })
        .collect();
    assert_eq!(commands.len(), 1, "one actual closed attributed command");
    let command = commands[0];
    assert_eq!(
        (created.mode, created.uid, created.gid),
        (0o700, 1234, 2345)
    );
    let owner = runtime.metadata_primary_service(&created.path).unwrap();
    let request = owner
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&client.node_id, &command.request_id)
        .unwrap()
        .unwrap();
    assert_eq!(request.operation, expected);
    assert_eq!(request.result, Some(DfsNamespaceResult::Applied));
    let snapshot = owner
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new(&shard.shard_id))
        .unwrap()
        .unwrap();
    let bodies: Vec<_> = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record
                .transaction
                .objects
                .iter()
                .any(|object| object.object_id == created.object_id)
        })
        .collect();
    assert_eq!(bodies.len(), 1, "one initial native directory publication");
    assert_eq!(bodies[0].status, DfsPreparedOperationStatus::Committed);
    let object = bodies[0]
        .transaction
        .objects
        .iter()
        .find(|object| object.object_id == created.object_id)
        .unwrap();
    assert_eq!(
        (object.mode, object.uid, object.gid),
        (0o700, 1234, 2345)
    );
    drop(owner);
    let reopened = client
        .posix_open_beneath_no_links_distributed("/", PARENT)
        .unwrap();
    let capture = runtime
        .capture_namespace_requests(&shard.primary_node_id, 32)
        .unwrap();
    let default = client
        .posix_mkdir_at_opened_directory_no_links(&reopened, "default")
        .unwrap();
    let calls = capture.records();
    drop(capture);
    assert_eq!(default.mode, 0o755);
    assert!(calls.iter().any(|head| matches!(&head.message,
        DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MkdirAtNoLinks { path, .. }
                if path == &default.path))));
    assert!(!calls.iter().any(|head| matches!(&head.message,
        DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(_)))));
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn attributed_mkdir_same_request_replays_exact_body_and_rejects_changed_fields() {
    let (runtime, mut client, shard) = fixture("native-attributed-mkdir-replay");
    let opened = client
        .posix_open_beneath_no_links_distributed("/", PARENT)
        .unwrap();
    let attrs = DfsPosixCreateAttrs::new(0o710, 321, 654);
    let original = operation(&opened, "stable", attrs);
    let request_id = "native-attributed-directory-original".to_string();
    client
        .route_namespace_operation_with_request_id(
            request_id.clone(),
            shard.primary_node_id.clone(),
            original.clone(),
        )
        .unwrap();
    let before = client
        .posix_namespace_lstat_distributed(&format!("{PARENT}/stable"))
        .unwrap();
    let parent_before_replay = client.posix_namespace_lstat_distributed(PARENT).unwrap();
    let owner = runtime.metadata_primary_service(PARENT).unwrap();
    let saved = owner
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&client.node_id, &request_id)
        .unwrap()
        .unwrap();
    assert_eq!(saved.operation, original);
    drop(owner);
    client
        .route_namespace_operation_with_request_id(
            request_id.clone(),
            shard.primary_node_id.clone(),
            original.clone(),
        )
        .unwrap();
    let DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(payload) = original.clone() else {
        unreachable!()
    };
    let mut changed = Vec::new();
    let mut attrs_change = payload.clone();
    attrs_change.attrs.mode ^= 0o010;
    changed.push(attrs_change);
    let mut path_change = payload.clone();
    path_change.path = format!("{PARENT}/different");
    changed.push(path_change);
    let mut id_change = payload.clone();
    id_change.expected_parent_object_id = DfsObjectId::generate();
    changed.push(id_change);
    let mut version_change = payload;
    version_change.expected_parent_version = DfsVersion::generate();
    changed.push(version_change);
    for payload in changed {
        let result = client.route_namespace_operation_with_request_id(
            request_id.clone(),
            shard.primary_node_id.clone(),
            DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(payload),
        );
        assert!(
            matches!(result, Err(DfsPosixError::Conflict { .. })),
            "{result:?}"
        );
    }
    let after = client
        .posix_namespace_lstat_distributed(&before.path)
        .unwrap();
    let parent_after_replay = client.posix_namespace_lstat_distributed(PARENT).unwrap();
    assert_eq!(
        (
            after.object_id,
            after.version,
            after.mode,
            after.uid,
            after.gid
        ),
        (before.object_id, before.version, 0o710, 321, 654)
    );
    assert_eq!(parent_after_replay.version, parent_before_replay.version);
    let owner = runtime.metadata_primary_service(PARENT).unwrap();
    let retained = owner
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&client.node_id, &request_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.operation, saved.operation);
    assert_eq!(retained.command_digest, saved.command_digest);
    assert_eq!(retained.result, Some(DfsNamespaceResult::Applied));
    assert!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), &format!("{PARENT}/different"))
            .unwrap()
            .is_none()
    );
    drop(owner);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn attributed_mkdir_stale_actual_parent_command_has_no_leaf_effect() {
    let (runtime, mut client, shard) = fixture("native-attributed-mkdir-stale-parent");
    let opened = client
        .posix_open_beneath_no_links_distributed("/", PARENT)
        .unwrap();
    // Construct the original command before the real parent changes, then
    // deliver it once through normal transport, bypassing no native guard.
    let original = operation(
        &opened,
        "must-not-exist",
        DfsPosixCreateAttrs::new(0o700, 21, 22),
    );
    let mut actor = runtime.metadata_primary_service(PARENT).unwrap();
    let changed = actor.posix_set_mode(PARENT, opened.mode ^ 0o010).unwrap();
    assert_eq!(changed.object_id, opened.object_id);
    assert_ne!(changed.version, opened.version);
    drop(actor);
    let capture = runtime
        .capture_namespace_requests(&shard.primary_node_id, 32)
        .unwrap();
    let result = client.route_namespace_operation_with_request_id(
        "native-stale-attributed-original".into(),
        shard.primary_node_id.clone(),
        original.clone(),
    );
    let calls = capture.records();
    drop(capture);
    assert!(
        matches!(result, Err(DfsPosixError::Conflict { ref path, .. }) if path == PARENT),
        "{result:?}"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|head| matches!(&head.message,
        DfsNetworkMessage::NamespaceRequest(request) if request.operation == original))
            .count(),
        1
    );
    let owner = runtime.metadata_primary_service(PARENT).unwrap();
    assert!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), &format!("{PARENT}/must-not-exist"))
            .unwrap()
            .is_none()
    );
    drop(owner);
    drop(client);
    runtime.close().unwrap();
}
