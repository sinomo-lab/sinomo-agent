//! A combined copy's capture of `users` keeps what `users` owns and leaves
//! the in-flight work of `root`, the copy's other shard, to `root`, so
//! ordinary in-flight work and long-lived floors of the copy do not stop
//! `users`' conversion.
use super::*;
use crate::metadata::canonical_generation::{
    CapturedMetadataSource, OriginalWorkRef, unresolved_beside_recorded_jobs_for_test,
};
use serde_json::json;
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsAdvisoryLockStateRecord, DfsContentReferenceOutcomeRetention, DfsInodeRecord,
    DfsMetadataCandidateState, DfsMetadataShardAssignment, DfsMetadataTableSnapshotTables,
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsPreparedMetadataEffect,
    DfsPreparedOperationDigest, DfsPreparedOperationPayload, DfsPreparedOperationRecord,
    DfsPreparedOperationScope, DfsPreparedOperationStatus, DfsReplicationPolicy, DfsVersion,
    DfsWriteIntentReceipt, DfsWriteIntentRecord, DfsWriteIntentStatus, DfsWritePublication,
};
use time::OffsetDateTime;

fn file_system() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("combined-attribution").unwrap()
}

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn config(shard: &str, prefix: Option<&str>) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: file_system(),
        shard_id: shard.into(),
        primary_node_id: node(&format!("{shard}-primary")),
        replica_node_ids: Vec::new(),
        epoch: 4,
        assignment: prefix.map_or(DfsMetadataShardAssignment::Root, |prefix| {
            DfsMetadataShardAssignment::PathPrefix {
                prefix: prefix.into(),
            }
        }),
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    }
}

fn catalog() -> Vec<DfsMetadataShardConfig> {
    vec![config("root", None), config("users", Some("/users"))]
}

fn object(name: &str, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id: DfsObjectId::from_legacy_named(name).unwrap(),
        file_system_id: file_system(),
        path: path.into(),
        kind,
        namespace_link_count: Some(1),
        tombstone_provenance: None,
        owner_node_id: node("combined-owner"),
        current_locations: Vec::new(),
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named(format!("{name}-version")).unwrap(),
        policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

fn users_file() -> DfsObjectMetadata {
    object("users-file", "/users/file", DfsObjectKind::File)
}

fn root_file() -> DfsObjectMetadata {
    object("root-file", "/root-file", DfsObjectKind::File)
}

/// The rows of both shards in one copy, as its projection reads them.
fn combined_rows() -> DfsMetadataTableSnapshotTables {
    let mut state = DfsMetadataCandidateState::new(Default::default()).unwrap();
    for object in [
        object("users-dir", "/users", DfsObjectKind::Directory),
        users_file(),
        root_file(),
    ] {
        state
            .apply(DfsPreparedMetadataEffect::PutObject { object })
            .unwrap();
    }
    state.snapshot()
}

fn source(shard: &str) -> CapturedMetadataSource {
    CapturedMetadataSource {
        retained_placement_reservations: Vec::new(),
        actual_source_root: PathBuf::from("/combined-root"),
        source_identity: "combined-source".into(),
        source_format: Some(3),
        source_snapshot_fingerprint: "combined-snapshot".into(),
        exact_prefix: None,
        source_journal_sequence: 9,
        source_state_fingerprint: "combined-state".into(),
        file_system_ids: vec![file_system()],
        physical_shard: DfsMetadataShardId::new(shard),
        projection_assignments: catalog(),
        original_handoff: None,
        evidence_root: PathBuf::from("/combined-evidence"),
        planned_target_root: PathBuf::from("/combined-image"),
        event_ledger_cut: Default::default(),
    }
}

type Partitioned = (
    DfsMetadataTableSnapshotTables,
    Vec<Value>,
    Vec<RequiredNativeWork>,
);

fn partition(
    rows: DfsMetadataTableSnapshotTables,
    operations: Vec<Value>,
    required: Vec<RequiredNativeWork>,
    shard: &str,
) -> Partitioned {
    partition_capture(
        rows,
        operations,
        required,
        &catalog(),
        &[file_system()],
        &DfsMetadataShardId::new(shard),
    )
    .unwrap()
}

/// What stops the shard's conversion before it settles its own jobs.
fn stopping((rows, operations, required): Partitioned, shard: &str) -> Vec<OriginalWorkRef> {
    unresolved_beside_recorded_jobs_for_test(source(shard), rows, &operations, required).unwrap()
}

fn families(work: &[RequiredNativeWork]) -> Vec<&str> {
    work.iter().map(|work| work.family.as_str()).collect()
}

/// An original adoption request of `authority`, moving `source_path` to
/// `object_path`, as the legacy projection retains it.
fn adoption(authority: &str, source_path: &str, object_path: &str) -> Value {
    let mut moved = object("adopted", object_path, DfsObjectKind::File);
    moved.namespace_link_count = None;
    let mut row = json!({
        "file_system_id": file_system(), "requester_node_id": node("combined-requester"),
        "request_id": format!("adopt-{object_path}"), "operation_id": format!("op-{object_path}"),
        "authority_shard_id": authority,
        "operation": {"adopt-object-at-path": {
            "object": moved, "source_path": source_path, "pin_transfer_id": null, "versions": [],
            "version_pins": [], "version_pin_set_fingerprint": null, "receipts": [], "intents": [],
            "intent_receipts": [], "intent_receipt_retention": {}, "content_reference_outcomes": [],
            "content_reference_outcome_retention": null, "conflicts": [], "advisory_lock_state": null,
        }},
        "command_digest": "", "status": "pending", "result": null, "posix_coordination_result": null,
        "recovery_attempts": 0, "last_recovery_error": null,
        "created_at": "1970-01-01T00:00:00Z", "updated_at": "1970-01-01T00:00:01Z",
    });
    row["command_digest"] =
        json!(crate::legacy_wire::original_namespace_request_digest(&row).unwrap());
    row
}

/// A retired advisory-lock preflight of `authority`, completed or pending,
/// as the legacy projection retains it.
fn preflight(authority: &str, status: &str) -> Value {
    let mut row = adoption(
        authority,
        "/root-source",
        &format!("/{authority}-preflight"),
    );
    row["request_id"] = json!(format!("preflight-{authority}-{status}"));
    row["status"] = json!(status);
    let fenced = json!({"object_id": DfsObjectId::from_legacy_named("preflighted").unwrap(),
        "state_fingerprint": "original-source-fence"});
    row["operation"] = json!({"advisory-lock-migration-prepare": {
        "path": "/target/file", "source_path": "/source/file",
        "object_id": fenced["object_id"], "state_fingerprint": fenced["state_fingerprint"],
    }});
    if status == "completed" {
        row["result"] = json!("applied");
        row["posix_coordination_result"] = json!({"advisory-lock-migration-prepared": fenced});
    }
    row["command_digest"] =
        json!(crate::legacy_wire::original_namespace_request_digest(&row).unwrap());
    row
}

fn work(family: &str, source_row: Value) -> RequiredNativeWork {
    RequiredNativeWork {
        family: family.into(),
        reason: "original native work".into(),
        source_row,
    }
}

fn metadata_payload(
    shard: &str,
    operation_id: &str,
    expected_paths: &[&str],
    effects: Vec<DfsPreparedMetadataEffect>,
) -> DfsPreparedOperationPayload {
    DfsPreparedOperationPayload::for_metadata_effects(
        DfsPreparedOperationScope {
            file_system_id: file_system(),
            shard_id: DfsMetadataShardId::new(shard),
            owner_epoch: 2,
            operation_id: operation_id.into(),
        },
        Vec::new(),
        expected_paths
            .iter()
            .map(|path| sinomo_dfs::DfsNamespacePathOccupantExpectation {
                path: (*path).into(),
                expected_object_id: None,
            })
            .collect(),
        effects,
        OffsetDateTime::UNIX_EPOCH,
    )
    .unwrap()
}

/// A prepared V7 operation of `shard` that records an access to `object`,
/// with the pending row the legacy projection keeps for it.
fn prepared_access(shard: &str, object: &DfsObjectMetadata) -> (DfsPreparedOperationRecord, Value) {
    let mut accessed = object.clone();
    accessed.accessed_at_unix_nanos = 1;
    let payload = metadata_payload(
        shard,
        &format!("{shard}-records-access"),
        &[],
        vec![DfsPreparedMetadataEffect::PutObject { object: accessed }],
    );
    let record = DfsPreparedOperationRecord {
        digest: DfsPreparedOperationDigest::at_sequence(&payload, 7).unwrap(),
        payload: Arc::new(payload),
        status: DfsPreparedOperationStatus::Prepared,
        committed_entry: None,
    };
    let row = json!({
        "payload": record.payload, "status": "prepared",
        "source_protocol_family": "prepared-operation-v7",
        "source_operation_digest": record.digest.payload_digest,
        "source_prepare_sequence": 7, "source_v7_exact_digest": record.digest,
        "source_v7_payload_exact": true, "source_v7_commit_reference": null,
    });
    (record, row)
}

/// An in-flight write to `object`, as the legacy projection keeps it: an
/// intent-only preparation bound to the capturing shard, beside the intent.
fn staging_write(bound: &str, object: &DfsObjectMetadata) -> (DfsWriteIntentRecord, Value) {
    let intent = DfsWriteIntentRecord {
        publication: DfsWritePublication::Current,
        intent_id: format!("intent-{}", object.object_id),
        file_system_id: file_system(),
        object_id: object.object_id.clone(),
        base_version: Some(object.version.clone()),
        target_version: DfsVersion::from_legacy_named("staged-version").unwrap(),
        coordinator_node_id: node("combined-writer"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 2,
        desired_replica_node_ids: Vec::new(),
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        status: DfsWriteIntentStatus::Staging,
        content_hash: None,
        content_size_bytes: None,
        object_metadata: Some(object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    let payload = metadata_payload(
        bound,
        &format!("write:{}", intent.intent_id),
        &[],
        vec![DfsPreparedMetadataEffect::PutIntent {
            intent: intent.clone(),
        }],
    );
    let row = json!({
        "payload": payload, "status": "prepared", "terminal_at": null,
        "source_operation_digest": "intent-digest", "source_prepare_sequence": null,
        "source_intent_only": true, "retired_reservation": true,
    });
    (intent, row)
}

/// Requests and adoptions `root` owns, interrupted or retained when the
/// copy was last written, are left to `root`; the shard keeps its own.
#[test]
fn a_combined_copy_leaves_another_shards_native_work_to_it() {
    let prepared_adoption = json!({"original-prepare": "root-bytes"});
    let root_adoption_body = metadata_payload(
        "root",
        "root-prepared-adoption",
        &["/users/target"],
        Vec::new(),
    );
    let root_adoption = json!({
        "payload": root_adoption_body, "status": "prepared", "source_operation_digest": "root-digest",
        "source_prepare_sequence": 3, "retired_reservation": true,
        "source_adoption_original": prepared_adoption, "source_adoption_requests": [],
    });
    let roots = vec![
        work(
            "namespace_requests.completed_preflight",
            preflight("root", "completed"),
        ),
        work(
            "namespace_requests.old_preflight",
            preflight("root", "pending"),
        ),
        work(
            "namespace_requests.old_adoption",
            adoption("root", "/root-source", "/root-moved"),
        ),
        work("prepared_operations.old_adoption", prepared_adoption),
    ];
    let partitioned = partition(
        combined_rows(),
        vec![root_adoption.clone()],
        roots.clone(),
        "users",
    );
    assert!(
        partitioned.1.is_empty(),
        "root's prepared adoption is root's"
    );
    assert!(partitioned.2.is_empty(), "{:?}", families(&partitioned.2));
    assert!(stopping(partitioned, "users").is_empty());

    // The same rules give them all to root.
    let (_, _, kept) = partition(combined_rows(), vec![root_adoption], roots, "root");
    assert_eq!(kept.len(), 4, "{:?}", families(&kept));

    // An adoption into users, and one out of users, are users' own.
    let into = adoption("users", "/root-source", "/users/moved");
    let out_of = adoption("root", "/users/source", "/root-moved");
    let (_, _, kept) = partition(
        combined_rows(),
        Vec::new(),
        vec![
            work("namespace_requests.old_adoption", into.clone()),
            work("namespace_requests.old_adoption", out_of.clone()),
        ],
        "users",
    );
    assert_eq!(
        kept.iter().map(|work| &work.source_row).collect::<Vec<_>>(),
        vec![&into, &out_of]
    );

    // From root's side, users' own work is users'.
    let (_, _, kept) = partition(
        combined_rows(),
        Vec::new(),
        vec![
            work(
                "namespace_requests.completed_preflight",
                preflight("users", "completed"),
            ),
            work(
                "namespace_requests.old_adoption",
                adoption("users", "/users/a", "/users/b"),
            ),
        ],
        "root",
    );
    assert!(kept.is_empty(), "{:?}", families(&kept));
}

/// Operations `root` prepared and had not settled, including ones whose
/// footprint names a users row and an in-flight write to a root file, are
/// left to `root`; the shard keeps its own.
#[test]
fn a_combined_copy_leaves_another_shards_unsettled_operations_to_it() {
    let (root_v7, root_v7_row) = prepared_access("root", &users_file());
    let (root_intent, root_write) = staging_write("users", &root_file());
    let mut rows = combined_rows();
    rows.prepared_operations_v7 = vec![root_v7.clone()];
    rows.intents = vec![root_intent];
    let partitioned = partition(
        rows.clone(),
        vec![root_v7_row.clone(), root_write.clone()],
        Vec::new(),
        "users",
    );
    assert!(partitioned.0.prepared_operations_v7.is_empty());
    assert!(partitioned.0.intents.is_empty());
    assert!(partitioned.1.is_empty());
    assert!(partitioned.2.is_empty(), "{:?}", families(&partitioned.2));
    assert!(stopping(partitioned, "users").is_empty());

    // Users keeps what it prepared itself, which its own recovery decides.
    let (users_v7, users_v7_row) = prepared_access("users", &users_file());
    rows.prepared_operations_v7.push(users_v7.clone());
    let (tables, operations, _) = partition(
        rows,
        vec![root_v7_row.clone(), root_write, users_v7_row.clone()],
        Vec::new(),
        "users",
    );
    assert_eq!(tables.prepared_operations_v7, vec![users_v7.clone()]);
    assert_eq!(operations, vec![users_v7_row.clone()]);

    // From root's side, users' preparation and an in-flight write to a
    // users file, bound to root's capture, are users'.
    let (users_intent, users_write) = staging_write("root", &users_file());
    let mut rows = combined_rows();
    rows.prepared_operations_v7 = vec![root_v7.clone(), users_v7];
    rows.intents = vec![users_intent];
    let (tables, operations, required) = partition(
        rows,
        vec![root_v7_row.clone(), users_v7_row, users_write],
        Vec::new(),
        "root",
    );
    assert_eq!(tables.prepared_operations_v7, vec![root_v7]);
    assert!(tables.intents.is_empty());
    assert_eq!(operations, vec![root_v7_row]);
    assert!(required.is_empty(), "{:?}", families(&required));
}

/// A shard's own retired advisory-lock preflights, completed or pending, are
/// outcomes of its capture, not work that stops it. An own pending adoption
/// still stops it, as does a preflight whose recorded identity does not
/// verify.
#[test]
fn a_shards_own_retired_preflights_do_not_stop_it() {
    let own = vec![
        work(
            "namespace_requests.completed_preflight",
            preflight("users", "completed"),
        ),
        work(
            "namespace_requests.old_preflight",
            preflight("users", "pending"),
        ),
    ];
    let partitioned = partition(combined_rows(), Vec::new(), own.clone(), "users");
    assert_eq!(partitioned.2.len(), 2, "{:?}", families(&partitioned.2));
    let stops = stopping(partitioned, "users");
    assert!(
        stops.is_empty(),
        "{:?}",
        stops.iter().map(|work| &work.family).collect::<Vec<_>>()
    );

    let mut tampered = preflight("users", "pending");
    tampered["operation"]["advisory-lock-migration-prepare"]["path"] = json!("/elsewhere");
    let mut work_left = own;
    work_left.push(work("namespace_requests.old_preflight", tampered));
    work_left.push(work(
        "namespace_requests.old_adoption",
        adoption("users", "/root-source", "/users/moved"),
    ));
    let stops = stopping(
        partition(combined_rows(), Vec::new(), work_left, "users"),
        "users",
    );
    assert_eq!(
        stops
            .iter()
            .map(|work| work.family.as_str())
            .collect::<Vec<_>>(),
        vec![
            "namespace_requests.old_preflight",
            "namespace_requests.old_adoption"
        ]
    );
}

/// Retry floors and outcome receipts of an object the copy no longer places
/// at any path are kept by every shard's capture, and its lock aggregate
/// stays in the copy. A row whose object keeps a row while it exists, here
/// an inode, still stops the capture without one.
#[test]
fn a_floor_or_receipt_of_an_object_no_row_places_is_kept_by_every_shard() {
    let gone = DfsObjectId::from_legacy_named("compacted-and-replaced").unwrap();
    let coordinator = node("combined-writer");
    let receipt = DfsWriteIntentReceipt {
        publication: DfsWritePublication::Current,
        intent_id: "gone-intent".into(),
        file_system_id: file_system(),
        object_id: gone.clone(),
        base_version: None,
        target_version: DfsVersion::from_legacy_named("gone-version").unwrap(),
        coordinator_node_id: coordinator.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 2,
        desired_replica_node_ids: Vec::new(),
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        status: DfsWriteIntentStatus::Committed,
        content_hash: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        terminal_at: OffsetDateTime::UNIX_EPOCH,
        retained_at: OffsetDateTime::UNIX_EPOCH,
    };
    let reference = DfsContentReferenceOutcomeRetention {
        file_system_id: file_system(),
        max_evicted_operation_order_token: Some("reference-token".into()),
        opaque_operation_history_evicted: false,
    };
    let mut rows = combined_rows();
    rows.intent_receipts.push(receipt.clone());
    rows.advisory_lock_states
        .push(DfsAdvisoryLockStateRecord::empty(
            file_system(),
            gone.clone(),
            2,
        ));
    let retention = &mut rows.intent_receipt_retention;
    retention
        .max_evicted_counter_by_object_and_coordinator
        .insert(gone.clone(), BTreeMap::from([(coordinator.clone(), 5)]));
    retention
        .max_evicted_operation_order_token_by_object_and_coordinator
        .insert(
            gone.clone(),
            BTreeMap::from([(coordinator, "order-token".to_owned())]),
        );
    retention
        .opaque_intent_history_evicted_object_ids
        .insert(gone.clone());
    rows.namespace_receipt_retention
        .content_reference_outcome_retention
        .insert(gone.clone(), reference.clone());
    for shard in ["users", "root"] {
        let partitioned = partition(rows.clone(), Vec::new(), Vec::new(), shard);
        let tables = &partitioned.0;
        assert!(
            partitioned.2.is_empty(),
            "{shard}: {:?}",
            families(&partitioned.2)
        );
        assert_eq!(tables.intent_receipts, vec![receipt.clone()], "{shard}");
        assert_eq!(
            tables.intent_receipt_retention, rows.intent_receipt_retention,
            "{shard}"
        );
        assert_eq!(
            tables
                .namespace_receipt_retention
                .content_reference_outcome_retention,
            BTreeMap::from([(gone.clone(), reference.clone())]),
            "{shard}"
        );
        assert!(tables.advisory_lock_states.is_empty(), "{shard}");
        DfsMetadataCandidateState::new(tables.clone()).unwrap();
        assert!(stopping(partitioned, shard).is_empty(), "{shard}");
    }

    let mut orphan = object("compacted-and-replaced", "/users/old", DfsObjectKind::File);
    orphan.object_id = gone;
    rows.inodes.push(DfsInodeRecord::from(&orphan));
    let (_, _, required) = partition(rows, Vec::new(), Vec::new(), "users");
    assert_eq!(families(&required), vec!["inodes"]);
}
