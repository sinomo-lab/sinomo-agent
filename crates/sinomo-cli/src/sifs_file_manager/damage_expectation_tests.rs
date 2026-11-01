//! DATA-only guards. No successful native holder, receipt or deletion is made.
use super::*;
fn data() -> (DfsStat, String, DfsLocalBlobDamageExpectation) {
    let local = NodeId::from_legacy_named("damage-data-local").unwrap();
    let other = NodeId::from_legacy_named("damage-data-other").unwrap();
    let stat = DfsStat {
        file_system_id: sinomo_runtime::dfs::DfsFileSystemId::from_legacy_named("damage-data-fs")
            .unwrap(),
        object_id: sinomo_runtime::dfs::DfsObjectId::from_legacy_named("damage-data-object")
            .unwrap(),
        path: "/services/damage-data-only.bin".into(),
        kind: DfsObjectKind::File,
        version: sinomo_runtime::dfs::DfsVersion::from_legacy_named("damage-data-version").unwrap(),
        content_hash: Some(format!("blake3:{}", "a".repeat(64))),
        size_bytes: Some(1048576),
        link_count: 1,
        mode: 0o600,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 1,
        modified_at_unix_nanos: 1,
        created_at_unix_nanos: 1,
        changed_at_unix_nanos: 1,
        policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
        current_locations: vec![local.clone(), other],
    };
    let local = local.to_string();
    let expected = DfsLocalBlobDamageExpectation {
        file_system_id: stat.file_system_id.to_string(),
        object_id: stat.object_id.to_string(),
        version: stat.version.to_string(),
        content_hash: stat.content_hash.clone().unwrap(),
        size_bytes: stat.size_bytes.unwrap(),
        node_id: local.clone(),
        path: stat.path.clone(),
    };
    (stat, local, expected)
}
#[test]
fn damage_expectation_accepts_exact_data_and_preserves_legacy_none() {
    let (mut stat, local, expected) = data();
    assert!(validate_local_blob_damage_expectation(&stat, &local, Some(&expected)).is_ok());
    stat.current_locations.truncate(1);
    assert!(validate_local_blob_damage_expectation(&stat, &local, None).is_ok());
}
#[test]
fn damage_expectation_rejects_every_different_subject_field() {
    let (stat, local, expected) = data();
    for field in [
        "file_system_id",
        "object_id",
        "version",
        "content_hash",
        "size_bytes",
        "node_id",
        "path",
    ] {
        let mut value = serde_json::to_value(&expected).unwrap();
        value[field] = if field == "size_bytes" {
            serde_json::json!(1048577)
        } else {
            serde_json::json!("different-actual-subject")
        };
        let changed: DfsLocalBlobDamageExpectation = serde_json::from_value(value).unwrap();
        assert!(
            validate_local_blob_damage_expectation(&stat, &local, Some(&changed)).is_err(),
            "{field}"
        );
    }
}
#[test]
fn damage_expectation_refuses_last_known_location_and_missing_size() {
    let (mut stat, local, expected) = data();
    stat.current_locations.truncate(1);
    assert!(validate_local_blob_damage_expectation(&stat, &local, Some(&expected)).is_err());
    let (mut stat, local, expected) = data();
    stat.size_bytes = None;
    assert!(validate_local_blob_damage_expectation(&stat, &local, Some(&expected)).is_err());
}
#[test]
fn damage_expectation_json_is_closed_and_requires_full_tuple() {
    let (_, _, expected) = data();
    let mut value = serde_json::to_value(&expected).unwrap();
    value["fallback"] = serde_json::json!(true);
    assert!(serde_json::from_value::<DfsLocalBlobDamageExpectation>(value).is_err());
    let mut value = serde_json::to_value(&expected).unwrap();
    value.as_object_mut().unwrap().remove("version");
    assert!(serde_json::from_value::<DfsLocalBlobDamageExpectation>(value).is_err());
}
