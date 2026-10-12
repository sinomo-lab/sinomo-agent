use sinomo_kv::*;

fn source(count: usize, value_bytes: usize) -> KvAuthoritativeRead<KvSnapshot> {
    let entries = (0..count)
        .map(|index| KvEntry {
            key: format!("/scope/{index:06}"),
            value: vec![index as u8; value_bytes],
            create_revision: KvRevision(1),
            modify_revision: KvRevision(7),
            version: KvVersion(1),
            lease_id: None,
        })
        .collect();
    KvAuthoritativeRead {
        value: KvSnapshot {
            revision: KvRevision(7),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: vec![],
            events: vec![],
            committed_requests: vec![],
            expired_requests: vec![],
        },
        authority: KvAuthorityProof {
            term: 4,
            commit_index: 12,
            revision: KvRevision(7),
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2,
            },
        },
    }
}

#[test]
fn complete_prefix_keeps_every_entry_beyond_ordinary_payload_bound() {
    let mut read = source(513, 16 * 1024);
    read.value.entries.push(KvEntry {
        key: "/unrelated/value".into(),
        ..read.value.entries[0].clone()
    });
    assert!(matches!(
        project_cluster_kv_authoritative_range_prefix(&read, "/scope/"),
        Err(KvError::RangePayloadLimit { .. })
    ));
    let complete =
        project_cluster_kv_authoritative_complete_range_prefix(&read, "/scope/").unwrap();
    assert_eq!(complete.authority, read.authority);
    assert_eq!(complete.value, read.value.entries[..513]);
    // A later owned snapshot cannot mix its new data into this proof's result.
    read.value.entries[0].value[0] = 99;
    read.value.revision = KvRevision(8);
    assert_eq!(complete.value[0].value[0], 0);
    assert_eq!(complete.authority.revision, KvRevision(7));
    assert!(project_cluster_kv_authoritative_complete_range_prefix(&read, "/scope/").is_err());
    assert_eq!(MAX_KV_COMMAND_PAYLOAD_BYTES, 8 * 1024 * 1024);
    assert_eq!(CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES, 8 * 1024 * 1024);
}

#[test]
fn complete_prefix_keeps_every_entry_beyond_ordinary_cardinality_bound() {
    let read = source(4097, 1);
    assert!(matches!(
        project_cluster_kv_authoritative_range_prefix(&read, "/scope/"),
        Err(KvError::RangeLimit { .. })
    ));
    assert_eq!(
        project_cluster_kv_authoritative_complete_range_prefix(&read, "/scope/")
            .unwrap()
            .value
            .len(),
        4097
    );
    assert!(
        project_cluster_kv_authoritative_complete_range_prefix(
            &source(CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES + 1, 0),
            "/scope/"
        )
        .is_err()
    );
}

#[test]
fn complete_prefix_rejects_invalid_scope_order_revision_and_authority() {
    let original =
        project_cluster_kv_authoritative_complete_range_prefix(&source(2, 1), "/scope/").unwrap();
    let mut altered = original.clone();
    altered.value[0].key = "/outside/a".into();
    assert!(
        validate_cluster_kv_authoritative_complete_range_prefix_read("/scope/", &altered).is_err()
    );
    altered = original.clone();
    altered.value.reverse();
    assert!(
        validate_cluster_kv_authoritative_complete_range_prefix_read("/scope/", &altered).is_err()
    );
    altered = original.clone();
    altered.value[1] = altered.value[0].clone();
    assert!(
        validate_cluster_kv_authoritative_complete_range_prefix_read("/scope/", &altered).is_err()
    );
    altered = original.clone();
    altered.value[0].modify_revision = KvRevision(8);
    assert!(
        validate_cluster_kv_authoritative_complete_range_prefix_read("/scope/", &altered).is_err()
    );
    altered = original.clone();
    altered.authority.term = 0;
    assert!(
        validate_cluster_kv_authoritative_complete_range_prefix_read("/scope/", &altered).is_err()
    );
    let mut malformed = source(2, 1);
    malformed.value.entries.reverse();
    assert!(project_cluster_kv_authoritative_complete_range_prefix(&malformed, "/scope/").is_err());
}

#[test]
fn complete_prefix_rejects_oversized_values_and_snapshots_without_partial_success() {
    assert!(
        project_cluster_kv_authoritative_complete_range_prefix(
            &source(1, MAX_KV_VALUE_BYTES + 1),
            "/scope/"
        )
        .is_err()
    );
    assert!(
        project_cluster_kv_authoritative_complete_range_prefix(
            &source(17, MAX_KV_VALUE_BYTES),
            "/scope/"
        )
        .is_err()
    );
}
