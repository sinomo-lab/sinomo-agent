//! Normal Main write interruption, proved by its exact durable prepare/canonical cut.
//!
//! A commit must reach every member of the shard's write set. A member the
//! primary cannot reach is removed unless that would take the write set below
//! the shard's W floor; then it stays, and the write stays prepared until the
//! member returns or a recruit replaces it. This probe therefore requires the
//! serving shard's write set to be every configured member, exactly at the W
//! floor its placement rules give it, with no other write-set capable member
//! left to recruit: only the interrupted member's return can complete the
//! write.
use super::*;
use sinomo_runtime::dfs::{
    DfsMetadataShardConfig, DfsMetadataShardId, DfsObjectMetadata, DfsPreparedMetadataEffect,
    DfsPreparedOperationRecord, DfsPreparedOperationStatus, DfsStableWriteOperationReconciliation,
    DfsWriteIntentStatus, DfsWriteOptions, LocalSobjMetadataBackend,
};

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_partition_write(ctx) {
        id: "cluster.dfs.partition-write",
        summary: "Capture, deny publication while a write-set member at the W floor is cut off, and recover the same ordinary write identity",
        tags: ["cluster-dfs", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime, SinomoTestRequirement::ClusterKvSnapshot],
        run: { run_partition_write(ctx) }
    }
}

fn fail(reason: impl Into<String>) -> SinomoTestError {
    SinomoTestError::failed(reason.into())
}

fn parse<T: serde::de::DeserializeOwned>(
    ctx: &SinomoTestContext,
    key: &str,
) -> Result<T, SinomoTestError> {
    serde_json::from_str(ctx.required_parameter(key)?)
        .map_err(|error| fail(format!("invalid {key}: {error}")))
}

fn original_pending_intent_matches(
    intent: &dfs::DfsWriteIntentRecord,
    original: &DfsObjectMetadata,
    operation_id: &str,
    target: &DfsVersion,
    hash: &str,
    assignment: &DfsMetadataShardConfig,
    options: &DfsWriteOptions,
) -> bool {
    intent.publication.is_current()
        && matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        )
        && intent.file_system_id == original.file_system_id
        && intent.object_id == original.object_id
        && intent.operation_id.as_deref() == Some(operation_id)
        && intent.base_version.as_ref() == Some(&original.version)
        && intent.target_version == *target
        && intent.coordinator_node_id == assignment.primary_node_id
        && intent.owner_epoch == assignment.epoch
        && intent.content_hash.as_deref() == Some(hash)
        && intent.policy == options.policy
        && dfs::validate_replication_plan(&intent.desired_replica_node_ids, &intent.policy).is_ok()
        && intent
            .desired_replica_node_ids
            .iter()
            .all(|node| options.replica_candidates.contains(node))
}

fn original_intent_matches(
    record: &DfsPreparedOperationRecord,
    original: &DfsObjectMetadata,
    operation_id: &str,
    target: &DfsVersion,
    hash: &str,
    assignment: &DfsMetadataShardConfig,
    options: &DfsWriteOptions,
) -> bool {
    if record.digest.file_system_id != assignment.file_system_id
        || record.digest.shard_id.0 != assignment.shard_id
        || record.digest.owner_epoch != assignment.epoch
    {
        return false;
    }
    if record
        .payload
        .prepared_intent
        .iter()
        .chain(record.payload.terminal_intent.iter())
        .chain(
            record
                .payload
                .metadata_effects
                .iter()
                .filter_map(|effect| match effect {
                    DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                    _ => None,
                }),
        )
        .any(|intent| {
            intent.file_system_id == original.file_system_id
                && intent.object_id == original.object_id
                && intent.operation_id.as_deref() == Some(operation_id)
                && matches!(
                    intent.status,
                    DfsWriteIntentStatus::Rejected | DfsWriteIntentStatus::Aborted
                )
        })
    {
        return false;
    }
    // A terminal intent is not a pending-write witness. Only the real active
    // preparation or active PutIntent effect can qualify this blocked phase.
    // record.validate() below independently checks the complete typed body.
    record
        .payload
        .prepared_intent
        .iter()
        .chain(
            record
                .payload
                .metadata_effects
                .iter()
                .filter_map(|effect| match effect {
                    DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                    _ => None,
                }),
        )
        .any(|intent| {
            original_pending_intent_matches(
                intent,
                original,
                operation_id,
                target,
                hash,
                assignment,
                options,
            )
        })
}

fn original_input_identity_matches(
    actual_hash: &str,
    actual_size: u64,
    expected_hash: &str,
    expected_size: u64,
) -> bool {
    actual_hash == expected_hash && actual_size == expected_size
}

fn verify_sealed_original_input(
    data: &SinomoDataDir,
    expected_hash: &str,
    expected_size: u64,
    deadline: Instant,
) -> Result<(String, u64), SinomoTestError> {
    if Instant::now() >= deadline {
        return Err(fail(
            "original input witness exhausted the existing probe clock",
        ));
    }
    // Ordinary byte input custody only: no object manifest, receipt, native
    // publication capability or metadata authority is minted by this read.
    let content = dfs::LocalContentStore::new(data.dfs_dir().join("content"))
        .map_err(|error| fail(format!("cannot inspect the sealed original input: {error}")))?;
    let mut stream = content
        .open_blob_stream(expected_hash)
        .map_err(|error| fail(format!("sealed original input is unavailable: {error}")))?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if Instant::now() >= deadline {
            return Err(fail(
                "original input witness exhausted the existing probe clock",
            ));
        }
        let read = stream
            .read(&mut buffer)
            .map_err(|error| fail(format!("cannot read sealed original input: {error}")))?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| fail("original input length overflow"))?;
        hasher.update(&buffer[..read]);
    }
    let hash = format!("blake3:{}", hasher.finalize().to_hex());
    if !original_input_identity_matches(&hash, size, expected_hash, expected_size)
        || Instant::now() >= deadline
    {
        return Err(fail(
            "sealed original input changes the exact hash/size or exceeds the existing probe clock",
        ));
    }
    Ok((hash, size))
}

fn run_partition_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let phase = ctx.required_parameter("phase")?.to_string();
    if !matches!(phase.as_str(), "baseline" | "blocked" | "recover") {
        return Err(fail("phase must be baseline, blocked or recover"));
    }
    let path = ctx.required_parameter("path")?.to_string();
    scoped_sifs_test_run_root(&path)?;
    let operation_id = ctx.required_parameter("operation-id")?.to_string();
    dfs::validate_write_operation_id(&operation_id).map_err(|error| fail(error.to_string()))?;
    let payload = required_payload_bytes(ctx)?;
    let hash = format!("blake3:{}", blake3::hash(&payload).to_hex());
    let unavailable = NodeId::new(ctx.required_parameter("unavailable-node-id")?)
        .map_err(|error| fail(error.to_string()))?;
    let started = Instant::now();
    let timeout = expect_timeout(ctx)?;
    let deadline = started + timeout;
    // A write-set commit waits for its members until its caller stops
    // waiting. While the member is cut off that wait is the caller's whole
    // budget, so the blocked phase gives the write half of this probe's clock
    // and keeps the rest to read the durable witness the write left.
    let write_deadline = if phase == "blocked" {
        started + timeout / 2
    } else {
        deadline
    };
    let authority = ctx
        .authoritative_cluster_snapshot()
        .ok_or_else(|| fail("exact current assignment proof is unavailable"))?;
    let observation = open_observation_with_deadline(ctx, deadline)?;
    let assignment = observation
        .metadata_shard_for_path_at_authority(&path, &authority)
        .map_err(fail)?
        .ok_or_else(|| fail("path has no actual assigned metadata shard"))?;
    let caller = NodeId::new(observation.node_label()).map_err(|error| fail(error.to_string()))?;
    let configured = std::iter::once(assignment.primary_node_id.clone())
        .chain(assignment.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    // The same admitted snapshot gives the shard, its write set and the rules
    // that size it.
    let snapshot = authority.snapshot();
    let rules = DfsShardRulesView::read(&snapshot)?;
    let membership = rules.membership(&snapshot, &assignment)?;
    let participants = membership
        .write_set
        .ok_or_else(|| fail("the Services shard has not switched to the write-set protocol"))?
        .into_iter()
        .collect::<BTreeSet<_>>();
    ctx.record("write_set", &participants);
    ctx.record("w_floor", membership.w_floor);
    ctx.record("write_set_member_count", rules.write_set_members);
    if caller != assignment.primary_node_id || assignment.shard_id != "services" {
        return Err(fail("probe must run at the actual Services primary"));
    }
    if participants != configured
        || participants.len() != membership.w_floor
        || participants.len() != rules.write_set_members
        || !participants.contains(&unavailable)
        || unavailable == caller
    {
        return Err(fail(format!(
            "the interrupted node must be a non-primary member of a write set that is every configured member, exactly at the shard's W floor, with no member left to recruit: write set {}, configured {}, W floor {}, write-set capable members {}",
            participants.len(),
            configured.len(),
            membership.w_floor,
            rules.write_set_members,
        )));
    }
    let data = SinomoDataDir::new(ctx.data_dir());
    let shard = DfsMetadataShardId::new(&assignment.shard_id);
    let root = dfs::local_dfs_metadata_shard_path(&data, &shard);
    let capture = || {
        LocalSobjMetadataBackend::open_for_inspection(shard.clone(), root.clone())
            .and_then(|backend| backend.export_table_snapshot())
            .map_err(|error| fail(error.to_string()))
    };
    let before = capture()?;
    let current = before
        .tables
        .objects
        .iter()
        .find(|row| row.path == path && row.kind == dfs::DfsObjectKind::File)
        .cloned()
        .ok_or_else(|| fail("probe requires its genuinely seeded current file before the cut"))?;
    let backend = open_backend_with_deadline(ctx, deadline)?;
    ctx.record("phase", &phase);
    ctx.record("path", &path);
    ctx.record("operation_id", &operation_id);
    ctx.record("assignment", &assignment);
    ctx.record("unavailable_node_id", &unavailable);
    ctx.record("caller_node_id", &caller);
    ctx.record("target_root", root.display().to_string());
    if phase == "baseline" {
        // The fault must interrupt the new original write, not a late receipt
        // publication for the seed. Prove setup through the real Main barrier
        // within this same deadline; the tested write keeps ordinary W/N options.
        let seed_sync = backend.sync_file_until(&path, deadline).map_err(fail)?;
        ctx.record("seed_sync", &seed_sync);
        let acknowledged = seed_sync
            .metadata_certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if seed_sync.target.file_system_id != current.file_system_id
            || seed_sync.target.object_id != current.object_id
            || seed_sync.target.version != current.version
            || Some(seed_sync.target.content_hash.as_str()) != current.content_hash.as_deref()
            || seed_sync
                .metadata_certificate
                .target
                .expected_shard
                .as_ref()
                != Some(&assignment)
            || acknowledged != participants
        {
            return Err(fail(
                "seed barrier did not certify the original file on the whole write set",
            ));
        }
        let settled = capture()?;
        let settled_current = settled
            .tables
            .objects
            .iter()
            .find(|row| row.object_id == current.object_id)
            .ok_or_else(|| fail("seed barrier lost the original canonical object"))?;
        if settled_current.version != current.version
            || settled_current.content_hash != current.content_hash
            || settled_current.content_size_bytes != current.content_size_bytes
        {
            return Err(fail("seed barrier changed the original file content"));
        }
        let options = backend
            .ordinary_write_options_until(&path, deadline)
            .map_err(fail)?;
        if !options.wait_for_commit || options.foreground_replica_repair {
            return Err(fail(
                "baseline must retain ordinary committed write options",
            ));
        }
        ctx.record("original", settled_current);
        ctx.record("write_options", &options);
        ctx.record("prefix", &settled.prefix_proof);
        ctx.record("new_content_hash", &hash);
        ctx.record("new_size_bytes", payload.len());
        return Ok(SinomoTestOutcome::passed(
            "captured the genuine current file, its whole write set at the W floor and ordinary write inputs",
        ));
    }
    let expected_assignment: DfsMetadataShardConfig = parse(ctx, "original-assignment-json")?;
    let original: DfsObjectMetadata = parse(ctx, "original-object-json")?;
    let options: DfsWriteOptions = parse(ctx, "original-options-json")?;
    if assignment != expected_assignment
        || original.path != path
        || original.file_system_id != assignment.file_system_id
        || original.object_id != current.object_id
        || !options.wait_for_commit
        || options.foreground_replica_repair
    {
        return Err(fail(
            "original operation DATA changed its full assignment, object scope or ordinary options",
        ));
    }
    let target = dfs::stable_write_target_version(&caller, &original.object_id, &operation_id);
    ctx.record("original", &original);
    ctx.record("write_options", &options);
    ctx.record("expected_target_version", &target);
    ctx.record("new_content_hash", &hash);
    ctx.record("new_size_bytes", payload.len());
    let result = backend
        .write_file_with_original_operation_until(
            &path,
            &payload,
            &operation_id,
            options.clone(),
            write_deadline,
        )
        .map_err(fail)?;
    ctx.record("write_return", format!("{result:?}"));
    ctx.record("write_wait_ms", started.elapsed().as_millis() as u64);
    let after = capture()?;
    ctx.record("prefix", &after.prefix_proof);
    let actual = after
        .tables
        .objects
        .iter()
        .find(|row| row.object_id == original.object_id)
        .ok_or_else(|| fail("actual primary lost the original canonical object"))?;
    ctx.record("canonical", actual);
    if phase == "blocked" {
        if result.is_ok() {
            return Err(fail(
                "write published while a write-set member at the W floor was disconnected",
            ));
        }
        let matching = after
            .tables
            .prepared_operations_v7
            .iter()
            .filter(|record| {
                original_intent_matches(
                    record,
                    &original,
                    &operation_id,
                    &target,
                    &hash,
                    &assignment,
                    &options,
                )
            })
            .collect::<Vec<_>>();
        ctx.record("original_prepare_records", &matching);
        for record in &matching {
            record.validate().map_err(|error| fail(error.to_string()))?;
        }
        if !matching.iter().any(|record| {
            record.status == DfsPreparedOperationStatus::Prepared
                && record.digest.prepare_sequence > before.materialized_through_sequence
        }) || actual.version != original.version
            || actual.content_hash != original.content_hash
            || actual.content_size_bytes != original.content_size_bytes
            || after.tables.versions.iter().any(|row| {
                row.object_id == original.object_id && row.version == target && row.visible
            })
            || matching.iter().any(|record| {
                record.status == DfsPreparedOperationStatus::Committed
                    && record
                        .payload
                        .transaction
                        .objects
                        .iter()
                        .any(|row| row.object_id == original.object_id && row.version == target)
            })
        {
            return Err(fail(
                "write error lacks an exact original durable Prepared/nonvisible publication witness",
            ));
        }
        let (sealed_hash, sealed_size) =
            verify_sealed_original_input(&data, &hash, payload.len() as u64, deadline)?;
        ctx.record("sealed_original_input_hash", &sealed_hash);
        ctx.record("sealed_original_input_size_bytes", sealed_size);
        // An error string is diagnostic only. The actual typed body and cut
        // above prove unavailability; no error-text or transport classification
        // can substitute for this exact operation and canonical nonvisibility.
        return Ok(SinomoTestOutcome::passed(
            "exact original write is durably Prepared and its new version remains nonvisible while the write-set member is cut off",
        ));
    }
    let stat = result
        .map_err(|error| fail(format!("same original operation did not recover: {error}")))?;
    let reconciliation = backend
        .reconcile_original_write_until(
            &path,
            &original.object_id,
            &operation_id,
            &hash,
            payload.len() as u64,
            deadline,
        )
        .map_err(fail)?
        .map_err(|error| fail(error.to_string()))?;
    ctx.record("reconciliation", &reconciliation);
    let DfsStableWriteOperationReconciliation::Applied { receipt } = reconciliation else {
        return Err(fail(
            "normal authority did not prove the original operation's exact Applied receipt",
        ));
    };
    if receipt.operation_id != operation_id
        || receipt.object_id != original.object_id
        || receipt.file_system_id != original.file_system_id
        || receipt.target_version != target
        || receipt.status != DfsWriteIntentStatus::Committed
        || receipt.content_hash.as_deref() != Some(hash.as_str())
        || receipt.content_size_bytes != Some(payload.len() as u64)
        || stat.object_id != original.object_id
        || stat.version != target
        || actual.version != target
        || actual.content_hash.as_deref() != Some(hash.as_str())
        || actual.content_size_bytes != Some(payload.len() as u64)
    {
        return Err(fail(
            "recovery changed the original caller operation/version/content tuple",
        ));
    }
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
    Ok(SinomoTestOutcome::passed(
        "same original operation recovered its actual committed receipt and canonical content after healing",
    ))
}

#[cfg(test)]
mod original_pending_guard_tests {
    use super::*;

    // Protocol DATA for the pure selection predicate, never a native receipt,
    // ACK, held owner, authenticated source or actual cluster outcome.
    fn inputs() -> (
        DfsObjectMetadata,
        DfsMetadataShardConfig,
        DfsWriteOptions,
        dfs::DfsWriteIntentRecord,
        DfsVersion,
        String,
    ) {
        let owner = NodeId::from_legacy_named("partition-predicate-owner").unwrap();
        let peer = NodeId::from_legacy_named("partition-predicate-peer").unwrap();
        let object = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("partition-predicate-object").unwrap(),
            file_system_id: dfs::sifs_file_system_id(),
            path: "/services/cluster-tests/runs/predicate/value.txt".into(),
            namespace_link_count: (dfs::DfsObjectKind::File).initial_namespace_link_count(),
            kind: dfs::DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(format!("blake3:{}", blake3::hash(b"old").to_hex())),
            content_size_bytes: Some(3),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("partition-predicate-old").unwrap(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let assignment = DfsMetadataShardConfig {
            file_system_id: object.file_system_id.clone(),
            shard_id: "services".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![peer.clone()],
            epoch: 7,
            assignment: dfs::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".into(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut policy = dfs::DfsReplicationPolicy::new(1, 2).unwrap();
        policy.required_node_ids = vec![owner.clone(), peer.clone()];
        let options = DfsWriteOptions {
            policy: policy.clone(),
            replica_candidates: vec![owner.clone(), peer.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        let target = dfs::stable_write_target_version(&owner, &object.object_id, "original");
        let hash = format!("blake3:{}", blake3::hash(b"new").to_hex());
        let intent = dfs::DfsWriteIntentRecord {
            publication: dfs::DfsWritePublication::Current,
            intent_id: "predicate-intent".into(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: target.clone(),
            coordinator_node_id: owner,
            operation_id: Some("original".into()),
            operation_order_token: None,
            owner_epoch: 7,
            desired_replica_node_ids: options.replica_candidates.clone(),
            policy,
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(hash.clone()),
            content_size_bytes: None,
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };
        (object, assignment, options, intent, target, hash)
    }

    #[test]
    fn pending_witness_requires_exact_original_body_and_active_status() {
        let (original, assignment, options, intent, target, hash) = inputs();
        let valid = |row: &dfs::DfsWriteIntentRecord| {
            original_pending_intent_matches(
                row,
                &original,
                "original",
                &target,
                &hash,
                &assignment,
                &options,
            )
        };
        assert!(valid(&intent));
        let mut changed = intent.clone();
        changed.operation_id = Some("foreign".into());
        assert!(!valid(&changed));
        let mut changed = intent.clone();
        changed.content_hash = Some(format!("blake3:{}", blake3::hash(b"different").to_hex()));
        assert!(!valid(&changed));
        let mut changed = intent.clone();
        changed.base_version = None;
        assert!(!valid(&changed));
        let mut changed = intent.clone();
        changed.target_version = original.version.clone();
        assert!(!valid(&changed));
        for status in [
            DfsWriteIntentStatus::Committed,
            DfsWriteIntentStatus::Rejected,
            DfsWriteIntentStatus::Aborted,
        ] {
            let mut changed = intent.clone();
            changed.status = status;
            assert!(!valid(&changed));
        }
        assert!(original_input_identity_matches(&hash, 3, &hash, 3));
        assert!(!original_input_identity_matches(&hash, 4, &hash, 3));
        assert!(!original_input_identity_matches("different", 3, &hash, 3));
    }

    #[test]
    fn pending_witness_requires_unique_full_n_and_required_candidates() {
        let (original, assignment, options, intent, target, hash) = inputs();
        let valid = |row: &dfs::DfsWriteIntentRecord| {
            original_pending_intent_matches(
                row,
                &original,
                "original",
                &target,
                &hash,
                &assignment,
                &options,
            )
        };
        for desired in [
            vec![],
            vec![intent.desired_replica_node_ids[0].clone()],
            vec![intent.desired_replica_node_ids[0].clone(); 2],
            vec![
                intent.desired_replica_node_ids[0].clone(),
                NodeId::from_legacy_named("foreign-candidate").unwrap(),
            ],
        ] {
            let mut changed = intent.clone();
            changed.desired_replica_node_ids = desired;
            assert!(!valid(&changed));
        }
        let mut changed = intent.clone();
        changed.policy.forbidden_node_ids = vec![changed.desired_replica_node_ids[0].clone()];
        let mut forbidden_options = options.clone();
        forbidden_options.policy = changed.policy.clone();
        assert!(!original_pending_intent_matches(
            &changed,
            &original,
            "original",
            &target,
            &hash,
            &assignment,
            &forbidden_options
        ));
    }
}
