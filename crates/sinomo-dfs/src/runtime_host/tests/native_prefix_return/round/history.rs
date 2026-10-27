//! Native provenance enters an immutable historical DATA slot, never authority.
use super::*;
use crate::metadata_backend::native_prefix_history::{
    NATIVE_PREFIX_HISTORY_FILE, NativePrefixHistoryDeadlineQualification,
    NativePrefixHistoryDisposition,
};
use std::os::unix::fs::MetadataExt;

const SEED: &str = "/native-prefix-return-seed";

fn slot(fixture: &Fixture) -> PathBuf {
    crate::local_dfs_metadata_shard_path(
        fixture.native.data_dir(&fixture.shard.primary_node_id),
        &DfsMetadataShardId::new(&fixture.shard.shard_id),
    )
    .join(NATIVE_PREFIX_HISTORY_FILE)
}

#[derive(Debug, PartialEq)]
struct FileObservation {
    bytes: Vec<u8>,
    dev: u64,
    ino: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}
fn file_observation(path: &Path) -> TestResult<FileObservation> {
    let meta = fs::symlink_metadata(path)?;
    assert!(meta.is_file() && !meta.file_type().is_symlink());
    Ok(FileObservation {
        bytes: fs::read(path)?,
        dev: meta.dev(),
        ino: meta.ino(),
        mtime: (meta.mtime(), meta.mtime_nsec()),
        ctime: (meta.ctime(), meta.ctime_nsec()),
    })
}
fn capture(
    fixture: &Fixture,
    selected: &crate::DfsMetadataQuorumSyncTarget,
) -> TestResult<OwnedNativeMetadataPrefixRound> {
    let mut transport = fixture.transport();
    transport.set_operation_deadline(Some(Instant::now() + CAP));
    let round = transport.capture_metadata_prefix_full_assignment_round(selected)?;
    assert_complete(&round, selected, fixture);
    Ok(round)
}
fn current_target(fixture: &Fixture) -> TestResult<crate::DfsMetadataQuorumSyncTarget> {
    let mut service = fixture.native.metadata_primary_service(SEED)?;
    let object = service
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&fixture.shard.file_system_id, SEED)?
        .unwrap();
    let target = service.capture_metadata_quorum_sync_target(&object)?;
    let proof = service.sync_metadata_shard_to_quorum(&target, CAP)?;
    assert_eq!(proof.acknowledged_node_ids.len(), 3);
    Ok(service.capture_metadata_quorum_sync_target(&object)?)
}
fn main_append(fixture: &Fixture, path: &str) -> TestResult<()> {
    let mut main = fixture.native.client(&fixture.shard.primary_node_id)?;
    let deadline = Instant::now() + CAP;
    let old = main.set_transport_operation_deadline(Some(deadline));
    let result = main.posix_mkdir(path);
    main.set_transport_operation_deadline(old);
    result?;
    assert!(
        Instant::now() <= deadline,
        "actual semantic append keeps its original budget"
    );
    Ok(())
}

fn checkpoint_actual_resident(fixture: &Fixture) -> TestResult<()> {
    let data = fixture.native.data_dir(&fixture.shard.primary_node_id);
    let resident = {
        let slot = daemon_dfs_message_service_cache()
            .try_existing_root(&data.root())?
            .ok_or("actual initialized native resident cache is unavailable")?;
        let cache = slot.lock().map_err(|_| "native cache lock poisoned")?;
        cache
            .as_ref()
            .and_then(|entry| entry.resident_metadata_backend.clone())
            .ok_or("actual resident metadata owner is unavailable")?
    };
    let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
    let shard = resident.scope_shard(&fixture.shard.file_system_id, &shard_id)?;
    // Actual scheduler admission precedes resident checkout and writer lock.
    // No cache mutex is retained across either wait.
    let scheduler = daemon_dfs_mutation_scheduler(data)?;
    let scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: fixture.shard.file_system_id.clone(),
        shard_id,
    };
    let admission = crate::test_runtime::executor()
        .block_on(async { tokio::time::timeout(CAP, scheduler.admit(&scope)).await })??;
    let result =
        shard.with_backend(|backend| backend.persist_current_metadata_checkpoint_for_test());
    drop(admission);
    result?;
    Ok(())
}

#[test]
fn native_prefix_history_retains_first_round_without_renewal_through_checkpoint_and_reopen() {
    run_round("native-prefix-history-durable", |fixture, report| {
        report["scope"] = serde_json::json!(
            "native full3 observation -> actual current writer -> immutable historical DATA"
        );
        report["limits"] = serde_json::json!(
            "no serving authority or renewed ACK; normal reopen/table checkpoint, not power loss; kernel quietness counters unavailable"
        );
        let markers = fixture.markers()?;
        let round = capture(fixture, &target(fixture))?;
        let path = slot(fixture);
        assert!(!path.exists());
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let first = service.retain_native_prefix_history(&round, Instant::now() + CAP)?;
        assert_eq!(
            first.disposition(),
            NativePrefixHistoryDisposition::WrittenAndSynced
        );
        assert_eq!(first.record().round_id(), round.round_id());
        assert_eq!(first.record().target(), round.target());
        assert_eq!(first.record().deadline_qualification(),
            &NativePrefixHistoryDeadlineQualification::OriginalProcessLocalDeadlineEnforcedNotReconstructible);
        for (data, peer) in first.record().peers().iter().zip(round.peer_outcomes()) {
            assert_eq!(data.peer(), peer.peer());
            assert_eq!(data.command(), peer.command());
            assert_eq!(
                data.attempt_id(),
                peer.native_return().unwrap().attempt_id()
            );
            assert_eq!(Some(data.response()), peer.response());
        }
        let before = file_observation(&path)?;
        let repeated = service.retain_native_prefix_history(&round, Instant::now() + CAP)?;
        assert_eq!(
            repeated.disposition(),
            NativePrefixHistoryDisposition::ExistingHistoricalData
        );
        assert_eq!(repeated.record(), first.record());
        assert_eq!(file_observation(&path)?, before);
        drop(service); // No retained backend/writer across native RPC.
        let fresh = capture(fixture, round.target())?;
        assert_ne!(fresh.round_id(), round.round_id());
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let semantic = service.retain_native_prefix_history(&fresh, Instant::now() + CAP)?;
        assert_eq!(
            semantic.disposition(),
            NativePrefixHistoryDisposition::ExistingHistoricalData
        );
        assert_eq!(semantic.record(), first.record());
        assert_eq!(
            file_observation(&path)?,
            before,
            "fresh transport correlation cannot rewrite historical DATA"
        );
        drop(service);
        assert_eq!(
            fixture.markers()?,
            markers,
            "historical intake and both duplicate forms cannot change an authority marker"
        );
        // Actual semantic activity makes the maintained table checkpoint useful.
        main_append(fixture, "/native-history-checkpoint-activity")?;
        let changed = current_target(fixture)?;
        assert!(changed.journal_sequence > round.target().journal_sequence);
        checkpoint_actual_resident(fixture)?;
        assert_eq!(file_observation(&path)?, before);
        let primary = fixture.shard.primary_node_id.clone();
        fixture.native.stop_node(&primary)?;
        fixture.native.reopen_node(primary)?;
        let service = fixture.native.metadata_primary_service(SEED)?;
        let read = service
            .read_native_prefix_history(&DfsMetadataShardId::new(&fixture.shard.shard_id))?
            .unwrap();
        assert_eq!(&read, first.record());
        assert_eq!(file_observation(&path)?, before);
        report["record"] = serde_json::json!(read);
        report["first_round"] = observed(&round);
        report["fresh_semantic_round"] = observed(&fresh);
        report["file_identity"] = serde_json::json!({"dev":before.dev,"ino":before.ino,"mtime":before.mtime,"ctime":before.ctime,
            "exact_duplicate_unchanged":true,"fresh_semantic_duplicate_unchanged":true,"checkpoint_and_reopen_unchanged":true});
        let after = fixture.markers()?;
        assert_eq!(
            after.keys().collect::<Vec<_>>(),
            markers.keys().collect::<Vec<_>>()
        );
        let primary_marker = path
            .parent()
            .unwrap()
            .join("journal/membership-handoff-write-freeze.sobj");
        let old: crate::DfsMetadataShardWriteFreeze = sinomo_core::object::serde_ext::from_bytes(
            markers
                .get(&primary_marker)
                .and_then(Option::as_ref)
                .ok_or("original completed native fence unavailable")?,
        )?;
        let actual: crate::DfsMetadataShardWriteFreeze =
            sinomo_core::object::serde_ext::from_bytes(
                after
                    .get(&primary_marker)
                    .and_then(Option::as_ref)
                    .ok_or("reopened native fence unavailable")?,
            )?;
        assert!(
            actual.rebind_prefix_proof.journal_sequence > old.rebind_prefix_proof.journal_sequence
        );
        assert_eq!(
            actual.rebind_prefix_proof.journal_sequence,
            changed.journal_sequence
        );
        assert_eq!(actual.rebind_prefix_proof.digest, changed.state_fingerprint);
        let mut expected = old.clone();
        expected.rebind_prefix_proof = actual.rebind_prefix_proof.clone();
        assert_eq!(
            actual, expected,
            "only the exact semantic descendant may advance; original fence identity/epochs/cut stay immutable"
        );
        for (other, bytes) in &markers {
            if other != &primary_marker {
                assert_eq!(after.get(other), Some(bytes));
            }
        }
        report["marker_transition"] = serde_json::json!({"before":old,"after":actual,"actual_semantic_target":changed,
            "intake_and_duplicates_byte_unchanged":true,"limit":"legitimate current-prefix rebind after explicit namespace activity/checkpoint/reopen; no time-only renewal"});
        Ok(vec![round, fresh])
    });
}

#[test]
fn native_prefix_history_refuses_partial_expired_and_foreign_writer_before_slot_creation() {
    run_round("native-prefix-history-refusals", |fixture, report| {
        let selected = target(fixture);
        let second = selected.replica_node_ids.iter().max().unwrap().clone();
        let held = hold(fixture, &second);
        let cursor = fixture.native.rpc_observations().len();
        let mut producer = fixture.transport();
        producer.set_operation_deadline(Some(Instant::now() + CAP));
        let partial = producer.capture_metadata_prefix_full_assignment_round(&selected)?;
        let calls = own_calls(
            &fixture.native.rpc_observations()[cursor..],
            &fixture.shard.primary_node_id,
            &second,
        );
        drop(held);
        drain_calls(&fixture.native, &calls)?;
        drop(producer);
        report["partial"] = observed(&partial);
        assert!(!partial.is_complete());
        assert!(
            partial
                .peer_outcomes()
                .iter()
                .any(|peer| peer.native_return().is_some())
        );
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let partial_error = service
            .retain_native_prefix_history(&partial, Instant::now() + CAP)
            .err()
            .ok_or("partial intake unexpectedly succeeded")?;
        assert!(!slot(fixture).exists());
        drop(service);
        let complete = capture(fixture, &selected)?;
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let expired = service
            .retain_native_prefix_history(&complete, Instant::now())
            .err()
            .ok_or("expired intake unexpectedly succeeded")?;
        assert!(!slot(fixture).exists());
        let scheduler =
            daemon_dfs_mutation_scheduler(fixture.native.data_dir(&fixture.shard.primary_node_id))?;
        let scope = DfsMessageExecutionScope::ExactShard {
            file_system_id: fixture.shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(&fixture.shard.shard_id),
        };
        let held_admission = crate::test_runtime::executor()
            .block_on(async { tokio::time::timeout(CAP, scheduler.admit(&scope)).await })??;
        let admission_deadline = Instant::now() + CAP;
        let blocked = service.retain_native_prefix_history(&complete, admission_deadline);
        drop(held_admission);
        let blocked_error = blocked
            .err()
            .ok_or("intake bypassed actual held native admission")?;
        assert!(Instant::now() >= admission_deadline);
        assert!(!slot(fixture).exists());
        drop(service);
        let foreign = fixture
            .native
            .metadata_shards()?
            .into_iter()
            .find(|shard| shard.shard_id != fixture.shard.shard_id)
            .ok_or("no actual other owning shard for refusal control")?;
        let foreign_path = crate::local_dfs_metadata_shard_path(
            fixture.native.data_dir(&foreign.primary_node_id),
            &DfsMetadataShardId::new(&foreign.shard_id),
        )
        .join(NATIVE_PREFIX_HISTORY_FILE);
        assert!(!foreign_path.exists());
        let mut foreign_service = open_dfs_runtime_service_on_shard(
            fixture.native.data_dir(&foreign.primary_node_id),
            Some(&fixture.native.runtime_handle()),
            &foreign,
        )?;
        let foreign_error = foreign_service
            .retain_native_prefix_history(&complete, Instant::now() + CAP)
            .err()
            .ok_or("foreign writer unexpectedly succeeded")?;
        assert!(!foreign_path.exists());
        assert!(!slot(fixture).exists());
        report["refusals"] = serde_json::json!({"partial":partial_error.to_string(),"expired":expired.to_string(),"held_actual_admission":blocked_error.to_string(),"actual_other_shard":foreign,"foreign":foreign_error.to_string(),"no_slot_created":true});
        Ok(vec![partial, complete])
    });
}

#[test]
fn native_prefix_history_conflicting_prefix_and_corrupt_slot_cannot_overwrite_first_record() {
    run_round("native-prefix-history-conflict", |fixture, report| {
        let round = capture(fixture, &target(fixture))?;
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let first = service.retain_native_prefix_history(&round, Instant::now() + CAP)?;
        drop(service);
        let path = slot(fixture);
        let before = file_observation(&path)?;
        main_append(fixture, "/native-history-semantic-conflict")?;
        let changed = current_target(fixture)?;
        assert_ne!(changed.state_fingerprint, round.target().state_fingerprint);
        let newer = capture(fixture, &changed)?;
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let conflict = service
            .retain_native_prefix_history(&newer, Instant::now() + CAP)
            .err()
            .ok_or("conflicting prefix overwrote immutable history")?;
        assert_eq!(file_observation(&path)?, before);
        assert_eq!(
            service
                .read_native_prefix_history(&DfsMetadataShardId::new(&fixture.shard.shard_id))?
                .as_ref(),
            Some(first.record())
        );
        // Preserve exact good bytes before explicit task-owned corruption. This
        // fault is DATA, never a source of a native capability.
        let mut retained = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.with_extension("retained-good-sobj"))?;
        retained.write_all(&before.bytes)?;
        retained.sync_all()?;
        drop(retained);
        let mut corrupt = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)?;
        corrupt.write_all(b"deliberately invalid historical DATA; not a native return")?;
        corrupt.sync_all()?;
        drop(corrupt);
        let bad = file_observation(&path)?;
        let read_error = service
            .read_native_prefix_history(&DfsMetadataShardId::new(&fixture.shard.shard_id))
            .err()
            .ok_or("corrupt slot read as valid DATA")?;
        let write_error = service
            .retain_native_prefix_history(&round, Instant::now() + CAP)
            .err()
            .ok_or("corrupt slot was silently replaced")?;
        assert_eq!(file_observation(&path)?, bad);
        report["record"] = serde_json::json!(first.record());
        report["newer_native_round"] = observed(&newer);
        report["refusals"] = serde_json::json!({"semantic_conflict":conflict.to_string(),"corrupt_read":read_error.to_string(),"corrupt_intake":write_error.to_string(),"original_preserved_separately":true,"no_overwrite":true});
        Ok(vec![round, newer])
    });
}

#[test]
fn native_prefix_history_reopen_after_physical_owner_replacement_refuses_rebinding() {
    run_round("native-prefix-history-physical", |fixture, report| {
        let round = capture(fixture, &target(fixture))?;
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let original = service.retain_native_prefix_history(&round, Instant::now() + CAP)?;
        drop(service);
        let path = slot(fixture);
        let before = file_observation(&path)?;
        let primary = fixture.shard.primary_node_id.clone();
        let root = fixture.source_root();
        fixture.native.stop_node(&primary)?;
        let named_owner = root.join("daemon/data-owner.lock");
        let old = identity(&named_owner)?;
        fs::rename(&named_owner, root.join("daemon/history-retired-owner.lock"))?;
        let replacement = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&named_owner)?;
        replacement.sync_all()?;
        let new = storage_file_identity(&replacement)?;
        drop(replacement);
        assert_ne!(old, new);
        fixture.native.reopen_node(primary)?;
        let mut service = fixture.native.metadata_primary_service(SEED)?;
        let read_error = service
            .read_native_prefix_history(&DfsMetadataShardId::new(&fixture.shard.shard_id))
            .err()
            .ok_or("copied historical owner silently rebound")?;
        let write_error = service
            .retain_native_prefix_history(&round, Instant::now() + CAP)
            .err()
            .ok_or("old native capability authorized the replacement owner")?;
        assert_eq!(file_observation(&path)?, before);
        report["historical_record"] = serde_json::json!(original.record());
        report["physical_refusal"] = serde_json::json!({"old_owner":old,"new_owner":new,"read_error":read_error.to_string(),"write_error":write_error.to_string(),"record_unchanged":true,
            "limit":"physical owner-file replacement under sequential normal owners; not same-directory table-generation replacement or power loss"});
        Ok(vec![round])
    });
}

#[test]
fn native_prefix_history_post_sync_deadline_retains_uncertainty_without_recertification() {
    use crate::metadata_backend::native_prefix_history::{
        NativePrefixHistoryRecord, with_after_publication_cut_for_test,
    };
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    run_round(
        "native-prefix-history-post-sync-expiry",
        |fixture, report| {
            report["scope"] = serde_json::json!(
                "actual native round and actual owning storage success, cut before final validation/return"
            );
            report["limits"] = serde_json::json!(
                "post-sync caller expiry, not fsync failure/SIGKILL/power loss/current authority or kernel quietness acceptance"
            );
            let markers = fixture.markers()?;
            let round = capture(fixture, &target(fixture))?;
            let original_round_deadline = round.deadline();
            let path = slot(fixture);
            assert!(!path.exists());
            let published = Rc::new(RefCell::new(None));
            let cut_count = Rc::new(Cell::new(0usize));
            let at_cut = Rc::clone(&published);
            let hits = Rc::clone(&cut_count);
            let expected_path = path.clone();
            let mut service = fixture.native.metadata_primary_service(SEED)?;
            let intake_deadline = Instant::now() + CAP;
            let result = with_after_publication_cut_for_test(
                move |actual_path, actual_deadline| {
                    assert_eq!(actual_path, expected_path);
                    assert_eq!(actual_deadline, intake_deadline);
                    assert!(
                        Instant::now() < actual_deadline,
                        "real storage success must reach the cut before original deadline"
                    );
                    hits.set(hits.get() + 1);
                    *at_cut.borrow_mut() = Some(
                        file_observation(actual_path).expect("actual published historical bytes"),
                    );
                    // No fake storage result or altered Instant: delay this exact
                    // caller until its already-installed intake budget expires.
                    loop {
                        let left = actual_deadline.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            break;
                        }
                        std::thread::sleep(left);
                    }
                },
                || service.retain_native_prefix_history(&round, intake_deadline),
            );
            report["initial_outcome"] = serde_json::json!({"error":result.as_ref().err().map(ToString::to_string),
            "cut_count":cut_count.get(),"intake_deadline_expired":Instant::now() >= intake_deadline,
            "original_round_deadline_unchanged":round.deadline() == original_round_deadline});
            let error = result
                .err()
                .ok_or("expired post-sync intake incorrectly acknowledged completion")?;
            assert!(
                error
                    .to_string()
                    .contains("publication/durability uncertain")
            );
            assert!(
                error
                    .to_string()
                    .contains("retained slot is not rolled back")
            );
            assert_eq!(cut_count.get(), 1);
            assert!(Instant::now() >= intake_deadline);
            assert_eq!(round.deadline(), original_round_deadline);
            let first = published
                .borrow_mut()
                .take()
                .ok_or("real publication cut was not reached")?;
            assert_eq!(file_observation(&path)?, first);
            // Readable, canonical historical DATA is explicitly not a fresh ACK.
            let record: NativePrefixHistoryRecord =
                sinomo_core::object::serde_ext::from_bytes(&first.bytes)?;
            assert_eq!(record.round_id(), round.round_id());
            assert_eq!(record.target(), round.target());
            assert_eq!(record.deadline_qualification(), &NativePrefixHistoryDeadlineQualification::OriginalProcessLocalDeadlineEnforcedNotReconstructible);
            for (old, observed) in record.peers().iter().zip(round.peer_outcomes()) {
                assert_eq!(old.peer(), observed.peer());
                assert_eq!(old.command(), observed.command());
                assert_eq!(
                    old.attempt_id(),
                    observed.native_return().unwrap().attempt_id()
                );
                assert_eq!(Some(old.response()), observed.response());
            }
            assert!(
                service
                    .retain_native_prefix_history(&round, intake_deadline)
                    .is_err()
            );
            assert_eq!(file_observation(&path)?, first);
            drop(service);
            let primary = fixture.shard.primary_node_id.clone();
            fixture.native.stop_node(&primary)?;
            fixture.native.reopen_node(primary)?;
            let service = fixture.native.metadata_primary_service(SEED)?;
            let cold = service
                .read_native_prefix_history(&DfsMetadataShardId::new(&fixture.shard.shard_id))?
                .unwrap();
            assert_eq!(cold, record);
            assert_eq!(file_observation(&path)?, first);
            drop(service); // no owner lease across the new actual native RPCs
            let fresh = capture(fixture, round.target())?;
            assert_ne!(fresh.round_id(), round.round_id());
            let mut service = fixture.native.metadata_primary_service(SEED)?;
            let duplicate_cut_count = Rc::new(Cell::new(0usize));
            let duplicates = Rc::clone(&duplicate_cut_count);
            let duplicate = with_after_publication_cut_for_test(
                move |_, _| duplicates.set(duplicates.get() + 1),
                || service.retain_native_prefix_history(&fresh, Instant::now() + CAP),
            )?;
            assert_eq!(
                duplicate.disposition(),
                NativePrefixHistoryDisposition::ExistingHistoricalData
            );
            assert_eq!(duplicate.record(), &record);
            assert_eq!(
                duplicate_cut_count.get(),
                0,
                "existing historical DATA cannot call publication/sync again"
            );
            assert_eq!(file_observation(&path)?, first);
            assert_eq!(fixture.markers()?, markers);
            assert_eq!(round.deadline(), original_round_deadline);
            report["historical_record"] = serde_json::json!(record);
            report["original_round"] = observed(&round);
            report["fresh_round"] = observed(&fresh);
            report["reopen_and_duplicate"] = serde_json::json!({"disposition":format!("{:?}", duplicate.disposition()),
            "cold_data_equal":true,"original_record_retained":true,"publication_cut_count":duplicate_cut_count.get(),
            "file_bytes_identity_mtime_ctime_unchanged":true,"authority_markers_unchanged":true,
            "file_dev":first.dev,"file_ino":first.ino,"file_mtime":first.mtime,"file_ctime":first.ctime,
            "first_error_remains_uncertain":error.to_string()});
            Ok(vec![round, fresh])
        },
    );
}

#[test]
fn native_prefix_history_canonical_incomplete_or_mismatched_data_refuses_without_repair() {
    use crate::metadata_backend::native_prefix_history::{
        NativePrefixHistoryRecord, with_after_publication_cut_for_test,
    };
    use std::cell::Cell;
    use std::rc::Rc;

    run_round(
        "native-prefix-history-structural-replay",
        |fixture, report| {
            report["scope"] = serde_json::json!(
                "native original intake, then deliberately TEST-SUPPLIED canonical SOBJ fault images; no native evidence minted from those images"
            );
            report["limits"] = serde_json::json!(
                "structural/physical reader refusal only, not cryptographic tamper authentication, fsync/power-loss or current authority"
            );
            let markers = fixture.markers()?;
            let round = capture(fixture, &target(fixture))?;
            let path = slot(fixture);
            let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
            let mut service = fixture.native.metadata_primary_service(SEED)?;
            let first = service.retain_native_prefix_history(&round, Instant::now() + CAP)?;
            assert_eq!(
                first.disposition(),
                NativePrefixHistoryDisposition::WrittenAndSynced
            );
            assert_eq!(
                service.read_native_prefix_history(&shard_id)?.as_ref(),
                Some(first.record())
            );
            let original = file_observation(&path)?;
            let data = serde_json::to_value(first.record())?;
            let round_trip: NativePrefixHistoryRecord = serde_json::from_value(data.clone())?;
            assert_eq!(
                crate::metadata_io::encode_sobj(&round_trip)?,
                original.bytes
            );
            let original_path = path.with_extension("structural-original.sobj");
            let mut saved = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&original_path)?;
            saved.write_all(&original.bytes)?;
            saved.sync_all()?;
            drop(saved);
            report["genuine_original_record"] = serde_json::json!(first.record());
            report["genuine_original_round"] = observed(&round);
            report["faults"] = serde_json::json!([]);

            let mut missing = data.clone();
            missing["peers"].as_array_mut().unwrap().pop().unwrap();
            let mut duplicate_attempt = data.clone();
            duplicate_attempt["peers"][1]["attempt_id"] = data["peers"][0]["attempt_id"].clone();
            let mut reply_sequence = data.clone();
            reply_sequence["peers"][0]["response"]["next_after_sequence"] = serde_json::json!(
                round
                    .target()
                    .journal_sequence
                    .checked_add(1)
                    .ok_or("test sequence overflow")?
            );
            let mut command_epoch = data.clone();
            let foreign_epoch = fixture
                .shard
                .epoch
                .checked_add(1)
                .ok_or("test epoch overflow")?;
            command_epoch["peers"][0]["command"]["authority_epoch"] =
                serde_json::json!(foreign_epoch);
            command_epoch["peers"][0]["command"]["prefix_probe"]["authority_epoch"] =
                serde_json::json!(foreign_epoch);
            let faults = [
                (
                    "missing-required-peer",
                    missing,
                    "unsupported or inconsistent scope",
                ),
                (
                    "duplicate-attempt",
                    duplicate_attempt,
                    "malformed or foreign",
                ),
                (
                    "mismatched-response-sequence",
                    reply_sequence,
                    "malformed or foreign",
                ),
                (
                    "foreign-command-and-probe-epoch",
                    command_epoch,
                    "malformed or foreign",
                ),
            ];
            let mut last = None;
            for (index, (name, value, expected_error)) in faults.into_iter().enumerate() {
                // Canonical decoding/encoding succeeds. This is deliberately
                // supplied corrupt DATA, not a raw frame converted to provenance.
                let fault: NativePrefixHistoryRecord = serde_json::from_value(value)?;
                let bytes = crate::metadata_io::encode_sobj(&fault)?;
                let decoded: NativePrefixHistoryRecord =
                    sinomo_core::object::serde_ext::from_bytes(&bytes)?;
                assert_eq!(crate::metadata_io::encode_sobj(&decoded)?, bytes);
                assert_ne!(bytes, original.bytes);
                let preserved_path = path.with_extension(format!("structural-fault-{index}.sobj"));
                let mut saved = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&preserved_path)?;
                saved.write_all(&bytes)?;
                saved.sync_all()?;
                drop(saved);
                let mut fault_file = fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&path)?;
                fault_file.write_all(&bytes)?;
                fault_file.sync_all()?;
                drop(fault_file);
                let before = file_observation(&path)?;
                let read_error = service
                    .read_native_prefix_history(&shard_id)
                    .err()
                    .ok_or("canonical contradictory DATA passed historical validation")?;
                let publications = Rc::new(Cell::new(0usize));
                let hits = Rc::clone(&publications);
                let intake = with_after_publication_cut_for_test(
                    move |_, _| hits.set(hits.get() + 1),
                    || service.retain_native_prefix_history(&round, Instant::now() + CAP),
                );
                let intake_error = intake
                    .err()
                    .ok_or("contradictory DATA was overwritten or acknowledged")?;
                report["faults"].as_array_mut().unwrap().push(serde_json::json!({"name":name,
                "classification":"TEST-SUPPLIED canonical DATA, not native provenance",
                "preserved_image":preserved_path,"read_error":read_error.to_string(),"intake_error":intake_error.to_string(),
                "publication_count":publications.get(),"canonical_decode_encode":true}));
                assert!(
                    read_error.to_string().contains(expected_error),
                    "wrong refusal: {read_error}"
                );
                assert!(
                    intake_error.to_string().contains(expected_error),
                    "wrong refusal: {intake_error}"
                );
                assert_eq!(publications.get(), 0);
                assert_eq!(
                    file_observation(&path)?,
                    before,
                    "reader/intake must preserve the contradictory evidence"
                );
                assert_eq!(fixture.markers()?, markers);
                assert_eq!(fs::read(&original_path)?, original.bytes);
                last = Some(before);
            }
            drop(service);
            let primary = fixture.shard.primary_node_id.clone();
            fixture.native.stop_node(&primary)?;
            fixture.native.reopen_node(primary)?;
            let mut service = fixture.native.metadata_primary_service(SEED)?;
            let cold = service
                .read_native_prefix_history(&shard_id)
                .err()
                .ok_or("cold contradictory DATA silently became absent or accepted")?;
            let cold_intake = service
                .retain_native_prefix_history(&round, Instant::now() + CAP)
                .err()
                .ok_or("cold contradictory DATA was silently repaired")?;
            assert!(cold.to_string().contains("malformed or foreign"));
            assert!(cold_intake.to_string().contains("malformed or foreign"));
            assert_eq!(file_observation(&path)?, last.unwrap());
            assert_eq!(fixture.markers()?, markers);
            assert_eq!(fs::read(&original_path)?, original.bytes);
            report["cold_refusal"] = serde_json::json!({"read":cold.to_string(),"intake":cold_intake.to_string(),
            "last_fault_and_original_preserved":true,"authority_markers_unchanged":true});
            Ok(vec![round])
        },
    );
}
