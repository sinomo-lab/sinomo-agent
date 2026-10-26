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
