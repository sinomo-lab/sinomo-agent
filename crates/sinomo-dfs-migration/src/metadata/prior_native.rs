//! Read-only inspection of original native generation-8 and generation-9 images.
//! Its hashes describe retained DATA; native recovery separately replays the
//! exact saved plan and committed activation request. DFS never opens this image.
use super::{Result, error, source_proof};
use crate::legacy_wire;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_core::object::{MAGIC, serde_ext};
use sinomo_core::storage::migration::stream::{MAX_RECORD_BYTES, MigrationStreamReader};
use sinomo_dfs::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format_version: u16,
    materialized_through_sequence: u64,
    prefix_proof: DfsMetadataPrefixProof,
    semantic_state_fingerprint: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum Record {
    Table {
        op: Value,
    },
    Transfer {
        transfer: DfsCrossShardPinTransferRecord,
    },
    IntentRetention {
        state: DfsWriteIntentReceiptRetentionState,
    },
    NamespaceRetention {
        state: Value,
    },
    Prepared {
        record: Value,
    },
    Eviction {
        record: DfsPreparedOperationEviction,
    },
}
fn native_document(bytes: &[u8], generation: u16) -> Result<()> {
    if bytes.len() < 6
        || &bytes[..4] != MAGIC
        || u16::from_le_bytes([bytes[4], bytes[5]]) != generation
    {
        return Err(error("prior native image has a foreign record generation"));
    }
    Ok(())
}
fn field<T: serde::de::DeserializeOwned>(value: &Value, name: &str) -> Result<T> {
    serde_json::from_value(
        value
            .get(name)
            .cloned()
            .ok_or_else(|| error(format!("prior native table row omits {name}")))?,
    )
    .map_err(error)
}
/// Interpret the missing required stage only in a converted DATA view. Original
/// bytes and original V7 digest validation precede this function at inspection.
pub(super) fn payload_data(mut value: Value) -> Result<DfsPreparedOperationPayload> {
    if let Some(effects) = value
        .get_mut("metadata_effects")
        .and_then(Value::as_array_mut)
    {
        for effect in effects {
            match effect.get("type").and_then(Value::as_str) {
                Some("put-namespace-job") => {
                    effect["job"] = serde_json::to_value(legacy_wire::normalize_namespace_job(
                        effect["job"].clone(),
                    )?)
                    .map_err(error)?;
                }
                Some("put-namespace-request") => {
                    effect["request"] = serde_json::to_value(
                        legacy_wire::normalize_namespace_request(effect["request"].clone())?,
                    )
                    .map_err(error)?;
                }
                _ => {}
            }
        }
    }
    for (key, completion) in [
        ("completed_request", false),
        ("completed_request_completion", true),
    ] {
        if let Some(row) = value.get_mut(key).filter(|row| !row.is_null()) {
            *row = if completion {
                serde_json::to_value(legacy_wire::normalize_request_completion(row.clone())?)
            } else {
                serde_json::to_value(legacy_wire::normalize_namespace_request(row.clone())?)
            }
            .map_err(error)?;
        }
    }
    let payload: DfsPreparedOperationPayload = serde_json::from_value(value).map_err(error)?;
    payload.validate()?;
    Ok(payload)
}
fn validate_prepare(row: &Value) -> Result<String> {
    legacy_wire::validate_original_v7_record(row)?;
    let digest: DfsPreparedOperationDigest = field(row, "digest")?;
    let payload = payload_data(field(row, "payload")?)?;
    if payload.file_system_id != digest.file_system_id
        || payload.shard_id != digest.shard_id
        || payload.owner_epoch != digest.owner_epoch
        || payload.operation_id != digest.operation_id
        || payload
            .expected_journal_sequence
            .is_some_and(|sequence| sequence.checked_add(1) != Some(digest.prepare_sequence))
    {
        return Err(error(
            "prior native preparation changes its scope/slot footprint",
        ));
    }
    let status: DfsPreparedOperationStatus = field(row, "status")?;
    let committed: Option<DfsPreparedOperationCommitReference> =
        serde_json::from_value(row.get("committed_entry").cloned().unwrap_or(Value::Null))
            .map_err(error)?;
    if (status == DfsPreparedOperationStatus::Committed) != committed.is_some() {
        return Err(error(
            "prior native preparation has no exact terminal reference",
        ));
    }
    if let Some(committed) = committed {
        committed.validate(&digest)?;
    }
    Ok(digest.operation_id)
}

struct Jobs<'a>(&'a [Value]);
impl Serialize for Jobs<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&legacy_wire::Native8Job(row))?;
        }
        seq.end()
    }
}
struct Prepares<'a>(&'a [Value]);
impl Serialize for Prepares<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&legacy_wire::Native8Prepare(row))?;
        }
        seq.end()
    }
}
/// The generation-8 table order and omission rules. Only original job and
/// prepare bodies need a source serializer; unchanged rows use normal DTOs.
struct Tables<'a> {
    current_data: &'a DfsMetadataTableSnapshotTables,
    jobs: &'a [Value],
    prepares: &'a [Value],
}
impl Serialize for Tables<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let t = self.current_data;
        let mut map = serializer.serialize_map(None)?;
        macro_rules! rows {
            ($field:ident) => {
                if !t.$field.is_empty() {
                    map.serialize_entry(stringify!($field), &t.$field)?;
                }
            };
        }
        map.serialize_entry(
            "prepared_operation_evictions",
            &t.prepared_operation_evictions,
        )?;
        rows!(file_systems);
        rows!(objects);
        rows!(file_system_objects);
        rows!(paths);
        rows!(inodes);
        rows!(dirfrags);
        rows!(dentries);
        rows!(hardlinks);
        rows!(compacted_tombstones);
        rows!(versions);
        rows!(version_pins);
        rows!(cross_shard_pin_transfers);
        rows!(receipts);
        rows!(intents);
        rows!(intent_receipts);
        if t.intent_receipt_retention != DfsWriteIntentReceiptRetentionState::default() {
            map.serialize_entry("intent_receipt_retention", &t.intent_receipt_retention)?;
        }
        rows!(advisory_lock_states);
        rows!(namespace_transaction_receipts);
        if !self.prepares.is_empty() {
            map.serialize_entry("prepared_operations_v7", &Prepares(self.prepares))?;
        }
        if !self.jobs.is_empty() {
            map.serialize_entry("namespace_jobs", &Jobs(self.jobs))?;
        }
        rows!(namespace_job_receipts);
        rows!(namespace_request_receipts);
        if t.namespace_receipt_retention
            != DfsMetadataTableSnapshotTables::default().namespace_receipt_retention
        {
            map.serialize_entry(
                "namespace_receipt_retention",
                &t.namespace_receipt_retention,
            )?;
        }
        map.serialize_entry("namespace_requests", &t.namespace_requests)?;
        map.end()
    }
}

pub(super) fn inspect(root: &Path) -> Result<DfsMetadataCandidateIdentity> {
    let path = root.join("records/tables/snapshot.sobj");
    let mut file = File::open(&path).map_err(error)?;
    let mut reader =
        MigrationStreamReader::from_file(file.try_clone().map_err(error)?).map_err(error)?;
    let header: Header = serde_ext::from_bytes(&reader.header).map_err(error)?;
    let generation = header.format_version;
    if !matches!(generation, 8 | 9) {
        return Err(error("unknown original native image generation"));
    }
    native_document(&reader.header, generation)?;
    source_proof::validate(&header.prefix_proof)?;
    if header.prefix_proof.version != 2
        || header.materialized_through_sequence != header.prefix_proof.journal_sequence
    {
        return Err(error("invalid prior native image header/cut"));
    }
    let mut tables = DfsMetadataTableSnapshotTables::default();
    let mut jobs = BTreeMap::new();
    let mut prepares = BTreeMap::new();
    while let Some(bytes) = reader
        .next_record(MAX_RECORD_BYTES as usize)
        .map_err(error)?
    {
        native_document(&bytes, generation)?;
        let record: Record = serde_ext::from_bytes(&bytes).map_err(error)?;
        match record {
            Record::Table { op } => {
                // Deserialize the complete current row after the one owned old
                // job-stage adaptation. Unknown operation fields remain errors.
                let mut current_op = op.clone();
                if op["type"].as_str() == Some("put-namespace-job") {
                    let original = op["job"].clone();
                    legacy_wire::validate_original_source_job(&original)?;
                    let job = legacy_wire::normalize_namespace_job(original.clone())?;
                    if jobs
                        .insert((job.file_system_id.clone(), job.job_id.clone()), original)
                        .is_some()
                    {
                        return Err(error("prior native image repeats a job identity"));
                    }
                    current_op["job"] = serde_json::to_value(job).map_err(error)?;
                }
                let _: DfsMetadataCandidateRow =
                    serde_json::from_value(current_op.clone()).map_err(error)?;
                macro_rules! row {
                    ($field:ident, $name:literal) => {{
                        tables.$field.push(field(&current_op, $name)?);
                    }};
                }
                match op["type"].as_str() {
                    Some("put-file-system") => row!(file_systems, "file_system"),
                    Some("put-object") => row!(objects, "object"),
                    Some("put-file-system-object") => row!(file_system_objects, "path_record"),
                    Some("put-path") => row!(paths, "path_record"),
                    Some("put-inode") => row!(inodes, "inode"),
                    Some("put-dir-fragment") => row!(dirfrags, "dirfrag"),
                    Some("put-dentry") => row!(dentries, "path_record"),
                    Some("put-hardlink") => tables.hardlinks.push(DfsHardlinkIndexSnapshotRecord {
                        target_object_id: field(&current_op, "target_object_id")?,
                        hardlink_object_id: field(&current_op, "hardlink_object_id")?,
                        path_record: field(&current_op, "path_record")?,
                    }),
                    Some("put-compacted-tombstone") => row!(compacted_tombstones, "object"),
                    Some("put-version") => row!(versions, "version"),
                    Some("put-version-pin") => row!(version_pins, "pin"),
                    Some("put-receipt") => row!(receipts, "receipt"),
                    Some("put-intent") => row!(intents, "intent"),
                    Some("put-intent-receipt") => row!(intent_receipts, "receipt"),
                    Some("put-advisory-lock-state") => row!(advisory_lock_states, "state"),
                    Some("put-namespace-transaction-receipt") => {
                        row!(namespace_transaction_receipts, "receipt")
                    }
                    Some("put-namespace-job") => row!(namespace_jobs, "job"),
                    Some("put-namespace-job-receipt") => row!(namespace_job_receipts, "receipt"),
                    Some("put-namespace-request") => row!(namespace_requests, "request"),
                    Some("put-namespace-request-receipt") => {
                        row!(namespace_request_receipts, "receipt")
                    }
                    _ => {
                        return Err(error(
                            "prior native snapshot contains a non-snapshot table operation",
                        ));
                    }
                }
            }
            Record::Transfer { transfer } => tables.cross_shard_pin_transfers.push(transfer),
            Record::IntentRetention { state } => tables.intent_receipt_retention = state,
            Record::NamespaceRetention { state } => {
                tables.namespace_receipt_retention =
                    serde_json::from_value(state).map_err(error)?;
            }
            Record::Prepared { record } => {
                let id = validate_prepare(&record)?;
                if generation == 9 {
                    let original: DfsPreparedOperationRecord =
                        serde_json::from_value(record.clone()).map_err(error)?;
                    original.validate()?;
                    if serde_json::to_value(&original).map_err(error)? != record {
                        return Err(error("original native9 preparation changes its typed body"));
                    }
                    tables.prepared_operations_v7.push(original);
                }
                if prepares.insert(id, record).is_some() {
                    return Err(error("prior native image repeats a preparation identity"));
                }
            }
            Record::Eviction { record } => {
                record.validate()?;
                tables.prepared_operation_evictions.push(record);
            }
        }
    }
    // next_record validates the original complete footer and EOF. Normal DATA
    // row validation checks namespace/table invariants, without publication.
    let tables = DfsMetadataCandidateState::new(tables)?.snapshot();
    if tables
        .prepared_operation_evictions
        .iter()
        .any(|row| prepares.contains_key(&row.digest.operation_id))
    {
        return Err(error(
            "prior native image overlaps an evicted and full operation footprint",
        ));
    }
    let jobs = jobs.into_values().collect::<Vec<_>>();
    let prepares = prepares.into_values().collect::<Vec<_>>();
    #[derive(Serialize)]
    struct Semantic<T: Serialize> {
        format_version: u16,
        materialized_through_sequence: u64,
        tables: T,
    }
    let mut semantic = blake3::Hasher::new();
    if generation == 8 {
        serde_json::to_writer(
            &mut semantic,
            &Semantic {
                format_version: generation,
                materialized_through_sequence: header.materialized_through_sequence,
                tables: Tables {
                    current_data: &tables,
                    jobs: &jobs,
                    prepares: &prepares,
                },
            },
        )
        .map_err(error)?;
    } else {
        // Generation9 has the exact current non-alias DTO shape/order. Its
        // source generation participates in the original semantic checksum.
        serde_json::to_writer(
            &mut semantic,
            &Semantic {
                format_version: generation,
                materialized_through_sequence: header.materialized_through_sequence,
                tables: &tables,
            },
        )
        .map_err(error)?;
    }
    let fingerprint = format!("blake3:{}", semantic.finalize().to_hex());
    if fingerprint != header.semantic_state_fingerprint {
        return Err(error("prior native image semantic checksum mismatch"));
    }
    file.seek(SeekFrom::Start(0)).map_err(error)?;
    let mut digest = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(error)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(DfsMetadataCandidateIdentity {
        state_fingerprint: fingerprint,
        file_fingerprint: format!("blake3:{}", digest.finalize().to_hex()),
        prefix_proof: header.prefix_proof,
    })
}

/// Historic desired topology is retained DATA under the new source cut. A9
/// certificate and actual8 receipt replay govern the later normal callback.
pub(super) fn topology_data(root: &Path, shard: &DfsMetadataShardId) -> Result<Option<Value>> {
    let marker = root.join("metadata-import-v7.json");
    if !marker.is_file() {
        return Ok(None);
    }
    let marker_bytes = std::fs::read(marker).map_err(error)?;
    let raw: Value = serde_json::from_slice(&marker_bytes).map_err(error)?;
    if raw["format_version"].as_u64() != Some(7) {
        return Err(error("prior native source has an unknown import marker"));
    }
    let cut: super::MetadataSourceCut =
        serde_json::from_value(raw["source_cut"].clone()).map_err(error)?;
    if cut.scope_shard != *shard {
        return Err(error(
            "prior native topology marker names another physical scope",
        ));
    }
    if cut.original_handoff.is_none() {
        return Ok(None);
    }
    let plan_bytes = std::fs::read(root.join("native-import-plan.json")).map_err(error)?;
    Ok(Some(serde_json::json!({
        "disposition":"preserved-original-terminal-data",
        "current_effects_authored":false,
        "prior_native_topology": {
            "marker_digest":blake3::hash(&marker_bytes).to_hex().to_string(),
            "marker_bytes":marker_bytes,
            "plan_digest":blake3::hash(&plan_bytes).to_hex().to_string(),
            "plan_bytes":plan_bytes,
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    const SNAPSHOT: &[u8] = include_bytes!("fixtures/current8-history-cleanup.snapshot.sobj");
    const TABLE_LOG: &[u8] = include_bytes!("fixtures/current8-history-cleanup.materialized.slog");
    const JOURNAL: &[u8] = include_bytes!("fixtures/current8-history-cleanup.journal.slog");

    // Exact native9 physical records produced by R1420's genuine normal
    // three-node runtime. No header/epoch/body/digest/receipt is remodeled.
    const NATIVE9_USERS: [(&str, &[u8]); 4] = [
        (
            "records/tables/snapshot.sobj",
            include_bytes!("fixtures/native9-r1420-users/records/tables/snapshot.sobj"),
        ),
        (
            "records/tables/materialized.slog",
            include_bytes!("fixtures/native9-r1420-users/records/tables/materialized.slog"),
        ),
        (
            "journal/head.sobj",
            include_bytes!("fixtures/native9-r1420-users/journal/head.sobj"),
        ),
        (
            "journal/segments/00000000000000000001.slog",
            include_bytes!(
                "fixtures/native9-r1420-users/journal/segments/00000000000000000001.slog"
            ),
        ),
    ];
    const NATIVE9_SERVICES: [(&str, &[u8]); 4] = [
        (
            "records/tables/snapshot.sobj",
            include_bytes!("fixtures/native9-r1420-services/records/tables/snapshot.sobj"),
        ),
        (
            "records/tables/materialized.slog",
            include_bytes!("fixtures/native9-r1420-services/records/tables/materialized.slog"),
        ),
        (
            "journal/head.sobj",
            include_bytes!("fixtures/native9-r1420-services/journal/head.sobj"),
        ),
        (
            "journal/segments/00000000000000000001.slog",
            include_bytes!(
                "fixtures/native9-r1420-services/journal/segments/00000000000000000001.slog"
            ),
        ),
    ];
    fn write_native9_fixture(root: &Path, files: &[(&str, &[u8])]) {
        for (relative, bytes) in files {
            let path = root.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
    }
    fn original_native9_prepares(root: &Path) -> Vec<Value> {
        use super::super::source::{SourceCursor, SourceRecord};
        let inventory = super::super::source::discover(root).unwrap();
        let mut cursor = SourceCursor::new(inventory.clone());
        let mut rows = Vec::new();
        while let Some(record) = super::super::source::next(root, &mut cursor).unwrap() {
            match record {
                SourceRecord::JournalFrame { entry, .. }
                    if entry["mutation"]["type"].as_str() == Some("prepare-operation-v7") =>
                {
                    rows.push(entry["mutation"]["record"].clone());
                }
                SourceRecord::TableBatch { batch, .. } => {
                    for op in batch["ops"].as_array().unwrap() {
                        if op["type"].as_str() == Some("put-prepared-operation-v7") {
                            rows.push(op["record"].clone());
                        }
                    }
                }
                SourceRecord::SnapshotRow { table, row, .. }
                    if table == "prepared_operations_v7" =>
                {
                    rows.push(row)
                }
                _ => {}
            }
        }
        inventory.verify(root).unwrap();
        rows
    }
    fn contains_old_alias_frame(row: &Value) -> bool {
        row["payload"]["metadata_effects"]
            .as_array()
            .is_some_and(|effects| {
                effects.iter().any(|effect| {
                    effect["type"].as_str() == Some("put-namespace-job")
                        && effect["job"]["pending_frames"]
                            .as_array()
                            .is_some_and(|frames| {
                                frames.iter().any(|frame| {
                                    frame["type"].as_str()
                                        == Some("cross-shard-hardlink-alias-commit")
                                })
                            })
                })
            })
    }
    #[test]
    fn actual_original_native9_non_alias_source_retains_full_prepare_identity_and_hash() {
        let dir = sinomo_testdir::TestDir::new("original-native9-non-alias-source");
        write_native9_fixture(dir.path(), &NATIVE9_USERS);
        let identity = inspect(dir.path()).unwrap();
        assert_eq!(
            identity,
            super::super::inspect_import_candidate(dir.path()).unwrap()
        );
        assert_eq!(
            super::super::original_native_root_format(dir.path()).unwrap(),
            Some(9)
        );
        assert!(!super::super::current_root(dir.path()).unwrap());
        assert!(
            LocalSobjMetadataBackend::open_for_inspection(
                DfsMetadataShardId::new("users"),
                dir.path()
            )
            .is_err(),
            "normal current reader never admits native9"
        );
        let rows = original_native9_prepares(dir.path());
        assert!(
            !rows.is_empty(),
            "a real producer body, not only its header, is required"
        );
        for row in rows {
            assert!(!contains_old_alias_frame(&row));
            let original = row.clone();
            legacy_wire::validate_original_v7_record(&row).unwrap();
            validate_prepare(&row).unwrap();
            let exact: DfsPreparedOperationRecord = serde_json::from_value(row.clone()).unwrap();
            exact.validate().unwrap();
            assert_eq!(serde_json::to_value(&exact).unwrap(), original);
            assert_eq!(
                serde_json::to_value(payload_data(row["payload"].clone()).unwrap()).unwrap(),
                original["payload"]
            );
            let mut changed = row;
            changed["payload"]["owner_epoch"] = serde_json::json!(exact.payload.owner_epoch + 1);
            assert!(legacy_wire::validate_original_v7_record(&changed).is_err());
        }
        for (relative, original) in NATIVE9_USERS {
            assert_eq!(std::fs::read(dir.join(relative)).unwrap(), original);
        }
    }
    #[test]
    fn actual_original_native9_alias_digest_is_valid_but_missing_original_comparisons_blocks_conversion()
     {
        let dir = sinomo_testdir::TestDir::new("original-native9-missing-alias-custody");
        write_native9_fixture(dir.path(), &NATIVE9_SERVICES);
        assert!(!super::super::current_root(dir.path()).unwrap());
        let rows = original_native9_prepares(dir.path());
        let original_aliases = rows
            .into_iter()
            .filter(contains_old_alias_frame)
            .collect::<Vec<_>>();
        assert!(
            !original_aliases.is_empty(),
            "the genuine R1420 durable producer body must be present"
        );
        for row in original_aliases {
            let original = row.clone();
            let calculated =
                legacy_wire::original_native9_alias_payload_digest(&row["payload"]).unwrap();
            eprintln!(
                "actual native9 alias original operation={} slot={} stored={} calculated={}",
                row["digest"]["operation_id"],
                row["digest"]["prepare_sequence"],
                row["digest"]["payload_digest"],
                calculated
            );
            assert_eq!(
                Some(calculated.as_str()),
                row["digest"]["payload_digest"].as_str()
            );
            legacy_wire::validate_original_v7_record(&row).unwrap();
            assert!(serde_json::from_value::<DfsPreparedOperationRecord>(row.clone()).is_err());
            assert!(matches!(
                payload_data(row["payload"].clone()),
                Err(MetadataBackendError::MissingOriginalSourceCustody { .. })
            ));
            assert_eq!(
                row, original,
                "no predicates, epochs, slots, digests or source bytes are fabricated"
            );
            let mut changed = row.clone();
            changed["payload"]["operation_id"] = Value::String("changed-original-id".into());
            assert!(legacy_wire::validate_original_v7_record(&changed).is_err());
            let mut wrong_digest = row.clone();
            wrong_digest["digest"]["payload_digest"] = Value::String(format!(
                "dfs-prepared-operation-v7:blake3:{}",
                "0".repeat(64)
            ));
            assert!(legacy_wire::validate_original_v7_record(&wrong_digest).is_err());
            let mut unknown = row.clone();
            unknown["payload"]["metadata_effects"][0]["job"]["unrecognized_custody"] =
                Value::Bool(true);
            assert!(legacy_wire::validate_original_v7_record(&unknown).is_err());
            let mut changed_frame = row;
            let effects = changed_frame["payload"]["metadata_effects"]
                .as_array_mut()
                .unwrap();
            let alias = effects
                .iter_mut()
                .filter_map(|effect| effect["job"]["pending_frames"].as_array_mut())
                .flat_map(|frames| frames.iter_mut())
                .find(|frame| frame["type"].as_str() == Some("cross-shard-hardlink-alias-commit"))
                .unwrap();
            alias["transaction"]["transaction_id"] =
                Value::String("changed-original-alias-body".into());
            assert!(legacy_wire::validate_original_v7_record(&changed_frame).is_err());
        }
        for (relative, original) in NATIVE9_SERVICES {
            assert_eq!(std::fs::read(dir.join(relative)).unwrap(), original);
        }
    }

    #[test]
    fn actual_prior_native_image_is_checked_without_current_admission() {
        let dir = sinomo_testdir::TestDir::new("actual-prior-native8-image");
        let path = dir.join("records/tables/snapshot.sobj");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, SNAPSHOT).unwrap();
        let identity = inspect(dir.path()).unwrap();
        assert!(identity.file_fingerprint.starts_with("blake3:"));
        assert!(!super::super::current_root(dir.path()).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), SNAPSHOT);
        let mut torn = SNAPSHOT.to_vec();
        torn.pop();
        std::fs::write(&path, torn).unwrap();
        assert!(inspect(dir.path()).is_err());
    }

    #[test]
    fn actual_prior_copy_prepare_hash_precedes_required_stage_conversion() {
        let dir = sinomo_testdir::TestDir::new("actual-prior-native8-copy-prepare");
        for (relative, bytes) in [
            ("records/tables/snapshot.sobj", SNAPSHOT),
            ("records/tables/materialized.slog", TABLE_LOG),
            ("journal/segments/00000000000000000001.slog", JOURNAL),
        ] {
            let path = dir.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        let inventory = super::super::source::discover(dir.path()).unwrap();
        let mut cursor = super::super::source::SourceCursor::new(inventory.clone());
        let mut rows = Vec::new();
        while let Some(record) = super::super::source::next(dir.path(), &mut cursor).unwrap() {
            match record {
                super::super::source::SourceRecord::TableBatch { batch, .. } => {
                    for op in batch["ops"].as_array().unwrap() {
                        if op["type"].as_str() == Some("put-prepared-operation-v7") {
                            rows.push(op["record"].clone());
                        }
                    }
                }
                super::super::source::SourceRecord::JournalFrame { entry, .. }
                    if entry["mutation"]["type"].as_str() == Some("prepare-operation-v7") =>
                {
                    rows.push(entry["mutation"]["record"].clone());
                }
                _ => {}
            }
        }
        let mut old_copy_count = 0;
        for row in rows {
            let old_copy = row["payload"]["metadata_effects"]
                .as_array()
                .is_some_and(|effects| {
                    effects.iter().any(|effect| {
                        effect["type"].as_str() == Some("put-namespace-job")
                            && effect["job"]["pending_frames"]
                                .as_array()
                                .is_some_and(|frames| {
                                    frames.iter().any(|frame| {
                                        frame["type"].as_str() == Some("content-copy")
                                            && frame["state"].get("source_release").is_none()
                                    })
                                })
                    })
                });
            if !old_copy {
                continue;
            }
            old_copy_count += 1;
            let original = row.clone();
            legacy_wire::validate_original_v7_record(&row).unwrap();
            validate_prepare(&row).unwrap();
            assert!(serde_json::from_value::<DfsPreparedOperationRecord>(row.clone()).is_err());
            let converted = payload_data(row["payload"].clone()).unwrap();
            assert!(converted.metadata_effects.iter().any(|effect| matches!(effect,
                DfsPreparedMetadataEffect::PutNamespaceJob { job }
                    if job.pending_frames.iter().any(|frame| matches!(frame,
                        DfsNamespaceJobFrame::ContentCopy { state } if state.source_release.is_none())))));
            // The typed converted body is distinct DATA, never a replacement
            // of the original source hash or an authorization to publish.
            assert_eq!(row, original);
            let mut tampered = row;
            tampered["payload"]["operation_id"] =
                Value::String("changed-original-operation".into());
            assert!(legacy_wire::validate_original_v7_record(&tampered).is_err());
        }
        assert!(
            old_copy_count > 0,
            "actual R125 normal producer must retain copy state without the new stage"
        );
        inventory.verify(dir.path()).unwrap();
    }
}
