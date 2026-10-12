//! Read-only retained ACK floors. The caller owns the physical source writer
//! fence; these shard-scoped files do not encode a filesystem identity.
use super::{MetadataBackendError, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::object::serde_ext;
use sinomo_core::storage::durable_file::{
    DurableLengthEndian, DurableRecordReadOptions, DurableRecordReader,
};
use sinomo_dfs::{DfsFileSystemId, DfsMetadataPrefixProof, DfsMetadataShardId};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io;
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyAcknowledgement {
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) journal_sequence: u64,
    #[serde(default)]
    pub(crate) state_fingerprint: String,
    #[serde(default)]
    pub(crate) durable_prefix_proof: Option<DfsMetadataPrefixProof>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Pointer {
    ack_floor_log_format: u32,
    kind: String,
    shard_id: DfsMetadataShardId,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
struct Record {
    format: u32,
    kind: String,
    shard_id: DfsMetadataShardId,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    checksum: String,
}
fn invalid(message: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(message.to_string())
}
fn io_error(path: &Path, source: io::Error) -> MetadataBackendError {
    MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    }
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_ext::from_bytes(bytes).map_err(invalid)
}
fn sidecar(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(path, error)),
    }
}
fn record(bytes: &[u8], kind: &str) -> Result<Record> {
    let record: Record = decode(bytes)?;
    if record.format != 1
        || record.kind != kind
        || record.checksum != blake3::hash(&record.payload).to_hex().as_str()
    {
        return Err(invalid(
            "retained acknowledgement log has invalid header/checksum",
        ));
    }
    Ok(record)
}
fn floor(bytes: &[u8], kind: &str, shard: &DfsMetadataShardId) -> Result<LegacyAcknowledgement> {
    let floor: LegacyAcknowledgement = decode(bytes)?;
    if &floor.shard_id != shard
        || kind == "authority"
            && (floor.journal_sequence == 0 || !floor.state_fingerprint.is_empty())
        || kind == "prefix"
            && floor.state_fingerprint.is_empty()
            && floor.durable_prefix_proof.is_none()
    {
        return Err(invalid(
            "retained acknowledgement has invalid scope or evidence",
        ));
    }
    if let Some(proof) = &floor.durable_prefix_proof {
        super::source_proof::validate(proof)?;
        if proof.journal_sequence != floor.journal_sequence {
            return Err(invalid("retained acknowledgement proof sequence differs"));
        }
    }
    Ok(floor)
}
fn merge(
    current: &LegacyAcknowledgement,
    incoming: &LegacyAcknowledgement,
) -> Result<LegacyAcknowledgement> {
    if incoming.journal_sequence != current.journal_sequence {
        return Ok(if incoming.journal_sequence > current.journal_sequence {
            incoming
        } else {
            current
        }
        .clone());
    }
    if !current.state_fingerprint.is_empty()
        && !incoming.state_fingerprint.is_empty()
        && current.state_fingerprint != incoming.state_fingerprint
        || current
            .durable_prefix_proof
            .as_ref()
            .zip(incoming.durable_prefix_proof.as_ref())
            .is_some_and(|(old, new)| old != new)
    {
        return Err(invalid(
            "retained acknowledgement forks at the same sequence",
        ));
    }
    let mut result = incoming.clone();
    if result.state_fingerprint.is_empty() {
        result.state_fingerprint = current.state_fingerprint.clone();
    }
    result.durable_prefix_proof = result
        .durable_prefix_proof
        .or_else(|| current.durable_prefix_proof.clone());
    Ok(result)
}
fn load(
    path: &Path,
    kind: &str,
    shard: &DfsMetadataShardId,
) -> Result<Option<LegacyAcknowledgement>> {
    let lock_path = path.with_extension("lock");
    let _lock = match File::open(&lock_path) {
        Ok(file) => {
            FileExt::lock_shared(&file).map_err(|error| io_error(&lock_path, error))?;
            Some(file)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(io_error(&lock_path, error)),
    };
    let mut migrated = false;
    let legacy = match sidecar(path)? {
        Some(bytes) => match decode::<Pointer>(&bytes) {
            Ok(pointer) => {
                if pointer.ack_floor_log_format != 1
                    || pointer.kind != kind
                    || &pointer.shard_id != shard
                {
                    return Err(invalid(
                        "retained acknowledgement pointer has foreign format/kind/shard",
                    ));
                }
                migrated = true;
                None
            }
            Err(_) => Some(floor(&bytes, kind, shard)?),
        },
        None => None,
    };
    let log_path = path.with_extension("slog");
    let file = match File::open(&log_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !migrated => return Ok(legacy),
        Err(error) => return Err(io_error(&log_path, error)),
    };
    let mut records = DurableRecordReader::new(
        file,
        &log_path,
        DurableRecordReadOptions::new(DurableLengthEndian::Little).max_record_bytes(16 * 1024),
    )
    .map_err(invalid)?;
    let mut current: Option<LegacyAcknowledgement> = None;
    let mut count = 0usize;
    while let Some(bytes) = records.next_record().map_err(invalid)? {
        count += 1;
        if count > 128 {
            return Err(invalid(
                "retained acknowledgement exceeds its source checkpoint interval",
            ));
        }
        let record = record(&bytes.bytes, kind)?;
        if &record.shard_id != shard {
            return Err(invalid(
                "retained acknowledgement log has invalid header/checksum",
            ));
        }
        let next = floor(&record.payload, kind, shard)?;
        if let Some(previous) = &current {
            if next.journal_sequence < previous.journal_sequence || merge(previous, &next)? != next
            {
                return Err(invalid(
                    "retained acknowledgement log regresses sequence or evidence",
                ));
            }
        }
        current = Some(next);
    }
    if migrated && current.is_none() {
        return Err(invalid(
            "migrated acknowledgement log has no complete floor",
        ));
    }
    // An interrupted pointer publication may legitimately leave an older plain
    // floor beside a newer log, or an old writer may have advanced that floor.
    match (current, legacy) {
        (Some(current), Some(legacy)) => Ok(Some(merge(&current, &legacy)?)),
        (current, legacy) => Ok(current.or(legacy)),
    }
}

// Discovery reads the encoded identity, never a destination/owner/root label.
fn encoded_shard(path: &Path, kind: &str) -> Result<Option<DfsMetadataShardId>> {
    if let Some(bytes) = sidecar(path)? {
        let shard = match decode::<Pointer>(&bytes) {
            Ok(pointer) => {
                if pointer.ack_floor_log_format != 1 || pointer.kind != kind {
                    return Err(invalid("foreign retained acknowledgement pointer"));
                }
                pointer.shard_id
            }
            Err(_) => decode::<LegacyAcknowledgement>(&bytes)?.shard_id,
        };
        return Ok(Some(shard));
    }
    let log_path = path.with_extension("slog");
    let file = match File::open(&log_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(&log_path, error)),
    };
    let mut reader = DurableRecordReader::new(
        file,
        &log_path,
        DurableRecordReadOptions::new(DurableLengthEndian::Little).max_record_bytes(16 * 1024),
    )
    .map_err(invalid)?;
    let Some(bytes) = reader.next_record().map_err(invalid)? else {
        return Ok(None);
    };
    let record = record(&bytes.bytes, kind)?;
    floor(&record.payload, kind, &record.shard_id)?;
    Ok(Some(record.shard_id))
}
/// Choose source DATA decoding from its actual declared generation, never by
/// trying the old reader after a failed current decode. Callers retain their
/// existing actual source writer, identity and native authority checks.
pub(crate) fn acknowledged_source_floors_for_root(
    root: &Path,
    file_system_id: &DfsFileSystemId,
    source_format: Option<u16>,
    shard: &DfsMetadataShardId,
) -> Result<Vec<LegacyAcknowledgement>> {
    let current = super::current_root(root)?;
    if source_format != Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION) {
        if current {
            return Err(invalid(
                "actual current ACK source cannot use a legacy format declaration",
            ));
        }
        return legacy_acknowledged_source_floors_for_root(root, file_system_id);
    }
    if !current {
        return Err(invalid(
            "declared current ACK source lacks its actual native generation header",
        ));
    }
    let (prefix, authority) =
        sinomo_dfs::read_current_metadata_acknowledgement_floors(root, shard)?;
    let mut floors = Vec::new();
    if let Some(prefix) = prefix {
        floors.push(LegacyAcknowledgement {
            shard_id: prefix.shard_id,
            journal_sequence: prefix.journal_sequence,
            state_fingerprint: prefix.state_fingerprint,
            durable_prefix_proof: Some(prefix.durable_prefix_proof),
        });
    }
    if let Some(authority) = authority {
        let floor = LegacyAcknowledgement {
            shard_id: authority.shard_id,
            journal_sequence: authority.journal_sequence,
            state_fingerprint: String::new(),
            durable_prefix_proof: Some(authority.durable_prefix_proof),
        };
        for prefix in &floors {
            // Preserve the existing same-sequence full-proof fork rejection.
            let _ = merge(prefix, &floor)?;
        }
        floors.push(floor);
    }
    Ok(floors)
}

pub(crate) fn legacy_acknowledged_source_floors_for_root(
    root: &Path,
    _file_system_id: &DfsFileSystemId,
) -> Result<Vec<LegacyAcknowledgement>> {
    let mut paths = BTreeSet::new();
    for kind in ["prefix", "authority"] {
        let directory = root
            .join("coordination")
            .join(format!("metadata-{kind}-ack-floors"));
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error(&directory, error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| io_error(&directory, error))?;
            let path = entry.path();
            if !matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("sobj" | "slog")
            ) {
                continue;
            }
            if !entry
                .file_type()
                .map_err(|error| io_error(&path, error))?
                .is_file()
            {
                return Err(invalid(
                    "retained acknowledgement leaf is not a regular file",
                ));
            }
            paths.insert((kind, path.with_extension("sobj")));
        }
    }
    let mut result = Vec::new();
    let mut by_shard: BTreeMap<DfsMetadataShardId, LegacyAcknowledgement> = BTreeMap::new();
    for (kind, path) in paths {
        let Some(shard) = encoded_shard(&path, kind)? else {
            continue;
        };
        let key = shard.0.trim_matches('/').replace(['/', '\\', ':'], "_");
        if path.file_name().and_then(|name| name.to_str()) != Some(format!("{key}.sobj").as_str()) {
            return Err(invalid(
                "retained acknowledgement file does not match its encoded source shard",
            ));
        }
        if let Some(ack) = load(&path, kind, &shard)? {
            if let Some(previous) = by_shard.get(&shard) {
                by_shard.insert(shard, merge(previous, &ack)?);
            } else {
                by_shard.insert(shard, ack.clone());
            }
            result.push(ack);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use sinomo_core::object::LengthMode;
    use std::fs;
    fn encode<T: serde::Serialize>(value: &T) -> Vec<u8> {
        serde_ext::to_bytes(value, 1, LengthMode::U32).unwrap()
    }
    fn path(root: &Path, kind: &str) -> std::path::PathBuf {
        root.join(format!(
            "coordination/metadata-{kind}-ack-floors/source.sobj"
        ))
    }
    fn write(path: &Path, bytes: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    fn row(sequence: u64, state: &str) -> Value {
        json!({"shard_id":"source","journal_sequence":sequence,"state_fingerprint":state})
    }
    fn frame(value: &Value, kind: &str) -> Vec<u8> {
        let payload = encode(value);
        let bytes = encode(&Record {
            format: 1,
            kind: kind.into(),
            shard_id: DfsMetadataShardId::new("source"),
            checksum: blake3::hash(&payload).to_hex().to_string(),
            payload,
        });
        let mut frame = (bytes.len() as u32).to_le_bytes().to_vec();
        frame.extend(bytes);
        frame
    }
    fn pointer() -> Vec<u8> {
        encode(&json!({"ack_floor_log_format":1,"kind":"prefix","shard_id":"source"}))
    }
    #[test]
    fn real_source_cuts_preserve_plain_evidence_and_ignore_only_incomplete_eof() {
        let root = sinomo_testdir::TestDir::new("migration-source-ack-floor");
        let p = path(&root, "prefix");
        let log = p.with_extension("slog");
        let fs = DfsFileSystemId::generate();
        write(&p, &encode(&row(3, "state-3")));
        write(&log, &frame(&row(4, "state-4"), "prefix"));
        assert_eq!(
            legacy_acknowledged_source_floors_for_root(&root, &fs)
                .unwrap()
                .iter()
                .map(|row| row.journal_sequence)
                .max()
                .unwrap_or(0),
            4
        );
        write(&p, &encode(&row(5, "state-5")));
        assert_eq!(
            legacy_acknowledged_source_floors_for_root(&root, &fs)
                .unwrap()
                .iter()
                .map(|row| row.journal_sequence)
                .max()
                .unwrap_or(0),
            5
        );
        write(&p, &pointer());
        let complete = frame(&row(6, "state-6"), "prefix");
        for cut in [1, 3, 4, complete.len() / 2, complete.len() - 1] {
            let mut bytes = frame(&row(4, "state-4"), "prefix");
            bytes.extend(&complete[..cut]);
            write(&log, &bytes);
            let floors = legacy_acknowledged_source_floors_for_root(&root, &fs).unwrap();
            assert_eq!(
                floors
                    .iter()
                    .map(|row| row.journal_sequence)
                    .max()
                    .unwrap_or(0),
                4
            );
            assert_eq!(
                floors
                    .iter()
                    .find(|row| !row.state_fingerprint.is_empty())
                    .unwrap()
                    .state_fingerprint,
                "state-4"
            );
            assert_eq!(
                fs::read(&log).unwrap(),
                bytes,
                "read cannot repair source tail"
            );
            assert_eq!(fs::read(&p).unwrap(), pointer());
        }
    }
    #[test]
    #[ignore = "requires genuine saved native ACK source root with complete normal prefix and authority ledgers"]
    fn actual_native_ack_ledgers_use_current_reader_and_reject_legacy_or_corrupt_data() {
        let root = std::path::PathBuf::from(
            std::env::var("SINOMO_MIGRATION_CURRENT_ACK_SOURCE_ROOT")
                .expect("operator must supply genuine saved native root DATA"),
        );
        let shard = DfsMetadataShardId::new("root");
        let fs = sinomo_dfs::sifs_file_system_id();
        assert!(super::super::current_root(&root).unwrap());
        let old_error = legacy_acknowledged_source_floors_for_root(&root, &fs)
            .unwrap_err()
            .to_string();
        assert!(
            old_error.contains("invalid type: map, expected option"),
            "{old_error}"
        );
        let (prefix, authority) =
            sinomo_dfs::read_current_metadata_acknowledgement_floors(&root, &shard).unwrap();
        let prefix = prefix.expect("genuine actual prefix ledger required");
        let authority = authority.expect("genuine actual authority ledger required");
        let floors = acknowledged_source_floors_for_root(
            &root,
            &fs,
            Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
            &shard,
        )
        .unwrap();
        assert_eq!(floors.len(), 2);
        assert_eq!(floors[0].shard_id, prefix.shard_id);
        assert_eq!(floors[0].journal_sequence, prefix.journal_sequence);
        assert_eq!(floors[0].state_fingerprint, prefix.state_fingerprint);
        assert_eq!(
            floors[0].durable_prefix_proof.as_ref(),
            Some(&prefix.durable_prefix_proof)
        );
        assert_eq!(floors[1].shard_id, authority.shard_id);
        assert_eq!(floors[1].journal_sequence, authority.journal_sequence);
        assert_eq!(
            floors[1].durable_prefix_proof.as_ref(),
            Some(&authority.durable_prefix_proof)
        );
        assert!(floors[1].state_fingerprint.is_empty());
        for wrong_format in [None, Some(6), Some(8)] {
            assert!(acknowledged_source_floors_for_root(&root, &fs, wrong_format, &shard).is_err());
        }
        assert!(
            acknowledged_source_floors_for_root(
                &root,
                &fs,
                Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
                &DfsMetadataShardId::new("foreign")
            )
            .is_err()
        );
        // Shared local DATA utility copies exact immutable files. No reader
        // branch manufactures a healthy ACK, format declaration or prefix.
        let copied = sinomo_testdir::TestDir::new("actual-native-ack-source-data-negatives");
        for kind in ["prefix", "authority"] {
            let relative = format!("coordination/metadata-{kind}-ack-floors");
            let destination = copied.join(&relative);
            std::fs::create_dir_all(&destination).unwrap();
            for suffix in ["sobj", "slog"] {
                std::fs::copy(
                    root.join(&relative).join(format!("root.{suffix}")),
                    destination.join(format!("root.{suffix}")),
                )
                .unwrap();
            }
        }
        let prefix_directory = copied.join("coordination/metadata-prefix-ack-floors");
        std::fs::copy(
            prefix_directory.join("root.sobj"),
            prefix_directory.join("foreign.sobj"),
        )
        .unwrap();
        assert!(sinomo_dfs::read_current_metadata_acknowledgement_floors(&copied, &shard).is_err());
        std::fs::remove_file(prefix_directory.join("foreign.sobj")).unwrap();
        let log = prefix_directory.join("root.slog");
        let original = std::fs::read(&log).unwrap();
        let mut corrupted = original.clone();
        let index = corrupted.len() / 2;
        corrupted[index] ^= 1;
        std::fs::write(&log, corrupted).unwrap();
        assert!(sinomo_dfs::read_current_metadata_acknowledgement_floors(&copied, &shard).is_err());
        std::fs::write(&log, original).unwrap();
        assert!(
            sinomo_dfs::read_current_metadata_acknowledgement_floors(&copied, &shard)
                .unwrap()
                .0
                .is_some()
        );
        std::fs::remove_file(&log).unwrap();
        assert!(sinomo_dfs::read_current_metadata_acknowledgement_floors(&copied, &shard).is_err());
    }

    #[test]
    fn real_pointer_requires_complete_log_and_rejects_regression_fork_foreign_scope_and_corruption()
    {
        let root = sinomo_testdir::TestDir::new("migration-source-ack-floor-invalid");
        let p = path(&root, "prefix");
        let log = p.with_extension("slog");
        let fs = DfsFileSystemId::generate();
        let read = || legacy_acknowledged_source_floors_for_root(&root, &fs);
        write(&p, &pointer());
        assert!(read().is_err());
        write(&log, b"");
        assert!(read().is_err());
        for next in [
            row(3, "stale"),
            row(4, "fork"),
            json!({"shard_id":"other","journal_sequence":5,"state_fingerprint":"foreign"}),
        ] {
            let mut bytes = frame(&row(4, "state-4"), "prefix");
            bytes.extend(frame(&next, "prefix"));
            write(&log, &bytes);
            assert!(read().is_err());
            assert_eq!(fs::read(&log).unwrap(), bytes);
        }
        let mut bytes = frame(&row(4, "state-4"), "prefix");
        let end = bytes.len() - 1;
        bytes[end] ^= 1;
        write(&log, &bytes);
        assert!(read().is_err());
        write(&log, &frame(&row(4, "state-4"), "authority"));
        assert!(read().is_err());
        write(&p, b"complete malformed sidecar");
        assert!(read().is_err());
    }
    #[test]
    fn equal_sequence_exact_prefix_forks_across_source_floor_kinds_fail() {
        let root = sinomo_testdir::TestDir::new("migration-source-ack-floor-proof-fork");
        let proof = |byte: &str| DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 7,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", byte.repeat(64)),
        };
        let prefix = LegacyAcknowledgement {
            shard_id: DfsMetadataShardId::new("source"),
            journal_sequence: 7,
            state_fingerprint: "state-7".into(),
            durable_prefix_proof: Some(proof("a")),
        };
        write(&path(&root, "prefix"), &encode(&prefix));
        let mut authority = LegacyAcknowledgement {
            shard_id: DfsMetadataShardId::new("source"),
            journal_sequence: 7,
            state_fingerprint: String::new(),
            durable_prefix_proof: Some(proof("b")),
        };
        write(&path(&root, "authority"), &encode(&authority));
        let fs = DfsFileSystemId::generate();
        assert!(legacy_acknowledged_source_floors_for_root(&root, &fs).is_err());
        authority.durable_prefix_proof = Some(proof("a"));
        write(&path(&root, "authority"), &encode(&authority));
        let floors = legacy_acknowledged_source_floors_for_root(&root, &fs).unwrap();
        assert_eq!(
            floors
                .iter()
                .map(|row| row.journal_sequence)
                .max()
                .unwrap_or(0),
            7
        );
        assert_eq!(
            floors
                .iter()
                .find(|row| !row.state_fingerprint.is_empty())
                .unwrap()
                .state_fingerprint,
            "state-7"
        );
        assert_eq!(
            floors
                .iter()
                .find(|row| row.state_fingerprint.is_empty())
                .unwrap()
                .durable_prefix_proof
                .as_ref()
                .unwrap()
                .digest,
            format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64))
        );
    }
    #[test]
    fn discovery_checks_encoded_physical_shards_instead_of_destination_labels() {
        let root = sinomo_testdir::TestDir::new("migration-source-ack-floor-discovery");
        let first = path(&root, "prefix");
        write(&first, &encode(&row(8, "first")));
        let other = first.with_file_name("physical-other.sobj");
        write(
            &other,
            &encode(
                &json!({"shard_id":"physical-other","journal_sequence":27,"state_fingerprint":"ahead"}),
            ),
        );
        let fs = DfsFileSystemId::generate();
        let floors = legacy_acknowledged_source_floors_for_root(&root, &fs).unwrap();
        assert_eq!(
            floors.iter().map(|floor| floor.journal_sequence).max(),
            Some(27)
        );
        assert!(
            floors
                .iter()
                .any(|floor| floor.shard_id == DfsMetadataShardId::new("physical-other"))
        );
        let wrong = first.with_file_name("wrong-label.sobj");
        write(
            &wrong,
            &encode(
                &json!({"shard_id":"encoded-source","journal_sequence":99,"state_fingerprint":"misplaced"}),
            ),
        );
        assert!(legacy_acknowledged_source_floors_for_root(&root, &fs).is_err());
        assert!(
            wrong.exists(),
            "discovery cannot repair or remove source evidence"
        );
    }
}
