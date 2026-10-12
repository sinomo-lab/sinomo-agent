//! Source-row membership for pre-mount content work. Staging is a cache only.
use super::*;
use sinomo_dfs::{DfsObjectId, DfsObjectMetadata, DfsObjectVersionRecord, DfsVersion};

/// Live final-head membership, owned by verified source reconstruction. It is
/// neither serializable authority nor a grant; the writer FD bounds its life.
#[derive(Debug)]
pub(super) struct MetadataSourceCurrentMembership {
    _source_lock: Arc<File>,
    scope: MetadataSourceContentWorkScope,
    staged_root: PathBuf,
    inventory: source::SourceInventory,
    verified_points: BTreeMap<Vec<u8>, source::VerifiedRecordPoint>,
    objects: BTreeMap<(DfsFileSystemId, DfsObjectId), DfsObjectMetadata>,
    versions: BTreeMap<
        (DfsFileSystemId, DfsObjectId, DfsVersion),
        (Option<String>, Option<u64>, Vec<DfsObjectMetadata>),
    >,
}
impl MetadataSourceCurrentMembership {
    pub(super) fn stage_root(&self) -> &Path {
        &self.staged_root
    }
    pub(super) fn scope_shard(&self) -> &DfsMetadataShardId {
        &self.scope.scope_shard
    }
    pub(super) fn matches_scope(&self, scope: &MetadataSourceContentWorkScope) -> bool {
        self.scope.source_identity == scope.source_identity
            && self.scope.source_snapshot_fingerprint == scope.source_snapshot_fingerprint
            && self.scope.source_journal_sequence == scope.source_journal_sequence
            && self.scope.scope_shard == scope.scope_shard
            && self.scope.file_system_ids == scope.file_system_ids
            && self.scope.projection_assignments == scope.projection_assignments
    }
}
pub(super) fn capture_current_membership(
    session: &mut Session,
    scope: &MetadataSourceContentWorkScope,
) -> Result<Arc<MetadataSourceCurrentMembership>> {
    if !session.replay_validated {
        return Err(error(
            "source membership requires complete independently verified replay",
        ));
    }
    let tables = session.projection.current_tables()?;
    let mut objects = tables
        .compacted_tombstones
        .into_iter()
        .map(|object| {
            (
                (object.file_system_id.clone(), object.object_id.clone()),
                object,
            )
        })
        .collect::<BTreeMap<_, _>>();
    for object in tables.objects {
        objects.insert(
            (object.file_system_id.clone(), object.object_id.clone()),
            object,
        );
    }
    Ok(Arc::new(MetadataSourceCurrentMembership {
        _source_lock: Arc::clone(&session._source_lock),
        scope: scope.clone(),
        staged_root: session.staged_root.clone(),
        inventory: session.inventory.clone(),
        verified_points: session.verified_source_points.clone(),
        objects,
        versions: tables
            .versions
            .into_iter()
            .map(|version| {
                (
                    (version.file_system_id, version.object_id, version.version),
                    (
                        version.content_hash.clone(),
                        version.size_bytes,
                        version
                            .retention
                            .values()
                            .filter_map(|state| state.grant.as_ref())
                            .map(|grant| grant.object.as_ref().clone())
                            .collect(),
                    ),
                )
            })
            .collect(),
    }))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetadataSourceVersionMembership {
    point: source::SourceCursor,
    record_digest: String,
    row_pointer: String,
    row_digest: String,
    prepared_version_index: Option<usize>,
    // Captured from the actual row, never reconstructed from another revision.
    source_object: Option<DfsObjectMetadata>,
    locator: Option<MetadataSourceContentLocator>,
    content_hash: Option<String>,
    size_bytes: Option<u64>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MetadataSourceObjectWitness {
    QualifiedCapture,
    CurrentPolicy,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataSourceVersionEvidence {
    pub source_identity: String,
    pub source_snapshot_fingerprint: String,
    pub source_journal_sequence: u64,
    pub object: DfsObjectMetadata,
    pub version: DfsObjectVersionRecord,
    pub locator: MetadataSourceContentLocator,
    pub object_witness: MetadataSourceObjectWitness,
    membership: MetadataSourceVersionMembership,
    object_membership: Option<MetadataSourceVersionMembership>,
}
fn digest(value: &impl Serialize) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinomo.dfs-migration.source-row.v1\0");
    serde_json::to_writer(&mut hash, value).map_err(error)?;
    Ok(hash.finalize().to_hex().to_string())
}
fn path(
    stage: &Path,
    fs: &DfsFileSystemId,
    object: &DfsObjectId,
    version: &DfsVersion,
) -> Result<PathBuf> {
    let key = blake3::hash(&serde_json::to_vec(&(fs, object, version)).map_err(error)?)
        .to_hex()
        .to_string();
    Ok(stage
        .join("source-version-evidence")
        .join(format!("{key}.json")))
}
fn qualified(version: &DfsObjectVersionRecord) -> Option<DfsObjectMetadata> {
    version
        .retention
        .values()
        .filter_map(|state| state.grant.as_ref())
        .map(|grant| grant.object.as_ref())
        .find(|object| {
            object.file_system_id == version.file_system_id
                && object.object_id == version.object_id
                && object.version == version.version
                && object.content_hash == version.content_hash
                && object.content_size_bytes == version.size_bytes
        })
        .cloned()
}
// The traversal follows schema-owned containers only. It never walks opaque
// module metadata, module payloads, or arbitrary object fields.
fn rows<'a>(value: &'a Value, pointer: String, found: &mut Vec<(String, bool, &'a Value)>) {
    let identity = value.get("file_system_id").is_some()
        && value.get("object_id").is_some()
        && value.get("version").is_some_and(Value::is_string);
    if identity && value.get("path").is_some_and(Value::is_string) {
        found.push((pointer, false, value));
        return;
    }
    if identity
        && [
            "size_bytes",
            "locations",
            "visible",
            "history_order",
            "retained",
        ]
        .iter()
        .any(|key| value.get(*key).is_some())
    {
        found.push((pointer, true, value));
        return;
    }
    for key in [
        "transaction",
        "record",
        "payload",
        "version_record",
        "visible_version",
        "version",
        "current_object",
        "source_object",
        "object_metadata",
        "committed_object",
        "object",
        "moved_object",
        "alias",
        "committed_target",
        "intent",
        "prepared_intent",
        "terminal_intent",
        "commit",
        "prepare",
    ] {
        if let Some(child) = value.get(key).filter(|child| child.is_object()) {
            rows(child, format!("{pointer}/{key}"), found);
        }
    }
    for key in [
        "versions",
        "objects",
        "metadata_effects",
        "ops",
        "pending_frames",
    ] {
        if let Some(children) = value.get(key).and_then(Value::as_array) {
            for (index, child) in children.iter().enumerate() {
                rows(child, format!("{pointer}/{key}/{index}"), found);
            }
        }
    }
}
fn operation_rows<'a>(op: &'a Value, prefix: &str, found: &mut Vec<(String, bool, &'a Value)>) {
    let fields: &[&str] = match op.get("type").and_then(Value::as_str) {
        Some("put-object" | "put-compacted-tombstone") => &["object"],
        Some("put-version") => &["version_record", "version"],
        Some("apply-namespace-transaction") => &["transaction"],
        Some("commit-visible-version") => &["visible_version", "committed_object"],
        _ => &[],
    };
    for key in fields {
        if let Some(row) = op.get(*key).filter(|row| row.is_object()) {
            rows(row, format!("{prefix}/{key}"), found);
        }
    }
}
fn record_rows(record: &source::SourceRecord) -> Vec<(String, bool, &Value)> {
    let mut found = Vec::new();
    match record {
        source::SourceRecord::SnapshotRow { table, row, .. } => {
            match table.replace('-', "_").as_str() {
                "versions" | "objects" | "compacted_tombstones" => {
                    rows(row, "/SnapshotRow/row".into(), &mut found)
                }
                "$ops" => operation_rows(row, "/SnapshotRow/row", &mut found),
                _ => {}
            }
        }
        source::SourceRecord::LooseRow { family, row, .. } => {
            match family.replace('-', "_").as_str() {
                "versions" | "objects" | "compacted_tombstones" => {
                    rows(row, "/LooseRow/row".into(), &mut found)
                }
                _ => {}
            }
        }
        source::SourceRecord::TableBatch { batch, .. } => {
            if let Some(ops) = batch.get("ops").and_then(Value::as_array) {
                for (i, op) in ops.iter().enumerate() {
                    operation_rows(op, &format!("/TableBatch/batch/ops/{i}"), &mut found);
                }
            }
        }
        source::SourceRecord::JournalFrame { entry, .. } => {
            if let Some(op) = entry.get("mutation") {
                operation_rows(op, "/JournalFrame/entry/mutation", &mut found);
            }
        }
        _ => {}
    }
    found
}
fn original_prepare(record: &source::SourceRecord) -> Option<(&str, &Value)> {
    match record {
        source::SourceRecord::SnapshotRow { table, row, .. }
            if table.replace('-', "_") == "namespace_prepares" =>
        {
            Some(("/SnapshotRow/row", row))
        }
        source::SourceRecord::JournalFrame { entry, .. } => entry
            .get("mutation")
            .filter(|op| op.get("type").and_then(Value::as_str) == Some("put-namespace-prepare"))
            .map(|op| ("/JournalFrame/entry/mutation", op)),
        _ => None,
    }
}
fn source_pointer(record: &source::SourceRecord, pointer: &str) -> Result<String> {
    let base = match record {
        source::SourceRecord::SnapshotRow { .. } => "/SnapshotRow/row",
        source::SourceRecord::LooseRow { .. } => "/LooseRow/row",
        source::SourceRecord::TableBatch { .. } => "/TableBatch/batch",
        source::SourceRecord::JournalFrame { .. } => "/JournalFrame/entry",
        _ => return Err(error("source row lacks original document context")),
    };
    pointer
        .strip_prefix(base)
        .map(str::to_owned)
        .ok_or_else(|| error("source row pointer leaves its original record"))
}
fn adapted_source_version(
    record: &source::SourceRecord,
    pointer: &str,
    raw: &Value,
) -> Result<Value> {
    let original = record
        .raw_document_context()?
        .ok_or_else(|| error("source version has no original type document"))?;
    let mut adapted = raw.clone();
    let paths = crate::legacy_wire::original_module_payload_paths(raw, "version", "")?;
    crate::legacy_wire::normalize_original_module_payloads_at(
        &mut adapted,
        &original,
        &paths,
        &source_pointer(record, pointer)?,
    )?;
    Ok(adapted)
}
fn decode_source_prepare(record: &source::SourceRecord, value: &Value) -> Result<Value> {
    let original = record
        .raw_document_context()?
        .ok_or_else(|| error("source prepare has no original type document"))?;
    let packed = ["packed_prepare_v1", "packed_prepare_v2"]
        .iter()
        .any(|key| value.get(*key).is_some_and(|value| !value.is_null()));
    let prefix = if matches!(record, source::SourceRecord::JournalFrame { .. }) {
        "/mutation"
    } else {
        ""
    };
    let (mut decoded, mappings) = if packed {
        let decoded = crate::legacy_wire::decode_original_packed_prepare(value)?;
        (decoded.value, Some(decoded.payload_sources))
    } else {
        let row = value
            .get("prepare")
            .filter(|value| !value.is_null())
            .unwrap_or(value);
        crate::legacy_wire::verify_raw_namespace_prepare(row, None)?;
        (row.clone(), None)
    };
    let mut paths = Vec::new();
    for (key, schema) in [
        ("transaction", "transaction"),
        ("completed_request", "request"),
        ("completed_request_completion", "completion"),
    ] {
        if let Some(row) = decoded.get(key).filter(|row| !row.is_null()) {
            paths.extend(crate::legacy_wire::original_module_payload_paths(
                row,
                schema,
                &format!("/{key}"),
            )?);
        }
    }
    if let Some(mappings) = mappings {
        let mapped = paths
            .iter()
            .map(|path| {
                mappings
                    .get(path)
                    .map(|raw| (path.clone(), format!("{prefix}{raw}")))
                    .ok_or_else(|| error("packed prepare lacks original payload type mapping"))
            })
            .collect::<Result<Vec<_>>>()?;
        crate::legacy_wire::normalize_original_module_payload_mappings(
            &mut decoded,
            &original,
            &mapped,
        )?;
    } else {
        let prefix = if value.get("prepare").is_some_and(|row| !row.is_null()) {
            format!("{prefix}/prepare")
        } else {
            prefix.into()
        };
        crate::legacy_wire::normalize_original_module_payloads_at(
            &mut decoded,
            &original,
            &paths,
            &prefix,
        )?;
    }
    Ok(decoded)
}
fn prepared_source_version(
    record: &source::SourceRecord,
    value: &Value,
    index: usize,
) -> Result<Value> {
    let decoded = decode_source_prepare(record, value)?;
    prepared_source_version_decoded(value, &decoded, index)
}
fn prepared_source_version_decoded(value: &Value, decoded: &Value, index: usize) -> Result<Value> {
    let mut row = decoded
        .get("transaction")
        .and_then(|tx| tx.get("versions"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.get(index))
        .cloned()
        .ok_or_else(|| error("source prepare version index is invalid"))?;
    let original = value
        .get("prepare")
        .filter(|value| !value.is_null())
        .unwrap_or(value);
    let locator = if let Some(packed) = value
        .get("packed_prepare_v1")
        .filter(|value| !value.is_null())
    {
        let row = packed
            .get("t")
            .and_then(|tx| tx.get("v"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.get(index))
            .ok_or_else(|| error("source v1 version locator is outside its real body"))?;
        MetadataSourceContentLocator {
            object_id: row
                .get("o")
                .and_then(Value::as_str)
                .ok_or_else(|| error("source v1 object locator is not text"))?
                .into(),
            version: row
                .get("v")
                .and_then(Value::as_str)
                .ok_or_else(|| error("source v1 version locator is not text"))?
                .into(),
            content_version: row
                .get("d")
                .filter(|plan| !plan.is_null())
                .map(|plan| {
                    plan.get("source_version")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| error("source v1 content generation is not text"))
                })
                .transpose()?,
        }
    } else if let Some(packed) = value
        .get("packed_prepare_v2")
        .filter(|value| !value.is_null())
    {
        let lookup = |column: &str, dictionary: &str| -> Result<String> {
            let slot = packed
                .get("v")
                .and_then(|cols| cols.get(column))
                .and_then(Value::as_array)
                .and_then(|rows| rows.get(index))
                .and_then(Value::as_u64)
                .and_then(|index| usize::try_from(index).ok())
                .ok_or_else(|| error("source packed version index is invalid"))?;
            packed
                .get("d")
                .and_then(|dict| dict.get(dictionary))
                .and_then(Value::as_array)
                .and_then(|rows| rows.get(slot))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| error("source packed locator is outside its actual dictionary"))
        };
        MetadataSourceContentLocator {
            object_id: lookup("o", "o")?,
            version: lookup("v", "v")?,
            content_version: packed
                .get("v")
                .and_then(|columns| columns.get("d"))
                .and_then(Value::as_array)
                .and_then(|plans| {
                    plans
                        .iter()
                        .find(|plan| plan.get("i").and_then(Value::as_u64) == Some(index as u64))
                })
                .map(|plan| {
                    plan.get("v")
                        .and_then(|value| value.get("source_version"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| error("source v2 content generation is not text"))
                })
                .transpose()?,
        }
    } else {
        crate::legacy_wire::source_content_identity(
            original
                .get("transaction")
                .and_then(|tx| tx.get("versions"))
                .and_then(Value::as_array)
                .and_then(|rows| rows.get(index))
                .ok_or_else(|| error("source canonical version locator is missing"))?,
        )?
        .ok_or_else(|| error("source canonical locator is missing"))?
        .3
    };
    row["object_id"] = Value::String(locator.object_id);
    row["version"] = Value::String(locator.version);
    if let Some(content_version) = locator.content_version {
        if !row.get("content_durability").is_some_and(Value::is_object) {
            return Err(error(
                "source packed content generation has no decoded durability",
            ));
        }
        row["content_durability"]["source_version"] = Value::String(content_version);
    } else if row
        .get("content_durability")
        .is_some_and(|plan| !plan.is_null())
    {
        return Err(error(
            "decoded durability lacks its original content generation",
        ));
    }
    // FS spelling also belongs to the original enclosing source record.
    row["file_system_id"] = value
        .get("file_system_id")
        .cloned()
        .or_else(|| original.get("file_system_id").cloned())
        .ok_or_else(|| error("source prepare omits its enclosing filesystem identity"))?;
    Ok(row)
}

/// Keep only independently replayed decoder boundaries for source membership.
pub(super) fn cache_verified_point(
    session: &mut Session,
    point: source::VerifiedRecordPoint,
    record: &source::SourceRecord,
) -> Result<()> {
    if !record_rows(record).is_empty() || original_prepare(record).is_some() {
        session.verified_source_points.insert(point.key()?, point);
    }
    Ok(())
}

/// Called once per real Source event with the decoder position captured before
/// reading it. Tail replay cannot replace raw source locators or provenance.
pub(super) fn capture(session: &mut Session, record: &source::SourceRecord) -> Result<()> {
    let found = record_rows(record);
    if found.is_empty() && original_prepare(record).is_none() {
        return Ok(());
    }
    let point = session
        .source_record_point
        .as_ref()
        .ok_or_else(|| error("actual source row lacks decoder provenance"))?
        .point();
    let record_digest = digest(record)?;
    for (row_pointer, is_version, raw) in found {
        // This digest is calculated on the actual source spelling BEFORE any
        // identifier, history, or policy normalization.
        let row_digest = digest(raw)?;
        let mut proof = MetadataSourceVersionMembership {
            point: point.point(),
            record_digest: record_digest.clone(),
            row_pointer,
            row_digest,
            prepared_version_index: None,
            source_object: None,
            locator: None,
            content_hash: None,
            size_bytes: None,
        };
        if is_version {
            let version = crate::legacy_wire::normalize_version(adapted_source_version(
                record,
                &proof.row_pointer,
                raw,
            )?)?;
            let (fs, object, rev, locator) = crate::legacy_wire::source_content_identity(raw)?
                .ok_or_else(|| error("actual source version lacks its raw locator"))?;
            if (fs.clone(), object.clone(), rev.clone())
                != (
                    version.file_system_id.clone(),
                    version.object_id.clone(),
                    version.version.clone(),
                )
            {
                return Err(error(
                    "source locator normalization changes the actual version tuple",
                ));
            }
            proof.content_hash = version.content_hash.clone();
            proof.size_bytes = version.size_bytes;
            if let Some(previous) =
                session
                    .source_content_membership
                    .get(&(fs.clone(), object.clone(), rev.clone()))
            {
                let canonical =
                    locator.object_id == object.as_str() && locator.version == rev.as_str();
                let older_named = previous.locator.as_ref().is_some_and(|old| {
                    old.object_id != object.as_str() || old.version != rev.as_str()
                });
                if canonical
                    && older_named
                    && previous.content_hash == proof.content_hash
                    && previous.size_bytes == proof.size_bytes
                {
                    // A later metadata serialization must not erase the exact
                    // older source spelling used by the retained manifest.
                    continue;
                }
            }
            proof.source_object = qualified(&version);
            proof.locator = Some(locator.clone());
            session
                .source_content_membership
                .insert((fs, object, rev), proof);
        } else {
            let object = crate::legacy_wire::normalize_object(raw.clone())?;
            proof.source_object = Some(object.clone());
            session.source_object_membership.insert(
                (object.file_system_id.clone(), object.object_id.clone()),
                proof,
            );
        }
    }
    if let Some((pointer, original)) = original_prepare(record) {
        let decoded = decode_source_prepare(record, original)?;
        let count = decoded
            .get("transaction")
            .and_then(|tx| tx.get("versions"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let raw_digest = digest(original)?;
        for index in 0..count {
            let raw = prepared_source_version_decoded(original, &decoded, index)?;
            let version = crate::legacy_wire::normalize_version(raw.clone())?;
            let (fs, object, rev, locator) = crate::legacy_wire::source_content_identity(&raw)?
                .ok_or_else(|| error("checked source prepare has no exact version locator"))?;
            // Existing real table-row provenance wins over a planned body.
            session
                .source_content_membership
                .entry((fs, object, rev))
                .or_insert_with(|| MetadataSourceVersionMembership {
                    point: point.point(),
                    record_digest: record_digest.clone(),
                    row_pointer: pointer.into(),
                    row_digest: raw_digest.clone(),
                    prepared_version_index: Some(index),
                    source_object: qualified(&version),
                    locator: Some(locator),
                    content_hash: version.content_hash,
                    size_bytes: version.size_bytes,
                });
        }
    }
    Ok(())
}
pub(super) fn source_locator(
    session: &Session,
    fs: &DfsFileSystemId,
    object: &DfsObjectId,
    version: &DfsVersion,
) -> Result<MetadataSourceContentLocator> {
    session
        .source_content_membership
        .get(&(fs.clone(), object.clone(), version.clone()))
        .and_then(|member| member.locator.clone())
        .ok_or_else(|| {
            error("immutable version has no independently captured raw source membership locator")
        })
}
pub(super) fn publish(session: &mut Session, captured: &CapturedSourceCut) -> Result<()> {
    let tables = session.projection.current_tables()?;
    for version in &tables.versions {
        let key = (
            version.file_system_id.clone(),
            version.object_id.clone(),
            version.version.clone(),
        );
        let Some(membership) = session.source_content_membership.get(&key).cloned() else {
            continue;
        };
        let locator = membership
            .locator
            .clone()
            .ok_or_else(|| error("proven source version lost its original locator"))?;
        let (object, object_witness, object_membership) =
            if let Some(object) = membership.source_object.clone() {
                (object, MetadataSourceObjectWitness::QualifiedCapture, None)
            } else {
                let Some(proof) = session
                    .source_object_membership
                    .get(&(key.0.clone(), key.1.clone()))
                    .cloned()
                else {
                    continue;
                };
                let object = proof
                    .source_object
                    .clone()
                    .ok_or_else(|| error("actual current source object lacks its row"))?;
                let current = tables
                    .objects
                    .iter()
                    .find(|current| {
                        current.file_system_id == object.file_system_id
                            && current.object_id == object.object_id
                    })
                    .or_else(|| {
                        tables.compacted_tombstones.iter().find(|current| {
                            current.file_system_id == object.file_system_id
                                && current.object_id == object.object_id
                        })
                    });
                if current != Some(&object) {
                    continue;
                }
                (
                    object,
                    MetadataSourceObjectWitness::CurrentPolicy,
                    Some(proof),
                )
            };
        let evidence = MetadataSourceVersionEvidence {
            source_identity: captured.source_identity.clone(),
            source_snapshot_fingerprint: captured.source_snapshot_fingerprint.clone(),
            source_journal_sequence: captured.source_journal_sequence,
            object,
            version: version.clone(),
            locator,
            object_witness,
            membership,
            object_membership,
        };
        let file = path(&session.staged_root, &key.0, &key.1, &key.2)?;
        fs::create_dir_all(
            file.parent()
                .ok_or_else(|| error("source evidence has no parent"))?,
        )
        .map_err(error)?;
        write_json(&file, &evidence)?;
    }
    Ok(())
}
fn actual_row(
    root: &Path,
    membership: &MetadataSourceVersionMembership,
    head: u64,
    current: &MetadataSourceCurrentMembership,
) -> Result<Value> {
    let point_key = serde_json::to_vec(&membership.point.point()).map_err(error)?;
    let record = match current.verified_points.get(&point_key) {
        Some(point) => point.read(root, &current.inventory)?,
        None => source::read_at_point(root, &membership.point)?,
    };
    if let source::SourceRecord::JournalFrame { entry, .. } = &record {
        if entry
            .get("sequence")
            .and_then(Value::as_u64)
            .is_none_or(|seq| seq > head)
        {
            return Err(error(
                "source membership frame is beyond the actual captured journal cut",
            ));
        }
    }
    if digest(&record)? != membership.record_digest {
        return Err(error(
            "actual source record differs from membership provenance",
        ));
    }
    let encoded = serde_json::to_value(&record).map_err(error)?;
    let row = encoded
        .pointer(&membership.row_pointer)
        .ok_or_else(|| error("source membership points outside its actual schema row"))?;
    if digest(row)? != membership.row_digest {
        return Err(error("actual raw source row differs before normalization"));
    }
    if let Some(index) = membership.prepared_version_index {
        let (pointer, original) = original_prepare(&record)
            .ok_or_else(|| error("source membership is not an actual supported prepare body"))?;
        if pointer != membership.row_pointer {
            return Err(error("source prepare membership changes its schema role"));
        }
        return prepared_source_version(&record, original, index);
    }
    let allowed = record_rows(&record);
    if !allowed
        .iter()
        .any(|(pointer, _, _)| pointer == &membership.row_pointer)
    {
        return Err(error("source membership enters opaque or non-row data"));
    }
    let is_version = allowed
        .iter()
        .find(|(pointer, _, _)| pointer == &membership.row_pointer)
        .is_some_and(|(_, is_version, _)| *is_version);
    if is_version {
        adapted_source_version(&record, &membership.row_pointer, row)
    } else {
        Ok(row.clone())
    }
}
pub fn lookup_owned_metadata_source_version(
    source: &MetadataSourceContentWorkScope,
    fs: &DfsFileSystemId,
    object: &DfsObjectId,
    version: &DfsVersion,
) -> Result<Option<MetadataSourceVersionEvidence>> {
    let current = source_current_membership(&source.source_root, source)?;
    lookup_metadata_source_version(
        &source.source_root,
        current.stage_root(),
        source,
        fs,
        object,
        version,
    )
}
/// An unsigned stage file supplies only a lookup hint. Actual source bytes,
/// bounded decoder state and immutable row identity are independently checked.
pub fn lookup_metadata_source_version(
    actual_source_root: &Path,
    actual_stage: &Path,
    source: &MetadataSourceContentWorkScope,
    fs: &DfsFileSystemId,
    object: &DfsObjectId,
    version: &DfsVersion,
) -> Result<Option<MetadataSourceVersionEvidence>> {
    let _fence = retain_source_fence(actual_source_root)?;
    validate_retained_source_identity(
        actual_source_root,
        &source.source_identity,
        &source.source_snapshot_fingerprint,
    )?;
    if !source.file_system_ids.contains(fs) {
        return Err(error("source content work requests a foreign filesystem"));
    }
    let current = source_current_membership(actual_source_root, source)?;
    let requested = (fs.clone(), object.clone(), version.clone());
    let immutable = current.versions.get(&requested).ok_or_else(|| {
        error("requested version is not in the actual verified final source view")
    })?;
    let file = path(actual_stage, fs, object, version)?;
    if !file.is_file() {
        return Ok(None);
    }
    let evidence: MetadataSourceVersionEvidence = read_json(&file)?;
    if evidence.source_identity != source.source_identity
        || evidence.source_snapshot_fingerprint != source.source_snapshot_fingerprint
        || evidence.source_journal_sequence != source.source_journal_sequence
        || evidence.version.file_system_id != *fs
        || evidence.version.object_id != *object
        || evidence.version.version != *version
        || evidence.object.file_system_id != *fs
        || evidence.object.object_id != *object
    {
        return Err(error(
            "source version evidence has a foreign cut or identity",
        ));
    }
    if (&evidence.version.content_hash, &evidence.version.size_bytes)
        != (&immutable.0, &immutable.1)
    {
        return Err(error(
            "staged version differs from verified final source immutable identity",
        ));
    }
    let raw = actual_row(
        actual_source_root,
        &evidence.membership,
        source.source_journal_sequence,
        &current,
    )?;
    let actual = crate::legacy_wire::normalize_version(raw.clone())?;
    let (actual_fs, actual_object, actual_version, locator) =
        crate::legacy_wire::source_content_identity(&raw)?
            .ok_or_else(|| error("actual source version has no raw content locator"))?;
    if (&actual_fs, &actual_object, &actual_version) != (fs, object, version)
        || locator != evidence.locator
        || actual.content_hash != evidence.version.content_hash
        || actual.size_bytes != evidence.version.size_bytes
        || actual.content_durability != evidence.version.content_durability
    {
        return Err(error(
            "staged content identity is not a member of the actual source version row",
        ));
    }
    match evidence.object_witness {
        MetadataSourceObjectWitness::QualifiedCapture => {
            if qualified(&actual).as_ref() != Some(&evidence.object)
                || !immutable.2.contains(&evidence.object)
                || evidence.object_membership.is_some()
            {
                return Err(error(
                    "qualified old object is not captured in the actual source version",
                ));
            }
        }
        MetadataSourceObjectWitness::CurrentPolicy => {
            let proof = evidence.object_membership.as_ref().ok_or_else(|| {
                error("current policy witness lacks actual source object membership")
            })?;
            let object = crate::legacy_wire::normalize_object(actual_row(
                actual_source_root,
                proof,
                source.source_journal_sequence,
                &current,
            )?)?;
            if object != evidence.object
                || current
                    .objects
                    .get(&(fs.clone(), evidence.object.object_id.clone()))
                    != Some(&object)
            {
                return Err(error(
                    "current policy witness restamps actual source object attributes",
                ));
            }
        }
    }
    Ok(Some(evidence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinomo_core::object::{LengthMode, serde_ext};

    pub(super) fn fixture() -> (
        sinomo_testdir::TestDir,
        PathBuf,
        PathBuf,
        MetadataSourceContentWorkScope,
        MetadataSourceVersionEvidence,
        Arc<MetadataSourceCurrentMembership>,
    ) {
        let temp = sinomo_testdir::TestDir::new("source-version-membership");
        let root = temp.join("source");
        let stage = temp.join("stage");
        fs::create_dir_all(root.join("records/tables")).unwrap();
        fs::create_dir_all(&stage).unwrap();
        let raw_object = json!({"file_system_id":"source-fs","object_id":"source-object","version":"new-source-version",
            "path":"/services/actual-current","kind":"file","owner_node_id":"source-node","policy":"replicated-eventual",
            "content_hash":"b".repeat(64),"content_size_bytes":11});
        let raw_version = json!({"file_system_id":"source-fs","object_id":"source-object","version":"old-source-version",
            "content_hash":"a".repeat(64),"size_bytes":7,"locations":["source-node"],
            "content_durability":{"source_version":"actual-data-generation","coordinator_node_id":"source-node","owner_epoch":7,
                "desired_replica_node_ids":["source-node"],"policy":{"min_commit_replicas":1,"desired_replicas":1}},
            "created_at":"2026-10-03T00:00:00Z","visible":false});
        let document = json!({"format_version":2,"materialized_through_sequence":0,
            "tables":{"objects":[raw_object.clone()],"versions":[raw_version.clone()]}});
        let bytes = serde_ext::to_bytes(&document, 2, LengthMode::U32).unwrap();
        fs::write(root.join("records/tables/snapshot.sobj"), bytes).unwrap();
        let inventory = source::discover(&root).unwrap();
        let source_identity = inventory.fingerprint.clone();
        let source_snapshot_fingerprint = inventory.snapshot_fingerprint().unwrap().to_owned();
        let mut cursor = source::SourceCursor::new(inventory.clone());
        let mut verified_points = BTreeMap::new();
        let mut object_proof = None;
        let mut version_proof = None;
        loop {
            let point = cursor.point();
            assert!(
                point.inventory.is_none(),
                "one row must not copy the file inventory"
            );
            let Some((verified_point, record)) = source::next_verified(&root, &mut cursor).unwrap()
            else {
                break;
            };
            verified_points.insert(verified_point.key().unwrap(), verified_point);
            if let source::SourceRecord::SnapshotRow { table, row, .. } = &record {
                let proof = MetadataSourceVersionMembership {
                    point,
                    record_digest: digest(&record).unwrap(),
                    row_pointer: "/SnapshotRow/row".into(),
                    row_digest: digest(row).unwrap(),
                    prepared_version_index: None,
                    source_object: None,
                    locator: None,
                    content_hash: None,
                    size_bytes: None,
                };
                match table.as_str() {
                    "objects" => object_proof = Some(proof),
                    "versions" => version_proof = Some(proof),
                    _ => {}
                }
            }
        }
        let object = crate::legacy_wire::normalize_object(raw_object).unwrap();
        let version = crate::legacy_wire::normalize_version(raw_version.clone()).unwrap();
        let (fs_id, _, _, locator) = crate::legacy_wire::source_content_identity(&raw_version)
            .unwrap()
            .unwrap();
        let scope = MetadataSourceContentWorkScope {
            source_root: root.clone(),
            source_identity: source_identity.clone(),
            source_snapshot_fingerprint: source_snapshot_fingerprint.clone(),
            source_journal_sequence: 0,
            file_system_ids: vec![fs_id],
            scope_shard: DfsMetadataShardId::new("root"),
            projection_assignments: Vec::new(),
            original_handoff: None,
        };
        let evidence = MetadataSourceVersionEvidence {
            source_identity,
            source_snapshot_fingerprint,
            source_journal_sequence: 0,
            object,
            version,
            locator,
            object_witness: MetadataSourceObjectWitness::CurrentPolicy,
            membership: version_proof.unwrap(),
            object_membership: object_proof,
        };
        let view = Arc::new(MetadataSourceCurrentMembership {
            _source_lock: retain_source_fence(&root).unwrap(),
            scope: scope.clone(),
            staged_root: stage.clone(),
            inventory,
            verified_points,
            objects: [(
                (
                    evidence.object.file_system_id.clone(),
                    evidence.object.object_id.clone(),
                ),
                evidence.object.clone(),
            )]
            .into_iter()
            .collect(),
            versions: [(
                (
                    evidence.version.file_system_id.clone(),
                    evidence.version.object_id.clone(),
                    evidence.version.version.clone(),
                ),
                (
                    evidence.version.content_hash.clone(),
                    evidence.version.size_bytes,
                    Vec::new(),
                ),
            )]
            .into_iter()
            .collect(),
        });
        register_source_current_membership(&root, &view).unwrap();
        let file = path(
            &stage,
            &evidence.version.file_system_id,
            &evidence.version.object_id,
            &evidence.version.version,
        )
        .unwrap();
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        write_json(&file, &evidence).unwrap();
        (temp, root, stage, scope, evidence, view)
    }
    #[test]
    fn source_rows_bind_raw_locator_and_preserve_newer_current_policy_object() {
        let (_temp, root, stage, scope, evidence, _view) = fixture();
        let checked = lookup_metadata_source_version(
            &root,
            &stage,
            &scope,
            &evidence.version.file_system_id,
            &evidence.version.object_id,
            &evidence.version.version,
        )
        .unwrap()
        .unwrap();
        assert_eq!(checked.locator.object_id, "source-object");
        assert_eq!(checked.locator.version, "old-source-version");
        assert_eq!(
            checked.locator.content_version.as_deref(),
            Some("actual-data-generation")
        );
        assert_ne!(
            checked
                .version
                .content_durability
                .as_ref()
                .unwrap()
                .source_version,
            checked.version.version
        );
        assert_ne!(checked.object.version, checked.version.version);
        assert_eq!(
            checked.object.content_hash.as_deref(),
            Some("b".repeat(64).as_str())
        );
        assert_eq!(
            checked.version.content_hash.as_deref(),
            Some("a".repeat(64).as_str())
        );
    }
    #[test]
    fn owned_decoder_points_preserve_actual_rows_and_reject_foreign_hints() {
        let (_temp, root, _stage, scope, evidence, view) = fixture();
        let key = serde_json::to_vec(&evidence.membership.point.point()).unwrap();
        assert!(view.verified_points.contains_key(&key));
        let checked = actual_row(
            &root,
            &evidence.membership,
            scope.source_journal_sequence,
            &view,
        )
        .unwrap();
        assert_eq!(
            crate::legacy_wire::normalize_version(checked).unwrap(),
            evidence.version
        );
        let mut changed = evidence.membership.clone();
        changed.record_digest = "0".repeat(64);
        assert!(actual_row(&root, &changed, scope.source_journal_sequence, &view).is_err());
        changed = evidence.membership.clone();
        changed.row_digest = "0".repeat(64);
        assert!(actual_row(&root, &changed, scope.source_journal_sequence, &view).is_err());
        changed = evidence.membership.clone();
        let mut point = serde_json::to_value(&changed.point).unwrap();
        point["plain"]["table"] = Value::String("forged-opaque-table".into());
        changed.point = serde_json::from_value(point).unwrap();
        assert!(
            !view
                .verified_points
                .contains_key(&serde_json::to_vec(&changed.point.point()).unwrap())
        );
        assert!(actual_row(&root, &changed, scope.source_journal_sequence, &view).is_err());
    }

    #[test]
    fn owned_decoder_points_recheck_actual_source_bytes_and_file_inventory() {
        let (_temp, root, _stage, scope, evidence, view) = fixture();
        let snapshot = root.join("records/tables/snapshot.sobj");
        let original = fs::read(&snapshot).unwrap();
        let mut changed = original.clone();
        let last = changed.len() - 1;
        changed[last] ^= 1;
        fs::write(&snapshot, &changed).unwrap();
        assert!(
            actual_row(
                &root,
                &evidence.membership,
                scope.source_journal_sequence,
                &view
            )
            .is_err()
        );
        let (_second_temp, root, _stage, scope, evidence, view) = fixture();
        assert!(
            actual_row(
                &root,
                &evidence.membership,
                scope.source_journal_sequence,
                &view
            )
            .is_ok()
        );
        // A new source file cannot be hidden by a valid decoder token, even
        // though the selected row bytes are unchanged.
        let original = fs::read(root.join("records/tables/snapshot.sobj")).unwrap();
        fs::write(root.join("additional-source.sobj"), &original).unwrap();
        assert!(
            actual_row(
                &root,
                &evidence.membership,
                scope.source_journal_sequence,
                &view
            )
            .is_err()
        );
    }

    #[test]
    fn unsigned_stage_cannot_change_source_bytes_locator_or_object_attributes() {
        let (_temp, root, stage, scope, evidence, _view) = fixture();
        let file = path(
            &stage,
            &evidence.version.file_system_id,
            &evidence.version.object_id,
            &evidence.version.version,
        )
        .unwrap();
        for mutation in 0..6 {
            let mut forged = evidence.clone();
            match mutation {
                0 => forged.locator.object_id = forged.version.object_id.to_string(),
                1 => forged.version.content_hash = Some("c".repeat(64)),
                2 => forged.object.version = forged.version.version.clone(),
                3 => forged.membership.row_digest = "0".repeat(64),
                4 => forged.locator.content_version = Some("guessed-data-generation".into()),
                _ => {
                    let mut point = serde_json::to_value(&forged.membership.point).unwrap();
                    point["plain"]["table"] = Value::String("forged-opaque-table".into());
                    forged.membership.point = serde_json::from_value(point).unwrap();
                }
            }
            write_json(&file, &forged).unwrap();
            assert!(
                lookup_metadata_source_version(
                    &root,
                    &stage,
                    &scope,
                    &evidence.version.file_system_id,
                    &evidence.version.object_id,
                    &evidence.version.version
                )
                .is_err()
            );
        }
    }
}

#[cfg(test)]
mod custody_scope_tests {
    use super::*;

    #[test]
    fn sibling_scopes_keep_their_actual_owned_stage_views() {
        let (_temp, root, stage, root_scope, _evidence, root_view) = super::tests::fixture();
        let mut sibling_scope = root_scope.clone();
        sibling_scope.scope_shard = DfsMetadataShardId::new("services");
        let sibling_view = Arc::new(MetadataSourceCurrentMembership {
            _source_lock: Arc::clone(&root_view._source_lock),
            scope: sibling_scope.clone(),
            staged_root: stage.with_file_name("services-stage"),
            inventory: root_view.inventory.clone(),
            verified_points: root_view.verified_points.clone(),
            objects: root_view.objects.clone(),
            versions: root_view.versions.clone(),
        });
        register_source_current_membership(&root, &sibling_view).unwrap();
        assert!(Arc::ptr_eq(
            &source_current_membership(&root, &root_scope).unwrap(),
            &root_view
        ));
        assert!(Arc::ptr_eq(
            &source_current_membership(&root, &sibling_scope).unwrap(),
            &sibling_view
        ));
        sibling_scope.source_journal_sequence += 1;
        assert!(source_current_membership(&root, &sibling_scope).is_err());
    }
}
