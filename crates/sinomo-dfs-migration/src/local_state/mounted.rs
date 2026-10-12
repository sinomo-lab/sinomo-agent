//! Exact released mounted-state field conversion. It changes representation,
//! never a phase, holder, outcome, clock, nonce, or permission.
use super::{Result, error};
use sinomo_core::object::{
    Array, Document, DocumentView, ElementView, Field, List, Object, OptionalValue, Payload,
    PayloadView, Value,
};

#[derive(Clone, Copy)]
pub(super) enum Role {
    Latch,
    Session,
    Operation,
    CleanupAuthorization,
}
const WORK_FIELDS: &[&str] = &[
    "publish_metadata",
    "repair_maintenance",
    "namespace_maintenance",
    "semantic_changes",
    "metadata_replica_progress",
    "metadata_materialization",
];
fn object(value: &mut Value) -> Result<&mut Object> {
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(error("mounted source field is not an object")),
    }
}
fn field<'a>(object: &'a mut Object, name: &str) -> Option<&'a mut Value> {
    object
        .fields
        .iter_mut()
        .find(|field| field.key == name)
        .map(|field| &mut field.value)
}
fn text(value: &Value) -> Option<&str> {
    if let Value::String(value) = value {
        Some(value)
    } else {
        None
    }
}
fn version(object: &Object) -> Result<()> {
    let value = object
        .fields
        .iter()
        .find(|field| field.key == "schema_version")
        .ok_or_else(|| error("mounted source omits its actual schema version"))?;
    let actual = match value.value {
        Value::Uint8(v) => v as u64,
        Value::Uint16(v) => v as u64,
        Value::Uint32(v) => v as u64,
        Value::Uint64(v) => v,
        _ => {
            return Err(error(
                "mounted source schema version is not an unsigned integer",
            ));
        }
    };
    if actual != 1 {
        return Err(error("unsupported original mounted-state schema"));
    }
    Ok(())
}
fn adapter(value: &mut Value) -> Result<()> {
    match text(value) {
        Some("cloud-files") => *value = Value::String("external-provider".into()),
        Some("external-provider" | "fuse" | "nfs") => {}
        _ => return Err(error("unknown original mounted adapter")),
    }
    Ok(())
}
fn work(value: &mut Value) -> Result<()> {
    let row = object(value)?;
    if row.fields.iter().any(|field| {
        !WORK_FIELDS.contains(&field.key.as_str()) && field.key != "attachment_cleanup"
    }) {
        return Err(error(
            "unknown original committed-work bit cannot be discarded",
        ));
    }
    if row
        .fields
        .iter()
        .any(|field| field.key == "attachment_cleanup")
        && row
            .fields
            .iter()
            .any(|field| field.key == "semantic_changes")
    {
        return Err(error(
            "original committed work repeats its semantic bit under aliases",
        ));
    }
    for item in &mut row.fields {
        if !matches!(item.value, Value::Boolean(_)) {
            return Err(error("original committed-work bit is not boolean"));
        }
        if item.key == "attachment_cleanup" {
            item.key = "semantic_changes".into();
        }
    }
    // Pinned released producer serialized ALL six bits unconditionally.
    // A permissive old reader default is not evidence of a produced omission.
    if row.fields.len() != WORK_FIELDS.len()
        || WORK_FIELDS
            .iter()
            .any(|name| !row.fields.iter().any(|field| field.key == *name))
    {
        return Err(error(
            "original committed work omits a released producer obligation",
        ));
    }
    Ok(())
}
fn captured(value: &mut Value) -> Result<()> {
    let row = object(value)?;
    if row.fields.len() != 2 {
        return Err(error("original capture wrapper changes released fields"));
    }
    work(
        field(row, "committed_work")
            .ok_or_else(|| error("original capture omits committed work"))?,
    )?;
    if !matches!(field(row, "committed_repair_records"), Some(Value::List(_))) {
        return Err(error("original capture omits its typed retry inventory"));
    }
    // Keep original canonical retry rows unchanged. The independent existing
    // retry source converter owns any separately proven old identity generation.

    Ok(())
}
fn marker(value: &mut Value) -> Result<()> {
    let row = object(value)?;
    version(row)?;
    let origin =
        object(field(row, "origin").ok_or_else(|| error("original marker lacks origin"))?)?;
    adapter(field(origin, "adapter").ok_or_else(|| error("original marker lacks adapter"))?)?;
    let phase = object(field(row, "phase").ok_or_else(|| error("original marker lacks phase"))?)?;
    let kind = text(field(phase, "phase").ok_or_else(|| error("original marker lacks phase tag"))?)
        .ok_or_else(|| error("original marker phase tag is not a string"))?
        .to_owned();
    match kind.as_str() {
        "captured" | "daemon-accepted" => {
            captured(
                field(phase, "captured_work")
                    .ok_or_else(|| error("original phase lacks captured work"))?,
            )?;
            if let Some(Value::Optional(optional)) = field(phase, "error_observation") {
                if let Some(observation) = optional.value.as_mut() {
                    let observation = object(observation)?;
                    captured(
                        field(observation, "captured_work")
                            .ok_or_else(|| error("original error lacks captured work"))?,
                    )?;
                }
            }
        }
        "unknown-after-error" => {
            let observation = object(
                field(phase, "observation")
                    .ok_or_else(|| error("original unknown phase lacks observation"))?,
            )?;
            captured(
                field(observation, "captured_work")
                    .ok_or_else(|| error("original error lacks captured work"))?,
            )?;
        }
        "prepared" | "armed" | "proven-no-write" | "aborted-unstarted" | "terminal" => {}
        _ => return Err(error("unknown original mounted phase")),
    }
    Ok(())
}
/// SAME R0 parser/builder preserves binary/list/array/optional leaf kinds and
/// unrelated original field order. This is not a JSON or full-table decoder.
pub(super) fn normalize(bytes: &[u8], role: Role) -> Result<Vec<u8>> {
    if bytes.len() > sinomo_dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES {
        return Err(error(
            "mounted original state exceeds its actual record bound",
        ));
    }
    let view = DocumentView::parse(bytes).map_err(error)?;
    if view.metadata.version_id != 1 {
        return Err(error("unsupported original mounted SOBJ generation"));
    }
    let PayloadView::Dynamic(value) = &view.payload else {
        return Err(error("original mounted state is not a dynamic record"));
    };
    let mut value = own(value);
    match role {
        Role::Latch => {
            let row = object(&mut value)?;
            version(row)?;
            if row.fields.len() != 4
                || !matches!(field(row, "generation"), Some(Value::Uint64(_)))
                || !matches!(field(row, "updated_at_unix_nanos"), Some(Value::Uint64(_)))
            {
                return Err(error(
                    "original latch omits/changes released counters or clocks",
                ));
            }
            work(
                field(row, "committed_work")
                    .ok_or_else(|| error("original latch omits committed work"))?,
            )?;
        }
        Role::Session => {
            let row = object(&mut value)?;
            version(row)?;
            adapter(field(row, "adapter").ok_or_else(|| error("original session lacks adapter"))?)?;
        }
        Role::Operation => marker(&mut value)?,
        Role::CleanupAuthorization => {
            let row = object(&mut value)?;
            let reason =
                text(field(row, "reason").ok_or_else(|| error("original cleanup lacks reason"))?)
                    .ok_or_else(|| error("original cleanup reason is not a string"))?
                    .to_owned();
            match reason.as_str() {
                "terminal" => marker(
                    field(row, "marker")
                        .ok_or_else(|| error("original terminal cleanup lacks marker"))?,
                )?,
                "corrupt-full-recovery" | "unpublished-marker" => {}
                _ => return Err(error("unknown original cleanup authorization")),
            }
        }
    }
    Document {
        metadata: view.metadata,
        schema: view.schema,
        payload: Payload::Dynamic(value),
    }
    .to_bytes()
    .map_err(error)
}
fn own(value: &ElementView<'_>) -> Value {
    match value {
        ElementView::Boolean(v) => Value::Boolean(*v),
        ElementView::Uint8(v) => Value::Uint8(*v),
        ElementView::Int8(v) => Value::Int8(*v),
        ElementView::Uint16(v) => Value::Uint16(*v),
        ElementView::Int16(v) => Value::Int16(*v),
        ElementView::Uint32(v) => Value::Uint32(*v),
        ElementView::Int32(v) => Value::Int32(*v),
        ElementView::Uint64(v) => Value::Uint64(*v),
        ElementView::Int64(v) => Value::Int64(*v),
        ElementView::Float(v) => Value::Float(*v),
        ElementView::Double(v) => Value::Double(*v),
        ElementView::String(v) => Value::String((*v).into()),
        ElementView::Bytes(v) => Value::Bytes(v.to_vec()),
        ElementView::Object(v) => Value::Object(Object::new(
            v.fields
                .iter()
                .map(|f| Field::new(f.key, own(&f.value)))
                .collect(),
        )),
        ElementView::List(v) => Value::list(List::new(
            v.element_type.clone(),
            v.values.iter().map(own).collect(),
        )),
        ElementView::Array(v) => Value::array(Array {
            element_type: v.element_type.clone(),
            element_size: v.element_size,
            dimensions: v.dimensions.clone(),
            data: v.data.to_vec(),
        }),
        ElementView::Optional(v) => Value::optional(OptionalValue {
            element_type: v.element_type.clone(),
            value: v.value.as_ref().map(|v| Box::new(own(v))),
        }),
    }
}

/// The caller already owns the native Main/Embedded data-root capability.
/// Existing store/state/lease files serialize this representation change with
/// the same original owner. A live old mount parks conversion, never times out
/// into fabricated death or recovery permission.
pub(super) fn convert(
    dfs_root: &std::path::Path,
    path: &std::path::Path,
    role: Role,
) -> Result<bool> {
    use fs2::FileExt;
    use sinomo_core::storage::durable_file::{DurableAtomicFileOptions, write_file_atomic};
    use std::fs::{self, OpenOptions};
    let outbox = dfs_root.join("mounted-mutation-outbox");
    let plain = |path: &std::path::Path| -> Result<()> {
        let metadata = fs::symlink_metadata(path).map_err(error)?;
        if metadata.is_symlink() || !metadata.is_file() {
            return Err(error("mounted source record/lock is not a plain file"));
        }
        Ok(())
    };
    plain(path)?;
    let lock_path = outbox.join("outbox.lock");
    plain(&lock_path)?;
    let store_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(error)?;
    match store_lock.try_lock_exclusive() {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
        Err(source) => return Err(error(source)),
    }
    let original = super::bounded_read(path)?;
    archive_original(dfs_root, path, &original)?;
    let normalized = normalize(&original, role)?;
    if normalized == original {
        return Ok(true);
    }
    let directory = path
        .parent()
        .ok_or_else(|| error("mounted source record has no owning directory"))?;
    let _lease = if matches!(role, Role::Session | Role::Operation) {
        let lease = directory.join("lease.lock");
        // Every genuine released live session/operation producer creates this
        // lease before publishing state. Absence is not proof of owner death.
        if !lease.exists() {
            return Ok(false);
        }
        plain(&lease)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lease)
            .map_err(error)?;
        match file.try_lock_exclusive() {
            Ok(()) => Some(file),
            Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(source) => return Err(error(source)),
        }
    } else {
        None
    };
    let state_lock_path = directory.join("state.lock");
    if state_lock_path.exists() {
        plain(&state_lock_path)?;
    }
    let state_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&state_lock_path)
        .map_err(error)?;
    match state_lock.try_lock_exclusive() {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
        Err(source) => return Err(error(source)),
    }
    if super::bounded_read(path)? != original {
        return Ok(false);
    }
    match role {
        Role::Latch => {
            let _: sinomo_dfs::MountedSifsMutationCommittedWorkLatch =
                sinomo_core::object::serde_ext::from_bytes(&normalized).map_err(error)?;
        }
        Role::Session => {
            let _: sinomo_dfs::MountedSifsHostMountSessionMarker =
                sinomo_core::object::serde_ext::from_bytes(&normalized).map_err(error)?;
        }
        Role::Operation => {
            let _: sinomo_dfs::MountedSifsMutationOutboxMarker =
                sinomo_core::object::serde_ext::from_bytes(&normalized).map_err(error)?;
        }
        Role::CleanupAuthorization => {}
    }
    // An exact original byte compare under actual original locks precedes the
    // same durable atomic writer. Phase and liveness evidence remain unchanged.
    if super::bounded_read(path)? != original {
        return Err(error("mounted source changed during conversion"));
    }
    write_file_atomic(
        path,
        &normalized,
        DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
    )
    .map_err(error)?;
    Ok(true)
}

fn archive_original(
    dfs_root: &std::path::Path,
    path: &std::path::Path,
    original: &[u8],
) -> Result<()> {
    use sinomo_core::storage::durable_file::{DurableAtomicFileOptions, write_file_atomic};
    let retained = dfs_root
        .join("migration-retained/local-state")
        .join(blake3::hash(original).to_hex().as_str())
        .join(path.strip_prefix(dfs_root).map_err(error)?);
    std::fs::create_dir_all(
        retained
            .parent()
            .ok_or_else(|| error("mounted original archive has no parent"))?,
    )
    .map_err(error)?;
    if retained.exists() {
        if super::bounded_read(&retained)? != original {
            return Err(error("mounted original archive changed"));
        }
    } else {
        write_file_atomic(
            &retained,
            original,
            DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
        )
        .map_err(error)?;
    }
    super::sync_archive_parents(dfs_root, &retained)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use sinomo_core::object::{LengthMode, serde_ext};
    #[derive(Serialize)]
    struct OriginalWork {
        publish_metadata: bool,
        repair_maintenance: bool,
        namespace_maintenance: bool,
        attachment_cleanup: bool,
        metadata_replica_progress: bool,
        metadata_materialization: bool,
    }
    #[derive(Serialize)]
    struct OriginalLatch {
        schema_version: u16,
        generation: u64,
        committed_work: OriginalWork,
        updated_at_unix_nanos: u64,
    }
    #[test]
    fn genuine_original_six_field_work_converts_after_exact_archive() {
        let original = serde_ext::to_bytes(
            &OriginalLatch {
                schema_version: 1,
                generation: 4,
                committed_work: OriginalWork {
                    publish_metadata: true,
                    repair_maintenance: false,
                    namespace_maintenance: false,
                    attachment_cleanup: true,
                    metadata_replica_progress: false,
                    metadata_materialization: false,
                },
                updated_at_unix_nanos: 99,
            },
            1,
            LengthMode::U32,
        )
        .unwrap();
        let dir = sinomo_testdir::TestDir::new("mounted-original-work-source");
        let outbox = dir.join("mounted-mutation-outbox");
        std::fs::create_dir_all(&outbox).unwrap();
        std::fs::write(outbox.join("outbox.lock"), b"").unwrap();
        let path = outbox.join("daemon-committed-work-latch.sobj");
        std::fs::write(&path, &original).unwrap();
        assert!(
            serde_ext::from_bytes::<sinomo_dfs::MountedSifsMutationCommittedWorkLatch>(&original)
                .is_err(),
            "normal current decoder rejects retired key"
        );
        assert!(convert(dir.path(), &path, Role::Latch).unwrap());
        let current: sinomo_dfs::MountedSifsMutationCommittedWorkLatch =
            serde_ext::from_bytes(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(current.generation, 4);
        assert_eq!(current.updated_at_unix_nanos, 99);
        assert!(current.committed_work.publish_metadata && current.committed_work.semantic_changes);
        assert!(
            !current.committed_work.repair_maintenance
                && !current.committed_work.namespace_maintenance
                && !current.committed_work.metadata_replica_progress
                && !current.committed_work.metadata_materialization
        );
        let retained = dir
            .join("migration-retained/local-state")
            .join(blake3::hash(&original).to_hex().as_str())
            .join("mounted-mutation-outbox/daemon-committed-work-latch.sobj");
        assert_eq!(std::fs::read(retained).unwrap(), original);
        assert!(convert(dir.path(), &path, Role::Latch).unwrap());
    }
    #[test]
    fn original_adapter_and_unrelated_raw_leaf_types_are_distinct() {
        let session = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("schema_version", Value::Uint16(1)),
                Field::new("adapter", Value::String("cloud-files".into())),
                Field::new(
                    "mount_identity",
                    Value::String("cloud-files/attachment_cleanup".into()),
                ),
                Field::new("opaque_bytes", Value::Bytes(b"null".to_vec())),
                Field::new(
                    "opaque_list",
                    Value::list(List::new(
                        sinomo_core::object::ElementType::Builtin(
                            sinomo_core::object::TypeId::Uint8,
                        ),
                        vec![
                            Value::Uint8(110),
                            Value::Uint8(117),
                            Value::Uint8(108),
                            Value::Uint8(108),
                        ],
                    )),
                ),
            ])),
        )
        .to_bytes()
        .unwrap();
        let converted = normalize(&session, Role::Session).unwrap();
        let view = DocumentView::parse(&converted).unwrap();
        let PayloadView::Dynamic(ElementView::Object(row)) = view.payload else {
            panic!("source object")
        };
        assert_eq!(
            row.get("adapter"),
            Some(&ElementView::String("external-provider"))
        );
        assert_eq!(
            row.get("mount_identity"),
            Some(&ElementView::String("cloud-files/attachment_cleanup"))
        );
        assert!(matches!(row.get("opaque_bytes"),Some(ElementView::Bytes(v)) if *v==b"null"));
        assert!(matches!(row.get("opaque_list"), Some(ElementView::List(_))));
        assert!(
            serde_json::from_value::<sinomo_dfs::MountedSifsMutationAdapter>(serde_json::json!(
                "cloud-files"
            ))
            .is_err()
        );
    }
    #[test]
    fn never_produced_partial_source_is_archived_and_rejected_without_rewrite() {
        let original = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![Field::new(
                "committed_work",
                Value::Object(Object::new(vec![Field::new(
                    "attachment_cleanup",
                    Value::Boolean(true),
                )])),
            )])),
        )
        .to_bytes()
        .unwrap();
        let dir = sinomo_testdir::TestDir::new("mounted-malformed-partial-source");
        let outbox = dir.join("mounted-mutation-outbox");
        std::fs::create_dir_all(&outbox).unwrap();
        std::fs::write(outbox.join("outbox.lock"), b"").unwrap();
        let path = outbox.join("daemon-committed-work-latch.sobj");
        std::fs::write(&path, &original).unwrap();
        assert!(convert(dir.path(), &path, Role::Latch).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let retained = dir
            .join("migration-retained/local-state")
            .join(blake3::hash(&original).to_hex().as_str())
            .join("mounted-mutation-outbox/daemon-committed-work-latch.sobj");
        assert_eq!(std::fs::read(retained).unwrap(), original);
    }
    #[test]
    fn duplicate_unknown_or_nonboolean_original_work_never_silently_drops() {
        for fields in [
            vec![
                Field::new("attachment_cleanup", Value::Boolean(true)),
                Field::new("semantic_changes", Value::Boolean(true)),
            ],
            vec![Field::new("unknown_flag", Value::Boolean(true))],
            vec![Field::new("publish_metadata", Value::Uint8(1))],
        ] {
            let bytes = Document::dynamic(
                1,
                LengthMode::U32,
                Value::Object(Object::new(vec![Field::new(
                    "committed_work",
                    Value::Object(Object::new(fields)),
                )])),
            )
            .to_bytes()
            .unwrap();
            assert!(normalize(&bytes, Role::Latch).is_err());
        }
    }
}
