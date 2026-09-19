//! Pin validation for the local metadata backend.

use super::*;

pub(super) fn file_version_capture_table_ops(
    tables: &DfsMetadataTables,
    record: &DfsObjectVersionRecord,
    pin: &DfsMetadataVersionPinRecord,
) -> Result<Vec<DfsMetadataTableOp>> {
    crate::file_history::validate_file_version_record(record)?;
    if record.retained.is_none() {
        return Err(MetadataBackendError::Serde(
            "file capture requires explicit provenance".into(),
        ));
    }
    validate_version_pin_payload(
        &record.file_system_id,
        &record.object_id,
        &record.version,
        &pin.pin_id,
        pin,
    )?;
    if !tables.file_systems.contains_key(&record.file_system_id) {
        return Err(MetadataBackendError::UnknownFileSystem {
            file_system_id: record.file_system_id.clone(),
        });
    }
    if tables
        .cross_shard_pin_transfer_fence(&record.object_id, &record.version, &pin.pin_id)
        .is_some()
    {
        return Err(MetadataBackendError::Serde(
            "file capture is fenced by a cross-shard transfer".into(),
        ));
    }
    let existing = tables
        .versions
        .get(&(record.object_id.clone(), record.version.clone()));
    if existing.is_some_and(|existing| {
        existing.content_hash != record.content_hash
            || existing.size_bytes != record.size_bytes
            || existing.file_system_id != record.file_system_id
            || (existing.retained.is_some() && existing.retained != record.retained)
    }) {
        return Err(MetadataBackendError::Serde(
            "file capture conflicts with an existing immutable version".into(),
        ));
    }
    let existing_pin = tables.version_pins.get(&(
        pin.object_id.clone(),
        pin.version.clone(),
        pin.pin_id.clone(),
    ));
    if existing_pin.is_some_and(|existing| existing != pin) {
        return Err(MetadataBackendError::Serde(
            "file capture pin identity was reused with a different payload".into(),
        ));
    }
    let mut ops = Vec::with_capacity(2);
    if existing != Some(record) {
        ops.push(DfsMetadataTableOp::PutVersion {
            version: record.clone(),
        });
    }
    if existing_pin.is_none() {
        require_version_pin_capacity_for_new(tables, &record.object_id)?;
        ops.push(DfsMetadataTableOp::PutVersionPin { pin: pin.clone() });
    }
    Ok(ops)
}

pub(super) fn validate_version_pin_payload(
    file_system_id: &DfsFileSystemId,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    pin_id: &str,
    pin: &DfsMetadataVersionPinRecord,
) -> Result<()> {
    if pin_id.trim().is_empty() || pin_id.len() > 256 {
        return Err(MetadataBackendError::Serde(
            "metadata version pin id must contain 1..=256 bytes".to_string(),
        ));
    }
    if &pin.file_system_id != file_system_id
        || &pin.object_id != object_id
        || &pin.version != version
        || pin.pin_id != pin_id
    {
        return Err(MetadataBackendError::Serde(format!(
            "metadata version pin payload does not match outer identity {file_system_id}/{object_id}/{version}/{pin_id}"
        )));
    }
    Ok(())
}

pub(super) fn require_version_pin_capacity_for_new(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
) -> Result<()> {
    let count = tables
        .version_pins
        .keys()
        .filter(|(pin_object_id, _, _)| pin_object_id == object_id)
        .count();
    if count >= MAX_DURABLE_VERSION_PINS_PER_OBJECT {
        return Err(MetadataBackendError::Serde(format!(
            "object {object_id} already has the maximum {} durable version pins",
            MAX_DURABLE_VERSION_PINS_PER_OBJECT
        )));
    }
    Ok(())
}

pub(super) fn cross_shard_pin_transfer_identity_matches(
    left: &DfsCrossShardPinTransferRecord,
    right: &DfsCrossShardPinTransferRecord,
) -> bool {
    left.transfer_id == right.transfer_id
        && left.file_system_id == right.file_system_id
        && left.object_id == right.object_id
        && normalize_path(&left.source_path) == normalize_path(&right.source_path)
        && normalize_path(&left.target_path) == normalize_path(&right.target_path)
        && left.target_version == right.target_version
        && left.created_at == right.created_at
}

pub(super) fn validate_cross_shard_pin_transfer_table_transition(
    tables: &DfsMetadataTables,
    outer_file_system_id: &DfsFileSystemId,
    outer_object_id: &DfsObjectId,
    outer_transfer_id: &str,
    authoritative_migration: bool,
    incoming_versions: &[DfsObjectVersionRecord],
    transfer: &DfsCrossShardPinTransferRecord,
) -> Result<bool> {
    if outer_transfer_id.trim().is_empty() || outer_transfer_id.len() > 256 {
        return Err(MetadataBackendError::Serde(
            "cross-shard pin transfer id must contain 1..=256 bytes".to_string(),
        ));
    }
    if &transfer.file_system_id != outer_file_system_id
        || &transfer.object_id != outer_object_id
        || transfer.transfer_id != outer_transfer_id
        || transfer.source_path != normalize_path(&transfer.source_path)
        || transfer.target_path != normalize_path(&transfer.target_path)
        || transfer.source_path == transfer.target_path
        || transfer.updated_at < transfer.created_at
    {
        return Err(MetadataBackendError::Serde(format!(
            "cross-shard pin transfer payload does not match canonical outer identity {outer_file_system_id}/{outer_object_id}/{outer_transfer_id}"
        )));
    }
    if transfer.source_pins.len() > MAX_DURABLE_VERSION_PINS_PER_OBJECT
        || transfer.copied_pins.len() > transfer.source_pins.len()
    {
        return Err(MetadataBackendError::Serde(format!(
            "cross-shard pin transfer {} exceeds the maximum {} acknowledged pins",
            transfer.transfer_id, MAX_DURABLE_VERSION_PINS_PER_OBJECT
        )));
    }
    let mut incoming_version_ids = BTreeSet::new();
    for version in incoming_versions {
        if version.file_system_id != transfer.file_system_id
            || version.object_id != transfer.object_id
            || !incoming_version_ids.insert(version.version.clone())
        {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} received an unrelated or duplicate version {}/{}",
                transfer.transfer_id, version.object_id, version.version
            )));
        }
        if let Some(existing) = tables
            .versions
            .get(&(version.object_id.clone(), version.version.clone()))
            && existing != version
        {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} conflicts with destination version {}/{}",
                transfer.transfer_id, version.object_id, version.version
            )));
        }
    }
    if !tables
        .versions
        .contains_key(&(transfer.object_id.clone(), transfer.target_version.clone()))
        && !incoming_versions
            .iter()
            .any(|version| version.version == transfer.target_version)
    {
        return Err(MetadataBackendError::Serde(format!(
            "cross-shard pin transfer {} is missing target version {}/{}",
            transfer.transfer_id, transfer.object_id, transfer.target_version
        )));
    }
    let key = (transfer.object_id.clone(), transfer.transfer_id.clone());
    let previous = tables.cross_shard_pin_transfers.get(&key);
    if let Some(previous) = previous
        && !cross_shard_pin_transfer_identity_matches(previous, transfer)
    {
        return Err(MetadataBackendError::Serde(format!(
            "cross-shard pin transfer {} immutable identity changed",
            transfer.transfer_id
        )));
    }
    if tables.cross_shard_pin_transfers.values().any(|other| {
        other.object_id == transfer.object_id && other.transfer_id != transfer.transfer_id
    }) {
        return Err(MetadataBackendError::Serde(format!(
            "object {} already has another active cross-shard pin transfer",
            transfer.object_id
        )));
    }

    let previous_owned = previous
        .map(|previous| previous.copied_pins.iter().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    let copied_pins = transfer.copied_pins.iter().collect::<BTreeSet<_>>();
    if copied_pins.len() != transfer.copied_pins.len() {
        return Err(MetadataBackendError::Serde(format!(
            "cross-shard pin transfer {} repeats an owned pin",
            transfer.transfer_id
        )));
    }
    let mut source_keys = BTreeSet::new();
    for pin in &transfer.source_pins {
        validate_version_pin_payload(
            &pin.file_system_id,
            &pin.object_id,
            &pin.version,
            &pin.pin_id,
            pin,
        )?;
        if pin.file_system_id != transfer.file_system_id || pin.object_id != transfer.object_id {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} acknowledges unrelated pin {} for {}/{}",
                transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
            )));
        }
        let pin_key = (
            pin.object_id.clone(),
            pin.version.clone(),
            pin.pin_id.clone(),
        );
        if !source_keys.insert(pin_key.clone()) {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} repeats acknowledged pin {} for {}/{}",
                transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
            )));
        }
        let version = tables
            .versions
            .get(&(pin.object_id.clone(), pin.version.clone()))
            .or_else(|| {
                incoming_versions
                    .iter()
                    .find(|version| version.version == pin.version)
            })
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} references missing version {}/{}",
                    transfer.transfer_id, pin.object_id, pin.version
                ))
            })?;
        if version.file_system_id != transfer.file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} pin version filesystem mismatch",
                transfer.transfer_id
            )));
        }
        if let Some(other) =
            tables.cross_shard_pin_transfer_fence(&pin.object_id, &pin.version, &pin.pin_id)
            && other.transfer_id != transfer.transfer_id
        {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin {} for {}/{} is already fenced by transfer {}",
                pin.pin_id, pin.object_id, pin.version, other.transfer_id
            )));
        }
        let desired_owned = copied_pins.contains(pin);
        let previously_owned = previous_owned.contains(pin);
        match tables.version_pins.get(&pin_key) {
            Some(existing) if existing == pin && desired_owned == previously_owned => {}
            Some(existing) if existing == pin && !desired_owned => {}
            Some(existing) if existing == pin => {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} cannot claim pre-existing target pin {} for {}/{}",
                    transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            Some(_) if previously_owned => {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} lost ownership of pin {} for {}/{} to a conflicting record",
                    transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            Some(_) => {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} cannot claim a pre-existing target pin {} for {}/{}",
                    transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            None if previously_owned => {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} is missing its owned pin {} for {}/{}",
                    transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            None if desired_owned => {}
            None if authoritative_migration => {}
            None => {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} is missing acknowledged independent pin {} for {}/{}",
                    transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
        }
    }
    for pin in &transfer.copied_pins {
        if !transfer.source_pins.iter().any(|source| source == pin) {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} owns pin {} for {}/{} outside its acknowledged source set",
                transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
            )));
        }
    }

    let current_pin_count = tables
        .version_pins
        .keys()
        .filter(|(object_id, _, _)| object_id == outer_object_id)
        .count();
    let stale_owned_count = previous
        .map(|previous| {
            previous
                .copied_pins
                .iter()
                .filter(|pin| !transfer.copied_pins.iter().any(|desired| desired == *pin))
                .count()
        })
        .unwrap_or(0);
    let newly_inserted_count = transfer
        .source_pins
        .iter()
        .filter(|pin| {
            !tables.version_pins.contains_key(&(
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            ))
        })
        .count();
    if current_pin_count
        .saturating_sub(stale_owned_count)
        .saturating_add(newly_inserted_count)
        > MAX_DURABLE_VERSION_PINS_PER_OBJECT
    {
        return Err(MetadataBackendError::Serde(format!(
            "cross-shard pin transfer {} would exceed the maximum {} durable pins for object {}",
            transfer.transfer_id, MAX_DURABLE_VERSION_PINS_PER_OBJECT, transfer.object_id
        )));
    }
    Ok(previous == Some(transfer))
}

pub(super) fn validate_cross_shard_source_eviction_table_transition(
    tables: &DfsMetadataTables,
    outer_file_system_id: &DfsFileSystemId,
    outer_object_id: &DfsObjectId,
    tombstone: &DfsObjectMetadata,
    removed_pins: &[DfsMetadataVersionPinRecord],
    removed_pin_transfers: &[DfsCrossShardPinTransferRecord],
) -> Result<bool> {
    if &tombstone.file_system_id != outer_file_system_id
        || &tombstone.object_id != outer_object_id
        || tombstone.kind != DfsObjectKind::Tombstone
        || !matches!(
            tombstone.tombstone_provenance,
            Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
        )
        || removed_pins.len() > MAX_DURABLE_VERSION_PINS_PER_OBJECT
    {
        return Err(MetadataBackendError::Serde(format!(
            "invalid atomic cross-shard source eviction for {outer_file_system_id}/{outer_object_id}"
        )));
    }
    let mut removed_pin_keys = BTreeSet::new();
    for pin in removed_pins {
        if pin.file_system_id != *outer_file_system_id
            || pin.object_id != *outer_object_id
            || !removed_pin_keys.insert((
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            ))
        {
            return Err(MetadataBackendError::Serde(format!(
                "atomic cross-shard source eviction for {outer_object_id} contains an unrelated or duplicate pin"
            )));
        }
    }
    let mut removed_transfer_keys = BTreeSet::new();
    for transfer in removed_pin_transfers {
        if transfer.file_system_id != *outer_file_system_id
            || transfer.object_id != *outer_object_id
            || !removed_transfer_keys
                .insert((transfer.object_id.clone(), transfer.transfer_id.clone()))
        {
            return Err(MetadataBackendError::Serde(format!(
                "atomic cross-shard source eviction for {outer_object_id} contains an unrelated or duplicate pin transfer"
            )));
        }
    }

    let current = tables
        .objects
        .get(outer_object_id)
        .or_else(|| tables.compacted_tombstones.get(outer_object_id));
    let already_applied = current == Some(tombstone)
        && removed_pins.iter().all(|pin| {
            tables.version_pins.get(&(
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            )) != Some(pin)
        })
        && removed_pin_transfers.iter().all(|transfer| {
            tables
                .cross_shard_pin_transfers
                .get(&(transfer.object_id.clone(), transfer.transfer_id.clone()))
                != Some(transfer)
        });
    if already_applied {
        return Ok(true);
    }

    let current_pins = tables
        .version_pins
        .values()
        .filter(|pin| &pin.object_id == outer_object_id)
        .collect::<BTreeSet<_>>();
    let removed_pins_set = removed_pins.iter().collect::<BTreeSet<_>>();
    if current_pins != removed_pins_set {
        return Err(MetadataBackendError::Serde(format!(
            "atomic cross-shard source eviction for {outer_object_id} does not remove the exact current pin set"
        )));
    }
    let current_transfers = tables
        .cross_shard_pin_transfers
        .values()
        .filter(|transfer| &transfer.object_id == outer_object_id)
        .map(|transfer| {
            (
                (transfer.object_id.clone(), transfer.transfer_id.clone()),
                transfer,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let removed_transfers = removed_pin_transfers
        .iter()
        .map(|transfer| {
            (
                (transfer.object_id.clone(), transfer.transfer_id.clone()),
                transfer,
            )
        })
        .collect::<BTreeMap<_, _>>();
    if current_transfers != removed_transfers {
        return Err(MetadataBackendError::Serde(format!(
            "atomic cross-shard source eviction for {outer_object_id} does not remove the exact current pin-transfer set"
        )));
    }
    Ok(false)
}
