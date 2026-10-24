//! Prospective original Repair phases in the existing content log. These are
//! private-fixture evidence, never a production floor or current-copy authority.
use super::*;
use crate::{DfsOriginalRepairMaterial, DfsRepairReplicaResponse};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalRepairKey {
    producer: String,
    request_id: String,
}
impl OriginalRepairKey {
    fn of(material: &DfsOriginalRepairMaterial) -> Self {
        Self {
            producer: material.original.source_node_id.to_string(),
            request_id: material.original.command.request_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalRepairPrepared {
    pub(super) key: OriginalRepairKey,
    pub(super) material: DfsOriginalRepairMaterial,
    pub(super) receiver: String,
    pub(super) store_incarnation: String,
    pub(super) logical_id: u64,
    pub(super) seal: String,
}
impl OriginalRepairPrepared {
    fn seal(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Seal<'a> {
            domain: &'static str,
            key: &'a OriginalRepairKey,
            material: &'a DfsOriginalRepairMaterial,
            receiver: &'a str,
            store_incarnation: &'a str,
            logical_id: u64,
        }
        Ok(blake3::hash(&encode_inline(&Seal {
            domain: "sinomo.dfs.original-repair.prepared.v1",
            key: &self.key,
            material: &self.material,
            receiver: &self.receiver,
            store_incarnation: &self.store_incarnation,
            logical_id: self.logical_id,
        })?)
        .to_hex()
        .to_string())
    }
    fn validate(&self, incarnation: &str) -> Result<()> {
        self.material.validate().map_err(inline_error)?;
        if self.key != OriginalRepairKey::of(&self.material)
            || self.logical_id == 0
            || self.receiver != self.material.original.target_node_id.as_str()
            || self.store_incarnation != incarnation
            || self.seal != self.seal()?
            || self.material.attachment.total_size_bytes > INLINE_BLOB_SMALL_LIMIT_BYTES
        {
            return Err(inline_error(
                "original Prepared identity/material is not exact",
            ));
        }
        hash_hex(&self.material.original.command.content_hash)?;
        validate_key(
            self.material.original.command.object_id.as_str(),
            self.material.original.command.version.as_str(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalRepairBinding {
    pub(super) key: OriginalRepairKey,
    pub(super) prepared_id: u64,
    pub(super) prepared_seal: String,
    pub(super) logical_id: u64,
    pub(super) store_incarnation: String,
    pub(super) manifest: ContentManifest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalRepairTerminal {
    pub(super) key: OriginalRepairKey,
    pub(super) prepared_id: u64,
    pub(super) prepared_seal: String,
    pub(super) bound_id: u64,
    pub(super) logical_id: u64,
    pub(super) store_incarnation: String,
    pub(super) response: DfsRepairReplicaResponse,
    pub(super) provenance: crate::publication_guard::NativeContentOnlyRepairProvenance,
}

/// Minted by the synchronized physical owner only, never deserialized.
#[derive(Clone, Debug)]
pub(crate) struct PreparedOriginalRepairRef {
    root: PathBuf,
    prepared: OriginalRepairPrepared,
}
impl PreparedOriginalRepairRef {
    pub(super) fn exact_version(&self) -> (&str, &str) {
        (
            self.prepared.material.original.command.object_id.as_str(),
            self.prepared.material.original.command.version.as_str(),
        )
    }
}
#[derive(Debug)]
pub(crate) struct BoundOriginalRepairContent {
    prepared: PreparedOriginalRepairRef,
    binding: OriginalRepairBinding,
}
impl BoundOriginalRepairContent {
    pub(crate) fn prepared_seal(&self) -> &str {
        &self.prepared.prepared.seal
    }
    pub(crate) fn bound_id(&self) -> u64 {
        self.binding.logical_id
    }
    pub(crate) fn manifest(&self) -> &ContentManifest {
        &self.binding.manifest
    }
}

impl LocalContentStore {
    pub(crate) fn requires_original_repair_fixture(&self) -> Result<()> {
        if self.inline_store.owner.expected_format != ORIGINAL_REPAIR_FIXTURE_VERSION {
            return Err(inline_error(
                "selected original Repair requires explicit native fixture content admission",
            ));
        }
        self.require_received_repair_fixture_identity()
    }

    pub(crate) fn prepare_original_repair(
        &self,
        material: &DfsOriginalRepairMaterial,
        bytes: &[u8],
        admission: &crate::publication_guard::NativeContentOnlyRepairAdmission,
    ) -> Result<PreparedOriginalRepairRef> {
        admission.revalidate(material).map_err(inline_error)?;
        admission.check_content_store(self)?;
        self.requires_original_repair_fixture()?;
        material.validate().map_err(inline_error)?;
        if bytes.len() as u64 != material.attachment.total_size_bytes
            || bytes.len() as u64 > INLINE_BLOB_SMALL_LIMIT_BYTES
        {
            return Err(inline_error(
                "selected original Repair body is unsupported or changed",
            ));
        }
        ensure_bytes_match_hash(&material.original.command.content_hash, bytes)?;
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        admission.check_content_owner(self, &reservation.sentinel.store_incarnation)?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("original Repair state poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        let key = OriginalRepairKey::of(material);
        if let Some(prior) = state.view.original_repairs.get(&key) {
            if &prior.material != material {
                return Err(inline_error(
                    "original Repair wire identity cannot change its material/incarnation",
                ));
            }
            // Historical delivery is a separate future purpose. Do not route a
            // stored positive through today's ordinary receipt-application path.
            if state.view.original_terminals.contains_key(&key) {
                return Err(inline_error(
                    "original Repair already has a Terminal; historical delivery is not enabled",
                ));
            }
            return Ok(PreparedOriginalRepairRef {
                root: self.root.clone(),
                prepared: prior.clone(),
            });
        }
        let mut prepared = OriginalRepairPrepared {
            key,
            material: material.clone(),
            receiver: material.original.target_node_id.to_string(),
            store_incarnation: reservation.sentinel.store_incarnation.clone(),
            logical_id: state
                .view
                .sequence
                .checked_add(1)
                .ok_or_else(|| inline_error("original Prepared sequence exhausted"))?,
            seal: String::new(),
        };
        prepared.seal = prepared.seal()?;
        let operation = InlineOperation::OriginalRepairPrepared(prepared.clone());
        let record = InlineRecord {
            format_version: ORIGINAL_REPAIR_FIXTURE_VERSION,
            store_incarnation: prepared.store_incarnation.clone(),
            generation: state
                .catalog
                .as_ref()
                .ok_or_else(|| inline_error("original Prepared has no catalog"))?
                .generation,
            sequence: prepared.logical_id,
            operation: operation.clone(),
        };
        if encode_inline(&record)?.len() > INLINE_FRAME_HEADER_LIMIT as usize {
            return Err(inline_error(
                "selected original Repair complete header exceeds limit",
            ));
        }
        drop(state);
        self.quota_stats_initialized.store(false, Ordering::Release);
        let existing_bytes = self
            .readable_blob_size(&material.original.command.content_hash)?
            .unwrap_or(0);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: (bytes.len() as u64).saturating_sub(existing_bytes),
            ..ContentStoreQuotaDelta::default()
        })?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("original Repair state poisoned"))?;
        append_operation(
            &self.root,
            &reservation.sentinel,
            &mut state,
            operation,
            bytes,
        )?;
        drop(state);
        if self.quota.is_some() {
            self.replace_stats_snapshot(&self.scan_stats()?)?;
        }
        Ok(PreparedOriginalRepairRef {
            root: self.root.clone(),
            prepared,
        })
    }

    pub(super) fn check_original_repair_reference(
        &self,
        reference: &PreparedOriginalRepairRef,
        bytes: &[u8],
    ) -> Result<()> {
        self.requires_original_repair_fixture()?;
        if reference.root != self.root {
            return Err(inline_error(
                "original Prepared belongs to another physical root",
            ));
        }
        let reservation = self.lock_content_reservations_for_write()?;
        reference
            .prepared
            .validate(&reservation.sentinel.store_incarnation)?;
        ensure_bytes_match_hash(
            &reference.prepared.material.original.command.content_hash,
            bytes,
        )?;
        if bytes.len() as u64 != reference.prepared.material.attachment.total_size_bytes {
            return Err(inline_error("original Prepared body length changed"));
        }
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("original Repair state poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        if state.view.original_repairs.get(&reference.prepared.key) != Some(&reference.prepared) {
            return Err(inline_error(
                "original Prepared is not synchronized under this physical owner",
            ));
        }
        Ok(())
    }

    pub(crate) fn bind_original_repair_content(
        &self,
        reference: &PreparedOriginalRepairRef,
        bytes: &[u8],
    ) -> Result<BoundOriginalRepairContent> {
        let _owner = self.content_mutation_guard()?;
        self.check_original_repair_reference(reference, bytes)?;
        {
            let reservation = self.lock_content_reservations_for_write()?;
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("original Repair state poisoned"))?;
            ensure_writer(
                &self.root,
                &reservation.sentinel.store_incarnation,
                &mut state,
            )?;
            if let Some(binding) = state.view.original_bindings.get(&reference.prepared.key) {
                validate_binding(
                    &state.view,
                    binding,
                    &reservation.sentinel.store_incarnation,
                )?;
                if state
                    .view
                    .live_manifest(&binding.manifest.object_id, &binding.manifest.version)
                    != Some(&binding.manifest)
                {
                    return Err(inline_error(
                        "original Bound no longer names a live exact version",
                    ));
                }
                return Ok(BoundOriginalRepairContent {
                    prepared: reference.clone(),
                    binding: binding.clone(),
                });
            }
        }
        self.try_write_inline_binding_for_original(reference, bytes)?
            .ok_or_else(|| {
                inline_error("admitted original Repair cannot use generic binding fallback")
            })?;
        let reservation = self.lock_content_reservations_for_write()?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("original Repair state poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        let binding = state
            .view
            .original_bindings
            .get(&reference.prepared.key)
            .ok_or_else(|| inline_error("actual original content binding is absent"))?
            .clone();
        validate_binding(
            &state.view,
            &binding,
            &reservation.sentinel.store_incarnation,
        )?;
        Ok(BoundOriginalRepairContent {
            prepared: reference.clone(),
            binding,
        })
    }

    pub(crate) fn finish_original_repair(
        &self,
        bound: &BoundOriginalRepairContent,
        outcome: &crate::service_repair::NativeOriginalRepairOutcome,
    ) -> Result<DfsRepairReplicaResponse> {
        self.requires_original_repair_fixture()?;
        outcome
            .revalidate(
                &bound.prepared.prepared.material,
                bound.prepared_seal(),
                bound.bound_id(),
            )
            .map_err(inline_error)?;
        outcome.check_content_store(self)?;
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        outcome.check_content_owner(self, &reservation.sentinel.store_incarnation)?;
        if bound.prepared.root != self.root {
            return Err(inline_error(
                "original outcome belongs to another physical root",
            ));
        }
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("original Repair state poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        validate_binding(
            &state.view,
            &bound.binding,
            &reservation.sentinel.store_incarnation,
        )?;
        if state.view.live_manifest(
            &bound.binding.manifest.object_id,
            &bound.binding.manifest.version,
        ) != Some(&bound.binding.manifest)
        {
            return Err(inline_error(
                "original Terminal content is not the live exact bound version",
            ));
        }
        let terminal = OriginalRepairTerminal {
            key: bound.binding.key.clone(),
            prepared_id: bound.binding.prepared_id,
            prepared_seal: bound.binding.prepared_seal.clone(),
            bound_id: bound.binding.logical_id,
            logical_id: state
                .view
                .sequence
                .checked_add(1)
                .ok_or_else(|| inline_error("original Terminal sequence exhausted"))?,
            store_incarnation: reservation.sentinel.store_incarnation.clone(),
            response: outcome.response().clone(),
            provenance: outcome.provenance(),
        };
        if let Some(old) = state.view.original_terminals.get(&terminal.key) {
            if old.response != terminal.response || old.provenance != terminal.provenance {
                return Err(inline_error(
                    "original Terminal result/provenance cannot change",
                ));
            }
            return Ok(old.response.clone());
        }
        append_operation(
            &self.root,
            &reservation.sentinel,
            &mut state,
            InlineOperation::OriginalRepairTerminal(terminal),
            &[],
        )?;
        Ok(outcome.response().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_owner_identity_rejects_other_admitted_private4_writer() {
        let root = sinomo_testdir::TestDir::new("original-repair-foreign-private-owner");
        let (expected_store, expected) =
            LocalContentStore::new_original_repair_fixture(root.join("expected")).unwrap();
        let (other, _) =
            LocalContentStore::new_original_repair_fixture(root.join("other")).unwrap();
        expected
            .check_original_store_identity(&expected_store)
            .unwrap();
        assert!(expected.check_original_store_identity(&other).is_err());
        let _owner = other.content_mutation_guard().unwrap();
        let reservation = other.lock_content_reservations_for_write().unwrap();
        let mut state = other.inline_store.owner.state.lock().unwrap();
        ensure_writer(
            &other.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )
        .unwrap();
        let before = (state.active_end, state.view.sequence);
        let catalog = state.catalog.as_ref().unwrap();
        let path = segment_path(
            &other.root,
            catalog.generation,
            catalog.segments.last().unwrap().id,
        );
        let bytes = fs::read(&path).unwrap();
        assert!(
            expected
                .check_original_store(&other, &reservation.sentinel.store_incarnation)
                .is_err()
        );
        assert_eq!((state.active_end, state.view.sequence), before);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        drop(state);
        drop(reservation);
        drop(_owner);
        let _owner = expected_store.content_mutation_guard().unwrap();
        let reservation = expected_store
            .lock_content_reservations_for_write()
            .unwrap();
        expected
            .check_original_store(&expected_store, &reservation.sentinel.store_incarnation)
            .unwrap();
        assert!(
            expected
                .check_original_store(&expected_store, "another-physical-incarnation")
                .is_err()
        );
    }

    // Storage codec/retention controls only. Raw phase fixtures deliberately
    // mint no NativeContentOnlyRepairAdmission or native Terminal capability.
    fn source_material(root: &Path) -> DfsOriginalRepairMaterial {
        let (queue, _content, message, descriptor, payload, _layout) =
            crate::repair_queue_store::tests::selected_original_fixture(root);
        queue
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        queue.record_original_repair_attempt(&message).unwrap();
        queue
            .original_repair(&message)
            .unwrap()
            .unwrap()
            .original_wire_material()
            .unwrap()
    }

    fn append_prepared(
        store: &LocalContentStore,
        material: DfsOriginalRepairMaterial,
    ) -> PreparedOriginalRepairRef {
        let _owner = store.content_mutation_guard().unwrap();
        let reservation = store.lock_content_reservations_for_write().unwrap();
        let mut state = store.inline_store.owner.state.lock().unwrap();
        ensure_writer(
            &store.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )
        .unwrap();
        let mut prepared = OriginalRepairPrepared {
            key: OriginalRepairKey::of(&material),
            receiver: material.original.target_node_id.to_string(),
            material,
            store_incarnation: reservation.sentinel.store_incarnation.clone(),
            logical_id: state.view.sequence + 1,
            seal: String::new(),
        };
        prepared.seal = prepared.seal().unwrap();
        append_operation(
            &store.root,
            &reservation.sentinel,
            &mut state,
            InlineOperation::OriginalRepairPrepared(prepared.clone()),
            b"complete original attachment",
        )
        .unwrap();
        PreparedOriginalRepairRef {
            root: store.root.clone(),
            prepared,
        }
    }

    #[test]
    fn original_prepared_file_replay_preserves_body_and_conflicting_incarnation_rejects() {
        let root = sinomo_testdir::TestDir::new("original-prepared-file-replay");
        let material = source_material(&root.join("source"));
        let (store, identity) =
            LocalContentStore::new_original_repair_fixture(root.join("receiver")).unwrap();
        let prepared = append_prepared(&store, material.clone());
        let before = store
            .original_repair_test_snapshot(&material)
            .unwrap()
            .unwrap();
        assert!(before.bound_id.is_none() && before.response.is_none());
        let mut changed = prepared.prepared.clone();
        changed
            .material
            .subject
            .queue_incarnation
            .push_str("-different");
        changed.seal = changed.seal().unwrap();
        let _owner = store.content_mutation_guard().unwrap();
        let reservation = store.lock_content_reservations_for_write().unwrap();
        let mut state = store.inline_store.owner.state.lock().unwrap();
        let end = state.active_end;
        assert!(
            append_operation(
                &store.root,
                &reservation.sentinel,
                &mut state,
                InlineOperation::OriginalRepairPrepared(changed),
                b"complete original attachment"
            )
            .is_err()
        );
        assert_eq!(state.active_end, end);
        drop(state);
        drop(reservation);
        drop(_owner);
        drop(prepared);
        store.compact_original_repair_fixture_for_test().unwrap();
        assert_eq!(
            store.original_repair_test_snapshot(&material).unwrap(),
            Some(before.clone())
        );
        drop(store);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            store.original_repair_test_snapshot(&material).unwrap(),
            Some(before)
        );
        assert!(LocalContentStore::new(root.join("receiver")).is_err());
    }

    #[test]
    fn original_bound_is_actual_content_operation_and_terminal_cannot_come_from_hash() {
        let root = sinomo_testdir::TestDir::new("original-bound-no-inferred-terminal");
        let material = source_material(&root.join("source"));
        let (store, identity) =
            LocalContentStore::new_original_repair_fixture(root.join("receiver")).unwrap();
        let prepared = append_prepared(&store, material.clone());
        let bound = store
            .bind_original_repair_content(&prepared, b"complete original attachment")
            .unwrap();
        assert_eq!(
            bound.manifest().content_hash,
            material.original.command.content_hash
        );
        let snapshot = store
            .original_repair_test_snapshot(&material)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.bound_id, Some(bound.bound_id()));
        assert!(snapshot.response.is_none());
        drop(bound);
        drop(prepared);
        drop(store);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            store.original_repair_test_snapshot(&material).unwrap(),
            Some(snapshot)
        );
        let reference = {
            let _owner = store.content_mutation_guard().unwrap();
            let reservation = store.lock_content_reservations_for_write().unwrap();
            let mut state = store.inline_store.owner.state.lock().unwrap();
            ensure_writer(
                &store.root,
                &reservation.sentinel.store_incarnation,
                &mut state,
            )
            .unwrap();
            let p = state
                .view
                .original_repairs
                .get(&OriginalRepairKey::of(&material))
                .unwrap()
                .clone();
            PreparedOriginalRepairRef {
                root: store.root.clone(),
                prepared: p,
            }
        };
        assert_eq!(
            store
                .bind_original_repair_content(&reference, b"complete original attachment")
                .unwrap()
                .bound_id(),
            reference.prepared.logical_id + 1
        );
        assert!(
            store
                .bind_original_repair_content(&reference, b"changed bytes")
                .is_err()
        );
    }

    #[test]
    fn original_terminal_codec_rejects_missing_bound_or_noncommitted_disposition() {
        let root = sinomo_testdir::TestDir::new("original-terminal-invalid-evidence");
        let material = source_material(&root.join("source"));
        let (store, _identity) =
            LocalContentStore::new_original_repair_fixture(root.join("receiver")).unwrap();
        let prepared = append_prepared(&store, material.clone());
        let command = &material.original.command;
        let response = DfsRepairReplicaResponse {
            request_id: command.request_id.clone(),
            intent_id: command.intent_id.clone(),
            file_system_id: command.file_system_id.clone(),
            object_id: command.object_id.clone(),
            version: command.version.clone(),
            replica_node_id: material.original.target_node_id.clone(),
            result: crate::DfsBlobTransferResult::Committed {
                content_hash: command.content_hash.clone(),
            },
        };
        let candidate = OriginalRepairTerminal { key: prepared.prepared.key.clone(), prepared_id: prepared.prepared.logical_id, prepared_seal: prepared.prepared.seal.clone(), bound_id: prepared.prepared.logical_id+1, logical_id: prepared.prepared.logical_id+1, store_incarnation: prepared.prepared.store_incarnation.clone(), response, provenance: crate::publication_guard::NativeContentOnlyRepairProvenance { receiver: material.original.target_node_id.clone(), admitted_shard: command.authority_shard.clone().unwrap(), effect: crate::publication_guard::NativeRepairEffectManifest::ReceiverContentAndLocalCacheOnly } };
        let reject = |candidate| {
            let _owner = store.content_mutation_guard().unwrap();
            let reservation = store.lock_content_reservations_for_write().unwrap();
            let mut state = store.inline_store.owner.state.lock().unwrap();
            let before = state.active_end;
            assert!(
                append_operation(
                    &store.root,
                    &reservation.sentinel,
                    &mut state,
                    InlineOperation::OriginalRepairTerminal(candidate),
                    &[]
                )
                .is_err()
            );
            assert_eq!(state.active_end, before);
        };
        reject(candidate.clone()); // exact hash/response without actual Bound
        let bound = store
            .bind_original_repair_content(&prepared, b"complete original attachment")
            .unwrap();
        let mut candidate = candidate;
        candidate.bound_id = bound.bound_id();
        candidate.logical_id = bound.bound_id() + 1;
        candidate.response.result = crate::DfsBlobTransferResult::Durable {
            content_hash: command.content_hash.clone(),
        };
        reject(candidate); // same tuple/hash, different effect disposition
        assert!(
            store
                .original_repair_test_snapshot(&material)
                .unwrap()
                .unwrap()
                .response
                .is_none()
        );
    }
}

pub(super) fn binding_for_reference(
    reference: &PreparedOriginalRepairRef,
    manifest: ContentManifest,
    logical_id: u64,
) -> OriginalRepairBinding {
    OriginalRepairBinding {
        key: reference.prepared.key.clone(),
        prepared_id: reference.prepared.logical_id,
        prepared_seal: reference.prepared.seal.clone(),
        logical_id,
        store_incarnation: reference.prepared.store_incarnation.clone(),
        manifest,
    }
}
fn validate_binding(
    view: &InlineBindingView,
    binding: &OriginalRepairBinding,
    incarnation: &str,
) -> Result<()> {
    let prepared = view
        .original_repairs
        .get(&binding.key)
        .ok_or_else(|| inline_error("original Bound has no exact Prepared"))?;
    prepared.validate(incarnation)?;
    let command = &prepared.material.original.command;
    if binding.prepared_id != prepared.logical_id
        || binding.prepared_seal != prepared.seal
        || binding.logical_id == 0
        || binding.store_incarnation != incarnation
        || binding.manifest.object_id != command.object_id.as_str()
        || binding.manifest.version != command.version.as_str()
        || binding.manifest.content_hash != command.content_hash
        || binding.manifest.size_bytes != prepared.material.attachment.total_size_bytes
        || !binding.manifest.extents.is_empty()
        || view
            .original_bindings
            .get(&binding.key)
            .is_some_and(|old| old != binding)
    {
        return Err(inline_error(
            "original Bound changed its exact Prepared/material",
        ));
    }
    Ok(())
}
fn validate_terminal(
    view: &InlineBindingView,
    terminal: &OriginalRepairTerminal,
    incarnation: &str,
) -> Result<()> {
    let prepared = view
        .original_repairs
        .get(&terminal.key)
        .ok_or_else(|| inline_error("Terminal has no original Prepared"))?;
    let bound = view
        .original_bindings
        .get(&terminal.key)
        .ok_or_else(|| inline_error("Terminal has no actual original Bound"))?;
    validate_binding(view, bound, incarnation)?;
    let command = &prepared.material.original.command;
    let response = &terminal.response;
    if terminal.logical_id == 0 || terminal.prepared_id != prepared.logical_id || terminal.prepared_seal != prepared.seal
        || terminal.bound_id != bound.logical_id || terminal.store_incarnation != incarnation
        || response.request_id != command.request_id || response.intent_id != command.intent_id || response.file_system_id != command.file_system_id
        || response.object_id != command.object_id || response.version != command.version
        || response.replica_node_id != prepared.material.original.target_node_id
        || response.result != (crate::DfsBlobTransferResult::Committed { content_hash: command.content_hash.clone() })
        || terminal.provenance.receiver != response.replica_node_id || Some(&terminal.provenance.admitted_shard) != command.authority_shard.as_ref()
        || terminal.provenance.effect != crate::publication_guard::NativeRepairEffectManifest::ReceiverContentAndLocalCacheOnly
        || view.original_terminals.get(&terminal.key).is_some_and(|old| old != terminal)
    { return Err(inline_error("original Terminal changed exact native outcome/effect identity")); }
    Ok(())
}

pub(super) fn fixture_operation(operation: &InlineOperation) -> bool {
    matches!(
        operation,
        InlineOperation::OriginalRepairPrepared(_)
            | InlineOperation::RestoreOriginalRepairPrepared(_)
            | InlineOperation::OriginalRepairBound { .. }
            | InlineOperation::RestoreOriginalRepairBound(_)
            | InlineOperation::OriginalRepairTerminal(_)
            | InlineOperation::RestoreOriginalRepairTerminal(_)
    )
}
pub(super) fn validate_phase_sequence(operation: &InlineOperation, sequence: u64) -> Result<()> {
    let actual = match operation {
        InlineOperation::OriginalRepairPrepared(p) => p.logical_id,
        InlineOperation::OriginalRepairBound { binding, .. } => binding.logical_id,
        InlineOperation::OriginalRepairTerminal(t) => t.logical_id,
        _ => return Ok(()),
    };
    if actual != sequence {
        return Err(inline_error(
            "original phase was not assigned by actual append",
        ));
    }
    Ok(())
}
pub(super) fn validate_operation_data(
    view: &InlineBindingView,
    operation: &InlineOperation,
    payload_hash: &str,
    payload_bytes: u64,
    incarnation: &str,
) -> Result<()> {
    match operation {
        InlineOperation::OriginalRepairPrepared(p)
        | InlineOperation::RestoreOriginalRepairPrepared(p) => {
            p.validate(incarnation)?;
            if view
                .original_repairs
                .get(&p.key)
                .is_some_and(|old| old != p)
            {
                return Err(inline_error("original wire identity changed its Prepared"));
            }
            let hash = &p.material.original.command.content_hash;
            let size = p.material.attachment.total_size_bytes;
            if matches!(operation, InlineOperation::OriginalRepairPrepared(_)) {
                if hash != payload_hash || size != payload_bytes {
                    return Err(inline_error("original Prepared body is not exact"));
                }
            } else if payload_bytes != 0
                || size != 0
                    && !view
                        .blobs
                        .get(hash)
                        .is_some_and(|blob| blob.size_bytes == size)
            {
                return Err(inline_error(
                    "restored original Prepared has no exact retained body",
                ));
            }
        }
        InlineOperation::OriginalRepairBound {
            binding,
            published_at_unix_nanos,
            reservation,
        } => {
            validate_binding(view, binding, incarnation)?;
            let ordinary = if binding.manifest.size_bytes == 0 {
                InlineOperation::Bind {
                    manifest: binding.manifest.clone(),
                    published_at_unix_nanos: *published_at_unix_nanos,
                    reservation: reservation.clone(),
                }
            } else {
                InlineOperation::Link {
                    manifest: binding.manifest.clone(),
                    published_at_unix_nanos: *published_at_unix_nanos,
                    reservation: reservation.clone(),
                }
            };
            validate_operation(view, &ordinary, payload_hash, payload_bytes, incarnation)?;
        }
        InlineOperation::RestoreOriginalRepairBound(binding) => {
            validate_binding(view, binding, incarnation)?;
            if payload_bytes != 0 {
                return Err(inline_error("restored original Bound has payload"));
            }
        }
        InlineOperation::OriginalRepairTerminal(t)
        | InlineOperation::RestoreOriginalRepairTerminal(t) => {
            validate_terminal(view, t, incarnation)?;
            if payload_bytes != 0 {
                return Err(inline_error("original Terminal has unexpected payload"));
            }
        }
        _ => return Err(inline_error("unexpected original Repair operation")),
    }
    Ok(())
}
pub(super) fn apply_prepared(
    view: &mut InlineBindingView,
    prepared: OriginalRepairPrepared,
    location: Option<InlineBlobEntry>,
) {
    let hash = &prepared.material.original.command.content_hash;
    if !view.original_repairs.contains_key(&prepared.key)
        && prepared.material.attachment.total_size_bytes != 0
    {
        change_root_count(&mut view.repair_hashes, hash.clone(), true);
    }
    if let Some(location) = location
        && prepared.material.attachment.total_size_bytes != 0
    {
        view.deleted_blobs.remove(hash);
        view.blobs.insert(hash.clone(), location);
    }
    view.original_repairs.insert(prepared.key.clone(), prepared);
}
pub(super) fn apply_binding(view: &mut InlineBindingView, binding: OriginalRepairBinding) {
    view.original_bindings.insert(binding.key.clone(), binding);
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OriginalRepairTestSnapshot {
    pub(crate) material: DfsOriginalRepairMaterial,
    pub(crate) bytes: Vec<u8>,
    pub(crate) prepared_id: u64,
    pub(crate) bound_id: Option<u64>,
    pub(crate) terminal_id: Option<u64>,
    pub(crate) response: Option<DfsRepairReplicaResponse>,
    pub(crate) store_incarnation: String,
}
#[cfg(test)]
impl LocalContentStore {
    pub(crate) fn original_repair_test_snapshot(
        &self,
        material: &DfsOriginalRepairMaterial,
    ) -> Result<Option<OriginalRepairTestSnapshot>> {
        self.requires_original_repair_fixture()?;
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("original Repair state poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        let key = OriginalRepairKey::of(material);
        let Some(prepared) = state.view.original_repairs.get(&key).cloned() else {
            return Ok(None);
        };
        let bound_id = state
            .view
            .original_bindings
            .get(&key)
            .map(|bound| bound.logical_id);
        let terminal = state.view.original_terminals.get(&key).cloned();
        drop(state);
        let bytes = self.read_blob(&prepared.material.original.command.content_hash)?;
        Ok(Some(OriginalRepairTestSnapshot {
            material: prepared.material,
            bytes,
            prepared_id: prepared.logical_id,
            bound_id,
            terminal_id: terminal.as_ref().map(|terminal| terminal.logical_id),
            response: terminal.map(|terminal| terminal.response),
            store_incarnation: prepared.store_incarnation,
        }))
    }
}
