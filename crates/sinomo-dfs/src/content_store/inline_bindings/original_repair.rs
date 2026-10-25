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
    #[cfg(test)]
    pub(crate) fn arm_original_repair_after_prepared_test_cut(&self) -> Result<()> {
        self.requires_original_repair_fixture()?;
        arm_original_repair_test_fault(&self.root, InlineFault::ReceiverAfterPrepared)
    }

    /// One native negative fixture swaps in a byte-exact physical copy while
    /// the actual content mutation owner excludes ordinary content writers.
    /// The copy is never admitted. Both roots are retained for failed evidence.
    #[cfg(test)]
    pub(crate) fn with_original_history_replaced_root_for_test<R>(&self, check: impl FnOnce() -> R) -> Result<R> {
        self.requires_original_repair_fixture()?;
        let _owner = self.content_mutation_guard()?;
        let _reservation = self.lock_content_reservations_for_write()?;
        let parent = self.root.parent().ok_or_else(|| inline_error("history fixture has no parent"))?;
        let copy = parent.join("history-replacement-copy");
        let parked = parent.join("history-original-physical-root");
        let rejected = parent.join("history-rejected-physical-copy");
        for path in [&copy, &parked, &rejected] {
            if fs::symlink_metadata(path).is_ok() { return Err(inline_error("history replacement fixture path is occupied")); }
        }
        fn copy_exact(source: &Path, target: &Path) -> Result<()> {
            fs::create_dir(target).map_err(|source| ContentStoreError::Io { path: target.to_owned(), source })?;
            for entry in fs::read_dir(source).map_err(|error| ContentStoreError::Io { path: source.to_owned(), source: error })? {
                let entry = entry.map_err(inline_error)?;
                let original = entry.path();
                let output = target.join(entry.file_name());
                let metadata = fs::symlink_metadata(&original).map_err(inline_error)?;
                if metadata.file_type().is_symlink() { return Err(inline_error("history copy cannot follow a symlink")); }
                if metadata.is_dir() { copy_exact(&original, &output)?; }
                else if metadata.is_file() {
                    fs::copy(&original, &output).map_err(inline_error)?;
                    if fs::read(&original).map_err(inline_error)? != fs::read(&output).map_err(inline_error)? {
                        return Err(inline_error("history physical copy differs from the actual owner"));
                    }
                } else { return Err(inline_error("history physical copy has a nonregular entry")); }
            }
            Ok(())
        }
        copy_exact(&self.root, &copy)?;
        struct Restore { root: PathBuf, parked: PathBuf, rejected: PathBuf, restored: bool }
        impl Restore {
            fn restore(&mut self) -> Result<()> {
                if self.restored { return Ok(()); }
                if self.root.exists() { fs::rename(&self.root, &self.rejected).map_err(inline_error)?; }
                fs::rename(&self.parked, &self.root).map_err(inline_error)?;
                self.restored = true;
                Ok(())
            }
        }
        impl Drop for Restore {
            fn drop(&mut self) { if let Err(error) = self.restore() { eprintln!("history fixture physical restoration failed: {error}"); } }
        }
        fs::rename(&self.root, &parked).map_err(inline_error)?;
        let mut restore = Restore { root: self.root.clone(), parked, rejected, restored: false };
        fs::rename(&copy, &self.root).map_err(inline_error)?;
        let result = check();
        restore.restore()?;
        Ok(result)
    }

    /// The owning Prepared codec, reused by history validators. This returns a
    /// deterministic DATA seal, never receiver acceptance or durability.
    pub(crate) fn original_repair_prepared_seal(
        material: &DfsOriginalRepairMaterial, receiver: &sinomo_core::node::NodeId,
        incarnation: &str, prepared_id: u64,
    ) -> Result<String> {
        material.validate().map_err(inline_error)?;
        OriginalRepairPrepared {
            key: OriginalRepairKey::of(material), material: material.clone(),
            receiver: receiver.to_string(), store_incarnation: incarnation.to_owned(),
            logical_id: prepared_id, seal: String::new(),
        }.seal()
    }

    #[cfg(test)]
    pub(crate) fn arm_original_history_certification_failure_for_test(&self) -> Result<()> {
        self.requires_original_repair_fixture()?;
        arm_original_repair_test_fault(&self.root, InlineFault::CatalogCertification)?;
        let _owner = self.content_mutation_guard()?;
        let _reservation = self.lock_content_reservations_for_write()?;
        // Explicit fixture forcing of the existing writer-recertification
        // branch; it is not evidence of an actual OS crash or unsynced ACK.
        self.inline_store.owner.state.lock().map_err(|_| inline_error("history state poisoned"))?.uncertain = true;
        Ok(())
    }

    /// Certify retained accepted history through the actual existing writer.
    /// This may sync/recover an unacknowledged tail; it never runs an effect or
    /// synthesizes a missing phase from current content/receipt/cache state.
    pub(crate) fn certified_original_repair_terminal(
        &self,
        request: &crate::DfsOriginalRepairHistoryRequest,
        verified: &crate::publication_guard::VerifiedOriginalRepairHistoryRead,
    ) -> Result<crate::DfsOriginalRepairHistoryResult> {
        verified.revalidate(request).map_err(inline_error)?;
        verified.check_content_store(self)?;
        self.requires_original_repair_fixture()?;
        let result = {
            let _owner = self.content_mutation_guard()?;
            let reservation = self.lock_content_reservations_for_write()?;
            verified.check_content_owner(self, &reservation.sentinel.store_incarnation)?;
            let mut state = self.inline_store.owner.state.lock().map_err(|_| inline_error("history state poisoned"))?;
            ensure_writer(&self.root, &reservation.sentinel.store_incarnation, &mut state)?;
            let key = OriginalRepairKey::of(&request.original_material);
            match state.view.original_repairs.get(&key) {
                Some(prepared) if prepared.material != request.original_material => return Err(inline_error("history query changed the original accepted material")),
                _ => {}
            }
            if let Some(terminal) = state.view.original_terminals.get(&key) {
                validate_terminal(&state.view, terminal, &reservation.sentinel.store_incarnation)?;
                crate::DfsOriginalRepairHistoryResult::HistoricalTerminal {
                    certificate: crate::DfsOriginalRepairHistoryCertificate {
                        store_incarnation: terminal.store_incarnation.clone(), prepared_id: terminal.prepared_id,
                        prepared_seal: terminal.prepared_seal.clone(), bound_id: terminal.bound_id,
                        terminal_id: terminal.logical_id,
                        response_digest: crate::repair_queue_store::repair_result_digest(&terminal.response).map_err(inline_error)?,
                        provenance: terminal.provenance.clone(),
                    },
                    exact_original_response: terminal.response.clone(),
                }
            } else {
                crate::DfsOriginalRepairHistoryResult::Pending { reason: crate::DfsOriginalRepairHistoryPending::MissingTerminal }
            }
        };
        // Configuration/projection callbacks are never called with the content
        // mutation/reservation/state locks held.
        verified.revalidate(request).map_err(inline_error)?;
        Ok(result)
    }

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
    fn original_repair_test_fault_lifetime_clears_early_errors_and_unwind_only() {
        let root = sinomo_testdir::TestDir::new("original-repair-worker-fault-lifetime");
        let (store, identity) = LocalContentStore::new_original_repair_fixture(root.join("content")).unwrap();
        let (other, other_identity) = LocalContentStore::new_original_repair_fixture(root.join("other")).unwrap();
        let assert_empty = || {
            assert!(INLINE_FAULT.with(|slot| slot.borrow().is_none()));
            assert!(ORIGINAL_REPAIR_TEST_FAULT.with(|slot| slot.borrow().is_none()));
        };
        assert_empty();
        for missing in [true, false] {
            let invoked = std::cell::Cell::new(false);
            let lookup = if missing { Ok(None) } else { Err(crate::DfsError::Metadata { message: "actual native owner lookup refused fixture DATA".into() }) };
            let refused = crate::runtime_host::run_original_repair_test_hook_with_cleanup(lookup, || {
                invoked.set(true);
                // The retained private4 store is valid and WOULD arm, so only
                // the mandatory native capture can prevent this callback.
                store.arm_original_repair_after_prepared_test_cut()?;
                Ok(())
            });
            let error = match refused { Ok(_) => panic!("missing/refused native owner cannot reach the hook"), Err(error) => error };
            assert!(!invoked.get());
            if missing { assert!(error.to_string().contains("requires its actual native DATA owner")); }
            else { assert!(error.to_string().contains("actual native owner lookup refused fixture DATA")); }
            assert_empty();
        }
        for catalog in [false, true] {
            for unwind in [false, true] {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
                    let _cleanup = identity.original_repair_test_fault_cleanup().unwrap();
                    if catalog { store.arm_original_history_certification_failure_for_test()?; }
                    else { store.arm_original_repair_after_prepared_test_cut()?; }
                    // No injection point is consumed: model the earlier
                    // owner/admission/preparation error or unwind explicitly.
                    if unwind { panic!("earlier worker preparation unwound"); }
                    Err(inline_error("earlier worker preparation returned error"))
                }));
                if unwind { assert!(outcome.is_err()); }
                else { assert!(outcome.unwrap().unwrap_err().to_string().contains("earlier worker preparation returned error")); }
                assert_empty();
            }
        }
        // A real subsequent writer recertifies successfully. Cleanup removed
        // only the injection; it did not reset the deliberately uncertain state.
        let object = crate::DfsObjectId::from_legacy_named("fault-lifetime-object").unwrap();
        let version = crate::DfsVersion::from_legacy_named("fault-lifetime-version").unwrap();
        store.put_object_version(object.as_str(), version.as_str(), b"next real owner write").unwrap();

        let other_cleanup = other_identity.original_repair_test_fault_cleanup().unwrap();
        other.arm_original_repair_after_prepared_test_cut().unwrap();
        drop(identity.original_repair_test_fault_cleanup());
        assert!(INLINE_FAULT.with(|slot| slot.borrow().as_ref().is_some_and(|(path, point)| path == &other.root && *point == InlineFault::ReceiverAfterPrepared)));
        drop(other_cleanup);
        assert_empty();

        let cleanup = identity.original_repair_test_fault_cleanup();
        store.arm_original_repair_after_prepared_test_cut().unwrap();
        INLINE_FAULT.with(|slot| *slot.borrow_mut() = Some((store.root.clone(), InlineFault::LostAppendCompletion)));
        drop(cleanup);
        assert!(take_inline_fault(&store.root, InlineFault::LostAppendCompletion), "another ordinary fault cannot be cleared by stale lifetime provenance");
        assert_empty();

        let cleanup = identity.original_repair_test_fault_cleanup();
        store.arm_original_repair_after_prepared_test_cut().unwrap();
        store.with_original_history_replaced_root_for_test(|| {
            assert!(identity.check_original_physical_identity().is_err());
            drop(cleanup);
            assert_empty(); // Cleanup needs no recovery/open of the now-refused root.
        }).unwrap();
        identity.check_original_store_identity(&store).unwrap();

        for point in [InlineFault::CatalogCertification, InlineFault::LostAppendCompletion] {
            INLINE_FAULT.with(|slot| *slot.borrow_mut() = Some((store.root.clone(), point)));
            let cleanup = identity.original_repair_test_fault_cleanup();
            assert!(store.arm_original_repair_after_prepared_test_cut().is_err(), "cannot overwrite an ordinary preexisting fault");
            drop(cleanup);
            assert!(INLINE_FAULT.with(|slot| slot.borrow().as_ref().is_some_and(|(path, armed)| path == &store.root && *armed == point)));
            assert!(take_inline_fault(&store.root, point));
            assert_empty();
        }
        // An ordinary same-point replacement BEFORE consumption must also
        // discard obsolete ownership; root/point equality alone is not enough.
        let cleanup = identity.original_repair_test_fault_cleanup();
        store.arm_original_repair_after_prepared_test_cut().unwrap();
        store.arm_received_repair_after_prepared_test_cut();
        drop(cleanup);
        assert!(take_inline_fault(&store.root, InlineFault::ReceiverAfterPrepared));
        assert_empty();
        // Consumption releases the lifetime tag immediately. A later ordinary
        // fault at the same root/point must survive this invocation's cleanup.
        let cleanup = identity.original_repair_test_fault_cleanup();
        store.arm_original_repair_after_prepared_test_cut().unwrap();
        assert!(store.received_repair_after_prepared_test_cut().unwrap_err().to_string()
            .contains("injected receiver cut after Prepared before content binding"));
        store.arm_received_repair_after_prepared_test_cut();
        drop(cleanup);
        assert!(take_inline_fault(&store.root, InlineFault::ReceiverAfterPrepared));
        assert_empty();
    }

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

    // OS-process/storage controls only: these raw fixtures mint no native
    // admission, Terminal, transport or metadata-commit evidence.
    #[cfg(unix)]
    mod process_storage {
        use super::super::super::received_repair::OriginalRepairProcessExpectation;
        use super::*;
        use std::os::unix::{fs::MetadataExt, process::ExitStatusExt};
        use std::process::{Child, Command, ExitStatus, Stdio};

        pub(super) const HELPER: &str = "content_store::inline_bindings::original_repair::tests::original_storage_phase_process_child";
        const INPUT_ENV: &str = "SINOMO_DFS_ORIGINAL_STORAGE_PROCESS_INPUT";
        const BODY: &[u8] = b"complete original attachment";
        const READY_CAP: Duration = Duration::from_secs(10);
        const REAP_CAP: Duration = Duration::from_secs(5);
        const RAW_CAP: usize = 1024 * 1024;
        type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
        pub(super) enum Phase {
            Prepared,
            Bound,
        }

        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            fixture: PathBuf,
            invocation: String,
            phase: Phase,
            expected: OriginalRepairProcessExpectation,
            material: DfsOriginalRepairMaterial,
        }

        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Ready {
            pid: u32,
            invocation: String,
            phase: Phase,
            expected: OriginalRepairProcessExpectation,
            material: DfsOriginalRepairMaterial,
            body: Vec<u8>,
            prepared_id: u64,
            bound_id: Option<u64>,
            terminal_id: Option<u64>,
            response: Option<DfsRepairReplicaResponse>,
        }

        #[derive(Debug, Eq, PartialEq, Serialize)]
        struct RawFile {
            relative: PathBuf,
            hash: String,
            bytes: Vec<u8>,
        }

        fn bounded_regular_file(path: &Path, cap: usize) -> TestResult<Vec<u8>> {
            let before = fs::symlink_metadata(path)?;
            if !before.is_file() || before.file_type().is_symlink() || before.len() > cap as u64 {
                return Err(format!(
                    "not a bounded regular process fixture file: {}",
                    path.display()
                )
                .into());
            }
            let mut file = fs::File::open(path)?;
            let opened = file.metadata()?;
            if (opened.dev(), opened.ino(), opened.len())
                != (before.dev(), before.ino(), before.len())
            {
                return Err("process fixture file changed while opening".into());
            }
            let mut bytes = Vec::new();
            (&mut file).take(cap as u64 + 1).read_to_end(&mut bytes)?;
            let after = fs::symlink_metadata(path)?;
            if bytes.len() as u64 != before.len()
                || bytes.len() > cap
                || !after.is_file()
                || after.file_type().is_symlink()
                || (
                    before.dev(),
                    before.ino(),
                    before.len(),
                    before.mtime(),
                    before.mtime_nsec(),
                ) != (
                    after.dev(),
                    after.ino(),
                    after.len(),
                    after.mtime(),
                    after.mtime_nsec(),
                )
            {
                return Err("process fixture file changed during bounded read".into());
            }
            Ok(bytes)
        }

        fn read_data<T: serde::de::DeserializeOwned>(path: &Path) -> TestResult<T> {
            Ok(sinomo_object_serde::from_bytes(&bounded_regular_file(
                path,
                INLINE_FRAME_HEADER_LIMIT as usize,
            )?)?)
        }

        fn write_data<T: Serialize>(path: &Path, data: &T) -> TestResult<()> {
            let bytes = crate::metadata_io::encode_sobj(data)?;
            if bytes.len() > INLINE_FRAME_HEADER_LIMIT as usize {
                return Err("process fixture DATA exceeds 64KiB".into());
            }
            crate::metadata_io::write_file_atomic(path, &bytes)?;
            Ok(())
        }

        fn raw_receiver(root: &Path) -> TestResult<Vec<RawFile>> {
            fn visit(
                root: &Path,
                path: &Path,
                total: &mut usize,
                entries: &mut usize,
                rows: &mut Vec<RawFile>,
            ) -> TestResult<()> {
                *entries += 1;
                if *entries > 256 {
                    return Err("process fixture capture exceeds 256 entries".into());
                }
                let metadata = fs::symlink_metadata(path)?;
                if metadata.file_type().is_symlink() {
                    return Err("symlink in process fixture capture".into());
                }
                if metadata.is_dir() {
                    for entry in fs::read_dir(path)? {
                        visit(root, &entry?.path(), total, entries, rows)?;
                    }
                } else if metadata.is_file() {
                    let bytes = bounded_regular_file(path, RAW_CAP - *total)?;
                    *total += bytes.len();
                    rows.push(RawFile {
                        relative: path.strip_prefix(root)?.to_owned(),
                        hash: blake3::hash(&bytes).to_hex().to_string(),
                        bytes,
                    });
                } else {
                    return Err("nonregular process fixture capture entry".into());
                }
                Ok(())
            }
            let mut rows = Vec::new();
            visit(root, root, &mut 0, &mut 0, &mut rows)?;
            rows.sort_by(|a, b| a.relative.cmp(&b.relative));
            Ok(rows)
        }

        /// Exact Child custody, including failed Result paths. The first drain
        /// deadline is permanent; Drop never grants another five seconds.
        struct OwnedChild {
            child: Option<Child>,
            fixture: Option<sinomo_testdir::TestDir>,
            root: PathBuf,
            reap_deadline: Option<Instant>,
        }

        impl OwnedChild {
            fn spawn(fixture: sinomo_testdir::TestDir, command: &mut Command) -> TestResult<Self> {
                let root = fixture.path().to_owned();
                match command.spawn() {
                    Ok(child) => Ok(Self {
                        child: Some(child),
                        fixture: Some(fixture),
                        root,
                        reap_deadline: None,
                    }),
                    Err(error) => {
                        eprintln!(
                            "retained process fixture after spawn error: {}",
                            fixture.keep().display()
                        );
                        Err(error.into())
                    }
                }
            }

            fn keep(&mut self) {
                if let Some(fixture) = self.fixture.take() {
                    eprintln!(
                        "retained undrained process fixture: {}",
                        fixture.keep().display()
                    );
                }
            }

            fn drain(&mut self, require_alive: bool) -> TestResult<ExitStatus> {
                let deadline = *self
                    .reap_deadline
                    .get_or_insert_with(|| Instant::now() + REAP_CAP);
                let child = self.child.as_mut().ok_or("process child already reaped")?;
                if let Some(status) = child.try_wait()? {
                    self.child = None;
                    if require_alive {
                        return Err(
                            format!("process child exited before owned kill: {status}").into()
                        );
                    }
                    if Instant::now() > deadline {
                        return Err("process child was reaped after original 5s cleanup deadline".into());
                    }
                    return Ok(status);
                }
                child.kill()?;
                loop {
                    if let Some(status) = child.try_wait()? {
                        self.child = None;
                        if Instant::now() > deadline {
                            return Err(
                                "owned process child was reaped after original 5s drain".into()
                            );
                        }
                        return Ok(status);
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(
                            "owned process child was not reaped within original 5s drain".into(),
                        );
                    }
                    std::thread::sleep(remaining.min(Duration::from_millis(10)));
                }
            }

            fn kill_and_reap(&mut self) -> TestResult<ExitStatus> {
                let result = self.drain(true);
                if result.is_err() {
                    self.keep();
                }
                result
            }

            fn ready(&mut self, deadline: Instant) -> TestResult<Ready> {
                let path = self.root.join("ready.sobj");
                loop {
                    if Instant::now() >= deadline {
                        return Err("original process phase did not become Ready within 10s including spawn".into());
                    }
                    let child = self
                        .child
                        .as_mut()
                        .ok_or("process child absent before Ready")?;
                    if let Some(status) = child.try_wait()? {
                        self.child = None;
                        return Err(format!("process child exited before Ready: {status}").into());
                    }
                    match fs::symlink_metadata(&path) {
                        Ok(_) => {
                            let ready: Ready = read_data(&path)?;
                            if ready.pid != child.id() {
                                return Err("Ready PID differs from exact Child".into());
                            }
                            if Instant::now() >= deadline {
                                return Err(
                                    "process Ready decode completed after 10s deadline".into()
                                );
                            }
                            return Ok(ready);
                        }
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                    std::thread::sleep(
                        Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
            }
        }

        impl Drop for OwnedChild {
            fn drop(&mut self) {
                if self.child.is_some() {
                    if let Err(error) = self.drain(false) {
                        self.keep();
                        eprintln!("exact process child cleanup unproved: {error}");
                        if !std::thread::panicking() {
                            panic!("exact process child cleanup unproved: {error}");
                        }
                    }
                }
            }
        }

        pub(super) fn child() -> TestResult<()> {
            let input_path = PathBuf::from(
                std::env::var_os(INPUT_ENV)
                    .ok_or("ignored process helper requires its parent's explicit input")?,
            );
            let input: Input = read_data(&input_path)?;
            let fixture_metadata = fs::symlink_metadata(&input.fixture)?;
            if !input.fixture.is_absolute()
                || !fixture_metadata.is_dir()
                || fixture_metadata.file_type().is_symlink()
                || input_path != input.fixture.join("input.sobj")
                || input.expected.root != input.fixture.join("receiver")
                || input.invocation
                    != format!(
                        "{}:{:?}",
                        input
                            .fixture
                            .file_name()
                            .ok_or("fixture name absent")?
                            .to_string_lossy(),
                        input.phase
                    )
            {
                return Err("process helper paths/invocation differ from owned fixture".into());
            }
            let store = LocalContentStore::reopen_original_process_fixture(&input.expected)?;
            let prepared = append_prepared(&store, input.material.clone());
            if input.phase == Phase::Bound {
                store.bind_original_repair_content(&prepared, BODY)?;
            }
            let actual = store
                .original_repair_test_snapshot(&input.material)?
                .ok_or("actual process phase absent")?;
            if actual.material != input.material
                || actual.bytes != BODY
                || actual.terminal_id.is_some()
                || actual.response.is_some()
                || actual.bound_id.is_some() != (input.phase == Phase::Bound)
            {
                return Err(
                    "actual process phase differs from requested storage-only state".into(),
                );
            }
            write_data(
                &input.fixture.join("ready.sobj"),
                &Ready {
                    pid: std::process::id(),
                    invocation: input.invocation,
                    phase: input.phase,
                    expected: input.expected,
                    material: actual.material,
                    body: actual.bytes,
                    prepared_id: actual.prepared_id,
                    bound_id: actual.bound_id,
                    terminal_id: actual.terminal_id,
                    response: actual.response,
                },
            )?;
            // Retain the live writer until the parent's kill. EOF/normal exit
            // is a failed witness, never a substitute for SIGKILL.
            let mut byte = [0u8];
            let waited = io::stdin().read_exact(&mut byte);
            std::hint::black_box(&store);
            drop(prepared);
            drop(store);
            Err(format!("parent did not SIGKILL live process helper: stdin={waited:?}").into())
        }

        pub(super) fn arm(phase: Phase) {
            let fixture = sinomo_testdir::TestDir::new("original-storage-owned-sigkill");
            let root = fixture.path().to_owned();
            let material = source_material(&root.join("source"));
            let (store, identity) =
                LocalContentStore::new_original_repair_fixture(root.join("receiver")).unwrap();
            let expected = identity.original_process_expectation().unwrap();
            let input = Input {
                invocation: format!("{}:{phase:?}", root.file_name().unwrap().to_string_lossy()),
                fixture: root.clone(),
                phase,
                expected: expected.clone(),
                material: material.clone(),
            };
            let input_path = root.join("input.sobj");
            write_data(&input_path, &input).unwrap();
            let stdout = fs::File::create(root.join("child.stdout.log")).unwrap();
            let stderr = fs::File::create(root.join("child.stderr.log")).unwrap();
            drop(store); // Identity DATA remains; no parent physical writer remains.
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", HELPER, "--ignored", "--nocapture"])
                .env(INPUT_ENV, &input_path)
                .stdin(Stdio::piped())
                .stdout(stdout)
                .stderr(stderr);
            let started = Instant::now();
            let ready_deadline = started + READY_CAP; // Includes spawn, open, append and Ready sync.
            let mut owned = OwnedChild::spawn(fixture, &mut command).unwrap();
            let ready = owned.ready(ready_deadline).unwrap();
            assert_eq!(ready.invocation, input.invocation);
            assert_eq!(ready.phase, phase);
            assert_eq!(ready.expected, expected);
            assert_eq!(ready.material, material);
            assert_eq!(ready.body, BODY);
            assert!(
                ready.prepared_id > 0 && ready.terminal_id.is_none() && ready.response.is_none()
            );
            assert_eq!(ready.bound_id.is_some(), phase == Phase::Bound);
            assert!(
                Instant::now() < ready_deadline,
                "complete Ready validation must fit original 10s deadline"
            );
            let ready_elapsed = started.elapsed();
            let kill_started = Instant::now();
            let status = owned.kill_and_reap().unwrap();
            assert_eq!(
                status.signal(),
                Some(9),
                "only parent-issued SIGKILL is this witness"
            );
            eprintln!(
                "original storage process {phase:?}: pid={} Ready={ready_elapsed:?}, kill/reap={:?}, signal={:?}, root={}",
                ready.pid,
                kill_started.elapsed(),
                status.signal(),
                root.display()
            );

            // Save actual bytes/hashes BEFORE any normal writer recovery.
            let raw = raw_receiver(&expected.root).unwrap();
            let encoded = crate::metadata_io::encode_sobj(&raw).unwrap();
            crate::metadata_io::write_file_atomic(&root.join("raw-postkill.sobj"), &encoded)
                .unwrap();
            let mut wrong_incarnation = expected.clone();
            wrong_incarnation.store_incarnation.push_str("-changed");
            assert!(
                LocalContentStore::reopen_original_process_fixture(&wrong_incarnation).is_err()
            );
            fs::create_dir(root.join("other-directory")).unwrap();
            let mut wrong_physical = expected.clone();
            wrong_physical.root = root.join("other-directory");
            assert!(LocalContentStore::reopen_original_process_fixture(&wrong_physical).is_err());
            assert!(
                LocalContentStore::new(expected.root.clone()).is_err(),
                "default2 cannot adopt private4"
            );
            assert_eq!(
                raw_receiver(&expected.root).unwrap(),
                raw,
                "refused opens must not rewrite the stored phase"
            );

            // This is actual owner recovery/certification, not an observation.
            let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
            let actual = store
                .original_repair_test_snapshot(&material)
                .unwrap()
                .unwrap();
            assert_eq!(actual.material, material);
            assert_eq!(actual.bytes, BODY);
            assert_eq!(actual.store_incarnation, expected.store_incarnation);
            assert_eq!(actual.prepared_id, ready.prepared_id);
            assert_eq!(actual.bound_id, ready.bound_id);
            assert!(actual.terminal_id.is_none() && actual.response.is_none());
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
                let prepared = state
                    .view
                    .original_repairs
                    .get(&OriginalRepairKey::of(&material))
                    .unwrap()
                    .clone();
                PreparedOriginalRepairRef {
                    root: store.root.clone(),
                    prepared,
                }
            };
            assert!(
                store
                    .bind_original_repair_content(&reference, b"changed body")
                    .is_err()
            );
            assert_eq!(
                store.original_repair_test_snapshot(&material).unwrap(),
                Some(actual.clone())
            );
            if phase == Phase::Bound {
                let command = &material.original.command;
                let (manifest, bytes) = store
                    .read_object_version(command.object_id.as_str(), command.version.as_str())
                    .unwrap();
                assert_eq!(bytes, BODY);
                assert_eq!(manifest.content_hash, command.content_hash);
                assert_eq!(manifest.size_bytes, BODY.len() as u64);
                assert_eq!(
                    store
                        .bind_original_repair_content(&reference, BODY)
                        .unwrap()
                        .bound_id(),
                    actual.bound_id.unwrap()
                );
                assert_eq!(
                    store.original_repair_test_snapshot(&material).unwrap(),
                    Some(actual)
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn original_storage_phases_survive_owned_process_kill() {
        process_storage::arm(process_storage::Phase::Prepared);
        process_storage::arm(process_storage::Phase::Bound);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "owned storage-only helper; launched twice by the parent SIGKILL case"]
    fn original_storage_phase_process_child() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        process_storage::child()
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
