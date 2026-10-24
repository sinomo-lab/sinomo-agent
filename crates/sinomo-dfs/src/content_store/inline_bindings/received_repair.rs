//! Receiver-observed invocation/content evidence. Experimental owner admission
//! is available only to fresh native fixtures; this is NOT original custody,
//! terminal outcome, current-copy authority or a production format activation.
use super::*;
use crate::DfsRepairReplicaCommand;
use sinomo_core::node::NodeId;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceivedRepairKey {
    requester: String,
    request_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum CaptureProvenance {
    DecodedInvocationUnprovenHistoricalOrigin,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceivedRepairPrepared {
    key: ReceivedRepairKey,
    // Qualified by physical incarnation, key and seal. Compaction preserves
    // this phase number but can reuse physical sequence numbers for other keys.
    logical_id: u64,
    receiver: String,
    store_incarnation: String,
    provenance: CaptureProvenance,
    // Exact decoded command with bytes extracted into the frame payload.
    head: DfsRepairReplicaCommand,
    body_hash: String,
    body_size: u64,
    seal: String,
}

impl ReceivedRepairPrepared {
    fn seal(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Seal<'a> {
            domain: &'static str,
            key: &'a ReceivedRepairKey,
            logical_id: u64,
            receiver: &'a str,
            store_incarnation: &'a str,
            provenance: &'a CaptureProvenance,
            head: &'a DfsRepairReplicaCommand,
            body_hash: &'a str,
            body_size: u64,
        }
        Ok(blake3::hash(&encode_inline(&Seal {
            domain: "sinomo.dfs.received-repair.decoded-invocation.v1",
            key: &self.key,
            logical_id: self.logical_id,
            receiver: &self.receiver,
            store_incarnation: &self.store_incarnation,
            provenance: &self.provenance,
            head: &self.head,
            body_hash: &self.body_hash,
            body_size: self.body_size,
        })?)
        .to_hex()
        .to_string())
    }

    fn validate(&self, incarnation: &str) -> Result<()> {
        validate_key(self.head.object_id.as_str(), self.head.version.as_str())?;
        hash_hex(&self.body_hash)?;
        if self.logical_id == 0
            || self.key.request_id.is_empty()
            || self.key.requester.is_empty()
            || self.receiver.is_empty()
            || self.key.request_id != self.head.request_id
            || !self.head.bytes.is_empty()
            || self.body_hash != self.head.content_hash
            || self.body_size > INLINE_BLOB_SMALL_LIMIT_BYTES
            || self.body_size == 0 && self.body_hash != intrinsic_empty_content_hash()
            || self.store_incarnation != incarnation
            || self.seal != self.seal()?
        {
            return Err(inline_error(
                "received Repair capture identity/material is not exact",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceivedRepairBinding {
    key: ReceivedRepairKey,
    prepared_id: u64,
    prepared_seal: String,
    logical_id: u64,
    store_incarnation: String,
    pub(super) manifest: ContentManifest,
}

/// No Deserialize/Serialize or public constructor: only the synchronized owner
/// can mint this value. It does not retain a resident store handle across reopen.
#[derive(Clone, Debug)]
pub(crate) struct PreparedReceivedRepairRef {
    root: PathBuf,
    prepared: ReceivedRepairPrepared,
}

/// Evidence of that actual local content substep, never DfsBlobTransferResult.
#[derive(Debug)]
pub(crate) struct PreparedRepairContentWitness {
    prepared: PreparedReceivedRepairRef,
    binding: ReceivedRepairBinding,
}

impl PreparedRepairContentWitness {
    pub(crate) fn manifest(&self) -> &ContentManifest {
        &self.binding.manifest
    }
    pub(crate) fn captured_request_id(&self) -> &str {
        &self.prepared.prepared.key.request_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReceivedRepairFixtureIdentity {
    root: PathBuf,
    store_incarnation: String,
    format: u32,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    created: std::time::SystemTime,
}

impl ReceivedRepairFixtureIdentity {
    pub(crate) fn is_at_root(&self, root: &Path) -> bool { self.root == root }
    pub(crate) fn check_original_physical_identity(&self) -> Result<()> {
        if self.format != ORIGINAL_REPAIR_FIXTURE_VERSION { return Err(inline_error("expected original Repair identity is not private4")); }
        self.check_directory()
    }

    /// Compare an actual writer's physical identity with the expectation
    /// retained by the native DATA owner. This performs no recovery or sync.
    pub(crate) fn check_original_store_identity(&self, store: &LocalContentStore) -> Result<()> {
        self.check_original_physical_identity()?;
        if store.inline_store.owner.expected_format != ORIGINAL_REPAIR_FIXTURE_VERSION
            || store.root != self.root
            || store.inline_store.owner.received_repair_identity.lock().map_err(|_| inline_error("fixture identity mutex poisoned"))?.as_ref() != Some(self)
        { return Err(inline_error("original Repair content store differs from actual native owner expectation")); }
        Ok(())
    }

    pub(crate) fn check_original_store(&self, store: &LocalContentStore, store_incarnation: &str) -> Result<()> {
        self.check_original_store_identity(store)?;
        if store_incarnation != self.store_incarnation { return Err(inline_error("original Repair writer sentinel differs from actual native content incarnation")); }
        Ok(())
    }
    fn check_directory(&self) -> Result<()> {
        let metadata =
            fs::symlink_metadata(&self.root).map_err(|source| ContentStoreError::Io {
                path: self.root.clone(),
                source,
            })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(inline_error("fixture root is not the original directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.dev() != self.device || metadata.ino() != self.inode {
                return Err(inline_error("fixture directory was physically replaced"));
            }
        }
        #[cfg(not(unix))]
        if metadata.created().ok() != Some(self.created) {
            return Err(inline_error(
                "fixture directory identity cannot be certified",
            ));
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
impl LocalContentStore {
    pub(crate) fn new_received_repair_fixture(
        root: impl Into<PathBuf>,
    ) -> Result<(Self, ReceivedRepairFixtureIdentity)> {
        Self::new_received_repair_fixture_for_format(root.into(), RECEIVED_REPAIR_FIXTURE_VERSION)
    }

    pub(crate) fn new_original_repair_fixture(root: impl Into<PathBuf>) -> Result<(Self, ReceivedRepairFixtureIdentity)> {
        Self::new_received_repair_fixture_for_format(root.into(), ORIGINAL_REPAIR_FIXTURE_VERSION)
    }

    fn new_received_repair_fixture_for_format(root: PathBuf, format: u32) -> Result<(Self, ReceivedRepairFixtureIdentity)> {
        // Atomic fresh creation: never convert/adopt even an existing empty root.
        fs::create_dir(&root).map_err(|source| ContentStoreError::Io {
            path: root.clone(),
            source,
        })?;
        let metadata = fs::symlink_metadata(&root).map_err(|source| ContentStoreError::Io {
            path: root.clone(),
            source,
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(inline_error("fresh fixture root changed before admission"));
        }
        let mut store = Self::received_repair_fixture_handle(&root, format);
        if store.inline_store.identity_error.is_some()
            || store.inline_store.owner.expected_format != format
            || store
                .inline_store
                .owner
                .received_repair_identity
                .lock()
                .map_err(|_| inline_error("fixture identity mutex poisoned"))?
                .is_some()
        {
            return Err(inline_error(
                "fresh fixture cannot replace an existing live content owner",
            ));
        }
        sync_dir(
            store
                .root
                .parent()
                .ok_or_else(|| inline_error("fixture root has no parent"))?,
        )?;
        let _owner = ContentMutationGuard::acquire(&store.root, true)?;
        create_dir_all(store.root.join("blobs"))?;
        create_dir_all(store.root.join("objects"))?;
        create_dir_all(store.root.join("staging"))?;
        store.initialize_content_reservation_writer_state()?;
        store.initialize_bindings_for_explicit_owner(format)?;
        initialize_file_reclamation_lock(&store.root)?;
        let sentinel = store
            .inline_sentinel_unlocked()?
            .ok_or_else(|| inline_error("fixture has no physical marker"))?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        let identity = ReceivedRepairFixtureIdentity {
            format,
            root: store.root.clone(),
            store_incarnation: sentinel.store_incarnation,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(not(unix))]
            created: metadata.created().map_err(|source| ContentStoreError::Io {
                path: store.root.clone(),
                source,
            })?,
        };
        identity.check_directory()?;
        *store
            .inline_store
            .owner
            .received_repair_identity
            .lock()
            .map_err(|_| inline_error("fixture identity mutex poisoned"))? = Some(identity.clone());
        Ok((store, identity))
    }

    pub(crate) fn reopen_received_repair_fixture(
        identity: &ReceivedRepairFixtureIdentity,
    ) -> Result<Self> {
        identity.check_directory()?;
        let mut store = Self::received_repair_fixture_handle(&identity.root, identity.format);
        if store.inline_store.identity_error.is_some() {
            return Err(inline_error(
                "fixture reopen has a conflicting live content owner",
            ));
        }
        {
            let mut accepted = store
                .inline_store
                .owner
                .received_repair_identity
                .lock()
                .map_err(|_| inline_error("fixture identity mutex poisoned"))?;
            if accepted.as_ref().is_some_and(|old| old != identity) {
                return Err(inline_error(
                    "fixture reopen cannot replace an accepted physical owner",
                ));
            }
            *accepted = Some(identity.clone());
        }
        let _owner = store.content_mutation_guard()?;
        let sentinel = store
            .inline_sentinel_unlocked()?
            .ok_or_else(|| inline_error("fixture marker is absent"))?;
        if sentinel.store_incarnation != identity.store_incarnation {
            return Err(inline_error("fixture physical incarnation changed"));
        }
        // Existing marker/catalog only: never initialize or allocate identity.
        read_catalog_for_format(
            &store.root,
            &identity.store_incarnation,
            identity.format,
        )?;
        store.content_reservation_writer_initialized = true;
        let reservation = store.lock_content_reservations_for_write()?;
        let mut state = store
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &store.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        drop(state);
        drop(reservation);
        Ok(store)
    }

    fn received_repair_fixture_handle(root: &Path, format: u32) -> Self {
        let lease = InlineStoreLease::new_for_format(root, format);
        Self {
            root: lease.root().to_owned(),
            inline_store: lease,
            content_reservation_writer_initialized: false,
            quota: None,
            quota_stats_initialized: Arc::new(AtomicBool::new(false)),
            stream_manifest_cache: Arc::new(Mutex::new(StreamManifestCache::default())),
            #[cfg(test)]
            blob_stream_open_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            stream_manifest_validation_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl LocalContentStore {
    // Only the private fresh initializer may temporarily have no accepted
    // identity. No such handle escapes it; capture requires a bound identity.
    pub(super) fn validate_received_repair_fixture_physical_identity(
        &self,
        sentinel: Option<&ContentStoreReservationBatchSentinel>,
    ) -> Result<()> {
        if !matches!(self.inline_store.owner.expected_format, RECEIVED_REPAIR_FIXTURE_VERSION | ORIGINAL_REPAIR_FIXTURE_VERSION) {
            return Ok(());
        }
        let accepted = self
            .inline_store
            .owner
            .received_repair_identity
            .lock()
            .map_err(|_| inline_error("fixture identity mutex poisoned"))?;
        if let Some(identity) = accepted.as_ref() {
            identity.check_directory()?;
            if identity.root != self.root || identity.format != self.inline_store.owner.expected_format
                || sentinel.is_some_and(|s| s.store_incarnation != identity.store_incarnation)
            {
                return Err(inline_error(
                    "fixture physical incarnation differs from accepted owner",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn require_received_repair_fixture_identity(&self) -> Result<()> {
        if self
            .inline_store
            .owner
            .received_repair_identity
            .lock()
            .map_err(|_| inline_error("fixture identity mutex poisoned"))?
            .is_none()
        {
            return Err(inline_error(
                "received Repair requires an accepted physical fixture owner",
            ));
        }
        self.validate_received_repair_fixture_physical_identity(None)
    }

    pub(crate) fn prepare_received_repair(
        &self,
        requester: &NodeId,
        receiver: &NodeId,
        command: &DfsRepairReplicaCommand,
    ) -> Result<Option<PreparedReceivedRepairRef>> {
        if self.inline_store.owner.expected_format != RECEIVED_REPAIR_FIXTURE_VERSION {
            return Ok(None);
        }
        self.require_received_repair_fixture_identity()?;
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        let key = ReceivedRepairKey {
            requester: requester.to_string(),
            request_id: command.request_id.clone(),
        };
        let existing = {
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("resident binding mutex poisoned"))?;
            ensure_writer(
                &self.root,
                &reservation.sentinel.store_incarnation,
                &mut state,
            )?;
            state.view.received_repairs.get(&key).cloned()
        };
        // A known capture cannot escape immutability through size fallback.
        if let Some(prior) = &existing {
            if command.bytes.len() as u64 != prior.body_size {
                return Err(inline_error(
                    "captured Repair retry changed its material length",
                ));
            }
        } else if command.bytes.len() as u64 > INLINE_BLOB_SMALL_LIMIT_BYTES
            || command.request_id.is_empty()
        {
            return Ok(None);
        }
        ensure_bytes_match_hash(&command.content_hash, &command.bytes)?;
        let mut head = command.clone();
        head.bytes.clear();
        if let Some(prior) = existing {
            if prior.head != head
                || prior.receiver != receiver.to_string()
                || prior.body_hash != content_hash(&command.bytes)
            {
                return Err(inline_error(
                    "captured Repair retry changed its exact decoded invocation",
                ));
            }
            return Ok(Some(PreparedReceivedRepairRef {
                root: self.root.clone(),
                prepared: prior,
            }));
        }
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        let mut prepared = ReceivedRepairPrepared {
            key,
            logical_id: state
                .view
                .sequence
                .checked_add(1)
                .ok_or_else(|| inline_error("capture identity exhausted"))?,
            receiver: receiver.to_string(),
            store_incarnation: reservation.sentinel.store_incarnation.clone(),
            provenance: CaptureProvenance::DecodedInvocationUnprovenHistoricalOrigin,
            head,
            body_hash: command.content_hash.clone(),
            body_size: command.bytes.len() as u64,
            seal: String::new(),
        };
        prepared.seal = prepared.seal()?;
        let operation = InlineOperation::ReceivedRepairPrepared(prepared.clone());
        let prospective = InlineRecord {
            format_version: RECEIVED_REPAIR_FIXTURE_VERSION,
            store_incarnation: prepared.store_incarnation.clone(),
            generation: state
                .catalog
                .as_ref()
                .expect("certified catalog")
                .generation,
            sequence: prepared.logical_id,
            operation: operation.clone(),
        };
        if encode_inline(&prospective)?.len() > INLINE_FRAME_HEADER_LIMIT as usize {
            return Ok(None);
        }
        // Admission is decided before mutation. The content owner excludes all
        // writers while quota reads temporarily release the resident mutex.
        drop(state);
        self.quota_stats_initialized.store(false, Ordering::Release);
        let existing_bytes = self.readable_blob_size(&command.content_hash)?.unwrap_or(0);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: (command.bytes.len() as u64).saturating_sub(existing_bytes),
            ..ContentStoreQuotaDelta::default()
        })?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        append_operation(
            &self.root,
            &reservation.sentinel,
            &mut state,
            operation,
            &command.bytes,
        )?;
        drop(state);
        if self.quota.is_some() {
            self.replace_stats_snapshot(&self.scan_stats()?)?;
        }
        Ok(Some(PreparedReceivedRepairRef {
            root: self.root.clone(),
            prepared,
        }))
    }

    pub(crate) fn store_prepared_received_repair(
        &self,
        reference: &PreparedReceivedRepairRef,
        bytes: &[u8],
    ) -> Result<PreparedRepairContentWitness> {
        let _owner = self.content_mutation_guard()?;
        self.check_received_repair_reference(reference, bytes)?;
        {
            let reservation = self.lock_content_reservations_for_write()?;
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("resident binding mutex poisoned"))?;
            ensure_writer(
                &self.root,
                &reservation.sentinel.store_incarnation,
                &mut state,
            )?;
            if let Some(binding) = state.view.received_bindings.get(&reference.prepared.key) {
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
                        "captured Repair binding is no longer the live exact version",
                    ));
                }
                return Ok(PreparedRepairContentWitness {
                    prepared: reference.clone(),
                    binding: binding.clone(),
                });
            }
        }
        self.try_write_inline_binding_with_prepared(
            reference.prepared.head.object_id.as_str(),
            reference.prepared.head.version.as_str(),
            bytes,
            Some(reference),
        )?
        .ok_or_else(|| {
            inline_error("admitted captured Repair cannot use generic content fallback")
        })?;
        let reservation = self.lock_content_reservations_for_write()?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        let binding = state
            .view
            .received_bindings
            .get(&reference.prepared.key)
            .cloned()
            .ok_or_else(|| inline_error("synchronized bound Repair record is absent"))?;
        validate_binding(
            &state.view,
            &binding,
            &reservation.sentinel.store_incarnation,
        )?;
        Ok(PreparedRepairContentWitness {
            prepared: reference.clone(),
            binding,
        })
    }

    pub(super) fn check_received_repair_reference(
        &self,
        reference: &PreparedReceivedRepairRef,
        bytes: &[u8],
    ) -> Result<()> {
        self.require_received_repair_fixture_identity()?;
        if self.inline_store.owner.expected_format != RECEIVED_REPAIR_FIXTURE_VERSION
            || reference.root != self.root
        {
            return Err(inline_error(
                "received Repair reference belongs to another explicit owner",
            ));
        }
        let reservation = self.lock_content_reservations_for_write()?;
        if reference.prepared.store_incarnation != reservation.sentinel.store_incarnation {
            return Err(inline_error(
                "received Repair reference names another physical incarnation",
            ));
        }
        ensure_bytes_match_hash(&reference.prepared.body_hash, bytes)?;
        if bytes.len() as u64 != reference.prepared.body_size {
            return Err(inline_error("received Repair body length changed"));
        }
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        if state.view.received_repairs.get(&reference.prepared.key) != Some(&reference.prepared) {
            return Err(inline_error(
                "received Repair has no exact synchronized Prepared",
            ));
        }
        Ok(())
    }
}

pub(super) fn binding_for_reference(
    reference: &PreparedReceivedRepairRef,
    manifest: ContentManifest,
    sequence: u64,
) -> ReceivedRepairBinding {
    ReceivedRepairBinding {
        key: reference.prepared.key.clone(),
        prepared_id: reference.prepared.logical_id,
        prepared_seal: reference.prepared.seal.clone(),
        logical_id: sequence,
        store_incarnation: reference.prepared.store_incarnation.clone(),
        manifest,
    }
}

fn validate_binding(
    view: &InlineBindingView,
    binding: &ReceivedRepairBinding,
    incarnation: &str,
) -> Result<()> {
    let prepared = view
        .received_repairs
        .get(&binding.key)
        .ok_or_else(|| inline_error("bound Repair has no Prepared"))?;
    if binding.logical_id == 0
        || binding.store_incarnation != incarnation
        || binding.prepared_id != prepared.logical_id
        || binding.prepared_seal != prepared.seal
        || binding.manifest.object_id != prepared.head.object_id.as_str()
        || binding.manifest.version != prepared.head.version.as_str()
        || binding.manifest.content_hash != prepared.body_hash
        || binding.manifest.size_bytes != prepared.body_size
        || !binding.manifest.extents.is_empty()
        || view
            .received_bindings
            .get(&binding.key)
            .is_some_and(|old| old != binding)
    {
        return Err(inline_error(
            "received Repair binding changed Prepared/material identity",
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
        InlineOperation::ReceivedRepairPrepared(p)
        | InlineOperation::RestoreReceivedRepairPrepared(p) => {
            p.validate(incarnation)?;
            if view
                .received_repairs
                .get(&p.key)
                .is_some_and(|old| old != p)
            {
                return Err(inline_error(
                    "received Repair capture changed under its local identity",
                ));
            }
            if matches!(operation, InlineOperation::ReceivedRepairPrepared(_)) {
                if p.body_hash != payload_hash || p.body_size != payload_bytes {
                    return Err(inline_error(
                        "received Repair Prepared payload is not exact",
                    ));
                }
            } else if payload_bytes != 0
                || (p.body_size != 0
                    && !view
                        .blobs
                        .get(&p.body_hash)
                        .is_some_and(|blob| blob.size_bytes == p.body_size))
            {
                return Err(inline_error(
                    "restored received Repair has no exact retained material",
                ));
            }
        }
        InlineOperation::ReceivedRepairBound {
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
        InlineOperation::RestoreReceivedRepairBound(binding) => {
            validate_binding(view, binding, incarnation)?;
            // Preserve historical binding provenance even after an ordinary
            // version deletion. This never recreates the live version row.
            if payload_bytes != 0 {
                return Err(inline_error(
                    "restored bound Repair carries unexpected payload",
                ));
            }
        }
        _ => return Err(inline_error("unexpected received Repair operation")),
    }
    Ok(())
}

pub(super) fn apply_prepared(
    view: &mut InlineBindingView,
    prepared: ReceivedRepairPrepared,
    location: Option<InlineBlobEntry>,
) {
    if !view.received_repairs.contains_key(&prepared.key) && prepared.body_size != 0 {
        change_root_count(&mut view.repair_hashes, prepared.body_hash.clone(), true);
    }
    if let Some(location) = location
        && prepared.body_size != 0
    {
        view.deleted_blobs.remove(&prepared.body_hash);
        view.blobs.insert(prepared.body_hash.clone(), location);
    }
    view.received_repairs.insert(prepared.key.clone(), prepared);
}

pub(super) fn apply_binding(view: &mut InlineBindingView, binding: ReceivedRepairBinding) {
    view.received_bindings.insert(binding.key.clone(), binding);
}

pub(super) fn fixture_operation(operation: &InlineOperation) -> bool {
    matches!(
        operation,
        InlineOperation::ReceivedRepairPrepared(_)
            | InlineOperation::RestoreReceivedRepairPrepared(_)
            | InlineOperation::ReceivedRepairBound { .. }
            | InlineOperation::RestoreReceivedRepairBound(_)
    )
}

pub(super) fn validate_phase_sequence(operation: &InlineOperation, sequence: u64) -> Result<()> {
    match operation {
        InlineOperation::ReceivedRepairPrepared(p) if p.logical_id != sequence => Err(
            inline_error("Prepared identity was not assigned by this actual append"),
        ),
        InlineOperation::ReceivedRepairBound { binding, .. } if binding.logical_id != sequence => {
            Err(inline_error(
                "bound identity was not assigned by this actual append",
            ))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ReceivedRepairTestSnapshot {
    pub(crate) command: DfsRepairReplicaCommand,
    pub(crate) prepared_id: u64,
    pub(crate) prepared_seal: String,
    pub(crate) bound_id: Option<u64>,
    pub(crate) store_incarnation: String,
}

#[cfg(test)]
impl LocalContentStore {
    pub(crate) fn received_repair_test_snapshot(
        &self,
        requester: &NodeId,
        request_id: &str,
    ) -> Result<Option<ReceivedRepairTestSnapshot>> {
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.root,
            &reservation.sentinel.store_incarnation,
            &mut state,
        )?;
        let key = ReceivedRepairKey {
            requester: requester.to_string(),
            request_id: request_id.into(),
        };
        let Some(prepared) = state.view.received_repairs.get(&key).cloned() else {
            return Ok(None);
        };
        let bound_id = state
            .view
            .received_bindings
            .get(&key)
            .map(|bound| bound.logical_id);
        drop(state);
        let mut command = prepared.head;
        command.bytes = self.read_blob(&prepared.body_hash)?;
        Ok(Some(ReceivedRepairTestSnapshot {
            command,
            prepared_id: prepared.logical_id,
            prepared_seal: prepared.seal,
            bound_id,
            store_incarnation: prepared.store_incarnation,
        }))
    }

    pub(crate) fn arm_received_repair_after_prepared_test_cut(&self) {
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((self.root.clone(), InlineFault::ReceiverAfterPrepared))
        });
    }

    pub(crate) fn received_repair_after_prepared_test_cut(&self) -> Result<()> {
        if take_inline_fault(&self.root, InlineFault::ReceiverAfterPrepared) {
            return Err(inline_error(
                "injected receiver cut after Prepared before content binding",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn command(request: &str, object: &str, bytes: &[u8]) -> DfsRepairReplicaCommand {
        DfsRepairReplicaCommand {
            request_id: request.into(),
            background_repair: true,
            intent_id: format!("intent-{request}"),
            file_system_id: crate::DfsFileSystemId::from_legacy_named("sifs").unwrap(),
            object_id: crate::DfsObjectId::from_legacy_named(object).unwrap(),
            expected_base_version: Some(crate::DfsVersion::from_legacy_named("base").unwrap()),
            version: crate::DfsVersion::from_legacy_named("target").unwrap(),
            content_hash: content_hash(bytes),
            bytes: bytes.to_vec(),
            object_metadata: None,
            authority_shard: None,
        }
    }

    fn peers() -> (NodeId, NodeId) {
        (
            NodeId::from_legacy_named("received-repair-requester").unwrap(),
            NodeId::from_legacy_named("received-repair-receiver").unwrap(),
        )
    }

    fn fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        LocalContentStore,
        ReceivedRepairFixtureIdentity,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let (store, identity) =
            LocalContentStore::new_received_repair_fixture(root.join("content")).unwrap();
        (root, store, identity)
    }

    #[test]
    fn received_repair_prepared_only_reopen_recovers_exact_captured_invocation() {
        let (_root, store, identity) = fixture("received-repair-prepared-reopen");
        let (requester, receiver) = peers();
        let input = command("original-request", "object", b"complete captured body");
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        let before = store
            .received_repair_test_snapshot(&requester, &input.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(before.command, input);
        assert_eq!(before.bound_id, None);
        assert!(
            store
                .read_manifest(input.object_id.as_str(), input.version.as_str())
                .is_err()
        );
        drop(store);
        let reopened = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            reopened
                .received_repair_test_snapshot(&requester, &input.request_id)
                .unwrap()
                .unwrap(),
            before
        );
        let retry = reopened
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        assert_eq!(retry.prepared, prepared.prepared);
        let witness = reopened
            .store_prepared_received_repair(&retry, &input.bytes)
            .unwrap();
        assert_eq!(witness.captured_request_id(), input.request_id);
        assert_eq!(witness.manifest().content_hash, input.content_hash);
    }

    #[test]
    fn received_repair_bound_reopen_and_exact_retry_preserve_phase_and_material() {
        let (_root, store, identity) = fixture("received-repair-bound-reopen");
        let (requester, receiver) = peers();
        for bytes in [b"owned decoded bytes".as_slice(), b"".as_slice()] {
            let input = command(
                &format!("request-{}", bytes.len()),
                &format!("object-{}", bytes.len()),
                bytes,
            );
            let prepared = store
                .prepare_received_repair(&requester, &receiver, &input)
                .unwrap()
                .unwrap();
            store
                .store_prepared_received_repair(&prepared, bytes)
                .unwrap();
        }
        let before = store.inline_binding_view().unwrap().unwrap();
        drop(store);
        let reopened = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        for bytes in [b"owned decoded bytes".as_slice(), b"".as_slice()] {
            let input = command(
                &format!("request-{}", bytes.len()),
                &format!("object-{}", bytes.len()),
                bytes,
            );
            let prepared = reopened
                .prepare_received_repair(&requester, &receiver, &input)
                .unwrap()
                .unwrap();
            let witness = reopened
                .store_prepared_received_repair(&prepared, bytes)
                .unwrap();
            assert_eq!(
                witness.binding,
                before.received_bindings[&prepared.prepared.key]
            );
            assert_eq!(
                reopened
                    .received_repair_test_snapshot(&requester, &input.request_id)
                    .unwrap()
                    .unwrap()
                    .command,
                input
            );
            assert_eq!(
                reopened
                    .read_object_version(input.object_id.as_str(), input.version.as_str())
                    .unwrap()
                    .1,
                bytes
            );
        }
        assert_eq!(
            reopened.inline_binding_view().unwrap().unwrap().sequence,
            before.sequence
        );
    }

    #[test]
    fn received_repair_matching_generic_binding_requires_actual_bound_append() {
        let (_root, store, _identity) = fixture("received-repair-generic-before-bound");
        let (requester, receiver) = peers();
        let input = command("same-existing-content", "object", b"already held bytes");
        store
            .put_object_version(
                input.object_id.as_str(),
                input.version.as_str(),
                &input.bytes,
            )
            .unwrap();
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        let before = store.inline_binding_view().unwrap().unwrap();
        assert!(before.received_bindings.is_empty());
        let witness = store
            .store_prepared_received_repair(&prepared, &input.bytes)
            .unwrap();
        assert_eq!(witness.binding.logical_id, before.sequence + 1);
        assert_eq!(
            store.inline_binding_view().unwrap().unwrap().sequence,
            before.sequence + 1
        );
    }

    #[test]
    fn received_repair_changed_known_key_cannot_escape_through_oversize_fallback() {
        let (_root, store, _identity) = fixture("received-repair-key-immutability");
        let (requester, receiver) = peers();
        let input = command("stable-key", "object", b"captured bytes");
        store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        let before = store.inline_binding_view().unwrap().unwrap().sequence;
        let mut changed = input.clone();
        changed.intent_id.push_str("changed");
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &changed)
                .is_err()
        );
        changed = command("stable-key", "object", b"changed bytes!");
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &changed)
                .is_err()
        );
        changed = command(
            "stable-key",
            "object",
            &vec![3; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1],
        );
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &changed)
                .is_err()
        );
        assert!(
            store
                .prepare_received_repair(
                    &requester,
                    &NodeId::from_legacy_named("different-receiver").unwrap(),
                    &input
                )
                .is_err()
        );
        assert_eq!(
            store.inline_binding_view().unwrap().unwrap().sequence,
            before
        );
        assert_eq!(
            store
                .received_repair_test_snapshot(&requester, &input.request_id)
                .unwrap()
                .unwrap()
                .command,
            input
        );
    }

    #[test]
    fn received_repair_new_unsupported_input_falls_back_before_any_capture() {
        let (_root, store, identity) = fixture("received-repair-unsupported-input");
        let (requester, receiver) = peers();
        let large = command(
            "new-large",
            "object",
            &vec![8; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1],
        );
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &large)
                .unwrap()
                .is_none()
        );
        let mut large_header = command("new-header", "object", b"small");
        large_header.intent_id = "x".repeat(INLINE_FRAME_HEADER_LIMIT as usize + 1);
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &large_header)
                .unwrap()
                .is_none()
        );
        let mut invalid = command("wrong-hash", "object", b"body");
        invalid.content_hash = content_hash(b"other");
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &invalid)
                .is_err()
        );
        assert_eq!(store.inline_binding_view().unwrap().unwrap().sequence, 0);
        // Unsupported material still uses ordinary durable reservation semantics.
        store
            .ensure_content_reservation(
                large.object_id.as_str(),
                large.version.as_str(),
                &large.content_hash,
            )
            .unwrap();
        drop(store);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        let roots = store.content_reservation_roots().unwrap();
        assert!(
            roots
                .reservations
                .iter()
                .any(|root| root.identity.object_id == large.object_id.as_str()
                    && root.identity.version == large.version.as_str()
                    && root.identity.content_hash == large.content_hash)
        );
        assert!(
            store
                .received_repair_test_snapshot(&requester, &large.request_id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn received_repair_quota_rejection_retains_prepared_material_without_binding_fallback() {
        let (_root, store, identity) = fixture("received-repair-quota-fences");
        let (requester, receiver) = peers();
        let input = command("quota-owned-body", "object", b"body above logical quota");
        let store = store.with_quota(ContentStoreQuota {
            max_blob_bytes: Some(1),
            ..ContentStoreQuota::default()
        });
        assert!(matches!(
            store.prepare_received_repair(&requester, &receiver, &input),
            Err(ContentStoreError::QuotaExceeded { .. })
        ));
        assert_eq!(store.inline_binding_view().unwrap().unwrap().sequence, 0);
        drop(store);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity)
            .unwrap()
            .with_quota(ContentStoreQuota {
                max_logical_object_bytes: Some(1),
                ..ContentStoreQuota::default()
            });
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        assert!(matches!(
            store.store_prepared_received_repair(&prepared, &input.bytes),
            Err(ContentStoreError::QuotaExceeded { .. })
        ));
        let snapshot = store
            .received_repair_test_snapshot(&requester, &input.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.command, input);
        assert_eq!(snapshot.bound_id, None);
        assert!(
            store
                .content_reservation_roots()
                .unwrap()
                .content_hashes
                .contains(&input.content_hash)
        );
        assert!(
            store
                .read_manifest(input.object_id.as_str(), input.version.as_str())
                .is_err()
        );
        drop(store);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            store
                .received_repair_test_snapshot(&requester, &input.request_id)
                .unwrap()
                .unwrap(),
            snapshot
        );
        store
            .store_prepared_received_repair(&prepared, &input.bytes)
            .unwrap();
    }

    #[test]
    fn received_repair_prepared_payload_survives_ordinary_gc_without_a_manifest() {
        let (_root, store, identity) = fixture("received-repair-pending-gc");
        let (requester, receiver) = peers();
        let input = command(
            "pending-during-gc",
            "pending-object",
            b"pending material is a root",
        );
        store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        let before = store
            .received_repair_test_snapshot(&requester, &input.request_id)
            .unwrap()
            .unwrap();
        let orphan = store
            .put_object_version_blob_only("ordinary-orphan", "v1", b"ordinary unrooted control")
            .unwrap();
        {
            let _owner = store.content_mutation_guard().unwrap();
            let _reservation = store.lock_content_reservations_for_write().unwrap();
            store
                .inline_release_reservations_unlocked(&BTreeSet::from([
                    ContentStoreReservationIdentity {
                        object_id: orphan.object_id.clone(),
                        version: orphan.version.clone(),
                        content_hash: orphan.content_hash.clone(),
                    },
                ]))
                .unwrap();
        }
        let mut deleted = 0;
        // As in maintained content-GC unit controls, this is an explicitly
        // supplied complete fixture catalog observation, not a namespace or
        // authority certifier supplied by the new witness.
        for (generation, observed) in [
            ("fixture-empty-catalog-1", u64::MAX - 1),
            ("fixture-empty-catalog-2", u64::MAX),
        ] {
            let report = store
                .collect_garbage(
                    ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                        .with_catalog_observation(generation, observed),
                )
                .unwrap();
            assert_eq!(report.blocked_by_scrub_issue_count, 0);
            deleted += report.deleted_orphan_count;
        }
        assert_eq!(
            deleted, 1,
            "ordinary unrooted control proves collection actually ran"
        );
        assert!(!store.has_blob(&orphan.content_hash).unwrap());
        assert_eq!(store.read_blob(&input.content_hash).unwrap(), input.bytes);
        drop(store);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        assert_eq!(
            store
                .received_repair_test_snapshot(&requester, &input.request_id)
                .unwrap()
                .unwrap(),
            before
        );
        assert!(
            store
                .read_manifest(input.object_id.as_str(), input.version.as_str())
                .is_err()
        );
    }

    #[test]
    fn received_repair_lost_prepared_completion_requires_real_catalog_recertification() {
        let (_root, store, identity) = fixture("received-repair-lost-prepared");
        let (requester, receiver) = peers();
        let input = command(
            "lost-prepared-reply",
            "object",
            b"durable before lost completion",
        );
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((store.root.clone(), InlineFault::LostAppendCompletion))
        });
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &input)
                .unwrap_err()
                .to_string()
                .contains("lost append completion")
        );
        let segment = segment_path(&store.root, 1, 0);
        let persisted = fs::read(&segment).unwrap();
        drop(store);
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((identity.root.clone(), InlineFault::CatalogCertification))
        });
        assert!(LocalContentStore::reopen_received_repair_fixture(&identity).is_err());
        assert_eq!(fs::read(&segment).unwrap(), persisted);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        let snapshot = store
            .received_repair_test_snapshot(&requester, &input.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.command, input);
        assert_eq!(snapshot.bound_id, None);
        let before = store.inline_binding_view().unwrap().unwrap().sequence;
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        assert_eq!(
            store.inline_binding_view().unwrap().unwrap().sequence,
            before
        );
        store
            .store_prepared_received_repair(&prepared, &input.bytes)
            .unwrap();
    }

    #[test]
    fn received_repair_lost_bound_completion_replays_only_exact_actual_binding() {
        let (_root, store, identity) = fixture("received-repair-lost-bound");
        let (requester, receiver) = peers();
        let input = command(
            "lost-content-reply",
            "object",
            b"actual synchronized content effect",
        );
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((store.root.clone(), InlineFault::LostAppendCompletion))
        });
        assert!(
            store
                .store_prepared_received_repair(&prepared, &input.bytes)
                .unwrap_err()
                .to_string()
                .contains("lost append completion")
        );
        drop(store);
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((identity.root.clone(), InlineFault::CatalogCertification))
        });
        assert!(LocalContentStore::reopen_received_repair_fixture(&identity).is_err());
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        let before = store.inline_binding_view().unwrap().unwrap().sequence;
        let witness = store
            .store_prepared_received_repair(&prepared, &input.bytes)
            .unwrap();
        assert_eq!(witness.binding.logical_id, before);
        assert_eq!(
            store.inline_binding_view().unwrap().unwrap().sequence,
            before
        );
        assert_eq!(
            store
                .read_object_version(input.object_id.as_str(), input.version.as_str())
                .unwrap()
                .1,
            input.bytes
        );
    }

    #[test]
    fn received_repair_partial_tail_cannot_invent_bound_completion() {
        let (_root, store, identity) = fixture("received-repair-partial-bound-tail");
        let (requester, receiver) = peers();
        let input = command("torn-bound-record", "object", b"prepared body survives");
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        let segment = segment_path(&store.root, 1, 0);
        let prepared_end = fs::metadata(&segment).unwrap().len();
        store
            .store_prepared_received_repair(&prepared, &input.bytes)
            .unwrap();
        drop(store);
        // Cut the actual persisted bound frame, not a synthetic serialized record.
        let file = fs::OpenOptions::new().write(true).open(&segment).unwrap();
        file.set_len(prepared_end + 3).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let store = LocalContentStore::reopen_received_repair_fixture(&identity).unwrap();
        let snapshot = store
            .received_repair_test_snapshot(&requester, &input.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.command, input);
        assert_eq!(snapshot.bound_id, None);
        assert!(
            store
                .read_manifest(input.object_id.as_str(), input.version.as_str())
                .is_err()
        );
        store
            .store_prepared_received_repair(&prepared, &input.bytes)
            .unwrap();
    }

    #[test]
    fn received_repair_foreign_and_replaced_physical_owners_reject_capabilities() {
        let (root, store, identity) = fixture("received-repair-physical-fences");
        let (_foreign_root, foreign, _) = fixture("received-repair-foreign-owner");
        let (requester, receiver) = peers();
        let input = command("physical-owner", "object", b"retained body");
        let prepared = store
            .prepare_received_repair(&requester, &receiver, &input)
            .unwrap()
            .unwrap();
        assert!(
            foreign
                .store_prepared_received_repair(&prepared, &input.bytes)
                .is_err()
        );
        fs::rename(&identity.root, root.join("original-content")).unwrap();
        // A live owner prevents a fresh fixture from adopting the replacement.
        assert!(LocalContentStore::new_received_repair_fixture(&identity.root).is_err());
        assert!(
            store
                .prepare_received_repair(&requester, &receiver, &input)
                .is_err()
        );
        assert!(
            store
                .store_prepared_received_repair(&prepared, &input.bytes)
                .is_err()
        );
        assert!(LocalContentStore::reopen_received_repair_fixture(&identity).is_err());
        drop(store);
        fs::remove_dir(&identity.root).unwrap();
        let (replacement, replacement_identity) =
            LocalContentStore::new_received_repair_fixture(&identity.root).unwrap();
        assert_ne!(
            replacement_identity.store_incarnation,
            identity.store_incarnation
        );
        assert!(
            replacement
                .store_prepared_received_repair(&prepared, &input.bytes)
                .is_err()
        );
        assert!(LocalContentStore::reopen_received_repair_fixture(&identity).is_err());
        assert_eq!(
            replacement.inline_binding_view().unwrap().unwrap().sequence,
            0
        );
    }

    #[test]
    fn received_repair_fixture_admission_preserves_default_v1_and_foreign_roots() {
        let (root, store, identity) = fixture("received-repair-admission");
        let before = fs::read(catalog_path(&store.root)).unwrap();
        assert!(LocalContentStore::new_received_repair_fixture(&store.root).is_err());
        drop(store);
        assert!(LocalContentStore::new(&identity.root).is_err());
        assert_eq!(fs::read(catalog_path(&identity.root)).unwrap(), before);
        let existing = root.join("existing-empty");
        fs::create_dir(&existing).unwrap();
        assert!(LocalContentStore::new_received_repair_fixture(&existing).is_err());
        let foreign = root.join("foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("owned"), b"do not adopt").unwrap();
        assert!(LocalContentStore::new_received_repair_fixture(&foreign).is_err());
        assert_eq!(fs::read(foreign.join("owned")).unwrap(), b"do not adopt");
        #[cfg(unix)]
        {
            let alias = root.join("alias");
            std::os::unix::fs::symlink(&identity.root, &alias).unwrap();
            assert!(LocalContentStore::new_received_repair_fixture(&alias).is_err());
        }
        let (_root, _owner, ordinary) =
            super::super::tests::active_store("received-repair-default-v1");
        let (requester, receiver) = peers();
        let input = command("v1-no-capture", "object", b"legacy body");
        assert!(
            ordinary
                .prepare_received_repair(&requester, &receiver, &input)
                .unwrap()
                .is_none()
        );
        ordinary
            .put_object_version(
                input.object_id.as_str(),
                input.version.as_str(),
                &input.bytes,
            )
            .unwrap();
        assert!(
            ordinary
                .inline_binding_view()
                .unwrap()
                .unwrap()
                .received_repairs
                .is_empty()
        );
    }
}
