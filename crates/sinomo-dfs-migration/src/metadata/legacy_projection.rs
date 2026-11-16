//! Converter-only replay into the ordinary current candidate projector.
//! Retained author protocols never become runtime write entry points.
use super::{MetadataBackendError, Result};
use crate::legacy_wire as wire;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sinomo_dfs::*;
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

#[derive(Default)]
pub(super) struct LegacyProjection {
    seed: DfsMetadataTableSnapshotTables,
    current: Option<DfsMetadataCandidateState>,
    scope: Option<DfsMetadataShardId>,
    file_system_ids: BTreeSet<DfsFileSystemId>,
    operations: BTreeMap<String, Value>,
    transactions: BTreeMap<String, DfsNamespaceTransaction>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), Value>,
    deferred: Vec<Value>,
    conflicts: BTreeMap<String, Value>,
    archived_conflicts: BTreeMap<String, String>,
    source_prepare_sequences: BTreeMap<String, u64>,
    current_import_candidate_count: Option<usize>,
    pin_ids: BTreeMap<(DfsObjectId, DfsVersion, String), String>,
    request_digests: BTreeMap<(sinomo_core::node::NodeId, String), (String, String)>,
    retired_adoption_receipts:
        BTreeMap<(sinomo_core::node::NodeId, String), DfsNamespaceRequestReceipt>,
    source_capsules: BTreeMap<String, super::scope_mapping::RequiredNativeWork>,
}
fn invalid(error: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(error.to_string())
}
fn decode<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(wire::normalize_source_leaf(value)?).map_err(invalid)
}
fn field(value: &Value, name: &str) -> Result<Value> {
    value
        .get(name)
        .filter(|value| !value.is_null())
        .cloned()
        .ok_or_else(|| invalid(format!("retained {name} payload is missing")))
}
fn text(value: &Value, name: &str) -> Result<String> {
    field(value, name)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("retained {name} is not text")))
}
fn status(value: &Value) -> Result<DfsPreparedOperationStatus> {
    decode(field(value, "status")?)
}
fn timestamp(value: &Value, name: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(
        &text(value, name)?,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(invalid)
}
fn hash(value: &Value) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    serde_json::to_writer(&mut hash, value).map_err(invalid)?;
    Ok(format!("blake3:{}", hash.finalize().to_hex()))
}

impl LegacyProjection {
    pub(super) fn bind_current_import_policy(&mut self, candidate_count: usize) -> Result<()> {
        if candidate_count == 0
            || self
                .current_import_candidate_count
                .is_some_and(|current| current != candidate_count)
        {
            return Err(invalid(
                "current import-policy candidate context is empty or changed after binding",
            ));
        }
        self.current_import_candidate_count = Some(candidate_count);
        Ok(())
    }

    fn normalize_fs(&self, value: Value) -> Result<DfsFileSystem> {
        wire::normalize_file_system_with_current_import_defaults(
            value,
            self.current_import_candidate_count,
        )
    }
    pub(super) fn bind_scope(&mut self, shard: DfsMetadataShardId) -> Result<()> {
        if self.scope.as_ref().is_some_and(|current| current != &shard) {
            return Err(invalid("conversion scope changed after binding"));
        }
        self.scope = Some(shard);
        Ok(())
    }
    fn state(&mut self) -> Result<&mut DfsMetadataCandidateState> {
        if self.current.is_none() {
            let mut state = DfsMetadataCandidateState::new(std::mem::take(&mut self.seed))?;
            // Loose layouts may lack secondary tables. Use the normal object
            // index planner, preserving complete canonical source objects.
            let objects = state.snapshot().objects;
            for object in objects {
                state.apply(DfsPreparedMetadataEffect::PutObject { object })?;
            }
            self.current = Some(state);
        }
        Ok(self.current.as_mut().expect("initialized candidate"))
    }
    fn normalize_pin(&mut self, row: Value) -> Result<DfsMetadataVersionPinRecord> {
        let original = text(&row, "pin_id")?;
        let pin = wire::normalize_version_pin(row)?;
        self.pin_ids.insert(
            (pin.object_id.clone(), pin.version.clone(), original),
            pin.pin_id.clone(),
        );
        Ok(pin)
    }
    /// Classify an old adoption BEFORE current operation serde can require an
    /// author-only source offer. The complete old input remains converter-owned.
    pub(super) fn classify_source_request(
        &mut self,
        row: Value,
    ) -> Result<Option<DfsNamespaceRequestRecord>> {
        if !retired_source_request(&row) {
            return self.normalize_request(row).map(Some);
        }
        wire::verify_original_namespace_request(&row)?;
        let preflight = old_advisory_preflight(&row);
        self.remember_fs(&row)?;
        let key = source_capsule_key(
            "namespace-request",
            &row,
            &[
                "file_system_id",
                "authority_shard_id",
                "requester_node_id",
                "request_id",
            ],
        )?;
        if let Some(previous) = self.source_capsules.get(&key) {
            if previous.source_row.get("command_digest") != row.get("command_digest")
                || previous.source_row.get("operation_id") != row.get("operation_id")
            {
                return Err(invalid(
                    "old adoption reuses a source request identity for another command",
                ));
            }
        }
        match text(&row, "status")?.as_str() {
            "completed" => {
                let mut receipt = serde_json::Map::new();
                for field_name in [
                    "file_system_id",
                    "requester_node_id",
                    "request_id",
                    "operation_id",
                    "authority_shard_id",
                    "command_digest",
                    "result",
                    "created_at",
                ] {
                    receipt.insert(field_name.into(), field(&row, field_name)?);
                }
                receipt.insert(
                    "posix_coordination_result".into(),
                    if preflight {
                        Value::Null
                    } else {
                        row.get("posix_coordination_result")
                            .cloned()
                            .unwrap_or(Value::Null)
                    },
                );
                // A full source request has no receipt-retention timestamp. Its
                // actual terminal update is the conservative native fallback;
                // a real source compact receipt takes precedence at final fold.
                receipt.insert("retained_at".into(), field(&row, "updated_at")?);
                let receipt = wire::normalize_namespace_request_receipt(Value::Object(receipt))?;
                if receipt.retained_at < receipt.created_at {
                    return Err(invalid("old adoption terminal timestamp precedes creation"));
                }
                let receipt_key = (
                    receipt.requester_node_id.clone(),
                    receipt.request_id.clone(),
                );
                if let Some(old) = self.retired_adoption_receipts.get(&receipt_key) {
                    let mut immutable = old.clone();
                    immutable.retained_at = receipt.retained_at;
                    if immutable != receipt {
                        return Err(invalid("old adoption has conflicting terminal results"));
                    }
                }
                self.retired_adoption_receipts.insert(receipt_key, receipt);
                self.source_capsules.remove(&key);
                if preflight {
                    self.source_capsules.insert(format!("{key}:completed-preflight"), super::scope_mapping::RequiredNativeWork {
                        family:"namespace_requests.completed_preflight".into(),
                        reason:"exact retired advisory-lock preflight auxiliary outcome is source-only provenance, not a current executable response".into(),
                        source_row:row,
                    });
                }
            }
            "pending" => {
                let principal = wire::normalize_source_leaf(row.clone())?;
                let requester = decode(field(&principal, "requester_node_id")?)?;
                if self
                    .retired_adoption_receipts
                    .contains_key(&(requester, text(&row, "request_id")?))
                {
                    return Err(invalid("old adoption reopens an actual terminal request"));
                }
                if row.get("result").is_some_and(|value| !value.is_null())
                    || row
                        .get("posix_coordination_result")
                        .is_some_and(|value| !value.is_null())
                {
                    return Err(invalid("pending old adoption carries a terminal outcome"));
                }
                self.source_capsules.insert(key,super::scope_mapping::RequiredNativeWork {
                    family:if preflight {"namespace_requests.old_preflight"} else {"namespace_requests.old_adoption"}.into(),
                    reason:if preflight {"retired advisory-lock preflight retains complete original request and requires actual original-scope native resolution"} else {"original adoption has no live source offer; complete original-scope request requires new native custody/resolution"}.into(),
                    source_row:row,
                });
            }
            _ => return Err(invalid("old adoption request has an unknown source status")),
        }
        Ok(None)
    }
    pub(super) fn required_native_work(&self) -> Vec<super::scope_mapping::RequiredNativeWork> {
        self.source_capsules.values().cloned().collect()
    }
    fn normalize_source_job(&mut self, row: Value) -> Result<Option<DfsNamespaceJobRecord>> {
        // Running job frames are current DATA. Fresh source custody is minted
        // only when the ordinary current coordinator resumes that data; an old
        // job row never supplies the author-only source offer.
        let job = wire::normalize_namespace_job(row.clone())?;
        let key = source_capsule_key("namespace-job", &row, &["file_system_id", "job_id"])?;
        self.source_capsules.remove(&key);
        Ok(Some(job))
    }
    fn normalize_request(&mut self, row: Value) -> Result<DfsNamespaceRequestRecord> {
        let source_digest = text(&row, "command_digest")?;
        let mut request = wire::normalize_namespace_request(row)?;
        let command = DfsNamespaceCommand {
            request_id: request.request_id.clone(),
            file_system_id: request.file_system_id.clone(),
            requesting_node_id: request.requester_node_id.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: request.operation.clone(),
        };
        request.command_digest = namespace_command_digest(&command).map_err(invalid)?;
        self.request_digests.insert(
            (
                request.requester_node_id.clone(),
                request.request_id.clone(),
            ),
            (source_digest, request.command_digest.clone()),
        );
        Ok(request)
    }
    fn remember_fs(&mut self, value: &Value) -> Result<()> {
        let mut value = value.clone();
        wire::source_identity_fields(&mut value)?;
        if let Some(id) = value.get("file_system_id") {
            self.file_system_ids.insert(decode(id.clone())?);
        }
        Ok(())
    }
    pub(super) fn push_snapshot_row_from_source(
        &mut self,
        table: &str,
        mut row: Value,
        source: &super::source_document::SourceDocumentContext<'_>,
    ) -> Result<()> {
        match table.replace('-', "_").as_str() {
            "prepared_operations_v7" => return self.store_v7_from_source(row, source, ""),
            "namespace_prepares" => {
                return self.store_prepare_from_source(row, source, "", None, None);
            }
            "$ops" => {
                if row.get("type").and_then(Value::as_str)
                    == Some("put-namespace-prepare-journal-reference")
                {
                    self.deferred.push(row);
                    return Ok(());
                }
                return self.apply_table_op_from_source(row, source, "", &|_| {
                    Err(invalid("snapshot journal reference inventory is not ready"))
                });
            }
            _ => {}
        }
        let schema = match table.replace('-', "_").as_str() {
            "filesystems" | "file_systems" => Some("file-system"),
            "versions" => Some("version"),
            "namespace_transactions" => Some("transaction"),
            "namespace_requests" => Some("request"),
            "namespace_jobs" => Some("job"),
            "namespace_job_receipts" => Some("job-receipt"),
            "namespace_transaction_receipts" => Some("reference-outcome"),
            "intents" => Some("intent"),
            "cross_shard_pin_transfers" => Some("pin-transfer"),
            _ => None,
        };
        if table.replace('-', "_") == "namespace_requests" && retired_source_request(&row) {
            self.classify_source_request(row)?;
            return Ok(());
        }
        if let Some(schema) = schema {
            wire::normalize_original_module_schema_at(&mut row, schema, "", source, "", None)?;
        }
        self.push_snapshot_row(table, row)
    }

    pub(super) fn push_snapshot_row(&mut self, table: &str, row: Value) -> Result<()> {
        if table == "$ops" {
            if row.get("type").and_then(Value::as_str)
                == Some("put-namespace-prepare-journal-reference")
            {
                self.deferred.push(row);
                return Ok(());
            }
            return self.apply_table_op(row, &|_| {
                Err(invalid("snapshot journal reference inventory is not ready"))
            });
        }
        if self.current.is_some() {
            return Err(invalid("snapshot rows arrived after suffix replay began"));
        }
        self.remember_fs(&row)?;
        match table.replace('-', "_").as_str() {
            "filesystems" | "file_systems" => {
                let file_system = self.normalize_fs(row)?;
                self.seed.file_systems.push(file_system);
            }
            "objects" => self.seed.objects.push(wire::normalize_object(row)?),
            "filesystem_objects" | "file_system_objects" => {
                self.seed.file_system_objects.push(decode(row)?)
            }
            "paths" => self.seed.paths.push(decode(row)?),
            "inodes" => self.seed.inodes.push(decode(row)?),
            "dirfrags" => self.seed.dirfrags.push(decode(row)?),
            "dentries" => self.seed.dentries.push(decode(row)?),
            "hardlinks" => self.seed.hardlinks.push(decode(row)?),
            "compacted_tombstones" => self
                .seed
                .compacted_tombstones
                .push(wire::normalize_object(row)?),
            "versions" => self.seed.versions.push(wire::normalize_version(row)?),
            "version_pins" => {
                let pin = self.normalize_pin(row)?;
                self.seed.version_pins.push(pin);
            }
            "cross_shard_pin_transfers" => self
                .seed
                .cross_shard_pin_transfers
                .push(wire::normalize_pin_transfer(row)?),
            "receipts" => self.seed.receipts.push(decode(row)?),
            "receipt_withdrawals" => self.seed.receipt_withdrawals.push(decode(row)?),
            "intents" => self.seed.intents.push(wire::normalize_intent(row)?),
            "intent_receipts" => self
                .seed
                .intent_receipts
                .push(wire::normalize_intent_receipt(row)?),
            "intent_receipt_retention" => {
                self.seed.intent_receipt_retention =
                    decode(wire::normalize_intent_retention_wire(row)?)?
            }
            "advisory_lock_states" => self.seed.advisory_lock_states.push(decode(row)?),
            "namespace_transaction_receipts" => {
                self.seed.namespace_transaction_receipts.push(decode(row)?)
            }
            "namespace_transactions" => {
                let tx = wire::normalize_namespace_transaction(row)?;
                self.transactions
                    .insert(tx.transaction_id.clone(), tx.clone());
                self.seed
                    .namespace_transaction_receipts
                    .push(transaction_receipt(&tx, tx.created_at)?);
            }
            "namespace_jobs" => {
                if let Some(job) = self.normalize_source_job(row)? {
                    self.seed.namespace_jobs.push(job);
                }
            }
            "namespace_job_receipts" => self
                .seed
                .namespace_job_receipts
                .push(wire::normalize_namespace_job_receipt(row)?),
            "namespace_requests" => {
                if let Some(request) = self.classify_source_request(row)? {
                    self.seed.namespace_requests.push(request);
                }
            }
            "namespace_request_receipts" => self
                .seed
                .namespace_request_receipts
                .push(wire::normalize_namespace_request_receipt(row)?),
            "namespace_receipt_retention" => self.seed.namespace_receipt_retention = decode(row)?,
            "namespace_prepares" => {
                self.store_prepare(row, None)?;
            }
            "namespace_prepare_receipts" => {
                self.store_terminal_receipt(row)?;
            }
            "prepared_operations_v7" => {
                self.store_v7(row)?;
            }
            "prepared_operation_evictions" => self
                .seed
                .prepared_operation_evictions
                .push(wire::normalize_prepared_eviction(row)?),
            "visible_commits" => {
                let commit = row.get("commit").cloned().unwrap_or(row);
                self.store_visible_commit(commit)?;
            }
            "conflicts" => {
                self.archive_conflict(row)?;
            }
            other => return Err(invalid(format!("unknown retained snapshot table {other}"))),
        }
        Ok(())
    }
    pub(super) fn resolve_deferred(
        &mut self,
        journal_lookup: &dyn Fn(u64) -> Result<(Value, super::source_document::SourceRawDocument)>,
    ) -> Result<()> {
        let pending = std::mem::take(&mut self.deferred);
        for (index, op) in pending.iter().enumerate() {
            if let Err(error) = self.apply_table_op_inner(op.clone(), journal_lookup) {
                self.deferred.extend_from_slice(&pending[index..]);
                return Err(error);
            }
        }
        Ok(())
    }
    pub(super) fn apply_table_op(
        &mut self,
        op: Value,
        journal_lookup: &dyn Fn(u64) -> Result<(Value, super::source_document::SourceRawDocument)>,
    ) -> Result<()> {
        self.resolve_deferred(journal_lookup)?;
        self.apply_table_op_inner(op, journal_lookup)
    }
    pub(super) fn apply_table_op_from_source(
        &mut self,
        mut op: Value,
        source: &super::source_document::SourceDocumentContext<'_>,
        prefix: &str,
        journal_lookup: &dyn Fn(u64) -> Result<(Value, super::source_document::SourceRawDocument)>,
    ) -> Result<()> {
        match op.get("type").and_then(Value::as_str) {
            Some("put-namespace-prepare") => {
                return self.store_prepare_from_source(
                    field(&op, "prepare")?,
                    source,
                    &format!("{prefix}/prepare"),
                    None,
                    None,
                );
            }
            Some("put-prepared-operation-v7") => {
                return self.store_v7_from_source(
                    field(&op, "record")?,
                    source,
                    &format!("{prefix}/record"),
                );
            }
            _ => {}
        }
        if op.get("type").and_then(Value::as_str) == Some("put-namespace-request")
            && op.get("request").is_some_and(retired_source_request)
        {
            self.classify_source_request(field(&op, "request")?)?;
            return Ok(());
        }
        if op.get("type").and_then(Value::as_str) == Some("reconcile-cross-shard-pin-transfer") {
            for (key, schema) in [("versions", "list/version"), ("transfer", "pin-transfer")] {
                let value = op
                    .get_mut(key)
                    .ok_or_else(|| invalid("source transfer omitted typed row"))?;
                wire::normalize_original_module_schema_at(
                    value,
                    schema,
                    &format!("/{key}"),
                    source,
                    prefix,
                    None,
                )?;
            }
        }
        let field_schema = match op.get("type").and_then(Value::as_str) {
            Some("put-file-system") => Some(("file_system", "file-system")),
            Some("put-version") => Some(("version", "version")),
            Some("put-namespace-transaction") => Some(("transaction", "transaction")),
            Some("put-namespace-request") => Some(("request", "request")),
            Some("put-namespace-job") => Some(("job", "job")),
            Some("put-namespace-job-receipt") => Some(("receipt", "job-receipt")),
            Some("put-namespace-transaction-receipt") => Some(("receipt", "reference-outcome")),
            Some("put-intent") => Some(("intent", "intent")),
            _ => None,
        };
        if let Some((field, schema)) = field_schema {
            let value = op
                .get_mut(field)
                .ok_or_else(|| invalid("source table operation omitted its typed row"))?;
            wire::normalize_original_module_schema_at(
                value,
                schema,
                &format!("/{field}"),
                source,
                prefix,
                None,
            )?;
        }
        self.apply_table_op(op, journal_lookup)
    }

    fn store_prepare_from_source(
        &mut self,
        mut row: Value,
        source: &super::source_document::SourceDocumentContext<'_>,
        prefix: &str,
        sequence: Option<u64>,
        status_override: Option<DfsPreparedOperationStatus>,
    ) -> Result<()> {
        wire::verify_raw_namespace_prepare(&row, None)?;
        self.store_verified_prepare_from_source(
            row,
            source,
            prefix,
            sequence,
            status_override,
            None,
        )
    }

    fn store_verified_prepare_from_source(
        &mut self,
        mut row: Value,
        source: &super::source_document::SourceDocumentContext<'_>,
        prefix: &str,
        sequence: Option<u64>,
        status_override: Option<DfsPreparedOperationStatus>,
        payload_sources: Option<&BTreeMap<String, String>>,
    ) -> Result<()> {
        let original = row.clone();
        let id = text(&row, "operation_id")?;
        let digest = text(&row, "operation_digest")?;
        let source_status = status_override.unwrap_or(status(&row)?);
        row["status"] = serde_json::to_value(source_status).map_err(invalid)?;
        let source_request = self.detach_original_adoption(&mut row)?;
        for (key, schema) in [
            ("transaction", "transaction"),
            ("completed_request", "request"),
            ("completed_request_completion", "completion"),
        ] {
            if let Some(value) = row.get_mut(key).filter(|value| !value.is_null()) {
                wire::normalize_original_module_schema_at(
                    value,
                    schema,
                    &format!("/{key}"),
                    source,
                    prefix,
                    payload_sources,
                )?;
            }
        }
        let parts = wire::normalize_namespace_prepare(row)?;
        self.store_prepared_parts(id.clone(), digest, source_status, parts, sequence)?;
        self.retain_prepared_adoption(&id, original, source_request)
    }

    fn store_v7_from_source(
        &mut self,
        mut row: Value,
        source: &super::source_document::SourceDocumentContext<'_>,
        prefix: &str,
    ) -> Result<()> {
        wire::validate_original_v7_record(&row)?;
        let original = row.clone();
        let id = text(&field(&row, "digest")?, "operation_id")?;
        let source_request = self.detach_original_adoption(
            row.get_mut("payload")
                .ok_or_else(|| invalid("source v7 payload is absent"))?,
        )?;
        let payload = row
            .get_mut("payload")
            .ok_or_else(|| invalid("source v7 payload is absent"))?;
        for (key, schema) in [
            ("transaction", "transaction"),
            ("metadata_effects", "list/metadata-effect"),
            ("prepared_intent", "intent"),
            ("terminal_intent", "intent"),
            ("completed_request", "request"),
            ("completed_request_completion", "completion"),
        ] {
            if let Some(value) = payload.get_mut(key).filter(|value| !value.is_null()) {
                wire::normalize_original_module_schema_at(
                    value,
                    schema,
                    &format!("/payload/{key}"),
                    source,
                    prefix,
                    None,
                )?;
            }
        }
        self.store_verified_v7(row)?;
        self.retain_prepared_adoption(&id, original, source_request)
    }

    fn detach_original_adoption(&self, parts: &mut Value) -> Result<Vec<Value>> {
        let mut requests = Vec::new();
        if let Some(request) = parts
            .get("completed_request")
            .filter(|row| retired_source_request(row))
            .cloned()
        {
            wire::verify_original_namespace_request(&request)?;
            parts["completed_request"] = Value::Null;
            requests.push(request);
        }
        if let Some(effects) = parts
            .get_mut("metadata_effects")
            .and_then(Value::as_array_mut)
        {
            let mut kept = Vec::with_capacity(effects.len());
            for effect in effects.drain(..) {
                if effect.get("type").and_then(Value::as_str) == Some("put-namespace-request") {
                    let request = field(&effect, "request")?;
                    if retired_source_request(&request) {
                        wire::verify_original_namespace_request(&request)?;
                        requests.push(request);
                        continue;
                    }
                }
                kept.push(effect);
            }
            *effects = kept;
        }
        Ok(requests)
    }
    fn retain_prepared_adoption(
        &mut self,
        id: &str,
        original: Value,
        requests: Vec<Value>,
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let record = self
            .operations
            .get_mut(id)
            .ok_or_else(|| invalid("source adoption preparation was not stored"))?;
        record["source_adoption_requests"] = json!(requests);
        record["source_adoption_original"] = original.clone();
        let source_status = record["status"].clone();
        record["retired_reservation"] = Value::Bool(source_status == "prepared");
        let key = source_capsule_key(
            "prepared-adoption",
            &record["payload"],
            &["file_system_id", "shard_id", "owner_epoch", "operation_id"],
        )?;
        record["source_adoption_capsule_key"] = Value::String(key.clone());
        if source_status == "prepared" {
            self.source_capsules.insert(key, super::scope_mapping::RequiredNativeWork {
                family: "prepared_operations.old_adoption".into(),
                reason: "complete original preparation requires live source custody before native execution".into(),
                source_row: original,
            });
        } else if source_status == "committed" {
            for request in requests {
                self.classify_source_request(request)?;
            }
        }
        Ok(())
    }

    fn finalize_prepared_adoption(&mut self, id: &str) -> Result<()> {
        let record = self
            .operations
            .get(id)
            .ok_or_else(|| invalid("source adoption terminal record is absent"))?;
        let requests = record
            .get("source_adoption_requests")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let key = record
            .get("source_adoption_capsule_key")
            .and_then(Value::as_str)
            .map(str::to_owned);
        for request in requests {
            self.classify_source_request(request)?;
        }
        if let Some(key) = key {
            self.source_capsules.remove(&key);
        }
        Ok(())
    }
    fn apply_table_op_inner(
        &mut self,
        mut op: Value,
        journal_lookup: &dyn Fn(u64) -> Result<(Value, super::source_document::SourceRawDocument)>,
    ) -> Result<()> {
        let kind = text(&op, "type")?;
        wire::source_identity_fields(&mut op)?;
        self.remember_fs(&op)?;
        match kind.as_str() {
            "put-namespace-prepare-journal-reference" => {
                let sequence = decode::<u64>(field(&op, "journal_sequence")?)?;
                let (entry, raw_document) = journal_lookup(sequence)?;
                if entry.get("sequence").and_then(Value::as_u64) != Some(sequence) {
                    return Err(invalid("retained prepare reference changed sequence"));
                }
                let mutation = field(&entry, "mutation")?;
                if mutation.get("operation_id") != op.get("operation_id")
                    || mutation.get("file_system_id") != op.get("file_system_id")
                    || entry.get("shard_id") != op.get("shard_id")
                {
                    return Err(invalid(
                        "retained prepare reference crosses operation scope",
                    ));
                }
                if let Some(raw_prepare) = mutation.get("prepare").filter(|value| !value.is_null())
                {
                    if raw_prepare.get("operation_digest") != op.get("operation_digest")
                        || raw_prepare.get("owner_epoch") != op.get("owner_epoch")
                    {
                        return Err(invalid(
                            "retained prepare reference changed digest or epoch",
                        ));
                    }
                    return self.store_prepare_from_source(
                        raw_prepare.clone(),
                        &raw_document.context()?,
                        "/mutation/prepare",
                        Some(sequence),
                        Some(status(&op)?),
                    );
                }
                let decoded = wire::decode_original_packed_prepare(&mutation)?;
                if decoded.value.get("operation_digest") != op.get("operation_digest")
                    || decoded.value.get("owner_epoch") != op.get("owner_epoch")
                {
                    return Err(invalid(
                        "retained packed prepare reference changed digest or epoch",
                    ));
                }
                self.store_verified_prepare_from_source(
                    decoded.value,
                    &raw_document.context()?,
                    "/mutation",
                    Some(sequence),
                    Some(status(&op)?),
                    Some(&decoded.payload_sources),
                )?;
            }
            "put-namespace-prepare" => {
                self.store_prepare(field(&op, "prepare")?, None)?;
            }
            "commit-namespace-prepare" => {
                self.commit_namespace(&op)?;
            }
            "put-namespace-prepare-receipt" => {
                self.store_terminal_receipt(field(&op, "receipt")?)?;
            }
            "remove-namespace-prepare-receipt" => {
                self.retain_evicted_terminal(&text(&op, "operation_id")?)?;
            }
            "put-namespace-transaction" => {
                let tx = wire::normalize_namespace_transaction(field(&op, "transaction")?)?;
                self.transactions
                    .insert(tx.transaction_id.clone(), tx.clone());
                self.state()?.apply_row(
                    DfsMetadataCandidateRow::PutNamespaceTransactionReceipt {
                        receipt: transaction_receipt(&tx, tx.created_at)?,
                    },
                )?;
            }
            "put-visible-commit" => {
                self.store_visible_commit(field(&op, "commit")?)?;
            }
            "remove-visible-commit" => {
                self.remove_visible(&op)?;
            }
            "put-conflict" => {
                self.archive_conflict(field(&op, "conflict")?)?;
            }
            "put-prepared-operation-v7" => {
                self.store_v7(field(&op, "record")?)?;
            }
            "commit-prepared-operation-v7" => {
                self.commit_v7(&op)?;
            }
            "resolve-prepared-operation" => {
                self.resolve_source_operation(decode(field(&op, "digest")?)?, status(&op)?)?;
            }
            "remove-prepared-operation" => {
                self.retain_evicted_terminal(&text(&field(&op, "digest")?, "operation_id")?)?;
            }
            "put-file-system" => {
                op["file_system"] =
                    serde_json::to_value(self.normalize_fs(field(&op, "file_system")?)?)
                        .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-object" | "put-compacted-tombstone" => {
                op["object"] = serde_json::to_value(wire::normalize_object(field(&op, "object")?)?)
                    .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-version" => {
                op["version"] =
                    serde_json::to_value(wire::normalize_version(field(&op, "version")?)?)
                        .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-intent" => {
                op["intent"] = serde_json::to_value(wire::normalize_intent(field(&op, "intent")?)?)
                    .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-namespace-job" => {
                if let Some(job) = self.normalize_source_job(field(&op, "job")?)? {
                    op["job"] = serde_json::to_value(job).map_err(invalid)?;
                    self.state()?.apply_row(decode(op)?)?;
                }
            }
            "put-namespace-request" => {
                if let Some(request) = self.classify_source_request(field(&op, "request")?)? {
                    op["request"] = serde_json::to_value(request).map_err(invalid)?;
                    self.state()?.apply_row(decode(op)?)?;
                }
            }
            "put-version-pin" => {
                op["pin"] = serde_json::to_value(self.normalize_pin(field(&op, "pin")?)?)
                    .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "remove-version-pin" => {
                let object: DfsObjectId = decode(field(&op, "object_id")?)?;
                let version: DfsVersion = decode(field(&op, "version")?)?;
                let old = text(&op, "pin_id")?;
                if let Some(current) = self.pin_ids.get(&(object, version, old)) {
                    op["pin_id"] = json!(current);
                }
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-intent-receipt" => {
                op["receipt"] =
                    serde_json::to_value(wire::normalize_intent_receipt(field(&op, "receipt")?)?)
                        .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-namespace-job-receipt" => {
                op["receipt"] = serde_json::to_value(wire::normalize_namespace_job_receipt(
                    field(&op, "receipt")?,
                )?)
                .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "put-namespace-request-receipt" => {
                op["receipt"] = serde_json::to_value(wire::normalize_namespace_request_receipt(
                    field(&op, "receipt")?,
                )?)
                .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            "reconcile-cross-shard-pin-transfer" => {
                op["transfer"] =
                    serde_json::to_value(wire::normalize_pin_transfer(field(&op, "transfer")?)?)
                        .map_err(invalid)?;
                op["versions"] = Value::Array(
                    field(&op, "versions")?
                        .as_array()
                        .ok_or_else(|| invalid("retained transfer versions are not a list"))?
                        .iter()
                        .cloned()
                        .map(wire::normalize_version)
                        .map(|result| {
                            result
                                .and_then(|version| serde_json::to_value(version).map_err(invalid))
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
                self.state()?.apply_row(decode(op)?)?;
            }
            "finalize-cross-shard-source-eviction" => {
                op["tombstone"] =
                    serde_json::to_value(wire::normalize_object(field(&op, "tombstone")?)?)
                        .map_err(invalid)?;
                self.state()?.apply_row(decode(op)?)?;
            }
            // These are unchanged current leaf schemas, validated by the same
            // native projector used by ordinary table-log recovery.
            "remove-object"
            | "put-file-system-object"
            | "remove-file-system-object"
            | "put-path"
            | "remove-path"
            | "put-inode"
            | "remove-inode"
            | "put-dir-fragment"
            | "put-dentry"
            | "remove-dentry"
            | "put-hardlink"
            | "remove-hardlink"
            | "remove-compacted-tombstone-path"
            | "remove-version"
            | "remove-cross-shard-pin-transfer"
            | "put-receipt"
            | "remove-receipt"
            | "remove-retained-repair-owner"
            | "remove-intent-receipt"
            | "merge-intent-receipt-retention"
            | "put-advisory-lock-state"
            | "put-namespace-transaction-receipt"
            | "remove-namespace-transaction-receipt"
            | "remove-namespace-job-receipt"
            | "remove-namespace-request-receipt" => self.state()?.apply_row(decode(op)?)?,
            _ => return Err(invalid(format!("unknown retained table operation {kind}"))),
        }
        Ok(())
    }
    fn decode_prepare_mutation(&self, mutation: &Value) -> Result<Value> {
        wire::decode_namespace_prepare_mutation_checked(mutation.clone())
    }
    fn store_prepare(&mut self, row: Value, sequence: Option<u64>) -> Result<()> {
        let source_digest = text(&row, "operation_digest")?;
        let source_status = status(&row)?;
        let id = text(&row, "operation_id")?;
        let parts = if sequence.is_some() {
            wire::normalize_namespace_prepare(row)?
        } else {
            wire::validate_original_namespace_prepare(row, None)?
        };
        self.store_prepared_parts(id, source_digest, source_status, parts, sequence)
    }
    fn store_prepared_parts(
        &mut self,
        id: String,
        source_digest: String,
        source_status: DfsPreparedOperationStatus,
        mut parts: Value,
        sequence: Option<u64>,
    ) -> Result<()> {
        if let Some(request) = parts
            .get("completed_request")
            .filter(|value| !value.is_null())
            .cloned()
        {
            parts["completed_request"] =
                serde_json::to_value(self.normalize_request(request)?).map_err(invalid)?;
        }
        let payload = prepare_payload(&parts)?;
        self.file_system_ids.insert(payload.file_system_id.clone());
        if let Some(sequence) = sequence {
            self.source_prepare_sequences.insert(id.clone(), sequence);
        }
        let terminal_at = if source_status == DfsPreparedOperationStatus::Prepared {
            Value::Null
        } else {
            field(&parts, "transaction")?
                .get("created_at")
                .cloned()
                .unwrap_or(Value::Null)
        };
        let retiring_reservation = id.starts_with("default-layout-fence-v1:");
        self.operations.insert(id,json!({"payload":payload,"status":source_status,"terminal_at":terminal_at,"source_operation_digest":source_digest,"source_prepare_sequence":sequence,"retired_reservation":retiring_reservation}));
        Ok(())
    }
    fn store_v7(&mut self, row: Value) -> Result<()> {
        wire::validate_original_v7_record(&row)?;
        self.store_verified_v7(row)
    }
    fn store_verified_v7(&mut self, row: Value) -> Result<()> {
        // Keep actual original protocol evidence separate from the normalized
        // DATA projection. Only an unchanged current digest can become a
        // current compact terminal; legacy digests are never rehashed here.
        let original_digest = field(&row, "digest")?;
        let exact_record = serde_json::from_value::<DfsPreparedOperationRecord>(row.clone()).ok()
            .filter(|record| record.validate().is_ok());
        let exact_digest = serde_json::from_value::<DfsPreparedOperationDigest>(original_digest.clone())
            .ok().filter(|digest| digest.protocol_version == sinomo_dfs::DFS_PREPARED_OPERATION_PROTOCOL_VERSION);
        let exact_commit = exact_digest.as_ref().map(|digest| {
            row.get("committed_entry").filter(|entry| !entry.is_null())
                .map(|entry| wire::normalize_prepared_commit_reference(entry.clone(), digest)).transpose()
        }).transpose()?.flatten();
        let digest = wire::normalize_source_leaf(field(&row, "digest")?)?;
        let typed_digest: DfsPreparedOperationDigest = decode(digest.clone())?;
        let commit_reference = row
            .get("committed_entry")
            .filter(|entry| !entry.is_null())
            .map(|entry| wire::normalize_prepared_commit_reference(entry.clone(), &typed_digest))
            .transpose()?;
        let mut payload = field(&row, "payload")?;
        wire::source_identity_fields(&mut payload)?;
        if payload.get("journal_operation_id").is_none() {
            payload["journal_operation_id"] = serde_json::to_value(
                wire::source_journal_operation_id(text(&payload, "operation_id")?.as_str()),
            )
            .map_err(invalid)?;
        }
        if let Some(receipts) = payload
            .get_mut("content_receipts")
            .and_then(Value::as_array_mut)
        {
            for receipt in receipts {
                *receipt = wire::normalize_source_leaf(receipt.take())?;
            }
        }
        if let Some(expectations) = payload
            .get_mut("expected_object_versions")
            .and_then(Value::as_array_mut)
        {
            for expected in expectations {
                *expected = wire::normalize_source_leaf(expected.take())?;
            }
        }
        if let Some(expectations) = payload
            .get_mut("expected_path_occupants")
            .and_then(Value::as_array_mut)
        {
            for expected in expectations {
                *expected = wire::normalize_source_leaf(expected.take())?;
            }
        }
        for key in [
            "protocol_version",
            "file_system_id",
            "shard_id",
            "owner_epoch",
            "operation_id",
        ] {
            if payload.get(key) != digest.get(key) {
                return Err(invalid(
                    "retained current preparation changes its digest scope",
                ));
            }
        }

        let source_status = status(&row)?;
        if (source_status == DfsPreparedOperationStatus::Committed) != commit_reference.is_some() {
            return Err(invalid(
                "retained current operation terminal status contradicts its commit identity",
            ));
        }
        let id = text(&digest, "operation_id")?;
        payload["transaction"] = serde_json::to_value(wire::normalize_namespace_transaction(
            field(&payload, "transaction")?,
        )?)
        .map_err(invalid)?;
        if let Some(intent) = payload
            .get("prepared_intent")
            .filter(|value| !value.is_null())
            .cloned()
        {
            payload["prepared_intent"] =
                serde_json::to_value(wire::normalize_intent(intent)?).map_err(invalid)?;
        }
        if let Some(intent) = payload
            .get("terminal_intent")
            .filter(|value| !value.is_null())
            .cloned()
        {
            payload["terminal_intent"] =
                serde_json::to_value(wire::normalize_intent(intent)?).map_err(invalid)?;
        }
        if payload.get("expected_journal_sequence").is_none() {
            payload["expected_journal_sequence"] = Value::Null;
        }
        if payload.get("metadata_effects").is_none() {
            payload["metadata_effects"] = json!([]);
        }
        let effects = field(&payload, "metadata_effects")?
            .as_array()
            .ok_or_else(|| invalid("current source effect body is not a list"))?
            .iter()
            .cloned()
            .map(|value| normalize_effect(value, self.current_import_candidate_count))
            .collect::<Result<Vec<_>>>()?;
        payload["metadata_effects"] = Value::Array(effects);
        if let Some(request) = payload
            .get("completed_request")
            .filter(|value| !value.is_null())
            .cloned()
        {
            payload["completed_request"] =
                serde_json::to_value(self.normalize_request(request)?).map_err(invalid)?;
        }
        let payload: DfsPreparedOperationPayload = decode(payload)?;
        payload.validate()?;
        self.file_system_ids.insert(payload.file_system_id.clone());
        let terminal_at = commit_reference
            .map(|entry| serde_json::to_value(entry).map_err(invalid))
            .transpose()?
            .and_then(|entry| entry.get("created_at").cloned())
            .unwrap_or(Value::Null);
        let exact_payload = exact_record.as_ref().is_some_and(|record| record.payload.as_ref() == &payload);
        let original_terminal = exact_record.as_ref().filter(|record| record.status != DfsPreparedOperationStatus::Prepared)
            .map(DfsPreparedOperationEviction::from_terminal_record).transpose()?;
        self.operations.insert(id,json!({"payload":payload,"status":source_status,"terminal_at":terminal_at,"source_operation_digest":field(&digest,"payload_digest")?,"source_prepare_sequence":field(&digest,"prepare_sequence")?,"source_protocol_family":"prepared-operation-v7","source_original_digest":original_digest,"source_v7_exact_digest":exact_digest,"source_v7_commit_reference":exact_commit,"source_v7_payload_exact":exact_payload,"source_v7_terminal":original_terminal}));
        Ok(())
    }
    fn store_terminal_receipt(&mut self, mut row: Value) -> Result<()> {
        let original = row.clone();
        wire::source_identity_fields(&mut row)?;
        let id = text(&row, "operation_id")?;
        let terminal = status(&row)?;
        if terminal == DfsPreparedOperationStatus::Prepared {
            return Err(invalid(
                "compact retained receipt cannot replace a prepared body",
            ));
        }
        if let Some(record) = self.operations.get_mut(&id) {
            if record["source_operation_digest"] != field(&row, "operation_digest")? {
                return Err(invalid("compact retained receipt changes operation digest"));
            }
            record["status"] = serde_json::to_value(terminal).map_err(invalid)?;
            record["terminal_at"] = field(&row, "retained_at")?;
            record["compact_terminal"] = Value::Bool(true);
            return Ok(());
        }
        let scope = DfsPreparedOperationScope {
            file_system_id: decode(field(&row, "file_system_id")?)?,
            shard_id: decode(field(&row, "shard_id")?)?,
            owner_epoch: decode(field(&row, "owner_epoch")?)?,
            operation_id: id.clone(),
        };
        let payload = DfsPreparedOperationPayload::for_metadata_effects(
            scope,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            timestamp(&row, "created_at")?,
        )?;
        self.file_system_ids.insert(payload.file_system_id.clone());
        self.operations.insert(id,json!({"payload":payload,"status":terminal,"terminal_at":field(&row,"retained_at")?,"source_operation_digest":field(&row,"operation_digest")?,"source_prepare_sequence":null,"compact_terminal":true,"source_terminal_receipt":original}));
        Ok(())
    }
    fn retain_evicted_terminal(&mut self, id: &str) -> Result<()> {
        let Some(record) = self.operations.get_mut(id) else {
            return Ok(());
        };
        if record["status"] == "prepared" {
            return Err(invalid("retained source evicts an active preparation"));
        }
        record["compact_terminal"] = Value::Bool(true);
        Ok(())
    }
    fn store_visible_commit(&mut self, mut commit: Value) -> Result<()> {
        wire::source_identity_fields(&mut commit)?;
        let object = wire::normalize_object(field(&commit, "committed_object")?)?;
        self.file_system_ids.insert(object.file_system_id.clone());
        self.visible_commits
            .insert((object.object_id.clone(), object.version.clone()), commit);
        Ok(())
    }
    fn remove_visible(&mut self, op: &Value) -> Result<()> {
        let key = (
            decode(field(op, "object_id")?)?,
            decode(field(op, "version")?)?,
        );
        // Collection retires a recovery body; the canonical visible rows remain.
        self.visible_commits.remove(&key);
        Ok(())
    }
    fn archive_conflict(&mut self, row: Value) -> Result<()> {
        self.remember_fs(&row)?;
        self.conflicts.insert(text(&row, "conflict_id")?, row);
        Ok(())
    }
    fn apply_effect(&mut self, effect: DfsPreparedMetadataEffect) -> Result<()> {
        match effect {
            DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status } => {
                self.resolve_source_operation(digest, status)
            }
            DfsPreparedMetadataEffect::RemovePreparedOperation { digest } => {
                self.retain_evicted_terminal(&digest.operation_id)
            }
            effect => self.state()?.apply(effect),
        }
    }
    fn resolve_source_operation(
        &mut self,
        digest: DfsPreparedOperationDigest,
        target_status: DfsPreparedOperationStatus,
    ) -> Result<()> {
        if !matches!(
            target_status,
            DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
        ) {
            return Err(invalid(
                "source resolution cannot publish or reopen a preparation",
            ));
        }
        let record = self
            .operations
            .get_mut(&digest.operation_id)
            .ok_or_else(|| invalid("source resolution has no original operation"))?;
        let digest_value = serde_json::to_value(&digest).map_err(invalid)?;
        for key in [
            "protocol_version",
            "file_system_id",
            "shard_id",
            "owner_epoch",
            "operation_id",
        ] {
            if record["payload"].get(key) != digest_value.get(key) {
                return Err(invalid("source resolution changed original scope"));
            }
        }
        if record["source_operation_digest"] != json!(digest.payload_digest)
            || record["source_prepare_sequence"] != json!(digest.prepare_sequence)
        {
            return Err(invalid(
                "source resolution changed original digest/sequence",
            ));
        }
        let status_value = serde_json::to_value(target_status).map_err(invalid)?;
        if record["status"] != "prepared" && record["status"] != status_value {
            return Err(invalid(
                "source resolution changes an existing terminal outcome",
            ));
        }
        record["status"] = status_value;
        if record.get("source_adoption_requests").is_some() {
            record["retired_reservation"] = Value::Bool(false);
            if let Some(key) = record
                .get("source_adoption_capsule_key")
                .and_then(Value::as_str)
            {
                self.source_capsules.remove(key);
            }
        }
        Ok(())
    }
    fn commit_namespace(&mut self, marker: &Value) -> Result<()> {
        let id = text(marker, "operation_id")?;
        let record = self
            .operations
            .get(&id)
            .cloned()
            .ok_or_else(|| invalid("retained namespace commit has no complete preparation"))?;
        if record["source_operation_digest"] != field(marker, "operation_digest")?
            || record["payload"]["owner_epoch"] != field(marker, "owner_epoch")?
        {
            return Err(invalid(
                "retained namespace commit changes its captured digest or epoch",
            ));
        }
        if record["status"] == "committed" {
            return Ok(());
        }
        if record["status"] != "prepared" {
            return Err(invalid(
                "retained namespace commit follows terminal rejection",
            ));
        }
        let payload: DfsPreparedOperationPayload = decode(field(&record, "payload")?)?;
        self.state()?.apply_namespace(&payload.transaction)?;
        for effect in payload.metadata_effects {
            self.apply_effect(effect)?;
        }
        if let Some(request) = payload.completed_request {
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutNamespaceRequest {
                    request: Box::new(request),
                })?;
        }
        if let Some(completion) = payload.completed_request_completion {
            let receipt = completion_receipt(&completion)?;
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutNamespaceRequestReceipt {
                    receipt: Box::new(receipt),
                })?;
        }
        let terminal_record = self.operations.get_mut(&id).expect("captured operation");
        terminal_record["status"] = json!("committed");
        if terminal_record.get("source_adoption_requests").is_some() {
            terminal_record["retired_reservation"] = Value::Bool(false);
        }
        self.finalize_prepared_adoption(&id)?;
        Ok(())
    }
    fn commit_v7(&mut self, marker: &Value) -> Result<()> {
        let digest = field(marker, "digest")?;
        let id = text(&digest, "operation_id")?;
        let record = self
            .operations
            .get(&id)
            .cloned()
            .ok_or_else(|| invalid("retained v7 commit lacks preparation"))?;
        for key in [
            "protocol_version",
            "file_system_id",
            "shard_id",
            "owner_epoch",
            "operation_id",
        ] {
            if record["payload"].get(key) != digest.get(key) {
                return Err(invalid("retained current commit changes original scope"));
            }
        }
        if record["source_operation_digest"] != field(&digest, "payload_digest")?
            || record["source_prepare_sequence"] != field(&digest, "prepare_sequence")?
        {
            return Err(invalid("retained v7 commit changed original identity"));
        }
        if record["status"] == "committed" {
            return Ok(());
        }
        if record["status"] != "prepared" {
            return Err(invalid("retained v7 commit follows rejection"));
        }
        let payload: DfsPreparedOperationPayload = decode(field(&record, "payload")?)?;
        for effect in payload.metadata_effects {
            self.apply_effect(effect)?;
        }
        self.state()?.apply_namespace(&payload.transaction)?;
        if let Some(terminal) = payload.terminal_intent {
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutIntent { intent: terminal })?;
        }
        if let Some(request) = payload.completed_request {
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutNamespaceRequest {
                    request: Box::new(request),
                })?;
        }
        if let Some(completion) = payload.completed_request_completion {
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutNamespaceRequestReceipt {
                    receipt: Box::new(completion_receipt(&completion)?),
                })?;
        }
        let terminal_record = self.operations.get_mut(&id).expect("captured operation");
        if let Some(raw_digest) = terminal_record.get("source_v7_exact_digest").filter(|value| !value.is_null()) {
            let exact: DfsPreparedOperationDigest = serde_json::from_value(raw_digest.clone()).map_err(invalid)?;
            if let Some(reference) = marker.get("committed_entry").filter(|value| !value.is_null()) {
                let reference = wire::normalize_prepared_commit_reference(reference.clone(), &exact)?;
                terminal_record["source_v7_commit_reference"] = serde_json::to_value(reference).map_err(invalid)?;
            }
        }
        terminal_record["status"] = json!("committed");
        if terminal_record.get("source_adoption_requests").is_some() {
            terminal_record["retired_reservation"] = Value::Bool(false);
        }
        self.finalize_prepared_adoption(&id)?;
        Ok(())
    }
    pub(super) fn apply_journal_from_source(
        &mut self,
        mut entry: Value,
        source: &super::source_document::SourceDocumentContext<'_>,
    ) -> Result<()> {
        let sequence = decode::<u64>(field(&entry, "sequence")?)?;
        let mutation = field(&entry, "mutation")?;
        let kind = text(&mutation, "type")?;
        if kind == "put-namespace-request"
            && mutation.get("request").is_some_and(retired_source_request)
        {
            self.classify_source_request(field(&mutation, "request")?)?;
            return Ok(());
        }
        if kind == "put-namespace-prepare" && mutation.get("prepare").is_some_and(|v| !v.is_null())
        {
            return self.store_prepare_from_source(
                field(&mutation, "prepare")?,
                source,
                "/mutation/prepare",
                Some(sequence),
                None,
            );
        }
        if kind == "put-namespace-prepare" {
            let decoded = wire::decode_original_packed_prepare(&mutation)?;
            return self.store_verified_prepare_from_source(
                decoded.value,
                source,
                "/mutation",
                Some(sequence),
                None,
                Some(&decoded.payload_sources),
            );
        }
        if kind == "prepare-operation-v7" {
            return self.store_v7_from_source(
                field(&mutation, "record")?,
                source,
                "/mutation/record",
            );
        }
        let schemas: &[(&str, &str)] = match kind.as_str() {
            "put-file-system" => &[("file_system", "file-system")],
            "put-version" => &[("version_record", "version")],
            "commit-visible-version" => &[("visible_version", "version")],
            "apply-namespace-transaction" => &[("transaction", "transaction")],
            "capture-file-version" => &[("record", "version")],
            "put-intent" => &[("intent", "intent")],
            "put-namespace-request" => &[("request", "request")],
            "put-namespace-job" => &[("job", "job")],
            "put-namespace-job-receipt" => &[("receipt", "job-receipt")],
            "put-namespace-transaction-receipt" => &[("receipt", "reference-outcome")],
            "reconcile-cross-shard-pin-transfer" => {
                &[("versions", "list/version"), ("transfer", "pin-transfer")]
            }
            _ => &[],
        };
        for (key, schema) in schemas {
            if let Some(value) = entry
                .get_mut("mutation")
                .and_then(|mutation| mutation.get_mut(*key))
                .filter(|value| !value.is_null())
            {
                wire::normalize_original_module_schema_at(
                    value,
                    schema,
                    &format!("/mutation/{key}"),
                    source,
                    "",
                    None,
                )?;
            }
        }
        self.apply_journal(entry)
    }

    pub(super) fn apply_journal(&mut self, entry: Value) -> Result<()> {
        let sequence = decode::<u64>(field(&entry, "sequence")?)?;
        let mut mutation = field(&entry, "mutation")?;
        let kind = text(&mutation, "type")?;
        wire::source_identity_fields(&mut mutation)?;
        self.remember_fs(&mutation)?;
        match kind.as_str() {
            "apply-namespace-transaction"
                if mutation.get("transaction").is_none_or(Value::is_null) =>
            {
                let id = text(&mutation, "transaction_id")?;
                mutation["transaction"] =
                    serde_json::to_value(self.transactions.get(&id).ok_or_else(|| {
                        invalid(
                            "payload-less transaction journal has no exact complete source body",
                        )
                    })?)
                    .map_err(invalid)?;
            }
            "put-file-system" if mutation.get("file_system").is_none_or(Value::is_null) => {
                let id = decode(field(&mutation, "file_system_id")?)?;
                mutation["file_system"] =
                    serde_json::to_value(self.state()?.file_system(&id).ok_or_else(|| {
                        invalid("payload-less filesystem journal has no source row")
                    })?)
                    .map_err(invalid)?;
            }
            "put-object" if mutation.get("object").is_none_or(Value::is_null) => {
                let id = decode(field(&mutation, "object_id")?)?;
                let row = self
                    .state()?
                    .object(&id)
                    .ok_or_else(|| invalid("payload-less object journal has no source row"))?;
                if serde_json::to_value(&row.file_system_id).map_err(invalid)?
                    != field(&mutation, "file_system_id")?
                    || row.path != text(&mutation, "path")?
                {
                    return Err(invalid(
                        "payload-less object source row crosses recorded scope/path",
                    ));
                }
                mutation["object"] = serde_json::to_value(row).map_err(invalid)?;
            }
            "put-version" if mutation.get("version_record").is_none_or(Value::is_null) => {
                let object = decode(field(&mutation, "object_id")?)?;
                let version = decode(field(&mutation, "version")?)?;
                mutation["version_record"] =
                    serde_json::to_value(self.state()?.version(&object, &version).ok_or_else(
                        || invalid("payload-less version journal has no exact source revision"),
                    )?)
                    .map_err(invalid)?;
            }
            "put-intent" if mutation.get("intent").is_none_or(Value::is_null) => {
                let object = decode(field(&mutation, "object_id")?)?;
                let id = text(&mutation, "intent_id")?;
                mutation["intent"] =
                    serde_json::to_value(self.state()?.intent(&object, &id).ok_or_else(|| {
                        invalid("payload-less intent journal has no exact source identity")
                    })?)
                    .map_err(invalid)?;
            }
            "put-receipt" if mutation.get("receipt").is_none_or(Value::is_null) => {
                let object = decode(field(&mutation, "object_id")?)?;
                let version = decode(field(&mutation, "version")?)?;
                let node = decode(field(&mutation, "replica_node_id")?)?;
                mutation["receipt"] = serde_json::to_value(
                    self.state()?
                        .receipt(&object, &version, &node)
                        .ok_or_else(|| {
                            invalid("payload-less receipt journal has no exact source tuple")
                        })?,
                )
                .map_err(invalid)?;
            }
            "put-namespace-job" if mutation.get("job").is_none_or(Value::is_null) => {
                let id = text(&mutation, "job_id")?;
                mutation["job"] =
                    serde_json::to_value(self.state()?.job(&id).ok_or_else(|| {
                        invalid("payload-less job journal has no durable source progress")
                    })?)
                    .map_err(invalid)?;
            }
            "put-advisory-lock-state" if mutation.get("state").is_none_or(Value::is_null) => {
                let object = decode(field(&mutation, "object_id")?)?;
                mutation["state"] =
                    serde_json::to_value(self.state()?.advisory(&object).ok_or_else(|| {
                        invalid("payload-less advisory journal has no source aggregate")
                    })?)
                    .map_err(invalid)?;
            }
            _ => {}
        }

        match kind.as_str() {
            "put-namespace-prepare" => {
                let row = self.decode_prepare_mutation(&mutation)?;
                self.store_prepare(row, Some(sequence))?;
            }
            "prepare-operation-v7" => {
                self.store_v7(field(&mutation, "record")?)?;
            }
            "commit-prepared-operation-v7" => {
                let mut marker = mutation.clone();
                marker["committed_entry"] = json!({"shard_id":field(&entry,"shard_id")?,
                    "sequence":sequence,"created_at":field(&entry,"created_at")?,"digest":field(&mutation,"digest")?});
                self.commit_v7(&marker)?;
            }
            "commit-namespace-prepare" => {
                self.commit_namespace(&mutation)?;
            }
            "commit-visible-version" => {
                let object = wire::normalize_object(field(&mutation, "committed_object")?)?;
                let version = wire::normalize_version(field(&mutation, "visible_version")?)?;
                self.state()?
                    .apply(DfsPreparedMetadataEffect::PutVersion { version })?;
                self.state()?
                    .apply(DfsPreparedMetadataEffect::PutObject { object })?;
            }
            "apply-namespace-transaction" => {
                let tx = wire::normalize_namespace_transaction(field(&mutation, "transaction")?)?;
                self.transactions
                    .insert(tx.transaction_id.clone(), tx.clone());
                self.state()?.apply_namespace(&tx)?;
            }
            "capture-file-version" => {
                self.state()?.apply(DfsPreparedMetadataEffect::PutVersion {
                    version: wire::normalize_version(field(&mutation, "record")?)?,
                })?;
                self.state()?
                    .apply(DfsPreparedMetadataEffect::PutVersionPin {
                        pin: decode(field(&mutation, "pin")?)?,
                    })?;
            }
            "remove-version-pin" => self.apply_table_op_inner(mutation.clone(), &|_| {
                Err(invalid("journal pin reference lookup unavailable"))
            })?,
            "put-intent-receipt"
            | "put-namespace-job-receipt"
            | "put-namespace-request-receipt" => self.apply_table_op_inner(
                json!({"type":kind,"receipt":field(&mutation,"receipt")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "put-file-system" => self.apply_table_op_inner(
                json!({"type":kind,"file_system":field(&mutation,"file_system")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "put-object" => {
                self.state()?.apply(DfsPreparedMetadataEffect::PutObject {
                    object: wire::normalize_object(field(&mutation, "object")?)?,
                })?;
            }
            "put-version" => self.apply_table_op_inner(
                json!({"type":kind,"version":field(&mutation,"version_record")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "put-receipt" => self.apply_table_op_inner(
                json!({"type":kind,"receipt":field(&mutation,"receipt")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "put-intent" => self.apply_table_op_inner(
                json!({"type":kind,"intent":field(&mutation,"intent")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "put-advisory-lock-state" => self.apply_table_op_inner(
                json!({"type":kind,"state":field(&mutation,"state")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "put-namespace-job" => self.apply_table_op_inner(
                json!({"type":kind,"job":field(&mutation,"job")?}),
                &|_| Err(invalid("journal tail reference lookup unavailable")),
            )?,
            "remove-version"
            | "reconcile-cross-shard-pin-transfer"
            | "remove-cross-shard-pin-transfer"
            | "finalize-cross-shard-source-eviction"
            | "remove-receipt"
            | "remove-retained-repair-owner"
            | "remove-intent-receipt"
            | "merge-intent-receipt-retention"
            | "remove-visible-commit"
            | "put-conflict"
            | "put-namespace-transaction-receipt"
            | "remove-namespace-transaction-receipt"
            | "put-namespace-prepare-receipt"
            | "remove-namespace-prepare-receipt"
            | "remove-namespace-job-receipt"
            | "put-namespace-request"
            | "remove-namespace-request-receipt" => {
                let mut op = mutation.clone();
                if let Some(object) = op.as_object_mut() {
                    object.remove("file_system_id");
                    object.remove("authoritative_migration");
                    object.remove("routing_change");
                }
                self.apply_table_op_inner(op, &|_| {
                    Err(invalid("journal tail reference lookup unavailable"))
                })?;
            }
            _ => return Err(invalid(format!("unknown retained journal mutation {kind}"))),
        }
        Ok(())
    }
    fn capture_legacy_intents(&mut self) -> Result<()> {
        let rows = self.state()?.snapshot().intents;
        for intent in rows {
            let id = format!("write:{}", intent.intent_id);
            if self.operations.contains_key(&id) {
                continue;
            }
            let shard = self.scope.clone().ok_or_else(|| {
                invalid("content intent conversion requires an actual bound shard")
            })?;
            let terminal = matches!(
                intent.status,
                DfsWriteIntentStatus::Committed
                    | DfsWriteIntentStatus::Aborted
                    | DfsWriteIntentStatus::Rejected
            );
            let state = if terminal {
                if intent.status == DfsWriteIntentStatus::Committed {
                    DfsPreparedOperationStatus::Committed
                } else {
                    DfsPreparedOperationStatus::Aborted
                }
            } else {
                DfsPreparedOperationStatus::Prepared
            };
            let payload = DfsPreparedOperationPayload::for_metadata_effects(
                DfsPreparedOperationScope {
                    file_system_id: intent.file_system_id.clone(),
                    shard_id: shard,
                    owner_epoch: intent.owner_epoch,
                    operation_id: id.clone(),
                },
                Vec::new(),
                Vec::new(),
                vec![DfsPreparedMetadataEffect::PutIntent {
                    intent: intent.clone(),
                }],
                intent.created_at,
            )?;
            self.operations.insert(id,json!({"payload":payload,"status":state,"terminal_at":intent.updated_at.map(|time|time.format(&time::format_description::well_known::Rfc3339)).transpose().map_err(invalid)?,"source_operation_digest":hash(&serde_json::to_value(&intent).map_err(invalid)?)?,"source_prepare_sequence":null,"source_intent_only":true,"retired_reservation":!terminal}));
        }
        let receipts = self.state()?.snapshot().intent_receipts;
        for receipt in receipts {
            let id = format!("write:{}", receipt.intent_id);
            if self.operations.contains_key(&id) {
                continue;
            }
            let shard = self
                .scope
                .clone()
                .ok_or_else(|| invalid("terminal write conversion requires actual bound shard"))?;
            let source = serde_json::to_value(&receipt).map_err(invalid)?;
            let status = if receipt.status == DfsWriteIntentStatus::Committed {
                DfsPreparedOperationStatus::Committed
            } else {
                DfsPreparedOperationStatus::Aborted
            };
            let payload = DfsPreparedOperationPayload::for_metadata_effects(
                DfsPreparedOperationScope {
                    file_system_id: receipt.file_system_id.clone(),
                    shard_id: shard,
                    owner_epoch: receipt.owner_epoch,
                    operation_id: id.clone(),
                },
                Vec::new(),
                Vec::new(),
                vec![DfsPreparedMetadataEffect::PutIntentReceipt {
                    receipt: receipt.clone(),
                }],
                receipt.created_at,
            )?;
            self.operations.insert(id,json!({"payload":payload,"status":status,"terminal_at":receipt.terminal_at.format(&time::format_description::well_known::Rfc3339).map_err(invalid)?,"source_operation_digest":hash(&source)?,"source_prepare_sequence":null,"compact_terminal":true,"source_terminal_receipt":source}));
        }
        Ok(())
    }
    fn recover_visible_commits(&mut self) -> Result<()> {
        let mut commits = self
            .visible_commits
            .values()
            .map(|commit| Ok((timestamp(commit, "created_at")?, commit.clone())))
            .collect::<Result<Vec<_>>>()?;
        commits.sort_by_key(|(created_at, _)| *created_at);
        let snapshot = self.state()?.snapshot();
        let tombstone_paths = snapshot
            .objects
            .iter()
            .chain(snapshot.compacted_tombstones.iter())
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .map(|object| {
                (
                    object.file_system_id.clone(),
                    object.path.trim_matches('/').to_owned(),
                )
            })
            .collect::<BTreeSet<_>>();
        for (_, commit) in commits {
            let object = wire::normalize_object(field(&commit, "committed_object")?)?;
            let expected: DfsVersion = decode(field(&commit, "expected_version")?)?;
            let existing = self.state()?.object(&object.object_id);
            if tombstone_paths.contains(&(
                object.file_system_id.clone(),
                object.path.trim_matches('/').to_owned(),
            )) || existing.as_ref().is_some_and(|current| {
                current.kind == DfsObjectKind::Tombstone
                    && current.path.trim_matches('/') == object.path.trim_matches('/')
            }) {
                continue;
            }
            if existing
                .as_ref()
                .is_some_and(|current| current.version == object.version)
                && self
                    .state()?
                    .version(&object.object_id, &object.version)
                    .is_some()
            {
                continue;
            }

            if existing.as_ref().is_some_and(|current| {
                current.version != expected && current.version != object.version
            }) {
                continue;
            }
            if existing.as_ref().is_some_and(|current| current == &object) {
                continue;
            }
            let version = wire::normalize_version(field(&commit, "visible_version")?)?;
            let required: Vec<sinomo_core::node::NodeId> = decode(
                commit
                    .get("required_replica_node_ids")
                    .cloned()
                    .unwrap_or(json!([])),
            )?;
            for node in required {
                let receipt = self
                    .state()?
                    .receipt(&object.object_id, &object.version, &node)
                    .ok_or_else(|| {
                        invalid("durable legacy content commit has no required source receipt")
                    })?;
                if receipt.content_hash != object.content_hash
                    || receipt.receipt.file_system_id != object.file_system_id
                {
                    return Err(invalid("legacy content commit receipt tuple changed"));
                }
            }
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutVersion { version })?;
            let mut intents = self
                .state()?
                .intents_for_version(&object.object_id, &object.version);
            for intent in &mut intents {
                intent.status = DfsWriteIntentStatus::Committed;
                intent.object_metadata = Some(object.clone());
                intent.updated_at = Some(timestamp(&commit, "created_at")?);
                self.state()?.apply(DfsPreparedMetadataEffect::PutIntent {
                    intent: intent.clone(),
                })?;
            }
            self.state()?
                .apply(DfsPreparedMetadataEffect::PutObject { object })?;
        }
        Ok(())
    }
    pub(super) fn current_tables(&mut self) -> Result<DfsMetadataTableSnapshotTables> {
        if !self.deferred.is_empty() {
            return Err(invalid(
                "unresolved retained journal references prevent conversion",
            ));
        }
        self.recover_visible_commits()?;
        self.capture_legacy_intents()?;
        let mut tables = self.state()?.snapshot();
        super::retained_content::restore_recorded_source_durability(&mut tables)?;
        for fallback in self.retired_adoption_receipts.values() {
            if let Some(actual) = tables.namespace_request_receipts.iter().find(|row| {
                row.requester_node_id == fallback.requester_node_id
                    && row.request_id == fallback.request_id
            }) {
                let mut normalized = actual.clone();
                normalized.retained_at = fallback.retained_at;
                if normalized != *fallback {
                    return Err(invalid(
                        "old adoption compact receipt changes its exact original terminal result",
                    ));
                }
            } else {
                tables.namespace_request_receipts.push(fallback.clone());
            }
        }
        let requests = tables
            .namespace_requests
            .iter()
            .map(|request| {
                (
                    (
                        request.requester_node_id.clone(),
                        request.request_id.clone(),
                    ),
                    request,
                )
            })
            .collect::<BTreeMap<_, _>>();
        for receipt in &mut tables.namespace_request_receipts {
            let key = (
                receipt.requester_node_id.clone(),
                receipt.request_id.clone(),
            );
            if let (Some((source, current)), Some(request)) =
                (self.request_digests.get(&key), requests.get(&key))
            {
                if &receipt.command_digest == source
                    && receipt.file_system_id == request.file_system_id
                    && receipt.operation_id == request.operation_id
                    && receipt.authority_shard_id == request.authority_shard_id
                {
                    receipt.command_digest = current.clone();
                }
            }
        }

        let version_indexes = tables
            .versions
            .iter()
            .enumerate()
            .map(|(index, version)| ((version.object_id.clone(), version.version.clone()), index))
            .collect::<BTreeMap<_, _>>();
        let objects = tables
            .objects
            .iter()
            .map(|object| (object.object_id.clone(), object.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut orders = BTreeMap::<DfsObjectId, u64>::new();
        for version in &tables.versions {
            let order = version
                .retention
                .get("module.dfs.migration")
                .map_or(0, |state| state.order);
            orders
                .entry(version.object_id.clone())
                .and_modify(|old| *old = (*old).max(order))
                .or_insert(order);
        }
        for conflict in self.conflicts.values() {
            let conflict_id = text(conflict, "conflict_id")?;
            let provenance = hash(conflict)?;
            if self.archived_conflicts.get(&conflict_id) == Some(&provenance) {
                continue;
            }
            let object_id: DfsObjectId = decode(field(conflict, "object_id")?)?;
            let fs: DfsFileSystemId = decode(field(conflict, "file_system_id")?)?;
            let detected = timestamp(conflict, "detected_at")?;
            let detected_nanos = u64::try_from(detected.unix_timestamp_nanos()).map_err(invalid)?;
            for key in ["local_version", "remote_version"] {
                let version: DfsVersion = decode(field(conflict, key)?)?;
                let index=*version_indexes.get(&(object_id.clone(),version.clone())).ok_or_else(||invalid("conflict alternative has no immutable source revision; archive retained, conversion unavailable"))?;
                let row = &mut tables.versions[index];
                if row.file_system_id != fs {
                    return Err(invalid("conflict alternative crosses filesystem scope"));
                }
                let hash_field = if key == "local_version" {
                    "local_content_hash"
                } else {
                    "remote_content_hash"
                };
                if let Some(expected) = conflict.get(hash_field).filter(|value| !value.is_null()) {
                    if serde_json::to_value(&row.content_hash).map_err(invalid)? != *expected {
                        return Err(invalid(
                            "conflict alternative immutable hash differs from source retention root",
                        ));
                    }
                }
                if row.content_hash.is_none() {
                    return Err(invalid(
                        "conflict alternative has no immutable content identity",
                    ));
                }
                let evidence = row
                    .retention
                    .values()
                    .filter_map(|state| state.grant.as_ref())
                    .map(|grant| grant.object.as_ref())
                    .find(|object| {
                        object.file_system_id == fs
                            && object.object_id == object_id
                            && object.version == version
                            && object.content_hash == row.content_hash
                            && object.content_size_bytes == row.size_bytes
                    })
                    .cloned()
                    .or_else(|| {
                        objects
                            .get(&object_id)
                            .filter(|object| {
                                object.file_system_id == fs
                                    && object.version == version
                                    && object.content_hash == row.content_hash
                                    && object.content_size_bytes == row.size_bytes
                            })
                            .cloned()
                    });
                if let Some(evidence) = evidence {
                    let order = orders.entry(object_id.clone()).or_default();
                    *order = order
                        .checked_add(1)
                        .ok_or_else(|| invalid("migration revision order overflow"))?;
                    row.retention.insert(
                        "module.dfs.migration".into(),
                        DfsRevisionRetentionState {
                            order: *order,
                            grant: Some(Box::new(DfsRevisionRetentionGrant {
                                object: Box::new(evidence),
                                granted_at_unix_nanos: detected_nanos,
                                origin: DfsRetentionGrantOrigin::ExplicitOperation,
                                minimum: None,
                            })),
                        },
                    );
                }
                row.module_metadata.insert("module.dfs.migration".into(),decode(json!({"schema_version":1,"payload":{"source_conflict_id":field(conflict,"conflict_id")?,"source_record_digest":provenance,"archive_owner":"dfs-migration-source-cut"}}))?);
                row.validate_extension_metadata().map_err(invalid)?;
                let pin_id = content_reference_storage_id(
                    "module.dfs.migration",
                    &format!("conflict:{}:{}", text(conflict, "conflict_id")?, key),
                );
                if !tables.version_pins.iter().any(|pin| {
                    pin.object_id == object_id && pin.version == version && pin.pin_id == pin_id
                }) {
                    tables.version_pins.push(DfsMetadataVersionPinRecord {
                        namespace: "module.dfs.migration".into(),
                        file_system_id: fs.clone(),
                        object_id: object_id.clone(),
                        version,
                        pin_id,
                        created_at: detected,
                    });
                }
            }
        }
        self.current = Some(DfsMetadataCandidateState::new(tables)?);
        for (id, conflict) in &self.conflicts {
            self.archived_conflicts.insert(id.clone(), hash(conflict)?);
        }
        Ok(self
            .current
            .as_ref()
            .expect("candidate finalized")
            .snapshot())
    }
    pub(super) fn pending_operations(&self) -> Result<Vec<Value>> {
        if !self.deferred.is_empty() {
            return Err(invalid(
                "retained pending projection has unresolved journal references",
            ));
        }
        Ok(self.operations.values().cloned().collect())
    }
    pub(super) fn source_file_system_ids(&self) -> Result<Vec<DfsFileSystemId>> {
        Ok(self.file_system_ids.iter().cloned().collect())
    }
}
fn old_advisory_preflight(row: &Value) -> bool {
    row.get("operation")
        .and_then(Value::as_object)
        .is_some_and(|variants| {
            variants.len() == 1 && variants.contains_key("advisory-lock-migration-prepare")
        })
}
fn retired_source_request(row: &Value) -> bool {
    old_adoption_without_offer(row) || old_advisory_preflight(row)
}
fn old_adoption_without_offer(row: &Value) -> bool {
    row.get("operation")
        .and_then(Value::as_object)
        .filter(|variants| variants.len() == 1)
        .and_then(|variants| variants.get("adopt-object-at-path"))
        .is_some_and(|body| body.get("source_offer").is_none_or(Value::is_null))
}
fn source_capsule_key(family: &str, row: &Value, fields: &[&str]) -> Result<String> {
    let values = fields
        .iter()
        .map(|name| field(row, name))
        .collect::<Result<Vec<_>>>()?;
    serde_json::to_string(&(family, values)).map_err(invalid)
}
fn prepare_payload(parts: &Value) -> Result<DfsPreparedOperationPayload> {
    let mut payload = DfsPreparedOperationPayload::for_namespace_transaction(
        DfsPreparedOperationScope {
            file_system_id: decode(field(parts, "file_system_id")?)?,
            shard_id: decode(field(parts, "shard_id")?)?,
            owner_epoch: decode(field(parts, "owner_epoch")?)?,
            operation_id: text(parts, "operation_id")?,
        },
        decode(
            parts
                .get("expected_object_versions")
                .cloned()
                .unwrap_or(json!([])),
        )?,
        decode(
            parts
                .get("expected_path_occupants")
                .cloned()
                .unwrap_or(json!([])),
        )?,
        wire::normalize_namespace_transaction(field(parts, "transaction")?)?,
        decode(parts.get("content_receipts").cloned().unwrap_or(json!([])))?,
        None,
        None,
        Vec::new(),
        parts
            .get("completed_request")
            .filter(|value| !value.is_null())
            .cloned()
            .map(wire::normalize_namespace_request)
            .transpose()?,
        parts
            .get("completed_request_completion")
            .filter(|value| !value.is_null())
            .cloned()
            .map(decode)
            .transpose()?,
    )?;
    payload.journal_operation_id = wire::source_journal_operation_id(&payload.operation_id);
    payload.validate()?;
    Ok(payload)
}
fn transaction_receipt(
    transaction: &DfsNamespaceTransaction,
    retained_at: OffsetDateTime,
) -> Result<DfsNamespaceTransactionReceipt> {
    let mut hash = blake3::Hasher::new();
    serde_json::to_writer(&mut hash, transaction).map_err(invalid)?;
    Ok(DfsNamespaceTransactionReceipt {
        content_reference_identity: transaction.content_reference_identity.clone(),
        contribution_digest: transaction.contribution_digest.clone(),
        transaction_id: transaction.transaction_id.clone(),
        file_system_id: transaction.file_system_id.clone(),
        transaction_digest: format!("blake3:{}", hash.finalize().to_hex()),
        created_at: transaction.created_at,
        retained_at,
    })
}
fn completion_receipt(
    completion: &DfsNamespaceRequestCompletion,
) -> Result<DfsNamespaceRequestReceipt> {
    let value = serde_json::to_value(completion).map_err(invalid)?;
    let mut row = value
        .as_object()
        .ok_or_else(|| invalid("current completion is not an object"))?
        .clone();
    row.remove("transaction_id");
    row.remove("status");
    let completed_at = row
        .remove("completed_at")
        .ok_or_else(|| invalid("current completion has no terminal time"))?;
    row.insert("retained_at".into(), completed_at);
    decode(Value::Object(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::node::NodeId;
    fn fs() -> DfsFileSystem {
        DfsFileSystem::sifs(NodeId::from_legacy_named("projection-owner").unwrap())
    }
    fn object(version: &str) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("projection-object").unwrap(),
            file_system_id: fs().file_system_id,
            path: "/docs/file".into(),
            kind: DfsObjectKind::File,
            namespace_link_count: None,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("projection-owner").unwrap(),
            current_locations: Vec::new(),
            content_hash: Some(format!("blake3:{version}")),
            content_size_bytes: Some(10),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(version).unwrap(),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }
    fn seeded() -> LegacyProjection {
        let mut projection = LegacyProjection::default();
        projection
            .bind_scope(DfsMetadataShardId::new("root"))
            .unwrap();
        projection
            .push_snapshot_row("file_systems", serde_json::to_value(fs()).unwrap())
            .unwrap();
        projection
    }
    #[test]
    fn original_opaque_array_survives_table_and_journal_conversion() {
        use sinomo_core::object::{LengthMode, serde_ext};
        let opaque = json!([110, 117, 108, 108]);
        let source_version = json!({
            "file_system_id":fs().file_system_id,
            "object_id":object("opaque").object_id,
            "version":object("opaque").version,
            "content_hash":"a".repeat(64),"size_bytes":10,"locations":[],
            "visible":false,"created_at":"1970-01-01T00:00:00Z",
            "module_metadata":{"vendor.opaque":{"schema_version":1,"payload":opaque}}
        });
        let table = json!({"sequence":1,"ops":[{"type":"put-version","version":source_version}]});
        let raw = super::super::source_document::SourceRawDocument::row(
            serde_ext::to_bytes(&table, 1, LengthMode::U32).unwrap(),
        );
        let mut projection = seeded();
        projection
            .apply_table_op_from_source(
                table["ops"][0].clone(),
                &raw.context().unwrap(),
                "/ops/0",
                &|_| Err(invalid("unused")),
            )
            .unwrap();
        let tables = projection.current_tables().unwrap();
        assert_eq!(
            tables.versions[0].module_metadata["vendor.opaque"].payload,
            opaque
        );

        let journal =
            json!({"sequence":1,"mutation":{"type":"put-version","version_record":source_version}});
        let raw = super::super::source_document::SourceRawDocument::row(
            serde_ext::to_bytes(&journal, 1, LengthMode::U32).unwrap(),
        );
        let mut projection = seeded();
        projection
            .apply_journal_from_source(journal, &raw.context().unwrap())
            .unwrap();
        let tables = projection.current_tables().unwrap();
        assert_eq!(
            tables.versions[0].module_metadata["vendor.opaque"].payload,
            opaque
        );
    }
    fn old_adoption_request(completed: bool) -> Value {
        let mut directory = object("old-adopt-directory");
        directory.kind = DfsObjectKind::Directory;
        directory.content_hash = None;
        directory.content_size_bytes = None;
        let mut request = json!({"file_system_id":fs().file_system_id,"requester_node_id":NodeId::from_legacy_named("old-adopt-requester").unwrap(),"request_id":"old-request","operation_id":"old-operation","authority_shard_id":"root",
            "operation":{"adopt-object-at-path":{"object":directory,"source_path":"/source/old-directory","pin_transfer_id":null,"versions":[],"version_pins":[],"version_pin_set_fingerprint":null,"receipts":[],"intents":[],"intent_receipts":[],"intent_receipt_retention":{},"content_reference_outcomes":[],"content_reference_outcome_retention":null,"conflicts":[],"advisory_lock_state":null}},
            "command_digest":"","status":if completed {"completed"} else {"pending"},"result":if completed {json!("applied")} else {Value::Null},"posix_coordination_result":null,"recovery_attempts":0,"last_recovery_error":null,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:01Z"});
        request["command_digest"] =
            json!(wire::original_namespace_request_digest(&request).unwrap());
        request
    }
    fn old_preflight_request(completed: bool) -> Value {
        let mut request = old_adoption_request(completed);
        request["operation"] = json!({"advisory-lock-migration-prepare": {
            "path":"/target/file", "source_path":"/source/file",
            "object_id":object("old-preflight").object_id, "state_fingerprint":"original-source-fence"
        }});
        request["posix_coordination_result"] = if completed {
            json!({"advisory-lock-migration-prepared": {"object_id":object("old-preflight").object_id, "state_fingerprint":"original-source-fence"}})
        } else {
            Value::Null
        };
        request["command_digest"] =
            json!(wire::original_namespace_request_digest(&request).unwrap());
        request
    }
    #[test]
    fn retired_preflight_terminal_receipt_keeps_exact_source_auxiliary_capsule() {
        let source = old_preflight_request(true);
        let mut projection = seeded();
        projection
            .push_snapshot_row("namespace_requests", source.clone())
            .unwrap();
        let tables = projection.current_tables().unwrap();
        assert!(tables.namespace_requests.is_empty());
        let receipt = &tables.namespace_request_receipts[0];
        assert_eq!(receipt.result, DfsNamespaceResult::Applied);
        assert_eq!(
            receipt.command_digest,
            source["command_digest"].as_str().unwrap()
        );
        assert!(receipt.posix_coordination_result.is_none());
        let work = projection.required_native_work();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].family, "namespace_requests.completed_preflight");
        assert_eq!(work[0].source_row, source);
    }
    #[test]
    fn retired_preflight_pending_and_prepared_plans_never_create_terminal_outcomes() {
        let pending = old_preflight_request(false);
        let mut projection = seeded();
        projection
            .push_snapshot_row("namespace_requests", pending.clone())
            .unwrap();
        assert!(
            projection
                .current_tables()
                .unwrap()
                .namespace_request_receipts
                .is_empty()
        );
        assert_eq!(
            projection.required_native_work()[0].family,
            "namespace_requests.old_preflight"
        );
        let mut tampered = pending;
        tampered["operation"]["advisory-lock-migration-prepare"]["state_fingerprint"] =
            json!("unrelated-fence");
        assert!(projection.classify_source_request(tampered).is_err());
        let planned = old_preflight_request(true);
        projection.operations.insert("source-prepared".into(),json!({"status":"prepared", "payload":{
            "file_system_id":fs().file_system_id,"shard_id":"root","owner_epoch":1,"operation_id":"source-prepared"
        }}));
        let original = json!({"complete_original_prepare":"retained","completed_request":planned});
        projection
            .retain_prepared_adoption("source-prepared", original.clone(), vec![planned])
            .unwrap();
        assert!(
            projection
                .current_tables()
                .unwrap()
                .namespace_request_receipts
                .is_empty()
        );
        let retained = projection
            .required_native_work()
            .into_iter()
            .find(|work| work.family == "prepared_operations.old_adoption")
            .unwrap();
        assert_eq!(retained.source_row, original);
    }
    #[test]
    fn old_completed_adoption_keeps_exact_receipt_without_executable_offer() {
        let source = old_adoption_request(true);
        assert!(
            wire::normalize_namespace_request(source.clone()).is_err(),
            "current operation requires real source custody"
        );
        let mut projection = seeded();
        projection
            .push_snapshot_row("namespace_requests", source.clone())
            .unwrap();
        let tables = projection.current_tables().unwrap();
        assert!(tables.namespace_requests.is_empty());
        assert!(projection.required_native_work().is_empty());
        assert_eq!(tables.namespace_request_receipts.len(), 1);
        let receipt = &tables.namespace_request_receipts[0];
        assert_eq!(
            receipt.command_digest,
            source["command_digest"].as_str().unwrap()
        );
        assert_eq!(receipt.operation_id, "old-operation");
        assert_eq!(receipt.authority_shard_id, DfsMetadataShardId::new("root"));
        assert_eq!(receipt.result, DfsNamespaceResult::Applied);
        assert_eq!(receipt.created_at, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(
            receipt.retained_at,
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)
        );
        assert!(tables.prepared_operations_v7.is_empty());
    }
    #[test]
    fn old_terminal_adoption_accepts_actual_later_timestamp_but_never_reopens() {
        let source = old_adoption_request(true);
        let mut projection = seeded();
        projection
            .push_snapshot_row("namespace_requests", source.clone())
            .unwrap();
        let mut later = source.clone();
        later["updated_at"] = json!("1970-01-01T00:00:02Z");
        projection
            .push_snapshot_row("namespace_requests", later.clone())
            .unwrap();
        let table = projection.current_tables().unwrap();
        assert_eq!(
            table.namespace_request_receipts[0].retained_at,
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2)
        );
        assert_eq!(
            table.namespace_request_receipts[0].command_digest,
            source["command_digest"].as_str().unwrap()
        );
        let mut changed = later;
        changed["operation_id"] = json!("unrelated-operation");
        assert!(
            projection
                .push_snapshot_row("namespace_requests", changed)
                .is_err()
        );
        assert!(
            projection
                .push_snapshot_row("namespace_requests", old_adoption_request(false))
                .is_err()
        );
        assert!(projection.required_native_work().is_empty());
    }
    #[test]
    fn old_pending_adoption_is_complete_required_work_and_tamper_rejected_first() {
        let source = old_adoption_request(false);
        let mut projection = seeded();
        projection
            .push_snapshot_row("namespace_requests", source.clone())
            .unwrap();
        let work = projection.required_native_work();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].source_row, source);
        let tables = projection.current_tables().unwrap();
        assert!(tables.namespace_requests.is_empty());
        assert!(tables.namespace_request_receipts.is_empty());
        let mut tampered = source;
        tampered["operation"]["adopt-object-at-path"]["source_path"] = json!("/unrelated-source");
        let mut untouched = seeded();
        assert!(
            untouched
                .push_snapshot_row("namespace_requests", tampered)
                .is_err()
        );
        assert!(untouched.required_native_work().is_empty());
    }
    #[test]
    fn old_running_adoption_job_restores_current_data_for_normal_resume() {
        let request = old_adoption_request(false);
        let job = json!({"job_id":"old-running-job","file_system_id":fs().file_system_id,"kind":"rename-directory","status":"running","root_path":"/source/old-directory","target_path":"/docs/file","pending_frames":[{"type":"cross-shard-adopt","moved_object":request["operation"]["adopt-object-at-path"]["object"],"old_path":"/source/old-directory","destination_primary_node_id":NodeId::from_legacy_named("old-adopt-destination").unwrap()}],"processed_object_count":0,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:01Z","completed_at":null,"failure_reason":null});
        let mut projection = seeded();
        projection
            .push_snapshot_row("namespace_jobs", job.clone())
            .unwrap();
        let tables = projection.current_tables().unwrap();
        assert_eq!(tables.namespace_jobs.len(), 1);
        let restored = &tables.namespace_jobs[0];
        assert_eq!(restored, &wire::normalize_namespace_job(job).unwrap());
        assert_eq!(restored.status, DfsNamespaceJobStatus::Running);
        assert!(projection.required_native_work().is_empty());
    }

    #[test]
    fn retained_transaction_outcome_does_not_resurrect_older_snapshot_head() {
        let mut projection = seeded();
        let newest = object("newest");
        projection
            .push_snapshot_row("objects", serde_json::to_value(&newest).unwrap())
            .unwrap();
        let previous = object("previous");
        let tx = DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: "old-terminal-transaction".into(),
            file_system_id: fs().file_system_id,
            objects: vec![previous],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        projection
            .push_snapshot_row("namespace_transactions", serde_json::to_value(tx).unwrap())
            .unwrap();
        let tables = projection.current_tables().unwrap();
        assert_eq!(tables.objects, vec![newest]);
        assert_eq!(tables.namespace_transaction_receipts.len(), 1);
    }
    #[test]
    fn compact_source_outcome_keeps_opaque_digest_and_terminal_identity() {
        let mut projection = seeded();
        let original = json!({"operation_id":"old-op","file_system_id":fs().file_system_id,"shard_id":"root","owner_epoch":4,"operation_digest":"old-opaque-command-digest","transaction_id":"old-tx","status":"committed","created_at":"1970-01-01T00:00:00Z","retained_at":"1970-01-01T00:00:01Z"});
        projection
            .push_snapshot_row("namespace_prepare_receipts", original.clone())
            .unwrap();
        let output = projection.pending_operations().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0]["source_operation_digest"],
            original["operation_digest"]
        );
        assert_eq!(output[0]["source_terminal_receipt"], original);
        assert_eq!(output[0]["compact_terminal"], true);
        assert!(output[0]["source_prepare_sequence"].is_null());
    }
    #[test]
    fn namespaced_conversion_pin_removal_uses_actual_original_storage_key() {
        let mut projection = seeded();
        let record = json!({"file_system_id":fs().file_system_id,"object_id":object("head").object_id,"version":object("head").version,"pin_id":"original-pin","created_at":"1970-01-01T00:00:00Z"});
        projection
            .push_snapshot_row("version_pins", record.clone())
            .unwrap();
        projection.apply_table_op(json!({"type":"remove-version-pin","object_id":record["object_id"],"version":record["version"],"pin_id":"original-pin"}),&|_|Err(invalid("unused"))).unwrap();
        assert!(projection.current_tables().unwrap().version_pins.is_empty());
    }
    #[test]
    fn unknown_source_operations_and_missing_preparation_fail_closed() {
        let mut projection = seeded();
        assert!(
            projection
                .apply_table_op(json!({"type":"future-mutator"}), &|_| Err(invalid(
                    "unused"
                )))
                .is_err()
        );
        assert!(
            projection
                .apply_journal(json!({"sequence":1,"mutation":{"type":"future-mutator"}}))
                .is_err()
        );
        assert!(projection.apply_table_op(json!({"type":"commit-namespace-prepare","operation_id":"missing","operation_digest":"opaque","owner_epoch":2}),&|_|Err(invalid("unused"))).is_err());
    }
}

fn normalize_effect(
    mut value: Value,
    current_import_candidate_count: Option<usize>,
) -> Result<Value> {
    wire::source_identity_fields(&mut value)?;
    match text(&value, "type")?.as_str() {
        "put-file-system" => {
            value["file_system"] =
                serde_json::to_value(wire::normalize_file_system_with_current_import_defaults(
                    field(&value, "file_system")?,
                    current_import_candidate_count,
                )?)
                .map_err(invalid)?
        }
        "put-object" => {
            value["object"] =
                serde_json::to_value(wire::normalize_object(field(&value, "object")?)?)
                    .map_err(invalid)?
        }
        "put-version" => {
            value["version"] =
                serde_json::to_value(wire::normalize_version(field(&value, "version")?)?)
                    .map_err(invalid)?
        }
        "put-intent" => {
            value["intent"] =
                serde_json::to_value(wire::normalize_intent(field(&value, "intent")?)?)
                    .map_err(invalid)?
        }
        "put-intent-receipt" => {
            value["receipt"] =
                serde_json::to_value(wire::normalize_intent_receipt(field(&value, "receipt")?)?)
                    .map_err(invalid)?
        }
        "put-namespace-job" => {
            value["job"] =
                serde_json::to_value(wire::normalize_namespace_job(field(&value, "job")?)?)
                    .map_err(invalid)?
        }
        "put-namespace-job-receipt" => {
            value["receipt"] = serde_json::to_value(wire::normalize_namespace_job_receipt(field(
                &value, "receipt",
            )?)?)
            .map_err(invalid)?
        }
        "put-namespace-request" => {
            value["request"] = serde_json::to_value(wire::normalize_namespace_request(field(
                &value, "request",
            )?)?)
            .map_err(invalid)?
        }
        "put-namespace-request-receipt" => {
            value["receipt"] = serde_json::to_value(wire::normalize_namespace_request_receipt(
                field(&value, "receipt")?,
            )?)
            .map_err(invalid)?
        }
        "put-version-pin" => {
            value["pin"] = serde_json::to_value(wire::normalize_version_pin(field(&value, "pin")?)?)
                .map_err(invalid)?
        }
        "reconcile-cross-shard-pin-transfer" => {
            value["transfer"] =
                serde_json::to_value(wire::normalize_pin_transfer(field(&value, "transfer")?)?)
                    .map_err(invalid)?;
            value["versions"] = Value::Array(
                field(&value, "versions")?
                    .as_array()
                    .ok_or_else(|| invalid("source version list malformed"))?
                    .iter()
                    .cloned()
                    .map(wire::normalize_version)
                    .map(|result| result.and_then(|row| serde_json::to_value(row).map_err(invalid)))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        "finalize-cross-shard-source-eviction" => {
            value["tombstone"] =
                serde_json::to_value(wire::normalize_object(field(&value, "tombstone")?)?)
                    .map_err(invalid)?
        }
        _ => {}
    }
    let _: DfsPreparedMetadataEffect = decode(value.clone())?;
    Ok(value)
}
