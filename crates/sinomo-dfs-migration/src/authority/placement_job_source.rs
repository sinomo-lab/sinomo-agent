//! Released v0/v1 placement DATA; new current work uses the sole normal producer.
//! No writer permission, executor, reader or queue is constructed here.
use super::*;
use serde_json::Value;
use sinomo_dfs::{DfsFileSystemConfig, DfsPlacementScope};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct OriginalPlacementJob {
    pub(super) entry: KvEntry,
    pub(super) original_value: Value,
    #[serde(default)]
    pub(super) source_schema: OriginalPlacementSchema,
    pub(super) raw_file_system_id: String,
    pub(super) job_id: String,
    pub(super) file_system_id: sinomo_dfs::DfsFileSystemId,
    pub(super) path: String,
    pub(super) scope: DfsPlacementScope,
    pub(super) state: OriginalPlacementState,
    pub(super) terminal: bool,
    pub(super) counters: OriginalPlacementCounters,
    pub(super) revision: u64,
    pub(super) source_created_at_unix_nanos: u64,
    pub(super) source_updated_at_unix_nanos: u64,
    pub(super) cursor: Option<Value>,
    pub(super) last_error: Option<String>,
}
/// Source generation is DATA. It chooses only exact source keys/digest domains.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(super) enum OriginalPlacementSchema {
    #[default]
    V0,
    V1Initial,
    V1Adaptive,
}
impl OriginalPlacementSchema {
    fn job_family(self) -> &'static str {
        match self {
            Self::V0 => "placement-reconciliations",
            _ => "placement-reconciliations-v1",
        }
    }
    fn source_domain(self) -> &'static str {
        match self {
            Self::V0 => "original-placement-job-v0",
            _ => "original-placement-job-v1",
        }
    }
    fn capsule_kind(self) -> &'static str {
        match self {
            Self::V0 => "placement-job-v0",
            _ => "placement-job-v1",
        }
    }
    fn reservation_domain(self) -> &'static str {
        match self {
            Self::V0 => "original-placement-reservation-v0",
            _ => "original-placement-reservation-v1",
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(super) enum OriginalPlacementState {
    Pending,
    Scanning,
    Repairing,
    Completed,
    Failed,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalPlacementCounters {
    pub total_object_count: u64,
    pub scanned_object_count: u64,
    pub matched_object_count: u64,
    pub updated_object_count: u64,
    pub scheduled_repair_count: u64,
    pub completed_repair_count: u64,
    pub pending_repair_count: u64,
}
fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|text| !text.is_empty() && !text.chars().any(char::is_control))
        .ok_or_else(|| error(format!("original placement lacks {field}")))
}
fn integer(value: &Value, field: &str) -> Result<u64> {
    value[field].as_u64().ok_or_else(|| {
        error(format!(
            "original placement {field} is not a required unsigned counter/clock"
        ))
    })
}
fn normalized_source_id(raw: &str) -> Result<sinomo_core::Id> {
    sinomo_core::Id::new(raw.to_owned())
        .or_else(|_| sinomo_core::Id::from_legacy_named(raw))
        .map_err(error)
}
fn required_object<'a>(
    value: &'a Value,
    field: &str,
) -> Result<&'a serde_json::Map<String, Value>> {
    value[field]
        .as_object()
        .ok_or_else(|| error(format!("original placement lacks complete {field} object")))
}
fn original_time(value: &Value, field: &str) -> Result<()> {
    time::OffsetDateTime::parse(
        text(value, field)?,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(error)
    .map(drop)
}
fn original_node_ids(value: &Value, field: &str) -> Result<BTreeSet<sinomo_core::node::NodeId>> {
    let Some(values) = value.get(field) else {
        return Ok(BTreeSet::new());
    };
    let values = values.as_array().ok_or_else(|| {
        error(format!(
            "original placement {field} is not a recorded node array"
        ))
    })?;
    let mut ids = BTreeSet::new();
    for value in values {
        let raw = value
            .as_str()
            .ok_or_else(|| error("original placement node identity is not text"))?;
        if !ids.insert(normalized_source_id(raw)?) {
            return Err(error("original placement repeats a recorded node identity"));
        }
    }
    Ok(ids)
}
fn recorded_optional_integer(value: &Value, field: &str) -> Result<()> {
    if value.get(field).is_some_and(|row| !row.is_null()) {
        integer(value, field)?;
    }
    Ok(())
}
fn recorded_optional_bool(value: &Value, field: &str) -> Result<()> {
    if value.get(field).is_some_and(|row| !row.is_boolean()) {
        return Err(error(format!(
            "original snapshot {field} is not a recorded boolean"
        )));
    }
    Ok(())
}
fn validate_original_snapshot(snapshot: &Value) -> Result<()> {
    let fields = snapshot
        .as_object()
        .ok_or_else(|| error("original planner snapshot is not an object"))?;
    if snapshot["schema_version"].as_u64() != Some(1)
        || fields.keys().any(|field| {
            ![
                "schema_version",
                "generation",
                "primary_node_id",
                "observed_at",
                "expires_at",
                "nodes",
                "paths",
            ]
            .contains(&field.as_str())
        })
    {
        return Err(error("unknown original planner snapshot schema/shape"));
    }
    for family in ["nodes", "paths"] {
        let Some(rows) = snapshot.get(family) else {
            continue;
        };
        for row in rows
            .as_array()
            .ok_or_else(|| error("original planner observations are not an array"))?
        {
            let fields = row
                .as_object()
                .ok_or_else(|| error("original planner observation is not an object"))?;
            original_time(row, "observed_at")?;
            original_time(row, "expires_at")?;
            if family == "nodes" {
                if fields.keys().any(|field| {
                    ![
                        "node_id",
                        "zone",
                        "storage_health",
                        "storage_freshness",
                        "accepts_new_content",
                        "capacity_weight",
                        "usable_capacity_bytes",
                        "reserved_free_bytes",
                        "content_location_count",
                        "network_metering",
                        "resource_pressure",
                        "observed_at",
                        "expires_at",
                    ]
                    .contains(&field.as_str())
                }) {
                    return Err(error("unknown original node observation shape"));
                }
                normalized_source_id(text(row, "node_id")?)?;
                recorded_optional_bool(row, "accepts_new_content")?;
                for field in [
                    "capacity_weight",
                    "usable_capacity_bytes",
                    "reserved_free_bytes",
                    "content_location_count",
                ] {
                    recorded_optional_integer(row, field)?;
                }
                if let Some(health) = row.get("storage_health") {
                    serde_json::from_value::<sinomo_dfs::DfsHealthState>(health.clone())
                        .map_err(error)?;
                }
                if let Some(freshness) = row.get("storage_freshness") {
                    serde_json::from_value::<sinomo_dfs::DfsObservationFreshness>(
                        freshness.clone(),
                    )
                    .map_err(error)?;
                }
                if let Some(metering) = row.get("network_metering") {
                    serde_json::from_value::<sinomo_dfs::DfsNetworkMetering>(metering.clone())
                        .map_err(error)?;
                }
                if let Some(pressure) = row.get("resource_pressure") {
                    serde_json::from_value::<sinomo_dfs::DfsResourcePressure>(pressure.clone())
                        .map_err(error)?;
                }
                if row
                    .get("zone")
                    .is_some_and(|zone| !zone.is_null() && !zone.is_string())
                {
                    return Err(error("original node zone has wrong type"));
                }
            } else {
                if fields.keys().any(|field| {
                    ![
                        "source_node_id",
                        "target_node_id",
                        "state",
                        "latency_micros",
                        "delivered_bytes_per_second",
                        "metering",
                        "observed_at",
                        "expires_at",
                    ]
                    .contains(&field.as_str())
                }) {
                    return Err(error("unknown original path observation shape"));
                }
                normalized_source_id(text(row, "source_node_id")?)?;
                normalized_source_id(text(row, "target_node_id")?)?;
                for field in ["latency_micros", "delivered_bytes_per_second"] {
                    recorded_optional_integer(row, field)?;
                }
                if let Some(state) = row.get("state") {
                    serde_json::from_value::<sinomo_dfs::DfsPlacementPathState>(state.clone())
                        .map_err(error)?;
                }
                if let Some(metering) = row.get("metering") {
                    serde_json::from_value::<sinomo_dfs::DfsNetworkMetering>(metering.clone())
                        .map_err(error)?;
                }
            }
        }
    }
    Ok(())
}
fn source_schema(value: &Value) -> Result<OriginalPlacementSchema> {
    match value.get("schema_version") {
        None => Ok(OriginalPlacementSchema::V0),
        Some(version) if version.as_u64() == Some(1) => {
            let later = [
                "adaptive",
                "processed_content_bytes",
                "placement_snapshot",
                "adaptive_budget",
            ]
            .iter()
            .any(|field| value.get(*field).is_some());
            if !later {
                return Ok(OriginalPlacementSchema::V1Initial);
            }
            let adaptive = value["adaptive"]
                .as_bool()
                .ok_or_else(|| error("later original placement lacks required adaptive flag"))?;
            integer(value, "processed_content_bytes")?;
            if let Some(snapshot) = value.get("placement_snapshot").filter(|row| !row.is_null()) {
                required_object(value, "placement_snapshot")?;
                integer(snapshot, "schema_version")?;
                integer(snapshot, "generation")?;
                normalized_source_id(text(snapshot, "primary_node_id")?)?;
                original_time(snapshot, "observed_at")?;
                original_time(snapshot, "expires_at")?;
                validate_original_snapshot(snapshot)?;
            }
            if let Some(budget) = value.get("adaptive_budget").filter(|row| !row.is_null()) {
                let fields = required_object(value, "adaptive_budget")?;
                if fields.len() != 3 {
                    return Err(error("unknown original adaptive budget shape"));
                }
                for field in [
                    "max_objects_per_tick",
                    "max_bytes_per_tick",
                    "max_millis_per_tick",
                ] {
                    if integer(budget, field)? == 0 {
                        return Err(error("original adaptive budget has a zero limit"));
                    }
                }
            }
            if adaptive
                && (value.get("placement_snapshot").is_none_or(Value::is_null)
                    || value.get("adaptive_budget").is_none_or(Value::is_null))
            {
                return Err(error(
                    "adaptive original placement lacks its actual snapshot/budget",
                ));
            }
            Ok(OriginalPlacementSchema::V1Adaptive)
        }
        _ => Err(error("unknown original placement schema version")),
    }
}
fn validate_original_v1(value: &Value, raw_fs: &str) -> Result<()> {
    let config = required_object(value, "accepted_config")?;
    if config.get("file_system_id").and_then(Value::as_str) != Some(raw_fs) {
        return Err(error(
            "original accepted config belongs to another filesystem",
        ));
    }
    if !value["accepted_config"]["display_name"].is_string() {
        return Err(error("original accepted config lacks display_name"));
    }
    normalized_source_id(text(&value["accepted_config"], "owner_node_id")?)?;
    if let Some(rules) = config.get("subtree_policies") {
        for rule in rules
            .as_array()
            .ok_or_else(|| error("original accepted policy list is malformed"))?
        {
            if !rule.is_object() {
                return Err(error("original accepted policy is not an object"));
            }
            text(rule, "path_prefix")?;
            serde_json::from_value::<sinomo_core::DfsObjectPolicy>(rule["policy"].clone())
                .map_err(error)?;
            if let Some(scope) = rule.get("scope") {
                serde_json::from_value::<DfsPlacementScope>(scope.clone()).map_err(error)?;
            }
            // Option counts in fbfb5621 were legitimately absent/null. Never
            // supply their historical W/N from current defaults here.
            for field in [
                "min_commit_replicas",
                "desired_replicas",
                "min_distinct_zones",
            ] {
                if rule.get(field).is_some_and(|row| !row.is_null()) {
                    integer(rule, field)?;
                }
            }
            original_node_ids(rule, "required_node_ids")?;
            original_node_ids(rule, "forbidden_node_ids")?;
        }
    }
    let candidates = value["candidate_snapshot"]
        .as_array()
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| error("original v1 placement lacks its nonempty candidate snapshot"))?;
    for candidate in candidates {
        if !candidate.is_object() {
            return Err(error("original placement candidate is not an object"));
        }
        normalized_source_id(text(candidate, "node_id")?)?;
        original_time(candidate, "updated_at")?;
        for field in ["accepts_metadata", "accepts_content"] {
            if !candidate[field].is_boolean() {
                return Err(error(format!("original candidate lacks {field}")));
            }
        }
        integer(candidate, "capacity_weight")?;
        // Later advisory fields are preserved verbatim. Never translate their
        // absence into invented node/device observations.
    }
    original_node_ids(value, "unavailable_node_ids")?;
    Ok(())
}
impl OriginalPlacementJob {
    pub(super) fn decode(entry: &KvEntry) -> Result<Self> {
        // Released producers: a5b262c4 v0, fbfb5621 v1, and 0e3044c1
        // adaptive v1. Counters/clocks remain REQUIRED across generations;
        // genuine optional original fields stay exact archived DATA.
        let value = crate::metadata::source_document::decode_original_document(&entry.value)?;
        let job_id = text(&value, "job_id")?.to_owned();
        let raw_fs = text(&value, "file_system_id")?.to_owned();
        let fs = normalized_source_id(&raw_fs)?;
        let source_schema = source_schema(&value)?;
        let expected = format!(
            "/dfs/filesystems/{raw_fs}/{}/{}",
            source_schema.job_family(),
            job_id.trim_matches('/').replace('/', "_")
        );
        if entry.key != expected {
            return Err(error("original placement job/key/schema identity differs"));
        }
        if source_schema != OriginalPlacementSchema::V0 {
            validate_original_v1(&value, &raw_fs)?;
        }
        let path = text(&value, "path")?.to_owned();
        if !path.starts_with('/') {
            return Err(error("original placement path is not absolute"));
        }
        let scope = match text(&value, "scope")? {
            "self-only" => DfsPlacementScope::SelfOnly,
            "subtree" => DfsPlacementScope::Subtree,
            _ => return Err(error("unknown original placement scope")),
        };
        let state = match text(&value, "state")? {
            "pending" => OriginalPlacementState::Pending,
            "scanning" => OriginalPlacementState::Scanning,
            "repairing" => OriginalPlacementState::Repairing,
            "completed" => OriginalPlacementState::Completed,
            "failed" => OriginalPlacementState::Failed,
            _ => return Err(error("unknown original placement state")),
        };
        let counters = OriginalPlacementCounters {
            total_object_count: integer(&value, "total_object_count")?,
            scanned_object_count: integer(&value, "scanned_object_count")?,
            matched_object_count: integer(&value, "matched_object_count")?,
            updated_object_count: integer(&value, "updated_object_count")?,
            scheduled_repair_count: integer(&value, "scheduled_repair_count")?,
            completed_repair_count: integer(&value, "completed_repair_count")?,
            pending_repair_count: integer(&value, "pending_repair_count")?,
        };
        if counters.matched_object_count > counters.scanned_object_count
            || counters.updated_object_count > counters.matched_object_count
            || counters.completed_repair_count > counters.scheduled_repair_count
        {
            return Err(error(
                "original placement counters contradict the released producer",
            ));
        }
        // The old live scanner could see growth after its initial count, and
        // repairing recomputed pending work from current rows. Do not invent
        // scanned<=total or pending<=scheduled invariants. Wall clocks could
        // regress; both exact required timestamps stay original DATA.
        let revision = integer(&value, "revision")?;
        let created = integer(&value, "created_at_unix_nanos")?;
        let updated = integer(&value, "updated_at_unix_nanos")?;
        let cursor = match value.get("cursor") {
            None | Some(Value::Null) => None,
            Some(cursor) => {
                text(cursor, "path")?;
                normalized_source_id(text(cursor, "object_id")?)?;
                if let Some(shard) = cursor.get("shard_id") {
                    if !shard.is_null() {
                        text(cursor, "shard_id")?;
                    }
                }
                Some(cursor.clone())
            }
        };
        let last_error = match value.get("last_error") {
            None | Some(Value::Null) => None,
            Some(Value::String(message)) => Some(message.clone()),
            _ => return Err(error("original placement last_error has wrong type")),
        };
        Ok(Self {
            entry: entry.clone(),
            original_value: value,
            source_schema,
            raw_file_system_id: raw_fs,
            job_id,
            file_system_id: fs,
            path,
            scope,
            state,
            terminal: matches!(
                state,
                OriginalPlacementState::Completed | OriginalPlacementState::Failed
            ),
            counters,
            revision,
            source_created_at_unix_nanos: created,
            source_updated_at_unix_nanos: updated,
            cursor,
            last_error,
        })
    }
    /// Current strict rows retain the normal reader/writer. Only exact released
    /// source generations which need nested DTO conversion enter this archive.
    pub(super) fn requires_conversion(entry: &KvEntry) -> Result<bool> {
        let value = crate::metadata::source_document::decode_original_document(&entry.value)?;
        let generation = source_schema(&value)?;
        let raw_fs = text(&value, "file_system_id")?;
        let job_id = text(&value, "job_id")?;
        let expected = format!(
            "/dfs/filesystems/{raw_fs}/{}/{}",
            generation.job_family(),
            job_id.trim_matches('/').replace('/', "_")
        );
        if entry.key != expected {
            return Err(error("original placement job/key/schema identity differs"));
        }
        if generation == OriginalPlacementSchema::V0 {
            return Ok(true);
        }
        if let Ok(current) = entry.decode::<sinomo_dfs::DfsPlacementReconciliationJob>() {
            if current.schema_version == 1
                && !current.candidate_snapshot.is_empty()
                && current.accepted_config.file_system_id == current.file_system_id
            {
                return Ok(false);
            }
        }
        if serde_json::from_value::<DfsFileSystemConfig>(value["accepted_config"].clone()).is_ok() {
            return Err(error(
                "current v1 job is malformed outside the released nested-config source boundary",
            ));
        }
        Ok(true)
    }
    pub(super) fn related_keys(&self) -> Vec<String> {
        if self.source_schema == OriginalPlacementSchema::V0 {
            return Vec::new();
        }
        let base = format!("/dfs/filesystems/{}", self.raw_file_system_id);
        vec![
            format!("{base}/placement-reconciliation-queue-v1"),
            format!(
                "{base}/placement-reconciliation-activity-v1/{}",
                self.job_id.trim_matches('/').replace('/', "_")
            ),
            format!("{base}/placement-reconciliation-activity-index-v1"),
        ]
    }
    pub(super) fn exact_retirement_compares(
        captured: &[MetadataCapturedKvEntry],
    ) -> Vec<KvCompare> {
        let mut compares = captured_compares(captured);
        for row in captured {
            if let Some(entry) = &row.entry {
                compares.push(KvCompare::CreateRevisionEquals {
                    key: row.key.clone(),
                    revision: entry.create_revision,
                });
                compares.push(KvCompare::VersionEquals {
                    key: row.key.clone(),
                    version: entry.version,
                });
            }
        }
        compares
    }
    /// Remove only this exact archived source identity from shared scheduling
    /// DATA. The caller compares and archives all supplied entries natively.
    pub(super) fn related_retirement_ops(
        &self,
        captured: &[MetadataCapturedKvEntry],
    ) -> Result<Vec<KvOp>> {
        let keys = self.related_keys();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut ops = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let entry = captured
                .iter()
                .find(|row| &row.key == key)
                .ok_or_else(|| error("placement retirement omits exact related key"))?;
            let Some(entry) = &entry.entry else { continue };
            let mut value =
                crate::metadata::source_document::decode_original_document(&entry.value)?;
            if !value.is_object() || value["schema_version"].as_u64() != Some(1) {
                return Err(error("unknown original placement queue/activity schema"));
            }
            let changed = match index {
                0 => {
                    let ids = value["job_ids"]
                        .as_array_mut()
                        .ok_or_else(|| error("original placement queue lacks job_ids"))?;
                    for id in ids.iter() {
                        if id.as_str().is_none() {
                            return Err(error("original placement queue identity is malformed"));
                        }
                    }
                    let before = ids.len();
                    ids.retain(|id| id.as_str() != Some(self.job_id.as_str()));
                    before != ids.len()
                }
                1 => {
                    let activity = &value["activity"];
                    if text(activity, "job_id")? != self.job_id
                        || text(activity, "file_system_id")? != self.raw_file_system_id
                        || text(activity, "path")? != self.path
                        || activity["scope"] != self.original_value["scope"]
                    {
                        return Err(error("original placement activity changes job identity"));
                    }
                    ops.push(KvOp::Delete { key: key.clone() });
                    false
                }
                2 => {
                    let recent = value["recent"].as_array_mut().ok_or_else(|| {
                        error("original placement activity index lacks recent cursors")
                    })?;
                    for cursor in recent.iter() {
                        let id = text(cursor, "job_id")?;
                        let created = integer(cursor, "created_at_unix_nanos")?;
                        if id == self.job_id && created != self.source_created_at_unix_nanos {
                            return Err(error(
                                "original placement activity cursor changes immutable creation identity",
                            ));
                        }
                    }
                    let before = recent.len();
                    recent.retain(|cursor| cursor["job_id"].as_str() != Some(self.job_id.as_str()));
                    before != recent.len()
                }
                _ => unreachable!(),
            };
            if changed {
                ops.push(KvOp::Put {
                    key: key.clone(),
                    value: sinomo_core::kv::encode_kv_value(&value).map_err(error)?,
                    lease_id: entry.lease_id,
                });
            }
        }
        Ok(ops)
    }
    pub(super) fn source_digest(&self) -> Result<String> {
        digest(&(self.source_schema.source_domain(), &self.entry))
    }
    pub(super) fn validate_current_goal(
        &self,
        job: &sinomo_dfs::DfsPlacementReconciliationJob,
    ) -> Result<()> {
        if self.terminal
            || job.schema_version != 1
            || job.job_id != self.current_job_id()?
            || job.file_system_id != self.file_system_id
            || job.path != self.path
            || job.scope != self.scope
            || job.unavailable_node_ids
                != original_node_ids(&self.original_value, "unavailable_node_ids")?
            || job.candidate_snapshot.is_empty()
            || job.accepted_config.file_system_id != self.file_system_id
            || job.adaptive != self.is_adaptive()
        {
            return Err(error(
                "current placement job changes its immutable original DATA goal",
            ));
        }
        if self.is_adaptive() {
            if job.adaptive_budget.as_ref() != Some(&self.current_adaptive_budget()?)
                || job.placement_snapshot.is_none()
                || job.processed_content_bytes
                    < integer(&self.original_value, "processed_content_bytes")?
            {
                return Err(error(
                    "current adaptive goal loosens original budget/spent accounting",
                ));
            }
        } else if job.adaptive_budget.is_some() {
            return Err(error("manual source changes execution class"));
        }
        Ok(())
    }
    pub(super) fn current_job_id(&self) -> Result<String> {
        Ok(format!("placement-import-v7-{}", self.source_digest()?))
    }
    pub(super) fn current_job_key(&self) -> Result<String> {
        Ok(sinomo_dfs::placement_reconciliation_key(
            &self.file_system_id,
            &self.current_job_id()?,
        ))
    }
    pub(super) fn capsule_for_scope(&self, assignment: &DfsMetadataShardConfig) -> Result<Value> {
        if self.terminal
            || assignment.file_system_id != self.file_system_id
            || assignment.shard_id.is_empty()
        {
            return Err(error(
                "terminal/foreign/empty scope cannot reserve original placement work",
            ));
        }
        let capsule_digest = digest(&(
            self.source_schema.reservation_domain(),
            &self.entry,
            &assignment.file_system_id,
            &assignment.shard_id,
        ))?;
        Ok(
            serde_json::json!({"capsule_kind":self.source_schema.capsule_kind(),"capsule_digest":capsule_digest,
            "source_entry_digest":self.source_digest()?,"source_row":self.entry,
            "original_source_scope":{"file_system_id":assignment.file_system_id,"shard_id":assignment.shard_id},
            "disposition":"data-after-original-reservation-retirement","required_native_work":"current-placement-reissue"}),
        )
    }
    pub(crate) fn verify_capsule(
        row: &Value,
    ) -> Result<crate::metadata::MetadataDataReservationRef> {
        if !matches!(
            row["capsule_kind"].as_str(),
            Some("placement-job-v0" | "placement-job-v1")
        ) || row["disposition"].as_str() != Some("data-after-original-reservation-retirement")
            || row["required_native_work"].as_str() != Some("current-placement-reissue")
        {
            return Err(error(
                "original placement capsule family/disposition differs",
            ));
        }
        let entry: KvEntry = serde_json::from_value(row["source_row"].clone()).map_err(error)?;
        let source = Self::decode(&entry)?;
        if row["capsule_kind"].as_str() != Some(source.source_schema.capsule_kind())
            || source.terminal
            || row["source_entry_digest"].as_str() != Some(source.source_digest()?.as_str())
        {
            return Err(error(
                "original placement capsule source/state/digest changed",
            ));
        }
        let scope = &row["original_source_scope"];
        if scope.as_object().is_none_or(|scope| {
            scope.len() != 2
                || !scope.contains_key("file_system_id")
                || !scope.contains_key("shard_id")
        }) {
            return Err(error(
                "original placement scope has extra or missing identity fields",
            ));
        }
        let fs: sinomo_dfs::DfsFileSystemId =
            serde_json::from_value(scope["file_system_id"].clone()).map_err(error)?;
        let shard = sinomo_dfs::DfsMetadataShardId::new(text(scope, "shard_id")?);
        if fs != source.file_system_id {
            return Err(error("original placement capsule changes filesystem"));
        }
        let actual = digest(&(
            source.source_schema.reservation_domain(),
            &entry,
            &fs,
            &shard.0,
        ))?;
        if row["capsule_digest"].as_str() != Some(actual.as_str()) {
            return Err(error(
                "original placement capsule changes original entry or scope",
            ));
        }
        Ok(crate::metadata::MetadataDataReservationRef {
            file_system_id: fs,
            original_shard_id: shard,
            capsule_digest: actual,
        })
    }
    pub(super) fn is_adaptive(&self) -> bool {
        self.source_schema == OriginalPlacementSchema::V1Adaptive
            && self.original_value["adaptive"].as_bool() == Some(true)
    }
    fn current_adaptive_budget(&self) -> Result<sinomo_dfs::DfsAdaptivePlacementBudget> {
        if !self.is_adaptive() {
            return Err(error("manual source has no adaptive execution budget"));
        }
        let budget = &self.original_value["adaptive_budget"];
        Ok(sinomo_dfs::DfsAdaptivePlacementBudget {
            max_objects_per_tick: usize::try_from(integer(budget, "max_objects_per_tick")?)
                .map_err(error)?,
            max_bytes_per_tick: integer(budget, "max_bytes_per_tick")?,
            max_millis_per_tick: integer(budget, "max_millis_per_tick")?,
        })
    }
    pub(super) fn build_current_job(
        &self,
        config: &DfsFileSystemConfig,
        candidates: Vec<sinomo_dfs::DfsNodeAdvertisement>,
        current_snapshot: Option<sinomo_dfs::DfsPlacementSnapshot>,
        current_time: u64,
    ) -> Result<sinomo_dfs::DfsPlacementReconciliationJob> {
        if self.terminal || config.file_system_id != self.file_system_id {
            return Err(error(
                "terminal/foreign original goal cannot create current work",
            ));
        }
        if self.is_adaptive() {
            let snapshot = current_snapshot.ok_or_else(|| {
                error("adaptive source reissue awaits an actual current host snapshot")
            })?;
            return sinomo_dfs::build_adaptive_placement_reconciliation_job(
                self.current_job_id()?,
                config,
                self.path.clone(),
                self.scope,
                original_node_ids(&self.original_value, "unavailable_node_ids")?,
                candidates,
                snapshot,
                self.current_adaptive_budget()?,
                integer(&self.original_value, "processed_content_bytes")?,
                current_time,
            )
            .map_err(error);
        }
        sinomo_dfs::build_placement_reconciliation_job(
            self.current_job_id()?,
            config,
            self.path.clone(),
            self.scope,
            original_node_ids(&self.original_value, "unavailable_node_ids")?,
            candidates,
            None,
            current_time,
        )
        .map_err(error)
    }
}

#[cfg(test)]
mod original_source_tests {
    use super::*;
    use sinomo_core::object::{LengthMode, serde_ext};
    // Faithful first-release source schema/field order, independent of the
    // strict current placement DTO. These fixtures grant no native authority.
    #[derive(Serialize)]
    struct V0Cursor<'a> {
        path: &'a str,
        object_id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        shard_id: Option<&'a str>,
    }
    #[derive(Serialize)]
    struct V0Job<'a> {
        job_id: &'a str,
        file_system_id: &'a str,
        path: &'a str,
        scope: &'a str,
        state: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor: Option<V0Cursor<'a>>,
        total_object_count: usize,
        scanned_object_count: usize,
        matched_object_count: usize,
        updated_object_count: usize,
        scheduled_repair_count: usize,
        completed_repair_count: usize,
        pending_repair_count: usize,
        revision: u64,
        created_at_unix_nanos: u64,
        updated_at_unix_nanos: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_error: Option<&'a str>,
    }
    fn wire(state: &str) -> V0Job<'_> {
        V0Job {
            job_id: "old-job",
            file_system_id: "old-source-fs",
            path: "/users/team",
            scope: "subtree",
            state,
            cursor: Some(V0Cursor {
                path: "/users/team/file",
                object_id: "old-object",
                shard_id: Some("root"),
            }),
            total_object_count: 1,
            scanned_object_count: 3,
            matched_object_count: 2,
            updated_object_count: 1,
            scheduled_repair_count: 1,
            completed_repair_count: 0,
            pending_repair_count: 4,
            revision: 9,
            created_at_unix_nanos: 42,
            updated_at_unix_nanos: 7,
            last_error: Some("original error\nwith context"),
        }
    }
    fn entry(bytes: Vec<u8>) -> KvEntry {
        KvEntry {
            key: "/dfs/filesystems/old-source-fs/placement-reconciliations/old-job".into(),
            value: bytes,
            create_revision: sinomo_core::kv::KvRevision(8),
            modify_revision: sinomo_core::kv::KvRevision(12),
            version: sinomo_core::kv::KvVersion(2),
            lease_id: Some(sinomo_core::kv::LeaseId(17)),
        }
    }
    fn original() -> KvEntry {
        let input = serde_ext::to_bytes(&wire("repairing"), 1, LengthMode::U32).unwrap();
        let dir = sinomo_testdir::TestDir::new("placement-original-v0-data");
        let path = dir.join("original.sobj");
        std::fs::write(&path, &input).unwrap();
        let row = entry(std::fs::read(&path).unwrap());
        assert_eq!(row.value, input);
        row
    }
    #[test]
    fn original_sobj_keeps_goal_state_raw_entry_and_valid_live_growth() {
        let row = original();
        let job = OriginalPlacementJob::decode(&row).unwrap();
        assert_eq!(job.entry, row);
        assert_eq!(job.state, OriginalPlacementState::Repairing);
        assert!(!job.terminal);
        assert_eq!(job.scope, DfsPlacementScope::Subtree);
        assert_eq!(job.path, "/users/team");
        assert_eq!(job.counters.scanned_object_count, 3);
        assert_eq!(job.counters.total_object_count, 1);
        assert_eq!(job.counters.pending_repair_count, 4);
        assert_eq!(job.source_updated_at_unix_nanos, 7);
        assert_eq!(job.source_created_at_unix_nanos, 42);
        assert_eq!(
            job.last_error.as_deref(),
            Some("original error\nwith context")
        );
        assert_eq!(
            job.original_value,
            crate::metadata::source_document::decode_original_document(&row.value).unwrap()
        );
        let restored: OriginalPlacementJob =
            serde_json::from_slice(&serde_json::to_vec(&job).unwrap()).unwrap();
        assert_eq!(restored.entry, row);
        assert_eq!(restored.original_value, job.original_value);
        assert_eq!(
            restored.current_job_id().unwrap(),
            job.current_job_id().unwrap()
        );
    }
    #[test]
    fn required_original_counters_and_exact_key_reject_malformed_source() {
        let original = original();
        let value =
            crate::metadata::source_document::decode_original_document(&original.value).unwrap();
        for field in [
            "total_object_count",
            "scanned_object_count",
            "matched_object_count",
            "updated_object_count",
            "scheduled_repair_count",
            "completed_repair_count",
            "pending_repair_count",
            "revision",
            "created_at_unix_nanos",
            "updated_at_unix_nanos",
        ] {
            for bad in [
                None,
                Some(serde_json::json!(-1)),
                Some(serde_json::json!("9")),
                Some(serde_json::json!(1.5)),
            ] {
                let mut changed = value.clone();
                match bad {
                    Some(value) => {
                        changed.as_object_mut().unwrap().insert(field.into(), value);
                    }
                    None => {
                        changed.as_object_mut().unwrap().remove(field);
                    }
                }
                let mut row = original.clone();
                row.value = serde_ext::to_bytes(&changed, 1, LengthMode::U32).unwrap();
                assert!(
                    OriginalPlacementJob::decode(&row).is_err(),
                    "malformed {field}"
                );
            }
        }
        let mut wrong = original.clone();
        wrong.key.push('x');
        assert!(OriginalPlacementJob::decode(&wrong).is_err());
        for (field, bad) in [
            ("scope", serde_json::json!("unknown")),
            ("state", serde_json::json!("superseded")),
            ("path", serde_json::json!("relative")),
            ("matched_object_count", serde_json::json!(4)),
            ("updated_object_count", serde_json::json!(3)),
            ("completed_repair_count", serde_json::json!(2)),
        ] {
            let mut changed = value.clone();
            changed.as_object_mut().unwrap().insert(field.into(), bad);
            let mut row = original.clone();
            row.value = serde_ext::to_bytes(&changed, 1, LengthMode::U32).unwrap();
            assert!(OriginalPlacementJob::decode(&row).is_err());
        }
    }
    #[test]
    fn terminal_original_outcomes_are_data_and_new_keys_are_stable() {
        for state in ["completed", "failed"] {
            let row = entry(serde_ext::to_bytes(&wire(state), 1, LengthMode::U32).unwrap());
            let source = OriginalPlacementJob::decode(&row).unwrap();
            assert!(source.terminal);
            assert_eq!(source.original_value["state"], state);
            assert_eq!(source.entry, row);
            assert_eq!(
                source.current_job_key().unwrap(),
                sinomo_dfs::placement_reconciliation_key(
                    &source.file_system_id,
                    &source.current_job_id().unwrap()
                )
            );
        }
    }
    #[test]
    fn capsule_binds_exact_original_bytes_revision_lease_and_one_actual_scope() {
        let source = OriginalPlacementJob::decode(&original()).unwrap();
        let assignment = DfsMetadataShardConfig {
            file_system_id: source.file_system_id.clone(),
            shard_id: "root".into(),
            primary_node_id: sinomo_core::node::NodeId::from_legacy_named("source-owner-data")
                .unwrap(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let capsule = source.capsule_for_scope(&assignment).unwrap();
        let actual = OriginalPlacementJob::verify_capsule(&capsule).unwrap();
        assert_eq!(actual.file_system_id, source.file_system_id);
        assert_eq!(actual.original_shard_id.0, "root");
        for field in ["modify_revision", "lease_id"] {
            let mut changed = capsule.clone();
            changed["source_row"][field] = serde_json::json!(99);
            assert!(OriginalPlacementJob::verify_capsule(&changed).is_err());
        }
        let mut changed = capsule.clone();
        changed["original_source_scope"]["shard_id"] = serde_json::json!("other");
        assert!(OriginalPlacementJob::verify_capsule(&changed).is_err());
        let mut changed = capsule.clone();
        changed["original_source_scope"]["owner_epoch"] = serde_json::json!(999);
        assert!(OriginalPlacementJob::verify_capsule(&changed).is_err());
        let mut assignment2 = assignment.clone();
        assignment2.shard_id = "other".into();
        let capsule2 = source.capsule_for_scope(&assignment2).unwrap();
        assert_ne!(capsule["capsule_digest"], capsule2["capsule_digest"]);
        assert_eq!(
            OriginalPlacementJob::verify_capsule(&capsule2)
                .unwrap()
                .original_shard_id
                .0,
            "other"
        );
    }
    // Exact fbfb5621 field schemas, independent from current DTO/defaults.
    #[derive(Serialize)]
    struct V1Policy<'a> {
        path_prefix: &'a str,
        policy: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_commit_replicas: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        desired_replicas: Option<usize>,
    }
    #[derive(Serialize)]
    struct V1Config<'a> {
        file_system_id: &'a str,
        display_name: &'a str,
        owner_node_id: &'a str,
        subtree_policies: Vec<V1Policy<'a>>,
    }
    #[derive(Serialize)]
    struct V1Candidate<'a> {
        node_id: &'a str,
        capacity_weight: u32,
        accepts_metadata: bool,
        accepts_content: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        zone: Option<&'a str>,
        updated_at: &'a str,
    }
    #[derive(Serialize)]
    struct V1Job<'a> {
        schema_version: u32,
        job_id: &'a str,
        file_system_id: &'a str,
        path: &'a str,
        scope: &'a str,
        unavailable_node_ids: Vec<&'a str>,
        candidate_snapshot: Vec<V1Candidate<'a>>,
        accepted_config: V1Config<'a>,
        state: &'a str,
        total_object_count: usize,
        scanned_object_count: usize,
        matched_object_count: usize,
        updated_object_count: usize,
        scheduled_repair_count: usize,
        completed_repair_count: usize,
        pending_repair_count: usize,
        revision: u64,
        created_at_unix_nanos: u64,
        updated_at_unix_nanos: u64,
    }
    fn first_v1() -> KvEntry {
        let fs = sinomo_dfs::sifs_file_system_id();
        let node = sinomo_core::node::NodeId::from_legacy_named("source-candidate-data").unwrap();
        let unavailable =
            sinomo_core::node::NodeId::from_legacy_named("source-unavailable-data").unwrap();
        let source = V1Job {
            schema_version: 1,
            job_id: "original-v1",
            file_system_id: fs.as_str(),
            path: "/users",
            scope: "subtree",
            unavailable_node_ids: vec![unavailable.as_str()],
            candidate_snapshot: vec![V1Candidate {
                node_id: node.as_str(),
                capacity_weight: 2,
                accepts_metadata: false,
                accepts_content: true,
                zone: None,
                updated_at: "2026-10-01T00:00:00Z",
            }],
            accepted_config: V1Config {
                file_system_id: fs.as_str(),
                display_name: "original sifs",
                owner_node_id: node.as_str(),
                subtree_policies: vec![V1Policy {
                    path_prefix: "/users",
                    policy: "replicated-eventual",
                    min_commit_replicas: None,
                    desired_replicas: None,
                }],
            },
            state: "pending",
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 4,
            created_at_unix_nanos: 17,
            updated_at_unix_nanos: 19,
        };
        let mut row = entry(serde_ext::to_bytes(&source, 1, LengthMode::U32).unwrap());
        row.key = format!("/dfs/filesystems/{fs}/placement-reconciliations-v1/original-v1");
        row
    }
    fn changed_v1(row: &KvEntry, value: &Value) -> KvEntry {
        let mut changed = row.clone();
        changed.value = serde_ext::to_bytes(value, 1, LengthMode::U32).unwrap();
        changed
    }
    #[test]
    fn genuine_initial_v1_retains_nonempty_candidates_config_and_unknown_old_counts() {
        let row = first_v1();
        assert!(
            row.decode::<sinomo_dfs::DfsPlacementReconciliationJob>()
                .is_err()
        );
        assert!(OriginalPlacementJob::requires_conversion(&row).unwrap());
        let source = OriginalPlacementJob::decode(&row).unwrap();
        assert_eq!(source.source_schema, OriginalPlacementSchema::V1Initial);
        assert_eq!(source.entry, row);
        assert_eq!(
            source.original_value["candidate_snapshot"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let policy = &source.original_value["accepted_config"]["subtree_policies"][0];
        assert!(policy.get("min_commit_replicas").is_none());
        assert!(policy.get("desired_replicas").is_none());
        let restored: OriginalPlacementJob =
            serde_json::from_slice(&serde_json::to_vec(&source).unwrap()).unwrap();
        assert_eq!(restored.entry, row);
        assert_eq!(restored.original_value, source.original_value);
        assert_eq!(
            restored.current_job_id().unwrap(),
            source.current_job_id().unwrap()
        );
    }
    #[test]
    fn actual_docker_v1_snapshot_keeps_recorded_resource_pressure_and_rejects_unknown_fields() {
        // Exact original DATA archived on Docker node2, 2026-10-08.
        // Key revision/lease and bytes are source evidence only, never authority.
        // Raw source SHA256: 62af89b9290ab4b83eba3e08a5f91be1e10bb10ed0ba48bec13a32a09d2441b0.
        let value = include_bytes!("../../testdata/placement-v1-resource-pressure.sobj").to_vec();
        let row = KvEntry {
            key: "/dfs/filesystems/3m3x1ehfwt2cca4jkz8r1jwq9g/placement-reconciliations-v1/01m47brgttv5sza0k5q08pa7c4".into(),
            value,
            create_revision: sinomo_core::kv::KvRevision(1068),
            modify_revision: sinomo_core::kv::KvRevision(1197),
            version: sinomo_core::kv::KvVersion(16),
            lease_id: None,
        };
        let original =
            crate::metadata::source_document::decode_original_document(&row.value).unwrap();
        let snapshot = &original["placement_snapshot"];
        validate_original_snapshot(snapshot).unwrap();
        let nodes = snapshot["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 6);
        assert!(
            nodes
                .iter()
                .all(|node| node.get("resource_pressure").is_some())
        );
        let source = OriginalPlacementJob::decode(&row).unwrap();
        assert!(source.terminal);
        assert_eq!(source.entry, row);
        assert_eq!(&source.original_value["placement_snapshot"], snapshot);
        for malformed in [
            serde_json::json!(null),
            serde_json::json!(1),
            serde_json::json!("unknown"),
            serde_json::json!({"normal": true}),
        ] {
            let mut changed = snapshot.clone();
            changed["nodes"][0]["resource_pressure"] = malformed;
            assert!(validate_original_snapshot(&changed).is_err());
        }
        let mut unknown = snapshot.clone();
        unknown["nodes"][0]["unknown_scope"] = serde_json::json!("foreign");
        assert!(validate_original_snapshot(&unknown).is_err());
        let mut without_pressure = snapshot.clone();
        without_pressure["nodes"][0]
            .as_object_mut()
            .unwrap()
            .remove("resource_pressure");
        validate_original_snapshot(&without_pressure).unwrap();
        assert!(
            without_pressure["nodes"][0]
                .get("resource_pressure")
                .is_none(),
            "omitted old optional pressure stays absent DATA"
        );
    }

    #[test]
    fn genuine_later_manual_and_adaptive_v1_keep_exact_planner_and_budget_data() {
        let row = first_v1();
        let initial =
            crate::metadata::source_document::decode_original_document(&row.value).unwrap();
        let mut manual = initial.clone();
        manual["adaptive"] = serde_json::json!(false);
        manual["processed_content_bytes"] = serde_json::json!(4096);
        let manual_row = changed_v1(&row, &manual);
        let decoded = OriginalPlacementJob::decode(&manual_row).unwrap();
        assert_eq!(decoded.source_schema, OriginalPlacementSchema::V1Adaptive);
        assert!(decoded.original_value.get("placement_snapshot").is_none());
        assert_eq!(decoded.original_value, manual);
        let mut adaptive = manual.clone();
        adaptive["adaptive"] = serde_json::json!(true);
        adaptive["placement_snapshot"] = serde_json::json!({"schema_version":1,"generation":23,
            "primary_node_id":initial["candidate_snapshot"][0]["node_id"],
            "observed_at":"2026-10-01T00:00:00Z","expires_at":"2026-10-01T00:05:00Z",
            "nodes":[],"paths":[]});
        adaptive["adaptive_budget"] = serde_json::json!({"max_objects_per_tick":31,"max_bytes_per_tick":8000,"max_millis_per_tick":777});
        let adaptive_row = changed_v1(&row, &adaptive);
        let decoded = OriginalPlacementJob::decode(&adaptive_row).unwrap();
        assert_eq!(decoded.entry, adaptive_row);
        assert_eq!(decoded.original_value, adaptive);
        assert_eq!(
            decoded.original_value["unavailable_node_ids"],
            initial["unavailable_node_ids"]
        );
        for field in [
            "adaptive",
            "processed_content_bytes",
            "placement_snapshot",
            "adaptive_budget",
        ] {
            let mut partial = adaptive.clone();
            partial.as_object_mut().unwrap().remove(field);
            assert!(
                OriginalPlacementJob::decode(&changed_v1(&row, &partial)).is_err(),
                "partial {field}"
            );
        }
        let mut extra = adaptive.clone();
        extra["adaptive_budget"]["unknown"] = serde_json::json!(1);
        assert!(OriginalPlacementJob::decode(&changed_v1(&row, &extra)).is_err());
    }
    #[test]
    fn v1_rejects_missing_source_snapshot_and_never_becomes_reader_only() {
        let row = first_v1();
        let original =
            crate::metadata::source_document::decode_original_document(&row.value).unwrap();
        for (field, bad) in [
            ("candidate_snapshot", serde_json::json!([])),
            ("accepted_config", Value::Null),
            ("schema_version", serde_json::json!(2)),
            ("revision", Value::Null),
        ] {
            let mut changed = original.clone();
            changed[field] = bad;
            assert!(OriginalPlacementJob::decode(&changed_v1(&row, &changed)).is_err());
        }
        let mut foreign = original.clone();
        foreign["accepted_config"]["file_system_id"] = serde_json::json!("foreign-source-fs");
        assert!(OriginalPlacementJob::decode(&changed_v1(&row, &foreign)).is_err());
        let mut wrong = row.clone();
        wrong.key = wrong
            .key
            .replace("placement-reconciliations-v1", "placement-reconciliations");
        assert!(OriginalPlacementJob::decode(&wrong).is_err());
    }
    #[test]
    fn v1_shared_cleanup_preserves_unrelated_order_flags_and_exact_native_compares() {
        let source = OriginalPlacementJob::decode(&first_v1()).unwrap();
        let keys = source.related_keys();
        let documents = [
            serde_json::json!({"schema_version":1,"job_ids":["current-before",source.job_id,"current-after"]}),
            serde_json::json!({"schema_version":1,"activity":{"job_id":source.job_id,"file_system_id":source.raw_file_system_id,"path":source.path,"scope":"subtree"}}),
            serde_json::json!({"schema_version":1,"recent":[{"job_id":"current-before","created_at_unix_nanos":2},
                {"job_id":source.job_id,"created_at_unix_nanos":17},{"job_id":"current-after","created_at_unix_nanos":3}],"partial":true,"history_truncated":true}),
        ];
        let captured = keys
            .iter()
            .zip(documents)
            .map(|(key, value)| {
                let mut row = entry(serde_ext::to_bytes(&value, 1, LengthMode::U32).unwrap());
                row.key = key.clone();
                MetadataCapturedKvEntry {
                    key: key.clone(),
                    entry: Some(row),
                }
            })
            .collect::<Vec<_>>();
        let saved = captured.clone();
        let ops = source.related_retirement_ops(&captured).unwrap();
        assert_eq!(
            serde_json::to_value(&captured).unwrap(),
            serde_json::to_value(&saved).unwrap()
        );
        assert_eq!(ops.len(), 3);
        match &ops[0] {
            KvOp::Put { value, .. } => {
                let queue =
                    crate::metadata::source_document::decode_original_document(value).unwrap();
                assert_eq!(
                    queue["job_ids"],
                    serde_json::json!(["current-before", "current-after"])
                );
            }
            _ => panic!("shared queue must retain unrelated current IDs"),
        }
        assert!(matches!(&ops[1],KvOp::Delete {key} if key == &keys[1]));
        match &ops[2] {
            KvOp::Put { value, .. } => {
                let index =
                    crate::metadata::source_document::decode_original_document(value).unwrap();
                assert_eq!(index["partial"], true);
                assert_eq!(index["history_truncated"], true);
                assert_eq!(index["recent"][0]["job_id"], "current-before");
                assert_eq!(index["recent"][1]["job_id"], "current-after");
            }
            _ => panic!("shared activity index must retain unrelated cursors"),
        }
        let compares = OriginalPlacementJob::exact_retirement_compares(&captured);
        for row in &captured {
            let original = row.entry.as_ref().unwrap();
            assert!(compares.iter().any(|compare| matches!(compare,KvCompare::ValueEquals {key,value} if key == &row.key && value == &original.value)));
            assert!(compares.iter().any(|compare| matches!(compare,KvCompare::ModifyRevisionEquals {key,revision} if key == &row.key && revision == &original.modify_revision)));
        }
        for row in &captured {
            let original = row.entry.as_ref().unwrap();
            assert!(compares.iter().any(|compare| matches!(compare,KvCompare::CreateRevisionEquals {key,revision} if key == &row.key && revision == &original.create_revision)));
            assert!(compares.iter().any(|compare| matches!(compare,KvCompare::VersionEquals {key,version} if key == &row.key && version == &original.version)));
            assert!(compares.iter().any(|compare| matches!(compare,KvCompare::LeaseEquals {key,lease_id} if key == &row.key && lease_id == &original.lease_id)));
        }
        let mut changed = captured.clone();
        let activity = documents_for_foreign_activity(&source);
        changed[1].entry.as_mut().unwrap().value =
            serde_ext::to_bytes(&activity, 1, LengthMode::U32).unwrap();
        assert!(source.related_retirement_ops(&changed).is_err());
    }
    fn documents_for_foreign_activity(source: &OriginalPlacementJob) -> Value {
        serde_json::json!({"schema_version":1,"activity":{"job_id":"replaced-source-id","file_system_id":source.raw_file_system_id,"path":source.path,"scope":"subtree"}})
    }
    #[test]
    fn valid_current_v1_stays_on_normal_path_and_partial_later_pair_is_rejected() {
        let node =
            sinomo_core::node::NodeId::from_legacy_named("current-placement-fixture").unwrap();
        let config = DfsFileSystemConfig::from(&sinomo_dfs::DfsFileSystem::sifs(node.clone()));
        let job = sinomo_dfs::build_placement_reconciliation_job(
            "current-v1".into(),
            &config,
            "/users".into(),
            DfsPlacementScope::Subtree,
            BTreeSet::new(),
            vec![sinomo_dfs::DfsNodeAdvertisement::content(node)],
            None,
            11,
        )
        .unwrap();
        let mut row = entry(sinomo_core::kv::encode_kv_value(&job).unwrap());
        row.key = sinomo_dfs::placement_reconciliation_key(&config.file_system_id, &job.job_id);
        assert!(!OriginalPlacementJob::requires_conversion(&row).unwrap());
        let mut partial =
            crate::metadata::source_document::decode_original_document(&row.value).unwrap();
        partial.as_object_mut().unwrap().remove("adaptive");
        assert!(OriginalPlacementJob::requires_conversion(&changed_v1(&row, &partial)).is_err());
        let mut malformed =
            crate::metadata::source_document::decode_original_document(&row.value).unwrap();
        malformed["candidate_snapshot"] = serde_json::json!([]);
        assert!(OriginalPlacementJob::requires_conversion(&changed_v1(&row, &malformed)).is_err());
    }
    #[test]
    fn adaptive_current_factory_keeps_exact_tick_caps_and_prior_spent_bytes() {
        // Pure source/current DTO construction DATA, not a native host sample
        // or permission witness. Production uses only the retained host getter.
        let row = first_v1();
        let mut raw =
            crate::metadata::source_document::decode_original_document(&row.value).unwrap();
        raw["adaptive"] = serde_json::json!(true);
        raw["processed_content_bytes"] = serde_json::json!(4096);
        raw["adaptive_budget"] = serde_json::json!({"max_objects_per_tick":31,"max_bytes_per_tick":8000,"max_millis_per_tick":777});
        raw["placement_snapshot"] = serde_json::json!({"schema_version":1,"generation":23,
            "primary_node_id":raw["candidate_snapshot"][0]["node_id"],
            "observed_at":"2026-10-01T00:00:00Z","expires_at":"2026-10-01T00:05:00Z"});
        let source = OriginalPlacementJob::decode(&changed_v1(&row, &raw)).unwrap();
        let node = normalized_source_id(raw["candidate_snapshot"][0]["node_id"].as_str().unwrap())
            .unwrap();
        let config = DfsFileSystemConfig::from(&sinomo_dfs::DfsFileSystem::sifs(node.clone()));
        let candidates = vec![sinomo_dfs::DfsNodeAdvertisement::content(node.clone())];
        assert!(
            source
                .build_current_job(&config, candidates.clone(), None, 19)
                .is_err()
        );
        let now = time::OffsetDateTime::now_utc();
        let current = sinomo_dfs::DfsPlacementSnapshot {
            schema_version: 1,
            generation: 24,
            primary_node_id: node,
            observed_at: now,
            expires_at: now + time::Duration::minutes(5),
            nodes: Vec::new(),
            paths: Vec::new(),
        };
        let new = source
            .build_current_job(&config, candidates, Some(current.clone()), 19)
            .unwrap();
        assert!(new.adaptive);
        let budget = new.adaptive_budget.unwrap();
        assert_eq!(budget.max_objects_per_tick, 31);
        assert_eq!(budget.max_bytes_per_tick, 8000);
        assert_eq!(budget.max_millis_per_tick, 777);
        assert_eq!(new.processed_content_bytes, 4096);
        assert_eq!(new.placement_snapshot, Some(current));
        assert_eq!(
            new.state,
            sinomo_dfs::DfsPlacementReconciliationState::Pending
        );
        assert!(new.cursor.is_none());
        assert_eq!(source.original_value, raw);
        assert_eq!(
            source.original_value["placement_snapshot"]["generation"],
            23
        );
    }
}
