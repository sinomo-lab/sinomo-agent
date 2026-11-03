//! Node-local append ledger. Only the host worker performs writes.
use serde::{Deserialize, Serialize};
pub use sinomo_api::app::usage::*;
use sinomo_core::local::SinomoDataDir;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const DAY_MS: u64 = 86_400_000;
// Retain millisecond unit totals so later price-effective boundaries and custom
// ranges remain exact even after raw evidence is expired.
const BUCKET_MS: u64 = 1;
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Rollup {
    dimensions: UsageRecord,
    totals: UsageTotals,
}
#[derive(Default, Serialize, Deserialize)]
struct Segment {
    #[serde(default)]
    indexed: BTreeMap<String, u64>,
    #[serde(default)]
    retained_from_ms: u64,
    #[serde(default)]
    offset: u64,
    rollups: BTreeMap<String, Rollup>,
}
pub struct UsageLedger {
    root: PathBuf,
}
impl UsageLedger {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        Self {
            root: data_dir.root().join("observe/usage"),
        }
    }
    fn lock_segment(&self, day: u64) -> Result<File, String> {
        fs::create_dir_all(&self.root).map_err(error)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join(format!("{day}.lock")))
            .map_err(error)?;
        file.lock().map_err(error)?;
        Ok(file)
    }
    pub fn append(&self, record: &UsageRecord) -> Result<(), String> {
        let _lock = self.lock_segment(record.timestamp_ms / DAY_MS)?;
        let path = self
            .root
            .join(format!("{}.jsonl", record.timestamp_ms / DAY_MS));
        super::append_jsonl_record_mode(
            &path,
            &record.clone().into_envelope(),
            super::JsonlRotationConfig {
                max_bytes: u64::MAX,
                retained_files: 0,
            },
            true,
        )
        .map_err(error)?;
        // A raw append is authoritative. Rebuildable rollups are refreshed by maintenance.
        Ok(())
    }
    pub fn maintain(&self, now_ms: u64, retention: &UsageRetention) -> Result<(), String> {
        retention.validate()?;
        fs::create_dir_all(&self.root).map_err(error)?;
        for entry in fs::read_dir(&self.root).map_err(error)? {
            let path = entry.map_err(error)?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
                continue;
            }
            let day = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or("invalid usage segment name")?;
            let _lock = self.lock_segment(day)?;
            if !path.exists() {
                continue;
            }
            let rollup = path.with_extension("rollup");
            if retention.raw_seconds.is_none()
                && retention.rollup_seconds.is_none()
                && fs::metadata(&rollup)
                    .and_then(|m| m.modified())
                    .ok()
                    .zip(fs::metadata(&path).and_then(|m| m.modified()).ok())
                    .is_some_and(|(projected, raw)| projected >= raw)
            {
                continue;
            }
            let mut segment = load_segment(&path, true)?;
            if let Some(seconds) = retention.rollup_seconds {
                let cutoff = now_ms.saturating_sub(seconds.saturating_mul(1000));
                segment.retained_from_ms = segment.retained_from_ms.max(cutoff);
                segment
                    .rollups
                    .retain(|_, r| r.dimensions.timestamp_ms >= cutoff);
                segment.indexed.retain(|_, time| *time >= cutoff);
            }
            atomic_json(&rollup, &segment)?;
            if let Some(seconds) = retention.raw_seconds {
                prune_raw(
                    &path,
                    &rollup,
                    &mut segment,
                    now_ms.saturating_sub(seconds.saturating_mul(1000)),
                )?;
            }
        }
        if let Some(seconds) = retention.rollup_seconds {
            let cutoff = now_ms.saturating_sub(seconds.saturating_mul(1000));
            for entry in fs::read_dir(&self.root).map_err(error)? {
                let path = entry.map_err(error)?.path();
                if path.extension().and_then(|s| s.to_str()) != Some("rollup") {
                    continue;
                }
                let day = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or("invalid rollup name")?;
                let _lock = self.lock_segment(day)?;
                if !path.exists() {
                    continue;
                }
                if day.saturating_add(1).saturating_mul(DAY_MS) <= cutoff {
                    let raw = path.with_extension("jsonl");
                    fs::remove_file(path).map_err(error)?;
                    if raw.exists() {
                        fs::remove_file(raw).map_err(error)?;
                    }
                }
            }
        }
        Ok(())
    }
    pub fn query(&self, query: &UsageQuery, node: &str) -> Result<UsageQueryResponse, String> {
        self.query_with_settings(query, node, &UsageSettings::default())
    }
    pub fn query_with_settings(
        &self,
        query: &UsageQuery,
        node: &str,
        settings: &UsageSettings,
    ) -> Result<UsageQueryResponse, String> {
        self.query_with_settings_at(
            query,
            node,
            settings,
            sinomo_api::observe::recording::timestamp_ms(),
        )
    }
    pub fn query_with_settings_at(
        &self,
        query: &UsageQuery,
        node: &str,
        settings: &UsageSettings,
        as_of_ms: u64,
    ) -> Result<UsageQueryResponse, String> {
        query.validate()?;
        fs::create_dir_all(&self.root).map_err(error)?;
        let mut response = UsageQueryResponse {
            answered: vec![node.into()],
            ..Default::default()
        };
        let mut budgets = Vec::new();
        for budget in &settings.budgets {
            if !budget.amount.is_finite() || budget.amount < 0.0 {
                return Err("budget amount must be finite and nonnegative".into());
            }
            let (start, end) = budget_period(budget.period, as_of_ms)?;
            budgets.push((
                budget,
                UsageQuery {
                    start_ms: start,
                    end_ms: as_of_ms.saturating_add(1).min(end),
                    filter: budget.scope.clone(),
                    limit: 0,
                    ..Default::default()
                },
                end,
                UsageCost::default(),
            ));
        }
        let mut days = BTreeMap::<u64, bool>::new();
        for entry in fs::read_dir(&self.root).map_err(error)? {
            let entry = entry.map_err(error)?;
            let path = entry.path();
            response.ledger_bytes = response
                .ledger_bytes
                .saturating_add(entry.metadata().map_err(error)?.len());
            if !matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("jsonl" | "rollup")
            ) {
                continue;
            }
            if let Some(day) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                let raw = path.extension().and_then(|s| s.to_str()) == Some("jsonl");
                days.entry(day).and_modify(|v| *v |= raw).or_insert(raw);
            }
        }
        let mut series = BTreeMap::<u64, UsageTotals>::new();
        let mut series_costs = BTreeMap::<u64, UsageCost>::new();
        let mut series_providers = BTreeMap::<u64, BTreeMap<String, UsageCost>>::new();
        let mut records = Vec::new();
        for (day, raw) in days {
            let needed = |q: &UsageQuery| {
                day.saturating_mul(DAY_MS) < q.end_ms
                    && day.saturating_add(1).saturating_mul(DAY_MS) > q.start_ms
            };
            let main_range = needed(query);
            if !main_range && !budgets.iter().any(|(_, q, _, _)| needed(q)) {
                continue;
            }
            let path = self.root.join(format!("{day}.jsonl"));
            let lock = self.lock_segment(day)?;
            let raw = raw && path.exists();
            if !raw && !path.with_extension("rollup").exists() {
                continue;
            }
            let mut segment = load_segment(&path, raw)?;
            let raw_snapshot = if raw {
                Some(File::open(&path).map_err(error)?)
            } else {
                None
            };
            drop(lock);
            if let Some(seconds) = settings.retention.rollup_seconds {
                let cutoff = as_of_ms.saturating_sub(seconds.saturating_mul(1000));
                segment
                    .rollups
                    .retain(|_, r| r.dimensions.timestamp_ms >= cutoff);
            }
            let has_edge = segment.rollups.values().any(|r| {
                let start = r.dimensions.timestamp_ms;
                start < query.end_ms
                    && start.saturating_add(BUCKET_MS) > query.start_ms
                    && (start < query.start_ms
                        || start.saturating_add(BUCKET_MS) > query.end_ms
                        || query.bucket_ms % BUCKET_MS != 0)
            });
            for rollup in segment.rollups.values() {
                let start = rollup.dimensions.timestamp_ms;
                let edge = start < query.start_ms
                    || start.saturating_add(BUCKET_MS) > query.end_ms
                    || query.bucket_ms % BUCKET_MS != 0;
                let main_match = main_range && matches_query(&rollup.dimensions, query) && !edge;
                if !main_match
                    && !budgets
                        .iter()
                        .any(|(_, q, _, _)| matches_query(&rollup.dimensions, q))
                {
                    continue;
                }
                let mut priced = rollup.dimensions.clone();
                if let UsageEvent::ModelRequest { units, .. } = &mut priced.event {
                    *units = rollup.totals.units.clone();
                }
                let mut cost = sinomo_api::app::usage::pricing::estimate(&priced, settings);
                cost.priced_requests *= rollup.totals.model_requests;
                cost.unpriced_requests *= rollup.totals.model_requests;
                cost.subscription_requests *= rollup.totals.model_requests;
                cost.local_requests *= rollup.totals.model_requests;
                for (_, q, _, budget_cost) in &mut budgets {
                    if matches_query(&rollup.dimensions, q) {
                        pricing::merge_cost(budget_cost, &cost);
                    }
                }
                if !main_match {
                    continue;
                }
                add_history(&mut response, &rollup.dimensions);
                let bucket = start / query.bucket_ms * query.bucket_ms;
                pricing::merge_cost(series_costs.entry(bucket).or_default(), &cost);
                if let UsageEvent::ModelRequest { provider, .. } = &rollup.dimensions.event {
                    pricing::merge_cost(
                        series_providers
                            .entry(bucket)
                            .or_default()
                            .entry(provider.clone())
                            .or_default(),
                        &cost,
                    );
                }
                let included = add_group(
                    &mut response.groups,
                    &rollup.dimensions,
                    &rollup.totals,
                    &cost,
                    query,
                );
                response.truncated |= !included;
                sinomo_api::app::usage::pricing::merge_cost(&mut response.cost, &cost);
                merge_totals(&mut response.totals, &rollup.totals);
                merge_totals(
                    series
                        .entry(start / query.bucket_ms * query.bucket_ms)
                        .or_default(),
                    &rollup.totals,
                );
            }
            if main_range && raw && (query.limit > 0 || has_edge) {
                visit_records_file(
                    raw_snapshot.expect("retained raw snapshot"),
                    0,
                    segment.offset,
                    |record| {
                        if !matches_query(&record, query) {
                            return;
                        }
                        let start = record.timestamp_ms / BUCKET_MS * BUCKET_MS;
                        if start < query.start_ms
                            || start.saturating_add(BUCKET_MS) > query.end_ms
                            || query.bucket_ms % BUCKET_MS != 0
                        {
                            let totals = record_totals(&record);
                            merge_totals(&mut response.totals, &totals);
                            merge_totals(
                                series
                                    .entry(record.timestamp_ms / query.bucket_ms * query.bucket_ms)
                                    .or_default(),
                                &totals,
                            );
                            let cost = sinomo_api::app::usage::pricing::estimate(&record, settings);
                            response.truncated |=
                                !add_group(&mut response.groups, &record, &totals, &cost, query);
                            sinomo_api::app::usage::pricing::merge_cost(&mut response.cost, &cost);
                        }
                        let retained = settings.retention.raw_seconds.is_none_or(|seconds| {
                            record.timestamp_ms
                                >= as_of_ms.saturating_sub(seconds.saturating_mul(1000))
                        });
                        if retained
                            && query.limit > 0
                            && query
                                .cursor
                                .as_ref()
                                .is_none_or(|cursor| cursor.after(&record))
                        {
                            retain_recent(&mut records, record, query.limit + 1);
                        }
                    },
                )?;
            } else if main_range && has_edge {
                response.omitted.push(format!("{node}: raw records expired; boundary buckets for day {day} are omitted (one-minute rollups)"));
            }
        }
        if records.len() > query.limit {
            records.truncate(query.limit);
            response.next_cursor = records.last().map(UsageCursor::from_record);
        }
        response.records = records;
        for record in &response.records {
            if matches!(record.event, UsageEvent::ModelRequest { .. }) {
                response
                    .record_costs
                    .insert(record.id.clone(), pricing::estimate(record, settings));
            }
        }
        response.series = series
            .into_iter()
            .map(|(start_ms, totals)| UsageBucket {
                start_ms,
                totals,
                cost: series_costs.remove(&start_ms).unwrap_or_default(),
                providers: series_providers.remove(&start_ms).unwrap_or_default(),
            })
            .collect();
        response
            .ledger_sizes
            .insert(node.to_string(), response.ledger_bytes);
        if response.truncated {
            response.warnings.push(format!("{node}: grouping limit reached; narrow filters or select a coarser grouping. Totals still include all matching work."));
        }
        for (budget, budget_query, end, cost) in budgets {
            let spent = cost.amounts.get(&budget.currency).copied().unwrap_or(0.0);
            response.budgets.push(UsageBudgetProgress {
                name: budget.name.clone(),
                currency: budget.currency.clone(),
                amount: budget.amount,
                spent,
                projected: spent * (end - budget_query.start_ms) as f64
                    / (as_of_ms.saturating_sub(budget_query.start_ms).max(1) as f64),
                period_start_ms: budget_query.start_ms,
                period_end_ms: end,
                unpriced_requests: cost.unpriced_requests,
            });
        }
        Ok(response)
    }
}
fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let temp = path.with_extension("tmp");
    let mut f = File::create(&temp).map_err(error)?;
    serde_json::to_writer(&mut f, value).map_err(error)?;
    f.sync_all().map_err(error)?;
    fs::rename(temp, path).map_err(error)?;
    sync_parent(path)
}
fn visit_records_from(
    path: &Path,
    offset: u64,
    visit: impl FnMut(UsageRecord),
) -> Result<u64, String> {
    let file = File::open(path).map_err(error)?;
    if file.metadata().map_err(error)?.len() < offset {
        return Err("usage raw segment is shorter than its rollup checkpoint".into());
    }
    let length = file.metadata().map_err(error)?.len();
    visit_records_file(file, offset, length, visit)
}
fn visit_records_file(
    mut file: File,
    offset: u64,
    length: u64,
    mut visit: impl FnMut(UsageRecord),
) -> Result<u64, String> {
    file.seek(SeekFrom::Start(offset)).map_err(error)?;
    let mut reader = BufReader::new(file.take(length.saturating_sub(offset)));
    let mut valid = offset;
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line).map_err(error)?;
        if n == 0 || line.last() != Some(&b'\n') {
            break;
        }
        let event: sinomo_core::base::EventEnvelope =
            serde_json::from_slice(&line).map_err(error)?;
        if event.kind.as_str() != PRUNED_MARKER {
            visit(serde_json::from_value(event.payload).map_err(error)?);
        }
        valid += n as u64;
    }
    Ok(valid)
}
fn load_segment(path: &Path, raw: bool) -> Result<Segment, String> {
    let rollup = path.with_extension("rollup");
    let mut segment = match File::open(&rollup) {
        Ok(file) => match serde_json::from_reader(file) {
            Ok(segment) => segment,
            Err(e) if raw && !raw_was_pruned(path)? => {
                tracing::warn!(target:"sinomo.observe.usage",%e,"rebuilding corrupt usage rollup from retained events");
                Segment::default()
            }
            Err(e) => return Err(error(e)),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && (!raw || !raw_was_pruned(path)?) => {
            Segment::default()
        }
        Err(e) => return Err(error(e)),
    };
    if raw {
        let before = segment.offset;
        let offset = visit_records_from(path, before, |record| add_record(&mut segment, &record))?;
        if offset != before {
            segment.offset = offset;
            atomic_json(&rollup, &segment)?;
        }
    }
    Ok(segment)
}

fn add_record(segment: &mut Segment, record: &UsageRecord) {
    if record.timestamp_ms < segment.retained_from_ms
        || segment
            .indexed
            .insert(record.id.clone(), record.timestamp_ms)
            .is_some()
    {
        return;
    }
    let mut dimensions = record.clone();
    if !matches!(
        record.event,
        UsageEvent::QuotaSampled { .. } | UsageEvent::SkillVersionPublished { .. }
    ) {
        dimensions.id.clear();
    }
    dimensions.timestamp_ms = record.timestamp_ms / BUCKET_MS * BUCKET_MS;
    dimensions.attribution.turn = None;
    dimensions.attribution.task = None;
    dimensions.attribution.operation = None;
    dimensions.attribution.tool_call = None;
    dimensions.attribution.caused_by.clear();
    match &mut dimensions.event {
        UsageEvent::ModelRequest {
            units,
            duration_ms,
            first_output_ms,
            ..
        } => {
            *units = Default::default();
            *duration_ms = None;
            *first_output_ms = None;
        }
        UsageEvent::ToolCall { duration_ms, .. } => *duration_ms = 0,
        _ => {}
    }
    let key = serde_json::to_string(&dimensions).expect("serializable usage dimensions");
    let r = segment.rollups.entry(key).or_insert(Rollup {
        dimensions,
        totals: Default::default(),
    });
    merge_totals(&mut r.totals, &record_totals(record));
}
pub fn matches_query(r: &UsageRecord, q: &UsageQuery) -> bool {
    if r.timestamp_ms < q.start_ms || r.timestamp_ms >= q.end_ms {
        return false;
    }
    let f = &q.filter;
    if f.event_kind
        .as_deref()
        .is_some_and(|kind| kind != r.event.kind())
    {
        return false;
    }
    let a = &r.attribution;
    let matches = |wanted: &Option<String>, actual: Option<&str>| {
        wanted.as_deref().is_none_or(|w| Some(w) == actual)
    };
    if !matches(&f.agent, a.agent.as_deref())
        || !matches(&f.chat, a.chat.as_deref())
        || !matches(&f.session, a.session.as_deref())
        || !matches(&f.purpose, Some(&a.purpose))
    {
        return false;
    }
    let (provider, account, model, operation, tool, skill) = match &r.event {
        UsageEvent::ModelRequest {
            provider,
            account,
            model,
            operation,
            ..
        } => (
            Some(provider.as_str()),
            Some(account.as_str()),
            Some(model.as_str()),
            Some(operation.as_str()),
            None,
            None,
        ),
        UsageEvent::ToolCall { tool, .. } => (None, None, None, None, Some(tool.as_str()), None),
        UsageEvent::SkillActivated { skill, .. }
        | UsageEvent::SkillVersionPublished { skill, .. } => {
            (None, None, None, None, None, Some(skill.as_str()))
        }
        UsageEvent::QuotaSampled { account, snapshot } => (
            Some(snapshot.provider.as_str()),
            Some(account.as_str()),
            None,
            None,
            None,
            None,
        ),
    };
    matches(&f.provider, provider)
        && matches(&f.account, account)
        && matches(&f.model, model)
        && matches(&f.operation, operation)
        && matches(&f.tool, tool)
        && matches(&f.skill, skill)
}
fn record_totals(record: &UsageRecord) -> UsageTotals {
    let mut t = UsageTotals {
        last_used_ms: Some(record.timestamp_ms),
        ..Default::default()
    };
    if let Some(agent) = &record.attribution.agent {
        t.agents.insert(agent.clone(), 1);
    }
    match &record.event {
        UsageEvent::ModelRequest {
            status,
            units,
            duration_ms,
            retry_count,
            reported,
            ..
        } => {
            t.model_requests = 1;
            t.reported_requests = u64::from(*reported);
            if let Some(retries) = retry_count {
                t.retries = *retries as u64;
                t.retry_reporting_requests = 1;
            }
            t.model_errors = u64::from(*status != UsageStatus::Succeeded);
            t.units = units.clone();
            if let Some(duration_ms) = duration_ms {
                t.model_latency_ms.insert(*duration_ms, 1);
            }
            t.statuses.insert(format!("model.{status:?}"), 1);
        }
        UsageEvent::ToolCall {
            status,
            duration_ms,
            detached,
            runtime,
            executing_node,
            ..
        } => {
            t.tool_calls = 1;
            t.tool_errors = u64::from(*status != UsageStatus::Succeeded);
            t.tool_latency_ms.insert(*duration_ms, 1);
            t.statuses.insert(format!("tool.{status:?}"), 1);
            t.detached_tool_calls = u64::from(*detached);
            t.runtimes.insert(runtime.clone(), 1);
            if let Some(node) = executing_node {
                t.executing_nodes.insert(node.clone(), 1);
            }
        }
        UsageEvent::SkillActivated { trigger, .. } => {
            t.skill_activations = 1;
            t.skill_triggers.insert(trigger.clone(), 1);
        }
        UsageEvent::SkillVersionPublished { .. } => t.skill_publications = 1,
        _ => {}
    }
    t
}

pub(crate) mod metrics;
pub(crate) mod service;

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(at: u64) -> UsageRecord {
        UsageRecord {
            id: format!("r{at}"),
            timestamp_ms: at,
            attribution: UsageAttribution {
                node: "n1".into(),
                ..Default::default()
            },
            event: UsageEvent::ModelRequest {
                provider: "fixture".into(),
                account: "default".into(),
                model: "test".into(),
                operation: "completion".into(),
                status: UsageStatus::Succeeded,
                error_class: None,
                retry_count: None,
                duration_ms: Some(42),
                first_output_ms: Some(5),
                reported: true,
                units: LanguageModelUsage {
                    input_tokens: Some(11),
                    ..Default::default()
                },
            },
        }
    }
    #[test]
    fn torn_tail_is_discarded_before_next_append() {
        let dir = sinomo_testdir::TestDir::new("usage-torn-tail");
        let data = SinomoDataDir::new(&dir);
        let ledger = UsageLedger::new(&data);
        ledger.append(&sample(60_000)).unwrap();
        OpenOptions::new()
            .append(true)
            .open(ledger.root.join("0.jsonl"))
            .unwrap()
            .write_all(b"{torn")
            .unwrap();
        ledger.append(&sample(120_000)).unwrap();
        let result = UsageLedger::new(&data)
            .query(&UsageQuery::default(), "n1")
            .unwrap();
        assert_eq!(result.totals.model_requests, 2);
        assert_eq!(result.totals.units.input_tokens, Some(22));
    }
    #[test]
    fn rollups_survive_raw_retention_and_restart() {
        let dir = sinomo_testdir::TestDir::new("usage-retention");
        let data = SinomoDataDir::new(&dir);
        let ledger = UsageLedger::new(&data);
        ledger.append(&sample(60_000)).unwrap();
        ledger
            .maintain(
                DAY_MS * 3,
                &UsageRetention {
                    raw_seconds: Some(0),
                    rollup_seconds: None,
                },
            )
            .unwrap();
        let q = UsageQuery {
            start_ms: 0,
            end_ms: DAY_MS,
            ..Default::default()
        };
        let result = UsageLedger::new(&data).query(&q, "n1").unwrap();
        assert_eq!(result.totals.model_requests, 1);
        assert!(result.records.is_empty());
        assert!(!result.partial());
        ledger
            .maintain(
                DAY_MS * 3,
                &UsageRetention {
                    raw_seconds: Some(0),
                    rollup_seconds: Some(0),
                },
            )
            .unwrap();
        assert_eq!(ledger.query(&q, "n1").unwrap().totals.model_requests, 0);
    }
    #[test]
    fn corruption_before_final_line_is_not_silently_ignored() {
        let dir = sinomo_testdir::TestDir::new("usage-corrupt");
        let ledger = UsageLedger::new(&SinomoDataDir::new(&dir));
        ledger.append(&sample(60_000)).unwrap();
        OpenOptions::new()
            .append(true)
            .open(ledger.root.join("0.jsonl"))
            .unwrap()
            .write_all(b"broken\n")
            .unwrap();
        assert!(ledger.query(&UsageQuery::default(), "n1").is_err());
    }
    #[test]
    fn exact_retention_prunes_only_expired_raw_events_and_keeps_replay_idempotent() {
        let root = sinomo_testdir::TestDir::new("usage-exact-retention");
        let ledger = UsageLedger::new(&SinomoDataDir::new(&root));
        ledger.append(&sample(1000)).unwrap();
        ledger.append(&sample(2000)).unwrap();
        ledger.query(&UsageQuery::default(), "node").unwrap();
        let raw = ledger.root.join("0.jsonl");
        let rollup = raw.with_extension("rollup");
        let mut before = load_segment(&raw, true).unwrap();
        before.offset = 0;
        atomic_json(&rollup, &before).unwrap();
        assert_eq!(
            ledger
                .query(&UsageQuery::default(), "node")
                .unwrap()
                .totals
                .model_requests,
            2,
            "restart before raw replacement must not replay counted events"
        );
        ledger
            .maintain(
                2500,
                &UsageRetention {
                    raw_seconds: Some(1),
                    rollup_seconds: None,
                },
            )
            .unwrap();
        let response = ledger.query(&UsageQuery::default(), "node").unwrap();
        assert_eq!(response.totals.model_requests, 2);
        assert_eq!(response.records.len(), 1);
        assert_eq!(response.records[0].timestamp_ms, 2000);
        let mut after = load_segment(&raw, true).unwrap();
        after.offset = 0;
        atomic_json(&rollup, &after).unwrap();
        ledger.append(&sample(3000)).unwrap();
        assert_eq!(
            ledger
                .query(&UsageQuery::default(), "node")
                .unwrap()
                .totals
                .model_requests,
            3,
            "restart after raw replacement must not duplicate retained records"
        );
        fs::remove_file(rollup).unwrap();
        assert!(
            ledger.query(&UsageQuery::default(), "node").is_err(),
            "pruned raw history cannot rebuild expired source data"
        );
    }
    #[test]
    fn rollup_rebuild_and_repricing_preserve_sub_minute_boundaries() {
        let root = sinomo_testdir::TestDir::new("usage-reprice-boundary");
        let ledger = UsageLedger::new(&SinomoDataDir::new(&root));
        for at in [1000, 2000] {
            let mut r = sample(at);
            if let UsageEvent::ModelRequest { units, .. } = &mut r.event {
                units.input_tokens = Some(1_000_000);
            }
            ledger.append(&r).unwrap();
        }
        let price = |at, rate| UsagePrice {
            provider: "fixture".into(),
            model_pattern: "test".into(),
            operation: "completion".into(),
            effective_from_ms: at,
            currency: "USD".into(),
            units: BTreeMap::from([("input_tokens".into(), rate)]),
            source: "test".into(),
        };
        let mut settings = UsageSettings {
            prices: vec![price(0, 1.0), price(1500, 3.0)],
            ..Default::default()
        };
        assert_eq!(
            ledger
                .query_with_settings(&UsageQuery::default(), "node", &settings)
                .unwrap()
                .cost
                .amounts["USD"],
            4.0
        );
        fs::remove_file(ledger.root.join("0.rollup")).unwrap();
        assert_eq!(
            ledger
                .query_with_settings(&UsageQuery::default(), "node", &settings)
                .unwrap()
                .cost
                .amounts["USD"],
            4.0
        );
        ledger
            .maintain(
                3000,
                &UsageRetention {
                    raw_seconds: Some(0),
                    rollup_seconds: None,
                },
            )
            .unwrap();
        settings.prices[1].units.insert("input_tokens".into(), 4.0);
        assert_eq!(
            ledger
                .query_with_settings(&UsageQuery::default(), "node", &settings)
                .unwrap()
                .cost
                .amounts["USD"],
            5.0
        );
        let range = UsageQuery {
            start_ms: 1500,
            end_ms: 2500,
            limit: 0,
            ..Default::default()
        };
        let response = ledger
            .query_with_settings(&range, "node", &settings)
            .unwrap();
        assert_eq!(response.totals.model_requests, 1);
        assert_eq!(response.cost.amounts["USD"], 4.0);
        assert!(!response.partial());
    }
    #[test]
    fn cursor_pages_all_history_beyond_the_old_collection_window() {
        let root = sinomo_testdir::TestDir::new("usage-cursor-pages");
        let ledger = UsageLedger::new(&SinomoDataDir::new(&root));
        for index in 0..1005 {
            ledger.append(&sample(1_000_000 + index)).unwrap();
        }
        let mut query = UsageQuery {
            limit: 200,
            ..Default::default()
        };
        let mut ids = std::collections::BTreeSet::new();
        loop {
            let response = ledger.query(&query, "node").unwrap();
            assert_eq!(response.totals.model_requests, 1005);
            for record in response.records {
                assert!(ids.insert(record.id));
            }
            let Some(cursor) = response.next_cursor else {
                break;
            };
            query.cursor = Some(cursor);
        }
        assert_eq!(ids.len(), 1005);
    }
    #[test]
    fn budgets_use_their_period_even_when_the_display_range_is_different() {
        let root = sinomo_testdir::TestDir::new("usage-budget-period");
        let ledger = UsageLedger::new(&SinomoDataDir::new(&root));
        let now = DAY_MS * 10 + DAY_MS / 2;
        let mut record = sample(now - 1000);
        if let UsageEvent::ModelRequest { units, .. } = &mut record.event {
            units.input_tokens = Some(1_000_000);
        }
        ledger.append(&record).unwrap();
        let settings = UsageSettings {
            prices: vec![UsagePrice {
                provider: "fixture".into(),
                model_pattern: "test".into(),
                operation: "completion".into(),
                effective_from_ms: 0,
                currency: "USD".into(),
                units: BTreeMap::from([("input_tokens".into(), 2.0)]),
                source: "test".into(),
            }],
            budgets: vec![UsageBudget {
                name: "Daily".into(),
                scope: Default::default(),
                period: BudgetPeriod::Day,
                currency: "USD".into(),
                amount: 5.0,
            }],
            ..Default::default()
        };
        let response = ledger
            .query_with_settings_at(
                &UsageQuery {
                    start_ms: 0,
                    end_ms: 1,
                    ..Default::default()
                },
                "node",
                &settings,
                now,
            )
            .unwrap();
        assert_eq!(response.totals.model_requests, 0);
        assert_eq!(response.budgets[0].spent, 2.0);
        assert_eq!(response.budgets[0].projected, 4.0);
    }
}

fn budget_period(period: BudgetPeriod, now: u64) -> Result<(u64, u64), String> {
    let date = time::OffsetDateTime::from_unix_timestamp((now / 1000).min(i64::MAX as u64) as i64)
        .map_err(error)?
        .date();
    let (start, end) = match period {
        BudgetPeriod::Day => (date, date + time::Duration::days(1)),
        BudgetPeriod::Week => {
            let start =
                date - time::Duration::days(date.weekday().number_days_from_monday() as i64);
            (start, start + time::Duration::days(7))
        }
        BudgetPeriod::Month => {
            let start =
                time::Date::from_calendar_date(date.year(), date.month(), 1).map_err(error)?;
            let end = time::Date::from_calendar_date(
                if date.month() == time::Month::December {
                    date.year() + 1
                } else {
                    date.year()
                },
                date.month().next(),
                1,
            )
            .map_err(error)?;
            (start, end)
        }
    };
    Ok((
        (start.midnight().assume_utc().unix_timestamp().max(0) as u64) * 1000,
        (end.midnight().assume_utc().unix_timestamp().max(0) as u64) * 1000,
    ))
}

fn add_group(
    groups: &mut Vec<UsageGroup>,
    record: &UsageRecord,
    totals: &UsageTotals,
    cost: &UsageCost,
    query: &UsageQuery,
) -> bool {
    let mut dimensions = UsageFilter {
        agent: record.attribution.agent.clone(),
        chat: record.attribution.chat.clone(),
        purpose: Some(record.attribution.purpose.clone()),
        ..Default::default()
    };
    match &record.event {
        UsageEvent::ModelRequest {
            provider,
            account,
            model,
            operation,
            ..
        } => {
            dimensions.provider = Some(provider.clone());
            dimensions.account = Some(account.clone());
            dimensions.model = Some(model.clone());
            dimensions.operation = Some(operation.clone());
        }
        UsageEvent::ToolCall { tool, .. } => dimensions.tool = Some(tool.clone()),
        UsageEvent::SkillActivated { skill, .. }
        | UsageEvent::SkillVersionPublished { skill, .. } => dimensions.skill = Some(skill.clone()),
        UsageEvent::QuotaSampled { account, snapshot } => {
            dimensions.provider = Some(snapshot.provider.clone());
            dimensions.account = Some(account.clone());
        }
    }
    let node = if matches!(query.group_by, UsageGrouping::All | UsageGrouping::Node) {
        record.attribution.node.clone()
    } else {
        String::new()
    };
    dimensions = match query.group_by {
        UsageGrouping::All => dimensions,
        UsageGrouping::Provider => UsageFilter {
            provider: dimensions.provider,
            account: dimensions.account,
            model: dimensions.model,
            ..Default::default()
        },
        UsageGrouping::Tool => UsageFilter {
            tool: dimensions.tool,
            ..Default::default()
        },
        UsageGrouping::Skill => UsageFilter {
            skill: dimensions.skill,
            ..Default::default()
        },
        UsageGrouping::Agent => UsageFilter {
            agent: dimensions.agent,
            ..Default::default()
        },
        UsageGrouping::Chat => UsageFilter {
            chat: dimensions.chat,
            ..Default::default()
        },
        UsageGrouping::Purpose => UsageFilter {
            purpose: dimensions.purpose,
            ..Default::default()
        },
        UsageGrouping::Node => UsageFilter::default(),
    };
    if let Some(group) = groups
        .iter_mut()
        .find(|g| g.node == node && g.dimensions == dimensions)
    {
        merge_totals(&mut group.totals, totals);
        sinomo_api::app::usage::pricing::merge_cost(&mut group.cost, cost);
    } else {
        if groups.len() >= query.max_groups {
            return false;
        }
        groups.push(UsageGroup {
            node,
            dimensions,
            totals: totals.clone(),
            cost: cost.clone(),
        });
    }
    true
}

pub(crate) fn config_store() -> Result<sinomo_config::ConfigStore, String> {
    let registry = sinomo_config::ConfigRegistry::builder()
        .register(sinomo_api::app::usage::pricing::config_definition().map_err(error)?)
        .map_err(error)?
        .build();
    Ok(sinomo_config::ConfigStore::new(registry))
}

pub(crate) fn start_maintenance(
    context: &sinomo_module::ComponentContext,
    scope: &mut sinomo_module::ComponentScope,
) {
    let Some(runtime) = context.runtime() else {
        return;
    };
    let data = context.data_dir().clone();
    let cluster = context.cluster_access();
    let mut shutdown = scope.shutdown_receiver();
    scope.own_task("observe usage retention",runtime.spawn(async move{
        let mut interval=tokio::time::interval(std::time::Duration::from_secs(60));
        loop{tokio::select!{_=shutdown.changed()=>break,_=interval.tick()=>{}}
            let result=async {
                let settings=if let Some(cluster)=cluster.as_deref(){
                    let value=config_store()?.get_async(&data,Some(cluster),pricing::SETTINGS_KEY).await.map_err(error)?;
                    serde_json::from_value::<UsageSettings>(value.value).map_err(error)?
                }else{UsageSettings::default()};
                settings.retention.validate()?;
                let dir=data.clone();tokio::task::spawn_blocking(move||UsageLedger::new(&dir).maintain(sinomo_api::observe::recording::timestamp_ms(),&settings.retention)).await.map_err(error)?
            }.await;
            if let Err(error)=result{tracing::warn!(target:"sinomo.observe.usage",%error,"usage retention maintenance failed");}
        }
    }));
}

pub(super) fn retain_recent(records: &mut Vec<UsageRecord>, record: UsageRecord, limit: usize) {
    let position = records
        .binary_search_by(|r| {
            record
                .timestamp_ms
                .cmp(&r.timestamp_ms)
                .then(r.id.cmp(&record.id))
        })
        .unwrap_or_else(|p| p);
    if position < limit {
        records.insert(position, record);
        records.truncate(limit);
    }
}
fn add_history(response: &mut UsageQueryResponse, record: &UsageRecord) {
    let rows = match record.event {
        UsageEvent::QuotaSampled { .. } => &mut response.quota_history,
        UsageEvent::SkillVersionPublished { .. } => &mut response.skill_history,
        _ => return,
    };
    if rows.len() >= 200 {
        response.history_truncated = true;
    }
    retain_recent(rows, record.clone(), 200);
}

const PRUNED_MARKER: &str = "observe.usage.raw.pruned";
fn sync_parent(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(error)?;
    }
    Ok(())
}
fn raw_was_pruned(path: &Path) -> Result<bool, String> {
    let mut line = Vec::new();
    BufReader::new(File::open(path).map_err(error)?)
        .read_until(b'\n', &mut line)
        .map_err(error)?;
    if line.last() != Some(&b'\n') {
        return Ok(false);
    }
    let event: sinomo_core::base::EventEnvelope = serde_json::from_slice(&line).map_err(error)?;
    Ok(event.kind.as_str() == PRUNED_MARKER)
}
fn prune_raw(path: &Path, rollup: &Path, segment: &mut Segment, cutoff: u64) -> Result<(), String> {
    let temporary = path.with_extension("prune-tmp");
    let mut output = File::create(&temporary).map_err(error)?;
    use sinomo_core::base::*;
    let marker = EventEnvelope::new(
        Id::generate(),
        time::OffsetDateTime::now_utc(),
        Id::derived("observe", "retention"),
        Source::new("observe.usage").unwrap(),
        EventKind::new(PRUNED_MARKER).unwrap(),
        Salience::new(0.0).unwrap(),
        Confidence::new(1.0).unwrap(),
        RoutingMetadata::new(
            RoutingScope::Local,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    );
    serde_json::to_writer(&mut output, &marker).map_err(error)?;
    output.write_all(b"\n").map_err(error)?;
    let mut removed = false;
    let mut failure = None;
    visit_records_from(path, 0, |record| {
        // Record identities make a restarted projection replay idempotent if
        // the process stops between the checkpoint and raw-file replacement.
        if record.timestamp_ms >= segment.retained_from_ms {
            segment
                .indexed
                .insert(record.id.clone(), record.timestamp_ms);
        }
        if record.timestamp_ms < cutoff {
            removed = true;
            return;
        }
        if failure.is_none() {
            let result = serde_json::to_writer(&mut output, &record.into_envelope())
                .map_err(error)
                .and_then(|_| output.write_all(b"\n").map_err(error));
            if let Err(e) = result {
                failure = Some(e);
            }
        }
    })?;
    if let Some(error) = failure {
        return Err(error);
    }
    if !removed {
        drop(output);
        fs::remove_file(temporary).map_err(error)?;
        return Ok(());
    }
    output.sync_all().map_err(error)?;
    drop(output);
    // Publish all rollups and replay identities before removing any raw data.
    segment.offset = 0;
    atomic_json(rollup, segment)?;
    fs::rename(&temporary, path).map_err(error)?;
    sync_parent(path)?;
    segment.offset = fs::metadata(path).map_err(error)?.len();
    atomic_json(rollup, segment)
}
