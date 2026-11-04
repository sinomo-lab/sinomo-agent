//! Data-only usage accounting contract. No request content or credentials belong here.
use serde::{Deserialize, Serialize};

/// Counts supplied by a provider. `None` means unreported, never zero.
/// Cache counts are subsets of input; reasoning is a subset of output.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanguageModelUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_images: Option<Vec<ImageUsage>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosted_searches: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub named_units: Vec<NamedUsageUnit>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ImageUsage {
    pub count: u64,
    pub size: Option<String>,
    pub quality: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NamedUsageUnit {
    pub name: String,
    pub count: u64,
}
impl LanguageModelUsage {
    pub fn reported(&self) -> bool {
        self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.cache_read_input_tokens.is_some()
            || self.cache_write_input_tokens.is_some()
            || self.reasoning_tokens.is_some()
            || self.generated_images.is_some()
            || self.hosted_searches.is_some()
            || !self.named_units.is_empty()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UsageAttribution {
    pub node: String,
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
    pub session: Option<String>,
    pub parent_session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_tool_call: Option<String>,
    pub chat: Option<String>,
    pub turn: Option<String>,
    pub task: Option<String>,
    pub operation: Option<String>,
    pub tool_call: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caused_by: Vec<String>,
    pub origin: String,
    pub purpose: String,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UsageStatus {
    Succeeded,
    Failed,
    Cancelled,
    Denied,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageRecord {
    pub id: String,
    pub timestamp_ms: u64,
    pub attribution: UsageAttribution,
    pub event: UsageEvent,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum UsageEvent {
    #[serde(rename = "lm.request.completed")]
    ModelRequest {
        provider: String,
        account: String,
        model: String,
        operation: String,
        status: UsageStatus,
        error_class: Option<String>,
        /// None when the adapter does not expose its internal retry count.
        retry_count: Option<u32>,
        duration_ms: Option<u64>,
        first_output_ms: Option<u64>,
        reported: bool,
        units: LanguageModelUsage,
    },
    #[serde(rename = "agent.tool.call.completed")]
    ToolCall {
        tool: String,
        status: UsageStatus,
        duration_ms: u64,
        detached: bool,
        runtime: String,
        #[serde(default)]
        executing_node: Option<String>,
        mcp_server: Option<String>,
    },
    #[serde(rename = "skill.activated")]
    SkillActivated {
        skill: String,
        version: String,
        content_hash: String,
        trigger: String,
        resource: Option<String>,
    },
    #[serde(rename = "skill.version.published")]
    SkillVersionPublished {
        skill: String,
        previous_version: Option<String>,
        version: String,
        previous_content_hash: Option<String>,
        content_hash: String,
        source: String,
    },
    #[serde(rename = "lm.provider.quota.sampled")]
    QuotaSampled {
        account: String,
        snapshot: crate::lm::LanguageModelProviderUsage,
    },
}

pub const USAGE_QUERY_ROUTE: &str = "/observe/usage/query";
pub const USAGE_RPC_CATEGORY: &str = "observe";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UsageFilter {
    #[serde(default)]
    pub event_kind: Option<String>,
    pub agent: Option<String>,
    pub chat: Option<String>,
    pub session: Option<String>,
    pub provider: Option<String>,
    pub account: Option<String>,
    pub model: Option<String>,
    pub operation: Option<String>,
    pub purpose: Option<String>,
    pub tool: Option<String>,
    pub skill: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UsageQuery {
    #[serde(default)]
    pub group_by: UsageGrouping,
    #[serde(default = "usage_group_limit")]
    pub max_groups: usize,
    pub start_ms: u64,
    pub end_ms: u64,
    /// Empty means the whole cluster.
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub filter: UsageFilter,
    pub bucket_ms: u64,
    #[serde(default)]
    pub cursor: Option<UsageCursor>,
    pub limit: usize,
}
impl Default for UsageQuery {
    fn default() -> Self {
        Self {
            group_by: UsageGrouping::All,
            max_groups: usage_group_limit(),
            start_ms: 0,
            end_ms: u64::MAX,
            nodes: Vec::new(),
            filter: UsageFilter::default(),
            bucket_ms: 3_600_000,
            cursor: None,
            limit: 100,
        }
    }
}
impl UsageQuery {
    pub fn validate(&self) -> Result<(), String> {
        if self.start_ms >= self.end_ms
            || self.bucket_ms == 0
            || self.limit > 1000
            || self.max_groups == 0
            || self.max_groups > 10000
        {
            return Err("usage query needs an increasing range, positive bucket width, and at most 1000 records".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageTotals {
    #[serde(default)]
    pub reported_requests: u64,
    #[serde(default)]
    pub retries: u64,
    #[serde(default)]
    pub retry_reporting_requests: u64,
    #[serde(default)]
    pub agents: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub runtimes: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub executing_nodes: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub last_used_ms: Option<u64>,
    #[serde(default, with = "latency_histogram")]
    pub model_latency_ms: std::collections::BTreeMap<u64, u64>,
    #[serde(default, with = "latency_histogram")]
    pub tool_latency_ms: std::collections::BTreeMap<u64, u64>,
    #[serde(default)]
    pub statuses: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub skill_triggers: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub detached_tool_calls: u64,
    pub model_requests: u64,
    pub model_errors: u64,
    pub tool_calls: u64,
    pub tool_errors: u64,
    pub skill_activations: u64,
    pub skill_publications: u64,
    pub units: LanguageModelUsage,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageQueryResponse {
    #[serde(default)]
    pub record_costs: std::collections::BTreeMap<String, UsageCost>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub quota_history: Vec<UsageRecord>,
    #[serde(default)]
    pub skill_history: Vec<UsageRecord>,
    #[serde(default)]
    pub history_truncated: bool,
    #[serde(default)]
    pub ledger_sizes: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub groups: Vec<UsageGroup>,
    #[serde(default)]
    pub budgets: Vec<UsageBudgetProgress>,
    #[serde(default)]
    pub cost: UsageCost,
    pub totals: UsageTotals,
    pub series: Vec<UsageBucket>,
    pub records: Vec<UsageRecord>,
    pub next_cursor: Option<UsageCursor>,
    pub answered: Vec<String>,
    pub failed: Vec<UsageNodeFailure>,
    pub omitted: Vec<String>,
    pub ledger_bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageBucket {
    #[serde(default)]
    pub cost: UsageCost,
    #[serde(default)]
    pub providers: std::collections::BTreeMap<String, UsageCost>,
    pub start_ms: u64,
    pub totals: UsageTotals,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageNodeFailure {
    pub node: String,
    pub error: String,
}
impl UsageQueryResponse {
    pub fn partial(&self) -> bool {
        self.truncated || !self.failed.is_empty() || !self.omitted.is_empty()
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UsageRetention {
    pub raw_seconds: Option<u64>,
    pub rollup_seconds: Option<u64>,
}
impl UsageRetention {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(rollup) = self.rollup_seconds {
            if self.raw_seconds.is_none_or(|raw| rollup < raw) {
                return Err("rollup retention must be at least raw retention".into());
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageSettings {
    #[serde(default)]
    pub retention: UsageRetention,
    #[serde(default)]
    pub prices: Vec<UsagePrice>,
    #[serde(default)]
    pub accounts: Vec<UsageBillingAccount>,
    #[serde(default)]
    pub budgets: Vec<UsageBudget>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsagePrice {
    pub provider: String,
    pub model_pattern: String,
    pub operation: String,
    pub effective_from_ms: u64,
    pub currency: String,
    /// Currency per million tokens, or per single image/search/named unit.
    pub units: std::collections::BTreeMap<String, f64>,
    pub source: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageBillingAccount {
    pub provider: String,
    pub account: String,
    pub mode: BillingMode,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BillingMode {
    #[default]
    Metered,
    Subscription,
    Local,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageBudget {
    pub name: String,
    pub scope: UsageFilter,
    pub period: BudgetPeriod,
    pub currency: String,
    pub amount: f64,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BudgetPeriod {
    Day,
    Week,
    Month,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageCost {
    #[serde(default)]
    pub price_sources: Vec<String>,
    pub amounts: std::collections::BTreeMap<String, f64>,
    pub priced_requests: u64,
    pub unpriced_requests: u64,
    pub subscription_requests: u64,
    pub local_requests: u64,
    pub unpriced_models: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageBudgetProgress {
    pub name: String,
    pub currency: String,
    pub amount: f64,
    pub spent: f64,
    pub projected: f64,
    pub period_start_ms: u64,
    pub period_end_ms: u64,
    pub unpriced_requests: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageGroup {
    pub dimensions: UsageFilter,
    pub node: String,
    pub totals: UsageTotals,
    pub cost: UsageCost,
}

pub fn latency_percentile(
    histogram: &std::collections::BTreeMap<u64, u64>,
    percent: u64,
) -> Option<u64> {
    let total = histogram.values().copied().fold(0u64, u64::saturating_add);
    if total == 0 {
        return None;
    }
    let target = ((total as u128 * percent.min(100) as u128).div_ceil(100)).max(1);
    let mut count = 0u128;
    for (latency, n) in histogram {
        count += *n as u128;
        if count >= target {
            return Some(*latency);
        }
    }
    None
}

pub mod pricing;

impl UsageEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ModelRequest { .. } => "lm.request.completed",
            Self::ToolCall { .. } => "agent.tool.call.completed",
            Self::SkillActivated { .. } => "skill.activated",
            Self::SkillVersionPublished { .. } => "skill.version.published",
            Self::QuotaSampled { .. } => "lm.provider.quota.sampled",
        }
    }
}
impl UsageRecord {
    pub fn into_envelope(self) -> sinomo_core::base::EventEnvelope {
        use sinomo_core::base::*;
        let origin = Id::new(self.attribution.node.clone())
            .unwrap_or_else(|_| Id::derived("observe-producer", "unassigned"));
        let mut envelope = EventEnvelope::new(
            Id::new(self.id.clone()).unwrap_or_else(|_| Id::generate()),
            time::OffsetDateTime::from_unix_timestamp_nanos(self.timestamp_ms as i128 * 1_000_000)
                .unwrap_or_else(|_| time::OffsetDateTime::now_utc()),
            origin,
            Source::new("usage").unwrap(),
            EventKind::new(self.event.kind()).unwrap(),
            Salience::new(0.1).unwrap(),
            Confidence::new(1.0).unwrap(),
            RoutingMetadata::new(
                RoutingScope::Local,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        );
        if let Some(session) = self
            .attribution
            .session
            .as_ref()
            .and_then(|s| Id::new(s.clone()).ok())
        {
            envelope = envelope.with_session(session);
        }
        for cause in &self.attribution.caused_by {
            if let Ok(id) = Id::new(cause.clone()) {
                envelope.links.caused_by.push(id);
            }
        }
        envelope.with_payload(serde_json::to_value(self).expect("usage data is serializable"))
    }
}
impl UsageAttribution {
    pub fn inherit_from(&mut self, parent: &Self) {
        macro_rules! fields {($($field:ident),*)=>{$(if self.$field.is_none(){self.$field=parent.$field.clone();})*}}
        fields!(
            agent,
            participant,
            session,
            parent_session,
            parent_operation,
            parent_tool_call,
            chat,
            turn,
            task,
            operation,
            tool_call
        );
        if self.caused_by.is_empty() {
            self.caused_by = parent.caused_by.clone();
        }
        if self.node.is_empty() {
            self.node = parent.node.clone();
        }
        if self.origin.is_empty() {
            self.origin = parent.origin.clone();
        }
        if self.purpose.is_empty() {
            self.purpose = parent.purpose.clone();
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UsageCursor {
    pub timestamp_ms: u64,
    pub id: String,
}
impl UsageCursor {
    pub fn after(&self, record: &UsageRecord) -> bool {
        record.timestamp_ms < self.timestamp_ms
            || (record.timestamp_ms == self.timestamp_ms && record.id > self.id)
    }
    pub fn from_record(record: &UsageRecord) -> Self {
        Self {
            timestamp_ms: record.timestamp_ms,
            id: record.id.clone(),
        }
    }
}

pub fn merge_totals(a: &mut UsageTotals, b: &UsageTotals) {
    macro_rules! sums {($($field:ident),*)=>{$(a.$field=a.$field.saturating_add(b.$field);)*}}
    sums!(
        model_requests,
        reported_requests,
        retries,
        retry_reporting_requests,
        model_errors,
        tool_calls,
        tool_errors,
        skill_activations,
        skill_publications,
        detached_tool_calls
    );
    macro_rules! maps {($($field:ident),*)=>{$(for (key,count) in &b.$field {let value=a.$field.entry(key.clone()).or_default();*value=value.saturating_add(*count);})*}}
    maps!(
        model_latency_ms,
        tool_latency_ms,
        statuses,
        skill_triggers,
        runtimes,
        executing_nodes,
        agents
    );
    a.last_used_ms = a.last_used_ms.max(b.last_used_ms);
    a.units.add(&b.units);
}

fn usage_group_limit() -> usize {
    1000
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UsageGrouping {
    #[default]
    All,
    Provider,
    Tool,
    Skill,
    Agent,
    Chat,
    Purpose,
    Node,
}

// SOBJ object keys are strings. Encode numeric histograms as entries instead
// of relying on JSON's special numeric-map-key coercion. Read older retained
// JSON projections as well, without changing the shared network deserializer.
mod latency_histogram {
    use super::*;
    use std::collections::BTreeMap;
    pub fn serialize<S: serde::Serializer>(
        value: &BTreeMap<u64, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<u64, u64>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Entries(Vec<(u64, u64)>),
            Legacy(BTreeMap<String, u64>),
        }
        let entries = match Wire::deserialize(deserializer)? {
            Wire::Entries(entries) => entries,
            Wire::Legacy(map) => map
                .into_iter()
                .map(|(key, value)| {
                    key.parse::<u64>()
                        .map(|key| (key, value))
                        .map_err(serde::de::Error::custom)
                })
                .collect::<Result<Vec<_>, D::Error>>()?,
        };
        let mut result = BTreeMap::new();
        for (key, value) in entries {
            if result.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate latency bucket"));
            }
        }
        Ok(result)
    }
}
#[cfg(test)]
mod wire_tests {
    use super::*;
    #[test]
    fn nonempty_histograms_round_trip_through_sobj_and_json() {
        let mut value = UsageQueryResponse::default();
        value.totals.model_latency_ms.insert(20, 3);
        value.totals.tool_latency_ms.insert(70, 2);
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            USAGE_QUERY_ROUTE,
            USAGE_RPC_CATEGORY,
            &value,
        )
        .unwrap();
        let decoded: UsageQueryResponse = rpc.decode_body().unwrap();
        assert_eq!(decoded, value);
        assert_eq!(
            serde_json::from_value::<UsageQueryResponse>(serde_json::to_value(&value).unwrap())
                .unwrap(),
            value
        );
    }
}

impl LanguageModelUsage {
    /// Add independent requests/attempts, preserving missing-versus-zero units.
    pub fn add(&mut self, other: &Self) {
        macro_rules! units {($($field:ident),*)=>{$(if let Some(n)=other.$field{self.$field=Some(self.$field.unwrap_or(0).saturating_add(n));})*}}
        units!(
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_write_input_tokens,
            reasoning_tokens,
            hosted_searches
        );
        if let Some(images) = &other.generated_images {
            let totals = self.generated_images.get_or_insert_with(Vec::new);
            for image in images {
                if let Some(old) = totals
                    .iter_mut()
                    .find(|old| old.size == image.size && old.quality == image.quality)
                {
                    old.count = old.count.saturating_add(image.count);
                } else {
                    totals.push(image.clone());
                }
            }
        }
        for unit in &other.named_units {
            if let Some(old) = self
                .named_units
                .iter_mut()
                .find(|old| old.name == unit.name)
            {
                old.count = old.count.saturating_add(unit.count);
            } else {
                self.named_units.push(unit.clone());
            }
        }
    }
}
