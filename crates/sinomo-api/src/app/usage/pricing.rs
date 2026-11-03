//! Query-time estimates; no request admission code reads this catalog or budgets.
use super::*;
use std::collections::BTreeMap;
pub const SETTINGS_KEY: &str = "usage.accounting";
pub fn default_prices() -> Vec<UsagePrice> {
    // Standard rates: https://developers.openai.com/api/docs/models/gpt-4.1-mini
    vec![UsagePrice {
        provider: "openai".into(),
        model_pattern: "gpt-4.1-mini*".into(),
        operation: "completion".into(),
        effective_from_ms: 1_744_588_800_000,
        currency: "USD".into(),
        units: BTreeMap::from([
            ("input_tokens".into(), 0.40),
            ("cache_read_input_tokens".into(), 0.10),
            ("output_tokens".into(), 1.60),
        ]),
        source: "https://developers.openai.com/api/docs/models/gpt-4.1-mini".into(),
    }]
}
pub fn estimate(record: &UsageRecord, settings: &UsageSettings) -> UsageCost {
    let UsageEvent::ModelRequest {
        provider,
        account,
        model,
        operation,
        reported,
        units,
        ..
    } = &record.event
    else {
        return UsageCost::default();
    };
    let mut result = UsageCost::default();
    match settings
        .accounts
        .iter()
        .find(|a| a.provider == *provider && a.account == *account)
        .map(|a| a.mode)
        .unwrap_or_default()
    {
        BillingMode::Subscription => {
            result.subscription_requests = 1;
            return result;
        }
        BillingMode::Local => {
            result.local_requests = 1;
            result.priced_requests = 1;
            return result;
        }
        BillingMode::Metered => {}
    }
    let defaults = default_prices();
    let matches = |p: &&UsagePrice| {
        p.provider == *provider
            && p.operation == *operation
            && p.effective_from_ms <= record.timestamp_ms
            && pattern(&p.model_pattern, model)
    };
    let price = settings
        .prices
        .iter()
        .filter(matches)
        .max_by_key(|p| {
            (
                p.effective_from_ms,
                p.model_pattern.trim_end_matches('*').len(),
            )
        })
        .or_else(|| {
            defaults.iter().filter(matches).max_by_key(|p| {
                (
                    p.effective_from_ms,
                    p.model_pattern.trim_end_matches('*').len(),
                )
            })
        });
    let value = price
        .filter(|_| *reported && units.reported())
        .and_then(|price| cost(units, price).map(|amount| (price.currency.clone(), amount)));
    if let Some((currency, amount)) = value {
        if let Some(price) = price {
            result.price_sources.push(price.source.clone());
        }
        result.priced_requests = 1;
        result.amounts.insert(currency, amount);
    } else {
        result.unpriced_requests = 1;
        result.unpriced_models.push(format!("{provider}/{model}"));
    }
    result
}
fn pattern(pattern: &str, value: &str) -> bool {
    pattern
        .strip_suffix('*')
        .map(|prefix| value.starts_with(prefix))
        .unwrap_or(pattern == value)
}
fn cost(u: &LanguageModelUsage, p: &UsagePrice) -> Option<f64> {
    let mut amount = 0.0;
    let mut add = |name: &str, count: Option<u64>, divisor: f64| -> Option<()> {
        if let Some(count) = count.filter(|n| *n > 0) {
            let rate = *p.units.get(name)?;
            if !rate.is_finite() || rate < 0.0 {
                return None;
            }
            amount += count as f64 * rate / divisor;
        }
        Some(())
    };
    let cache = u
        .cache_read_input_tokens
        .unwrap_or(0)
        .checked_add(u.cache_write_input_tokens.unwrap_or(0))?;
    let input = match u.input_tokens {
        Some(n) => Some(n.checked_sub(cache)?),
        None if cache > 0 => return None,
        None => None,
    };
    add("input_tokens", input, 1e6)?;
    add("cache_read_input_tokens", u.cache_read_input_tokens, 1e6)?;
    add("cache_write_input_tokens", u.cache_write_input_tokens, 1e6)?;
    // A separate reasoning price replaces the output rate for that subset.
    let output = if p.units.contains_key("reasoning_tokens") {
        let n = u
            .output_tokens?
            .checked_sub(u.reasoning_tokens.unwrap_or(0))?;
        add("reasoning_tokens", u.reasoning_tokens, 1e6)?;
        Some(n)
    } else {
        u.output_tokens
    };
    add("output_tokens", output, 1e6)?;
    add("hosted_searches", u.hosted_searches, 1.0)?;
    if let Some(images) = &u.generated_images {
        for image in images {
            add(
                &format!(
                    "image:{}:{}",
                    image.size.as_deref().unwrap_or("unknown"),
                    image.quality.as_deref().unwrap_or("unknown")
                ),
                Some(image.count),
                1.0,
            )?;
        }
    }
    for unit in &u.named_units {
        add(&unit.name, Some(unit.count), 1.0)?;
    }
    amount.is_finite().then_some(amount)
}
pub fn merge_cost(a: &mut UsageCost, b: &UsageCost) {
    for source in &b.price_sources {
        if !a.price_sources.contains(source) {
            a.price_sources.push(source.clone());
        }
    }
    for (currency, amount) in &b.amounts {
        *a.amounts.entry(currency.clone()).or_default() += amount;
    }
    a.priced_requests += b.priced_requests;
    a.unpriced_requests += b.unpriced_requests;
    a.subscription_requests += b.subscription_requests;
    a.local_requests += b.local_requests;
    for model in &b.unpriced_models {
        if !a.unpriced_models.contains(model) {
            a.unpriced_models.push(model.clone());
        }
    }
}
pub fn config_definition() -> Result<sinomo_config::ConfigDefinition, sinomo_config::ConfigError> {
    use sinomo_config::*;
    ConfigDefinition::new(
        SETTINGS_KEY,
        "sinomo-observe",
        ConfigValueType::Object {
            schema_name: "UsageSettings".into(),
        },
        "Usage retention, price overrides, billing modes, and display-only budgets.",
        ConfigClusterMode::ClusterOnly,
    )?
    .with_default(&UsageSettings::default())?
    .with_schema(settings_schema())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> UsageRecord {
        UsageRecord {
            id: "test".into(),
            timestamp_ms: u64::MAX,
            attribution: Default::default(),
            event: UsageEvent::ModelRequest {
                provider: "openai".into(),
                account: "default".into(),
                model: "gpt-4.1-mini".into(),
                operation: "completion".into(),
                status: UsageStatus::Succeeded,
                error_class: None,
                retry_count: None,
                duration_ms: Some(1),
                first_output_ms: None,
                reported: true,
                units: LanguageModelUsage {
                    input_tokens: Some(1_000_000),
                    cache_read_input_tokens: Some(500_000),
                    output_tokens: Some(1_000_000),
                    ..Default::default()
                },
            },
        }
    }
    #[test]
    fn override_reprices_history_without_double_counting_cache() {
        let r = record();
        let mut s = UsageSettings::default();
        assert!((estimate(&r, &s).amounts["USD"] - 1.85).abs() < 1e-9);
        let mut p = default_prices().remove(0);
        p.units.insert("output_tokens".into(), 0.0);
        s.prices.push(p);
        assert!((estimate(&r, &s).amounts["USD"] - 0.25).abs() < 1e-9);
    }
    #[test]
    fn unreported_and_missing_prices_are_unpriced() {
        let mut r = record();
        if let UsageEvent::ModelRequest { reported, .. } = &mut r.event {
            *reported = false;
        }
        assert_eq!(estimate(&r, &UsageSettings::default()).unpriced_requests, 1);
    }
    #[test]
    fn subscription_and_local_do_not_need_metered_prices() {
        for mode in [BillingMode::Subscription, BillingMode::Local] {
            let s = UsageSettings {
                accounts: vec![UsageBillingAccount {
                    provider: "openai".into(),
                    account: "default".into(),
                    mode,
                }],
                ..Default::default()
            };
            let c = estimate(&record(), &s);
            assert!(c.amounts.is_empty());
            assert_eq!(c.unpriced_requests, 0);
            assert_eq!(c.subscription_requests + c.local_requests, 1);
        }
    }
    #[test]
    fn retention_order_is_enforced_by_the_shared_configuration_schema() {
        let definition = config_definition().unwrap();
        assert!(
            definition
                .validate_value(
                    &serde_json::json!({"retention":{"raw_seconds":null,"rollup_seconds":60}})
                )
                .is_err()
        );
        assert!(
            definition
                .validate_value(
                    &serde_json::json!({"retention":{"raw_seconds":120,"rollup_seconds":60}})
                )
                .is_err()
        );
        definition
            .validate_value(
                &serde_json::json!({"retention":{"raw_seconds":60,"rollup_seconds":120}}),
            )
            .unwrap();
        definition
            .validate_value(&serde_json::json!({"retention":{}}))
            .unwrap();
    }
}

fn settings_schema() -> sinomo_config::ConfigSchema {
    use sinomo_config::*;
    let text = || ConfigSchemaValue::string();
    let field = |name: &str, label: &str, description: &str, value| {
        ConfigSchemaField::new(name, label, description, value)
    };
    let choices = |values: &[&str]| {
        ConfigSchemaValue::string_choices(
            values
                .iter()
                .map(|v| ConfigSchemaChoice::new(*v, *v))
                .collect(),
        )
    };
    let nonnegative = || {
        ConfigSchemaValue::f64(Some(ConfigNumberRange {
            min: Some(0.0),
            max: None,
        }))
    };
    let scope = ConfigSchemaValue::object(
        [
            "agent",
            "chat",
            "session",
            "provider",
            "account",
            "model",
            "operation",
            "purpose",
            "tool",
            "skill",
        ]
        .into_iter()
        .map(|name| field(name, name, "Optional exact usage filter.", text()).optional())
        .collect(),
    );
    ConfigSchema::object("UsageSettings","Durable usage accounting. Budgets are display-only; estimates do not affect requests.",vec![
        field("retention","Retention","Omitted values retain history forever. Rollups must be kept at least as long as raw records.",ConfigSchemaValue::object(vec![
            field("raw_seconds","Raw history (seconds)","Blank retains raw records forever.",ConfigSchemaValue::u64(None)).optional(),
            field("rollup_seconds","Rollup history (seconds)","Blank retains rollups forever.",ConfigSchemaValue::u64(None)).optional(),
        ])).optional(),
        field("accounts","Billing modes","Choose metered, subscription, or local billing per provider account.",ConfigSchemaValue::list(ConfigSchemaValue::object(vec![field("provider","Provider","Provider id.",text()),field("account","Account","Provider account name.",text()),field("mode","Billing mode","Subscription requests are included in plan; local requests cost zero.",choices(&["metered","subscription","local"]))]))).optional(),
        field("prices","Price overrides","Editing prices re-prices retained history. Token rates are per million tokens; other rates are per unit.",ConfigSchemaValue::list(ConfigSchemaValue::object(vec![
            field("provider","Provider","Provider id.",text()),field("model_pattern","Model pattern","Exact model id or a prefix ending in *.",text()),field("operation","Operation","completion, image_generation, web_search, or another provider operation.",text()),field("effective_from_ms","Effective from","Unix timestamp in milliseconds.",ConfigSchemaValue::u64(None)),field("currency","Currency","ISO currency code.",text()),field("units","Unit prices","Token field names, hosted_searches, image:SIZE:QUALITY, or named units.",ConfigSchemaValue::map(nonnegative())),field("source","Price source","Catalog URL or operator price source.",text()),
        ]))).optional(),
        field("budgets","Budgets","Display-only spending targets; never enforce a limit.",ConfigSchemaValue::list(ConfigSchemaValue::object(vec![field("name","Name","Unique display name.",text()),field("scope","Scope","Empty scope means the whole cluster.",scope),field("period","Period","UTC day, Monday-based week, or calendar month.",choices(&["day","week","month"])),field("currency","Currency","Budget currency.",text()),field("amount","Amount","Spending target for this period.",nonnegative())]))).optional(),
    ]).with_optional_u64_order("/retention/raw_seconds","/retention/rollup_seconds")
}
