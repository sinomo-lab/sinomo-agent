//! Usage instruments use Observe's existing metrics recorder; no parallel counter store.
use super::*;
pub(crate) fn record(record: &UsageRecord) {
    let status = |s: &UsageStatus| match s {
        UsageStatus::Succeeded => "succeeded",
        UsageStatus::Failed => "failed",
        UsageStatus::Cancelled => "cancelled",
        UsageStatus::Denied => "denied",
    };
    match &record.event {
        UsageEvent::ModelRequest {
            provider,
            status: s,
            units,
            duration_ms,
            retry_count,
            ..
        } => {
            let provider = sinomo_api::lm::normalize_model_provider(provider).unwrap_or_else(
                || match provider.as_str() {
                    "minicpm" | "minicpm-o" | "rust-minicpm-v46" | "pytorch" => provider.clone(),
                    _ => "other".into(),
                },
            );
            ::metrics::counter!("sinomo.usage.model.requests","provider"=>provider.clone(),"status"=>status(s)).increment(1);
            if let Some(duration_ms) = duration_ms {
                ::metrics::histogram!("sinomo.usage.model.duration_seconds","provider"=>provider.clone()).record(*duration_ms as f64/1000.0);
            }
            if let Some(retries) = retry_count {
                ::metrics::counter!("sinomo.usage.model.retries","provider"=>provider.clone())
                    .increment(*retries as u64);
            }
            for (unit, count) in [
                ("input", units.input_tokens),
                ("output", units.output_tokens),
                ("cache_read", units.cache_read_input_tokens),
                ("cache_write", units.cache_write_input_tokens),
                ("reasoning", units.reasoning_tokens),
            ] {
                if let Some(count) = count {
                    ::metrics::counter!("sinomo.usage.model.tokens","provider"=>provider.clone(),"unit"=>unit).increment(count);
                }
            }
        }
        UsageEvent::ToolCall {
            status: s,
            duration_ms,
            ..
        } => {
            ::metrics::counter!("sinomo.usage.tool.calls","status"=>status(s)).increment(1);
            ::metrics::histogram!("sinomo.usage.tool.duration_seconds")
                .record(*duration_ms as f64 / 1000.0);
        }
        UsageEvent::SkillActivated { .. } => {
            ::metrics::counter!("sinomo.usage.skill.activations").increment(1)
        }
        UsageEvent::SkillVersionPublished { .. } => {
            ::metrics::counter!("sinomo.usage.skill.publications").increment(1)
        }
        _ => {}
    }
}
pub(crate) fn register() {
    for name in [
        "sinomo.usage.model.requests",
        "sinomo.usage.model.tokens",
        "sinomo.usage.model.retries",
        "sinomo.usage.tool.calls",
        "sinomo.usage.skill.activations",
        "sinomo.usage.skill.publications",
        "sinomo.observe.events.delivery_failures",
    ] {
        ::metrics::describe_counter!(
            name,
            ::metrics::Unit::Count,
            "Retained usage and observation delivery outcomes."
        );
    }
    for name in [
        "sinomo.usage.model.duration_seconds",
        "sinomo.usage.tool.duration_seconds",
    ] {
        ::metrics::describe_histogram!(
            name,
            ::metrics::Unit::Seconds,
            "Duration of completed agent work."
        );
    }
}
