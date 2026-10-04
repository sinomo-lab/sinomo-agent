use sinomo_api::lm::{
    LanguageModelProviderOptions, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
    effective_model_name, model_supports_reasoning_effort, normalize_model_id_for_provider,
};

#[test]
fn shared_settings_preserve_current_reasoning_levels_and_wire_mappings() {
    let options = LanguageModelProviderOptions {
        reasoning_effort: Some(ProviderReasoningEffort::Ultra),
        ..Default::default()
    };
    let value = serde_json::to_value(&options).unwrap();
    assert_eq!(value["reasoning_effort"], "ultra");
    assert_eq!(
        serde_json::from_value::<LanguageModelProviderOptions>(value).unwrap(),
        options
    );
    assert!(ProviderReasoningEffort::VALUES.contains(&"ultra"));
    assert_eq!(
        "ultra".parse::<ProviderReasoningEffort>().unwrap(),
        ProviderReasoningEffort::Ultra
    );
    assert_eq!(
        ProviderReasoningEffort::Max.as_codex_cli_value(),
        Some("max")
    );
    assert_eq!(
        ProviderReasoningEffort::Ultra.as_codex_cli_value(),
        Some("ultra")
    );
    assert_eq!(
        ProviderReasoningEffort::Ultra.as_anthropic_effort_value(),
        Some("max")
    );
}

#[test]
fn shared_provider_defaults_match_the_updated_adapters() {
    for (provider, model) in [
        ("anthropic", "claude-sonnet-5-5"),
        ("claude", "claude-sonnet-5-5"),
        ("gemini", "gemini-3.8-flash"),
        ("deepseek", "deepseek-flash"),
        ("github-copilot", "gpt-6.1-sol"),
        ("openai", "gpt-6.1-sol"),
        ("codex", "gpt-6.1-sol"),
    ] {
        assert_eq!(
            effective_model_name(provider, PROVIDER_DEFAULT_MODEL),
            model
        );
        assert!(model_supports_reasoning_effort(provider, model));
        assert_eq!(
            effective_model_name(provider, "custom-model"),
            "custom-model"
        );
    }
    assert!(model_supports_reasoning_effort("openai", "gpt-6-astra"));
    assert!(!model_supports_reasoning_effort("openai", "gpt-4.1"));
}

#[test]
fn shared_nvidia_normalization_preserves_the_actual_model_owner() {
    assert_eq!(
        normalize_model_id_for_provider("nvidia", "nvidia/moonshotai/kimi-k2.5"),
        "moonshotai/kimi-k2.5"
    );
    assert_eq!(
        normalize_model_id_for_provider("nvidia", "nvidia/nvidia/nemotron-3-super-120b-a12b"),
        "nvidia/nemotron-3-super-120b-a12b"
    );
}

#[test]
fn grok_and_kimi_have_canonical_identities_defaults_and_reasoning_controls() {
    use sinomo_api::lm::*;
    for (alias, provider, model) in [("xai", "grok", "grok-4.7"), ("moonshot", "kimi", "kimi-k3")] {
        assert_eq!(normalize_model_provider(alias).as_deref(), Some(provider));
        assert_eq!(
            effective_model_name(provider, PROVIDER_DEFAULT_MODEL),
            model
        );
        assert!(model_supports_reasoning_effort(provider, model));
    }
    assert!(!model_supports_reasoning_effort("grok", "grok-3"));
    assert!(!model_supports_reasoning_effort("kimi", "moonshot-v1-8k"));
}
