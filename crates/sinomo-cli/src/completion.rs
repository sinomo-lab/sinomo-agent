use std::borrow::Cow;

use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliAutocompleteOption, CliAutocompleteSpec, CliAutocompleteValue,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SlashCompletionSet {
    pub replace_start: usize,
    pub replace_end: usize,
    pub items: Vec<SlashCompletionItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SlashCompletionItem {
    pub label: String,
    pub detail: String,
    pub insert: Option<String>,
    pub kind: SlashCompletionKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SlashCompletionKind {
    Command,
    Option,
    Value,
    Hint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlashCommandCompletionLayer {
    #[cfg(test)]
    Standard,
    Tui,
}

#[derive(Clone, Debug)]
struct CommandSpec {
    word: Cow<'static, str>,
    detail: Cow<'static, str>,
    children: Vec<CommandSpec>,
    options: Vec<OptionSpec>,
    values: Vec<ValueSpec>,
}

#[derive(Clone, Debug)]
struct OptionSpec {
    name: Cow<'static, str>,
    detail: Cow<'static, str>,
    value: Option<ValueSpec>,
}

#[derive(Clone, Debug)]
struct ValueSpec {
    placeholder: Cow<'static, str>,
    detail: Cow<'static, str>,
    choices: Vec<ValueChoice>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValueChoice {
    value: Cow<'static, str>,
    detail: Cow<'static, str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompletionToken {
    text: String,
    start: usize,
    end: usize,
}

#[derive(Clone, Debug)]
struct TuiModelCatalog {
    provider: String,
    detail: String,
    models: Vec<String>,
}

#[derive(Clone, Debug)]
struct CachedTuiModelCatalogs {
    loaded_at: std::time::Instant,
    catalogs: Vec<TuiModelCatalog>,
}

const TUI_MODEL_CATALOG_TTL: std::time::Duration = std::time::Duration::from_secs(30);
static TUI_MODEL_CATALOG_CACHE: std::sync::OnceLock<
    std::sync::Mutex<Option<CachedTuiModelCatalogs>>,
> = std::sync::OnceLock::new();

const TUI_FILE_TARGET_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: Cow::Borrowed("sifs:/"),
        detail: Cow::Borrowed("browse the SIFS root"),
    },
    ValueChoice {
        value: Cow::Borrowed("sifs:/sinomo/nodes"),
        detail: Cow::Borrowed("browse node-owned SIFS files"),
    },
    ValueChoice {
        value: Cow::Borrowed("sifs:/services"),
        detail: Cow::Borrowed("browse service-owned SIFS files"),
    },
    ValueChoice {
        value: Cow::Borrowed("sifs:/users"),
        detail: Cow::Borrowed("browse user-owned SIFS files"),
    },
    ValueChoice {
        value: Cow::Borrowed("local:."),
        detail: Cow::Borrowed("browse local files from the current directory"),
    },
];

const TUI_AGENT_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: Cow::Borrowed("main-agent"),
        detail: Cow::Borrowed("user-facing interactive agent"),
    },
    ValueChoice {
        value: Cow::Borrowed("background-agent"),
        detail: Cow::Borrowed("silent maintenance and cluster-support agent"),
    },
    ValueChoice {
        value: Cow::Borrowed("coding-agent"),
        detail: Cow::Borrowed("workspace coding and verification agent"),
    },
    ValueChoice {
        value: Cow::Borrowed("evolution-agent"),
        detail: Cow::Borrowed("supervised self-evolution mutation agent"),
    },
];

#[cfg(not(test))]
const PROVIDER_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: Cow::Borrowed("codex"),
        detail: Cow::Borrowed("Codex CLI provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("github-copilot"),
        detail: Cow::Borrowed("GitHub Copilot provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("openai"),
        detail: Cow::Borrowed("OpenAI API provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("anthropic"),
        detail: Cow::Borrowed("Anthropic API provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("deepseek"),
        detail: Cow::Borrowed("DeepSeek API provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("gemini"),
        detail: Cow::Borrowed("Gemini API provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("nvidia"),
        detail: Cow::Borrowed("NVIDIA API provider"),
    },
    ValueChoice {
        value: Cow::Borrowed("minicpm"),
        detail: Cow::Borrowed("local MiniCPM runtime"),
    },
];

const THINKING_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: Cow::Borrowed("on"),
        detail: Cow::Borrowed("show model thinking stream when available"),
    },
    ValueChoice {
        value: Cow::Borrowed("off"),
        detail: Cow::Borrowed("hide model thinking stream"),
    },
    ValueChoice {
        value: Cow::Borrowed("status"),
        detail: Cow::Borrowed("show current thinking-stream mode"),
    },
];

#[cfg(test)]
fn slash_completions(input: &str, cursor: usize) -> Option<SlashCompletionSet> {
    slash_completions_for_layer(input, cursor, SlashCommandCompletionLayer::Standard)
}

pub(crate) fn slash_completions_for_layer(
    input: &str,
    cursor: usize,
    layer: SlashCommandCompletionLayer,
) -> Option<SlashCompletionSet> {
    if cursor > input.len() || !input.is_char_boundary(cursor) || !input.starts_with('/') {
        return None;
    }

    let (tokens, replace_start, replace_end, prefix) = completion_tokens(input, cursor)?;
    if tokens.iter().any(|token| token.text == "--local") {
        return None;
    }
    if layer == SlashCommandCompletionLayer::Tui
        && let Some(completions) =
            tui_model_quick_completions(&tokens, replace_start, replace_end, &prefix)
    {
        return Some(completions);
    }
    let mut registry = slash_command_registry();
    if layer == SlashCommandCompletionLayer::Tui {
        apply_tui_slash_command_layer(&mut registry);
    }
    let (node, command_consumed) = resolve_command_node(&registry, &tokens);
    let tokens_before_current = tokens_before_current(&tokens, replace_start);
    let options = available_options(&registry, node);

    let mut items = Vec::new();
    if let Some(value) = expected_value_spec(&tokens_before_current, &options)
        && !prefix.starts_with("--")
    {
        push_value_items(&mut items, value, &prefix);
    } else {
        let completing_command =
            tokens_before_current.len() == command_consumed && !prefix.starts_with("--");
        if completing_command {
            for child in &node.children {
                push_word_item(
                    &mut items,
                    child.word.as_ref(),
                    child.detail.as_ref(),
                    SlashCompletionKind::Command,
                    &prefix,
                    true,
                );
            }
        }

        if !completing_command || node.children.is_empty() || prefix.starts_with("--") {
            for option in &options {
                push_word_item(
                    &mut items,
                    option.name.as_ref(),
                    option.detail.as_ref(),
                    SlashCompletionKind::Option,
                    &prefix,
                    true,
                );
            }
            if !prefix.starts_with("--") {
                push_next_value_hint(
                    &mut items,
                    node,
                    &options,
                    &tokens_before_current,
                    command_consumed,
                    &prefix,
                );
            }
        }
    }

    if items.is_empty() {
        return None;
    }
    Some(SlashCompletionSet {
        replace_start,
        replace_end,
        items,
    })
}

fn completion_tokens(
    input: &str,
    cursor: usize,
) -> Option<(Vec<CompletionToken>, usize, usize, String)> {
    let before_cursor = &input[..cursor];
    let command = before_cursor.strip_prefix('/')?;
    let tokens = tokenize_command(command, 1);
    if before_cursor.ends_with(char::is_whitespace) {
        return Some((tokens, cursor, cursor, String::new()));
    }
    if let Some(current) = tokens.last() {
        return Some((
            tokens.clone(),
            current.start,
            current.end,
            current.text.clone(),
        ));
    }
    Some((tokens, cursor, cursor, String::new()))
}

fn tokenize_command(input: &str, offset: usize) -> Vec<CompletionToken> {
    let mut tokens = Vec::new();
    let mut token_start = None;
    for (index, character) in input.char_indices() {
        if character.is_whitespace() {
            if let Some(start) = token_start.take() {
                tokens.push(CompletionToken {
                    text: input[start..index].to_string(),
                    start: offset + start,
                    end: offset + index,
                });
            }
        } else if token_start.is_none() {
            token_start = Some(index);
        }
    }
    if let Some(start) = token_start {
        tokens.push(CompletionToken {
            text: input[start..].to_string(),
            start: offset + start,
            end: offset + input.len(),
        });
    }
    tokens
}

fn tokens_before_current(tokens: &[CompletionToken], replace_start: usize) -> Vec<String> {
    tokens
        .iter()
        .filter(|token| token.start < replace_start)
        .map(|token| token.text.clone())
        .collect()
}

fn resolve_command_node<'a>(
    registry: &'a CommandSpec,
    tokens: &[CompletionToken],
) -> (&'a CommandSpec, usize) {
    let mut node = registry;
    let mut consumed = 0;
    for token in tokens {
        if token.text == "--local" {
            consumed += 1;
            continue;
        }
        let Some(child) = node
            .children
            .iter()
            .find(|child| child.word.as_ref() == token.text)
        else {
            break;
        };
        node = child;
        consumed += 1;
    }
    (node, consumed)
}

fn available_options(registry: &CommandSpec, node: &CommandSpec) -> Vec<OptionSpec> {
    let mut options = registry.options.clone();
    options.extend(node.options.iter().cloned());
    options
}

fn expected_value_spec<'a>(
    tokens_before_current: &[String],
    options: &'a [OptionSpec],
) -> Option<&'a ValueSpec> {
    let previous = tokens_before_current.last()?;
    options
        .iter()
        .find(|option| option.name.as_ref() == previous)
        .and_then(|option| option.value.as_ref())
}

fn push_next_value_hint(
    items: &mut Vec<SlashCompletionItem>,
    node: &CommandSpec,
    options: &[OptionSpec],
    tokens_before_current: &[String],
    command_consumed: usize,
    prefix: &str,
) {
    if !prefix.is_empty() || node.values.is_empty() {
        return;
    }
    let positional_count =
        positional_count_after_command(tokens_before_current, command_consumed, options);
    if let Some(value) = node.values.get(positional_count) {
        push_value_items(items, value, prefix);
    }
}

fn positional_count_after_command(
    tokens_before_current: &[String],
    command_consumed: usize,
    options: &[OptionSpec],
) -> usize {
    let mut positional_count = 0;
    let mut index = command_consumed;
    while let Some(token) = tokens_before_current.get(index) {
        if token.starts_with("--") {
            if option_expects_value(options, token)
                && tokens_before_current
                    .get(index + 1)
                    .is_some_and(|next| !next.starts_with("--"))
            {
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        positional_count += 1;
        index += 1;
    }
    positional_count
}

fn option_expects_value(options: &[OptionSpec], name: &str) -> bool {
    options
        .iter()
        .any(|option| option.name.as_ref() == name && option.value.is_some())
}

fn push_value_items(items: &mut Vec<SlashCompletionItem>, value: &ValueSpec, prefix: &str) {
    if value.choices.is_empty() {
        if prefix.is_empty() {
            items.push(SlashCompletionItem {
                label: value.placeholder.to_string(),
                detail: value.detail.to_string(),
                insert: None,
                kind: SlashCompletionKind::Hint,
            });
        }
        return;
    }

    for choice in &value.choices {
        push_word_item(
            items,
            choice.value.as_ref(),
            choice.detail.as_ref(),
            SlashCompletionKind::Value,
            prefix,
            true,
        );
    }
}

fn push_word_item(
    items: &mut Vec<SlashCompletionItem>,
    word: &str,
    detail: &str,
    kind: SlashCompletionKind,
    prefix: &str,
    append_space: bool,
) {
    if !word.starts_with(prefix) {
        return;
    }
    let insert = if append_space {
        format!("{word} ")
    } else {
        word.to_string()
    };
    items.push(SlashCompletionItem {
        label: word.to_string(),
        detail: detail.to_string(),
        insert: Some(insert),
        kind,
    });
}

#[derive(Default)]
struct TuiModelCompletionContext {
    provider: Option<String>,
    model: Option<String>,
    account: Option<String>,
}

fn tui_model_quick_completions(
    tokens: &[CompletionToken],
    replace_start: usize,
    replace_end: usize,
    prefix: &str,
) -> Option<SlashCompletionSet> {
    let first = tokens.first()?;
    if first.text != "model" {
        return None;
    }
    if tokens.len() == 1 && replace_start <= first.end {
        return None;
    }

    let before = tokens_before_current(tokens, replace_start);
    let mut args = before.into_iter().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "use") {
        args.remove(0);
    }

    let catalogs = tui_model_catalogs();
    let context = tui_model_completion_context(&args);
    let mut items = Vec::new();

    if let Some(option) = pending_tui_model_option(&args) {
        if !prefix.starts_with("--") {
            match option {
                "--provider" => push_tui_model_provider_items(&mut items, &catalogs, prefix),
                "--model" => push_tui_model_model_items(
                    &mut items,
                    &catalogs,
                    context.provider.as_deref(),
                    prefix,
                ),
                "--account" if prefix.is_empty() => {
                    push_hint_item(&mut items, "account", "provider account")
                }
                _ => {}
            }
        }
    } else if prefix.starts_with("--") {
        push_tui_model_option_items(&mut items, &context, prefix);
    } else if context.provider.is_none() {
        push_tui_model_provider_items(&mut items, &catalogs, prefix);
    } else if context.model.is_none() {
        push_tui_model_model_items(&mut items, &catalogs, context.provider.as_deref(), prefix);
    } else if prefix.is_empty() {
        push_tui_model_option_items(&mut items, &context, prefix);
    }

    if items.is_empty() {
        return None;
    }
    Some(SlashCompletionSet {
        replace_start,
        replace_end,
        items,
    })
}

fn tui_model_completion_context(args: &[String]) -> TuiModelCompletionContext {
    let mut context = TuiModelCompletionContext::default();
    let mut index = 0;
    while let Some(token) = args.get(index) {
        match token.as_str() {
            "--provider" => {
                if let Some(value) = args.get(index + 1).filter(|value| !value.starts_with("--")) {
                    context.provider = Some(value.clone());
                    index += 2;
                } else {
                    index += 1;
                }
            }
            "--model" => {
                if let Some(value) = args.get(index + 1).filter(|value| !value.starts_with("--")) {
                    context.model = Some(value.clone());
                    index += 2;
                } else {
                    index += 1;
                }
            }
            "--account" => {
                if let Some(value) = args.get(index + 1).filter(|value| !value.starts_with("--")) {
                    context.account = Some(value.clone());
                    index += 2;
                } else {
                    index += 1;
                }
            }
            value if value.starts_with("--") => {
                index += 1;
            }
            value if context.provider.is_none() => {
                context.provider = Some(value.to_string());
                index += 1;
            }
            value if context.model.is_none() => {
                context.model = Some(value.to_string());
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    context
}

fn pending_tui_model_option(args: &[String]) -> Option<&'static str> {
    match args.last().map(String::as_str) {
        Some("--provider") => Some("--provider"),
        Some("--model") => Some("--model"),
        Some("--account") => Some("--account"),
        _ => None,
    }
}

fn push_tui_model_provider_items(
    items: &mut Vec<SlashCompletionItem>,
    catalogs: &[TuiModelCatalog],
    prefix: &str,
) {
    for catalog in catalogs {
        push_word_item(
            items,
            &catalog.provider,
            &catalog.detail,
            SlashCompletionKind::Value,
            prefix,
            true,
        );
    }
}

fn push_tui_model_model_items(
    items: &mut Vec<SlashCompletionItem>,
    catalogs: &[TuiModelCatalog],
    provider: Option<&str>,
    prefix: &str,
) {
    let Some(provider) = provider else {
        if prefix.is_empty() {
            push_hint_item(items, "model", "model id");
        }
        return;
    };
    let Some(catalog) = find_tui_model_catalog(catalogs, provider) else {
        if prefix.is_empty() {
            push_hint_item(items, "model", "model id");
        }
        return;
    };
    if catalog.models.is_empty() {
        if prefix.is_empty() {
            push_hint_item(items, "model", "model id");
        }
        return;
    }
    for model in &catalog.models {
        push_word_item(
            items,
            model,
            "available model",
            SlashCompletionKind::Value,
            prefix,
            true,
        );
    }
}

fn push_tui_model_option_items(
    items: &mut Vec<SlashCompletionItem>,
    context: &TuiModelCompletionContext,
    prefix: &str,
) {
    if context.provider.is_none() {
        push_word_item(
            items,
            "--provider",
            "provider id",
            SlashCompletionKind::Option,
            prefix,
            true,
        );
    }
    if context.model.is_none() {
        push_word_item(
            items,
            "--model",
            "model id",
            SlashCompletionKind::Option,
            prefix,
            true,
        );
    }
    if context.account.is_none() {
        push_word_item(
            items,
            "--account",
            "provider account",
            SlashCompletionKind::Option,
            prefix,
            true,
        );
    }
}

fn push_hint_item(items: &mut Vec<SlashCompletionItem>, label: &str, detail: &str) {
    items.push(SlashCompletionItem {
        label: label.to_string(),
        detail: detail.to_string(),
        insert: None,
        kind: SlashCompletionKind::Hint,
    });
}

fn find_tui_model_catalog<'a>(
    catalogs: &'a [TuiModelCatalog],
    provider: &str,
) -> Option<&'a TuiModelCatalog> {
    let provider = provider.trim();
    if provider.eq_ignore_ascii_case("minicpm")
        || provider.eq_ignore_ascii_case(sinomo_pytorch::MINICPM_PROVIDER_ID)
    {
        return catalogs
            .iter()
            .find(|catalog| catalog.provider == "minicpm");
    }
    if let Some(provider) = sinomo_lm::normalize_model_provider(provider) {
        return catalogs.iter().find(|catalog| catalog.provider == provider);
    }
    catalogs
        .iter()
        .find(|catalog| catalog.provider.eq_ignore_ascii_case(provider))
}

fn tui_model_catalogs() -> Vec<TuiModelCatalog> {
    let cache = TUI_MODEL_CATALOG_CACHE.get_or_init(|| std::sync::Mutex::new(None));
    if let Ok(mut guard) = cache.lock() {
        if let Some(cached) = guard
            .as_ref()
            .filter(|cached| cached.loaded_at.elapsed() < TUI_MODEL_CATALOG_TTL)
        {
            return cached.catalogs.clone();
        }
        let catalogs = build_tui_model_catalogs();
        *guard = Some(CachedTuiModelCatalogs {
            loaded_at: std::time::Instant::now(),
            catalogs: catalogs.clone(),
        });
        return catalogs;
    }
    build_tui_model_catalogs()
}

fn build_tui_model_catalogs() -> Vec<TuiModelCatalog> {
    let mut catalogs = Vec::new();
    push_tui_model_catalog(
        &mut catalogs,
        TuiModelCatalog {
            provider: "minicpm".to_string(),
            detail: "local MiniCPM runtime".to_string(),
            models: vec![sinomo_pytorch::MINICPM_DEFAULT_MODEL.to_string()],
        },
    );

    #[cfg(not(test))]
    if let Ok(remote_catalogs) = sinomo_lm::collect_provider_model_catalogs(
        sinomo_lm::default_model_provider_ids().iter().copied(),
        false,
    ) {
        for catalog in remote_catalogs {
            push_tui_model_catalog(
                &mut catalogs,
                TuiModelCatalog {
                    detail: tui_model_provider_detail(&catalog.provider).to_string(),
                    provider: catalog.provider,
                    models: catalog.models,
                },
            );
        }
    }

    catalogs.sort_by(|left, right| left.provider.cmp(&right.provider));
    catalogs
}

fn push_tui_model_catalog(catalogs: &mut Vec<TuiModelCatalog>, mut catalog: TuiModelCatalog) {
    catalog.models.sort();
    catalog.models.dedup();
    if let Some(existing) = catalogs
        .iter_mut()
        .find(|existing| existing.provider == catalog.provider)
    {
        existing.models.extend(catalog.models);
        existing.models.sort();
        existing.models.dedup();
        return;
    }
    catalogs.push(catalog);
}

#[cfg(not(test))]
fn tui_model_provider_detail(provider: &str) -> &'static str {
    PROVIDER_CHOICES
        .iter()
        .find(|choice| choice.value.as_ref() == provider)
        .map(|choice| choice.detail.as_ref())
        .unwrap_or("available model provider")
}

fn slash_command_registry() -> CommandSpec {
    let mut registry = cmd(
        "",
        "",
        vec![
            cmd(
                "quit",
                "end the interactive session",
                vec![],
                vec![],
                vec![],
            ),
            cmd(
                "exit",
                "end the interactive session",
                vec![],
                vec![],
                vec![],
            ),
            cmd(
                "thinking",
                "control visible model thinking stream",
                vec![],
                vec![],
                vec![value("mode", "thinking display mode", THINKING_CHOICES)],
            ),
        ],
        vec![],
        vec![],
    );
    append_registered_slash_commands(&mut registry);
    registry
}
fn append_registered_slash_commands(registry: &mut CommandSpec) {
    for spec in super::cli_command_registry().autocomplete_specs() {
        registry
            .children
            .extend(command_specs_from_autocomplete(spec));
    }
}

fn command_specs_from_autocomplete(spec: CliAutocompleteSpec) -> Vec<CommandSpec> {
    let mut specs = Vec::with_capacity(1 + spec.aliases.len());
    specs.push(command_spec_from_autocomplete(&spec, spec.word.clone()));
    for alias in &spec.aliases {
        specs.push(command_spec_from_autocomplete(&spec, alias.clone()));
    }
    specs
}

fn command_spec_from_autocomplete(spec: &CliAutocompleteSpec, word: String) -> CommandSpec {
    CommandSpec {
        word: Cow::Owned(word),
        detail: Cow::Owned(spec.detail.clone()),
        children: spec
            .children
            .iter()
            .cloned()
            .flat_map(command_specs_from_autocomplete)
            .collect(),
        options: spec
            .options
            .iter()
            .cloned()
            .map(option_spec_from_autocomplete)
            .collect(),
        values: spec
            .values
            .iter()
            .cloned()
            .map(value_spec_from_autocomplete)
            .collect(),
    }
}

fn option_spec_from_autocomplete(option: CliAutocompleteOption) -> OptionSpec {
    OptionSpec {
        name: Cow::Owned(option.name),
        detail: Cow::Owned(option.detail),
        value: option.value.map(value_spec_from_autocomplete),
    }
}

fn value_spec_from_autocomplete(value: CliAutocompleteValue) -> ValueSpec {
    ValueSpec {
        placeholder: Cow::Owned(value.placeholder),
        detail: Cow::Owned(value.detail),
        choices: value
            .choices
            .into_iter()
            .map(choice_from_autocomplete)
            .collect(),
    }
}

fn choice_from_autocomplete(choice: CliAutocompleteChoice) -> ValueChoice {
    ValueChoice {
        value: Cow::Owned(choice.value),
        detail: Cow::Owned(choice.detail),
    }
}

fn apply_tui_slash_command_layer(registry: &mut CommandSpec) {
    registry.children.extend(tui_slash_command_layer());
    if let Some(model) = registry
        .children
        .iter_mut()
        .find(|command| command.word.as_ref() == "model")
    {
        model.detail = Cow::Borrowed("switch the active TUI chat model");
        model.children.clear();
        model.options.clear();
        model.values = vec![value("provider", "available model provider", &[])];
    }
    if let Some(agent) = registry
        .children
        .iter_mut()
        .find(|command| command.word.as_ref() == "agent")
    {
        agent.detail = Cow::Borrowed("switch the active TUI chat agent");
        agent.children = vec![
            cmd(
                "status",
                "show the active TUI agent",
                vec![],
                vec![],
                vec![],
            ),
            cmd("list", "list available TUI agents", vec![], vec![], vec![]),
            cmd(
                "use",
                "switch the active TUI agent",
                vec![],
                vec![],
                vec![value("agent-id", "available agent", TUI_AGENT_CHOICES)],
            ),
        ];
        agent.options.clear();
        agent.values = vec![value("agent-id", "available agent", TUI_AGENT_CHOICES)];
    }
}

fn tui_slash_command_layer() -> Vec<CommandSpec> {
    vec![
        cmd(
            "files",
            "open the TUI file manager for SIFS by default",
            vec![],
            vec![option(
                "--sifs",
                "browse SIFS explicitly",
                Some(value("path", "SIFS path", TUI_FILE_TARGET_CHOICES)),
            )],
            vec![value(
                "path",
                "SIFS path or local:<path> to open",
                TUI_FILE_TARGET_CHOICES,
            )],
        ),
        cmd(
            "sessions",
            "open the local TUI session list",
            vec![],
            vec![],
            vec![],
        ),
        cmd(
            "settings",
            "open the TUI settings page",
            vec![],
            vec![],
            vec![],
        ),
        cmd(
            "resume",
            "resume a local TUI session",
            vec![],
            vec![],
            vec![value("session-id", "session id from /sessions", &[])],
        ),
    ]
}

fn cmd(
    word: &'static str,
    detail: &'static str,
    children: Vec<CommandSpec>,
    options: Vec<OptionSpec>,
    values: Vec<ValueSpec>,
) -> CommandSpec {
    CommandSpec {
        word: Cow::Borrowed(word),
        detail: Cow::Borrowed(detail),
        children,
        options,
        values,
    }
}

fn option(name: &'static str, detail: &'static str, value: Option<ValueSpec>) -> OptionSpec {
    OptionSpec {
        name: Cow::Borrowed(name),
        detail: Cow::Borrowed(detail),
        value,
    }
}

fn value(
    placeholder: &'static str,
    detail: &'static str,
    choices: &'static [ValueChoice],
) -> ValueSpec {
    ValueSpec {
        placeholder: Cow::Borrowed(placeholder),
        detail: Cow::Borrowed(detail),
        choices: choices.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slash_completion_lists_top_level_commands() {
        let completions = slash_completions("/", 1).expect("completions");

        assert_eq!(completions.replace_start, 1);
        assert_eq!(completions.replace_end, 1);
        assert!(completions.items.iter().any(|item| item.label == "cluster"));
        assert!(!completions.items.iter().any(|item| item.label == "files"));
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "thinking")
        );
    }

    #[test]
    fn tui_slash_completion_adds_tui_only_commands() {
        let completions = slash_completions_for_layer("/", 1, SlashCommandCompletionLayer::Tui)
            .expect("completions");

        assert!(completions.items.iter().any(|item| item.label == "files"));
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "sessions")
        );
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "settings")
        );
        assert!(completions.items.iter().any(|item| item.label == "resume"));
        assert!(completions.items.iter().any(|item| item.label == "cluster"));

        let file_targets = slash_completions_for_layer(
            "/files ",
            "/files ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("file target completions");
        assert!(file_targets.items.iter().any(|item| item.label == "sifs:/"));
        assert!(
            file_targets
                .items
                .iter()
                .any(|item| item.label == "local:.")
        );

        let agent_choices = slash_completions_for_layer(
            "/agent use ",
            "/agent use ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("agent choices");
        assert!(
            agent_choices
                .items
                .iter()
                .any(|item| item.label == "coding-agent")
        );
    }

    #[test]
    fn slash_completion_blocks_removed_local_process_option() {
        let standard = slash_completions("/--", "/--".len());
        assert!(
            standard
                .as_ref()
                .is_none_or(|set| !set.items.iter().any(|item| item.label == "--local"))
        );

        let tui = slash_completions_for_layer("/--", "/--".len(), SlashCommandCompletionLayer::Tui);
        assert!(
            tui.as_ref()
                .is_none_or(|set| !set.items.iter().any(|item| item.label == "--local"))
        );
        assert!(
            slash_completions_for_layer(
                "/--local ",
                "/--local ".len(),
                SlashCommandCompletionLayer::Tui
            )
            .is_none()
        );
    }

    #[test]
    fn tui_slash_completion_adds_session_resume_hint() {
        let completions = slash_completions_for_layer(
            "/resume ",
            "/resume ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("resume hint");

        assert!(
            completions.items.iter().any(|item| {
                item.kind == SlashCompletionKind::Hint && item.label == "session-id"
            })
        );
    }

    #[test]
    fn tui_slash_completion_adds_model_switch_commands() {
        let completions = slash_completions_for_layer(
            "/model ",
            "/model ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("model completions");

        assert!(completions.items.iter().any(|item| item.label == "minicpm"));
        assert!(!completions.items.iter().any(|item| item.label == "use"));
        assert!(!completions.items.iter().any(|item| item.label == "list"));

        let completions = slash_completions_for_layer(
            "/model minicpm ",
            "/model minicpm ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("model choices");
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == sinomo_pytorch::MINICPM_DEFAULT_MODEL)
        );

        let model_prefix = &sinomo_pytorch::MINICPM_DEFAULT_MODEL[..8];
        let input = format!("/model minicpm {model_prefix}");
        let completions =
            slash_completions_for_layer(&input, input.len(), SlashCommandCompletionLayer::Tui)
                .expect("filtered model choices");
        assert_eq!(completions.items.len(), 1);
        assert_eq!(
            completions.items[0].label,
            sinomo_pytorch::MINICPM_DEFAULT_MODEL
        );

        let input = format!(
            "/model minicpm {} --a",
            sinomo_pytorch::MINICPM_DEFAULT_MODEL
        );
        let completions =
            slash_completions_for_layer(&input, input.len(), SlashCommandCompletionLayer::Tui)
                .expect("account option");
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "--account")
        );

        let completions = slash_completions_for_layer(
            "/model --provider ",
            "/model --provider ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("provider option values");
        assert!(completions.items.iter().any(|item| item.label == "minicpm"));

        let completions = slash_completions_for_layer(
            "/model --provider minicpm --model ",
            "/model --provider minicpm --model ".len(),
            SlashCommandCompletionLayer::Tui,
        )
        .expect("model option values");
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == sinomo_pytorch::MINICPM_DEFAULT_MODEL)
        );
    }

    #[test]
    fn slash_completion_filters_subcommands() {
        let completions = slash_completions("/cluster r", "/cluster r".len()).expect("completions");

        assert!(completions.items.iter().any(|item| item.label == "role"));
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "repair-members")
        );
        assert!(!completions.items.iter().any(|item| item.label == "health"));

        let completions =
            slash_completions("/cluster s", "/cluster s".len()).expect("state completions");
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "state-authority")
        );
        assert!(
            !completions
                .items
                .iter()
                .any(|item| item.label == "raft-role")
        );
    }

    #[test]
    fn slash_completion_uses_registered_command_autocomplete() {
        let top = slash_completions("/ob", "/ob".len()).expect("registered alias");
        assert!(top.items.iter().any(|item| item.label == "obs"));

        let commands = slash_completions("/observe ", "/observe ".len()).expect("observe commands");
        assert!(commands.items.iter().any(|item| item.label == "events"));
        assert!(commands.items.iter().any(|item| item.label == "logs"));

        let options = slash_completions("/observe events --", "/observe events --".len())
            .expect("observe event options");
        assert!(options.items.iter().any(|item| item.label == "--kind"));
        assert!(options.items.iter().any(|item| item.label == "--json"));

        let value = slash_completions("/observe events --kind ", "/observe events --kind ".len())
            .expect("observe kind hint");
        assert!(
            value
                .items
                .iter()
                .any(|item| { item.kind == SlashCompletionKind::Hint && item.label == "prefix" })
        );

        let invite_options = slash_completions("/cluster invite --", "/cluster invite --".len())
            .expect("cluster invite options");
        assert!(
            invite_options
                .items
                .iter()
                .any(|item| item.label == "--channel-output")
        );
        let channel_output = slash_completions(
            "/cluster invite --channel-output ",
            "/cluster invite --channel-output ".len(),
        )
        .expect("cluster channel output hint");
        assert!(channel_output.items.iter().any(|item| {
            item.kind == SlashCompletionKind::Hint && item.label == "introducer=path"
        }));
    }

    #[test]
    fn slash_completion_uses_declarative_models_schema() {
        let runtime = slash_completions("/models runtime set --p", "/models runtime set --p".len())
            .expect("runtime options");
        for expected in [
            "--pytorch-endpoint",
            "--pytorch-python",
            "--pytorch-device",
            "--pytorch-timeout-seconds",
            "--pytorch-mock",
        ] {
            assert!(
                runtime.items.iter().any(|item| item.label == expected),
                "missing completion {expected}"
            );
        }
        assert!(!runtime.items.iter().any(|item| item.label == "--python"));

        let effort = slash_completions(
            "/model options set --reasoning-effort ",
            "/model options set --reasoning-effort ".len(),
        )
        .expect("reasoning values");
        assert!(effort.items.iter().any(|item| item.label == "high"));
        assert!(effort.items.iter().any(|item| item.label == "clear"));
    }

    #[test]
    fn slash_completion_lists_module_activation_commands() {
        let completions = slash_completions("/module ", "/module ".len()).expect("module");

        assert!(completions.items.iter().any(|item| item.label == "list"));
        assert!(completions.items.iter().any(|item| item.label == "install"));
        assert!(completions.items.iter().any(|item| item.label == "inspect"));
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "native-dylib")
        );
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "native-rust")
        );
        assert!(completions.items.iter().any(|item| item.label == "wasix"));

        let options = slash_completions(
            "/module wasix activate --",
            "/module wasix activate --".len(),
        )
        .expect("wasix activate options");

        assert!(options.items.iter().any(|item| item.label == "--artifact"));
        assert!(
            options
                .items
                .iter()
                .any(|item| item.label == "--library-name")
        );

        let wasix_commands =
            slash_completions("/module wasix ", "/module wasix ".len()).expect("wasix commands");
        assert!(
            wasix_commands
                .items
                .iter()
                .any(|item| item.label == "unload")
        );

        let unload_options = slash_completions(
            "/module native-dylib unload --",
            "/module native-dylib unload --".len(),
        )
        .expect("native unload options");
        assert!(
            unload_options
                .items
                .iter()
                .any(|item| item.label == "--module")
        );

        let native_rust_commands =
            slash_completions("/module native-rust ", "/module native-rust ".len())
                .expect("native-rust commands");
        assert!(
            native_rust_commands
                .items
                .iter()
                .any(|item| item.label == "enable")
        );

        let native_rust_modules = slash_completions(
            "/module native-rust disable ",
            "/module native-rust disable ".len(),
        )
        .expect("native-rust module choices");
        assert!(
            native_rust_modules
                .items
                .iter()
                .any(|item| item.label == "module.observe")
        );
    }

    #[test]
    fn slash_completion_suggests_options_and_values() {
        let completions =
            slash_completions("/dfs pin add --m", "/dfs pin add --m".len()).expect("option");
        assert_eq!(completions.items[0].label, "--mode");

        let completions =
            slash_completions("/dfs pin add --mode ", "/dfs pin add --mode ".len()).expect("value");
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "metadata")
        );
        assert!(completions.items.iter().any(|item| item.label == "content"));
    }

    #[test]
    fn slash_completion_shows_free_form_value_hint() {
        let completions =
            slash_completions("/rollout apply --repo ", "/rollout apply --repo ".len())
                .expect("hint");

        assert_eq!(completions.items[0].kind, SlashCompletionKind::Hint);
        assert_eq!(completions.items[0].insert, None);
    }

    #[test]
    fn slash_completion_does_not_count_option_values_as_positionals() {
        let completions = slash_completions(
            "/providers auth --account default ",
            "/providers auth --account default ".len(),
        )
        .expect("provider choices");

        assert!(completions.items.iter().any(|item| item.label == "openai"));
        assert!(completions.items.iter().any(|item| item.label == "codex"));
    }
}
