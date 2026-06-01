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
    Standard,
    Tui,
}

#[derive(Clone, Debug)]
struct CommandSpec {
    word: &'static str,
    detail: &'static str,
    children: Vec<CommandSpec>,
    options: Vec<OptionSpec>,
    values: Vec<ValueSpec>,
}

#[derive(Clone, Debug)]
struct OptionSpec {
    name: &'static str,
    detail: &'static str,
    value: Option<ValueSpec>,
}

#[derive(Clone, Debug)]
struct ValueSpec {
    placeholder: &'static str,
    detail: &'static str,
    choices: &'static [ValueChoice],
}

#[derive(Clone, Debug)]
struct ValueChoice {
    value: &'static str,
    detail: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompletionToken {
    text: String,
    start: usize,
    end: usize,
}

const BOOLEAN_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "true",
        detail: "enable the setting",
    },
    ValueChoice {
        value: "false",
        detail: "disable the setting",
    },
];

const NODE_ROLE_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "workstation",
        detail: "desktop or laptop node",
    },
    ValueChoice {
        value: "mobile",
        detail: "mobile node",
    },
    ValueChoice {
        value: "server",
        detail: "server node",
    },
    ValueChoice {
        value: "sensor",
        detail: "sensor node",
    },
    ValueChoice {
        value: "test",
        detail: "test node",
    },
];

const RAFT_ROLE_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "voter",
        detail: "participates in quorum",
    },
    ValueChoice {
        value: "learner",
        detail: "replicates without voting",
    },
    ValueChoice {
        value: "non-voting",
        detail: "non-voting cluster member",
    },
];

const DFS_PIN_MODE_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "metadata",
        detail: "pin metadata only",
    },
    ValueChoice {
        value: "content",
        detail: "pin content data",
    },
];

const SANDBOX_TMP_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "clear",
        detail: "clear tmp storage",
    },
    ValueChoice {
        value: "retain",
        detail: "retain tmp storage",
    },
];

const SANDBOX_TRANSPORT_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "direct",
        detail: "use direct host networking",
    },
    ValueChoice {
        value: "daemon",
        detail: "route through daemon sandbox service",
    },
];

const PROVIDER_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "codex",
        detail: "Codex CLI provider",
    },
    ValueChoice {
        value: "github-copilot",
        detail: "GitHub Copilot provider",
    },
    ValueChoice {
        value: "openai",
        detail: "OpenAI API provider",
    },
    ValueChoice {
        value: "anthropic",
        detail: "Anthropic API provider",
    },
    ValueChoice {
        value: "deepseek",
        detail: "DeepSeek API provider",
    },
    ValueChoice {
        value: "gemini",
        detail: "Gemini API provider",
    },
    ValueChoice {
        value: "nvidia",
        detail: "NVIDIA API provider",
    },
    ValueChoice {
        value: "minicpm",
        detail: "local MiniCPM runtime",
    },
];

const TELEMETRY_WEIGHT_CHOICES: &[ValueChoice] = &[ValueChoice {
    value: "unlimited",
    detail: "remove the numeric cap",
}];

const THINKING_CHOICES: &[ValueChoice] = &[
    ValueChoice {
        value: "on",
        detail: "show model thinking stream when available",
    },
    ValueChoice {
        value: "off",
        detail: "hide model thinking stream",
    },
    ValueChoice {
        value: "status",
        detail: "show current thinking-stream mode",
    },
];

pub(crate) fn slash_completions(input: &str, cursor: usize) -> Option<SlashCompletionSet> {
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
                    child.word,
                    child.detail,
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
                    option.name,
                    option.detail,
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
        let Some(child) = node.children.iter().find(|child| child.word == token.text) else {
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
        .find(|option| option.name == previous)
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
        .any(|option| option.name == name && option.value.is_some())
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

    for choice in value.choices {
        push_word_item(
            items,
            choice.value,
            choice.detail,
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

fn slash_command_registry() -> CommandSpec {
    cmd(
        "",
        "",
        vec![
            cmd("help", "show top-level CLI help", vec![], vec![], vec![]),
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
            cmd(
                "cluster",
                "inspect or modify cluster membership",
                vec![
                    cmd(
                        "init",
                        "bootstrap a new local cluster",
                        vec![],
                        opts(&["--name", "--cluster-id", "--node-id"]),
                        vec![],
                    ),
                    cmd(
                        "invite",
                        "create a portable join invite",
                        vec![],
                        opts(&["--introducer", "--ttl-seconds", "--output"]),
                        vec![],
                    ),
                    cmd(
                        "join",
                        "record intent to join an existing cluster",
                        vec![],
                        opts(&["--name", "--cluster-id", "--node-id"]),
                        vec![value("invite", "join URI or invite file", &[])],
                    ),
                    cmd(
                        "role",
                        "set documented node role",
                        vec![],
                        vec![],
                        vec![
                            value("node-id", "cluster node id", &[]),
                            value("role", "node role", NODE_ROLE_CHOICES),
                        ],
                    ),
                    cmd(
                        "raft-role",
                        "set Raft membership role",
                        vec![],
                        vec![],
                        vec![
                            value("node-id", "cluster node id", &[]),
                            value("role", "Raft role", RAFT_ROLE_CHOICES),
                        ],
                    ),
                    cmd(
                        "repair-members",
                        "import member records from a reachable peer",
                        vec![],
                        opts(&["--from", "--promote"]),
                        vec![],
                    ),
                    cmd(
                        "remove",
                        "remove non-voter members",
                        vec![],
                        vec![],
                        vec![value("node-id", "cluster node id", &[])],
                    ),
                    cmd("status", "show cluster status", vec![], vec![], vec![]),
                    cmd("health", "show node health", vec![], vec![], vec![]),
                    cmd(
                        "doctor",
                        "diagnose cluster reachability",
                        vec![],
                        vec![],
                        vec![],
                    ),
                    cmd("peers", "list known peers", vec![], vec![], vec![]),
                    cmd(
                        "leave",
                        "remove local cluster state",
                        vec![],
                        vec![],
                        vec![],
                    ),
                ],
                vec![],
                vec![],
            ),
            cmd(
                "install",
                "install or update local daemon boot entries",
                vec![],
                opts(&["--host", "--port", "--shadow-port", "--no-start"]),
                vec![],
            ),
            cmd(
                "uninstall",
                "remove boot entries and optionally stop daemons",
                vec![],
                opts(&["--no-stop"]),
                vec![],
            ),
            cmd(
                "daemon",
                "control local daemons",
                vec![
                    cmd(
                        "run",
                        "run main daemon in foreground",
                        vec![],
                        opts(&["--host", "--port"]),
                        vec![],
                    ),
                    cmd(
                        "start",
                        "start main daemon detached",
                        vec![],
                        opts(&["--host", "--port"]),
                        vec![],
                    ),
                    cmd("status", "probe main daemon", vec![], vec![], vec![]),
                    cmd("stop", "stop main daemon", vec![], vec![], vec![]),
                    cmd(
                        "shadow",
                        "control shadow daemon",
                        vec![
                            cmd(
                                "run",
                                "run shadow daemon in foreground",
                                vec![],
                                opts(&["--host", "--port"]),
                                vec![],
                            ),
                            cmd(
                                "start",
                                "start shadow daemon detached",
                                vec![],
                                opts(&["--host", "--port"]),
                                vec![],
                            ),
                            cmd("status", "probe shadow daemon", vec![], vec![], vec![]),
                            cmd("stop", "stop shadow daemon", vec![], vec![], vec![]),
                        ],
                        vec![],
                        vec![],
                    ),
                ],
                vec![],
                vec![],
            ),
            cmd(
                "rollout",
                "manage source and binary rollout",
                vec![
                    cmd(
                        "init-source",
                        "initialize managed source repo",
                        vec![],
                        opts(&["--repo"]),
                        vec![],
                    ),
                    cmd(
                        "import-source",
                        "merge external repo into managed source",
                        vec![],
                        opts(&["--repo"]),
                        vec![],
                    ),
                    cmd(
                        "export-source",
                        "merge managed source into external repo",
                        vec![],
                        opts(&["--repo"]),
                        vec![],
                    ),
                    cmd(
                        "apply-external",
                        "import then apply managed source",
                        vec![],
                        opts(&["--repo", "--host", "--port"]),
                        vec![],
                    ),
                    cmd(
                        "apply",
                        "build and stage managed source",
                        vec![],
                        opts(&["--repo", "--host", "--port"]),
                        vec![],
                    ),
                    cmd(
                        "cluster",
                        "roll out to cluster targets",
                        vec![],
                        opts(&[
                            "--repo",
                            "--binary",
                            "--target",
                            "--remote-build",
                            "--remote-repo",
                            "--skip-local",
                        ]),
                        vec![],
                    ),
                    cmd(
                        "advance",
                        "advance candidate rollout state",
                        vec![],
                        opts(&[
                            "--node",
                            "--scorecard",
                            "--module-replay-report",
                            "--shadow-telemetry",
                            "--shadow-telemetry-limit",
                            "--incident",
                            "--user-corrections",
                            "--rollback-trigger",
                            "--evidence",
                        ]),
                        vec![],
                    ),
                    cmd(
                        "telemetry-gates",
                        "inspect or update auto-promotion gates",
                        vec![
                            cmd("show", "show telemetry gates", vec![], vec![], vec![]),
                            cmd(
                                "set",
                                "set telemetry gates",
                                vec![],
                                opts(&[
                                    "--enable-auto-promotion",
                                    "--disable-auto-promotion",
                                    "--event-limit",
                                    "--min-events",
                                    "--max-auto-weight-bps",
                                    "--max-candidate-latency-p95-ms",
                                ]),
                                vec![],
                            ),
                        ],
                        vec![],
                        vec![],
                    ),
                    cmd(
                        "diagnostics",
                        "fetch rollout diagnostics",
                        vec![],
                        opts(&["--target"]),
                        vec![],
                    ),
                    cmd(
                        "status",
                        "show rollout paths and state",
                        vec![],
                        vec![],
                        vec![],
                    ),
                ],
                vec![],
                vec![],
            ),
            cmd(
                "dfs",
                "inspect and manage SIFS/DFS state",
                dfs_children(),
                vec![],
                vec![],
            ),
            cmd(
                "sandbox",
                "manage local sandbox instances and toolchains",
                sandbox_children(),
                vec![],
                vec![],
            ),
            cmd(
                "models",
                "configure language model defaults and runtime",
                models_children(),
                vec![],
                vec![],
            ),
            cmd(
                "model",
                "alias for models",
                models_children(),
                vec![],
                vec![],
            ),
            cmd(
                "providers",
                "manage provider credentials",
                providers_children(),
                vec![],
                vec![],
            ),
            cmd(
                "usage",
                "show provider usage",
                vec![],
                opts(&["--provider", "--json"]),
                vec![],
            ),
            cmd(
                "evolve",
                "run self-evolution workflows",
                evolve_children(),
                vec![],
                vec![],
            ),
            cmd(
                "agent",
                "run direct agent utilities",
                vec![cmd(
                    "run",
                    "run a one-shot agent prompt",
                    vec![],
                    opts(&["--prompt", "--provider", "--model", "--account"]),
                    vec![],
                )],
                vec![],
                vec![],
            ),
        ],
        vec![option("--local", "force local file-backed mode", None)],
        vec![],
    )
}

fn apply_tui_slash_command_layer(registry: &mut CommandSpec) {
    registry.children.extend(tui_slash_command_layer());
}

fn tui_slash_command_layer() -> Vec<CommandSpec> {
    vec![cmd(
        "files",
        "open the local TUI file browser",
        vec![],
        vec![],
        vec![value("path", "directory or file to open", &[])],
    )]
}

fn dfs_children() -> Vec<CommandSpec> {
    vec![
        cmd(
            "status",
            "summarize DFS metadata and repairs",
            vec![],
            opts(&["--cluster", "--metadata-root", "--json"]),
            vec![],
        ),
        cmd(
            "filesystem",
            "inspect one filesystem",
            vec![],
            opts(&["--metadata-root", "--json"]),
            vec![value("file-system-id", "DFS filesystem id", &[])],
        ),
        cmd(
            "path",
            "inspect one object by filesystem path",
            vec![],
            opts(&["--filesystem", "--metadata-root", "--json"]),
            vec![value("path", "filesystem path", &[])],
        ),
        cmd(
            "object",
            "inspect one object",
            vec![],
            opts(&["--metadata-root", "--json"]),
            vec![value("object-id", "DFS object id", &[])],
        ),
        cmd(
            "conflicts",
            "list conflict records",
            vec![],
            opts(&["--filesystem", "--object", "--metadata-root", "--json"]),
            vec![],
        ),
        cmd(
            "repair",
            "list inferred missing replica repairs",
            vec![],
            opts(&["--filesystem", "--object", "--metadata-root", "--json"]),
            vec![],
        ),
        cmd(
            "services",
            "list DFS service advertisements",
            vec![],
            opts(&["--json"]),
            vec![],
        ),
        cmd(
            "smoke",
            "write a small SIFS smoke file",
            vec![],
            opts(&["--json"]),
            vec![],
        ),
        cmd(
            "metadata",
            "metadata maintenance",
            vec![cmd(
                "publish",
                "publish primary metadata shard journal",
                vec![],
                opts(&["--json"]),
                vec![],
            )],
            vec![],
            vec![],
        ),
        cmd(
            "pin",
            "manage local DFS pins",
            vec![
                cmd(
                    "list",
                    "list pins",
                    vec![],
                    opts(&["--filesystem", "--pin-root", "--json"]),
                    vec![],
                ),
                cmd(
                    "add",
                    "add a DFS pin",
                    vec![],
                    opts(&[
                        "--path",
                        "--filesystem",
                        "--object",
                        "--node",
                        "--mode",
                        "--pin-root",
                        "--json",
                    ]),
                    vec![],
                ),
                cmd(
                    "remove",
                    "remove a DFS pin",
                    vec![],
                    opts(&["--path", "--filesystem", "--node", "--pin-root", "--json"]),
                    vec![],
                ),
            ],
            vec![],
            vec![],
        ),
        cmd(
            "expose",
            "manage local SIFS exposure",
            vec![
                cmd(
                    "init",
                    "ensure default exposure config",
                    vec![],
                    opts(&["--config", "--json"]),
                    vec![],
                ),
                cmd(
                    "list",
                    "list exposure config",
                    vec![],
                    opts(&["--config", "--json"]),
                    vec![],
                ),
                cmd(
                    "add",
                    "add exposure entry",
                    vec![],
                    opts(&[
                        "--source",
                        "--dfs-path",
                        "--id",
                        "--filesystem",
                        "--node",
                        "--read-only",
                        "--follow-symlinks",
                        "--config",
                        "--json",
                    ]),
                    vec![],
                ),
                cmd(
                    "remove",
                    "remove exposure entry",
                    vec![],
                    opts(&["--id", "--config", "--json"]),
                    vec![],
                ),
            ],
            vec![],
            vec![],
        ),
    ]
}

fn sandbox_children() -> Vec<CommandSpec> {
    vec![
        cmd("help", "show sandbox help", vec![], vec![], vec![]),
        cmd(
            "create",
            "create a sandbox instance",
            vec![],
            opts(&[
                "--daemon",
                "--id",
                "--toolchain",
                "--memory-mb",
                "--wall-time-ms",
                "--max-input-bytes",
                "--max-output-bytes",
                "--max-workspace-bytes",
                "--max-written-bytes-per-command",
                "--max-file-count",
                "--tmp",
                "--keep-tmp",
                "--mount",
            ]),
            vec![],
        ),
        cmd(
            "status",
            "show sandbox status",
            vec![],
            opts(&["--daemon", "--id", "--json"]),
            vec![],
        ),
        cmd(
            "doctor",
            "diagnose sandbox state",
            vec![],
            opts(&["--daemon", "--id", "--json"]),
            vec![],
        ),
        cmd(
            "config",
            "show sandbox config",
            vec![],
            opts(&["--daemon", "--id", "--json"]),
            vec![],
        ),
        cmd(
            "ls",
            "list sandbox files",
            vec![],
            opts(&["--daemon", "--id", "--path"]),
            vec![],
        ),
        cmd(
            "cat",
            "read sandbox file",
            vec![],
            opts(&["--daemon", "--id", "--path", "--max-bytes"]),
            vec![],
        ),
        cmd(
            "write",
            "write sandbox file",
            vec![],
            opts(&["--daemon", "--id", "--path", "--text", "--stdin"]),
            vec![],
        ),
        cmd(
            "fetch",
            "fetch URL into sandbox",
            vec![],
            opts(&["--daemon", "--id", "--url", "--output", "--transport"]),
            vec![],
        ),
        cmd(
            "checkpoint",
            "checkpoint sandbox state",
            vec![],
            opts(&["--daemon", "--id", "--label"]),
            vec![],
        ),
        cmd(
            "resume",
            "resume sandbox from checkpoint",
            vec![],
            opts(&["--daemon", "--id", "--checkpoint"]),
            vec![],
        ),
        cmd(
            "reboot",
            "reboot sandbox",
            vec![],
            opts(&["--daemon", "--id"]),
            vec![],
        ),
        cmd(
            "destroy",
            "destroy sandbox",
            vec![],
            opts(&["--daemon", "--id"]),
            vec![],
        ),
        cmd(
            "run",
            "run sandbox command",
            vec![],
            opts(&["--daemon", "--id", "--", "--cwd", "--env", "--timeout-ms"]),
            vec![value("command", "command and arguments", &[])],
        ),
        cmd(
            "shell",
            "run interactive shell command",
            vec![],
            opts(&["--daemon", "--id", "--cwd"]),
            vec![],
        ),
        cmd(
            "logs",
            "show sandbox logs",
            vec![],
            opts(&["--daemon", "--id", "--tail"]),
            vec![],
        ),
        cmd(
            "policy",
            "manage sandbox policy",
            vec![cmd(
                "set",
                "set sandbox policy",
                vec![],
                opts(&["--allow-network", "--allow-host-fs", "--json"]),
                vec![],
            )],
            vec![],
            vec![],
        ),
        cmd(
            "module-artifact",
            "manage module artifacts",
            vec![
                cmd(
                    "register",
                    "register a module artifact",
                    vec![],
                    opts(&["--path", "--module", "--version", "--capability", "--json"]),
                    vec![],
                ),
                cmd(
                    "rollout-material",
                    "write rollout material",
                    vec![],
                    opts(&["--artifact", "--output", "--json"]),
                    vec![],
                ),
            ],
            vec![],
            vec![],
        ),
        cmd(
            "module-replay",
            "run module replay fixtures",
            vec![cmd(
                "run",
                "run replay fixture",
                vec![],
                opts(&["--fixture", "--artifact", "--json"]),
                vec![],
            )],
            vec![],
            vec![],
        ),
        cmd(
            "toolchain",
            "manage sandbox toolchains",
            vec![
                cmd(
                    "add-command",
                    "add a command to a toolchain",
                    vec![],
                    opts(&[
                        "--toolchain",
                        "--name",
                        "--command",
                        "--memory-mb",
                        "--wall-time-ms",
                        "--max-output-bytes",
                    ]),
                    vec![],
                ),
                cmd(
                    "add-artifact",
                    "add artifact to toolchain",
                    vec![],
                    opts(&["--toolchain", "--path", "--name"]),
                    vec![],
                ),
                cmd(
                    "hash",
                    "hash toolchain material",
                    vec![],
                    opts(&["--path", "--json"]),
                    vec![],
                ),
                cmd(
                    "import-busybox",
                    "import BusyBox package",
                    vec![],
                    opts(&["--toolchain", "--path"]),
                    vec![],
                ),
                cmd(
                    "import-package",
                    "import package into toolchain",
                    vec![],
                    opts(&["--toolchain", "--path", "--name"]),
                    vec![],
                ),
                cmd(
                    "verify-busybox",
                    "verify BusyBox toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-bash",
                    "verify bash toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-coding",
                    "verify coding toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-make",
                    "verify make toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-makepkg",
                    "verify makepkg toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-tcc",
                    "verify tcc toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-python",
                    "verify Python toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
                cmd(
                    "verify-quickjs",
                    "verify QuickJS toolchain",
                    vec![],
                    opts(&["--toolchain", "--json"]),
                    vec![],
                ),
            ],
            vec![],
            vec![],
        ),
    ]
}

fn models_children() -> Vec<CommandSpec> {
    vec![
        cmd(
            "list",
            "list known models",
            vec![],
            opts(&["--provider"]),
            vec![],
        ),
        cmd(
            "default",
            "set default provider/model",
            vec![],
            opts(&["--provider", "--model", "--account"]),
            vec![],
        ),
        cmd(
            "runtime",
            "inspect or update local model runtime",
            vec![
                cmd("show", "show runtime config", vec![], vec![], vec![]),
                cmd(
                    "set",
                    "set runtime config",
                    vec![],
                    opts(&[
                        "--minicpm-endpoint",
                        "--minicpm-model-dir",
                        "--native-models-cache-dir",
                        "--max-input-tokens",
                        "--max-output-tokens",
                        "--python",
                        "--device",
                        "--timeout-seconds",
                        "--mock",
                    ]),
                    vec![],
                ),
            ],
            vec![],
            vec![],
        ),
    ]
}

fn providers_children() -> Vec<CommandSpec> {
    vec![
        cmd(
            "auth",
            "store provider credentials",
            vec![],
            opts(&["--account"]),
            vec![value("provider", "provider id", PROVIDER_CHOICES)],
        ),
        cmd(
            "test",
            "test provider credentials",
            vec![],
            opts(&["--account", "--model"]),
            vec![value("provider", "provider id", PROVIDER_CHOICES)],
        ),
        cmd("list", "list configured providers", vec![], vec![], vec![]),
    ]
}

fn evolve_children() -> Vec<CommandSpec> {
    vec![
        cmd(
            "propose",
            "create an evolution proposal",
            vec![],
            opts(&[
                "--objective",
                "--repo",
                "--allow",
                "--verify",
                "--provider",
                "--model",
                "--dry-run",
                "--approve",
                "--pause-after-proposal",
                "--max-wall-time",
                "--budget-policy",
                "--approval",
            ]),
            vec![],
        ),
        cmd(
            "approve",
            "approve a proposed job",
            vec![],
            opts(&["--job"]),
            vec![],
        ),
        cmd(
            "reject",
            "reject a proposed job",
            vec![],
            opts(&["--job", "--reason"]),
            vec![],
        ),
        cmd(
            "restore",
            "restore from candidate state",
            vec![],
            opts(&["--job"]),
            vec![],
        ),
        cmd(
            "status",
            "show evolution status",
            vec![],
            opts(&["--job"]),
            vec![],
        ),
        cmd(
            "show",
            "show job details",
            vec![],
            opts(&["--job", "--json"]),
            vec![],
        ),
        cmd(
            "verify",
            "run verification",
            vec![],
            opts(&["--job"]),
            vec![],
        ),
        cmd(
            "packet",
            "show mutation packet",
            vec![],
            opts(&["--job"]),
            vec![],
        ),
        cmd(
            "cleanup",
            "clean old evolution artifacts",
            vec![],
            opts(&["--job"]),
            vec![],
        ),
        cmd("job", "job utilities", vec![], opts(&["--job"]), vec![]),
        cmd(
            "schedule",
            "schedule evolution work",
            vec![],
            opts(&["--campaign", "--resource", "--estimate", "--json"]),
            vec![],
        ),
        cmd(
            "campaign",
            "manage long-horizon campaigns",
            vec![
                cmd(
                    "approve",
                    "approve campaign",
                    vec![],
                    opts(&["--campaign"]),
                    vec![],
                ),
                cmd(
                    "reject",
                    "reject campaign",
                    vec![],
                    opts(&["--campaign", "--reason"]),
                    vec![],
                ),
                cmd(
                    "status",
                    "show campaign status",
                    vec![],
                    opts(&["--campaign"]),
                    vec![],
                ),
            ],
            vec![],
            vec![],
        ),
        cmd(
            "trusted-signers",
            "manage trusted evolution signers",
            vec![cmd(
                "list",
                "list trusted signers",
                vec![],
                opts(&["--json"]),
                vec![],
            )],
            vec![],
            vec![],
        ),
        cmd(
            "models",
            "configure evolution model defaults",
            models_children(),
            vec![],
            vec![],
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
        word,
        detail,
        children,
        options,
        values,
    }
}

fn opts(names: &[&'static str]) -> Vec<OptionSpec> {
    names.iter().map(|name| option_for_name(name)).collect()
}

fn option_for_name(name: &'static str) -> OptionSpec {
    let value = match name {
        "--provider" => Some(value("provider", "provider id", PROVIDER_CHOICES)),
        "--mode" => Some(value("mode", "pin mode", DFS_PIN_MODE_CHOICES)),
        "--tmp" => Some(value("mode", "tmp handling mode", SANDBOX_TMP_CHOICES)),
        "--transport" => Some(value(
            "transport",
            "fetch transport",
            SANDBOX_TRANSPORT_CHOICES,
        )),
        "--mock" | "--allow-network" | "--allow-host-fs" => {
            Some(value("boolean", "true or false", BOOLEAN_CHOICES))
        }
        "--max-auto-weight-bps" | "--max-candidate-latency-p95-ms" => Some(value(
            "n|unlimited",
            "numeric value or unlimited",
            TELEMETRY_WEIGHT_CHOICES,
        )),
        "--json"
        | "--local"
        | "--no-start"
        | "--no-stop"
        | "--remote-build"
        | "--skip-local"
        | "--shadow-telemetry"
        | "--enable-auto-promotion"
        | "--disable-auto-promotion"
        | "--cluster"
        | "--daemon"
        | "--keep-tmp"
        | "--stdin"
        | "--read-only"
        | "--follow-symlinks"
        | "--dry-run"
        | "--approve"
        | "--pause-after-proposal" => None,
        _ => Some(value(option_placeholder(name), option_detail(name), &[])),
    };
    option(name, option_detail(name), value)
}

fn option(name: &'static str, detail: &'static str, value: Option<ValueSpec>) -> OptionSpec {
    OptionSpec {
        name,
        detail,
        value,
    }
}

fn value(
    placeholder: &'static str,
    detail: &'static str,
    choices: &'static [ValueChoice],
) -> ValueSpec {
    ValueSpec {
        placeholder,
        detail,
        choices,
    }
}

fn option_placeholder(name: &str) -> &'static str {
    match name {
        "--host" => "host",
        "--port" | "--shadow-port" => "port",
        "--repo"
        | "--remote-repo"
        | "--binary"
        | "--output"
        | "--path"
        | "--source"
        | "--config"
        | "--metadata-root"
        | "--pin-root"
        | "--scorecard"
        | "--module-replay-report"
        | "--fixture" => "path",
        "--target" | "--introducer" | "--from" | "--minicpm-endpoint" | "--url" => "endpoint",
        "--model" => "model",
        "--account" => "account",
        "--node" | "--node-id" | "--promote" => "node-id",
        "--cluster-id" => "cluster-id",
        "--name" => "name",
        "--ttl-seconds"
        | "--timeout-seconds"
        | "--event-limit"
        | "--min-events"
        | "--tail"
        | "--max-bytes"
        | "--memory-mb"
        | "--wall-time-ms"
        | "--max-input-bytes"
        | "--max-output-bytes"
        | "--max-workspace-bytes"
        | "--max-written-bytes-per-command"
        | "--max-file-count"
        | "--max-input-tokens"
        | "--max-output-tokens"
        | "--shadow-telemetry-limit"
        | "--user-corrections"
        | "--timeout-ms" => "n",
        "--filesystem" => "file-system-id",
        "--object" => "object-id",
        "--dfs-path" => "dfs-path",
        "--id" => "id",
        "--toolchain" => "toolchain-id",
        "--mount" => "sifs-path:mount-path[:ro|rw]",
        "--text" => "text",
        "--label" => "label",
        "--checkpoint" => "checkpoint-id",
        "--cwd" => "path",
        "--env" => "KEY=VALUE",
        "--command" => "command",
        "--capability" => "capability",
        "--artifact" => "artifact-id",
        "--version" => "version",
        "--python" => "path",
        "--device" => "device",
        "--objective" | "--reason" | "--budget-policy" | "--approval" | "--rollback-trigger"
        | "--evidence" | "--resource" | "--estimate" | "--allow" | "--verify" => "value",
        "--job" => "job-id",
        "--campaign" => "campaign-id",
        "--incident" => "event-id",
        _ => "value",
    }
}

fn option_detail(name: &str) -> &'static str {
    match name {
        "--local" => "force local file-backed mode",
        "--json" => "print JSON output",
        "--provider" => "provider id",
        "--model" => "model id",
        "--account" => "provider account",
        "--host" => "bind or target host",
        "--port" => "port number",
        "--repo" => "repository path",
        "--path" => "filesystem path",
        "--target" => "target host:port",
        "--daemon" => "use daemon-backed storage",
        "--mode" => "mode value",
        _ => option_placeholder(name),
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
        assert!(completions.items.iter().any(|item| item.label == "cluster"));
    }

    #[test]
    fn slash_completion_filters_subcommands() {
        let completions = slash_completions("/cluster r", "/cluster r".len()).expect("completions");

        assert!(completions.items.iter().any(|item| item.label == "role"));
        assert!(
            completions
                .items
                .iter()
                .any(|item| item.label == "raft-role")
        );
        assert!(!completions.items.iter().any(|item| item.label == "health"));
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
