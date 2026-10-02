//! Agent and skill command grammar, metadata, and registration.
use crate::{AgentSessionControlEventCursor, SkillActivationScope, SkillCatalogSourceKind, TaskId};
use sinomo_lm::{CODEX_DEFAULT_MODEL, CODEX_PROVIDER_ID};
use sinomo_runtime::{cli_registry::*, events::SessionId};
use std::{path::PathBuf, str::FromStr};
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentCliCommand {
    Export(AgentExportOptions),
    Import(AgentImportOptions),
    Inspect(AgentInspectOptions),
    Readiness(AgentReadinessOptions),
    Run(AgentRunOptions),
    Session(AgentSessionCliCommand),
    Validate(AgentValidateOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentSessionCliCommand {
    Inspect(AgentSessionInspectOptions),
    Progress(AgentSessionProgressOptions),
    TerminalResults(AgentSessionTerminalResultsOptions),
    Steer(AgentSessionMessageOptions),
    FollowUp(AgentSessionMessageOptions),
    Interrupt(AgentSessionMessageOptions),
    Cancel(AgentSessionMessageOptions),
    Unpause(AgentSessionResumeOptions),
    Resume(AgentSessionResumeOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SkillCliCommand {
    List(SkillListOptions),
    Show(SkillShowOptions),
    Import(SkillImportOptions),
    Export(SkillExportOptions),
    Activate(SkillActivateOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillListOptions {
    pub all: bool,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillShowOptions {
    pub skill_id: String,
    pub version: Option<String>,
    pub text: bool,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillImportOptions {
    pub path: PathBuf,
    pub skill_id: Option<String>,
    pub version: String,
    pub source: SkillCatalogSourceKind,
    pub source_id: String,
    pub activate: bool,
    pub scope: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillExportOptions {
    pub skill_id: String,
    pub version: Option<String>,
    pub output: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillActivateOptions {
    pub skill_id: String,
    pub version: Option<String>,
    pub scope: String,
    pub reason: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentInspectOptions {
    pub agent: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentExportOptions {
    pub agent: String,
    pub output: Option<PathBuf>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentImportOptions {
    pub path: PathBuf,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentValidateOptions {
    pub agent: String,
    pub file: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentReadinessOptions {
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentRunOptions {
    pub prompt: String,
    pub agent: String,
    pub agent_explicit: bool,
    pub provider: String,
    pub provider_explicit: bool,
    pub model: String,
    pub model_explicit: bool,
    pub account: Option<String>,
    pub session_id: Option<SessionId>,
    pub compact_after: bool,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentSessionInspectOptions {
    pub session_id: SessionId,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentSessionProgressOptions {
    pub session_id: SessionId,
    pub cursor: Option<AgentSessionControlEventCursor>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentSessionTerminalResultsOptions {
    pub session_id: SessionId,
    pub task_id: Option<TaskId>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentSessionMessageOptions {
    pub session_id: SessionId,
    pub message: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentSessionResumeOptions {
    pub session_id: SessionId,
    pub json: bool,
}

pub fn agent_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(AgentCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<AgentCliCommand, E> {
    DeclarativeCommandHandler::new(
        "agent",
        CliCommandPolicy::local_capable(),
        agent_cli_spec,
        parse_agent_matches,
        run,
    )
    .with_parsed_policy(agent_cli_policy)
}

pub fn skill_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(SkillCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<SkillCliCommand, E> {
    DeclarativeCommandHandler::new(
        "skill",
        CliCommandPolicy::local_capable(),
        skill_cli_spec,
        parse_skill_matches,
        run,
    )
    .with_parsed_policy(skill_cli_policy)
}

fn agent_cli_policy(command: &AgentCliCommand) -> CliCommandPolicy {
    match command {
        AgentCliCommand::Inspect(_)
        | AgentCliCommand::Export(_)
        | AgentCliCommand::Run(_)
        | AgentCliCommand::Validate(AgentValidateOptions { file: None, .. }) => {
            CliCommandPolicy::default()
        }
        AgentCliCommand::Import(_)
        | AgentCliCommand::Readiness(_)
        | AgentCliCommand::Session(_)
        | AgentCliCommand::Validate(AgentValidateOptions { file: Some(_), .. }) => {
            CliCommandPolicy::local_capable()
        }
    }
}

fn skill_cli_policy(command: &SkillCliCommand) -> CliCommandPolicy {
    match command {
        SkillCliCommand::List(_) | SkillCliCommand::Show(_) | SkillCliCommand::Export(_) => {
            CliCommandPolicy::default()
        }
        SkillCliCommand::Import(_) | SkillCliCommand::Activate(_) => {
            CliCommandPolicy::local_capable()
        }
    }
}

fn agent_cli_spec() -> CliCommandSpec {
    let json = || CliOptionSpec::flag("--json", "print JSON output");
    let session = || {
        CliOptionSpec::value(
            "--session",
            "agent session id",
            CliValueSpec::new("session-id", "agent session id"),
        )
        .with_aliases(["--session-id"])
    };
    let optional_agent =
        || CliPositionalSpec::optional(CliValueSpec::new("agent-id", "agent package id"));
    let session_positional =
        || CliPositionalSpec::optional(CliValueSpec::new("session-id", "agent session id"));
    let message_command = |name: &'static str, about: &'static str| {
        CliCommandSpec::new(name, about)
            .with_options([
                session(),
                CliOptionSpec::value(
                    "--message",
                    "message or reason text",
                    CliValueSpec::new("text", "message text"),
                )
                .with_aliases(["--reason"])
                .repeatable(),
                json(),
            ])
            .with_positionals([
                session_positional(),
                CliPositionalSpec::optional(CliValueSpec::new("message", "message text"))
                    .repeatable(),
            ])
    };
    let sessions = CliCommandSpec::new("session", "inspect and control agent sessions")
        .with_aliases(["sessions"])
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("inspect", "inspect persisted agent session state")
                .with_options([session(), json()])
                .with_positionals([session_positional()]),
            CliCommandSpec::new("progress", "read agent session control-plane events")
                .with_options([
                    session(),
                    CliOptionSpec::value(
                        "--cursor",
                        "event cursor offset",
                        CliValueSpec::new("offset", "event cursor offset"),
                    )
                    .with_aliases(["--offset"]),
                    CliOptionSpec::value(
                        "--last-event",
                        "last observed event id",
                        CliValueSpec::new("event-id", "event id"),
                    )
                    .with_aliases(["--last-event-id"]),
                    json(),
                ])
                .with_positionals([session_positional()]),
            CliCommandSpec::new("terminal-results", "read terminal agent task results")
                .with_aliases(["results"])
                .with_options([
                    session(),
                    CliOptionSpec::value(
                        "--task",
                        "filter by task id",
                        CliValueSpec::new("task-id", "agent task id"),
                    )
                    .with_aliases(["--task-id"]),
                    json(),
                ])
                .with_positionals([session_positional()]),
            message_command("steer", "steer a live agent session"),
            message_command("follow-up", "queue a follow-up for a live agent session")
                .with_aliases(["followup"]),
            message_command(
                "interrupt",
                "pause a live agent session: no new model requests, running tools finish",
            )
            .with_aliases(["pause"]),
            message_command(
                "cancel",
                "cancel a live agent session's turn now, stopping running tools and processes",
            ),
            CliCommandSpec::new("unpause", "let a paused agent session run turns again")
                .with_options([session(), json()])
                .with_positionals([session_positional()]),
            CliCommandSpec::new("resume", "reattach to a live agent session")
                .with_options([session(), json()])
                .with_positionals([session_positional()]),
        ]);
    CliCommandSpec::new("agent", "run and inspect Sinomo agents")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("export", "export an agent package")
                .with_options([CliOptionSpec::value(
                    "--output",
                    "output JSON path",
                    CliValueSpec::new("path", "output path"),
                )
                .with_aliases(["-o"])])
                .with_positionals([optional_agent()]),
            CliCommandSpec::new("import", "import an agent package")
                .with_options([
                    CliOptionSpec::value(
                        "--file",
                        "agent package JSON path",
                        CliValueSpec::new("path", "package path"),
                    ),
                    json(),
                ])
                .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                    "path",
                    "package path",
                ))]),
            CliCommandSpec::new("inspect", "inspect an agent package")
                .with_options([
                    CliOptionSpec::value(
                        "--agent",
                        "agent package id",
                        CliValueSpec::new("agent-id", "agent package id"),
                    ),
                    json(),
                ])
                .with_positionals([optional_agent()]),
            CliCommandSpec::new("readiness", "run deterministic local agent readiness gates")
                .with_aliases(["ready"])
                .with_options([json()]),
            CliCommandSpec::new(
                "run",
                "run one agent turn with durable session resume and persisted context projection",
            )
            .with_options([
                CliOptionSpec::value(
                    "--prompt",
                    "agent prompt",
                    CliValueSpec::new("text", "prompt text"),
                ),
                CliOptionSpec::value(
                    "--agent",
                    "agent package id",
                    CliValueSpec::new("agent-id", "agent package id"),
                ),
                CliOptionSpec::value(
                    "--provider",
                    "model provider",
                    CliValueSpec::new("provider", "provider id"),
                ),
                CliOptionSpec::value(
                    "--model",
                    "model id",
                    CliValueSpec::new("model", "model id"),
                ),
                CliOptionSpec::value(
                    "--account",
                    "provider account",
                    CliValueSpec::new("account", "provider account"),
                ),
                session(),
                CliOptionSpec::flag("--compact-after", "compact the session after the turn"),
                json(),
            ])
            .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                "prompt",
                "agent prompt",
            ))]),
            sessions,
            CliCommandSpec::new("validate", "validate an agent package")
                .with_options([
                    CliOptionSpec::value(
                        "--file",
                        "agent package JSON path",
                        CliValueSpec::new("path", "package path"),
                    ),
                    json(),
                ])
                .with_positionals([optional_agent()]),
        ])
}

fn skill_cli_spec() -> CliCommandSpec {
    let json = || CliOptionSpec::flag("--json", "print JSON output");
    let version = || {
        CliOptionSpec::value(
            "--version",
            "skill version",
            CliValueSpec::new("version", "skill version"),
        )
    };
    let skill = || {
        CliOptionSpec::value(
            "--skill",
            "skill id",
            CliValueSpec::new("skill-id", "skill id"),
        )
        .with_aliases(["--id"])
    };
    let optional_skill = || CliPositionalSpec::optional(CliValueSpec::new("skill-id", "skill id"));
    CliCommandSpec::new("skill", "manage standard Agent Skills packages in SIFS")
        .with_aliases(["skills"])
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("list", "list catalog skills")
                .with_aliases(["ls"])
                .with_options([
                    CliOptionSpec::flag("--all", "include inactive versions").with_aliases(["-a"]),
                    json(),
                ]),
            CliCommandSpec::new("show", "show a skill descriptor")
                .with_aliases(["inspect", "describe"])
                .with_options([
                    skill(),
                    version(),
                    CliOptionSpec::flag("--text", "include stored SKILL.md text")
                        .with_aliases(["--markdown"]),
                    json(),
                ])
                .with_positionals([optional_skill()]),
            CliCommandSpec::new(
                "import",
                "validate and import an Agent Skills package directory",
            )
            .with_options([
                CliOptionSpec::value(
                    "--file",
                    "skill directory or its SKILL.md file",
                    CliValueSpec::new("path", "skill path"),
                )
                .with_aliases(["--path"]),
                skill(),
                version(),
                CliOptionSpec::value(
                    "--source",
                    "catalog source kind",
                    CliValueSpec::new("kind", "source kind")
                        .with_choices([
                            CliAutocompleteChoice::new("sinomo", "built-in Sinomo skill"),
                            CliAutocompleteChoice::new("module", "module-provided skill"),
                            CliAutocompleteChoice::new("user", "user-provided skill"),
                            CliAutocompleteChoice::new("agent", "agent-provided skill"),
                            CliAutocompleteChoice::new("evolution", "evolution-provided skill"),
                        ])
                        .restricted(),
                ),
                CliOptionSpec::value(
                    "--source-id",
                    "source identity",
                    CliValueSpec::new("id", "source identity"),
                ),
                CliOptionSpec::flag("--activate", "activate the imported skill"),
                CliOptionSpec::flag("--no-activate", "leave the imported skill inactive"),
                CliOptionSpec::value(
                    "--scope",
                    "activation scope",
                    CliValueSpec::new("scope", "activation scope"),
                ),
                json(),
            ])
            .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                "path",
                "skill path",
            ))]),
            CliCommandSpec::new(
                "export",
                "export a catalog skill as an Agent Skills directory",
            )
            .with_options([
                skill(),
                version(),
                CliOptionSpec::value(
                    "--output",
                    "output directory",
                    CliValueSpec::new("path", "output directory"),
                )
                .with_aliases(["-o"]),
                json(),
            ])
            .with_positionals([optional_skill()]),
            CliCommandSpec::new("activate", "activate a catalog skill")
                .with_aliases(["enable"])
                .with_options([
                    skill(),
                    version(),
                    CliOptionSpec::value(
                        "--scope",
                        "activation scope",
                        CliValueSpec::new("scope", "activation scope"),
                    ),
                    CliOptionSpec::value(
                        "--reason",
                        "activation reason",
                        CliValueSpec::new("text", "activation reason"),
                    ),
                    json(),
                ])
                .with_positionals([optional_skill()]),
        ])
}

fn parse_agent_matches(matches: &CliCommandMatches) -> Result<AgentCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["agent", "export"] => Ok(AgentCliCommand::Export(AgentExportOptions {
            agent: matches
                .positionals()
                .first()
                .cloned()
                .unwrap_or_else(|| "main-agent".to_string()),
            output: matches.value("--output").map(PathBuf::from),
        })),
        ["agent", "import"] => {
            let path = matches
                .value("--file")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .ok_or_else(|| CliParseError::Parse {
                    detail: "agent import requires a package JSON path".to_string(),
                })?;
            Ok(AgentCliCommand::Import(AgentImportOptions {
                path: PathBuf::from(path),
                json: matches.contains("--json"),
            }))
        }
        ["agent", "inspect"] => Ok(AgentCliCommand::Inspect(AgentInspectOptions {
            agent: matches
                .value("--agent")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .unwrap_or("main-agent")
                .to_string(),
            json: matches.contains("--json"),
        })),
        ["agent", "readiness"] => Ok(AgentCliCommand::Readiness(AgentReadinessOptions {
            json: matches.contains("--json"),
        })),
        ["agent", "run"] => parse_agent_run_matches(matches).map(AgentCliCommand::Run),
        ["agent", "session", command] => {
            parse_agent_session_matches(command, matches).map(AgentCliCommand::Session)
        }
        ["agent", "validate"] => Ok(AgentCliCommand::Validate(AgentValidateOptions {
            agent: matches
                .positionals()
                .first()
                .cloned()
                .unwrap_or_else(|| "main-agent".to_string()),
            file: matches.value("--file").map(PathBuf::from),
            json: matches.contains("--json"),
        })),
        _ => Err(CliParseError::Parse {
            detail: "agent requires a subcommand".to_string(),
        }),
    }
}

fn parse_agent_run_matches(matches: &CliCommandMatches) -> Result<AgentRunOptions, CliParseError> {
    let prompt = matches
        .value("--prompt")
        .or_else(|| matches.positionals().first().map(String::as_str))
        .ok_or_else(|| CliParseError::Parse {
            detail: "agent run requires --prompt <text>".to_string(),
        })?;
    let agent_explicit = matches.contains("--agent");
    let provider_explicit = matches.contains("--provider");
    let model_explicit = matches.contains("--model");
    let session_id = matches
        .value("--session")
        .map(parse_agent_session_id)
        .transpose()
        .map_err(semantic_parse_error)?;
    Ok(AgentRunOptions {
        prompt: prompt.to_string(),
        agent: matches.value("--agent").unwrap_or("main-agent").to_string(),
        agent_explicit,
        provider: matches
            .value("--provider")
            .unwrap_or(CODEX_PROVIDER_ID)
            .to_string(),
        provider_explicit,
        model: matches
            .value("--model")
            .unwrap_or(CODEX_DEFAULT_MODEL)
            .to_string(),
        model_explicit,
        account: matches.value("--account").map(str::to_string),
        session_id,
        compact_after: matches.contains("--compact-after"),
        json: matches.contains("--json"),
    })
}

fn agent_session_id_from_matches(matches: &CliCommandMatches) -> Result<SessionId, CliParseError> {
    let value = matches
        .value("--session")
        .or_else(|| matches.positionals().first().map(String::as_str))
        .ok_or_else(|| CliParseError::Parse {
            detail: format!(
                "agent session {} requires a session id",
                matches.command_name()
            ),
        })?;
    parse_agent_session_id(value).map_err(semantic_parse_error)
}

fn parse_agent_session_matches(
    command: &str,
    matches: &CliCommandMatches,
) -> Result<AgentSessionCliCommand, CliParseError> {
    let session_id = agent_session_id_from_matches(matches)?;
    let json = matches.contains("--json");
    match command {
        "inspect" => Ok(AgentSessionCliCommand::Inspect(
            AgentSessionInspectOptions { session_id, json },
        )),
        "progress" => {
            let offset = matches
                .value("--cursor")
                .map(|value| {
                    value
                        .parse::<usize>()
                        .map_err(|source| CliParseError::Parse {
                            detail: format!("invalid --cursor value `{value}`: {source}"),
                        })
                })
                .transpose()?;
            let last_event_id = matches
                .value("--last-event")
                .map(|value| {
                    TaskId::from_str(value).map_err(|source| CliParseError::Parse {
                        detail: format!("invalid agent task id `{value}`: {source}"),
                    })
                })
                .transpose()?;
            let cursor = (offset.is_some() || last_event_id.is_some()).then(|| {
                AgentSessionControlEventCursor {
                    offset: offset.unwrap_or_default(),
                    last_event_id,
                }
            });
            Ok(AgentSessionCliCommand::Progress(
                AgentSessionProgressOptions {
                    session_id,
                    cursor,
                    json,
                },
            ))
        }
        "terminal-results" => {
            let task_id = matches
                .value("--task")
                .map(|value| {
                    TaskId::from_str(value).map_err(|source| CliParseError::Parse {
                        detail: format!("invalid agent task id `{value}`: {source}"),
                    })
                })
                .transpose()?;
            Ok(AgentSessionCliCommand::TerminalResults(
                AgentSessionTerminalResultsOptions {
                    session_id,
                    task_id,
                    json,
                },
            ))
        }
        "steer" | "follow-up" | "interrupt" | "cancel" => {
            let positional_start = usize::from(!matches.contains("--session"));
            let message = matches
                .values("--message")
                .chain(
                    matches
                        .positionals()
                        .iter()
                        .skip(positional_start)
                        .map(String::as_str),
                )
                .collect::<Vec<_>>()
                .join(" ");
            let message = match command {
                "interrupt" if message.trim().is_empty() => "operator interrupted".to_string(),
                "cancel" if message.trim().is_empty() => "operator cancelled".to_string(),
                _ => message,
            };
            if message.trim().is_empty() {
                return Err(CliParseError::Parse {
                    detail: format!("agent session {command} requires a message"),
                });
            }
            let options = AgentSessionMessageOptions {
                session_id,
                message,
                json,
            };
            match command {
                "steer" => Ok(AgentSessionCliCommand::Steer(options)),
                "follow-up" => Ok(AgentSessionCliCommand::FollowUp(options)),
                "interrupt" => Ok(AgentSessionCliCommand::Interrupt(options)),
                "cancel" => Ok(AgentSessionCliCommand::Cancel(options)),
                _ => unreachable!(),
            }
        }
        "unpause" => Ok(AgentSessionCliCommand::Unpause(AgentSessionResumeOptions {
            session_id,
            json,
        })),
        "resume" => Ok(AgentSessionCliCommand::Resume(AgentSessionResumeOptions {
            session_id,
            json,
        })),
        _ => Err(CliParseError::Parse {
            detail: format!("unknown agent session command `{command}`"),
        }),
    }
}

fn parse_skill_matches(matches: &CliCommandMatches) -> Result<SkillCliCommand, CliParseError> {
    let skill_id = || {
        matches
            .value("--skill")
            .or_else(|| matches.positionals().first().map(String::as_str))
            .map(str::to_string)
    };
    match matches.command_name() {
        "list" => Ok(SkillCliCommand::List(SkillListOptions {
            all: matches.contains("--all"),
            json: matches.contains("--json"),
        })),
        "show" => Ok(SkillCliCommand::Show(SkillShowOptions {
            skill_id: skill_id().ok_or_else(|| CliParseError::Parse {
                detail: "skill show requires a skill id".to_string(),
            })?,
            version: matches.value("--version").map(str::to_string),
            text: matches.contains("--text"),
            json: matches.contains("--json"),
        })),
        "import" => {
            if matches.contains("--activate") && matches.contains("--no-activate") {
                return Err(CliParseError::Parse {
                    detail: "skill import cannot combine --activate and --no-activate".to_string(),
                });
            }
            let path = matches
                .value("--file")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .ok_or_else(|| CliParseError::Parse {
                    detail: "skill import requires an Agent Skills package directory".to_string(),
                })?;
            let source = matches
                .value("--source")
                .map(parse_skill_source_kind)
                .transpose()
                .map_err(semantic_parse_error)?
                .unwrap_or(SkillCatalogSourceKind::User);
            Ok(SkillCliCommand::Import(SkillImportOptions {
                path: PathBuf::from(path),
                skill_id: matches.value("--skill").map(str::to_string),
                version: matches.value("--version").unwrap_or("1.0.0").to_string(),
                source,
                source_id: matches
                    .value("--source-id")
                    .unwrap_or("local-user")
                    .to_string(),
                activate: !matches.contains("--no-activate"),
                scope: matches
                    .value("--scope")
                    .unwrap_or(SkillActivationScope::Cluster.as_str())
                    .to_string(),
                json: matches.contains("--json"),
            }))
        }
        "export" => Ok(SkillCliCommand::Export(SkillExportOptions {
            skill_id: skill_id().ok_or_else(|| CliParseError::Parse {
                detail: "skill export requires a skill id".to_string(),
            })?,
            version: matches.value("--version").map(str::to_string),
            output: matches.value("--output").map(PathBuf::from),
            json: matches.contains("--json"),
        })),
        "activate" => Ok(SkillCliCommand::Activate(SkillActivateOptions {
            skill_id: skill_id().ok_or_else(|| CliParseError::Parse {
                detail: "skill activate requires a skill id".to_string(),
            })?,
            version: matches.value("--version").map(str::to_string),
            scope: matches
                .value("--scope")
                .unwrap_or(SkillActivationScope::Cluster.as_str())
                .to_string(),
            reason: matches
                .value("--reason")
                .unwrap_or("manual skill activation")
                .to_string(),
            json: matches.contains("--json"),
        })),
        _ => Err(CliParseError::Parse {
            detail: "skill requires a subcommand".to_string(),
        }),
    }
}

fn parse_skill_source_kind(value: &str) -> Result<SkillCatalogSourceKind, CliParseError> {
    match value {
        "sinomo" => Ok(SkillCatalogSourceKind::Sinomo),
        "module" => Ok(SkillCatalogSourceKind::Module),
        "user" => Ok(SkillCatalogSourceKind::User),
        "agent" => Ok(SkillCatalogSourceKind::Agent),
        "evolution" => Ok(SkillCatalogSourceKind::Evolution),
        _ => Err(CliParseError::Parse {
            detail: format!(
                "invalid skill source `{value}`; expected sinomo, module, user, agent, or evolution"
            ),
        }),
    }
}

pub fn parse_agent_session_id(value: &str) -> Result<SessionId, CliParseError> {
    SessionId::from_str(value)
        .or_else(|_| SessionId::from_legacy_named(value))
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid agent session id `{value}`: {source}"),
        })
}
/// Parse this domain's arguments without executing a command.
pub fn parse_agent_command(args: &[String]) -> Result<AgentCliCommand, CliParseError> {
    parse_command_args(agent_cli_spec(), args, parse_agent_matches)
}

/// Parse this domain's arguments without executing a command.
pub fn parse_skill_command(args: &[String]) -> Result<SkillCliCommand, CliParseError> {
    parse_command_args(skill_cli_spec(), args, parse_skill_matches)
}

pub fn register_cli_commands<E: Into<CliCommandError> + 'static>(
    registry: CliCommandRegistry,
    run_agent: fn(AgentCliCommand) -> Result<(), E>,
    run_skill: fn(SkillCliCommand) -> Result<(), E>,
) -> Result<CliCommandRegistry, String> {
    registry
        .try_register(agent_cli_command_handler(run_agent))?
        .try_register(skill_cli_command_handler(run_skill))
}
