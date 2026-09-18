//! Memory command grammar and registration. Execution is supplied by the host.
use crate::{IndexRootId, MemoryQuery};
use sinomo_runtime::{cli_registry::*, core::Id};
use std::path::PathBuf;

const DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE: usize = 128;

const MAX_MEMORY_ROOT_LIST_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemoryCliCommand {
    RootAdd { path: PathBuf },
    RootList(MemoryRootListOptions),
    Index { root_id: IndexRootId },
    Query(MemoryQueryOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryRootListOptions {
    pub limit: usize,
    pub after: Option<IndexRootId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryQueryOptions {
    pub text: String,
    pub max_records: usize,
    pub max_bytes: usize,
}

pub fn memory_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(MemoryCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<MemoryCliCommand, E> {
    DeclarativeCommandHandler::new(
        "memory",
        CliCommandPolicy::local_capable(),
        memory_cli_spec,
        parse_memory_matches,
        run,
    )
}

fn memory_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("memory", "manage indexed-file memory")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("root", "manage indexed memory roots")
                .help_on_empty()
                .with_children([
                    CliCommandSpec::new("add", "register a local memory source root")
                        .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                            "path",
                            "source root path",
                        ))]),
                    CliCommandSpec::new("list", "list registered local memory roots").with_options(
                        [
                            CliOptionSpec::value(
                                "--limit",
                                "maximum roots to return",
                                CliValueSpec::new("count", "positive result limit"),
                            ),
                            CliOptionSpec::value(
                                "--after",
                                "continue after a root id",
                                CliValueSpec::new("root-id", "memory root id"),
                            ),
                        ],
                    ),
                ]),
            CliCommandSpec::new("index", "index a registered memory root").with_positionals([
                CliPositionalSpec::required(CliValueSpec::new("root-id", "memory root id")),
            ]),
            CliCommandSpec::new("query", "query indexed-file memory")
                .with_options([
                    CliOptionSpec::value(
                        "--max-records",
                        "maximum evidence records",
                        CliValueSpec::new("count", "positive record limit"),
                    ),
                    CliOptionSpec::value(
                        "--max-bytes",
                        "maximum evidence bytes",
                        CliValueSpec::new("bytes", "positive byte limit"),
                    ),
                ])
                .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                    "text",
                    "query text",
                ))
                .repeatable()]),
        ])
}

fn parse_memory_matches(matches: &CliCommandMatches) -> Result<MemoryCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["memory", "root", "add"] => Ok(MemoryCliCommand::RootAdd {
            path: PathBuf::from(required_positional(matches, 0)?),
        }),
        ["memory", "root", "list"] => {
            let limit = matches
                .value("--limit")
                .map(|value| parse_positive_memory_limit("--limit", value))
                .transpose()
                .map_err(semantic_parse_error)?
                .unwrap_or(DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE);
            if limit > MAX_MEMORY_ROOT_LIST_PAGE_SIZE {
                return Err(CliParseError::Parse {
                    detail: format!(
                        "--limit must be at most {MAX_MEMORY_ROOT_LIST_PAGE_SIZE}, got `{limit}`"
                    ),
                });
            }
            let after = matches
                .value("--after")
                .map(parse_memory_root_id)
                .transpose()
                .map_err(semantic_parse_error)?;
            Ok(MemoryCliCommand::RootList(MemoryRootListOptions {
                limit,
                after,
            }))
        }
        ["memory", "index"] => Ok(MemoryCliCommand::Index {
            root_id: parse_memory_root_id(required_positional(matches, 0)?)
                .map_err(semantic_parse_error)?,
        }),
        ["memory", "query"] => {
            let defaults = MemoryQuery::default();
            let max_records = matches
                .value("--max-records")
                .map(|value| parse_positive_memory_limit("--max-records", value))
                .transpose()
                .map_err(semantic_parse_error)?
                .unwrap_or(defaults.max_records);
            let max_bytes = matches
                .value("--max-bytes")
                .map(|value| parse_positive_memory_limit("--max-bytes", value))
                .transpose()
                .map_err(semantic_parse_error)?
                .unwrap_or(defaults.max_bytes);
            Ok(MemoryCliCommand::Query(MemoryQueryOptions {
                text: matches.positionals().join(" "),
                max_records,
                max_bytes,
            }))
        }
        _ => Err(CliParseError::Parse {
            detail: "memory requires a subcommand".to_string(),
        }),
    }
}

fn parse_memory_root_id(value: &str) -> Result<IndexRootId, CliParseError> {
    Id::new(value.to_string()).map_err(|source| CliParseError::Parse {
        detail: format!("invalid memory root id `{value}`: {source}"),
    })
}

fn parse_positive_memory_limit(option: &str, value: &str) -> Result<usize, CliParseError> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| CliParseError::Parse {
            detail: format!("{option} must be a positive integer, got `{value}`"),
        })
}
/// Parse this domain's arguments without executing a command.
pub fn parse_memory_command(args: &[String]) -> Result<MemoryCliCommand, CliParseError> {
    parse_command_args(memory_cli_spec(), args, parse_memory_matches)
}

/// Register the owner's grammar with a host-supplied execution adapter.
pub fn register_cli_commands<E: Into<CliCommandError> + 'static>(
    registry: CliCommandRegistry,
    run: fn(MemoryCliCommand) -> Result<(), E>,
) -> Result<CliCommandRegistry, String> {
    registry.try_register(memory_cli_command_handler(run))
}
