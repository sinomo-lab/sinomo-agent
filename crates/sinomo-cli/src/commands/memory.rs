use super::*;
use serde::Serialize;
use sinomo_memory::{
    IndexRootId, IndexRootPolicy, IndexedFileMemoryStore, LocalIndexRootConfig,
    MemoryAccessContext, MemoryQuery, MemorySourceCatalog,
};
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec, CliParseError,
    CliPositionalSpec, CliValueSpec,
};
use sinomo_runtime::core::Id;

const DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE: usize = 128;
const MAX_MEMORY_ROOT_LIST_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum MemoryCliCommand {
    RootAdd { path: PathBuf },
    RootList(MemoryRootListOptions),
    Index { root_id: IndexRootId },
    Query(MemoryQueryOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct MemoryRootListOptions {
    pub(crate) limit: usize,
    pub(crate) after: Option<IndexRootId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct MemoryQueryOptions {
    pub(crate) text: String,
    pub(crate) max_records: usize,
    pub(crate) max_bytes: usize,
}

pub(crate) fn memory_cli_command_handler() -> DeclarativeCommandHandler<MemoryCliCommand> {
    DeclarativeCommandHandler::new(
        "memory",
        CliCommandPolicy::local_capable(),
        memory_cli_spec,
        parse_memory_matches,
        run_memory_cli_command,
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

#[cfg(test)]
fn parse_memory_schema_for_test(args: &[String]) -> Result<MemoryCliCommand, CliError> {
    parse_schema_command_for_test(memory_cli_spec(), args, parse_memory_matches)
}

fn parse_memory_root_id(value: &str) -> Result<IndexRootId, CliError> {
    Id::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid memory root id `{value}`: {source}"),
    })
}

fn parse_positive_memory_limit(option: &str, value: &str) -> Result<usize, CliError> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| CliError::Parse {
            detail: format!("{option} must be a positive integer, got `{value}`"),
        })
}

pub(crate) fn run_memory_cli_command(command: MemoryCliCommand) -> Result<(), CliError> {
    match command {
        MemoryCliCommand::RootAdd { path } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let store = IndexedFileMemoryStore::from_data_dir(&data_dir);
            let root = store.register_local_root(LocalIndexRootConfig::new(
                path,
                IndexRootPolicy::cluster_local(),
            ))?;
            print_memory_json("memory root", &root)
        }
        MemoryCliCommand::RootList(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let store = IndexedFileMemoryStore::from_data_dir(&data_dir);
            let page = store.root_page(options.after.as_ref(), options.limit)?;
            print_memory_json("memory roots", &page)
        }
        MemoryCliCommand::Index { root_id } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let store = IndexedFileMemoryStore::from_data_dir(&data_dir);
            let report = store.index_root(&root_id)?;
            print_memory_json("memory index report", &report)
        }
        MemoryCliCommand::Query(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let store = IndexedFileMemoryStore::from_data_dir(&data_dir);
            let query = MemoryQuery {
                task_hint: Some(options.text),
                max_records: options.max_records,
                max_bytes: options.max_bytes,
                ..MemoryQuery::default()
            };
            let bundle = store.retrieve(&MemoryAccessContext::cluster_local(), &query)?;
            print_memory_json("memory evidence", &bundle)
        }
    }
}

fn print_memory_json<T: Serialize>(context: &str, value: &T) -> Result<(), CliError> {
    let json = serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
        path: context.to_string(),
        detail: source.to_string(),
    })?;
    println!("{json}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_parser_covers_first_slice_commands() {
        assert_eq!(
            parse_memory_schema_for_test(&["root".into(), "add".into(), "/tmp/notes".into()])
                .expect("root add"),
            MemoryCliCommand::RootAdd {
                path: PathBuf::from("/tmp/notes")
            }
        );
        assert_eq!(
            parse_memory_schema_for_test(&["root".into(), "list".into()]).expect("root list"),
            MemoryCliCommand::RootList(MemoryRootListOptions {
                limit: DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE,
                after: None,
            })
        );
        let after = Id::derived("memory-cli-test", b"after");
        assert_eq!(
            parse_memory_schema_for_test(&[
                "root".into(),
                "list".into(),
                "--limit".into(),
                "7".into(),
                "--after".into(),
                after.to_string(),
            ])
            .expect("bounded root list"),
            MemoryCliCommand::RootList(MemoryRootListOptions {
                limit: 7,
                after: Some(after),
            })
        );
        let root_id = Id::derived("memory-cli-test", b"root");
        assert_eq!(
            parse_memory_schema_for_test(&["index".into(), root_id.to_string()]).expect("index"),
            MemoryCliCommand::Index { root_id }
        );

        let defaults = MemoryQuery::default();
        assert_eq!(
            parse_memory_schema_for_test(&["query".into(), "deployment".into(), "notes".into()])
                .expect("query"),
            MemoryCliCommand::Query(MemoryQueryOptions {
                text: "deployment notes".to_string(),
                max_records: defaults.max_records,
                max_bytes: defaults.max_bytes,
            })
        );
        assert_eq!(
            parse_memory_schema_for_test(&[
                "query".into(),
                "deployment".into(),
                "--max-records".into(),
                "3".into(),
                "--max-bytes".into(),
                "4096".into(),
            ])
            .expect("bounded query"),
            MemoryCliCommand::Query(MemoryQueryOptions {
                text: "deployment".to_string(),
                max_records: 3,
                max_bytes: 4096,
            })
        );
    }

    #[test]
    fn memory_parser_rejects_invalid_limits_and_missing_arguments() {
        assert!(matches!(
            parse_memory_schema_for_test(&["query".into()]),
            Err(CliError::Parse { .. })
        ));
        assert!(matches!(
            parse_memory_schema_for_test(&[
                "query".into(),
                "notes".into(),
                "--max-records".into(),
                "0".into(),
            ]),
            Err(CliError::Parse { .. })
        ));
        assert!(matches!(
            parse_memory_schema_for_test(&["index".into(), "not-an-id".into()]),
            Err(CliError::Parse { .. })
        ));
        assert!(matches!(
            parse_memory_schema_for_test(&[
                "root".into(),
                "list".into(),
                "--limit".into(),
                "257".into(),
            ]),
            Err(CliError::Parse { .. })
        ));
    }
}
