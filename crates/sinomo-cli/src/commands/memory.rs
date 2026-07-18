use super::*;
use serde::Serialize;
use sinomo_memory::{
    IndexRootId, IndexRootPolicy, IndexedFileMemoryStore, LocalIndexRootConfig,
    MemoryAccessContext, MemoryQuery, MemorySourceCatalog,
};
use sinomo_runtime::core::Id;

const DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE: usize = 128;
const MAX_MEMORY_ROOT_LIST_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum MemoryCliCommand {
    Help,
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

pub(crate) fn parse_memory_command(args: &[String]) -> Result<MemoryCliCommand, CliError> {
    match args.split_first() {
        None => Ok(MemoryCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(MemoryCliCommand::Help)
        }
        Some((command, rest)) if command == "root" => parse_memory_root_command(rest),
        Some((command, [root_id])) if command == "index" => Ok(MemoryCliCommand::Index {
            root_id: parse_memory_root_id(root_id)?,
        }),
        Some((command, rest)) if command == "query" => {
            Ok(MemoryCliCommand::Query(parse_memory_query_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_memory_root_command(args: &[String]) -> Result<MemoryCliCommand, CliError> {
    match args.split_first() {
        Some((command, [path])) if command == "add" => Ok(MemoryCliCommand::RootAdd {
            path: PathBuf::from(path),
        }),
        Some((command, rest)) if command == "list" => Ok(MemoryCliCommand::RootList(
            parse_memory_root_list_options(rest)?,
        )),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(MemoryCliCommand::Help)
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_memory_root_list_options(args: &[String]) -> Result<MemoryRootListOptions, CliError> {
    let mut limit = DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE;
    let mut after = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--limit" => {
                index += 1;
                limit = parse_positive_memory_limit(
                    "--limit",
                    args.get(index).ok_or(CliError::Usage)?,
                )?;
                if limit > MAX_MEMORY_ROOT_LIST_PAGE_SIZE {
                    return Err(CliError::Parse {
                        detail: format!(
                            "--limit must be at most {MAX_MEMORY_ROOT_LIST_PAGE_SIZE}, got `{limit}`"
                        ),
                    });
                }
            }
            "--after" => {
                index += 1;
                after = Some(parse_memory_root_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(MemoryRootListOptions { limit, after })
}

fn parse_memory_root_id(value: &str) -> Result<IndexRootId, CliError> {
    Id::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid memory root id `{value}`: {source}"),
    })
}

fn parse_memory_query_options(args: &[String]) -> Result<MemoryQueryOptions, CliError> {
    let defaults = MemoryQuery::default();
    let mut max_records = defaults.max_records;
    let mut max_bytes = defaults.max_bytes;
    let mut text = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--max-records" => {
                index += 1;
                max_records = parse_positive_memory_limit(
                    "--max-records",
                    args.get(index).ok_or(CliError::Usage)?,
                )?;
            }
            "--max-bytes" => {
                index += 1;
                max_bytes = parse_positive_memory_limit(
                    "--max-bytes",
                    args.get(index).ok_or(CliError::Usage)?,
                )?;
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => text.push(value.to_string()),
        }
        index += 1;
    }
    if text.is_empty() {
        return Err(CliError::Usage);
    }
    Ok(MemoryQueryOptions {
        text: text.join(" "),
        max_records,
        max_bytes,
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
        MemoryCliCommand::Help => {
            print_memory_help();
            Ok(())
        }
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
            parse_memory_command(&["root".into(), "add".into(), "/tmp/notes".into()])
                .expect("root add"),
            MemoryCliCommand::RootAdd {
                path: PathBuf::from("/tmp/notes")
            }
        );
        assert_eq!(
            parse_memory_command(&["root".into(), "list".into()]).expect("root list"),
            MemoryCliCommand::RootList(MemoryRootListOptions {
                limit: DEFAULT_MEMORY_ROOT_LIST_PAGE_SIZE,
                after: None,
            })
        );
        let after = Id::derived("memory-cli-test", b"after");
        assert_eq!(
            parse_memory_command(&[
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
            parse_memory_command(&["index".into(), root_id.to_string()]).expect("index"),
            MemoryCliCommand::Index { root_id }
        );

        let defaults = MemoryQuery::default();
        assert_eq!(
            parse_memory_command(&["query".into(), "deployment".into(), "notes".into()])
                .expect("query"),
            MemoryCliCommand::Query(MemoryQueryOptions {
                text: "deployment notes".to_string(),
                max_records: defaults.max_records,
                max_bytes: defaults.max_bytes,
            })
        );
        assert_eq!(
            parse_memory_command(&[
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
            parse_memory_command(&["query".into()]),
            Err(CliError::Usage)
        ));
        assert!(matches!(
            parse_memory_command(&[
                "query".into(),
                "notes".into(),
                "--max-records".into(),
                "0".into(),
            ]),
            Err(CliError::Parse { .. })
        ));
        assert!(matches!(
            parse_memory_command(&["index".into(), "not-an-id".into()]),
            Err(CliError::Parse { .. })
        ));
        assert!(matches!(
            parse_memory_command(&["root".into(), "list".into(), "--limit".into(), "257".into(),]),
            Err(CliError::Parse { .. })
        ));
    }
}
