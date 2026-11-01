use super::*;
use serde::Serialize;
pub(crate) use sinomo_memory::cli::MemoryCliCommand;
#[cfg(test)]
pub(crate) use sinomo_memory::cli::MemoryQueryOptions;
use sinomo_memory::cli::MemoryRootFilesystem;
#[cfg(test)]
pub(crate) use sinomo_memory::cli::MemoryRootListOptions;
use sinomo_memory::{
    IndexRootPolicy, IndexedFileMemoryStore, LocalIndexRootConfig, MemoryAccessContext,
    MemoryQuery, MemorySourceCatalog,
};

#[cfg(test)]
use sinomo_runtime::core::Id;

#[cfg(test)]
fn parse_memory_schema_for_test(args: &[String]) -> Result<MemoryCliCommand, CliError> {
    sinomo_memory::cli::parse_memory_command(args).map_err(Into::into)
}

pub(crate) fn run_memory_cli_command(command: MemoryCliCommand) -> Result<(), CliError> {
    match command {
        MemoryCliCommand::RootAdd { path, filesystem } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let store = IndexedFileMemoryStore::from_data_dir(&data_dir);
            let config = LocalIndexRootConfig::new(path, IndexRootPolicy::cluster_local());
            let root = match filesystem {
                MemoryRootFilesystem::Local => store.register_local_root(config)?,
                MemoryRootFilesystem::Sifs => store.register_sifs_root(config)?,
            };
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
                path: PathBuf::from("/tmp/notes"),
                filesystem: MemoryRootFilesystem::Local,
            }
        );
        assert_eq!(
            parse_memory_schema_for_test(&[
                "root".into(),
                "add".into(),
                "/users/notes".into(),
                "--filesystem".into(),
                "sifs".into(),
            ])
            .expect("SIFS root add"),
            MemoryCliCommand::RootAdd {
                path: PathBuf::from("/users/notes"),
                filesystem: MemoryRootFilesystem::Sifs,
            }
        );
        assert_eq!(
            parse_memory_schema_for_test(&["root".into(), "list".into()]).expect("root list"),
            MemoryCliCommand::RootList(MemoryRootListOptions {
                limit: 128,
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
            parse_memory_schema_for_test(&[
                "root".into(),
                "add".into(),
                "/notes".into(),
                "--filesystem".into(),
                "unknown".into(),
            ]),
            Err(CliError::Parse { .. })
        ));
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
