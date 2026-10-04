//! Module-owned DFS CLI grammar, composed by the product CLI host.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sinomo_api::dfs::*;
use sinomo_cli_registry::*;
use sinomo_dfs::cli::{
    default_dfs_file_system_id, non_empty_string, parse_dfs_file_system_id, parse_dfs_object_id,
    parse_dfs_version,
};
use sinomo_dfs::{DfsFileSystemId, DfsObjectId, DfsVersion};
use std::path::PathBuf;
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsFileHistoryPinOptions {
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: Option<DfsObjectId>,
    pub version: Option<DfsVersion>,
    pub operation: crate::DfsFileHistoryPinOperation,
    pub operation_id: Option<String>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsFileHistoryRetentionOptions {
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub retention: Option<crate::DfsFileHistoryRetention>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsFileHistoryCliCommand {
    List {
        request: crate::DfsFileHistoryListRequest,
        json: bool,
    },
    Read {
        identity: crate::DfsFileVersionIdentity,
        output: Option<PathBuf>,
    },
    Restore {
        source: crate::DfsFileVersionIdentity,
        path: String,
        expected: Option<crate::DfsFileVersionIdentity>,
        recover: bool,
        operation_id: Option<String>,
        json: bool,
    },
}

/// History-owned command payload used by a composing product CLI.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsHistoryCliCommand {
    History(DfsFileHistoryCliCommand),
    Pin(DfsFileHistoryPinOptions),
    Retention(DfsFileHistoryRetentionOptions),
}

pub fn cli_spec() -> CliCommandSpec {
    let option = |name: &'static str, help: &'static str, value: &'static str| {
        CliOptionSpec::value(name, help, CliValueSpec::new(value, help))
    };
    let path = || CliPositionalSpec::required(CliValueSpec::new("path", "absolute DFS path"));
    let mut commands = Vec::new();
    commands.push(
        CliCommandSpec::new(
            "list",
            "list explicit file history, including deleted files",
        )
        .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
            "path",
            "captured or current subtree path",
        ))])
        .with_options([
            option("--filesystem", "DFS filesystem id", "file-system-id").with_aliases(["--fs"]),
            option("--object", "canonical object identity", "object-id"),
            option(
                "--cursor",
                "continuation token from the preceding page",
                "cursor",
            ),
            option(
                "--limit",
                "maximum records scanned per page (1..128)",
                "limit",
            ),
            CliOptionSpec::flag("--json", "print JSON output"),
        ]),
    );
    commands.push(
        CliCommandSpec::new("read", "stream an explicitly retained file version").with_options([
            option("--filesystem", "source DFS filesystem id", "file-system-id")
                .with_aliases(["--fs"]),
            option("--object", "source object identity", "object-id").required(),
            option("--version", "source version", "version").required(),
            option(
                "--output",
                "write to a new local file instead of stdout",
                "file",
            ),
        ]),
    );
    for name in ["restore", "recover"] {
        commands.push(
            CliCommandSpec::new(
                name,
                if name == "restore" {
                    "replace an observed current file with retained content"
                } else {
                    "recover retained content at an unoccupied path"
                },
            )
            .with_positionals([path()])
            .with_options([
                option("--filesystem", "source DFS filesystem id", "file-system-id")
                    .with_aliases(["--fs"]),
                option("--object", "source object identity", "object-id").required(),
                option("--version", "source version", "version").required(),
                option(
                    "--expected-object",
                    "exact destination object to replace",
                    "object-id",
                ),
                option(
                    "--expected-version",
                    "exact destination generation to replace",
                    "version",
                ),
                option(
                    "--operation-id",
                    "stable identity for exact retries",
                    "operation-id",
                ),
                CliOptionSpec::flag("--json", "print JSON output"),
            ]),
        );
    }
    for (name, help) in [
        ("capture", "capture the current file and name its pin"),
        ("pin", "pin a retained file version"),
        ("unpin", "release one named history pin"),
    ] {
        commands.push(
            CliCommandSpec::new(name, help)
                .with_positionals([path()])
                .with_options([
                    option("--filesystem", "DFS filesystem id", "file-system-id")
                        .with_aliases(["--fs"]),
                    option("--object", "stable DFS object identity", "object-id"),
                    option("--version", "exact DFS version", "version"),
                    option("--pin", "independent history pin name", "name").required(),
                    option(
                        "--operation-id",
                        "reuse the same identity after an ambiguous outcome",
                        "operation-id",
                    ),
                    CliOptionSpec::flag("--json", "print JSON output"),
                ]),
        );
    }
    commands.push(
        CliCommandSpec::new(
            "retention",
            "show retention or set an explicit subtree capture rule",
        )
        .with_positionals([path()])
        .with_options([
            option("--filesystem", "DFS filesystem id", "file-system-id").with_aliases(["--fs"]),
            option(
                "--keep-last",
                "number of policy captures to keep; zero disables count retention",
                "count",
            ),
            option(
                "--keep-for-seconds",
                "capture age window; zero disables age retention",
                "seconds",
            ),
            CliOptionSpec::flag("--json", "print JSON output"),
        ]),
    );
    CliCommandSpec::new("history", "manage explicitly retained file content")
        .with_children(commands)
}

pub fn parse_matches(matches: &CliCommandMatches) -> Result<DfsHistoryCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["dfs", "history", "list"] => {
            let after = matches
                .value("--cursor")
                .map(|value| {
                    let bytes =
                        URL_SAFE_NO_PAD
                            .decode(value)
                            .map_err(|error| CliParseError::Parse {
                                detail: error.to_string(),
                            })?;
                    serde_json::from_slice(&bytes).map_err(|error| CliParseError::Parse {
                        detail: error.to_string(),
                    })
                })
                .transpose()?;
            let limit = matches
                .value("--limit")
                .unwrap_or("128")
                .parse::<u32>()
                .map_err(|error| CliParseError::Parse {
                    detail: error.to_string(),
                })?;
            if !(1..=128).contains(&limit) {
                return Err(CliParseError::Parse {
                    detail: "history limit must be in 1..=128".into(),
                });
            }
            Ok(DfsHistoryCliCommand::History(
                DfsFileHistoryCliCommand::List {
                    request: crate::DfsFileHistoryListRequest {
                        file_system_id: matches
                            .value("--filesystem")
                            .map(parse_dfs_file_system_id)
                            .transpose()?
                            .unwrap_or_else(default_dfs_file_system_id),
                        path: matches.positionals().first().cloned(),
                        object_id: matches
                            .value("--object")
                            .map(parse_dfs_object_id)
                            .transpose()?,
                        after,
                        limit,
                    },
                    json: matches.contains("--json"),
                },
            ))
        }
        ["dfs", "history", command @ ("read" | "restore" | "recover")] => {
            let source = crate::DfsFileVersionIdentity {
                file_system_id: matches
                    .value("--filesystem")
                    .map(parse_dfs_file_system_id)
                    .transpose()?
                    .unwrap_or_else(default_dfs_file_system_id),
                object_id: parse_dfs_object_id(required_option(matches, "--object")?)?,
                version: parse_dfs_version(required_option(matches, "--version")?)?,
            };
            if *command == "read" {
                return Ok(DfsHistoryCliCommand::History(
                    DfsFileHistoryCliCommand::Read {
                        identity: source,
                        output: matches.value("--output").map(PathBuf::from),
                    },
                ));
            }
            let expected = match (matches.value("--expected-object"), matches.value("--expected-version")) {
                (Some(object), Some(version)) if *command == "restore" => Some(crate::DfsFileVersionIdentity { file_system_id: sifs_file_system_id(), object_id: parse_dfs_object_id(object)?, version: parse_dfs_version(version)? }),
                (None, None) => None,
                _ => return Err(CliParseError::Parse { detail: "restore expectations require both --expected-object and --expected-version; recover requires an unoccupied path".into() }),
            };
            Ok(DfsHistoryCliCommand::History(
                DfsFileHistoryCliCommand::Restore {
                    source,
                    path: required_positional(matches, 0)?.to_string(),
                    expected,
                    recover: *command == "recover",
                    operation_id: matches.value("--operation-id").map(str::to_string),
                    json: matches.contains("--json"),
                },
            ))
        }
        ["dfs", "history", command @ ("capture" | "pin" | "unpin")] => {
            let pin_id = non_empty_string(required_option(matches, "--pin")?, "pin")
                .map_err(semantic_parse_error)?;
            let operation = match *command {
                "capture" => crate::DfsFileHistoryPinOperation::Capture { pin_id },
                "pin" => crate::DfsFileHistoryPinOperation::Pin { pin_id },
                _ => crate::DfsFileHistoryPinOperation::Unpin { pin_id },
            };
            let version = matches
                .value("--version")
                .map(parse_dfs_version)
                .transpose()
                .map_err(semantic_parse_error)?;
            if *command != "capture" && version.is_none() {
                return Err(CliParseError::Parse {
                    detail: "history pin and unpin require --version".into(),
                });
            }
            Ok(DfsHistoryCliCommand::Pin(DfsFileHistoryPinOptions {
                path: required_positional(matches, 0)?.to_string(),
                file_system_id: matches
                    .value("--filesystem")
                    .map(parse_dfs_file_system_id)
                    .transpose()
                    .map_err(semantic_parse_error)?
                    .unwrap_or_else(default_dfs_file_system_id),
                object_id: matches
                    .value("--object")
                    .map(parse_dfs_object_id)
                    .transpose()
                    .map_err(semantic_parse_error)?,
                version,
                operation,
                operation_id: matches.value("--operation-id").map(str::to_string),
                json: matches.contains("--json"),
            }))
        }
        ["dfs", "history", "retention"] => {
            let retention =
                if matches.contains("--keep-last") || matches.contains("--keep-for-seconds") {
                    Some(crate::DfsFileHistoryRetention {
                        keep_last: matches
                            .value("--keep-last")
                            .unwrap_or("0")
                            .parse()
                            .map_err(|error| CliParseError::Parse {
                                detail: format!("invalid retention count: {error}"),
                            })?,
                        keep_for_seconds: matches
                            .value("--keep-for-seconds")
                            .unwrap_or("0")
                            .parse()
                            .map_err(|error| CliParseError::Parse {
                                detail: format!("invalid retention seconds: {error}"),
                            })?,
                    })
                } else {
                    None
                };
            Ok(DfsHistoryCliCommand::Retention(
                DfsFileHistoryRetentionOptions {
                    path: required_positional(matches, 0)?.to_string(),
                    file_system_id: matches
                        .value("--filesystem")
                        .map(parse_dfs_file_system_id)
                        .transpose()
                        .map_err(semantic_parse_error)?
                        .unwrap_or_else(default_dfs_file_system_id),
                    retention,
                    json: matches.contains("--json"),
                },
            ))
        }
        _ => Err(CliParseError::Parse {
            detail: "unknown DFS file history command".into(),
        }),
    }
}
