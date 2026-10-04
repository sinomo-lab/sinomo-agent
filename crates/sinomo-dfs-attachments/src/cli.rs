//! Module-owned DFS CLI grammar, composed by the product CLI host.
use sinomo_api::dfs::*;
use sinomo_cli_registry::*;

use std::path::PathBuf;
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsAttachmentCliOptions {
    pub action: String,
    pub path: String,
    pub kind: Option<String>,
    pub name: String,
    pub input: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub schema: Option<String>,
    pub media_type: Option<String>,
    pub bind_version: bool,
    pub limit: usize,
    pub cursor: Option<String>,
    pub operation_id: Option<String>,
}

pub fn cli_spec() -> CliCommandSpec {
    let option =
        |name, help, value| CliOptionSpec::value(name, help, CliValueSpec::new(value, help));
    CliCommandSpec::new(
        "attachment",
        "manage file tags, transcripts and custom DFS attachments",
    )
    .with_children(
        ["list", "get", "put", "delete", "tags", "transcript"].map(|action| {
            CliCommandSpec::new(action, "read or conditionally update file-associated data")
                .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                    "path",
                    "source SIFS file",
                ))])
                .with_options([
                    option("--kind", "namespaced attachment kind", "kind"),
                    option("--name", "attachment variant (default: default)", "name"),
                    option(
                        "--input",
                        "payload file; tags are JSON, transcripts NDJSON",
                        "path",
                    ),
                    option("--output", "write payload to this local file", "path"),
                    option("--schema", "versioned custom schema identifier", "schema"),
                    option("--media-type", "custom payload media type", "media-type"),
                    CliOptionSpec::flag(
                        "--bind-version",
                        "bind custom data to the current source version",
                    ),
                    option("--limit", "list page size, 1..64", "count"),
                    option("--cursor", "opaque list continuation", "cursor"),
                    option(
                        "--operation-id",
                        "stable mutation identity for reconciliation",
                        "id",
                    ),
                ])
        }),
    )
}

pub fn parse_matches(
    matches: &CliCommandMatches,
) -> Result<DfsAttachmentCliOptions, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["dfs", "attachment", action] => {
            if matches.contains("--input") && !matches!(*action, "put" | "tags" | "transcript") {
                return Err(CliParseError::Parse {
                    detail: "--input is only valid for put, tags, or transcript".into(),
                });
            }
            if *action == "put" && !matches.contains("--input") {
                return Err(CliParseError::Parse {
                    detail: "attachment put requires --input".into(),
                });
            }
            let limit = matches
                .value("--limit")
                .unwrap_or("32")
                .parse::<usize>()
                .map_err(|_| CliParseError::Parse {
                    detail: "invalid attachment page limit".into(),
                })?;
            if !(1..=DFS_ATTACHMENT_MAX_PAGE).contains(&limit) {
                return Err(CliParseError::Parse {
                    detail: "attachment page limit must be 1..64".into(),
                });
            }
            Ok(DfsAttachmentCliOptions {
                action: (*action).into(),
                path: required_positional(matches, 0)?.into(),
                kind: matches.value("--kind").map(str::to_owned),
                name: matches.value("--name").unwrap_or("default").into(),
                input: matches.value("--input").map(PathBuf::from),
                output: matches.value("--output").map(PathBuf::from),
                schema: matches.value("--schema").map(str::to_owned),
                media_type: matches.value("--media-type").map(str::to_owned),
                bind_version: matches.contains("--bind-version"),
                limit,
                cursor: matches.value("--cursor").map(str::to_owned),
                operation_id: matches.value("--operation-id").map(str::to_owned),
            })
        }
        _ => Err(CliParseError::Parse {
            detail: "unknown DFS attachment command".into(),
        }),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn parse_dfs_command(args: &[String]) -> Result<DfsAttachmentCliOptions, CliParseError> {
        parse_command_args(
            sinomo_dfs::cli::dfs_cli_spec().with_children([cli_spec()]),
            args,
            parse_matches,
        )
    }
    #[test]
    fn attachment_cli_requires_explicit_write_intent_and_bounded_pages() {
        let read = ["attachment", "tags", "/users/file"].map(String::from);
        assert!(parse_dfs_command(&read).unwrap().input.is_none());
        for args in [
            vec![
                "attachment",
                "get",
                "/users/file",
                "--kind",
                "org.example.data",
                "--input",
                "payload",
            ],
            vec![
                "attachment",
                "put",
                "/users/file",
                "--kind",
                "org.example.data",
            ],
            vec!["attachment", "list", "/users/file", "--limit", "1000000"],
        ] {
            assert!(
                parse_dfs_command(&args.into_iter().map(String::from).collect::<Vec<_>>()).is_err()
            );
        }
    }
}
