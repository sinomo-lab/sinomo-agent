use super::*;
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliPositionalSpec, CliValueSpec,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DebugCliCommand {
    Sobj(DebugSobjOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DebugSobjOptions {
    pub(crate) path: PathBuf,
}

pub(crate) fn debug_cli_command_handler() -> DeclarativeCommandHandler<DebugCliCommand> {
    DeclarativeCommandHandler::new(
        "debug",
        CliCommandPolicy::default(),
        debug_cli_spec,
        parse_debug_matches,
        run_debug_cli_command,
    )
}

fn debug_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("debug", "inspect low-level Sinomo data")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("sobj", "decode and print a SOBJ document").with_positionals([
                CliPositionalSpec::required(CliValueSpec::new("path", "SOBJ file path")),
            ]),
        ])
}

fn parse_debug_matches(
    matches: &CliCommandMatches,
) -> Result<DebugCliCommand, sinomo_runtime::cli_registry::CliParseError> {
    match matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["debug", "sobj"] => Ok(DebugCliCommand::Sobj(DebugSobjOptions {
            path: PathBuf::from(required_positional(matches, 0)?),
        })),
        _ => Err(sinomo_runtime::cli_registry::CliParseError::Parse {
            detail: "debug requires a subcommand".to_string(),
        }),
    }
}

pub(crate) fn run_debug_cli_command(command: DebugCliCommand) -> Result<(), CliError> {
    match command {
        DebugCliCommand::Sobj(options) => debug_print_sobj(options),
    }
}

fn debug_print_sobj(options: DebugSobjOptions) -> Result<(), CliError> {
    let bytes = fs::read(&options.path).map_err(|source| CliError::Io {
        path: options.path.display().to_string(),
        detail: source.to_string(),
    })?;
    let view = sinomo_runtime::object::DocumentView::parse(&bytes).map_err(|source| {
        debug_sobj_parse_error(&options.path, format!("invalid SOBJ document: {source}"))
    })?;
    println!(
        "{}",
        sinomo_runtime::object::text::document_view_to_text(&view)
    );
    Ok(())
}

fn debug_sobj_parse_error(path: &Path, detail: impl Into<String>) -> CliError {
    CliError::Parse {
        detail: format!("{}: {}", path.display(), detail.into()),
    }
}
