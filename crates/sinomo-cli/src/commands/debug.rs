use super::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DebugCliCommand {
    Help,
    Sobj(DebugSobjOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DebugSobjOptions {
    pub(crate) path: PathBuf,
}

pub(crate) fn parse_debug_command(args: &[String]) -> Result<DebugCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DebugCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DebugCliCommand::Help)
        }
        Some((command, rest)) if command == "sobj" => {
            Ok(DebugCliCommand::Sobj(parse_debug_sobj_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_debug_sobj_options(args: &[String]) -> Result<DebugSobjOptions, CliError> {
    match args {
        [path] => Ok(DebugSobjOptions {
            path: PathBuf::from(path),
        }),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn run_debug_cli_command(command: DebugCliCommand) -> Result<(), CliError> {
    match command {
        DebugCliCommand::Help => {
            print_debug_help();
            Ok(())
        }
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
