use crate::{
    ArtifactId, DeclarativeModuleInstallError, DeclarativeModuleInstallRequest,
    ModuleBackendRegistration, ModuleDeclarativeResourceKind, ModuleId, ModuleLoaderStoreError,
    ModuleUnloadRecord, NativeDylibModuleInstallError, NativeDylibModuleInstallRequest,
    NativeModule, install_declarative_module_package, install_native_dylib_module_from_file,
    module_loader_applied_at_now, native_module_cache_path, read_declarative_module_loader_state,
    read_declarative_module_registration, remove_native_dylib_module_loader_state,
    set_native_rust_module_enabled, validate_native_dylib_module_cache_key,
    validate_native_dylib_module_library_name, write_module_unload_record,
};
use sinomo_cli_registry::{
    CliAutocompleteChoice, CliAutocompleteOption, CliAutocompleteSpec, CliAutocompleteValue,
    CliCommand, CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp,
    CliCommandMetadata, CliCommandPolicy, CliParseError,
};
use sinomo_core::ModuleKind;
use sinomo_local::SinomoDataDir;
use std::fmt;
use std::path::PathBuf;

const MODULE_POLICY: CliCommandPolicy = CliCommandPolicy {
    requires_cluster_state: false,
    supports_degraded_local_mode: false,
};

#[derive(Clone, Debug, Default)]
pub struct ModuleCliCommandConfig {
    pub toggleable_native_rust_modules: Vec<ModuleNativeRustToggleTarget>,
    pub wasix: Option<ModuleWasixCliConfig>,
}

impl ModuleCliCommandConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_toggleable_native_rust_module(
        mut self,
        module_id: ModuleId,
        detail: impl Into<String>,
    ) -> Self {
        let value = module_id.to_string();
        self.toggleable_native_rust_modules
            .push(ModuleNativeRustToggleTarget {
                module_id,
                value,
                detail: detail.into(),
            });
        self
    }

    pub fn with_toggleable_native_rust_module_name(
        mut self,
        module_id: &str,
        detail: impl Into<String>,
    ) -> Result<Self, CliParseError> {
        self.toggleable_native_rust_modules
            .push(ModuleNativeRustToggleTarget {
                module_id: parse_cli_module_id(module_id)?,
                value: module_id.to_string(),
                detail: detail.into(),
            });
        Ok(self)
    }

    pub fn with_wasix(mut self, wasix: ModuleWasixCliConfig) -> Self {
        self.wasix = Some(wasix);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleNativeRustToggleTarget {
    pub module_id: ModuleId,
    pub value: String,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleWasixCliConfig {
    pub module_id: &'static str,
    pub interface_version: &'static str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ModuleCliCommand {
    Help,
    Install(ModuleInstallOptions),
    Inspect(ModuleInspectOptions),
    NativeDylib(ModuleNativeDylibCliCommand),
    NativeRust(ModuleNativeRustCliCommand),
    Wasix(ModuleWasixCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ModuleNativeDylibCliCommand {
    Activate(ModuleNativeDylibActivateOptions),
    Unload(ModuleNativeDylibUnloadOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ModuleNativeRustCliCommand {
    Enable(ModuleNativeRustToggleOptions),
    Disable(ModuleNativeRustToggleOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ModuleWasixCliCommand {
    Activate(ModuleWasixActivateOptions),
    Unload,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
struct ModuleNativeDylibActivateOptions {
    source_path: PathBuf,
    expected_module_id: Option<ModuleId>,
    artifact_id: Option<ArtifactId>,
    expected_interface_version: Option<String>,
    cache_key: Option<String>,
    library_name: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ModuleWasixActivateOptions {
    source_path: PathBuf,
    artifact_id: Option<ArtifactId>,
    library_name: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ModuleNativeDylibUnloadOptions {
    module_id: ModuleId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ModuleNativeRustToggleOptions {
    module_id: ModuleId,
    reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ModuleInstallOptions {
    source_path: PathBuf,
    source: Option<String>,
    activate: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ModuleInspectOptions {
    module_id: ModuleId,
}

#[derive(Clone, Debug)]
pub struct ModuleCliCommandHandler {
    config: ModuleCliCommandConfig,
}

pub fn module_cli_command_handler(config: ModuleCliCommandConfig) -> ModuleCliCommandHandler {
    ModuleCliCommandHandler { config }
}

impl CliCommandHandler for ModuleCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        module_cli_metadata()
    }

    fn help(&self) -> CliCommandHelp {
        module_cli_help(&self.config)
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        module_cli_autocomplete(&self.config)
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(ModuleCliCommandRunner {
            command: parse_module_command(args, &self.config)?,
            config: self.config.clone(),
        }))
    }
}

#[derive(Debug)]
struct ModuleCliCommandRunner {
    command: ModuleCliCommand,
    config: ModuleCliCommandConfig,
}

impl CliCommand for ModuleCliCommandRunner {
    fn name(&self) -> &'static str {
        "module"
    }

    fn policy(&self) -> CliCommandPolicy {
        MODULE_POLICY
    }

    fn help(&self) -> CliCommandHelp {
        module_cli_help(&self.config)
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        module_cli_autocomplete(&self.config)
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        run_module_cli_command(context, self.command, &self.config)
    }
}

fn module_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "module",
        &[],
        "Manage local module activation.",
        MODULE_POLICY,
    )
}

fn module_cli_help(config: &ModuleCliCommandConfig) -> CliCommandHelp {
    let mut lines = vec![
        "  sinomo module".to_string(),
        "      Show module commands.".to_string(),
        "  sinomo module install [--inactive] [--source <text>] <folder>".to_string(),
        "      Import a declarative module package, cache its payload, register its catalog data, and activate it by default.".to_string(),
        "  sinomo module inspect <module-id>".to_string(),
        "      Show a locally registered declarative module package and activation state.".to_string(),
        "  sinomo module native-dylib activate [--module <module-id>] [--artifact <artifact-id>] [--interface <version>] [--cache-key <key>] [--library-name <file-name>] [--source <text>] <library>".to_string(),
        "      Install a native dynamic library module into the local data-dir module cache and activate its loader state.".to_string(),
        "  sinomo module native-dylib unload <module-id>".to_string(),
        "      Remove node-local native module activation state and best-effort release a cached native handle.".to_string(),
        "  sinomo module native-rust enable <module-id> [--reason <text>]".to_string(),
        "      Enable a toggleable built-in native Rust module at runtime.".to_string(),
        "  sinomo module native-rust disable <module-id> [--reason <text>]".to_string(),
        "      Disable a toggleable built-in native Rust module at runtime.".to_string(),
    ];
    if config.wasix.is_some() {
        lines.extend([
            "  sinomo module wasix activate [--artifact <artifact-id>] [--library-name <file-name>] [--source <text>] <library>".to_string(),
            "      Activate the trusted WASIX sandbox runtime module.".to_string(),
            "  sinomo module wasix unload".to_string(),
            "      Unload the active trusted WASIX sandbox runtime module state.".to_string(),
        ]);
    }
    CliCommandHelp::new("Module:", lines)
}

fn module_cli_autocomplete(config: &ModuleCliCommandConfig) -> CliAutocompleteSpec {
    let module_choices = config
        .toggleable_native_rust_modules
        .iter()
        .map(|target| CliAutocompleteChoice::new(target.value.clone(), target.detail.clone()))
        .collect::<Vec<_>>();
    let module_value =
        || CliAutocompleteValue::new("module-id", "module id", module_choices.iter().cloned());

    let mut children = vec![
        CliAutocompleteSpec::new("install", "install a declarative module package")
            .with_options([
                CliAutocompleteOption::flag("--inactive", "register without activating"),
                CliAutocompleteOption::with_value(
                    "--source",
                    "source label",
                    CliAutocompleteValue::new("text", "source label", []),
                ),
            ])
            .with_values([CliAutocompleteValue::new(
                "folder",
                "declarative module package folder",
                [],
            )]),
        CliAutocompleteSpec::new("inspect", "inspect a declarative module package")
            .with_values([CliAutocompleteValue::new("module-id", "module id", [])]),
        CliAutocompleteSpec::new("native-dylib", "manage native dynamic library modules")
            .with_children([
                CliAutocompleteSpec::new("activate", "activate a native library module")
                    .with_options([
                        CliAutocompleteOption::with_value(
                            "--module",
                            "expected module id",
                            CliAutocompleteValue::new("module-id", "module id", []),
                        ),
                        CliAutocompleteOption::with_value(
                            "--artifact",
                            "artifact id",
                            CliAutocompleteValue::new("artifact-id", "artifact id", []),
                        ),
                        CliAutocompleteOption::with_value(
                            "--interface",
                            "expected interface version",
                            CliAutocompleteValue::new("version", "interface version", []),
                        ),
                        CliAutocompleteOption::with_value(
                            "--cache-key",
                            "module cache key",
                            CliAutocompleteValue::new("key", "cache key", []),
                        ),
                        CliAutocompleteOption::with_value(
                            "--library-name",
                            "cached library file name",
                            CliAutocompleteValue::new("file-name", "library file name", []),
                        ),
                        CliAutocompleteOption::with_value(
                            "--source",
                            "source label",
                            CliAutocompleteValue::new("text", "source label", []),
                        ),
                    ])
                    .with_values([CliAutocompleteValue::new(
                        "library",
                        "native dynamic library path",
                        [],
                    )]),
                CliAutocompleteSpec::new("unload", "unload native module state")
                    .with_options([CliAutocompleteOption::with_value(
                        "--module",
                        "module id",
                        CliAutocompleteValue::new("module-id", "module id", []),
                    )])
                    .with_values([CliAutocompleteValue::new("module-id", "module id", [])]),
            ]),
        CliAutocompleteSpec::new("native-rust", "manage built-in native Rust modules")
            .with_children([
                CliAutocompleteSpec::new("enable", "enable a native Rust module")
                    .with_options([CliAutocompleteOption::with_value(
                        "--reason",
                        "reason text",
                        CliAutocompleteValue::new("text", "reason text", []),
                    )])
                    .with_values([module_value()]),
                CliAutocompleteSpec::new("disable", "disable a native Rust module")
                    .with_options([CliAutocompleteOption::with_value(
                        "--reason",
                        "reason text",
                        CliAutocompleteValue::new("text", "reason text", []),
                    )])
                    .with_values([module_value()]),
            ]),
    ];
    if config.wasix.is_some() {
        children.push(
            CliAutocompleteSpec::new("wasix", "manage the WASIX sandbox runtime module")
                .with_children([
                    CliAutocompleteSpec::new("activate", "activate the WASIX runtime module")
                        .with_options([
                            CliAutocompleteOption::with_value(
                                "--artifact",
                                "artifact id",
                                CliAutocompleteValue::new("artifact-id", "artifact id", []),
                            ),
                            CliAutocompleteOption::with_value(
                                "--library-name",
                                "cached library file name",
                                CliAutocompleteValue::new("file-name", "library file name", []),
                            ),
                            CliAutocompleteOption::with_value(
                                "--source",
                                "source label",
                                CliAutocompleteValue::new("text", "source label", []),
                            ),
                        ])
                        .with_values([CliAutocompleteValue::new(
                            "library",
                            "WASIX native dynamic library path",
                            [],
                        )]),
                    CliAutocompleteSpec::new(
                        "unload",
                        "remove the active WASIX runtime module state",
                    ),
                ]),
        );
    }
    CliAutocompleteSpec::new("module", "manage local module activation").with_children(children)
}

fn parse_module_command(
    args: &[String],
    config: &ModuleCliCommandConfig,
) -> Result<ModuleCliCommand, CliParseError> {
    match args.split_first() {
        None => Ok(ModuleCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ModuleCliCommand::Help)
        }
        Some((command, rest)) if command == "install" => Ok(ModuleCliCommand::Install(
            parse_module_install_options(rest)?,
        )),
        Some((command, rest)) if command == "inspect" => Ok(ModuleCliCommand::Inspect(
            parse_module_inspect_options(rest)?,
        )),
        Some((command, rest)) if command == "native-dylib" => {
            parse_module_native_dylib_command(rest)
        }
        Some((command, rest)) if command == "native-rust" => parse_module_native_rust_command(rest),
        Some((command, rest)) if command == "wasix" && config.wasix.is_some() => {
            parse_module_wasix_command(rest)
        }
        Some((command, _)) if command == "wasix" => Err(CliParseError::Parse {
            detail: "WASIX module CLI support is not configured".to_string(),
        }),
        Some((command, _)) => Err(CliParseError::Parse {
            detail: format!(
                "module command `{command}` is not implemented; run `sinomo module help`"
            ),
        }),
    }
}

fn parse_module_native_rust_command(args: &[String]) -> Result<ModuleCliCommand, CliParseError> {
    match args.split_first() {
        Some((command, rest)) if command == "enable" => Ok(ModuleCliCommand::NativeRust(
            ModuleNativeRustCliCommand::Enable(parse_module_native_rust_toggle_options(rest)?),
        )),
        Some((command, rest)) if command == "disable" => Ok(ModuleCliCommand::NativeRust(
            ModuleNativeRustCliCommand::Disable(parse_module_native_rust_toggle_options(rest)?),
        )),
        _ => Err(CliParseError::Usage),
    }
}

fn parse_module_native_dylib_command(args: &[String]) -> Result<ModuleCliCommand, CliParseError> {
    match args.split_first() {
        Some((command, rest)) if command == "activate" => Ok(ModuleCliCommand::NativeDylib(
            ModuleNativeDylibCliCommand::Activate(parse_module_native_dylib_activate_options(
                rest,
            )?),
        )),
        Some((command, rest)) if command == "unload" => Ok(ModuleCliCommand::NativeDylib(
            ModuleNativeDylibCliCommand::Unload(parse_module_native_dylib_unload_options(rest)?),
        )),
        _ => Err(CliParseError::Usage),
    }
}

fn parse_module_install_options(args: &[String]) -> Result<ModuleInstallOptions, CliParseError> {
    let mut source_path = None;
    let mut source = None;
    let mut activate = true;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--inactive" => {
                activate = false;
            }
            "--source" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                source = Some(non_empty_value(value, "source")?);
            }
            value if value.starts_with('-') => return Err(CliParseError::Usage),
            value => {
                if source_path.is_none() {
                    source_path = Some(PathBuf::from(value));
                } else {
                    return Err(CliParseError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleInstallOptions {
        source_path: source_path.ok_or(CliParseError::Usage)?,
        source,
        activate,
    })
}

fn parse_module_inspect_options(args: &[String]) -> Result<ModuleInspectOptions, CliParseError> {
    let mut module_id = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                module_id = Some(parse_cli_module_id(value)?);
            }
            value if value.starts_with('-') => return Err(CliParseError::Usage),
            value => {
                if module_id.is_none() {
                    module_id = Some(parse_cli_module_id(value)?);
                } else {
                    return Err(CliParseError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleInspectOptions {
        module_id: module_id.ok_or(CliParseError::Usage)?,
    })
}

fn parse_module_wasix_command(args: &[String]) -> Result<ModuleCliCommand, CliParseError> {
    match args.split_first() {
        Some((command, rest)) if command == "activate" => Ok(ModuleCliCommand::Wasix(
            ModuleWasixCliCommand::Activate(parse_module_wasix_activate_options(rest)?),
        )),
        Some((command, [])) if command == "unload" => {
            Ok(ModuleCliCommand::Wasix(ModuleWasixCliCommand::Unload))
        }
        _ => Err(CliParseError::Usage),
    }
}

fn parse_module_native_rust_toggle_options(
    args: &[String],
) -> Result<ModuleNativeRustToggleOptions, CliParseError> {
    let mut module_id = None;
    let mut reason = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                module_id = Some(parse_cli_module_id(value)?);
            }
            "--reason" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                reason = Some(non_empty_value(value, "reason")?);
            }
            value if value.starts_with('-') => return Err(CliParseError::Usage),
            value => {
                if module_id.is_none() {
                    module_id = Some(parse_cli_module_id(value)?);
                } else {
                    return Err(CliParseError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleNativeRustToggleOptions {
        module_id: module_id.ok_or(CliParseError::Usage)?,
        reason,
    })
}

fn parse_module_native_dylib_activate_options(
    args: &[String],
) -> Result<ModuleNativeDylibActivateOptions, CliParseError> {
    let mut options = ModuleNativeDylibActivateOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.expected_module_id = Some(parse_cli_module_id(value)?);
            }
            "--artifact" | "--artifact-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.artifact_id = Some(parse_cli_module_artifact_id(value)?);
            }
            "--interface" | "--interface-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.expected_interface_version = Some(non_empty_value(value, "interface")?);
            }
            "--cache-key" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.cache_key = Some(validate_native_module_cache_key(value)?);
            }
            "--library-name" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.library_name = Some(validate_native_module_library_name(value)?);
            }
            "--source" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.source = Some(non_empty_value(value, "source")?);
            }
            value if value.starts_with('-') => return Err(CliParseError::Usage),
            value => {
                if options.source_path.as_os_str().is_empty() {
                    options.source_path = PathBuf::from(value);
                } else {
                    return Err(CliParseError::Usage);
                }
            }
        }
        index += 1;
    }
    if options.source_path.as_os_str().is_empty() {
        return Err(CliParseError::Usage);
    }
    Ok(options)
}

fn parse_module_wasix_activate_options(
    args: &[String],
) -> Result<ModuleWasixActivateOptions, CliParseError> {
    let mut source_path = None;
    let mut artifact_id = None;
    let mut library_name = None;
    let mut source = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--artifact" | "--artifact-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                artifact_id = Some(parse_cli_module_artifact_id(value)?);
            }
            "--library-name" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                library_name = Some(validate_native_module_library_name(value)?);
            }
            "--source" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                source = Some(non_empty_value(value, "source")?);
            }
            value if value.starts_with('-') => return Err(CliParseError::Usage),
            value => {
                if source_path.is_none() {
                    source_path = Some(PathBuf::from(value));
                } else {
                    return Err(CliParseError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleWasixActivateOptions {
        source_path: source_path.ok_or(CliParseError::Usage)?,
        artifact_id,
        library_name,
        source,
    })
}

fn parse_module_native_dylib_unload_options(
    args: &[String],
) -> Result<ModuleNativeDylibUnloadOptions, CliParseError> {
    let mut module_id = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                module_id = Some(parse_cli_module_id(value)?);
            }
            value if value.starts_with('-') => return Err(CliParseError::Usage),
            value => {
                if module_id.is_none() {
                    module_id = Some(parse_cli_module_id(value)?);
                } else {
                    return Err(CliParseError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleNativeDylibUnloadOptions {
        module_id: module_id.ok_or(CliParseError::Usage)?,
    })
}

fn run_module_cli_command(
    context: &mut CliCommandContext<'_>,
    command: ModuleCliCommand,
    config: &ModuleCliCommandConfig,
) -> Result<(), CliCommandError> {
    match command {
        ModuleCliCommand::Help => module_cli_help(config).write(context),
        ModuleCliCommand::Install(options) => install_declarative_module(context, options),
        ModuleCliCommand::Inspect(options) => inspect_declarative_module(context, options),
        ModuleCliCommand::NativeDylib(ModuleNativeDylibCliCommand::Activate(options)) => {
            activate_native_dylib_module(context, options)
        }
        ModuleCliCommand::NativeDylib(ModuleNativeDylibCliCommand::Unload(options)) => {
            unload_native_dylib_module(context, options)
        }
        ModuleCliCommand::NativeRust(ModuleNativeRustCliCommand::Enable(options)) => {
            set_native_rust_module_runtime_enabled(context, options, config, true)
        }
        ModuleCliCommand::NativeRust(ModuleNativeRustCliCommand::Disable(options)) => {
            set_native_rust_module_runtime_enabled(context, options, config, false)
        }
        ModuleCliCommand::Wasix(ModuleWasixCliCommand::Activate(options)) => {
            activate_wasix_native_module(context, options, config)
        }
        ModuleCliCommand::Wasix(ModuleWasixCliCommand::Unload) => {
            unload_wasix_native_module(context, config)
        }
    }
}

fn install_declarative_module(
    context: &mut CliCommandContext<'_>,
    options: ModuleInstallOptions,
) -> Result<(), CliCommandError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    data_dir.ensure_root_layout().map_err(command_error)?;
    let mut request = DeclarativeModuleInstallRequest::new(
        options
            .source
            .unwrap_or_else(|| format!("local:{}", options.source_path.display())),
    );
    if !options.activate {
        request = request.inactive();
    }
    let result =
        install_declarative_module_package(&data_dir.root(), &options.source_path, request)
            .map_err(declarative_module_install_error_to_command)?;

    context.stdout_line(format_args!("module: {}", result.registration.module_id()))?;
    context.stdout_line(format_args!(
        "version: {}",
        result.registration.manifest.version
    ))?;
    context.stdout_line(format_args!("backend: declarative"))?;
    context.stdout_line(format_args!("hash: {}", result.state.content_hash))?;
    context.stdout_line(format_args!("payload-ref: {}", result.state.payload_ref))?;
    context.stdout_line(format_args!("payload: {}", result.payload_path.display()))?;
    context.stdout_line(format_args!("catalog: {}", result.catalog_path.display()))?;
    context.stdout_line(format_args!("active: {}", result.state.active))?;
    if let Some(path) = result.state_path.as_ref() {
        context.stdout_line(format_args!("state: {}", path.display()))?;
    }
    write_declarative_resource_summary(context, &result.registration.surfaces)?;
    Ok(())
}

fn inspect_declarative_module(
    context: &mut CliCommandContext<'_>,
    options: ModuleInspectOptions,
) -> Result<(), CliCommandError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    data_dir.ensure_root_layout().map_err(command_error)?;
    let registration = read_declarative_module_registration(&data_dir.root(), &options.module_id)
        .map_err(module_loader_store_error_to_command)?
        .ok_or_else(|| {
            CliCommandError::new(format!(
                "declarative module `{}` is not registered locally",
                options.module_id
            ))
        })?;
    let state = read_declarative_module_loader_state(&data_dir.root(), &options.module_id)
        .map_err(module_loader_store_error_to_command)?;

    context.stdout_line(format_args!("module: {}", registration.module_id()))?;
    context.stdout_line(format_args!("version: {}", registration.manifest.version))?;
    context.stdout_line(format_args!("backend: declarative"))?;
    if let ModuleBackendRegistration::Declarative { payload_ref } = &registration.backend {
        context.stdout_line(format_args!("payload-ref: {payload_ref}"))?;
    }
    match state {
        Some(state) => {
            context.stdout_line(format_args!("active: {}", state.active))?;
            context.stdout_line(format_args!("hash: {}", state.content_hash))?;
            context.stdout_line(format_args!("installed-at: {}", state.installed_at))?;
            context.stdout_line(format_args!("source: {}", state.source))?;
        }
        None => {
            context.stdout_line(format_args!("active: false"))?;
            context.stdout_line(format_args!("state: not-active"))?;
        }
    }
    write_declarative_resource_summary(context, &registration.surfaces)?;
    Ok(())
}

fn write_declarative_resource_summary(
    context: &mut CliCommandContext<'_>,
    surfaces: &crate::ModuleSurfaces,
) -> Result<(), CliCommandError> {
    let mut skills = 0usize;
    let mut prompts = 0usize;
    let mut prompt_packs = 0usize;
    let mut eval_suites = 0usize;
    let mut other = 0usize;
    for resource in &surfaces.declarative_resources {
        match resource.kind {
            ModuleDeclarativeResourceKind::Skill => skills += 1,
            ModuleDeclarativeResourceKind::Prompt => prompts += 1,
            ModuleDeclarativeResourceKind::PromptPack => prompt_packs += 1,
            ModuleDeclarativeResourceKind::EvalSuite => eval_suites += 1,
            ModuleDeclarativeResourceKind::Example
            | ModuleDeclarativeResourceKind::Policy
            | ModuleDeclarativeResourceKind::Config => other += 1,
        }
    }
    context.stdout_line(format_args!("skills: {skills}"))?;
    context.stdout_line(format_args!("prompts: {prompts}"))?;
    context.stdout_line(format_args!("prompt-packs: {prompt_packs}"))?;
    context.stdout_line(format_args!("eval-suites: {eval_suites}"))?;
    context.stdout_line(format_args!("other-resources: {other}"))?;
    for resource in &surfaces.declarative_resources {
        context.stdout_line(format_args!(
            "resource: {} {} {}",
            resource.kind.as_str(),
            resource.id,
            resource.path
        ))?;
    }
    Ok(())
}

fn activate_wasix_native_module(
    context: &mut CliCommandContext<'_>,
    options: ModuleWasixActivateOptions,
    config: &ModuleCliCommandConfig,
) -> Result<(), CliCommandError> {
    let wasix = config
        .wasix
        .as_ref()
        .ok_or_else(|| CliCommandError::new("WASIX module CLI support is not configured"))?;
    activate_native_dylib_module(
        context,
        ModuleNativeDylibActivateOptions {
            source_path: options.source_path,
            expected_module_id: Some(parse_cli_module_id(wasix.module_id).map_err(command_error)?),
            artifact_id: options.artifact_id,
            expected_interface_version: Some(wasix.interface_version.to_string()),
            cache_key: Some("module/sandbox/wasix".to_string()),
            library_name: options.library_name,
            source: options.source,
        },
    )
}

fn unload_wasix_native_module(
    context: &mut CliCommandContext<'_>,
    config: &ModuleCliCommandConfig,
) -> Result<(), CliCommandError> {
    let wasix = config
        .wasix
        .as_ref()
        .ok_or_else(|| CliCommandError::new("WASIX module CLI support is not configured"))?;
    unload_native_dylib_module(
        context,
        ModuleNativeDylibUnloadOptions {
            module_id: parse_cli_module_id(wasix.module_id).map_err(command_error)?,
        },
    )
}

fn set_native_rust_module_runtime_enabled(
    context: &mut CliCommandContext<'_>,
    options: ModuleNativeRustToggleOptions,
    config: &ModuleCliCommandConfig,
    enabled: bool,
) -> Result<(), CliCommandError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    data_dir.ensure_root_layout().map_err(command_error)?;
    ensure_native_rust_module_toggleable(&options.module_id, config)?;
    let path = set_native_rust_module_enabled(
        &data_dir.root(),
        &options.module_id,
        enabled,
        "local-cli",
        options.reason.unwrap_or_else(|| {
            if enabled {
                "operator enabled native Rust module".to_string()
            } else {
                "operator disabled native Rust module".to_string()
            }
        }),
    )
    .map_err(module_loader_store_error_to_command)?;

    context.stdout_line(format_args!("module: {}", options.module_id))?;
    context.stdout_line(format_args!("backend: native-rust"))?;
    context.stdout_line(format_args!("enabled: {enabled}"))?;
    context.stdout_line(format_args!("state: {}", path.display()))?;
    Ok(())
}

fn ensure_native_rust_module_toggleable(
    module_id: &ModuleId,
    config: &ModuleCliCommandConfig,
) -> Result<(), CliCommandError> {
    if config
        .toggleable_native_rust_modules
        .iter()
        .any(|target| target.module_id == *module_id)
    {
        Ok(())
    } else {
        Err(CliCommandError::new(format!(
            "module `{module_id}` does not support runtime enable/disable"
        )))
    }
}

fn activate_native_dylib_module(
    context: &mut CliCommandContext<'_>,
    options: ModuleNativeDylibActivateOptions,
) -> Result<(), CliCommandError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    data_dir.ensure_root_layout().map_err(command_error)?;
    let result = install_native_dylib_module_from_file(
        &data_dir.root(),
        &options.source_path,
        NativeDylibModuleInstallRequest {
            expected_module_id: options.expected_module_id,
            artifact_id: options.artifact_id,
            expected_interface_version: options.expected_interface_version,
            cache_key: options.cache_key,
            library_name: options.library_name,
            source: options
                .source
                .unwrap_or_else(|| format!("local:{}", options.source_path.display())),
        },
    )
    .map_err(native_dylib_module_install_error_to_command)?;

    context.stdout_line(format_args!("module: {}", result.descriptor_module_id))?;
    context.stdout_line(format_args!("module-state-id: {}", result.state.module_id))?;
    if let Some(artifact_id) = result.state.artifact_id.as_ref() {
        context.stdout_line(format_args!("artifact: {artifact_id}"))?;
    }
    context.stdout_line(format_args!(
        "interface: {}",
        result.state.interface_version
    ))?;
    context.stdout_line(format_args!("platform: {}", result.state.platform_triple))?;
    context.stdout_line(format_args!("hash: {}", result.state.content_hash))?;
    context.stdout_line(format_args!("cache: {}", result.cache_path.display()))?;
    context.stdout_line(format_args!("state: {}", result.state_path.display()))?;
    Ok(())
}

fn unload_native_dylib_module(
    context: &mut CliCommandContext<'_>,
    options: ModuleNativeDylibUnloadOptions,
) -> Result<(), CliCommandError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    data_dir.ensure_root_layout().map_err(command_error)?;
    let module_id = options.module_id;
    let previous_state = remove_native_dylib_module_loader_state(&data_dir.root(), &module_id)
        .map_err(module_loader_store_error_to_command)?;

    let mut artifact_id = None;
    let mut content_hash = None;
    let mut cache_key = None;
    let mut library_name = None;
    let mut runtime_unload_attempted = false;
    let mut runtime_unload_succeeded = previous_state.is_none();
    let mut runtime_unload_detail = "no active native-dylib loader state".to_string();
    let mut cache_path = None;

    if let Some(state) = previous_state.as_ref() {
        artifact_id = state.artifact_id.clone();
        content_hash = Some(state.content_hash.clone());
        cache_key = Some(state.cache_key.clone());
        library_name = Some(state.library_name.clone());
        let path = native_module_cache_path(
            data_dir.root(),
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        cache_path = Some(path.clone());
        if path.exists() {
            runtime_unload_attempted = true;
            match unsafe {
                NativeModule::load_from_content_addressed_path(&path, &state.content_hash)
            } {
                Ok(module) => {
                    let report = module.unload();
                    runtime_unload_succeeded = report.shutdown_ok && report.library_handle_released;
                    runtime_unload_detail = format!(
                        "{}; library_handle_released={}",
                        report.detail, report.library_handle_released
                    );
                }
                Err(error) => {
                    runtime_unload_succeeded = false;
                    runtime_unload_detail =
                        format!("load cached native module for unload probe: {error}");
                }
            }
        } else {
            runtime_unload_detail = format!(
                "cached native module library not found at {}; active state was still removed",
                path.display()
            );
        }
    }

    let record = ModuleUnloadRecord {
        module_id: module_id.clone(),
        backend: ModuleKind::NativeDylib,
        artifact_id,
        content_hash,
        cache_key,
        library_name,
        state_removed: previous_state.is_some(),
        runtime_unload_attempted,
        runtime_unload_succeeded,
        runtime_unload_detail: runtime_unload_detail.clone(),
        unloaded_at: module_loader_applied_at_now(),
        source: "local-cli".to_string(),
    };
    let record_path = write_module_unload_record(&data_dir.root(), &record)
        .map_err(module_loader_store_error_to_command)?;

    context.stdout_line(format_args!("module-state-id: {module_id}"))?;
    context.stdout_line(format_args!(
        "state: {}",
        if previous_state.is_some() {
            "removed"
        } else {
            "not-active"
        }
    ))?;
    if let Some(cache_path) = cache_path.as_ref() {
        context.stdout_line(format_args!("cache: {}", cache_path.display()))?;
    }
    context.stdout_line(format_args!(
        "runtime-unload: {}",
        if runtime_unload_attempted {
            if runtime_unload_succeeded {
                "ok"
            } else {
                "failed"
            }
        } else {
            "skipped"
        }
    ))?;
    context.stdout_line(format_args!("detail: {runtime_unload_detail}"))?;
    context.stdout_line(format_args!("record: {}", record_path.display()))?;
    Ok(())
}

fn validate_native_module_cache_key(value: &str) -> Result<String, CliParseError> {
    validate_native_dylib_module_cache_key(value)
        .map_err(native_dylib_module_install_error_to_parse)
}

fn validate_native_module_library_name(value: &str) -> Result<String, CliParseError> {
    validate_native_dylib_module_library_name(value)
        .map_err(native_dylib_module_install_error_to_parse)
}

fn non_empty_value(value: &str, label: &str) -> Result<String, CliParseError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(CliParseError::Parse {
            detail: format!("{label} must be non-empty"),
        });
    }
    Ok(trimmed)
}

fn parse_cli_module_id(value: &str) -> Result<ModuleId, CliParseError> {
    ModuleId::new(value.to_string())
        .or_else(|_| ModuleId::from_legacy_named(value))
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid module id `{value}`: {source}"),
        })
}

fn parse_cli_module_artifact_id(value: &str) -> Result<ArtifactId, CliParseError> {
    ArtifactId::new(value.to_string())
        .or_else(|_| ArtifactId::from_legacy_named(value))
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid module artifact id `{value}`: {source}"),
        })
}

fn module_loader_store_error_to_command(error: ModuleLoaderStoreError) -> CliCommandError {
    command_error(error)
}

fn native_dylib_module_install_error_to_command(
    error: NativeDylibModuleInstallError,
) -> CliCommandError {
    command_error(error)
}

fn declarative_module_install_error_to_command(
    error: DeclarativeModuleInstallError,
) -> CliCommandError {
    command_error(error)
}

fn native_dylib_module_install_error_to_parse(
    error: NativeDylibModuleInstallError,
) -> CliParseError {
    match error {
        NativeDylibModuleInstallError::Descriptor { detail } => CliParseError::Parse { detail },
        other => CliParseError::Parse {
            detail: other.to_string(),
        },
    }
}

fn command_error(error: impl fmt::Display) -> CliCommandError {
    CliCommandError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ModuleCliCommandConfig {
        ModuleCliCommandConfig::new()
            .with_toggleable_native_rust_module_name(
                "module.observe",
                "operational observation module",
            )
            .expect("module id")
            .with_wasix(ModuleWasixCliConfig {
                module_id: "module.sandbox.wasix",
                interface_version: "sinomo.sandbox.wasix.v1",
            })
    }

    #[test]
    fn module_handler_parses_native_rust_toggle() {
        let handler = module_cli_command_handler(test_config());
        let command = handler
            .parse(&[
                "native-rust".to_string(),
                "disable".to_string(),
                "module.observe".to_string(),
            ])
            .expect("parse");
        assert_eq!(command.name(), "module");
        assert_eq!(command.policy(), MODULE_POLICY);
    }

    #[test]
    fn module_handler_exposes_help_and_autocomplete() {
        let handler = module_cli_command_handler(test_config());
        let help = handler.help();
        assert_eq!(help.title, "Module:");
        assert!(
            help.lines
                .iter()
                .any(|line| line.contains("module install"))
        );
        assert!(
            help.lines
                .iter()
                .any(|line| line.contains("native-rust enable"))
        );

        let autocomplete = handler.autocomplete();
        assert_eq!(autocomplete.word, "module");
        assert!(
            autocomplete
                .children
                .iter()
                .any(|child| child.word == "install")
        );
        assert!(
            autocomplete
                .children
                .iter()
                .any(|child| child.word == "inspect")
        );
        assert!(
            autocomplete
                .children
                .iter()
                .any(|child| child.word == "native-rust")
        );
        assert!(
            autocomplete
                .children
                .iter()
                .any(|child| child.word == "wasix")
        );
    }

    #[test]
    fn module_handler_parses_declarative_install_and_inspect() {
        let install = parse_module_command(
            &[
                "install".to_string(),
                "--inactive".to_string(),
                "--source".to_string(),
                "test".to_string(),
                "/tmp/pkg".to_string(),
            ],
            &test_config(),
        )
        .expect("install parse");
        assert!(matches!(
            install,
            ModuleCliCommand::Install(ModuleInstallOptions {
                ref source_path,
                ref source,
                activate: false,
            }) if source_path == &PathBuf::from("/tmp/pkg")
                && source.as_deref() == Some("test")
        ));

        let inspect = parse_module_command(
            &[
                "inspect".to_string(),
                "module.skills.rust_debugging".to_string(),
            ],
            &test_config(),
        )
        .expect("inspect parse");
        assert!(matches!(
            inspect,
            ModuleCliCommand::Inspect(ModuleInspectOptions { .. })
        ));
    }
}
