//! Initial setup only. Cluster evolution and release activation remain rollout-owned.
#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use rfd::{MessageButtons, MessageDialog, MessageDialogResult};
use sinomo_runtime::{installer::initial_setup::*, rollout::desktop};
#[cfg(target_os = "linux")]
use std::process::Command;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    if let Err(error) = run() {
        eprintln!("Sinomo setup: {error}");
        if !env::args().any(|arg| arg == "--no-dialogs") {
            MessageDialog::new()
                .set_title("Sinomo setup")
                .set_level(rfd::MessageLevel::Error)
                .set_description(error.to_string())
                .show();
        }
        std::process::exit(1);
    }
}

fn value(args: &[String], key: &str) -> Result<PathBuf> {
    args.windows(2)
        .find(|p| p[0] == key)
        .map(|p| PathBuf::from(&p[1]))
        .ok_or_else(|| format!("missing {key}").into())
}

fn run() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        #[cfg(target_os = "linux")]
        Some("--verify-package") => {
            let exe = env::current_exe()?;
            let (_guard, source) = unpack_payload(exe.parent().ok_or("no package directory")?)?;
            desktop::verify_bundle(&source, &desktop::read_manifest(&source)?)?;
            Ok(())
        }
        Some("install" | "configure") => {
            if args.iter().any(|arg| arg == "--interactive") {
                install_gui(&args)
            } else {
                install_command(&args)
            }
        }
        #[cfg(target_os = "linux")]
        Some("--launch-cli" | "--launch-app") => {
            let root = installed_root()?;
            let receipt = read_receipt(&root)?;
            let stem = if args[0] == "--launch-cli" {
                "sinomo"
            } else {
                "sinomo-app"
            };
            let mut command = Command::new(receipt.data_dir.join("bin").join(stem));
            command.env("SINOMO_INSTALL_DIR", &root);
            command.args(&args[1..]);
            if env::var("SINOMO_DATA_DIR")
                .ok()
                .is_none_or(|value| value.trim().is_empty())
            {
                command.env("SINOMO_DATA_DIR", &receipt.data_dir);
            }
            let status = command.status()?;
            std::process::exit(status.code().unwrap_or(1));
        }
        Some("uninstall") => {
            let root = value(&args, "--install-dir")?;
            let delete_data = args.iter().any(|a| a == "--delete-data");
            uninstall(&root, delete_data)
        }
        Some("--uninstall") => {
            let root = installed_root()?;
            let receipt = read_receipt(&root)?;
            if !confirm(&format!(
                "Uninstall Sinomo from {}? Your data is kept unless you choose deletion next.",
                root.display()
            )) {
                return Ok(());
            }
            let delete = receipt.owns_data
                && confirm(&format!(
                    "Permanently delete ALL Sinomo data in {}? Choose No to keep it.",
                    receipt.data_dir.display()
                ));
            uninstall(&root, delete)?;
            info("Sinomo has been uninstalled.");
            Ok(())
        }
        None => install_gui(&args),
        _ => Err("unknown setup command".into()),
    }
}

fn confirm(message: &str) -> bool {
    MessageDialog::new()
        .set_title("Sinomo setup")
        .set_description(message)
        .set_buttons(MessageButtons::YesNo)
        .show()
        == MessageDialogResult::Yes
}

fn optional_integration(label: &str, current: bool) -> Option<bool> {
    let result = MessageDialog::new()
        .set_title("Sinomo configuration")
        .set_description(format!(
            "{label}\nCurrent setting: {}",
            if current { "enabled" } else { "disabled" }
        ))
        .set_buttons(MessageButtons::YesNoCancelCustom(
            "Keep current".into(),
            "Enable".into(),
            "Disable".into(),
        ))
        .show();
    match result {
        MessageDialogResult::Custom(value) if value == "Enable" => Some(true),
        MessageDialogResult::Custom(value) if value == "Disable" => Some(false),
        _ => None,
    }
}

fn info(message: &str) {
    MessageDialog::new()
        .set_title("Sinomo setup")
        .set_description(message)
        .show();
}

fn installed_root() -> Result<PathBuf> {
    if let Some(root) = env::var_os("SINOMO_INSTALL_DIR") {
        return Ok(root.into());
    }
    let data = sinomo_runtime::local::SinomoDataDir::from_env_or_home()?;
    if let Ok(root) = fs::read_to_string(data.root().join("config/installer-owner")) {
        return Ok(root.into());
    }
    Ok(env::current_exe()?
        .parent()
        .ok_or("no executable directory")?
        .to_path_buf())
}

fn select_folder(title: &str, default: &Path) -> Option<PathBuf> {
    // The native folder chooser permits creating a new folder before selection.
    rfd::FileDialog::new()
        .set_title(title)
        .set_directory(default)
        .pick_folder()
}

fn flag(args: &[String], yes: &str, no: &str) -> Result<Option<bool>> {
    if args.iter().any(|v| v == yes) && args.iter().any(|v| v == no) {
        return Err(format!("choose {yes} or {no}").into());
    }
    Ok(if args.iter().any(|v| v == yes) {
        Some(true)
    } else if args.iter().any(|v| v == no) {
        Some(false)
    } else {
        None
    })
}
fn optional_value(args: &[String], key: &str) -> Option<PathBuf> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone().into())
}
fn selected_paths(args: &[String]) -> Result<(PathBuf, PathBuf)> {
    let (root, data) = resolve_locations(
        optional_value(args, "--install-dir").as_deref(),
        optional_value(args, "--data-dir").as_deref(),
    )?;
    Ok((
        if root.is_absolute() {
            root
        } else {
            env::current_dir()?.join(root)
        },
        if data.is_absolute() {
            data
        } else {
            env::current_dir()?.join(data)
        },
    ))
}
fn install_options(args: &[String]) -> Result<(IntegrationOptions, services::Options)> {
    let integrations = IntegrationOptions {
        add_to_path: flag(args, "--add-to-path", "--no-add-to-path")?
            .or_else(|| args.iter().any(|v| v == "--path").then_some(true)),
        menu_shortcut: flag(args, "--menu-shortcut", "--no-menu-shortcut")?
            .or_else(|| args.iter().any(|v| v == "--menu").then_some(true)),
        desktop_shortcut: flag(args, "--desktop-shortcut", "--no-desktop-shortcut")?,
    };
    Ok((
        integrations,
        services::Options {
            start: !args.iter().any(|v| v == "--no-start"),
            startup: flag(args, "--startup", "--no-startup")?,
            ..services::Options::default()
        },
    ))
}
fn install_command(args: &[String]) -> Result<()> {
    let (root, data) = selected_paths(args)?;
    let (integrations, options) = install_options(args)?;
    apply_install(
        &root,
        &data,
        &integrations,
        &options,
        args.iter().any(|arg| arg == "--repair"),
    )
}
fn apply_install(
    root: &Path,
    data: &Path,
    integrations: &IntegrationOptions,
    options: &services::Options,
    repairing: bool,
) -> Result<()> {
    let exe = env::current_exe()?;
    if !repairing && (root.join(RECEIPT).is_file() || runtime_present(data)) {
        return install(&exe, None, root, data, integrations, options);
    }
    let package = exe.parent().ok_or("no package directory")?;
    #[cfg(target_os = "linux")]
    let (_guard, source) = unpack_payload(package)?;
    #[cfg(not(target_os = "linux"))]
    let source = package.to_path_buf();
    desktop::verify_bundle(&source, &desktop::read_manifest(&source)?)?;
    let setup = if repairing { repair } else { install };
    setup(
        &source.join(binary("sinomo")),
        Some(&source),
        root,
        data,
        integrations,
        options,
    )
}
fn install_gui(args: &[String]) -> Result<()> {
    let (mut root, mut data) = selected_paths(args)?;
    let existing = root.join(RECEIPT).is_file() || runtime_present(&data);
    let mut repairing = args.iter().any(|arg| arg == "--repair");
    if existing && !repairing {
        let choice = MessageDialog::new()
            .set_title("Sinomo maintenance")
            .set_description("Verify/configure the installation, or repair its binaries from this package. Repair stops this node and replaces packaged runtime files while preserving your data. Downgrades are not supported.")
            .set_buttons(MessageButtons::YesNoCancelCustom("Verify/configure".into(), "Repair binaries".into(), "Cancel".into()))
            .show();
        match choice {
            MessageDialogResult::Custom(value) if value == "Repair binaries" => repairing = true,
            MessageDialogResult::Custom(value) if value == "Verify/configure" => {}
            _ => return Ok(()),
        }
    }
    if !existing {
        info(
            "Choose the setup metadata folder and a separate data folder. Program files live in the data folder. Existing installations are verified and reconfigured without replacing evolved components.",
        );
        let Some(selected) = select_folder("Sinomo installation folder", &root) else {
            return Ok(());
        };
        root = selected;
        let Some(selected) = select_folder("Sinomo data folder", &data) else {
            return Ok(());
        };
        data = selected;
    }
    let mut current = if root.join(RECEIPT).is_file() {
        configuration(&root)?
    } else {
        Configuration {
            startup: true,
            ..Configuration::default()
        }
    };
    let (mut integrations, mut options) = install_options(args)?;
    current.add_to_path = integrations.add_to_path.unwrap_or(current.add_to_path);
    current.menu_shortcut = integrations.menu_shortcut.unwrap_or(current.menu_shortcut);
    current.desktop_shortcut = integrations
        .desktop_shortcut
        .unwrap_or(current.desktop_shortcut);
    current.startup = options.startup.unwrap_or(current.startup);
    if confirm(if repairing {
        "Change PATH, startup or shortcut settings before repair? Choose No to preserve the current settings. Repair replaces packaged binaries and retains application data; downgrades are not supported."
    } else if existing {
        "Verify this Sinomo installation and change PATH, startup or shortcut settings? Choose No to verify and preserve the current settings."
    } else {
        "Configure optional PATH, startup and shortcuts? Choose No to use the shared default settings."
    }) {
        integrations.add_to_path = optional_integration(
            "Add the binary directory to user PATH?",
            current.add_to_path,
        )
        .or(integrations.add_to_path);
        options.startup = optional_integration(
            "Start main and shadow automatically at boot/login?",
            current.startup,
        )
        .or(options.startup);
        let desktop_available = data.join("bin").join(binary("sinomo-app")).is_file() || !existing;
        if desktop_available {
            integrations.menu_shortcut = optional_integration(
                "Create a Start Menu/applications-menu shortcut?",
                current.menu_shortcut,
            )
            .or(integrations.menu_shortcut);
            integrations.desktop_shortcut =
                optional_integration("Create a desktop shortcut?", current.desktop_shortcut)
                    .or(integrations.desktop_shortcut);
        }
    }
    apply_install(&root, &data, &integrations, &options, repairing)?;
    info(if repairing {
        "Packaged runtime binaries repaired. Application data and managed source were preserved. Downgrades are not supported."
    } else {
        "Sinomo installation verified. Selected PATH, startup and shortcut settings are applied. Existing component versions and data are preserved."
    });
    Ok(())
}

#[cfg(target_os = "linux")]
fn unpack_payload(
    package: &Path,
) -> Result<(sinomo_runtime::storage::scratch::ScratchPath, PathBuf)> {
    let root = env::temp_dir().join(format!(
        "sinomo-setup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    fs::create_dir(&root)?;
    let guard = sinomo_runtime::storage::scratch::ScratchPath::new(root.clone());
    let mut archive = tar::Archive::new(fs::File::open(package.join("sinomo-payload.tar"))?);
    let mut allowed = payload_files();
    allowed.extend(["initial-payload.tar".into(), binary("sinomo-setup")]);
    let mut seen = std::collections::BTreeSet::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let name = entry.path()?.to_string_lossy().into_owned();
        let library = name.strip_prefix("lib/").is_some_and(|leaf| {
            !leaf.is_empty() && !leaf.contains(['/', '\\']) && leaf != "." && leaf != ".."
        });
        if (!allowed.contains(&name) && !library)
            || !entry.header().entry_type().is_file()
            || !seen.insert(name)
        {
            return Err("invalid installer payload entry".into());
        }
        if !entry.unpack_in(&root)? {
            return Err("payload escaped extraction directory".into());
        }
    }
    if !allowed.iter().all(|name| seen.contains(name)) {
        return Err("incomplete installer payload".into());
    }
    Ok((guard, root))
}
