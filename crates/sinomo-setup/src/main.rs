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
        Some("configure") => {
            let root = value(&args, "--install-dir")?;
            let data = value(&args, "--data-dir")?;
            configure(&root, &data)?;
            #[cfg(windows)]
            windows_integration(&root, "install", &args)?;
            complete_services(
                &root,
                &services::Options {
                    start: !args.iter().any(|arg| arg == "--no-start"),
                    ..services::Options::default()
                },
            )?;
            Ok(())
        }
        Some("uninstall") => {
            let root = value(&args, "--install-dir")?;
            let delete_data = args.iter().any(|a| a == "--delete-data");
            #[cfg(windows)]
            {
                uninstall_nsis(&root, delete_data)
            }
            #[cfg(not(windows))]
            {
                uninstall(&root, delete_data)
            }
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
        None => install_gui(),
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

fn optional_integration(message: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        // Default/close means no optional integration, matching CLI and NSIS defaults.
        MessageDialog::new()
            .set_title("Sinomo setup")
            .set_description(message)
            .set_buttons(MessageButtons::YesNoCancelCustom(
                "Skip".into(),
                "Enable".into(),
                "Cancel".into(),
            ))
            .show()
            == MessageDialogResult::Custom("Enable".into())
    }
    #[cfg(not(target_os = "linux"))]
    {
        confirm(message)
    }
}

fn info(message: &str) {
    MessageDialog::new()
        .set_title("Sinomo setup")
        .set_description(message)
        .show();
}

fn installed_root() -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    if let Some(appimage) = env::var_os("APPIMAGE") {
        return Ok(PathBuf::from(appimage)
            .parent()
            .ok_or("no AppImage directory")?
            .to_path_buf());
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

fn install_gui() -> Result<()> {
    let home = home()?;
    info(
        "Select or create an EMPTY folder for the Sinomo CLI and app, then select or create a separate data folder. Cancel either chooser to exit.",
    );
    let Some(root) = select_folder("Sinomo installation folder (must be empty)", &home) else {
        return Ok(());
    };
    let Some(data) = select_folder("Sinomo data folder", &home) else {
        return Ok(());
    };
    validate_locations(&root, &data)?;
    if fs::read_dir(&root)?.next().is_some() {
        return Err("Choose an empty installation folder. Existing installations must be managed through Sinomo rollout, not replaced by setup.".into());
    }
    let path = optional_integration(
        "Add the CLI installation directory to PATH for future login sessions?",
    );
    let menu = optional_integration("Add Sinomo to the applications menu?");
    let desktop = optional_integration("Add a Sinomo desktop shortcut?");
    if !confirm(&format!(
        "Install Sinomo in {} using data in {}?",
        root.display(),
        data.display()
    )) {
        return Ok(());
    }
    let exe = env::current_exe()?;
    let package = exe.parent().ok_or("no package directory")?;
    #[cfg(target_os = "linux")]
    let (_payload_guard, unpacked) = unpack_payload(package)?;
    #[cfg(target_os = "linux")]
    let source = unpacked.as_path();
    #[cfg(not(target_os = "linux"))]
    let source = package;
    desktop::verify_bundle(source, &desktop::read_manifest(source)?)?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::PermissionsExt;
        let image = env::var_os("APPIMAGE").ok_or(
            "Run the packaged Sinomo installer AppImage to install its bundled runtime libraries.",
        )?;
        fs::copy(image, root.join("Sinomo.AppImage"))?;
        fs::set_permissions(
            root.join("Sinomo.AppImage"),
            fs::Permissions::from_mode(0o755),
        )?;
        for (name, argument) in [
            ("sinomo-app", "--launch-app"),
            ("sinomo-setup", "--uninstall"),
        ] {
            fs::write(
                root.join(name),
                format!(
                    "#!/bin/sh\nexec {} {} \"$@\"\n",
                    shell_quote(root.join("Sinomo.AppImage").to_str().unwrap()),
                    argument
                ),
            )?;
            fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o755))?;
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        for name in payload_files() {
            fs::copy(source.join(&name), root.join(&name))?;
        }
        fs::copy(&exe, root.join(binary("sinomo-setup")))?;
    }
    #[cfg(target_os = "linux")]
    fs::copy(source.join("sinomo"), root.join("sinomo"))?;
    configure_from(&root, &data, source)?;
    #[cfg(target_os = "linux")]
    linux_integration(&root, path, menu, desktop)?;
    #[cfg(not(target_os = "linux"))]
    let _ = (path, menu, desktop);
    complete_services(&root, &services::Options::default())?;
    info(
        "Sinomo is installed and its per-user daemon services are running. Open sinomo-app from the installation folder or your chosen shortcuts. PATH changes apply to future login sessions. Use Uninstall Sinomo in the applications menu to remove it.",
    );
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
    let allowed = payload_files();
    let mut seen = std::collections::BTreeSet::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let name = entry.path()?.to_string_lossy().into_owned();
        if !allowed.contains(&name) || !entry.header().entry_type().is_file() || !seen.insert(name)
        {
            return Err("invalid installer payload entry".into());
        }
        if !entry.unpack_in(&root)? {
            return Err("payload escaped extraction directory".into());
        }
    }
    if seen.len() != allowed.len() {
        return Err("incomplete installer payload".into());
    }
    Ok((guard, root))
}
