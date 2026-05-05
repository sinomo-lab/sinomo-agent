use super::*;

pub(crate) fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstallLayout {
    pub(crate) data_dir: PathBuf,
    pub(crate) bin_dir: PathBuf,
    pub(crate) binary: PathBuf,
    pub(crate) shadow_binary: PathBuf,
    pub(crate) boot_dir: PathBuf,
    pub(crate) main_boot_entry: PathBuf,
    pub(crate) shadow_boot_entry: PathBuf,
}

impl InstallLayout {
    pub(crate) fn new(data_dir: &SinomoDataDir, boot_dir: PathBuf) -> Self {
        let data_root = data_dir.root();
        let bin_dir = data_root.join("bin");
        let binary = bin_dir.join(format!("sinomo{}", env::consts::EXE_SUFFIX));
        let shadow_binary = bin_dir.join(format!("sinomo-shadow{}", env::consts::EXE_SUFFIX));
        let main_boot_entry = boot_entry_path(&boot_dir, INSTALL_MAIN_BOOT_NAME);
        let shadow_boot_entry = boot_entry_path(&boot_dir, INSTALL_SHADOW_BOOT_NAME);
        Self {
            data_dir: data_root,
            bin_dir,
            binary,
            shadow_binary,
            boot_dir,
            main_boot_entry,
            shadow_boot_entry,
        }
    }
}
