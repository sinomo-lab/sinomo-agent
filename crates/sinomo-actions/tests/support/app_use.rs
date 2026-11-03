use sinomo_runtime::device::desktop::protocol::*;
use sinomo_runtime::module::*;
use std::path::{Path, PathBuf};

pub fn install(root: &Path) -> NativeDylibModuleInstallResult {
    let library = std::env::var_os("SINOMO_APP_USE_MODULE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/debug")
                .join(format!(
                    "{}sinomo_app_use_module{}",
                    std::env::consts::DLL_PREFIX,
                    std::env::consts::DLL_SUFFIX
                ))
        });
    install_native_dylib_module_from_file(
        root,
        &library,
        NativeDylibModuleInstallRequest {
            expected_module_id: Some(ModuleId::from_legacy_named(APP_USE_OS_MODULE_ID).unwrap()),
            artifact_id: None,
            expected_interface_version: Some("sinomo.app-use.os.v2".into()),
            cache_key: Some(APP_USE_OS_CACHE_KEY.into()),
            library_name: None,
            source: "native app-use acceptance artifact".into(),
        },
    )
    .expect("build sinomo-app-use-module first or set SINOMO_APP_USE_MODULE")
}
