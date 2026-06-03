use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sinomo_module::{ModuleId, NativeDylibModuleLoaderState};
use sinomo_sandbox::{
    LocalSandboxInstanceStore, LocalSandboxToolchainStore, SandboxId, SandboxToolchainId,
};
use sinomo_sandbox_wasix_api::{
    SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, SINOMO_SANDBOX_WASIX_MODULE_ID,
    SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION,
};
use sinomo_sandbox_wasix_host::SandboxWasixModuleCommandRunner;
use time::OffsetDateTime;

#[test]
fn active_native_module_runs_preview1_module_when_env_set() {
    let Ok(module_path) = std::env::var("SINOMO_WASIX_NATIVE_MODULE_PATH") else {
        eprintln!("skipping: SINOMO_WASIX_NATIVE_MODULE_PATH is not set");
        return;
    };
    let module_path = PathBuf::from(module_path);
    let root = unique_temp("sinomo-wasix-host-loader");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create temp root");

    install_active_wasix_module(&root, &module_path);

    let runner =
        SandboxWasixModuleCommandRunner::from_active_module(&root).expect("load active module");
    let store = LocalSandboxInstanceStore::new(root.join("sandbox").join("runtime"));
    let toolchain_store = LocalSandboxToolchainStore::new(root.join("sandbox").join("cache"));
    let sandbox_id = SandboxId::generate();
    store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::generate(),
            OffsetDateTime::now_utc(),
        )
        .expect("create sandbox");
    let wasm = wat::parse_str(
        r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1 2)
          (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
          (func (export "_start")
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            drop))
        "#,
    )
    .expect("compile wat");

    let outcome = runner
        .run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
            &store,
            &toolchain_store,
            &sandbox_id,
            wasm,
            vec!["hello.wasm".to_string()],
            None,
            5_000,
            None,
            Vec::new(),
            None,
        )
        .expect("run wasm through active module");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    let _ = fs::remove_dir_all(root);
}

fn install_active_wasix_module(data_dir_root: &Path, module_path: &Path) {
    let bytes = fs::read(module_path).expect("read WASIX native module");
    let content_hash = sinomo_module::native_module_content_hash(&bytes);
    let library_name = module_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("module library file name")
        .to_string();
    let cache_key = "module/sandbox/wasix";
    let cache_path = sinomo_module::native_module_cache_path(
        data_dir_root,
        cache_key,
        &content_hash,
        &library_name,
    );
    fs::create_dir_all(cache_path.parent().expect("cache parent")).expect("create cache dir");
    fs::write(&cache_path, &bytes).expect("write cached module");

    let module = unsafe {
        sinomo_sandbox_wasix_host::SandboxWasixNativeModule::load_from_content_addressed_path(
            &cache_path,
            &content_hash,
        )
    }
    .expect("load cached module");
    let descriptor = module.descriptor().clone();
    drop(module);

    let module_id = ModuleId::from_legacy_named(SINOMO_SANDBOX_WASIX_MODULE_ID).expect("module id");
    sinomo_module::write_native_dylib_module_loader_state(
        data_dir_root,
        &NativeDylibModuleLoaderState {
            module_id,
            artifact_id: None,
            content_hash,
            cache_key: cache_key.to_string(),
            library_name,
            platform_triple: descriptor.platform_triple,
            native_abi_version: SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION,
            interface_version: SINOMO_SANDBOX_WASIX_INTERFACE_VERSION.to_string(),
            activated_at: OffsetDateTime::now_utc().to_string(),
            source: format!("env:{}", module_path.display()),
        },
    )
    .expect("write loader state");
}

fn unique_temp(prefix: &str) -> PathBuf {
    std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("test-tmp")
        .join(format!(
            "{prefix}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
}
