use std::path::PathBuf;
use std::sync::Arc;

const FIXTURES_ENV: &str = "SINOMO_NATIVE_HOT_UNLOAD_FIXTURES";

#[test]
fn native_dylib_fixtures_load_unload_and_reload_in_one_process() {
    let Some(fixtures) = std::env::var_os(FIXTURES_ENV) else {
        return;
    };
    let fixtures = fixtures.to_string_lossy();
    for fixture in fixtures
        .split(';')
        .map(str::trim)
        .filter(|fixture| !fixture.is_empty())
    {
        let (expected_module_id, path) = fixture
            .split_once('=')
            .unwrap_or_else(|| panic!("invalid {FIXTURES_ENV} entry `{fixture}`"));
        let path = PathBuf::from(path);
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read native fixture {}: {error}", path.display()));
        let content_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());

        for cycle in 1..=2 {
            let module = unsafe {
                sinomo_module::NativeModule::load_from_content_addressed_path(&path, &content_hash)
            }
            .unwrap_or_else(|error| {
                panic!("load native fixture `{expected_module_id}` cycle {cycle}: {error}")
            });
            assert_eq!(module.descriptor().module_id, expected_module_id);
            let report = module.unload();
            assert!(report.shutdown_called, "{expected_module_id}: {report:?}");
            assert!(report.shutdown_ok, "{expected_module_id}: {report:?}");
            assert!(
                report.library_handle_released,
                "{expected_module_id}: {report:?}"
            );
        }

        if expected_module_id == "module.network-iroh" {
            verify_network_module_entry_hot_unload(&path, &content_hash);
        }
    }
}

fn verify_network_module_entry_hot_unload(path: &PathBuf, content_hash: &str) {
    for cycle in 1..=2 {
        let library = unsafe {
            sinomo_module::NativeLibrary::load_from_content_addressed_path(path, content_hash)
        }
        .unwrap_or_else(|error| panic!("load network fixture cycle {cycle}: {error}"));
        let entry = unsafe {
            library
                .symbol::<sinomo_core::network::network_module_abi::SinomoNetworkModuleCreateV1Fn>(
                    sinomo_core::network::network_module_abi::SINOMO_NETWORK_MODULE_ENTRY_SYMBOL,
                )
        }
        .unwrap_or_else(|error| panic!("resolve network entry cycle {cycle}: {error}"));
        let mut instance = std::ptr::null_mut();
        let mut vtable =
            sinomo_core::network::network_module_abi::SinomoNetworkModuleVtableV1::empty();
        let status = unsafe { entry(&mut instance, &mut vtable) };
        assert_eq!(
            status,
            sinomo_core::network::network_module_abi::SINOMO_NETWORK_MODULE_STATUS_OK
        );
        let module = unsafe {
            sinomo_core::network::FfiNetworkModule::from_entry_parts(
                instance,
                vtable,
                Box::new(library),
            )
        }
        .unwrap_or_else(|error| panic!("adapt network fixture cycle {cycle}: {error}"));
        let registry = sinomo_core::network::NetworkModuleRegistry::new();
        registry.register(Arc::new(module), None, true, false);
        let report = registry
            .unregister("iroh", std::time::Duration::from_secs(10))
            .unwrap_or_else(|error| panic!("unregister network fixture cycle {cycle}: {error}"));
        assert!(report.object_released, "cycle {cycle}: {report:?}");
    }
}
