use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use sinomo_core::local::SinomoDataDir;
use sinomo_core::service::ServiceRegistry;
use sinomo_module::{
    ActiveNativeComponent, ActiveNativeComponentResponse, ComponentContext, ComponentFactory,
    ComponentReconciler, NativeDylibModuleLoaderState, StaticComponentFactory,
    module_loader_applied_at_now, native_module_cache_path, native_module_content_hash,
    write_native_dylib_module_loader_state,
};
use sinomo_module_api::ServiceRequirement;

#[test]
fn independently_built_component_loads_and_reconciles_without_a_core_dependency() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let fixture = workspace.join("fixtures/external-component/Cargo.toml");
    let target = workspace.join("target/external-component-fixture");
    let status = Command::new("cargo")
        .args(["build", "--manifest-path"])
        .arg(&fixture)
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .expect("build external component fixture");
    assert!(status.success(), "external fixture build failed");

    let library = fixture_library(&target);
    let requirement = ServiceRequirement::new("sinomo.fixture.external-echo", "1.0.0")
        .from_service("external-echo");
    let component = unsafe { ActiveNativeComponent::load_verified(&library, &requirement) }
        .expect("bind external component");
    let descriptor = component.descriptor().clone();
    assert_eq!(descriptor.module_id, "fixture.external.echo");
    descriptor
        .component
        .validate()
        .expect("external component descriptor");
    assert_eq!(descriptor.component.offers.len(), 1);
    assert!(component.unload().shutdown_ok);

    let data_root = sinomo_testdir::TestDir::new("external-component-active-binding");
    let bytes = fs::read(&library).expect("read external component");
    let content_hash = native_module_content_hash(&bytes);
    let library_name = library
        .file_name()
        .and_then(|name| name.to_str())
        .expect("fixture library name")
        .to_string();
    let cache_key = "fixture/external-echo";
    let cache_path = native_module_cache_path(&data_root, cache_key, &content_hash, &library_name);
    fs::create_dir_all(cache_path.parent().expect("cache parent")).expect("create cache");
    fs::write(&cache_path, bytes).expect("write cached component");
    write_native_dylib_module_loader_state(
        &data_root,
        &NativeDylibModuleLoaderState {
            module_id: sinomo_module::ModuleId::from_legacy_named(&descriptor.module_id)
                .expect("fixture module id"),
            artifact_id: None,
            content_hash,
            replaces_content_hash: None,
            cache_key: cache_key.to_string(),
            library_name,
            platform_triple: descriptor.platform_triple.clone(),
            native_abi_version: descriptor.native_abi_version,
            interface_version: descriptor.interface_version.clone(),
            activated_at: module_loader_applied_at_now(),
            source: "external-fixture-test".to_string(),
        },
    )
    .expect("write active component state");
    let component = unsafe { ActiveNativeComponent::resolve(&data_root, &requirement) }
        .expect("resolve active external component");
    let response = component
        .invoke_sobj::<_, Vec<u8>>("fixture.external.echo", &vec![1_u8, 2, 3, 4], None)
        .expect("invoke external fixture");
    let ActiveNativeComponentResponse::Value(response) = response else {
        panic!("external fixture returned {response:?}");
    };
    assert_eq!(response, [1, 2, 3, 4]);
    assert!(component.unload().shutdown_ok);

    let factory: Arc<dyn ComponentFactory> = Arc::new(
        StaticComponentFactory::new(descriptor.component, |_, _| Ok(()))
            .expect("external descriptor factory"),
    );
    let data_dir = Arc::new(SinomoDataDir::new(data_root.path()));
    let mut reconciler =
        ComponentReconciler::new(ComponentContext::new(data_dir), ServiceRegistry::default());
    let report = reconciler
        .reconcile(&[factory])
        .expect("reconcile external component");
    assert_eq!(report.active, ["fixture.external.echo"]);
}

fn fixture_library(target: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    let filename = "sinomo_external_component_fixture.dll";
    #[cfg(target_os = "macos")]
    let filename = "libsinomo_external_component_fixture.dylib";
    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    let filename = "libsinomo_external_component_fixture.so";
    target.join("debug").join(filename)
}
