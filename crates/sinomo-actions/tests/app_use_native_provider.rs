//! Exercises an actual independently built artifact without desktop input.
#[path = "support/app_use.rs"]
mod support;
use sinomo_actions::desktop::provider::DesktopProviderClient;
use sinomo_runtime::device::desktop::{protocol::*, *};
use sinomo_runtime::module::*;
use sinomo_runtime::node::{LocalNodePermissionStore, LocalNodePermissions};
use std::sync::Arc;

#[test]
#[ignore = "build sinomo-app-use-module first; loads and unloads its actual artifact"]
fn native_provider_authority_removal_and_reload() {
    let root = sinomo_testdir::TestDir::new("app-use-native-lifecycle");
    for _ in 0..2 {
        let installed = support::install(&root);
        let store = LocalNodePermissionStore::new(&root);
        let mut grants = LocalNodePermissions::new();
        grants.set_remote_management_enabled(true);
        store.save(&grants).unwrap();
        let requirement = ServiceRequirement::new(APP_USE_OS_INTERFACE, APP_USE_OS_VERSION)
            .from_service(APP_USE_OS_SERVICE);
        let unbound = unsafe { ActiveNativeComponent::resolve(&root, &requirement) }.unwrap();
        let response = unbound
            .invoke_sobj::<_, DesktopProviderResponse>(
                APP_USE_OS_OPERATION,
                &DesktopProviderRequest {
                    permission_store: LocalNodePermissionStore::new(&root)
                        .path()
                        .to_string_lossy()
                        .into_owned(),
                    command: DesktopProviderCommand::Targets {
                        mode: DesktopControlMode::Raw,
                    },
                },
                None,
            )
            .unwrap();
        assert!(matches!(
            response,
            ActiveNativeComponentResponse::Value(DesktopProviderResponse::Failed {
                error: DesktopError::PermissionDenied(_)
            })
        ));
        assert!(unbound.unload().shutdown_ok);

        grants.set_remote_management_enabled(false);
        store.save(&grants).unwrap();

        let provider = DesktopProviderClient::resolve(&root).unwrap();
        let video_client = DesktopProviderClient::resolve(&root).unwrap();
        assert!(
            Arc::ptr_eq(&provider, &video_client),
            "App Use and video must share the callback owner"
        );
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let client = video_client.clone();
                scope.spawn(move || {
                    for _ in 0..8 {
                        assert_eq!(
                            client.capabilities().unwrap().platform,
                            std::env::consts::OS
                        );
                    }
                });
            }
        });
        drop(video_client);
        let capabilities = provider.capabilities().unwrap();
        assert_eq!(capabilities.platform, std::env::consts::OS);
        assert!(matches!(
            provider.targets(DesktopControlMode::Background),
            Err(DesktopError::PermissionDenied(_))
        ));
        let raw_target = DesktopTarget::Monitor {
            device_name: "never injected".into(),
        };
        assert!(matches!(
            provider.open_input(raw_target.clone(), DesktopControlMode::Background, 1000),
            Err(DesktopError::Invalid(_))
        ));
        assert!(matches!(
            provider.open_input(raw_target, DesktopControlMode::Raw, 1000),
            Err(DesktopError::PermissionDenied(_))
        ));
        remove_native_dylib_module_loader_state(&root, &installed.state.module_id).unwrap();
        assert!(matches!(
            provider.capabilities(),
            Err(DesktopError::Closed(_))
        ));
        let report = Arc::try_unwrap(provider).ok().unwrap().unload();
        assert!(
            report.shutdown_ok && report.library_handle_released,
            "{report:?}"
        );
    }
}
