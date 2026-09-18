//! A separately built module owns a command that the CLI binary never enumerates.
use serde::{Serialize, de::DeserializeOwned};
use sinomo_cli::module_commands::{ModuleCliTransport, compose_module_commands};
use sinomo_runtime::{
    api::cli::*,
    cli_registry::{CliCommandContext, CliCommandRegistry},
    local::SinomoDataDir,
    module::*,
    network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse, ObjectRpcRequest},
    service::{
        ResponseSink, ServiceContext, ServiceDisposition, ServiceError, ServiceRegistry,
        ServiceRegistrySnapshotStore, ServiceSigner,
    },
};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Sink(Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>);
impl ResponseSink for Sink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        *self.0.lock().unwrap() = Some(response);
        Ok(())
    }
}
struct Transport {
    services: ServiceRegistrySnapshotStore,
    data_dir: Arc<SinomoDataDir>,
    runtime: tokio::runtime::Runtime,
}
impl Transport {
    fn rpc<Q: Serialize, R: DeserializeOwned>(&self, route: &str, body: &Q) -> Result<R, String> {
        self.runtime.block_on(self.rpc_async(route, body))
    }
    async fn rpc_async<Q: Serialize, R: DeserializeOwned>(
        &self,
        route: &str,
        body: &Q,
    ) -> Result<R, String> {
        let request = ObjectRpcRequest::encode_body(route, CLI_RPC_CATEGORY, body)
            .map_err(|e| e.to_string())?;
        let response = Arc::new(Mutex::new(None));
        let context = ServiceContext::new(
            Arc::clone(&self.data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(Sink(Arc::clone(&response))),
        );
        match self
            .services
            .active_snapshot()
            .registry()
            .dispatch(context, request)
            .await
            .map_err(|e| e.to_string())?
        {
            ServiceDisposition::Continue => (),
            ServiceDisposition::Stop => return Err("unexpected host stop".into()),
        }
        let outgoing = response.lock().unwrap().take().ok_or("missing response")?;
        outgoing
            .into_materialized_value_response(CLI_MAX_OUTPUT_BYTES)
            .await
            .map_err(|e| e.to_string())?
            .decode_body()
            .map_err(|e| e.to_string())
    }
}
impl ModuleCliTransport for Transport {
    fn catalog(&self) -> Result<CliCatalogResponse, String> {
        self.rpc(CLI_CATALOG_ROUTE, &CliCatalogRequest)
    }
    fn invoke(&self, request: CliInvokeRequest, _: Duration) -> Result<CliInvokeResponse, String> {
        self.rpc(CLI_INVOKE_ROUTE, &request)
    }
}

fn fixture_library(workspace: &Path) -> PathBuf {
    let target = workspace.join("target/external-component-fixture");
    assert!(
        Command::new(env!("CARGO"))
            .args(["build", "--manifest-path"])
            .arg(workspace.join("fixtures/external-component/Cargo.toml"))
            .env("CARGO_TARGET_DIR", &target)
            .status()
            .unwrap()
            .success()
    );
    let file = if cfg!(windows) {
        "sinomo_external_component_fixture.dll"
    } else if cfg!(target_os = "macos") {
        "libsinomo_external_component_fixture.dylib"
    } else {
        "libsinomo_external_component_fixture.so"
    };
    target.join("debug").join(file)
}

#[test]
fn separately_built_command_is_discovered_parsed_executed_and_withdrawn() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let library = fixture_library(workspace);
    let requirement = sinomo_runtime::module_api::ServiceRequirement::new(
        "sinomo.fixture.external-echo",
        "1.0.0",
    )
    .from_service("external-echo");
    let loaded = unsafe { ActiveNativeComponent::load_verified(&library, &requirement) }.unwrap();
    let descriptor = loaded.descriptor().clone();
    assert!(loaded.unload().shutdown_ok);
    let root = sinomo_testdir::TestDir::new("cli-runtime-command");
    let data_dir = Arc::new(SinomoDataDir::new(&root));
    data_dir.ensure_root_layout().unwrap();
    let bytes = std::fs::read(&library).unwrap();
    let hash = native_module_content_hash(&bytes);
    let library_name = library.file_name().unwrap().to_string_lossy().into_owned();
    let cache_key = "fixture/cli";
    let cached = native_module_cache_path(root.path(), cache_key, &hash, &library_name);
    std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
    std::fs::write(&cached, bytes).unwrap();
    let state = NativeDylibModuleLoaderState {
        module_id: ModuleId::from_legacy_named(&descriptor.module_id).unwrap(),
        artifact_id: None,
        content_hash: hash,
        replaces_content_hash: None,
        cache_key: cache_key.into(),
        library_name,
        platform_triple: descriptor.platform_triple.clone(),
        native_abi_version: descriptor.native_abi_version,
        interface_version: descriptor.interface_version.clone(),
        activated_at: module_loader_applied_at_now(),
        source: "CLI integration fixture".into(),
    };
    write_native_dylib_module_loader_state(root.path(), &state).unwrap();
    let registration =
        native_dylib_module_registration_from_loader_state(root.path(), &state).unwrap();
    for invalid in [
        "alias-collision",
        "reserved",
        "missing-service",
        "completion-mismatch",
    ] {
        let mut candidate = registration.clone();
        let mut command: CliCommandDescriptor = sinomo_runtime::module_api::decode_sobj(
            &candidate.surfaces.extension_contributions[0].payload_sobj,
        )
        .unwrap();
        match invalid {
            "alias-collision" => {
                command.command_id = "fixture.second-command".into();
                command.name = "second-command".into();
                command.autocomplete_sobj.clear();
                candidate
                    .surfaces
                    .extension_contributions
                    .push(command.contribution().unwrap());
            }
            "reserved" => {
                command.name = "daemon".into();
                candidate.surfaces.extension_contributions[0].payload_sobj =
                    sinomo_runtime::module_api::encode_sobj(&command).unwrap();
            }
            "missing-service" => {
                command.service_id = "not-owned-by-this-module".into();
                candidate.surfaces.extension_contributions[0] = command.contribution().unwrap();
            }
            _ => {
                command.name = "mismatched-completion".into();
                candidate.surfaces.extension_contributions[0] = command.contribution().unwrap();
            }
        }
        let candidate_store = ModuleRegistrySnapshotStore::default();
        candidate_store.register(candidate).unwrap();
        assert!(
            compile_default_extension_registry(&candidate_store.active_snapshot().unwrap())
                .is_err(),
            "{invalid} must be rejected before publication"
        );
    }
    let snapshots = ModuleRegistrySnapshotStore::default();
    snapshots.register(registration.clone()).unwrap();
    let materialized = materialize_module_service_snapshot_with_extensions(
        ServiceRegistry::default(),
        module_service_snapshot(&snapshots).unwrap(),
        root.path().to_path_buf(),
        Arc::new(PassthroughNativeModuleRequestPreparer),
        NativeModuleServiceMaterialization::new("fixture"),
    )
    .unwrap();
    let host = materialized.runtime.clone();
    let transport = Arc::new(Transport {
        services: ServiceRegistrySnapshotStore::new(materialized.registry),
        data_dir,
        runtime: tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    });
    let registry = compose_module_commands(CliCommandRegistry::new(), transport.clone()).unwrap();
    assert!(
        compose_module_commands(registry.clone(), transport.clone()).is_err(),
        "duplicate roots and aliases must fail registration"
    );
    assert!(
        registry
            .metadata()
            .iter()
            .any(|command| command.name == "fixture-cli")
    );
    assert!(
        registry
            .autocomplete_specs()
            .iter()
            .any(|spec| spec.word == "fixture-cli"
                && spec.children.iter().any(|child| child.word == "echo"))
    );
    let run = |args: &[&str]| {
        let command = registry
            .parse(&args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = command.run(&mut CliCommandContext::new(&mut stdout, &mut stderr));
        (result, stdout, stderr)
    };
    let (result, stdout, _) = run(&["fx", "echo", "two words", "--", "--local"]);
    result.unwrap();
    assert_eq!(stdout, b"two words|--local");
    let (result, stdout, _) = run(&["fx", "echo", "", "雪"]);
    result.unwrap();
    assert_eq!(stdout, "|雪".as_bytes());
    let (result, stdout, _) = run(&["fixture-cli", "--help"]);
    result.unwrap();
    assert!(String::from_utf8(stdout).unwrap().contains("echo"));
    let (result, _, stderr) = run(&["fixture-cli", "fail"]);
    assert_eq!(result.unwrap_err().exit_code(), 7);
    assert!(
        String::from_utf8(stderr)
            .unwrap()
            .contains("fixture failure")
    );
    assert_eq!(
        run(&["fixture-cli", "unknown"]).0.unwrap_err().exit_code(),
        2
    );
    assert_eq!(
        run(&["fixture-cli"]).0.unwrap_err().exit_code(),
        7,
        "the module owns the empty-argument behavior"
    );
    let catalog = transport.catalog().unwrap();
    let entry = &catalog.commands[0];
    let invoke = |action, args, stdin| CliInvokeRequest {
        generation: catalog.generation,
        catalog_id: catalog.catalog_id.clone(),
        module_id: entry.module_id.clone(),
        request: CliModuleRequest {
            command_id: entry.command.command_id.clone(),
            invocation_id: sinomo_runtime::core::Id::generate().to_string(),
            action,
            args,
            stdin,
            current_dir: None,
        },
    };
    let result = transport
        .invoke(
            invoke(CliAction::Complete, vec!["e".into()], Vec::new()),
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(
        matches!(result, CliInvokeResponse::Completed(ref result) if result.completions == ["echo"])
    );
    let result = transport
        .invoke(
            invoke(
                CliAction::Invoke,
                vec!["echo".into()],
                b"piped input".to_vec(),
            ),
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(
        matches!(result, CliInvokeResponse::Completed(ref result) if result.stdout == b"piped input")
    );
    let mut previous_host = invoke(CliAction::Invoke, vec!["echo".into()], Vec::new());
    previous_host.catalog_id = "previous-daemon-incarnation".into();
    assert_eq!(
        transport
            .invoke(previous_host, Duration::from_secs(5))
            .unwrap(),
        CliInvokeResponse::CatalogChanged
    );

    // The artifact identity is pinned even if loader state changes before publication.
    let mut changed_state = state.clone();
    changed_state.content_hash = "changed-before-publication".into();
    write_native_dylib_module_loader_state(root.path(), &changed_state).unwrap();
    let result = transport
        .invoke(
            invoke(CliAction::Invoke, vec!["echo".into()], Vec::new()),
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(
        matches!(result, CliInvokeResponse::ExecutionFailed { ref detail } if detail.contains("changed")),
        "{result:?}"
    );
    write_native_dylib_module_loader_state(root.path(), &state).unwrap();

    transport.runtime.block_on(async {
        let wait_until_started = || async {
            tokio::time::timeout(Duration::from_secs(3), async {
                while host
                    .invocations
                    .status(&entry.module_id)
                    .unwrap()
                    .active_invocations
                    < 3
                {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("native invocation started");
        };
        let request = invoke(CliAction::Invoke, vec!["wait".into()], Vec::new());
        let id = request.request.invocation_id.clone();
        let task_transport = Arc::clone(&transport);
        let task = tokio::spawn(async move {
            task_transport
                .rpc_async::<_, CliInvokeResponse>(CLI_INVOKE_ROUTE, &request)
                .await
        });
        wait_until_started().await;
        let cancelled = host
            .native_operations
            .cancel_request(
                None,
                sinomo_runtime::network::ObjectRpcCancelRequest {
                    operation_id: id,
                    reason: "fixture cancellation".into(),
                },
            )
            .unwrap();
        assert!(cancelled.cancelled);
        let result = task.await.unwrap().unwrap();
        assert!(
            matches!(result, CliInvokeResponse::Completed(ref result) if result.exit_code == 130)
        );

        let request = invoke(CliAction::Invoke, vec!["wait".into()], Vec::new());
        let task_transport = Arc::clone(&transport);
        let task = tokio::spawn(async move {
            task_transport
                .rpc_async::<_, CliInvokeResponse>(CLI_INVOKE_ROUTE, &request)
                .await
        });
        wait_until_started().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        host.invocations.begin_drain(&entry.module_id).unwrap();
        assert!(
            host.invocations.complete_unload(&entry.module_id).is_err(),
            "blocking native cleanup must keep unload leased"
        );
        assert!(
            host.invocations
                .wait_for_drain(&entry.module_id, Duration::from_secs(3))
                .await
                .unwrap()
                .drained
        );
        host.invocations.reactivate(&entry.module_id).unwrap();
    });

    // The registry generation changes without replacing the CLI code or its transport.
    let empty = snapshots
        .replace_registry(ModuleRegistry::default())
        .unwrap();
    let withdrawn = materialize_module_service_snapshot_for_runtime(
        ServiceRegistry::default(),
        module_service_snapshot(&snapshots).unwrap(),
        root.path().to_path_buf(),
        Arc::new(PassthroughNativeModuleRequestPreparer),
        NativeModuleServiceMaterialization::new("fixture"),
        host.clone(),
    )
    .unwrap();
    let services = transport
        .services
        .replace_registry(withdrawn.registry)
        .unwrap();
    host.generations
        .publish(empty, withdrawn.extension_registry, services.version())
        .unwrap();
    assert!(transport.catalog().unwrap().commands.is_empty());
    host.invocations.begin_drain(&entry.module_id).unwrap();
    assert_eq!(
        host.invocations
            .status(&entry.module_id)
            .unwrap()
            .active_invocations,
        0
    );
    host.invocations.complete_unload(&entry.module_id).unwrap();
    assert!(matches!(
        transport
            .invoke(
                invoke(CliAction::Invoke, vec!["echo".into()], Vec::new()),
                Duration::from_secs(5)
            )
            .unwrap(),
        CliInvokeResponse::CatalogChanged
    ));
    let refreshed = compose_module_commands(CliCommandRegistry::new(), transport.clone()).unwrap();
    assert!(
        refreshed
            .parse(&["fixture-cli".into(), "echo".into()])
            .unwrap()
            .is_none()
    );

    snapshots.register(registration).unwrap();
    host.invocations.reactivate(&entry.module_id).unwrap();
    let reloaded = materialize_module_service_snapshot_for_runtime(
        ServiceRegistry::default(),
        module_service_snapshot(&snapshots).unwrap(),
        root.path().to_path_buf(),
        Arc::new(PassthroughNativeModuleRequestPreparer),
        NativeModuleServiceMaterialization::new("fixture"),
        host.clone(),
    )
    .unwrap();
    let services = transport
        .services
        .replace_registry(reloaded.registry)
        .unwrap();
    host.generations
        .publish(
            snapshots.active_snapshot().unwrap(),
            reloaded.extension_registry,
            services.version(),
        )
        .unwrap();
    let refreshed = compose_module_commands(CliCommandRegistry::new(), transport.clone()).unwrap();
    let command = refreshed
        .parse(&["fx".into(), "echo".into(), "reloaded".into()])
        .unwrap()
        .unwrap();
    let mut stdout = Vec::new();
    command
        .run(&mut CliCommandContext::new(&mut stdout, &mut Vec::new()))
        .unwrap();
    assert_eq!(stdout, b"reloaded");
    assert_eq!(
        transport
            .invoke(
                invoke(CliAction::Invoke, vec!["echo".into()], Vec::new()),
                Duration::from_secs(5)
            )
            .unwrap(),
        CliInvokeResponse::CatalogChanged
    );
}
