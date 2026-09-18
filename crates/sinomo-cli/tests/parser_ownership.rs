//! Domain crates register and parse commands without the CLI's composition or runners.
use sinomo_runtime::cli_registry::{CliCommandContext, CliCommandError, CliCommandRegistry};

fn unexpected_execution<T>(_: T) -> Result<(), CliCommandError> {
    panic!("parsing and help must not invoke the execution adapter")
}

fn owner_registry() -> CliCommandRegistry {
    let registry =
        sinomo_memory::cli::register_cli_commands(CliCommandRegistry::new(), unexpected_execution)
            .unwrap();
    let registry = sinomo_agent::cli::register_cli_commands(
        registry,
        unexpected_execution,
        unexpected_execution,
    )
    .unwrap();
    let registry = sinomo_lm::cli::register_cli_commands(
        registry,
        unexpected_execution,
        unexpected_execution,
        |_, _| panic!("model help must not invoke the host adapter"),
        unexpected_execution,
    )
    .unwrap();
    let registry =
        sinomo_runtime::dfs::cli::register_cli_commands(registry, unexpected_execution).unwrap();
    let registry =
        sinomo_runtime::rollout_host::cli::register_cli_commands(registry, unexpected_execution)
            .unwrap();
    sinomo_runtime::test::cli::register_cli_commands(registry, unexpected_execution).unwrap()
}

#[test]
fn all_domain_owners_supply_registration_help_and_validation() {
    let registry = owner_registry();
    let mut names = registry
        .metadata()
        .into_iter()
        .map(|metadata| metadata.name)
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        [
            "agent",
            "dfs",
            "memory",
            "models",
            "providers",
            "rollout",
            "skill",
            "test",
            "usage",
            "voice"
        ]
    );
    for name in &names {
        let command = registry
            .parse(&[name.clone(), "--help".into()])
            .unwrap()
            .unwrap();
        assert!(command.is_help(), "{name}");
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        command
            .run(&mut CliCommandContext::new(&mut stdout, &mut stderr))
            .unwrap();
        assert!(!stdout.is_empty(), "{name} must supply its own help");
        assert!(stderr.is_empty(), "{name}");
        assert!(
            registry
                .parse(&[name.clone(), "--unknown-migration-option".into()])
                .is_err(),
            "{name}"
        );
    }
    for alias in ["model", "skills"] {
        assert!(
            registry
                .parse(&[alias.into(), "--help".into()])
                .unwrap()
                .unwrap()
                .is_help()
        );
    }
    let completion = registry.autocomplete_specs();
    for (command, subcommand) in [
        ("agent", "session"),
        ("models", "options"),
        ("dfs", "mount"),
        ("rollout", "resume"),
        ("voice", "call"),
    ] {
        assert!(completion.iter().any(|spec| spec.word == command
            && spec.children.iter().any(|child| child.word == subcommand)));
    }
}

#[test]
fn owners_preserve_policy_and_model_root_behavior() {
    let registry = owner_registry();
    for args in [
        vec!["dfs", "status", "--cluster"],
        vec!["test", "run", "fixture", "--require-cluster"],
    ] {
        let command = registry
            .parse(&args.into_iter().map(String::from).collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        assert!(command.policy().requires_cluster_state);
    }
    let voice = registry
        .parse(&["voice".into(), "devices".into()])
        .unwrap()
        .unwrap();
    assert!(!voice.policy().requires_cluster_state);
    let overview = sinomo_lm::cli::models::parse_models_command(&[]).unwrap();
    assert!(matches!(
        overview,
        sinomo_lm::cli::models::ModelsCliCommand::Overview
    ));
    assert!(
        sinomo_runtime::rollout_host::cli::parse_rollout_command(&[
            "verify".into(),
            "--target".into(),
            "node".into()
        ])
        .is_err()
    );
}
