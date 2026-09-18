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
        ("rollout", "app"),
        ("voice", "call"),
    ] {
        assert!(completion.iter().any(|spec| spec.word == command
            && spec.children.iter().any(|child| child.word == subcommand)));
    }
}

#[test]
fn rollout_owner_preserves_desktop_commands() {
    use sinomo_runtime::rollout_host::cli::{RolloutCliCommand, parse_rollout_command};

    for args in [
        vec!["app", "build", "--repo", "source", "--target", "node"],
        vec![
            "app",
            "stage",
            "--directory",
            "bundle",
            "--target",
            "one",
            "--target",
            "two",
        ],
        vec![
            "app",
            "package",
            "--directory",
            "bundle",
            "--revision",
            "commit",
            "--target-os",
            "windows",
            "--target-arch",
            "x86_64",
        ],
        vec!["app", "status"],
        vec!["app", "launch"],
    ] {
        let args = args.into_iter().map(String::from).collect::<Vec<_>>();
        let RolloutCliCommand::Desktop(options) = parse_rollout_command(&args).unwrap() else {
            panic!("desktop command must remain owned by rollout");
        };
        assert_eq!(options.action, args[1]);
        if options.action == "stage" {
            assert_eq!(options.targets, ["one", "two"]);
        }
        if options.action == "package" {
            assert_eq!(options.target_os, "windows");
            assert_eq!(options.target_arch, "x86_64");
        }
        let mut full_args = vec!["rollout".to_owned()];
        full_args.extend(args);
        assert!(
            !owner_registry()
                .parse(&full_args)
                .unwrap()
                .unwrap()
                .is_help()
        );
    }
    for args in [
        vec!["app", "stage"],
        vec!["app", "package", "--directory", "bundle"],
    ] {
        assert!(
            parse_rollout_command(&args.into_iter().map(String::from).collect::<Vec<_>>()).is_err()
        );
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
