use std::env;

fn main() {
    let runtime_config = sinomo_cli::default_cli_runtime_config()
        .with_native_rust_module_binding(sinomo_evolution::EVOLUTION_NATIVE_RUST_MODULE_BINDING)
        .with_test_cases(sinomo_evolution::register_evolution_test_cases);
    sinomo_cli::install_cli_runtime_config(runtime_config)
        .expect("default CLI module bindings should be installed once");
    if let Err(error) = sinomo_cli::run(env::args().skip(1).collect()) {
        eprintln!("sinomo: {error}");
        std::process::exit(1);
    }
}
