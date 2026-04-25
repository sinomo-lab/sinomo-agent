use std::env;

fn main() {
    if let Err(error) = sinomo_cli::run(env::args().skip(1).collect()) {
        eprintln!("sinomo: {error}");
        std::process::exit(1);
    }
}
