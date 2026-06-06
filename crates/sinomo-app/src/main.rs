//! Entry point for the Sinomo client app.
//!
//! With arguments, it behaves like the `sinomo` CLI (so the same binary can be
//! scripted). With no arguments, it launches the graphical app.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if !args.is_empty() {
        return match sinomo_cli::run(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("sinomo: {error}");
                ExitCode::FAILURE
            }
        };
    }

    match sinomo_app::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sinomo-app: {error}");
            ExitCode::FAILURE
        }
    }
}
