//! Entry point for the Sinomo graphical client.

use std::process::ExitCode;

fn main() -> ExitCode {
    match sinomo_app::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("sinomo-app: {error}");
            ExitCode::FAILURE
        }
    }
}
