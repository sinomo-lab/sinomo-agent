use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MainExitObservation {
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub requested_shutdown: bool,
}

impl MainExitObservation {
    pub fn exit_code(exit_code: i32) -> Self {
        Self {
            exit_code: Some(exit_code),
            signal: None,
            requested_shutdown: false,
        }
    }

    pub fn signal(signal: i32) -> Self {
        Self {
            exit_code: None,
            signal: Some(signal),
            requested_shutdown: false,
        }
    }

    pub fn requested_shutdown() -> Self {
        Self {
            exit_code: Some(0),
            signal: None,
            requested_shutdown: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MainExitKind {
    CleanExit,
    RequestedShutdown,
    TerminatedOrKilled,
    Crash,
}

pub fn classify_main_exit(observation: MainExitObservation) -> MainExitKind {
    if observation.requested_shutdown {
        return MainExitKind::RequestedShutdown;
    }

    if let Some(exit_code) = observation.exit_code {
        return if exit_code == 0 {
            MainExitKind::CleanExit
        } else {
            MainExitKind::Crash
        };
    }

    match observation.signal {
        Some(4 | 6 | 7 | 8 | 11) => MainExitKind::Crash,
        Some(1 | 2 | 9 | 15) => MainExitKind::TerminatedOrKilled,
        Some(_) => MainExitKind::TerminatedOrKilled,
        None => MainExitKind::TerminatedOrKilled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_exit_classifier_restarts_only_real_crashes() {
        assert_eq!(
            classify_main_exit(MainExitObservation::requested_shutdown()),
            MainExitKind::RequestedShutdown
        );
        assert_eq!(
            classify_main_exit(MainExitObservation::exit_code(0)),
            MainExitKind::CleanExit
        );
        assert_eq!(
            classify_main_exit(MainExitObservation::exit_code(101)),
            MainExitKind::Crash
        );
        assert_eq!(
            classify_main_exit(MainExitObservation::signal(15)),
            MainExitKind::TerminatedOrKilled
        );
        assert_eq!(
            classify_main_exit(MainExitObservation::signal(9)),
            MainExitKind::TerminatedOrKilled
        );
        assert_eq!(
            classify_main_exit(MainExitObservation::signal(11)),
            MainExitKind::Crash
        );
    }
}
