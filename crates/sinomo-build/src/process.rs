use std::ffi::OsStr;
use std::process::Command;

/// Creates a captured, non-interactive build subprocess.
///
/// Windows console programs allocate a visible terminal when the daemon does
/// not already own a console. Build-service commands always capture their
/// output and never accept terminal input, so suppress that window explicitly.
pub(crate) fn noninteractive_command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    configure_noninteractive_command(&mut command);
    command
}

fn configure_noninteractive_command(command: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }

    #[cfg(not(windows))]
    let _ = command;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noninteractive_command_preserves_program_and_arguments() {
        let mut command = noninteractive_command("cargo");
        command.arg("--version");

        assert_eq!(command.get_program(), OsStr::new("cargo"));
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![OsStr::new("--version")]
        );
    }

    #[cfg(windows)]
    #[test]
    fn hidden_windows_command_still_captures_output() {
        let output = noninteractive_command("cmd")
            .args(["/d", "/c", "echo", "sinomo-hidden-build"])
            .output()
            .expect("run hidden command");

        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("sinomo-hidden-build"));
    }
}
