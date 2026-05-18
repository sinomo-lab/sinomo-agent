use super::*;
use crate::{WasiProcess, syscalls::*};

/// ### `proc_signal()`
/// Sends a signal to a child process
///
/// ## Parameters
///
/// * `pid` - Handle of the child process to wait on
/// * `sig` - Signal to send the child process
#[instrument(level = "trace", skip_all, fields(%pid, ?sig), ret)]
pub fn proc_signal(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    sig: Signal,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let pid_signed = pid as i32;
    let ret = match pid_signed {
        0 => signal_process_group(env, env.process.pgid(), sig),
        -1 => signal_all_processes(env, sig),
        pid if pid < -1 => match pid.checked_neg() {
            Some(pgid) => signal_process_group(env, WasiProcessId::from(pgid as u32), sig),
            None => Errno::Inval,
        },
        _ => {
            let pid: WasiProcessId = pid.into();
            if let Some(process) = env.control_plane.get_process(pid) {
                if sig != Signal::Signone {
                    process.signal_process(sig);
                }
                Errno::Success
            } else {
                Errno::Badf
            }
        }
    };

    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ret)
}

fn signal_process_group(env: &WasiEnv, pgid: WasiProcessId, sig: Signal) -> Errno {
    let targets = env
        .control_plane
        .processes()
        .into_iter()
        .filter(|process| process.pgid() == pgid)
        .collect::<Vec<_>>();
    signal_process_targets(targets, sig)
}

fn signal_all_processes(env: &WasiEnv, sig: Signal) -> Errno {
    signal_process_targets(env.control_plane.processes(), sig)
}

fn signal_process_targets(targets: Vec<WasiProcess>, sig: Signal) -> Errno {
    if targets.is_empty() {
        return Errno::Badf;
    }
    if sig != Signal::Signone {
        for process in targets {
            process.signal_process_direct(sig);
        }
    }
    Errno::Success
}
