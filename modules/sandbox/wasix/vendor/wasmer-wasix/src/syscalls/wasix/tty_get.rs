use super::*;
use crate::WasiTtyState;
use crate::syscalls::*;

/// ### `tty_get()`
/// Retrieves the current state of the TTY
#[instrument(level = "trace", skip_all, ret)]
pub fn tty_get<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    tty_state: WasmPtr<Tty, M>,
) -> Errno {
    let env = ctx.data();
    let state = if let Some(bridge) = env.runtime.tty() {
        bridge.tty_get()
    } else {
        WasiTtyState::default()
    };
    let state = Tty {
        cols: state.cols,
        rows: state.rows,
        width: state.width,
        height: state.height,
        stdin_tty: state.stdin_tty,
        stdout_tty: state.stdout_tty,
        stderr_tty: state.stderr_tty,
        echo: state.echo,
        line_buffered: state.line_buffered,
    };

    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(tty_state.write(&memory, state));

    Errno::Success
}
