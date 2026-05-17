use tracing::debug;
use wasmer::WasmRef;
use windows_sys::Win32::{
    Foundation::FILETIME,
    System::Threading::{GetCurrentProcess, GetCurrentThread, GetProcessTimes, GetThreadTimes},
};

use crate::syscalls::types::wasi::{self, Timestamp};

pub fn platform_clock_res_get(
    clock_id: wasi::Snapshot0Clockid,
    resolution: WasmRef<Timestamp>,
) -> Result<i64, wasi::Errno> {
    let resolution_val = match clock_id {
        // resolution of monotonic clock at 10ms, from:
        // https://docs.microsoft.com/en-us/windows/desktop/api/sysinfoapi/nf-sysinfoapi-gettickcount64
        wasi::Snapshot0Clockid::Monotonic => 10_000_000,
        // TODO: verify or compute this
        wasi::Snapshot0Clockid::Realtime => 1,
        // Windows process/thread CPU time APIs report 100ns units.
        wasi::Snapshot0Clockid::ProcessCputimeId => 100,
        wasi::Snapshot0Clockid::ThreadCputimeId => 100,
        _ => return Err(wasi::Errno::Inval),
    };
    Ok(resolution_val)
}

pub fn platform_clock_time_get(
    clock_id: wasi::Snapshot0Clockid,
    precision: Timestamp,
) -> Result<i64, wasi::Errno> {
    let nanos = match clock_id {
        wasi::Snapshot0Clockid::Monotonic => {
            let tick_ms =
                unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() };
            tick_ms * 1_000_000
        }
        wasi::Snapshot0Clockid::Realtime => {
            let duration = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| {
                    debug!("Error in wasi::platform_clock_time_get: {:?}", e);
                    wasi::Errno::Io
                })?;
            duration.as_nanos() as u64
        }
        wasi::Snapshot0Clockid::ProcessCputimeId => process_cpu_time_nanos()?,
        wasi::Snapshot0Clockid::ThreadCputimeId => thread_cpu_time_nanos()?,
        _ => return Err(wasi::Errno::Inval),
    };
    Ok(nanos as i64)
}

fn process_cpu_time_nanos() -> Result<u64, wasi::Errno> {
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    let ok = unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if ok == 0 {
        return Err(wasi::Errno::Io);
    }
    cpu_filetime_to_nanos(kernel, user)
}

fn thread_cpu_time_nanos() -> Result<u64, wasi::Errno> {
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    let ok = unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if ok == 0 {
        return Err(wasi::Errno::Io);
    }
    cpu_filetime_to_nanos(kernel, user)
}

fn cpu_filetime_to_nanos(kernel: FILETIME, user: FILETIME) -> Result<u64, wasi::Errno> {
    let kernel = filetime_100ns(kernel);
    let user = filetime_100ns(user);
    kernel
        .checked_add(user)
        .and_then(|ticks| ticks.checked_mul(100))
        .ok_or(wasi::Errno::Overflow)
}

fn filetime_100ns(time: FILETIME) -> u64 {
    ((time.dwHighDateTime as u64) << 32) | time.dwLowDateTime as u64
}
