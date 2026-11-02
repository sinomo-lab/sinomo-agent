//! Standalone CLI daemon lifetime, distinct from app-owned background work.

pub(super) const BACKGROUND_FLAGS: u32 = 0x0000_0200 | 0x0800_0000 | 0x0000_0008;
const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
const JOB_OBJECT_LIMIT_BREAKAWAY_OK: u32 = 0x0000_0800;
const JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK: u32 = 0x0000_1000;
const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;

fn resident_flags(job_flags: Option<u32>) -> Result<u32, &'static str> {
    let Some(flags) = job_flags else {
        return Ok(BACKGROUND_FLAGS);
    };
    if flags & JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE == 0
        || flags & JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK != 0
    {
        return Ok(BACKGROUND_FLAGS);
    }
    if flags & JOB_OBJECT_LIMIT_BREAKAWAY_OK != 0 {
        return Ok(BACKGROUND_FLAGS | CREATE_BREAKAWAY_FROM_JOB);
    }
    Err(
        "the parent job would terminate this resident daemon and does not permit detachment; use a persistent per-user launch context",
    )
}

/// Only `daemon start` / `daemon shadow start` request independent residence.
/// Generic background commands and app-owned daemon jobs retain their owner.
#[cfg(windows)]
pub(super) fn prepare(command: &mut std::process::Command) -> std::io::Result<()> {
    use std::os::windows::process::CommandExt;
    let flags = resident_flags(inherited_job_flags()?).map_err(std::io::Error::other)?;
    command.creation_flags(flags);
    Ok(())
}

#[cfg(windows)]
fn inherited_job_flags() -> std::io::Result<Option<u32>> {
    use std::{ffi::c_void, mem::size_of, ptr};

    #[repr(C)]
    #[derive(Default)]
    struct BasicLimits {
        process_user_time: i64,
        job_user_time: i64,
        flags: u32,
        minimum_working_set: usize,
        maximum_working_set: usize,
        active_process_limit: u32,
        affinity: usize,
        priority_class: u32,
        scheduling_class: u32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct ExtendedLimits {
        basic: BasicLimits,
        io_counters: [u64; 6],
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory: usize,
        peak_job_memory: usize,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn IsProcessInJob(process: *mut c_void, job: *mut c_void, result: *mut i32) -> i32;
        fn QueryInformationJobObject(
            job: *mut c_void,
            class: u32,
            information: *mut c_void,
            size: u32,
            returned_size: *mut u32,
        ) -> i32;
    }
    let mut in_job = 0;
    // Pseudo-handle and current inherited job only: no privilege acquisition,
    // foreign job handle, policy mutation, or retry with different authority.
    if unsafe { IsProcessInJob(GetCurrentProcess(), ptr::null_mut(), &mut in_job) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    if in_job == 0 {
        return Ok(None);
    }
    let mut limits = ExtendedLimits::default();
    if unsafe {
        QueryInformationJobObject(
            ptr::null_mut(),
            9, // JobObjectExtendedLimitInformation
            (&mut limits as *mut ExtendedLimits).cast(),
            size_of::<ExtendedLimits>() as u32,
            ptr::null_mut(),
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Some(limits.basic.flags))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_and_app_owned_background_flags_do_not_break_ownership() {
        assert_eq!(BACKGROUND_FLAGS & CREATE_BREAKAWAY_FROM_JOB, 0);
        assert_eq!(resident_flags(None), Ok(BACKGROUND_FLAGS));
        assert_eq!(resident_flags(Some(0)), Ok(BACKGROUND_FLAGS));
        assert_eq!(
            resident_flags(Some(JOB_OBJECT_LIMIT_BREAKAWAY_OK)),
            Ok(BACKGROUND_FLAGS)
        );
    }

    #[test]
    fn closing_launcher_job_requires_and_uses_existing_breakaway_permission() {
        let flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_BREAKAWAY_OK;
        assert_eq!(
            resident_flags(Some(flags)),
            Ok(BACKGROUND_FLAGS | CREATE_BREAKAWAY_FROM_JOB)
        );
    }

    #[test]
    fn silent_breakaway_uses_existing_job_policy_without_an_extra_request() {
        let flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK;
        assert_eq!(resident_flags(Some(flags)), Ok(BACKGROUND_FLAGS));
    }

    #[test]
    fn restrictive_launcher_job_fails_before_spawn_instead_of_escaping() {
        assert!(resident_flags(Some(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)).is_err());
        assert!(resident_flags(Some(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | 0x0000_0008)).is_err());
    }
}
