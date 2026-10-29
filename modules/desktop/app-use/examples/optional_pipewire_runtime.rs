//! Probe optional client availability/lifetime in an explicitly selected fixture.
#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--expect-unavailable") {
        assert!(pipewire::check_available().is_err());
        println!("optional PipeWire unavailable without initialization");
        return Ok(());
    }
    let fd_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
    let before = fd_count();
    for _ in 0..2 {
        pipewire::try_init().map_err(std::io::Error::other)?;
        {
            let main_loop = pipewire::main_loop::MainLoopRc::new(None)?;
            let _context = pipewire::context::ContextRc::new(&main_loop, None)?;
        }
        // All context/loop objects and their owned threads were destroyed above.
        unsafe { pipewire::release_optional_runtime() };
        let maps = std::fs::read_to_string("/proc/self/maps")?;
        assert!(!maps.contains("libpipewire-0.3.so"));
        assert_eq!(
            fd_count(),
            before,
            "optional client left a descriptor alive"
        );
    }
    println!("optional PipeWire initialized, drained and unloaded twice");
    Ok(())
}
