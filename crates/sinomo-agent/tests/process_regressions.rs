#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![cfg(unix)]
use sinomo_agent::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::{Duration, Instant};

thread_local! { static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) }; }
struct AllocationCounter;
#[global_allocator]
static ALLOCATOR: AllocationCounter = AllocationCounter;
fn record_allocation(size: usize) {
    let _ = ALLOCATED.try_with(|value| {
        if let Some(previous) = value.get() {
            value.set(Some(previous + size));
        }
    });
}
unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
}
#[cfg(unix)]
#[test]
fn process_poll_at_eof_must_not_copy_all_previous_output() {
    let root = sinomo_testdir::TestDir::new("agent-review-output-poll");
    let manager = AgentProcessManager::new().with_output_directory(root.join("capture"));
    let process = manager
        .start(
            AgentProcessStart::new(
                "bounded output fixture",
                "/bin/sh",
                ["-c", "head -c 8388608 /dev/zero"],
                root.path(),
            ),
            16,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = manager.snapshot(&process.process_id, 0, 0, 16).unwrap();
        if snapshot.status == AgentProcessLifecycleStatus::Completed
            && snapshot.stdout_total_bytes == 8 * 1024 * 1024
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bounded command fixture must finish"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    ALLOCATED.set(Some(0));
    let snapshot = manager
        .snapshot(&process.process_id, 8 * 1024 * 1024, 0, 16)
        .unwrap();
    let allocated = ALLOCATED.replace(None).unwrap();
    assert!(snapshot.stdout.is_empty());
    let path = root.join("capture").join(&process.process_id);
    assert_eq!(
        std::fs::metadata(path.join("stdout")).unwrap().len(),
        8 * 1024 * 1024
    );
    drop(manager);
    let deadline = Instant::now() + Duration::from_secs(1);
    while path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        !path.exists(),
        "completed capture cleaned up after its owner drops"
    );
    assert!(
        allocated < 64 * 1024,
        "polling zero new bytes allocated {allocated} bytes"
    );
}

#[test]
fn command_keeps_complete_file_evidence_with_bounded_previews() {
    let root = sinomo_testdir::TestDir::new("agent-command-file-evidence");
    let evidence = root.join("evidence");
    let mut config = LocalWorkspaceAgentConfig::new(root.path())
        .using_local_process_commands()
        .allowing_shell_commands(true)
        .with_evidence_dir(&evidence);
    config.max_command_output_bytes = 32;
    let registry = AgentToolRegistry::local_workspace_with_config(config)
        .with_local_permission_checker(|permission| {
            Ok(permission == sinomo_runtime::node::LocalNodePermission::RemoteManagement)
        });
    let session = InteractionSession::new(
        sinomo_runtime::events::SessionId::generate(),
        sinomo_runtime::node::NodeId::generate(),
        InteractionSurface::new("process-regression", InteractionSurfaceKind::CommandLine),
    );
    let outcome = registry.execute(
        &session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "run_command".to_string(),
            input: serde_json::json!({"command": "head -c 1048576 /dev/zero | tr '\\000' x"}),
            r#async: false,
            summary: "bounded preview with complete evidence".to_string(),
        },
    );
    assert_eq!(
        outcome.status,
        AgentToolStatus::Succeeded,
        "{}",
        outcome.output
    );
    assert_eq!(outcome.output["stdout"], "x".repeat(32));
    assert_eq!(outcome.output["stdout_truncated"], true);
    let path = outcome.output["stdout_ref"]["uri"]
        .as_str()
        .expect("full stdout artifact");
    let contents = std::fs::read(path).unwrap();
    assert_eq!(contents.len(), 1024 * 1024);
    assert!(contents.iter().all(|byte| *byte == b'x'));
    for entry in std::fs::read_dir(&evidence).unwrap() {
        let path = entry.unwrap().path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            assert!(
                std::fs::metadata(path).unwrap().len() < 16 * 1024,
                "outcome metadata must reference full streams instead of duplicating them"
            );
        }
    }
}
