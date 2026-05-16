use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_stage_layout(name: &str) -> (LocalSandboxInstanceLayout, PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("{name}-{}-{nanos}", std::process::id()));
    let sandbox_id = SandboxId::from_legacy_named(name).expect("sandbox id");
    (LocalSandboxInstanceLayout::new(&root, &sandbox_id), root)
}

#[test]
fn toolchain_runtime_cache_reuses_content_addressed_stage() {
    let (layout, root) = unique_stage_layout("sinomo-sandbox-toolchain-cache-reuse");
    let artifacts = vec![
        ("share/pkg/data.txt".to_string(), b"support".to_vec()),
        ("Lib/site.py".to_string(), b"stdlib".to_vec()),
    ];
    let commands = vec![
        ("grep".to_string(), b"grep module".to_vec()),
        ("sh".to_string(), b"sh module".to_vec()),
    ];

    let first_preopens =
        stage_toolchain_runtime_to_local(&layout, &artifacts, &commands).expect("first stage");
    assert_eq!(
        first_preopens
            .iter()
            .map(|preopen| preopen.guest_path.as_str())
            .collect::<Vec<_>>(),
        vec!["/Lib", "/bin", "/share"]
    );

    let cache_root = layout.toolchain_runtime_cache_root();
    let cache_entries = fs::read_dir(&cache_root)
        .expect("cache root")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(cache_entries.len(), 1);
    let cache_dir = cache_entries[0].path();
    assert_eq!(
        fs::read(cache_dir.join("share/pkg/data.txt")).expect("artifact"),
        b"support"
    );
    assert_eq!(
        fs::read(cache_dir.join("bin/sh")).expect("command"),
        b"sh module"
    );

    let reversed_artifacts = vec![
        ("Lib/site.py".to_string(), b"stdlib".to_vec()),
        ("share/pkg/data.txt".to_string(), b"support".to_vec()),
    ];
    let reversed_commands = vec![
        ("sh".to_string(), b"sh module".to_vec()),
        ("grep".to_string(), b"grep module".to_vec()),
    ];
    let second_preopens =
        stage_toolchain_runtime_to_local(&layout, &reversed_artifacts, &reversed_commands)
            .expect("second stage");

    assert_eq!(first_preopens, second_preopens);
    assert_eq!(
        fs::read_dir(&cache_root)
            .expect("cache root")
            .collect::<Result<Vec<_>, _>>()
            .expect("cache entries")
            .len(),
        1
    );

    fs::remove_dir_all(root).ok();
}

#[test]
fn toolchain_runtime_cache_changes_when_content_changes() {
    let (layout, root) = unique_stage_layout("sinomo-sandbox-toolchain-cache-change");
    let artifacts = vec![("share/pkg/data.txt".to_string(), b"support".to_vec())];
    let commands = vec![("sh".to_string(), b"sh module".to_vec())];
    stage_toolchain_runtime_to_local(&layout, &artifacts, &commands).expect("first stage");

    let changed_artifacts = vec![("share/pkg/data.txt".to_string(), b"changed".to_vec())];
    stage_toolchain_runtime_to_local(&layout, &changed_artifacts, &commands)
        .expect("changed stage");

    let cache_entries = fs::read_dir(layout.toolchain_runtime_cache_root())
        .expect("cache root")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(cache_entries.len(), 2);

    fs::remove_dir_all(root).ok();
}

#[test]
fn toolchain_runtime_cache_repairs_incomplete_or_extra_complete_stage() {
    let (layout, root) = unique_stage_layout("sinomo-sandbox-toolchain-cache-repair");
    let artifacts = vec![("share/pkg/data.txt".to_string(), b"support".to_vec())];
    let commands = vec![("sh".to_string(), b"sh module".to_vec())];

    stage_toolchain_runtime_to_local(&layout, &artifacts, &commands).expect("first stage");
    let cache_root = layout.toolchain_runtime_cache_root();
    let cache_entries = fs::read_dir(&cache_root)
        .expect("cache root")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(cache_entries.len(), 1);
    let cache_dir = cache_entries[0].path();
    fs::remove_file(cache_dir.join("bin/sh")).expect("remove cached command");
    fs::create_dir_all(cache_dir.join("share/pkg/stale")).expect("untracked nested dir");
    fs::write(cache_dir.join("share/pkg/stale/data.txt"), b"stale").expect("untracked nested file");
    fs::create_dir_all(cache_dir.join("share/pkg/empty")).expect("untracked empty dir");

    stage_toolchain_runtime_to_local(&layout, &artifacts, &commands).expect("repair stage");

    let repaired_entries = fs::read_dir(&cache_root)
        .expect("cache root")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(repaired_entries.len(), 1);
    let repaired_dir = repaired_entries[0].path();
    assert_eq!(
        fs::read(repaired_dir.join("bin/sh")).expect("command"),
        b"sh module"
    );
    assert!(!repaired_dir.join("share/pkg/stale").exists());
    assert!(!repaired_dir.join("share/pkg/empty").exists());

    fs::remove_dir_all(root).ok();
}

#[test]
fn toolchain_runtime_cache_repairs_stale_complete_manifest() {
    let (layout, root) = unique_stage_layout("sinomo-sandbox-toolchain-cache-marker");
    let artifacts = vec![("share/pkg/data.txt".to_string(), b"support".to_vec())];
    let commands = vec![("sh".to_string(), b"sh module".to_vec())];

    stage_toolchain_runtime_to_local(&layout, &artifacts, &commands).expect("first stage");
    let cache_root = layout.toolchain_runtime_cache_root();
    let cache_entries = fs::read_dir(&cache_root)
        .expect("cache root")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(cache_entries.len(), 1);
    let cache_dir = cache_entries[0].path();
    fs::write(cache_dir.join(".complete"), b"complete\n").expect("stale marker");

    stage_toolchain_runtime_to_local(&layout, &artifacts, &commands).expect("repair stage");

    let complete_marker = fs::read_to_string(cache_dir.join(".complete")).expect("complete marker");
    assert!(complete_marker.starts_with("sinomo-toolchain-runtime-cache-v1\n"));
    assert!(complete_marker.contains("artifact\tshare/pkg/data.txt\t7\tblake3:"));
    assert!(complete_marker.contains("command\tsh\t9\tblake3:"));

    fs::remove_dir_all(root).ok();
}
