use crate::internal::*;
use std::time::{SystemTime, UNIX_EPOCH};

mod wat;
pub(super) use wat::*;

pub(super) fn test_wasi_request(timeout_ms: u64) -> SandboxCommandRequest {
    SandboxCommandRequest {
        sandbox_id: SandboxId::from_legacy_named("sandbox_wasi_test").expect("sandbox"),
        argv: vec!["test.wasm".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        stdin: None,
        env: BTreeMap::new(),
        timeout_ms,
    }
}

pub(super) fn unique_test_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let root = std::env::var_os("SINOMO_SANDBOX_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_dir()
                .expect("current dir")
                .join("target")
                .join("sinomo-test-tmp")
        });
    root.join(format!("{prefix}-{nanos}"))
}

pub(super) fn busybox_applet_config_symbol(command: &str) -> Option<&'static str> {
    Some(match command {
        "cat" => "CONFIG_CAT=y",
        "awk" => "CONFIG_AWK=y",
        "chmod" => "CONFIG_CHMOD=y",
        "date" => "CONFIG_DATE=y",
        "cp" => "CONFIG_CP=y",
        "cmp" => "CONFIG_CMP=y",
        "diff" => "CONFIG_DIFF=y",
        "dirname" => "CONFIG_DIRNAME=y",
        "echo" => "CONFIG_ECHO=y",
        "env" => "CONFIG_ENV=y",
        "false" => "CONFIG_FALSE=y",
        "find" => "CONFIG_FIND=y",
        "grep" => "CONFIG_GREP=y",
        "gzip" => "CONFIG_GZIP=y",
        "head" => "CONFIG_HEAD=y",
        "ln" => "CONFIG_LN=y",
        "ls" => "CONFIG_LS=y",
        "mkdir" => "CONFIG_MKDIR=y",
        "mv" => "CONFIG_MV=y",
        "patch" => "CONFIG_PATCH=y",
        "printf" => "CONFIG_PRINTF=y",
        "pwd" => "CONFIG_PWD=y",
        "rm" => "CONFIG_RM=y",
        "rmdir" => "CONFIG_RMDIR=y",
        "sed" => "CONFIG_SED=y",
        "sha256sum" => "CONFIG_SHA256SUM=y",
        "sh" => "CONFIG_ASH=y",
        "sort" => "CONFIG_SORT=y",
        "tail" => "CONFIG_TAIL=y",
        "tar" => "CONFIG_TAR=y",
        "test" => "CONFIG_TEST=y",
        "touch" => "CONFIG_TOUCH=y",
        "true" => "CONFIG_TRUE=y",
        "tr" => "CONFIG_TR=y",
        "uname" => "CONFIG_UNAME=y",
        "uniq" => "CONFIG_UNIQ=y",
        "wc" => "CONFIG_WC=y",
        _ => return None,
    })
}

pub(super) fn create_test_instance(
    name: &str,
) -> (SandboxInstanceManifest, LocalSandboxInstanceLayout, PathBuf) {
    let runtime_root = unique_test_dir(name);
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named(name).expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let (manifest, layout) = store
        .create(sandbox_id, toolchain_id, OffsetDateTime::now_utc())
        .expect("create");
    (manifest, layout, runtime_root)
}

pub(super) fn read_staged_toolchain_cache_file(
    layout: &LocalSandboxInstanceLayout,
    relative_path: &str,
) -> Vec<u8> {
    let cache_root = layout.toolchain_runtime_cache_root();
    let mut matches = Vec::new();
    let entries = fs::read_dir(&cache_root).expect("list toolchain cache");
    for entry in entries {
        let entry = entry.expect("read toolchain cache entry");
        let root = entry.path();
        if !root.join(".complete").is_file() {
            continue;
        }
        let candidate = root.join(relative_path);
        if candidate.is_file() {
            matches.push(fs::read(&candidate).expect("read staged toolchain cache file"));
        }
    }
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one staged toolchain cache file for {relative_path}"
    );
    matches.remove(0)
}

pub(super) fn completed_command_record(name: &str) -> SandboxCommandRecord {
    let sandbox_id = SandboxId::from_legacy_named(name).expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named(&format!("{name}_command")).expect("command");
    let now = OffsetDateTime::now_utc();
    SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::Completed,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec!["echo".to_string(), "hello".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::new(),
        started_at: now,
        completed_at: Some(now),
        response: Some(SandboxCommandResponse {
            command_id: command_id.clone(),
            exit_status: 0,
            stdout_ref: sandbox_command_stdout_path(&sandbox_id, &command_id),
            stderr_ref: sandbox_command_stderr_path(&sandbox_id, &command_id),
            changed_paths: Vec::new(),
            resource_usage: SandboxResourceUsage::default(),
        }),
        error: None,
    }
}
