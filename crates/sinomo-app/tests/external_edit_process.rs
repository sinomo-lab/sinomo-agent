//! Exercise the real desktop helper, native directory watcher, and durable queue without
//! launching an interactive editor or contacting a user's SIFS daemon.
#![cfg(all(feature = "desktop", not(target_os = "android")))]
use sinomo_runtime::{
    core::storage::durable_file::{DurableAtomicFileOptions, write_file_atomic},
    file_sync::DurableWritebackQueue,
    local::SinomoDataDir,
};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const OWNER: &str = "module.files.external-edit";
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(data: &SinomoDataDir, key: &str) -> Process {
    Process(
        Command::new(env!("CARGO_BIN_EXE_sinomo-app"))
            .args(["--external-edit-worker", key])
            .env("SINOMO_DATA_DIR", data.root())
            .stdin(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
fn wait(label: &str, mut predicate: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(18);
    while !predicate() {
        assert!(Instant::now() < end, "helper did not publish {label}");
        std::thread::sleep(Duration::from_millis(40));
    }
}
fn status(dir: &Path) -> String {
    fs::read(dir.join("session.sobj"))
        .ok()
        .and_then(|b| sinomo_runtime::module_api::decode_sobj::<serde_json::Value>(&b).ok())
        .and_then(|v| v.get("status")?.as_str().map(str::to_owned))
        .unwrap_or_default()
}
#[test]
fn external_edit_helper_observes_atomic_save_and_recovers_pending_work_after_exit() {
    let temporary =
        std::env::temp_dir().join(format!("ee-{}", sinomo_runtime::core::Id::generate()));
    let data = SinomoDataDir::new(&temporary);
    let key = blake3::hash(b"test-node\0test-object").to_hex().to_string();
    let dir = data
        .module_runtime_dir()
        .join(OWNER)
        .join("sessions")
        .join(&key);
    fs::create_dir_all(dir.join("working")).unwrap();
    let working = dir.join("working/editor.txt");
    fs::write(&working, b"base").unwrap();
    let manifest = serde_json::json!({"node":"test-node","object":"test-object","path":"/files/editor.txt","address":"127.0.0.1:0","version":"v1","hash":format!("blake3:{}",blake3::hash(b"base").to_hex()),"finished":false,"status":"Starting"});
    fs::write(
        dir.join("session.sobj"),
        sinomo_runtime::module_api::encode_sobj(&manifest).unwrap(),
    )
    .unwrap();
    let q = DurableWritebackQueue::open(OWNER, &data, &key, "/").unwrap();
    let journal = q.journal_path().to_path_buf();
    drop(q);
    let helper = start(&data, &key);
    wait("initial offline status", || {
        status(&dir).starts_with("Offline")
    });
    let initial_len = fs::metadata(&journal).unwrap().len();
    write_file_atomic(
        &working,
        b"changed\0bytes",
        DurableAtomicFileOptions::new("working", "editor-save").sync_all(),
    )
    .unwrap();
    wait("durable journal append after atomic save", || {
        fs::metadata(&journal).is_ok_and(|m| m.len() > initial_len)
    });
    drop(helper);
    let queue = DurableWritebackQueue::open(OWNER, &data, &key, "/").unwrap();
    let operation = queue.operations().next().unwrap();
    assert_eq!(
        queue
            .read_staged_content(&operation.request.operation_id)
            .unwrap(),
        b"changed\0bytes"
    );
    assert_eq!(
        operation
            .request
            .preconditions
            .source
            .as_ref()
            .unwrap()
            .version,
        "v1"
    );
    drop(queue);
    fs::write(dir.join("control"), b"finish").unwrap();
    let helper = start(&data, &key);
    wait("retained finish result", || {
        fs::read(dir.join("session.sobj"))
            .ok()
            .and_then(|b| sinomo_runtime::module_api::decode_sobj::<serde_json::Value>(&b).ok())
            .is_some_and(|v| {
                v.get("last_action")
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s.starts_with("Finish deferred"))
            })
    });
    drop(helper);
    assert_eq!(fs::read(&working).unwrap(), b"changed\0bytes");
    fs::remove_dir_all(temporary).unwrap();
}

#[test]
fn editor_lifetime_previous_boot_reclaims_only_confirmed_working_bytes() {
    let temporary =
        std::env::temp_dir().join(format!("reboot-{}", sinomo_runtime::core::Id::generate()));
    let data = SinomoDataDir::new(&temporary);
    let key = blake3::hash(b"test-node\0test-object").to_hex().to_string();
    let dir = data
        .module_runtime_dir()
        .join(OWNER)
        .join("sessions")
        .join(&key);
    fs::create_dir_all(dir.join("working")).unwrap();
    fs::create_dir_all(dir.join("opens")).unwrap();
    fs::write(dir.join("opens/prior"), b"11111111111111111111111111111111").unwrap();
    let working = dir.join("working/editor.txt");
    fs::write(&working, b"base").unwrap();
    let backup = dir.join("working/preserved-other");
    fs::write(&backup, b"keep").unwrap();
    let manifest = serde_json::json!({"node":"test-node","object":"test-object","path":"/files/editor.txt","address":"127.0.0.1:0","version":"v1","hash":format!("blake3:{}",blake3::hash(b"base").to_hex()),"finished":false,"status":"Synced"});
    fs::write(
        dir.join("session.sobj"),
        sinomo_runtime::module_api::encode_sobj(&manifest).unwrap(),
    )
    .unwrap();
    let helper = start(&data, &key);
    wait("previous boot cleanup", || {
        !working.exists() && status(&dir).starts_with("Finished")
    });
    assert_eq!(fs::read(backup).unwrap(), b"keep");
    drop(helper);
    fs::remove_dir_all(temporary).unwrap();
}

#[test]
fn editor_lifetime_launch_monitor_waits_for_docs_and_classifies_handoffs() {
    use std::io::{BufRead, BufReader};
    let temp =
        std::env::temp_dir().join(format!("launch-{}", sinomo_runtime::core::Id::generate()));
    fs::create_dir(&temp).unwrap();
    let src = temp.join("fixture.rs");
    fs::write(&src,r#"
        use std::{env,fs,thread,time::Duration,process::Command};
        fn main() {
            let args:Vec<_>=env::args_os().skip(1).collect();
            let dir=std::path::PathBuf::from(env::var_os("TEST_EDIT_SIGNALS").unwrap());
            match env::var("TEST_EDIT_MODE").unwrap().as_str() {
                "wait" => {assert_eq!(args[0],"--wait");}
                "fast" => return,
                "handoff" if args[0]!="--child" => {
                    Command::new(env::current_exe().unwrap()).arg("--child").arg(args.last().unwrap()).spawn().unwrap();
                    thread::sleep(Duration::from_millis(650));return;
                }
                _=>{}
            }
            assert!(std::path::Path::new(args.last().unwrap()).is_file());
            fs::write(dir.join("ready"),b"").unwrap();
            for _ in 0..300 {if dir.join("release").exists(){return} thread::sleep(Duration::from_millis(100));}
            std::process::exit(1)
        }"#).unwrap();
    let ordinary = temp.join(format!(
        "editor-{}{}",
        sinomo_runtime::core::Id::generate(),
        std::env::consts::EXE_SUFFIX
    ));
    assert!(
        Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .arg(&src)
            .arg("-o")
            .arg(&ordinary)
            .status()
            .unwrap()
            .success()
    );
    let waiter = temp.join(format!("subl{}", std::env::consts::EXE_SUFFIX));
    fs::copy(&ordinary, &waiter).unwrap();
    let data = SinomoDataDir::new(&temp);
    let key = blake3::hash(b"test-node\0test-object").to_hex().to_string();
    let dir = data
        .module_runtime_dir()
        .join(OWNER)
        .join("sessions")
        .join(&key);
    fs::create_dir_all(dir.join("working")).unwrap();
    fs::create_dir(dir.join("opens")).unwrap();
    let working = dir.join("working/editor.txt");
    fs::write(&working, b"base").unwrap();
    let manifest = serde_json::json!({"node":"test-node","object":"test-object","path":"/files/editor.txt","address":"127.0.0.1:0","version":"v1","hash":format!("blake3:{}",blake3::hash(b"base").to_hex()),"finished":false,"status":"Synced"});
    fs::write(
        dir.join("session.sobj"),
        sinomo_runtime::module_api::encode_sobj(&manifest).unwrap(),
    )
    .unwrap();
    let run = |mode: &str, editor: &Path| {
        let token = sinomo_runtime::core::Id::generate().to_string();
        let ticket = dir.join("opens").join(&token);
        fs::write(&ticket, b"unknown").unwrap();
        let signals = temp.join(mode);
        fs::create_dir(&signals).unwrap();
        let mut child = Process(
            Command::new(env!("CARGO_BIN_EXE_sinomo-app"))
                .args(["--external-edit-launch", &key, &token])
                .env("SINOMO_DATA_DIR", data.root())
                .env("SINOMO_EXTERNAL_EDITOR", editor)
                .env("TEST_EDIT_MODE", mode)
                .env("TEST_EDIT_SIGNALS", &signals)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let mut reply = String::new();
        BufReader::new(child.0.stdout.take().unwrap())
            .read_line(&mut reply)
            .unwrap();
        assert_eq!(reply.trim(), "OK");
        if mode != "fast" {
            wait("fixture editor ready", || signals.join("ready").exists());
            if mode == "handoff" {
                std::thread::sleep(Duration::from_secs(3));
            }
            assert!(ticket.exists());
            fs::write(signals.join("release"), b"").unwrap();
        }
        wait("launch monitor exit", || {
            child.0.try_wait().unwrap().is_some()
        });
        let result = ticket.with_extension(if mode == "fast" {
            "ambiguous"
        } else {
            "closed"
        });
        assert!(
            result.exists(),
            "{mode} produced the wrong lifecycle result"
        );
        result
    };
    let ambiguous = run("fast", &ordinary);
    let helper = start(&data, &key);
    run("wait", &waiter);
    assert!(working.is_file() && ambiguous.exists());
    run("handoff", &ordinary);
    assert!(working.is_file() && ambiguous.exists());
    fs::write(dir.join("control"), b"finish").unwrap();
    wait("manual cleanup", || {
        !working.exists() && status(&dir).starts_with("Finished")
    });
    drop(helper);
    fs::remove_dir_all(temp).unwrap();
}
