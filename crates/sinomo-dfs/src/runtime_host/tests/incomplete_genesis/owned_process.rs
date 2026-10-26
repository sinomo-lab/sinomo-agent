//! Whole DEFAULT-P0 fixture process loss; no surviving-node availability claim.
#![cfg(target_os = "linux")]
use super::*;
use crate::test_runtime::RetainedNativeGenesisFixture;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::{
    fs::{FileExt, MetadataExt},
    process::{CommandExt, ExitStatusExt},
};
use std::process::{Child, Command, ExitStatus, Stdio};

const HELPER: &str = "runtime_host::tests::incomplete_genesis::owned_process::retained_default_genesis_process_child";
const INPUT_ENV: &str = "SINOMO_DFS_RETAINED_DEFAULT_GENESIS_INPUT";
const READY_CAP: Duration = Duration::from_secs(60);
const REAP_CAP: Duration = Duration::from_secs(5);
const DATA_CAP: usize = 2 * 1024 * 1024;
const RAW_CAP: usize = 16 * 1024 * 1024;
const ENTRY_CAP: usize = 512;
type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    owned_root: PathBuf,
    invocation: String,
}

/// Expected durable DATA, never an owner, admission or authenticated return.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    pid: u32,
    invocation: String,
    manifest: RetainedNativeGenesisFixture,
    request: DfsMessageCommand,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DirectoryId {
    device: u64,
    inode: u64,
    uid: u32,
}
impl DirectoryId {
    fn read(path: &Path) -> TestResult<Self> {
        let m = fs::symlink_metadata(path)?;
        if !m.is_dir() || m.file_type().is_symlink() {
            return Err("process DATA path is not a physical directory".into());
        }
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
            uid: m.uid(),
        })
    }
    fn of(m: &fs::Metadata) -> Self {
        Self {
            device: m.dev(),
            inode: m.ino(),
            uid: m.uid(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct FileId {
    device: u64,
    inode: u64,
    uid: u32,
    size: u64,
    modified: i64,
    modified_nanos: i64,
}
impl FileId {
    fn of(m: &fs::Metadata) -> Self {
        Self {
            device: m.dev(),
            inode: m.ino(),
            uid: m.uid(),
            size: m.len(),
            modified: m.mtime(),
            modified_nanos: m.mtime_nsec(),
        }
    }
}

fn read_pinned(file: &File, size: u64, cap: usize) -> TestResult<Vec<u8>> {
    let size = usize::try_from(size)?;
    if size > cap {
        return Err("process DATA exceeds its bounded read".into());
    }
    let mut bytes = vec![0; size];
    let mut offset = 0;
    while offset < size {
        let read = file.read_at(&mut bytes[offset..], offset as u64)?;
        if read == 0 {
            return Err("pinned process file ended early".into());
        }
        offset += read;
    }
    if file.read_at(&mut [0u8], size as u64)? != 0 {
        return Err("pinned process file grew".into());
    }
    Ok(bytes)
}

fn read_data<T: serde::de::DeserializeOwned>(path: &Path) -> TestResult<T> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err("process DATA is not a regular non-symlink file".into());
    }
    let file = File::open(path)?;
    if FileId::of(&before) != FileId::of(&file.metadata()?) {
        return Err("process DATA changed while opening".into());
    }
    let bytes = read_pinned(&file, before.len(), DATA_CAP)?;
    if FileId::of(&fs::symlink_metadata(path)?) != FileId::of(&before) {
        return Err("process DATA changed during read".into());
    }
    Ok(sinomo_core::object::serde_ext::from_bytes(&bytes)?)
}

fn write_data<T: Serialize>(path: &Path, data: &T) -> TestResult<()> {
    let bytes = crate::metadata_io::encode_sobj(data)?;
    if bytes.len() > DATA_CAP {
        return Err("process input/Ready exceeds2MiB".into());
    }
    crate::metadata_io::write_file_atomic(path, &bytes)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct RawFile {
    relative: PathBuf,
    physical: FileId,
    blake3: String,
    bytes: Vec<u8>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct RawDirectory {
    relative: PathBuf,
    physical: DirectoryId,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct RawTree {
    root: PathBuf,
    directories: Vec<RawDirectory>,
    files: Vec<RawFile>,
}

struct PinnedFile {
    path: PathBuf,
    file: File,
    physical: FileId,
}
struct PinnedTree {
    root: PathBuf,
    directories: Vec<(PathBuf, File, DirectoryId)>,
    files: Vec<PinnedFile>,
}
impl PinnedTree {
    fn open(root: PathBuf, uid: u32, entries: &mut usize, bytes: &mut usize) -> TestResult<Self> {
        fn visit(
            tree: &mut PinnedTree,
            path: PathBuf,
            uid: u32,
            entries: &mut usize,
            bytes: &mut usize,
        ) -> TestResult<()> {
            *entries += 1;
            if *entries > ENTRY_CAP {
                return Err("process capture exceeds512 entries".into());
            }
            let m = fs::symlink_metadata(&path)?;
            if m.file_type().is_symlink() || m.uid() != uid {
                return Err("unowned/symlink process capture entry".into());
            }
            let file = File::open(&path)?;
            let opened = file.metadata()?;
            if m.is_dir() {
                let id = DirectoryId::read(&path)?;
                if !opened.is_dir() || DirectoryId::of(&opened) != id {
                    return Err("process directory changed while pinning".into());
                }
                tree.directories.push((path.clone(), file, id));
                for entry in fs::read_dir(path)? {
                    visit(tree, entry?.path(), uid, entries, bytes)?;
                }
            } else if m.is_file() {
                let id = FileId::of(&m);
                if !opened.is_file() || FileId::of(&opened) != id {
                    return Err("process file changed while pinning".into());
                }
                *bytes = bytes
                    .checked_add(usize::try_from(id.size)?)
                    .ok_or("capture size overflow")?;
                if *bytes > RAW_CAP {
                    return Err("process capture exceeds16MiB".into());
                }
                tree.files.push(PinnedFile {
                    path,
                    file,
                    physical: id,
                });
            } else {
                return Err("nonregular process capture entry".into());
            }
            Ok(())
        }
        let mut tree = Self {
            root: root.clone(),
            directories: Vec::new(),
            files: Vec::new(),
        };
        visit(&mut tree, root, uid, entries, bytes)?;
        tree.directories.sort_by(|a, b| a.0.cmp(&b.0));
        tree.files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(tree)
    }

    fn capture(&self) -> TestResult<RawTree> {
        fn names(root: &Path, out: &mut Vec<PathBuf>) -> TestResult<()> {
            if out.len() >= ENTRY_CAP {
                return Err("process capture namespace grew beyond bound".into());
            }
            let m = fs::symlink_metadata(root)?;
            if m.file_type().is_symlink() {
                return Err("process namespace acquired a symlink".into());
            }
            out.push(root.to_owned());
            if m.is_dir() {
                for e in fs::read_dir(root)? {
                    names(&e?.path(), out)?;
                }
            } else if !m.is_file() {
                return Err("process namespace acquired a nonregular entry".into());
            }
            Ok(())
        }
        let mut actual = Vec::new();
        names(&self.root, &mut actual)?;
        actual.sort();
        let mut expected: Vec<_> = self
            .directories
            .iter()
            .map(|x| x.0.clone())
            .chain(self.files.iter().map(|x| x.path.clone()))
            .collect();
        expected.sort();
        if actual != expected {
            return Err("process capture namespace changed".into());
        }
        let mut directories = Vec::new();
        for (path, file, id) in &self.directories {
            if DirectoryId::read(path)? != *id || DirectoryId::of(&file.metadata()?) != *id {
                return Err("pinned process directory replaced".into());
            }
            directories.push(RawDirectory {
                relative: path.strip_prefix(&self.root)?.to_owned(),
                physical: id.clone(),
            });
        }
        let mut files = Vec::new();
        for pin in &self.files {
            let before = fs::symlink_metadata(&pin.path)?;
            if !before.is_file()
                || before.file_type().is_symlink()
                || FileId::of(&before) != pin.physical
                || FileId::of(&pin.file.metadata()?) != pin.physical
            {
                return Err("pinned process file replaced or changed".into());
            }
            let bytes = read_pinned(&pin.file, pin.physical.size, RAW_CAP)?;
            if FileId::of(&fs::symlink_metadata(&pin.path)?) != pin.physical
                || FileId::of(&pin.file.metadata()?) != pin.physical
            {
                return Err("pinned process file changed during capture".into());
            }
            files.push(RawFile {
                relative: pin.path.strip_prefix(&self.root)?.to_owned(),
                physical: pin.physical.clone(),
                blake3: blake3::hash(&bytes).to_hex().to_string(),
                bytes,
            });
        }
        Ok(RawTree {
            root: self.root.clone(),
            directories,
            files,
        })
    }
}

/// Same exact-Child custody as the R34 process control. First drain deadline is
/// permanent; failure/unproved reap retains the owned fixture before normalDrop.
struct OwnedChild {
    child: Option<Child>,
    fixture: Option<sinomo_testdir::TestDir>,
    root: PathBuf,
    reap_deadline: Option<Instant>,
    kill_before: Option<Instant>,
    identity: Option<ProcessIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ProcessIdentity {
    pid: u32,
    start: u64,
    group: u32,
    session: u32,
    uid: u32,
}
impl ProcessIdentity {
    fn read(pid: u32) -> TestResult<Self> {
        let path = PathBuf::from(format!("/proc/{pid}"));
        let uid = fs::metadata(&path)?.uid();
        let bytes = fs::read(path.join("stat"))?;
        if bytes.len() > 4096 {
            return Err("owned process stat exceeds bound".into());
        }
        let text = std::str::from_utf8(&bytes)?;
        let end = text
            .rfind(')')
            .ok_or("owned process stat has no command terminator")?;
        let fields = text[end + 1..].split_whitespace().collect::<Vec<_>>();
        Ok(Self {
            pid,
            start: fields
                .get(19)
                .ok_or("missing owned process start")?
                .parse()?,
            group: fields
                .get(2)
                .ok_or("missing owned process group")?
                .parse()?,
            session: fields
                .get(3)
                .ok_or("missing owned process session")?
                .parse()?,
            uid,
        })
    }
}
impl OwnedChild {
    fn spawn(fixture: sinomo_testdir::TestDir, command: &mut Command) -> TestResult<Self> {
        let root = fixture.path().to_owned();
        match command.spawn() {
            Ok(child) => {
                let mut owned = Self {
                    child: Some(child),
                    fixture: Some(fixture),
                    root,
                    reap_deadline: None,
                    kill_before: None,
                    identity: None,
                };
                let identity = (|| -> TestResult<ProcessIdentity> {
                    let identity = ProcessIdentity::read(owned.child.as_ref().unwrap().id())?;
                    if identity.group != identity.pid
                        || identity.start == 0
                        || identity.uid != DirectoryId::read(&owned.root)?.uid
                    {
                        return Err(
                            "spawned process did not retain its unique owned group/identity".into(),
                        );
                    }
                    Ok(identity)
                })();
                let identity = match identity {
                    Ok(identity) => identity,
                    Err(error) => {
                        owned.keep();
                        return Err(error);
                    }
                };
                owned.identity = Some(identity);
                Ok(owned)
            }
            Err(error) => {
                eprintln!(
                    "retained native process spawn failure: {}",
                    fixture.keep().display()
                );
                Err(error.into())
            }
        }
    }
    fn keep(&mut self) {
        if let Some(fixture) = self.fixture.take() {
            eprintln!(
                "retained native process fixture: {}",
                fixture.keep().display()
            );
        }
    }
    fn drain(&mut self, require_alive: bool) -> TestResult<ExitStatus> {
        let deadline = *self
            .reap_deadline
            .get_or_insert_with(|| Instant::now() + REAP_CAP);
        let child = self.child.as_mut().ok_or("native process already reaped")?;
        if let Some(status) = child.try_wait()? {
            self.child = None;
            if require_alive {
                return Err(format!("native child exited before ownedkill: {status}").into());
            }
            if Instant::now() > deadline {
                return Err("native child reaped after original5s cleanup".into());
            }
            return Ok(status);
        }
        if require_alive && self.kill_before.is_some_and(|cut| Instant::now() >= cut) {
            return Err("original process-ready window expired before owned SIGKILL".into());
        }
        child.kill()?;
        let killed_late = require_alive && self.kill_before.is_some_and(|cut| Instant::now() > cut);
        loop {
            if let Some(status) = child.try_wait()? {
                self.child = None;
                if killed_late {
                    return Err("owned SIGKILL returned after original process-ready window".into());
                }
                if Instant::now() > deadline {
                    return Err("native child reaped after original5s kill".into());
                }
                return Ok(status);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("native child reap unproved within original5s".into());
            }
            std::thread::sleep(remaining.min(Duration::from_millis(10)));
        }
    }
    fn kill_and_reap(&mut self, kill_before: Instant) -> TestResult<ExitStatus> {
        self.reap_deadline
            .get_or_insert_with(|| Instant::now() + REAP_CAP);
        self.kill_before = Some(kill_before);
        let result = (|| {
            let expected = self
                .identity
                .as_ref()
                .ok_or("owned process identity missing")?
                .clone();
            if ProcessIdentity::read(expected.pid)? != expected {
                return Err("owned process identity changed before SIGKILL".into());
            }
            let status = self.drain(true)?;
            // The native fixture uses threads, not detached node processes.
            // Reject any unexpected surviving member; never kill an ambient PID.
            let mut scanned = 0usize;
            for entry in fs::read_dir("/proc")? {
                scanned += 1;
                if scanned > 65_536 {
                    return Err("owned group drain scan exceeds bound".into());
                }
                let entry = entry?;
                let Some(pid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u32>().ok())
                else {
                    continue;
                };
                let metadata = match fs::metadata(entry.path()) {
                    Ok(m) => m,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                if metadata.uid() != expected.uid {
                    continue;
                }
                match ProcessIdentity::read(pid) {
                    Ok(actual) if actual.group == expected.group => {
                        return Err(
                            "owned native child group still has a process after reap".into()
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        if entry.path().exists() {
                            return Err(error);
                        }
                    }
                }
                if Instant::now() > self.reap_deadline.unwrap() {
                    return Err("owned group drain exceeded original5s".into());
                }
            }
            if Instant::now() > self.reap_deadline.unwrap() {
                return Err("owned group drain exceeded original5s".into());
            }
            Ok(status)
        })();
        if result.is_err() {
            self.keep();
        }
        result
    }
    fn ready(&mut self, deadline: Instant) -> TestResult<Ready> {
        loop {
            if Instant::now() >= deadline {
                return Err("native Ready exceeds original60s including spawn/nativework".into());
            }
            let child = self
                .child
                .as_mut()
                .ok_or("native child absent before Ready")?;
            if let Some(status) = child.try_wait()? {
                self.child = None;
                return Err(format!("native child exited before Ready: {status}").into());
            }
            let path = self.root.join("ready.sobj");
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    let ready: Ready = read_data(&path)?;
                    if ready.pid != child.id() {
                        return Err("native Ready PID differs from exactChild".into());
                    }
                    if Instant::now() >= deadline {
                        return Err("native Ready decoded after60s".into());
                    }
                    return Ok(ready);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            std::thread::sleep(
                Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.is_some()
            && let Err(error) = self.drain(false)
        {
            self.keep();
            eprintln!("native exactChild cleanup unproved: {error}");
            if !std::thread::panicking() {
                panic!("native exactChild cleanup unproved: {error}");
            }
        }
    }
}

fn pin_ancestors(
    owned: &Path,
    destination: &Path,
) -> TestResult<Vec<(PathBuf, File, DirectoryId)>> {
    if !destination.is_absolute()
        || destination
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err("native child path is not confined".into());
    }
    let relative = destination.strip_prefix(owned)?;
    let uid = DirectoryId::read(owned)?.uid;
    let mut path = owned.to_owned();
    let mut pins = Vec::new();
    for part in std::iter::once(None).chain(relative.components().map(Some)) {
        if let Some(part) = part {
            path.push(part.as_os_str());
        }
        let id = DirectoryId::read(&path)?;
        let file = File::open(&path)?;
        let opened = file.metadata()?;
        if id.uid != uid || !opened.is_dir() || DirectoryId::of(&opened) != id {
            return Err("native child ancestor changed/foreignowner".into());
        }
        pins.push((path.clone(), file, id));
    }
    Ok(pins)
}

fn save_report(path: &Path, report: &serde_json::Value) -> TestResult<()> {
    let bytes = serde_json::to_vec_pretty(report)?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err("retained process report exceeds4MiB".into());
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

#[test]
fn default_prepared_genesis_survives_owned_sigkill_and_same_fixture_reconstruction() {
    let fixture = sinomo_testdir::TestDir::new("retained-default-genesis-owned-sigkill");
    let root = fixture.path().to_owned();
    let child_tmp = root.join("child-tmp");
    fs::create_dir(&child_tmp).unwrap();
    let input = Input {
        owned_root: root.clone(),
        invocation: format!(
            "{}:held-default-genesis",
            root.file_name().unwrap().to_string_lossy()
        ),
    };
    let input_path = root.join("input.sobj");
    write_data(&input_path, &input).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", HELPER, "--ignored", "--nocapture"])
        .process_group(0)
        .env(INPUT_ENV, &input_path)
        .env("TMPDIR", &child_tmp)
        .env("SINOMO_TEST_KEEP_TMP", "1")
        .stdin(Stdio::piped())
        .stdout(File::create(root.join("child.stdout.log")).unwrap())
        .stderr(File::create(root.join("child.stderr.log")).unwrap());
    let started = Instant::now();
    let deadline = started + READY_CAP;
    let mut owned = OwnedChild::spawn(fixture, &mut command).unwrap();
    let mut report = serde_json::json!({
        "scope":"all three native fixture nodes died in one owned process; not one-A-loss with survivors",
        "format":"captured default P0 expected DATA only, no native admission or authenticated return deserialized",
        "limits":"normal OS-process loss, not power loss or quietness/full availability acceptance",
    });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> TestResult<()> {
        let ready = owned.ready(deadline)?;
        assert_eq!(ready.invocation, input.invocation);
        let expected = ready.manifest;
        assert_eq!(expected.format_version, 1);
        assert_eq!(expected.nodes.len(), 3);
        assert_eq!(expected.shard.primary_node_id, expected.primary);
        assert!(expected.shard.replica_node_ids.is_empty());
        assert_eq!(
            expected.original_record.status,
            DfsPreparedOperationStatus::Prepared
        );
        assert!(expected.original_record.committed_entry.is_none());
        assert!(!expected.original_frame.is_empty());
        assert!(expected.original_frame.len() <= DATA_CAP);
        let mut pins = pin_ancestors(&child_tmp, &expected.fixture_root)?;
        let fixture_id = DirectoryId::read(&expected.fixture_root)?;
        assert_eq!(
            (fixture_id.device, fixture_id.inode),
            expected.fixture_identity
        );
        for node in &expected.nodes {
            pins.extend(pin_ancestors(&expected.fixture_root, &node.data_root)?);
            let actual = DirectoryId::read(&node.data_root)?;
            assert_eq!((actual.device, actual.inode), node.data_identity);
            assert_eq!(node.data_root.file_name().unwrap(), node.node_id.as_str());
        }
        let a_data = expected
            .nodes
            .iter()
            .find(|node| node.node_id == expected.primary)
            .unwrap()
            .data_root
            .clone();
        let a_dir = SinomoDataDir::new(a_data.clone());
        let shard_id = DfsMetadataShardId::new(&expected.shard.shard_id);
        let shard_root = crate::local_dfs_metadata_shard_path(&a_dir, &shard_id);
        pins.extend(pin_ancestors(&a_data, &shard_root)?);
        let actual = DirectoryId::read(&shard_root)?;
        assert_eq!((actual.device, actual.inode), expected.shard_identity);
        report["expected_manifest"] = serde_json::to_value(&expected)?;
        report["same_request"] = serde_json::to_value(&ready.request)?;
        report["owned_process_identity"] = serde_json::to_value(&owned.identity)?;
        assert!(owned.child.as_mut().unwrap().try_wait()?.is_none());
        assert!(
            Instant::now() < deadline,
            "spawn, Ready and actual live root pins retain original60s"
        );
        let status = owned.kill_and_reap(deadline)?;
        assert_eq!(status.signal(), Some(9));
        report["actual_signal"] = serde_json::json!(status.signal());
        report["ready_and_kill_elapsed"] = serde_json::json!(format!("{:?}", started.elapsed()));
        for (path, file, id) in &pins {
            assert_eq!(DirectoryId::read(path)?, *id);
            assert_eq!(DirectoryId::of(&file.metadata()?), *id);
        }
        // No parent native initializer, validator or inspection reader has run yet.
        // Live node/fixture/shard identities are pinned; all regular physical
        // files are now captured after exactChild SIGKILL+group drain.
        let mut entries = 0;
        let mut bytes = 0;
        let raw_pins = PinnedTree::open(
            expected.fixture_root.clone(),
            fixture_id.uid,
            &mut entries,
            &mut bytes,
        )?;
        let raw = raw_pins.capture()?;
        assert!(
            raw.files.iter().any(|file| file
                .bytes
                .windows(expected.original_frame.len())
                .any(|window| window == expected.original_frame.as_slice())),
            "actual postkill file contains the full original frame bytes"
        );
        let encoded = crate::metadata_io::encode_sobj(&raw)?;
        if encoded.len() > RAW_CAP + DATA_CAP {
            return Err("raw retained tree encoding exceeds fixed bound".into());
        }
        crate::metadata_io::write_file_atomic(
            &root.join("raw-postkill-before-native-open.sobj"),
            &encoded,
        )?;
        report["raw_postkill"] = serde_json::json!({"files":raw.files.len(),"entries":entries,"bytes":bytes,
            "encoded_blake3":blake3::hash(&encoded).to_hex().to_string(),"captured_before_any_native_open":true});
        let mut missing = expected.clone();
        missing.nodes[0].data_root = expected.fixture_root.join("absent-node");
        let missing_result = DfsTestRuntime::validate_retained_genesis_fixture(&missing);
        report["missing_root_refusal"] = serde_json::to_value(&missing_result)?;
        assert!(missing_result.is_err());
        assert_eq!(
            raw_pins.capture()?,
            raw,
            "missing-root preflight cannot mutate actual retained files"
        );
        let mut foreign = expected.clone();
        foreign.nodes[0].data_identity = expected.nodes[1].data_identity;
        let foreign_result = DfsTestRuntime::validate_retained_genesis_fixture(&foreign);
        report["foreign_actual_node_identity_refusal"] = serde_json::to_value(&foreign_result)?;
        assert!(foreign_result.is_err());
        assert_eq!(
            raw_pins.capture()?,
            raw,
            "foreign-physical-identity preflight cannot mutate actual files"
        );
        let mut altered = expected.clone();
        altered
            .original_record
            .digest
            .operation_id
            .push_str("-altered");
        let altered_result = DfsTestRuntime::validate_retained_genesis_fixture(&altered);
        report["altered_original_refusal"] = serde_json::to_value(&altered_result)?;
        assert!(altered_result.is_err());
        assert_eq!(
            raw_pins.capture()?,
            raw,
            "altered-original preflight cannot mutate actual files"
        );
        let valid = DfsTestRuntime::validate_retained_genesis_fixture(&expected);
        report["exact_preflight"] = serde_json::to_value(&valid)?;
        valid?;
        assert_eq!(
            raw_pins.capture()?,
            raw,
            "successful preflight is not writer recovery"
        );
        // Only this constructor may rebuild process-local Fabric and native
        // retained Raft owners; failure is recorded without bootstrap fallback.
        let restored = DfsTestRuntime::reopen_retained_genesis_fixture(expected.clone());
        if let Err(error) = &restored {
            report["retained_native_constructor_error"] = serde_json::json!(error);
        }
        let (mut native, phase, startup) = restored?;
        report["retained_native_startup"] = serde_json::to_value(&startup)?;
        let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> TestResult<()> {
                assert_eq!(phase.original_record(), &expected.original_record);
                assert_eq!(phase.shard(), &expected.shard);
                let held = native.read_deferred_native_genesis(&phase);
                report["held_readback"] = serde_json::to_value(&held)?;
                assert_eq!(held?, expected.original_record);
                let held_frames = actual_frames(&shard_root)?;
                assert!(
                    held_frames
                        .iter()
                        .any(|frame| frame.bytes == expected.original_frame)
                );
                let legacy = native.metadata_shards()?;
                let markers = legacy_markers(
                    &native,
                    &expected
                        .nodes
                        .iter()
                        .map(|node| node.node_id.clone())
                        .collect::<Vec<_>>(),
                    &legacy,
                );
                let peer_transport = expected.nodes.iter().filter(|node|node.node_id!=expected.primary)
                .find_map(|node|native.component_context(&node.node_id).ok().and_then(|context|context.object_rpc_transport()))
                .ok_or("retained fixture has no actual ready peer context for endpoint observation")?;
                let route = peer_transport.availability(&expected.primary);
                let context = native.component_context(&expected.primary).map(drop);
                report["retained_primary_readiness"] =
                    serde_json::json!({"route":format!("{route:?}"),"context":context});
                let callable =
                    route == sinomo_core::network::RoutedAvailability::Candidate && context.is_ok();
                assert_eq!(
                    route == sinomo_core::network::RoutedAvailability::Candidate,
                    context.is_ok()
                );
                if let Err(error) = &startup {
                    let exact_deferral = error.contains(
                        crate::default_cluster::genesis::NATIVE_GENESIS_COMPLETION_DEFERRED,
                    );
                    let bounded_wait = callable
                        && error
                            == "mounted native DFS owners did not complete their current unparked startup generation before the existing network deadline";
                    if !exact_deferral && !bounded_wait {
                        return Err(
                            format!("unclassified actual retained startup error: {error}").into(),
                        );
                    }
                } else {
                    assert!(callable);
                }
                let DfsNetworkMessage::NamespaceRequest(body) =
                    &ready.request.routed_message.message
                else {
                    return Err("actual child request is not its stamped namespace mutation".into());
                };
                assert_eq!(
                    ready.request.routed_message.source_node_id,
                    expected.primary
                );
                assert_eq!(
                    ready.request.routed_message.target_node_id,
                    expected.primary
                );
                assert_eq!(body.authority_shards, vec![expected.shard.clone()]);
                let current = native
                    .metadata_shards()?
                    .into_iter()
                    .find(|shard| shard.shard_id == expected.shard.shard_id);
                assert_eq!(current.as_ref(), Some(&expected.shard));
                if callable {
                    let refused_call =
                        native_call(&native, &expected.primary, &ready.request, None);
                    report["held_same_request"] = serde_json::json!({"response":refused_call.response,"expired":refused_call.expired});
                    assert!(refused(&refused_call));
                    assert!(refused_call.response.as_ref().err().is_some_and(|error|error.contains(&expected.original_record.digest.operation_id)
                    && error.contains("remains prepared; mutation deferred before content or request binding")));
                    assert_eq!(
                        native.read_deferred_native_genesis(&phase)?,
                        expected.original_record
                    );
                    assert_eq!(actual_frames(&shard_root)?, held_frames);
                    assert_eq!(
                        legacy_markers(
                            &native,
                            &expected
                                .nodes
                                .iter()
                                .map(|node| node.node_id.clone())
                                .collect::<Vec<_>>(),
                            &legacy
                        ),
                        markers
                    );
                } else {
                    report["held_same_request"] =
                        serde_json::json!("not sent: actual endpoint/context not ready");
                }
                let completed = native.complete_deferred_native_genesis(&phase);
                report["same_original_completion"] = serde_json::to_value(&completed)?;
                let completed = completed?;
                assert_eq!(completed.digest, expected.original_record.digest);
                assert_eq!(completed.payload, expected.original_record.payload);
                assert_eq!(completed.status, DfsPreparedOperationStatus::Committed);
                assert!(completed.committed_entry.is_some());
                drop(peer_transport);
                native.stop_node(&expected.primary)?;
                let reopened = native.reopen_node(expected.primary.clone());
                report["ordinary_completed_reopen"] = serde_json::to_value(&reopened)?;
                reopened?;
                drop(native.component_context(&expected.primary)?);
                let prefix = match &expected.shard.assignment {
                    crate::DfsMetadataShardAssignment::PathPrefix { prefix } => prefix,
                    _ => return Err("retained singleton lost its original prefix".into()),
                };
                let synced = native.sync_completed_native_genesis(&phase, prefix);
                report["exact_assigned_sync"] = serde_json::to_value(&synced)?;
                let synced = synced?;
                assert_eq!(synced.target.expected_shard.as_ref(), Some(&expected.shard));
                assert_eq!(synced.acknowledged_node_ids, vec![expected.primary.clone()]);
                assert_eq!(synced.quorum_size, 1);
                let applied = native_call(&native, &expected.primary, &ready.request, None);
                report["same_request_after_sync"] =
                    serde_json::json!({"response":applied.response,"expired":applied.expired});
                assert!(!applied.expired);
                assert!(applied.response.as_ref().map_err(Clone::clone)?.outbound_messages.iter().any(|frame|
                matches!(&frame.message,DfsNetworkMessage::NamespaceResponse(reply) if reply.request_id==body.request_id && matches!(reply.result,DfsNamespaceResult::Applied))));
                let DfsNamespaceOperation::Mkdir { path } = &body.operation else {
                    unreachable!()
                };
                let read_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let actual = loop {
                    let value = crate::local_state::with_current_owned_dfs_shard_inspection(
                        native.data_dir(&expected.primary),
                        &shard_id,
                        |backend| {
                            backend
                                .get_object_by_path(&expected.shard.file_system_id, path)
                                .map_err(crate::DfsError::from)
                        },
                    )?;
                    if let Some(Some(object)) = value {
                        break object;
                    }
                    if Instant::now() >= read_deadline {
                        return Err("actual applied directory unavailable at original5s".into());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                };
                assert_eq!(&actual.path, path);
                assert_eq!(actual.kind, DfsObjectKind::Directory);
                report["actual_post_object"] = serde_json::to_value(&actual)?;
                assert_eq!(native.observation_dropped_count(), 0);
                Ok(())
            },
        ));
        let closed = native.close();
        drop(phase);
        report["normal_reconstructed_close"] = serde_json::to_value(&closed)?;
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
        checked.unwrap()?;
        closed?;
        Ok(())
    }));
    report["control_outcome"] = match &result {
        Ok(Ok(())) => serde_json::json!("completed"),
        Ok(Err(error)) => serde_json::json!(error.to_string()),
        Err(_) => serde_json::json!("panic: original assertion retained"),
    };
    let saved = save_report(
        &root.join("retained-default-genesis-process-report.json"),
        &report,
    );
    if !matches!(&result, Ok(Ok(()))) || saved.is_err() {
        owned.keep();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    result.unwrap().unwrap();
    saved.unwrap();
}

#[test]
#[ignore = "owned whole-native-fixture process helper; invoked only by parent"]
fn retained_default_genesis_process_child() -> TestResult<()> {
    let input_path = PathBuf::from(std::env::var_os(INPUT_ENV).ok_or("parent input missing")?);
    let input: Input = read_data(&input_path)?;
    if input_path != input.owned_root.join("input.sobj")
        || std::env::var_os("TMPDIR").map(PathBuf::from) != Some(input.owned_root.join("child-tmp"))
        || input.invocation
            != format!(
                "{}:held-default-genesis",
                input
                    .owned_root
                    .file_name()
                    .ok_or("owned root name missing")?
                    .to_string_lossy()
            )
    {
        return Err("owned process input/invocation paths changed".into());
    }
    DirectoryId::read(&input.owned_root)?;
    let (native, nodes) = DfsTestRuntime::start_cluster("retained-default-genesis-process", 3)?;
    let primary = native.root_shard()?.primary_node_id;
    let prefix = crate::sifs_node_path(&primary);
    let phase = native.begin_deferred_native_genesis("process-held-native-node-scope", &prefix)?;
    let shard = phase.shard().clone();
    assert!(shard.replica_node_ids.is_empty());
    assert_eq!(
        phase.original_record().status,
        DfsPreparedOperationStatus::Prepared
    );
    let operation = DfsNamespaceOperation::Mkdir {
        path: format!("{prefix}/same-request-after-whole-fixture-loss"),
    };
    let stamps=crate::NetworkedDfsService::<crate::DfsNodeTransport>::affected_namespace_shards_from_snapshot(&shard.file_system_id,&operation,&native.metadata_shards()?)?;
    assert_eq!(stamps, vec![shard.clone()]);
    let request = command(
        &primary,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: format!("retained-genesis-mkdir-{}", sinomo_core::Id::generate()),
            file_system_id: shard.file_system_id.clone(),
            requesting_node_id: primary.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: stamps,
            operation,
        }),
        495,
    );
    let manifest = native.capture_retained_genesis_fixture(&phase)?;
    assert_eq!(manifest.nodes.len(), nodes.len());
    assert_eq!(&manifest.original_record, phase.original_record());
    let ready = Ready {
        pid: std::process::id(),
        invocation: input.invocation,
        manifest,
        request,
    };
    write_data(&input.owned_root.join("ready.sobj"), &ready)?;
    // EOF is a failed child outcome. Keeping the genuine runtime/negative
    // phase alive needs no authority or five-second operation renewal.
    let mut byte = [0u8];
    let waited = io::stdin().read_exact(&mut byte);
    std::hint::black_box((&native, &phase));
    let closed = native.close();
    drop(phase);
    Err(format!(
        "parent did not SIGKILL the actual whole fixture: stdin={waited:?}, close={closed:?}"
    )
    .into())
}
