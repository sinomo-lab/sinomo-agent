//! Whole-fixture process/file durability, not a surviving-cluster failure test.
use super::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::{
    fs::{FileExt, MetadataExt},
    process::ExitStatusExt,
};
use std::process::{Child, Command, ExitStatus, Stdio};

const HELPER: &str = "service_sync::native_selected_data_tests::native_retired_repair_intake_tests::native_intake_process_tests::normal_native_intake_process_child";
const INPUT_ENV: &str = "SINOMO_DFS_NATIVE_INTAKE_PROCESS_INPUT";
const READY_CAP: Duration = Duration::from_secs(60);
const REAP_CAP: Duration = Duration::from_secs(5);
const DATA_CAP: usize = 64 * 1024;
const RAW_CAP: usize = 16 * 1024 * 1024;
const ENTRY_CAP: usize = 512;
type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Phase {
    Ack,
    Compacted,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    owned_root: PathBuf,
    invocation: String,
    phase: Phase,
}

/// Only actual stored DATA is serialized. Native admission, the capability,
/// its reference and all runtime/physical-owner handles remain inside child.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    pid: u32,
    invocation: String,
    phase: Phase,
    a_data: PathBuf,
    queue_snapshot: PathBuf,
    birth: crate::repair_queue_store::RepairOriginalBirth,
    record: RetainedNativeOwnedRepairCompletion,
    body: Vec<u8>,
    queue_head: u64,
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
        return Err("process input/Ready exceeds64KiB".into());
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
}
impl OwnedChild {
    fn spawn(fixture: sinomo_testdir::TestDir, command: &mut Command) -> TestResult<Self> {
        let root = fixture.path().to_owned();
        match command.spawn() {
            Ok(child) => Ok(Self {
                child: Some(child),
                fixture: Some(fixture),
                root,
                reap_deadline: None,
            }),
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
        child.kill()?;
        loop {
            if let Some(status) = child.try_wait()? {
                self.child = None;
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
    fn kill_and_reap(&mut self) -> TestResult<ExitStatus> {
        let result = self.drain(true);
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

fn run_arm(phase: Phase) {
    let fixture = sinomo_testdir::TestDir::new("normal-native-intake-owned-sigkill");
    let root = fixture.path().to_owned();
    let child_tmp = root.join("child-tmp");
    fs::create_dir(&child_tmp).unwrap();
    let input = Input {
        owned_root: root.clone(),
        invocation: format!("{}:{phase:?}", root.file_name().unwrap().to_string_lossy()),
        phase,
    };
    let input_path = root.join("input.sobj");
    write_data(&input_path, &input).unwrap();
    let stdout = File::create(root.join("child.stdout.log")).unwrap();
    let stderr = File::create(root.join("child.stderr.log")).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", HELPER, "--ignored", "--nocapture"])
        .env(INPUT_ENV, &input_path)
        .env("TMPDIR", &child_tmp)
        .env("SINOMO_TEST_KEEP_TMP", "1")
        .stdin(Stdio::piped())
        .stdout(stdout)
        .stderr(stderr);
    let started = Instant::now();
    let deadline = started + READY_CAP;
    let mut owned = OwnedChild::spawn(fixture, &mut command).unwrap();
    let ready = owned.ready(deadline).unwrap();
    assert_eq!(ready.invocation, input.invocation);
    assert_eq!(ready.phase, phase);
    assert_eq!(
        ready.record.material().original.routed(),
        ready.birth.original
    );
    assert_eq!(ready.record.material().attachment, ready.birth.attachment);
    assert_eq!(ready.record.payload_root(), &ready.birth.root);
    assert!(ready.birth.observed.is_empty() && ready.birth.closed_at_sequence.is_none());
    assert_eq!(ready.body, vec![0x5a; 4096]);
    assert_eq!(
        ready.record.material().attachment.total_size_bytes,
        ready.body.len() as u64
    );
    let command = &ready.record.material().original.command;
    assert_eq!(
        ready.a_data.file_name().unwrap(),
        ready.birth.original.source_node_id.as_str()
    );
    let data_dir = sinomo_core::local::SinomoDataDir::new(ready.a_data.clone());
    assert_eq!(
        ready.queue_snapshot,
        crate::default_dfs_repair_attempt_snapshot_path(&data_dir)
    );
    let mut ancestor_pins = pin_ancestors(&child_tmp, &ready.a_data).unwrap();
    let uid = DirectoryId::read(&root).unwrap().uid;
    let mut entries = 0;
    let mut bytes = 0;
    let roots = [
        ready.queue_snapshot.parent().unwrap().to_owned(),
        data_dir.dfs_dir().join("content"),
        crate::local_dfs_metadata_shard_path(
            &data_dir,
            &DfsMetadataShardId::new(command.authority_shard.as_ref().unwrap().shard_id.clone()),
        ),
    ];
    let pinned: Vec<_> = roots
        .into_iter()
        .map(|path| {
            ancestor_pins.extend(pin_ancestors(&ready.a_data, &path).unwrap());
            PinnedTree::open(path, uid, &mut entries, &mut bytes).unwrap()
        })
        .collect();
    let before: Vec<_> = pinned.iter().map(|tree| tree.capture().unwrap()).collect();
    crate::metadata_io::write_file_atomic(
        &root.join("raw-alive.sobj"),
        &crate::metadata_io::encode_sobj(&before).unwrap(),
    )
    .unwrap();
    assert_eq!(
        pinned
            .iter()
            .map(|tree| tree.capture().unwrap())
            .collect::<Vec<_>>(),
        before,
        "actual native physical files must be stable while ownedchild is alive"
    );
    assert!(owned.child.as_mut().unwrap().try_wait().unwrap().is_none());
    assert!(
        Instant::now() < deadline,
        "complete live identity/raw capture fits original60s readiness"
    );
    let ready_elapsed = started.elapsed();
    let kill_started = Instant::now();
    let status = owned.kill_and_reap().unwrap();
    let kill_elapsed = kill_started.elapsed();
    assert_eq!(status.signal(), Some(9));
    for (path, file, id) in &ancestor_pins {
        assert_eq!(&DirectoryId::read(path).unwrap(), id);
        assert_eq!(&DirectoryId::of(&file.metadata().unwrap()), id);
    }
    // Nothing above/below opens a native or content writer. Capture unchanged
    // physical originals BEFORE the queue's pure forensic DATA decoder.
    let after: Vec<_> = pinned.iter().map(|tree| tree.capture().unwrap()).collect();
    crate::metadata_io::write_file_atomic(
        &root.join("raw-postkill.sobj"),
        &crate::metadata_io::encode_sobj(&after).unwrap(),
    )
    .unwrap();
    assert_eq!(
        after, before,
        "queue/content/metadata bytes and original physical files survived owned SIGKILL"
    );
    let inspected = RetainedNativeOwnedRepairCompletion::inspect_expected_process_data_for_test(
        &ready.queue_snapshot,
        &ready.record,
    )
    .unwrap();
    assert_eq!(inspected.retained(), &ready.record);
    assert_eq!(inspected.original_birth(), &ready.birth);
    assert!(
        inspected.original_birth().observed.is_empty()
            && inspected.original_birth().closed_at_sequence.is_none()
    );
    assert_eq!(inspected.log_head(), ready.queue_head);
    assert_eq!(
        inspected.log_valid_bytes(),
        inspected.log_file_bytes(),
        "no unacknowledged/torn suffix is repaired or accepted as a pass"
    );
    if phase == Phase::Compacted {
        assert!(inspected.snapshot_floor() >= ready.record.retained_at_sequence());
    }
    assert_eq!(
        pinned
            .iter()
            .map(|tree| tree.capture().unwrap())
            .collect::<Vec<_>>(),
        after,
        "forensic DATA parsing cannot alter any file or mint a writer ACK"
    );
    eprintln!(
        "NATIVE_INTAKE_PROCESS {phase:?} pid={} Ready={ready_elapsed:?} kill/reap={:?} signal={:?} files={} bytes={} queuehead={} root={}",
        ready.pid,
        kill_elapsed,
        status.signal(),
        pinned.iter().map(|tree| tree.files.len()).sum::<usize>(),
        bytes,
        inspected.log_head(),
        root.display()
    );
}

#[test]
fn normal_native_intake_data_survives_owned_sigkill() {
    run_arm(Phase::Ack);
    run_arm(Phase::Compacted);
}

#[test]
#[ignore = "whole native fixture process helper; owned only by SIGKILL parent"]
fn normal_native_intake_process_child() -> TestResult<()> {
    let input_path = PathBuf::from(
        std::env::var_os(INPUT_ENV)
            .ok_or("native process helper requires parent's explicit input")?,
    );
    let input: Input = read_data(&input_path)?;
    DirectoryId::read(&input.owned_root)?;
    if input_path != input.owned_root.join("input.sobj")
        || std::env::var_os("TMPDIR").map(PathBuf::from) != Some(input.owned_root.join("child-tmp"))
        || input.invocation
            != format!(
                "{}:{:?}",
                input
                    .owned_root
                    .file_name()
                    .ok_or("owned root has no name")?
                    .to_string_lossy(),
                input.phase
            )
    {
        return Err("native process input paths/invocation changed".into());
    }
    let name = match input.phase {
        Phase::Ack => "native-intake-process-ack",
        Phase::Compacted => "native-intake-process-compacted",
    };
    let fixture = genuine_native_original(name);
    let material = fixture.birth.original_wire_material()?;
    let before = metadata_facts(&fixture);
    let selection = crate::runtime_host::select_native_original_repair_custody(
        fixture.runtime.data_dir(&fixture.shard.primary_node_id),
        &material,
    )?;
    let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    let completion = held_normal_return(&fixture, &selection, deadline);
    let wires = repair_wire_calls(&fixture);
    selection.allow_intake();
    let reference = wait_retained(&fixture, &selection, deadline);
    let source = queue(&fixture);
    let record = source.retained_native_owned_repair_completion(&reference)?;
    assert_record(&record, &completion);
    assert_no_effects(&fixture, &before);
    drop(completion);
    drop(selection);
    tick(&fixture, deadline);
    if input.phase == Phase::Compacted {
        source.compact_if_needed(DfsRepairQueueCompactionOptions::force())?;
    }
    assert_eq!(
        source.retained_native_owned_repair_completion(&reference)?,
        record
    );
    assert_eq!(repair_wire_calls(&fixture), wires);
    assert_no_effects(&fixture, &before);
    let birth = source
        .original_repair(&fixture.birth.original)?
        .ok_or("actual original absent after native ACK")?;
    let a_data = fixture
        .runtime
        .data_dir(&fixture.shard.primary_node_id)
        .root()
        .to_owned();
    let layout = fixture
        .runtime
        .selected_original_data_layout(&fixture.shard.primary_node_id)
        .unwrap();
    let queue_snapshot = layout.queue_layout().snapshot_path().to_owned();
    let (_, body) = fixture
        .runtime
        .client(&fixture.shard.primary_node_id)
        .unwrap()
        .content()
        .read_object_version(
            material.original.command.object_id.as_str(),
            material.original.command.version.as_str(),
        )?;
    assert_eq!(body, vec![0x5a; 4096]);
    let ready = Ready {
        pid: std::process::id(),
        invocation: input.invocation,
        phase: input.phase,
        a_data,
        queue_snapshot,
        birth,
        record,
        body,
        queue_head: source.view()?.head_sequence,
    };
    write_data(&input.owned_root.join("ready.sobj"), &ready)?;
    // Keep ALL three native nodes/live stores in this child. Only parent's
    // actual SIGKILL is a positive witness; EOF/normal teardown fails.
    let mut byte = [0u8];
    let waited = io::stdin().read_exact(&mut byte);
    std::hint::black_box((&fixture, &source, &layout));
    Err(format!("parent did not SIGKILL actual native fixture: stdin={waited:?}").into())
}
