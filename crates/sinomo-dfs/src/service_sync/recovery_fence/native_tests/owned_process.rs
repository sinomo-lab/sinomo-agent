//! Whole-native-fixture SIGKILL cuts; persisted bytes remain historical DATA.
#![cfg(target_os = "linux")]
use super::*;
use crate::metadata_api::DfsSelectedLocalRecoveryFence;
use crate::metadata_api::selected_local_recovery::{
    SelectedRecoveryWriterCut, SelectedRecoveryWriterHook,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io;
use std::os::unix::{
    fs::{FileExt, MetadataExt},
    process::{CommandExt, ExitStatusExt},
};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};

const HELPER: &str = "service_sync::recovery_fence::native_tests::owned_process::native_local_recovery_fence_process_child";
const INPUT_ENV: &str = "SINOMO_DFS_LOCAL_RECOVERY_PROCESS_INPUT";
const READY_CAP: Duration = Duration::from_secs(60);
const REAP_CAP: Duration = Duration::from_secs(5);
const DATA_CAP: usize = 2 * 1024 * 1024;
const RAW_CAP: usize = 16 * 1024 * 1024;
const ENTRY_CAP: usize = 512;
type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Phase {
    BeforePublication,
    AfterNamespaceSync,
}
impl Phase {
    fn cut(self) -> SelectedRecoveryWriterCut {
        match self {
            Self::BeforePublication => SelectedRecoveryWriterCut::BeforeMarkerPublication,
            Self::AfterNamespaceSync => SelectedRecoveryWriterCut::AfterMarkerNamespaceSync,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    owned_root: PathBuf,
    invocation: String,
    phase: Phase,
}

// This copies actual observed writer DATA only. No native owner/context,
// reservation verification result, callback return or ACK is deserialized.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    pid: u32,
    invocation: String,
    phase: Phase,
    b_data: PathBuf,
    marker_path: PathBuf,
    fence: DfsSelectedLocalRecoveryFence,
    stored_marker_bytes: Option<Vec<u8>>,
    primary_marker_path: PathBuf,
    primary_marker_bytes: Vec<u8>,
    // Parent spawn precedes helper entry, making parent_start + this elapsed
    // + five seconds an EARLIER bound than the actual child's install cutoff.
    install_elapsed_nanos: u64,
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
            return Err("original local operation expired before owned SIGKILL".into());
        }
        child.kill()?;
        let killed_late = require_alive && self.kill_before.is_some_and(|cut| Instant::now() > cut);
        loop {
            if let Some(status) = child.try_wait()? {
                self.child = None;
                if killed_late {
                    return Err(
                        "owned SIGKILL returned after original local operation deadline".into(),
                    );
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

fn pin_file(path: PathBuf, uid: u32) -> TestResult<PinnedFile> {
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.uid() != uid {
        return Err("native observed file is not an owned regular file".into());
    }
    let file = File::open(&path)?;
    let physical = FileId::of(&metadata);
    if FileId::of(&file.metadata()?) != physical {
        return Err("native observed file changed while pinning".into());
    }
    Ok(PinnedFile {
        path,
        file,
        physical,
    })
}

fn capture_file(pin: &PinnedFile) -> TestResult<RawFile> {
    let before = fs::symlink_metadata(&pin.path)?;
    if before.file_type().is_symlink()
        || !before.is_file()
        || FileId::of(&before) != pin.physical
        || FileId::of(&pin.file.metadata()?) != pin.physical
    {
        return Err("native observed file was replaced or changed".into());
    }
    let bytes = read_pinned(&pin.file, pin.physical.size, DATA_CAP)?;
    if FileId::of(&fs::symlink_metadata(&pin.path)?) != pin.physical {
        return Err("native observed file changed during capture".into());
    }
    Ok(RawFile {
        relative: pin.path.clone(),
        physical: pin.physical.clone(),
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        bytes,
    })
}

fn run_arm(phase: Phase) {
    let fixture = sinomo_testdir::TestDir::new("native-local-recovery-fence-owned-sigkill");
    let root = fixture.path().to_owned();
    let child_tmp = root.join("child-tmp");
    fs::create_dir(&child_tmp).unwrap();
    let input = Input {
        owned_root: root.clone(),
        phase,
        invocation: format!("{}:{phase:?}", root.file_name().unwrap().to_string_lossy()),
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
        .stdin(Stdio::null())
        .stdout(File::create(root.join("child.stdout.log")).unwrap())
        .stderr(File::create(root.join("child.stderr.log")).unwrap());
    // Includes spawn and all native setup. No test-worker override is added.
    let started = Instant::now();
    let ready_deadline = started + READY_CAP;
    let mut owned = OwnedChild::spawn(fixture, &mut command).unwrap();
    let ready = owned.ready(ready_deadline).unwrap();
    let ready_elapsed = started.elapsed();
    assert_eq!(ready.invocation, input.invocation);
    assert_eq!(ready.phase, phase);
    ready.fence.validate().unwrap(); // structural DATA validation, not authority
    assert_eq!(
        ready.b_data.file_name().unwrap(),
        ready.fence.acceptor_node_id.as_str()
    );
    assert_ne!(
        ready.fence.acceptor_node_id,
        ready.fence.reservation.serving_shard.primary_node_id
    );
    assert!(
        ready
            .fence
            .reservation
            .serving_shard
            .replica_node_ids
            .contains(&ready.fence.acceptor_node_id)
    );
    assert_eq!(ready.fence.physical.data_root, ready.b_data);
    let id = DfsMetadataShardId::new(&ready.fence.reservation.serving_shard.shard_id);
    let data = sinomo_core::local::SinomoDataDir::new(&ready.b_data);
    let shard_root = crate::local_dfs_metadata_shard_path(&data, &id);
    assert_eq!(ready.fence.physical.shard_root, shard_root);
    assert_eq!(
        ready.marker_path,
        shard_root.join("journal/membership-handoff-write-freeze.sobj")
    );
    let primary = &ready.fence.reservation.serving_shard.primary_node_id;
    let primary_data = ready.b_data.parent().unwrap().join(primary.as_str());
    assert_eq!(
        ready.primary_marker_path,
        crate::local_dfs_metadata_shard_path(
            &sinomo_core::local::SinomoDataDir::new(&primary_data),
            &id,
        )
        .join("journal/membership-handoff-write-freeze.sobj")
    );
    let elapsed = Duration::from_nanos(ready.install_elapsed_nanos);
    assert!(elapsed < READY_CAP);
    // Parent spawn precedes helper entry. This is conservatively EARLIER
    // than the child's actual five-second Instant, never a renewed budget.
    let kill_before = started
        .checked_add(elapsed)
        .unwrap()
        .checked_add(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap()
        .min(ready_deadline);
    assert!(
        Instant::now() < kill_before,
        "original local install deadline expired before capture"
    );
    let mut ancestor_pins = pin_ancestors(&child_tmp, &shard_root).unwrap();
    ancestor_pins
        .extend(pin_ancestors(&child_tmp, ready.primary_marker_path.parent().unwrap()).unwrap());
    let uid = DirectoryId::read(&root).unwrap().uid;
    let mut entries = 0;
    let mut bytes = 0;
    let pinned = PinnedTree::open(shard_root.clone(), uid, &mut entries, &mut bytes).unwrap();
    let primary_marker = pin_file(ready.primary_marker_path.clone(), uid).unwrap();
    let native_owner = pin_file(ready.b_data.join("daemon/data-owner.lock"), uid).unwrap();
    assert_eq!(
        native_owner.physical.device,
        ready.fence.physical.owner_device
    );
    assert_eq!(
        native_owner.physical.inode,
        ready.fence.physical.owner_inode
    );
    let data_identity = DirectoryId::read(&ready.b_data).unwrap();
    assert_eq!(data_identity.device, ready.fence.physical.data_device);
    assert_eq!(data_identity.inode, ready.fence.physical.data_inode);
    let shard_identity = DirectoryId::read(&shard_root).unwrap();
    assert_eq!(shard_identity.device, ready.fence.physical.shard_device);
    assert_eq!(shard_identity.inode, ready.fence.physical.shard_inode);
    let before = pinned.capture().unwrap();
    let primary_before = capture_file(&primary_marker).unwrap();
    let owner_before = capture_file(&native_owner).unwrap();
    assert_eq!(primary_before.bytes, ready.primary_marker_bytes);
    let relative_marker = Path::new("journal/membership-handoff-write-freeze.sobj");
    match phase {
        Phase::BeforePublication => {
            assert!(ready.stored_marker_bytes.is_none());
            assert!(
                !before
                    .files
                    .iter()
                    .any(|file| file.relative == relative_marker)
            );
            assert_eq!(
                fs::symlink_metadata(&ready.marker_path).unwrap_err().kind(),
                io::ErrorKind::NotFound
            );
        }
        Phase::AfterNamespaceSync => {
            let expected = ready.stored_marker_bytes.as_ref().unwrap();
            assert!(!expected.is_empty());
            assert_eq!(
                &before
                    .files
                    .iter()
                    .find(|file| file.relative == relative_marker)
                    .unwrap()
                    .bytes,
                expected
            );
        }
    }
    crate::metadata_io::write_file_atomic(
        &root.join("raw-alive.sobj"),
        &crate::metadata_io::encode_sobj(&before).unwrap(),
    )
    .unwrap();
    assert_eq!(pinned.capture().unwrap(), before);
    assert_eq!(capture_file(&primary_marker).unwrap(), primary_before);
    assert_eq!(capture_file(&native_owner).unwrap(), owner_before);
    assert_eq!(
        ProcessIdentity::read(ready.pid).unwrap(),
        owned.identity.clone().unwrap()
    );
    assert!(owned.child.as_mut().unwrap().try_wait().unwrap().is_none());
    assert!(
        Instant::now() < kill_before,
        "pinning cannot extend the original local operation deadline"
    );
    let before_kill_elapsed = started.elapsed();
    let kill_started = Instant::now();
    let status = owned.kill_and_reap(kill_before).unwrap();
    let kill_elapsed = kill_started.elapsed();
    assert_eq!(
        status.signal(),
        Some(9),
        "normal/early exit does not prove this cut"
    );
    for (path, file, expected) in &ancestor_pins {
        assert_eq!(&DirectoryId::read(path).unwrap(), expected);
        assert_eq!(&DirectoryId::of(&file.metadata().unwrap()), expected);
    }
    // No writer, recovery, native opener or ACK certifier is invoked. Capture
    // the original physical bytes FIRST; even later decoding would be DATA only.
    let after = pinned.capture().unwrap();
    crate::metadata_io::write_file_atomic(
        &root.join("raw-postkill.sobj"),
        &crate::metadata_io::encode_sobj(&after).unwrap(),
    )
    .unwrap();
    assert_eq!(after, before);
    assert_eq!(capture_file(&primary_marker).unwrap(), primary_before);
    assert_eq!(capture_file(&native_owner).unwrap(), owner_before);
    write_data(
        &root.join("process-witness.sobj"),
        owned.identity.as_ref().unwrap(),
    )
    .unwrap();
    eprintln!(
        "LOCAL_RECOVERY_PROCESS {phase:?} pid={} group={} start={} signal={:?} spawn_to_ready={ready_elapsed:?} spawn_to_kill={before_kill_elapsed:?} kill_reap_group={kill_elapsed:?} files={} bytes={} root={}",
        ready.pid,
        owned.identity.as_ref().unwrap().group,
        owned.identity.as_ref().unwrap().start,
        status.signal(),
        after.files.len(),
        bytes,
        root.display()
    );
}

#[test]
fn native_local_recovery_fence_cuts_survive_owned_sigkill_without_a_result_ack() {
    run_arm(Phase::BeforePublication);
    run_arm(Phase::AfterNamespaceSync);
}

#[test]
#[ignore = "whole native fixture process helper; owned only by SIGKILL parent"]
fn native_local_recovery_fence_process_child() -> TestResult<()> {
    let child_started = Instant::now();
    let input_path =
        PathBuf::from(std::env::var_os(INPUT_ENV).ok_or("explicit parent input required")?);
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
                    .ok_or("owned root name missing")?
                    .to_string_lossy(),
                input.phase
            )
    {
        return Err("owned process input paths/invocation differ".into());
    }
    let name = match input.phase {
        Phase::BeforePublication => "native-recovery-process-before",
        Phase::AfterNamespaceSync => "native-recovery-process-after-sync",
    };
    let mut fixture = NativeFenceFixture::new(name, true);
    fixture.seed("/services/native-local-recovery-fence.bin");
    fixture.no_cooperative_marker();
    let request = fixture.request();
    fixture.reserve(&request);
    let b_data = fixture
        .runtime
        .data_dir(&fixture.acceptor)
        .root()
        .to_owned();
    let shard = fixture.shard_id();
    let shard_root =
        crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&fixture.acceptor), &shard);
    let primary_marker_path = crate::local_dfs_metadata_shard_path(
        fixture.runtime.data_dir(&fixture.shard.primary_node_id),
        &shard,
    )
    .join("journal/membership-handoff-write-freeze.sobj");
    let primary_marker_bytes = fs::read(&primary_marker_path)?;
    let install_started = Instant::now();
    let deadline = install_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let ready_path = input.owned_root.join("ready.sobj");
    let phase = input.phase;
    let hook = SelectedRecoveryWriterHook::install(
        shard_root,
        request.clone(),
        phase.cut(),
        deadline,
        move |observed| {
            let invalid = |detail: String| crate::MetadataBackendError::Serde(detail);
            if observed.cut != phase.cut() {
                return Err(invalid("wrong selected writer cut".into()));
            }
            if ready_path.exists() {
                return Err(invalid("selected writer cut emitted Ready twice".into()));
            }
            let elapsed = u64::try_from(install_started.duration_since(child_started).as_nanos())
                .map_err(|error| invalid(error.to_string()))?;
            let ready = Ready {
                pid: std::process::id(),
                invocation: input.invocation.clone(),
                phase,
                b_data: b_data.clone(),
                marker_path: observed.marker_path.clone(),
                fence: observed.fence.clone(),
                stored_marker_bytes: observed.stored_marker_bytes.clone(),
                primary_marker_path: primary_marker_path.clone(),
                primary_marker_bytes: primary_marker_bytes.clone(),
                install_elapsed_nanos: elapsed,
            };
            write_data(&ready_path, &ready).map_err(|error| invalid(error.to_string()))
        },
    )?;
    let result = fixture
        .service()
        .install_selected_local_recovery_fence(&request, deadline);
    drop(hook);
    drop(fixture.service.take());
    drop(fixture.control.take());
    let closed = fixture.runtime.close();
    Err(format!("parent did not SIGKILL while actual writer cut was held: result={result:?}, close={closed:?}").into())
}
