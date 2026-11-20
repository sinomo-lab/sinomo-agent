//! Linux mount targets which do not exist in their backing directory need a
//! guest directory entry before Bubblewrap can attach the native SIFS mount.
mod metadata;
mod planning;
mod router;

use super::{NativeMountProjection, SandboxLocalExecutionLease, SandboxMountMode};
use fuser::{BackgroundSession, Config, MountOption};
use router::{Branch, Router};
use std::{
    collections::HashMap,
    fs, io,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    process::Command,
    sync::{Arc, LazyLock, Mutex, Weak},
};

struct Bind {
    source: PathBuf,
    target: PathBuf,
    flag: &'static str,
}

type RouterSlot = Mutex<Weak<SharedView>>;
#[derive(Eq, Hash, PartialEq)]
struct ViewKey {
    instance: PathBuf,
    mounts: Vec<(PathBuf, PathBuf, bool)>,
}
static VIEWS: LazyLock<Mutex<HashMap<ViewKey, Weak<RouterSlot>>>> = LazyLock::new(Default::default);

struct SharedView {
    binds: Vec<Bind>,
    // Drop the owner-only mount before releasing its command journal. A peer
    // can keep both alive after the command that first installed them exits.
    _session: BackgroundSession,
    _owner: Arc<SandboxLocalExecutionLease>,
    _slot: Arc<RouterSlot>,
}

pub(super) struct MountView {
    direct: Vec<Bind>,
    shared: Option<Arc<SharedView>>,
}
impl MountView {
    pub fn prepare(
        projection: &NativeMountProjection,
        lease: &Arc<SandboxLocalExecutionLease>,
    ) -> io::Result<Self> {
        let stage = lease
            .native_mount_view_directory()
            .ok_or_else(|| io::Error::other("missing native execution lease"))?;
        let commands = stage
            .parent()
            .and_then(std::path::Path::parent)
            .expect("command journal in native-commands")
            .canonicalize()?;
        let mut mounts: Vec<_> = projection
            .mounts
            .iter()
            .map(|m| {
                (
                    m.guest_path.clone(),
                    m.source_host_path.clone(),
                    m.mode == SandboxMountMode::ReadOnly,
                )
            })
            .collect();
        mounts.sort_unstable();
        let key = ViewKey {
            instance: commands,
            mounts,
        };
        let slot = {
            let mut views = VIEWS.lock().unwrap();
            views.retain(|_, slot| slot.strong_count() != 0);
            let entry = views.entry(key).or_default();
            entry.upgrade().unwrap_or_else(|| {
                let slot = Arc::new(RouterSlot::default());
                *entry = Arc::downgrade(&slot);
                slot
            })
        };
        let mut current = slot.lock().unwrap();
        if let Some(shared) = current.upgrade() {
            return Ok(Self {
                direct: vec![],
                shared: Some(shared),
            });
        }
        let planning::Plan { mut binds, parents } = planning::Plan::new(projection)?;
        if parents.is_empty() {
            return Ok(Self {
                direct: binds,
                shared: None,
            });
        }
        fs::DirBuilder::new().mode(0o700).create(&stage)?;
        let stage = stage.canonicalize()?;
        let mountpoint = stage.join("router");
        fs::create_dir(&mountpoint)?;
        let command_dir = stage.parent().expect("command staging");
        let commands = command_dir.parent().expect("native commands root");
        let (alias_parent, (branch, _)) = parents.first_key_value().expect("routed parent");
        let aliases = alias_parent.join(branch.unused_control_name()?);
        let mut submounts: Vec<_> = sinomo_sandbox::read_linux_sandbox_mounts()?
            .into_iter()
            .map(|m| m.target)
            .collect();
        submounts.sort_by_key(|path| path.components().count());
        let declared: Vec<_> = binds.iter().map(|b| b.target.clone()).collect();
        let mut branches = Vec::new();
        for (target, (branch, read_only)) in &parents {
            let mut covered = Vec::<PathBuf>::new();
            for submount in &submounts {
                if submount == &branch.backing
                    || !submount.starts_with(&branch.backing)
                    || covered.iter().any(|p| submount.starts_with(p))
                {
                    continue;
                }
                covered.push(submount.clone());
                let dest = target.join(submount.strip_prefix(&branch.backing).unwrap());
                if declared
                    .iter()
                    .any(|m| m != target && m.starts_with(target) && dest.starts_with(m))
                {
                    continue;
                }
                // A disconnected or vanished host submount cannot be cloned.
                // Leave its name on the live backing filesystem; failure to
                // access it must not prevent using an unrelated SIFS mount.
                if let Err(error) = fs::symlink_metadata(submount) {
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ENOENT | libc::ENOTCONN | libc::ECONNREFUSED)
                    ) {
                        continue;
                    }
                    return Err(error);
                }
                binds.push(Bind {
                    source: submount.clone(),
                    target: dest,
                    flag: if *read_only {
                        "--ro-bind"
                    } else {
                        "--dev-bind"
                    },
                });
            }
            let name = branches.len().to_string();
            // A bind of the original directory lets the kernel operate on its
            // native socket, FIFO and device inodes, including ones added later.
            binds.push(Bind {
                source: branch.backing.clone(),
                target: aliases.join(&name),
                flag: if *read_only {
                    "--ro-bind"
                } else {
                    "--dev-bind"
                },
            });
            binds.push(Bind {
                source: mountpoint.join(&name),
                target: target.clone(),
                flag: if *read_only { "--ro-bind" } else { "--bind" },
            });
            branches.push((branch.clone(), *read_only));
        }
        binds.push(Bind {
            source: commands.to_owned(),
            target: aliases.join("executions"),
            flag: "--ro-bind",
        });
        binds.sort_by_key(|b| b.target.components().count());
        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::FSName(
                command_dir
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            ),
            MountOption::Subtype("sinomo-native".into()),
            MountOption::DefaultPermissions,
            MountOption::NoSuid,
            MountOption::NoDev,
        ];
        let session = fuser::spawn_mount2(
            Router::new(branches, aliases, commands.to_owned(), mountpoint.clone())?,
            &mountpoint,
            &config,
        )
        .map_err(|cause| {
            io::Error::new(
                cause.kind(),
                format!(
                    "mount owner-only FUSE router at {} (requires access to /dev/fuse): {cause}",
                    mountpoint.display()
                ),
            )
        })?;
        let shared = Arc::new(SharedView {
            binds,
            _session: session,
            _owner: lease.clone(),
            _slot: slot.clone(),
        });
        *current = Arc::downgrade(&shared);
        Ok(Self {
            direct: vec![],
            shared: Some(shared),
        })
    }

    pub fn append_arguments(&self, command: &mut Command) {
        for bind in self.shared.as_ref().map_or(&self.direct, |v| &v.binds) {
            command.arg(bind.flag).arg(&bind.source).arg(&bind.target);
        }
    }
}
