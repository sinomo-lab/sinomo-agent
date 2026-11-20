//! Resolve mount destinations in the view resulting from the preceding mounts,
//! including guest-absolute symlinks. A child mount can therefore use an entry
//! in an already projected SIFS directory as its parent.
use super::{Bind, Branch, NativeMountProjection, SandboxMountMode};
use std::{
    collections::{BTreeMap, VecDeque},
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
};

pub(super) struct Plan {
    pub binds: Vec<Bind>,
    pub parents: BTreeMap<PathBuf, (Branch, bool)>,
}
#[derive(Clone)]
struct Location {
    guest: PathBuf,
    backing: PathBuf,
    read_only: bool,
    // Index of the last existing directory before a missing path component.
    missing_parent: Option<usize>,
}

fn destination(
    path: &Path,
    mounted: &BTreeMap<PathBuf, Location>,
) -> io::Result<(PathBuf, Option<Location>)> {
    let mut stack = vec![Location {
        guest: "/".into(),
        backing: "/".into(),
        read_only: false,
        missing_parent: None,
    }];
    let mut parts: VecDeque<OsString> = path.iter().map(OsStr::to_owned).collect();
    let mut links = 0;
    while let Some(name) = parts.pop_front() {
        if name == "/" {
            stack.truncate(1);
            continue;
        }
        if name == "." {
            continue;
        }
        if name == ".." {
            if stack.len() > 1 {
                stack.pop();
            }
            continue;
        }
        let parent = stack.last().unwrap();
        let guest = parent.guest.join(&name);
        if let Some(location) = mounted.get(&guest) {
            stack.push(location.clone());
            continue;
        }
        let backing = parent.backing.join(&name);
        let mut missing_parent = parent.missing_parent;
        if missing_parent.is_none() {
            match fs::symlink_metadata(&backing) {
                Ok(meta) if meta.is_symlink() => {
                    if parts.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("mount target {} is a symlink", guest.display()),
                        ));
                    }
                    links += 1;
                    if links > 40 {
                        return Err(io::Error::from_raw_os_error(libc::ELOOP));
                    }
                    let target = fs::read_link(&backing)?;
                    for component in target.iter().rev() {
                        parts.push_front(component.to_owned());
                    }
                    continue;
                }
                Ok(meta) if !meta.is_dir() && !parts.is_empty() => {
                    return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
                }
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    missing_parent = Some(stack.len() - 1)
                }
                Err(e) => return Err(e),
            }
        }
        stack.push(Location {
            guest,
            backing,
            read_only: parent.read_only,
            missing_parent,
        });
    }
    let target = stack.pop().expect("root location");
    Ok((
        target.guest,
        target.missing_parent.map(|i| stack[i].clone()),
    ))
}

impl Plan {
    pub fn new(projection: &NativeMountProjection) -> io::Result<Self> {
        let mut mounts: Vec<_> = projection.mounts.iter().collect();
        mounts.sort_by_key(|m| m.guest_path.components().count());
        let mut mounted = BTreeMap::new();
        let mut parents = BTreeMap::new();
        let mut binds = Vec::new();
        for mount in mounts {
            let (target, missing) = destination(&mount.guest_path, &mounted)?;
            if mounted.contains_key(&target) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate native mount destination {}", target.display()),
                ));
            }
            if let Some(parent) = missing {
                parents
                    .entry(parent.guest)
                    .or_insert_with(|| (Branch::directory(parent.backing), parent.read_only));
            }
            let read_only = mount.mode == SandboxMountMode::ReadOnly;
            mounted.insert(
                target.clone(),
                Location {
                    guest: target.clone(),
                    backing: mount.source_host_path.clone(),
                    read_only,
                    missing_parent: None,
                },
            );
            binds.push(Bind {
                source: mount.source_host_path.clone(),
                target,
                flag: if read_only { "--ro-bind" } else { "--bind" },
            });
        }
        // Reserve every mount name attached through a routed parent, even when
        // a host object happens to occupy that name during this launch.
        for bind in &binds {
            for parent in bind.target.ancestors().skip(1) {
                if let Some((branch, _)) = parents.get_mut(parent) {
                    branch.reserve(
                        bind.target.strip_prefix(parent).unwrap(),
                        fs::metadata(&bind.source)?.is_dir(),
                    );
                    break;
                }
                if mounted.contains_key(parent) {
                    break;
                }
            }
        }
        Ok(Self { parents, binds })
    }
}
