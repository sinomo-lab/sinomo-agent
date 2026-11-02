//! Setup compensation uses the existing installation admission and one durable
//! filesystem preimage. Component evolution remains rollout-owned.
use super::{Result, binary};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
const JOURNAL: &str = "sinomo-setup-recovery.json";
#[derive(Serialize, Deserialize)]
struct FileBefore {
    path: PathBuf,
    bytes: Option<Vec<u8>>,
    link: Option<PathBuf>,
    #[serde(default)]
    mode: Option<u32>,
}
#[derive(Serialize, Deserialize)]
struct Recovery {
    #[serde(default)]
    committed: bool,
    root: PathBuf,
    data: PathBuf,
    files: Vec<FileBefore>,
    payload_files: Option<BTreeSet<PathBuf>>,
}
pub(super) struct SetupTransaction {
    recovery: Recovery,
    finished: bool,
}
fn inventory(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut found = BTreeSet::new();
    if !root.is_dir() {
        return Ok(found);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        found.insert(path.clone());
        if entry.file_type()?.is_dir() {
            found.extend(inventory(&path)?);
        }
    }
    Ok(found)
}
fn capture(path: PathBuf) -> Result<FileBefore> {
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(FileBefore {
                path,
                bytes: None,
                link: None,
                mode: None,
            });
        }
        Err(e) => return Err(e.into()),
    };
    if !metadata.is_file() && !metadata.file_type().is_symlink() {
        return Err(format!("setup file is a directory: {}", path.display()).into());
    }
    let link = metadata
        .file_type()
        .is_symlink()
        .then(|| fs::read_link(&path))
        .transpose()?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && link.is_some() => None,
        Err(e) => return Err(e.into()),
    };
    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(&path)
            .ok()
            .map(|meta| meta.permissions().mode())
    };
    #[cfg(not(unix))]
    let mode = None;
    Ok(FileBefore {
        path,
        bytes,
        link,
        mode,
    })
}
fn remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
impl SetupTransaction {
    pub(super) fn begin(root: &Path, data: &Path, payload: bool) -> Result<Self> {
        let mut paths = vec![
            root.join(super::RECEIPT),
            root.join("sinomo-data-dir"),
            root.join("windows-integration.ps1"),
            root.join("uninstall.ps1"),
            root.join("windows-integration.json"),
            root.join("windows-integration.json.next"),
            root.join(binary("sinomo-setup")),
            data.join("config/installer-owner"),
            data.join("config/uninstalled"),
            data.join("bin/sinomo-data-dir"),
            data.join("config/initial-payload.json"),
        ];
        // Existing setup executables are never replaced, and can be large.
        paths.retain(|p| p != &root.join(binary("sinomo-setup")) || !p.exists());
        paths.extend(super::services::effect_paths(root, data)?);
        #[cfg(target_os = "linux")]
        paths.extend(super::linux_integration_paths()?);
        let payload_files = payload.then(|| inventory(data)).transpose()?;
        if let Some(entries) = &payload_files {
            // Payload seeding preserves existing blobs/source. Snapshot existing
            // small metadata too, so interrupted default-module registration rolls back.
            for path in entries {
                if path.file_name().is_some_and(|name| {
                    name == "activation.owner.json" || name == "activation.lock"
                }) {
                    continue;
                }
                if fs::symlink_metadata(path).is_ok_and(|m| m.is_file() && m.len() <= 1024 * 1024) {
                    paths.push(path.clone());
                }
            }
        }
        paths.sort();
        paths.dedup();
        let files = paths.into_iter().map(capture).collect::<Result<Vec<_>>>()?;
        let recovery = Recovery {
            committed: false,
            root: root.to_path_buf(),
            data: data.to_path_buf(),
            files,
            payload_files,
        };
        let bytes = serde_json::to_vec(&recovery)?;
        sinomo_core::storage::durable_file::write_file_atomic(
            &root.join(JOURNAL),
            &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
                "installer",
                "recovery",
            )
            .sync_all(),
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.join(JOURNAL), fs::Permissions::from_mode(0o600))?;
        }
        Ok(Self {
            recovery,
            finished: false,
        })
    }
    pub(super) fn commit(&mut self) -> Result<()> {
        self.recovery.committed = true;
        let durable_commit = sinomo_core::storage::durable_file::write_file_atomic(
            &self.recovery.root.join(JOURNAL),
            &serde_json::to_vec(&self.recovery)?,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
                "installer",
                "recovery",
            )
            .sync_all(),
        );
        if let Err(error) = durable_commit {
            self.recovery.committed = false;
            return Err(error.into());
        }
        #[cfg(windows)]
        super::windows_integration(&self.recovery.root, "commit", &[])?;
        remove(&self.recovery.root.join(JOURNAL))?;
        self.finished = true;
        Ok(())
    }
    pub(super) fn rollback(&mut self) -> Result<()> {
        if self.recovery.committed {
            self.finished = true;
            return Ok(());
        }
        #[cfg(windows)]
        super::windows_integration(&self.recovery.root, "rollback", &[])?;
        restore(&self.recovery)?;
        remove(&self.recovery.root.join(JOURNAL))?;
        self.finished = true;
        Ok(())
    }
}
impl Drop for SetupTransaction {
    fn drop(&mut self) {
        if !self.finished {
            if let Err(error) = self.rollback() {
                eprintln!(
                    "Setup recovery retained at {}: {error}",
                    self.recovery.root.join(JOURNAL).display()
                );
            }
        }
    }
}
fn restore(recovery: &Recovery) -> Result<()> {
    if let Some(before) = &recovery.payload_files {
        let after = inventory(&recovery.data)?;
        let mut added = after.difference(before).cloned().collect::<Vec<_>>();
        added.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
        for path in added {
            if path.is_dir() {
                let _ = fs::remove_dir(&path);
            } else {
                remove(&path)?;
            }
        }
    }
    for file in recovery.files.iter().rev() {
        if let Some(target) = &file.link {
            if fs::read_link(&file.path).ok().as_ref() != Some(target) {
                remove(&file.path)?;
                fs::create_dir_all(file.path.parent().ok_or("recovery parent")?)?;
                #[cfg(unix)]
                std::os::unix::fs::symlink(target, &file.path)?;
                #[cfg(windows)]
                std::os::windows::fs::symlink_file(target, &file.path)?;
            }
        }
        if let Some(bytes) = &file.bytes {
            fs::create_dir_all(file.path.parent().ok_or("recovery parent")?)?;
            fs::write(&file.path, bytes)?;
            #[cfg(unix)]
            if let Some(mode) = file.mode {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&file.path, fs::Permissions::from_mode(mode))?;
            }
        } else if file.link.is_none() {
            remove(&file.path)?;
        }
    }
    Ok(())
}
pub(super) fn recover(root: &Path, data: &Path) -> Result<()> {
    let path = root.join(JOURNAL);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let recovery: Recovery = serde_json::from_slice(&bytes)?;
    if recovery.root != root || recovery.data != data {
        return Err("setup recovery belongs to another installation".into());
    }
    if recovery.committed {
        #[cfg(windows)]
        super::windows_integration(root, "commit", &[])?;
    } else {
        #[cfg(windows)]
        super::windows_integration(root, "rollback", &[])?;
        restore(&recovery)?;
    }
    remove(&path)
}
