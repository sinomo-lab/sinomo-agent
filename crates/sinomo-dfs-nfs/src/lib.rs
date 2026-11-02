//! Direct NFSv3 presentation of SIFS.
//!
//! Unlike a projected or synchronized mount, this adapter stores no host-side
//! file tree. The Windows or Unix NFS client sends operations to a loopback
//! user-mode server and every operation is resolved against mounted SIFS.

mod adapter;
mod handoff;
mod module;
mod native_mount;
mod supervisor;

use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
use std::net::SocketAddr;
use std::path::PathBuf;
use thiserror::Error;

pub use adapter::SifsNfsFileSystem;
pub use module::*;
pub use native_mount::*;
pub use supervisor::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsNfsServerOptions {
    pub data_dir: Option<PathBuf>,
    pub bind: String,
    pub export_name: String,
    pub root_path: String,
    pub read_only: bool,
}

impl Default for DfsNfsServerOptions {
    fn default() -> Self {
        Self {
            data_dir: None,
            // Windows Client for NFS discovers mount and NFS programs through
            // portmapper. The nfs3_server listener multiplexes all programs on
            // one TCP port, so the loopback portmapper port is the compatible
            // default. Binding it requires an elevated Windows service.
            bind: "127.0.0.1:111".to_string(),
            export_name: "sifs".to_string(),
            root_path: "/".to_string(),
            read_only: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum DfsNfsError {
    #[error("invalid NFS configuration: {0}")]
    InvalidConfiguration(String),
    #[error("cannot open mounted SIFS service: {0}")]
    Dfs(String),
    #[error("cannot bind SIFS NFS server at {address}: {detail}")]
    Bind { address: String, detail: String },
    #[error("SIFS NFS server failed: {0}")]
    Serve(String),
}

pub type Result<T> = std::result::Result<T, DfsNfsError>;

pub struct DfsNfsServer {
    listener: NFSTcpListener<SifsNfsFileSystem>,
    mount_events: tokio::sync::mpsc::Receiver<bool>,
    local_addr: SocketAddr,
    export_name: String,
}

impl std::fmt::Debug for DfsNfsServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsNfsServer")
            .field("local_addr", &self.local_addr)
            .field("export_name", &self.export_name)
            .finish_non_exhaustive()
    }
}

impl DfsNfsServer {
    pub async fn bind(options: DfsNfsServerOptions) -> Result<Self> {
        validate_server_options(&options)?;
        let data_dir = options.data_dir.clone();
        let root_path = options.root_path.clone();
        let read_only = options.read_only;
        let filesystem = tokio::task::spawn_blocking(move || {
            SifsNfsFileSystem::open(data_dir, root_path, read_only)
        })
        .await
        .map_err(|source| DfsNfsError::Dfs(format!("NFS open worker failed: {source}")))??;
        let mut listener = NFSTcpListener::bind(&options.bind, filesystem)
            .await
            .map_err(|source| DfsNfsError::Bind {
                address: options.bind.clone(),
                detail: source.to_string(),
            })?;
        listener.with_export_name(&options.export_name);
        let (mount_events_tx, mount_events) = tokio::sync::mpsc::channel(16);
        listener.set_mount_listener(mount_events_tx);
        let local_addr = SocketAddr::new(listener.get_listen_ip(), listener.get_listen_port());
        Ok(Self {
            listener,
            mount_events,
            local_addr,
            export_name: options.export_name,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn export_name(&self) -> &str {
        &self.export_name
    }

    pub async fn serve(self) -> Result<()> {
        self.serve_inner(false).await
    }

    /// Serve until an NFS client sends UMOUNT for this export.
    ///
    /// This lets a foreground Windows drive mapping terminate cleanly when a
    /// second CLI invocation runs `dfs nfs unmount`.
    pub async fn serve_until_unmounted(self) -> Result<()> {
        self.serve_inner(true).await
    }

    async fn serve_inner(mut self, stop_on_unmount: bool) -> Result<()> {
        let serving = self.listener.handle_forever();
        tokio::pin!(serving);
        loop {
            tokio::select! {
                result = &mut serving => {
                    return result.map_err(|source| DfsNfsError::Serve(source.to_string()));
                }
                Some(mounted) = self.mount_events.recv() => {
                    if stop_on_unmount && !mounted {
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn validate_server_options(options: &DfsNfsServerOptions) -> Result<()> {
    let address = options
        .bind
        .parse::<SocketAddr>()
        .map_err(|source| DfsNfsError::InvalidConfiguration(source.to_string()))?;
    if !address.ip().is_loopback() {
        return Err(DfsNfsError::InvalidConfiguration(format!(
            "refusing non-loopback NFS bind {}; remote NFS exposure requires a separate authenticated transport design",
            address.ip()
        )));
    }
    if options.export_name.is_empty()
        || !options
            .export_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(DfsNfsError::InvalidConfiguration(
            "export name must contain only ASCII letters, digits, '-' or '_'".to_string(),
        ));
    }
    normalize_root_path(&options.root_path).map(|_| ())
}

pub(crate) fn normalize_root_path(path: &str) -> Result<String> {
    if !path.starts_with('/') || path.bytes().any(|byte| byte == 0) {
        return Err(DfsNfsError::InvalidConfiguration(format!(
            "SIFS export root must be an absolute path: {path}"
        )));
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                return Err(DfsNfsError::InvalidConfiguration(format!(
                    "SIFS export root cannot contain '..': {path}"
                )));
            }
            component => components.push(component),
        }
    }
    Ok(if components.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", components.join("/"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nfs3_client::Nfs3ConnectionBuilder;
    use nfs3_client::nfs3_types::nfs3;
    use nfs3_client::nfs3_types::xdr_codec::Opaque;
    use nfs3_client::tokio::TokioConnector;

    #[test]
    fn server_options_fail_closed_to_loopback() {
        let mut options = DfsNfsServerOptions {
            bind: "0.0.0.0:111".to_string(),
            ..DfsNfsServerOptions::default()
        };
        assert!(validate_server_options(&options).is_err());
        options.bind = "127.0.0.1:0".to_string();
        assert!(validate_server_options(&options).is_ok());
    }

    #[test]
    fn root_normalization_never_allows_escape() {
        assert_eq!(
            normalize_root_path("//services/./apps").unwrap(),
            "/services/apps"
        );
        assert!(normalize_root_path("services").is_err());
        assert!(normalize_root_path("/services/../users").is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_speaks_mount_and_nfs_over_tcp() {
        let owner = sinomo_vfs_sifs::test_support::NativeMountedOwner::start("sinomo-dfs-nfs-wire")
            .expect("start native SIFS owner");
        let server = DfsNfsServer::bind(DfsNfsServerOptions {
            data_dir: Some(owner.data_dir().root()),
            bind: "127.0.0.1:0".to_string(),
            export_name: "wire-test".to_string(),
            root_path: "/".to_string(),
            read_only: false,
        })
        .await
        .expect("bind NFS server");
        let port = server.local_addr().port();
        let serving = tokio::spawn(server.serve_until_unmounted());

        let mut connection = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            Nfs3ConnectionBuilder::new(TokioConnector, "127.0.0.1", "/wire-test")
                .connect_from_privileged_port(false)
                .mount_port(port)
                .nfs3_port(port)
                .mount(),
        )
        .await
        .expect("wire mount timed out")
        .expect("wire mount failed");
        let root_handle = connection.root_nfs_fh3();
        let created = connection
            .create(&nfs3::CREATE3args {
                where_: nfs3::diropargs3 {
                    dir: root_handle.clone(),
                    name: nfs3::filename3::from(b"wire.txt".as_slice()),
                },
                how: nfs3::createhow3::GUARDED(nfs3::sattr3::default()),
            })
            .await
            .expect("CREATE RPC failed")
            .expect("CREATE returned NFS error");
        let nfs3::Nfs3Option::Some(file_handle) = created.obj else {
            panic!("CREATE omitted the file handle");
        };
        let payload = b"wire-protocol";
        let written = connection
            .write(&nfs3::WRITE3args {
                file: file_handle.clone(),
                offset: 0,
                count: payload.len() as u32,
                stable: nfs3::stable_how::FILE_SYNC,
                data: Opaque::borrowed(payload),
            })
            .await
            .expect("WRITE RPC failed")
            .expect("WRITE returned NFS error");
        assert_eq!(written.count, payload.len() as u32);
        assert_eq!(written.committed, nfs3::stable_how::FILE_SYNC);
        let read = connection
            .read(&nfs3::READ3args {
                file: file_handle,
                offset: 0,
                count: 1024,
            })
            .await
            .expect("READ RPC failed")
            .expect("READ returned NFS error");
        assert_eq!(read.data.as_ref(), payload);
        assert!(read.eof);
        let response = connection
            .readdir(&nfs3::READDIR3args {
                dir: root_handle,
                cookie: 0,
                cookieverf: nfs3::cookieverf3::default(),
                count: 128 * 1024,
            })
            .await
            .expect("READDIR RPC failed")
            .expect("READDIR returned NFS error");
        assert!(response.reply.eof);
        connection.unmount().await.expect("unmount RPC failed");
        tokio::time::timeout(std::time::Duration::from_secs(2), serving)
            .await
            .expect("server did not stop after UMOUNT")
            .expect("server task failed")
            .expect("server returned an error");
    }
}
