use crate::{AuthenticatedDaemonCommand, DaemonError, DaemonPaths, authenticate_daemon_command};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub const DAEMON_CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
pub const DAEMON_COMMAND_MAX_BYTES: usize = 128 * 1024 * 1024;

pub fn handle_authenticated_daemon_connection<F, E>(
    stream: &mut TcpStream,
    paths: &DaemonPaths,
    handler: F,
) -> Result<bool, E>
where
    F: FnOnce(AuthenticatedDaemonCommand, &mut TcpStream) -> Result<bool, E>,
    E: From<DaemonError>,
{
    prepare_daemon_client_stream(stream)?;
    let request = read_daemon_command_line(stream)?;
    let command = request.trim();
    let authenticated = match authenticate_daemon_command(paths, command) {
        Ok(authenticated) => authenticated,
        Err(error) => {
            eprintln!("sinomo daemon auth failed: {error}");
            stream
                .write_all(b"auth failed\n")
                .map_err(|source| DaemonError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            return Ok(false);
        }
    };
    handler(authenticated, stream)
}

fn prepare_daemon_client_stream(stream: &TcpStream) -> Result<(), DaemonError> {
    stream
        .set_nonblocking(false)
        .map_err(|source| DaemonError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?;
    stream
        .set_read_timeout(Some(DAEMON_CLIENT_READ_TIMEOUT))
        .map_err(|source| DaemonError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?;
    stream
        .set_write_timeout(Some(DAEMON_CLIENT_WRITE_TIMEOUT))
        .map_err(|source| DaemonError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })
}

pub fn read_daemon_command_line(stream: &mut TcpStream) -> Result<String, DaemonError> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = stream
            .read(&mut chunk)
            .map_err(|source| DaemonError::Network {
                address: "daemon client".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            break;
        }
        if let Some(newline) = chunk[..read].iter().position(|byte| *byte == b'\n') {
            if request.len().saturating_add(newline) > DAEMON_COMMAND_MAX_BYTES {
                return Err(DaemonError::Parse {
                    detail: format!("daemon command exceeded {} bytes", DAEMON_COMMAND_MAX_BYTES),
                });
            }
            request.extend_from_slice(&chunk[..newline]);
            break;
        }
        if request.len().saturating_add(read) > DAEMON_COMMAND_MAX_BYTES {
            return Err(DaemonError::Parse {
                detail: format!("daemon command exceeded {} bytes", DAEMON_COMMAND_MAX_BYTES),
            });
        }
        request.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(request).map_err(|source| DaemonError::Parse {
        detail: format!("daemon command is not utf-8: {source}"),
    })
}
