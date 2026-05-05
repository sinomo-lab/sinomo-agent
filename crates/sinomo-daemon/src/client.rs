use crate::{DaemonError, sign_daemon_command_for_local_node};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::time::Duration;

pub fn send_daemon_command(
    address: &str,
    command: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let signed = sign_daemon_command_for_local_node(command)?;
    send_daemon_command_raw(address, &signed, timeout)
}

pub fn send_daemon_command_raw(
    address: &str,
    command: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let socket_address = address
        .to_socket_addrs()
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?
        .next()
        .ok_or_else(|| DaemonError::Network {
            address: address.to_string(),
            detail: "no socket addresses resolved".to_string(),
        })?;
    let mut stream = TcpStream::connect_timeout(&socket_address, timeout).map_err(|source| {
        DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        }
    })?;
    stream
        .set_nonblocking(false)
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    stream
        .write_all(format!("{command}\n").as_bytes())
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    let _ = stream.shutdown(Shutdown::Write);
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    Ok(response)
}
