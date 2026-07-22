//! Diagnostic lower bound for one-frame Sinomo object RPC over TCP.
//!
//! This intentionally omits routing, authentication, routed streams, and
//! application dispatch. It measures the real object-frame codec and kernel
//! TCP path either directly or through one decode/forward relay. Both fresh
//! and persistent connections use the exact same request/response wire.

use sinomo_network::{
    ObjectFrameIoOptions, ObjectRpcRequest, ObjectRpcResponse, read_object_frame_async,
    write_object_frame_async,
};
use std::error::Error;
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

const SEQUENCE_METADATA_KEY: &str = "sinomo.bench.sequence";
type AnyError = Box<dyn Error + Send + Sync>;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), AnyError> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [mode, listen] if mode == "server" => run_server(listen).await,
        [mode, listen, target] if mode == "relay" => run_relay(listen, target).await,
        [
            mode,
            target,
            requests,
            warmup,
            connection_mode,
            payload_bytes,
        ] if mode == "client" => {
            let requests = requests.parse::<usize>()?;
            let warmup = warmup.parse::<usize>()?;
            let payload_bytes = payload_bytes.parse::<usize>()?;
            run_client(
                target,
                requests,
                warmup,
                connection_mode,
                payload_bytes,
                "none",
            )
            .await
        }
        [
            mode,
            target,
            requests,
            warmup,
            connection_mode,
            payload_bytes,
            io_mode,
        ] if mode == "client" => {
            let requests = requests.parse::<usize>()?;
            let warmup = warmup.parse::<usize>()?;
            let payload_bytes = payload_bytes.parse::<usize>()?;
            run_client(
                target,
                requests,
                warmup,
                connection_mode,
                payload_bytes,
                io_mode,
            )
            .await
        }
        _ => {
            eprintln!(
                "usage:\n  naive_tcp_object_forward server <listen>\n  naive_tcp_object_forward relay <listen> <target>\n  naive_tcp_object_forward client <target> <requests> <warmup> <fresh|persistent> <payload-bytes> [none|default]"
            );
            Err("invalid arguments".into())
        }
    }
}

async fn run_server(listen: &str) -> Result<(), AnyError> {
    let listener = TcpListener::bind(listen).await?;
    println!("ready server {}", listener.local_addr()?);
    loop {
        let (stream, _) = listener.accept().await?;
        configure_stream(&stream)?;
        tokio::spawn(async move {
            if let Err(error) = serve_echo_connection(stream).await {
                eprintln!("echo connection stopped: {error}");
            }
        });
    }
}

async fn serve_echo_connection(mut stream: TcpStream) -> Result<(), AnyError> {
    loop {
        let request: ObjectRpcRequest =
            match read_object_frame_async(&mut stream, ObjectFrameIoOptions::none()).await {
                Ok(request) => request,
                Err(_) => return Ok(()),
            };
        let mut response = ObjectRpcResponse::from_encoded_body(request.route, request.body);
        if let Some(sequence) = request.metadata.get(SEQUENCE_METADATA_KEY) {
            response
                .metadata
                .insert(SEQUENCE_METADATA_KEY.to_string(), sequence.clone());
        }
        write_object_frame_async(&mut stream, &response, ObjectFrameIoOptions::none()).await?;
    }
}

async fn run_relay(listen: &str, target: &str) -> Result<(), AnyError> {
    let listener = TcpListener::bind(listen).await?;
    println!("ready relay {} -> {target}", listener.local_addr()?);
    loop {
        let (inbound, _) = listener.accept().await?;
        configure_stream(&inbound)?;
        let target = target.to_string();
        tokio::spawn(async move {
            if let Err(error) = serve_relay_connection(inbound, &target).await {
                eprintln!("relay connection stopped: {error}");
            }
        });
    }
}

async fn serve_relay_connection(mut inbound: TcpStream, target: &str) -> Result<(), AnyError> {
    let mut outbound = connect(target).await?;
    loop {
        let request: ObjectRpcRequest =
            match read_object_frame_async(&mut inbound, ObjectFrameIoOptions::none()).await {
                Ok(request) => request,
                Err(_) => return Ok(()),
            };
        write_object_frame_async(&mut outbound, &request, ObjectFrameIoOptions::none()).await?;
        let response: ObjectRpcResponse =
            read_object_frame_async(&mut outbound, ObjectFrameIoOptions::none()).await?;
        write_object_frame_async(&mut inbound, &response, ObjectFrameIoOptions::none()).await?;
    }
}

async fn run_client(
    target: &str,
    requests: usize,
    warmup: usize,
    connection_mode: &str,
    payload_bytes: usize,
    io_mode: &str,
) -> Result<(), AnyError> {
    if requests == 0 {
        return Err("requests must be greater than zero".into());
    }
    if !matches!(connection_mode, "fresh" | "persistent") {
        return Err("connection mode must be fresh or persistent".into());
    }
    if !matches!(io_mode, "none" | "default") {
        return Err("I/O mode must be none or default".into());
    }
    let io = if io_mode == "default" {
        ObjectFrameIoOptions::default()
    } else {
        ObjectFrameIoOptions::none()
    };
    let payload = vec![0x5a_u8; payload_bytes];
    let mut persistent = if connection_mode == "persistent" {
        Some(connect(target).await?)
    } else {
        None
    };
    for sequence in 0..warmup {
        if let Some(stream) = persistent.as_mut() {
            round_trip(stream, sequence, &payload, io).await?;
        } else {
            let mut stream = connect(target).await?;
            round_trip(&mut stream, sequence, &payload, io).await?;
        }
    }

    let measured_started = Instant::now();
    let mut latencies_us = Vec::with_capacity(requests);
    for sequence in 0..requests {
        let started = Instant::now();
        if let Some(stream) = persistent.as_mut() {
            round_trip(stream, sequence + warmup, &payload, io).await?;
        } else {
            let mut stream = connect(target).await?;
            round_trip(&mut stream, sequence + warmup, &payload, io).await?;
        }
        latencies_us.push(started.elapsed().as_micros() as u64);
    }
    let elapsed = measured_started.elapsed();
    latencies_us.sort_unstable();
    let percentile = |percent: usize| {
        let rank = latencies_us
            .len()
            .saturating_mul(percent)
            .saturating_add(99)
            / 100;
        latencies_us[rank.saturating_sub(1).min(latencies_us.len() - 1)]
    };
    println!(
        "mode={connection_mode} io={io_mode} requests={requests} payload_bytes={payload_bytes} min_us={} p50_us={} p95_us={} max_us={} requests_per_second={:.2}",
        latencies_us[0],
        percentile(50),
        percentile(95),
        latencies_us[latencies_us.len() - 1],
        requests as f64 / elapsed.as_secs_f64(),
    );
    Ok(())
}

async fn round_trip(
    stream: &mut TcpStream,
    sequence: usize,
    payload: &[u8],
    io: ObjectFrameIoOptions,
) -> Result<(), AnyError> {
    let mut request = ObjectRpcRequest::from_encoded_body(
        "/bench/naive-tcp-object-forward",
        "network.benchmark",
        payload.to_vec(),
    );
    request
        .metadata
        .insert(SEQUENCE_METADATA_KEY.to_string(), sequence.to_string());
    write_object_frame_async(stream, &request, io).await?;
    let response: ObjectRpcResponse = read_object_frame_async(stream, io).await?;
    if response.body != payload
        || response.metadata.get(SEQUENCE_METADATA_KEY) != Some(&sequence.to_string())
    {
        return Err(format!("response mismatch for sequence {sequence}").into());
    }
    Ok(())
}

async fn connect(address: &str) -> Result<TcpStream, AnyError> {
    let stream = TcpStream::connect(address).await?;
    configure_stream(&stream)?;
    Ok(stream)
}

fn configure_stream(stream: &TcpStream) -> Result<(), std::io::Error> {
    stream.set_nodelay(true)
}
