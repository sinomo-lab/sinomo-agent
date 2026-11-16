//! Model-free consumer for real daemon media routes. Run with SINOMO_DATA_DIR
//! pointing at the calling node, then ADDRESS TARGET_NODE REQUEST_JSON_PATH.
//! A local invite request writes its result to a caller-selected private file.
use serde_json::{Value, json};
use sinomo_runtime::{
    api::DaemonApiClient,
    network::{ObjectRpcCall, ObjectRpcRequest},
    node::NodeId,
};
use std::{
    io::Write,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncReadExt;

fn emit(value: Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
fn request(
    route: &str,
    category: &str,
    value: Value,
) -> Result<ObjectRpcRequest, Box<dyn std::error::Error>> {
    macro_rules! typed {
        ($ty:ty) => {
            ObjectRpcRequest::encode_body(route, category, &serde_json::from_value::<$ty>(value)?)?
        };
    }
    Ok(match route {
        "/app-use/control" => typed!(sinomo_actions::desktop::DesktopControlRequest),
        "/app-use/capture" => typed!(sinomo_actions::desktop::DesktopStreamRequest),
        "/media/audio/devices" => typed!(sinomo_audio::service::MediaAudioDevicesRequest),
        "/media/audio/capture" => typed!(sinomo_audio::service::MediaAudioCaptureRequest),
        "/media/video/capture" => typed!(sinomo_video::service::VideoCaptureRequest),
        "/media/video/control" => typed!(sinomo_video::service::VideoControlRequest),
        "/media/video/devices" | "/media/sensors/read" => {
            ObjectRpcRequest::encode_body(route, category, &())?
        }
        _ => return Err("unsupported probe route; add its typed request contract".into()),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: media_e2e ADDRESS TARGET_NODE REQUEST_JSON_PATH".into());
    }
    let input: Value = serde_json::from_slice(&std::fs::read(&args[3])?)?;
    let client = DaemonApiClient::at_address(&args[1]).with_timeout(Duration::from_secs(10));
    let mode = input["mode"].as_str().unwrap_or("rpc");
    if mode == "state-evidence" {
        let data = sinomo_runtime::local::SinomoDataDir::from_env_or_home()?;
        let paths = sinomo_runtime::network::ClusterPaths::new(&data);
        emit(
            json!({"raw":sinomo_runtime::cluster::read_cluster_state_without_kv(&paths)?,
            "merged":sinomo_runtime::cluster::read_cluster_state(&paths)?}),
        );
        return Ok(());
    }
    if mode == "raft-evidence" {
        let snapshot = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(
            &sinomo_runtime::kv::ClusterKvPaths::new(
                input["snapshot_root"]
                    .as_str()
                    .ok_or("snapshot_root path")?,
            ),
        )?;
        let state: sinomo_runtime::core::ClusterBootstrapState =
            sinomo_runtime::network::read_object_file(std::path::Path::new(
                input["state"].as_str().ok_or("state path")?,
            ))?;
        let config = snapshot
            .entries
            .iter()
            .find(|entry| entry.key == "/cluster/raft/config")
            .map(|entry| entry.decode::<sinomo_runtime::kv::ClusterKvRaftConfigRecord>())
            .transpose()?;
        emit(
            json!({"revision":snapshot.revision,"config":config,"state_members":state.members.iter().map(|member| json!({"node":member.node_id,"state":member.state,"role":member.role,"node_role":member.node_role})).collect::<Vec<_>>() }),
        );
        return Ok(());
    }
    if mode == "loopback-ping" || mode == "network-ping" {
        use sinomo_runtime::{cluster, local::SinomoDataDir, network};
        let data = SinomoDataDir::from_env_or_home()?;
        let state = cluster::read_cluster_state_without_kv(&network::ClusterPaths::new(&data))?;
        let node = args[2].parse::<NodeId>()?;
        if mode == "loopback-ping" && node != state.local_node_id {
            return Err("loopback control must target this node".into());
        }
        for response_bytes in [0, 16 * 1024, 64 * 1024] {
            for iteration in 0..35 {
                let nonce = format!("media-latency-{response_bytes}-{iteration}");
                let call = ObjectRpcCall::encode_value(
                    network::CLUSTER_NETWORK_PING_ROUTE,
                    network::CLUSTER_NETWORK_RPC_CATEGORY,
                    cluster::ClusterNetworkPingRequest {
                        nonce: nonce.clone(),
                        payload: Vec::new(),
                        response_payload_bytes: response_bytes,
                        payload_mode: cluster::ClusterNetworkPingPayloadMode::EmbeddedSobj,
                        request_attachment_bytes: 0,
                        response_attachment_bytes: 0,
                    },
                )?;
                let sent_unix_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
                let started = Instant::now();
                let response: cluster::ClusterNetworkPingResponse = client
                    .object_rpc_call_to_target_async(&node, call, Duration::from_secs(5), true)
                    .await?
                    .decode_body()
                    .await?;
                let elapsed_us = started.elapsed().as_micros();
                let completed_unix_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
                if response.nonce != nonce
                    || response.responder_node_id != node
                    || response.response_payload.len() != response_bytes
                {
                    return Err("loopback ping response mismatch".into());
                }
                emit(
                    json!({"kind":if mode == "loopback-ping" {"local_object_rpc_control"} else {"cross_node_object_rpc_control"},"response_bytes":response_bytes,
                    "iteration":iteration,"warmup":iteration<5,"elapsed_us":elapsed_us,"verified":true,
                    "sent_unix_ns":sent_unix_ns,"completed_unix_ns":completed_unix_ns,
                    "remote_observed_unix_ns":response.observed_at.unix_timestamp_nanos()}),
                );
            }
        }
        return Ok(());
    }
    if mode == "directory" {
        use sinomo_runtime::{cluster, local::SinomoDataDir, network};
        let data = SinomoDataDir::from_env_or_home()?;
        let paths = network::ClusterPaths::new(&data);
        let state = if let Some(path) = input["membership_snapshot"].as_str() {
            network::read_object_file(std::path::Path::new(path))?
        } else {
            cluster::read_cluster_state_without_kv(&paths)?
        };
        let (version, members, endpoints) = if input["membership_snapshot"].is_string() {
            // Public, archived membership for offline replay only; never write
            // this snapshot into the running node's authority store.
            (
                network::DirectoryVersion(0),
                state.members.clone(),
                Vec::new(),
            )
        } else {
            let directory = network::cluster_directory_with_bootstrap_state(
                network::cluster_directory_for(&paths),
                &state,
            );
            directory
                .current_records_snapshot(Instant::now() + Duration::from_secs(10))?
                .into_parts()
        };
        let view =
            network::DirectorySnapshot::try_from_records(version, members.clone(), endpoints)?;
        if let Some(path) = input["transport_prelude"].as_str() {
            let bytes = std::fs::read(path)?;
            let metadata =
                network::read_authenticated_transport_prelude(&mut bytes.as_slice()).await?;
            let receiver = input["receiver_node"]
                .as_str()
                .ok_or("missing receiver_node")?
                .parse::<NodeId>()?;
            // Offline verification only: do not sign as this node or modify its
            // persisted identity. The verifier uses the captured sender proof
            // and the supplied public membership directory.
            let verifier = network::DaemonRoutedSessionIdentity::new(receiver.clone(), data)?;
            let verified = network::verify_builtin_authenticated_transport_metadata(
                &metadata, &receiver, &view, &verifier,
            );
            let mut receiver_state = state.clone();
            receiver_state.local_node_id = receiver;
            let scope_result = verified.as_ref().map(|peer| {
                network::cluster_routed_stream_pool_scopes_for_authenticated_directory(
                    &receiver_state,
                    &members,
                    peer,
                    &view.node(peer).unwrap().public_key,
                )
            });
            emit(
                json!({"offline_transport_verification":format!("{verified:?}"),
                "offline_scope_verification":format!("{scope_result:?}"),
                "peer":metadata.peer_transport_identity,"transport":format!("{:?}",metadata.transport),
                "capabilities":format!("{:?}",metadata.capabilities)}),
            );
            return Ok(());
        }
        let peers: Vec<_> = view.nodes().iter().map(|(id, node)| json!({"node":id.to_string(),
            "public_key":node.public_key,"scopes":format!("{:?}",network::cluster_routed_stream_pool_scopes_for_authenticated_directory(&state,&members,id,&node.public_key))})).collect();
        emit(
            json!({"local_node":state.local_node_id.to_string(),"member_count":members.len(),"peers":peers}),
        );
        return Ok(());
    }
    if mode == "invite" {
        let request = serde_json::from_value(input["request"].clone())?;
        let reply = client.cluster_invite_with_request(request)?;
        let path = input["output"]
            .as_str()
            .ok_or("missing private invite output")?;
        std::fs::write(path, reply.invite)?;
        emit(json!({"invite_written":true}));
        return Ok(());
    }
    if mode == "local" {
        let request = request(
            input["route"].as_str().ok_or("missing route")?,
            input["category"].as_str().unwrap_or("media"),
            input["request"].clone(),
        )?;
        let started = Instant::now();
        let response: Value = client.call_request_async(&request).await?.decode_body()?;
        emit(json!({"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,"response":response}));
        return Ok(());
    }
    let node = args[2].parse::<NodeId>()?;
    let route = input["route"].as_str().ok_or("missing route")?;
    let call = ObjectRpcCall::value(request(
        route,
        input["category"].as_str().unwrap_or("media"),
        input["request"].clone(),
    )?);
    let started = Instant::now();
    let result = client
        .object_rpc_call_to_target_async(&node, call, Duration::from_secs(10), true)
        .await?;
    result.response.error_detail()?;
    if matches!(mode, "video" | "video-latency") {
        let header: Value = result.response.decode_body()?;
        let version = header
            .as_u64()
            .or_else(|| header.get("version").and_then(Value::as_u64));
        if !matches!(version, Some(1 | 2 | 3)) {
            return Err("unsupported video stream version".into());
        }
        if input["request"]["source"]["kind"] == "screen" && version != Some(3) {
            return Err("screen video requires the geometry-aware stream contract".into());
        }
    }
    let connected_ms = started.elapsed().as_secs_f64() * 1000.0;
    if mode == "rpc" {
        let response: Value = result.decode_body().await?;
        emit(json!({"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,"response":response}));
        return Ok(());
    }
    emit(
        json!({"connected_ms":connected_ms,"route":result.response.route,"target":node.to_string()}),
    );
    let count = input["samples"].as_u64().unwrap_or(100).min(1000);
    let mut previous = started;
    if mode == "video-latency" {
        let mut body = result.body.cancel_on_drop();
        let mut decoder = sinomo_video::stream::VideoStreamReader::default();
        for sample in 0..count {
            let (metadata, image) =
                tokio::time::timeout(Duration::from_secs(5), decoder.read_decoded(&mut body))
                    .await??
                    .ok_or("video ended before requested samples")?;
            if input["request"]["source"]["kind"] == "screen" {
                let expected: sinomo_runtime::device::desktop::DesktopTarget =
                    serde_json::from_value(input["request"]["source"]["target"].clone())?;
                if !matches!(&metadata.identity, Some(sinomo_runtime::device::desktop::surfaces::DesktopCaptureIdentity::Target { target, .. }) if target == &expected)
                {
                    return Err("video source identity does not match requested screen".into());
                }
            }
            let received = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
            let decode_image = Instant::now();
            let (width, height, saved, pixels) = match image {
                sinomo_video::stream::ReceivedFrame::Image(image) => {
                    let decoded = image::load_from_memory(&image.image)?;
                    (decoded.width(), decoded.height(), Some(image.image), None)
                }
                native => {
                    let frame = native.into_pixels()?;
                    (frame.width, frame.height, None, Some(frame))
                }
            };
            let ready = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
            let now = Instant::now();
            emit(
                json!({"sample":sample,"warmup":sample<5,"elapsed_ms":now.duration_since(started).as_secs_f64()*1000.0,
                "gap_ms":now.duration_since(previous).as_secs_f64()*1000.0,
                "received_unix_us":received,"ready_unix_us":ready,"metadata":metadata,
                "wire_received_unix_us":decoder.last_received_unix_us,
                "wire_bytes":decoder.last_wire_bytes,"video_decode_us":decoder.last_decode_us,
                "image_decode_us":decode_image.elapsed().as_micros(),"decoded_width":width,"decoded_height":height}),
            );
            if let Some(directory) = input["save_frames"].as_str() {
                if sample == 0 || sample + 1 == count {
                    std::fs::create_dir_all(directory)?;
                    if let Some(image) = saved {
                        std::fs::write(format!("{directory}/{sample:04}.image"), &image)?;
                    }
                    if let Some(frame) = pixels {
                        image::RgbImage::from_raw(frame.width, frame.height, frame.pixels.to_vec())
                            .ok_or("invalid decoded RGB surface")?
                            .save(format!("{directory}/{sample:04}.png"))?;
                    }
                }
            }
            previous = now;
        }
        return Ok(());
    }
    if mode == "video" {
        let source = sinomo_video::stream::RemoteVideoSource::from_body(result.body);
        let mut seen = 0;
        let mut last_progress = Instant::now();
        while seen < count {
            if last_progress.elapsed() > Duration::from_secs(5) {
                return Err("video made no progress for five seconds".into());
            }
            if let Some((metadata, image)) = source.take_latest().map_err(std::io::Error::other)? {
                let now = Instant::now();
                emit(
                    json!({"sample":seen,"elapsed_ms":now.duration_since(started).as_secs_f64()*1000.0,
                    "gap_ms":now.duration_since(previous).as_secs_f64()*1000.0,
                    "received_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
                    "metadata":metadata,"encoded_bytes":image.image.len()}),
                );
                if let Some(directory) = input["save_frames"].as_str() {
                    std::fs::create_dir_all(directory)?;
                    std::fs::write(format!("{directory}/{seen:04}.image"), &image.image)?;
                }
                seen += 1;
                previous = now;
                last_progress = now;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    } else if mode == "audio" {
        let bytes = input["chunk_bytes"].as_u64().unwrap_or(4800).min(1_000_000) as usize;
        let mut buffer = vec![0; bytes];
        let mut body = result.body.cancel_on_drop();
        for sample in 0..count {
            tokio::time::timeout(Duration::from_secs(5), body.read_exact(&mut buffer)).await??;
            let now = Instant::now();
            emit(
                json!({"sample":sample,"elapsed_ms":now.duration_since(started).as_secs_f64()*1000.0,
                "gap_ms":now.duration_since(previous).as_secs_f64()*1000.0,"bytes":buffer.len(),
                "nonzero_bytes":buffer.iter().filter(|&&b| b!=0).count()}),
            );
            previous = now;
        }
    } else {
        return Err("unknown consumer mode".into());
    }
    emit(
        json!({"complete":true,"samples":count,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
    );
    Ok(())
}
