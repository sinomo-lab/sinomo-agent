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
        "/media/video/capture" => typed!(sinomo_video::service::CameraCaptureRequest),
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
    if mode == "directory" {
        use sinomo_runtime::{cluster, local::SinomoDataDir, network};
        let data = SinomoDataDir::from_env_or_home()?;
        let paths = network::ClusterPaths::new(&data);
        let state = cluster::read_cluster_state_without_kv(&paths)?;
        let directory = network::cluster_directory_with_bootstrap_state(
            network::cluster_directory_for(&paths),
            &state,
        );
        let records =
            directory.current_records_snapshot(Instant::now() + Duration::from_secs(10))?;
        let (version, members, endpoints) = records.into_parts();
        let view =
            network::DirectorySnapshot::try_from_records(version, members.clone(), endpoints)?;
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
