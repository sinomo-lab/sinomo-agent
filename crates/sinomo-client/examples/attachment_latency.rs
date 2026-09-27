//! Persistent-client attachment latency probe. Run inside a disposable node:
//! attachment_latency <count> <warmup> <bytes> <run-name> [ordinary|attachment|reads]
//! Setup, source inspection, verification and process startup are not timed.
use sinomo_client::{SifsBrowser, SifsListOptions};
use sinomo_runtime::dfs::*;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn main() -> Result<(), String> {
    let args = std::env::args().collect::<Vec<_>>();
    let count = args
        .get(1)
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(100);
    let warmup = args
        .get(2)
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(10);
    let size = args
        .get(3)
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(128);
    let run = args.get(4).map(String::as_str).unwrap_or("probe");
    let mode = args.get(5).map(String::as_str).unwrap_or("attachment");
    if count == 0
        || count > 10000
        || size == 0
        || size > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES
        || !run.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err("invalid probe arguments".into());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let browser = SifsBrowser::open_daemon_on_runtime("127.0.0.1:21353", runtime.handle())?;
    let base = format!("/services/cluster-tests/runs/attachment-latency-{run}");
    let path = format!("{base}/source");
    let existing = args.get(6).is_some_and(|v| v == "existing");
    if !existing {
        browser.write_file(&path, b"stable attachment source")?;
    }
    let stat = browser.stat(&path)?;
    let source = DfsAttachmentSource {
        path: path.clone(),
        file_system_id: DfsFileSystemId::new(&stat.file_system_id).map_err(|e| e.to_string())?,
        object_id: DfsObjectId::new(&stat.object_id).map_err(|e| e.to_string())?,
    };
    let metadata = DfsAttachmentMetadata {
        key: DfsAttachmentKey {
            kind: "org.sinomo.latency".into(),
            name: "value".into(),
        },
        schema: "org.sinomo.latency/v1".into(),
        media_type: "application/octet-stream".into(),
        source_version: None,
        producer: "persistent-client-probe".into(),
    };
    let ordinary = format!("{base}/ordinary");
    let mut bytes = vec![42; size];
    if !existing {
        browser.write_file(&ordinary, &bytes)?;
    }
    let mut expected = match browser.file_attachment(&DfsAttachmentRequest::Get {
        source: source.clone(),
        key: metadata.key.clone(),
    })? {
        DfsAttachmentResponse::Descriptor { attachment } => attachment.map(|a| a.revision),
        _ => return Err("unexpected initial descriptor".into()),
    };
    let mut samples = Vec::new();
    for i in 0..count + warmup {
        bytes[0] = (i % 251) as u8;
        let operation_id = sinomo_runtime::core::Id::generate().to_string();
        let request = DfsAttachmentRequest::Put {
            source: source.clone(),
            metadata: metadata.clone(),
            expected: expected.clone(),
            operation_id: operation_id.clone(),
            bytes: bytes.clone(),
        };
        let started_unix_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_micros();
        let started = Instant::now();
        let result = match mode {
            "ordinary" => browser.write_file(&ordinary, &bytes).map(|_| None),
            "reads" => browser
                .list_shallow_page(
                    &base,
                    SifsListOptions {
                        limit: Some(64),
                        ..Default::default()
                    },
                )
                .map(|_| None),
            "attachment" => browser
                .file_attachment(&request)
                .and_then(|response| match response {
                    DfsAttachmentResponse::Stored { attachment } => Ok(Some(attachment.revision)),
                    other => Err(format!("unexpected publication response: {other:?}")),
                }),
            _ => return Err("unknown probe mode".into()),
        };
        let elapsed_us = started.elapsed().as_micros() as u64;
        match result {
            Ok(revision) => {
                if revision.is_some() {
                    expected = revision;
                }
            }
            Err(error) => {
                println!(
                    "{}",
                    serde_json::json!({"sample":i,"operation_id":operation_id,"started_unix_us":started_unix_us,"elapsed_us":elapsed_us,"error":error})
                );
                return Err(error);
            }
        }
        if i >= warmup {
            samples.push(elapsed_us);
        }
        println!(
            "{}",
            serde_json::json!({"sample":i,"operation_id":operation_id,"started_unix_us":started_unix_us,"warmup":i<warmup,"elapsed_us":elapsed_us})
        );
    }
    if mode == "ordinary" {
        assert_eq!(browser.read_file(&ordinary)?, bytes);
    }
    if mode == "attachment"
        && let Some(revision) = &expected
    {
        let mut received = Vec::new();
        loop {
            match browser.file_attachment(&DfsAttachmentRequest::Read {
                source: source.clone(),
                key: metadata.key.clone(),
                revision: revision.clone(),
                offset: received.len() as u64,
                max_bytes: DFS_ATTACHMENT_MAX_RANGE_BYTES,
            })? {
                DfsAttachmentResponse::Data { bytes, eof } => {
                    received.extend(bytes);
                    if eof {
                        break;
                    }
                }
                other => return Err(format!("unexpected read response: {other:?}")),
            }
        }
        assert_eq!(received, bytes);
    }
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    let percentile = |p: usize| sorted[(sorted.len() * p).div_ceil(100).saturating_sub(1)];
    println!(
        "{}",
        serde_json::json!({"summary":true,"mode":mode,"bytes":size,"count":count,"warmup":warmup,"p50_us":percentile(50),"p95_us":percentile(95),"p99_us":percentile(99),"max_us":sorted.last(),"samples_us":samples,"source":source,"target_path":match mode { "ordinary" => &ordinary, "reads" => &base, _ => &path },"verified":true})
    );
    Ok(())
}
