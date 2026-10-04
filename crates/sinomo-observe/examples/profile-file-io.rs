//! Local end-to-end smoke test for capture -> real file I/O -> standard report.
//! This is not a DFS/cluster benchmark. Never use its timings for DFS acceptance.
use sinomo_core::storage::io_trace;
use sinomo_observe::{ObserveControlRequest, ObserveRuntimeConfig, ObserveTracesRequest};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("usage: profile-file-io NEW_OUTPUT_DIR")?,
    );
    std::fs::create_dir(&output)?;
    std::fs::create_dir(output.join("local-smoke"))?;
    sinomo_observe::init_observe_runtime(ObserveRuntimeConfig {
        process_role: "local-smoke".into(),
        node_id: Some("local-smoke".into()),
        filter: "warn".into(),
        quiet_filter_directives: Vec::new(),
    })?;
    let id = "local-file-io-smoke";
    sinomo_observe::update_observe_control(ObserveControlRequest {
        capture_id: Some(id.into()),
        clear_traces: true,
        filter: Some("warn,sinomo.perf=trace".into()),
        ..Default::default()
    })?;
    let request = ObserveTracesRequest {
        capture_id: Some(id.into()),
        ..Default::default()
    };
    let before = sinomo_observe::read_observe_traces(&request);
    let bytes = [42; 4096];
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(output.join("data.bin"))?;
    for _ in 0..20 {
        let span = tracing::trace_span!(target: "sinomo.perf", "local.write_sync_read", stage = "local.write_sync_read", outcome = "unknown");
        let _entered = span.enter();
        file.seek(SeekFrom::Start(0))?;
        io_trace::write_all(&mut file, &bytes)?;
        io_trace::sync_all(&file)?;
        file.seek(SeekFrom::Start(0))?;
        let mut read = [0; 4096];
        let mut offset = 0;
        while offset < read.len() {
            let count = io_trace::read(&mut file, &mut read[offset..])?;
            if count == 0 {
                return Err("unexpected EOF".into());
            }
            offset += count;
        }
        assert_eq!(bytes, read);
        span.record("outcome", "ok");
    }
    let after = sinomo_observe::read_observe_traces(&request);
    sinomo_observe::update_observe_control(ObserveControlRequest {
        clear_capture: true,
        expected_capture_id: Some(id.into()),
        ..Default::default()
    })?;
    for (index, response) in [before, after].into_iter().enumerate() {
        std::fs::write(
            output.join(format!("local-smoke/traces-{index:06}.json")),
            serde_json::to_vec_pretty(&response)?,
        )?;
    }
    std::fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1, "capture_id": id, "nodes": ["local-smoke"],
            "classification": "local-instrumentation-smoke-not-dfs-benchmark",
            "workload_exit_code": 0, "workload": "20 x 4096-byte overwrite + sync_all + verified read",
            "profile": "debug", "errors": [],
        }))?,
    )?;
    Ok(())
}
