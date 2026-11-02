use sinomo_observe::*;

#[test]
fn terminal_console_guard_preserves_collection_file_policy_and_restores_console() {
    if let Ok(mode) = std::env::var("SINOMO_CONSOLE_GUARD_CHILD") {
        let dir = sinomo_testdir::TestDir::new("observe-terminal-console");
        let path = dir.join("existing-file-sink.log");
        let config = ObserveRuntimeConfig {
            process_role: "console-guard-test".into(),
            node_id: None,
            filter: "warn".into(),
            quiet_filter_directives: Vec::new(),
            live_log_writing: false,
        };
        if mode == "stderr" {
            init_observe_runtime_with_stderr(config).unwrap();
        } else {
            init_observe_runtime_with_log_file(config, &path).unwrap();
        }
        tracing::warn!("BEFORE_CONSOLE");
        let outer = suppress_observe_console();
        tracing::warn!(operation_id = "same-operation", "HIDDEN_CONSOLE_WARNING");
        let inner = suppress_observe_console();
        std::thread::spawn(|| tracing::error!("HIDDEN_WORKER_ERROR"))
            .join()
            .unwrap();
        drop(inner);
        tracing::warn!("STILL_HIDDEN");
        tracing::info!("FILTERED_INFO");
        let logs = read_observe_logs(
            dir.path(),
            &ObserveLogsRequest {
                source: Some(ObserveLogSource::Runtime),
                ..Default::default()
            },
        );
        assert!(
            logs.records
                .iter()
                .any(|r| r.message == "HIDDEN_CONSOLE_WARNING"
                    && r.operation_id.as_deref() == Some("same-operation"))
        );
        assert!(
            logs.records
                .iter()
                .any(|r| r.message == "HIDDEN_WORKER_ERROR")
        );
        assert!(!logs.records.iter().any(|r| r.message == "FILTERED_INFO"));
        if mode == "file" {
            let text = std::fs::read_to_string(&path).unwrap();
            assert!(
                text.contains("HIDDEN_CONSOLE_WARNING") && text.contains("HIDDEN_WORKER_ERROR")
            );
        }
        drop(outer);
        tracing::warn!("AFTER_CONSOLE");
        return;
    }
    for mode in ["file", "stderr"] {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "terminal_console_guard_preserves_collection_file_policy_and_restores_console",
                "--nocapture",
            ])
            .env("SINOMO_CONSOLE_GUARD_CHILD", mode)
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let console = if mode == "file" { &stdout } else { &stderr };
        assert!(
            console.contains("BEFORE_CONSOLE") && console.contains("AFTER_CONSOLE"),
            "{out:?}"
        );
        for hidden in [
            "HIDDEN_CONSOLE_WARNING",
            "HIDDEN_WORKER_ERROR",
            "STILL_HIDDEN",
        ] {
            assert!(
                !stdout.contains(hidden) && !stderr.contains(hidden),
                "{out:?}"
            );
        }
    }
}
