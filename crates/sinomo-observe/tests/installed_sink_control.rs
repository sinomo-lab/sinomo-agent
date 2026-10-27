use sinomo_observe::{
    ObserveControlRequest, ObserveRuntimeConfig, init_observe_runtime, observe_control_snapshot,
    update_observe_control,
};

#[test]
fn live_writing_control_requires_installed_owned_sinks() {
    const CHILD: &str = "SINOMO_TEST_OBSERVE_INSTALLED_SINK_CHILD";
    let Ok(mode) = std::env::var(CHILD) else {
        // A fresh process exercises the actual global subscriber boundary without
        // changing another test's tracing subscriber or runtime singleton.
        for mode in ["owned", "preinstalled"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "live_writing_control_requires_installed_owned_sinks",
                    "--nocapture",
                ])
                .env(CHILD, mode)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode}: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        }
        return;
    };
    let owned = match mode.as_str() {
        "owned" => true,
        "preinstalled" => {
            tracing::subscriber::set_global_default(tracing::subscriber::NoSubscriber::default())
                .unwrap();
            false
        }
        _ => panic!("unexpected child mode {mode}"),
    };
    init_observe_runtime(ObserveRuntimeConfig {
        process_role: "installed-sink-control".to_string(),
        node_id: None,
        filter: "trace".to_string(),
        quiet_filter_directives: Vec::new(),
        live_log_writing: false,
    })
    .unwrap();
    assert_eq!(observe_control_snapshot().live_log_writing, owned.then_some(false));
    for enabled in [true, false] {
        let result = update_observe_control(ObserveControlRequest {
            live_log_writing: Some(enabled),
            ..Default::default()
        });
        if owned {
            assert_eq!(result.unwrap().live_log_writing, Some(enabled));
            assert_eq!(observe_control_snapshot().live_log_writing, Some(enabled));
        } else {
            assert!(result.unwrap_err().to_string().contains("formatted sinks are not installed"));
            assert_eq!(observe_control_snapshot().live_log_writing, None);
        }
    }
}
