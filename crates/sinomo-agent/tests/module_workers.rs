//! Exercise the production module host and configured provider without an installed tool module.
use sinomo_agent::{module_sessions::ModuleSessionHost, *};
use sinomo_runtime::{
    local::SinomoDataDir,
    module::*,
    module_api::{self, sessions::*},
};

#[test]
fn module_worker_runs_without_interactive_subagents() {
    if std::env::var_os("SINOMO_TEST_MODULE_WORKER_CHILD").is_none() {
        let root = sinomo_testdir::TestDir::new("module-worker-process");
        let script = root.path().join("script.json");
        let reply = sinomo_lm::ScriptedReply::from_response(&sinomo_lm::LanguageModelResponse {
            provider: "scripted".into(), model: "scripted".into(),
            content: r#"{"response_mode":"answer","text":"host worker finished","tool_calls":[],"next_state":"listening"}"#.into(),
            items: vec![], tool_calls: vec![], usage: None,
        });
        sinomo_lm::ModelScript::new()
            .reply(reply)
            .save(&script)
            .unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "module_worker_runs_without_interactive_subagents",
                "--nocapture",
            ])
            .env("SINOMO_TEST_MODULE_WORKER_CHILD", root.path())
            .env(sinomo_lm::SCRIPTED_PROVIDER_SCRIPT_ENV, script)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let data = SinomoDataDir::new(std::env::var_os("SINOMO_TEST_MODULE_WORKER_CHILD").unwrap());
    let candidate = data.root().join("workers/jobs/candidate");
    std::fs::create_dir_all(&candidate).unwrap();
    let host = ModuleSessionHost::new(&data);
    let mut descriptor = sinomo_subagents::descriptor();
    descriptor.module_id = "module.workers".into();
    let mut worker = module_api::NativeModuleAgentDefinition::new(
        "worker",
        "Worker",
        "deliberation",
        "Complete the task",
    );
    worker.worker = Some(SessionWorkerProfile {
        workspace_roots: vec!["workers/jobs".into()],
    });
    descriptor.surfaces.agent_definitions = vec![worker];
    let grants = host
        .host_services(
            data.root().as_path(),
            &descriptor,
            &NativeModuleOperationCancellationToken::new(),
        )
        .unwrap();
    let call = |request| -> serde_json::Value {
        let bytes = grants[SESSION_INTERFACE]
            .host_service(&module_api::host::HostServiceRequest {
                interface_id: SESSION_INTERFACE.into(),
                interface_version: SESSION_VERSION.into(),
                operation: SESSION_ACCESS.into(),
                payload_sobj: module_api::encode_sobj(&request).unwrap(),
            })
            .unwrap();
        module_api::decode_sobj(&bytes).unwrap()
    };
    let started = call(SessionRequest::Start {
        worker: SessionWorkerRequest {
            agent_id: "worker".into(),
            provider: "scripted".into(),
            model: "scripted".into(),
            workspace: candidate.display().to_string(),
            instructions: "finish the task".into(),
            context: vec![],
            max_steps: Some(2),
        },
    });
    let id = started["session_id"].as_str().unwrap().to_string();
    let state = call(SessionRequest::Command {
        command: SessionCommand::Wait {
            id: id.clone(),
            timeout_ms: 5000,
        },
    });
    assert_eq!(state["status"], "completed", "{state}");
    let result = call(SessionRequest::Command {
        command: SessionCommand::Result { id: id.clone() },
    });
    assert_eq!(
        result["run"]["final_decision"]["text"],
        "host worker finished"
    );
    let store = FileAgentSessionStore::for_data_dir(&data);
    assert!(
        !store
            .load_session(&id.parse().unwrap())
            .unwrap()
            .unwrap()
            .transcript
            .is_empty()
    );
    call(SessionRequest::Command {
        command: SessionCommand::Archive { id },
    });
}
