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
            .reply(sinomo_lm::ScriptedReply::new().text(serde_json::json!({
                "response_mode":"answer", "text":"probe denied effects", "next_state":"listening",
                "tool_calls":[
                    {"tool":"write_file","input":{"path":"forbidden.txt","content":"changed"},"async":false,"summary":"probe"},
                    {"tool":"run_command","input":{"command":"touch forbidden-command.txt"},"async":false,"summary":"probe"},
                    {"tool":"read_file","input":{"path":"../other/secret.txt"},"async":false,"summary":"probe"}
                ]
            }).to_string()))
            .reply(reply)
            .reply(sinomo_lm::ScriptedReply::new().tool_call("write_file", serde_json::json!({"path":"permitted.txt","content":"scoped edit"})))
            .reply(sinomo_lm::ScriptedReply::new().text(r#"{"response_mode":"answer","text":"writer finished","tool_calls":[],"next_state":"listening"}"#))
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
    let other = data.root().join("workers/jobs/other");
    std::fs::create_dir_all(&other).unwrap();
    std::fs::write(other.join("secret.txt"), "must stay isolated").unwrap();
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
        capabilities: vec!["read_workspace".into()],
        max_steps: Some(4),
    });
    let mut writer = worker.clone();
    writer.id = "writer".into();
    writer
        .worker
        .as_mut()
        .unwrap()
        .capabilities
        .push("write_workspace".into());
    descriptor.surfaces.agent_definitions = vec![worker, writer];
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
    let started = call(SessionRequest::Command {
        command: SessionCommand::Create {
            name: "Worker".into(),
            instructions: "finish the task".into(),
            prompt: String::new(),
            idle_timeout_secs: 86400,
            worker: Some(SessionWorkerBinding {
                agent_id: "worker".into(),
                provider: "scripted".into(),
                model: "scripted".into(),
                workspace: candidate.display().to_string(),
                context: vec![],
                max_steps: Some(2),
            }),
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
    assert!(!candidate.join("forbidden.txt").exists());
    assert!(!candidate.join("forbidden-command.txt").exists());
    assert_eq!(
        std::fs::read_to_string(other.join("secret.txt")).unwrap(),
        "must stay isolated"
    );
    let results = result["run"]["tool_results"].as_array().unwrap();
    assert_eq!(results.len(), 3, "{result}");
    assert!(
        results.iter().all(|r| r["status"] != "succeeded"),
        "{results:?}"
    );
    assert!(results.iter().filter(|r| r["status"] == "blocked").count() >= 2);
    assert!(!result.to_string().contains("must stay isolated"));
    let store = FileAgentSessionStore::for_data_dir(&data);
    assert!(
        !store
            .load_session(&id.parse().unwrap())
            .unwrap()
            .unwrap()
            .transcript
            .is_empty()
    );
    let persisted = store.load_session(&id.parse().unwrap()).unwrap().unwrap();
    assert_eq!(
        persisted
            .workspace_binding
            .as_ref()
            .unwrap()
            .local_root
            .as_ref(),
        Some(&candidate)
    );
    assert_eq!(
        persisted
            .workspace_binding
            .as_ref()
            .unwrap()
            .write_policy
            .as_deref(),
        Some("read_only")
    );
    call(SessionRequest::Command {
        command: SessionCommand::Archive { id },
    });
    let started = call(SessionRequest::Command {
        command: SessionCommand::Create {
            name: "Writer".into(),
            instructions: "make the scoped edit".into(),
            prompt: String::new(),
            idle_timeout_secs: 86400,
            worker: Some(SessionWorkerBinding {
                agent_id: "writer".into(),
                provider: "scripted".into(),
                model: "scripted".into(),
                workspace: other.display().to_string(),
                context: vec![],
                max_steps: Some(2),
            }),
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
    assert_eq!(
        std::fs::read_to_string(other.join("permitted.txt")).unwrap(),
        "scoped edit"
    );
    assert!(!candidate.join("permitted.txt").exists());
    call(SessionRequest::Command {
        command: SessionCommand::Archive { id },
    });
}
