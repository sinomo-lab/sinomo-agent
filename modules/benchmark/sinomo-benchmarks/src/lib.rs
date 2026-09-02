//! Optional native-dylib benchmark module for validating Sinomo's agent loop.
//!
//! These adapters deliberately live outside the production runtime. They can be
//! built as an opt-in native-dylib module for catalog/inspection purposes, and
//! they still expose explicit research harness scripts for Terminal-Bench/Harbor,
//! SWE-bench, MCU, and MineStudio.

use serde::{Deserialize, Serialize};
use sinomo_agent::{
    AgentToolKind, AgentToolSpec, default_coding_agent_tools, default_main_agent_tools,
    main_agent_system_prompt,
};
use sinomo_pytorch::{MINICPM_DEFAULT_MODEL, PytorchDevice, PytorchRuntimeConfig};
use sinomo_runtime::module_api::{
    ComponentDescriptor, NativeModuleBackend, NativeModuleBodyMode,
    NativeModuleCapabilitiesResponse, NativeModuleDescriptor, NativeModuleErrorResponse,
    NativeModuleInvocationRequest, NativeModuleInvocationResponse, NativeModuleLifecycle,
    NativeModuleObjectRpcRoute, NativeModuleRequest, NativeModuleResponse, NativeModuleSurfaces,
    SINOMO_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_MODULE_STATUS_UNSUPPORTED_ABI,
    SINOMO_NATIVE_MODULE_ABI_VERSION, ServiceOffer, SinomoModuleByteBuffer, SinomoModuleByteSlice,
    SinomoModuleHostApiV2, SinomoModuleHostIsCancelledFn, SinomoModuleStatusCode, decode_sobj,
    encode_sobj,
};
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use thiserror::Error;

pub const SINOMO_BENCHMARKS_MODULE_ID: &str = "module.benchmarks";
pub const SINOMO_BENCHMARKS_INTERFACE_VERSION: &str = "sinomo.benchmarks.v1";
pub const SINOMO_BENCHMARKS_OPERATION: &str = "benchmarks.catalog";
pub const SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION: &str = "benchmarks.cancel-probe";
pub const SINOMO_BENCHMARKS_CANCEL_PROBE_ROUTE: &str = "/benchmarks/cancel-probe";
pub const SINOMO_BENCHMARKS_RPC_CATEGORY: &str = "benchmarks";
pub const SINOMO_BENCHMARKS_MODULE_ROOT: &str = "modules/benchmark/sinomo-benchmarks";

#[derive(Clone, Copy)]
struct StoredHostApi {
    user_data: usize,
    is_cancelled: Option<SinomoModuleHostIsCancelledFn>,
}

static MODULE_HOST_API: Mutex<Option<StoredHostApi>> = Mutex::new(None);
static MODULE_LIFECYCLE: NativeModuleLifecycle = NativeModuleLifecycle::new();

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkHarnessPlan {
    pub name: String,
    pub setup_commands: Vec<String>,
    pub run_command: String,
    pub required_tools: Vec<AgentToolKind>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkFamily {
    Coding,
    Terminal,
    VisionAction,
    AgentDiscussion,
    AgentSkill,
    Memory,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkSuiteDescriptor {
    pub name: String,
    pub family: BenchmarkFamily,
    pub task_count: usize,
    pub dataset: Option<String>,
    pub split: Option<String>,
    pub runner: String,
    pub baseline: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SweBenchSuite {
    pub name: String,
    pub dataset: String,
    pub split: String,
    pub selection: String,
    pub instances: Vec<SweBenchInstance>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SweBenchInstance {
    pub instance_id: String,
    pub repo: String,
    pub base_commit: String,
    pub difficulty: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BenchmarkModuleRequest {
    Catalog,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkCancelProbeRequest {
    pub max_runtime_ms: u64,
    pub poll_interval_ms: u64,
}

impl Default for BenchmarkCancelProbeRequest {
    fn default() -> Self {
        Self {
            max_runtime_ms: 0,
            poll_interval_ms: 25,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BenchmarkModuleResponse {
    Catalog(BenchmarkModuleCatalog),
    CancelProbe(BenchmarkCancelProbeResponse),
    Error(BenchmarkModuleError),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkCancelProbeResponse {
    pub cancelled: bool,
    pub elapsed_ms: u64,
    pub polls: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkModuleCatalog {
    pub module_root: String,
    pub suites: Vec<BenchmarkSuiteDescriptor>,
    pub plans: Vec<BenchmarkHarnessPlan>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkModuleError {
    pub code: String,
    pub detail: String,
}

pub fn benchmark_module_root() -> &'static str {
    SINOMO_BENCHMARKS_MODULE_ROOT
}

pub fn benchmark_module_path(relative_path: &str) -> String {
    format!(
        "{}/{}",
        SINOMO_BENCHMARKS_MODULE_ROOT,
        relative_path.trim_start_matches('/')
    )
}

fn required_tool_kinds(
    tools: Vec<AgentToolSpec>,
    required: &[AgentToolKind],
) -> Vec<AgentToolKind> {
    let mut kinds = tools
        .into_iter()
        .filter(|tool| required.contains(&tool.kind))
        .map(|tool| tool.kind)
        .collect::<Vec<_>>();
    for required_kind in required {
        if !kinds.contains(required_kind) {
            kinds.push(required_kind.clone());
        }
    }
    kinds
}

pub fn benchmark_module_catalog() -> BenchmarkModuleCatalog {
    BenchmarkModuleCatalog {
        module_root: SINOMO_BENCHMARKS_MODULE_ROOT.to_string(),
        suites: vec![
            swebench_verified_10_descriptor(),
            agent_channel_discussion_descriptor(),
            agent_skill_quality_descriptor(),
            longmemeval_s_descriptor(),
            memory_tools_longmemeval_s_descriptor(),
        ],
        plans: vec![
            swebench_verified_10_plan(),
            agent_skill_quality_plan(),
            agent_compaction_quality_plan(),
            agent_memory_quality_plan(),
            longmemeval_s_plan(),
            memory_tools_longmemeval_s_plan(),
            agent_channel_discussion_plan(),
            terminalbench_harbor_plan(),
            mcu_minestudio_plan(),
            minestudio_plan(),
            minestudio_vision_action_loop_plan(),
            minestudio_live_duplex_loop_plan(),
            sifs_fuse_benchmark_plan(),
        ],
    }
}

pub fn benchmarks_module_descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: SINOMO_BENCHMARKS_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "sinomo-benchmarks".to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SINOMO_BENCHMARKS_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: vec![
            SINOMO_BENCHMARKS_OPERATION.to_string(),
            SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string(),
        ],
        capabilities: vec![
            "benchmark.catalog.read".to_string(),
            "benchmark.agent.cancel-probe".to_string(),
            "benchmark.agent.compaction.plan".to_string(),
            "benchmark.agent.memory.plan".to_string(),
            "benchmark.agent.memory.longmemeval-s.plan".to_string(),
            "benchmark.agent.memory.memory-tools-longmemeval-s.plan".to_string(),
            "benchmark.agent.channel-discussion.plan".to_string(),
            "benchmark.agent.skill.plan".to_string(),
            "benchmark.swebench.plan".to_string(),
            "benchmark.minestudio.plan".to_string(),
            "benchmark.sifs-fuse.plan".to_string(),
            "benchmark.terminalbench.plan".to_string(),
        ],
        surfaces: NativeModuleSurfaces {
            object_rpc_routes: vec![NativeModuleObjectRpcRoute {
                route: SINOMO_BENCHMARKS_CANCEL_PROBE_ROUTE.to_string(),
                category: SINOMO_BENCHMARKS_RPC_CATEGORY.to_string(),
                operation: SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string(),
                auth: "local-or-cluster-member".to_string(),
                request_body_mode: NativeModuleBodyMode::None,
                response_body_mode: NativeModuleBodyMode::None,
                required_local_permissions: Vec::new(),
                exact_member_identity: false,
                concurrency_limit: None,
                timeout_ms: None,
            }],
            capability_exports: vec![
                SINOMO_BENCHMARKS_OPERATION.to_string(),
                SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string(),
            ],
            ..Default::default()
        },
        component: ComponentDescriptor::new(SINOMO_BENCHMARKS_MODULE_ID, env!("CARGO_PKG_VERSION"))
            .with_offer(
                ServiceOffer::new(
                    SINOMO_BENCHMARKS_RPC_CATEGORY,
                    "sinomo.benchmarks/v1",
                    "1.0.0",
                )
                .with_provider_capability(SINOMO_BENCHMARKS_OPERATION),
            ),
    }
}

pub fn swebench_verified_10_suite() -> SweBenchSuite {
    serde_json::from_str(include_str!("../swebench/verified_10.json"))
        .expect("checked-in SWE-bench Verified suite is valid JSON")
}

pub fn swebench_verified_10_descriptor() -> BenchmarkSuiteDescriptor {
    let suite = swebench_verified_10_suite();
    BenchmarkSuiteDescriptor {
        name: suite.name,
        family: BenchmarkFamily::Coding,
        task_count: suite.instances.len(),
        dataset: Some(suite.dataset),
        split: Some(suite.split),
        runner: benchmark_module_path("swebench/run_swebench_verified.py"),
        baseline: Some("mini-extra swebench".to_string()),
    }
}

pub fn agent_channel_discussion_descriptor() -> BenchmarkSuiteDescriptor {
    BenchmarkSuiteDescriptor {
        name: "agent-channel-discussion".to_string(),
        family: BenchmarkFamily::AgentDiscussion,
        task_count: 8,
        dataset: Some("native-fixtures-plus-clembench-derived-miniatures".to_string()),
        split: Some("fixture-and-live-subset".to_string()),
        runner: benchmark_module_path("agent/run_channel_discussion_eval.py"),
        baseline: Some("single-role or official clembench backend baseline".to_string()),
    }
}

pub fn agent_skill_quality_descriptor() -> BenchmarkSuiteDescriptor {
    BenchmarkSuiteDescriptor {
        name: "agent-skill-quality".to_string(),
        family: BenchmarkFamily::AgentSkill,
        task_count: 2,
        dataset: Some("benchmark-only-active-skill-fixtures".to_string()),
        split: Some("fixture-and-live-model".to_string()),
        runner: benchmark_module_path("agent/run_skill_eval.py"),
        baseline: Some("no-skill and no-update ablation".to_string()),
    }
}

pub fn longmemeval_s_descriptor() -> BenchmarkSuiteDescriptor {
    BenchmarkSuiteDescriptor {
        name: "agent-memory-longmemeval-s".to_string(),
        family: BenchmarkFamily::Memory,
        task_count: 500,
        dataset: Some(
            "xiaowu0162/longmemeval-cleaned@98d7416c24c778c2fee6e6f3006e7a073259d48f".to_string(),
        ),
        split: Some("longmemeval_s_cleaned".to_string()),
        runner: benchmark_module_path("agent/run_longmemeval_s.py"),
        baseline: Some("Sinomo indexed-file lexical retrieval".to_string()),
    }
}

pub fn memory_tools_longmemeval_s_descriptor() -> BenchmarkSuiteDescriptor {
    BenchmarkSuiteDescriptor {
        name: "agent-memory-tools-longmemeval-s".to_string(),
        family: BenchmarkFamily::Memory,
        task_count: 500,
        dataset: Some(
            "xiaowu0162/longmemeval-cleaned@98d7416c24c778c2fee6e6f3006e7a073259d48f".to_string(),
        ),
        split: Some("longmemeval_s_cleaned".to_string()),
        runner: benchmark_module_path("agent/run_longmemeval_s.py"),
        baseline: Some(
            "Sinomo session-history and memory-note tools over an isolated VFS".to_string(),
        ),
    }
}

pub fn agent_skill_quality_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "agent-skill-quality".to_string(),
        setup_commands: vec![
            "cargo build -p sinomo".to_string(),
            format!(
                "python3 {} --mode plan",
                benchmark_module_path("agent/run_skill_eval.py")
            ),
            format!(
                "python3 {} --mode fixture --summary-out benchmark-results/agent-skill/fixture-summary.json",
                benchmark_module_path("agent/run_skill_eval.py")
            ),
        ],
        run_command: format!(
            "python3 {} --mode run --provider codex --model gpt-5.5 --summary-out benchmark-results/agent-skill/summary.json --progress-out benchmark-results/agent-skill/progress.jsonl",
            benchmark_module_path("agent/run_skill_eval.py")
        ),
        required_tools: vec![
            AgentToolKind::SkillSearch,
            AgentToolKind::SkillDescribe,
            AgentToolKind::SkillUpdate,
        ],
        notes: vec![
            "This is a live/model eval harness for progressive skill disclosure and skill evolution writeback, not a normal Rust unit test.".to_string(),
            "The hidden-skill scenario requires reading active SKILL.md text through skill_search/skill_describe; the expected marker is absent from the user prompt.".to_string(),
            "The update scenario requires skill_update to persist a revealed shortcut, then verifies a fresh follow-up run can use the updated skill.".to_string(),
            "Run mode seeds benchmark-only skills into a temporary SINOMO_DATA_DIR and copies only local model provider configuration from the operator data dir.".to_string(),
        ],
    }
}

pub fn agent_channel_discussion_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "agent-channel-discussion".to_string(),
        setup_commands: vec![
            "cargo build -p sinomo".to_string(),
            format!(
                "python3 {} --mode plan --suite all",
                benchmark_module_path("agent/run_channel_discussion_eval.py")
            ),
            format!(
                "python3 {} --mode fixture --suite all --summary-out benchmark-results/agent-channel-discussion/fixture-summary.json",
                benchmark_module_path("agent/run_channel_discussion_eval.py")
            ),
        ],
        run_command: format!(
            "python3 {} --mode run --suite clembench-lite --provider codex --model gpt-5.4-mini --summary-out benchmark-results/agent-channel-discussion/summary.json --progress-out benchmark-results/agent-channel-discussion/progress.jsonl",
            benchmark_module_path("agent/run_channel_discussion_eval.py")
        ),
        required_tools: required_tool_kinds(
            default_main_agent_tools(),
            &[AgentToolKind::RunShellCommand],
        ),
        notes: vec![
            "This is a live/model eval harness for channel and multi-agent discussion behavior, not a Rust unit test.".to_string(),
            "Fixture mode covers native consensus, hidden-info merge, useful silence, response-limit containment, and final handoff cases.".to_string(),
            "The clembench-lite suite contains small Taboo, private/shared, and reference-game miniatures inspired by clembench communication games.".to_string(),
            "The optional --mode clembench adapter prepares a trimmed upstream clembench workspace with a generated Sinomo clemcore backend.".to_string(),
        ],
    }
}

pub fn agent_compaction_quality_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "agent-compaction-quality".to_string(),
        setup_commands: vec![
            "cargo build -p sinomo".to_string(),
            format!(
                "python3 {} --mode plan",
                benchmark_module_path("agent/run_compaction_eval.py")
            ),
            format!(
                "python3 {} --mode fixture --summary-out benchmark-results/agent-compaction/fixture-summary.json",
                benchmark_module_path("agent/run_compaction_eval.py")
            ),
        ],
        run_command: format!(
            "python3 {} --mode run --agent coding-agent --provider codex --model gpt-5.5 --summary-out benchmark-results/agent-compaction/summary.json --progress-out benchmark-results/agent-compaction/progress.jsonl",
            benchmark_module_path("agent/run_compaction_eval.py")
        ),
        required_tools: required_tool_kinds(
            default_coding_agent_tools(),
            &[
                AgentToolKind::ReadFile,
                AgentToolKind::SearchFiles,
                AgentToolKind::ApplyPatch,
                AgentToolKind::RunShellCommand,
            ],
        ),
        notes: vec![
            "This is a live/model eval harness for compaction quality, not a normal Rust unit test.".to_string(),
            "Checkpoint turns use --compact-after to submit AgentOp::compact; continuation runs start a new CLI process and resume the resulting durable projection.".to_string(),
            "The checked-in plan covers long-session resume, constraint retention, coding after compaction, and cluster-ops after compaction.".to_string(),
            "Default plan/fixture modes are deterministic; --mode run calls the configured Sinomo agent and scores marker retention in generated summaries and post-compaction probes.".to_string(),
        ],
    }
}

pub fn agent_memory_quality_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "agent-memory-quality".to_string(),
        setup_commands: vec![
            "cargo build -p sinomo".to_string(),
            format!(
                "python3 {} --mode plan",
                benchmark_module_path("agent/run_memory_eval.py")
            ),
            format!(
                "python3 {} --mode fixture --summary-out benchmark-results/agent-memory/fixture-summary.json",
                benchmark_module_path("agent/run_memory_eval.py")
            ),
        ],
        run_command: format!(
            "python3 {} --mode run --provider codex --model gpt-5.2-codex --summary-out benchmark-results/agent-memory/summary.json --progress-out benchmark-results/agent-memory/progress.jsonl",
            benchmark_module_path("agent/run_memory_eval.py")
        ),
        required_tools: required_tool_kinds(
            default_main_agent_tools(),
            &[AgentToolKind::RunShellCommand],
        ),
        notes: vec![
            "This is a live/model eval harness for long-horizon memory quality, not a Rust unit test.".to_string(),
            "The checked-in scenarios cover information extraction, multi-session reasoning, knowledge updates, temporal reasoning, scope-boundary abstention, and procedural workflow recall.".to_string(),
            "The axes are inspired by LongMemEval, LoCoMo, BEAM, MemoryAgentBench, MemoryArena, LongMemEval-V2, and RAGAS retrieval metrics.".to_string(),
            "Default plan/fixture modes are deterministic; --mode run calls the configured Sinomo agent and scores marker use, citations, forbidden leakage, source coverage, and cache churn.".to_string(),
        ],
    }
}

pub fn longmemeval_s_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "agent-memory-longmemeval-s".to_string(),
        setup_commands: vec![
            "cargo build -p sinomo".to_string(),
            format!(
                "python3 {} --mode plan --summary-out benchmark-results/agent-memory/longmemeval-s/plan.json",
                benchmark_module_path("agent/run_longmemeval_s.py")
            ),
        ],
        run_command: format!(
            "python3 {} --mode run --dataset benchmark-results/datasets/longmemeval_s_cleaned.json --reader none --summary-out benchmark-results/agent-memory/longmemeval-s/summary.json --retrieval-out benchmark-results/agent-memory/longmemeval-s/retrieval.jsonl --progress-out benchmark-results/agent-memory/longmemeval-s/progress.jsonl",
            benchmark_module_path("agent/run_longmemeval_s.py")
        ),
        required_tools: vec![AgentToolKind::RunShellCommand],
        notes: vec![
            "The cleaned LongMemEval-S suite contains 500 questions and requires an explicit path to the pinned external dataset; the harness never downloads it implicitly."
                .to_string(),
            "The catalog command is the deterministic retrieval-only baseline; add --reader sinomo and --predictions-out to emit the official question_id/hypothesis reader contract."
                .to_string(),
            "Every question runs in a fresh disposable SINOMO_DATA_DIR, HOME, XDG, temporary, and workspace tree so synthetic histories cannot mix with one another or normal operational data."
                .to_string(),
            "Only the explicitly selected account configuration and selected provider credential files may be copied into a reader case; unrelated accounts plus cluster, DFS, permissions, events, chats, sessions, memory, and index state are never copied from the operator data directory."
                .to_string(),
        ],
    }
}

pub fn memory_tools_longmemeval_s_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "agent-memory-tools-longmemeval-s".to_string(),
        setup_commands: vec![
            "cargo build --release -p sinomo-benchmarks --features memory-tools-bench --bin sinomo-memory-tools-bench"
                .to_string(),
            format!(
                "python3 {} --mode plan --retriever memory-tools --summary-out benchmark-results/agent-memory/memory-tools-longmemeval-s/plan.json",
                benchmark_module_path("agent/run_longmemeval_s.py")
            ),
        ],
        run_command: format!(
            "python3 {} --mode run --retriever memory-tools --dataset benchmark-results/datasets/longmemeval_s_cleaned.json --reader none --summary-out benchmark-results/agent-memory/memory-tools-longmemeval-s/summary.json --retrieval-out benchmark-results/agent-memory/memory-tools-longmemeval-s/retrieval.jsonl --progress-out benchmark-results/agent-memory/memory-tools-longmemeval-s/progress.jsonl",
            benchmark_module_path("agent/run_longmemeval_s.py")
        ),
        required_tools: vec![
            AgentToolKind::SessionHistorySearch,
            AgentToolKind::MemorySearch,
            AgentToolKind::MemoryWrite,
            AgentToolKind::RunShellCommand,
        ],
        notes: vec![
            "This baseline uses the same pinned 500-question LongMemEval-S dataset, fixed development subset, and retrieval metrics as the indexed-file baseline."
                .to_string(),
            "Each case seeds background activities and Markdown notes through the production memory tools on a fresh in-memory VFS; it does not require DFS, a daemon, or operator state."
                .to_string(),
            "Question terms are deterministic and query both session_history_search and memory_search; the report includes each source ranking plus their reciprocal-rank fusion."
                .to_string(),
        ],
    }
}

pub fn sifs_fuse_benchmark_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "sifs-fuse".to_string(),
        setup_commands: vec![
            "cargo test -p sinomo-benchmarks --features sifs-fuse-bench --bin sinomo-sifs-fuse-bench"
                .to_string(),
            "cargo build --release -p sinomo-benchmarks --features sifs-fuse-bench --bin sinomo-sifs-fuse-bench"
                .to_string(),
        ],
        run_command: "target/release/sinomo-sifs-fuse-bench --root \"$SINOMO_SIFS_FUSE_BENCH_ROOT\" --suite full --require-fuse --min-sequential-read-mib-s \"${SINOMO_SIFS_FUSE_MIN_SEQUENTIAL_READ_MIB_S:-0}\" --min-parallel-create-speedup \"${SINOMO_SIFS_FUSE_MIN_PARALLEL_CREATE_SPEEDUP:-0}\" --json-out benchmark-results/sifs-fuse/report.json"
            .to_string(),
        required_tools: vec![AgentToolKind::RunShellCommand],
        notes: vec![
            "This is an opt-in Unix integration and performance harness for the kernel FUSE mount over a live SIFS cluster, not a normal Rust unit test."
                .to_string(),
            "SINOMO_SIFS_FUSE_BENCH_ROOT must identify an existing caller-writable SIFS subtree; every run uses a unique child and removes it on exit."
                .to_string(),
            "Correctness failures produce a non-zero exit; performance thresholds are opt-in so unconfigured runs report measurements without imposing host-specific gates."
                .to_string(),
            "Set SINOMO_SIFS_FUSE_MIN_SEQUENTIAL_READ_MIB_S and SINOMO_SIFS_FUSE_MIN_PARALLEL_CREATE_SPEEDUP to make the catalog run enforce read-throughput and concurrency-scaling floors."
                .to_string(),
        ],
    }
}

pub fn terminalbench_harbor_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "terminalbench-harbor".to_string(),
        setup_commands: vec![
            "python -m pip install harbor".to_string(),
            "harbor run --help".to_string(),
        ],
        run_command: "harbor run -d terminal-bench/terminal-bench-2 --agent-import-path sinomo_harbor_agent:SinomoHarborAgent"
            .to_string(),
        required_tools: required_tool_kinds(
            default_main_agent_tools(),
            &[
                AgentToolKind::RunShellCommand,
                AgentToolKind::StartAsyncTask,
                AgentToolKind::ReadSifs,
                AgentToolKind::WriteSifs,
            ],
        ),
        notes: vec![
            "Harbor custom agents implement BaseAgent or BaseInstalledAgent; current Terminal-Bench 2 dataset id is terminal-bench/terminal-bench-2.".to_string(),
            "The generated adapter uses an external agent so Sinomo can keep tools outside the task container."
                .to_string(),
        ],
    }
}

pub fn swebench_verified_10_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "swebench-verified-10".to_string(),
        setup_commands: vec![
            "python3 -m pip install mini-swe-agent swebench".to_string(),
            "cargo build -p sinomo-benchmarks --bin sinomo-swebench-agent".to_string(),
            "sinomo --local providers test deepseek".to_string(),
        ],
        run_command: format!(
            "python3 {} --mode both --provider deepseek --model deepseek-v4-flash --baseline-model deepseek/deepseek-v4-flash --evaluate --summary-out benchmark-results/swebench/summary.json --progress-out benchmark-results/swebench/progress.jsonl",
            benchmark_module_path("swebench/run_swebench_verified.py")
        ),
        required_tools: required_tool_kinds(
            default_coding_agent_tools(),
            &[
                AgentToolKind::ReadFile,
                AgentToolKind::SearchFiles,
                AgentToolKind::ApplyPatch,
                AgentToolKind::RunShellCommand,
                AgentToolKind::RequestUserInput,
            ],
        ),
        notes: vec![
            "The Sinomo path runs the Rust coding Agent with local workspace tools and writes SWE-bench prediction JSONL.".to_string(),
            "The baseline path delegates to mini-SWE-agent with a regex filter over the same 10 Verified instance ids.".to_string(),
            "Official scoring remains the SWE-bench harness over princeton-nlp/SWE-bench_Verified.".to_string(),
        ],
    }
}

pub fn minestudio_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "minestudio-smoke".to_string(),
        setup_commands: vec![
            "python -m pip install minestudio".to_string(),
            "python -m minestudio.simulator.entry".to_string(),
        ],
        run_command: "python minestudio/sinomo_minestudio_smoke.py".to_string(),
        required_tools: vec![AgentToolKind::ObserveVision, AgentToolKind::RunShellCommand],
        notes: vec![
            "MineStudio requires JDK 8 plus Xvfb or VirtualGL for rendering.".to_string(),
            "The smoke path captures one observation frame and sends it through the MiniCPM vision mode."
                .to_string(),
            "The Docker runtime under minestudio/docker starts a real MinecraftSim behind an HTTP reset/observe/step wrapper."
                .to_string(),
        ],
    }
}

pub fn mcu_minestudio_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "mcu-minestudio".to_string(),
        setup_commands: vec![
            "git clone --depth 1 https://github.com/CraftJarvis/MCU.git /tmp/mcu".to_string(),
            format!(
                "docker compose -f {} up -d --build minestudio",
                benchmark_module_path("minestudio/docker/compose.yml")
            ),
            format!(
                "python {} --help",
                benchmark_module_path("mcu/mcu_live_duplex_runner.py")
            ),
        ],
        run_command: format!(
            "MCU_ROOT=/tmp/mcu SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python {}",
            benchmark_module_path("mcu/mcu_live_duplex_runner.py")
        ),
        required_tools: vec![AgentToolKind::ObserveVision, AgentToolKind::RunShellCommand],
        notes: vec![
            "MCU-turbo task configs are consumed from MCU_benchmark/task_configs and executed through the MineStudio HTTP wrapper.".to_string(),
            "The full-duplex runner sends frame sequences to MiniCPM-o with stream=true and records MineStudio RecordCallback MP4s for every task.".to_string(),
        ],
    }
}

pub fn mcu_game_tool_specs() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ObserveVision,
            "observe_vision",
            "Attach the current Minecraft camera frame for multimodal reasoning.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "inspect_game_state",
            "Read structured Minecraft state provided by the benchmark harness: task text, inventory, nearby blocks, entities, health, hunger, and position.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "send_game_action",
            "Issue one bounded Minecraft action macro through the benchmark harness: look, move, jump, attack, mine, place, craft, use, select_hotbar, or wait.",
            true,
        ),
    ]
}

pub fn minestudio_vision_action_loop_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "minestudio-vision-action-loop".to_string(),
        setup_commands: vec![
            "python -m pip install minestudio".to_string(),
            format!(
                "docker compose -f {} build",
                benchmark_module_path("minestudio/docker/compose.yml")
            ),
            format!(
                "python {} --help",
                benchmark_module_path("minestudio/minestudio_vision_action_loop.py")
            ),
        ],
        run_command: format!(
            "SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python {}",
            benchmark_module_path("minestudio/minestudio_vision_action_loop.py")
        ),
        required_tools: vec![AgentToolKind::ObserveVision],
        notes: vec![
            "The loop starts a real MineStudio MinecraftSim environment and sends task text plus rendered pov frames to MiniCPM-o.".to_string(),
            "It does not include structured game-state text such as inventory, position, nearby blocks, health, reward, or simulator info dictionaries.".to_string(),
            "Recovered action macros are mapped through the benchmark-local MineStudio action bridge and recorded as replay trajectory artifacts.".to_string(),
        ],
    }
}

pub fn minestudio_live_duplex_loop_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "minestudio-live-duplex-loop".to_string(),
        setup_commands: vec![
            format!(
                "docker compose -f {} up -d minestudio",
                benchmark_module_path("minestudio/docker/compose.yml")
            ),
            format!(
                "python {} --help",
                benchmark_module_path("minestudio/minestudio_live_duplex_loop.py")
            ),
        ],
        run_command: format!(
            "SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python {}",
            benchmark_module_path("minestudio/minestudio_live_duplex_loop.py")
        ),
        required_tools: vec![AgentToolKind::ObserveVision],
        notes: vec![
            "The loop sends chronological MineStudio rendered frame sequences to MiniCPM-o with stream=true and consumes streaming response chunks.".to_string(),
            "It applies the first recovered action macro back to the paused MineStudio environment, so wall-clock speed does not affect benchmark correctness.".to_string(),
            "Each run asks MineStudio's RecordCallback to write high-frame-rate episode MP4s next to the JSON and observation PNG artifacts.".to_string(),
        ],
    }
}

pub fn harbor_external_agent_python() -> &'static str {
    r#"import json
import os
import shlex
import subprocess
from pathlib import Path

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext


def run_sinomo_agent(prompt: str) -> str:
    default_provider = "minicpm-o" if os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT")) else "codex"
    default_model = (
        "openbmb/MiniCPM-o-4_5"
        if default_provider == "minicpm-o"
        else "gpt-5.2-codex"
    )
    command = [
        os.environ.get("SINOMO_BIN", "sinomo"),
        "agent",
        "run",
        "--provider",
        os.environ.get("SINOMO_HARBOR_PROVIDER", default_provider),
        "--model",
        os.environ.get("SINOMO_HARBOR_MODEL", default_model),
        "--prompt",
        prompt,
    ]
    proc = subprocess.run(command, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        return (
            "Sinomo host agent failed; fall back to direct shell inspection. "
            f"stderr={proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def exec_output_text(result, field: str) -> str:
    value = getattr(result, field, None)
    return value if value is not None else ""


class SinomoHarborAgent(BaseAgent):
    @staticmethod
    def name() -> str:
        return "sinomo"

    def version(self) -> str | None:
        return "0.1.0"

    async def setup(self, environment: BaseEnvironment) -> None:
        host = subprocess.run(
            [os.environ.get("SINOMO_BIN", "sinomo"), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        if host.returncode != 0:
            raise RuntimeError(f"sinomo host binary is unavailable: {host.stderr}")
        await environment.exec("pwd && ls -la")

    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        first_probe = await environment.exec("pwd && find . -maxdepth 2 -type f | sort | head -80")
        first_probe_stdout = exec_output_text(first_probe, "stdout")
        prompt = json.dumps(
            {
                "benchmark": "terminalbench-harbor",
                "instruction": instruction,
                "environment_probe": first_probe_stdout,
                "available_action": "Return a concise shell plan. The adapter will execute one bounded shell command if it is safe.",
            },
            indent=2,
        )
        decision = run_sinomo_agent(prompt)
        safe_command = "true"
        if "pytest" in decision:
            safe_command = "pytest -q"
        elif "test.sh" in first_probe_stdout:
            safe_command = "bash test.sh"
        elif "ls" in decision:
            safe_command = "ls -la"
        result = await environment.exec(safe_command)
        stdout = exec_output_text(result, "stdout")
        stderr = exec_output_text(result, "stderr")
        output = (
            "sinomo_decision:\n"
            + decision
            + "\n\nexecuted:\n"
            + shlex.quote(safe_command)
            + "\n\nstdout:\n"
            + stdout
            + "\nstderr:\n"
            + stderr
        )
        logs_dir = Path(self.logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)
        output_path = logs_dir / "sinomo-output.txt"
        output_path.write_text(output)
        context.metadata = {
            **(context.metadata or {}),
            "sinomo_output": str(output_path),
            "sinomo_command": safe_command,
        }
"#
}

pub fn minestudio_smoke_python() -> &'static str {
    r#"import base64
import json
import os
import subprocess
import tempfile
import urllib.request
from pathlib import Path

from PIL import Image


def main():
    frame = Image.new("RGB", (64, 64), color=(90, 160, 90))
    with tempfile.TemporaryDirectory() as tmp:
        image_path = Path(tmp) / "frame.png"
        frame.save(image_path)
        request = {
            "mode": "vision",
            "system_prompt": "Describe the Minecraft observation and choose the next high-level action.",
            "messages": [{"role": "user", "content": "What should the agent do next?"}],
            "image_path": str(image_path),
            "stream": False,
            "metadata": {"benchmark": "minestudio-smoke"},
        }
        endpoint = os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT"))
        if endpoint:
            request.pop("image_path", None)
            request["image_name"] = image_path.name
            request["image_base64"] = base64.b64encode(image_path.read_bytes()).decode("ascii")
            http_request = urllib.request.Request(
                endpoint.rstrip("/") + "/complete",
                data=json.dumps(request).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(http_request, timeout=300) as response:
                print(response.read().decode("utf-8"))
            return 0
        proc = subprocess.run(
            ["sinomo-pytorch-bridge"],
            input=json.dumps(request),
            text=True,
            capture_output=True,
            check=False,
        )
        print(proc.stdout)
        return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
"#
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentEnvironmentConfig {
    pub pytorch: PytorchRuntimeConfig,
    pub prompt: String,
}

impl Default for AgentEnvironmentConfig {
    fn default() -> Self {
        Self {
            pytorch: PytorchRuntimeConfig {
                model: MINICPM_DEFAULT_MODEL.to_string(),
                device: PytorchDevice::Auto,
                ..PytorchRuntimeConfig::default()
            },
            prompt: main_agent_system_prompt(&default_main_agent_tools()),
        }
    }
}

#[derive(Debug, Error)]
pub enum BenchmarkError {
    #[error("benchmark environment `{name}` is disabled; rebuild with feature `env-bench`")]
    Disabled { name: String },
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[cfg(feature = "env-bench")]
pub fn docker_smoke_command(plan: &BenchmarkHarnessPlan) -> String {
    match plan.name.as_str() {
        "terminalbench-harbor" => "docker run --rm python:3.12 bash -lc 'pip install harbor && harbor run --help'".to_string(),
        "minestudio-smoke" => "docker run --rm python:3.10 bash -lc 'pip install minestudio && python - <<\\'PY\\'\nimport minestudio\nprint(\"minestudio import ok\")\nPY'".to_string(),
        "mcu-minestudio" => "docker run --rm python:3.10 bash -lc 'pip install minestudio && python - <<\\'PY\\'\nimport minestudio\nprint(\"mcu minestudio import ok\")\nPY'".to_string(),
        "minestudio-vision-action-loop" => "docker run --rm python:3.10 bash -lc 'pip install minestudio && python - <<\\'PY\\'\nfrom minestudio.simulator import MinecraftSim\nprint(\"real minestudio env import ok\", MinecraftSim)\nPY'".to_string(),
        "minestudio-live-duplex-loop" => format!(
            "python3 {} --help",
            benchmark_module_path("minestudio/minestudio_live_duplex_loop.py")
        ),
        _ => "docker --version".to_string(),
    }
}

#[cfg(not(feature = "env-bench"))]
pub fn docker_smoke_command(plan: &BenchmarkHarnessPlan) -> Result<String, BenchmarkError> {
    Err(BenchmarkError::Disabled {
        name: plan.name.clone(),
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&benchmarks_module_descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    if !MODULE_LIFECYCLE.initialize() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    match MODULE_HOST_API.lock() {
        Ok(mut stored) => {
            *stored = Some(StoredHostApi {
                user_data: host.user_data as usize,
                is_cancelled: host.is_cancelled,
            });
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => {
            let _ = MODULE_LIFECYCLE.begin_shutdown();
            SINOMO_MODULE_STATUS_RUNTIME_ERROR
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let Some(_invocation) = MODULE_LIFECYCLE.begin_invocation() else {
        return write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "module-unavailable",
                "benchmark module is not initialized or is shutting down",
            )),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        );
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        invoke_unchecked(request_sobj, response_sobj)
    }));
    match result {
        Ok(status) => status,
        Err(_) => write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "panic",
                "benchmark module panicked while handling invocation",
            )),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        ),
    }
}

fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let response = match decode_native_module_request(request_sobj) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: benchmarks_module_descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => invoke_benchmark_operation(request),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "invalid-request",
            &format!("decode benchmark module request: {error}"),
        )),
    };
    write_response(response_sobj, response, SINOMO_MODULE_STATUS_OK)
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    if !MODULE_LIFECYCLE.begin_shutdown() {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    }
    match MODULE_HOST_API.lock() {
        Ok(mut stored) => {
            *stored = None;
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

fn invoke_benchmark_operation(request: NativeModuleInvocationRequest) -> NativeModuleResponse {
    if request.operation == SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION {
        return invoke_cancel_probe_operation(request);
    }
    if request.operation != SINOMO_BENCHMARKS_OPERATION {
        return NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!(
                "unsupported benchmark module operation `{}`",
                request.operation
            ),
        ));
    }
    let benchmark_request = match decode_sobj::<BenchmarkModuleRequest>(&request.payload_sobj) {
        Ok(request) => request,
        Err(error) => {
            return NativeModuleResponse::Error(native_error_response(
                "invalid-benchmark-request",
                &format!("decode benchmark catalog request: {error}"),
            ));
        }
    };
    let benchmark_response = match benchmark_request {
        BenchmarkModuleRequest::Catalog => {
            BenchmarkModuleResponse::Catalog(benchmark_module_catalog())
        }
    };
    match encode_sobj(&benchmark_response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: SINOMO_BENCHMARKS_OPERATION.to_string(),
            payload_sobj,
            response_body: None,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-benchmark-response",
            &format!("encode benchmark catalog response: {error}"),
        )),
    }
}

fn invoke_cancel_probe_operation(request: NativeModuleInvocationRequest) -> NativeModuleResponse {
    let probe_request = decode_sobj::<BenchmarkCancelProbeRequest>(&request.payload_sobj)
        .unwrap_or_else(|_| BenchmarkCancelProbeRequest::default());
    let poll_interval_ms = probe_request.poll_interval_ms.clamp(1, 250);
    let deadline = Duration::from_millis(probe_request.max_runtime_ms.min(300_000));
    let poll_interval = Duration::from_millis(poll_interval_ms);
    let started = Instant::now();
    let mut polls = 0_u64;

    loop {
        polls = polls.saturating_add(1);
        if benchmark_host_is_cancelled() {
            return encode_cancel_probe_response(BenchmarkCancelProbeResponse {
                cancelled: true,
                elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                polls,
            });
        }
        if started.elapsed() >= deadline {
            return encode_cancel_probe_response(BenchmarkCancelProbeResponse {
                cancelled: false,
                elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                polls,
            });
        }
        std::thread::sleep(poll_interval);
    }
}

fn encode_cancel_probe_response(response: BenchmarkCancelProbeResponse) -> NativeModuleResponse {
    match encode_sobj(&response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string(),
            payload_sobj,
            response_body: None,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-cancel-probe-response",
            &format!("encode benchmark cancel-probe response: {error}"),
        )),
    }
}

fn benchmark_host_is_cancelled() -> bool {
    let Ok(stored) = MODULE_HOST_API.lock() else {
        return false;
    };
    let Some(stored) = *stored else {
        return false;
    };
    let Some(is_cancelled) = stored.is_cancelled else {
        return false;
    };
    unsafe { is_cancelled(stored.user_data as *mut c_void) != 0 }
}

fn decode_native_module_request(
    request_sobj: SinomoModuleByteSlice,
) -> Result<NativeModuleRequest, sinomo_runtime::module_api::NativeModuleApiError> {
    let request_bytes = unsafe { request_sobj.as_slice() };
    decode_sobj(request_bytes)
}

fn native_error_response(code: &str, detail: &str) -> NativeModuleErrorResponse {
    NativeModuleErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

fn write_response(
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
    success_status: SinomoModuleStatusCode,
) -> SinomoModuleStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => {
            let status = write_buffer(out, Ok(bytes));
            if status == SINOMO_MODULE_STATUS_OK {
                success_status
            } else {
                status
            }
        }
        Err(error) => write_buffer(
            out,
            encode_sobj(&NativeModuleResponse::Error(native_error_response(
                "encode-native-response",
                &format!("encode native benchmark module response: {error}"),
            ))),
        ),
    }
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_runtime::module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    match bytes {
        Ok(bytes) => {
            if out.is_null() {
                SINOMO_MODULE_STATUS_INVALID_ARGUMENT
            } else {
                unsafe {
                    *out = SinomoModuleByteBuffer::from_vec(bytes);
                }
                SINOMO_MODULE_STATUS_OK
            }
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    static MODULE_ABI_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn terminalbench_plan_uses_harbor_custom_agent_shape() {
        let plan = terminalbench_harbor_plan();
        assert!(plan.run_command.contains("--agent-import-path"));
        assert!(harbor_external_agent_python().contains("BaseAgent"));
        assert!(harbor_external_agent_python().contains("run_sinomo_agent"));
        assert!(harbor_external_agent_python().contains("environment.exec"));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
    }

    #[test]
    fn swebench_verified_suite_is_small_diverse_and_stable() {
        let suite = swebench_verified_10_suite();
        assert_eq!(suite.dataset, "princeton-nlp/SWE-bench_Verified");
        assert_eq!(suite.split, "test");
        assert_eq!(suite.instances.len(), 10);
        let instance_ids: BTreeSet<_> = suite
            .instances
            .iter()
            .map(|instance| instance.instance_id.as_str())
            .collect();
        let repos: BTreeSet<_> = suite
            .instances
            .iter()
            .map(|instance| instance.repo.as_str())
            .collect();
        let mut difficulties = BTreeMap::new();
        for instance in &suite.instances {
            *difficulties
                .entry(instance.difficulty.as_str())
                .or_insert(0) += 1;
        }
        assert_eq!(instance_ids.len(), 10);
        assert_eq!(repos.len(), 10);
        assert_eq!(difficulties.get("<15 min fix"), Some(&4));
        assert_eq!(difficulties.get("15 min - 1 hour"), Some(&5));
        assert_eq!(difficulties.get("1-4 hours"), Some(&1));
        assert!(instance_ids.contains("django__django-14752"));
        assert!(instance_ids.contains("sympy__sympy-19495"));
        assert!(instance_ids.contains("sphinx-doc__sphinx-7462"));
        assert!(instance_ids.contains("pylint-dev__pylint-8898"));
    }

    #[test]
    fn swebench_plan_wires_sinomo_and_mini_swe_agent() {
        let descriptor = swebench_verified_10_descriptor();
        assert_eq!(descriptor.family, BenchmarkFamily::Coding);
        assert_eq!(descriptor.task_count, 10);
        assert_eq!(descriptor.baseline.as_deref(), Some("mini-extra swebench"));
        let plan = swebench_verified_10_plan();
        assert!(plan.run_command.contains("run_swebench_verified.py"));
        assert!(plan.run_command.contains("--mode both"));
        assert!(plan.run_command.contains("deepseek-v4-flash"));
        assert!(plan.run_command.contains("--progress-out"));
        assert!(plan.required_tools.contains(&AgentToolKind::ReadFile));
        assert!(plan.required_tools.contains(&AgentToolKind::ApplyPatch));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
        assert!(include_str!("../swebench/run_swebench_verified.py").contains("mini-extra"));
        assert!(
            include_str!("../swebench/run_swebench_verified.py")
                .contains("swebench.harness.run_evaluation")
        );
        assert!(
            include_str!("../swebench/run_swebench_verified.py")
                .contains("official_evaluation_prerequisite_issues")
        );
        assert!(
            include_str!("../swebench/run_swebench_verified.py").contains("evaluation-blocked")
        );
    }

    #[test]
    fn agent_channel_discussion_plan_lists_native_and_clembench_paths() {
        let descriptor = agent_channel_discussion_descriptor();
        assert_eq!(descriptor.name, "agent-channel-discussion");
        assert_eq!(descriptor.family, BenchmarkFamily::AgentDiscussion);
        assert_eq!(descriptor.task_count, 8);
        assert!(
            descriptor
                .runner
                .contains("agent/run_channel_discussion_eval.py")
        );
        let plan = agent_channel_discussion_plan();
        assert_eq!(plan.name, "agent-channel-discussion");
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("--mode fixture --suite all"))
        );
        assert!(
            plan.run_command
                .contains("agent/run_channel_discussion_eval.py --mode run")
        );
        assert!(plan.run_command.contains("gpt-5.4-mini"));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("clembench-lite"))
        );
        let runner = include_str!("../agent/run_channel_discussion_eval.py");
        assert!(runner.contains("native-consensus-arithmetic"));
        assert!(runner.contains("native-hidden-info-merge"));
        assert!(runner.contains("native-silence-routing"));
        assert!(runner.contains("native-loop-bound"));
        assert!(runner.contains("native-final-handoff"));
        assert!(runner.contains("clem-taboo-mini"));
        assert!(runner.contains("clem-privateshared-mini"));
        assert!(runner.contains("clem-reference-mini"));
        assert!(runner.contains("sinomo_api.py"));
        assert!(runner.contains("clemcore"));
    }

    #[test]
    fn swebench_runner_rebuilds_default_agent_and_records_binary_provenance() {
        let runner = include_str!("../swebench/run_swebench_verified.py");
        assert!(runner.contains("DEFAULT_AGENT_BIN"));
        assert!(runner.contains("sinomo_agent_bin_explicit"));
        assert!(runner.contains("rebuilt-default-before-run"));
        assert!(runner.contains("explicit-binary-no-build"));
        assert!(runner.contains("agent_binary"));
        assert!(runner.contains("sha256"));
        assert!(runner.contains("\"cargo\", \"build\""));
    }

    #[test]
    fn swebench_runner_keeps_main_pass_uncapped_by_default() {
        let runner = include_str!("../swebench/run_swebench_verified.py");
        assert!(runner.contains("default=None"));
        assert!(runner.contains("unlimited-default"));
        assert!(runner.contains("if args.max_steps is not None:"));
        assert!(runner.contains("command.extend([\"--max-steps\", str(args.max_steps)])"));
    }

    #[test]
    fn swebench_runner_marks_agent_failures_as_harness_blocked() {
        let runner = include_str!("../swebench/run_swebench_verified.py");
        assert!(runner.contains("write_agent_run_blocked_report"));
        assert!(runner.contains("\"status\": \"harness-blocked\""));
        assert!(runner.contains("\"failure_class\": \"harness-or-environment\""));
        assert!(runner.contains("sinomo-instance-blocked"));
        assert!(runner.contains("sinomo_prediction_prerequisite_issues"));
        assert!(runner.contains("harness_blocked_instances"));
        assert!(runner.contains("missing_report_instances"));
    }

    #[test]
    fn swebench_runner_records_host_dependency_probe() {
        let runner = include_str!("../swebench/run_swebench_verified.py");
        assert!(runner.contains("def host_dependency_probe"));
        assert!(runner.contains("def should_stop_for_host_dependency_blockers"));
        assert!(runner.contains("\"host_dependencies\""));
        assert!(runner.contains("\"host-dependency-probe\""));
        assert!(runner.contains("\"host-dependency-blocked\""));
        assert!(runner.contains("\"harness_status\""));
        assert!(runner.contains("\"blocking_issues\""));
        assert!(runner.contains("\"git\""));
        assert!(runner.contains("\"cargo\""));
        assert!(runner.contains("\"mini-extra\""));
        assert!(runner.contains("\"swebench\""));
        assert!(runner.contains("docker_reachability_probe"));
        assert!(runner.contains("write_evaluation_blocked_marker"));
        assert!(runner.contains("explicit Sinomo agent binary does not exist"));
    }

    #[test]
    fn swebench_runner_aggregates_agent_model_metrics() {
        let runner = include_str!("../swebench/run_swebench_verified.py");
        assert!(runner.contains("agent_model_metrics"));
        assert!(runner.contains("first_successful_patch_turn"));
        assert!(runner.contains("instances_with_successful_patch"));
        assert!(runner.contains("coding_validation_status_counts"));
        assert!(runner.contains("coding_stop_recommendation_counts"));
        assert!(runner.contains("coding_validation_failure_kind_counts"));
        assert!(runner.contains("agent_model_response_parse_failures"));
        assert!(runner.contains("agent_native_tool_call_turns"));
        assert!(runner.contains("agent_vendor_fallback_tool_call_turns"));
        assert!(runner.contains("agent_model_repair_attempts"));
        assert!(runner.contains("agent_event_count"));
        assert!(runner.contains("agent_tool_call_items"));
        assert!(runner.contains("agent_tool_result_items"));
        assert!(runner.contains("orphan_tool_calls"));
        assert!(runner.contains("orphan_tool_results"));
        assert!(runner.contains("agent_final_decision_sources"));
    }

    #[test]
    fn agent_compaction_quality_plan_is_live_eval_not_unit_gate() {
        let plan = agent_compaction_quality_plan();
        assert_eq!(plan.name, "agent-compaction-quality");
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("agent/run_compaction_eval.py --mode fixture"))
        );
        assert!(
            plan.run_command
                .contains("agent/run_compaction_eval.py --mode run")
        );
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
        assert!(plan.required_tools.contains(&AgentToolKind::ApplyPatch));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("live/model eval harness"))
        );
        let runner = include_str!("../agent/run_compaction_eval.py");
        assert!(runner.contains("long-session-resume"));
        assert!(runner.contains("constraint-retention"));
        assert!(runner.contains("coding-after-compaction"));
        assert!(runner.contains("cluster-ops-after-compaction"));
        assert!(runner.contains("SCENARIOS"));
        assert!(runner.contains("validate_scenario_definitions"));
        assert!(runner.contains("continuation_resumes_durable_cli_session"));
        assert!(runner.contains("continuation_does_not_reinject_summary"));
        assert!(runner.contains("phase_one_invokes_agent_op_compact"));
        assert!(runner.contains("phase_one_requires_persisted_projection_evidence"));
        assert!(runner.contains("--compact-after"));
        assert!(runner.contains("compaction_evidence_issues"));
        assert!(runner.contains("--session"));
        assert!(runner.contains("missing_marker_phases"));
    }

    #[test]
    fn agent_skill_quality_plan_is_live_eval_not_unit_gate() {
        let plan = agent_skill_quality_plan();
        assert_eq!(plan.name, "agent-skill-quality");
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("agent/run_skill_eval.py --mode plan"))
        );
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("agent/run_skill_eval.py --mode fixture"))
        );
        assert!(
            plan.run_command
                .contains("agent/run_skill_eval.py --mode run")
        );
        assert!(plan.run_command.contains("--model gpt-5.5"));
        assert!(plan.required_tools.contains(&AgentToolKind::SkillSearch));
        assert!(plan.required_tools.contains(&AgentToolKind::SkillDescribe));
        assert!(plan.required_tools.contains(&AgentToolKind::SkillUpdate));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("progressive skill disclosure"))
        );
        let runner = include_str!("../agent/run_skill_eval.py");
        assert!(runner.contains("hidden-skill-disclosure"));
        assert!(runner.contains("skill-update-followup-shortcut"));
        assert!(runner.contains("run_mode_verifies_skill_update_text"));
        assert!(runner.contains("followup_uses_fresh_data_dir_except_provider_config"));
    }

    #[test]
    fn agent_memory_quality_plan_is_live_eval_not_unit_gate() {
        let plan = agent_memory_quality_plan();
        assert_eq!(plan.name, "agent-memory-quality");
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("agent/run_memory_eval.py --mode plan"))
        );
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("agent/run_memory_eval.py --mode fixture"))
        );
        assert!(
            plan.run_command
                .contains("agent/run_memory_eval.py --mode run")
        );
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
        assert!(plan.notes.iter().any(|note| note.contains("LongMemEval")));
        let runner = include_str!("../agent/run_memory_eval.py");
        assert!(runner.contains("information-extraction"));
        assert!(runner.contains("multi-session-reasoning"));
        assert!(runner.contains("knowledge-update-supersession"));
        assert!(runner.contains("temporal-reasoning"));
        assert!(runner.contains("scope-boundary-abstention"));
        assert!(runner.contains("workflow-gotcha"));
        assert!(runner.contains("retrieval-quality"));
        assert!(runner.contains("downstream-model-usefulness"));
        assert!(runner.contains("governance-and-scope-correctness"));
        assert!(runner.contains("operational-cache-efficiency"));
        assert!(runner.contains("marker_aliases"));
        assert!(runner.contains("marker_aliases_score_equivalent_phrasing"));
        assert!(runner.contains("forbidden_marker_violations"));
        assert!(runner.contains("required_source_ref_hit_count"));
        assert!(runner.contains("cache_churn_records"));
        assert!(runner.contains("RAGAS context precision"));
    }

    #[test]
    fn longmemeval_s_suite_and_plan_use_isolated_external_data() {
        let descriptor = longmemeval_s_descriptor();
        assert_eq!(descriptor.name, "agent-memory-longmemeval-s");
        assert_eq!(descriptor.family, BenchmarkFamily::Memory);
        assert_eq!(descriptor.task_count, 500);
        assert_eq!(
            descriptor.dataset.as_deref(),
            Some("xiaowu0162/longmemeval-cleaned@98d7416c24c778c2fee6e6f3006e7a073259d48f")
        );
        assert_eq!(descriptor.split.as_deref(), Some("longmemeval_s_cleaned"));
        assert!(descriptor.runner.ends_with("agent/run_longmemeval_s.py"));

        let plan = longmemeval_s_plan();
        assert_eq!(plan.name, "agent-memory-longmemeval-s");
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("run_longmemeval_s.py --mode plan"))
        );
        assert!(plan.run_command.contains("run_longmemeval_s.py --mode run"));
        assert!(
            plan.run_command
                .contains("benchmark-results/datasets/longmemeval_s_cleaned.json")
        );
        assert!(plan.run_command.contains("--reader none"));
        assert!(plan.run_command.contains("--retrieval-out"));
        assert_eq!(plan.required_tools, vec![AgentToolKind::RunShellCommand]);
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("fresh disposable SINOMO_DATA_DIR"))
        );
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("normal operational data"))
        );
    }

    #[test]
    fn memory_tools_longmemeval_s_uses_the_same_suite_without_dfs() {
        let descriptor = memory_tools_longmemeval_s_descriptor();
        assert_eq!(descriptor.name, "agent-memory-tools-longmemeval-s");
        assert_eq!(descriptor.family, BenchmarkFamily::Memory);
        assert_eq!(descriptor.task_count, 500);
        assert_eq!(descriptor.dataset, longmemeval_s_descriptor().dataset);
        assert_eq!(descriptor.split, longmemeval_s_descriptor().split);
        assert!(descriptor.runner.ends_with("agent/run_longmemeval_s.py"));

        let plan = memory_tools_longmemeval_s_plan();
        assert_eq!(plan.name, "agent-memory-tools-longmemeval-s");
        assert!(plan.setup_commands.iter().any(|command| {
            command.contains("--features memory-tools-bench --bin sinomo-memory-tools-bench")
        }));
        assert!(plan.run_command.contains("--retriever memory-tools"));
        assert!(plan.run_command.contains("--reader none"));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::SessionHistorySearch)
        );
        assert!(plan.required_tools.contains(&AgentToolKind::MemorySearch));
        assert!(plan.required_tools.contains(&AgentToolKind::MemoryWrite));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("does not require DFS"))
        );
    }

    #[test]
    fn minestudio_plan_keeps_vision_path_explicit() {
        let plan = minestudio_plan();
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        assert!(minestudio_smoke_python().contains("\"mode\": \"vision\""));
    }

    #[test]
    fn mcu_plan_uses_game_environment_tools() {
        let plan = mcu_minestudio_plan();
        assert!(plan.run_command.contains("mcu_live_duplex_runner.py"));
        assert!(plan.run_command.contains("SINOMO_MINESTUDIO_ENV_URL"));
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        let tools = mcu_game_tool_specs();
        assert!(tools.iter().any(|tool| tool.name == "inspect_game_state"));
        assert!(tools.iter().any(|tool| tool.name == "send_game_action"));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("MCU_benchmark/task_configs"))
        );
        assert!(include_str!("../mcu/mcu_live_duplex_runner.py").contains("call_minicpm_stream"));
    }

    #[test]
    fn minestudio_vision_loop_is_vision_only() {
        let plan = minestudio_vision_action_loop_plan();
        assert!(
            plan.run_command
                .contains("minestudio_vision_action_loop.py")
        );
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("minestudio"))
        );
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("compose.yml build"))
        );
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("real MineStudio MinecraftSim"))
        );
        assert!(
            include_str!("../minestudio/minestudio_vision_action_loop.py").contains("MinecraftSim")
        );
        assert!(
            !include_str!("../minestudio/minestudio_vision_action_loop.py").contains("ImageDraw")
        );
    }

    #[test]
    fn minestudio_live_duplex_loop_uses_streaming_frames() {
        let plan = minestudio_live_duplex_loop_plan();
        assert!(plan.run_command.contains("SINOMO_MINESTUDIO_ENV_URL"));
        assert!(plan.run_command.contains("minestudio_live_duplex_loop.py"));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("RecordCallback"))
        );
        assert!(
            include_str!("../minestudio/minestudio_live_duplex_loop.py")
                .contains("\"image_sequence_base64\"")
        );
        assert!(
            include_str!("../minestudio/minestudio_live_duplex_loop.py")
                .contains("\"stream\": True")
        );
        assert!(
            include_str!("../minestudio/minestudio_live_duplex_loop.py")
                .contains("collect_minestudio_recordings")
        );
    }

    #[test]
    fn default_environment_prompt_mentions_async_and_memory_tools() {
        let config = AgentEnvironmentConfig::default();
        assert!(config.prompt.contains("async"));
        assert!(config.prompt.contains("session_history_search"));
        assert!(config.prompt.contains("memory_search"));
    }

    #[test]
    fn benchmark_module_descriptor_exports_catalog_operation() {
        let descriptor = benchmarks_module_descriptor();

        assert_eq!(descriptor.module_id, SINOMO_BENCHMARKS_MODULE_ID);
        assert_eq!(
            descriptor.interface_version,
            SINOMO_BENCHMARKS_INTERFACE_VERSION
        );
        assert_eq!(descriptor.backend, NativeModuleBackend::NativeDylib);
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_BENCHMARKS_OPERATION.to_string())
        );
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.catalog.read".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.cancel-probe".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.compaction.plan".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.memory.plan".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.memory.longmemeval-s.plan".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.memory.memory-tools-longmemeval-s.plan".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.channel-discussion.plan".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.agent.skill.plan".to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.sifs-fuse.plan".to_string())
        );
        assert!(descriptor.surfaces.object_rpc_routes.iter().any(|route| {
            route.route == SINOMO_BENCHMARKS_CANCEL_PROBE_ROUTE
                && route.category == SINOMO_BENCHMARKS_RPC_CATEGORY
                && route.operation == SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION
        }));
        assert!(
            descriptor
                .surfaces
                .capability_exports
                .contains(&SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string())
        );
    }

    #[test]
    fn benchmark_module_catalog_lists_research_harnesses() {
        let catalog = benchmark_module_catalog();

        assert_eq!(catalog.module_root, SINOMO_BENCHMARKS_MODULE_ROOT);
        assert_eq!(catalog.suites.len(), 5);
        assert_eq!(catalog.suites[0].name, "swebench-verified-10");
        assert_eq!(catalog.suites[1].name, "agent-channel-discussion");
        assert_eq!(catalog.suites[2].name, "agent-skill-quality");
        assert_eq!(catalog.suites[3].name, "agent-memory-longmemeval-s");
        assert_eq!(catalog.suites[4].name, "agent-memory-tools-longmemeval-s");
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "swebench-verified-10")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "agent-skill-quality")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "agent-compaction-quality")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "agent-memory-quality")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "agent-memory-longmemeval-s")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "agent-memory-tools-longmemeval-s")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "agent-channel-discussion")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "minestudio-live-duplex-loop")
        );
        assert!(catalog.plans.iter().any(|plan| plan.name == "sifs-fuse"));
        assert!(
            catalog
                .plans
                .iter()
                .all(|plan| !plan.run_command.contains("crates/sinomo-benchmarks"))
        );
    }

    #[test]
    fn sifs_fuse_plan_is_opt_in_and_reproducible() {
        let plan = sifs_fuse_benchmark_plan();

        assert_eq!(plan.name, "sifs-fuse");
        assert_eq!(plan.setup_commands.len(), 2);
        assert!(plan.setup_commands[0].contains("cargo test"));
        assert!(plan.setup_commands[1].contains("--release"));
        assert!(plan.setup_commands[1].contains("--features sifs-fuse-bench"));
        assert!(plan.run_command.contains("$SINOMO_SIFS_FUSE_BENCH_ROOT"));
        assert!(plan.run_command.contains("--suite full"));
        assert!(plan.run_command.contains("--require-fuse"));
        assert!(plan.run_command.contains("--min-sequential-read-mib-s"));
        assert!(
            plan.run_command
                .contains("SINOMO_SIFS_FUSE_MIN_SEQUENTIAL_READ_MIB_S")
        );
        assert!(plan.run_command.contains("--min-parallel-create-speedup"));
        assert!(
            plan.run_command
                .contains("SINOMO_SIFS_FUSE_MIN_PARALLEL_CREATE_SPEEDUP")
        );
        assert!(
            plan.run_command
                .contains("benchmark-results/sifs-fuse/report.json")
        );
        assert_eq!(plan.required_tools, vec![AgentToolKind::RunShellCommand]);
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("performance thresholds are opt-in"))
        );
    }

    #[test]
    fn benchmark_module_native_abi_returns_catalog() {
        let _guard = MODULE_ABI_TEST_LOCK.lock().expect("ABI test lock");
        assert_eq!(
            sinomo_module_init(SinomoModuleHostApiV2::empty()),
            SINOMO_MODULE_STATUS_OK
        );
        let payload_sobj = encode_sobj(&BenchmarkModuleRequest::Catalog).expect("payload");
        let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: SINOMO_BENCHMARKS_OPERATION.to_string(),
            payload_sobj,
            host_data_dir_root: None,
            cancellation_wait_handle: None,
            request_body: None,
            response_body_path: None,
        });
        let request_sobj = encode_sobj(&request).expect("request");
        let mut response_buffer = SinomoModuleByteBuffer::null();

        let status = sinomo_module_invoke(
            SinomoModuleByteSlice::from_slice(&request_sobj),
            &mut response_buffer,
        );

        assert_eq!(status, SINOMO_MODULE_STATUS_OK);
        let response_bytes = unsafe { SinomoModuleByteBuffer::into_vec(response_buffer) };
        let native_response: NativeModuleResponse =
            decode_sobj(&response_bytes).expect("native response");
        let NativeModuleResponse::Invocation(response) = native_response else {
            panic!("expected invocation response");
        };
        assert_eq!(response.operation, SINOMO_BENCHMARKS_OPERATION);
        let benchmark_response: BenchmarkModuleResponse =
            decode_sobj(&response.payload_sobj).expect("benchmark response");
        let BenchmarkModuleResponse::Catalog(catalog) = benchmark_response else {
            panic!("expected catalog response");
        };
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "swebench-verified-10")
        );
        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
        let mut rejected_buffer = SinomoModuleByteBuffer::null();
        assert_eq!(
            sinomo_module_invoke(
                SinomoModuleByteSlice::from_slice(&request_sobj),
                &mut rejected_buffer,
            ),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR
        );
        let rejected_bytes = unsafe { SinomoModuleByteBuffer::into_vec(rejected_buffer) };
        assert!(matches!(
            decode_sobj::<NativeModuleResponse>(&rejected_bytes).expect("rejected response"),
            NativeModuleResponse::Error(NativeModuleErrorResponse { ref code, .. })
                if code == "module-unavailable"
        ));
        assert_eq!(
            sinomo_module_init(SinomoModuleHostApiV2::empty()),
            SINOMO_MODULE_STATUS_OK
        );
        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
    }

    unsafe extern "C" fn test_host_is_cancelled(user_data: *mut c_void) -> u8 {
        let cancelled = unsafe { (user_data as *const AtomicBool).as_ref() };
        cancelled.is_some_and(|cancelled| cancelled.load(Ordering::SeqCst)) as u8
    }

    #[test]
    fn benchmark_module_native_abi_cancel_probe_observes_host_cancellation() {
        let _guard = MODULE_ABI_TEST_LOCK.lock().expect("ABI test lock");
        let cancelled = AtomicBool::new(false);
        let status = sinomo_module_init(SinomoModuleHostApiV2 {
            abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            user_data: &cancelled as *const AtomicBool as *mut c_void,
            output_chunk: None,
            log: None,
            is_cancelled: Some(test_host_is_cancelled),
            local_permission_granted: None,
        });
        assert_eq!(status, SINOMO_MODULE_STATUS_OK);

        let payload_sobj = encode_sobj(&BenchmarkCancelProbeRequest {
            max_runtime_ms: 2_000,
            poll_interval_ms: 5,
        })
        .expect("payload");
        let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION.to_string(),
            payload_sobj,
            host_data_dir_root: None,
            cancellation_wait_handle: None,
            request_body: None,
            response_body_path: None,
        });
        let request_sobj = encode_sobj(&request).expect("request");

        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let mut response_buffer = SinomoModuleByteBuffer::null();
                let status = sinomo_module_invoke(
                    SinomoModuleByteSlice::from_slice(&request_sobj),
                    &mut response_buffer,
                );
                let response_bytes = unsafe { SinomoModuleByteBuffer::into_vec(response_buffer) };
                (status, response_bytes)
            });
            std::thread::sleep(Duration::from_millis(50));
            assert_eq!(
                sinomo_module_shutdown(),
                SINOMO_MODULE_STATUS_RUNTIME_ERROR,
                "shutdown must not invalidate callbacks held by an active invocation"
            );
            cancelled.store(true, Ordering::SeqCst);
            let (status, response_bytes) = worker.join().expect("cancel probe worker");
            assert_eq!(status, SINOMO_MODULE_STATUS_OK);
            let native_response: NativeModuleResponse =
                decode_sobj(&response_bytes).expect("native response");
            let NativeModuleResponse::Invocation(response) = native_response else {
                panic!("expected invocation response");
            };
            assert_eq!(response.operation, SINOMO_BENCHMARKS_CANCEL_PROBE_OPERATION);
            let cancel_response: BenchmarkCancelProbeResponse =
                decode_sobj(&response.payload_sobj).expect("cancel response");
            assert!(cancel_response.cancelled);
            assert!(cancel_response.polls > 1);
        });

        assert_eq!(sinomo_module_shutdown(), SINOMO_MODULE_STATUS_OK);
    }
}
