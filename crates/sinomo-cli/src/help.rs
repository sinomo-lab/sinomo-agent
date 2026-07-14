use super::*;

pub(crate) const BUILTIN_TOP_LEVEL_USAGE_COMMANDS: &[&str] = &[
    "help",
    "cluster",
    "install",
    "uninstall",
    "daemon",
    "debug",
    "rollout",
    "dfs",
    "models",
    "model",
    "usage",
    "providers",
    "voice",
    "agent",
    "skill",
    "test",
];

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo");
    for command in BUILTIN_TOP_LEVEL_USAGE_COMMANDS {
        println!("  sinomo {command}");
    }
    for command in cli_command_registry().metadata() {
        if !BUILTIN_TOP_LEVEL_USAGE_COMMANDS.contains(&command.name) {
            println!("  sinomo {}", command.name);
        }
    }
    println!();
    println!("Run `sinomo <subcommand> help` for detailed commands.");
    println!();
    println!("TUI:");
    println!(
        "  Run `sinomo` with no arguments to connect the command-line user interface to the local daemon when available."
    );
    println!("  Inside the TUI, prefix any CLI command with `/`, for example `/models list`.");
    println!("  Use `/quit` or `/exit` to end the session.");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
    println!("  SINOMO_CODEX_CREDENTIALS_FILE  Overrides Sinomo's Codex credentials file");
    println!("  SINOMO_CLAUDE_CREDENTIALS_FILE Overrides Sinomo's Claude credentials file");
    println!("  ANTHROPIC_API_KEY           Used by `providers auth/test anthropic`");
    println!("  OPENAI_API_KEY              Used by `providers auth/test openai`");
    println!("  DEEPSEEK_API_KEY            Used by `providers auth/test deepseek`");
    println!("  GEMINI_API_KEY              Used by `providers auth/test gemini`");
    println!("  NVIDIA_API_KEY              Used by `providers auth/test nvidia`");
    println!("  Cluster runtime settings live in KV; see `sinomo models runtime`.");
}

pub(super) fn print_agent_help() {
    println!("Agent:");
    println!("  sinomo agent");
    println!("      Show agent commands.");
    println!("  sinomo agent export [agent-id] [--output <path>]");
    println!("      Export an agent package JSON artifact.");
    println!("  sinomo agent import <path> [--json]");
    println!("      Validate and store an imported agent package under the local data directory.");
    println!("  sinomo agent inspect [agent-id] [--json]");
    println!(
        "      Inspect an agent package: prompt, tools, skills, context, compaction, and model preferences."
    );
    println!("  sinomo agent readiness [--json]");
    println!(
        "      Run deterministic local readiness gates for packages, tools, protocol parsing, compaction, resume, and command evidence."
    );
    println!(
        "  sinomo agent run [--prompt <text>] [--agent <agent-id>] [--provider <provider>] [--model <model>] [--account <account>] [--session <session-id>] [--compact-after] [--json]"
    );
    println!(
        "      Run one agent turn, optionally resume its durable session, and print the final response."
    );
    println!(
        "      With --compact-after, run real compaction after the turn and persist its context projection."
    );
    println!(
        "      The built-in coding-agent may modify only the explicit workspace directory where this CLI command is run; unsafe roots are rejected."
    );
    println!("  sinomo agent validate [agent-id|--file <path>] [--json]");
    println!("      Validate an agent package from the catalog or a JSON artifact.");
    println!("  sinomo agent session");
    println!("      Show agent session inspect/control commands.");
    println!("  sinomo agent session inspect <session-id> [--json]");
    println!(
        "      Inspect persisted session state and control-plane snapshot through daemon RPC."
    );
    println!(
        "  sinomo agent session progress <session-id> [--cursor <offset>] [--last-event <event-id>] [--json]"
    );
    println!("      Read persisted control-plane events after a cursor through daemon RPC.");
    println!("  sinomo agent session terminal-results <session-id> [--task <task-id>] [--json]");
    println!("      Read terminal task results through daemon RPC.");
    println!("  sinomo agent session steer <session-id> <message> [--json]");
    println!("      Queue steering for a live session exposed by an injected runtime host.");
    println!("  sinomo agent session follow-up <session-id> <message> [--json]");
    println!(
        "      Queue a user follow-up for a live session exposed by an injected runtime host."
    );
    println!("  sinomo agent session interrupt <session-id> <reason> [--json]");
    println!("      Interrupt the active live turn exposed by an injected runtime host.");
    println!("  sinomo agent session resume <session-id> [--json]");
    println!("      Reattach to a live session exposed by an injected runtime host.");
}

pub(super) fn print_agent_session_help() {
    println!("Agent sessions:");
    println!("  sinomo agent session inspect <session-id> [--json]");
    println!(
        "  sinomo agent session progress <session-id> [--cursor <offset>] [--last-event <event-id>] [--json]"
    );
    println!("  sinomo agent session terminal-results <session-id> [--task <task-id>] [--json]");
    println!("  sinomo agent session steer <session-id> <message> [--json]");
    println!("  sinomo agent session follow-up <session-id> <message> [--json]");
    println!("  sinomo agent session interrupt <session-id> <reason> [--json]");
    println!("  sinomo agent session resume <session-id> [--json]");
}

pub(super) fn print_skill_help() {
    println!("Skills:");
    println!("  sinomo skill");
    println!("      Show skill commands.");
    println!("  sinomo skill list [--all] [--json]");
    println!("      List active catalog skills, or every known version with --all.");
    println!("  sinomo skill show <skill-id> [--version <version>] [--text|--json]");
    println!(
        "      Show a catalog skill descriptor and, when requested, the stored SKILL.md text."
    );
    println!(
        "  sinomo skill import <path-or-dir> [--id <skill-id>] [--version <version>] [--source user|agent|evolution] [--source-id <id>] [--no-activate] [--json]"
    );
    println!("      Import a community-style SKILL.md file into the SIFS-backed catalog.");
    println!("  sinomo skill export <skill-id> [--version <version>] [--output <path>] [--json]");
    println!("      Export a catalog skill as a portable SKILL.md file.");
    println!("  sinomo skill activate <skill-id> [--version <version>] [--scope <scope>] [--json]");
    println!("      Make an existing catalog skill version active for a scope.");
}

pub(super) fn print_voice_help() {
    println!("Voice:");
    println!("  sinomo voice");
    println!("      Show voice commands.");
    println!("  sinomo voice devices");
    println!("      List native microphones, cameras, and speakers visible on this node.");
    println!(
        "  sinomo voice call [--provider openai|minicpm-o] [--model <model>] [--input <raw-pcm>|-] [--video-frame <jpg|png>] [--output <raw-pcm>|-]"
    );
    println!(
        "      Stream mono raw PCM plus optional image frames to a realtime model, persist session media chunks, and write raw response audio."
    );
    println!("      OpenAI defaults: gpt-realtime-2, 24 kHz pcm16-le input/output.");
    println!(
        "      MiniCPM-o defaults: MiniCPM-o-4.5, 16 kHz f32-le input and 24 kHz f32-le output."
    );
    println!(
        "      Useful options: --native, --native-mic, --native-camera, --native-speaker, --mic-device <selector>, --camera-device <path-or-name>, --speaker-device <selector>, --call-ms <n>, --instructions <text>, --voice <voice-or-base64-ref-audio>, --endpoint <url>, --session <session_id>, --chunk-ms <n> (default 100), --video-detail auto|low|high, --video-frame-ms <n>, --turn manual|semantic-vad, --no-audio, --no-pacing."
    );
}

pub(super) fn print_cluster_help() {
    println!("Cluster:");
    println!("  sinomo cluster");
    println!("      Show cluster commands.");
    println!(
        "  sinomo cluster init [--name <name>] [--cluster-id cluster_...] [--node-id node_...]"
    );
    println!("      Bootstrap a new single-node cluster through the running daemon.");
    println!(
        "      First boot only: with no daemon and no cluster state, initializes directly (--cluster-id/--node-id apply only there)."
    );
    println!(
        "  sinomo cluster invite [--introducer <host:port>] [--ttl-seconds <seconds>] [--output <path>]"
    );
    println!(
        "      Create a portable join invite through the running daemon (defaults to daemon-advertised introducers)."
    );
    println!("  sinomo cluster join <invite> [--name <name>]");
    println!(
        "      Join an existing cluster through the running daemon from a signed sinomo-join URI or invite file."
    );
    println!(
        "      A node already settled in a cluster migrates via the direct path instead; --cluster-id/--node-id overrides apply only there."
    );
    println!("  sinomo cluster migrate plan <invite> [--mode single-node|cluster-merge]");
    println!(
        "      Plan migration from the current initialized cluster into another cluster without mutating local state."
    );
    println!("  sinomo cluster rename <node-id> <name>");
    println!(
        "      Change a node display name in quorum-backed cluster membership (through the running daemon)."
    );
    println!("  sinomo cluster rename-hostnames");
    println!("      Rename inferable cluster members to their advertised hostnames.");
    println!("  sinomo cluster role <node-id> <workstation|mobile|server|sensor|test>");
    println!(
        "      Change the documented node role in quorum-backed cluster membership (through the running daemon)."
    );
    println!("  sinomo cluster state-authority <node-id> <authority|replica|observer>");
    println!("      Change state authority membership through quorum-backed cluster membership.");
    println!("  sinomo cluster repair-members --from <host:port> [--promote <node-id>]");
    println!(
        "      Import same-cluster members from a reachable peer snapshot, then optionally promote one."
    );
    println!("  sinomo cluster remove <node-id> [node-id ...]");
    println!(
        "      Remove members without state authority from quorum-backed cluster membership (through the running daemon)."
    );
    println!("  sinomo cluster cleanup-legacy-shadow-members");
    println!(
        "      Remove stale shadow-only daemon member records from quorum-backed cluster membership."
    );
    println!("  sinomo cluster status");
    println!("      Show local cluster status and node health counts.");
    println!("  sinomo cluster health");
    println!(
        "      Report node online/offline state, daemon version, platform, and rollout status."
    );
    println!("  sinomo cluster doctor");
    println!(
        "      Diagnose endpoint advertisement, reachability records, stale status, and duplicated roaming endpoints."
    );
    println!("  sinomo cluster peers");
    println!(
        "      List known peers, preferring the daemon's health view; falls back to local cluster state."
    );
    println!("  sinomo cluster leave");
    println!(
        "      Leave the current cluster and start a fresh single-node cluster (through the running daemon)."
    );
}

pub(super) fn print_install_help() {
    println!("Install:");
    println!(
        "  sinomo install [--host <host>] [--port <port>] [--shadow-port <port>] [--no-start]"
    );
    println!(
        "      Create or update the local install, add boot entries, expose the client in the user bin directory, then start daemons."
    );
    println!("  sinomo uninstall [--no-stop]");
    println!("      Remove Sinomo boot entries and stop daemons. The data directory is retained.");
}

pub(super) fn print_daemon_help() {
    println!("Daemon:");
    println!("  sinomo daemon");
    println!("      Show daemon commands.");
    println!("  sinomo daemon run [--host <host>] [--port <port>]");
    println!("      Run the daemon in the foreground. Defaults to 127.0.0.1:21353.");
    println!("  sinomo daemon start [--host <host>] [--port <port>]");
    println!("      Start the daemon as a detached background process.");
    println!("  sinomo daemon status");
    println!("      Probe the data-directory lock and daemon listener.");
    println!("  sinomo daemon stop");
    println!("      Ask the running daemon to shut down.");
    println!("  sinomo daemon shadow run|start|status|stop [--host <host>] [--port <port>]");
    println!("      Control the shadow daemon. Defaults to 127.0.0.1:21354.");
}

pub(super) fn print_debug_help() {
    println!("Debug:");
    println!("  sinomo debug");
    println!("      Show debug commands.");
    println!("  sinomo debug sobj <path>");
    println!("      Decode a SOBJ file and print the typed text representation.");
}

pub(super) fn print_rollout_help() {
    println!("Rollout:");
    println!("  sinomo rollout");
    println!("      Show rollout commands.");
    println!("  sinomo rollout init-source [--repo <path>] [--force-source]");
    println!("      Initialize or merge an external git checkout into the managed source repo.");
    println!("  sinomo rollout import-source [--repo <path>] [--force-source]");
    println!(
        "      Merge external repo changes into the managed source repo without retaining external remotes."
    );
    println!("  sinomo rollout export-source --repo <path>");
    println!(
        "      Merge managed source changes, including self-evolution commits, into an external repo."
    );
    println!(
        "  sinomo rollout apply-external [--repo <path>] [--force-source] [--host <host>] [--port <port>]"
    );
    println!(
        "      Import external repo changes into managed source, then build and apply the managed source."
    );
    println!(
        "  sinomo rollout apply [--repo <path>] [--force-source] [--host <host>] [--port <port>]"
    );
    println!(
        "      Build the data-directory source repo, stage main and shadow binaries, then verify main health."
    );
    println!(
        "  sinomo rollout cluster [--repo <path> | --binary <path>] [--target <host:port>] [--remote-build] [--force-source] [--remote-repo <path>] [--skip-local] [--skip-native-modules]"
    );
    println!(
        "      Resolve one cached or cluster-built binary/module set per target OS and architecture, distribute it to compatible nodes, then activate through shadow supervision or the main-only bootstrap path."
    );
    println!(
        "      Build servers are filtered by target support and ranked by exact cache hit, queue pressure, available slots, CPU, memory, and transfer locality."
    );
    println!(
        "      --remote-build keeps the legacy per-target source build path for mixed-version recovery; it bypasses cluster build scheduling and may be slow on constrained targets."
    );
    println!(
        "      --force-source stashes local changes in the managed source repo before overwriting it with the rollout source."
    );
    println!(
        "      --skip-native-modules pushes only the locally built binary for supervisor ABI bootstrapping; it cannot be combined with --remote-build or --remote-repo."
    );
    println!(
        "  sinomo rollout module native-dylib <module-id|package|source|cache-key> [--repo <path>] [--target <host:port>] [--skip-local]"
    );
    println!(
        "      Resolve one cluster-built native-dylib artifact per target OS and architecture, install it on compatible targets, and refresh the daemon through shadow supervision."
    );
    println!(
        "  sinomo rollout advance [--node <node-id>] [--scorecard <path> | --module-replay-report <path> | --shadow-telemetry [--shadow-telemetry-limit <n>]] [--incident <event-id>] [--user-corrections <n>] [--rollback-trigger <trigger>] [--evidence <ref>]"
    );
    println!(
        "      Read the persisted candidate rollout state, record the next promotion or rollback decision from scorecard, replay, live shadow telemetry, or incident evidence, and update rollout runtime state."
    );
    println!(
        "  sinomo rollout telemetry-gates show|set [--enable-auto-promotion|--disable-auto-promotion] [--event-limit <n>] [--min-events <n>] [--max-auto-weight-bps <n|unlimited>] [--max-candidate-latency-p95-ms <n|unlimited>]"
    );
    println!("      Inspect or update the daemon's automatic shadow-telemetry promotion gates.");
    println!("  sinomo rollout diagnostics [--target <host:port>]");
    println!(
        "      Fetch rollout state, build logs, and candidate stdout/stderr tails from a daemon."
    );
    println!("  sinomo rollout clear-status [--target <host:port>]");
    println!(
        "      Clear stale rollout status from the local data dir or a daemon target without deleting pending rollout artifacts."
    );
    println!("  sinomo rollout status");
    println!("      Show source, binary, and rollout state paths.");
}

pub(super) fn print_dfs_help() {
    println!("DFS:");
    println!("  sinomo dfs");
    println!("      Show DFS commands.");
    println!("  sinomo dfs status [--cluster] [--metadata-root <path>] [--json]");
    println!("      Summarize local shard metadata, writes, repairs, and conflicts.");
    println!("  sinomo dfs filesystem <file_system_id> [--metadata-root <path>] [--json]");
    println!("      Inspect one filesystem.");
    println!(
        "  sinomo dfs path <path> [--filesystem <file_system_id>] [--metadata-root <path>] [--json]"
    );
    println!("      Inspect one object by filesystem path, including replica locations.");
    println!("  sinomo dfs object <object_id> [--metadata-root <path>] [--json]");
    println!("      Inspect one object with versions, receipts, intents, conflicts, and repairs.");
    println!(
        "  sinomo dfs conflicts [--filesystem <file_system_id>] [--object <object_id>] [--metadata-root <path>] [--json]"
    );
    println!("      List conflict records.");
    println!(
        "  sinomo dfs conflicts resolve (--path <path>|--object <object_id>) [--filesystem <file_system_id>] [--id <conflict_id>] [--delete-file] [--metadata-root <path>] [--json]"
    );
    println!("      Delete matching conflict records; optionally unlink the DFS file.");
    println!(
        "  sinomo dfs conflicts versions (--path <path>|--object <object_id>) [--filesystem <file_system_id>] [--id <conflict_id>] [--metadata-root <path>] [--json]"
    );
    println!("      Show the stored candidate versions for a conflicted DFS object.");
    println!(
        "  sinomo dfs conflicts use-version (--path <path>|--object <object_id>) --version <version> [--filesystem <file_system_id>] [--id <conflict_id>] [--metadata-root <path>] [--json]"
    );
    println!(
        "      Make one stored conflict version visible and remove matching conflict records."
    );
    println!(
        "  sinomo dfs conflicts save-version (--path <path>|--object <object_id>) --version <version> --to <path> [--filesystem <file_system_id>] [--id <conflict_id>] [--json]"
    );
    println!("      Copy one stored conflict version to a new DFS file for manual merge.");
    println!(
        "  sinomo dfs repair [--filesystem <file_system_id>] [--object <object_id>] [--metadata-root <path>] [--json]"
    );
    println!("      List inferred missing replica repairs.");
    println!("  sinomo dfs services [--json]");
    println!("      List DFS metadata and content service advertisements from cluster KV.");
    println!("  sinomo dfs smoke [--json]");
    println!(
        "      Write a small SIFS file with two content replicas and report the committed locations."
    );
    println!("  sinomo dfs metadata publish [--json]");
    println!("      Publish the local primary metadata shard journal to configured replicas.");
    println!(
        "  sinomo dfs metadata shard transfer-primary <shard> --to <node_id> [--filesystem <file_system_id>] [--expected-epoch <n>] [--json]"
    );
    println!(
        "      Transfer a metadata shard primary to an existing replica through fenced cluster KV."
    );
    println!(
        "  sinomo dfs mount <mount-point> [--root <sifs-path>] [--read-only] [--allow-other] [--data-dir <path>]"
    );
    println!(
        "      Expose SIFS locally through FUSE on Unix or Windows Cloud Files on Windows; the command stays attached while mounted."
    );
    println!("  sinomo dfs pin list [--filesystem <file_system_id>] [--pin-root <path>] [--json]");
    println!(
        "  sinomo dfs pin add --path <path> [--filesystem <file_system_id>] [--object <object_id>] [--node <node_id>] [--mode metadata|content] [--pin-root <path>] [--json]"
    );
    println!(
        "  sinomo dfs pin remove --path <path> [--filesystem <file_system_id>] [--node <node_id>] [--pin-root <path>] [--json]"
    );
    println!("  sinomo dfs expose init [--config <path>] [--json]");
    println!("      Ensure the default local SIFS exposure exists.");
    println!("  sinomo dfs expose list [--config <path>] [--json]");
    println!(
        "  sinomo dfs expose add --source <path> --dfs-path <path> [--id <id>] [--filesystem <file_system_id>] [--node <node_id>] [--read-only] [--follow-symlinks] [--config <path>] [--json]"
    );
    println!("  sinomo dfs expose remove --id <id> [--config <path>] [--json]");
}

pub(super) fn print_models_help() {
    println!("Models:");
    println!("  sinomo models");
    println!("      Show the current default model and this help.");
    println!("  sinomo model");
    println!("      Alias for `sinomo models`.");
    println!("  sinomo models list [--provider <provider>]");
    println!("      List available or known models.");
    println!("  sinomo models default --provider <provider> --model <model>");
    println!("      Set the default model for TUI and model-backed commands.");
    println!(
        "  sinomo models options [show] [--provider <provider>] [--account <account>] [--model <model>]"
    );
    println!("      Show provider-specific account or model options.");
    println!(
        "  sinomo models options set --provider <provider> [--account <account>] [--model <model>] [--reasoning-effort none|minimal|low|medium|high|xhigh|max|clear] [--reasoning-budget-tokens <n|clear>] [--text-verbosity low|medium|high|clear] [--vendor-option <key=value>] [--clear-vendor-option <key>]"
    );
    println!("      Update provider-specific account or model options.");
    println!(
        "  sinomo models options clear --provider <provider> [--account <account>] [--model <model>]"
    );
    println!("      Clear provider-specific options for an account or one model override.");
    println!("  sinomo models runtime [show]");
    println!("      Show cluster-scoped native model runtime settings.");
    println!(
        "  sinomo models runtime set [--native-models-cache-dir <path>] [--minicpm-v46-model-dir <path>] [--minicpm-v46-max-input-tokens <n>] [--minicpm-v46-max-output-tokens <n>] [--pytorch-endpoint <url>] [--pytorch-python <path>] [--pytorch-device auto|cpu|cuda] [--pytorch-timeout-seconds <n>] [--pytorch-mock true|false]"
    );
    println!("      Update cluster-scoped native model runtime settings.");
    println!();
    println!("Providers:");
    for provider in default_model_provider_ids() {
        println!("  {provider}");
    }
}

pub(super) fn print_usage_help() {
    println!("Usage:");
    println!("  sinomo usage [--provider <provider>] [--json]");
    println!("      Show subscription usage for authenticated providers.");
    println!();
    println!("Providers:");
    for provider in subscription_usage_provider_ids() {
        println!("  {provider}");
    }
}

pub(super) fn print_providers_help() {
    println!("Providers:");
    println!("  sinomo providers");
    println!("      Show provider commands.");
    println!("  sinomo providers auth <provider>");
    println!("      Authenticate a provider and store credentials in Sinomo's data dir.");
    println!("  sinomo providers test <provider>");
    println!("      Validate stored provider credentials.");
    println!();
    println!("Provider IDs:");
    println!("  github-copilot");
    println!("  codex");
    println!("  claude");
    println!("  anthropic");
    println!("  openai");
    println!("  deepseek");
    println!("  gemini");
    println!("  nvidia");
}

pub(super) fn print_test_help() {
    println!("Tests:");
    println!("  sinomo test");
    println!("      Show test commands.");
    println!("  sinomo test list [--tag <tag>] [--json]");
    println!("      List registered runtime tests.");
    println!(
        "  sinomo test run <test-id>|--tag <tag> [--provider <provider>] [--account <account>] [--model <model>] [--live] [--require-cluster] [--json] [--param <key=value> ...]"
    );
    println!("      Run one registered runtime test or all tests with a matching tag.");
    println!(
        "  sinomo test smoke [--provider codex|deepseek|openai] [--account <account>] [--model <model>] [--live] [--require-cluster] [--json]"
    );
    println!("      Run the provider native-tool live smoke test.");
}
