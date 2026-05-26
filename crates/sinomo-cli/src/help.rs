use super::*;

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo");
    println!("  sinomo help");
    println!("  sinomo cluster");
    println!("  sinomo install");
    println!("  sinomo uninstall");
    println!("  sinomo daemon");
    println!("  sinomo rollout");
    println!("  sinomo dfs");
    println!("  sinomo sandbox");
    println!("  sinomo models");
    println!("  sinomo usage");
    println!("  sinomo providers");
    println!("  sinomo evolve");
    println!();
    println!("Run `sinomo <subcommand> help` for detailed commands.");
    println!();
    println!("CUI:");
    println!(
        "  Run `sinomo` with no arguments to connect the command-line user interface to the local daemon when available."
    );
    println!("  Inside the CUI, prefix any CLI command with `/`, for example `/models list`.");
    println!("  Use `/quit` or `/exit` to end the session.");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
    println!("  SINOMO_CODEX_CREDENTIALS_FILE  Overrides Sinomo's Codex credentials file");
    println!("  ANTHROPIC_API_KEY           Used by `providers auth/test anthropic`");
    println!("  OPENAI_API_KEY              Used by `providers auth/test openai`");
    println!("  GEMINI_API_KEY              Used by `providers auth/test gemini`");
    println!("  NVIDIA_API_KEY              Used by `providers auth/test nvidia`");
    println!("  Cluster runtime settings live in KV; see `sinomo models runtime`.");
}

pub(super) fn print_cluster_help() {
    println!("Cluster:");
    println!("  sinomo cluster");
    println!("      Show cluster commands.");
    println!(
        "  sinomo cluster init [--name <name>] [--cluster-id cluster_...] [--node-id node_...]"
    );
    println!("      Bootstrap a new single-node cluster and clear orphan-node state.");
    println!(
        "  sinomo cluster invite [--introducer <host:port>] [--ttl-seconds <seconds>] [--output <path>]"
    );
    println!(
        "      Create a portable join invite. Pass a LAN-reachable introducer address for remote nodes."
    );
    println!(
        "  sinomo cluster join <invite> [--name <name>] [--cluster-id cluster_...] [--node-id node_...]"
    );
    println!(
        "      Record intent to join an existing cluster from a signed sinomo-join URI or invite file."
    );
    println!("  sinomo cluster role <node-id> <workstation|mobile|server|sensor|test>");
    println!("      Change the documented node role in quorum-backed cluster membership.");
    println!("  sinomo cluster raft-role <node-id> <voter|learner|non-voting>");
    println!("      Change the Raft membership state through quorum-backed cluster membership.");
    println!("  sinomo cluster repair-members --from <host:port> [--promote <node-id>]");
    println!(
        "      Import same-cluster members from a reachable peer snapshot, then optionally promote one."
    );
    println!("  sinomo cluster remove <node-id> [node-id ...]");
    println!("      Remove non-voter members from quorum-backed cluster membership.");
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
    println!("      List known peers. Fails while the node is orphaned.");
    println!("  sinomo cluster leave");
    println!("      Remove local cluster state.");
}

pub(super) fn print_install_help() {
    println!("Install:");
    println!(
        "  sinomo install [--host <host>] [--port <port>] [--shadow-port <port>] [--no-start]"
    );
    println!(
        "      Create or update the local install, add boot entries for the main and shadow daemons, then start them."
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

pub(super) fn print_rollout_help() {
    println!("Rollout:");
    println!("  sinomo rollout");
    println!("      Show rollout commands.");
    println!("  sinomo rollout init-source [--repo <path>]");
    println!("      Initialize or merge an external git checkout into the managed source repo.");
    println!("  sinomo rollout import-source [--repo <path>]");
    println!(
        "      Merge external repo changes into the managed source repo without retaining external remotes."
    );
    println!("  sinomo rollout export-source --repo <path>");
    println!(
        "      Merge managed source changes, including self-evolution commits, into an external repo."
    );
    println!("  sinomo rollout apply-external [--repo <path>] [--host <host>] [--port <port>]");
    println!(
        "      Import external repo changes into managed source, then build and apply the managed source."
    );
    println!("  sinomo rollout apply [--repo <path>] [--host <host>] [--port <port>]");
    println!(
        "      Build the data-directory source repo, install bin/sinomo and bin/sinomo-shadow, restart shadow supervision, then verify main health."
    );
    println!(
        "  sinomo rollout cluster [--repo <path> | --binary <path>] [--target <host:port>] [--remote-build] [--remote-repo <path>] [--skip-local]"
    );
    println!(
        "      Build or use a dev binary for compatible nodes, or ask targets to build from their own source repo, then restart each node through its shadow daemon."
    );
    println!("  sinomo rollout diagnostics [--target <host:port>]");
    println!(
        "      Fetch rollout state, build logs, and candidate stdout/stderr tails from a daemon."
    );
    println!("  sinomo rollout status");
    println!("      Show source, binary, and rollout state paths.");
}

pub(super) fn print_dfs_help() {
    println!("DFS:");
    println!("  sinomo dfs");
    println!("      Show DFS commands.");
    println!("  sinomo dfs status [--metadata-root <path>] [--json]");
    println!("      Summarize local shard metadata, writes, repairs, and conflicts.");
    println!("  sinomo dfs filesystem <file_system_id> [--metadata-root <path>] [--json]");
    println!("      Inspect one filesystem.");
    println!("  sinomo dfs object <object_id> [--metadata-root <path>] [--json]");
    println!("      Inspect one object with versions, receipts, intents, conflicts, and repairs.");
    println!(
        "  sinomo dfs conflicts [--filesystem <file_system_id>] [--object <object_id>] [--metadata-root <path>] [--json]"
    );
    println!("      List conflict records.");
    println!(
        "  sinomo dfs repair [--filesystem <file_system_id>] [--object <object_id>] [--metadata-root <path>] [--json]"
    );
    println!("      List inferred missing replica repairs.");
    println!("  sinomo dfs metadata publish [--json]");
    println!("      Publish the local primary metadata shard journal to configured replicas.");
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

pub(super) fn print_sandbox_help() {
    println!("Sandbox:");
    println!("  sinomo sandbox");
    println!("      Show sandbox commands.");
    println!("  sinomo sandbox help");
    println!("      Show sandbox commands.");
    println!();
    println!("Implemented local staging commands:");
    println!(
        "  sinomo sandbox create [--daemon] [--id <sandbox-id>] [--toolchain <toolchain-id>] [--memory-mb <n>] [--wall-time-ms <n>] [--max-input-bytes <n>] [--max-output-bytes <n>] [--max-workspace-bytes <n>] [--max-written-bytes-per-command <n>] [--max-file-count <n>] [--tmp clear|retain] [--keep-tmp] [--mount <sifs-path>:<mount-path>[:ro|rw]]"
    );
    println!(
        "      Create a sandbox instance; --daemon stores the manifest and workspace in SIFS."
    );
    println!("  sinomo sandbox status <sandbox-id> [--daemon]");
    println!("      Show a sandbox instance manifest.");
    println!(
        "  sinomo sandbox config <sandbox-id> [--daemon] [--cwd <path>] [--env NAME=VALUE] [--unset-env NAME]"
    );
    println!("      Update persisted sandbox cwd and environment.");
    println!(
        "  sinomo sandbox run <sandbox-id> [--daemon] [--stdin] [--timeout-ms <ms>] [--allow-capability <name>] <command> -- [args...]"
    );
    println!(
        "      Run a named command from the sandbox's pinned toolchain; --allow-capability is local-only and --daemon uses the cluster sandbox policy."
    );
    println!(
        "  sinomo sandbox run <sandbox-id> [--timeout-ms <ms>] --wasm <module.wasm> -- [args...]"
    );
    println!("      Run an explicit WASI module for runtime verification.");
    println!(
        "  sinomo sandbox process-plan <sandbox-id> [--daemon] <plan.json> [--timeout-ms <ms>]"
    );
    println!(
        "      Run a structured process plan of pinned toolchain commands with per-step cwd/env and explicit stdout-to-stdin pipes; --daemon uses SIFS through the local daemon."
    );
    println!("  sinomo sandbox shell <sandbox-id> [--daemon] [--timeout-ms <ms>] [-- <script>]");
    println!(
        "      Run the pinned real `sh` command with stdin or the provided script; --daemon uses SIFS through the local daemon."
    );
    println!(
        "  sinomo sandbox toolchain add-command [--daemon] [--toolchain <toolchain-id>] [--version <version>] [--target-abi preview1|wasix] [--provenance <text>] [--capability <name>] [--memory-mb <n>] [--wall-time-ms <n>] [--max-input-bytes <n>] [--max-output-bytes <n>] [--max-workspace-bytes <n>] [--max-written-bytes-per-command <n>] [--max-file-count <n>] <command> <module.wasm>"
    );
    println!(
        "      Add a real WASI command module to a toolchain; WASIX is the default target ABI; --daemon stores it in SIFS."
    );
    println!(
        "  sinomo sandbox toolchain add-artifact [--daemon] [--toolchain <toolchain-id>] [--version <version>] [--provenance <text>] <lib|share|include|etc/path> <file>"
    );
    println!("      Add a support artifact to a toolchain; --daemon stores it in SIFS.");
    println!(
        "  sinomo sandbox toolchain import-busybox [--daemon] --version <version> [--provenance <text>] <busybox.wasm>"
    );
    println!(
        "      Import a real Sinomo-maintained BusyBox WASIX module as the default toolchain applets; --daemon stores it in SIFS."
    );
    println!(
        "  sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>] <package.json>"
    );
    println!(
        "      Import or merge a declared toolchain package containing command modules and support artifacts."
    );
    println!("  sinomo sandbox toolchain verify-package <package.json>");
    println!(
        "      Validate a package manifest, referenced files, and BLAKE3 hashes without importing it."
    );
    println!("  sinomo sandbox toolchain hash <file>");
    println!("      Print the BLAKE3 content hash used by sandbox package manifests.");
    println!("  sinomo sandbox toolchain busybox-plan");
    println!("      Show the default BusyBox applet set and fidelity cases.");
    println!("  sinomo sandbox toolchain bash-plan");
    println!("      Show the required Bash WASIX package target and fidelity cases.");
    println!("  sinomo sandbox toolchain build-tools-plan");
    println!(
        "      Show real compiler/build-tool package status and gates for makepkg, tcc, and make."
    );
    println!("  sinomo sandbox toolchain quickjs-plan");
    println!("      Show the maintained QuickJS package target and fidelity cases.");
    println!("  sinomo sandbox toolchain python-plan");
    println!("      Show the required CPython/pip WASIp1 package target and fidelity cases.");
    println!("  sinomo sandbox toolchain verify-bash <sandbox-id> [--daemon] [--allow-unsigned]");
    println!("      Run fidelity cases against the sandbox's pinned Bash toolchain.");
    println!(
        "  sinomo sandbox toolchain verify-busybox <sandbox-id> [--daemon] [--allow-unsigned]"
    );
    println!("      Run fidelity cases against the sandbox's pinned BusyBox toolchain.");
    println!(
        "  sinomo sandbox toolchain verify-quickjs <sandbox-id> [--daemon] [--allow-unsigned]"
    );
    println!("      Run fidelity cases against the sandbox's pinned QuickJS toolchain.");
    println!("  sinomo sandbox toolchain verify-python <sandbox-id> [--daemon] [--allow-unsigned]");
    println!("      Run fidelity cases against the sandbox's pinned CPython/pip toolchain.");
    println!("  sinomo sandbox toolchain verify-make <sandbox-id> [--daemon] [--allow-unsigned]");
    println!("      Run fidelity cases against the sandbox's pinned GNU Make WASIX toolchain.");
    println!(
        "  sinomo sandbox toolchain verify-makepkg <sandbox-id> [--daemon] [--allow-unsigned]"
    );
    println!("      Run fidelity cases against the sandbox's pinned makepkg WASIX package.");
    println!("  sinomo sandbox toolchain verify-tcc <sandbox-id> [--daemon] [--allow-unsigned]");
    println!("      Run fidelity cases against the sandbox's pinned TinyCC WASIX toolchain.");
    println!(
        "  sinomo sandbox toolchain verify-coding <sandbox-id> [--daemon] [--allow-unsigned] [--full]"
    );
    println!(
        "      Run coding fidelity cases against one pinned toolchain; --full expects that same sandbox to also provide Bash, Python/pip, Make, and TCC. The maintained full gate script uses separate WASIp1 and WASIX workspaces."
    );
    println!("  sinomo sandbox doctor [<sandbox-id>]");
    println!("      Inspect local sandbox/toolchain readiness and policy compatibility.");
    println!("  sinomo sandbox policy show");
    println!("      Show the cluster-scoped sandbox execution policy.");
    println!(
        "  sinomo sandbox policy set [--allow-unsigned-toolchains|--require-signed-toolchains] [--allow-remote-local-owned-mounts|--deny-remote-local-owned-mounts] [--allow-mount-prefix <sifs-prefix>] [--allow-http-host <host>] [--max-memory-mb <n>] [--max-wall-time-ms <n>] [--max-input-bytes <n>] [--max-output-bytes <n>] [--max-workspace-bytes <n>] [--max-written-bytes-per-command <n>] [--max-file-count <n>] [--max-http-timeout-ms <n>] [--max-concurrent-commands-per-sandbox <n>] [--max-concurrent-sandboxes-per-node <n>] [--allow-capability <name>] [--allow-toolchain <toolchain-id>] [--allow-toolchain-provenance-prefix <prefix>] [--allow-package-cache-prefix <sifs-prefix>]"
    );
    println!("      Update cluster-scoped sandbox execution policy in cluster KV.");
    println!("  sinomo sandbox logs <sandbox-id> [<command-id>] [--daemon]");
    println!("      List command history or print stdout/stderr for one command.");
    println!("  sinomo sandbox ls <sandbox-id> [path] [--daemon]");
    println!("      List sandbox files.");
    println!("  sinomo sandbox cat <sandbox-id> <path> [--range <start:end>] [--daemon]");
    println!("      Print one sandbox file or byte range.");
    println!(
        "  sinomo sandbox write <sandbox-id> <path> [--file <host-path>|--stdin|<literal>] [--daemon]"
    );
    println!("      Write bytes to one sandbox file.");
    println!(
        "  sinomo sandbox fetch <sandbox-id> <url> <output-path> [--daemon] [--timeout-ms <ms>] [--max-response-bytes <n>] [--header <name:value>]"
    );
    println!(
        "      Fetch an allowlisted HTTP(S) URL through daemon-mediated network transport into a sandbox file."
    );
    println!("  sinomo sandbox checkpoint <sandbox-id> [--daemon]");
    println!("      Create a sandbox checkpoint.");
    println!("  sinomo sandbox resume <sandbox-id> <checkpoint-id> [--daemon]");
    println!("      Restore a sandbox checkpoint.");
    println!("  sinomo sandbox reboot <sandbox-id> [--daemon]");
    println!(
        "      Start a new sandbox session; clears /tmp unless the instance policy retains it."
    );
    println!("  sinomo sandbox destroy <sandbox-id> [--daemon]");
    println!("      Delete a sandbox instance and all checkpoints.");
    println!();
    println!(
        "Local commands use the local staging backend. Use --daemon on create, status, config, toolchain imports/adds, toolchain verifiers, run, process-plan, shell, logs, ls, cat, write, fetch, checkpoint, resume, reboot, and destroy for the SIFS-backed daemon path. `sandbox process-plan` executes structured steps, not shell syntax. `sandbox shell` only invokes a pinned real `sh` command when the toolchain provides one; it does not fake shell behavior."
    );
}

pub(super) fn print_models_help() {
    println!("Models:");
    println!("  sinomo models");
    println!("      Show the current default model and this help.");
    println!("  sinomo models list [--provider <provider>]");
    println!("      List available or known models.");
    println!("  sinomo models default --provider <provider> --model <model>");
    println!("      Set the default model for CUI and model-backed commands.");
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
    println!("  anthropic");
    println!("  openai");
    println!("  gemini");
    println!("  nvidia");
}

pub(super) fn print_evolve_help() {
    println!("Evolution:");
    println!("  sinomo evolve");
    println!("      Show evolution commands.");
    println!(
        "  sinomo evolve propose \"<objective>\" [--repo <path>] [--provider <provider>] [--model <model>] [--verify \"<command>\"]"
    );
    println!(
        "      With --dry-run or --pause-after-proposal, write a phase-0 campaign bundle under $SINOMO_DATA_DIR/evolution/campaigns/."
    );
    println!(
        "      Campaign flags: --objective <text> --horizon <duration> --budget-policy <policy> --reserve <resource:percent%> --approval <policy> --max-wall-time <duration> [--approve]"
    );
    println!(
        "  sinomo evolve schedule <campaign> --dry-run [--resource <resource:used/total[:window]>] [--estimate <resource:units>]"
    );
    println!(
        "      Write phase-1 resource-ledger, lease request/decision, scheduler dry-run, and pause/schedule decision artifacts."
    );
    println!(
        "  sinomo evolve iterate <campaign> --dry-run [--task <task_id>] [--path <glob>] [--verify \"<command>\"]"
    );
    println!(
        "      Write phase-2 scheduled-iteration handoff artifacts without starting an implementation agent."
    );
    println!(
        "  sinomo evolve evaluate <campaign> --job <evojob_...> --task <task_id> [--failure-class <class>] [--root-cause <text>] [--learned <text>] [--usage <resource:estimated[:actual]>]"
    );
    println!(
        "      Write phase-3 stage scorecard/adaptation artifacts and update campaign state after candidate evaluation."
    );
    println!(
        "  sinomo evolve tick --dry-run [--scheduler-service <service_id>] [--campaign <campaign>] [--workload <system:id:status[:title]>] [--workload-estimate <system:id:resource:units>] [--resource <resource:used/total[:window]>] [--estimate <task:resource:units>] [--max-concurrent <n>] [--max-concurrent-workloads <n>] [--idle-window-open] [--reset-window-open]"
    );
    println!(
        "      Write phase-4 background scheduler plan/status/event artifacts and dry-run iteration handoffs without autonomous execution."
    );
    println!("  sinomo evolve approve [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve reject [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve restore [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve status [--job evojob_...]");
    println!("  sinomo evolve show [--diff|--scorecard|--decision|--packet] [--job evojob_...]");
    println!("  sinomo evolve verify [--job evojob_...]");
    println!("  sinomo evolve packet [--job evojob_...]");
    println!("  sinomo evolve cleanup [--job evojob_...]");
    println!("  sinomo evolve job create \"<objective>\" [--repo <path>]");
    println!("  sinomo evolve models ...");
    println!("      Compatibility alias for `sinomo models ...`.");
}
