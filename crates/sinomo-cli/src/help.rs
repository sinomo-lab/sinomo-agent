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
    println!("  SINOMO_CODEX_AUTH_ISSUER       Overrides the Codex ChatGPT auth issuer");
    println!("  SINOMO_CODEX_CLIENT_ID         Overrides the Codex ChatGPT OAuth client ID");
    println!("  SINOMO_CODEX_API_BASE_URL      Overrides the Codex ChatGPT backend API base URL");
    println!("  ANTHROPIC_API_KEY           Used by `providers auth/test anthropic`");
    println!("  OPENAI_API_KEY              Used by `providers auth/test openai`");
    println!("  GEMINI_API_KEY              Used by `providers auth/test gemini`");
    println!("  NVIDIA_API_KEY              Used by `providers auth/test nvidia`");
    println!("  SINOMO_NVIDIA_API_BASE_URL  Overrides the NVIDIA API base URL");
    println!(
        "  SINOMO_COPILOT_CLIENT_ID    Defaults to {}",
        GitHubCopilotConfig::default().client_id
    );
    println!(
        "                               GitHub App client IDs are not accepted by Copilot token exchange"
    );
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
    println!("  sinomo cluster status");
    println!("      Show local cluster status and node health counts.");
    println!("  sinomo cluster health");
    println!(
        "      Report node online/offline state, daemon version, platform, and rollout status."
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
    println!("  sinomo models list [--provider <provider>]");
    println!("      List available or known models.");
    println!("  sinomo models default --provider <provider> --model <model>");
    println!("      Set the default model for CUI and model-backed commands.");
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
