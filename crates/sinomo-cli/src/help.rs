use super::*;

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo");
    for command in cli_command_registry().metadata() {
        println!("  sinomo {}", command.name);
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
