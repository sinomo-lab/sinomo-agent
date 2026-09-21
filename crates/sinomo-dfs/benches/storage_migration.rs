//! Run explicitly with `cargo bench -p sinomo-dfs --features migration-bench --bench storage_migration`.
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Cargo also executes harness-free benches during `test --all-targets`.
    // Only an explicit `cargo bench` invocation should run this scale workload.
    if !std::env::args().any(|argument| argument == "--bench") {
        println!(
            "Skipped storage migration benchmark; run cargo bench --bench storage_migration --features migration-bench"
        );
        return;
    }
    let root = std::env::var_os("SINOMO_MIGRATION_SCALE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let run = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/storage-migration-bench")
                .join(format!("{run}-{}", std::process::id()))
        });
    let count = std::env::var("SINOMO_MIGRATION_SCALE_RECORDS")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("record count must be an integer")
        })
        .unwrap_or(1_000_000);
    let result = sinomo_dfs::benchmark_storage_migration(&root, count);
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    println!("Results: {}", root.join("scale-result.json").display());
}
