use std::{env, path::PathBuf};
pub fn include_paths() -> Vec<PathBuf> {
    let read = |key: &str| {
        let value = env::var_os(format!("DEP_LIBSPA_0.2_{key}"))
            .or_else(|| env::var_os(format!("DEP_LIBSPA_0_2_{key}")))
            .unwrap_or_else(|| panic!("vendored libspa-sys did not publish {key}"));
        PathBuf::from(value)
    };
    let root = read("SINOMO_HEADERS");
    vec![
        root.join("spa/include"),
        root.join("src"),
        read("SINOMO_GENERATED"),
    ]
}
