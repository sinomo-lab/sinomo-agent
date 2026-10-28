// Vendored header inputs: see ../UPSTREAM.json. No host pkg-config lookup.
use std::{env, fs, path::PathBuf};
pub fn prepare() -> Vec<PathBuf> {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("headers");
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("include");
    fs::create_dir_all(out.join("pipewire")).unwrap();
    let version = fs::read_to_string(root.join("src/pipewire/version.h.in"))
        .unwrap()
        .replace("@PIPEWIRE_VERSION_MAJOR@", "0")
        .replace("@PIPEWIRE_VERSION_MINOR@", "3")
        .replace("@PIPEWIRE_VERSION_MICRO@", "65")
        .replace("@PIPEWIRE_API_VERSION@", "\"0.3\"");
    fs::write(out.join("pipewire/version.h"), version).unwrap();
    println!("cargo:rerun-if-changed=headers");
    println!("cargo:rerun-if-changed=headers.rs");
    println!("cargo:sinomo_headers={}", root.display());
    println!("cargo:sinomo_generated={}", out.display());
    vec![root.join("spa/include"), root.join("src"), out]
}
