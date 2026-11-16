fn main() {
    println!("cargo:rerun-if-changed=src/desktop_gpu.c");
    #[cfg(feature = "desktop-gpu")]
    {
        let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
        if !matches!(target.as_str(), "linux" | "windows" | "macos") {
            return;
        }
        let mut build = cc::Build::new();
        build.file("src/desktop_gpu.c");
        for name in ["libavcodec", "libavutil", "libswscale"] {
            let library = pkg_config::Config::new()
                .probe(name)
                .expect("desktop-gpu requires FFmpeg development libraries via pkg-config");
            for path in library.include_paths {
                build.include(path);
            }
        }
        build.compile("sinomo_desktop_gpu");
    }
}
