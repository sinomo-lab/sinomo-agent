//! Embeds build metadata and target-specific application metadata into the
//! final `sinomo-app` binary.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use android_build::{DebugInfo, Dexer, JavaBuild};

const ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV: &str = "SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE";

fn main() {
    println!("cargo:rerun-if-changed=assets/logo.ico");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        winresource::WindowsResource::new()
            .set_icon("assets/logo.ico")
            .compile()
            .expect("compile Sinomo executable icon");
    }
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-arg=-Wl,--disable-new-dtags,-rpath,$ORIGIN/lib");
    }
    build_android_lifecycle_bridge();
    embed_macos_privacy_plist();
    println!("cargo:rerun-if-env-changed={ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV}");
    let commit = std::env::var(ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SINOMO_GIT_COMMIT={commit}");
    watch_git_head();
}

fn build_android_lifecycle_bridge() {
    println!("cargo:rerun-if-changed=java/");
    if !std::env::var("TARGET").is_ok_and(|target| target.contains("android")) {
        return;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    panic!("Android Java build is only supported from Linux, macOS, or Windows hosts");

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    build_android_lifecycle_java();
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn build_android_lifecycle_java() {
    let android_jar = android_build::android_jar(None).expect("locate Android android.jar");
    let release = std::env::var("PROFILE").as_deref() == Ok("release");
    let java_path = "java/net/sinomo/app/runtime/SinomoLifecycleCallbacks.java";
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let classes_out_dir = out_dir.join("java/net/sinomo/app/runtime");
    let _ = fs::remove_dir_all(&classes_out_dir);
    fs::create_dir_all(&classes_out_dir).expect("create Android Java classes directory");
    JavaBuild::new()
        .file(java_path)
        .file("java/net/sinomo/app/runtime/SinomoVideoEncoder.java")
        .class_path(&android_jar)
        .classes_out_dir(&classes_out_dir)
        .java_source_version(8)
        .java_target_version(8)
        .debug_info(DebugInfo {
            line_numbers: !release,
            source_files: !release,
            variables: !release,
        })
        .compile()
        .expect("compile Android lifecycle bridge");

    let dex_output_dir = out_dir.join("lifecycle-dex");
    fs::create_dir_all(&dex_output_dir).expect("create Android lifecycle DEX directory");
    let java_classes_root = out_dir.join("java");
    Dexer::new()
        .android_jar(&android_jar)
        .class_path(&java_classes_root)
        .collect_classes(&java_classes_root)
        .expect("collect Android lifecycle classes")
        .release(release)
        .android_min_api(26)
        .out_dir(&dex_output_dir)
        .run()
        .expect("dex Android lifecycle bridge");

    let dex_path = dex_output_dir.join("classes.dex");
    assert!(
        dex_path.exists(),
        "Android lifecycle DEX was not created at {}",
        dex_path.display()
    );
    println!(
        "cargo:rustc-env=SINOMO_ANDROID_LIFECYCLE_DEX_PATH={}",
        dex_path.display()
    );
}

fn embed_macos_privacy_plist() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"),
    );
    let plist = manifest_dir.join("assets").join("macos").join("Info.plist");
    println!("cargo:rerun-if-changed={}", plist.display());
    println!(
        "cargo:rustc-link-arg-bin=sinomo-app=-Wl,-sectcreate,__TEXT,__info_plist,{}",
        plist.display()
    );
}

fn git_commit() -> Option<String> {
    let hash = run(&["rev-parse", "--short=12", "HEAD"])?;
    let hash = hash.trim();
    if hash.is_empty() {
        return None;
    }
    let dirty = Command::new("git")
        .args(["diff", "--quiet", "HEAD"])
        .status()
        .map(|status| !status.success())
        .unwrap_or(false);
    Some(if dirty {
        format!("{hash}-dirty")
    } else {
        hash.to_string()
    })
}

fn watch_git_head() {
    let Some(git_dir) = run(&["rev-parse", "--git-dir"]) else {
        return;
    };
    let git_dir = PathBuf::from(git_dir.trim());
    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display()
    );

    let Ok(head) = fs::read_to_string(git_dir.join("HEAD")) else {
        return;
    };
    let Some(reference) = head.strip_prefix("ref:").map(str::trim) else {
        return;
    };
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join(reference).display()
    );
}

fn run(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
