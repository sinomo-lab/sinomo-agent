fn main() {
    let target = std::env::var("TARGET").expect("Cargo provides the compilation target");
    println!("cargo:rustc-env=SINOMO_API_RUST_TARGET={target}");
}
