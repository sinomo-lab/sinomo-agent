mod dependency_headers;

fn main() {
    // FIXME: It would be nice to run this only when tests are run.
    println!("cargo:rerun-if-changed=tests/pod.c");

    let include_paths = dependency_headers::include_paths();

    cc::Build::new()
        .file("tests/pod.c")
        .flag("-Wno-missing-field-initializers")
        .includes(&include_paths)
        .compile("pod");
}
