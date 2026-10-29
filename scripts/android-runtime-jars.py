#!/usr/bin/env python3
"""Resolve the locked TLS verifier's JVM helper for the native Android APK.

cargo-apk does not resolve Maven dependencies. Use the AAR shipped with the
locked Rust dependency and a pinned, hash-checked Kotlin runtime, then let the
APK packager merge both JARs into the application class loader's DEX.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request
import zipfile

KOTLIN_VERSION = "1.9.25"
KOTLIN_SHA256 = "f9cdcdbff1f5de85380ae526977e683726c2aa42db1ed6e6e50ae89e496e95fd"
KOTLIN_URL = (
    "https://repo.maven.apache.org/maven2/org/jetbrains/kotlin/kotlin-stdlib/"
    f"{KOTLIN_VERSION}/kotlin-stdlib-{KOTLIN_VERSION}.jar"
)


def prepare(output):
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--format-version", "1",
        "--filter-platform", "aarch64-linux-android",
    ]))
    packages = [p for p in metadata["packages"]
                if p["name"] == "rustls-platform-verifier-android"]
    if len(packages) != 1:
        raise ValueError("expected exactly one locked Android TLS helper")
    package = packages[0]
    root = Path(package["manifest_path"]).parent
    version = package["version"]
    aar = (root / "maven/rustls/rustls-platform-verifier" / version
           / f"rustls-platform-verifier-{version}.aar")
    output.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(aar) as archive:
        (output / "rustls-platform-verifier.jar").write_bytes(archive.read("classes.jar"))

    kotlin = output / "kotlin-stdlib.jar"
    data = kotlin.read_bytes() if kotlin.exists() else b""
    if hashlib.sha256(data).hexdigest() != KOTLIN_SHA256:
        with urllib.request.urlopen(KOTLIN_URL, timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != KOTLIN_SHA256:
            raise ValueError("Kotlin runtime SHA-256 mismatch")
        kotlin.write_bytes(data)
    print(f"Android TLS helper {version}, Kotlin {KOTLIN_VERSION}: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    prepare(parser.parse_args().output)
