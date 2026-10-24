#!/usr/bin/env python3
"""Build the disposable fixture and non-suppressing UI inspector; do not install."""
import argparse
import os
from pathlib import Path
import subprocess
import zipfile


def run(*args, **kwargs):
    subprocess.run([str(arg) for arg in args], check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", type=Path, default=os.environ.get("ANDROID_HOME"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--keystore", type=Path, required=True)
    parser.add_argument("--password-env", required=True)
    args = parser.parse_args()
    if args.sdk is None:
        parser.error("--sdk or ANDROID_HOME is required")
    if args.password_env not in os.environ:
        parser.error("signing password environment variable is not set")
    source = Path(__file__).resolve().parent
    tools = args.sdk / "build-tools/35.0.0"
    jar = args.sdk / "platforms/android-35/android.jar"
    for name, directory, java in [
        ("fixture", source, "FixtureActivity.java"),
        ("inspect", source / "inspect", "Inspect.java"),
    ]:
        work = args.output / name
        classes, dex = work / "classes", work / "dex"
        classes.mkdir(parents=True, exist_ok=True)
        dex.mkdir(parents=True, exist_ok=True)
        run("javac", "--release", "8", "-cp", jar, "-d", classes, directory / java)
        run(tools / "d8", "--lib", jar, "--min-api", "26", "--output", dex, *classes.rglob("*.class"))
        run(tools / "aapt2", "link", "-I", jar, "--manifest", directory / "AndroidManifest.xml", "-o", work / "unsigned.apk")
        with zipfile.ZipFile(work / "unsigned.apk", "a") as archive:
            archive.write(dex / "classes.dex", "classes.dex")
        run(tools / "zipalign", "-f", "4", work / "unsigned.apk", work / "aligned.apk")
        run(tools / "apksigner", "sign", "--ks", args.keystore, "--ks-pass", f"env:{args.password_env}",
            "--out", work / f"{name}.apk", work / "aligned.apk")
        run(tools / "apksigner", "verify", work / f"{name}.apk")


if __name__ == "__main__":
    main()
