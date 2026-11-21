#!/usr/bin/env python3
"""Inspect the signed ARM64 testing APK produced by the Android CI job."""
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile


def check(apk):
    sdk = Path(os.environ["ANDROID_HOME"])
    tools = sdk / "build-tools/35.0.0"
    analyzer = sdk / "cmdline-tools/latest/bin/apkanalyzer"
    subprocess.run([tools / "apksigner", "verify", "--print-certs", apk], check=True)
    subprocess.run([tools / "zipalign", "-c", "-P", "16", "4", apk], check=True)

    android = "{http://schemas.android.com/apk/res/android}"
    manifest = ET.fromstring(subprocess.check_output([analyzer, "manifest", "print", apk]))
    app = manifest.find("application")
    if manifest.get("package") != "net.sinomo.app" or app is None or app.get(android + "debuggable") != "true":
        raise ValueError("expected a debuggable net.sinomo.app")
    services = {s.get(android + "name") for s in app.findall("service")}
    if not {"net.sinomo.app.use.AppUseProjectionService", "net.sinomo.app.use.AppUseAccessibilityService"} <= services:
        raise ValueError("App Use service declarations are missing")
    badging = subprocess.check_output([tools / "aapt", "dump", "badging", apk], text=True)
    icons = {line.split(":'", 1)[1].rstrip("'") for line in badging.splitlines()
             if line.startswith("application-icon-")}
    with zipfile.ZipFile(apk) as package:
        files = package.namelist()
        if not icons or not icons.issubset(files) or any(Path(icon).stem != "ic_launcher" for icon in icons):
            raise ValueError(f"expected packaged Sinomo launcher icons, got {sorted(icons)}")
        if "lib/arm64-v8a/libmain.so" not in files or "classes.dex" not in files:
            raise ValueError("ARM64 NativeActivity library or application DEX is missing")
        if len(files) != len(set(files)):
            raise ValueError("duplicate APK archive entries")
    for cls in ("net.sinomo.app.use.AppUseAccessibilityService",
                "org.rustls.platformverifier.CertificateVerifier", "kotlin.jvm.internal.Intrinsics"):
        subprocess.run([analyzer, "dex", "code", "--class", cls, apk], stdout=subprocess.DEVNULL, check=True)
    print("Verified ARM64 NativeActivity, launcher icon, App Use services, TLS verifier and Kotlin runtime.")


if __name__ == "__main__":
    check(Path(sys.argv[1]).resolve())
