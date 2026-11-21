#!/usr/bin/env python3
"""Add App Use's OS services to a cargo-apk APK, then align and re-sign it.

cargo-apk cannot declare services or package Java classes. Its generated manifest
remains the source of the app's package/version/activity/permissions. The provider
Rust dylib is still built and installed through the native module lifecycle.
"""
import argparse
import copy
import os
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ANDROID = "{http://schemas.android.com/apk/res/android}"
ET.register_namespace("android", ANDROID[1:-1])
SOURCE = Path(__file__).resolve().parent


def merge_manifest(source, destination, debuggable=False):
    tree = ET.parse(source)
    root = tree.getroot()
    application = root.find("application")
    if application is None or root.get("package") != "net.sinomo.app":
        raise ValueError("expected the generated net.sinomo.app manifest")
    application.set(ANDROID + "hasCode", "true")
    if debuggable:
        application.set(ANDROID + "debuggable", "true")
    fragment = ET.parse(SOURCE / "manifest.xml").getroot()
    for permission in fragment.findall("uses-permission"):
        if not any(p.get(ANDROID + "name") == permission.get(ANDROID + "name") for p in root.findall("uses-permission")):
            root.append(copy.deepcopy(permission))
    for component in fragment.find("application"):
        if any(c.get(ANDROID + "name") == component.get(ANDROID + "name") for c in application):
            raise ValueError("App Use components must be merged exactly once")
        application.append(copy.deepcopy(component))
    tree.write(destination, encoding="utf-8", xml_declaration=True)


def run(*args):
    subprocess.run([str(arg) for arg in args], check=True)


def signers(tools, apk):
    result = subprocess.check_output([str(tools / "apksigner"), "verify", "--print-certs", str(apk)], text=True)
    return {line.strip() for line in result.splitlines() if "certificate SHA-256 digest:" in line}


def package(apk, manifest, sdk, tools, keystore, password_env, alias, java_archives=(), debuggable=False,
            app_res=None):
    if not apk.is_file() or not manifest.is_file() or not keystore.is_file():
        raise ValueError("APK, generated manifest and existing signing keystore are required")
    if password_env not in os.environ:
        raise ValueError(f"set {password_env} to the existing APK signing password")
    original_signers = signers(tools, apk)
    if not original_signers:
        raise ValueError("base APK has no verified signing certificate")
    # Build alongside the APK; publish only after signature verification succeeds.
    with tempfile.TemporaryDirectory(prefix="app-use-", dir=apk.parent) as temporary:
        work = Path(temporary)
        merged = work / "AndroidManifest.xml"
        merge_manifest(manifest, merged, debuggable=debuggable)
        target_sdk = ET.parse(merged).getroot().find("uses-sdk").get(ANDROID + "targetSdkVersion")
        android_jar = sdk / "platforms" / f"android-{target_sdk}" / "android.jar"
        classes = work / "classes"
        classes.mkdir()
        run("javac", "--release", "8", "-classpath", android_jar, "-d", classes, *sorted((SOURCE / "java").rglob("*.java")))
        dex = work / "dex"
        dex.mkdir()
        run(tools / "d8", "--lib", android_jar, "--min-api", "26", "--output", dex,
            *sorted(classes.rglob("*.class")), *java_archives)
        resources = []
        for directory in ([app_res] if app_res is not None else []) + [SOURCE / "res"]:
            compiled_dir = work / f"resources-{len(resources)}.zip"
            run(tools / "aapt2", "compile", "--dir", directory, "-o", compiled_dir)
            resources.append(compiled_dir)
        compiled = work / "resources.apk"
        run(tools / "aapt2", "link", "-I", android_jar, "--manifest", merged, "-o", compiled, *resources)
        unsigned = work / "unsigned.apk"
        with zipfile.ZipFile(apk) as original, zipfile.ZipFile(compiled) as resource_apk, zipfile.ZipFile(unsigned, "w") as output:
            # App's existing Java bytecode is not silently replaced. The current
            # lifecycle bridge is embedded DEX loaded by Rust, not classes.dex.
            if any(n.startswith("classes") and n.endswith(".dex") for n in original.namelist()):
                raise ValueError("base APK already contains DEX; use a merged DEX build")
            original_resources = {n for n in original.namelist() if n.startswith("res/") and not n.endswith("/")}
            if original_resources and (app_res is None or original_resources.difference(resource_apk.namelist())):
                raise ValueError("base APK resources are missing from the merged resources")
            for info in original.infolist():
                if info.filename in ("AndroidManifest.xml", "resources.arsc") or info.filename.startswith(("META-INF/", "res/")):
                    continue
                output.writestr(info, original.read(info))
            for info in resource_apk.infolist():
                output.writestr(info, resource_apk.read(info))
            output.write(dex / "classes.dex", "classes.dex")
        aligned = work / "aligned.apk"
        run(tools / "zipalign", "-P", "16", "-f", "4", unsigned, aligned)
        signed = work / "signed.apk"
        alias_args = ["--ks-key-alias", alias] if alias else []
        run(tools / "apksigner", "sign", "--ks", keystore, *alias_args,
            "--ks-pass", f"env:{password_env}", "--key-pass", f"env:{password_env}", "--out", signed, aligned)
        run(tools / "apksigner", "verify", signed)
        if signers(tools, signed) != original_signers:
            raise ValueError("App Use packaging must preserve the original APK signer")
        signed.replace(apk)
        signature = signed.with_suffix(".apk.idsig")
        if signature.exists():
            signature.replace(apk.with_suffix(".apk.idsig"))
        else:
            apk.with_suffix(".apk.idsig").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("apk", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--sdk", type=Path, default=os.environ.get("ANDROID_HOME"))
    parser.add_argument("--build-tools", default="35.0.0")
    parser.add_argument("--keystore", type=Path, required=True)
    parser.add_argument("--password-env", required=True)
    parser.add_argument("--alias", help="key alias, only needed for keystores with multiple entries")
    parser.add_argument("--java-archive", type=Path, action="append", default=[],
                        help="additional application JAR to merge into the APK's DEX")
    parser.add_argument("--app-res", type=Path, help="host application's Android resource source directory")
    parser.add_argument("--debuggable", action="store_true",
                        help="enable Android debugging for an explicitly selected development build")
    args = parser.parse_args()
    if args.sdk is None:
        parser.error("--sdk or ANDROID_HOME is required")
    manifest = args.manifest or args.apk.parent / "AndroidManifest.xml"
    package(args.apk.resolve(), manifest.resolve(), args.sdk, args.sdk / "build-tools" / args.build_tools,
            args.keystore, args.password_env, args.alias, args.java_archive, args.debuggable, args.app_res)


if __name__ == "__main__":
    main()
