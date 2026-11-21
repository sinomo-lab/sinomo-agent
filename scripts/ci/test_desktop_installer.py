"""Validate the platform payload contract independently of native packager tools."""
import importlib.util
from pathlib import Path
import tempfile
import struct
import unittest

spec = importlib.util.spec_from_file_location("installer", Path(__file__).with_name("build-desktop-installer.py"))
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class PayloadContract(unittest.TestCase):
    def test_both_formats_carry_runtime_and_sibling_manifest(self):
        for target in ("x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"):
            config = installer.configuration(Path("stage"), Path("dist"), target, "1.2.3")
            self.assertEqual([b["path"] for b in config["binaries"] if b["main"]], ["sinomo-setup"])
            if "windows" in target:
                self.assertEqual({b["path"] for b in config["binaries"]},
                                 {"sinomo", "sinomo-app", "sinomo-ui-extension-host", "sinomo-setup"})
                self.assertIn("desktop-release.json", [r["target"] for r in config["resources"]])
                self.assertEqual(config["nsis"]["installMode"], "currentUser")
                self.assertTrue(Path(config["nsis"]["installerIcon"]).is_file())
            else:
                self.assertEqual([b["path"] for b in config["binaries"]], ["sinomo-setup"])
                self.assertEqual(config["appimage"]["files"][str(Path("stage/sinomo-payload.tar"))],
                                 "usr/bin/sinomo-payload.tar")

    def test_staged_icon_is_the_sinomo_logo(self):
        with tempfile.TemporaryDirectory() as directory:
            icon = Path(directory) / "icon.png"
            installer.write_icon(icon)
            data = icon.read_bytes()
            self.assertTrue(data.startswith(b"\x89PNG\r\n\x1a\n"))
            self.assertEqual(struct.unpack(">II", data[16:24]), (256, 256))
            self.assertEqual(data, (installer.ROOT / "crates/sinomo-app/assets/logo.png").read_bytes())


if __name__ == "__main__":
    unittest.main()
