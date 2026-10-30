"""Validate the platform payload contract independently of native packager tools."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("installer", Path(__file__).with_name("build-desktop-installer.py"))
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class PayloadContract(unittest.TestCase):
    def test_both_formats_carry_runtime_and_sibling_manifest(self):
        for target in ("x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"):
            config = installer.configuration(Path("stage"), Path("dist"), target, "1.2.3")
            self.assertEqual({b["path"] for b in config["binaries"]},
                             {"sinomo", "sinomo-app", "sinomo-ui-extension-host", "sinomo-setup"})
            self.assertEqual([b["path"] for b in config["binaries"] if b["main"]], ["sinomo-setup"])
            if "windows" in target:
                self.assertIn("desktop-release.json", [r["target"] for r in config["resources"]])
                self.assertEqual(config["nsis"]["installMode"], "currentUser")
            else:
                self.assertEqual(config["appimage"]["files"][str(Path("stage/sinomo-payload.tar"))],
                                 "usr/bin/sinomo-payload.tar")

    def test_generated_icon_is_a_square_rgba_png(self):
        with tempfile.TemporaryDirectory() as directory:
            icon = Path(directory) / "icon.png"
            installer.write_icon(icon)
            data = icon.read_bytes()
            self.assertTrue(data.startswith(b"\x89PNG\r\n\x1a\n"))
            self.assertEqual(installer.struct.unpack(">II", data[16:24]), (256, 256))


if __name__ == "__main__":
    unittest.main()
