import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location("app_use_package", Path(__file__).with_name("package.py"))
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)
A = packager.ANDROID


class ManifestBoundaryTests(unittest.TestCase):
    def test_preserves_host_identity_and_keeps_os_services_guarded(self):
        with tempfile.TemporaryDirectory() as work:
            source = Path(work) / "base.xml"
            result = Path(work) / "merged.xml"
            source.write_text('''<manifest xmlns:android="http://schemas.android.com/apk/res/android" package="net.sinomo.app" android:versionCode="42">
              <uses-sdk android:minSdkVersion="26" android:targetSdkVersion="35"/>
              <uses-permission android:name="android.permission.INTERNET"/>
              <application android:hasCode="false"><activity android:name="android.app.NativeActivity"><meta-data android:name="android.app.lib_name" android:value="main"/></activity></application>
            </manifest>''')
            packager.merge_manifest(source, result)
            root = ET.parse(result).getroot()
            self.assertEqual(root.get(A + "versionCode"), "42")
            app = root.find("application")
            self.assertEqual(app.get(A + "hasCode"), "true")
            self.assertEqual(app.find("activity/meta-data").get(A + "value"), "main")
            services = {s.get(A + "name"): s for s in app.findall("service")}
            self.assertEqual(services["net.sinomo.app.use.AppUseProjectionService"].get(A + "exported"), "false")
            self.assertEqual(services["net.sinomo.app.use.AppUseAccessibilityService"].get(A + "permission"), "android.permission.BIND_ACCESSIBILITY_SERVICE")
            permissions = {p.get(A + "name") for p in root.findall("uses-permission")}
            self.assertIn("android.permission.INTERNET", permissions)
            self.assertIn("android.permission.FOREGROUND_SERVICE_MEDIA_PROJECTION", permissions)
            with self.assertRaises(ValueError):
                packager.merge_manifest(result, Path(work) / "duplicate.xml")


if __name__ == "__main__":
    unittest.main()
