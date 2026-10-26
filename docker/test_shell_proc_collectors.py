#!/usr/bin/env python3
"""Exercise the maintained shell collectors against controlled proc files."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("owned_window", Path(__file__).with_name("measure-owned-io.py"))
window = importlib.util.module_from_spec(spec)
spec.loader.exec_module(window)


class ShellCollectorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.proc = self.root / "proc"
        self.proc.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        # Missing pgrep/awk/cat is intentional: these snapshots must not need
        # their executable/library loads inside every measured container.
        for name in ["sh", "tr", "stat", "readlink"]:
            executable = shutil.which("dash") if name == "sh" else shutil.which(name)
            (self.bin / name).symlink_to(executable or shutil.which(name))
        self.environment = dict(os.environ, PATH=str(self.bin))
        self.output = self.root / "logs"
        self.output.mkdir()
        self.executable = self.root / "sinomo"
        self.executable.write_bytes(b"controlled executable identity")
        self.make_process(10, "main", 100)
        self.make_process(11, "shadow", 200)
        self.make_process(12, "helper", 300)

    def make_process(self, pid, role, base):
        proc = self.proc / str(pid)
        proc.mkdir()
        (proc / "comm").write_text("sinomo\n")
        args = ["sinomo", "daemon", "run", "--fixture"]
        if role == "shadow":
            args.insert(2, "shadow")
        elif role == "helper":
            args = ["sinomo", "test", "run", "--fixture"]
        (proc / "cmdline").write_bytes(b"\0".join(a.encode() for a in args) + b"\0")
        fields = ["S"] + ["0"] * 49
        fields[19] = str(base + 7)
        (proc / "stat").write_text(f"{pid} (sinomo) " + " ".join(fields) + "\n")
        (proc / "exe").symlink_to(self.executable)
        (proc / "fd").mkdir()
        for fd, suffix in [(1, "stdout"), (2, "stderr")]:
            sink = self.output / f"{role}.{suffix}.log"
            sink.touch()
            (proc / "fd" / str(fd)).symlink_to(sink)
        (proc / "io").write_text(
            f"rchar: {base}\nwchar: {base * 2}\nsyscr: 10\nsyscw: 20\n"
            f"read_bytes: {base * 3}\nwrite_bytes: 0\ncancelled_write_bytes: 0\n"
        )

    def run_shell(self, script):
        return subprocess.run([str(self.bin / "sh"), "-c", script.replace("/proc/", str(self.proc) + "/")],
                              env=self.environment, capture_output=True, text=True, timeout=5)

    def legacy_function(self, name):
        source = Path(__file__).with_name("measure-idle.sh").read_text()
        function = name + "() {" + source.split(name + "() {", 1)[1].split("\n}\n", 1)[0] + "\n}"
        return self.run_shell("docker() { shift 4; \"$@\"; }\n" + function + f"\n{name} fixture\n")

    def owned_snapshot(self):
        result = self.run_shell(window.SNAPSHOT)
        self.assertEqual(result.returncode, 0, result.stderr)
        # The sink parser's /data ownership rule is exercised with a controlled
        # pathname while preserving the actual stat device/inode/mode fields.
        raw = result.stdout.replace(str(self.output), "/data/daemon")
        with patch.object(window, "command", return_value=raw):
            return window.daemon_snapshot("fixture")

    def test_legacy_totals_and_role_start_identity_without_external_scanners(self):
        result = self.legacy_function("node_io")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "10:107,11:207 300 600 900 0")

    def test_owned_raw_fields_and_file_identities_are_preserved(self):
        snapshot = self.owned_snapshot()
        self.assertEqual(set(snapshot["roles"]), {"main", "shadow"})
        for role, pid, base in [("main", 10, 100), ("shadow", 11, 200)]:
            row = snapshot["roles"][role]
            self.assertEqual(row["identity"][:2], [str(pid), str(base + 7)])
            self.assertEqual(row["io"]["rchar"], base)
            self.assertEqual(row["io"]["read_bytes"], base * 3)
            self.assertEqual(row["io"]["cancelled_write_bytes"], 0)
            self.assertEqual(int(row["sinks"]["stdout"][2]), (self.output / f"{role}.stdout.log").stat().st_ino)

    def test_duplicate_roles_missing_io_and_invalid_start_fail_closed(self):
        self.make_process(14, "main", 400)
        self.assertNotEqual(self.legacy_function("node_io").returncode, 0)
        with self.assertRaises(window.IncompleteObservation):
            self.owned_snapshot()
        shutil.rmtree(self.proc / "14")
        (self.proc / "10" / "io").unlink()
        self.assertNotEqual(self.legacy_function("node_io").returncode, 0)
        self.assertNotEqual(self.run_shell(window.SNAPSHOT).returncode, 0)
        (self.proc / "10" / "io").write_text("rchar: 100\nwchar: 200\nread_bytes: 300\nwrite_bytes: 0\n")
        (self.proc / "10" / "stat").write_text("10 (sinomo) S 0\n")
        self.assertNotEqual(self.legacy_function("node_io").returncode, 0)
        self.assertNotEqual(self.run_shell(window.SNAPSHOT).returncode, 0)

    def test_counter_and_sink_changes_are_detected_from_actual_shell_output(self):
        before = self.owned_snapshot()
        io = self.proc / "10" / "io"
        io.write_text(io.read_text().replace("write_bytes: 0", "write_bytes: 4096"))
        after = self.owned_snapshot()
        self.assertEqual(window.daemon_delta(before, after)["main"]["write_bytes"], 4096)
        io.write_text(io.read_text().replace("rchar: 100", "rchar: 1"))
        with self.assertRaisesRegex(window.IncompleteObservation, "reset"):
            window.daemon_delta(before, self.owned_snapshot())
        sink = self.proc / "11" / "fd" / "1"
        replacement = self.output / "replacement.log"
        replacement.touch()
        sink.unlink()
        sink.symlink_to(replacement)
        with self.assertRaises(window.IncompleteObservation):
            window.daemon_delta(after, self.owned_snapshot())

    def test_missing_or_non_numeric_legacy_counter_is_not_zero(self):
        io = self.proc / "10" / "io"
        original = io.read_text()
        for text in [original.replace("read_bytes: 300\n", ""),
                     original.replace("read_bytes: 300", "read_bytes: nope"),
                     original + "read_bytes: 400\n"]:
            io.write_text(text)
            self.assertNotEqual(self.legacy_function("node_io").returncode, 0)

    def test_network_counter_values_keep_the_existing_non_loopback_scope(self):
        net = self.proc / "net"
        net.mkdir()
        def row(name, rx, tx):
            return f"{name}: {rx} 0 0 0 0 0 0 0 {tx} 0 0 0 0 0 0 0\n"
        body = "Inter-| Receive | Transmit\n face |bytes |bytes\n" + row("lo", 999, 999) + row("eth0", 100, 200) + row("eth1", 300, 400)
        (net / "dev").write_text(body)
        result = self.legacy_function("node_net")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "400 600")
        (net / "dev").write_text(body.replace("eth0: 100", "eth0: invalid"))
        self.assertNotEqual(self.legacy_function("node_net").returncode, 0)

    @unittest.skipUnless(sys.platform == "linux", "requires Linux proc and prctl")
    def test_real_owned_processes_have_exact_kernel_counter_and_identity_values(self):
        fixture = self.root / "parked_process.py"
        fixture.write_text(
            "import ctypes,os,signal,sys\n"
            "assert ctypes.CDLL(None).prctl(15,b'sinomo',0,0,0)==0\n"
            "os.write(int(sys.argv[-1]),b'R')\n"
            "os.close(int(sys.argv[-1]))\n"
            "signal.pause()\n"
        )
        children = []
        try:
            for role in ["main", "shadow"]:
                ready_read, ready_write = os.pipe()
                args = ["daemon", "run"] if role == "main" else ["daemon", "shadow", "run"]
                with (self.output / f"real-{role}.stdout.log").open("w") as stdout, \
                        (self.output / f"real-{role}.stderr.log").open("w") as stderr:
                    child = subprocess.Popen([sys.executable, str(fixture), *args, str(ready_write)],
                                             pass_fds=[ready_write], stdout=stdout, stderr=stderr)
                children.append((role, child))
                os.close(ready_write)
                try:
                    self.assertEqual(os.read(ready_read, 1), b"R")
                finally:
                    os.close(ready_read)
            # Restrict enumeration to these two owned processes; no unrelated
            # daemon is inspected, signalled, or included in the control.
            script = window.SNAPSHOT.replace(
                "/proc/[0-9]*/comm", " ".join(f"/proc/{child.pid}/comm" for _, child in children)
            )
            result = subprocess.run([str(self.bin / "sh"), "-c", script], env=self.environment,
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            with patch.object(window, "command", return_value=result.stdout.replace(str(self.output), "/data/daemon")):
                snapshot = window.daemon_snapshot("owned-kernel-fixture")
            for role, child in children:
                proc = Path("/proc") / str(child.pid)
                expected = {key.rstrip(":"): int(value) for key, value in
                            (line.split() for line in (proc / "io").read_text().splitlines())}
                self.assertEqual(snapshot["roles"][role]["io"], expected)
                start = (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]
                self.assertEqual(snapshot["roles"][role]["identity"][:2], [str(child.pid), start])
                self.assertIsNone(child.poll())
        finally:
            for _, child in children:
                if child.poll() is None:
                    child.terminate()
                child.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
