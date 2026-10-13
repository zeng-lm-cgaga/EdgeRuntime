#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2]
CMAKE = "cmake"


class CmakeOptionsTest(unittest.TestCase):
    def configure(self, directory, *options, success=True):
        command = [CMAKE, "-S", str(SOURCE), "-B", str(directory),
                   "-DBUILD_TESTING=OFF", "-DCMAKE_BUILD_TYPE=Debug",
                   "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON", *options]
        result = subprocess.run(command,
                                capture_output=True, text=True, timeout=45)
        print(json.dumps({"argv": command, "exit": result.returncode,
                          "stdout": result.stdout, "stderr": result.stderr}), flush=True)
        self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
        if not success:
            self.assertIn("Conflicting options", result.stderr)
            return
        cache = (directory / "CMakeCache.txt").read_text()
        for name in ("WARNINGS_AS_ERRORS", "ENABLE_SANITIZERS", "ENABLE_FAILPOINTS"):
            values = [line.split("=", 1)[1] for line in cache.splitlines()
                      if line.startswith((f"SELFIPCS_{name}:BOOL=", f"EDGERUNTIME_{name}:BOOL="))]
            self.assertEqual(len(values), 2)
            self.assertEqual(values[0], values[1])
        commands = json.loads((directory / "compile_commands.json").read_text())
        for entry in commands:
            for name, flag in (("ENABLE_SANITIZERS", "-fsanitize=address,undefined"),
                               ("ENABLE_FAILPOINTS", "-DEDGERUNTIME_ENABLE_FAILPOINTS=1")):
                enabled = f"SELFIPCS_{name}:BOOL=ON" in cache.splitlines()
                self.assertEqual(flag in entry["command"], enabled)

    def test_new_and_legacy_switches_and_reconfiguration(self):
        for prefix in ("SELFIPCS", "EDGERUNTIME"):
            with self.subTest(prefix=prefix), tempfile.TemporaryDirectory(prefix="selfipcs-options-") as root:
                directory = Path(root)
                self.configure(directory, f"-D{prefix}_ENABLE_SANITIZERS=ON",
                               f"-D{prefix}_ENABLE_FAILPOINTS=OFF")
                self.configure(directory, f"-D{prefix}_ENABLE_SANITIZERS=OFF",
                               f"-D{prefix}_ENABLE_FAILPOINTS=ON")
                other = "EDGERUNTIME" if prefix == "SELFIPCS" else "SELFIPCS"
                self.configure(directory, f"-D{other}_ENABLE_SANITIZERS=ON",
                               f"-D{other}_ENABLE_FAILPOINTS=OFF")

    def test_conflicting_explicit_configuration_fails(self):
        with tempfile.TemporaryDirectory(prefix="selfipcs-conflict-") as root:
            self.configure(Path(root), "-DSELFIPCS_ENABLE_SANITIZERS=ON",
                           "-DEDGERUNTIME_ENABLE_SANITIZERS=OFF", success=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmake", default="cmake")
    arguments, remaining = parser.parse_known_args()
    CMAKE = arguments.cmake
    unittest.main(argv=[__file__, *remaining])
