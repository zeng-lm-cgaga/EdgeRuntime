#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / "scripts"))
from self_ipcs_compat import build_target

CMAKE = "cmake"
BUILD_TYPE = "Release"
SANITIZERS = "OFF"
FAILPOINTS = "OFF"
WORK_DIRECTORY = None


class CmakeBuildTargetsTest(unittest.TestCase):
    def run_command(self, *command):
        environment = dict(os.environ)
        environment["ASAN_OPTIONS"] = "detect_leaks=0:abort_on_error=1"
        environment["UBSAN_OPTIONS"] = "halt_on_error=1:print_stacktrace=1"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, env=environment, start_new_session=True)
        try:
            output, _ = process.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                output, _ = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                output, _ = process.communicate()
            self.fail(f"command timed out: {command}\n{output}")
        print(json.dumps({"argv": command, "exit": process.returncode,
                          "output": output}), flush=True)
        self.assertEqual(process.returncode, 0, output)

    def configure(self, source, build, *options):
        self.run_command(CMAKE, "-S", str(source), "-B", str(build), "-G", "Unix Makefiles",
                         f"-DCMAKE_BUILD_TYPE={BUILD_TYPE}", "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
                         "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF",
                         "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF", *options)

    def write_consumers(self, directory, setup, targets):
        directory.mkdir()
        (directory / "main.cpp").write_text(
            '#include <cstring>\n#include <edge_runtime/error.hpp>\n'
            '#include <self_ipcs/common/error.hpp>\n'
            'int main() {\n'
            '  return std::strcmp(self_ipcs::to_string(self_ipcs::ErrorCode::kTimeout), "Timeout")'
            ' || std::strcmp(edge_runtime::to_string(edge_runtime::ErrorCode::kTimeout), "Timeout");\n'
            '}\n')
        definitions = []
        for index, target in enumerate(targets):
            name = f"consumer_{index}"
            definitions.extend([f"add_executable({name} main.cpp)",
                                f"target_link_libraries({name} PRIVATE {target})"])
            if SANITIZERS == "ON":
                definitions.extend([
                    f"target_compile_options({name} PRIVATE -fsanitize=address,undefined)",
                    f"target_link_options({name} PRIVATE -fsanitize=address,undefined)"])
        (directory / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.22)\nproject(target_consumers LANGUAGES CXX)\n"
            + setup + "\n" + "\n".join(definitions) + "\n")

    def check_consumers(self, build, count, archive):
        for index in range(count):
            link = (build / "CMakeFiles" / f"consumer_{index}.dir" / "link.txt").read_text()
            self.assertEqual(link.count("libself_ipcs.a"), 1, link)
            self.assertNotIn("libedge_runtime.a", link)
            self.assertIn(str(archive), link)
            self.run_command(str(build / f"consumer_{index}"))

    def check_compatibility(self, root):
        source = root / "source"
        build = root / "build"
        self.write_consumers(source, f'add_subdirectory("{SOURCE}" runtime)',
                             ("self_ipcs", "edge_runtime", "SelfIPCs::self_ipcs",
                              "EdgeRuntime::edge_runtime"))
        self.configure(source, build, "-DBUILD_TESTING=OFF",
                       f"-DSELFIPCS_ENABLE_SANITIZERS={SANITIZERS}",
                       f"-DSELFIPCS_ENABLE_FAILPOINTS={FAILPOINTS}")
        runtime = build / "runtime"
        self.run_command(CMAKE, "--build", str(build), "--target", "edge_runtime", "--parallel", "2")
        archive = runtime / "src" / "libself_ipcs.a"
        self.assertTrue(archive.is_file())
        objects = sorted((runtime / "src").rglob("*.o"))
        before = {path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
                  for path in [archive, *objects]}
        self.run_command(CMAKE, "--build", str(build), "--target", "self_ipcs", "--parallel", "2")
        self.assertEqual(before, {path: (path.stat().st_mtime_ns,
                                        hashlib.sha256(path.read_bytes()).hexdigest())
                                  for path in before})
        self.assertEqual(sorted(runtime.rglob("*.a")), [archive])
        self.assertEqual(len(objects), len(list((SOURCE / "src" / "edge_runtime").rglob("*.cpp"))))
        commands = json.loads((build / "compile_commands.json").read_text())
        core = [entry for entry in commands
                if Path(entry["file"]).is_relative_to(SOURCE / "src" / "edge_runtime")]
        self.assertEqual(len(core), len(objects))
        self.assertEqual(len({entry["file"] for entry in core}), len(core))
        for entry in core:
            self.assertEqual("-fsanitize=address,undefined" in entry["command"], SANITIZERS == "ON")
            self.assertEqual("-DEDGERUNTIME_ENABLE_FAILPOINTS=1" in entry["command"], FAILPOINTS == "ON")
        directories = (build / "CMakeFiles" / "TargetDirectories.txt").read_text()
        for target in ("self_ipcs", "edge_runtime"):
            self.assertIn(f"/{target}.dir", directories)
            self.assertEqual(build_target(build, target), "self_ipcs")
        self.run_command(CMAKE, "--build", str(build), "--target",
                         *(f"consumer_{index}" for index in range(4)), "--parallel", "2")
        self.check_consumers(build, 4, Path("runtime/src/libself_ipcs.a"))

        prefix = root / "install"
        for directory in (runtime / "src", runtime):
            self.run_command(CMAKE, f"-DCMAKE_INSTALL_PREFIX={prefix}",
                             f"-DCMAKE_INSTALL_CONFIG_NAME={BUILD_TYPE}",
                             "-DCMAKE_INSTALL_LOCAL_ONLY=1", "-P", str(directory / "cmake_install.cmake"))
        installed_archive = next(prefix.rglob("libself_ipcs.a"))
        self.assertEqual(hashlib.sha256(installed_archive.read_bytes()).hexdigest(), before[archive][1])
        self.assertEqual(sorted(prefix.rglob("*.a")), [installed_archive])
        self.assertFalse(list((prefix / "include").rglob("detail")))
        for configuration in prefix.rglob("*.cmake"):
            self.assertNotIn(str(SOURCE), configuration.read_text())
            self.assertNotIn(str(build), configuration.read_text())
        for index, (packages, targets) in enumerate((
                (("SelfIPCs",), ("SelfIPCs::self_ipcs",)),
                (("EdgeRuntime",), ("EdgeRuntime::edge_runtime",)),
                (("SelfIPCs", "EdgeRuntime"), ("SelfIPCs::self_ipcs", "EdgeRuntime::edge_runtime")),
                (("EdgeRuntime", "SelfIPCs"), ("EdgeRuntime::edge_runtime", "SelfIPCs::self_ipcs")))):
            package_source = root / f"package-source-{index}"
            package_build = root / f"package-build-{index}"
            setup = "\n".join(f"find_package({package} CONFIG REQUIRED)" for package in packages)
            setup += '\nif(NOT TARGET Threads::Threads)\nmessage(FATAL_ERROR "Threads missing")\nendif()'
            self.write_consumers(package_source, setup, targets)
            self.configure(package_source, package_build, f"-DCMAKE_PREFIX_PATH={prefix}")
            self.run_command(CMAKE, "--build", str(package_build), "--parallel", "2")
            self.check_consumers(package_build, len(targets), installed_archive)
            for entry in json.loads((package_build / "compile_commands.json").read_text()):
                self.assertNotIn(str(SOURCE / "include"), entry["command"])
                self.assertIn(str(prefix / "include"), entry["command"])

    def test_build_and_link_compatibility(self):
        if WORK_DIRECTORY is not None:
            WORK_DIRECTORY.mkdir(parents=True, exist_ok=False)
            self.check_compatibility(WORK_DIRECTORY)
        else:
            with tempfile.TemporaryDirectory(prefix="selfipcs-build-targets-") as directory:
                self.check_compatibility(Path(directory))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmake", default="cmake")
    parser.add_argument("--build-type", choices=("Debug", "Release", "RelWithDebInfo", "MinSizeRel"),
                        default="Release")
    parser.add_argument("--sanitizers", choices=("ON", "OFF"), default="OFF")
    parser.add_argument("--failpoints", choices=("ON", "OFF"), default="OFF")
    parser.add_argument("--work-dir", type=Path, default=os.environ.get("SELFIPCS_COMPAT_WORK_DIR"))
    arguments, remaining = parser.parse_known_args()
    CMAKE = arguments.cmake
    BUILD_TYPE = arguments.build_type
    SANITIZERS = arguments.sanitizers
    FAILPOINTS = arguments.failpoints
    WORK_DIRECTORY = arguments.work_dir
    unittest.main(argv=[__file__, *remaining])
