#!/usr/bin/env python3
"""Serial, bounded fork/exec IPC measurements with immutable per-run evidence."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

from pressure_process import run_logged_process


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def save_json(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")


def message_budget(planned: list[dict]) -> dict:
    totals = [case.get("producers", 1) * case.get("messages", 2000) for case in planned]
    if any(total > 32000000 for total in totals):
        raise ValueError("per-case total must not exceed 32,000,000 messages")
    return {"total_messages": sum(totals), "max_case_messages": max(totals),
            "estimated_delivery_evidence_bytes": sum(totals) * 80,
            "max_validation_memory_bytes": max(totals) * 64}


def command_output(argv: list[str], cwd: Path) -> str:
    return subprocess.check_output(argv, cwd=cwd, text=True, stderr=subprocess.STDOUT)


def cases(section: str, messages: int) -> list[dict]:
    result = []
    if section == "spsc":
        for placement in ("same", "different"):
            for repeat in range(1, 4):
                result.append({"kind": "spsc", "placement": placement, "repeat": repeat})
    elif section == "mpmc":
        for payload, producers, consumers in ((64, 1, 1), (64, 4, 4), (64, 16, 16),
                                               (4096, 4, 4)):
            for wait in ("busy", "blocking"):
                for repeat in range(1, 4):
                    result.append({"kind": "mpmc", "payload": payload,
                                   "producers": producers, "consumers": consumers,
                                   "wait": wait, "messages": 20000, "repeat": repeat})
        for producers, consumers in ((1, 4), (4, 1)):
            result.append({"kind": "mpmc", "payload": 64, "producers": producers,
                           "consumers": consumers, "wait": "busy", "messages": 20000,
                           "repeat": 1, "supplemental": True})
    elif section == "pool":
        for payload in (4096, 16384):
            for workers in (4, 16):
                for repeat in range(1, 4):
                    result.append({"kind": "pool", "payload": payload, "producers": workers,
                                   "consumers": workers, "messages": 20000, "blocks": 128,
                                   "capacity": 64, "repeat": repeat})
        result.append({"kind": "pool", "payload": 4096, "producers": 4, "consumers": 1,
                       "messages": 2000, "blocks": 4, "capacity": 2, "hold_ms": 200,
                       "repeat": 1, "supplemental": True})
    else:
        result.append({"kind": "mpmc", "payload": 64, "producers": 16, "consumers": 16,
                       "wait": "busy", "messages": messages, "repeat": 1,
                       "supplemental": True})
    return result


def invocation(case: dict, binary_dir: Path, output: Path, cpu_a: int, cpu_b: int,
               repo: Path) -> list[str]:
    if case["kind"] == "spsc":
        placement = case["placement"]
        return [str(binary_dir / "edge_shm_bench"), "--evidence", "--only",
                f"shm-futex-64B-{placement}-100-controlled,"
                f"socket-64B-{placement}-100-controlled",
                "--producer", str(binary_dir / "edge_shm_bench_producer"),
                "--consumer", str(binary_dir / "edge_shm_bench_consumer"),
                "--sock", str(binary_dir / "edge_shm_bench_sock"),
                "--ctl", str(binary_dir / "edge_shm_ctl"), "--cpu-a", str(cpu_a),
                "--cpu-b", str(cpu_b), "--git-dir", str(repo), "--out-dir", str(output)]
    tool = "edge_mpmc_bench" if case["kind"] == "mpmc" else "edge_buffer_pool_bench"
    argv = [str(binary_dir / tool), "--producers", str(case["producers"]),
            "--consumers", str(case["consumers"]), "--messages", str(case["messages"]),
            "--capacity", str(case.get("capacity", 64)), "--runs", "1",
            "--timeout-ms", "110000", "--out-dir", str(output), "--keep-worker-reports",
            "--ready-file", str(output.parent / "ready.txt")]
    if case["kind"] == "mpmc":
        argv += ["--payload-size", str(case["payload"]), "--retry-policy", "yield",
                 "--wait-policy", case["wait"]]
    else:
        argv += ["--block-size", str(case["payload"]), "--block-count", str(case["blocks"]),
                 "--test-consumer-hold-ms", str(case.get("hold_ms", 0))]
    return argv


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--section", choices=("spsc", "mpmc", "pool", "calibrate", "sustained"),
                        required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--binary-dir", type=Path)
    parser.add_argument("--messages", type=int, default=100000,
                        help="messages per producer for calibrate/sustained (max 2,000,000)")
    parser.add_argument("--cpu-a", type=int, default=0)
    parser.add_argument("--cpu-b", type=int, default=1)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    binary_dir = (args.binary_dir or repo / "build/release/tools").resolve()
    output_root = args.output_root.resolve()
    if not output_root.is_dir() or str(output_root) == "/":
        parser.error("--output-root must be an existing non-root directory")
    if not 1 <= args.messages <= 2000000:
        parser.error("--messages must be in 1..2000000")
    if args.cpu_a == args.cpu_b or not {args.cpu_a, args.cpu_b} <= os.sched_getaffinity(0):
        parser.error("choose two distinct available CPUs")
    cache = binary_dir.parent / "CMakeCache.txt"
    cache_text = cache.read_text()
    for setting in ("CMAKE_BUILD_TYPE:STRING=Release", "CMAKE_CXX_FLAGS_RELEASE:STRING=-O3 -DNDEBUG",
                    "EDGERUNTIME_ENABLE_SANITIZERS:BOOL=OFF", "EDGERUNTIME_ENABLE_FAILPOINTS:BOOL=OFF"):
        if setting not in cache_text.splitlines():
            parser.error(f"Release configuration required: {setting}")
    if shutil.disk_usage(output_root).free < 4 * 1024**3:
        parser.error("at least 4 GiB free disk required")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    root = output_root / f"{args.section}-{timestamp}-{os.getpid()}"
    root.mkdir()
    planned = cases(args.section, args.messages)
    save_json(root / "planned.json", {"section": args.section, "cases": planned,
                                      "budget": message_budget(planned)})
    source = {"head": command_output(["git", "rev-parse", "HEAD"], repo).strip(),
              "status": command_output(["git", "status", "--porcelain=v1"], repo),
              "environment": "VM_ONLY", "cpu_mask": sorted(os.sched_getaffinity(0)),
              "cpu_a": args.cpu_a, "cpu_b": args.cpu_b, "cwd": str(repo),
              "cache_sha256": digest(cache)}
    archive = binary_dir.parent / "src/libedge_runtime.a"
    source["core_archive"] = {"path": str(archive), "sha256": digest(archive)}
    (root / "source.patch").write_text(command_output(["git", "diff", "--binary", "HEAD"], repo))
    shutil.copy2(cache, root / "CMakeCache.txt")
    shutil.copy2(binary_dir.parent / "compile_commands.json", root / "compile_commands.json")
    source["binaries"] = {str(path): digest(path) for path in sorted(binary_dir.glob("edge_*"))
                          if path.is_file() and os.access(path, os.X_OK)}
    for label, argv in (("uname", ["uname", "-a"]), ("cpu", ["lscpu"]),
                        ("compiler", ["g++", "--version"]), ("cmake", ["cmake", "--version"]),
                        ("processes", ["ps", "-eo", "pid,ppid,pgid,pcpu,args", "--sort=-pcpu"])):
        (root / f"{label}.txt").write_text(command_output(argv, repo))
    save_json(root / "manifest.json", source)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("EDGE_FAILPOINT") or key in ("LD_PRELOAD", "ASAN_OPTIONS", "UBSAN_OPTIONS"):
            env.pop(key)
    env["LC_ALL"] = "C"
    outcomes = []
    print(f"evidence={root}", flush=True)
    for ordinal, case in enumerate(planned, 1):
        case_dir = root / f"case-{ordinal:02d}"
        case_dir.mkdir()
        output = case_dir / "output"
        argv = invocation(case, binary_dir, output, args.cpu_a, args.cpu_b, repo)
        metadata = {"case": case, "argv": argv, "source": source, "start_epoch_ns": time.time_ns(),
                    "section": args.section, "watchdog_s": 120, "environment": {"LC_ALL": "C"}}
        save_json(case_dir / "command.json", metadata)
        try:
            result = run_logged_process(argv, cwd=repo, env=env,
                                        stdout_path=case_dir / "stdout.raw",
                                        stderr_path=case_dir / "stderr.raw",
                                        sample_path=case_dir / "process-samples.jsonl", timeout_s=120,
                                        capture_affinity=case["kind"] == "spsc")
        except BaseException as error:
            failure = {"returncode": None, "error": repr(error), "traceback": traceback.format_exc(),
                       "end_epoch_ns": time.time_ns(), "cleanup_unverified": True}
            save_json(case_dir / "exit.json", failure)
            outcomes.append({"ordinal": ordinal, "case": case, "success": False, **failure})
            save_json(root / "outcomes.json", outcomes)
            raise
        save_json(case_dir / "exit.json", asdict(result))
        success = result.returncode == 0 and not result.timed_out and not result.orphan_group_reaped
        outcomes.append({"ordinal": ordinal, "case": case, "success": success, **asdict(result)})
        print(f"case={ordinal} exit={result.returncode} timeout={result.timed_out} config={case}",
              flush=True)
    save_json(root / "outcomes.json", outcomes)
    return 0 if all(result["success"] for result in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
