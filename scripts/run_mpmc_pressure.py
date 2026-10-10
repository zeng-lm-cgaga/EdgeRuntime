#!/usr/bin/env python3
import argparse
import csv
import json
import os
import shlex
import tempfile
import time
from pathlib import Path

from pressure_process import run_logged_process

RUN_DIR = Path()
EXE = Path()
RESULTS = Path()
MESSAGES = 20000
PROCESS_TIMEOUT_S = 420.0
SOURCE_SHA = "unknown"

BASE = [
    ("mpmc64_1p1c", 64, 1, 1),
    ("mpmc64_1p4c", 64, 1, 4),
    ("mpmc64_4p1c", 64, 4, 1),
    ("mpmc64_4p4c", 64, 4, 4),
    ("mpmc64_8p8c", 64, 8, 8),
    ("mpmc64_16p16c", 64, 16, 16),
]
REPRESENTATIVE_4K = [
    ("mpmc4k_1p4c", 4096, 1, 4),
    ("mpmc4k_4p1c", 4096, 4, 1),
    ("mpmc4k_8p8c", 4096, 8, 8),
]
CASES = [
    (name, payload, producers, consumers, "busy-yield", 3)
    for name, payload, producers, consumers in BASE
] + [
    (name, payload, producers, consumers, "blocking", 3)
    for name, payload, producers, consumers in BASE
] + [
    (name, payload, producers, consumers, "busy-yield", 2)
    for name, payload, producers, consumers in REPRESENTATIVE_4K
] + [
    (name, payload, producers, consumers, "blocking", 2)
    for name, payload, producers, consumers in REPRESENTATIVE_4K
] + [
    ("mpmc64_capacity1_4p1c", 64, 4, 1, "busy-yield", 3),
]

def parse_results(stdout):
    rows = []
    for line in stdout.splitlines():
        if not line.startswith("RESULT "):
            continue
        fields = {}
        for item in shlex.split(line[len("RESULT "):]):
            if "=" in item:
                key, value = item.split("=", 1)
                fields[key] = value
        rows.append(fields)
    return rows

def run_case(name, payload, producers, consumers, mode, runs):
    case_dir = RESULTS / f"{name}-{mode}"
    case_dir.mkdir(parents=True, exist_ok=False)
    out_dir = case_dir / "latency-csv"
    out_dir.mkdir()
    wait_policy = "blocking" if mode == "blocking" else "busy"
    argv = [
        str(EXE),
        "--payload-size", str(payload),
        "--producers", str(producers),
        "--consumers", str(consumers),
        "--messages", str(MESSAGES),
        "--capacity", "64" if "capacity1" not in name else "1",
        "--runs", str(runs),
        "--timeout-ms", "120000",
        "--retry-policy", "yield",
        "--wait-policy", wait_policy,
        "--out-dir", str(out_dir),
        "--ready-file", str(case_dir / "ready.txt"),
    ]
    env = os.environ.copy()
    for key in ("EDGE_FAILPOINT", "EDGE_FAILPOINT_MODE", "EDGE_FAILPOINT_COUNT",
                "ASAN_OPTIONS", "UBSAN_OPTIONS", "LD_PRELOAD"):
        env.pop(key, None)
    env["LC_ALL"] = "C"
    invocation = {
        "case": name,
        "mode": mode,
        "cwd": str(RUN_DIR),
        "argv": argv,
        "env_subset": {key: env[key] for key in ("LC_ALL",) if key in env},
        "source_sha": SOURCE_SHA,
        "binary": str(EXE),
        "start_epoch_ns": time.time_ns(),
    }
    outcome = run_logged_process(
        argv,
        cwd=RUN_DIR,
        env=env,
        stdout_path=case_dir / "stdout.raw",
        stderr_path=case_dir / "stderr.raw",
        sample_path=case_dir / "process-samples.jsonl",
        timeout_s=PROCESS_TIMEOUT_S,
        ready_path=case_dir / "ready.txt",
        ready_roles={"READY"},
        startup_timeout_s=30.0,
    )
    invocation["end_epoch_ns"] = outcome.end_epoch_ns
    invocation["returncode"] = outcome.returncode
    invocation["parent_pid"] = outcome.observed_child_pids[0]
    invocation["observed_child_pids"] = outcome.observed_child_pids[1:]
    invocation["timed_out"] = outcome.timed_out
    invocation["term_sent"] = outcome.term_sent
    invocation["kill_sent"] = outcome.kill_sent
    invocation["orphan_group_reaped"] = outcome.orphan_group_reaped
    invocation["expected_children_per_internal_run"] = producers + consumers
    (case_dir / "invocation.json").write_text(json.dumps(invocation, indent=2) + "\n")
    stdout = (case_dir / "stdout.raw").read_text()
    rows = parse_results(stdout)
    if (outcome.returncode != 0 or outcome.timed_out or outcome.orphan_group_reaped or
            len(rows) != runs):
        raise RuntimeError(
            f"{name}/{mode}: exit={outcome.returncode}, timeout={outcome.timed_out}, "
            f"orphan_group={outcome.orphan_group_reaped}, result_lines={len(rows)}"
        )
    for ordinal, row in enumerate(rows, 1):
        required = {
            "expected": str(MESSAGES * producers),
            "published": str(MESSAGES * producers),
            "delivered": str(MESSAGES * producers),
            "errors": "0",
            "recovery": "0",
            "correctness": "PASS",
        }
        for key, expected in required.items():
            if row.get(key) != expected:
                raise RuntimeError(f"{name}/{mode}/run{ordinal}: {key}={row.get(key)} expected={expected}")
        row.update({"case": name, "mode": mode, "ordinal": str(ordinal)})
        rows[ordinal - 1] = row
    return rows

def main():
    parser = argparse.ArgumentParser(description="Run the bounded Release MPMC pressure matrix")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--source-sha", default="unknown")
    parser.add_argument("--messages", type=int, default=20000)
    parser.add_argument("--runs", type=int, default=None,
                        help="override all case run counts")
    parser.add_argument("--timeout-s", type=float, default=None,
                        help="bounded timeout for each driver process")
    args = parser.parse_args()
    if args.messages <= 0 or (args.runs is not None and args.runs <= 0):
        parser.error("--messages and --runs must be positive")
    binary = args.binary.resolve()
    output_root = args.output_root.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"--binary is not an executable file: {binary}")
    if not output_root.is_dir() or str(output_root) == "/":
        parser.error("--output-root must be an existing non-root directory")
    global EXE, RUN_DIR, RESULTS, MESSAGES, PROCESS_TIMEOUT_S, SOURCE_SHA, CASES
    EXE = binary
    MESSAGES = args.messages
    SOURCE_SHA = args.source_sha
    RUN_DIR = Path(tempfile.mkdtemp(prefix="edgeruntime-mpmc-pressure-", dir=output_root))
    RESULTS = RUN_DIR / "mpmc-results"
    RESULTS.mkdir()
    if args.timeout_s is not None:
        if args.timeout_s <= 0:
            parser.error("--timeout-s must be positive")
        PROCESS_TIMEOUT_S = args.timeout_s
    if args.runs is not None:
        CASES = [(name, payload, producers, consumers, mode, args.runs)
                 for name, payload, producers, consumers, mode, _ in CASES]

    all_rows = []
    for case in CASES:
        all_rows.extend(run_case(*case))
    keys = sorted({key for row in all_rows for key in row})
    with (RESULTS / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
    (RESULTS / "driver-summary.json").write_text(json.dumps({
        "cases": len(CASES),
        "result_rows": len(all_rows),
        "messages_per_producer": MESSAGES,
        "source_sha": SOURCE_SHA,
        "release_binary": str(EXE),
        "run_dir": str(RUN_DIR),
    }, indent=2) + "\n")
    print(f"MPMC_PRESSURE_PASS cases={len(CASES)} result_rows={len(all_rows)} run_dir={RUN_DIR}")


if __name__ == "__main__":
    main()
