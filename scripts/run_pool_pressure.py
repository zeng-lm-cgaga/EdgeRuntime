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
PROCESS_TIMEOUT_S = 420.0
SOURCE_SHA = "unknown"

CASES = [
    ("pool4k_4p4c", 4096, 512, 4, 4, 64, 20000, 3),
    ("pool4k_16p16c", 4096, 512, 16, 16, 64, 20000, 3),
    ("pool16k_4p4c", 16384, 1024, 4, 4, 64, 20000, 3),
    ("pool16k_16p16c", 16384, 1024, 16, 16, 64, 20000, 3),
    ("pool4k_smallpool_4p1c", 4096, 2, 4, 1, 8, 2000, 3),
    ("pool4k_smallqueue_4p1c", 4096, 128, 4, 1, 1, 20000, 3),
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

def run_case(name, block_size, block_count, producers, consumers, capacity, messages, runs):
    case_dir = RESULTS / name
    case_dir.mkdir(parents=True, exist_ok=False)
    out_dir = case_dir / "latency-csv"
    out_dir.mkdir()
    argv = [
        str(EXE),
        "--block-size", str(block_size),
        "--block-count", str(block_count),
        "--producers", str(producers),
        "--consumers", str(consumers),
        "--messages", str(messages),
        "--capacity", str(capacity),
        "--runs", str(runs),
        "--timeout-ms", "120000",
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
        "cwd": str(RUN_DIR),
        "argv": argv,
        "env_subset": {"LC_ALL": "C"},
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
            f"{name}: exit={outcome.returncode}, timeout={outcome.timed_out}, "
            f"orphan_group={outcome.orphan_group_reaped}, result_lines={len(rows)}"
        )
    for ordinal, row in enumerate(rows, 1):
        expected = str(messages * producers)
        required = {
            "expected": expected,
            "published": expected,
            "delivered": expected,
            "errors": "0",
            "recovery": "0",
            "invalid_payload": "0",
            "correctness": "PASS",
        }
        for key, expected_value in required.items():
            if row.get(key) != expected_value:
                raise RuntimeError(f"{name}/run{ordinal}: {key}={row.get(key)} expected={expected_value}")
        row.update({"case": name, "ordinal": str(ordinal)})
        rows[ordinal - 1] = row
    return rows

def main():
    parser = argparse.ArgumentParser(description="Run the bounded Release Pool pressure matrix")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--source-sha", default="unknown")
    parser.add_argument("--runs", type=int, default=None,
                        help="override all case run counts")
    parser.add_argument("--timeout-s", type=float, default=None,
                        help="bounded timeout for each driver process")
    args = parser.parse_args()
    if args.runs is not None and args.runs <= 0:
        parser.error("--runs must be positive")
    binary = args.binary.resolve()
    output_root = args.output_root.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"--binary is not an executable file: {binary}")
    if not output_root.is_dir() or str(output_root) == "/":
        parser.error("--output-root must be an existing non-root directory")
    global EXE, RUN_DIR, RESULTS, PROCESS_TIMEOUT_S, SOURCE_SHA, CASES
    EXE = binary
    SOURCE_SHA = args.source_sha
    RUN_DIR = Path(tempfile.mkdtemp(prefix="edgeruntime-pool-pressure-", dir=output_root))
    RESULTS = RUN_DIR / "pool-results"
    RESULTS.mkdir()
    if args.timeout_s is not None:
        if args.timeout_s <= 0:
            parser.error("--timeout-s must be positive")
        PROCESS_TIMEOUT_S = args.timeout_s
    if args.runs is not None:
        CASES = [(name, block_size, block_count, producers, consumers, capacity, messages,
                  args.runs)
                 for name, block_size, block_count, producers, consumers, capacity, messages, _
                 in CASES]
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
        "source_sha": SOURCE_SHA,
        "release_binary": str(EXE),
        "run_dir": str(RUN_DIR),
    }, indent=2) + "\n")
    print(f"POOL_PRESSURE_PASS cases={len(CASES)} result_rows={len(all_rows)} run_dir={RUN_DIR}")


if __name__ == "__main__":
    main()
