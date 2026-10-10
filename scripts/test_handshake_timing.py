#!/usr/bin/env python3
"""Verify handshake overlap and full-window duration from independent child reports."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import tempfile

from pressure_process import run_logged_process


CHILD_RE = re.compile(r"^CHILD .* start_ns=(\d+) finish_ns=(\d+)")


def fields(line: str) -> dict[str, str]:
    return {
        item.split("=", 1)[0]: item.split("=", 1)[1]
        for item in line.split()
        if "=" in item
    }


def check_helper(binary: Path, name: str, args: list[str], root: Path) -> dict[str, object]:
    case_dir = root / name
    case_dir.mkdir()
    output = case_dir / "stdout.raw"
    result = run_logged_process(
        [str(binary), *args],
        cwd=case_dir,
        env=os.environ.copy(),
        stdout_path=output,
        stderr_path=case_dir / "stderr.raw",
        sample_path=case_dir / "process-samples.jsonl",
        timeout_s=30.0,
    )
    lines = output.read_text(encoding="utf-8").splitlines()
    children = [CHILD_RE.match(line) for line in lines if line.startswith("CHILD ")]
    intervals = [(int(match.group(1)), int(match.group(2))) for match in children if match]
    go_line = next(line for line in lines if line.startswith("GO_SENT "))
    go_ns = int(fields(go_line)["ns"])
    result_line = next(line for line in lines if line.startswith("RESULT "))
    result_fields = fields(result_line)
    finish_min = min(finish for _, finish in intervals)
    finish_max = max(finish for _, finish in intervals)
    expected_duration_us = (finish_max - go_ns) // 1000
    measured_duration_us = int(result_fields["duration_us"])
    if (result.returncode != 0 or result.timed_out or len(intervals) < 2 or
            finish_max <= finish_min or finish_max - finish_min < 1_000_000 or
            result_fields.get("overlap") != "PASS" or
            measured_duration_us != expected_duration_us or
            result_fields.get("correctness") != "PASS"):
        raise RuntimeError(
            f"{name}: exit={result.returncode}, intervals={intervals}, "
            f"duration={measured_duration_us}, expected={expected_duration_us}, "
            f"result={result_fields}"
        )
    return {
        "name": name,
        "returncode": result.returncode,
        "go_ns": go_ns,
        "finish_min_ns": finish_min,
        "finish_max_ns": finish_max,
        "finish_spread_ns": finish_max - finish_min,
        "duration_us": measured_duration_us,
        "result": result_fields,
        "cleanup_lines": [line for line in lines if line.startswith("CLEANUP ")],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mpmc-binary", type=Path, required=True)
    parser.add_argument("--pool-binary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    args = parser.parse_args()
    mpmc = args.mpmc_binary.resolve()
    pool = args.pool_binary.resolve()
    root_parent = args.output_root.resolve()
    if (not mpmc.is_file() or not os.access(mpmc, os.X_OK) or
            not pool.is_file() or not os.access(pool, os.X_OK)):
        parser.error("both helper binaries must be executable files")
    if not root_parent.is_dir() or str(root_parent) == "/":
        parser.error("--output-root must be an existing non-root directory")
    root = Path(tempfile.mkdtemp(prefix="edgeruntime-handshake-timing-", dir=root_parent))
    cases = [
        check_helper(
            mpmc,
            "mpmc",
            ["--producers", "2", "--consumers", "1", "--messages", "8", "--capacity", "2",
             "--delay-us", "20000", "--wait-policy", "blocking", "--timeout-ms", "10000"],
            root,
        ),
        check_helper(
            pool,
            "pool",
            ["--block-size", "4096", "--block-count", "16", "--producers", "2",
             "--consumers", "2", "--messages", "8", "--capacity", "2", "--delay-us", "10000",
             "--timeout-ms", "10000"],
            root,
        ),
    ]
    pool_cleanup = cases[1]["cleanup_lines"]
    if len(pool_cleanup) != 1:
        raise RuntimeError(f"pool cleanup output is not one line: {pool_cleanup}")
    report = {"root": str(root), "cases": cases}
    (root / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
