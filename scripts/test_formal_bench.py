#!/usr/bin/env python3
"""Focused regression tests for the formal matrix and independent CSV verifier."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from pressure_process import run_logged_process
from run_formal_bench import cases, digest, message_budget
from verify_formal_bench import check_delivery, check_plans, quantiles


BINARY_DIR = None
OUTPUT_ROOT = Path("/tmp")


class FormalBenchTest(unittest.TestCase):
    def test_finite_matrix(self):
        self.assertEqual(len(cases("spsc", 1)), 6)
        matrix = cases("mpmc", 1)
        self.assertEqual(len(matrix), 26)
        self.assertTrue(all(case["messages"] == 20000 for case in matrix))
        self.assertEqual(len(cases("pool", 1)), 13)
        self.assertEqual(cases("sustained", 123)[0]["messages"], 123)
        self.assertEqual(message_budget(cases("mpmc", 1))["total_messages"], 3100000)
        self.assertEqual(message_budget(cases("pool", 1))["total_messages"], 2408000)
        with self.assertRaises(ValueError):
            message_budget(cases("sustained", 2000001))

    def test_missing_planned_cases_fail(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            (root / "planned.json").write_text(json.dumps({"section": "spsc", "cases": cases("spsc", 1)}))
            checks = check_plans(root)
            self.assertTrue(checks[0]["errors"])

    def test_nearest_rank(self):
        self.assertEqual(quantiles([100, 1, 50, 2]),
                         {"p50_ns": 2, "p95_ns": 100, "p99_ns": 100, "max_ns": 100})
        with self.assertRaises(ValueError):
            quantiles([])

    def test_sha256_on_python310(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            path = Path(directory) / "input"
            path.write_bytes(b"formal benchmark")
            self.assertEqual(digest(path), hashlib.sha256(b"formal benchmark").hexdigest())

    def test_worker_retention_and_independent_negative_checks(self):
        if BINARY_DIR is None:
            self.skipTest("--binary-dir enables native fork/exec coverage")
        root = Path(tempfile.mkdtemp(prefix="edgeruntime-formal-regression-", dir=OUTPUT_ROOT))
        for kind, tool, arguments in (
                ("mpmc", "edge_mpmc_bench", ["--payload-size", "64"]),
                ("pool", "edge_buffer_pool_bench", ["--block-size", "4096", "--block-count", "4"])):
            case_dir = root / kind
            case_dir.mkdir()
            output = case_dir / "output"
            argv = [str(BINARY_DIR / tool), *arguments, "--producers", "2", "--consumers", "2",
                    "--messages", "32", "--capacity", "2", "--timeout-ms", "5000",
                    "--out-dir", str(output), "--keep-worker-reports"]
            result = run_logged_process(argv, cwd=case_dir, env=os.environ.copy(),
                                        stdout_path=case_dir / "stdout.raw",
                                        stderr_path=case_dir / "stderr.raw",
                                        sample_path=case_dir / "process-samples.jsonl", timeout_s=15)
            (case_dir / "exit.json").write_text(json.dumps(asdict(result)))
            case = {"kind": kind, "payload": 64 if kind == "mpmc" else 4096,
                    "producers": 2, "consumers": 2, "messages": 32, "blocks": 4,
                    "wait": "busy", "capacity": 2}
            verified = check_delivery(case_dir, case)
            self.assertTrue(verified["valid"])
            self.assertEqual(len(verified["workers"]), 4)
            original = Path(verified["csv"]).read_text()
            lines = original.splitlines()
            lines[2] = lines[1]
            Path(verified["csv"]).write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                check_delivery(case_dir, case)
            Path(verified["csv"]).write_text(original)
            worker = Path(verified["workers"][0]["path"])
            original_worker = worker.read_bytes()
            worker.write_bytes(original_worker[:-1])
            with self.assertRaisesRegex(ValueError, "invalid worker report"):
                check_delivery(case_dir, case)
            worker.write_bytes(original_worker)
            wrong_case = dict(case, payload=16384 if kind == "pool" else 4096)
            with self.assertRaisesRegex(ValueError, "configuration"):
                check_delivery(case_dir, wrong_case)
        print(f"native regression evidence={root}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary-dir", type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    args, remaining = parser.parse_known_args()
    BINARY_DIR = args.binary_dir.resolve() if args.binary_dir else None
    OUTPUT_ROOT = args.output_root.resolve()
    unittest.main(argv=[__file__, *remaining])
