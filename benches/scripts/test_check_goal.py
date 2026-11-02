"""Regression tests for the Windows goal report validator."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_goal.py")
WINDOW_BYTES = 4 * 1048576


def bucket(seconds, windows):
    return {"seconds": seconds, "windows": windows,
            "unique_application_bytes": windows * WINDOW_BYTES,
            "complete_second": True}


def report():
    rounds = {"seconds": 2.0, "windows": 2000,
              "unique_application_bytes": 2000 * WINDOW_BYTES,
              "gb_per_second": 1000 * WINDOW_BYTES / 1e9,
              "error": None, "timeline_truncated": False,
              "timeline": [bucket(1.0, 1000), bucket(1.0, 1000)],
              "tx": {"aborted": 0, "unknown": 0, "crc_failures": 0},
              "rx": {"aborted": 0, "unknown": 0, "crc_failures": 0}}
    cells = []
    for mode in ("l2_slice", "l3_slice"):
        cells.append({"mode": mode, "d": 65507, "message": 1048576,
                      "processing_threads": 1, "window": 4, "batch_size": 32,
                      "channels": 1, "hold_first_ms": 0, "loss_ppm": 0,
                      "rtt_ms": 0, "rounds": [copy.deepcopy(rounds)]})
    return {"schema": 1, "platform": "x86_64-windows",
            "checksum": "CRC32C (default)", "verify_payload": True,
            "payload_variants": 4, "cells": cells}


class GoalValidatorTests(unittest.TestCase):
    def check(self, data):
        target = Path("target")
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(SCRIPT), str(path), "--rounds", "1",
                 "--duration-ms", "2000", "--require-timeline",
                 "--require-payload-verification", "--require-distinct-payloads"],
                capture_output=True, text=True, check=False)

    def test_valid_subwindows(self):
        result = self.check(report())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_extended_bucket_cannot_hide_a_stall(self):
        data = report()
        data["cells"][0]["rounds"][0]["timeline"] = [bucket(2.0, 2000)]
        result = self.check(data)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exceeds 1.1s", result.stderr)

    def test_missing_payload_verification_is_rejected(self):
        data = report()
        del data["verify_payload"]
        result = self.check(data)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("payload verification is not enabled", result.stderr)

    def test_missing_distinct_payloads_is_rejected(self):
        data = report()
        data["payload_variants"] = 1
        result = self.check(data)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("four distinct payloads are not enabled", result.stderr)


if __name__ == "__main__":
    unittest.main()
