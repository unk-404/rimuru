"""Validate the Criterion-to-Benchcat conversion for both sampling modes."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("benchcat-comparison.py").resolve()


class ComparisonTests(unittest.TestCase):
    def run_conversion(self, mean):
        Path("target").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir="target") as tmp:
            target = Path(tmp).resolve()
            for run in (1, 2):
                for name in ("rimuru_l3", "quinn_stream"):
                    folder = target / "criterion" / f"ordered_comparison_pass{run}_t1_d1200_c1_w4" / name / "new"
                    folder.mkdir(parents=True)
                    (folder / "estimates.json").write_text(json.dumps({
                        "slope": None, "mean": {"point_estimate": mean}
                    }), encoding="utf-8")
            env = dict(os.environ, CARGO_TARGET_DIR=str(target))
            result = subprocess.run([sys.executable, str(SCRIPT)], env=env,
                                    capture_output=True, text=True, check=False)
            if result.returncode == 0:
                output = json.loads((target / "benchcat-rimuru_l3.json").read_text(encoding="utf-8"))
                self.assertEqual(len(output["benchmarks"]), 2)
                self.assertEqual(output["benchmarks"][0]["value"], 4 * 1048576 * 1e9 / mean)
            return result

    def test_flat_sampling_without_slope(self):
        result = self.run_conversion(20_000_000)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_mean_fails(self):
        result = self.run_conversion(0)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid Criterion mean", result.stderr)


if __name__ == "__main__":
    unittest.main()
