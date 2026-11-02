"""Convert the two counterbalanced Criterion passes to benchcat series."""
import json
import math
import os
from pathlib import Path

TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "target"))
ROOT = TARGET / "criterion"
for name in ("rimuru_l3", "quinn_stream"):
    records = []
    for run in (1, 2):
        estimates = ROOT / f"ordered_comparison_pass{run}_t1_d1200_c1_w4" / name / "new/estimates.json"
        slope = json.loads(estimates.read_text(encoding="utf-8"))["slope"]["point_estimate"]
        if not math.isfinite(slope) or slope <= 0:
            raise ValueError(f"Invalid Criterion slope: {estimates}")
        records.append({
            "id": f"pass{run}",
            "group": "Ordered complete-message goodput",
            "label": f"Counterbalanced pass {run}",
            "note": "1 MiB messages · window 4 · UDP payload limit 1200 · CRC32C",
            "unit": "B/s",
            "value": 4 * 1048576 * 1e9 / slope,
            "direction": "higher",
        })
    path = TARGET / f"benchcat-{name}.json"
    path.write_text(json.dumps({"schema": "benchcat/report@1", "meta": {
        "tool": "Criterion", "tool_version": "0.8.2"}, "benchmarks": records},
        indent=2) + "\n", encoding="utf-8")
