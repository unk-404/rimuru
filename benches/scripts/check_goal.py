"""Validate every L2/L3 window in a Windows loopback goal report."""

import argparse
import json
import math
import pathlib
import sys

MAX_SUBWINDOW_SECONDS = 1.1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=pathlib.Path)
    parser.add_argument("--rounds", type=int, required=True)
    parser.add_argument("--duration-ms", type=int, required=True)
    parser.add_argument("--require-timeline", action="store_true",
                        help="also require and check each complete one-second subwindow")
    parser.add_argument("--require-payload-verification", action="store_true",
                        help="require byte-for-byte application payload verification")
    parser.add_argument("--require-distinct-payloads", action="store_true",
                        help="require byte verification with distinct in-flight payloads")
    args = parser.parse_args()
    if args.rounds < 1 or args.duration_ms < 1:
        parser.error("--rounds and --duration-ms must be positive")

    report = json.loads(args.report.read_text(encoding="utf-8"))
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    require(report.get("schema") == 1, "unexpected schema")
    require(report.get("platform", "").endswith("-windows"), "not a Windows report")
    require(report.get("checksum") == "CRC32C (default)", "CRC32C is not enabled")
    if args.require_payload_verification or args.require_distinct_payloads:
        require(report.get("verify_payload") is True, "payload verification is not enabled")
    if args.require_distinct_payloads:
        require(report.get("payload_variants") == 4, "four distinct payloads are not enabled")
    cells = report.get("cells", [])
    require(len(cells) == 2, "expected exactly two cells")
    require({c.get("mode") for c in cells} == {"l2_slice", "l3_slice"},
            "expected L2 and L3 slice cells")
    for cell in cells:
        mode = cell.get("mode", "unknown")
        expected = {"d": 65507, "message": 1048576, "processing_threads": 1,
                    "window": 4, "batch_size": 32, "channels": 1,
                    "hold_first_ms": 0, "loss_ppm": 0, "rtt_ms": 0}
        for key, value in expected.items():
            require(cell.get(key) == value, f"{mode}: {key} must be {value}")
        rounds = cell.get("rounds", [])
        require(len(rounds) == args.rounds, f"{mode}: expected {args.rounds} rounds")
        for index, result in enumerate(rounds, 1):
            label = f"{mode} round {index}"
            seconds = result.get("seconds", 0)
            total = result.get("unique_application_bytes", 0)
            windows = result.get("windows", 0)
            require(result.get("error") is None, f"{label}: delivery error")
            require(math.isfinite(seconds) and seconds >= args.duration_ms / 1000,
                    f"{label}: window shorter than requested")
            require(windows > 0 and total == windows * 4 * 1048576,
                    f"{label}: incomplete application bytes")
            actual = total / seconds / 1e9 if seconds > 0 else 0
            require(math.isfinite(actual) and actual > 4, f"{label}: {actual:.3f} GB/s <= 4")
            require(math.isclose(actual, result.get("gb_per_second", 0), rel_tol=1e-9),
                    f"{label}: recorded throughput disagrees with bytes/time")
            for side in ("tx", "rx"):
                counters = result.get(side, {})
                for key in ("aborted", "unknown", "crc_failures"):
                    require(counters.get(key) == 0, f"{label}: {side}.{key} is nonzero or missing")
            if args.require_timeline:
                timeline = result.get("timeline", [])
                require(not result.get("timeline_truncated", False), f"{label}: truncated timeline")
                require(bool(timeline), f"{label}: timeline missing")
                require(any(b.get("complete_second") is True for b in timeline),
                        f"{label}: no complete one-second subwindow")
                require(sum(b.get("unique_application_bytes", 0) for b in timeline) == total,
                        f"{label}: timeline bytes do not match total")
                require(sum(b.get("windows", 0) for b in timeline) == windows,
                        f"{label}: timeline windows do not match total")
                require(math.isclose(sum(b.get("seconds", 0) for b in timeline), seconds,
                                     rel_tol=0, abs_tol=0.01),
                        f"{label}: timeline does not cover the round")
                for part, bucket in enumerate(timeline, 1):
                    duration = bucket.get("seconds", 0)
                    count = bucket.get("windows", 0)
                    size = bucket.get("unique_application_bytes", 0)
                    complete = bucket.get("complete_second")
                    require(math.isfinite(duration) and duration > 0,
                            f"{label} subwindow {part}: invalid duration")
                    require(count > 0 and size == count * 4 * 1048576,
                            f"{label} subwindow {part}: incomplete bytes")
                    require(complete is True or complete is False,
                            f"{label} subwindow {part}: missing completeness flag")
                    if complete is True:
                        require(duration >= 1, f"{label} subwindow {part}: shorter than one second")
                        require(duration <= MAX_SUBWINDOW_SECONDS,
                                f"{label} subwindow {part}: exceeds {MAX_SUBWINDOW_SECONDS}s")
                        speed = size / duration / 1e9 if duration > 0 else 0
                        require(math.isfinite(speed) and speed > 4,
                                f"{label} subwindow {part}: {speed:.3f} GB/s <= 4")
                    if complete is False:
                        require(part == len(timeline) and duration < 1,
                                f"{label} subwindow {part}: invalid partial tail")
            print(f"{label}: {actual:.3f} GB/s; {seconds:.3f} s; "
                  f"retransmissions={result.get('tx', {}).get('retransmissions', '?')}; "
                  f"tx/rx peak={result.get('tx', {}).get('memory_peak', '?')}/"
                  f"{result.get('rx', {}).get('memory_peak', '?')} bytes")
    if errors:
        for error in errors:
            print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print("PASS: every reported window meets the specified goal and checks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
