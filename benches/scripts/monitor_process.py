"""Sample a benchmark process; requires the optional Python package psutil."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

import psutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command or not 0.1 <= args.interval <= 10:
        parser.error("provide a command and an interval between 0.1 and 10 seconds")

    started = time.monotonic()
    child = subprocess.Popen(command)
    process = psutil.Process(child.pid)
    samples = []
    while child.poll() is None:
        try:
            with process.oneshot():
                cpu = process.cpu_times()
                memory = process.memory_info()
                samples.append({"seconds": time.monotonic() - started,
                                "cpu_seconds": cpu.user + cpu.system,
                                "working_set_bytes": memory.rss,
                                "threads": process.num_threads()})
        except psutil.NoSuchProcess:
            break
        time.sleep(args.interval)
    exit_code = child.wait()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"schema": 1, "command": command,
                                       "interval_seconds": args.interval,
                                       "exit_code": exit_code, "samples": samples},
                                      indent=2) + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
