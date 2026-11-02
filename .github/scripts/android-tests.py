"""Run Cargo-produced native test executables on the CI Android emulator."""
import json
import pathlib
import subprocess
import sys

tests = []
for line in pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    try:
        item = json.loads(line)
    except json.JSONDecodeError:
        continue
    if item.get("reason") == "compiler-artifact" and item.get("profile", {}).get("test") and item.get("executable"):
        tests.append(item["executable"])
if not tests:
    raise SystemExit("No Android test executables found")
for index, executable in enumerate(tests):
    remote = f"/data/local/tmp/rimuru-test-{index}"
    subprocess.run(["adb", "push", executable, remote], check=True)
    subprocess.run(["adb", "shell", "chmod", "700", remote], check=True)
    subprocess.run(["adb", "shell", remote, "--test-threads=1"], check=True)
