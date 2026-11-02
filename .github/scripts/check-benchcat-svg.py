"""Require both CI charts to be nonempty XML SVG documents."""
from pathlib import Path
from xml.etree import ElementTree

for name in ("transport-benchmarks.svg", "transport-comparison.svg"):
    path = Path("target") / name
    if not path.read_bytes().startswith(b"<?xml"):
        raise ValueError(f"{path}: missing XML declaration")
    root = ElementTree.parse(path).getroot()
    if root.tag != "{http://www.w3.org/2000/svg}svg":
        raise ValueError(f"{path}: not an SVG document")
    print(f"{path}: {path.stat().st_size} bytes")
