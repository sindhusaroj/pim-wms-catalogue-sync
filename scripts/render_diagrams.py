"""Render every Mermaid block in mermaid.md to docs/diagrams/<name>.svg.

Each block must be preceded by a `<!-- file: name -->` marker. Uses the
minlag/mermaid-cli Docker image, so no Node.js install is needed.

    python scripts/render_diagrams.py
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "mermaid.md"
OUT = ROOT / "docs" / "diagrams"
WORK = ROOT / "build" / "diagrams"
IMAGE = "minlag/mermaid-cli:11.4.2"
BLOCK = re.compile(r"<!-- file: ([\w-]+) -->\s*```mermaid\n(.*?)```", re.S)


def main() -> int:
    blocks = BLOCK.findall(SOURCE.read_text(encoding="utf-8"))
    if not blocks:
        print("no diagrams found", file=sys.stderr)
        return 1
    shutil.rmtree(WORK, ignore_errors=True)
    WORK.mkdir(parents=True)
    OUT.mkdir(parents=True, exist_ok=True)
    for name, body in blocks:
        (WORK / f"{name}.mmd").write_text(body, encoding="utf-8")
    script = (
        "set -e; for f in /data/build/diagrams/*.mmd; do n=$(basename \"$f\" .mmd); "
        "/home/mermaidcli/node_modules/.bin/mmdc -p /puppeteer-config.json -q -b white -i \"$f\" -o \"/data/docs/diagrams/$n.svg\"; echo \"rendered $n\"; done"
    )
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    subprocess.run(["docker", "run", "--rm", "-v", f"{ROOT}:/data", "--entrypoint", "sh", IMAGE, "-c", script],
                   check=True, env=env)
    print(f"{len(blocks)} diagrams -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
