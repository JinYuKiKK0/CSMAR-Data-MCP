"""Unified lint entrypoint: ruff + pyright over csmar_mcp and tests.

Default mode (pre-push / CI): check-only, no write-back.
`--fix` mode (pre-commit): ruff auto-fix + ruff format write back, then pyright check.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

TARGETS = ["csmar_mcp", "tests"]


def run(cmd: list[str]) -> int:
    print(f"$ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, cwd=Path(__file__).resolve().parent.parent).returncode


def main() -> int:
    fix = "--fix" in sys.argv[1:]

    if fix:
        steps: list[list[str]] = [
            ["ruff", "check", "--fix", *TARGETS],
            ["ruff", "format", *TARGETS],
            ["pyright"],
        ]
    else:
        steps = [
            ["ruff", "check", *TARGETS],
            ["ruff", "format", "--check", *TARGETS],
            ["pyright"],
        ]

    for step in steps:
        rc = run(step)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
