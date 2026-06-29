"""`compare` subcommand handler (Step 6b: cross-platform comparison)."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def cmd_compare(args) -> int:
    from ..compare.pipeline import run_compare

    project_path = Path(args.project)
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)  # noqa: T201
        return 1

    arm_dir = args.arm_run_dir
    x86_dir = args.x86_run_dir

    # Auto-detect run dirs from latest run.
    if not arm_dir or not x86_dir:
        runs_dir = project_path.parent / "runs"
        if not runs_dir.is_dir():
            print("Error: no runs/ directory found, specify --arm-run-dir and --x86-run-dir",  # noqa: T201
                  file=sys.stderr)
            return 1
        latest = max(runs_dir.iterdir(), key=lambda p: p.name)
        if not arm_dir:
            arm_dir = latest / "arm"
        if not x86_dir:
            x86_dir = latest / "x86"

    if not arm_dir.is_dir():
        print(f"Error: ARM run directory not found: {arm_dir}", file=sys.stderr)  # noqa: T201
        return 1
    if not x86_dir.is_dir():
        print(f"Error: x86 run directory not found: {x86_dir}", file=sys.stderr)  # noqa: T201
        return 1

    result = run_compare(
        project_path, arm_dir, x86_dir,
        output_dir=args.output_dir,
        top_n=args.top_n,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if result.get("status") == "completed" else 1
