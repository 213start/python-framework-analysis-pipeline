"""`backfill` subcommand handlers (Step 6: data backfill)."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def handle(args) -> int:
    if args.backfill_command == "run":
        return cmd_backfill_run(args)
    if args.backfill_command == "status":
        return cmd_backfill_status(args)
    return 2


def cmd_backfill_run(args) -> int:
    from ..backfill.pipeline import run_backfill

    project_path = Path(args.project)
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)  # noqa: T201
        return 1

    arm_dir = Path(args.arm_run_dir)
    x86_dir = Path(args.x86_run_dir)
    output = Path(args.output) if args.output else None

    return run_backfill(project_path, arm_dir, x86_dir, output)


def cmd_backfill_status(args) -> int:
    from ..config import resolve_four_layer_root

    project_path = Path(args.project)
    root = resolve_four_layer_root(project_path)

    info: dict = {"project": str(project_path), "fourLayerRoot": str(root)}

    ds_dir = root / "datasets"
    ds_files = list(ds_dir.glob("*.dataset.json")) if ds_dir.is_dir() else []
    info["dataset"] = {"exists": bool(ds_files), "file": ds_files[0].name if ds_files else None}

    src_dir = root / "sources"
    src_files = list(src_dir.glob("*.source.json")) if src_dir.is_dir() else []
    info["source"] = {"exists": bool(src_files), "file": src_files[0].name if src_files else None}

    proj_dir = root / "projects"
    proj_files = list(proj_dir.glob("*.project.json")) if proj_dir.is_dir() else []
    info["project"] = {"exists": bool(proj_files), "file": proj_files[0].name if proj_files else None}

    if ds_files:
        ds = json.loads(ds_files[0].read_text(encoding="utf-8"))
        info["dataset"]["cases"] = len(ds.get("cases", []))
        info["dataset"]["functions"] = len(ds.get("functions", []))
        info["dataset"]["hasStackOverview"] = bool(ds.get("stackOverview"))

    print(json.dumps(info, ensure_ascii=False, indent=2))  # noqa: T201
    return 0
