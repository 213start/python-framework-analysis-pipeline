"""`benchmark` subcommand handlers (Step 5a: benchmark execution)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from ._common import now_date_str


def handle(args) -> int:
    if args.bench_command == "run":
        return cmd_benchmark_run(args)
    return 2


def cmd_benchmark_run(args) -> int:
    from ..orchestrator import _run_benchmark
    from ..config import load_project_config

    project_path = Path(args.project)
    run_dir = Path(args.run_dir) if args.run_dir else project_path.parent / "runs" / now_date_str()
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        _run_benchmark(project_path, run_dir, args.platform)
        print(json.dumps({"status": "completed", "platform": args.platform}))  # noqa: T201
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)  # noqa: T201
        return 1
