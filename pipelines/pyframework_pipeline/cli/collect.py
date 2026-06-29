"""`collect` subcommand handlers (Step 5b: remote data collection)."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def handle(args) -> int:
    if args.collect_command == "run":
        return cmd_collect_run(args)
    return 2


def cmd_collect_run(args) -> int:
    from ..orchestrator import _run_collect

    project_path = Path(args.project)
    run_dir = Path(args.run_dir)

    try:
        _run_collect(project_path, run_dir, args.platform)
        print(json.dumps({"status": "collected", "platform": args.platform}))  # noqa: T201
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)  # noqa: T201
        return 1
