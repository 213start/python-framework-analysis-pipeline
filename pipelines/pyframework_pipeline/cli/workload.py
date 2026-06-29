"""`workload` subcommand handlers (Step 4: workload deployment)."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def handle(args) -> int:
    if args.workload_command == "deploy":
        return cmd_workload_deploy(args)
    return 2


def cmd_workload_deploy(args) -> int:
    from ..orchestrator import _run_workload_deploy

    project_path = Path(args.project)
    try:
        _run_workload_deploy(project_path, Path("/tmp/run"), args.platform, yes=False)
        print(json.dumps({"status": "deployed", "platform": args.platform}))  # noqa: T201
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)  # noqa: T201
        return 1
