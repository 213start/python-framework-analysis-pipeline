"""`config` subcommand handlers (Step 1: config fetch + integrity check)."""
from __future__ import annotations

import json
from pathlib import Path


def handle(args) -> int:
    if args.config_command == "validate":
        return cmd_config_validate(args)
    return 2


def cmd_config_validate(args) -> int:
    from ..config import validate_pipeline_config

    result = validate_pipeline_config(
        Path(args.project),
        require_bridge_token=not args.skip_bridge_token,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if result["status"] == "ok" else 1
