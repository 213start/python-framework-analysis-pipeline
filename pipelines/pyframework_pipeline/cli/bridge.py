"""`bridge` subcommand handlers (Step 7: diff-analysis Issue bridging)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from ._common import resolve_bridge_config


def handle(args) -> int:
    if args.bridge_command == "publish":
        return cmd_bridge_publish(args)
    if args.bridge_command == "fetch":
        return cmd_bridge_fetch(args)
    if args.bridge_command == "status":
        return cmd_bridge_status(args)
    return 2


def cmd_bridge_publish(args) -> int:
    from ..bridge.analysis import publish

    project_path = Path(args.project)
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)  # noqa: T201
        return 1

    config = resolve_bridge_config(args)
    if config is None:
        return 1

    try:
        result = publish(
            project_path,
            repo=config["repo"],
            platform=config["platform"],
            token=config.get("token", ""),
            bridge_type=config.get("type", "discussion"),
            discussion_category=config.get("category", "General"),
            dry_run=args.dry_run,
            max_lines=args.max_lines,
            base_url=args.base_url,
            symbols=args.symbols,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)  # noqa: T201
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0


def cmd_bridge_fetch(args) -> int:
    from ..bridge.analysis import fetch

    project_path = Path(args.project)
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)  # noqa: T201
        return 1

    config = resolve_bridge_config(args)
    if config is None:
        return 1

    try:
        result = fetch(
            project_path,
            repo=config["repo"],
            platform=config["platform"],
            token=config["token"],
            bridge_type=config.get("type", "discussion"),
            base_url=args.base_url,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)  # noqa: T201
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if result.get("failed", 0) == 0 else 1


def cmd_bridge_status(args) -> int:
    from ..bridge.analysis import status

    project_path = Path(args.project)
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)  # noqa: T201
        return 1

    result = status(project_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0
