"""`validate` top-level command handler (validate four-layer input or project.yaml)."""
from __future__ import annotations

import json
from pathlib import Path


def cmd_validate(args) -> int:
    from ..validators.four_layer import validate_four_layer_project

    report = validate_four_layer_project(Path(args.path))
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if report.status == "ok" else 1
