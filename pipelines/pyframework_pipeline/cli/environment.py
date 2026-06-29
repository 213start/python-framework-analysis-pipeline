"""`environment` subcommand handlers (Step 3: environment setup)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from ._common import schemas_dir, load_adapter


def handle(args) -> int:
    if args.env_command == "plan":
        return cmd_env_plan(args)
    if args.env_command == "deploy":
        return cmd_env_deploy(args)
    if args.env_command == "teardown":
        return cmd_env_teardown(args)
    if args.env_command == "validate":
        return cmd_env_validate(args)
    if args.env_command == "preflight":
        return cmd_env_preflight(args)
    return 2


def cmd_env_plan(args) -> int:
    from ..environment.parser import load_environment_yaml
    from ..environment.planning import generate_plan

    project_path = Path(args.project)
    env_yaml_path = project_path.parent / "environment.yaml"
    if not env_yaml_path.exists():
        print(f"Error: environment.yaml not found at {env_yaml_path}", file=sys.stderr)  # noqa: T201
        return 1

    env_config = load_environment_yaml(env_yaml_path)
    framework = env_config.get("framework", "")

    try:
        adapter = load_adapter(framework)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)  # noqa: T201
        return 1

    try:
        plan = generate_plan(project_path, args.platform, adapter)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)  # noqa: T201
        return 1

    plan_json = json.dumps(plan, ensure_ascii=False, indent=2)
    if args.output:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "environment-plan.json").write_text(plan_json, encoding="utf-8")
        print(f"Plan written to {output_dir / 'environment-plan.json'}")  # noqa: T201
    else:
        print(plan_json)  # noqa: T201
    return 0


def cmd_env_deploy(args) -> int:
    from ..environment.deploy import deploy_plan

    project_path = Path(args.project)
    plan_path = Path(args.plan) if args.plan else None
    output_dir = Path(args.output) if args.output else None

    result = deploy_plan(project_path, args.platform, plan_path,
                         output_dir=output_dir, yes=args.yes)
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if result.get("status") != "failed" else 1


def cmd_env_teardown(args) -> int:
    from ..environment.deploy import teardown

    project_path = Path(args.project)
    result = teardown(project_path, args.platform, yes=args.yes)
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if result.get("status") != "failed" else 1


def cmd_env_validate(args) -> int:
    from ..environment.records import validate_run

    run_dir = Path(args.run_dir)
    if not run_dir.is_dir():
        print(f"Error: {run_dir} is not a directory", file=sys.stderr)  # noqa: T201
        return 1

    schemas = schemas_dir()
    report = validate_run(run_dir, schemas)
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    return 0 if report.status == "ok" else 1


def cmd_env_preflight(args) -> int:
    from ..environment.preflight import run_preflight

    output_dir = Path(args.output) if args.output else None
    report = run_preflight(
        Path(args.project),
        args.platform,
        output_dir=output_dir,
        check_timeout=args.timeout,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))  # noqa: T201
    return 1 if report["status"] == "error" else 0
