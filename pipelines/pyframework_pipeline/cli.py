import argparse
import json
import sys
from pathlib import Path

from .validators.four_layer import validate_four_layer_project


def _schemas_dir() -> Path:
    """Resolve the schemas/ directory relative to this package."""
    return Path(__file__).resolve().parent.parent.parent / "schemas"


def _load_adapter(framework: str):
    """Load the environment adapter for a given framework."""
    if framework == "pyflink":
        from .adapters.pyflink.environment import PyFlinkEnvironmentAdapter
        return PyFlinkEnvironmentAdapter()
    raise ValueError(f"No environment adapter for framework: {framework}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyframework-pipeline",
        description="Python 框架自动化分析流程工具。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # validate
    validate_parser = subparsers.add_parser(
        "validate",
        help="校验四层输入目录或 project.yaml。",
    )
    validate_parser.add_argument("path", help="四层输入目录或 project.yaml 路径")

    # environment
    env_parser = subparsers.add_parser(
        "environment",
        help="环境搭建相关命令。",
    )
    env_sub = env_parser.add_subparsers(dest="env_command", required=True)

    # environment plan
    plan_parser = env_sub.add_parser(
        "plan",
        help="生成环境搭建计划（plan-only，不执行远程命令）。",
    )
    plan_parser.add_argument(
        "project",
        help="project.yaml 路径",
    )
    plan_parser.add_argument(
        "--platform",
        required=True,
        help="目标平台 ID，例如 arm、x86",
    )
    plan_parser.add_argument(
        "--output",
        help="输出目录（默认打印到 stdout）",
    )

    # environment validate
    env_validate_parser = env_sub.add_parser(
        "validate",
        help="校验环境记录和 readiness 报告。",
    )
    env_validate_parser.add_argument(
        "run_dir",
        help="包含 environment-plan.json / environment-record.json / readiness-report.json 的运行目录",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate":
        report = validate_four_layer_project(Path(args.path))
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        return 0 if report.status == "ok" else 1

    if args.command == "environment":
        return _handle_environment(args)

    parser.print_help(sys.stderr)
    return 2


def _handle_environment(args) -> int:
    if args.env_command == "plan":
        return _cmd_env_plan(args)
    if args.env_command == "validate":
        return _cmd_env_validate(args)
    return 2


def _cmd_env_plan(args) -> int:
    from .environment.parser import load_environment_yaml
    from .environment.planning import generate_plan

    project_path = Path(args.project)
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)
        return 1

    # Detect framework from environment.yaml
    env_yaml_path = project_path.parent / "environment.yaml"
    if not env_yaml_path.exists():
        print(f"Error: environment.yaml not found at {env_yaml_path}", file=sys.stderr)
        return 1

    env_config = load_environment_yaml(env_yaml_path)
    framework = env_config.get("framework", "")

    try:
        adapter = _load_adapter(framework)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        plan = generate_plan(project_path, args.platform, adapter)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    plan_json = json.dumps(plan, ensure_ascii=False, indent=2)

    if args.output:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "environment-plan.json").write_text(plan_json, encoding="utf-8")
        print(f"Plan written to {output_dir / 'environment-plan.json'}")
    else:
        print(plan_json)

    return 0


def _cmd_env_validate(args) -> int:
    from .environment.records import validate_run

    run_dir = Path(args.run_dir)
    if not run_dir.is_dir():
        print(f"Error: {run_dir} is not a directory", file=sys.stderr)
        return 1

    schemas = _schemas_dir()
    report = validate_run(run_dir, schemas)
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.status == "ok" else 1
