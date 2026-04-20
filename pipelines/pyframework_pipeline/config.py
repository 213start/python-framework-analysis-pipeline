from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def resolve_four_layer_root(path: Path) -> Path:
    if path.is_dir():
        return path

    if path.name != "project.yaml":
        raise ValueError(f"Unsupported project config: {path}")

    config = parse_simple_yaml(path)
    root_value = config.get("fourLayerRoot")
    if not root_value:
        raise ValueError(f"{path} is missing fourLayerRoot")

    return (path.parent / root_value).resolve()


def parse_simple_yaml(path: Path) -> dict[str, str]:
    """Extract top-level flat key:value pairs from a YAML file."""
    result: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if value.strip():
            result[key.strip()] = value.strip().strip('"').strip("'")
    return result


def load_project_config(path: Path) -> dict[str, Any]:
    """Load project.yaml with full nested YAML support."""
    if not path.exists():
        raise FileNotFoundError(f"Project config not found: {path}")
    text = path.read_text(encoding="utf-8")
    from .environment.parser import parse_yaml
    return parse_yaml(text)


def get_bridge_config(project_path: Path) -> dict[str, Any]:
    """Read bridge configuration from project.yaml + resolve token from env."""
    config = load_project_config(project_path)
    bridge = config.get("bridge", {})
    if not bridge.get("repo"):
        raise ValueError(
            "project.yaml missing bridge.repo. "
            "Add: bridge:\n  repo: owner/repo\n  platform: github|gitcode"
        )
    if not bridge.get("platform"):
        raise ValueError("project.yaml missing bridge.platform (github or gitcode)")
    env_var = bridge.get("tokenEnvVar", "PYFRAMEWORK_BRIDGE_TOKEN")
    token = os.environ.get(env_var, "")
    bridge["token"] = token
    if not token:
        raise ValueError(
            f"Bridge token not set. Run: export {env_var}=<your-api-token>"
        )
    return bridge


def get_workload_config(project_path: Path) -> dict[str, Any]:
    """Read workload configuration from project.yaml."""
    config = load_project_config(project_path)
    workload = config.get("workload", {})
    if not workload.get("localDir"):
        raise ValueError(
            "project.yaml missing workload.localDir. "
            "Add: workload:\n  localDir: workload/tpch/pyflink"
        )
    return workload


def get_run_config(project_path: Path) -> dict[str, Any]:
    """Read run configuration from project.yaml."""
    config = load_project_config(project_path)
    run = config.get("run", {})
    platforms = run.get("platforms", [])
    if not platforms:
        raise ValueError(
            "project.yaml missing run.platforms. "
            "Add: run:\n  platforms: [arm, x86]"
        )
    return run


def load_environment_config(project_path: Path) -> dict[str, Any]:
    """Load environment.yaml sibling to project.yaml."""
    env_path = project_path.parent / "environment.yaml"
    if not env_path.exists():
        raise FileNotFoundError(f"environment.yaml not found at {env_path}")
    from .environment.parser import load_environment_yaml
    return load_environment_yaml(env_path)
