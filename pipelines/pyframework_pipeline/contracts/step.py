"""Pipeline step contracts used by the OOP execution layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


class StepError(Exception):
    """Raised when a pipeline step fails."""


@dataclass
class RunContext:
    """Runtime state passed to a registered pipeline step."""

    adapter: Any
    project_path: Path
    run_dir: Path
    platform: str | None
    config: dict[str, Any] = field(default_factory=dict)
    paths: dict[str, Path] = field(default_factory=dict)
    state: Any = None


@runtime_checkable
class Step(Protocol):
    """Executable pipeline step with dependency metadata."""

    name: str
    requires: tuple[str, ...]
    produces: tuple[str, ...]

    def run(self, ctx: RunContext) -> None:
        """Run the step for the provided context."""
