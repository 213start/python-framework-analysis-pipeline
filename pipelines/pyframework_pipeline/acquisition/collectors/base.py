"""Base collector protocol for framework-specific acquisition logic."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class TimingCollector(Protocol):
    """Collect timing data from benchmark output."""

    def collect(self, run_dir: Path, platform: str, **kwargs: Any) -> dict[str, Any]:
        ...


class PerfCollector(Protocol):
    """Collect and process perf profile data."""

    def collect(self, run_dir: Path, platform: str, **kwargs: Any) -> dict[str, Any]:
        ...


class AsmCollector(Protocol):
    """Collect machine code / assembly data."""

    def collect(self, run_dir: Path, platform: str, **kwargs: Any) -> dict[str, Any]:
        ...
