"""Framework adapter contracts.

Adapters expose framework-specific strategy points while orchestration keeps the
step lifecycle generic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class WorkloadHandle:
    """Location of a deployed workload on a remote platform."""

    container: str | None = None
    host: str | None = None
    env_dir: Path = Path(".")
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PerfAttachSpec:
    """How perf should attach to framework processes for a workload run."""

    command: str
    output_path: Path
    pid_selector: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DisassemblySpec:
    """Where machine-code disassembly should be collected from."""

    source_path: Path
    output_dir: Path
    symbol_filter: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class FrameworkAdapter(Protocol):
    """Six strategy points required by framework-specific pipelines."""

    framework_id: str

    def deploy_workload(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        yes: bool = False,
    ) -> WorkloadHandle:
        """Deploy the workload and return the handle used by later steps."""

    def run_benchmark(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        force: bool = False,
    ) -> Path:
        """Run benchmark cases and return the normalized timing artifact."""

    def perf_attach_strategy(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
    ) -> PerfAttachSpec:
        """Return the framework-specific perf attach strategy."""

    def normalize_timing(
        self,
        timing_path: Path,
        *,
        platform: str,
    ) -> dict[str, Any]:
        """Normalize raw timing output into the common case schema."""

    def collect_flamegraph(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        enabled: bool = False,
    ) -> Path | None:
        """Collect an optional Python-level flamegraph artifact."""

    def disassembly_source(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
    ) -> DisassemblySpec:
        """Return where to collect disassembly for this framework."""
