from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...contracts.adapter import DisassemblySpec, PerfAttachSpec, WorkloadHandle
from ..registry import register_adapter


@register_adapter
class UdfBenchmarkingAdapter:
    framework_id = "udfbenchmarking"

    def describe(self) -> str:
        return "UDF_Benchmarking adapter"

    def deploy_workload(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        yes: bool = False,
    ) -> WorkloadHandle:
        from ... import orchestrator

        orchestrator._run_workload_deploy(project_path, run_dir, platform, yes=yes)
        return WorkloadHandle(env_dir=run_dir / platform)

    def run_benchmark(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        force: bool = False,
    ) -> Path:
        from ... import orchestrator

        orchestrator._run_benchmark(project_path, run_dir, platform, force=force)
        return run_dir / platform / "timing" / "timing-normalized.json"

    def perf_attach_strategy(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
    ) -> PerfAttachSpec:
        return PerfAttachSpec(
            command="perf record -F 999 -g -e task-clock",
            output_path=run_dir / platform / "perf" / "data" / f"perf-{platform}.data",
        )

    def normalize_timing(
        self,
        timing_path: Path,
        *,
        platform: str,
    ) -> dict[str, Any]:
        return json.loads(timing_path.read_text(encoding="utf-8"))

    def collect_flamegraph(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        enabled: bool = False,
    ) -> Path | None:
        if not enabled:
            return None
        python_dir = run_dir / platform / "python"
        return python_dir if python_dir.exists() else None

    def disassembly_source(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
    ) -> DisassemblySpec:
        arch = "arm64" if platform == "arm" else "x86_64"
        return DisassemblySpec(
            source_path=run_dir / platform / "perf" / "data",
            output_dir=run_dir / platform / "asm" / arch,
        )
