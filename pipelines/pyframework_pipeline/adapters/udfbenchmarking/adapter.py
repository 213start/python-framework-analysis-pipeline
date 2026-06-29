"""UDF_Benchmarking framework adapter.

Implements the framework-specific acquisition strategies for the
UDF_Benchmarking benchmark. The deploy logic here was moved out of the
orchestrator (Phase 3 OOP refactor) so the adapter is the single source for
UDF_Benchmarking deploy behaviour, not a shim that delegates back.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ...contracts.adapter import DisassemblySpec, PerfAttachSpec, WorkloadHandle
from ..registry import register_adapter

logger = logging.getLogger(__name__)


def _container(env_config: dict[str, Any]) -> str:
    """Container name running the UDF_Benchmarking benchmark."""
    return str(
        env_config.get("software", {}).get(
            "udfBenchmarkingContainer",
            "udf-benchmarking-bench",
        )
    )


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
        """Deploy the UDF_Benchmarking workload into its container."""
        from ...config import get_workload_config, load_environment_config
        from ...remote import build_executor, get_platform_host_ref
        from ...contracts.step import StepError

        workload = get_workload_config(project_path)
        local_dir = project_path.parent / workload["localDir"]
        if not local_dir.exists():
            raise StepError(f"Workload directory not found: {local_dir}")

        env_config = load_environment_config(project_path)
        host_ref = get_platform_host_ref(env_config, platform)
        executor = build_executor(host_ref, env_config)

        container = _container(env_config)
        remote_dir = "/tmp/pyframework-workload"
        executor.run(f"rm -rf {remote_dir}", timeout=15)
        logger.info("Uploading UDF_Benchmarking workload %s to %s", local_dir, remote_dir)
        ok = executor.push_dir(local_dir, remote_dir)
        if not ok:
            raise StepError(
                f"Failed to upload UDF_Benchmarking workload:\n"
                f"  Local: {local_dir}\n"
                f"  Remote: {remote_dir}"
            )

        clean_result = executor.run(
            f"docker exec -u root {container} bash -lc "
            "'rm -rf /workspace/benchmark && mkdir -p /workspace/benchmark && "
            "cp -a /opt/UDF_Benchmarking/. /workspace/benchmark/'",
            timeout=120,
            stream=True,
        )
        if clean_result.returncode != 0:
            raise StepError(
                f"Failed to prepare UDF_Benchmarking benchmark directory "
                f"(exit {clean_result.returncode}):\n"
                f"  stdout: {clean_result.stdout[:500]}\n"
                f"  stderr: {clean_result.stderr[:500]}"
            )

        cp_result = executor.run(
            f"docker cp {remote_dir}/. {container}:/workspace/benchmark",
            timeout=120,
            stream=True,
        )
        if cp_result.returncode != 0:
            raise StepError(
                f"Failed to copy workload to {container} (exit {cp_result.returncode}):\n"
                f"  stdout: {cp_result.stdout[:500]}\n"
                f"  stderr: {cp_result.stderr[:500]}"
            )
        executor.run(
            f"docker exec -u root {container} chown -R root:root /workspace/benchmark",
            timeout=15,
        )
        return WorkloadHandle(container=container, host=str(host_ref), env_dir=run_dir / platform)

    def run_benchmark(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        force: bool = False,
    ) -> Path:
        """Run the UDF_Benchmarking benchmark.

        Benchmark execution routes through the orchestrator's top-level
        ``_run_benchmark`` dispatcher; only ``deploy_workload`` is fully
        extracted into this adapter for now.
        """
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
            command="perf",
            output_path=run_dir / platform / "perf.data",
        )

    def normalize_timing(
        self,
        timing_path: Path,
        *,
        platform: str,
    ) -> dict[str, Any]:
        if timing_path.exists():
            return json.loads(timing_path.read_text(encoding="utf-8"))
        return {}

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
        from ... import orchestrator

        orchestrator._run_benchmark(project_path, run_dir, platform, force=True)
        out = run_dir / platform / "flamegraph"
        return out if out.exists() else None

    def disassembly_source(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
    ) -> DisassemblySpec:
        return DisassemblySpec(
            source_path=run_dir / platform,
            output_dir=run_dir / platform / "asm",
        )
