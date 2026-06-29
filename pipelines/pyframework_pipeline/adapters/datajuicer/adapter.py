"""Data-Juicer framework adapter.

Implements the framework-specific acquisition strategies for the Data-Juicer
benchmark. The deploy/benchmark logic here was moved out of the orchestrator
(Phase 3 OOP refactor) so the adapter is the single source for Data-Juicer
behaviour, not a shim that delegates back to the orchestrator.
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
    """Container name running the Data-Juicer benchmark."""
    return str(
        env_config.get("software", {}).get("dataJuicerContainer", "data-juicer-bench")
    )


def _config_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _modalities(workload: dict[str, Any], env_config: dict[str, Any]) -> list[str]:
    """Resolve the benchmark modalities (text-only supported)."""
    raw = workload.get("modalities")
    if raw in ("", None):
        raw = env_config.get("software", {}).get("benchmarkModalities", ["text"])
    if isinstance(raw, list):
        values = [str(item).strip() for item in raw]
    else:
        values = [item.strip() for item in str(raw).replace(",", " ").split()]
    selected = [item for item in values if item == "text"]
    return selected or ["text"]


def _benchmark_name(env_config: dict[str, Any]) -> str:
    return str(
        env_config.get("software", {}).get("benchmarkName", "data-juicer-text")
    )


def _python_flamegraph_config(env_config: dict[str, Any]) -> dict[str, Any]:
    raw = env_config.get("software", {}).get("pythonFlamegraph", {})
    if isinstance(raw, dict):
        enabled = _config_bool(raw.get("enabled", False))
        rate = int(raw.get("rate", 100) or 100)
        subprocesses = _config_bool(raw.get("subprocesses", True))
    else:
        enabled = _config_bool(raw)
        rate = 100
        subprocesses = True
    return {"enabled": enabled, "rate": rate, "subprocesses": subprocesses}


@register_adapter
class DataJuicerAdapter:
    framework_id = "datajuicer"

    def describe(self) -> str:
        return "Data-Juicer benchmark adapter"

    def deploy_workload(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        yes: bool = False,
    ) -> WorkloadHandle:
        """Deploy the Data-Juicer workload into its container."""
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
        logger.info("Uploading Data-Juicer workload %s to %s", local_dir, remote_dir)
        ok = executor.push_dir(local_dir, remote_dir)
        if not ok:
            raise StepError(
                f"Failed to upload Data-Juicer workload:\n"
                f"  Local: {local_dir}\n"
                f"  Remote: {remote_dir}"
            )

        clean_result = executor.run(
            f"docker exec -u root {container} bash -lc "
            "'rm -rf /workspace/benchmark && mkdir -p /workspace/benchmark'",
            timeout=120,
            stream=True,
        )
        if clean_result.returncode != 0:
            raise StepError(
                f"Failed to clean Data-Juicer benchmark directory "
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
        """Run the Data-Juicer benchmark (perf + timing + optional flamegraph).

        Owns the framework-specific benchmark logic, moved verbatim from the
        orchestrator's _run_datajuicer_benchmark / _run_datajuicer_python_flamegraph
        (Phase 3 extraction). The adapter is the single source for Data-Juicer
        acquisition behaviour.
        """
        import shlex
        import shutil

        from ...config import get_workload_config, load_environment_config
        from ...remote import build_executor, get_platform_host_ref
        from ...contracts.step import StepError

        workload = get_workload_config(project_path)
        env_config = load_environment_config(project_path)
        host_ref = get_platform_host_ref(env_config, platform)
        executor = build_executor(host_ref, env_config)

        container = _container(env_config)
        modalities = _modalities(workload, env_config)
        modalities_arg = ",".join(modalities)
        platform_run_dir = run_dir / platform
        platform_run_dir.mkdir(parents=True, exist_ok=True)
        timing_path = platform_run_dir / "timing" / "timing-normalized.json"
        python_dir = platform_run_dir / "python"

        if force:
            from ... import orchestrator

            orchestrator._run_workload_deploy(project_path, run_dir, platform)
            for old in [
                timing_path,
                platform_run_dir / "timing" / "timing-raw.json",
                platform_run_dir / "perf" / "data" / f"perf-{platform}.data",
            ]:
                if old.exists():
                    old.unlink()
                    logger.info("[5a] Force: removed old artifact %s", old.relative_to(run_dir))
            if python_dir.exists():
                shutil.rmtree(python_dir)
                logger.info("[5a] Force: removed old artifact %s", python_dir.relative_to(run_dir))

        if timing_path.exists() and timing_path.stat().st_size > 0:
            logger.info("[5a] Data-Juicer timing exists, skipping benchmark on %s", platform)
        else:
            timeout = int(
                workload.get(
                    "timeout",
                    env_config.get("software", {}).get("benchmarkTimeout", 1800),
                )
            )
            remote_output = f"/tmp/pyframework-datajuicer-run-{platform}"
            host_output = f"/tmp/pyframework-datajuicer-output-{platform}"
            runner_args = (
                f"--platform {shlex.quote(platform)} "
                f"--output-dir {shlex.quote(remote_output)} "
                f"--modalities {shlex.quote(modalities_arg)}"
            )
            script = (
                "set -euo pipefail; "
                f"rm -rf {shlex.quote(remote_output)} /tmp/perf-udf.data; "
                "cd /workspace/benchmark; "
                "command -v perf >/dev/null; "
                f"perf record -F 999 -g -e task-clock -o /tmp/perf-udf.data -- "
                f"python3 benchmark_runner.py {runner_args}; "
                "test -s /tmp/perf-udf.data"
            )
            logger.info("[5a] Running Data-Juicer modalities=%s on %s", modalities_arg, platform)
            result = executor.run(
                f"docker exec {container} bash -lc {shlex.quote(script)}",
                timeout=timeout,
                stream=True,
            )
            if result.returncode != 0:
                raise StepError(
                    f"Data-Juicer benchmark failed (exit {result.returncode}):\n"
                    f"  Container: {container}\n"
                    f"  Modalities: {modalities_arg}\n"
                    f"  output: {result.stdout[-2000:]}\n"
                    f"  stderr: {result.stderr[-1000:]}"
                )

            executor.run(f"rm -rf {host_output}", timeout=15)
            cp_result = executor.run(
                f"docker cp {container}:{remote_output}/. {host_output}",
                timeout=120,
                stream=True,
            )
            if cp_result.returncode != 0:
                raise StepError(
                    f"Failed to copy Data-Juicer benchmark output (exit {cp_result.returncode}):\n"
                    f"  stdout: {cp_result.stdout[:500]}\n"
                    f"  stderr: {cp_result.stderr[:500]}"
                )
            if not executor.fetch_dir(host_output, platform_run_dir):
                raise StepError(
                    f"Failed to fetch Data-Juicer benchmark output from {host_output}:\n"
                    f"  docker cp stdout: {cp_result.stdout[:500]}\n"
                    f"  docker cp stderr: {cp_result.stderr[:500]}"
                )
            executor.run(f"rm -rf {host_output}", timeout=15)

            if not timing_path.exists() or timing_path.stat().st_size == 0:
                raise StepError(
                    f"Data-Juicer benchmark did not produce {timing_path}:\n"
                    f"  docker cp stdout: {cp_result.stdout[:500]}\n"
                    f"  docker cp stderr: {cp_result.stderr[:500]}"
                )

        self._run_python_flamegraph(
            executor, container, platform, platform_run_dir, modalities_arg, env_config, workload,
        )
        return timing_path

    def _run_python_flamegraph(
        self,
        executor: Any,
        container: str,
        platform: str,
        platform_run_dir: Path,
        modalities_arg: str,
        env_config: dict[str, Any],
        workload: dict[str, Any],
    ) -> None:
        """Optional py-spy flamegraph collection (moved from orchestrator)."""
        import shlex
        from ...contracts.step import StepError

        config = _python_flamegraph_config(env_config)
        if not config["enabled"]:
            return

        benchmark_name = _benchmark_name(env_config)
        flamegraph_dir = platform_run_dir / "python" / "flamegraphs"
        flamegraph_path = flamegraph_dir / f"{benchmark_name}.svg"
        manifest_path = platform_run_dir / "python" / "manifest.json"
        if flamegraph_path.exists() and flamegraph_path.stat().st_size > 0:
            logger.info("[5a] Python flamegraph exists, skipping py-spy on %s", platform)
            return

        timeout = int(
            workload.get(
                "timeout",
                env_config.get("software", {}).get("benchmarkTimeout", 1800),
            )
        )
        remote_profile = f"/tmp/pyframework-datajuicer-python-{platform}"
        remote_output = f"/tmp/pyframework-datajuicer-python-run-{platform}"
        host_output = f"/tmp/pyframework-datajuicer-python-output-{platform}"
        runner_args = (
            f"--platform {shlex.quote(platform)} "
            f"--output-dir {shlex.quote(remote_output)} "
            f"--modalities {shlex.quote(modalities_arg)}"
        )
        subprocesses_arg = "--subprocesses " if config["subprocesses"] else ""
        script = (
            "set -euo pipefail; "
            f"rm -rf {shlex.quote(remote_output)} {shlex.quote(remote_profile)}; "
            f"mkdir -p {shlex.quote(remote_profile)}/flamegraphs; "
            "cd /workspace/benchmark; "
            "command -v py-spy >/dev/null; "
            "py-spy record "
            f"--rate {int(config['rate'])} "
            f"{subprocesses_arg}"
            "--format flamegraph "
            f"-o {shlex.quote(remote_profile)}/flamegraphs/{shlex.quote(benchmark_name)}.svg -- "
            f"python3 benchmark_runner.py {runner_args}; "
            f"test -s {shlex.quote(remote_profile)}/flamegraphs/{shlex.quote(benchmark_name)}.svg"
        )
        logger.info("[5a] Running Data-Juicer Python flamegraph on %s", platform)
        result = executor.run(
            f"docker exec {container} bash -lc {shlex.quote(script)}",
            timeout=timeout,
            stream=True,
        )
        if result.returncode != 0:
            raise StepError(
                f"Data-Juicer Python flamegraph failed (exit {result.returncode}):\n"
                f"  Container: {container}\n"
                f"  Modalities: {modalities_arg}\n"
                f"  output: {result.stdout[-2000:]}\n"
                f"  stderr: {result.stderr[-1000:]}"
            )

        executor.run(f"rm -rf {host_output}", timeout=15)
        cp_result = executor.run(
            f"docker cp {container}:{remote_profile}/. {host_output}",
            timeout=120,
            stream=True,
        )
        if cp_result.returncode != 0:
            raise StepError(
                f"Failed to copy Data-Juicer Python flamegraph output (exit {cp_result.returncode}):\n"
                f"  stdout: {cp_result.stdout[:500]}\n"
                f"  stderr: {cp_result.stderr[:500]}"
            )
        local_python_dir = platform_run_dir / "python"
        if not executor.fetch_dir(host_output, local_python_dir):
            raise StepError(
                f"Failed to fetch Data-Juicer Python flamegraph output from {host_output}:\n"
                f"  docker cp stdout: {cp_result.stdout[:500]}\n"
                f"  docker cp stderr: {cp_result.stderr[:500]}"
            )
        flamegraph_dir.mkdir(parents=True, exist_ok=True)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "framework": "datajuicer",
                    "platform": platform,
                    "profiler": "py-spy",
                    "cases": [
                        {
                            "caseId": benchmark_name,
                            "format": "flamegraph",
                            "rate": int(config["rate"]),
                            "subprocesses": bool(config["subprocesses"]),
                            "path": str(flamegraph_path.relative_to(local_python_dir)),
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        executor.run(f"rm -rf {host_output}", timeout=15)

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
        # Flamegraph collection is integrated into run_benchmark (it reads the
        # pythonFlamegraph env config). This method exists for the adapter
        # contract; the actual collection happens during run_benchmark.
        if not enabled:
            return None
        from ...config import load_environment_config

        env_config = load_environment_config(project_path)
        if not _python_flamegraph_config(env_config)["enabled"]:
            return None
        benchmark_name = _benchmark_name(env_config)
        out = run_dir / platform / "python" / "flamegraphs" / f"{benchmark_name}.svg"
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
