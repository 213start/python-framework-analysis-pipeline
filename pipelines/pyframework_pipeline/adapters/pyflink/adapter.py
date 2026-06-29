from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ...contracts.adapter import DisassemblySpec, PerfAttachSpec, WorkloadHandle
from ...contracts.step import StepError
from . import benchmark as _bench
from ..registry import register_adapter

logger = logging.getLogger(__name__)


@register_adapter
class PyFlinkAdapter:
    framework_id = "pyflink"

    def describe(self) -> str:
        return "PyFlink reference adapter"

    def deploy_workload(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        yes: bool = False,
    ) -> WorkloadHandle:
        """Deploy the PyFlink workload (upload + docker cp + optional JAR build).

        Owns the framework-specific deploy logic, moved verbatim from
        orchestrator._run_workload_deploy's pyflink path (Phase 3 extraction).
        """
        from ...config import get_workload_config, load_environment_config
        from ...remote import build_executor, get_platform_host_ref

        workload = get_workload_config(project_path)
        local_dir = project_path.parent / workload["localDir"]
        if not local_dir.exists():
            raise StepError(f"Workload directory not found: {local_dir}")

        env_config = load_environment_config(project_path)
        host_ref = get_platform_host_ref(env_config, platform)
        executor = build_executor(host_ref, env_config)

        # Upload workload to remote host staging.
        remote_dir = "/tmp/pyframework-workload"
        executor.run(f"rm -rf {remote_dir}", timeout=15)
        logger.info("Uploading %s to %s:%s", local_dir, host_ref, remote_dir)
        ok = executor.push_dir(local_dir, remote_dir)
        if not ok:
            raise StepError(f"Failed to upload workload to {host_ref}:\n  Local: {local_dir}\n  Remote: {remote_dir}")

        # Distribute to containers via docker cp (chown to flink user).
        jm_result = executor.run(f"docker cp {remote_dir}/. flink-jm:/opt/flink/usrlib")
        if jm_result.returncode != 0:
            raise StepError(
                f"Failed to copy workload to JM (exit {jm_result.returncode}):\n"
                f"  Command: docker cp {remote_dir}/. flink-jm:/opt/flink/usrlib\n"
                f"  stdout: {jm_result.stdout[:500]}\n"
                f"  stderr: {jm_result.stderr[:500]}"
            )
        executor.run(
            "docker exec -u root flink-jm chown -R flink:flink /opt/flink/usrlib",
            timeout=15,
        )

        for i in range(1, 3):  # tm1, tm2
            tm_result = executor.run(
                f"docker cp {remote_dir}/. flink-tm{i}:/opt/flink/usrlib"
            )
            if tm_result.returncode != 0:
                raise StepError(
                    f"Failed to copy workload to TM{i} (exit {tm_result.returncode}):\n"
                    f"  Command: docker cp {remote_dir}/. flink-tm{i}:/opt/flink/usrlib\n"
                    f"  stdout: {tm_result.stdout[:500]}\n"
                    f"  stderr: {tm_result.stderr[:500]}"
                )
            executor.run(
                f"docker exec -u root flink-tm{i} chown -R flink:flink /opt/flink/usrlib",
                timeout=15,
            )

        # Build JAR inside container if missing (after docker cp).
        build_sh = local_dir / "java-udf" / "build.sh"
        jar_name = "FlinkDemo-1.0-SNAPSHOT.jar"
        jar_local = local_dir / "java-udf" / jar_name
        if build_sh.exists() and not jar_local.exists():
            logger.info("JAR not found locally, building inside container...")
            result = executor.run(
                f"docker exec flink-jm bash -c "
                f"'cd /opt/flink/usrlib/java-udf && bash build.sh'",
                timeout=120,
                stream=True,
            )
            if result.returncode != 0:
                raise StepError(
                    f"Container build failed (exit {result.returncode}):\n"
                    f"  Command: docker exec flink-jm bash -c 'cd /opt/flink/usrlib/java-udf && bash build.sh'\n"
                    f"  output: {result.stdout[-2000:]}"
                )
        return WorkloadHandle(env_dir=run_dir / platform)

    def run_benchmark(
        self,
        project_path: Path,
        run_dir: Path,
        platform: str,
        *,
        force: bool = False,
    ) -> Path:
        """Run the PyFlink benchmark (perf-wrapped queries + timing).

        Owns the framework-specific benchmark logic, moved verbatim from
        orchestrator._run_benchmark's pyflink path (Phase 3 extraction).
        """
        import time

        from ...config import get_workload_config, load_environment_config
        from ...remote import build_executor, get_platform_host_ref

        workload = get_workload_config(project_path)
        queries = workload.get("queries", [])
        rows = workload.get("rows", 10_000_000)
        env_config = load_environment_config(project_path)
        host_ref = get_platform_host_ref(env_config, platform)
        executor = build_executor(host_ref, env_config)

        python_bin = _bench.find_container_python(executor, env_config)
        _bench.ensure_jar(executor)

        platform_run_dir = run_dir / platform
        platform_run_dir.mkdir(parents=True, exist_ok=True)
        timing_path = platform_run_dir / "timing" / "timing-normalized.json"
        tm_count = _bench.parse_tm_count(env_config)

        if force:
            self.deploy_workload(project_path, run_dir, platform)
            removed = []
            for old in [
                platform_run_dir / "timing" / "timing-normalized.json",
                platform_run_dir / "timing" / "timing-raw.json",
                platform_run_dir / "perf" / "data" / f"perf-{platform}.data",
            ]:
                if old.exists():
                    old.unlink()
                    removed.append(old.relative_to(run_dir))
            if removed:
                logger.info("[5a] Force: removed old artifacts: %s", removed)

        # --- Sub-step: run benchmark with perf (artifact: timing-normalized.json) ---
        if timing_path.exists() and timing_path.stat().st_size > 0:
            logger.info("[5a] timing-normalized.json exists, skipping benchmark on %s", platform)
        else:
            if not queries:
                raise StepError("No queries configured")

            logger.info("[5a] Deploying perf wrapper on %s...", platform)
            _bench.ensure_container_perf(executor, tm_count, include_jm=True)
            _bench.ensure_pyflink_runner(executor, python_bin, tm_count)
            perf_binary = _bench.find_container_perf(executor)
            _bench.deploy_perf_wrapper(executor, tm_count, python_bin, perf_binary, include_jm=True)

            wall_clock_times: dict[str, dict] = {}

            for query in queries:
                logger.info("[5a] Running query %s on %s...", query, platform)
                result = executor.run(
                    f"docker exec flink-jm {python_bin} "
                    f"/opt/flink/usrlib/benchmark_runner.py "
                    f"--query {query} --rows {rows} "
                    f"--python-executable /tmp/_perf_python_wrapper.sh",
                    timeout=300,
                    stream=True,
                )
                if result.returncode != 0:
                    raise StepError(
                        f"Benchmark {query} failed (exit {result.returncode}):\n"
                        f"  Command: docker exec flink-jm {python_bin} benchmark_runner.py --query {query} --rows {rows}\n"
                        f"  output: {result.stdout[-2000:]}"
                    )

                wc = _bench.parse_benchmark_result(result.stdout, query)
                if wc:
                    wall_clock_times[query] = wc
                    logger.info("  %s: wall-clock %.3fs, throughput %s rows/s",
                                query, wc["wallClockSeconds"], wc.get("throughputRowsPerSec", "-"))

                summary_found = _bench.parse_benchmark_summary(
                    result.stdout, query, wall_clock_times,
                )
                if not summary_found:
                    _bench.collect_operator_timing(executor, tm_count, query, wall_clock_times)

                # Collect container logs.
                jm_logs = executor.docker_logs("flink-jm", tail=200)
                (platform_run_dir / "tm-stdout-jm.log").write_text(jm_logs, encoding="utf-8")
                for i in range(1, tm_count + 1):
                    logs = executor.docker_logs(f"flink-tm{i}", tail=50)
                    (platform_run_dir / f"tm-stdout-tm{i}.log").write_text(logs, encoding="utf-8")

            _bench.merge_wall_clock_times(platform_run_dir, platform, wall_clock_times)

            # Verify perf.data was created (check JM first for local mode, then TMs).
            perf_check = None
            perf_container = None
            for c in ["flink-jm"] + [f"flink-tm{i}" for i in range(1, tm_count + 1)]:
                check = executor.run(
                    f"docker exec {c} bash -c "
                    "'ls -lh /tmp/perf-udf.data 2>&1'",
                    timeout=15,
                )
                if check.returncode == 0 and "No such file" not in check.stdout:
                    perf_check = check
                    perf_container = c
                    break
            if perf_check is None:
                raise StepError(
                    f"[5a] perf.data was not found in any container (JM + TMs) after "
                    f"running {len(queries)} queries.  Last check: {check.stdout.strip()}"
                )
            logger.info("[5a] perf.data verified in %s: %s", perf_container, perf_check.stdout.strip())
        return timing_path

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
        return None

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
