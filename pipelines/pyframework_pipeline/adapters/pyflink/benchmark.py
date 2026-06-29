"""PyFlink benchmark + workload-deploy logic (extracted from orchestrator).

Owns the framework-specific deploy/benchmark/perf-wrapper logic for pyflink,
moved verbatim out of orchestrator.py (Phase 3 extraction) so PyFlinkAdapter is
the single source of pyflink acquisition behaviour, not a shim.

The functions here are module-level (not adapter methods) so they can be unit-
tested and reused; PyFlinkAdapter.deploy_workload / run_benchmark call them.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ...contracts.step import StepError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Benchmark stdout parsing
# ---------------------------------------------------------------------------

def parse_benchmark_result(stdout: str, query_id: str) -> dict | None:
    """Parse BENCHMARK_RESULT JSON from benchmark_runner.py stdout."""
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if '"BENCHMARK_RESULT"' in line:
            try:
                data = json.loads(line)
                if data.get("type") == "BENCHMARK_RESULT":
                    return data
            except json.JSONDecodeError:
                continue
    return None


def parse_benchmark_summary(
    stdout: str,
    query_id: str,
    wall_clock_times: dict[str, dict],
) -> bool:
    """Parse [BENCHMARK_SUMMARY] from benchmark_runner.py stdout.

    Returns True if a summary was found and merged.
    """
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if "BENCHMARK_SUMMARY" not in line:
            continue
        try:
            json_str = line.split("BENCHMARK_SUMMARY] ", 1)[1].strip()
            stats = json.loads(json_str)
            wc = wall_clock_times.get(query_id, {})
            wc["recordCount"] = wc.get("recordCount", 0) + stats.get("recordCount", 0)
            wc["totalPyDurationNs"] = wc.get("totalPyDurationNs", 0) + stats.get("totalPyDurationNs", 0)
            wc["totalFrameworkOverheadNs"] = (
                wc.get("totalFrameworkOverheadNs", 0)
                + stats.get("totalFrameworkOverheadNs", 0)
            )
            wall_clock_times[query_id] = wc
            logger.info("  %s: %d records, py=%d ns, fw=%d ns",
                        query_id, stats.get("recordCount", 0),
                        stats.get("totalPyDurationNs", 0),
                        stats.get("totalFrameworkOverheadNs", 0))
            return True
        except (json.JSONDecodeError, IndexError):
            return False
    return False


def collect_operator_timing(
    executor: Any,
    tm_count: int,
    query_id: str,
    wall_clock_times: dict[str, dict],
) -> None:
    """Collect operator/framework timing from docker logs (remote cluster fallback)."""
    containers = ["flink-jm"] + [f"flink-tm{i}" for i in range(1, tm_count + 1)]
    for c in containers:
        label = "JM" if c == "flink-jm" else c.replace("flink-", "").upper()
        result = executor.run(
            f"docker logs {c} 2>&1 | grep BENCHMARK_SUMMARY | tail -1",
            timeout=120,
        )
        if result.returncode == 0 and "BENCHMARK_SUMMARY" in (result.stdout or ""):
            try:
                line = result.stdout.strip()
                json_str = line.split("BENCHMARK_SUMMARY] ", 1)[1].strip()
                stats = json.loads(json_str)
                wc = wall_clock_times.get(query_id, {})
                wc["recordCount"] = wc.get("recordCount", 0) + stats.get("recordCount", 0)
                wc["totalPyDurationNs"] = wc.get("totalPyDurationNs", 0) + stats.get("totalPyDurationNs", 0)
                wc["totalFrameworkOverheadNs"] = (
                    wc.get("totalFrameworkOverheadNs", 0)
                    + stats.get("totalFrameworkOverheadNs", 0)
                )
                wall_clock_times[query_id] = wc
                logger.info("  %s %s: %d records, py=%d ns, fw=%d ns",
                            query_id, label, stats.get("recordCount", 0),
                            stats.get("totalPyDurationNs", 0),
                            stats.get("totalFrameworkOverheadNs", 0))
                break
            except (json.JSONDecodeError, IndexError):
                pass


def merge_wall_clock_times(
    platform_run_dir: Path,
    platform: str,
    wall_clock_times: dict[str, dict],
) -> None:
    """Merge wall-clock timing into timing/timing-normalized.json."""
    timing_path = platform_run_dir / "timing" / "timing-normalized.json"
    timing_path.parent.mkdir(parents=True, exist_ok=True)

    if timing_path.exists():
        data = json.loads(timing_path.read_text(encoding="utf-8"))
    else:
        data = {"schemaVersion": 1, "platform": platform, "cases": []}

    cases_by_id = {c["caseId"]: c for c in data.get("cases", [])}

    for query_id, wc in wall_clock_times.items():
        case = cases_by_id.get(query_id)
        if case is None:
            case = {"caseId": query_id, "metrics": {}}
            data.setdefault("cases", []).append(case)
            cases_by_id[query_id] = case

        wall_clock_ns = int(wc["wallClockSeconds"] * 1e9)
        case["metrics"]["wallClockTime"] = {"wall_clock_ns": wall_clock_ns}
        case["metrics"]["tmE2eTime"] = {"wall_clock_ns": wall_clock_ns}

        record_count = wc.get("recordCount", 0)
        bundle_size = wc.get("bundleSize", 0)
        py_ns = wc.get("totalPyDurationNs", 0)
        fw_ns = wc.get("totalFrameworkOverheadNs", 0)
        if py_ns > 0:
            case["metrics"]["businessOperatorTime"] = {
                "total_ns": py_ns,
                **({"per_invocation_ns": py_ns / record_count} if record_count else {}),
            }
        if fw_ns > 0:
            actual_fw = fw_ns // bundle_size if bundle_size else fw_ns
            case["metrics"]["frameworkCallTime"] = {
                "total_ns": actual_fw,
                **({"per_invocation_ns": actual_fw / record_count} if record_count else {}),
            }

    timing_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("Wrote wall-clock timing for %d queries to %s",
                len(wall_clock_times), timing_path.relative_to(platform_run_dir))


# ---------------------------------------------------------------------------
# Container discovery + perf-wrapper deploy
# ---------------------------------------------------------------------------

def parse_tm_count(env_config: dict) -> int:
    """Parse TM count from environment.yaml software.clusterTopology (e.g. '1jm-2tm')."""
    software = env_config.get("software", {})
    topology = software.get("clusterTopology", "")
    if "-" in topology:
        parts = topology.split("-")
        if len(parts) >= 2 and parts[-1].endswith("tm"):
            try:
                return int(parts[-1].rstrip("tm"))
            except ValueError:
                pass
    return 2  # fallback


def find_container_python(
    executor: Any,
    env_config: dict | None = None,
    container: str = "flink-jm",
) -> str:
    """Find the Python binary path inside a benchmark container."""
    py_version = "3.14.3"
    if env_config:
        py_version = env_config.get("software", {}).get("pythonVersion", py_version)
    expected = f"/root/.pyenv/versions/{py_version}/bin/python3"
    result = executor.run(
        f"docker exec {container} ls {expected}",
        timeout=15,
    )
    if result.returncode == 0 and result.stdout.strip():
        return expected
    result = executor.run(
        f"docker exec {container} bash -c "
        "'ls /root/.pyenv/versions/*/bin/python3 2>/dev/null | sort -V | tail -1'",
        timeout=15,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    result = executor.run(
        f"docker exec {container} bash -c 'command -v python3 || command -v python'",
        timeout=15,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "python3"


def ensure_jar(executor: Any) -> None:
    """Ensure Java UDF JAR exists inside JM container, build if missing."""
    jar_path = "/opt/flink/usrlib/java-udf/FlinkDemo-1.0-SNAPSHOT.jar"
    check = executor.run(
        f"docker exec flink-jm ls {jar_path}",
        timeout=15,
    )
    if check.returncode == 0 and check.stdout.strip():
        return
    logger.info("[5a] JAR not found, building inside JM container...")
    result = executor.run(
        "docker exec flink-jm bash -c "
        "'cd /opt/flink/usrlib/java-udf && bash build.sh'",
        timeout=120,
        stream=True,
    )
    if result.returncode != 0:
        raise StepError(
            f"JAR build failed (exit {result.returncode}):\n"
            f"  output: {result.stdout[-2000:]}"
        )


def ensure_container_perf(
    executor: Any,
    tm_count: int,
    include_jm: bool = False,
) -> str:
    """Verify perf is available in containers (installed during image build)."""
    containers = []
    if include_jm:
        containers.append("flink-jm")
    containers.extend(f"flink-tm{i}" for i in range(1, tm_count + 1))

    for c in containers:
        check = executor.run(
            f"docker exec {c} bash -c "
            "'ls /usr/lib/linux-tools-*/perf 2>/dev/null | sort -V | tail -1'",
            timeout=30,
        )
        if check.returncode != 0 or not check.stdout.strip():
            raise StepError(
                f"perf not found in {c}. linux-tools must be installed "
                f"during image build (build-flink-image.sh Phase 5).\n"
                f"  stdout: {check.stdout[:500]}\n"
                f"  stderr: {check.stderr[:500]}"
            )
    return executor.run(
        "docker exec flink-tm1 bash -c "
        "'ls /usr/lib/linux-tools-*/perf 2>/dev/null | sort -V | tail -1'",
        timeout=30,
    ).stdout.strip()


def ensure_pyflink_runner(
    executor: Any,
    python_bin: str,
    tm_count: int,
) -> None:
    """Ensure pyflink-udf-runner.sh exists in all containers."""
    check = executor.run(
        f"docker exec flink-jm {python_bin} -c "
        "'import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), \"bin\", \"pyflink-udf-runner.sh\"))'",
        timeout=15,
    )
    if check.returncode != 0:
        logger.warning("[5a] Could not locate pyflink package dir: %s", check.stderr.strip())
        return

    runner_path = check.stdout.strip()
    containers = ["flink-jm"] + [f"flink-tm{i}" for i in range(1, tm_count + 1)]

    for c in containers:
        exists = executor.run(f"docker exec {c} test -f {runner_path}", timeout=10)
        if exists.returncode == 0:
            logger.info("[5a] pyflink-udf-runner.sh exists on %s", c)
            continue

        logger.info("[5a] pyflink-udf-runner.sh missing on %s, creating", c)
        import base64

        runner_script = (
            '#!/usr/bin/env bash\n'
            'python=${python:-python}\n'
            'if [ -n "$_PYTHON_WORKING_DIR" ]; then\n'
            '    cd "$_PYTHON_WORKING_DIR"\n'
            '    if [[ "$python" == ${_PYTHON_WORKING_DIR}* ]]; then\n'
            '        chmod +x "$python"\n'
            '    fi\n'
            'fi\n'
            'log="${BOOT_LOG_DIR}/flink-python-udf-boot.log"\n'
            '${python} -m pyflink.fn_execution.beam.beam_boot "$@" 2>&1 | tee ${log}\n'
        )
        encoded = base64.b64encode(runner_script.encode()).decode()
        bin_dir = runner_path.rsplit("/", 1)[0]
        executor.run(
            f"docker exec {c} bash -c "
            f"'mkdir -p {bin_dir} && "
            f"echo {encoded} | base64 -d > {runner_path} && "
            f"chmod +x {runner_path}'",
            timeout=15,
        )
        logger.info("[5a] Created pyflink-udf-runner.sh on %s", c)


def find_container_perf(
    executor: Any,
    container: str = "flink-tm1",
) -> str:
    """Find the perf binary path inside a benchmark container."""
    result = executor.run(
        f"docker exec {container} bash -c "
        "'ls /usr/lib/linux-tools-*/perf 2>/dev/null | sort -V | tail -1'",
        timeout=30,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    result = executor.run(
        f"docker exec {container} bash -c 'command -v perf'",
        timeout=30,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "/usr/bin/perf"


def deploy_perf_wrapper(
    executor: Any,
    tm_count: int,
    python_bin: str,
    perf_binary: str,
    include_jm: bool = False,
) -> None:
    """Deploy perf wrapper script to containers."""
    import base64

    script = (
        "#!/bin/bash\n"
        "export OPENBLAS_NUM_THREADS=1\n"
        "export OMP_NUM_THREADS=1\n"
        "export MKL_NUM_THREADS=1\n"
        "export NUMEXPR_NUM_THREADS=1\n"
        f"exec {perf_binary} record -F 999 -g -e task-clock "
        f"-o /tmp/perf-udf.data -- {python_bin} \"$@\" 2>/dev/null\n"
    )
    encoded = base64.b64encode(script.encode()).decode()

    wrapper_path = "/tmp/_perf_python_wrapper.sh"
    containers = []
    if include_jm:
        containers.append("flink-jm")
    containers.extend(f"flink-tm{i}" for i in range(1, tm_count + 1))
    for c in containers:
        executor.run(
            f"docker exec {c} rm -f /tmp/perf-udf.data",
            timeout=30,
        )
        executor.run(
            f"docker exec {c} bash -c "
            f"'echo {encoded} | base64 -d > {wrapper_path} && "
            f"chmod +x {wrapper_path}'",
            timeout=30,
        )


__all__ = [
    "parse_benchmark_result",
    "parse_benchmark_summary",
    "collect_operator_timing",
    "merge_wall_clock_times",
    "parse_tm_count",
    "find_container_python",
    "ensure_jar",
    "ensure_container_perf",
    "ensure_pyflink_runner",
    "find_container_perf",
    "deploy_perf_wrapper",
]
