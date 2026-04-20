"""Step-by-step pipeline orchestrator with resumability.

Chains steps 3 through 7 for all configured platforms, tracking state
in pipeline-run.json for resume-from-failure.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step registry
# ---------------------------------------------------------------------------

STEP_DEFS: list[dict[str, str]] = [
    {"step": "3",  "name": "environment deploy"},
    {"step": "4",  "name": "workload deploy"},
    {"step": "5a", "name": "benchmark run"},
    {"step": "5b", "name": "collect"},
    {"step": "5c", "name": "acquire all"},
    {"step": "6",  "name": "backfill run"},
    {"step": "7",  "name": "bridge publish"},
]

# Steps that run per-platform (need --platform).
PER_PLATFORM_STEPS = {"3", "4", "5a", "5b"}

# Steps that run once after all platforms.
GLOBAL_STEPS = {"5c", "6", "7"}


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


class PipelineRunState:
    """Tracks pipeline run state in pipeline-run.json."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self.data = {}

    def init(self, project_id: str, platforms: list[str]) -> None:
        if not self.data or not self.data.get("steps"):
            self.data = {
                "runId": _run_id(),
                "projectId": project_id,
                "platforms": platforms,
                "steps": [],
            }

    def is_completed(self, step: str, platform: str | None = None) -> bool:
        for s in self.data.get("steps", []):
            if s["step"] == step and s.get("platform") == platform:
                return s["status"] == "completed"
        return False

    def mark_running(self, step: str, platform: str | None = None) -> None:
        self.data.setdefault("steps", []).append({
            "step": step,
            "name": next(
                (d["name"] for d in STEP_DEFS if d["step"] == step), step
            ),
            "platform": platform,
            "status": "running",
            "startedAt": _now_iso(),
        })
        self._save()

    def mark_completed(self, step: str, platform: str | None = None) -> None:
        for s in reversed(self.data.get("steps", [])):
            if s["step"] == step and s.get("platform") == platform and s["status"] == "running":
                s["status"] = "completed"
                s["completedAt"] = _now_iso()
                break
        self._save()

    def mark_failed(
        self, step: str, platform: str | None = None, error: str = "",
    ) -> None:
        for s in reversed(self.data.get("steps", [])):
            if s["step"] == step and s.get("platform") == platform and s["status"] == "running":
                s["status"] = "failed"
                s["error"] = error
                s["completedAt"] = _now_iso()
                break
        self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    project_path: Path,
    run_dir: Path,
    *,
    resume_from: str | None = None,
    stop_before: str | None = None,
    force: bool = False,
    yes: bool = False,
) -> int:
    """Execute the full pipeline from step 3 to 7.

    Returns 0 on success, 1 on failure.
    """
    from .config import (
        get_run_config,
        get_workload_config,
        get_bridge_config,
        load_project_config,
    )

    config = load_project_config(project_path)
    project_id = config.get("id", "unknown")
    run_config = get_run_config(project_path)
    platforms = run_config.get("platforms", [])

    state_path = run_dir / "pipeline-run.json"
    state = PipelineRunState(state_path)

    if force:
        state.data = {}

    state.init(project_id, platforms)

    # Determine start point.
    step_ids = [d["step"] for d in STEP_DEFS]
    start_idx = 0
    if resume_from:
        if resume_from in step_ids:
            start_idx = step_ids.index(resume_from)
        else:
            logger.error("Unknown step: %s. Valid: %s", resume_from, step_ids)
            return 1

    stop_idx = len(STEP_DEFS)
    if stop_before:
        if stop_before in step_ids:
            stop_idx = step_ids.index(stop_before)
        else:
            logger.error("Unknown step: %s. Valid: %s", stop_before, step_ids)
            return 1

    for idx in range(start_idx, stop_idx):
        step_def = STEP_DEFS[idx]
        step_id = step_def["step"]
        step_name = step_def["name"]

        if step_id in PER_PLATFORM_STEPS:
            # Run for each platform.
            for plat in platforms:
                if state.is_completed(step_id, plat):
                    logger.info("[S%s/%s] Already completed, skipping", step_id, plat)
                    continue

                state.mark_running(step_id, plat)
                logger.info("[S%s/%s] %s", step_id, plat, step_name)

                try:
                    _execute_step(
                        step_id, project_path, run_dir, plat, yes=yes,
                    )
                    state.mark_completed(step_id, plat)
                except StepError as exc:
                    state.mark_failed(step_id, plat, str(exc))
                    _print_resume_hint(step_id, plat, project_path)
                    return 1

        elif step_id in GLOBAL_STEPS:
            if state.is_completed(step_id):
                logger.info("[S%s] Already completed, skipping", step_id)
                continue

            state.mark_running(step_id)
            logger.info("[S%s] %s", step_id, step_name)

            try:
                _execute_step(
                    step_id, project_path, run_dir, None, yes=yes,
                )
                state.mark_completed(step_id)
            except StepError as exc:
                state.mark_failed(step_id, error=str(exc))
                _print_resume_hint(step_id, None, project_path)
                return 1

    logger.info("Pipeline completed successfully")
    return 0


class StepError(Exception):
    """Raised when a pipeline step fails."""


def _execute_step(
    step_id: str,
    project_path: Path,
    run_dir: Path,
    platform: str | None,
    *,
    yes: bool = False,
) -> None:
    """Execute a single pipeline step."""
    if step_id == "3":
        from .environment.deploy import deploy_plan
        plan_path = run_dir / platform / "environment-plan.json"
        result = deploy_plan(project_path, platform, plan_path, yes=yes)
        # Save record.
        record = result.get("record", {})
        if record:
            record_dir = run_dir / platform
            record_dir.mkdir(parents=True, exist_ok=True)
            (record_dir / "environment-record.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        if result.get("status") == "failed":
            raise StepError(f"environment deploy failed: {result.get('failed', 0)} steps failed")

    elif step_id == "4":
        _run_workload_deploy(project_path, run_dir, platform, yes=yes)

    elif step_id == "5a":
        _run_benchmark(project_path, run_dir, platform)

    elif step_id == "5b":
        _run_collect(project_path, run_dir, platform)

    elif step_id == "5c":
        _run_acquire_all(project_path, run_dir)

    elif step_id == "6":
        _run_backfill(project_path, run_dir)

    elif step_id == "7":
        _run_bridge_publish(project_path)

    else:
        raise StepError(f"Unknown step: {step_id}")


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def _run_workload_deploy(
    project_path: Path, run_dir: Path, platform: str, *, yes: bool = False,
) -> None:
    from .config import get_workload_config, load_environment_config
    from .remote import build_executor, get_platform_host_ref

    workload = get_workload_config(project_path)
    local_dir = project_path.parent / workload["localDir"]

    if not local_dir.exists():
        raise StepError(f"Workload directory not found: {local_dir}")

    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)

    # Upload workload to remote.
    remote_dir = "/tmp/pyframework-workload"
    logger.info("Uploading %s to %s:%s", local_dir, host_ref, remote_dir)
    ok = executor.push_dir(local_dir, remote_dir)
    if not ok:
        raise StepError(f"Failed to upload workload to {host_ref}")

    # If container build mode, compile JAR inside container.
    if workload.get("build") == "container":
        logger.info("Building JAR inside container...")
        result = executor.run(
            f"docker exec flink-jm bash -c "
            f"'cd {remote_dir} && ./build.sh'",
            timeout=120,
        )
        if result.returncode != 0:
            raise StepError(f"Container build failed: {result.stderr}")

    # Distribute to containers via docker cp.
    jm_result = executor.run(f"docker cp {remote_dir}/. flink-jm:/opt/flink/usrlib")
    if jm_result.returncode != 0:
        raise StepError(f"Failed to copy workload to JM: {jm_result.stderr}")

    for i in range(1, 3):  # tm1, tm2
        tm_result = executor.run(
            f"docker cp {remote_dir}/. flink-tm{i}:/opt/flink/usrlib"
        )
        if tm_result.returncode != 0:
            logger.warning("Failed to copy to flink-tm%d: %s", i, tm_result.stderr)


def _run_benchmark(
    project_path: Path, run_dir: Path, platform: str,
) -> None:
    from .config import get_workload_config, load_environment_config
    from .remote import build_executor, get_platform_host_ref

    workload = get_workload_config(project_path)
    queries = workload.get("queries", [])
    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)

    # Start perf record on TM containers (background).
    for i in range(1, 3):
        executor.run(
            f"docker exec -d flink-tm{i} /usr/local/bin/perf record -g -o /tmp/perf.data -a",
            timeout=10,
        )

    # Run benchmarks.
    platform_run_dir = run_dir / platform
    platform_run_dir.mkdir(parents=True, exist_ok=True)

    for query in queries:
        logger.info("Running query %s on %s...", query, platform)
        result = executor.run(
            f"docker exec flink-jm python3 /opt/flink/usrlib/benchmark_runner.py "
            f"--query {query} --rows 100000",
            timeout=300,
        )
        if result.returncode != 0:
            raise StepError(f"Benchmark {query} failed: {result.stderr[:200]}")

        # Save TM stdout.
        for i in range(1, 3):
            logs = executor.docker_logs(f"flink-tm{i}")
            log_path = platform_run_dir / f"tm-stdout-tm{i}.log"
            log_path.write_text(logs, encoding="utf-8")

    # Stop perf.
    for i in range(1, 3):
        executor.run(
            f"docker exec flink-tm{i} bash -c 'kill -INT $(pidof perf) || true'",
            timeout=10,
        )


def _run_collect(
    project_path: Path, run_dir: Path, platform: str,
) -> None:
    from .config import load_environment_config
    from .remote import build_executor, get_platform_host_ref

    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)

    platform_run_dir = run_dir / platform

    # Collect perf.data from TM1.
    perf_dir = platform_run_dir / "perf" / "data"
    perf_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Collecting perf.data from flink-tm1...")
    result = executor.run("docker exec flink-tm1 cat /tmp/perf.data", timeout=60)
    if result.returncode == 0 and result.stdout:
        (perf_dir / "perf.data").write_bytes(result.stdout.encode("utf-8", errors="replace"))
    else:
        logger.warning("Failed to collect perf.data: %s", result.stderr)

    # Collect objdump from JM.
    asm_dir = platform_run_dir / "asm" / ("arm64" if platform == "arm" else "x86_64")
    asm_dir.mkdir(parents=True, exist_ok=True)
    libpython = "/opt/flink/.pyenv/versions/3.14.3/lib/libpython3.14.so.1.0"
    for sym in ["_PyObject_Malloc", "_PyEval_EvalFrameDefault", "PyDict_GetItem"]:
        logger.info("Collecting objdump for %s...", sym)
        result = executor.run(
            f"docker exec flink-jm bash -c "
            f"'objdump -S -d {libpython} 2>/dev/null | awk \"/<{sym}>:/,/^$/\" | head -200'",
            timeout=60,
        )
        if result.returncode == 0 and result.stdout:
            (asm_dir / f"{sym}.s").write_text(result.stdout, encoding="utf-8")

    # TM logs already collected in benchmark step, just verify.
    logger.info("Collection complete for %s", platform)


def _run_acquire_all(project_path: Path, run_dir: Path) -> None:
    from .acquisition.timing import collect_timing
    from .acquisition.perf_profile import collect_perf
    from .acquisition.machine_code import collect_asm

    config = load_project_config(project_path)
    run_config = get_run_config(project_path)
    platforms = run_config.get("platforms", [])

    for plat in platforms:
        plat_dir = run_dir / plat
        stdout_files = list(plat_dir.glob("tm-stdout-*.log"))
        timing_result = collect_timing(plat_dir, plat, stdout_files or None)
        logger.info("Timing %s: %d cases", plat, len(timing_result.get("cases", [])))

        perf_data = plat_dir / "perf" / "data" / "perf.data"
        perf_result = collect_perf(
            plat_dir, plat,
            perf_data if perf_data.exists() else None,
            None,
        )
        logger.info("Perf %s: %s", plat, perf_result.get("status", "unknown"))

        asm_result = collect_asm(
            plat_dir, plat,
            perf_data if perf_data.exists() else None,
            None,
            [],
        )
        logger.info("ASM %s: %s", plat, asm_result.get("status", "unknown"))


def _run_backfill(project_path: Path, run_dir: Path) -> None:
    from .backfill.pipeline import run_backfill
    from .config import get_run_config

    run_config = get_run_config(project_path)
    platforms = run_config.get("platforms", [])

    if len(platforms) < 2:
        raise StepError("Need at least 2 platforms for backfill")

    arm_dir = run_dir / platforms[0]
    x86_dir = run_dir / platforms[1]

    rc = run_backfill(project_path, arm_dir, x86_dir)
    if rc != 0:
        raise StepError("Backfill failed")


def _run_bridge_publish(project_path: Path) -> None:
    from .bridge.analysis import publish
    from .config import get_bridge_config

    bridge_config = get_bridge_config(project_path)
    result = publish(
        project_path,
        repo=bridge_config["repo"],
        platform=bridge_config["platform"],
        token=bridge_config["token"],
    )
    if result.get("errors", 0) > 0:
        raise StepError(f"Bridge publish had {result['errors']} errors")


import sys

from .config import load_project_config, get_run_config


def _print_resume_hint(
    step_id: str, platform: str | None, project_path: Path,
) -> None:
    plat_str = f' on platform "{platform}"' if platform else ""
    step_name = next((d["name"] for d in STEP_DEFS if d["step"] == step_id), step_id)
    print(
        f"\nERROR: Step {step_id} \"{step_name}\" failed{plat_str}",
        file=sys.stderr,
    )
    print(
        f"\nResume from this step:\n"
        f"  pyframework-pipeline run {project_path} --resume-from {step_id}",
        file=sys.stderr,
    )
