"""Step 6b: cross-platform performance comparison using python-performance-kits."""

from __future__ import annotations

import json
import logging
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Path to the vendor compare script (relative to repo root).
_COMPARE_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "vendor" / "python-performance-kits" / "scripts" / "perf_insights"
    / "run_compare_pipeline.py"
)


def _clip_output(value: str | None, limit: int = 4000) -> str:
    """Return a safe, bounded subprocess output snippet for logs/results."""
    return (value or "")[:limit]


def _geomean_e2e_time(run_dir: Path) -> float:
    """Compute geometric mean of query wall-clock times from timing-normalized.json.

    Returns 0.0 if timing data is not available.
    """
    timing_path = run_dir / "timing" / "timing-normalized.json"
    if not timing_path.exists():
        return 0.0
    try:
        data = json.loads(timing_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0.0

    times_ns: list[float] = []
    for case in data.get("cases", []):
        wc = case.get("metrics", {}).get("wallClockTime", {})
        ns = wc.get("wall_clock_ns")
        if ns and ns > 0:
            times_ns.append(float(ns))

    if not times_ns:
        return 0.0

    # Geometric mean in seconds.
    log_sum = sum(math.log(t) for t in times_ns)
    return math.exp(log_sum / len(times_ns)) / 1e9


def run_compare(
    project_yaml: Path,
    arm_run_dir: Path,
    x86_run_dir: Path,
    output_dir: Path | None = None,
    top_n: int = 20,
) -> dict[str, Any]:
    """Run cross-platform comparison (ARM baseline vs x86 target).

    Parameters
    ----------
    project_yaml : Path
        Path to project.yaml.
    arm_run_dir : Path
        ARM platform run directory.
    x86_run_dir : Path
        x86 platform run directory.
    output_dir : Path or None
        Output directory. Defaults to <project>/runs/<latest>/compare/.
    top_n : int
        Top N items in reports.

    Returns
    -------
    dict with comparison summary.
    """
    framework = ""
    try:
        from ..config import load_environment_config
        framework = str(load_environment_config(project_yaml).get("framework", ""))
    except Exception:
        framework = ""
    if framework == "pytorch":
        return _run_pytorch_compare(project_yaml, arm_run_dir, x86_run_dir, output_dir, top_n)

    arm_csv = arm_run_dir / "perf" / "data" / "perf_records.csv"
    x86_csv = x86_run_dir / "perf" / "data" / "perf_records.csv"

    if not arm_csv.exists():
        return {"status": "error", "reason": f"ARM perf CSV not found: {arm_csv}"}
    if not x86_csv.exists():
        return {"status": "error", "reason": f"x86 perf CSV not found: {x86_csv}"}

    if output_dir is None:
        # Default: <arm_run_dir>/../compare/
        output_dir = arm_run_dir.parent / "compare"
    output_dir.mkdir(parents=True, exist_ok=True)

    arm_e2e = _geomean_e2e_time(arm_run_dir)
    x86_e2e = _geomean_e2e_time(x86_run_dir)

    logger.info("ARM e2e time (geomean): %.2f s", arm_e2e)
    logger.info("x86 e2e time (geomean): %.2f s", x86_e2e)

    # Detect Python version from ARM timing or perf CSV.
    python_version = _detect_python_version(arm_csv)

    # Use root mode (-R/-S): each platform's perf/ dir contains data/perf_records.csv.
    # This also gives the compare report access to single-platform outputs for
    # machine code comparison.
    arm_perf_root = arm_run_dir / "perf"
    x86_perf_root = x86_run_dir / "perf"

    cmd = [
        sys.executable, str(_COMPARE_SCRIPT),
        "-R", str(arm_perf_root),
        "-S", str(x86_perf_root),
        "-x", str(arm_e2e),
        "-y", str(x86_e2e),
        "-p", "arm",
        "-q", "x86",
        "-a", "aarch64",
        "-A", "x86_64",
        "-o", str(output_dir),
        "-n", str(top_n),
        "--skip-annotate",
    ]
    if python_version:
        cmd.extend(["-V", python_version])

    logger.info("Running compare pipeline: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
        check=False,
    )
    if result.returncode != 0:
        stderr = _clip_output(result.stderr)
        stdout = _clip_output(result.stdout)
        logger.error(
            "Compare pipeline failed (exit %d): stderr=%s stdout=%s",
            result.returncode,
            stderr,
            stdout,
        )
        return {
            "status": "error",
            "returncode": result.returncode,
            "stderr": stderr,
            "stdout": stdout,
        }

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info("  compare: %s", line)

    return {
        "status": "completed",
        "output_dir": str(output_dir),
        "arm_e2e_time": arm_e2e,
        "x86_e2e_time": x86_e2e,
    }


def _run_pytorch_compare(
    project_yaml: Path,
    arm_run_dir: Path,
    x86_run_dir: Path,
    output_dir: Path | None,
    top_n: int,
) -> dict[str, Any]:
    regions = ["aot_trace_joint_graph", "fw_compiler_base", "bytecode_tracing"]
    if output_dir is None:
        output_dir = arm_run_dir.parent / "compare" / "pytorch"
    output_dir.mkdir(parents=True, exist_ok=True)

    arm_e2e = _geomean_e2e_time(arm_run_dir)
    x86_e2e = _geomean_e2e_time(x86_run_dir)
    completed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for region in regions:
        arm_root = arm_run_dir / "perf" / "pytorch" / region
        x86_root = x86_run_dir / "perf" / "pytorch" / region
        arm_csv = arm_root / "data" / "perf_records.csv"
        x86_csv = x86_root / "data" / "perf_records.csv"
        region_output = output_dir / region

        if not arm_csv.exists() or not x86_csv.exists():
            errors.append({
                "region": region,
                "reason": f"missing perf CSV: arm={arm_csv.exists()} x86={x86_csv.exists()}",
            })
            continue

        python_version = _detect_python_version(arm_csv)
        cmd = [
            sys.executable, str(_COMPARE_SCRIPT),
            "-R", str(arm_root),
            "-S", str(x86_root),
            "-x", str(arm_e2e),
            "-y", str(x86_e2e),
            "-p", "arm",
            "-q", "x86",
            "-a", "aarch64",
            "-A", "x86_64",
            "-o", str(region_output),
            "-n", str(top_n),
            "--skip-annotate",
        ]
        if python_version:
            cmd.extend(["-V", python_version])

        logger.info("Running PyTorch compare for %s: %s", region, " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
            check=False,
        )
        if result.returncode != 0:
            stderr = _clip_output(result.stderr)
            stdout = _clip_output(result.stdout)
            logger.error(
                "PyTorch compare failed for %s (exit %d): stderr=%s stdout=%s",
                region,
                result.returncode,
                stderr,
                stdout,
            )
            errors.append({
                "region": region,
                "returncode": result.returncode,
                "stderr": stderr,
                "stdout": stdout,
            })
            continue
        completed.append({
            "region": region,
            "output_dir": str(region_output),
        })

    summary = {
        "status": "completed" if completed and not errors else "partial" if completed else "error",
        "framework": "pytorch",
        "output_dir": str(output_dir),
        "arm_e2e_time": arm_e2e,
        "x86_e2e_time": x86_e2e,
        "regions": completed,
        "errors": errors,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def _detect_python_version(perf_csv: Path) -> str:
    """Extract Python version from perf_records.csv header row."""
    import csv as csv_mod
    try:
        with open(perf_csv, newline="", encoding="utf-8") as f:
            reader = csv_mod.DictReader(f)
            row = next(reader, None)
            if row:
                return row.get("python_version", "")
    except Exception:
        pass
    return ""
