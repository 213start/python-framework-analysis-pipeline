"""Sub-step 5b: Perf profile data collection.

Invokes the analyze pipeline (formerly python-performance-kits, now
``pyframework_pipeline.analyze``) to process perf.data files into
classified CSV summaries with CPython-centric category mapping.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from .manifest import AcquisitionManifest, AcquisitionSection

# The analyze subpackage ships the perf_insights scripts; the single-platform
# entry module drives them via sibling file-path subprocess calls (resolved
# with Path(__file__).with_name(...), which works inside the package).
_PIPELINE_MODULE = "pyframework_pipeline.analyze.run_single_platform_pipeline"

# Category mapping: analyze 14 top-level -> framework L1.
# (CPython domain knowledge; Phase 2 will single-source this.)
CATEGORY_MAP = {
    "Interpreter": "Interpreter",
    "Memory": "Memory",
    "GC": "GC",
    "Tuple": "Object Model",
    "Dict": "Object Model",
    "List": "Object Model",
    "Set": "Object Model",
    "Misc Objects": "Object Model",
    "Int": "Type Operations",
    "Float": "Type Operations",
    "String": "Type Operations",
    "Calls": "Calls / Dispatch",
    "Dynamic": "Calls / Dispatch",
    "Import": "Interpreter",
    "Compiler": "Interpreter",
    "Concurrency": "Native Boundary",
    "Library": "Native Boundary",
    "Kernel": "Kernel",
    "Unknown": "Unknown",
}


def collect_perf(
    run_dir: Path,
    platform: str,
    perf_data: Path | None = None,
    kits_dir: Path | None = None,  # deprecated; the analyze subpackage is now the source
    benchmark: str = "tpch",
    platform_id: str = "",
    top_n: int = 50,
) -> dict[str, Any]:
    """Process perf.data through the analyze pipeline.

    Parameters
    ----------
    run_dir : Path
        The run output directory.
    platform : str
        Platform identifier (arm/x86).
    perf_data : Path | None
        Path to perf.data file. If None, looks in run_dir/perf.data.
    kits_dir : Path | None
        Deprecated. Kept for CLI/API backwards-compatibility; the analyze
        subpackage (``pyframework_pipeline.analyze``) is now the single
        source of the pipeline scripts and is invoked as a module.
    benchmark : str
        Benchmark name for metadata.
    platform_id : str
        Platform ID for the pipeline.
    top_n : int
        Number of top hotspots to annotate.

    Returns
    -------
    dict with file paths to generated CSV outputs.
    """
    if perf_data is None:
        perf_data = run_dir / "perf.data"

    if not perf_data.exists():
        return {"status": "skipped", "reason": f"perf.data not found: {perf_data}"}

    perf_dir = run_dir / "perf"
    perf_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        _PIPELINE_MODULE,
        str(perf_data),
        "--output", str(perf_dir),
        "--benchmark", benchmark,
        "--platform", platform_id or platform,
    ]
    if top_n:
        cmd.extend(["--top-n", str(top_n)])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        return {
            "status": "failed",
            "reason": f"analyze pipeline failed (exit {result.returncode})",
            "stderr": result.stderr[:500],
        }

    # Collect output files
    files = {}
    for name in ["category_summary.csv", "shared_object_summary.csv",
                  "symbol_hotspots.csv", "ip_hotspots.csv",
                  "instruction_hotspots.csv"]:
        f = perf_dir / "tables" / name
        if f.exists():
            files[name] = str(f.relative_to(run_dir))

    return {
        "status": "collected",
        "files": files,
    }
