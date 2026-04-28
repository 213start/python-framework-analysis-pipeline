"""Sub-step 5b: Perf profile data collection.

Invokes python-performance-kits pipeline to process perf.data files into
classified CSV summaries with CPython-centric category mapping.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from .manifest import AcquisitionManifest, AcquisitionSection

# Default path to python-performance-kits (as git submodule)
DEFAULT_KITS_DIR = Path(__file__).resolve().parents[3] / "vendor" / "python-performance-kits"

# Category mapping: python-performance-kits 14 top-level -> framework L1
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
    kits_dir: Path | None = None,
    benchmark: str = "tpch",
    platform_id: str = "",
    top_n: int = 50,
) -> dict[str, Any]:
    """Process perf.data through python-performance-kits pipeline.

    Parameters
    ----------
    run_dir : Path
        The run output directory.
    platform : str
        Platform identifier (arm/x86).
    perf_data : Path | None
        Path to perf.data file. If None, looks in run_dir/perf.data.
    kits_dir : Path | None
        Path to python-performance-kits. If None, uses default submodule path.
    benchmark : str
        Benchmark name for metadata.
    platform_id : str
        Platform ID for kits.
    top_n : int
        Number of top hotspots to annotate.

    Returns
    -------
    dict with file paths to generated CSV outputs.
    """
    if kits_dir is None:
        kits_dir = DEFAULT_KITS_DIR
    if perf_data is None:
        perf_data = run_dir / "perf.data"

    if not perf_data.exists():
        return {"status": "skipped", "reason": f"perf.data not found: {perf_data}"}

    perf_dir = run_dir / "perf"
    perf_dir.mkdir(parents=True, exist_ok=True)

    pipeline_script = kits_dir / "scripts" / "perf_insights" / "run_single_platform_pipeline.py"
    if not pipeline_script.exists():
        return {"status": "failed", "reason": f"Kits pipeline not found: {pipeline_script}"}

    cmd = [
        sys.executable,
        str(pipeline_script),
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
            "reason": f"Kits pipeline failed (exit {result.returncode})",
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
