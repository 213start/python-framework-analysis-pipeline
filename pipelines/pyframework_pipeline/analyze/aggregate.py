"""C3 aggregate subflow: classified records -> summary tables.

Path-in/path-out: reads a classified_records.csv and writes the three summary
CSVs (category_summary.csv, shared_object_summary.csv, symbol_hotspots.csv)
into an output directory, using the analyze summarize_platform_perf logic.

Algorithm-true: this drives the existing summarize_platform_perf script as a
module, exactly as run_single_platform_pipeline does. The AggregatedTables
contract (contracts.tables) describes the produced files.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_SUMMARIZE = "pyframework_pipeline.analyze.summarize_platform_perf"


def _run(cmd: list[str]) -> None:
    completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"aggregate step failed (exit {completed.returncode}): {completed.stderr.strip()[:500]}"
        )


def aggregate(
    *,
    input_path: Path = Path("classified_records.csv"),
    output_dir: Path = Path("tables"),
    script_input: Path | None = None,
    log_level: str = "INFO",
) -> Path:
    """Aggregate classified records into summary tables (path-in/path-out).

    Writes category_summary.csv / shared_object_summary.csv / symbol_hotspots.csv
    (and ip_hotspots.csv if ``script_input`` is given) into ``output_dir``.
    Returns the output directory.
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", _SUMMARIZE,
        str(input_path), "-o", str(output_dir), "-l", log_level,
    ]
    if script_input is not None:
        cmd += ["--script-input", str(script_input)]
    _run(cmd)
    return output_dir


__all__ = ["aggregate"]
