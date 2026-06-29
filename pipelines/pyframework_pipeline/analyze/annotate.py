"""C4 annotate subflow: records + instruction data -> annotated hotspots.

Path-in/path-out: consumes a classified records CSV plus the instruction-level
data (perf annotate over perf.data), and writes the annotated records
(records_enriched.csv) and instruction_hotspots.csv.

Per the spec's C4 contract, the instruction input is supplied (from B5
acquisition or externally) — C4 correlates, it does not perform acquisition
itself. The perf.data is required only because the analyze annotate script
runs `perf annotate` against it to derive instruction samples; if perf.data is
absent the subflow is a no-op (graceful skip), leaving the records unannotated.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ANNOTATE = "pyframework_pipeline.analyze.annotate_perf_hotspots"


def _run(cmd: list[str]) -> bool:
    """Run a command; return True on success, False on non-zero (graceful)."""
    completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
    return completed.returncode == 0


def annotate(
    *,
    records_path: Path = Path("classified_records.csv"),
    perf_data: Path | None = None,
    output_dir: Path = Path("annotate"),
    perf_bin: str = "perf",
    top_n: int = 50,
    log_level: str = "INFO",
) -> Path | None:
    """Annotate records with instruction-level hotspots (path-in/path-out).

    Writes records_enriched.csv and instruction_hotspots.csv into ``output_dir``.
    Returns the output directory, or None if the step was skipped (no perf.data).
    """
    records_path = Path(records_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if perf_data is None or not Path(perf_data).exists():
        # C4 correlates instruction data; without perf.data there is nothing to
        # annotate. Graceful skip — caller keeps the unannotated records.
        return None

    cmd = [
        sys.executable, "-m", _ANNOTATE,
        str(records_path),
        "--perf-data", str(perf_data),
        "--output", str(output_dir),
        "--perf-bin", perf_bin,
        "--top-n", str(top_n),
        "-l", log_level,
    ]
    if not _run(cmd):
        # annotate is best-effort (perf annotate may fail on stripped binaries);
        # mirror run_single_platform_pipeline which continues without annotation.
        return None
    return output_dir


__all__ = ["annotate"]
