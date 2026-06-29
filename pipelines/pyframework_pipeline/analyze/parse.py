"""C1 parse subflow: perf.data -> PerfRecord CSV (uncategorized).

Path-in/path-out: consumes a ``perf.data`` (and optional perf-script CSV) and
writes a normalized records CSV in the analyze NORMALIZED_FIELDS schema. The
records are uncategorized (category_* empty) — C2 fills those.

This is a thin composition layer: it drives the existing analyze scripts
(perf_data_to_csv / perf_script_to_csv / normalize_perf_records) the same way
``run_single_platform_pipeline`` does, so algorithms are unchanged. The
subflow is the programmatic equivalent of that entry point's parse stage.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from .run_single_platform_pipeline import build_output_paths

_PERF_DATA_TO_CSV = "pyframework_pipeline.analyze.perf_data_to_csv"
_PERF_SCRIPT_TO_CSV = "pyframework_pipeline.analyze.perf_script_to_csv"
_NORMALIZE = "pyframework_pipeline.analyze.normalize_perf_records"


def _run(cmd: list[str]) -> None:
    completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"parse step failed (exit {completed.returncode}): {completed.stderr.strip()[:500]}"
        )


def parse(
    perf_data: Path,
    *,
    output: Path = Path("perf_records.csv"),
    script_output: Path | None = None,
    perf_bin: str = "perf",
    platform_id: str = "",
    arch: str = "",
    python_version: str = "",
    build_id: str = "",
    benchmark: str = "",
    event: str = "cycles",
    rules: Path | None = None,
    log_level: str = "INFO",
) -> Path:
    """Parse a perf.data into normalized records (PerfRecord CSV, uncategorized).

    Parameters are the configurable inputs/outputs. Returns the records CSV path.
    Raises FileNotFoundError if ``perf_data`` does not exist.
    """
    perf_data = Path(perf_data)
    if not perf_data.exists():
        raise FileNotFoundError(f"perf.data not found: {perf_data}")

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    script_output = Path(script_output) if script_output else output.with_name("perf_script.csv")

    raw_csv = output.with_suffix(".raw.csv")

    # perf.data -> raw CSV (cycles) + perf script CSV
    _run([
        sys.executable, "-m", _PERF_DATA_TO_CSV,
        str(perf_data), "-o", str(raw_csv), "-p", perf_bin, "-l", log_level,
    ])
    _run([
        sys.executable, "-m", _PERF_SCRIPT_TO_CSV,
        str(perf_data), "-o", str(script_output), "-p", perf_bin, "-l", log_level,
    ])

    # raw CSV -> normalized records CSV
    normalize_cmd = [
        sys.executable, "-m", _NORMALIZE,
        str(raw_csv), "-o", str(output),
        "-p", platform_id, "-a", arch, "-V", python_version, "-i", build_id,
        "-b", benchmark, "-e", event, "-l", log_level,
    ]
    if rules is not None:
        normalize_cmd += ["--rules", str(rules)]
    _run(normalize_cmd)

    return output


# Re-export so callers building the full pipeline path map can reuse it.
__all__ = ["parse", "build_output_paths"]
