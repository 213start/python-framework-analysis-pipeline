"""Sub-step 5c: Machine code / assembly collection.

Uses perf annotate for instruction-level profiling and objdump for
full binary disassembly.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .manifest import AcquisitionManifest, AcquisitionSection

DEFAULT_KITS_DIR = Path(__file__).resolve().parents[4] / "vendor" / "python-performance-kits"


def collect_asm(
    run_dir: Path,
    platform: str,
    perf_data: Path | None = None,
    kits_dir: Path | None = None,
    binaries: list[Path] | None = None,
    top_n: int = 20,
) -> dict[str, Any]:
    """Collect machine code annotations and binary dumps.

    Parameters
    ----------
    run_dir : Path
        The run output directory.
    platform : str
        Platform identifier.
    perf_data : Path | None
        Path to perf.data file.
    kits_dir : Path | None
        Path to python-performance-kits.
    binaries : list[Path] | None
        Binary files to objdump (e.g. libpython3.14.so).
    top_n : int
        Number of top hotspots to annotate.

    Returns
    -------
    dict with asm file paths and metadata.
    """
    if kits_dir is None:
        kits_dir = DEFAULT_KITS_DIR
    if perf_data is None:
        perf_data = run_dir / "perf.data"

    asm_dir = run_dir / "asm" / platform
    objdump_dir = run_dir / "asm" / "objdump"
    asm_dir.mkdir(parents=True, exist_ok=True)
    objdump_dir.mkdir(parents=True, exist_ok=True)

    hotspot_files = []
    objdump_files = []

    # Use python-performance-kits annotate script for hotspots
    if perf_data.exists():
        annotate_script = (
            kits_dir / "scripts" / "perf_insights" / "annotate_perf_hotspots.py"
        )
        if annotate_script.exists():
            # Read hotspot symbols from perf output if available
            records_csv = run_dir / "perf" / "data" / "perf_records.csv"
            if records_csv.exists():
                import sys as _sys
                cmd = [
                    _sys.executable,
                    str(annotate_script),
                    str(records_csv),
                    "--perf-data", str(perf_data),
                    "--output", str(asm_dir.parent),
                    "--top-n", str(top_n),
                ]
                subprocess.run(cmd, capture_output=True, text=True, check=False)

                # Collect generated .s files
                for f in sorted(asm_dir.glob("*.s")):
                    hotspot_files.append(str(f.relative_to(run_dir)))
                for f in sorted((asm_dir.parent / "tables").glob("instruction_hotspots.csv")):
                    hotspot_files.append(str(f.relative_to(run_dir)))

    # objdump for specified binaries
    if binaries:
        for binary in binaries:
            if binary.exists():
                out_name = binary.name + ".dump"
                out_path = objdump_dir / out_name
                result = subprocess.run(
                    ["objdump", "-d", "-C", str(binary)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode == 0:
                    out_path.write_text(result.stdout, encoding="utf-8")
                    objdump_files.append(str(out_path.relative_to(run_dir)))

    return {
        "status": "collected" if (hotspot_files or objdump_files) else "skipped",
        "hotspotCount": len(hotspot_files),
        "hotspotFiles": hotspot_files,
        "objdumpFiles": objdump_files,
    }
