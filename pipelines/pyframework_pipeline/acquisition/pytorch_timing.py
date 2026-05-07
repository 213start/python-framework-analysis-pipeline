"""Parse PyTorch ``torch._dynamo.utils.compile_times()`` text output.

PyTorch Inductor local-region profiling runs the demo twice and records three
local perf.data files.  The matching elapsed times come from the demo's
``compile_times()`` text, keyed by the corresponding ``dynamo_timed`` names.
This module converts the merged text into the common ``timing-normalized.json``
shape consumed by timing backfill.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PyTorchTimingTarget:
    case_id: str
    source_case: str
    dynamo_timed_name: str
    data_file_prefix: str


PYTORCH_TIMING_TARGETS: tuple[PyTorchTimingTarget, ...] = (
    PyTorchTimingTarget(
        case_id="aot_trace_joint_graph",
        source_case="aot_trace",
        dynamo_timed_name="aot_trace_joint_graph",
        data_file_prefix="aot_trace_joint_graph",
    ),
    PyTorchTimingTarget(
        case_id="fw_compiler_base",
        source_case="aot_trace",
        dynamo_timed_name="compile_fx.<locals>.fw_compiler_base",
        data_file_prefix="fw_compiler_base",
    ),
    PyTorchTimingTarget(
        case_id="bytecode_tracing",
        source_case="bytecode_tracing",
        dynamo_timed_name="bytecode_tracing",
        data_file_prefix="bytecode_tracing",
    ),
)

_CASE_HEADER_RE = re.compile(r"^=====\s+CASE:\s+([^=\s]+)(?:\s+.*?)?=====")
_METRIC_ROW_RE = re.compile(r"^\s*(?P<name>[^,]+?)\s*,\s*(?P<seconds>[0-9]+(?:\.[0-9]+)?)\s*$")


def collect_pytorch_timing(
    platform_run_dir: Path,
    platform: str,
    *,
    results_dir_name: str = "pytorch-results",
) -> dict[str, Any]:
    """Parse merged PyTorch compile timing and write timing JSON files.

    Parameters
    ----------
    platform_run_dir:
        Per-platform run directory, e.g. ``runs/<id>/arm``.
    platform:
        Platform identifier to embed in the output.
    results_dir_name:
        Directory under ``platform_run_dir`` containing PyTorch benchmark files.

    Returns
    -------
    dict
        Summary with status, paths, and missing targets.
    """
    results_dir = platform_run_dir / results_dir_name
    merged_txt = results_dir / "pytorch-compile-times.txt"
    if not merged_txt.is_file():
        return {
            "status": "skipped",
            "reason": f"merged PyTorch compile timing not found: {merged_txt}",
            "cases": 0,
        }

    text = merged_txt.read_text(encoding="utf-8", errors="replace")
    metrics_by_case = parse_compile_times_text(text)

    cases: list[dict[str, Any]] = []
    missing: list[str] = []
    for target in PYTORCH_TIMING_TARGETS:
        seconds = (metrics_by_case.get(target.source_case) or {}).get(target.dynamo_timed_name)
        if seconds is None:
            missing.append(f"{target.source_case}:{target.dynamo_timed_name}")
            continue
        data_files = sorted(
            p.relative_to(platform_run_dir).as_posix()
            for p in (results_dir / target.source_case).glob(f"{target.data_file_prefix}*.data")
        )
        total_ns = int(seconds * 1_000_000_000)
        metric = {
            "total_ns": total_ns,
            "wall_clock_ns": total_ns,
            "seconds": seconds,
            "source": "pytorch_compile_times",
            "sourceText": (merged_txt.relative_to(platform_run_dir)).as_posix(),
            "sourceCase": target.source_case,
            "dynamoTimedName": target.dynamo_timed_name,
            "dataFilePrefix": target.data_file_prefix,
            "dataFiles": data_files,
        }
        cases.append({
            "caseId": target.case_id,
            "metrics": {
                "frameworkCallTime": metric,
                "wallClockTime": dict(metric),
            },
        })

    timing_dir = platform_run_dir / "timing"
    timing_dir.mkdir(parents=True, exist_ok=True)
    normalized = {
        "schemaVersion": 1,
        "platform": platform,
        "framework": "pytorch",
        "source": "pytorch_compile_times",
        "cases": cases,
    }
    normalized_path = timing_dir / "timing-normalized.json"
    normalized_path.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    local_timing_path = timing_dir / "pytorch-local-timing.json"
    local_timing_path.write_text(
        json.dumps({**normalized, "missing": missing}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return {
        "status": "collected" if cases else "empty",
        "cases": len(cases),
        "missing": missing,
        "normalized_file": str(normalized_path.relative_to(platform_run_dir)),
        "local_timing_file": str(local_timing_path.relative_to(platform_run_dir)),
    }


def parse_compile_times_text(text: str) -> dict[str, dict[str, float]]:
    """Parse merged compile-times text into ``{case: {function: seconds}}``.

    The runner writes headers such as ``===== CASE: aot_trace =====`` and
    ``===== CASE: aot_trace FILE: 950_compile.txt =====``.  Rows are parsed from
    the ``Function, Runtimes (s)`` table by exact function name.
    """
    current_case = "default"
    in_table = False
    parsed: dict[str, dict[str, float]] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        header = _CASE_HEADER_RE.match(line)
        if header:
            current_case = header.group(1)
            parsed.setdefault(current_case, {})
            in_table = False
            continue

        if line == "Function, Runtimes (s)":
            parsed.setdefault(current_case, {})
            in_table = True
            continue

        if not in_table:
            continue

        match = _METRIC_ROW_RE.match(line)
        if not match:
            # Ignore non-table trailers but keep parsing later rows if present.
            continue
        parsed.setdefault(current_case, {})[match.group("name").strip()] = float(match.group("seconds"))

    return parsed
