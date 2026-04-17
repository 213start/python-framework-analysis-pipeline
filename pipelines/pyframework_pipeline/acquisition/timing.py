"""Sub-step 5a: Timing data collection.

Parses TM stdout logs containing [BENCHMARK_SUMMARY] JSON lines produced by
PostUDF. Computes the four core timing metrics with three normalization views.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .manifest import AcquisitionManifest, AcquisitionSection

BENCHMARK_SUMMARY_RE = re.compile(r"\[BENCHMARK_SUMMARY\]\s*(\{.*\})")


def collect_timing(
    run_dir: Path,
    platform: str,
    stdout_files: list[Path] | None = None,
) -> dict[str, Any]:
    """Parse timing data from TM stdout logs.

    Parameters
    ----------
    run_dir : Path
        The run output directory (e.g. runs/2026-04-16-arm/).
    platform : str
        Platform identifier.
    stdout_files : list[Path] | None
        Explicit list of TM stdout log files. If None, auto-discovers
        from run_dir/tm-stdout*.log.

    Returns
    -------
    dict with keys: cases, raw_file, normalized_file.
    """
    timing_dir = run_dir / "timing"
    timing_dir.mkdir(parents=True, exist_ok=True)

    if stdout_files is None:
        stdout_files = sorted(run_dir.glob("tm-stdout*.log"))

    if not stdout_files:
        return {"cases": [], "raw_file": "", "normalized_file": ""}

    # Parse all [BENCHMARK_SUMMARY] lines from all stdout files
    summaries = []
    for f in stdout_files:
        summaries.extend(_parse_summaries(f))

    if not summaries:
        return {"cases": [], "raw_file": "", "normalized_file": ""}

    # Group by query (caseId) — when multiple subtasks produce summaries
    cases = _aggregate_summaries(summaries)

    # Write raw
    raw_file = timing_dir / "timing-raw.json"
    raw_data = {
        "schemaVersion": 1,
        "platform": platform,
        "summaries": [s for group in cases.values() for s in group["raw"]],
    }
    raw_file.write_text(
        json.dumps(raw_data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    # Compute normalized metrics per case
    normalized_cases = []
    for case_id, group in cases.items():
        normalized_cases.append(_compute_metrics(case_id, group))

    normalized_file = timing_dir / "timing-normalized.json"
    normalized_data = {
        "schemaVersion": 1,
        "platform": platform,
        "cases": normalized_cases,
    }
    normalized_file.write_text(
        json.dumps(normalized_data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    return {
        "cases": list(cases.keys()),
        "raw_file": str(timing_dir.relative_to(run_dir) / "timing-raw.json"),
        "normalized_file": str(timing_dir.relative_to(run_dir) / "timing-normalized.json"),
    }


def _parse_summaries(path: Path) -> list[dict[str, Any]]:
    """Extract [BENCHMARK_SUMMARY] JSON objects from a log file."""
    results = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = BENCHMARK_SUMMARY_RE.search(line)
        if m:
            try:
                results.append(json.loads(m.group(1)))
            except json.JSONDecodeError:
                continue
    return results


def _aggregate_summaries(
    summaries: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Group summaries by caseId (extracted from source info or default 'default').

    When multiple subtasks produce summaries for the same query, aggregate
    by summing counts and computing weighted averages.
    """
    # Current PostUDF doesn't include caseId — treat all as one case
    # Future: extract caseId from log context or filename
    grouped: dict[str, dict[str, Any]] = {}

    for s in summaries:
        case_id = s.get("caseId", "default")
        if case_id not in grouped:
            grouped[case_id] = {"raw": [], "total_overhead": 0, "total_py": 0, "total_records": 0}
        grouped[case_id]["raw"].append(s)
        grouped[case_id]["total_records"] += s.get("recordCount", 0)
        grouped[case_id]["total_overhead"] += s.get("totalFrameworkOverheadNs", 0)
        grouped[case_id]["total_py"] += s.get("totalPyDurationNs", 0)

    return grouped


def _compute_metrics(case_id: str, group: dict[str, Any]) -> dict[str, Any]:
    """Compute 4 metrics × 3 normalization views for one case."""
    n = group["total_records"]
    total_overhead = group["total_overhead"]
    total_py = group["total_py"]
    total_framework = total_overhead + total_py  # total round-trip
    subtask_count = len(group["raw"])

    # Weighted averages (ns)
    avg_overhead = total_overhead / n if n > 0 else 0
    avg_py = total_py / n if n > 0 else 0

    return {
        "caseId": case_id,
        "platform": "",
        "recordCount": n,
        "subtaskCount": subtask_count,
        "timingSource": "benchmark_runner_post_udf",
        "metrics": {
            "frameworkCallTime": {
                "batch_total_ns": total_overhead,
                "per_invocation_ns": round(avg_overhead, 1),
                "per_record_ns": round(avg_overhead, 1),
            },
            "businessOperatorTime": {
                "batch_total_ns": total_py,
                "per_invocation_ns": round(avg_py, 1),
                "per_record_ns": round(avg_py, 1),
            },
        },
        "normalization": {
            "default": "per_invocation_ns",
            "available": ["batch_total_ns", "per_invocation_ns", "per_record_ns"],
        },
    }
