"""Backfill Dataset.cases[].metrics from timing-normalized.json.

Reads arm + x86 timing-normalized.json, matches by caseId,
computes delta values, and formats timing into human-readable strings.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


# Mapping from timing-normalized.json metric keys to dataset metric keys.
_METRIC_KEY_MAP: dict[str, str] = {
    "frameworkCallTime": "framework",
    "businessOperatorTime": "operator",
}

# Nanosecond thresholds for human-readable formatting.
_NS_US = 1_000              # 1 microsecond
_NS_MS = 1_000_000          # 1 millisecond
_NS_S = 1_000_000_000       # 1 second


def backfill_timing(
    arm_run_dir: Path,
    x86_run_dir: Path,
    dataset: dict,
) -> dict:
    """Backfill Dataset.cases[].metrics from timing-normalized.json.

    Parameters
    ----------
    arm_run_dir : Path
        ARM platform run directory (contains timing/timing-normalized.json).
    x86_run_dir : Path
        x86 platform run directory (contains timing/timing-normalized.json).
    dataset : dict
        The dataset dict whose ``cases`` list will be mutated in place.

    Returns
    -------
    dict with ``cases_updated`` count and optional ``warnings`` list.
    """
    arm_timing = _load_timing_json(arm_run_dir)
    x86_timing = _load_timing_json(x86_run_dir)

    if not arm_timing and not x86_timing:
        return {"cases_updated": 0, "warnings": ["no timing-normalized.json found in either run dir"]}

    # Index timing cases by caseId for fast lookup.
    arm_by_id = {c["caseId"]: c for c in arm_timing.get("cases", [])}
    x86_by_id = {c["caseId"]: c for c in x86_timing.get("cases", [])}

    # Build a lookup from dataset case legacy IDs (and primary IDs) to case dicts.
    ds_case_by_legacy: dict[str, dict] = {}
    for ds_case in dataset.get("cases", []):
        legacy = ds_case.get("legacyCaseId")
        if legacy:
            ds_case_by_legacy[legacy] = ds_case
        # Also allow matching by the dataset's own ``id`` field.
        cid = ds_case.get("id", "")
        if cid:
            ds_case_by_legacy[cid] = ds_case

    cases_updated = 0
    warnings: list[str] = []

    # Collect all unique caseIds seen in either timing file.
    all_timing_ids = set(arm_by_id) | set(x86_by_id)

    for tid in sorted(all_timing_ids):
        arm_case = arm_by_id.get(tid)
        x86_case = x86_by_id.get(tid)

        # Resolve to the dataset case via legacyCaseId or direct id match.
        ds_case = ds_case_by_legacy.get(tid)
        if ds_case is None:
            warnings.append(f"timing caseId '{tid}' not found in dataset cases")
            continue

        metrics = _build_metrics(arm_case, x86_case)
        ds_case["metrics"] = metrics
        cases_updated += 1

    return {"cases_updated": cases_updated, "warnings": warnings}


def _load_timing_json(run_dir: Path) -> dict[str, Any]:
    """Load timing/timing-normalized.json from a run directory.

    Returns an empty dict if the file does not exist or cannot be parsed.
    """
    p = run_dir / "timing" / "timing-normalized.json"
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _build_metrics(
    arm_case: dict | None,
    x86_case: dict | None,
) -> dict[str, Any]:
    """Build the metrics dict for a single dataset case.

    Reads ``frameworkCallTime`` and ``businessOperatorTime`` from each
    platform's timing case, formats values, and computes delta percentages.
    """
    metrics: dict[str, Any] = {}

    for src_key, dst_key in _METRIC_KEY_MAP.items():
        arm_ns = _extract_per_invocation_ns(arm_case, src_key)
        x86_ns = _extract_per_invocation_ns(x86_case, src_key)

        metrics[dst_key] = _build_platform_entry(arm_ns, x86_ns)

    # Delta shorthand keys at the top level of the metrics dict.
    for dst_key in _METRIC_KEY_MAP.values():
        entry = metrics[dst_key]
        if entry.get("delta") is not None:
            metrics[f"{dst_key}Delta"] = entry["delta"]

    # Placeholder nulls for demo/tm when not available from timing data.
    for placeholder_key in ("demo", "tm"):
        if placeholder_key not in metrics:
            metrics[placeholder_key] = None

    return metrics


def _extract_per_invocation_ns(case: dict | None, metric_key: str) -> float | None:
    """Extract per_invocation_ns from a timing case dict.

    Returns None if the case or metric is absent.
    """
    if case is None:
        return None
    case_metrics = case.get("metrics")
    if case_metrics is None:
        return None
    metric = case_metrics.get(metric_key)
    if metric is None:
        return None
    val = metric.get("per_invocation_ns")
    if val is None:
        return None
    return float(val)


def _build_platform_entry(
    arm_ns: float | None,
    x86_ns: float | None,
) -> dict[str, str | None]:
    """Build a single metric entry with arm, x86, and delta fields."""
    entry: dict[str, str | None] = {
        "arm": _format_ns(arm_ns) if arm_ns is not None else None,
        "x86": _format_ns(x86_ns) if x86_ns is not None else None,
        "delta": None,
    }
    if arm_ns is not None and x86_ns is not None and x86_ns != 0:
        entry["delta"] = _compute_delta_pct(arm_ns, x86_ns)
    return entry


def _compute_delta_pct(arm: float, x86: float) -> str:
    """Compute delta as (arm - x86) / x86 * 100, formatted like '+8.2%'."""
    pct = (arm - x86) / x86 * 100
    return f"{pct:+.1f}%"


def _format_ns(ns: float) -> str:
    """Format nanoseconds to human-readable string.

    Uses natural 1x unit boundaries so each tier shows values >= 1.
    Millisecond and second tiers use two decimal places; microsecond
    and nanosecond tiers use one.

    Examples::

        5_230_000_000  -> '5.23 s'
        1_770_000      -> '1.77 ms'
        891_200        -> '891.2 us'   (micro sign)
        234.5          -> '234.5 ns'
    """
    if ns >= _NS_S:
        return f"{ns / _NS_S:.2f} s"
    if ns >= _NS_MS:
        return f"{ns / _NS_MS:.2f} ms"
    if ns >= _NS_US:
        return f"{ns / _NS_US:.1f} \u00b5s"
    return f"{ns:.1f} ns"
