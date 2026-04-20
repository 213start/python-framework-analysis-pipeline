"""Backfill Dataset.stackOverview and Dataset.functions[] from perf CSV.

Reads arm + x86 perf_records.csv produced by python-performance-kits,
maps categories via CATEGORY_MAP, aggregates by component/category/symbol,
and builds timing breakdowns with cross-platform delta calculations.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# --- Category mapping: python-performance-kits top-level -> framework L1 ---
CATEGORY_MAP: dict[str, str] = {
    "Interpreter": "interpreter",
    "Memory": "memory",
    "GC": "gc",
    "Tuple": "object_model",
    "Dict": "object_model",
    "List": "object_model",
    "Set": "object_model",
    "Misc Objects": "object_model",
    "Int": "type_operations",
    "Float": "type_operations",
    "String": "type_operations",
    "Calls": "calls_dispatch",
    "Dynamic": "calls_dispatch",
    "Import": "interpreter",
    "Compiler": "interpreter",
    "Concurrency": "native_boundary",
    "Library": "native_boundary",
    "Kernel": "kernel",
    "Unknown": "unknown",
}

# Human-readable display names for L1 categories
_CATEGORY_DISPLAY: dict[str, str] = {
    "interpreter": "Interpreter",
    "memory": "Memory",
    "gc": "GC",
    "object_model": "Object Model",
    "type_operations": "Type Operations",
    "calls_dispatch": "Calls / Dispatch",
    "native_boundary": "Native Boundary",
    "kernel": "Kernel",
    "unknown": "Unknown",
}

# --- Component mapping: shared_object prefix -> component ---
COMPONENT_MAP: dict[str, str] = {
    "libpython": "cpython",
    "python3": "cpython",
    "libc-": "glibc",
    "libm-": "glibc",
    "[kernel]": "kernel",
    "vmlinux": "kernel",
    "libstdc++": "third_party",
    "libgcc": "third_party",
}

# Human-readable display names for components
_COMPONENT_DISPLAY: dict[str, str] = {
    "cpython": "CPython",
    "glibc": "glibc",
    "kernel": "Kernel",
    "third_party": "Third Party",
    "bridge_runtime": "Bridge Runtime",
    "unknown": "Unknown",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_func_id(symbol: str) -> str:
    """Generate a deterministic function ID from symbol name."""
    digest = hashlib.md5(symbol.encode("utf-8")).hexdigest()[:8]
    return f"func_{digest}"


def _resolve_component(shared_object: str) -> str:
    """Map a shared_object string to a component identifier."""
    so = shared_object.strip()
    so_lower = so.lower()
    for prefix, component in COMPONENT_MAP.items():
        if so_lower.startswith(prefix.lower()):
            return component
    # Kernel can appear as [kernel.kallsyms], [kernel], vmlinux
    if so_lower.startswith("[kernel") or "vmlinux" in so_lower:
        return "kernel"
    # Check for python binary patterns
    if "python" in so_lower and ("lib" in so_lower or so_lower.startswith("python")):
        return "cpython"
    return "unknown"


def _resolve_l1_category(category_top: str, category_map: dict[str, str]) -> str:
    """Map a python-performance-kits top-level category to framework L1."""
    return category_map.get(category_top, "unknown")


def _parse_float(value: str) -> float:
    """Parse a string to float, returning 0.0 for empty/invalid."""
    if not value or not value.strip():
        return 0.0
    try:
        return float(value.strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _parse_int(value: str) -> int:
    """Parse a string to int, returning 0 for empty/invalid."""
    if not value or not value.strip():
        return 0
    try:
        return int(float(value.strip().replace(",", "")))
    except (ValueError, TypeError):
        return 0


def _format_ms(value_ms: float) -> str:
    """Format a millisecond value as human-readable string."""
    if value_ms == 0.0:
        return "0.0 ms"
    return f"{value_ms:.1f} ms"


def _format_pct(value: float) -> str:
    """Format a percentage value as human-readable string."""
    if value == 0.0:
        return "0.0%"
    return f"{value:.1f}%"


def _format_delta(delta_ms: float) -> str:
    """Format a delta value with sign prefix."""
    if delta_ms == 0.0:
        return "+0.0 ms"
    sign = "+" if delta_ms > 0 else ""
    return f"{sign}{delta_ms:.1f} ms"


def _format_delta_pct(delta: float) -> str:
    """Format a delta percentage with sign prefix."""
    if delta == 0.0:
        return "0.0%"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.1f}%"


def _read_perf_csv(csv_path: Path) -> list[dict[str, str]]:
    """Read a perf_records.csv and return list of row dicts."""
    if not csv_path.exists():
        logger.warning("perf CSV not found: %s", csv_path)
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def _period_to_ms(period_str: str, total_period: int) -> float:
    """Convert a period (cycle count) share percentage to estimated ms.

    Since we work with self_share percentages directly from the CSV,
    this converts percentage to the corresponding time portion.
    """
    return _parse_float(period_str)


# ---------------------------------------------------------------------------
# Core aggregation
# ---------------------------------------------------------------------------

def _aggregate_symbols(
    rows: list[dict[str, str]],
    category_map: dict[str, str],
    total_self_share: float,
) -> dict[str, dict[str, Any]]:
    """Aggregate rows by symbol into a dict keyed by symbol.

    Returns {symbol: {self_share, children_share, period, sample_count,
                       category_top, category_sub, shared_object, component, l1}}
    """
    agg: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = row.get("symbol", "").strip()
        if not symbol:
            continue

        self_share = _parse_float(row.get("self", "0"))
        children_share = _parse_float(row.get("children", "0"))
        period = _parse_int(row.get("period", "0"))
        sample_count = _parse_int(row.get("sample_count", "0"))
        category_top = row.get("category_top", "Unknown").strip()
        category_sub = row.get("category_sub", "").strip()
        shared_object = row.get("shared_object", "").strip()

        component = _resolve_component(shared_object)
        l1 = _resolve_l1_category(category_top, category_map)

        if symbol in agg:
            agg[symbol]["self_share"] += self_share
            agg[symbol]["children_share"] = max(agg[symbol]["children_share"], children_share)
            agg[symbol]["period"] += period
            agg[symbol]["sample_count"] += sample_count
        else:
            agg[symbol] = {
                "self_share": self_share,
                "children_share": children_share,
                "period": period,
                "sample_count": sample_count,
                "category_top": category_top,
                "category_sub": category_sub,
                "shared_object": shared_object,
                "component": component,
                "l1": l1,
            }

    return agg


def _aggregate_by_component(
    arm_agg: dict[str, dict[str, Any]],
    x86_agg: dict[str, dict[str, Any]],
    arm_total_self: float,
    x86_total_self: float,
) -> dict[str, dict[str, float]]:
    """Aggregate symbol data by component for both platforms.

    Returns {component_id: {arm_self, x86_self, arm_children, x86_children}}
    """
    comp: dict[str, dict[str, float]] = {}

    for symbol, data in arm_agg.items():
        cid = data["component"]
        bucket = comp.setdefault(cid, {"arm_self": 0.0, "x86_self": 0.0,
                                        "arm_children": 0.0, "x86_children": 0.0})
        bucket["arm_self"] += data["self_share"]
        bucket["arm_children"] += data["children_share"]

    for symbol, data in x86_agg.items():
        cid = data["component"]
        bucket = comp.setdefault(cid, {"arm_self": 0.0, "x86_self": 0.0,
                                        "arm_children": 0.0, "x86_children": 0.0})
        bucket["x86_self"] += data["self_share"]
        bucket["x86_children"] += data["children_share"]

    return comp


def _aggregate_by_category(
    arm_agg: dict[str, dict[str, Any]],
    x86_agg: dict[str, dict[str, Any]],
    category_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Aggregate symbol data by L1 category for both platforms.

    Returns {l1_category: {arm_self, x86_self, arm_children, x86_children,
                            top_arm_symbol, top_arm_self, top_x86_self}}
    """
    cat: dict[str, dict[str, Any]] = {}

    for symbol, data in arm_agg.items():
        l1 = data["l1"]
        bucket = cat.setdefault(l1, {
            "arm_self": 0.0, "x86_self": 0.0,
            "arm_children": 0.0, "x86_children": 0.0,
            "top_arm_symbol": None, "top_arm_self": 0.0,
        })
        bucket["arm_self"] += data["self_share"]
        bucket["arm_children"] += data["children_share"]
        if data["self_share"] > bucket["top_arm_self"]:
            bucket["top_arm_self"] = data["self_share"]
            bucket["top_arm_symbol"] = symbol

    for symbol, data in x86_agg.items():
        l1 = data["l1"]
        bucket = cat.setdefault(l1, {
            "arm_self": 0.0, "x86_self": 0.0,
            "arm_children": 0.0, "x86_children": 0.0,
            "top_arm_symbol": None, "top_arm_self": 0.0,
        })
        bucket["x86_self"] += data["self_share"]
        bucket["x86_children"] += data["children_share"]

    return cat


# ---------------------------------------------------------------------------
# Build output structures
# ---------------------------------------------------------------------------

def _compute_total_self_share(rows: list[dict[str, str]]) -> float:
    """Sum all self% values from perf rows to get total."""
    return sum(_parse_float(r.get("self", "0")) for r in rows)


def _build_components(
    comp_agg: dict[str, dict[str, float]],
    arm_total_self: float,
    x86_total_self: float,
    arm_total_ms: float,
    x86_total_ms: float,
) -> list[dict[str, Any]]:
    """Build stackOverview.components list."""
    delta_total = arm_total_ms - x86_total_ms
    if delta_total == 0.0:
        delta_total = 1.0  # avoid division by zero

    components = []
    # Sort by arm_self descending
    for cid in sorted(comp_agg, key=lambda c: comp_agg[c]["arm_self"], reverse=True):
        data = comp_agg[cid]
        arm_share = data["arm_self"]
        x86_share = data["x86_self"]

        arm_time = arm_total_ms * arm_share / 100.0 if arm_total_self > 0 else 0.0
        x86_time = x86_total_ms * x86_share / 100.0 if x86_total_self > 0 else 0.0
        delta = arm_time - x86_time

        arm_pct = arm_share  # relative to total perf samples
        x86_pct = x86_share
        delta_contrib = (delta / delta_total) * 100.0 if delta_total != 0 else 0.0

        components.append({
            "id": cid,
            "name": _COMPONENT_DISPLAY.get(cid, cid),
            "armTime": _format_ms(arm_time),
            "x86Time": _format_ms(x86_time),
            "armShare": _format_pct(arm_pct),
            "x86Share": _format_pct(x86_pct),
            "delta": _format_delta(delta),
            "deltaContribution": _format_pct(abs(delta_contrib)),
        })

    return components


def _build_categories(
    cat_agg: dict[str, dict[str, Any]],
    arm_agg: dict[str, dict[str, Any]],
    arm_total_self: float,
    x86_total_self: float,
    arm_total_ms: float,
    x86_total_ms: float,
) -> list[dict[str, Any]]:
    """Build stackOverview.categories list."""
    delta_total = arm_total_ms - x86_total_ms
    if delta_total == 0.0:
        delta_total = 1.0

    categories = []
    for l1 in sorted(cat_agg, key=lambda c: cat_agg[c]["arm_self"], reverse=True):
        data = cat_agg[l1]
        arm_share = data["arm_self"]
        x86_share = data["x86_self"]

        arm_time = arm_total_ms * arm_share / 100.0 if arm_total_self > 0 else 0.0
        x86_time = x86_total_ms * x86_share / 100.0 if x86_total_self > 0 else 0.0
        delta = arm_time - x86_time
        delta_contrib = (delta / delta_total) * 100.0 if delta_total != 0 else 0.0

        top_func_id = None
        top_symbol = data.get("top_arm_symbol")
        if top_symbol:
            top_func_id = _generate_func_id(top_symbol)

        categories.append({
            "id": l1,
            "name": _CATEGORY_DISPLAY.get(l1, l1),
            "level": "L1",
            "armTime": _format_ms(arm_time),
            "x86Time": _format_ms(x86_time),
            "armShare": _format_pct(arm_share),
            "x86Share": _format_pct(x86_share),
            "delta": _format_delta(delta),
            "deltaContribution": _format_pct(abs(delta_contrib)),
            "topFunctionId": top_func_id,
        })

    return categories


def _build_functions(
    arm_agg: dict[str, dict[str, Any]],
    x86_agg: dict[str, dict[str, Any]],
    arm_total_self: float,
    x86_total_self: float,
    arm_total_ms: float,
    x86_total_ms: float,
    top_n: int,
) -> list[dict[str, Any]]:
    """Build Dataset.functions list from merged arm + x86 symbol aggregates.

    Merges by symbol, sorted by max(self_arm, self_x86) descending, limited
    to top_n entries.
    """
    delta_total = arm_total_ms - x86_total_ms
    if delta_total == 0.0:
        delta_total = 1.0

    # Merge arm + x86 by symbol
    all_symbols: set[str] = set(arm_agg) | set(x86_agg)
    merged: list[dict[str, Any]] = []

    for symbol in all_symbols:
        arm = arm_agg.get(symbol)
        x86 = x86_agg.get(symbol)

        arm_self_share = arm["self_share"] if arm else 0.0
        x86_self_share = x86["self_share"] if x86 else 0.0
        arm_children_share = arm["children_share"] if arm else 0.0
        x86_children_share = x86["children_share"] if x86 else 0.0

        # Use whichever side has data for metadata; prefer arm
        meta = arm or x86
        if meta is None:
            continue

        func_id = _generate_func_id(symbol)

        arm_time_self = arm_total_ms * arm_self_share / 100.0 if arm_total_self > 0 else 0.0
        x86_time_self = x86_total_ms * x86_self_share / 100.0 if x86_total_self > 0 else 0.0
        arm_time_total = arm_total_ms * arm_children_share / 100.0 if arm_total_self > 0 else 0.0
        x86_time_total = x86_total_ms * x86_children_share / 100.0 if x86_total_self > 0 else 0.0
        delta = arm_time_self - x86_time_self
        delta_contrib = (delta / delta_total) * 100.0 if delta_total != 0 else 0.0

        merged.append({
            "id": func_id,
            "symbol": symbol,
            "component": meta["component"],
            "categoryL1": meta["l1"],
            "categoryL2": meta.get("category_sub", ""),
            "metrics": {
                "selfArm": _format_ms(arm_time_self),
                "selfX86": _format_ms(x86_time_self),
                "totalArm": _format_ms(arm_time_total),
                "totalX86": _format_ms(x86_time_total),
                "armShare": _format_pct(arm_self_share),
                "x86Share": _format_pct(x86_self_share),
                "delta": _format_delta(delta),
                "deltaContribution": _format_pct(abs(delta_contrib)),
            },
            "callPath": [],
            "caseIds": [],
            "patternIds": [],
            "artifactIds": [],
        })

    # Sort by max(self_arm, self_x86) time descending
    merged.sort(
        key=lambda f: max(
            _parse_float(f["metrics"]["selfArm"].replace(" ms", "").replace("+", "")),
            _parse_float(f["metrics"]["selfX86"].replace(" ms", "").replace("+", "")),
        ),
        reverse=True,
    )

    return merged[:top_n]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def backfill_perf(
    arm_run_dir: Path,
    x86_run_dir: Path,
    dataset: dict,
    category_map: dict | None = None,
    top_n: int = 20,
) -> dict:
    """Backfill Dataset.stackOverview and Dataset.functions from perf CSVs.

    Parameters
    ----------
    arm_run_dir : Path
        ARM run output directory containing perf/data/perf_records.csv.
    x86_run_dir : Path
        x86 run output directory containing perf/data/perf_records.csv.
    dataset : dict
        The four-layer dataset dict to update in-place.
    category_map : dict | None
        Optional override for CATEGORY_MAP (category_top -> L1 category).
    top_n : int
        Maximum number of functions to include.

    Returns
    -------
    dict with summary counts:
        - components: number of component entries
        - categories: number of category entries
        - functions: number of function entries
        - arm_rows: number of ARM perf CSV rows read
        - x86_rows: number of x86 perf CSV rows read
    """
    cat_map = category_map if category_map is not None else CATEGORY_MAP

    # Locate perf_records.csv files
    arm_csv = arm_run_dir / "perf" / "data" / "perf_records.csv"
    x86_csv = x86_run_dir / "perf" / "data" / "perf_records.csv"

    arm_rows = _read_perf_csv(arm_csv)
    x86_rows = _read_perf_csv(x86_csv)

    if not arm_rows and not x86_rows:
        logger.warning("No perf CSV data found for either platform")
        return {
            "components": 0,
            "categories": 0,
            "functions": 0,
            "arm_rows": 0,
            "x86_rows": 0,
        }

    # Compute total self% per platform
    arm_total_self = _compute_total_self_share(arm_rows)
    x86_total_self = _compute_total_self_share(x86_rows)

    # Aggregate symbols per platform
    arm_agg = _aggregate_symbols(arm_rows, cat_map, arm_total_self)
    x86_agg = _aggregate_symbols(x86_rows, cat_map, x86_total_self)

    # Estimate total wall-clock time in ms from dataset metrics if available.
    # Use sum of all platform totals from existing cases, or derive from perf data.
    arm_total_ms = _estimate_total_ms(dataset, "arm", arm_total_self, arm_rows)
    x86_total_ms = _estimate_total_ms(dataset, "x86", x86_total_self, x86_rows)

    # Aggregate by component and category
    comp_agg = _aggregate_by_component(arm_agg, x86_agg, arm_total_self, x86_total_self)
    cat_agg = _aggregate_by_category(arm_agg, x86_agg, cat_map)

    # Build output structures
    components = _build_components(comp_agg, arm_total_self, x86_total_self,
                                   arm_total_ms, x86_total_ms)
    categories = _build_categories(cat_agg, arm_agg,
                                   arm_total_self, x86_total_self,
                                   arm_total_ms, x86_total_ms)
    functions = _build_functions(arm_agg, x86_agg,
                                 arm_total_self, x86_total_self,
                                 arm_total_ms, x86_total_ms,
                                 top_n=top_n)

    # Compute platform totals
    arm_total_time = sum(
        _parse_float(c["armTime"].replace(" ms", "").replace("+", ""))
        for c in components
    )
    x86_total_time = sum(
        _parse_float(c["x86Time"].replace(" ms", "").replace("+", ""))
        for c in components
    )

    # Update dataset in-place
    dataset["stackOverview"] = {
        "platformTotals": {
            "arm": _format_ms(arm_total_time),
            "x86": _format_ms(x86_total_time),
        },
        "components": components,
        "categories": categories,
    }

    # Merge functions: append new, update existing by id
    existing_funcs = {f["id"]: f for f in dataset.get("functions", [])}
    for func in functions:
        fid = func["id"]
        if fid in existing_funcs:
            # Update metrics and metadata, preserve existing cross-references
            existing = existing_funcs[fid]
            existing["metrics"] = func["metrics"]
            existing["component"] = func["component"]
            existing["categoryL1"] = func["categoryL1"]
            existing["categoryL2"] = func["categoryL2"]
        else:
            existing_funcs[fid] = func

    dataset["functions"] = list(existing_funcs.values())

    summary = {
        "components": len(components),
        "categories": len(categories),
        "functions": len(functions),
        "arm_rows": len(arm_rows),
        "x86_rows": len(x86_rows),
    }
    logger.info(
        "perf backfill complete: %d components, %d categories, %d functions "
        "(arm_rows=%d, x86_rows=%d)",
        summary["components"], summary["categories"], summary["functions"],
        summary["arm_rows"], summary["x86_rows"],
    )
    return summary


def _estimate_total_ms(
    dataset: dict,
    platform: str,
    total_self_share: float,
    rows: list[dict[str, str]],
) -> float:
    """Estimate total wall-clock time in ms from dataset metrics or perf data.

    Tries to derive from dataset.cases[].metrics first, then falls back to
    a heuristic based on perf sample counts.
    """
    # Try to extract from dataset case metrics
    total_ms = 0.0
    for case in dataset.get("cases", []):
        metrics = case.get("metrics", {})
        demo = metrics.get("demo") or {}
        time_str = demo.get(platform, "")
        if time_str:
            total_ms += _parse_time_to_ms(time_str)

    if total_ms > 0:
        return total_ms

    # Fallback: estimate from period sums (cycles -> ms heuristic).
    # At ~3 GHz, 1 period = 1 cycle ~= 0.333 ns. This is rough.
    total_period = sum(_parse_int(r.get("period", "0")) for r in rows)
    if total_period > 0:
        # Assume ~3 GHz clock; convert cycles to ms
        return total_period / 3_000_000.0

    return 100.0  # default fallback: 100ms


def _parse_time_to_ms(time_str: str) -> float:
    """Parse a human-readable time string (e.g., '5.23 s', '1.77 ms') to ms."""
    s = time_str.strip()
    if not s:
        return 0.0
    try:
        if s.endswith(" ms"):
            return float(s[:-3].strip())
        elif s.endswith(" s"):
            return float(s[:-2].strip()) * 1000.0
        elif s.endswith(" us"):
            return float(s[:-3].strip()) / 1000.0
        elif s.endswith(" ns"):
            return float(s[:-3].strip()) / 1_000_000.0
        else:
            return float(s)
    except (ValueError, TypeError):
        return 0.0
