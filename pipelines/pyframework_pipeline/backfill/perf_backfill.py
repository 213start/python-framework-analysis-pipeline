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

# --- Category mapping: python-performance-kits category_top -> four-layer L1 ---
# Handles the full cpython_category_rules.json taxonomy.
_CATEGORY_TO_L1: dict[str, str] = {
    "CPython.Interpreter": "interpreter",
    "CPython.Memory": "memory",
    "CPython.GC": "gc",
    "CPython.Objects": "object_model",
    "CPython.Calls": "calls_dispatch",
    "CPython.Lookup": "lookup",
    "CPython.Import": "import_loading",
    "CPython.Compiler": "compiler",
    "CPython.Concurrency": "concurrency",
    "CPython.Exceptions": "exceptions",
    "CPython.Runtime": "runtime",
    "Kernel": "kernel",
    "glibc": "glibc",
    "Library": "library",
    "Unknown": "unknown",
}

# L2 sub-categories derived from python-performance-kits category_sub.
# Only non-empty sub-categories are mapped; the prefix "CPython.Objects." etc. is stripped.
_L2_SHORT_NAME: dict[str, str] = {
    "CPython.Objects.Dict": "dict",
    "CPython.Objects.List": "list",
    "CPython.Objects.Tuple": "tuple",
    "CPython.Objects.Set": "set",
    "CPython.Objects.Int": "int",
    "CPython.Objects.Float": "float",
    "CPython.Objects.Str": "str",
    "CPython.Calls.Vectorcall": "vectorcall",
    "CPython.Lookup.Attribute": "attribute",
    "CPython.Import.ModuleLoading": "module_loading",
    "CPython.Concurrency.GIL": "gil",
    "CPython.Concurrency.Threading": "threading",
    "CPython.Concurrency.Async": "async",
    "CPython.Exceptions.BaseException": "base_exception",
}

# Human-readable display names for L1 categories
_CATEGORY_DISPLAY: dict[str, str] = {
    "interpreter": "Interpreter",
    "memory": "Memory",
    "gc": "GC",
    "object_model": "Object Model",
    "calls_dispatch": "Calls / Dispatch",
    "lookup": "Lookup",
    "import_loading": "Import",
    "compiler": "Compiler",
    "concurrency": "Concurrency",
    "exceptions": "Exceptions",
    "runtime": "Runtime",
    "kernel": "Kernel",
    "glibc": "glibc",
    "library": "Library",
    "unknown": "Unknown",
}

# Legacy category_top values from older CSV formats — fallback mapping.
_LEGACY_CATEGORY_MAP: dict[str, str] = {
    "Interpreter": "interpreter",
    "Memory": "memory",
    "GC": "gc",
    "Data Structure": "object_model",
    "Object Model": "object_model",
    "Tuple": "object_model",
    "Dict": "object_model",
    "List": "object_model",
    "Set": "object_model",
    "Int": "object_model",
    "Float": "object_model",
    "String": "object_model",
    "Calls": "calls_dispatch",
    "Call Protocol": "calls_dispatch",
    "Dynamic": "calls_dispatch",
    "Import": "interpreter",
    "Compiler": "compiler",
    "Concurrency": "concurrency",
    "Library": "library",
    "Runtime": "runtime",
    "Generator": "concurrency",
    "Kernel": "kernel",
    "glibc": "glibc",
    "Unknown": "unknown",
}

# --- Component mapping: shared_object prefix -> component ---
_COMPONENT_MAP: dict[str, str] = {
    "libpython": "cpython",
    "python3": "cpython",
    "python": "cpython",
    "libc-": "glibc",
    "libm-": "glibc",
    "ld-linux": "glibc",
    "libpthread": "glibc",
    "libstdc++": "third_party",
    "libgcc": "third_party",
    "[kernel": "kernel",
    "vmlinux": "kernel",
    "libjvm": "bridge_runtime",
    "libflink": "bridge_runtime",
}

# python-performance-kits category_top → component (when available).
_CATEGORY_TO_COMPONENT: dict[str, str] = {
    "CPython.Interpreter": "cpython",
    "CPython.Memory": "cpython",
    "CPython.GC": "cpython",
    "CPython.Objects": "cpython",
    "CPython.Calls": "cpython",
    "CPython.Lookup": "cpython",
    "CPython.Import": "cpython",
    "CPython.Compiler": "cpython",
    "CPython.Concurrency": "cpython",
    "CPython.Exceptions": "cpython",
    "CPython.Runtime": "cpython",
    "Kernel": "kernel",
    "glibc": "glibc",
}

# Human-readable display names for components
_COMPONENT_DISPLAY: dict[str, str] = {
    "cpython": "CPython",
    "glibc": "glibc",
    "kernel": "Kernel",
    "third_party": "Third Party",
    "bridge_runtime": "Bridge Runtime",
    "library": "Library",
    "unknown": "Unknown",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_func_id(symbol: str) -> str:
    """Generate a deterministic function ID from symbol name."""
    digest = hashlib.md5(symbol.encode("utf-8")).hexdigest()[:8]
    return f"func_{digest}"


def _resolve_component(shared_object: str, category_top: str = "") -> str:
    """Map a shared_object + category_top to a component identifier.

    Prefers category-based resolution for CPython/Kernel/glibc, falls back
    to shared_object prefix matching for Library/Unknown.
    """
    if category_top:
        comp = _CATEGORY_TO_COMPONENT.get(category_top)
        if comp:
            return comp

    so = shared_object.strip()
    so_lower = so.lower()
    for prefix, component in _COMPONENT_MAP.items():
        if so_lower.startswith(prefix.lower()):
            return component
    if "python" in so_lower and ("lib" in so_lower or so_lower.startswith("python")):
        return "cpython"
    return "unknown"


def _resolve_l1_category(category_top: str) -> str:
    """Map a python-performance-kits category_top to framework L1.

    Handles both the full CPython.* taxonomy and legacy format.
    """
    l1 = _CATEGORY_TO_L1.get(category_top)
    if l1:
        return l1
    return _LEGACY_CATEGORY_MAP.get(category_top, "unknown")


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
# Row filtering
# ---------------------------------------------------------------------------

# Python-related category_top prefixes (from cpython_category_rules.json).
_PYTHON_CATEGORIES = frozenset({
    "CPython.Runtime", "CPython.Interpreter", "CPython.Memory", "CPython.GC",
    "CPython.Objects", "CPython.Calls", "CPython.Lookup", "CPython.Import",
    "CPython.Compiler", "CPython.Concurrency", "CPython.Exceptions",
})

# Shared objects that are Python runtime dependencies.
_PYTHON_SO_KEYWORDS = ("python", "libpython")

# Kernel idle/scheduler symbols that pollute Python analysis.
# perf record -a misattributes CPU idle samples to container processes due to
# PID namespace isolation. These symbols provide zero insight into Python perf.
_KERNEL_IDLE_SYMBOLS = frozenset({
    "default_idle_call", "cpu_startup_entry", "do_idle",
    "cpuidle_idle_call", "schedule_idle", "arch_call_rest_init",
    "rest_init", "start_kernel", "secondary_start_kernel",
    "__primary_switched", "__secondary_switched",
})


def _filter_python_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Keep only rows from Python worker processes with resolved symbols.

    perf record -a captures ALL processes (JVM threads, bash, perf itself).
    When pid_command is available, filter to Python workers (python3, pyflink-udf-run).
    When pid_command is absent, fall back to category/SO-based filtering.

    Also drops:
    - Unresolved hex-address symbols (0x...) which provide no insight.
    - Kernel idle symbols misattributed to container processes by perf -a.
    """
    has_pid = any("pid_command" in (r or {}) for r in rows[:100]) if rows else False
    filtered = []
    for row in rows:
        sym = (row.get("symbol") or "").strip()
        if sym.startswith("0x") or sym == "[unknown]":
            continue
        if sym in _KERNEL_IDLE_SYMBOLS:
            continue

        if has_pid:
            cmd = (row.get("pid_command") or "").strip().lower()
            if "python" not in cmd and "pyflink" not in cmd:
                continue
        else:
            # Fallback: category/SO-based filtering
            cat = (row.get("category_top") or "").strip()
            so = (row.get("shared_object") or "").strip().lower()
            if cat in _PYTHON_CATEGORIES:
                pass
            elif any(kw in so for kw in _PYTHON_SO_KEYWORDS):
                pass
            elif ".cpython-" in so:
                pass
            else:
                continue

        filtered.append(row)
    return filtered


# ---------------------------------------------------------------------------
# Core aggregation
# ---------------------------------------------------------------------------

def _aggregate_symbols(
    rows: list[dict[str, str]],
    total_self_share: float,
) -> dict[str, dict[str, Any]]:
    """Aggregate rows by symbol into a dict keyed by symbol.

    Returns {symbol: {self_share, children_share, period, sample_count,
                       category_top, category_sub, shared_object, component, l1, l2}}
    """
    agg: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = (row.get("symbol") or "").strip()
        if not symbol:
            continue

        self_share = _parse_float(row.get("self", "0"))
        children_share = _parse_float(row.get("children", "0"))
        period = _parse_int(row.get("period", "0"))
        sample_count = _parse_int(row.get("sample_count", "0"))
        category_top = (row.get("category_top") or "Unknown").strip()
        category_sub = (row.get("category_sub") or "").strip()
        shared_object = (row.get("shared_object") or "").strip()

        component = _resolve_component(shared_object, category_top)
        l1 = _resolve_l1_category(category_top)
        l2 = _L2_SHORT_NAME.get(category_sub, "")

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
                "l2": l2,
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
    # Sum raw shares for normalization (perf CSV may only capture top N rows).
    arm_share_total = sum(d["arm_self"] for d in comp_agg.values())
    x86_share_total = sum(d["x86_self"] for d in comp_agg.values())

    # Pre-compute all deltas so deltaContribution sums to 100%.
    deltas: dict[str, float] = {}
    for cid in comp_agg:
        data = comp_agg[cid]
        arm_time = arm_total_ms * data["arm_self"] / 100.0 if arm_total_self > 0 else 0.0
        x86_time = x86_total_ms * data["x86_self"] / 100.0 if x86_total_self > 0 else 0.0
        deltas[cid] = arm_time - x86_time
    net_delta = sum(deltas.values()) or 1.0

    components = []
    # Sort by arm_self descending
    for cid in sorted(comp_agg, key=lambda c: comp_agg[c]["arm_self"], reverse=True):
        data = comp_agg[cid]
        arm_share = data["arm_self"]
        x86_share = data["x86_self"]
        delta = deltas[cid]

        # Normalize share to captured total, not raw perf sample total.
        arm_pct = (arm_share / arm_share_total * 100.0) if arm_share_total > 0 else 0.0
        x86_pct = (x86_share / x86_share_total * 100.0) if x86_share_total > 0 else 0.0
        delta_contrib = delta / net_delta * 100.0

        components.append({
            "id": cid,
            "name": _COMPONENT_DISPLAY.get(cid, cid),
            "armTime": _format_ms(arm_total_ms * arm_share / 100.0) if arm_total_self > 0 else "0.0 ms",
            "x86Time": _format_ms(x86_total_ms * x86_share / 100.0) if x86_total_self > 0 else "0.0 ms",
            "armShare": _format_pct(arm_pct),
            "x86Share": _format_pct(x86_pct),
            "delta": _format_delta(delta),
            "deltaContribution": _format_delta_pct(delta_contrib),
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
    # Sum raw shares for normalization.
    arm_share_total = sum(d["arm_self"] for d in cat_agg.values())
    x86_share_total = sum(d["x86_self"] for d in cat_agg.values())

    # Pre-compute all deltas so deltaContribution sums to 100%.
    deltas: dict[str, float] = {}
    for l1 in cat_agg:
        d = cat_agg[l1]
        arm_time = arm_total_ms * d["arm_self"] / 100.0 if arm_total_self > 0 else 0.0
        x86_time = x86_total_ms * d["x86_self"] / 100.0 if x86_total_self > 0 else 0.0
        deltas[l1] = arm_time - x86_time
    net_delta = sum(deltas.values()) or 1.0

    categories = []
    for l1 in sorted(cat_agg, key=lambda c: cat_agg[c]["arm_self"], reverse=True):
        data = cat_agg[l1]
        arm_share = data["arm_self"]
        x86_share = data["x86_self"]
        delta = deltas[l1]

        # Normalize share to captured total.
        arm_pct = (arm_share / arm_share_total * 100.0) if arm_share_total > 0 else 0.0
        x86_pct = (x86_share / x86_share_total * 100.0) if x86_share_total > 0 else 0.0

        top_func_id = None
        top_symbol = data.get("top_arm_symbol")
        if top_symbol:
            top_func_id = _generate_func_id(top_symbol)

        categories.append({
            "id": l1,
            "name": _CATEGORY_DISPLAY.get(l1, l1),
            "level": "L1",
            "armTime": _format_ms(arm_total_ms * arm_share / 100.0) if arm_total_self > 0 else "0.0 ms",
            "x86Time": _format_ms(x86_total_ms * x86_share / 100.0) if x86_total_self > 0 else "0.0 ms",
            "armShare": _format_pct(arm_pct),
            "x86Share": _format_pct(x86_pct),
            "delta": _format_delta(delta),
            "deltaContribution": _format_delta_pct(delta / net_delta * 100.0),
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
    # Compute total self shares for normalization.
    arm_self_total = sum(d["self_share"] for d in arm_agg.values())
    x86_self_total = sum(d["self_share"] for d in x86_agg.values())

    # Pre-compute all deltas so deltaContribution sums to 100%.
    func_deltas: dict[str, float] = {}
    for symbol in set(arm_agg) | set(x86_agg):
        arm = arm_agg.get(symbol)
        x86 = x86_agg.get(symbol)
        a_self = arm_total_ms * (arm["self_share"] if arm else 0.0) / 100.0 if arm_total_self > 0 else 0.0
        x_self = x86_total_ms * (x86["self_share"] if x86 else 0.0) / 100.0 if x86_total_self > 0 else 0.0
        func_deltas[symbol] = a_self - x_self
    net_delta = sum(func_deltas.values()) or 1.0

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
        delta = func_deltas[symbol]
        delta_contrib = delta / net_delta * 100.0

        # Normalize shares to captured total.
        arm_pct = (arm_self_share / arm_self_total * 100.0) if arm_self_total > 0 else 0.0
        x86_pct = (x86_self_share / x86_self_total * 100.0) if x86_self_total > 0 else 0.0

        merged.append({
            "id": func_id,
            "symbol": symbol,
            "component": meta["component"],
            "categoryL1": meta["l1"],
            "categoryL2": meta.get("l2", "") or meta.get("category_sub", ""),
            "metrics": {
                "selfArm": _format_ms(arm_time_self),
                "selfX86": _format_ms(x86_time_self),
                "totalArm": _format_ms(arm_time_total),
                "totalX86": _format_ms(x86_time_total),
                "armShare": _format_pct(arm_pct),
                "x86Share": _format_pct(x86_pct),
                "delta": _format_delta(delta),
                "deltaContribution": _format_delta_pct(delta_contrib),
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


def _build_component_details(
    comp_agg: dict[str, dict[str, float]],
    arm_agg: dict[str, dict[str, Any]],
    x86_agg: dict[str, dict[str, Any]],
    arm_total_self: float,
    x86_total_self: float,
    arm_total_ms: float,
    x86_total_ms: float,
    functions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build componentDetails list from perf aggregation data.

    Each component entry includes:
    - Real timing from perf data
    - Categories derived from L1 aggregation filtered to this component
    - hotspotIds referencing actual function IDs
    """
    # Group functions by component.
    funcs_by_component: dict[str, list[dict[str, Any]]] = {}
    for func in functions:
        comp = func["component"]
        funcs_by_component.setdefault(comp, []).append(func)

    # Group L1 categories by component (via symbol aggregation).
    comp_categories: dict[str, dict[str, dict[str, float]]] = {}
    for symbol, data in arm_agg.items():
        comp = data["component"]
        l1 = data["l1"]
        bucket = comp_categories.setdefault(comp, {})
        cat_entry = bucket.setdefault(l1, {"arm_self": 0.0, "x86_self": 0.0})
        cat_entry["arm_self"] += data["self_share"]

    for symbol, data in x86_agg.items():
        comp = data["component"]
        l1 = data["l1"]
        bucket = comp_categories.setdefault(comp, {})
        cat_entry = bucket.setdefault(l1, {"arm_self": 0.0, "x86_self": 0.0})
        cat_entry["x86_self"] += data["self_share"]

    delta_total = arm_total_ms - x86_total_ms
    if delta_total == 0.0:
        delta_total = 1.0

    # Pre-compute all deltas for deltaContribution normalization.
    cd_deltas: dict[str, float] = {}
    for cid in comp_agg:
        d = comp_agg[cid]
        a_t = arm_total_ms * d["arm_self"] / 100.0 if arm_total_self > 0 else 0.0
        x_t = x86_total_ms * d["x86_self"] / 100.0 if x86_total_self > 0 else 0.0
        cd_deltas[cid] = a_t - x_t
    cd_net_delta = sum(cd_deltas.values()) or 1.0

    details = []
    for cid in sorted(comp_agg, key=lambda c: comp_agg[c]["arm_self"], reverse=True):
        data = comp_agg[cid]
        arm_share = data["arm_self"]
        x86_share = data["x86_self"]
        delta = cd_deltas[cid]

        arm_time = arm_total_ms * arm_share / 100.0 if arm_total_self > 0 else 0.0
        x86_time = x86_total_ms * x86_share / 100.0 if x86_total_self > 0 else 0.0

        # Normalize shares relative to total captured.
        arm_share_total = sum(d["arm_self"] for d in comp_agg.values())
        x86_share_total = sum(d["x86_self"] for d in comp_agg.values())
        arm_pct = (arm_share / arm_share_total * 100.0) if arm_share_total > 0 else 0.0
        x86_pct = (x86_share / x86_share_total * 100.0) if x86_share_total > 0 else 0.0

        # Categories for this component.
        comp_cats = comp_categories.get(cid, {})
        categories = []
        for l1 in sorted(comp_cats, key=lambda c: comp_cats[c]["arm_self"], reverse=True):
            cat_data = comp_cats[l1]
            cat_arm_time = arm_total_ms * cat_data["arm_self"] / 100.0 if arm_total_self > 0 else 0.0
            cat_x86_time = x86_total_ms * cat_data["x86_self"] / 100.0 if x86_total_self > 0 else 0.0
            cat_delta = cat_arm_time - cat_x86_time
            categories.append({
                "id": l1,
                "name": _CATEGORY_DISPLAY.get(l1, l1),
                "delta": _format_delta(cat_delta),
            })

        # Hotspot function IDs (top functions in this component).
        comp_funcs = funcs_by_component.get(cid, [])
        hotspot_ids = [f["id"] for f in comp_funcs[:5]]

        details.append({
            "id": cid,
            "name": _COMPONENT_DISPLAY.get(cid, cid),
            "armTime": _format_ms(arm_time),
            "x86Time": _format_ms(x86_time),
            "armShare": _format_pct(arm_pct),
            "x86Share": _format_pct(x86_pct),
            "delta": _format_delta(delta),
            "deltaContribution": _format_delta_pct(delta / cd_net_delta * 100.0),
            "categories": categories,
            "hotspotIds": hotspot_ids,
        })

    return details


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def backfill_perf(
    arm_run_dir: Path,
    x86_run_dir: Path,
    dataset: dict,
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

    # Filter to Python-runtime-relevant rows only.
    # perf record -a captures all processes (JVM, bash, perf itself, etc.),
    # but we only care about Python worker and its dependencies.
    arm_rows = _filter_python_rows(arm_rows)
    x86_rows = _filter_python_rows(x86_rows)
    logger.info("After Python filter: arm=%d, x86=%d rows", len(arm_rows), len(x86_rows))

    # Compute total self% per platform
    arm_total_self = _compute_total_self_share(arm_rows)
    x86_total_self = _compute_total_self_share(x86_rows)

    # Aggregate symbols per platform
    arm_agg = _aggregate_symbols(arm_rows, arm_total_self)
    x86_agg = _aggregate_symbols(x86_rows, x86_total_self)

    # Estimate total wall-clock time in ms from dataset metrics if available.
    # Use sum of all platform totals from existing cases, or derive from perf data.
    arm_total_ms = _estimate_total_ms(dataset, "arm", arm_total_self, arm_rows)
    x86_total_ms = _estimate_total_ms(dataset, "x86", x86_total_self, x86_rows)

    # Aggregate by component and category
    comp_agg = _aggregate_by_component(arm_agg, x86_agg, arm_total_self, x86_total_self)
    cat_agg = _aggregate_by_category(arm_agg, x86_agg)

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

    # Nullify topFunctionId references that were cut by top_n
    func_ids = {f["id"] for f in functions}
    for cat in categories:
        tfid = cat.get("topFunctionId")
        if tfid and tfid not in func_ids:
            cat["topFunctionId"] = None

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

    # Replace functions entirely — stale entries from previous runs (different
    # filter criteria, unresolved symbols, etc.) should not persist.
    dataset["functions"] = functions

    # Build componentDetails from perf data (replace placeholder data).
    dataset["componentDetails"] = _build_component_details(
        comp_agg, arm_agg, x86_agg,
        arm_total_self, x86_total_self,
        arm_total_ms, x86_total_ms,
        functions,
    )

    # Do NOT estimate operator/framework from perf data.
    # perf -F 999 with -a severely undersamples PyFlink's bursty workload
    # (estimated 12.9 ns/row vs actual ~2 us/row, 160x underestimate).
    # Real timing comes from PostUDF's [BENCHMARK_SUMMARY] in TM log files.
    # TODO: orchestrator should capture TM log files and parse BENCHMARK_SUMMARY.

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


def _compute_cpu_utilization(rows: list[dict[str, str]],
                             sample_rate_hz: int = 999) -> float:
    """Estimate Python worker CPU utilization from perf sample count.

    Returns the fraction of wall-clock time spent on CPU by Python workers.
    Uses sample count / rate to estimate total CPU time, then divides by
    the sum of all case wall-clock times from the dataset.

    perf -a captures everything; we already filtered to Python worker rows.
    At 999 Hz, N samples ≈ N/999 seconds of CPU time.
    """
    if not rows:
        return 0.0
    total_samples = sum(_parse_int(r.get("sample_count", "1")) for r in rows)
    cpu_time_s = total_samples / sample_rate_hz
    return cpu_time_s


def _estimate_case_operator_framework(
    dataset: dict,
    arm_cpu_s: float,
    x86_cpu_s: float,
) -> None:
    """Estimate operator/framework timing from perf CPU utilization when missing.

    The perf data covers ALL queries. We distribute CPU time proportionally
    to each case's wall-clock time:
    - operator = case_wallclock × (total_cpu / total_wallclock)
    - framework = case_wallclock - operator
    """
    # Sum all case wall-clock times to compute ratio.
    total_arm_ms = sum(
        _parse_time_to_ms((c.get("metrics", {}).get("demo") or {}).get("arm", ""))
        for c in dataset.get("cases", [])
    )
    total_x86_ms = sum(
        _parse_time_to_ms((c.get("metrics", {}).get("demo") or {}).get("x86", ""))
        for c in dataset.get("cases", [])
    )
    arm_cpu_ms = arm_cpu_s * 1000.0
    x86_cpu_ms = x86_cpu_s * 1000.0
    arm_ratio = arm_cpu_ms / total_arm_ms if total_arm_ms > 0 else 0.0
    x86_ratio = x86_cpu_ms / total_x86_ms if total_x86_ms > 0 else 0.0

    for case in dataset.get("cases", []):
        metrics = case.get("metrics", {})

        # Always overwrite — perf estimation improves with each run and
        # previous estimates from different methods may be stale.
        demo = metrics.get("demo") or {}
        arm_demo_ms = _parse_time_to_ms(demo.get("arm", ""))
        x86_demo_ms = _parse_time_to_ms(demo.get("x86", ""))

        arm_op_ms = arm_demo_ms * arm_ratio
        x86_op_ms = x86_demo_ms * x86_ratio
        arm_fw_ms = arm_demo_ms - arm_op_ms
        x86_fw_ms = x86_demo_ms - x86_op_ms

        # Build operator entry
        op_entry: dict[str, str | None] = {
            "arm": _format_ms(arm_op_ms) if arm_demo_ms > 0 else None,
            "x86": _format_ms(x86_op_ms) if x86_demo_ms > 0 else None,
            "delta": None,
        }
        if arm_op_ms > 0 and x86_op_ms > 0:
            op_entry["delta"] = _format_delta_pct(
                (arm_op_ms - x86_op_ms) / x86_op_ms * 100)

        # Build framework entry
        fw_entry: dict[str, str | None] = {
            "arm": _format_ms(arm_fw_ms) if arm_demo_ms > 0 else None,
            "x86": _format_ms(x86_fw_ms) if x86_demo_ms > 0 else None,
            "delta": None,
        }
        if arm_fw_ms > 0 and x86_fw_ms > 0:
            fw_entry["delta"] = _format_delta_pct(
                (arm_fw_ms - x86_fw_ms) / x86_fw_ms * 100)

        metrics["operator"] = op_entry
        metrics["framework"] = fw_entry

    logger.info("Estimated operator/framework from perf: arm_cpu=%.2fs (%.1f%%), x86_cpu=%.2fs (%.1f%%)",
                arm_cpu_s, arm_ratio * 100, x86_cpu_s, x86_ratio * 100)


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
        for source_key in ("demo", "framework", "tm"):
            source = metrics.get(source_key) or {}
            time_str = source.get(platform, "")
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


# ---------------------------------------------------------------------------
# Instruction-level backfill (from python-performance-kits annotate output)
# ---------------------------------------------------------------------------

_INSTRUCTION_SHARE_THRESHOLD = 0.5  # Only include instructions >= 0.5% share


def backfill_instructions(
    arm_run_dir: Path,
    x86_run_dir: Path,
    dataset: dict,
    share_threshold: float = _INSTRUCTION_SHARE_THRESHOLD,
) -> dict:
    """Backfill instruction-level disassembly data into dataset functions.

    Reads instruction_hotspots.csv produced by python-performance-kits
    annotate_perf_hotspots.py (``perf annotate --stdio``), filters to
    instructions above *share_threshold*, and attaches them to matching
    functions in the dataset.

    Parameters
    ----------
    arm_run_dir : Path
        ARM run directory containing perf/tables/instruction_hotspots.csv.
    x86_run_dir : Path
        x86 run directory containing perf/tables/instruction_hotspots.csv.
    dataset : dict
        The four-layer dataset dict to update in-place.
    share_threshold : float
        Minimum instruction_share (local % within function) to include.

    Returns
    -------
    dict with summary counts.
    """
    arm_instr_csv = arm_run_dir / "perf" / "tables" / "instruction_hotspots.csv"
    x86_instr_csv = x86_run_dir / "perf" / "tables" / "instruction_hotspots.csv"

    arm_instr = _read_perf_csv(arm_instr_csv)
    x86_instr = _read_perf_csv(x86_instr_csv)

    if not arm_instr and not x86_instr:
        logger.warning("No instruction_hotspots.csv found for either platform")
        return {"arm_instructions": 0, "x86_instructions": 0, "functions_enriched": 0}

    # Build lookup: symbol -> list of hot instructions per platform
    arm_by_sym = _group_instructions_by_symbol(arm_instr, share_threshold)
    x86_by_sym = _group_instructions_by_symbol(x86_instr, share_threshold)

    # Build symbol -> func lookup for matching
    sym_to_func = {f["symbol"]: f for f in dataset.get("functions", [])}

    enriched = 0
    for symbol, arm_rows in arm_by_sym.items():
        func = sym_to_func.get(symbol)
        if not func:
            continue
        func.setdefault("instructions", {})["arm"] = arm_rows
        enriched += 1

    for symbol, x86_rows in x86_by_sym.items():
        func = sym_to_func.get(symbol)
        if not func:
            continue
        func.setdefault("instructions", {})["x86"] = x86_rows
        if "arm" not in func.get("instructions", {}):
            enriched += 1

    summary = {
        "arm_instructions": sum(len(v) for v in arm_by_sym.values()),
        "x86_instructions": sum(len(v) for v in x86_by_sym.values()),
        "functions_enriched": enriched,
    }
    logger.info(
        "instruction backfill complete: %d arm / %d x86 instructions, "
        "%d functions enriched",
        summary["arm_instructions"], summary["x86_instructions"],
        summary["functions_enriched"],
    )
    return summary


def _group_instructions_by_symbol(
    rows: list[dict[str, str]],
    threshold: float,
) -> dict[str, list[dict[str, Any]]]:
    """Group instruction rows by symbol, filtering by share threshold.

    Returns {symbol: [{offset, share, text}, ...]} sorted by share desc.
    """
    from collections import defaultdict

    by_sym: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        share = _parse_float(row.get("instruction_share", "0"))
        if share < threshold:
            continue
        symbol = (row.get("symbol") or "").strip()
        if not symbol:
            continue
        by_sym[symbol].append({
            "offset": row.get("instruction_offset", ""),
            "share": share,
            "text": row.get("instruction_text", ""),
        })

    # Sort each symbol's instructions by share descending
    for symbol in by_sym:
        by_sym[symbol].sort(key=lambda x: x["share"], reverse=True)

    return dict(by_sym)
