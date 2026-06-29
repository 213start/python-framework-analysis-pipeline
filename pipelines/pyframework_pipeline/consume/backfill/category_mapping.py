"""CPython category / component / L1 resolution for the consume backfill.

The mapping data and resolvers (formerly inlined in backfill/perf_backfill.py)
live here as CPython domain knowledge (framework-agnostic). The base L1/L2
taxonomy is single-sourced in ``analyze.category_mapping``; this module adds
the display names, component mapping, legacy fallback, and source-provenance
resolution used by the backfill aggregation steps.
"""
from __future__ import annotations

from typing import Any

from ...analyze.category_mapping import CATEGORY_TO_L1 as _CATEGORY_TO_L1
from ...analyze.category_mapping import L2_SHORT_NAME as _L2_SHORT_NAME  # noqa: F401

# Source file provenance derived from shared_object.
LIB_DISPLAY: dict[str, str] = {
    "libpython": "CPython",
    "libscipy_openblas": "OpenBLAS (scipy)",
    "libopenblas": "OpenBLAS",
    "libarrow_python": "PyArrow",
    "libarrow.so": "Apache Arrow",
    "libjvm": "JVM (OpenJDK)",
}

KERNEL_SYMBOLS = {
    "unmap_page_range", "copy_page_range", "__tlb_remove_page",
    "free_pages_and_swap_cache", "__alloc_pages_nodemask",
}

# Human-readable display names for L1 categories.
CATEGORY_DISPLAY: dict[str, str] = {
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
LEGACY_CATEGORY_MAP: dict[str, str] = {
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

# Component mapping: shared_object prefix -> component.
COMPONENT_MAP: dict[str, str] = {
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

# analyze category_top -> component (when available).
CATEGORY_TO_COMPONENT: dict[str, str] = {
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

# Human-readable display names for components.
COMPONENT_DISPLAY: dict[str, str] = {
    "cpython": "CPython",
    "glibc": "glibc",
    "kernel": "Kernel",
    "third_party": "Third Party",
    "bridge_runtime": "Bridge Runtime",
    "library": "Library",
    "unknown": "Unknown",
}


def resolve_source_info(symbol: str, shared_object: str,
                        source_map: dict[str, dict] | None = None) -> dict[str, str]:
    """Derive sourceFile and origin from shared_object, symbol, and source map."""
    if shared_object == "[kernel.kallsyms]":
        return {"sourceFile": "Linux Kernel", "origin": "kernel"}
    if source_map and symbol in source_map:
        return {"sourceFile": source_map[symbol].get("sourceFile", ""), "origin": "CPython"}
    so_lower = shared_object.lower()
    for lib_prefix, display in LIB_DISPLAY.items():
        if lib_prefix.lower() in so_lower:
            return {"sourceFile": display, "origin": display}
    if "libc" in so_lower or "ld-linux" in so_lower:
        return {"sourceFile": "glibc", "origin": "glibc"}
    if shared_object and shared_object != "[unknown]":
        return {"sourceFile": shared_object, "origin": shared_object}
    return {"sourceFile": "", "origin": ""}


def resolve_component(shared_object: str, category_top: str = "") -> str:
    """Map a shared_object + category_top to a component identifier.

    Prefers category-based resolution for CPython/Kernel/glibc, falls back
    to shared_object prefix matching, then generic catch-all for libraries.
    """
    if category_top:
        comp = CATEGORY_TO_COMPONENT.get(category_top)
        if comp:
            return comp

    so = shared_object.strip()
    so_lower = so.lower()
    for prefix, component in COMPONENT_MAP.items():
        if so_lower.startswith(prefix.lower()):
            return component
    if "python" in so_lower and ("lib" in so_lower or so_lower.startswith("python")):
        return "cpython"
    if so_lower.endswith(".so") or ".so." in so_lower:
        return "third_party"
    return "unknown"


def resolve_l1_category(category_top: str) -> str:
    """Map a category_top to framework L1 (handles CPython.* + legacy)."""
    l1 = _CATEGORY_TO_L1.get(category_top)
    if l1:
        return l1
    return LEGACY_CATEGORY_MAP.get(category_top, "unknown")


__all__ = [
    "CATEGORY_DISPLAY",
    "CATEGORY_TO_COMPONENT",
    "COMPONENT_DISPLAY",
    "COMPONENT_MAP",
    "KERNEL_SYMBOLS",
    "LEGACY_CATEGORY_MAP",
    "LIB_DISPLAY",
    "resolve_component",
    "resolve_l1_category",
    "resolve_source_info",
]
