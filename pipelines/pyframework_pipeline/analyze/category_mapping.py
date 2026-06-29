"""CPython category mapping: the live L1/L2 classification for backfill.

Single source for mapping the analyze category_top / category_sub values
(produced by cpython_category_rules.json) to the four-layer framework L1 and
the L2 short names. This is CPython domain knowledge (fixed, framework-
agnostic); formerly inlined in backfill/perf_backfill.py.

Note: the old `CATEGORY_MAP` in acquisition/perf_profile.py (Tuple/Dict/List/
Int/Float ... keys) was stale dead code — it did not match the category_top
values the rules JSON actually produces (CPython.*, Kernel, Library, glibc)
and had no callers; it was removed during the Phase-2 convergence.
"""
from __future__ import annotations

# Handles the full cpython_category_rules.json taxonomy. Keys are the
# category_top values the rules JSON actually emits.
CATEGORY_TO_L1: dict[str, str] = {
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

# L2 sub-categories derived from category_sub. Only non-empty sub-categories
# are mapped; the prefix "CPython.Objects." etc. is stripped.
L2_SHORT_NAME: dict[str, str] = {
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


def category_to_l1(category_top: str) -> str:
    """Map a category_top to its framework L1 (lowercased short id)."""
    return CATEGORY_TO_L1.get(category_top, "unknown")


def l2_short_name(category_sub: str) -> str:
    """Map a category_sub to its L2 short name (empty if not mapped)."""
    return L2_SHORT_NAME.get(category_sub, "")


__all__ = ["CATEGORY_TO_L1", "L2_SHORT_NAME", "category_to_l1", "l2_short_name"]
