"""Consume-layer backfill: split perf_backfill into focused modules.

Per spec §5.4, the former monolithic ``backfill/perf_backfill.py`` (1232 lines)
is decomposed here by responsibility:

  * category_mapping     — CPython category/component/L1 resolution + display maps
  * hotspot_backfill     — symbol/category/component aggregation + the backfill_perf
                           entry (Dataset.stackOverview / Dataset.functions)
  * instruction_backfill — instruction-level hotspot backfill (backfill_instructions)
  * binding              — function-id generation + entity binding helpers

``backfill/perf_backfill.py`` remains as a backwards-compatible shim that
re-exports the public entry points, so existing importers
(``backfill.pipeline``, ``orchestrator``) are unchanged.

NOTE: the hotspot/instruction re-exports are resolved lazily (via ``__getattr__``)
to avoid a circular import: perf_backfill imports category_mapping from this
package at load time, which would otherwise trigger importing hotspot_backfill,
which imports perf_backfill.
"""
from __future__ import annotations

from . import category_mapping  # noqa: F401  (safe: no back-import to perf_backfill)


def __getattr__(name: str):  # PEP 562
    if name == "backfill_perf":
        from .hotspot_backfill import backfill_perf
        return backfill_perf
    if name == "backfill_instructions":
        from .instruction_backfill import backfill_instructions
        return backfill_instructions
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["category_mapping", "backfill_perf", "backfill_instructions"]
