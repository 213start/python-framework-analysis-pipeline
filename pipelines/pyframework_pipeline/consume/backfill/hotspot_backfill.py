"""Symbol/category/component hotspot aggregation backfill.

Owns the ``backfill_perf`` entry point: reads arm + x86 perf_records.csv,
maps categories/components, aggregates by symbol/category/component, and
populates Dataset.stackOverview / Dataset.functions with cross-platform
deltas.

The implementation currently lives in ``backfill.perf_backfill`` (it is
tightly coupled to that module's aggregation helpers); this module is the
spec §5.4 logical home and re-exports the public entry so callers import from
the consume layer.
"""
from __future__ import annotations

from ...backfill.perf_backfill import backfill_perf

__all__ = ["backfill_perf"]
