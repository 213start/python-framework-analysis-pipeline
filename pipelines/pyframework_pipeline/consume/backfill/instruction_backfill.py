"""Instruction-level hotspot backfill.

Owns ``backfill_instructions``: reads instruction_hotspots.csv from arm/x86
and populates the Source-layer instruction details (top-N instructions per
symbol, with cross-platform delta). Per spec §5.4.

The implementation currently lives in ``backfill.perf_backfill``; this module
is the consume-layer logical home and re-exports the public entry.
"""
from __future__ import annotations

from ...backfill.perf_backfill import backfill_instructions

__all__ = ["backfill_instructions"]
