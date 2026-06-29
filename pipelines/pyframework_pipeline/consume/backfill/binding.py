"""Function-id generation and entity binding helpers for backfill.

Per spec §5.4. The function-id generator (deterministic md5[:8] of the symbol)
lives here; the broader four-layer entity binding is coordinated by
``backfill.binding_generator`` (a sibling module), which this re-exports for
the consume-layer facade.
"""
from __future__ import annotations

import hashlib

from ...backfill.binding_generator import *  # noqa: F401,F403  (re-export binding helpers)


def generate_func_id(symbol: str) -> str:
    """Generate a deterministic function ID from symbol name."""
    digest = hashlib.md5(symbol.encode("utf-8")).hexdigest()[:8]
    return f"func_{digest}"


__all__ = ["generate_func_id"]
