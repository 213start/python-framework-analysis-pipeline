"""Consume layer: turn analysis artifacts into presentable outputs.

The consume layer is a pure downstream consumer of the analyze stage's final
artifacts. It produces no new analysis conclusions — only transforms them into
presentation forms:

  * render  — analyze artifacts -> human-readable reports (platform / compare)
  * backfill— analyze artifacts -> four-layer JSON (Framework/Dataset/Source/...)
  * bridge  — four-layer JSON    -> Issue / Discussion

The three sub-concepts live in their own packages (``analyze/render_*`` for
report rendering, ``backfill/`` for four-layer JSON assembly, ``bridge/`` for
publishing); this module is the cohesive entry point and owns the renderer
registry that selects a report format by name.

Per spec §5.3, renderers register via the decorator and are selected by name;
new report formats only register a renderer, no change to the render skeleton.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ..registry import get_renderer_registry

# Re-export the backfill and bridge sub-packages so callers have one import.
from .. import backfill  # noqa: F401  (consume facade re-export)
from .. import bridge  # noqa: F401  (consume facade re-export)

# Importing the render module triggers @register_renderer at import time.
from . import render as _render  # noqa: F401


def register_renderer(name: str) -> Callable[[Any], Any]:
    """Register a renderer (callable) under ``name`` in the renderer registry."""

    def _decorate(renderer: Any) -> Any:
        get_renderer_registry().register(name, renderer)
        return renderer

    return _decorate


def get_renderer(name: str) -> Any:
    """Return the renderer registered under ``name``."""
    return get_renderer_registry().get(name)


def render(name: str, *, tables_dir: Path, output_path: Path, **kwargs: Any) -> Any:
    """Run the named renderer over ``tables_dir``, writing to ``output_path``."""
    return get_renderer(name)(tables_dir=tables_dir, output_path=output_path, **kwargs)


__all__ = ["register_renderer", "get_renderer", "render", "backfill", "bridge"]
