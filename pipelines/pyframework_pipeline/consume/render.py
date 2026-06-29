"""Registered report renderers (platform / compare).

Each renderer adapts an existing analyze render_report function to the
configurable path-in/path-out shape the consume layer exposes. The underlying
render_report functions RETURN the report text (they don't write a file), so
each renderer writes the returned text to ``output_path``.

Spec §5.3: renderers are selected by name; the render skeleton is unchanged
when a new format is added.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

# Import register_renderer from the consume package via a deferred import inside
# each decorator call below, OR import the underlying registry directly to avoid
# a circular import (consume/__init__ imports this module, which would otherwise
# import back from consume/__init__).
from ..registry import get_renderer_registry


def _register(name: str):
    """Return a decorator that registers a renderer under ``name``."""
    def _decorate(fn):
        get_renderer_registry().register(name, fn)
        return fn
    return _decorate


@_register("platform")
def render_platform(*, tables_dir: Path, output_path: Path, top_n: int = 50, **_: Any) -> Path:
    """Render a single-platform report from a tables directory."""
    from ..analyze.render_platform_report import render_report

    text = render_report(tables_dir, top_n, report_style="formal")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


@_register("platform_full")
def render_platform_full(*, tables_dir: Path, output_path: Path, top_n: int = 50, **_: Any) -> Path:
    """Render the full (verbose) single-platform report."""
    from ..analyze.render_platform_report import render_report

    text = render_report(tables_dir, top_n, report_style="full")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


@_register("compare")
def render_compare(*, tables_dir: Path, output_path: Path, top_n: int = 20, **_: Any) -> Path:
    """Render a cross-platform compare report from a tables directory."""
    from ..analyze.render_compare_report import render_report

    text = render_report(tables_dir, top_n)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


@_register("compare_integrated")
def render_compare_integrated(*, tables_dir: Path, output_path: Path, top_n: int = 20, **_: Any) -> Path:
    """Render the integrated cross-platform compare report."""
    from ..analyze.render_compare_integrated_report import render_report

    text = render_report(tables_dir, top_n)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


__all__ = [
    "render_platform",
    "render_platform_full",
    "render_compare",
    "render_compare_integrated",
]
