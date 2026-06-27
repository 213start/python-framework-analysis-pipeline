"""Pipeline step modules."""

from __future__ import annotations

_BUILTINS_REGISTERED = False


def register_builtin_steps() -> None:
    """Import built-in step classes so their decorators populate the registry."""

    global _BUILTINS_REGISTERED
    if _BUILTINS_REGISTERED:
        return

    from . import pipeline_steps as _pipeline_steps

    # Keep the imported module alive for static analyzers and future reload hooks.
    _pipeline_steps.__name__
    _BUILTINS_REGISTERED = True


__all__ = ["register_builtin_steps"]
