"""Registries for OOP pipeline steps and renderers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .contracts.step import Step


class StepRegistry:
    """Registry and dependency resolver for pipeline step classes."""

    def __init__(self) -> None:
        self._steps: dict[str, type[Step]] = {}

    def register(self, step_cls: type[Step]) -> type[Step]:
        name = getattr(step_cls, "name", "")
        if not name:
            raise ValueError("step class must define a non-empty name")
        self._steps[str(name)] = step_cls
        return step_cls

    def get(self, name: str) -> type[Step]:
        try:
            return self._steps[name]
        except KeyError as exc:
            raise KeyError(f"unknown step: {name}") from exc

    def names(self) -> set[str]:
        return set(self._steps)

    def resolve_plan(self, requested: Iterable[str]) -> list[type[Step]]:
        """Return requested steps plus dependencies in executable order."""

        requested_names = list(requested)
        producers: dict[str, str] = {}
        for step_name, step_cls in self._steps.items():
            for artifact in getattr(step_cls, "produces", ()):
                producers[str(artifact)] = step_name

        resolved: list[type[Step]] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def dependency_step_name(dep: str) -> str:
            if dep in self._steps:
                return dep
            if dep in producers:
                return producers[dep]
            raise ValueError(f"missing dependency: {dep}")

        def visit(step_name: str) -> None:
            if step_name in visited:
                return
            if step_name in visiting:
                raise ValueError(f"cycle detected at step: {step_name}")
            if step_name not in self._steps:
                raise KeyError(f"unknown step: {step_name}")

            visiting.add(step_name)
            step_cls = self._steps[step_name]
            for dep in getattr(step_cls, "requires", ()):
                visit(dependency_step_name(str(dep)))
            visiting.remove(step_name)

            visited.add(step_name)
            resolved.append(step_cls)

        for requested_name in requested_names:
            visit(str(requested_name))

        return resolved


class RendererRegistry:
    """Small registry for report renderers keyed by output format."""

    def __init__(self) -> None:
        self._renderers: dict[str, Any] = {}

    def register(self, name: str, renderer: Any) -> Any:
        if not name:
            raise ValueError("renderer name must be non-empty")
        self._renderers[name] = renderer
        return renderer

    def get(self, name: str) -> Any:
        try:
            return self._renderers[name]
        except KeyError as exc:
            raise KeyError(f"unknown renderer: {name}") from exc

    def names(self) -> set[str]:
        return set(self._renderers)


_STEP_REGISTRY = StepRegistry()
_RENDERER_REGISTRY = RendererRegistry()


def register_step(step_cls: type[Step]) -> type[Step]:
    """Class decorator for registering a pipeline step."""

    return _STEP_REGISTRY.register(step_cls)


def get_registry() -> StepRegistry:
    """Return the process-local step registry."""

    return _STEP_REGISTRY


def get_renderer_registry() -> RendererRegistry:
    """Return the process-local renderer registry."""

    return _RENDERER_REGISTRY
