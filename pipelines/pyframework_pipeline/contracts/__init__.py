"""Shared OOP contracts for the framework analysis pipeline."""

from .adapter import (
    DisassemblySpec,
    FrameworkAdapter,
    PerfAttachSpec,
    WorkloadHandle,
)
from .step import RunContext, Step, StepError

__all__ = [
    "DisassemblySpec",
    "FrameworkAdapter",
    "PerfAttachSpec",
    "RunContext",
    "Step",
    "StepError",
    "WorkloadHandle",
]
