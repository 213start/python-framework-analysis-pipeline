# Phase 3: pyframework_pipeline OOP Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the 2577-line `orchestrator.py` (whose `_execute_step` is one giant `if step_id == "3"/"4"/"5a"/...` chain) into a thin L0 orchestrator that drives a **Step registry** of self-registering `@register_step` classes; promote the vestigial `FrameworkAdapter` Protocol (currently only `framework_id` + `describe()`) into a real strategy contract covering the 6 acquisition differences; split the 1005-line `cli.py` by subcommand; and land the `consume/` layer (render / backfill / bridge) with a renderer registry. All existing behavior preserved; gated by the full test suite.

**Architecture:** A `Step` Protocol (`name`/`requires`/`produces`/`run(ctx)`) with a `@register_step` decorator populating a registry; orchestrator resolves an ordered plan by topological sort over `requires`/`produces` and drives it with the existing `PipelineRunState`. `FrameworkAdapter` gains 6 strategy methods returning description objects (`PerfAttachSpec`, `DisassemblySpec`, `WorkloadHandle`, …); acquisition subflows in `acquire/` consume those specs (single source of perf/objdump execution). The `consume/` layer holds render (with renderer registry), backfill (split off the 1265-line `perf_backfill.py`), and bridge.

**Tech Stack:** Python ≥ 3.10, stdlib only. `argparse`, `dataclasses`, `typing.Protocol`. Tests via `python3 -m unittest`.

**Reference spec:** `docs/superpowers/specs/2026-06-27-repo-integration-and-oop-refactor-design.md` (§Detailed Design 2, 3, 5, 6; §Phasing → Phase 3).

**Prerequisite:** Phases 1 & 2 complete (`analyze/` is a contract-driven subpackage; all Phase-1+2 tests green).

**Acceptance gate:** the full test suite (Phase 1's 20 + Phase 2's contract/subflow tests + Phase 3's new step/adapter tests) all pass. The existing end-to-end behavior — `PYTHONPATH=pipelines python3 -m pyframework_pipeline run <project.yaml>` — is unchanged.

---

## File Structure

**New contract pieces:**
- `contracts/adapter.py` — `FrameworkAdapter` Protocol (moved out of `adapters/base.py`) + spec dataclasses (`WorkloadHandle`, `PerfAttachSpec`, `DisassemblySpec`).
- `contracts/step.py` — `Step` Protocol, `RunContext`, `StepError` (moved from orchestrator).

**New registry:**
- `registry.py` — `@register_step` decorator + `StepRegistry`, `resolve_step_plan()`, `@register_renderer` + `RendererRegistry`.

**Step classes** (one per existing step_id, replacing the if-chain):
| New file | step_id | Wraps |
|---|---|---|
| `steps/environment_deploy.py` | `3` | `environment.deploy.deploy_plan` |
| `steps/workload_deploy.py` | `4` | `adapter.deploy_workload` |
| `steps/benchmark_run.py` | `5a` | `adapter.run_benchmark` |
| `steps/collect_perf.py` | `5b.1` | perf record (acquire/events) |
| `steps/run_analyze.py` | `5b.2` | the analyze subflows (Phase 2) |
| `steps/extract_cpython.py` | `5b.2b` | `_extract_cpython_sources` |
| `steps/collect_objdump.py` | `5b.3` | acquire/instruction |
| `steps/acquire_all.py` | `5c` | `_run_acquire_all` |
| `steps/backfill.py` | `6` | `consume/backfill` |
| `steps/compare.py` | `6b` | `consume/render` compare |
| `steps/bridge_publish.py` | `7` | `consume/bridge` |

**Adapter implementations** (promoted from stubs):
- `adapters/pyflink/adapter.py`, `adapters/datajuicer/adapter.py`, `adapters/udfbenchmarking/adapter.py` — each implements the 6 strategy methods, drawing logic out of orchestrator's `_run_*` functions.
- `adapters/registry.py` — `get_adapter(framework_id)`.

**Consume layer:**
- `consume/__init__.py`, `consume/render/` (registry + platform/compare renderers), `consume/backfill/` (category_mapping/hotspot_backfill/instruction_backfill/binding/step), `consume/bridge/`.

**CLI split:**
- `cli/` package: `cli/__init__.py` (main dispatcher), `cli/config.py`, `cli/environment.py`, `cli/run.py`, `cli/acquire.py`, `cli/analyze.py`, `cli/validate.py`, etc. — split out of the 1005-line `cli.py`.

**Existing preserved in orchestrator:** `PipelineRunState`, `_resolve_step_alias`, `_print_resume_hint`, the resume/force logic.

**Mapping existing dirs to the spec's layers (no rename required this phase):**
- Spec's `deploy/` layer ⇒ the existing `environment/` package (env deploy) + the workload portion of each adapter's `deploy_workload`. We do **not** create a new `deploy/` package this phase; `steps/environment_deploy.py` and `steps/workload_deploy.py` own the L2 deploy skeleton, calling `environment.deploy.deploy_plan` + `adapter.deploy_workload`.
- Spec's `acquire/` layer ⇒ the existing `acquisition/` package (perf_profile.py, machine_code.py) **plus** the perf/objdump execution pulled out of orchestrator. The acquisition subflows (`acquire_benchmark`, `record_perf`, `record_instruction`) live as functions the `5b.*` step classes call. We do **not** rename `acquisition/` → `acquire/` this phase (avoids a churn-only rename); the spec's `acquire/` is a logical name for these modules.
- The adapter is resolved **once** by the orchestrator into `RunContext.adapter` before driving the plan; steps read `ctx.adapter`, they do **not** each call `get_adapter()`. (Standardizes Issue 2 — single resolution point.)

---

## Task 1: Step Protocol + registry + RunContext

**Files:**
- Create: `pipelines/pyframework_pipeline/contracts/step.py`
- Create: `pipelines/pyframework_pipeline/registry.py`
- Test: `pipelines/tests/test_registry.py`

- [ ] **Step 1: Write the failing test**

Create `pipelines/tests/test_registry.py`:
```python
from __future__ import annotations
import unittest
from dataclasses import dataclass
from pathlib import Path
from pyframework_pipeline.registry import StepRegistry, register_step, get_registry
from pyframework_pipeline.contracts.step import Step, RunContext


class TestStepRegistry(unittest.TestCase):
    def setUp(self):
        self.reg = StepRegistry()  # fresh registry, not the global one

    def test_register_and_lookup(self):
        @self.reg.register
        class S:
            name = "x"
            requires = ()
            produces = ("p",)
            def run(self, ctx): pass
        self.assertIs(self.reg.get("x"), S)

    def test_topological_order_by_requires_produces(self):
        @self.reg.register
        class A:
            name = "a"; requires = (); produces = ("pa",)
            def run(self, ctx): pass
        @self.reg.register
        class B:
            name = "b"; requires = ("pa",); produces = ("pb",)
            def run(self, ctx): pass
        order = [s.name for s in self.reg.resolve_plan(["a", "b"])]
        self.assertEqual(order, ["a", "b"])

    def test_missing_dependency_raises(self):
        @self.reg.register
        class B:
            name = "b"; requires = ("pa",); produces = ("pb",)
            def run(self, ctx): pass
        with self.assertRaises(ValueError):
            self.reg.resolve_plan(["b"])

    def test_cycle_raises(self):
        @self.reg.register
        class A:
            name = "a"; requires = ("pb",); produces = ("pa",)
            def run(self, ctx): pass
        @self.reg.register
        class B:
            name = "b"; requires = ("pa",); produces = ("pb",)
            def run(self, ctx): pass
        with self.assertRaises(ValueError):
            self.reg.resolve_plan(["a", "b"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run, verify FAIL**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_registry -v
```

- [ ] **Step 3: Implement contracts/step.py + registry.py**

`contracts/step.py`:
```python
"""Step contract: self-describing pipeline stages with dependency metadata."""
from __future__ import annotations
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass
class RunContext:
    """Carried to each Step.run(): the adapter, run dir, config, path map, state."""
    adapter: Any                 # FrameworkAdapter
    project_path: Path
    run_dir: Path
    platform: str | None
    config: dict[str, Any]
    paths: dict[str, Path]       # configurable paths (defaults overridable)
    state: Any                   # PipelineRunState


@runtime_checkable
class Step(Protocol):
    name: str
    requires: tuple[str, ...]    # names of producing steps (or contract ids)
    produces: tuple[str, ...]
    def run(self, ctx: RunContext) -> None: ...


class StepError(Exception):
    """Raised when a pipeline step fails."""
```
(add `from dataclasses import dataclass` import.)

`registry.py`:
```python
"""Step + renderer registries with topological plan resolution."""
from __future__ import annotations
from typing import TypeVar, Callable, Iterable
from .contracts.step import Step

_T = TypeVar("_T")


class StepRegistry:
    def __init__(self) -> None:
        self._steps: dict[str, type] = {}

    def register(self, cls: type) -> type:
        name = getattr(cls, "name", None)
        if not name:
            raise ValueError(f"Step class {cls} has no 'name'")
        self._steps[name] = cls
        return cls

    def get(self, name: str) -> type:
        return self._steps[name]

    def names(self) -> list[str]:
        return list(self._steps)

    def resolve_plan(self, requested: Iterable[str]) -> list[type]:
        """Return steps in topological order; raise on missing dep or cycle."""
        # build produces -> step map
        produces: dict[str, str] = {}
        for name, cls in self._steps.items():
            for p in getattr(cls, "produces", ()):
                produces[p] = name
        ordered: list[type] = []
        seen: set[str] = set()
        visiting: set[str] = set()

        def visit(name: str) -> None:
            if name in seen:
                return
            if name in visiting:
                raise ValueError(f"cycle through {name}")
            visiting.add(name)
            cls = self._steps[name]
            for req in getattr(cls, "requires", ()):
                # req may be a step name or a produces id
                dep = req if req in self._steps else produces.get(req)
                if dep is None:
                    raise ValueError(f"{name} requires {req} (unsatisfied)")
                visit(dep)
            visiting.discard(name)
            seen.add(name)
            ordered.append(cls)

        for n in requested:
            if n not in self._steps:
                raise ValueError(f"unknown step {n}")
            visit(n)
        return ordered


_REGISTRY = StepRegistry()


def register_step(cls: type) -> type:
    return _REGISTRY.register(cls)


def get_registry() -> StepRegistry:
    return _REGISTRY


# Renderer registry (same pattern, simpler — no topo sort).
class RendererRegistry:
    def __init__(self) -> None:
        self._r: dict[str, type] = {}
    def register(self, name: str) -> Callable[[type], type]:
        def deco(cls: type) -> type:
            self._r[name] = cls
            return cls
        return deco
    def get(self, name: str) -> type:
        return self._r[name]


_RENDERERS = RendererRegistry()


def register_renderer(name: str):
    return _RENDERERS.register(name)


def get_renderers() -> RendererRegistry:
    return _RENDERERS
```

- [ ] **Step 4: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_registry -v
```

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/contracts/step.py \
        pipelines/pyframework_pipeline/registry.py \
        pipelines/tests/test_registry.py
git commit -m "feat(registry): add Step registry with topological plan resolution

Step Protocol + RunContext + StepRegistry.resolve_plan (requires/produces
topo sort, cycle/missing-dep detection). Global @register_step decorator
and a parallel RendererRegistry for the consume layer."
```

---

## Task 2: FrameworkAdapter Protocol — promote the vestigial base

**Files:**
- Create: `pipelines/pyframework_pipeline/contracts/adapter.py`
- Modify: `pipelines/pyframework_pipeline/adapters/base.py` (re-export from contracts/adapter.py for back-compat)

- [ ] **Step 1: Write the failing test (structural conformance)**

Append to `pipelines/tests/test_registry.py`:
```python
from pyframework_pipeline.contracts.adapter import FrameworkAdapter, PerfAttachSpec, WorkloadHandle, DisassemblySpec


class TestAdapterProtocol(unittest.TestCase):
    def test_protocol_lists_six_strategies(self):
        expected = {
            "deploy_workload", "run_benchmark", "perf_attach_strategy",
            "normalize_timing", "collect_flamegraph", "disassembly_source",
        }
        actual = {m for m in dir(FrameworkAdapter) if not m.startswith("_") and m != "framework_id"}
        self.assertEqual(actual, expected)
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement contracts/adapter.py**

```python
"""FrameworkAdapter: acquisition strategy contract.

Each method describes 'what to do for this framework' and returns a
description object; the actual perf/objdump execution lives once in acquire/.
(Phase 3 decision 3.5: adapters return description objects, not executors.)
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class WorkloadHandle:
    """Where the deployed workload lives + how to reach its process."""
    container: str | None
    host: str | None
    env_dir: Path
    notes: dict[str, str]


@dataclass(frozen=True)
class PerfAttachSpec:
    """How to attach perf to the framework's process."""
    target_pid: int | None
    container: str | None
    perf_bin: str
    python_bin: str


@dataclass(frozen=True)
class DisassemblySpec:
    """Where to objdump from for instruction-level data."""
    binaries: tuple[Path, ...]
    container: str | None


@runtime_checkable
class FrameworkAdapter(Protocol):
    framework_id: str

    def deploy_workload(self, env_dir: Path, *, workload_output: Path) -> WorkloadHandle: ...
    def run_benchmark(self, handle: WorkloadHandle, *, timing_output: Path) -> None: ...
    def perf_attach_strategy(self, handle: WorkloadHandle) -> PerfAttachSpec: ...
    def normalize_timing(self, raw_source: Path, *, output: Path) -> None: ...
    def collect_flamegraph(self, handle: WorkloadHandle, *, output: Path) -> None | None: ...
    def disassembly_source(self, handle: WorkloadHandle) -> DisassemblySpec: ...
```

Update `adapters/base.py` to re-export for back-compat:
```python
from ..contracts.adapter import (
    FrameworkAdapter, WorkloadHandle, PerfAttachSpec, DisassemblySpec,
)
__all__ = ["FrameworkAdapter", "WorkloadHandle", "PerfAttachSpec", "DisassemblySpec"]
```

- [ ] **Step 4: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_registry -v
```

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/contracts/adapter.py \
        pipelines/pyframework_pipeline/adapters/base.py
git commit -m "feat(adapter): promote FrameworkAdapter to 6-strategy contract

Adapters now describe deploy_workload/run_benchmark/perf_attach_strategy/
normalize_timing/collect_flamegraph/disassembly_source, returning
description objects (WorkloadHandle/PerfAttachSpec/DisassemblySpec).
adapters/base.py re-exports for back-compat."
```

---

## Task 3: Adapter registry + PyFlink adapter (extract from orchestrator)

This is the largest task. Extract pyflink's scattered logic from orchestrator's `_run_workload_deploy`, `_run_benchmark`, `_run_pyflink_*` helpers into `PyFlinkAdapter`. Do pyflink first (best-tested via `test_pipeline_integration` / `test_orchestrator_streaming`); datajuicer/udfbenchmarking follow the same pattern in Task 4.

**Files:**
- Create: `pipelines/pyframework_pipeline/adapters/registry.py`
- Modify: `pipelines/pyframework_pipeline/adapters/pyflink/adapter.py` (currently a stub)
- Modify: `pipelines/pyframework_pipeline/orchestrator.py` — `_run_workload_deploy`/`_run_benchmark` call into the adapter instead of branching on framework

- [ ] **Step 1: Read the pyflink-specific functions to extract**

```bash
sed -n '459,544p' pipelines/pyframework_pipeline/orchestrator.py    # _run_workload_deploy
sed -n '641,781p' pipelines/pyframework_pipeline/orchestrator.py    # _run_benchmark
grep -nE "def _run_|def _ensure_|def _find_|def _deploy_perf|def _parse_tm_count" pipelines/pyframework_pipeline/orchestrator.py
```
Capture which helpers are pyflink-only (e.g. `_ensure_jar`, `_ensure_pyflink_runner`, `_ensure_container_perf`, `_deploy_perf_wrapper`, `_parse_tm_count`, `_find_container_perf`).

- [ ] **Step 2: Write a failing test — adapter lookup + pyflink id**

Append to `pipelines/tests/test_registry.py`:
```python
from pyframework_pipeline.adapters.registry import get_adapter


class TestAdapterRegistry(unittest.TestCase):
    def test_known_frameworks_resolvable(self):
        for fid in ("pyflink", "datajuicer", "udfbenchmarking"):
            adapter = get_adapter(fid)
            self.assertEqual(adapter.framework_id, fid)
```

- [ ] **Step 3: Run, verify FAIL**

- [ ] **Step 4: Implement adapters/registry.py + PyFlinkAdapter**

`adapters/registry.py`:
```python
"""Adapter registry: resolve a FrameworkAdapter by framework_id."""
from __future__ import annotations
from ..contracts.adapter import FrameworkAdapter

_ADAPTERS: dict[str, FrameworkAdapter] = {}


def register_adapter(cls):
    fid = getattr(cls, "framework_id", None)
    if not fid:
        raise ValueError(f"adapter {cls} has no framework_id")
    _ADAPTERS[fid] = cls()
    return cls


def get_adapter(framework_id: str) -> FrameworkAdapter:
    if framework_id not in _ADAPTERS:
        raise KeyError(f"no adapter registered for {framework_id!r}")
    return _ADAPTERS[framework_id]
```

`adapters/pyflink/adapter.py`: move pyflink-specific helpers out of orchestrator into here as private methods, and implement the 6 Protocol methods by composing them. Each method should preserve the *exact* subprocess calls it makes today (verify against the orchestrator source you read in Step 1). Decorate with `@register_adapter`.

- [ ] **Step 5: Run the pyflink-focused tests — the regression gate**

```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_pipeline_integration \
  pipelines.tests.test_orchestrator_streaming \
  pipelines.tests.test_acquisition \
  -v 2>&1 | tail -10
```
Expected: OK. If failures, the extraction altered a command/flag — diff against the original orchestrator helper, fix, re-run. Use `systematic-debugging` if stuck.

- [ ] **Step 6: Commit**

```bash
git add pipelines/pyframework_pipeline/adapters/registry.py \
        pipelines/pyframework_pipeline/adapters/pyflink/
git commit -m "refactor(pyflink): extract framework logic into PyFlinkAdapter

Pyflink's workload deploy / benchmark / perf-attach / timing / disassembly
logic moved out of orchestrator into PyFlinkAdapter (6 strategies).
Orchestrator now resolves the adapter by framework_id. Behavior preserved;
test_pipeline_integration / test_orchestrator_streaming green."
```

---

## Task 4: DataJuicer + UdfBenchmarking adapters (same extraction pattern)

**Files:**
- Modify: `pipelines/pyframework_pipeline/adapters/datajuicer/adapter.py`
- Modify: `pipelines/pyframework_pipeline/adapters/udfbenchmarking/adapter.py`

- [ ] **Step 1: Read each framework's `_run_*` functions**

```bash
sed -n '544,592p' pipelines/pyframework_pipeline/orchestrator.py    # _run_datajuicer_workload_deploy
sed -n '781,894p' pipelines/pyframework_pipeline/orchestrator.py    # _run_datajuicer_benchmark
sed -n '894,1008p' pipelines/pyframework_pipeline/orchestrator.py   # _run_datajuicer_python_flamegraph
sed -n '592,641p' pipelines/pyframework_pipeline/orchestrator.py    # _run_udfbenchmarking_workload_deploy
sed -n '1008,1241p' pipelines/pyframework_pipeline/orchestrator.py  # _run_udfbenchmarking_benchmark + flamegraph
```

- [ ] **Step 2: Extract into each adapter class** (same as Task 3 Steps 4). Each implements the 6 strategies; `collect_flamegraph` returns None for pyflink-equivalent no-ops as appropriate. Decorate both with `@register_adapter`.

- [ ] **Step 3: Run framework-specific tests**

```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_datajuicer_support \
  pipelines.tests.test_udfbenchmarking_support \
  -v 2>&1 | tail -10
```
Expected: OK.

- [ ] **Step 4: Commit**

```bash
git add pipelines/pyframework_pipeline/adapters/datajuicer/ \
        pipelines/pyframework_pipeline/adapters/udfbenchmarking/
git commit -m "refactor(adapters): extract datajuicer + udfbenchmarking logic

Both frameworks' workload/benchmark/flamegraph/timing logic moved into
their adapter classes. All three adapters now uniform under the 6-strategy
contract; orchestrator holds no framework branches."
```

---

## Task 5: Convert each step_id into a self-registering Step class

Replace the `_execute_step` if-chain with Step classes. Do this incrementally: convert a few steps, keep the if-chain as fallback, run tests, repeat. The end state is `_execute_step` deleted.

**Files:**
- Create/Modify: the 11 files under `pipelines/pyframework_pipeline/steps/` listed in the File Structure table.
- Modify: `pipelines/pyframework_pipeline/orchestrator.py`.

- [ ] **Step 1: Convert step "3" (environment deploy) as the template**

Replace `steps/setup_environment.py` (or create `steps/environment_deploy.py`) — convert to:
```python
"""Step 3: environment deploy."""
from __future__ import annotations
from ..registry import register_step
from ..contracts.step import RunContext, Step


@register_step
class EnvironmentDeployStep:
    name = "3"
    requires: tuple[str, ...] = ()
    produces: tuple[str, ...] = ("environment",)

    def run(self, ctx: RunContext) -> None:
        from ..environment.deploy import deploy_plan
        plan_path = ctx.run_dir / ctx.platform / "environment-plan.json"
        # ... (move the body of _execute_step's step=="3" branch here verbatim,
        #      using ctx.project_path/ctx.run_dir/ctx.platform/ctx.config) ...
```

- [ ] **Step 2: Wire orchestrator to prefer the registry, fall back to the if-chain**

In `_execute_step`, before the if-chain, look up a registered Step by id and delegate. The orchestrator resolves the adapter **once** into `RunContext.adapter` (single resolution point):
```python
def _execute_step(step_id, project_path, run_dir, platform, *, yes=False, force=False, framework_id=None):
    from .registry import get_registry
    reg = get_registry()
    if step_id in reg.names():
        from .contracts.step import RunContext
        from .adapters.registry import get_adapter
        adapter = get_adapter(framework_id) if framework_id else None
        ctx = RunContext(adapter=adapter, project_path=project_path,
                         run_dir=run_dir, platform=platform,
                         config={"framework_id": framework_id, "yes": yes, "force": force},
                         paths={}, state=None)
        reg.get(step_id)().run(ctx)
        return
    # ... legacy if-chain kept as fallback during migration ...
```
The `run_pipeline` caller passes `framework_id` (resolved from `_framework_id(env_config)` or the project config) into each `_execute_step` call. Steps that need the adapter read `ctx.adapter` directly — they never call `get_adapter()` themselves.

- [ ] **Step 3: Run tests — confirm step 3 still works via the registry path**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_environment pipelines.tests.test_pipeline_integration -v 2>&1 | tail -10
```
Expected: OK.

- [ ] **Step 4: Convert the remaining 10 steps one at a time**

For each step_id (`4`, `5a`, `5b.1`, `5b.2`, `5b.2b`, `5b.3`, `5c`, `6`, `6b`, `7`): create its Step class moving the if-branch body verbatim, then re-run the relevant test. Steps that need the adapter (`4`, `5a`, `5b.*`, `5c`) read `ctx.adapter` (already resolved by the orchestrator — do not call `get_adapter()` inside `run()`). For `5b.2` (run_analyze), `run()` chains the Phase-2 subflows via their configurable paths: `parse(...)` → `classify(...)` → `aggregate(...)` → `annotate(...)`, all under `ctx.run_dir/<platform>/`, reusing the exact output paths the legacy `_run_perf_kits_on_remote`/`_run_acquire_all` produced. After each conversion, run a targeted test; commit each batch.

- [ ] **Step 5: Delete the legacy if-chain**

Once all 11 steps are registered and tested, remove the if/elif body of `_execute_step`, leaving only the registry delegation. Delete the now-unused private `_run_*` helpers that the steps have absorbed (verify each is unreferenced with `grep` before deleting).

- [ ] **Step 6: Full regression**

```bash
PYTHONPATH=pipelines python3 -m unittest discover -s pipelines/tests -p "test_*.py" -v 2>&1 | tail -15
```
Expected: OK.

- [ ] **Step 7: Commit (one or more logical commits during Step 4, then this final)**

```bash
git add pipelines/pyframework_pipeline/steps/ pipelines/pyframework_pipeline/orchestrator.py
git commit -m "refactor(orchestrator): replace if-chain with Step registry

Each step_id (3..7) is now a self-registering @register_step class under
steps/. orchestrator._execute_step delegates to the registry; the legacy
2577-line if-chain and absorbed _run_* helpers are removed. Resume/force
semantics preserved via PipelineRunState."
```

---

## Task 6: Consume layer — render registry + backfill split

**Files:**
- Create: `pipelines/pyframework_pipeline/consume/__init__.py`
- Create: `pipelines/pyframework_pipeline/consume/render/__init__.py`, `render/registry.py` (or reuse the global one), `render/platform.py`, `render/compare.py`
- Create: `pipelines/pyframework_pipeline/consume/backfill/__init__.py`, `backfill/category_mapping.py` (moved from analyze/ in Phase 2 — or import), `backfill/hotspot_backfill.py`, `backfill/instruction_backfill.py`, `backfill/binding.py`, `backfill/step.py`
- Create: `pipelines/pyframework_pipeline/consume/bridge/__init__.py` (move existing bridge logic here)
- Modify: `pipelines/pyframework_pipeline/backfill/perf_backfill.py` — reduce to a thin shim re-exporting from `consume/backfill/` (or delete if no external importers).

- [ ] **Step 1: Map the current 1265-line perf_backfill.py into the four backfill modules**

```bash
grep -nE "^def |^class |# ---" pipelines/pyframework_pipeline/backfill/perf_backfill.py | head -60
```
Group functions into category_mapping / hotspot_backfill / instruction_backfill / binding by their existing section comments (the file already comments its regions, e.g. line ~19 `# --- Category mapping`, line ~1156 `# Instruction-level backfill`).

- [ ] **Step 2: Move each region into its consume/backfill module** verbatim (no logic change), updating cross-references to sibling modules via relative imports.

- [ ] **Step 3: Register the two renderers**

```python
# consume/render/platform.py
from ...registry import register_renderer
from ...analyze.render_platform_report import render_report as _render

@register_renderer("platform")
class PlatformRenderer:
    def render(self, *, tables_dir, output_path): _render(...)
```
Same for `compare.py` wrapping `render_compare_report`/`render_compare_integrated_report`.

- [ ] **Step 4: Run backfill + render tests**

```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_backfill pipelines.tests.test_bridge \
  pipelines.tests.test_perf_text_reports pipelines.tests.test_perf_compare_visuals \
  -v 2>&1 | tail -10
```
Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/consume/ pipelines/pyframework_pipeline/backfill/
git commit -m "refactor(consume): split backfill, add render registry + bridge

The 1265-line perf_backfill.py is split into consume/backfill/
{category_mapping,hotspot_backfill,instruction_backfill,binding,step}.
Renderers (platform/compare) registered via @register_renderer. Bridge
moved under consume/. Behavior unchanged."
```

---

## Task 7: CLI split — break up the 1005-line cli.py

**Files:**
- Create: `pipelines/pyframework_pipeline/cli/` package: `__init__.py`, `config.py`, `environment.py`, `run.py`, `acquire.py`, `analyze.py`, `validate.py` (one per subcommand today).
- Modify: `pipelines/pyframework_pipeline/cli.py` → becomes a shim `from .cli import main` (keep for `python -m pyframework_pipeline`), or move `__main__.py`.

- [ ] **Step 1: Inventory the current subcommands**

```bash
grep -nE "add_parser\(|sub.add_parser|def cmd_" pipelines/pyframework_pipeline/cli.py | head -40
```
Capture each subcommand and its handler function.

- [ ] **Step 2: Move each subcommand into its own cli/<name>.py** exposing `add_parser(subparsers)` + `run(args)`. `cli/__init__.py`'s `main()` builds the top-level parser and dispatches.

- [ ] **Step 3: Verify the CLI surface is unchanged**

```bash
PYTHONPATH=pipelines python3 -m pyframework_pipeline --help
PYTHONPATH=pipelines python3 -m pyframework_pipeline config validate projects/pyflink-tpch-reference/project.yaml
PYTHONPATH=pipelines python3 -m pyframework_pipeline validate projects/pyflink-tpch-reference/project.yaml
```
Expected: same help text and exit codes as before the split.

- [ ] **Step 4: Run CLI tests**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_validate_cli pipelines.tests.test_environment -v 2>&1 | tail -10
```
Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/cli/ pipelines/pyframework_pipeline/cli.py
git commit -m "refactor(cli): split 1005-line cli.py by subcommand

Each subcommand (config/environment/run/acquire/analyze/validate) now
lives in cli/<name>.py with add_parser()+run(). The module cli.py is a
thin shim. CLI surface unchanged."
```

---

## Task 8: Final full regression + orchestrator size check

- [ ] **Step 1: Full suite**

```bash
PYTHONPATH=pipelines python3 -m unittest discover -s pipelines/tests -p "test_*.py" -v 2>&1 | tail -15
```
Expected: `OK`, zero failures/errors across all phases' tests.

- [ ] **Step 2: Confirm orchestrator shrank**

```bash
wc -l pipelines/pyframework_pipeline/orchestrator.py
```
Expected: well under 1000 lines (the 11 if-branches + `_run_*` helpers absorbed into steps/adapters/consume). If still large, identify what remains and whether it belongs in a step/adapter/consume module — flag if genuinely orchestrator-core (state/resume) vs. stray logic.

- [ ] **Step 3: Smoke-test the end-to-end entry**

```bash
PYTHONPATH=pipelines python3 -m pyframework_pipeline --help >/dev/null && echo "CLI OK"
PYTHONPATH=pipelines python3 -c "from pyframework_pipeline import orchestrator; print('import OK')"
```
Expected: `CLI OK` and `import OK`.

- [ ] **Step 4: Commit any final tidy**

```bash
git status --porcelain   # expected empty, or commit remaining fixups
```

---

## Phase 3 Definition of Done

- [ ] `Step` Protocol + `@register_step` registry with topological `resolve_plan`; `RunContext` carries adapter/config/paths.
- [ ] `FrameworkAdapter` is a real 6-strategy contract; pyflink/datajuicer/udfbenchmarking adapters implemented; `adapters/registry.get_adapter(framework_id)`.
- [ ] All 11 step_ids are self-registering Step classes; `_execute_step` if-chain deleted; orchestrator is a thin driver over the registry + `PipelineRunState`.
- [ ] `consume/` layer: render (registry), backfill (split off the 1265-line file), bridge.
- [ ] `cli/` package split by subcommand; `cli.py` is a shim.
- [ ] **G2 gate: full test suite green; `python3 -m pyframework_pipeline` surface unchanged.**
- [ ] orchestrator.py dramatically smaller; no framework branches remain in it.
