# UDF Benchmarking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Data-Juicer-style adapter for `stone31415/UDF_Benchmarking` with Python 3.11 and opt-in Python flamegraph support enabled in the reference project.

**Architecture:** Implement a new `udfbenchmarking` single-container framework adapter, a build script, orchestrator paths for deploy/benchmark/flamegraph, and a reference project. Convert upstream benchmark CSV/summary output into the existing timing artifact format so backfill/compare can stay unchanged.

**Tech Stack:** Python 3.11, Docker, Daft, OpenCV headless, scikit-image, psutil, perf, py-spy, unittest.

---

### Task 1: Register Environment And Config Surface

**Files:**
- Create: `pipelines/pyframework_pipeline/adapters/udfbenchmarking/__init__.py`
- Create: `pipelines/pyframework_pipeline/adapters/udfbenchmarking/adapter.py`
- Create: `pipelines/pyframework_pipeline/adapters/udfbenchmarking/environment.py`
- Create: `pipelines/pyframework_pipeline/adapters/udfbenchmarking/scripts/build-udfbenchmarking-image.sh`
- Modify: `pipelines/pyframework_pipeline/cli.py`
- Modify: `pipelines/pyframework_pipeline/config.py`
- Modify: `pipelines/pyframework_pipeline/environment/preflight.py`
- Modify: `schemas/environment.schema.json`
- Test: `pipelines/tests/test_udfbenchmarking_support.py`

- [ ] Write failing tests for plan generation, py-spy readiness, and config validation.
- [ ] Run `PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_udfbenchmarking_support -v` and confirm the new tests fail.
- [ ] Add the adapter, build script, CLI loader, config validation, and schema fields.
- [ ] Re-run the targeted tests and confirm they pass.

### Task 2: Add Workload Deployment And Benchmark Execution

**Files:**
- Modify: `pipelines/pyframework_pipeline/orchestrator.py`
- Test: `pipelines/tests/test_udfbenchmarking_support.py`

- [ ] Write failing tests for workload deployment into `/workspace/benchmark` and `perf record` benchmark execution.
- [ ] Run the targeted test file and confirm the new tests fail.
- [ ] Add `_run_udfbenchmarking_workload_deploy`, `_run_udfbenchmarking_benchmark`, and helpers to convert CSV output into timing JSON.
- [ ] Re-run the targeted tests and confirm they pass.

### Task 3: Add Optional Python Flamegraphs

**Files:**
- Modify: `pipelines/pyframework_pipeline/orchestrator.py`
- Test: `pipelines/tests/test_udfbenchmarking_support.py`

- [ ] Write a failing test that enables `software.pythonFlamegraph.enabled` and expects a `py-spy record` command plus `python/manifest.json`.
- [ ] Run the targeted test and confirm it fails.
- [ ] Add `_run_udfbenchmarking_python_flamegraph` using the same artifact contract as Data-Juicer.
- [ ] Re-run the targeted tests and confirm they pass.

### Task 4: Add Reference Project

**Files:**
- Create: `projects/udf-benchmarking-reference/project.yaml`
- Create: `projects/udf-benchmarking-reference/environment.yaml.example`
- Create: `projects/udf-benchmarking-reference/workload/config.yaml`
- Create: `projects/udf-benchmarking-reference/workload/.gitkeep`

- [ ] Add a smoke-sized `MockVideoE2EUDF` config with Python flamegraphs enabled in the environment example.
- [ ] Validate with `PYTHONPATH=pipelines python3 -m pyframework_pipeline config validate projects/udf-benchmarking-reference/project.yaml --skip-bridge-token`.

### Task 5: Verify And Commit

**Files:**
- All files changed above.

- [ ] Run `PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_udfbenchmarking_support -v`.
- [ ] Run `PYTHONPATH=pipelines python3 -m unittest discover -s pipelines/tests -v`.
- [ ] Run `python3 -m py_compile pipelines/pyframework_pipeline/orchestrator.py pipelines/pyframework_pipeline/adapters/udfbenchmarking/environment.py`.
- [ ] Run `git diff --check`.
- [ ] Commit implementation changes without staging generated run artifacts.

### Task 6: Real-Machine Validation

**Files:**
- Runtime artifacts under `projects/udf-benchmarking-reference/runs/<date>-e2e`.

- [ ] Copy `environment.yaml.example` to ignored `environment.yaml` and set `blue-98` and `zen5`.
- [ ] Run the pipeline in tmux with persistent logs, stopping before bridge publish.
- [ ] Confirm `PIPELINE_EXIT=0`, ARM/x86 timing, perf, compare, and Python flamegraph artifacts exist.
- [ ] Fix any real-machine issues with failing tests first.
