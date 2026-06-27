# Phase 1: Mechanical Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `python-performance-kits` (16 scripts, ~5777 lines) into `pyframework_pipeline/analyze/` as a first-class subpackage, switch all host-side coupling points from subprocess-on-vendor to in-package imports, migrate the 9 kits tests, and delete the git submodule — with **zero algorithm change** and all tests green.

**Architecture:** Single package `pyframework_pipeline` gains a new `analyze/` subpackage. The 16 kits scripts are copied verbatim (re-named off the `perf_insights` path) into `analyze/`, their inter-script `import` statements rewritten to package-relative form, the 9 kits tests copied in with import paths rewritten, and the 4 host-side coupling points (`acquisition/perf_profile.py`, `acquisition/machine_code.py`, `compare/pipeline.py`, `orchestrator._run_perf_kits_on_remote`) switched from subprocess-on-vendor to package imports. The one **exception** is `_run_perf_kits_on_remote`, which must continue to ship *flat script files* into a remote container (the container has no access to this repo's package) — it is re-pointed to read the same scripts from `analyze/` as flat files rather than from `vendor/`.

**Tech Stack:** Python ≥ 3.10, stdlib only (no pytest, no third-party deps). Tests run via `python3 -m unittest discover` with `PYTHONPATH=pipelines`.

**Reference spec:** `docs/superpowers/specs/2026-06-27-repo-integration-and-oop-refactor-design.md` (§Phasing → Phase 1).

**Acceptance gate (G2 baseline for this phase):** the existing 11 test modules in `pipelines/tests/` **plus** the 9 migrated kits test modules all pass, before the submodule is deleted.

---

## File Structure

**New package `pipelines/pyframework_pipeline/analyze/`** (created in Task 2), holding the 16 scripts copied verbatim from `vendor/python-performance-kits/scripts/perf_insights/`:

| Source (vendor) | Target (analyze/) | Responsibility |
|---|---|---|
| `perf_data_to_csv.py` | `analyze/perf_data_to_csv.py` | `perf.data` → `perf_script.csv` |
| `perf_script_to_csv.py` | `analyze/perf_script_to_csv.py` | `perf script` text → records |
| `normalize_perf_records.py` | `analyze/normalize_perf_records.py` | record normalization |
| `perf_analysis_common.py` | `analyze/perf_analysis_common.py` | shared classify/aggregate/CSV helpers |
| `summarize_platform_perf.py` | `analyze/summarize_platform_perf.py` | aggregate → summary tables |
| `annotate_perf_hotspots.py` | `analyze/annotate_perf_hotspots.py` | instruction-level hotspot annotation |
| `show_symbol_machine_code.py` | `analyze/show_symbol_machine_code.py` | single-symbol machine-code report |
| `run_single_platform_pipeline.py` | `analyze/run_single_platform_pipeline.py` | single-platform entry point |
| `run_compare_pipeline.py` | `analyze/run_compare_pipeline.py` | cross-platform compare entry point |
| `compare_platform_perf.py` | `analyze/compare_platform_perf.py` | comparison logic |
| `render_platform_report.py` | `analyze/render_platform_report.py` | platform text report |
| `render_platform_visuals.py` | `analyze/render_platform_visuals.py` | platform SVG visuals |
| `render_platform_machine_code_report.py` | `analyze/render_platform_machine_code_report.py` | machine-code report |
| `render_compare_report.py` | `analyze/render_compare_report.py` | compare text report |
| `render_compare_integrated_report.py` | `analyze/render_compare_integrated_report.py` | compare integrated report |
| `render_compare_visuals.py` | `analyze/render_compare_visuals.py` | compare SVG visuals |

**Shared resource:** `vendor/.../scripts/perf_insights/cpython_category_rules.json` → `analyze/cpython_category_rules.json`.

**Modified files (host-side coupling points):**
- `pipelines/pyframework_pipeline/acquisition/perf_profile.py` — drop `DEFAULT_KITS_DIR`, import `analyze.run_single_platform_pipeline`.
- `pipelines/pyframework_pipeline/acquisition/machine_code.py` — drop `DEFAULT_KITS_DIR`, import `analyze.annotate_perf_hotspots`.
- `pipelines/pyframework_pipeline/compare/pipeline.py` — drop `_COMPARE_SCRIPT`, import `analyze.run_compare_pipeline`.
- `pipelines/pyframework_pipeline/orchestrator.py` — `_run_perf_kits_on_remote` re-pointed; `_init_submodules` becomes a no-op; submodule init removed from any other call site.
- `.gitmodules` — remove `python-performance-kits` entry.
- `vendor/python-performance-kits` — submodule removed via `git rm`.

**New tests (migrated from `vendor/python-performance-kits/tests/`):** `pipelines/tests/test_perf_data_to_csv.py`, `test_perf_analysis_pipeline.py` (= common helpers), `test_perf_annotation_pipeline.py`, `test_perf_runner.py` (= single platform), `test_perf_compare_runner.py`, `test_perf_compare_visuals.py`, `test_perf_text_reports.py`, `test_perf_visuals.py`, `test_show_symbol_machine_code.py` — each with import paths rewritten from `scripts.perf_insights.X` → `pyframework_pipeline.analyze.X`.

---

## Task 1: Snapshot the source-of-truth commit hash

The submodule is being deleted; record its exact commit for the provenance commit message.

**Files:** none (informational, captured into the Task 2 commit message).

- [ ] **Step 1: Record the kits submodule pinned commit**

Run from repo root:
```bash
git submodule status vendor/python-performance-kits
```
Expected: one line like ` <40-hex-sha> vendor/python-performance-kits (...)`. Copy the 40-hex `<sha>` — it will be cited in the Task 2 commit message as the provenance of the copied code.

- [ ] **Step 2: Confirm the kits working tree is clean (unmodified)**

Run:
```bash
git -C vendor/python-performance-kits status --porcelain
```
Expected: empty output (no local modifications to the submodule). If output is non-empty, **stop** — the submodule has uncommitted changes that must be reviewed before copying; flag to the user.

---

## Task 2: Create the `analyze/` subpackage and copy kits scripts verbatim

**Files:**
- Create: `pipelines/pyframework_pipeline/analyze/__init__.py`
- Create: `pipelines/pyframework_pipeline/analyze/<16 scripts>` (copied from vendor)
- Create: `pipelines/pyframework_pipeline/analyze/cpython_category_rules.json`

- [ ] **Step 1: Create the package directory and `__init__.py`**

```bash
mkdir -p pipelines/pyframework_pipeline/analyze
```

Write `pipelines/pyframework_pipeline/analyze/__init__.py` with only a module docstring (no exports yet — Phase 2 defines the public API):
```python
"""Performance analysis pipeline (acquired from python-performance-kits).

Hosts the perf-data parsing, CPython category classification, aggregation,
annotation, and reporting scripts. Phase 1 preserves the original script
structure verbatim; Phase 2 refactors it into a domain-modelled pipeline.
"""
```

- [ ] **Step 2: Copy the 16 scripts and the rules JSON verbatim**

Copy every `.py` and the rules JSON from the vendor scripts dir into `analyze/`, preserving filenames exactly (the spec says drop the *path* `perf_insights`, not the filenames — filenames like `perf_data_to_csv.py` stay):
```bash
cp vendor/python-performance-kits/scripts/perf_insights/*.py \
   pipelines/pyframework_pipeline/analyze/
cp vendor/python-performance-kits/scripts/perf_insights/cpython_category_rules.json \
   pipelines/pyframework_pipeline/analyze/
```

Verify all 16 scripts + the JSON arrived:
```bash
ls pipelines/pyframework_pipeline/analyze/
```
Expected output contains exactly these 17 entries: `__init__.py`, `cpython_category_rules.json`, `perf_data_to_csv.py`, `perf_script_to_csv.py`, `normalize_perf_records.py`, `perf_analysis_common.py`, `summarize_platform_perf.py`, `annotate_perf_hotspots.py`, `show_symbol_machine_code.py`, `run_single_platform_pipeline.py`, `run_compare_pipeline.py`, `compare_platform_perf.py`, `render_platform_report.py`, `render_platform_visuals.py`, `render_platform_machine_code_report.py`, `render_compare_report.py`, `render_compare_integrated_report.py`, `render_compare_visuals.py`.

- [ ] **Step 3: Do NOT commit yet**

The copied files have `import` statements pointing at `scripts.perf_insights.X` (their old location). Those are fixed in Task 3 before any commit, so the tree is importable in one atomic commit. Proceed to Task 3.

---

## Task 3: Rewrite intra-package imports to package-relative form

The 16 scripts import each other as `from perf_analysis_common import ...` (flat siblings, since they ran from one directory) or `from scripts.perf_insights.X import ...` (when imported by tests). Inside the new package they must import each other relatively.

**Files:**
- Modify: every `pipelines/pyframework_pipeline/analyze/*.py` that imports a sibling.

- [ ] **Step 1: Enumerate every inter-script import in the copied files**

Run:
```bash
grep -rnE "^(import|from) (perf_analysis_common|perf_data_to_csv|perf_script_to_csv|normalize_perf_records|summarize_platform_perf|annotate_perf_hotspots|show_symbol_machine_code|run_single_platform_pipeline|run_compare_pipeline|compare_platform_perf|render_platform_report|render_platform_visuals|render_platform_machine_code_report|render_compare_report|render_compare_integrated_report|render_compare_visuals)" pipelines/pyframework_pipeline/analyze/
```
Capture the output — every matched line must be rewritten.

- [ ] **Step 2: Rewrite each matched import to relative form**

For each line `from <sibling> import X` → `from .<sibling> import X`.
For each line `import <sibling>` → `from . import <sibling>`.

Examples (apply to every match found in Step 1):
```python
# before
from perf_analysis_common import load_rules, classify_sample, write_csv_rows
# after
from .perf_analysis_common import load_rules, classify_sample, write_csv_rows
```
```python
# before
import perf_analysis_common
# after
from . import perf_analysis_common
```

- [ ] **Step 3: Verify no flat sibling imports remain**

Re-run the Step 1 grep. Expected: **no matches**. If any remain, rewrite them (Step 2) and re-verify.

- [ ] **Step 4: Smoke-test that the package imports**

From repo root:
```bash
PYTHONPATH=pipelines python3 -c "import pyframework_pipeline.analyze.run_single_platform_pipeline; import pyframework_pipeline.analyze.run_compare_pipeline; import pyframework_pipeline.analyze.annotate_perf_hotspots; print('OK')"
```
Expected: prints `OK`, no traceback.

- [ ] **Step 5: Commit the new subpackage**

```bash
git add pipelines/pyframework_pipeline/analyze/
git commit -m "feat(analyze): integrate python-performance-kits scripts

Verbatim copy of the 16 perf_insights scripts + cpython_category_rules.json
from vendor/python-performance-kits @ <sha from Task 1>, relocated into
pyframework_pipeline/analyze/ as a first-class subpackage. Intra-package
imports rewritten to relative form. Algorithms unchanged.

Provenance: vendor/python-performance-kits submodule commit <sha>.
See docs/superpowers/specs/2026-06-27-repo-integration-and-oop-refactor-design.md"
```
(Replace `<sha from Task 1>` with the actual hash captured in Task 1.)

---

## Task 4: Migrate the 9 kits tests with rewritten import paths

**Files:**
- Create: `pipelines/tests/test_perf_data_to_csv.py` (migrated from vendor)
- Create: `pipelines/tests/test_perf_analysis_pipeline.py` (migrated)
- Create: `pipelines/tests/test_perf_annotation_pipeline.py` (migrated)
- Create: `pipelines/tests/test_perf_runner.py` (migrated)
- Create: `pipelines/tests/test_perf_compare_runner.py` (migrated)
- Create: `pipelines/tests/test_perf_compare_visuals.py` (migrated)
- Create: `pipelines/tests/test_perf_text_reports.py` (migrated)
- Create: `pipelines/tests/test_perf_visuals.py` (migrated)
- Create: `pipelines/tests/test_show_symbol_machine_code.py` (migrated)

- [ ] **Step 1: Copy the 9 test files verbatim**

```bash
cp vendor/python-performance-kits/tests/test_perf_data_to_csv.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_analysis_pipeline.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_annotation_pipeline.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_runner.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_compare_runner.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_compare_visuals.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_text_reports.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_perf_visuals.py pipelines/tests/
cp vendor/python-performance-kits/tests/test_show_symbol_machine_code.py pipelines/tests/
```

- [ ] **Step 2: Rewrite `scripts.perf_insights.X` → `pyframework_pipeline.analyze.X` in all 9 files**

Run (this is the single mechanical rewrite):
```bash
python3 - <<'PY'
from pathlib import Path
for p in Path("pipelines/tests").glob("test_perf_*.py"):
    t = p.read_text(encoding="utf-8")
    t = t.replace("scripts.perf_insights.", "pyframework_pipeline.analyze.")
    p.write_text(t, encoding="utf-8")
# also the one non-test_perf_ named file
p = Path("pipelines/tests/test_show_symbol_machine_code.py")
t = p.read_text(encoding="utf-8")
t = t.replace("scripts.perf_insights.", "pyframework_pipeline.analyze.")
p.write_text(t, encoding="utf-8")
PY
```

- [ ] **Step 3: Verify no `scripts.perf_insights` references remain in tests**

```bash
grep -rn "scripts.perf_insights" pipelines/tests/ || echo "none"
```
Expected: `none`.

- [ ] **Step 4: Run the 9 migrated tests and confirm they pass**

From repo root:
```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_perf_data_to_csv \
  pipelines.tests.test_perf_analysis_pipeline \
  pipelines.tests.test_perf_annotation_pipeline \
  pipelines.tests.test_perf_runner \
  pipelines.tests.test_perf_compare_runner \
  pipelines.tests.test_perf_compare_visuals \
  pipelines.tests.test_perf_text_reports \
  pipelines.tests.test_perf_visuals \
  pipelines.tests.test_show_symbol_machine_code \
  -v
```
Expected: all 9 modules report `OK` (no failures, no errors). The kits algorithms are unchanged from vendor, so these tests must pass as-is — they are the G2 gate proving the copy is faithful.

- [ ] **Step 5: Commit the migrated tests**

```bash
git add pipelines/tests/test_perf_*.py pipelines/tests/test_show_symbol_machine_code.py
git commit -m "test(analyze): migrate kits tests into pipelines/tests

9 test modules copied from vendor/python-performance-kits/tests with import
paths rewritten scripts.perf_insights.X -> pyframework_pipeline.analyze.X.
All pass unchanged, proving the Phase-1 copy is algorithm-faithful."
```

---

## Task 5: Switch host-side coupling points to package imports — acquisition

Two files in `acquisition/` currently resolve a vendor script path and `subprocess.run` it. Both now import the package function directly.

### Task 5a: `acquisition/perf_profile.py`

**Files:**
- Modify: `pipelines/pyframework_pipeline/acquisition/perf_profile.py` (lines ~16-17, ~47, ~75-127)

- [ ] **Step 1: Read the current file to confirm line numbers**

```bash
sed -n '1,130p' pipelines/pyframework_pipeline/acquisition/perf_profile.py
```
Confirm: line 16-17 define `DEFAULT_KITS_DIR`; `collect_perf` takes `kits_dir` param (line ~47); the body builds `pipeline_script` path (line ~86) and `subprocess.run`s it.

- [ ] **Step 2: Replace the subprocess invocation with a package import call**

Replace the `subprocess`-based body of `collect_perf`. Concretely:
- Remove `import subprocess` and `import sys` if now unused (check other uses first — `sys` may be used elsewhere; only remove if unused).
- Remove the `DEFAULT_KITS_DIR` constant (lines 16-17) and the `kits_dir` parameter from `collect_perf`'s signature.
- Replace the script-path resolution + `subprocess.run` block with a direct call to the pipeline's main function.

The target function in `analyze/run_single_platform_pipeline.py` is the one currently invoked as `python run_single_platform_pipeline.py <perf.data> --output <dir> --benchmark <b> --platform <p> [--top-n N]`. Before editing, read it to find its programmatic entry (a `main(args)` or `run(...)`) — every kits entry script has one:

```bash
grep -nE "^def (main|run|pipeline|run_single_platform)" pipelines/pyframework_pipeline/analyze/run_single_platform_pipeline.py
```

Then import and call it. Example shape (adapt to the actual signature found):
```python
from ..analyze.run_single_platform_pipeline import run_single_platform_pipeline

def collect_perf(
    run_dir: Path,
    platform: str,
    perf_data: Path | None = None,
    benchmark: str = "tpch",
    platform_id: str = "",
    top_n: int = 50,
) -> dict[str, Any]:
    if perf_data is None:
        perf_data = run_dir / "perf.data"
    if not perf_data.exists():
        return {"status": "skipped", "reason": f"perf.data not found: {perf_data}"}

    perf_dir = run_dir / "perf"
    perf_dir.mkdir(parents=True, exist_ok=True)

    run_single_platform_pipeline(
        perf_data=perf_data,
        output=perf_dir,
        benchmark=benchmark,
        platform=platform_id or platform,
        top_n=top_n,
    )
    # ... keep the existing output-file collection loop that reads
    # perf_dir/"tables"/*.csv and returns the {"status": "collected", "files": ...} dict.
```

> If `run_single_platform_pipeline.py` exposes only a CLI `main()` that parses `sys.argv` (no clean `run(...)` function), **do not** shell out. Instead, in Task 5a first add a thin `run(...)` wrapper to `analyze/run_single_platform_pipeline.py` that the existing `main()` also calls (DRY), then import that wrapper here. The goal of Phase 1 is "import not subprocess"; a small wrapper is in keeping with that.

- [ ] **Step 3: Verify the module imports**

```bash
PYTHONPATH=pipelines python3 -c "from pyframework_pipeline.acquisition import perf_profile; print('OK')"
```
Expected: `OK`.

### Task 5b: `acquisition/machine_code.py`

**Files:**
- Modify: `pipelines/pyframework_pipeline/acquisition/machine_code.py` (lines ~21, ~87, ~112-143)

- [ ] **Step 1: Replace the `annotate_perf_hotspots` subprocess call with a package import**

This file calls the `annotate_perf_hotspots.py` script via subprocess (lines ~127-143). Apply the same transformation as 5a:
- Remove `DEFAULT_KITS_DIR` (line 21) and the `kits_dir` parameter from `collect_asm`.
- Read `analyze/annotate_perf_hotspots.py` to find its programmatic entry:
  ```bash
  grep -nE "^def (main|run|annotate|render)" pipelines/pyframework_pipeline/analyze/annotate_perf_hotspots.py
  ```
- Replace the `subprocess.run([... annotate_script, records_csv, "--perf-data", ..., "--output", ..., "--top-n", ...])` block with a direct call to that entry function, passing the same inputs as keyword arguments. Add a thin `run(...)` wrapper to `annotate_perf_hotspots.py` first if it only has a CLI `main()`.

- [ ] **Step 2: Verify the module imports**

```bash
PYTHONPATH=pipelines python3 -c "from pyframework_pipeline.acquisition import machine_code; print('OK')"
```
Expected: `OK`.

- [ ] **Step 3: Commit Task 5**

```bash
git add pipelines/pyframework_pipeline/acquisition/perf_profile.py \
        pipelines/pyframework_pipeline/acquisition/machine_code.py \
        pipelines/pyframework_pipeline/analyze/run_single_platform_pipeline.py \
        pipelines/pyframework_pipeline/analyze/annotate_perf_hotspots.py
git commit -m "refactor(acquisition): call analyze scripts via import, not subprocess

perf_profile.collect_perf and machine_code.collect_asm now import the
analyze package functions directly (run_single_platform_pipeline,
annotate_perf_hotspots) instead of resolving vendor paths and shelling
out. Thin run() wrappers added where entry scripts only exposed main().
Behavior unchanged."
```

---

## Task 6: Switch host-side coupling — `compare/pipeline.py`

**Files:**
- Modify: `pipelines/pyframework_pipeline/compare/pipeline.py` (lines 15-19, and the `subprocess.run` call on `_COMPARE_SCRIPT`)

- [ ] **Step 1: Locate the compare invocation**

```bash
grep -nE "_COMPARE_SCRIPT|subprocess.run" pipelines/pyframework_pipeline/compare/pipeline.py
```
Confirm the `subprocess.run` that invokes `_COMPARE_SCRIPT` (defined lines 15-19).

- [ ] **Step 2: Replace with a package import call**

- Remove `_COMPARE_SCRIPT` (lines 15-19).
- Find the programmatic entry of the compare script:
  ```bash
  grep -nE "^def (main|run|compare|run_compare)" pipelines/pyframework_pipeline/analyze/run_compare_pipeline.py
  ```
- Replace the `subprocess.run([sys.executable, str(_COMPARE_SCRIPT), ...])` with a direct call to that entry (add a thin `run(...)` wrapper to `run_compare_pipeline.py` first if only a CLI `main()` exists).

- [ ] **Step 3: Verify import**

```bash
PYTHONPATH=pipelines python3 -c "from pyframework_pipeline.compare import pipeline; print('OK')"
```
Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add pipelines/pyframework_pipeline/compare/pipeline.py \
        pipelines/pyframework_pipeline/analyze/run_compare_pipeline.py
git commit -m "refactor(compare): call analyze run_compare_pipeline via import

compare/pipeline.py no longer resolves the vendor compare script; it
imports run_compare_pipeline from the analyze package. Behavior unchanged."
```

---

## Task 7: Re-point `orchestrator._run_perf_kits_on_remote` to the `analyze/` scripts

This is the **one place** that cannot use an import: it ships flat script files into a remote container and runs them there (the container cannot import this repo's package). It must be re-pointed from `vendor/.../scripts/perf_insights/` to `pipelines/pyframework_pipeline/analyze/`, but kept as a file-copying mechanism.

**Files:**
- Modify: `pipelines/pyframework_pipeline/orchestrator.py` (function `_run_perf_kits_on_remote`, lines ~2228-2343)

- [ ] **Step 1: Read the current function**

```bash
sed -n '2228,2343p' pipelines/pyframework_pipeline/orchestrator.py
```
Confirm: it computes `kits_local = repo_root / "vendor" / "python-performance-kits"`, `scripts_dir = kits_local / "scripts" / "perf_insights"`, then pushes 12 named files + the rules JSON from `scripts_dir` into the container.

- [ ] **Step 2: Re-point `scripts_dir` to the analyze package directory**

Replace the `kits_local` / `scripts_dir` resolution (lines ~2250-2251) with the package location:
```python
# Scripts now live in the analyze subpackage (moved out of vendor/).
# repo_root = parent of pipelines/
if project_path:
    repo_root = project_path.parent.parent.parent
else:
    repo_root = Path(__file__).resolve().parents[2]
scripts_dir = repo_root / "pipelines" / "pyframework_pipeline" / "analyze"
```

Update the file list (lines ~2267-2280) to drop `perf_analysis_common.py` only if absent (it IS present in analyze/) — keep the same 12 names. Add `cpython_category_rules.json` (already in the list). The pushed-file loop and the in-container invocation (`{container_kits}/run_single_platform_pipeline.py ...`) stay identical — the scripts still run as flat files inside the container.

- [ ] **Step 3: Remove the submodule-init fallback**

In the same function, the block:
```python
if not scripts_dir.exists():
    _init_submodules(repo_root)
if not scripts_dir.exists():
    logger.warning("python-performance-kits not found at %s, skipping remote pipeline", kits_local)
    return
```
becomes (no submodule to init anymore):
```python
if not scripts_dir.exists():
    logger.warning("analyze scripts not found at %s, skipping remote pipeline", scripts_dir)
    return
```
Drop the now-unused `kits_local` variable.

- [ ] **Step 4: Verify orchestrator imports and `_run_perf_kits_on_remote` is syntactically valid**

```bash
PYTHONPATH=pipelines python3 -c "from pyframework_pipeline import orchestrator; print('OK')"
PYTHONPATH=pipelines python3 -c "import ast, pathlib; ast.parse(pathlib.Path('pipelines/pyframework_pipeline/orchestrator.py').read_text()); print('parse OK')"
```
Expected: both print `OK` / `parse OK`.

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/orchestrator.py
git commit -m "refactor(orchestrator): point remote perf-kits run at analyze/ scripts

_run_perf_kits_on_remote now ships flat scripts from
pipelines/pyframework_pipeline/analyze/ into the container (the container
cannot import this repo's package, so it still runs them as flat files).
Submodule-init fallback removed; no vendor/ reference remains in this path."
```

---

## Task 8: Remove all remaining `vendor`/`python-performance-kits` references and the submodule

After Tasks 5-7, no code references `vendor/`. Delete the submodule and clean up doc references.

**Files:**
- Modify: `.gitmodules` (remove the `python-performance-kits` entry)
- Delete: `vendor/python-performance-kits` (via `git rm`, de-initializing the submodule)
- Modify: `pipelines/pyframework_pipeline/orchestrator.py` — make `_init_submodules` (line ~154) a no-op stub (keep the function for API stability but it does nothing; or remove if no callers remain — check with grep first).

- [ ] **Step 1: Confirm no code references vendor/python-performance-kits**

```bash
git grep -nE "vendor/python-performance-kits|python-performance-kits|_init_submodules" -- ':!docs' ':!docs/superpowers' ':!*.md'
```
Expected: the only matches are inside `orchestrator.py` (the `_init_submodules` definition + any call sites) and possibly the docstring of `_run_perf_kits_on_remote` (harmless, update in Step 4). **No** path references to `vendor/` should remain in `.py` files.

- [ ] **Step 2: Neutralize `_init_submodules`**

Check call sites:
```bash
git grep -nE "_init_submodules" -- ':!docs'
```
If the only remaining reference is the definition itself (Task 7 removed its call site), replace the function body with a no-op that returns immediately (keeps the symbol for safety):
```python
def _init_submodules(repo_root: Path) -> None:
    """No-op: vendor submodules were removed in Phase 1 of the refactor.

    Kept as a stub so any lingering call sites degrade gracefully rather
    than NameError.
    """
    return
```
If there are other call sites, either remove them or leave them calling the no-op (they will harmlessly do nothing).

- [ ] **Step 3: De-init and remove the submodule**

```bash
git submodule deinit -f vendor/python-performance-kits
git rm -f vendor/python-performance-kits
```
Then remove the `.gitmodules` entry (if `git rm` didn't already empty it):
```bash
# Inspect .gitmodules; if only the kits entry remains, delete the file, else edit.
cat .gitmodules
```
If `.gitmodules` is now empty or only had the kits stanza, `git rm .gitmodules`. If other submodules remain, edit `.gitmodules` to delete just the `[submodule "vendor/python-performance-kits"]` stanza.

Also clean the submodule metadata:
```bash
rm -rf .git/modules/vendor/python-performance-kits
```

- [ ] **Step 4: Update stale docstring in `_run_perf_kits_on_remote`**

If its docstring still says "Run python-performance-kits pipeline inside the container", update to "Run the analyze/perf pipeline inside the container". (Docstring only; behavior already fixed in Task 7.)

- [ ] **Step 5: Verify the repo still imports end-to-end**

```bash
PYTHONPATH=pipelines python3 -c "import pyframework_pipeline.orchestrator; print('OK')"
ls vendor/python-performance-kits 2>&1 | head -1   # expected: "No such file or directory"
test -f .gitmodules && echo "gitmodules present" || echo "gitmodules removed"
```
Expected: `OK`; the `ls` errors (gone); `gitmodules removed` (or `present` if other submodules legitimately remain — verify content then).

- [ ] **Step 6: Commit submodule removal**

```bash
git add -A
git commit -m "chore: remove python-performance-kits git submodule

Code now lives in pipelines/pyframework_pipeline/analyze/ as a first-class
subpackage (Phase 1 of the integration refactor). _init_submodules is a
no-op stub. Provenance recorded in the analyze/ integration commit."
```

---

## Task 9: Full G2 regression — run all 20 test modules

**Files:** none (verification only).

- [ ] **Step 1: Discover and run the full test suite**

From repo root, run unittest discovery over the whole `pipelines/tests/` directory (20 modules expected: the original 11 + the 9 migrated):
```bash
PYTHONPATH=pipelines python3 -m unittest discover -s pipelines/tests -p "test_*.py" -v 2>&1 | tail -40
```
Expected: the summary line reads `OK` and the test count reflects ~20 modules worth of tests with **zero failures, zero errors**.

- [ ] **Step 2: If any test fails, STOP and do not proceed to Phase 2**

Phase 1's contract is "all green, algorithm unchanged". A failure here means the copy or the import switch altered behavior. Use the `systematic-debugging` skill: reproduce the single failing test with `-v`, read the diff against the vendor original, fix, re-run. Do **not** disable or skip a test to get green.

- [ ] **Step 3: Confirm the original 11 specifically still pass (regression sanity)**

```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_acquisition \
  pipelines.tests.test_backfill \
  pipelines.tests.test_bridge \
  pipelines.tests.test_datajuicer_support \
  pipelines.tests.test_environment \
  pipelines.tests.test_four_layer_validator \
  pipelines.tests.test_orchestrator_streaming \
  pipelines.tests.test_pipeline_integration \
  pipelines.tests.test_udfbenchmarking_support \
  pipelines.tests.test_validate_cli \
  -v 2>&1 | tail -5
```
Expected: `OK`.

- [ ] **Step 4: Final commit (only if any uncommitted fixups remain; otherwise skip)**

If Step 2 produced fixup commits, ensure the working tree is clean:
```bash
git status --porcelain
```
Expected: empty.

---

## Phase 1 Definition of Done

- [ ] `pipelines/pyframework_pipeline/analyze/` exists with 16 scripts + `cpyphone_category_rules.json`, intra-package imports relative.
- [ ] 9 kits tests migrated to `pipelines/tests/` with rewritten import paths.
- [ ] `acquisition/perf_profile.py`, `acquisition/machine_code.py`, `compare/pipeline.py` use package imports, not subprocess.
- [ ] `orchestrator._run_perf_kits_on_remote` ships scripts from `analyze/` (the one legitimate file-copy path).
- [ ] `vendor/python-performance-kits` submodule deleted; `.gitmodules` updated; `_init_submodules` is a no-op.
- [ ] **G2 gate: all 20 test modules pass** (original 11 + migrated 9).
- [ ] Provenance commit hash recorded in the Task 2 commit message.
