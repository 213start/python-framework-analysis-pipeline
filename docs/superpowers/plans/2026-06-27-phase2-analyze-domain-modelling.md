# Phase 2: Analyze Layer Domain Modelling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the flat `analyze/` scripts (landed in Phase 1) into a domain-modelled, contract-driven pipeline: define the `contracts/` data classes (records / tables / instruction / timing) with mandatory file serialization, extract the four analysis subflows (parse / classify / aggregate / annotate) into focused classes whose inputs and outputs are *configurable file paths with defaults*, and converge the duplicated CPython category rules into one built-in resource. **Algorithms are unchanged** — equivalence is policed by the migrated kits tests.

**Architecture:** A new `pyframework_pipeline/contracts/` subpackage holds frozen dataclasses plus `to_file()`/`from_file()` serialization (serialization is mandatory, not optional — every step's I/O is on disk for auditability). The four analysis subflows (`analyze/parse.py`, `classify.py`, `aggregate.py`, `annotate.py`) each expose a function `f(input_path=..., output_path=..., ...) -> None` reading/writing contract files. The existing scripts are re-wired so their CLI `main()`/`run()` delegate to these functions (DRY). `CATEGORY_MAP` duplication between `acquisition/perf_profile.py` and `backfill/perf_backfill.py` is collapsed onto the single built-in CPython rules.

**Tech Stack:** Python ≥ 3.10, stdlib only. `dataclasses`, `csv`, `json`, `importlib.resources`. Tests via `python3 -m unittest`.

**Reference spec:** `docs/superpowers/specs/2026-06-27-repo-integration-and-oop-refactor-design.md` (§Detailed Design 1, 4; §Phasing → Phase 2).

**Prerequisite:** Phase 1 complete (`vendor/` removed; `analyze/` subpackage present; 20 test modules green).

**Acceptance gate:** the 20 Phase-1 test modules still pass, **plus** new contract/subflow unit tests added in this phase. Algorithm equivalence is proven by the migrated kits tests (`test_perf_*`, `test_perf_text_reports`, etc.) passing unchanged.

---

## File Structure

**New `pipelines/pyframework_pipeline/contracts/`** subpackage:

| File | Responsibility |
|---|---|
| `contracts/__init__.py` | re-export public contract types |
| `contracts/records.py` | `RawSample`, `PerfRecord` (frozen dataclasses) + CSV serialization |
| `contracts/tables.py` | `CategoryRow`, `SymbolRow`, `SharedObjectRow`, `AggregatedTables` + CSV serialization |
| `contracts/instruction.py` | `InstructionSample`, `DisassemblyBlock`, `InstructionDataset` + serialization |
| `contracts/timing.py` | `TimingEntry`, `TimingDataset` + JSON serialization |

**Refactored `analyze/` subflows:**

| File | Responsibility |
|---|---|
| `analyze/parse.py` | C1: `perf.data`/perf-script → `PerfRecord` CSV (was logic inside `perf_data_to_csv.py` + `perf_script_to_csv.py` + `normalize_perf_records.py`) |
| `analyze/classify.py` | C2: `PerfRecord` → classified `PerfRecord`; `CategoryClassifier` class wrapping `perf_analysis_common` rule logic; loads built-in rules |
| `analyze/aggregate.py` | C3: classified records → `AggregatedTables`; wraps `aggregate_rows` |
| `analyze/annotate.py` | C4: `[records + tables] + InstructionDataset` → annotated hotspots CSV; wraps `annotate_perf_hotspots` logic |
| `analyze/perf_analysis_common.py` | retained as the shared helper core (classify/aggregate/CSV primitives); rule loading moves into `classify.py` |
| `analyze/rules/` or `analyze/cpython_category_rules.json` | the single source of CPython rules (built-in resource) |
| other `render_*.py`, `compare_*.py`, `show_symbol_machine_code.py`, `run_*_pipeline.py` | retained; their `run()`/`main()` now delegate to the four subflow functions |

**Modified (dedup):**
- `acquisition/perf_profile.py` — remove its local `CATEGORY_MAP` (14→L1), import the converged rules from `analyze`.
- `backfill/perf_backfill.py` — remove its local `CATEGORY_MAP` (the 14→L1 + sub-category tables, lines ~19-217), import the converged mapping.

**New tests:**
- `pipelines/tests/test_contracts.py` — serialization round-trips for all four contract families.
- `pipelines/tests/test_analyze_subflows.py` — each of C1-C4 invoked as a path-in/path-out function on tiny fixtures.

---

## Critical constraint: contract field names MUST match the existing CSV schema

The kits pipeline today exchanges `dict[str, str]` rows whose schema is the `NORMALIZED_FIELDS` list in `analyze/perf_analysis_common.py` (Phase 1 copy). The new contract dataclasses are typed wrappers over **the same column names** — do not invent new field names, or the migrated kits tests (which read/write these CSVs) will break. The mapping is fixed below and MUST be followed in every task:

| CSV column (`NORMALIZED_FIELDS`) | Contract field | Type (CSV stores strings; contract stores typed) |
|---|---|---|
| `ip` | `ip` | `int` (hex string in CSV) |
| `pid` | `pid` | `int` |
| `command` | `command` | `str` |
| `shared_object` | `shared_object` | `str` |
| `symbol` | `symbol` | `str \| None` (empty → None) |
| `period` | `period` | `int` |
| `children` | `children_share` | `float` (percent) |
| `self` | `period_share` | `float` (percent) |
| `category_top` | `category_top` | `str` |
| `category_sub` | `category_sub` | `str` |
| `category_reason` | `category_reason` | `str` |
| `benchmark`, `platform_id`, `arch`, `python_version`, `build_id`, `event`, `source_report`, `sample_count`, `pid_command`, `instruction_text`, `instruction_offset`, `instruction_share` | same name | passthrough (str) |

> The spec's `RawSample`/`PerfRecord` field list (§Detailed Design 1) is the *design intent*; this table is the *ground truth* that keeps the existing CSVs valid. Where they differ (e.g. spec lists `tid`, `timestamp`, `callchain` which the current CSVs do not carry), **defer** those fields to Phase 3 or omit — do not add columns the current pipeline does not produce. Note this deferral in Task 9.

---

## Task 1: contracts/records.py — RawSample / PerfRecord + CSV serialization

**Files:**
- Create: `pipelines/pyframework_pipeline/contracts/__init__.py`
- Create: `pipelines/pyframework_pipeline/contracts/records.py`
- Test: `pipelines/tests/test_contracts.py` (created here, extended in later tasks)

- [ ] **Step 1: Write the failing test for PerfRecord CSV round-trip**

Create `pipelines/tests/test_contracts.py`:
```python
from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.contracts.records import PerfRecord, RawSample


class TestPerfRecordCsv(unittest.TestCase):
    def test_roundtrip_writes_and_reads_same_fields(self):
        rec = PerfRecord(
            ip=0x400000,
            pid=1234,
            command="python3",
            shared_object="/lib/libpython3.14.so",
            symbol="PyEval_EvalFrameDefault",
            period=1000000,
            children_share=42.5,
            period_share=12.5,
            category_top="Interpreter",
            category_sub="ceval",
            category_reason="symbol_regex:PyEval",
        )
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "recs.csv"
            PerfRecord.write_csv(p, [rec])
            got = PerfRecord.read_csv(p)
        self.assertEqual(got, [rec])

    def test_empty_symbol_becomes_none_and_back(self):
        rec = RawSample(
            ip=0x100, pid=1, command="c", shared_object="so",
            symbol=None, period=0, children_share=0.0, period_share=0.0,
        )
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.csv"
            RawSample.write_csv(p, [rec])
            got = RawSample.read_csv(p)
        self.assertEqual(got, [rec])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run it to verify it fails**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts -v
```
Expected: FAIL (ModuleNotFoundError: pyframework_pipeline.contracts.records).

- [ ] **Step 3: Implement contracts/records.py**

Create `pipelines/pyframework_pipeline/contracts/__init__.py` (empty docstring only for now).
Create `pipelines/pyframework_pipeline/contracts/records.py`:
```python
"""Record-level contracts: RawSample (C1 output) and PerfRecord (C2 output).

Wraps the perf_records.csv / classified_records.csv rows with typed fields.
CSV column names match the kits NORMALIZED_FIELDS schema exactly so existing
readers and the migrated kits tests keep working.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, asdict, fields as dc_fields
from pathlib import Path
from typing import ClassVar


def _fmt_int(value: int) -> str:
    return str(value)


def _parse_int(value: str) -> int:
    raw = (value or "").strip()
    if not raw:
        return 0
    return int(float(raw.replace(",", "")))


def _fmt_float(value: float) -> str:
    # kits stores percent as e.g. "12.5" (parse_percent/format_float).
    return format(value, ".6f").rstrip("0").rstrip(".") if value != 0 else "0"


def _parse_float(value: str) -> float:
    raw = (value or "").strip().rstrip("%")
    return float(raw) if raw else 0.0


@dataclass(frozen=True)
class RawSample:
    ip: int
    pid: int
    command: str
    shared_object: str
    symbol: str | None
    period: int
    children_share: float
    period_share: float

    CSV_FIELDS: ClassVar[tuple[str, ...]] = (
        "ip", "pid", "command", "shared_object", "symbol",
        "period", "children", "self",
    )

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "RawSample":
        return cls(
            ip=_parse_int(row.get("ip", "")),
            pid=_parse_int(row.get("pid", "")),
            command=row.get("command", ""),
            shared_object=row.get("shared_object", ""),
            symbol=(row.get("symbol", "").strip() or None),
            period=_parse_int(row.get("period", "")),
            children_share=_parse_float(row.get("children", "")),
            period_share=_parse_float(row.get("self", "")),
        )

    def to_row(self) -> dict[str, str]:
        return {
            "ip": format(self.ip, "x"),
            "pid": _fmt_int(self.pid),
            "command": self.command,
            "shared_object": self.shared_object,
            "symbol": self.symbol or "",
            "period": _fmt_int(self.period),
            "children": _fmt_float(self.children_share),
            "self": _fmt_float(self.period_share),
        }


@dataclass(frozen=True)
class PerfRecord(RawSample):
    category_top: str = ""
    category_sub: str = ""
    category_reason: str = ""

    CSV_FIELDS: ClassVar[tuple[str, ...]] = RawSample.CSV_FIELDS + (
        "category_top", "category_sub", "category_reason",
    )

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "PerfRecord":
        base = RawSample.from_row(row)
        return cls(
            **{f.name: getattr(base, f.name) for f in dc_fields(RawSample)},
            category_top=row.get("category_top", ""),
            category_sub=row.get("category_sub", ""),
            category_reason=row.get("category_reason", ""),
        )

    def to_row(self) -> dict[str, str]:
        row = super().to_row()
        row.update(
            category_top=self.category_top,
            category_sub=self.category_sub,
            category_reason=self.category_reason,
        )
        return row


def _read_csv(path: Path, ctor):
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [ctor(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, records, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(r.to_row() for r in records)


# Attach convenience classmethods used by the tests.
@classmethod
def _perf_write(cls, path: Path, records: list["PerfRecord"]) -> None:
    _write_csv(path, records, cls.CSV_FIELDS)


@classmethod
def _perf_read(cls, path: Path) -> list["PerfRecord"]:
    return _read_csv(path, cls.from_row)  # type: ignore[arg-type]


@classmethod
def _raw_write(cls, path: Path, records: list["RawSample"]) -> None:
    _write_csv(path, records, cls.CSV_FIELDS)


@classmethod
def _raw_read(cls, path: Path) -> list["RawSample"]:
    return _read_csv(path, cls.from_row)  # type: ignore[arg-type]


PerfRecord.write_csv = _perf_write      # type: ignore[attr-defined]
PerfRecord.read_csv = _perf_read        # type: ignore[attr-defined]
RawSample.write_csv = _raw_write        # type: ignore[attr-defined]
RawSample.read_csv = _raw_read          # type: ignore[attr-defined]
```

> The `write_csv`/`read_csv` are attached as classmethods at module load because `@dataclass` generates the class before we can cleanly add methods that reference the not-yet-fully-defined class. An equivalent, equally valid approach is to define free functions `write_records_csv(path, records)` / `read_records_csv(path) -> list[PerfRecord]` and call those from the tests — choose whichever reads cleanest, but keep the test's call shape (`PerfRecord.write_csv(p, [rec])`).

- [ ] **Step 4: Run the test to verify it passes**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts -v
```
Expected: PASS (both test methods).

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/contracts/__init__.py \
        pipelines/pyframework_pipeline/contracts/records.py \
        pipelines/tests/test_contracts.py
git commit -m "feat(contracts): add RawSample/PerfRecord records contract

Typed frozen dataclasses wrapping the perf_records.csv /
classified_records.csv schema (columns match kits NORMALIZED_FIELDS).
Mandatory CSV serialization via write_csv/read_csv classmethods."
```

---

## Task 2: contracts/tables.py — AggregatedTables + rows

**Files:**
- Create: `pipelines/pyframework_pipeline/contracts/tables.py`
- Modify: `pipelines/tests/test_contracts.py` (add TestAggregatedTablesCsv)

- [ ] **Step 1: Write the failing test**

Append to `pipelines/tests/test_contracts.py` (before the `if __name__` block):
```python
from pyframework_pipeline.contracts.tables import (
    AggregatedTables, CategoryRow, SymbolRow, SharedObjectRow,
)


class TestAggregatedTablesCsv(unittest.TestCase):
    def test_roundtrip(self):
        tables = AggregatedTables(
            by_category=(
                CategoryRow(category_top="Interpreter", category_sub="ceval",
                            sample_count=10, period_total=1000,
                            period_share=50.0, children_share=80.0),
            ),
            by_symbol=(
                SymbolRow(symbol="PyEval_EvalFrameDefault",
                          shared_object="/lib/libpython3.14.so",
                          category_top="Interpreter",
                          sample_count=10, period_total=1000, period_share=50.0),
            ),
            by_shared_object=(
                SharedObjectRow(shared_object="/lib/libpython3.14.so",
                                sample_count=10, period_total=1000, period_share=50.0),
            ),
            total_period=2000,
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            tables.write_dir(base)
            got = AggregatedTables.read_dir(base)
        self.assertEqual(got, tables)
```

- [ ] **Step 3: Run it to verify it fails**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts.TestAggregatedTablesCsv -v
```
Expected: FAIL (ImportError).

- [ ] **Step 4: Implement contracts/tables.py**

The kits aggregate output is three CSVs: `category_summary.csv`, `symbol_hotspots.csv`, `shared_object_summary.csv`. Map their actual columns (inspect `summarize_platform_perf.py` first — do NOT guess):
```bash
grep -nE "fieldnames|write_csv_rows\(|category_summary|symbol_hotspots|shared_object_summary" pipelines/pyframework_pipeline/analyze/summarize_platform_perf.py | head -30
```
Then write `contracts/tables.py` with frozen dataclasses whose CSV column names match exactly what `summarize_platform_perf.py` writes. Shape:
```python
"""Table-level contracts: aggregated category/symbol/shared-object summaries."""
from __future__ import annotations
import csv
from dataclasses import dataclass
from pathlib import Path

# ... _fmt_int / _parse_int / _fmt_float / _parse_float (shared with records;
#     either duplicate the 4 tiny helpers here or, preferably, move them to a
#     private contracts/_serde.py and import from both records.py and tables.py)

@dataclass(frozen=True)
class CategoryRow:
    category_top: str
    category_sub: str
    sample_count: int
    period_total: int
    period_share: float
    children_share: float
    # add passthrough columns the actual CSV carries (platform_id, benchmark, etc.)

@dataclass(frozen=True)
class SymbolRow:
    symbol: str
    shared_object: str
    category_top: str
    sample_count: int
    period_total: int
    period_share: float

@dataclass(frozen=True)
class SharedObjectRow:
    shared_object: str
    sample_count: int
    period_total: int
    period_share: float

@dataclass(frozen=True)
class AggregatedTables:
    by_category: tuple[CategoryRow, ...]
    by_symbol: tuple[SymbolRow, ...]
    by_shared_object: tuple[SharedObjectRow, ...]
    total_period: int

    def write_dir(self, base: Path) -> None: ...   # writes the 3 CSVs
    @classmethod
    def read_dir(cls, base: Path) -> "AggregatedTables": ...  # reads the 3 CSVs
```
Implement `write_dir`/`read_dir` using the column lists you confirmed from `summarize_platform_perf.py`. (Move the 4 serde helpers to `contracts/_serde.py` in this step and re-import in `records.py` to avoid duplication — DRY.)

- [ ] **Step 5: Run the test to verify it passes**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts.TestAggregatedTablesCsv -v
```
Expected: PASS.

- [ ] **Step 6: Re-run the records test to confirm the _serde.py refactor didn't break it**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts -v
```
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add pipelines/pyframework_pipeline/contracts/
git commit -m "feat(contracts): add AggregatedTables contract (category/symbol/so)

Frozen dataclasses + dir-based CSV serialization matching the three
summary CSVs written by summarize_platform_perf. Shared serde helpers
moved to contracts/_serde.py."
```

---

## Task 3: contracts/instruction.py + contracts/timing.py

**Files:**
- Create: `pipelines/pyframework_pipeline/contracts/instruction.py`
- Create: `pipelines/pyframework_pipeline/contracts/timing.py`
- Modify: `pipelines/tests/test_contracts.py` (add InstructionDataset + TimingDataset round-trip tests)

- [ ] **Step 1: Inspect the actual instruction CSV + timing JSON schemas**

```bash
grep -nE "fieldnames|instruction_hotspots|write_csv_rows" pipelines/pyframework_pipeline/analyze/annotate_perf_hotspots.py | head
grep -rnE "timing-normalized|wallClockTime|wall_clock_ns" pipelines/pyframework_pipeline/compare/pipeline.py pipelines/pyframework_pipeline/orchestrator.py | head
```
Capture the exact column names for `instruction_hotspots.csv` and the JSON shape of `timing-normalized.json` (it has `cases[].metrics.wallClockTime.wall_clock_ns` per compare/pipeline.py `_geomean_e2e_time`).

- [ ] **Step 2: Write failing tests for both**

Append to `pipelines/tests/test_contracts.py`:
```python
from pyframework_pipeline.contracts.instruction import (
    InstructionDataset, InstructionSample, DisassemblyBlock,
)
from pyframework_pipeline.contracts.timing import TimingDataset, TimingEntry


class TestInstructionDataset(unittest.TestCase):
    def test_roundtrip(self):
        ds = InstructionDataset(
            samples=(InstructionSample(ip=0x400, instruction_text="mov rax,1",
                                       instruction_offset=0, period=100, period_share=5.0),),
            disassembly=(DisassemblyBlock(shared_object="libpython", symbol="PyEval",
                                          start_ip=0x400,
                                          instructions=((0, "mov rax,1"), (1, "ret"))),),
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            ds.write_dir(base)
            got = InstructionDataset.read_dir(base)
        self.assertEqual(got, ds)


class TestTimingDataset(unittest.TestCase):
    def test_roundtrip(self):
        ds = TimingDataset(
            entries=(TimingEntry(label="q1", value_ns=1_000_000_000, unit_hint="ns", query_id="q1"),),
            platform_id="x86", benchmark="tpch",
        )
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "timing.json"
            ds.write_json(p)
            got = TimingDataset.read_json(p)
        self.assertEqual(got, ds)
```

- [ ] **Step 3: Run, verify FAIL (ImportError)**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts.TestInstructionDataset pipelines.tests.test_contracts.TestTimingDataset -v
```

- [ ] **Step 4: Implement contracts/instruction.py and contracts/timing.py**

`instruction.py`: `InstructionSample`/`DisassemblyBlock`/`InstructionDataset` with `write_dir`/`read_dir` using the column names from Step 1. `DisassemblyBlock` may serialize to a small JSON sidecar (it carries a tuple of (offset, asm) pairs that don't fit a flat CSV cleanly) — store samples in `instruction_hotspots.csv` and disassembly blocks in `disasm_blocks.json` side-by-side in the dir.

`timing.py`: `TimingEntry`/`TimingDataset` with `write_json`/`read_json`. **Match the existing `timing-normalized.json` shape** (`cases[].metrics.wallClockTime.wall_clock_ns`) so current readers keep working — the `TimingDataset` is a typed view over that structure; if the existing JSON nests cases/metrics, model it faithfully (a `TimingEntry` may map to one case).

- [ ] **Step 5: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_contracts -v
```
Expected: all four test classes PASS.

- [ ] **Step 6: Commit**

```bash
git add pipelines/pyframework_pipeline/contracts/instruction.py \
        pipelines/pyframework_pipeline/contracts/timing.py \
        pipelines/tests/test_contracts.py
git commit -m "feat(contracts): add instruction + timing contracts

InstructionDataset (samples CSV + disasm JSON sidecar) and TimingDataset
(JSON, matching existing timing-normalized.json shape). All four contract
families now have mandatory file serialization."
```

---

## Task 4: analyze/parse.py — C1 subflow (perf.data → PerfRecord CSV)

C1 today lives across `perf_data_to_csv.py`, `perf_script_to_csv.py`, `normalize_perf_records.py`. This task wraps that logic behind one path-in/path-out function.

**Files:**
- Create: `pipelines/pyframework_pipeline/analyze/parse.py`
- Test: `pipelines/tests/test_analyze_subflows.py` (created here)

- [ ] **Step 1: Find the existing entry that turns perf.data into perf_records.csv**

```bash
grep -nE "^def (main|run|convert|perf_data_to_csv|normalize)" pipelines/pyframework_pipeline/analyze/perf_data_to_csv.py pipelines/pyframework_pipeline/analyze/normalize_perf_records.py
```
Identify the function that, given a `perf.data` path and a platform id, writes `perf_records.csv`. (From Phase 1, `run_single_platform_pipeline.run()` orchestrates this sequence.)

- [ ] **Step 2: Write the failing test**

Create `pipelines/tests/test_analyze_subflows.py`:
```python
from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.parse import parse


class TestParse(unittest.TestCase):
    def test_parse_accepts_configurable_paths(self):
        # We cannot run real perf here; assert the function signature is
        # path-based and raises a clear error when perf.data is absent,
        # rather than resolving a vendor path.
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            with self.assertRaises(FileNotFoundError):
                parse(
                    perf_data=base / "missing.perf.data",
                    output=base / "out.csv",
                    platform_id="x86",
                    arch="x86_64",
                    python_version="3.14.0",
                    build_id="b1",
                    benchmark="tpch",
                    event="cycles",
                )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run, verify FAIL**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_analyze_subflows -v
```

- [ ] **Step 4: Implement analyze/parse.py**

Write `parse(perf_data: Path, output: Path, *, platform_id, arch, python_version, build_id, benchmark, event, ...) -> None` that:
1. Asserts `perf_data` exists (raise `FileNotFoundError` if not).
2. Calls the existing `perf_data_to_csv` / `normalize_perf_records` functions (imported from the sibling modules) to produce records.
3. Writes `output` as a `PerfRecord` CSV (uncategorized — `category_*` empty) via `PerfRecord.write_csv`.

Use defaults for optional params where sensible, but `perf_data`/`output` are required. Re-use the existing parsing logic — do **not** re-implement perf parsing.

- [ ] **Step 5: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_analyze_subflows -v
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pipelines/pyframework_pipeline/analyze/parse.py pipelines/tests/test_analyze_subflows.py
git commit -m "feat(analyze): add C1 parse subflow (perf.data -> PerfRecord CSV)

path-in/path-out function with configurable paths; wraps existing
perf_data_to_csv + normalize logic."
```

---

## Task 5: analyze/classify.py — C2 subflow + CategoryClassifier + converged rules

**Files:**
- Create: `pipelines/pyframework_pipeline/analyze/classify.py`
- Modify: `pipelines/pyframework_pipeline/analyze/perf_analysis_common.py` — keep helpers, but `load_rules`/`classify_record` now imported by classify.py
- Test: `pipelines/tests/test_analyze_subflows.py` (add TestClassify)

- [ ] **Step 1: Write the failing test (classifier on tiny fixture)**

Append to `pipelines/tests/test_analyze_subflows.py`:
```python
from pyframework_pipeline.analyze.classify import classify, CategoryClassifier
from pyframework_pipeline.contracts.records import PerfRecord


class TestClassify(unittest.TestCase):
    def test_classify_path_roundtrip(self):
        # Build a tiny uncategorized PerfRecord CSV, classify it, read back.
        rec = PerfRecord(
            ip=0x400000, pid=1, command="python3",
            shared_object="/lib/libpython3.14.so",
            symbol="PyEval_EvalFrameDefault", period=100,
            children_share=0.0, period_share=10.0,
            category_top="", category_sub="", category_reason="",
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            inp = base / "in.csv"; PerfRecord.write_csv(inp, [rec])
            out = base / "out.csv"
            classify(input_path=inp, output_path=out)
            got = PerfRecord.read_csv(out)
        self.assertEqual(len(got), 1)
        # PyEval_EvalFrameDefault is a CPython interpreter symbol -> classified.
        self.assertNotEqual(got[0].category_top, "Unknown")

    def test_classifier_default_rules_load(self):
        clf = CategoryClassifier()  # loads built-in cpython rules
        self.assertTrue(len(clf.rules) > 0)
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement analyze/classify.py**

```python
"""C2 classification: PerfRecord -> classified PerfRecord (CPython 14 cat)."""
from __future__ import annotations
from importlib import resources
from pathlib import Path
from .perf_analysis_common import Rule, load_rules, classify_record
from ..contracts.records import PerfRecord

_BUILTIN_RULES_RESOURCE = "cpython_category_rules.json"  # shipped in analyze/


class CategoryClassifier:
    """Classifies a PerfRecord's symbol/shared_object into CPython categories.

    Rules are CPython domain knowledge (fixed, framework-agnostic). The
    default rules ship with the package; a custom rules file may be supplied.
    """
    def __init__(self, rules_path: Path | None = None) -> None:
        if rules_path is None:
            rules_path = Path(__file__).resolve().parent / _BUILTIN_RULES_RESOURCE
        self.rules: list[Rule] = load_rules(rules_path)

    def classify_record(self, rec: PerfRecord) -> tuple[str, str, str]:
        return classify_record(
            {"symbol": rec.symbol or "", "shared_object": rec.shared_object},
            self.rules,
        )


def classify(
    *,
    input_path: Path = Path("perf_records.csv"),
    output_path: Path = Path("classified_records.csv"),
    rules_path: Path | None = None,
) -> None:
    clf = CategoryClassifier(rules_path=rules_path)
    recs = PerfRecord.read_csv(input_path)
    classified = [
        rec.__class__(**{**{f.name: getattr(rec, f.name) for f in rec.__dataclass_fields__},
                         **dict(zip(("category_top", "category_sub", "category_reason"),
                                    clf.classify_record(rec)))})
        for rec in recs
    ]
    PerfRecord.write_csv(output_path, classified)
```
(If `classify_record` needs the full row dict including `category_top` per `perf_analysis_common._match_group`, pass a dict built from `rec.to_row()`.)

- [ ] **Step 4: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_analyze_subflows -v
```

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/analyze/classify.py pipelines/tests/test_analyze_subflows.py
git commit -m "feat(analyze): add C2 classify subflow + CategoryClassifier

Loads built-in CPython rules; path-in/path-out classification producing
classified PerfRecord CSV. Rules are framework-agnostic CPython domain
knowledge."
```

---

## Task 6: analyze/aggregate.py (C3) and analyze/annotate.py (C4)

**Files:**
- Create: `pipelines/pyframework_pipeline/analyze/aggregate.py`
- Create: `pipelines/pyframework_pipeline/annotate.py` (note: `annotate_perf_hotspots.py` already exists; the new C4 subflow is `analyze/annotate.py` — keep the old script, delegate to it)

> Naming collision: `analyze/annotate_perf_hotspots.py` (the kits script) vs the new `analyze/annotate.py` (the C4 subflow). These differ in suffix, so both can coexist. The C4 subflow `annotate.py` delegates to the script's logic.

- [ ] **Step 1: Write failing tests for aggregate + annotate signatures**

Append to `test_analyze_subflows.py`:
```python
from pyframework_pipeline.analyze.aggregate import aggregate
from pyframework_pipeline.analyze.annotate import annotate


class TestAggregateSignature(unittest.TestCase):
    def test_aggregate_paths_configurable(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            # empty classified CSV -> aggregate writes empty tables dir, no crash
            PerfRecord.write_csv(base / "in.csv", [])
            aggregate(input_path=base / "in.csv", output_dir=base / "tables")
            self.assertTrue((base / "tables").is_dir())


class TestAnnotateSignature(unittest.TestCase):
    def test_annotate_paths_configurable(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            # missing instruction dir -> annotate skips gracefully (returns), no crash
            annotate(
                records_path=base / "recs.csv",
                tables_dir=base / "tables",
                instruction_dir=base / "nope",
                output=base / "annotated.csv",
            )
            # No assertion on contents; this is a signature/graceful-skip test.
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement aggregate.py and annotate.py**

`aggregate.py`: `aggregate(*, input_path=Path("classified_records.csv"), output_dir=Path("tables")) -> None` — read classified records, call the existing `aggregate_rows` (from `perf_analysis_common`) for the three groupings (category / symbol / shared_object), write the three summary CSVs into `output_dir` via the `AggregatedTables` contract's column layout (or directly with `write_csv_rows` — but prefer routing through the contract so the schema is single-sourced).

`annotate.py`: `annotate(*, records_path, tables_dir, instruction_dir, output) -> None` — read the classified records + tables + `InstructionDataset.read_dir(instruction_dir)` (if present; skip if absent), call the existing `annotate_perf_hotspots` logic to merge instruction samples onto hotspots, write `output`. If `instruction_dir` is missing/empty, write an empty/passthrough `output` and return (graceful skip).

- [ ] **Step 4: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_analyze_subflows -v
```

- [ ] **Step 5: Re-run the migrated kits tests — the algorithm equivalence gate**

```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_perf_analysis_pipeline \
  pipelines.tests.test_perf_annotation_pipeline \
  pipelines.tests.test_perf_text_reports \
  pipelines.tests.test_perf_runner \
  -v 2>&1 | tail -10
```
Expected: all OK. If any fail, the new wrappers diverged from the original logic — fix before committing.

- [ ] **Step 6: Commit**

```bash
git add pipelines/pyframework_pipeline/analyze/aggregate.py \
        pipelines/pyframework_pipeline/analyze/annotate.py \
        pipelines/tests/test_analyze_subflows.py
git commit -m "feat(analyze): add C3 aggregate + C4 annotate subflows

path-in/path-out functions; C4 consumes both analysis-internal records/
tables and the (external-or-B5) InstructionDataset per the contract.
Algorithm-equivalence verified against migrated kits tests."
```

---

## Task 7: Converge the duplicated CPython category mapping

`acquisition/perf_profile.py` has a `CATEGORY_MAP` (14→L1) and `backfill/perf_backfill.py` has a larger duplicated mapping (lines ~19-217, 14→L1 + sub-categories). Per the spec, these are **not** framework-specific — they're CPython domain knowledge and must be single-sourced.

**Files:**
- Create: `pipelines/pyframework_pipeline/analyze/category_mapping.py` (the converged L1+L2 mapping)
- Modify: `pipelines/pyframework_pipeline/acquisition/perf_profile.py` (delete its `CATEGORY_MAP`, import)
- Modify: `pipelines/pyframework_pipeline/backfill/perf_backfill.py` (delete its mapping block, import)

- [ ] **Step 1: Compare the two mappings to confirm they're the same domain knowledge**

```bash
sed -n '19,40p' pipelines/pyframework_pipeline/acquisition/perf_profile.py
sed -n '19,220p' pipelines/pyframework_pipeline/backfill/perf_backfill.py
```
If the two mappings disagree on any category, **stop and flag to the user** — that's a genuine inconsistency to resolve, not silently pick one. If they agree (possibly one is a superset), proceed.

- [ ] **Step 2: Create the converged mapping module**

`analyze/category_mapping.py`:
```python
"""CPython category mapping: 14 category_top -> framework L1 + L2 sub.

This is CPython domain knowledge (fixed, framework-agnostic), single-sourced
here. Formerly duplicated in acquisition/perf_profile.py and
backfill/perf_backfill.py.
"""
from __future__ import annotations

CATEGORY_TO_L1: dict[str, str] = {
    "Interpreter": "Interpreter",
    "Memory": "Memory",
    # ... the full agreed mapping from Step 1 ...
}

# L2 sub-category derivation, if the backfill version carried logic for it.
def l2_sub(category_top: str, category_sub: str) -> str:
    ...
```
Use the union of both mappings (they should be identical; if one had extra entries, keep the superset and note it).

- [ ] **Step 3: Replace both call sites with imports**

- `acquisition/perf_profile.py`: delete the local `CATEGORY_MAP` constant; `from ..analyze.category_mapping import CATEGORY_TO_L1 as CATEGORY_MAP` (keep the name at call sites to minimize churn, or rename — but keep behavior identical).
- `backfill/perf_backfill.py`: delete the local mapping block (lines ~19-217 per Step 1); import the converged mapping + `l2_sub`.

- [ ] **Step 4: Run the backfill + acquisition tests (they exercise these mappings)**

```bash
PYTHONPATH=pipelines python3 -m unittest \
  pipelines.tests.test_backfill \
  pipelines.tests.test_acquisition \
  pipelines.tests.test_pipeline_integration \
  -v 2>&1 | tail -10
```
Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/analyze/category_mapping.py \
        pipelines/pyframework_pipeline/acquisition/perf_profile.py \
        pipelines/pyframework_pipeline/backfill/perf_backfill.py
git commit -m "refactor: converge duplicated CPython category mapping

The 14->L1 (+L2) mapping was duplicated in acquisition/perf_profile.py
and backfill/perf_backfill.py. Single-sourced into analyze/category_mapping
as CPython domain knowledge (fixed, framework-agnostic). Behavior unchanged."
```

---

## Task 8: Single thin CLI entry point (single/compare subcommands)

Per spec decision I2-variant: one entry script, `single`/`compare` subcommands, each delegating to package functions.

**Files:**
- Modify: `pipelines/pyframework_pipeline/analyze/run_single_platform_pipeline.py`
- Modify: `pipelines/pyframework_pipeline/analyze/run_compare_pipeline.py`
- Create: `pipelines/pyframework_pipeline/analyze/cli.py` (the single entry, or fold into the package `__main__`)

> Keep `run_single_platform_pipeline.py` / `run_compare_pipeline.py` as importable modules (their `run()` is used by acquisition/compare). The single CLI dispatcher lives in `analyze/cli.py` and is invoked as `python3 -m pyframework_pipeline.analyze.cli {single|compare} ...`. This preserves the "scripts can run standalone" kits convention while having one entry.

- [ ] **Step 1: Write a failing smoke test for the CLI dispatcher**

Append to `test_analyze_subflows.py`:
```python
import subprocess, sys


class TestAnalyzeCli(unittest.TestCase):
    def test_single_subcommand_help(self):
        r = subprocess.run(
            [sys.executable, "-m", "pyframework_pipeline.analyze.cli", "single", "--help"],
            capture_output=True, text=True, env={"PYTHONPATH": "pipelines", "PATH": "/usr/bin:/bin"},
            check=False,
        )
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_compare_subcommand_help(self):
        r = subprocess.run(
            [sys.executable, "-m", "pyframework_pipeline.analyze.cli", "compare", "--help"],
            capture_output=True, text=True, env={"PYTHONPATH": "pipelines", "PATH": "/usr/bin:/bin"},
            check=False,
        )
        self.assertEqual(r.returncode, 0, r.stderr)
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement analyze/cli.py**

Use `argparse` subparsers. The `single` subcommand calls `parse` → `classify` → `aggregate` → (optional) `annotate` (chained via the path contracts). The `compare` subcommand calls `run_compare_pipeline.run(...)`. Both accept the configurable paths with defaults.

```python
"""Single CLI entry for the analyze pipeline: `single` and `compare` subcommands.

Usage:
  python3 -m pyframework_pipeline.analyze.cli single <perf.data> -o <dir> ...
  python3 -m pyframework_pipeline.analyze.cli compare <baseline_dir> <target_dir> ...
"""
from __future__ import annotations
import argparse
from .parse import parse
from .classify import classify
from .aggregate import aggregate
from .annotate import annotate
from .run_compare_pipeline import run as run_compare


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pyframework_pipeline.analyze.cli")
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("single", help="analyze one platform's perf.data")
    s.add_argument("perf_data")
    s.add_argument("-o", "--output", default=".")
    s.add_argument("--benchmark", default="tpch")
    s.add_argument("--platform", default="")
    s.add_argument("--top-n", type=int, default=50)
    # ...arch/python_version/build_id/event with defaults...
    c = sub.add_parser("compare", help="compare two platforms")
    c.add_argument("baseline_dir")
    c.add_argument("target_dir")
    c.add_argument("-o", "--output", default=".")
    # ...baseline_platform/target_platform/e2e times with defaults...
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "single":
        # chain the four subflows via configurable paths under args.output
        ...
        return 0
    if args.command == "compare":
        run_compare(...)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run, verify PASS**

```bash
PYTHONPATH=pipelines python3 -m unittest pipelines.tests.test_analyze_subflows.TestAnalyzeCli -v
```

- [ ] **Step 5: Commit**

```bash
git add pipelines/pyframework_pipeline/analyze/cli.py pipelines/tests/test_analyze_subflows.py
git commit -m "feat(analyze): single CLI entry with single/compare subcommands

Replaces the two-script entry convention with one dispatcher
(python3 -m pyframework_pipeline.analyze.cli) delegating to the C1-C4
subflow functions and run_compare_pipeline. Scripts remain importable."
```

---

## Task 9: Full G2 regression + document contract spec-field deferrals

- [ ] **Step 1: Run the entire suite (20 migrated + new contract/subflow tests)**

```bash
PYTHONPATH=pipelines python3 -m unittest discover -s pipelines/tests -p "test_*.py" -v 2>&1 | tail -20
```
Expected: `OK`, zero failures/errors.

- [ ] **Step 2: Record contract field deferrals in the spec**

Append a short note to `docs/superpowers/specs/2026-06-27-repo-integration-and-oop-refactor-design.md` §Detailed Design 1 (or a new "Implementation Notes" subsection) documenting that the spec-listed `RawSample` fields `tid`, `timestamp`, `callchain` were **deferred** in Phase 2 because the current kits CSV schema (`NORMALIZED_FIELDS`) does not carry them; they will be added if/when the upstream perf parsing produces them. This keeps the spec honest about what shipped.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-27-repo-integration-and-oop-refactor-design.md
git commit -m "docs: note Phase-2 contract field deferrals (tid/timestamp/callchain)"
```

---

## Phase 2 Definition of Done

- [ ] `contracts/` subpackage with `records.py`, `tables.py`, `instruction.py`, `timing.py`; every contract has mandatory file serialization with a passing round-trip test.
- [ ] `analyze/parse.py` (C1), `classify.py` (C2 + `CategoryClassifier`), `aggregate.py` (C3), `annotate.py` (C4) — each a path-in/path-out function.
- [ ] Duplicated `CATEGORY_MAP` converged into `analyze/category_mapping.py`.
- [ ] Single CLI entry (`analyze/cli.py`) with `single`/`compare` subcommands.
- [ ] Contract column names match the actual kits CSV schema (no invented columns).
- [ ] **G2 gate: all Phase-1 tests still pass + new contract/subflow tests pass; migrated kits tests prove algorithm equivalence.**
- [ ] Spec updated with the `tid`/`timestamp`/`callchain` deferral note.
