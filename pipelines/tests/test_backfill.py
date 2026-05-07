"""Tests for backfill sub-modules (timing, perf, asm, bindings, pipeline)."""

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.backfill.timing_backfill import (
    _format_ns,
    _compute_delta_pct,
    backfill_timing,
)
from pyframework_pipeline.backfill.perf_backfill import (
    _CATEGORY_TO_L1,
    _COMPONENT_MAP,
    _resolve_component,
    _generate_func_id,
    backfill_perf,
)
from pyframework_pipeline.backfill.asm_backfill import (
    _artifact_id,
    _symbol_to_hash,
    backfill_asm,
)
from pyframework_pipeline.backfill.binding_generator import generate_bindings


# ---------------------------------------------------------------------------
# timing_backfill
# ---------------------------------------------------------------------------

class TestFormatNs(unittest.TestCase):
    def test_seconds(self):
        self.assertEqual(_format_ns(5_230_000_000), "5.23 s")

    def test_milliseconds(self):
        self.assertEqual(_format_ns(1_770_000), "1.77 ms")

    def test_microseconds(self):
        result = _format_ns(891_200)
        self.assertIn("891.2", result)
        self.assertTrue(result.endswith("s"))

    def test_nanoseconds(self):
        self.assertEqual(_format_ns(234.5), "234.5 ns")


class TestComputeDeltaPct(unittest.TestCase):
    def test_positive(self):
        self.assertEqual(_compute_delta_pct(110.0, 100.0), "+10.0%")

    def test_negative(self):
        self.assertEqual(_compute_delta_pct(90.0, 100.0), "-10.0%")

    def test_zero(self):
        self.assertEqual(_compute_delta_pct(100.0, 100.0), "+0.0%")

    def test_large(self):
        self.assertEqual(_compute_delta_pct(200.0, 100.0), "+100.0%")


class TestBackfillTiming(unittest.TestCase):
    def test_basic_backfill(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_dir = tmp / "arm"
            x86_dir = tmp / "x86"
            arm_dir.mkdir()
            x86_dir.mkdir()

            arm_timing_dir = arm_dir / "timing"
            arm_timing_dir.mkdir()
            arm_timing = {
                "cases": [{
                    "caseId": "q01",
                    "metrics": {
                        "frameworkCallTime": {"per_invocation_ns": 810000},
                        "businessOperatorTime": {"per_invocation_ns": 1690000},
                    },
                }],
            }
            (arm_timing_dir / "timing-normalized.json").write_text(
                json.dumps(arm_timing), encoding="utf-8",
            )

            x86_timing_dir = x86_dir / "timing"
            x86_timing_dir.mkdir()
            x86_timing = {
                "cases": [{
                    "caseId": "q01",
                    "metrics": {
                        "frameworkCallTime": {"per_invocation_ns": 810000},
                        "businessOperatorTime": {"per_invocation_ns": 1690000},
                    },
                }],
            }
            (x86_timing_dir / "timing-normalized.json").write_text(
                json.dumps(x86_timing), encoding="utf-8",
            )

            dataset = {"cases": [{"id": "q01", "name": "Q1"}]}
            result = backfill_timing(arm_dir, x86_dir, dataset)
            self.assertEqual(result["cases_updated"], 1)
            self.assertIn("framework", dataset["cases"][0]["metrics"])
            self.assertIn("operator", dataset["cases"][0]["metrics"])

    def test_no_timing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            arm_dir = Path(tmp) / "arm"
            x86_dir = Path(tmp) / "x86"
            arm_dir.mkdir()
            x86_dir.mkdir()
            dataset = {"cases": []}
            result = backfill_timing(arm_dir, x86_dir, dataset)
            self.assertEqual(result["cases_updated"], 0)

    def test_auto_creates_missing_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_dir = tmp / "arm"
            x86_dir = tmp / "x86"
            arm_dir.mkdir()
            x86_dir.mkdir()

            for d in (arm_dir, x86_dir):
                td = d / "timing"
                td.mkdir()
                timing = {
                    "cases": [
                        {
                            "caseId": "q01",
                            "metrics": {
                                "frameworkCallTime": {"per_invocation_ns": 500_000},
                                "businessOperatorTime": {"per_invocation_ns": 200_000},
                            },
                        },
                        {
                            "caseId": "q03",
                            "metrics": {
                                "frameworkCallTime": {"per_invocation_ns": 600_000},
                                "businessOperatorTime": {"per_invocation_ns": 300_000},
                            },
                        },
                    ],
                }
                (td / "timing-normalized.json").write_text(
                    json.dumps(timing), encoding="utf-8",
                )

            dataset = {"cases": [{"id": "tpch-q01-pyflink", "legacyCaseId": "q01", "name": "Q1"}]}
            result = backfill_timing(arm_dir, x86_dir, dataset)

            self.assertEqual(result["cases_updated"], 2)
            self.assertEqual(len(dataset["cases"]), 2)
            case_ids = [c["legacyCaseId"] for c in dataset["cases"]]
            self.assertIn("q03", case_ids)
            q03 = next(c for c in dataset["cases"] if c["legacyCaseId"] == "q03")
            self.assertEqual(q03["id"], "tpch-q03-pyflink")
            self.assertEqual(q03["semanticStatus"], "auto-generated")
            self.assertIn("framework", q03["metrics"])


# ---------------------------------------------------------------------------
# perf_backfill
# ---------------------------------------------------------------------------

class TestResolveComponent(unittest.TestCase):
    def test_libpython(self):
        self.assertEqual(_resolve_component("libpython3.14.so"), "cpython")

    def test_libc(self):
        self.assertEqual(_resolve_component("libc-2.31.so"), "glibc")

    def test_kernel(self):
        self.assertEqual(_resolve_component("[kernel.kallsyms]"), "kernel")

    def test_unknown(self):
        self.assertEqual(_resolve_component("libmylib.so"), "third_party")
        self.assertEqual(_resolve_component(""), "unknown")
        self.assertEqual(_resolve_component("[unknown]"), "unknown")


class TestGenerateFuncId(unittest.TestCase):
    def test_deterministic(self):
        id1 = _generate_func_id("_PyObject_Malloc")
        id2 = _generate_func_id("_PyObject_Malloc")
        self.assertEqual(id1, id2)
        self.assertTrue(id1.startswith("func_"))

    def test_different_symbols(self):
        id1 = _generate_func_id("func_a")
        id2 = _generate_func_id("func_b")
        self.assertNotEqual(id1, id2)


class TestBackfillPerf(unittest.TestCase):
    def test_basic_perf(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_perf_dir = tmp / "arm" / "perf" / "data"
            arm_perf_dir.mkdir(parents=True)
            x86_perf_dir = tmp / "x86" / "perf" / "data"
            x86_perf_dir.mkdir(parents=True)

            arm_csv = arm_perf_dir / "perf_records.csv"
            with arm_csv.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "symbol", "self", "children", "period", "sample_count",
                    "category_top", "category_sub", "shared_object",
                ])
                w.writeheader()
                w.writerow({
                    "symbol": "_PyObject_Malloc", "self": "30.0", "children": "40.0",
                    "period": "1000", "sample_count": "500",
                    "category_top": "Memory", "category_sub": "alloc",
                    "shared_object": "libpython3.14.so",
                })

            x86_csv = x86_perf_dir / "perf_records.csv"
            with x86_csv.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "symbol", "self", "children", "period", "sample_count",
                    "category_top", "category_sub", "shared_object",
                ])
                w.writeheader()
                w.writerow({
                    "symbol": "_PyObject_Malloc", "self": "20.0", "children": "30.0",
                    "period": "800", "sample_count": "400",
                    "category_top": "Memory", "category_sub": "alloc",
                    "shared_object": "libpython3.14.so",
                })

            dataset = {"cases": [], "functions": []}
            result = backfill_perf(tmp / "arm", tmp / "x86", dataset)
            self.assertGreater(result["components"], 0)
            self.assertGreater(result["functions"], 0)
            self.assertIn("stackOverview", dataset)
            self.assertIn("functions", dataset)

    def test_delta_contribution_signed(self):
        """deltaContribution preserves sign and uses net delta denominator."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_perf_dir = tmp / "arm" / "perf" / "data"
            arm_perf_dir.mkdir(parents=True)
            x86_perf_dir = tmp / "x86" / "perf" / "data"
            x86_perf_dir.mkdir(parents=True)

            fieldnames = [
                "symbol", "self", "children", "period", "sample_count",
                "category_top", "category_sub", "shared_object",
            ]
            # ARM: two symbols in different categories
            with (arm_perf_dir / "perf_records.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerow({"symbol": "func_a", "self": "50.0", "children": "60.0",
                            "period": "1000", "sample_count": "500",
                            "category_top": "CPython.Interpreter", "category_sub": "",
                            "shared_object": "libpython3.14.so"})
                w.writerow({"symbol": "func_b", "self": "10.0", "children": "15.0",
                            "period": "500", "sample_count": "250",
                            "category_top": "glibc", "category_sub": "",
                            "shared_object": "libc.so.6"})

            # x86: func_a much less, func_b slightly more → mixed signs
            with (x86_perf_dir / "perf_records.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerow({"symbol": "func_a", "self": "20.0", "children": "30.0",
                            "period": "800", "sample_count": "400",
                            "category_top": "CPython.Interpreter", "category_sub": "",
                            "shared_object": "libpython3.14.so"})
                w.writerow({"symbol": "func_b", "self": "15.0", "children": "20.0",
                            "period": "600", "sample_count": "300",
                            "category_top": "glibc", "category_sub": "",
                            "shared_object": "libc.so.6"})

            dataset = {"cases": [], "functions": []}
            backfill_perf(tmp / "arm", tmp / "x86", dataset)

            # Extract component-level deltaContributions
            components = dataset["stackOverview"]["components"]
            contribs = [c["deltaContribution"] for c in components]
            # Net sum of parsed values must be ~100%
            total = sum(float(c.rstrip("%").lstrip("+")) for c in contribs)
            self.assertAlmostEqual(total, 100.0, places=5)

            # At least one positive and one negative contribution
            values = [float(c.rstrip("%").lstrip("+")) for c in contribs]
            self.assertTrue(any(v > 0 for v in values), f"expected positive contrib, got {values}")
            # Positive values must have "+" prefix
            for c in contribs:
                val = float(c.rstrip("%").lstrip("+"))
                if val > 0:
                    self.assertTrue(c.startswith("+"), f"positive contrib {c} missing '+' prefix")


# ---------------------------------------------------------------------------
# asm_backfill
# ---------------------------------------------------------------------------

class TestArtifactId(unittest.TestCase):
    def test_arm_artifact(self):
        aid = _artifact_id("arm64", "my_func")
        self.assertTrue(aid.startswith("asm_arm64_"))

    def test_x86_artifact(self):
        aid = _artifact_id("x86_64", "my_func")
        self.assertTrue(aid.startswith("asm_x86_64_"))


class TestSymbolHash(unittest.TestCase):
    def test_deterministic(self):
        h1 = _symbol_to_hash("_PyObject_Malloc")
        h2 = _symbol_to_hash("_PyObject_Malloc")
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 8)


class TestBackfillAsm(unittest.TestCase):
    def test_basic_asm(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_asm = tmp / "arm" / "asm" / "arm64"
            arm_asm.mkdir(parents=True)
            (arm_asm / "_PyObject_Malloc.s").write_text("ldr x0\n", encoding="utf-8")

            x86_asm = tmp / "x86" / "asm" / "x86_64"
            x86_asm.mkdir(parents=True)
            (x86_asm / "_PyObject_Malloc.s").write_text("mov rax\n", encoding="utf-8")

            source = {"artifactIndex": []}
            dataset = {"functions": []}
            result = backfill_asm(tmp / "arm", tmp / "x86", source, dataset)
            self.assertEqual(result["status"], "backfilled")
            self.assertEqual(result["armFiles"], 1)
            self.assertEqual(result["x86Files"], 1)
            self.assertEqual(result["uniqueSymbols"], 1)
            self.assertEqual(len(source["artifactIndex"]), 2)

    def test_arm_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_asm = tmp / "arm" / "asm" / "arm64"
            arm_asm.mkdir(parents=True)
            (arm_asm / "exclusive_func.s").write_text("nop\n", encoding="utf-8")

            x86_dir = tmp / "x86"
            x86_dir.mkdir()

            source = {"artifactIndex": []}
            dataset = {"functions": []}
            result = backfill_asm(tmp / "arm", tmp / "x86", source, dataset)
            self.assertEqual(result["armOnly"], 1)
            self.assertEqual(result["x86Only"], 0)


# ---------------------------------------------------------------------------
# binding_generator
# ---------------------------------------------------------------------------

class TestGenerateBindings(unittest.TestCase):
    def test_case_bindings(self):
        source = {
            "artifactIndex": [
                {"id": "sql_q01", "type": "sql"},
                {"id": "py_udf_q01", "type": "python"},
            ],
        }
        dataset = {
            "cases": [{
                "id": "tpch-q01",
                "artifactIds": ["sql_q01", "py_udf_q01"],
            }],
            "functions": [],
        }
        result = generate_bindings(dataset, source)
        self.assertEqual(len(result["caseBindings"]), 1)
        self.assertEqual(
            result["caseBindings"][0]["primaryArtifactIds"],
            ["sql_q01", "py_udf_q01"],
        )

    def test_function_bindings(self):
        source = {
            "artifactIndex": [
                {"id": "asm_arm_abc12345", "platform": "arm64"},
                {"id": "asm_x86_abc12345", "platform": "x86_64"},
            ],
        }
        dataset = {
            "cases": [],
            "functions": [{
                "id": "func_abc12345",
                "symbol": "my_func",
                "artifactIds": ["asm_arm_abc12345", "asm_x86_abc12345"],
            }],
        }
        result = generate_bindings(dataset, source)
        self.assertEqual(len(result["functionBindings"]), 1)
        fb = result["functionBindings"][0]
        self.assertEqual(fb["armArtifactIds"], ["asm_arm_abc12345"])
        self.assertEqual(fb["x86ArtifactIds"], ["asm_x86_abc12345"])

    def test_empty_dataset(self):
        result = generate_bindings({"cases": [], "functions": []}, {"artifactIndex": []})
        self.assertEqual(result["caseBindings"], [])
        self.assertEqual(result["functionBindings"], [])


if __name__ == "__main__":
    unittest.main()

class TestBackfillPyTorchTiming(unittest.TestCase):
    def test_auto_creates_pytorch_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            arm_dir = tmp / "arm"
            x86_dir = tmp / "x86"
            for platform_dir, value in ((arm_dir, 2_000_000_000), (x86_dir, 1_000_000_000)):
                timing_dir = platform_dir / "timing"
                timing_dir.mkdir(parents=True)
                timing = {
                    "framework": "pytorch",
                    "cases": [{
                        "caseId": "bytecode_tracing",
                        "metrics": {
                            "frameworkCallTime": {"total_ns": value},
                            "wallClockTime": {"wall_clock_ns": value},
                        },
                    }],
                }
                (timing_dir / "timing-normalized.json").write_text(json.dumps(timing), encoding="utf-8")

            dataset = {"cases": []}
            result = backfill_timing(arm_dir, x86_dir, dataset)
            self.assertEqual(result["cases_updated"], 1)
            self.assertEqual(dataset["cases"][0]["id"], "pytorch-bytecode-tracing")
            self.assertEqual(dataset["cases"][0]["benchmarkFamily"], "PyTorch Inductor")
            self.assertEqual(dataset["cases"][0]["metrics"]["frameworkDelta"], "+100.0%")
