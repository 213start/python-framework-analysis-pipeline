from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.perf_analysis_common import (
    NORMALIZED_FIELDS,
    aggregate_rows,
    classify_record,
    compare_aggregates,
    load_rules,
    normalize_raw_row,
    write_csv_rows,
)
from pyframework_pipeline.analyze.summarize_platform_perf import summarize_ip_hotspots_from_script


RULES_PATH = Path(__file__).resolve().parents[1] / "pyframework_pipeline" / "analyze" / "cpython_category_rules.json"


class ClassificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rules = load_rules(RULES_PATH)

    def test_classify_kernel(self) -> None:
        top, sub, reason = classify_record(
            {"symbol": "copy_user_generic", "shared_object": "[kernel.kallsyms]"},
            self.rules,
        )
        self.assertEqual(top, "Kernel")
        self.assertEqual(sub, "")
        self.assertIn("shared_object", reason)

    def test_classify_glibc(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "memcpy", "shared_object": "/usr/lib/libc.so.6"},
            self.rules,
        )
        self.assertEqual(top, "glibc")
        self.assertEqual(sub, "")

    def test_classify_cpython_objects_dict(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "PyDict_GetItem", "shared_object": "/usr/bin/python3.12"},
            self.rules,
        )
        self.assertEqual(top, "CPython.Objects")
        self.assertEqual(sub, "CPython.Objects.Dict")

    def test_classify_cpython_objects_set_internal(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "_PySet_Contains", "shared_object": "/usr/bin/python3.12"},
            self.rules,
        )
        self.assertEqual(top, "CPython.Objects")
        self.assertEqual(sub, "CPython.Objects.Set")

    def test_classify_cpython_objects_list_internal(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "list_dealloc", "shared_object": "/usr/bin/python3.12"},
            self.rules,
        )
        self.assertEqual(top, "CPython.Objects")
        self.assertEqual(sub, "CPython.Objects.List")

    def test_classify_baseexception_vectorcall(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "BaseException_vectorcall", "shared_object": "/usr/bin/python3.12"},
            self.rules,
        )
        self.assertEqual(top, "CPython.Exceptions")
        self.assertEqual(sub, "CPython.Exceptions.BaseException")

    def test_classify_list_vectorcall(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "list_vectorcall", "shared_object": "/usr/bin/python3.12"},
            self.rules,
        )
        self.assertEqual(top, "CPython.Objects")
        self.assertEqual(sub, "CPython.Objects.List")

    def test_classify_dict_vectorcall(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "dict_vectorcall", "shared_object": "/usr/bin/python3.12"},
            self.rules,
        )
        self.assertEqual(top, "CPython.Objects")
        self.assertEqual(sub, "CPython.Objects.Dict")

    def test_classify_library(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "foo_extension_hotspot", "shared_object": "/tmp/custom_ext.so"},
            self.rules,
        )
        self.assertEqual(top, "Library")
        self.assertEqual(sub, "")

    def test_cpython_rule_requires_cpython_shared_object(self) -> None:
        top, sub, _ = classify_record(
            {"symbol": "PyDict_GetItem", "shared_object": "/tmp/custom_ext.so"},
            self.rules,
        )
        self.assertEqual(top, "Library")
        self.assertEqual(sub, "")

    def test_classify_runtime_gap_symbols(self) -> None:
        cases = [
            ("_Py_Dealloc", "CPython.Memory", ""),
            ("PyType_genericAlloc", "CPython.Memory", ""),
            ("_copy_characters", "CPython.Objects", "CPython.Objects.Str"),
            ("ucs2lib_utf8_encoder", "CPython.Objects", "CPython.Objects.Str"),
            ("getset_get", "CPython.Lookup", "CPython.Lookup.Attribute"),
        ]
        for symbol, expected_top, expected_sub in cases:
            with self.subTest(symbol=symbol):
                top, sub, _ = classify_record(
                    {"symbol": symbol, "shared_object": "/usr/bin/python3.12"},
                    self.rules,
                )
                self.assertEqual(top, expected_top)
                self.assertEqual(sub, expected_sub)


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rules = load_rules(RULES_PATH)

    def test_normalize_row(self) -> None:
        row = normalize_raw_row(
            {
                "Children": "52.10%",
                "Self": "12.34%",
                "Period": "4096",
                "Pid:Command": "1234:python3",
                "IP": "7f00aa10",
                "Symbol": "_PyEval_EvalFrameDefault",
                "Shared Object": "/usr/bin/python3.12",
            },
            platform_id="amd-baseline",
            arch="x86_64",
            python_version="3.12.2",
            build_id="cpython-opt",
            benchmark="richards",
            event="cycles",
            source_report="perf_report_csv",
            rules=self.rules,
        )
        self.assertEqual(row["category_top"], "CPython.Interpreter")
        self.assertEqual(row["pid"], "1234")
        self.assertEqual(row["command"], "python3")
        self.assertEqual(row["children"], "52.1")

    def test_normalize_row_cleans_perf_symbol_prefix(self) -> None:
        row = normalize_raw_row(
            {
                "Children": "10%",
                "Self": "5%",
                "Period": "100",
                "Pid:Command": "1234:python3",
                "IP": "7f00aa10",
                "Symbol": "[.] _PyEval_EvalFrameDefault",
                "Shared Object": "/usr/bin/python3.12",
            },
            platform_id="amd-baseline",
            arch="x86_64",
            python_version="3.12.2",
            build_id="cpython-opt",
            benchmark="richards",
            event="cycles",
            source_report="perf_report_csv",
            rules=self.rules,
        )
        self.assertEqual(row["symbol"], "_PyEval_EvalFrameDefault")
        self.assertEqual(row["category_top"], "CPython.Interpreter")

    def test_normalize_row_cleans_compiler_suffix(self) -> None:
        row = normalize_raw_row(
            {
                "Children": "10%",
                "Self": "5%",
                "Period": "100",
                "Pid:Command": "1234:python3",
                "IP": "7f00aa10",
                "Symbol": "[.] list_dealloc.lto_priv.0",
                "Shared Object": "/usr/bin/python3.12",
            },
            platform_id="amd-baseline",
            arch="x86_64",
            python_version="3.12.2",
            build_id="cpython-opt",
            benchmark="richards",
            event="cycles",
            source_report="perf_report_csv",
            rules=self.rules,
        )
        self.assertEqual(row["symbol"], "list_dealloc")
        self.assertEqual(row["category_top"], "CPython.Objects")
        self.assertEqual(row["category_sub"], "CPython.Objects.List")

    def test_compare_aggregates(self) -> None:
        baseline_rows = [
            {
                "platform_id": "amd-baseline",
                "benchmark": "richards",
                "category_top": "CPython.Interpreter",
                "category_sub": "",
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyEval_EvalFrameDefault",
                "children": "40",
                "self": "20",
                "period": "100",
                "sample_count": "1",
            },
            {
                "platform_id": "amd-baseline",
                "benchmark": "richards",
                "category_top": "glibc",
                "category_sub": "",
                "shared_object": "/usr/lib/libc.so.6",
                "symbol": "memcpy",
                "children": "10",
                "self": "10",
                "period": "50",
                "sample_count": "1",
            },
        ]
        target_rows = [
            {
                "platform_id": "arm-target",
                "benchmark": "richards",
                "category_top": "CPython.Interpreter",
                "category_sub": "",
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyEval_EvalFrameDefault",
                "children": "30",
                "self": "15",
                "period": "120",
                "sample_count": "1",
            }
        ]
        rows = compare_aggregates(
            baseline_rows,
            target_rows,
            ["category_top"],
            baseline_platform="amd",
            target_platform="arm",
            baseline_e2e_time=10.0,
            target_e2e_time=8.0,
        )
        self.assertEqual(rows[0]["category_top"], "CPython.Interpreter")
        self.assertEqual(rows[0]["baseline_est_time"], "2")
        self.assertEqual(rows[0]["target_est_time"], "1.2")
        self.assertEqual(rows[0]["delta_time"], "0.8")
        self.assertEqual(rows[0]["delta_share"], "5")

    def test_compare_aggregates_baseline_only(self) -> None:
        baseline_rows = [
            {
                "platform_id": "amd-baseline",
                "benchmark": "richards",
                "category_top": "CPython.Interpreter",
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyEval_EvalFrameDefault",
                "children": "40",
                "self": "20",
                "period": "100",
                "sample_count": "1",
            }
        ]
        target_rows = [
            {
                "platform_id": "arm-target",
                "benchmark": "richards",
                "category_top": "CPython.Interpreter",
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyEval_EvalFrameDefault",
                "children": "30",
                "self": "15",
                "period": "120",
                "sample_count": "1",
            },
            {
                "platform_id": "arm-target",
                "benchmark": "richards",
                "category_top": "CPython.Runtime",
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyRuntime_OnlyOnTarget",
                "children": "10",
                "self": "8",
                "period": "20",
                "sample_count": "1",
            },
        ]
        rows = compare_aggregates(
            baseline_rows,
            target_rows,
            ["category_top", "shared_object", "symbol"],
            baseline_platform="amd",
            target_platform="arm",
            baseline_e2e_time=10.0,
            target_e2e_time=8.0,
            include_target_only=False,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "_PyEval_EvalFrameDefault")

    def test_write_normalized_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "normalized.csv"
            write_csv_rows(
                output,
                NORMALIZED_FIELDS,
                [
                    {
                        "platform_id": "amd",
                        "arch": "x86_64",
                        "python_version": "3.12.2",
                        "build_id": "",
                        "benchmark": "richards",
                        "event": "cycles",
                        "children": "1",
                        "self": "1",
                        "period": "1",
                        "pid": "",
                        "command": "python3",
                        "pid_command": "python3",
                        "shared_object": "/usr/bin/python3.12",
                        "symbol": "_PyEval_EvalFrameDefault",
                        "ip": "",
                        "category_top": "CPython.Interpreter",
                        "category_sub": "",
                        "category_reason": "symbol_prefix:_PyEval_",
                        "source_report": "perf_report_csv",
                        "sample_count": "1",
                        "instruction_text": "",
                        "instruction_offset": "",
                        "extra_field": "ignored"
                    }
                ],
            )
            self.assertTrue(output.exists())

    def test_aggregate_rows(self) -> None:
        rows = aggregate_rows(
            [
                {
                    "platform_id": "amd",
                    "benchmark": "richards",
                    "category_top": "CPython.Interpreter",
                    "symbol": "_PyEval_EvalFrameDefault",
                    "shared_object": "/usr/bin/python3.12",
                    "children": "10",
                    "self": "5",
                    "period": "100",
                    "sample_count": "1",
                },
                {
                    "platform_id": "amd",
                    "benchmark": "richards",
                    "category_top": "CPython.Interpreter",
                    "symbol": "_PyEval_EvalFrameDefault",
                    "shared_object": "/usr/bin/python3.12",
                    "children": "15",
                    "self": "7",
                    "period": "200",
                    "sample_count": "1",
                },
            ],
            ["platform_id", "benchmark", "category_top"],
        )
        self.assertEqual(rows[0]["children_share"], "25")
        self.assertEqual(rows[0]["period_sum"], "300")

    def test_summarize_ip_hotspots_from_script(self) -> None:
        rows = summarize_ip_hotspots_from_script(
            [
                {"Period": "60", "Pid:Command": "1:python3", "IP": "aaa", "Symbol": "hot_symbol", "Shared Object": "/usr/bin/python3.12"},
                {"Period": "30", "Pid:Command": "1:python3", "IP": "bbb", "Symbol": "hot_symbol", "Shared Object": "/usr/bin/python3.12"},
                {"Period": "10", "Pid:Command": "1:python3", "IP": "ccc", "Symbol": "other_symbol", "Shared Object": "/usr/bin/python3.12"},
            ],
            [
                {"category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_symbol"},
                {"category_top": "CPython.Runtime", "shared_object": "/usr/bin/python3.12", "symbol": "other_symbol"},
            ],
            platform_id="amd-baseline",
            benchmark="richards",
        )
        self.assertEqual(rows[0]["ip"], "aaa")
        self.assertEqual(rows[0]["category_top"], "CPython.Interpreter")
        self.assertEqual(rows[0]["self_share"], "60")
        self.assertEqual(rows[1]["ip"], "bbb")
        self.assertEqual(rows[1]["self_share"], "30")

    def test_summarize_ip_hotspots_from_script_falls_back_to_symbol_metadata(self) -> None:
        rows = summarize_ip_hotspots_from_script(
            [
                {"Period": "80", "Pid:Command": "", "IP": "aaa", "Symbol": "_PyEval_EvalFrameDefault", "Shared Object": "inlined"},
                {"Period": "20", "Pid:Command": "", "IP": "bbb", "Symbol": "_PyEval_EvalFrameDefault", "Shared Object": "0xffff8c0012340000"},
            ],
            [
                {
                    "category_top": "CPython.Interpreter",
                    "shared_object": "/usr/bin/python3.12",
                    "symbol": "_PyEval_EvalFrameDefault",
                }
            ],
            platform_id="amd-baseline",
            benchmark="richards",
        )
        self.assertEqual(rows[0]["category_top"], "CPython.Interpreter")
        self.assertEqual(rows[0]["shared_object"], "/usr/bin/python3.12")
        self.assertEqual(rows[0]["self_share"], "80")


if __name__ == "__main__":
    unittest.main()
