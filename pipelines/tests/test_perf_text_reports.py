from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.perf_analysis_common import write_csv_rows
from pyframework_pipeline.analyze.render_compare_integrated_report import render_report as render_compare_integrated_report
from pyframework_pipeline.analyze.render_compare_report import render_report as render_compare_report
from pyframework_pipeline.analyze.render_platform_machine_code_report import render_report as render_machine_code_report
from pyframework_pipeline.analyze.render_platform_report import render_report as render_platform_report


class TextReportTests(unittest.TestCase):
    def test_render_platform_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [
                    {
                        "platform_id": "amd-baseline",
                        "benchmark": "richards",
                        "category_top": "CPython.Interpreter",
                        "children_share": "40",
                        "self_share": "20",
                        "period_sum": "100",
                        "sample_count": "1",
                        "top_shared_object": "/usr/bin/python3.12 (20%)",
                        "top_symbols_preview": "_PyEval_EvalFrameDefault (20%)",
                    }
                ],
            )
            write_csv_rows(
                root / "shared_object_summary.csv",
                ["platform_id", "benchmark", "shared_object", "children_share", "self_share", "period_sum", "sample_count", "top_symbols_preview"],
                [
                    {
                        "platform_id": "amd-baseline",
                        "benchmark": "richards",
                        "shared_object": "/usr/bin/python3.12",
                        "children_share": "40",
                        "self_share": "20",
                        "period_sum": "100",
                        "sample_count": "1",
                        "top_symbols_preview": "_PyEval_EvalFrameDefault (20%)",
                    }
                ],
            )
            write_csv_rows(
                root / "instruction_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "instruction_offset", "instruction_share", "instruction_text"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "7f00aa10", "instruction_offset": "0x0", "instruction_share": "35", "instruction_text": "mov %rax,%rbx"}
                ],
            )
            write_csv_rows(
                root / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [
                    {
                        "platform_id": "amd-baseline",
                        "benchmark": "richards",
                        "category_top": "CPython.Interpreter",
                        "category_sub": "",
                        "shared_object": "/usr/bin/python3.12",
                        "symbol": "_PyEval_EvalFrameDefault",
                        "children_share": "40",
                        "self_share": "20",
                        "period_sum": "100",
                        "sample_count": "1",
                        "rank_in_category": "1",
                        "rank_in_shared_object": "1",
                    }
                ],
            )
            write_csv_rows(
                root / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {
                        "platform_id": "amd-baseline",
                        "benchmark": "richards",
                        "category_top": "CPython.Interpreter",
                        "shared_object": "/usr/bin/python3.12",
                        "symbol": "_PyEval_EvalFrameDefault",
                        "ip": "7f00aa10",
                        "children_share": "40",
                        "self_share": "20",
                        "period_sum": "100",
                        "sample_count": "1",
                        "instruction_offset": "0x0",
                        "instruction_share": "35",
                        "hotspot_self": "35",
                        "instruction_text": "mov %rax,%rbx",
                        "rank_in_symbol": "1",
                    }
                ],
            )

            text = render_platform_report(root, 10, sort_by="self")
            self.assertIn("# 单平台性能报告", text)
            self.assertIn("amd-baseline", text)
            self.assertIn("_PyEval_EvalFrameDefault", text)
            self.assertIn("## 热点 DSO", text)
            self.assertIn("## 热点分类总表", text)
            self.assertIn("## 热点分类", text)
            self.assertIn("#### 函数: `_PyEval_EvalFrameDefault`", text)
            self.assertIn("| Shared Object | Self% |", text)
            self.assertIn("| Category | Self% |", text)
            self.assertIn("| Symbol | Shared Object | Self% |", text)
            self.assertIn("| IP | Address Self% |", text)

    def test_render_platform_report_with_filter_and_annotation_priority(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_shared_object": "/usr/bin/python3.12 (20%)", "top_symbols_preview": "a"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "glibc", "children_share": "10", "self_share": "10", "period_sum": "20", "sample_count": "1", "top_shared_object": "/usr/lib/libc.so.6 (10%)", "top_symbols_preview": "b"},
                ],
            )
            write_csv_rows(
                root / "shared_object_summary.csv",
                ["platform_id", "benchmark", "shared_object", "children_share", "self_share", "period_sum", "sample_count", "top_symbols_preview"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "shared_object": "/usr/bin/python3.12", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_symbols_preview": "a"}],
            )
            write_csv_rows(
                root / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "1"}],
            )
            write_csv_rows(
                root / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "1", "children_share": "5", "self_share": "5", "period_sum": "10", "sample_count": "1", "instruction_offset": "", "instruction_share": "", "hotspot_self": "5", "instruction_text": "", "rank_in_symbol": "2"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "2", "children_share": "4", "self_share": "4", "period_sum": "8", "sample_count": "1", "instruction_offset": "0x0", "instruction_share": "17", "hotspot_self": "4", "instruction_text": "mov x0, x1", "rank_in_symbol": "1"},
                ],
            )
            write_csv_rows(
                root / "instruction_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "instruction_offset", "instruction_share", "instruction_text"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "2", "instruction_offset": "0x0", "instruction_share": "17", "instruction_text": "mov x0, x1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "3", "instruction_offset": "0x4", "instruction_share": "9", "instruction_text": "add x0, x0, #1"},
                ],
            )
            text = render_platform_report(root, 10, sort_by="self", category="CPython.Interpreter")
            self.assertIn("分类过滤: CPython.Interpreter", text)
            self.assertNotIn("glibc", text)
            self.assertIn("| 1 | 5 |", text)
            self.assertIn("| 2 | 4 |", text)

    def test_render_platform_report_trims_zero_self_and_aligns_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_shared_object": "/usr/bin/python3.12 (20%)", "top_symbols_preview": "a"},
                ],
            )
            write_csv_rows(
                root / "shared_object_summary.csv",
                ["platform_id", "benchmark", "shared_object", "children_share", "self_share", "period_sum", "sample_count", "top_symbols_preview"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "shared_object": "/usr/bin/python3.12", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_symbols_preview": "a"}],
            )
            write_csv_rows(
                root / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "children_share": "10", "self_share": "2", "period_sum": "10", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_2", "children_share": "10", "self_share": "1", "period_sum": "10", "sample_count": "1", "rank_in_category": "2", "rank_in_shared_object": "2"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_3", "children_share": "10", "self_share": "0.4", "period_sum": "10", "sample_count": "1", "rank_in_category": "3", "rank_in_shared_object": "3"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_4", "children_share": "10", "self_share": "0.2", "period_sum": "10", "sample_count": "1", "rank_in_category": "4", "rank_in_shared_object": "4"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "cold_0", "children_share": "10", "self_share": "0", "period_sum": "10", "sample_count": "1", "rank_in_category": "5", "rank_in_shared_object": "5"},
                ],
            )
            write_csv_rows(
                root / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "a", "children_share": "5", "self_share": "0.8", "period_sum": "10", "sample_count": "1", "instruction_offset": "0x100", "instruction_share": "40", "hotspot_self": "0.8", "instruction_text": "inst_a", "rank_in_symbol": "1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "b", "children_share": "4", "self_share": "0.6", "period_sum": "8", "sample_count": "1", "instruction_offset": "0x1600", "instruction_share": "30", "hotspot_self": "0.6", "instruction_text": "inst_b", "rank_in_symbol": "2"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "c", "children_share": "3", "self_share": "0.4", "period_sum": "6", "sample_count": "1", "instruction_offset": "0x20", "instruction_share": "20", "hotspot_self": "0.4", "instruction_text": "inst_c", "rank_in_symbol": "3"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "d", "children_share": "2", "self_share": "0.2", "period_sum": "4", "sample_count": "1", "instruction_offset": "0x8", "instruction_share": "10", "hotspot_self": "0.2", "instruction_text": "inst_d", "rank_in_symbol": "4"},
                ],
            )
            write_csv_rows(
                root / "instruction_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "instruction_offset", "instruction_share", "instruction_text"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "a", "instruction_offset": "0x100", "instruction_share": "40", "instruction_text": "inst_a"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "b", "instruction_offset": "0x1600", "instruction_share": "30", "instruction_text": "inst_b"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "c", "instruction_offset": "0x20", "instruction_share": "20", "instruction_text": "inst_c"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "d", "instruction_offset": "0x8", "instruction_share": "10", "instruction_text": "inst_d"},
                ],
            )

            text = render_platform_report(root, 10, sort_by="self")
            self.assertIn("hot_1", text)
            self.assertIn("hot_2", text)
            self.assertIn("hot_3", text)
            self.assertNotIn("hot_4", text)
            self.assertNotIn("cold_0", text)
            self.assertIn("| a | 0.8 |", text)
            self.assertIn("| b | 0.6 |", text)
            self.assertIn("| c | 0.4 |", text)
            self.assertIn("| d | 0.2 |", text)

    def test_render_platform_report_full_keeps_positive_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_shared_object": "/usr/bin/python3.12 (20%)", "top_symbols_preview": "a"},
                ],
            )
            write_csv_rows(
                root / "shared_object_summary.csv",
                ["platform_id", "benchmark", "shared_object", "children_share", "self_share", "period_sum", "sample_count", "top_symbols_preview"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "shared_object": "/usr/bin/python3.12", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_symbols_preview": "a"}],
            )
            write_csv_rows(
                root / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "children_share": "10", "self_share": "2", "period_sum": "10", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_2", "children_share": "10", "self_share": "1", "period_sum": "10", "sample_count": "1", "rank_in_category": "2", "rank_in_shared_object": "2"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_3", "children_share": "10", "self_share": "0.4", "period_sum": "10", "sample_count": "1", "rank_in_category": "3", "rank_in_shared_object": "3"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_4", "children_share": "10", "self_share": "0.2", "period_sum": "10", "sample_count": "1", "rank_in_category": "4", "rank_in_shared_object": "4"},
                ],
            )
            write_csv_rows(
                root / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "a", "children_share": "5", "self_share": "5", "period_sum": "10", "sample_count": "1", "instruction_offset": "0x10", "instruction_share": "", "hotspot_self": "0.4", "instruction_text": "inst_a", "rank_in_symbol": "1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "b", "children_share": "4", "self_share": "4", "period_sum": "8", "sample_count": "1", "instruction_offset": "0x20", "instruction_share": "", "hotspot_self": "0.2", "instruction_text": "inst_b", "rank_in_symbol": "2"},
                ],
            )

            text = render_platform_report(root, 10, sort_by="self", report_style="full")
            self.assertIn("- 视图: full", text)
            self.assertIn("hot_3", text)
            self.assertIn("hot_4", text)
            self.assertIn("| a | 0.4 |", text)
            self.assertIn("| b | 0.2 |", text)

    def test_render_platform_machine_code_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_shared_object": "/usr/bin/python3.12 (20%)", "top_symbols_preview": "a"},
                ],
            )
            write_csv_rows(
                root / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "children_share": "10", "self_share": "2", "period_sum": "10", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_2", "children_share": "10", "self_share": "1", "period_sum": "10", "sample_count": "1", "rank_in_category": "2", "rank_in_shared_object": "2"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_3", "children_share": "10", "self_share": "0.4", "period_sum": "10", "sample_count": "1", "rank_in_category": "3", "rank_in_shared_object": "3"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "hot_4", "children_share": "10", "self_share": "0.2", "period_sum": "10", "sample_count": "1", "rank_in_category": "4", "rank_in_shared_object": "4"},
                ],
            )
            write_csv_rows(
                root / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "a", "children_share": "5", "self_share": "2", "period_sum": "10", "sample_count": "1", "instruction_offset": "0x100", "instruction_share": "40", "hotspot_self": "2", "instruction_text": "inst_hot", "rank_in_symbol": "1"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "ip": "b", "children_share": "4", "self_share": "2", "period_sum": "8", "sample_count": "1", "instruction_offset": "0x20", "instruction_share": "10", "hotspot_self": "2", "instruction_text": "inst_cold", "rank_in_symbol": "2"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_2", "ip": "c", "children_share": "3", "self_share": "1", "period_sum": "6", "sample_count": "1", "instruction_offset": "0x8", "instruction_share": "50", "hotspot_self": "1", "instruction_text": "inst_filtered", "rank_in_symbol": "1"},
                ],
            )
            write_csv_rows(
                root / "instruction_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "segment_id", "line_index", "ip", "instruction_offset", "instruction_share", "instruction_text"],
                [
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "segment_id": "0", "line_index": "1", "ip": "a", "instruction_offset": "0x100", "instruction_share": "40", "instruction_text": "inst_hot"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_1", "segment_id": "1", "line_index": "2", "ip": "b", "instruction_offset": "0x20", "instruction_share": "10", "instruction_text": "inst_cold"},
                    {"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "hot_2", "segment_id": "0", "line_index": "1", "ip": "c", "instruction_offset": "0x8", "instruction_share": "50", "instruction_text": "inst_filtered"},
                ],
            )

            text = render_machine_code_report(root, 10)
            self.assertIn("# 单平台机器码报告", text)
            self.assertIn("#### 函数: `hot_1`", text)
            self.assertIn("#### 函数: `hot_2`", text)
            self.assertIn("#### 函数: `hot_3`", text)
            self.assertNotIn("#### 函数: `hot_4`", text)
            self.assertIn("| Instruction% | IP | Offset | Instruction |", text)
            self.assertIn("##### IP 段 1", text)
            self.assertIn("##### IP 段 2", text)
            self.assertIn("- IP: `a`", text)
            self.assertIn("- IP Self%: 2", text)
            self.assertIn('<span style="color:#c62828">40</span>', text)
            self.assertIn('<span style="color:#ef6c00">10</span>', text)
            self.assertIn('<span style="color:#c62828">50</span>', text)
            self.assertIn("_No instructions_", text)

    def test_render_compare_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_compare.csv",
                ["benchmark", "category_top", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {
                        "benchmark": "richards",
                        "category_top": "CPython.Interpreter",
                        "baseline_platform": "amd",
                        "target_platform": "arm",
                        "baseline_share": "20",
                        "target_share": "15",
                        "baseline_e2e_time": "10",
                        "target_e2e_time": "8",
                        "baseline_est_time": "2",
                        "target_est_time": "1.2",
                        "delta_time": "0.8",
                        "delta_share": "5",
                    }
                ],
            )
            write_csv_rows(
                root / "shared_object_compare.csv",
                ["benchmark", "category_top", "shared_object", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {
                        "benchmark": "richards",
                        "category_top": "CPython.Interpreter",
                        "shared_object": "/usr/bin/python3.12",
                        "baseline_platform": "amd",
                        "target_platform": "arm",
                        "baseline_share": "20",
                        "target_share": "15",
                        "baseline_e2e_time": "10",
                        "target_e2e_time": "8",
                        "baseline_est_time": "2",
                        "target_est_time": "1.2",
                        "delta_time": "0.8",
                        "delta_share": "5",
                    }
                ],
            )
            write_csv_rows(
                root / "symbol_compare.csv",
                ["benchmark", "category_top", "shared_object", "symbol", "baseline_rank", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {
                        "benchmark": "richards",
                        "category_top": "CPython.Interpreter",
                        "shared_object": "/usr/bin/python3.12",
                        "symbol": "_PyEval_EvalFrameDefault",
                        "baseline_rank": "1",
                        "baseline_platform": "amd",
                        "target_platform": "arm",
                        "baseline_share": "20",
                        "target_share": "15",
                        "baseline_e2e_time": "10",
                        "target_e2e_time": "8",
                        "baseline_est_time": "2",
                        "target_est_time": "1.2",
                        "delta_time": "0.8",
                        "delta_share": "5",
                    }
                ],
            )

            text = render_compare_report(root, 10, sort_by="delta_time")
            self.assertIn("# 双平台对比报告", text)
            self.assertIn("amd", text)
            self.assertIn("arm", text)
            self.assertIn("_PyEval_EvalFrameDefault", text)
            self.assertIn("arm faster", text)
            self.assertIn("| Category | amd% | arm% | amd Time (s) | arm Time (s) | Delta Time (s) | Note |", text)
            self.assertIn("| Shared Object | amd Time (s) | arm Time (s) | Delta Time (s) | Note |", text)
            self.assertIn("| Category | Symbol | amd Time (s) | arm Time (s) | Delta Time (s) | Note |", text)

    def test_render_compare_report_with_filter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_compare.csv",
                ["benchmark", "category_top", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {"benchmark": "richards", "category_top": "CPython.Interpreter", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "15", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "1.2", "delta_time": "0.8", "delta_share": "5"},
                    {"benchmark": "richards", "category_top": "glibc", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "10", "target_share": "8", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "1", "target_est_time": "0.64", "delta_time": "0.36", "delta_share": "2"},
                ],
            )
            write_csv_rows(
                root / "shared_object_compare.csv",
                ["benchmark", "category_top", "shared_object", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "15", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "1.2", "delta_time": "0.8", "delta_share": "5"}],
            )
            write_csv_rows(
                root / "symbol_compare.csv",
                ["benchmark", "category_top", "shared_object", "symbol", "baseline_rank", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "baseline_rank": "1", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "15", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "1.2", "delta_time": "0.8", "delta_share": "5"}],
            )
            text = render_compare_report(root, 10, sort_by="delta_time", category="CPython.Interpreter")
            self.assertIn("分类过滤: CPython.Interpreter", text)
            self.assertNotIn("glibc", text)

    def test_render_compare_report_skips_zero_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_compare.csv",
                ["benchmark", "category_top", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {"benchmark": "richards", "category_top": "CPython.Memory", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "0", "target_share": "0", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "0", "target_est_time": "0", "delta_time": "0", "delta_share": "0"},
                    {"benchmark": "richards", "category_top": "CPython.Interpreter", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "15", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "1.2", "delta_time": "0.8", "delta_share": "5"},
                ],
            )
            write_csv_rows(
                root / "shared_object_compare.csv",
                ["benchmark", "category_top", "shared_object", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "15", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "1.2", "delta_time": "0.8", "delta_share": "5"}],
            )
            write_csv_rows(
                root / "symbol_compare.csv",
                ["benchmark", "category_top", "shared_object", "symbol", "baseline_rank", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "baseline_rank": "1", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "15", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "1.2", "delta_time": "0.8", "delta_share": "5"}],
            )
            text = render_compare_report(root, 10, sort_by="delta_time")
            self.assertIn("CPython.Interpreter", text)
            self.assertNotIn("CPython.Memory", text)

    def test_render_compare_integrated_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            compare_root = root / "compare"
            baseline_root = root / "baseline"
            target_root = root / "target"
            visuals_root = compare_root / "visuals"
            visuals_root.mkdir(parents=True)
            (visuals_root / "category_compare.svg").write_text("<svg><text>cat</text></svg>", encoding="utf-8")
            (visuals_root / "shared_object_compare.svg").write_text("<svg><text>dso</text></svg>", encoding="utf-8")
            (visuals_root / "symbol_delta.svg").write_text("<svg><text>sym</text></svg>", encoding="utf-8")

            write_csv_rows(
                compare_root / "tables" / "category_compare.csv",
                ["benchmark", "category_top", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {"benchmark": "richards", "category_top": "CPython.Interpreter", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "40", "target_share": "30", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "4", "target_est_time": "2.4", "delta_time": "1.6", "delta_share": "10"},
                    {"benchmark": "richards", "category_top": "CPython.Memory", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "0", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "0", "delta_time": "2", "delta_share": "20"},
                ],
            )
            write_csv_rows(
                compare_root / "tables" / "shared_object_compare.csv",
                ["benchmark", "category_top", "shared_object", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "40", "target_share": "30", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "4", "target_est_time": "2.4", "delta_time": "1.6", "delta_share": "10"}],
            )
            write_csv_rows(
                compare_root / "tables" / "symbol_compare.csv",
                ["benchmark", "category_top", "shared_object", "symbol", "baseline_rank", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [
                    {"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "baseline_rank": "1", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "40", "target_share": "30", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "4", "target_est_time": "2.4", "delta_time": "1.6", "delta_share": "10"},
                    {"benchmark": "richards", "category_top": "CPython.Memory", "shared_object": "/usr/bin/python3.12", "symbol": "_PyObject_Malloc", "baseline_rank": "1", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "20", "target_share": "0", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "2", "target_est_time": "0", "delta_time": "2", "delta_share": "20"},
                ],
            )

            write_csv_rows(
                baseline_root / "tables" / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Interpreter", "children_share": "0", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_shared_object": "", "top_symbols_preview": ""},
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Memory", "children_share": "0", "self_share": "10", "period_sum": "60", "sample_count": "1", "top_shared_object": "", "top_symbols_preview": ""},
                ],
            )
            write_csv_rows(
                baseline_root / "tables" / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "children_share": "0", "self_share": "20", "period_sum": "100", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "1"},
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Memory", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "_PyObject_Malloc", "children_share": "0", "self_share": "10", "period_sum": "60", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "2"},
                ],
            )
            write_csv_rows(
                baseline_root / "tables" / "instruction_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "segment_id", "line_index", "ip", "instruction_offset", "instruction_share", "instruction_text"],
                [
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "segment_id": "0", "line_index": "1", "ip": "aaa", "instruction_offset": "0x10", "instruction_share": "40", "instruction_text": "mov %rax,%rbx"},
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Memory", "shared_object": "/usr/bin/python3.12", "symbol": "_PyObject_Malloc", "segment_id": "0", "line_index": "1", "ip": "ccc", "instruction_offset": "0x30", "instruction_share": "22", "instruction_text": "bl _PyObject_Malloc"},
                ],
            )
            write_csv_rows(
                baseline_root / "tables" / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "aaa", "children_share": "0", "self_share": "8", "period_sum": "40", "sample_count": "1", "instruction_offset": "0x10", "instruction_share": "40", "hotspot_self": "8", "instruction_text": "mov %rax,%rbx", "rank_in_symbol": "1"},
                    {"platform_id": "amd", "benchmark": "richards", "category_top": "CPython.Memory", "shared_object": "/usr/bin/python3.12", "symbol": "_PyObject_Malloc", "ip": "ccc", "children_share": "0", "self_share": "3", "period_sum": "20", "sample_count": "1", "instruction_offset": "0x30", "instruction_share": "22", "hotspot_self": "3", "instruction_text": "bl _PyObject_Malloc", "rank_in_symbol": "1"},
                ],
            )
            write_csv_rows(
                target_root / "tables" / "instruction_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "segment_id", "line_index", "ip", "instruction_offset", "instruction_share", "instruction_text"],
                [
                    {"platform_id": "arm", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "segment_id": "0", "line_index": "1", "ip": "bbb", "instruction_offset": "0x20", "instruction_share": "35", "instruction_text": "ldr x0, [x1]"},
                    {"platform_id": "arm", "benchmark": "richards", "category_top": "CPython.Memory", "shared_object": "/usr/bin/python3.12", "symbol": "_PyObject_Malloc", "segment_id": "0", "line_index": "1", "ip": "ddd", "instruction_offset": "0x40", "instruction_share": "18", "instruction_text": "blr x9"},
                ],
            )
            write_csv_rows(
                target_root / "tables" / "ip_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip", "children_share", "self_share", "period_sum", "sample_count", "instruction_offset", "instruction_share", "hotspot_self", "instruction_text", "rank_in_symbol"],
                [
                    {"platform_id": "arm", "benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "ip": "bbb", "children_share": "0", "self_share": "7", "period_sum": "35", "sample_count": "1", "instruction_offset": "0x20", "instruction_share": "35", "hotspot_self": "7", "instruction_text": "ldr x0, [x1]", "rank_in_symbol": "1"},
                    {"platform_id": "arm", "benchmark": "richards", "category_top": "CPython.Memory", "shared_object": "/usr/bin/python3.12", "symbol": "_PyObject_Malloc", "ip": "ddd", "children_share": "0", "self_share": "2", "period_sum": "15", "sample_count": "1", "instruction_offset": "0x40", "instruction_share": "18", "hotspot_self": "2", "instruction_text": "blr x9", "rank_in_symbol": "1"},
                ],
            )

            text = render_compare_integrated_report(
                compare_root / "tables",
                top_n=10,
                baseline_root=baseline_root,
                target_root=target_root,
                visuals_dir=visuals_root,
            )
            self.assertIn("<!DOCTYPE html>", text)
            self.assertIn("<html", text)
            self.assertIn("双平台整合报告", text)
            self.assertIn("热点对比", text)
            self.assertIn("差异分析", text)
            self.assertIn("<img", text)
            self.assertIn("data:image/svg+xml;base64,", text)
            self.assertIn("<table", text)
            self.assertIn('class="analysis-category"', text)
            self.assertIn('class="function-compare"', text)
            self.assertIn('class="code-compare"', text)
            self.assertIn('class="ip-segment"', text)
            self.assertIn("amd Self%=20%", text)
            self.assertIn("amd Self%=10%", text)
            self.assertIn("_PyEval_EvalFrameDefault", text)
            self.assertIn("_PyObject_Malloc", text)
            self.assertLess(text.index("CPython.Memory"), text.index("CPython.Interpreter"))
            self.assertIn("IP Self%=8%", text)
            self.assertIn("IP Self%=7%", text)
            self.assertNotIn("IP 段 1", text)
            self.assertIn("IP=aaa", text)
            self.assertIn("IP=bbb", text)
            self.assertIn('style="color:#c62828"', text)
            self.assertIn("mov %rax,%rbx", text)
            self.assertIn("ldr x0, [x1]", text)


if __name__ == "__main__":
    unittest.main()
