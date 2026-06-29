"""Tests for the consume layer facade + renderer registry."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pyframework_pipeline.consume  # noqa: F401  (triggers renderer registration)
from pyframework_pipeline.consume import render as _render_module  # noqa: F401
from pyframework_pipeline.consume import get_renderer, render
from pyframework_pipeline.registry import get_renderer_registry


class TestRendererRegistry(unittest.TestCase):
    def test_builtin_renderers_registered(self):
        names = get_renderer_registry().names()
        self.assertIn("platform", names)
        self.assertIn("platform_full", names)
        self.assertIn("compare", names)
        self.assertIn("compare_integrated", names)

    def test_get_renderer_callable(self):
        self.assertTrue(callable(get_renderer("platform")))
        self.assertTrue(callable(get_renderer("compare")))

    def test_unknown_renderer_raises(self):
        with self.assertRaises(KeyError):
            get_renderer("nope")


class TestRenderPlatformOutput(unittest.TestCase):
    def test_render_platform_writes_report(self):
        # Build a minimal tables dir with the three summary CSVs the renderer
        # reads, then render and assert a non-empty file is produced.
        import csv

        with tempfile.TemporaryDirectory() as d:
            tables = Path(d) / "tables"
            tables.mkdir()
            for name, cols, rows in [
                ("category_summary.csv",
                 ["platform_id","benchmark","category_top","children_share","self_share","period_sum","sample_count","top_shared_object","top_symbols_preview"],
                 [{"platform_id":"x86","benchmark":"tpch","category_top":"Interpreter","children_share":"80","self_share":"50","period_sum":"1000","sample_count":"10","top_shared_object":"libpython","top_symbols_preview":"PyEval"}]),
                ("shared_object_summary.csv",
                 ["platform_id","benchmark","shared_object","children_share","self_share","period_sum","sample_count","top_symbols_preview"],
                 [{"platform_id":"x86","benchmark":"tpch","shared_object":"libpython","children_share":"80","self_share":"50","period_sum":"1000","sample_count":"10","top_symbols_preview":"PyEval"}]),
                ("symbol_hotspots.csv",
                 ["platform_id","benchmark","category_top","category_sub","shared_object","symbol","children_share","self_share","period_sum","sample_count","rank_in_category","rank_in_shared_object"],
                 [{"platform_id":"x86","benchmark":"tpch","category_top":"Interpreter","category_sub":"ceval","shared_object":"libpython","symbol":"PyEval","children_share":"80","self_share":"50","period_sum":"1000","sample_count":"10","rank_in_category":"1","rank_in_shared_object":"1"}]),
                ("ip_hotspots.csv",
                 ["platform_id","benchmark","shared_object","symbol","ip","self_share","period_sum","sample_count","rank_in_symbol"],
                 [{"platform_id":"x86","benchmark":"tpch","shared_object":"libpython","symbol":"PyEval","ip":"4194304","self_share":"50","period_sum":"1000","sample_count":"10","rank_in_symbol":"1"}]),
            ]:
                with (tables / name).open("w", encoding="utf-8", newline="") as fh:
                    w = csv.DictWriter(fh, fieldnames=cols)
                    w.writeheader()
                    w.writerows(rows)

            out = Path(d) / "report.md"
            render("platform", tables_dir=tables, output_path=out, top_n=10)
            self.assertTrue(out.exists())
            self.assertGreater(out.stat().st_size, 0)
            text = out.read_text(encoding="utf-8")
            self.assertIn("Interpreter", text)


if __name__ == "__main__":
    unittest.main()
