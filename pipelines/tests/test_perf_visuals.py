from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.perf_analysis_common import write_csv_rows
from pyframework_pipeline.analyze.render_platform_visuals import render_visuals


class PerfVisualTests(unittest.TestCase):
    def test_render_visuals(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_summary.csv",
                ["platform_id", "benchmark", "category_top", "children_share", "self_share", "period_sum", "sample_count", "top_shared_object", "top_symbols_preview"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_shared_object": "/usr/bin/python3.12 (20%)", "top_symbols_preview": "_PyEval"}],
            )
            write_csv_rows(
                root / "shared_object_summary.csv",
                ["platform_id", "benchmark", "shared_object", "children_share", "self_share", "period_sum", "sample_count", "top_symbols_preview"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "shared_object": "/usr/bin/python3.12", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "top_symbols_preview": "_PyEval"}],
            )
            write_csv_rows(
                root / "symbol_hotspots.csv",
                ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol", "children_share", "self_share", "period_sum", "sample_count", "rank_in_category", "rank_in_shared_object"],
                [{"platform_id": "amd-baseline", "benchmark": "richards", "category_top": "CPython.Interpreter", "category_sub": "", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "children_share": "40", "self_share": "20", "period_sum": "100", "sample_count": "1", "rank_in_category": "1", "rank_in_shared_object": "1"}],
            )

            output_dir = root / "visuals"
            files = render_visuals(root, output_dir, 10)
            self.assertEqual(len(files), 3)
            self.assertTrue((output_dir / "category_share.svg").exists())
            category_svg = (output_dir / "category_share.svg").read_text(encoding="utf-8")
            self.assertIn("<svg", category_svg)
            self.assertIn("Category Distribution", category_svg)
            self.assertIn("Others", category_svg)


if __name__ == "__main__":
    unittest.main()
