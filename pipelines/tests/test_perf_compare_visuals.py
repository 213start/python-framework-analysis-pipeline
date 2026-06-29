from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.perf_analysis_common import write_csv_rows
from pyframework_pipeline.analyze.render_compare_visuals import render_visuals


class PerfCompareVisualTests(unittest.TestCase):
    def test_render_compare_visuals(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_csv_rows(
                root / "category_compare.csv",
                ["benchmark", "category_top", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "40", "target_share": "30", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "4", "target_est_time": "2.4", "delta_time": "1.6", "delta_share": "10"}],
            )
            write_csv_rows(
                root / "shared_object_compare.csv",
                ["benchmark", "category_top", "shared_object", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "40", "target_share": "30", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "4", "target_est_time": "2.4", "delta_time": "1.6", "delta_share": "10"}],
            )
            write_csv_rows(
                root / "symbol_compare.csv",
                ["benchmark", "category_top", "shared_object", "symbol", "baseline_rank", "baseline_platform", "target_platform", "baseline_share", "target_share", "baseline_e2e_time", "target_e2e_time", "baseline_est_time", "target_est_time", "delta_time", "delta_share"],
                [{"benchmark": "richards", "category_top": "CPython.Interpreter", "shared_object": "/usr/bin/python3.12", "symbol": "_PyEval_EvalFrameDefault", "baseline_rank": "1", "baseline_platform": "amd", "target_platform": "arm", "baseline_share": "40", "target_share": "30", "baseline_e2e_time": "10", "target_e2e_time": "8", "baseline_est_time": "4", "target_est_time": "2.4", "delta_time": "1.6", "delta_share": "10"}],
            )

            output_dir = root / "visuals"
            files = render_visuals(root, output_dir, 10)
            self.assertEqual(len(files), 3)
            self.assertTrue((output_dir / "category_compare.svg").exists())
            category_svg = (output_dir / "category_compare.svg").read_text(encoding="utf-8")
            symbol_svg = (output_dir / "symbol_delta.svg").read_text(encoding="utf-8")
            self.assertIn("<svg", category_svg)
            self.assertIn("Category Delta", category_svg)
            self.assertIn("Hot Symbol Delta", symbol_svg)
            self.assertIn("Right = arm faster", category_svg)
            self.assertIn("s", category_svg)


if __name__ == "__main__":
    unittest.main()
