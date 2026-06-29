from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from pyframework_pipeline.analyze.run_single_platform_pipeline import (
    build_output_paths,
    default_benchmark,
    default_platform_id,
)


class PerfRunnerTests(unittest.TestCase):
    def test_default_benchmark(self) -> None:
        self.assertEqual(default_benchmark(Path("/tmp/richards.perf.data")), "richards.perf")
        self.assertEqual(default_benchmark(Path("/tmp/perf.data")), "perf_run")

    def test_default_platform_id(self) -> None:
        self.assertTrue(default_platform_id().endswith("-local"))

    def test_build_output_paths(self) -> None:
        paths = build_output_paths(Path("/tmp/demo"))
        self.assertEqual(paths["records_csv"], Path("/tmp/demo/data/perf_records.csv"))
        self.assertEqual(paths["platform_report"], Path("/tmp/demo/reports/platform_report.md"))
        self.assertEqual(paths["platform_report_full"], Path("/tmp/demo/reports/platform_report_full.md"))
        self.assertEqual(paths["platform_machine_code_report"], Path("/tmp/demo/reports/platform_machine_code_report.md"))


if __name__ == "__main__":
    unittest.main()
