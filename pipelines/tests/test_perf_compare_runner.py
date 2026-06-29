from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.run_compare_pipeline import (
    build_output_paths,
    compare_command,
    infer_platform_root_from_records,
    use_root_inputs,
    use_records_inputs,
    validate_args,
)
from pyframework_pipeline.analyze.compare_platform_perf import build_shared_object_compare


class PerfCompareRunnerTests(unittest.TestCase):
    def test_build_output_paths(self) -> None:
        paths = build_output_paths(Path("/tmp/compare-demo"))
        self.assertEqual(paths["baseline_root"], Path("/tmp/compare-demo/baseline"))
        self.assertEqual(paths["target_root"], Path("/tmp/compare-demo/target"))
        self.assertEqual(paths["compare_tables"], Path("/tmp/compare-demo/tables"))
        self.assertEqual(paths["compare_report"], Path("/tmp/compare-demo/reports/compare_report.md"))
        self.assertEqual(paths["compare_report_html"], Path("/tmp/compare-demo/reports/compare_report.html"))

    def test_validate_args_accepts_raw_inputs(self) -> None:
        args = argparse.Namespace(
            baseline_root=None,
            target_root=None,
            baseline_input=Path("/tmp/amd.perf.data"),
            target_input=Path("/tmp/arm.perf.data"),
            baseline_records=None,
            target_records=None,
        )
        validate_args(args)
        self.assertFalse(use_records_inputs(args))
        self.assertFalse(use_root_inputs(args))

    def test_validate_args_accepts_records_inputs(self) -> None:
        args = argparse.Namespace(
            baseline_root=None,
            target_root=None,
            baseline_input=None,
            target_input=None,
            baseline_records=Path("/tmp/amd.csv"),
            target_records=Path("/tmp/arm.csv"),
        )
        validate_args(args)
        self.assertTrue(use_records_inputs(args))

    def test_validate_args_accepts_root_inputs(self) -> None:
        args = argparse.Namespace(
            baseline_root=Path("/tmp/amd-root"),
            target_root=Path("/tmp/arm-root"),
            baseline_input=None,
            target_input=None,
            baseline_records=None,
            target_records=None,
        )
        validate_args(args)
        self.assertTrue(use_root_inputs(args))

    def test_validate_args_rejects_mixed_modes(self) -> None:
        args = argparse.Namespace(
            baseline_root=None,
            target_root=None,
            baseline_input=Path("/tmp/amd.perf.data"),
            target_input=Path("/tmp/arm.perf.data"),
            baseline_records=Path("/tmp/amd.csv"),
            target_records=Path("/tmp/arm.csv"),
        )
        with self.assertRaises(ValueError):
            validate_args(args)

    def test_compare_command_uses_records_csvs(self) -> None:
        command = compare_command(
            baseline=Path("/tmp/amd/perf_records.csv"),
            target=Path("/tmp/arm/perf_records.csv"),
            baseline_platform_id="amd-baseline",
            target_platform_id="arm-target",
            baseline_e2e_time=10.0,
            target_e2e_time=8.0,
            output_dir=Path("/tmp/out"),
            log_level="INFO",
        )
        self.assertIn("/tmp/amd/perf_records.csv", command)
        self.assertIn("/tmp/arm/perf_records.csv", command)
        self.assertIn("/tmp/out", command)

    def test_infer_platform_root_from_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "tables").mkdir(parents=True)
            self.assertEqual(
                infer_platform_root_from_records(root / "data" / "perf_records.csv"),
                root,
            )

    def test_build_shared_object_compare_normalizes_architecture_tokens(self) -> None:
        baseline_rows = [
            {
                "platform_id": "amd",
                "benchmark": "richards",
                "category_top": "Library",
                "shared_object": "/usr/lib/python3.12/lib-dynload/_json.cpython-312-x86_64-linux-gnu.so",
                "symbol": "encoder_listencode_obj",
                "children": "0",
                "self": "12",
                "period": "100",
                "sample_count": "1",
            }
        ]
        target_rows = [
            {
                "platform_id": "arm",
                "benchmark": "richards",
                "category_top": "Library",
                "shared_object": "/usr/lib/python3.12/lib-dynload/_json.cpython-312-aarch64-linux-gnu.so",
                "symbol": "encoder_listencode_obj",
                "children": "0",
                "self": "9",
                "period": "100",
                "sample_count": "1",
            }
        ]
        rows = build_shared_object_compare(
            baseline_rows,
            target_rows,
            baseline_platform="amd",
            target_platform="arm",
            baseline_e2e_time=10.0,
            target_e2e_time=8.0,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["shared_object"],
            "/usr/lib/python3.12/lib-dynload/_json.cpython-312-x86_64-linux-gnu.so",
        )
        self.assertEqual(rows[0]["delta_time"], "0.48")


if __name__ == "__main__":
    unittest.main()
