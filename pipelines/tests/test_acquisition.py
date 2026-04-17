"""Tests for data acquisition module."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_YAML = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"


class CliInvoker:
    """Helper to run the pipeline CLI as a subprocess."""

    @staticmethod
    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "pyframework_pipeline", *args],
            cwd=REPO_ROOT,
            env={"PYTHONPATH": str(REPO_ROOT / "pipelines")},
            text=True,
            capture_output=True,
            check=False,
        )


class TimingCollectorTest(unittest.TestCase):
    """Test timing data collection from TM stdout logs."""

    SAMPLE_STDOUT = """\
[PreUDF Start]: Start Time(ns)=1000000
[Benchmark Counter]: Count=1000000, Avg Overhead=95000.0 ns, Last Overhead=94000 ns
[PostUDF End]: End Time(ns)=5000000000
[BENCHMARK_SUMMARY] {"recordCount": 1000000, "avgFrameworkOverheadNs": 95811.0, "avgPyDurationNs": 324.5, "totalFrameworkOverheadNs": 95811980, "totalPyDurationNs": 324500}
"""

    def test_parse_benchmark_summary(self) -> None:
        from pipelines.pyframework_pipeline.acquisition.timing import _parse_summaries

        with tempfile.TemporaryDirectory() as tmp:
            log_file = Path(tmp) / "tm-stdout.log"
            log_file.write_text(self.SAMPLE_STDOUT, encoding="utf-8")

            summaries = _parse_summaries(log_file)
            self.assertEqual(len(summaries), 1)
            self.assertEqual(summaries[0]["recordCount"], 1000000)
            self.assertAlmostEqual(summaries[0]["avgFrameworkOverheadNs"], 95811.0)

    def test_parse_multiple_summaries(self) -> None:
        from pipelines.pyframework_pipeline.acquisition.timing import _parse_summaries

        with tempfile.TemporaryDirectory() as tmp:
            log_file = Path(tmp) / "tm-stdout.log"
            log_file.write_text(self.SAMPLE_STDOUT + self.SAMPLE_STDOUT, encoding="utf-8")

            summaries = _parse_summaries(log_file)
            self.assertEqual(len(summaries), 2)

    def test_collect_timing_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "2026-04-16-arm"
            run_dir.mkdir()

            # Create a sample stdout file
            stdout_file = run_dir / "tm-stdout.log"
            stdout_file.write_text(self.SAMPLE_STDOUT, encoding="utf-8")

            result = CliInvoker.run(
                "acquire", "timing", str(PROJECT_YAML),
                "--platform", "arm",
                "--run-dir", str(run_dir),
                "--stdout-file", str(stdout_file),
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            # Check timing output
            normalized = json.loads((run_dir / "timing" / "timing-normalized.json").read_text())
            self.assertIn("cases", normalized)
            self.assertEqual(len(normalized["cases"]), 1)
            self.assertEqual(normalized["cases"][0]["recordCount"], 1000000)
            self.assertIn("frameworkCallTime", normalized["cases"][0]["metrics"])
            self.assertIn("businessOperatorTime", normalized["cases"][0]["metrics"])

            # Check manifest
            manifest = json.loads((run_dir / "acquisition-manifest.json").read_text())
            self.assertEqual(manifest["timing"]["status"], "collected")


class PerfCollectorTest(unittest.TestCase):
    """Test perf profile collection (skip when no perf.data)."""

    def test_collect_perf_skips_missing_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            result = CliInvoker.run(
                "acquire", "perf", str(PROJECT_YAML),
                "--platform", "arm",
                "--run-dir", str(run_dir),
            )
            # Should succeed (skip, not fail)
            self.assertEqual(result.returncode, 0, result.stderr)
            output = json.loads(result.stdout)
            self.assertEqual(output["status"], "skipped")


class AsmCollectorTest(unittest.TestCase):
    """Test machine code collection."""

    def test_collect_asm_skips_no_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            result = CliInvoker.run(
                "acquire", "asm", str(PROJECT_YAML),
                "--platform", "arm",
                "--run-dir", str(run_dir),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            output = json.loads(result.stdout)
            self.assertEqual(output["status"], "skipped")


class AcquisitionValidateTest(unittest.TestCase):
    """Test acquire validate command."""

    def test_validate_rejects_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = CliInvoker.run("acquire", "validate", tmp)
            self.assertEqual(result.returncode, 1)
            output = json.loads(result.stdout)
            self.assertEqual(output["status"], "error")

    def test_validate_accepts_valid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            manifest = {
                "schemaVersion": 1,
                "projectId": "test",
                "platform": "arm",
                "runDir": str(run_dir),
                "timing": {"status": "pending"},
                "perf": {"status": "pending"},
                "asm": {"status": "pending"},
            }
            (run_dir / "acquisition-manifest.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )

            result = CliInvoker.run("acquire", "validate", str(run_dir))
            self.assertEqual(result.returncode, 0, result.stderr)
            output = json.loads(result.stdout)
            self.assertEqual(output["status"], "ok")


class ManifestModelTest(unittest.TestCase):
    """Test AcquisitionManifest data model."""

    def test_roundtrip(self) -> None:
        from pipelines.pyframework_pipeline.acquisition.manifest import (
            AcquisitionManifest,
            AcquisitionSection,
            load_manifest,
        )

        with tempfile.TemporaryDirectory() as tmp:
            m = AcquisitionManifest(
                projectId="test-project",
                platform="arm",
                runDir="runs/test",
                timing=AcquisitionSection(
                    status="collected",
                    files={"raw": "timing/timing-raw.json"},
                ),
            )
            path = Path(tmp) / "acquisition-manifest.json"
            m.write(path)

            loaded = load_manifest(path)
            self.assertEqual(loaded.projectId, "test-project")
            self.assertEqual(loaded.timing.status, "collected")
            self.assertEqual(loaded.perf.status, "pending")
            self.assertEqual(loaded.timing.files["raw"], "timing/timing-raw.json")


class SshExecutorTest(unittest.TestCase):
    """Test SSH executor construction."""

    def test_from_string(self) -> None:
        from pipelines.pyframework_pipeline.acquisition.ssh_executor import SshExecutor

        ex = SshExecutor.from_string("root@192.168.1.1")
        self.assertEqual(ex.user, "root")
        self.assertEqual(ex.host, "192.168.1.1")

        ex2 = SshExecutor.from_string("myhost")
        self.assertEqual(ex2.user, "")
        self.assertEqual(ex2.host, "myhost")
