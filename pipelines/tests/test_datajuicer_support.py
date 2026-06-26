"""Tests for Data-Juicer framework support."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "pipelines"))


class CliInvoker:
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


class DataJuicerEnvironmentTest(unittest.TestCase):
    def test_plan_uses_cpu_text_only_benchmark_and_mirrors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_datajuicer_project(Path(tmp))

            result = CliInvoker.run(
                "environment", "plan", str(project_yaml), "--platform", "arm"
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertEqual(plan["framework"], "datajuicer")
        step_ids = [step["id"] for step in plan["steps"]]
        self.assertIn("build-datajuicer-image", step_ids)
        self.assertIn("start-datajuicer", step_ids)
        self.assertIn("readiness-datajuicer", step_ids)

        build_step = next(
            s for s in plan["steps"] if s["id"] == "build-datajuicer-image"
        )
        start_step = next(s for s in plan["steps"] if s["id"] == "start-datajuicer")

        self.assertEqual(
            build_step["scriptPath"],
            "adapters/datajuicer/scripts/build-datajuicer-image.sh",
        )
        self.assertIn(
            "PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple",
            build_step["command"],
        )
        self.assertIn(
            "APT_MIRROR=http://mirrors.tuna.tsinghua.edu.cn/debian",
            build_step["command"],
        )
        self.assertIn("HF_ENDPOINT=https://hf-mirror.com", build_step["command"])
        self.assertIn("DATA_JUICER_BENCH_MODALITIES=text", start_step["command"])
        self.assertIn("DATA_JUICER_BENCH_USE_FULL_ARCHIVE=false", start_step["command"])
        self.assertIn("HF_ENDPOINT=https://hf-mirror.com", start_step["command"])
        self.assertIn("--privileged", start_step["command"])
        self.assertIn("data-juicer-bench", start_step["command"])
        self.assertNotIn("--gpus", start_step["command"])
        self.assertNotIn(" image ", start_step["command"])
        self.assertNotIn(" video ", start_step["command"])
        self.assertNotIn(" audio ", start_step["command"])

    def test_config_validate_accepts_datajuicer_image_schema(self) -> None:
        from pyframework_pipeline.config import validate_pipeline_config

        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_datajuicer_project(Path(tmp))

            report = validate_pipeline_config(project_yaml, require_bridge_token=False)

        self.assertEqual(report["status"], "ok", report["issues"])
        self.assertEqual(report["issueCount"], 0)


class DataJuicerBenchmarkRunnerTest(unittest.TestCase):
    def test_cpu_runtime_patch_skips_torch_auto_install_only(self) -> None:
        runner = _load_datajuicer_runner()

        with tempfile.TemporaryDirectory() as tmp:
            patch_path = runner.write_cpu_only_runtime_patch(Path(tmp))
            patch_text = patch_path.read_text(encoding="utf-8")

        self.assertIn("LazyLoader.check_packages", patch_text)
        self.assertIn("LazyLoader._load", patch_text)
        self.assertIn("_package_base(spec) != \"torch\"", patch_text)
        self.assertIn("class _TorchStub", patch_text)
        self.assertIn("empty_cache", patch_text)
        self.assertIn("CPU-only benchmark", patch_text)

    def test_text_only_data_prep_generates_jsonl_without_archive(self) -> None:
        runner = _load_datajuicer_runner()

        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp)
            data_dir = runner.ensure_benchmark_data(
                work_dir,
                ["http://example.invalid/perf_bench_data.tar.gz"],
                timeout=1,
                modalities=["text"],
                use_full_archive=False,
                text_rows=8,
            )

            text_file = data_dir / "text" / "wiki-10k.jsonl"
            rows = [
                json.loads(line)
                for line in text_file.read_text(encoding="utf-8").splitlines()
            ]
            archive_exists = (work_dir / "perf_bench_data.tar.gz").exists()

            self.assertEqual(data_dir, work_dir / "perf_bench_data")
            self.assertTrue(text_file.exists())
            self.assertFalse(archive_exists)
            self.assertEqual(len(rows), 8)
            self.assertEqual(rows[0]["id"], "wiki-00000")
            self.assertIn("text-only record", rows[0]["text"])
            self.assertGreater(rows[0]["__dj__stats__"]["num_token"], 0)


class DataJuicerOrchestratorTest(unittest.TestCase):
    def test_workload_deploy_targets_datajuicer_container(self) -> None:
        from pyframework_pipeline.orchestrator import _run_workload_deploy

        fake = FakeExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_datajuicer_project(Path(tmp))
            run_dir = Path(tmp) / "runs" / "test"

            with patch("pyframework_pipeline.remote.build_executor", return_value=fake):
                _run_workload_deploy(project_yaml, run_dir, "arm")

        self.assertEqual(
            fake.pushed_dirs,
            [(str(project_yaml.parent / "workload"), "/tmp/pyframework-workload")],
        )
        commands = "\n".join(fake.commands)
        self.assertIn("rm -rf /workspace/benchmark", commands)
        self.assertIn("mkdir -p /workspace/benchmark", commands)
        self.assertIn("docker cp /tmp/pyframework-workload/. data-juicer-bench:/workspace/benchmark", commands)
        self.assertIn("chown -R root:root /workspace/benchmark", commands)
        self.assertNotIn("flink-jm", commands)
        self.assertNotIn("flink-tm", commands)

    def test_benchmark_runs_datajuicer_text_only_and_fetches_timing(self) -> None:
        from pyframework_pipeline.orchestrator import _run_benchmark

        fake = FakeExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_datajuicer_project(Path(tmp))
            run_dir = Path(tmp) / "runs" / "test"

            with patch("pyframework_pipeline.remote.build_executor", return_value=fake):
                _run_benchmark(project_yaml, run_dir, "arm", force=True)

            timing_path = run_dir / "arm" / "timing" / "timing-normalized.json"
            self.assertTrue(timing_path.exists())
            timing = json.loads(timing_path.read_text(encoding="utf-8"))

        commands = "\n".join(fake.commands)
        self.assertIn("data-juicer-bench", commands)
        self.assertIn("benchmark_runner.py", commands)
        self.assertIn("--modalities text", commands)
        self.assertIn("--platform arm", commands)
        self.assertIn("perf record", commands)
        self.assertNotIn("--modalities image", commands)
        self.assertNotIn("--modalities video", commands)
        self.assertNotIn("--modalities audio", commands)
        self.assertEqual(timing["cases"][0]["caseId"], "data-juicer-text")

    def test_collect_binary_uses_tmp_staging_path(self) -> None:
        from pyframework_pipeline.orchestrator import _collect_binary_from_container

        fake = FakeExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            ok = _collect_binary_from_container(
                fake,
                "data-juicer-bench",
                "/tmp/perf-udf.data",
                Path(tmp) / "perf-arm.data",
            )

        commands = "\n".join(fake.commands)
        self.assertTrue(ok)
        self.assertIn("/tmp/_collect_perf-arm.data", commands)
        self.assertNotIn("/opt/flink/_collect", commands)


class FakeExecutor:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.pushed_dirs: list[tuple[str, str]] = []

    def run(self, command: str, **_kwargs):
        self.commands.append(command)
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    def push_dir(self, local_dir: Path, remote_dir: str) -> bool:
        self.pushed_dirs.append((str(local_dir), remote_dir))
        return True

    def fetch_dir(self, _remote_dir: str, local_dir: Path) -> bool:
        timing_dir = local_dir / "timing"
        timing_dir.mkdir(parents=True, exist_ok=True)
        (timing_dir / "timing-normalized.json").write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "platform": "arm",
                    "cases": [
                        {
                            "caseId": "data-juicer-text",
                            "metrics": {
                                "wallClockTime": {"wall_clock_ns": 1_000_000_000},
                                "tmE2eTime": {"wall_clock_ns": 1_000_000_000},
                                "frameworkCallTime": {"total_ns": 100_000_000},
                                "businessOperatorTime": {"total_ns": 900_000_000},
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return True

    def fetch_file(self, _remote_path: str, _local_path: Path) -> bool:
        return True


def _write_datajuicer_project(project_dir: Path) -> Path:
    workload_dir = project_dir / "workload"
    workload_dir.mkdir(parents=True)
    (workload_dir / "benchmark_runner.py").write_text(
        "print('runner')\n",
        encoding="utf-8",
    )
    (project_dir / "datasets").mkdir()
    (project_dir / "sources").mkdir()
    (project_dir / "projects").mkdir()

    project_yaml = project_dir / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "id: data-juicer-benchmark-reference",
                "name: Data-Juicer Benchmark Reference",
                "fourLayerRoot: .",
                "workload:",
                "  localDir: workload",
                "  benchmark: upstream-performance",
                "  modalities:",
                "    - text",
                "bridge:",
                "  repo: datajuicer/data-juicer",
                "  platform: github",
                "run:",
                "  platforms:",
                "    - arm",
                "    - x86",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (project_dir / "environment.yaml").write_text(
        "\n".join(
            [
                "schemaVersion: 1",
                "framework: datajuicer",
                "mode: plan-only",
                "platforms:",
                "  - id: arm",
                "    arch: aarch64",
                "    hosts:",
                "      - role: client",
                "        hostRef: arm-host",
                "  - id: x86",
                "    arch: x86_64",
                "    hosts:",
                "      - role: client",
                "        hostRef: x86-host",
                "software:",
                "  pythonImage: python:3.11-slim",
                "  dataJuicerVersion: 1.5.2",
                "  dataJuicerImages:",
                "    arm: data-juicer-bench:1.5.2-py311-arm",
                "    x86: data-juicer-bench:1.5.2-py311-x86",
                "  dataJuicerContainer: data-juicer-bench",
                "  benchmarkDataUrl: http://dail-wlcb.oss-cn-wulanchabu.aliyuncs.com/data_juicer/perf_bench_data/perf_bench_data.tar.gz",
                "  benchmarkUseFullArchive: false",
                "  benchmarkModalities:",
                "    - text",
                "  aptMirror: http://mirrors.tuna.tsinghua.edu.cn/debian",
                "  aptSecurityMirror: http://mirrors.tuna.tsinghua.edu.cn/debian-security",
                "  hfEndpoint: https://hf-mirror.com",
                "  pipIndexUrl: https://pypi.tuna.tsinghua.edu.cn/simple",
                "  pipTrustedHosts:",
                "    - pypi.tuna.tsinghua.edu.cn",
                "  pipTimeout: 180",
                "  pipRetries: 10",
                "  profilingTools:",
                "    - perf",
                "    - objdump",
                "hostRefs:",
                "  arm-host:",
                "    connect: ssh",
                "    alias: blue-98",
                "    capabilities:",
                "      ssh: true",
                "      docker: true",
                "  x86-host:",
                "    connect: ssh",
                "    alias: zen5",
                "    capabilities:",
                "      ssh: true",
                "      docker: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    return project_yaml


def _load_datajuicer_runner():
    runner_path = (
        REPO_ROOT / "workload" / "data-juicer" / "benchmark" / "benchmark_runner.py"
    )
    spec = importlib.util.spec_from_file_location(
        "datajuicer_benchmark_runner",
        runner_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {runner_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
