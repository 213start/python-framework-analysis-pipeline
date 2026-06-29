"""Tests for UDF_Benchmarking framework support."""

from __future__ import annotations

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


class UdfBenchmarkingEnvironmentTest(unittest.TestCase):
    def test_plan_uses_python311_udf_container_and_py_spy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_udf_project(Path(tmp), python_flamegraph=True)

            result = CliInvoker.run(
                "environment", "plan", str(project_yaml), "--platform", "arm"
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertEqual(plan["framework"], "udfbenchmarking")
        step_ids = [step["id"] for step in plan["steps"]]
        self.assertIn("build-udfbenchmarking-image", step_ids)
        self.assertIn("start-udfbenchmarking", step_ids)
        self.assertIn("readiness-udfbenchmarking", step_ids)
        self.assertIn("verify-udfbenchmarking-perf-tools", step_ids)

        build_step = next(
            s for s in plan["steps"] if s["id"] == "build-udfbenchmarking-image"
        )
        start_step = next(s for s in plan["steps"] if s["id"] == "start-udfbenchmarking")
        verify_step = next(
            s for s in plan["steps"] if s["id"] == "verify-udfbenchmarking-perf-tools"
        )

        self.assertEqual(
            build_step["scriptPath"],
            "adapters/udfbenchmarking/scripts/build-udfbenchmarking-image.sh",
        )
        self.assertIn("BASE_IMAGE=python:3.11-slim", build_step["command"])
        self.assertIn("UDF_BENCHMARKING_REPO=https://gitcode.com/stone31415/UDF_Benchmarking.git", build_step["command"])
        self.assertIn("PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple", build_step["command"])
        self.assertIn("--privileged", start_step["command"])
        self.assertIn("udf-benchmarking-bench", start_step["command"])
        self.assertIn("command -v py-spy", verify_step["command"])
        self.assertNotIn("flink-jm", start_step["command"])
        self.assertNotIn("data-juicer", start_step["command"])

    def test_config_validate_accepts_udfbenchmarking_image_schema(self) -> None:
        from pyframework_pipeline.config import validate_pipeline_config

        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_udf_project(Path(tmp), python_flamegraph=True)

            report = validate_pipeline_config(project_yaml, require_bridge_token=False)

        self.assertEqual(report["status"], "ok", report["issues"])
        self.assertEqual(report["issueCount"], 0)

    def test_build_script_installs_runtime_and_py_spy(self) -> None:
        script = (
            REPO_ROOT
            / "pipelines"
            / "pyframework_pipeline"
            / "adapters"
            / "udfbenchmarking"
            / "scripts"
            / "build-udfbenchmarking-image.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("getdaft", script)
        self.assertIn("opencv-python-headless", script)
        self.assertIn("scikit-image", script)
        self.assertIn("py-spy", script)
        self.assertIn("python -c", script)

    def test_build_script_handles_apt_source_and_perf_package_variants(self) -> None:
        script = (
            REPO_ROOT
            / "pipelines"
            / "pyframework_pipeline"
            / "adapters"
            / "udfbenchmarking"
            / "scripts"
            / "build-udfbenchmarking-image.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("https://deb.debian.org/debian-security", script)
        self.assertIn("security.debian.org/debian-security", script)
        self.assertIn(
            "if ! DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends linux-perf",
            script,
        )
        self.assertIn("linux-tools-common linux-tools-generic", script)
        self.assertIn("find /usr/lib/linux-tools", script)
        self.assertIn('ln -sf "$perf_real" /usr/local/bin/perf', script)
        self.assertIn("command -v perf", script)

    def test_build_plan_forwards_host_proxy_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_udf_project(Path(tmp), proxy_env=True)

            result = CliInvoker.run(
                "environment", "plan", str(project_yaml), "--platform", "arm"
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        build_step = next(
            s for s in plan["steps"] if s["id"] == "build-udfbenchmarking-image"
        )

        self.assertIn("http_proxy=http://proxy.internal:3128", build_step["command"])
        self.assertIn("HTTPS_PROXY=http://secure-proxy.internal:3129", build_step["command"])
        self.assertIn("NO_PROXY=localhost,127.0.0.1,.internal", build_step["command"])

    def test_build_script_forwards_proxy_build_args(self) -> None:
        script = (
            REPO_ROOT
            / "pipelines"
            / "pyframework_pipeline"
            / "adapters"
            / "udfbenchmarking"
            / "scripts"
            / "build-udfbenchmarking-image.sh"
        ).read_text(encoding="utf-8")

        for name in ("http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"):
            self.assertIn(f"ARG {name}", script)
            self.assertIn(f"--build-arg \"{name}=${{{name}:-}}\"", script)

    def test_reference_config_keeps_upstream_parameterized_udf_defaults(self) -> None:
        from pyframework_pipeline.environment.parser import parse_yaml

        config = parse_yaml(
            (
                REPO_ROOT
                / "projects"
                / "udf-benchmarking-reference"
                / "workload"
                / "config.yaml"
            ).read_text(encoding="utf-8")
        )

        self.assertIn("MicroUDF", config)
        self.assertIn("SeriesMethodUDF", config)
        self.assertIn("PartitionScheduleUDF", config)
        self.assertIn("MacroUDF", config)
        self.assertEqual(config["SeriesMethodUDF"]["udf_count"], [1])
        self.assertEqual(config["PartitionScheduleUDF"]["num_partitions"], [1])


class UdfBenchmarkingOrchestratorTest(unittest.TestCase):
    def test_workload_deploy_targets_udfbenchmarking_container(self) -> None:
        from pyframework_pipeline.orchestrator import _run_workload_deploy

        fake = FakeExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_udf_project(Path(tmp))
            run_dir = Path(tmp) / "runs" / "test"

            with patch("pyframework_pipeline.remote.build_executor", return_value=fake):
                _run_workload_deploy(project_yaml, run_dir, "arm")

        self.assertEqual(
            fake.pushed_dirs,
            [(str(project_yaml.parent / "workload"), "/tmp/pyframework-workload")],
        )
        commands = "\n".join(fake.commands)
        self.assertIn("rm -rf /workspace/benchmark", commands)
        self.assertIn("cp -a /opt/UDF_Benchmarking/.", commands)
        self.assertIn("docker cp /tmp/pyframework-workload/. udf-benchmarking-bench:/workspace/benchmark", commands)
        self.assertIn("chown -R root:root /workspace/benchmark", commands)
        self.assertNotIn("flink-jm", commands)
        self.assertNotIn("data-juicer", commands)

    def test_benchmark_converts_upstream_csv_to_timing(self) -> None:
        from pyframework_pipeline.orchestrator import _run_benchmark

        fake = FakeExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_udf_project(Path(tmp))
            run_dir = Path(tmp) / "runs" / "test"

            with patch("pyframework_pipeline.remote.build_executor", return_value=fake):
                _run_benchmark(project_yaml, run_dir, "arm", force=True)

            timing_path = run_dir / "arm" / "timing" / "timing-normalized.json"
            raw_path = run_dir / "arm" / "timing" / "timing-raw.json"
            self.assertTrue(timing_path.exists())
            self.assertTrue(raw_path.exists())
            timing = json.loads(timing_path.read_text(encoding="utf-8"))
            raw = json.loads(raw_path.read_text(encoding="utf-8"))

        commands = "\n".join(fake.commands)
        self.assertIn("udf-benchmarking-bench", commands)
        self.assertIn("python3 main.py", commands)
        self.assertIn("--config-file config.yaml", commands)
        self.assertIn("--output /tmp/pyframework-udfbenchmarking-run-arm", commands)
        self.assertIn("perf record", commands)
        self.assertNotIn("py-spy record", commands)
        self.assertEqual(timing["cases"][0]["caseId"], "MockVideoE2EUDF")
        self.assertEqual(
            timing["cases"][0]["metrics"]["wallClockTime"]["wall_clock_ns"],
            1_500_000_000,
        )
        self.assertEqual(raw["framework"], "udfbenchmarking")
        self.assertEqual(raw["benchmark"], "MockVideoE2EUDF")

    def test_optional_python_flamegraph_runs_when_enabled(self) -> None:
        from pyframework_pipeline.orchestrator import _run_benchmark

        fake = FakeExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            project_yaml = _write_udf_project(Path(tmp), python_flamegraph=True)
            run_dir = Path(tmp) / "runs" / "test"

            with patch("pyframework_pipeline.remote.build_executor", return_value=fake):
                _run_benchmark(project_yaml, run_dir, "arm", force=True)

            flamegraph = (
                run_dir
                / "arm"
                / "python"
                / "flamegraphs"
                / "MockVideoE2EUDF.svg"
            )
            manifest = run_dir / "arm" / "python" / "manifest.json"
            self.assertTrue(flamegraph.exists())
            self.assertTrue(manifest.exists())
            manifest_json = json.loads(manifest.read_text(encoding="utf-8"))

        commands = "\n".join(fake.commands)
        self.assertIn("command -v py-spy", commands)
        self.assertIn("py-spy record", commands)
        self.assertIn("--rate 77", commands)
        self.assertIn("--subprocesses", commands)
        self.assertIn("--format flamegraph", commands)
        self.assertIn("python3 main.py --config-file config.yaml", commands)
        self.assertIn("/tmp/pyframework-udfbenchmarking-python-arm", commands)
        self.assertIn("MockVideoE2EUDF.svg", commands)
        self.assertEqual(manifest_json["framework"], "udfbenchmarking")
        self.assertEqual(manifest_json["cases"][0]["caseId"], "MockVideoE2EUDF")


class FakeExecutor:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.pushed_dirs: list[tuple[str, str]] = []
        self.pushed_files: list[tuple[str, str]] = []

    def run(self, command: str, **_kwargs):
        self.commands.append(command)
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    def push_dir(self, local_dir: Path, remote_dir: str) -> bool:
        self.pushed_dirs.append((str(local_dir), remote_dir))
        return True

    def push_file(self, local_path: Path, remote_path: str) -> bool:
        self.pushed_files.append((str(local_path), remote_path))
        return True

    def fetch_dir(self, _remote_dir: str, local_dir: Path) -> bool:
        if "python" in str(local_dir):
            flamegraphs = local_dir / "flamegraphs"
            flamegraphs.mkdir(parents=True, exist_ok=True)
            (flamegraphs / "MockVideoE2EUDF.svg").write_text(
                "<svg></svg>\n",
                encoding="utf-8",
            )
            return True

        local_dir.mkdir(parents=True, exist_ok=True)
        (local_dir / "MockVideoE2EUDF.csv").write_text(
            "\n".join(
                [
                    "iteration,records_processed,time_seconds,records_per_second,collect_time_seconds,postprocess_time_seconds",
                    "1,2,1.0,2.0,0.8,0.2",
                    "2,2,2.0,1.0,1.6,0.4",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (local_dir / "MockVideoE2EUDF_summary.json").write_text(
            json.dumps(
                {
                    "records_processed": 2,
                    "time_seconds": 1.5,
                    "collect_time_seconds": 1.2,
                    "postprocess_time_seconds": 0.3,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return True

    def fetch_file(self, _remote_path: str, local_path: Path) -> bool:
        local_path.write_bytes(b"perfdata")
        return True


def _write_udf_project(
    project_dir: Path,
    *,
    python_flamegraph: bool = False,
    proxy_env: bool = False,
) -> Path:
    workload_dir = project_dir / "workload"
    workload_dir.mkdir(parents=True)
    (workload_dir / "config.yaml").write_text(
        "\n".join(
            [
                "repeat: 1",
                "warmup: false",
                "num_rows: 1",
                "runner: native",
                "native_num_threads: 2",
                "FilterUDF:",
                "  - MockVideoE2EUDF",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (project_dir / "datasets").mkdir()
    (project_dir / "sources").mkdir()
    (project_dir / "projects").mkdir()

    project_yaml = project_dir / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "id: udf-benchmarking-reference",
                "name: UDF Benchmarking Reference",
                "fourLayerRoot: .",
                "workload:",
                "  localDir: workload",
                "  benchmark: MockVideoE2EUDF",
                "bridge:",
                "  repo: stone31415/UDF_Benchmarking",
                "  platform: gitcode",
                "run:",
                "  platforms:",
                "    - arm",
                "    - x86",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    lines = [
        "schemaVersion: 1",
        "framework: udfbenchmarking",
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
        "  udfBenchmarkingRepo: https://gitcode.com/stone31415/UDF_Benchmarking.git",
        "  udfBenchmarkingRevision: e5f892d",
        "  udfBenchmarkingImages:",
        "    arm: udf-benchmarking-bench:py311-arm",
        "    x86: udf-benchmarking-bench:py311-x86",
        "  udfBenchmarkingContainer: udf-benchmarking-bench",
        "  benchmarkName: MockVideoE2EUDF",
        "  benchmarkConfigFile: config.yaml",
        "  aptMirror: http://mirrors.tuna.tsinghua.edu.cn/debian",
        "  aptSecurityMirror: http://mirrors.tuna.tsinghua.edu.cn/debian-security",
        "  pipIndexUrl: https://pypi.tuna.tsinghua.edu.cn/simple",
        "  pipTrustedHosts:",
        "    - pypi.tuna.tsinghua.edu.cn",
        "  pipTimeout: 180",
        "  pipRetries: 10",
        "  profilingTools:",
        "    - perf",
        "    - objdump",
    ]
    if python_flamegraph:
        lines.extend(
            [
                "    - py-spy",
                "  pythonFlamegraph:",
                "    enabled: true",
                "    rate: 77",
                "    subprocesses: true",
            ]
        )
    lines.extend(
        [
            "hostRefs:",
            "  arm-host:",
            "    connect: ssh",
            "    alias: blue-98",
        ]
    )
    if proxy_env:
        lines.extend(
            [
                "    env:",
                "      http_proxy: http://proxy.internal:3128",
                "      HTTPS_PROXY: http://secure-proxy.internal:3129",
                "      NO_PROXY: localhost,127.0.0.1,.internal",
            ]
        )
    lines.extend(
        [
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
    (project_dir / "environment.yaml").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    return project_yaml
