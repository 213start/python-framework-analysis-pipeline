"""Tests for environment plan generation and record validation."""

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


class EnvironmentPlanTest(unittest.TestCase):
    """Test 'environment plan' subcommand."""

    def test_plan_generates_arm_plan(self) -> None:
        result = CliInvoker.run(
            "environment", "plan", str(PROJECT_YAML), "--platform", "arm"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)

        self.assertEqual(plan["projectId"], "pyflink-tpch-reference")
        self.assertEqual(plan["framework"], "pyflink")
        self.assertEqual(plan["platform"], "arm")
        self.assertEqual(plan["mode"], "plan-only")
        self.assertTrue(plan["planHash"].startswith("sha256:"))
        self.assertGreater(len(plan["steps"]), 0)

    def test_plan_generates_x86_plan(self) -> None:
        result = CliInvoker.run(
            "environment", "plan", str(PROJECT_YAML), "--platform", "x86"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertEqual(plan["platform"], "x86")

    def test_plan_rejects_unknown_platform(self) -> None:
        result = CliInvoker.run(
            "environment", "plan", str(PROJECT_YAML), "--platform", "riscv"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("not found", result.stderr)

    def test_plan_steps_have_required_fields(self) -> None:
        result = CliInvoker.run(
            "environment", "plan", str(PROJECT_YAML), "--platform", "arm"
        )
        plan = json.loads(result.stdout)

        required_fields = [
            "id", "kind", "hostRef", "command",
            "required", "mutatesHost", "requiresPrivilege",
            "requiresApproval", "rollbackHint",
        ]
        for step in plan["steps"]:
            for field in required_fields:
                self.assertIn(field, step, f"Step {step.get('id', '?')} missing {field}")

    def test_plan_contains_framework_steps(self) -> None:
        result = CliInvoker.run(
            "environment", "plan", str(PROJECT_YAML), "--platform", "arm"
        )
        plan = json.loads(result.stdout)
        step_ids = [s["id"] for s in plan["steps"]]

        self.assertIn("install-pyflink", step_ids)
        self.assertIn("readiness-pyflink-import", step_ids)
        self.assertIn("readiness-mini-job", step_ids)

    def test_plan_writes_to_output_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = CliInvoker.run(
                "environment", "plan", str(PROJECT_YAML),
                "--platform", "arm", "--output", tmp,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            plan_path = Path(tmp) / "environment-plan.json"
            self.assertTrue(plan_path.exists())
            plan = json.loads(plan_path.read_text())
            self.assertEqual(plan["platform"], "arm")


class EnvironmentValidateTest(unittest.TestCase):
    """Test 'environment validate' subcommand."""

    def test_validate_rejects_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = CliInvoker.run("environment", "validate", tmp)

        self.assertEqual(result.returncode, 1)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "error")
        self.assertGreater(report["issueCount"], 0)

    def test_validate_accepts_valid_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)

            # Generate a plan
            CliInvoker.run(
                "environment", "plan", str(PROJECT_YAML),
                "--platform", "arm", "--output", str(tmp_dir),
            )
            plan = json.loads((tmp_dir / "environment-plan.json").read_text())

            # Write a matching record
            record = {
                "schemaVersion": 1,
                "projectId": plan["projectId"],
                "platform": plan["platform"],
                "planHash": plan["planHash"],
                "startedAt": "2026-04-15T10:00:00Z",
                "finishedAt": "2026-04-15T10:12:00Z",
                "mode": "manual-record",
                "provenance": {
                    "recordedBy": "manual",
                    "operatorRef": "test-operator",
                    "source": "test",
                },
                "facts": {"arch": "aarch64", "python": "3.11.6"},
                "steps": [
                    {"id": s["id"], "status": "passed"}
                    for s in plan["steps"]
                    if not s["mutatesHost"]
                ] + [
                    {"id": s["id"], "status": "passed", "logPath": f"logs/{s['id']}.log"}
                    for s in plan["steps"]
                    if s["mutatesHost"]
                ],
            }
            (tmp_dir / "environment-record.json").write_text(
                json.dumps(record, indent=2)
            )

            # Write a readiness report
            readiness = {
                "schemaVersion": 1,
                "projectId": plan["projectId"],
                "platform": plan["platform"],
                "status": "ready",
                "checks": [
                    {"id": "pyflink-import", "status": "passed", "message": "OK"},
                    {"id": "mini-job", "status": "passed", "message": "OK"},
                ],
            }
            (tmp_dir / "readiness-report.json").write_text(
                json.dumps(readiness, indent=2)
            )

            result = CliInvoker.run("environment", "validate", str(tmp_dir))

        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["issueCount"], 0)

    def test_validate_detects_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)

            CliInvoker.run(
                "environment", "plan", str(PROJECT_YAML),
                "--platform", "arm", "--output", str(tmp_dir),
            )

            record = {
                "schemaVersion": 1,
                "projectId": "pyflink-tpch-reference",
                "platform": "arm",
                "planHash": "sha256:WRONG",
                "startedAt": "2026-04-15T10:00:00Z",
                "finishedAt": "2026-04-15T10:12:00Z",
                "mode": "manual-record",
                "provenance": {"recordedBy": "manual"},
                "steps": [],
            }
            (tmp_dir / "environment-record.json").write_text(
                json.dumps(record, indent=2)
            )

            result = CliInvoker.run("environment", "validate", str(tmp_dir))

        self.assertEqual(result.returncode, 1)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "error")
        messages = " ".join(i["message"] for i in report["issues"])
        self.assertIn("does not match", messages)

    def test_validate_detects_unknown_step_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)

            CliInvoker.run(
                "environment", "plan", str(PROJECT_YAML),
                "--platform", "arm", "--output", str(tmp_dir),
            )
            plan = json.loads((tmp_dir / "environment-plan.json").read_text())

            record = {
                "schemaVersion": 1,
                "projectId": plan["projectId"],
                "platform": plan["platform"],
                "planHash": plan["planHash"],
                "startedAt": "2026-04-15T10:00:00Z",
                "finishedAt": "2026-04-15T10:12:00Z",
                "mode": "manual-record",
                "provenance": {"recordedBy": "manual"},
                "steps": [
                    {"id": "nonexistent-step", "status": "passed"},
                ],
            }
            (tmp_dir / "environment-record.json").write_text(
                json.dumps(record, indent=2)
            )

            result = CliInvoker.run("environment", "validate", str(tmp_dir))

        self.assertEqual(result.returncode, 1)
        report = json.loads(result.stdout)
        messages = " ".join(i["message"] for i in report["issues"])
        self.assertIn("not found in plan", messages)


class YamlParserTest(unittest.TestCase):
    """Test the environment YAML parser."""

    def test_parses_full_environment_yaml(self) -> None:
        from pyframework_pipeline.environment.parser import load_environment_yaml

        env = load_environment_yaml(
            REPO_ROOT / "projects" / "pyflink-tpch-reference" / "environment.yaml"
        )

        self.assertEqual(env["schemaVersion"], 1)
        self.assertEqual(env["framework"], "pyflink")
        self.assertEqual(env["mode"], "plan-only")
        self.assertEqual(len(env["platforms"]), 2)
        self.assertEqual(env["platforms"][0]["id"], "arm")
        self.assertEqual(env["platforms"][0]["arch"], "aarch64")
        self.assertEqual(len(env["platforms"][0]["hosts"]), 3)
        self.assertEqual(env["software"]["pythonVersion"], "3.11")
        self.assertTrue(env["software"]["dockerRequired"])
        self.assertIn("arm-client", env["hostRefs"])

    def test_parses_capabilities(self) -> None:
        from pyframework_pipeline.environment.parser import load_environment_yaml

        env = load_environment_yaml(
            REPO_ROOT / "projects" / "pyflink-tpch-reference" / "environment.yaml"
        )

        caps = env["hostRefs"]["arm-client"]["capabilities"]
        self.assertTrue(caps["ssh"])
        self.assertFalse(caps["sudo"])
        self.assertTrue(caps["docker"])


if __name__ == "__main__":
    unittest.main()
