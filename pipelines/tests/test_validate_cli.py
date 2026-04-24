import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "four-layer" / "pyflink-reference"


class ValidateCliTest(unittest.TestCase):
    def run_cli(
        self,
        *args: str,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = {"PYTHONPATH": str(REPO_ROOT / "pipelines")}
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            [sys.executable, "-m", "pyframework_pipeline", *args],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_validate_accepts_current_four_layer_example(self) -> None:
        result = self.run_cli("validate", str(EXAMPLE_ROOT))

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["projectId"], "tpch-pyflink-reference")
        self.assertEqual(payload["errorCount"], 0)

    def test_validate_reports_missing_artifact_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir) / "pyflink-reference"
            self.copy_tree(EXAMPLE_ROOT, temp_root)
            source_path = temp_root / "sources" / "pyflink-reference-source.source.json"
            source = json.loads(source_path.read_text())
            # Remove an artifact that is still referenced by a function.
            removed_id = "asm_arm64_a8fe4a73"
            source["artifactIndex"] = [
                artifact for artifact in source["artifactIndex"] if artifact["id"] != removed_id
            ]
            source_path.write_text(json.dumps(source, ensure_ascii=False, indent=2))

            result = self.run_cli("validate", str(temp_root))

        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertGreater(payload["errorCount"], 0)
        self.assertIn(removed_id, "\n".join(error["message"] for error in payload["errors"]))

    def test_validate_accepts_project_yaml_config(self) -> None:
        project_config = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"

        result = self.run_cli("validate", str(project_config))

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["projectId"], "tpch-pyflink-reference")

    def test_config_validate_rejects_placeholder_bridge_token(self) -> None:
        project_config = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"

        result = self.run_cli(
            "config", "validate", str(project_config),
            extra_env={"PYFRAMEWORK_BRIDGE_TOKEN": "fake-token"},
        )

        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertIn(
            "placeholder",
            "\n".join(issue["message"] for issue in payload["issues"]),
        )

    def test_config_validate_can_skip_bridge_token_for_pre_bridge_checks(self) -> None:
        project_config = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"

        result = self.run_cli(
            "config", "validate", str(project_config), "--skip-bridge-token",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["projectId"], "pyflink-tpch-reference")
        self.assertEqual(payload["issueCount"], 0)

    def test_config_validate_rejects_missing_environment_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_config = Path(temp_dir) / "project.yaml"
            project_config.write_text(
                "\n".join([
                    "id: missing-env",
                    "fourLayerRoot: ./four-layer",
                    "workload:",
                    "  localDir: workload",
                    "run:",
                    "  platforms:",
                    "    - arm",
                ]),
                encoding="utf-8",
            )

            result = self.run_cli("config", "validate", str(project_config))

        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertIn(
            "environment.yaml",
            "\n".join(issue["message"] for issue in payload["issues"]),
        )

    def test_run_stops_at_config_gate_without_bridge_token(self) -> None:
        project_config = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"

        result = self.run_cli("run", str(project_config), "--force")

        self.assertEqual(result.returncode, 1)
        self.assertNotIn("ImportError", result.stderr)
        self.assertIn("config validation failed", result.stderr)
        self.assertIn("PYFRAMEWORK_BRIDGE_TOKEN", result.stderr)

    def test_run_stop_before_bridge_does_not_require_bridge_token(self) -> None:
        project_config = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"

        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.run_cli(
                "run", str(project_config),
                "--run-dir", temp_dir,
                "--stop-before", "3",
                "--force",
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("PYFRAMEWORK_BRIDGE_TOKEN", result.stderr)

    def test_run_can_stop_before_remote_steps_after_config_gate(self) -> None:
        project_config = REPO_ROOT / "projects" / "pyflink-tpch-reference" / "project.yaml"

        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.run_cli(
                "run", str(project_config),
                "--run-dir", temp_dir,
                "--stop-before", "3",
                "--force",
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("ImportError", result.stderr)

    def copy_tree(self, source: Path, destination: Path) -> None:
        for item in source.rglob("*"):
            target = destination / item.relative_to(source)
            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(item.read_bytes())


if __name__ == "__main__":
    unittest.main()
