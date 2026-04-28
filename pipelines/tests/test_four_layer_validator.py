"""Tests for four_layer validator: partial data, missing layers, config gate errors."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.validators.four_layer import validate_four_layer_project

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "four-layer" / "pyflink-reference"


class TestValidatePartialData(unittest.TestCase):
    """Backfill may produce dataset + source but not framework or project refs."""

    def _make_four_layer_root(self, tmp: Path, *, include_framework: bool = False) -> Path:
        """Build a minimal four-layer directory with dataset + source only.

        This simulates what backfill produces before all four layers are complete.
        """
        root = tmp / "four-layer"
        root.mkdir()

        # Dataset with all schema-required fields.
        ds_dir = root / "datasets"
        ds_dir.mkdir()
        ds_data = {
            "id": "tpch",
            "frameworkId": "tpch",
            "cases": [],
            "functions": [],
            "patterns": [],
            "rootCauses": [],
            "stackOverview": {"categories": [], "components": []},
        }
        (ds_dir / "tpch.dataset.json").write_text(
            json.dumps(ds_data, ensure_ascii=False), encoding="utf-8",
        )

        # Source with all schema-required fields.
        src_dir = root / "sources"
        src_dir.mkdir()
        src_data = {
            "id": "tpch",
            "frameworkId": "tpch",
            "repo": {"url": "https://github.com/test/repo"},
            "sourceFiles": [],
            "sourceAnchors": [],
            "artifactIndex": [],
        }
        (src_dir / "tpch.source.json").write_text(
            json.dumps(src_data, ensure_ascii=False), encoding="utf-8",
        )

        # Project JSON: has datasetRef + sourceRef but no frameworkRef
        # (framework layer not yet populated).
        proj_dir = root / "projects"
        proj_dir.mkdir()
        proj_data: dict = {
            "id": "tpch-pyflink-reference",
            "name": "TPC-H PyFlink Reference",
            "datasetRef": "tpch",
            "sourceRef": "tpch",
        }
        if include_framework:
            proj_data["frameworkRef"] = "tpch"
            fw_dir = root / "frameworks"
            fw_dir.mkdir()
            fw_data = {
                "id": "tpch",
                "name": "PyFlink",
                "analysisScope": [],
                "metricDefinitions": [],
                "taxonomy": {"components": [], "categoriesL1": []},
            }
            (fw_dir / "tpch.framework.json").write_text(
                json.dumps(fw_data, ensure_ascii=False), encoding="utf-8",
            )
        (proj_dir / "tpch.project.json").write_text(
            json.dumps(proj_data, ensure_ascii=False), encoding="utf-8",
        )

        return root

    def test_ok_when_no_four_layer_data_exists(self) -> None:
        """Directories exist but are empty — data not generated yet."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "empty-project"
            root.mkdir()
            (root / "datasets").mkdir()
            (root / "sources").mkdir()

            report = validate_four_layer_project(root)
            self.assertEqual(report.status, "ok")
            self.assertEqual(len(report.errors), 0)

    def test_ok_when_no_json_files_exist(self) -> None:
        """Directories don't exist at all."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bare-dir"
            root.mkdir()

            report = validate_four_layer_project(root)
            self.assertEqual(report.status, "ok")
            self.assertEqual(len(report.errors), 0)

    def test_partial_data_without_framework_is_ok(self) -> None:
        """Backfill produced dataset + source + project, but no framework yet.

        The validator should not report missing_ref for frameworkRef when
        the framework layer has not been populated.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = self._make_four_layer_root(Path(tmp), include_framework=False)

            report = validate_four_layer_project(root)
            self.assertEqual(
                report.status, "ok",
                f"Expected ok for partial data, got errors: "
                f"{[e.to_dict() for e in report.errors]}",
            )

    def test_auto_discover_without_project_refs(self) -> None:
        """Backfill created dataset + source files but .project.json has no refs.

        The validator should auto-discover files by scanning the directories
        instead of reporting missing_ref for every layer.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "four-layer"
            root.mkdir()

            # Dataset.
            ds_dir = root / "datasets"
            ds_dir.mkdir()
            ds_data = {
                "id": "tpch",
                "frameworkId": "tpch",
                "cases": [],
                "functions": [],
                "patterns": [],
                "rootCauses": [],
                "stackOverview": {"categories": [], "components": []},
            }
            (ds_dir / "tpch.dataset.json").write_text(
                json.dumps(ds_data), encoding="utf-8",
            )

            # Source.
            src_dir = root / "sources"
            src_dir.mkdir()
            src_data = {
                "id": "tpch",
                "frameworkId": "tpch",
                "repo": {"url": "https://github.com/test/repo"},
                "sourceFiles": [],
                "sourceAnchors": [],
                "artifactIndex": [],
            }
            (src_dir / "tpch.source.json").write_text(
                json.dumps(src_data), encoding="utf-8",
            )

            # Project JSON exists but has no refs at all (empty backfill output).
            proj_dir = root / "projects"
            proj_dir.mkdir()
            proj_data = {"id": "tpch"}
            (proj_dir / "tpch.project.json").write_text(
                json.dumps(proj_data), encoding="utf-8",
            )

            report = validate_four_layer_project(root)
            self.assertEqual(
                report.status, "ok",
                f"Expected ok for auto-discovered data, got errors: "
                f"{[e.to_dict() for e in report.errors]}",
            )

    def test_full_data_validates_ok(self) -> None:
        """All four layers present — should validate without errors."""
        with tempfile.TemporaryDirectory() as tmp:
            root = self._make_four_layer_root(Path(tmp), include_framework=True)

            report = validate_four_layer_project(root)
            self.assertEqual(
                report.status, "ok",
                f"Expected ok for full data, got errors: "
                f"{[e.to_dict() for e in report.errors]}",
            )

    def test_missing_artifact_in_source_reports_error(self) -> None:
        """Project references an artifact that was removed from source."""
        with tempfile.TemporaryDirectory() as tmp:
            root = self._make_four_layer_root(Path(tmp), include_framework=True)

            # Add a function binding that references a non-existent artifact.
            proj_path = root / "projects" / "tpch.project.json"
            proj = json.loads(proj_path.read_text())
            proj["functionBindings"] = [{
                "functionId": "func_abc",
                "armArtifactIds": ["missing_artifact_xyz"],
            }]
            proj_path.write_text(json.dumps(proj), encoding="utf-8")

            report = validate_four_layer_project(root)
            self.assertEqual(report.status, "error")
            error_msgs = " ".join(e.message for e in report.errors)
            self.assertIn("missing_artifact_xyz", error_msgs)


class TestConfigGateErrorDetail(unittest.TestCase):
    """Config gate should expose individual validation errors, not just count."""

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

    def test_config_validate_reports_error_details(self) -> None:
        """When four-layer validation fails, each error appears in issues."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fl"
            root.mkdir()

            # Create dataset + source to trigger validation, but leave
            # project JSON empty (no refs).
            ds_dir = root / "datasets"
            ds_dir.mkdir()
            (ds_dir / "x.dataset.json").write_text(
                json.dumps({"id": "test"}), encoding="utf-8",
            )
            src_dir = root / "sources"
            src_dir.mkdir()
            (src_dir / "x.source.json").write_text(
                json.dumps({"id": "test"}), encoding="utf-8",
            )

            proj_yaml = Path(tmp) / "project.yaml"
            proj_yaml.write_text(
                "\n".join([
                    "id: test-detail",
                    "fourLayerRoot: ./fl",
                    "workload:",
                    "  localDir: workload",
                    "run:",
                    "  platforms:",
                    "    - arm",
                ]),
                encoding="utf-8",
            )

            result = self.run_cli("config", "validate", str(proj_yaml), "--skip-bridge-token")

            payload = json.loads(result.stdout)
            if payload["status"] == "error":
                messages = " ".join(i["message"] for i in payload["issues"])
                # Each four-layer error should appear as a separate issue,
                # not just "failed with N errors".
                self.assertNotIn("failed with", messages)


if __name__ == "__main__":
    unittest.main()
