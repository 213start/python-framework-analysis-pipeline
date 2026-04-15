"""Validate environment records and readiness reports.

This module checks that manually or automatically filled records are
internally consistent and match their referenced plan.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..validators.schema import validate_json_schema


@dataclass
class ValidationIssue:
    path: str
    message: str


@dataclass
class EnvironmentValidationReport:
    status: str  # "ok" or "error"
    runDir: str
    issues: list[ValidationIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "runDir": self.runDir,
            "issueCount": len(self.issues),
            "issues": [
                {"path": i.path, "message": i.message}
                for i in self.issues
            ],
        }


def validate_run(run_dir: Path, schemas_dir: Path | None = None) -> EnvironmentValidationReport:
    """Validate a run directory containing plan + record + readiness report.

    Parameters
    ----------
    run_dir : Path
        Directory containing ``environment-plan.json``, ``environment-record.json``,
        and optionally ``readiness-report.json``.
    schemas_dir : Path, optional
        Directory with JSON Schema files. If provided, schemas are loaded from here.

    Returns
    -------
    EnvironmentValidationReport
    """
    report = EnvironmentValidationReport(status="ok", runDir=str(run_dir))
    schemas = _load_schemas(schemas_dir) if schemas_dir else {}

    # Load plan
    plan_path = run_dir / "environment-plan.json"
    if not plan_path.exists():
        report.issues.append(ValidationIssue(path="", message="environment-plan.json not found"))
        report.status = "error"
        return report

    plan = _load_json(plan_path)
    if plan is None:
        report.issues.append(ValidationIssue(path="environment-plan.json", message="Invalid JSON"))
        report.status = "error"
        return report

    # Validate plan against schema
    if "environment-plan" in schemas:
        issues = validate_json_schema(plan, schemas["environment-plan"], "plan")
        for iss in issues:
            report.issues.append(ValidationIssue(path=f"plan.{iss.path}", message=iss.message))

    # Load and validate record
    record_path = run_dir / "environment-record.json"
    if record_path.exists():
        record = _load_json(record_path)
        if record is None:
            report.issues.append(ValidationIssue(path="environment-record.json", message="Invalid JSON"))
        else:
            _validate_record(plan, record, report)
            if "environment-record" in schemas:
                issues = validate_json_schema(record, schemas["environment-record"], "record")
                for iss in issues:
                    report.issues.append(ValidationIssue(path=f"record.{iss.path}", message=iss.message))
    else:
        report.issues.append(ValidationIssue(path="", message="environment-record.json not found"))

    # Load and validate readiness report
    readiness_path = run_dir / "readiness-report.json"
    if readiness_path.exists():
        readiness = _load_json(readiness_path)
        if readiness is None:
            report.issues.append(ValidationIssue(path="readiness-report.json", message="Invalid JSON"))
        else:
            _validate_readiness(plan, readiness, report)
            if "readiness-report" in schemas:
                issues = validate_json_schema(readiness, schemas["readiness-report"], "readiness")
                for iss in issues:
                    report.issues.append(ValidationIssue(path=f"readiness.{iss.path}", message=iss.message))
    else:
        report.issues.append(ValidationIssue(path="", message="readiness-report.json not found (optional but recommended)"))

    if report.issues:
        report.status = "error"
    return report


def _validate_record(plan: dict[str, Any], record: dict[str, Any], report: EnvironmentValidationReport) -> None:
    """Cross-validate record against plan."""
    # planHash must match
    plan_hash = plan.get("planHash", "")
    record_hash = record.get("planHash", "")
    if plan_hash and record_hash and plan_hash != record_hash:
        report.issues.append(ValidationIssue(
            path="record.planHash",
            message=f"Record planHash '{record_hash}' does not match plan '{plan_hash}'",
        ))

    # Step IDs in record must come from plan
    plan_step_ids = {s["id"] for s in plan.get("steps", [])}
    for step in record.get("steps", []):
        step_id = step.get("id", "")
        if step_id not in plan_step_ids:
            report.issues.append(ValidationIssue(
                path=f"record.steps[{step_id}]",
                message=f"Step '{step_id}' not found in plan",
            ))

    # mutatesHost steps need status and logPath or note
    mutating_plan_steps = {s["id"] for s in plan.get("steps", []) if s.get("mutatesHost")}
    for step in record.get("steps", []):
        if step.get("id") in mutating_plan_steps:
            if step.get("status") not in ("passed", "failed", "skipped"):
                report.issues.append(ValidationIssue(
                    path=f"record.steps[{step['id']}]",
                    message="Mutating step must have a status",
                ))
            if not step.get("logPath") and not step.get("note"):
                report.issues.append(ValidationIssue(
                    path=f"record.steps[{step['id']}]",
                    message="Mutating step should have logPath or note",
                ))

    # manual-record mode needs provenance
    if record.get("mode") == "manual-record":
        provenance = record.get("provenance", {})
        if provenance.get("recordedBy") != "manual":
            report.issues.append(ValidationIssue(
                path="record.provenance.recordedBy",
                message="manual-record mode should have recordedBy=manual",
            ))


def _validate_readiness(plan: dict[str, Any], readiness: dict[str, Any], report: EnvironmentValidationReport) -> None:
    """Validate readiness report basic consistency."""
    status = readiness.get("status")
    if status not in ("ready", "not-ready", "unknown"):
        report.issues.append(ValidationIssue(
            path="readiness.status",
            message=f"Invalid readiness status: {status}",
        ))

    for check in readiness.get("checks", []):
        if check.get("status") not in ("passed", "failed", "unknown"):
            report.issues.append(ValidationIssue(
                path=f"readiness.checks[{check.get('id', '?')}]",
                message=f"Invalid check status: {check.get('status')}",
            ))


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _load_schemas(schemas_dir: Path) -> dict[str, dict]:
    """Load environment-related JSON Schemas from a directory."""
    schema_files = {
        "environment-plan": "environment-plan.schema.json",
        "environment-record": "environment-record.schema.json",
        "readiness-report": "readiness-report.schema.json",
    }
    result: dict[str, dict] = {}
    for key, filename in schema_files.items():
        path = schemas_dir / filename
        if path.exists():
            data = _load_json(path)
            if data:
                result[key] = data
    return result
