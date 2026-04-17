"""Generate Project.caseBindings and functionBindings from Dataset + Source.

Creates cross-reference bindings that link Dataset entities (cases, functions)
to Source entities (artifacts, sourceAnchors).
"""

from __future__ import annotations

from typing import Any


def generate_bindings(
    dataset: dict,
    source: dict,
) -> dict[str, Any]:
    """Generate Project binding arrays from Dataset + Source cross-references.

    Parameters
    ----------
    dataset : dict
        The Dataset layer (must have ``cases`` and ``functions``).
    source : dict
        The Source layer (must have ``artifactIndex``).

    Returns
    -------
    dict with ``caseBindings`` and ``functionBindings`` arrays.
    """
    artifact_by_id = {
        a["id"]: a for a in source.get("artifactIndex", [])
    }

    case_bindings = _build_case_bindings(
        dataset.get("cases", []),
        artifact_by_id,
    )
    function_bindings = _build_function_bindings(
        dataset.get("functions", []),
        artifact_by_id,
    )

    return {
        "caseBindings": case_bindings,
        "functionBindings": function_bindings,
    }


def _build_case_bindings(
    cases: list[dict],
    artifact_by_id: dict[str, dict],
) -> list[dict]:
    """Build caseBindings: link each case to its SQL and UDF artifacts."""
    bindings: list[dict] = []

    for case in cases:
        case_id = case.get("id", "")
        legacy_id = case.get("legacyCaseId", "")

        # Collect primary artifacts (SQL + Python UDF) from the case's artifactIds.
        primary_ids: list[str] = []
        for aid in case.get("artifactIds", []):
            if aid in artifact_by_id:
                primary_ids.append(aid)

        binding: dict[str, Any] = {
            "caseId": case_id,
        }
        if legacy_id:
            binding["legacyCaseId"] = legacy_id
        if primary_ids:
            binding["primaryArtifactIds"] = primary_ids

        bindings.append(binding)

    return bindings


def _build_function_bindings(
    functions: list[dict],
    artifact_by_id: dict[str, dict],
) -> list[dict]:
    """Build functionBindings: link each function to its ARM/x86 asm artifacts."""
    bindings: list[dict] = []

    for func in functions:
        func_id = func.get("id", "")
        if not func_id:
            continue

        arm_artifacts: list[str] = []
        x86_artifacts: list[str] = []

        # Collect assembly artifact IDs from the function's artifactIds.
        for aid in func.get("artifactIds", []):
            art = artifact_by_id.get(aid)
            if art is None:
                continue
            platform = art.get("platform", "")
            if platform == "arm64":
                arm_artifacts.append(aid)
            elif platform == "x86_64":
                x86_artifacts.append(aid)

        binding: dict[str, Any] = {
            "functionId": func_id,
        }
        if arm_artifacts:
            binding["armArtifactIds"] = arm_artifacts
        if x86_artifacts:
            binding["x86ArtifactIds"] = x86_artifacts

        bindings.append(binding)

    return bindings
