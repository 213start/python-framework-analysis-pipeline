"""`acquire` subcommand handlers (Step 5c: data parsing, local)."""
from __future__ import annotations

import json
from pathlib import Path

from ._common import resolve_run_dir, schemas_dir, write_manifest


def handle(args) -> int:
    if args.acquire_command == "timing":
        return cmd_acquire_timing(args)
    if args.acquire_command == "perf":
        return cmd_acquire_perf(args)
    if args.acquire_command == "asm":
        return cmd_acquire_asm(args)
    if args.acquire_command == "validate":
        return cmd_acquire_validate(args)
    if args.acquire_command == "all":
        return cmd_acquire_all(args)
    return 2


def cmd_acquire_timing(args) -> int:
    from ..acquisition.timing import collect_timing
    from ..acquisition.manifest import AcquisitionManifest, AcquisitionSection

    run_dir = resolve_run_dir(args)
    stdout_files = [Path(f) for f in (args.stdout_files or [])]

    result = collect_timing(run_dir, args.platform, stdout_files or None)
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201

    manifest_path = run_dir / "acquisition-manifest.json"
    if manifest_path.exists():
        from ..acquisition.manifest import load_manifest
        manifest = load_manifest(manifest_path)
    else:
        manifest = AcquisitionManifest(platform=args.platform, runDir=str(run_dir))
    manifest.timing = AcquisitionSection(
        status="collected" if result["cases"] else "skipped",
        files={"raw": result.get("raw_file", ""), "normalized": result.get("normalized_file", "")},
        extra={"cases": result.get("cases", [])},
    )
    write_manifest(run_dir, manifest)
    return 0


def cmd_acquire_perf(args) -> int:
    from ..acquisition.perf_profile import collect_perf
    from ..acquisition.manifest import AcquisitionManifest, AcquisitionSection

    run_dir = resolve_run_dir(args)
    perf_data = Path(args.perf_data) if args.perf_data else None
    kits_dir = Path(args.kits_dir) if args.kits_dir else None

    result = collect_perf(run_dir, args.platform, perf_data, kits_dir, top_n=args.top_n)
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201

    manifest_path = run_dir / "acquisition-manifest.json"
    if manifest_path.exists():
        from ..acquisition.manifest import load_manifest
        manifest = load_manifest(manifest_path)
    else:
        manifest = AcquisitionManifest(platform=args.platform, runDir=str(run_dir))
    manifest.perf = AcquisitionSection(
        status=result.get("status", "pending"),
        files=result.get("files", {}),
    )
    write_manifest(run_dir, manifest)
    return 0 if result.get("status") != "failed" else 1


def cmd_acquire_asm(args) -> int:
    from ..acquisition.machine_code import collect_asm
    from ..acquisition.manifest import AcquisitionManifest, AcquisitionSection

    run_dir = resolve_run_dir(args)
    perf_data = Path(args.perf_data) if args.perf_data else None
    kits_dir = Path(args.kits_dir) if args.kits_dir else None
    binaries = [Path(b) for b in (args.binaries or [])]

    result = collect_asm(run_dir, args.platform, perf_data, kits_dir, binaries, args.top_n)
    print(json.dumps(result, ensure_ascii=False, indent=2))  # noqa: T201

    manifest_path = run_dir / "acquisition-manifest.json"
    if manifest_path.exists():
        from ..acquisition.manifest import load_manifest
        manifest = load_manifest(manifest_path)
    else:
        manifest = AcquisitionManifest(platform=args.platform, runDir=str(run_dir))
    manifest.asm = AcquisitionSection(
        status=result.get("status", "pending"),
        extra={
            "hotspotCount": result.get("hotspotCount", 0),
            "objdumpFiles": result.get("objdumpFiles", []),
        },
    )
    write_manifest(run_dir, manifest)
    return 0 if result.get("status") != "failed" else 1


def cmd_acquire_validate(args) -> int:
    run_dir = Path(args.run_dir)
    manifest_path = run_dir / "acquisition-manifest.json"

    if not manifest_path.exists():
        print(json.dumps({  # noqa: T201
            "status": "error",
            "errors": [f"acquisition-manifest.json not found in {run_dir}"],
        }, ensure_ascii=False, indent=2))
        return 1

    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    schema_path = schemas_dir() / "acquisition-manifest.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    from ..validators.schema import validate_json_schema

    schema_issues = validate_json_schema(manifest_data, schema, "acquisition")
    if schema_issues:
        print(json.dumps({  # noqa: T201
            "status": "error",
            "errors": [
                f"{issue.path}: {issue.message}"
                for issue in schema_issues
            ],
        }, ensure_ascii=False, indent=2))
        return 1

    errors = []
    for section in ("timing", "perf", "asm"):
        sec = manifest_data.get(section, {})
        if sec.get("status") == "collected":
            for fname in sec.get("files", {}).values():
                if not (run_dir / fname).exists():
                    errors.append(f"{section}: missing file {fname}")

    if errors:
        print(json.dumps({"status": "error", "errors": errors}, ensure_ascii=False, indent=2))  # noqa: T201
        return 1

    print(json.dumps({  # noqa: T201
        "status": "ok",
        "sections": {
            s: manifest_data.get(s, {}).get("status", "pending")
            for s in ("timing", "perf", "asm")
        },
    }, ensure_ascii=False, indent=2))
    return 0


def cmd_acquire_all(args) -> int:
    rc = 0
    rc |= cmd_acquire_timing(args)
    rc |= cmd_acquire_perf(args)
    rc |= cmd_acquire_asm(args)
    return rc
