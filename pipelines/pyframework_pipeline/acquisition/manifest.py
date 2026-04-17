"""Acquisition manifest — shared data model for acquisition-manifest.json."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


@dataclass
class AcquisitionSection:
    """One section of the acquisition manifest (timing, perf, or asm)."""
    status: str = "pending"  # pending | collected | failed | skipped
    files: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"status": self.status}
        if self.files:
            d["files"] = self.files
        d.update(self.extra)
        return d


@dataclass
class AcquisitionManifest:
    """Master manifest for a single run's data acquisition."""
    projectId: str = ""
    platform: str = ""
    runDir: str = ""
    timing: AcquisitionSection = field(default_factory=AcquisitionSection)
    perf: AcquisitionSection = field(default_factory=AcquisitionSection)
    asm: AcquisitionSection = field(default_factory=AcquisitionSection)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "projectId": self.projectId,
            "platform": self.platform,
            "runDir": self.runDir,
            "timing": self.timing.to_dict(),
            "perf": self.perf.to_dict(),
            "asm": self.asm.to_dict(),
        }

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )


def load_manifest(path: Path) -> AcquisitionManifest:
    """Load an existing manifest from disk."""
    data = json.loads(path.read_text(encoding="utf-8"))
    m = AcquisitionManifest(
        projectId=data.get("projectId", ""),
        platform=data.get("platform", ""),
        runDir=data.get("runDir", ""),
    )
    for section_name in ("timing", "perf", "asm"):
        raw = data.get(section_name, {})
        section = AcquisitionSection(
            status=raw.get("status", "pending"),
            files=raw.get("files", {}),
            extra={k: v for k, v in raw.items() if k not in ("status", "files")},
        )
        setattr(m, section_name, section)
    return m
