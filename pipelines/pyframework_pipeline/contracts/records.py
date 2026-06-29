"""Record-level contracts: RawSample (C1 output) and PerfRecord (C2 output).

Typed frozen wrappers over the perf_records.csv / classified_records.csv
rows. CSV column names match the analyze NORMALIZED_FIELDS schema exactly so
existing readers and the migrated kits tests keep working.

A `PerfRecord` with empty category_* fields means "not yet classified" (C1
output); after C2 the same type carries the CPython categories.

Serialization (`to_file`/`from_file`) is mandatory: every step's input and
output lives on disk for auditability and so any subflow can be run standalone.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ._serde import fmt_float, fmt_int, parse_float, parse_int, read_csv_rows, write_csv_rows

# The on-disk column order matches analyze.perf_analysis_common.NORMALIZED_FIELDS
# so this contract reads/writes files the kits pipeline already produces.
RECORD_FIELDS: tuple[str, ...] = (
    "platform_id",
    "arch",
    "python_version",
    "build_id",
    "benchmark",
    "event",
    "children",
    "self",
    "period",
    "pid",
    "command",
    "pid_command",
    "shared_object",
    "symbol",
    "ip",
    "category_top",
    "category_sub",
    "category_reason",
    "source_report",
    "sample_count",
)


@dataclass(frozen=True)
class RawSample:
    """One normalized perf sample (C1 parse output, uncategorized)."""

    platform_id: str = ""
    arch: str = ""
    python_version: str = ""
    build_id: str = ""
    benchmark: str = ""
    event: str = ""
    children_share: float = 0.0
    period_share: float = 0.0
    period: int = 0
    pid: int = 0
    command: str = ""
    pid_command: str = ""
    shared_object: str = ""
    symbol: str = ""
    ip: int = 0
    source_report: str = ""
    sample_count: int = 0

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "RawSample":
        return cls(
            platform_id=row.get("platform_id", ""),
            arch=row.get("arch", ""),
            python_version=row.get("python_version", ""),
            build_id=row.get("build_id", ""),
            benchmark=row.get("benchmark", ""),
            event=row.get("event", ""),
            children_share=parse_float(row.get("children", "")),
            period_share=parse_float(row.get("self", "")),
            period=parse_int(row.get("period", "")),
            pid=parse_int(row.get("pid", "")),
            command=row.get("command", ""),
            pid_command=row.get("pid_command", ""),
            shared_object=row.get("shared_object", ""),
            symbol=row.get("symbol", ""),
            ip=parse_int(row.get("ip", "")),
            source_report=row.get("source_report", ""),
            sample_count=parse_int(row.get("sample_count", "")),
        )

    def to_row(self) -> dict[str, str]:
        return {
            "platform_id": self.platform_id,
            "arch": self.arch,
            "python_version": self.python_version,
            "build_id": self.build_id,
            "benchmark": self.benchmark,
            "event": self.event,
            "children": fmt_float(self.children_share),
            "self": fmt_float(self.period_share),
            "period": fmt_int(self.period),
            "pid": fmt_int(self.pid),
            "command": self.command,
            "pid_command": self.pid_command,
            "shared_object": self.shared_object,
            "symbol": self.symbol,
            "ip": fmt_int(self.ip),
            "category_top": "",
            "category_sub": "",
            "category_reason": "",
            "source_report": self.source_report,
            "sample_count": fmt_int(self.sample_count),
        }


@dataclass(frozen=True)
class PerfRecord(RawSample):
    """A classified record (C2 output). Adds CPython category fields.

    Empty category_top means "unclassified" — this is how a PerfRecord read
    back from a C1-produced perf_records.csv looks.
    """

    category_top: str = ""
    category_sub: str = ""
    category_reason: str = ""

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "PerfRecord":
        base = RawSample.from_row(row)
        return cls(
            **{name: getattr(base, name) for name in RawSample.__dataclass_fields__},
            category_top=row.get("category_top", ""),
            category_sub=row.get("category_sub", ""),
            category_reason=row.get("category_reason", ""),
        )

    def to_row(self) -> dict[str, str]:
        row = super().to_row()
        row["category_top"] = self.category_top
        row["category_sub"] = self.category_sub
        row["category_reason"] = self.category_reason
        return row


def records_to_file(path: Path, records: list[RawSample]) -> None:
    """Write records to a CSV with the RECORD_FIELDS schema (category cols empty)."""
    write_csv_rows(path, RECORD_FIELDS, [r.to_row() for r in records])


def records_from_file(path: Path) -> list[PerfRecord]:
    """Read records from CSV as PerfRecord (category empty if unclassified)."""
    return [PerfRecord.from_row(row) for row in read_csv_rows(path)]


def raw_samples_from_file(path: Path) -> list[RawSample]:
    """Read records from CSV as RawSample (categories discarded)."""
    return [RawSample.from_row(row) for row in read_csv_rows(path)]


# Convenience methods bound onto the classes so callers can do
# `PerfRecord.to_file(path, [rec])` / `PerfRecord.from_file(path)`.
def _to_file_records(cls, path: Path, records: list[RawSample]) -> None:
    records_to_file(path, records)


def _from_file_perf(cls, path: Path) -> list[PerfRecord]:
    return records_from_file(path)


def _from_file_raw(cls, path: Path) -> list[RawSample]:
    return raw_samples_from_file(path)


RawSample.to_file = classmethod(_to_file_records)  # type: ignore[attr-defined]
RawSample.from_file = classmethod(_from_file_raw)  # type: ignore[attr-defined]
PerfRecord.to_file = classmethod(_to_file_records)  # type: ignore[attr-defined]
PerfRecord.from_file = classmethod(_from_file_perf)  # type: ignore[attr-defined]
