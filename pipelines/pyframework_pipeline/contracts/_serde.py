"""Shared serialization helpers for contract dataclasses.

Contract serialization is mandatory (not optional) so every pipeline step's
input and output is auditable on disk. CSVs are the on-disk form for record
and table contracts; these helpers read/write dict[str, str] rows that match
the kits NORMALIZED_FIELDS schema so existing readers and migrated tests keep
working.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable, Sequence


def fmt_int(value: int) -> str:
    return str(value)


def parse_int(value: str) -> int:
    raw = (value or "").strip()
    if not raw:
        return 0
    return int(float(raw.replace(",", "")))


def fmt_float(value: float) -> str:
    # kits stores percents/floats via format_float (6 digits); mirror that so
    # round-trips are byte-stable against the original CSVs.
    if value == 0:
        return "0"
    return format(value, ".6f").rstrip("0").rstrip(".")


def parse_float(value: str) -> float:
    raw = (value or "").strip().rstrip("%")
    return float(raw) if raw else 0.0


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv_rows(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if v is None else str(v)) for k, v in row.items()})
