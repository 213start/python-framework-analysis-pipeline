"""Timing contract: the B3 acquisition output (wall-clock query timings).

Wraps the timing-normalized.json produced by the timing step. JSON shape
matches what compare/pipeline.py reads today (`cases[].metrics.wallClockTime.
wall_clock_ns`) so existing readers keep working. Serialization is mandatory.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class TimingEntry:
    """One query's wall-clock timing observation."""

    label: str = ""              # query / case id
    wall_clock_ns: int = 0       # nanoseconds (the field compare reads)


@dataclass(frozen=True)
class TimingDataset:
    """B3 output: the set of query timings for one platform."""

    entries: tuple[TimingEntry, ...] = ()
    platform_id: str = ""
    benchmark: str = ""

    def write_json(self, path: Path) -> None:
        """Write in the timing-normalized.json shape compare/pipeline.py expects."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "platform_id": self.platform_id,
            "benchmark": self.benchmark,
            "cases": [
                {
                    "label": e.label,
                    "metrics": {"wallClockTime": {"wall_clock_ns": e.wall_clock_ns}},
                }
                for e in self.entries
            ],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def read_json(cls, path: Path) -> "TimingDataset":
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        entries: list[TimingEntry] = []
        for case in data.get("cases", []):
            wc = case.get("metrics", {}).get("wallClockTime", {})
            ns = wc.get("wall_clock_ns")
            if ns is None:
                continue
            entries.append(
                TimingEntry(label=case.get("label", case.get("query", "")), wall_clock_ns=int(ns))
            )
        return cls(
            entries=tuple(entries),
            platform_id=data.get("platform_id", ""),
            benchmark=data.get("benchmark", ""),
        )
