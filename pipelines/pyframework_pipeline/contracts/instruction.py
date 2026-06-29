"""Instruction-level contract: the C4 input derived from perf annotate.

Wraps the instruction_hotspots.csv produced by analyze/annotate_perf_hotspots.py.
Column names match the analyze INSTRUCTION_FIELDS schema exactly. The C4
annotate subflow consumes this (alongside the classified records / tables) to
correlate instruction-level hotspots with symbols/categories. Serialization is
mandatory; the instruction input may also be supplied externally (hand-prepared
file at the configured path) so the analyze chain can run without acquisition.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ._serde import fmt_float, fmt_int, parse_float, parse_int, read_csv_rows, write_csv_rows

INSTRUCTION_FIELDS: tuple[str, ...] = (
    "category_top",
    "shared_object",
    "symbol",
    "segment_id",
    "line_index",
    "ip",
    "instruction_offset",
    "instruction_share",
    "instruction_text",
)

INSTRUCTION_HOTSPOTS_NAME = "instruction_hotspots.csv"


@dataclass(frozen=True)
class InstructionSample:
    """One instruction-level hotspot row."""

    category_top: str = ""
    shared_object: str = ""
    symbol: str = ""
    segment_id: str = ""
    line_index: int = 0
    ip: int = 0
    instruction_offset: str = ""  # hex string in CSV (e.g. "0x40")
    instruction_share: float = 0.0
    instruction_text: str = ""

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "InstructionSample":
        return cls(
            category_top=row.get("category_top", ""),
            shared_object=row.get("shared_object", ""),
            symbol=row.get("symbol", ""),
            segment_id=row.get("segment_id", ""),
            line_index=parse_int(row.get("line_index", "")),
            ip=parse_int(row.get("ip", "")),
            instruction_offset=row.get("instruction_offset", ""),
            instruction_share=parse_float(row.get("instruction_share", "")),
            instruction_text=row.get("instruction_text", ""),
        )

    def to_row(self) -> dict[str, str]:
        return {
            "category_top": self.category_top,
            "shared_object": self.shared_object,
            "symbol": self.symbol,
            "segment_id": self.segment_id,
            "line_index": fmt_int(self.line_index),
            "ip": fmt_int(self.ip),
            "instruction_offset": self.instruction_offset,
            "instruction_share": fmt_float(self.instruction_share),
            "instruction_text": self.instruction_text,
        }


@dataclass(frozen=True)
class InstructionDataset:
    """C4 instruction input contract: the instruction_hotspots table."""

    samples: tuple[InstructionSample, ...] = ()

    def write_dir(self, base: Path) -> None:
        write_csv_rows(
            base / INSTRUCTION_HOTSPOTS_NAME, INSTRUCTION_FIELDS,
            [s.to_row() for s in self.samples],
        )

    @classmethod
    def read_dir(cls, base: Path) -> "InstructionDataset":
        p = base / INSTRUCTION_HOTSPOTS_NAME
        if not p.exists():
            return cls()
        return cls(samples=tuple(InstructionSample.from_row(r) for r in read_csv_rows(p)))

    def write_file(self, path: Path) -> None:
        write_csv_rows(path, INSTRUCTION_FIELDS, [s.to_row() for s in self.samples])

    @classmethod
    def read_file(cls, path: Path) -> "InstructionDataset":
        if not path.exists():
            return cls()
        return cls(samples=tuple(InstructionSample.from_row(r) for r in read_csv_rows(path)))
