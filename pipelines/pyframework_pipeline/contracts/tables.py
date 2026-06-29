"""Table-level contracts: aggregated category/symbol/shared-object summaries.

Wraps the three summary CSVs produced by analyze/summarize_platform_perf.py:
category_summary.csv, shared_object_summary.csv, symbol_hotspots.csv. Column
names match exactly so the contract reads/writes files the pipeline already
produces. Serialization (`write_dir`/`read_dir`) is mandatory.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ._serde import fmt_float, fmt_int, parse_float, parse_int, read_csv_rows, write_csv_rows

CATEGORY_SUMMARY_FIELDS: tuple[str, ...] = (
    "platform_id",
    "benchmark",
    "category_top",
    "children_share",
    "self_share",
    "period_sum",
    "sample_count",
    "top_shared_object",
    "top_symbols_preview",
)

SHARED_OBJECT_SUMMARY_FIELDS: tuple[str, ...] = (
    "platform_id",
    "benchmark",
    "shared_object",
    "children_share",
    "self_share",
    "period_sum",
    "sample_count",
    "top_symbols_preview",
)

SYMBOL_HOTSPOTS_FIELDS: tuple[str, ...] = (
    "platform_id",
    "benchmark",
    "category_top",
    "category_sub",
    "shared_object",
    "symbol",
    "children_share",
    "self_share",
    "period_sum",
    "sample_count",
    "rank_in_category",
    "rank_in_shared_object",
)

CATEGORY_SUMMARY_NAME = "category_summary.csv"
SHARED_OBJECT_SUMMARY_NAME = "shared_object_summary.csv"
SYMBOL_HOTSPOTS_NAME = "symbol_hotspots.csv"


@dataclass(frozen=True)
class CategoryRow:
    platform_id: str = ""
    benchmark: str = ""
    category_top: str = ""
    children_share: float = 0.0
    self_share: float = 0.0
    period_sum: int = 0
    sample_count: int = 0
    top_shared_object: str = ""
    top_symbols_preview: str = ""

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "CategoryRow":
        return cls(
            platform_id=row.get("platform_id", ""),
            benchmark=row.get("benchmark", ""),
            category_top=row.get("category_top", ""),
            children_share=parse_float(row.get("children_share", "")),
            self_share=parse_float(row.get("self_share", "")),
            period_sum=parse_int(row.get("period_sum", "")),
            sample_count=parse_int(row.get("sample_count", "")),
            top_shared_object=row.get("top_shared_object", ""),
            top_symbols_preview=row.get("top_symbols_preview", ""),
        )

    def to_row(self) -> dict[str, str]:
        return {
            "platform_id": self.platform_id,
            "benchmark": self.benchmark,
            "category_top": self.category_top,
            "children_share": fmt_float(self.children_share),
            "self_share": fmt_float(self.self_share),
            "period_sum": fmt_int(self.period_sum),
            "sample_count": fmt_int(self.sample_count),
            "top_shared_object": self.top_shared_object,
            "top_symbols_preview": self.top_symbols_preview,
        }


@dataclass(frozen=True)
class SharedObjectRow:
    platform_id: str = ""
    benchmark: str = ""
    shared_object: str = ""
    children_share: float = 0.0
    self_share: float = 0.0
    period_sum: int = 0
    sample_count: int = 0
    top_symbols_preview: str = ""

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "SharedObjectRow":
        return cls(
            platform_id=row.get("platform_id", ""),
            benchmark=row.get("benchmark", ""),
            shared_object=row.get("shared_object", ""),
            children_share=parse_float(row.get("children_share", "")),
            self_share=parse_float(row.get("self_share", "")),
            period_sum=parse_int(row.get("period_sum", "")),
            sample_count=parse_int(row.get("sample_count", "")),
            top_symbols_preview=row.get("top_symbols_preview", ""),
        )

    def to_row(self) -> dict[str, str]:
        return {
            "platform_id": self.platform_id,
            "benchmark": self.benchmark,
            "shared_object": self.shared_object,
            "children_share": fmt_float(self.children_share),
            "self_share": fmt_float(self.self_share),
            "period_sum": fmt_int(self.period_sum),
            "sample_count": fmt_int(self.sample_count),
            "top_symbols_preview": self.top_symbols_preview,
        }


@dataclass(frozen=True)
class SymbolRow:
    platform_id: str = ""
    benchmark: str = ""
    category_top: str = ""
    category_sub: str = ""
    shared_object: str = ""
    symbol: str = ""
    children_share: float = 0.0
    self_share: float = 0.0
    period_sum: int = 0
    sample_count: int = 0
    rank_in_category: int = 0
    rank_in_shared_object: int = 0

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "SymbolRow":
        return cls(
            platform_id=row.get("platform_id", ""),
            benchmark=row.get("benchmark", ""),
            category_top=row.get("category_top", ""),
            category_sub=row.get("category_sub", ""),
            shared_object=row.get("shared_object", ""),
            symbol=row.get("symbol", ""),
            children_share=parse_float(row.get("children_share", "")),
            self_share=parse_float(row.get("self_share", "")),
            period_sum=parse_int(row.get("period_sum", "")),
            sample_count=parse_int(row.get("sample_count", "")),
            rank_in_category=parse_int(row.get("rank_in_category", "")),
            rank_in_shared_object=parse_int(row.get("rank_in_shared_object", "")),
        )

    def to_row(self) -> dict[str, str]:
        return {
            "platform_id": self.platform_id,
            "benchmark": self.benchmark,
            "category_top": self.category_top,
            "category_sub": self.category_sub,
            "shared_object": self.shared_object,
            "symbol": self.symbol,
            "children_share": fmt_float(self.children_share),
            "self_share": fmt_float(self.self_share),
            "period_sum": fmt_int(self.period_sum),
            "sample_count": fmt_int(self.sample_count),
            "rank_in_category": fmt_int(self.rank_in_category),
            "rank_in_shared_object": fmt_int(self.rank_in_shared_object),
        }


@dataclass(frozen=True)
class AggregatedTables:
    """C3 output: the three summary tables bundled as one contract."""

    by_category: tuple[CategoryRow, ...] = ()
    by_shared_object: tuple[SharedObjectRow, ...] = ()
    by_symbol: tuple[SymbolRow, ...] = ()

    def write_dir(self, base: Path) -> None:
        write_csv_rows(
            base / CATEGORY_SUMMARY_NAME, CATEGORY_SUMMARY_FIELDS,
            [r.to_row() for r in self.by_category],
        )
        write_csv_rows(
            base / SHARED_OBJECT_SUMMARY_NAME, SHARED_OBJECT_SUMMARY_FIELDS,
            [r.to_row() for r in self.by_shared_object],
        )
        write_csv_rows(
            base / SYMBOL_HOTSPOTS_NAME, SYMBOL_HOTSPOTS_FIELDS,
            [r.to_row() for r in self.by_symbol],
        )

    @classmethod
    def read_dir(cls, base: Path) -> "AggregatedTables":
        def _read(name: str, ctor):
            p = base / name
            if not p.exists():
                return ()
            return tuple(ctor(row) for row in read_csv_rows(p))

        return cls(
            by_category=_read(CATEGORY_SUMMARY_NAME, CategoryRow.from_row),
            by_shared_object=_read(SHARED_OBJECT_SUMMARY_NAME, SharedObjectRow.from_row),
            by_symbol=_read(SYMBOL_HOTSPOTS_NAME, SymbolRow.from_row),
        )
